//! Sync — peer 間で WAL レコードを pull して本体に LWW apply する。
//!
//! # 使い方
//!
//! ```
//! use std::sync::Arc;
//! use enchudb_engine::Engine;
//! use enchudb_engine::transport::{InMemoryTransport, Transport};
//! use enchudb_sync::Syncer;
//!
//! let path = format!("/tmp/enchudb-sync-doc-{}.db", std::process::id());
//! let _ = std::fs::remove_file(&path);
//! let _ = std::fs::remove_file(format!("{}.oplog", path));
//! let _ = std::fs::remove_file(format!("{}.tables", path));
//! let _ = std::fs::remove_file(format!("{}.db.lock", path));
//! // Sync を使う場合は必ず WAL + sync tables 有効な Engine を使う。
//! {
//!     let mut eng_init = Engine::create_standalone(&path).unwrap();
//!     eng_init.enable_sync_tables().unwrap();
//!     eng_init.flush().unwrap();
//! }
//! let eng_a = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
//!
//! let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
//! let syncer = Syncer::new(eng_a.clone(), transport);
//! let out = syncer.pull_once(2); // 未知の peer から pull、0 件
//! assert_eq!(out.received, 0);
//! # drop(eng_a);
//! # let _ = std::fs::remove_file(&path);
//! # let _ = std::fs::remove_file(format!("{}.oplog", path));
//! # let _ = std::fs::remove_file(format!("{}.tables", path));
//! # let _ = std::fs::remove_file(format!("{}.db.lock", path));
//! ```
//!
//! # LWW 規則
//!
//! 受信 op を `HlcStore` の既存 HLC と比較して:
//!
//! - `(eid, himo)` ペアの既存 HLC より受信 HLC が**厳密に大きい** → apply
//! - それ以外(等しい、または既存が大きい) → skip
//!
//! Delete は特殊: himo を持たないため per-himo の比較はせず、 **tombstone slot
//! (sentinel himo_id = `u16::MAX`) との LWW** で判定する。 apply されると
//! tombstone HLC が記録され、 以後それより古い Tie/Untie は skip される
//! (削除済み entity の復活防止)。 逆に reorder 配送で「新しい Tie の後に古い
//! Delete」が届いた場合、 per-himo HLC は参照しないため Delete が entity を
//! 物理削除する — 同一 author の log は HLC 順なのでこの経路は再送/gossip の
//! 交錯時のみ (0.9.0 で doc を実装に合わせて訂正、 挙動は従来どおり)。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use enchudb_engine::engine::{Engine, RemoteApply};
use enchudb_engine::hlc_store::HlcStore;
use enchudb_engine::transport::{Transport, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};

use crate::subscription::{AllRecords, SubscriptionFilter};

pub struct Syncer {
    engine: Arc<Engine>,
    transport: Arc<dyn Transport>,
    /// 各 pull 先 (link) から最後に pull した地点。 #216: relay の stream は
    /// 複数 author の merge で HLC 非単調なので、 cursor は **link × author** の
    /// 2 段 map — author ごとの substream は relay を何 hop 挟んでも単調、 が
    /// この粒度を健全にする不変式。 relay を使わない 1-hop 構成では
    /// `{link: {author=link: hlc}}` の 1 entry で従来と同一挙動。
    /// `cursor_path` が設定されていれば update のたびにディスクに保存し、
    /// `Syncer::new` 時にロードして差分同期を継続する。
    last_pulled:
        std::sync::Mutex<std::collections::HashMap<PeerId, std::collections::HashMap<PeerId, Hlc>>>,
    /// `last_pulled` の永続化先。 `None` ならメモリのみ。
    cursor_path: std::sync::RwLock<Option<PathBuf>>,
    /// Phase C: true なら署名検証を強制。未署名 or 検証失敗 op は reject。
    require_signature: std::sync::atomic::AtomicBool,
    /// request4: per-peer subscription filter。 default は `AllRecords` (全送り、
    /// 旧 `publish_since` の挙動)。 `set_subscription_filter` で差し替え可。
    subscription_filter: std::sync::RwLock<Arc<dyn SubscriptionFilter>>,
    /// #9 foot-gun ガード: self_peer == 0 で foreign record を apply した事の一度だけ警告。
    warned_unconfigured_peer: std::sync::atomic::AtomicBool,
    // 0.9.0: 旧 Content reorder buffer (`pending_ops`) は削除 — content は
    // TieNamed で運ばれ、 自力で entity 写像を作れるため退避が不要になった。
}

/// 1 回の pull-apply サイクルの結果。
#[derive(Debug, Clone, Default)]
pub struct SyncOutcome {
    /// 受信総数。
    pub received: usize,
    /// LWW で新規/上書きされた op 数。
    pub applied: usize,
    /// **LWW で古いと判定して skip した op 数**。 正常系の counter。
    ///
    /// 「相手より自分の方が新しい」だけなので、 再配送は不要。 宛先を解決できずに
    /// 捨てた分は [`SyncOutcome::dropped_unresolved`] に分けてある (両者を合算すると、
    /// 無視して良い LWW noise に本物の欠落が紛れる)。
    pub skipped: usize,
    /// **宛先を解決できずに捨てた op 数**。 pull cursor はこれを越えて前進するので、
    /// **二度と再配送されない**。
    ///
    /// 内訳は「entity 写像が引けない」「himo を定義できない (予算枯渇)」
    /// 「ref の target を解決できない」。 一度も sync されていない foreign entity 宛の
    /// `Delete` のように、 **正常系でも 0 にならない** (消す対象が無いので no-op が
    /// 正しい) が、 予期しない増加は配送欠落の兆候。
    ///
    /// 「配送で落ちた」のか「自分のゲートで止めた」のかを caller が切り分けるための
    /// counter。 **vocab 未翻訳は [`SyncOutcome::dropped_vocab`] に分けてある** —
    /// 背景値のあるこの counter に、 定常 0 であるべき値を混ぜると閾値が引けない。
    pub dropped_unresolved: usize,
    /// **author の vocab id を local vid に翻訳できず、 書かずに捨てた op 数**。
    ///
    /// text 値 (Tag/Leaf) は `(author_peer, remote_vid) → local_vid` の写像でしか
    /// 意味を持たない。 写像は `.vocabmap` sidecar で永続するが、 ring buffer の
    /// 巻き込みで `Vocab` op 自体を取り逃した場合は欠ける。 欠けた vid を**生値の
    /// まま cell に書く**と、 その番号が指す**無関係な local 文字列**が入る
    /// (実地発現: `path` 列に別行の PK、 `size` 列に mtime、 `key` 列に hash)。
    /// 黙って壊すより書かない方が良いのでここで数える。
    ///
    /// [`SyncOutcome::dropped_unresolved`] と**分けてある**: あちらは一度も sync
    /// されていない foreign entity 宛の `Delete` などで**正常系でも 0 にならない**
    /// 背景値を持つ。 こちらは**定常状態で 0 であるべき**値で、 > 0 なら当該 cell は
    /// **古いまま**なので、 caller は再 author / bootstrap で埋め直すこと。
    pub dropped_vocab: usize,
    /// Phase C: 署名検証で reject した op 数。
    pub rejected_signature: usize,
    /// Phase C: ACL で reject した op 数。
    pub rejected_acl: usize,
    /// #210: **容量が足りず apply を拒否した op 数** (ディスク満杯 / content 天井)。
    ///
    /// 値は一切書いていないので、 [`SyncOutcome::skipped`] (= LWW で古い、 再配送不要)
    /// とは意味が真逆で、 **空きが出てからの再配送が必要**。 そのため
    /// [`SyncOutcome::min_rejected_hlc`] にも計上して cursor を止める。
    ///
    /// > 0 なら engine 側の `FaultKind::DiskSpace` / `ContentSpace` も同時に立っている。
    pub rejected_capacity: usize,
    /// #78 (0.9.0): reject された record の最小 HLC。 pull cursor は
    /// これを越えて前進しない — pubkey 登録との race 窓で reject された record が
    /// 永久に再配送されない silent gap を防ぐ (次回 pull で再検証される)。
    ///
    /// #210: 署名 / ACL に加えて **容量拒否** ([`SyncOutcome::rejected_capacity`]) も
    /// ここに計上する。 どちらも 「今は入れられないが、 条件が変われば入る」 ので
    /// cursor を越えさせてはいけない。
    pub min_rejected_hlc: Option<Hlc>,
    /// #140: **自分の cursor より新しい履歴が publisher 側で既に reclaim されていた**。
    ///
    /// `_sync_ops` は ring buffer なので、 登録済み全 peer が consume した分は捨てられる。
    /// 未登録の新規 peer や長期オフラインの peer は、 差分 pull では**追いつけない**
    /// (部分履歴を全履歴として適用すると store が黙って不完全になる = #140)。
    ///
    /// `true` のとき **records は一切適用していない**。 caller は差分 pull を諦めて
    /// [`Syncer::bootstrap_pull`] (author が `serve_state` 済みの transport) か
    /// `GET /bootstrap` (replica 型の snapshot 取得) からやり直す必要がある。
    pub history_truncated: bool,
}

/// #140: [`Syncer::bootstrap_pull`] の結果。
#[derive(Debug, Clone)]
pub struct BootstrapOutcome {
    /// state record 適用の内訳 (LWW 冪等なので再 bootstrap では applied が減る)。
    pub outcome: SyncOutcome,
    /// ghost sweep で削除した entity 数 (= state に現れなかった author の行)。
    pub swept: usize,
    /// state 合成時点の HLC。 適用後の pull cursor はここに揃っている。
    pub as_of: Hlc,
}

/// `apply_one` の結果。
///
/// 「LWW で古いから適用しない」と「宛先を解決できないから捨てる」を **bool 1 本で
/// 返していたため合算されていた**。 前者は正常系で再配送も不要、 後者は cursor が
/// 越えるので二度と来ない — 監視上まったく別物なので型で分ける。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyResult {
    /// 適用した。
    Applied,
    /// LWW で古いと判定した (または既に適用済みの重複)。 再配送は不要。
    SkippedOlder,
    /// **宛先を解決できず捨てた**。 cursor は越えるので二度と再配送されない。
    Dropped,
    /// **author の vocab id を翻訳できず捨てた**。 `Dropped` と分けるのは、
    /// あちらが正常系でも立つ背景値を持つのに対し、 こちらは定常 0 であるべき
    /// = 監視上の意味が逆だから ([`SyncOutcome::dropped_vocab`])。
    DroppedVocab,
    /// #210: **容量が足りず apply を拒否した**。 値は書いていないので、
    /// `SkippedOlder` (再配送不要) ではなく **cursor を止めて再配送させる**。
    RejectedCapacity,
}

impl From<RemoteApply> for ApplyResult {
    fn from(r: RemoteApply) -> Self {
        match r {
            RemoteApply::Applied => ApplyResult::Applied,
            RemoteApply::Stale => ApplyResult::SkippedOlder,
            RemoteApply::RejectedCapacity => ApplyResult::RejectedCapacity,
        }
    }
}

impl ApplyResult {
    /// `remote_*_apply` の LWW 判定 (bool) を型に持ち上げる。
    /// `false` = 「自分の方が新しいので書かなかった」= 正常系。
    #[inline]
    fn from_lww(applied: bool) -> Self {
        if applied { ApplyResult::Applied } else { ApplyResult::SkippedOlder }
    }
}

impl Syncer {
    /// WAL 無しの Engine で Syncer を作ると panic する。
    ///
    /// Sync は WAL に commit 済みレコードを追記し、`publish_since` でそれを
    /// 他 peer に流す設計。`Engine::open` / `Engine::create` で開いた
    /// 旧来の WAL 無し Engine を渡すと `publish_since` は常に 0 件配送する
    /// silent footgun を作るので、ここで loud に止める。
    ///
    /// WAL 有効な Engine を作るには `Engine::open_concurrent_with_oplog` /
    /// `Engine::create_concurrent_with_oplog` を使うこと。
    pub fn new(engine: Arc<Engine>, transport: Arc<dyn Transport>) -> Self {
        match Self::try_new(engine, transport) {
            Ok(s) => s,
            Err(e) => panic!("{e}"),
        }
    }

    /// [`Syncer::new`] の非 panic 版 (#59)。
    ///
    /// `new` の 2 つの前提 (WAL 有効 / sync tables 有効) は 「使い方の誤り」 なので
    /// loud に止めるのが既定だが、 embedded DB を host app に埋め込む caller は
    /// process を殺さずに判断したい。 前提を満たさなければ `Err` を返す。
    pub fn try_new(
        engine: Arc<Engine>,
        transport: Arc<dyn Transport>,
    ) -> Result<Self, String> {
        let Some(wal) = engine.oplog_arc() else {
            return Err(
                "Syncer requires a WAL-enabled Engine. \
                 Use Engine::open_concurrent_with_oplog / create_concurrent_with_oplog \
                 instead of Engine::open / create."
                    .to_string(),
            );
        };
        // 0.8.0: sync 配信の primary は `_sync_ops` 一本、 legacy oplog iter
        // fallback は撤去。 `Database::enable_sync()` (= `enable_sync_tables`) を
        // 呼んでない engine で Syncer を attach するのは fatal。
        if !engine.sync_tables_enabled() {
            return Err(
                "Syncer requires sync tables (_sync_ops / _sync_peers). \
                 Call Database::enable_sync() / Engine::enable_sync_tables() before \
                 attaching Syncer."
                    .to_string(),
            );
        }
        // 0.8.0: oplog auto_reset を OFF にする hack は撤去。 publish path が
        // `_sync_ops` 経由になったので、 oplog ring buffer は通常通り自然 reset
        // して構わない (= transfer 自動化で `_sync_ops` に bridge 済の record
        // だけが oplog に残る、 全 peer ack 後の reclaim は `_sync_ops` 側で行う)。
        let _ = wal; // unused after 0.8.0 fallback removal
        let syncer = Self {
            engine: engine.clone(),
            transport,
            last_pulled: std::sync::Mutex::new(std::collections::HashMap::new()),
            cursor_path: std::sync::RwLock::new(None),
            require_signature: std::sync::atomic::AtomicBool::new(false),
            subscription_filter: std::sync::RwLock::new(Arc::new(AllRecords)),
            warned_unconfigured_peer: std::sync::atomic::AtomicBool::new(false),
        };
        // request17 step 6: **v9 DB では hydrate しない**。
        //
        // v9 (per-cell version column) では LWW の版数が cell と一緒に永続するので、
        // 「配送バッファ (`_sync_ops`) から揮発 HashMap を再構築する」必要が無い。
        // この再構築こそが #140 / #154 / #160 の共通の根 (= 配れる履歴が reclaim
        // されたら記憶も消える) だったので、 v9 では経路ごと通らない。
        //
        // pre-v9 DB (v8 以前で作られ、 migration していない DB) は版数の置き場が
        // 揮発 `HlcStore` のままなので、 従来どおり hydrate する。 pre-v9 の
        // サポートを落とす時にこの分岐ごと消える。
        if !engine.has_cell_version() {
            syncer.hydrate_hlc_store(&engine);
        }
        Ok(syncer)
    }

    /// `last_pulled` の永続化先を設定し、 既存ファイルから cursor をロードする。
    /// `pull_once` で cursor が前進すると自動的にこのパスへ atomic write する。
    /// `None` に戻したい場合は `Syncer::new` で作り直す。
    pub fn with_cursor_path(self, path: PathBuf) -> Self {
        self.load_cursors(&path);
        *self.cursor_path.write().unwrap() = Some(path);
        self
    }

    fn load_cursors(&self, path: &Path) {
        let Ok(s) = std::fs::read_to_string(path) else { return };
        let mut guard = self.last_pulled.lock().unwrap();
        for line in s.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            // #216 v2 書式: `link author wall logical hlc_peer` (5 field)。
            // legacy 4 field (`link wall logical hlc_peer`) は author=link の
            // cursor として読む — 他 author は Hlc::ZERO 起点になり再配送されるが
            // apply は LWW で冪等。 旧 scalar cursor が silent drop した relayed
            // record は、 この再配送で自己修復される。
            let (link, author, rest) = match parts.len() {
                5 => {
                    let Ok(l) = parts[0].parse::<PeerId>() else { continue };
                    let Ok(a) = parts[1].parse::<PeerId>() else { continue };
                    (l, a, &parts[2..])
                }
                4 => {
                    let Ok(l) = parts[0].parse::<PeerId>() else { continue };
                    (l, l, &parts[1..])
                }
                _ => continue,
            };
            let Ok(wall) = rest[0].parse::<u64>() else { continue };
            let Ok(logical) = rest[1].parse::<u32>() else { continue };
            let Ok(peer) = rest[2].parse::<PeerId>() else { continue };
            guard
                .entry(link)
                .or_default()
                .insert(author, Hlc { wall, logical, peer });
        }
    }

    /// `last_pulled` を atomic write でディスクに保存。 `cursor_path` 未設定なら no-op。
    /// 書式 (#216 v2): 1 行 1 エントリ、 `link author wall logical hlc_peer`
    /// (空白区切り)。 失敗しても sync は続行 (cursor は次回ロードで古いまま、
    /// multi-apply は LWW で吸収)。
    fn save_cursors(&self) {
        let path = match self.cursor_path.read().unwrap().clone() {
            Some(p) => p,
            None => return,
        };
        let guard = self.last_pulled.lock().unwrap();
        let mut buf = String::new();
        for (link, authors) in guard.iter() {
            for (author, h) in authors.iter() {
                buf.push_str(&format!(
                    "{} {} {} {} {}\n",
                    link, author, h.wall, h.logical, h.peer
                ));
            }
        }
        drop(guard);
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, buf).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    /// engine の WAL と `_sync_ops` を読んで HlcStore に LWW entry を再構築する。
    /// `Syncer::new` 内から呼ばれる。 Delete は sentinel (`u16::MAX`) で残す
    /// (= tombstone) ので、 後で来る古い HLC の Tie/Untie/Content が `apply_one`
    /// 内の tombstone check で skip される。
    ///
    /// #154: WAL だけでは足りない。 WAL ring は bridge 済み領域を fold する
    /// (`Engine::wal_fold_safe`) ので、 **fold された record の HLC は reopen 後の
    /// hydrate で復元されない**。 その状態で cursor を持たない caller が `Hlc::ZERO`
    /// から pull すると、 相手 ring の陳腐 record が「未知」と判定されて再 apply され、
    /// ローカルのより新しい行が巻き戻る (tombstone の記憶も消えるので削除済み entity の
    /// 復活にもなる)。 fold で WAL から消えた record は bridge 先の `_sync_ops`
    /// (永続) に残っているので、 そちらも歩く。
    /// **pre-v9 DB 専用の legacy 経路** (request17 step 6)。 v9 DB は版数を cell と
    /// 一緒に永続するので呼ばれない (`Syncer::new` の分岐を参照)。
    fn hydrate_hlc_store(&self, engine: &Engine) {
        let store = engine.hlc_store();
        // 1. WAL の生存範囲 (fold されていない分)
        if let Some(wal) = engine.oplog_arc() {
            for rec in wal.iter_committed() {
                Self::hydrate_one(engine, store, &rec);
            }
        }
        // 2. #154: bridge 先の `_sync_ops` (fold 済み record はここにしか残っていない)
        for payload in engine.pending_sync_ops(0) {
            if let Some(rec) = enchudb_oplog::oplog::decode_sync_ops_payload(&payload) {
                Self::hydrate_one(engine, store, &rec);
            }
        }
    }

    /// hydrate の 1 record 分。 2 つの source (WAL / `_sync_ops`) から呼ばれるので
    /// **monotonic max** (`try_set`) で merge する — 片方の古い entry が
    /// もう片方の新しい entry を潰さないため (旧実装は単一 source 前提の `force_set`)。
    ///
    /// #154: eid は必ず `resolve_remote_eid_existing` を通す。 `_sync_ops` の record は
    /// **逆写像で元 owner の世界番号に宛名が書き戻されている** (request10 / #76) ため、
    /// 生の eid を key にすると apply 側 (= local eid で lookup) と一致せず、
    /// hydrate したのに LWW が効かないという silent な取りこぼしになる。
    /// 写像は `.eidmap` で永続化されているので reopen 後も引ける。 写像が無い
    /// foreign eid は「local に一度も届いていない entity」なので hydrate 不要。
    fn hydrate_one(engine: &Engine, store: &HlcStore, rec: &enchudb_oplog::oplog::Record) {
        let Some(local_eid) = (match &rec.op {
            DecodedOp::Tie { eid, .. }
            | DecodedOp::Untie { eid, .. }
            | DecodedOp::Delete { eid }
            | DecodedOp::Content { eid, .. }
            | DecodedOp::TieNamed { eid, .. }
            | DecodedOp::TieLeaf { eid, .. }
            | DecodedOp::TieRef { eid, .. } => engine.resolve_remote_eid_existing(*eid),
            DecodedOp::Commit | DecodedOp::Vocab { .. } => None,
        }) else {
            return;
        };
        match &rec.op {
            DecodedOp::Tie { himo_id, .. }
            | DecodedOp::Untie { himo_id, .. }
            | DecodedOp::TieRef { himo_id, .. } => {
                store.try_set(local_eid, *himo_id, rec.hlc);
            }
            DecodedOp::Delete { .. } => {
                store.try_set(local_eid, u16::MAX, rec.hlc);
            }
            DecodedOp::Content { key, .. } => {
                let key_hash = enchudb_oplog::content_key_hash15(key);
                store.try_set(local_eid, key_hash | 0x8000, rec.hlc);
            }
            // 0.9.0 / 0.12.0 (#88): 名前を local hid に解決できる場合のみ LWW entry を
            // 張る (未定義 = local に一度も届いていない himo は hydrate 不要)。
            DecodedOp::TieNamed { himo_name, .. } | DecodedOp::TieLeaf { himo_name, .. } => {
                if let Some(hid) = engine.himo_id(himo_name) {
                    store.try_set(local_eid, hid as u16, rec.hlc);
                }
            }
            DecodedOp::Commit | DecodedOp::Vocab { .. } => {}
        }
    }

    /// Phase C: 署名検証を必須にする。未署名 or 検証失敗 op は reject される。
    pub fn set_require_signature(&self, on: bool) {
        self.require_signature.store(on, std::sync::atomic::Ordering::Release);
    }

    /// 0.7.0 (Phase 5): peer に対する initial sync 完了マーク。 user code が
    /// (1) transport.bootstrap_to 等で peer の snapshot を local に copy
    /// (2) その時点での peer の `current_sync_lsn()` を別 RPC / shake-hands で取得
    /// (3) 本 API で「ここまで配信済み」 を engine の `_sync_peers` に記録
    /// (4) 以降は通常の pull_once / publish_since で incremental sync
    ///
    /// 0.7.0 では transport wire の bootstrap response に lsn を入れる拡張は
    /// 入れていない (= example で「user が別経路で lsn を取る」 pattern を提示)。
    /// 0.8.0 で transport API を拡張して 1 行に纏める想定。
    pub fn mark_initial_sync_complete(&self, peer: PeerId, snapshot_lsn: u32) -> Result<(), String> {
        self.engine.ack_sync(peer, snapshot_lsn)
    }

    /// #140: 自 engine の live state を transport に配布登録する (author 側)。
    ///
    /// 登録後、 truncated puller は `bootstrap_pull` で現在状態を取得できる。
    /// provider は呼ばれるたびに `Engine::state_records` で合成する (常に最新)。
    /// state を運べない transport (default 実装) では no-op。
    pub fn serve_state(&self) {
        // Weak 必須: transport は peer より長生きする。 強参照で capture すると、
        // restart で drop したはずの engine (background consumer 込み) が provider の
        // 中で生き続け、 同一 DB file を再 open した新 engine と並走して sidecar
        // persist が衝突する (sunsu2 chaos の restart で実測)。
        let engine = Arc::downgrade(&self.engine);
        let self_peer = self.engine.peer_id();
        self.transport.register_state_provider(
            self_peer,
            Arc::new(move || {
                let eng = engine.upgrade()?;
                let (records, as_of) = eng.state_records();
                Some(enchudb_engine::transport::StateBatch { records, as_of, complete: true })
            }),
        );
    }

    /// #140: truncation からの復旧経路 — author の live state を取得して適用する。
    ///
    /// `pull_once` が `history_truncated` を返した peer に対して呼ぶ。
    /// 1. `Transport::fetch_state(from)` で StateBatch 取得 (None = 運べない transport
    ///    または author 未登録 → 復旧不能、 None を返す)
    /// 2. 通常の apply 経路 (LWW、 冪等) で適用 + durable barrier
    /// 3. **ghost sweep**: 自 store に居る author の行のうち state に現れなかった
    ///    entity を削除 — author の現在状態に無い = author 側で削除済み。 truncated
    ///    期間に tombstone を取り逃した亡霊 (#140 の原症状) をここで吸収する。
    ///    sweep の Delete は自分の oplog にも載って伝播するが、 対象は「author が
    ///    既に消した行」なので LWW 的に正しい方向にしか働かない
    /// 4. cursor を `as_of` へ前進 (後退はさせない) — 以降は通常の差分 pull
    pub fn bootstrap_pull(&self, from: PeerId) -> Option<BootstrapOutcome> {
        let batch = self.transport.fetch_state(from)?;
        let gossip = self.engine.gossip_remote_apply();
        let mut relay_accepted: Vec<usize> = Vec::new();
        let outcome =
            self.apply_records_impl(&batch.records, gossip.then_some(&mut relay_accepted));

        // #209: relay が bootstrap で取った state も原型のまま relay stream へ —
        // これで新規 relay も author の複製 + 配布点として立ち上がれる。
        if gossip {
            for &i in &relay_accepted {
                let _ = self.engine.relay_record(&batch.records[i]);
            }
        }

        // pull_once と同じ barrier: 派生 state (eidmap / vocabmap / tables) が
        // durable になる前に cursor を進めない。
        if outcome.applied > 0 && !self.persist_applied_state(from) {
            return Some(BootstrapOutcome { outcome, swept: 0, as_of: batch.as_of });
        }

        let mut swept = 0usize;
        if batch.complete {
            let covered: std::collections::HashSet<u32> = batch
                .records
                .iter()
                .filter_map(|r| match &r.op {
                    DecodedOp::Tie { eid, .. }
                    | DecodedOp::Untie { eid, .. }
                    | DecodedOp::Delete { eid }
                    | DecodedOp::Content { eid, .. }
                    | DecodedOp::TieNamed { eid, .. }
                    | DecodedOp::TieLeaf { eid, .. }
                    | DecodedOp::TieRef { eid, .. } => Some(enchudb_oplog::eid_local(*eid)),
                    DecodedOp::Commit | DecodedOp::Vocab { .. } => None,
                })
                .collect();
            for (foreign_local, local) in self.engine.translated_locals_of(from) {
                if !covered.contains(&foreign_local) {
                    self.engine.delete(local as u64);
                    swept += 1;
                }
            }
        }

        // cursor := max(現行, as_of)。 進んだら永続 + pull-as-ack (#149) も記録。
        // #216: StateBatch は author (= from) 単一なので author=from の entry のみ
        // 前進させる。 ack は link の author 別 cursor の min (保守側 — 「hlc ≤ ack
        // は全 author 分消化済み」の証明として健全)。
        let self_peer = self.engine.peer_id();
        let (advanced, ack) = {
            let mut guard = self.last_pulled.lock().unwrap();
            let link = guard.entry(from).or_default();
            let cur = link.entry(from).or_insert(Hlc::ZERO);
            let advanced = if batch.as_of > *cur {
                *cur = batch.as_of;
                true
            } else {
                false
            };
            let ack = link.values().min().copied().unwrap_or(Hlc::ZERO);
            (advanced, ack)
        };
        if advanced {
            self.save_cursors();
            self.transport.record_pull_ack(from, self_peer, ack);
        }

        Some(BootstrapOutcome { outcome, swept, as_of: batch.as_of })
    }

    /// 指定 peer から未取得レコードを 1 回 pull して本体に apply。
    /// request4: `pull_as(self_peer, from, since)` 経由で broadcast log +
    /// (from, self_peer) targeted log を両方拾う。 partial sync 対応 transport
    /// (InMemoryTransport 等) では targeted 経由の per-peer record も受信できる。
    pub fn pull_once(&self, from: PeerId) -> SyncOutcome {
        // #216: cursor は link × author の vector — relay の stream は複数 author の
        // merge で HLC 非単調なので、 scalar cursor は relay された古い HLC の
        // record を永久に落とす (silent data loss)。 author substream は単調なので
        // author 粒度の cursor が健全。
        let since: Vec<(PeerId, Hlc)> = {
            let guard = self.last_pulled.lock().unwrap();
            guard
                .get(&from)
                .map(|m| m.iter().map(|(a, h)| (*a, *h)).collect())
                .unwrap_or_default()
        };
        let self_peer = self.engine.peer_id();
        let floor = self.transport.history_floor(from);

        // #140: publisher が広告した履歴の下限より自分の cursor が古いなら、 差分では
        // 埋められない穴がある。 部分履歴をそのまま適用すると store が黙って不完全に
        // なる (削除 record を取りこぼせば削除済み entity が復活する) ので、 **何も
        // 適用せず** truncation を通知して caller に bootstrap を促す。
        //
        // cursor が floor 以上なら、 落ちた分は既に自分が consume 済みなので安全。
        // #216: 判定は author 別 cursor の min (保守側 — floor は publisher ring
        // 全体の scalar で、 どの author の履歴が落ちたかまでは語らない)。
        let link_min = since.iter().map(|(_, h)| *h).min().unwrap_or(Hlc::ZERO);
        if let Some(floor) = floor {
            if link_min < floor {
                return SyncOutcome { history_truncated: true, ..SyncOutcome::default() };
            }
        }

        let fetched = self.transport.pull_as_multi(self_peer, from, &since);
        // #216: default 実装 transport (全量 fetch fallback) 向けの受信側 filter。
        // per-author filter 済み transport (InMemory) では素通り。
        let cursor_of = |author: PeerId| {
            since
                .iter()
                .find(|(p, _)| *p == author)
                .map(|(_, h)| *h)
                .unwrap_or(Hlc::ZERO)
        };
        let records: Vec<WireRecord> = fetched
            .into_iter()
            .filter(|r| r.hlc > cursor_of(r.author_peer))
            .collect();

        // #216: publisher が reclaim 済み (floor あり) の link に **cursor 未登録の
        // 新 author** が現れた場合、 その author の古い record は既に ring から
        // 落ちている可能性がある (scalar floor では author 別に判別できない)。
        // 適用と cursor 前進は行った上で truncation を通知し、 app に当該 author の
        // bootstrap を促す — 次回以降は author が map に載るので clean diff に戻る。
        let new_author_on_reclaimed_link = floor.is_some()
            && records
                .iter()
                .any(|r| !since.iter().any(|(p, _)| *p == r.author_peer));
        let gossip = self.engine.gossip_remote_apply();
        let mut relay_accepted: Vec<usize> = Vec::new();
        let mut outcome =
            self.apply_records_impl(&records, gossip.then_some(&mut relay_accepted));
        outcome.history_truncated |= new_author_on_reclaimed_link;

        // #209: relay (gossip) — accept した record を**原型のまま** (原 eid / 原
        // value / 原 HLC / 原署名) 自分の WAL へ。 翻訳後の姿を relay すると
        // direct 経路との混在で row 重複 / vocab 写像汚染になる。 append は
        // barrier より前: crash で append が飛んでも cursor 未前進なら再 pull で
        // 再適用+再判定される。 既知の狭い窓 (apply 済み・append 前の crash で
        // 当該 record が relay stream から漏れる) は #209 に記録済み — 下流の
        // 回収路は author 直 bootstrap。
        if gossip {
            for &i in &relay_accepted {
                let _ = self.engine.relay_record(&records[i]);
            }
        }

        // **cursor は、 それが消費した state より先に durable になってはいけない。**
        //
        // 適用は cell (mmap) のほかに 3 つの派生 state を動かす: `.tables` の
        // next_local、 `.eidmap` の entity 写像、 `.vocabmap` の text 写像。 後ろ 2 つは
        // memory から消えると復元手段が無い (受信 op は自分の WAL に残らない)。
        //
        // 先に cursor を落とすと「cursor は消費済みと言うが写像は無い」が確定し、
        // 差分 pull では**二度と埋まらない** (cursor が越えているので当該 record は
        // 再配送されない)。 実地では text cell に無関係な文字列が入り、 相手が消した
        // 行がこちらで生き残った。
        //
        // caller 側では直せない — `pull_once` が return した時点で cursor は既に
        // 落ちているため。 だから barrier はここに置く。 失敗したら cursor を進めず、
        // 次の pull で同じ record を再適用する (apply は冪等: LWW と `get_or_insert`)。
        //
        // 条件が `applied > 0` なのは、 何も書いていない pull で fsync しないため。
        // 「写像だけ確保して drop した」 record (例: ref の target が引けない Tie) は
        // 未永続の slot を残しうるが、 その slot には何も書かれていないので crash で
        // 失われても **leak であって破損ではない** (次に同じ entity が来たら別 slot に
        // 張り直る)。
        if outcome.applied > 0 && !self.persist_applied_state(from) {
            return outcome;
        }

        // last_pulled を進める(空 pull でも既存のままで OK)。 進んだら disk に保存。
        // #78 (0.9.0): reject (署名/ACL) された record を cursor が越えない。
        // 旧実装は reject 込みで max HLC まで前進し、 pubkey 登録との race 窓の
        // record が永久に再配送されない silent gap を作っていた。 reject があった
        // 場合は「最小 reject HLC 未満の accepted record」までしか進めない
        // (= reject 分は次回 pull で再配送・再検証される)。
        // #216: 前進は author 別。 minrej は batch 全体の scalar を全 author に
        // 適用する (author を跨いで保守側に倒すだけ — reject は稀な例外経路)。
        let mut advanced = false;
        {
            let mut guard = self.last_pulled.lock().unwrap();
            let link = guard.entry(from).or_default();
            for r in &records {
                if let Some(minrej) = outcome.min_rejected_hlc {
                    if r.hlc >= minrej {
                        continue;
                    }
                }
                let cur = link.entry(r.author_peer).or_insert(Hlc::ZERO);
                if r.hlc > *cur {
                    *cur = r.hlc;
                    advanced = true;
                }
            }
        }
        if advanced {
            self.save_cursors();
        }

        // #149: 確定 cursor を pull-as-ack として transport に記録する。 cursor は
        // 上の durable barrier を通過した後にしか前進しないので、 そのまま
        // 「ここまで消化済み」の到達証明として使える。 truncation / persist 失敗の
        // early return はここに来ない (= 未消化の cursor を ack しない)。
        // 空 pull でも既存 cursor を再記録する — author 側が ack 状態を失っても、
        // pull が回っている限り watermark が自然回復する。
        //
        // #216: ack は author 別 cursor の **min** — 「hlc ≤ ack は全 author 分
        // 消化済み」の保守的な証明。 既知の狭い race: link に新 author が現れる
        // 直前の ack はその author の未消化分を覆えない (transport は max 保持)。
        // publisher がその ack で当該 record を reclaim すると、 上の
        // new-author-on-reclaimed-link 検知が truncation として拾い、 bootstrap で
        // 回収する — silent にはならない。
        let cursor = {
            let guard = self.last_pulled.lock().unwrap();
            guard
                .get(&from)
                .and_then(|m| m.values().min().copied())
                .unwrap_or(Hlc::ZERO)
        };
        if cursor > Hlc::ZERO {
            self.transport.record_pull_ack(from, self_peer, cursor);
        }

        outcome
    }

    /// 適用した state を durable にする。 失敗したら `false` (= cursor を進めない)。
    ///
    /// 進めないだけで data は memory 上に載っているので、 次の pull で同じ record が
    /// 再配送され再適用される (apply は冪等)。 永続できない状態が続く限り cursor は
    /// 止まったままで、 これは 「黙って先へ進んで欠落を作る」 より望ましい。
    fn persist_applied_state(&self, from: PeerId) -> bool {
        match self.engine.persist_sync_state() {
            Ok(()) => true,
            Err(e) => {
                eprintln!(
                    "[enchudb] sync: persist_sync_state failed ({}), holding pull cursor for \
                     peer {} — the same records will be re-applied on the next pull.",
                    e, from
                );
                false
            }
        }
    }

    /// request4: subscription filter を差し替える (起動時 1 度設定する想定)。
    /// default は `AllRecords` (全送り、 旧 `publish_since` の挙動 = SaaS 用)。
    /// SNS partial sync の caller は自前 struct で `impl SubscriptionFilter` する。
    pub fn set_subscription_filter(&self, filter: Arc<dyn SubscriptionFilter>) {
        *self.subscription_filter.write().unwrap() = filter;
    }

    /// #149: transport に溜まった pull ack を消化し、 ring に圧力があれば reclaim を回す。
    ///
    /// puller が `pull_once` で記録した cursor (HLC) を `ack_sync_up_to_hlc` で
    /// `_sync_peers.consumed_lsn` に写す (= watermark は常に最新)。 ただし
    /// **reclaim は `_sync_ops` の使用率が 50% を超えている時だけ**呼ぶ。
    ///
    /// 履歴は容量が許す限り保持するのが正しい: reclaim すると floor が上がり、
    /// **その author をまだ一度も pull していない follower** (作成直後 / 長期 offline)
    /// は cursor < floor で truncation 行きになる。 bootstrap (#140) が受け皿だが、
    /// 差分で追いつけるならその方が安いので、 reclaim は「容量管理」であって
    /// 「消化済みの掃除」ではない。 eager に回すと、 round 1 個ずれて参加した
    /// follower が即 bootstrap 送りになる (sunsu2 Phase 2 chaos で実測)。
    ///
    /// `publish_since` の冒頭から自動で呼ばれるので、 app は publish/pull を
    /// 回すだけで良い。 publish しない caller (pull 専用 hub 等) が明示的に
    /// 回せるよう pub。 戻り値は consumed_lsn が前進した peer 数。
    pub fn absorb_pull_acks(&self) -> usize {
        let self_peer = self.engine.peer_id();
        let acks = self.transport.take_pull_acks(self_peer);
        if acks.is_empty() {
            return 0;
        }
        let mut advanced = 0usize;
        for (by, cursor) in acks {
            // self row は sync_watermark から除外されるが、 そもそも作らない
            if by == self_peer {
                continue;
            }
            if let Ok(lsn) = self.engine.ack_sync_up_to_hlc(by, cursor)
                && lsn > 0
            {
                advanced += 1;
            }
        }
        if advanced > 0
            && let Some(usage) = self.engine.table_eid_usage("_sync_ops")
            && usage.live * 2 >= usage.capacity
        {
            self.engine.reclaim_sync_ops();
        }
        advanced
    }

    /// 自 peer の commit 済み ops を transport に publish
    /// (source は `_sync_ops` ring — [`Syncer::collect_records_since`] 参照)。
    /// 戻り値は publish したレコード数 (重複カウントしない、 最終 broadcast/peer 別
    /// のいずれか単一経路で配信した数)。
    ///
    /// request4: transport が `known_peers()` を返すなら **per-peer 経路** (=
    /// `publish_since_for_peer` を全 peer に対して呼ぶ) で配信。 known_peers が
    /// 空なら **broadcast 経路** (= 旧 `publish_since` の挙動) にフォールバック。
    /// = 既存 caller (broadcast 前提) は API 不変で動く。
    pub fn publish_since(&self, since: Hlc) -> usize {
        // #149: publish の beat で pull ack を消化して reclaim を回す。 absorb →
        // advertise の順序が要: reclaim で上がった floor を同じ beat で広告する。
        self.absorb_pull_acks();
        self.advertise_history_floor();
        let peers = self.transport.known_peers();
        if peers.is_empty() {
            // backward compat: known_peers 未実装 transport (HTTP/WS push 等) は
            // 旧 broadcast 経路。 filter は無視される。
            let filtered = self.collect_records_since(since);
            let count = filtered.len();
            let self_peer = self.engine.peer_id();
            self.transport.publish(self_peer, filtered);
            return count;
        }
        let self_peer = self.engine.peer_id();
        let mut total = 0usize;
        for p in peers {
            if p == self_peer { continue; } // 自分には送らない
            total += self.publish_since_for_peer(p, since);
        }
        total
    }

    /// #140: 自分の `_sync_ops` が reclaim 済みなら、 配れる履歴の下限を transport に広告する。
    ///
    /// #191: 下限は「reclaim で消えた record の最大 HLC」。 cursor >= floor の puller は
    /// 消えた分を全部消化済みなので差分 pull を続けて良い。 cursor < floor の puller だけが
    /// 差分で埋められない穴を持つ = bootstrap 対象。 reclaim が起きていなければ
    /// 何も広告しない (= 全履歴が配れる)。
    fn advertise_history_floor(&self) {
        // #191: 以前の「生存 record の最小 HLC (空なら Hlc::MAX)」は、 消化完了直後の
        // 正常な cursor (max_reclaimed <= cursor < min_alive) まで gap と誤認し、
        // reclaim 1 回で既追従 follower 全員が bootstrap 行きになっていた。
        if let Some(floor) = self.engine.sync_reclaimed_floor() {
            self.transport.set_history_floor(self.engine.peer_id(), floor);
            return;
        }
        // fallback: fix 前に reclaim してから reopen した既存 DB (floor 未記録)。
        // 正確な下限は失われているので過大側 (min alive / MAX) に倒す —
        // silent gap より余分な bootstrap 誘導の方が安全。
        if !self.engine.sync_history_reclaimed() {
            return;
        }
        let floor = self
            .collect_records_since(Hlc::ZERO)
            .into_iter()
            .map(|r| r.hlc)
            .min();
        let floor = floor.unwrap_or(Hlc { wall: u64::MAX, logical: u32::MAX, peer: u32::MAX });
        self.transport.set_history_floor(self.engine.peer_id(), floor);
    }

    /// 0.8.0: `since` HLC より新しい WireRecord を集める。 `_sync_ops` 経由
    /// (= publish の primary source、 legacy oplog iter fallback は 0.8.0 で
    /// 撤去、 `Syncer::new` で `sync_tables_enabled` チェック済み)。
    fn collect_records_since(&self, since: Hlc) -> Vec<WireRecord> {
        // #216: relay (gossip) の ring は HLC 非単調 — relayed record が原 HLC の
        // まま後から乗るので、 scalar since での間引きは relayed 分を落とす。
        // gossip 有効時は常に全量 collect し、 重複は transport 側の dedupe
        // ((peer, hlc) 一意) に任せる。
        let since = if self.engine.gossip_remote_apply() { Hlc::ZERO } else { since };
        let payloads = self.engine.pending_sync_ops(0);
        let mut out = Vec::with_capacity(payloads.len());
        for p in &payloads {
            let Some(rec) = enchudb_oplog::oplog::decode_sync_ops_payload(p) else {
                continue;
            };
            if rec.hlc > since {
                out.push(WireRecord::from(rec));
            }
        }
        out
    }

    /// request4: `target_peer` 限定で publish。 `SubscriptionFilter::should_send`
    /// で per-peer に絞った record のみを `transport.publish_to(self_peer,
    /// target_peer, ...)` で送る。 戻り値は実際に送った record 数。
    ///
    /// SaaS の full sync (= AllRecords filter) では `since` フィルタ後の全 record
    /// を target に送る (= 旧 broadcast 経路の挙動を per-peer 化したもの)。
    /// SNS partial sync では `SubscriptionFilter::should_send` で「target が
    /// 関心ある record か」 を判定してから送る。
    pub fn publish_since_for_peer(&self, target_peer: PeerId, since: Hlc) -> usize {
        let self_peer = self.engine.peer_id();
        let filter = self.subscription_filter.read().unwrap().clone();
        let filtered: Vec<WireRecord> = self.collect_records_since(since)
            .into_iter()
            .filter(|r| filter.should_send(target_peer, r))
            .collect();
        let count = filtered.len();
        self.transport.publish_to(self_peer, target_peer, filtered);
        count
    }

    /// 受信レコードを LWW で apply する。Phase C: 署名検証 + ACL も通す。
    /// WS push client などの外部から呼び出すために public。
    /// #141: apply 本体の前に走る **PK bind pass**。
    ///
    /// `resolve_remote_eid` は `(author, remote_eid)` 写像しか見ないので、 初見の
    /// foreign entity には無条件で新規 local eid を払い出す。 その結果、 2 台が同じ
    /// 自然キーの row を **独立に** 作ってから相互 sync すると、 同一 PK の entity が
    /// author ごとに二重化し、 恒久チャーンループ → WAL 膨張 → oplog リング一周
    /// (#140 の tombstone 消失) まで連鎖する。
    ///
    /// そこで apply 前に batch を走査し、 PK himo への Tie を見つけたら
    /// 「その PK 値を既に持つ local entity」を引いて、 写像をそこへ固定する
    /// (`bind_remote_eid`)。 以降の `resolve_remote_eid` は払い出しではなくその
    /// entity を返すので、 LWW は himo 単位で通常どおり効いて 1 row に収束する。
    ///
    /// **既知の制約**: bind できるのは PK Tie が batch に含まれる場合だけ。 ある row の
    /// PK Tie と非 PK Tie が別 batch に分かれ、 かつ非 PK 側が先に届くと、 その時点で
    /// 新規 eid が払い出されて束ね損なう。 1 row の insert は 1 commit = 同 batch に
    /// 載るのが通常なので実運用の主経路は塞がるが、 完全ではない (既に二重化した
    /// store の修復も別途必要 — #141 参照)。
    fn bind_by_primary_key(&self, records: &[WireRecord]) {
        // PK Tie を含まない batch では rebuild も lookup もしない (hot path 保護)。
        let has_pk_tie = records.iter().any(|rec| {
            matches!(&rec.op, DecodedOp::Tie { himo_id, .. } if self.engine.is_pk_himo(*himo_id))
        });
        if !has_pk_tie {
            return;
        }
        // query_by_id は cylinder index 経由なので、 直前の tie を見えるようにする
        // (schema 層の upsert が PK lookup 前に rebuild するのと同じ理由)。
        self.engine.rebuild();

        for rec in records {
            let DecodedOp::Tie { eid, himo_id, value } = &rec.op else { continue };
            if !self.engine.is_pk_himo(*himo_id) {
                continue;
            }
            // 既に写像がある foreign entity は触らない (先に確定した束ね先を優先)。
            if self.engine.resolve_remote_eid_existing(*eid).is_some() {
                continue;
            }
            // PK は Tag / Number 想定 (Ref を PK にはしない)。 remote vocab vid を
            // local vid に翻訳してから引く。 **未翻訳の生 vid で引いてはいけない** —
            // vid は author ローカル番号で、 fresh store 同士は intern 順が対称なので
            // ほぼ必ず数値衝突し、 無関係な row へ誤 bind → 上書き → 恒久チャーンに
            // なる (0.17.0 リグレッション。 新規文字列の Vocab record は同 batch 内で
            // 未適用のため translate_remote_vid の fallback 生値が漏れていた)。
            let local_value = match self
                .engine
                .try_translate_remote_vid(rec.author_peer, *himo_id, *value)
            {
                Some(v) => v,
                None => {
                    // mapping 未登録 = この vid の Vocab record は同 batch 内に居る
                    // はず。 その bytes で local vocab を引き、 同じ文字列が local に
                    // 既存ならその vid で照合する。 bytes ごと未知なら、 この PK
                    // 文字列を持つ既存 row は存在し得ない → bind せず払い出しに任せる。
                    let from_batch = records.iter().find_map(|r| match &r.op {
                        DecodedOp::Vocab { vid, bytes }
                            if r.author_peer == rec.author_peer && vid == value =>
                        {
                            self.engine.vocab_id_bytes(bytes)
                        }
                        _ => None,
                    });
                    match from_batch {
                        Some(v) => v,
                        None => continue,
                    }
                }
            };
            let Some(existing) = self
                .engine
                .query_by_id(&[(*himo_id, local_value)])
                .into_iter()
                .next()
            else {
                continue; // 同じ PK の row はまだ無い → 通常の払い出しに任せる
            };
            self.engine.bind_remote_eid(rec.author_peer, *eid, existing);
        }
    }

    /// 受信 record 列を適用する。 `pull_once` の中身であり、 transport を介さず
    /// record を直接持っている caller (WS push など) の入口でもある。
    ///
    /// **注意**: 自前で cursor を持つ caller は、 cursor を永続する
    /// 前に [`Engine::persist_sync_state`] を呼ぶこと。 適用が作った写像
    /// (`.eidmap` / `.vocabmap`) より先に cursor が durable になると、 その間の
    /// crash で「消費済みと言うが写像は無い」が確定し、 差分 pull では埋まらない。
    /// `pull_once` はこの順序を内側で守っている。
    pub fn apply_records(&self, records: &[WireRecord]) -> SyncOutcome {
        self.apply_records_impl(records, None)
    }

    /// #209: `relay_accepted` が Some なら、 relay (gossip) すべき record の index を
    /// 集める: **apply が accept (Applied) したものだけ** — LWW gate が cyclic
    /// topology の echo を止める栓なので、 skip した record を relay してはいけない。
    /// Commit (dedupe identity なし) と自 author の record (発信元に戻ってきた echo)
    /// も除外する。
    fn apply_records_impl(
        &self,
        records: &[WireRecord],
        mut relay_accepted: Option<&mut Vec<usize>>,
    ) -> SyncOutcome {
        let mut out = SyncOutcome::default();
        // #9 foot-gun ガード: self_peer 未設定 (= 0) で foreign record を apply すると、
        // author 0 == self 0 が `resolve_remote_eid` の identity 分岐に落ち、 翻訳されず
        // #9 の衝突 (自分の entity をサイレント上書き) が再発する。 sync には必ず非 0 の
        // peer_id が要る。 一度だけ loud に警告する。
        if !records.is_empty()
            && self.engine.peer_id() == 0
            && !self
                .warned_unconfigured_peer
                .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            eprintln!(
                "warning: Syncer applying foreign records with peer_id == 0; call \
                 Engine::set_peer_id(<non-zero>) before sync or foreign entities can \
                 collide with local ones (#9)."
            );
        }
        self.bind_by_primary_key(records);
        let store = self.engine.hlc_store().clone();
        let require_sig = self.require_signature.load(std::sync::atomic::Ordering::Acquire);
        let pubkeys = self.engine.pubkeys().clone();
        let acl = self.engine.acl().clone();
        let note_reject = |out: &mut SyncOutcome, hlc: Hlc| {
            if out.min_rejected_hlc.map_or(true, |m| hlc < m) {
                out.min_rejected_hlc = Some(hlc);
            }
        };
        let self_peer = self.engine.peer_id();
        for (idx, rec) in records.iter().enumerate() {
            out.received += 1;

            // ACL チェック(未定義なら全員通す)
            if !acl.is_writer(rec.author_peer) {
                out.rejected_acl += 1;
                note_reject(&mut out, rec.hlc);
                continue;
            }

            if require_sig {
                if rec.signature == [0u8; 64] {
                    out.rejected_signature += 1;
                    note_reject(&mut out, rec.hlc);
                    continue;
                }
                if pubkeys.get(rec.author_peer).is_none() {
                    out.rejected_signature += 1;
                    note_reject(&mut out, rec.hlc);
                    continue;
                }
                if !pubkeys.verify(rec.author_peer, &rec.signed_bytes, &rec.signature) {
                    out.rejected_signature += 1;
                    note_reject(&mut out, rec.hlc);
                    continue;
                }
            }
            match self.apply_one(&store, rec) {
                ApplyResult::Applied => {
                    out.applied += 1;
                    if let Some(list) = relay_accepted.as_deref_mut() {
                        let relayable = !matches!(rec.op, DecodedOp::Commit)
                            && rec.author_peer != self_peer;
                        if relayable {
                            list.push(idx);
                        }
                    }
                }
                ApplyResult::SkippedOlder => out.skipped += 1,
                ApplyResult::Dropped => out.dropped_unresolved += 1,
                ApplyResult::DroppedVocab => out.dropped_vocab += 1,
                ApplyResult::RejectedCapacity => {
                    // #210: 「今は容量が無い」 だけなので cursor を止めて再配送させる。
                    out.rejected_capacity += 1;
                    note_reject(&mut out, rec.hlc);
                }
            }
        }
        out
    }

    // 0.9.0: 旧 Content reorder buffer (`buffer_pending` / `drain_pending`) は削除。
    // content は TieNamed で運ばれ、 TieNamed 自身が entity 写像を作れるため
    // 「Tie より先に届いた Content の退避」という問題が構造的に消えた。
    // (旧 buffer の既知バグ: in-memory のみで cursor だけ永続前進 → 再起動で
    //  buffered record が恒久喪失 / eviction が「最古」でなく任意 bucket を破棄)

    fn apply_one(&self, store: &HlcStore, rec: &WireRecord) -> ApplyResult {
        match &rec.op {
            DecodedOp::Tie { eid, himo_id, value } => {
                // #9: foreign eid を自分の eid 空間の local eid に翻訳 (初見なら払い出し)。
                // 以降の LWW bookkeeping (HlcStore) と body apply を全て local_eid で行う。
                // NOTE(#9): gossip_remote_apply が ON のとき remote_tie_apply は local_eid
                // で relayed append する。 gossip 転送の正しさには元の foreign eid で
                // append すべきで、 別 commit で body-eid / relay-eid を分離する。 現状
                // gossip は default off。
                // table-less (himo を closed table に解決できない) なら確保先が無いので
                // skip。 entity / ref value を先に解決してから LWW を更新する。
                let local_eid = match self.engine.resolve_remote_eid(*eid, *himo_id) {
                    Some(e) => e,
                    None => return ApplyResult::Dropped,
                };
                // #9: entity 写像ができたので、 この foreign entity 宛に Tie より先に届いて
                // 退避していた Content を drain して apply する (= 配送順序ロス防止)。
                // #9: Ref himo は value 自体が foreign target eid なので、 ref の target
                // table 空間の local eid に翻訳 (確保できなければ skip)。 それ以外
                // (Tag/Symbol) は remote vocab vid を local vid に変換 (Number は identity)。
                let value = if self.engine.himo_is_ref(*himo_id) {
                    match self.engine.resolve_remote_ref_value(rec.author_peer, *value, *himo_id) {
                        Some(v) => v,
                        None => return ApplyResult::Dropped,
                    }
                } else {
                    // 未翻訳の生値を書くと**無関係な local 文字列**が cell に入る
                    // (`SyncOutcome::dropped_vocab` 参照)。 書かずに捨てる。
                    match self.engine.try_translate_remote_vid(rec.author_peer, *himo_id, *value) {
                        Some(v) => v,
                        None => return ApplyResult::DroppedVocab,
                    }
                };
                // request17 step 5: LWW / tombstone の判定は engine (`set_cell`) の
                // 内側だけ。 ここで判定して別関数で適用する形は、 呼び忘れれば黙って
                // 壊れる (実際 ローカル write 経路がそうなっていた = #154/#160 の根)。
                ApplyResult::from_lww(self.engine.remote_tie_apply(local_eid, *himo_id, value, rec.hlc))
            }
            DecodedOp::TieRef { eid, himo_id, target } => {
                // #183: Ref 値の target を**世界番号 (u64) 同乗**で運ぶ Tie。author の
                // bridge が「translated foreign entity への Ref write」を書き換えた形。
                // 行 eid は通常の翻訳、target は「産みの親」key (0.11 semantics =
                // eid_peer が author) で ref target table 空間の local eid へ翻訳する。
                // 自分が target の産みの親なら identity、第三者 peer なら target 自身の
                // Tie と同じ写像に収束する (resolve_remote_ref_value の key 空間共有)。
                let local_eid = match self.engine.resolve_remote_eid(*eid, *himo_id) {
                    Some(e) => e,
                    None => return ApplyResult::Dropped,
                };
                let value = match self.engine.resolve_remote_ref_value(
                    enchudb_oplog::eid_peer(*target),
                    enchudb_oplog::eid_local(*target),
                    *himo_id,
                ) {
                    Some(v) => v,
                    None => return ApplyResult::Dropped,
                };
                ApplyResult::from_lww(self.engine.remote_tie_apply(local_eid, *himo_id, value, rec.hlc))
            }
            DecodedOp::Untie { eid, himo_id } => {
                // #9: foreign eid を翻訳 (table-less なら確保先が無いので skip)。
                let local_eid = match self.engine.resolve_remote_eid(*eid, *himo_id) {
                    Some(e) => e,
                    None => return ApplyResult::Dropped,
                };
                // #9: 写像ができたので退避中の Content を drain。
                ApplyResult::from_lww(self.engine.remote_untie_apply(local_eid, *himo_id, rec.hlc))
            }
            DecodedOp::Delete { eid } => {
                // #9: Delete は himo を持たず table を導けないので既存の翻訳のみ引く。
                // 未登録 (= ここに一度も sync されてない foreign entity) なら消す対象が
                // 無いので skip。
                let local_eid = match self.engine.resolve_remote_eid_existing(*eid) {
                    Some(e) => e,
                    None => return ApplyResult::Dropped,
                };
                // Delete は全 himo に波及。 tombstone 版数は engine が
                // (v9 なら tombstone column に永続で) 記録する。 後続の古い HLC の
                // Tie/Untie/Content は engine 側の tombstone 判定で skip され、
                // 削除済み entity が復活しない。
                ApplyResult::from_lww(self.engine.remote_delete_apply(local_eid, rec.hlc))
            }
            DecodedOp::TieNamed { eid, himo_name, himo_kind, value } => {
                // 0.9.0: 動的 himo (content 互換層の `_c_{key}`) は id が peer 間で
                // 揃わないため名前で解決する。 受信側に未定義なら lazy 定義 —
                // これで Content 専用の reorder buffer / key hash が不要になる
                // (mapping は Tie と同じ resolve_remote_eid で作られる)。
                let local_hid = match self.engine.ensure_himo_named(himo_name, *himo_kind) {
                    Ok(h) => h,
                    Err(_) => return ApplyResult::Dropped, // himo 予算枯渇等 — 適用不能
                };
                let local_eid = match self.engine.resolve_remote_eid(*eid, local_hid) {
                    Some(e) => e,
                    None => return ApplyResult::Dropped,
                };
                // 値は author-local vid → local vid に変換 (Leaf/Tag のみ、 Number は identity)。
                // Tie と同じく、 翻訳できないなら書かない (`SyncOutcome::dropped_vocab`)。
                let value = match self.engine.try_translate_remote_vid(rec.author_peer, local_hid, *value) {
                    Some(v) => v,
                    None => return ApplyResult::DroppedVocab,
                };
                ApplyResult::from_lww(self.engine.remote_tie_apply(local_eid, local_hid, value, rec.hlc))
            }
            DecodedOp::TieLeaf { eid, himo_name, himo_kind, bytes } => {
                // 0.12.0 (#88): Leaf payload を bytes 同乗で受信。 名前で himo 解決 →
                // eid 翻訳 → LWW → LeafStore.insert + cell set (vid mapping 不要)。
                let local_hid = match self.engine.ensure_himo_named(himo_name, *himo_kind) {
                    Ok(h) => h,
                    Err(_) => return ApplyResult::Dropped,
                };
                let local_eid = match self.engine.resolve_remote_eid(*eid, local_hid) {
                    Some(e) => e,
                    None => return ApplyResult::Dropped,
                };
                ApplyResult::from(self.engine.remote_tieleaf_apply(local_eid, local_hid, bytes, rec.hlc))
            }
            DecodedOp::Content { eid, key, data } => {
                // legacy (pre-0.9): 0.9.0 以降は content が TieNamed で運ばれるため、
                // この arm はアップグレード移行期の旧 WAL 残渣にのみ到達する。
                // 旧実装が持っていた reorder buffer (未着 Tie 待ち退避) は TieNamed が
                // 自力で写像を作ることで構造的に不要になったため削除 — 写像が無い
                // legacy Content は skip (= 一度も Tie されない entity 宛で、 旧経路
                // でも実質死んでいたデータ)。
                let local_eid = match self.engine.resolve_remote_eid_existing(*eid) {
                    Some(e) => e,
                    None => return ApplyResult::Dropped,
                };
                // legacy Content は cell を持たないので key 単位の LWW だけここに残す
                // (tombstone 判定は engine 側 `remote_content_apply` が行う)。
                let slot = enchudb_oplog::content_key_hash15(key) | 0x8000;
                // #210: HLC の記録は **apply が成功してから**。 先に `try_set` すると、
                // 容量拒否で 1 byte も書けなかった record の HLC だけが残り、 空きが
                // 出た後の再配送が 「新しくない」 と judge されて永久に入らなくなる。
                if store.get(local_eid, slot).is_some_and(|cur| rec.hlc <= cur) {
                    return ApplyResult::SkippedOlder;
                }
                let r = ApplyResult::from(self.engine.remote_content_apply(local_eid, key, data, rec.hlc));
                if r == ApplyResult::Applied {
                    store.try_set(local_eid, slot, rec.hlc);
                }
                r
            }
            DecodedOp::Commit => ApplyResult::Applied, // boundary marker、apply は不要
            DecodedOp::Vocab { vid, bytes } => {
                // 0.8.4 issue #30: 既に同 (author_peer, vid, bytes) を登録済みなら
                // skip。 これが無いと gossip_remote_apply ON で同じ vocab record が
                // 再 apply され続け、 caller (Syncer) の applied counter が永久に
                // 0 に戻らず amplification loop の見かけになる。
                if self.engine.has_remote_vocab(rec.author_peer, *vid, bytes) {
                    return ApplyResult::SkippedOlder;
                }
                // author_peer の (vid, bytes) を受信。
                // Engine 側の remote_vocab_apply に委譲 (peer 別 vid mapping を構築)。
                self.engine.remote_vocab_apply(rec.author_peer, *vid, bytes);
                ApplyResult::Applied
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enchudb_engine::{ValueType};
    use enchudb_oplog::PeerId;
    use enchudb_engine::transport::InMemoryTransport;

    /// 固定 path だと並列 test run (別 binary / 前回 run の残骸) と衝突するため pid を混ぜる
    fn test_path(name: &str) -> String {
        format!("/tmp/enchudb_sync_{}_{}.db", name, std::process::id())
    }

    fn new_eng(path: &str, peer: PeerId) -> Arc<Engine> {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.oplog", path));
        let _ = std::fs::remove_file(format!("{}.tables", path));
        let _ = std::fs::remove_file(format!("{}.crc", path));
        let _ = std::fs::remove_file(format!("{}.db.lock", path));
        {
            let mut eng = Engine::create_standalone(path).unwrap();
            // 0.8.0: user table 経路で使う (= anonymous は enable_sync_tables で
            // 閉じるため)。 既存 test の "rows.val" himo は "rows.val" 名前空間に。
            eng.define_table("rows", 1000).unwrap();
            eng.define_himo_in("rows", "val", ValueType::Number, 100).unwrap();
            // vid 翻訳が要るのは text (Tag/Leaf) だけ。 `dropped_vocab` の test 用。
            eng.define_himo_in("rows", "name", ValueType::Tag, 100).unwrap();
            eng.enable_sync_tables().unwrap();
            eng.flush().unwrap();
        }
        let eng = Engine::open_concurrent_with_oplog(path, 4 * 1024 * 1024).unwrap();
        eng.set_peer_id(peer);
        eng
    }

    #[test]
    fn lww_newer_wins() {
        let path_a = test_path("a");
        let eng_a = new_eng(&path_a, 1);
        let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_a.clone(), transport.clone());

        // peer 2 からの古い op
        let eid = enchudb_oplog::make_eid(2, 7);
        let rec_old = WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid, himo_id: 0, value: 10 });
        let rec_new = WireRecord::unsigned(Hlc { wall: 200, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid, himo_id: 0, value: 20 });
        let out = syncer.apply_records(&[rec_new.clone(), rec_old.clone()]);
        assert_eq!(out.applied, 1);
        assert_eq!(out.skipped, 1);

        // より古い HLC で再送しても skip
        let out2 = syncer.apply_records(&[rec_old.clone()]);
        assert_eq!(out2.applied, 0);
        assert_eq!(out2.skipped, 1);

        let _ = std::fs::remove_file(path_a);
    }

    /// **翻訳できない remote vid を cell に書かない。**
    ///
    /// text の vid は author ローカルな番号でしかない。 `Vocab` op を受け損ねた
    /// (ring buffer の巻き込み / プロセスを跨いだ) 状態で生値のまま書くと、 その番号が
    /// 指す**無関係な local 文字列**が入る。 実地では `path` 列に別行の PK、
    /// `size` 列に mtime、 `key` 列に hash が現れた (15962 行中 12 行)。
    ///
    /// ここでは local に文字列を 1 つ intern してから、 その vid を「remote の vid」と
    /// して Vocab 無しで送りつける。 旧実装はこれを素通しして local 文字列を書いていた。
    #[test]
    fn untranslatable_remote_vid_is_dropped_not_written() {
        let path_a = test_path("vocabgap_a");
        let eng_a = new_eng(&path_a, 1);
        let hid = eng_a.himo_id("rows.name").expect("rows.name") as u16;

        // local に 1 つ intern → この vid が「無関係な既存文字列」の役。
        let local_eid = enchudb_oplog::make_eid(1, 1);
        eng_a.tie_text_to(local_eid, "rows.name", "LOCAL-ONLY");
        let colliding_vid = eng_a.vocab_id_bytes(b"LOCAL-ONLY").expect("interned");

        // peer 2 が同じ番号を自分の vid として使った体で、 Vocab 無しに Tie だけ送る。
        let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_a.clone(), transport.clone());
        let remote_eid = enchudb_oplog::make_eid(2, 9);
        let rec = WireRecord::unsigned(
            Hlc { wall: 100, logical: 0, peer: 2 },
            2,
            DecodedOp::Tie { eid: remote_eid, himo_id: hid, value: colliding_vid },
        );
        let out = syncer.apply_records(&[rec]);

        assert_eq!(out.applied, 0, "翻訳不能な vid は適用しない");
        assert_eq!(out.dropped_vocab, 1, "捨てたことを数える");
        // 相手の entity に local 文字列が漏れていない。
        if let Some(local) = eng_a.resolve_remote_eid_existing(remote_eid) {
            assert_eq!(
                eng_a.get_text_owned(local, "rows.name"),
                None,
                "未翻訳 vid の生値が書かれてはいけない"
            );
        }

        // Vocab が届けば通る。
        let recs = vec![
            WireRecord::unsigned(
                Hlc { wall: 200, logical: 0, peer: 2 },
                2,
                DecodedOp::Vocab { vid: colliding_vid, bytes: b"REMOTE-VALUE".to_vec() },
            ),
            WireRecord::unsigned(
                Hlc { wall: 201, logical: 0, peer: 2 },
                2,
                DecodedOp::Tie { eid: remote_eid, himo_id: hid, value: colliding_vid },
            ),
        ];
        let out2 = syncer.apply_records(&recs);
        assert_eq!(out2.dropped_vocab, 0);
        let local = eng_a.resolve_remote_eid_existing(remote_eid).expect("mapping");
        assert_eq!(
            eng_a.get_text_owned(local, "rows.name").as_deref(),
            Some(&b"REMOTE-VALUE"[..])
        );

        let _ = std::fs::remove_file(path_a);
    }

    #[test]
    fn two_peer_pull_and_apply() {
        let path_a = test_path("2peer_a");
        let eng_a = new_eng(&path_a, 1);
        let transport = Arc::new(InMemoryTransport::new());

        // peer 2 が tie した体で transport に直接 publish
        let eid_b = enchudb_oplog::make_eid(2, 3);
        transport.publish(2, vec![
            WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid: eid_b, himo_id: 0, value: 42 }),
        ]);

        let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
        let out = syncer.pull_once(2);
        assert_eq!(out.applied, 1);

        // #9: peer A は foreign eid をそのまま使わず、 自分の eid 空間の local eid に
        // 翻訳して置く。 元の foreign local (=3) ではなく翻訳後の eid で値を引く。
        let local = eng_a
            .resolve_remote_eid_existing(eid_b)
            .expect("translation mapping should exist after apply");
        let v = eng_a.get(local, "rows.val");
        assert_eq!(v, Some(42));

        let _ = std::fs::remove_file(path_a);
    }

    #[test]
    fn pull_incremental_advances_cursor() {
        let path_a = test_path("cursor_a");
        let eng_a = new_eng(&path_a, 1);
        let transport = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);

        // 1st round
        transport.publish(2, vec![WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid: enchudb_oplog::make_eid(2, 1), himo_id: 0, value: 10 })]);
        let out1 = syncer.pull_once(2);
        assert_eq!(out1.received, 1);

        // 2nd pull should see only new records
        transport.publish(2, vec![WireRecord::unsigned(Hlc { wall: 200, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid: enchudb_oplog::make_eid(2, 2), himo_id: 0, value: 20 })]);
        let out2 = syncer.pull_once(2);
        assert_eq!(out2.received, 1);
        assert_eq!(out2.applied, 1);

        let _ = std::fs::remove_file(path_a);
    }

    // ──────────────── request4: SubscriptionFilter / per-peer publish ────────────────

    /// SubscriptionFilter 未設定 (= default AllRecords) で、 publish_since が
    /// 旧 broadcast 経路と等価に動く事を確認。
    #[test]
    fn default_filter_is_backward_compatible() {
        let path_a = test_path("default_filter");
        let eng_a = new_eng(&path_a, 1);
        let transport = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);

        // peer 2/3 を transport に register (known_peers 経由で per-peer publish される)
        transport.register_peer(2);
        transport.register_peer(3);

        // peer 1 で書き込み → publish_since で他 peer に配信
        let e = eng_a.entity_in("rows").unwrap();
        eng_a.tie_async(e, "rows.val", 42);
        eng_a.flush_writes();
        eng_a.oplog_sync().unwrap();
        // 0.8.0: publish path が _sync_ops 経由になったので transfer を明示発火
        // (= 通常運用では consumer thread の fsync interval で自動だが、 test
        // では同期的に進める)
        eng_a.transfer_oplog_to_sync_ops();
        let count = syncer.publish_since(Hlc::ZERO);
        assert!(count > 0, "should publish at least the tie record");

        // peer 2 と peer 3 が pull_as すると同じ records を受信できる (default filter)
        let recs_2 = transport.pull_as(2, 1, Hlc::ZERO);
        let recs_3 = transport.pull_as(3, 1, Hlc::ZERO);
        assert_eq!(recs_2.len(), recs_3.len(), "default filter should send same set to all peers");
        assert!(recs_2.iter().any(|r| matches!(r.op, DecodedOp::Tie { value: 42, .. })));

        let _ = std::fs::remove_file(path_a);
    }

    /// 自前 SubscriptionFilter で peer 別に絞った配信ができる事を確認。
    #[test]
    fn custom_filter_can_partition_records_per_peer() {
        use crate::subscription::SubscriptionFilter;

        let path_a = test_path("partition_filter");
        let eng_a = new_eng(&path_a, 1);
        let transport = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);

        transport.register_peer(2);
        transport.register_peer(3);

        // 「peer 2 にだけ送る」 filter
        struct OnlyToPeer2;
        impl SubscriptionFilter for OnlyToPeer2 {
            fn should_send(&self, target: PeerId, _r: &WireRecord) -> bool {
                target == 2
            }
        }
        syncer.set_subscription_filter(Arc::new(OnlyToPeer2));

        let e = eng_a.entity_in("rows").unwrap();
        eng_a.tie_async(e, "rows.val", 77);
        eng_a.flush_writes();
        eng_a.oplog_sync().unwrap();
        eng_a.transfer_oplog_to_sync_ops();
        syncer.publish_since(Hlc::ZERO);

        let recs_2 = transport.pull_as(2, 1, Hlc::ZERO);
        let recs_3 = transport.pull_as(3, 1, Hlc::ZERO);
        // peer 2 は受信、 peer 3 は 0 件
        assert!(recs_2.iter().any(|r| matches!(r.op, DecodedOp::Tie { value: 77, .. })));
        assert!(recs_3.iter().all(|r| !matches!(r.op, DecodedOp::Tie { value: 77, .. })),
            "peer 3 should not see value=77 (filter excludes)");

        let _ = std::fs::remove_file(path_a);
    }

    /// publish_since_for_peer を直接呼んだ場合の動作確認。
    #[test]
    fn publish_since_for_peer_targets_one_peer_only() {
        let path_a = test_path("pubsincefor");
        let eng_a = new_eng(&path_a, 1);
        let transport = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);

        let e = eng_a.entity_in("rows").unwrap();
        eng_a.tie_async(e, "rows.val", 99);
        eng_a.flush_writes();
        eng_a.oplog_sync().unwrap();
        eng_a.transfer_oplog_to_sync_ops();

        // peer 5 のみに publish (filter default AllRecords)
        let n = syncer.publish_since_for_peer(5, Hlc::ZERO);
        assert!(n > 0);

        let recs_5 = transport.pull_as(5, 1, Hlc::ZERO);
        let recs_6 = transport.pull_as(6, 1, Hlc::ZERO);
        assert!(recs_5.iter().any(|r| matches!(r.op, DecodedOp::Tie { value: 99, .. })));
        assert!(recs_6.iter().all(|r| !matches!(r.op, DecodedOp::Tie { value: 99, .. })));

        let _ = std::fs::remove_file(path_a);
    }
}
