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

use enchudb_engine::engine::Engine;
use enchudb_engine::hlc_store::HlcStore;
use enchudb_engine::transport::{Transport, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};

use crate::subscription::{AllRecords, SubscriptionFilter};

pub struct Syncer {
    engine: Arc<Engine>,
    transport: Arc<dyn Transport>,
    /// 各 peer から最後に pull した地点(HLC)。次回はここより後だけ取る。
    /// `cursor_path` が設定されていれば update のたびにディスクに保存し、
    /// `Syncer::new` 時にロードして差分同期を継続する。
    last_pulled: std::sync::Mutex<std::collections::HashMap<PeerId, Hlc>>,
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
    /// LWW で古いと判定して skip した op 数。
    pub skipped: usize,
    /// Phase C: 署名検証で reject した op 数。
    pub rejected_signature: usize,
    /// Phase C: ACL で reject した op 数。
    pub rejected_acl: usize,
    /// #78 (0.9.0): reject (署名/ACL) された record の最小 HLC。 pull cursor は
    /// これを越えて前進しない — pubkey 登録との race 窓で reject された record が
    /// 永久に再配送されない silent gap を防ぐ (次回 pull で再検証される)。
    pub min_rejected_hlc: Option<Hlc>,
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
        let wal = engine.oplog_arc().unwrap_or_else(|| {
            panic!(
                "Syncer requires a WAL-enabled Engine. \
                 Use Engine::open_concurrent_with_oplog / create_concurrent_with_oplog \
                 instead of Engine::open / create."
            )
        });
        // 0.8.0: sync 配信の primary は `_sync_ops` 一本、 legacy oplog iter
        // fallback は撤去。 `Database::enable_sync()` (= `enable_sync_tables`) を
        // 呼んでない engine で Syncer を attach するのは fatal。
        if !engine.sync_tables_enabled() {
            panic!(
                "Syncer requires sync tables (_sync_ops / _sync_peers). \
                 Call Database::enable_sync() / Engine::enable_sync_tables() before \
                 attaching Syncer."
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
        // HlcStore は engine 内部のメモリ構造で永続化されない。 engine reopen 後は
        // 空状態なので、 attach 時に WAL を walk して LWW state を再構築する。
        // これがないと sync で過去レコードを未知扱いして再 apply してしまい、
        // 削除済み entity が復活する (= ① のバグの根本)。
        syncer.hydrate_hlc_store(&engine);
        syncer
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
            if parts.len() != 4 {
                continue;
            }
            let Ok(p) = parts[0].parse::<PeerId>() else { continue };
            let Ok(wall) = parts[1].parse::<u64>() else { continue };
            let Ok(logical) = parts[2].parse::<u32>() else { continue };
            let Ok(peer) = parts[3].parse::<PeerId>() else { continue };
            guard.insert(p, Hlc { wall, logical, peer });
        }
    }

    /// `last_pulled` を atomic write でディスクに保存。 `cursor_path` 未設定なら no-op。
    /// 書式: 1 行 1 エントリ、 `peer_id wall logical hlc_peer` (空白区切り)。
    /// 失敗しても sync は続行 (cursor は次回ロードで古いまま、 multi-apply は LWW で吸収)。
    fn save_cursors(&self) {
        let path = match self.cursor_path.read().unwrap().clone() {
            Some(p) => p,
            None => return,
        };
        let guard = self.last_pulled.lock().unwrap();
        let mut buf = String::new();
        for (p, h) in guard.iter() {
            buf.push_str(&format!("{} {} {} {}\n", p, h.wall, h.logical, h.peer));
        }
        drop(guard);
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, buf).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    /// engine の WAL を読んで HlcStore に LWW entry を再構築する。
    /// `Syncer::new` 内から呼ばれる。 Delete は sentinel (`u16::MAX`) で残す
    /// (= tombstone) ので、 後で来る古い HLC の Tie/Untie/Content が `apply_one`
    /// 内の tombstone check で skip される。
    fn hydrate_hlc_store(&self, engine: &Engine) {
        let Some(wal) = engine.oplog_arc() else { return };
        let store = engine.hlc_store();
        for rec in wal.iter_committed() {
            match &rec.op {
                DecodedOp::Tie { eid, himo_id, .. }
                | DecodedOp::Untie { eid, himo_id } => {
                    store.force_set(*eid, *himo_id, rec.hlc);
                }
                DecodedOp::Delete { eid } => {
                    store.force_set(*eid, u16::MAX, rec.hlc);
                }
                DecodedOp::Content { eid, key, .. } => {
                    let key_hash = enchudb_oplog::content_key_hash15(key);
                    store.force_set(*eid, key_hash | 0x8000, rec.hlc);
                }
                DecodedOp::TieNamed { eid, himo_name, .. } => {
                    // 0.9.0: 名前を local hid に解決できる場合のみ LWW entry を張る
                    // (未定義 = local に一度も届いていない himo は hydrate 不要)。
                    if let Some(hid) = engine.himo_id(himo_name) {
                        store.force_set(*eid, hid as u16, rec.hlc);
                    }
                }
                DecodedOp::TieLeaf { eid, himo_name, .. } => {
                    // 0.12.0 (#88): Leaf も名前で hid 解決して LWW entry を張る (TieNamed と同扱い)。
                    if let Some(hid) = engine.himo_id(himo_name) {
                        store.force_set(*eid, hid as u16, rec.hlc);
                    }
                }
                DecodedOp::Commit | DecodedOp::Vocab { .. } => {}
            }
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

    /// 指定 peer から未取得レコードを 1 回 pull して本体に apply。
    /// request4: `pull_as(self_peer, from, since)` 経由で broadcast log +
    /// (from, self_peer) targeted log を両方拾う。 partial sync 対応 transport
    /// (InMemoryTransport 等) では targeted 経由の per-peer record も受信できる。
    pub fn pull_once(&self, from: PeerId) -> SyncOutcome {
        let since = {
            let guard = self.last_pulled.lock().unwrap();
            guard.get(&from).copied().unwrap_or(Hlc::ZERO)
        };
        let self_peer = self.engine.peer_id();
        let records = self.transport.pull_as(self_peer, from, since);
        let outcome = self.apply_records(&records);

        // last_pulled を進める(空 pull でも既存のままで OK)。 進んだら disk に保存。
        // #78 (0.9.0): reject (署名/ACL) された record を cursor が越えない。
        // 旧実装は reject 込みで max HLC まで前進し、 pubkey 登録との race 窓の
        // record が永久に再配送されない silent gap を作っていた。 reject があった
        // 場合は「最小 reject HLC 未満の accepted record」までしか進めない
        // (= reject 分は次回 pull で再配送・再検証される)。
        let target = match outcome.min_rejected_hlc {
            None => records.iter().map(|r| r.hlc).max(),
            Some(minrej) => records.iter().map(|r| r.hlc).filter(|h| *h < minrej).max(),
        };
        let advanced = if let Some(last) = target {
            let mut guard = self.last_pulled.lock().unwrap();
            let cur = guard.get(&from).copied().unwrap_or(Hlc::ZERO);
            if last > cur {
                guard.insert(from, last);
                true
            } else {
                false
            }
        } else {
            false
        };
        if advanced {
            self.save_cursors();
        }

        outcome
    }

    /// request4: subscription filter を差し替える (起動時 1 度設定する想定)。
    /// default は `AllRecords` (全送り、 旧 `publish_since` の挙動 = SaaS 用)。
    /// SNS partial sync の caller は自前 struct で `impl SubscriptionFilter` する。
    pub fn set_subscription_filter(&self, filter: Arc<dyn SubscriptionFilter>) {
        *self.subscription_filter.write().unwrap() = filter;
    }

    /// 自 peer の commit 済み ops を transport に publish。
    /// `iter_committed` は checkpoint を無視して WAL 全体を列挙するので、
    /// 既に本体に apply 済みでも WAL ring buffer 内にあれば拾える。
    /// 戻り値は publish したレコード数 (重複カウントしない、 最終 broadcast/peer 別
    /// のいずれか単一経路で配信した数)。
    ///
    /// request4: transport が `known_peers()` を返すなら **per-peer 経路** (=
    /// `publish_since_for_peer` を全 peer に対して呼ぶ) で配信。 known_peers が
    /// 空なら **broadcast 経路** (= 旧 `publish_since` の挙動) にフォールバック。
    /// = 既存 caller (broadcast 前提) は API 不変で動く。
    pub fn publish_since(&self, since: Hlc) -> usize {
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

    /// 0.8.0: `since` HLC より新しい WireRecord を集める。 `_sync_ops` 経由
    /// (= publish の primary source、 legacy oplog iter fallback は 0.8.0 で
    /// 撤去、 `Syncer::new` で `sync_tables_enabled` チェック済み)。
    fn collect_records_since(&self, since: Hlc) -> Vec<WireRecord> {
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
    pub fn apply_records(&self, records: &[WireRecord]) -> SyncOutcome {
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
        let store = self.engine.hlc_store().clone();
        let require_sig = self.require_signature.load(std::sync::atomic::Ordering::Acquire);
        let pubkeys = self.engine.pubkeys().clone();
        let acl = self.engine.acl().clone();
        let note_reject = |out: &mut SyncOutcome, hlc: Hlc| {
            if out.min_rejected_hlc.map_or(true, |m| hlc < m) {
                out.min_rejected_hlc = Some(hlc);
            }
        };
        for rec in records {
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
            if self.apply_one(&store, rec) {
                out.applied += 1;
            } else {
                out.skipped += 1;
            }
        }
        out
    }

    // 0.9.0: 旧 Content reorder buffer (`buffer_pending` / `drain_pending`) は削除。
    // content は TieNamed で運ばれ、 TieNamed 自身が entity 写像を作れるため
    // 「Tie より先に届いた Content の退避」という問題が構造的に消えた。
    // (旧 buffer の既知バグ: in-memory のみで cursor だけ永続前進 → 再起動で
    //  buffered record が恒久喪失 / eviction が「最古」でなく任意 bucket を破棄)

    fn apply_one(&self, store: &HlcStore, rec: &WireRecord) -> bool {
        // 受信した WireRecord の header フィールドを Engine::remote_*_apply の relayed 引数に
        // そのまま渡す (gossip 経路で `OpLog::append_relayed` が元 HLC/author/署名を保持するため)。
        #[inline]
        fn relayed_header(rec: &WireRecord) -> enchudb_oplog::oplog::RelayedHeader {
            enchudb_oplog::oplog::RelayedHeader {
                hlc: rec.hlc,
                author: rec.author_peer,
                signature: rec.signature,
                pubkey_fp: rec.pubkey_fp,
            }
        }
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
                    None => return false,
                };
                // #9: entity 写像ができたので、 この foreign entity 宛に Tie より先に届いて
                // 退避していた Content を drain して apply する (= 配送順序ロス防止)。
                // #9: Ref himo は value 自体が foreign target eid なので、 ref の target
                // table 空間の local eid に翻訳 (確保できなければ skip)。 それ以外
                // (Tag/Symbol) は remote vocab vid を local vid に変換 (Number は identity)。
                let value = if self.engine.himo_is_ref(*himo_id) {
                    match self.engine.resolve_remote_ref_value(rec.author_peer, *value, *himo_id) {
                        Some(v) => v,
                        None => return false,
                    }
                } else {
                    self.engine.translate_remote_vid(rec.author_peer, *himo_id, *value)
                };
                // Tombstone check: 同 entity に tombstone (sentinel HLC) が記録済みで
                // それより古い Tie は復活させない。
                if let Some(tomb) = store.get(local_eid, u16::MAX) {
                    if rec.hlc < tomb {
                        return false;
                    }
                }
                if !store.try_set(local_eid, *himo_id, rec.hlc) {
                    return false;
                }
                self.engine.remote_tie_apply(local_eid, *himo_id, value, Some(relayed_header(rec)));
                true
            }
            DecodedOp::Untie { eid, himo_id } => {
                // #9: foreign eid を翻訳 (table-less なら確保先が無いので skip)。
                let local_eid = match self.engine.resolve_remote_eid(*eid, *himo_id) {
                    Some(e) => e,
                    None => return false,
                };
                // #9: 写像ができたので退避中の Content を drain。
                if let Some(tomb) = store.get(local_eid, u16::MAX) {
                    if rec.hlc < tomb {
                        return false;
                    }
                }
                if !store.try_set(local_eid, *himo_id, rec.hlc) {
                    return false;
                }
                self.engine.remote_untie_apply(local_eid, *himo_id, Some(relayed_header(rec)));
                true
            }
            DecodedOp::Delete { eid } => {
                // #9: Delete は himo を持たず table を導けないので既存の翻訳のみ引く。
                // 未登録 (= ここに一度も sync されてない foreign entity) なら消す対象が
                // 無いので skip。
                let local_eid = match self.engine.resolve_remote_eid_existing(*eid) {
                    Some(e) => e,
                    None => return false,
                };
                // Delete は全 himo に波及。sentinel himo_id = 0xFFFF で HLC を記録。
                // remove_entity は呼ばず tombstone を残す。 後続の古い HLC の
                // Tie/Untie/Content は上の tombstone check で skip され、 削除済み
                // entity が復活しない。
                if !store.try_set(local_eid, u16::MAX, rec.hlc) {
                    return false;
                }
                self.engine.remote_delete_apply(local_eid, Some(relayed_header(rec)));
                true
            }
            DecodedOp::TieNamed { eid, himo_name, himo_kind, value } => {
                // 0.9.0: 動的 himo (content 互換層の `_c_{key}`) は id が peer 間で
                // 揃わないため名前で解決する。 受信側に未定義なら lazy 定義 —
                // これで Content 専用の reorder buffer / key hash が不要になる
                // (mapping は Tie と同じ resolve_remote_eid で作られる)。
                let local_hid = match self.engine.ensure_himo_named(himo_name, *himo_kind) {
                    Ok(h) => h,
                    Err(_) => return false, // himo 予算枯渇等 — 適用不能
                };
                let local_eid = match self.engine.resolve_remote_eid(*eid, local_hid) {
                    Some(e) => e,
                    None => return false,
                };
                // 値は author-local vid → local vid に変換 (Leaf/Tag のみ、 Number は identity)
                let value = self.engine.translate_remote_vid(rec.author_peer, local_hid, *value);
                if let Some(tomb) = store.get(local_eid, u16::MAX) {
                    if rec.hlc < tomb {
                        return false;
                    }
                }
                if !store.try_set(local_eid, local_hid, rec.hlc) {
                    return false;
                }
                self.engine.remote_tie_apply(local_eid, local_hid, value, Some(relayed_header(rec)));
                true
            }
            DecodedOp::TieLeaf { eid, himo_name, himo_kind, bytes } => {
                // 0.12.0 (#88): Leaf payload を bytes 同乗で受信。 名前で himo 解決 →
                // eid 翻訳 → LWW → LeafStore.insert + cell set (vid mapping 不要)。
                let local_hid = match self.engine.ensure_himo_named(himo_name, *himo_kind) {
                    Ok(h) => h,
                    Err(_) => return false,
                };
                let local_eid = match self.engine.resolve_remote_eid(*eid, local_hid) {
                    Some(e) => e,
                    None => return false,
                };
                if let Some(tomb) = store.get(local_eid, u16::MAX) {
                    if rec.hlc < tomb {
                        return false;
                    }
                }
                if !store.try_set(local_eid, local_hid, rec.hlc) {
                    return false;
                }
                self.engine.remote_tieleaf_apply(local_eid, local_hid, bytes, Some(relayed_header(rec)));
                true
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
                    None => return false,
                };
                // Content は key 単位で LWW。himo_id を使えないので hash で代用。
                if let Some(tomb) = store.get(local_eid, u16::MAX) {
                    if rec.hlc < tomb {
                        return false;
                    }
                }
                let key_hash = enchudb_oplog::content_key_hash15(key);
                if !store.try_set(local_eid, key_hash | 0x8000, rec.hlc) {
                    return false;
                }
                self.engine.remote_content_apply(local_eid, key, data, Some(relayed_header(rec)));
                true
            }
            DecodedOp::Commit => true, // boundary marker、apply は不要
            DecodedOp::Vocab { vid, bytes } => {
                // 0.8.4 issue #30: 既に同 (author_peer, vid, bytes) を登録済みなら
                // skip。 これが無いと gossip_remote_apply ON で同じ vocab record が
                // 再 apply され続け、 caller (Syncer) の applied counter が永久に
                // 0 に戻らず amplification loop の見かけになる。
                if self.engine.has_remote_vocab(rec.author_peer, *vid, bytes) {
                    return false;
                }
                // author_peer の (vid, bytes) を受信。
                // Engine 側の remote_vocab_apply に委譲 (peer 別 vid mapping を構築)。
                self.engine.remote_vocab_apply(rec.author_peer, *vid, bytes, Some(relayed_header(rec)));
                true
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
