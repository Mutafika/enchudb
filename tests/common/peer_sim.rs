//! 実 `Engine` + `Syncer` を 1 本の transport にぶら下げた **peer 試験環境**。
//!
//! ## なぜ要るか
//!
//! sync の欠陥はこれまで実アプリ (syncretic) の症状から逆算していた。 それは
//! 反証には使えるが、 **仕様の基準にすると危ない**。 syncretic は disk という
//! 外部の真実を持っているので「壊れたら再 scan で埋め直す」が成立するが、
//! library の利用者にとっては DB が唯一の真実で、 同じ逃げ道が無い。
//!
//! なのでここでは受け入れ基準を 1 本に固定する:
//!
//! > **アプリ層の再 author を一切挟まずに、 peer 同士が収束すること。**
//!
//! ## 既存 harness との関係
//!
//! - `tests/common/chaos_sim.rs`: partition / crash / drop / delay を持つが、
//!   対象は抽象 message と GCounter/OrSet で、 enchudb の sync 経路を踏まない
//! - `tests/v32_two_peer_sync.rs`: 実 Engine を動かすが happy path のみ、
//!   故障注入が無く、 各 test が peer 構築を手配線している
//!
//! その間を埋めるのがこの module。
//!
//! ## 注入できる故障
//!
//! - [`PeerSim::restart`] — **揮発 state の消去**。 `peer_vocab_map` /
//!   `HlcStore` は memory 上にしか無いので、 プロセスを跨ぐと消える。 一方
//!   `Syncer` の pull cursor は `with_cursor_path` で永続する。 この非対称が
//!   「cursor は消費済みと言うが、 写像はもう無い」窓を作る
//! - [`PeerSim::author_text`] + [`PeerSim::deliver`] — **batch 境界の分割**。
//!   author が出す `Vocab` / `Tie` を個別に配送できるので、 「`Vocab` は前の
//!   セッションで消費済み、 `Tie` は今回届く」を決定的に再現できる
//!
//! ring 窓の追い越し (`_sync_ops` の reclaim) は追って足す。

#![allow(dead_code)]

use std::sync::Arc;

use enchudb::sync::{SyncOutcome, Syncer};
use enchudb::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb::{Engine, ValueType};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::Hlc;

/// sim が使う table 名。 `enable_sync_tables()` が anonymous table を閉じるので
/// named table が必須 (`v32_two_peer_sync.rs` と同じ理由)。
pub const TABLE: &str = "t";

/// 全 peer に定義する himo。 peer 間で **定義順が同じ**なので himo_id 空間も揃う。
const HIMOS: &[(&str, ValueType, u32)] = &[
    ("name", ValueType::Tag, 0),
    ("val", ValueType::Number, 1000),
];

const OPLOG_CAPACITY: usize = 16 * 1024 * 1024;
const MAX_ENTITIES: u32 = 1000;

/// `TABLE` 内 himo の qualified name。
pub fn q(himo: &str) -> String {
    format!("{}.{}", TABLE, himo)
}

/// author が 1 回の text write で出す wire record 列。
///
/// `Vocab` → `Tie` の順。 この 2 つを**別々の pull に跨がせる**のが
/// vocab 写像欠落の再現手段なので、 まとめずに保持する。
pub struct Authored {
    /// author 側 (= 発行 peer) の eid。 収束判定の識別子に使う。
    pub eid: u64,
    /// **author ローカル**の vocab id。 受信側でこの番号のまま cell に書くと
    /// 受信側の無関係な文字列を指す (= 写像が要る理由)。
    pub vid: u32,
    /// 書いた文字列そのもの。 写像の照合に使う。
    pub value: String,
    /// `(author_peer, vid) → bytes` を運ぶ record。
    pub vocab: WireRecord,
    /// cell に vid を書く record。 `vocab` 無しでは意味を持たない。
    pub tie: WireRecord,
}

impl Authored {
    /// 通常の author が出す順序 (`Vocab` → `Tie`)。
    pub fn all(&self) -> Vec<WireRecord> {
        vec![self.vocab.clone(), self.tie.clone()]
    }
}

/// 1 台の peer。 `eng` / `syncer` が `None` の間は「停止中」。
struct SimPeer {
    id: u32,
    db_path: String,
    cursor_path: String,
    eng: Option<Arc<Engine>>,
    syncer: Option<Syncer>,
}

impl SimPeer {
    fn eng(&self) -> &Arc<Engine> {
        self.eng.as_ref().expect("peer is running")
    }

    fn syncer(&self) -> &Syncer {
        self.syncer.as_ref().expect("peer is running")
    }

    /// プロセス終了相当。 memory 上の state (`peer_vocab_map` / `HlcStore`) は
    /// ここで消える。 disk 上の cell と cursor file は残る。
    fn shutdown(&mut self) {
        if let Some(eng) = &self.eng {
            eng.flush_writes();
            let _ = eng.oplog_sync();
        }
        // Syncer が `Arc<Engine>` を握っているので先に落とす。 両方消えて初めて
        // writer lock (`.db.lock`) が解放され、 開き直せる。
        self.syncer = None;
        self.eng = None;
    }

    fn boot(&mut self, transport: &Arc<InMemoryTransport>) {
        assert!(self.eng.is_none(), "peer {} is already running", self.id);
        let eng = Engine::open_concurrent_with_oplog(&self.db_path, OPLOG_CAPACITY).unwrap();
        eng.set_peer_id(self.id);
        let syncer = Syncer::new(eng.clone(), transport.clone() as Arc<dyn Transport>)
            .with_cursor_path(std::path::PathBuf::from(&self.cursor_path));
        self.eng = Some(eng);
        self.syncer = Some(syncer);
    }
}

/// 複数 peer + 共有 transport。 `Drop` で作業ディレクトリごと消す。
pub struct PeerSim {
    dir: String,
    transport: Arc<InMemoryTransport>,
    peers: Vec<SimPeer>,
    /// 収束判定の対象 `(author eid, himo)`。 author 時に自動で積まれる。
    tracked: Vec<(u64, String)>,
    /// 手発行 record 用の HLC 採番。 wall を単調増加させるだけ。
    next_wall: u64,
}

impl PeerSim {
    /// `n` 台の peer を立ち上げる。 peer id は `1..=n`。
    pub fn new(tag: &str, n: usize) -> Self {
        let dir = format!(
            "{}/enchudb-peersim-{}-{}",
            std::env::temp_dir().display(),
            tag,
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create sim dir");

        let transport = Arc::new(InMemoryTransport::new());
        let mut peers = Vec::with_capacity(n);
        for i in 0..n {
            let id = (i + 1) as u32;
            let db_path = format!("{}/peer{}.db", dir, id);
            let cursor_path = format!("{}/peer{}.cursors", dir, id);
            Self::build_db(&db_path);
            let mut p = SimPeer { id, db_path, cursor_path, eng: None, syncer: None };
            p.boot(&transport);
            peers.push(p);
            transport.register_peer(id);
        }

        Self { dir, transport, peers, tracked: Vec::new(), next_wall: 1_000 }
    }

    /// schema を作る。 `Arc<Engine>` は `&mut` を取れないので、 plain create で
    /// 定義してから WAL 付きで開き直す (`v32_two_peer_sync.rs` と同じ手順)。
    fn build_db(path: &str) {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_table(TABLE, MAX_ENTITIES).unwrap();
        for (name, vt, max_values) in HIMOS {
            eng.define_himo_in(TABLE, name, *vt, *max_values).unwrap();
        }
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }

    /// peer `i` の cursor file に**永続済み**の `from` 向け HLC。
    ///
    /// `Syncer::save_cursors` は `pull_once` が caller に return する**前**に
    /// 走る。 つまりここに値が入っている時点で、 caller はまだ何もできていない。
    pub fn persisted_cursor(&self, i: usize, from: u32) -> Option<(u64, u32, u32)> {
        let s = std::fs::read_to_string(&self.peers[i].cursor_path).ok()?;
        for line in s.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() == 4 && f[0].parse::<u32>() == Ok(from) {
                return Some((
                    f[1].parse().ok()?,
                    f[2].parse().ok()?,
                    f[3].parse().ok()?,
                ));
            }
        }
        None
    }

    /// peer `i` の DB path (crash copy の検査用)。
    pub fn db_path(&self, i: usize) -> &str {
        &self.peers[i].db_path
    }

    pub fn peer_id(&self, i: usize) -> u32 {
        self.peers[i].id
    }

    pub fn engine(&self, i: usize) -> &Arc<Engine> {
        self.peers[i].eng()
    }

    pub fn transport(&self) -> &Arc<InMemoryTransport> {
        &self.transport
    }

    fn hid(&self, i: usize, himo: &str) -> u16 {
        self.peers[i].eng().himo_id(&q(himo)).expect("himo defined") as u16
    }

    fn mint(&mut self, peer: u32) -> Hlc {
        self.next_wall += 1;
        Hlc { wall: self.next_wall, logical: 0, peer }
    }

    // ──── 故障注入 ────

    /// **peer を再起動する** (= プロセスを跨ぐ)。
    ///
    /// `Engine` と `Syncer` を落として同じ path から開き直す。 memory 上の
    /// `peer_vocab_map` / `HlcStore` は消え、 disk 上の cell と cursor は残る。
    /// 実運用の再起動と同じ非対称をここで作る。
    pub fn restart(&mut self, i: usize) {
        let transport = self.transport.clone();
        self.peers[i].shutdown();
        self.peers[i].boot(&transport);
    }

    /// **電源断相当の拾い直し**。 clean close を挟まずに、 いま disk にある物だけを
    /// 別 path へコピーして開く。
    ///
    /// `restart` は in-process の drop なので、 どうしても clean shutdown になる。
    /// SIGKILL で失われる state (= まだ sidecar に落ちていない物) を見るには、
    /// 「今の disk の中身だけで開き直せるか」を直接見るしかない。
    ///
    /// 返る `Engine` は元 peer とは別 DB。 検査専用で、 sim には組み込まれない。
    pub fn crash_snapshot(&self, i: usize) -> Arc<Engine> {
        self.crash_snapshot_at(i, "crashcopy")
    }

    /// `crash_snapshot` の copy 先を分ける版 (1 test 内で 2 回撮る用)。
    pub fn crash_snapshot_at(&self, i: usize, tag: &str) -> Arc<Engine> {
        let src = self.peers[i].db_path.clone();
        let dst = format!("{}.{}", src, tag);
        let src_name = std::path::Path::new(&src)
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let dir = std::path::Path::new(&src).parent().unwrap().to_path_buf();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            // `.lock` は flock 済みなのでコピーしない (再取得は開き直し側で行う)。
            if !name.starts_with(&src_name) || name.ends_with(".lock") {
                continue;
            }
            let suffix = &name[src_name.len()..];
            let _ = std::fs::copy(entry.path(), format!("{}{}", dst, suffix));
        }
        let eng = Engine::open_concurrent_with_oplog(&dst, OPLOG_CAPACITY).unwrap();
        eng.set_peer_id(self.peers[i].id);
        eng
    }

    // ──── author / 配送 ────

    /// peer `i` が新しい entity に text を書く。
    ///
    /// peer 自身の cell は確定させたうえで、 **wire record は配送せずに返す**。
    /// `Vocab` と `Tie` を別々の pull に跨がせられるようにするため。
    pub fn author_text(&mut self, i: usize, himo: &str, value: &str) -> Authored {
        let eid = self.peers[i].eng().entity_in(TABLE).unwrap();
        self.author_text_to(i, eid, himo, value)
    }

    /// `author_text` の既存 entity 版。
    pub fn author_text_to(&mut self, i: usize, eid: u64, himo: &str, value: &str) -> Authored {
        let peer = self.peers[i].id;
        let himo_id = self.hid(i, himo);
        self.peers[i].eng().tie_text_to(eid, &q(himo), value);
        let vid = self.peers[i]
            .eng()
            .vocab_id(value)
            .expect("author interned the value");

        let h_vocab = self.mint(peer);
        let h_tie = self.mint(peer);
        self.tracked.push((eid, himo.to_string()));
        Authored {
            eid,
            vid,
            value: value.to_string(),
            vocab: WireRecord::unsigned(
                h_vocab,
                peer,
                DecodedOp::Vocab { vid, bytes: value.as_bytes().to_vec() },
            ),
            tie: WireRecord::unsigned(h_tie, peer, DecodedOp::Tie { eid, himo_id, value: vid }),
        }
    }

    /// peer `i` が entity を削除する wire record を作る。 peer 自身の行も消す。
    ///
    /// `Delete` は himo を持たないので、 受信側は `resolve_remote_eid_existing`
    /// (= 既存の写像のみ) でしか宛先を引けない。 写像が無ければ `apply_one` は
    /// `false` を返し、 **cursor はそれを越えて前進する**。
    pub fn author_delete(&mut self, i: usize, eid: u64) -> WireRecord {
        let peer = self.peers[i].id;
        self.peers[i].eng().delete(eid);
        let hlc = self.mint(peer);
        WireRecord::unsigned(hlc, peer, DecodedOp::Delete { eid })
    }

    /// peer `i` のローカル書き込み。 sync には出さない (vid 空間を埋める用途)。
    pub fn write_local_text(&mut self, i: usize, himo: &str, value: &str) -> u64 {
        let eid = self.peers[i].eng().entity_in(TABLE).unwrap();
        self.peers[i].eng().tie_text_to(eid, &q(himo), value);
        eid
    }

    /// record を transport に載せる (= author が publish した状態にする)。
    pub fn deliver(&self, records: Vec<WireRecord>) {
        if records.is_empty() {
            return;
        }
        let peer = records[0].author_peer;
        debug_assert!(records.iter().all(|r| r.author_peer == peer));
        self.transport.publish(peer, records);
    }

    /// peer `i` が `from` の vid → 自分の vid の写像を持っているか。
    ///
    /// `Vocab` op を適用した直後は `true`。 これが `false` に戻ると、 以降届く
    /// `Tie` は翻訳できない。
    pub fn has_vocab_mapping(&self, i: usize, authored: &Authored, from: u32) -> bool {
        self.peers[i]
            .eng()
            .has_remote_vocab(from, authored.vid, authored.value.as_bytes())
    }

    /// peer `i` が peer `from` から pull する。
    pub fn pull(&self, i: usize, from: u32) -> SyncOutcome {
        self.peers[i].syncer().pull_once(from)
    }

    /// 全 peer が他の全 peer から pull する、 を 1 巡。
    pub fn pull_all(&self) {
        for i in 0..self.peers.len() {
            for j in 0..self.peers.len() {
                if i != j {
                    self.pull(i, self.peers[j].id);
                }
            }
        }
    }

    // ──── 判定 ────

    /// peer `i` から見た `(author eid, himo)` の値。
    ///
    /// author 自身なら local eid そのまま、 他 peer なら foreign eid 翻訳を引く。
    /// 翻訳が無い (= その entity をまだ知らない) なら `None`。
    pub fn read(&self, i: usize, author_eid: u64, himo: &str) -> Option<Vec<u8>> {
        let p = &self.peers[i];
        let local = if enchudb_oplog::eid_peer(author_eid) == p.id {
            author_eid
        } else {
            p.eng().resolve_remote_eid_existing(author_eid)?
        };
        p.eng().get_text_owned(local, &q(himo))
    }

    /// **この harness の受け入れ基準**: author された全 cell が全 peer で一致すること。
    ///
    /// アプリ層の再 author / bootstrap を一切挟まずに成立する必要がある。
    pub fn assert_converged(&self) {
        let mut diverged = Vec::new();
        for (eid, himo) in &self.tracked {
            let vals: Vec<Option<Vec<u8>>> =
                (0..self.peers.len()).map(|i| self.read(i, *eid, himo)).collect();
            if vals.iter().any(|v| *v != vals[0]) {
                let shown: Vec<String> = vals
                    .iter()
                    .enumerate()
                    .map(|(i, v)| format!("peer{}={}", self.peers[i].id, show(v)))
                    .collect();
                diverged.push(format!("  eid {:#x} / {}: {}", eid, himo, shown.join("  ")));
            }
        }
        if !diverged.is_empty() {
            panic!(
                "peers did not converge ({} cell(s) diverged):\n{}",
                diverged.len(),
                diverged.join("\n")
            );
        }
    }
}

impl Drop for PeerSim {
    fn drop(&mut self) {
        for p in &mut self.peers {
            p.shutdown();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn show(v: &Option<Vec<u8>>) -> String {
    match v {
        None => "<none>".to_string(),
        Some(b) => format!("{:?}", String::from_utf8_lossy(b)),
    }
}
