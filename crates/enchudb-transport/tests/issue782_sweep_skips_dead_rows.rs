//! #782: state の取り直しの掃除 (complete な state に無い書き手の行を消す) は、 もう消えている行を消し直さない。
//!
//! 前は、 書き手が前に消して削除がもう届いている行 (受け手では消えていて、 写像と削除の版数だけが残っている) も、 state を
//! 取り直すたびに今の時刻の新しい Delete で消し直した。 消えた行への新しい削除はその行の名前の削除の時刻を上げる (#551) ので、
//! 前の削除の後に同じ名前で足し直された行 (新しいデッキ) が、 掃除の Delete を受けた peer で消えた。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{StateBatch, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue782-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
    fn db(&self, id: u32) -> String {
        self.0.join(format!("p{id}")).to_str().unwrap().to_string()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 名前 `k` (Number) と `v` (Number)、 `t` (Tag)、 `body` (Leaf) を持つ表 `notes`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        e.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 16 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

struct Client {
    eng: Arc<Engine>,
    t: Arc<HttpHostTransport>,
    s: Syncer,
    /// 送った state (前提を確かめる)。
    sent: Arc<Mutex<Vec<StateBatch>>>,
}

/// state の配布元が 「engine の state をそのまま」 (`Syncer::serve_state` が自分の分として登録する物と同じ中身) の client。
fn client(eng: Arc<Engine>, url: &str) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t.clone());
    let sent: Arc<Mutex<Vec<StateBatch>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let (weak, sent) = (Arc::downgrade(&eng), sent.clone());
        t.register_state_provider(
            eng.peer_id(),
            Arc::new(move || {
                let eng = weak.upgrade()?;
                let (records, as_of) = eng.state_records();
                let batch = StateBatch { records, as_of, complete: true };
                sent.lock().unwrap().push(batch.clone());
                Some(batch)
            }),
        );
    }
    Client { eng, t, s, sent }
}

/// 次の書き込みの HLC の壁時計を進める (別の client の書き込みとの前後を、 書いた順にする)。
fn tick() {
    std::thread::sleep(Duration::from_millis(3));
}

impl Client {
    fn me(&self) -> PeerId {
        self.eng.peer_id()
    }
    fn himo(&self, name: &str) -> u16 {
        self.eng.himo_id(name).unwrap() as u16
    }
    fn create(&self, k: u32, v: u32) -> u64 {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
        tick();
        e
    }
    fn row(&self, k: u32) -> Option<u64> {
        self.eng.pull_raw("notes.k", k).first().copied()
    }
    fn v(&self, k: u32) -> Option<u64> {
        self.row(k).and_then(|e| self.eng.get(e, "notes.v"))
    }
    fn t(&self, k: u32) -> Option<String> {
        self.row(k).and_then(|e| self.eng.get_text_owned(e, "notes.t")).map(|b| String::from_utf8(b).unwrap())
    }
    fn body(&self, k: u32) -> Option<String> {
        self.row(k).and_then(|e| self.eng.get_text_owned(e, "notes.body")).map(|b| String::from_utf8(b).unwrap())
    }
    fn cells(&self, k: u32) -> (Option<u64>, Option<String>, Option<String>) {
        (self.v(k), self.t(k), self.body(k))
    }
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
    }
    fn pull(&self) {
        let out = self.s.pull_once(HOST);
        assert!(!out.history_truncated, "前提: client {} は抜けを言われない", self.me());
    }
    fn sync(&self) {
        self.eng.oplog_commit();
        self.push();
        self.pull();
    }
}

fn all(cs: &[&Client]) {
    for _ in 0..3 {
        for c in cs {
            c.sync();
        }
    }
}

fn host_cursor(host: &HttpHost, c: PeerId) -> Hlc {
    host.syncer().cursors_of(c).into_iter().find(|(a, _)| *a == c).map_or(Hlc::ZERO, |(_, h)| h)
}

/// ホストの c の cursor のすぐ上に c の floor を置く (= c が送れずに落とした record がある、 と言う)。 次の publish でホストは
/// c の state を求める。
fn ask_for_state(host: &HttpHost, c: &Client) {
    let cur = host_cursor(host, c.me());
    let floor = Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer };
    c.t.set_history_floor_multi(c.me(), &[(c.me(), floor)]);
}

/// c が publish → ホストが state を求める → c が送る。 送った state を返す。
fn push_state(host: &HttpHost, c: &Client) -> StateBatch {
    let before = c.sent.lock().unwrap().len();
    ask_for_state(host, c);
    c.push();
    let sent = c.sent.lock().unwrap();
    assert_eq!(sent.len(), before + 1, "前提: ホストが client {} の state を求め、 1 回送った", c.me());
    let batch = sent.last().cloned().unwrap();
    assert_eq!(host_cursor(host, c.me()), batch.as_of, "前提: ホストの client {} の cursor は state の as_of", c.me());
    batch
}

/// 欄の順 = 落とす順 (DB の directory は最後に消す)。
struct World {
    c1: Client,
    c2: Client,
    c3: Client,
    host: HttpHost,
    host_eng: Arc<Engine>,
    _dir: Dir,
}

fn world(tag: &str) -> World {
    let dir = Dir::new(tag);
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2, c3) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url), client(db(&dir.db(3), 3), &url));
    World { c1, c2, c3, host, host_eng, _dir: dir }
}

impl World {
    fn host_cells(&self, k: u32) -> (Option<u64>, Option<String>, Option<String>) {
        let row = self.host_eng.pull_raw("notes.k", k).first().copied();
        let text = |h: &str| row.and_then(|e| self.host_eng.get_text_owned(e, h)).map(|b| String::from_utf8(b).unwrap());
        (row.and_then(|e| self.host_eng.get(e, "notes.v")), text("notes.t"), text("notes.body"))
    }
    /// 行 `k` の (v, t, body) を [c1, host, c2, c3] の順に。
    fn everywhere(&self, k: u32) -> [(Option<u64>, Option<String>, Option<String>); 4] {
        [self.c1.cells(k), self.host_cells(k), self.c2.cells(k), self.c3.cells(k)]
    }
}

impl Client {
    fn untie_v(&self, k: u32) -> (u64, Hlc) {
        let row = self.row(k).unwrap();
        self.eng.untie(row, "notes.v");
        self.eng.oplog_commit();
        tick();
        (row, self.eng.cell_hlc(row, self.himo("notes.v")))
    }
    fn has_row(&self, k: u32) -> bool {
        self.row(k).is_some()
    }
}

impl World {
    /// 行 `k` があるかを [c1, host, c2, c3] の順に。
    fn rows(&self, k: u32) -> [bool; 4] {
        [self.c1.has_row(k), !self.host_eng.pull_raw("notes.k", k).is_empty(), self.c2.has_row(k), self.c3.has_row(k)]
    }
    fn vs(&self, k: u32) -> [Option<u64>; 4] {
        self.everywhere(k).map(|c| c.0)
    }
}

/// c2 が行 1 / 行 3、 c1 が行 2 を作り、 全員に届いた世界。
fn seeded(tag: &str) -> World {
    let w = world(tag);
    w.c2.create(1, 10);
    w.c2.create(3, 30);
    w.c1.create(2, 20);
    all(&[&w.c1, &w.c2, &w.c3]);
    for (k, v) in [(1, 10), (2, 20), (3, 30)] {
        assert_eq!(w.vs(k), [Some(v); 4], "前提: 行 {k} が全員に届いた");
    }
    assert!(host_cursor(&w.host, 1) > Hlc::ZERO, "前提: ホストは c1 の record を受けた");
    w
}


fn ring_deletes(eng: &Engine) -> Vec<(u64, Hlc, PeerId)> {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter_map(|r| match &r.op {
            DecodedOp::Delete { eid } => Some((*eid, r.hlc, r.author_peer)),
            _ => None,
        })
        .collect()
}

/// c1 が名前 7 の行を作って消し、 全員に届いた世界。 戻り値: c1 の行の世界番号と、 削除の版数。
fn created_and_deleted(w: &World) -> (u64, Hlc) {
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let row = c1.create(7, 70);
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(7), [Some(70); 4], "前提: 行 7 が全員に届いた");
    c1.eng.delete(row);
    c1.eng.oplog_commit();
    tick();
    let tomb = c1.eng.tombstone_hlc(row);
    all(&[c1, c2, c3]);
    assert_eq!(w.rows(7), [false; 4], "前提: 削除は全員に届いた");
    assert!(ring_deletes(&w.host_eng).contains(&(row, tomb, 1)), "前提: 削除の record はホストの ring に載った");
    (row, tomb)
}

/// issue の形: c1 が消した名前 7 を c2 が足し直した後に、 ホストが c1 の state を取り直す。 c2 の行は残る。
#[test]
fn a_row_readded_under_a_deleted_name_survives_a_state_resync() {
    let w = seeded("readd");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let (row, tomb) = created_and_deleted(&w);
    c2.create(7, 77);
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(7), [Some(77); 4], "前提: c2 が足し直した行 7 は全員にある");
    let before = ring_deletes(&w.host_eng);

    let batch = push_state(&w.host, c1);
    assert!(batch.complete && tomb < batch.as_of);
    // #784: c1 の state には、 消した行の Delete (元の削除の版数) だけがある。 ホストはその削除をもう同じ版数で受けている
    let of_row: Vec<_> = batch
        .records
        .iter()
        .filter(|r| matches!(&r.op, DecodedOp::Tie { eid, .. } | DecodedOp::Untie { eid, .. } | DecodedOp::Delete { eid } | DecodedOp::TieLeaf { eid, .. } if *eid == row))
        .collect();
    assert!(of_row.len() == 1 && matches!(of_row[0].op, DecodedOp::Delete { .. }) && of_row[0].hlc == tomb, "前提: c1 の state の、 消した行の record は元の版数の Delete だけ: {of_row:?}");
    assert_eq!(ring_deletes(&w.host_eng), before, "もう消えている行の Delete を、 ホストの ring に載せ直した");
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(7), [Some(77); 4], "c1 の state の取り直しで、 c2 が足し直した行 7 が消えた [c1, host, c2, c3]");
    assert_eq!(w.vs(2), [Some(20); 4], "c1 の他の行はそのまま");
}

/// 足し直しが無くても、 消えた行を消し直さない: 取り直しを繰り返しても、 ホストの ring の Delete と、 ホストの行の削除の版数は
/// 変わらない (前は取り直すたびに新しい版数の Delete が 1 つ増えた)。
#[test]
fn a_state_resync_does_not_delete_a_dead_row_again() {
    let w = seeded("again");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let (row, tomb) = created_and_deleted(&w);
    let before = ring_deletes(&w.host_eng);
    for _ in 0..3 {
        push_state(&w.host, c1);
        assert_eq!(ring_deletes(&w.host_eng), before, "掃除が、 もう消えている行を消し直した (ホストの ring の Delete)");
    }
    all(&[c1, c2, c3]);
    assert_eq!(w.rows(7), [false; 4]);
    assert_eq!(ring_deletes(&w.host_eng).iter().filter(|d| d.0 == row).map(|d| d.1).collect::<Vec<_>>(), vec![tomb], "行 7 の Delete は元の 1 つだけ");
}

/// 対照: 生きている行の掃除は今までどおり。 c1 が自分の行を消し、 出す前にホストが c1 の state を取り直す → 全員から消える。
#[test]
fn control_an_unpublished_delete_of_a_live_row_is_still_swept() {
    let w = seeded("live");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    c1.eng.delete(c1.row(2).unwrap());
    c1.eng.oplog_commit();
    tick();
    assert_eq!(w.rows(2), [false, true, true, true], "前提: 行 2 は c1 でだけ消えている");
    let n = ring_deletes(&w.host_eng).len();
    push_state(&w.host, c1);
    // #784: 消すのは掃除 (ホストの今の時刻の Delete) でなく、 state が運ぶ c1 の Delete
    assert_eq!(ring_deletes(&w.host_eng).len(), n + 1, "前提: ホストの ring に行 2 の Delete が 1 つ載った");
    all(&[c1, c2, c3]);
    assert_eq!(w.rows(2), [false; 4], "c1 が消した自分の行 2 が、 state の取り直しの後も残る");
    // もう一度取り直しても、 消し直さない
    push_state(&w.host, c1);
    assert_eq!(ring_deletes(&w.host_eng).len(), n + 1, "消えた行を消し直した");
    assert_eq!(w.vs(1), [Some(10); 4]);
    assert_eq!(w.vs(3), [Some(30); 4]);
}
