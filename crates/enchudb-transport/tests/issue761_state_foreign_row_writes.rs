//! #761: ホストが client の state を取り直しても、 その client が **他の client の行** に書いてまだ出していなかった書き込みが残る。
//!
//! 前は、 書き手 c の state (`Engine::state_records`) は c が作った行だけだった。 ホストは state を当てた後 `cursor[c] = as_of`
//! にして、 as_of より前の c の record を受けない (#753)。 c が他の client の行に書いた分は state にも差分にも乗らず、 c の DB に
//! だけ残って、 食い違ったまま戻らなかった。 今は、 自分の時計の版数を持つ他の peer の行の cell も、 自分の record の形 (宛名は
//! 行の世界番号 = 差分で出す時と同じ形) で state に入る。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{StateBatch, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId, eid_local, eid_peer, make_eid};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue761-{tag}-{}", std::process::id()));
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

/// issue の形: c1 が c2 の行に書いて (Number / Tag / Leaf)、 出す前にホストが c1 の state を取り直す。 書き込みは全員に届く。
#[test]
fn unpublished_writes_to_another_clients_row_survive_a_state_resync() {
    let w = world("w1");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    // c2 が行 1 を作り、 全員に届く。 c1 も自分の行 2 を持つ (ホストの c1 の cursor を ZERO でなくする)
    c2.create(1, 10);
    c1.create(2, 20);
    all(&[c1, c2, c3]);
    let before = (Some(10), None, None);
    assert_eq!(w.everywhere(1), [before.clone(), before.clone(), before.clone(), before.clone()], "前提: c2 の行 1 が全員に届いた");
    assert!(host_cursor(&w.host, 1) > Hlc::ZERO, "前提: ホストは c1 の record を受けた");

    // c1 が c2 の行 1 を書き換える (commit、 まだ出さない)
    let row = c1.row(1).unwrap();
    c1.eng.tie_to(row, "notes.v", 11u32);
    c1.eng.tie_text_to(row, "notes.t", "from-c1");
    c1.eng.tie_text_to(row, "notes.body", "body-from-c1");
    c1.eng.oplog_commit();
    tick();
    let written: Vec<Hlc> = ["notes.v", "notes.t", "notes.body"].iter().map(|h| c1.eng.cell_hlc(row, c1.himo(h))).collect();
    assert!(written.iter().all(|h| h.peer == 1 && *h > host_cursor(&w.host, 1)), "前提: 3 つの cell は c1 の時計の版数で、 ホストはまだ受けていない ({written:?})");
    assert_eq!(w.host_cells(1), before, "前提: ホストにはまだ届いていない");

    // ホストが c1 の state を求め、 c1 が送る
    let batch = push_state(&w.host, c1);
    assert!(written.iter().all(|h| *h < batch.as_of), "前提: 書き込みは as_of より前 (ホストはもう差分で受けない)");
    assert!(batch.complete);

    all(&[c1, c2, c3]);
    let after = (Some(11), Some("from-c1".to_string()), Some("body-from-c1".to_string()));
    assert_eq!(
        w.everywhere(1),
        [after.clone(), after.clone(), after.clone(), after.clone()],
        "c1 が c2 の行に書いた分が、 state の取り直しの後 [c1, host, c2, c3] で揃わない"
    );
    assert_eq!(w.everywhere(2).map(|c| c.0), [Some(20); 4], "c1 の自分の行はそのまま");
}

/// state には、 自分の時計の版数の cell だけが入る: c2 が書いた cell (c1 は受けただけ) は c1 の state に入らない。 c1 が書いた後に
/// c2 が上書きした cell も入らない (版数は c2 の時計)。
#[test]
fn only_cells_written_by_the_client_are_in_its_state() {
    let w = world("w2");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    c2.create(1, 10);
    c2.create(3, 30);
    c1.create(2, 20);
    all(&[c1, c2, c3]);
    // c1 が c2 の行 1 と行 3 に書き、 全員に届く。 その後 c2 が行 3 を上書きする
    c1.eng.tie_to(c1.row(1).unwrap(), "notes.v", 11u32);
    c1.eng.tie_to(c1.row(3).unwrap(), "notes.v", 31u32);
    c1.eng.oplog_commit();
    tick();
    all(&[c1, c2, c3]);
    c2.eng.tie_to(c2.row(3).unwrap(), "notes.v", 32u32);
    c2.eng.oplog_commit();
    tick();
    all(&[c1, c2, c3]);
    assert_eq!(w.everywhere(1).map(|c| c.0), [Some(11); 4]);
    assert_eq!(w.everywhere(3).map(|c| c.0), [Some(32); 4]);

    let (k, v) = (c1.himo("notes.k"), c1.himo("notes.v"));
    let world_of = |c: &Client, key: u32| make_eid(2, eid_local(c.row(key).unwrap()));
    let (row1, row3) = (world_of(c2, 1), world_of(c2, 3));
    let batch = push_state(&w.host, c1);
    // 他の peer の行宛ての record
    let foreign: Vec<(u64, u16, u64, Hlc)> = batch
        .records
        .iter()
        .filter_map(|r| match &r.op {
            DecodedOp::Tie { eid, himo_id, value } if eid_peer(*eid) != 1 => Some((*eid, *himo_id, *value, r.hlc)),
            _ => None,
        })
        .collect();
    assert!(batch.records.iter().all(|r| r.author_peer == 1), "c1 が作った state の record は全部 c1 の record");
    assert_eq!(foreign.len(), 1, "c1 の state に入る他の peer の行の cell は、 行 1 の v だけ ({foreign:?})");
    let (eid, himo, value, hlc) = foreign[0];
    assert_eq!((eid, himo, value, hlc.peer), (row1, v, 11, 1), "宛名は c2 の行 1 の世界番号、 版数は c1 の時計");
    assert!(!batch.records.iter().any(|r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, .. } if *eid == row1 && *himo_id == k)), "c2 が書いた名前のカードは入らない");
    assert!(!batch.records.iter().any(|r| matches!(&r.op, DecodedOp::Tie { eid, .. } if *eid == row3)), "c2 が上書きした行 3 は入らない");

    all(&[c1, c2, c3]);
    assert_eq!(w.everywhere(1).map(|c| c.0), [Some(11); 4]);
    assert_eq!(w.everywhere(3).map(|c| c.0), [Some(32); 4]);
    assert_eq!(w.everywhere(2).map(|c| c.0), [Some(20); 4]);
}

/// 他の peer の行宛ての record が、 state に無い自分の行の掃除 (ghost sweep) を止めない: c1 が自分の行を消し、 同じ番号 (local) の
/// c2 の行に書く。 ホストは c1 の state を当てた後、 c1 の消した行を掃除する (c2 の行の record の番号を c1 の行と取り違えない)。
#[test]
fn a_write_to_another_clients_row_does_not_shield_a_deleted_row_from_the_sweep() {
    let w = world("w3");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    // c1 と c2 が、 それぞれ自分の DB で同じ番号の行を作る
    let mine = c1.create(5, 50);
    let theirs = c2.create(6, 60);
    assert_eq!(eid_local(mine), eid_local(theirs), "前提: c1 の行 5 と c2 の行 6 は、 それぞれの DB で同じ番号 (local)");
    all(&[c1, c2, c3]);
    assert_eq!(w.everywhere(5).map(|c| c.0), [Some(50); 4]);
    assert_eq!(w.everywhere(6).map(|c| c.0), [Some(60); 4]);

    // c1 が自分の行 5 を消し、 c2 の行 6 に書く (どちらも出さない)
    c1.eng.delete(c1.row(5).unwrap());
    c1.eng.tie_to(c1.row(6).unwrap(), "notes.v", 61u32);
    c1.eng.oplog_commit();
    tick();
    let batch = push_state(&w.host, c1);
    let theirs_world = make_eid(2, eid_local(theirs));
    assert!(
        batch.records.iter().any(|r| matches!(&r.op, DecodedOp::Tie { eid, .. } if *eid == theirs_world)),
        "前提: state に c2 の行 6 宛ての record がある (番号は c1 の消した行 5 と同じ)"
    );
    assert!(
        !batch.records.iter().any(|r| matches!(&r.op, DecodedOp::Tie { eid, .. } if *eid == make_eid(1, eid_local(mine)))),
        "前提: state に c1 の消した行 5 は無い"
    );

    all(&[c1, c2, c3]);
    assert_eq!(w.everywhere(5).map(|c| c.0), [None; 4], "c1 が消した行 5 が [c1, host, c2, c3] のどこかに残った");
    assert_eq!(w.everywhere(6).map(|c| c.0), [Some(61); 4]);
}
