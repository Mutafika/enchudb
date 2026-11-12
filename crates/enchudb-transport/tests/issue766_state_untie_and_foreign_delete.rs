//! #766: ホストが client の state を取り直しても、 その client がまだ出していなかった untie (自分の行・他の client の行) と、
//! 他の client の行の削除が残る。
//!
//! 前は、 state (`Engine::state_records`) は値のある cell だけを運んだ。 ホストは state を当てた後 `cursor[c] = as_of` にして、
//! as_of より前の c の record を受けない (#753)。 c が出していなかった untie と、 他の client の行の削除は、 state にも差分にも
//! 乗らず、 c の DB にだけ残った。 今は、 生きている行の 「自分の時計の版数で値の無い cell」 を Untie、 「自分の時計の削除の版数を
//! 持つ他の peer の行」 を Delete の record として state に入れる (どちらも差分で出す時と同じ形)。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{StateBatch, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId, eid_local, make_eid};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue766-{tag}-{}", std::process::id()));
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

/// 形 1: c1 が c2 の行を消し、 出す前にホストが c1 の state を取り直す。 削除は全員に届く。
#[test]
fn an_unpublished_delete_of_another_clients_row_survives_a_state_resync() {
    let w = seeded("del");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let row = c1.row(1).unwrap();
    let world_row = make_eid(2, eid_local(c2.row(1).unwrap()));
    c1.eng.delete(row);
    c1.eng.oplog_commit();
    tick();
    let tomb = c1.eng.tombstone_hlc(row);
    assert!(tomb.peer == 1 && tomb > host_cursor(&w.host, 1), "前提: 削除は c1 の時計の版数で、 ホストはまだ受けていない ({tomb:?})");
    assert_eq!(w.rows(1), [false, true, true, true], "前提: 行 1 は c1 でだけ消えている");

    let batch = push_state(&w.host, c1);
    assert!(tomb < batch.as_of, "前提: 削除は as_of より前 (ホストはもう差分で受けない)");
    assert!(batch.complete);
    let deletes: Vec<(u64, Hlc, PeerId)> = batch
        .records
        .iter()
        .filter_map(|r| match &r.op {
            DecodedOp::Delete { eid } => Some((*eid, r.hlc, r.author_peer)),
            _ => None,
        })
        .collect();

    all(&[c1, c2, c3]);
    assert_eq!(w.rows(1), [false; 4], "c1 が c2 の行 1 を消した分が、 state の取り直しの後 [c1, host, c2, c3] で揃わない");
    assert_eq!(deletes, vec![(world_row, tomb, 1)], "state の Delete は c2 の行 1 の世界番号宛て、 c1 の record、 版数は削除の版数");
    assert_eq!(w.vs(1), [None; 4]);
    assert_eq!(w.vs(2), [Some(20); 4], "c1 の自分の行はそのまま");
    assert_eq!(w.vs(3), [Some(30); 4], "c2 のもう 1 つの行はそのまま");
}

/// 形 2: c1 が c2 の行の `v` を untie し、 出す前にホストが c1 の state を取り直す。 untie は全員に届く (行は残る)。
#[test]
fn an_unpublished_untie_on_another_clients_row_survives_a_state_resync() {
    let w = seeded("untie-foreign");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let world_row = make_eid(2, eid_local(c2.row(3).unwrap()));
    let (_, at) = c1.untie_v(3);
    assert!(at.peer == 1 && at > host_cursor(&w.host, 1), "前提: untie は c1 の時計の版数で、 ホストはまだ受けていない ({at:?})");
    assert_eq!(w.vs(3), [None, Some(30), Some(30), Some(30)], "前提: v は c1 でだけ外れている");

    let batch = push_state(&w.host, c1);
    assert!(at < batch.as_of, "前提: untie は as_of より前 (ホストはもう差分で受けない)");
    let v = c1.himo("notes.v");
    let unties: Vec<(u64, u16, Hlc, PeerId)> = batch
        .records
        .iter()
        .filter_map(|r| match &r.op {
            DecodedOp::Untie { eid, himo_id } => Some((*eid, *himo_id, r.hlc, r.author_peer)),
            _ => None,
        })
        .collect();

    all(&[c1, c2, c3]);
    assert_eq!(w.vs(3), [None; 4], "c1 が c2 の行 3 の v を外した分が、 state の取り直しの後 [c1, host, c2, c3] で揃わない");
    assert_eq!(unties, vec![(world_row, v, at, 1)], "state の Untie は c2 の行 3 の世界番号宛て、 c1 の record、 版数は untie の版数");
    assert_eq!(w.rows(3), [true; 4], "行 3 そのものは残る");
    assert_eq!(w.vs(1), [Some(10); 4]);
    assert_eq!(w.vs(2), [Some(20); 4]);
}

/// 形 3: c1 が自分の行の `v` を untie し、 出す前にホストが c1 の state を取り直す。 untie は全員に届く (行は残る)。
#[test]
fn an_unpublished_untie_on_the_clients_own_row_survives_a_state_resync() {
    let w = seeded("untie-own");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let (row, at) = c1.untie_v(2);
    assert!(at.peer == 1 && at > host_cursor(&w.host, 1), "前提: untie は c1 の時計の版数で、 ホストはまだ受けていない ({at:?})");
    assert_eq!(w.vs(2), [None, Some(20), Some(20), Some(20)], "前提: v は c1 でだけ外れている");

    let batch = push_state(&w.host, c1);
    assert!(at < batch.as_of, "前提: untie は as_of より前 (ホストはもう差分で受けない)");
    let v = c1.himo("notes.v");

    all(&[c1, c2, c3]);
    assert_eq!(w.vs(2), [None; 4], "c1 が自分の行 2 の v を外した分が、 state の取り直しの後 [c1, host, c2, c3] で揃わない");
    assert!(
        batch.records.iter().any(|r| r.hlc == at && r.author_peer == 1 && matches!(&r.op, DecodedOp::Untie { eid, himo_id } if *eid == row && *himo_id == v)),
        "state に自分の行 2 の v の Untie がある"
    );
    assert_eq!(w.rows(2), [true; 4], "行 2 そのものは残る");
    assert_eq!(w.vs(1), [Some(10); 4]);
    assert_eq!(w.vs(3), [Some(30); 4]);
}

/// state に入る Untie / Delete は自分の時計の物だけ: c2 が外した cell・c2 が消した行 (c1 は受けただけ) は c1 の state に入らない。
/// c1 が外した後に c2 が書き直した cell は値のある cell で、 Untie は出ない。 消えた行の cell の Untie も出ない。
#[test]
fn only_unties_and_deletes_made_by_the_client_are_in_its_state() {
    let w = seeded("only-mine");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    c2.create(4, 40);
    c2.create(5, 50);
    c1.create(6, 60);
    all(&[c1, c2, c3]);
    // c2 が自分の行 1 の v を外し、 自分の行 4 を消す。 c1 が c2 の行 3 の v を外し、 その後 c2 が書き直す。
    // c1 が c2 の行 5 の v を外してから行 5 を消し、 自分の行 6 の v を外してから行 6 を消す
    c2.untie_v(1);
    c2.eng.delete(c2.row(4).unwrap());
    c2.eng.oplog_commit();
    tick();
    c1.untie_v(3);
    all(&[c1, c2, c3]);
    c2.eng.tie_to(c2.row(3).unwrap(), "notes.v", 31u32);
    c2.eng.oplog_commit();
    tick();
    let world5 = make_eid(2, eid_local(c2.row(5).unwrap()));
    let (row5, _) = c1.untie_v(5);
    c1.eng.delete(row5);
    let (row6, _) = c1.untie_v(6);
    c1.eng.delete(row6);
    c1.eng.oplog_commit();
    tick();
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(1), [None; 4]);
    assert_eq!(w.rows(1), [true; 4]);
    assert_eq!(w.vs(3), [Some(31); 4]);
    assert_eq!(w.rows(4), [false; 4]);
    assert_eq!(w.rows(5), [false; 4]);
    assert_eq!(w.rows(6), [false; 4]);

    let batch = push_state(&w.host, c1);
    assert!(batch.records.iter().all(|r| r.author_peer == 1), "c1 が作った state の record は全部 c1 の record");
    assert!(batch.records.iter().all(|r| r.hlc != Hlc::ZERO));
    let unties: Vec<u64> = batch.records.iter().filter_map(|r| match &r.op { DecodedOp::Untie { eid, .. } => Some(*eid), _ => None }).collect();
    assert_eq!(unties, Vec::<u64>::new(), "c2 が外した cell・書き直された cell・消えた行の cell の Untie は入らない");
    let deletes: Vec<u64> = batch.records.iter().filter_map(|r| match &r.op { DecodedOp::Delete { eid } => Some(*eid), _ => None }).collect();
    assert_eq!(deletes, vec![world5], "Delete は c1 が消した c2 の行 5 だけ (c2 が消した行 4・c1 が消した自分の行 6 は入らない)");

    all(&[c1, c2, c3]);
    assert_eq!(w.vs(1), [None; 4]);
    assert_eq!(w.rows(1), [true; 4]);
    assert_eq!(w.vs(2), [Some(20); 4]);
    assert_eq!(w.vs(3), [Some(31); 4]);
    assert_eq!(w.rows(4), [false; 4]);
    assert_eq!(w.rows(5), [false; 4]);
    assert_eq!(w.rows(6), [false; 4]);
}

/// 他の peer の行宛ての Untie / Delete が、 state に無い自分の行の掃除 (ghost sweep) を止めない: c1 が自分の行を消し、 同じ番号
/// (local) の c2 の行を untie する / 消す。 ホストは c1 の消した行を掃除する (c2 の行宛ての record の番号を c1 の行と取り違えない)。
#[test]
fn an_untie_or_delete_of_another_clients_row_does_not_shield_a_deleted_row_from_the_sweep() {
    let w = world("sweep");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    // c1 と c2 が、 それぞれ自分の DB で同じ番号の行を 2 つずつ作る
    let (mine_a, mine_b) = (c1.create(5, 50), c1.create(7, 70));
    let (theirs_a, theirs_b) = (c2.create(6, 60), c2.create(8, 80));
    assert_eq!((eid_local(mine_a), eid_local(mine_b)), (eid_local(theirs_a), eid_local(theirs_b)), "前提: c1 と c2 の行は、 それぞれの DB で同じ番号 (local)");
    all(&[c1, c2, c3]);
    for (k, v) in [(5, 50), (6, 60), (7, 70), (8, 80)] {
        assert_eq!(w.vs(k), [Some(v); 4]);
    }

    // c1 が自分の行 5 / 7 を消し、 c2 の行 6 の v を外し、 c2 の行 8 を消す (どれも出さない)
    c1.eng.delete(c1.row(5).unwrap());
    c1.eng.delete(c1.row(7).unwrap());
    c1.untie_v(6);
    c1.eng.delete(c1.row(8).unwrap());
    c1.eng.oplog_commit();
    tick();
    let batch = push_state(&w.host, c1);
    let (world_a, world_b) = (make_eid(2, eid_local(theirs_a)), make_eid(2, eid_local(theirs_b)));
    assert!(batch.records.iter().any(|r| matches!(&r.op, DecodedOp::Untie { eid, .. } if *eid == world_a)), "前提: state に c2 の行 6 宛ての Untie がある (番号は c1 の消した行 5 と同じ)");
    assert!(batch.records.iter().any(|r| matches!(&r.op, DecodedOp::Delete { eid } if *eid == world_b)), "前提: state に c2 の行 8 宛ての Delete がある (番号は c1 の消した行 7 と同じ)");
    let mine = [make_eid(1, eid_local(mine_a)), make_eid(1, eid_local(mine_b))];
    assert!(
        !batch.records.iter().any(|r| match &r.op {
            DecodedOp::Tie { eid, .. } | DecodedOp::Untie { eid, .. } | DecodedOp::Delete { eid } | DecodedOp::TieLeaf { eid, .. } => mine.contains(eid),
            _ => false,
        }),
        "前提: state に c1 の消した行 5 / 7 宛ての record は無い"
    );

    all(&[c1, c2, c3]);
    assert_eq!(w.rows(5), [false; 4], "c1 が消した行 5 が [c1, host, c2, c3] のどこかに残った");
    assert_eq!(w.rows(7), [false; 4], "c1 が消した行 7 が [c1, host, c2, c3] のどこかに残った");
    assert_eq!(w.vs(6), [None; 4]);
    assert_eq!(w.rows(6), [true; 4]);
    assert_eq!(w.rows(8), [false; 4]);
}

/// 消した他の peer の行の slot を使い回した後 (写像は外れ、 削除の版数は退避表にある) でも、 その削除は state に入る。
#[test]
fn a_delete_of_another_clients_row_survives_after_its_slot_is_reused() {
    let w = seeded("reuse");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let row = c1.row(1).unwrap();
    let their_local = eid_local(c2.row(1).unwrap());
    let world_row = make_eid(2, their_local);
    assert_eq!(c1.eng.eid_translator().get(2, their_local), Some(eid_local(row)), "前提: c1 は c2 の行 1 を翻訳した slot に持つ");
    c1.eng.delete(row);
    c1.eng.oplog_commit();
    tick();
    let tomb = c1.eng.tombstone_hlc(row);
    assert!(tomb.peer == 1 && tomb > host_cursor(&w.host, 1), "前提: 削除は c1 の時計の版数で、 ホストはまだ受けていない");
    // c1 が、 消した行の slot が使い回されるまで自分の行を作る (表の枠を使い切ると空いた slot を使う)
    let h = c1.himo("notes.k");
    let mut made = 0u32;
    while c1.eng.eid_translator().get(2, their_local).is_some() {
        assert!(made < 20_000, "前提: 消した行の slot が使い回される");
        let e = c1.eng.entity_in("notes").unwrap();
        c1.eng.tie_to_by_id(e, h, 1_000 + made);
        made += 1;
    }
    c1.eng.oplog_commit();
    tick();
    assert_eq!(w.rows(1), [false, true, true, true], "前提: 行 1 は c1 でだけ消えている");

    let batch = push_state(&w.host, c1);
    assert!(tomb < batch.as_of);
    let deletes: Vec<(u64, Hlc)> =
        batch.records.iter().filter_map(|r| match &r.op { DecodedOp::Delete { eid } => Some((*eid, r.hlc)), _ => None }).collect();

    all(&[c1, c2, c3]);
    assert_eq!(w.rows(1), [false; 4], "slot を使い回した後の、 c2 の行 1 の削除が [c1, host, c2, c3] で揃わない");
    assert_eq!(deletes, vec![(world_row, tomb)]);
    assert_eq!(w.vs(1_000 + made - 1), [None; 4], "c1 が作った行は v を持たない");
    assert_eq!(w.rows(1_000 + made - 1), [true; 4], "c1 が slot を使い回して作った行は全員に届く");
    assert_eq!(w.vs(3), [Some(30); 4]);
}
