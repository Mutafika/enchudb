//! #783 (ホストの側): ホストが取り直した state で、 自分の `cursor[書き手]` 以下の版数の record を初めて受けて載せた時、 その書き手の
//! floor を上げて下の client に配る。 下の client は抜けを知り、 ホストの持つその書き手の state を取り直す。
//!
//! 形は手で作る (`http_host.rs` の `the_host_asks_a_client_for_its_state` と同じ): c1 の書き込み X の record を送らずに、 その後の
//! Y だけ送る (X が WAL に載らずに落ち、 Y が floor より先に配られた形)。 その後 c1 の floor が上がり、 ホストが c1 の state を
//! 取り直す。 道具は `issue784_state_own_deletes.rs` と同じ。
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue783h-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `v` (Number)、 `t` (Tag)、 `body` (Leaf)、 `r` (Ref) を持つ表 `notes`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        e.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
        e.define_himo_in("notes", "r", ValueType::Ref, 0).unwrap();
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
    /// 立てると、 state から自分の行の Delete を外す (この直しより前の client が送る state の形)。
    legacy: Arc<std::sync::atomic::AtomicBool>,
    /// 次の 1 回の state の `as_of` をこの値にする (state を作り始めたのがその時刻だった、 という形)。
    as_of_once: Arc<Mutex<Option<Hlc>>>,
}

/// state の配布元が 「engine の state をそのまま」 (`Syncer::serve_state` が自分の分として登録する物と同じ中身) の client。
fn client(eng: Arc<Engine>, url: &str) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t.clone());
    let sent: Arc<Mutex<Vec<StateBatch>>> = Arc::new(Mutex::new(Vec::new()));
    let legacy = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let as_of_once: Arc<Mutex<Option<Hlc>>> = Arc::new(Mutex::new(None));
    {
        let (weak, sent, legacy, as_of_once) = (Arc::downgrade(&eng), sent.clone(), legacy.clone(), as_of_once.clone());
        t.register_state_provider(
            eng.peer_id(),
            Arc::new(move || {
                let eng = weak.upgrade()?;
                let me = eng.peer_id();
                let (mut records, as_of) = eng.state_records();
                if legacy.load(std::sync::atomic::Ordering::Acquire) {
                    records.retain(|r| !matches!(&r.op, DecodedOp::Delete { eid } if (*eid >> 32) as u32 == me));
                }
                let as_of = as_of_once.lock().unwrap().take().unwrap_or(as_of);
                let batch = StateBatch { records, as_of, complete: true };
                sent.lock().unwrap().push(batch.clone());
                Some(batch)
            }),
        );
    }
    Client { eng, t, s, sent, legacy, as_of_once }
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
    url: String,
    dir: Dir,
}

fn world(tag: &str) -> World {
    let dir = Dir::new(tag);
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2, c3) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url), client(db(&dir.db(3), 3), &url));
    World { c1, c2, c3, host, host_eng, url, dir }
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


fn ring_of(eng: &Engine) -> Vec<enchudb::transport::WireRecord> {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(enchudb::transport::WireRecord::from)
        .collect()
}

impl Client {
    /// ホストから受ける。 抜けを言われたら、 その書き手の state をホストから取り直して、 もう一度受ける (アプリがする事)。
    /// 取り直した書き手を返す。
    fn pull_or_refetch(&self) -> Vec<PeerId> {
        let out = self.s.pull_once(HOST);
        let authors = out.truncated_authors.clone();
        for a in &authors {
            assert!(self.s.bootstrap_pull_via(HOST, *a).is_some(), "前提: ホストは書き手 {a} の state を配れる");
        }
        if out.history_truncated {
            assert!(!authors.is_empty(), "前提: 抜けた書き手が分かる");
            let again = self.s.pull_once(HOST);
            assert!(!again.history_truncated, "取り直した後も抜けを言われる (client {})", self.me());
        }
        authors
    }
}

/// c1 が `x` を書き (送らない)、 その後に行 4 の v を書いて (Y) それだけ送る。 Y が全員に届いた後、 ホストが c1 の state を取り直す。
/// 戻り値は (c2 が取り直した書き手, c3 が取り直した書き手)。
fn drop_x_then_state(w: &World, x: impl FnOnce(&Client)) -> (Vec<PeerId>, Vec<PeerId>) {
    w.c1.create(4, 40);
    all(&[&w.c1, &w.c2, &w.c3]);
    drop_x_then_state_y(w, x)
}

/// [`drop_x_then_state`] の、 行 4 (Y を書く行) がもうある版 (c1 が自分の行を持たない世界など)。
fn drop_x_then_state_y(w: &World, x: impl FnOnce(&Client)) -> (Vec<PeerId>, Vec<PeerId>) {
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let v = c1.himo("notes.v");
    let row4 = c1.row(4).unwrap();
    let before = host_cursor(&w.host, 1);
    tick();
    x(c1);
    c1.eng.oplog_commit();
    tick();
    c1.eng.tie_to(row4, "notes.v", 41u32);
    c1.eng.oplog_commit();
    tick();
    let h_y = c1.eng.cell_hlc(row4, v);
    c1.eng.oplog_sync().unwrap();
    let y: Vec<_> = ring_of(&c1.eng).into_iter().filter(|r| r.hlc == h_y).collect();
    assert_eq!(y.len(), 1, "前提: Y の record");
    c1.t.publish_to(1, HOST, y);
    assert_eq!(host_cursor(&w.host, 1), h_y, "前提: ホストは Y を受けた (X は受けていない)");
    c3.pull();
    c2.pull();
    assert_eq!(w.vs(4), [Some(41); 4], "前提: Y は全員に届いた");
    let cursors = [c2, c3].map(|c| c.s.cursors_of(HOST));
    let floors = w.host.syncer().late_state_floors();
    let batch = push_state(&w.host, c1);
    assert!(batch.records.iter().any(|r| r.author_peer == 1 && r.hlc > before && r.hlc < h_y), "前提: state に X がある");
    assert_eq!(w.host.syncer().late_state_floors(), floors + 1, "ホストが、 cursor 以下の record を載せたのに floor を上げない");
    // ホストの持つ c1 の state: c1 の時計の版数の物は c1 の record、 c1 の行でない行宛ての物は c1 の時計の版数だけ、
    // 同じ宛先は 1 回だけ
    let replica = c2.t.fetch_state(1).expect("前提: ホストは c1 の state を配れる");
    assert!(!replica.complete, "ホストの持つ state は掃除をさせない");
    assert_eq!(replica.as_of, batch.as_of, "ホストの持つ c1 の state の as_of は、 ホストの c1 の cursor");
    let mut seen = std::collections::HashSet::new();
    for r in &replica.records {
        if r.hlc.peer == 1 {
            assert_eq!(r.author_peer, 1, "c1 の時計の版数の物が、 他の書き手の record: {r:?}");
        }
        let key = match &r.op {
            DecodedOp::Tie { eid, himo_id, .. } | DecodedOp::Untie { eid, himo_id } | DecodedOp::TieRef { eid, himo_id, .. } => {
                (*eid, *himo_id as u32)
            }
            DecodedOp::TieLeaf { eid, himo_name, .. } => (*eid, c1.himo(himo_name) as u32),
            DecodedOp::Delete { eid } => (*eid, u32::MAX),
            _ => continue,
        };
        if (key.0 >> 32) as u32 != 1 {
            assert_eq!((r.author_peer, r.hlc.peer), (1, 1), "c1 の行でない行宛てに、 c1 の時計でない版数の record: {r:?}");
        }
        assert!(seen.insert(key), "同じ宛先が 2 回: {r:?}");
    }
    // ホストの floor は pull の返事で届く (その返事の c1 の record は渡されず、 次の pull の前の判定が抜けを言う)
    let mut got = (c2.pull_or_refetch(), c3.pull_or_refetch());
    for _ in 0..3 {
        c1.eng.oplog_commit();
        c1.push();
        assert!(c1.pull_or_refetch().is_empty(), "c1 は自分の floor で抜けを言われない");
        for (c, g) in [(c2, &mut got.0), (c3, &mut got.1)] {
            c.eng.oplog_commit();
            c.push();
            g.extend(c.pull_or_refetch());
        }
    }
    // 取り直しは c1 の cursor だけを動かす (他の書き手の cursor は、 差分で受けた分しか進まない = 取り直す前より後ろには戻らない)
    for (c, before) in [c2, c3].into_iter().zip(cursors) {
        let now = c.s.cursors_of(HOST);
        for (a, h) in before {
            let n = now.iter().find(|(p, _)| *p == a).map(|(_, h)| *h).unwrap();
            assert!(n >= h, "client {} の cursor[{a}] が戻った", c.me());
        }
        assert_eq!(now.iter().find(|(p, _)| *p == 1).map(|(_, h)| *h), Some(host_cursor(&w.host, 1)), "c1 の cursor はホストの cursor");
    }
    got
}

/// 値の cell (Number)。 issue の `explore7` の形。
#[test]
fn a_dropped_tie_recovered_by_state_reaches_the_other_clients() {
    let w = seeded("tie");
    let got = drop_x_then_state(&w, |c1| c1.eng.tie_to(c1.row(2).unwrap(), "notes.v", 21u32));
    assert_eq!(got, (vec![1], vec![1]), "c2 / c3 は c1 の抜けを言われて、 1 回だけ取り直す");
    assert_eq!(w.vs(2), [Some(21); 4], "state で戻った書き込み X が、 他の client に届かない");
    assert_eq!(w.vs(4), [Some(41); 4]);
    assert_eq!(w.vs(1), [Some(10); 4]);
}

/// Tag の cell (語の record ごと)。
#[test]
fn a_dropped_tag_recovered_by_state_reaches_the_other_clients() {
    let w = seeded("tag");
    drop_x_then_state(&w, |c1| c1.eng.tie_text_to(c1.row(2).unwrap(), "notes.t", "dropped word"));
    assert_eq!(w.everywhere(2).map(|c| c.1), [(); 4].map(|_| Some("dropped word".to_string())));
}

/// Leaf の cell。
#[test]
fn a_dropped_leaf_recovered_by_state_reaches_the_other_clients() {
    let w = seeded("leaf");
    drop_x_then_state(&w, |c1| c1.eng.tie_text_to(c1.row(2).unwrap(), "notes.body", "dropped body"));
    assert_eq!(w.everywhere(2).map(|c| c.2), [(); 4].map(|_| Some("dropped body".to_string())));
}

/// 新しい行。
#[test]
fn a_dropped_new_row_recovered_by_state_reaches_the_other_clients() {
    let w = seeded("row");
    drop_x_then_state(&w, |c1| {
        let e = c1.eng.entity_in("notes").unwrap();
        c1.eng.tie_to(e, "notes.k", 9u32);
        c1.eng.tie_to(e, "notes.v", 90u32);
    });
    assert_eq!(w.vs(9), [Some(90); 4]);
}

/// 自分の行の untie (replica の state が、 書き手の時計の版数の外した cell を Untie で運ぶ)。
#[test]
fn a_dropped_untie_of_an_own_row_reaches_the_other_clients() {
    let w = seeded("untie");
    let got = drop_x_then_state(&w, |c1| c1.eng.untie(c1.row(2).unwrap(), "notes.v"));
    assert_eq!(got, (vec![1], vec![1]));
    assert_eq!(w.vs(2), [None; 4], "自分の行の untie");
    assert_eq!(w.rows(2), [true; 4], "行は残る");
}

/// 自分の行の削除 (書き手の時計の削除の版数を持つ、 書き手の行の Delete)。
#[test]
fn a_dropped_delete_of_an_own_row_reaches_the_other_clients() {
    let w = seeded("delete");
    let got = drop_x_then_state(&w, |c1| c1.eng.delete(c1.row(2).unwrap()));
    assert_eq!(got, (vec![1], vec![1]));
    assert_eq!(w.rows(2), [false; 4], "自分の行の削除");
    for k in [1, 3, 4] {
        assert_eq!(w.rows(k), [true; 4], "他の行 {k} は残る");
    }
}

/// 他の client (c2) の行への書き込み (Number / Tag / Leaf)。 c2 は自分の行宛ての c1 の record を受け、 c3 は c2 の行の写しに受ける。
#[test]
fn a_dropped_write_to_another_clients_row_reaches_the_other_clients() {
    let w = seeded("foreign");
    let got = drop_x_then_state(&w, |c1| {
        let row = c1.row(1).unwrap();
        c1.eng.tie_to(row, "notes.v", 11u32);
        c1.eng.tie_text_to(row, "notes.t", "c1 word");
        c1.eng.tie_text_to(row, "notes.body", "c1 body");
    });
    assert_eq!(got, (vec![1], vec![1]));
    let want = (Some(11), Some("c1 word".to_string()), Some("c1 body".to_string()));
    assert_eq!(w.everywhere(1), [(); 4].map(|_| want.clone()), "他の client (c2) の行への書き込み");
    assert_eq!(w.vs(3), [Some(30); 4]);
}

/// 他の client (c2) の行の untie。
#[test]
fn a_dropped_untie_of_another_clients_row_reaches_the_other_clients() {
    let w = seeded("foreign-untie");
    let got = drop_x_then_state(&w, |c1| c1.eng.untie(c1.row(1).unwrap(), "notes.v"));
    assert_eq!(got, (vec![1], vec![1]));
    assert_eq!(w.vs(1), [None; 4], "他の client (c2) の行の untie");
    assert_eq!(w.rows(1), [true; 4]);
}

/// 他の client (c2) の行の削除。
#[test]
fn a_dropped_delete_of_another_clients_row_reaches_the_other_clients() {
    let w = seeded("foreign-delete");
    let got = drop_x_then_state(&w, |c1| c1.eng.delete(c1.row(1).unwrap()));
    assert_eq!(got, (vec![1], vec![1]));
    assert_eq!(w.rows(1), [false; 4], "他の client (c2) の行の削除");
    for k in [2, 3, 4] {
        assert_eq!(w.rows(k), [true; 4], "他の行 {k} は残る");
    }
}

/// 書き手が自分の行を 1 つも持たない (他の client の行に書くだけ)。 ホストは、 行の写像を持たない書き手の state も配る。
#[test]
fn a_writer_with_no_rows_of_its_own_is_refetched_too() {
    let w = world("norows");
    for (k, v) in [(1, 10), (3, 30), (4, 40)] {
        w.c2.create(k, v);
    }
    all(&[&w.c1, &w.c2, &w.c3]);
    w.c1.eng.tie_to(w.c1.row(3).unwrap(), "notes.v", 31u32);
    all(&[&w.c1, &w.c2, &w.c3]);
    assert_eq!(w.vs(3), [Some(31); 4], "前提: c1 の書き込みが全員に届いた");
    assert!(!w.host_eng.replicated_authors().contains(&1), "前提: ホストに c1 の行は無い");
    let got = drop_x_then_state_y(&w, |c1| c1.eng.tie_to(c1.row(1).unwrap(), "notes.v", 11u32));
    assert_eq!(got, (vec![1], vec![1]));
    assert_eq!(w.vs(1), [Some(11); 4]);
}

/// floor が上がった後、 書き手の書いた cell が全部他の client に書き換えられた = ホストの持つその書き手の state は空。 取り直しは
/// 空の state で済む (配れないと、 下の client はその書き手の抜けを言われ続ける)。
#[test]
fn a_refetch_of_a_writer_whose_cells_were_all_overwritten_ends() {
    let w = world("empty");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    for (k, v) in [(1, 10), (4, 40)] {
        c2.create(k, v);
    }
    all(&[c1, c2, c3]);
    let v = c1.himo("notes.v");
    let (row1, row4) = (c1.row(1).unwrap(), c1.row(4).unwrap());
    tick();
    c1.eng.tie_to(row1, "notes.v", 11u32);
    c1.eng.oplog_commit();
    tick();
    c1.eng.tie_to(row4, "notes.v", 41u32);
    c1.eng.oplog_commit();
    tick();
    let h_y = c1.eng.cell_hlc(row4, v);
    c1.eng.oplog_sync().unwrap();
    let y: Vec<_> = ring_of(&c1.eng).into_iter().filter(|r| r.hlc == h_y).collect();
    c1.t.publish_to(1, HOST, y);
    c2.pull();
    c3.pull();
    push_state(&w.host, c1);
    assert_eq!(w.host.syncer().late_state_floors(), 1, "前提: ホストは c1 の floor を上げた");
    assert!(c2.pull_or_refetch().is_empty(), "floor はこの pull の返事で届く");
    assert_eq!(c2.pull_or_refetch(), vec![1]);
    assert_eq!(c2.v(1), Some(11), "前提: c2 は取り直して X を受けた");
    // c2 が c1 の書いた cell を全部書き換える。 c3 はまだ取り直していない
    c2.eng.tie_to(c2.row(1).unwrap(), "notes.v", 12u32);
    c2.eng.tie_to(c2.row(4).unwrap(), "notes.v", 42u32);
    c2.sync();
    assert!(w.host_eng.state_records_for(1).0.is_empty(), "前提: ホストの持つ c1 の state は空");
    let mut got = c3.pull_or_refetch();
    got.extend(c3.pull_or_refetch());
    got.extend(c3.pull_or_refetch());
    assert_eq!(got, vec![1], "c3 は c1 を 1 回取り直して済む");
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(1), [Some(12); 4]);
    assert_eq!(w.vs(4), [Some(42); 4]);
}

/// ホスト自身の行 (ホストが自分で書く形) への書き込み・untie・削除。 宛名はホストの番号。
#[test]
fn dropped_writes_to_a_row_of_the_host_reach_the_other_clients() {
    let seeded_with_host_row = |tag: &str| {
        let w = seeded(tag);
        let e = w.host_eng.entity_in("notes").unwrap();
        w.host_eng.tie_to(e, "notes.k", 7u32);
        w.host_eng.tie_to(e, "notes.v", 70u32);
        w.host_eng.oplog_commit();
        w.host_eng.oplog_sync().unwrap();
        tick();
        all(&[&w.c1, &w.c2, &w.c3]);
        assert_eq!(w.vs(7), [Some(70); 4], "前提: ホストの行が全員に届いた");
        w
    };
    let w = seeded_with_host_row("host-tie");
    drop_x_then_state(&w, |c1| {
        let row = c1.row(7).unwrap();
        c1.eng.tie_to(row, "notes.v", 71u32);
        c1.eng.tie_text_to(row, "notes.t", "on the host row");
    });
    assert_eq!(w.vs(7), [Some(71); 4], "ホストの行への書き込み");
    assert_eq!(w.everywhere(7).map(|c| c.1), [(); 4].map(|_| Some("on the host row".to_string())));
    let w = seeded_with_host_row("host-untie");
    drop_x_then_state(&w, |c1| c1.eng.untie(c1.row(7).unwrap(), "notes.v"));
    assert_eq!(w.vs(7), [None; 4], "ホストの行の untie");
    assert_eq!(w.rows(7), [true; 4]);
    let w = seeded_with_host_row("host-delete");
    drop_x_then_state(&w, |c1| c1.eng.delete(c1.row(7).unwrap()));
    assert_eq!(w.rows(7), [false; 4], "ホストの行の削除");
}

/// 消して、 同じ名前で足し直す (新しいデッキ)。 削除は前のデッキだけを消し、 足し直した行が残る (自分の行 / 他の client の行)。
#[test]
fn a_dropped_delete_then_re_add_under_the_same_name_keeps_the_new_row() {
    for (tag, k) in [("readd-own", 2u32), ("readd-foreign", 1u32)] {
        let w = seeded(tag);
        let old = [w.c1.row(k), w.c2.row(k), w.c3.row(k)];
        drop_x_then_state(&w, |c1| {
            c1.eng.delete(c1.row(k).unwrap());
            c1.eng.oplog_commit();
            tick();
            let e = c1.eng.entity_in("notes").unwrap();
            c1.eng.tie_to(e, "notes.k", k);
            c1.eng.tie_to(e, "notes.v", 99u32);
        });
        assert_eq!(w.vs(k), [Some(99); 4], "{tag}: 足し直した行");
        for (c, old) in [&w.c1, &w.c2, &w.c3].into_iter().zip(old) {
            assert_ne!(c.row(k), old, "{tag}: client {} に前のデッキが残っている", c.me());
            assert_eq!(c.eng.pull_raw("notes.k", k).len(), 1, "{tag}: client {} で名前 {k} の行が 1 つでない", c.me());
        }
        assert_eq!(w.host_eng.pull_raw("notes.k", k).len(), 1, "{tag}: ホストで名前 {k} の行が 1 つでない");
    }
}

impl Client {
    /// 行 `k` の `r` が指す行の名前。
    fn r(&self, k: u32) -> Option<u64> {
        let to = self.eng.get(self.row(k)?, "notes.r")?;
        self.eng.get(enchudb_oplog::make_eid(0, to as u32), "notes.k")
    }
}

/// 他の行から張った ref: 指す先が 書き手自身の行 / 第三者の行 / ホストの行。
#[test]
fn dropped_refs_from_rows_of_others_reach_the_other_clients() {
    let w = seeded("ref");
    let e = w.host_eng.entity_in("notes").unwrap();
    w.host_eng.tie_to(e, "notes.k", 7u32);
    w.host_eng.tie_to(e, "notes.v", 70u32);
    w.host_eng.oplog_commit();
    w.host_eng.oplog_sync().unwrap();
    tick();
    all(&[&w.c1, &w.c2, &w.c3]);
    assert_eq!(w.vs(7), [Some(70); 4], "前提: ホストの行が全員に届いた");
    drop_x_then_state(&w, |c1| {
        let row = |k| c1.row(k).unwrap();
        c1.eng.tie_ref_to(row(1), "notes.r", row(2)); // c2 の行 → c1 の行
        c1.eng.tie_ref_to(row(3), "notes.r", row(1)); // c2 の行 → c2 の行
        c1.eng.tie_ref_to(row(7), "notes.r", row(7)); // ホストの行 → ホストの行
    });
    let host_r = |k: u32| {
        let row = w.host_eng.pull_raw("notes.k", k).first().copied()?;
        let to = w.host_eng.get(row, "notes.r")?;
        w.host_eng.get(enchudb_oplog::make_eid(0, to as u32), "notes.k")
    };
    for (k, to) in [(1, 2), (3, 1), (7, 7)] {
        assert_eq!([w.c1.r(k), host_r(k), w.c2.r(k), w.c3.r(k)], [Some(to); 4], "行 {k} の ref");
    }
}

/// 後から来た client: floor より前なので c1 の state を取り直し、 残りは差分で受ける。 混ぜた X (untie / 他の行への書き込み /
/// 他の行の削除 / 自分の行の削除) の後の中身が、 前から居る client と同じ。
#[test]
fn a_client_that_joins_after_the_floor_ends_with_the_same_rows() {
    let w = seeded("join");
    w.c1.create(5, 50);
    w.c2.create(6, 60);
    all(&[&w.c1, &w.c2, &w.c3]);
    drop_x_then_state(&w, |c1| {
        c1.eng.untie(c1.row(2).unwrap(), "notes.v");
        c1.eng.tie_to(c1.row(1).unwrap(), "notes.v", 11u32);
        c1.eng.delete(c1.row(3).unwrap());
        c1.eng.delete(c1.row(5).unwrap());
        c1.eng.untie(c1.row(6).unwrap(), "notes.v");
    });
    let c4 = client(db(&w.dir.db(4), 4), &w.url);
    let mut refetched = Vec::new();
    for _ in 0..4 {
        refetched.extend(c4.pull_or_refetch());
    }
    assert!(refetched.contains(&1), "前提: c4 は c1 を取り直した ({refetched:?})");
    for k in 1..=6 {
        let want = w.c1.cells(k);
        assert_eq!(w.everywhere(k), [(); 4].map(|_| want.clone()), "行 {k}");
        assert_eq!(c4.cells(k), want, "後から来た client の行 {k}");
        assert_eq!(w.rows(k), [w.c1.has_row(k); 4]);
        assert_eq!(c4.has_row(k), w.c1.has_row(k), "後から来た client の行 {k} の有無");
    }
    assert_eq!(w.rows(3), [false; 4]);
    assert_eq!(w.rows(5), [false; 4]);
    assert_eq!(w.vs(2), [None; 4]);
    assert_eq!(w.vs(6), [None; 4]);
    drop(c4);
}

/// #711 の形と一緒に: c1 の行を c2 が書き換えた cell (版数は c2 の時計、 c1 の state の 「c1 の行」 の側) と、 c1 が c2 の行に書いた
/// cell (版数は c1 の時計、 c1 の state の 「他の行」 の側)。 同じ cell が c1 の state に 2 回出ない (`drop_x_then_state` が見る)。
/// c2 の state にも、 c2 が c1 の行に書いた cell が c2 の record で入る (どちらを先に取り直しても同じ答え)。
#[test]
fn a_row_rewritten_by_another_client_and_a_write_to_its_row_do_not_collide() {
    let w = seeded("cross");
    w.c2.eng.tie_to(w.c2.row(2).unwrap(), "notes.v", 25u32);
    w.c2.eng.tie_text_to(w.c2.row(2).unwrap(), "notes.t", "c2 word");
    all(&[&w.c1, &w.c2, &w.c3]);
    assert_eq!(w.vs(2), [Some(25); 4], "前提: c2 の書き換えが全員に届いた");
    let got = drop_x_then_state(&w, |c1| {
        c1.eng.tie_to(c1.row(1).unwrap(), "notes.v", 11u32);
        c1.eng.tie_text_to(c1.row(1).unwrap(), "notes.t", "c1 word");
    });
    assert_eq!(got, (vec![1], vec![1]));
    let v = w.c1.himo("notes.v") as u32;
    let dest = |r: &enchudb::transport::WireRecord| match &r.op {
        DecodedOp::Tie { eid, himo_id, .. } => Some((r.author_peer, r.hlc.peer, *eid >> 32, *himo_id as u32)),
        _ => None,
    };
    let of1: Vec<_> = w.c3.t.fetch_state(1).unwrap().records.iter().filter_map(dest).filter(|d| d.3 == v).collect();
    let of2: Vec<_> = w.c3.t.fetch_state(2).unwrap().records.iter().filter_map(dest).filter(|d| d.3 == v).collect();
    // (書き手, 版数の時計, 行の持ち主, 列)
    assert!(of1.contains(&(1, 2, 1, v)), "c1 の state: c1 の行を c2 が書き換えた cell {of1:?}");
    assert!(of1.contains(&(1, 1, 2, v)), "c1 の state: c1 が c2 の行に書いた cell {of1:?}");
    assert!(of2.contains(&(2, 2, 1, v)), "c2 の state: c2 が c1 の行に書いた cell {of2:?}");
    // c1 が c2 の行に書いた cell は、 c2 の state にも 「c2 の行の cell」 として出る (前からの形: 行の持ち主の state は、 その行の
    // 値のある cell を全部運ぶ)。 別の batch で、 同じ版数・同じ値 = 後から当てた方は当たらない
    assert!(of2.contains(&(2, 1, 2, v)), "c2 の state: c2 の行の、 c1 が書いた cell {of2:?}");
    // c2 の state も取り直す (順は c1 の後)
    for c in [&w.c2, &w.c3] {
        let me = c.me();
        if me != 2 {
            assert!(c.s.bootstrap_pull_via(HOST, 2).is_some());
        }
    }
    all(&[&w.c1, &w.c2, &w.c3]);
    assert_eq!(w.everywhere(1).map(|c| (c.0, c.1)), [(); 4].map(|_| (Some(11), Some("c1 word".to_string()))));
    assert_eq!(w.everywhere(2).map(|c| (c.0, c.1)), [(); 4].map(|_| (Some(25), Some("c2 word".to_string()))));
}

/// 対照: 取り直した state が cursor より後の record しか載せない時 (普通の取り直し = 書き手が落ちた後の record を先に配って
/// いない) は、 floor を上げない = 下の client は取り直さない。
#[test]
fn a_state_with_nothing_below_the_cursor_raises_no_floor() {
    let w = seeded("clean");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    tick();
    c1.eng.tie_to(c1.row(2).unwrap(), "notes.v", 21u32);
    c1.eng.oplog_commit();
    tick();
    push_state(&w.host, c1);
    push_state(&w.host, c1);
    assert_eq!(w.host.syncer().late_state_floors(), 0, "cursor より後の record だけの state で floor を上げた");
    for c in [c2, c3] {
        assert!(c.pull_or_refetch().is_empty());
        assert!(c.pull_or_refetch().is_empty(), "下の client が抜けを言われた");
    }
    assert_eq!(w.vs(2), [Some(21); 4]);
}

/// 値段 (`--ignored --nocapture`、 release で): c1 の 10 万行が全員に届いた後、 落ちた 1 件のためにホストが floor を上げ、 c3 が
/// c1 の state をホストから取り直す。
#[test]
#[ignore]
fn cost_of_a_refetch_100k_rows() {
    let dir = Dir::new("cost");
    let big = |path: &str, peer: PeerId| {
        {
            let mut e = Engine::create_with_cell_version(path, 400_000).unwrap();
            e.define_table("notes", 150_000).unwrap();
            e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
            e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
            e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
            e.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
            let k = e.himo_id("notes.k").unwrap() as u16;
            e.set_table_pk("notes", k).unwrap();
            e.enable_sync_tables().unwrap();
            e.flush().unwrap();
        }
        let e = Engine::open_concurrent_with_oplog(path, 256 << 20).unwrap();
        e.set_peer_id(peer);
        e
    };
    let host_eng = big(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c3) = (client(big(&dir.db(1), 1), &url), client(big(&dir.db(3), 3), &url));
    const N: u32 = 100_000;
    for chunk in 0..10 {
        for i in chunk * (N / 10)..(chunk + 1) * (N / 10) {
            let e = c1.eng.entity_in("notes").unwrap();
            c1.eng.tie_to(e, "notes.k", i);
            c1.eng.tie_to(e, "notes.v", i);
            c1.eng.tie_text_to(e, "notes.t", &format!("tag {}", i % 100));
            c1.eng.tie_text_to(e, "notes.body", &format!("body of the note number {i}"));
        }
        c1.sync();
        c3.sync();
    }
    all(&[&c1, &c3]);
    assert_eq!(c3.v(N - 1), Some((N - 1) as u64), "前提: 10 万行が c3 に届いた");
    // X (送らない) → Y (送る) → ホストが c1 の state を取り直す
    let (row_x, row_y) = (c1.row(1).unwrap(), c1.row(2).unwrap());
    tick();
    c1.eng.tie_to(row_x, "notes.v", 777u32);
    c1.eng.oplog_commit();
    tick();
    c1.eng.tie_to(row_y, "notes.v", 888u32);
    c1.eng.oplog_commit();
    tick();
    let h_y = c1.eng.cell_hlc(row_y, c1.himo("notes.v"));
    c1.eng.oplog_sync().unwrap();
    let y: Vec<_> = ring_of(&c1.eng).into_iter().filter(|r| r.hlc == h_y).collect();
    c1.t.publish_to(1, HOST, y);
    c3.pull();
    let t = std::time::Instant::now();
    let batch = push_state(&host, &c1);
    let host_ms = t.elapsed().as_millis();
    assert_eq!(host.syncer().late_state_floors(), 1);
    let own_bytes = enchudb::transport::encode_batch(&batch.records).len();
    assert!(c3.pull_or_refetch().is_empty());
    let replica = c3.t.fetch_state(1).unwrap();
    let bytes = enchudb::transport::encode_batch(&replica.records).len();
    let t = std::time::Instant::now();
    assert_eq!(c3.pull_or_refetch(), vec![1]);
    let ms = t.elapsed().as_millis();
    assert_eq!(c3.v(1), Some(777));
    println!(
        "[cost] 10 万行 (列 4 本): c1 の state {} record / {:.1} MB、 ホストが求めて当てるまで {host_ms} ms。 ホストの持つ c1 の state {} record / {:.1} MB、 c3 が抜けを知って取り直して当て終えるまで {ms} ms",
        batch.records.len(),
        own_bytes as f64 / 1e6,
        replica.records.len(),
        bytes as f64 / 1e6
    );
    // state を作る時間 (9 回、 最良 / 中央値、 ms) と大きさ。 普段 = c1 の untie / 削除が 0
    let time = |f: &dyn Fn() -> usize| {
        let mut ms: Vec<f64> = (0..9)
            .map(|_| {
                let t = std::time::Instant::now();
                std::hint::black_box(f());
                t.elapsed().as_secs_f64() * 1e3
            })
            .collect();
        ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        (ms[0], ms[4])
    };
    let size = |eng: &Arc<Engine>, a: PeerId| {
        let (r, _) = eng.state_records_for(a);
        (r.len(), enchudb::transport::encode_batch(&r).len() as f64 / 1e6)
    };
    let report = |what: &str| {
        let own = time(&|| c1.eng.state_records().0.len());
        let of1 = time(&|| host_eng.state_records_for(1).0.len());
        let of3 = time(&|| host_eng.state_records_for(3).0.len());
        let (n1, mb1) = size(&host_eng, 1);
        let (n3, mb3) = size(&host_eng, 3);
        println!(
            "[cost] {what}: c1 自身の state 最良 {:.2} / 中央値 {:.2} ms。 ホストの持つ c1 の state {:.2} / {:.2} ms ({n1} record / {mb1:.2} MB)。 ホストの持つ c3 (行なし) の state {:.2} / {:.2} ms ({n3} record / {mb3:.2} MB)",
            own.0, own.1, of1.0, of1.1, of3.0, of3.1
        );
    };
    report("普段 (untie / 削除 0)");
    // c1 が 1 万 cell を外し、 1 万行を消す (差分で全員に届く)
    for i in 10..10_010u32 {
        c1.eng.untie(c1.row(i).unwrap(), "notes.v");
    }
    for i in 20_000..30_000u32 {
        c1.eng.delete(c1.row(i).unwrap());
    }
    all(&[&c1, &c3]);
    assert_eq!(c3.v(10), None, "前提: untie が c3 に届いた");
    assert!(!c3.has_row(20_000), "前提: 削除が c3 に届いた");
    report("untie 1 万 + 削除 1 万の後");
    drop((c1, c3, host));
}
