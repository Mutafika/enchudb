//! #784 / #800: 書き手の state は、 自分の行の削除を、 削除の版数の `Delete` の record で運ぶ。
//!
//! 前は運ばず、 state に現れない書き手の行を、 受け手 (ホスト) の掃除が **今の時刻** の Delete で消していた。
//!
//! - #784: 名前で束ねた行 (c1 と c2 が同じ名前で別々に作って 1 行になった) を c1 が消し、 出す前に state を取り直すと、
//!   c2 の行が先に届いていたホストでは、 c1 の行の番号は c2 の行の別名で、 掃除が見つけられず、 行がホストと他の client に残った
//! - #800: c1 が自分の行を消した後に同じ名前の行が足されていると (c1 自身の足し直し / 削除を知らない c2 の追加)、 掃除の
//!   Delete の方が新しいので、 足された行も消えた (形 1 は全員から消え、 形 2 は答えが割れた)
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue784-{tag}-{}", std::process::id()));
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


fn n_rows(eng: &Engine, k: u32) -> usize {
    eng.pull_raw("notes.k", k).len()
}

impl World {
    fn counts(&self, k: u32) -> [usize; 4] {
        [n_rows(&self.c1.eng, k), n_rows(&self.host_eng, k), n_rows(&self.c2.eng, k), n_rows(&self.c3.eng, k)]
    }
}

/// 形 1: c1 が名前 7 の行 (全員に届いている) を消して同じ名前で足し直し、 どちらも出す前に (`via_state`) ホストが c1 の state を
/// 取り直す。 対照 (`!via_state`) は差分で出す。
fn readd_by_self(tag: &str, via_state: bool) -> World {
    let w = seeded(tag);
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let old = c1.create(7, 70);
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(7), [Some(70); 4], "前提: 行 7 が全員に届いた");
    c1.eng.delete(old);
    c1.eng.oplog_commit();
    tick();
    let new = c1.create(7, 77);
    assert_ne!(old, new, "前提: 足し直した行は別の行");
    assert_eq!(w.vs(7), [Some(77), Some(70), Some(70), Some(70)], "前提: 消して足し直したのは c1 だけ");
    if via_state {
        let batch = push_state(&w.host, c1);
        assert!(batch.complete);
        println!("[{tag}] state の後 (c1 はまだ受けていない): {:?} / 行の数 {:?}", w.vs(7), w.counts(7));
    }
    all(&[c1, c2, c3]);
    println!("[{tag}] 揃えた後: {:?} / 行の数 {:?} / deletes {:?}", w.vs(7), w.counts(7), ring_deletes(&w.host_eng));
    w
}

#[test]
fn control_self_readd_by_diff() {
    let w = readd_by_self("s1d", false);
    assert_eq!(w.vs(7), [Some(77); 4], "対照: 差分なら、 足し直した行 7 が全員にある");
    assert_eq!(w.counts(7), [1; 4]);
}

#[test]
fn self_readd_survives_a_state_resync() {
    let w = readd_by_self("s1s", true);
    assert_eq!(w.vs(7), [Some(77); 4], "c1 が消して足し直した行 7 が、 state の取り直しで消えた [c1, host, c2, c3]");
    assert_eq!(w.counts(7), [1; 4]);
    assert_eq!(w.vs(2), [Some(20); 4]);
}

/// 形 2: c1 が名前 7 の行を消し (出していない)、 c2 が同じ名前の行を足して全員 (c1 以外) に届いた後に、 c1 の state を取り直す。
fn readd_by_other(tag: &str, via_state: bool) -> World {
    let w = seeded(tag);
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let old = c1.create(7, 70);
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(7), [Some(70); 4], "前提: 行 7 が全員に届いた");
    c1.eng.delete(old);
    c1.eng.oplog_commit();
    tick();
    // c2 は c1 の行 7 を持ったまま、 同じ名前の行を足す
    c2.create(7, 77);
    all(&[c2, c3]);
    println!("[{tag}] c2 が足した後: {:?} / 行の数 {:?}", w.vs(7), w.counts(7));
    if via_state {
        let batch = push_state(&w.host, c1);
        assert!(batch.complete);
        println!("[{tag}] state の後: {:?} / 行の数 {:?}", w.vs(7), w.counts(7));
    }
    all(&[c1, c2, c3]);
    println!("[{tag}] 揃えた後: {:?} / 行の数 {:?} / deletes {:?}", w.vs(7), w.counts(7), ring_deletes(&w.host_eng));
    w
}

#[test]
fn control_other_readd_by_diff() {
    let w = readd_by_other("s2d", false);
    let v = w.vs(7);
    assert!(v.iter().all(|x| *x == v[0]), "対照: 差分で食い違う {v:?}");
}

#[test]
fn other_readd_matches_the_diff_answer_after_a_state_resync() {
    let d = readd_by_other("s2c", false).vs(7);
    let w = readd_by_other("s2s", true);
    let v = w.vs(7);
    assert!(v.iter().all(|x| *x == v[0]), "state の取り直しで食い違う {v:?}");
    assert_eq!(v, d, "state の取り直しの答えが、 差分の答えと違う");
}

impl Client {
    /// `create` の、 待たない版 (大量に作る時)。
    fn create_quick(&self, k: u32, v: u32) -> u64 {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        e
    }
}

/// c1 の state の中の、 自分の行の Delete (宛名の番号, 版数)。
fn own_deletes(batch: &StateBatch, me: PeerId) -> Vec<(u64, Hlc)> {
    batch
        .records
        .iter()
        .filter_map(|r| match &r.op {
            DecodedOp::Delete { eid } if (*eid >> 32) as u32 == me && r.author_peer == me => Some((*eid, r.hlc)),
            _ => None,
        })
        .collect()
}

/// #784 の形: c1 と c2 が同じ名前 5 の行を別々に作り、 全員で 1 行に束なった後、 c1 が束ねた行を消し、 出す前に state を取り直す。
/// `first` = ホストに先に届く方。
fn bound_row_delete(tag: &str, first: u32) {
    let w = seeded(tag);
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    c1.create(5, 51);
    c2.create(5, 52);
    if first == 1 {
        c1.sync();
        c2.sync();
    } else {
        c2.sync();
        c1.sync();
    }
    all(&[c1, c2, c3]);
    assert_eq!(w.counts(5), [1; 4], "前提: 名前 5 は全員で 1 行");
    let row = c1.row(5).unwrap();
    c1.eng.delete(row);
    c1.eng.oplog_commit();
    tick();
    let tomb = c1.eng.tombstone_hlc(row);
    assert_eq!(w.counts(5), [0, 1, 1, 1], "前提: 消したのは c1 だけ");
    let batch = push_state(&w.host, c1);
    assert!(batch.complete && tomb < batch.as_of);
    assert!(own_deletes(&batch, 1).iter().any(|d| d.1 == tomb), "c1 の state に、 消した行の Delete (版数 {tomb:?}) が無い: {:?}", own_deletes(&batch, 1));
    all(&[c1, c2, c3]);
    assert_eq!(w.counts(5), [0; 4], "束ねた行の削除が揃わない [c1, host, c2, c3]");
    assert_eq!(w.vs(2), [Some(20); 4], "c1 の他の行はそのまま");
    assert_eq!(w.vs(1), [Some(10); 4]);
}

#[test]
fn a_bound_row_delete_reaches_everyone_c1_first() {
    bound_row_delete("b1", 1);
}

#[test]
fn a_bound_row_delete_reaches_everyone_c2_first() {
    bound_row_delete("b2", 2);
}

/// 掃除との二重が無い: 消した行の Delete は書き手の物 (削除の版数) が 1 つだけホストの ring に載り、 ホストの掃除の Delete
/// (ホストが書き手、 今の時刻) は載らない。 取り直しを繰り返しても増えない。 語 (Tag) を持つ他の行はそのまま届く。
#[test]
fn the_state_delete_replaces_the_sweep() {
    let w = seeded("once");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let row = c1.row(2).unwrap();
    c1.eng.delete(row);
    c1.eng.oplog_commit();
    tick();
    let tomb = c1.eng.tombstone_hlc(row);
    // 同じ state に、 新しい語を持つ行 (Vocab + Tie) も入る
    let e = c1.create(8, 80);
    c1.eng.tie_text_to(e, "notes.t", "fresh-word");
    c1.eng.oplog_commit();
    tick();
    assert_eq!(w.rows(2), [false, true, true, true], "前提: 行 2 は c1 でだけ消えている");
    let batch = push_state(&w.host, c1);
    assert_eq!(own_deletes(&batch, 1), vec![(row, tomb)], "c1 の state の自分の行の Delete");
    assert_eq!(ring_deletes(&w.host_eng), vec![(row, tomb, 1)], "ホストの ring の Delete は c1 の物 1 つ (掃除の Delete は無い)");
    assert_eq!(w.host_eng.tombstone_hlc(w.host_eng.translated_locals_of(1).iter().find(|(f, _)| *f == row as u32).unwrap().1 as u64), tomb, "ホストの行の削除の版数は c1 の削除の版数");
    for _ in 0..2 {
        push_state(&w.host, c1);
        assert_eq!(ring_deletes(&w.host_eng), vec![(row, tomb, 1)], "取り直しで Delete が増えた");
    }
    all(&[c1, c2, c3]);
    assert_eq!(w.rows(2), [false; 4], "c1 が消した行 2 が残る");
    assert_eq!(w.everywhere(8), [(Some(80), Some("fresh-word".to_string()), None), (Some(80), Some("fresh-word".to_string()), None), (Some(80), Some("fresh-word".to_string()), None), (Some(80), Some("fresh-word".to_string()), None)]);
    assert_eq!(w.vs(1), [Some(10); 4]);
    assert_eq!(w.vs(3), [Some(30); 4]);
}

/// ホストが見た事の無い行 (作って消した、 どちらも出していない) の Delete も、 答えを変えない。 その名前は後で足せる。
#[test]
fn a_delete_of_a_row_the_host_never_saw() {
    let w = seeded("never");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let row = c1.create(9, 90);
    c1.eng.delete(row);
    c1.eng.oplog_commit();
    tick();
    let batch = push_state(&w.host, c1);
    assert_eq!(own_deletes(&batch, 1).len(), 1, "前提: state に Delete が 1 つ");
    all(&[c1, c2, c3]);
    assert_eq!(w.rows(9), [false; 4]);
    c2.create(9, 99);
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(9), [Some(99); 4], "消した後に足した名前 9 の行");
    assert_eq!(w.counts(9), [1; 4]);
}

/// 開き直した後も、 削除の版数は slot に残っていて state に入る。
#[test]
fn the_delete_is_in_the_state_after_a_reopen() {
    let dir = Dir::new("reopen");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c2 = client(db(&dir.db(2), 2), &url);
    let (row, tomb) = {
        let c1 = client(db(&dir.db(1), 1), &url);
        let row = c1.create(7, 70);
        all(&[&c1, &c2]);
        assert_eq!(c2.v(7), Some(70), "前提: 行 7 が c2 に届いた");
        c1.eng.delete(row);
        c1.eng.oplog_commit();
        c1.create(7, 77);
        c1.eng.oplog_sync().unwrap();
        (row, c1.eng.tombstone_hlc(row))
    };
    let e = Engine::open_concurrent_with_oplog(&dir.db(1), 16 << 20).unwrap();
    e.set_peer_id(1);
    let c1 = client(e, &url);
    let (records, _) = c1.eng.state_records();
    let batch = StateBatch { records, as_of: Hlc::ZERO, complete: true };
    assert_eq!(own_deletes(&batch, 1), vec![(row, tomb)], "開き直した c1 の state の Delete");
    let batch = push_state(&host, &c1);
    assert_eq!(own_deletes(&batch, 1), vec![(row, tomb)]);
    all(&[&c1, &c2]);
    assert_eq!([c1.v(7), host_eng.pull_raw("notes.k", 7).first().and_then(|e| host_eng.get(*e, "notes.v")), c2.v(7)], [Some(77); 3]);
    drop((c1, c2, host));
}

/// slot を使い回すと削除の版数は消える = state に Delete は出ない (数は増え続けない)。 その行は今までどおりホストの掃除か、
/// 同じ番号の新しい行の書き込みで決まる。
#[test]
fn reused_slots_carry_no_delete() {
    let w = seeded("reuse");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let rows: Vec<u64> = (0..20).map(|i| c1.create(100 + i, i)).collect();
    all(&[c1, c2, c3]);
    for r in &rows {
        c1.eng.delete(*r);
    }
    c1.eng.oplog_commit();
    tick();
    let (records, _) = c1.eng.state_records();
    let n = own_deletes(&StateBatch { records, as_of: Hlc::ZERO, complete: true }, 1).len();
    assert_eq!(n, 20, "前提: 消した 20 行の Delete が state にある");
    // 表の枠 (4096) を使い切ると、 空いた slot を使い回す
    c1.eng.oplog_sync().unwrap();
    let mut reused = 0;
    let mut made = 0u32;
    while reused < 20 {
        assert!(made < 5000, "前提: 消した行の slot が使い回されない (使い回した {reused})");
        let e = c1.create_quick(1000 + made, made);
        made += 1;
        // #731: 場所を使い回した行の eid は世代が違う (既定の DB の場所は下 24 bit)
        if rows.iter().any(|r| (*r as u32) & 0xFF_FFFF == (e as u32) & 0xFF_FFFF) {
            reused += 1;
            let (records, _) = c1.eng.state_records();
            let left = own_deletes(&StateBatch { records, as_of: Hlc::ZERO, complete: true }, 1);
            assert_eq!(left.len(), 20 - reused, "使い回した slot の Delete が state に残った / 使い回していない slot の Delete が消えた");
        }
    }
    c1.eng.oplog_commit();
    tick();
    push_state(&w.host, c1);
    all(&[c1, c2, c3]);
    for i in 0..20 {
        assert_eq!(w.rows(100 + i), [false; 4], "消した行 {} が残る", 100 + i);
    }
    for i in [0u32, made - 1] {
        assert_eq!(w.vs(1000 + i), [Some(i as u64); 4], "使い回した後に作った行 {}", 1000 + i);
    }
}

/// state の大きさ: ホストが配る 「忘れてよい線」 より前の自分の行の削除は、 `Syncer` が配る state に入れない (ホストはもう受けて
/// いる)。 線を知らない間と、 線より後の削除は入る。
#[test]
fn deletes_before_the_forget_line_are_left_out() {
    let dir = Dir::new("line");
    let cursors: PathBuf = format!("{}.cursors", dir.db(HOST)).into();
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors)).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    let old = c1.create(7, 70);
    all(&[&c1, &c2]);
    c1.eng.delete(old);
    c1.eng.oplog_commit();
    tick();
    let tomb = c1.eng.tombstone_hlc(old);
    assert_eq!(own_deletes(&c1.s.own_state(), 1), vec![(old, tomb)], "線を知らない間は、 消した行の Delete が state にある");
    // 全員が出し終え・受け終えて、 線が削除を越えるまで回す
    let t0 = std::time::Instant::now();
    let mut reclaimed = 0;
    while host.forget_line() <= tomb || reclaimed == 0 {
        assert!(t0.elapsed() < Duration::from_secs(60), "前提: ホストの忘れてよい線が削除を越えない / c1 の ring が回収されない: {:?} <= {tomb:?}、 回収 {reclaimed}", host.forget_line());
        c1.create(1000 + t0.elapsed().as_millis() as u32, 0);
        all(&[&c1, &c2]);
        // 線はホストの ring に残る record を越えない
        host_eng.reclaim_sync_ops();
        // c1 の ring (`_sync_ops`、 配らない表) の行も回収する: 回収した行の slot にも削除の版数が残るが、 state には出ない
        reclaimed += c1.eng.reclaim_sync_ops();
    }
    c1.pull();
    assert_eq!(c2.row(7), None, "前提: 削除は c2 に届いている");
    let (records, _) = c1.eng.state_records();
    assert_eq!(own_deletes(&StateBatch { records, as_of: Hlc::ZERO, complete: true }, 1), vec![(old, tomb)], "前提: engine の state には Delete がある");
    assert_eq!(own_deletes(&c1.s.own_state(), 1), vec![], "線より前の削除が、 配る state に入った");
    // 線より後の削除は入る
    let new = c1.create(8, 80);
    c1.eng.delete(new);
    c1.eng.oplog_commit();
    let tomb2 = c1.eng.tombstone_hlc(new);
    assert_eq!(own_deletes(&c1.s.own_state(), 1), vec![(new, tomb2)], "線より後の削除");
    drop((c1, c2, host));
}

/// この直しより前の client (state に自分の行の Delete が無い) の state は、 今までどおりホストの掃除が消す。 他の peer の行宛ての
/// Untie / Delete (番号が、 消した自分の行と同じ) を、 消した行と取り違えない (#761 / #766)。
#[test]
fn a_state_without_own_deletes_is_still_swept() {
    let w = world("legacy");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    c1.legacy.store(true, std::sync::atomic::Ordering::Release);
    let (mine_a, mine_b) = (c1.create(5, 50), c1.create(7, 70));
    let (theirs_a, theirs_b) = (c2.create(6, 60), c2.create(8, 80));
    assert_eq!((mine_a as u32, mine_b as u32), (theirs_a as u32, theirs_b as u32), "前提: c1 と c2 の行は、 それぞれの DB で同じ番号 (local)");
    all(&[c1, c2, c3]);
    for (k, v) in [(5, 50), (6, 60), (7, 70), (8, 80)] {
        assert_eq!(w.vs(k), [Some(v); 4]);
    }
    c1.eng.delete(c1.row(5).unwrap());
    c1.eng.delete(c1.row(7).unwrap());
    c1.untie_v(6);
    c1.eng.delete(c1.row(8).unwrap());
    c1.eng.oplog_commit();
    tick();
    let n = ring_deletes(&w.host_eng).len();
    let batch = push_state(&w.host, c1);
    assert_eq!(own_deletes(&batch, 1), vec![], "前提: state に c1 の自分の行の Delete は無い");
    assert!(batch.records.iter().any(|r| matches!(&r.op, DecodedOp::Delete { eid } if (*eid >> 32) as u32 == 2)), "前提: c2 の行 8 宛ての Delete はある");
    let swept: Vec<_> = ring_deletes(&w.host_eng)[n..].iter().filter(|d| d.2 == HOST).map(|d| d.0).collect();
    assert_eq!(swept.len(), 2, "ホストの掃除が、 c1 の消した行 5 / 7 を消す (ホストの Delete 2 つ): {swept:?}");
    all(&[c1, c2, c3]);
    assert_eq!(w.rows(5), [false; 4]);
    assert_eq!(w.rows(7), [false; 4]);
    assert_eq!(w.vs(6), [None; 4]);
    assert_eq!(w.rows(6), [true; 4]);
    assert_eq!(w.rows(8), [false; 4]);
}

/// state を作っている間に消した行 (削除の版数が as_of より後): ホストは当てるが載せない (#753、 差分に任せる)。 その差分が届く前に
/// もう一度取り直した時は、 2 回目の state の Delete を載せる (記録済みだからと載せないと、 cursor が削除を越えていて、 差分でも
/// 届かず、 他の client に行が残る)。
#[test]
fn a_delete_left_to_the_diff_is_relayed_by_the_next_state() {
    let w = seeded("twice");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let before = c1.eng.cell_hlc(c1.row(2).unwrap(), c1.himo("notes.v"));
    tick();
    let row = c1.row(2).unwrap();
    c1.eng.delete(row);
    c1.eng.oplog_commit();
    tick();
    let tomb = c1.eng.tombstone_hlc(row);
    // 1 回目: 作り始めは削除の前 (as_of < 削除の版数)、 走査は削除の後
    let as_of = Hlc { wall: tomb.wall - 1, logical: 0, peer: 1 };
    assert!(before < as_of && as_of < tomb);
    *c1.as_of_once.lock().unwrap() = Some(as_of);
    let n = ring_deletes(&w.host_eng).len();
    let batch = push_state(&w.host, c1);
    assert_eq!((batch.as_of, own_deletes(&batch, 1)), (as_of, vec![(row, tomb)]), "前提: 1 回目の state は as_of より後の Delete を持つ");
    assert_eq!(w.rows(2), [false, false, true, true], "前提: ホストは当てた");
    assert_eq!(ring_deletes(&w.host_eng).len(), n, "前提: ホストは as_of より後の Delete を載せていない (差分に任せた)");
    // 差分が届く前に、 もう一度取り直す
    let batch = push_state(&w.host, c1);
    assert!(batch.as_of > tomb && own_deletes(&batch, 1) == vec![(row, tomb)], "前提: 2 回目の state は as_of 以下の Delete を持つ");
    assert_eq!(ring_deletes(&w.host_eng)[n..].to_vec(), vec![(row, tomb, 1)], "2 回目の state の Delete をホストが載せない");
    all(&[c1, c2, c3]);
    assert_eq!(w.rows(2), [false; 4], "c1 が消した行 2 が他の client に残った");
    assert_eq!(w.vs(1), [Some(10); 4]);
}
