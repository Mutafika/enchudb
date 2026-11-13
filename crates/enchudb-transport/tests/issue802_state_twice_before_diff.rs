//! #802: ホストが、 client の state のうち版数が `as_of` より後の record (書き手が state を作っている間に書いた cell) を 「当てて
//! 載せない」 (#753、 差分で届いた時に載せる) で済ませた後、 その差分が届く前に **もう一度** その client の state を取り直した時。
//!
//! 2 回目の state の同じ record は、 ホストではもう同じ版数で当たっていて当たらず、 「当たった物だけ載せる」 ので載らなかった。
//! ホストの `cursor[client]` は 2 回目の `as_of` (その record より後) に進み、 差分でも二度と受けない = その書き込みはホストと
//! 書き手にだけあり、 他の client に届かなかった。
//!
//! 直し: 取り直す前の cursor より後・as_of 以下の、 書き手自身の record で、 当たらなかったが cell の版数がその record の版数と
//! 同じ物は載せる (前の state が当てて載せなかった物。 差分で受けた物は cursor 以下)。
//!
//! 道具は `issue784_state_own_deletes.rs` と同じ (「次の 1 回の state の `as_of` を指定する」 口 = state を作り始めたのが書き込みの
//! 前で、 走査が書き込みの後だった形を作る)。
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue802-{tag}-{}", std::process::id()));
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


/// ホストの ring の record のうち、 書き手 `author` の、 版数 `h` の物の数。
fn ring_count(eng: &Engine, author: PeerId, h: Hlc) -> usize {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| r.author_peer == author && r.hlc == h && !matches!(r.op, DecodedOp::Vocab { .. }))
        .count()
}

/// 「作り始めは書き込みの前、 走査は書き込みの後」 の state の as_of: `h` (書き込みの版数) のすぐ前。
fn just_before(h: Hlc, floor: Hlc) -> Hlc {
    let as_of = Hlc { wall: h.wall - 1, logical: 0, peer: 1 };
    assert!(floor < as_of && as_of < h, "前提: as_of は前の書き込みより後、 見る書き込みより前");
    as_of
}

/// c1 の行 2 の列 `col` に `write` で書き、 1 回目の state (as_of は書き込みの前) → 差分が届く前に 2 回目の state。 書き込みの
/// 版数と、 2 回の state の後のホストの ring にその版数の record が何本あるかを返す。
fn two_states(w: &World, col: &str, write: impl FnOnce(&Client, u64)) -> (Hlc, usize) {
    let c1 = &w.c1;
    let row = c1.row(2).unwrap();
    let before = host_cursor(&w.host, 1);
    tick();
    write(c1, row);
    c1.eng.oplog_commit();
    tick();
    let h = c1.eng.cell_hlc(row, c1.himo(col));
    *c1.as_of_once.lock().unwrap() = Some(just_before(h, before));
    let batch = push_state(&w.host, c1);
    assert!(batch.records.iter().any(|r| r.hlc == h), "前提: 1 回目の state は as_of より後のその record を持つ");
    assert_eq!(ring_count(&w.host_eng, 1, h), 0, "前提: ホストは as_of より後の record を載せていない (差分に任せた)");
    assert_eq!(w.host_eng.cell_hlc(w.host_eng.pull_raw("notes.k", 2)[0], c1.himo(col)), h, "前提: ホストは当てた");
    // 差分が届く前に、 もう一度取り直す
    let batch = push_state(&w.host, c1);
    assert!(batch.as_of > h && batch.records.iter().any(|r| r.hlc == h), "前提: 2 回目の state は as_of 以下のその record を持つ");
    (h, ring_count(&w.host_eng, 1, h))
}

/// issue の再現: Number の cell。
#[test]
fn a_tie_left_to_the_diff_is_relayed_by_the_next_state() {
    let w = seeded("tie");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let (h, n) = two_states(&w, "notes.v", |c, row| c.eng.tie_to(row, "notes.v", 99u32));
    assert_eq!(n, 1, "2 回目の state の record をホストが載せない");
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(2), [Some(99); 4], "state を作っている間に書いた cell が、 2 回目の取り直しの後、 他の client に届かない");
    assert_eq!(ring_count(&w.host_eng, 1, h), 1, "差分で届いた同じ record をもう一度載せた");
}

/// Tag の cell: 語の record (Vocab) も一緒に届く (下の受け手が翻訳できる)。
#[test]
fn a_tag_left_to_the_diff_is_relayed_with_its_word() {
    let w = seeded("tag");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let (_, n) = two_states(&w, "notes.t", |c, row| c.eng.tie_text_to(row, "notes.t", "late word"));
    assert_eq!(n, 1, "2 回目の state の record をホストが載せない");
    all(&[c1, c2, c3]);
    assert_eq!(w.everywhere(2).map(|c| c.1), [(); 4].map(|_| Some("late word".to_string())), "Tag の cell が他の client に届かない");
}

/// 前からある語を指す Tag の cell (語の record はホストがもう持っている)。
#[test]
fn a_tag_pointing_at_a_known_word_is_relayed_with_its_word() {
    let w = seeded("tag-known");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    c1.eng.tie_text_to(c1.row(2).unwrap(), "notes.body", "x");
    let other = c1.create(7, 70);
    c1.eng.tie_text_to(other, "notes.t", "old word");
    all(&[c1, c2, c3]);
    assert_eq!(w.everywhere(7).map(|c| c.1), [(); 4].map(|_| Some("old word".to_string())), "前提: 語は全員に届いている");
    let (_, n) = two_states(&w, "notes.t", |c, row| c.eng.tie_text_to(row, "notes.t", "old word"));
    assert_eq!(n, 1, "2 回目の state の record をホストが載せない");
    all(&[c1, c2, c3]);
    assert_eq!(w.everywhere(2).map(|c| c.1), [(); 4].map(|_| Some("old word".to_string())), "Tag の cell が他の client に届かない");
}

/// Leaf の cell。
#[test]
fn a_leaf_left_to_the_diff_is_relayed_by_the_next_state() {
    let w = seeded("leaf");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let (_, n) = two_states(&w, "notes.body", |c, row| c.eng.tie_text_to(row, "notes.body", "late body"));
    assert_eq!(n, 1, "2 回目の state の record をホストが載せない");
    all(&[c1, c2, c3]);
    assert_eq!(w.everywhere(2).map(|c| c.2), [(); 4].map(|_| Some("late body".to_string())), "Leaf の cell が他の client に届かない");
}

/// 外した cell (Untie)。
#[test]
fn an_untie_left_to_the_diff_is_relayed_by_the_next_state() {
    let w = seeded("untie");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let (_, n) = two_states(&w, "notes.v", |c, row| c.eng.untie(row, "notes.v"));
    assert_eq!(n, 1, "2 回目の state の record をホストが載せない");
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(2), [None; 4], "外した cell が他の client に残った");
    assert_eq!(w.rows(2), [true; 4], "行 2 は残る");
}

/// state を作っている間に作った行 (名前のカードも as_of より後)。
#[test]
fn a_row_created_during_the_state_is_relayed_by_the_next_state() {
    let w = seeded("row");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let before = host_cursor(&w.host, 1);
    tick();
    let e = c1.eng.entity_in("notes").unwrap();
    c1.eng.tie_to(e, "notes.k", 9u32);
    c1.eng.tie_to(e, "notes.v", 90u32);
    c1.eng.tie_text_to(e, "notes.t", "nine");
    c1.eng.oplog_commit();
    tick();
    let h = c1.eng.cell_hlc(e, c1.himo("notes.k"));
    *c1.as_of_once.lock().unwrap() = Some(just_before(h, before));
    push_state(&w.host, c1);
    assert_eq!(w.rows(9), [true, true, false, false], "前提: 1 回目の後、 行 9 はホストと c1 にだけある");
    assert_eq!(ring_count(&w.host_eng, 1, h), 0, "前提: ホストは名前のカードを載せていない");
    push_state(&w.host, c1);
    all(&[c1, c2, c3]);
    assert_eq!(w.rows(9), [true; 4], "state を作っている間に作った行が、 他の client に届かない");
    assert_eq!(w.everywhere(9), [(); 4].map(|_| (Some(90), Some("nine".to_string()), None)));
}

/// 対照: 取り直しが 1 回だけなら、 載せるのは差分で届いた時の 1 回だけ (state からは載せない = #753)。
#[test]
fn a_single_state_still_leaves_the_record_to_the_diff() {
    let w = seeded("once");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let row = c1.row(2).unwrap();
    let before = host_cursor(&w.host, 1);
    tick();
    c1.eng.tie_to(row, "notes.v", 99u32);
    c1.eng.oplog_commit();
    tick();
    let h = c1.eng.cell_hlc(row, c1.himo("notes.v"));
    *c1.as_of_once.lock().unwrap() = Some(just_before(h, before));
    push_state(&w.host, c1);
    assert_eq!(ring_count(&w.host_eng, 1, h), 0, "as_of より後の record を state から載せた (#753)");
    all(&[c1, c2, c3]);
    assert_eq!(ring_count(&w.host_eng, 1, h), 1, "差分で届いた record を載せるのは 1 回");
    assert_eq!(w.vs(2), [Some(99); 4]);
}

/// 対照: もう差分で受けて載せてある record (取り直す前の cursor 以下) は、 取り直しても載せ直さない。
#[test]
fn a_record_already_relayed_by_the_diff_is_not_relayed_again() {
    let w = seeded("settled");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let row = c1.row(2).unwrap();
    tick();
    c1.eng.tie_to(row, "notes.v", 99u32);
    c1.eng.oplog_commit();
    tick();
    let h = c1.eng.cell_hlc(row, c1.himo("notes.v"));
    all(&[c1, c2, c3]);
    assert_eq!(ring_count(&w.host_eng, 1, h), 1, "前提: 差分で載った");
    push_state(&w.host, c1);
    push_state(&w.host, c1);
    assert_eq!(ring_count(&w.host_eng, 1, h), 1, "差分で載せてある record を、 取り直しで載せ直した");
    assert_eq!(w.vs(2), [Some(99); 4]);
}

/// 他の client (c2) が c1 の行に書いた cell (版数は c2 の時計) は、 c1 の state に入っていても c1 の record として載せない
/// (c2 の差分で届いて載っている)。
#[test]
fn a_cell_written_by_another_client_is_not_relayed_from_the_state() {
    let w = seeded("foreign");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    let v = c1.himo("notes.v");
    tick();
    c2.eng.tie_to(c2.row(2).unwrap(), "notes.v", 55u32);
    c2.eng.oplog_commit();
    tick();
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(2), [Some(55); 4], "前提: c2 の書き込みが全員に届いた");
    let h = c1.eng.cell_hlc(c1.row(2).unwrap(), v);
    assert_eq!(h.peer, 2, "前提: c1 の行 2 の v の版数は c2 の時計");
    let n = ring_count(&w.host_eng, 1, h) + ring_count(&w.host_eng, 2, h);
    push_state(&w.host, c1);
    push_state(&w.host, c1);
    assert_eq!(ring_count(&w.host_eng, 1, h) + ring_count(&w.host_eng, 2, h), n, "他の client の書いた cell を state から載せた");
}

/// 名前で束ねた行 (c1 と c2 が同じ名前 5 で別々に作って 1 行になった。 ホストには c2 の行が先に届いていて、 c1 の行の番号は
/// その別名) の cell でも同じ。
#[test]
fn a_tie_on_a_bound_row_is_relayed_by_the_next_state() {
    let w = seeded("bound");
    let (c1, c2, c3) = (&w.c1, &w.c2, &w.c3);
    c1.create(5, 51);
    c2.create(5, 52);
    c2.sync();
    c1.sync();
    all(&[c1, c2, c3]);
    let count = |e: &Engine| e.pull_raw("notes.k", 5).len();
    assert_eq!([count(&c1.eng), count(&w.host_eng), count(&c2.eng), count(&c3.eng)], [1; 4], "前提: 名前 5 は全員で 1 行");
    let row = c1.row(5).unwrap();
    let before = host_cursor(&w.host, 1);
    tick();
    c1.eng.tie_to(row, "notes.v", 99u32);
    c1.eng.oplog_commit();
    tick();
    let h = c1.eng.cell_hlc(row, c1.himo("notes.v"));
    *c1.as_of_once.lock().unwrap() = Some(just_before(h, before));
    push_state(&w.host, c1);
    assert_eq!(ring_count(&w.host_eng, 1, h), 0, "前提: ホストは as_of より後の record を載せていない");
    assert_eq!(w.vs(5), [Some(99), Some(99), Some(52), Some(52)], "前提: ホストは当てた");
    push_state(&w.host, c1);
    assert_eq!(ring_count(&w.host_eng, 1, h), 1, "2 回目の state の record をホストが載せない");
    all(&[c1, c2, c3]);
    assert_eq!(w.vs(5), [Some(99); 4], "束ねた行の cell が他の client に届かない");
}

/// 1 回目と 2 回目の間にホストを開き直す (cursor の置き場のあるホスト)。 cursor と cell の版数は残るので、 同じに載せる。
#[test]
fn the_record_is_relayed_after_the_host_reopens() {
    let dir = Dir::new("reopen");
    let cursors = PathBuf::from(format!("{}.cursors", dir.db(HOST)));
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
    let addr = host.addr();
    let url = format!("http://{addr}");
    let (c1, c3) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(3), 3), &url));
    c1.create(2, 20);
    all(&[&c1, &c3]);
    assert_eq!(c3.v(2), Some(20), "前提: 行 2 が c3 に届いた");
    let row = c1.row(2).unwrap();
    let before = host_cursor(&host, 1);
    tick();
    c1.eng.tie_to(row, "notes.v", 99u32);
    c1.eng.oplog_commit();
    tick();
    let h = c1.eng.cell_hlc(row, c1.himo("notes.v"));
    let as_of = just_before(h, before);
    *c1.as_of_once.lock().unwrap() = Some(as_of);
    push_state(&host, &c1);
    assert_eq!(ring_count(&host_eng, 1, h), 0, "前提: ホストは as_of より後の record を載せていない");
    // ホストを閉じて、 同じ口で開き直す
    drop(host);
    assert_eq!(Arc::strong_count(&host_eng), 1, "前提: ホストの engine を閉じられる");
    drop(host_eng);
    let host_eng = Engine::open_concurrent_with_oplog(&dir.db(HOST), 16 << 20).unwrap();
    host_eng.set_peer_id(HOST);
    let host = HttpHost::start(&addr.to_string(), host_eng.clone(), Some(cursors)).unwrap();
    assert_eq!(host_cursor(&host, 1), as_of, "前提: 開き直したホストの c1 の cursor は 1 回目の as_of");
    assert_eq!(host_eng.get(host_eng.pull_raw("notes.k", 2)[0], "notes.v"), Some(99), "前提: 当てた値は残っている");
    push_state(&host, &c1);
    assert_eq!(ring_count(&host_eng, 1, h), 1, "開き直した後の 2 回目の state の record をホストが載せない");
    all(&[&c1, &c3]);
    assert_eq!(c3.v(2), Some(99), "state を作っている間に書いた cell が、 ホストを開き直した後の取り直しで c3 に届かない");
    drop((c1, c3, host));
}

/// 対照: 当たらなかったのが 「同じ版数でもう当たっている」 からでなく、 **負けた** から (ホストにはもっと新しい c2 の書き込みが
/// ある) の record は、 今までどおり state からは載せない (state の record は当たった物だけ)。
#[test]
fn a_losing_record_is_not_relayed_from_the_state() {
    let w = seeded("lose");
    let (c1, c2) = (&w.c1, &w.c2);
    let row = c1.row(2).unwrap();
    tick();
    c1.eng.tie_to(row, "notes.v", 99u32);
    c1.eng.oplog_commit();
    tick();
    let h = c1.eng.cell_hlc(row, c1.himo("notes.v"));
    // c2 の後からの書き込みが、 先にホストに届く (c1 はまだ受けていない)
    c2.eng.tie_to(c2.row(2).unwrap(), "notes.v", 55u32);
    c2.eng.oplog_commit();
    c2.push();
    let host_row = w.host_eng.pull_raw("notes.k", 2)[0];
    assert!(w.host_eng.cell_hlc(host_row, c1.himo("notes.v")) > h, "前提: ホストの cell は c1 の書き込みより新しい");
    assert!(host_cursor(&w.host, 1) < h, "前提: c1 の書き込みはホストの cursor より後");
    let batch = push_state(&w.host, c1);
    assert!(batch.as_of > h && batch.records.iter().any(|r| r.hlc == h), "前提: state は as_of 以下のその record を持つ");
    assert_eq!(ring_count(&w.host_eng, 1, h), 0, "負けた record を state から載せた");
    assert_eq!(w.host_eng.get(host_row, "notes.v"), Some(55));
}

/// 対照: cursor の置き場の無いホストを開き直した後 (c1 の cursor が無い) の取り直しでは、 差分で受けて載せてある record を
/// 載せ直さない (cursor が無いと、 前の state が載せなかった物と見分けられない。 見分けずに載せると state の全部を載せ直す)。
#[test]
fn a_host_without_a_cursor_does_not_relay_the_whole_state_again() {
    let dir = Dir::new("nocursor");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let addr = host.addr();
    let c1 = client(db(&dir.db(1), 1), &format!("http://{addr}"));
    c1.create(2, 20);
    c1.sync();
    let h = c1.eng.cell_hlc(c1.row(2).unwrap(), c1.himo("notes.v"));
    assert_eq!(ring_count(&host_eng, 1, h), 1, "前提: 差分で載った");
    drop(host);
    assert_eq!(Arc::strong_count(&host_eng), 1, "前提: ホストの engine を閉じられる");
    drop(host_eng);
    let host_eng = Engine::open_concurrent_with_oplog(&dir.db(HOST), 16 << 20).unwrap();
    host_eng.set_peer_id(HOST);
    let host = HttpHost::start(&addr.to_string(), host_eng.clone(), None).unwrap();
    assert_eq!(host_cursor(&host, 1), Hlc::ZERO, "前提: 開き直したホストに c1 の cursor は無い");
    assert_eq!(ring_count(&host_eng, 1, h), 1, "前提: ring の行は残っている");
    let batch = push_state(&host, &c1);
    assert!(batch.records.iter().any(|r| r.hlc == h), "前提: state にその record がある");
    assert_eq!(ring_count(&host_eng, 1, h), 1, "cursor の無いホストが、 載せてある record を state から載せ直した");
    drop((c1, host));
}
