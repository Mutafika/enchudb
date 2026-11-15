//! #914 / #915: 自分の消えた行の場所を使い回した後も、 その行の削除は state に入り、 その行宛ての後からの削除・改名は名前の
//! 削除優先 (#551) を通る。
//!
//! #910 (#731 段 4) から、 場所を使い回すと自分の消えた行 X (世代 0 | 場所) と新しい行 Y (世代 1 | 場所) は別の番号になった。
//! 使い回す時に場所の削除の版数を消す (`clear_cell_versions`) ので、 前は X の削除がどこにも残らなかった:
//!
//! - #914: X の削除を取りこぼした peer R が state で取り直すと、 state に X の Delete が無い。 R は名前で Y を (削除を知らない)
//!   X に束ね、 掃除が X を今の時刻で消して Y ごと消え、 その削除を配った先 (P) でも名前で Y が消える。 Y は A でだけ生きる
//! - #915: B が X を消した (A の Y の作成より後の版数) 削除、 X を改名する書き込みは、 A の入口で捨てられ、 名前の削除優先が
//!   A でだけ働かない。 P と B では Y (改名なら同じ名前の行) が消え、 A でだけ生きる
//! - state の Ref: 消えた行を指したまま場所を使い回した ref は state に出せなかった (受け手では null、 差分で受けた peer では
//!   消えた行を指す ref = `company is null` の答えが割れる)
//!
//! 直し: 使い回す時に、 自分の消えた行の削除の版数と名前を identity (自分の peer, 世代 | 場所) に移し (#166 / #545 で翻訳した行に
//! しているのと同じ形)、 state の自分の削除として出す。 その identity 宛ての削除は名前の削除の時刻を上げ、 名前の書き込みは
//! 消えた行への改名と同じ扱い。

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};

const OPLOG: usize = 1 << 20;
const A: PeerId = 1;
const B: PeerId = 2;
const P: PeerId = 3;
const C: PeerId = 4;

static RUN: AtomicU64 = AtomicU64::new(0);

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let n = RUN.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("enchudb-issue914-{tag}-{}-{n}", std::process::id()));
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

/// 表 `notes` (枠 `cap`): 名前 `k` (Number)、 `v` (Number)、 同じ表を指す Ref `next`。
fn create_db(path: &str, cap: u32) {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("notes", cap).unwrap();
    eng.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
    eng.define_ref_in("notes", "next", "notes").unwrap();
    let k = eng.himo_id("notes.k").unwrap() as u16;
    eng.set_table_pk("notes", k).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
}

fn open(path: &str, peer: PeerId) -> Arc<Engine> {
    let eng = Engine::open_concurrent_with_oplog(path, OPLOG).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// 書き込みの HLC の壁時計を進める (別の peer の書き込みとの前後を、 書いた順にする)。
fn tick() {
    std::thread::sleep(Duration::from_millis(3));
}

struct Peer {
    eng: Arc<Engine>,
    s: Syncer,
    /// 取り直した (`bootstrap_pull`) 回数。
    resyncs: AtomicU64,
}

fn peer(dir: &Dir, tr: &Arc<InMemoryTransport>, id: PeerId, cap: u32) -> Peer {
    create_db(&dir.db(id), cap);
    attach(dir, tr, id)
}

/// 開いて Syncer を付ける (state を配る)。
fn attach(dir: &Dir, tr: &Arc<InMemoryTransport>, id: PeerId) -> Peer {
    let eng = open(&dir.db(id), id);
    let t: Arc<dyn Transport> = tr.clone();
    let s = Syncer::new(eng.clone(), t);
    s.serve_state();
    Peer { eng, s, resyncs: AtomicU64::new(0) }
}

impl Peer {
    fn create(&self, k: u32, v: u32) -> u64 {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
        tick();
        e
    }
    fn rows(&self, k: u32) -> Vec<u64> {
        self.eng.pull_raw("notes.k", k)
    }
    fn row(&self, k: u32) -> u64 {
        let r = self.rows(k);
        assert_eq!(r.len(), 1, "前提: peer {} に名前 {k} の行が 1 つ", self.eng.peer_id());
        r[0]
    }
    fn v(&self, k: u32) -> Vec<u64> {
        let mut out: Vec<u64> = self.rows(k).into_iter().filter_map(|e| self.eng.get(e, "notes.v")).collect();
        out.sort_unstable();
        out
    }
    fn push(&self) {
        self.eng.oplog_commit();
        self.eng.oplog_sync().unwrap();
        self.eng.transfer_oplog_to_sync_ops();
        self.s.publish_since(Hlc::ZERO);
    }
    /// `from` から受ける。 抜けを言われたら state で取り直す (アプリが `history_truncated` にする事)。
    fn pull(&self, from: PeerId) {
        let out = self.s.pull_once(from);
        if out.history_truncated {
            self.s.bootstrap_pull(from).expect("state を取れない");
            self.resyncs.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// 全員が全員から受けて配る (3 周)。
fn all(ps: &[&Peer]) {
    for _ in 0..3 {
        for p in ps {
            p.push();
        }
        for p in ps {
            for q in ps {
                if p.eng.peer_id() != q.eng.peer_id() {
                    p.pull(q.eng.peer_id());
                }
            }
        }
    }
}

/// `eng` の state の中の、 自分の行の Delete (宛名の番号, 版数)。
fn own_state_deletes(eng: &Engine) -> Vec<(u64, Hlc)> {
    let me = eng.peer_id();
    let (records, _) = eng.state_records();
    records
        .iter()
        .filter_map(|r| match &r.op {
            DecodedOp::Delete { eid } if enchudb_oplog::eid_peer(*eid) == me && r.author_peer == me => Some((*eid, r.hlc)),
            _ => None,
        })
        .collect()
}

/// A が名前 7 の行 X を消し、 同じ名前の Y を足して X の場所を使い回す。 戻り値は (X, Y, X の削除の版数)。
fn delete_and_reuse(a: &Peer, x: u64, k: u32, v: u32) -> (u64, Hlc) {
    a.eng.delete(x);
    a.eng.oplog_commit();
    let tomb = a.eng.tombstone_hlc(x);
    assert_ne!(tomb, Hlc::ZERO, "前提: X の削除の版数");
    tick();
    let y = a.create(k, v);
    assert_eq!(a.eng.eid_slot(y), a.eng.eid_slot(x), "前提: X の場所を使い回した");
    assert_ne!(y, x, "前提: 使い回した行は世代の違う番号");
    (y, tomb)
}

/// #914: A が名前 7 の X を消し、 同じ名前の Y を足して場所を使い回す。 P は差分で受け、 R は離れていて state で取り直す。
/// Y は 3 台で生き、 X は 3 台で消える。
///
/// master (dc642b2) では A の state に X の Delete が無く、 R は Y を (削除を知らない) X に束ねてから掃除で今の時刻に消し、 その削除で
/// P でも Y が消えた (Y は A でだけ生きる)。
#[test]
fn a_reused_row_survives_a_state_resync() {
    let dir = Dir::new("resync");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P] {
        tr.register_peer(id);
    }
    // R = B
    let (a, r, p) = (peer(&dir, &tr, A, 2), peer(&dir, &tr, B, 2), peer(&dir, &tr, P, 2));
    let x = a.create(7, 70);
    a.create(1, 10);
    all(&[&a, &r, &p]);
    assert_eq!([a.v(7), r.v(7), p.v(7)], [vec![70], vec![70], vec![70]], "前提: X が 3 台に届いた");
    // R が離れている間に、 A が X を消して同じ名前の Y を足し、 P に配る
    let (y, tomb) = delete_and_reuse(&a, x, 7, 77);
    a.push();
    p.pull(A);
    assert_eq!(p.v(7), vec![77], "前提: P は X の削除と Y を差分で受けた");
    assert_eq!(r.v(7), vec![70], "前提: R は X の削除を知らない");
    // R の cursor より後の A の record を回収した (R は抜けを言われて state で取り直す)
    let floor = a.eng.cell_hlc(y, a.eng.himo_id("notes.v").unwrap() as u16);
    assert!(a.eng.raise_history_floor(A, Hlc { logical: floor.logical + 1, ..floor }), "前提: floor を上げられない");
    a.push();
    r.pull(A);
    assert_eq!(r.resyncs.load(Ordering::Relaxed), 1, "前提: R は state で取り直した");
    all(&[&a, &r, &p]);
    eprintln!("#914 Y の v: A {:?} R {:?} P {:?}", a.v(7), r.v(7), p.v(7));
    assert_eq!([a.v(7), r.v(7), p.v(7)], [vec![77], vec![77], vec![77]], "Y が 3 台で生きていない [A, R, P]");
    assert_eq!(a.eng.get(y, "notes.v"), Some(77));
    assert_eq!([a.v(1), r.v(1), p.v(1)], [vec![10], vec![10], vec![10]], "他の行はそのまま");
    // 仕組み: A の state は、 使い回した場所の前の住人 X の Delete を、 X の番号と削除の版数で運ぶ
    assert!(own_state_deletes(&a.eng).contains(&(x, tomb)), "A の state に X の Delete が無い: {:?}", own_state_deletes(&a.eng));
}

/// #915: B が X を消し (Y の作成より後の版数)、 A は X を消して同じ名前の Y を足し場所を使い回していた。 B の削除は名前 7 の
/// デッキの削除 = その前に名前 7 になった Y も消える (削除優先、 #551)。 3 台で同じ答え (Y は消える)。
///
/// master では B の削除が A の入口で捨てられ、 Y は A でだけ生きた。 `reopen` は A を使い回しの後に開き直す (identity に移した
/// 削除の版数と名前が開き直しを越える)。
fn later_delete_by_another_peer(tag: &str, reopen: bool) {
    let dir = Dir::new(tag);
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P] {
        tr.register_peer(id);
    }
    let (mut a, b, p) = (peer(&dir, &tr, A, 2), peer(&dir, &tr, B, 2), peer(&dir, &tr, P, 2));
    let x = a.create(7, 70);
    a.create(1, 10);
    all(&[&a, &b, &p]);
    let (y, _) = delete_and_reuse(&a, x, 7, 77);
    a.push();
    tick();
    // B は A の削除と Y を知らずに X を消す (版数は Y の作成より後)
    let x_b = b.row(7);
    b.eng.delete(x_b);
    b.push();
    assert!(b.eng.tombstone_hlc(x_b) > a.eng.cell_hlc(y, a.eng.himo_id("notes.k").unwrap() as u16), "前提: B の削除は Y の作成より後");
    if reopen {
        drop(a);
        a = attach(&dir, &tr, A);
        assert_eq!(a.v(7), vec![77], "前提: 開き直した A に Y がある");
    }
    // P は A を先に、 B を後に受ける
    p.pull(A);
    p.pull(B);
    a.pull(B);
    all(&[&a, &b, &p]);
    eprintln!("#915 名前 7 の行: A {:?} B {:?} P {:?}", a.v(7), b.v(7), p.v(7));
    assert_eq!([a.v(7), b.v(7), p.v(7)], [vec![], vec![], vec![]], "B の削除 (Y の作成より後) で Y が 3 台で消えない [A, B, P]");
    assert!(!a.eng.is_live(y), "A で Y が生きている");
    assert_eq!([a.v(1), b.v(1), p.v(1)], [vec![10], vec![10], vec![10]], "他の行はそのまま");
}

#[test]
fn a_later_delete_by_another_peer_reaches_the_reused_name() {
    later_delete_by_another_peer("later-delete", false);
}

#[test]
fn a_later_delete_by_another_peer_reaches_the_reused_name_after_reopen() {
    later_delete_by_another_peer("later-delete-reopen", true);
}

/// #915 の改名の形: B が X を名前 8 に改名する (A の X の削除を知らずに)。 名前 8 の行 W は X の削除より前からある = 削除を先に
/// 受けた peer では、 消えた行への改名でその名前も消した名前になり、 W も消える (#551)。 3 台で同じ答え。
///
/// master では A がその改名を入口で捨て、 W は A でだけ生きた。
#[test]
fn a_rename_of_the_deleted_row_reaches_the_name() {
    let dir = Dir::new("rename");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P] {
        tr.register_peer(id);
    }
    let (a, b, p) = (peer(&dir, &tr, A, 3), peer(&dir, &tr, B, 3), peer(&dir, &tr, P, 3));
    let x = a.create(7, 70);
    a.create(1, 10);
    all(&[&a, &b, &p]);
    // W (名前 8) を足し (B はまだ受けない)、 X を消して名前 9 の Y で場所を使い回す
    a.create(8, 80);
    let (_y, _) = delete_and_reuse(&a, x, 9, 90);
    a.push();
    tick();
    // B は X を名前 8 に改名する
    let x_b = b.row(7);
    b.eng.tie_to(x_b, "notes.k", 8u32);
    b.push();
    p.pull(A);
    p.pull(B);
    a.pull(B);
    all(&[&a, &b, &p]);
    eprintln!("#915 改名: 名前 8 A {:?} B {:?} P {:?} / 名前 9 A {:?} B {:?} P {:?}", a.v(8), b.v(8), p.v(8), a.v(9), b.v(9), p.v(9));
    assert_eq!([a.v(8), b.v(8), p.v(8)], [vec![], vec![], vec![]], "消えた X への改名の名前 8 の行 W が 3 台で消えない [A, B, P]");
    assert_eq!([a.v(9), b.v(9), p.v(9)], [vec![90], vec![90], vec![90]], "使い回した行 Y (名前 9) はそのまま");
    assert_eq!([a.v(1), b.v(1), p.v(1)], [vec![10], vec![10], vec![10]], "他の行はそのまま");
}

/// #915 の改名の形 (後から届く行): 名前 8 の行 V を P が X の削除より前に作り、 出すのは最後。 消えた X への改名で名前 8 は X の
/// 削除の時刻に消した名前になる = 後から届いた V も、 消えたデッキの一員として 3 台で消える (A は改名を入口で捨てても、 名前を
/// 消した名前として覚える)。
#[test]
fn a_rename_of_the_deleted_row_reaches_a_row_that_arrives_later() {
    let dir = Dir::new("rename-late");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P] {
        tr.register_peer(id);
    }
    let (a, b, p) = (peer(&dir, &tr, A, 2), peer(&dir, &tr, B, 2), peer(&dir, &tr, P, 2));
    let x = a.create(7, 70);
    a.create(1, 10);
    all(&[&a, &b, &p]);
    // P が名前 8 の V を作る (まだ出さない)
    p.create(8, 80);
    let (_y, _) = delete_and_reuse(&a, x, 9, 90);
    a.push();
    tick();
    let x_b = b.row(7);
    b.eng.tie_to(x_b, "notes.k", 8u32);
    b.push();
    a.pull(B);
    p.pull(A);
    p.pull(B);
    b.pull(A);
    all(&[&a, &b, &p]);
    eprintln!("#915 改名 (後から届く行): 名前 8 A {:?} B {:?} P {:?}", a.v(8), b.v(8), p.v(8));
    assert_eq!([a.v(8), b.v(8), p.v(8)], [vec![], vec![], vec![]], "後から届いた名前 8 の行 V が 3 台で消えない [A, B, P]");
    assert_eq!([a.v(9), b.v(9), p.v(9)], [vec![90], vec![90], vec![90]], "使い回した行 Y (名前 9) はそのまま");
}

/// state の Ref (#910 の読み直しの問い 2): 行 r0 が消えた行 X を指したまま、 A が X の場所を使い回す。 差分で受けた P も、 state で
/// 始めた C も、 r0 は消えた行を指す (null でない、 使い回した Y を指さない)。
///
/// master では A の state に r0 の ref が無く (出せない)、 C でだけ null だった。
#[test]
fn a_ref_to_the_deleted_row_is_in_the_state() {
    let dir = Dir::new("ref");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, P, C] {
        tr.register_peer(id);
    }
    let (a, p) = (peer(&dir, &tr, A, 2), peer(&dir, &tr, P, 2));
    let r0 = a.create(1, 10);
    let x = a.create(2, 20);
    a.eng.tie_ref_to(r0, "notes.next", x);
    a.eng.oplog_commit();
    a.push();
    p.pull(A);
    let (y, tomb) = delete_and_reuse(&a, x, 3, 30);
    a.push();
    p.pull(A);
    assert_eq!(a.eng.get(r0, "notes.next"), Some(enchudb_oplog::eid_local(x) as u64), "前提: A の r0 は消えた X を指したまま");
    let c = peer(&dir, &tr, C, 2);
    c.s.bootstrap_pull(A).expect("state を取れない");
    let target = |q: &Peer| q.eng.get(q.row(1), "notes.next").map(|v| enchudb_oplog::make_eid(q.eng.peer_id(), v as u32));
    let (on_p, on_c) = (target(&p), target(&c));
    eprintln!("state の Ref: P {on_p:x?} C {on_c:x?}");
    assert!(on_p.is_some(), "前提: 差分で受けた P の r0 は消えた X を指す");
    assert_eq!(on_c.is_some(), on_p.is_some(), "r0 の ref が null かが P と C で割れる");
    for (q, t) in [(&p, on_p), (&c, on_c)] {
        let t = t.unwrap();
        assert!(!q.eng.is_live(t), "peer {} の r0 が生きた行を指す", q.eng.peer_id());
        assert_ne!(Some(t), q.rows(3).first().copied(), "peer {} の r0 が使い回した Y を指す", q.eng.peer_id());
    }
    assert_eq!([p.v(3), c.v(3)], [vec![30], vec![30]], "Y は P と C に届く");
    assert_eq!(a.eng.get(y, "notes.v"), Some(30));
    assert!(own_state_deletes(&a.eng).contains(&(x, tomb)), "A の state に X の Delete が無い");
}

/// 開き直し (identity に移した削除の版数の永続 = `eidmap`) の後も、 state に X の Delete が同じ版数で入る。 忘れてよい線
/// (`forget_deletes_before`) を越えた削除は state から外れる。
#[test]
fn the_moved_delete_survives_reopen_and_is_forgotten_by_the_line() {
    let dir = Dir::new("persist");
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    let (x, tomb) = {
        let a = peer(&dir, &tr, A, 2);
        let x = a.create(7, 70);
        a.create(1, 10);
        let (_, tomb) = delete_and_reuse(&a, x, 7, 77);
        a.push();
        assert!(own_state_deletes(&a.eng).contains(&(x, tomb)), "A の state に X の Delete が無い");
        (x, tomb)
    };
    let a = attach(&dir, &tr, A);
    assert!(own_state_deletes(&a.eng).contains(&(x, tomb)), "開き直した A の state に X の Delete が無い: {:?}", own_state_deletes(&a.eng));
    assert!(a.eng.forget_deletes_before(Hlc { logical: tomb.logical + 1, ..tomb }) > 0, "前提: 線より前の削除を忘れた");
    assert!(!own_state_deletes(&a.eng).iter().any(|d| d.0 == x), "線より前の X の Delete が state に残る");
}
