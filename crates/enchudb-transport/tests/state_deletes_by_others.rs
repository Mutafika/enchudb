//! author の state に、 author の行を**他の peer が消した** Delete も入れる (書き手 = author、 版数 = その削除の版数)。
//!
//! 掃除 (#140) は 「author の state に無い = author が消した」 と読む。 前は author の state の削除は author の時計の物だけ (#784) で、
//! 他の peer B が消した author の行 X は state に現れず、 B の削除を受ける前に author の state を直接取った受け手 C は、 X に何かの
//! record が当たっていれば (author の差分・第三の peer の書き込み・C のアプリ) 今の時刻の削除で消して配った。 その削除は名前の
//! 削除優先 (#551) を通り、 B の削除の後に同じ名前で足した行 Y を C 以外の全員で消した (#923 の残り)。
//!
//! 直し: author の state の Delete に、 author の行を他の peer が消した物もその版数で入れる (#761 / #766 の延長、 他の peer が書いた
//! author の行の cell を author の record で出す #711 と同じ形)。 受け手は X を B の削除の版数で消し、 掃除の対象から外す。

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
        let dir = std::env::temp_dir().join(format!("enchudb-state-others-del-{tag}-{}-{n}", std::process::id()));
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

/// 書き込みの HLC の壁時計を進める (別の peer の書き込みとの前後を、 書いた順にする)。
fn tick() {
    std::thread::sleep(Duration::from_millis(3));
}

struct Peer {
    eng: Arc<Engine>,
    s: Syncer,
}

/// 作って開き、 Syncer を付ける (state を配る)。
fn peer(dir: &Dir, tr: &Arc<InMemoryTransport>, id: PeerId, cap: u32) -> Peer {
    create_db(&dir.db(id), cap);
    let eng = Engine::open_concurrent_with_oplog(&dir.db(id), OPLOG).unwrap();
    eng.set_peer_id(id);
    let t: Arc<dyn Transport> = tr.clone();
    let s = Syncer::new(eng.clone(), t);
    s.serve_state();
    Peer { eng, s }
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
    /// 名前 `k` の行の `next` の指す行 (この peer の eid)。
    fn next_of(&self, k: u32) -> Option<u64> {
        self.eng.get(self.row(k), "notes.next").map(|v| enchudb_oplog::make_eid(self.eng.peer_id(), v as u32))
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
        }
    }
    /// この peer の state の Delete (宛名, 書き手, 版数)。
    fn state_deletes(&self) -> Vec<(u64, PeerId, Hlc)> {
        self.s
            .own_state()
            .records
            .iter()
            .filter_map(|r| match &r.op {
                DecodedOp::Delete { eid } => Some((*eid, r.author_peer, r.hlc)),
                _ => None,
            })
            .collect()
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

/// A の X (名前 7) を W が指し、 B が X を消し (B の時計)、 A が名前 7 の Y を足す (`reuse` なら X の場所を使い回す)。 A / B / P は
/// 全部を受ける。 戻り値は (A, B, P, W, X, Y, B の削除の版数)。 `third` なら、 B の削除の前に P が A の行 X に書く (P の時計)。
fn deleted_by_b_and_readded(dir: &Dir, tr: &Arc<InMemoryTransport>, cap: u32, third: bool) -> (Peer, Peer, Peer, u64, u64, u64, Hlc) {
    let (a, b, p) = (peer(dir, tr, A, cap), peer(dir, tr, B, cap), peer(dir, tr, P, cap));
    let w = a.create(1, 10);
    let x = a.create(7, 70);
    a.eng.tie_ref_to(w, "notes.next", x);
    a.eng.oplog_commit();
    all(&[&a, &b, &p]);
    if third {
        p.eng.tie_to(p.row(7), "notes.v", 71u32);
        p.eng.oplog_commit();
        tick();
        all(&[&a, &b, &p]);
        assert_eq!([a.v(7), b.v(7), p.v(7)], [vec![71], vec![71], vec![71]], "前提: P の書き込みが 3 台に届いた");
    }
    // B が X を消し、 A と P が受ける
    b.eng.delete(b.row(7));
    b.push();
    tick();
    a.pull(B);
    p.pull(B);
    let tomb = a.eng.tombstone_hlc(x);
    assert_eq!(tomb.peer, B, "前提: A の X の削除の版数は B の時計");
    let y = a.create(7, 77);
    all(&[&a, &b, &p]);
    assert_eq!([a.v(7), b.v(7), p.v(7)], [vec![77], vec![77], vec![77]], "前提: Y が 3 台で生きる");
    assert_eq!(a.eng.get(w, "notes.next"), Some(enchudb_oplog::eid_local(x) as u64), "前提: A の W は消えた X を指したまま");
    (a, b, p, w, x, y, tomb)
}

/// A の state に、 B が消した A の行 X の Delete が、 書き手 A・B の削除の版数で入る。
fn assert_state_has_the_delete(a: &Peer, x: u64, tomb: Hlc) {
    let dels = a.state_deletes();
    assert!(dels.contains(&(x, A, tomb)), "A の state に、 B が消した X の Delete (書き手 A、 版数 {tomb:?}) が無い: {dels:?}");
}

/// [B1] (第三の peer): B の削除の前に P が A の行 X に書く (P の時計)。 新しく入った C は P から先に受けて X を作り (P の書き込みが
/// 当たった行)、 B より先に A の state を直接取る。 C は X を B の削除の版数で消し、 掃除しない。 Y は 4 台で生きる。
///
/// 直す前: A の state に X の Delete が無く、 C の掃除が X (何も当たっていない行ではない) を今の時刻で消して配り、 名前の削除優先で
/// A / B / P の Y を消した (C でだけ生きる)。
#[test]
fn a_row_another_peer_wrote_and_b_deleted_is_not_swept_with_a_new_delete() {
    let dir = Dir::new("third");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P, C] {
        tr.register_peer(id);
    }
    let (a, b, p, _w, x, y, tomb) = deleted_by_b_and_readded(&dir, &tr, 100, true);
    assert_state_has_the_delete(&a, x, tomb);
    // replica 版 (P が配る A の state) も同じ Delete を持つ (author 本人の state と同じ中身)
    let (replica, _) = p.eng.state_records_for(A);
    assert!(
        replica.iter().any(|r| r.author_peer == A && r.hlc == tomb && matches!(&r.op, DecodedOp::Delete { eid } if *eid == x)),
        "P が配る A の state に、 B が消した X の Delete (書き手 A、 版数 {tomb:?}) が無い"
    );
    let c = peer(&dir, &tr, C, 100);
    c.pull(P);
    let x_c = c.eng.resolve_remote_eid_existing(x).expect("前提: C は P の書き込みで A の X を作った");
    assert!(!c.eng.slot_untouched(c.eng.eid_slot(x_c)), "前提: C の X には P の書き込みが当たっている");
    let out = c.s.bootstrap_pull(A).expect("state を取れない");
    eprintln!("[B1] C の取り直し: {out:?}");
    assert_eq!(out.swept, 0, "掃除が B の消した X を今の時刻で消した");
    all(&[&a, &b, &p, &c]);
    eprintln!("[B1] 名前 7 の行: A {:?} B {:?} P {:?} C {:?}", a.v(7), b.v(7), p.v(7), c.v(7));
    assert_eq!([a.v(7), b.v(7), p.v(7), c.v(7)], [vec![77], vec![77], vec![77], vec![77]], "B の削除の後に足した Y が 4 台で生きていない [A, B, P, C]");
    assert!(a.eng.is_live(y));
    let x_c = c.eng.slot_eid(c.eng.eid_slot(c.eng.resolve_remote_eid_existing(x).unwrap()));
    assert!(!c.eng.is_live(x_c), "C の X が生きている");
    assert_eq!(c.eng.tombstone_hlc(x_c), tomb, "C の X の削除の版数が B の削除でない");
}

/// PR #942 の 「弱い所」 (author の record が当たった行): C は A の差分で X を受けた後、 B の削除を受ける前に A の state を直接取る。
/// C は X を B の削除の版数で消し、 掃除しない。 Y は 4 台で生きる。
///
/// 直す前: C の掃除が X を今の時刻で消して配り、 名前の削除優先で Y を消した。
#[test]
fn a_row_the_author_wrote_and_b_deleted_is_not_swept_with_a_new_delete() {
    let dir = Dir::new("author");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P, C] {
        tr.register_peer(id);
    }
    // C は最初から居て、 A の X を差分で受ける (B の流れは受けない)
    let (a, b, p) = (peer(&dir, &tr, A, 100), peer(&dir, &tr, B, 100), peer(&dir, &tr, P, 100));
    let c = peer(&dir, &tr, C, 100);
    let w = a.create(1, 10);
    let x = a.create(7, 70);
    a.eng.tie_ref_to(w, "notes.next", x);
    a.eng.oplog_commit();
    all(&[&a, &b, &p]);
    a.push();
    c.pull(A);
    assert_eq!(c.v(7), vec![70], "前提: C は A の差分で X を受けた");
    b.eng.delete(b.row(7));
    b.push();
    tick();
    a.pull(B);
    p.pull(B);
    let tomb = a.eng.tombstone_hlc(x);
    assert_eq!(tomb.peer, B, "前提: A の X の削除の版数は B の時計");
    let y = a.create(7, 77);
    all(&[&a, &b, &p]);
    assert_state_has_the_delete(&a, x, tomb);
    // C が B より先に A の state を直接取る
    let out = c.s.bootstrap_pull(A).expect("state を取れない");
    eprintln!("弱い所 C の取り直し: {out:?}");
    assert_eq!(out.swept, 0, "掃除が B の消した X を今の時刻で消した");
    all(&[&a, &b, &p, &c]);
    eprintln!("弱い所 名前 7 の行: A {:?} B {:?} P {:?} C {:?}", a.v(7), b.v(7), p.v(7), c.v(7));
    assert_eq!([a.v(7), b.v(7), p.v(7), c.v(7)], [vec![77], vec![77], vec![77], vec![77]], "B の削除の後に足した Y が 4 台で生きていない [A, B, P, C]");
    assert!(a.eng.is_live(y));
    assert_eq!([a.v(1), b.v(1), p.v(1), c.v(1)], [vec![10], vec![10], vec![10], vec![10]], "他の行はそのまま");
}
