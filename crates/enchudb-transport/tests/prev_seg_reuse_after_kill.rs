//! 場所を使い回した直後に落ちても (kill = page cache は残る = 写し)、 使い回しが identity に移した前の住人の削除が残る
//! (`prev.seg`、 EID_REUSE_PLAN.md の 「決まった事 (2026-11-15 夜)」 = 案 C)。
//!
//! 使い回しは前の住人 (消えた行) の削除の版数と名前を identity に移し (自分の行 = #914 / #915、 翻訳した行 = #166 / #925)、 場所の
//! 削除の版数 (tomb.seg) をその場で 0 にする。 移した先 (`foreign_tombs` と名前の記録) は consumer の次の周期まで memory だけなので、
//! その前に落ちると前の住人の削除がどこにも残らなかった:
//!
//! - 自分の行 X: 開いた A の state に X の Delete が無く、 X の削除を取りこぼした peer R が state で取り直すと、 名前で Y を X に
//!   束ねて掃除が今の時刻で消し、 Y が R と (その削除を受けた) P で消えた (#914 の形)。 X 宛ての遅い削除は A の入口で捨てられた (#915)
//! - 翻訳した行 F (A が自分で消した、 `.eidmap` の写像の面に削除の版数が無い): 開くと写像が戻り、 使い回した Y が C の行 F として
//!   扱われた (#925 の推測は面に削除の版数がある時しか外さない)
//!
//! 今は使い回す時に前の住人を `prev.seg` に書き、 開く時に sidecar に届いていない分を戻す。 前提は状態で確かめる (場所が同じ・番号が
//! 違う、 `.eidmap` が memory と違う = 移した退避が disk に無い)。
#![cfg(unix)]

use std::path::{Path, PathBuf};
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
        let dir = std::env::temp_dir().join(format!("enchudb-prevseg-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
    fn db(&self, id: u32) -> String {
        self.0.join(format!("p{id}")).to_str().unwrap().to_string()
    }
    fn image(&self, id: u32) -> String {
        self.0.join(format!("p{id}-image")).to_str().unwrap().to_string()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

/// 表 `notes` (枠 `cap`): 名前 `k` (Number)、 `v` (Number)。
fn create_db(path: &str, cap: u32) {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("notes", cap).unwrap();
    eng.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
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
    /// 取り直した (`bootstrap_pull`) 回数。
    resyncs: AtomicU64,
}

fn peer(dir: &Dir, tr: &Arc<InMemoryTransport>, id: PeerId, cap: u32) -> Peer {
    create_db(&dir.db(id), cap);
    attach(&dir.db(id), tr, id)
}

fn attach(path: &str, tr: &Arc<InMemoryTransport>, id: PeerId) -> Peer {
    let eng = Engine::open_concurrent_with_oplog(path, OPLOG).unwrap();
    eng.set_peer_id(id);
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

/// 落ちる像を撮るまでに `oplog_sync` (WAL・名前の記録 (`pktombs` / `pkformer`)・本体を書き出す。 `.eidmap` は書かない) を呼ぶ所。
#[derive(Clone, Copy, PartialEq)]
enum Sync {
    /// 呼ばない (名前の記録も X の削除の前のまま)
    Never,
    /// X を消した後・使い回す前 (名前の記録は X の消えた名前を場所の記録に持つ = 使い回しの前の物)
    BeforeReuse,
    /// 使い回した後 (名前の記録は identity に移した後の物)
    AfterReuse,
}

/// A の consumer の周期を止めて、 名前 `k` の行 X を消し、 同じ名前の Y を足して X の場所を使い回し、 落ちた像 (写し) を撮る。
/// 戻り値は (Y, X の削除の版数)。
fn delete_reuse_and_crash(dir: &Dir, a: &Peer, x: u64, k: u32, v: u32, sync: Sync) -> (u64, Hlc) {
    let hold = a.eng.hold_consumer();
    a.eng.delete(x);
    a.eng.oplog_commit();
    let tomb = a.eng.tombstone_hlc(x);
    assert_ne!(tomb, Hlc::ZERO, "前提: X の削除の版数");
    if sync == Sync::BeforeReuse {
        a.eng.oplog_sync().unwrap();
    }
    tick();
    let y = a.create(k, v);
    assert_eq!(a.eng.eid_slot(y), a.eng.eid_slot(x), "前提: X の場所を使い回した");
    assert_ne!(y, x, "前提: 使い回した行は世代の違う番号");
    if sync == Sync::AfterReuse {
        a.eng.oplog_sync().unwrap();
    }
    assert!(a.eng.eidmap_disk_diff().unwrap().is_some(), "前提: 移した X の削除は `.eidmap` に届いていない");
    copy_dir(Path::new(&dir.db(A)), Path::new(&dir.image(A)));
    drop(hold);
    (y, tomb)
}

/// #914 の形 (落ちた像): A が名前 7 の X を消して同じ名前の Y で場所を使い回した直後に落ち、 開き直す。 X の削除を取りこぼした R が
/// state で取り直しても、 Y は 3 台で生き、 X は 3 台で消える。 開いた A の state に X の Delete (X の番号と削除の版数) がある。
fn reused_row_survives_a_resync_after_a_crash(tag: &str, sync: Sync) {
    let dir = Dir::new(tag);
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P] {
        tr.register_peer(id);
    }
    let (r, p) = (peer(&dir, &tr, B, 2), peer(&dir, &tr, P, 2));
    let (x, y, tomb) = {
        let a = peer(&dir, &tr, A, 2);
        let x = a.create(7, 70);
        a.create(1, 10);
        all(&[&a, &r, &p]);
        assert_eq!([a.v(7), r.v(7), p.v(7)], [vec![70], vec![70], vec![70]], "前提: X が 3 台に届いた");
        let (y, tomb) = delete_reuse_and_crash(&dir, &a, x, 7, 77, sync);
        (x, y, tomb)
    };
    let a = attach(&dir.image(A), &tr, A);
    assert_eq!(a.v(7), vec![77], "前提: 開いた A に Y がある");
    assert!(!a.eng.is_live(x), "前提: 開いた A で X は消えている");
    assert_eq!(a.eng.prev_residents_restored(), 1, "開く時に `prev.seg` の記録から X の削除を戻していない");
    // P は差分で受ける。 R の cursor より後の A の record を回収した (R は抜けを言われて state で取り直す)
    a.push();
    p.pull(A);
    let floor = a.eng.cell_hlc(y, a.eng.himo_id("notes.v").unwrap() as u16);
    assert!(a.eng.raise_history_floor(A, Hlc { logical: floor.logical + 1, ..floor }), "前提: floor を上げられない");
    a.push();
    r.pull(A);
    assert_eq!(r.resyncs.load(Ordering::Relaxed), 1, "前提: R は state で取り直した");
    all(&[&a, &r, &p]);
    eprintln!("{tag}: Y の v: A {:?} R {:?} P {:?}", a.v(7), r.v(7), p.v(7));
    assert_eq!([a.v(7), r.v(7), p.v(7)], [vec![77], vec![77], vec![77]], "Y が 3 台で生きていない [A, R, P]");
    assert_eq!([a.v(1), r.v(1), p.v(1)], [vec![10], vec![10], vec![10]], "他の行はそのまま");
    // 仕組み: 開いた A の state は、 使い回した場所の前の住人 X の Delete を、 X の番号と削除の版数で運ぶ
    assert!(own_state_deletes(&a.eng).contains(&(x, tomb)), "開いた A の state に X の Delete が無い: {:?}", own_state_deletes(&a.eng));
}

#[test]
fn a_reused_row_survives_a_resync_after_a_crash() {
    reused_row_survives_a_resync_after_a_crash("resync", Sync::Never);
}

#[test]
fn a_reused_row_survives_a_resync_after_a_crash_past_the_checkpoint() {
    reused_row_survives_a_resync_after_a_crash("resync-synced", Sync::AfterReuse);
}

/// #915 の形 (落ちた像): B が X を消し (A の Y の作成より後の版数)、 A は X を消して同じ名前の Y を足し場所を使い回した直後に
/// 落ちていた。 開き直した A にも B の削除が名前の削除優先 (#551) を通って届き、 Y は 3 台で消える。 `Sync::BeforeReuse` の像は
/// 名前の記録 (`pktombs` / `pkformer`) が使い回しの前の物 (場所の記録に X の消えた名前がある) = 開く時に identity に移す。
///
/// (`Sync::Never` = X の削除の名前が一度も書き出されないまま落ちた像は、 使い回しに関わらず消えた名前が残らない (前から、
/// 名前の記録は sidecar だけ)。 ここでは見ない)
fn later_delete_reaches_the_reused_name_after_a_crash(tag: &str, sync: Sync) {
    let dir = Dir::new(tag);
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P] {
        tr.register_peer(id);
    }
    let (b, p) = (peer(&dir, &tr, B, 2), peer(&dir, &tr, P, 2));
    let y = {
        let a = peer(&dir, &tr, A, 2);
        let x = a.create(7, 70);
        a.create(1, 10);
        all(&[&a, &b, &p]);
        let (y, _) = delete_reuse_and_crash(&dir, &a, x, 7, 77, sync);
        y
    };
    tick();
    // B は A の削除と Y を知らずに X を消す (版数は Y の作成より後)
    let x_b = b.row(7);
    b.eng.delete(x_b);
    b.push();
    let a = attach(&dir.image(A), &tr, A);
    assert_eq!(a.v(7), vec![77], "前提: 開いた A に Y がある");
    assert!(b.eng.tombstone_hlc(x_b) > a.eng.cell_hlc(y, a.eng.himo_id("notes.k").unwrap() as u16), "前提: B の削除は Y の作成より後");
    p.pull(A);
    p.pull(B);
    a.pull(B);
    all(&[&a, &b, &p]);
    eprintln!("{tag}: 名前 7 の行: A {:?} B {:?} P {:?}", a.v(7), b.v(7), p.v(7));
    assert_eq!([a.v(7), b.v(7), p.v(7)], [vec![], vec![], vec![]], "B の削除 (Y の作成より後) で Y が 3 台で消えない [A, B, P]");
    assert!(!a.eng.is_live(y), "A で Y が生きている");
    assert_eq!([a.v(1), b.v(1), p.v(1)], [vec![10], vec![10], vec![10]], "他の行はそのまま");
}

#[test]
fn a_later_delete_by_another_peer_reaches_the_reused_name_after_a_crash() {
    later_delete_reaches_the_reused_name_after_a_crash("later-delete", Sync::BeforeReuse);
}

#[test]
fn a_later_delete_by_another_peer_reaches_the_reused_name_after_a_crash_past_the_checkpoint() {
    later_delete_reaches_the_reused_name_after_a_crash("later-delete-synced", Sync::AfterReuse);
}

/// #925 の形 (面に削除の版数の無い写像): A (枠 1 の表) が C の行 F を受けて写像を書き出した後、 A のアプリが F を消し (`.eidmap` の
/// 写像の面には削除の版数が無いまま)、 consumer の周期の前に Y を足して F の場所を使い回した直後に落ちる。 開いた A で写像
/// (C, f) は戻らず、 Y は A の行として B と C に届き、 A の削除を知らない B の F への書き込みは A の Y に当たらず、 F は 3 台で消える。
///
/// 前は #925 の推測 (面に削除の版数がある時だけ外す) が写像を戻し、 Y が C の行 F として扱われた: B の F への書き込みが A で Y に
/// 当たって 3 台の Y を書き換え (再生する像)、 A でだけ Y が消えた (checkpoint の先の像)。
fn translated_slot_reused_after_a_local_delete(tag: &str, synced: bool) {
    let dir = Dir::new(tag);
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, C] {
        tr.register_peer(id);
    }
    let c = peer(&dir, &tr, C, 4);
    let b = peer(&dir, &tr, B, 4);
    let f_world = {
        let a = peer(&dir, &tr, A, 1);
        let f0 = c.create(5, 50);
        c.push();
        a.pull(C);
        b.pull(C);
        assert_eq!([a.v(5), b.v(5)], [vec![50], vec![50]], "前提: A と B に F がある");
        a.eng.persist_sync_state().unwrap();
        assert_eq!(a.eng.eidmap_disk_diff().unwrap(), None, "前提: A の `.eidmap` は F の写像 (削除の版数なし) を書き出し済み");
        let f_on_a = a.row(5);
        let hold = a.eng.hold_consumer();
        a.eng.delete(f_on_a);
        a.eng.oplog_commit();
        tick();
        let y = a.eng.entity_in("notes").unwrap();
        a.eng.tie_to(y, "notes.k", 7u32);
        a.eng.tie_to(y, "notes.v", 70u32);
        a.eng.oplog_commit();
        assert_eq!(a.eng.eid_slot(y), a.eng.eid_slot(f_on_a), "前提: Y は F の場所を使い回した");
        if synced {
            a.eng.oplog_sync().unwrap();
        }
        assert!(a.eng.eidmap_disk_diff().unwrap().is_some(), "前提: 外した写像は `.eidmap` に届いていない");
        copy_dir(Path::new(&dir.db(A)), Path::new(&dir.image(A)));
        drop(hold);
        f0
    };
    let a = attach(&dir.image(A), &tr, A);
    assert_eq!(a.v(7), vec![70], "前提: 開いた A に Y がある");
    let mapped = a.eng.resolve_remote_eid_existing(f_world);
    // A の削除をまだ知らない B が F を書き換える ((C, f) 宛て)。 A でこの書き込みが Y に当たってはいけない
    tick();
    let f_b = b.row(5);
    b.eng.tie_to(f_b, "notes.v", 55u32);
    b.push();
    all(&[&a, &b, &c]);
    eprintln!("{tag}: Y の v: A {:?} B {:?} C {:?} / F: A {:?} B {:?} C {:?}", a.v(7), b.v(7), c.v(7), a.v(5), b.v(5), c.v(5));
    assert_eq!([a.v(7), b.v(7), c.v(7)], [vec![70], vec![70], vec![70]], "Y が 3 台で生きていない [A, B, C]");
    assert_eq!([a.v(5), b.v(5), c.v(5)], [vec![], vec![], vec![]], "F が 3 台で消えていない [A, B, C]");
    assert_eq!(mapped, None, "開いた A で F の写像が戻った");
}

#[test]
fn a_locally_deleted_translated_slot_reused_right_before_a_crash_stays_unmapped() {
    translated_slot_reused_after_a_local_delete("translated", false);
}

#[test]
fn a_locally_deleted_translated_slot_reused_right_before_a_crash_past_the_checkpoint_stays_unmapped() {
    translated_slot_reused_after_a_local_delete("translated-synced", true);
}
