//! 場所の使い回し (`Engine::entity_in` の再利用の道) の後始末を、 場所を生かす前に済ませる (#925 / #919 の A2 の残り /
//! #921 の読み直し [5])。
//!
//! - #925: 自分の insert が、 消えた翻訳した行 F0 (C の行) の場所を使い回した直後に落ちる (kill = page cache は残る = 写し) と、
//!   外した写像 (C, f0) → 場所 が `.eidmap` から戻り、 新しい自分の行 Y が C の行として扱われた (Y が相手に届かない、 届き直した
//!   F0 の削除で Y が A でだけ消える)。 開く時に 「写像の面は消えた行 (削除の版数がある) なのに場所が生きていて、 場所の削除の
//!   版数が無い」 写像を戻さない
//! - #919 の A2 の残り: 覚えの無い bridge (開き直した後) は、 場所の前の住人宛ての書き込みを配らなかった。 前の住人が自分の行なら
//!   使い回す時に退避した identity (自分の peer, 世代 | 場所) があるので、 自分の世界番号で全部配る (消える前の改名が相手に届く)
//! - #921 の読み直し [5]: 使い回した自分の消えた行への改名 (別の peer から) で、 その名前を前に持っていた別の生きた行の 「前の
//!   名前」 を外す (使い回していない peer が消えた行の slot に付け替えて外すのと同じ答え)
//!
//! 前提は状態で確かめる (場所が同じ・番号が違う、 写しの時点で bridge が動いていない)。
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue925-{tag}-{}-{n}", std::process::id()));
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
    fn push(&self) {
        self.eng.oplog_commit();
        self.eng.oplog_sync().unwrap();
        self.eng.transfer_oplog_to_sync_ops();
        self.s.publish_since(Hlc::ZERO);
    }
    fn pull(&self, from: PeerId) {
        let out = self.s.pull_once(from);
        assert!(!out.history_truncated, "前提: 抜けは無い (peer {} ← {from})", self.eng.peer_id());
    }
}

fn transport(ids: &[PeerId]) -> Arc<InMemoryTransport> {
    let tr = Arc::new(InMemoryTransport::new());
    for &id in ids {
        tr.register_peer(id);
    }
    tr
}

/// #925: A (枠 1 の表) が C の行 F0 を受け、 C の削除も受ける。 consumer の周期の前 (止めてある) に、 A のアプリが Y を足して
/// F0 の場所を使い回し、 そこで落ちた像 (写し) を開く。 Y は A の行として C と B に届き、 F0 宛ての遅い削除 (B の) は Y を消さない。
///
/// 前は、 外した写像 (C, f0) → 場所 が `.eidmap` に残っていて開き直すと戻り、 bridge が Y の書き込みを (C, f0) 宛てに出し (C では
/// F0 が消えているので捨てる = Y が届かない)、 B の削除が Y を A でだけ消した。 今は、 開く時に 「写像の面は消えた行 (C の削除を
/// 受けた pull が書き出した版数がある) なのに場所が生きていて、 場所の削除の版数が無い」 写像を戻さない。
#[test]
fn a_translated_slot_reused_right_before_a_crash_stays_unmapped() {
    let dir = Dir::new("translated");
    let tr = transport(&[A, B, C]);
    let c = peer(&dir, &tr, C, 4);
    let b = peer(&dir, &tr, B, 4);
    let (y_slot, f0_slot) = {
        let a = peer(&dir, &tr, A, 1);
        let f0 = c.create(5, 50);
        c.push();
        a.pull(C);
        b.pull(C);
        assert_eq!([a.v(5), b.v(5)], [vec![50], vec![50]], "前提: A と B に F0 がある");
        // A の `.eidmap` は F0 の写像を書き出し済み (この後の削除は、 書いた写像の面を書き直す形で載る)
        a.eng.persist_sync_state().unwrap();
        assert_eq!(a.eng.eidmap_disk_diff().unwrap(), None, "前提: A の `.eidmap` は memory と同じ");
        let f0_on_a = a.row(5);
        // C が F0 を消し、 A は受ける (B は受けない)
        c.eng.delete(f0);
        c.push();
        a.pull(C);
        assert_eq!(a.v(5), Vec::<u64>::new(), "前提: A で F0 が消えた");
        // consumer の周期を止めて (写像の外れを書き出す周期を来させない)、 Y を足して F0 の場所を使い回す
        let hold = a.eng.hold_consumer();
        let y = a.eng.entity_in("notes").unwrap();
        a.eng.tie_to(y, "notes.k", 7u32);
        a.eng.tie_to(y, "notes.v", 70u32);
        a.eng.oplog_commit();
        assert_eq!(a.eng.eid_slot(y), a.eng.eid_slot(f0_on_a), "前提: Y は F0 の場所を使い回した");
        copy_dir(Path::new(&dir.db(A)), Path::new(&dir.image(A)));
        drop(hold);
        (a.eng.eid_slot(y), a.eng.eid_slot(f0_on_a))
    };
    assert_eq!(y_slot, f0_slot);
    // 落ちた像を開く
    let a = attach(&dir.image(A), &tr, A);
    assert_eq!(a.v(7), vec![70], "前提: 開いた A に Y がある");
    // 戻さなかった写像は、 次の書き出しで `.eidmap` からも外れる (開くたびに見分け直さない)
    a.eng.persist_sync_state().unwrap();
    assert_eq!(a.eng.eidmap_disk_diff().unwrap(), None, "開く時に戻さなかった写像が `.eidmap` に残った");
    tick();
    // B は C の削除を知らずに F0 を消す (版数は C の削除より後)
    let f0_on_b = b.row(5);
    b.eng.delete(f0_on_b);
    b.push();
    a.push();
    c.pull(A);
    b.pull(A);
    a.pull(B);
    c.pull(B);
    a.push();
    c.pull(A);
    b.pull(A);
    eprintln!("#925 Y の v: A {:?} B {:?} C {:?}", a.v(7), b.v(7), c.v(7));
    assert_eq!(a.v(7), vec![70], "F0 宛ての B の削除が、 F0 の場所を使い回した Y を A で消した");
    assert_eq!([b.v(7), c.v(7)], [vec![70], vec![70]], "Y が B / C に届かない (外した写像が戻り、 Y を F0 宛てに配った) [B, C]");
    assert_eq!([a.v(5), b.v(5), c.v(5)], [vec![], vec![], vec![]], "F0 は 3 台で消えたまま");
}

/// #919 の A2 の残り: A (枠 1 の表) の行 X (名前 1) を P が受けた後、 bridge の前に A が X を名前 2 に改名して消し、 名前 3 の Y で
/// 場所を使い回す。 P は X の削除より前に名前 2 の W を作っている。 名前の記録を書き出した後 (consumer の周期は止めてあり、 bridge は
/// 動いていない) に落ちた像を開くと、 覚えの無い bridge が X の改名と削除を自分の世界番号で配る = P でも名前 2 が X の削除の時刻に
/// 消えたデッキになり、 W は A と P の両方で消える (#551)。
///
/// 前は、 覚えの無い bridge が前の住人宛ての書き込み (X の改名) を配らず (前の住人が他の peer の行だった時に相手に幻の行を作らない
/// ため)、 P は X を名前 1 のまま消した。 W は A でだけ消え、 P で生きた。
#[test]
fn a_rename_before_the_bridge_reaches_the_peer_after_a_crash() {
    let dir = Dir::new("rename");
    let tr = transport(&[A, P]);
    let p = peer(&dir, &tr, P, 4);
    let (x, y) = {
        let a = peer(&dir, &tr, A, 1);
        let x = a.create(1, 10);
        a.push();
        p.pull(A);
        assert_eq!(p.v(1), vec![10], "前提: P に X がある");
        // P が名前 2 の W を作る (まだ出さない)
        p.create(2, 20);
        let hold = a.eng.hold_consumer();
        a.eng.tie_to(x, "notes.k", 2u32);
        a.eng.oplog_commit();
        tick();
        a.eng.delete(x);
        a.eng.oplog_commit();
        tick();
        let y = a.create(3, 30);
        assert_eq!(a.eng.eid_slot(y), a.eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        assert_ne!(y, x, "前提: 使い回した行は世代の違う番号");
        // 名前の記録 (使い回す時に退避した X の identity も) を書き出す。 bridge は動かさない
        a.eng.persist_sync_state().unwrap();
        copy_dir(Path::new(&dir.db(A)), Path::new(&dir.image(A)));
        drop(hold);
        (x, y)
    };
    let a = attach(&dir.image(A), &tr, A);
    assert_eq!(a.v(3), vec![30], "前提: 開いた A に Y がある");
    assert!(!a.eng.is_live(x), "前提: 開いた A で X は消えている");
    a.push();
    p.pull(A);
    p.push();
    a.pull(P);
    a.push();
    p.pull(A);
    eprintln!("#919 A2: 名前 2 A {:?} P {:?} / 名前 3 A {:?} P {:?}", a.v(2), p.v(2), a.v(3), p.v(3));
    assert_eq!([a.v(2), p.v(2)], [vec![], vec![]], "X の消える前の改名 (名前 2) が P に届かず、 W が A と P で割れた [A, P]");
    assert_eq!([a.v(3), p.v(3)], [vec![30], vec![30]], "使い回した行 Y はそのまま");
    assert_eq!(p.v(1), Vec::<u64>::new(), "X は P でも消えた");
    assert_eq!(a.eng.get(y, "notes.v"), Some(30));
}

/// #921 の読み直し [5]: A の行 L (名前 5) を名前 6 に改名した後、 A が使い回した消えた行 X を B (X の削除を知らない) が名前 5 に
/// 改名する。 P (X の場所を使い回していない) はその改名で前の名前 5 を消えた X の slot に付け替え、 L は前の名前 5 を持たない。 その
/// 後に、 L が名前 5 だった間に作られた名前 5 の行 Z (C が作る) が届くと、 A も P も Z を L に束ねない (別の行のまま)。
///
/// 前は A が改名を identity の名前に足すだけで L の前の名前 5 を外さず、 A でだけ Z を L に束ねた。
#[test]
fn a_rename_of_the_reused_deleted_row_releases_the_former_name() {
    let dir = Dir::new("former");
    let tr = transport(&[A, B, P, C]);
    let (a, b, p, c) = (peer(&dir, &tr, A, 2), peer(&dir, &tr, B, 4), peer(&dir, &tr, P, 4), peer(&dir, &tr, C, 4));
    a.create(5, 50);
    let x = a.create(1, 10);
    a.push();
    for q in [&b, &p] {
        q.pull(A);
    }
    assert_eq!([b.v(1), p.v(1)], [vec![10], vec![10]], "前提: B と P に X がある");
    // A が X を消して名前 9 の Y で場所を使い回す。 P は受ける、 B は受けない
    a.eng.delete(x);
    a.eng.oplog_commit();
    tick();
    let y = a.create(9, 90);
    assert_eq!(a.eng.eid_slot(y), a.eng.eid_slot(x), "前提: Y は X の場所を使い回した");
    a.push();
    p.pull(A);
    assert!(p.v(1).is_empty() && p.v(9) == vec![90], "前提: P は X の削除と Y を受けた");
    // C が名前 5 の Z を作る (X の削除より後・L の改名より前。 まだ出さない)
    tick();
    c.create(5, 55);
    tick();
    // A が L を名前 6 に改名し、 P は受ける
    let l = a.row(5);
    a.eng.tie_to(l, "notes.k", 6u32);
    a.eng.oplog_commit();
    a.push();
    p.pull(A);
    assert_eq!(p.v(6), vec![50], "前提: P で L は名前 6");
    tick();
    // B が (X の削除を知らずに) X を名前 5 に改名する
    let x_b = b.row(1);
    b.eng.tie_to(x_b, "notes.k", 5u32);
    b.push();
    a.pull(B);
    p.pull(B);
    // Z が届く
    c.push();
    a.pull(C);
    p.pull(C);
    eprintln!("[5]: 名前 5 A {:?} P {:?} / 名前 6 A {:?} P {:?}", a.v(5), p.v(5), a.v(6), p.v(6));
    assert_eq!([a.v(5), a.v(6)], [p.v(5), p.v(6)], "名前 5 の Z を L に束ねるかが A と P で割れた ([名前 5, 名前 6])");
    assert_eq!([p.v(5), p.v(6)], [vec![55], vec![50]], "前提: P は Z を L に束ねない");
}
