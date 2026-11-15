//! #923: author の state を author から直接取った受け手 (`complete: true`) の掃除が、 Ref の解決で払い出しただけの行 (その行宛ての
//! record が 1 つも当たっていない写像) を今の時刻で消して配っていた。
//!
//! author の state にある Ref の的が state に無いのは、 「author が持つその行に中身が無い」 (他の peer が消した / 中身を書いていない)
//! で、 受け手が新しい削除を作る理由にならない。 前は掃除の削除 (受け手の今の時刻) が配られ、
//!
//! - 他の peer B が消した行 X を指す ref の時: A / B / P では X は B の削除の版数で死んでいて、 より新しい削除として名前の削除優先
//!   (#551) を通り、 X の削除の後に同じ名前で足した行 Y を消した (C でだけ生きる)
//! - 中身の無い生きた行を指す ref の時: A の生きた行が A でも消えた
//!
//! 直し: 掃除は、 版数のある cell も削除の版数も無い (どの書き手の record も当たっていない) 生きた行を消さない。 他の peer の削除は
//! その peer の流れ / state (#766) が運ぶ。

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
        let dir = std::env::temp_dir().join(format!("enchudb-issue923-{tag}-{}-{n}", std::process::id()));
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

/// 表 `notes` (枠 100 = 場所を使い回さない): 名前 `k` (Number)、 `v` (Number)、 同じ表を指す Ref `next`。
fn create_db(path: &str) {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("notes", 100).unwrap();
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
fn peer(dir: &Dir, tr: &Arc<InMemoryTransport>, id: PeerId) -> Peer {
    create_db(&dir.db(id));
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
    /// この peer が配る自分の Delete の record (自分の ring、 書き出してから読む)。
    fn own_deletes(&self) -> Vec<(u64, Hlc)> {
        self.eng.oplog_commit();
        self.eng.oplog_sync().unwrap();
        self.eng.transfer_oplog_to_sync_ops();
        self.eng
            .pending_sync_ops(0)
            .iter()
            .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
            .filter_map(|r| match &r.op {
                DecodedOp::Delete { eid } if r.author_peer == self.eng.peer_id() => Some((*eid, r.hlc)),
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

/// issue の手順: A の X (名前 7) を W が指す。 B が X を消し (B の時計)、 A と P が受ける。 A が名前 7 の Y を足す (別の場所)。 新しく
/// 入った C が、 B の流れを受ける前に A の state を A から直接取る。 Y は X の削除の後に作った = 4 台で生きる。
///
/// 直す前: C の掃除が Ref の解決で作った空の行 (A, X) を今の時刻で消して配り、 A / B / P は名前 7 を上げて Y を消した (C でだけ生きる)。
#[test]
fn a_ref_target_from_a_direct_state_is_not_swept_with_a_new_delete() {
    let dir = Dir::new("other-delete");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, B, P, C] {
        tr.register_peer(id);
    }
    let (a, b, p) = (peer(&dir, &tr, A), peer(&dir, &tr, B), peer(&dir, &tr, P));
    let w = a.create(1, 10);
    let x = a.create(7, 70);
    a.eng.tie_ref_to(w, "notes.next", x);
    a.eng.oplog_commit();
    all(&[&a, &b, &p]);
    assert_eq!([a.v(7), b.v(7), p.v(7)], [vec![70], vec![70], vec![70]], "前提: X が 3 台に届いた");
    // B が X を消し、 A と P が受ける
    b.eng.delete(b.row(7));
    b.push();
    tick();
    a.pull(B);
    p.pull(B);
    let tomb = a.eng.tombstone_hlc(x);
    assert_eq!(tomb.peer, B, "前提: A の X の削除の版数は B の時計");
    // A が名前 7 の Y を足し (X の場所は使い回さない)、 全員が受ける
    let y = a.create(7, 77);
    assert_ne!(a.eng.eid_slot(y), a.eng.eid_slot(x), "前提: Y は X の場所を使い回さない");
    all(&[&a, &b, &p]);
    assert_eq!([a.v(7), b.v(7), p.v(7)], [vec![77], vec![77], vec![77]], "前提: Y が 3 台で生きる");
    assert_eq!(a.eng.get(w, "notes.next"), Some(enchudb_oplog::eid_local(x) as u64), "前提: A の W は消えた X を指したまま");
    // 前提: A の state に W の ref (X) はあり、 X の Delete は無い (B の時計の削除は B が運ぶ)
    let state = a.s.own_state();
    assert!(state.complete, "前提: author の state");
    let next = a.eng.himo_id("notes.next").unwrap() as u16;
    assert!(
        state.records.iter().any(|r| matches!(&r.op, DecodedOp::Tie { himo_id, value, .. } | DecodedOp::TieRef { himo_id, target: value, .. }
            if *himo_id == next && (*value as u32) == enchudb_oplog::eid_local(x))),
        "前提: A の state に X を指す ref がある"
    );
    assert!(
        !state.records.iter().any(|r| matches!(&r.op, DecodedOp::Delete { eid } if *eid == x)),
        "前提: A の state に X の Delete は無い"
    );
    // 新しく入った C が、 B より先に A の state を直接取る
    let c = peer(&dir, &tr, C);
    let out = c.s.bootstrap_pull(A).expect("state を取れない");
    let x_c = c.next_of(1).expect("前提: C の W は Ref の的を指す");
    assert_eq!(c.v(7), vec![77], "前提: C は state で Y を受けた");
    eprintln!("C の取り直し: {out:?}、 X の的は生きている {}", c.eng.is_live(x_c));
    assert_eq!(out.swept, 0, "掃除が Ref の的として払い出しただけの行を消した");
    assert!(c.own_deletes().is_empty(), "C が自分の時計の Delete を作った: {:?}", c.own_deletes());
    all(&[&a, &b, &p, &c]);
    eprintln!("名前 7 の行: A {:?} B {:?} P {:?} C {:?}", a.v(7), b.v(7), p.v(7), c.v(7));
    assert_eq!(
        [a.v(7), b.v(7), p.v(7), c.v(7)],
        [vec![77], vec![77], vec![77], vec![77]],
        "X の削除の後に足した Y が 4 台で生きていない [A, B, P, C]"
    );
    assert!(a.eng.is_live(y));
    assert_eq!([a.v(1), b.v(1), p.v(1), c.v(1)], [vec![10], vec![10], vec![10], vec![10]], "他の行はそのまま");
    // C の X は B の流れで、 B の削除の版数で消える (掃除の今の時刻ではない)
    assert!(!c.eng.is_live(x_c), "C の X が B の削除を受けても生きている");
    assert_eq!(c.eng.tombstone_hlc(x_c), tomb, "C の X の削除の版数が B の削除でない");
}

/// 中身の無い生きた行を指す ref: A の行 X には cell が無い (state に X の record は出ない)。 C が A の state を直接取っても X は消えない
/// (A でも)。 後から A が X に書いた値は 4 台に届く。
///
/// 直す前: C の掃除が X を今の時刻で消して配り、 A の生きた行 X が A でも消えた (後の書き込みも当たらない)。
#[test]
fn a_ref_to_a_live_row_without_cells_keeps_the_row() {
    let dir = Dir::new("empty-live");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, P, C] {
        tr.register_peer(id);
    }
    let (a, p) = (peer(&dir, &tr, A), peer(&dir, &tr, P));
    let w = a.create(1, 10);
    let x = a.eng.entity_in("notes").unwrap();
    a.eng.tie_ref_to(w, "notes.next", x);
    a.eng.oplog_commit();
    all(&[&a, &p]);
    assert!(a.eng.is_live(x), "前提: A の X は生きている");
    let state = a.s.own_state();
    let xs: Vec<_> = state.records.iter().filter(|r| matches!(&r.op, DecodedOp::Delete { eid } if *eid == x)).collect();
    assert!(xs.is_empty(), "前提: A の state に X の Delete は無い");
    let c = peer(&dir, &tr, C);
    let out = c.s.bootstrap_pull(A).expect("state を取れない");
    let x_c = c.next_of(1).expect("前提: C の W は X を指す");
    assert_eq!(out.swept, 0, "掃除が中身の無い生きた行を消した ({out:?})");
    all(&[&a, &p, &c]);
    assert!(a.eng.is_live(x), "C の掃除で、 A の生きた行 X が消えた");
    assert!(c.eng.is_live(x_c), "C の X が消えた");
    // A が後から X に書く: 4 台に届く
    a.eng.tie_to(x, "notes.k", 9u32);
    a.eng.tie_to(x, "notes.v", 90u32);
    a.eng.oplog_commit();
    all(&[&a, &p, &c]);
    assert_eq!([a.v(9), p.v(9), c.v(9)], [vec![90], vec![90], vec![90]], "A が後から X に書いた値が届かない [A, P, C]");
    assert_eq!(c.next_of(1), Some(c.row(9)), "C の W が X を指さない");
}

/// 対照: author の record が当たった行は、 今までどおり掃除が消す (author の削除を運ばない state = この直しより前の author / 忘れて
/// よい線より前の削除)。 値のある行 (名前 8) と、 author が cell を全部外した行 (名前 9、 値は無く外した cell の版数だけ) の両方。
#[test]
fn rows_the_author_wrote_are_still_swept() {
    let dir = Dir::new("control");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [A, C] {
        tr.register_peer(id);
    }
    let a = peer(&dir, &tr, A);
    let c = peer(&dir, &tr, C);
    a.create(1, 10);
    let valued = a.create(8, 80);
    let bare = a.create(9, 90);
    all(&[&a, &c]);
    // A が名前 9 の行の cell を全部外す (C は外したのを受ける)
    a.eng.untie(bare, "notes.v");
    a.eng.untie(bare, "notes.k");
    a.eng.oplog_commit();
    all(&[&a, &c]);
    let at_c = |e: u64| c.eng.resolve_remote_eid_existing(e).map(|i| c.eng.slot_eid(c.eng.eid_slot(i))).unwrap();
    let (valued_c, bare_c) = (at_c(valued), at_c(bare));
    assert!(c.eng.is_live(bare_c) && c.eng.get(bare_c, "notes.v").is_none(), "前提: C の名前 9 の行は生きていて値が無い");
    assert!(!c.eng.slot_untouched(c.eng.eid_slot(bare_c)), "前提: 外した cell の版数がある");
    // A が 2 行を消す。 A の state は自分の削除を運ばない (この直しより前の author と同じ形)
    a.eng.delete(valued);
    a.eng.delete(bare);
    a.eng.oplog_commit();
    let weak = Arc::downgrade(&a.eng);
    tr.register_state_provider(
        A,
        Arc::new(move || {
            let eng = weak.upgrade()?;
            let (mut records, as_of) = eng.state_records();
            records.retain(|r| !matches!(&r.op, DecodedOp::Delete { eid } if enchudb_oplog::eid_peer(*eid) == A));
            Some(enchudb::transport::StateBatch { records, as_of, complete: true })
        }),
    );
    let out = c.s.bootstrap_pull(A).expect("state を取れない");
    assert_eq!(out.swept, 2, "author が書いた 2 行を掃除が消さない ({out:?})");
    assert!(!c.eng.is_live(valued_c), "値のある行が残った");
    assert!(!c.eng.is_live(bare_c), "author が cell を全部外した行が残った");
    assert_eq!(c.v(1), vec![10], "他の行はそのまま");
}
