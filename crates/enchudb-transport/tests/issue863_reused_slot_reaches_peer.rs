//! #863: 表が消した行の slot を使い回して足した新しい行が、 その削除を受けた相手にも現れる。
//!
//! 相手は削除を受けた slot (世界番号) 宛ての書き込みを、 版数によらず捨てる (削除優先、 `slot_is_dead`)。 使い回した行は消えた行と
//! 同じ世界番号で配られるので、 そのままでは相手に現れず、 黙って食い違う。
//!
//! - engine 直、 名前の無い表: eid 空間を使い切って枠を足せない表 / 枠を足せる表
//! - schema 層、 名前のある表 (#714 / pktombs の道): 違う名前 / 同じ名前を足す
//! - ホスト (relay) を挟む形
//!
//! master (cacb0bb) ではどの形も食い違った: 表が枠を使い切ると、 枠を足せる時でも先に消えた行の slot を拾っていた
//! (`rebuild_free_locals` が枠を足す前)。 直し (#731 の段 4): 使い回した場所の行は世代の違う番号 (世代 | 場所) で配られる =
//! 相手から見て消えた行と別の世界番号の新しい行。 消えた行の番号 (前の世代) 宛ての書き込みは今まで通り捨てる。

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use enchudb::engine::Engine;
use enchudb::schema::Database;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const OPLOG: usize = 1 << 20;
const A: PeerId = 1;
const B: PeerId = 2;
const HOST: PeerId = 100;

static RUN: AtomicU64 = AtomicU64::new(0);

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let n = RUN.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("enchudb-issue863-{tag}-{}-{n}", std::process::id()));
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

fn settle(eng: &Engine) {
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

/// engine 直の DB: 表 `notes` (枠 4、 名前の無い表)。 `full` なら eid 空間を他の表で使い切る (枠を足せない)。
fn engine_db(path: &str, peer: PeerId, full: bool) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("notes", 4).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    if full {
        let rest = eng.remaining_eid_capacity();
        if rest > 0 {
            eng.define_table("pad", rest).unwrap();
        }
    }
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// A の表を 4 行で埋めて 2 行目を消し、 B が受ける。 その後 A が 1 行足し、 B が受ける。 戻り値は (消した行, 足した行, A で足した行が
/// 生きているか, B に足した行が現れたか)。
fn engine_case(full: bool) -> (u64, Result<u64, String>, bool) {
    let dir = Dir::new(if full { "engine-full" } else { "engine-grow" });
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    tr.register_peer(B);
    let t: Arc<dyn Transport> = tr.clone();
    let (a, b) = (engine_db(&dir.db(A), A, full), engine_db(&dir.db(B), B, full));
    let (sa, sb) = (Syncer::new(a.clone(), t.clone()), Syncer::new(b.clone(), t));
    let rows: Vec<u64> = (0..4u32)
        .map(|i| {
            let e = a.entity_in("notes").unwrap();
            a.tie_to(e, "notes.note", 10 + i);
            e
        })
        .collect();
    a.delete(rows[1]);
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    sb.pull_once(A);
    assert_eq!(b.pull_raw("notes.note", 10u32).len(), 1, "前提: B に行がある");
    assert!(b.pull_raw("notes.note", 11u32).is_empty(), "前提: B で消えている");
    let y = a.entity_in("notes");
    if let Ok(y) = y {
        a.tie_to(y, "notes.note", 99u32);
    }
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sb.pull_once(A);
    }
    let on_b = b.pull_raw("notes.note", 99u32).len() == 1;
    (rows[1], y, on_b)
}

/// eid 空間を使い切って枠を足せない表: 消した行の場所を使い回して足した行 (世代の違う番号) が相手に現れる。
#[test]
fn a_full_table_without_room_to_grow() {
    let (x, y, on_b) = engine_case(true);
    eprintln!("#863 engine 枠を足せない: 消した {x:#x} 足した {y:?} B に現れた {on_b}");
    let y = y.expect("枠を足せない表で、 消した行の場所を使い回さずに断った");
    assert_ne!(y, x, "消した行と同じ eid を渡した");
    assert!(on_b, "足した行が B に現れない (消した {x:#x}、 足した {y:#x})");
}

/// 相手の行を翻訳して置いた slot は、 枠を足せない時に拾ってよい: 足した自分の行は相手から見て別の世界番号なので、 相手に現れる。
#[test]
fn a_slot_that_held_a_peer_row_is_reused_and_reaches_the_peer() {
    let dir = Dir::new("foreign-slot");
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    tr.register_peer(B);
    let t: Arc<dyn Transport> = tr.clone();
    let (a, b) = (engine_db(&dir.db(A), A, false), engine_db(&dir.db(B), B, true));
    let (sa, sb) = (Syncer::new(a.clone(), t.clone()), Syncer::new(b.clone(), t));
    // A の 4 行が B の枠 4 を埋める (翻訳して置く)
    let rows: Vec<u64> = (0..4u32)
        .map(|i| {
            let e = a.entity_in("notes").unwrap();
            a.tie_to(e, "notes.note", 10 + i);
            e
        })
        .collect();
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    sb.pull_once(A);
    assert_eq!(b.table_eid_usage("notes").unwrap().live, 4, "前提: B の枠が A の行で埋まった");
    assert!(b.entity_in("notes").is_err(), "前提: B の表は満杯で、 枠も足せない");
    // A が 1 行消す → B の、 その行を置いた slot が空く
    a.delete(rows[1]);
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    sb.pull_once(A);
    assert!(b.pull_raw("notes.note", 11u32).is_empty(), "前提: B で消えている");
    // B が 1 行足す: 相手の行を置いた slot を拾う
    let y = b.entity_in("notes").expect("相手の行を置いていた slot を拾わずに断った");
    b.tie_to(y, "notes.note", 77u32);
    settle(&b);
    // #914: 前の住人は相手の行 (削除は相手の identity に移す、 #166) = B の自分の行の削除は無い (どの書き手の state にも)
    for by in [A, B] {
        let (st, _) = b.state_records_for(by);
        assert!(
            !st.iter().any(|r| matches!(&r.op, enchudb_oplog::oplog::DecodedOp::Delete { eid } if enchudb_oplog::eid_peer(*eid) == B)),
            "相手の行を置いていた slot の削除を、 B の自分の行の削除として出した ({by} の state)"
        );
    }
    sb.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sa.pull_once(B);
    }
    assert_eq!(a.pull_raw("notes.note", 77u32).len(), 1, "B が足した行が A に現れない");
}

/// 確かめ: eid 空間が残っていて表が枠を足せる時。
#[test]
fn a_full_table_with_room_to_grow() {
    let (x, y, on_b) = engine_case(false);
    eprintln!("#863 engine 枠を足せる: 消した {x:#x} 足した {y:?} B に現れた {on_b}");
    let y = y.expect("枠を足せる表で、 足すのを断った");
    assert!(on_b, "足した行が B に現れない (使い回し {})", y == x);
}

/// schema 層の DB: 表 `t` (名前 = id、 枠 4)。
fn schema_db(path: &str, peer: PeerId) -> Arc<Database> {
    let mut b = Database::create_with_capacity(path, 4096).unwrap();
    b.table("t").number("id").number("a").primary_key("id").with_capacity(4).build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(peer);
    db
}

fn ids(db: &Database) -> Vec<i64> {
    db.engine().rebuild();
    let t = db.get_table("t").unwrap();
    let mut v: Vec<i64> = t
        .all()
        .find()
        .unwrap()
        .into_iter()
        .filter_map(|e| match t.entity(e).get("id") {
            Some(enchudb::schema::Value::Number(n)) => Some(n),
            _ => None,
        })
        .collect();
    v.sort_unstable();
    v
}

/// 名前のある表で、 名前 1 の行を消してから名前 `new_id` の行を足す。 `host` なら A → ホスト → B、 でなければ A → B に直に。
/// 戻り値は (消した行, 足した行, A の名前, B の名前)。
fn schema_case(tag: &str, new_id: i64, host: bool) -> (u64, u64, Vec<i64>, Vec<i64>) {
    let dir = Dir::new(tag);
    let a = schema_db(&dir.db(A), A);
    let b = schema_db(&dir.db(B), B);
    type Step = Box<dyn Fn()>;
    let (push, pull): (Step, Step);
    let mut keep: Vec<Box<dyn std::any::Any>> = Vec::new();
    if host {
        let host_db = schema_db(&dir.db(HOST), HOST);
        let h = HttpHost::start("127.0.0.1:0", host_db.arc_engine(), None).unwrap();
        let url = format!("http://{}", h.addr());
        let ta = Arc::new(HttpHostTransport::connect(url.clone(), A).unwrap());
        let tb = Arc::new(HttpHostTransport::connect(url, B).unwrap());
        let sa = Arc::new(Syncer::new(a.arc_engine(), ta));
        let sb = Arc::new(Syncer::new(b.arc_engine(), tb));
        sa.serve_state();
        sb.serve_state();
        let (sa2, sb2, a2, hdb) = (sa.clone(), sb.clone(), a.clone(), host_db.clone());
        push = Box::new(move || {
            settle(a2.engine());
            sa2.publish_since(Hlc::ZERO);
            settle(hdb.engine());
        });
        pull = Box::new(move || {
            let out = sb2.pull_once(HOST);
            assert!(!out.history_truncated, "{out:?}");
        });
        keep.push(Box::new(h));
        keep.push(Box::new(host_db));
        keep.push(Box::new(sa));
        keep.push(Box::new(sb));
    } else {
        let tr = Arc::new(InMemoryTransport::new());
        tr.register_peer(A);
        tr.register_peer(B);
        let t: Arc<dyn Transport> = tr.clone();
        let sa = Arc::new(Syncer::new(a.arc_engine(), t.clone()));
        let sb = Arc::new(Syncer::new(b.arc_engine(), t));
        let (sa2, sb2, a2) = (sa.clone(), sb.clone(), a.clone());
        push = Box::new(move || {
            settle(a2.engine());
            sa2.publish_since(Hlc::ZERO);
        });
        pull = Box::new(move || {
            sb2.pull_once(A);
        });
        keep.push(Box::new(sa));
        keep.push(Box::new(sb));
    }
    let t = a.get_table("t").unwrap();
    let rows: Vec<u64> = (0..4i64).map(|i| t.insert().set("id", i).set("a", i * 10).commit().unwrap()).collect();
    t.entity(rows[1]).delete().unwrap();
    push();
    for _ in 0..3 {
        pull();
    }
    assert_eq!(ids(&b), vec![0, 2, 3], "前提: B は名前 1 の削除まで受けた");
    let y = t.insert().set("id", new_id).set("a", 1000i64).commit().unwrap();
    push();
    for _ in 0..3 {
        pull();
    }
    let out = (rows[1], y, ids(&a), ids(&b));
    drop(keep);
    out
}

/// 確かめ: 名前のある表で、 消した行の slot を使い回して別の名前の行を足す。
#[test]
fn a_named_table_adds_another_name() {
    let (x, y, on_a, on_b) = schema_case("named-other", 9, false);
    eprintln!("#863 schema 別の名前: 消した {x:#x} 足した {y:#x} A {on_a:?} B {on_b:?}");
    assert_eq!(on_b, on_a, "A と B の行が食い違う (使い回し {})", y == x);
}

/// 確かめ: 名前のある表で、 消した行と同じ名前の行を足す (消した後に足した同じ名前のデッキは新しいデッキ)。
#[test]
fn a_named_table_adds_the_same_name_again() {
    let (x, y, on_a, on_b) = schema_case("named-same", 1, false);
    eprintln!("#863 schema 同じ名前: 消した {x:#x} 足した {y:#x} A {on_a:?} B {on_b:?}");
    assert_eq!(on_b, on_a, "A と B の行が食い違う (使い回し {})", y == x);
}

/// 確かめ: ホストを挟む形。
#[test]
fn through_a_host() {
    let (x, y, on_a, on_b) = schema_case("host", 9, true);
    eprintln!("#863 ホスト: 消した {x:#x} 足した {y:#x} A {on_a:?} B {on_b:?}");
    assert_eq!(on_b, on_a, "A と B の行が食い違う (使い回し {})", y == x);
}

/// A の表 (枠 4、 枠を足せる) を 4 行で埋めて B に配る。 戻り値は
/// (A, B, A の Syncer, B の Syncer, A の 4 行)。
fn reused_pair(dir: &Dir, tr: &Arc<InMemoryTransport>) -> (Arc<Engine>, Arc<Engine>, Syncer, Syncer, Vec<u64>) {
    let t: Arc<dyn Transport> = tr.clone();
    let (a, b) = (engine_db(&dir.db(A), A, false), engine_db(&dir.db(B), B, false));
    let (sa, sb) = (Syncer::new(a.clone(), t.clone()), Syncer::new(b.clone(), t));
    let rows: Vec<u64> = (0..4u32)
        .map(|i| {
            let e = a.entity_in("notes").unwrap();
            a.tie_to(e, "notes.note", 10 + i);
            e
        })
        .collect();
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    sb.pull_once(A);
    assert_eq!(b.pull_raw("notes.note", 11u32).len(), 1, "前提: B に 2 行目がある");
    (a, b, sa, sb, rows)
}

fn reuse_second_row(a: &Engine, rows: &[u64]) -> u64 {
    a.delete(rows[1]);
    let y = a.entity_in("notes").unwrap();
    assert_eq!(a.eid_slot(y), a.eid_slot(rows[1]), "前提: 2 行目の場所を使い回した");
    assert_ne!(y, rows[1], "前提: 使い回した行は世代の違う番号");
    a.tie_to(y, "notes.note", 99u32);
    y
}

/// 削除優先が使い回しを越える: B が A の消えた行 (前の住人、 世代 0 の番号) に書いた record は、 A でその場所を使い回した行 (世代 1) に
/// 当たらない。 B の当ても、 A の削除を受けた後は消えた行への書き込み。
#[test]
fn a_write_to_the_deleted_row_does_not_land_on_the_reused_row() {
    let dir = Dir::new("stale-write");
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    tr.register_peer(B);
    let (a, b, sa, sb, rows) = reused_pair(&dir, &tr);
    // B が A の 2 行目 (世代 0) を書き換える (まだ配らない)
    let on_b = b.pull_raw("notes.note", 11u32)[0];
    b.tie_to(on_b, "notes.note", 500u32);
    settle(&b);
    // A は 2 行目を消して場所を使い回す
    let y = reuse_second_row(&a, &rows);
    settle(&a);
    // B の書き込みが A に届く
    sb.publish_since(Hlc::ZERO);
    let out = sa.pull_once(B);
    assert_eq!(out.dropped_unresolved, 0, "{out:?}");
    assert_eq!(a.get(y, "notes.note"), Some(99), "消えた行への書き込みが使い回した行に当たった");
    assert!(a.pull_raw("notes.note", 500u32).is_empty(), "消えた行への書き込みが A に残った");
    // A の削除と使い回した行が B に届く
    sa.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sb.pull_once(A);
    }
    assert!(b.pull_raw("notes.note", 500u32).is_empty(), "B で消えた行が残った");
    assert_eq!(b.pull_raw("notes.note", 99u32).len(), 1, "使い回した行が B に現れない");
    let live = |e: &Engine| (10..14u32).chain([99]).map(|v| e.pull_raw("notes.note", v).len()).sum::<usize>();
    assert_eq!((live(&a), live(&b)), (4, 4), "A と B の行の数");
}

/// 開き直し (写像 eidmap と世代の永続): 使い回した行は開き直した後も同じ番号で出て、 B は同じ行に当てる。 B が書き換えた物も A の同じ行に
/// 当たる。
#[test]
fn the_reused_row_keeps_its_number_across_reopen() {
    let dir = Dir::new("reopen");
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    tr.register_peer(B);
    let y;
    let y_on_b;
    {
        let (a, b, sa, sb, rows) = reused_pair(&dir, &tr);
        y = reuse_second_row(&a, &rows);
        settle(&a);
        sa.publish_since(Hlc::ZERO);
        for _ in 0..3 {
            sb.pull_once(A);
        }
        let on_b = b.pull_raw("notes.note", 99u32);
        assert_eq!(on_b.len(), 1, "使い回した行が B に現れない");
        y_on_b = on_b[0];
        b.oplog_sync().unwrap();
    }
    let open = |id: u32, peer: PeerId| {
        let e = Engine::open_concurrent_with_oplog(&dir.db(id), OPLOG).unwrap();
        e.set_peer_id(peer);
        e
    };
    let (a, b) = (open(A, A), open(B, B));
    let t: Arc<dyn Transport> = tr.clone();
    let (sa, sb) = (Syncer::new(a.clone(), t.clone()), Syncer::new(b.clone(), t));
    assert_eq!(a.get(y, "notes.note"), Some(99), "開き直した A で使い回した行の番号が変わった");
    a.tie_to(y, "notes.note", 100u32);
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sb.pull_once(A);
    }
    assert_eq!(b.pull_raw("notes.note", 100u32), vec![y_on_b], "開き直した後の書き込みが B の同じ行に当たらない");
    b.tie_to(y_on_b, "notes.note", 101u32);
    settle(&b);
    sb.publish_since(Hlc::ZERO);
    sa.pull_once(B);
    assert_eq!(a.get(y, "notes.note"), Some(101), "B の書き換えが A の使い回した行に当たらない");
    assert_eq!(b.table_eid_usage("notes").unwrap().live, 4, "B の行の数");
}

/// state (取り直し): 使い回した行は state にも世代込みの番号で入り、 state で始めた peer C は、 その後の差分を同じ行に当てる。
#[test]
fn the_state_carries_the_reused_rows_number() {
    const C: PeerId = 3;
    let dir = Dir::new("state");
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    tr.register_peer(B);
    tr.register_peer(C);
    let (a, _b, sa, _sb, rows) = reused_pair(&dir, &tr);
    let y = reuse_second_row(&a, &rows);
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    sa.serve_state();
    let state = sa.own_state();
    let world: Vec<u64> = state
        .records
        .iter()
        .filter_map(|r| match &r.op {
            enchudb_oplog::oplog::DecodedOp::Tie { eid, .. } => Some(*eid),
            _ => None,
        })
        .collect();
    assert!(world.contains(&y), "state に使い回した行の番号 (世代込み) が無い: {world:x?}");
    assert!(!world.contains(&rows[1]), "state に消えた行の番号がある: {world:x?}");
    let c = engine_db(&dir.db(C), C, false);
    let t: Arc<dyn Transport> = tr.clone();
    let sc = Syncer::new(c.clone(), t);
    sc.bootstrap_pull(A).expect("state を取れない");
    let on_c = c.pull_raw("notes.note", 99u32);
    assert_eq!(on_c.len(), 1, "state で使い回した行が C に現れない");
    assert!(c.pull_raw("notes.note", 11u32).is_empty(), "state で消えた行が C に現れた");
    a.tie_to(y, "notes.note", 123u32);
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sc.pull_once(A);
    }
    assert_eq!(c.pull_raw("notes.note", 123u32), on_c, "state の後の差分が C の同じ行に当たらない");
    assert_eq!(c.table_eid_usage("notes").unwrap().live, 4, "C の行の数");
}

/// state の形: 自分の行の宛名・自分の行を指す Ref の値・外した cell (Untie)・消えた行 (Delete) は世代込みの番号。 消えた行を指したまま
/// 場所を使い回した ref は、 消えた行の削除が自分の時計の物 (= その Delete が同じ state に入る) なら消えた行の番号のまま出す (今の住人を
/// 指さない。 他の peer の時計の削除なら出さない、 issue914 の `a_ref_to_a_row_another_peer_deleted_does_not_remove_the_readded_row`)。
/// #914: 使い回した場所の前の住人の Delete も、 前の住人の番号で出す (使い回す時に削除の版数を identity に移す)。
#[test]
fn the_state_names_own_rows_with_their_generation() {
    use enchudb_oplog::oplog::DecodedOp;
    let dir = Dir::new("state-shape");
    let a = ref_db(&dir.db(A), A);
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    let t: Arc<dyn Transport> = tr.clone();
    let sa = Syncer::new(a.clone(), t);
    let rows: Vec<u64> = (0..4u32)
        .map(|i| {
            let e = a.entity_in("notes").unwrap();
            a.tie_to(e, "notes.note", 10 + i);
            e
        })
        .collect();
    // rows[0] → rows[1] (この後消えて場所を使い回される)、 rows[2] → y (使い回した行)
    a.tie_ref_to(rows[0], "notes.next", rows[1]);
    let y = reuse_second_row(&a, &rows);
    a.tie_ref_to(rows[2], "notes.next", y);
    a.tie_to(y, "notes.memo", 7u32);
    a.untie(y, "notes.memo");
    // rows[3] を消して場所を使い回した z を、 さらに消す
    a.delete(rows[3]);
    let z = a.entity_in("notes").unwrap();
    assert_eq!(a.eid_slot(z), a.eid_slot(rows[3]), "前提: 4 行目の場所を使い回した");
    a.tie_to(z, "notes.note", 55u32);
    a.delete(z);
    settle(&a);
    let (note, memo, next) = ["notes.note", "notes.memo", "notes.next"].map(|h| a.himo_id(h).unwrap() as u16).into();
    let st = sa.own_state().records;
    let has = |f: &dyn Fn(&DecodedOp) -> bool| st.iter().any(|r| f(&r.op));
    assert!(has(&|op| matches!(op, DecodedOp::Tie { eid, himo_id, value: 99 } if *eid == y && *himo_id == note)), "使い回した行の Tie の宛名");
    assert!(
        has(&|op| matches!(op, DecodedOp::Tie { eid, himo_id, value } if *eid == rows[2] && *himo_id == next
            && *value == enchudb_oplog::eid_local(y) as u64)),
        "使い回した行を指す ref の値が世代込みでない"
    );
    assert!(
        has(&|op| matches!(op, DecodedOp::Tie { eid, himo_id, value } if *eid == rows[0] && *himo_id == next
            && *value == enchudb_oplog::eid_local(rows[1]) as u64)),
        "消えた行を指したまま場所を使い回した ref が、 消えた行の番号で出ない"
    );
    assert!(
        !has(&|op| matches!(op, DecodedOp::Tie { eid, himo_id, value } if *eid == rows[0] && *himo_id == next
            && *value == enchudb_oplog::eid_local(y) as u64)),
        "消えた行を指したまま場所を使い回した ref が、 今の住人を指した"
    );
    assert!(has(&|op| matches!(op, DecodedOp::Untie { eid, himo_id } if *eid == y && *himo_id == memo)), "使い回した行の Untie の宛名");
    assert!(has(&|op| matches!(op, DecodedOp::Delete { eid } if *eid == z)), "使い回して消えた行の Delete の宛名");
    assert!(has(&|op| matches!(op, DecodedOp::Delete { eid } if *eid == rows[1])), "使い回した場所の前の住人 (2 行目) の Delete が無い");
    assert!(has(&|op| matches!(op, DecodedOp::Delete { eid } if *eid == rows[3])), "使い回した場所の前の住人 (4 行目) の Delete が無い");
    assert!(!has(&|op| matches!(op, DecodedOp::Delete { eid } if *eid == y)), "生きている使い回した行の Delete を出した");
}

/// 表 `notes` (枠 4、 名前の無い表) に Number 2 本と、 同じ表を指す Ref `next`。
fn ref_db(path: &str, peer: PeerId) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("notes", 4).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "memo", ValueType::Number, 0).unwrap();
    eng.define_ref_in("notes", "next", "notes").unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// Ref と使い回し: A が使い回した行 y を指す ref は、 B でも y の写しを指す (差分の道、 素の Tie の値 = 世代 | 場所)。 B が A の消えた行を
/// 指して書いた ref は、 A では消えた行を指す ref (中身の無い行) のまま = 場所を使い回した y を指さない。
#[test]
fn refs_follow_the_generation_both_ways() {
    let dir = Dir::new("refs");
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    tr.register_peer(B);
    let t: Arc<dyn Transport> = tr.clone();
    let (a, b) = (ref_db(&dir.db(A), A), ref_db(&dir.db(B), B));
    let (sa, sb) = (Syncer::new(a.clone(), t.clone()), Syncer::new(b.clone(), t));
    let rows: Vec<u64> = (0..4u32)
        .map(|i| {
            let e = a.entity_in("notes").unwrap();
            a.tie_to(e, "notes.note", 10 + i);
            e
        })
        .collect();
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    sb.pull_once(A);
    let on_b = |v: u32| b.pull_raw("notes.note", v);
    let (r0_b, r1_b) = (on_b(10)[0], on_b(11)[0]);
    // B が A の 2 行目 (この後 A で消える) を指す ref を書く (まだ配らない)
    b.tie_ref_to(r0_b, "notes.next", r1_b);
    settle(&b);
    // A は 2 行目を消して場所を使い回し、 3 行目から y を指す
    let y = reuse_second_row(&a, &rows);
    a.tie_ref_to(rows[2], "notes.next", y);
    settle(&a);
    sa.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sb.pull_once(A);
    }
    let y_b = on_b(99);
    assert_eq!(y_b.len(), 1, "前提: 使い回した行が B に現れた");
    let target = |e: &Engine, row: u64| e.get(row, "notes.next").map(|v| enchudb_oplog::make_eid(e.peer_id(), v as u32));
    assert_eq!(target(&b, on_b(12)[0]), Some(y_b[0]), "B で、 使い回した行を指す ref が y の写しを指さない");
    // B の ref が A に届く: 消えた行 (前の住人) を指す ref
    sb.publish_since(Hlc::ZERO);
    sa.pull_once(B);
    let got = target(&a, rows[0]).expect("B の ref が A に届かない");
    assert_ne!(got, y, "消えた行を指す ref が、 場所を使い回した行を指した");
    assert_eq!(got, rows[1], "消えた行を指す ref が、 消えた行の番号でない");
    // 逆引き (#899: Ref の円柱は場所を鍵に、 entry に値の世代): 相手から届いた値でも、 y を指す行は y を指して書いた行だけ
    let pointing = |e: &Engine, to: u64| {
        let mut v = e.pull_raw("notes.next", enchudb_oplog::eid_local(to));
        v.sort_unstable();
        v
    };
    assert_eq!(pointing(&a, y), vec![rows[2]], "A で y を指す行の逆引き");
    assert_eq!(pointing(&a, rows[1]), vec![rows[0]], "A で消えた行を指す行の逆引き");
    assert_eq!(pointing(&b, y_b[0]), vec![on_b(12)[0]], "B で y の写しを指す行の逆引き");
}

/// 別名 (#178) の知らせが A のもう無い行 (場所を使い回した前の住人) を名指しても、 使い回した行に相手の世界番号を結ばない。
#[test]
fn an_alias_naming_a_deleted_row_does_not_bind_the_reused_row() {
    use enchudb::transport::WireRecord;
    use enchudb_oplog::oplog::DecodedOp;
    let dir = Dir::new("alias");
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    tr.register_peer(B);
    let (a, _b, sa, _sb, rows) = reused_pair(&dir, &tr);
    let y = reuse_second_row(&a, &rows);
    settle(&a);
    let other = enchudb_oplog::make_eid(B, 7);
    let at = |wall: u64| Hlc { wall, logical: 0, peer: B };
    let note = a.himo_id("notes.note").unwrap() as u16;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    sa.apply_records(&[
        WireRecord::unsigned(at(now + 1000), B, DecodedOp::Alias { eid: rows[1], same_as: other }),
        WireRecord::unsigned(at(now + 2000), B, DecodedOp::Tie { eid: other, himo_id: note, value: 777 }),
    ]);
    assert_eq!(a.get(y, "notes.note"), Some(99), "相手の世界番号が使い回した行に結ばれた");
    assert_eq!(a.pull_raw("notes.note", 777u32).len(), 1, "相手の行が別の行として現れない");
    // もう無い行のより新しい削除は、 使い回す時に移した削除の版数を上げるだけ (#915)。 使い回した行には当たらない
    assert!(a.remote_delete_apply_foreign(rows[1], at(now + 3000)), "もう無い自分の行のより新しい削除で、 移した削除の版数を上げない");
    assert!(!a.remote_delete_apply_foreign(rows[1], at(now + 3000)), "同じ削除をもう一度上げた");
    assert!(a.is_live(y), "もう無い行の削除で使い回した行が消えた");
}
