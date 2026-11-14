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
//! (`rebuild_free_locals` が枠を足す前)。 直し: sync の DB の user の表は、 先に枠を足し、 足せない時だけ穴を拾い、 消した自分の
//! 行の slot は拾わない (拾える穴が無ければ足すのを断る)。 相手の行を翻訳して置いた slot は拾ってよい (世界番号が相手の物)。

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

/// eid 空間を使い切って枠を足せない表: 消した行の slot を使い回さず、 足すのを断る (黙って食い違わない)。
#[test]
fn a_full_table_without_room_to_grow() {
    let (x, y, on_b) = engine_case(true);
    eprintln!("#863 engine 枠を足せない: 消した {x:#x} 足した {y:?} B に現れた {on_b}");
    match y {
        Ok(y) => panic!("消した行の slot を使い回した (足した {y:#x}、 消した {x:#x}、 B に現れた {on_b})"),
        Err(e) => assert!(e.contains("#863"), "断った理由が #863 でない: {e}"),
    }
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
    let (push, pull): (Box<dyn Fn()>, Box<dyn Fn()>);
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
