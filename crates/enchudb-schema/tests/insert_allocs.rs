//! 名前 (主キー) のある表の insert 1 行の heap の確保の回数。 0.36 を切る前の測りで、 insert 1 行が 0.35.0 の 309 ns から
//! 389 ns になった。 名前の確かめ (#603) で主キーの索引が組まれるようになり、 値がみな違う主キーは値ごとに bucket を作る。 その
//! bucket を空の backing で作って最初の 1 件ですぐ作り直していた (確保 2 回と捨てる 1 回が余計)。 名前のカードを最初に書く
//! 並べ直し (#575) も insert のたびに Vec を作っていた。
//!
//! 確保の回数を数える allocator で、 主キーの索引を組んだ後の insert を数える。 この binary の test はこの 1 本だけ。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static A: Counting = Counting;

use enchudb_schema::Database;

#[test]
fn inserting_a_named_row_allocates_little() {
    let n: i64 = 20_000;
    let path = format!(
        "/tmp/enchudb-insert-allocs-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let mut db = Database::create_growable_with_capacity(&path, 200_000).unwrap();
    db.table("t")
        .number("id")
        .number("age")
        .number("grp")
        .primary_key("id")
        .with_capacity(100_000)
        .build()
        .unwrap();
    let t = db.get_table("t").unwrap();
    // 最初の 1 行で主キーの索引を組む (名前の確かめ)。 数えるのはその後
    t.insert().set("id", 1_000_000i64).set("age", 1i64).set("grp", 1i64).commit().unwrap();
    let before = ALLOCS.load(Ordering::Relaxed);
    for i in 0..n {
        t.insert().set("id", i).set("age", i % 80).set("grp", i % 7).commit().unwrap();
    }
    let per_row = (ALLOCS.load(Ordering::Relaxed) - before) as f64 / n as f64;
    eprintln!("allocations per insert: {per_row:.2}");
    drop(t);
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
    // v0.35.0 は 5.00 回 (主キーの索引を組まない)、 この直しの前の master は 8.03 回、 直した後 6.01 回 (主キーの値ごとの bucket の分が
    // 0.35.0 より多い)
    assert!(per_row < 6.5, "insert 1 行の確保が多い: {per_row:.2} 回");
}
