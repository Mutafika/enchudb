//! 位置の印 (#578) を宣言していない列の bucket は、 印の置き場を持たない (宣言の無い列は宣言が無い時のまま)。
//!
//! 0.36 の前の測りで、 社員の表 (主キー id の値がみな違う = bucket が 100 万個) の RSS が #578 の後に +35 MB だった。 bucket の
//! backing ごとに古い印の語の列 (`Box<[AtomicU64]>`) と 「印が全部か」 の flag を、 宣言の有無によらず持っていた。
//!
//! 生存ヒープを数える allocator で、 readonly で開いて `pull_raw` を 1 回呼んだ (索引を組んだ) 前後を測る。 この binary の test は
//! この 1 本だけ (他の test の確保を数えないため)。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size(), Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let np = unsafe { System.realloc(p, l, new) };
        if !np.is_null() {
            if new >= l.size() {
                LIVE.fetch_add(new - l.size(), Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(l.size() - new, Ordering::Relaxed);
            }
        }
        np
    }
}

#[global_allocator]
static A: Counting = Counting;

use enchudb_engine::{Engine, GrowableOptions, ValueType};

/// 値がみな違う列 (20 万行、 値 0..20 万) の索引を組むと、 bucket が 20 万個できる。 宣言の無い列の bucket 1 個の常駐は、 bucket と
/// backing の struct と中身 (1 件) と、 値の配列の 1 枠 (8 B) だけ。 印の置き場を持つと 1 個あたり +32 B ほど増える。
#[test]
fn undeclared_buckets_carry_no_marks() {
    let rows: u32 = 200_000;
    let path = format!(
        "/tmp/enchudb-marks-heap-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    {
        let mut eng =
            Engine::create_growable_opts(&path, GrowableOptions { max_entities: rows + 1024, ..Default::default() })
                .unwrap();
        eng.define_himo("id", ValueType::Number, 0);
        for i in 0..rows {
            let e = eng.entity().unwrap();
            eng.tie(e, "id", i);
        }
        eng.flush().unwrap();
    }
    let eng = Engine::open_readonly(&path).unwrap();
    let base = LIVE.load(Ordering::Relaxed);
    assert_eq!(eng.pull_raw("id", 777).len(), 1);
    let per_bucket = (LIVE.load(Ordering::Relaxed) - base) as f64 / rows as f64;
    eprintln!("index heap per bucket: {per_bucket:.1} B");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
    assert!(per_bucket < 100.0, "宣言の無い列の bucket が印の置き場を持つ (1 個あたり {per_bucket:.1} B)");
}
