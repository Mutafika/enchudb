//! issue #127: dirty (clean_flag≠1) / legacy (VIX2) DB の readonly open が
//! **index_cap 比例** の heap shadow を確保していた regression test。
//!
//! 旧実装: shadow = `vec![0u8; index_region_size(index_cap)]`。 確保は calloc
//! (仮想) だが、 #123 で hash が一様分散になったため rebuild が shadow の全ページに
//! live slot を書いて全ページを物理化し、 readonly open 1 回ごとに index_cap × 13B
//! (vocab_max_entries=4M で 52MB、 既定式 max_entities×16 だと数百 MB) の anon RSS を
//! Engine 寿命の間占有していた。 naruhodo (1GB VPS) の boot +~300MB / storm OOM の正体。
//!
//! fix 後: shadow は count 比例の compact 形式 `(fxhash, vid)` sorted。
//! 本 test は counting allocator で readonly open の heap 増分を実測し、
//! index_cap 比例 (>= 50MB) なら fail する閾値 (4MB) で固定する。

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

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue127-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for sfx in ["", ".oplog", ".tables", ".crc", ".schema", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{}{}", path, sfx));
    }
}

/// dirty DB の readonly open の heap 増分が count 比例 (数十 KB) であること。
/// 旧実装 (index_cap 比例 shadow) だと vocab_max_entries=4M で +52MB になり fail する。
#[test]
fn dirty_readonly_open_heap_is_count_proportional() {
    let path = tmp_path("compact");
    cleanup(&path);

    const N_TAGS: usize = 1000;
    {
        let opts = GrowableOptions {
            max_entities: 1_000_000,
            vocab_max_entries: Some(4_000_000), // index_region_size ≈ 52MB
            ..Default::default()
        };
        let mut eng = Engine::create_growable_opts(&path, opts).unwrap();
        eng.define_himo("kind", ValueType::Tag, 0);
        for i in 0..N_TAGS {
            let e = eng.entity().unwrap();
            eng.tie_text(e, "kind", &format!("tag{:04}", i));
        }
        eng.flush().unwrap();
    }

    // writer open は clean flag を dirty に flip する。 mem::forget で graceful close
    // (Drop の sync_and_mark_clean) を飛ばす = crash 相当。 disk は dirty のまま。
    {
        let eng = Engine::open_standalone(&path).unwrap();
        std::mem::forget(eng);
    }

    let before = LIVE.load(Ordering::Relaxed);
    let eng = Engine::open_readonly(&path).unwrap();
    let delta = LIVE.load(Ordering::Relaxed).saturating_sub(before);

    assert!(
        eng.vocab_index_rebuilt_on_load(),
        "premise broken: dirty open のはずが rebuild が走っていない (test が shadow 経路を踏んでいない)"
    );
    const LIMIT: usize = 4 * 1024 * 1024;
    assert!(
        delta < LIMIT,
        "readonly shadow must be count-proportional: open allocated {} bytes \
         (>= {} = index_cap-proportional territory; vocab_max_entries=4M の \
         index_region_size ≈ 52MB)",
        delta,
        LIMIT,
    );

    // correctness: shadow lookup で既存 vocab が全部引けること (compact shadow の
    // binary search 経路)。
    for i in (0..N_TAGS).step_by(97) {
        let tag = format!("tag{:04}", i);
        assert!(
            eng.vocab_id(&tag).is_some(),
            "shadow lookup miss: {tag}"
        );
    }
    // 存在しない値は None
    assert_eq!(eng.vocab_id("no-such-tag"), None);

    drop(eng);
    cleanup(&path);
}
