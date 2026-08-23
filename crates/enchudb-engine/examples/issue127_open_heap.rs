//! issue #127 調査 harness: readonly open が heap (anon) をどれだけ確保するか。
//!
//! counting global allocator で Engine::open_readonly 前後の live heap 差分を測る。
//! シナリオ:
//!   1. clean DB (graceful close 済み) の readonly open ×3
//!   2. dirty DB (writer open 後 mem::forget = crash 相当) の readonly open ×3
//! それぞれ #122 vocab_max_entries knob あり (4M) / なし (既定式) で。
//!
//! 実行: cargo run --release -p enchudb-engine --example issue127_open_heap

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let n = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(n, Ordering::Relaxed);
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
                let n = LIVE.fetch_add(new - l.size(), Ordering::Relaxed) + (new - l.size());
                PEAK.fetch_max(n, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(l.size() - new, Ordering::Relaxed);
            }
        }
        np
    }
}

#[global_allocator]
static A: Counting = Counting;

fn mb(n: usize) -> f64 {
    n as f64 / (1024.0 * 1024.0)
}

use enchudb_engine::{Engine, GrowableOptions, ValueType};

fn build_db(path: &str, vocab_knob: Option<u32>) {
    let _ = std::fs::remove_file(path);
    for sfx in [".tables", ".crc", ".db.lock", ".oplog", ".schema", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{}{}", path, sfx));
    }
    let opts = GrowableOptions {
        max_entities: 1_000_000,
        vocab_max_entries: vocab_knob,
        ..Default::default()
    };
    let mut eng = Engine::create_growable_opts(path, opts).unwrap();
    eng.define_himo("kind", ValueType::Tag, 0);
    eng.define_himo("num", ValueType::Number, 0);
    for i in 0..100_000u32 {
        let e = eng.entity();
        eng.tie_text(e, "kind", &format!("tag{}", i % 1000));
        eng.tie(e, "num", i);
    }
    eng.flush().unwrap();
    // graceful close (Drop) = clean flag 永続化
}

fn make_dirty(path: &str) {
    // writer open は clean flag を dirty に flip する。 mem::forget で Drop を
    // 飛ばす = crash 相当。 flag は dirty のまま disk に残る。
    let eng = Engine::open_standalone(path).unwrap();
    std::mem::forget(eng);
}

fn measure_open(label: &str, path: &str) {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let eng = Engine::open_readonly(path).unwrap();
    let after = LIVE.load(Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    println!(
        "{:>40}: live +{:8.2} MB (peak +{:8.2} MB)",
        label,
        mb(after.saturating_sub(before)),
        mb(peak.saturating_sub(before)),
    );
    drop(eng);
}

fn main() {
    let dir = std::env::temp_dir().join(format!("issue127-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    for (knob, name) in [(Some(4_000_000u32), "knob=4M"), (None, "knob=None(default 16M)")] {
        let path = dir.join(format!("db-{}.ecdb", if knob.is_some() { "4m" } else { "def" }));
        let path = path.to_str().unwrap().to_string();

        println!("=== {} (max_entities=1M) ===", name);
        build_db(&path, knob);

        for i in 0..3 {
            measure_open(&format!("clean readonly open #{}", i + 1), &path);
        }

        make_dirty(&path);
        for i in 0..3 {
            measure_open(&format!("dirty readonly open #{}", i + 1), &path);
        }
        println!();
    }

    let _ = std::fs::remove_dir_all(&dir);
}
