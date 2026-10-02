//! #373: Tag 列の索引の速さとメモリ。 表 T 個 (`id` tag 主キー、 `kind` tag) に合計 N 行を、 表を順番に回しながら
//! 入れる (辞書 ID が表をまたいで交互に進む = 1 つの列の値は辞書 ID の範囲に T 個に 1 つ)。 #358 の再現と同じ形。
//!
//! 測るもの:
//! - insert: 行を入れる速さ (索引を組んだ後、 書き込みのたびに索引も更新する)
//! - where_eq(id): 主キーで 1 行引く (Tag の索引の等値)
//! - where_eq(kind).count: 1 つの値に全行が居る列の件数
//! - 索引のヒープ: 数えるだけの allocator で、 索引を組む場合と組まない場合の差
//! - 開き直した後の最初の where_eq(id): 索引を Column から組み直す時間 (全部の表の `id` 列)
//!
//! `cargo run --release -p enchudb-schema --example tag_index_bench [N] [T...]`
//! (既定 N = 200,000、 T = 1 4 16 64)

use enchudb_schema::Database;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::Instant;

static LIVE: AtomicIsize = AtomicIsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            LIVE.fetch_add(l.size() as isize, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new_size) };
        if !q.is_null() {
            LIVE.fetch_add(new_size as isize - l.size() as isize, Ordering::Relaxed);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for suf in ["", ".tables", ".oplog", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".positions"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

struct Run {
    heap: isize,
    insert_per_s: f64,
    eq_ns: f64,
    count_ns: f64,
    rebuild_ms: f64,
}

fn run(tables: usize, rows: usize, index: bool) -> Run {
    let path = format!("/tmp/enchudb-tag-index-bench-{}-{tables}-{index}", std::process::id());
    cleanup(&path);
    // table は既定で 1 M 行ずつ eid を予約する
    let mut b = Database::create_growable_with_capacity(&path, (tables as u32 + 2) * 1_000_000).unwrap();
    for t in 0..tables {
        b.table(&format!("t{t}")).tag("id").tag("kind").primary_key("id").build().unwrap();
    }
    let db = b.finish_with_oplog(1 << 30).unwrap();
    let tbs: Vec<_> = (0..tables).map(|t| db.get_table(&format!("t{t}")).unwrap()).collect();
    for (t, tb) in tbs.iter().enumerate() {
        tb.insert().set("id", format!("seed{t}")).set("kind", "k").commit().unwrap();
        if index {
            tb.where_eq("id", format!("seed{t}")).find_one().unwrap();
            tb.where_eq("kind", "k").count().unwrap();
        }
    }
    db.engine().flush_writes();
    let before = LIVE.load(Ordering::Relaxed);
    let t0 = Instant::now();
    for i in 0..rows / tables {
        for (t, tb) in tbs.iter().enumerate() {
            tb.insert().set("id", format!("t{t}_r{i}")).set("kind", "k").commit().unwrap();
        }
    }
    let insert_per_s = rows as f64 / t0.elapsed().as_secs_f64();
    // 書き込みの列 (consumer がまだ当てていない分) もヒープにある。 当て終わってから測る
    db.engine().flush_writes();
    db.engine().oplog_sync().unwrap();
    let heap = LIVE.load(Ordering::Relaxed) - before;

    let (mut eq_ns, mut count_ns) = (0.0, 0.0);
    if index {
        // 引く鍵は先に作っておく (format! の時間を測らない)
        let per = rows / tables;
        let keys: Vec<(usize, String)> = (0..20_000usize)
            .map(|j| {
                let x = j.wrapping_mul(2_654_435_761) % rows;
                (x % tables, format!("t{}_r{}", x % tables, (x / tables) % per))
            })
            .collect();
        // 5 回測って最小 (他の負荷の揺れを除く)
        eq_ns = f64::MAX;
        count_ns = f64::MAX;
        for _ in 0..5 {
            let t0 = Instant::now();
            let mut hit = 0usize;
            for (t, k) in &keys {
                hit += tbs[*t].where_eq("id", k.as_str()).find_one().unwrap().is_some() as usize;
            }
            eq_ns = eq_ns.min(t0.elapsed().as_nanos() as f64 / keys.len() as f64);
            assert_eq!(hit, keys.len(), "引けない行がある");
            let reps = 2_000;
            let t0 = Instant::now();
            let mut n = 0usize;
            for j in 0..reps {
                n += tbs[j % tables].where_eq("kind", "k").count().unwrap();
            }
            count_ns = count_ns.min(t0.elapsed().as_nanos() as f64 / reps as f64);
            assert!(n > 0);
        }
    }
    drop(tbs);
    drop(db);
    let mut rebuild_ms = 0.0;
    if index {
        let db = Database::open(&path).unwrap();
        let tbs: Vec<_> = (0..tables).map(|t| db.get_table(&format!("t{t}")).unwrap()).collect();
        let t0 = Instant::now();
        for (t, tb) in tbs.iter().enumerate() {
            assert!(tb.where_eq("id", format!("seed{t}")).find_one().unwrap().is_some());
        }
        rebuild_ms = t0.elapsed().as_secs_f64() * 1e3;
    }
    cleanup(&path);
    Run { heap, insert_per_s, eq_ns, count_ns, rebuild_ms }
}

/// 1 回の計測は別の process で (前の計測が残した確保 — epoch で後から解放されるもの — が、 次の計測のヒープの
/// 増分から引かれないように)。
fn run_in_child(tables: usize, rows: usize, index: bool) -> Run {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--child", &tables.to_string(), &rows.to_string(), if index { "1" } else { "0" }])
        .stderr(std::process::Stdio::null())
        .output()
        .unwrap();
    let line = String::from_utf8(out.stdout).unwrap();
    let f: Vec<f64> = line.split_whitespace().map(|x| x.parse().unwrap()).collect();
    assert_eq!(f.len(), 5, "child の出力: {line:?}");
    Run { heap: f[0] as isize, insert_per_s: f[1], eq_ns: f[2], count_ns: f[3], rebuild_ms: f[4] }
}

fn main() {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.first().map(String::as_str) == Some("--child") {
        let (tables, rows, index) = (raw[1].parse().unwrap(), raw[2].parse().unwrap(), raw[3] == "1");
        let r = run(tables, rows, index);
        println!("{} {} {} {} {}", r.heap, r.insert_per_s, r.eq_ns, r.count_ns, r.rebuild_ms);
        return;
    }
    let args: Vec<usize> = std::env::args().skip(1).filter_map(|a| a.parse().ok()).collect();
    let rows = args.first().copied().unwrap_or(200_000);
    let counts: Vec<usize> = if args.len() > 1 { args[1..].to_vec() } else { vec![1, 4, 16, 64] };
    println!("rows {rows}");
    println!("| tables | index heap | B/row | insert rows/s | where_eq(id) | where_eq(kind).count | reopen 後の初回 where_eq(id) |");
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    for t in counts {
        let with = run_in_child(t, rows, true);
        let without = run_in_child(t, rows, false);
        let idx = with.heap - without.heap;
        if std::env::var("ENCHU_BENCH_RAW").is_ok() {
            eprintln!("tables {t}: with index +{:.2} MB, without +{:.2} MB", with.heap as f64 / 1e6, without.heap as f64 / 1e6);
        }
        println!(
            "| {t} | {:.2} MB | {} | {:.0} | {:.0} ns | {:.1} µs | {:.1} ms |",
            idx as f64 / 1e6,
            idx / rows as isize,
            with.insert_per_s,
            with.eq_ns,
            with.count_ns / 1e3,
            with.rebuild_ms,
        );
    }
}
