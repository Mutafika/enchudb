//! open split bench — **open / 最初の読み / drop を分離**して測る (0.26.1)。
//!
//! `open_cost_bench` は 「開く → 小さな query 1 本 → 閉じる」 を 1 つの数字で出すので、
//! **D2 (遅延 HimoStore) がコストを open から初回タッチへ移した**ことが見えない。
//! 実際に consumer が払う額は **触る列の本数**で決まるので、 そこを切り分ける。
//!
//! 消費側 (`sf`) の 「1 行読むと +0.28 ms」 という報告がこの bench の動機で、 内訳は
//! 「12 列 × ~20 µs = segment open 12 本」 だった。 **行数には比例しない** (`col()` は
//! `OnceLock` なので himo 1 本につき 1 回)。
//!
//! ```text
//! cargo run --release --example open_split_bench
//! ```
use enchudb::{Engine, ValueType};
use std::time::Instant;

const HIMOS: u32 = 117; // `sf` の形
const ENTS: u32 = 9211;
const REPS: usize = 40;
const WARMUP: usize = 5;

fn us(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

fn build(path: &str) {
    let _ = enchudb::db_files::remove_db(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("t", 60_000).unwrap();
    for h in 0..HIMOS {
        eng.define_himo_in("t", &format!("h{h}"), ValueType::Number, 0).unwrap();
    }
    for i in 0..ENTS {
        let e = eng.entity_in("t").unwrap();
        for h in 0..HIMOS {
            eng.tie(e, &format!("t.h{h}"), i + h);
        }
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
}

fn min_of(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::INFINITY, f64::min)
}

/// `touch` 本の himo を `rows` 行ぶん読む。 列名は**事前生成**する
/// (ループ内で `format!` すると読みの数字が String 割り当てに埋もれる)。
fn measure(path: &str, touch: u32, rows: u32) -> (f64, f64, f64) {
    let names: Vec<String> = (0..touch).map(|h| format!("t.h{h}")).collect();
    let (mut o, mut r, mut d) = (vec![], vec![], vec![]);
    for i in 0..REPS {
        let t = Instant::now();
        let eng = Engine::open_readonly(path).unwrap();
        let t_open = t.elapsed();

        let t = Instant::now();
        for row in 0..rows {
            for n in &names {
                std::hint::black_box(eng.get(row.into(), n));
            }
        }
        let t_read = t.elapsed();

        let t = Instant::now();
        drop(eng);
        let t_drop = t.elapsed();

        if i >= WARMUP {
            o.push(us(t_open));
            r.push(us(t_read));
            d.push(us(t_drop));
        }
    }
    (min_of(&o), min_of(&r), min_of(&d))
}

fn main() {
    let base = format!("/tmp/enchu_opensplit_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let p = format!("{base}/db");
    build(&p);

    println!("open split bench — himo {HIMOS} / entity {ENTS} / min of {}", REPS - WARMUP);
    println!("{:>6} {:>6} {:>10} {:>12} {:>10} {:>10}", "列", "行", "open", "読み", "drop", "計");
    // ① 触る himo 数を振る (行は 1) — ~20 µs/himo が出る
    for touch in [1u32, 12, 30, HIMOS] {
        let (o, r, d) = measure(&p, touch, 1);
        println!("{touch:>6} {:>6} {o:>10.1} {r:>12.1} {d:>10.1} {:>10.1}", 1, o + r + d);
    }
    // ② 行数を振る (列は 12 固定) — 行数には比例しないことが出る
    for rows in [10u32, 100, 1000] {
        let (o, r, d) = measure(&p, 12, rows);
        println!("{:>6} {rows:>6} {o:>10.1} {r:>12.1} {d:>10.1} {:>10.1}", 12, o + r + d);
    }
    println!("\n① は himo 1 本 = file 1 本の open (~20 µs)。 ② は固定 + 行あたり ~0.17 µs。");
    println!("`col()` は OnceLock なので、 himo 1 本につき open は Engine 1 つあたり 1 回。");

    let _ = std::fs::remove_dir_all(&base);
}
