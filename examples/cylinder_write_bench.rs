//! `LockFreeCylinder` の dense 事前確保をやめた時に、 **書き込み側がどれだけ損をするか**
//! (request23 案 F の採用条件)。
//!
//! `new(max_values)` は `min(max_values+1, DENSE_CAP)` 個の bucket を **open のたびに himo
//! ごとに** 確保する。 これを削ると open は `max_values` 非依存になる (himo 117 /
//! max_values 100k で 312 → 6.8 ms) が、 write が既存の doubling 成長経路を踏むようになる。
//!
//! `v10_lifecycle_bench` は `max_values = 100` なので**差が出る条件になっていない**。
//! ここでは **`max_values` が大きく、 実際に多数の distinct value を書く**形で測る。
//!
//! ```text
//! cargo run --release --example cylinder_write_bench
//! ```
//! `lockfree_cylinder.rs` の `hint` を変えて 3 通り (現行 / 0 / 64) 走らせて比較する。

use enchudb::{Engine, ValueType};
use std::time::Instant;

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

/// max_values を M と宣言した himo に、 distinct 個の異なる値を書く。
fn run(tag: &str, max_values: u32, distinct: u32, ents: u32) {
    let path = format!("/tmp/enchu_cylw_{}_{tag}.db", std::process::id());
    let _ = enchudb::db_files::remove_db(&path);

    let mut eng = Engine::create_with_capacity(&path, 262_144).unwrap();
    eng.define_table("t", 200_000).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, max_values).unwrap();

    let eids: Vec<u64> = (0..ents).map(|_| eng.entity_in("t").unwrap()).collect();

    // cold: 初回の書き込み (事前確保が無いと doubling がここで走る)
    let t = Instant::now();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "t.v", (i as u32) % distinct);
    }
    let cold = ms(t);

    // warm: 同じ値をもう一度 (bucket は既にある)
    let t = Instant::now();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "t.v", (i as u32) % distinct);
    }
    let warm = ms(t);

    // 昇順に触る = doubling を最大回数踏ませる形
    let t = Instant::now();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "t.v", (i as u32).min(distinct - 1));
    }
    let ascending = ms(t);

    eng.flush().unwrap();
    let t = Instant::now();
    let hits = eng.pull_raw("t.v", distinct / 2).len();
    let query = ms(t);

    let t = Instant::now();
    drop(eng);
    let drop_ms = ms(t);

    let t = Instant::now();
    let eng2 = Engine::open_readonly(&path).unwrap();
    let open = ms(t);
    drop(eng2);
    let _ = enchudb::db_files::remove_db(&path);

    println!(
        "{:>10} {:>9} {:>9} {:>10.1} {:>10.1} {:>11.1} {:>9.3} {:>8.1} {:>8.2}  hits={hits}",
        tag, max_values, distinct, cold, warm, ascending, query, drop_ms, open
    );
}

fn main() {
    println!(
        "{:>10} {:>9} {:>9} {:>10} {:>10} {:>11} {:>9} {:>8} {:>8}",
        "case", "max_val", "distinct", "cold ms", "warm ms", "昇順 ms", "query ms", "drop ms", "open ms"
    );
    let ents = 200_000;
    // 宣言だけ大きく、 実際に使う値は少ない (sinfo / kenning の形)
    run("大宣言/小", 100_000, 1_000, ents);
    // 宣言どおり全部使う (成長経路の最悪形)
    run("全部使う", 100_000, 100_000, ents);
    // 小さい宣言 (現行でも事前確保が軽い形)
    run("小宣言", 1_000, 1_000, ents);
}
