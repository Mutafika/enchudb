//! #378: Tag 列に値を書く速さ (辞書に入れる経路)。 1 thread で新しい値 / 8 thread で別々の新しい値 / 8 thread で同じ値を
//! ずらして追う (issue の再現の形)。 辞書の語数も出す。
//!
//! 実行: cargo run --release -p enchudb-engine --example issue378_vocab_insert_bench

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use std::sync::{Arc, Barrier};
use std::time::Instant;

fn run(label: &str, threads: u32, per: u32, shared: bool) {
    let path = format!("/tmp/enchudb-issue378-bench-{}-{}", std::process::id(), label.len());
    let _ = std::fs::remove_dir_all(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions { max_entities: 4_000_000, ..Default::default() }).unwrap();
    eng.define_table("t", 3_000_000).unwrap();
    eng.define_himo_in("t", "v", ValueType::Tag, 0).unwrap();
    let base = eng.vocab_usage().entries;
    let eng = Arc::new(eng);
    let hid = eng.himo_id("t.v").unwrap() as u16;
    let start = Arc::new(Barrier::new(threads as usize + 1));
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let (eng, start) = (eng.clone(), start.clone());
            std::thread::spawn(move || {
                let rows: Vec<u64> = (0..per).map(|_| eng.entity_in("t").unwrap()).collect();
                let vals: Vec<String> = (0..per)
                    .map(|i| if shared { format!("s{}", (7 * i + 1001 * t) % per) } else { format!("u{t}-{i}") })
                    .collect();
                start.wait();
                for i in 0..per as usize {
                    eng.tie_text_to_by_id(rows[i], hid, &vals[i]);
                }
            })
        })
        .collect();
    start.wait();
    let t0 = Instant::now();
    for h in hs {
        h.join().unwrap();
    }
    let took = t0.elapsed();
    let n = threads as u64 * per as u64;
    println!(
        "{label:<22} {:>7.0} ns/書き込み  ({n} 書き込み、 {:?}、 辞書の語 {})",
        took.as_nanos() as f64 / n as f64,
        took,
        eng.vocab_usage().entries - base
    );
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

fn main() {
    run("1 thread 新しい値", 1, 300_000, false);
    run("8 thread 別々の値", 8, 100_000, false);
    run("8 thread 同じ値を追う", 8, 12_000, true);
}
