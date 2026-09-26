//! 再帰 (under / above) の購読の書き込み + poll の速さ。 社員 N 人の 10 分木、 付け替え 8 割 + seed の切り替え 2 割。
//! `cargo run --release -p enchudb-schema --example hier_bench -- <under|above> <batch> [N]`
//! batch = 1 回の poll にまとめる書き込みの数 (1 = 1 件ずつ)。

use enchudb_schema::{Database, Value};
use std::time::Instant;

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let up = args.get(1).map(String::as_str) == Some("above");
    let batch: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let n: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let ops: usize = if batch == 1 { 50_000 } else { batch * 10 };

    let path = format!("target/hier_bench_{}.db", std::process::id());
    let mut db = Database::create_with_capacity(&path, n as u32 + 16).unwrap();
    db.table("emps").number("id").number("seed").ref_to("boss", "emps").primary_key("id").build().unwrap();
    let t = db.get_table("emps").unwrap();
    let mut rng = Rng(0x5eed_0000_0000_0001);
    let mut emps: Vec<u64> = Vec::with_capacity(n as usize);
    let mut seeds: Vec<usize> = Vec::new();
    for i in 0..n {
        let s = i % (n / 100) == 7;
        let mut b = t.insert().set("id", i as i64).set("seed", s as i64);
        if i > 0 {
            b = b.set("boss", Value::Ref(emps[((i - 1) / 10) as usize]));
        }
        emps.push(b.commit().unwrap());
        if s {
            seeds.push(i as usize);
        }
    }
    let seed_q = t.where_eq("seed", 1i64);
    let live = if up { t.all().above("boss", seed_q) } else { t.all().under("boss", seed_q) }.subscribe().unwrap();
    let first = live.poll().added.len();

    let start = Instant::now();
    let mut moved = 0usize;
    for k in 0..ops {
        if rng.below(10) < 8 {
            let i = 1 + rng.below(n - 1) as usize;
            t.entity(emps[i]).set("boss", Value::Ref(emps[rng.below(i as u64) as usize])).commit().unwrap();
        } else {
            let j = rng.below(seeds.len() as u64) as usize;
            t.entity(emps[seeds[j]]).set("seed", 0i64).commit().unwrap();
            let s = rng.below(n) as usize;
            t.entity(emps[s]).set("seed", 1i64).commit().unwrap();
            seeds[j] = s;
        }
        if (k + 1) % batch == 0 {
            let d = live.poll();
            moved += d.added.len() + d.removed.len();
        }
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        "{} batch={batch} n={n}: {:.1} 万 ops/s (最初の答え {first}、 差分 {moved})",
        if up { "above" } else { "under" },
        ops as f64 / secs / 1e4
    );
    drop(live);
    drop(t);
    drop(db);
    // DB はディレクトリ
    let _ = std::fs::remove_dir_all(&path);
    for ext in ["", ".oplog", ".tables", ".lock", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{ext}"));
    }
}
