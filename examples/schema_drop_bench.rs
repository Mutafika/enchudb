//! schema `Database` の drop コスト bench (#261)。
//!
//! 書き込みゼロの rw session (= 開いて読んで閉じるだけ) が、 `impl Drop for Database`
//! の無条件 `persist_schema()` で `.schema` fsync + engine flush を毎回払っている。
//! 消費側 (kenning の増分 update、 `sf` の条件付き更新) は 1 コマンド 1 process なので
//! これが定数として効く。
//!
//! ```text
//! cargo run --release --example schema_drop_bench             # 合成 DB
//! cargo run --release --example schema_drop_bench <db_dir>    # 既存 DB (必ず隔離コピーで)
//! ```
use enchudb::schema::Database;
use enchudb::Engine;
use std::time::{Duration, Instant};

const REPS: usize = 10;
const WARMUP: usize = 3;

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn build(path: &str) {
    let _ = enchudb::db_files::remove_db(path);
    let mut db = Database::create_with_capacity(path, 65_536).unwrap();
    for t in 0..7 {
        let mut tb = db.table(&format!("t{t}"));
        for c in 0..7 {
            tb = tb.leaf(&format!("c{c}"));
        }
        tb.build().unwrap();
    }
    for t in 0..7 {
        let tbl = db.get_table(&format!("t{t}")).unwrap();
        for i in 0..565 {
            let mut row = tbl.insert();
            for c in 0..7 {
                row = row.set(&format!("c{c}"), format!("v{i}/{c}"));
            }
            row.commit().unwrap();
        }
    }
}

fn min_of(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::INFINITY, f64::min)
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn measure<T>(open: impl Fn() -> T) -> (Vec<f64>, Vec<f64>) {
    let (mut o, mut d) = (vec![], vec![]);
    for i in 0..REPS + WARMUP {
        let t = Instant::now();
        let h = open();
        let t_open = t.elapsed();
        let t = Instant::now();
        drop(h);
        let t_drop = t.elapsed();
        if i >= WARMUP {
            o.push(ms(t_open));
            d.push(ms(t_drop));
        }
    }
    (o, d)
}

/// sidecar の mtime が drop で動いたか (= 書き込みゼロなのに書き直しているか)。
fn sidecar_mtimes(path: &str) -> Vec<(String, std::time::SystemTime)> {
    ["schema", "tables", "segments"]
        .iter()
        .filter_map(|n| {
            let p = std::path::Path::new(path).join(n);
            std::fs::metadata(&p).ok()?.modified().ok().map(|m| (n.to_string(), m))
        })
        .collect()
}

fn main() {
    let arg = std::env::args().nth(1);
    let path = match &arg {
        Some(p) => p.clone(),
        None => {
            let p = "/tmp/enchu_schema_drop_bench.db".to_string();
            build(&p);
            p
        }
    };
    {
        let eng = Engine::open_readonly(&path).unwrap();
        println!(
            "db: {path}  entities={} himos={} tables={}",
            eng.entity_count(),
            eng.himo_count(),
            eng.list_user_tables().len()
        );
    }

    // 書き込みゼロの rw open → drop で sidecar が書き直されるか
    let before = sidecar_mtimes(&path);
    {
        let _db = Database::open(&path).unwrap();
    }
    let after = sidecar_mtimes(&path);
    let touched: Vec<&str> = before
        .iter()
        .zip(after.iter())
        .filter(|(b, a)| b.1 != a.1)
        .map(|(b, _)| b.0.as_str())
        .collect();
    println!("write-zero rw open→drop で mtime が動いた sidecar: {touched:?}");

    println!("\n{:<32} {:>10} {:>10} {:>10} {:>10}", "mode", "open min", "open med", "drop min", "drop med");
    let report = |name: &str, (mut o, mut d): (Vec<f64>, Vec<f64>)| {
        println!(
            "{:<32} {:>8.2}ms {:>8.2}ms {:>8.2}ms {:>8.2}ms",
            name,
            min_of(&o),
            median(&mut o),
            min_of(&d),
            median(&mut d)
        );
    };
    report("Engine::open_standalone", measure(|| Engine::open_standalone(&path).unwrap()));
    report("Database::open (rw, 書き込みなし)", measure(|| Database::open(&path).unwrap()));
    report("Database::open_readonly", measure(|| Database::open_readonly(&path).unwrap()));

    if arg.is_none() {
        let _ = enchudb::db_files::remove_db(&path);
    }
}
