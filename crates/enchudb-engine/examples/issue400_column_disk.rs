//! #400: 表を順に作って行を入れた DB の、 列 (himo/) の実ディスク。 cell を離さない形 (旧) / 離す形 / 旧形式を
//! `migrate_column_pad` で移した形 を比べる。 書き込みと開き直しの時間も出す。
//!
//! 実行: cargo run --release -p enchudb-engine --example issue400_column_disk [表の数] [1 表の行数] [1 表の列数]

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use std::os::unix::fs::MetadataExt;
use std::time::Instant;

fn dir_physical(dir: &std::path::Path) -> (u64, u64) {
    let (mut phys, mut len) = (0, 0);
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let m = e.metadata().unwrap();
        phys += m.blocks() * 512;
        len += m.len();
    }
    (phys, len)
}

fn build(path: &str, pad: bool, tables: u32, rows: u32, cols: u32) -> std::time::Duration {
    let _ = std::fs::remove_dir_all(path);
    let mut eng = Engine::create_growable_opts(
        path,
        GrowableOptions { max_entities: 16_000_000, column_pad: Some(pad), ..Default::default() },
    )
    .unwrap();
    let mut ids = Vec::new();
    for t in 0..tables {
        let name = format!("t{t}");
        eng.define_table(&name, rows + 16).unwrap();
        let mut hs = Vec::new();
        for c in 0..cols {
            eng.define_himo_in(&name, &format!("c{c}"), ValueType::Number, 0).unwrap();
            hs.push(eng.himo_id(&format!("{name}.c{c}")).unwrap() as u16);
        }
        ids.push((name, hs));
    }
    let t0 = Instant::now();
    for (name, hs) in &ids {
        for i in 0..rows {
            let e = eng.entity_in(name).unwrap();
            for &h in hs {
                eng.tie_to_by_id(e, h, i);
            }
        }
    }
    eng.flush().unwrap();
    t0.elapsed()
}

fn main() {
    let args: Vec<u32> = std::env::args().skip(1).map(|s| s.parse().unwrap()).collect();
    let (tables, rows, cols) = (args.first().copied().unwrap_or(8), args.get(1).copied().unwrap_or(100_000), args.get(2).copied().unwrap_or(5));
    println!("表 {tables} × 行 {rows} × 列 {cols}");
    for (label, pad, migrate) in [("旧形式", false, false), ("離す", true, false), ("旧形式を移行", false, true)] {
        let path = format!("/tmp/enchudb-issue400-bench-{}", label.len());
        let took = build(&path, pad, tables, rows, cols);
        let mut extra = String::new();
        if migrate {
            let t = Instant::now();
            let n = Engine::migrate_column_pad(&path).unwrap();
            extra = format!("  移行 {n} 本 {:?}", t.elapsed());
        }
        let t = Instant::now();
        let eng = Engine::open_readonly(&path).unwrap();
        let hits = eng.pull_raw(&format!("t{}.c0", tables - 1), 7u32).len();
        let open = t.elapsed();
        drop(eng);
        let (phys, len) = dir_physical(&std::path::Path::new(&path).join("himo"));
        println!(
            "{label:<8} 列の実ディスク {:>8.1} MB (見かけ {:>8.1} MB)  書き込み {:>7.1?}  開いて 1 回引く {:>6.1?} (hit {hits}){extra}",
            phys as f64 / 1e6,
            len as f64 / 1e6,
            took,
            open
        );
        let _ = std::fs::remove_dir_all(&path);
    }
}
