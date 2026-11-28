//! 調べ: 案 F の大きい束のデッキの読み (値 10 種、 1 行 10 回)。 t を古い → 新しいに動かして 1 回ずつの時間。
use enchudb_engine::{Engine, ValueType};
use std::time::Instant;
fn main() {
    let dir = "/home/user/swfdbg.db";
    let _ = std::fs::remove_dir_all(dir);
    let mut eng = Engine::create_with_capacity(dir, 1_001_024).unwrap();
    eng.define_table("t", 1_000_016).unwrap();
    eng.define_himo_in("t", "h", ValueType::Number, 10).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 256 << 20).unwrap();
    let rows: Vec<u64> = (0..1_000_000).map(|_| eng.entity_in("t").unwrap()).collect();
    let hid = eng.himo_id("t.h").unwrap() as u16;
    for (i, &e) in rows.iter().enumerate() {
        eng.tie_to_by_id(e, hid, (i % 10) as u32);
    }
    eng.declare_history("t.h").unwrap();
    let mut x = 7u64;
    for _ in 0..10_000_000u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        eng.tie_to_by_id(rows[(x % 1_000_000) as usize], hid, ((x >> 32) % 10) as u32);
    }
    for (name, f) in [("E", false), ("F", true)] {
        eng.history_set_e("t.h", true, None).unwrap();
        eng.history_set_f("t.h", f).unwrap();
        let a = Instant::now();
        eng.history_observe("t.h", &[0]).unwrap();
        let obs = a.elapsed().as_millis();
        let detail = eng.history_e_deck_detail("t.h").unwrap();
        let clock = eng.history_clock("t.h").unwrap();
        let h0 = eng.history(rows[1000], "t.h").unwrap()[0].time;
        let mut line = format!("{name}: observe {obs} ms, deck {:?}, big={} |", detail, eng.history_e_deck_big("t.h", 0).unwrap());
        for q in [0u64, 1, 3, 5, 7, 9, 10] {
            let t = h0 + (clock - h0) * q / 10;
            let a = Instant::now();
            let n = eng.pull_at("t.h", 0, t).unwrap().len();
            line += &format!(" t={q}/10: {:.1} ms ({n})", a.elapsed().as_secs_f64() * 1000.0);
        }
        println!("{line}");
        eng.history_set_e("t.h", false, None).unwrap();
    }
    let clock = eng.history_clock("t.h").unwrap();
    let h0 = eng.history(rows[1000], "t.h").unwrap()[0].time;
    let mut line = "base:".to_string();
    eng.pull_at("t.h", 0, h0).unwrap();
    for q in [0u64, 1, 3, 5, 7, 9, 10] {
        let t = h0 + (clock - h0) * q / 10;
        let a = Instant::now();
        let n = eng.pull_at("t.h", 0, t).unwrap().len();
        line += &format!(" t={q}/10: {:.1} ms ({n})", a.elapsed().as_secs_f64() * 1000.0);
    }
    println!("{line}");
}
