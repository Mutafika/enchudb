//! 調べ: 案 E を切って入れ直した後も係が動くか。
use enchudb_engine::{Engine, ValueType};
use std::time::Duration;
fn main() {
    let dir = "/home/user/swe_restart.db";
    let _ = std::fs::remove_dir_all(dir);
    let mut eng = Engine::create_with_capacity(dir, 300_000).unwrap();
    eng.define_table("t", 200_000).unwrap();
    eng.define_himo_in("t", "h", ValueType::Number, 100).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 64 << 20).unwrap();
    let rows: Vec<u64> = (0..100_000).map(|_| eng.entity_in("t").unwrap()).collect();
    let hid = eng.himo_id("t.h").unwrap() as u16;
    eng.declare_history("t.h").unwrap();
    for round in 0..3 {
        eng.history_set_e("t.h", true, Some(256)).unwrap();
        let _ = eng.history_e_stats("t.h");
        for i in 0..200_000u64 {
            eng.tie_to_by_id(rows[(i * 7919 % 100_000) as usize], hid, (i % 100) as u32);
        }
        std::thread::sleep(Duration::from_millis(300));
        println!("round {round}: {:?}", eng.history_e_stats("t.h").unwrap());
        eng.history_set_e("t.h", false, None).unwrap();
    }
}
