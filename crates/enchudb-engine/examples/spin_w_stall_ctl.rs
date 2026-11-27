//! 対照: 読み手なしで書き手 1 本を回した時の書き込み 1 回の最長 (WAL の同期などの止まり)。 spin_w_cmp3 の 「書き手の止まり」 と比べる。
//! 形ごとに 10 秒 × 3: 今の形 / 案 E (係だけ動く)。 行 100 万、 値 1000 種。
use enchudb_engine::{Engine, ValueType};
use std::time::{Duration, Instant};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/spin_w_stall_ctl".into());
    let _ = std::fs::remove_dir_all(&dir);
    let mut eng = Engine::create_with_capacity(&dir, 1_001_024).unwrap();
    eng.define_table("t", 1_000_016).unwrap();
    eng.define_himo_in("t", "h", ValueType::Number, 1000).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 256 * 1024 * 1024).unwrap();
    let rows: Vec<u64> = (0..1_000_000).map(|_| eng.entity_in("t").unwrap()).collect();
    let hid = eng.himo_id("t.h").unwrap() as u16;
    for (i, &e) in rows.iter().enumerate() {
        eng.tie_to_by_id(e, hid, (i % 1000) as u32);
    }
    eng.oplog_sync().unwrap();
    eng.declare_history("t.h").unwrap();
    let mut r = Rng(7);
    for (name, e) in [("base", false), ("E", true), ("base", false), ("E", true), ("base", false), ("E", true)] {
        eng.history_set_e("t.h", e, None).unwrap();
        let start = Instant::now();
        let (mut worst, mut n) = (0u128, 0u64);
        while start.elapsed() < Duration::from_secs(10) {
            let a = Instant::now();
            eng.tie_to_by_id(rows[(r.next() % 1_000_000) as usize], hid, (r.next() % 1000) as u32);
            worst = worst.max(a.elapsed().as_nanos());
            n += 1;
        }
        println!("{name}: writes={n} worst={:.1} ms", worst as f64 / 1e6);
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
