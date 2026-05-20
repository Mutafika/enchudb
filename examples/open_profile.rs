//! issue2 調査: open 経路の page reclaim を load step 別に分解する。
//!
//! 使い方:
//!   cargo run --release --example open_profile -- [default|cap65k|cap1M]
//!
//! 動作:
//!   1. 指定 capacity で DB を作る + tag を 100 件 vocab insert (count > 0 にする)
//!   2. drop して再 open (ENCHU_OPEN_PROFILE=1 で load_from_backing 計装)
//!   3. open 時の各 step の Δreclaim を stderr に dump

use std::time::Instant;

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "default".to_string());
    let path = format!("/tmp/enchudb_open_profile_{}.db", mode);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));

    // Phase 1: build a non-empty DB (count > 0 になるよう tag を insert)
    {
        let mut eng = match mode.as_str() {
            "default" => enchudb::Engine::create_growable(&path).unwrap(),
            "cap65k" => enchudb::Engine::create_growable_with_capacity(&path, 65_536).unwrap(),
            "cap1M" => enchudb::Engine::create_growable_with_capacity(&path, 1_048_576).unwrap(),
            other => {
                eprintln!("unknown mode: {}", other);
                std::process::exit(2);
            }
        };
        // vocab に entry が乗らないと rebuild_index は走らない。 Tag himo に
        // string を tie するため tie_text を使う。
        eng.define_himo("tag", enchudb::HimoType::Tag, 1000);
        for i in 0..100 {
            let e = eng.entity();
            eng.tie_text(e, "tag", &format!("v{}", i));
        }
        // schema 層が himo_reg にも入れるので、 多めに define して count を稼ぐ
        for i in 0..200 {
            eng.define_himo(&format!("h{}", i), enchudb::HimoType::Number, 100);
        }
        eng.flush().unwrap();
    }

    // Phase 2: clean shutdown 経路の open profile
    unsafe { std::env::set_var("ENCHU_OPEN_PROFILE", "1"); }
    eprintln!("=== reopen mode={} (clean shutdown) ===", mode);
    let t0 = Instant::now();
    drop(enchudb::Engine::open_standalone(&path).unwrap());
    eprintln!("[open_profile] total open wall-clock: {} ms", t0.elapsed().as_millis());

    // Phase 3: crash 相当 (insert 後 flush 呼ばず forget) の状態を作って再 open
    {
        let mut eng = enchudb::Engine::open_standalone(&path).unwrap();
        let e = eng.entity();
        eng.tie_text(e, "tag", "extra");
        // Drop も flush も呼ばずに leak。 graceful close 経路を回避し、
        // disk 上の clean flag を 0 のまま残す = 次 open で rebuild を強制する
        std::mem::forget(eng);
    }
    eprintln!("=== reopen mode={} (dirty = simulated crash) ===", mode);
    let t0 = Instant::now();
    drop(enchudb::Engine::open_standalone(&path).unwrap());
    eprintln!("[open_profile] total open wall-clock: {} ms", t0.elapsed().as_millis());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
}
