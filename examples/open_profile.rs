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
    if std::env::var_os("ENCHU_OPEN_PROFILE_CRASH_CHILD").is_some() {
        // 親の Phase 3 から呼ばれる: 1 件書いて flush も Drop も通さず exit
        // (disk 上の clean flag を 0 のまま残す = 次 open で rebuild を強制する)。
        let mut eng = enchudb::Engine::open_standalone(&path).unwrap();
        let e = eng.entity().unwrap();
        eng.tie_text(e, "tag", "extra");
        std::mem::forget(eng);
        std::process::exit(0);
    }
    let _ = enchudb::db_files::remove_db(&path);

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
        eng.define_himo("tag", enchudb::ValueType::Tag, 1000);
        for i in 0..100 {
            let e = eng.entity().unwrap();
            eng.tie_text(e, "tag", &format!("v{}", i));
        }
        // schema 層が himo_reg にも入れるので、 多めに define して count を稼ぐ
        for i in 0..200 {
            eng.define_himo(&format!("h{}", i), enchudb::ValueType::Number, 100);
        }
        eng.flush().unwrap();
    }

    // Phase 2: clean shutdown 経路の open profile
    unsafe { std::env::set_var("ENCHU_OPEN_PROFILE", "1"); }
    eprintln!("=== reopen mode={} (clean shutdown) ===", mode);
    let t0 = Instant::now();
    drop(enchudb::Engine::open_standalone(&path).unwrap());
    eprintln!("[open_profile] total open wall-clock: {} ms", t0.elapsed().as_millis());

    // Phase 3: crash 相当の状態を作って再 open。 同 process で `mem::forget` すると
    // writer registry に残って次の open が WouldBlock になる (0.2x で入った in-process
    // 排他) ので、 子 process で書いて Drop を通さず exit する。
    {
        let st = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(&mode)
            .env("ENCHU_OPEN_PROFILE_CRASH_CHILD", "1")
            .status()
            .unwrap();
        assert!(st.success(), "crash child failed");
    }
    eprintln!("=== reopen mode={} (dirty = simulated crash) ===", mode);
    let t0 = Instant::now();
    drop(enchudb::Engine::open_standalone(&path).unwrap());
    eprintln!("[open_profile] total open wall-clock: {} ms", t0.elapsed().as_millis());

    let _ = enchudb::db_files::remove_db(&path);
}
