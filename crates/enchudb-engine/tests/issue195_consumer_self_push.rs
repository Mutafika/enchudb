//! #195: write queue が満杯の burst 中に consumer の bridge tick が走っても
//! livelock しないこと。
//!
//! fix 前: bridge (`transfer_oplog_to_sync_ops`、consumer thread 上) が record
//! ごとの `entity_in("_sync_ops")` で `Op::EntityCreated` を blocking push し、
//! 唯一の drainer (= consumer 自身) が満杯 queue に blocking → producer と
//! yield spin の自縄自縛。小 queue (#116 scaled default) で顕在化した。
//!
//! watchdog 形式: burst を別 thread で走らせ、制限時間内に完走しなければ red。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn tmp_path() -> String {
    format!(
        "/tmp/enchudb-issue195-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

#[test]
fn bridge_tick_during_full_queue_burst_does_not_livelock() {
    let path = tmp_path();
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 1_048_576).unwrap();
    eng.define_table("t", 200_000).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    // 小 queue (1024) + 大 burst (40k) — consumer の 100ms bridge tick が burst に
    // ほぼ確実に重なる構成。fix 前はここで livelock した (45s 超)。
    let eng: Arc<Engine> =
        Engine::concurrentize_with_oplog_queue(eng, 256 * 1024 * 1024, 1024).unwrap();
    eng.set_peer_id(1);

    const N: u32 = 40_000;
    let writer = {
        let eng = eng.clone();
        std::thread::spawn(move || {
            for i in 0..N {
                let e = eng.entity_in("t").unwrap();
                eng.tie_to(e, "t.v", i % 1000);
            }
            eng.oplog_commit();
            eng.flush_writes();
            eng.oplog_sync().unwrap();
        })
    };

    // watchdog: 健全なら 1-2 秒、fix 前は無限。30s で見切る。
    let deadline = Instant::now() + Duration::from_secs(30);
    while !writer.is_finished() {
        assert!(
            Instant::now() < deadline,
            "#195 livelock: {N} tie burst (queue 1024) が 30s で完走しない"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    writer.join().unwrap();

    // bridge も完走すること (lsn が N に到達)
    let deadline = Instant::now() + Duration::from_secs(30);
    while eng.current_sync_lsn() < N {
        assert!(Instant::now() < deadline, "bridge が {N} record を移し切らない");
        std::thread::sleep(Duration::from_millis(50));
    }

    drop(eng);
    cleanup(&path);
}
