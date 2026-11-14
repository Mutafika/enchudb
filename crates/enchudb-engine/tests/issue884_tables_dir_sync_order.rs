//! #884: `tables` を置き換えた (rename した) 後、 checkpoint を進めた WAL の header を書き出すより前に、 親の directory を fsync して
//! rename を disk へ送る。 電源断で rename が戻る file system では、 checkpoint だけが進んで `tables` が前の中身に戻り、 #857 / #870 / #881
//! の前提 (消した行・足した枠が sidecar にある間だけ WAL を畳む) が崩れる。
//!
//! rename の戻りは crashsim の像では作れないので、 I/O の点 (`enchudb_oplog::io_point`) を通った順で確かめる: consumer の周期 1 回の中で
//! `oplog.fsync` (書き出し) → `sidecar.rename` (`tables`) → `tables.dir_sync` → (checkpoint を進める) → 次の `oplog.fsync` (進めた
//! checkpoint の header を書き出す)。 点の記録 (`ENCHU_FAULT_TRACE`) は process に 1 つなので、 この file の試験は 1 本だけ。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use std::time::{Duration, Instant};

#[test]
fn the_tables_rename_is_synced_before_the_checkpoint_is_written_out() {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let root = std::env::temp_dir().join(format!("enchudb-issue884-order-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let trace = root.join("trace");
    // 点の記録は最初の点で読まれるので、 engine を作る前に置く
    // SAFETY: この test binary の試験は 1 本で、 env を読む thread はまだ無い
    unsafe { std::env::set_var("ENCHU_FAULT_TRACE", &trace) };
    let db = root.join("db");
    let mut eng = Engine::create_growable_with_capacity(db.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let a = eng.himo_id("t.a").unwrap() as u16;
    let hold = eng.hold_consumer();
    // 行を足す (`next_local` が変わる = 次の周期が `tables` を置き換える)
    let e = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(e, a, 1u32);
    let read = || -> Vec<String> {
        std::fs::read_to_string(&trace)
            .unwrap_or_default()
            .lines()
            .map(|l| l.split('\t').nth(1).unwrap_or_default().to_string())
            .collect()
    };
    let start = read().len();
    let before = eng.stats().durable_lsn;
    drop(hold);
    // consumer の周期が 1 回まわる (Commit を打って書き出し、 `tables` を置き、 checkpoint を進める)
    let until = Instant::now() + Duration::from_secs(20);
    while eng.stats().durable_lsn <= before {
        assert!(Instant::now() < until, "前提: consumer の周期がまわらない");
        std::thread::sleep(Duration::from_millis(5));
    }
    // 進めた checkpoint の header を書き出す
    eng.oplog_sync().unwrap();
    drop(eng);
    let sites = read()[start..].to_vec();
    let _ = std::fs::remove_dir_all(&root);
    let i = sites
        .iter()
        .position(|s| s == "tables.dir_sync")
        .unwrap_or_else(|| panic!("`tables` を置き換えた後に directory を fsync していない: {sites:?}"));
    assert_eq!(sites[i - 1], "sidecar.rename", "directory の fsync が `tables` の rename の直後でない: {sites:?}");
    assert!(sites[..i].iter().any(|s| s == "oplog.fsync"), "前提: 周期の書き出しの後に `tables` を置いた: {sites:?}");
    assert!(
        sites[i + 1..].iter().any(|s| s == "oplog.fsync"),
        "前提: checkpoint を進めた header の書き出しが、 directory の fsync の後にある: {sites:?}"
    );
}
