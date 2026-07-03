//! 0.7.0 破壊テスト集 — 「使い物になるか」 を確かめる。
//!
//! 1. oplog 末尾 garbage / truncate からの recover (= crash 耐性)
//! 2. writer flock 排他 + open_readonly 並走 (= multi-process)
//! 3. eid 空間飽和 (= reserved table auto-clamp が小 max_entities でも安全か)
//! 4. transfer 冪等性 + ack 未来値 (= 危険入力で reclaim が暴走しない)
//! 5. stress: 10k row insert → transfer → reclaim 1 cycle (= leak しない)

use enchudb_engine::{Engine, HimoType};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-destructive-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

// ─────────────────────────────────────────────────────────────────────
// 1. oplog 末尾 garbage / truncate → reopen
// ─────────────────────────────────────────────────────────────────────

#[test]
fn oplog_tail_garbage_is_dropped_on_reopen() {
    let path = tmp_path("oplog_garbage");
    cleanup(&path);

    // 5 件 tie してから oplog file の末尾に garbage 64 byte を追記。
    {
        let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
        eng.define_table("notes", 1000).unwrap();
        eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
        let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
        for i in 1u32..=5 {
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
        }
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
    }

    // oplog 末尾に garbage 64 byte 追記
    {
        use std::io::Write;
        let oplog_path = format!("{}.oplog", path);
        let mut f = std::fs::OpenOptions::new()
            .write(true).append(true).open(&oplog_path).unwrap();
        f.write_all(&[0xDEu8; 64]).unwrap();
        f.sync_all().unwrap();
    }

    // reopen → panic せず、 5 件全部読めるはず
    let eng = Engine::open_standalone(&path).unwrap();
    // pull(himo, value) で値 1..=5 がそれぞれ 1 entity 持ってるか
    let mut found = Vec::new();
    for v in 1u32..=5 {
        if !eng.pull("notes.note", v).is_empty() {
            found.push(v);
        }
    }
    assert_eq!(found, vec![1, 2, 3, 4, 5], "tail garbage must not corrupt earlier records");

    cleanup(&path);
}

#[test]
fn oplog_truncation_mid_record_recovers() {
    let path = tmp_path("oplog_trunc");
    cleanup(&path);

    let oplog_path = format!("{}.oplog", path);

    // 10 件 tie
    {
        let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
        eng.define_table("notes", 1000).unwrap();
        eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
        let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
        for i in 1u32..=10 {
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
        }
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
    }

    // oplog を末尾 32 byte 削る (= 最後の record を半分に切る)
    let len = std::fs::metadata(&oplog_path).unwrap().len();
    assert!(len > 64);
    let new_len = len - 32;
    let f = std::fs::OpenOptions::new().write(true).open(&oplog_path).unwrap();
    f.set_len(new_len).unwrap();
    f.sync_all().unwrap();

    // reopen → panic せず開く (= 部分 record は recovery で捨てられる)
    let eng = Engine::open_standalone(&path).unwrap();
    let mut found = Vec::new();
    for v in 1u32..=10 {
        if !eng.pull("notes.note", v).is_empty() {
            found.push(v);
        }
    }
    // 少なくとも 0 件以上、 10 件以下 (= 部分 truncate なので 8-10 のどれか)。
    assert!(!found.is_empty() && found.len() <= 10,
            "partial truncation should leave a valid prefix, got {} values", found.len());

    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────────────
// 2. writer 排他 + readonly 並走
// ─────────────────────────────────────────────────────────────────────

#[test]
fn double_writer_open_blocks_until_first_drops() {
    // 0.7.0 設計: flock(LOCK_EX) は **blocking** (SQLite と同様、 取れるまで待つ)。
    // #80: ただし **同一プロセス**の二重 open は flock では検知できず無期限
    // ハングになるため、 プロセス内 registry で fast-fail する仕様に変更。
    // 別プロセス writer との blocking 排他は従来通り。
    let path = tmp_path("double_writer");
    cleanup(&path);

    let eng1 = Engine::create_with_capacity(&path, 65_536).unwrap();

    // 同一プロセスの 2 番目の writer open は block せず即エラー
    let started = std::time::Instant::now();
    match Engine::open_standalone(&path) {
        Ok(_) => panic!("second writer open in the same process should fail fast"),
        Err(e) => {
            assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock);
            assert!(e.to_string().contains("already open for writing in this process"));
        }
    }
    assert!(
        started.elapsed() < std::time::Duration::from_millis(500),
        "fast-fail should not block"
    );

    // 1 番目を drop すれば 2 番目が成功する
    drop(eng1);
    let eng2 = Engine::open_standalone(&path);
    assert!(eng2.is_ok(), "second writer should succeed after first drop");
    drop(eng2);

    cleanup(&path);
}

#[test]
fn readonly_open_succeeds_alongside_writer() {
    let path = tmp_path("readonly_alongside");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", 42u32);
    eng.flush_writes();

    // writer open 中に readonly open は通る
    let ro = Engine::open_readonly(&path).unwrap();
    assert!(ro.is_readonly());

    // ro から読める (pull で逆引き)
    let eids = ro.pull("notes.note", 42);
    assert_eq!(eids.len(), 1);
    assert_eq!(eids[0], enchudb_oplog::eid_local(e));

    drop(eng);
    drop(ro);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────────────
// 3. eid 空間飽和 + reserved table overflow 安全性
// ─────────────────────────────────────────────────────────────────────

#[test]
fn enable_sync_clamps_when_eid_space_is_tight() {
    // max_entities をかなり小さくして enable_sync_tables が破綻しないか
    let path = tmp_path("eid_tight");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 4096).unwrap();
    eng.define_table("notes", 1000).unwrap();  // user table 1000
    // remaining = 4096 - 1000 = 3096 で enable_sync (clamp 走るはず)
    eng.enable_sync_tables().unwrap();

    assert!(eng.sync_tables_enabled());
    // _sync_ops / _sync_peers が居る (size は clamp されてる)
    let tables: Vec<String> = eng.list_tables().into_iter().map(|(_, n, _, _)| n).collect();
    assert!(tables.iter().any(|n| n == "_sync_ops"));
    assert!(tables.iter().any(|n| n == "_sync_peers"));

    cleanup(&path);
}

#[test]
fn entity_in_returns_err_when_table_eid_range_exhausted() {
    let path = tmp_path("table_full");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("tiny", 4).unwrap();  // 4 row しか入らない table
    eng.define_himo_in("tiny", "v", HimoType::Number, 0).unwrap();

    // 4 件は OK
    for _ in 0..4 {
        eng.entity_in("tiny").unwrap();
    }
    // 5 件目は範囲外で Err
    let result = eng.entity_in("tiny");
    assert!(result.is_err(),
            "5th entity_in on size=4 table should fail, got {:?}", result);

    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────────────
// 4. transfer 冪等性 + ack 未来値
// ─────────────────────────────────────────────────────────────────────

#[test]
fn ack_with_future_lsn_does_not_corrupt_reclaim() {
    let path = tmp_path("future_ack");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    for i in 1u32..=5 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();

    let pending_before = eng.pending_sync_ops(0).len();
    assert!(pending_before > 0);

    // peer ack を未来の lsn (= 999) に設定
    eng.ack_sync(1, 999).unwrap();
    // sync_watermark は 999 になる (誰も他に居ないから min = 999)
    assert_eq!(eng.sync_watermark(), 999);

    // reclaim → 全 row 消える (lsn < 999 全部)、 panic / overflow しない
    let purged = eng.reclaim_sync_ops();
    assert!(purged > 0, "should purge some rows, got {purged}");

    // pending(0) は空
    let after = eng.pending_sync_ops(0);
    assert_eq!(after.len(), 0, "all should be reclaimed");

    cleanup(&path);
}

#[test]
fn transfer_after_reclaim_continues_lsn_monotonically() {
    let path = tmp_path("lsn_mono");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    for i in 1u32..=3 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    let lsn1 = eng.current_sync_lsn();
    assert!(lsn1 > 0);

    // 全 ack + reclaim
    eng.ack_sync(1, lsn1).unwrap();
    eng.reclaim_sync_ops();

    // 追加 3 件 tie + transfer
    for i in 4u32..=6 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    let lsn2 = eng.current_sync_lsn();
    assert!(lsn2 > lsn1,
            "lsn must advance after reclaim+transfer cycle: {lsn1} → {lsn2}");

    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────────────
// 5. stress: 10k row insert → transfer → reclaim
// ─────────────────────────────────────────────────────────────────────

#[test]
fn stress_10k_cycle() {
    let path = tmp_path("stress_10k");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 128_000).unwrap();
    eng.define_table("rows", 50_000).unwrap();
    eng.define_himo_in("rows", "v", HimoType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 64 * 1024 * 1024).unwrap();

    // 10_000 件 insert
    for i in 0u32..10_000 {
        let e = eng.entity_in("rows").unwrap();
        eng.tie_to(e, "rows.v", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();

    // 0.8.0 以降、 `concurrentize_with_oplog` の background consumer thread が
    // fsync 後に自動で `transfer_oplog_to_sync_ops` を呼ぶ (= engine.rs:4093)。
    // 手動呼び出しは idempotent な insurance としてのみ意味を持ち、 戻り値は
    // background との race で 0 になりうる (= 旧 0.7.0 期 test の `transferred >=
    // 10_000` assert は 0.8.0 以降 flaky)。 真の確認は `pending_sync_ops` か
    // `current_sync_lsn` で行う (= どちらも race の影響を受けない)。
    eng.transfer_oplog_to_sync_ops(); // idempotent insurance
    let pending_before_reclaim = eng.pending_sync_ops(0).len();
    assert!(pending_before_reclaim >= 10_000,
            "10k+ records should be in sync queue, got {pending_before_reclaim}");

    let lsn_full = eng.current_sync_lsn();
    assert!(lsn_full >= 10_000);

    // 半分まで ack + reclaim
    let half = lsn_full / 2;
    eng.ack_sync(1, half).unwrap();
    let purged = eng.reclaim_sync_ops();
    assert!(purged > 0);
    let remaining = eng.pending_sync_ops(0).len();
    // reclaim 後の残り件数は約 half 件
    assert!(remaining > 0 && remaining < 10_000);

    // 全 ack + reclaim でほぼ 0 になる
    eng.ack_sync(1, lsn_full).unwrap();
    eng.reclaim_sync_ops();
    let final_pending = eng.pending_sync_ops(0).len();
    assert!(final_pending < 100,
            "almost all should be reclaimed, got {final_pending}");

    cleanup(&path);
}
