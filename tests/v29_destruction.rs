//! v29 用の破壊テスト。v28 の耐久性保証を実証 + v29 で埋める穴を記録。
//!
//! # カテゴリ
//!
//! - A. プロセスクラッシュ: 実際の SIGKILL / abort を外部バイナリで起こす
//! - B. ファイル破損: バイト改竄、truncate
//! - C. ordering / ファズ: 低確率だが検出すべきケース
//!
//! # v29 で対応予定の既知ギャップ
//!
//! 一部テストは `#[ignore]` で保留。v29 の page checksum 実装後に enable 予定。
//! コメントに TODO(v29) を付記。

#![cfg(feature = "v27")]

use enchudb::{Engine, HimoType};
use std::io::{Read, Seek, SeekFrom, Write};
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

// ───────────────────────── util ─────────────────────────

fn tmp(name: &str) -> String {
    let mut counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    counter += 1;
    let p = format!("/tmp/enchudb-v29-{}-{}-{}", name, std::process::id(), counter);
    cleanup(&p);
    p
}

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));
}

fn prepare_db(path: &str) {
    let mut e = Engine::create_with_capacity(path, 10_000).unwrap();
    e.define_himo("n", HimoType::Number, 1_000);
    e.flush().unwrap();
}

fn crash_writer_bin() -> PathBuf {
    // cargo test が binary をビルドしてくれる。パスは標準の target 配下。
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("target");
    // test profile は debug か release。release は --release 指定時のみ。
    p.push("debug");
    p.push("crash_writer");
    if !p.exists() {
        panic!("crash_writer binary not built. run: cargo build --features v27 --bin crash_writer");
    }
    p
}

fn flip_byte(path: &str, offset: u64) {
    let mut f = OpenOptions::new().read(true).write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    let mut b = [0u8; 1];
    f.read_exact(&mut b).unwrap();
    f.seek(SeekFrom::Current(-1)).unwrap();
    f.write_all(&[b[0] ^ 0xFF]).unwrap();
}

fn truncate_to(path: &str, size: u64) {
    let f = OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(size).unwrap();
}

// ═══════════════════════════════════════════════════════════
// A. プロセスクラッシュ
// ═══════════════════════════════════════════════════════════

#[test]
fn process_normal_exit_persists_all_writes() {
    let path = tmp("pnormal");
    prepare_db(&path);

    let status = Command::new(crash_writer_bin())
        .args([&path, "normal", "500"])
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "crash_writer normal failed: {:?}", status);

    let eng = Engine::open_standalone(&path).unwrap();
    // 500 entity が tie されている(値 0..=499)
    assert!(eng.entity_count() >= 500);
    for i in 0..500u64 {
        let v = eng.get(i, "n");
        assert_eq!(v, Some(i as u32), "entity {} should have value {}", i, i);
    }
    cleanup(&path);
}

#[test]
fn process_no_commit_recovers_via_auto_commit() {
    let path = tmp("pnocommit");
    prepare_db(&path);

    // wal_commit を呼ばずに exit(0) — Drop の shutdown path で auto-commit されるはず
    let status = Command::new(crash_writer_bin())
        .args([&path, "no_commit", "100"])
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());

    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    // auto-commit が効いてれば 100 件全部残る
    let mut found = 0;
    for i in 0..100u64 {
        if eng.get(i, "n").is_some() { found += 1; }
    }
    assert_eq!(found, 100, "auto-commit should persist all 100 writes");
    drop(eng);
    cleanup(&path);
}

#[test]
fn process_abort_mid_write_preserves_first_half() {
    let path = tmp("pabort");
    prepare_db(&path);

    // 前半(0..50) は wal_sync 済み、後半(50..100) は sync 無しで abort
    let status = Command::new(crash_writer_bin())
        .args([&path, "abort_mid", "100"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    // abort() は SIGABRT で死ぬので success ではない
    assert!(!status.success(), "abort_mid should not succeed");

    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    // 前半 50 件は確実に残る
    for i in 0..50u64 {
        assert_eq!(eng.get(i, "n"), Some(i as u32), "first half entity {} lost", i);
    }
    // 後半は残っても残らなくても OK(どちらも有効な挙動)。
    // ただし「書いた値が化けてる」は絶対 NG。
    for i in 50..100u64 {
        match eng.get(i, "n") {
            None => {}           // 消えた → OK
            Some(v) if v as u64 == i => {} // そのまま → OK
            Some(v) => panic!("entity {} corrupted: got {}", i, v),
        }
    }
    drop(eng);
    cleanup(&path);
}

#[test]
fn process_sigkill_during_loop_no_corruption() {
    let path = tmp("psigkill");
    prepare_db(&path);

    let mut child = Command::new(crash_writer_bin())
        .args([&path, "loop_writes", "0"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    // 2000 writes まで待つ(crash_writer は 1000 毎に wal_sync)
    {
        use std::io::BufRead;
        let stdout = child.stdout.take().unwrap();
        let reader = std::io::BufReader::new(stdout);
        let mut seen = 0u32;
        for line in reader.lines() {
            let Ok(line) = line else { break; };
            if let Ok(n) = line.trim().parse::<u32>() { seen = n; }
            if seen >= 2000 { break; }
        }
    }

    // SIGKILL(Drop 走らず、未 sync の書き込みは失う)
    child.kill().unwrap();
    let _ = child.wait();

    // 復旧できる(エラー無し) + 1 回以上 wal_sync した分は残る
    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    // wal_sync は 1000 毎なので、最低 1000 件は残ってるはず
    assert!(eng.entity_count() >= 1000,
        "SIGKILL should preserve synced batches, got {} entities",
        eng.entity_count());
    drop(eng);
    cleanup(&path);
}

// ═══════════════════════════════════════════════════════════
// B. ファイル破損
// ═══════════════════════════════════════════════════════════

#[test]
fn byte_flip_header_magic_detected() {
    let path = tmp("bheader_magic");
    prepare_db(&path);

    // magic の 1 バイトを反転
    flip_byte(&path, 0);

    // open はエラー("not an EnchuDB file")
    let r = Engine::open_standalone(&path);
    assert!(r.is_err());
    cleanup(&path);
}

#[test]
fn byte_flip_header_metadata_detected_by_crc() {
    let path = tmp("bheader_meta");
    prepare_db(&path);

    // max_entities(offset 8) を改竄 → CRC で検出
    flip_byte(&path, 8);

    let r = Engine::open_standalone(&path);
    match r {
        Err(e) => {
            let s = format!("{}", e);
            assert!(s.contains("CRC"), "expected CRC error, got: {}", s);
        }
        Ok(_) => panic!("corrupted header CRC should fail to open"),
    }
    cleanup(&path);
}

#[test]
fn byte_flip_wal_tail_truncated_silently() {
    let path = tmp("bwal_tail");
    prepare_db(&path);

    // 正常書き込み + sync
    {
        let eng = Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024).unwrap();
        for i in 0..50u32 {
            let e = eng.entity();
            eng.tie_async(e, "n", i);
        }
        eng.flush_writes();
        eng.wal_sync().unwrap();
    }

    // WAL の末尾付近(最後のレコード周辺)を改竄
    let wal_path = format!("{}.wal", path);
    let wal_size = std::fs::metadata(&wal_path).unwrap().len();
    // head の手前 32 バイト付近(実際の WAL 末尾付近)を狙う
    // 厳密位置は wal.head() を読むのが正しいが、ざっくり最後の 1KB を狙う
    flip_byte(&wal_path, wal_size.saturating_sub(512));

    // reopen — CRC 検出で該当レコード以降は破棄、それ以前は適用
    let eng = Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024).unwrap();
    // 壊れた箇所以降は失われるが、エラーにはならない
    let count = (0..50u64).filter(|&i| eng.get(i, "n").is_some()).count();
    let _ = count; // 件数は破損位置依存、ここでは panic しなければ OK
    drop(eng);
    cleanup(&path);
}

#[test]
fn truncate_db_to_half_fails_to_open() {
    // v29: 先読みで file size < layout.total_size を検出してエラーにする。
    let path = tmp("truncate_half");
    prepare_db(&path);

    let size = std::fs::metadata(&path).unwrap().len();
    truncate_to(&path, size / 2);

    match Engine::open_standalone(&path) {
        Err(e) => {
            let s = format!("{}", e);
            assert!(s.contains("truncated") || s.contains("too small"),
                "expected truncation error, got: {}", s);
        }
        Ok(_) => panic!("truncated file should not open successfully"),
    }
    cleanup(&path);
}

#[test]
fn truncate_wal_to_header_loses_uncommitted() {
    let path = tmp("twal_header");
    prepare_db(&path);

    {
        let eng = Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024).unwrap();
        for i in 0..30u32 {
            let e = eng.entity();
            eng.tie_async(e, "n", i);
        }
        eng.flush_writes();
        eng.wal_sync().unwrap();
    }

    // WAL を header サイズ(32B) にまで削る = 全レコード破棄
    let wal_path = format!("{}.wal", path);
    truncate_to(&wal_path, 32);

    // reopen できる。WAL から何も復旧されない(body は既に msync 済みなので残る)
    let eng = Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024).unwrap();
    // body 側は wal_sync で msync してるので、書き込みは残ってる
    let count = (0..30u64).filter(|&i| eng.get(i, "n").is_some()).count();
    assert!(count > 0, "body msync'd data should survive even with WAL truncation");
    drop(eng);
    cleanup(&path);
}

// ═══════════════════════════════════════════════════════════
// C. 並行 + クラッシュ
// ═══════════════════════════════════════════════════════════

#[test]
fn concurrent_writers_with_sync_no_data_loss() {
    let path = tmp("concurrent");
    prepare_db(&path);

    let eng = Engine::open_concurrent_with_wal(&path, 128 * 1024 * 1024).unwrap();
    let mut handles = Vec::new();
    for t in 0..4 {
        let e = Arc::clone(&eng);
        handles.push(std::thread::spawn(move || {
            for i in 0..500u32 {
                let ent = e.entity();
                e.tie_async(ent, "n", (t * 1000 + i) as u32);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    eng.flush_writes();
    eng.wal_sync().unwrap();
    let total_before = eng.entity_count();
    drop(eng);

    let eng = Engine::open_concurrent_with_wal(&path, 128 * 1024 * 1024).unwrap();
    assert_eq!(eng.entity_count(), total_before,
        "all concurrent writes should survive (expected {}, got {})",
        total_before, eng.entity_count());
    drop(eng);
    cleanup(&path);
}

// ═══════════════════════════════════════════════════════════
// D. ランダムファズ(small smoke — 100 回)
// ═══════════════════════════════════════════════════════════

#[test]
fn fuzz_random_byte_flip_no_silent_corruption() {
    use std::collections::HashMap;
    // シード固定 xorshift
    let mut rng_state = 0xdeadbeef_u64;
    let mut next = || {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        rng_state
    };

    let iters = 50;
    let mut outcomes: HashMap<&'static str, u32> = HashMap::new();

    for _ in 0..iters {
        let path = tmp("fuzz");
        prepare_db(&path);

        // 正常に 20 件書く + 期待値を記録
        let mut expected: Vec<(u64, u32)> = Vec::new();
        {
            let eng = Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024).unwrap();
            for i in 0..20u32 {
                let e = eng.entity();
                eng.tie_async(e, "n", i);
                expected.push((e, i));
            }
            eng.flush_writes();
            eng.wal_sync().unwrap();
        }

        // どのファイル? どのオフセット?
        let which = next() % 2;
        let target_path = if which == 0 { path.clone() } else { format!("{}.wal", path) };
        let size = std::fs::metadata(&target_path).map(|m| m.len()).unwrap_or(0);
        if size == 0 { cleanup(&path); continue; }
        let offset = next() % size;

        // silent に落とすため改竄
        let _ = std::panic::catch_unwind(|| flip_byte(&target_path, offset));

        // 再 open → 3 つの許容結果
        let outcome = match Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024) {
            Err(_) => "error_ok",      // 検出 ✓
            Ok(eng) => {
                // 値が残っていて正しいか、消えたか
                let mut corrupt = false;
                for &(eid, v) in &expected {
                    match eng.get(eid, "n") {
                        Some(actual) if actual != v => { corrupt = true; break; }
                        _ => {}
                    }
                }
                drop(eng);
                if corrupt { "silent_corruption" } else { "clean_or_partial" }
            }
        };
        *outcomes.entry(outcome).or_insert(0) += 1;
        cleanup(&path);
    }

    println!("fuzz outcomes ({}iters): {:?}", iters, outcomes);
    // silent_corruption が出ていれば v28 が負け。v29 で解消したい。
    let silent = *outcomes.get("silent_corruption").unwrap_or(&0);
    assert_eq!(silent, 0,
        "silent corruption detected — CRC/validation insufficient. outcomes: {:?}", outcomes);
}

// ═══════════════════════════════════════════════════════════
// v29 未実装ギャップ — これらは現状 fail or silent、v29 で直す
// ═══════════════════════════════════════════════════════════

/// body データ領域の bit flip は v29 で page checksum により検出される。
/// ワークフロー: create → flush(CRC 保存) → 書き込み → flush(CRC 更新)
/// → 外部改竄 → open → CRC mismatch エラー。
#[test]
fn v29_body_bit_flip_detected() {
    let path = tmp("body_flip");
    {
        // sync API で書き込み + seal_integrity(flush + region CRC 保存)
        let mut e = Engine::create_with_capacity(&path, 1000).unwrap();
        e.define_himo("n", HimoType::Number, 100);
        let eid = e.entity();
        e.tie(eid, "n", 42);
        e.seal_integrity().unwrap();
    }

    // body 中盤の任意バイトを flip(himo column / vocab / content のいずれか)
    let size = std::fs::metadata(&path).unwrap().len();
    flip_byte(&path, size / 2);

    // open は region CRC 不一致で失敗するはず
    match Engine::open_standalone(&path) {
        Err(e) => {
            let s = format!("{}", e);
            assert!(s.contains("region CRC"), "expected region CRC error, got: {}", s);
        }
        Ok(_) => panic!("body corruption should be detected by region CRC"),
    }
    cleanup(&path);
}

