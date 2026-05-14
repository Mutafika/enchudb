//! v32 WAL + 署名下の crash / recovery E2E。
//!
//! v29_destruction.rs は v27 WAL(署名無し)の耐久性検証。v32 では:
//! - 署名付きレコードが WAL に物理的に残ること
//! - SIGKILL 後の recover で署名が失われないこと
//! - audit() で署名と著者 peer を正しく列挙できること
//! を追加確認する。

#![cfg(feature = "v32")]

use enchudb::{AuditFilter, Engine, HimoType};
use enchudb_wal::keys::Keypair;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

// ───────────────────────── util ─────────────────────────

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn tmp(tag: &str) -> String {
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let p = format!("/tmp/enchudb-v32-crash-{}-{}-{}", tag, std::process::id(), n);
    cleanup(&p);
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

fn prepare_db(path: &str) {
    let mut e = Engine::create_with_capacity(path, 10_000).unwrap();
    e.define_himo("n", HimoType::Value, 1_000);
    e.flush().unwrap();
}

fn crash_writer_bin() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("target");
    p.push("debug");
    p.push("crash_writer");
    assert!(
        p.exists(),
        "crash_writer binary not built. run: cargo build --features v32 --bin crash_writer"
    );
    p
}

// ═══════════════════════════════════════════════════════════
// In-process: signed WAL roundtrip
// ═══════════════════════════════════════════════════════════

#[test]
fn signed_wal_records_survive_reopen() {
    // tie_async で書いた signed record が reopen 後の audit で全件取れる。
    let path = tmp("signed_reopen");
    prepare_db(&path);

    let kp = Arc::new(Keypair::from_bytes(&[42u8; 32]));
    let pub_bytes = kp.public_bytes();

    let initial_count = {
        let eng = Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(3);
        eng.set_keypair(Some(kp.clone()));

        for i in 0..50u32 {
            let e = eng.entity();
            eng.tie_async(e, "n", i);
        }
        eng.wal_commit();
        eng.flush_writes();
        eng.wal_sync().unwrap();

        let recs = eng.audit(&AuditFilter::default());
        assert!(recs.len() >= 50, "pre-drop audit should see 50 ties");
        for r in &recs {
            assert_ne!(r.signature, [0u8; 64], "signed record must have non-zero sig");
            assert_eq!(r.author_peer, 3);
        }
        recs.len()
    };

    // reopen し、recover 後にも audit で全件見え、署名保持されてる。
    let eng = Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(3);
    eng.pubkeys().force_register(3, &pub_bytes);
    let recs = eng.audit(&AuditFilter::default());
    assert_eq!(
        recs.len(),
        initial_count,
        "post-reopen audit should see same # records"
    );
    for r in &recs {
        assert_ne!(r.signature, [0u8; 64], "sig must persist across reopen");
        assert_eq!(r.author_peer, 3);
        // TOFU 登録済み pubkey で検証可能
        assert!(
            eng.pubkeys().verify(3, &r.signed_bytes, &r.signature),
            "sig must verify post-reopen"
        );
    }

    // 本体への apply も復元されている
    assert_eq!(eng.entity_count(), 50);
    for i in 0..50u64 {
        assert_eq!(eng.get(i, "n"), Some(i as u32));
    }

    drop(eng);
    cleanup(&path);
}

// ═══════════════════════════════════════════════════════════
// SIGKILL during signed tie_async loop
// ═══════════════════════════════════════════════════════════

#[test]
fn sigkill_during_v32_signed_loop_preserves_synced_and_signatures() {
    // v32_signed_loop は 500 件毎に wal_sync。SIGKILL 後の復旧で
    //   1) 1 回以上同期した分(>=500)は entity として残る
    //   2) WAL レコードの署名は消えない(reopen 後の audit で verify 通る)
    // を確認する。
    let path = tmp("sigkill_v32");
    prepare_db(&path);

    let kp = Arc::new(Keypair::from_bytes(&[7u8; 32]));
    let pub_bytes = kp.public_bytes();

    let mut child = Command::new(crash_writer_bin())
        .args([&path, "v32_signed_loop", "0"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    {
        use std::io::BufRead;
        let stdout = child.stdout.take().unwrap();
        let reader = std::io::BufReader::new(stdout);
        let mut seen = 0u32;
        for line in reader.lines() {
            let Ok(line) = line else { break; };
            if let Ok(n) = line.trim().parse::<u32>() {
                seen = n;
            }
            if seen >= 1500 {
                break;
            }
        }
    }

    child.kill().unwrap();
    let _ = child.wait();

    // recover
    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);
    // child 側と同じ pubkey を TOFU 登録
    eng.pubkeys().force_register(1, &pub_bytes);

    // 最低 500 件(=child が 1 回 wal_sync 到達)は残ってる
    let ec = eng.entity_count();
    assert!(
        ec >= 500,
        "SIGKILL should preserve synced v32 batches, got {} entities",
        ec
    );

    // WAL レコードの署名を verify。reopen 後も壊れてない。
    let recs = eng.audit(&AuditFilter::default());
    assert!(!recs.is_empty(), "should have WAL records after recover");
    let mut verified = 0usize;
    for r in &recs {
        assert_ne!(r.signature, [0u8; 64], "signed record post-recover");
        assert_eq!(r.author_peer, 1);
        if eng.pubkeys().verify(1, &r.signed_bytes, &r.signature) {
            verified += 1;
        }
    }
    assert!(
        verified >= recs.len() - 1,
        "all (or all-but-trailing) sigs should verify, got {}/{}",
        verified,
        recs.len()
    );

    drop(eng);
    cleanup(&path);
}
