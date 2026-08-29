//! 署名付き oplog の crash / recovery E2E。
//!
//! durability_destruction.rs は署名無し oplog の耐久性検証。ここでは:
//! - 署名付きレコードが WAL に物理的に残ること
//! - SIGKILL 後の recover で署名が失われないこと
//! - audit() で署名と著者 peer を正しく列挙できること
//! を追加確認する。

use enchudb::{AuditFilter, Engine, ValueType};
use enchudb_oplog::keys::Keypair;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

// ───────────────────────── util ─────────────────────────

static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn tmp(tag: &str) -> String {
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let p = format!("/tmp/enchudb-crash-{}-{}-{}", tag, std::process::id(), n);
    cleanup(&p);
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

fn prepare_db(path: &str) {
    let mut e = Engine::create_with_capacity(path, 10_000).unwrap();
    e.define_himo("n", ValueType::Number, 1_000);
    e.flush().unwrap();
}

fn crash_writer_bin() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("target");
    p.push("debug");
    p.push("crash_writer");
    assert!(
        p.exists(),
        "crash_writer binary not built. run: cargo build -p enchudb-engine --bin crash_writer"
    );
    p
}

// ═══════════════════════════════════════════════════════════
// In-process: signed WAL roundtrip
// ═══════════════════════════════════════════════════════════

/// **現行の durability 設計と前提が食い違っている。**
///
/// このテストは「WAL に書いた signed record が reopen 後も `audit()` で全件見える」
/// ことを期待しているが、 oplog は **session をまたぐ audit log ではなく ring buffer**。
/// graceful shutdown 時に consumer thread が `advance_checkpoint(head)` するので
/// checkpoint == head になり、 次 open で `try_reset()` が head を HEADER_SIZE へ
/// 巻き戻す。 `audit()` は `iter_committed()` = head までの scan なので 0 件になる。
///
/// 0.8.0 以降、 session をまたいで残る sync record は `_sync_ops` 側。
/// 「reopen 後も署名付き履歴を追える」ことを保証すべきかは設計判断が要るため、
/// 期待値を黙って緩めず ignore で可視化する。
#[test]
#[ignore = "oplog ring は session をまたぐ audit log ではない — graceful shutdown で checkpoint が head に追いつき、次 open の try_reset で ring が畳まれるため audit() が空になる"]
fn signed_wal_records_survive_reopen() {
    // tie_async で書いた signed record が reopen 後の audit で全件取れる。
    let path = tmp("signed_reopen");
    prepare_db(&path);

    let kp = Arc::new(Keypair::from_bytes(&[42u8; 32]));
    let pub_bytes = kp.public_bytes();

    let initial_count = {
        let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(3);
        eng.set_keypair(Some(kp.clone()));

        for i in 0..50u32 {
            let e = eng.entity().unwrap();
            eng.tie_async(e, "n", i);
        }
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();

        let recs = eng.audit(&AuditFilter::default());
        assert!(recs.len() >= 50, "pre-drop audit should see 50 ties");
        for r in &recs {
            assert_ne!(r.signature, [0u8; 64], "signed record must have non-zero sig");
            assert_eq!(r.author_peer, 3);
        }
        recs.len()
    };

    // reopen し、recover 後にも audit で全件見え、署名保持されてる。
    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
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
fn sigkill_during_signed_loop_preserves_synced_and_signatures() {
    // signed_loop は 500 件毎に oplog_sync (進捗 print は 50 件毎)。SIGKILL 後に
    //   1) 1 回以上同期した分(>=500)は recovery で entity として残る
    //   2) ring に残った committed record の署名は SIGKILL でも壊れない
    // を確認する。
    //
    // #204: ring の fold (try_reset) は「checkpoint == head なら無条件」— 全 record
    // 適用済みの ring を畳むのは正しい挙動で、auto_reset gate は撤去済み (vestigial、
    // oplog.rs の try_reset doc 参照)。つまり **SIGKILL の瞬間に ring が空なことも
    // 正当にあり得る** (consumer tick が直前に fold した場合)。旧実装は「reopen 後の
    // audit() が必ず非空」を仮定していて、これが kill × tick の race で負荷依存
    // flake になっていた。
    //
    // 対策:
    // - kill は sync 境界 (1500) の直後 (seen >= 1550) に同期 — fold が走る窓を
    //   最小化して residue が残る確率を上げる (tick は 100ms 周期、窓は ~15ms)
    // - 署名検証は SIGKILL 直後の .oplog を **engine を通さず直接読む** (reopen 時の
    //   fold に依存しない)
    // - それでも fold 済みだった試行は「全 record 適用済みの正常形」として recovery
    //   (1) だけ検証し、署名 residue が取れるまで bounded retry — どの試行でも
    //   (1) は必ず assert され、(2) は residue の取れた試行で assert される
    const ATTEMPTS: usize = 8;
    let kp = Arc::new(Keypair::from_bytes(&[7u8; 32]));
    let pub_bytes = kp.public_bytes();
    let mut signature_checked = false;

    for attempt in 0..ATTEMPTS {
        let path = tmp(&format!("sigkill-signed-{attempt}"));
        prepare_db(&path);

        let mut child = Command::new(crash_writer_bin())
            .args([&path, "signed_loop", "0"])
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
                if seen >= 1550 {
                    break;
                }
            }
        }

        child.kill().unwrap();
        let _ = child.wait();

        // fold 前の ring を直接読む (file bytes は kill では消えない)
        let recs = enchudb_oplog::oplog::OpLog::open(std::path::Path::new(&format!(
            "{path}/oplog"
        )))
        .unwrap()
        .iter_committed();

        // (1) recovery は residue の有無に関わらず必ず成立すること
        let eng = Engine::open_concurrent_with_oplog(&path, 64 * 1024 * 1024).unwrap();
        eng.set_peer_id(1);
        eng.pubkeys().force_register(1, &pub_bytes);
        let ec = eng.entity_count();
        assert!(
            ec >= 500,
            "SIGKILL should preserve synced batches, got {ec} entities (attempt {attempt})"
        );

        // (2) residue が取れた試行で署名を verify
        if !recs.is_empty() {
            let mut verified = 0usize;
            for r in &recs {
                assert_ne!(r.signature, [0u8; 64], "signed record post-crash");
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
            signature_checked = true;
        }

        drop(eng);
        cleanup(&path);
        if signature_checked {
            break;
        }
    }

    // fold 確率 (実測 ~15-30%/試行) が 8 連続で出る確率は ~1e-5 未満。ここに来たら
    // 「ring に committed record が残る経路が消えた」ので、設計変更を疑うこと。
    assert!(
        signature_checked,
        "{ATTEMPTS} 回の SIGKILL 全てで ring が fold 済みだった — 署名検証経路が一度も走っていない"
    );
}
