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

/// `crash_writer` (enchudb-engine の bin) の path。 root package の test からは cargo が
/// 他 crate の bin を作ってくれないので、 この test と同じ target dir / profile に 1 回だけ
/// `cargo build` する (事前 build 不要、 古い binary も作り直される、 #272)。
fn crash_writer_bin() -> PathBuf {
    static BIN: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BIN.get_or_init(|| {
        // test binary は <target>/<profile>/deps/<name>、 bin は <target>/<profile>/<bin>
        let exe = std::env::current_exe().unwrap();
        let profile_dir = exe.parent().and_then(|d| d.parent()).unwrap().to_path_buf();
        let target_dir = profile_dir.parent().unwrap();
        let profile = match profile_dir.file_name().and_then(|s| s.to_str()) {
            Some("debug") | None => "dev",
            Some(p) => p,
        };
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let status = Command::new(cargo)
            .args(["build", "-p", "enchudb-engine", "--bin", "crash_writer", "--profile", profile])
            .arg("--target-dir")
            .arg(target_dir)
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .expect("cargo build crash_writer");
        assert!(status.success(), "crash_writer の build に失敗: {status:?}");
        let bin = profile_dir.join("crash_writer");
        assert!(bin.exists(), "build したのに crash_writer が無い: {}", bin.display());
        bin
    })
    .clone()
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
        assert_eq!(eng.get(i, "n"), Some(i));
    }

    drop(eng);
    cleanup(&path);
}

// ═══════════════════════════════════════════════════════════
// SIGKILL during signed tie_async loop
// ═══════════════════════════════════════════════════════════

#[test]
fn sigkill_during_signed_loop_preserves_synced_and_signatures() {
    // signed_loop は 500 件毎に oplog_sync し、 1500 件目の commit を changefeed に配る所で止まって
    // `held` を出す。 そこで SIGKILL して
    //   1) 同期した分 (>=500) は recovery で entity として残る
    //   2) ring に残った committed record の署名は SIGKILL でも壊れない
    // を確認する。
    //
    // #204 / #377: ring の fold (try_reset) は 「全 record 適用済み」 なら無条件で、 旧版は kill の
    // 瞬間に ring が畳まれた直後だと署名 record が 1 つも残らなかった (負荷の下で 24 回中 17 回、
    // CI で 8 回続けて空になって落ちた)。 子が配る途中で止まっている間は ring が畳まれない
    // (`crash_writer` の `Hold` の doc) ので、 そこで kill すれば配っている batch が必ず ring に残る。
    //
    // 署名検証は SIGKILL 直後の .oplog を **engine を通さず直接読む** (reopen 時の fold に依存しない)。
    let kp = Arc::new(Keypair::from_bytes(&[7u8; 32]));
    let pub_bytes = kp.public_bytes();
    let path = tmp("sigkill-signed");
    prepare_db(&path);

    let mut child = Command::new(crash_writer_bin())
        .args([&path, "signed_loop", "0"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let held = {
        use std::io::BufRead;
        let reader = std::io::BufReader::new(child.stdout.take().unwrap());
        reader.lines().map_while(Result::ok).find_map(|l| l.strip_prefix("held ").and_then(|n| n.trim().parse::<usize>().ok()))
    };
    child.kill().unwrap();
    let _ = child.wait();
    let held = held.expect("子が changefeed の所で止まらずに終わった");
    assert!(held > 0, "空の batch で止まった");

    let recs = enchudb_oplog::oplog::OpLog::open(std::path::Path::new(&format!("{path}/oplog")))
        .unwrap()
        .iter_committed();
    assert!(recs.len() >= held, "配っていた {held} 件が ring に残っていない ({} 件)", recs.len());

    // (1) recovery
    let eng = Engine::open_concurrent_with_oplog(&path, 64 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);
    eng.pubkeys().force_register(1, &pub_bytes);
    let ec = eng.entity_count();
    assert!(ec >= 500, "SIGKILL should preserve synced batches, got {ec} entities");

    // (2) commit 済み record の署名は全部通る (書きかけの record は CRC で scan が止まるので混ざらない)
    for r in &recs {
        assert_ne!(r.signature, [0u8; 64], "signed record post-crash");
        assert_eq!(r.author_peer, 1);
    }
    let verified = recs.iter().filter(|r| eng.pubkeys().verify(1, &r.signed_bytes, &r.signature)).count();
    assert_eq!(verified, recs.len(), "署名の通らない record がある");

    drop(eng);
    cleanup(&path);
}
