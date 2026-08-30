//! v10 の **crash consistency**: DB が directory + segment file 群になったので、 flush は
//! 複数 file への msync に分かれる。 「途中で電源が落ちた」 相当 (SIGKILL) の後に、
//!
//! 1. **再 open できること** (writer lock が残って開けない、 にならない)
//! 2. **flush が返った batch のデータが全部残っていること** (durability)
//! 3. **値が化けていないこと** (torn write が読めてしまわない)
//!
//! を、 kill の時点をずらしながら繰り返し確かめる。 v9 の 1 ファイルでは flush が 1 本の
//! msync だったので、 ここは v10 で性質が変わった箇所。
//!
//! 子は 「1 batch 書く → flush → 進捗 file に batch 番号を fsync」 を繰り返す。 親は
//! 任意の時点で SIGKILL する。 進捗 file は DB の外に置く (DB の一部を検証に使わない)。
//!
//! **このテストが証明していないこと**: SIGKILL は **process の死**であって電源断ではない。
//! mmap の dirty page は page cache に残り、 process が死んでも file には反映される。
//! つまりここで見ているのは 「書き手が途中で落ちても、 生きている OS から見た DB が壊れて
//! いないか」。 電源断 (page cache ごと失う) の検証には fault injection FS が要る。
//!
//! 検証器が本当に噛むことは、 子が 1 箇所だけ違う値を書くように改造すると
//! `flush 済みの値が違う: eid=1000 h0 got=Some(0) want=7000` で落ちることで確認済み。

use enchudb_engine::{Engine, ValueType};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

const CHILD_DB_ENV: &str = "ENCHU_CRASH_DB";
const CHILD_PROGRESS_ENV: &str = "ENCHU_CRASH_PROGRESS";
const BATCH: u32 = 250;
const BATCHES: u32 = 60;
const HIMOS: u32 = 3;

fn value_of(i: u32, h: u32) -> u32 {
    i.wrapping_mul(7).wrapping_add(h * 1_000_003)
}

/// child: batch ごとに flush して、 進捗を fsync してから次へ。 SIGKILL されるまで回る。
#[test]
fn crash_writer_child() {
    let Ok(db) = std::env::var(CHILD_DB_ENV) else { return };
    let progress = std::env::var(CHILD_PROGRESS_ENV).unwrap();

    let mut eng = Engine::create_with_capacity(&db, 65_536).unwrap();
    eng.define_table("t", 40_000).unwrap();
    for h in 0..HIMOS {
        eng.define_himo_in("t", &format!("h{h}"), ValueType::Number, 40_000).unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();

    for b in 0..BATCHES {
        for k in 0..BATCH {
            let i = b * BATCH + k;
            let e = eng.entity_in("t").unwrap();
            assert_eq!(e, u64::from(i), "eid が連番でない");
            for h in 0..HIMOS {
                eng.tie(e, &format!("t.h{h}"), value_of(i, h));
            }
        }
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        // flush が返った = ここまでは durable、 という主張を外に記録する。
        let mut f = std::fs::File::create(&progress).unwrap();
        write!(f, "{}", b + 1).unwrap();
        f.sync_all().unwrap();
    }
}

fn verify_after_crash(db: &str, committed_batches: u32) -> String {
    let eng = match Engine::open(db) {
        Ok(e) => e,
        Err(e) => return format!("再 open できない: {e}"),
    };
    let must_have = committed_batches * BATCH;
    if (eng.entity_count() as u32) < must_have {
        return format!(
            "flush 済みの entity が消えた: count={} < {must_have} (batch {committed_batches})",
            eng.entity_count()
        );
    }
    for i in 0..must_have {
        for h in 0..HIMOS {
            let got = eng
                .get(u64::from(i), &format!("t.h{h}"))
                .and_then(|v| v.to_string().parse::<u32>().ok());
            if got != Some(value_of(i, h)) {
                return format!(
                    "flush 済みの値が違う: eid={i} h{h} got={got:?} want={}",
                    value_of(i, h)
                );
            }
        }
    }
    String::new()
}

#[test]
fn sigkill_mid_write_keeps_everything_that_was_flushed() {
    let mut report = vec![];
    for (round, delay_ms) in [120u64, 250, 400, 650, 900, 1400].into_iter().enumerate() {
        let db = format!("/tmp/enchu_crash_{}_{round}.db", std::process::id());
        let progress = format!("/tmp/enchu_crash_{}_{round}.progress", std::process::id());
        let _ = std::fs::remove_dir_all(&db);
        let _ = std::fs::remove_file(&progress);

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["crash_writer_child", "--exact", "--test-threads=1"])
            .env(CHILD_DB_ENV, &db)
            .env(CHILD_PROGRESS_ENV, &progress)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");

        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        let killed = unsafe { libc::kill(child.id() as i32, libc::SIGKILL) } == 0;
        let status = child.wait().unwrap();

        let committed: u32 = std::fs::read_to_string(&progress)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);

        // まだ 1 batch も flush していない / 既に完走していた round は情報が薄いので記録だけ。
        let finished_normally = status.success();
        let err = if Path::new(&db).exists() {
            verify_after_crash(&db, committed)
        } else {
            "DB directory が無い".to_string()
        };
        report.push((round, delay_ms, killed, finished_normally, committed, err));

        let _ = std::fs::remove_dir_all(&db);
        let _ = std::fs::remove_file(&progress);
    }

    let mut failures = vec![];
    eprintln!("\n=== SIGKILL round ===");
    for (round, delay, killed, finished, committed, err) in &report {
        eprintln!(
            "  round {round} kill@{delay}ms killed={killed} 完走={finished} flush 済み batch={committed} ({} entity) → {}",
            committed * BATCH,
            if err.is_empty() { "OK" } else { err.as_str() }
        );
        if !err.is_empty() {
            failures.push(format!("round {round} (kill@{delay}ms): {err}"));
        }
    }
    // 少なくとも 1 round は 「書いている途中で殺した」 状態でなければテストの意味が無い。
    let mid_write = report.iter().filter(|(_, _, _, fin, c, _)| !fin && *c > 0 && *c < BATCHES).count();
    assert!(mid_write >= 1, "書き込み中に殺せた round が無い (BATCHES / delay の調整が要る)");
    assert!(failures.is_empty(), "crash 後に壊れた round:\n{}", failures.join("\n"));
}
