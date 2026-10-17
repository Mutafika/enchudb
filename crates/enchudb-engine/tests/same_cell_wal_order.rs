//! #503: 同じ cell を並行に書くと、 WAL に載る順と本体に当てる順がずれていた。
//!
//! 書き込み (`tie_to` / `untie` / `delete` / Tag / ref) は、 WAL に積んで HLC を振り (`append_lock` の下 = WAL の順 = HLC の順)、
//! 行の lock の下で本体に当てる。 0.34 までは WAL に積むのが行の lock の外だった (Leaf と schema の `commit()` は前から中)。
//! 同じ cell の 2 本が積む順と当てる順を入れ違えると:
//!
//! - sync の表の無い DB (版数を持たない = 後から当てた方が残る): メモリには古い方が残り、 WAL の最後の record と食い違う。
//!   開き直すと WAL の順に当て直すので読める値が変わり、 change listener に順に渡る record を当てた写しとも食い違う
//! - sync の表のある DB (版数を持つ): 後から当てる古い方を弾いて (`Err(OlderThanCell)`)、 「clock went backwards?」 と誤報した
//!
//! 書き手 8 本が同じ cell に barrier でそろえて 1 回ずつ書く × 3000 回。 毎回、 メモリの値が WAL の最後の record と同じかを見る
//! (WAL を畳まないよう `transfer_lock_for_fold` を持つ)。 直す前: tie で 3000 回中 175・178 回、 tie と untie で 74〜154 回ずれ、
//! sync の表ありは 24000 回の書き込みのうち 1398〜3180 回を弾いた。 ずれた回の直後に子 process を SIGKILL して開き直すと、
//! 10 回中 10 回、 読めていた値が変わった (下の `a_value_read_before_a_kill_is_the_value_after_reopen`)。

use enchudb_engine::{db_files, Engine, ValueType};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{eid_local, EntityId};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

const WRITERS: usize = 8;
const ROUNDS: u64 = 10_000;
const WAL_CAP: usize = 64 << 20;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    /// 数の列を tie
    Tie,
    /// 奇数の書き手は untie
    TieUntie,
    /// 奇数の書き手は delete
    TieDelete,
    /// Tag の列 (WAL には Vocab と Tie)
    Tag,
    /// ref の列
    Ref,
    /// sync の表あり (版数を持つ)、 数の列を tie
    Sync,
}

struct Db {
    eng: Arc<Engine>,
    hid: u16,
    e: EntityId,
    /// ref の行き先 (書き手ごとに 1 つ)
    targets: Vec<EntityId>,
    /// 最後に落とす (engine を閉じてから消す)
    _cleanup: Cleanup,
}

struct Cleanup(String);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = db_files::remove_db(&self.0);
    }
}

fn open(mode: Mode) -> Db {
    let path = format!("/tmp/enchudb-same-cell-wal-order-{mode:?}-{}", std::process::id());
    let _ = db_files::remove_db(&path);
    let mut eng = Engine::create_with_capacity(&path, 1 << 16).unwrap();
    let col = if mode == Mode::Sync {
        eng.define_table("t", 1 << 10).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        "t.v"
    } else {
        eng.define_himo("v", ValueType::Number64, 0);
        eng.define_himo("tag", ValueType::Tag, 0);
        eng.define_himo("r", ValueType::Ref, 0);
        match mode {
            Mode::Tag => "tag",
            Mode::Ref => "r",
            _ => "v",
        }
    };
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, WAL_CAP).unwrap();
    let hid = eng.himo_id(col).unwrap() as u16;
    let e = if mode == Mode::Sync { eng.entity_in("t").unwrap() } else { eng.entity().unwrap() };
    let targets = if mode == Mode::Ref { (0..WRITERS).map(|_| eng.entity().unwrap()).collect() } else { Vec::new() };
    Db { eng, hid, e, targets, _cleanup: Cleanup(path) }
}

/// 書き手 `t` の `r` 回目の書き込み。 1 回の中で書き手ごとに値が違う。 弾かれたら true。
fn write(db: &Db, mode: Mode, t: usize, r: u64) -> bool {
    let (eng, e, hid) = (&db.eng, db.e, db.hid);
    let v = r * WRITERS as u64 + t as u64 + 1;
    let odd = t % 2 == 1;
    match mode {
        Mode::Tie => eng.try_tie_to_by_id(e, hid, v).is_err(),
        Mode::Sync => eng.try_tie_to_by_id(e, hid, v as u32).is_err(),
        Mode::TieUntie if odd => {
            eng.untie_by_id(e, hid);
            false
        }
        Mode::TieDelete if odd => {
            eng.delete(e);
            false
        }
        Mode::TieUntie | Mode::TieDelete => eng.try_tie_to_by_id(e, hid, v).is_err(),
        Mode::Tag => eng.try_tie_text_to_by_id(e, hid, &format!("t{}", t + WRITERS * (r % 4) as usize)).is_err(),
        Mode::Ref => eng.try_tie_ref_to_by_id(e, hid, db.targets[t]).is_err(),
    }
}

/// WAL の `from` から先を読み、 cell (e, hid) を最後に書いた record で `last` を更新する (Tie = Some(値)、 Untie / Delete = None)。
fn scan_last(db: &Db, from: &mut u64, last: &mut Option<Option<u64>>) {
    let (recs, _, tail, end) = db.eng.oplog().unwrap().iter_from_with_tail(*from);
    let le = eid_local(db.e);
    for r in recs.iter().chain(tail.iter()) {
        match r.op {
            DecodedOp::Tie { eid, himo_id, value } if eid_local(eid) == le && himo_id == db.hid => *last = Some(Some(value)),
            DecodedOp::Untie { eid, himo_id } if eid_local(eid) == le && himo_id == db.hid => *last = Some(None),
            DecodedOp::Delete { eid } if eid_local(eid) == le => *last = Some(None),
            _ => {}
        }
    }
    *from = end;
}

/// ずれた回 (回, メモリの値, WAL の最後の record の値)。
type Mismatch = (u64, Option<u64>, Option<u64>);

/// 書き手 8 本が同じ cell に 1 回ずつ書く × `rounds` 回。 毎回メモリと WAL の最後の record を比べ、 (ずれた回、 弾かれた書き込み)
/// を返す。 `stop_at_mismatch` なら最初にずれた回で止める。 呼び手は WAL を畳まないよう `transfer_lock_for_fold` を持つこと。
fn run_rounds(db: &Db, mode: Mode, rounds: u64, stop_at_mismatch: bool) -> (Vec<Mismatch>, u64) {
    let start = Arc::new(Barrier::new(WRITERS + 1));
    let done = Arc::new(Barrier::new(WRITERS + 1));
    let go = Arc::new(AtomicU64::new(0));
    let rejected = AtomicU64::new(0);
    let mut mismatches = Vec::new();
    std::thread::scope(|s| {
        for t in 0..WRITERS {
            let (start, done, go, rejected) = (&start, &done, &go, &rejected);
            s.spawn(move || loop {
                start.wait();
                let r = go.load(Ordering::Acquire);
                if r == u64::MAX {
                    return;
                }
                if write(db, mode, t, r) {
                    rejected.fetch_add(1, Ordering::Relaxed);
                }
                done.wait();
            });
        }
        let (mut from, mut last) = (enchudb_oplog::oplog::HEADER_SIZE as u64, None);
        for r in 0..rounds {
            go.store(r, Ordering::Release);
            start.wait();
            done.wait();
            scan_last(db, &mut from, &mut last);
            let mem = db.eng.get_by_id(db.e, db.hid);
            let wal = last.expect("WAL にこの cell の record が無い");
            if mem != wal {
                mismatches.push((r, mem, wal));
                if stop_at_mismatch {
                    break;
                }
            }
        }
        go.store(u64::MAX, Ordering::Release);
        start.wait();
    });
    (mismatches, rejected.into_inner())
}

/// 書き手 8 本が同じ cell を `mode` で書く × 1 万回。 毎回、 メモリの値が WAL の最後の record と同じで、 弾かれた書き込みが無い。
fn race_on_one_cell(mode: Mode) {
    let db = open(mode);
    let no_fold = db.eng.transfer_lock_for_fold();
    let (mismatches, rejected) = run_rounds(&db, mode, ROUNDS, false);
    drop(no_fold);
    assert_eq!(db.eng.wal_dropped_records(), 0, "{mode:?}: WAL から落ちた (比べられない)");
    assert!(
        mismatches.is_empty(),
        "{mode:?}: {ROUNDS} 回中 {} 回、 メモリの値が WAL の最後の record と違う (回, メモリ, WAL): {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(5)]
    );
    assert_eq!(rejected, 0, "{mode:?}: ローカルの書き込みが版数で弾かれた");
}

#[test]
fn tie_race_leaves_the_last_wal_record() {
    race_on_one_cell(Mode::Tie);
}

#[test]
fn tie_untie_race_leaves_the_last_wal_record() {
    race_on_one_cell(Mode::TieUntie);
}

#[test]
fn tie_delete_race_leaves_the_last_wal_record() {
    race_on_one_cell(Mode::TieDelete);
}

#[test]
fn tag_race_leaves_the_last_wal_record() {
    race_on_one_cell(Mode::Tag);
}

#[test]
fn ref_race_leaves_the_last_wal_record() {
    race_on_one_cell(Mode::Ref);
}

/// sync の表のある DB: 版数で古い方を弾いて合わせていた (誤報つき)。 弾かずに合うこと。
#[test]
fn sync_db_race_rejects_no_local_write() {
    race_on_one_cell(Mode::Sync);
}

const CHILD_ENV: &str = "ENCHU_SAME_CELL_CHILD";

/// 子: ずれた回が出るか 2000 回書いたら、 その時に読めた値を `READY <値|none> <eid>` と出して止まる (親が殺す)。
#[test]
fn child_writes_until_a_mismatch_then_waits_to_be_killed() {
    let Ok(path) = std::env::var(CHILD_ENV) else { return };
    let _ = db_files::remove_db(&path);
    let mut eng = Engine::create_with_capacity(&path, 1 << 16).unwrap();
    eng.define_himo("v", ValueType::Number64, 0);
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, WAL_CAP).unwrap();
    let hid = eng.himo_id("v").unwrap() as u16;
    let e = eng.entity().unwrap();
    let db = Db { eng, hid, e, targets: Vec::new(), _cleanup: Cleanup(path) };
    // 殺されるまで畳ませない (開き直しで WAL から当て直す道を通す)
    let _no_fold = db.eng.transfer_lock_for_fold();
    let _ = run_rounds(&db, Mode::Tie, 2000, true);
    let mem = db.eng.get_by_id(e, hid).map_or("none".to_string(), |v| v.to_string());
    {
        use std::io::Write;
        println!("READY {mem} {e}");
        std::io::stdout().flush().unwrap();
    }
    // 親が殺す (殺されずに 60 秒経ったら、 片付けて抜ける)
    std::thread::sleep(std::time::Duration::from_secs(60));
}

/// 親: 子が書いた後で SIGKILL し、 開き直して読む。 落ちる前に読めた値と同じであること。
#[cfg(unix)]
#[test]
fn a_value_read_before_a_kill_is_the_value_after_reopen() {
    use std::io::BufRead;
    if std::env::var(CHILD_ENV).is_ok() {
        return;
    }
    for k in 0..5 {
        let path = format!("/tmp/enchudb-same-cell-kill-{}-{k}", std::process::id());
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_writes_until_a_mismatch_then_waits_to_be_killed", "--nocapture", "--test-threads", "1"])
            .env(CHILD_ENV, &path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut out = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        // test の枠は 「test ... 」 を改行なしで出すので、 行の途中も見る
        let ready = loop {
            line.clear();
            assert!(out.read_line(&mut line).unwrap() > 0, "子が READY を出さずに終わった");
            if let Some(i) = line.find("READY") {
                break line[i..].to_string();
            }
        };
        child.kill().unwrap();
        child.wait().unwrap();
        let f: Vec<&str> = ready.split_whitespace().collect();
        let before = f[1].parse::<u64>().ok();
        let e: EntityId = f[2].parse().unwrap();
        let eng = Engine::open_concurrent_with_oplog(&path, WAL_CAP).unwrap();
        let after = eng.get_by_id(e, eng.himo_id("v").unwrap() as u16);
        drop(eng);
        let _ = db_files::remove_db(&path);
        assert_eq!(after, before, "{k} 回目: SIGKILL の前に読めた値が、 開き直した後に変わった");
    }
}
