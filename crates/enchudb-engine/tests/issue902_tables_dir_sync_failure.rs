//! #902: `tables` を置き換えた (rename した) 後の directory の fsync (#884) に失敗したら、 次の書き出しは中身が file と同じでも
//! directory の fsync をやり直してから 「置いた」 と返す。
//!
//! 前は、 失敗した後の呼び出しが `write_atomic_if_changed` で 「中身は file と同じ」 (rename は通っている) と見て何も書かず、 directory の
//! fsync を飛ばしたまま `persisted_next` を進めた = checkpoint が rename の永続を待たずに進みえた (#857 / #870 / #881 が頼る `tables` の
//! 永続が崩れる)。 また、 覚えた中身 (`tables_persisted`) は失敗の前のままだったので、 中身が前に置いた形に戻ると file (失敗した回の
//! 中身) に触らずに済ませ、 外した宣言が開き直すと戻った。
//!
//! I/O の故障は crashsim の `fault` (`ENCHU_FAULT_AT` で EIO、 子 process): 数えるだけの回で `tables.dir_sync` の番号を見て、 その点で
//! 起こす。 consumer の thread も同じ数を進めうるので、 当たる点がずれたら前後を試す。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::fault;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const CHILD_DB_ENV: &str = "ENCHU_ISSUE902_CHILD_DB";
const CHILD_CASE_ENV: &str = "ENCHU_ISSUE902_CASE";

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(tag: &str) -> Scratch {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = std::env::temp_dir().join(format!("enchudb-issue902-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    Scratch(p)
}

/// sync しない DB、 表 `t` (列 a)。
fn engine(db: &str) -> (Arc<Engine>, u16) {
    let mut eng = Engine::create_growable_with_capacity(db, 1 << 16).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let a = eng.himo_id("t.a").unwrap() as u16;
    (eng, a)
}

/// 子: 親が選んだ形を回し、 点の番号と結果を 1 行出す (I/O の故障は親が `ENCHU_FAULT_*` で選ぶ)。 親が呼ばない時は何もしない。
/// consumer は止めておく (周期が `tables` を置くと、 失敗の後を先に覆う)。
///
/// - `retry`: 行を足して消し (消した行が sidecar より先 = `tables` を置くまで `oplog_sync` は checkpoint を進めない、 #857)、
///   `persist_tables` を 2 回 (1 回目の directory の fsync を親が失敗させる) → `oplog_sync`。
///   出力: `c0 c1 c2 1回目の成否 2回目の成否 checkpointが消した行を越えたか`
/// - `revert`: `tables` を置いた後、 列の印を宣言して (`tables` を置き換える。 その directory の fsync を親が失敗させる) 外し、 閉じて
///   開き直す。 出力: `c0 c1 c2 開き直した後の印の宣言の数`
#[test]
fn child_tables_dir_sync() {
    let Ok(db) = std::env::var(CHILD_DB_ENV) else { return };
    let case = std::env::var(CHILD_CASE_ENV).unwrap();
    let (eng, a) = engine(&db);
    let hold = eng.hold_consumer();
    let line = match case.as_str() {
        "retry" => {
            let wal = eng.oplog().unwrap().clone();
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, a, 1u32);
            eng.delete(e);
            let head = wal.head();
            let c0 = fault::count();
            let r1 = eng.persist_tables().is_ok();
            let c1 = fault::count();
            let r2 = eng.persist_tables().is_ok();
            let c2 = fault::count();
            eng.oplog_sync().unwrap();
            let moved = wal.checkpoint() >= head;
            format!("{c0} {c1} {c2} {r1} {r2} {moved}")
        }
        "revert" => {
            eng.persist_tables().unwrap();
            let c0 = fault::count();
            eng.declare_marks("t.a").unwrap();
            let c1 = fault::count();
            eng.drop_marks("t.a").unwrap();
            let c2 = fault::count();
            drop(hold);
            drop(eng);
            let st = Engine::open_standalone(&db).unwrap();
            let marks = st.marks_declarations().len();
            drop(st);
            println!("ISSUE902 {c0} {c1} {c2} {marks}");
            return;
        }
        other => panic!("unknown case {other}"),
    };
    drop(hold);
    drop(eng);
    println!("ISSUE902 {line}");
}

/// [`child_tables_dir_sync`] を別の process で回す。 戻り値: (結果の欄, 起こした故障 (`ENCHU_FAULT` の後ろ))。
fn run_child(case: &str, db: &Path, envs: &[(&str, String)]) -> (Vec<String>, Option<String>) {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_tables_dir_sync", "--nocapture", "--test-threads=1"])
        .env(CHILD_DB_ENV, db)
        .env(CHILD_CASE_ENV, case)
        .envs(envs.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .unwrap();
    let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "子が落ちた ({case} {envs:?}): {stdout}\n{stderr}");
    let fired = stderr.lines().find_map(|l| l.split_once("ENCHU_FAULT ").map(|(_, r)| r.to_string()));
    let line = stdout
        .lines()
        .find_map(|l| l.split_once("ISSUE902 ").map(|(_, r)| r))
        .unwrap_or_else(|| panic!("子の結果が無い: {stdout}\n{stderr}"));
    (line.split(' ').map(str::to_string).collect(), fired)
}

/// 点の記録 (`n<TAB>名前`)
fn sites(trace: &Path) -> Vec<(u64, String)> {
    std::fs::read_to_string(trace)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| l.split_once('\t').map(|(n, s)| (n.parse().unwrap(), s.to_string())))
        .collect()
}

/// `case` を数えるだけで回し、 欄 `from..to` (点の番号の窓) の中の `tables.dir_sync` で EIO を起こす回を返す: (欄, 点の記録)。
fn fail_first_dir_sync(s: &Scratch, case: &str, window: (usize, usize)) -> (Vec<String>, Vec<(u64, String)>) {
    let counted = s.0.join("counted");
    let trace = s.0.join("trace0");
    let (f, fired) = run_child(case, &counted, &[("ENCHU_FAULT_TRACE", trace.to_str().unwrap().to_string())]);
    assert!(fired.is_none(), "前提: 数えるだけの回は故障を起こさない");
    let (lo, hi): (u64, u64) = (f[window.0].parse().unwrap(), f[window.1].parse().unwrap());
    let n = sites(&trace)
        .into_iter()
        .find(|(n, s)| *n > lo && *n <= hi && s == "tables.dir_sync")
        .map(|(n, _)| n)
        .unwrap_or_else(|| panic!("前提: {case}: 窓 {lo}..={hi} で `tables` を置き換えて directory を fsync した"));
    // consumer の thread が止まる前に進めた数は回ごとに違いうるので、 当たる点がずれたら前後を試す
    for (k, d) in [0i64, -1, 1, -2, 2, -3, 3, -4, 4].into_iter().enumerate() {
        let at = (n as i64 + d) as u64;
        let db = s.0.join(format!("db{k}"));
        let trace = s.0.join(format!("trace{}", k + 1));
        let (f, fired) = run_child(
            case,
            &db,
            &[("ENCHU_FAULT_AT", at.to_string()), ("ENCHU_FAULT_TRACE", trace.to_str().unwrap().to_string())],
        );
        let (lo, hi): (u64, u64) = (f[window.0].parse().unwrap(), f[window.1].parse().unwrap());
        if fired.as_deref().is_some_and(|x| x.ends_with(" tables.dir_sync")) && at > lo && at <= hi {
            return (f, sites(&trace));
        }
    }
    panic!("前提: {case}: 窓の `tables.dir_sync` に故障を当てられなかった (数えた回の点 {n})");
}

/// directory の fsync に失敗した後の書き出しは、 中身が file と同じでも fsync をやり直してから置いたと返す (それまで checkpoint は消した行を
/// 越えない)。
#[test]
fn a_failed_tables_dir_sync_is_redone_before_the_checkpoint_moves() {
    let s = scratch("retry");
    let (f, sites) = fail_first_dir_sync(&s, "retry", (0, 1));
    let (c1, c2): (u64, u64) = (f[1].parse().unwrap(), f[2].parse().unwrap());
    let (r1, r2, moved) = (f[3] == "true", f[4] == "true", f[5] == "true");
    assert!(!r1, "前提: 1 回目は directory の fsync の失敗を返す");
    assert!(r2 && moved, "前提: 2 回目は置けて、 checkpoint が消した行を越えた ({f:?})");
    let second: Vec<&(u64, String)> = sites.iter().filter(|(n, _)| *n > c1 && *n <= c2).collect();
    let redone = second.iter().any(|(_, s)| s == "tables.dir_sync");
    assert!(redone, "directory の fsync をやり直さずに 「置いた」 と返し、 checkpoint を進めた (2 回目の点 {second:?})");
}

/// directory の fsync に失敗した後は、 覚えた中身を捨てる: 中身が前に置いた形に戻っても、 file (失敗した回の中身) を置き直す。 前は覚えた
/// 中身 (失敗の前) と同じと見て file に触らず、 外した宣言が開き直すと戻った。
#[test]
fn a_failed_tables_dir_sync_forgets_what_was_placed() {
    let s = scratch("revert");
    let (counted, _) = run_child("revert", &s.0.join("plain"), &[]);
    assert_eq!(counted[3], "0", "前提: 故障が無ければ、 外した宣言は開き直しても無い");
    let (f, _) = fail_first_dir_sync(&s, "revert", (0, 1));
    assert_eq!(f[3], "0", "directory の fsync に失敗した後、 外した宣言が開き直すと戻った ({f:?})");
}
