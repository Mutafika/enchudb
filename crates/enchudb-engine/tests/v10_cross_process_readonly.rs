//! v10: directory DB を **別 process** が `open_readonly` で開けること (oboro / opyula の
//! Leaf 直読み経路)。 sidecar (tables) が directory の中に移っても reader は同じ path 文字列
//! で開けて、 writer が lock を握ったまま後から足した himo (= 新しい segment file) も
//! 次の open で見える。 child は自分自身 (test binary) を `readonly_probe_child` で起動。

use enchudb_engine::{Engine, ValueType};
use std::process::Command;

const CHILD_PATH_ENV: &str = "ENCHU_V10_RO_CHILD_PATH";
const CHILD_EIDS_ENV: &str = "ENCHU_V10_RO_CHILD_EIDS";
const PROBE_PREFIX: &str = "PROBE ";

fn tmp() -> String {
    let path = format!("/tmp/enchu_v10_xproc_ro_{}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    path
}

/// child 側: env に path があれば readonly open して要約 1 行を stdout に出す。 無ければ no-op。
#[test]
fn readonly_probe_child() {
    let Ok(path) = std::env::var(CHILD_PATH_ENV) else { return };
    let eids: Vec<u64> = std::env::var(CHILD_EIDS_ENV)
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect();
    let eng = Engine::open_readonly(&path).expect("open_readonly in child");
    let mut tables: Vec<String> = eng.list_user_tables().into_iter().map(|t| t.1).collect();
    tables.sort();
    let vals: Vec<String> = eids
        .iter()
        .map(|&e| {
            let n = eng.get(e, "widgets.n").map(|v| v.to_string()).unwrap_or_default();
            let m = eng.get(e, "widgets.m").map(|v| v.to_string()).unwrap_or_default();
            format!("{n}:{m}")
        })
        .collect();
    println!(
        "{PROBE_PREFIX}count={} himos={} tables={} vals={}",
        eng.entity_count(),
        eng.himo_count(),
        tables.join("+"),
        vals.join(",")
    );
}

fn probe(path: &str, eids: &[u64]) -> String {
    let eids: Vec<String> = eids.iter().map(|e| e.to_string()).collect();
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["readonly_probe_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_PATH_ENV, path)
        .env(CHILD_EIDS_ENV, eids.join(","))
        .output()
        .expect("spawn child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "child failed: status={:?}\nstdout:\n{stdout}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    // `--nocapture` でも harness が行頭に `test … ... ` を出すので、 prefix の位置から取る。
    stdout
        .lines()
        .find_map(|l| l.find(PROBE_PREFIX).map(|i| l[i + PROBE_PREFIX.len()..].trim().to_string()))
        .unwrap_or_else(|| panic!("no probe line in child stdout:\n{stdout}"))
}

#[test]
fn another_process_opens_dir_db_readonly_while_writer_holds_lock() {
    let path = tmp();
    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("widgets", 100).unwrap();
    eng.define_himo_in("widgets", "n", ValueType::Number, 100).unwrap();
    let eids: Vec<u64> = (0..3u32)
        .map(|i| {
            let e = eng.entity_in("widgets").unwrap();
            eng.tie(e, "widgets.n", 10 + i);
            e
        })
        .collect();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();

    // writer は lock を握ったまま (drop しない)。 reader は flock を取らないので開ける。
    assert_eq!(probe(&path, &eids), "count=3 himos=1 tables=widgets vals=10:,11:,12:");

    // 後から himo を足す = 新しい segment file (himo/0001.seg)。 次の readonly open で見える。
    eng.define_himo_in("widgets", "m", ValueType::Number, 100).unwrap();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "widgets.m", 100 + i as u32);
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    assert_eq!(probe(&path, &eids), "count=3 himos=2 tables=widgets vals=10:100,11:101,12:102");

    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
