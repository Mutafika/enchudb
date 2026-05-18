//! enchu CLI の最低限の動作保証。 `--create --tiny` → REPL 経由で
//! define / insert / query / dot command を一通り回し、 stdout に
//! 期待値が出ることを確認する。

use std::io::Write;
use std::process::{Command, Stdio};

fn bin() -> std::path::PathBuf {
    // CARGO_BIN_EXE_<name> は integration test では自動で渡される。
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_enchu"))
}

fn tmp_db(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("enchu_cli_smoke_{}_{}.db", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(format!("{}.wal", p.display()));
    let _ = std::fs::remove_file(format!("{}.lock", p.display()));
    p
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path.display()));
    let _ = std::fs::remove_file(format!("{}.lock", path.display()));
}

#[test]
fn create_and_basic_repl_flow() {
    let db = tmp_db("basic");

    let status = Command::new(bin())
        .args(["--create", "--tiny", db.to_str().unwrap()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .expect("spawn enchu --create");
    assert!(status.success(), "create exited with {status:?}");
    assert!(db.exists(), "db file should exist after --create");

    let mut child = Command::new(bin())
        .arg(db.to_str().unwrap())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn enchu REPL");

    let script = "\
.define name tag
.define age num
+ name:\"alice\" age:30
+ name:\"bob\" age:25
+ name:\"carol\" age:30
age:30 | count
age:25
.entity 1
.himos
";
    child.stdin.as_mut().unwrap().write_all(script.as_bytes()).unwrap();
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait enchu");
    assert!(out.status.success(), "REPL exited with {:?}\nstderr: {}", out.status, String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);

    // 期待 output: define×2 / insert×3 / count=2 / age:25 → eid 1 / .entity 1 → bob / .himos
    assert!(stdout.contains("defined name"), "missing define name\n{stdout}");
    assert!(stdout.contains("defined age"), "missing define age\n{stdout}");
    assert!(stdout.contains("+0"), "first insert should print +0\n{stdout}");
    assert!(stdout.contains("+1"), "second insert should print +1\n{stdout}");
    assert!(stdout.contains("+2"), "third insert should print +2\n{stdout}");
    // age:30 | count = 2 が単独行として出る
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.iter().any(|l| l.trim() == "2"), "count should yield 2\n{stdout}");
    // age:25 は 1 件 (bob = eid 1)
    assert!(lines.iter().any(|l| l.trim() == "1"), "age:25 should match eid 1\n{stdout}");
    // .entity 1 で bob が読める
    assert!(stdout.contains("name: \"bob\""), ".entity 1 should print bob\n{stdout}");
    assert!(stdout.contains("age: 25"), ".entity 1 should print age 25\n{stdout}");
    // .himos の表
    assert!(stdout.contains("name\ttag"));
    assert!(stdout.contains("age\tnum"));

    cleanup(&db);
}

#[test]
fn one_shot_exec_via_dash_e() {
    let db = tmp_db("one_shot");

    // create + define + insert を 3 回の one-shot で投入
    for q in [".dummy"].iter().take(0) { let _ = q; } // (no-op, structural)
    let status = Command::new(bin())
        .args(["--create", "--tiny", db.to_str().unwrap()])
        .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit())
        .status().unwrap();
    assert!(status.success());

    // define を REPL stdin pipe で 1 度に流す (define は -e の query_lang 構文じゃないので)
    let mut child = Command::new(bin())
        .arg(db.to_str().unwrap())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn().unwrap();
    child.stdin.as_mut().unwrap().write_all(b".define k tag\n.define v num\n").unwrap();
    drop(child.stdin.take());
    let _ = child.wait_with_output().unwrap();

    // -e で 1 行 insert
    let out = Command::new(bin())
        .args([db.to_str().unwrap(), "-e", "+ k:\"foo\" v:42"])
        .output().unwrap();
    assert!(out.status.success(), "one-shot insert failed: {:?}\n{}", out.status, String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains('+'), "insert should print +<eid>\n{stdout}");

    // -e で 1 行 query
    let out = Command::new(bin())
        .args([db.to_str().unwrap(), "-e", "v:42 | count"])
        .output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.lines().any(|l| l.trim() == "1"), "v:42 | count should be 1\n{stdout}");

    cleanup(&db);
}
