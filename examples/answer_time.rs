//! 宣言された答え (DECLARED_ANSWERS_PLAN の段階 0): 開いた後に購読を張って最初の答えが出るまでの時間を測る。
//! 子 process に表を書かせてきれいに閉じ (open_time と同じ形)、 APFS の clone で写して新しい process で開き、 購読の登録 + 初回 poll
//! の時間を種類ごとに測る (集合 / 件数の group / 合計の group / 上位 10 件)。 cold = 写しをそのまま、 warm = 写しを先に全部読む。
//!
//! ```text
//! cargo run --release --example answer_time -- 100000 1000000      # 行数 (既定 100000 1000000)
//! ANSWER_TIME_REPS=5  ANSWER_TIME_CACHE=cold,warm  ANSWER_TIME_DIR=<置き場>
//! ```

use enchudb::schema::Database;
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("child") => child(&args[2], args[3].parse().unwrap()),
        Some("measure") => measure(&args[2], &args[3]),
        _ => driver(&args[1..]),
    }
}

/// users: id (名前) / age / city (Tag、 200 種) / score。 きれいに閉じる (Drop)
fn child(dir: &str, rows: u32) {
    let _ = enchudb::db_files::remove_db(dir);
    let mut db = Database::create(dir).unwrap();
    let users = db.table("users").number("id").number("age").tag("city").number("score").primary_key("id").build().unwrap();
    for i in 0..rows {
        let h = i.wrapping_mul(2_654_435_761);
        users
            .insert()
            .set("id", i as i64)
            .set("age", ((h >> 8) % 80 + 10) as i64)
            .set("city", format!("c{}", (h >> 3) % 200))
            .set("score", ((h >> 12) % 100_000) as i64)
            .commit()
            .unwrap();
    }
    drop(db);
    println!("CLEAN");
}

/// 開いて、 購読 1 種類を張って初回 poll まで。 kind: open / set / counts / sums / top
fn measure(dir: &str, kind: &str) {
    let t0 = Instant::now();
    let db = Database::open(dir).unwrap();
    let open_ms = ms(t0.elapsed());
    let users = db.get_table("users").unwrap();
    let t1 = Instant::now();
    let n = match kind {
        "open" => 0usize,
        "set" => users.all().where_gt("age", 30i64).subscribe().unwrap().poll().added.len(),
        "counts" => users.all().where_gt("age", 30i64).subscribe_counts("city").unwrap().poll().len(),
        "sums" => users.all().where_gt("age", 30i64).subscribe_sums("city", "score").unwrap().poll_sums().len(),
        "top" => users.all().order_by_desc("score").limit(10).subscribe().unwrap().poll().added.len(),
        other => panic!("知らない種類 {other}"),
    };
    println!("MS {open_ms:.3} {:.3} n {n}", ms(t1.elapsed()));
    std::process::exit(0);
}

fn env_list(name: &str, default: &[&str]) -> Vec<String> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => v.split(',').map(|s| s.trim().to_string()).collect(),
        _ => default.iter().map(|s| s.to_string()).collect(),
    }
}

fn warm_dir(dir: &str) {
    fn walk(p: &Path, buf: &mut Vec<u8>) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, buf);
                } else if let Ok(mut f) = std::fs::File::open(&path) {
                    while let Ok(n) = f.read(buf) {
                        if n == 0 {
                            break;
                        }
                    }
                }
            }
        }
    }
    let mut buf = vec![0u8; 4 << 20];
    walk(Path::new(dir), &mut buf);
}

fn clone_dir(src: &str, dst: &str) {
    let _ = std::fs::remove_dir_all(dst);
    let ok = Command::new("cp").args(["-c", "-R", src, dst]).stderr(Stdio::null()).status().is_ok_and(|s| s.success());
    if !ok {
        let _ = std::fs::remove_dir_all(dst);
        assert!(Command::new("cp").args(["-R", src, dst]).status().unwrap().success());
    }
}

fn driver(args: &[String]) {
    let reps: usize = std::env::var("ANSWER_TIME_REPS").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let base = std::env::var("ANSWER_TIME_DIR").unwrap_or_else(|_| format!("/tmp/enchu_answer_time_{}", std::process::id()));
    let caches = env_list("ANSWER_TIME_CACHE", &["cold", "warm"]);
    let sizes: Vec<u32> = if args.is_empty() { vec![100_000, 1_000_000] } else { args.iter().map(|s| s.parse().unwrap()).collect() };
    let exe = std::env::current_exe().unwrap();
    std::fs::create_dir_all(&base).unwrap();
    println!("answer_time — 開いた後の購読の登録 + 初回 poll (ms、 {reps} 回の中央値 / 最小 / 最大)、 置き場 {base}");
    println!("{:>9} {:<7} {:<5} {:>9} {:>9} {:>9} {:>9} {:>8}", "行", "種類", "cache", "open", "中央値", "最小", "最大", "答え n");
    for &rows in &sizes {
        let dir = format!("{base}/u{rows}");
        let st = Command::new(&exe).args(["child", &dir, &rows.to_string()]).stdout(Stdio::null()).status().unwrap();
        assert!(st.success());
        for kind in ["open", "set", "counts", "sums", "top"] {
            for cache in &caches {
                let mut open_v = Vec::new();
                let mut v = Vec::new();
                let mut n = 0usize;
                for r in 0..reps {
                    let copy = format!("{dir}_m{r}");
                    clone_dir(&dir, &copy);
                    if cache == "warm" {
                        warm_dir(&copy);
                    }
                    let out = Command::new(&exe).args(["measure", &copy, kind]).output().unwrap();
                    let text = String::from_utf8_lossy(&out.stdout).to_string();
                    if let Some(rest) = text.lines().find_map(|l| l.strip_prefix("MS ")) {
                        let mut it = rest.split_whitespace();
                        open_v.push(it.next().unwrap().parse::<f64>().unwrap());
                        v.push(it.next().unwrap().parse::<f64>().unwrap());
                        n = it.nth(1).unwrap().parse().unwrap();
                    } else {
                        eprintln!("   失敗 ({copy}): {text}{}", String::from_utf8_lossy(&out.stderr));
                    }
                    let _ = std::fs::remove_dir_all(&copy);
                }
                if v.is_empty() {
                    println!("{rows:>9} {kind:<7} {cache:<5} (失敗)");
                    continue;
                }
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                open_v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                println!(
                    "{rows:>9} {kind:<7} {cache:<5} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {n:>8}",
                    open_v[open_v.len() / 2],
                    v[v.len() / 2],
                    v[0],
                    v[v.len() - 1]
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
    let _ = std::fs::remove_dir_all(&base);
}
