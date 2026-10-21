//! 開く時間 (OPEN_TIME_PLAN.md の段階 0): 子 process に書かせて SIGKILL し、 新しい process で開く時間を測る。
//!
//! 形: 行 × 列 (列は Number / Tag (語 1 万) / Leaf の順に混ぜる、 50 行に 1 行は消す)、 sync の表あり (`s`) / なし。
//! 止め方 3 つ: `clean` (きれいに閉じる) / `idle` (書き終えて同期し、 暇にしてから kill = スマホの background の kill) /
//! `mid` (書いている最中に kill)。 開き方 2 つ: `writer` (`open_concurrent_with_oplog`、 復旧と #451 の照合が走る) /
//! `readonly` (復旧なし = 下限)。 止めた DB は APFS の clone (`cp -c`) で写してから開くので、 同じ像を何回でも開ける
//! (開くと復旧が済んで次は 「きれいに閉じた後」 になるので、 元は触らない)。
//!
//! ```text
//! cargo run --release -p enchudb-engine --example open_time                          # 既定の形 5 つ
//! cargo run --release -p enchudb-engine --example open_time -- 100000x3s 1000000x10    # 行x列[s = sync あり]
//! OPEN_TIME_REPS=5 (開く回数、 中央値)  OPEN_TIME_DIR=<置き場> (既定 /tmp/enchu_open_time_<pid>)
//! OPEN_TIME_STOPS=clean,idle,mid  OPEN_TIME_KINDS=writer,readonly
//! OPEN_TIME_MIX=ntl (列の種類の並び: n = Number / t = Tag / l = Leaf、 列 c は mix[c % len])  OPEN_TIME_DELETE_EVERY=50 (0 = 消さない)
//! OPEN_TIME_CACHE=cold,warm (cold = 写しをそのまま開く。 APFS の clone は page cache が別なので region はディスクから読む /
//! warm = 写しの file を全部読んでから開く = 落ちる直前まで触っていた page が残っている形)
//! ```
//!
//! sync ありは `_sync_ops` の枠 (残りの eid 空間の半分、 最大 100 万行) に配る分が入る大きさまで (行 × (列 + 1) が目安。
//! 超えると bridge が ack 待ちで止まり、 oplog が畳めずに伸びる)。

use enchudb_engine::{Engine, ValueType};
use std::fmt::Write as _;
use std::io::{BufRead, BufReader, Write as _};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const OPLOG_CAP: usize = 256 * 1024 * 1024;

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("child") => child(&args[2], args[3].parse().unwrap(), args[4].parse().unwrap(), args[5] == "1", &args[6]),
        Some("open") => open_once(&args[2], &args[3]),
        _ => driver(&args[1..]),
    }
}

fn mix() -> Vec<u8> {
    let m = std::env::var("OPEN_TIME_MIX").unwrap_or_else(|_| "ntl".to_string());
    let v: Vec<u8> = m.bytes().filter(|b| b"ntl".contains(b)).collect();
    if v.is_empty() { b"ntl".to_vec() } else { v }
}

fn delete_every() -> u32 {
    std::env::var("OPEN_TIME_DELETE_EVERY").ok().and_then(|s| s.parse().ok()).unwrap_or(50)
}

fn kind_of(c: u32) -> ValueType {
    let m = mix();
    match m[c as usize % m.len()] {
        b'n' => ValueType::Number,
        b't' => ValueType::Tag,
        _ => ValueType::Leaf,
    }
}

/// 書く側 (子 process)。 止め方は stdout の 1 行で親に知らせる: `MID` (半分書いた) / `IDLE` (書き終えて同期した) / `CLEAN`。
fn child(dir: &str, rows: u32, cols: u32, sync: bool, stop: &str) {
    let _ = enchudb_engine::db_files::remove_db(dir);
    // 行の分 + 消した分 + sync の表の分 (`_sync_ops` は残りの半分、 最大 100 万行)
    let cap = rows.saturating_mul(3).max(1 << 21);
    let mut eng = Engine::create_with_cell_version(dir, cap).unwrap();
    eng.define_table("t", rows + rows / 8 + 16).unwrap();
    for c in 0..cols {
        eng.define_himo_in("t", &format!("c{c}"), kind_of(c), 0).unwrap();
    }
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let names: Vec<String> = (0..cols).map(|c| format!("t.c{c}")).collect();
    let mut text = String::new();
    let del = delete_every();
    let t0 = Instant::now();
    for i in 0..rows {
        if stop == "mid" && i == rows / 2 {
            println!("MID");
            std::io::stdout().flush().unwrap();
        }
        let e = eng.entity_in("t").unwrap();
        for c in 0..cols {
            let name = &names[c as usize];
            match kind_of(c) {
                ValueType::Number => eng.tie_async(e, name, (i.wrapping_mul(2_654_435_761) >> 8) % 1000 + c),
                ValueType::Tag => {
                    text.clear();
                    write!(text, "w{}", i.wrapping_mul(7).wrapping_add(c) % 10_000).unwrap();
                    eng.tie_text_async(e, name, &text);
                }
                _ => {
                    text.clear();
                    write!(text, "行 {i} の本文 {c}: 短い文がここに入る").unwrap();
                    eng.tie_text_async(e, name, &text);
                }
            }
        }
        if del > 0 && i % del == del - 1 {
            eng.delete(e);
        }
        if i % 1000 == 999 {
            eng.oplog_commit();
        }
    }
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    println!("WROTE {:.0} ms entities {}", ms(t0.elapsed()), eng.entity_count());
    if stop == "clean" {
        drop(eng);
        println!("CLEAN");
        return;
    }
    // idle (と、 MID で kill されなかった mid): 暇にして親の kill を待つ
    println!("IDLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}

/// 開く側 (新しい process)。 開く呼び出しだけを測り、 閉じずに exit (写しは捨てるので Drop の書き出しは要らない)。
fn open_once(dir: &str, kind: &str) {
    let t = Instant::now();
    let (took, n) = if kind == "writer" {
        let eng = Engine::open_concurrent_with_oplog(dir, OPLOG_CAP).unwrap();
        let took = t.elapsed();
        let n = eng.entity_count();
        std::mem::forget(eng);
        (took, n)
    } else {
        let eng = Engine::open_readonly(dir).unwrap();
        let took = t.elapsed();
        let n = eng.entity_count();
        std::mem::forget(eng);
        (took, n)
    };
    println!("OPEN_MS {:.3} entities {n}", ms(took));
    std::process::exit(0);
}

fn env_list(name: &str, default: &[&str]) -> Vec<String> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => v.split(',').map(|s| s.trim().to_string()).collect(),
        _ => default.iter().map(|s| s.to_string()).collect(),
    }
}

/// `100000x3s` → (行, 列, sync あり)
fn parse_spec(spec: &str) -> (u32, u32, bool) {
    let sync = spec.ends_with('s');
    let body = spec.trim_end_matches('s');
    let (r, c) = body.split_once('x').unwrap_or_else(|| panic!("形は 行x列[s]: {spec}"));
    (r.parse().unwrap(), c.parse().unwrap(), sync)
}

fn load1() -> String {
    Command::new("sysctl")
        .args(["-n", "vm.loadavg"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.split_whitespace().nth(1).map(str::to_string))
        .or_else(|| std::fs::read_to_string("/proc/loadavg").ok().and_then(|s| s.split_whitespace().next().map(str::to_string)))
        .unwrap_or_else(|| "-".to_string())
}

/// 実ディスク (sparse の穴は数えない)、 MiB
fn du_mib(dir: &str) -> f64 {
    use std::os::unix::fs::MetadataExt;
    fn walk(p: &Path, acc: &mut u64) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if let Ok(m) = path.metadata() {
                    *acc += m.blocks() * 512;
                }
            }
        }
    }
    let mut acc = 0;
    walk(Path::new(dir), &mut acc);
    acc as f64 / (1024.0 * 1024.0)
}

/// 写しの file を全部読んで page cache に載せる (sparse の穴は 0 を読むだけ)
fn warm_dir(dir: &str) {
    fn walk(p: &Path, buf: &mut Vec<u8>) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, buf);
                } else if let Ok(mut f) = std::fs::File::open(&path) {
                    use std::io::Read;
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

/// 止めた DB の写し (APFS の clone、 無ければ普通の copy)
fn clone_dir(src: &str, dst: &str) {
    let _ = std::fs::remove_dir_all(dst);
    let ok = Command::new("cp").args(["-c", "-R", src, dst]).stderr(Stdio::null()).status().is_ok_and(|s| s.success());
    if !ok {
        let _ = std::fs::remove_dir_all(dst);
        assert!(Command::new("cp").args(["-R", src, dst]).status().unwrap().success(), "cp -R {src} {dst}");
    }
}

/// 子 process に書かせ、 止め方に合わせて kill する。 (書いた ms、 entity の数。 mid は 0)
fn write_db(exe: &Path, dir: &str, rows: u32, cols: u32, sync: bool, stop: &str) -> (f64, u32) {
    let mut ch = Command::new(exe)
        .args(["child", dir, &rows.to_string(), &cols.to_string(), if sync { "1" } else { "0" }, stop])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut rd = BufReader::new(ch.stdout.take().unwrap());
    let (mut wrote, mut entities) = (0.0, 0);
    let mut line = String::new();
    loop {
        line.clear();
        if rd.read_line(&mut line).unwrap() == 0 {
            break;
        }
        let l = line.trim();
        if let Some(rest) = l.strip_prefix("WROTE ") {
            let mut it = rest.split_whitespace();
            wrote = it.next().and_then(|x| x.parse().ok()).unwrap_or(0.0);
            entities = it.nth(2).and_then(|x| x.parse().ok()).unwrap_or(0);
        } else if l == "MID" {
            ch.kill().unwrap();
            break;
        } else if l == "IDLE" {
            // consumer の tick (100 ms) が書き出して checkpoint を進めるのを待ってから kill (background で暇な時の kill)
            std::thread::sleep(Duration::from_millis(1500));
            ch.kill().unwrap();
            break;
        } else if l == "CLEAN" {
            break;
        }
    }
    let _ = ch.wait();
    (wrote, entities)
}

fn driver(specs: &[String]) {
    let reps: usize = std::env::var("OPEN_TIME_REPS").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let base = std::env::var("OPEN_TIME_DIR").unwrap_or_else(|_| format!("/tmp/enchu_open_time_{}", std::process::id()));
    let stops = env_list("OPEN_TIME_STOPS", &["clean", "idle", "mid"]);
    let kinds = env_list("OPEN_TIME_KINDS", &["writer", "readonly"]);
    let caches = env_list("OPEN_TIME_CACHE", &["cold", "warm"]);
    let specs: Vec<String> = if specs.is_empty() {
        ["100000x3s", "100000x10s", "100000x3", "1000000x3", "1000000x10"].iter().map(|s| s.to_string()).collect()
    } else {
        specs.to_vec()
    };
    let exe = std::env::current_exe().unwrap();
    std::fs::create_dir_all(&base).unwrap();
    println!(
        "open_time — 開く時間 (ms、 {reps} 回の中央値 / 最小 / 最大)、 列 {}、 delete 1/{}、 負荷 {}、 置き場 {base}",
        String::from_utf8_lossy(&mix()),
        delete_every(),
        load1()
    );
    println!(
        "{:<12} {:<6} {:>9} {:>9} {:>8}  {:<9} {:<5} {:>9} {:>9} {:>9}",
        "形", "止め方", "書き ms", "entity", "MiB", "開き方", "cache", "中央値", "最小", "最大"
    );
    for spec in &specs {
        let (rows, cols, sync) = parse_spec(spec);
        for stop in &stops {
            let dir = format!("{base}/{spec}_{stop}");
            let (wrote_ms, entities) = write_db(&exe, &dir, rows, cols, sync, stop);
            let size = du_mib(&dir);
            for (kind, cache) in kinds.iter().flat_map(|k| caches.iter().map(move |c| (k, c))) {
                let mut v = Vec::with_capacity(reps);
                for r in 0..reps {
                    let copy = format!("{dir}_open{r}");
                    clone_dir(&dir, &copy);
                    if cache == "warm" {
                        warm_dir(&copy);
                    }
                    let out = Command::new(&exe).args(["open", &copy, kind]).output().unwrap();
                    let text = String::from_utf8_lossy(&out.stdout).to_string();
                    let got = text
                        .lines()
                        .find_map(|l| l.strip_prefix("OPEN_MS "))
                        .and_then(|l| l.split_whitespace().next())
                        .and_then(|x| x.parse::<f64>().ok());
                    match got {
                        Some(x) => v.push(x),
                        None => eprintln!("   開けなかった ({copy}): {text}{}", String::from_utf8_lossy(&out.stderr)),
                    }
                    let _ = std::fs::remove_dir_all(&copy);
                }
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                if v.is_empty() {
                    println!("{spec:<12} {stop:<6} {wrote_ms:>9.0} {entities:>9} {size:>8.0}  {kind:<9} {cache:<5} (開けなかった)");
                    continue;
                }
                println!(
                    "{spec:<12} {stop:<6} {wrote_ms:>9.0} {entities:>9} {size:>8.0}  {kind:<9} {cache:<5} {:>9.2} {:>9.2} {:>9.2}",
                    v[v.len() / 2],
                    v[0],
                    v[v.len() - 1]
                );
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
    let _ = std::fs::remove_dir_all(&base);
}
