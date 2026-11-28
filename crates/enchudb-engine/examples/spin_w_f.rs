//! スピン 案 W: 案 F (案 E の書き込みと鎖のまま、 デッキは束の大きさで読み方を選ぶ) を、 現実に寄せた場面で今の形 / 案 C / 案 E と比べる。
//!
//! - 書き込みの量: 1 行あたり約 10 回の変更 と 約 50 回 (今までの極端な量) の 2 段
//! - 見る値は一部: 値 1000 種のうち 1 / 10 値、 値 10 種のうち 1 値
//! - 数: 書き手の上乗せ (乱れた行 / 熱い行、 見る前 = 今の形 / C / E(=F の書き手)、 見た後 = 今の形 / E / F)、 係の遅れ、 時点 t の 1 行、
//!   時点 t のデッキ (円柱の何倍)、 最初の並びの時間と並べた書き手の最長、 並びの memory
//! - 案 E / F の時点 t の 1 行: 並びを持った / 捨てた (`history_forget_decks`) / 今の形に戻した で同じ読みを比べる (page cache の見立て)
//!
//! `spin_w_f <dir>`。 行 100 万、 書き手 1 本。 1 run は短く (組 10、 読みは 2 秒まで)。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ROWS: u32 = 1_000_000;
const CAP: usize = 256 * 1024 * 1024;
const N: usize = 20_000;
const PAIRS: usize = 10;
const READ_BUDGET: Duration = Duration::from_secs(2);

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn med(v: &mut [f64]) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn cpu_ticks() -> u64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let rest = &s[s.rfind(')').unwrap() + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    f[11].parse::<u64>().unwrap() + f[12].parse::<u64>().unwrap()
}

fn dirty_kb() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .unwrap()
        .lines()
        .filter(|l| l.starts_with("Dirty:") || l.starts_with("Writeback:"))
        .map(|l| l.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap())
        .sum()
}

fn rss_anon_mb() -> u64 {
    unsafe {
        libc::malloc_trim(0);
    }
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|l| l.starts_with("RssAnon:"))
        .map_or(0, |l| l.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() / 1024)
}

/// 裏 (書き出しの msync・係) が静まるまで待つ (`iso-run1.txt`)。
fn quiesce() {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        let c0 = cpu_ticks();
        std::thread::sleep(Duration::from_millis(200));
        if ((cpu_ticks() - c0) as f64 / 100.0) / 0.2 < 0.15 && dirty_kb() < 32 * 1024 {
            return;
        }
    }
}

fn rewrite(eng: &Engine, rows: &[u64], hid: u16, card: u64, rng: &mut Rng) -> f64 {
    let t = Instant::now();
    for _ in 0..N {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        eng.tie_to_by_id(e, hid, (rng.next() % card) as u32);
    }
    t.elapsed().as_nanos() as f64 / N as f64
}

/// 宣言なしの列との差の中央値 (静めて温めてから、 宣言なしの列は前と後の平均)。
fn extra(eng: &Engine, rows: &[u64], u: &str, h: &str, card: u64, rng: &mut Rng) -> f64 {
    let (uh, hh) = (eng.himo_id(u).unwrap() as u16, eng.himo_id(h).unwrap() as u16);
    quiesce();
    rewrite(eng, rows, uh, card, rng);
    let mut d = Vec::new();
    for _ in 0..PAIRS {
        let b = rewrite(eng, rows, uh, card, rng);
        let x = rewrite(eng, rows, hh, card, rng);
        d.push(x - (b + rewrite(eng, rows, uh, card, rng)) / 2.0);
    }
    med(&mut d)
}

/// 時点 t の 1 行: (乱れた行 ns, 熱い行の遠い t ns)。
fn rows_at(eng: &Engine, rows: &[u64], hot: &[u64], h: &str, rng: &mut Rng) -> (f64, f64) {
    let clock = eng.history_clock(h).unwrap();
    let h0 = eng.history(rows[1000], h).unwrap()[0].time;
    let span = clock - h0;
    let n = 100_000;
    let t = Instant::now();
    for _ in 0..n {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        std::hint::black_box(eng.value_at(e, h, h0 + rng.next() % (span + 1)).unwrap());
    }
    let row = t.elapsed().as_nanos() as f64 / n as f64;
    let t = Instant::now();
    for _ in 0..n {
        let e = hot[(rng.next() % hot.len() as u64) as usize];
        std::hint::black_box(eng.value_at(e, h, h0 + 1).unwrap());
    }
    (row, t.elapsed().as_nanos() as f64 / n as f64)
}

/// 時点 t のデッキ (見た値から): (中央値 µs, 円柱の何倍、 回数)。
fn decks_at(eng: &Engine, rows: &[u64], h: &str, vals: &[u64], rng: &mut Rng) -> (f64, f64, usize) {
    let clock = eng.history_clock(h).unwrap();
    let h0 = eng.history(rows[1000], h).unwrap()[0].time;
    let span = clock - h0;
    let (mut us, mut ratio) = (Vec::new(), Vec::new());
    let start = Instant::now();
    while us.len() < 500 && start.elapsed() < READ_BUDGET {
        let v = vals[(rng.next() % vals.len() as u64) as usize];
        let tt = h0 + rng.next() % (span + 1);
        let a = Instant::now();
        std::hint::black_box(eng.pull_at(h, v, tt).unwrap());
        let x = a.elapsed().as_nanos() as f64;
        let b = Instant::now();
        std::hint::black_box(eng.pull_raw(h, v as u32));
        let y = b.elapsed().as_nanos() as f64;
        us.push(x / 1000.0);
        ratio.push(x / y.max(1.0));
    }
    let n = us.len();
    (med(&mut us), med(&mut ratio), n)
}

/// 見る値の並びを作る (書き手 1 本を並べる)。 (作った ms, 書き手の書き込み 1 回の最長 ms)
fn first_view(eng: &Arc<Engine>, rows: &[u64], h: &str, card: u64, vals: &[u64]) -> (f64, f64) {
    let hh = eng.himo_id(h).unwrap() as u16;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w = {
        let (eng, rows, stop) = (eng.clone(), rows.to_vec(), stop.clone());
        std::thread::spawn(move || {
            let mut r = Rng(99);
            let mut worst = 0u128;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let e = rows[(r.next() % rows.len() as u64) as usize];
                let a = Instant::now();
                eng.tie_to_by_id(e, hh, (r.next() % card) as u32);
                worst = worst.max(a.elapsed().as_nanos());
            }
            worst
        })
    };
    std::thread::sleep(Duration::from_millis(20));
    let a = Instant::now();
    eng.history_observe(h, vals).unwrap();
    let ms = a.elapsed().as_secs_f64() * 1000.0;
    std::thread::sleep(Duration::from_millis(20));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    (ms, w.join().unwrap() as f64 / 1e6)
}

/// 形: 0 = 今の形、 1 = 案 E、 2 = 案 F。
fn set_mode(eng: &Engine, h: &str, m: usize) {
    eng.history_forget_decks(h).unwrap();
    eng.history_set_e(h, m > 0, None).unwrap();
    if m > 0 {
        eng.history_set_f(h, m == 2).unwrap();
    }
}

fn bulk(eng: &Engine, rows: &[u64], hid: u16, card: u64, n: u64, rng: &mut Rng) {
    for _ in 0..n {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        eng.tie_to_by_id(e, hid, (rng.next() % card) as u32);
    }
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/spin_w_f".into());
    let _ = std::fs::remove_dir_all(&dir);
    let mut eng = Engine::create_with_capacity(&dir, ROWS + 1024).unwrap();
    eng.define_table("t", ROWS + 16).unwrap();
    for (c, card) in [("u1000", 1000), ("h1000", 1000), ("u10", 10), ("h10", 10)] {
        eng.define_himo_in("t", c, ValueType::Number, card).unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    let rows: Vec<u64> = (0..ROWS).map(|_| eng.entity_in("t").unwrap()).collect();
    for (c, card) in [("t.u1000", 1000u64), ("t.h1000", 1000), ("t.u10", 10), ("t.h10", 10)] {
        let hid = eng.himo_id(c).unwrap() as u16;
        for (i, &e) in rows.iter().enumerate() {
            eng.tie_to_by_id(e, hid, (i as u64 % card) as u32);
        }
    }
    eng.oplog_sync().unwrap();
    eng.declare_history("t.h1000").unwrap();
    eng.declare_history("t.h10").unwrap();
    let hot: Vec<u64> = rows[..100].to_vec();
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let names = ["base", "E", "F"];
    let mut done_per_row = 0u64;
    // F_STAGES=50 なら 10 回 / 行の段は書くだけで測らない。 F_NO_C=1 なら 50 回 / 行の段で案 C を測らない (全体を繋ぐ後片付けが終わらない)
    let stages: Vec<u64> = std::env::var("F_STAGES").map_or(vec![10, 50], |s| s.split(',').map(|x| x.parse().unwrap()).collect());
    let no_c_at_50 = std::env::var("F_NO_C").is_ok();
    for per_row in [10u64, 50] {
        // 書き込みの量を 1 行あたり per_row 回に (今の形で書く)
        let t0 = Instant::now();
        for (h, card) in [("t.h1000", 1000u64), ("t.h10", 10)] {
            set_mode(&eng, h, 0);
            bulk(&eng, &rows, eng.himo_id(h).unwrap() as u16, card, (per_row - done_per_row) * u64::from(ROWS), &mut rng);
        }
        done_per_row = per_row;
        println!("== {per_row} changes per row (bulk {:.0} s, rss_anon {} MB)", t0.elapsed().as_secs_f64(), rss_anon_mb());
        if !stages.contains(&per_row) {
            continue;
        }
        let only_card: Option<u64> = std::env::var("F_CARD").ok().map(|x| x.parse().unwrap());
        let modes: Vec<usize> = std::env::var("F_MODES").map_or(vec![0, 1, 2], |x| x.split(',').map(|y| y.parse().unwrap()).collect());
        for (card, u, h, views) in [(1000u64, "t.u1000", "t.h1000", &[1usize, 10][..]), (10, "t.u10", "t.h10", &[1][..])] {
            if only_card.is_some_and(|c| c != card) {
                continue;
            }
            // ---- 見る前: 今の形 / C / E (F の書き手は E と同じ)、 2 組で順を回す ----
            let mut s = String::new();
            for (who, set) in [("random", &rows[..]), ("hot", &hot[..])] {
                let (mut vb, mut vc, mut ve, mut lag) = (Vec::new(), Vec::new(), Vec::new(), 0u32);
                for r in 0..2 {
                    for k in 0..3 {
                        match (k + r) % 3 {
                            0 => {
                                set_mode(&eng, h, 0);
                                vb.push(extra(&eng, set, u, h, card, &mut rng));
                            }
                            1 if no_c_at_50 && per_row >= 50 => {}
                            1 => {
                                set_mode(&eng, h, 0);
                                eng.history_set_lazy(h, true).unwrap();
                                vc.push(extra(&eng, set, u, h, card, &mut rng));
                                eng.history_set_lazy(h, false).unwrap();
                            }
                            _ => {
                                set_mode(&eng, h, 1);
                                let _ = eng.history_e_stats(h);
                                ve.push(extra(&eng, set, u, h, card, &mut rng));
                                lag = lag.max(eng.history_e_stats(h).unwrap().1);
                                set_mode(&eng, h, 0);
                            }
                        }
                    }
                }
                s += &format!(
                    " | {who}: base={:.0} C={:.0} E={:.0} (lag max {lag})",
                    med(&mut vb),
                    med(&mut vc),
                    med(&mut ve)
                );
            }
            println!("card={card} per_row={per_row} before view:{s}");
            // ---- 見た後 ----
            for &nv in views {
                let vals: Vec<u64> = (0..nv as u64).collect();
                for &m in &modes {
                    set_mode(&eng, h, m);
                    let m0 = rss_anon_mb();
                    let (obs_ms, stall) = first_view(&eng, &rows, h, card, &vals);
                    let mem = rss_anon_mb().saturating_sub(m0);
                    let _ = eng.history_e_stats(h);
                    let xr = extra(&eng, &rows, u, h, card, &mut rng);
                    let xh = extra(&eng, &hot, u, h, card, &mut rng);
                    let lag = if m > 0 { eng.history_e_stats(h).unwrap().1 } else { 0 };
                    let (row, far) = rows_at(&eng, &rows, &hot, h, &mut rng);
                    let (dus, dx, dn) = decks_at(&eng, &rows, h, &vals, &mut rng);
                    let dmem = if m > 0 { eng.history_e_stats(h).unwrap().2 / 1_000_000 } else { 0 };
                    let mut line = format!(
                        "card={card} per_row={per_row} view={nv} {}: first view {obs_ms:.0} ms (writer worst {stall:.1} ms), write after random={xr:.0} hot={xh:.0} ns (lag max {lag}), value_at={row:.0} ns far_hot={far:.0} ns, deck={dus:.1} µs ({dx:.1}x cylinder, {dn} reads), mem +{mem} MB (decks {dmem} MB)",
                        names[m]
                    );
                    if m > 0 {
                        // 3. 時点 t の 1 行: 並びを持った (上) / 捨てた / 今の形に戻した
                        eng.history_forget_decks(h).unwrap();
                        let (r_forgot, _) = rows_at(&eng, &rows, &hot, h, &mut rng);
                        eng.history_set_e(h, false, None).unwrap();
                        let (r_base, _) = rows_at(&eng, &rows, &hot, h, &mut rng);
                        line += &format!(" | value_at held={row:.0} forgot={r_forgot:.0} as_base={r_base:.0} ns");
                    }
                    println!("{line}");
                    set_mode(&eng, h, 0);
                }
            }
        }
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
