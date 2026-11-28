//! スピン 案 W (案 E に一本化した後) の測り: 書き手は糸なしで足すだけ、 裏の係と読み手が繋ぐ、 見た値の並びは係が足す (控え + 差分)。
//!
//! - 書き込みの量: 1 行あたり約 10 回 / 約 50 回の変更の 2 段 (`E_STAGES=10,50`)
//! - 見る値は一部: 値 1000 種のうち 1 / 10 値、 値 10 種のうち 1 値
//! - 数: 書き手の上乗せ (宣言の無い同じ形の列との差、 乱れた行 / 熱い行、 見る前と見た後)、 係の遅れ、 最初の並びの時間と並べた書き手の
//!   最長、 時点 t のデッキ (円柱の何倍)、 並びの memory、 時点 t の 1 行
//! - 時点 t の 1 行は順を入れ替えて 2 回: S1 = 並びを持ったまま → 捨てて、 S2 = 捨てて → 作り直して持ったまま (どちらも前に静める)
//! - `--features spin-w-bench` の build なら、 1 行の読みの中身: 1 回にたどる entry の数 (そのうち床を探す道)、 読み手が係の lock を
//!   取った回数と繋いだ数、 entry の file の待ちの重ならない random な読み 1 回の ns
//!
//! `spin_w_e <dir>`。 行 100 万、 書き手 1 本。 1 run (2 段) は x86 4 vCPU で約 25 分。

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

/// process の CPU 時間 (tick、 Linux の /proc。 無い OS では 0)。
fn cpu_ticks() -> u64 {
    let Ok(s) = std::fs::read_to_string("/proc/self/stat") else { return 0 };
    let rest = &s[s.rfind(')').unwrap() + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    f[11].parse::<u64>().unwrap() + f[12].parse::<u64>().unwrap()
}

/// Dirty + Writeback (kB、 Linux の /proc。 無い OS では 0)。
fn dirty_kb() -> u64 {
    let Ok(m) = std::fs::read_to_string("/proc/meminfo") else { return 0 };
    m.lines()
        .filter(|l| l.starts_with("Dirty:") || l.starts_with("Writeback:"))
        .map(|l| l.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap())
        .sum()
}

/// 裏 (書き出しの msync・係) が静まるまで待つ (`iso-run1.txt`)。 /proc の無い OS では 2 秒待つだけ。
fn quiesce() {
    if !std::path::Path::new("/proc/self/stat").exists() {
        std::thread::sleep(Duration::from_secs(2));
        return;
    }
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

/// 時点 t の 1 行: (乱れた行 ns, 熱い行の遠い t ns, 中身)。
fn rows_at(eng: &Engine, rows: &[u64], hot: &[u64], h: &str, rng: &mut Rng) -> (f64, f64, String) {
    let clock = eng.history_clock(h).unwrap();
    let h0 = eng.history(rows[1000], h).unwrap()[0].time;
    let span = clock - h0;
    let n = 100_000;
    #[cfg(feature = "spin-w-bench")]
    let _ = enchudb_engine::history::probe::take();
    let t = Instant::now();
    for _ in 0..n {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        std::hint::black_box(eng.value_at(e, h, h0 + rng.next() % (span + 1)).unwrap());
    }
    let row = t.elapsed().as_nanos() as f64 / n as f64;
    #[cfg(feature = "spin-w-bench")]
    let inside = {
        let (hops, fl, locks, linked) = enchudb_engine::history::probe::take();
        format!(
            "hops/read={:.1} (floor {:.1}), reader locks={locks} linked={linked}",
            hops as f64 / n as f64,
            fl as f64 / n as f64
        )
    };
    #[cfg(not(feature = "spin-w-bench"))]
    let inside = String::new();
    let t = Instant::now();
    for _ in 0..n {
        let e = hot[(rng.next() % hot.len() as u64) as usize];
        std::hint::black_box(eng.value_at(e, h, h0 + 1).unwrap());
    }
    (row, t.elapsed().as_nanos() as f64 / n as f64, inside)
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

fn bulk(eng: &Engine, rows: &[u64], hid: u16, card: u64, n: u64, rng: &mut Rng) {
    for _ in 0..n {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        eng.tie_to_by_id(e, hid, (rng.next() % card) as u32);
    }
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/spin_w_e".into());
    let stages: Vec<u64> = std::env::var("E_STAGES").map_or(vec![10, 50], |s| s.split(',').map(|x| x.parse().unwrap()).collect());
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
    let mut done_per_row = 0u64;
    for &per_row in &stages {
        let t0 = Instant::now();
        for (h, card) in [("t.h1000", 1000u64), ("t.h10", 10)] {
            eng.history_forget_decks(h).unwrap();
            bulk(&eng, &rows, eng.himo_id(h).unwrap() as u16, card, per_row.saturating_sub(done_per_row) * u64::from(ROWS), &mut rng);
        }
        done_per_row = done_per_row.max(per_row);
        println!("== {per_row} changes per row (bulk {:.0} s)", t0.elapsed().as_secs_f64());
        for (card, u, h, views) in [(1000u64, "t.u1000", "t.h1000", &[1usize, 10][..]), (10, "t.u10", "t.h10", &[1][..])] {
            eng.history_forget_decks(h).unwrap();
            let _ = eng.history_e_stats(h);
            let br = extra(&eng, &rows, u, h, card, &mut rng);
            let bh = extra(&eng, &hot, u, h, card, &mut rng);
            let lag = eng.history_e_stats(h).unwrap().1;
            #[cfg(feature = "spin-w-bench")]
            let dep = format!(", dependent random entry read={:.0} ns", eng.history_probe_dependent_reads(h, 1_000_000).unwrap());
            #[cfg(not(feature = "spin-w-bench"))]
            let dep = String::new();
            let (n, _, eb, _) = eng.history_stats(h).unwrap();
            println!(
                "card={card} per_row={per_row} before view: write random={br:.0} hot={bh:.0} ns (lag max {lag}), entries={n} ({:.0} MB){dep}",
                eb as f64 / 1e6
            );
            for &nv in views {
                let vals: Vec<u64> = (0..nv as u64).collect();
                eng.history_forget_decks(h).unwrap();
                let (obs_ms, stall) = first_view(&eng, &rows, h, card, &vals);
                let _ = eng.history_e_stats(h);
                let xr = extra(&eng, &rows, u, h, card, &mut rng);
                let xh = extra(&eng, &hot, u, h, card, &mut rng);
                let (_, lag, dmem) = eng.history_e_stats(h).unwrap();
                let (dus, dx, dn) = decks_at(&eng, &rows, h, &vals, &mut rng);
                // 時点 t の 1 行: S1 = 持ったまま → 捨てて
                quiesce();
                let (held1, far, in1) = rows_at(&eng, &rows, &hot, h, &mut rng);
                eng.history_forget_decks(h).unwrap();
                let (forgot1, _, _) = rows_at(&eng, &rows, &hot, h, &mut rng);
                // S2 = 捨てたまま → 作り直して持ったまま
                quiesce();
                let (forgot2, _, in2) = rows_at(&eng, &rows, &hot, h, &mut rng);
                eng.history_observe(h, &vals).unwrap();
                quiesce();
                let (held2, _, _) = rows_at(&eng, &rows, &hot, h, &mut rng);
                println!(
                    "card={card} per_row={per_row} view={nv}: first view {obs_ms:.0} ms (writer worst {stall:.1} ms), write after random={xr:.0} hot={xh:.0} ns (lag max {lag}), deck={dus:.1} µs ({dx:.1}x cylinder, {dn} reads), decks {:.0} MB | value_at S1 held={held1:.0} forgot={forgot1:.0} / S2 forgot={forgot2:.0} held={held2:.0} ns, far_hot={far:.0} ns | {in1} | {in2}",
                    dmem as f64 / 1e6
                );
            }
            eng.history_forget_decks(h).unwrap();
        }
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
