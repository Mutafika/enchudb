//! スピン 案 W の 3 回目の比べ: 今の形 / 案 C / 案 E (書き手はいつも足すだけ、 裏の係と読み手が繋ぐ)。
//!
//! - 書き込みの上乗せ: 宣言の無い同じ形の列との差を 2 万回 × 60 組の中央値。 デッキを見る前と、 全部の値を見た後 (今の形は書き手が並びを保つ、
//!   案 E は係が保つ)
//! - 案 E の係の遅れ: 書き手 1 本の全力の間の (len − 繋いだ所) の最大
//! - 読み: 時点 t の 1 行 (random な行 / 熱い行の遠い t)、 時点 t のデッキ (中央値と円柱の読みの何倍か)。 1 つの形の読みは合わせて 3 秒まで
//! - 最初に見た値の並びを作る時間と、 その間に並べた書き手 1 本の書き込み 1 回の最長 (止まり)
//! - 案 E の控えの間隔の下限 K を 256 / 1024 / 4096 で、 デッキの読みと memory
//!
//! `spin_w_cmp3 <dir>`。 行 100 万、 列 u1000 / h1000 (値 0..1000)、 u10 / h10 (値 0..10)。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ROWS: u32 = 1_000_000;
const CAP: usize = 256 * 1024 * 1024;
const N: usize = 20_000;
const PAIRS: usize = 30;
const READ_BUDGET: Duration = Duration::from_secs(3);

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

/// 裏 (書き出しの msync・係) が静まるまで待つ (CPU と Dirty + Writeback、 60 秒まで)。 形を切り替えた (全体を繋いだ) 後の書き出しが
/// 次の段の測りに重ならないように (`iso-run1.txt` の切り分け)。
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

/// /proc/self/status の RssAnon (MB、 file を貼った page は数えない)。 先に malloc の手元に残った空きを OS に返す (段の memory を前の段の残りと分けて見る)。
fn rss_mb() -> u64 {
    unsafe {
        libc::malloc_trim(0);
    }
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|l| l.starts_with("RssAnon:"))
        .map_or(0, |l| l.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap() / 1024)
}

fn rewrite(eng: &Engine, rows: &[u64], hid: u16, card: u64, rng: &mut Rng) -> f64 {
    let t = Instant::now();
    for _ in 0..N {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        eng.tie_to_by_id(e, hid, (rng.next() % card) as u32);
    }
    t.elapsed().as_nanos() as f64 / N as f64
}

/// 宣言なしの列との差の中央値 (形は呼ぶ前に決める)。
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

/// (時点 t の 1 行 ns, 熱い行の遠い t ns, デッキ µs, 円柱の何倍か, 回数)。
fn reads(eng: &Engine, rows: &[u64], hot: &[u64], h: &str, card: u64, rng: &mut Rng) -> (f64, f64, f64, f64, usize) {
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
    let far = t.elapsed().as_nanos() as f64 / n as f64;
    let (mut us, mut ratio) = (Vec::new(), Vec::new());
    let start = Instant::now();
    while us.len() < 1000 && start.elapsed() < READ_BUDGET {
        let v = rng.next() % card;
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
    let cnt = us.len();
    (row, far, med(&mut us), med(&mut ratio), cnt)
}

/// 全部の値を見る (最初に見た値の並びを作る)。 書き手 1 本を並べ、 その書き込み 1 回の最長 (止まり) も。
/// 値 0 は 1 つだけで読み (1 値の最初の読み)、 残りはまとめて作る (`history_observe`: 1 回の走査で全部)。
/// (1 値の最初の読み ms, 残り全部をまとめて作る ms, 書き手の最長 ms)
fn observe_all(eng: &Arc<Engine>, rows: &[u64], h: &str, card: u64) -> (f64, f64, f64) {
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
    let clock = eng.history_clock(h).unwrap();
    let a = Instant::now();
    eng.pull_at(h, 0, clock - 1).unwrap();
    let one = a.elapsed().as_secs_f64() * 1000.0;
    let a = Instant::now();
    let rest: Vec<u64> = (1..card).collect();
    eng.history_observe(h, &rest).unwrap();
    let all = a.elapsed().as_secs_f64() * 1000.0;
    std::thread::sleep(Duration::from_millis(20));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let worst = w.join().unwrap();
    (one, all, worst as f64 / 1e6)
}

fn obs_str(o: (f64, f64, f64), card: u64) -> String {
    format!("first view: 1 value={:.0} ms, other {} values together={:.0} ms (writer stalled {:.1} ms)", o.0, card - 1, o.1, o.2)
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/spin_w_cmp3".into());
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
    let only: Option<u64> = std::env::var("CMP3_CARD").ok().map(|s| s.parse().unwrap());
    for (card, u, h) in [(1000u64, "t.u1000", "t.h1000"), (10, "t.u10", "t.h10")] {
        if only.is_some_and(|o| o != card) {
            continue;
        }
        eprintln!("rss {} MB: card={card} start", rss_mb());
        // ---- 見る前 ----
        let mut before = Vec::new();
        for (who, set) in [("random", &rows[..]), ("hot", &hot[..])] {
            // 今の形 / C / E を 4 組、 組ごとに順を回す (各段の前に静める)
            let (mut vb, mut vc, mut ve, mut lags) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for r in 0..4 {
                for k in 0..3 {
                    match (k + r) % 3 {
                        0 => vb.push(extra(&eng, set, u, h, card, &mut rng)),
                        1 => {
                            eng.history_set_lazy(h, true).unwrap();
                            vc.push(extra(&eng, set, u, h, card, &mut rng));
                            eng.history_set_lazy(h, false).unwrap(); // 繋ぐ
                        }
                        _ => {
                            eng.history_set_e(h, true, None).unwrap();
                            std::thread::sleep(Duration::from_millis(20));
                            let _ = eng.history_e_stats(h);
                            ve.push(extra(&eng, set, u, h, card, &mut rng));
                            let (now, max, _) = eng.history_e_stats(h).unwrap();
                            lags.push(format!("{max}/{now}"));
                            eng.history_set_e(h, false, None).unwrap();
                        }
                    }
                }
            }
            before.push(format!(
                "{who}: base={:.0} C={:.0} E={:.0} (E lag max/at end per round {})",
                med(&mut vb),
                med(&mut vc),
                med(&mut ve),
                lags.join(" ")
            ));
        }
        println!("card={card} write_extra_ns before decks: {}", before.join(" | "));
        // ---- 全部見た後 ----
        // 今の形: 全部の値の並びを作る (書き手が保つ)
        eprintln!("rss {} MB: before base observe", rss_mb());
        let base_obs = observe_all(&eng, &rows, h, card);
        eprintln!("rss {} MB: after base observe", rss_mb());
        let mut after = Vec::new();
        let rb = reads(&eng, &rows, &hot, h, card, &mut rng);
        for (who, set) in [("random", &rows[..]), ("hot", &hot[..])] {
            after.push(format!("{who}: base={:.0}", extra(&eng, set, u, h, card, &mut rng)));
        }
        // 今の形の並びを捨てる (案 E の並びと合わせて持つと memory が足りない)
        eng.history_forget_decks(h).unwrap();
        // 案 E: K を変えて
        let mut erows = Vec::new();
        // 値 10 種は束が 10 万なので控えの間隔は K でなく束の大きさで決まる (K を変えても控えは変わらない)。 K=1024 だけ
        let ks: &[usize] = if card <= 10 { &[1024] } else { &[1024, 256, 4096] };
        for &k in ks {
            eng.history_set_e(h, true, Some(k)).unwrap();
            eprintln!("rss {} MB: E K={k} before observe", rss_mb());
            let obs = observe_all(&eng, &rows, h, card);
            eprintln!("rss {} MB: E K={k} after observe (decks {} MB)", rss_mb(), eng.history_e_stats(h).unwrap().2 / 1_000_000);
            if card <= 10 {
                for (v, n, cap, ns, nm) in eng.history_e_deck_detail(h).unwrap() {
                    eprintln!("  value {v}: diffs {n} (cap {cap}), snaps {ns} ({nm} eids)");
                }
                eprintln!("  entries {}", eng.history_stats(h).unwrap().0);
            }
            let _ = eng.history_e_stats(h);
            let ex: Vec<String> = [("random", &rows[..]), ("hot", &hot[..])]
                .iter()
                .map(|(who, set)| format!("{who}={:.0}", extra(&eng, set, u, h, card, &mut rng)))
                .collect();
            let (lag_now, lag_max, mem) = eng.history_e_stats(h).unwrap();
            let r = reads(&eng, &rows, &hot, h, card, &mut rng);
            eprintln!("rss {} MB: E K={k} after reads", rss_mb());
            erows.push(format!(
                "  E K={k}: write_extra_ns {} (lag max {lag_max}, at end {lag_now}), {}, value_at={:.0} ns, far_hot={:.0} ns, deck={:.1} µs ({:.1}x cylinder, {} reads), decks mem={:.0} MB",
                ex.join(" "),
                obs_str(obs, card),
                r.0,
                r.1,
                r.2,
                r.3,
                r.4,
                mem as f64 / 1e6
            ));
            eng.history_set_e(h, false, None).unwrap();
        }
        println!(
            "card={card} after decks: base write_extra_ns {} | base {}, value_at={:.0} ns, far_hot={:.0} ns, deck={:.1} µs ({:.1}x cylinder, {} reads)",
            after.join(" "),
            obs_str(base_obs, card),
            rb.0,
            rb.1,
            rb.2,
            rb.3,
            rb.4
        );
        for r in erows {
            println!("card={card}{r}");
        }
        let (n, obs, eb, hb) = eng.history_stats(h).unwrap();
        println!("card={card} disk: entries={n} observed={obs} entry_bytes={eb} head_bytes={hb}");
        eng.history_forget_decks(h).unwrap();
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
