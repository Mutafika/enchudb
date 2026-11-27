//! スピン 案 W の 2 回目の比べ: 書き込み = 今の形 / 案 A / 案 C、 デッキの読み = 今の形 / 案 B / 案 D。
//!
//! - 書き込みの上乗せ: 宣言の無い同じ形の列との差を 2 万回 × 60 組の中央値で (今の形と案 A は交互、 案 C は別の段で同じ取り方)
//! - 案 C: 見る前の上乗せ、 最初の読みで繋ぐ時間、 その間に書き手が止まる長さ (別の thread の書き込み 1 回の最長)
//! - デッキの読み: 時点 t (地平〜今の一様) の random な値。 1 回の読みにかかった時間の中央値と、 同じ値の今の円柱の読み (`pull_raw`) の何倍か。
//!   1 つの形の読みは合わせて 3 秒まで (越えたら打ち切って回数を出す)
//!
//! `spin_w_cmp2 <dir>`。 行 100 万、 列 u1000 / h1000 (値 0..1000)、 u10 / h10 (値 0..10)。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ROWS: u32 = 1_000_000;
const CAP: usize = 256 * 1024 * 1024;
const N: usize = 20_000;
const PAIRS: usize = 60;
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

fn rewrite(eng: &Engine, rows: &[u64], hid: u16, card: u64, rng: &mut Rng) -> f64 {
    let t = Instant::now();
    for _ in 0..N {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        eng.tie_to_by_id(e, hid, (rng.next() % card) as u32);
    }
    t.elapsed().as_nanos() as f64 / N as f64
}

/// `set(m)` で形を m にしてから書く。 宣言なしの列との差の中央値 (形ごと)。
fn extras(eng: &Engine, rows: &[u64], u: &str, h: &str, card: u64, modes: usize, set: &dyn Fn(usize), rng: &mut Rng) -> Vec<f64> {
    let (uh, hh) = (eng.himo_id(u).unwrap() as u16, eng.himo_id(h).unwrap() as u16);
    let mut d: Vec<Vec<f64>> = vec![Vec::new(); modes];
    for p in 0..PAIRS {
        let base = rewrite(eng, rows, uh, card, rng);
        for k in 0..modes {
            let m = (k + p) % modes;
            set(m);
            d[m].push(rewrite(eng, rows, hh, card, rng) - base);
        }
    }
    d.iter_mut().map(|v| med(v)).collect()
}

/// デッキの読み: (中央値 µs, 円柱の読みの何倍か (中央値), 回数, 平均の束の大きさ)。 合わせて READ_BUDGET まで。
fn decks(eng: &Engine, h: &str, card: u64, h0: u64, span: u64, rng: &mut Rng) -> (f64, f64, usize, usize) {
    let (mut us, mut ratio) = (Vec::new(), Vec::new());
    let mut total = 0;
    let start = Instant::now();
    while us.len() < 1000 && start.elapsed() < READ_BUDGET {
        let v = rng.next() % card;
        let t = h0 + rng.next() % (span + 1);
        let a = Instant::now();
        let got = eng.pull_at(h, v, t).unwrap();
        let x = a.elapsed().as_nanos() as f64;
        let b = Instant::now();
        let now = eng.pull_raw(h, v as u32);
        let y = b.elapsed().as_nanos() as f64;
        std::hint::black_box(&now);
        total += got.len();
        us.push(x / 1000.0);
        ratio.push(x / y.max(1.0));
    }
    let n = us.len();
    (med(&mut us), med(&mut ratio), n, total / n.max(1))
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/spin_w_cmp2".into());
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
    for (card, u, h) in [(1000u64, "t.u1000", "t.h1000"), (10, "t.u10", "t.h10")] {
        let hh = eng.himo_id(h).unwrap() as u16;
        for (who, set_rows) in [("random", &rows[..]), ("hot", &hot[..])] {
            // 今の形 / 案 A (デッキを見る前)
            let set_ab = |m: usize| eng.history_set_modes(h, m == 1, false).unwrap();
            let ab = extras(&eng, set_rows, u, h, card, 2, &set_ab, &mut rng);
            // 案 C (見られるまで繋がない): 同じ取り方で今の形と並べる段 (今の形 = 繋いだ後)。 繋ぐのは段の終わりに 1 回
            eng.history_set_modes(h, false, false).unwrap();
            eng.history_set_lazy(h, true).unwrap();
            let c = extras(&eng, set_rows, u, h, card, 1, &|_| {}, &mut rng);
            // 最初の読みで繋ぐ: 書き手 1 本を並べ、 書き込み 1 回の最長 (止まった長さ)
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let w = {
                let (eng, rows, stop) = (eng.clone(), rows.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut r = Rng(77);
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
            std::thread::sleep(Duration::from_millis(50));
            let link = eng.history_set_lazy(h, false).unwrap();
            std::thread::sleep(Duration::from_millis(50));
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let worst = w.join().unwrap();
            let (n, _, _, _) = eng.history_stats(h).unwrap();
            println!(
                "card={card} rows={who} write_extra_ns: base={:.0} A={:.0} C={:.0} | C link={:.0} ms for {n} entries, writer stalled {:.0} ms",
                ab[0],
                ab[1],
                c[0],
                link.map_or(0.0, |d| d.as_secs_f64() * 1000.0),
                worst as f64 / 1e6
            );
        }
        // デッキの読み (今の形 / 案 B / 案 D)
        eng.history_set_modes(h, false, false).unwrap();
        let clock = eng.history_clock(h).unwrap();
        let h0 = eng.history(rows[1000], h).unwrap()[0].time;
        let span = clock - h0;
        for (name, blocks, snaps) in [("base", false, false), ("B", true, false), ("D", false, true)] {
            eng.history_set_modes(h, false, blocks).unwrap();
            eng.history_set_snaps(h, snaps).unwrap();
            // 最初の 1 回 (並び / 控えを作る)。 今の形は値ごとに並びを作るので、 全部の値を温める (合わせた時間も出す)
            let a = Instant::now();
            let _ = eng.pull_at(h, 0, h0 + span / 2).unwrap();
            let first_ms = a.elapsed().as_secs_f64() * 1000.0;
            let mut warm = String::new();
            if !blocks && !snaps {
                let a = Instant::now();
                for v in 1..card {
                    let _ = eng.pull_at(h, v, h0 + span / 2).unwrap();
                }
                warm = format!(", warm all values={:.0} ms", a.elapsed().as_secs_f64() * 1000.0);
            }
            let (us, ratio, cnt, len) = decks(&eng, h, card, h0, span, &mut rng);
            let (ns, nb) = eng.history_snaps_bytes(h).unwrap();
            println!(
                "card={card} deck {name}: first={first_ms:.0} ms{warm}, median={us:.1} µs ({ratio:.1}x of the cylinder read), reads={cnt}, avg_deck={len}{}",
                if snaps { format!(", snaps={ns} ({:.0} MB)", nb as f64 / 1e6) } else { String::new() }
            );
            eng.history_set_snaps(h, false).unwrap();
        }
        eng.history_set_modes(h, false, false).unwrap();
        let (n, obs, eb, hb) = eng.history_stats(h).unwrap();
        println!("card={card} disk: entries={n} observed={obs} entry_bytes={eb} head_bytes={hb}");
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
