//! スピン 案 W: 書き込みの上乗せの 2 案 (A = 溜めてまとめて繋ぐ、 B = デッキを区間ごとの控えで読む) を今の形と比べる。
//! 同じ DB の中で形を切り替え (`Engine::history_set_modes`)、 宣言の無い同じ形の列との差を 2 万回 × 60 組の中央値で取る。
//!
//! `spin_w_cmp <dir>` (`--features spin-w-bench` は要らない: 公開の API だけ)
//!
//! 行 100 万。 列: u1000 / h1000 (値 0..1000、 h だけ履歴)、 u10 / h10 (値 0..10)。 書き手 1 本。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::Instant;

const ROWS: u32 = 1_000_000;
const CAP: usize = 256 * 1024 * 1024;
const N: usize = 20_000;
const PAIRS: usize = 60;

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
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// 書き換え n 回: ns / 回。 `rows` から random に、 値は 0..card。
fn rewrite(eng: &Engine, rows: &[u64], hid: u16, card: u64, rng: &mut Rng) -> f64 {
    let t = Instant::now();
    for _ in 0..N {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        eng.tie_to_by_id(e, hid, (rng.next() % card) as u32);
    }
    t.elapsed().as_nanos() as f64 / N as f64
}

/// 形 (batch, blocks) ごとの上乗せ: 宣言なしの列 `u` との差の中央値。 組の中の順は回す。
fn extras(eng: &Engine, rows: &[u64], u: &str, h: &str, card: u64, modes: &[(bool, bool)], rng: &mut Rng) -> Vec<f64> {
    let (uh, hh) = (eng.himo_id(u).unwrap() as u16, eng.himo_id(h).unwrap() as u16);
    let mut d: Vec<Vec<f64>> = vec![Vec::new(); modes.len()];
    for p in 0..PAIRS {
        let base = rewrite(eng, rows, uh, card, rng);
        for k in 0..modes.len() {
            let m = (k + p) % modes.len();
            eng.history_set_modes(h, modes[m].0, modes[m].1).unwrap();
            let x = rewrite(eng, rows, hh, card, rng);
            d[m].push(x - base);
        }
    }
    d.iter_mut().map(|v| med(v)).collect()
}

/// 読み: 時点 t の 1 行 (random な行 / 熱い行の遠い t) と、 時点 t のデッキ (random な値と t)。 (ns, ns, µs, 平均の束の大きさ)
fn reads(eng: &Engine, rows: &[u64], hot: &[u64], h: &str, card: u64, rng: &mut Rng) -> (f64, f64, f64, usize) {
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
    // 温め (値ごとの並び / 区間の控えを作る)
    for v in 0..card {
        eng.pull_at(h, v, h0 + 1).unwrap();
    }
    let m = if card > 100 { 1000 } else { 100 };
    let mut total = 0;
    let t = Instant::now();
    for _ in 0..m {
        total += eng.pull_at(h, rng.next() % card, h0 + rng.next() % (span + 1)).unwrap().len();
    }
    (row, far, t.elapsed().as_nanos() as f64 / m as f64 / 1000.0, total / m)
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/spin_w_cmp".into());
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
    let modes = [(false, false), (true, false)];
    let modes_b = [(false, true), (true, true)];
    for (card, u, h) in [(1000u64, "t.u1000", "t.h1000"), (10, "t.u10", "t.h10")] {
        for (who, set) in [("random", &rows[..]), ("hot", &hot[..])] {
            // デッキを見る前: 今の形 / 案 A
            let before = extras(&eng, set, u, h, card, &modes, &mut rng);
            // 全部の値のデッキを見た後 (今の形の値ごとの並び): 今の形 / 案 A
            eng.history_set_modes(h, false, false).unwrap();
            let clock = eng.history_clock(h).unwrap();
            for v in 0..card {
                eng.pull_at(h, v, clock - 1).unwrap();
            }
            let after = extras(&eng, set, u, h, card, &modes, &mut rng);
            // 案 B (書き手は並びを保たない): B / A+B
            let after_b = extras(&eng, set, u, h, card, &modes_b, &mut rng);
            println!(
                "card={card} rows={who} before: base={:.0} A={:.0} | after_decks: base={:.0} A={:.0} B={:.0} AB={:.0}",
                before[0], before[1], after[0], after[1], after_b[0], after_b[1]
            );
        }
        // 読み (熱い行は書き換えの多い行になっている)
        for (name, m) in [("base", (false, false)), ("A", (true, false)), ("B", (false, true)), ("AB", (true, true))] {
            eng.history_set_modes(h, m.0, m.1).unwrap();
            let (row, far, deck, len) = reads(&eng, &rows, &hot, h, card, &mut rng);
            println!("card={card} reads {name}: value_at_ns={row:.0} value_at_far_hot_ns={far:.0} deck_us={deck:.1} avg_deck={len}");
        }
        let (n, obs, eb, hb) = eng.history_stats(h).unwrap();
        println!("card={card} disk: entries={n} observed={obs} entry_bytes={eb} head_bytes={hb} bytes_per_entry={:.1}", eb as f64 / n as f64);
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
