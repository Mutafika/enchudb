//! スピン 案 W の試作の測り (`scripts/spin-w/bench.sh` が v0.38.0 と交互に回す)。
//!
//! `spin_w_bench base <dir>`: 宣言の無い DB の書き込み (書き換え) / 読み / 開く時間 (v0.38.0 でも build できる API だけ)
//! `spin_w_bench hist <dir>`: (`--features spin-w-bench` の build だけ) 宣言した列の書き換えの上乗せ・時点 t の 1 行・時点 t のデッキ・
//! 1 回の書き込みあたりの disk・宣言のある DB の開く時間
//!
//! 行 100 万、 列 v は値 0..1000 (= 値ごとに約 1000 行)。 書き手 1 本。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::Instant;

const ROWS: u32 = 1_000_000;
const VALUES: u64 = 1000;
const CAP: usize = 256 * 1024 * 1024;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn build(dir: &str, extra_col: bool) -> (Arc<Engine>, Vec<u64>) {
    let _ = std::fs::remove_dir_all(dir);
    let mut eng = Engine::create_with_capacity(dir, ROWS + 1024).unwrap();
    eng.define_table("t", ROWS + 16).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, VALUES as u32).unwrap();
    if extra_col {
        eng.define_himo_in("t", "h", ValueType::Number, VALUES as u32).unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    let rows: Vec<u64> = (0..ROWS).map(|_| eng.entity_in("t").unwrap()).collect();
    (eng, rows)
}

fn fill(eng: &Engine, rows: &[u64], col: &str) {
    let hid = eng.himo_id(col).unwrap() as u16;
    for (i, &e) in rows.iter().enumerate() {
        eng.tie_to_by_id(e, hid, (i as u64 % VALUES) as u32);
    }
    eng.oplog_sync().unwrap();
}

/// 書き換え n 回 (random な行に random な値): ns / 回。
fn rewrite(eng: &Engine, rows: &[u64], col: &str, n: usize, rng: &mut Rng) -> f64 {
    let hid = eng.himo_id(col).unwrap() as u16;
    let t = Instant::now();
    for _ in 0..n {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        eng.tie_to_by_id(e, hid, (rng.next() % VALUES) as u32);
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

/// 読み n 回: ns / 回。
fn read(eng: &Engine, rows: &[u64], col: &str, n: usize, rng: &mut Rng) -> f64 {
    let hid = eng.himo_id(col).unwrap() as u16;
    let mut sum = 0u64;
    let t = Instant::now();
    for _ in 0..n {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        sum = sum.wrapping_add(eng.get_by_id(e, hid).unwrap_or(0));
    }
    std::hint::black_box(sum);
    t.elapsed().as_nanos() as f64 / n as f64
}

/// 円柱の読み (値 1 つの行の束) n 回: µs / 回。
fn pull(eng: &Engine, col: &str, n: usize, rng: &mut Rng) -> f64 {
    let mut sum = 0usize;
    let t = Instant::now();
    for _ in 0..n {
        sum += eng.pull_raw(col, (rng.next() % VALUES) as u32).len();
    }
    std::hint::black_box(sum);
    t.elapsed().as_nanos() as f64 / n as f64 / 1000.0
}

fn open_ms(dir: &str) -> f64 {
    let t = Instant::now();
    let eng = Engine::open_concurrent_with_oplog(dir, CAP).unwrap();
    let ms = t.elapsed().as_secs_f64() * 1000.0;
    drop(eng);
    ms
}

fn base(dir: &str) {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let (eng, rows) = build(dir, false);
    fill(&eng, &rows, "t.v");
    // 温め
    rewrite(&eng, &rows, "t.v", 200_000, &mut rng);
    let w = rewrite(&eng, &rows, "t.v", 1_000_000, &mut rng);
    eng.oplog_sync().unwrap();
    let r = read(&eng, &rows, "t.v", 2_000_000, &mut rng);
    let p = pull(&eng, "t.v", 2000, &mut rng);
    drop(eng);
    let o = open_ms(dir);
    println!("base write_ns={w:.1} read_ns={r:.1} pull_us={p:.2} open_ms={o:.1}");
}

#[cfg(feature = "spin-w-bench")]
fn hist(dir: &str) {
    use enchudb_engine::history::pack;
    let _ = pack;
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    // 列 v (宣言なし) と h (宣言あり) を同じ表に。 同じ build で書き換えの差を取る
    let (eng, rows) = build(dir, true);
    fill(&eng, &rows, "t.v");
    fill(&eng, &rows, "t.h");
    let td = Instant::now();
    eng.declare_history("t.h").unwrap();
    let declare_ms = td.elapsed().as_secs_f64() * 1000.0;
    let (n0, _, eb0, _) = eng.history_stats("t.h").unwrap();
    // 書き換えの上乗せ: 宣言なし / 宣言ありを 2 万回ずつ交互に 60 組。 組ごとの差の中央値 (書き出しの周期の揺れを組で消す)
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    rewrite(&eng, &rows, "t.v", 100_000, &mut rng);
    rewrite(&eng, &rows, "t.h", 100_000, &mut rng);
    let pairs = |eng: &Engine, rng: &mut Rng| {
        let (mut wv, mut wh, mut d) = (Vec::new(), Vec::new(), Vec::new());
        for i in 0..60 {
            let (a, b) = if i % 2 == 0 {
                let a = rewrite(eng, &rows, "t.v", 20_000, rng);
                (a, rewrite(eng, &rows, "t.h", 20_000, rng))
            } else {
                let b = rewrite(eng, &rows, "t.h", 20_000, rng);
                (rewrite(eng, &rows, "t.v", 20_000, rng), b)
            };
            wv.push(a);
            wh.push(b);
            d.push(b - a);
        }
        (med(&mut wv), med(&mut wh), med(&mut d))
    };
    let (mv, mh, extra) = pairs(&eng, &mut rng);
    // 書き換えの多い行 (100 行 × 1000 回) を作る: 遠い t の 1 行
    let hot: Vec<u64> = rows[..100].to_vec();
    let hid = eng.himo_id("t.h").unwrap() as u16;
    for _ in 0..1000 {
        for &e in &hot {
            eng.tie_to_by_id(e, hid, (rng.next() % VALUES) as u32);
        }
    }
    eng.oplog_sync().unwrap();
    let (n1, obs, eb1, hb1) = eng.history_stats("t.h").unwrap();
    let horizon = eng.value_at(rows[0], "t.h", u64::MAX).map(|_| ()).ok();
    let _ = horizon;
    let clock = eng.history_clock("t.h").unwrap();
    // 地平は最初の行の最初の entry の時刻
    let h0 = eng.history(rows[0], "t.h").unwrap()[0].time;
    let span = clock - h0;
    // 時点 t の 1 行: random な行 / 書き換えの多い行、 t は地平〜今の一様
    let n = 200_000;
    let t = Instant::now();
    let mut sum = 0u64;
    for _ in 0..n {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        let tt = h0 + rng.next() % (span + 1);
        sum = sum.wrapping_add(eng.value_at(e, "t.h", tt).unwrap().value.unwrap_or(0));
    }
    let row_ns = t.elapsed().as_nanos() as f64 / n as f64;
    let t = Instant::now();
    for _ in 0..n {
        let e = hot[(rng.next() % hot.len() as u64) as usize];
        let tt = h0 + rng.next() % (span + 1);
        sum = sum.wrapping_add(eng.value_at(e, "t.h", tt).unwrap().value.unwrap_or(0));
    }
    let hot_ns = t.elapsed().as_nanos() as f64 / n as f64;
    // 遠い t (地平の直後) の書き換えの多い行
    let t = Instant::now();
    for _ in 0..n {
        let e = hot[(rng.next() % hot.len() as u64) as usize];
        sum = sum.wrapping_add(eng.value_at(e, "t.h", h0 + 1).unwrap().value.unwrap_or(0));
    }
    let far_ns = t.elapsed().as_nanos() as f64 / n as f64;
    std::hint::black_box(sum);
    // 時点 t のデッキ: 最初に見た時 (並びを作る) と、 その後 (random な t)
    let tb = Instant::now();
    let first = eng.pull_at("t.h", 7, clock).unwrap().len();
    let build_ms = tb.elapsed().as_secs_f64() * 1000.0;
    for v in 0..VALUES {
        eng.pull_at("t.h", v, h0 + 1).unwrap(); // 過去の時点 (今の時点は円柱を読むので並びを作らない)
    }
    let m = 2000;
    let t = Instant::now();
    let mut total = 0usize;
    for _ in 0..m {
        let v = rng.next() % VALUES;
        let tt = h0 + rng.next() % (span + 1);
        total += eng.pull_at("t.h", v, tt).unwrap().len();
    }
    let deck_us = t.elapsed().as_nanos() as f64 / m as f64 / 1000.0;
    let t = Instant::now();
    for _ in 0..m {
        let v = rng.next() % VALUES;
        total += eng.pull_at("t.h", v, clock).unwrap().len();
    }
    let deck_now_us = t.elapsed().as_nanos() as f64 / m as f64 / 1000.0;
    // 書き手が保つ: デッキを見た後の書き換えの上乗せ
    let (_, _, after_obs) = pairs(&eng, &mut rng);
    let per_write = (eb1 - eb0) as f64 / (n1 - n0) as f64;
    drop(eng);
    let o = open_ms(dir);
    println!(
        "hist declare_ms={declare_ms:.0} write_undeclared_ns={mv:.1} write_declared_ns={mh:.1} extra_ns={extra:.1} extra_after_decks_ns={after_obs:.1} \
         value_at_ns={row_ns:.0} value_at_hot_ns={hot_ns:.0} value_at_far_hot_ns={far_ns:.0} \
         deck_build_ms={build_ms:.1} deck_first_len={first} deck_us={deck_us:.1} deck_now_us={deck_now_us:.1} avg_deck={} \
         entries={n1} observed={obs} entry_bytes={eb1} head_bytes={hb1} bytes_per_write={per_write:.1} open_ms={o:.1}",
        total / (2 * m)
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(2).cloned().unwrap_or_else(|| "/tmp/spin_w_bench".into());
    match args.get(1).map(String::as_str) {
        Some("base") => base(&dir),
        #[cfg(feature = "spin-w-bench")]
        Some("hist") => hist(&dir),
        _ => eprintln!("usage: spin_w_bench base|hist <dir>"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
