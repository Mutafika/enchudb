//! 球 (proto/ball-history) の書き込みの値段と、 時点を指した読みの速さを測る (2026-10-26)。
//! 書き手の段は phase22/acb.rs の `w` と同じ形 (表 t: id = 主キー、 a / b = Number 0..99、 w = BigInt、 engine の
//! `tie_to_by_id` で書き手 <threads> 本、 Barrier でそろえて全員が終わるまでの ns / 1 回)。 feature `ball` の版は
//! BALL=1 で a / b / w を球にする (表を作った直後、 行の無い時)。
//!
//! usage: ballb w <threads> <seed>    (DB は BALL_DIR の下、 終わったら消す。 BALL_TOTAL 既定 96 万、 BALL_CYCLES 既定 3)
//!        ballb r <seed>              (feature ball のみ。 読みの段)
//!
//! r: 表 t (id、 a = Number 0..99) に BALL_TOTAL 行、 a を球にしてから、 1 回につき 10% の行の a を書き換えるのを 10 回。
//!    回の終わりの時刻を控え、 5 回目の終わり (t5) と今について:
//!    - pull_now   : pull_raw("t.a", v) の 1 本の ns (v = 0..99 を回す)
//!    - pull_at_t5 : ball_pull_at("t.a", v, t5)
//!    - pull_at_now: ball_pull_at("t.a", v, 今)
//!    - get_now    : get(e, "t.a")、 value_at_t5 : ball_value_at(e, "t.a", t5) (e は乱数)
//!    答え合わせ: 全部の v で ball_pull_at(v, 今) = pull_raw(v) (並べて)、 t5 の答えは手元の写し (5 回目の後の a) と比べる。
//!    面の数と取った byte、 RSS の峰も出す。
//! 出力: 1 行 1 JSON (stdout)。

use enchudb::schema::Database;
use enchudb::Engine;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::time::Instant;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn db_path(tag: &str) -> String {
    let dir = std::env::var("BALL_DIR").expect("BALL_DIR");
    format!("{dir}/ballb-{tag}-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
        for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp", ".schema"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0));
        }
    }
}

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn ball_on() -> bool {
    cfg!(feature = "ball") && std::env::var("BALL").is_ok_and(|v| v == "1")
}

#[allow(unused_variables)]
fn declare(eng: &Engine, himos: &[&str]) {
    #[cfg(feature = "ball")]
    for h in himos {
        assert!(eng.declare_ball(h).unwrap());
    }
}

fn build_tag() -> &'static str {
    if cfg!(feature = "ball") { "proto" } else { "base" }
}

fn rss_peak_kb() -> u64 {
    // getrusage の ru_maxrss は macOS では byte
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let v = ru.ru_maxrss as u64;
    if cfg!(target_os = "macos") { v / 1024 } else { v }
}

fn mode_w(threads: usize, seed: u64) {
    let path = db_path("w");
    let _c = Cleanup(path.clone());
    let total = env_usize("BALL_TOTAL", 960_000);
    let cycles = env_usize("BALL_CYCLES", 3);
    let per = total / threads;
    let ball = ball_on();
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").number("b").bigint("w").primary_key("id").with_capacity(total as u32 + 1000).build().unwrap();
    let t = db.get_table("t").unwrap();
    let eng: Arc<Engine> = db.arc_engine();
    let h = |c: &str| eng.himo_id(c).unwrap() as u16;
    let (hid, ha, hb, hw) = (h("t.id"), h("t.a"), h("t.b"), h("t.w"));
    // 球にする (宣言が円柱を組む。 組んだ後の紐は宣言できないので索引を作る前に)。 BALL_COLS: 球にする列 (既定は 3 本とも)
    if ball {
        let cols = std::env::var("BALL_COLS").unwrap_or_else(|_| "t.a,t.b,t.w".into());
        declare(&eng, &cols.split(',').collect::<Vec<_>>());
    }
    // 索引を作る (組んだ後の書き手 = 値の鍵の道、 acb の prebuild=1)
    let _ = t.where_eq("a", 1i64).count().unwrap();
    let _ = t.where_eq("b", 1i64).count().unwrap();
    let _ = eng.count_value_by_id(hw, 1);
    let tag = build_tag();
    let bl = u8::from(ball);
    let run = |stage: &str, cycle: usize, ops_per_thread: usize, f: &(dyn Fn(usize) + Sync)| {
        let start = Barrier::new(threads + 1);
        let t0 = std::thread::scope(|s| {
            for th in 0..threads {
                let start = &start;
                s.spawn(move || {
                    start.wait();
                    f(th);
                });
            }
            start.wait();
            Instant::now()
        });
        let ns = t0.elapsed().as_nanos() as f64;
        println!(
            "{{\"mode\":\"w\",\"build\":\"{tag}\",\"ball\":{bl},\"kind\":\"{stage}\",\"threads\":{threads},\"seed\":{seed},\"cycle\":{cycle},\"total\":{total},\"ns\":{:.1}}}",
            ns / (ops_per_thread * threads) as f64 * threads as f64
        );
    };
    let mine: Vec<std::sync::Mutex<Vec<u64>>> = (0..threads).map(|_| std::sync::Mutex::new(Vec::with_capacity(per))).collect();
    let val = |th: usize, i: usize, salt: u64| -> u64 {
        let mut r = Rng((seed << 20) ^ ((th as u64) << 40) ^ (i as u64) ^ salt.wrapping_mul(0x9E37_79B9) | 1);
        r.next()
    };
    run("w_ins", 0, per * 4, &|th| {
        let mut v = mine[th].lock().unwrap();
        for i in 0..per {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, hid, (th * per + i) as u32);
            eng.tie_to_by_id(e, ha, (val(th, i, 1) % 100) as u32);
            eng.tie_to_by_id(e, hb, (val(th, i, 2) % 100) as u32);
            eng.tie_to_by_id(e, hw, val(th, i, 3) % (1 << 40));
            v.push(e);
        }
    });
    let check = |stage: &str, with_b: usize| {
        assert_eq!(t.all().count().unwrap(), total, "{stage} all");
        assert_eq!(t.where_range("b", 0, 99).count().unwrap(), with_b, "{stage} b のある row");
    };
    check("w_ins", total);
    for c in 1..=cycles {
        run("w_upd", c, per * 3, &|th| {
            let v = mine[th].lock().unwrap();
            for (i, &e) in v.iter().enumerate() {
                eng.tie_to_by_id(e, ha, (val(th, i, 11 + 10 * c as u64) % 100) as u32);
                eng.tie_to_by_id(e, hb, (val(th, i, 12 + 10 * c as u64) % 100) as u32);
                eng.tie_to_by_id(e, hw, val(th, i, 13 + 10 * c as u64) % (1 << 40));
            }
        });
        check("w_upd", total);
        run("w_untie", c, per, &|th| {
            let v = mine[th].lock().unwrap();
            for &e in v.iter() {
                eng.untie_by_id(e, hb);
            }
        });
        check("w_untie", 0);
        run("w_retie", c, per, &|th| {
            let v = mine[th].lock().unwrap();
            for (i, &e) in v.iter().enumerate() {
                eng.tie_to_by_id(e, hb, (val(th, i, 21 + c as u64) % 100) as u32);
            }
        });
        check("w_retie", total);
    }
    #[cfg(feature = "ball")]
    let faces: u64 = ["t.a", "t.b", "t.w"].iter().filter_map(|h| eng.ball_stats(h)).map(|s| s.0).sum();
    #[cfg(not(feature = "ball"))]
    let faces = 0u64;
    #[cfg(feature = "ball")]
    let bytes: usize = ["t.a", "t.b", "t.w"].iter().filter_map(|h| eng.ball_stats(h)).map(|s| s.1).sum();
    #[cfg(not(feature = "ball"))]
    let bytes = 0usize;
    #[cfg(feature = "ball")]
    let diffs: u64 = ["t.a", "t.b", "t.w"].iter().filter_map(|h| eng.ball_diff_count(h)).sum();
    #[cfg(not(feature = "ball"))]
    let diffs = 0u64;
    println!(
        "{{\"mode\":\"w\",\"build\":\"{tag}\",\"ball\":{bl},\"kind\":\"ok\",\"threads\":{threads},\"seed\":{seed},\"faces\":{faces},\"ball_bytes\":{bytes},\"diffs\":{diffs},\"rss_peak_kb\":{}}}",
        rss_peak_kb()
    );
}

#[cfg(feature = "ball")]
fn mode_r(seed: u64) {
    let path = db_path("r");
    let _c = Cleanup(path.clone());
    let total = env_usize("BALL_TOTAL", 960_000);
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").with_capacity(total as u32 + 1000).build().unwrap();
    let t = db.get_table("t").unwrap();
    let eng: Arc<Engine> = db.arc_engine();
    let (hid, ha) = (eng.himo_id("t.id").unwrap() as u16, eng.himo_id("t.a").unwrap() as u16);
    // a の円柱は組まない (球の宣言が、 入れた後の列から組む)
    let mut r = Rng(seed | 1);
    let mut cur: Vec<u32> = Vec::with_capacity(total);
    let mut eids = Vec::with_capacity(total);
    for i in 0..total {
        let e = eng.entity_in("t").unwrap();
        let a = (r.next() % 100) as u32;
        eng.tie_to_by_id(e, hid, i as u32);
        eng.tie_to_by_id(e, ha, a);
        cur.push(a);
        eids.push(e);
    }
    eng.declare_ball("t.a").unwrap();
    let mut t5 = 0;
    let mut at5 = Vec::new();
    for round in 1..=10 {
        for _ in 0..total / 10 {
            let i = (r.next() % total as u64) as usize;
            let a = (r.next() % 100) as u32;
            eng.tie_to_by_id(eids[i], ha, a);
            cur[i] = a;
        }
        if round == 5 {
            t5 = eng.ball_now();
            at5 = cur.clone();
        }
    }
    let now = eng.ball_now();
    // 答え合わせ
    for v in 0..100u64 {
        let mut p = eng.pull_raw("t.a", v as u32);
        p.sort_unstable();
        assert_eq!(eng.ball_pull_at("t.a", v, now), p, "今の v={v}");
        let want: Vec<u64> = (0..total).filter(|&i| at5[i] as u64 == v).map(|i| eids[i]).collect();
        assert_eq!(eng.ball_pull_at("t.a", v, t5), want, "t5 の v={v}");
    }
    let time = |name: &str, n: usize, f: &dyn Fn(usize) -> usize| {
        let mut xs: Vec<f64> = (0..5)
            .map(|_| {
                let t0 = Instant::now();
                let mut acc = 0usize;
                for k in 0..n {
                    acc += f(k);
                }
                black_box(acc);
                t0.elapsed().as_nanos() as f64 / n as f64
            })
            .collect();
        xs.sort_by(f64::total_cmp);
        println!("{{\"mode\":\"r\",\"kind\":\"{name}\",\"seed\":{seed},\"total\":{total},\"ns\":{:.1}}}", xs[2]);
    };
    time("pull_now", 200, &|k| eng.pull_raw("t.a", (k % 100) as u32).len());
    time("pull_at_now", 200, &|k| eng.ball_pull_at("t.a", (k % 100) as u64, now).len());
    time("pull_at_t5", 200, &|k| eng.ball_pull_at("t.a", (k % 100) as u64, t5).len());
    let picks: Vec<u64> = (0..100_000).map(|k| eids[(Rng(k as u64 * 0x9E37 + seed | 1).next() % total as u64) as usize]).collect();
    time("get_now", 100_000, &|k| eng.get(picks[k], "t.a").unwrap_or(0) as usize);
    time("value_at_t5", 100_000, &|k| eng.ball_value_at(picks[k], "t.a", t5).unwrap_or(0) as usize);
    let (faces, bytes) = eng.ball_stats("t.a").unwrap();
    println!(
        "{{\"mode\":\"r\",\"kind\":\"ok\",\"seed\":{seed},\"total\":{total},\"faces\":{faces},\"ball_bytes\":{bytes},\"diffs\":{},\"rss_peak_kb\":{}}}",
        eng.ball_diff_count("t.a").unwrap(),
        rss_peak_kb()
    );
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "w" => mode_w(a[2].parse().unwrap(), a[3].parse().unwrap()),
        #[cfg(feature = "ball")]
        "r" => mode_r(a[2].parse().unwrap()),
        m => panic!("unknown mode {m}"),
    }
}
