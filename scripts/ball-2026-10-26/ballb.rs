//! 球 (proto/ball-history) の書き込みの値段と、 時点を指した読みの速さを測る (2026-10-26)。
//! 書き手の段は phase22/acb.rs の `w` と同じ形 (表 t: id = 主キー、 a / b = Number 0..99、 w = BigInt、 engine の
//! `tie_to_by_id` で書き手 <threads> 本、 Barrier でそろえて全員が終わるまでの ns / 1 回)。 feature `ball` の版は
//! BALL=1 で a / b / w を球にする (表を作った直後、 行の無い時)。
//!
//! usage: ballb w <threads> <seed>    (DB は BALL_DIR の下、 終わったら消す。 BALL_TOTAL 既定 96 万、 BALL_CYCLES 既定 3、
//!                                     BALL_WAL=1 で WAL 付き (BALL_WAL_CAP 既定 2 GiB)、 9 回目で足した)
//!        ballb r <seed>              (feature ball のみ。 読みの段)
//!        ballb h <seed>              (feature ball のみ。 熱い行の読み、 10 回目)
//!        ballb rw <seed>             (feature ball のみ。 大きい値の列の時点の読み、 16 回目)
//!
//! r: 表 t (id、 a = Number 0..99) に BALL_TOTAL 行、 a を球にしてから、 1 回につき 10% の行の a を書き換えるのを BALL_ROUNDS 回
//!    (既定 10、 7 回目で足した)。 回の終わりの時刻を控え、 1 回目の終わり (t1)、 5 回目の終わり (t5) と今について:
//!    - pull_now   : pull_raw("t.a", v) の 1 本の ns (v = 0..99 を回す)
//!    - pull_at_t5 : ball_pull_at("t.a", v, t5)
//!    - pull_at_now: ball_pull_at("t.a", v, 今)
//!    - get_now    : get(e, "t.a")、 value_at_t5 : ball_value_at(e, "t.a", t5) (e は乱数)
//!    - pull_at_t1 : ball_pull_at("t.a", v, t1) (最後に測る、 7 回目で足した)
//!    答え合わせ: 全部の v で ball_pull_at(v, 今) = pull_raw(v) (並べて)、 t5 / t1 の答えは手元の写し (5 回目 / 1 回目の後の a) と比べる。
//!    面の数と取った byte、 RSS の峰も出す。
//! h: 熱い行 (10 回目で足した)。 表 t に BALL_TOTAL 行 (a = 0..99) を入れて a を球にした後、 そのうち BALL_HOT 行 (既定 1000) だけを
//!    書き換える (1 行あたり BALL_HOT_WRITES 回、 既定 2000。 行は毎回乱数で選ぶので 1 行の面は散らばる。 1 割は外す)。 書くたびに時刻を控え:
//!    - value_at_hot_all   : 熱い行の、 履歴全体から一様に選んだ t
//!    - value_at_hot_recent: 熱い行の、 最後の 1% の書き込みの間の t
//!    - value_at_cold      : 書き換えていない行 (面は前からの 1 つ) の一様な t
//!    答え合わせ: 測る組 (行, t) を全部、 手元の (時刻, 値) の列の二分探索と比べる。
//! rw: 大きい値 (16 回目で足した)。 表 t (id、 w = BigInt) に BALL_TOTAL 行 (w = 2^20〜2^40 の乱数 = ほぼ 1 行に 1 値) を入れて w を球に
//!    した後、 1 回につき 10% の行を新しい乱数に書き換えるのを BALL_ROUNDS 回。 乱数の行の、 その時の値で引く:
//!    pull_now (pull_raw)、 pull_at_now / pull_at_t5 / pull_at_t1 (ball_pull_at)。 答え合わせ: 200 行 × 3 時点を手元の写しと比べる。
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
    // BALL_WAL=1: WAL (oplog) 付きの concurrent に移してから書く (9 回目で足した。 球の時刻を WAL の HLC にした版を測るため)
    let wal = std::env::var("BALL_WAL").is_ok_and(|v| v == "1");
    let db: Arc<Database> = {
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").number("a").number("b").bigint("w").primary_key("id").with_capacity(total as u32 + 1000).build().unwrap();
        if wal { db.finish_with_oplog(env_usize("BALL_WAL_CAP", 2 << 30)).unwrap() } else { Arc::new(db) }
    };
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
            "{{\"mode\":\"w\",\"build\":\"{tag}\",\"ball\":{bl},\"wal\":{wal},\"kind\":\"{stage}\",\"threads\":{threads},\"seed\":{seed},\"cycle\":{cycle},\"total\":{total},\"ns\":{:.1}}}",
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
        "{{\"mode\":\"w\",\"build\":\"{tag}\",\"ball\":{bl},\"wal\":{wal},\"kind\":\"ok\",\"threads\":{threads},\"seed\":{seed},\"faces\":{faces},\"ball_bytes\":{bytes},\"diffs\":{diffs},\"rss_peak_kb\":{}}}",
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
    let rounds = env_usize("BALL_ROUNDS", 10);
    let (mut t1, mut t5) = (0, 0);
    let (mut at1, mut at5) = (Vec::new(), Vec::new());
    for round in 1..=rounds {
        for _ in 0..total / 10 {
            let i = (r.next() % total as u64) as usize;
            let a = (r.next() % 100) as u32;
            eng.tie_to_by_id(eids[i], ha, a);
            cur[i] = a;
        }
        if round == 1 {
            t1 = eng.ball_now();
            at1 = cur.clone();
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
        assert_eq!(eng.ball_pull_at("t.a", v, now).unwrap(), p, "今の v={v}");
        let want: Vec<u64> = (0..total).filter(|&i| at5[i] as u64 == v).map(|i| eids[i]).collect();
        assert_eq!(eng.ball_pull_at("t.a", v, t5).unwrap(), want, "t5 の v={v}");
        let want: Vec<u64> = (0..total).filter(|&i| at1[i] as u64 == v).map(|i| eids[i]).collect();
        assert_eq!(eng.ball_pull_at("t.a", v, t1).unwrap(), want, "t1 の v={v}");
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
    time("pull_at_now", 200, &|k| eng.ball_pull_at("t.a", (k % 100) as u64, now).unwrap().len());
    time("pull_at_t5", 200, &|k| eng.ball_pull_at("t.a", (k % 100) as u64, t5).unwrap().len());
    let picks: Vec<u64> = (0..100_000).map(|k| eids[(Rng(k as u64 * 0x9E37 + seed | 1).next() % total as u64) as usize]).collect();
    time("get_now", 100_000, &|k| eng.get(picks[k], "t.a").unwrap_or(0) as usize);
    time("value_at_t5", 100_000, &|k| eng.ball_value_at(picks[k], "t.a", t5).unwrap().unwrap_or(0) as usize);
    time("pull_at_t1", 200, &|k| eng.ball_pull_at("t.a", (k % 100) as u64, t1).unwrap().len());
    let (faces, bytes) = eng.ball_stats("t.a").unwrap();
    println!(
        "{{\"mode\":\"r\",\"kind\":\"ok\",\"seed\":{seed},\"total\":{total},\"rounds\":{rounds},\"faces\":{faces},\"ball_bytes\":{bytes},\"diffs\":{},\"rss_peak_kb\":{}}}",
        eng.ball_diff_count("t.a").unwrap(),
        rss_peak_kb()
    );
}

/// 大きい値 (2^20 以上、 ID / 時刻の列の形: 値はほぼ 1 行に 1 つ) の列の時点の読み (16 回目、 大きい値の差分を値の順の run にした時)。
/// 入れた後に球にして、 1 割ずつ新しい大きい値に書き換える回を `BALL_ROUNDS` 回。 昔の値で時点 t を引く (答えは普通 1 行)。
#[cfg(feature = "ball")]
fn mode_rw(seed: u64) {
    let path = db_path("rw");
    let _c = Cleanup(path.clone());
    let total = env_usize("BALL_TOTAL", 960_000);
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").bigint("w").primary_key("id").with_capacity(total as u32 + 1000).build().unwrap();
    let eng: Arc<Engine> = db.arc_engine();
    let (hid, hw) = (eng.himo_id("t.id").unwrap() as u16, eng.himo_id("t.w").unwrap() as u16);
    let mut r = Rng(seed | 1);
    let wide = |r: &mut Rng| (1u64 << 20) + r.next() % ((1u64 << 40) - (1 << 20));
    let mut cur: Vec<u64> = Vec::with_capacity(total);
    let mut eids = Vec::with_capacity(total);
    for i in 0..total {
        let e = eng.entity_in("t").unwrap();
        let w = wide(&mut r);
        eng.tie_to_by_id(e, hid, i as u32);
        eng.tie_to_by_id(e, hw, w);
        cur.push(w);
        eids.push(e);
    }
    eng.declare_ball("t.w").unwrap();
    let rounds = env_usize("BALL_ROUNDS", 10);
    let (mut t1, mut t5) = (0, 0);
    let (mut at1, mut at5) = (Vec::new(), Vec::new());
    for round in 1..=rounds {
        for _ in 0..total / 10 {
            let i = (r.next() % total as u64) as usize;
            let w = wide(&mut r);
            eng.tie_to_by_id(eids[i], hw, w);
            cur[i] = w;
        }
        if round == 1 {
            t1 = eng.ball_now();
            at1 = cur.clone();
        }
        if round == 5 {
            t5 = eng.ball_now();
            at5 = cur.clone();
        }
    }
    let now = eng.ball_now();
    let picks: Vec<usize> = (0..10_000).map(|k| (Rng(k as u64 * 0x9E37 + seed | 1).next() % total as u64) as usize).collect();
    // 答え合わせ (200 本): その時の値を持っていた行の全部
    for &i in picks.iter().take(200) {
        for (t, at) in [(t1, &at1), (t5, &at5), (now, &cur)] {
            let v = at[i];
            let want: Vec<u64> = (0..total).filter(|&j| at[j] == v).map(|j| eids[j]).collect();
            assert_eq!(eng.ball_pull_at("t.w", v, t).unwrap(), want, "t={t} v={v}");
        }
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
        println!("{{\"mode\":\"rw\",\"kind\":\"{name}\",\"seed\":{seed},\"total\":{total},\"ns\":{:.1}}}", xs[2]);
    };
    time("pull_now", 10_000, &|k| eng.pull_raw("t.w", cur[picks[k]]).len());
    time("pull_at_now", 10_000, &|k| eng.ball_pull_at("t.w", cur[picks[k]], now).unwrap().len());
    time("pull_at_t5", 10_000, &|k| eng.ball_pull_at("t.w", at5[picks[k]], t5).unwrap().len());
    time("pull_at_t1", 10_000, &|k| eng.ball_pull_at("t.w", at1[picks[k]], t1).unwrap().len());
    let (faces, bytes) = eng.ball_stats("t.w").unwrap();
    println!(
        "{{\"mode\":\"rw\",\"kind\":\"ok\",\"seed\":{seed},\"total\":{total},\"rounds\":{rounds},\"faces\":{faces},\"ball_bytes\":{bytes},\"diffs\":{},\"rss_peak_kb\":{}}}",
        eng.ball_diff_count("t.w").unwrap(),
        rss_peak_kb()
    );
}

#[cfg(feature = "ball")]
fn mode_h(seed: u64) {
    let path = db_path("h");
    let _c = Cleanup(path.clone());
    let total = env_usize("BALL_TOTAL", 960_000);
    let hot = env_usize("BALL_HOT", 1000);
    let writes = env_usize("BALL_HOT_WRITES", 2000);
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").with_capacity(total as u32 + 1000).build().unwrap();
    let eng: Arc<Engine> = db.arc_engine();
    let (hid, ha) = (eng.himo_id("t.id").unwrap() as u16, eng.himo_id("t.a").unwrap() as u16);
    let mut r = Rng(seed | 1);
    let mut eids = Vec::with_capacity(total);
    let mut first = Vec::with_capacity(total);
    for i in 0..total {
        let e = eng.entity_in("t").unwrap();
        let a = (r.next() % 100) as u32;
        eng.tie_to_by_id(e, hid, i as u32);
        eng.tie_to_by_id(e, ha, a);
        eids.push(e);
        first.push(a);
    }
    eng.declare_ball("t.a").unwrap();
    // 熱い行は表に散らす。 行ごとの (時刻, 値) の列 (宣言の前からの値は時刻 0)
    let stride = total / hot;
    let hot_ix: Vec<usize> = (0..hot).map(|k| k * stride).collect();
    let mut hist: Vec<Vec<(u64, Option<u32>)>> = hot_ix.iter().map(|&i| vec![(0, Some(first[i]))]).collect();
    let n = hot * writes;
    let mut recent_from = 0;
    for w in 0..n {
        let h = (r.next() % hot as u64) as usize;
        let e = eids[hot_ix[h]];
        let cur = hist[h].last().unwrap().1;
        // 値を必ず変える (同じ値の書き直しは球を回さない)
        let new = match (r.next() % 10, cur) {
            (0, Some(_)) => None,
            (_, c) => {
                let v = (r.next() % 100) as u32;
                Some(if Some(v) == c { (v + 1) % 100 } else { v })
            }
        };
        match new {
            Some(v) => eng.tie_to_by_id(e, ha, v),
            None => eng.untie_by_id(e, ha),
        }
        let tm = eng.ball_now();
        hist[h].push((tm, new));
        if w == n - n / 100 {
            recent_from = tm;
        }
    }
    let end = eng.ball_now();
    let q = 100_000usize;
    let mut qr = Rng(seed.wrapping_mul(0x9E37) | 1);
    let mut pick = |lo: u64| -> Vec<(usize, u64)> { (0..q).map(|_| ((qr.next() % hot as u64) as usize, lo + qr.next() % (end - lo + 1))).collect() };
    let all = pick(0);
    let recent = pick(recent_from);
    // 書き換えていない行: 熱い行は stride (偶数) の倍数番なので、 奇数番
    assert!(stride % 2 == 0);
    let cold: Vec<(usize, u64)> = (0..q).map(|_| ((qr.next() % total as u64) as usize | 1, qr.next() % (end + 1))).collect();
    // 答え合わせ (熱い行は手元の列、 書き換えていない行は前からの値)
    for set in [&all, &recent] {
        for &(h, t) in set.iter() {
            let k = hist[h].partition_point(|x| x.0 <= t) - 1;
            assert_eq!(eng.ball_value_at(eids[hot_ix[h]], "t.a", t).unwrap(), hist[h][k].1.map(u64::from), "h={h} t={t}");
        }
    }
    for &(i, t) in &cold {
        assert_eq!(eng.ball_value_at(eids[i], "t.a", t).unwrap(), Some(first[i] as u64), "i={i} t={t}");
    }
    let time = |name: &str, f: &dyn Fn(usize) -> usize| {
        let mut xs: Vec<f64> = (0..5)
            .map(|_| {
                let t0 = Instant::now();
                let mut acc = 0usize;
                for k in 0..q {
                    acc += f(k);
                }
                black_box(acc);
                t0.elapsed().as_nanos() as f64 / q as f64
            })
            .collect();
        xs.sort_by(f64::total_cmp);
        println!("{{\"mode\":\"h\",\"kind\":\"{name}\",\"seed\":{seed},\"hot\":{hot},\"writes\":{writes},\"ns\":{:.1}}}", xs[2]);
    };
    let he: Vec<u64> = hot_ix.iter().map(|&i| eids[i]).collect();
    time("value_at_hot_all", &|k| eng.ball_value_at(he[all[k].0], "t.a", all[k].1).unwrap().unwrap_or(0) as usize);
    time("value_at_hot_recent", &|k| eng.ball_value_at(he[recent[k].0], "t.a", recent[k].1).unwrap().unwrap_or(0) as usize);
    time("value_at_cold", &|k| eng.ball_value_at(eids[cold[k].0], "t.a", cold[k].1).unwrap().unwrap_or(0) as usize);
    let (faces, bytes) = eng.ball_stats("t.a").unwrap();
    println!(
        "{{\"mode\":\"h\",\"kind\":\"ok\",\"seed\":{seed},\"hot\":{hot},\"writes\":{writes},\"faces\":{faces},\"ball_bytes\":{bytes},\"rss_peak_kb\":{}}}",
        rss_peak_kb()
    );
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a[1].as_str() {
        "w" => mode_w(a[2].parse().unwrap(), a[3].parse().unwrap()),
        #[cfg(feature = "ball")]
        "r" => mode_r(a[2].parse().unwrap()),
        #[cfg(feature = "ball")]
        "h" => mode_h(a[2].parse().unwrap()),
        #[cfg(feature = "ball")]
        "rw" => mode_rw(a[2].parse().unwrap()),
        m => panic!("unknown mode {m}"),
    }
}
