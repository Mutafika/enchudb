//! 球の土台 (11 回目) の測り。 表 t (id, a) に BALL_ROWS 行 (a = 0..99) を入れ、 WAL + changefeed の記録付きで a を球にする。
//!
//! r <記録 MiB> <書き込み数> <seed>: 組み直しと開く時間 (P1 / P3)
//!    1. 宣言 (土台を作る) の時間 → 書き換え (1 本、 upsert で a を乱数に) → 閉じる
//!    2. 開き直す時間 (土台あり) → 宣言 (組み直し) の数と時間 (`ball_rebuild_stats`) → 答え合わせ → 閉じる
//!    3. 開き直す時間 (土台あり、 2 回目) → 閉じる → ball/ を消す → 開き直す時間 (土台なし)
//!    答え合わせ: 全部の値 v で ball_pull_at(v, 今) = pull_raw(v)、 乱数の 1000 行で ball_value_at(e, 今) = get(e)、 書き込みの途中で
//!    控えた時刻 (地平より後のもの) のデッキ = 手元の写し
//! w <base|nobase> <書き込み数> <seed>: 記録が捨て続ける時の書き手 1 本の書き換え (P2)。 base = 記録を作ってから宣言 (土台あり)、
//!    nobase = 宣言してから記録を作る (メモリだけの球、 記録が捨てても何も当てない)。 記録 1 MiB、 WAL 8 MiB
//! 出力: 1 行 1 JSON (stdout)。

use enchudb::schema::Database;
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

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn db_path(tag: &str) -> String {
    let dir = std::env::var("BALL_DIR").expect("BALL_DIR");
    format!("{dir}/ballp-{tag}-{}", std::process::id())
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const WAL: usize = 8 << 20;

fn build(path: &str, rows: usize) -> Database {
    let mut db = Database::create(path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    {
        let t = db.get_table("t").unwrap();
        for i in 0..rows {
            t.insert().set("id", i as i64).set("a", (i % 100) as i64).commit().unwrap();
        }
    }
    db
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows = env_usize("BALL_ROWS", 100_000);
    match args[1].as_str() {
        "r" => rebuild(args[2].parse().unwrap(), args[3].parse().unwrap(), args[4].parse().unwrap(), rows),
        "w" => writes(args[2] == "base", args[3].parse().unwrap(), args[4].parse().unwrap(), rows),
        "o" => opens(args[2].parse().unwrap(), args[3].parse().unwrap(), rows),
        m => panic!("mode {m}"),
    }
}

fn rebuild(log_mib: usize, n: usize, seed: u64, rows: usize) {
    let path = db_path("r");
    let _c = Cleanup(path.clone());
    let mut r = Rng(seed | 1);
    let mut cur: Vec<u64> = (0..rows).map(|i| (i % 100) as u64).collect();
    let mut checks: Vec<(u64, Vec<u64>)> = Vec::new();
    let declare_first_ns;
    let (first, next);
    {
        let db = build(&path, rows).finish_with_oplog(WAL).unwrap();
        let eng = db.engine();
        eng.enable_change_log(log_mib << 20).unwrap();
        let t0 = Instant::now();
        assert!(eng.declare_ball("t.a").unwrap());
        declare_first_ns = t0.elapsed().as_nanos() as u64;
        let t = db.get_table("t").unwrap();
        for k in 0..n {
            let i = (r.next() % rows as u64) as usize;
            let v = r.next() % 100;
            t.upsert().set("id", i as i64).set("a", v as i64).commit().unwrap();
            cur[i] = v;
            if (k + 1) % (n / 20).max(1) == 0 {
                checks.push((eng.ball_now(), cur.clone()));
            }
        }
        (first, next) = eng.change_log_range().unwrap();
    }
    let open = |p: &str| {
        let t0 = Instant::now();
        let db = Database::open_with_oplog(p, WAL).unwrap();
        (db, t0.elapsed().as_nanos() as u64)
    };
    let (db, open_base_ns) = open(&path);
    let eng = db.engine();
    let t0 = Instant::now();
    assert!(eng.declare_ball("t.a").unwrap());
    let declare_ns = t0.elapsed().as_nanos() as u64;
    let st = eng.ball_rebuild_stats().expect("組み直した");
    // 答え合わせ
    let hid = eng.himo_id("t.a").unwrap();
    let now = eng.ball_now();
    for v in 0..100u64 {
        let mut want = eng.pull_raw("t.a", v);
        want.sort_unstable();
        assert_eq!(eng.ball_pull_at("t.a", v, now).unwrap(), want, "今の v={v}");
    }
    let t = db.get_table("t").unwrap();
    let eid_of: Vec<u64> = (0..rows).map(|i| t.where_eq("id", i as i64).find_one().unwrap().unwrap()).collect();
    for _ in 0..1000 {
        let i = (r.next() % rows as u64) as usize;
        assert_eq!(eng.ball_value_at(eid_of[i], "t.a", now).unwrap(), eng.get_by_id(eid_of[i], hid as u16), "今の行 {i}");
    }
    let hz = eng.ball_horizon("t.a").unwrap();
    let mut seen = 0;
    for (k, (tm, rowsv)) in checks.iter().enumerate() {
        if *tm < hz {
            continue;
        }
        seen += 1;
        for v in [0u64, 37, 99] {
            let want: Vec<u64> = (0..rows).filter(|&i| rowsv[i] == v).map(|i| eid_of[i]).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
            assert_eq!(eng.ball_pull_at("t.a", v, *tm).unwrap(), want, "控え {k} v={v}");
        }
    }
    drop(t);
    drop(db);
    let (db, open_base2_ns) = open(&path);
    drop(db);
    std::fs::remove_dir_all(std::path::Path::new(&path).join("ball")).unwrap();
    let (db, open_nobase_ns) = open(&path);
    drop(db);
    println!(
        "{{\"mode\":\"r\",\"log_mib\":{log_mib},\"rows\":{rows},\"writes\":{n},\"seed\":{seed},\"first\":{first},\"next\":{next},\"declare_first_ns\":{declare_first_ns},\"open_base_ns\":{open_base_ns},\"open_base2_ns\":{open_base2_ns},\"open_nobase_ns\":{open_nobase_ns},\"declare_ns\":{declare_ns},\"records\":{},\"applied\":{},\"fixed\":{},\"log_ns\":{},\"apply_ns\":{},\"check_ns\":{},\"total_ns\":{},\"ns_per_record\":{:.1},\"checks_after_horizon\":{seen},\"checks\":{}}}",
        st.records,
        st.applied,
        st.fixed,
        st.log_ns,
        st.apply_ns,
        st.check_ns,
        st.total_ns,
        declare_ns as f64 / st.records.max(1) as f64,
        checks.len()
    );
}

fn writes(base: bool, n: usize, seed: u64, rows: usize) {
    let path = db_path(if base { "wb" } else { "wn" });
    let _c = Cleanup(path.clone());
    let mut r = Rng(seed | 1);
    let db = build(&path, rows).finish_with_oplog(WAL).unwrap();
    let eng = db.engine();
    let log_mib = env_usize("BALL_LOG_MIB", 1);
    if base {
        eng.enable_change_log(log_mib << 20).unwrap();
        assert!(eng.declare_ball("t.a").unwrap());
    } else {
        assert!(eng.declare_ball("t.a").unwrap());
        eng.enable_change_log(log_mib << 20).unwrap();
    }
    let has_base = std::path::Path::new(&path).join("ball").exists();
    assert_eq!(has_base, base);
    let t = db.get_table("t").unwrap();
    let ids: Vec<(i64, i64)> = (0..n).map(|_| ((r.next() % rows as u64) as i64, (r.next() % 100) as i64)).collect();
    let t0 = Instant::now();
    for &(i, v) in &ids {
        t.upsert().set("id", i).set("a", v).commit().unwrap();
    }
    let write_ns = t0.elapsed().as_nanos() as u64;
    let t1 = Instant::now();
    eng.oplog_sync().unwrap();
    let range = eng.change_log_range().unwrap();
    let sync_ns = t1.elapsed().as_nanos() as u64;
    assert!(range.0 > 0, "記録が捨てていない");
    println!(
        "{{\"mode\":\"w\",\"log_mib\":{log_mib},\"v\":\"{}\",\"rows\":{rows},\"writes\":{n},\"seed\":{seed},\"write_ns\":{write_ns},\"ns_per_write\":{:.1},\"sync_ns\":{sync_ns},\"first\":{},\"next\":{}}}",
        if base { "base" } else { "nobase" },
        write_ns as f64 / n as f64,
        range.0,
        range.1
    );
}

/// o <記録 MiB> <回数>: 開く時間を 土台あり / なし (ball/ を別の名前に動かす) で交互に (P3、 回す順の効きを除く)。
fn opens(log_mib: usize, k: usize, rows: usize) {
    let path = db_path("o");
    let _c = Cleanup(path.clone());
    {
        let db = build(&path, rows).finish_with_oplog(WAL).unwrap();
        let eng = db.engine();
        eng.enable_change_log(log_mib << 20).unwrap();
        assert!(eng.declare_ball("t.a").unwrap());
        let t = db.get_table("t").unwrap();
        let mut r = Rng(7);
        for _ in 0..(log_mib * 6000) {
            t.upsert().set("id", (r.next() % rows as u64) as i64).set("a", (r.next() % 100) as i64).commit().unwrap();
        }
    }
    let ball = std::path::Path::new(&path).join("ball");
    let moved = std::path::Path::new(&path).join("ball.x");
    let open = |p: &str| {
        let t0 = Instant::now();
        let db = Database::open_with_oplog(p, WAL).unwrap();
        let ns = t0.elapsed().as_nanos() as u64;
        drop(db);
        ns
    };
    open(&path); // 温める
    let (mut with, mut without) = (Vec::new(), Vec::new());
    for i in 0..k {
        // 順を回ごとに入れ替える
        for j in 0..2 {
            if (i + j) % 2 == 0 {
                with.push(open(&path));
            } else {
                std::fs::rename(&ball, &moved).unwrap();
                without.push(open(&path));
                std::fs::rename(&moved, &ball).unwrap();
            }
        }
    }
    with.sort_unstable();
    without.sort_unstable();
    println!(
        "{{\"mode\":\"o\",\"log_mib\":{log_mib},\"k\":{k},\"open_base_med_ns\":{},\"open_nobase_med_ns\":{},\"open_base\":{:?},\"open_nobase\":{:?}}}",
        with[k / 2], without[k / 2], with, without
    );
}
