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
//! l <記録 MiB> <書き込み数> <seed>: (12 回目、 14 回目に BALL_LISTEN=1 で何もしない listener を付ける形) 開き直した後、 書き手 1 本が書き続ける中で最初の読みが球を組む。 書き込み 1 回ごとの
//!    時間を取り、 組む間 / 外の最大・p99 と、 組んだ時の段ごとの時間 (`ball_rebuild_stats`: 公開中の間 = 書き手が待ちうる間) を出す (P1)
//!    19 回目: `BALL_REFOLD_WAIT=<µs>,<回>` で組みの最初の <回> 回の読みを読み直しにする (3 なら 4 回目が change_emit を持って読む)。
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

/// WAL の大きさ (`BALL_WAL_MIB`、 既定 8 MiB)。
fn wal_cap() -> usize {
    env_usize("BALL_WAL_MIB", 8) << 20
}

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
        "l" => lazy(args[2].parse().unwrap(), args[3].parse().unwrap(), args[4].parse().unwrap(), rows),
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
        "{{\"mode\":\"r\",\"log_mib\":{log_mib},\"rows\":{rows},\"writes\":{n},\"seed\":{seed},\"first\":{first},\"next\":{next},\"declare_first_ns\":{declare_first_ns},\"open_base_ns\":{open_base_ns},\"open_base2_ns\":{open_base2_ns},\"open_nobase_ns\":{open_nobase_ns},\"declare_ns\":{declare_ns},\"records\":{},\"applied\":{},\"side_ops\":{},\"log_ns\":{},\"catch_ns\":{},\"publish_ns\":{},\"total_ns\":{},\"ns_per_record\":{:.1},\"checks_after_horizon\":{seen},\"checks\":{}}}",
        st.records,
        st.applied,
        st.side_ops,
        st.log_ns,
        st.catch_ns,
        st.publish_ns,
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

fn lazy(log_mib: usize, n: usize, seed: u64, rows: usize) {
    let path = db_path("l");
    let _c = Cleanup(path.clone());
    let mut r = Rng(seed | 1);
    {
        let db = build(&path, rows).finish_with_oplog(wal_cap()).unwrap();
        let eng = db.engine();
        eng.enable_change_log(log_mib << 20).unwrap();
        // BALL_NONE: 球を宣言しない対照 (16 回目、 同じ書き方で球の分を分ける)
        if std::env::var_os("BALL_NONE").is_none() {
            assert!(eng.declare_ball("t.a").unwrap());
        }
        let t = db.get_table("t").unwrap();
        for _ in 0..n {
            t.upsert().set("id", (r.next() % rows as u64) as i64).set("a", (r.next() % 100) as i64).commit().unwrap();
        }
    }
    let db = Database::open_with_oplog(&path, wal_cap()).unwrap();
    let eng = db.engine();
    if std::env::var_os("BALL_LISTEN").is_some() {
        // 何もしない listener (通知係が consumer の書き出しのたびに記録へ写す。 居なければ畳む時だけ)
        struct Quiet;
        impl enchudb::changefeed::ChangeListener for Quiet {}
        eng.add_change_listener(std::sync::Arc::new(Quiet));
    }
    // BALL_REFOLD_WAIT=<µs>,<回> (19 回目): 組みの最初の <回> 回の読みは、 WAL を読んだ後に読み始めた区画が畳まれるまで待つ (読み直しにする)。
    // <回> = 3 なら 4 回目は待たずに change_emit を持って読む = 持つ回の値段を測る
    if let Ok(v) = std::env::var("BALL_REFOLD_WAIT") {
        let (us, n) = v.split_once(',').expect("BALL_REFOLD_WAIT=<µs>,<回>");
        eng.ball_set_refold_wait(us.parse().unwrap(), n.parse().unwrap());
    }
    let records = eng.change_log_range().map_or(0, |(f, x)| x - f);
    let sw0 = eng.wal_switches();
    let stop = std::sync::atomic::AtomicBool::new(false);
    let origin = Instant::now();
    let ((lats, slow), b0, b1) = std::thread::scope(|s| {
        let w = s.spawn(|| {
            let t = db.get_table("t").unwrap();
            let mut r = Rng(seed ^ 0xABCD);
            let mut lats: Vec<(u64, u64)> = Vec::with_capacity(4 << 20);
            // 10 ms を越えた書き込みの間に起きたこと (空き作りで畳んだ回 / 区画を畳んだ回 / 区画を移った回、 14 回目)
            let mut slow: Vec<(u64, u64, [u64; 3])> = Vec::new();
            let marks = || [eng.wal_room_folds(), eng.oplog().map_or(0, |w| w.folds()), eng.wal_switches()];
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let (i, v) = ((r.next() % rows as u64) as i64, (r.next() % 100) as i64);
                let m0 = marks();
                let t0 = Instant::now();
                t.upsert().set("id", i).set("a", v).commit().unwrap();
                let ns = t0.elapsed().as_nanos() as u64;
                lats.push(((t0 - origin).as_nanos() as u64, ns));
                if ns > 10_000_000 {
                    let m1 = marks();
                    slow.push(((t0 - origin).as_nanos() as u64, ns, [m1[0] - m0[0], m1[1] - m0[1], m1[2] - m0[2]]));
                }
            }
            (lats, slow)
        });
        std::thread::sleep(std::time::Duration::from_millis(300));
        let b0 = origin.elapsed().as_nanos() as u64;
        if std::env::var_os("BALL_NONE").is_none() {
            let _ = eng.ball_horizon("t.a");
        }
        let b1 = origin.elapsed().as_nanos() as u64;
        std::thread::sleep(std::time::Duration::from_millis(300));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        (w.join().unwrap(), b0, b1)
    });
    // 19 回目: 書き手の走った時間と区画を移った回数 (区画が埋まるまでの時間 = w_ns / switches)
    let (w_ns, switches) = (origin.elapsed().as_nanos() as u64, eng.wal_switches() - sw0);
    if std::env::var_os("BALL_NONE").is_some() {
        let slow: Vec<String> = slow.iter().map(|&(at, ns, d)| format!("[{},{},{},{},{},0]", at, ns, d[0], d[1], d[2])).collect();
        let max = lats.iter().map(|&(_, l)| l).max().unwrap_or(0);
        println!(
            "{{\"mode\":\"l\",\"none\":1,\"w_ns\":{w_ns},\"switches\":{switches},\"wal_mib\":{},\"log_mib\":{log_mib},\"rows\":{rows},\"writes\":{n},\"seed\":{seed},\"records\":{records},\"build_ns\":0,\"publish_ns\":0,\"slow\":[{}],\"out_max_ns\":{max},\"out_n\":{}}}",
            wal_cap() >> 20, slow.join(","), lats.len()
        );
        return;
    }
    let st = eng.ball_rebuild_stats().expect("組んだ");
    // 答え合わせ: 今のデッキ
    let now = eng.ball_now();
    for v in 0..100u64 {
        let mut want = eng.pull_raw("t.a", v);
        want.sort_unstable();
        assert_eq!(eng.ball_pull_at("t.a", v, now).unwrap(), want, "今の v={v}");
    }
    let stat = |xs: &mut Vec<u64>| {
        xs.sort_unstable();
        if xs.is_empty() {
            return (0, 0, 0);
        }
        (xs.len() as u64, xs[xs.len() * 99 / 100], *xs.last().unwrap())
    };
    let mut inside: Vec<u64> = lats.iter().filter(|&&(at, l)| at + l >= b0 && at <= b1).map(|&(_, l)| l).collect();
    let mut outside: Vec<u64> = lats.iter().filter(|&&(at, l)| !(at + l >= b0 && at <= b1)).map(|&(_, l)| l).collect();
    let slow: Vec<String> = slow.iter().map(|&(at, ns, d)| format!("[{},{},{},{},{},{}]", at, ns, d[0], d[1], d[2], u8::from(at + ns >= b0 && at <= b1))).collect();
    let (ni, p99i, maxi) = stat(&mut inside);
    let (no, p99o, maxo) = stat(&mut outside);
    println!(
        "{{\"mode\":\"l\",\"w_ns\":{w_ns},\"switches\":{switches},\"wal_mib\":{},\"log_mib\":{log_mib},\"rows\":{rows},\"writes\":{n},\"seed\":{seed},\"records\":{records},\"build_ns\":{},\"read_records\":{},\"side_ops\":{},\"log_ns\":{},\"catch_ns\":{},\"publish_ns\":{},\"publish_parts_ns\":{:?},\"rounds\":{},\"prof_ns\":{:?},\"prof_n\":{:?},\"setup_ns\":{:?},\"log_prof_ns\":{:?},\"log_prof_n\":{:?},\"skipped\":{},\"prequiesce_ns\":{},\"refolds\":{},\"refold_run_max\":{},\"held_reads\":{},\"held_max_ns\":{},\"log_lock_ns\":{:?},\"applied\":{},\"replays\":{},\"slow\":[{}],\"in_n\":{ni},\"in_p99_ns\":{p99i},\"in_max_ns\":{maxi},\"out_n\":{no},\"out_p99_ns\":{p99o},\"out_max_ns\":{maxo}}}",
        wal_cap() >> 20, b1 - b0, st.records, st.side_ops, st.log_ns, st.catch_ns, st.publish_ns, st.publish_parts_ns, st.rounds, st.prof_ns, st.prof_n, st.setup_ns, st.log_prof_ns, st.log_prof_n, st.skipped, st.prequiesce_ns, st.refolds, st.refold_run_max, st.held_reads, st.held_max_ns, st.log_lock_ns, st.applied, st.replays, slow.join(",")
    );
}
