//! 球の土台 (試作、 12 回目、 `enchudb_engine::ball_base`): 開き直した後、 書き手が球の紐を書き続ける中で、 最初の読みが土台 + 記録 +
//! WAL の残りから球を組む (書き手を止めずに。 公開中を置いて Commit を積み、 その HLC を閾値にする約束は `enchudb_engine::ball::Balls`)。
//!
//! 書き手 4 本がそれぞれ自分の行を回ごとに書き換え / 外し、 柵でそろえた所で時刻 (WAL の時計を球の時刻と同じに詰めたもの) と手元の写しを
//! 控える。 組む読みは別の thread から回の途中で呼ぶ (組む間を試験の口で広げる)。 書き終えた後、 地平以降の全部の控えで `ball_pull_at` /
//! `ball_value_at` が写しと同じ。 開き直しを何回も重ねる (前の session の控えも毎回確かめる)。 値には大きい値 (2^20 以上 = 列の糸) も混ぜる。

use enchudb_engine::ball::TooOld;
use enchudb_schema::Database;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Barrier, Mutex};

const TH: usize = 4;
const PER: usize = 250;
const VALS: [u64; 6] = [3, 5, 8, 13, 21, 2_000_000];
const WAL: usize = 8 << 20;

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-ball-lazy-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// WAL の時計の今を球の時刻と同じ詰め方で (球を組んでいない間も進む時刻)。
fn wal_now(db: &Database) -> u64 {
    let h = db.engine().oplog().unwrap().mint_hlc();
    h.wall << 23 | (h.logical as u64).min((1 << 23) - 1)
}

struct Hist {
    eids: Vec<u64>,
    cur: Vec<Option<u64>>,
    checks: Vec<(u64, Vec<Option<u64>>)>,
}

impl Hist {
    /// 全部の控えで答えを比べる。 返り値は (比べた控え, 地平より前で TooOld だった控え)。
    fn verify(&self, db: &Database, what: &str) -> (usize, usize) {
        let eng = db.engine();
        let hz = eng.ball_horizon("t.a").unwrap();
        let (mut seen, mut old) = (0, 0);
        for (k, (t, cur)) in self.checks.iter().enumerate() {
            if *t < hz {
                assert_eq!(eng.ball_pull_at("t.a", VALS[0], *t), Err(TooOld { horizon: hz }), "{what}: 回 {k}");
                old += 1;
                continue;
            }
            seen += 1;
            for v in VALS {
                let mut want: Vec<u64> = (0..self.eids.len()).filter(|&i| cur[i] == Some(v)).map(|i| self.eids[i]).collect();
                want.sort_unstable();
                assert_eq!(eng.ball_pull_at("t.a", v, *t).unwrap(), want, "{what}: 回 {k} t={t} v={v}");
            }
            for (i, &e) in self.eids.iter().enumerate() {
                assert_eq!(eng.ball_value_at(e, "t.a", *t).unwrap(), cur[i], "{what}: 回 {k} t={t} e={e}");
            }
        }
        // 同じ書き込みを組む側と書き手の両方が当てると、 同じ値の面が隙間なく続く (答えは集合として同じなので、 面で見る)。
        // 外して同じ値を書き直した時は前の面が閉じてから次が上を向く (隙間がある)
        for &e in &self.eids {
            let fs = eng.ball_faces(e, "t.a");
            for w in fs.windows(2) {
                assert!(w[0].from <= w[0].to && w[0].to <= w[1].from, "{what}: e={e} 面の時刻の順 {fs:?}");
                assert!(!(w[0].value == w[1].value && w[0].to == w[1].from), "{what}: e={e} 同じ値の面が続く (二重に当てた) {fs:?}");
            }
        }
        (seen, old)
    }
}

/// 表 t (id, a) に TH × PER 行を入れ、 記録付きで a を球にして、 1 本で何回か書いて控える。
fn create(path: &str, log_cap: usize) -> Hist {
    let mut db = Database::create(path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    let mut eids = Vec::new();
    let mut cur = Vec::new();
    {
        let t = db.get_table("t").unwrap();
        for i in 0..TH * PER {
            let v = VALS[i % VALS.len()];
            eids.push(t.insert().set("id", i as i64).set("a", v as i64).commit().unwrap());
            cur.push(Some(v));
        }
    }
    let db = db.finish_with_oplog(WAL).unwrap();
    let eng = db.engine();
    eng.enable_change_log(log_cap).unwrap();
    assert!(eng.declare_ball("t.a").unwrap());
    let mut h = Hist { eids, cur, checks: Vec::new() };
    h.checks.push((eng.ball_now(), h.cur.clone()));
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let mut r = Rng(0x2545_F491_4F6C_DD1D);
    for _ in 0..10 {
        for _ in 0..100 {
            let i = r.below((TH * PER) as u64) as usize;
            let v = VALS[r.below(VALS.len() as u64) as usize];
            eng.tie_to_by_id(h.eids[i], ha, v as u32);
            eng.commit();
            h.cur[i] = Some(v);
        }
        h.checks.push((eng.ball_now(), h.cur.clone()));
    }
    h
}

/// 開き直して、 書き手 TH 本が `rounds` 回 × `per` 件ずつ書く間に、 回 `build_at` の後で別の thread が最初の読みを呼んで球を組む。
/// 返り値は (組む間に書いた数 (試験の前提: 組むのと書くのが重なった), 組んだ時の数)。
fn concurrent_session(
    path: &str,
    h: &mut Hist,
    rounds: usize,
    per: usize,
    build_at: usize,
    pause_ms: u64,
    seed: u64,
    min_seen: usize,
) -> (u64, enchudb_engine::engine::BallRebuildStats) {
    let db = Database::open_with_oplog(path, WAL).unwrap();
    let eng = db.engine();
    eng.ball_set_build_pause(pause_ms);
    assert!(eng.ball_rebuild_stats().is_none(), "開くだけでは組まない");
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let bar = Barrier::new(TH + 1);
    let shared = Mutex::new(h.cur.clone());
    let writes = AtomicU64::new(0);
    let round_now = AtomicUsize::new(0);
    let during = AtomicU64::new(0);
    let eids = &h.eids;
    let mut checks = Vec::new();
    std::thread::scope(|s| {
        for th in 0..TH {
            let (bar, shared, writes) = (&bar, &shared, &writes);
            let mut mine: Vec<Option<u64>> = h.cur[th * PER..(th + 1) * PER].to_vec();
            s.spawn(move || {
                let mut r = Rng(seed ^ (0x9E37_79B9_7F4A_7C15u64.wrapping_mul(th as u64 + 1)));
                for _ in 0..rounds {
                    for _ in 0..per {
                        let k = r.below(PER as u64) as usize;
                        let e = eids[th * PER + k];
                        let x = r.below(VALS.len() as u64 + 1) as usize;
                        if x == VALS.len() {
                            eng.untie_by_id(e, ha);
                            mine[k] = None;
                        } else {
                            eng.tie_to_by_id(e, ha, VALS[x] as u32);
                            mine[k] = Some(VALS[x]);
                        }
                        eng.commit();
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                    shared.lock().unwrap()[th * PER..(th + 1) * PER].copy_from_slice(&mine);
                    bar.wait();
                    bar.wait();
                }
            });
        }
        let (round_now, writes, during) = (&round_now, &writes, &during);
        s.spawn(move || {
            while round_now.load(Ordering::Acquire) < build_at {
                std::thread::yield_now();
            }
            let before = writes.load(Ordering::Relaxed);
            // 最初の読み: 組む (答えは見ない)
            let _ = eng.ball_value_at(eids[0], "t.a", 0);
            during.store(writes.load(Ordering::Relaxed) - before, Ordering::Relaxed);
        });
        for round in 0..rounds {
            bar.wait();
            checks.push((wal_now(&db), shared.lock().unwrap().clone()));
            round_now.store(round + 1, Ordering::Release);
            bar.wait();
        }
    });
    h.cur = shared.into_inner().unwrap();
    h.checks.extend(checks);
    let st = eng.ball_rebuild_stats().expect("組んだ");
    eprintln!("組む: {st:?}、 組む間の書き込み {}", during.load(Ordering::Relaxed));
    let (seen, _) = h.verify(&db, "書き手と並んで組んだ後");
    // 記録が捨てない大きさなら全部の控えが地平以降。 捨てる時 (T3) は最後の数回だけ
    assert!(seen >= rounds.min(min_seen), "地平以降の控え {seen}");
    // 組んだ後は書き手が球に書く (今の答え = 今の列)
    let now = eng.ball_now().max(wal_now(&db));
    for v in VALS {
        let mut want = eng.pull_raw("t.a", v as u32);
        want.sort_unstable();
        assert_eq!(eng.ball_pull_at("t.a", v, now).unwrap(), want, "今の v={v}");
    }
    (during.load(Ordering::Relaxed), st)
}

/// (T2) 書き手 4 本が書き続ける中で最初の読みが組む。 開き直しを 8 回重ねる (前の session の控えも毎回確かめる)。
#[test]
fn the_first_read_builds_while_writers_write() {
    let path = tmp_path("t2");
    cleanup(&path);
    let mut h = create(&path, 16 << 20);
    let mut overlapped = 0;
    for k in 0..8u64 {
        let (during, _) = concurrent_session(&path, &mut h, 30, 30, 5 + k as usize % 7, 30, 0x1234 + k, usize::MAX);
        overlapped += usize::from(during > 0);
    }
    assert!(overlapped >= 6, "組むのと書くのが重なっていない — 試験の前提 ({overlapped}/8)");
    // 最後にもう 1 回開き直して、 書かずに全部の控え
    let db = Database::open_with_oplog(&path, WAL).unwrap();
    let (seen, old) = h.verify(&db, "最後に開き直した後");
    assert_eq!((seen, old), (h.checks.len(), 0));
    drop(db);
    cleanup(&path);
}

/// (T3) 組む間に記録が捨てる (記録 1 MiB、 組む間を広げて書き手が何周も書く): 土台から組み直して答えが合う。
#[test]
fn a_build_restarts_when_the_log_drops_what_it_was_reading() {
    let path = tmp_path("t3");
    cleanup(&path);
    let mut h = create(&path, 1 << 20);
    let mut restarted = 0;
    for k in 0..3u64 {
        let (_, st) = concurrent_session(&path, &mut h, 20, 400, 3, 300, 0x77 + k, 2);
        restarted += usize::from(st.attempts >= 2);
    }
    assert!(restarted >= 1, "組む間に記録が捨てていない — 試験の前提 (組み直した session {restarted}/3)");
    cleanup(&path);
}
