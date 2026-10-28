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
/// WAL は大きめ (畳む間は append が止まり、 公開の約束の場所を書き手が踏めない: この base の WAL は 1 区画)
const WAL: usize = 64 << 20;

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-ball-lazy-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
}

/// 試験が落ちても (panic でも) DB を消す。
struct Gone(String);
impl Drop for Gone {
    fn drop(&mut self) {
        cleanup(&self.0);
    }
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
    /// 宣言の時の地平 (記録が捨てていなければ組み直した後も同じ)。
    hz0: u64,
    /// entity ごとの、 宣言からの面の数 (宣言の時の値の面 1 + 値を変えて上を向いた回数)。 地平が宣言のままなら球の面の数と同じ
    /// (同じ書き込みを 2 度当てると、 同じ値の面 / 戻って進む面が増える)。
    faces: Vec<usize>,
}

/// 書き込み 1 つで面が増えるか (値があって前と違う)。
fn turns(old: Option<u64>, new: Option<u64>) -> bool {
    new.is_some() && new != old
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
        if hz == self.hz0 {
            for (i, &e) in self.eids.iter().enumerate() {
                assert_eq!(eng.ball_faces(e, "t.a").len(), self.faces[i], "{what}: e={e} 面の数");
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
fn create(path: &str, wal: usize, log_cap: usize) -> Hist {
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
    let db = db.finish_with_oplog(wal).unwrap();
    let eng = db.engine();
    eng.enable_change_log(log_cap).unwrap();
    assert!(eng.declare_ball("t.a").unwrap());
    let n = eids.len();
    let mut h = Hist { eids, cur, checks: Vec::new(), hz0: eng.ball_horizon("t.a").unwrap(), faces: vec![1; n] };
    h.checks.push((eng.ball_now(), h.cur.clone()));
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let mut r = Rng(0x2545_F491_4F6C_DD1D);
    for _ in 0..10 {
        for _ in 0..100 {
            let i = r.below((TH * PER) as u64) as usize;
            let v = VALS[r.below(VALS.len() as u64) as usize];
            eng.tie_to_by_id(h.eids[i], ha, v as u32);
            eng.commit();
            h.faces[i] += usize::from(turns(h.cur[i], Some(v)));
            h.cur[i] = Some(v);
        }
        h.checks.push((eng.ball_now(), h.cur.clone()));
    }
    h
}

/// 組んだ時の数: (組む間に書いた数, 組んだ時の段ごとの数, 公開中で待った書き手, 閾値より前で球に書かなかった書き手)。
struct Session {
    during: u64,
    /// 組む間に WAL を畳んだ回数 (WAL の周の差)。
    folds: u32,
    st: enchudb_engine::engine::BallRebuildStats,
    waits: u64,
    covered: u64,
}

/// 開き直して、 書き手 TH 本が 1 回 `per` 件ずつ書き続ける間に、 回 `build_at` の後で別の thread が最初の読みを呼んで球を組む。
/// 書き手は組み終えた後も `after` 回書く (公開の時に書き手が居るように)。
#[allow(clippy::too_many_arguments)]
fn concurrent_session(
    path: &str,
    wal: usize,
    h: &mut Hist,
    per: usize,
    gap_us: u64,
    build_at: usize,
    after: usize,
    pause_ms: u64,
    publish_pause_us: (u64, u64),
    scan_pause_us: u64,
    seed: u64,
    min_seen: usize,
) -> Session {
    let db = Database::open_with_oplog(path, wal).unwrap();
    let eng = db.engine();
    eng.ball_set_build_pause(pause_ms);
    eng.ball_set_publish_pause(publish_pause_us.0, publish_pause_us.1);
    eng.ball_set_scan_pause(scan_pause_us);
    assert!(eng.ball_rebuild_stats().is_none(), "開くだけでは組まない");
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let bar = Barrier::new(TH + 1);
    let shared = Mutex::new(h.cur.clone());
    let faces = Mutex::new(h.faces.clone());
    let writes = AtomicU64::new(0);
    let round_now = AtomicUsize::new(0);
    let during = AtomicU64::new(0);
    let folds = std::sync::atomic::AtomicU32::new(0);
    // 組み終えた回 + 1 (0 = まだ)
    let built_at = AtomicUsize::new(0);
    let go = std::sync::atomic::AtomicBool::new(true);
    let eids = &h.eids;
    let mut checks = Vec::new();
    std::thread::scope(|s| {
        for th in 0..TH {
            let (bar, shared, writes, go, faces) = (&bar, &shared, &writes, &go, &faces);
            let mut mine: Vec<Option<u64>> = h.cur[th * PER..(th + 1) * PER].to_vec();
            let mut turned = vec![0usize; PER];
            s.spawn(move || {
                let mut r = Rng(seed ^ (0x9E37_79B9_7F4A_7C15u64.wrapping_mul(th as u64 + 1)));
                loop {
                    for _ in 0..per {
                        let k = r.below(PER as u64) as usize;
                        let e = eids[th * PER + k];
                        let x = r.below(VALS.len() as u64 + 1) as usize;
                        let new = (x < VALS.len()).then(|| VALS[x]);
                        match new {
                            None => eng.untie_by_id(e, ha),
                            Some(v) => eng.tie_to_by_id(e, ha, v as u32),
                        }
                        turned[k] += usize::from(turns(mine[k], new));
                        mine[k] = new;
                        eng.commit();
                        writes.fetch_add(1, Ordering::Relaxed);
                        if gap_us > 0 {
                            std::thread::sleep(std::time::Duration::from_micros(gap_us));
                        }
                    }
                    shared.lock().unwrap()[th * PER..(th + 1) * PER].copy_from_slice(&mine);
                    bar.wait();
                    bar.wait();
                    if !go.load(Ordering::Acquire) {
                        break;
                    }
                }
                let mut f = faces.lock().unwrap();
                for (k, t) in turned.into_iter().enumerate() {
                    f[th * PER + k] += t;
                }
            });
        }
        let (round_now, writes, during, built_at, folds) = (&round_now, &writes, &during, &built_at, &folds);
        s.spawn(move || {
            while round_now.load(Ordering::Acquire) < build_at {
                std::thread::yield_now();
            }
            let before = writes.load(Ordering::Relaxed);
            let lap = eng.oplog().unwrap().lap();
            // 最初の読み: 組む (答えは見ない)
            let _ = eng.ball_value_at(eids[0], "t.a", 0);
            during.store(writes.load(Ordering::Relaxed) - before, Ordering::Relaxed);
            folds.store(eng.oplog().unwrap().lap() - lap, Ordering::Relaxed);
            built_at.store(round_now.load(Ordering::Acquire) + 1, Ordering::Release);
        });
        for round in 0..100_000 {
            bar.wait();
            checks.push((wal_now(&db), shared.lock().unwrap().clone()));
            round_now.store(round + 1, Ordering::Release);
            let b = built_at.load(Ordering::Acquire);
            if b != 0 && round + 1 >= b + after {
                go.store(false, Ordering::Release);
            }
            bar.wait();
            if !go.load(Ordering::Acquire) {
                break;
            }
        }
    });
    h.cur = shared.into_inner().unwrap();
    h.faces = faces.into_inner().unwrap();
    let rounds = checks.len();
    h.checks.extend(checks);
    let st = eng.ball_rebuild_stats().expect("組んだ");
    let (waits, covered) = eng.ball_publish_counts();
    eprintln!(
        "組む: {st:?}、 組む間の書き込み {}、 畳んだ {}、 回 {rounds}、 公開中で待った {waits}、 閾値より前 {covered}",
        during.load(Ordering::Relaxed),
        folds.load(Ordering::Relaxed)
    );
    let (seen, _) = h.verify(&db, "書き手と並んで組んだ後");
    // 地平以降の控え: 書き手が速いと記録が捨てて地平がこの session の中に入る (T3 は最後の数回だけ)
    assert!(seen >= rounds.min(min_seen), "地平以降の控え {seen}");
    // 組んだ後は書き手が球に書く (今の答え = 今の列)
    let now = eng.ball_now().max(wal_now(&db));
    for v in VALS {
        let mut want = eng.pull_raw("t.a", v as u32);
        want.sort_unstable();
        assert_eq!(eng.ball_pull_at("t.a", v, now).unwrap(), want, "今の v={v}");
    }
    Session { during: during.load(Ordering::Relaxed), folds: folds.load(Ordering::Relaxed), st, waits, covered }
}

/// (T2) 書き手 4 本が書き続ける中で最初の読みが組む (書き手は 1 件ごとに 20 µs 空けて、 組む側が追いつける速さに)。 開き直しを 9 回重ねる
/// (前の session の控えも毎回確かめる)。 書き手は組み終えた後も書くので、 公開中に待つ書き手と、 組む側が当てた (閾値より前の)
/// 書き込みを組んだ後に書く書き手が出る (前提として数える)。 公開中の間は数 µs なので、 試験の口で広げる: H を取る前に 2 ms
/// (H より前に積んで待つ書き手) / 書き終えを待った後に 2 ms (H 以降に積んで待つ書き手 = 組む側の残りの読みに H 以降の record がある) / なし を回す。
#[test]
fn the_first_read_builds_while_writers_write() {
    let path = tmp_path("t2");
    cleanup(&path);
    let _gone = Gone(path.clone());
    let mut h = create(&path, WAL, 128 << 20);
    let (mut overlapped, mut waits, mut covered) = (0, 0, 0);
    for k in 0..9u64 {
        let pp = [(2000, 0), (0, 2000), (0, 0)][k as usize % 3];
        let ss = concurrent_session(&path, WAL, &mut h, 30, 20, 3 + k as usize % 5, 4, k % 3, pp, 0, 0x1234 + k, 100);
        overlapped += usize::from(ss.during > 0);
        waits += ss.waits;
        covered += ss.covered;
    }
    assert!(overlapped >= 7, "組むのと書くのが重なっていない — 試験の前提 ({overlapped}/9)");
    assert!(waits > 0 && covered > 0, "公開の時の約束の場所を踏んでいない — 試験の前提 (待った {waits}, 閾値より前 {covered})");
    // 最後にもう 1 回開き直して、 書かずに全部の控え
    let db = Database::open_with_oplog(&path, WAL).unwrap();
    let (seen, old) = h.verify(&db, "最後に開き直した後");
    assert_eq!(seen + old, h.checks.len());
    assert!(seen >= 100, "地平以降の控え {seen}");
    assert_eq!(db.engine().ball_horizon("t.a"), Some(h.hz0), "記録が捨てていない (面の数を確かめた) — 試験の前提");
    drop(db);
    cleanup(&path);
}

/// (T3) 組む間に記録が捨てる (記録 1 MiB、 WAL 1 MiB、 追いつきの間を広げて書き手が何周も書く): 組む側が読む前に捨てた所は、 捨てる口が
/// 土台に当てる前に渡した控えから当てる (組み直さない)。 WAL も何度も畳む。 書き手が全力で追いつきが縮まないので、 2 回目で公開中に入る
/// (回の数で見る)。
#[test]
fn a_build_reads_what_the_log_dropped_from_the_side_notes() {
    let path = tmp_path("t3");
    cleanup(&path);
    let _gone = Gone(path.clone());
    const SMALL_WAL: usize = 1 << 20;
    let mut h = create(&path, SMALL_WAL, 1 << 20);
    let mut used = 0;
    for k in 0..3u64 {
        let ss = concurrent_session(&path, SMALL_WAL, &mut h, 400, 0, 3, 3, 300, (0, 0), 0, 0x77 + k, 2);
        used += usize::from(ss.st.side_ops > 0);
        // 追いつきの間を広げて書き手が全力 = 縮まない: 2 回目で公開中へ (16 回まで回さない)
        assert!(ss.st.rounds <= 4, "縮まない追いつきを回し続けた: {} 回", ss.st.rounds);
    }
    assert!(used >= 2, "組む間に記録が捨てた所を読んでいない — 試験の前提 ({used}/3)");
    cleanup(&path);
}

/// (T4) 組む間に consumer が記録に写して WAL を畳む (WAL 1 MiB、 記録 256 MiB = 捨てない、 追いつきの間を試験の口で広げる): 同じ周の
/// 間は記録を読まずに WAL の続きから (記録は番号だけ進める) / 畳んだ後は WAL の前の位置が前の周の物 (周で見る)、 記録は当てた HLC まで
/// 飛んで読む / WAL を読む前か読む間に畳むと読んだ所は新しい周の途中 (読んだ分は捨てて記録から読み直す)。 面の数まで合う。
#[test]
fn a_build_reads_on_while_the_wal_folds() {
    let path = tmp_path("t4");
    cleanup(&path);
    let _gone = Gone(path.clone());
    const SMALL_WAL: usize = 1 << 20;
    let mut h = create(&path, SMALL_WAL, 256 << 20);
    let (mut folded, mut refolds) = (0, 0);
    for k in 0..3u64 {
        // WAL の位置を取った後・読む前に 20 ms 止まる (その間に畳むと、 読む所が新しい周の途中になる)
        let ss = concurrent_session(&path, SMALL_WAL, &mut h, 30, 20, 3, 3, 40, (0, 0), 20_000, 0x99 + k, 100);
        assert_eq!(ss.st.side_ops, 0, "記録が捨てた — 試験の前提");
        folded += usize::from(ss.folds > 0);
        refolds += usize::from(ss.st.refolds > 0);
    }
    assert!(folded >= 2, "組む間に WAL を畳んでいない — 試験の前提 ({folded}/3)");
    assert!(refolds >= 2, "WAL を読む間に畳んでいない — 試験の前提 ({refolds}/3)");
    let db = Database::open_with_oplog(&path, SMALL_WAL).unwrap();
    assert_eq!(db.engine().ball_horizon("t.a"), Some(h.hz0), "記録が捨てていない (面の数を確かめた) — 試験の前提");
    drop(db);
    cleanup(&path);
}
