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

/// 試験が落ちても (panic でも) DB を消す。 `BALL_KEEP` の写しは通った時だけ消す (落ちた試験の写しを残す)。
struct Gone(String);
impl Drop for Gone {
    fn drop(&mut self) {
        cleanup(&self.0);
        if !std::thread::panicking()
            && let Some(c) = keep_case(&self.0)
        {
            let _ = std::fs::remove_dir_all(c);
        }
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

/// 18 回目 (答えの食い違いの調べ): `BALL_KEEP` (directory) があれば、 session を開く前ごとに DB の写し (APFS の clone、 前の session を
/// 閉じた後の形) を `{BALL_KEEP}/{DB の名前}/pre` に置き直し、 答えが食い違ったら、 控え (`hist.txt`) と説明 (`why.txt`) と食い違った時の
/// DB の写し (`post`、 開いたまま写す) を足して残す。 `keep_replay` (`#[ignore]`) が `pre` を静かに開いて組み直し、 写した時までの控えを比べる。
fn keep_case(path: &str) -> Option<std::path::PathBuf> {
    std::env::var_os("BALL_KEEP").map(|r| std::path::PathBuf::from(r).join(std::path::Path::new(path).file_name().unwrap()))
}

fn clone_dir(from: &str, to: &std::path::Path) {
    let _ = std::fs::remove_dir_all(to);
    std::fs::create_dir_all(to.parent().unwrap()).unwrap();
    let ok = std::process::Command::new("cp").arg("-cR").arg(from).arg(to).status().is_ok_and(|s| s.success());
    assert!(ok, "写せない: {from} → {to:?}");
}

/// 開く (`BALL_KEEP` があれば、 開く前の DB を写して、 その時の控えの数と面の数を覚え、 控えも書き出す = 食い違わなくても `keep_replay` で開ける)。
fn open_kept(path: &str, wal: usize, h: &mut Hist) -> std::sync::Arc<Database> {
    if let Some(c) = keep_case(path) {
        clone_dir(path, &c.join("pre"));
        h.pre_checks = h.checks.len();
        h.pre_faces = h.faces.clone();
        h.dump(&c.join("hist.txt"));
    }
    Database::open_with_oplog(path, wal).unwrap()
}

struct Hist {
    path: String,
    eids: Vec<u64>,
    cur: Vec<Option<u64>>,
    checks: Vec<(u64, Vec<Option<u64>>)>,
    /// 宣言の時の地平 (記録が捨てていなければ組み直した後も同じ)。
    hz0: u64,
    /// entity ごとの、 宣言からの面の数 (宣言の時の値の面 1 + 値を変えて上を向いた回数)。 地平が宣言のままなら球の面の数と同じ
    /// (同じ書き込みを 2 度当てると、 同じ値の面 / 戻って進む面が増える)。
    faces: Vec<usize>,
    /// 最後に `open_kept` が写した時の控えの数と面の数 (`BALL_KEEP` の時だけ)。
    pre_checks: usize,
    pre_faces: Vec<usize>,
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
                let got = eng.ball_pull_at("t.a", v, *t).unwrap();
                if got != want {
                    let only_ball: Vec<u64> = got.iter().filter(|e| !want.contains(e)).copied().collect();
                    let only_hist: Vec<u64> = want.iter().filter(|e| !got.contains(e)).copied().collect();
                    let diff: Vec<u64> = only_ball.iter().chain(&only_hist).copied().collect();
                    let head = format!("回 {k} t={t} v={v} (地平 {hz}、 宣言の地平 {}、 控え {}): 球だけ {only_ball:?}、 写しだけ {only_hist:?}", self.hz0, self.checks.len());
                    self.fail(db, what, k, &diff, head);
                }
            }
            for (i, &e) in self.eids.iter().enumerate() {
                let got = eng.ball_value_at(e, "t.a", *t).unwrap();
                if got != cur[i] {
                    self.fail(db, what, k, &[e], format!("回 {k} t={t} e={e}: 球 {got:?}、 写し {:?}", cur[i]));
                }
            }
        }
        if hz == self.hz0 {
            for (i, &e) in self.eids.iter().enumerate() {
                let n = eng.ball_faces(e, "t.a").len();
                if n != self.faces[i] {
                    self.fail(db, what, self.checks.len() - 1, &[e], format!("e={e} 面の数: 球 {n}、 写し {}", self.faces[i]));
                }
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

    /// 食い違い: 食い違った entity ごとに、 回 `k` の前後 3 回の控え (時刻、 写し、 球の答え) と球の面を全部と、 列と名簿の今を並べ、
    /// 最後に組んだ時の追いつきの記録 (`Engine::ball_rebuild_trace`) を足して panic する。 `BALL_KEEP` があれば写しと控えを残す。
    fn fail(&self, db: &Database, what: &str, k: usize, eids: &[u64], head: String) -> ! {
        let eng = db.engine();
        let mut why = format!("{what}: {head}\n");
        let (lo, hi) = (k.saturating_sub(3), (k + 3).min(self.checks.len() - 1));
        let ha = eng.himo_id("t.a").unwrap() as u16;
        for &e in eids.iter().take(4) {
            let i = self.eids.iter().position(|&x| x == e).unwrap();
            why += &format!("e={e}: 控え (回, 時刻, 写し, 球の答え)\n");
            for (j, (t, cur)) in self.checks.iter().enumerate().take(hi + 1).skip(lo) {
                why += &format!("  {j} {t} {:?} {:?}\n", cur[i], eng.ball_value_at(e, "t.a", *t));
            }
            let fs = eng.ball_faces(e, "t.a");
            let col = eng.get_by_id(e, ha);
            let in_deck: Vec<u64> = VALS.iter().copied().filter(|&v| eng.pull_raw("t.a", v as u32).contains(&e)).collect();
            why += &format!("e={e}: 列の今 {col:?}、 名簿の今 {in_deck:?}、 面 {} 枚 (写しの数 {}) (値, from, to)\n", fs.len(), self.faces[i]);
            for f in &fs {
                why += &format!("  {} {} {}\n", f.value, f.from, f.to);
            }
        }
        why += "最後に組んだ時の記録:\n";
        for l in eng.ball_rebuild_trace() {
            why += &format!("  {l}\n");
        }
        if let Some(c) = keep_case(&self.path) {
            clone_dir(&self.path, &c.join("post"));
            self.dump(&c.join("hist.txt"));
            std::fs::write(c.join("why.txt"), &why).unwrap();
            why += &format!("残した: {c:?}\n");
        }
        panic!("{why}");
    }

    /// 控えを書き出す: 1 行目 `hz0 pre_checks`、 2 行目 eid、 3 行目 写した時の面の数、 以降は控えごとに `時刻 値…` (`-` = 値なし)。
    fn dump(&self, to: &std::path::Path) {
        let join = |it: &mut dyn Iterator<Item = String>| it.collect::<Vec<_>>().join(" ");
        let mut s = format!("{} {}\n", self.hz0, self.pre_checks);
        s += &(join(&mut self.eids.iter().map(|e| e.to_string())) + "\n");
        s += &(join(&mut self.pre_faces.iter().map(|n| n.to_string())) + "\n");
        for (t, cur) in &self.checks {
            s += &format!("{t} {}\n", join(&mut cur.iter().map(|v| v.map_or("-".into(), |v| v.to_string()))));
        }
        std::fs::write(to, s).unwrap();
    }

    /// [`Self::dump`] を読む (写した時までの控えと面の数、 `path` は開く DB)。
    fn load(from: &std::path::Path, path: &str) -> Hist {
        let s = std::fs::read_to_string(from).unwrap();
        let mut lines = s.lines();
        let head: Vec<u64> = lines.next().unwrap().split(' ').map(|x| x.parse().unwrap()).collect();
        let eids: Vec<u64> = lines.next().unwrap().split(' ').map(|x| x.parse().unwrap()).collect();
        let faces: Vec<usize> = lines.next().unwrap().split(' ').map(|x| x.parse().unwrap()).collect();
        let checks: Vec<(u64, Vec<Option<u64>>)> = lines
            .take(head[1] as usize)
            .map(|l| {
                let mut it = l.split(' ');
                let t = it.next().unwrap().parse().unwrap();
                (t, it.map(|v| (v != "-").then(|| v.parse().unwrap())).collect())
            })
            .collect();
        let cur = checks.last().unwrap().1.clone();
        Hist { path: path.into(), eids, cur, checks, hz0: head[0], faces: faces.clone(), pre_checks: head[1] as usize, pre_faces: faces }
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
    let mut h = Hist {
        path: path.into(),
        eids,
        cur,
        checks: Vec::new(),
        hz0: eng.ball_horizon("t.a").unwrap(),
        faces: vec![1; n],
        pre_checks: 0,
        pre_faces: Vec::new(),
    };
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
    /// 組む間に WAL の区画を畳んだ回数 (`OpLog::folds` の差)。
    folds: u32,
    /// 組む間に WAL の書き手が区画を移った回数 (`OpLog::switches` の差、 0.37)。
    switches: u64,
    st: enchudb_engine::engine::BallRebuildStats,
    waits: u64,
    covered: u64,
    /// 「頭から」 の段取り (`head_wait_us`): 組む回の前に書き手を止めた間に WAL が空になったか / 組む側が頭から読む所で待ち始めたか。
    emptied: bool,
    head_waited: bool,
}

/// 開き直して、 書き手 TH 本が 1 回 `per` 件ずつ書き続ける間に、 回 `build_at` の後で別の thread が最初の読みを呼んで球を組む。
/// 書き手は組み終えた後も `after` 回書く (公開の時に書き手が居るように)。 `head_wait_us` > 0 なら 「頭から」 の段取り: 組む回の前に
/// 書き手を止めたまま WAL が空になるのを待ち (最大 `head_wait_us`)、 組む側が頭から読む所で畳まれるのを待ち始めてから (試験の口
/// `ball_set_scan_pause`、 最大 `head_wait_us`) 書き手を戻す。
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
    listen: bool,
    mark_pause_us: u64,
    cross_wait_us: u64,
    head_wait_us: u64,
    refold_wait: (u64, u64),
    ahead_wait_us: u64,
    late_wait_us: u64,
) -> Session {
    let db = open_kept(path, wal, h);
    let eng = db.engine();
    if listen {
        // changefeed の listener が居ると、 通知係が consumer の書き出しのたびに記録へ写す (居なければ畳む時だけ)
        struct Quiet;
        impl enchudb_engine::changefeed::ChangeListener for Quiet {}
        eng.add_change_listener(std::sync::Arc::new(Quiet));
    }
    eng.ball_set_build_pause(pause_ms);
    eng.ball_set_publish_pause(publish_pause_us.0, publish_pause_us.1);
    eng.ball_set_scan_pause(scan_pause_us);
    eng.ball_set_mark_pause(mark_pause_us);
    eng.ball_set_cross_wait(cross_wait_us);
    eng.ball_set_refold_wait(refold_wait.0, refold_wait.1);
    eng.ball_set_ahead_wait(ahead_wait_us);
    eng.ball_set_late_wait(late_wait_us);
    assert!(eng.ball_rebuild_stats().is_none(), "開くだけでは組まない");
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let bar = Barrier::new(TH + 1);
    let shared = Mutex::new(h.cur.clone());
    let faces = Mutex::new(h.faces.clone());
    let writes = AtomicU64::new(0);
    let refused = AtomicU64::new(0);
    let round_now = AtomicUsize::new(0);
    let during = AtomicU64::new(0);
    let folds = std::sync::atomic::AtomicU32::new(0);
    let switches = AtomicU64::new(0);
    // 組み終えた回 + 1 (0 = まだ)
    let built_at = AtomicUsize::new(0);
    let go = std::sync::atomic::AtomicBool::new(true);
    let eids = &h.eids;
    let mut checks = Vec::new();
    let (mut emptied, mut head_waited) = (false, false);
    std::thread::scope(|s| {
        for th in 0..TH {
            let (bar, shared, writes, go, faces, refused) = (&bar, &shared, &writes, &go, &faces, &refused);
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
                        // 断られた書き込み (WAL が満杯で畳めない、 #553) は列にも球にも入らない = 写しにも入れない。 この行を書くのは
                        // この thread だけなので、 外した後に値が残っていれば断られた
                        let ok = match new {
                            None => {
                                eng.untie_by_id(e, ha);
                                eng.get_by_id(e, ha).is_none()
                            }
                            Some(v) => eng.try_tie_to_by_id(e, ha, v as u32).is_ok(),
                        };
                        if ok {
                            turned[k] += usize::from(turns(mine[k], new));
                            mine[k] = new;
                        } else {
                            refused.fetch_add(1, Ordering::Relaxed);
                        }
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
        let (round_now, writes, during, built_at, folds, switches) = (&round_now, &writes, &during, &built_at, &folds, &switches);
        let builder = s.spawn(move || {
            while round_now.load(Ordering::Acquire) < build_at {
                std::thread::yield_now();
            }
            let before = writes.load(Ordering::Relaxed);
            let (folds0, switches0) = (eng.oplog().unwrap().folds(), eng.oplog().unwrap().switches());
            // 最初の読み: 組む (答えは見ない)
            let _ = eng.ball_value_at(eids[0], "t.a", 0);
            during.store(writes.load(Ordering::Relaxed) - before, Ordering::Relaxed);
            folds.store((eng.oplog().unwrap().folds() - folds0) as u32, Ordering::Relaxed);
            switches.store(eng.oplog().unwrap().switches() - switches0, Ordering::Relaxed);
            built_at.store(round_now.load(Ordering::Acquire) + 1, Ordering::Release);
        });
        let wal = eng.oplog().unwrap();
        let until = |f: &dyn Fn() -> bool| {
            let t = std::time::Instant::now();
            while !f() && t.elapsed() < std::time::Duration::from_micros(head_wait_us) {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            f()
        };
        for round in 0..100_000 {
            bar.wait();
            checks.push((wal_now(&db), shared.lock().unwrap().clone()));
            // 「頭から」: 書き手は止まっている (次の柵の前)。 consumer が書き出して全部の区画を畳むと、 写した所は周 0 になる
            let head = head_wait_us > 0 && round + 1 == build_at;
            let waits0 = eng.ball_head_waits();
            if head {
                emptied = until(&|| wal.is_empty());
            }
            round_now.store(round + 1, Ordering::Release);
            if head {
                head_waited = until(&|| eng.ball_head_waits() != waits0);
            }
            let b = built_at.load(Ordering::Acquire);
            if b != 0 && round + 1 >= b + after {
                go.store(false, Ordering::Release);
            }
            // 組む側が組み終えずに終わった (panic): 書き手を止めて抜ける (scope の終わりで panic を伝える。 見ないと回の上限まで回り続ける)
            if b == 0 && builder.is_finished() {
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
        "組む: {st:?}、 組む間の書き込み {}、 畳んだ {}、 区画を移った {}、 回 {rounds}、 公開中で待った {waits}、 閾値より前 {covered}、 空 {emptied}、 頭から {head_waited}、 断られた {}、 控え {}",
        during.load(Ordering::Relaxed),
        folds.load(Ordering::Relaxed),
        switches.load(Ordering::Relaxed),
        refused.load(Ordering::Relaxed),
        h.checks.len()
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
    Session {
        during: during.load(Ordering::Relaxed),
        folds: folds.load(Ordering::Relaxed),
        switches: switches.load(Ordering::Relaxed),
        st,
        waits,
        covered,
        emptied,
        head_waited,
    }
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
        let ss = concurrent_session(&path, WAL, &mut h, 30, 20, 3 + k as usize % 5, 4, k % 3, pp, 0, 0x1234 + k, 100, false, 0, 0, 0, (0, 0), 0, 0);
        overlapped += usize::from(ss.during > 0);
        waits += ss.waits;
        covered += ss.covered;
    }
    assert!(overlapped >= 7, "組むのと書くのが重なっていない — 試験の前提 ({overlapped}/9)");
    assert!(waits > 0 && covered > 0, "公開の時の約束の場所を踏んでいない — 試験の前提 (待った {waits}, 閾値より前 {covered})");
    // 最後にもう 1 回開き直して、 書かずに全部の控え
    let db = open_kept(&path, WAL, &mut h);
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
        let ss = concurrent_session(&path, SMALL_WAL, &mut h, 400, 0, 3, 3, 300, (0, 0), 0, 0x77 + k, 2, false, 0, 0, 0, (0, 0), 0, 0);
        used += usize::from(ss.st.side_ops > 0);
        // 追いつきの間を広げて書き手が全力 = 縮まない: すぐ公開中へ (打ち切らなければ 16 + 2 回。 回の大きさは畳み方で揺れるので幅を持つ)
        assert!(ss.st.rounds <= 8, "縮まない追いつきを回し続けた: {} 回", ss.st.rounds);
    }
    assert!(used >= 2, "組む間に記録が捨てた所を読んでいない — 試験の前提 ({used}/3)");
    cleanup(&path);
}

/// (T5) 同じ区画の WAL の続き (WAL 64 MiB、 追いつきの前に consumer の 1 tick より長く止まる): 写す側が組む側の読み終えた所より
/// 先まで記録に写していても、 組む側は記録を読まずに WAL を読み終えた所から読む (写した所から読むと間の record を落とす)。 記録へ写すのは
/// listener が居る時の通知係なので、 何もしない listener を付ける。 WAL の続きを読む回は試験の口で、 写す側が読み終えた所より先まで写すまで
/// 最大 1 秒待ってから組を取る (18 回目: 止まる 150 ms だけでは、 8 本並べて 24 本中 7 本が前提 (2 回) を踏まなかった)。
#[test]
fn a_build_continues_the_wal_where_it_stopped() {
    let path = tmp_path("t5");
    cleanup(&path);
    let _gone = Gone(path.clone());
    let mut h = create(&path, WAL, 128 << 20);
    let mut ahead = 0;
    for k in 0..3u64 {
        let ss = concurrent_session(&path, WAL, &mut h, 30, 20, 3, 4, 150, (0, 0), 0, 0x5555 + k, 100, true, 0, 0, 0, (0, 0), 1_000_000, 0);
        ahead += ss.st.ahead;
    }
    // 踏んだ回の和 (並べて回すと session ごとの数は揺れる)。 0.37 はもう一方の区画を畳んでも同じ区画の続きは続く (畳まないことは前提にしない)
    assert!(ahead >= 2, "写す側が組む側より先に写していない — 試験の前提 ({ahead} 回)");
    cleanup(&path);
}

/// (T7) 記録が WAL より小さい (記録 1 MiB、 WAL 64 MiB、 書き手は 1 件ごとに 20 µs 空ける、 追いつきの前に 300 ms): 記録は組の番号より先まで捨て、
/// 組む側は捨てる口の控えで先まで当てた後に、 写した所から WAL を読む (WAL はその record をまだ持つ) = 当てた record が重なる。 前に当てた所より前の
/// HLC は当てない (古い record を後から当てると面が化ける)。 listener を付ける (通知係が書き出しのたびに記録へ写す = 記録が組む間に何度も捨てる)。
/// 組を読んだ後・記録を読む前に試験の口で、 記録が組の番号より先まで捨てるまで待つ (最大 2 秒。 15 回目まで 50 ms 止まる形だった)。 重なるのは
/// change_emit を持たずに読んだ回だけ (持つ回は記録が捨てない)。 18 回目: 書き手が全力 (17 回目まで) だと、 #686 / #687 の後は静かな機械で
/// 読みが毎回区画の畳みに追い越され、 3 回の読み直しの後の持って読む回で終わるので重ならない (負荷 5 前後で 3 session とも重なり 0)。
#[test]
fn a_build_drops_what_it_already_applied_from_the_side_notes() {
    let path = tmp_path("t7");
    cleanup(&path);
    let _gone = Gone(path.clone());
    let mut h = create(&path, WAL, 1 << 20);
    let (mut dups, mut side) = (0, 0);
    for k in 0..3u64 {
        let ss = concurrent_session(&path, WAL, &mut h, 400, 20, 3, 3, 300, (0, 0), 0, 0x7777 + k, 2, true, 2_000_000, 0, 0, (0, 0), 0, 0);
        dups += usize::from(ss.st.dups > 0);
        side += usize::from(ss.st.side_ops > 0);
    }
    assert!(side >= 2 && dups >= 2, "控えと WAL が重なっていない — 試験の前提 (控え {side}/3、 重なり {dups}/3)");
    cleanup(&path);
}

/// (T6) 組む間に WAL の書き手が区画を移る (0.37 の 2 区画、 WAL 2 MiB = 区画 1 MiB、 追いつきの前に 40 ms 止まる): 区画を移っただけでは
/// 読み直さず、 前の区画の残りから新しい区画へ続けて読む (新しい区画を読まないと、 移った後の record を落とす)。 WAL の続きを読む前に試験の口で、
/// 書き手が次の区画へ移るまで最大 1 秒待つ (15 回目まで 40 ms の止まりだけで、 8 本並べると 24 回中 22 回移らなかった)。 18 回目: 移る先が
/// 空いてから移るのを待つ間と読む間は change_emit を持つ (持たないと、 移った後の古い区画が読む前に畳まれて読み直しになり、 8 本並べて
/// 24 本中 10〜12 本が前提を踏まなかった)。
#[test]
fn a_build_reads_on_across_wal_halves() {
    let path = tmp_path("t6");
    cleanup(&path);
    let _gone = Gone(path.clone());
    const MID_WAL: usize = 2 << 20;
    let mut h = create(&path, MID_WAL, 128 << 20);
    let mut crossed = 0;
    for k in 0..3u64 {
        let ss = concurrent_session(&path, MID_WAL, &mut h, 30, 20, 3, 4, 40, (0, 0), 0, 0x6666 + k, 100, true, 0, 1_000_000, 0, (0, 0), 0, 0);
        crossed += ss.st.crossed;
    }
    // 踏んだ回の和 (並べて回すと session ごとの数は揺れる)
    assert!(crossed >= 2, "区画を移った後に続けて読んでいない — 試験の前提 ({crossed} 回)");
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
        // WAL の位置を取った後・読む前に、 読み始める所の区画が畳まれるまで最大 200 ms 待つ (畳むと、 読む所が新しい周の途中になる。
        // 15 回目まで 20 ms 止まる形で、 並べて回すと 16 回中 2 回その間に畳まなかった)
        let ss = concurrent_session(&path, SMALL_WAL, &mut h, 30, 20, 3, 3, 40, (0, 0), 200_000, 0x99 + k, 100, false, 0, 0, 0, (0, 0), 0, 0);
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

/// (T8) WAL が空になった後に組む (WAL 1 MiB、 記録 256 MiB = 捨てない): 書き手を止めて consumer が全部の区画を畳むと、 写した所は
/// 「頭から」 (周 0、 区画の周が無い) になる。 組む側が位置を取った後・WAL を読む前に書き手を戻し、 区画が埋まって畳まれるまで待つ
/// (試験の口): 頭からの位置は、 組を置いた後に畳んだかを畳んだ回で見て、 畳んでいれば記録から読み直す (見ないと、 畳んで記録に写した
/// 区画を飛ばして次の区画だけを読む)。 面の数まで合う。 T4 は書き手が止まらないので写した所が頭からにならない (16 回目: 9 session で 0 回)。
/// 16 回目: 12 本並べて 36 session 全部で前提を踏み、 変異 「頭からの時に組の後に畳んだかを見ない」 は 12 本中 12 本で落ちる (`ball_mut_v16b`)。
#[test]
fn a_build_from_an_empty_wal_rereads_when_it_folds() {
    let path = tmp_path("t8");
    cleanup(&path);
    let _gone = Gone(path.clone());
    const SMALL_WAL: usize = 1 << 20;
    let mut h = create(&path, SMALL_WAL, 256 << 20);
    let (mut emptied, mut waited, mut heads) = (0, 0, 0);
    for k in 0..3u64 {
        let ss = concurrent_session(&path, SMALL_WAL, &mut h, 30, 20, 3, 3, 0, (0, 0), 500_000, 0xAA + k, 100, false, 0, 0, 2_000_000, (0, 0), 0, 0);
        assert_eq!(ss.st.side_ops, 0, "記録が捨てた — 試験の前提");
        emptied += usize::from(ss.emptied);
        waited += usize::from(ss.head_waited);
        heads += usize::from(ss.st.head_refolds > 0);
    }
    assert!(emptied >= 2 && waited >= 2, "WAL が空になった後に頭から組んでいない — 試験の前提 (空 {emptied}/3、 頭から {waited}/3)");
    assert!(heads >= 2, "頭からの位置で組の後に畳んでいない — 試験の前提 ({heads}/3)");
    let db = Database::open_with_oplog(&path, SMALL_WAL).unwrap();
    assert_eq!(db.engine().ball_horizon("t.a"), Some(h.hz0), "記録が捨てていない (面の数を確かめた) — 試験の前提");
    drop(db);
    cleanup(&path);
}

/// (T9) 追いつきの読み直しに上限 (18 回目、 WAL 1 MiB = 区画 512 KiB、 記録 256 MiB = 捨てない): 試験の口 (`ball_set_refold_wait`) で、
/// 組みの最初の 6 回の読みは WAL を読んだ後・確かめる前に、 読み始めた所の区画が畳まれるまで (最大 1 秒) 待つ = change_emit を持たずに
/// 読む回は毎回読み直しになる。 3 回続いた後の回は change_emit を持って読むので畳まれず (待ち切ってから確かめる)、 その回で返る: 1 回の追いつきで続いた読み直しは
/// ちょうど 3 回 (3 未満は試験の口が読み直しにしていない = 前提)、 持って読んだ回は 1 回以上。 面の数まで合う。 持たずに読み直し続ける
/// 形は、 書き手が区画を埋めて畳まれるのが区画を読むより速いと終わらない (17 回目の T7、 merge の後に 4 回中 2 回)。
#[test]
fn a_build_reads_holding_the_copy_lock_after_three_refolds() {
    let path = tmp_path("t9");
    cleanup(&path);
    let _gone = Gone(path.clone());
    const SMALL_WAL: usize = 1 << 20;
    let mut h = create(&path, SMALL_WAL, 256 << 20);
    for k in 0..3u64 {
        let ss = concurrent_session(&path, SMALL_WAL, &mut h, 30, 20, 3, 3, 0, (0, 0), 0, 0xBB + k, 100, false, 0, 0, 0, (1_000_000, 6), 0, 0);
        assert_eq!(ss.st.side_ops, 0, "記録が捨てた — 試験の前提");
        assert_eq!(ss.st.refold_run_max, 3, "1 回の追いつきで続いた読み直し (session {k}): {:?}", ss.st);
        assert!(ss.st.held_reads >= 1, "change_emit を持って読んでいない (session {k})");
    }
    let db = Database::open_with_oplog(&path, SMALL_WAL).unwrap();
    assert_eq!(db.engine().ball_horizon("t.a"), Some(h.hz0), "記録が捨てていない (面の数を確かめた) — 試験の前提");
    drop(db);
    cleanup(&path);
}

/// (T12) 持つ回の記録の先読み (20 回目、 WAL 1 MiB、 記録 256 MiB = 捨てない): T9 と同じく組みの最初の 6 回の読みを読み直しにして持つ回を
/// 作り、 試験の口 (`ball_set_late_wait`) で、 持つ回の記録の先読み (lock の外) の後・change_emit を取る前に、 写す側が先読みの所より先まで
/// 写すまで (最大 1 秒) 待つ。 持った後に、 先読みの後に写した分を記録から読む (前提: `held_late` ≥ 1)。 面の数まで合う (読まないと、 先読みの
/// 後に写して畳んだ区画の書き込みを飛ばす)。
#[test]
fn a_held_read_reads_what_was_copied_after_its_pre_read() {
    let path = tmp_path("t12");
    cleanup(&path);
    let _gone = Gone(path.clone());
    const SMALL_WAL: usize = 1 << 20;
    let mut h = create(&path, SMALL_WAL, 256 << 20);
    for k in 0..3u64 {
        let ss = concurrent_session(&path, SMALL_WAL, &mut h, 30, 20, 3, 3, 0, (0, 0), 0, 0xC2 + k, 100, false, 0, 0, 0, (1_000_000, 6), 0, 1_000_000);
        assert_eq!(ss.st.side_ops, 0, "記録が捨てた — 試験の前提");
        assert!(ss.st.held_reads >= 1, "change_emit を持って読んでいない — 試験の前提 (session {k})");
        assert!(ss.st.held_late >= 1, "持った後に先読みの後の分を読んでいない — 試験の前提 (session {k}): {:?}", ss.st);
    }
    let db = Database::open_with_oplog(&path, SMALL_WAL).unwrap();
    assert_eq!(db.engine().ball_horizon("t.a"), Some(h.hz0), "記録が捨てていない (面の数を確かめた) — 試験の前提");
    drop(db);
    cleanup(&path);
}

/// (T10) 頭からを読んで何も無かった次の回 (18 回目、 WAL 64 MiB = 書き手がもう一方の区画へ移らず、 使っていない区画の周は 0。 記録 128 MiB =
/// 捨てない。 listener なし = 記録に写すのは畳む時だけ): 組む側の追いつきの前ごとに 3 秒休ませ、 書き手 (この thread) は休みに合わせて書く。
/// 1 回目の休み: 2000 件書いて今の区画をその場で畳ませる = 1 回目の追いつきは記録から 1024 件以上を読み (縮んでいないので次の回へ続ける)、
/// WAL は頭から (周 0 の位置) 読んで何も無い。 2 回目の休み: 書く → その場で畳む (畳む前の record は記録にだけある) → また書く。
/// 頭からの位置を続きにすると (`OpLog::holds` は周 0 の区画があれば真)、 2 回目は畳んだ後の区画だけを読んで記録を組の番号まで飛ばす =
/// 畳む前の書き込みを落とす (T5 を 12 本並べて 360 本中 4 本)。 直した形は 2 回目に記録を読む。
#[test]
fn a_build_does_not_continue_from_the_head_after_a_fold() {
    use enchudb_oplog::oplog::{wal_pos, HEADER_SIZE};
    const PAUSE_MS: u64 = 3000;
    let path = tmp_path("t10");
    cleanup(&path);
    let _gone = Gone(path.clone());
    let mut h = create(&path, WAL, 128 << 20);
    let db = open_kept(&path, WAL, &mut h);
    let eng = db.engine();
    let wal = eng.oplog().unwrap();
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let until = |f: &dyn Fn() -> bool, ms: u64| {
        let t = std::time::Instant::now();
        while !f() && t.elapsed() < std::time::Duration::from_millis(ms) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        f()
    };
    assert!(until(&|| wal.is_empty(), 3000), "開き直した後に WAL が空にならない — 試験の前提");
    assert!(wal.holds(wal_pos(0, HEADER_SIZE as u64)), "周 0 の区画が無い — 試験の前提 (使っていない区画の周は 0)");
    eng.ball_set_build_pause(PAUSE_MS);
    let e0 = h.eids[0];
    let mut r = Rng(0x7E10);
    let mut write = |h: &mut Hist, n: usize| {
        for _ in 0..n {
            let i = r.below((TH * PER) as u64) as usize;
            let x = r.below(VALS.len() as u64 + 1) as usize;
            let new = (x < VALS.len()).then(|| VALS[x]);
            match new {
                None => eng.untie_by_id(h.eids[i], ha),
                Some(v) => eng.tie_to_by_id(h.eids[i], ha, v as u32),
            }
            eng.commit();
            h.faces[i] += usize::from(turns(h.cur[i], new));
            h.cur[i] = new;
        }
        h.checks.push((wal_now(&db), h.cur.clone()));
    };
    // 書いた後、 consumer が今の区画をその場で畳む (畳む前に記録へ写す) まで待つ
    let folded_since = |f0: u64| until(&|| wal.folds() > f0 && wal.is_empty(), 2000);
    // 組む側が休んでいる間に済ませた (次の追いつきは休みの後)
    let in_pause = |t: std::time::Instant| t.elapsed() < std::time::Duration::from_millis(PAUSE_MS - 200);
    std::thread::scope(|s| {
        let b = s.spawn(|| {
            let _ = eng.ball_value_at(e0, "t.a", 0);
        });
        assert!(until(&|| eng.ball_build_steps() >= 1, 10_000), "最初の記録の読みが返らない");
        let t1 = std::time::Instant::now();
        let f0 = wal.folds();
        write(&mut h, 2000);
        assert!(folded_since(f0), "1 回目の休み: 書いた後に今の区画をその場で畳んでいない — 試験の前提");
        assert!(in_pause(t1), "1 回目の休みの間に書いて畳み終えていない — 試験の前提");
        assert!(until(&|| eng.ball_build_steps() >= 2, 10_000), "1 回目の追いつきが返らない");
        let t2 = std::time::Instant::now();
        let f1 = wal.folds();
        write(&mut h, 200);
        assert!(folded_since(f1), "2 回目の休み: 書いた後に今の区画をその場で畳んでいない — 試験の前提");
        write(&mut h, 50);
        assert!(in_pause(t2), "2 回目の休みの間に書いて畳み終えていない — 試験の前提");
        eng.ball_set_build_pause(0);
        b.join().unwrap();
    });
    let trace = eng.ball_rebuild_trace();
    eprintln!("組む: {:?}、 記録 {trace:#?}", eng.ball_rebuild_stats());
    assert!(trace.iter().any(|l| l.contains("start 0x20 end 0x20")), "頭からを読んで何も無い回が無い — 試験の前提: {trace:#?}");
    h.verify(&db, "頭からを読んだ次の回の後");
    drop(db);
    cleanup(&path);
}

/// (T11) 組む側が公開中に落ちる (19 回目、 WAL 64 MiB、 記録 128 MiB): 試験の口で、 公開中 (書き終えを待った後) に 300 ms 止まってから panic する。
/// 止まる間に書き手は公開中で待つ (前提: 待った書き手 ≥ 1)。 panic の後、 書き手は公開中のまま待たずに進む (2 秒以内に全部の書き手の数が増える)。
/// 書き手を止めた後の次の読みで組み直し、 全部の控えの答えと面の数が合う (待っていた書き手の書き込みは次に組む時に WAL / 記録から当てる)。
/// 書き手は scope の外の thread (公開中のまま止まった書き手を待たずに試験を落とすため)。
#[test]
fn writers_go_on_when_a_build_panics_while_publishing() {
    let path = tmp_path("t11");
    cleanup(&path);
    let _gone = Gone(path.clone());
    let mut h = create(&path, WAL, 128 << 20);
    let db = open_kept(&path, WAL, &mut h);
    let eng = db.engine();
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let counts: std::sync::Arc<Vec<AtomicU64>> = std::sync::Arc::new((0..TH).map(|_| AtomicU64::new(0)).collect());
    let writers: Vec<_> = (0..TH)
        .map(|th| {
            let (db, stop, counts) = (db.clone(), stop.clone(), counts.clone());
            let eids = h.eids[th * PER..(th + 1) * PER].to_vec();
            let mut mine: Vec<Option<u64>> = h.cur[th * PER..(th + 1) * PER].to_vec();
            std::thread::spawn(move || {
                let eng = db.engine();
                let mut turned = vec![0usize; PER];
                let mut r = Rng(0x1111_2222 ^ (0x9E37_79B9_7F4A_7C15u64.wrapping_mul(th as u64 + 1)));
                while !stop.load(Ordering::Acquire) {
                    let k = r.below(PER as u64) as usize;
                    let x = r.below(VALS.len() as u64 + 1) as usize;
                    let new = (x < VALS.len()).then(|| VALS[x]);
                    let ok = match new {
                        None => {
                            eng.untie_by_id(eids[k], ha);
                            eng.get_by_id(eids[k], ha).is_none()
                        }
                        Some(v) => eng.try_tie_to_by_id(eids[k], ha, v as u32).is_ok(),
                    };
                    if ok {
                        turned[k] += usize::from(turns(mine[k], new));
                        mine[k] = new;
                    }
                    eng.commit();
                    counts[th].fetch_add(1, Ordering::Release);
                    std::thread::sleep(std::time::Duration::from_micros(20));
                }
                (mine, turned)
            })
        })
        .collect();
    eng.ball_set_publish_pause(0, 300_000);
    eng.ball_set_publish_panic(true);
    let e0 = h.eids[0];
    let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eng.ball_value_at(e0, "t.a", 0)));
    assert!(built.is_err(), "試験の口で panic していない — 試験の前提");
    // 20 回目: 落ちた組みの控えは手放している (手放さないと、 次に組むまで記録が捨てるたびに控えへ足し続ける。 答えには出ない)
    assert!(!eng.ball_side_active(), "組む側が落ちた後も、 組む間の控えを足し続けている");
    let (waits, _) = eng.ball_publish_counts();
    assert!(waits >= 1, "公開中で待った書き手が居ない — 試験の前提");
    let c0: Vec<u64> = counts.iter().map(|c| c.load(Ordering::Acquire)).collect();
    let t = std::time::Instant::now();
    while counts.iter().zip(&c0).any(|(c, &c0)| c.load(Ordering::Acquire) <= c0) && t.elapsed() < std::time::Duration::from_secs(2) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let moved: Vec<bool> = counts.iter().zip(&c0).map(|(c, &c0)| c.load(Ordering::Acquire) > c0).collect();
    assert!(moved.iter().all(|&m| m), "組む側が公開中に落ちた後、 書き手が公開中のまま動かない: {moved:?}");
    stop.store(true, Ordering::Release);
    for (th, w) in writers.into_iter().enumerate() {
        let (mine, turned) = w.join().unwrap();
        h.cur[th * PER..(th + 1) * PER].copy_from_slice(&mine);
        for (k, n) in turned.into_iter().enumerate() {
            h.faces[th * PER + k] += n;
        }
    }
    h.checks.push((wal_now(&db), h.cur.clone()));
    eng.ball_set_publish_pause(0, 0);
    // 次の読みで組み直す (試験の口は 1 回で落ちた)
    let _ = eng.ball_value_at(e0, "t.a", 0);
    assert!(eng.ball_rebuild_stats().is_some(), "落ちた後の次の読みで組み直していない");
    h.verify(&db, "公開中に落ちた後に組み直した後");
    drop(db);
    cleanup(&path);
}

/// 18 回目の調べ (`#[ignore]`): 残した写し (`BALL_KEEP_CASE` = `BALL_KEEP` の下の 1 つ) の `pre` を別の場所に写して書き手なしで開き
/// (WAL `BALL_KEEP_WAL` byte、 既定 64 MiB)、 最初の読みで組み直して、 写した時までの控えを比べる (食い違えば `fail` と同じ説明)。
#[test]
#[ignore]
fn keep_replay() {
    let case = std::path::PathBuf::from(std::env::var("BALL_KEEP_CASE").expect("BALL_KEEP_CASE"));
    let wal = std::env::var("BALL_KEEP_WAL").ok().map_or(WAL, |s| s.parse().unwrap());
    let path = tmp_path("replay");
    cleanup(&path);
    let _gone = Gone(path.clone());
    clone_dir(case.join("pre").to_str().unwrap(), std::path::Path::new(&path));
    let h = Hist::load(&case.join("hist.txt"), &path);
    let db = Database::open_with_oplog(&path, wal).unwrap();
    // `BALL_KEEP_EIDS` (eid を `,` で区切る) の面を出す (落ちた時の面と比べて、 抜けた書き込みの時刻を見る)
    if let Ok(list) = std::env::var("BALL_KEEP_EIDS") {
        let _ = db.engine().ball_value_at(h.eids[0], "t.a", 0);
        for e in list.split(',').map(|x| x.parse::<u64>().unwrap()) {
            eprintln!("e={e}: 面 (値, from, to)");
            for f in db.engine().ball_faces(e, "t.a") {
                eprintln!("  {} {} {}", f.value, f.from, f.to);
            }
        }
        eprintln!("組んだ時の記録: {:#?}", db.engine().ball_rebuild_trace());
    }
    let (seen, old) = h.verify(&db, "残した写しを静かに開き直した後");
    eprintln!("合った: 控え {} (地平以降 {seen}、 前 {old})、 組む: {:?}", h.checks.len(), db.engine().ball_rebuild_stats());
}
