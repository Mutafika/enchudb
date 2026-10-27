//! 球の土台 (試作、 11 回目、 `enchudb_engine::ball_base`): changefeed の記録 (`enable_change_log`) のある DB で球にした紐は、
//! 閉じて開き直し、 宣言し直すと、 開き直す前の履歴が読める。 schema の書き込み (upsert / untie / delete / insert) を回ごとに重ね、
//! 回の終わりの時刻と手元の写しを控える。 開き直した後、 地平以降の全部の控えで `ball_pull_at` / `ball_value_at` が写しと同じ、
//! 地平より前は `TooOld`。 記録が捨てた時は地平が最後に捨てた record に進む。 記録を作り直した (土台が記録の外を指す) 時は今から。

use enchudb_engine::ball::TooOld;
use enchudb_schema::Database;
use std::collections::BTreeMap;
use std::sync::Arc;

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-ball-reopen-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
}

const CITIES: [&str; 4] = ["Tokyo", "Osaka", "Kyoto", "Fukuoka"];
const WAL: usize = 8 << 20;

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// eid → (city の語の番号, age)。
type Rows = BTreeMap<u64, (Option<u64>, Option<u64>)>;

/// 手元の写しと、 書いた回の控え (時刻, 写し)。
struct World {
    rows: Rows,
    ids: BTreeMap<u64, i64>,
    next_id: i64,
    r: Rng,
    checks: Vec<(u64, Rows)>,
}

impl World {
    fn new(seed: u64) -> Self {
        Self { rows: BTreeMap::new(), ids: BTreeMap::new(), next_id: 0, r: Rng(seed), checks: Vec::new() }
    }

    fn insert(&mut self, db: &Database) {
        let users = db.get_table("users").unwrap();
        let (c, a) = (CITIES[self.r.below(4) as usize], self.r.below(50));
        let e = users.insert().set("id", self.next_id).set("city", c).set("age", a as i64).commit().unwrap();
        assert!(!self.rows.contains_key(&e));
        self.rows.insert(e, (Some(db.engine().vocab_id(c).unwrap() as u64), Some(a)));
        self.ids.insert(e, self.next_id);
        self.next_id += 1;
    }

    /// `rounds` 回 × `per` 件を書き、 回ごとに (ball_now, 写し) を控える。
    fn write(&mut self, db: &Database, rounds: usize, per: usize) {
        let users = db.get_table("users").unwrap();
        let eng = db.engine();
        for _ in 0..rounds {
            for _ in 0..per {
                let live: Vec<u64> = self.rows.keys().copied().collect();
                let e = live[self.r.below(live.len() as u64) as usize];
                match self.r.below(10) {
                    0..=3 => {
                        let c = CITIES[self.r.below(4) as usize];
                        users.upsert().set("id", self.ids[&e]).set("city", c).commit().unwrap();
                        self.rows.get_mut(&e).unwrap().0 = Some(eng.vocab_id(c).unwrap() as u64);
                    }
                    4..=5 => {
                        let a = self.r.below(50);
                        users.upsert().set("id", self.ids[&e]).set("age", a as i64).commit().unwrap();
                        self.rows.get_mut(&e).unwrap().1 = Some(a);
                    }
                    6 => {
                        eng.untie(e, "users.city");
                        eng.commit();
                        self.rows.get_mut(&e).unwrap().0 = None;
                    }
                    7 => {
                        users.entity(e).delete().unwrap();
                        self.rows.remove(&e);
                        self.ids.remove(&e);
                    }
                    _ => self.insert(db),
                }
            }
            self.checks.push((eng.ball_now(), self.rows.clone()));
        }
    }

    /// 控えの全部の時刻で答えを比べる。 地平より前は TooOld。 返り値は (比べた控え, TooOld だった控え)。
    fn verify(&self, db: &Database, what: &str) -> (usize, usize) {
        let eng = db.engine();
        // 地平は紐ごと (宣言は 1 本ずつ)。 間に書き込みが無いので、 控えの時刻は 2 本とも同じ側にある
        let hz = eng.ball_horizon("users.city").unwrap();
        let hz_a = eng.ball_horizon("users.age").unwrap();
        assert!(self.checks.iter().all(|(t, _)| (*t < hz) == (*t < hz_a)), "{what}: 地平の間に控え ({hz}, {hz_a})");
        let vids: Vec<u64> = CITIES.iter().map(|c| eng.vocab_id(c).unwrap() as u64).collect();
        let all_eids: Vec<u64> =
            self.checks.iter().flat_map(|(_, rs)| rs.keys().copied()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        let (mut seen, mut old) = (0, 0);
        for (k, (t, rs)) in self.checks.iter().enumerate() {
            if *t < hz {
                assert_eq!(eng.ball_pull_at("users.city", vids[0], *t), Err(TooOld { horizon: hz }), "{what}: 回 {k}");
                assert_eq!(eng.ball_value_at(all_eids[0], "users.age", *t), Err(TooOld { horizon: hz_a }), "{what}: 回 {k}");
                old += 1;
                continue;
            }
            seen += 1;
            for &vid in &vids {
                let want: Vec<u64> = rs.iter().filter(|(_, (c, _))| *c == Some(vid)).map(|(&e, _)| e).collect();
                assert_eq!(eng.ball_pull_at("users.city", vid, *t).unwrap(), want, "{what}: 回 {k} t={t} city={vid}");
            }
            for a in [0u64, 7, 49] {
                let want: Vec<u64> = rs.iter().filter(|(_, (_, ra))| *ra == Some(a)).map(|(&e, _)| e).collect();
                assert_eq!(eng.ball_pull_at("users.age", a, *t).unwrap(), want, "{what}: 回 {k} t={t} age={a}");
            }
            for &e in &all_eids {
                let (c, a) = rs.get(&e).copied().unwrap_or((None, None));
                assert_eq!(eng.ball_value_at(e, "users.city", *t).unwrap(), c, "{what}: 回 {k} e={e}");
                assert_eq!(eng.ball_value_at(e, "users.age", *t).unwrap(), a, "{what}: 回 {k} e={e}");
            }
        }
        // 今の答え = 今の読み
        let users = db.get_table("users").unwrap();
        let now = eng.ball_now();
        for c in CITIES {
            let mut cur = users.where_eq("city", c).find().unwrap();
            cur.sort_unstable();
            assert_eq!(eng.ball_pull_at("users.city", eng.vocab_id(c).unwrap() as u64, now).unwrap(), cur, "{what}: 今の {c}");
        }
        (seen, old)
    }
}

fn create(path: &str, log_cap: usize) -> Arc<Database> {
    let mut db = Database::create(path).unwrap();
    db.table("users").number("id").tag("city").number("age").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(WAL).unwrap();
    db.engine().enable_change_log(log_cap).unwrap();
    db
}

fn declare(db: &Database) -> (bool, bool) {
    let eng = db.engine();
    (eng.declare_ball("users.city").unwrap(), eng.declare_ball("users.age").unwrap())
}

/// (T1) 記録が捨てない大きさ: 開き直した後の地平は最初の宣言の時刻のまま、 全部の控えが答えと同じ。 開き直した後に書いた分も、
/// もう 1 度開き直して同じ。
#[test]
fn reopen_reads_the_history_before_close() {
    let path = tmp_path("t1");
    cleanup(&path);
    let mut w = World::new(0x2545_F491_4F6C_DD1D);
    let hz0;
    {
        let db = create(&path, 64 << 20);
        for _ in 0..200 {
            w.insert(&db);
        }
        assert_eq!(declare(&db), (true, true));
        hz0 = db.engine().ball_horizon("users.city").unwrap();
        w.write(&db, 40, 40);
        assert_eq!(w.verify(&db, "閉じる前"), (40, 0));
        assert_eq!(std::fs::read_dir(std::path::Path::new(&path).join("ball")).unwrap().count(), 2, "紐 2 本の土台");
    }
    {
        let db = Database::open_with_oplog(&path, WAL).unwrap();
        // 宣言し直すまでは球が無い (メモリの上の物)
        assert_eq!(db.engine().ball_horizon("users.city"), None);
        assert_eq!(declare(&db), (true, true), "この process では初めての宣言");
        let st = db.engine().ball_rebuild_stats().unwrap();
        assert!(st.records > 1000, "記録から読んだ: {st:?}");
        assert_eq!(st.fixed, 0, "記録に載っていない書き込みは無い: {st:?}");
        assert_eq!(db.engine().ball_horizon("users.city"), Some(hz0), "捨てていないので地平は最初の宣言");
        assert_eq!(w.verify(&db, "開き直した後"), (40, 0));
        // 開き直した後に書く
        w.write(&db, 20, 40);
        assert_eq!(w.verify(&db, "開き直して書いた後"), (60, 0));
    }
    {
        let db = Database::open_with_oplog(&path, WAL).unwrap();
        assert_eq!(declare(&db), (true, true));
        assert_eq!(db.engine().ball_horizon("users.city"), Some(hz0));
        assert_eq!(w.verify(&db, "2 回目に開き直した後"), (60, 0));
    }
    cleanup(&path);
}

/// (T2) 記録 1 MiB で何周も捨てさせる: 開き直した後の地平は宣言の時刻より後 (最後に捨てた record)、 地平以降の控えは答えと同じ、
/// 前は TooOld。
#[test]
fn reopen_after_the_log_dropped_records() {
    let path = tmp_path("t2");
    cleanup(&path);
    let mut w = World::new(0x9E37_79B9_7F4A_7C15);
    let hz0;
    {
        let db = create(&path, 1 << 20);
        for _ in 0..200 {
            w.insert(&db);
        }
        declare(&db);
        hz0 = db.engine().ball_horizon("users.city").unwrap();
        w.write(&db, 150, 100);
        let (first, next) = db.engine().change_log_range().unwrap();
        assert!(first > next / 2, "記録が何周も捨てていない — 試験の前提 ({first}, {next})");
        // 開いている間は、 メモリの球が宣言から持っている (記録より長い)
        assert_eq!(w.verify(&db, "閉じる前"), (150, 0));
    }
    let db = Database::open_with_oplog(&path, WAL).unwrap();
    declare(&db);
    let hz = db.engine().ball_horizon("users.city").unwrap();
    assert!(hz > hz0, "地平が進んでいない: {hz} <= {hz0}");
    assert_eq!(db.engine().ball_horizon("users.age"), Some(hz), "2 本の土台は同じ record まで当てた");
    let (seen, old) = w.verify(&db, "開き直した後");
    assert!(seen >= 20 && old >= 20, "地平が控えの間にない — 試験の前提 (後 {seen}, 前 {old})");
    eprintln!("T2: 地平以降の控え {seen} / 前 {old}、 組み直し {:?}", db.engine().ball_rebuild_stats().unwrap());
    drop(db);
    cleanup(&path);
}

/// (T4) 記録を作り直した (土台が記録の外を指す): 土台は消え、 宣言は今から。 前の控えは全部 TooOld、 今の答えは今の読み。
#[test]
fn a_base_the_log_no_longer_covers_starts_over() {
    let path = tmp_path("t4");
    cleanup(&path);
    let mut w = World::new(0x1234_5678_9ABC_DEF1);
    {
        let db = create(&path, 1 << 20);
        for _ in 0..200 {
            w.insert(&db);
        }
        declare(&db);
        w.write(&db, 10, 40);
    }
    std::fs::remove_file(std::path::Path::new(&path).join("changefeed.seg")).unwrap();
    let db = Database::open_with_oplog(&path, WAL).unwrap();
    db.engine().enable_change_log(1 << 20).unwrap();
    assert_eq!(std::fs::read_dir(std::path::Path::new(&path).join("ball")).unwrap().count(), 0, "使えない土台は消す");
    assert_eq!(declare(&db), (true, true));
    assert!(db.engine().ball_rebuild_stats().is_none(), "組み直していない (今から)");
    assert_eq!(w.verify(&db, "作り直した後"), (0, 10));
    // 今からの履歴は残る
    let mut w2 = World { rows: w.rows.clone(), ids: w.ids.clone(), next_id: w.next_id, r: Rng(77), checks: Vec::new() };
    w2.write(&db, 10, 40);
    drop(db);
    let db = Database::open_with_oplog(&path, WAL).unwrap();
    declare(&db);
    assert_eq!(w2.verify(&db, "作り直して書いて開き直した後"), (10, 0));
    drop(db);
    cleanup(&path);
}
