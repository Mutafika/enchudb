//! 球 (試作、 `enchudb_engine::ball`、 履歴は球の面とデッキの差分の帳面に残す): schema の書き込み (insert / upsert / untie / delete) を回ごとに重ね、 回の終わりの時刻を
//! 外から控える。 全部の回の時刻で、 デッキの答え (`ball_pull_at`) と球の答え (`ball_value_at`) を手元の写しと比べる。
//! 最後の時刻の答えは今の読み (`where_eq(..).find()`) とも比べる。 schema の表はこの回数では消した eid を使い回さない
//! (使い回しは engine の `ball::tests::wide_values_and_reused_eids` が見る)。

use enchudb_schema::Database;
use std::collections::BTreeMap;

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-ball-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
}

const CITIES: [&str; 4] = ["Tokyo", "Osaka", "Kyoto", "Fukuoka"];

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// eid → (city, age)。
type Rows = BTreeMap<u64, (Option<&'static str>, Option<i64>)>;

#[test]
fn as_of_answers_match_a_log_of_schema_writes() {
    let path = tmp_path("asof");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("users").number("id").tag("city").number("age").primary_key("id").build().unwrap();
    let users = db.get_table("users").unwrap();
    let eng = db.engine();
    let mut rows: Rows = BTreeMap::new();
    let mut next_id = 0i64;
    let mut ids: BTreeMap<u64, i64> = BTreeMap::new();
    // 宣言の前からある row
    for i in 0..200i64 {
        let (c, a) = (CITIES[i as usize % 4], i % 50);
        let e = users.insert().set("id", i).set("city", c).set("age", a).commit().unwrap();
        rows.insert(e, (Some(c), Some(a)));
        ids.insert(e, i);
        next_id = i + 1;
    }
    assert!(eng.declare_ball("users.city").unwrap());
    assert!(eng.declare_ball("users.age").unwrap());
    assert!(!eng.declare_ball("users.city").unwrap(), "2 回目は何もしない");
    let mut checks: Vec<(u64, Rows)> = vec![(eng.ball_now(), rows.clone())];
    let mut r = Rng(0x2545_F491_4F6C_DD1D);
    // 値が変わった書き込みの差分の数 (古い値があれば 出た 1、 新しい値があれば 入った 1) を手元で数える
    let (mut city_d, mut age_d) = (0u64, 0u64);
    let diffs = |old: Option<&str>, new: Option<&str>| if old == new { 0 } else { u64::from(old.is_some()) + u64::from(new.is_some()) };
    let diffs_n = |old: Option<i64>, new: Option<i64>| if old == new { 0 } else { u64::from(old.is_some()) + u64::from(new.is_some()) };
    for _round in 0..60 {
        for _ in 0..40 {
            let live: Vec<u64> = rows.keys().copied().collect();
            let e = live[r.below(live.len() as u64) as usize];
            match r.below(10) {
                0..=3 => {
                    let c = CITIES[r.below(4) as usize];
                    users.upsert().set("id", ids[&e]).set("city", c).commit().unwrap();
                    city_d += diffs(rows[&e].0, Some(c));
                    rows.get_mut(&e).unwrap().0 = Some(c);
                }
                4..=5 => {
                    let a = r.below(50) as i64;
                    users.upsert().set("id", ids[&e]).set("age", a).commit().unwrap();
                    age_d += diffs_n(rows[&e].1, Some(a));
                    rows.get_mut(&e).unwrap().1 = Some(a);
                }
                6 => {
                    eng.untie(e, "users.city");
                    city_d += diffs(rows[&e].0, None);
                    rows.get_mut(&e).unwrap().0 = None;
                }
                7 => {
                    users.entity(e).delete().unwrap();
                    city_d += diffs(rows[&e].0, None);
                    age_d += diffs_n(rows[&e].1, None);
                    rows.remove(&e);
                    ids.remove(&e);
                }
                _ => {
                    let (c, a) = (CITIES[r.below(4) as usize], r.below(50) as i64);
                    let e = users.insert().set("id", next_id).set("city", c).set("age", a).commit().unwrap();
                    assert!(!rows.contains_key(&e));
                    city_d += 1;
                    age_d += 1;
                    rows.insert(e, (Some(c), Some(a)));
                    ids.insert(e, next_id);
                    next_id += 1;
                }
            }
        }
        checks.push((eng.ball_now(), rows.clone()));
    }
    let all_eids: Vec<u64> = checks.iter().flat_map(|(_, rs)| rs.keys().copied()).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    for (k, (t, rs)) in checks.iter().enumerate() {
        for c in CITIES {
            let vid = eng.vocab_id(c).unwrap() as u64;
            let want: Vec<u64> = rs.iter().filter(|(_, (rc, _))| *rc == Some(c)).map(|(&e, _)| e).collect();
            assert_eq!(eng.ball_pull_at("users.city", vid, *t), want, "回 {k} t={t} city={c}");
        }
        for a in [0i64, 7, 49] {
            let want: Vec<u64> = rs.iter().filter(|(_, (_, ra))| *ra == Some(a)).map(|(&e, _)| e).collect();
            assert_eq!(eng.ball_pull_at("users.age", a as u64, *t), want, "回 {k} t={t} age={a}");
        }
        for &e in &all_eids {
            let (c, a) = rs.get(&e).copied().unwrap_or((None, None));
            assert_eq!(eng.ball_value_at(e, "users.city", *t), c.map(|c| eng.vocab_id(c).unwrap() as u64), "回 {k} e={e}");
            assert_eq!(eng.ball_value_at(e, "users.age", *t), a.map(|a| a as u64), "回 {k} e={e}");
        }
    }
    // 今の時刻の答え = 今の読み
    let now = eng.ball_now();
    for c in CITIES {
        let mut cur = users.where_eq("city", c).find().unwrap();
        cur.sort_unstable();
        assert_eq!(eng.ball_pull_at("users.city", eng.vocab_id(c).unwrap() as u64, now), cur, "今の {c}");
    }
    // 値の変わった書き込み (書き換え・外す・消す・足す) は全部、 デッキの差分の帳面に届いた
    assert_eq!(eng.ball_diff_count("users.city"), Some(city_d));
    assert_eq!(eng.ball_diff_count("users.age"), Some(age_d));
    let (faces, _bytes) = eng.ball_stats("users.city").unwrap();
    assert!(faces > 200, "書き換えの面が積まれている ({faces})");
    drop(users);
    drop(db);
    cleanup(&path);
}

#[test]
fn leaf_columns_cannot_be_balls() {
    let path = tmp_path("leaf");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").column("memo", enchudb_schema::ColumnType::Leaf).primary_key("id").build().unwrap();
    assert!(db.engine().declare_ball("t.memo").is_err());
    assert!(db.engine().declare_ball("t.nope").is_err());
    drop(db);
    cleanup(&path);
}

/// 書き手 4 本が自分の row 16 個の a (値 0..6) を書いたり外したりする (engine の `tie_to_by_id` / `untie_by_id`)。 回ごとに
/// Barrier でそろえ、 そろった時刻と書き手の写しを控える。 全部の回の時刻で、 デッキのカードの答えと球の答えを写しと比べる
/// (同じ値の帳面に書き手が並んで書き足す・帳面を伸ばして写す、 を並べて通す)。
#[test]
fn concurrent_writers_leave_history_in_the_ledgers() {
    const TH: usize = 4;
    const PER: usize = 16;
    const VALS: u64 = 6;
    let path = tmp_path("conc");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let mut eids = Vec::new();
    for i in 0..(TH * PER) as i64 {
        eids.push(t.insert().set("id", i).commit().unwrap());
    }
    let eng = db.arc_engine();
    assert!(eng.declare_ball("t.a").unwrap());
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let rounds = 200;
    let bar = std::sync::Barrier::new(TH + 1);
    let diffs = std::sync::atomic::AtomicU64::new(0);
    let log = std::sync::Mutex::new(vec![None::<u64>; TH * PER]);
    let mut checks: Vec<(u64, Vec<Option<u64>>)> = Vec::new();
    std::thread::scope(|s| {
        for th in 0..TH {
            let (eng, bar, log, eids, diffs) = (&eng, &bar, &log, &eids, &diffs);
            s.spawn(move || {
                let mut r = Rng(0x9E37_79B9_7F4A_7C15 ^ th as u64);
                let mut mine = vec![None::<u64>; PER];
                for _ in 0..rounds {
                    for _ in 0..30 {
                        let k = r.below(PER as u64) as usize;
                        let e = eids[th * PER + k];
                        let new = match r.below(VALS + 1) {
                            VALS => {
                                eng.untie_by_id(e, ha);
                                None
                            }
                            v => {
                                eng.tie_to_by_id(e, ha, v as u32);
                                Some(v)
                            }
                        };
                        if mine[k] != new {
                            diffs.fetch_add(u64::from(mine[k].is_some()) + u64::from(new.is_some()), std::sync::atomic::Ordering::Relaxed);
                        }
                        mine[k] = new;
                    }
                    log.lock().unwrap()[th * PER..(th + 1) * PER].copy_from_slice(&mine);
                    bar.wait();
                    bar.wait();
                }
            });
        }
        for _ in 0..rounds {
            bar.wait();
            checks.push((eng.ball_now(), log.lock().unwrap().clone()));
            bar.wait();
        }
    });
    for (k, (tm, want)) in checks.iter().enumerate() {
        for v in 0..VALS {
            let deck: Vec<u64> = (0..TH * PER).filter(|&i| want[i] == Some(v)).map(|i| eids[i]).collect();
            assert_eq!(eng.ball_pull_at("t.a", v, *tm), deck, "回 {k} t={tm} v={v}");
        }
        for i in 0..TH * PER {
            assert_eq!(eng.ball_value_at(eids[i], "t.a", *tm), want[i], "回 {k} e={}", eids[i]);
        }
    }
    // 今の読み (名簿) も合う
    let last = &checks.last().unwrap().1;
    for v in 0..VALS {
        let mut cur = eng.pull_raw("t.a", v as u32);
        cur.sort_unstable();
        let want: Vec<u64> = (0..TH * PER).filter(|&i| last[i] == Some(v)).map(|i| eids[i]).collect();
        assert_eq!(cur, want, "今の v={v}");
    }
    assert_eq!(eng.ball_diff_count("t.a"), Some(diffs.into_inner()));
    drop(t);
    drop(db);
    cleanup(&path);
}
