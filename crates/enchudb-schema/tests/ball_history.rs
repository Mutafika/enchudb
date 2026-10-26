//! 球 (試作、 `enchudb_engine::ball`、 履歴はデッキのカードのスピン = 向きの時刻に残す): schema の書き込み (insert / upsert / untie / delete) を回ごとに重ね、 回の終わりの時刻を
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
    for _round in 0..60 {
        for _ in 0..40 {
            let live: Vec<u64> = rows.keys().copied().collect();
            let e = live[r.below(live.len() as u64) as usize];
            match r.below(10) {
                0..=3 => {
                    let c = CITIES[r.below(4) as usize];
                    users.upsert().set("id", ids[&e]).set("city", c).commit().unwrap();
                    rows.get_mut(&e).unwrap().0 = Some(c);
                }
                4..=5 => {
                    let a = r.below(50) as i64;
                    users.upsert().set("id", ids[&e]).set("age", a).commit().unwrap();
                    rows.get_mut(&e).unwrap().1 = Some(a);
                }
                6 => {
                    eng.untie(e, "users.city");
                    rows.get_mut(&e).unwrap().0 = None;
                }
                7 => {
                    users.entity(e).delete().unwrap();
                    rows.remove(&e);
                    ids.remove(&e);
                }
                _ => {
                    let (c, a) = (CITIES[r.below(4) as usize], r.below(50) as i64);
                    let e = users.insert().set("id", next_id).set("city", c).set("age", a).commit().unwrap();
                    assert!(!rows.contains_key(&e));
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
    // 書いた値は全部、 デッキのカードが時刻を持つ値 (円柱の配列の中) で、 外れた時刻は全部書けた
    for c in CITIES {
        assert!(eng.ball_value_in_deck("users.city", eng.vocab_id(c).unwrap() as u64), "{c}");
    }
    assert!(eng.ball_value_in_deck("users.age", 49));
    assert_eq!(eng.ball_spin_misses(), 0);
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
