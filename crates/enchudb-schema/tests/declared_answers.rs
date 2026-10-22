//! 宣言された答え (`Query::answer` / `answer_counts` / `answer_sums`、 `Database::answer`): 宣言は `{db}/tables` に残り、 開き直すと
//! engine が購読を張り直して名前で取れる。 最初の poll が種 (登録時点の全件 / 全 group)、 以後は差分。 閉じずに落ちた後 (WAL の
//! 再生) も再生された行が答えに入る (子 process が閉じずに exit する、 `child_entry` が入口)。 同じ名前に同じ形は同じ購読、 違う形は
//! 置き換え、 外すと消える。 readonly で開いても取れる。

use enchudb_schema::{Answer, Database, LiveCounts, Value};
use std::collections::BTreeMap;
use std::process::Command;

const CHILD_ENV: &str = "ENCHU_DECLARED_ANSWERS_CHILD";
const OPLOG: usize = 8 << 20;

fn tmp_path(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("enchu-declared-answers-{tag}-{}", std::process::id()));
    let _ = enchudb_engine::db_files::remove_db(&p);
    p.to_str().unwrap().to_string()
}

/// companies (id, city) × 3 と users (id, age, score (BigInt), company) × `n`。 age は 10..=39、 score は負 (合計も負)
fn make(path: &str, n: i64) -> Database {
    let mut db = Database::create(path).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .bigint("score")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    let companies = db.get_table("companies").unwrap();
    let cs: Vec<u64> = ["Tokyo", "Osaka", "Tokyo"]
        .iter()
        .enumerate()
        .map(|(i, c)| companies.insert().set("id", i as i64).set("city", *c).commit().unwrap())
        .collect();
    let users = db.get_table("users").unwrap();
    for i in 0..n {
        users
            .insert()
            .set("id", i)
            .set("age", 10 + i % 30)
            .set("score", i * 1000 - 20_000)
            .set("company", Value::Ref(cs[(i % 3) as usize]))
            .commit()
            .unwrap();
    }
    db
}

fn sorted(mut v: Vec<u64>) -> Vec<u64> {
    v.sort_unstable();
    v
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => panic!("group の値が文字でない: {other:?}"),
    }
}

fn counts_map(c: &LiveCounts) -> BTreeMap<String, (u64, i128)> {
    c.poll_sums().into_iter().map(|(v, n, s)| (text(&v), (n, s))).collect()
}

/// 宣言 4 つ: 集合 / ref の先で group した件数 / BigInt の合計 / 上位 5 件。 返り値は宣言した時の答え (後で比べる)
fn declare_all(db: &Database) -> (Vec<u64>, BTreeMap<String, (u64, i128)>, BTreeMap<String, (u64, i128)>, Vec<u64>) {
    let users = db.get_table("users").unwrap();
    let adults = users.all().where_gt("age", 30i64).answer("adults").unwrap();
    let by_city = users.all().where_gt("age", 20i64).answer_counts("by_city", "company.city").unwrap();
    let score_by_city = users.all().answer_sums("score_by_city", "company.city", "score").unwrap();
    let top = users.where_eq("company.city", "Tokyo").order_by_desc("score").limit(5).answer("top").unwrap();
    let a = sorted(adults.poll().added);
    let b = counts_map(&by_city);
    let c = counts_map(&score_by_city);
    top.poll();
    let t = top.ranked();
    assert_eq!(a, sorted(users.all().where_gt("age", 30i64).find().unwrap()), "宣言した購読の最初の poll は find と同じ");
    assert_eq!(t, users.where_eq("company.city", "Tokyo").order_by_desc("score").limit(5).subscribe().unwrap().ranked());
    (a, b, c, t)
}

#[test]
fn declarations_survive_reopen_and_first_poll_seeds() {
    let path = tmp_path("reopen");
    let (adults0, by_city0, score0, top0) = {
        let db = make(&path, 30);
        let r = declare_all(&db);
        assert_eq!(db.answers(), vec!["adults", "by_city", "score_by_city", "top"]);
        r
    };
    assert!(!by_city0.is_empty() && by_city0.values().all(|(n, s)| *n > 0 && *s == 0), "件数だけの宣言の合計は 0");
    assert!(score0.values().any(|(_, s)| *s < 0), "負の合計も (BigInt)");

    let db = Database::open(&path).unwrap();
    assert_eq!(db.answers(), vec!["adults", "by_city", "score_by_city", "top"], "宣言は開き直すと戻る");
    assert!(db.answer("nope").is_none());
    assert!(db.answer("adults").unwrap().into_counts().is_none(), "集合の宣言は counts でない");
    let users = db.get_table("users").unwrap();
    let companies = db.get_table("companies").unwrap();

    // 集合: 最初の poll = 閉じる前の答え + 開いてから書いた行 (種は最初の poll が作る)
    let late = users.insert().set("id", 100i64).set("age", 35i64).set("score", 1i64).commit().unwrap();
    let adults = db.answer("adults").and_then(Answer::into_query).unwrap();
    let mut want = adults0.clone();
    want.push(late);
    assert_eq!(sorted(adults.poll().added), sorted(want));
    assert_eq!(adults.count(), adults0.len() + 1);
    users.entity(late).set("age", 20i64).commit().unwrap();
    let d = adults.poll();
    assert_eq!((d.added, d.removed), (vec![], vec![late]), "開き直した後の差分が届く");

    // ref の先で group した件数: 閉じる前と同じ + 会社の所在地の書き換えで group が動く
    let by_city = db.answer("by_city").and_then(Answer::into_counts).unwrap();
    assert_eq!(counts_map(&by_city), by_city0);
    let osaka = companies.where_eq("city", "Osaka").find_one().unwrap().unwrap();
    companies.entity(osaka).set("city", "Nagoya").commit().unwrap();
    let moved = counts_map(&by_city);
    assert_eq!(moved.get("Osaka").map(|x| x.0), Some(0), "Osaka の group が消えた");
    assert_eq!(moved.get("Nagoya").map(|x| x.0), by_city0.get("Osaka").map(|x| x.0), "Nagoya に移った");

    // BigInt の合計: 閉じる前と同じ (合計の列の型は table の定義から戻す。 late は会社が無いので group に入らない)
    let score_by_city = db.answer("score_by_city").and_then(Answer::into_counts).unwrap();
    let renamed: BTreeMap<String, (u64, i128)> =
        score0.iter().map(|(k, v)| (if k == "Osaka" { "Nagoya".to_string() } else { k.clone() }, *v)).collect();
    assert_eq!(counts_map(&score_by_city), renamed, "宣言された合計 = 閉じる前の合計 (Osaka は Nagoya に改名済み)");
    let u0 = users.where_eq("id", 0i64).find_one().unwrap().unwrap();
    users.entity(u0).set("score", 123_456i64).commit().unwrap();
    let d: BTreeMap<String, (u64, i128)> = score_by_city.poll_sums().into_iter().map(|(v, n, s)| (text(&v), (n, s))).collect();
    let fresh: BTreeMap<String, (u64, i128)> = counts_map(&users.all().subscribe_sums("company.city", "score").unwrap());
    assert_eq!(d.keys().collect::<Vec<_>>(), vec!["Tokyo"], "書き換えた行の group だけ届く");
    assert_eq!(d.get("Tokyo"), fresh.get("Tokyo"), "宣言された合計の差分 = 今張った合計");
    assert_eq!(fresh.get("Tokyo").map(|x| x.1), score0.get("Tokyo").map(|x| x.1 - (0 - 20_000) + 123_456));

    // 上位 5 件: 並びも戻る (上で u0 (Tokyo) の score を 123456 にしたので、 閉じる前の並びの先頭に u0 が入り 5 位が落ちる)
    let top = db.answer("top").and_then(Answer::into_query).unwrap();
    top.poll();
    let mut want = top0.clone();
    want.insert(0, u0);
    want.truncate(5);
    assert_eq!(top.ranked(), want);
    assert_eq!(top.ranked(), users.where_eq("company.city", "Tokyo").order_by_desc("score").limit(5).subscribe().unwrap().ranked());
    let best = users.insert().set("id", 101i64).set("age", 10i64).set("score", 1_000_000i64).set("company", Value::Ref(companies.where_eq("id", 0i64).find_one().unwrap().unwrap())).commit().unwrap();
    let d = top.poll();
    assert_eq!(d.added, vec![best]);
    assert_eq!(d.removed, vec![want[4]], "5 位が押し出される");
    assert_eq!(top.ranked()[0], best);
    drop((adults, by_city, score_by_city, top));
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn same_shape_is_the_same_subscription_different_shape_replaces_and_drop_removes() {
    let path = tmp_path("replace");
    {
        let db = make(&path, 30);
        let users = db.get_table("users").unwrap();
        let a = users.all().where_gt("age", 30i64).answer("adults").unwrap();
        let b = users.all().where_gt("age", 30i64).answer("adults").unwrap();
        assert_eq!(a.id(), b.id(), "同じ形は同じ購読");
        assert_eq!(db.answer("adults").and_then(Answer::into_query).map(|q| q.id()), Some(a.id()));
        let c = users.all().where_gt("age", 35i64).answer("adults").unwrap();
        assert_ne!(c.id(), a.id(), "違う形は置き換え");
        assert_eq!(db.answers(), vec!["adults"]);
        // 種類が変わっても置き換え
        let k = users.all().answer_counts("adults", "company.city").unwrap();
        assert_eq!(db.answer("adults").and_then(Answer::into_counts).map(|q| q.id()), Some(k.id()));
        let _ = users.all().where_gt("age", 35i64).answer("adults").unwrap();
        let _ = users.all().answer_counts("by_city", "company.city").unwrap();
        assert_eq!(db.answers(), vec!["adults", "by_city"]);
        assert!(db.drop_answer("by_city"));
        assert!(!db.drop_answer("by_city"));
        assert!(db.answer("by_city").is_none());
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.answers(), vec!["adults"], "外した宣言は戻らない");
    let users = db.get_table("users").unwrap();
    let adults = db.answer("adults").and_then(Answer::into_query).unwrap();
    assert_eq!(sorted(adults.poll().added), sorted(users.all().where_gt("age", 35i64).find().unwrap()), "置き換えた形で戻る");
    // 形の決まりは subscribe と同じ
    assert!(users.all().limit(3).answer("x").is_err(), "limit には order_by が要る");
    assert!(users.all().answer_counts("x", "nope").is_err());
    assert!(users.all().answer_sums("x", "company.city", "age").is_ok(), "Number の和");
    assert!(users.all().answer_sums("y", "company.city", "company").is_err(), "Ref の和は不可");
    drop(adults);
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn readonly_open_can_take_declared_answers() {
    let path = tmp_path("readonly");
    let adults0 = {
        let db = make(&path, 30);
        declare_all(&db).0
    };
    let db = Database::open_readonly(&path).unwrap();
    let adults = db.answer("adults").and_then(Answer::into_query).unwrap();
    assert_eq!(sorted(adults.poll().added), adults0);
    let by_city = db.answer("by_city").and_then(Answer::into_counts).unwrap();
    assert!(by_city.poll().iter().all(|(v, n)| matches!(v, Value::Text(_)) && *n > 0), "group の値は文字 (Tag)");
    drop((adults, by_city));
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 閉じずに落ちた後: WAL の再生で戻った行も宣言された答えに入る (購読は再生より前に張る)。 子 process が oplog 付きで開いて
/// 行を足し、 閉じずに exit する。
#[test]
fn rows_replayed_from_the_wal_reach_the_declared_answer() {
    let path = tmp_path("replay");
    let adults0 = {
        let db = make(&path, 30);
        declare_all(&db).0
    };
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["child_entry", "--exact", "--nocapture"])
        .env(CHILD_ENV, &path)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(out.status.success() && text.contains("CHILD_WROTE"), "子 process が書けなかった: {text}{}", String::from_utf8_lossy(&out.stderr));
    let db = Database::open_with_oplog(&path, OPLOG).unwrap();
    assert_eq!(db.answers(), vec!["adults", "by_city", "score_by_city", "top"]);
    let users = db.get_table("users").unwrap();
    let adults = db.answer("adults").and_then(Answer::into_query).unwrap();
    let got = sorted(adults.poll().added);
    let want = sorted(users.all().where_gt("age", 30i64).find().unwrap());
    assert_eq!(got, want, "再生で戻った行が答えに入る");
    assert_eq!(got.len(), adults0.len() + 5, "子が足した 5 行 (age 50) が再生で戻っている");
    let by_city = db.answer("by_city").and_then(Answer::into_counts).unwrap();
    assert_eq!(counts_map(&by_city), counts_map(&users.all().where_gt("age", 20i64).subscribe_counts("company.city").unwrap()));
    drop((adults, by_city));
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 子 process の入口 (環境変数が無ければ何もしない)。 oplog 付きで開いて 5 行足し、 閉じずに exit する。
#[test]
fn child_entry() {
    let Ok(path) = std::env::var(CHILD_ENV) else { return };
    let db = Database::open_with_oplog(&path, OPLOG).unwrap();
    let users = db.get_table("users").unwrap();
    let companies = db.get_table("companies").unwrap();
    let c = companies.where_eq("id", 1i64).find_one().unwrap().unwrap();
    for i in 0..5i64 {
        users.insert().set("id", 200 + i).set("age", 50i64).set("score", i).set("company", Value::Ref(c)).commit().unwrap();
    }
    db.engine().oplog_sync().unwrap();
    println!("CHILD_WROTE");
    std::process::exit(0);
}
