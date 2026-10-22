//! 差分の位置: `LiveQuery::poll_since(版)` / `snapshot()` (集合・OR・上位 k・集計) と、 名前付きの位置を DB に置く `_cursors`
//! (`Database::prepare_cursors` / `cursor_get` / `cursor_set`)。 版は process を跨いで単調 (開き直した後の前の版は `Truncated`)、
//! 記録の上限を越えた版も `Truncated`、 どちらも `snapshot` で取り直す。 `poll` / 束の poll で渡した差分も記録に残る。
//! 位置は普通の書き込みと同じく WAL に載る (子 process が閉じずに exit → 再生で戻る、 `child_entry` が入口)。

use enchudb_schema::{Answer, Cursor, Database, LiveCounts, LiveDelta, Truncated, Value, CURSOR_ANSWER, CURSOR_CHANGEFEED};
use std::collections::BTreeMap;
use std::process::Command;

const CHILD_ENV: &str = "ENCHU_CURSORS_CHILD";
const OPLOG: usize = 8 << 20;

fn tmp_path(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("enchu-cursors-{tag}-{}", std::process::id()));
    let _ = enchudb_engine::db_files::remove_db(&p);
    p.to_str().unwrap().to_string()
}

/// companies (id, city) × 3 と users (id, age, company) × `n` (age は 10..=39)。 `_cursors` も作る。
fn make(path: &str, n: i64) -> Database {
    let mut db = Database::create(path).unwrap();
    db.prepare_cursors().unwrap();
    db.prepare_cursors().unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("users").number("id").number("age").ref_to("company", "companies").primary_key("id").build().unwrap();
    let companies = db.get_table("companies").unwrap();
    let cs: Vec<u64> = ["Tokyo", "Osaka", "Tokyo"]
        .iter()
        .enumerate()
        .map(|(i, c)| companies.insert().set("id", i as i64).set("city", *c).commit().unwrap())
        .collect();
    let users = db.get_table("users").unwrap();
    for i in 0..n {
        users.insert().set("id", i).set("age", 10 + i % 30).set("company", Value::Ref(cs[(i % 3) as usize])).commit().unwrap();
    }
    db
}

fn sorted(mut v: Vec<u64>) -> Vec<u64> {
    v.sort_unstable();
    v
}

fn delta(removed: Vec<u64>, added: Vec<u64>) -> LiveDelta {
    LiveDelta { added: sorted(added), removed: sorted(removed) }
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => panic!("group の値が文字でない: {other:?}"),
    }
}

fn counts_of(rows: Vec<(Value, u64, i128)>) -> BTreeMap<String, u64> {
    rows.into_iter().map(|(v, n, _)| (text(&v), n)).collect()
}

fn insert_user(db: &Database, id: i64, age: i64, company: i64) -> u64 {
    let c = db.get_table("companies").unwrap().where_eq("id", company).find_one().unwrap().unwrap();
    db.get_table("users").unwrap().insert().set("id", id).set("age", age).set("company", Value::Ref(c)).commit().unwrap()
}

/// 集合 (age > 30) と OR (age < 15 または age > 35) の宣言された答え: `snapshot` が全体と版、 `poll_since(版)` が entity ごとに
/// 最初と最後の出入りで組んだ差分 (入って出た row は出ない、 出て入り直した row は両方)、 `poll` で渡した差分も記録に残る。
#[test]
fn poll_since_composes_the_moves_since_a_version() {
    let path = tmp_path("compose");
    let db = make(&path, 30);
    let users = db.get_table("users").unwrap();
    let adults = users.all().where_gt("age", 30i64).answer("adults").unwrap();
    let edges = users.all().where_lt("age", 15i64).or(users.all().where_gt("age", 35i64)).answer("edges").unwrap();
    for (q, want) in [
        (&adults, users.all().where_gt("age", 30i64).find().unwrap()),
        (&edges, users.all().where_lt("age", 15i64).or(users.all().where_gt("age", 35i64)).find().unwrap()),
    ] {
        let (all, v0) = q.snapshot();
        assert_eq!(all, sorted(want), "snapshot は find と同じ");
        assert_eq!(q.poll_since(v0), Ok((LiveDelta::default(), v0)), "今の版からは空");
        assert!(q.poll().is_empty(), "snapshot の後の poll は空");
    }
    let (_, a0) = adults.snapshot();
    let (_, e0) = edges.snapshot();

    // 2 行入る
    let u100 = insert_user(&db, 100, 40, 0);
    let u101 = insert_user(&db, 101, 36, 1);
    let (d, a1) = adults.poll_since(a0).unwrap();
    assert_eq!(d, delta(vec![], vec![u100, u101]));
    assert!(a1 > a0);
    assert_eq!(adults.poll_since(a1), Ok((LiveDelta::default(), a1)));
    assert_eq!(edges.poll_since(e0).unwrap().0, delta(vec![], vec![u100, u101]));
    let (_, e1) = edges.snapshot();

    // u100 が出る、 u102 が入って出る (= 何も無し)、 u103 が入る
    users.entity(u100).set("age", 20i64).commit().unwrap();
    let u102 = insert_user(&db, 102, 50, 2);
    users.entity(u102).delete().unwrap();
    let u103 = insert_user(&db, 103, 12, 0);
    let (d, a2) = adults.poll_since(a1).unwrap();
    assert_eq!(d, delta(vec![u100], vec![]), "入って出た u102 は出ない");
    assert_eq!(adults.poll_since(a0), Ok((delta(vec![], vec![u101]), a2)), "a0 からは u100 (入って出た) も消える");
    let (d, e2) = edges.poll_since(e1).unwrap();
    assert_eq!(d, delta(vec![u100], vec![u103]), "OR の購読も同じ決まり (u103 は age < 15 の枝)");

    // u100 が入り直す (出て入った = removed と added の両方)、 `poll` で渡した分も記録に残る
    users.entity(u100).set("age", 33i64).commit().unwrap();
    assert_eq!(adults.poll(), delta(vec![], vec![u100]));
    let u104 = insert_user(&db, 104, 31, 1);
    let (d, a3) = adults.poll_since(a1).unwrap();
    assert_eq!(d, delta(vec![u100], vec![u100, u104]), "a1 の時点で居た u100 が出て入り直した");
    assert_eq!(adults.poll_since(a2), Ok((delta(vec![], vec![u100, u104]), a3)), "a2 の時点では居なかった = added だけ");
    assert_eq!(adults.poll_since(a0), Ok((delta(vec![], vec![u100, u101, u104]), a3)));
    assert_eq!(edges.poll_since(e2).unwrap().0, delta(vec![], vec![]), "age 33 は OR のどちらの枝にも入らない");

    // removed → added の順に積めば今の find と同じ
    let (all, a4) = adults.snapshot();
    assert_eq!(a4, a3, "snapshot は差分が無ければ版を進めない");
    assert_eq!(all, sorted(users.all().where_gt("age", 30i64).find().unwrap()));
    assert!(adults.poll_since(a4 + 1).is_err(), "先の版は組めない");
    assert_eq!(adults.poll_since(a4 + 1), Err(Truncated { from: a4 }));
    drop((adults, edges));
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 上位 k 件の答えと、 束 (`LiveGroup`) の poll で渡した差分: どちらも `poll_since` の記録に残る。
#[test]
fn top_k_and_group_polls_are_in_the_record() {
    let path = tmp_path("topk");
    let db = make(&path, 30);
    let users = db.get_table("users").unwrap();
    let top = users.where_eq("company.city", "Tokyo").order_by_desc("age").limit(3).answer("top").unwrap();
    let (all, v0) = top.snapshot();
    assert_eq!(all, sorted(top.ranked()), "snapshot は今の先頭 3 件 (eid 昇順)");
    let group = db.live_group();
    group.add(&top);
    // age 39 の Tokyo の行が入り、 3 位が出る
    let before = top.ranked();
    let u = insert_user(&db, 100, 39, 0);
    let polled = group.poll();
    assert_eq!(polled.len(), 1, "束の poll が差分を取り出す");
    assert_eq!(polled[0].1, delta(vec![before[2]], vec![u]));
    assert!(top.poll().is_empty(), "束が取り出した差分は poll には来ない");
    let (d, v1) = top.poll_since(v0).unwrap();
    assert_eq!(d, delta(vec![before[2]], vec![u]), "束で渡した差分も poll_since の記録に残る");
    assert!(v1 > v0);
    drop(group);
    drop(top);
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 集計の答え: `poll_since` は版以降に動いた group と今の (件数, 合計) (動いて戻った group も今の値で届く)、 `snapshot` は全 group。
#[test]
fn counts_poll_since_reports_the_groups_that_moved() {
    let path = tmp_path("counts");
    let db = make(&path, 30);
    let users = db.get_table("users").unwrap();
    let by_city = users.all().where_gt("age", 20i64).answer_counts("by_city", "company.city").unwrap();
    let (all, v0) = by_city.snapshot();
    let want: BTreeMap<String, u64> =
        counts_of(users.all().where_gt("age", 20i64).subscribe_counts("company.city").unwrap().snapshot().0);
    assert_eq!(counts_of(all.clone()), want);
    let (tokyo, osaka) = (want["Tokyo"], want["Osaka"]);
    assert!(by_city.poll().is_empty(), "snapshot の後の poll は空");
    assert_eq!(by_city.poll_since(v0).unwrap(), (vec![], v0));

    // Osaka が 1 つ増える
    let u = insert_user(&db, 100, 25, 1);
    let (d, v1) = by_city.poll_since(v0).unwrap();
    assert_eq!(counts_of(d), BTreeMap::from([("Osaka".to_string(), osaka + 1)]), "動いた group だけ");
    // 会社を Tokyo に移す (両方動く)、 さらに Osaka に戻す (= v1 からは Osaka は動いて戻った、 Tokyo は動いて戻った)
    let tokyo_c = db.get_table("companies").unwrap().where_eq("id", 0i64).find_one().unwrap().unwrap();
    users.entity(u).set("company", Value::Ref(tokyo_c)).commit().unwrap();
    let (d, v2) = by_city.poll_since(v1).unwrap();
    assert_eq!(counts_of(d), BTreeMap::from([("Osaka".to_string(), osaka), ("Tokyo".to_string(), tokyo + 1)]));
    let osaka_c = db.get_table("companies").unwrap().where_eq("id", 1i64).find_one().unwrap().unwrap();
    users.entity(u).set("company", Value::Ref(osaka_c)).commit().unwrap();
    let (d, v3) = by_city.poll_since(v1).unwrap();
    assert_eq!(
        counts_of(d),
        BTreeMap::from([("Osaka".to_string(), osaka + 1), ("Tokyo".to_string(), tokyo)]),
        "動いて戻った group も今の値で届く (値で上書きすれば今の表)"
    );
    assert!(v3 > v2 && v2 > v1 && v1 > v0);
    assert_eq!(by_city.poll_since(v3).unwrap(), (vec![], v3));
    let (all, v4) = by_city.snapshot();
    assert_eq!(v4, v3);
    assert_eq!(counts_of(all), BTreeMap::from([("Osaka".to_string(), osaka + 1), ("Tokyo".to_string(), tokyo)]));
    drop(by_city);
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 記録の上限を越えたら古い差分から捨て、 その前の版は `Truncated { from }` (`from` から先は組める)。 `snapshot` で取り直す。
#[test]
fn versions_dropped_from_the_record_are_truncated() {
    let path = tmp_path("truncated");
    let db = make(&path, 30);
    // 1 行の差分 = 8 byte、 上限 40 byte = 5 つまで (宣言より前に設定する: 記録は宣言した時に始まる)
    db.engine().set_answer_log_limit(40);
    let users = db.get_table("users").unwrap();
    let adults = users.all().where_gt("age", 30i64).answer("adults").unwrap();
    let (_, v0) = adults.snapshot();
    let mut versions = vec![v0];
    for i in 0..8 {
        insert_user(&db, 100 + i, 40, 0);
        versions.push(adults.poll_since(*versions.last().unwrap()).unwrap().1);
    }
    let last = *versions.last().unwrap();
    let err = adults.poll_since(v0).unwrap_err();
    assert!(err.from > v0 && err.from < last, "捨てた分の先の版: {err:?} (v0 {v0}, last {last})");
    assert_eq!(adults.poll_since(err.from - 1), Err(err), "from より前はどこも同じ Truncated");
    let (d, v) = adults.poll_since(err.from).unwrap();
    assert_eq!(v, last);
    assert_eq!(d.added.len() as u64, last - err.from, "from から先は 1 版 1 行");
    assert_eq!(adults.poll_since(last + 1), Err(Truncated { from: last }), "先の版は今の版を from に");
    let (all, v) = adults.snapshot();
    assert_eq!(v, last);
    assert_eq!(all, sorted(users.all().where_gt("age", 30i64).find().unwrap()));
    assert_eq!(adults.poll_since(v), Ok((LiveDelta::default(), v)));
    drop(adults);
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 宣言された答えは宣言した時 (開く時) から記録する: 最初の `poll` (種) も `poll_since` で組める。 集計も同じ。
#[test]
fn declared_answers_record_from_the_declaration() {
    let path = tmp_path("declared");
    let db = make(&path, 30);
    let users = db.get_table("users").unwrap();
    let adults = users.all().where_gt("age", 30i64).answer("adults").unwrap();
    let seed = adults.poll();
    assert!(!seed.is_empty());
    let err = adults.poll_since(0).unwrap_err();
    assert!(err.from > 0);
    let (d, v) = adults.poll_since(err.from).unwrap();
    assert_eq!(d, seed, "宣言の直後の poll (種) が記録の最初");
    assert_eq!(v, err.from + 1);
    let by_city = users.all().answer_counts("by_city", "company.city").unwrap();
    let seed = counts_of(by_city.poll_sums());
    let err = by_city.poll_since(0).unwrap_err();
    let (d, v) = by_city.poll_since(err.from).unwrap();
    assert_eq!(counts_of(d), seed);
    assert_eq!(v, err.from + 1);
    drop((adults, by_city));
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// `subscribe` の購読は最初の `poll_since` / `snapshot` から記録する (その前に `poll` で渡した差分は組めない)。
#[test]
fn subscriptions_start_recording_at_the_first_poll_since() {
    let path = tmp_path("subscribe");
    let db = make(&path, 30);
    let users = db.get_table("users").unwrap();
    let q = users.all().where_gt("age", 30i64).subscribe().unwrap();
    let seed = q.poll();
    assert_eq!(seed.added.len(), users.all().where_gt("age", 30i64).count().unwrap());
    let err = q.poll_since(0).unwrap_err();
    assert!(err.from > 0);
    let u = insert_user(&db, 100, 40, 0);
    let (d, v) = q.poll_since(err.from).unwrap();
    assert_eq!(d, delta(vec![], vec![u]), "記録を始めた後の差分は組める");
    assert!(v > err.from);
    let (all, v2) = q.snapshot();
    assert_eq!(v2, v);
    assert_eq!(all.len(), seed.added.len() + 1);
    drop(q);
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// `_cursors`: 置いた位置がそのまま読める (HLC の有無も)、 名前ごとに別、 無い名前は None、 開き直しても残る、 `_cursors` は
/// user の表の一覧に出ない。 作っていない DB では `cursor_set` は `BadValue`、 `cursor_get` は None。
#[test]
fn cursors_round_trip_and_survive_a_reopen() {
    let path = tmp_path("cursors");
    let hlc = enchudb_oplog::Hlc { wall: 1_700_000_000_123, logical: 7, peer: 3 };
    {
        let db = make(&path, 3);
        assert!(db.cursors_ready());
        assert!(!db.list_tables().iter().any(|t| t.name == "_cursors"), "_cursors は user の表ではない");
        assert_eq!(db.cursor_get("feed"), None);
        let c1 = Cursor { kind: CURSOR_CHANGEFEED, pos: 17, hlc: Some(hlc) };
        db.cursor_set("feed", c1).unwrap();
        assert_eq!(db.cursor_get("feed"), Some(c1));
        let c2 = Cursor { kind: CURSOR_ANSWER, pos: u64::MAX - 1, hlc: None };
        db.cursor_set("adults", c2).unwrap();
        assert_eq!(db.cursor_get("adults"), Some(c2));
        assert_eq!(db.cursor_get("feed"), Some(c1), "名前ごとに別");
        // 書き換え (HLC を外す) と、 不正な値
        db.cursor_set("feed", Cursor { kind: 9, pos: 18, hlc: None }).unwrap();
        assert_eq!(db.cursor_get("feed"), Some(Cursor { kind: 9, pos: 18, hlc: None }));
        assert!(db.cursor_set("", c1).is_err());
        assert!(db.cursor_set("x", Cursor { pos: u64::MAX, ..c1 }).is_err());
        assert_eq!(db.cursor_get("x"), None, "拒否した名前は行を作らない");
    }
    let db = Database::open(&path).unwrap();
    assert!(db.cursors_ready(), "開き直しても _cursors は在る");
    assert_eq!(db.cursor_get("feed"), Some(Cursor { kind: 9, pos: 18, hlc: None }));
    assert_eq!(db.cursor_get("adults"), Some(Cursor { kind: CURSOR_ANSWER, pos: u64::MAX - 1, hlc: None }));
    db.cursor_set("feed", Cursor { kind: 9, pos: 19, hlc: Some(hlc) }).unwrap();
    assert_eq!(db.cursor_get("feed"), Some(Cursor { kind: 9, pos: 19, hlc: Some(hlc) }));
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);

    let path = tmp_path("no-cursors");
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").primary_key("id").build().unwrap();
    assert!(!db.cursors_ready());
    assert_eq!(db.cursor_get("feed"), None);
    assert!(matches!(db.cursor_set("feed", Cursor { kind: 1, pos: 1, hlc: None }), Err(enchudb_schema::SchemaError::BadValue(_))));
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 開き直した後: 前の process の版は `Truncated` (版は process を跨いで単調)、 `snapshot` で取り直して位置を置き直す。
#[test]
fn versions_from_a_previous_open_are_truncated() {
    let path = tmp_path("reopen");
    let (v0, n0) = {
        let db = make(&path, 30);
        let users = db.get_table("users").unwrap();
        let adults = users.all().where_gt("age", 30i64).answer("adults").unwrap();
        let by_city = users.all().answer_counts("by_city", "company.city").unwrap();
        let (all, v0) = adults.snapshot();
        let (_, c0) = by_city.snapshot();
        db.cursor_set("adults", Cursor { kind: CURSOR_ANSWER, pos: v0, hlc: None }).unwrap();
        db.cursor_set("by_city", Cursor { kind: CURSOR_ANSWER, pos: c0, hlc: None }).unwrap();
        (v0, all.len())
    };
    let db = Database::open(&path).unwrap();
    let pos = db.cursor_get("adults").unwrap().pos;
    assert_eq!(pos, v0);
    let adults = db.answer("adults").and_then(Answer::into_query).unwrap();
    let err = adults.poll_since(pos).unwrap_err();
    assert!(err.from > v0, "開き直した後の版の始まりは前の版より大きい: {} <= {v0}", err.from);
    let (all, v1) = adults.snapshot();
    assert_eq!(all.len(), n0);
    assert!(v1 >= err.from && v1 > v0);
    db.cursor_set("adults", Cursor { kind: CURSOR_ANSWER, pos: v1, hlc: None }).unwrap();
    let u = insert_user(&db, 100, 40, 0);
    assert_eq!(adults.poll_since(db.cursor_get("adults").unwrap().pos).unwrap().0, delta(vec![], vec![u]));
    let by_city: LiveCounts = db.answer("by_city").and_then(Answer::into_counts).unwrap();
    let c_pos = db.cursor_get("by_city").unwrap().pos;
    assert!(by_city.poll_since(c_pos).is_err(), "集計の版も process を跨いで単調");
    let (all, _) = by_city.snapshot();
    assert_eq!(counts_of(all), counts_of(users_counts(&db)));
    drop((adults, by_city));
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

fn users_counts(db: &Database) -> Vec<(Value, u64, i128)> {
    db.get_table("users").unwrap().all().subscribe_counts("company.city").unwrap().snapshot().0
}

/// 閉じずに落ちた後: 処理の書き込みの後に置いた位置は、 その書き込みと一緒に WAL から戻る (子 process が oplog 付きで開いて
/// 行を足し、 位置を置いて、 閉じずに exit する)。
#[test]
fn a_cursor_written_after_the_rows_is_replayed_with_them() {
    let path = tmp_path("replay");
    let n0 = {
        let db = make(&path, 30);
        db.get_table("users").unwrap().all().count().unwrap()
    };
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["child_entry", "--exact", "--nocapture"])
        .env(CHILD_ENV, &path)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(out.status.success() && text.contains("CHILD_WROTE"), "子 process が書けなかった: {text}{}", String::from_utf8_lossy(&out.stderr));
    let db = Database::open_with_oplog(&path, OPLOG).unwrap();
    assert_eq!(db.cursor_get("feed"), Some(Cursor { kind: CURSOR_CHANGEFEED, pos: 5, hlc: None }), "位置が再生で戻る");
    assert_eq!(db.get_table("users").unwrap().all().count().unwrap(), n0 + 5, "位置より前に書いた行も戻っている");
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 子 process の入口 (環境変数が無ければ何もしない)。 oplog 付きで開いて 5 行足し、 位置を置き、 閉じずに exit する。
#[test]
fn child_entry() {
    let Ok(path) = std::env::var(CHILD_ENV) else { return };
    let db = Database::open_with_oplog(&path, OPLOG).unwrap();
    for i in 0..5i64 {
        insert_user(&db, 200 + i, 50, 1);
    }
    db.cursor_set("feed", Cursor { kind: CURSOR_CHANGEFEED, pos: 5, hlc: None }).unwrap();
    db.engine().oplog_sync().unwrap();
    println!("CHILD_WROTE");
    std::process::exit(0);
}
