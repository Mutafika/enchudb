//! 入口 (行の変更の取り込み・読んだ位置) と出口 (live 購読の差分の書き出し)。

use enchudb_connect::memory::Topic;
use enchudb_connect::{prepare, Debezium, Ingest, JsonRows, LiveExport, Message, Position};
use enchudb_schema::{Database, Value};
use serde_json::{json, Value as Json};

fn tmp(name: &str) -> String {
    let p = format!("/tmp/enchu-connect-{name}-{}.db", std::process::id());
    cleanup(&p);
    p
}

fn cleanup(p: &str) {
    for suffix in ["", ".oplog", ".tables", ".eidmap", ".crc"] {
        let _ = std::fs::remove_file(format!("{p}{suffix}"));
    }
}

fn setup(path: &str) -> Database {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("companies").number("id").tag("name").tag("city").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .tag("name")
        .number("age")
        .tag("city")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    prepare(&mut db).unwrap();
    db
}

fn find(db: &Database, table: &str, id: i64) -> Option<u64> {
    db.get_table(table).unwrap().where_eq("id", id).find_one().unwrap()
}

fn get(db: &Database, table: &str, id: i64, col: &str) -> Option<Value> {
    let t = db.get_table(table).unwrap();
    t.entity(find(db, table, id)?).get(col)
}

fn msgs(payloads: &[Json]) -> Vec<Message> {
    payloads
        .iter()
        .enumerate()
        .map(|(i, p)| Message {
            position: Position { stream: "t".into(), partition: 0, offset: i as u64 },
            key: None,
            payload: Some(p.to_string().into_bytes()),
        })
        .collect()
}

/// upsert / 参照先の主キー (まだ無ければ主キーだけの row) / null で値を外す / delete / 壊れたメッセージは
/// 理由付きで弾いて流れは止めない。
#[test]
fn json_rows_apply_by_primary_key() {
    let path = tmp("rows");
    let db = setup(&path);
    let ing = Ingest::new(&db, JsonRows).unwrap();
    let r = ing.apply(&msgs(&[
        // 会社 10 はまだ来ていない
        json!({"table": "users", "row": {"id": 1, "name": "a", "age": 30, "company": 10, "extra": "読み捨て"}}),
        json!({"table": "users", "op": "upsert", "row": {"id": 2, "name": "b", "age": 40, "company": 10}}),
        json!({"table": "companies", "row": {"id": 10, "name": "acme", "city": "Tokyo"}}),
        json!({"table": "users", "row": {"id": 1, "age": null}}),
        json!({"table": "users", "op": "delete", "key": 2}),
        json!({"table": "nope", "row": {"id": 1}}),
        json!({"table": "users", "row": {"id": 3, "age": 5_000_000_000u64}}),
        json!({"table": "users", "row": {"name": "no key"}}),
        json!({"table": "users", "row": {"id": 4, "name": "d"}}),
    ]));
    assert_eq!(r.applied, 6, "{:?}", r.rejected);
    assert_eq!(r.rejected.iter().map(|x| x.0.offset).collect::<Vec<_>>(), vec![5, 6, 7]);
    // 参照先は 1 row だけ (主キーだけの row に本体が upsert された)
    assert_eq!(db.get_table("companies").unwrap().all().count().unwrap(), 1);
    assert_eq!(get(&db, "users", 1, "company"), Some(Value::Ref(find(&db, "companies", 10).unwrap())));
    assert_eq!(get(&db, "companies", 10, "city"), Some(Value::Text("Tokyo".into())));
    assert_eq!(get(&db, "users", 1, "age"), None, "null で値が外れていない");
    assert_eq!(get(&db, "users", 1, "name"), Some(Value::Text("a".into())), "row に無い列は触らない");
    assert_eq!(find(&db, "users", 2), None);
    assert!(find(&db, "users", 4).is_some(), "弾いたメッセージの後も流れが止まった");
    drop(db);
    cleanup(&path);
}

/// 読んだ位置は DB の中に残り、 開き直しても続きから読む。 再送された位置は飛ばす。
#[test]
fn offsets_survive_reopen_and_replays_are_skipped() {
    let path = tmp("offsets");
    let topic = Topic::new("orders");
    for i in 0..3 {
        topic.push(None, json!({"table": "users", "row": {"id": i, "age": i}}).to_string().as_bytes());
    }
    {
        let db = setup(&path);
        let ing = Ingest::new(&db, JsonRows).unwrap();
        let mut src = topic.source();
        ing.resume(&mut src).unwrap();
        let r = ing.run_once(&mut src, 100).unwrap();
        assert_eq!(r.applied, 3);
        assert_eq!(ing.next_offset("orders", 0), 3);
    }
    topic.push(None, json!({"table": "users", "row": {"id": 0, "age": 99}}).to_string().as_bytes());
    let db = Database::open(&path).unwrap();
    let ing = Ingest::new(&db, JsonRows).unwrap();
    assert_eq!(ing.next_offset("orders", 0), 3, "読んだ位置が開き直しで消えた");
    let mut src = topic.source();
    ing.resume(&mut src).unwrap();
    let r = ing.run_once(&mut src, 100).unwrap();
    assert_eq!((r.applied, r.replayed), (1, 0), "続きから読んでいない");
    assert_eq!(get(&db, "users", 0, "age"), Some(Value::Number(99)));
    // 先頭から再送されても、 当てた位置は飛ばす (id 0 の age が 0 に戻らない)
    let r = ing.run_once(&mut topic.source(), 100).unwrap();
    assert_eq!((r.applied, r.replayed), (0, 4));
    assert_eq!(get(&db, "users", 0, "age"), Some(Value::Number(99)));
    drop(db);
    cleanup(&path);
}

fn payloads(t: &Topic) -> Vec<Json> {
    t.messages().into_iter().map(|(_, p)| serde_json::from_slice(&p).unwrap()).collect()
}

/// live 購読の出入りが主キーと中身で届く (ref は参照先の主キー)。 削除で出た row も主キーで届く。
/// 集計の購読は動いた group の件数と合計。
#[test]
fn live_export_emits_rows_by_primary_key() {
    let path = tmp("export");
    let db = setup(&path);
    let users = db.get_table("users").unwrap();
    let mut ex = LiveExport::new(&db);
    ex.add("tokyo", "users", users.where_eq("city", "Tokyo").subscribe().unwrap()).unwrap();
    ex.add_counts("by_city", users.all().subscribe_sums("city", "age").unwrap());
    let out = Topic::new("out");
    let ing = Ingest::new(&db, JsonRows).unwrap();
    ing.apply(&msgs(&[
        json!({"table": "companies", "row": {"id": 10, "name": "acme"}}),
        json!({"table": "users", "row": {"id": 1, "name": "a", "age": 30, "city": "Tokyo", "company": 10}}),
        json!({"table": "users", "row": {"id": 2, "name": "b", "age": 20, "city": "Tokyo"}}),
        json!({"table": "users", "row": {"id": 3, "name": "c", "age": 5, "city": "Osaka"}}),
    ]));
    ex.pump(&mut out.sink()).unwrap();
    let mut got = payloads(&out);
    got.sort_by_key(|p| p.to_string());
    assert_eq!(
        got,
        vec![
            json!({"sub": "by_city", "group": "Osaka", "count": 1, "sum": 5}),
            json!({"sub": "by_city", "group": "Tokyo", "count": 2, "sum": 50}),
            json!({"sub": "tokyo", "op": "add", "key": 1,
                   "row": {"id": 1, "name": "a", "age": 30, "city": "Tokyo", "company": 10}}),
            json!({"sub": "tokyo", "op": "add", "key": 2, "row": {"id": 2, "name": "b", "age": 20, "city": "Tokyo"}}),
        ]
    );
    let out = Topic::new("out2");
    ing.apply(&[
        Message {
            position: Position { stream: "t".into(), partition: 0, offset: 10 },
            key: None,
            payload: Some(json!({"table": "users", "row": {"id": 1, "city": "Osaka"}}).to_string().into_bytes()),
        },
        Message {
            position: Position { stream: "t".into(), partition: 0, offset: 11 },
            key: None,
            payload: Some(json!({"table": "users", "op": "delete", "key": 2}).to_string().into_bytes()),
        },
    ]);
    ex.pump(&mut out.sink()).unwrap();
    let mut got = payloads(&out);
    got.sort_by_key(|p| p.to_string());
    assert_eq!(
        got,
        vec![
            json!({"sub": "by_city", "group": "Tokyo", "count": 0, "sum": 0}),
            json!({"sub": "by_city", "group": "Osaka", "count": 2, "sum": 35}),
            json!({"sub": "tokyo", "op": "remove", "key": 1}),
            json!({"sub": "tokyo", "op": "remove", "key": 2}),
        ]
    );
    // 同じ row の出入りは同じ key (Kafka では同じ partition に順に届く)
    assert_eq!(out.messages().iter().filter(|(k, _)| k == b"tokyo/1").count(), 1);
    drop(ex);
    drop(db);
    cleanup(&path);
}

/// Debezium の event: c / r / u は after の upsert、 d は before の主キーで削除、 tombstone と関係ない op は無視。
#[test]
fn debezium_events() {
    let path = tmp("debezium");
    let db = setup(&path);
    let ing = Ingest::new(&db, Debezium::with_table_map(|t| t.trim_start_matches("public_").to_string())).unwrap();
    let ev = |op: &str, before: Json, after: Json| {
        json!({"schema": {}, "payload": {"op": op, "before": before, "after": after,
               "source": {"db": "shop", "table": "public_users"}}})
    };
    let mut m = msgs(&[
        ev("r", Json::Null, json!({"id": 1, "name": "a", "age": 30})),
        ev("c", Json::Null, json!({"id": 2, "name": "b", "age": 40})),
        ev("u", json!({"id": 1, "name": "a", "age": 30}), json!({"id": 1, "name": "a", "age": 31})),
        ev("d", json!({"id": 2, "name": "b", "age": 40}), Json::Null),
        json!({"op": "t", "source": {"table": "public_users"}}),
    ]);
    m.push(Message { position: Position { stream: "t".into(), partition: 0, offset: 5 }, key: None, payload: None });
    let r = ing.apply(&m);
    assert!(r.rejected.is_empty(), "{:?}", r.rejected);
    assert_eq!(r.applied, 4);
    assert_eq!(get(&db, "users", 1, "age"), Some(Value::Number(31)));
    assert_eq!(find(&db, "users", 2), None);
    assert_eq!(ing.next_offset("t", 0), 6);
    drop(db);
    cleanup(&path);
}

/// prepare していない DB は分かる形で断る。
#[test]
fn ingest_needs_prepare() {
    let path = tmp("noprep");
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("users").number("id").primary_key("id").build().unwrap();
    assert!(Ingest::new(&db, JsonRows).err().unwrap().contains("prepare"));
    drop(db);
    cleanup(&path);
}
