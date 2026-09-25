//! 実際の Kafka に繋ぐ結合テスト。 `ENCHU_KAFKA=host:port` がある時だけ走る (無ければ何もせず通る)。
//!
//! ```sh
//! docker run -d --name enchu-kafka-test -p 19092:9092 \
//!   -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
//!   -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
//!   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
//!   -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
//!   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
//!   -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
//!   apache/kafka:latest
//! ENCHU_KAFKA=localhost:19092 cargo test -p enchudb-kafka --test kafka
//! ```

use enchudb_connect::{prepare, Ingest, JsonRows, LiveExport, OutMessage, Sink, Source};
use enchudb_kafka::{ensure_topic, KafkaSink, KafkaSource};
use enchudb_schema::Database;
use serde_json::{json, Value as Json};
use std::collections::BTreeSet;

fn brokers() -> Option<Vec<String>> {
    match std::env::var("ENCHU_KAFKA") {
        Ok(b) => Some(b.split(',').map(str::to_string).collect()),
        Err(_) => {
            eprintln!("ENCHU_KAFKA が無いので Kafka の結合テストを飛ばす");
            None
        }
    }
}

fn produce(brokers: &[String], topic: &str, rows: impl IntoIterator<Item = (i64, Json)>) {
    let mut sink = KafkaSink::connect(brokers.to_vec(), topic).unwrap();
    let msgs: Vec<OutMessage> = rows
        .into_iter()
        .map(|(id, row)| OutMessage {
            key: id.to_string().into_bytes(),
            payload: json!({"table": "users", "row": row}).to_string().into_bytes(),
        })
        .collect();
    sink.send(&msgs).unwrap();
}

/// 全部読めるまで回す (Kafka は書いた直後に見えないことがある)。
fn ingest_all(ing: &Ingest<JsonRows>, src: &mut KafkaSource, want: usize) -> (usize, usize) {
    let (mut applied, mut replayed) = (0, 0);
    for _ in 0..200 {
        let r = ing.run_once(src, 1000).unwrap();
        assert!(r.rejected.is_empty(), "{:?}", r.rejected);
        applied += r.applied;
        replayed += r.replayed;
        if applied + replayed >= want {
            break;
        }
    }
    (applied, replayed)
}

/// Kafka の topic → enchu (主キーで upsert、 位置は DB) → live 購読の差分 → 別の topic。 開き直しても
/// 続きから読み、 先頭から読み直しても当てた位置は飛ばす。
#[test]
fn kafka_in_live_out_and_resume() {
    let Some(brokers) = brokers() else { return };
    let tag = format!("{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
    let (tin, tout) = (format!("enchu-in-{tag}"), format!("enchu-out-{tag}"));
    ensure_topic(brokers.clone(), &tin, 3, 1).unwrap();
    ensure_topic(brokers.clone(), &tout, 3, 1).unwrap();

    let cities = ["Tokyo", "Osaka", "Kyoto"];
    produce(&brokers, &tin, (0..300i64).map(|i| (i, json!({"id": i, "age": i % 70, "city": cities[(i % 3) as usize]}))));
    // 同じ key の 2 通目 (同じ partition に後から入る = 後勝ち)
    produce(&brokers, &tin, (0..30i64).map(|i| (i * 3, json!({"id": i * 3, "city": "Osaka"}))));

    let path = format!("/tmp/enchu-kafka-{tag}.db");
    let mut db = Database::create_growable_with_capacity(&path, 4096).unwrap();
    db.table("users").number("id").number("age").tag("city").primary_key("id").build().unwrap();
    prepare(&mut db).unwrap();
    let users = db.get_table("users").unwrap();
    let mut ex = LiveExport::new(&db);
    ex.add("tokyo", "users", users.where_eq("city", "Tokyo").subscribe().unwrap()).unwrap();
    let mut out = KafkaSink::connect(brokers.clone(), &tout).unwrap();

    {
        let ing = Ingest::new(&db, JsonRows).unwrap();
        let mut src = KafkaSource::connect(brokers.clone(), &tin).unwrap();
        ing.resume(&mut src).unwrap();
        assert_eq!(ingest_all(&ing, &mut src, 330), (330, 0));
        ex.pump(&mut out).unwrap();
    }
    // 0, 3, 6, .. (Tokyo だった) の先頭 30 本は Osaka に書き換わった
    let tokyo: BTreeSet<i64> = (0..300).filter(|i| i % 3 == 0 && *i >= 90).collect();
    let got: BTreeSet<i64> = users
        .where_eq("city", "Tokyo")
        .find()
        .unwrap()
        .into_iter()
        .map(|e| match users.entity(e).get("id") {
            Some(enchudb_schema::Value::Number(n)) => n,
            v => panic!("{v:?}"),
        })
        .collect();
    assert_eq!(got, tokyo, "同じ key の後の値が勝っていない");

    // 出口の topic を読んで、 live 購読の add が主キーで届いている
    let mut read = KafkaSource::connect(brokers.clone(), &tout).unwrap();
    let mut added = BTreeSet::new();
    for _ in 0..100 {
        for m in read.fetch(1000).unwrap() {
            let p: Json = serde_json::from_slice(m.payload.as_deref().unwrap()).unwrap();
            assert_eq!(p["op"], "add");
            added.insert(p["key"].as_i64().unwrap());
        }
        if added.len() >= tokyo.len() {
            break;
        }
    }
    assert_eq!(added, tokyo);

    // 開き直し: 続きの 10 件だけ当たる
    produce(&brokers, &tin, (300..310i64).map(|i| (i, json!({"id": i, "city": "Tokyo"}))));
    let ing = Ingest::new(&db, JsonRows).unwrap();
    let mut src = KafkaSource::connect(brokers.clone(), &tin).unwrap();
    ing.resume(&mut src).unwrap();
    assert_eq!(ingest_all(&ing, &mut src, 10), (10, 0), "続きから読んでいない");
    // 先頭から読み直すと、 当てた位置は全部飛ばす
    let mut again = KafkaSource::connect(brokers.clone(), &tin).unwrap();
    assert_eq!(ingest_all(&ing, &mut again, 340), (0, 340));
    ex.pump(&mut out).unwrap();
    assert_eq!(users.where_eq("city", "Tokyo").count().unwrap(), tokyo.len() + 10);

    drop(ex);
    drop(db);
    for s in ["", ".oplog", ".tables", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{path}{s}"));
    }
}
