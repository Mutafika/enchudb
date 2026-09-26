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

/// BigInt 列 (ms の時刻 / 負の主キー / 10 進の文字列) を Kafka から取り込み、 live の差分と合計 (u64 を
/// 超える合計は 10 進の文字列) を Kafka へ。 開き直しても値と読んだ位置が残る。
#[test]
fn kafka_bigint_round_trip() {
    let Some(brokers) = brokers() else { return };
    let tag = format!("{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
    let (tin, tout) = (format!("enchu-big-in-{tag}"), format!("enchu-big-out-{tag}"));
    ensure_topic(brokers.clone(), &tin, 3, 1).unwrap();
    ensure_topic(brokers.clone(), &tout, 3, 1).unwrap();
    let big = 9_223_372_036_854_775_000i64;
    let t0 = 1_790_000_000_000i64;
    let mut rows: Vec<(String, Json)> = (0..100i64).map(|i| (format!("{}", -i - 1), json!({"id": -i - 1, "at": t0 + i, "kind": "ms"}))).collect();
    rows.push(("min".into(), json!({"id": "9223372036854775806", "at": i64::MIN, "kind": "edge"})));
    rows.extend((0..3i64).map(|i| (format!("z{i}"), json!({"id": 1000 + i, "at": big, "kind": "z"}))));
    // 同じ key の 2 通目 (後勝ち)
    rows.push(("-1".into(), json!({"id": -1, "at": -7})));
    let mut sink = KafkaSink::connect(brokers.clone(), &tin).unwrap();
    let msgs: Vec<OutMessage> = rows
        .iter()
        .map(|(k, row)| OutMessage { key: k.clone().into_bytes(), payload: json!({"table": "events", "row": row}).to_string().into_bytes() })
        .collect();
    sink.send(&msgs).unwrap();

    let path = format!("/tmp/enchu-kafka-big-{tag}.db");
    let mut db = Database::create_growable_with_capacity(&path, 4096).unwrap();
    db.table("events").bigint("id").bigint("at").tag("kind").primary_key("id").build().unwrap();
    prepare(&mut db).unwrap();
    let events = db.get_table("events").unwrap();
    let mut ex = LiveExport::new(&db);
    ex.add("ms", "events", events.where_eq("kind", "ms").subscribe().unwrap()).unwrap();
    ex.add_counts("by_kind", events.all().subscribe_sums("kind", "at").unwrap());
    let mut out = KafkaSink::connect(brokers.clone(), &tout).unwrap();
    {
        let ing = Ingest::new(&db, JsonRows).unwrap();
        let mut src = KafkaSource::connect(brokers.clone(), &tin).unwrap();
        ing.resume(&mut src).unwrap();
        assert_eq!(ingest_all(&ing, &mut src, rows.len()), (rows.len(), 0));
        ex.pump(&mut out).unwrap();
    }
    let at = |db: &Database, id: i64| {
        let t = db.get_table("events").unwrap();
        t.entity(t.where_eq("id", id).find_one().unwrap().unwrap()).get("at")
    };
    use enchudb_schema::Value;
    assert_eq!(at(&db, -1), Some(Value::Number(-7)), "同じ key の後の値が勝っていない");
    assert_eq!(at(&db, -100), Some(Value::Number(t0 + 99)));
    assert_eq!(at(&db, i64::MAX - 1), Some(Value::Number(i64::MIN)));

    // 出口: 行は主キーと ms の時刻のまま、 合計は u64 を超えれば文字列
    let mut read = KafkaSource::connect(brokers.clone(), &tout).unwrap();
    let (mut added, mut sums) = (std::collections::BTreeMap::new(), std::collections::BTreeMap::new());
    for _ in 0..100 {
        for m in read.fetch(1000).unwrap() {
            let p: Json = serde_json::from_slice(m.payload.as_deref().unwrap()).unwrap();
            if p["sub"] == "ms" {
                added.insert(p["key"].as_i64().unwrap(), p["row"]["at"].clone());
            } else {
                sums.insert(p["group"].as_str().unwrap().to_string(), p["sum"].clone());
            }
        }
        if added.len() >= 100 && sums.len() >= 3 {
            break;
        }
    }
    assert_eq!(added.len(), 100);
    assert_eq!(added[&-1], json!(-7));
    assert_eq!(added[&-50], json!(t0 + 49));
    assert_eq!(sums["z"], json!((big as i128 * 3).to_string()), "u64 を超える合計");
    assert_eq!(sums["edge"], json!(i64::MIN));
    let ms_sum: i64 = (1..100i64).map(|i| t0 + i).sum::<i64>() - 7;
    assert_eq!(sums["ms"], json!(ms_sum));
    drop(ex);
    drop(db);

    // 開き直し: 値が残り、 先頭から読み直しても当てた位置は飛ばす
    let db = Database::open(&path).unwrap();
    assert_eq!(at(&db, -1), Some(Value::Number(-7)));
    assert_eq!(at(&db, 1002), Some(Value::Number(big)));
    let ing = Ingest::new(&db, JsonRows).unwrap();
    let mut again = KafkaSource::connect(brokers.clone(), &tin).unwrap();
    assert_eq!(ingest_all(&ing, &mut again, rows.len()), (0, rows.len()));
    drop(ing);
    drop(db);
    for s in ["", ".oplog", ".tables", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{path}{s}"));
    }
    let _ = std::fs::remove_dir_all(&path);
}
