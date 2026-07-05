//! v32 HTTP transport の E2E 統合テスト。
//!
//! origin Engine → HTTP relay → replica Engine のデータ伝搬を確認する。
//! "2-node localhost デモ" の最小形。

#![cfg(feature = "v32")]

use std::sync::Arc;
use std::time::Duration;

use enchudb::{Engine, ValueType};
use enchudb_oplog::Hlc;
use enchudb::sync::Syncer;
use enchudb::transport::{Transport, WireRecord};
use enchudb_transport::http::{HttpRelay, HttpTransport};
use enchudb_oplog::oplog::DecodedOp;

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-v32-http-{}-{}", tag, std::process::id());
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

#[test]
fn origin_publishes_replica_pulls_over_http() {
    // relay サーバー起動
    let relay = HttpRelay::start("127.0.0.1:0").unwrap();
    let url = format!("http://{}", relay.addr());

    // origin: 通常の書き込み DB
    let origin_path = tmp("origin");
    {
        let mut eng = Engine::create_standalone(&origin_path).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        eng.flush().unwrap();
    }
    let origin = Engine::open_concurrent_with_oplog(&origin_path, 16 * 1024 * 1024).unwrap();
    origin.set_peer_id(1);

    // replica: read-only
    let replica_path = tmp("replica");
    {
        let mut eng = Engine::create_standalone(&replica_path).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        eng.flush().unwrap();
    }
    let replica = Engine::open_concurrent_replica(&replica_path, 16 * 1024 * 1024).unwrap();
    replica.set_peer_id(9);

    // origin 側 transport = HTTP client、publish 用
    let origin_transport: Arc<dyn Transport> = Arc::new(HttpTransport::new(url.clone()));
    // replica 側 transport も同じ relay を指す
    let replica_transport: Arc<dyn Transport> = Arc::new(HttpTransport::new(url.clone()));

    // origin 側で publish (直接 transport に流す、WAL 経由じゃなく手動 WireRecord)
    let eid = enchudb_oplog::make_eid(1, 7);
    let himo_id = origin.himo_id("val").unwrap() as u16;
    let records = vec![
        WireRecord::unsigned(
            Hlc { wall: 100, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie { eid, himo_id, value: 42 },
        ),
        WireRecord::unsigned(
            Hlc { wall: 200, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie {
                eid: enchudb_oplog::make_eid(1, 8),
                himo_id,
                value: 88,
            },
        ),
    ];
    origin_transport.publish(1, records);

    // relay が受け取るまで最大 2s 待つ (fixed sleep は CI/loaded machine で足りない)
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while relay.record_count() < 2 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(relay.record_count(), 2, "relay should hold 2 records");

    // replica 側 Syncer で pull
    let syncer = Syncer::new(replica.clone(), replica_transport);
    let out = syncer.pull_once(1);
    assert_eq!(out.received, 2);
    assert_eq!(out.applied, 2);
    assert_eq!(out.skipped, 0);

    // replica 側に反映
    assert_eq!(replica.get(eid, "val"), Some(42));
    assert_eq!(replica.get(enchudb_oplog::make_eid(1, 8), "val"), Some(88));

    cleanup(&origin_path);
    cleanup(&replica_path);
}

#[test]
fn multiple_replicas_sync_from_same_origin() {
    let relay = HttpRelay::start("127.0.0.1:0").unwrap();
    let url = format!("http://{}", relay.addr());

    // origin
    let origin_path = tmp("multi_origin");
    {
        let mut eng = Engine::create_standalone(&origin_path).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        eng.flush().unwrap();
    }
    let origin = Engine::open_concurrent_with_oplog(&origin_path, 16 * 1024 * 1024).unwrap();
    origin.set_peer_id(1);

    // replica A
    let replica_a_path = tmp("multi_replica_a");
    {
        let mut eng = Engine::create_standalone(&replica_a_path).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        eng.flush().unwrap();
    }
    let replica_a = Engine::open_concurrent_replica(&replica_a_path, 16 * 1024 * 1024).unwrap();
    replica_a.set_peer_id(101);

    // replica B
    let replica_b_path = tmp("multi_replica_b");
    {
        let mut eng = Engine::create_standalone(&replica_b_path).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        eng.flush().unwrap();
    }
    let replica_b = Engine::open_concurrent_replica(&replica_b_path, 16 * 1024 * 1024).unwrap();
    replica_b.set_peer_id(102);

    // origin が publish
    let himo_id = origin.himo_id("val").unwrap() as u16;
    let eid = enchudb_oplog::make_eid(1, 1);
    let origin_t: Arc<dyn Transport> = Arc::new(HttpTransport::new(url.clone()));
    origin_t.publish(1, vec![
        WireRecord::unsigned(
            Hlc { wall: 100, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie { eid, himo_id, value: 555 },
        ),
    ]);

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while relay.record_count() < 1 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    // 両 replica が同じ relay から pull
    let ta: Arc<dyn Transport> = Arc::new(HttpTransport::new(url.clone()));
    let tb: Arc<dyn Transport> = Arc::new(HttpTransport::new(url.clone()));
    let sa = Syncer::new(replica_a.clone(), ta);
    let sb = Syncer::new(replica_b.clone(), tb);

    let oa = sa.pull_once(1);
    let ob = sb.pull_once(1);
    assert_eq!(oa.applied, 1);
    assert_eq!(ob.applied, 1);

    assert_eq!(replica_a.get(eid, "val"), Some(555));
    assert_eq!(replica_b.get(eid, "val"), Some(555));

    cleanup(&origin_path);
    cleanup(&replica_a_path);
    cleanup(&replica_b_path);
}

#[test]
fn fresh_replica_bootstraps_then_syncs() {
    // シナリオ:
    // 1. origin に書き込みして close (ファイルが flush 済みの状態)
    // 2. origin relay を bootstrap 付きで起動
    // 3. replica が bootstrap で DB 取得
    // 4. origin が新規書き込みを publish
    // 5. replica が incremental sync で追いつく

    let origin_path = tmp("boot_origin");
    let replica_path = tmp("boot_replica");

    // step 1: origin に初期データ書いて close
    // create_compact でファイルサイズを MB 級に抑える (bootstrap 全転送が現実的に)
    {
        let mut eng = Engine::create_compact(&origin_path).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        let e1 = eng.entity();
        eng.tie(e1, "val", 42);
        let _e1_id = e1;
        eng.rebuild();
        eng.flush().unwrap();
    }

    // step 2: origin re-open + relay 起動
    let origin = Engine::open_concurrent_with_oplog(&origin_path, 16 * 1024 * 1024).unwrap();
    origin.set_peer_id(1);
    let relay = HttpRelay::start_with_bootstrap("127.0.0.1:0", &origin_path).unwrap();
    let url = format!("http://{}", relay.addr());

    // step 3: replica が bootstrap で DB 取得
    let client = HttpTransport::new(url.clone());
    let snapshot_hlc = client.bootstrap_to(&replica_path).unwrap();
    assert_eq!(snapshot_hlc, Hlc::ZERO, "no records published yet, HLC=0");

    // replica を open
    let replica = Engine::open_concurrent_replica(&replica_path, 16 * 1024 * 1024).unwrap();
    replica.set_peer_id(9);

    // replica で初期データが読める (bootstrap で DB ごとコピーされた)
    let entities: Vec<_> = replica.entities();
    assert_eq!(entities.len(), 1);
    let e1 = entities[0];
    assert_eq!(replica.get(e1, "val"), Some(42));

    // step 4: origin が新データを publish
    let origin_t = HttpTransport::new(url.clone());
    let new_eid = enchudb_oplog::make_eid(1, 2);
    let himo_id = origin.himo_id("val").unwrap() as u16;
    origin_t.publish(1, vec![
        WireRecord::unsigned(
            Hlc { wall: 1000, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie { eid: new_eid, himo_id, value: 999 },
        ),
    ]);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while relay.record_count() < 1 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    // step 5: replica が incremental sync
    let client_t: Arc<dyn Transport> = Arc::new(HttpTransport::new(url));
    let syncer = Syncer::new(replica.clone(), client_t);
    let out = syncer.pull_once(1);
    assert_eq!(out.received, 1);
    assert_eq!(out.applied, 1);

    // replica で新データも読める
    assert_eq!(replica.get(new_eid, "val"), Some(999));
    // 旧データも残ってる
    assert_eq!(replica.get(e1, "val"), Some(42));

    cleanup(&origin_path);
    cleanup(&replica_path);
}

#[test]
fn incremental_pull_advances_cursor() {
    let relay = HttpRelay::start("127.0.0.1:0").unwrap();
    let url = format!("http://{}", relay.addr());

    let path = tmp("cursor_replica");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        eng.flush().unwrap();
    }
    let replica = Engine::open_concurrent_replica(&path, 16 * 1024 * 1024).unwrap();
    replica.set_peer_id(9);

    let himo_id = replica.himo_id("val").unwrap() as u16;

    let publisher: Arc<dyn Transport> = Arc::new(HttpTransport::new(url.clone()));
    publisher.publish(1, vec![
        WireRecord::unsigned(
            Hlc { wall: 100, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie { eid: enchudb_oplog::make_eid(1, 1), himo_id, value: 1 },
        ),
    ]);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while relay.record_count() < 1 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    let client_t: Arc<dyn Transport> = Arc::new(HttpTransport::new(url.clone()));
    let syncer = Syncer::new(replica.clone(), client_t);

    let r1 = syncer.pull_once(1);
    assert_eq!(r1.received, 1);
    assert_eq!(r1.applied, 1);

    // 2 回目は空 (cursor 進んでるので)
    let r2 = syncer.pull_once(1);
    assert_eq!(r2.received, 0);

    // 新しい record publish
    publisher.publish(1, vec![
        WireRecord::unsigned(
            Hlc { wall: 200, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie { eid: enchudb_oplog::make_eid(1, 2), himo_id, value: 2 },
        ),
    ]);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while relay.record_count() < 2 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    let r3 = syncer.pull_once(1);
    assert_eq!(r3.received, 1);
    assert_eq!(r3.applied, 1);

    cleanup(&path);
}
