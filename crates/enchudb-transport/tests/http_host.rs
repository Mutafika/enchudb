//! ホスト (全 client が経由する、 DB を持つ peer) の HTTP の口 (`HttpHost`) と client の transport (`HttpHostTransport`)。
//!
//! 前の HTTP の口 (`HttpRelay` + `HttpTransport`) は record を memory に預かる掲示板で、 ack / floor / state を運ばなかった:
//! client の `_sync_ops` は回収されず、 抜けた client の取り直しも起きず、 再起動で record が消えた。

use std::sync::Arc;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::Transport;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};
use enchudb_transport::http::HttpTransport;

const HOST: PeerId = 100;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-httphost-{tag}-{}-{nanos}", std::process::id())
}

/// 表 `notes` (列 `v`) を持つ sync の DB を作って開く。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    reopen(path, peer)
}

fn reopen(path: &str, peer: PeerId) -> Arc<Engine> {
    let e = Engine::open_concurrent_with_oplog(path, 16 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

struct Client {
    eng: Arc<Engine>,
    t: Arc<HttpHostTransport>,
    s: Syncer,
}

fn client(eng: Arc<Engine>, url: &str) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t.clone());
    s.serve_state();
    Client { eng, t, s }
}

impl Client {
    fn write(&self, v: u32) -> u64 {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
        e
    }

    /// 送って受ける (WAL を書き出して `_sync_ops` に載せてから)。
    fn sync(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
        let out = self.s.pull_once(self.t.host_id());
        assert!(!out.history_truncated, "抜けは別の試験で見る: {out:?}");
    }

    fn has(&self, v: u32) -> usize {
        self.eng.pull_raw("notes.v", v).len()
    }
}

/// client 3 台がホスト経由で揃う (足した行、 書き換え、 別の client の行の削除)。 ホストも同じ中身を持つ。
#[test]
fn three_clients_converge_through_the_host() {
    let hp = tmp("host");
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let paths: Vec<String> = (1..=3).map(|i| tmp(&format!("c{i}"))).collect();
    let cs: Vec<Client> = (0..3).map(|i| client(db(&paths[i], i as u32 + 1), &url)).collect();

    let rows: Vec<u64> = (0..3).map(|i| cs[i].write(10 + i as u32)).collect();
    for _ in 0..2 {
        for c in &cs {
            c.sync();
        }
    }
    for c in &cs {
        assert_eq!([c.has(10), c.has(11), c.has(12)], [1, 1, 1], "peer {} に届かない", c.eng.peer_id());
    }
    // 1 が自分の行を書き換え、 2 が 3 の行を消す (2 は 3 の行を受けて持っている)
    cs[0].eng.tie_to(rows[0], "notes.v", 20u32);
    cs[0].eng.oplog_commit();
    let three_at_two = cs[1].eng.pull_raw("notes.v", 12)[0];
    cs[1].eng.delete(three_at_two);
    cs[1].eng.oplog_commit();
    for _ in 0..2 {
        for c in &cs {
            c.sync();
        }
    }
    for c in &cs {
        assert_eq!(
            [c.has(10), c.has(20), c.has(11), c.has(12)],
            [0, 1, 1, 0],
            "peer {} が揃わない",
            c.eng.peer_id()
        );
    }
    let h = |v: u32| host_eng.pull_raw("notes.v", v).len();
    assert_eq!([h(10), h(20), h(11), h(12)], [0, 1, 1, 0], "ホストが揃わない");
    drop(cs);
    drop(host);
    for p in paths.iter().chain([&hp]) {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// ホストの ack が client に届き (client の `_sync_ops` の回収が回る)、 client の ack がホストに届く。
/// 前の HTTP の口は ack を運ばず、 `sync_watermark` は 0 のまま (= 回収されない)。
#[test]
fn acks_flow_both_ways() {
    let hp = tmp("ack-host");
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (p1, p2) = (tmp("ack-c1"), tmp("ack-c2"));
    let (c1, c2) = (client(db(&p1, 1), &url), client(db(&p2, 2), &url));
    for i in 0..20 {
        c1.write(100 + i);
    }
    c1.sync();
    c2.sync();
    c1.sync(); // 1 回目の publish の返事 (ホストの ack) を取り込む
    assert!(c1.eng.sync_watermark() > 0, "ホストの ack が client に届かない");
    let before = c1.eng.pending_sync_ops(0).len();
    let purged = c1.eng.reclaim_sync_ops();
    assert!(before >= 20, "前提: 送った行が ring にある ({before})");
    // 回収は ack の位置の手前まで (`lsn < watermark`)
    assert!(purged + 1 >= before && c1.eng.pending_sync_ops(0).len() <= 1, "ack で回収できない ({purged} / {before})");
    assert!(host_eng.sync_watermark() > 0, "client の ack がホストに届かない");
    drop((c1, c2, host));
    for p in [&p1, &p2, &hp] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// ホストを閉じて開き直しても続く (record の置き場はホストの DB)。
#[test]
fn the_host_keeps_going_after_a_restart() {
    let hp = tmp("restart-host");
    let cursors = std::path::PathBuf::from(format!("{hp}.cursors"));
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
    let url = format!("http://{}", host.addr());
    let (p1, p2) = (tmp("restart-c1"), tmp("restart-c2"));
    let c1 = client(db(&p1, 1), &url);
    c1.write(1);
    c1.sync();
    drop(host);
    drop(host_eng);

    let host_eng = reopen(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
    let url = format!("http://{}", host.addr());
    // 1 は前のホストに送っただけ。 2 は開き直したホストから受ける
    let c2 = client(db(&p2, 2), &url);
    c2.write(2);
    c2.sync();
    let c1 = client(c1.eng.clone(), &url);
    c1.sync();
    c2.sync();
    assert_eq!([c2.has(1), c2.has(2)], [1, 1], "開き直したホストから前の record が届かない");
    assert_eq!([c1.has(1), c1.has(2)], [1, 1]);
    drop((c1, c2, host));
    let _ = std::fs::remove_file(&cursors);
    for p in [&p1, &p2, &hp] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// ホストが ring を回収した後に初めて来た client は抜けを知り、 ホストの state から取り直して続ける。
#[test]
fn a_client_behind_the_floor_bootstraps_from_the_host() {
    let hp = tmp("floor-host");
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (p1, p3) = (tmp("floor-c1"), tmp("floor-c3"));
    let c1 = client(db(&p1, 1), &url);
    for i in 0..10 {
        c1.write(200 + i);
    }
    c1.sync();
    c1.sync();
    // c1 は全部を受けて ack した。 ホストは ring を回収する (c3 はまだ来ていないので数えない)
    assert!(host_eng.reclaim_sync_ops() > 0, "前提: ホストが回収した");

    let c3 = client(db(&p3, 3), &url);
    let out = c3.s.pull_once(HOST);
    assert!(out.history_truncated, "回収の後に来た client が抜けを知らない: {out:?}");
    for a in out.truncated_authors.clone() {
        assert!(c3.s.bootstrap_pull_via(HOST, a).is_some(), "書き手 {a} の state を取れない");
    }
    assert!(!c3.s.pull_once(HOST).history_truncated, "取り直した後も抜けている");
    assert_eq!((0..10).map(|i| c3.has(200 + i)).sum::<usize>(), 10, "取り直しで行が揃わない");
    c1.write(300);
    c1.sync();
    c3.sync();
    assert_eq!(c3.has(300), 1, "取り直した後の差分が届かない");
    // c1 自身の行がホストで回収されても、 c1 は自分の分の抜けを言わない (自分の record は受けない)
    c1.sync();
    c1.sync();
    drop((c1, c3, host));
    for p in [&p1, &p3, &hp] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// ホストが client の floor に届かない (client が送れずに落とした record がある) と、 client に state を求め、 client は自分の
/// state を送る。 ホストは送られなかった行も持つ。
#[test]
fn the_host_asks_a_client_for_its_state() {
    let hp = tmp("state-host");
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let p1 = tmp("state-c1");
    let c1 = client(db(&p1, 1), &url);
    // 送らなかった行 (record は ring にあるが、 floor で 「ここまでは配れない」 と言う)
    c1.write(400);
    c1.write(401);
    c1.eng.oplog_sync().unwrap();
    let floor = Hlc { wall: u64::MAX / 2, logical: 0, peer: 1 };
    c1.t.set_history_floor_multi(1, &[(1, floor)]);
    // 新しい行を 1 つだけ送る: ホストの c1 の cursor は floor に届かない → state を求める
    let e = c1.write(402);
    c1.eng.oplog_sync().unwrap();
    let rec = c1.eng.pending_sync_ops(0).last().cloned().unwrap();
    let rec = enchudb::transport::WireRecord::from(enchudb_oplog::oplog::decode_sync_ops_payload(&rec).unwrap());
    c1.t.publish_to(1, HOST, vec![rec]);
    let h = |v: u32| host_eng.pull_raw("notes.v", v).len();
    assert_eq!([h(400), h(401), h(402)], [1, 1, 1], "ホストが client の state を取り直さない");
    let _ = e;
    drop((c1, host));
    for p in [&p1, &hp] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// 前の client (`HttpTransport`、 `/publish` と `/pull`) もホストと話せる (ack は運ばない)。
#[test]
fn an_old_client_still_syncs() {
    let hp = tmp("old-host");
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (p1, p2) = (tmp("old-c1"), tmp("old-c2"));
    let new = client(db(&p1, 1), &url);
    let old_eng = db(&p2, 2);
    let old_t: Arc<dyn Transport> = Arc::new(HttpTransport::new(url.clone()));
    let old = Syncer::new(old_eng.clone(), old_t);
    new.write(500);
    new.sync();
    let e = old_eng.entity_in("notes").unwrap();
    old_eng.tie_to(e, "notes.v", 501u32);
    old_eng.oplog_commit();
    old_eng.oplog_sync().unwrap();
    old.publish_since(Hlc::ZERO);
    old.pull_once(HOST);
    new.sync();
    assert_eq!(old_eng.pull_raw("notes.v", 500).len(), 1, "前の client がホストから受けない");
    assert_eq!(new.has(501), 1, "前の client の行が届かない");
    drop((new, old, host));
    for p in [&p1, &p2, &hp] {
        let _ = std::fs::remove_dir_all(p);
    }
}

/// 遅れた client (前に 1 回だけ受けて止まっている) がいても、 他の client の `_sync_ops` の回収を止めない。 ホストは record の
/// 置き場の権威なので、 client への ack を下流の消化で丸めない (丸めると止まった 1 台が全員の ring を止める)。
#[test]
fn a_lagging_client_does_not_hold_back_the_others() {
    let hp = tmp("lag-host");
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (p1, p2) = (tmp("lag-c1"), tmp("lag-c2"));
    let (c1, c2) = (client(db(&p1, 1), &url), client(db(&p2, 2), &url));
    c1.write(600);
    c1.sync();
    c2.sync(); // c2 は 600 だけ受けて止まる
    for i in 0..20 {
        c1.write(700 + i);
    }
    c1.sync();
    c1.sync();
    let before = c1.eng.pending_sync_ops(0).len();
    let purged = c1.eng.reclaim_sync_ops();
    assert!(purged + 1 >= before, "止まった client が c1 の回収を止めた ({purged} / {before})");
    drop((c1, c2, host));
    for p in [&p1, &p2, &hp] {
        let _ = std::fs::remove_dir_all(p);
    }
}
