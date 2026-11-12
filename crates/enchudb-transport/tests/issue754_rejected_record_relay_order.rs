//! #754: batch の中で容量で断られた record の後ろの、 同じ書き手の record を、 断られた record より先に中継しない。
//!
//! 受け手の `cursor[a]` は 「中継の流れのうち、 書き手 a の record はここまで受けた」。 中継が a の record X を容量で断り
//! (cursor を止めて届き直させる)、 同じ batch の後ろの a の record Z を当てて載せると、 下の受け手は Z を受けて `cursor[a]`
//! が X の HLC を越える。 空きが戻って X が通って載っても、 cursor の手前なので二度と届かなかった。
//!
//! 今は、 中継は、 容量で断った record より後 (HLC) の同じ書き手の record を当てず載せない。 X は cursor を止めるので Z も
//! 一緒に届き直し、 X が通った batch で順に当たって載る。
//!
//! 署名で断られた record の後ろ (record ごとに署名が欠けている形) は直していない: X が後で通るのはホストが署名を求めなく
//! なった時だけで、 その形を止めると 「断られた record の後ろの負けた record は 1 回載る」
//! (`issue698_host_relays_losing_records.rs` の `a_losing_record_redelivered_under_a_held_cursor_is_relayed_once`) と両立しない。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb::{FaultKind, ValueType};
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue754-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
    fn db(&self, id: u32) -> String {
        self.0.join(format!("p{id}")).to_str().unwrap().to_string()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 名前 `k` (Number) と `v` (Number)、 `t` (Tag)、 `body` (Leaf) を持つ表 `notes`。 伸びる形 (空きが足りないと書き込みを断る)。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_growable_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        e.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 64 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

fn row(e: &Engine, k: u32) -> Option<u64> {
    e.pull_raw("notes.k", k).first().copied()
}
fn v(e: &Engine, k: u32) -> Option<u64> {
    row(e, k).and_then(|r| e.get(r, "notes.v"))
}
fn text(e: &Engine, k: u32, himo: &str) -> Option<String> {
    row(e, k).and_then(|r| e.get_text_owned(r, himo).map(|b| String::from_utf8_lossy(&b).into_owned()))
}
/// (v, t, body の長さ)
fn cells(e: &Engine, k: u32) -> (Option<u64>, Option<String>, Option<usize>) {
    (v(e, k), text(e, k, "notes.t"), text(e, k, "notes.body").map(|b| b.len()))
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
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
    }
    fn pull(&self) {
        let out = self.s.pull_once(HOST);
        assert!(!out.history_truncated, "前提: 抜けを言われない ({out:?})");
    }
    fn sync(&self) {
        self.push();
        self.pull();
    }
    fn cursor(&self, author: PeerId) -> Hlc {
        self.s.cursors_of(HOST).into_iter().find(|(a, _)| *a == author).map_or(Hlc::ZERO, |(_, h)| h)
    }
    fn himo(&self, name: &str) -> u16 {
        self.eng.himo_id(name).unwrap() as u16
    }
}

fn host_cursor(host: &HttpHost, c: PeerId) -> Hlc {
    host.syncer().cursors_of(c).into_iter().find(|(a, _)| *a == c).map_or(Hlc::ZERO, |(_, h)| h)
}

/// ホストの空きが足りず、 c1 の大きな Leaf (X) だけ断られる。 同じ batch の後ろの書き込み (Z: Number と Tag) は当たる。
/// その間に pull した c3 に、 空きが戻って X が通った後、 X が届く。
#[test]
fn a_record_refused_for_capacity_reaches_a_client_that_pulled_in_between() {
    let dir = Dir::new("cap");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c3) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(3), 3), &url));
    let r = c1.eng.entity_in("notes").unwrap();
    c1.eng.tie_to(r, "notes.k", 1u32);
    c1.eng.tie_to(r, "notes.v", 10u32);
    c1.eng.tie_text_to(r, "notes.t", "before");
    c1.eng.tie_text_to(r, "notes.body", "first");
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    let first = (Some(10), Some("before".to_string()), Some(5));
    assert_eq!([cells(&host_eng, 1), cells(&c3.eng, 1)], [first.clone(), first.clone()], "前提: 最初の行がホストと c3 に届いた");

    // ホストの空きが足りない間に、 c1 が X (大きな Leaf) → Z (Number と Tag) の順に書いて出す
    host_eng.set_space_margin(u64::MAX / 2);
    let big = "x".repeat(8 << 20);
    c1.eng.tie_text_to(r, "notes.body", &big); // X
    c1.eng.tie_to(r, "notes.v", 11u32); // Z
    c1.eng.tie_text_to(r, "notes.t", "after"); // Z (Vocab + Tie)
    c1.eng.oplog_commit();
    let x = c1.eng.cell_hlc(r, c1.himo("notes.body"));
    assert!(x < c1.eng.cell_hlc(r, c1.himo("notes.v")), "前提: X は Z より前");
    c1.push();
    assert!(host_eng.fault_count(FaultKind::DiskSpace) > 0, "前提: ホストは空きが足りず X を断った");
    assert_eq!(cells(&host_eng, 1).2, Some(5), "前提: ホストに X は当たっていない");
    assert!(host_cursor(&host, 1) < x, "前提: ホストの c1 の cursor は X の手前で止まった (X は届き直す)");

    // その間に c3 が pull する
    c3.pull();
    let between = c3.cursor(1);

    // 空きが戻り、 X が届き直して通る
    host_eng.set_space_margin(0);
    for _ in 0..2 {
        c1.push();
    }
    let last = (Some(11), Some("after".to_string()), Some(big.len()));
    assert_eq!(cells(&host_eng, 1), last, "前提: 空きが戻って X がホストに当たった");
    assert!(host_cursor(&host, 1) > x, "前提: ホストの c1 の cursor が X を越えた");
    for _ in 0..2 {
        c3.pull();
    }
    assert_eq!(cells(&c3.eng, 1), last, "c3 の行 (v, t, body の長さ): 後で通った X か、 その後ろの Z が届いていない");
    assert!(between < x, "X が断られている間に pull した c3 の、 書き手 1 の cursor が X を越えていた ({between:?} >= {x:?})");
    drop((c1, c3, host));
}

/// 対照: 断られる record が無ければ、 途中の pull で Z はすぐ届く。
#[test]
fn control_nothing_is_held_back_without_a_refusal() {
    let dir = Dir::new("ctl");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c3) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(3), 3), &url));
    let r = c1.eng.entity_in("notes").unwrap();
    c1.eng.tie_to(r, "notes.k", 1u32);
    c1.eng.tie_to(r, "notes.v", 10u32);
    c1.eng.tie_text_to(r, "notes.body", "first");
    c1.eng.oplog_commit();
    c1.sync();
    c3.sync();
    c1.eng.tie_text_to(r, "notes.body", "second");
    c1.eng.tie_to(r, "notes.v", 11u32);
    c1.eng.tie_text_to(r, "notes.t", "after");
    c1.eng.oplog_commit();
    c1.push();
    c3.pull();
    assert_eq!(cells(&c3.eng, 1), (Some(11), Some("after".to_string()), Some(6)));
    drop((c1, c3, host));
}

/// ホストでない中継 (A(1) → R(2) → C(3)) でも同じ: R が A の X を容量で断り、 後ろの Z を当てる。 その間に R から pull した
/// C に、 後で通った X が届く。
#[test]
fn a_plain_relay_does_not_relay_past_a_refused_record() {
    let dir = Dir::new("mesh");
    let mem = Arc::new(InMemoryTransport::new());
    for p in [1, 2, 3] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();
    let (a, rl, c) = (db(&dir.db(1), 1), db(&dir.db(2), 2), db(&dir.db(3), 3));
    rl.set_gossip_remote_apply(true);
    let (sa, sr, sc) =
        (Syncer::new(a.clone(), transport.clone()), Syncer::new(rl.clone(), transport.clone()), Syncer::new(c.clone(), transport.clone()));
    let relay_cycle = || {
        a.oplog_sync().unwrap();
        sa.publish_since(Hlc::ZERO);
        sr.pull_once(1);
        rl.oplog_sync().unwrap();
        sr.publish_since(Hlc::ZERO);
    };
    let r = a.entity_in("notes").unwrap();
    a.tie_to(r, "notes.k", 1u32);
    a.tie_to(r, "notes.v", 10u32);
    a.tie_text_to(r, "notes.t", "before");
    a.tie_text_to(r, "notes.body", "first");
    a.oplog_commit();
    relay_cycle();
    sc.pull_once(2);
    let first = (Some(10), Some("before".to_string()), Some(5));
    assert_eq!([cells(&rl, 1), cells(&c, 1)], [first.clone(), first], "前提: 最初の行が R と C に届いた");

    rl.set_space_margin(u64::MAX / 2);
    let big = "x".repeat(8 << 20);
    a.tie_text_to(r, "notes.body", &big); // X
    a.tie_to(r, "notes.v", 11u32); // Z
    a.tie_text_to(r, "notes.t", "after"); // Z (Vocab + Tie)
    a.oplog_commit();
    let x = a.cell_hlc(r, a.himo_id("notes.body").unwrap() as u16);
    relay_cycle();
    assert!(rl.fault_count(FaultKind::DiskSpace) > 0, "前提: R は空きが足りず X を断った");
    assert_eq!(cells(&rl, 1).2, Some(5), "前提: R に X は当たっていない");

    sc.pull_once(2);
    let between = sc.cursors_of(2).into_iter().find(|(p, _)| *p == 1).map_or(Hlc::ZERO, |(_, h)| h);

    rl.set_space_margin(0);
    for _ in 0..2 {
        relay_cycle();
    }
    let last = (Some(11), Some("after".to_string()), Some(big.len()));
    assert_eq!(cells(&rl, 1), last, "前提: 空きが戻って X が R に当たった");
    for _ in 0..2 {
        sc.pull_once(2);
    }
    assert_eq!(cells(&c, 1), last, "C の行 (v, t, body の長さ): 後で通った X か、 その後ろの Z が届いていない");
    assert!(between < x, "X が断られている間に pull した C の、 書き手 1 の cursor が X を越えていた ({between:?} >= {x:?})");
}
