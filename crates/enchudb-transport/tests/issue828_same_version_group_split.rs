//! #828: 同じ (書き手, 版数) の record の組が、 ring の端で割れた所を受け手に配らない。
//!
//! 受け手の cursor は書き手ごとの版数で、 `hlc > cursor` の record だけを受ける。 state を通った record は同じ版数で並ぶ
//! (Tag の cell の Vocab と Tie / 版数の無い cell が `as_of` で何本も)。 ホストの ring に空きが足りないと bridge は入る所まで
//! 写して止まり (#787)、 その止まった所が組の途中だと、 受け手は組の前半だけ受けて cursor をその版数に進め、 ring が空いた後に
//! 載る後半は `hlc > cursor` を通らなかった。
//!
//! ここの試験は切れ目を試験が作らない: ホストの ring (8,192 行) を本物の state で埋め、 bridge が止まった所を見る。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;
/// c3 に何も届かない周がこれだけ続いたら、 もう届かないと見る (1 周 = ホストの書き出し + c3 の pull + 他の client の sync)。
const IDLE_ROUNDS: u32 = 15;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue828-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `v` (Number)、 `t` (Tag) を持つ表 `notes`。 `seed` の名前の行は WAL を通さずに書く (版数の無い cell =
/// state では全部 `as_of` の版数で出る)。
fn db(path: &str, peer: PeerId, seed: std::ops::Range<u32>) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 32_768).unwrap();
        e.define_table("notes", 16_384).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.set_peer_id(peer);
        for key in seed {
            let row = e.entity_in("notes").unwrap();
            e.tie(row, "notes.k", key);
            e.tie(row, "notes.v", key);
        }
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 64 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

struct Client {
    eng: Arc<Engine>,
    t: Arc<HttpHostTransport>,
    s: Syncer,
    /// state で取り直した書き手。
    booted: std::cell::RefCell<Vec<PeerId>>,
}

fn client(eng: Arc<Engine>, url: &str) -> Client {
    client_with_cursors(eng, url, None)
}

/// `cursors` = 受けた位置の置き場 (ホストを開き直した後、 同じ engine に付け直す時に続きから受ける)。
fn client_with_cursors(eng: Arc<Engine>, url: &str, cursors: Option<PathBuf>) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let mut s = Syncer::new(eng.clone(), t.clone());
    if let Some(p) = cursors {
        s = s.with_cursor_path(p);
    }
    s.serve_state();
    Client { eng, t, s, booted: Default::default() }
}

impl Client {
    fn row(&self, k: u32) -> Option<u64> {
        self.eng.pull_raw("notes.k", k).first().copied()
    }
    fn v(&self, k: u32) -> Option<u64> {
        self.row(k).and_then(|e| self.eng.get(e, "notes.v"))
    }
    fn t(&self, k: u32) -> Option<String> {
        self.row(k).and_then(|e| self.eng.get_text_owned(e, "notes.t")).map(|b| String::from_utf8(b).unwrap())
    }
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
    }
    fn pull(&self) -> usize {
        let mut received = 0;
        for _ in 0..4 {
            let out = self.s.pull_once(HOST);
            received += out.received;
            if !out.history_truncated {
                break;
            }
            for a in out.truncated_authors {
                assert!(self.s.bootstrap_pull_via(HOST, a).is_some(), "書き手 {a} の state を取れない");
                self.booted.borrow_mut().push(a);
            }
        }
        received
    }
    fn sync(&self) {
        self.push();
        self.pull();
    }
    fn cursor(&self, author: PeerId) -> Hlc {
        self.s.cursors_of(HOST).into_iter().find(|(a, _)| *a == author).map_or(Hlc::ZERO, |(_, h)| h)
    }
}

fn host_cursor(host: &HttpHost, c: PeerId) -> Hlc {
    host.syncer().cursors_of(c).into_iter().find(|(a, _)| *a == c).map_or(Hlc::ZERO, |(_, h)| h)
}

/// ホストの c の cursor のすぐ上に c の floor を置く (= c が送れずに落とした record がある、 と言う)。 次の publish でホストは
/// c の state を求める。
fn ask_for_state(host: &HttpHost, c: &Client) {
    let cur = host_cursor(host, c.eng.peer_id());
    let floor = Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer };
    c.t.set_history_floor_multi(c.eng.peer_id(), &[(c.eng.peer_id(), floor)]);
}

/// ring の中身 (lsn の順): (書き手, 版数, Vocab か)。
fn ring(e: &Engine) -> Vec<(PeerId, Hlc, bool)> {
    e.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(|r| (r.author_peer, r.hlc, matches!(r.op, DecodedOp::Vocab { .. })))
        .collect()
}

/// ring の最後の record が、 それを指す Tie のまだ載っていない Vocab か (= Vocab と Tie の組が ring の端で割れている)。
fn tail_is_a_lone_vocab(e: &Engine) -> bool {
    ring(e).last().is_some_and(|(_, _, vocab)| *vocab)
}

/// Tag の cell の組 (Vocab と Tie が同じ版数) が ring の端で割れる形。 state は 1 行につき record 3 つ (名前の Tie、 Vocab、
/// Tag の Tie)、 ring に入り切らない。 割れる所は ring の空きの数で決まるので、 詰め物の record の数 (0 / 1 / 2) を変えた 3 回の
/// どれかで必ず Vocab の後ろで止まる (前提として確かめる)。
#[test]
fn a_word_and_its_cell_cut_apart_at_the_edge_of_the_hosts_ring_both_reach_a_client() {
    const N: u32 = 3_200;
    let mut cut_inside_a_group = 0;
    for pad in 0..3u32 {
        let dir = Dir::new(&format!("tag{pad}"));
        let host_eng = db(&dir.db(HOST), HOST, 0..0);
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
        let url = format!("http://{}", host.addr());
        let c1 = client(db(&dir.db(1), 1, 0..0), &url);
        let c3 = client(db(&dir.db(3), 3, 0..0), &url);
        let named = |k: u32| {
            let e = c1.eng.entity_in("notes").unwrap();
            c1.eng.tie_to(e, "notes.k", k);
            e
        };
        named(1);
        c1.eng.oplog_commit();
        for _ in 0..2 {
            c1.sync();
            c3.sync();
        }
        assert!(c3.row(1).is_some(), "前提: c3 は c1 の行を受けた");
        // 詰め物 (名前だけの行 = record 1 つ) と、 語を持つ行 (record 3 つ)
        for k in 0..pad {
            named(10 + k);
        }
        for k in 100..100 + N {
            let e = named(k);
            c1.eng.tie_text_to(e, "notes.t", &format!("w{k}"));
        }
        c1.eng.oplog_commit();
        ask_for_state(&host, &c1);
        c1.push();
        let host_t = |k: u32| {
            host_eng.pull_raw("notes.k", k).first().and_then(|e| host_eng.get_text_owned(*e, "notes.t")).map(|b| String::from_utf8(b).unwrap())
        };
        assert_eq!((100..100 + N).filter(|k| host_t(*k) != Some(format!("w{k}"))).count(), 0, "前提: ホストに state が全部当たった");
        let full = ring(&host_eng).len();
        assert!(full < 3 * N as usize, "前提: state は ring に入り切っていない (ring {full} 行、 state の record {})", 3 * N);
        // c3 が受け続ける (受けて ack すると ring が回収され、 consumer の周期で残りが載る)
        let mut cut = false;
        // 何も届かない周が続いたら終わり (ring に載った物は c3 が全部受けた、 ring が空いても次が載らない)
        let mut idle = 0;
        let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            host_eng.oplog_sync().unwrap();
            // この後の pull は、 Vocab と Tie の間で止まった ring を読む
            let lone = tail_is_a_lone_vocab(&host_eng);
            let got = c3.pull();
            cut |= lone;
            idle = if got == 0 { idle + 1 } else { 0 };
            c1.sync();
            let missing = (100..100 + N).filter(|k| c3.t(*k) != Some(format!("w{k}"))).count();
            if missing == 0 || idle >= IDLE_ROUNDS {
                break;
            }
            assert!(std::time::Instant::now() < end, "60 秒で届き終わらない (c3 に語の無い行 {missing}、 ホストの ring {} 行)", ring(&host_eng).len());
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        cut_inside_a_group += cut as u32;
        let missing: Vec<u32> = (100..100 + N).filter(|k| c3.t(*k) != Some(format!("w{k}"))).collect();
        assert!(
            missing.is_empty(),
            "詰め物 {pad}: c3 に語の無い行 {} (先頭 {:?}、 行そのものが無い {})。 Vocab と Tie の間で止まった ring を c3 が読んだ: {cut}",
            missing.len(),
            &missing[..missing.len().min(5)],
            (100..100 + N).filter(|k| c3.row(*k).is_none()).count()
        );
    }
    assert!(cut_inside_a_group > 0, "前提: 3 回のどれかで、 c3 は Vocab と Tie の間で止まった ring を読んだ");
}

/// 版数の無い cell (WAL を通さずに書いた行) は、 state で全部 `as_of` の版数で出る = 同じ版数の Tie が行の数だけ並ぶ。 その組が
/// ring の端で割れる形: 先に c2 の record で ring をほぼ埋めておき、 c1 の state (同じ版数の record 600 本) の途中で止める。
#[test]
fn cells_without_a_version_cut_apart_at_the_edge_of_the_hosts_ring_all_reach_a_client() {
    const SEED: std::ops::Range<u32> = 1_000..1_300;
    let dir = Dir::new("asof");
    let host_eng = db(&dir.db(HOST), HOST, 0..0);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1, SEED), &url);
    let c2 = client(db(&dir.db(2), 2, 0..0), &url);
    let c3 = client(db(&dir.db(3), 3, 0..0), &url);
    let write = |c: &Client, k: u32| {
        let e = c.eng.entity_in("notes").unwrap();
        c.eng.tie_to(e, "notes.k", k);
        c.eng.tie_to(e, "notes.v", k);
    };
    write(&c1, 1);
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c2.sync();
        c3.sync();
    }
    assert_eq!(c3.v(1), Some(1), "前提: c3 は c1 の行を受けた");
    assert_eq!(c3.v(SEED.start), None, "前提: 版数の無い行は差分では出ていない");
    // c2 の record で ring をほぼ埋める (c3 は受けていないので回収されない)
    for k in 10_000..13_900 {
        write(&c2, k);
    }
    c2.eng.oplog_commit();
    c2.push();
    host_eng.oplog_sync().unwrap();
    let before = ring(&host_eng).len();
    assert!((7_700..8_000).contains(&before), "前提: ring は c2 の record でほぼ埋まった ({before} 行)");
    ask_for_state(&host, &c1);
    c1.push();
    host_eng.oplog_sync().unwrap();
    let host_v = |k: u32| host_eng.pull_raw("notes.k", k).first().and_then(|e| host_eng.get(*e, "notes.v"));
    assert_eq!(SEED.filter(|k| host_v(*k) != Some(*k as u64)).count(), 0, "前提: ホストに state が全部当たった");
    let r = ring(&host_eng);
    let (_, as_of, _) = *r.last().unwrap();
    let on_ring = r.iter().filter(|(a, h, _)| *a == 1 && *h == as_of).count();
    assert!(
        on_ring > 1 && on_ring < 2 * SEED.len(),
        "前提: 同じ版数 {as_of:?} の組 (state の record {} 本) が ring の端で割れている (載った {on_ring} 本、 ring {} 行)",
        2 * SEED.len(),
        r.len()
    );
    let mut idle = 0;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let got = c3.pull();
        idle = if got == 0 { idle + 1 } else { 0 };
        c1.sync();
        c2.sync();
        host_eng.oplog_sync().unwrap();
        let missing = SEED.filter(|k| c3.v(*k) != Some(*k as u64)).count();
        if missing == 0 || idle >= IDLE_ROUNDS {
            break;
        }
        assert!(std::time::Instant::now() < end, "60 秒で届き終わらない (c3 で食い違う行 {missing}、 ホストの ring {} 行)", ring(&host_eng).len());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let missing: Vec<u32> = SEED.filter(|k| c3.v(*k) != Some(*k as u64)).collect();
    assert!(
        missing.is_empty(),
        "c3 で v が食い違う行 {} (先頭 {:?}、 行そのものが無い {})",
        missing.len(),
        &missing[..missing.len().min(5)],
        SEED.filter(|k| c3.row(*k).is_none()).count()
    );
}

/// 同じ版数の組が ring より大きい形 (版数の無い cell 10,000 本、 ring 8,192 行)。 組の行は配られないので ack で空かない =
/// 待っても組はそろわない。 ホストは書き手の floor を組の版数に上げ、 client は state で取り直す。
#[test]
fn cells_without_a_version_that_outnumber_the_hosts_ring_reach_a_client_by_state() {
    const SEED: std::ops::Range<u32> = 1_000..6_000;
    let dir = Dir::new("big");
    let host_eng = db(&dir.db(HOST), HOST, 0..0);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1, SEED), &url);
    let c3 = client(db(&dir.db(3), 3, 0..0), &url);
    let e = c1.eng.entity_in("notes").unwrap();
    c1.eng.tie_to(e, "notes.k", 1u32);
    c1.eng.tie_to(e, "notes.v", 1u32);
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    assert_eq!(c3.v(1), Some(1), "前提: c3 は c1 の行を受けた");
    ask_for_state(&host, &c1);
    c1.push();
    host_eng.oplog_sync().unwrap();
    let host_v = |k: u32| host_eng.pull_raw("notes.k", k).first().and_then(|e| host_eng.get(*e, "notes.v"));
    assert_eq!(SEED.filter(|k| host_v(*k) != Some(*k as u64)).count(), 0, "前提: ホストに state が全部当たった");
    let r = ring(&host_eng);
    let (_, as_of, _) = *r.last().unwrap();
    let on_ring = r.iter().filter(|(a, h, _)| *a == 1 && *h == as_of).count();
    assert!(on_ring > 8_000 && on_ring < 2 * SEED.len(), "前提: 同じ版数の組 ({} 本) が ring に入り切らない (載った {on_ring} 本)", 2 * SEED.len());
    let mut idle = 0;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let got = c3.pull();
        idle = if got == 0 { idle + 1 } else { 0 };
        c1.sync();
        host_eng.oplog_sync().unwrap();
        let missing = SEED.filter(|k| c3.v(*k) != Some(*k as u64)).count();
        if missing == 0 || idle >= IDLE_ROUNDS {
            break;
        }
        assert!(std::time::Instant::now() < end, "60 秒で届き終わらない (c3 で食い違う行 {missing}、 ホストの ring {} 行)", ring(&host_eng).len());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let missing: Vec<u32> = SEED.filter(|k| c3.v(*k) != Some(*k as u64)).collect();
    assert!(missing.is_empty(), "c3 で v が食い違う行 {} (先頭 {:?})", missing.len(), &missing[..missing.len().min(5)]);
    let floor = host_eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == 1).map(|(_, h)| h);
    assert_eq!(floor, Some(as_of), "ホストは c1 の floor を組の版数に上げた");
    assert_eq!(*c3.booted.borrow(), vec![1], "c3 は c1 の state を取り直した");
    // 組の残りが載り終えると ring は空く (取り直した client の cursor は組の版数以上 = ack で回収される)
    let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while host_eng.bridge_pending_records() > 0 || ring(&host_eng).len() > 8_000 {
        c3.pull();
        c1.sync();
        host_eng.oplog_sync().unwrap();
        assert!(std::time::Instant::now() < end, "60 秒で ring が空かない ({} 行)", ring(&host_eng).len());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// 組が ring の端で割れたままホストを閉じ、 開き直す。 「どこまで配ってよいか」 は memory の覚えなので、 開き直した後の最初の
/// pull の前に、 ring に残っている行と WAL の続きから決め直す。
#[test]
fn a_group_cut_apart_before_the_host_closes_is_still_whole_for_a_client_after_reopen() {
    const SEED: std::ops::Range<u32> = 1_000..1_300;
    let dir = Dir::new("reopen");
    let cursors = |id: u32| Some(dir.0.join(format!("cursors{id}")));
    let host_eng = db(&dir.db(HOST), HOST, 0..0);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), cursors(HOST)).unwrap();
    let url = format!("http://{}", host.addr());
    let engines = [db(&dir.db(1), 1, SEED), db(&dir.db(2), 2, 0..0), db(&dir.db(3), 3, 0..0)];
    let connect = |url: &str| {
        let [e1, e2, e3] = engines.clone();
        [client_with_cursors(e1, url, cursors(1)), client_with_cursors(e2, url, cursors(2)), client_with_cursors(e3, url, cursors(3))]
    };
    let [c1, c2, c3] = connect(&url);
    let write = |c: &Client, k: u32| {
        let e = c.eng.entity_in("notes").unwrap();
        c.eng.tie_to(e, "notes.k", k);
        c.eng.tie_to(e, "notes.v", k);
    };
    write(&c1, 1);
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c2.sync();
        c3.sync();
    }
    assert_eq!(c3.v(1), Some(1), "前提: c3 は c1 の行を受けた");
    for k in 10_000..13_900 {
        write(&c2, k);
    }
    c2.eng.oplog_commit();
    c2.push();
    ask_for_state(&host, &c1);
    c1.push();
    host_eng.oplog_sync().unwrap();
    let split = |e: &Engine| {
        let r = ring(e);
        let (_, as_of, _) = *r.last().unwrap();
        (r.iter().filter(|(a, h, _)| *a == 1 && *h == as_of).count(), r.len())
    };
    let (on_ring, len) = split(&host_eng);
    assert!(on_ring > 1 && on_ring < 2 * SEED.len(), "前提: 同じ版数の組が ring の端で割れている (載った {on_ring} 本、 ring {len} 行)");
    // 閉じて開き直す (client は誰も、 割れた所をまだ受けていない)
    drop((c1, c2, c3));
    drop(host);
    // (engine は Arc の中で閉じる: consumer が Arc の中の住所を指しているので、 取り出して動かさない)
    assert_eq!(Arc::strong_count(&host_eng), 1, "前提: ホストの engine を持っているのは試験だけ (drop で閉じる)");
    drop(host_eng);
    let host_eng = Engine::open_concurrent_with_oplog(&dir.db(HOST), 64 << 20).unwrap();
    host_eng.set_peer_id(HOST);
    assert_eq!(split(&host_eng), (on_ring, len), "前提: 開き直した直後も、 組は ring の上で割れたまま");
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), cursors(HOST)).unwrap();
    let url = format!("http://{}", host.addr());
    let [c1, c2, c3] = connect(&url);
    let mut idle = 0;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let got = c3.pull();
        idle = if got == 0 { idle + 1 } else { 0 };
        c1.sync();
        c2.sync();
        host_eng.oplog_sync().unwrap();
        let missing = SEED.filter(|k| c3.v(*k) != Some(*k as u64)).count();
        if missing == 0 || idle >= IDLE_ROUNDS {
            break;
        }
        assert!(std::time::Instant::now() < end, "60 秒で届き終わらない (c3 で食い違う行 {missing}、 ホストの ring {} 行)", ring(&host_eng).len());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let missing: Vec<u32> = SEED.filter(|k| c3.v(*k) != Some(*k as u64)).collect();
    assert!(missing.is_empty(), "c3 で v が食い違う行 {} (先頭 {:?})", missing.len(), &missing[..missing.len().min(5)]);
    assert!(c3.booted.borrow().is_empty(), "c3 は差分だけで受けた (取り直していない)");
}

/// ホストでない中継 (A → R → D、 `InMemoryTransport`)。 R の ring を R 自身の record でほぼ埋めておき、 R が A の state
/// (版数の無い cell = 同じ版数の Tie 600 本) で取り直す。 R の publish は、 ring の端で割れた組を出さない。
#[test]
fn a_relay_does_not_publish_a_group_cut_apart_at_the_edge_of_its_ring() {
    const SEED: std::ops::Range<u32> = 1_000..1_300;
    const A: PeerId = 1;
    const R: PeerId = 4;
    const D: PeerId = 5;
    let dir = Dir::new("mesh");
    let a = db(&dir.db(A), A, SEED);
    let r = db(&dir.db(R), R, 0..0);
    let d = db(&dir.db(D), D, 0..0);
    r.set_gossip_remote_apply(true);
    let up: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let sa = Syncer::new(a.clone(), up.clone());
    let sr_up = Syncer::new(r.clone(), up);
    sa.serve_state();
    let down = Arc::new(InMemoryTransport::new());
    let link: Arc<dyn Transport> = down.clone();
    let sr = Syncer::new(r.clone(), link.clone());
    let sd = Syncer::new(d.clone(), link);
    // D も publish する (`InMemoryTransport` の `known_peers` は publish した peer。 R だけだと、 R の 2 回目からの publish は
    // 宛先が無くて何も出さない)
    let e = d.entity_in("notes").unwrap();
    d.tie_to(e, "notes.k", 9u32);
    d.oplog_sync().unwrap();
    assert!(sd.publish_since(Hlc::ZERO) > 0, "前提: D が transport に居る");
    // R の record で R の ring をほぼ埋める (D は受けていないので回収されない)
    for k in 10_000..13_900u32 {
        let e = r.entity_in("notes").unwrap();
        r.tie_to(e, "notes.k", k);
        r.tie_to(e, "notes.v", k);
    }
    r.oplog_commit();
    r.oplog_sync().unwrap();
    let before = ring(&r).len();
    assert!((7_700..8_100).contains(&before), "前提: R の ring は R の record でほぼ埋まった ({before} 行)");
    sr_up.bootstrap_pull_via(A, A).expect("A の state");
    r.oplog_sync().unwrap();
    let v = |e: &Engine, k: u32| e.pull_raw("notes.k", k).first().and_then(|row| e.get(*row, "notes.v"));
    assert_eq!(SEED.filter(|k| v(&r, *k) != Some(*k as u64)).count(), 0, "前提: R に state が全部当たった");
    let on_ring = ring(&r).iter().filter(|(author, _, _)| *author == A).count();
    assert!(on_ring > 1 && on_ring < 2 * SEED.len(), "前提: A の同じ版数の組が R の ring の端で割れている (載った {on_ring} 本)");
    let mut idle = 0;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        r.oplog_sync().unwrap();
        sr.publish_since(Hlc::ZERO);
        let got = sd.pull_once(R).received;
        idle = if got == 0 { idle + 1 } else { 0 };
        let missing = SEED.filter(|k| v(&d, *k) != Some(*k as u64)).count();
        if missing == 0 || idle >= IDLE_ROUNDS {
            break;
        }
        assert!(std::time::Instant::now() < end, "60 秒で届き終わらない (D で食い違う行 {missing}、 R の ring {} 行)", ring(&r).len());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let missing: Vec<u32> = SEED.filter(|k| v(&d, *k) != Some(*k as u64)).collect();
    assert!(missing.is_empty(), "D で v が食い違う行 {} (先頭 {:?})", missing.len(), &missing[..missing.len().min(5)]);
}
