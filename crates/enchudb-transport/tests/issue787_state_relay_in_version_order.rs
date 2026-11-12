//! #787: ホストは client の state を、 版数の昇順に並べて中継に載せる。 ring に入り切らない大きさの state で、 途中まで載った所を
//! 受けた client の cursor が、 まだ載っていない record を越えない。
//!
//! 前は state の順 (列ごと) のまま載せ、 ring に写し終えるまで pull を止めていた (#753 の gate)。 ring に空きが足りないと bridge は
//! 入る所まで写して止まり、 gate が開く。 途中まで載った state を受けた client の `cursor[書き手]` はその中の最大の版数まで進み、
//! ring が空いた後に載る残り (版数の小さい record) は配られなかった。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::Transport;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue787-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `v` (Number)、 `t` (Tag) を持つ表 `notes`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 32_768).unwrap();
        e.define_table("notes", 16_384).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
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
}

fn client(eng: Arc<Engine>, url: &str) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t.clone());
    s.serve_state();
    Client { eng, t, s }
}

impl Client {
    fn write(&self, k: u32, v: u32) -> u64 {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        e
    }
    fn row(&self, k: u32) -> Option<u64> {
        self.eng.pull_raw("notes.k", k).first().copied()
    }
    fn v(&self, k: u32) -> Option<u64> {
        self.row(k).and_then(|e| self.eng.get(e, "notes.v"))
    }
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
    }
    /// 受ける。 抜けを言われたら、 言われた書き手を取り直して受け直す。 戻り値: 取り直した書き手。
    fn pull(&self) -> Vec<PeerId> {
        let mut booted = Vec::new();
        for _ in 0..4 {
            let out = self.s.pull_once(HOST);
            if !out.history_truncated {
                break;
            }
            for a in out.truncated_authors {
                assert!(self.s.bootstrap_pull_via(HOST, a).is_some(), "書き手 {a} の state を取れない");
                booted.push(a);
            }
        }
        booted
    }
    fn sync(&self) -> Vec<PeerId> {
        self.push();
        self.pull()
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


fn ring_len(e: &Engine) -> usize {
    e.pending_sync_ops(0).len()
}

/// issue の形: ホストの ring (8,192 行) より大きい state (12,000 record)。 c3 は ring が空くたびに続きを受け、 全部届く。
#[test]
fn a_state_larger_than_the_hosts_ring_reaches_a_client_below_the_host() {
    const N: u32 = 6_000;
    let dir = Dir::new("ring");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1), &url);
    let c3 = client(db(&dir.db(3), 3), &url);
    c1.write(1, 1);
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    assert_eq!(c3.v(1), Some(1), "前提: c3 は c1 の行を受けた");
    // c1 がまだ出していない行 (ホストの ring より多い record)
    for k in 2..=N {
        c1.write(k, k);
    }
    c1.eng.oplog_commit();
    ask_for_state(&host, &c1);
    c1.push();
    let host_v = |k: u32| host_eng.pull_raw("notes.k", k).first().and_then(|e| host_eng.get(*e, "notes.v"));
    assert_eq!((1..=N).filter(|k| host_v(*k) != Some(*k as u64)).count(), 0, "前提: ホストに state が全部当たった");
    let full = ring_len(&host_eng);
    let missing_now = differing(&c1, |k| c3.v(k), 1..=N).len();
    assert!(full < 2 * (N as usize - 1) && missing_now > 0, "前提: state は ring に入り切っていない (ring {full} 行、 state の record {})", 2 * (N - 1));
    // c3 が受け続ける (受けて ack すると ring が回収され、 consumer の周期で残りが載る)
    let mut cut = false;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        c3.pull();
        c1.sync();
        let missing = differing(&c1, |k| c3.v(k), 1..=N).len();
        // 前提: c3 は途中まで載った所を受けた (一部だけ届いている時がある)
        cut |= missing > 0 && missing < missing_now;
        // ring の残りも c3 が受け終えたら終わり (c3 の cursor の手前で落ちる record も、 ring からは回収される)
        if missing == 0 || (ring_len(&host_eng) <= 4 && c3.cursor(1) >= host_cursor(&host, 1)) {
            break;
        }
        assert!(std::time::Instant::now() < end, "60 秒で届き終わらない (c3 で食い違う行 {missing}、 ホストの ring {} 行)", ring_len(&host_eng));
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let missing = differing(&c1, |k| c3.v(k), 1..=N);
    assert!(cut, "前提: c3 は、 state が途中まで載った所を受けた");
    assert!(
        missing.is_empty(),
        "ホストの ring より大きい state: c3 で v が食い違う行 {} (先頭 {:?})。 行そのものが無い {}",
        missing.len(),
        &missing[..missing.len().min(5)],
        (1..=N).filter(|k| c3.row(*k).is_none()).count()
    );
}

/// ホストの ring に載った c1 の state の record は、 版数の昇順。 前提: state の順 (列ごと) は昇順ではない。
#[test]
fn a_relayed_state_is_in_version_order_on_the_hosts_ring() {
    let dir = Dir::new("order");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1), &url);
    let c3 = client(db(&dir.db(3), 3), &url);
    c1.write(1, 1);
    c1.eng.oplog_commit();
    c1.sync();
    let before = host_cursor(&host, 1);
    for k in 2..=50 {
        c1.write(k, k);
    }
    c1.eng.oplog_commit();
    let (records, _) = c1.eng.state_records();
    let fresh: Vec<Hlc> = records.iter().filter(|r| r.hlc > before).map(|r| r.hlc).collect();
    assert!(fresh.len() >= 98 && fresh.windows(2).any(|w| w[0] > w[1]), "前提: state の順は版数の昇順ではない");
    ask_for_state(&host, &c1);
    c1.push();
    let relayed: Vec<Hlc> = host_eng
        .pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| r.author_peer == 1 && r.hlc > before)
        .map(|r| r.hlc)
        .collect();
    assert!(relayed.len() >= 98, "前提: state の record がホストの ring に載った ({} 行)", relayed.len());
    assert!(relayed.windows(2).all(|w| w[0] <= w[1]), "ホストの ring の、 state の record が版数の昇順でない");
    for _ in 0..2 {
        c3.sync();
    }
    assert_eq!(differing(&c1, |k| c3.v(k), 1..=50), Vec::<u32>::new());
}

/// 並べ直しても、 語の Vocab はそれを指す Tie の前にある。 state の中で Vocab は、 最初に走査した cell (行 2、 後から書いた) の版数で
/// 出る。 同じ語を先に書いた行 3 の Tie は、 版数の順ではその Vocab より前に来る。
#[test]
fn a_word_reaches_a_row_whose_cell_is_older_than_the_vocab_record() {
    let dir = Dir::new("word");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1), &url);
    let c3 = client(db(&dir.db(3), 3), &url);
    c1.write(1, 1);
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    let (r2, r3) = (c1.write(2, 2), c1.write(3, 3));
    c1.eng.tie_text_to(r3, "notes.t", "w");
    c1.eng.tie_text_to(r2, "notes.t", "w");
    c1.eng.oplog_commit();
    let t_himo = c1.eng.himo_id("notes.t").unwrap() as u16;
    let (records, _) = c1.eng.state_records();
    let vocab = records.iter().find(|r| matches!(&r.op, DecodedOp::Vocab { bytes, .. } if bytes == b"w")).map(|r| r.hlc).expect("前提: state に語 w の Vocab");
    assert!(c1.eng.cell_hlc(r3, t_himo) < vocab, "前提: 行 3 の cell は、 state の Vocab の版数 (行 2 の cell) より前");
    ask_for_state(&host, &c1);
    c1.push();
    for _ in 0..2 {
        c3.sync();
    }
    let t = |e: &Engine, k: u32| {
        e.pull_raw("notes.k", k).first().and_then(|r| e.get_text_owned(*r, "notes.t")).map(|b| String::from_utf8(b).unwrap())
    };
    let want = Some("w".to_string());
    assert_eq!([t(&host_eng, 2), t(&host_eng, 3)], [want.clone(), want.clone()], "前提: ホストに state が当たった");
    assert_eq!([t(&c3.eng, 2), t(&c3.eng, 3)], [want.clone(), want], "c3 の行 2 / 行 3 の t: Vocab がそれを指す Tie の後ろに載った");
}

fn differing(a: &Client, b_row: impl Fn(u32) -> Option<u64>, keys: std::ops::RangeInclusive<u32>) -> Vec<u32> {
    keys.filter(|k| a.v(*k) != b_row(*k)).collect()
}
