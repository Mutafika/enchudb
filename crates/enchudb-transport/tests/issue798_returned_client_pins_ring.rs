//! #798: 外された client が戻って 1 回だけ受けた後、 ホストの ring (`_sync_ops`) の回収が止まらない。
//!
//! 黙ったので外した client は回収の数から外す (`forget_sync_peer`)。 その client が戻って受けると、 ホストは返事で `STALE` を
//! 返す (#712) が、 同じ要求の cursor を ack として受けていた = client の消化の位置が置き直り、 回収の数に戻った。 client は
//! `rebase_required` に従って新しい peer id の DB に移るので、 古い id ではもう来ない。 「外した後に戻ってきた」 client は
//! 二度と外れないので、 回収の線がその位置で止まり、 ring が埋まる一方だった。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue798-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `v` (Number) を持つ表 `notes`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 16_384).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
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
    fn insert(&self, k: u32, v: u32) {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
    }
    /// 名前 `k` の行の `v` を書き換える (行は増えない、 record だけが増える)。
    fn set(&self, k: u32, v: u32) {
        let e = self.eng.pull_raw("notes.k", k)[0];
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
    }
    fn v(&self, k: u32) -> Option<u64> {
        self.eng.pull_raw("notes.k", k).first().and_then(|e| self.eng.get(*e, "notes.v"))
    }
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
    }
    fn pull(&self) {
        for _ in 0..4 {
            let out = self.s.pull_once(HOST);
            if !out.history_truncated {
                break;
            }
            for a in out.truncated_authors {
                assert!(self.s.bootstrap_pull_via(HOST, a).is_some(), "書き手 {a} の state を取れない");
            }
        }
    }
    fn sync(&self) {
        self.push();
        self.pull();
    }
}

/// ホストが client ごとの状態を置く file (`<cursor_path>.clients`) の、 client `id` の行の (dropped, returned)。
fn host_marks(cursors: &std::path::Path, id: PeerId) -> Option<(bool, bool)> {
    let text = std::fs::read_to_string(format!("{}.clients", cursors.display())).ok()?;
    text.lines().find_map(|l| {
        let f: Vec<&str> = l.split(' ').collect();
        match f.as_slice() {
            ["C", c, _, _, _, dropped, returned] if c.parse() == Ok(id) => Some((*dropped == "1", *returned == "1")),
            _ => None,
        }
    })
}

/// ホストの回収の数に `peer` が居るか (`_sync_peers` の行に消化の位置があるか)。
fn counted(host: &Engine, peer: PeerId) -> bool {
    host.pull_raw("_sync_peers.peer_id", peer).iter().any(|e| host.get(*e, "_sync_peers.consumed_lsn").is_some())
}

/// `c2_returns`: 外された c2 が戻って 1 回だけ受けるか (false = 対照: 外されたまま来ない)。
/// 戻り値: (c1 が書き続ける間にホストの ring の行が減った回数、 最後の書き込みが c3 に届いたか)。
fn run(tag: &str, c2_returns: bool) -> (usize, bool) {
    let dir = Dir::new(tag);
    let cursors: PathBuf = format!("{}.cursors", dir.db(HOST)).into();
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
    host.set_silence_limit(Duration::from_secs(600));
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1), &url);
    let c2 = client(db(&dir.db(2), 2), &url);
    let c3 = client(db(&dir.db(3), 3), &url);
    c1.insert(1, 10);
    for _ in 0..2 {
        c1.sync();
        c2.sync();
        c3.sync();
    }
    assert_eq!([c2.v(1), c3.v(1)], [Some(10), Some(10)], "前提: c2 / c3 が行を受けた");
    // c2 が黙る。 c1 / c3 が来た直後に上限を縮め、 ホストが c2 を外した印を置くまで待つ
    c1.sync();
    c3.sync();
    host.set_silence_limit(Duration::from_secs(2));
    let t0 = std::time::Instant::now();
    while host_marks(&cursors, 2) != Some((true, false)) {
        assert!(t0.elapsed() < Duration::from_secs(60), "前提: ホストが c2 を外さない: {:?}", host_marks(&cursors, 2));
        std::thread::sleep(Duration::from_millis(100));
        c1.sync();
        c3.sync();
    }
    // 以後は誰も外さない (c1 / c3 は下で長く書く)
    host.set_silence_limit(Duration::from_secs(600));
    assert_eq!([host_marks(&cursors, 1), host_marks(&cursors, 3)], [Some((false, false)); 2], "前提: c1 / c3 は外されていない");
    // 留守の間に c1 が書く (戻った c2 が受ける物がある)
    for v in 11..=20 {
        c1.set(1, v);
    }
    c1.sync();
    c3.sync();
    assert_eq!(c3.v(1), Some(20), "前提: 留守の間の書き込みが c3 に届いた");
    assert_eq!([1, 2, 3].map(|p| counted(&host_eng, p)), [true, false, true], "前提: 外した c2 だけが回収の数に居ない");
    if c2_returns {
        // c2 が戻って 1 回だけ受ける。 取り直せと言われ、 この id ではもう来ない
        let _ = c2.s.pull_once(HOST);
        assert_eq!(c2.v(1), Some(20), "前提: 戻った c2 は留守の間の書き込みを受けた");
        assert!(c2.t.rebase_required(), "前提: 戻った c2 に rebase_required が立つ (#712)");
        assert_eq!(host_marks(&cursors, 2), Some((false, true)), "前提: ホストは c2 を 「外した後に戻ってきた」 と見ている");
    }
    // c1 が書き続け、 c3 が受けて ack する。 ring の半分を越えれば回収が走るはず
    let cap = host_eng.table_eid_usage("_sync_ops").unwrap().capacity;
    let live = || host_eng.table_eid_usage("_sync_ops").unwrap().live;
    let mut shrank = 0usize;
    let mut before = live();
    let mut n = 0u32;
    let mut written = 0u32;
    // ring の 3 倍の record を書く (回収が無ければ満杯になる量)
    while written < cap * 3 {
        for _ in 0..200 {
            n += 1;
            c1.set(1, n);
            written += 1;
        }
        c1.sync();
        c3.sync();
        c1.sync();
        let now = live();
        if now < before {
            shrank += 1;
        }
        before = now;
    }
    assert!(written > cap, "前提: ring の大きさ ({cap}) より多く書いた");
    for _ in 0..3 {
        c1.sync();
        c3.sync();
    }
    let delivered = c3.v(1) == Some(n as u64);
    if c2_returns {
        assert!(!counted(&host_eng, 2), "戻ってきた c2 が回収の数に戻った");
        // 取り直さずに受け続ける c2 も、 回収された分は抜けを言われて取り直し、 今の値に届く
        c2.pull();
        c2.pull();
        assert_eq!(c2.v(1), Some(n as u64), "数に居ない c2 が、 回収の後の値を受け取れない");
        assert!(!counted(&host_eng, 2), "受け続けた c2 が回収の数に戻った");
        // 前の版が c2 を数に戻したまま保存したホストを開き直す: 開く時に外す
        drop(host);
        host_eng.ack_sync(2, 1).unwrap();
        assert!(counted(&host_eng, 2), "前提: c2 を回収の数に戻した (前の版が残す形)");
        let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
        assert!(!counted(&host_eng, 2), "開き直したホストが、 戻ってきた c2 を回収の数に残した");
        assert_eq!([1, 3].map(|p| counted(&host_eng, p)), [true, true], "来続けている c1 / c3 を数から外した");
        drop((c1, c2, c3, host));
        return (shrank, delivered);
    }
    eprintln!("[{tag}] counted {:?}", [1, 2, 3].map(|p| counted(&host_eng, p)));
    eprintln!("[{tag}] cap {cap} written {written} live {} shrank {shrank} c3.v {:?} want {n}", live(), c3.v(1));
    drop((c1, c2, c3, host));
    (shrank, delivered)
}

#[test]
fn a_returned_client_that_left_does_not_pin_the_host_ring() {
    let (shrank, delivered) = run("ctl", false);
    assert!(shrank > 0 && delivered, "前提 (対照): c2 が外されたままなら回収が進み、 配送も続く (回収 {shrank} 回、 届いた {delivered})");
    let (shrank, delivered) = run("ret", true);
    assert!(shrank > 0, "外された c2 が 1 回受けただけで、 ホストの ring の回収が止まった");
    assert!(delivered, "ホストの ring が満杯で、 c1 の書き込みが c3 に届かない");
}
