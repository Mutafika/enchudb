//! #713: ホストが止まっていた間は、 client の沈黙に数えない。
//!
//! 前は client の `last_seen` が壁時計で、 ホストが沈黙の上限 (既定 30 日) より長く止まった後は、 開き直して最初の client が来た
//! 時点で他の全員が 「黙った」 として外され、 その後に来て record を出すと `STALE` で断られた (新しい peer id の DB に作り直すまで
//! 書き込みを受け付けない)。 client は黙っていたのではなく、 ホストに届かなかっただけ。
//!
//! 30 日は待てないので、 止まっていた時間 (6 秒) より上限を短く (5 秒) して同じ順を踏む。 時計そのもの (置き場の値の続きから
//! 進む、 壁時計が 60 日進んでいても) は `host.rs` の単体 `the_host_clock_resumes_from_the_saved_value` が見ている。
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue713-{tag}-{}", std::process::id()));
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
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
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
    fn write(&self, k: u32, v: u32) {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
        // 次の書き込みの HLC の壁時計を進める (別の client の書き込みとの前後を、 書いた順にする)
        std::thread::sleep(Duration::from_millis(3));
    }
    fn row(&self, k: u32) -> Option<u64> {
        self.eng.pull_raw("notes.k", k).first().copied()
    }
    fn v(&self, k: u32) -> Option<u64> {
        self.row(k).and_then(|e| self.eng.get(e, "notes.v"))
    }
    fn t(&self, k: u32) -> Option<String> {
        self.row(k).and_then(|e| self.eng.get_text_owned(e, "notes.t").map(|b| String::from_utf8_lossy(&b).into_owned()))
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

const LIMIT: Duration = Duration::from_secs(5);
const DOWN: Duration = Duration::from_secs(6);

#[test]
fn a_host_that_was_down_longer_than_the_silence_limit_keeps_its_clients() {
    let dir = Dir::new("d");
    let cursors: PathBuf = format!("{}.cursors", dir.db(HOST)).into();
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
    host.set_silence_limit(Duration::from_secs(3600));
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    c1.write(1, 10);
    c2.write(2, 20);
    for _ in 0..2 {
        c1.sync();
        c2.sync();
    }
    assert_eq!([c1.v(2), c2.v(1)], [Some(20), Some(10)], "前提: 2 台が揃った");
    // ここから c2 が次に来るまでのうち、 ホストが動いていた時間を測る (上限より短いこと = c2 は黙っていない)
    let c2_last_came = std::time::Instant::now();
    // ホストが止まる
    drop(host);
    drop(host_eng);
    let up_before_stop = c2_last_came.elapsed();
    let stopped = std::time::Instant::now();
    std::thread::sleep(DOWN);
    // 開き直す (同じ置き場)
    let host_eng = Engine::open_concurrent_with_oplog(&dir.db(HOST), 16 << 20).unwrap();
    host_eng.set_peer_id(HOST);
    let down = stopped.elapsed();
    let restarted = std::time::Instant::now();
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
    let url = format!("http://{}", host.addr());
    let t1 = Arc::new(HttpHostTransport::connect(url.clone(), 1).unwrap());
    let t2 = Arc::new(HttpHostTransport::connect(url.clone(), 2).unwrap());
    let (s1, s2) = (Syncer::new(c1.eng.clone(), t1.clone()), Syncer::new(c2.eng.clone(), t2.clone()));
    // c1 が先に来る。 その後で上限を、 止まっていた時間より短くする (= 止まっていた間が上限を越えた状態)
    c1.eng.oplog_sync().unwrap();
    s1.publish_since(Hlc::ZERO);
    s1.pull_once(HOST);
    host.set_silence_limit(LIMIT);
    // c2 が (ホストが戻ったので) 新しい行を出す
    c2.write(3, 30);
    c2.eng.oplog_sync().unwrap();
    s2.publish_since(Hlc::ZERO);
    s2.pull_once(HOST);
    let up = up_before_stop + restarted.elapsed();
    // c1 がもう一度来る (黙った client を外す計算が、 c2 の後にも走る)
    c1.write(4, 40);
    c1.eng.oplog_sync().unwrap();
    s1.publish_since(Hlc::ZERO);
    s1.pull_once(HOST);
    assert!(down > LIMIT, "前提: ホストが止まっていた時間 ({down:?}) が上限 ({LIMIT:?}) を越えていない");
    assert!(up < LIMIT, "前提: c2 が来なかった間にホストが動いていた時間 ({up:?}) が上限 ({LIMIT:?}) を越えた (この機械が遅すぎる)");
    let got = [host_eng.pull_raw("notes.k", 3).len(), host_eng.pull_raw("notes.k", 4).len()];
    let marks = [host_marks(&cursors, 1), host_marks(&cursors, 2)];
    let stale = [t1.rebase_required(), t2.rebase_required()];
    drop((s1, s2, c1, c2, host));
    assert_eq!(stale, [false, false], "ホストが止まっていただけなのに、 client を断った (rebase_required [c1, c2]、 ホストの印 {marks:?})");
    assert_eq!(got, [1, 1], "ホストに届いた行 [c2 の 3, c1 の 4]");
    assert_eq!(marks, [Some((false, false)); 2], "ホストが client を外した / 戻ったと見ている (dropped, returned)");
}
