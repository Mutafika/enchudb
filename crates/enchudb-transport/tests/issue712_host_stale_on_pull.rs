//! #712: 黙ったので外された client が戻って受けるだけの時も、 ホストは pull の返事で知らせる (`rebase_required`)。
//!
//! 前は `STALE` を返すのは client が自分の record を出した時だけだった。 受けるだけの client は、 ホストの回収で抜けを言われて
//! ホストの state で取り直すが、 その state は replica の物 (`complete: false`) で、 留守の間に消えた行の掃除は走らない =
//! 消えた行を持ったまま、 何も知らされなかった。
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue712-{tag}-{}", std::process::id()));
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

#[test]
fn a_returning_reader_is_told_to_rebase() {
    let dir = Dir::new("g");
    let cursors: PathBuf = format!("{}.cursors", dir.db(HOST)).into();
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
    host.set_silence_limit(Duration::from_secs(600));
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1), &url);
    let c2 = client(db(&dir.db(2), 2), &url);
    let c3 = client(db(&dir.db(3), 3), &url);
    c1.write(1, 10);
    c1.write(2, 20);
    for _ in 0..2 {
        c1.sync();
        c2.sync();
        c3.sync();
    }
    assert_eq!([c3.v(1), c3.v(2)], [Some(10), Some(20)], "前提: c3 が 2 行を受けた");
    assert!(!c3.t.rebase_required() && !c2.t.rebase_required(), "前提: まだ誰も外されていない");
    // c3 が黙る。 c1 / c2 が来た直後に上限を縮めて待つ (http_host_horizon.rs の作法)
    c1.sync();
    c2.sync();
    host.set_silence_limit(Duration::from_secs(3));
    // c1 / c2 は来続け、 c3 だけが上限を越える (状態で待つ: ホストが c3 を外した印を置くまで)
    let t0 = std::time::Instant::now();
    while host_marks(&cursors, 3) != Some((true, false)) {
        assert!(t0.elapsed() < Duration::from_secs(60), "前提: ホストが c3 を外さない: {:?}", host_marks(&cursors, 3));
        std::thread::sleep(Duration::from_millis(100));
        c1.sync();
        c2.sync();
    }
    assert_eq!([host_marks(&cursors, 1), host_marks(&cursors, 2)], [Some((false, false)); 2], "前提: 来続けた c1 / c2 は外されていない");
    // 留守の間に c1 が行 1 を消し、 ホストが回収する
    let e = c1.row(1).unwrap();
    c1.eng.delete(e);
    c1.eng.oplog_commit();
    for i in 0..8 {
        c1.write(100 + i, 0);
        c1.sync();
        c2.sync();
        host_eng.reclaim_sync_ops();
    }
    assert_eq!(host_eng.pull_raw("notes.k", 1).len(), 0, "前提: ホストで行 1 は消えた");
    let deletes_left = host_eng
        .pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| matches!(r.op, enchudb_oplog::oplog::DecodedOp::Delete { .. }))
        .count();
    assert_eq!(deletes_left, 0, "前提: 削除の record はホストの ring から回収された (差分では届かない)");
    // c3 が戻って受ける (出す物は無い)
    let before = c3.eng.current_sync_lsn();
    let mut booted = Vec::new();
    for _ in 0..3 {
        booted.extend(c3.sync());
    }
    assert_eq!(c3.eng.current_sync_lsn(), before, "前提: c3 は自分の record を出していない");
    assert_eq!(host_marks(&cursors, 3), Some((false, true)), "前提: ホストは c3 を 「外した後に戻ってきた」 と見ている");
    assert_eq!(c3.v(107), Some(0), "戻った client も受け取れる (留守の間に足された行)");
    assert!(
        c3.t.rebase_required(),
        "留守の間に消えた行を持ったまま、 取り直せと言われない (c3 の行 1 = {:?}、 取り直した書き手 {booted:?})",
        c3.v(1)
    );
    // 外されていない client には立たない
    assert!(!c1.t.rebase_required() && !c2.t.rebase_required(), "来続けた client に rebase_required が立った");
    drop((c1, c2, c3, host));
}
