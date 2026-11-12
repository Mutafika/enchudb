//! #711: ホストの state に、 別の client が書き換えた Tag の cell が入る。
//!
//! ホストが配る書き手 a の state は a の record の形 (vid は a の空間)。 a の行を別の client が書き換えた Tag の語は a の空間に
//! 無く、 前は state から落ちていた。 ホストを置く形では、 書き換えた側の state は自分の行だけなので、 その書き換えはどの state
//! にも入らず、 回収より後に来た client に届かなかった。 今は、 書いた client の record の形 (書き手 = 版数の peer) で出す。
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue711-{tag}-{}", std::process::id()));
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

fn host_t(host_eng: &Engine, k: u32) -> Option<String> {
    let e = *host_eng.pull_raw("notes.k", k).first()?;
    host_eng.get_text_owned(e, "notes.t").map(|b| String::from_utf8_lossy(&b).into_owned())
}

#[test]
fn a_tag_cell_edited_by_another_client_is_in_the_hosts_state() {
    let dir = Dir::new("b2");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    let e = c1.eng.entity_in("notes").unwrap();
    c1.eng.tie_to(e, "notes.k", 1u32);
    c1.eng.tie_text_to(e, "notes.t", "written-by-1");
    c1.eng.oplog_commit();
    // c1 の、 誰も書き換えない行 (対照)
    let e = c1.eng.entity_in("notes").unwrap();
    c1.eng.tie_to(e, "notes.k", 2u32);
    c1.eng.tie_text_to(e, "notes.t", "untouched");
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c2.sync();
    }
    assert_eq!(c2.t(1).as_deref(), Some("written-by-1"), "前提: c2 が c1 の行を受けた");
    std::thread::sleep(Duration::from_millis(3));
    c2.eng.tie_text_to(c2.row(1).unwrap(), "notes.t", "edited-by-2");
    c2.eng.oplog_commit();
    // 水位を押す行を足して、 全員が全部受ける
    c2.write(8, 80);
    c1.write(9, 90);
    for _ in 0..3 {
        c1.sync();
        c2.sync();
    }
    c1.write(10, 100);
    c2.write(11, 110);
    for _ in 0..3 {
        c1.sync();
        c2.sync();
    }
    assert_eq!(c1.t(1).as_deref(), Some("edited-by-2"), "前提: c1 に書き換えが届いた");
    assert_eq!(host_t(&host_eng, 1).as_deref(), Some("edited-by-2"), "前提: ホストに書き換えが当たった");
    let dropped_before = host_eng.state_records_dropped();
    // 書き換えの語 (c2 の Vocab) がホストの ring にあるか
    let in_ring = || {
        host_eng
            .pending_sync_ops(0)
            .iter()
            .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
            .any(|r| matches!(&r.op, enchudb_oplog::oplog::DecodedOp::Vocab { bytes, .. } if bytes == b"edited-by-2"))
    };
    assert!(in_ring(), "前提: 書き換えの record はホストの ring に載っていた");
    assert!(host_eng.reclaim_sync_ops() > 0, "前提: ホストが回収した");
    assert!(!in_ring(), "前提: 書き換えの record はホストの ring から消えた (差分では届かない)");
    // 初めて来た c3: 抜けを知って、 ホストの state で取り直す
    let c3 = client(db(&dir.db(3), 3), &url);
    let booted = c3.pull();
    assert!(booted.contains(&1), "前提: c3 は書き手 1 を取り直した ({booted:?})");
    for _ in 0..3 {
        c3.sync();
    }
    let got = [host_t(&host_eng, 1), c1.t(1), c2.t(1), c3.t(1)];
    assert!(
        got.iter().all(|t| t.as_deref() == Some("edited-by-2")),
        "行 1 の t [host, c1, c2, c3]: 取り直した c3 だけ、 別の client の書き換えを持たない: {got:?}"
    );
    assert_eq!(c3.t(2).as_deref(), Some("untouched"), "書き換えていない行の Tag");
    assert_eq!(host_eng.state_records_dropped() - dropped_before, 0, "ホストが state から cell を落とした");
    // 取り直しの後も差分で受け続ける: c2 がもう一度書き換える
    std::thread::sleep(Duration::from_millis(3));
    c2.eng.tie_text_to(c2.row(1).unwrap(), "notes.t", "edited-again-by-2");
    c2.eng.oplog_commit();
    for _ in 0..2 {
        c2.sync();
        c1.sync();
        assert_eq!(c3.sync(), Vec::<PeerId>::new(), "取り直した後にまた抜けを言われた");
    }
    let got = [host_t(&host_eng, 1), c1.t(1), c2.t(1), c3.t(1)];
    assert!(got.iter().all(|t| t.as_deref() == Some("edited-again-by-2")), "その後の書き換え [host, c1, c2, c3]: {got:?}");
    drop((c1, c2, c3, host));
}
