//! #699: ホストは、 自分が求めていない state (`POST /v2/state`) を受けない。
//!
//! 前は送り手も、 求めたかも確かめなかった: 空で complete の state を 1 つ送ると、 ghost sweep が 「その書き手の行は全部消えた」 と
//! 見てホストの行を消し、 ホストの削除として全 client に配った (ホストが署名必須 + ACL でも、 record が 0 件なので確かめる物が無い)。
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(std::path::PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("review-host-boot-{tag}-{}", std::process::id()));
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
    s: Syncer,
}

fn client(eng: Arc<Engine>, url: &str) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t);
    s.serve_state();
    Client { eng, s }
}

impl Client {
    fn write(&self, k: u32, v: u32) {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
        std::thread::sleep(Duration::from_millis(3));
    }
    fn row(&self, k: u32) -> Option<u64> {
        self.eng.pull_raw("notes.k", k).first().copied()
    }
    fn v(&self, k: u32) -> Option<u64> {
        self.row(k).and_then(|e| self.eng.get(e, "notes.v"))
    }
    fn t(&self, k: u32) -> Option<String> {
        self.row(k).and_then(|e| self.eng.get_text(e, "notes.t").map(|b| String::from_utf8_lossy(b).into_owned()))
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

fn raw_post(addr: std::net::SocketAddr, path: &str, body: &[u8]) -> u16 {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    let head = format!("POST {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    s.write_all(head.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out);
    String::from_utf8_lossy(&out).split(' ').nth(1).and_then(|c| c.parse().ok()).unwrap_or(0)
}

/// 鍵も peer id も持たない誰かが、 書き手 1 の 「空で complete の state」 を送る。 ホストは署名必須 + ACL (書き手 1 と 2 だけ)。
#[test]
fn an_unsolicited_empty_complete_state_deletes_nothing() {
    let got = empty_state_attack("s", false);
    assert_eq!(got, [5, 5, 5], "求めていない空の state で、 書き手 1 の行が消えた");
}

/// client も署名必須の形 (前は、 ホストの掃除の削除を client が断って、 ホストだけ空のまま食い違った)。
#[test]
fn an_unsolicited_empty_complete_state_when_clients_require_signatures() {
    let got = empty_state_attack("s2", true);
    assert_eq!(got, [5, 5, 5], "求めていない空の state で、 ホストの書き手 1 の行が消えた");
}

fn empty_state_attack(tag: &str, clients_require: bool) -> [usize; 3] {
    let dir = Dir::new(tag);
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    // client 1 と 2 は鍵を持ち、 ホストは署名必須 + 書き手は 1 と 2 だけ
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    for c in [&c1, &c2] {
        let kp = Arc::new(enchudb::keys::Keypair::generate());
        c.eng.set_keypair(Some(kp.clone()));
        host_eng.pubkeys().force_register(c.eng.peer_id(), &kp.public_bytes());
        for other in [&c1, &c2] {
            other.eng.pubkeys().force_register(c.eng.peer_id(), &kp.public_bytes());
        }
    }
    host.syncer().set_require_signature(true);
    if clients_require {
        c1.s.set_require_signature(true);
        c2.s.set_require_signature(true); // ホストが中継した record の署名を client も確かめる
    }
    host_eng.acl().add_writer(1);
    host_eng.acl().add_writer(2);
    for k in 1..=5 {
        c1.write(k, k * 10);
    }
    for _ in 0..2 {
        c1.sync();
        c2.sync();
    }
    let count = |e: &Engine| (1..=5).map(|k| e.pull_raw("notes.k", k).len()).sum::<usize>();
    assert_eq!([count(&host_eng), count(&c2.eng)], [5, 5], "前提: 署名付きの 5 行がホストと c2 に届いた");
    // 鍵も peer id も持たない誰かが、 書き手 1 の 「空で完全な state」 を送る
    let mut body = Vec::new();
    let batch = enchudb::transport::encode_batch(&[]);
    body.extend_from_slice(&(batch.len() as u32).to_le_bytes());
    body.extend_from_slice(&batch);
    body.extend_from_slice(&[0u8; 16]); // as_of = ZERO
    body.push(1); // complete
    let status = raw_post(host.addr(), "/v2/state?author=1", &body);
    assert_eq!(status, 409, "求めていない state は断る");
    println!("\n[S] POST /v2/state?author=1 (空・complete) → {status}、 ホストの行 {} / 5", count(&host_eng));
    for _ in 0..2 {
        c1.sync();
        c2.sync();
    }
    let got = [count(&host_eng), count(&c1.eng), count(&c2.eng)];
    println!("[S] client も署名必須 = {clients_require}: その後の行の数 [host, c1, c2] = {got:?} (どれも 5 のはず)");
    drop((c1, c2, host));
    got
}
