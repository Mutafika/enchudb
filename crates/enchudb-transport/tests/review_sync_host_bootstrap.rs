//! review (0.37.4): ホストの回収の後に来た client の取り直し (`bootstrap_pull_via(HOST, author)`)。 ホストが配る state は
//! replica の物 (`Engine::state_records_for(author)`): author の行の今の cell を、 cell の版数のまま author の record として出し、
//! `as_of` = 出した record の版数の最大。
//!
//! - B1: author の行の cell を別の client が書き換えていると、 `as_of` は書き換えた client の時計の版数になる。 受け手は
//!   `cursor[author] = as_of` にするので、 author がそれより前に書いてまだ出していなかった record を、 後から配られても cursor で落とす
//! - B2: author の行の Tag の cell を別の client が書き換えていると、 その語は author の辞書の写像に無いので state から落ちる
//!   (`state_records_dropped`)。 書き換えた client の state は自分の行だけなので、 その書き換えはどの state にも入らない

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

/// B1: 取り直した cursor が、 author のまだ出していない (より前の時刻の) record を越える。
#[test]
fn a_bootstrap_cursor_passes_the_authors_unpublished_older_write() {
    let dir = Dir::new("b1");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    // c1 の行 1 と、 回収の水位を押す行 9。 c2 も受ける
    c1.write(1, 10);
    c1.write(9, 90);
    for _ in 0..2 {
        c1.sync();
        c2.sync();
    }
    assert_eq!(c2.v(1), Some(10), "前提: c2 が c1 の行を受けた");
    // c1 はオフラインで行 2 を書く (出さない)
    c1.write(2, 20);
    // その後で c2 が c1 の行 1 を書き換えて出す (版数は c2 の時計、 c1 の行 2 より後)
    let e = c2.row(1).unwrap();
    c2.eng.tie_to(e, "notes.v", 11u32);
    c2.eng.oplog_commit();
    c2.sync();
    c2.sync();
    assert_eq!(host_eng.get(host_eng.pull_raw("notes.k", 1)[0], "notes.v"), Some(11), "前提: ホストに c2 の書き換えが当たった");
    // ホストが回収する (c1 の最初の record が ring から消え、 c1 の floor ができる)
    let reclaimed = host_eng.reclaim_sync_ops();
    println!("[B1] ホストの回収: {reclaimed} 行、 floor {:?}", host_eng.sync_reclaimed_floors());
    assert!(reclaimed > 0, "前提: ホストが回収した");
    // 初めて来た c3: 抜けを知って取り直す
    let c3 = client(db(&dir.db(3), 3), &url);
    let booted = c3.pull();
    println!("[B1] c3 が取り直した書き手: {booted:?}、 c3 の cursor: {:?}", c3.s.cursors_of(HOST));
    assert!(booted.contains(&1), "前提: c3 は書き手 1 を取り直した ({booted:?})");
    assert_eq!(c3.v(1), Some(11), "前提: 取り直しで行 1 が今の値で入った");
    // c1 が戻ってきて、 オフラインの行 2 を出す
    for _ in 0..3 {
        c1.sync();
        c2.sync();
        c3.sync();
    }
    let got = [host_eng.pull_raw("notes.k", 2).len(), c1.eng.pull_raw("notes.k", 2).len(), c2.eng.pull_raw("notes.k", 2).len(), c3.eng.pull_raw("notes.k", 2).len()];
    println!("[B1] 行 2 の数 [host, c1, c2, c3] = {got:?}、 c3 の cursor: {:?}", c3.s.cursors_of(HOST));
    drop((c1, c2, c3, host));
    assert_eq!(got, [1, 1, 1, 1], "c1 が後から出した行 2 が、 取り直した c3 にだけ届かない");
}

/// B1 の対照: c2 が c1 の行を書き換えなければ (`as_of` は c1 の時計の版数)、 c1 のオフラインの行は c3 に届く。
#[test]
fn control_without_the_cross_author_edit() {
    let dir = Dir::new("b1c");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    c1.write(1, 10);
    c1.write(9, 90);
    for _ in 0..2 {
        c1.sync();
        c2.sync();
    }
    c1.write(2, 20);
    c2.write(7, 70); // c2 は自分の行を書くだけ
    c2.sync();
    c2.sync();
    assert!(host_eng.reclaim_sync_ops() > 0, "前提: ホストが回収した");
    let c3 = client(db(&dir.db(3), 3), &url);
    let booted = c3.pull();
    assert!(booted.contains(&1), "前提: c3 は書き手 1 を取り直した ({booted:?})");
    for _ in 0..3 {
        c1.sync();
        c2.sync();
        c3.sync();
    }
    let got = c3.eng.pull_raw("notes.k", 2).len();
    drop((c1, c2, c3, host));
    assert_eq!(got, 1, "対照: 書き換えが無くても届かない (別の原因)");
}

/// B2: 別の client が書き換えた Tag の cell が、 ホストの state から落ちる。
#[test]
fn a_tag_cell_edited_by_another_client_is_missing_from_the_hosts_state() {
    let dir = Dir::new("b2");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    let e = c1.eng.entity_in("notes").unwrap();
    c1.eng.tie_to(e, "notes.k", 1u32);
    c1.eng.tie_text_to(e, "notes.t", "written-by-1");
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
    let before = host_eng.state_records_dropped();
    let reclaimed = host_eng.reclaim_sync_ops();
    let left: Vec<String> = host_eng
        .pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(|r| format!("author {} {:?}", r.author_peer, r.op))
        .collect();
    println!("[B2] ホストの回収: {reclaimed} 行、 ring の残り: {left:?}");
    assert!(reclaimed > 0, "前提: ホストが回収した");
    assert!(!left.iter().any(|l| l.contains("edited-by-2")), "前提: 書き換えの record は ring から消えた ({left:?})");
    let c3 = client(db(&dir.db(3), 3), &url);
    let booted = c3.pull();
    for _ in 0..3 {
        c3.sync();
    }
    let got = [
        host_eng.pull_raw("notes.k", 1).first().and_then(|e| host_eng.get_text(*e, "notes.t").map(|b| String::from_utf8_lossy(b).into_owned())),
        c1.t(1),
        c2.t(1),
        c3.t(1),
    ];
    println!(
        "[B2] c3 が取り直した書き手: {booted:?}、 行 1 の t [host, c1, c2, c3] = {got:?}、 ホストが state から落とした cell: {}",
        host_eng.state_records_dropped() - before
    );
    drop((c1, c2, c3, host));
    assert!(got.iter().all(|t| t.as_deref() == Some("edited-by-2")), "取り直した c3 だけ、 別の client の書き換えを持たない: {got:?}");
}

/// G: 黙ったので外された client が戻って受けるだけの時。 ホストは回収済みなので抜けを言い、 client はホストの state で取り直す。
/// ホストの state は replica の物 (`complete: false`) なので、 留守の間に消えた行の掃除 (ghost sweep) は走らない。 client は
/// 何も出していないので `STALE` も受けない = 消えた行を持ったまま、 何も知らされない。
#[test]
fn a_returning_reader_keeps_rows_deleted_while_it_was_away() {
    let dir = Dir::new("g");
    let host_eng = db(&dir.db(HOST), HOST);
    let host =
        HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(format!("{}.cursors", dir.db(HOST)).into())).unwrap();
    host.set_silence_limit(Duration::from_secs(600));
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1), &url);
    let t3 = Arc::new(HttpHostTransport::connect(url.clone(), 3).unwrap());
    let e3 = db(&dir.db(3), 3);
    let s3 = Syncer::new(e3.clone(), t3.clone());
    s3.serve_state();
    let c3 = Client { eng: e3, s: s3 };
    c1.write(1, 10);
    c1.write(2, 20);
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    assert_eq!([c3.v(1), c3.v(2)], [Some(10), Some(20)], "前提: c3 が 2 行を受けた");
    // c3 が黙る。 c1 が来た直後に上限を縮めて待つ (http_host_horizon.rs の作法)
    c1.sync();
    host.set_silence_limit(Duration::from_secs(1));
    std::thread::sleep(Duration::from_millis(1500));
    let e = c1.row(1).unwrap();
    c1.eng.delete(e);
    c1.eng.oplog_commit();
    for i in 0..8 {
        c1.write(100 + i, 0);
        c1.sync();
        host_eng.reclaim_sync_ops();
    }
    assert_eq!(host_eng.pull_raw("notes.k", 1).len(), 0, "前提: ホストで行 1 は消えた");
    let left: Vec<String> = host_eng
        .pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| matches!(r.op, enchudb_oplog::oplog::DecodedOp::Delete { .. }))
        .map(|r| format!("{:?}", r.op))
        .collect();
    assert!(left.is_empty(), "前提: 削除の record はホストの ring から回収された ({left:?})");
    // c3 が戻って受ける (出す物は無い)
    let mut booted = Vec::new();
    for _ in 0..3 {
        booted.extend(c3.sync());
    }
    let got = [c3.eng.pull_raw("notes.k", 1).len(), c3.eng.pull_raw("notes.k", 2).len(), c3.eng.pull_raw("notes.k", 107).len()];
    println!(
        "[G] c3 が取り直した書き手: {booted:?}、 c3 の行 [1 (消えた), 2, 107 (留守の間に足された)] = {got:?}、 rebase_required = {}",
        t3.rebase_required()
    );
    let stale = t3.rebase_required();
    drop((c1, c3, host));
    assert!(got[0] == 0 || stale, "留守の間に消えた行 1 を持ったまま、 取り直せとも言われない (行 {got:?}、 rebase_required {stale})");
}

/// 生の HTTP の POST (試験の中だけ、 127.0.0.1)。 戻り値は status。
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

/// S: ホストの `POST /v2/state?author=a` は、 誰が送ったかを確かめない。 record の無い `complete` の state は、 当てる record が
/// 無いので署名も ACL も通らずに済み、 ghost sweep が 「author a の行は全部消えた」 と見てホストの a の行を全部消す
/// (`Engine::delete` = ホストの削除として全 client に配られる)。 署名必須 + ACL のホストでも。
#[test]
fn an_empty_complete_state_from_anyone_deletes_all_rows_of_an_author() {
    let got = empty_state_attack("s", false);
    assert_eq!(got, [5, 5, 5], "鍵を持たない送り手の空の state で、 書き手 1 の行が消えた");
}

/// client も署名必須なら、 ホストの掃除の削除 (ホストは鍵を持たない = 署名なし) は client で断られる: 消えるのはホストの行だけで、
/// ホストと client が食い違ったままになる (後から来て取り直す client はホストの state を受ける)。
#[test]
fn an_empty_complete_state_from_anyone_when_clients_require_signatures() {
    let got = empty_state_attack("s2", true);
    assert_eq!(got, [5, 5, 5], "鍵を持たない送り手の空の state で、 ホストの書き手 1 の行が消えた");
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

/// D: ホストが上限 (既定 30 日) より長く止まっていた後。 client は黙っていたのではなく、 ホストに届かなかっただけ。 開き直した
/// ホストは、 最初に来た client の時刻を新しくしてから 「黙った client」 を外すので、 2 番目以降に来た client は全部
/// 「外した後に戻ってきた」 扱いになり、 以後の publish を全部 `STALE` で断られる (新しい peer id の DB に作り直すまで)。
/// 上限を 3 秒に縮めて同じ順を踏む (開き直した直後に c1 が来る → 上限を縮める = 止まっていた間が上限を越えた状態)。
#[test]
fn a_host_that_was_down_longer_than_the_silence_limit_refuses_its_clients() {
    let dir = Dir::new("d");
    let cursors: std::path::PathBuf = format!("{}.cursors", dir.db(HOST)).into();
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
    let line_before = host.forget_line();
    // ホストが止まる
    drop(host);
    drop(host_eng);
    std::thread::sleep(Duration::from_secs(4));
    // 開き直す (同じ置き場)。 c1 が先に来て、 その後で 「止まっていた間 > 上限」 にする
    let host_eng = Engine::open_concurrent_with_oplog(&dir.db(HOST), 16 << 20).unwrap();
    host_eng.set_peer_id(HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors)).unwrap();
    let url = format!("http://{}", host.addr());
    let t1 = Arc::new(HttpHostTransport::connect(url.clone(), 1).unwrap());
    let t2 = Arc::new(HttpHostTransport::connect(url.clone(), 2).unwrap());
    let (s1, s2) = (Syncer::new(c1.eng.clone(), t1.clone()), Syncer::new(c2.eng.clone(), t2.clone()));
    c1.eng.oplog_sync().unwrap();
    s1.publish_since(Hlc::ZERO);
    s1.pull_once(HOST);
    host.set_silence_limit(Duration::from_secs(3));
    // c2 が (ホストが戻ったので) 新しい行を出す
    c2.write(3, 30);
    c2.eng.oplog_sync().unwrap();
    s2.publish_since(Hlc::ZERO);
    s2.pull_once(HOST);
    c1.write(4, 40);
    c1.eng.oplog_sync().unwrap();
    s1.publish_since(Hlc::ZERO);
    s1.pull_once(HOST);
    let got = [host_eng.pull_raw("notes.k", 3).len(), host_eng.pull_raw("notes.k", 4).len()];
    println!(
        "[D] 線は止まっていた間に進んでいない: {} / ホストに届いた行 [c2 の 3, c1 の 4] = {got:?} / rebase_required [c1, c2] = [{}, {}]",
        host.forget_line() == line_before,
        t1.rebase_required(),
        t2.rebase_required()
    );
    let stale = t2.rebase_required();
    drop((s1, s2, c1, c2, host));
    assert!(!stale && got[0] == 1, "ホストが止まっていただけなのに、 c2 の書き込みを断った (STALE = {stale}、 行 {got:?})");
}

/// R: host.rs の doc の通りに組んだ client (`Syncer::new(engine, transport)`、 cursor の置き場なし) を開き直す。 ホストが一度でも
/// 回収していると (定常の運用)、 cursor の無い client は 「初めて来た client」 と同じ扱いで抜けを言われ、 pull は何も当てずに返る。
/// アプリが取り直し (`bootstrap_pull_via`) を呼ばない限り、 その client の受け取りは止まったまま。
#[test]
fn a_restarted_client_without_a_cursor_file_is_told_to_bootstrap() {
    let dir = Dir::new("r");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    for k in 1..=6 {
        c1.write(k, k);
    }
    for _ in 0..3 {
        c1.sync();
        c2.sync();
    }
    assert!(host_eng.reclaim_sync_ops() > 0, "前提: ホストが回収した");
    assert_eq!(c2.v(6), Some(6), "前提: c2 は全部受けている");
    // c2 のプロセスを開き直す (engine はそのまま、 Syncer と transport を作り直す = cursor は memory だけだった)
    let eng2 = c2.eng.clone();
    drop(c2);
    let c2 = client(eng2, &url);
    c1.write(7, 7);
    c1.sync();
    let out = c2.s.pull_once(HOST);
    println!("[R] 開き直した c2 の最初の pull: history_truncated = {}, truncated_authors = {:?}, applied = {}、 行 7 = {:?}", out.history_truncated, out.truncated_authors, out.applied, c2.v(7));
    let again = c2.s.pull_once(HOST);
    println!("[R] 2 回目の pull: history_truncated = {}, applied = {}、 行 7 = {:?}", again.history_truncated, again.applied, c2.v(7));
    let got = c2.v(7);
    drop((c1, c2, host));
    assert_eq!(got, Some(7), "全部受けていた client が、 開き直しただけで受け取れなくなった");
}
