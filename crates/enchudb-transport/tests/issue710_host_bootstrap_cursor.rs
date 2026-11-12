//! #710: ホストからの取り直し (bootstrap) の cursor が、 書き手のまだ出していない record を越えない。
//!
//! ホストが配る書き手 a の state の `as_of` は、 受け手の `cursor[a]` になる (以後 a の record は as_of より後だけ受ける)。 前は
//! 「state に出した record の版数の最大」 で、 a の行を別の client が書き換えていると、 その cell の版数 (別の client の時計) が
//! as_of になった。 a がそれより前に書いてまだ出していない record は、 取り直した client に二度と届かなかった。
//! 今は、 ホストが a 自身の record を受けた位置 (ホストの cursor)。
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue710-{tag}-{}", std::process::id()));
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

/// c1 の行を c2 が書き換えた後にホストから取り直した c3 に、 c1 がそれより前に書いてまだ出していなかった行が届く。
#[test]
fn a_bootstrap_cursor_does_not_pass_the_authors_unpublished_older_write() {
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
    let host_received = host.syncer().cursors_of(1).into_iter().find(|(a, _)| *a == 1).map(|(_, h)| h).expect("ホストの c1 の cursor");
    // c1 はオフラインで行 2 を書く (出さない)
    c1.write(2, 20);
    // その後で c2 が c1 の行 1 を書き換えて出す (版数は c2 の時計、 c1 の行 2 より後)
    let e = c2.row(1).unwrap();
    c2.eng.tie_to(e, "notes.v", 11u32);
    c2.eng.oplog_commit();
    c2.sync();
    c2.sync();
    let host_row = host_eng.pull_raw("notes.k", 1)[0];
    assert_eq!(host_eng.get(host_row, "notes.v"), Some(11), "前提: ホストに c2 の書き換えが当たった");
    let edited = host_eng.cell_hlc(host_row, host_eng.himo_id("notes.v").unwrap() as u16);
    assert!(edited.peer == 2 && edited > host_received, "前提: c1 の行の cell の版数が c2 の時計で、 ホストが c1 から受けた位置より後 ({edited:?} / {host_received:?})");
    // ホストが回収する (c1 の最初の record が ring から消え、 c1 の floor ができる)
    assert!(host_eng.reclaim_sync_ops() > 0, "前提: ホストが回収した");
    // 初めて来た c3: 抜けを知って取り直す
    let c3 = client(db(&dir.db(3), 3), &url);
    let booted = c3.pull();
    assert!(booted.contains(&1), "前提: c3 は書き手 1 を取り直した ({booted:?})");
    assert_eq!(c3.v(1), Some(11), "前提: 取り直しで行 1 が今の値で入った");
    let cursor = c3.s.cursors_of(HOST).into_iter().find(|(a, _)| *a == 1).map(|(_, h)| h);
    assert_eq!(cursor, Some(host_received), "取り直した c3 の、 書き手 1 の cursor は、 ホストが c1 から受けた位置");
    // c1 が戻ってきて、 オフラインの行 2 を出す
    for _ in 0..3 {
        c1.sync();
        c2.sync();
        c3.sync();
    }
    let got = [host_eng.pull_raw("notes.k", 2).len(), c1.eng.pull_raw("notes.k", 2).len(), c2.eng.pull_raw("notes.k", 2).len(), c3.eng.pull_raw("notes.k", 2).len()];
    assert_eq!(got, [1, 1, 1, 1], "c1 が後から出した行 2 [host, c1, c2, c3]: 取り直した c3 にだけ届かない");
    assert_eq!(c3.v(2), Some(20));
    // c3 はその後、 取り直しなしで受け続ける
    c1.write(3, 30);
    c1.sync();
    assert_eq!(c3.pull(), Vec::<PeerId>::new(), "取り直した後の pull でまた抜けを言われた");
    assert_eq!(c3.v(3), Some(30));
    drop((c1, c2, c3, host));
}

/// 対照: c2 が c1 の行を書き換えなければ (前から通る)、 c1 のオフラインの行は取り直した c3 に届く。
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
    assert_eq!(c3.v(2), Some(20));
    drop((c1, c2, c3, host));
}

/// cursor の置き場 (`Syncer::with_cursor_path`) を持つ client は、 開き直しても続きから受ける (README の例の形)。 置き場の無い
/// client は、 ホストが一度でも回収していると 「初めて来た client」 と同じ扱いで抜けを言われ、 取り直すまで受け取りが止まる。
#[test]
fn a_restarted_client_continues_only_with_a_cursor_file() {
    let dir = Dir::new("r");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1), &url);
    let with_file = |eng: Arc<Engine>| {
        let t = Arc::new(HttpHostTransport::connect(url.clone(), eng.peer_id()).unwrap());
        let s = Syncer::new(eng.clone(), t.clone()).with_cursor_path(format!("{}.cursors", dir.db(2)).into());
        s.serve_state();
        Client { eng, t, s }
    };
    let c2 = with_file(db(&dir.db(2), 2));
    let c3 = client(db(&dir.db(3), 3), &url);
    for k in 1..=6 {
        c1.write(k, k);
    }
    for _ in 0..3 {
        c1.sync();
        c2.sync();
        c3.sync();
    }
    assert!(host_eng.reclaim_sync_ops() > 0, "前提: ホストが回収した");
    assert_eq!([c2.v(6), c3.v(6)], [Some(6), Some(6)], "前提: c2 と c3 は全部受けている");
    // 2 台のプロセスを開き直す (engine はそのまま、 Syncer と transport を作り直す)
    let (eng2, eng3) = (c2.eng.clone(), c3.eng.clone());
    drop((c2, c3));
    let (c2, c3) = (with_file(eng2), client(eng3, &url));
    c1.write(7, 7);
    c1.sync();
    let out = c2.s.pull_once(HOST);
    assert!(!out.history_truncated, "cursor の置き場のある client が、 開き直しただけで抜けを言われた: {out:?}");
    assert_eq!(c2.v(7), Some(7));
    let out = c3.s.pull_once(HOST);
    assert!(out.history_truncated && c3.v(7).is_none(), "cursor の置き場の無い client は抜けを言われる (README に書いた形): {out:?}");
    assert!(!c3.pull().is_empty(), "取り直しを通る");
    assert_eq!(c3.v(7), Some(7), "取り直した後は受け取れる");
    drop((c1, c2, c3, host));
}
