//! #780: 中継の state で取り直した受け手にも、 取り直しの後に届く Tie の語が届く。
//!
//! 書き手は Tag を書くたびに Vocab + Tie を出す。 ホストは、 もう写像を持っている Vocab (同じ書き手・番号・語) を中継に載せない
//! (最初の 1 回だけ載せる)。 その 1 回が ring から回収された後に来た client は、 ホストの state (今 cell にある語だけ) で
//! 取り直す。 state に無い語 (前に使って、 今はどの cell にも無い語) を書き手がもう一度使うと、 その client には Tie だけが届き、
//! 翻訳できずに捨てていた (`dropped_vocab`、 cursor は進む)。 今は、 ホストは登録済みの Vocab も載せ、 ホストでない中継は載せる
//! Tie が指す Vocab をその Tie の前に載せる。 state を中継する道も同じ。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue780-{tag}-{}", std::process::id()));
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


fn in_ring(host_eng: &Engine, word: &[u8]) -> bool {
    host_eng
        .pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .any(|r| matches!(&r.op, enchudb_oplog::oplog::DecodedOp::Vocab { bytes, .. } if bytes == word))
}

fn set_t(c: &Client, k: u32, word: &str) {
    let e = c.row(k).unwrap_or_else(|| {
        let e = c.eng.entity_in("notes").unwrap();
        c.eng.tie_to(e, "notes.k", k);
        e
    });
    c.eng.tie_text_to(e, "notes.t", word);
    c.eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(3));
}

/// ここまで: c1 が行 1 の t に "x" を書いて全員に届き、 "y" に書き換えて "x" はどの cell にも無くなる。 ホストが回収した後に c3 が
/// 初めて来て、 ホストの state で c1 を取り直す。
struct World {
    c1: Client,
    c2: Client,
    c3: Client,
    host: HttpHost,
    host_eng: Arc<Engine>,
    _dir: Dir,
}

fn late_client_world(tag: &str) -> World {
    let dir = Dir::new(tag);
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    set_t(&c1, 1, "x");
    for _ in 0..2 {
        c1.sync();
        c2.sync();
    }
    assert_eq!(c2.t(1).as_deref(), Some("x"), "前提: c2 が c1 の行を受けた");
    assert!(in_ring(&host_eng, b"x"), "前提: 語 x の Vocab はホストの ring に載った");
    set_t(&c1, 1, "y");
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
    assert_eq!(host_t(&host_eng, 1).as_deref(), Some("y"), "前提: ホストに書き換えが当たった");
    assert!(host_eng.reclaim_sync_ops() > 0, "前提: ホストが回収した");
    assert!(!in_ring(&host_eng, b"x"), "前提: 語 x の Vocab はホストの ring から消えた");
    // 初めて来た c3: 抜けを知って、 ホストの state で取り直す
    let c3 = client(db(&dir.db(3), 3), &url);
    let booted = c3.pull();
    assert!(booted.contains(&1), "前提: c3 は書き手 1 を取り直した ({booted:?})");
    for _ in 0..2 {
        c3.sync();
    }
    assert_eq!(c3.t(1).as_deref(), Some("y"), "前提: c3 は取り直しで今の語を受けた");
    World { c1, c2, c3, host, host_eng, _dir: dir }
}

impl World {
    /// 行 `k` の t を [host, c1, c2, c3] の順に。
    fn ts(&self, k: u32) -> [Option<String>; 4] {
        [host_t(&self.host_eng, k), self.c1.t(k), self.c2.t(k), self.c3.t(k)]
    }
}

/// 差分の道: c1 が前に使った語 "x" を、 c3 が取り直した後にもう一度使う。 c3 にも届く。
#[test]
fn a_word_reused_after_a_late_client_bootstrapped_reaches_that_client() {
    let w = late_client_world("reuse");
    set_t(&w.c1, 2, "x");
    for _ in 0..3 {
        w.c1.sync();
        w.c2.sync();
        assert_eq!(w.c3.sync(), Vec::<PeerId>::new(), "取り直した後にまた抜けを言われた");
    }
    assert!(in_ring(&w.host_eng, b"x"), "ホストは、 もう写像を持っている語の Vocab も中継に載せる");
    let want = Some("x".to_string());
    assert_eq!(w.ts(2), [want.clone(), want.clone(), want.clone(), want], "行 2 の t [host, c1, c2, c3]: 取り直した c3 だけ、 前に使われた語の Tie を翻訳できない");
    // 同じ語をもう一度 (今度は c3 も写像を持っている): 何度使っても届く
    set_t(&w.c1, 3, "x");
    for _ in 0..2 {
        w.c1.sync();
        w.c2.sync();
        w.c3.sync();
    }
    let want = Some("x".to_string());
    assert_eq!(w.ts(3), [want.clone(), want.clone(), want.clone(), want]);
}

/// state を中継する道: c1 が前に使った語 "x" をもう一度使い、 出す前にホストが c1 の state を取り直す。 state の Tie は
/// ホストで当たって中継に載り、 その語の Vocab はホストにはもう登録済み (当たらない)。 c3 にも届く。
#[test]
fn a_word_reused_in_a_relayed_state_reaches_a_late_client() {
    let w = late_client_world("state");
    set_t(&w.c1, 2, "x");
    let row2 = make_world_eid(&w.c1, 2);
    // ホストの c1 の cursor のすぐ上に c1 の floor を置く (= c1 が送れずに落とした record がある、 と言う)。 次の publish で
    // ホストは c1 の state を求める
    let cursor = |host: &HttpHost| host.syncer().cursors_of(1).into_iter().find(|(a, _)| *a == 1).map_or(Hlc::ZERO, |(_, h)| h);
    let cur = cursor(&w.host);
    assert!(w.c1.eng.cell_hlc(row2, w.c1.eng.himo_id("notes.t").unwrap() as u16) > cur, "前提: ホストは行 2 の書き込みをまだ受けていない");
    w.c1.t.set_history_floor_multi(1, &[(1, Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer })]);
    w.c1.push();
    assert!(cursor(&w.host) > cur, "前提: ホストは c1 の state を取り直した (cursor = as_of)");
    assert_eq!(host_t(&w.host_eng, 2).as_deref(), Some("x"), "前提: state の Tie がホストに当たった");
    for _ in 0..3 {
        w.c1.sync();
        w.c2.sync();
        assert_eq!(w.c3.sync(), Vec::<PeerId>::new(), "取り直した後にまた抜けを言われた");
    }
    let want = Some("x".to_string());
    assert_eq!(w.ts(2), [want.clone(), want.clone(), want.clone(), want], "行 2 の t [host, c1, c2, c3]: 取り直した c3 だけ、 state の Tie を翻訳できない");
}

fn make_world_eid(c: &Client, k: u32) -> u64 {
    c.row(k).unwrap()
}

// ─────────────────────────────────────────────────────────────
// ホストでない中継 (A → R → C、 後から D)
// ─────────────────────────────────────────────────────────────

const A: PeerId = 1;
const R: PeerId = 2;
const C: PeerId = 3;
const D: PeerId = 4;

fn t_of(e: &Engine, k: u32) -> Option<String> {
    let row = *e.pull_raw("notes.k", k).first()?;
    e.get_text_owned(row, "notes.t").map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn put_t(e: &Engine, k: u32, word: &str) {
    let row = e.pull_raw("notes.k", k).first().copied().unwrap_or_else(|| {
        let r = e.entity_in("notes").unwrap();
        e.tie_to(r, "notes.k", k);
        r
    });
    e.tie_text_to(row, "notes.t", word);
    e.oplog_commit();
    std::thread::sleep(Duration::from_millis(3));
}

/// 中継 R の下に後から来た D が、 R の state で A を取り直す。 その後 A が前に使った語をもう一度使う。 D にも届く。
#[test]
fn a_word_reused_after_a_late_peer_bootstrapped_below_a_plain_relay_reaches_it() {
    let dir = Dir::new("mesh");
    let mem = Arc::new(InMemoryTransport::new());
    for p in [A, R, C, D] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();
    let (a, r, c) = (db(&dir.db(A), A), db(&dir.db(R), R), db(&dir.db(C), C));
    r.set_gossip_remote_apply(true);
    let (sa, sr, sc) =
        (Syncer::new(a.clone(), transport.clone()), Syncer::new(r.clone(), transport.clone()), Syncer::new(c.clone(), transport.clone()));
    sa.serve_state();
    sr.serve_state();
    let cycle = || {
        a.oplog_commit();
        a.oplog_sync().unwrap();
        sa.publish_since(Hlc::ZERO);
        assert!(!sr.pull_once(A).history_truncated, "前提: R は抜けを言われない");
        r.oplog_sync().unwrap();
        sr.publish_since(Hlc::ZERO);
        assert!(!sc.pull_once(R).history_truncated, "前提: C は抜けを言われない");
    };
    put_t(&a, 1, "x");
    cycle();
    cycle();
    assert_eq!(t_of(&c, 1).as_deref(), Some("x"), "前提: C が A の行を受けた");
    assert!(in_ring(&r, b"x"), "前提: 語 x の Vocab は R の ring に載った");
    put_t(&a, 1, "y");
    for k in 8..=11 {
        // 水位を押す行
        put_t(&a, k, "pad");
        cycle();
        cycle();
    }
    assert_eq!(t_of(&c, 1).as_deref(), Some("y"), "前提: C に書き換えが届いた");
    assert!(r.reclaim_sync_ops() > 0, "前提: R が回収した");
    assert!(!in_ring(&r, b"x"), "前提: 語 x の Vocab は R の ring から消えた");
    r.oplog_sync().unwrap();
    sr.publish_since(Hlc::ZERO);
    // 後から来た D: 抜けを知って、 R の state で A を取り直す
    let d = db(&dir.db(D), D);
    let sd = Syncer::new(d.clone(), transport);
    let out = sd.pull_once(R);
    assert!(out.history_truncated, "前提: D は抜けを言われた");
    let authors = if out.truncated_authors.is_empty() { vec![A] } else { out.truncated_authors.clone() };
    assert!(authors.contains(&A), "前提: 抜けた書き手に A が居る ({authors:?})");
    for author in authors {
        assert!(sd.bootstrap_pull_via(R, author).is_some(), "前提: D は R から書き手 {author} の state を取れる");
    }
    assert!(!sd.pull_once(R).history_truncated, "前提: 取り直した D はもう抜けを言われない");
    assert_eq!(t_of(&d, 1).as_deref(), Some("y"), "前提: D は取り直しで今の語を受けた");
    // A が前に使った語をもう一度使う
    put_t(&a, 2, "x");
    for _ in 0..3 {
        cycle();
        assert!(!sd.pull_once(R).history_truncated, "取り直した後にまた抜けを言われた");
    }
    let got = [t_of(&a, 2), t_of(&r, 2), t_of(&c, 2), t_of(&d, 2)];
    let want = Some("x".to_string());
    assert_eq!(got, [want.clone(), want.clone(), want.clone(), want], "行 2 の t [A, R, C, D]: 取り直した D だけ、 前に使われた語の Tie を翻訳できない");
    // 中継は、 載せる Tie が指す Vocab だけを載せる: Tie が当たらない (同じ record が届き直した) 時は Vocab も載せない
    let vocab_x = |e: &Engine| {
        e.pending_sync_ops(0)
            .iter()
            .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
            .filter(|r| matches!(&r.op, enchudb_oplog::oplog::DecodedOp::Vocab { bytes, .. } if bytes == b"x"))
            .count()
    };
    assert_eq!(vocab_x(&r), 1, "R の ring の、 語 x の Vocab の数 (行 2 の Tie の前に 1 回)");
    drop((sa, sr, sc, sd));
}
