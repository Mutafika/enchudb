//! #795: ホストは、 登録済みの語の Vocab を、 最後に state を配った後にもう載せていれば載せ直さない。
//!
//! #780 から、 ホストは登録済みの Vocab (同じ書き手・番号・語を前に当てた) も毎回中継に載せていた: 語の種類の少ない Tag の列
//! では、 書き込み 1 回につきホストの ring の行が 1 つ増え、 下流の client が受ける量も同じだけ増えた (10 万行で ring が 1.25 倍)。
//! 今は語ごとに、 最初の 1 回 (当たった Vocab) と、 state を配るごとに高々 1 回 (登録済みの Vocab) だけ載せる。 #780 の形
//! (state で取り直した client が、 その後にもう一度使われた語を受ける) は、 取り直しで世代が進むので今までどおり届く
//! (`issue780_reused_word_after_bootstrap.rs` の 3 本)。
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
        let dir = std::env::temp_dir().join(format!("enchudb-issue795-{tag}-{}", std::process::id()));
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


/// ホストの ring にある、 語 `word` の Vocab の record の数。
fn vocabs_in_ring(host_eng: &Engine, word: &[u8]) -> usize {
    host_eng
        .pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| matches!(&r.op, enchudb_oplog::oplog::DecodedOp::Vocab { bytes, .. } if bytes == word))
        .count()
}

/// 新しい行 `k` を作って t に `word` を書く。
fn add_row(c: &Client, k: u32, word: &str) {
    let e = c.eng.entity_in("notes").unwrap();
    c.eng.tie_to(e, "notes.k", k);
    c.eng.tie_text_to(e, "notes.t", word);
    c.eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(3));
}

#[test]
fn a_known_word_is_relayed_once_more_and_then_only_after_a_state_was_served() {
    let dir = Dir::new("known");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    // c1 が同じ語を 6 行に書く。 1 行ずつホストへ送る (batch ごとに Vocab + Tie)
    for k in 1..=6u32 {
        add_row(&c1, k, "w");
        c1.push();
    }
    // ホスト: 最初の 1 回 (当たった) + 登録済みの 1 回。 残りの 4 回は載せない (前は 6 回とも載せた)
    assert_eq!(vocabs_in_ring(&host_eng, b"w"), 2, "ホストの ring の、 語 w の Vocab の数");
    // 差分で追う c2 には全部届く
    for _ in 0..2 {
        c2.sync();
    }
    for k in 1..=6u32 {
        assert_eq!(c2.t(k).as_deref(), Some("w"), "c2 の行 {k}");
        assert_eq!(host_t(&host_eng, k).as_deref(), Some("w"), "ホストの行 {k}");
    }

    // 初めて来た c3 が、 ホストの state で c1 を取り直す (state を配った = 世代が進む)
    let c3 = client(db(&dir.db(3), 3), &url);
    assert!(c3.s.bootstrap_pull_via(HOST, 1).is_some(), "前提: ホストが c1 の state を配る");
    // その後の同じ語: Vocab をもう 1 回だけ載せ、 その後はまた載せない
    for k in 7..=10u32 {
        add_row(&c1, k, "w");
        c1.push();
    }
    assert_eq!(vocabs_in_ring(&host_eng, b"w"), 3, "state を配った後は、 登録済みの語の Vocab を 1 回だけ載せ直す");
    for _ in 0..2 {
        c2.sync();
        assert_eq!(c3.sync(), Vec::<PeerId>::new(), "取り直した後に抜けを言われた");
    }
    for k in 1..=10u32 {
        assert_eq!(c2.t(k).as_deref(), Some("w"), "c2 の行 {k}");
        assert_eq!(c3.t(k).as_deref(), Some("w"), "c3 の行 {k}");
    }
    drop(host);
}

/// 1 つの batch に同じ語が何度も入っていても同じ (ホストは batch の中の 2 つ目の登録済みの Vocab から載せない)。
#[test]
fn a_batch_that_repeats_a_word_relays_its_vocab_twice() {
    let dir = Dir::new("batch");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    for k in 1..=50u32 {
        add_row(&c1, k, if k % 2 == 0 { "even" } else { "odd" });
    }
    c1.push();
    assert_eq!(vocabs_in_ring(&host_eng, b"even"), 2);
    assert_eq!(vocabs_in_ring(&host_eng, b"odd"), 2);
    for _ in 0..2 {
        c2.sync();
    }
    for k in 1..=50u32 {
        assert_eq!(c2.t(k).as_deref(), Some(if k % 2 == 0 { "even" } else { "odd" }), "c2 の行 {k}");
    }
    drop(host);
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

/// #780 の形を、 「もう載せ直さない」 と覚えた語で: c1 が語 x を何度も使い (ホストは 2 回載せて、 以後は載せない)、 全部を別の語に
/// 書き換えて x はどの cell にも無くなる。 後から来た c3 がホストの state で取り直す (state に x は無い)。 その後に c1 が x を
/// もう一度使う。 state を配った時に世代が進んでいるので、 ホストは x の Vocab を載せ、 c3 にも届く。
#[test]
fn a_word_the_host_stopped_relaying_reaches_a_client_that_bootstrapped_since() {
    let dir = Dir::new("stopped");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(2), 2), &url));
    for k in 1..=4u32 {
        set_t(&c1, k, "x");
        c1.push();
    }
    assert_eq!(vocabs_in_ring(&host_eng, b"x"), 2, "前提: ホストは語 x の Vocab を 2 回載せて、 以後は載せていない");
    for _ in 0..2 {
        c1.sync();
        c2.sync();
    }
    for k in 1..=4u32 {
        assert_eq!(c2.t(k).as_deref(), Some("x"), "前提: c2 の行 {k}");
        set_t(&c1, k, "y");
    }
    // 水位を押す行を足して、 全員が全部受ける。 ホストが回収する
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
    assert_eq!(host_t(&host_eng, 4).as_deref(), Some("y"), "前提: ホストに書き換えが当たった");
    assert!(host_eng.reclaim_sync_ops() > 0, "前提: ホストが回収した");
    assert_eq!(vocabs_in_ring(&host_eng, b"x"), 0, "前提: 語 x の Vocab はホストの ring から消えた");
    // 初めて来た c3: 抜けを知って、 ホストの state で c1 を取り直す
    let c3 = client(db(&dir.db(3), 3), &url);
    let booted = c3.pull();
    assert!(booted.contains(&1), "前提: c3 は書き手 1 を取り直した ({booted:?})");
    for _ in 0..2 {
        c3.sync();
    }
    assert_eq!(c3.t(4).as_deref(), Some("y"), "前提: c3 は取り直しで今の語を受けた");
    // c1 が x をもう一度使う
    set_t(&c1, 5, "x");
    for _ in 0..3 {
        c1.sync();
        c2.sync();
        assert_eq!(c3.sync(), Vec::<PeerId>::new(), "取り直した後にまた抜けを言われた");
    }
    let want = Some("x".to_string());
    assert_eq!(
        [host_t(&host_eng, 5), c1.t(5), c2.t(5), c3.t(5)],
        [want.clone(), want.clone(), want.clone(), want],
        "行 5 の t [host, c1, c2, c3]: 取り直した c3 だけ、 Vocab を受けずに Tie を受けて翻訳できない"
    );
    assert_eq!(vocabs_in_ring(&host_eng, b"x"), 1, "state を配った後の語 x は、 Vocab をもう一度載せる");
    drop(host);
}
