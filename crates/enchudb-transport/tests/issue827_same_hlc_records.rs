//! #827: transport (`InMemoryTransport` / `HttpRelay`) が、 版数 (HLC) だけで 「同じ record」 を決めて落としていた。
//!
//! 同じ版数の別の record は正しく並ぶ。 state の Tag の cell は、 語の record (Vocab) と値の record (Tie) が cell の版数で並び
//! (`Engine::state_records_for`)、 中継が state を当てて ring に載せると ring にも同じ版数の 2 行が載る。 書き手自身の state は
//! 版数の無い cell を全部 `as_of` で出すので、 同じ版数の Tie が何本も並ぶ。
//!
//! - `InMemoryTransport`: pull が版数で重複を落とし (`dedup_by_key(|r| r.hlc)`)、 **1 回の publish でも** 2 つ目 (Tie) が届かない。
//!   publish も、 もう log にある版数の record を足さない。
//! - `HttpRelay`: pull は落とさない。 publish が、 もう log にある版数の record を足さない = 同じ版数の 2 つが **別の publish に
//!   分かれた時** (中継の ring への写しが間で切れた後。 gossip の中継は次の publish で ring の全部を出し直す) に 2 つ目が入らない。
//!   1 回の publish に両方入っていれば届く (下の `http_relay_one_publish_delivers_the_tag` が対照)。
//!
//! 直した後も、 同じ record の出し直しで log は増えない (gossip の中継は毎回 ring の全部を publish する)。

use std::path::PathBuf;
use std::sync::Arc;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::http::{HttpRelay, HttpTransport};

const A: PeerId = 1;
const R: PeerId = 4;
const D: PeerId = 5;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue827-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `t` (Tag) を持つ表 `notes`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
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

fn t(e: &Engine, k: u32) -> Option<String> {
    let row = e.pull_raw("notes.k", k).first().copied()?;
    e.get_text_owned(row, "notes.t").map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// engine の ring (`_sync_ops`) の record を、 載っている順に。 読む前に WAL を ring に写す
fn ring(e: &Engine) -> Vec<WireRecord> {
    e.oplog_sync().unwrap();
    e.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(WireRecord::from)
        .collect()
}

fn vocab(h: Hlc, vid: u32, word: &[u8]) -> WireRecord {
    WireRecord::unsigned(h, A, DecodedOp::Vocab { vid, bytes: word.to_vec() })
}
fn tie(h: Hlc, eid: u64, himo_id: u16, value: u64) -> WireRecord {
    WireRecord::unsigned(h, A, DecodedOp::Tie { eid, himo_id, value })
}
fn ops(recs: &[WireRecord]) -> Vec<(Hlc, DecodedOp)> {
    recs.iter().map(|r| (r.hlc, r.op.clone())).collect()
}

/// A が行 1 の t に "x" を書き、 R (ホストでない中継) が A の state で取り直して ring に載せる。 返すのは R の ring と、
/// その中の 「t の Tie と同じ版数の Vocab」 の位置 (前提: 同じ版数の Vocab と Tie が、 この順で R の ring に両方ある)。
fn relay_takes_the_state(a: &Arc<Engine>, r: &Arc<Engine>) -> (Vec<WireRecord>, usize) {
    let mem = Arc::new(InMemoryTransport::new());
    let link: Arc<dyn Transport> = mem.clone();
    r.set_gossip_remote_apply(true);
    let sa = Syncer::new(a.clone(), link.clone());
    let sr = Syncer::new(r.clone(), link);
    sa.serve_state();

    let row = a.entity_in("notes").unwrap();
    a.tie_to(row, "notes.k", 1u32);
    a.tie_text_to(row, "notes.t", "x");
    a.oplog_commit();
    a.oplog_sync().unwrap();

    // 差分の流れ (書き手自身の ring) では、 版数は record ごとに違う = 「同じ版数 = 同じ record」 が成り立つのはここまで
    let own = ring(a);
    let mut hlcs: Vec<Hlc> = own.iter().map(|r| r.hlc).collect();
    hlcs.sort();
    hlcs.dedup();
    assert_eq!(hlcs.len(), own.len(), "前提: A の差分の record は版数が全部違う: {:?}", ops(&own));

    sr.bootstrap_pull_via(A, A).expect("A の state");
    assert_eq!(t(r, 1).as_deref(), Some("x"), "前提: R に state が当たった");

    let ring = ring(r);
    let t_himo = r.himo_id("notes.t").unwrap() as u16;
    let tie_at = ring
        .iter()
        .position(|w| matches!(&w.op, DecodedOp::Tie { himo_id, .. } if *himo_id == t_himo))
        .unwrap_or_else(|| panic!("前提: R の ring に t の Tie がある: {:?}", ops(&ring)));
    let h = ring[tie_at].hlc;
    let vocab_at = ring
        .iter()
        .position(|w| w.hlc == h && matches!(w.op, DecodedOp::Vocab { .. }))
        .unwrap_or_else(|| panic!("前提: R の ring に、 t の Tie と同じ版数の Vocab がある: {:?}", ops(&ring)));
    assert!(vocab_at < tie_at, "前提: Vocab が Tie の前: {:?}", ops(&ring));
    (ring, vocab_at)
}

// ───────────────────────── InMemoryTransport ─────────────────────────

#[test]
fn in_memory_keeps_distinct_records_of_one_version() {
    let h = Hlc { wall: 10, logical: 0, peer: A };
    let both = vec![vocab(h, 1, b"x"), tie(h, 1, 2, 1)];

    // 1 回の publish
    let mem = InMemoryTransport::new();
    mem.publish(A, both.clone());
    assert_eq!(ops(&mem.pull_as_multi(R, A, &[])), ops(&both), "pull_as_multi");
    assert_eq!(ops(&mem.pull_as(R, A, Hlc::ZERO)), ops(&both), "pull_as");
    assert_eq!(ops(&mem.pull(A, Hlc::ZERO)), ops(&both), "pull");

    // Vocab だけ先に publish された後に、 全部を出し直す
    let mem = InMemoryTransport::new();
    mem.publish(A, both[..1].to_vec());
    mem.publish(A, both.clone());
    assert_eq!(ops(&mem.pull_as_multi(R, A, &[])), ops(&both), "分かれた publish");

    // 宛先つきの log も同じ
    let mem = InMemoryTransport::new();
    mem.publish_to(A, R, both[..1].to_vec());
    mem.publish_to(A, R, both.clone());
    assert_eq!(ops(&mem.pull_as_multi(R, A, &[])), ops(&both), "publish_to");

    // 版数の無い cell の state の形: 同じ版数 (as_of) の Tie が何本も
    let mem = InMemoryTransport::new();
    let ties: Vec<WireRecord> = (1..=50u64).map(|e| tie(h, e, 2, 7)).collect();
    mem.publish(A, ties.clone());
    assert_eq!(ops(&mem.pull_as_multi(R, A, &[])), ops(&ties), "同じ版数の Tie 50 本");
}

#[test]
fn in_memory_republish_does_not_grow() {
    let h = Hlc { wall: 10, logical: 0, peer: A };
    let h2 = Hlc { wall: 11, logical: 0, peer: A };
    let recs = vec![vocab(h, 1, b"x"), tie(h, 1, 2, 1), tie(h2, 2, 2, 1)];
    let mem = InMemoryTransport::new();
    for _ in 0..3 {
        mem.publish(A, recs.clone());
        mem.publish_to(A, R, recs.clone());
    }
    assert_eq!(mem.len_of(A), 3, "同じ record の出し直しで log が増えない");
    // broadcast と宛先つきの両方にある record は 1 つで届く
    assert_eq!(ops(&mem.pull_as_multi(R, A, &[])), ops(&recs));
    assert_eq!(ops(&mem.pull_as(R, A, Hlc::ZERO)), ops(&recs));
    // 1 回の publish の中に同じ record が 2 つあっても 1 つ
    let mem = InMemoryTransport::new();
    mem.publish(A, [recs.clone(), recs.clone()].concat());
    assert_eq!(mem.len_of(A), 3);
}

/// A → R (中継) → D。 R が A の state で取り直して載せた Tag の cell が D に届く。
#[test]
fn in_memory_relayed_state_tag_reaches_downstream() {
    let dir = Dir::new("mem");
    let [a, r, d] = [A, R, D].map(|id| db(&dir.db(id), id));
    let (ring, _) = relay_takes_the_state(&a, &r);

    let down = Arc::new(InMemoryTransport::new());
    let link: Arc<dyn Transport> = down.clone();
    let sr = Syncer::new(r.clone(), link.clone());
    let sd = Syncer::new(d.clone(), link);
    sr.publish_since(Hlc::ZERO);
    assert_eq!(down.len_of(R), ring.len(), "R の ring の record が全部 transport に入る: {:?}", ops(&ring));
    let out = sd.pull_once(R);
    assert_eq!(t(&d, 1).as_deref(), Some("x"), "D の行 1 の t ({out:?})");
}

// ───────────────────────── HttpRelay ─────────────────────────

#[test]
fn http_relay_keeps_distinct_records_of_one_version() {
    let relay = HttpRelay::start("127.0.0.1:0").unwrap();
    let http = HttpTransport::new(format!("http://{}", relay.addr()));
    let h = Hlc { wall: 10, logical: 0, peer: A };
    let h2 = Hlc { wall: 11, logical: 0, peer: A };
    let recs = vec![vocab(h, 1, b"x"), tie(h, 1, 2, 1), tie(h, 2, 2, 1), tie(h2, 3, 2, 1)];

    // Vocab だけ先に publish された後に、 全部を出し直す (gossip の中継が、 写しの進んだ ring をもう一度全部 publish する形)
    http.publish(R, recs[..1].to_vec());
    assert_eq!(ops(&http.pull(R, Hlc::ZERO)), ops(&recs[..1]), "前提: Vocab だけ入っている");
    http.publish(R, recs.clone());
    assert_eq!(ops(&http.pull(R, Hlc::ZERO)), ops(&recs), "同じ版数の Tie も入る");

    // 同じ record の出し直しで log は増えない
    for _ in 0..3 {
        http.publish(R, recs.clone());
    }
    assert_eq!(relay.record_count(), recs.len(), "出し直しで増えない");
    assert_eq!(ops(&http.pull(R, Hlc::ZERO)), ops(&recs));
}

/// 対照: R の ring が 1 回の publish で全部出れば、 `HttpRelay` 経由でも D に届く (直す前から)。
#[test]
fn http_relay_one_publish_delivers_the_tag() {
    let dir = Dir::new("http1");
    let [a, r, d] = [A, R, D].map(|id| db(&dir.db(id), id));
    let (ring, _) = relay_takes_the_state(&a, &r);

    let relay = HttpRelay::start("127.0.0.1:0").unwrap();
    let url = format!("http://{}", relay.addr());
    let sr = Syncer::new(r.clone(), Arc::new(HttpTransport::new(url.clone())));
    let sd = Syncer::new(d.clone(), Arc::new(HttpTransport::new(url)));
    sr.publish_since(Hlc::ZERO);
    assert_eq!(relay.record_count(), ring.len(), "R の ring の record が全部 relay に入る");
    let out = sd.pull_once(R);
    assert_eq!(t(&d, 1).as_deref(), Some("x"), "D の行 1 の t ({out:?})");
}

/// A → R (中継) → `HttpRelay` → D。 R の ring の、 同じ版数の Vocab と Tie の間で publish が分かれる (ring への写しが Vocab まで
/// 進んだ所で 1 回 publish され、 次の publish が ring の全部を出し直す)。 分け目は試験が作る: R の本物の ring の record を、
/// Vocab までと全部の 2 回に分けて、 R として publish する。 D は 2 回目の後にだけ pull する。
#[test]
fn http_relay_split_publish_delivers_the_tag() {
    let dir = Dir::new("http2");
    let [a, r, d] = [A, R, D].map(|id| db(&dir.db(id), id));
    let (ring, vocab_at) = relay_takes_the_state(&a, &r);

    let relay = HttpRelay::start("127.0.0.1:0").unwrap();
    let url = format!("http://{}", relay.addr());
    let http = HttpTransport::new(url.clone());
    http.publish(R, ring[..=vocab_at].to_vec());
    assert_eq!(relay.record_count(), vocab_at + 1, "前提: Vocab まで relay に入っている");
    http.publish(R, ring.clone());
    assert_eq!(relay.record_count(), ring.len(), "出し直した ring の残り (同じ版数の Tie を含む) が relay に入る");

    let sd = Syncer::new(d.clone(), Arc::new(HttpTransport::new(url)));
    let out = sd.pull_once(R);
    assert_eq!(t(&d, 1).as_deref(), Some("x"), "D の行 1 の t ({out:?})");
}
