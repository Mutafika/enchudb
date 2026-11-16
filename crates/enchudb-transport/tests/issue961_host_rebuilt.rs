//! #961 (b): ホストを同じ URL の裏で作り直した (空の DB) / 古い控えから戻した時、 client が前のホストに ack 済みだった record を
//! 新しいホストに送り直す。
//!
//! client の transport (`HttpHostTransport`) は 「ホストが受けた位置」 (`acked`) を覚え、 それ以下の record を送らない。 前は上がる
//! だけで、 同じ process の client は作り直したホストに ack 済みの record を送らず、 新しい record だけを送った。 新しいホストの cursor は
//! その新しい record で進むので、 後で古い record が届いても `hlc > cursor` で落ちる = 前のホストに ack 済みの record は、 新しい
//! ホストにも、 そこから受ける他の client にも二度と届かない (client の ring が回収されていなければ floor も言わないので、 ホストは
//! state も求めない)。
//!
//! 直し: client は publish の要求に `acked` を付け、 ホストは自分の cursor がそこまで届いていなければ当てずに `BEHIND` と今の位置を
//! 返す。 client は覚えを置き換えて、 次の beat で足りない所から送り直す。
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{StateBatch, StateProvider, Transport, WireRecord};
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue961-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
    fn path(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_string()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 名前 `k` (Number) と `v` (Number) を持つ表 `notes` の DB を作って開く。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    open(path, peer)
}

fn open(path: &str, peer: PeerId) -> Arc<Engine> {
    let e = Engine::open_concurrent_with_oplog(path, 16 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

/// ホストを止めて、 engine が閉じ終わる (他の thread の参照が無くなる) まで待つ。
fn stop(host: HttpHost, eng: Arc<Engine>) {
    let weak = Arc::downgrade(&eng);
    drop(host);
    drop(eng);
    let t0 = std::time::Instant::now();
    while weak.upgrade().is_some() {
        assert!(t0.elapsed() < Duration::from_secs(10), "ホストの engine が閉じない");
        std::thread::sleep(Duration::from_millis(5));
    }
}

struct Client {
    eng: Arc<Engine>,
    t: Arc<HttpHostTransport>,
    s: Syncer,
}

fn client(eng: Arc<Engine>, url: &str) -> Client {
    client_via(eng, url, |t| t)
}

/// `Syncer` には `wrap` が包んだ transport を渡す (`Client::t` は包む前の物)。
fn client_via(eng: Arc<Engine>, url: &str, wrap: impl FnOnce(Arc<HttpHostTransport>) -> Arc<dyn Transport>) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), wrap(t.clone()));
    s.serve_state();
    Client { eng, t, s }
}

impl Client {
    fn write(&self, k: u32) {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", k * 10);
        self.eng.oplog_commit();
        std::thread::sleep(Duration::from_millis(3));
    }
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        while self.eng.transfer_oplog_to_sync_ops() > 0 {}
        self.s.publish_since(Hlc::ZERO);
    }
    /// 受ける。 抜けを言われたら、 言われた書き手を取り直して受け直す。
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
    /// transport が 「ホストが受けた」 と覚えている自分の位置。
    fn acked(&self) -> Hlc {
        let me = self.eng.peer_id();
        let f = self.t.publish_floor(me, HOST).unwrap_or_default();
        f.iter().find(|(a, _)| *a == me).map_or(Hlc::ZERO, |(_, h)| *h)
    }
}

fn has(eng: &Engine, k: u32) -> bool {
    eng.pull_raw("notes.k", k).len() == 1
}

/// ring にある自分の record の版数の最大 (直前に書いた行の record)。
fn top_own(eng: &Engine) -> Hlc {
    let me = eng.peer_id();
    eng.settled_sync_records(|a, _| a == me).iter().map(|r| r.hlc).max().unwrap_or(Hlc::ZERO)
}

/// 空の DB で作り直した (同じ addr)。 client a は前のホストに k=1 を ack 済み (ring は回収していない)。 a が k=2 を書いて出すと、
/// 新しいホストには k=1 も届き、 作り直す前から繋いでいた client b (まだ何も受けていない) にも k=1 と k=2 が届く。
#[test]
fn a_rebuilt_empty_host_gets_the_records_acked_by_the_previous_one() {
    let dir = Dir::new("empty");
    let h1 = db(&dir.path("h1"), HOST);
    let host = HttpHost::start("127.0.0.1:0", h1.clone(), Some(dir.path("h1.cursors").into())).unwrap();
    let addr = host.addr().to_string();
    let url = format!("http://{addr}");
    let a = client(db(&dir.path("a"), 1), &url);
    let b = client(db(&dir.path("b"), 2), &url);
    a.write(1);
    a.push();
    assert!(has(&h1, 1), "前提: 前のホストに k=1 が届いた");
    let top1 = top_own(&a.eng);
    assert!(top1 > Hlc::ZERO && a.acked() >= top1, "前提: a は前のホストに k=1 を ack 済み ({:?} / {top1:?})", a.acked());
    stop(host, h1);

    let h2 = db(&dir.path("h2"), HOST);
    let host = HttpHost::start(&addr, h2.clone(), Some(dir.path("h2.cursors").into())).unwrap();
    a.write(2);
    for _ in 0..3 {
        a.push();
    }
    b.pull();
    let got = [has(&h2, 1), has(&h2, 2), has(&b.eng, 1), has(&b.eng, 2)];
    drop(host);
    assert_eq!(got, [true; 4], "[新しいホストの k=1, k=2, b の k=1, k=2] (前のホストに ack 済みの k=1 が届かない)");
}

/// 古い控えから戻した (同じ addr)。 控えは k=1 まで受けた時の DB と cursor の置き場。 その後に前のホストが k=2 を受けて ack した。
/// 戻したホストに a が k=3 を出すと、 k=2 も届き、 b にも届く。
#[test]
fn a_host_restored_from_an_older_backup_gets_the_records_acked_after_it() {
    let dir = Dir::new("backup");
    let (h, cur) = (dir.path("h"), dir.path("h.cursors"));
    let h1 = db(&h, HOST);
    let host = HttpHost::start("127.0.0.1:0", h1.clone(), Some(cur.clone().into())).unwrap();
    let addr = host.addr().to_string();
    let url = format!("http://{addr}");
    let a = client(db(&dir.path("a"), 1), &url);
    let b = client(db(&dir.path("b"), 2), &url);
    a.write(1);
    a.push();
    assert!(has(&h1, 1), "前提: k=1 が届いた");
    // 控えを取る (DB は `snapshot_export`、 cursor の置き場は写す)
    h1.oplog_sync().unwrap();
    h1.snapshot_export(&dir.path("bak")).unwrap();
    for ext in ["", ".clients"] {
        if Path::new(&format!("{cur}{ext}")).exists() {
            std::fs::copy(format!("{cur}{ext}"), dir.path(&format!("bak.cursors{ext}"))).unwrap();
        }
    }
    assert!(Path::new(&dir.path("bak.cursors")).exists(), "前提: 控えに cursor の置き場がある");
    a.write(2);
    a.push();
    assert!(has(&h1, 2), "前提: 控えの後に k=2 が届いた");
    let top2 = top_own(&a.eng);
    assert!(a.acked() >= top2, "前提: a は k=2 を ack 済み");
    stop(host, h1);

    // 控えから戻す
    let h2 = open(&dir.path("bak"), HOST);
    assert!(has(&h2, 1) && !has(&h2, 2), "前提: 控えは k=1 だけ");
    let host = HttpHost::start(&addr, h2.clone(), Some(dir.path("bak.cursors").into())).unwrap();
    a.write(3);
    for _ in 0..3 {
        a.push();
    }
    b.pull();
    let got = [has(&h2, 1), has(&h2, 2), has(&h2, 3), has(&b.eng, 1), has(&b.eng, 2), has(&b.eng, 3)];
    drop(host);
    assert_eq!(got, [true; 6], "[戻したホストの k=1, 2, 3, b の k=1, 2, 3] (控えの後に ack 済みの k=2 が届かない)");
}

/// 前のホストの ack で a の ring を回収した後に作り直した: a が回収した k=1 は送り直せないので、 ホストは floor で state を求める
/// (#699 の道)。 `BEHIND` で当てずに返す回を挟んでも、 その道が止まらない。
#[test]
fn a_rebuilt_host_still_asks_for_the_state_of_a_reclaimed_client() {
    let dir = Dir::new("reclaimed");
    let h1 = db(&dir.path("h1"), HOST);
    let host = HttpHost::start("127.0.0.1:0", h1.clone(), Some(dir.path("h1.cursors").into())).unwrap();
    let addr = host.addr().to_string();
    let url = format!("http://{addr}");
    let a = client(db(&dir.path("a"), 1), &url);
    let b = client(db(&dir.path("b"), 2), &url);
    a.write(1);
    a.push();
    assert!(has(&h1, 1), "前提: 前のホストに k=1 が届いた");
    a.eng.ack_sync(HOST, a.eng.current_sync_lsn() + 1).unwrap();
    assert!(a.eng.reclaim_sync_ops() > 0, "前提: a の ring を回収した");
    stop(host, h1);

    let h2 = db(&dir.path("h2"), HOST);
    let host = HttpHost::start(&addr, h2.clone(), Some(dir.path("h2.cursors").into())).unwrap();
    a.write(2);
    for _ in 0..4 {
        a.push();
    }
    b.pull();
    let got = [has(&h2, 1), has(&h2, 2), has(&b.eng, 1), has(&b.eng, 2)];
    drop(host);
    assert_eq!(got, [true; 4], "[新しいホストの k=1, k=2, b の k=1, k=2]");
}

/// 割り込み点: `HttpHostTransport` を包み、 印を付けた thread の次の `publish_to` を、 呼ばれた所 (`Syncer` が `publish_floor` を
/// 聞いて record を集めた後、 送る前) で止める。 他の口はそのまま渡す。
struct Paused {
    inner: Arc<HttpHostTransport>,
    pause: Mutex<Option<Pause>>,
}

struct Pause {
    thread: ThreadId,
    arrived: Sender<()>,
    go: Receiver<()>,
}

impl Transport for Paused {
    fn pull(&self, from: PeerId, since: Hlc) -> Vec<WireRecord> {
        self.inner.pull(from, since)
    }
    fn publish(&self, peer: PeerId, records: Vec<WireRecord>) {
        self.inner.publish(peer, records)
    }
    fn publish_to(&self, from: PeerId, to: PeerId, records: Vec<WireRecord>) {
        let me = std::thread::current().id();
        let pause = {
            let mut p = self.pause.lock().unwrap();
            if p.as_ref().is_some_and(|p| p.thread == me) { p.take() } else { None }
        };
        if let Some(p) = pause {
            p.arrived.send(()).unwrap();
            p.go.recv().unwrap();
        }
        self.inner.publish_to(from, to, records)
    }
    fn publish_floor(&self, from: PeerId, to: PeerId) -> Option<Vec<(PeerId, Hlc)>> {
        self.inner.publish_floor(from, to)
    }
    fn uses_published_through(&self) -> bool {
        self.inner.uses_published_through()
    }
    fn note_published_through(&self, peer: PeerId, through: Hlc) {
        self.inner.note_published_through(peer, through)
    }
    fn forget_horizon(&self, from: PeerId) -> Option<Hlc> {
        self.inner.forget_horizon(from)
    }
    fn known_peers(&self) -> Vec<PeerId> {
        self.inner.known_peers()
    }
    fn pull_as_multi(&self, to: PeerId, from: PeerId, since: &[(PeerId, Hlc)]) -> Vec<WireRecord> {
        self.inner.pull_as_multi(to, from, since)
    }
    fn history_floor_multi(&self, peer: PeerId) -> Option<Vec<(PeerId, Hlc)>> {
        self.inner.history_floor_multi(peer)
    }
    fn set_history_floor(&self, peer: PeerId, floor: Hlc) {
        self.inner.set_history_floor(peer, floor)
    }
    fn set_history_floor_multi(&self, peer: PeerId, floors: &[(PeerId, Hlc)]) {
        self.inner.set_history_floor_multi(peer, floors)
    }
    fn record_pull_ack_multi(&self, author: PeerId, by: PeerId, cursors: &[(PeerId, Hlc)]) {
        self.inner.record_pull_ack_multi(author, by, cursors)
    }
    fn take_pull_acks_multi(&self, author: PeerId) -> Vec<(PeerId, Vec<(PeerId, Hlc)>)> {
        self.inner.take_pull_acks_multi(author)
    }
    fn register_state_provider(&self, peer: PeerId, provider: StateProvider) {
        self.inner.register_state_provider(peer, provider)
    }
    fn register_state_provider_for(&self, author: PeerId, by: PeerId, provider: StateProvider) {
        self.inner.register_state_provider_for(author, by, provider)
    }
    fn fetch_state(&self, author: PeerId) -> Option<StateBatch> {
        self.inner.fetch_state(author)
    }
}

/// 同じ transport の publish を並べて回した (review965 F1)。 作り直したホストに、 thread A の publish が前のホストの ack (k=1) より
/// 後の record (k=2) だけを集めた後、 送る前に止まる。 その間に thread B の publish が `BEHIND` を受けて覚えを下げる。 A が続きを
/// 送っても、 ホストの cursor が k=1 を越えない (前は A が下がった覚えを名乗って k=2 を当て、 その ack で覚えが k=2 に上がり、 k=1 は
/// 二度と集めなかった)。
#[test]
fn a_publish_collected_before_a_behind_does_not_jump_the_gap() {
    let dir = Dir::new("parallel");
    let h1 = db(&dir.path("h1"), HOST);
    let host = HttpHost::start("127.0.0.1:0", h1.clone(), Some(dir.path("h1.cursors").into())).unwrap();
    let addr = host.addr().to_string();
    let url = format!("http://{addr}");
    let paused = Arc::new(Mutex::new(None::<Arc<Paused>>));
    let a = {
        let paused = paused.clone();
        client_via(db(&dir.path("a"), 1), &url, move |inner| {
            let p = Arc::new(Paused { inner, pause: Mutex::new(None) });
            *paused.lock().unwrap() = Some(p.clone());
            p
        })
    };
    let paused = paused.lock().unwrap().clone().unwrap();
    let b = client(db(&dir.path("b"), 2), &url);
    a.write(1);
    a.push();
    assert!(has(&h1, 1), "前提: 前のホストに k=1 が届いた");
    let top1 = top_own(&a.eng);
    assert!(a.acked() >= top1, "前提: a は前のホストに k=1 を ack 済み");
    stop(host, h1);

    let h2 = db(&dir.path("h2"), HOST);
    let host = HttpHost::start(&addr, h2.clone(), Some(dir.path("h2.cursors").into())).unwrap();
    a.write(2);
    a.eng.oplog_sync().unwrap();
    while a.eng.transfer_oplog_to_sync_ops() > 0 {}
    let (arrived_tx, arrived_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let lowered = std::thread::scope(|s| {
        let (a, paused) = (&a, &paused);
        let thread_a = s.spawn(move || {
            *paused.pause.lock().unwrap() =
                Some(Pause { thread: std::thread::current().id(), arrived: arrived_tx, go: go_rx });
            a.s.publish_since(Hlc::ZERO);
        });
        arrived_rx.recv_timeout(Duration::from_secs(30)).expect("前提: A が割り込み点に着いた");
        // B: 同じ Syncer の publish。 ホストの cursor は空なので `BEHIND`
        a.s.publish_since(Hlc::ZERO);
        let lowered = a.acked();
        go_tx.send(()).unwrap();
        thread_a.join().unwrap();
        lowered
    });
    assert!(lowered < top1, "前提: B が `BEHIND` を受けて覚えを下げた ({lowered:?} / {top1:?})");
    for _ in 0..3 {
        a.push();
    }
    b.pull();
    let got = [has(&h2, 1), has(&h2, 2), has(&b.eng, 1), has(&b.eng, 2)];
    drop(host);
    assert_eq!(got, [true; 4], "[新しいホストの k=1, k=2, b の k=1, k=2] (`BEHIND` の前に集めた publish が k=1 を越えた)");
}

/// 書かない client も (review965 F6): 作り直したホストに、 a は何も書かずに publish だけを回す。 書き込みが無くても publish は
/// 配り終えの線を言うので `BEHIND` を受け、 前のホストに ack 済みの k=1 を送り直す (次に書くまで待たない)。
#[test]
fn a_client_that_does_not_write_also_resends_to_a_rebuilt_host() {
    let dir = Dir::new("quiet");
    let h1 = db(&dir.path("h1"), HOST);
    let host = HttpHost::start("127.0.0.1:0", h1.clone(), Some(dir.path("h1.cursors").into())).unwrap();
    let addr = host.addr().to_string();
    let url = format!("http://{addr}");
    let a = client(db(&dir.path("a"), 1), &url);
    let b = client(db(&dir.path("b"), 2), &url);
    a.write(1);
    a.push();
    assert!(has(&h1, 1), "前提: 前のホストに k=1 が届いた");
    let top1 = top_own(&a.eng);
    assert!(a.acked() >= top1, "前提: a は前のホストに k=1 を ack 済み");
    stop(host, h1);

    let h2 = db(&dir.path("h2"), HOST);
    let host = HttpHost::start(&addr, h2.clone(), Some(dir.path("h2.cursors").into())).unwrap();
    for _ in 0..3 {
        std::thread::sleep(Duration::from_millis(3));
        a.push();
    }
    assert_eq!(top_own(&a.eng), top1, "前提: a は何も書いていない");
    b.pull();
    let got = [has(&h2, 1), has(&b.eng, 1)];
    drop(host);
    assert_eq!(got, [true; 2], "[新しいホストの k=1, b の k=1] (書かない client が ack 済みの k=1 を送り直さない)");
}

/// ホストの ring にある、 書き手 `author` の record の数 (Vocab を除く)。
fn ring_rows(eng: &Engine, author: PeerId) -> usize {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| r.author_peer == author && !matches!(r.op, enchudb_oplog::oplog::DecodedOp::Vocab { .. }))
        .count()
}

/// cursor の置き場の無いホストを開き直す (review965 F4、 `HttpHost::start` の doc): 同じ process の client は前の ack を覚えているので
/// `BEHIND` を受け、 ring の自分の record を全部送り直し、 ホストはそれを全部もう一度中継に載せる (#698: 当たらなかった record も
/// 載せる)。 送り直すのは 1 回だけ (その ack で覚えが戻る): ホストの ring に増えた a の record は、 a の ring の record の数を越えない。
/// 今の数はちょうど 1 回分 (載せ直しを減らす直しを入れたら、 この数を直す)。
#[test]
fn a_reopened_host_without_a_cursor_file_relays_the_ring_once_more() {
    let dir = Dir::new("nocursor");
    let h = dir.path("h");
    let h1 = db(&h, HOST);
    let host = HttpHost::start("127.0.0.1:0", h1.clone(), None).unwrap();
    let addr = host.addr().to_string();
    let url = format!("http://{addr}");
    let a = client(db(&dir.path("a"), 1), &url);
    for k in 1..=3 {
        a.write(k);
    }
    a.push();
    assert!((1..=3).all(|k| has(&h1, k)), "前提: ホストに k=1..3 が届いた");
    let own = a.eng.settled_sync_records(|x, _| x == 1).iter().filter(|r| !matches!(r.op, enchudb_oplog::oplog::DecodedOp::Vocab { .. })).count();
    let before = ring_rows(&h1, 1);
    assert!(own > 0 && before == own, "前提: ホストの ring に a の record が 1 回ずつ載った ({before} / a の ring {own})");
    stop(host, h1);

    let h2 = open(&h, HOST);
    let host = HttpHost::start(&addr, h2.clone(), None).unwrap();
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(3));
        a.push();
    }
    let resent = ring_rows(&h2, 1) - before;
    drop(host);
    assert!(resent <= own, "開き直すたびに ring を 2 回以上送り直した ({resent} / a の ring {own})");
    assert_eq!(resent, own, "今の数: 開き直したホストは a の ring を 1 回だけ載せ直す");
}

/// 全部を通すとは言わない filter (`SubscriptionFilter::sends_all` が false)。 `Syncer` は `publish_floor` を聞かずに `since` から集める。
struct AskEach;
impl enchudb::sync::SubscriptionFilter for AskEach {
    fn should_send(&self, _target_peer: PeerId, _record: &WireRecord) -> bool {
        true
    }
}

/// `publish_floor` を聞かない publish は、 前に聞いた時の控えに縛られない (review965 F1 の直しの形)。 a は最初 (既定の filter で)
/// `publish_floor` を聞いて publish し、 その後 filter を替えて聞かなくなる。 作り直したホストが `BEHIND` を返して覚えを下げた後も、
/// 聞かない publish は送る (前の控えが残ると、 下げた回数と食い違ったまま毎回送らない)。
#[test]
fn a_publish_that_does_not_ask_for_the_floor_is_not_held_by_an_old_read() {
    let dir = Dir::new("filter");
    let h1 = db(&dir.path("h1"), HOST);
    let host = HttpHost::start("127.0.0.1:0", h1.clone(), Some(dir.path("h1.cursors").into())).unwrap();
    let addr = host.addr().to_string();
    let url = format!("http://{addr}");
    let a = client(db(&dir.path("a"), 1), &url);
    let b = client(db(&dir.path("b"), 2), &url);
    a.write(1);
    a.push();
    assert!(has(&h1, 1), "前提: 前のホストに k=1 が届いた");
    let top1 = top_own(&a.eng);
    assert!(a.acked() >= top1, "前提: a は前のホストに k=1 を ack 済み");
    a.s.set_subscription_filter(Arc::new(AskEach));
    stop(host, h1);

    let h2 = db(&dir.path("h2"), HOST);
    let host = HttpHost::start(&addr, h2.clone(), Some(dir.path("h2.cursors").into())).unwrap();
    a.write(2);
    for _ in 0..3 {
        a.push();
    }
    b.pull();
    let got = [has(&h2, 1), has(&h2, 2), has(&b.eng, 1), has(&b.eng, 2)];
    drop(host);
    assert_eq!(got, [true; 4], "[新しいホストの k=1, k=2, b の k=1, k=2] (前に `publish_floor` を聞いた控えが、 聞かない publish を止めた)");
}
