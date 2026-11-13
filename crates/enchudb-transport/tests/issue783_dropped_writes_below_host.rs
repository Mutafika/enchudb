//! #783: ホストを置く形で、 client の書き込みが WAL に載らずに落ちた (#57) 後、 state の取り直しで他の client にも届くこと。
//!
//! 落ちた書き込み X の後に書けた record Y が、 floor が上がる前に配られると、 他の client c3 の `cursor[c1]` が Y まで進む。 その後
//! ホストが c1 の state で X を取り直して元の版数で載せても、 c3 には cursor の手前で配られず、 抜けも知らされなかった。
//!
//! 直し (書き手の側): bridge は、 落ちた record より後の自分の record を、 floor を上げる前に `_sync_ops` に写さない (写す周には
//! 先に floor を上げる。 前に上げてから 1 秒の間は、 上げずに手前で止まる)。
//!
//! 見る事は 2 つ:
//! - 書き手の ring の約束 (時間に依らない): ring に 「落ちた書き込みより後の自分の record」 があるなら、 floor はその落ちた書き込みを
//!   覆っている
//! - 端から端: ホストと c3 に全部の行が届く

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue783-{tag}-{}", std::process::id()));
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

fn db(path: &str, peer: PeerId, wal: usize) -> Arc<Engine> {
    let mut eng = Engine::create_with_cell_version(path, 65_536).unwrap();
    eng.define_table("notes", 20_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, wal).unwrap();
    eng.set_peer_id(peer);
    eng
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

fn write_notes(eng: &Arc<Engine>, from: u32, n: u32) -> Vec<u64> {
    (from..from + n)
        .map(|i| {
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
            eng.tie_text_to(e, "notes.body", &format!("body {i}"));
            e
        })
        .collect()
}

fn settle(eng: &Arc<Engine>) {
    eng.oplog_commit();
    eng.flush_writes();
    let _ = eng.oplog_sync();
    eng.transfer_oplog_to_sync_ops();
}

fn has_note(eng: &Arc<Engine>, i: u32) -> bool {
    let Some(e) = eng.pull_raw("notes.note", i).first().copied() else { return false };
    eng.get_text_owned(e, "notes.body").is_some_and(|b| b == format!("body {i}").as_bytes())
}

fn missing(eng: &Arc<Engine>, n: u32) -> Vec<u32> {
    (0..n).filter(|i| !has_note(eng, *i)).collect()
}

/// 書き手の ring の約束: ring にある自分の record のどれかより前の版数で、 ring に無い (= WAL に載らずに落ちた) 自分の書き込みは、
/// floor が覆っている。 破れていたら (落ちた cell の版数, それより後の ring の record の版数, floor) を返す。
///
/// ring を先に読み、 floor を後で読む (bridge は floor を上げてから写す。 逆に読むと、 間に consumer の bridge が入った時に
/// 「floor は古く、 ring は新しい」 を見て、 破れていないのに破れたと言う)。
fn ring_promise_broken(eng: &Arc<Engine>, rows: &[u64]) -> Option<(Hlc, Hlc, Hlc)> {
    let me = eng.peer_id();
    let ring: Vec<Hlc> = eng
        .pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| r.author_peer == me)
        .map(|r| r.hlc)
        .collect();
    let in_ring: std::collections::HashSet<Hlc> = ring.iter().copied().collect();
    let top = ring.iter().copied().max()?;
    let floor =
        eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == me).map_or(Hlc::ZERO, |(_, h)| h);
    let cols = [eng.himo_id("notes.note").unwrap() as u16, eng.himo_id("notes.body").unwrap() as u16];
    rows.iter()
        .flat_map(|e| cols.iter().map(move |c| eng.cell_hlc(*e, *c)))
        .find(|h| *h > floor && !in_ring.contains(h) && *h < top)
        .map(|h| (h, top, floor))
}

/// 64 KiB の WAL を溢れさせる (Commit を打てなくして、 満杯でも畳めないようにする。 #57 の試験と同じ)。 その後、 WAL が空くのを
/// 待って (上限 60 ms = floor が静かになって上がる 100 ms より短い)、 落ちた後の record を書く。 戻り値は (溢れさせた行, 後の行)。
fn overflow_then_write(c: &Client, from: u32, n: u32) -> (Vec<u64>, Vec<u64>) {
    let dropped = c.eng.wal_dropped_records();
    c.eng.oplog().unwrap().fail_next_commits(u32::MAX);
    let burst = write_notes(&c.eng, from, n);
    c.eng.oplog().unwrap().fail_next_commits(0);
    assert!(c.eng.wal_dropped_records() > dropped, "前提: WAL に載らずに落ちた書き込みがある");
    settle(&c.eng);
    let wal = c.eng.oplog().unwrap().clone();
    let end = Instant::now() + Duration::from_millis(60);
    while !wal.is_empty() && Instant::now() < end {
        settle(&c.eng);
    }
    let late = write_notes(&c.eng, from + n, 3);
    settle(&c.eng);
    (burst, late)
}

/// c1 が配り、 c3 が受けるのを、 ホストと c3 に `n` 行が揃うまで回す (上限 20 秒)。 c3 が抜けを言われたら、 その書き手の state を
/// ホストから取り直す (アプリがする事)。 回すたびに c1 の ring の約束を見る。
fn converge(host_eng: &Arc<Engine>, c1: &Client, c3: &Client, rows: &[u64], n: u32) {
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        settle(&c1.eng);
        if let Some(b) = ring_promise_broken(&c1.eng, rows) {
            panic!("ring に、 floor が覆っていない落ちた書き込みより後の record がある (落ちた cell, ring の record, floor) = {b:?}");
        }
        c1.s.publish_since(Hlc::ZERO);
        let out = c3.s.pull_once(HOST);
        for a in out.truncated_authors {
            c3.s.bootstrap_pull_via(HOST, a);
        }
        if missing(host_eng, n).is_empty() && missing(&c3.eng, n).is_empty() {
            return;
        }
        if Instant::now() >= end {
            let (h, c) = (missing(host_eng, n), missing(&c3.eng, n));
            panic!(
                "WAL に載らずに落ちた書き込みが届かない: ホストに無い note {} 件 / c3 に無い note {} 件 (先頭 {:?})",
                h.len(),
                c.len(),
                &c[..c.len().min(5)]
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// 落ちた後に書けた record を、 落ちが止んですぐ (静かになって floor が上がる前に) 配る。
#[test]
fn dropped_writes_reach_clients_below_the_host() {
    let dir = Dir::new("once");
    let host_eng = db(&dir.db(HOST), HOST, 16 << 20);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1, 64 * 1024), &url);
    let c3 = client(db(&dir.db(3), 3, 16 << 20), &url);
    write_notes(&c1.eng, 0, 10);
    settle(&c1.eng);
    c1.s.publish_since(Hlc::ZERO);
    assert!(!c3.s.pull_once(HOST).history_truncated);
    assert!(missing(&c3.eng, 10).is_empty(), "前提: c3 は最初の note を受けた");

    let (burst, late) = overflow_then_write(&c1, 10, 5_000);
    let rows: Vec<u64> = burst.iter().chain(&late).copied().collect();
    if let Some(b) = ring_promise_broken(&c1.eng, &rows) {
        panic!("落ちた後の record を、 floor を上げる前に ring に写した (落ちた cell, ring の record, floor) = {b:?}");
    }
    // 落ちた後の record を配る。 c3 が受ける (前は、 ここで c3 の cursor が落ちた書き込みを越えた)
    c1.s.publish_since(Hlc::ZERO);
    let out = c3.s.pull_once(HOST);
    for a in out.truncated_authors {
        c3.s.bootstrap_pull_via(HOST, a);
    }
    converge(&host_eng, &c1, &c3, &rows, 5_013);
    drop((c1, c3, host));
}

/// 落ちが続けて 2 回 (前に floor を上げてから 1 秒の内)。 2 回目は、 floor が上がるまで bridge が手前で止まる。 止まっている間も、
/// 止まった後も、 ring の約束が保たれ、 全部届く。
#[test]
fn a_second_drop_right_after_a_floor_raise_holds_the_bridge() {
    let dir = Dir::new("twice");
    let host_eng = db(&dir.db(HOST), HOST, 16 << 20);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&dir.db(1), 1, 64 * 1024), &url);
    let c3 = client(db(&dir.db(3), 3, 16 << 20), &url);

    let (burst, late) = overflow_then_write(&c1, 0, 3_000);
    let mut rows: Vec<u64> = burst.iter().chain(&late).copied().collect();
    assert!(ring_promise_broken(&c1.eng, &rows).is_none(), "1 回目: 落ちた後の record を、 floor を上げる前に ring に写した");
    let bumps = c1.eng.wal_drop_floor_bumps();
    let holds = c1.eng.wal_drop_bridge_holds();
    let t = Instant::now();
    let (burst, late) = overflow_then_write(&c1, 3_003, 3_000);
    rows.extend(burst.iter().chain(&late));
    let within = t.elapsed() < Duration::from_millis(900);
    if let Some(b) = ring_promise_broken(&c1.eng, &rows) {
        panic!("2 回目: 落ちた後の record を、 floor を上げる前に ring に写した (落ちた cell, ring の record, floor) = {b:?}");
    }
    // 1 秒の内に 2 回目が終わったなら、 上げずに止まったか、 静かになって上げた (どちらでも約束は保たれる)。 止まった回数を見るのは
    // floor を上げていない時だけ
    println!(
        "[twice] 2 回目の後: floor を上げた回数 {bumps} → {} / bridge が止まった回数 {holds} → {} / 1 秒の内 {within}",
        c1.eng.wal_drop_floor_bumps(),
        c1.eng.wal_drop_bridge_holds()
    );
    if within && c1.eng.wal_drop_floor_bumps() == bumps {
        assert!(c1.eng.wal_drop_bridge_holds() > holds, "floor を上げていないのに、 bridge が止まってもいない");
    }
    converge(&host_eng, &c1, &c3, &rows, 6_006);
    drop((c1, c3, host));
}
