//! #830 (3): 同じ (書き手, 版数) の record の組の間に、 自分の書き込みがたくさん挟まった形。
//!
//! state を載せる側 (ホスト / 中継) は、 state の record を自分の WAL に 1 つずつ載せる (`Engine::relay_record`)。 その間、 自分の
//! アプリの書き込みは止まらない (gate / `state_relay` が止めるのは配る側だけ) ので、 同じ版数の record の間に自分の record が
//! 挟まって WAL に並ぶ。 #828 の bridge は直近に写した鍵を 64 個だけ覚えていたので、 65 個以上挟まった所で ring が満杯になると、
//! 組の頭 (もう ring に載った行) を見失い、 「全部配ってよい」 と決めた: 受け手は組の前半だけ受けて cursor をその版数に進め、
//! ring が空いた後に載る後半は `hlc > cursor` を通らなかった。
//!
//! ここの試験は、 state を載せる thread と自分の書き手の thread の重なり方を 1 本の thread で並べる (載せる側が 2 回の
//! `relay_record` の間で止まって、 その間に書き手が書いた、 と同じ WAL の並び)。 state の record は書き手の本物の state
//! (`Syncer::own_state`)、 配る / 受けるのは本物の `Syncer`。

use std::path::PathBuf;
use std::sync::Arc;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::{Hlc, PeerId};

/// D に何も届かない周がこれだけ続いたら、 もう届かないと見る。
const IDLE_ROUNDS: u32 = 15;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue830-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `v` (Number) を持つ表 `notes`。 `seed` の名前の行は WAL を通さずに書く (版数の無い cell = state では
/// 全部 `as_of` の版数で出る = 同じ版数の Tie が行の数 × 2 本)。
fn db(path: &str, peer: PeerId, seed: std::ops::Range<u32>) -> Arc<Engine> {
    db_sized(path, peer, seed, 16_384)
}

/// `notes` の枠が `rows` 行の DB (ring はその半分の行)。
fn db_sized(path: &str, peer: PeerId, seed: std::ops::Range<u32>, rows: u32) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 2 * rows).unwrap();
        e.define_table("notes", rows).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.set_peer_id(peer);
        for key in seed {
            let row = e.entity_in("notes").unwrap();
            e.tie(row, "notes.k", key);
            e.tie(row, "notes.v", key);
        }
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 64 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

/// ring の中身 (lsn の順): (書き手, 版数)。 Commit の行は除く。
fn ring(e: &Engine) -> Vec<(PeerId, Hlc)> {
    e.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| !matches!(r.op, enchudb_oplog::oplog::DecodedOp::Commit))
        .map(|r| (r.author_peer, r.hlc))
        .collect()
}

fn v(e: &Engine, k: u32) -> Option<u64> {
    e.pull_raw("notes.k", k).first().and_then(|row| e.get(*row, "notes.v"))
}

/// A → R (中継) → D。 R は A の state (同じ版数の Tie 600 本) を載せている途中で、 自分の書き込みが挟まる。 R の ring は、
/// 組の頭 `HEAD` 本と、 挟まった書き込みの途中まで入った所で満杯になる。 `between` = 頭の後ろに載る自分の record の数の範囲
/// (ring の空きをそこまで詰めてから始める)。
fn group_with_own_writes_between(tag: &str, between: std::ops::Range<u32>) {
    const SEED: std::ops::Range<u32> = 1_000..1_300;
    const A: PeerId = 1;
    const R: PeerId = 4;
    const D: PeerId = 5;
    /// 挟まる前に載せる組の record の数
    const HEAD: usize = 10;
    let dir = Dir::new(tag);
    let a = db(&dir.db(A), A, SEED);
    let r = db(&dir.db(R), R, 0..0);
    let d = db(&dir.db(D), D, 0..0);
    r.set_gossip_remote_apply(true);
    // A の本物の state
    let up: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let state = Syncer::new(a.clone(), up).own_state();
    let group: Vec<_> = state.records.iter().filter(|rec| rec.author_peer == A && rec.hlc == state.as_of).cloned().collect();
    assert_eq!(group.len(), 2 * SEED.len(), "前提: A の state は同じ版数 (as_of) の Tie が 600 本");
    let link: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let sr = Syncer::new(r.clone(), link.clone());
    let sd = Syncer::new(d.clone(), link);
    // D は受けるだけで、 何も publish しない (#831)
    // R の record で R の ring をほぼ埋める (D は受けていないので回収されない)
    let mine = r.entity_in("notes").unwrap();
    r.tie_to(mine, "notes.k", 10_000u32);
    for i in 0..7_800u32 {
        r.tie_to(mine, "notes.v", i);
    }
    r.oplog_sync().unwrap();
    // ring の空きを 頭 + `between` に入る所まで詰める (1 回で record 1 つ + Commit の 2 行)
    let free = loop {
        let free = r.table_eid_usage("_sync_ops").unwrap().free;
        if free < HEAD as u32 + between.end - 1 {
            break free;
        }
        r.tie_to(mine, "notes.v", 50_000 + free);
        r.oplog_sync().unwrap();
    };
    assert!(free >= HEAD as u32 + between.start, "前提: R の ring の空きは {free} 行");
    // 組の頭 → 自分の書き込み (ring の空きより多い) → 組の残り、 の順に WAL に並ぶ
    for rec in &group[..HEAD] {
        assert!(r.relay_record(rec));
    }
    for i in 0..free + 200 {
        r.tie_to(mine, "notes.v", 100_000 + i);
    }
    for rec in &group[HEAD..] {
        assert!(r.relay_record(rec));
    }
    r.oplog_sync().unwrap();
    let on = ring(&r);
    let head_at = on.iter().rposition(|key| *key == (A, state.as_of)).expect("前提: 組の頭が R の ring にある");
    assert_eq!(on.iter().filter(|key| **key == (A, state.as_of)).count(), HEAD, "前提: ring に載った組の record は頭の {HEAD} 本だけ");
    let after_head = on.len() - 1 - head_at;
    let free_now = r.table_eid_usage("_sync_ops").unwrap().free;
    assert!(
        between.contains(&(after_head as u32)) && free_now == 0,
        "前提: 組の頭の後ろに R の record が {after_head} 本 ({between:?} のはず) 載って ring が満杯 (空き {free_now} 行)"
    );
    let mut idle = 0;
    let end = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        r.oplog_sync().unwrap();
        sr.publish_since(Hlc::ZERO);
        let got = sd.pull_once(R).received;
        idle = if got == 0 { idle + 1 } else { 0 };
        let missing = SEED.filter(|k| v(&d, *k) != Some(*k as u64)).count();
        if missing == 0 || idle >= IDLE_ROUNDS {
            break;
        }
        assert!(std::time::Instant::now() < end, "60 秒で届き終わらない (D で食い違う行 {missing}、 R の ring {} 行)", ring(&r).len());
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let missing: Vec<u32> = SEED.filter(|k| v(&d, *k) != Some(*k as u64)).collect();
    assert!(
        missing.is_empty(),
        "D で v が食い違う行 {} (先頭 {:?}、 行そのものが無い {})。 組の頭の後ろに挟まった R の record {after_head} 本",
        missing.len(),
        &missing[..missing.len().min(5)],
        SEED.filter(|k| d.pull_raw("notes.k", *k).is_empty()).count()
    );
}

/// 挟まった自分の record が 64 本より多い (#828 の最初の形の bridge が覚えていた鍵の数を越える)。
#[test]
fn a_group_whose_head_is_behind_many_own_writes_is_not_published_in_part() {
    group_with_own_writes_between("many", 65..600);
}

/// 対照: 挟まった自分の record が 64 本より少ない (#828 の最初の形でも通る = 上の試験が落ちるのは挟まった数のせい)。
#[test]
fn a_group_whose_head_is_behind_a_few_own_writes_is_not_published_in_part() {
    group_with_own_writes_between("few", 20..60);
}

/// #830 (1): ホストでない中継が **差分で** 受けた同じ版数の組。 A → R1 (A の state を載せた = ring に同じ版数の Tie が並ぶ) →
/// R2 (R1 から差分で受ける中継) → D。 R2 の `pull_once` が組を 1 つずつ WAL に載せている間に、 別の thread が R2 の WAL を
/// 書き出して (`oplog_sync` = Commit + bridge) 配り、 D が受ける。 載せている途中の所を D が受けると、 D の `cursor[A]` が
/// その版数に進み、 後から載った残りは `hlc > cursor` を通らない。
///
/// 割り込む所は試験が作らない (thread の重なりに任せる): R2 の WAL を書き出し続ける thread (consumer の周期 / アプリの
/// `oplog_sync` の役) と、 R2 の ring を配って D が受け続ける thread を、 R2 の `pull_once` と並べて回す。 重なったか (= 組が
/// 途中まで R2 の ring に写ったか) は前提として確かめ、 重なるまで作り直す。 組は 40,000 本 (載せるのに数十 ms) なので、 ほぼ
/// 1 回目で重なる。 直し (`pull_once` が `state_relay` を持つ) を外すと 12 回中 12 回落ちた。
#[test]
fn a_relay_does_not_publish_a_group_it_is_still_relaying_from_a_diff() {
    use std::sync::atomic::{AtomicBool, Ordering};
    const SEED: std::ops::Range<u32> = 1_000..21_000;
    /// `notes` の枠 (ring はその半分 = 組の 40,000 本が入る)
    const ROWS: u32 = 131_072;
    const A: PeerId = 1;
    const R1: PeerId = 3;
    const R2: PeerId = 4;
    const D: PeerId = 5;
    let mut overlapped = 0;
    let mut tried = 0;
    while overlapped < 1 && tried < 10 {
        tried += 1;
        let dir = Dir::new(&format!("diff{tried}"));
        let a = db_sized(&dir.db(A), A, SEED, ROWS);
        let r1 = db_sized(&dir.db(R1), R1, 0..0, ROWS);
        let r2 = db_sized(&dir.db(R2), R2, 0..0, ROWS);
        let d = db_sized(&dir.db(D), D, 0..0, ROWS);
        r1.set_gossip_remote_apply(true);
        r2.set_gossip_remote_apply(true);
        // A — R1
        let up: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
        let sa = Syncer::new(a.clone(), up.clone());
        sa.serve_state();
        let sr1_up = Syncer::new(r1.clone(), up);
        // R1 — R2 — D (R2 は 1 つの `Syncer` で受けて配る = `state_relay` が効く形)
        let mesh: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
        let sr1 = Syncer::new(r1.clone(), mesh.clone());
        let sr2 = Syncer::new(r2.clone(), mesh.clone());
        let sd = Syncer::new(d.clone(), mesh);
        // R2 と D は、 自分の行を書かない (#831)
        let as_of = sr1_up.bootstrap_pull_via(A, A).expect("A の state").as_of;
        r1.oplog_sync().unwrap();
        let group = |e: &Engine| ring(e).iter().filter(|key| **key == (A, as_of)).count();
        let whole = group(&r1);
        assert_eq!(whole, 2 * SEED.len(), "前提: R1 の ring に、 A の同じ版数の Tie が全部載っている");
        assert!(sr1.publish_since(Hlc::ZERO) >= whole, "前提: R1 が組を配った");
        let stop = AtomicBool::new(false);
        let saw_part = std::thread::scope(|scope| {
            // 書き出す thread: R2 の WAL を閉じて ring に写す
            let racer = scope.spawn(|| {
                // 組の行が R2 の ring に初めて見えた時の数 (D が受けて ack すると行は回収されるので、 最初の 1 回だけ見る)
                let mut first_seen = 0;
                while !stop.load(Ordering::Acquire) {
                    r2.oplog_sync().unwrap();
                    if first_seen == 0 {
                        first_seen = group(&r2);
                    }
                }
                first_seen > 0 && first_seen < whole
            });
            // 配る thread: R2 の ring を配り、 D が受ける
            scope.spawn(|| {
                while !stop.load(Ordering::Acquire) {
                    sr2.publish_since(Hlc::ZERO);
                    sd.pull_once(R2);
                }
            });
            // R2 が R1 から差分で受ける (当てて、 1 つずつ載せる)
            let got = sr2.pull_once(R1);
            assert!(!got.history_truncated && got.applied >= whole, "前提: R2 は組を差分で受けて全部当てた (当たった {})", got.applied);
            stop.store(true, Ordering::Release);
            racer.join().unwrap()
        });
        if !saw_part {
            continue;
        }
        overlapped += 1;
        let mut idle = 0;
        loop {
            r2.oplog_sync().unwrap();
            sr2.publish_since(Hlc::ZERO);
            let got = sd.pull_once(R2).received;
            idle = if got == 0 { idle + 1 } else { 0 };
            let missing = SEED.filter(|k| v(&d, *k) != Some(*k as u64)).count();
            if missing == 0 || idle >= IDLE_ROUNDS {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let missing: Vec<u32> = SEED.filter(|k| v(&d, *k) != Some(*k as u64)).collect();
        assert!(
            missing.is_empty(),
            "{tried} 回目: D で v が食い違う行 {} (先頭 {:?}、 行そのものが無い {})",
            missing.len(),
            &missing[..missing.len().min(5)],
            SEED.filter(|k| d.pull_raw("notes.k", *k).is_empty()).count()
        );
    }
    assert!(overlapped > 0, "前提: {tried} 回のどれかで、 R2 が組を載せている途中の ring を配る thread が見た");
}
