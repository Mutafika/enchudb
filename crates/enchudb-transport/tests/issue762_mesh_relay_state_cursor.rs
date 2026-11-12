//! #762: ホストでない中継 (`set_gossip_remote_apply` だけ、 A → R → C) が取り直した state を載せる道で、 下の受け手 C の
//! 書き手ごとの cursor が、 その書き手のまだ届いていない record を越えない。 #753 (ホスト) と同じ 2 つの形:
//!
//! 1. 書き手 A が state を作っている間に書いた cell (版数が `as_of` より後) を R が当てて載せる → C の `cursor[A]` が as_of を
//!    越え、 同じ間に書かれて state に入らなかった (走査の済んだ cell の) record が、 後で差分で R に届いて載っても落ちる。
//! 2. R が state を載せている途中の publish → 載った所までを受けた C の cursor が、 まだ載っていない (版数の小さい) record を
//!    越える (state の record は HLC の順に並ばない)。
#![allow(dead_code)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, StateBatch, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};

const A: PeerId = 1;
const R: PeerId = 2;
const C: PeerId = 3;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue762-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `v` (Number) を持つ表 `notes`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 262_144).unwrap();
        e.define_table("notes", 32_768).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 64 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

fn row(e: &Engine, k: u32) -> Option<u64> {
    e.pull_raw("notes.k", k).first().copied()
}

fn v(e: &Engine, k: u32) -> Option<u64> {
    row(e, k).and_then(|r| e.get(r, "notes.v"))
}

fn write(e: &Engine, k: u32, val: u32) {
    let r = e.entity_in("notes").unwrap();
    e.tie_to(r, "notes.k", k);
    e.tie_to(r, "notes.v", val);
}

/// `v` が食い違う名前 (`k`)。
fn differing(a: &Engine, b: &Engine, keys: std::ops::RangeInclusive<u32>) -> Vec<u32> {
    keys.filter(|k| v(a, *k) != v(b, *k)).collect()
}

/// A (書き手) → R (中継、 ホストではない) → C。 欄の順 = 落とす順 (DB の directory は最後に消す)。
struct Mesh {
    sa: Syncer,
    sr: Syncer,
    sc: Syncer,
    a: Arc<Engine>,
    r: Arc<Engine>,
    c: Arc<Engine>,
    mem: Arc<InMemoryTransport>,
    /// A が配った state (前提を確かめる)。
    sent: Arc<Mutex<Vec<StateBatch>>>,
    /// A が state を作っている間だけ立つ印。
    building: Arc<AtomicBool>,
    _dir: Dir,
}

fn mesh(tag: &str) -> Mesh {
    let dir = Dir::new(tag);
    let mem = Arc::new(InMemoryTransport::new());
    for p in [A, R, C] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();
    let (a, r, c) = (db(&dir.db(A), A), db(&dir.db(R), R), db(&dir.db(C), C));
    r.set_gossip_remote_apply(true);
    let (sa, sr, sc) =
        (Syncer::new(a.clone(), transport.clone()), Syncer::new(r.clone(), transport.clone()), Syncer::new(c.clone(), transport));
    // A の state の配布元: 「engine の state をそのまま」 (`Syncer::serve_state` が自分の分として登録する物と同じ中身)
    let sent: Arc<Mutex<Vec<StateBatch>>> = Arc::new(Mutex::new(Vec::new()));
    let building = Arc::new(AtomicBool::new(false));
    {
        let (weak, sent, building) = (Arc::downgrade(&a), sent.clone(), building.clone());
        mem.register_state_provider(
            A,
            Arc::new(move || {
                let eng = weak.upgrade()?;
                building.store(true, Ordering::Release);
                let (records, as_of) = eng.state_records();
                building.store(false, Ordering::Release);
                let batch = StateBatch { records, as_of, complete: true };
                sent.lock().unwrap().push(batch.clone());
                Some(batch)
            }),
        );
    }
    Mesh { sa, sr, sc, a, r, c, mem, sent, building, _dir: dir }
}

impl Mesh {
    fn a_publish(&self) {
        self.a.oplog_commit();
        self.a.oplog_sync().unwrap();
        self.sa.publish_since(Hlc::ZERO);
    }
    /// R が A から受ける。 抜けを言われたら A の state を取り直す。 戻り値: 取り直したか。
    fn r_pull(&self) -> bool {
        let out = self.sr.pull_once(A);
        if !out.history_truncated {
            return false;
        }
        assert_eq!(out.truncated_authors, vec![A], "前提: R が言われた抜けは書き手 A");
        assert!(self.sr.bootstrap_pull_via(A, A).is_some(), "前提: R は A の state を取れる");
        true
    }
    fn r_publish(&self) {
        self.r.oplog_sync().unwrap();
        self.sr.publish_since(Hlc::ZERO);
    }
    fn c_pull(&self) -> usize {
        let out = self.sc.pull_once(R);
        assert!(!out.history_truncated, "前提: C は抜けを言われない");
        out.received
    }
    fn cycle(&self) {
        self.a_publish();
        assert!(!self.r_pull(), "前提: この回で R は取り直さない");
        self.r_publish();
        self.c_pull();
    }
    fn r_cursor(&self) -> Hlc {
        self.sr.cursors_of(A).into_iter().find(|(p, _)| *p == A).map_or(Hlc::ZERO, |(_, h)| h)
    }
    fn c_cursor(&self) -> Hlc {
        self.sc.cursors_of(R).into_iter().find(|(p, _)| *p == A).map_or(Hlc::ZERO, |(_, h)| h)
    }
    /// A の floor を、 R の cursor のすぐ上に置く (= A が配れずに落とした record がある、 と言う)。 次の `r_pull` で R は A の
    /// state を取り直す。
    fn ask_r_to_bootstrap(&self) {
        let cur = self.r_cursor();
        self.mem.set_history_floor_multi(A, &[(A, Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer })]);
    }
}

/// 形 1: A が state を作っている間も書き続ける。 state に入らなかった書き込みは後で差分で R に届き、 C もそれを受ける。
#[test]
fn writes_made_while_the_state_is_built_reach_a_peer_below_the_relay() {
    const N: u32 = 12_000;
    let m = mesh("w1");
    for k in 1..=N {
        write(&m.a, k, 0);
    }
    for _ in 0..2 {
        m.cycle();
    }
    assert_eq!(differing(&m.a, &m.c, 1..=N), Vec::<u32>::new(), "前提: C は A の行を全部受けた");
    assert!(m.sent.lock().unwrap().is_empty(), "前提: まだ state は配っていない");

    let v_himo = m.a.himo_id("notes.v").unwrap() as u16;
    let rows: Vec<u64> = (1..=N).map(|k| row(&m.a, k).unwrap()).collect();
    // 書き手: A が state を作っている間だけ、 まだ書いていない行の v を 1 回ずつ書き換える (行の順はばらす)
    let next = Arc::new(AtomicU32::new(0));
    let rewrite = {
        let (eng, rows) = (m.a.clone(), rows.clone());
        move |i: u32| {
            eng.tie_to(rows[((i as u64 * 7919) % N as u64) as usize], "notes.v", i + 1);
            eng.oplog_commit();
        }
    };
    let mut found = None;
    for attempt in 0..6 {
        m.ask_r_to_bootstrap();
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (rewrite, next, stop, building) = (rewrite.clone(), next.clone(), stop.clone(), m.building.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    if !building.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                        continue;
                    }
                    let i = next.load(Ordering::Acquire);
                    if i >= N {
                        break;
                    }
                    rewrite(i);
                    next.store(i + 1, Ordering::Release);
                }
            })
        };
        // R が抜けを言われ、 書き手の横で A が state を作り、 R が当てて載せる
        let booted = m.r_pull();
        stop.store(true, Ordering::Release);
        writer.join().unwrap();
        assert!(booted, "前提: R は A の state を取り直した");
        let batch = m.sent.lock().unwrap().last().cloned().unwrap();
        assert_eq!(m.sent.lock().unwrap().len(), attempt + 1, "前提: 1 回の取り直しで state を 1 回配った");
        assert_eq!(m.r_cursor(), batch.as_of, "前提: R の、 書き手 A の cursor は state の as_of");
        // state に入った (行, v の版数)
        let in_state: HashSet<(u64, Hlc)> = batch
            .records
            .iter()
            .filter_map(|r| match &r.op {
                DecodedOp::Tie { eid, himo_id, .. } if *himo_id == v_himo => Some((*eid, r.hlc)),
                _ => None,
            })
            .collect();
        let top = batch.records.iter().map(|r| r.hlc).max().unwrap();
        // state を作り始めた後に書いて、 state に入らなかった (走査の済んだ cell の) 書き込みのうち、 state の版数の最大より前の物
        let missed = rows
            .iter()
            .filter(|row| {
                let h = m.a.cell_hlc(**row, v_himo);
                h > batch.as_of && h < top && !in_state.contains(&(**row, h))
            })
            .count();
        if top > batch.as_of && missed > 0 {
            found = Some((batch, top, missed));
            break;
        }
    }
    let (batch, top, missed) = found.expect("前提: state を作っている間の書き込みが、 state に入った物と入らなかった物の両方ある");
    println!("[w1] as_of {:?} / state の版数の最大 {top:?} / その間で state に入らなかった書き込み {missed} 件", batch.as_of);

    // C が、 R の載せた state を受ける (A の差分はまだ R に無い)
    m.r_publish();
    m.c_pull();
    let after_state = m.c_cursor();
    // A が残りを出し、 R が中継し、 C が受ける
    for _ in 0..3 {
        m.cycle();
    }
    assert_eq!(differing(&m.a, &m.r, 1..=N), Vec::<u32>::new(), "前提: R は A と同じ");
    let bad = differing(&m.a, &m.c, 1..=N);
    assert!(bad.is_empty(), "A の書き込みが C に届かない行が {} 行 (例 k = {:?})", bad.len(), &bad[..bad.len().min(5)]);
    assert!(
        after_state <= batch.as_of,
        "R の載せた state を受けた C の、 書き手 A の cursor が as_of を越えた ({:?} > {:?}): A が as_of より後に書いてまだ出していない record を越える",
        after_state,
        batch.as_of
    );
}

/// 形 2: R が A の state を載せている間、 別の thread が R の WAL を ring に写し (consumer の周期と同じ事)、 もう 1 つの thread が R の
/// publish と C の pull を回し続ける。 ring に途中まで載った所で publish が重なっても、 残りが C に届く。
///
/// 前提 (途中まで載った ring が見えていた間に publish の呼び出しが重なった) は assert する。 重なり方は thread の進み次第なので、
/// 重なるまで行を足して 6 回までやり直す。 直しなしでは、 重なった publish が途中までの ring を配って落ちる (手元では
/// 3 回中 3 回)。
#[test]
fn a_publish_while_the_relay_relays_a_state_misses_nothing() {
    use std::time::Instant;
    const M: u32 = 4_000;
    let m = mesh("w2");
    write(&m.a, 1, 1);
    for _ in 0..2 {
        m.cycle();
    }
    assert_eq!(v(&m.c, 1), Some(1), "前提: C は A の最初の行を受けた");

    let v_himo = m.a.himo_id("notes.v").unwrap() as u16;
    let mut hi = 1u32;
    let mut split = false;
    for _attempt in 0..6 {
        // A が出さずに持つ行 (R には state でだけ届く)
        let lo = hi + 1;
        hi += M;
        for k in lo..=hi {
            write(&m.a, k, k);
        }
        m.a.oplog_commit();
        m.a.oplog_sync().unwrap();
        // floor を A の今の時計の上に置く: 持っている record は 「配れない」 と言う (R は差分を当てず、 state を取り直す)
        let top = m.a.cell_hlc(row(&m.a, hi).unwrap(), v_himo);
        assert!(top > m.r_cursor(), "前提: 新しい行は R がまだ受けていない");
        m.mem.set_history_floor_multi(A, &[(A, Hlc { wall: top.wall, logical: top.logical + 1, peer: top.peer })]);
        let before = m.r.current_sync_lsn();
        let stop = AtomicBool::new(false);
        let (seen, calls) = std::thread::scope(|s| {
            // R の WAL を ring に写し続け、 写すたびに ring がどこまで載ったか (lsn) を控える
            let bridge = s.spawn(|| {
                let mut seen: Vec<(Instant, u32)> = Vec::new();
                while !stop.load(Ordering::Acquire) {
                    // consumer の周期と同じ事 (Commit で閉じて ring に写す) を、 待たずに回す
                    m.r.oplog_commit();
                    m.r.transfer_oplog_to_sync_ops();
                    let lsn = m.r.current_sync_lsn();
                    if seen.last().is_none_or(|(_, l)| *l != lsn) {
                        seen.push((Instant::now(), lsn));
                    }
                }
                seen
            });
            // R の publish と C の pull を回し続け、 publish の呼び出しの始めと終わりを控える
            let pump = s.spawn(|| {
                let mut calls: Vec<(Instant, Instant, usize)> = Vec::new();
                loop {
                    let done = stop.load(Ordering::Acquire);
                    let from = Instant::now();
                    m.sr.publish_since(Hlc::ZERO);
                    let to = Instant::now();
                    calls.push((from, to, m.c_pull()));
                    if done {
                        return calls;
                    }
                }
            });
            let booted = m.r_pull(); // 抜けを言われる → A の state を取る → 当てて載せる
            stop.store(true, Ordering::Release);
            assert!(booted, "前提: R は A の state を取り直した");
            (bridge.join().unwrap(), pump.join().unwrap())
        });
        let after = m.r.current_sync_lsn();
        let r_has = (lo..=hi).filter(|k| m.r.pull_raw("notes.k", *k).len() == 1).count();
        assert_eq!(r_has as u32, M, "前提: R は A の出していなかった行を state で受けた");
        assert!(after - before >= 2 * M, "前提: R は state の record を ring に載せた ({before} → {after})");
        // ring が途中まで載っているのが見えた間 (最初に見えた時 〜 最後に見えた時) に重なった publish の呼び出し
        let partial: Vec<(Instant, u32)> = seen.iter().copied().filter(|(_, lsn)| *lsn > before && *lsn < after).collect();
        let overlapped = match (partial.first(), partial.last()) {
            (Some((first, _)), Some((last, _))) => calls.iter().filter(|(from, to, _)| from <= last && to >= first).count(),
            _ => 0,
        };
        println!(
            "[w2] ring {before} → {after}、 途中まで載った ring が見えた回 {} (lsn {:?})、 その間に重なった publish {overlapped} / 全 {} 回、 C が 1 回で受けた数 {:?}",
            partial.len(),
            partial.iter().map(|(_, l)| *l).collect::<Vec<_>>(),
            calls.len(),
            calls.iter().map(|c| c.2).filter(|n| *n > 0).collect::<Vec<_>>()
        );
        if overlapped > 0 {
            split = true;
            break;
        }
    }
    assert!(split, "前提: R の ring に state が途中まで載っていた間に、 R の publish が重なった");
    for _ in 0..3 {
        m.cycle();
    }
    assert_eq!(differing(&m.a, &m.r, 1..=hi), Vec::<u32>::new(), "前提: R は A と同じ");
    let bad = differing(&m.a, &m.c, 1..=hi);
    assert!(bad.is_empty(), "A の行が C と食い違う行が {} 行 (例 k = {:?})", bad.len(), &bad[..bad.len().min(5)]);
}

/// 対照: 書き手が止まっていて、 publish も重ならない取り直し。 state の record は全部 R に当たって載り、 C に届く
/// (as_of より後の record が無いので、 何も後回しにならない)。
#[test]
fn control_a_quiet_bootstrap_relays_the_whole_state() {
    const M: u32 = 200;
    let m = mesh("ctl");
    write(&m.a, 1, 1);
    for _ in 0..2 {
        m.cycle();
    }
    for k in 2..=M {
        write(&m.a, k, k);
    }
    m.a.oplog_commit();
    m.a.oplog_sync().unwrap();
    let top = m.a.cell_hlc(row(&m.a, M).unwrap(), m.a.himo_id("notes.v").unwrap() as u16);
    m.mem.set_history_floor_multi(A, &[(A, Hlc { wall: top.wall, logical: top.logical + 1, peer: top.peer })]);
    assert!(m.r_pull(), "前提: R は A の state を取り直した");
    let batch = m.sent.lock().unwrap().last().cloned().unwrap();
    assert!(batch.records.iter().all(|r| r.hlc <= batch.as_of), "前提: state に as_of より後の record は無い");
    assert_eq!(differing(&m.a, &m.r, 1..=M), Vec::<u32>::new(), "R は state だけで A と同じになる");
    // A は何も出さない: C に届くのは R が載せた state だけ
    m.r_publish();
    m.c_pull();
    assert_eq!(differing(&m.a, &m.c, 1..=M), Vec::<u32>::new(), "C は R の載せた state だけで A と同じになる");
}
