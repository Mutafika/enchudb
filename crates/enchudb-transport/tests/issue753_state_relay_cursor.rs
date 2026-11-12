//! #753: client が送った state (`POST /v2/state`) をホストが中継する道で、 受け手の書き手ごとの cursor が、 その書き手の
//! まだ届いていない record を越えない。
//!
//! 受け手の `cursor[a]` は 「ホストの流れのうち、 書き手 a の record はここまで受けた」。 これが成り立つのは、 ホストが a の
//! record を、 pull から見て HLC の順に載せる時だけ。 state の record はそう並ばない (HLC は cell の版数、 並びは列ごと)。
//! 2 つの形で cursor が越えた:
//!
//! 1. state を作っている間に書いた cell: state の record の版数が `as_of` (state を作り始めた時刻) より後になる。 受け手の
//!    cursor はそこまで進み、 同じ間に書いて state に入らなかった (もう走査の済んだ cell の) record は、 後で差分で届いても
//!    cursor の手前で落ちた。
//! 2. ホストが state を載せている途中の pull: 載った所までを受けた client の cursor が、 まだ載っていない (版数の小さい) record を
//!    越えた。
#![allow(dead_code)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{StateBatch, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue753-{tag}-{}", std::process::id()));
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
    fn write(&self, k: u32, v: u32) -> u64 {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        e
    }
    fn row(&self, k: u32) -> Option<u64> {
        self.eng.pull_raw("notes.k", k).first().copied()
    }
    fn v(&self, k: u32) -> Option<u64> {
        self.row(k).and_then(|e| self.eng.get(e, "notes.v"))
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
    fn cursor(&self, author: PeerId) -> Hlc {
        self.s.cursors_of(HOST).into_iter().find(|(a, _)| *a == author).map_or(Hlc::ZERO, |(_, h)| h)
    }
}

fn host_cursor(host: &HttpHost, c: PeerId) -> Hlc {
    host.syncer().cursors_of(c).into_iter().find(|(a, _)| *a == c).map_or(Hlc::ZERO, |(_, h)| h)
}

/// ホストの c の cursor のすぐ上に c の floor を置く (= c が送れずに落とした record がある、 と言う)。 次の publish でホストは
/// c の state を求める。
fn ask_for_state(host: &HttpHost, c: &Client) {
    let cur = host_cursor(host, c.eng.peer_id());
    let floor = Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer };
    c.t.set_history_floor_multi(c.eng.peer_id(), &[(c.eng.peer_id(), floor)]);
}

/// 行ごとに `v` が食い違う名前 (`k`) を数える。
fn differing(a: &Client, b_row: impl Fn(u32) -> Option<u64>, keys: std::ops::RangeInclusive<u32>) -> Vec<u32> {
    keys.filter(|k| a.v(*k) != b_row(*k)).collect()
}

/// state の配布元が 「engine の state をそのまま」 (`Syncer::serve_state` が自分の分として登録する物と同じ中身) の client。 送った
/// batch を控え (前提を確かめる)、 state を作っている間だけ印を立てる。
fn recording_client(eng: Arc<Engine>, url: &str) -> (Client, Arc<Mutex<Vec<StateBatch>>>, Arc<AtomicBool>) {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t.clone());
    let sent: Arc<Mutex<Vec<StateBatch>>> = Arc::new(Mutex::new(Vec::new()));
    let building = Arc::new(AtomicBool::new(false));
    {
        let (weak, sent, building) = (Arc::downgrade(&eng), sent.clone(), building.clone());
        t.register_state_provider(
            eng.peer_id(),
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
    (Client { eng, t, s }, sent, building)
}

/// 形 1: c1 が state を作っている間も書き続ける。 state に入らなかった書き込みは後で差分で届き、 c3 もそれを受ける。
#[test]
fn writes_made_while_the_state_is_built_reach_a_client_that_pulled_the_state() {
    const N: u32 = 12_000;
    let dir = Dir::new("w1");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, sent, building) = recording_client(db(&dir.db(1), 1), &url);
    let c3 = client(db(&dir.db(3), 3), &url);
    for k in 1..=N {
        c1.write(k, 0);
    }
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    assert_eq!(differing(&c1, |k| c3.v(k), 1..=N), Vec::<u32>::new(), "前提: c3 は c1 の行を全部受けた");
    assert!(sent.lock().unwrap().is_empty(), "前提: まだ state は送っていない");

    let v_himo = c1.eng.himo_id("notes.v").unwrap() as u16;
    let rows: Vec<u64> = (1..=N).map(|k| c1.row(k).unwrap()).collect();
    // 書き手: c1 が state を作っている間だけ、 まだ書いていない行の v を 1 回ずつ書き換える (行の順はばらす)
    let next = Arc::new(AtomicU32::new(0));
    let rewrite = {
        let (eng, rows) = (c1.eng.clone(), rows.clone());
        move |i: u32| {
            eng.tie_to(rows[((i as u64 * 7919) % N as u64) as usize], "notes.v", i + 1);
            eng.oplog_commit();
        }
    };
    let mut found = None;
    for attempt in 0..6 {
        ask_for_state(&host, &c1);
        // 出す record を 1 つ (publish が空だとホストに聞かない)
        rewrite(next.fetch_add(1, Ordering::AcqRel));
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (rewrite, next, stop, building) = (rewrite.clone(), next.clone(), stop.clone(), building.clone());
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
        // publish → ホストが state を求める → 書き手の横で state を作って送る
        c1.push();
        stop.store(true, Ordering::Release);
        writer.join().unwrap();
        let batch = sent.lock().unwrap().last().cloned().expect("前提: ホストが c1 の state を求め、 c1 が送った");
        assert_eq!(sent.lock().unwrap().len(), attempt + 1, "前提: 1 回の publish で state を 1 回送った");
        assert_eq!(host_cursor(&host, 1), batch.as_of, "前提: ホストの c1 の cursor は state の as_of");
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
        let missed: Vec<u64> = rows
            .iter()
            .copied()
            .filter(|row| {
                let h = c1.eng.cell_hlc(*row, v_himo);
                h > batch.as_of && h < top && !in_state.contains(&(*row, h))
            })
            .collect();
        if top > batch.as_of && !missed.is_empty() {
            found = Some((batch, top, missed.len()));
            break;
        }
    }
    let (batch, top, missed) = found.expect("前提: state を作っている間の書き込みが、 state に入った物と入らなかった物の両方ある");
    println!("[w1] as_of {:?} / state の版数の最大 {top:?} / その間で state に入らなかった書き込み {missed} 件", batch.as_of);

    // c3 が state の中継を受ける (c1 の差分はまだホストに無い)
    assert_eq!(c3.pull(), Vec::<PeerId>::new(), "前提: c3 は取り直しなしで受けた");
    let after_state = c3.cursor(1);
    // c1 が残りを出し、 c3 が受ける
    for _ in 0..3 {
        c1.sync();
        c3.sync();
    }
    assert_eq!(differing(&c1, |k| host_eng.pull_raw("notes.k", k).first().and_then(|e| host_eng.get(*e, "notes.v")), 1..=N), Vec::<u32>::new(), "前提: ホストは c1 と同じ");
    let bad = differing(&c1, |k| c3.v(k), 1..=N);
    assert!(bad.is_empty(), "c1 の書き込みが c3 に届かない行が {} 行 (例 k = {:?})", bad.len(), &bad[..bad.len().min(5)]);
    assert!(
        after_state <= batch.as_of,
        "state の中継を受けた c3 の、 書き手 1 の cursor が as_of を越えた ({:?} > {:?}): c1 が as_of より後に書いてまだ出していない record を越える",
        after_state,
        batch.as_of
    );
    drop((c1, c3, host));
}

/// 形 2: ホストが c1 の state を載せている間、 c3 が pull し続ける。 載った所までを受けても、 残りが届く。
#[test]
fn a_pull_while_the_host_relays_a_state_misses_nothing() {
    const M: u32 = 4_000;
    let dir = Dir::new("w2");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c3) = (client(db(&dir.db(1), 1), &url), client(db(&dir.db(3), 3), &url));
    c1.write(1, 1);
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    assert_eq!(c3.v(1), Some(1), "前提: c3 は c1 の最初の行を受けた");

    let mut hi = 1u32;
    let mut split = false;
    for _attempt in 0..6 {
        // c1 が出さずに持つ行 (ホストには state でだけ届く)
        let lo = hi + 1;
        hi += M;
        for k in lo..=hi {
            c1.write(k, k);
        }
        c1.eng.oplog_commit();
        c1.eng.oplog_sync().unwrap();
        // floor を c1 の今の時計の上に置く: 持っている record は 「配れない」 と言う (ホストは差分を当てず、 state を求める)
        let top = c1.eng.cell_hlc(c1.row(hi).unwrap(), c1.eng.himo_id("notes.v").unwrap() as u16);
        assert!(top > host_cursor(&host, 1), "前提: 新しい行はホストがまだ受けていない");
        c1.t.set_history_floor_multi(1, &[(1, Hlc { wall: top.wall, logical: top.logical + 1, peer: top.peer })]);
        hi += 1;
        c1.write(hi, hi);
        c1.eng.oplog_commit();
        // c3 は pull し続ける。 pull ごとに、 始めた時と終えた時にホストの ring がどこまで載っていたか (lsn) と、 受けた record の
        // 数を控える
        let before = host_eng.current_sync_lsn();
        let stop = Arc::new(AtomicBool::new(false));
        let pulls = std::thread::scope(|s| {
            let puller = s.spawn(|| {
                let mut pulls = Vec::new();
                loop {
                    let done = stop.load(Ordering::Acquire);
                    let at = host_eng.current_sync_lsn();
                    let out = c3.s.pull_once(HOST);
                    assert!(!out.history_truncated, "前提: c3 は抜けを言われない");
                    pulls.push((at, host_eng.current_sync_lsn(), out.received));
                    if done {
                        return pulls;
                    }
                }
            });
            c1.push(); // publish → ホストが state を求める → state を送る → ホストが当てて載せる
            stop.store(true, Ordering::Release);
            puller.join().unwrap()
        });
        let after = host_eng.current_sync_lsn();
        let host_has = (lo..=hi - 1).filter(|k| host_eng.pull_raw("notes.k", *k).len() == 1).count();
        assert_eq!(host_has as u32, M, "前提: ホストは c1 の出していなかった行を state で受けた");
        assert!(after - before >= 2 * M, "前提: ホストは state の record を ring に載せた ({before} → {after})");
        // state を載せている間に重なった pull: 途中まで載った所で始めたか、 pull の間に ring が伸びた
        let midway: Vec<(u32, u32, usize)> =
            pulls.iter().copied().filter(|(at, end, _)| *at < after && (*at > before || end > at)).collect();
        println!("[w2] ring {before} → {after}、 重なった pull (始めた時の lsn, 終えた時の lsn, 受けた数): {midway:?}");
        if !midway.is_empty() {
            split = true;
            break;
        }
    }
    assert!(split, "前提: ホストが state を ring に載せている間に、 c3 の pull が重なった");
    for _ in 0..3 {
        c1.sync();
        c3.sync();
    }
    let bad = differing(&c1, |k| c3.v(k), 1..=hi);
    assert!(bad.is_empty(), "c1 の行が c3 と食い違う行が {} 行 (例 k = {:?})", bad.len(), &bad[..bad.len().min(5)]);
    drop((c1, c3, host));
}

/// 対照 (issue に書いた形、 前から通る): c1 の行を c2 が書き換え、 c1 がそれを受けた後に state を送る。 state には c2 の時計の
/// 版数が c1 の record として入るが、 c1 の時計は受けた版数より後に進んでいて (`as_of` はその後)、 ホストは as_of より前の
/// c1 の record を受けない。 c3 の cursor は as_of を越えず、 c1 の後の書き込みは届く。
#[test]
fn control_a_state_that_carries_another_clients_clock() {
    let dir = Dir::new("ctl");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, sent, _) = recording_client(db(&dir.db(1), 1), &url);
    let (c2, c3) = (client(db(&dir.db(2), 2), &url), client(db(&dir.db(3), 3), &url));
    let all = |c: &[&Client]| {
        for _ in 0..2 {
            for c in c {
                c.eng.oplog_commit();
                c.sync();
            }
        }
    };
    c1.write(1, 10);
    all(&[&c1, &c2, &c3]);
    assert_eq!([c2.v(1), c3.v(1)], [Some(10), Some(10)], "前提: c1 の行が届いた");
    // c1 が出さずに行 2 を書き、 その後で c2 が c1 の行 1 を書き換える (c2 の時計、 c1 の行 2 より後)
    std::thread::sleep(std::time::Duration::from_millis(3));
    c1.write(2, 20);
    c1.eng.oplog_commit();
    std::thread::sleep(std::time::Duration::from_millis(3));
    c2.eng.tie_to(c2.row(1).unwrap(), "notes.v", 11u32);
    all(&[&c2]);
    assert!(c1.pull().is_empty());
    let v_himo = c1.eng.himo_id("notes.v").unwrap() as u16;
    let edited = c1.eng.cell_hlc(c1.row(1).unwrap(), v_himo);
    let unpublished = c1.eng.cell_hlc(c1.row(2).unwrap(), v_himo);
    assert!(edited.peer == 2 && edited > unpublished, "前提: c1 の行 1 の版数は c2 の時計で、 c1 の出していない行 2 より後 ({edited:?} / {unpublished:?})");
    // ホストが c1 の state を求める
    ask_for_state(&host, &c1);
    c1.push();
    let batch = sent.lock().unwrap().last().cloned().expect("前提: c1 が state を送った");
    assert!(batch.records.iter().any(|r| r.author_peer == 1 && r.hlc == edited), "前提: state に、 c2 の時計の版数が c1 の record として入った");
    assert!(batch.as_of > edited, "c1 の時計は、 受けた c2 の版数より後");
    assert!(c3.pull().is_empty());
    assert!(c3.cursor(1) <= batch.as_of, "c3 の、 書き手 1 の cursor が as_of を越えた");
    assert_eq!(c3.v(2), Some(20), "c1 の出していなかった行 2 が state で届いた");
    // c1 の後の書き込み
    c1.write(3, 30);
    all(&[&c1, &c2, &c3]);
    assert_eq!([c2.v(3), c3.v(3)], [Some(30), Some(30)]);
    assert_eq!([c1.v(1), c2.v(1), c3.v(1)], [Some(11); 3]);
    drop((c1, c2, c3, host));
}
