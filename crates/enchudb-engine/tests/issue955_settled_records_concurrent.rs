//! #955 (F5): 配る側の本番の道 (`Engine::settled_sync_records`、 `Syncer` の publish とホストの pull が読む) を、 書き手・受け手
//! (ack + 回収)・bridge が並んで動く中で読む。 読んだ答えのどれでも:
//!
//! - (a) 同じ (書き手, 版数) の組 (中継した state の形、 #828 / #830) は全部か 0 本。 受け手がまだ ack していない組について見る
//!   (ack した組は回収で前から消えるので、 読んでいる間に一部だけ残りうる。 その組は受け手がもう受けている)
//! - (b) 1 回の答えに同じ record が 2 つ無い
//! - (c) 自分の書いた Tie は版数の昇順 (lsn の順 = WAL に積んだ順)
//!
//! 組を載せる側は、 載せ終えて書き出す (`oplog_sync` = Commit + bridge の 1 周) まで配る側の読みを止める (ホストの口の gate #753、
//! 中継の `state_relay` #762 と同じ形。 ここでは RwLock の write)。 ring は小さく (枠 500 ほど)、 受け手は組を見つけてから 3 周 ack を
//! 遅らせるので、 組はよく ring の端で割れる (割れた組を受け手が見た回の数を前提として見る)。 回収で空いた場所は次の行に使い回される
//! (読む前に使い回された場所を読まない事は、 engine の単体 `a_slot_reused_between_the_scan_and_the_read_is_not_handed_out` が
//! 決まった順で見る)。
//!
//! falsify (`scripts/mutate.py`、 Mac M4 Max で各 10 回): `settled_sync_records` が配ってよい行の終わりを見ない (`lsn <= settled` を
//! 外す) と (a) で落ちる (10 / 10)。 読む時に lsn の列を読み直してその値で ring を引く (#949 の前の読み) のは、 この試験では
//! 落ちなかった (0 / 10。 なめてから読むまでの間に場所が使い回される窓が狭い。 単体の試験が決まった順で見る)。

use enchudb_engine::transport::WireRecord;
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

const ME: PeerId = 1;
/// 組の書き手 (中継した state の書き手)。
const AUTHOR: PeerId = 9;
const PULLER: PeerId = 77;
const RUN: Duration = Duration::from_millis(1500);
const WAL: usize = 64 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("enchudb-issue955-{tag}-{}-{nanos}", std::process::id())).to_str().unwrap().to_string()
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn next(x: &mut u64) -> u64 {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    *x
}

struct Shared {
    eng: Arc<Engine>,
    /// 組を載せる間の、 配る側の読みの止め
    gate: RwLock<()>,
    /// 組の版数 (wall) → 本数。 載せる前に置く。 0 = 載せ切れなかった組 (WAL が満杯で落ちた。 見ない)
    sizes: Mutex<HashMap<u64, usize>>,
    /// 受け手が ack した組の版数 (wall) の上限。 ack する前に上げる
    acked: AtomicU64,
    stop: AtomicBool,
    failures: Mutex<Vec<String>>,
    groups: AtomicUsize,
    broken_groups: AtomicUsize,
    /// 受け手が見た、 ack していない組が ring の上で割れていた周の数
    splits_seen: AtomicUsize,
    reads: AtomicUsize,
    /// 答えに ack していない組が全部入っていた読みの数
    reads_with_group: AtomicUsize,
}

impl Shared {
    fn fail(&self, msg: String) {
        let mut f = self.failures.lock().unwrap();
        if f.len() < 5 {
            f.push(msg);
        }
        self.stop.store(true, Ordering::SeqCst);
    }
}

/// 書き手: 自分の Tie を少しずつ書き、 3 回に 1 回ほど組 (50〜300 本) を中継した形で載せる。
fn writer(s: &Shared) {
    let eng = &s.eng;
    let e = eng.entity_in("notes").unwrap();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut i = 0u32;
    let mut wall = 1_000u64;
    let mut vid = 0u32;
    while !s.stop.load(Ordering::SeqCst) {
        for _ in 0..1 + next(&mut rng) % 20 {
            eng.tie_to(e, "notes.note", i);
            i += 1;
        }
        if next(&mut rng).is_multiple_of(3) {
            let size = 50 + (next(&mut rng) % 251) as usize;
            let _gate = s.gate.write().unwrap();
            wall += 1;
            s.sizes.lock().unwrap().insert(wall, size);
            let hlc = Hlc { wall, logical: 0, peer: AUTHOR };
            let mut ok = true;
            for _ in 0..size {
                let rec = WireRecord::unsigned(hlc, AUTHOR, DecodedOp::Vocab { vid, bytes: format!("g{vid}").into_bytes() });
                ok &= eng.relay_record(&rec);
                vid += 1;
            }
            if !ok {
                s.sizes.lock().unwrap().insert(wall, 0);
                s.broken_groups.fetch_add(1, Ordering::Relaxed);
            }
            // 載せ終えて書き出してから読みを通す (Commit + bridge の 1 周)
            let _ = eng.oplog_sync();
            s.groups.fetch_add(1, Ordering::Relaxed);
        }
        std::thread::sleep(Duration::from_micros(300));
    }
}

/// 受け手: ring の行を見て、 自分の Tie は全部、 組は 3 周見てから ack し、 回収して bridge を回す。
fn acker(s: &Shared) {
    let eng = &s.eng;
    let mut seen: HashMap<u64, u32> = HashMap::new();
    while !s.stop.load(Ordering::SeqCst) {
        let ring: Vec<(PeerId, Hlc)> = eng
            .pending_sync_ops(0)
            .iter()
            .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
            .filter(|r| !matches!(r.op, DecodedOp::Commit))
            .map(|r| (r.author_peer, r.hlc))
            .collect();
        let top_me = ring.iter().filter(|(a, _)| *a == ME).map(|(_, h)| *h).max();
        let mut on: HashMap<u64, usize> = HashMap::new();
        for (_, h) in ring.iter().filter(|(a, _)| *a == AUTHOR) {
            *on.entry(h.wall).or_default() += 1;
        }
        let acked = s.acked.load(Ordering::SeqCst);
        {
            let sizes = s.sizes.lock().unwrap();
            if on.iter().any(|(w, c)| *w > acked && sizes.get(w).is_some_and(|n| *n > 0 && c < n)) {
                s.splits_seen.fetch_add(1, Ordering::Relaxed);
            }
        }
        for w in on.keys() {
            *seen.entry(*w).or_default() += 1;
        }
        if let Some(w) = seen.iter().filter(|(_, r)| **r >= 3).map(|(w, _)| *w).max() {
            s.acked.fetch_max(w, Ordering::SeqCst);
        }
        let acked = s.acked.load(Ordering::SeqCst);
        seen.retain(|w, _| *w > acked);
        let mut cursors = vec![(AUTHOR, Hlc { wall: acked, logical: u32::MAX, peer: u32::MAX })];
        if let Some(t) = top_me {
            cursors.push((ME, t));
        }
        let _ = eng.ack_sync_up_to_cursors(PULLER, &cursors);
        eng.reclaim_sync_ops();
        eng.transfer_oplog_to_sync_ops();
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// 読み手: 配る側の道で読み、 (a) (b) (c) を見る。 半分は (書き手, 版数) だけで決まる絞りを掛ける。
fn reader(s: &Shared, seed: u64) {
    let eng = &s.eng;
    let mut n = seed;
    while !s.stop.load(Ordering::SeqCst) {
        n += 1;
        let all = n.is_multiple_of(2);
        let recs = {
            let _gate = s.gate.read().unwrap();
            if all {
                eng.settled_sync_records(|_, _| true)
            } else {
                eng.settled_sync_records(|a, h| a != AUTHOR || h.wall % 2 == 0)
            }
        };
        // 読み終えた後に読む: これより上の組は、 読んでいる間に一度も ack されていない (= 回収で消えていない)
        let acked = s.acked.load(Ordering::SeqCst);
        let sizes = s.sizes.lock().unwrap().clone();
        let mut per: HashMap<u64, usize> = HashMap::new();
        for r in recs.iter().filter(|r| r.author_peer == AUTHOR) {
            *per.entry(r.hlc.wall).or_default() += 1;
        }
        let mut whole = 0;
        for (w, c) in &per {
            if *w <= acked {
                continue;
            }
            let size = sizes.get(w).copied().unwrap_or(0);
            if size == 0 {
                continue;
            }
            if *c != size {
                s.fail(format!("(a) 組 (書き手 {AUTHOR}, wall {w}) を割って配った: {c} / {size} 本 (ack は wall {acked} まで、 絞り {all})"));
            } else {
                whole += 1;
            }
        }
        let mut keys = HashSet::new();
        for r in &recs {
            if !keys.insert((r.author_peer, r.hlc, r.signed_bytes.clone())) {
                s.fail(format!("(b) 1 回の答えに同じ record が 2 つ: (書き手 {}, {:?})", r.author_peer, r.hlc));
                break;
            }
        }
        let mine: Vec<Hlc> =
            recs.iter().filter(|r| r.author_peer == ME && matches!(r.op, DecodedOp::Tie { .. })).map(|r| r.hlc).collect();
        if let Some(w) = mine.windows(2).find(|w| w[0] >= w[1]) {
            s.fail(format!("(c) 自分の Tie が版数の昇順でない: {:?} の後に {:?}", w[0], w[1]));
        }
        s.reads.fetch_add(1, Ordering::Relaxed);
        if whole > 0 {
            s.reads_with_group.fetch_add(1, Ordering::Relaxed);
        }
        std::thread::sleep(Duration::from_micros(50));
    }
}

#[test]
fn concurrent_reads_never_split_a_group_or_repeat_or_reorder_records() {
    let path = tmp_path("stress");
    let _cleanup = Cleanup(path.clone());
    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(ME);
    let ring = eng.table_eid_usage("_sync_ops").unwrap().capacity;
    assert!((300..1000).contains(&ring), "前提: ring は小さい ({ring} 行)");
    let s = Arc::new(Shared {
        eng,
        gate: RwLock::new(()),
        sizes: Mutex::new(HashMap::new()),
        acked: AtomicU64::new(0),
        stop: AtomicBool::new(false),
        failures: Mutex::new(Vec::new()),
        groups: AtomicUsize::new(0),
        broken_groups: AtomicUsize::new(0),
        splits_seen: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
        reads_with_group: AtomicUsize::new(0),
    });
    let mut threads = Vec::new();
    {
        let s = s.clone();
        threads.push(std::thread::spawn(move || writer(&s)));
    }
    {
        let s = s.clone();
        threads.push(std::thread::spawn(move || acker(&s)));
    }
    for seed in 0..2u64 {
        let s = s.clone();
        threads.push(std::thread::spawn(move || reader(&s, seed)));
    }
    let t0 = Instant::now();
    while t0.elapsed() < RUN && !s.stop.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(10));
    }
    s.stop.store(true, Ordering::SeqCst);
    for t in threads {
        t.join().expect("thread が panic した");
    }
    let failures = s.failures.lock().unwrap().clone();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    let groups = s.groups.load(Ordering::Relaxed);
    let broken = s.broken_groups.load(Ordering::Relaxed);
    let splits = s.splits_seen.load(Ordering::Relaxed);
    let reads = s.reads.load(Ordering::Relaxed);
    let with_group = s.reads_with_group.load(Ordering::Relaxed);
    eprintln!("ring {ring} 行、 組 {groups} (載せ切れなかった {broken})、 割れた組を見た周 {splits}、 読み {reads} (ack していない組が全部入った {with_group})");
    assert!(groups >= 20 && broken * 2 < groups, "前提: 組を載せた ({groups}、 載せ切れなかった {broken})");
    assert!(splits >= 3, "前提: ack していない組が ring の端で割れた周がある ({splits})");
    assert!(reads >= 100 && with_group >= 10, "前提: 読んだ ({reads})、 ack していない組を配った読みがある ({with_group})");
    let s = Arc::try_unwrap(s).ok().expect("前提: thread は全部終わった");
    assert_eq!(Arc::strong_count(&s.eng), 1);
}
