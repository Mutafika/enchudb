//! sync の bridge の soak (#268): 書き続ける peer A から pull する peer B へ、 配布が止まらず全部届くかを長く回して見る。
//!
//! ```text
//! SOAK_SECS=1800 cargo run --release -p enchudb-sync --example bridge_soak
//! ```
//!
//! 1 行 1 JSON を標準出力に出し (progress / stalled / recovered / still_stalled / dropped / done)、 止まり・落ち・
//! B の取りこぼし・B が歴史を切られた回が 1 つでもあれば exit 1。 engine の #268 の警告は標準エラーに出る。
//! 0.37.3 の後の master で 30 分 × 4 本を回した結果と、 この soak が何を捕まえるかの変異は
//! `scripts/bridge-soak-2026-11-03/README.md`。
//!
//! #268 の報告 (ファイル同期の消費側アプリ) の形をまねて、 時間を縮めて回す。
//!
//! - A: 書き手 1 本。 files 表 (fid / hash / size = Number、 path = Tag) の 2 万件を塊で書き換え、 塊の間で休む
//! - A の publish thread: 50 ms ごとに `transfer_oplog_to_sync_ops` + `publish_since(ZERO)` (実機は 1 秒ごと)。
//!   engine の consumer も 100 ms ごとに bridge する (2 本が競る、 実機と同じ)
//! - B: 100 ms ごとに pull (歴史を切られたら bootstrap)
//! - 見張り: 書いた (commit した) のに A の `current_sync_lsn` が STALL_MS 動かない = 止まり。 1 行 1 JSON で残す。
//!   WAL の小さい soak では止まりが WAL を数秒で埋め、 落ち (`wal_dropped_records`) → floor → B の bootstrap で抜けるので、
//!   落ち / B が歴史を切られた (`history_truncated`) も異常として数える (変異 M1 で確かめた: 止まりは 5 秒に届かず、 落ち 5749 件)
//! - 終わり: 書き手を止めて A を書き出し、 B が全ファイルの最後の hash を持つまで待って突き合わせる
//!
//! WAL を小さく (既定 1 MiB = 区画 512 KiB) して畳む回数を実機より何桁も増やす。
//! 環境変数: SOAK_SECS (既定 600) / SOAK_WAL_KIB (1024) / SOAK_RATE (1 秒あたりの書き換えの上限、 既定 20000) /
//! SOAK_STALL_MS (5000) / SOAK_DIR (DB を置く directory、 既定は temp の `enchudb-bridge-soak`) / SOAK_SEED (1) /
//! SOAK_SIGN (A が署名する、 既定 1) / SOAK_REPORT_S (途中経過の間隔、 30)

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const FILES: usize = 20_000;

/// file ごとに A が最後に書いた (eid, hash)
type LastHash = Vec<Option<(u64, u32)>>;

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn make_engine(path: &str, peer: PeerId, wal: usize) -> Arc<Engine> {
    let _ = std::fs::remove_dir_all(path);
    let mut eng = Engine::create_with_cell_version(path, 1 << 20).unwrap();
    eng.define_table("files", 65_536).unwrap();
    eng.define_himo_in("files", "fid", ValueType::Number, 0).unwrap();
    eng.define_himo_in("files", "hash", ValueType::Number, 0).unwrap();
    eng.define_himo_in("files", "size", ValueType::Number, 0).unwrap();
    eng.define_himo_in("files", "path", ValueType::Tag, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, wal).unwrap();
    eng.set_peer_id(peer);
    eng
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

fn dump(eng: &Engine) -> String {
    let ring = eng.table_eid_usage("_sync_ops");
    let wal = eng.oplog().unwrap();
    format!(
        "\"oplog_head\":{},\"written_head\":{},\"oplog_checkpoint\":{},\"bridge_cursor\":{},\"bridge_last_from\":{},\"bridge_empty_scans\":{},\
         \"bridge_last_scan_stop\":\"{}\",\"bridge_pending_records\":{},\"wal_commit_failures\":{},\"wal_dropped_records\":{},\
         \"wal_drop_floor_bumps\":{},\"wal_room_folds\":{},\"lap\":{},\"switches\":{},\"sync_lsn\":{},\"durable_lsn\":{},\
         \"ring_live\":{},\"ring_free\":{},\"fold_safe\":{}",
        eng.oplog_head(),
        wal.written_head(),
        eng.oplog_checkpoint(),
        eng.sync_ops_bridge_offset(),
        eng.bridge_last_from(),
        eng.bridge_empty_scans(),
        eng.bridge_last_scan_stop(),
        eng.bridge_pending_records(),
        eng.wal_commit_failures(),
        eng.wal_dropped_records(),
        eng.wal_drop_floor_bumps(),
        eng.wal_room_folds(),
        wal.lap(),
        wal.switches(),
        eng.current_sync_lsn(),
        eng.durable_lsn(),
        ring.as_ref().map_or(0, |u| u.live),
        ring.as_ref().map_or(0, |u| u.free),
        eng.wal_fold_safe(),
    )
}

fn main() {
    let secs = env_u64("SOAK_SECS", 600);
    let wal_kib = env_u64("SOAK_WAL_KIB", 1024) as usize;
    let rate = env_u64("SOAK_RATE", 20_000);
    let stall_ms = env_u64("SOAK_STALL_MS", 5_000);
    let seed = env_u64("SOAK_SEED", 1);
    let dir = std::env::var("SOAK_DIR")
        .unwrap_or_else(|_| std::env::temp_dir().join("enchudb-bridge-soak").to_string_lossy().into_owned());
    std::fs::create_dir_all(&dir).unwrap();
    let (pa, pb) = (format!("{dir}/a-{seed}"), format!("{dir}/b-{seed}"));

    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let eng_a = make_engine(&pa, 1, wal_kib * 1024);
    // 実機と同じく A は署名する (SOAK_SIGN=0 で外す)。 署名の分だけ record を書く時間が延びる
    if env_u64("SOAK_SIGN", 1) == 1 {
        eng_a.set_keypair(Some(Arc::new(enchudb_oplog::keys::Keypair::generate())));
    }
    let eng_b = make_engine(&pb, 2, 64 * 1024 * 1024);
    let sync_a = Arc::new(Syncer::new(eng_a.clone(), transport.clone()));
    let sync_b = Arc::new(Syncer::new(eng_b.clone(), transport.clone()));
    sync_a.serve_state();

    let stop_writer = Arc::new(AtomicBool::new(false));
    let stop_all = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicU64::new(0));
    let last_hash: Arc<Mutex<LastHash>> = Arc::new(Mutex::new(vec![None; FILES]));
    let (b_applied, b_truncated, b_boots) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let stalls = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    // 書き手
    let writer = {
        let (eng, stop, committed, last_hash) = (eng_a.clone(), stop_writer.clone(), committed.clone(), last_hash.clone());
        std::thread::spawn(move || {
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
            let mut eids: Vec<Option<u64>> = vec![None; FILES];
            let mut written = 0u64;
            let start = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                // 塊の大きさ: 多くは小さく、 時々大きい (scan で一度に見つかった変更)
                let burst = match rng.below(10) {
                    0..=5 => 1 + rng.below(20),
                    6..=8 => 1 + rng.below(500),
                    _ => 1 + rng.below(5_000),
                };
                let per_commit = 1 + rng.below(50);
                for k in 0..burst {
                    let f = rng.below(FILES as u64) as usize;
                    let e = match eids[f] {
                        Some(e) => e,
                        None => {
                            let e = eng.entity_in("files").unwrap();
                            eng.tie_to(e, "files.fid", f as u32);
                            eng.tie_text_to(e, "files.path", &format!("dir{}/file{}.bundle", f % 97, f));
                            eids[f] = Some(e);
                            e
                        }
                    };
                    let h = rng.below(u32::MAX as u64 - 1) as u32;
                    eng.tie_to(e, "files.hash", h);
                    eng.tie_to(e, "files.size", rng.below(1_000_000) as u32);
                    last_hash.lock().unwrap()[f] = Some((e, h));
                    written += 1;
                    if (k + 1) % per_commit == 0 {
                        eng.commit();
                        committed.store(written, Ordering::Release);
                    }
                }
                eng.commit();
                committed.store(written, Ordering::Release);
                // 上限の速さを越えないように休む + 時々長く休む (畳んだ後に静かな時間を作る)
                let ahead_ms = (written * 1000 / rate.max(1)).saturating_sub(start.elapsed().as_millis() as u64);
                let idle = match rng.below(20) {
                    0 => 500 + rng.below(2_500),
                    1..=4 => rng.below(200),
                    _ => 0,
                };
                std::thread::sleep(Duration::from_millis(ahead_ms + idle));
            }
        })
    };

    // A の publish (host の publish thread のまね)
    let publisher = {
        let (eng, sync, stop) = (eng_a.clone(), sync_a.clone(), stop_all.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                eng.transfer_oplog_to_sync_ops();
                sync.publish_since(Hlc::ZERO);
                std::thread::sleep(Duration::from_millis(50));
            }
        })
    };

    // B の pull
    let puller = {
        let (sync, stop, applied, truncated, boots) = (sync_b.clone(), stop_all.clone(), b_applied.clone(), b_truncated.clone(), b_boots.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let out = sync.pull_once(1);
                applied.fetch_add(out.applied as u64, Ordering::Relaxed);
                if out.history_truncated {
                    truncated.fetch_add(1, Ordering::Relaxed);
                    if sync.bootstrap_pull(1).is_some() {
                        boots.fetch_add(1, Ordering::Relaxed);
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };

    // 見張り
    let watcher = {
        let (eng, stop, committed, stalls) = (eng_a.clone(), stop_all.clone(), committed.clone(), stalls.clone());
        std::thread::spawn(move || {
            let mut last_lsn = eng.current_sync_lsn();
            let mut changed_at = Instant::now();
            let mut committed_at_change = committed.load(Ordering::Acquire);
            let mut in_stall = false;
            let mut last_still = Instant::now();
            let mut last_dropped = 0u64;
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(200));
                let dropped = eng.wal_dropped_records();
                if dropped != last_dropped {
                    println!("{{\"kind\":\"dropped\",\"at_ms\":{},\"new\":{},{}}}", now_ms(), dropped - last_dropped, dump(&eng));
                    last_dropped = dropped;
                }
                let lsn = eng.current_sync_lsn();
                let c = committed.load(Ordering::Acquire);
                if lsn != last_lsn {
                    if in_stall {
                        println!("{{\"kind\":\"recovered\",\"at_ms\":{},\"stalled_ms\":{},{}}}", now_ms(), changed_at.elapsed().as_millis(), dump(&eng));
                        in_stall = false;
                    }
                    last_lsn = lsn;
                    changed_at = Instant::now();
                    committed_at_change = c;
                } else if c > committed_at_change && changed_at.elapsed() >= Duration::from_millis(stall_ms) {
                    if !in_stall {
                        in_stall = true;
                        last_still = Instant::now();
                        stalls.fetch_add(1, Ordering::Relaxed);
                        println!("{{\"kind\":\"stalled\",\"at_ms\":{},\"elapsed_ms\":{},\"pending_writes\":{},{}}}", now_ms(), changed_at.elapsed().as_millis(), c - committed_at_change, dump(&eng));
                    } else if last_still.elapsed() >= Duration::from_secs(30) {
                        last_still = Instant::now();
                        println!("{{\"kind\":\"still_stalled\",\"at_ms\":{},\"elapsed_ms\":{},{}}}", now_ms(), changed_at.elapsed().as_millis(), dump(&eng));
                    }
                }
            }
        })
    };

    // 途中経過
    let mut next_report = Duration::from_secs(10);
    while t0.elapsed() < Duration::from_secs(secs) {
        std::thread::sleep(Duration::from_millis(200));
        if t0.elapsed() >= next_report {
            next_report += Duration::from_secs(env_u64("SOAK_REPORT_S", 30));
            println!(
                "{{\"kind\":\"progress\",\"t_s\":{},\"written\":{},\"b_applied\":{},\"b_truncated\":{},\"b_boots\":{},\"stalls\":{},{},\"b\":{{{}}}}}",
                t0.elapsed().as_secs(),
                committed.load(Ordering::Acquire),
                b_applied.load(Ordering::Relaxed),
                b_truncated.load(Ordering::Relaxed),
                b_boots.load(Ordering::Relaxed),
                stalls.load(Ordering::Relaxed),
                dump(&eng_a),
                dump(&eng_b)
            );
        }
    }

    // 終わり: 書き手を止めて書き出し、 B が追いつくのを待つ
    stop_writer.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    eng_a.commit();
    eng_a.flush_writes();
    let _ = eng_a.oplog_sync();
    let want: LastHash = last_hash.lock().unwrap().clone();
    let a_bad = want.iter().filter_map(|w| *w).filter(|&(e, h)| eng_a.get(e, "files.hash") != Some(h as u64)).count();
    let missing = |eng_b: &Engine| -> usize {
        want.iter()
            .enumerate()
            .filter(|(_, w)| w.is_some())
            .filter(|(f, w)| {
                let h = w.unwrap().1 as u64;
                let got = eng_b.pull_raw("files.fid", *f as u32).first().and_then(|&e| eng_b.get(e, "files.hash"));
                got != Some(h)
            })
            .count()
    };
    let settle_start = Instant::now();
    let mut miss = usize::MAX;
    while settle_start.elapsed() < Duration::from_secs(60) {
        let _ = eng_a.oplog_sync();
        eng_a.transfer_oplog_to_sync_ops();
        std::thread::sleep(Duration::from_millis(500));
        miss = missing(&eng_b);
        if miss == 0 {
            break;
        }
    }
    stop_all.store(true, Ordering::Relaxed);
    for t in [publisher, puller, watcher] {
        t.join().unwrap();
    }
    let files = want.iter().filter(|w| w.is_some()).count();
    println!(
        "{{\"kind\":\"done\",\"secs\":{},\"wal_kib\":{},\"rate\":{},\"seed\":{},\"written\":{},\"files\":{},\"a_bad\":{},\"b_missing\":{},\
         \"settle_ms\":{},\"b_applied\":{},\"b_truncated\":{},\"b_boots\":{},\"stalls\":{},{}}}",
        secs,
        wal_kib,
        rate,
        seed,
        committed.load(Ordering::Acquire),
        files,
        a_bad,
        miss,
        settle_start.elapsed().as_millis(),
        b_applied.load(Ordering::Relaxed),
        b_truncated.load(Ordering::Relaxed),
        b_boots.load(Ordering::Relaxed),
        stalls.load(Ordering::Relaxed),
        dump(&eng_a)
    );
    let eng_dropped = eng_a.wal_dropped_records();
    drop((sync_a, sync_b));
    drop((eng_a, eng_b));
    let _ = std::fs::remove_dir_all(&pa);
    let _ = std::fs::remove_dir_all(&pb);
    let ok = miss == 0
        && a_bad == 0
        && stalls.load(Ordering::Relaxed) == 0
        && eng_dropped == 0
        && b_truncated.load(Ordering::Relaxed) == 0;
    std::process::exit(if ok { 0 } else { 1 });
}
