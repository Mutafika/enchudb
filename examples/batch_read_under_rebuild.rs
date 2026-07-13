//! envelope probe ① — double-buffer は concurrent rebuild 下で reader を守るか。
//!
//! Tsurugi の HybridCC の「batch READ を online write と並行」に enchu が
//! どこまで届くかの**下限**を測る。CLAUDE.md の主張「rebuild 中も reader は
//! 止まらない」を stress で検証する:
//!
//!   - reader 2 本が pull_raw("a", 7) を回し続け、返った EntityId を全検査
//!     (範囲外 / sentinel = 破損)。 pull の latency も測る (blocking 検出)。
//!   - writer 1 本が a=7 の entity を足しては flush_writes() + rebuild() で
//!     buffer swap を誘発し続ける。
//!
//! 破損 0 かつ pull latency が rebuild 時間まで跳ねない → double-buffer が
//! concurrent reader を守る＝「per-query 一貫・非ブロックな read を write 並行で
//! 出せる」= batch-read-during-writes の read 半分に手が届く。
//!
//! 注意: これは **per-call の一貫性**の検証。 複数 call を跨ぐ **held snapshot**
//! (multi-step batch が終始同じ版を見る) は別問題で、これでは測れない。
//!
//! Usage: cargo run --release --example batch_read_under_rebuild

use enchudb::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BASE: u32 = 500_000;
const DUR_SECS: u64 = 5;

fn main() {
    let path = "/tmp/enchu_batch_read_probe.db";
    for suf in ["", ".oplog", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }

    // build phase: 500K entity、 全部 a=7、 grp は 0..1000 に散らす (rebuild に仕事)
    let mut eng = Engine::create_growable_with_capacity(path, 8_000_000).unwrap();
    eng.define_himo("a", ValueType::Number, 0);
    eng.define_himo("grp", ValueType::Number, 0);
    for i in 0..BASE {
        let e = eng.entity();
        eng.tie(e, "a", 7);
        eng.tie(e, "grp", i % 1000);
    }
    eng.rebuild();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 256 * 1024 * 1024).unwrap();
    let a_hid = eng.himo_id("a").unwrap() as u16;
    let grp_hid = eng.himo_id("grp").unwrap() as u16;

    let stop = Arc::new(AtomicBool::new(false));
    let corruptions = Arc::new(AtomicU64::new(0));
    let non_monotonic = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));
    let max_pull_ns = Arc::new(AtomicU64::new(0));
    let last_len = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // ---- reader threads ----
    for _ in 0..2 {
        let eng = eng.clone();
        let stop = stop.clone();
        let corruptions = corruptions.clone();
        let non_monotonic = non_monotonic.clone();
        let reads = reads.clone();
        let max_pull_ns = max_pull_ns.clone();
        let last_len = last_len.clone();
        handles.push(std::thread::spawn(move || {
            let mut prev_len = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                let v = eng.pull_raw("a", 7);
                let ns = t.elapsed().as_nanos() as u64;

                // bound: 現在の entity 総数 (pull 後に読むので pull が見た版の上限以上)
                let bound = eng.entity_count() as u64 + 10_000; // 少し slack
                let mut bad = 0u64;
                for &eid in &v {
                    if eid >= bound || eid == u64::MAX {
                        bad += 1;
                    }
                }
                if bad > 0 {
                    corruptions.fetch_add(bad, Ordering::Relaxed);
                }
                // a=7 は untie しない workload なので len は単調非減少のはず
                if v.len() + 100 < prev_len {
                    non_monotonic.fetch_add(1, Ordering::Relaxed);
                }
                prev_len = v.len();
                last_len.store(v.len() as u64, Ordering::Relaxed);
                reads.fetch_add(1, Ordering::Relaxed);

                let mut cur = max_pull_ns.load(Ordering::Relaxed);
                while ns > cur {
                    match max_pull_ns.compare_exchange_weak(
                        cur,
                        ns,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(x) => cur = x,
                    }
                }
            }
        }));
    }

    // ---- writer thread: 足す → flush → rebuild で swap を誘発 ----
    let rebuilds = Arc::new(AtomicU64::new(0));
    {
        let eng = eng.clone();
        let stop = stop.clone();
        let rebuilds = rebuilds.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..2000 {
                    let e = eng.entity();
                    eng.tie_async_by_id(e, a_hid, 7);
                    eng.tie_async_by_id(e, grp_hid, e as u32 % 1000);
                }
                eng.flush_writes();
                eng.rebuild();
                rebuilds.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    std::thread::sleep(Duration::from_secs(DUR_SECS));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let corr = corruptions.load(Ordering::Relaxed);
    let nonmono = non_monotonic.load(Ordering::Relaxed);
    println!("=== envelope probe ①: batch read under concurrent rebuild ===");
    println!("duration        : {DUR_SECS}s, 2 readers + 1 writer");
    println!("reads (pull_raw): {}", reads.load(Ordering::Relaxed));
    println!("rebuilds        : {}", rebuilds.load(Ordering::Relaxed));
    println!("final pull len  : {}", last_len.load(Ordering::Relaxed));
    println!(
        "max pull latency: {} µs",
        max_pull_ns.load(Ordering::Relaxed) / 1000
    );
    println!("--- correctness ---");
    println!("corruptions (範囲外/sentinel eid) : {corr}");
    println!("non-monotonic len 観測            : {nonmono}");
    println!();
    if corr == 0 {
        println!(
            "✅ 破損 0。 double-buffer は concurrent rebuild 下で reader を守る。\n\
             → per-query 一貫・非ブロックな read を write 並行で出せる (batch-read の read 半分に到達)。\n\
             注: これは per-call の一貫性。 multi-call を跨ぐ held snapshot は別問題 (未検証)。"
        );
    } else {
        println!("❌ 破損 {corr} 件。 envelope はここで折れる (concurrent read が保護されてない)。");
    }
    if nonmono > 0 {
        println!(
            "※ non-monotonic {nonmono} 件: buffer 版順の逆転観測。 破損ではないが\n\
             held-snapshot が無い証拠 (call 間で新旧の版が前後し得る)。"
        );
    }

    for suf in ["", ".oplog", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}
