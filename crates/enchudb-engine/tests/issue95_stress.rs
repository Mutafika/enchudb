//! #95 破壊テスト — LockFreeCylinder を敵対的条件で殴る。
//!
//! smoke（issue95_lockfree_read.rs）とは別に、 lock-free 構造が「壊れない」
//! ことを churn / crash / grow の 3 面から詰める。
//!
//! (1) churn_storm_exact       — 大量の値更新で bucket を stale だらけにし、
//!                               並行 read の構造 invariant（dup なし・範囲内）を保ちつつ、
//!                               quiesce 後の pull が **live 集合と厳密一致**する。
//! (2) crash_recovery_compacts — churn → drop（crash 相当）→ reopen で Cylinder が
//!                               column から rebuild され、 stale が消え pull が厳密。
//! (3) grow_under_read         — dense 配列の realloc を多発させつつ reader が
//!                               旧配列を掴んだまま回っても epoch 解放が安全（破損なし）。

use enchudb_engine::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

fn fresh(path: &str) {
    for suf in ["", ".oplog", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn define(eng: &Arc<Engine>, name: &str, vt: ValueType) -> u16 {
    // build phase 相当: Arc 単一所有のうちに define。
    let eng_mut = unsafe { &mut *(Arc::as_ptr(eng) as *mut Engine) };
    eng_mut.define_himo(name, vt, 0);
    eng.himo_id(name).unwrap() as u16
}

const VALUES: u32 = 8;

/// (1) churn storm: 更新の嵐で stale を積み、 quiesce 後に厳密一致を要求。
#[test]
fn churn_storm_exact() {
    let path = "/tmp/test_issue95_churn.db";
    fresh(path);
    // oplog は churn 全量（~820k ties）を drop なく載せられるサイズにする。
    let eng: Arc<Engine> =
        Engine::create_concurrent_with_oplog(path, 256 * 1024 * 1024).expect("create");
    let vhid = define(&eng, "v", ValueType::Number);

    let n = 20_000u32;
    let rounds = 40u32;

    // entity を確保して初期値を張る。
    let eids: Vec<_> = (0..n).map(|_| eng.entity()).collect();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie_async_by_id(e, vhid, (i as u32) % VALUES);
    }
    eng.flush_writes();

    // reader x4: 並走中は「構造 invariant」だけ検査（値の一致は writer と race するので不可）。
    //   - 返る eid に重複なし（append + dedup 保証）
    //   - eid が既知集合内（範囲外 = 破損）
    let stop = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(AtomicU64::new(0));
    let readers: Vec<_> = (0..4)
        .map(|r| {
            let eng = eng.clone();
            let stop = stop.clone();
            let bad = bad.clone();
            std::thread::spawn(move || {
                let mut k = r % VALUES;
                let mut seen = vec![false; n as usize];
                while !stop.load(Ordering::Relaxed) {
                    let hits = eng.pull_raw("v", k);
                    for &e in &hits {
                        let lid = enchudb_oplog::eid_local(e);
                        if lid >= n {
                            bad.fetch_add(1, Ordering::Relaxed); // 範囲外 = 破損
                            continue;
                        }
                        if seen[lid as usize] {
                            bad.fetch_add(1, Ordering::Relaxed); // 同一 pull 内で dup
                        }
                        seen[lid as usize] = true;
                    }
                    for &e in &hits {
                        let lid = enchudb_oplog::eid_local(e) as usize;
                        if lid < n as usize {
                            seen[lid] = false;
                        }
                    }
                    k = (k + 1) % VALUES;
                }
            })
        })
        .collect();

    // writer: 各 entity を round ごとに別の値へ張り替え → 旧 bucket に stale が溜まる。
    // deterministic（決定的）に final value を追跡する。
    let mut final_val = vec![0u32; n as usize];
    for round in 1..=rounds {
        for (i, &e) in eids.iter().enumerate() {
            // index と round を混ぜた擬似ランダムな値遷移（seed 不要・決定的）
            let v = ((i as u32).wrapping_mul(2654435761).wrapping_add(round.wrapping_mul(40503)))
                % VALUES;
            eng.tie_async_by_id(e, vhid, v);
            final_val[i] = v;
        }
    }
    eng.flush_writes();
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().unwrap();
    }
    assert_eq!(bad.load(Ordering::Relaxed), 0, "並行 read で構造破損を観測");

    // quiesce 後: churn で stale だらけの bucket が、 lazy verify + dedup で live 集合に厳密一致。
    let mut expect: Vec<Vec<u32>> = vec![Vec::new(); VALUES as usize];
    for i in 0..n as usize {
        expect[final_val[i] as usize].push(i as u32);
    }
    let mut total_live = 0usize;
    for k in 0..VALUES {
        let mut got: Vec<u32> = eng
            .pull_raw("v", k)
            .iter()
            .map(|&e| enchudb_oplog::eid_local(e))
            .collect();
        got.sort_unstable();
        assert_eq!(
            got, expect[k as usize],
            "value {k}: churn 後の pull が live 集合と一致しない（stale 残 or 欠落）"
        );
        total_live += got.len();
    }
    assert_eq!(total_live, n as usize, "全 entity がちょうど 1 bucket に属さない");
    fresh(path);
}

/// (2) crash → reopen で Cylinder が column から rebuild され、 churn の stale が消える。
#[test]
fn crash_recovery_compacts() {
    let path = "/tmp/test_issue95_crash.db";
    fresh(path);

    let n = 5_000u32;
    let mut final_val = vec![0u32; n as usize];

    // --- phase 1: churn して durable 化してから drop（= crash 相当）---
    {
        let eng: Arc<Engine> =
            Engine::create_concurrent_with_oplog(path, 32 * 1024 * 1024).expect("create");
        let vhid = define(&eng, "v", ValueType::Number);
        let eids: Vec<_> = (0..n).map(|_| eng.entity()).collect();
        // 3 回張り替えて各 bucket に stale を作る。
        for pass in 0..3u32 {
            for (i, &e) in eids.iter().enumerate() {
                let v = ((i as u32).wrapping_add(pass.wrapping_mul(3))) % VALUES;
                eng.tie_async_by_id(e, vhid, v);
                final_val[i] = v;
            }
        }
        eng.flush_writes();
        eng.oplog_sync().expect("durable"); // fsync + body msync + checkpoint
        drop(eng); // crash 相当（明示 flush なし、 durable state のみ残る）
    }

    // --- phase 2: reopen。 Cylinder は column から lazy rebuild → stale ゼロで厳密一致 ---
    {
        let eng: Arc<Engine> =
            Engine::open_concurrent_with_oplog(path, 32 * 1024 * 1024).expect("reopen");
        let mut expect: Vec<Vec<u32>> = vec![Vec::new(); VALUES as usize];
        for i in 0..n as usize {
            expect[final_val[i] as usize].push(i as u32);
        }
        let mut total = 0usize;
        for k in 0..VALUES {
            let mut got: Vec<u32> = eng
                .pull_raw("v", k)
                .iter()
                .map(|&e| enchudb_oplog::eid_local(e))
                .collect();
            got.sort_unstable();
            assert_eq!(
                got, expect[k as usize],
                "reopen 後 value {k}: rebuild が live 集合に一致しない"
            );
            total += got.len();
        }
        assert_eq!(total, n as usize, "reopen 後の総 live 数が合わない");
    }
    fresh(path);
}

/// (3) dense 配列 realloc の多発 × 旧配列を掴む reader = epoch 解放の安全性。
#[test]
fn grow_under_read() {
    let path = "/tmp/test_issue95_grow.db";
    fresh(path);
    let eng: Arc<Engine> =
        Engine::create_concurrent_with_oplog(path, 32 * 1024 * 1024).expect("create");
    let vhid = define(&eng, "v", ValueType::Number);

    let stop = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(AtomicU64::new(0));

    // reader: 小 value を読み続ける。 dense 配列が背後で何度も swap されても
    // 掴んだ旧配列が epoch で解放されず読めること（use-after-free なら破損 or crash）。
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let eng = eng.clone();
            let stop = stop.clone();
            let bad = bad.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let hits = eng.pull_raw("v", 1);
                    // value 1 に張った eid は必ず素性が知れている（下の writer 参照）。
                    for &e in &hits {
                        let lid = enchudb_oplog::eid_local(e);
                        // value 1 に入れたのは lid % 100 == 1 のものだけ。
                        if lid % 100 != 1 {
                            bad.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    // writer: value を疎に大きくしていって dense 外側 Vec の resize を多発させる。
    // value = lid % 100（1 は reader が読む固定 bucket、 他は配列を伸ばすため散らす）。
    let n = 200_000u32;
    for _ in 0..n {
        let e = eng.entity();
        let lid = enchudb_oplog::eid_local(e);
        eng.tie_async_by_id(e, vhid, lid % 100);
    }
    eng.flush_writes();
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().unwrap();
    }
    assert_eq!(bad.load(Ordering::Relaxed), 0, "grow 中の read で破損を観測");

    // value 1 の件数 = lid % 100 == 1 の entity 数。
    let want = (0..n).filter(|lid| lid % 100 == 1).count();
    assert_eq!(eng.pull_raw("v", 1).len(), want, "grow 後の件数不一致");
    fresh(path);
}
