//! 64 bit 列 (`ValueType::Number64`、 cell 8B、 FILE_VERSION 11)。
//!
//! - u32 に入らない値が書けて読め、 等値で引ける (u32 の列との AND も)
//! - 幅に入らない値は書かない (u64::MAX = 空の印 / u32 の列への大きな値)、 fault に積む
//! - reopen で戻り、 64 bit 列を持つ DB だけ v11 になる
//! - まだ対応していない経路 (live / u32 の集計) は黙って上位を切らずに断る
use enchudb_engine::{db_files, Engine, FaultKind, LivePred, ValueType};
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    let p = format!("{}/enchudb-number64-{}-{}", std::env::temp_dir().display(), tag, std::process::id());
    let _ = db_files::remove_db(&p);
    p
}

/// header の版 (header.seg の offset 4)。
fn file_version(path: &str) -> u32 {
    let b = std::fs::read(std::path::Path::new(path).join("header.seg")).unwrap();
    u32::from_le_bytes(b[4..8].try_into().unwrap())
}

const BIG: [u64; 6] = [0, u32::MAX as u64 - 1, u32::MAX as u64, u32::MAX as u64 + 1, 1 << 40, u64::MAX - 1];

#[test]
fn values_beyond_u32_round_trip_and_are_pulled() {
    let p = tmp("roundtrip");
    let mut eng = Engine::create_with_capacity(&p, 4096).unwrap();
    eng.define_table("t", 3900).unwrap();
    eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
    let ts = eng.himo_id("t.ts").unwrap() as u16;
    let es: Vec<u64> = (0..BIG.len()).map(|_| eng.entity_in("t").unwrap()).collect();
    for (i, (&e, &v)) in es.iter().zip(BIG.iter()).enumerate() {
        eng.tie_by_id(e, ts, v);
        eng.tie(e, "t.n", (i % 2) as u32);
    }
    for (&e, &v) in es.iter().zip(BIG.iter()) {
        assert_eq!(eng.get(e, "t.ts"), Some(v), "get");
        assert_eq!(eng.get_by_id(e, ts), Some(v), "get_by_id");
        assert_eq!(eng.pull_raw("t.ts", v), vec![enchudb_oplog::eid_local(e) as u64], "pull_raw {v}");
    }
    // 下位 32 bit が同じ別の値に当たらない
    assert!(eng.pull_raw("t.ts", (1u64 << 40) + 1).is_empty());
    assert!(eng.pull_raw("t.ts", 1u64 << 8).is_empty());
    // u32 の列との AND
    let n = eng.himo_id("t.n").unwrap() as u16;
    assert_eq!(eng.query_by_id64(&[(ts, u32::MAX as u64 + 1), (n, 1)]), vec![es[3]]);
    assert!(eng.query_by_id64(&[(ts, u32::MAX as u64 + 1), (n, 0)]).is_empty());
    // 容量の端の entity (4B 幅の領域なら半分より先は範囲外)
    let far = (0..2600).map(|_| eng.entity_in("t").unwrap()).last().unwrap();
    assert!(enchudb_oplog::eid_local(far) > 2100, "端の entity になっていない: {far}");
    eng.tie_by_id(far, ts, u64::MAX - 2);
    assert_eq!(eng.get(far, "t.ts"), Some(u64::MAX - 2), "容量の端の cell");
    // 書き換え / 外す / 削除
    eng.tie_by_id(es[4], ts, 1u64 << 41);
    assert!(eng.pull_raw("t.ts", 1u64 << 40).is_empty(), "旧値の bucket に残っている");
    assert_eq!(eng.pull_raw("t.ts", 1u64 << 41), vec![enchudb_oplog::eid_local(es[4]) as u64]);
    eng.untie(es[5], "t.ts");
    assert_eq!(eng.get(es[5], "t.ts"), None);
    eng.delete(es[3]);
    assert!(eng.pull_raw("t.ts", u32::MAX as u64 + 1).is_empty());
    drop(eng);
    let _ = db_files::remove_db(&p);
}

#[test]
fn values_that_do_not_fit_are_rejected_with_a_fault() {
    let p = tmp("reject");
    let mut eng = Engine::create_with_capacity(&p, 4096).unwrap();
    eng.define_table("t", 1024).unwrap();
    eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
    let (ts, n) = (eng.himo_id("t.ts").unwrap() as u16, eng.himo_id("t.n").unwrap() as u16);
    let e = eng.entity_in("t").unwrap();
    eng.tie_by_id(e, ts, 7u64);
    eng.tie_by_id(e, n, 7u32);
    let before = eng.fault_count(FaultKind::ValueOutOfRange);
    eng.tie_by_id(e, ts, u64::MAX); // 空の印
    eng.tie_by_id(e, n, u32::MAX); // u32 の列の空の印
    eng.tie_by_id(e, n, u32::MAX as u64 + 5); // u32 の列に大きな値
    eng.tie(e, "t.n", -1i64); // 負の数
    assert_eq!(eng.fault_count(FaultKind::ValueOutOfRange), before + 4);
    assert_eq!(eng.get(e, "t.ts"), Some(7), "拒否した値で cell が変わった");
    assert_eq!(eng.get(e, "t.n"), Some(7), "拒否した値で cell が変わった");
    drop(eng);
    let _ = db_files::remove_db(&p);
}

#[test]
fn reopen_keeps_values_and_only_wide_dbs_become_v11() {
    let (p, q) = (tmp("reopen"), tmp("reopen-narrow"));
    {
        let mut eng = Engine::create_with_capacity(&p, 4096).unwrap();
        eng.define_table("t", 3900).unwrap();
        eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
        eng.flush().unwrap();
        assert_eq!(file_version(&p), 10, "64 bit 列を持つ前は v10 のまま");
        eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
        for (i, &v) in BIG.iter().enumerate() {
            let e = eng.entity_in("t").unwrap();
            eng.tie(e, "t.n", i as u32);
            eng.tie(e, "t.ts", v);
        }
        // 容量の端の entity (reopen 後の領域も 8B 幅で開けているか)
        let far = (0..2600).map(|_| eng.entity_in("t").unwrap()).last().unwrap();
        eng.tie(far, "t.n", 999u32);
        eng.tie(far, "t.ts", u64::MAX - 2);
        eng.flush().unwrap();
        let mut narrow = Engine::create_with_capacity(&q, 4096).unwrap();
        narrow.define_himo("n", ValueType::Number, 0);
        narrow.flush().unwrap();
    }
    assert_eq!(file_version(&p), 11);
    assert_eq!(file_version(&q), 10, "64 bit 列の無い DB まで v11 にしない");
    let eng = Engine::open_standalone(&p).unwrap();
    for (i, &v) in BIG.iter().enumerate() {
        let e = eng.pull_raw("t.n", i as u32)[0];
        assert_eq!(eng.get(e, "t.ts"), Some(v), "reopen 後の値");
        assert_eq!(eng.pull_raw("t.ts", v), vec![e], "reopen 後の索引");
    }
    let far = eng.pull_raw("t.n", 999u32)[0];
    assert_eq!(eng.get(far, "t.ts"), Some(u64::MAX - 2), "reopen 後の容量の端の cell");
    drop(eng);
    let _ = db_files::remove_db(&p);
    let _ = db_files::remove_db(&q);
}

#[test]
fn concurrent_writes_go_through_the_oplog() {
    let p = tmp("concurrent");
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&p, 16 << 20).unwrap();
    let ts = eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    let a = eng.entity().unwrap();
    let b = eng.entity().unwrap();
    eng.tie_to_by_id(a, ts, 1u64 << 50);
    eng.tie_async_by_id(b, ts, (1u64 << 50) + 1);
    eng.flush_writes();
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    assert_eq!(eng.get(a, "ts"), Some(1 << 50));
    assert_eq!(eng.get(b, "ts"), Some((1 << 50) + 1));
    // 監査で読める record は 64 bit のまま
    let vals: Vec<u64> = eng
        .audit(&Default::default())
        .into_iter()
        .filter_map(|r| match r.op {
            enchudb_oplog::oplog::DecodedOp::Tie { himo_id, value, .. } if himo_id == ts => Some(value),
            _ => None,
        })
        .collect();
    assert_eq!(vals, vec![1 << 50, (1 << 50) + 1]);
    drop(eng);
    let _ = db_files::remove_db(&p);
}

/// live は 64 bit 列を条件 / group / 合計 / 並びに使える (段階 D まで断っていた入口が通る)。
#[test]
fn live_accepts_64_bit_columns() {
    let p = tmp("refuse");
    let mut eng = Engine::create_with_capacity(&p, 4096).unwrap();
    eng.define_table("t", 1024).unwrap();
    eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
    let (ts, n) = (eng.himo_id("t.ts").unwrap() as u16, eng.himo_id("t.n").unwrap() as u16);
    let e = eng.entity_in("t").unwrap();
    eng.tie_by_id(e, ts, 1u64 << 40);
    eng.tie_by_id(e, n, 1u32);
    assert!(eng.subscribe(vec![LivePred::Present { himo_id: ts }]).is_ok());
    assert_eq!(eng.find_by(vec![LivePred::Eq { himo_id: ts, value: 1 << 40 }]).unwrap(), vec![e]);
    assert!(eng.subscribe_counts(vec![LivePred::Present { himo_id: n }], vec![], ts).is_ok());
    assert!(eng.subscribe_sums(vec![LivePred::Present { himo_id: n }], vec![], n, ts).is_ok());
    assert!(eng.subscribe_top(vec![LivePred::Present { himo_id: n }], vec![], ts, false, 3).is_ok());
    drop(eng);
    let _ = db_files::remove_db(&p);
}

/// u32 の集計は 64 bit 列の値を上位ごと切り捨てない (黙った誤りでなく panic)。
#[test]
#[should_panic(expected = "64-bit column")]
fn u32_aggregates_panic_on_64_bit_columns() {
    let p = tmp("agg");
    let mut eng = Engine::create_with_capacity(&p, 4096).unwrap();
    eng.define_table("t", 1024).unwrap();
    eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
    let e = eng.entity_in("t").unwrap();
    eng.tie(e, "t.ts", 1u64 << 40);
    let _ = eng.sum("t.ts", &[e]);
}

/// 64 bit 列の segment は 8B 幅で確保し、 reopen でも 8B 幅で開く。 予約は 64 KiB 単位に切り上がり、
/// reopen の予約は既定で大きい (2^28 entity) ので、 差が出るのは 「予約 = 上限」 で上限の半分より先の
/// eid。 4B 幅で確保すると範囲外で panic する。
#[test]
fn wide_segments_are_sized_for_8_byte_cells() {
    let p = tmp("sizing");
    let opts = enchudb_engine::GrowableOptions {
        max_entities: 32_768,
        reserve_entities: Some(32_768),
        ..Default::default()
    };
    let far_v = u64::MAX - 3;
    let far = {
        let mut eng = Engine::create_growable_opts(&p, opts).unwrap();
        eng.define_table("t", 32_000).unwrap();
        eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
        let far = (0..30_000).map(|_| eng.entity_in("t").unwrap()).last().unwrap();
        assert!(enchudb_oplog::eid_local(far) > 20_000);
        eng.tie(far, "t.ts", far_v);
        assert_eq!(eng.get(far, "t.ts"), Some(far_v));
        eng.flush().unwrap();
        far
    };
    let mut eng = Engine::open_standalone(&p).unwrap();
    assert_eq!(eng.get(far, "t.ts"), Some(far_v), "reopen 後の上限の端の cell");
    eng.tie(far, "t.ts", far_v - 1);
    assert_eq!(eng.get(far, "t.ts"), Some(far_v - 1));
    drop(eng);
    let _ = db_files::remove_db(&p);
}

/// crash 相当 (WAL にだけ TIE64 の record がある) を reopen の recovery が 64 bit のまま body に入れる。
/// 作り方は `recovery_replays_uncommitted_tail.rs` と同じ (WAL を直接叩いて決定的に)。
#[test]
fn recovery_replays_wide_values() {
    use enchudb_oplog::oplog::{Op, OpLog};
    const CAP: usize = 8 * 1024 * 1024;
    let p = tmp("replay");
    let (e, hid) = {
        let mut eng = Engine::create_with_capacity(&p, 256).unwrap();
        eng.define_himo("v", ValueType::Number64, 0);
        let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
        eng.set_peer_id(42);
        let e = eng.entity().unwrap();
        let hid = eng.himo_id("v").unwrap() as u16;
        eng.tie_async(e, "v", 1u64 << 33);
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        (e, hid)
    };
    let big = (1u64 << 60) + 7;
    {
        let wal = OpLog::open(std::path::Path::new(&format!("{p}/oplog"))).unwrap();
        let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), enchudb_oplog::eid_local(e));
        wal.append_at_hlc(Op::Tie { eid: oplog_eid, himo_id: hid, value: big }, enchudb_oplog::Hlc { wall: u64::MAX / 2, logical: 0, peer: 42 })
            .unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(&p, CAP).unwrap();
    assert_eq!(eng.get(e, "v"), Some(big), "recovery が 64 bit の値を入れていない");
    drop(eng);
    let _ = db_files::remove_db(&p);
}

/// 範囲 (`pull_range`) と 64 bit の集計が、 書き換え / 外す / 削除の後でも素朴に数えた結果と一致する。
/// 値は dense (< 2^20) と大きな値 (run) を混ぜ、 範囲は両方をまたぐものも。
#[test]
fn ranges_and_64_bit_aggregates_match_oracle() {
    let p = tmp("range");
    let mut eng = Engine::create_with_capacity(&p, 4096).unwrap();
    eng.define_table("t", 3000).unwrap();
    eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
    let mut x = 0x0bad_5eed_u64;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let pick = |r: u64| -> u64 {
        match r % 4 {
            0 => r % 50,                        // dense
            1 => (1 << 20) + r % 3000,          // dense の境目の先
            2 => (1u64 << 40) + (r >> 8) % 5000, // 大きな値
            _ => u64::MAX - 2 - r % 100,        // 上端
        }
    };
    let es: Vec<u64> = (0..2000).map(|_| eng.entity_in("t").unwrap()).collect();
    let mut cells: Vec<Option<u64>> = vec![None; es.len()];
    let mut ncells: Vec<Option<u32>> = vec![None; es.len()];
    let _ = eng.pull_raw("t.ts", 0u32); // 索引を組んで以後維持させる
    for step in 0..6000 {
        let i = (next() % es.len() as u64) as usize;
        match next() % 6 {
            0 => {
                eng.untie(es[i], "t.ts");
                cells[i] = None;
            }
            _ => {
                let v = pick(next());
                eng.tie(es[i], "t.ts", v);
                cells[i] = Some(v);
                let n = (next() % 1_000_000) as u32 + (1 << 20);
                eng.tie(es[i], "t.n", n); // u32 の列の大きな値 (run)
                ncells[i] = Some(n);
            }
        }
        if step % 200 == 0 {
            let (a, b) = (pick(next()), pick(next()));
            let (lo, hi) = (a.min(b), a.max(b));
            let want: Vec<u64> = es.iter().zip(&cells).filter(|(_, c)| c.is_some_and(|v| lo <= v && v <= hi)).map(|(&e, _)| enchudb_oplog::eid_local(e) as u64).collect();
            assert_eq!(eng.pull_range("t.ts", lo, hi), want, "step {step}: pull_range {lo}..={hi}");
            let (a, b) = ((next() % 1_000_000) as u32 + (1 << 20), (next() % 1_000_000) as u32 + (1 << 20));
            let (nlo, nhi) = (a.min(b), a.max(b));
            let want: Vec<u64> = es.iter().zip(&ncells).filter(|(_, c)| c.is_some_and(|v| nlo <= v && v <= nhi)).map(|(&e, _)| enchudb_oplog::eid_local(e) as u64).collect();
            assert_eq!(eng.pull_range("t.n", nlo, nhi), want, "step {step}: u32 列の pull_range");
            let set: Vec<u64> = es.iter().step_by(3).copied().collect();
            let vals: Vec<u64> = es.iter().zip(&cells).step_by(3).filter_map(|(_, c)| *c).collect();
            assert_eq!(eng.sum64("t.ts", &set), vals.iter().map(|&v| v as u128).sum::<u128>(), "step {step}: sum64");
            assert_eq!(eng.min64("t.ts", &set), vals.iter().copied().min(), "step {step}: min64");
            assert_eq!(eng.max64("t.ts", &set), vals.iter().copied().max(), "step {step}: max64");
            let all: Vec<u64> = cells.iter().filter_map(|c| *c).collect();
            let (n, sum, mn, mx) = eng.stats_range64("t.ts", 0, u32::MAX);
            assert_eq!((n as usize, sum), (all.len(), all.iter().map(|&v| v as u128).sum::<u128>()), "step {step}: stats_range64");
            assert_eq!((mn, mx), (all.iter().copied().min(), all.iter().copied().max()));
        }
    }
    // 1 値だけの範囲 = 等値 (範囲の両端の bucket / run を落とさない)
    let mut present: Vec<u64> = cells.iter().filter_map(|c| *c).collect();
    present.sort_unstable();
    present.dedup();
    for &v in &present {
        let mut eq = eng.pull_raw("t.ts", v);
        eq.sort_unstable();
        assert_eq!(eng.pull_range("t.ts", v, v), eq, "pull_range {v}..={v}");
    }
    // 空の範囲 / 逆順
    assert!(eng.pull_range("t.ts", 10u64, 9u64).is_empty());
    drop(eng);
    let _ = db_files::remove_db(&p);
}

/// 64 bit 列の購読 (等値 / 範囲 / In / 否定 / ref の先の範囲 / 64 bit 列で group の件数 / 64 bit の値の合計 /
/// 64 bit 列で並べた上位 k 件 (昇順・降順・ref の先)) が、 書き換え・外す・削除の中で手で数えた結果と一致する。
/// 最初は u32 に収まる値だけで購読の状態 (値のページ) を作り、 途中から大きな値を混ぜる (ページが 8 B に
/// 作り直される経路)。
#[test]
fn live_queries_on_64_bit_columns_match_oracle() {
    use enchudb_engine::LivePred as P;
    use std::collections::{BTreeMap, BTreeSet};
    let p = tmp("live");
    let mut eng = Engine::create_with_capacity(&p, 4096).unwrap();
    eng.define_table("c", 64).unwrap();
    eng.define_table("u", 2000).unwrap();
    eng.define_himo_in("c", "big", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("u", "ts", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("u", "n", ValueType::Number, 0).unwrap();
    eng.define_ref_in("u", "co", "c").unwrap();
    let h = |name: &str| eng.himo_id(name).unwrap() as u16;
    let (big, ts, n, co) = (h("c.big"), h("u.ts"), h("u.n"), h("u.co"));
    let cos: Vec<u64> = (0..20).map(|_| eng.entity_in("c").unwrap()).collect();
    let users: Vec<u64> = (0..300).map(|_| eng.entity_in("u").unwrap()).collect();
    let mut x = 0x5eed64u64;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let pick = |r: u64, wide: bool| -> u64 {
        if !wide {
            return r % 40;
        }
        match r % 5 {
            0 | 1 => r % 40,
            2 => (1u64 << 40) + (r >> 8) % 30,
            3 => u32::MAX as u64 + (r >> 8) % 5,
            _ => u64::MAX - 2 - (r >> 8) % 10,
        }
    };
    for &c in &cos {
        eng.tie_by_id(c, big, next() % 40);
    }
    for &u in &users {
        eng.tie_by_id(u, ts, next() % 40);
        eng.tie_by_id(u, n, (next() % 6) as u32);
        eng.tie_ref(u, "u.co", cos[(next() % cos.len() as u64) as usize]);
    }
    // 購読 (値は大きな値も含めて先に決める)
    let (v_eq, v_big) = (7u64, (1u64 << 40) + 3);
    let (lo, hi) = (u32::MAX as u64, u64::MAX - 5);
    let present = || P::Present { himo_id: n };
    let names = ["eq small", "eq big", "range wide", "in", "not", "via range", "2 ranges"];
    let preds: Vec<Vec<P>> = vec![
        vec![P::Eq { himo_id: ts, value: v_eq }],
        vec![P::Eq { himo_id: ts, value: v_big }],
        vec![P::Range { himo_id: ts, lo, hi }],
        vec![P::In { himo_id: ts, values: vec![3, u64::MAX - 2, (1 << 40) + 1] }],
        vec![present(), P::Not(Box::new(P::Eq { himo_id: ts, value: v_eq }))],
        // 上端を 2^20 に: 大きな値の会社は後から張る範囲 (late) で初めて範囲に入る
        vec![P::Via { path: vec![co], pred: Box::new(P::Range { himo_id: big, lo: 20, hi: 1 << 20 }) }],
        // 範囲 2 本 (2 本目は穴でない固定の条件)
        vec![
            P::Range { himo_id: ts, lo: 0, hi: 30 },
            P::Via { path: vec![co], pred: Box::new(P::Range { himo_id: big, lo: u32::MAX as u64, hi: u64::MAX - 1 }) },
        ],
    ];
    let qs: Vec<_> = preds.iter().map(|p| eng.subscribe(p.clone()).unwrap()).collect();
    let counts = eng.subscribe_counts(vec![present()], vec![], ts).unwrap();
    let sums = eng.subscribe_sums(vec![present()], vec![], n, ts).unwrap();
    let top_asc = eng.subscribe_top(vec![present()], vec![], ts, false, 7).unwrap();
    let top_desc = eng.subscribe_top(vec![present()], vec![], ts, true, 7).unwrap();
    let top_via = eng.subscribe_top(vec![present()], vec![co], big, true, 5).unwrap();
    let mut seen: Vec<BTreeSet<u64>> = vec![BTreeSet::new(); qs.len()];
    let mut groups: BTreeMap<u64, u64> = BTreeMap::new();
    let mut sum_groups: BTreeMap<u64, (u64, u128)> = BTreeMap::new();
    let mut alive: Vec<u64> = users.clone();
    // 大きな値が入った後から張る範囲の購読 (帯が分かれる時に大きな値の記録も忘れさせるか)
    // (ref の先の範囲か, lo, hi, 購読, 積分)。 ref の先の範囲は会社の節に 「帯に入るか」 の記録が残るので、
    // 帯が分かれた時に大きな値の会社の記録も忘れさせないと取りこぼす
    let mut late: Vec<(bool, u64, u64, enchudb_engine::LiveQuery, BTreeSet<u64>)> = Vec::new();
    for step in 0..1500 {
        let wide = step >= 300;
        let i = (next() % alive.len() as u64) as usize;
        let u = alive[i];
        match next() % 8 {
            0 => eng.untie(u, "u.ts"),
            1 => {
                let c = cos[(next() % cos.len() as u64) as usize];
                eng.tie_by_id(c, big, pick(next(), wide));
            }
            2 => eng.tie_ref(u, "u.co", cos[(next() % cos.len() as u64) as usize]),
            3 => eng.tie_by_id(u, n, (next() % 6) as u32),
            4 if next() % 4 == 0 => {
                // 削除して作り直す
                eng.delete(u);
                let e = eng.entity_in("u").unwrap();
                eng.tie_by_id(e, ts, pick(next(), wide));
                eng.tie_by_id(e, n, (next() % 6) as u32);
                alive[i] = e;
            }
            _ => eng.tie_by_id(u, ts, pick(next(), wide)),
        }
        if step >= 450 && step % 150 == 0 {
            let (a, b) = (pick(next(), true), pick(next(), true));
            let (l, h2) = (a.min(b), a.max(b));
            let via = (step / 150) % 2 == 0;
            let pred = if via {
                P::Via { path: vec![co], pred: Box::new(P::Range { himo_id: big, lo: l, hi: h2 }) }
            } else {
                P::Range { himo_id: ts, lo: l, hi: h2 }
            };
            let q = eng.subscribe(vec![pred]).unwrap();
            late.push((via, l, h2, q, BTreeSet::new()));
        }
        if step % 25 != 0 {
            continue;
        }
        let tsv = |e: u64| eng.get_by_id(e, ts);
        let bigv = |e: u64| eng.get_by_id(e, co).and_then(|c| eng.get_by_id(c, big));
        let has_n = |e: u64| eng.get_by_id(e, n).is_some();
        let set = |f: &dyn Fn(u64) -> bool| alive.iter().copied().filter(|&e| f(e)).collect::<BTreeSet<u64>>();
        let want = [
            set(&|e| tsv(e) == Some(v_eq)),
            set(&|e| tsv(e) == Some(v_big)),
            set(&|e| tsv(e).is_some_and(|v| lo <= v && v <= hi)),
            set(&|e| tsv(e).is_some_and(|v| [3, u64::MAX - 2, (1 << 40) + 1].contains(&v))),
            set(&|e| has_n(e) && tsv(e) != Some(v_eq)),
            set(&|e| bigv(e).is_some_and(|v| (20..=1 << 20).contains(&v))),
            set(&|e| tsv(e).is_some_and(|v| v <= 30) && bigv(e).is_some_and(|v| v >= u32::MAX as u64)),
        ];
        for (k, q) in qs.iter().enumerate() {
            let d = q.poll(&eng);
            for e in d.removed {
                assert!(seen[k].remove(&e), "step {step} [{}] 持っていない row の removed", names[k]);
            }
            for e in d.added {
                assert!(seen[k].insert(e), "step {step} [{}] 持っている row の added", names[k]);
            }
            assert_eq!(seen[k], want[k], "step {step} [{}] 積分 != 手で数えた結果", names[k]);
            assert_eq!(q.count(&eng), want[k].len(), "step {step} [{}] count", names[k]);
        }
        for (via, l, h2, q, seen) in late.iter_mut() {
            let d = q.poll(&eng);
            for e in d.removed {
                assert!(seen.remove(&e));
            }
            for e in d.added {
                assert!(seen.insert(e));
            }
            let (l, h2) = (*l, *h2);
            let key = if *via { &bigv as &dyn Fn(u64) -> Option<u64> } else { &tsv };
            assert_eq!(*seen, set(&|e| key(e).is_some_and(|v| l <= v && v <= h2)), "step {step} [late range via={via} {l}..={h2}]");
        }
        // group の件数 (64 bit 列で group)
        for (v, c) in counts.poll(&eng) {
            if c == 0 { groups.remove(&v); } else { groups.insert(v, c); }
        }
        let mut wg: BTreeMap<u64, u64> = BTreeMap::new();
        for &e in &alive {
            if has_n(e) && let Some(v) = tsv(e) {
                *wg.entry(v).or_default() += 1;
            }
        }
        assert_eq!(groups, wg, "step {step} [counts]");
        // 64 bit の値の合計 (group は u32 の列)
        for (g, a) in sums.poll_sums(&eng) {
            if a.count == 0 { sum_groups.remove(&g); } else { sum_groups.insert(g, (a.count, a.sum)); }
        }
        let mut ws: BTreeMap<u64, (u64, u128)> = BTreeMap::new();
        for &e in &alive {
            if let Some(g) = eng.get_by_id(e, n) {
                let w = ws.entry(g).or_default();
                w.0 += 1;
                w.1 += tsv(e).unwrap_or(0) as u128;
            }
        }
        assert_eq!(sum_groups, ws, "step {step} [sums]");
        // 上位 k 件 (同じ値は eid の昇順)
        let top = |key: &dyn Fn(u64) -> Option<u64>, desc: bool, k: usize| -> Vec<u64> {
            let mut v: Vec<(u64, u64)> = alive
                .iter()
                .copied()
                .filter(|&e| has_n(e))
                .filter_map(|e| key(e).map(|x| (if desc { u64::MAX - x } else { x }, e)))
                .collect();
            v.sort_unstable();
            v.into_iter().take(k).map(|x| x.1).collect()
        };
        assert_eq!(top_asc.ranked(&eng), top(&tsv, false, 7), "step {step} [top asc]");
        assert_eq!(top_desc.ranked(&eng), top(&tsv, true, 7), "step {step} [top desc]");
        assert_eq!(top_via.ranked(&eng), top(&bigv, true, 5), "step {step} [top via]");
    }
    drop((qs, counts, sums, top_asc, top_desc, top_via, late));
    drop(eng);
    let _ = db_files::remove_db(&p);
}
