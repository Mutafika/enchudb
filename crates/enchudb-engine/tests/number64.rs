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
        assert_eq!(eng.get64(e, "t.ts"), Some(v), "get64");
        assert_eq!(eng.get_by_id64(e, ts), Some(v), "get_by_id64");
        // u32 の get は収まる時だけ (切り詰めない)
        assert_eq!(eng.get(e, "t.ts"), u32::try_from(v).ok(), "get (u32)");
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
    assert_eq!(eng.get64(far, "t.ts"), Some(u64::MAX - 2), "容量の端の cell");
    // 書き換え / 外す / 削除
    eng.tie_by_id(es[4], ts, 1u64 << 41);
    assert!(eng.pull_raw("t.ts", 1u64 << 40).is_empty(), "旧値の bucket に残っている");
    assert_eq!(eng.pull_raw("t.ts", 1u64 << 41), vec![enchudb_oplog::eid_local(es[4]) as u64]);
    eng.untie(es[5], "t.ts");
    assert_eq!(eng.get64(es[5], "t.ts"), None);
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
    assert_eq!(eng.get64(e, "t.ts"), Some(7), "拒否した値で cell が変わった");
    assert_eq!(eng.get64(e, "t.n"), Some(7), "拒否した値で cell が変わった");
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
        assert_eq!(eng.get64(e, "t.ts"), Some(v), "reopen 後の値");
        assert_eq!(eng.pull_raw("t.ts", v), vec![e], "reopen 後の索引");
    }
    let far = eng.pull_raw("t.n", 999u32)[0];
    assert_eq!(eng.get64(far, "t.ts"), Some(u64::MAX - 2), "reopen 後の容量の端の cell");
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
    assert_eq!(eng.get64(a, "ts"), Some(1 << 50));
    assert_eq!(eng.get64(b, "ts"), Some((1 << 50) + 1));
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

#[test]
fn unsupported_paths_refuse_64_bit_columns() {
    let p = tmp("refuse");
    let mut eng = Engine::create_with_capacity(&p, 4096).unwrap();
    eng.define_table("t", 1024).unwrap();
    eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
    let (ts, n) = (eng.himo_id("t.ts").unwrap() as u16, eng.himo_id("t.n").unwrap() as u16);
    let e = eng.entity_in("t").unwrap();
    eng.tie_by_id(e, ts, 1u64 << 40);
    eng.tie_by_id(e, n, 1u32);
    // live (段階 D まで): 購読 / find_by / group / sum / 並び
    assert!(eng.subscribe(vec![LivePred::Present { himo_id: ts }]).is_err());
    assert!(eng.find_by(vec![LivePred::Eq { himo_id: ts, value: 1 }]).is_err());
    assert!(eng.subscribe_counts(vec![LivePred::Present { himo_id: n }], vec![], ts).is_err());
    assert!(eng.subscribe_sums(vec![LivePred::Present { himo_id: n }], vec![], n, ts).is_err());
    assert!(eng.subscribe_top(vec![LivePred::Present { himo_id: n }], vec![], ts, false, 3).is_err());
    // u32 の列の購読は従来どおり
    assert!(eng.subscribe(vec![LivePred::Present { himo_id: n }]).is_ok());
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
        assert_eq!(eng.get64(far, "t.ts"), Some(far_v));
        eng.flush().unwrap();
        far
    };
    let mut eng = Engine::open_standalone(&p).unwrap();
    assert_eq!(eng.get64(far, "t.ts"), Some(far_v), "reopen 後の上限の端の cell");
    eng.tie(far, "t.ts", far_v - 1);
    assert_eq!(eng.get64(far, "t.ts"), Some(far_v - 1));
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
    assert_eq!(eng.get64(e, "v"), Some(big), "recovery が 64 bit の値を入れていない");
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
