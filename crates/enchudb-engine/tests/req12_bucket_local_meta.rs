//! request12 P1: bucket ローカルメタデータ (per-bucket verify flag + live counter)。
//!
//! 検証点 (engine 公開 API 越し):
//! - `himo_cardinality` が churn 後も正確 (旧実装は「一度でも触った bucket 数」の
//!   over-count — このテストは master では落ちる = 修正の falsification)
//! - churn した bucket の pull は verify + dedup で正しく、無傷 bucket の pull も正しい
//! - untie / re-tie 往復 (dup slot) でも件数が壊れない

use enchudb_engine::{Engine, ValueType};

fn fresh(path: &str) {
    for suf in ["", ".oplog", ".lock", ".tables", ".crc"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// churn で値が移動しても cardinality は「live な値の数」を返す。
#[test]
fn cardinality_stays_exact_under_churn() {
    let path = "/tmp/test_req12_cardinality.db";
    fresh(path);
    let mut eng = Engine::create_standalone(path).expect("create");
    eng.define_himo("s", ValueType::Number, 10);

    let eids: Vec<_> = (0..5).map(|_| eng.entity()).collect();
    for &e in &eids {
        eng.tie(e, "s", 0);
    }
    assert_eq!(eng.himo_cardinality("s"), Some(1));

    // 全員 v0 → v1 へ移動。live な値は 1 個のまま。
    // (旧実装は v0/v1 両方を数えて 2 を返す = ここで落ちる)
    for &e in &eids {
        eng.tie(e, "s", 1);
    }
    assert_eq!(
        eng.himo_cardinality("s"),
        Some(1),
        "churn 後の cardinality が over-count (live 基準になっていない)"
    );
    assert_eq!(eng.pull("s", 0).len(), 0);
    assert_eq!(eng.pull("s", 1).len(), 5);

    // 1 匹だけ v2 へ → live な値は 2 個
    eng.tie(eids[0], "s", 2);
    assert_eq!(eng.himo_cardinality("s"), Some(2));

    drop(eng);
    fresh(path);
}

/// untie で live が減り、全滅した値は cardinality から消える。
#[test]
fn untie_updates_counts() {
    let path = "/tmp/test_req12_untie.db";
    fresh(path);
    let mut eng = Engine::create_standalone(path).expect("create");
    eng.define_himo("s", ValueType::Number, 10);

    let eids: Vec<_> = (0..3).map(|_| eng.entity()).collect();
    for &e in &eids {
        eng.tie(e, "s", 5);
    }
    assert_eq!(eng.pull("s", 5).len(), 3);
    assert_eq!(eng.himo_cardinality("s"), Some(1));

    eng.untie(eids[0], "s");
    assert_eq!(eng.pull("s", 5).len(), 2, "untie した eid が pull に残っている");
    assert_eq!(eng.himo_cardinality("s"), Some(1));

    eng.untie(eids[1], "s");
    eng.untie(eids[2], "s");
    assert_eq!(eng.pull("s", 5).len(), 0);
    assert_eq!(
        eng.himo_cardinality("s"),
        Some(0),
        "全滅した値が cardinality に残っている"
    );

    drop(eng);
    fresh(path);
}

/// v0 → v1 → v0 の往復 re-tie: v0 bucket に同一 eid の slot が 2 本入るが、
/// verify + dedup で 1 件に潰れる。件数系も壊れない。
#[test]
fn retie_roundtrip_dedups() {
    let path = "/tmp/test_req12_roundtrip.db";
    fresh(path);
    let mut eng = Engine::create_standalone(path).expect("create");
    eng.define_himo("s", ValueType::Number, 10);

    let a = eng.entity();
    let b = eng.entity();
    eng.tie(a, "s", 0);
    eng.tie(b, "s", 0);

    // a を v0 → v1 → v0 と往復
    eng.tie(a, "s", 1);
    eng.tie(a, "s", 0);

    let mut got = eng.pull("s", 0);
    got.sort_unstable();
    let mut want = vec![
        enchudb_oplog::eid_local(a),
        enchudb_oplog::eid_local(b),
    ];
    want.sort_unstable();
    assert_eq!(got, want, "往復 re-tie の dup が dedup されていない");
    assert_eq!(eng.pull("s", 1).len(), 0);
    assert_eq!(eng.himo_cardinality("s"), Some(1));

    drop(eng);
    fresh(path);
}

/// 無傷の値の pull は churn の影響を受けず正しいまま (fast path の正しさ)。
#[test]
fn untouched_value_pull_correct_after_churn() {
    let path = "/tmp/test_req12_untouched.db";
    fresh(path);
    let mut eng = Engine::create_standalone(path).expect("create");
    eng.define_himo("s", ValueType::Number, 10);

    let mut v7 = Vec::new();
    for i in 0..30u32 {
        let e = eng.entity();
        eng.tie(e, "s", i % 3); // v0/v1/v2
        if i % 3 == 0 {
            // 後で v7 に固定する組
        }
        if i < 10 {
            let e2 = eng.entity();
            eng.tie(e2, "s", 7);
            v7.push(enchudb_oplog::eid_local(e2));
        }
    }
    // v0 ⇔ v1 で churn を大量に起こす (v7 は無傷)
    let movers: Vec<_> = eng.pull("s", 0);
    for round in 0..10u32 {
        let to = if round % 2 == 0 { 1 } else { 0 };
        for &lid in &movers {
            // pull は local id を返すので eid に戻す (peer 0)
            eng.tie(lid as u64, "s", to);
        }
    }

    let mut got = eng.pull("s", 7);
    got.sort_unstable();
    v7.sort_unstable();
    assert_eq!(got, v7, "無傷 bucket の pull が churn で壊れた");

    drop(eng);
    fresh(path);
}
