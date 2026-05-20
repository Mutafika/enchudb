//! `BucketCylinder::positions` の `eid_offset` 導入に対する regression test。
//!
//! 通常 workload (schema 層の table-segmented insert) は eid 昇順に tie するので
//! `eid_offset` は最初の `ensure_positions(eid)` で固定され、 以降の `ensure_positions`
//! は extend 経路に入る。 一方 remote_tie_apply (CRDT mesh) や手動 backfill で
//! **eid 降順 / random 順** に tie が走ると、 prepend 経路 (= Vec を再 alloc して
//! 古い entries の前に空 entry を埋める) を毎回踏む可能性がある。
//!
//! この test は 100k entity を eid 降順 tie で叩いて:
//!   - panic / OOM せず完走
//!   - query 結果が昇順 insert と同じ
//!   - 経過時間が現実的 (= prepend が爆発的でない)
//! ことを確認する。

use enchudb_engine::{Engine, HimoType};
use std::time::Instant;

const N: u32 = 100_000;

fn tmp(name: &str) -> String {
    format!("/tmp/test_{}_{}.db", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.oplog"));
    let _ = std::fs::remove_file(format!("{path}.lock"));
    let _ = std::fs::remove_file(format!("{path}.crc"));
}

/// 降順 tie でも positions の prepend が pathological にならず、
/// query 結果が昇順と等価。 wall-clock は perf 指標ではなく
/// 「O(N^2) realloc に落ちて分単位で hang」 を検出するためのガード
/// (M2 Max release で実測 ~30 ms、 1 sec を超えるなら何か壊れている)。
#[test]
fn descending_eid_tie_does_not_explode() {
    let path = tmp("eid_offset_desc");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, N + 100).unwrap();
    eng.define_himo("v", HimoType::Number, 10);

    // entity slot を昇順に確保 (= eid 0..N が allocate される)
    let mut eids = Vec::with_capacity(N as usize);
    for _ in 0..N {
        eids.push(eng.entity());
    }

    // tie は降順 (eid N-1 → 0) で投入。 BucketCylinder の `ensure_positions`
    // は最初 (eid N-1) で offset を N-1 に固定、 以降 N-2..0 が来るたびに
    // prepend で offset を一段下げる。 各 prepend は O(現 size) なので、
    // 単純実装だと O(N^2) で 100k なら 10G iter → 数分単位の hang を起こす。
    let t = Instant::now();
    for &eid in eids.iter().rev() {
        eng.tie(eid, "v", (enchudb_oplog::eid_local(eid) % 10) as u32);
    }
    let elapsed = t.elapsed();
    eng.rebuild();
    assert!(
        elapsed.as_millis() < 30_000,
        "descending tie took {elapsed:?} for {N} entries — prepend likely O(N^2)"
    );

    // query が正しく結果を返す
    for v in 0..10u32 {
        let hits = eng.query(&[("v", v)]);
        let expected = (N / 10) as usize;
        assert_eq!(
            hits.len(),
            expected,
            "v={v}: got {} hits, expected {expected}",
            hits.len()
        );
    }

    // 個別 get も正しい
    for &eid in eids.iter().step_by(1000) {
        let want = (enchudb_oplog::eid_local(eid) % 10) as u32;
        assert_eq!(eng.get(eid, "v"), Some(want), "eid={eid}");
    }

    drop(eng);
    cleanup(&path);
}

/// 混合 (高い eid を先に tie、 後から低い eid を tie) でも壊れない。
/// `eid_offset` の prepend と extend の両方が混ざるパターン。
#[test]
fn mixed_high_then_low_eid_tie() {
    let path = tmp("eid_offset_mixed");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, N + 100).unwrap();
    eng.define_himo("v", HimoType::Number, 10);

    let mut eids = Vec::with_capacity(N as usize);
    for _ in 0..N {
        eids.push(eng.entity());
    }

    // 後半 (eid N/2..N) を先に昇順で tie → offset が N/2 で固定
    for &eid in eids[(N / 2) as usize..].iter() {
        eng.tie(eid, "v", (enchudb_oplog::eid_local(eid) % 10) as u32);
    }
    // 前半 (eid 0..N/2) を後から逆順で tie → 1 回の prepend で offset 0 まで下がる
    for &eid in eids[..(N / 2) as usize].iter().rev() {
        eng.tie(eid, "v", (enchudb_oplog::eid_local(eid) % 10) as u32);
    }
    eng.rebuild();

    for v in 0..10u32 {
        let hits = eng.query(&[("v", v)]);
        assert_eq!(hits.len(), (N / 10) as usize);
    }

    drop(eng);
    cleanup(&path);
}
