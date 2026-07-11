//! issue #54 / #88: vocab orphan 検出 API (`vocab_orphan_stats`) の regression。
//!
//! #88 (0.12.0) で **Leaf は vocab でなく LeafStore に格納**され、 re-tie / remove で
//! slot が回収されるようになった。 そのため旧「Leaf が vocab に orphan を残す」
//! テストは「Leaf は vocab を汚さず LeafStore で footprint が有界化する」検証へ更新。
//! Tag の dedup / orphan 検出は従来通り vocab 側 (`tag_dedup_no_orphan`)。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue54-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn leaf_retie_reclaims() {
    // #88: Leaf は vocab を使わず LeafStore に入り、 同サイズ re-tie で旧 slot を
    // free して再利用する → footprint が re-tie 回数に比例して増えない。
    let path = tmp_path("leaf-retie");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_table("notes", 100).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();

    let eid = eng.entity_in("notes").unwrap();
    eng.tie_text(eid, "notes.body", "aaaaa");
    // Leaf は vocab に一切入らない (旧: ここで vocab_total=1)
    assert_eq!(eng.vocab_orphan_stats().vocab_total, 0, "Leaf は vocab を使わない");
    let fp1 = eng.leaf_footprint().expect("v6 は leaf region あり");
    assert!(fp1 > 0, "leaf footprint should grow after first tie");

    // 同サイズで 50 回 re-tie → 毎回旧 slot を free → 再利用で footprint 不変
    for _ in 0..50 {
        eng.tie_text(eid, "notes.body", "bbbbb");
    }
    let fp2 = eng.leaf_footprint().unwrap();
    assert_eq!(fp1, fp2, "同サイズ re-tie で footprint が増えた = 回収されてない");
    // vocab は依然 clean
    assert_eq!(eng.vocab_orphan_stats().vocab_total, 0);

    cleanup(&path);
}

#[test]
fn leaf_remove_reclaims() {
    // #88: Leaf の untie は LeafStore の slot を free する (旧: vocab に orphan 残置)。
    let path = tmp_path("leaf-remove");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_table("notes", 100).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();

    let eid = eng.entity_in("notes").unwrap();
    eng.tie_text(eid, "notes.body", "hello world");
    assert_eq!(eng.vocab_orphan_stats().vocab_total, 0, "Leaf は vocab を使わない");
    let fp_before = eng.leaf_footprint().unwrap();
    assert!(fp_before > 0);

    // untie → 末尾 slot を free → footprint 後退 (旧: orphan として残置)
    eng.untie(eid, "notes.body");
    let fp_after = eng.leaf_footprint().unwrap();
    assert!(
        fp_after < fp_before,
        "untie で footprint が回収されていない (before={fp_before}, after={fp_after})",
    );

    cleanup(&path);
}

#[test]
fn tag_dedup_no_orphan() {
    let path = tmp_path("tag-dedup");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_table("items", 100).unwrap();
    eng.define_himo_in("items", "kind", ValueType::Tag, 0).unwrap();

    // 同じ tag を 3 個 tie → vocab に 1 vid のみ (= dedup)、 orphan 0
    for _ in 0..3 {
        let eid = eng.entity_in("items").unwrap();
        eng.tie_text(eid, "items.kind", "fruit");
    }
    let s0 = eng.vocab_orphan_stats();
    assert_eq!(s0.vocab_total, 1);
    assert_eq!(s0.live_vids, 1);
    assert_eq!(s0.orphan_vids, 0);

    cleanup(&path);
}

#[test]
fn leaf_churn_footprint_bounded() {
    // #88: Leaf を大量 churn しても vocab は空のまま、 LeafStore footprint は有界。
    // 旧テスト (dead_ratio_helper) は「4 vid 中 3 が vocab orphan」を見ていたが、
    // #88 で Leaf は vocab に載らなくなったので footprint 有界性の検証へ更新。
    let path = tmp_path("ratio");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_table("n", 100).unwrap();
    eng.define_himo_in("n", "v", ValueType::Leaf, 0).unwrap();

    let eid = eng.entity_in("n").unwrap();
    for v in ["a", "b", "c", "d"] {
        eng.tie_text(eid, "n.v", v);
    }
    // vocab は汚れない (旧: 4 vid 中 3 orphan、 dead_ratio 0.75 だった)
    let s = eng.vocab_orphan_stats();
    assert_eq!(s.vocab_total, 0, "Leaf churn は vocab を汚さない");
    assert_eq!(s.orphan_vids, 0);

    // 同サイズで更に churn → footprint 不変 (回収が効いている)
    let fp1 = eng.leaf_footprint().unwrap();
    for _ in 0..20 {
        eng.tie_text(eid, "n.v", "e");
    }
    let fp2 = eng.leaf_footprint().unwrap();
    assert_eq!(fp1, fp2, "churn で footprint が有界化していない");

    cleanup(&path);
}
