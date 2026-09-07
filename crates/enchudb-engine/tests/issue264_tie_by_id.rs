//! #264: build phase (`&mut self`) の tie に `*_by_id` が無く、 名前 lookup を避ける手段が
//! API に存在しなかった。 `*_by_id` は `&self` 系 (`tie_to_by_id` / `tie_text_to_by_id` /
//! `tie_ref_to_by_id`) にしか生えておらず、 `&self` 系への移行は代替にならない
//! (release で validate が消える / `himo_is_in_engine_internal_table` を毎 tie 払う)。
//!
//! 報告者の実測では名前 lookup は 72M tie のフルリビルドに対して 0.115 % で、 **性能の
//! 緊急性は無い**。 よって gate は速度ではなく **等価性と契約**:
//!
//! 1. `tie_by_id` / `tie_text_by_id` / `tie_ref_by_id` が名前版と同じ結果を書く
//! 2. sentinel 拒否も名前版と同じ (fault 計上 + cell 不変)。 名前版は **himo を定義する前に**
//!    弾く (拒否された write で himo が生えない)
//! 3. `validate_eid_for_himo` は release でも効く (`&self` 系の debug_assert と違う点)
//! 4. id 版は himo を **定義しない** (未定義 id を渡す使い方をさせない)
use enchudb_engine::{db_files, Engine, FaultKind, ValueType};
use enchudb_oplog::EntityId;

fn tmp(tag: &str) -> String {
    let p = format!("{}/enchudb-issue264-{}-{}", std::env::temp_dir().display(), tag, std::process::id());
    let _ = db_files::remove_db(&p);
    p
}

/// 2 table (`a` / `b`) + Number / Tag / Leaf / Ref の himo を持つ engine。
fn build(path: &str) -> Engine {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("a", 1024).unwrap();
    eng.define_table("b", 1024).unwrap();
    eng.define_himo_in("a", "n", ValueType::Number, 0).unwrap();
    eng.define_himo_in("a", "tag", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("a", "leaf", ValueType::Leaf, 0).unwrap();
    eng.define_ref_in("a", "r", "b").unwrap();
    eng
}

fn hid(eng: &Engine, name: &str) -> u16 {
    eng.himo_id(name).unwrap_or_else(|| panic!("himo '{name}' not defined")) as u16
}

#[test]
fn by_id_matches_the_named_version() {
    let p = tmp("equiv");
    let mut eng = build(&p);
    let (h_n, h_tag, h_leaf, h_r) =
        (hid(&eng, "a.n"), hid(&eng, "a.tag"), hid(&eng, "a.leaf"), hid(&eng, "a.r"));

    let target = eng.entity_in("b").unwrap();
    let named = eng.entity_in("a").unwrap();
    let by_id = eng.entity_in("a").unwrap();

    eng.tie(named, "a.n", 42);
    eng.tie_text(named, "a.tag", "kenning");
    eng.tie_text(named, "a.leaf", "本文はここ");
    eng.tie_ref(named, "a.r", target);

    eng.tie_by_id(by_id, h_n, 42);
    eng.tie_text_by_id(by_id, h_tag, "kenning");
    eng.tie_text_by_id(by_id, h_leaf, "本文はここ");
    eng.tie_ref_by_id(by_id, h_r, target);

    assert_eq!(eng.get(by_id, "a.n"), eng.get(named, "a.n"));
    assert_eq!(eng.get_text_owned(by_id, "a.tag"), eng.get_text_owned(named, "a.tag"));
    assert_eq!(eng.get_text_owned(by_id, "a.leaf"), eng.get_text_owned(named, "a.leaf"));
    assert_eq!(eng.get(by_id, "a.r"), eng.get(named, "a.r"));
    // Tag は dedupe されるので、 2 行が同じ vocab id を指しているのが正しい
    assert_eq!(eng.pull_raw("a.tag", eng.get(named, "a.tag").unwrap()).len(), 2);
    // re-tie (上書き) も等価。 Leaf は旧 offset の free を伴う経路
    eng.tie_text_by_id(by_id, h_leaf, "書き換えた");
    eng.tie_text(named, "a.leaf", "書き換えた");
    assert_eq!(eng.get_text_owned(by_id, "a.leaf"), eng.get_text_owned(named, "a.leaf"));

    drop(eng);
    let _ = db_files::remove_db(&p);
}

#[test]
fn sentinel_is_rejected_the_same_way_and_does_not_define_the_himo() {
    let p = tmp("sentinel");
    let mut eng = build(&p);
    let e = eng.entity_in("a").unwrap();
    let h_n = hid(&eng, "a.n");

    let before = eng.fault_count(FaultKind::ValueOutOfRange);
    let himos_before = eng.himo_count();

    // 名前版: 未定義の himo 名 + sentinel → 拒否され、 **himo は生えない**
    eng.tie(e, "a.brand_new", u32::MAX);
    eng.tie_ref(e, "a.brand_new_ref", u32::MAX as EntityId);
    assert_eq!(eng.himo_count(), himos_before, "拒否された write で himo が定義されている");
    assert!(eng.himo_id("a.brand_new").is_none());

    // id 版: 同じく拒否され、 cell は不変
    eng.tie_by_id(e, h_n, 7);
    eng.tie_by_id(e, h_n, u32::MAX);
    assert_eq!(eng.get(e, "a.n"), Some(7), "sentinel 拒否が cell を壊している");

    assert_eq!(
        eng.fault_count(FaultKind::ValueOutOfRange) - before,
        3,
        "sentinel 拒否は名前版 / id 版とも 1 回ずつ計上される (二重計上も欠落もしない)"
    );

    drop(eng);
    let _ = db_files::remove_db(&p);
}

/// `&self` 系の `*_to_by_id` は `debug_assert` なので release では素通りする。
/// build phase の id 版は **release でも** table extents を守る — これがこの API を
/// `&self` 系で置き換えられない理由。
#[test]
#[should_panic(expected = "FK violation")]
fn ref_target_outside_the_table_still_panics_in_release() {
    let p = tmp("fkviolation");
    let mut eng = build(&p);
    let e = eng.entity_in("a").unwrap();
    let h_r = hid(&eng, "a.r");
    // target は b の extents 外 (a 側の eid)
    eng.tie_ref_by_id(e, h_r, e);
    let _ = db_files::remove_db(&p);
}

#[test]
#[should_panic(expected = "not in himo's table")]
fn eid_outside_the_himo_table_still_panics_in_release() {
    let p = tmp("eidrange");
    let mut eng = build(&p);
    let b_eid = eng.entity_in("b").unwrap();
    let h_n = hid(&eng, "a.n");
    // b の eid を a の himo に張る
    eng.tie_by_id(b_eid, h_n, 1);
    let _ = db_files::remove_db(&p);
}
