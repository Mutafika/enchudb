//! #166: **slot が再利用されても翻訳写像が古い slot を指したまま残る**問題。
//!
//! # 何が壊れていたか
//!
//! `EidTranslator` は `(author_peer, foreign_local) → local` の写像を持つが、
//! remove API がそもそも無く、 production から一度も消されなかった。 一方 slot は
//! `remote_delete_apply` → `entities.free` で free list に戻り、 `entity_in` の
//! 再利用枝 (`rebuild_free_locals` 経由) が別の entity に払い出す。
//!
//! 結果、 **削除済み foreign entity 宛の record が、 その slot を引き継いだ無関係な
//! entity に書き込まれた** (silent な cross-entity 破壊)。 LWW 判定は普通に通るので
//! どこにも警告が出ない。
//!
//! # 直すときの落とし穴
//!
//! 写像を消すだけでは駄目で、 それだと 「破壊」 が **「削除済み entity の復活」** に
//! 化ける: 写像が無い = 初見扱いなので、 削除より古い record が新しい slot を確保して
//! 適用されてしまう (#140 の再来)。
//!
//! tombstone は slot ではなく **identity に属する事実**なので、 slot を手放す時点で
//! `(peer, foreign_local) → Hlc` に退避し、 同じ identity に新しい slot を払い出す
//! ときに書き戻す。 これで判定経路は `set_cell` 1 本のまま (A-2) で済む。
//!
//! この test file は **その両方**を固定する。

use enchudb_engine::engine::Engine;
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue166-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(p: &str) {
    for s in ["", ".oplog", ".tables", ".tables.tmp", ".crc", ".lock", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", p, s));
    }
}

fn hlc(w: u64, p: u32) -> Hlc {
    Hlc { wall: w, logical: 0, peer: p }
}

/// slot 1 個の table を用意し、 foreign entity を 1 つ翻訳して返す。
/// 戻り値: (engine, himo_id, foreign_eid, translated_local_eid)
fn setup(path: &str) -> (Engine, u16, enchudb_oplog::EntityId, enchudb_oplog::EntityId) {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    // slot 3 個。 X が 1 個使い、 詰め物 2 個で枯渇 → 再利用が起きる。
    // 「再利用後にもう一度翻訳する」 ために詰め物を 1 個消して席を空ける。
    eng.define_table("t", 3).unwrap();
    eng.define_himo_in("t", "age", ValueType::Number, 0).unwrap();
    let hid = eng.himo_id("t.age").unwrap() as u16;
    eng.set_peer_id(2);

    let foreign = enchudb_oplog::make_eid(1, 777);
    let translated = eng.resolve_remote_eid(foreign, hid).expect("翻訳できない");
    assert!(eng.remote_tie_apply(translated, hid, 111, hlc(1000, 1), None));
    assert_eq!(eng.get(translated, "t.age"), Some(111));
    (eng, hid, foreign, translated)
}

/// slot が再利用されるまで `entity_in` を回す。
/// 戻り値: (目的の slot を掴んだ eid, それまでに払い出した詰め物の eid 一覧)。
fn take_over_slot(eng: &Engine, slot: u32) -> (enchudb_oplog::EntityId, Vec<enchudb_oplog::EntityId>) {
    let mut fillers = Vec::new();
    for _ in 0..8 {
        match eng.entity_in("t") {
            Ok(e) if enchudb_oplog::eid_local(e) == slot => return (e, fillers),
            Ok(e) => fillers.push(e),
            Err(e) => panic!("entity_in 失敗: {e} (fillers={})", fillers.len()),
        }
    }
    panic!("前提が崩れた: slot {slot} が再利用されなかった");
}

/// 再翻訳用に席を 1 つ空ける (詰め物を 1 個消す)。
fn free_one_slot(eng: &Engine, fillers: &[enchudb_oplog::EntityId]) {
    let victim = *fillers.first().expect("詰め物が無い = 前提が崩れた");
    eng.delete(victim);
}

/// **本題**: 削除済み foreign entity 宛の record が、 slot を引き継いだ別 entity を
/// 上書きしない。
///
/// falsify: `clear_cell_versions` から `evict_translation_for_reuse` の呼びを外すと、
/// 最後の assert が `Some(999)` で落ちる。
#[test]
fn record_for_deleted_foreign_entity_does_not_hit_the_new_tenant() {
    let path = tmp("no_crosswrite");
    let (eng, hid, foreign, translated) = setup(&path);
    let slot = enchudb_oplog::eid_local(translated);

    // X を remote delete → slot が free list に戻る
    assert!(eng.remote_delete_apply(translated, hlc(2000, 1), None));
    assert_eq!(eng.get(translated, "t.age"), None, "delete が効いていない");

    // 別 entity Y が slot を引き継ぐ
    let (y, fillers) = take_over_slot(&eng, slot);
    eng.tie_to(y, "t.age", 555);
    assert_eq!(eng.get(y, "t.age"), Some(555));
    free_one_slot(&eng, &fillers);

    // 削除済み X 宛の record が再配送される
    if let Some(t) = eng.resolve_remote_eid(foreign, hid) {
        assert_ne!(
            enchudb_oplog::eid_local(t),
            slot,
            "写像が古い slot を指したまま (= Y に書き込む)",
        );
        eng.remote_tie_apply(t, hid, 999, hlc(3000, 1), None);
    }

    assert_eq!(
        eng.get(y, "t.age"),
        Some(555),
        "削除済み foreign entity 宛の record が、 slot を引き継いだ別 entity を上書きした",
    );
    drop(eng);
    cleanup(&path);
}

/// **落とし穴側**: 写像を外したことで、 削除より **古い** record が新しい slot を
/// 確保して復活する、 という形に化けていないこと。
///
/// falsify: `restore_foreign_tombstone` の呼びを外すと、 この test が
/// 「復活した」 で落ちる (`record_for_deleted_...` のほうは通ったままなので、
/// 2 本で初めて両側が固定される)。
#[test]
fn older_record_does_not_resurrect_a_deleted_foreign_entity_after_slot_reuse() {
    let path = tmp("no_resurrect");
    let (eng, hid, foreign, translated) = setup(&path);
    let slot = enchudb_oplog::eid_local(translated);

    assert!(eng.remote_delete_apply(translated, hlc(2000, 1), None));
    let (y, fillers) = take_over_slot(&eng, slot);
    eng.tie_to(y, "t.age", 555);
    free_one_slot(&eng, &fillers);

    // **削除 (2000) より古い** Tie が再配送される
    let t = eng.resolve_remote_eid(foreign, hid).expect("翻訳できない");
    let applied = eng.remote_tie_apply(t, hid, 111, hlc(1500, 1), None);

    assert!(!applied, "削除より古い Tie が適用された (削除済み entity が復活)");
    assert!(
        eng.pull("t.age", 111).is_empty(),
        "削除済み foreign entity が別 slot で復活した — tombstone が identity 側に残っていない",
    );
    // 新しい住人は無傷
    assert_eq!(eng.get(y, "t.age"), Some(555), "新しい住人を巻き込んだ");
    drop(eng);
    cleanup(&path);
}

/// 削除より **新しい** record は、 slot が回った後でも正しく適用される
/// (= tombstone を退避したせいで永久に閉じてしまう、 という過剰防御になっていない)。
#[test]
fn newer_record_still_applies_after_slot_reuse() {
    let path = tmp("newer_ok");
    let (eng, hid, foreign, translated) = setup(&path);
    let slot = enchudb_oplog::eid_local(translated);

    assert!(eng.remote_delete_apply(translated, hlc(2000, 1), None));
    let (y, fillers) = take_over_slot(&eng, slot);
    eng.tie_to(y, "t.age", 555);
    free_one_slot(&eng, &fillers);

    // 削除 (2000) より新しい Tie → LWW 的には採用されるべき
    let t = eng.resolve_remote_eid(foreign, hid).expect("翻訳できない");
    assert!(
        eng.remote_tie_apply(t, hid, 111, hlc(3000, 1), None),
        "削除より新しい Tie が適用されない (退避 tombstone が過剰に効いている)",
    );
    assert_eq!(eng.get(t, "t.age"), Some(111));
    assert_eq!(eng.get(y, "t.age"), Some(555), "新しい住人を巻き込んだ");
    drop(eng);
    cleanup(&path);
}

/// 退避した削除記録が **reopen を跨いで残る** (`.eidmap` v3 の
/// 「写像を持たない entry」)。 これが無いと再起動で忘れて復活する。
#[test]
fn evicted_tombstone_survives_reopen() {
    let path = tmp("reopen");
    let (foreign, hid) = {
        let (eng, hid, foreign, translated) = setup(&path);
        let slot = enchudb_oplog::eid_local(translated);

        assert!(eng.remote_delete_apply(translated, hlc(2000, 1), None));
        let (y, fillers) = take_over_slot(&eng, slot);
        eng.tie_to(y, "t.age", 555);
        free_one_slot(&eng, &fillers);

        eng.persist_tables().unwrap(); // .eidmap もここで書かれる
        eng.body_msync().unwrap();
        (foreign, hid)
    };

    let eng2 = Engine::open_standalone(&path).unwrap();
    eng2.set_peer_id(2);

    // 削除より古い Tie。 reopen で忘れていれば通ってしまう
    let t = eng2.resolve_remote_eid(foreign, hid).expect("翻訳できない");
    let applied = eng2.remote_tie_apply(t, hid, 111, hlc(1500, 1), None);
    eng2.rebuild();

    assert!(!applied, "reopen 後に削除より古い Tie が適用された");
    assert!(
        eng2.pull("t.age", 111).is_empty(),
        "reopen で退避 tombstone を忘れ、 削除済み entity が復活した",
    );
    drop(eng2);
    cleanup(&path);
}

/// slot 再利用が **deadlock しない**。
///
/// `get_or_insert_with` は以前 translator の write lock を保持したまま alloc を
/// 呼んでいたので、 alloc 側 (`entity_in` → slot 再利用 → `remove_local`) が同じ
/// lock を取ると self-deadlock する。 この test は 「再利用 slot を掴む払い出しを
/// 翻訳経路から行う」 = まさにその経路を踏む。
#[test]
fn translating_into_a_reused_slot_does_not_deadlock() {
    let path = tmp("deadlock");
    let (eng, hid, _foreign, translated) = setup(&path);

    // 1 個目の foreign を消して slot を空ける
    assert!(eng.remote_delete_apply(translated, hlc(2000, 1), None));

    // 別 peer の foreign entity を翻訳する = alloc 経路で free slot を掴む。
    // ここで固まるなら deadlock (test harness の timeout で落ちる)。
    for i in 0..4u32 {
        let f = enchudb_oplog::make_eid(3, 500 + i);
        if let Some(t) = eng.resolve_remote_eid(f, hid) {
            eng.remote_tie_apply(t, hid, 700 + i, hlc(4000 + i as u64, 3), None);
        }
    }
    drop(eng);
    cleanup(&path);
}
