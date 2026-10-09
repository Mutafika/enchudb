//! #414: 書き換え / untie / delete で cell が指さなくなった Leaf の slot は、 cell の付け替えがディスクに書き出されて
//! から空きに戻す。 すぐ戻すと、 使い回した中身が付け替えより先にディスクに届き、 電源断の後に書き出しの返った値が
//! 空 / 別の行の値になる (電源断の像で確かめるのは `tests/power_loss.rs` の `power_loss_keeps_rewritten_leaf_values`)。
//!
//! ここは空きに戻る契機を、 Leaf 領域の high water (`leaf_footprint`) で見る: 旧 slot が空きに戻っていれば、 同じ
//! 大きさの次の値はそこに入って high water が動かない。

use enchudb_engine::{Engine, ValueType};

fn tmp(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("issue414_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p.to_str().unwrap().to_string()
}

fn standalone(tag: &str) -> (String, Engine) {
    let path = tmp(tag);
    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    (path, eng)
}

fn fp(eng: &Engine) -> u64 {
    eng.leaf_footprint().unwrap()
}

/// 書き換えの旧 slot: flush の前は使い回さない (同じ大きさの値を別の行に書くと high water が伸びる)、 flush の後は使い回す。
#[test]
fn rewritten_slot_is_reused_only_after_flush() {
    let (path, mut eng) = standalone("rewrite");
    let a = eng.entity().unwrap();
    let tail = eng.entity().unwrap();
    eng.tie_text(a, "body", "aaaaaaaa");
    eng.tie_text(tail, "body", "keeps the high water above a");
    eng.flush().unwrap();

    eng.tie_text(a, "body", "a value that does not fit the old slot of a");
    let c = eng.entity().unwrap();
    let before = fp(&eng);
    eng.tie_text(c, "body", "cccccccc");
    assert!(fp(&eng) > before, "flush の前に旧 slot を使い回した");

    eng.flush().unwrap();
    let d = eng.entity().unwrap();
    let before = fp(&eng);
    eng.tie_text(d, "body", "dddddddd");
    assert_eq!(fp(&eng), before, "flush の後も旧 slot が空きに戻らない");

    for (e, want) in [(a, "a value that does not fit the old slot of a"), (c, "cccccccc"), (d, "dddddddd")] {
        assert_eq!(eng.get_text_owned(e, "body").as_deref(), Some(want.as_bytes()));
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// untie / delete の slot も同じ。
#[test]
fn untied_and_deleted_slots_are_reused_only_after_flush() {
    for delete in [false, true] {
        let (path, mut eng) = standalone(if delete { "delete" } else { "untie" });
        let a = eng.entity().unwrap();
        let tail = eng.entity().unwrap();
        eng.tie_text(a, "body", "aaaaaaaa");
        eng.tie_text(tail, "body", "keeps the high water above a");
        eng.flush().unwrap();

        if delete {
            eng.delete(a);
        } else {
            eng.untie(a, "body");
        }
        let c = eng.entity().unwrap();
        let before = fp(&eng);
        eng.tie_text(c, "body", "cccccccc");
        assert!(fp(&eng) > before, "delete={delete}: flush の前に旧 slot を使い回した");

        eng.flush().unwrap();
        let d = eng.entity().unwrap();
        let before = fp(&eng);
        eng.tie_text(d, "body", "dddddddd");
        assert_eq!(fp(&eng), before, "delete={delete}: flush の後も旧 slot が空きに戻らない");
        drop(eng);
        let _ = std::fs::remove_dir_all(&path);
    }
}

/// consumer の無い書き手が flush せずに書き換え続けても、 待っている旧 slot が目安 (4 MiB と Leaf 領域の 1/4 の大きい方)
/// を越えたら書き手が自分で本体を書き出して空きに戻す。 Leaf 領域は書き換えの量 (20 MiB) に比例して伸びない。
#[test]
fn rewrites_without_flush_stay_bounded() {
    let (path, mut eng) = standalone("bounded");
    let e = eng.entity().unwrap();
    for i in 0..20_000u32 {
        eng.tie_text(e, "body", &format!("{i:0>1024}"));
    }
    let fp = fp(&eng);
    assert!(fp < 8 << 20, "書き換えの旧 slot が空きに戻らず Leaf 領域が伸びた: {fp} B");
    assert_eq!(eng.get_text_owned(e, "body"), Some(format!("{:0>1024}", 19_999).into_bytes()));
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// concurrent + oplog: `oplog_sync` (と consumer の周期の書き出し) の後は旧 slot を使い回す。
#[test]
fn oplog_sync_releases_old_slots() {
    let (path, eng) = standalone("oplog");
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let e = eng.entity().unwrap();
    for i in 0..2_000u32 {
        eng.tie_text_to(e, "body", &format!("{i:0>1024}"));
        eng.oplog_sync().unwrap();
    }
    let fp = fp(&eng);
    assert!(fp < 64 << 10, "oplog_sync の後も旧 slot が空きに戻らない: {fp} B");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// Memory backing (`from_bytes`) はディスクに書き出さない (待つ契機も来ない) ので、 旧 slot をすぐ空きに戻す。
#[test]
fn memory_backing_reuses_at_once() {
    let (path, mut eng) = standalone("memory");
    let e = eng.entity().unwrap();
    eng.tie_text(e, "body", "first");
    eng.flush().unwrap();
    drop(eng);
    let packed = std::path::PathBuf::from(format!("{path}.packed"));
    Engine::pack_dir(&path, &packed).unwrap();
    let mut mem = Engine::from_bytes(std::fs::read(&packed).unwrap()).unwrap();
    mem.tie_text(e, "body", "second");
    let before = fp(&mem);
    for _ in 0..100 {
        mem.tie_text(e, "body", "third!");
    }
    assert!(fp(&mem) <= before + 64, "Memory backing で旧 slot が空きに戻らない");
    drop(mem);
    let _ = std::fs::remove_file(&packed);
    let _ = std::fs::remove_dir_all(&path);
}

/// 開き直すと、 書き出しを待っていた slot も空きになる (待ちは永続化しない、 open の空きは live の slot の隙間)。
#[test]
fn waiting_slots_become_free_on_reopen() {
    let (path, mut eng) = standalone("reopen");
    let a = eng.entity().unwrap();
    let tail = eng.entity().unwrap();
    eng.tie_text(a, "body", "aaaaaaaa");
    eng.tie_text(tail, "body", "keeps the high water above a");
    eng.flush().unwrap();
    eng.untie(a, "body");
    drop(eng); // drop の flush で空きに戻っていても、 戻っていなくても、 開き直した後は空き
    let mut eng = Engine::open_standalone(&path).unwrap();
    let c = eng.entity().unwrap();
    let before = fp(&eng);
    eng.tie_text(c, "body", "cccccccc");
    assert_eq!(fp(&eng), before, "開き直した後に旧 slot が空きになっていない");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
