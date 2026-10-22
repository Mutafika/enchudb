//! #178: 同じ entity を指す 2 つの local 行を畳む (`Engine::merge_entity_into`)。
//!
//! 2 台が同じ自然キーの行を別々に作ってから束ねると、 束ねる前に出て行った書き込みが相手で別の行として払い出され、
//! 同じ entity が 2 行になる。 ここで固定するのは、 翻訳した行 (src) を自分の行 (dst) に畳んだ時の形:
//!
//! - cell は版数ごとの LWW (src が新しい cell だけ入る、 src の外しも当たる、 Leaf は中身ごと、 版数の無い cell は dst が空の時だけ)
//! - src の世界番号は dst の別名になる (以後その世界番号宛ての record は dst に当たる)。 dst の逆写像 (書き戻しの宛名) は変えない
//! - 別名は開き直しても残る (`eidalias`)。 dst の slot が回る時は別名も外れる (残すと別名宛ての record が新しい住人に入る)
//! - src の削除版数は dst にも当たる。 自分の行 / 同じ行 / 生きていない行 / 別の表の行は畳まない

use enchudb_engine::{Engine, LivePred, ValueType};
use enchudb_oplog::{make_eid, EntityId, Hlc};
use std::sync::Arc;

const CAP: usize = 8 * 1024 * 1024;
const SELF_PEER: u32 = 5;
const OTHER_PEER: u32 = 9;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-merge178-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn make_db(path: &str) -> Arc<Engine> {
    let mut eng = Engine::create_with_cell_version(path, 4096).unwrap();
    eng.define_table("files", 256).unwrap();
    eng.define_table("other", 256).unwrap();
    for (c, t) in [("key", ValueType::Number), ("size", ValueType::Number), ("mtime", ValueType::Number), ("note", ValueType::Number)] {
        eng.define_himo_in("files", c, t, 0).unwrap();
    }
    eng.define_himo_in("files", "name", ValueType::Leaf, 0).unwrap();
    eng.define_himo_in("files", "kind", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("other", "x", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    eng.set_peer_id(SELF_PEER);
    eng
}

fn hid(eng: &Engine, name: &str) -> u16 {
    eng.himo_id(name).unwrap() as u16
}

/// 相手の世界番号 `foreign` の写しの行を払い出す (受けた record の当て方と同じ)。
fn translated_row(eng: &Engine, foreign: EntityId) -> EntityId {
    eng.resolve_remote_eid(foreign, hid(eng, "files.size")).expect("払い出せる")
}

/// 自分の行 (主キーと数列) と、 相手の写しの行 (自分の行より新しい cell・古い cell・外し・Leaf・Tag) を作る。
fn two_rows(eng: &Engine) -> (EntityId, EntityId, EntityId) {
    let dst = eng.entity_in("files").unwrap();
    eng.tie_to(dst, "files.key", 1);
    eng.tie_to(dst, "files.size", 100);
    eng.tie_to(dst, "files.mtime", 5);
    eng.tie_to(dst, "files.note", 1);
    eng.flush_writes();
    // 辞書に語を入れる (相手の Tag の値は受けた時に自分の辞書の番号へ訳される、 ここでは訳した後の番号を当てる)
    let scratch = eng.entity_in("files").unwrap();
    eng.tie_text_to(scratch, "files.kind", "img");
    eng.flush_writes();
    let img = eng.vocab_id("img").unwrap();

    let foreign = make_eid(OTHER_PEER, 77);
    let src = translated_row(eng, foreign);
    let future = |l: u32| Hlc { wall: u64::MAX / 2, logical: l, peer: OTHER_PEER };
    let past = Hlc { wall: 1, logical: 0, peer: OTHER_PEER };
    assert!(eng.remote_tie_apply(src, hid(eng, "files.size"), 200, future(0)));
    assert!(eng.remote_tie_apply(src, hid(eng, "files.mtime"), 6, past));
    assert!(eng.remote_tieleaf_apply(src, hid(eng, "files.name"), b"from-9", future(1)).applied());
    assert!(eng.remote_tie_apply(src, hid(eng, "files.kind"), img, future(2)));
    // note は相手が外した (自分の 1 より新しい外し)。 外しの版数を載せるために、 先に古い値を当ててから外す
    assert!(eng.remote_tie_apply(src, hid(eng, "files.note"), 9, past));
    assert!(eng.remote_untie_apply(src, hid(eng, "files.note"), future(3)));
    (dst, src, foreign)
}

#[test]
fn merging_a_translated_row_moves_newer_cells_and_redirects_its_identity() {
    let path = tmp_path("moves");
    let eng = make_db(&path);
    let (dst, src, foreign) = two_rows(&eng);
    let live = eng.subscribe(vec![LivePred::Eq { himo_id: hid(&eng, "files.size"), value: 200 }]).unwrap();
    assert_eq!(live.poll(&eng).added, vec![src], "前提: 畳む前は相手の写しの行が size 200");

    let moved = eng.merge_entity_into(src, dst).expect("畳める");
    assert_eq!(moved, 4, "当たる cell の数 (size・name・kind・note の外し)");
    assert_eq!(eng.get(dst, "files.size"), Some(200), "相手の新しい値が入らない");
    assert_eq!(eng.get(dst, "files.mtime"), Some(5), "相手の古い値で上書きした");
    assert_eq!(eng.get(dst, "files.note"), None, "相手の新しい外しが当たらない");
    assert_eq!(eng.get(dst, "files.key"), Some(1), "自分の主キーが消えた");
    assert_eq!(eng.get_text_owned(dst, "files.name").as_deref(), Some(&b"from-9"[..]), "Leaf の中身が写らない");
    assert_eq!(eng.get_text_owned(dst, "files.kind").as_deref(), Some(&b"img"[..]), "Tag が写らない");
    for c in ["files.size", "files.mtime", "files.name", "files.kind"] {
        assert_eq!(eng.get(src, c), None, "畳んだ行の {c} が残っている");
    }
    let d = live.poll(&eng);
    assert_eq!((d.removed, d.added), (vec![src], vec![dst]), "購読に畳んだ行の出入りが届かない");

    // 相手の世界番号は dst の別名になる。 逆写像 (dst に書いた record の宛名) は自分の世界番号のまま
    assert_eq!(eng.resolve_remote_eid_existing(foreign), Some(dst), "相手の世界番号が dst に当たらない");
    assert_eq!(eng.resolve_remote_eid(foreign, hid(&eng, "files.size")), Some(dst), "払い出しが新しい行を作った");
    assert!(eng.translated_locals_of(OTHER_PEER).is_empty(), "別名が逆写像 (翻訳先) として数えられた");
    let later = Hlc { wall: u64::MAX / 2, logical: 9, peer: OTHER_PEER };
    assert!(eng.remote_tie_apply(eng.resolve_remote_eid(foreign, hid(&eng, "files.size")).unwrap(), hid(&eng, "files.size"), 300, later));
    assert_eq!(eng.get(dst, "files.size"), Some(300), "別名宛ての record が dst に当たらない");

    // 開き直しても別名は残り、 逆写像にはならない
    eng.persist_tables().unwrap();
    drop(live);
    drop(eng);
    let eng = Engine::open_concurrent_with_oplog(&path, CAP).unwrap();
    eng.set_peer_id(SELF_PEER);
    assert_eq!(eng.resolve_remote_eid_existing(foreign), Some(dst), "開き直すと別名が消えた");
    assert!(eng.translated_locals_of(OTHER_PEER).is_empty(), "開き直すと別名が逆写像になった");
    assert_eq!(eng.get(dst, "files.size"), Some(300));
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// dst を消して slot が回る時、 別名も外れる (残すと別名宛ての record が新しい住人に入る)。 削除の版数は別名の世界番号にも
/// 退避されるので、 削除より古い record は新しい行を作らない。 開き直しても別名は戻らない。
#[test]
fn reusing_the_merged_slot_drops_the_alias() {
    let path = tmp_path("reuse");
    let eng = make_db(&path);
    let (dst, src, foreign) = two_rows(&eng);
    eng.merge_entity_into(src, dst).unwrap();
    eng.persist_tables().unwrap();
    assert!(std::path::Path::new(&path).join("eidalias").exists(), "前提: 別名の sidecar がある");

    eng.delete(dst);
    eng.flush_writes();
    // slot が回るまで払い出す (表の範囲 256 を使い切ると、 空いた slot を数え直して使い回す)
    let mut reused = false;
    for _ in 0..600 {
        let e = eng.entity_in("files").unwrap();
        if enchudb_oplog::eid_local(e) == enchudb_oplog::eid_local(dst) {
            reused = true;
            break;
        }
    }
    assert!(reused, "前提: dst の slot が回る");
    assert_eq!(eng.resolve_remote_eid_existing(foreign), None, "回った slot に別名が残った");
    let old = Hlc { wall: 1, logical: 0, peer: OTHER_PEER };
    let fresh = translated_row(&eng, foreign);
    assert_ne!(enchudb_oplog::eid_local(fresh), enchudb_oplog::eid_local(dst));
    assert!(!eng.remote_tie_apply(fresh, hid(&eng, "files.size"), 1, old), "削除より古い record が新しい行に入った");

    eng.persist_tables().unwrap();
    assert!(!std::path::Path::new(&path).join("eidalias").exists(), "別名が無くなったのに sidecar が残った");
    drop(eng);
    let eng = Engine::open_concurrent_with_oplog(&path, CAP).unwrap();
    assert_ne!(eng.resolve_remote_eid_existing(foreign), Some(dst), "開き直すと回った slot に別名が戻った");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// src の削除版数は dst にも当たる (同じ entity の削除): 削除より古い dst の cell は落ち、 削除の後に書かれた src の cell は入る。
/// 削除優先 (2026-10-21) で消えた行には書けないので、 生きた src に版数が載った形 (前の版の DB の生き残り / 途中で切れた削除の跡) で見る。
#[test]
fn merging_applies_the_src_delete_to_dst() {
    let path = tmp_path("tomb");
    let eng = make_db(&path);
    let dst = eng.entity_in("files").unwrap();
    eng.tie_to(dst, "files.key", 1);
    eng.tie_to(dst, "files.mtime", 5);
    eng.flush_writes();
    let foreign = make_eid(OTHER_PEER, 80);
    let src = translated_row(&eng, foreign);
    let t = |w: u64| Hlc { wall: w, logical: 0, peer: OTHER_PEER };
    assert!(eng.remote_tie_apply(src, hid(&eng, "files.size"), 1, t(u64::MAX / 4)));
    assert!(eng.set_tombstone(src, t(u64::MAX / 3)));
    assert!(eng.remote_tie_apply(src, hid(&eng, "files.size"), 2, t(u64::MAX / 2)));

    eng.merge_entity_into(src, dst).unwrap();
    assert_eq!(eng.get(dst, "files.size"), Some(2), "削除の後の値が入らない");
    assert_eq!(eng.get(dst, "files.mtime"), None, "削除より古い dst の cell が残った");
    assert_eq!(eng.get(dst, "files.key"), None, "削除より古い dst の主キーが残った");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// 版数の無い cell は dst に値が無い時だけ入る (LWW は版数の無い書き込みをいつも通すので、 そのまま当てると dst の新しい値を消す)。
#[test]
fn unversioned_src_cells_only_fill_empty_dst_cells() {
    let path = tmp_path("zero");
    let eng = make_db(&path);
    let dst = eng.entity_in("files").unwrap();
    eng.tie_to(dst, "files.size", 100);
    eng.flush_writes();
    let src = translated_row(&eng, make_eid(OTHER_PEER, 81));
    assert!(eng.remote_tie_apply(src, hid(&eng, "files.size"), 7, Hlc::ZERO));
    assert!(eng.remote_tie_apply(src, hid(&eng, "files.mtime"), 8, Hlc::ZERO));
    eng.merge_entity_into(src, dst).unwrap();
    assert_eq!(eng.get(dst, "files.size"), Some(100), "版数の無い src の値が dst の値を上書きした");
    assert_eq!(eng.get(dst, "files.mtime"), Some(8), "dst に無い cell が入らない");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// 断る形: 自分の行を翻訳した行に (#612: 自分の行どうしは畳める、 `tests/sync_own_rows_fold.rs`)、 同じ行に、 別の表の行に、
/// 生きていない行に。
#[test]
fn merge_refuses_own_into_translated_same_dead_and_cross_table_rows() {
    let path = tmp_path("refuse");
    let eng = make_db(&path);
    let own = eng.entity_in("files").unwrap();
    eng.tie_to(own, "files.size", 1);
    let own2 = eng.entity_in("files").unwrap();
    eng.tie_to(own2, "files.size", 2);
    let x = eng.entity_in("other").unwrap();
    eng.tie_to(x, "other.x", 3);
    eng.flush_writes();
    let src = translated_row(&eng, make_eid(OTHER_PEER, 82));
    assert!(eng.remote_tie_apply(src, hid(&eng, "files.size"), 4, Hlc { wall: 10, logical: 0, peer: OTHER_PEER }));

    assert!(eng.merge_entity_into(own, src).is_err(), "自分の行を翻訳した行に畳んだ (その行に書いた record の宛名が相手の世界番号になる)");
    assert!(eng.merge_entity_into(src, src).is_err(), "同じ行に畳んだ");
    assert!(eng.merge_entity_into(src, x).is_err(), "別の表の行に畳んだ");
    let dead = eng.entity_in("files").unwrap();
    eng.delete(dead);
    eng.flush_writes();
    assert!(eng.merge_entity_into(src, dead).is_err(), "生きていない行に畳んだ");
    // 断った後も src / own はそのまま
    assert_eq!(eng.get(src, "files.size"), Some(4));
    assert_eq!(eng.get(own, "files.size"), Some(1));
    assert_eq!(eng.get(own2, "files.size"), Some(2));
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
