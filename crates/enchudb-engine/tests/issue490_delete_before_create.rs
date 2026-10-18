//! #490: **作成より先に届いた Delete** を捨てていた問題。
//!
//! A が作った行 x を B が消し、 3 台目の R が A より先に B から pull すると、 R には x の写像がまだ無い。
//! Delete は himo を持たないので写像を払い出せず、 前は捨てていた。 pull の cursor は Delete を越えるので
//! Delete は二度と届かず、 後から A の作成が届くと x は R でだけ生き返った。
//!
//! 直しは #166 の退避表の流用: 写像の無い Delete の削除版数を `(peer, foreign_local)` に載せ、 作成が届いて
//! slot を払い出す時に書き戻す。 ここでは engine の入口 (`remote_delete_apply_foreign`) を直に叩いて判定の形を
//! 固定する。 sync を通した再現は root の `tests/sync_delete_before_create.rs`。

use enchudb_engine::engine::Engine;
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue490-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(p: &str) {
    let _ = std::fs::remove_dir_all(p); // v10: DB は directory
    for s in ["", ".oplog", ".tables", ".tables.tmp", ".crc", ".lock", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", p, s));
    }
}

fn hlc(w: u64, p: u32) -> Hlc {
    Hlc { wall: w, logical: 0, peer: p }
}

/// 受け手 (peer 2) を作る。 table `t` (slot 16) に列 `age`。
/// 戻り値: (engine, `t.age` の himo id)
fn setup(path: &str) -> (Engine, u16) {
    cleanup(path);
    // #166 の test と同じく、 版数・tombstone を持つ形 (= sync に参加する DB) で作る
    let mut eng = Engine::create_with_cell_version(path, 4096).unwrap();
    eng.define_table("t", 16).unwrap();
    eng.define_himo_in("t", "age", ValueType::Number, 0).unwrap();
    let hid = eng.himo_id("t.age").unwrap() as u16;
    eng.set_peer_id(2);
    (eng, hid)
}

/// A (peer 1) が作った entity の世界番号。
fn of_a(foreign_local: u32) -> enchudb_oplog::EntityId {
    enchudb_oplog::make_eid(1, foreign_local)
}

/// **本題**: 写像の無い Delete を受けた後に届いた、 削除より古い作成が当たらない。
///
/// falsify: `remote_delete_apply_foreign` の写像が無い枝で、 退避表に載せずに `true` を返すと
/// 「削除より古い作成が当たった」 で落ちる (= 直す前の、 捨てていた形)。
#[test]
fn create_older_than_an_early_delete_does_not_land() {
    let path = tmp("older");
    let (eng, hid) = setup(&path);
    let x = of_a(777);

    // B (peer 3) の Delete が、 A の作成より先に届く
    assert!(eng.remote_delete_apply_foreign(x, hlc(2000, 3)), "写像の無い Delete を採らなかった");
    assert!(eng.resolve_remote_eid_existing(x).is_none(), "Delete が写像を作った");

    // A の作成 (削除より古い) が後から届く
    let t = eng.resolve_remote_eid(x, hid).expect("翻訳できない");
    assert!(!eng.remote_tie_apply(t, hid, 111, hlc(1000, 1)), "削除より古い作成が当たった");
    eng.rebuild();
    assert!(eng.pull("t.age", 111).is_empty(), "削除より古い作成で行が生き返った");
    drop(eng);
    cleanup(&path);
}

/// 削除より新しい record は当たる (退避した削除が効きすぎて、 その entity を永久に閉じる形になっていない)。
#[test]
fn record_newer_than_an_early_delete_still_lands() {
    let path = tmp("newer");
    let (eng, hid) = setup(&path);
    let x = of_a(777);

    assert!(eng.remote_delete_apply_foreign(x, hlc(2000, 3)));
    let t = eng.resolve_remote_eid(x, hid).expect("翻訳できない");
    assert!(eng.remote_tie_apply(t, hid, 222, hlc(3000, 4)), "削除より新しい record が当たらない");
    assert_eq!(eng.get(t, "t.age"), Some(222));
    drop(eng);
    cleanup(&path);
}

/// 写像の無い Delete どうしの LWW は、 写像のある Delete と同じ: 載っている削除より **真に古い** 時だけ不採用、
/// 同じ版数の再配送は採る。 書き戻すのは一番新しい削除。
#[test]
fn early_deletes_follow_the_same_lww_as_mapped_deletes() {
    let path = tmp("lww");
    let (eng, hid) = setup(&path);

    let x = of_a(777);
    assert!(eng.remote_delete_apply_foreign(x, hlc(2000, 3)));
    assert!(eng.remote_delete_apply_foreign(x, hlc(2000, 3)), "同じ Delete の再配送を拒んだ");
    assert!(!eng.remote_delete_apply_foreign(x, hlc(1500, 4)), "載っている削除より古い Delete を採った");
    assert!(eng.remote_delete_apply_foreign(x, hlc(2500, 4)));
    // 2000 と 2500 の間の作成も弾かれる (= 書き戻すのは 2500)
    let t = eng.resolve_remote_eid(x, hid).expect("翻訳できない");
    assert!(!eng.remote_tie_apply(t, hid, 111, hlc(2200, 1)), "一番新しい削除が書き戻されていない");

    // 対照: 写像のある entity に同じ並びの Delete を当てると、 同じ答え
    let y = of_a(778);
    let ty = eng.resolve_remote_eid(y, hid).expect("翻訳できない");
    assert!(eng.remote_tie_apply(ty, hid, 5, hlc(1000, 1)));
    assert!(eng.remote_delete_apply_foreign(y, hlc(2000, 3)));
    assert!(eng.remote_delete_apply_foreign(y, hlc(2000, 3)));
    assert!(!eng.remote_delete_apply_foreign(y, hlc(1500, 4)));
    assert!(eng.remote_delete_apply_foreign(y, hlc(2500, 4)));
    assert!(!eng.remote_tie_apply(ty, hid, 6, hlc(2200, 1)));
    drop(eng);
    cleanup(&path);
}

/// 写像があれば、 前と同じく本体まで消す (`remote_delete_apply` と同じ)。
#[test]
fn mapped_entity_is_deleted_as_before() {
    let path = tmp("mapped");
    let (eng, hid) = setup(&path);
    let x = of_a(777);

    let t = eng.resolve_remote_eid(x, hid).expect("翻訳できない");
    assert!(eng.remote_tie_apply(t, hid, 111, hlc(1000, 1)));
    assert!(eng.remote_delete_apply_foreign(x, hlc(2000, 3)));
    eng.rebuild();
    assert_eq!(eng.get(t, "t.age"), None, "写像のある entity が消えていない");
    assert!(eng.pull("t.age", 111).is_empty());
    drop(eng);
    cleanup(&path);
}

/// 退避した削除が **reopen を越える** (`.eidmap` の 「写像を持たない tombstone だけの entry」)。
/// これが無いと、 再起動の後に届いた作成で生き返る。
#[test]
fn early_delete_survives_reopen() {
    let path = tmp("reopen");
    let x = of_a(777);
    {
        let (eng, _hid) = setup(&path);
        assert!(eng.remote_delete_apply_foreign(x, hlc(2000, 3)));
        eng.persist_tables().unwrap(); // .eidmap もここで書かれる
        eng.body_msync().unwrap();
    }

    let eng2 = Engine::open_standalone(&path).unwrap();
    eng2.set_peer_id(2);
    let hid = eng2.himo_id("t.age").unwrap() as u16;
    let t = eng2.resolve_remote_eid(x, hid).expect("翻訳できない");
    assert!(!eng2.remote_tie_apply(t, hid, 111, hlc(1000, 1)), "reopen で削除を忘れ、 古い作成が当たった");
    // 対照: 削除を受けていない別の entity の、 同じ版数の作成は当たる
    let z = of_a(779);
    let tz = eng2.resolve_remote_eid(z, hid).expect("翻訳できない");
    assert!(eng2.remote_tie_apply(tz, hid, 333, hlc(1000, 1)), "削除を受けていない entity の作成が当たらない");
    drop(eng2);
    cleanup(&path);
}
