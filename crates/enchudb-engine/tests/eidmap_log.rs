//! #560: `.eidmap` の追記 (`eidmap.log`)。
//!
//! pull で何か当てるたびの書き出し (`persist_sync_state`) は、 変わった key だけを `eidmap.log` に追記する (前は `eidmap` を全部
//! 書き直していた)。 きれいに閉じずに落ちても、 開き直すと `eidmap` + log から写像と削除の記録が戻る。 きれいに閉じる時は `eidmap`
//! をまとめ直し、 log は空 (header だけ) になる。 落ちた像は、 開いたままの DB の directory を写して作る (写しは閉じる処理を通らない)。

use enchudb_engine::ValueType;
use enchudb_engine::db_files::{EIDMAP, EIDMAP_LOG, path_for};
use enchudb_engine::engine::Engine;
use enchudb_oplog::{Hlc, make_eid};
use std::sync::Arc;

const PEER: u32 = 2;
const AUTHOR: u32 = 1;
const LOG_HEADER: u64 = 24;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-eidmaplog-{tag}-{}-{nanos}", std::process::id())
}

fn hlc(w: u64) -> Hlc {
    Hlc { wall: w, logical: 0, peer: AUTHOR }
}

/// 受け手 (peer 2) を作って concurrent で開き直す。 table `t` に列 `v`。
fn open(path: &str) -> (Arc<Engine>, u16) {
    {
        let mut eng = Engine::create_with_cell_version(path, 4096).unwrap();
        eng.define_table("t", 1000).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.set_peer_id(PEER);
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(path, 4 << 20).unwrap();
    let h = eng.himo_id("t.v").unwrap() as u16;
    (eng, h)
}

/// A の行 `i` を受ける (写像を払い出して値を当てる)。
fn receive(eng: &Engine, h: u16, i: u32, w: u64) {
    let local = eng.resolve_remote_eid(make_eid(AUTHOR, i), h).unwrap();
    assert!(eng.remote_tie_apply(local, h, i + 100, hlc(w)));
}

/// 開いたままの DB の directory を写す (= 閉じる処理を通らない、 落ちた後の像)。
fn copy_dir(from: &str, to: &str) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dst = std::path::Path::new(to).join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(e.path().to_str().unwrap(), dst.to_str().unwrap());
        } else if e.file_name() != "lock" {
            std::fs::copy(e.path(), &dst).unwrap();
        }
    }
}

fn len(path: &str, name: &str) -> u64 {
    std::fs::metadata(path_for(path, name)).map(|m| m.len()).unwrap_or(0)
}

/// 書き出しのたびの追記は、 きれいに閉じずに落ちても開き直すと戻る (写像と、 受けた削除の版数)。
#[test]
fn appended_mappings_survive_an_unclean_close() {
    let (path, image) = (tmp("unclean"), tmp("unclean-image"));
    let (eng, h) = open(&path);
    for i in 0..5 {
        receive(&eng, h, i, 1_000 + i as u64);
    }
    eng.persist_sync_state().unwrap();
    let eidmap_len = len(&path, EIDMAP);
    for i in 5..8 {
        receive(&eng, h, i, 2_000 + i as u64);
    }
    let dead = eng.resolve_remote_eid_existing(make_eid(AUTHOR, 2)).unwrap();
    assert!(eng.remote_delete_apply(dead, hlc(3_000)));
    eng.persist_sync_state().unwrap();
    assert_eq!(len(&path, EIDMAP), eidmap_len, "前提: 2 回目の書き出しは eidmap を書き直さない (追記)");
    assert!(len(&path, EIDMAP_LOG) > LOG_HEADER, "前提: 2 回目の書き出しは eidmap.log に追記した");
    let expect: Vec<_> = (0..8).map(|i| eng.resolve_remote_eid_existing(make_eid(AUTHOR, i))).collect();
    copy_dir(&path, &image);
    drop(eng);

    let back = Engine::open_concurrent_with_oplog(&image, 4 << 20).unwrap();
    for i in 0..8 {
        assert_eq!(back.resolve_remote_eid_existing(make_eid(AUTHOR, i)), expect[i as usize], "A の行 {i} の写像が戻らない");
    }
    assert_eq!(back.tombstone_hlc(dead), hlc(3_000), "受けた削除の版数が戻らない");
    assert_eq!(back.eidmap_disk_diff().unwrap(), None);
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&image);
}

/// きれいに閉じると `eidmap` をまとめ直し、 log は header だけになる (読まない旧 binary も `eidmap` だけで全部ある)。
#[test]
fn a_clean_close_compacts_the_log() {
    let path = tmp("clean");
    let (eng, h) = open(&path);
    receive(&eng, h, 0, 1_000);
    eng.persist_sync_state().unwrap();
    for i in 1..20 {
        receive(&eng, h, i, 1_000 + i as u64);
        eng.persist_sync_state().unwrap();
    }
    assert!(len(&path, EIDMAP_LOG) > LOG_HEADER, "前提: 閉じる前は log に追記がある");
    drop(eng);
    assert_eq!(len(&path, EIDMAP_LOG), LOG_HEADER, "閉じた後の log に record が残った");
    let back = Engine::open_concurrent_with_oplog(&path, 4 << 20).unwrap();
    for i in 0..20 {
        assert!(back.resolve_remote_eid_existing(make_eid(AUTHOR, i)).is_some(), "A の行 {i} の写像が戻らない");
    }
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
}

/// log の終わりが切れていたら (追記の途中で落ちた)、 切れた所から後を捨て、 次の書き出しでまとめ直す (切れた跡の後ろに追記すると、
/// 次に読む時にそこで止まって失う)。
#[test]
fn a_torn_log_tail_is_dropped_and_rewritten() {
    let (path, image) = (tmp("torn"), tmp("torn-image"));
    let (eng, h) = open(&path);
    receive(&eng, h, 0, 1_000);
    eng.persist_sync_state().unwrap();
    receive(&eng, h, 1, 1_001);
    eng.persist_sync_state().unwrap();
    receive(&eng, h, 2, 1_002);
    eng.persist_sync_state().unwrap();
    copy_dir(&path, &image);
    drop(eng);
    // 最後の record (A の行 2) を切る
    let log = path_for(&image, EIDMAP_LOG);
    let n = std::fs::metadata(&log).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&log).unwrap().set_len(n - 5).unwrap();

    let back = Engine::open_concurrent_with_oplog(&image, 4 << 20).unwrap();
    assert!(back.resolve_remote_eid_existing(make_eid(AUTHOR, 1)).is_some(), "切れていない record の写像が戻らない");
    assert!(back.resolve_remote_eid_existing(make_eid(AUTHOR, 2)).is_none(), "前提: 切れた record は読まない");
    let (h2, _) = (back.himo_id("t.v").unwrap() as u16, ());
    receive(&back, h2, 3, 1_003);
    back.persist_sync_state().unwrap();
    assert_eq!(len(&image, EIDMAP_LOG), LOG_HEADER, "切れた log の後ろに追記した (まとめ直していない)");
    assert_eq!(back.eidmap_disk_diff().unwrap(), None);
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&image);
}

/// relay の bootstrap は `eidmap` を log を当てた形で配る (受け手は log を取りに来ない)。 配った `eidmap` だけで開いた受け手に、
/// log にだけある写像と削除の版数も戻る。
#[test]
fn bootstrap_serves_the_eidmap_with_the_log_applied() {
    let (path, image) = (tmp("boot"), tmp("boot-image"));
    let (eng, h) = open(&path);
    receive(&eng, h, 0, 1_000);
    eng.persist_sync_state().unwrap();
    for i in 1..6 {
        receive(&eng, h, i, 1_000 + i as u64);
    }
    let dead = eng.resolve_remote_eid_existing(make_eid(AUTHOR, 1)).unwrap();
    assert!(eng.remote_delete_apply(dead, hlc(3_000)));
    eng.persist_sync_state().unwrap();
    assert!(len(&path, EIDMAP_LOG) > LOG_HEADER, "前提: log に追記がある (まとめ直していない)");
    let expect: Vec<_> = (0..6).map(|i| eng.resolve_remote_eid_existing(make_eid(AUTHOR, i))).collect();

    let served = Engine::eidmap_for_bootstrap(&path).unwrap().expect("eidmap がある");
    copy_dir(&path, &image);
    std::fs::remove_file(path_for(&image, EIDMAP_LOG)).unwrap();
    std::fs::write(path_for(&image, EIDMAP), &served).unwrap();
    let back = Engine::open_concurrent_with_oplog(&image, 4 << 20).unwrap();
    for i in 0..6 {
        assert_eq!(back.resolve_remote_eid_existing(make_eid(AUTHOR, i)), expect[i as usize], "A の行 {i} の写像が配られない");
    }
    assert_eq!(back.tombstone_hlc(dead), hlc(3_000), "受けた削除の版数が配られない");
    drop(back);
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&image);
}

/// 行より先に届いた削除 (写像の無い世界番号宛て、 退避表に載る) も追記に載る: 落ちた後に開き直してから行が届いても、 行は消えたまま
/// (載らないと、 削除より古い行の値が当たって消した行が戻る)。
#[test]
fn a_delete_before_its_row_survives_an_unclean_close() {
    let (path, image) = (tmp("early-delete"), tmp("early-delete-image"));
    let (eng, h) = open(&path);
    receive(&eng, h, 0, 1_000);
    eng.persist_sync_state().unwrap();
    assert!(eng.remote_delete_apply_foreign(make_eid(AUTHOR, 7), hlc(3_000)));
    eng.persist_sync_state().unwrap();
    assert!(len(&path, EIDMAP_LOG) > LOG_HEADER, "前提: 2 回目の書き出しは eidmap.log に追記した");
    copy_dir(&path, &image);
    drop(eng);

    let back = Engine::open_concurrent_with_oplog(&image, 4 << 20).unwrap();
    let h = back.himo_id("t.v").unwrap() as u16;
    let local = back.resolve_remote_eid(make_eid(AUTHOR, 7), h).unwrap();
    assert!(!back.remote_tie_apply(local, h, 107, hlc(2_000)), "削除より古い行の値が当たった (先に届いた削除が戻らない)");
    assert_eq!(back.tombstone_hlc(local), hlc(3_000));
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&image);
}
