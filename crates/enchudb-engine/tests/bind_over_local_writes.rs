//! #178 の**検知**: 「自分が書いた行が、 後から foreign identity に束ねられた」 を数える。
//!
//! 書き戻しの宛名付け替えは bridge 時に `eid_translator.reverse()` を引く。 つまり
//! **束ねられる前に書いた分は自分の eid のまま出て行く**。 受け側はそれを既存行に
//! 結び付ける手段が無い (`bind_by_primary_key` は PK Tie が同 batch に居る時だけ効く)
//! ので、 **PK を持たない重複行**を払い出す。 その行は代表 column (= PK) を持たないため
//! `Table::all()` の母集団にも入らず、 アプリの監査からも見えない。
//!
//! 実地 (syncretic の chaos soak、 rev 8c9fbf4) の seed=1 で両側に 1 件ずつ出た。
//! 直し方は 3 案あって選定にはデータが要るので、 **まず観測できるようにする**。
//! ここで固定するのは 「出たら数える」 「常態では数えない」 の 2 点。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::Hlc;
use std::sync::Arc;

const CAP: usize = 8 * 1024 * 1024;
const SELF_PEER: u32 = 5;
const OTHER_PEER: u32 = 9;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-bind-over-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn make_db(path: &str) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("files", 256).unwrap();
    eng.define_himo_in("files", "key", ValueType::Number, 0).unwrap();
    eng.define_himo_in("files", "size", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    eng.set_peer_id(SELF_PEER);
    eng
}

#[test]
fn binding_over_a_row_this_peer_wrote_is_counted() {
    let path = tmp_path("counted");
    cleanup(&path);

    let eng = make_db(&path);
    assert_eq!(eng.bind_over_local_writes(), 0, "初期値は 0");

    // 自分で行を作って書く (= まだ誰の identity にも束ねられていない)。
    let row = eng.entity_in("files").expect("row");
    eng.tie_to(row, "files.key", 1);
    eng.tie_to(row, "files.size", 100);
    eng.flush_writes();

    // 後から相手の同じ PK の record が届き、 PK bind でこの行に束ねられる。
    let foreign = enchudb_oplog::make_eid(OTHER_PEER, 77);
    eng.bind_remote_eid(OTHER_PEER, foreign, row);

    assert_eq!(
        eng.bind_over_local_writes(),
        1,
        "bind 前に自分が書いていた行なのに数えていない \
         (相手側に PK 無しの重複行が生えている可能性を見逃す)"
    );
    assert_eq!(eng.stats().bind_over_local_writes, 1, "stats に出ていない");

    cleanup(&path);
}

#[test]
fn binding_over_an_untouched_row_is_not_counted() {
    let path = tmp_path("clean");
    cleanup(&path);

    let eng = make_db(&path);

    // 空の slot に束ねる (= 通常の翻訳先払い出しと同じ形)。
    let row = eng.entity_in("files").expect("row");
    let foreign = enchudb_oplog::make_eid(OTHER_PEER, 78);
    eng.bind_remote_eid(OTHER_PEER, foreign, row);
    assert_eq!(
        eng.bind_over_local_writes(),
        0,
        "自分が書いていない行への bind まで数えている (常態で警報が鳴る)"
    );

    // 相手が著者の cell だけが載っている行への bind も常態 (再 bind / slot 回し)。
    let recv = eng.entity_in("files").expect("row2");
    assert!(eng.remote_tie_apply(
        recv,
        eng.himo_id("files.size").unwrap() as u16,
        7,
        Hlc { wall: 100, logical: 0, peer: OTHER_PEER }));
    let foreign2 = enchudb_oplog::make_eid(OTHER_PEER, 79);
    eng.bind_remote_eid(OTHER_PEER, foreign2, recv);
    assert_eq!(
        eng.bind_over_local_writes(),
        0,
        "相手が著者の cell しか無い行への bind を数えている"
    );

    cleanup(&path);
}
