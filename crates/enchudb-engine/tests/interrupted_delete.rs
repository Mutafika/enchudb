//! **削除は「途中で止まったまま」で固まってはいけない。**
//!
//! `Engine` の delete 3 経路 (`delete` / `remote_delete_apply` / WAL replay) は
//! いずれも **(1) tombstone 版数を書く → (2) 全 himo の cell を落とす →
//! (3) live 登録を外す** の順で流す。 (1) と (2) の間で殺されると
//! 「tombstone は在るが行は生きている」 が残る。
//!
//! query 経路 (`entities_with_himo` 等) は tombstone を見ないので、 この行は
//! **アプリからは生きて見える** — 消したはずのファイルが復活して見える形になる。
//! しかも旧実装は判定と適用を bool 1 本に潰していたため、 同じ Delete が
//! 再配送 / replay されても `set_tombstone_local` が同値を弾いて `false` を返し、
//! **本体除去に到達しない = 二度と直らない**。
//!
//! 実地 (syncretic の chaos soak、 SIGKILL 混じり): 保全した peer store 3/3 に
//! 1 件ずつ在った。 うち 1 件は 9 cell 全部が生き残って「見える亡霊」に、
//! 2 件は himo ループが途中まで進んで識別子だけ落ちた残骸になっていた
//! (生き残りが毎回ループの接尾辞になるので、 中断点がループ内だと判る)。
//!
//! ここでは crash 相当を **`set_tombstone` を直接呼んで**決定的に作る
//! (tombstone だけ書いて本体を残す = (1) の直後で殺された状態)。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::Hlc;
use std::sync::Arc;

const CAP: usize = 8 * 1024 * 1024;
const PEER: u32 = 7;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-interrupted-delete-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn fresh(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn hlc(wall: u64) -> Hlc {
    Hlc { wall, logical: 0, peer: PEER }
}

/// 行を 2 cell 書いて開いたまま返す。
fn open_with_row(path: &str) -> (Arc<Engine>, u64, u16, u16) {
    let mut eng = Engine::create_with_capacity(path, 256).unwrap();
    eng.define_himo("a", ValueType::Number, 0);
    eng.define_himo("b", ValueType::Number, 0);
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    eng.set_peer_id(PEER);
    let eid = eng.entity();
    let ha = eng.himo_id("a").expect("himo a") as u16;
    let hb = eng.himo_id("b").expect("himo b") as u16;
    // remote 経路で書く = cell に版数が載る (local write でも載るが、 版数を
    // test 側で決め打ちしたいのでこちらを使う)。
    assert!(eng.remote_tie_apply(eid, ha, 11, hlc(100), None));
    assert!(eng.remote_tie_apply(eid, hb, 22, hlc(100), None));
    (eng, eid, ha, hb)
}

/// crash 相当: tombstone だけ書いて本体を残す。
fn interrupt_delete_at_tombstone(eng: &Engine, eid: u64, at: Hlc) {
    assert!(eng.set_tombstone(eid, at), "tombstone を記録できていない");
    assert_ne!(
        eng.tombstone_hlc(eid),
        Hlc::ZERO,
        "v9 (per-cell version) の DB でないと この test は成立しない"
    );
    assert_eq!(eng.get(eid, "a"), Some(11), "本体はまだ残っている状態を作る");
    assert!(eng.is_live(eid));
}

#[test]
fn reopen_finishes_a_delete_that_stopped_after_the_tombstone() {
    let path = tmp_path("sweep");
    fresh(&path);

    let eid = {
        let (eng, eid, _, _) = open_with_row(&path);
        interrupt_delete_at_tombstone(&eng, eid, hlc(200));
        eng.flush_writes();
        eng.oplog_sync().expect("durable");
        eid
    };

    let eng = Engine::open_concurrent_with_oplog(&path, CAP).expect("reopen");
    assert_eq!(
        eng.get(eid, "a"),
        None,
        "tombstone より古い cell が open 後も生きている \
         (削除が途中で止まった行が掃除されていない)"
    );
    assert_eq!(eng.get(eid, "b"), None);
    assert!(!eng.is_live(eid), "行が live のまま = query から見えてしまう");

    fresh(&path);
}

#[test]
fn redelivered_delete_finishes_a_half_applied_one() {
    let path = tmp_path("idempotent");
    fresh(&path);

    let (eng, eid, _, _) = open_with_row(&path);
    interrupt_delete_at_tombstone(&eng, eid, hlc(200));

    // 同じ Delete がもう一度届く。 旧実装は tombstone が同値なので `false` を
    // 返して本体除去に到達しなかった。
    assert!(
        eng.remote_delete_apply(eid, hlc(200), None),
        "同じ版数の Delete が再配送されたら、 本体除去は冪等にやり直されるべき"
    );
    assert_eq!(eng.get(eid, "a"), None, "再配送された Delete で本体が落ちていない");
    assert_eq!(eng.get(eid, "b"), None);
    assert!(!eng.is_live(eid));

    fresh(&path);
}

#[test]
fn a_row_recreated_after_the_delete_survives() {
    let path = tmp_path("recreated");
    fresh(&path);

    let eid = {
        let (eng, eid, ha, _) = open_with_row(&path);
        interrupt_delete_at_tombstone(&eng, eid, hlc(200));
        // 削除より後に作り直された cell (LWW 上は生きているのが正しい)。
        assert!(eng.remote_tie_apply(eid, ha, 99, hlc(300), None));
        eng.flush_writes();
        eng.oplog_sync().expect("durable");
        eid
    };

    let eng = Engine::open_concurrent_with_oplog(&path, CAP).expect("reopen");
    assert_eq!(
        eng.get(eid, "a"),
        Some(99),
        "tombstone より新しい cell (= 削除後に作り直された行) を掃除で消してはいけない"
    );
    assert!(eng.is_live(eid));
    // 削除より古い方の cell は掃除される。
    assert_eq!(eng.get(eid, "b"), None);

    fresh(&path);
}

#[test]
fn a_stale_delete_does_not_wipe_a_newer_row() {
    let path = tmp_path("stale");
    fresh(&path);

    let (eng, eid, ha, _) = open_with_row(&path);
    interrupt_delete_at_tombstone(&eng, eid, hlc(200));
    assert!(eng.remote_tie_apply(eid, ha, 99, hlc(300), None));

    // 既に記録した削除より **古い** Delete。 巻き戻してはいけない。
    assert!(
        !eng.remote_delete_apply(eid, hlc(150), None),
        "記録済みの削除より古い Delete は不採用のはず"
    );
    assert_eq!(eng.get(eid, "a"), Some(99), "古い Delete が新しい行を消した");
    assert!(eng.is_live(eid));

    fresh(&path);
}

#[test]
fn redelivered_delete_keeps_cells_written_after_it() {
    let path = tmp_path("redeliver-keeps");
    fresh(&path);

    let (eng, eid, ha, _) = open_with_row(&path);
    interrupt_delete_at_tombstone(&eng, eid, hlc(200));
    // 削除の後に作り直された cell。
    assert!(eng.remote_tie_apply(eid, ha, 99, hlc(300), None));

    // 同じ Delete (版数 200) の再配送。 冪等に本体を掃除するが、 **削除より後に
    // 書かれた cell まで巻き添えにしてはいけない**。
    assert!(eng.remote_delete_apply(eid, hlc(200), None));
    assert_eq!(
        eng.get(eid, "a"),
        Some(99),
        "再配送された Delete が、 その後に書かれた cell を消した"
    );
    assert_eq!(eng.get(eid, "b"), None, "削除より古い cell は落ちるべき");
    assert!(eng.is_live(eid), "生き残る cell が在るなら行は live のまま");

    fresh(&path);
}
