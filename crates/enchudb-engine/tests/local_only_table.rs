//! request19: **local-only table** — WAL / commit の耐久性は使うが peer には配らない table。
//!
//! 「この端末で観測した事実」 (例: 「この path を、 まさに disk と突き合わせた」) は、
//! 相手に配ると**嘘になる**。 一方で、 その記録が本体の行より先に消えると
//! 「証拠だけ無い」 状態になり、 アプリの判断 (削除ゲート等) が壊れる。 つまり
//! **WAL には載せたいが `_sync_ops` には流したくない** table が要る。
//!
//! `_sync_ops` / `_sync_peers` が既にその性質を持っていたので、 除外判定を
//! 「決め打ち 2 table」 から 「**reserved table (= `_` 始まり) 全部**」 へ一般化した。
//! `is_reserved` は名前だけで決まるので sidecar の format 変更は無く、 reopen を
//! 跨いでそのまま効く。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::Duration;

const CAP: usize = 8 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-local-only-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// user table 1 つ + local-only table 1 つを持つ DB を作る。
fn make_db(path: &str) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("notes", 64).unwrap();
    eng.define_himo_in("notes", "n", ValueType::Number, 0).unwrap();
    eng.define_reserved_table("_seen", 64).unwrap();
    eng.define_himo_in("_seen", "n", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    eng.set_peer_id(11);
    eng
}

/// 1 cell 書いて WAL commit → bridge まで流す。
///
/// bridge は consumer thread (≤100ms 周期) も回すので、 明示 transfer に加えて
/// 1 tick 待ってから測る。
fn write_and_bridge(eng: &Arc<Engine>, eid: u64, himo: &str, v: u32) {
    eng.tie_to(eid, himo, v);
    eng.flush_writes();
    eng.oplog_commit();
    eng.oplog_sync().expect("durable");
    eng.transfer_oplog_to_sync_ops();
    std::thread::sleep(Duration::from_millis(250));
    eng.transfer_oplog_to_sync_ops();
}

/// bridge 済み payload (oplog record の wire bytes) に marker 値が u32 LE で載っているか。
fn bridged(eng: &Arc<Engine>, marker: u32) -> bool {
    let pat = marker.to_le_bytes();
    eng.pending_sync_ops(0)
        .iter()
        .any(|p| p.windows(4).any(|w| w == pat))
}

#[test]
fn writes_to_a_local_only_table_are_not_bridged() {
    let path = tmp_path("bridge");
    cleanup(&path);

    let eng = make_db(&path);
    let note = eng.entity_in("notes").expect("notes entity");
    let seen = eng.entity_in("_seen").expect("_seen entity");

    const USER_MARKER: u32 = 0x1234_5678;
    const LOCAL_MARKER: u32 = 0x0BAD_F00D;

    write_and_bridge(&eng, note, "notes.n", USER_MARKER);
    assert!(
        bridged(&eng, USER_MARKER),
        "user table の write が bridge されていない — test の前提が壊れている"
    );

    write_and_bridge(&eng, seen, "_seen.n", LOCAL_MARKER);
    assert!(
        !bridged(&eng, LOCAL_MARKER),
        "local-only table の write が `_sync_ops` に流れている (peer に配られてしまう)"
    );
    // 配らないだけで、 自分では読めること。
    assert_eq!(eng.get(seen, "_seen.n"), Some(LOCAL_MARKER));

    cleanup(&path);
}

#[test]
fn local_only_survives_reopen_and_stays_local() {
    let path = tmp_path("reopen");
    cleanup(&path);

    let seen = {
        let eng = make_db(&path);
        let seen = eng.entity_in("_seen").expect("_seen entity");
        write_and_bridge(&eng, seen, "_seen.n", 7);
        seen
    };
    const REOPEN_MARKER: u32 = 0x0CAF_E123;

    let eng = Engine::open_concurrent_with_oplog(&path, CAP).expect("reopen");
    assert_eq!(
        eng.get(seen, "_seen.n"),
        Some(7),
        "local-only table の中身が reopen で消えている (WAL / commit の耐久性が効いていない)"
    );

    // reopen 後の write も bridge されないこと (= reserved 判定が sidecar から復元されている)。
    write_and_bridge(&eng, seen, "_seen.n", REOPEN_MARKER);
    assert!(
        !bridged(&eng, REOPEN_MARKER),
        "reopen 後に local-only table の write が bridge され始めている"
    );

    cleanup(&path);
}

#[test]
fn clear_local_only_tables_wipes_only_those() {
    let path = tmp_path("clear");
    cleanup(&path);

    let eng = make_db(&path);
    let note = eng.entity_in("notes").expect("notes entity");
    let seen = eng.entity_in("_seen").expect("_seen entity");
    write_and_bridge(&eng, note, "notes.n", 3);
    write_and_bridge(&eng, seen, "_seen.n", 4);
    let pending_before = eng.pending_sync_ops(0).len();
    assert!(pending_before > 0, "`_sync_ops` に user table の record が入っていない");

    let cleared = eng.clear_local_only_tables();
    assert_eq!(cleared, 1, "local-only table の行が落ちていない");
    assert_eq!(eng.get(seen, "_seen.n"), None, "local-only の行が残っている");
    assert_eq!(
        eng.get(note, "notes.n"),
        Some(3),
        "user table まで消している (snapshot 受け側で本体が飛ぶ)"
    );
    assert_eq!(
        eng.pending_sync_ops(0).len(),
        pending_before,
        "engine 内部 table (`_sync_ops`) まで消している — bootstrap の backlog が失われる"
    );

    cleanup(&path);
}

/// local-only table の write が **WAL に載る**こと (= oplog head が進む)。
///
/// ここが request19 の本体。 載っていないと 「本体の行は WAL 経由で復元されるのに、
/// それに対する観測記録だけ消える」 が起きる (= 台帳を失った path の削除が
/// 永久に見送られる、 という実地の失敗そのもの)。
///
/// 対比のため engine 内部 table (`_sync_peers`) への write も測る — あちらは
/// **載ってはいけない** (`_sync_ops` の行は WAL record から作られるので、 積むと
/// WAL が自分自身を食う)。
#[test]
fn local_only_writes_are_logged_in_the_wal() {
    let path = tmp_path("wal");
    cleanup(&path);

    let eng = make_db(&path);
    let seen = eng.entity_in("_seen").expect("_seen entity");
    let peers_row = eng.entity_in("_sync_peers").expect("_sync_peers entity");

    let before = eng.stats().oplog_head;
    eng.tie_to(seen, "_seen.n", 5);
    eng.flush_writes();
    let after_local_only = eng.stats().oplog_head;
    assert!(
        after_local_only > before,
        "local-only table の write が WAL に載っていない \
         (crash すると本体の行だけ復元され、 観測記録は消える)"
    );

    eng.tie_to(peers_row, "_sync_peers.peer_id", 99);
    eng.flush_writes();
    assert_eq!(
        eng.stats().oplog_head,
        after_local_only,
        "engine 内部 table への write が WAL に載っている (WAL が自分自身を食う)"
    );

    cleanup(&path);
}

/// WAL に届いていた local-only write が、 crash 後の recovery で本体に入ること。
#[test]
fn local_only_write_is_replayed_after_crash() {
    use enchudb_oplog::oplog::{Op, OpLog};
    use enchudb_oplog::Hlc;

    let path = tmp_path("replay");
    cleanup(&path);

    let (seen, hid) = {
        let eng = make_db(&path);
        let seen = eng.entity_in("_seen").expect("_seen entity");
        let hid = eng.himo_id("_seen.n").expect("himo") as u16;
        eng.tie_to(seen, "_seen.n", 1);
        eng.flush_writes();
        eng.oplog_sync().expect("durable");
        (seen, hid)
    };

    // crash 相当: WAL にだけ record を置く (body 未適用)。
    {
        let wal = OpLog::open(std::path::Path::new(&format!("{path}/oplog"))).expect("open wal");
        let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), enchudb_oplog::eid_local(seen));
        wal.append_at_hlc(
            Op::Tie { eid: oplog_eid, himo_id: hid, value: 42 },
            Hlc { wall: u64::MAX / 2, logical: 0, peer: 11 },
        )
        .expect("append");
    }

    let eng = Engine::open_concurrent_with_oplog(&path, CAP).expect("reopen");
    assert_eq!(
        eng.get(seen, "_seen.n"),
        Some(42),
        "WAL に届いていた local-only write が recovery で本体に入っていない"
    );

    cleanup(&path);
}
