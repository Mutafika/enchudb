//! #1066 (#1046 の (1)): sync する DB (sync の表がある) は、 WAL 付きの session でしか、 ユーザの表の行を書けない。
//!
//! WAL なしで開いた engine (`open_standalone` / build phase = `concurrentize_with_oplog` の前) の書き込みは版数 (WAL の時計) が付かず、
//! bridge にも #451 の作り直しにも拾われず相手に配られなかった。 消した行は tombstone なしで死に、 遅れた受信で次の世代に生き返った
//! (#1046)。 今は、 ユーザの表への書き込み (行の払い出し / tie / untie / delete / content) を何も変えずに断る
//! (`FaultKind::NoWal`)。 予約の表 (`_` で始まる)・相手から受けた書き込み・開く時の内部の書き込みは断らない。 読みはできる。
//!
//! falsify: engine の `refuses_walless_sync_write` (と `try_entity_in_row` の断り) を外すと、 WAL なしの書き込みが通って落ちる。
use enchudb_engine::engine::Engine;
use enchudb_engine::{Cursor, FaultKind, TieRejected, ValueType};
use enchudb_oplog::Hlc;

const WAL: usize = 4 * 1024 * 1024;

struct Rm(String);
impl Drop for Rm {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir()
        .join(format!("enchudb-issue1066-{tag}-{}-{nanos}", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

/// 表 t (列 v = Number、 n = Leaf) の sync する DB を WAL 付きで作り、 行 X (v = 1、 n = "x") を書いて閉じる。 X を返す。
fn sync_db_with_a_row(path: &str) -> u64 {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("t", 16).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "n", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(1);
    let x = eng.entity_in("t").unwrap();
    eng.tie_to(x, "t.v", 1u32);
    eng.tie_text_to(x, "t.n", "x");
    eng.oplog_sync().unwrap();
    x
}

/// 行の見える物 (生きているか、 v、 n、 削除の版数、 content)。
type RowState = (bool, Option<u64>, Option<Vec<u8>>, Hlc, Option<Vec<u8>>);

/// 行 X の見える物。 世代は eid に入っている (X の eid のまま引けるか)。
fn state(eng: &Engine, x: u64) -> RowState {
    (eng.is_live(x), eng.get(x, "t.v"), eng.get_text_owned(x, "t.n"), eng.tombstone_hlc(x), eng.get_content_owned(x, "memo"))
}

/// WAL なしで開いた sync する DB: ユーザの表への書き込みは全部断られ、 何も変わらない (行・cell・Leaf・削除の版数・世代)。 読みはできる。
/// 相手から受けた書き込み (版数を持つ) と予約の表 (`_cursors`) の書き込みは通る。 WAL 付きで開き直すと書ける。
#[test]
fn a_sync_db_opened_without_the_wal_refuses_user_writes() {
    let path = tmp("standalone");
    let _rm = Rm(path.clone());
    let x = sync_db_with_a_row(&path);
    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        eng.set_peer_id(1);
        let before = state(&eng, x);
        assert_eq!(before, (true, Some(1), Some(b"x".to_vec()), Hlc::ZERO, None), "前提: X が読める");
        let v = eng.himo_id("t.v").unwrap() as u16;
        let n = eng.himo_id("t.n").unwrap() as u16;
        let faults = eng.fault_count(FaultKind::NoWal);
        let refused = [
            ("entity_in", eng.entity_in("t").is_err()),
            ("try_tie_to_by_id", eng.try_tie_to_by_id(x, v, 2u32) == Err(TieRejected::Fault(FaultKind::NoWal))),
            ("try_tie_text_to_by_id", eng.try_tie_text_to_by_id(x, n, "y") == Err(TieRejected::Fault(FaultKind::NoWal))),
            ("try_delete", eng.try_delete(x) == Err(TieRejected::Fault(FaultKind::NoWal))),
        ];
        eng.tie_to(x, "t.v", 3u32);
        eng.tie_text_to(x, "t.n", "z");
        eng.untie(x, "t.v");
        eng.content(x, "memo", b"m");
        eng.delete(x);
        // build phase の形 (`&mut self`) も
        eng.tie(x, "t.v", 4u32);
        eng.tie(x, "t.new", 4u32);
        eng.tie_text(x, "t.n", "w");
        let after = state(&eng, x);
        let counted = eng.fault_count(FaultKind::NoWal) - faults;
        let grew_himo = eng.himo_id("t.new").is_some() || eng.himo_id("_c_memo").is_some();
        for (what, ok) in &refused {
            assert!(ok, "WAL なしで開いた sync する DB で {what} が断られない");
        }
        assert_eq!(after, before, "WAL なしで開いた sync する DB の書き込みが X を変えた");
        assert!(!grew_himo, "断った書き込みが紐を生やした");
        assert!(counted >= 11, "断った書き込みを fault に数えない ({counted})");
        // 相手から受けた書き込み (版数を持つ) は当たる (#1066 の対象外)
        let f = eng.resolve_remote_eid(enchudb_oplog::make_eid(2, 7), v).expect("前提: 受けた行の場所");
        assert!(
            eng.remote_tie_apply_result(f, v, 9u32, Hlc { wall: 1_000, logical: 0, peer: 2 }).applied(),
            "WAL なしで開いた DB に、 相手から受けた書き込みが当たらない"
        );
    }
    // WAL 付きで開き直すと書ける
    let eng = Engine::open_concurrent_with_oplog(&path, WAL).unwrap();
    eng.set_peer_id(1);
    eng.tie_to(x, "t.v", 5u32);
    let y = eng.entity_in("t").expect("WAL 付きで開き直しても行を足せない");
    eng.tie_to(y, "t.v", 6u32);
    eng.delete(x);
    assert_eq!(eng.get(y, "t.v"), Some(6));
    assert!(!eng.is_live(x), "WAL 付きで開き直しても消せない");
    assert_ne!(eng.tombstone_hlc(x), Hlc::ZERO, "WAL 付きの削除に版数が付かない");
}

/// build phase (`concurrentize_with_oplog` の前) に sync の表を作った DB は、 ユーザの表の行を書けない。 予約の表 (`_cursors`) は
/// 書ける。 `concurrentize_with_oplog` の後は書ける。
#[test]
fn the_build_phase_of_a_sync_db_refuses_user_rows() {
    let path = tmp("build");
    let _rm = Rm(path.clone());
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("t", 16).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    // sync の表を作る前は書ける (今まで通り)
    let before_sync = eng.entity_in("t").expect("sync の表の無い DB で行を足せない");
    eng.tie(before_sync, "t.v", 1u32);
    eng.enable_sync_tables().unwrap();
    eng.prepare_cursors().unwrap();
    let refused_row = eng.entity_in("t").is_err();
    eng.tie(before_sync, "t.v", 2u32);
    let kept = eng.get(before_sync, "t.v");
    // 予約の表 (local-only の `_cursors`) は書ける
    let cursor = Cursor { kind: 7, pos: 42, hlc: None };
    let cursor_written = eng.cursor_set("c", cursor).is_ok() && eng.cursor_get("c") == Some(cursor);
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(1);
    let after = eng.entity_in("t").expect("WAL 付きになった後に行を足せない");
    eng.tie_to(after, "t.v", 3u32);
    eng.tie_to(before_sync, "t.v", 4u32);
    assert!(refused_row, "build phase の sync する DB でユーザの表に行を足せた");
    assert_eq!(kept, Some(1), "build phase の sync する DB でユーザの表の行を書き換えた");
    assert!(cursor_written, "build phase の sync する DB で予約の表 (`_cursors`) を書けない");
    assert_eq!((eng.get(after, "t.v"), eng.get(before_sync, "t.v")), (Some(3), Some(4)), "WAL 付きになった後に書けない");
}

/// sync の表の無い DB は、 WAL なしで開いても今まで通り書ける。
#[test]
fn a_db_without_sync_tables_still_writes_without_the_wal() {
    let path = tmp("nosync");
    let _rm = Rm(path.clone());
    {
        let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
        eng.define_table("t", 16).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.flush().unwrap();
    }
    let mut eng = Engine::open_standalone(&path).unwrap();
    let x = eng.entity_in("t").expect("sync の表の無い DB で行を足せない");
    eng.tie(x, "t.v", 1u32);
    let y = eng.entity_in("t").unwrap();
    eng.tie(y, "t.v", 2u32);
    eng.delete(y);
    assert_eq!(eng.get(x, "t.v"), Some(1));
    assert!(!eng.is_live(y));
    assert_eq!(eng.fault_count(FaultKind::NoWal), 0);
}
