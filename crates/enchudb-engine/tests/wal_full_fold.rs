//! WAL 満杯 brick の回帰テスト。
//!
//! commit group の途中で WAL が「Commit 1 個すら append できない」満杯
//! （`append_dead`）に達すると、 閉じの Commit が書けず tail は永久に未 commit の
//! まま残る。 旧 `wal_fold_safe`（offset >= head のみ）はこの tail を理由に fold を
//! 恒久拒否し、 以後の append 全滅（無音 drop）→ 新規変更が sync から永久欠落 →
//! reopen のたび旧 backlog だけを全量再 bridge、 という**自己修復不能の brick** に
//! なっていた（実運用で発現: ring 満杯の backpressure 中に大量登録 burst が WAL を
//! 埋め切った）。 本テストは oplog を直接操作してその brick を決定的に再現し、
//! 「append_dead + committed 残なし」で畳んでよい、 の修正を固定する。 同時に
//! 「WAL に余裕がある書きかけ group は畳んではいけない」の保護も回帰で固定する。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::Op;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-walfold-{}-{}-{}",
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
    for suffix in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

fn small_engine(path: &str, wal_bytes: usize) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 256).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "blob", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, wal_bytes).unwrap();
    eng.set_peer_id(42);
    eng
}

/// Record サイズの前提（v2 layout）: Tie = 112 + 16 = 128、 Commit = 112、
/// TieLeaf = 112 + 16 + name + bytes。 前提が変わったら本テストの操縦計算も
/// 崩れるので、 まず実測でズレを検出する。
#[test]
fn record_size_assumptions() {
    let path = tmp_path("sizes");
    cleanup(&path);
    let eng = small_engine(&path, 8 * 1024);
    let e = eng.entity_in("notes").unwrap();
    let note_hid = eng.himo_id("notes.note").unwrap() as u16;
    let wal = eng.oplog().unwrap().clone();

    let f0 = wal.free_bytes();
    wal.append(Op::Tie { eid: e, himo_id: note_hid, value: 1 }).unwrap();
    let f1 = wal.free_bytes();
    assert_eq!(f0 - f1, 128, "Tie record サイズが前提とズレた");
    wal.append(Op::Commit).unwrap();
    let f2 = wal.free_bytes();
    assert_eq!(f1 - f2, 112, "Commit record サイズが前提とズレた");
    wal.append(Op::TieLeaf {
        eid: e,
        himo_name: "notes.blob",
        himo_kind: ValueType::Leaf as u8,
        bytes: &[0u8; 10],
    })
    .unwrap();
    let f3 = wal.free_bytes();
    assert_eq!(f2 - f3, 112 + 16 + 10 + 10, "TieLeaf record サイズが前提とズレた");

    cleanup(&path);
}

#[test]
fn wal_full_mid_group_folds_after_bridge_drains() {
    let path = tmp_path("brick");
    cleanup(&path);
    let eng = small_engine(&path, 8 * 1024);
    let e = eng.entity_in("notes").unwrap();
    let note_hid = eng.himo_id("notes.note").unwrap() as u16;
    let wal = eng.oplog().unwrap().clone();

    // ── Phase 1: ring を満杯にして bridge を backpressure で止める ────────────
    // committed group を WAL へ直書きし、 bridge → fold で WAL を回転させ続ける。
    // ack しないので ring の生存 row は単調増加し、 いずれ transfer が読み残す
    // （= wal_fold_safe が false のまま）= ring 満杯、 でループを抜ける。
    // 充填は「Phase 2 が未 commit tail を作れる余白 (400B)」を必ず残して止める —
    // 使い切ると jam 成立時に free < 138 で孤児 record すら書けない周回がありうる。
    let mut v = 0u32;
    loop {
        while wal.free_bytes() >= 240 + 400 {
            wal.append(Op::Tie { eid: e, himo_id: note_hid, value: v }).unwrap();
            wal.append(Op::Commit).unwrap();
            v += 1;
        }
        while eng.transfer_oplog_to_sync_ops() > 0 {}
        eng.oplog_sync().unwrap();
        if !eng.wal_fold_safe() {
            break; // 読み残し committed が残った = ring 満杯 (backpressure 成立)
        }
        if wal.try_reset() {
            eng.reset_sync_ops_offset();
        }
        assert!(v < 20_000, "ring が満杯にならない");
    }

    // ── Phase 2: 未 commit の tail を作り、 WAL を append_dead まで正確に埋める。
    // Tie (128B) を Commit 無しで積み、 最後は TieLeaf の可変 payload で残りを
    // 112 バイト未満（= Commit も入らない）へ落とす。 閉じの Commit は満杯で
    // 失敗する = 孤児 group 確定。
    while wal.free_bytes() >= 128 + 168 {
        wal.append(Op::Tie { eid: e, himo_id: note_hid, value: v }).unwrap();
        v += 1;
    }
    let free = wal.free_bytes(); // ∈ [168, 296)
    let pad = vec![0u8; free.saturating_sub(194) as usize];
    wal.append(Op::TieLeaf {
        eid: e,
        himo_name: "notes.blob",
        himo_kind: ValueType::Leaf as u8,
        bytes: &pad,
    })
    .unwrap();
    assert!(
        wal.append_dead(),
        "WAL が append_dead にならない (free={})",
        wal.free_bytes()
    );
    assert!(
        wal.append(Op::Commit).is_err(),
        "満杯のはずの WAL に Commit が入った"
    );

    // ── Phase 3: ack + reclaim で ring を空け、 bridge に committed 分を
    // 読み切らせる。 未 commit の孤児 tail だけが残る。
    let mut rounds = 0;
    loop {
        eng.ack_sync(7, eng.current_sync_lsn()).unwrap();
        eng.reclaim_sync_ops();
        let moved = eng.transfer_oplog_to_sync_ops();
        rounds += 1;
        assert!(rounds < 300, "bridge が収束しない");
        if moved == 0 {
            break;
        }
    }
    eng.ack_sync(7, eng.current_sync_lsn()).unwrap();
    eng.reclaim_sync_ops();

    // ── 本丸: 旧実装は offset < head（未 commit の孤児 tail）で fold を恒久拒否
    // = brick。 修正後は「append_dead + committed 残なし」で畳んでよい。
    assert!(
        eng.wal_fold_safe(),
        "満杯 WAL の未 commit tail (孤児 group) が fold を恒久ブロックしている"
    );

    // fold を実行（consumer tick が先に畳んでいてもよい）。
    if wal.append_dead() {
        eng.oplog_sync().unwrap();
        if eng.wal_fold_safe() && wal.try_reset() {
            eng.reset_sync_ops_offset();
        }
    }
    let mut waited = 0;
    while wal.append_dead() && waited < 100 {
        std::thread::sleep(Duration::from_millis(20));
        waited += 1;
    }
    assert!(!wal.append_dead(), "fold 判定は通ったのに WAL が畳まれていない");

    // ── 畳まれた後の新規 write が sync 経路に乗る（brick 解消の end-to-end 証明）。
    let before = eng.pending_sync_ops(0).len();
    wal.append(Op::Tie { eid: e, himo_id: note_hid, value: 777_777 }).unwrap();
    wal.append(Op::Commit).unwrap();
    let moved = eng.transfer_oplog_to_sync_ops();
    assert!(moved > 0, "fold 後の新規 write が bridge に乗らない");
    assert!(
        eng.pending_sync_ops(0).len() > before,
        "fold 後の新規 write が _sync_ops に現れない"
    );

    cleanup(&path);
}

#[test]
fn uncommitted_tail_with_room_blocks_fold() {
    let path = tmp_path("guard");
    cleanup(&path);
    let eng = small_engine(&path, 8 * 1024);
    let e = eng.entity_in("notes").unwrap();
    let note_hid = eng.himo_id("notes.note").unwrap() as u16;
    let wal = eng.oplog().unwrap().clone();

    // committed group を 1 つ流して bridge を追いつかせる。
    wal.append(Op::Tie { eid: e, himo_id: note_hid, value: 1 }).unwrap();
    wal.append(Op::Commit).unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}

    // 書きかけ group（未 commit、 WAL には十分な余裕がある）。
    wal.append(Op::Tie { eid: e, himo_id: note_hid, value: 2 }).unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    assert!(!wal.append_dead(), "前提が崩れた: WAL に余裕があるはず");
    assert!(
        !eng.wal_fold_safe(),
        "余裕のある WAL の書きかけ group を fold 可能と判定してはいけない"
    );

    // 閉じれば bridge が読み切り、 fold してよくなる。
    wal.append(Op::Commit).unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    assert!(eng.wal_fold_safe(), "commit 後も fold 可能にならない");

    cleanup(&path);
}
