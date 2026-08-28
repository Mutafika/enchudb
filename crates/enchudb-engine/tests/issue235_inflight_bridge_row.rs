//! #235 — #217 の dead-row purge が **書き込み途中の bridge row** を食べていた。
//!
//! bridge は `lsn` を先に、 `payload` を最後に tie していた。 `_sync_ops` の走査は
//! 全部 `entities_with_himo(lsn_hid)` 経由なので、 lsn を tie した瞬間に row が索引へ
//! 載る — payload はまだ無い。 そこへ `ack_sync_prefix` の dead-row 分岐が当たると、
//! **`consumed` 述語を通らないまま** 「壊れている」 と判定して purge する。
//! oplog cursor は既に越えているので、 その record は二度と bridge されない。
//!
//! 実測 (fix 前): 51960 lsn 発行に対し `_sync_ops` は 51956 行、
//! `sync_dead_rows_purged` = 4。 **破損を一切注入していない健全な系**での silent loss。
//!
//! fix: (1) `lsn` を **最後**に書いて row の commit marker にする
//! (索引に載っている row は必ず完成している)。 (2) backstop として、 最新 lsn の row は
//! dead 判定せず break する (未完成 row は常に高々 1 つ、 かつ必ず最新 lsn)。

use enchudb_engine::engine::Engine;
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue235-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// bridge と ack を並走させた時、 **健全な row が dead 判定されない**こと。
///
/// これは競合の検出 test なので、 緑は 「この窓が塞がっている」 の証拠ではなく
/// 「今回の走行では踏まなかった」 でしかない。 正しさの根拠は書き込み順
/// (lsn = commit marker) と 「未完成 row は必ず最新 lsn」 の不変式の方。
/// 検出力の実測: **fix を両方外すと 3 回中 2 回落ちる** (1〜2 件を purge)。
/// 100% ではないので、 落ちなかった回を「安全」と読まないこと。 fix 後は 5/5 green。
#[test]
fn inflight_bridge_row_is_not_eaten_by_ack() {
    let path = tmp_path("inflight");
    cleanup(&path);
    let mut eng = Engine::create_with_capacity(&path, 200_000).unwrap();
    eng.define_table("notes", 60_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 64 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);

    // 件数で区切る (時間で区切ると速い機械で notes の eid 枠を食い潰す)。
    const NOTES: u32 = 40_000;
    let done = Arc::new(AtomicBool::new(false));

    // writer + bridge: 書いて bridge する
    let e2 = eng.clone();
    let d2 = done.clone();
    let writer = std::thread::spawn(move || {
        let mut i = 0u32;
        while i < NOTES {
            for _ in 0..20 {
                let e = e2.entity_in("notes").unwrap();
                e2.tie_to(e, "notes.note", i);
                i += 1;
            }
            e2.oplog_commit();
            e2.flush_writes();
            e2.oplog_sync().unwrap();
            e2.transfer_oplog_to_sync_ops();
        }
        d2.store(true, Ordering::Release);
        i
    });

    // acker: 「全部消化済み」の cursor で prefix walk を回す。 これで walk は必ず
    // 末尾 (= 書き込み途中の row) まで到達する。
    let far = Hlc { wall: u64::MAX, logical: u32::MAX, peer: u32::MAX };
    let cursors = [(1u32, far), (0u32, far)];
    while !done.load(Ordering::Acquire) {
        let _ = eng.ack_sync_up_to_cursors(2, &cursors);
    }
    let written = writer.join().unwrap();

    // reclaim は一度も呼んでいないので、 発行した lsn の数 = 生存 row 数のはず。
    let rows = eng.pending_sync_ops(0).len();
    let issued = eng.current_sync_lsn();
    let dead = eng.sync_dead_rows_purged();

    assert_eq!(
        dead, 0,
        "健全な系で dead row が {dead} 件 purge された = 書き込み途中の bridge row を \
         食べている (written={written})"
    );
    assert_eq!(
        rows, issued as usize,
        "_sync_ops の行数 {rows} が発行 lsn {issued} と一致しない = record が \
         sync stream から消えている (oplog cursor は越えているので再 bridge されない)"
    );
    cleanup(&path);
}

/// `reclaim_sync_ops` も同じ扉を持っていた: **decode 可否を見ずに削除する**ので、
/// caller が実在より先を ack して watermark が未完成 row を跨ぐと、
/// `ack_sync_prefix` と同じ silent loss になる。 `ack_sync` は生の lsn を受ける
/// public API なので、 caller 側だけでは防げない (enchudb 自身の
/// `issue221_purge_atomicity` と sunsu2 の `relay_death_*` が実際に使っている)。
///
/// ここで **payload を剥がして「decode 不能」を作っている**のは意図的。 engine は
/// 「まだ書けていない」 と 「壊れている」 を**観測上区別できない**、 というのが
/// #235 の要点そのものなので、 「見分けが付かないなら最新 row は壊すな」 を
/// 直接 assert する。 実際の in-flight 競合の方は
/// `inflight_bridge_row_is_not_eaten_by_ack` (並行) が見る。
///
/// 逆に **完成している最新 row は従来どおり reclaim される** — そこまで避けると
/// reclaim の意味論が変わる (`destructive_0_7_0::ack_with_future_lsn_does_not_corrupt_reclaim`)。
#[test]
fn over_ack_does_not_let_reclaim_eat_an_undecodable_newest_row() {
    let path = tmp_path("overack");
    cleanup(&path);
    let mut eng = Engine::create_with_capacity(&path, 50_000).unwrap();
    eng.define_table("notes", 10_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);

    for i in 0..50u32 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();

    let newest = eng.current_sync_lsn();
    assert!(newest > 0, "前提: bridge が回っていること");

    // 最新 row を「書き込み途中」と同じ観測状態にする (payload が引けない)。
    let newest_eid = *eng.pull_raw("_sync_ops.lsn", newest).first().expect("row exists");
    eng.untie(newest_eid, "_sync_ops.payload");

    // 実在より先を ack する (caller の誤り、 だが API は受け付ける)。
    eng.ack_sync(2, newest + 10).unwrap();
    eng.reclaim_sync_ops();

    assert!(
        !eng.pull_raw("_sync_ops.lsn", newest).is_empty(),
        "decode 不能な最新 lsn {newest} の row が over-ack で消えた — \
         書き込み途中ならこれがそのまま silent loss になる"
    );
    // 手前の row は普通に reclaim されていること (guard が広すぎないこと)。
    assert!(
        eng.pull_raw("_sync_ops.lsn", newest - 1).is_empty(),
        "guard が広すぎる — 完成済みの row まで reclaim されなくなっている"
    );
    cleanup(&path);
}
