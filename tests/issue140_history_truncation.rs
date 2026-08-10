//! #140 regression — reclaim で落ちた履歴を「完全な履歴」として配ってしまう。
//!
//! `_sync_ops` は ring buffer で、 `lsn < sync_watermark()` (= 登録済み全 peer が
//! consume 済みの境界) の row は `reclaim_sync_ops` で purge される。 問題は
//! **purge されたことが pull 側に一切伝わらない**こと。
//!
//! `pending_sync_ops` / `Syncer::collect_records_since` は生き残った row を返すだけなので、
//! cursor 0 の新規 peer は「作成 record はあるが削除 record は purge 済み」という
//! 部分履歴を **全履歴だと信じて** 再生する。 結果、 削除済み entity が復活する。
//!
//! 実機 (下流 syncretic) では 1402 file entity 中 611 が亡霊化した。
//!
//! ## 実測でわかった経路の訂正
//!
//! - #140 本文は「16MiB の oplog リング一周」と書いているが、 consumer thread は
//!   `try_reset()` の **前に** `transfer_oplog_to_sync_ops()` を流し切るので、 oplog の
//!   巻き戻し自体では失われない。 実際の retention 境界は **`_sync_ops` の reclaim**。
//! - reclaim は `lsn` 昇順 (= 古い順) に落とすので、 素の「作成 → 削除 → reclaim」では
//!   **作成 record が先に落ちる**。 本文の「作成 record はあるが削除 record が無い」並びには
//!   素の手順ではならない。 あの並びは「削除済み entity の tombstone が期限切れした後に、
//!   同じ論理行の作成 record が生き残る」状況 — つまり #141 のチャーンループ (同一 PK の
//!   重複を作り続ける) が生成源だった可能性が高い。
//!
//! ## このテストが押さえる欠陥
//!
//! 亡霊化の有無によらず、 **reclaim 後に cursor 0 で pull した peer は「届かなかった履歴が
//! ある」ことを知らされない**。 pull は成功扱いで返り、 peer は不完全な store を持ったまま
//! 同期済みだと信じる。 bootstrap-first (= #140 の第 1 案) ではこの状態を検知して
//! 「bootstrap が必要」と返さなければならない。

use std::sync::Arc;

use enchudb::schema::Database;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const TABLE: &str = "notes";
const COL_BODY: &str = "body";

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-issue140-{}-{}", tag, std::process::id());
    for suffix in ["", ".oplog", ".crc", ".tables", ".schema", ".eidmap", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc", ".tables", ".schema", ".eidmap", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

fn make_peer(path: &str, peer: u32) -> Arc<Database> {
    let mut b = Database::create_with_capacity(path, 65_536).unwrap();
    b.table(TABLE).tag(COL_BODY).build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(16 * 1024 * 1024).unwrap();
    db.engine().set_peer_id(peer);
    db
}

fn flush(db: &Database) {
    let eng = db.engine();
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

/// reclaim で履歴が落ちた store に cursor 0 の新規 peer がフル pull したとき、
/// **黙って不完全な store になってはならない** (収束するか、 truncation を通知するか)。
#[test]
fn full_pull_after_reclaim_must_not_silently_lose_history() {
    let pa = tmp("origin");
    let pb = tmp("fresh");

    let db_a = make_peer(&pa, 1);

    // 1. entity を 2 つ作り、 片方を削除する。 生存側 ("kept") が A の live state。
    let doomed = db_a
        .get_table(TABLE)
        .unwrap()
        .insert()
        .set(COL_BODY, "to be deleted")
        .commit()
        .unwrap();
    db_a.get_table(TABLE)
        .unwrap()
        .insert()
        .set(COL_BODY, "kept")
        .commit()
        .unwrap();
    flush(&db_a);
    db_a.engine().delete(doomed);
    flush(&db_a);
    db_a.engine().rebuild();
    let a_live = db_a.get_table(TABLE).unwrap().all().count().unwrap();
    assert_eq!(a_live, 1, "A の live state は kept のみ");

    // 2. 既知 peer (id=2) が最新まで consume したことにして reclaim を走らせる。
    //    これで作成/削除 record が `_sync_ops` から purge される。
    let eng_a = db_a.engine();
    let latest_lsn = eng_a.current_sync_lsn();
    eng_a.ack_sync(2, latest_lsn).unwrap();
    let purged = eng_a.reclaim_sync_ops();
    assert!(purged > 0, "reclaim が走っていること (purged={purged})");

    // 3. **未登録の新規 peer** が cursor 0 でフル pull する。
    //    削除 record は purge 済みなので、 素朴に再生すると entity が復活する。
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let sync_a = Syncer::new(db_a.arc_engine(), transport.clone());
    sync_a.publish_since(Hlc::ZERO);

    let db_b = make_peer(&pb, 9);
    let sync_b = Syncer::new(db_b.arc_engine(), transport.clone());
    let out = sync_b.pull_once(1);
    db_b.engine().rebuild();

    // 4. 「収束する」か「追いつけないと通知する」かのどちらかであること。
    //    黙って不完全な store になるのが #140。
    let b_live = db_b.get_table(TABLE).unwrap().all().count().unwrap();
    let ghosts = db_b
        .get_table(TABLE)
        .unwrap()
        .where_eq(COL_BODY, "to be deleted")
        .count()
        .unwrap();
    assert_eq!(ghosts, 0, "削除済み entity が {ghosts} 件復活した");

    // この筋書きは意図的に reclaim を起こしているので、 **必ず** truncation 側に落ちること。
    // (これを assert しないと「B に何も届かないので亡霊も居ない」で自明に通ってしまう —
    //  最初に書いた版が実際にその vacuous pass だった。)
    assert!(
        out.history_truncated,
        "reclaim 済みなのに truncation が通知されていない: {out:?}",
    );

    if out.history_truncated {
        // 通知された場合は何も適用していないこと (部分適用は不完全な store を作る)。
        assert_eq!(out.applied, 0, "truncation 通知時は適用しないこと: {out:?}");
        assert_eq!(b_live, 0, "truncation 通知時は store を触らないこと");
    } else {
        assert_eq!(
            b_live, a_live,
            "#140: 通知も無く B が不完全になっている (A={a_live} / B={b_live}、 {out:?})",
        );
    }

    drop(db_a);
    drop(db_b);
    cleanup(&pa);
    cleanup(&pb);
}
