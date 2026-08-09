//! #141 regression — 同一 PK の entity が cross-author apply で重複払い出しされる。
//!
//! 2 台がそれぞれ**同じ自然キー (PK) を持つ row を独立に作成**してから相互 sync すると、
//! `Syncer::apply_one` → `Engine::resolve_remote_eid` が `(author, remote_eid)` 写像だけで
//! 解決し、初見の remote_eid には `alloc_translated_local` で**新規 eid を払い出す**。
//! 適用先 table に同じ PK の既存 row がいても束ねないため、 同一 PK の entity が 2 個並ぶ。
//!
//! 実機 (下流 syncretic) では 1 table 内 2,358 entity 中 788 個が同一キー文字列の重複、
//! 恒久チャーンループ → WAL 膨張 → oplog リング一周 → #140 の tombstone 消失、 と連鎖した。
//!
//! **PK は schema 層の概念で engine は関知しない** (`enchudb-sync` と `enchudb-schema` は
//! 兄弟 crate で互いに見えない) ため、 このテストは schema 層 (`Database`) + `Syncer` の
//! 組み合わせで書く。

use std::sync::Arc;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const TABLE: &str = "articles";
const COL_URL: &str = "url";
const COL_TITLE: &str = "title";

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-issue141-{}-{}", tag, std::process::id());
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

/// PK 付き table を持つ sync 可能な Database を作る (bisquit の store.rs と同じ作法)。
fn make_peer(path: &str, peer: u32) -> Arc<Database> {
    let mut b = Database::create_with_capacity(path, 65_536).unwrap();
    b.table(TABLE)
        .tag(COL_URL)
        .tag(COL_TITLE)
        .primary_key(COL_URL)
        .build()
        .unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(16 * 1024 * 1024).unwrap();
    db.engine().set_peer_id(peer);
    db
}

/// A の書き込みを transport に流し切る。
fn publish_all(db: &Database, syncer: &Syncer) {
    let eng = db.engine();
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    // 0.8.0: publish の primary は `_sync_ops`。 background 転送待ちに依存すると
    // 空振りするので明示的に回す (sleep より決定的)。
    eng.transfer_oplog_to_sync_ops();
    syncer.publish_since(Hlc::ZERO);
}

/// `table` 内で `url` に一致する row を数える。
fn count_by_url(db: &Database, url: &str) -> usize {
    db.get_table(TABLE)
        .expect("table exists")
        .where_eq(COL_URL, url)
        .count()
        .unwrap()
}

#[test]
fn same_pk_created_independently_converges_to_one_entity() {
    let pa = tmp("a");
    let pb = tmp("b");

    let db_a = make_peer(&pa, 1);
    let db_b = make_peer(&pb, 2);

    // 1. sync 前に、 A と B が **同じ URL の row を独立に** 作る。
    const URL: &str = "https://example.com/same-article";
    db_a.get_table(TABLE)
        .unwrap()
        .upsert()
        .set(COL_URL, URL)
        .set(COL_TITLE, "written on A")
        .commit()
        .unwrap();
    db_b.get_table(TABLE)
        .unwrap()
        .upsert()
        .set(COL_URL, URL)
        .set(COL_TITLE, "written on B")
        .commit()
        .unwrap();

    assert_eq!(count_by_url(&db_a, URL), 1, "作成直後の A は 1 row");
    assert_eq!(count_by_url(&db_b, URL), 1, "作成直後の B は 1 row");

    // 2. 相互 pull。
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let sync_a = Syncer::new(db_a.arc_engine(), transport.clone());
    let sync_b = Syncer::new(db_b.arc_engine(), transport.clone());

    publish_all(&db_a, &sync_a);
    publish_all(&db_b, &sync_b);

    sync_b.pull_once(1);
    sync_a.pull_once(2);

    db_a.engine().rebuild();
    db_b.engine().rebuild();

    // 3. 同じ PK なので、 両側とも 1 row に収束すべき。
    //    現状は `resolve_remote_eid` が PK を見ずに新規 eid を払い出すため 2 になる。
    let n_a = count_by_url(&db_a, URL);
    let n_b = count_by_url(&db_b, URL);
    assert_eq!(n_a, 1, "A: 同一 PK の row が {n_a} 個に増えている (#141)");
    assert_eq!(n_b, 1, "B: 同一 PK の row が {n_b} 個に増えている (#141)");

    // 4. 束ねた結果、 LWW が himo 単位で効いていること (title はどちらか一方に確定し、
    //    「両方消える」「片方だけ row が残る」にはならない)。
    let tbl_a = db_a.get_table(TABLE).unwrap();
    let rows_a = tbl_a.where_eq(COL_URL, URL).find().unwrap();
    assert_eq!(rows_a.len(), 1, "A: 1 row に収束していること");
    let title_a = tbl_a.entity(rows_a[0]).get(COL_TITLE);
    assert!(
        matches!(&title_a, Some(Value::Text(t)) if t == "written on A" || t == "written on B"),
        "A: title が LWW でどちらかに確定していること (got {title_a:?})",
    );

    drop(db_a);
    drop(db_b);
    cleanup(&pa);
    cleanup(&pb);
}

/// PK 無し table は従来どおり (bind pass が誤爆して別 entity を束ねない)。
#[test]
fn table_without_pk_still_allocates_separate_entities() {
    let pa = tmp("nopk_a");
    let pb = tmp("nopk_b");

    let make = |path: &str, peer: u32| -> Arc<Database> {
        let mut b = Database::create_with_capacity(path, 65_536).unwrap();
        b.table(TABLE).tag(COL_URL).tag(COL_TITLE).build().unwrap();
        b.enable_sync().unwrap();
        let db = b.finish_with_oplog(16 * 1024 * 1024).unwrap();
        db.engine().set_peer_id(peer);
        db
    };
    let db_a = make(&pa, 1);
    let db_b = make(&pb, 2);

    const URL: &str = "https://example.com/no-pk";
    db_a.get_table(TABLE).unwrap().insert().set(COL_URL, URL).commit().unwrap();
    db_b.get_table(TABLE).unwrap().insert().set(COL_URL, URL).commit().unwrap();

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let sync_a = Syncer::new(db_a.arc_engine(), transport.clone());
    let sync_b = Syncer::new(db_b.arc_engine(), transport.clone());
    publish_all(&db_a, &sync_a);
    publish_all(&db_b, &sync_b);
    sync_b.pull_once(1);
    db_b.engine().rebuild();

    // PK 宣言が無ければ「同じ値の別 row」は別 entity のまま (= 従来の意味論)。
    assert_eq!(
        count_by_url(&db_b, URL),
        2,
        "PK 無し table では独立 row は束ねられないこと",
    );

    drop(db_a);
    drop(db_b);
    cleanup(&pa);
    cleanup(&pb);
}

/// #141: PK は `.tables` sidecar の optional trailer に永続化され、 reopen 後も
/// engine 側に復元されること。 復元されないと reopen 後の apply が PK を見失って
/// また二重化する。
///
/// trailer 方式にしているのは前方互換のため — version は 1 のままなので、 #141 以前の
/// バイナリはこの block を無視して従来どおり開ける (PK が落ちるだけ)。
#[test]
fn pk_survives_reopen_via_tables_sidecar() {
    let path = tmp("pk_persist");

    let pk_himo = {
        let db = make_peer(&path, 1);
        let hid = db
            .engine()
            .table_pk_himo(TABLE)
            .expect("build 直後は PK が engine に載っていること");
        drop(db);
        hid
    };

    // reopen — sidecar から PK が戻ること。
    let db = Database::open_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    assert_eq!(
        db.engine().table_pk_himo(TABLE),
        Some(pk_himo),
        "reopen 後も PK himo が復元されること",
    );
    assert!(db.engine().is_pk_himo(pk_himo));

    drop(db);
    cleanup(&path);
}
