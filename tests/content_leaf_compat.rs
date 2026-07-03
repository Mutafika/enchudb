//! 0.9.0 content→Leaf 統合の互換テスト。
//!
//! content 系 API の保存先は旧 ContentStore region から `_c_{key}` Leaf himo
//! (bytes は vocab) に変わった。 API シグネチャは不変で、 挙動として:
//! - roundtrip / reopen 永続化が効く
//! - WAL recovery は TieNamed replay で復元する
//! - delete で content も消える (旧経路は残留していた = 監査 #79 M3)
//! - key 衝突しない (旧経路は fnv%16 で "memo"/"summary" 等が衝突 = #79 H12)

use enchudb::Engine;

fn tmp(name: &str) -> String {
    let p = format!("/tmp/enchudb-cleaf-{}-{}", name, std::process::id());
    for sfx in ["", ".oplog", ".lock", ".crc", ".tables", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{}{}", p, sfx));
    }
    p
}

#[test]
fn content_roundtrip_and_reopen() {
    let path = tmp("roundtrip");
    {
        let mut eng = Engine::create_standalone(&path).expect("create");
        let e = eng.entity();
        eng.content(e, "memo", b"hello world");
        eng.content(e, "data", &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(eng.get_content(e, "memo"), Some(b"hello world".as_ref()));
        assert_eq!(eng.get_content(e, "data"), Some([0xDE, 0xAD, 0xBE, 0xEF].as_ref()));
        assert_eq!(eng.get_content(e, "nope"), None);
        // 上書き (LWW 相当: 新しい tie が勝つ)
        eng.content(e, "memo", b"updated");
        assert_eq!(eng.get_content(e, "memo"), Some(b"updated".as_ref()));
        eng.flush().expect("flush");
    }
    // reopen しても読める (himo 定義は header/himoreg に永続)
    let eng = Engine::open_standalone(&path).expect("reopen");
    let e = 0u64; // 最初の entity
    assert_eq!(eng.get_content(e, "memo"), Some(b"updated".as_ref()));
    assert_eq!(eng.get_content(e, "data"), Some([0xDE, 0xAD, 0xBE, 0xEF].as_ref()));
}

/// 旧経路 (#79 H12): fnv1a(key) % 16 が同じ slot になる key 同士が上書きし合った。
/// "memo" と "summary" は旧実装で衝突するペア。 新経路では独立していること。
#[test]
fn colliding_keys_are_independent() {
    let path = tmp("collide");
    let mut eng = Engine::create_standalone(&path).expect("create");
    let e = eng.entity();
    eng.content(e, "memo", b"private");
    eng.content(e, "summary", b"public");
    assert_eq!(
        eng.get_content(e, "memo"),
        Some(b"private".as_ref()),
        "memo が summary に上書きされた (旧 mod-16 衝突の再発)"
    );
    assert_eq!(eng.get_content(e, "summary"), Some(b"public".as_ref()));
    // 17 個以上の key も入る (旧経路は 16 slot 固定だった)
    for i in 0..20 {
        let key = format!("k{i}");
        eng.content(e, &key, key.as_bytes());
    }
    for i in 0..20 {
        let key = format!("k{i}");
        assert_eq!(eng.get_content(e, &key), Some(key.as_bytes()));
    }
}

/// delete で content も消えること (旧経路は ContentStore::remove の呼び出しが
/// ゼロで、eid 再利用時に前世の content が新 entity に継承されていた)。
#[test]
fn delete_clears_content() {
    let path = tmp("delete");
    let mut eng = Engine::create_standalone(&path).expect("create");
    let e = eng.entity();
    eng.content(e, "secret", b"classified");
    assert!(eng.get_content(e, "secret").is_some());
    eng.delete(e);
    assert_eq!(
        eng.get_content(e, "secret"),
        None,
        "deleted entity の content が残留 (#79 M3 の再発)"
    );
}

/// concurrent + WAL: content_async が crash 相当 (flush せず drop → WAL recovery)
/// で復元されること。 復元は TieNamed replay 経由。
#[test]
fn content_async_survives_wal_recovery() {
    let path = tmp("walrec");
    drop(Engine::create_standalone(&path).expect("create"));
    let e;
    {
        let eng = Engine::open_concurrent_with_oplog(&path, 1 << 20).expect("open");
        e = eng.entity();
        eng.content_async(e, "blob", b"survives crash");
        eng.oplog_commit();
        eng.flush_writes();
        // graceful drop (最終 Commit + checkpoint) — recovery 経路は
        // v32_crash_recovery 系が subprocess kill で担保しているので、
        // ここでは「WAL 経由の非同期 content が reopen 後に読めること」を固定
    }
    let eng = Engine::open_concurrent_with_oplog(&path, 1 << 20).expect("reopen");
    assert_eq!(
        eng.get_content(e, "blob"),
        Some(b"survives crash".as_ref()),
        "content_async が reopen 後に消えた"
    );
}
