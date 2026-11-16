//! sidecar (`tables` / `eidmap` / `vocabmap`) は tmp write → rename で置き換わるので、
//! **呼び出し側が chmod した mode が rename で消える** (新しい inode が umask 由来の
//! 0644 で生まれる)。 consumer (消費側の CLI) が open のたびに 0600 を掛け直しているのに
//! 書き直された sidecar だけ 0644 に戻っていた、 という実測から。
//!
//! v10 は sidecar が directory の中に増えるので、 面としても広がる。

#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn mode_of(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap_or_else(|e| panic!("{p:?}: {e}")).permissions().mode() & 0o777
}

#[test]
fn persist_keeps_the_mode_the_caller_set_on_sidecars() {
    let path = format!("/tmp/enchu_sidecar_mode_{}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);

    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("widgets", 100).unwrap();
    eng.define_himo_in("widgets", "n", ValueType::Number, 100).unwrap();
    let e0 = eng.entity_in("widgets").unwrap();
    eng.tie(e0, "widgets.n", 1);
    eng.flush().unwrap();
    eng.persist_tables().unwrap();

    // consumer が締める (消費側の CLI の restrict_global_db_perms 相当)。
    let dir = Path::new(&path);
    let sidecars: Vec<_> = ["tables", "eidmap", "vocabmap", "schema"]
        .iter()
        .map(|n| dir.join(n))
        .filter(|p| p.exists())
        .collect();
    assert!(sidecars.iter().any(|p| p.ends_with("tables")), "tables sidecar が無い");
    for p in &sidecars {
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(mode_of(p), 0o600);
    }

    // その後の書き込みで sidecar が書き直される。
    for i in 0..8u32 {
        let e = eng.entity_in("widgets").unwrap();
        eng.tie(e, "widgets.n", 100 + i);
    }
    eng.define_himo_in("widgets", "m", ValueType::Number, 100).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    drop(eng);

    let loosened: Vec<String> = sidecars
        .iter()
        .filter(|p| mode_of(p) != 0o600)
        .map(|p| format!("{} -> {:o}", p.file_name().unwrap().to_string_lossy(), mode_of(p)))
        .collect();
    let _ = std::fs::remove_dir_all(&path);
    assert!(loosened.is_empty(), "persist で mode が緩んだ sidecar: {loosened:?}");
}

/// #959 / review987 2 回目 [A]: 名前の記録 (`pktombs` / `pkformer` / `pkformer.log`) は、 書き出しの lock の外のまとめ直しが一時 file を
/// rename で置き換える。 呼び手が締めた mode をまとめ直しの後も保つ。 最初のまとめ直しが作る `pkformer` (file の無い DB の最初の
/// 書き出しは log だけ) は、 締めた log の mode を継ぐ。
#[test]
fn compacted_name_records_keep_the_mode_the_caller_set() {
    use enchudb_engine::db_files::{PKFORMER, PKFORMER_LOG, PKTOMBS, path_for};
    use std::os::unix::fs::MetadataExt;
    const ROWS: u32 = 512;
    let path = format!("/tmp/enchu_sidecar_mode_names_{}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    {
        let mut eng = Engine::create_with_cell_version(&path, 1 << 16).unwrap();
        eng.define_table("t", ROWS).unwrap();
        eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
        let key = eng.himo_id("t.key").unwrap() as u16;
        eng.set_table_pk("t", key).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(&path, 64 << 20).unwrap();
    eng.set_peer_id(1);
    let key = eng.himo_id("t.key").unwrap() as u16;
    let mut rows: Vec<u64> = (0..ROWS)
        .map(|i| {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, key, i);
            e
        })
        .collect();
    let mut names: Vec<u32> = (0..ROWS).collect();
    // 満杯の表で、 名前を i と i + ROWS の間で行き来させながら消して足す (同じ名前を何度も消す = `pktombs` がまとめ直す。 使い回すたびに
    // 前の住人の名前が identity に移る = `pkformer` が増える)。 30 回目に消した名前の削除の版数を、 後で忘れる線にする
    let mut line = None;
    let mut churn = |round: u32| {
        for i in 0..(ROWS / 2) as usize {
            let at = (i * 2 + round as usize % 2) % ROWS as usize;
            let gone = names[at];
            eng.delete(rows[at]);
            if round == 30 {
                line = eng.deleted_name_version(key, gone as u64);
            }
            let e = eng.entity_in("t").unwrap();
            names[at] = if names[at] < ROWS { names[at] + ROWS } else { names[at] - ROWS };
            eng.tie_to_by_id(e, key, names[at]);
            rows[at] = e;
        }
        eng.persist_sync_state().unwrap();
    };
    let files = [PKTOMBS, PKFORMER, PKFORMER_LOG].map(|n| path_for(&path, n));
    let ino = |p: &Path| std::fs::metadata(p).map(|m| m.ino()).ok();
    // 最初の書き出し (`pktombs` と log だけ) の後、 まとめ直しの前に締める
    churn(0);
    assert!(files[0].exists() && files[2].exists(), "前提: 最初の書き出しが pktombs と log を書いた");
    for p in [&files[0], &files[2]] {
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let before: Vec<Option<u64>> = files.iter().map(|p| ino(p)).collect();
    for round in 1..40u32 {
        churn(round);
        eng.compact_name_records_now();
    }
    // 線より前の削除を忘れる → `pkformer` + log が縮んでまとめ直す
    eng.forget_deletes_before(line.expect("前提: 30 回目に消した"));
    eng.persist_sync_state().unwrap();
    eng.compact_name_records_now();
    let replaced: Vec<bool> = files.iter().zip(&before).map(|(p, b)| ino(p).is_some() && ino(p) != *b).collect();
    let loosened: Vec<String> = files
        .iter()
        .filter(|p| p.exists() && mode_of(p) != 0o600)
        .map(|p| format!("{} -> {:o}", p.file_name().unwrap().to_string_lossy(), mode_of(p)))
        .collect();
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
    assert_eq!(replaced, [true; 3], "前提: まとめ直しが 3 つの file を置き換えた (pktombs / pkformer / pkformer.log)");
    assert!(loosened.is_empty(), "まとめ直しで mode が緩んだ名前の記録: {loosened:?}");
}
