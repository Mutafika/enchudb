//! sidecar (`tables` / `eidmap` / `vocabmap`) は tmp write → rename で置き換わるので、
//! **呼び出し側が chmod した mode が rename で消える** (新しい inode が umask 由来の
//! 0644 で生まれる)。 consumer (sinfo) が open のたびに 0600 を掛け直しているのに
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

    // consumer が締める (sinfo の restrict_global_db_perms 相当)。
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
