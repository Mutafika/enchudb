//! v10 の移行が **片道でない** ことの gate。
//!
//! v8 → v9 は writer open で in-place に stamp を刻む片道移行だったので、 知らずに
//! 新 binary を本物の DB に向けると戻せなくなった。 v10 は format 変更なので、 同じ性質
//! だと危険度が一段上がる。 実装は 「1 ファイル DB は open を **拒否** し、 明示 API
//! (`migrate_v9_to_v10`) で **別 path** へ写す」 だが、 それを assert で固定する:
//!
//! 1. `open` / `open_readonly` は 1 ファイル DB を Err で弾く
//! 2. **その時 src file は 1 byte も変わらない** (mtime も内容も)
//! 3. lock / sidecar を src の隣に作らない
//! 4. `migrate_v9_to_v10` の後も src は不変 (= 失敗したらやり直せる)

use enchudb_engine::{Engine, ValueType};
use std::collections::BTreeSet;
use std::path::Path;

fn digest(p: &Path) -> (u64, u64, u64) {
    let md = std::fs::metadata(p).unwrap();
    let bytes = std::fs::read(p).unwrap();
    // FNV-1a で十分 (改変検知が目的)。
    let mut h: u64 = 0xcbf29ce484222325;
    for b in &bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let mtime = md.modified().unwrap().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        as u64;
    (h, md.len(), mtime)
}

fn neighbors(dir: &Path, stem: &str) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let n = e.unwrap().file_name().to_string_lossy().to_string();
            n.starts_with(stem).then_some(n)
        })
        .collect()
}

#[test]
fn opening_a_single_file_db_rejects_without_touching_it() {
    let base = format!("/tmp/enchu_v10_legacy_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let dir_db = format!("{base}/src.db");
    let packed = format!("{base}/legacy.db");

    // v10 の DB を作って、 v9 互換の 1 ファイルに pack する (= 移行前の DB を模す)。
    let mut eng = Engine::create_with_capacity(&dir_db, 1024).unwrap();
    eng.define_table("widgets", 100).unwrap();
    eng.define_himo_in("widgets", "n", ValueType::Number, 100).unwrap();
    let eids: Vec<u64> = (0..5u32)
        .map(|i| {
            let e = eng.entity_in("widgets").unwrap();
            eng.tie(e, "widgets.n", 7 + i);
            e
        })
        .collect();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    Engine::pack_dir(&dir_db, Path::new(&packed)).unwrap();
    drop(eng);

    let p = Path::new(&packed);
    let before = digest(p);
    let before_neighbors = neighbors(Path::new(&base), "legacy.db");

    // 1 + 2 + 3: 拒否され、 src は不変で、 隣に何も生えない。
    for (label, err) in [
        ("open", Engine::open(&packed).err().map(|e| e.to_string())),
        ("open_readonly", Engine::open_readonly(&packed).err().map(|e| e.to_string())),
    ] {
        let err = err.unwrap_or_else(|| panic!("{label} が 1 ファイル DB を開いてしまった"));
        assert!(
            err.contains("single-file") || err.contains("migrate"),
            "{label}: 移行への誘導が無い: {err}"
        );
        assert_eq!(digest(p), before, "{label} が src file を書き換えた");
        assert_eq!(neighbors(Path::new(&base), "legacy.db"), before_neighbors,
            "{label} が src の隣に file を作った");
    }

    // 4: 明示移行は別 path に作り、 src は不変のまま。
    let migrated = format!("{base}/migrated.db");
    Engine::migrate_v9_to_v10(&packed, &migrated).unwrap();
    assert_eq!(digest(p), before, "migrate_v9_to_v10 が src file を書き換えた");
    assert_eq!(neighbors(Path::new(&base), "legacy.db"), before_neighbors,
        "migrate が src の隣に file を作った");

    let eng = Engine::open_readonly(&migrated).unwrap();
    assert_eq!(eng.entity_count(), 5);
    for (i, &e) in eids.iter().enumerate() {
        assert_eq!(
            eng.get(e, "widgets.n").map(|v| v.to_string()),
            Some((7 + i as u32).to_string()),
            "移行後に値が違う: eid={e}"
        );
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&base);
}
