//! issue #118: max_himos が schema 層から設定できず 256 決め打ち → schema が育つと
//! 無関係 table の open が「too many himos (max 256)」で巻き添え死。
//!
//! 修正方針 (実測で軌道修正):
//! - **default は 256 据え置き**。 himo 領域 = `max_himos × Column::region_size(max_entities)`
//!   = max_entities 比例の列領域を max_himos 倍する構造なので、 default を上げると 16M entity
//!   DB の apparent が ~16GB→~256GB (sparse だが macOS phys inflate) に膨れる。 全 DB default
//!   の引き上げは footprint 的に不可。
//! - 代わりに `GrowableOptions { max_himos, .. }` + `Database::create_growable_with` で
//!   **必要な consumer が明示 opt-in** で引き上げる (max_himos 含む全 layout knob を露出)。
//! - 天井エラーを actionable 化 (GrowableOptions で引き上げよ、 と案内)。
//!
//! ※ 固定 /tmp 併用の偽 flaky を避けるため path は pid+nanos で unique 化。

use enchudb_engine::ValueType;
use enchudb_schema::{Database, GrowableOptions};

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "enchudb-issue118-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// `GrowableOptions.max_himos` で天井を明示的に引き上げ / 縮小でき、 enforce される。
#[test]
fn create_growable_with_exposes_max_himos() {
    // (1) max_himos=8192 で旧 256 天井を超える 2000 himo を定義できる。
    let dir = tmp_dir("raise");
    let path = dir.join("db.ecdb");
    // max_entities は控えめにして apparent 肥大を抑える (8192 himo × 列領域)。
    let mut db = Database::create_growable_with(
        path.to_str().unwrap(),
        GrowableOptions { max_entities: 4096, max_himos: 8192, ..Default::default() },
    )
    .unwrap();
    let eng = db.engine_mut().unwrap();
    eng.define_table("t", 4096).unwrap();
    for i in 0..2000u32 {
        eng.define_himo_in("t", &format!("c{i}"), ValueType::Number, 0)
            .unwrap_or_else(|e| panic!("max_himos=8192 なのに himo {i} で失敗: {e}"));
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);

    // (2) max_himos=64 は enforce され、 エラーは actionable (max_himos に言及)。
    let dir2 = tmp_dir("small");
    let path2 = dir2.join("db.ecdb");
    let mut db2 = Database::create_growable_with(
        path2.to_str().unwrap(),
        GrowableOptions { max_entities: 4096, max_himos: 64, ..Default::default() },
    )
    .unwrap();
    let eng2 = db2.engine_mut().unwrap();
    eng2.define_table("t", 4096).unwrap();
    let err = (0..200u32)
        .find_map(|i| eng2.define_himo_in("t", &format!("c{i}"), ValueType::Number, 0).err());
    let msg = err.expect("max_himos=64 なら 200 定義で必ず上限に当たるはず");
    assert!(
        msg.contains("max_himos") || msg.contains("too many himos"),
        "actionable な message であるべき: {msg}"
    );
    drop(db2);
    let _ = std::fs::remove_dir_all(&dir2);
}

/// default は 256 据え置き（footprint 理由で raise しない）ことを固定する。
/// = default create で 256 を大きく超える himo は定義できず、 actionable に落ちる。
#[test]
fn default_max_himos_stays_conservative() {
    let dir = tmp_dir("default");
    let path = dir.join("db.ecdb");
    let mut db = Database::create_growable_with_capacity(path.to_str().unwrap(), 4096).unwrap();
    let eng = db.engine_mut().unwrap();
    eng.define_table("t", 4096).unwrap();
    // default が 4096 等に上がっていれば 400 は通ってしまう。 256 据え置きなら途中で落ちる。
    let err = (0..400u32)
        .find_map(|i| eng.define_himo_in("t", &format!("c{i}"), ValueType::Number, 0).err());
    let msg = err.expect("default(256) なら 400 定義で上限に当たるはず (raise されていない)");
    assert!(
        msg.contains("256") || msg.contains("max_himos"),
        "default 天井 256 の actionable message であるべき: {msg}"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
