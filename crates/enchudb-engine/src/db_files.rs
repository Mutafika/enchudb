//! v10: DB directory 内の補助 file (sidecar) の名前と path helper。
//!
//! v9 まで sidecar は `{db}.tables` のように本体 file の**隣**に置いていたが、 v10 で本体が
//! directory になったので全部その**中**に入れる (`{db}/tables`)。 DB を動かす / 消す /
//! 写すのが `mv` / `rm -r` / `cp -r` 1 回で済み、 取りこぼしが起きない。
//!
//! 命名: sidecar は拡張子無しの短い名前、 本体 (segment) は `*.seg` と `himo/` / `ver/`
//! (`crate::segments::SegmentKind::rel_path`)。 両者は名前だけで区別できる。

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// WAL (`enchudb_oplog::OpLog`)。
pub const OPLOG: &str = "oplog";
/// table 定義 (`TableDef` の binary encode)。
pub const TABLES: &str = "tables";
/// #9: eid 翻訳テーブル + foreign tombstone。
pub const EIDMAP: &str = "eidmap";
/// peer ごとの text vid 写像。
pub const VOCABMAP: &str = "vocabmap";
/// region CRC table (`integrity`)。
pub const CRC: &str = "crc";
/// writer 排他 (flock)。 中身は空。
pub const LOCK: &str = "lock";
/// `enchudb-schema` の schema 定義 (text)。
pub const SCHEMA: &str = "schema";

/// segment 以外に DB directory へ置かれ得る file 名の一覧 (migrate / copy 用)。
pub const ALL: [&str; 7] = [OPLOG, TABLES, EIDMAP, VOCABMAP, CRC, LOCK, SCHEMA];

/// HTTP bootstrap (`/bootstrap/{name}`) で本体と別に配る sidecar。 `.eidmap` 無しの
/// restore は再 sync で重複 entity / tombstone 喪失、 `.vocabmap` 無しは受信済み `Vocab`
/// の写像喪失を起こす (#78-H9)。
pub const BOOTSTRAP: [&str; 3] = [EIDMAP, TABLES, VOCABMAP];

/// `{db_dir}/{name}`。
pub fn path_for(db_path: impl AsRef<Path>, name: &str) -> PathBuf {
    db_path.as_ref().join(name)
}

/// atomic 書き換え用の一時 file (`{sidecar}.tmp`)。 rename で本体に置き換える。
pub fn tmp_path_for(sidecar: &Path) -> PathBuf {
    with_suffix(sidecar, ".tmp")
}

/// 破損 sidecar の退避先 (`{sidecar}.corrupt-{unix_ts}`)。 issue #52 の fail-readable。
pub fn corrupt_backup_path_for(sidecar: &Path, unix_ts: u64) -> PathBuf {
    with_suffix(sidecar, &format!(".corrupt-{unix_ts}"))
}

/// v9 以前の置き場 (`{db_file}.{name}`)。 `Engine::migrate_v9_to_v10` が旧 sidecar を拾う用。
pub fn legacy_path_for(db_file: &str, name: &str) -> PathBuf {
    PathBuf::from(format!("{db_file}.{name}"))
}

/// directory entry が DB 本体 (segment file か `himo/` / `ver/`) か。
pub fn is_segment_entry(name: &OsStr) -> bool {
    let s = name.to_string_lossy();
    s.ends_with(".seg") || s == "himo" || s == "ver"
}

/// directory entry を DB の複製に含めるか。 本体 + sidecar は含め、 `lock` (flock 中、
/// 複製先は開き直し側が取る) と `*.tmp` (書きかけ) は除く。
pub fn is_copyable_entry(name: &OsStr) -> bool {
    let s = name.to_string_lossy();
    if s == LOCK || s.ends_with(".tmp") {
        return false;
    }
    is_segment_entry(name) || ALL.contains(&s.as_ref()) || s.contains(".corrupt-")
}

fn with_suffix(p: &Path, sfx: &str) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(sfx);
    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_live_inside_the_db_dir() {
        assert_eq!(path_for("/x/a.db", TABLES), PathBuf::from("/x/a.db/tables"));
        assert_eq!(tmp_path_for(&path_for("/x/a.db", TABLES)), PathBuf::from("/x/a.db/tables.tmp"));
        assert_eq!(
            corrupt_backup_path_for(&path_for("/x/a.db", SCHEMA), 7),
            PathBuf::from("/x/a.db/schema.corrupt-7")
        );
        assert_eq!(legacy_path_for("/x/a.db", EIDMAP), PathBuf::from("/x/a.db.eidmap"));
    }

    #[test]
    fn entry_classification() {
        assert!(is_segment_entry(OsStr::new("header.seg")));
        assert!(is_segment_entry(OsStr::new("himo")));
        assert!(!is_segment_entry(OsStr::new("tables")));
        assert!(is_copyable_entry(OsStr::new("tables")));
        assert!(is_copyable_entry(OsStr::new("tables.corrupt-1")));
        assert!(!is_copyable_entry(OsStr::new("lock")));
        assert!(!is_copyable_entry(OsStr::new("tables.tmp")));
        assert!(!is_copyable_entry(OsStr::new("a.bootstrap.packed")));
    }
}
