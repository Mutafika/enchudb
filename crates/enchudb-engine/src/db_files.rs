//! v10: DB directory 内の補助 file (sidecar) の名前と path helper。
//!
//! v9 まで sidecar は `{db}.tables` のように本体 file の**隣**に置いていたが、 v10 で本体が
//! directory になったので全部その**中**に入れる (`{db}/tables`)。 DB を動かす / 消す /
//! 写すのが `mv` / `rm -r` / `cp -r` 1 回で済み、 取りこぼしが起きない。
//!
//! 命名: sidecar は拡張子無しの短い名前、 本体 (segment) は `*.seg` と `himo/` / `ver/`
//! (`crate::segments::SegmentKind::rel_path`)。 両者は名前だけで区別できる。

use std::ffi::OsStr;
use std::io;
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
/// 各 segment の 「前回 flush 時点の file 長」 (`crate::segments::verify_manifest`)。
/// 未使用 region も 0 byte な v10 で、 切り詰めを stat だけで検出するための記録。
pub const SEGMENTS: &str = "segments";

/// segment 以外に DB directory へ置かれ得る file 名の一覧 (migrate / copy 用)。
pub const ALL: [&str; 8] = [OPLOG, TABLES, EIDMAP, VOCABMAP, CRC, LOCK, SCHEMA, SEGMENTS];

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

/// v9 以前の writer lock (`{db_file}.db.lock`)。 `remove_db` の掃除用。
fn legacy_lock_path_for(db_file: &str) -> PathBuf {
    PathBuf::from(format!("{db_file}.db.lock"))
}

/// DB を丸ごと消す。 v10 の directory も、 legacy (v9 以前) の単一 file + `{path}.oplog` 等の
/// sidecar も消す。 無ければ何もしない (Ok)。 example / test / tool の 「前回の残骸を掃除」 用。
pub fn remove_db(db_path: impl AsRef<Path>) -> io::Result<()> {
    let db = db_path.as_ref();
    match std::fs::symlink_metadata(db) {
        Ok(m) if m.is_dir() => std::fs::remove_dir_all(db)?,
        Ok(_) => std::fs::remove_file(db)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let s = db.to_string_lossy();
    for name in ALL {
        ignore_not_found(std::fs::remove_file(legacy_path_for(&s, name)))?;
    }
    ignore_not_found(std::fs::remove_file(legacy_lock_path_for(&s)))
}

fn ignore_not_found(r: io::Result<()>) -> io::Result<()> {
    match r {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

/// DB の disk 使用量 (bytes)。 `apparent` は見かけ (sparse の穴込み、 `ls -l` の合計)、
/// `physical` は実際に block を持つ分 (`du` 相当。 unix は `st_blocks * 512`、 それ以外は
/// apparent と同じ)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiskUsage {
    pub apparent: u64,
    pub physical: u64,
}

impl DiskUsage {
    pub fn apparent_mb(&self) -> f64 {
        self.apparent as f64 / (1024.0 * 1024.0)
    }
    pub fn physical_mb(&self) -> f64 {
        self.physical as f64 / (1024.0 * 1024.0)
    }
}

/// v10 directory (中身を再帰) または legacy 単一 file + sidecar の合計 disk 使用量。 無ければ 0。
pub fn disk_usage(db_path: impl AsRef<Path>) -> DiskUsage {
    let db = db_path.as_ref();
    let mut u = DiskUsage::default();
    accumulate_usage(db, &mut u);
    let s = db.to_string_lossy();
    for name in ALL {
        accumulate_usage(&legacy_path_for(&s, name), &mut u);
    }
    u
}

fn accumulate_usage(p: &Path, u: &mut DiskUsage) {
    let Ok(m) = std::fs::symlink_metadata(p) else { return };
    if m.is_dir() {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                accumulate_usage(&e.path(), u);
            }
        }
    } else {
        u.apparent += m.len();
        u.physical += physical_bytes(&m);
    }
}

#[cfg(unix)]
fn physical_bytes(m: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    m.blocks() * 512
}

#[cfg(not(unix))]
fn physical_bytes(m: &std::fs::Metadata) -> u64 {
    m.len()
}

/// sidecar を atomic に置き換える (tmp write → fsync → rename)。 **内容が現行と
/// 同じなら何も書かない** (#261)。
///
/// `tables` / `eidmap` / `vocabmap` / `schema` が同じ手順を踏むので 1 箇所に寄せてある。
/// tmp 名は `{sidecar}.tmp` (= sidecar ごとに別名) なので、 同時 persist しても
/// 互いの tmp を踏まない。
///
/// rename は **新しい inode** を置くので、 呼び出し側が chmod した mode は放っておくと
/// umask 由来 (典型的には 0644) に戻る。 consumer が DB を締めている前提を壊さないよう、
/// 置き換え前の mode を tmp に写してから rename する (無ければ umask のまま)。
///
/// #261: 「書き込みゼロで開いて閉じただけ」 の session でも `Database` / `Engine` の Drop が
/// sidecar を書き直しており、 その fsync (APFS で ~6〜8 ms) が 1 コマンド 1 process の
/// 消費側に定数として乗っていた。 dirty flag を各層に足す代わりに、 **書く直前に現行
/// file と読み比べて同一なら skip** する。 memory 上の flag と違って別 process が
/// 書き換えていた場合も検出でき、 「flag の立て忘れ = silent な永続化漏れ」 も起こらない。
/// sidecar はどれも小さく page cache に載っているので、 比較の read は fsync より 3 桁安い。
///
/// 戻り値は **実際に書いたか** (false = 内容一致で skip)。
#[cfg(not(target_arch = "wasm32"))]
pub fn write_atomic_if_changed(sidecar: &Path, bytes: &[u8]) -> io::Result<bool> {
    use std::io::Write;
    if is_unchanged(sidecar, bytes) {
        return Ok(false);
    }
    let tmp_path = tmp_path_for(sidecar);
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    inherit_mode(sidecar, &tmp_path);
    std::fs::rename(&tmp_path, sidecar)?;
    Ok(true)
}

/// 現行 sidecar が `bytes` と完全一致か。 不在 / 読めない / 長さ違いは false
/// (= 書く方に倒す)。 長さを先に見るので、 違う内容で read が走ることはまず無い。
#[cfg(not(target_arch = "wasm32"))]
fn is_unchanged(sidecar: &Path, bytes: &[u8]) -> bool {
    match std::fs::metadata(sidecar) {
        Ok(md) if md.len() == bytes.len() as u64 => {}
        _ => return false,
    }
    matches!(std::fs::read(sidecar), Ok(cur) if cur == bytes)
}

/// `from` が既にあればその mode を `to` に写す。 mode が取れない / 設定できない環境
/// (Windows、 権限不足) では黙って諦める — 内容の永続化を mode の都合で失敗させない。
#[cfg(not(target_arch = "wasm32"))]
fn inherit_mode(from: &Path, to: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(from) {
            let mode = md.permissions().mode() & 0o777;
            let _ = std::fs::set_permissions(to, std::fs::Permissions::from_mode(mode));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (from, to);
    }
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

    /// #261: 同一内容なら write ごと skip する (= fsync を払わない)。
    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn write_atomic_skips_identical_content() {
        let root = std::env::temp_dir().join(format!("enchu_sidecar_gate_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let sc = root.join(TABLES);

        // 不在 → 書く
        assert!(write_atomic_if_changed(&sc, b"abc").unwrap());
        assert_eq!(std::fs::read(&sc).unwrap(), b"abc");
        let mtime = std::fs::metadata(&sc).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));

        // 同一内容 → 書かない (mtime も動かない)
        assert!(!write_atomic_if_changed(&sc, b"abc").unwrap());
        assert_eq!(std::fs::metadata(&sc).unwrap().modified().unwrap(), mtime);
        // tmp を残さない
        assert!(!tmp_path_for(&sc).exists());

        // 長さ同じ / 中身違い → 書く (長さ比較だけで済ませていないことの確認)
        assert!(write_atomic_if_changed(&sc, b"abd").unwrap());
        assert_eq!(std::fs::read(&sc).unwrap(), b"abd");
        // 長さ違い → 書く
        assert!(write_atomic_if_changed(&sc, b"abdefg").unwrap());
        assert_eq!(std::fs::read(&sc).unwrap(), b"abdefg");

        // skip でも mode は維持される (書かないので当然だが、 契約として固定)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&sc, std::fs::Permissions::from_mode(0o600)).unwrap();
            assert!(!write_atomic_if_changed(&sc, b"abdefg").unwrap());
            assert_eq!(std::fs::metadata(&sc).unwrap().permissions().mode() & 0o777, 0o600);
            // 書いた場合も rename 先の mode を引き継ぐ
            assert!(write_atomic_if_changed(&sc, b"zzz").unwrap());
            assert_eq!(std::fs::metadata(&sc).unwrap().permissions().mode() & 0o777, 0o600);
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn remove_db_and_disk_usage_cover_dir_and_legacy_layout() {
        let root = std::env::temp_dir().join(format!("enchu_db_files_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        // v10 layout: directory
        let v10 = root.join("v10.db");
        std::fs::create_dir_all(v10.join("himo")).unwrap();
        std::fs::write(v10.join("header.seg"), vec![1u8; 4096]).unwrap();
        std::fs::write(v10.join("himo").join("0000.seg"), vec![2u8; 8192]).unwrap();
        let u = disk_usage(&v10);
        assert_eq!(u.apparent, 4096 + 8192);
        assert!(u.physical >= u.apparent, "physical {} < apparent {}", u.physical, u.apparent);
        remove_db(&v10).unwrap();
        assert!(!v10.exists());
        // legacy layout: file + sidecars
        let v9 = root.join("v9.db");
        let v9s = v9.to_string_lossy().to_string();
        std::fs::write(&v9, vec![0u8; 100]).unwrap();
        std::fs::write(legacy_path_for(&v9s, OPLOG), vec![0u8; 10]).unwrap();
        std::fs::write(legacy_lock_path_for(&v9s), b"").unwrap();
        assert_eq!(disk_usage(&v9).apparent, 110);
        remove_db(&v9).unwrap();
        assert!(!v9.exists() && !legacy_path_for(&v9s, OPLOG).exists() && !legacy_lock_path_for(&v9s).exists());
        // 無いものを消しても Ok
        remove_db(&v9).unwrap();
        assert_eq!(disk_usage(&v9), DiskUsage::default());
        let _ = std::fs::remove_dir_all(&root);
    }
}
