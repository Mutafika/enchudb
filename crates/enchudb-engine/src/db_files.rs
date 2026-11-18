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
/// #560: `eidmap` の後に変わった分の追記 (`crate::eidmap_log`)。 header が今の `eidmap` と合う時だけ読む。 読まない旧 binary は
/// `eidmap` だけを読む (きれいに閉じた後は追記が空なので全部ある)。
pub const EIDMAP_LOG: &str = "eidmap.log";
/// peer ごとの text vid 写像。
pub const VOCABMAP: &str = "vocabmap";
/// #513: 消した主キーの記録 (`crate::pk_tombs`)。
pub const PKTOMBS: &str = "pktombs";
/// #178: 翻訳の別名 (同じ entity の 2 つ目の世界番号、 `EidTranslator::alias`)。 `eidmap` と分けて置くので、 読まない旧 binary
/// は別名を知らないだけ (前と同じ 2 行のまま) で済む。
pub const EIDALIAS: &str = "eidalias";
/// 主キーの前の名前 (`crate::pk_former`)。 読まない旧 binary は今の名前でだけ束ねる。
pub const PKFORMER: &str = "pkformer";
/// #720: `pkformer` の後に変わった分の追記 (`crate::pk_former`)。 header が今の `pkformer` と合う時だけ読む。 読まない旧 binary は
/// `pkformer` だけを読む (きれいに閉じた後は追記が空なので全部ある)。
pub const PKFORMER_LOG: &str = "pkformer.log";
/// region CRC table (`integrity`)。
pub const CRC: &str = "crc";
/// writer 排他 (flock)。 中身は空。
pub const LOCK: &str = "lock";
/// `enchudb-schema` の schema 定義 (text)。
pub const SCHEMA: &str = "schema";
/// 各 segment の 「前回 flush 時点の file 長」 (`crate::segments::verify_manifest`)。
/// 未使用 region も 0 byte な v10 で、 切り詰めを stat だけで検出するための記録。
pub const SEGMENTS: &str = "segments";
/// 開く時間: きれいに閉じた時の Leaf の空き一覧 (`LeafStore::free_list_bytes`)。 紐名の辞書の印が `CLEAN_LEAF` の時だけ信じる
pub const LEAFFREE: &str = "leaf.free";

/// #415: create の途中の印。 mkdir の直後に置き、 作った中身を全部書き出してから消す。 これが残っている directory は、
/// 作る途中で落ちた残骸 (開けない、 create が片付けて作り直す)。 出来上がった DB には無いので [`ALL`] に入れない。
pub const CREATING: &str = "creating";

/// segment 以外に DB directory へ置かれ得る file 名の一覧 (migrate / copy 用)。
pub const ALL: [&str; 14] = [
    OPLOG, TABLES, EIDMAP, EIDMAP_LOG, EIDALIAS, VOCABMAP, PKTOMBS, PKFORMER, PKFORMER_LOG, CRC, LOCK, SCHEMA, LEAFFREE, SEGMENTS,
];

/// HTTP bootstrap (`/bootstrap/{name}`) で本体と別に配る sidecar。 `.eidmap` 無しの
/// restore は再 sync で重複 entity / tombstone 喪失、 `.vocabmap` 無しは受信済み `Vocab`
/// の写像喪失を起こす (#78-H9)。 `pktombs` 無しは、 それまでに消した主キーの行が、 後から届いた同じ主キーの
/// 古い行に当たらない (#513。 旧 server は 404 を返し、 受け手は無しで続ける)。 `eidalias` 無しは、 畳んだ行の世界番号宛ての
/// record が新しい行として払い出される (#178)。 `pkformer` 無しは、 改名を知らない peer の古い名前の行が改名した行に束ならない。
/// `eidmap` は `eidmap.log` を当てた形で配る (`Engine::eidmap_for_bootstrap`、 #560)。 log は配らない (古い受け手は取りに来ない、
/// 別々に読むと間にまとめ直しが入って合わなくなる)。 `pkformer` も同じく `pkformer.log` を当てた形で配る
/// (`Engine::pkformer_for_bootstrap`、 #720)。
pub const BOOTSTRAP: [&str; 6] = [EIDMAP, EIDALIAS, TABLES, VOCABMAP, PKTOMBS, PKFORMER];

/// `{db_dir}/{name}`。
pub fn path_for(db_path: impl AsRef<Path>, name: &str) -> PathBuf {
    db_path.as_ref().join(name)
}

/// atomic 書き換え用の一時 file (`{sidecar}.tmp`)。 rename で本体に置き換える。
pub fn tmp_path_for(sidecar: &Path) -> PathBuf {
    with_suffix(sidecar, ".tmp")
}

/// #959 / review987 [1]: 書き出しの lock の外のまとめ直しが作る一時 file (`{sidecar}.compact.tmp`)。 lock の下で書き直す
/// [`tmp_path_for`] と別の名前 (まとめ直しが lock の外で書いている間に、 lock の下の書き直しが同じ一時 file を踏まない)。
pub fn compact_tmp_path_for(sidecar: &Path) -> PathBuf {
    with_suffix(sidecar, ".compact.tmp")
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

/// #437: create が DB directory に置き得る名前か (segment / sidecar / `lock` / 作成中の印 / sidecar を書き換える
/// 一時 file `{sidecar}.tmp`・`segments.{pid}.{thread}.tmp`)。 これ以外の entry がある directory は、 create の残骸と
/// 見なさない (DB と無関係な directory を片付けない)。
pub fn is_create_entry(name: &OsStr) -> bool {
    let s = name.to_string_lossy();
    if is_segment_entry(name) || s == CREATING || ALL.contains(&s.as_ref()) {
        return true;
    }
    s.ends_with(".tmp") && ALL.iter().any(|n| s.strip_prefix(n).is_some_and(|rest| rest.starts_with('.')))
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
        enchudb_oplog::io_point("sidecar.write")?;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(bytes)?;
        enchudb_oplog::io_point("sidecar.sync")?;
        sync_file(&f, SyncKind::Durable)?;
        #[cfg(all(feature = "crashsim", unix))]
        enchudb_oplog::crashsim::file_synced(&tmp_path);
    }
    inherit_mode(sidecar, &tmp_path);
    enchudb_oplog::io_point("sidecar.rename")?;
    // 電源断の模擬: 置き換えで手放す旧 sidecar の控えを捨てる (inode の番号は次に作った file に渡る)
    #[cfg(all(feature = "crashsim", unix))]
    enchudb_oplog::crashsim::releasing(sidecar, || std::fs::rename(&tmp_path, sidecar))?;
    #[cfg(not(all(feature = "crashsim", unix)))]
    std::fs::rename(&tmp_path, sidecar)?;
    Ok(true)
}

/// [`write_atomic_if_changed`] の、 中身を `parts` の順につないだ形 (つないだ写しを作らない、 #959 / review987 [6])。 読み比べは
/// しない (いつも書く)。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn write_atomic_parts(sidecar: &Path, parts: &[&[u8]]) -> io::Result<()> {
    use std::io::Write;
    let tmp_path = tmp_path_for(sidecar);
    {
        enchudb_oplog::io_point("sidecar.write")?;
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(&tmp_path)?;
        for part in parts {
            f.write_all(part)?;
        }
        enchudb_oplog::io_point("sidecar.sync")?;
        sync_file(&f, SyncKind::Durable)?;
        #[cfg(all(feature = "crashsim", unix))]
        enchudb_oplog::crashsim::file_synced(&tmp_path);
    }
    inherit_mode(sidecar, &tmp_path);
    enchudb_oplog::io_point("sidecar.rename")?;
    #[cfg(all(feature = "crashsim", unix))]
    enchudb_oplog::crashsim::releasing(sidecar, || std::fs::rename(&tmp_path, sidecar))?;
    #[cfg(not(all(feature = "crashsim", unix)))]
    std::fs::rename(&tmp_path, sidecar)?;
    Ok(())
}

/// #884: `tables` の rename を disk へ送る (親の directory を素の `fsync`)。 他の sidecar は今のまま (呼ばない): checkpoint を進める
/// 前提にしているのは `tables` だけ (#857 / #870 / #881: 消した行・足した枠が sidecar にある間だけ WAL を畳んでよい)。 電源断で rename
/// が戻る file system (ext4 / xfs は rename の永続に directory の fsync が要る) では、 置いたはずの `tables` が前の中身に戻り、 その前提が
/// 崩れる。
///
/// macOS も F_FULLFSYNC (`File::sync_all`) にしない (1 回 5 ms 前後、 素の fsync は数 µs)。 順の根拠: 呼び手 (`persist_tables_file_with`)
/// はこれを `tables` を置いた直後・checkpoint を進める前に呼ぶ。 checkpoint は WAL の header にあり、 それを disk へ送るのは WAL の
/// 書き出し (`OpLog::fsync` = msync(MS_SYNC)、 どの OS でも素の書き出しで F_FULLFSYNC にしない) か OS の書き戻しで、 どちらも
/// checkpoint を進めた後 = この fsync より後に始まる。 = rename は header より先に drive へ送られる (WAL の record と header の順が
/// 頼っているのと同じ層の順。 drive の cache の中の順は、 WAL 自体も保証していない)。 Windows は directory を開けないので何もしない
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn sync_tables_dir(tables: &Path) -> io::Result<()> {
    match tables.parent() {
        Some(dir) => sync_dir_plain(dir, "tables.dir_sync"),
        None => Ok(()),
    }
}

/// directory `dir` を素の `fsync` (中の rename を disk へ送る。 `site` は I/O の点の名前)。 [`sync_tables_dir`] と同じ層の順の約束
/// (F_FULLFSYNC にしない)。 review946c [C]: `prev.seg` の 「書き出した周期」 (header) を進める前に、 使い回しの退避の載る sidecar
/// (`.eidmap` のまとめ直し・`eidalias`・`pktombs` / `pkformer` のまとめ直し = rename) を disk へ送る。 Windows は何もしない
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn sync_dir_plain(dir: &Path, site: &'static str) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        enchudb_oplog::io_point(site)?;
        let f = std::fs::File::open(dir)?;
        #[cfg(test)]
        sync_counts::issued(sync_counts::Issued::DirSync(site));
        // SAFETY: 開いている file の fd を渡すだけ
        if unsafe { libc::fsync(f.as_raw_fd()) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(not(unix))]
    let _ = (dir, site);
    Ok(())
}

/// #513: sidecar に追記して書き出す (`pktombs`)。 file が無ければ Err (追記は、 先にまとめて書いた file にだけ足す)。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn append_synced(sidecar: &Path, bytes: &[u8]) -> io::Result<()> {
    append_synced_in(sidecar, bytes, &mut SyncChain::default(), false)
}

/// #1018: [`append_synced`] の、 続けて書き出す sidecar の組 ([`SyncChain`]) の中の 1 つ。 `more` = この後に同じ組で書き出す
/// sidecar がある (それが disk まで書き出すので、 この追記は順の印だけでよい)。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn append_synced_in(sidecar: &Path, bytes: &[u8], chain: &mut SyncChain, more: bool) -> io::Result<()> {
    use std::io::Write;
    enchudb_oplog::io_point("sidecar.append")?;
    #[cfg(test)]
    sync_counts::fail_point(sidecar)?;
    let mut f = std::fs::OpenOptions::new().append(true).open(sidecar)?;
    f.write_all(bytes)?;
    enchudb_oplog::io_point("sidecar.append_sync")?;
    let done = sync_file(&f, if more { SyncKind::Ordered } else { SyncKind::Durable })?;
    #[cfg(all(feature = "crashsim", unix))]
    enchudb_oplog::crashsim::file_synced(sidecar);
    chain.wrote(sidecar, done);
    Ok(())
}

/// #1018: sidecar の書き出しの強さ。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SyncKind {
    /// disk に届くまで (macOS は fcntl の F_FULLFSYNC = drive の cache も吐かせる、 1 回 4〜5 ms。 他は `File::sync_all`)
    Durable,
    /// 順だけ (macOS の F_BARRIERFSYNC、 1 回 0.4 ms 前後): この file の中身は、 この後に disk へ送る物より先に disk に届く。 返った
    /// 時に届いたとは言えないので、 呼び手は同じ組の最後に [`SyncKind::Durable`] を 1 つ置く ([`SyncChain`])。 macOS の
    /// F_FULLFSYNC は、 前に書き出した物 (同じ device) も届けてから返る (fcntl(2) の F_FULLFSYNC / F_BARRIERFSYNC)。 順の印を
    /// 持たない OS は [`SyncKind::Durable`] で書き出す。 F_BARRIERFSYNC を受けない file system (APFS / HFS の外) は、 macOS 14 以降は
    /// kernel が F_FULLFSYNC に上げて呼び直し、 13 以前は ENOTTY / ENOTSUP / EINVAL を返すので [`sync_file`] が F_FULLFSYNC を出す。
    /// 順の印は drive の対応が要る (fcntl(2): 「Apple SSDs are guaranteed to provide」。 外付けの drive が受けない時の順は file system の
    /// 作り次第)
    Ordered,
}

/// #1018: `f` を `how` の強さで書き出し、 実際に書き出した強さを返す (順の印の無い OS / file system は `Durable`)。
///
/// macOS で F_BARRIERFSYNC が失敗した時、 disk まで書き出し直すのは file system が受けない時 (ENOTTY / ENOTSUP / EINVAL、 kernel が
/// F_FULLFSYNC に上げるのと同じ集合。 macOS 13 以前) だけ。 他の失敗 (EIO / ENOSPC …) は Err で返す: 書き戻しに失敗した page を
/// kernel が捨てていれば、 2 回目の書き出しは書く物が無いまま成功を返し、 失った中身を 「書けた」 と覚える (fsyncgate の形)。
/// EINTR は同じ syscall を出し直す (`File::sync_all` と同じ)。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn sync_file(f: &std::fs::File, how: SyncKind) -> io::Result<SyncKind> {
    #[cfg(target_vendor = "apple")]
    {
        if how == SyncKind::Ordered {
            match fcntl_sync(f, libc::F_BARRIERFSYNC) {
                Ok(()) => return Ok(SyncKind::Ordered),
                Err(e) if matches!(e.raw_os_error(), Some(libc::ENOTTY | libc::ENOTSUP | libc::EINVAL)) => {}
                Err(e) => return Err(e),
            }
        }
        fcntl_sync(f, libc::F_FULLFSYNC)?;
        Ok(SyncKind::Durable)
    }
    // 順の印の無い OS は、 どちらを頼まれても disk まで
    #[cfg(not(target_vendor = "apple"))]
    {
        let _ = how;
        #[cfg(test)]
        sync_counts::issued(sync_counts::Issued::Full);
        f.sync_all()?;
        Ok(SyncKind::Durable)
    }
}

/// #1018: macOS の書き出しの fcntl (`cmd` = F_FULLFSYNC / F_BARRIERFSYNC) を 1 つ出す。 EINTR は出し直す (`File::sync_all` と同じ)。
/// 試験は出した `cmd` と結果をここで数える (`sync_counts`)。
#[cfg(all(not(target_arch = "wasm32"), target_vendor = "apple"))]
fn fcntl_sync(f: &std::fs::File, cmd: libc::c_int) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    loop {
        #[cfg(test)]
        let injected = sync_counts::injected_error(cmd);
        #[cfg(not(test))]
        let injected: Option<i32> = None;
        let r = match injected {
            Some(e) => Err(io::Error::from_raw_os_error(e)),
            // SAFETY: 開いている file の fd を渡すだけ
            None if unsafe { libc::fcntl(f.as_raw_fd(), cmd) } == -1 => Err(io::Error::last_os_error()),
            None => Ok(()),
        };
        #[cfg(test)]
        sync_counts::issued_fcntl(cmd, r.as_ref().err().and_then(io::Error::raw_os_error));
        match r {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            r => return r,
        }
    }
}

/// #1018: 続けて書き出す sidecar の組 (`eidmap.log` → `pktombs` → `pkformer.log` …、 1 つの書き出しの lock の下) の書き出しを、
/// 後ろに続く物がある間は順の印だけ ([`SyncKind::Ordered`])、 最後の 1 つを disk まで ([`SyncKind::Durable`]) にする。 組の中の順
/// (前の file が後の file より先に disk に届く) と、 組を終えた時に全部が disk に届いていることは、 1 つずつ disk まで書き出す時と
/// 同じ。 前は 1 つずつ F_FULLFSYNC で、 場所を使い回し続ける名前のある表 (使い回しのたびに前の住人の名前を `pkformer` に移す、
/// #914) の `oplog_sync()` が 0.37.4 の 2 倍かかった (`pktombs` と `pkformer.log` で 2 回)。
///
/// 順の印で書き出した後に、 同じ組で disk まで書き出す物が続かなかった時 (続くはずの sidecar に書く物が無かった / 書けなかった) は、
/// [`Self::finish`] (呼ばずに落とした時は `Drop`) が最後に順の印で書き出した file を disk まで書き出す。
#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct SyncChain {
    /// 順の印だけで書き出し、 その後に disk まで書き出した物が無い file
    ordered: Option<PathBuf>,
}

#[cfg(not(target_arch = "wasm32"))]
impl SyncChain {
    /// `path` を `done` の強さで書き出した。 disk まで書き出したなら、 前に順の印で書き出した物も届いている。
    pub(crate) fn wrote(&mut self, path: &Path, done: SyncKind) {
        self.ordered = match done {
            SyncKind::Durable => None,
            SyncKind::Ordered => Some(path.to_path_buf()),
        };
    }

    /// 組の中で、 順の印を使わない書き出し (tmp + rename の `write_atomic_*`) で disk まで書き出した。 確かに書き出した時だけ呼ぶ
    /// (中身が同じで書かなかった時に呼ぶと、 前に順の印で書いた file を組の終わりに書き出さない)。
    pub(crate) fn wrote_durable(&mut self) {
        self.ordered = None;
    }

    /// 組を終える: 順の印だけで書き出した物が残っていれば、 disk まで書き出す。
    pub(crate) fn finish(&mut self) -> io::Result<()> {
        self.finish_with(true)
    }

    /// `sim` = I/O の点と電源断の模擬の控え (crashsim) を通す。 panic の巻き戻しの途中 (`Drop`) は通さない (控えの lock が毒されていると
    /// その中で panic し、 巻き戻しの中の panic で process が止まる)。
    fn finish_with(&mut self, sim: bool) -> io::Result<()> {
        let Some(path) = self.ordered.take() else { return Ok(()) };
        if sim {
            enchudb_oplog::io_point("sidecar.chain_sync")?;
        }
        let f = std::fs::File::open(&path)?;
        sync_file(&f, SyncKind::Durable)?;
        #[cfg(all(feature = "crashsim", unix))]
        if sim {
            enchudb_oplog::crashsim::file_synced(&path);
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SyncChain {
    /// 途中で失敗して抜けた組 (`?`) も、 順の印だけで書き出した物を disk まで書き出す (書き出した記録は memory で済んだ印になっている)。
    /// panic の巻き戻しの途中は、 書き出すだけで電源断の模擬の控えには触らない。
    fn drop(&mut self) {
        let _ = self.finish_with(!std::thread::panicking());
    }
}

/// #1018: 試験用。 この thread が出した書き出しの syscall の並びと、 追記を失敗させる file の名前・F_BARRIERFSYNC に返させる errno。
#[cfg(test)]
pub(crate) mod sync_counts {
    use std::cell::RefCell;
    use std::io;
    use std::path::Path;

    /// 出した書き出しの syscall。
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    #[cfg_attr(not(target_vendor = "apple"), allow(dead_code))]
    pub(crate) enum Issued {
        /// disk まで (macOS は fcntl の F_FULLFSYNC、 他は `File::sync_all`)
        Full,
        /// macOS の F_FULLFSYNC が失敗した (errno)
        FullFailed(i32),
        /// macOS の F_BARRIERFSYNC (順の印)
        Barrier,
        /// macOS の F_BARRIERFSYNC が失敗した (errno)
        BarrierFailed(i32),
        /// directory の素の fsync (I/O の点の名前)
        DirSync(&'static str),
    }

    thread_local! {
        static ISSUED: RefCell<Vec<Issued>> = const { RefCell::new(Vec::new()) };
        static FAIL: RefCell<Option<String>> = const { RefCell::new(None) };
    }
    #[cfg(target_vendor = "apple")]
    thread_local! {
        static BARRIER_ERRORS: RefCell<std::collections::VecDeque<i32>> = const { RefCell::new(std::collections::VecDeque::new()) };
    }

    pub(crate) fn issued(i: Issued) {
        ISSUED.with(|v| v.borrow_mut().push(i));
    }

    #[cfg(target_vendor = "apple")]
    pub(super) fn issued_fcntl(cmd: libc::c_int, err: Option<i32>) {
        issued(match (cmd == libc::F_BARRIERFSYNC, err) {
            (true, None) => Issued::Barrier,
            (true, Some(e)) => Issued::BarrierFailed(e),
            (false, None) => Issued::Full,
            (false, Some(e)) => Issued::FullFailed(e),
        });
    }

    /// この thread が出した syscall の並びを返して空にする。
    pub(crate) fn take() -> Vec<Issued> {
        ISSUED.with(|v| std::mem::take(&mut *v.borrow_mut()))
    }

    /// この thread の、 名前が `name` の sidecar への追記を失敗させる (None で外す)。
    pub(crate) fn fail_appends_to(name: Option<&str>) {
        FAIL.with(|f| *f.borrow_mut() = name.map(str::to_string));
    }

    pub(super) fn fail_point(path: &Path) -> io::Result<()> {
        let hit = FAIL.with(|f| f.borrow().as_deref().is_some_and(|n| path.file_name().is_some_and(|x| x == n)));
        if hit { Err(io::Error::other("sync_counts: 試験の失敗")) } else { Ok(()) }
    }

    /// この thread の次の F_BARRIERFSYNC を、 呼ばずに `errnos` を順に返す形にする (尽きたら本物を呼ぶ)。
    #[cfg(target_vendor = "apple")]
    pub(crate) fn fail_barriers_with(errnos: &[i32]) {
        BARRIER_ERRORS.with(|q| *q.borrow_mut() = errnos.iter().copied().collect());
    }

    #[cfg(target_vendor = "apple")]
    pub(super) fn injected_error(cmd: libc::c_int) -> Option<i32> {
        if cmd != libc::F_BARRIERFSYNC {
            return None;
        }
        BARRIER_ERRORS.with(|q| q.borrow_mut().pop_front())
    }
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
pub(crate) fn inherit_mode(from: &Path, to: &Path) {
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

/// #1018 の読み直し: `sync_file` が強さごとに出す syscall (試験は出した syscall を数える。 頼んだ強さでなく)。
#[cfg(all(test, not(target_arch = "wasm32")))]
mod sync_file_tests {
    use super::sync_counts::{self, Issued};
    use super::{SyncKind, sync_file};

    fn file(tag: &str) -> (std::path::PathBuf, std::fs::File) {
        use std::io::Write;
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let p = std::env::temp_dir().join(format!("enchudb-syncfile-{tag}-{}-{nanos}", std::process::id()));
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&p).unwrap();
        f.write_all(b"x").unwrap();
        (p, f)
    }

    /// disk まで = F_FULLFSYNC (macOS) / `sync_all` を 1 回。 順の印 = macOS は F_BARRIERFSYNC を 1 回 (他の OS は disk まで)。
    #[test]
    fn each_strength_issues_its_own_syscall() {
        let (p, f) = file("kinds");
        sync_counts::take();
        assert_eq!(sync_file(&f, SyncKind::Durable).unwrap(), SyncKind::Durable);
        assert_eq!(sync_counts::take(), vec![Issued::Full], "disk までの書き出しが F_FULLFSYNC / sync_all でない");
        let done = sync_file(&f, SyncKind::Ordered).unwrap();
        if cfg!(target_vendor = "apple") {
            assert_eq!(done, SyncKind::Ordered);
            assert_eq!(sync_counts::take(), vec![Issued::Barrier], "順の印が F_BARRIERFSYNC でない");
        } else {
            assert_eq!(done, SyncKind::Durable, "順の印の無い OS は disk まで書き出したと答える");
            assert_eq!(sync_counts::take(), vec![Issued::Full]);
        }
        let _ = std::fs::remove_file(&p);
    }

    /// F_BARRIERFSYNC を受けない file system (ENOTTY / ENOTSUP / EINVAL、 kernel が F_FULLFSYNC に上げるのと同じ集合) だけ disk まで書き出し
    /// 直す。 EINTR は F_BARRIERFSYNC を出し直す。 他の失敗 (EIO など) は書き出し直さずに Err (書き戻しに失敗した page を kernel が捨てて
    /// いれば、 2 回目の書き出しは成功を返してしまう)。
    #[cfg(target_vendor = "apple")]
    #[test]
    fn a_failed_barrier_falls_back_only_when_the_file_system_does_not_take_it() {
        let (p, f) = file("fallback");
        for e in [libc::ENOTTY, libc::ENOTSUP, libc::EINVAL] {
            sync_counts::take();
            sync_counts::fail_barriers_with(&[e]);
            assert_eq!(sync_file(&f, SyncKind::Ordered).unwrap(), SyncKind::Durable, "errno {e}: 受けない file system");
            assert_eq!(sync_counts::take(), vec![Issued::BarrierFailed(e), Issued::Full], "errno {e}: disk まで書き出し直さない");
        }
        for e in [libc::EIO, libc::ENOSPC, libc::EBADF] {
            sync_counts::take();
            sync_counts::fail_barriers_with(&[e]);
            let r = sync_file(&f, SyncKind::Ordered);
            assert_eq!(r.err().and_then(|x| x.raw_os_error()), Some(e), "errno {e}: 失敗を返さない");
            assert_eq!(sync_counts::take(), vec![Issued::BarrierFailed(e)], "errno {e}: 失敗の後に書き出し直した (fsyncgate の形)");
        }
        sync_counts::take();
        sync_counts::fail_barriers_with(&[libc::EINTR, libc::EINTR]);
        assert_eq!(sync_file(&f, SyncKind::Ordered).unwrap(), SyncKind::Ordered, "EINTR の後に順の印を出し直さない");
        assert_eq!(
            sync_counts::take(),
            vec![Issued::BarrierFailed(libc::EINTR), Issued::BarrierFailed(libc::EINTR), Issued::Barrier],
            "EINTR は F_BARRIERFSYNC を出し直す"
        );
        sync_counts::fail_barriers_with(&[]);
        let _ = std::fs::remove_file(&p);
    }
}
