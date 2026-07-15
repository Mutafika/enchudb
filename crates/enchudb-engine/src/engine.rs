//! v24 Engine — 量子円柱。単一ファイル。全コンポーネントが1つのmmapを共有。
//!
//!   entity() → ID 振る
//!   tie_text → 文字列を紐で張る（Vocabulary 経由）
//!   tie     → u32 値を紐で張る
//!   untie → 紐を外す
//!   content/get_content → 非索引テキスト
//!   query → 円柱の重なりを一発で返す
//!   delete → entity 削除
//!   commit → WAL Commit marker append (WAL 有効時のみ意味あり)
//!   open/flush → 永続化（mmap なので open は即利用可）

#[cfg(not(target_arch = "wasm32"))]
use std::fs::OpenOptions;
use std::io;

#[cfg(not(target_arch = "wasm32"))]
use memmap2::MmapMut;

// std::time::Instant panics on wasm32-unknown-unknown ("time not implemented").
// load_from_backing's [open_profile] timer is on the wasm read path, so alias
// Instant to the web-time shim there.
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

use crate::region::Region;

/// get_entity の戻り値。
#[derive(Debug, PartialEq)]
pub enum EntityValue<'a> {
    Num(u32),
    Text(&'a [u8]),
    Content(&'a [u8]),
}

/// v29: Engine の実行時状態スナップショット。
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub entity_count: u32,
    pub himo_count: u32,
    /// WAL の次 append 位置(byte offset)
    pub oplog_head: u64,
    /// 本体へ反映 + fsync 済みの位置
    pub oplog_checkpoint: u64,
    /// WAL ファイル容量(設定値、sparse)
    pub oplog_capacity: u64,
    /// head - checkpoint。大きいと未 fsync が溜まっている
    pub oplog_lag_bytes: u64,
    /// 発行済みの最大 LSN
    pub oplog_next_lsn: u64,
    /// fsync/msync 完了済みの LSN(背景 fsync が進めた地点)
    pub durable_lsn: u64,
    /// WriteQueue に滞留中の op 数
    pub queue_len: usize,
    /// writer が push した累計
    pub pushed: u64,
    /// consumer が apply した累計
    pub applied: u64,
    /// v32: 自 peer の peer_id(非 v32 では 0)
    pub peer_id: u32,
    /// v32: HlcStore の総エントリ数((eid, himo) で区別、非 v32 では 0)
    pub hlc_entries: usize,
    /// v32: HlcStore 内の最大 HLC(LWW 基準の最新書き込み時刻、非 v32 では None)
    pub max_hlc: Option<enchudb_oplog::Hlc>,
}

/// 0.8.16 (issue #54): vocab の orphan (= 死蔵 vid) 検出スナップショット。
/// `ValueType::Leaf` の値は `vocab.insert` (常に新 vid 払出) で書かれるため、
/// re-tie / remove で旧 vid が himo から外れても vocab data 側は残置する。
/// 同 vid を参照する live cell が皆無の vid を orphan として計上する。
///
/// 計測は read-only で、 vocab 自体は変更しない。 `Tag` himo (= `get_or_insert`
/// 経由で dedup) も live set の collection 対象 (= ある vid を Tag が参照中なら
/// Leaf が後から orphan にしても live 判定)。
#[derive(Debug, Clone)]
pub struct VocabOrphanStats {
    /// vocab に発行済みの全 vid 数 (= `Vocabulary::count()`)。
    pub vocab_total: u32,
    /// いずれかの himo cell から参照されている vid 数。
    pub live_vids: u32,
    /// `vocab_total - live_vids`。 死蔵の vid 数。
    pub orphan_vids: u32,
    /// orphan vid に対応する vocab data の総 byte 数 (= 救出可能領域)。
    pub orphan_bytes: u64,
    /// live vid に対応する vocab data の総 byte 数 (= 健全に使われてる領域)。
    pub live_bytes: u64,
}

impl VocabOrphanStats {
    /// `orphan_vids / vocab_total` の比率 (`vocab_total == 0` なら 0.0)。
    pub fn dead_ratio(&self) -> f64 {
        if self.vocab_total == 0 { 0.0 }
        else { self.orphan_vids as f64 / self.vocab_total as f64 }
    }
}

/// #88 (0.12.0): v5 (leaf region 無し = Leaf を vocab に格納) DB を v6
/// (LeafStore あり) へ移送した結果。 `Engine::migrate_bytes_v5_to_v6` 等が返す。
#[derive(Debug, Clone, Default)]
pub struct MigrationStats {
    /// 入力が既に v6 (leaf region あり) で移送不要だった。 他フィールドは 0。
    pub already_v6: bool,
    /// 移送対象になった Leaf himo 数。
    pub leaf_himos: u32,
    /// vocab → LeafStore に移した cell (= Leaf 値) の数。
    pub cells_moved: u64,
    /// 移した payload の総 byte 数 (slot header / padding は含まない生 bytes)。
    pub bytes_moved: u64,
    /// 移送後の LeafStore footprint (high_water、 byte)。
    pub leaf_footprint: u64,
    /// 移送後も vocab data に残る旧 Leaf bytes (= 死蔵)。 本 migration は
    /// vocab compaction をしないので、 この分は footprint に残る (既知の trade-off)。
    pub vocab_orphan_bytes_left: u64,
}

fn oplog_path_for(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.oplog", path))
}

/// β-light step 7: table 定義 metadata の sidecar path。 `.tables`。
/// 中身は binary encoded `TableDef` 配列、 atomic 書き換え (`.tables.tmp` →
/// rename) で更新。 v4 DB は sidecar なし、 open 時は anonymous fallback。
#[cfg(not(target_arch = "wasm32"))]
fn tables_path_for(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.tables", path))
}

/// β-light step 7: tables Vec を binary encode。
///
/// layout:
///   magic: "TBL1" (4)
///   version: u32 = 1
///   table_count: u32
///   per table:
///     name_len: u32
///     name: u8[name_len]
///     eid_range_lo: u32
///     eid_range_hi: u32
///     next_local: u32
///     himo_count: u32
///     himo_ids: u32[himo_count]
///     fk_count: u32
///     fk_refs: [(u32 himo_id, u32 (target_table as u32))] × fk_count
fn serialize_tables(tables: &[TableDef]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + tables.len() * 128);
    out.extend_from_slice(b"TBL1");
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&(tables.len() as u32).to_le_bytes());
    for t in tables {
        let name_bytes = t.name.as_bytes();
        out.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(name_bytes);
        out.extend_from_slice(&t.eid_range_lo.to_le_bytes());
        out.extend_from_slice(&t.eid_range_hi.to_le_bytes());
        out.extend_from_slice(&t.next_local.load(std::sync::atomic::Ordering::Relaxed).to_le_bytes());
        let himo_ids = t.himo_ids.read().unwrap();
        out.extend_from_slice(&(himo_ids.len() as u32).to_le_bytes());
        for &h in himo_ids.iter() {
            out.extend_from_slice(&h.to_le_bytes());
        }
        drop(himo_ids);
        out.extend_from_slice(&(t.fk_refs.len() as u32).to_le_bytes());
        for &(hid, tid) in &t.fk_refs {
            out.extend_from_slice(&hid.to_le_bytes());
            out.extend_from_slice(&(tid as u32).to_le_bytes());
        }
    }
    out
}

/// β-light step 7: tables sidecar を decode。 magic 不一致 / 短すぎる buffer
/// は Err。 部分破損は次の field 読みで失敗 → Err として扱う。
fn deserialize_tables(buf: &[u8]) -> Result<Vec<TableDef>, String> {
    if buf.len() < 12 || &buf[0..4] != b"TBL1" {
        return Err("tables sidecar: bad magic".into());
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if version != 1 {
        return Err(format!("tables sidecar: unsupported version {}", version));
    }
    let table_count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    // 破損ガード: 巨大 count をそのまま with_capacity に渡すと OOM abort。 各 table は
    // 最低 24 byte (name_len + lo/hi/next/himo_count/fk_count = 6×u32、 name 空) なので
    // 残りバッファで prealloc を cap する (truncation 自体は下の read_u32! が検出する)。
    let mut tables = Vec::with_capacity(table_count.min((buf.len() - 12) / 24));
    let mut off = 12usize;

    macro_rules! read_u32 {
        ($buf:expr, $off:expr) => {{
            if $off + 4 > $buf.len() {
                return Err("tables sidecar: truncated".into());
            }
            let v = u32::from_le_bytes($buf[$off..$off + 4].try_into().unwrap());
            $off += 4;
            v
        }};
    }

    for _ in 0..table_count {
        let name_len = read_u32!(buf, off) as usize;
        if off + name_len > buf.len() {
            return Err("tables sidecar: truncated (name)".into());
        }
        let name = String::from_utf8_lossy(&buf[off..off + name_len]).to_string();
        off += name_len;
        let eid_range_lo = read_u32!(buf, off);
        let eid_range_hi = read_u32!(buf, off);
        let next_local_u32 = read_u32!(buf, off);
        let next_local = std::sync::atomic::AtomicU32::new(next_local_u32);
        let himo_count = read_u32!(buf, off) as usize;
        let mut himo_ids = Vec::with_capacity(himo_count.min((buf.len() - off) / 4));
        for _ in 0..himo_count {
            himo_ids.push(read_u32!(buf, off));
        }
        let fk_count = read_u32!(buf, off) as usize;
        let mut fk_refs = Vec::with_capacity(fk_count.min((buf.len() - off) / 8));
        for _ in 0..fk_count {
            let hid = read_u32!(buf, off);
            let tid = read_u32!(buf, off);
            fk_refs.push((hid, tid as TableId));
        }
        tables.push(TableDef {
            name,
            himo_ids: std::sync::RwLock::new(himo_ids),
            eid_range_lo,
            eid_range_hi,
            fk_refs,
            next_local,
            free_locals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            free_locals_nonempty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
    }
    Ok(tables)
}

/// β-light step 7: tables を sidecar に atomic 書き換え。 fsync まで含む。
#[cfg(not(target_arch = "wasm32"))]
fn persist_tables_to_sidecar(db_path: &str, tables: &[TableDef]) -> io::Result<()> {
    use std::io::Write;
    let sidecar = tables_path_for(db_path);
    let tmp_path = sidecar.with_extension("tables.tmp");
    let bytes = serialize_tables(tables);
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, &sidecar)?;
    Ok(())
}

/// 0.8.15 (issue #52): persist 失敗で残った `.tables.tmp` を open 時に明示削除。
/// 通常 persist は `truncate(true)` で上書きするが、 disk full → recovery 後の
/// 状態を確実に clean にするためだけの safety net。 削除失敗は warning だけで続行。
#[cfg(not(target_arch = "wasm32"))]
fn cleanup_tables_tmp(db_path: &str) {
    let sidecar = tables_path_for(db_path);
    let tmp_path = sidecar.with_extension("tables.tmp");
    if tmp_path.exists() {
        if let Err(e) = std::fs::remove_file(&tmp_path) {
            eprintln!(
                "warning: failed to remove stale tables tmp {}: {}",
                tmp_path.display(),
                e
            );
        }
    }
}

/// 0.8.15 (issue #52): 破損 sidecar を `.tables.corrupt-<unix_ts>` に rename して
/// 退避し、 anonymous fallback で open 続行する。 user 側は schema crate の
/// synthesize 経路で engine 内 table 定義から復元できる。
#[cfg(not(target_arch = "wasm32"))]
fn rename_corrupt_sidecar(db_path: &str, kind: &str, err: &io::Error) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let sidecar = match kind {
        "tables" => tables_path_for(db_path),
        // 将来 schema 用に同 helper を使う場合のため switch (今は tables のみ)
        _ => return,
    };
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let backup = sidecar.with_extension(format!("{}.corrupt-{}", kind, ts));
    eprintln!(
        "warning: {} sidecar parse failed ({}): renaming to {} (anonymous fallback)",
        kind,
        err,
        backup.display()
    );
    if let Err(e) = std::fs::rename(&sidecar, &backup) {
        eprintln!(
            "warning: failed to rename corrupt sidecar {} -> {}: {}",
            sidecar.display(),
            backup.display(),
            e
        );
    }
}

/// β-light step 7: 既存 sidecar を読む。 不在 (v4 DB) なら Ok(None)。
#[cfg(not(target_arch = "wasm32"))]
fn load_tables_from_sidecar(db_path: &str) -> io::Result<Option<Vec<TableDef>>> {
    let sidecar = tables_path_for(db_path);
    match std::fs::read(&sidecar) {
        Ok(buf) => match deserialize_tables(&buf) {
            Ok(tables) => Ok(Some(tables)),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// #9: eid 翻訳テーブルの sidecar path。 `.eidmap`。 中身は
/// `(author_peer, foreign_local, local)` の binary 配列。 不在 (= sync してない DB
/// や旧 DB) なら open 時に空の translator で続行 (additive、 後方互換)。
#[cfg(not(target_arch = "wasm32"))]
fn eidmap_path_for(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.eidmap", path))
}

/// #9: eidmap sidecar の 1 entry。 `(author_peer, foreign_local, local, tombstone_hlc)`。
/// `tombstone_hlc == Hlc::ZERO` は「削除されていない」。 v2 (0.8.19) で tombstone を追加し、
/// reopen 後の削除済み entity 復活 (resurrection) を防ぐ。
type EidmapEntry = (enchudb_oplog::PeerId, u32, u32, enchudb_oplog::Hlc);

/// #9: 翻訳 entry を binary encode (v2)。
/// layout: magic "EIDM"(4) + version u32=2 + count u32
///         + (peer u32, foreign u32, local u32, tomb_wall u64, tomb_logical u32, tomb_peer u32) × count
fn serialize_eidmap(entries: &[EidmapEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + entries.len() * 28);
    out.extend_from_slice(b"EIDM");
    out.extend_from_slice(&2u32.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for &(peer, foreign_local, local, tomb) in entries {
        out.extend_from_slice(&peer.to_le_bytes());
        out.extend_from_slice(&foreign_local.to_le_bytes());
        out.extend_from_slice(&local.to_le_bytes());
        out.extend_from_slice(&tomb.wall.to_le_bytes());
        out.extend_from_slice(&tomb.logical.to_le_bytes());
        out.extend_from_slice(&tomb.peer.to_le_bytes());
    }
    out
}

/// #9: eidmap sidecar を decode。 magic 不一致 / truncated は Err。 v1 (tombstone 無し、
/// 12 byte/entry) と v2 (tombstone 込み、 28 byte/entry) の両方を読める。
fn deserialize_eidmap(buf: &[u8]) -> Result<Vec<EidmapEntry>, String> {
    if buf.len() < 12 || &buf[0..4] != b"EIDM" {
        return Err("eidmap sidecar: bad magic".into());
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let entry_size = match version {
        1 => 12usize,
        2 => 28usize,
        v => return Err(format!("eidmap sidecar: unsupported version {}", v)),
    };
    let count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    // 破損 / torn header ガード: `count` は信用しない。 各 entry は固定長なので上限は
    // 残りバッファで決まる。 bogus な巨大 count をそのまま with_capacity に渡すと数 GB の
    // 確保要求で open 時に abort する (.eidmap は CRC 無し)。 上限超過は破損とみなし Err
    // → load_eidmap_from_sidecar 側で空 translator に graceful fallback。
    let max_entries = (buf.len() - 12) / entry_size;
    if count > max_entries {
        return Err(format!(
            "eidmap sidecar: count {} exceeds buffer capacity {} (corrupt/torn)",
            count, max_entries
        ));
    }
    let mut entries = Vec::with_capacity(count);
    let mut off = 12usize;
    for _ in 0..count {
        if off + entry_size > buf.len() {
            return Err("eidmap sidecar: truncated".into());
        }
        let peer = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
        let foreign_local = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap());
        let local = u32::from_le_bytes(buf[off + 8..off + 12].try_into().unwrap());
        let tomb = if entry_size == 28 {
            let wall = u64::from_le_bytes(buf[off + 12..off + 20].try_into().unwrap());
            let logical = u32::from_le_bytes(buf[off + 20..off + 24].try_into().unwrap());
            let tpeer = u32::from_le_bytes(buf[off + 24..off + 28].try_into().unwrap());
            enchudb_oplog::Hlc { wall, logical, peer: tpeer }
        } else {
            enchudb_oplog::Hlc::ZERO
        };
        off += entry_size;
        entries.push((peer, foreign_local, local, tomb));
    }
    Ok(entries)
}

/// #9: eidmap を sidecar に atomic 書き換え (fsync 込み)。 entries 空なら何もしない
/// (= sync してない DB に空ファイルを作らない)。
#[cfg(not(target_arch = "wasm32"))]
fn persist_eidmap_to_sidecar(db_path: &str, entries: &[EidmapEntry]) -> io::Result<()> {
    use std::io::Write;
    if entries.is_empty() {
        return Ok(());
    }
    let sidecar = eidmap_path_for(db_path);
    let tmp_path = sidecar.with_extension("eidmap.tmp");
    let bytes = serialize_eidmap(entries);
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, &sidecar)?;
    Ok(())
}

/// #9: eidmap sidecar を読む。 不在なら Ok(None)。
#[cfg(not(target_arch = "wasm32"))]
fn load_eidmap_from_sidecar(db_path: &str) -> io::Result<Option<Vec<EidmapEntry>>> {
    let sidecar = eidmap_path_for(db_path);
    match std::fs::read(&sidecar) {
        Ok(buf) => match deserialize_eidmap(&buf) {
            Ok(entries) => Ok(Some(entries)),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// writer 排他用 sidecar の path。 `.db.lock`。
#[cfg(not(target_arch = "wasm32"))]
fn writer_lock_path_for(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.lock", path))
}

/// `oplog_record_queue` (= bounded ArrayQueue) への blocking push。
/// queue 満杯時は `yield_now` で consumer の進捗を待つ (issue4 backpressure)。
/// #77-M2: consumer 死亡 (poisoned) 時は無限待ちせず panic で失敗する。
/// push 成功時に `wal_push_count` を進める (flush_writes の WAL barrier 用)。
#[inline]
fn push_oplog_record_blocking(
    wq: &crossbeam_queue::ArrayQueue<enchudb_oplog::oplog::OwnedOp>,
    rec: enchudb_oplog::oplog::OwnedOp,
    poisoned: &std::sync::atomic::AtomicBool,
    wal_push_count: &std::sync::atomic::AtomicU64,
) {
    let mut rec = rec;
    loop {
        match wq.push(rec) {
            Ok(()) => {
                wal_push_count.fetch_add(1, std::sync::atomic::Ordering::Release);
                return;
            }
            Err(returned) => {
                if poisoned.load(std::sync::atomic::Ordering::Acquire) {
                    panic!("enchudb consumer thread has panicked — WAL record queue is dead (#77-M2)");
                }
                rec = returned;
                std::thread::yield_now();
            }
        }
    }
}

/// 同一プロセス内で writer lock を保持中の lock path の registry (#80)。
/// flock は open file description 単位なので、同一プロセスからの二重 open は
/// block 検知できず無期限ハングになる。flock に入る前にここで fast-fail する。
#[cfg(not(target_arch = "wasm32"))]
static WRITER_LOCK_REGISTRY: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

#[cfg(not(target_arch = "wasm32"))]
fn writer_registry() -> std::sync::MutexGuard<'static, std::collections::HashSet<std::path::PathBuf>>
{
    WRITER_LOCK_REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// writer lock の保持を表す guard。 drop で registry から抜け、 fd close で
/// flock も解放される。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct WriterLock {
    _file: std::fs::File,
    key: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for WriterLock {
    fn drop(&mut self) {
        writer_registry().remove(&self.key);
        // `_file` はこの後 drop され、 close で flock が解放される。
        // registry 解除 → close の順なので、 競合した同一プロセスの open は
        // fast-fail ではなく一瞬 flock で待ってから取得する (spurious error なし)。
    }
}

/// `.db.lock` に flock(LOCK_EX) を取り、 guard を返す。 guard が drop されると
/// lock も解放。 **別プロセス**が保持中は block する (= sqlite と同様、 取れる
/// まで待つ)。 **同一プロセス**が既に保持中は block せず即エラー (#80、
/// `ErrorKind::WouldBlock`)。 readonly open は呼ばない。 writer 系の
/// open / create だけ呼ぶ。
#[cfg(not(target_arch = "wasm32"))]
fn acquire_writer_lock(path: &str) -> io::Result<WriterLock> {
    use std::os::fd::AsRawFd;
    let lock_path = writer_lock_path_for(path);
    if let Some(parent) = lock_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    // 直前に create 済みなので canonicalize は通常成功する (symlink / 相対 path
    // の表記揺れで registry をすり抜けないための正規化)。
    let key = lock_path.canonicalize().unwrap_or(lock_path);
    if !writer_registry().insert(key.clone()) {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "\"{path}\" is already open for writing in this process \
                 (drop the existing Engine handle first, or use open_readonly)"
            ),
        ));
    }
    let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        let err = io::Error::last_os_error();
        writer_registry().remove(&key);
        return Err(err);
    }
    Ok(WriterLock { _file: f, key })
}

/// 0.9.0 (H11): create 系 API の既存ファイルガード。
/// 旧実装は `create(true).truncate(true)` で **既存 DB を無警告で 0 バイトに破壊**
/// していた (typo った path を create しただけで全損)。 FFI 契約
/// (`enchudb_create`: 「既存ファイルがあると Engine 側でエラー」) 通り、
/// 存在する path への create は `AlreadyExists` で拒否する。
/// 既存 DB を開くなら `Engine::open*`、 作り直すなら caller が明示的に削除すること。
#[cfg(not(target_arch = "wasm32"))]
fn ensure_create_target_absent(path: &str) -> io::Result<()> {
    if std::path::Path::new(path).exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "database file already exists: \"{path}\" — refusing to overwrite. \
                 use Engine::open* to open the existing DB, or remove the file first"
            ),
        ));
    }
    Ok(())
}

/// H11: 新規 DB ファイルを `create_new` (= O_EXCL) で開く。
/// `ensure_create_target_absent` の存在チェック〜open の間に他 process が
/// 同 path を作った TOCTOU race も OS レベルで `AlreadyExists` になる。
#[cfg(not(target_arch = "wasm32"))]
fn open_new_db_file(path: &str) -> io::Result<std::fs::File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| {
            if e.kind() == io::ErrorKind::AlreadyExists {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "database file already exists: \"{path}\" — refusing to overwrite \
                         (created concurrently?)"
                    ),
                )
            } else {
                e
            }
        })
}

/// `snapshot_export` の結果。どのファイルを書き出したか。
#[derive(Debug, Clone)]
pub struct SnapshotFiles {
    pub main: String,
    pub oplog: Option<String>,
    pub crc: Option<String>,
}

/// `audit()` に渡すフィルタ条件。None は「そのフィールドで絞らない」。
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// HLC 下限(inclusive)。これより古い record は除外。
    pub from_hlc: Option<enchudb_oplog::Hlc>,
    /// HLC 上限(inclusive)。これより新しい record は除外。
    pub to_hlc: Option<enchudb_oplog::Hlc>,
    /// 書き手 peer_id。これ以外の author を除外。
    pub author_peer: Option<enchudb_oplog::PeerId>,
    /// 署名者 pubkey の指紋(8B)。一致しない record は除外。
    pub pubkey_fp: Option<[u8; 8]>,
}

// ════════════════ バッキングストア ════════════════

/// mmap (native) または Vec<u8> (wasm/テスト) のどちらかを保持。
/// Engine が drop されるまでポインタが安定している。
enum Backing {
    #[cfg(not(target_arch = "wasm32"))]
    Mmap(MmapMut),
    /// growable mmap (虚仮アドレス予約 + 必要分だけ commit)。
    /// 空 DB が page サイズで始まり書き込みに応じて拡張する。
    /// 既存 `Mmap` バリアントは `set_len(huge)` で sparse な巨大ファイルを
    /// 作るが、 backup ツール (Time Machine, naive rsync) が apparent
    /// size でカウントすると詰まる。 `Growable` は実消費 = ファイル
    /// 論理サイズなので backup ツール的にも穏当。
    #[cfg(not(target_arch = "wasm32"))]
    Growable(std::sync::Arc<crate::growable_map::GrowableMap>),
    Memory(Vec<u8>),
}

impl Backing {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Mmap(m) => m.as_mut_ptr(),
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Growable(g) => g.base(),
            Backing::Memory(v) => v.as_mut_ptr(),
        }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Mmap(m) => &mut m[..],
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Growable(g) => unsafe {
                std::slice::from_raw_parts_mut(g.base(), g.committed())
            },
            Backing::Memory(v) => &mut v[..],
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn flush_to_disk(&self) -> io::Result<()> {
        match self {
            Backing::Mmap(m) => m.flush(),
            Backing::Growable(g) => g.flush(0, g.committed()),
            Backing::Memory(_) => Ok(()),
        }
    }

    /// 限定 byte range だけ msync。 clean-flag のような 16 byte 程度のメタ更新で
    /// 25 GB 全体を msync すると 70 ms 級になるので、 該当 page だけに絞る。
    /// offset / len は内部で page (4 KB) 境界に丸める。
    #[cfg(not(target_arch = "wasm32"))]
    fn flush_range(&self, offset: usize, len: usize) -> io::Result<()> {
        const PAGE: usize = 4096;
        let aligned_off = offset & !(PAGE - 1);
        let end = offset + len;
        let aligned_end = (end + PAGE - 1) & !(PAGE - 1);
        let aligned_len = aligned_end - aligned_off;
        match self {
            Backing::Mmap(m) => m.flush_range(aligned_off, aligned_len),
            Backing::Growable(g) => g.flush(aligned_off, aligned_len),
            Backing::Memory(_) => Ok(()),
        }
    }

    /// Growable backing なら GrowableMap への Arc を返す。 `Region::with_grower`
    /// で region を作り直したい後付けの init path 用 (例: `ensure_himo` で
    /// himo_slots 領域を lazy commit する)。
    #[cfg(not(target_arch = "wasm32"))]
    fn grower(&self) -> Option<std::sync::Arc<crate::growable_map::GrowableMap>> {
        match self {
            Backing::Growable(g) => Some(g.clone()),
            _ => None,
        }
    }

    /// 0.9.0 himo dynamic definition: `&self` 版 `as_mut_ptr`。
    /// `ensure_himo_dynamic` が Arc<Engine> 越しに新規 himo の column region を
    /// 作るために使う。 header_mut と同じ前提 (定義は himo_def_lock で直列化、
    /// 未公開 slot 領域にしか触れない)。
    fn base_ptr_shared(&self) -> *mut u8 {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Mmap(m) => m.as_ptr() as *mut u8,
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Growable(g) => g.base(),
            Backing::Memory(v) => v.as_ptr() as *mut u8,
        }
    }

    /// 0.9.0 himo dynamic definition: `&self` 版 `as_slice_mut`。
    /// header の value_types / max_values / himo_count / CRC を `&self` から
    /// 更新するために使う。 これらの byte は himo_def_lock 下でしか書かれず、
    /// reader は runtime にこの領域を読まない (open 時のみ) 前提。
    #[allow(clippy::mut_from_ref)]
    fn slice_mut_shared(&self) -> &mut [u8] {
        unsafe {
            match self {
                #[cfg(not(target_arch = "wasm32"))]
                Backing::Mmap(m) => {
                    std::slice::from_raw_parts_mut(m.as_ptr() as *mut u8, m.len())
                }
                #[cfg(not(target_arch = "wasm32"))]
                Backing::Growable(g) => std::slice::from_raw_parts_mut(g.base(), g.committed()),
                Backing::Memory(v) => {
                    std::slice::from_raw_parts_mut(v.as_ptr() as *mut u8, v.len())
                }
            }
        }
    }

    /// v32: &self 経由で header 領域に unsafe で書き込む。
    /// mmap 経由の並行書き込みは atomic スライス更新でガードされる前提。
    #[allow(clippy::mut_from_ref)]
    fn header_mut(&self) -> &mut [u8] {
        unsafe {
            match self {
                #[cfg(not(target_arch = "wasm32"))]
                Backing::Mmap(m) => {
                    let ptr = m.as_ptr() as *mut u8;
                    std::slice::from_raw_parts_mut(ptr, HEADER_SIZE)
                }
                #[cfg(not(target_arch = "wasm32"))]
                Backing::Growable(g) => {
                    let ptr = g.base();
                    std::slice::from_raw_parts_mut(ptr, HEADER_SIZE)
                }
                Backing::Memory(v) => {
                    let ptr = v.as_ptr() as *mut u8;
                    std::slice::from_raw_parts_mut(ptr, HEADER_SIZE)
                }
            }
        }
    }
}
use crate::append_vec::AppendVec;
use crate::vocabulary::Vocabulary;
use crate::entity_set::EntitySet;
use crate::himo_store::{HimoStore, ValueType};
use crate::content_store::ContentStore;
use crate::leaf_store::{LeafStore, cap_bytes_for_shift, MAX_OFF_SHIFT};
use crate::column::Column;

// ════════════════ ギャロッピング交差 ════════════════
// 旧 v24/v26 経路で使っていた。v27+ でも将来再利用の余地あり。

#[allow(dead_code)]
#[inline]
fn galloping_intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (small, big) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if small.is_empty() { return vec![]; }
    let mut result = Vec::with_capacity(small.len());
    let mut lo = 0usize;
    for &val in small {
        lo = gallop_ge(big, val, lo);
        if lo >= big.len() { break; }
        if big[lo] == val { result.push(val); lo += 1; }
    }
    result
}

#[allow(dead_code)]
#[inline]
fn gallop_ge(big: &[u32], val: u32, lo: usize) -> usize {
    let n = big.len();
    if lo >= n { return n; }
    if big[lo] >= val { return lo; }
    let mut step = 1usize;
    let mut hi = lo + step;
    while hi < n && big[hi] < val { step *= 2; hi = (lo + step).min(n); }
    let from = lo + step / 2;
    let to = hi.min(n);
    from + big[from..to].partition_point(|&x| x < val)
}

/// bitmap から set bit の entity ID を抽出。
#[allow(dead_code)]
#[inline]
fn extract_bitmap(bitmap: &[u64]) -> Vec<u32> {
    let mut result = Vec::new();
    for (i, &word) in bitmap.iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros();
            result.push((i * 64 + bit as usize) as u32);
            w &= w - 1;
        }
    }
    result
}

// ════════════════ ファイルレイアウト ════════════════

const FILE_MAGIC: [u8; 4] = *b"ECDB";
/// v5 (進行中): engine が table (行型) を認知する。 v4 DB は anonymous table
/// 1 個に migrate されて open 可能 (= 旧 flat 空間と同じ動作)。
///
/// v4: undo region 廃止 — `UndoLog` を撤去し、 `rollback()` API を削除。
/// undo は WAL 有効時には Commit で自動 clear される redundant な層であり、
/// standalone mode では `record()` 内の spin-wait が consumer 不在で
/// permanent hang を起こしていた (GitHub issue #1)。 v3 DB は再作成必要。
/// v7 (0.13.0, #90): LeafStore の cell offset を word 単位化 (16/32/64GB 選択可)。
/// v6 (0.12.0) は leaf offset が byte なので、 v6 region は self-describing な
/// `off_shift == 0` として v7 engine が read-through で扱う (migration 不要)。
const FILE_VERSION: u32 = 7;
/// v4 DB を後方互換で open する識別子。 v4 → v5 migrate は open 時透過。
const FILE_VERSION_LEGACY_V5: u32 = 5;
const FILE_VERSION_LEGACY_V4: u32 = 4;
/// v6 (0.12.0, #88): byte-offset LeafStore。 v7 engine が read-through で open
/// (leaf off_shift=0)。 v5→v6 migration の出力 version でもある。
const FILE_VERSION_LEGACY_V6: u32 = 6;
const HEADER_SIZE: usize = 4096;

const DEFAULT_MAX_ENTITIES: u32 = 16_777_216;
const DEFAULT_MAX_HIMOS: u32 = 256;
const DEFAULT_CYL_MAX_VALUES: u32 = 65536;
const DEFAULT_VOCAB_DATA_SIZE: usize = 512 * 1024 * 1024;
/// v6 (0.12.0, #88): Leaf payload 用 `LeafStore` の default 予約。 vocab と同等
/// (set_len sparse / growable lazy commit なので実 usage まで物理消費しない)。 tunable。
const DEFAULT_LEAF_DATA_SIZE: usize = 512 * 1024 * 1024;
/// v7 (0.13.0, #90): 新規 DB の LeafStore offset shift の default = 2 (16GB cap、
/// slot align4 で v6 と byte 等価)。
const DEFAULT_LEAF_OFF_SHIFT: u32 = 2;

/// v7 (#90): LeafStore region の addressable 上限を選ぶ (create 時)。 cell 参照を
/// word offset (`byte >> shift`) で持つことで、 列幅・indirection を増やさず cap を
/// 拡げる。 大きいほど slot alignment (= padding) が粗くなるので、 payload が小さい
/// 用途は `Gb16`、 wikipulse のような大 payload × 巨大 working set は `Gb32`/`Gb64`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafScale {
    /// shift 2 / align4 / ~16GB。 default。
    Gb16,
    /// shift 3 / align8 / ~32GB。
    Gb32,
    /// shift 4 / align16 / ~64GB。
    Gb64,
}

impl LeafScale {
    #[inline]
    pub fn off_shift(self) -> u32 {
        match self {
            LeafScale::Gb16 => 2,
            LeafScale::Gb32 => 3,
            LeafScale::Gb64 => 4,
        }
    }
    #[inline]
    pub fn cap_bytes(self) -> u64 { cap_bytes_for_shift(self.off_shift()) }
}

// ヘッダオフセット
const H_MAGIC: usize = 0;
const H_VERSION: usize = 4;
const H_MAX_ENTITIES: usize = 8;
const H_MAX_HIMOS: usize = 12;
const H_HIMO_COUNT: usize = 16;
const H_VOCAB_MAX_ENTRIES: usize = 20;
const H_VOCAB_INDEX_CAP: usize = 24;
const H_VOCAB_DATA_SIZE: usize = 28;  // u64
const H_HIMOREG_MAX_ENTRIES: usize = 36;
const H_HIMOREG_INDEX_CAP: usize = 40;
const H_HIMOREG_DATA_SIZE: usize = 44; // u64
const H_CONTENT_DATA_SIZE: usize = 52; // u64
const H_CYL_MAX_VALUES: usize = 60;
/// v28: ヘッダ整合性 CRC。[H_MAGIC..H_HEADER_CRC] の CRC32(FNV-1a)。
/// create/flush/define_himo 時に更新、open 時に検証。
const H_HEADER_CRC: usize = 64; // u32
/// v32: この DB を所有する peer の id。0 は「未設定 / single peer」。
/// CRC 保護外(後から set_peer_id で上書き可能)。
const H_PEER_ID: usize = 68; // u32
/// 72..76 は予約済み (v3 まで `H_UNDO_MAX_ENTRIES` が居た跡地)。 v4 で undo 全廃に
/// 伴って read/write 共に廃止、 オフセット安定のためスロットは空のまま (= 0)
/// を保つ。 後続フィールドを追加するなら 80.. を使うこと。
/// Backing kind flag (u32). 0 = EAGER (default, also legacy zero-fill), 1 = GROWABLE.
/// 用途: `validate_file_size` で auto-extend を許すか strict check するかの分岐のみ。
/// CRC 保護外。 accidental truncation 検出が目的、 adversarial tampering は対象外。
const H_BACKING_KIND: usize = 76; // u32
/// v6 (0.12.0, #88): LeafStore data region size (u64)。 0 = leaf region 無し
/// (pre-v6 DB)。 CRC 保護外 (H_PEER_ID / H_BACKING_KIND と同様、 破損は
/// try_from_params の checked arithmetic + u32::MAX assert で捕捉)。
const H_LEAF_DATA_SIZE: usize = 80; // u64

const BACKING_KIND_GROWABLE: u32 = 1;
const H_HIMO_TYPES: usize = 256;

// header 2048.. は旧 v0.4.0 までの NTupleView 永続化スロット (H_VIEW_COUNT /
// H_VIEWS_OFF + N × 17 bytes) の跡地。 issue #4 で NTupleTable / define_view を
// 撤去したのに伴い read / write 共に廃止。 file format は v4 のまま (header bytes
// は 0 で残置)、 既存 DB は何もせず読める。

fn align8(n: usize) -> usize { (n + 7) & !7 }

/// ヘッダ整合性 CRC を計算する対象領域。
/// magic, version, max_entities, max_himos, himo_count,
/// vocab_*, himoreg_*, content_data_size, cyl_max_values の固定レイアウト部のみ。
/// value_types/max_values 領域は runtime で変動するので CRC 範囲外。

#[inline]
fn compute_header_crc(buf: &[u8]) -> u32 {
    // [0..H_HEADER_CRC) 範囲を FNV-1a 32bit。
    // 固定レイアウトメタデータ(magic, version, max_entities, max_himos, himo_count,
    // vocab_*, himoreg_*, content_data_size, cyl_max_values)のみを対象。
    // 再現性デバッグ用。
    let mut h: u32 = 0x811c9dc5;
    for &b in &buf[0..H_HEADER_CRC] {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    let _dbg = (h, &buf[0..H_HEADER_CRC]);
    h
}

#[inline]
fn write_header_crc(buf: &mut [u8]) {
    let crc = compute_header_crc(buf);
    buf[H_HEADER_CRC..H_HEADER_CRC + 4].copy_from_slice(&crc.to_le_bytes());
}

/// ヘッダ CRC を検証。不一致なら Err。
fn verify_header_crc(buf: &[u8]) -> Result<(), String> {
    let stored = u32::from_le_bytes(buf[H_HEADER_CRC..H_HEADER_CRC + 4].try_into().unwrap());
    if stored == 0 { return Ok(()); }
    let computed = compute_header_crc(buf);
    if stored != computed {
        return Err(format!(
            "header CRC mismatch: stored={:08x}, computed={:08x} — file may be corrupt",
            stored, computed,
        ));
    }
    Ok(())
}

/// 0.9.0 (L1): header field の自己整合チェック。
/// `verify_header_crc` は stored CRC == 0 (v27 以前の legacy DB) を素通しするため、
/// 破損した himo_count / *_size / *_cap がそのまま layout 計算へ流れて panic /
/// OOB region を起こし得た。 ここで「header 自身の field 同士の関係」だけを検証
/// する (新しい定数上限は導入しない → 既存の正常 DB を誤って弾かない)。
fn sanity_check_header_fields(
    max_himos: u32, himo_count: u32,
    vocab_max_entries: u32, vocab_index_cap: u32, vocab_data_size: usize,
    himoreg_max_entries: u32, himoreg_index_cap: u32, himoreg_data_size: usize,
    content_data_size: usize,
) -> Result<(), String> {
    if himo_count > max_himos {
        return Err(format!(
            "himo_count {} exceeds max_himos {} — corrupt header", himo_count, max_himos,
        ));
    }
    // data_end は mmap 上の AtomicU32 (#77)。 u32::MAX 超は create 時点で拒否
    // されるので、 header にあれば破損。
    for (name, size) in [
        ("vocab_data_size", vocab_data_size),
        ("himoreg_data_size", himoreg_data_size),
        ("content_data_size", content_data_size),
    ] {
        if size > u32::MAX as usize {
            return Err(format!(
                "{} {} exceeds format limit {} (u32 data_end) — corrupt header",
                name, size, u32::MAX,
            ));
        }
    }
    // vocab / himoreg の index は cap-1 を hash mask に使う線形 probe
    // (Vocabulary::lookup)。 cap == 0 は即 OOB / 除算相当、 非 2^n は probe が
    // 全 slot を巡回できず無限 loop。 cap < max_entries は index が先に満杯に
    // なり insert が無限 probe。 create 経路は必ず next_power_of_two(>= entries)
    // で焼くので、 これらを満たさない header は破損。
    for (name, max_entries, index_cap) in [
        ("vocab", vocab_max_entries, vocab_index_cap),
        ("himoreg", himoreg_max_entries, himoreg_index_cap),
    ] {
        if max_entries == 0 || index_cap == 0 {
            return Err(format!(
                "{} max_entries {} / index_cap {} must be nonzero — corrupt header",
                name, max_entries, index_cap,
            ));
        }
        if !index_cap.is_power_of_two() {
            return Err(format!(
                "{} index_cap {} is not a power of two — corrupt header", name, index_cap,
            ));
        }
        if index_cap < max_entries {
            return Err(format!(
                "{} index_cap {} < max_entries {} — corrupt header", name, index_cap, max_entries,
            ));
        }
    }
    Ok(())
}

fn himo_maxv_base(max_himos: u32) -> usize {
    (H_HIMO_TYPES + max_himos as usize + 3) & !3
}

struct Layout {
    entities_off: usize,
    entities_size: usize,
    vocab_data_off: usize,
    vocab_data_size: usize,
    vocab_offsets_off: usize,
    vocab_offsets_size: usize,
    vocab_index_off: usize,
    vocab_index_size: usize,
    vocab_max_entries: u32,
    vocab_index_cap: u32,
    himoreg_data_off: usize,
    himoreg_data_size: usize,
    himoreg_offsets_off: usize,
    himoreg_offsets_size: usize,
    himoreg_index_off: usize,
    himoreg_index_size: usize,
    himoreg_max_entries: u32,
    himoreg_index_cap: u32,
    content_index_off: usize,
    content_index_size: usize,
    content_data_off: usize,
    content_data_size: usize,
    leaf_data_off: usize,
    leaf_data_size: usize,
    himo_base_off: usize,
    himo_col_size: usize,
    #[allow(dead_code)]
    himo_cyl_size: usize,
    himo_slot_size: usize,
    cyl_max_values: u32,
    total_size: usize,
}

impl Layout {
    fn compute(max_entities: u32, max_himos: u32, vocab_data_size: usize, content_data_size: Option<usize>, cyl_max_values: Option<u32>, leaf_data_size: Option<usize>) -> Self {
        let vocab_max_entries = max_entities.saturating_mul(16).min(256_000_000);
        Self::compute_with_caps(
            max_entities, max_himos,
            vocab_max_entries, vocab_data_size,
            content_data_size, cyl_max_values, leaf_data_size,
        )
    }

    /// `compute` のフル版 — vocab_max_entries も override できる。
    /// tiny preset で multiplier ×16 が過剰な場合に ×1 程度に絞る用。
    fn compute_with_caps(
        max_entities: u32,
        max_himos: u32,
        vocab_max_entries: u32,
        vocab_data_size: usize,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
        leaf_data_size: Option<usize>,
    ) -> Self {
        let vocab_index_cap = vocab_max_entries.next_power_of_two();
        let himoreg_max_entries = max_himos.max(256);
        let himoreg_index_cap = (himoreg_max_entries * 2).next_power_of_two();
        let himoreg_data_size = 64 * 1024;
        let content_data_size = content_data_size.unwrap_or_else(ContentStore::data_region_size);
        let cyl_max_values = cyl_max_values.unwrap_or(DEFAULT_CYL_MAX_VALUES);

        // #77: data_end (vocab / content) は mmap 上の AtomicU32 で、 index の
        // offset/len も u32。 4 GiB 超の data region を許すと append offset が
        // wrap して先頭から silent 上書きするため、 create 時点で拒否する。
        assert!(
            vocab_data_size <= u32::MAX as usize,
            "vocab_data_size {} exceeds format limit {} (u32 data_end)",
            vocab_data_size, u32::MAX,
        );
        assert!(
            content_data_size <= u32::MAX as usize,
            "content_data_size {} exceeds format limit {} (u32 data_end)",
            content_data_size, u32::MAX,
        );

        // v6 (#88): create 経路の leaf region 予約サイズ。 None = default。
        // Some(0) は「leaf region 無し」= v5 相当 DB (migration test / bench の
        // before 生成、 及び將来の pre-v6 互換 create に使う)。
        let leaf_data_size = leaf_data_size.unwrap_or(DEFAULT_LEAF_DATA_SIZE);
        Self::from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
        )
    }

    fn from_params(
        max_entities: u32, max_himos: u32,
        vocab_max_entries: u32, vocab_index_cap: u32, vocab_data_size: usize,
        himoreg_max_entries: u32, himoreg_index_cap: u32, himoreg_data_size: usize,
        content_data_size: usize, leaf_data_size: usize, cyl_max_values: u32,
    ) -> Self {
        // create 経路 (= プログラム引数由来の params) 用。 open 経路 (= disk header
        // 由来の params) は `try_from_params` を使い、 破損 header を InvalidData に
        // 落とすこと (0.9.0 L1)。
        Self::try_from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
        )
        .expect("layout size overflow — parameters too large")
    }

    /// 0.9.0 (L1): checked arithmetic 版。 header CRC==0 の legacy DB では
    /// 破損した size field がそのまま流れ込むため、 usize wrap で過小な
    /// total_size を計算 → OOB region を map する事故を Err で防ぐ。
    fn try_from_params(
        max_entities: u32, max_himos: u32,
        vocab_max_entries: u32, vocab_index_cap: u32, vocab_data_size: usize,
        himoreg_max_entries: u32, himoreg_index_cap: u32, himoreg_data_size: usize,
        content_data_size: usize, leaf_data_size: usize, cyl_max_values: u32,
    ) -> Result<Self, String> {
        // EntitySet::region_size 内部の `(max_entities + 7)` が u32 で wrap しない
        // ガード。 この値の DB は create 時点で作れない (debug では overflow panic)。
        if max_entities > u32::MAX - 7 {
            return Err(format!(
                "max_entities {} exceeds format limit — corrupt header?", max_entities,
            ));
        }
        // v7 (#90): LeafStore の high_water は word u32。 addressable 上限は
        // off_shift で決まる (16/32/64GB)。 layout は shift を知らないので、 ここでは
        // 絶対上限 (= MAX_OFF_SHIFT = 64GB) だけ弾く。 shift 個別の cap は create 側で
        // `LeafScale::cap_bytes` に対して検証する。
        let leaf_abs_cap = cap_bytes_for_shift(MAX_OFF_SHIFT);
        if leaf_data_size as u64 > leaf_abs_cap {
            return Err(format!(
                "leaf_data_size {} exceeds absolute format limit {} (64GB) — corrupt header?",
                leaf_data_size, leaf_abs_cap,
            ));
        }
        let ck_add = |a: usize, b: usize| -> Result<usize, String> {
            a.checked_add(b)
                .ok_or_else(|| "layout total_size overflow — corrupt header fields?".to_string())
        };

        // v3 layout: 固定上限の region 群を前に、 append-only な
        // variable region 群 (vocab_data / himoreg_data / content_data)
        // を末尾に集める。 これで「ファイル末尾のみ伸びる」 monotonic
        // grow が実現でき、 sparse hole に頼らずに apparent size を
        // 実 usage に追従させられる (Phase B Step 1)。
        let mut off = HEADER_SIZE;

        // ── 固定 cluster: 上限が max_entities / max_himos / *_cap で決まる ──
        let entities_off = off;
        let entities_size = align8(EntitySet::region_size(max_entities));
        off = ck_add(off, entities_size)?;

        let vocab_offsets_off = off;
        let vocab_offsets_size = align8(Vocabulary::offsets_region_size(vocab_max_entries));
        off = ck_add(off, vocab_offsets_size)?;

        let vocab_index_off = off;
        let vocab_index_size = align8(Vocabulary::index_region_size(vocab_index_cap));
        off = ck_add(off, vocab_index_size)?;

        let himoreg_offsets_off = off;
        let himoreg_offsets_size = align8(Vocabulary::offsets_region_size(himoreg_max_entries));
        off = ck_add(off, himoreg_offsets_size)?;

        let himoreg_index_off = off;
        let himoreg_index_size = align8(Vocabulary::index_region_size(himoreg_index_cap));
        off = ck_add(off, himoreg_index_size)?;

        let content_index_off = off;
        let content_index_size = align8(ContentStore::index_region_size_for(max_entities));
        off = ck_add(off, content_index_size)?;

        let himo_col_size = align8(Column::region_size(max_entities, 4));
        let himo_cyl_size = 0usize;
        let himo_slot_size = himo_col_size;

        let himo_base_off = off;
        let himo_total = himo_slot_size
            .checked_mul(max_himos as usize)
            .ok_or_else(|| "layout himo region overflow — corrupt header fields?".to_string())?;
        off = ck_add(off, himo_total)?;

        // ── Variable cluster (tail): append-only で伸びる region 群 ──
        let vocab_data_off = off;
        let vocab_data_size = align8(Vocabulary::data_region_size(vocab_data_size));
        off = ck_add(off, vocab_data_size)?;

        let himoreg_data_off = off;
        let himoreg_data_size = align8(Vocabulary::data_region_size(himoreg_data_size));
        off = ck_add(off, himoreg_data_size)?;

        let content_data_off = off;
        let content_data_size = align8(content_data_size);
        off = ck_add(off, content_data_size)?;

        // v6 (#88): Leaf payload store。 append-only variable cluster の末尾に追加。
        // size 0 (pre-v6 header) なら region 無し (off 不変)。
        let leaf_data_off = off;
        let leaf_data_size = align8(leaf_data_size);
        off = ck_add(off, leaf_data_size)?;

        Ok(Layout {
            entities_off, entities_size,
            vocab_data_off, vocab_data_size,
            vocab_offsets_off, vocab_offsets_size,
            vocab_index_off, vocab_index_size,
            vocab_max_entries, vocab_index_cap,
            himoreg_data_off, himoreg_data_size,
            himoreg_offsets_off, himoreg_offsets_size,
            himoreg_index_off, himoreg_index_size,
            himoreg_max_entries, himoreg_index_cap,
            content_index_off, content_index_size,
            content_data_off, content_data_size,
            leaf_data_off, leaf_data_size,
            himo_base_off, himo_col_size, himo_cyl_size, himo_slot_size,
            cyl_max_values,
            total_size: off,
        })
    }

    fn himo_col_off(&self, hid: usize) -> usize {
        self.himo_base_off + hid * self.himo_slot_size
    }
    #[allow(dead_code)]
    fn himo_cyl_a_off(&self, hid: usize) -> usize {
        self.himo_base_off + hid * self.himo_slot_size + self.himo_col_size
    }
    #[allow(dead_code)]
    fn himo_cyl_b_off(&self, hid: usize) -> usize {
        self.himo_base_off + hid * self.himo_slot_size + self.himo_col_size + self.himo_cyl_size
    }
}

// ════════════════ Engine ════════════════

// 旧 v0.4.0 まで居た `NTupleEntry` / `NTupleTable` (n-tuple 観測窓) は issue #4 で
// 撤去。 schema / SQL / RAG / sync / transport いずれの crate からも参照されておらず
// (engine 内テストだけが叩いていた)、 query 経路の \"best_lookup_ref\" branch と
// tie / untie / apply_op / open の hook と合わせて約 350 行の dead weight だった。
// 過去の API は `Engine::define_view` で、 0.4.0 までの DB で永続化された
// header H_VIEW_COUNT は今は無視される (zero 残置で害なし)。

// ════════════════ Table (β-light step 2: 内部 data のみ) ════════════════
//
// 「entity に tie する」 = 「行に値を入れる」 ことの自己認識を engine に
// 与えるための table 概念。 各 table は himo set (列) + eid_range (行範囲)
// を持つ。 step 2 では anonymous table (id=0) 1 個だけが存在し、 全 himo /
// 全 entity が自動でここに属する (旧 API 完全互換)。
//
// step 3+ で define_table / entity_in API を公開し、 非 anonymous table の
// 切り出し + Ref validation + table-local positions へ進む。

/// Table 識別子。 u16 で 65536 table までを表現 (実用上十分)。
/// `ANONYMOUS_TABLE` (= 0) は engine 起動時に必ず存在する default table。
pub type TableId = u16;
pub const ANONYMOUS_TABLE: TableId = 0;

/// 1 つの table の定義。 名前 (anonymous は空文字)、 所属 himo の id 列、
/// eid_range (anonymous は open-ended)、 FK 参照 (Ref himo の target table)。
///
/// step 2 段階では engine 内部にしか露出せず、 一部 field は step 3+ で
/// 使われる。 dead code 警告は段階実装の意図的な姿。
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct TableDef {
    /// table 名 (anonymous は ""、 user 定義 table は固有名)。
    pub name: String,
    /// この table の column 軸を成す himo の id 列 (engine.himos の index)。
    /// 0.9.0: `&self` の himo 定義 (`ensure_himo_dynamic_in`) が attach できる
    /// よう RwLock 化。 write は himo_def_lock 下でのみ発生する低頻度 path。
    pub himo_ids: std::sync::RwLock<Vec<u32>>,
    /// この table が占有する eid 範囲の下限 (inclusive)。
    pub eid_range_lo: u32,
    /// 上限 (exclusive)。 `u32::MAX` は open-ended (= まだ後続 table が
    /// 切られていない、 次の entity 確保で伸びる)。 後続 table が定義された
    /// 瞬間に現在の next_eid に固定される。
    pub eid_range_hi: u32,
    /// Ref himo の target table 一覧 (himo_id, target_table_id)。
    /// step 5 で validation に使う。
    pub fk_refs: Vec<(u32, TableId)>,
    /// 次に entity_in で割り当てる eid の table 内 local offset。
    /// global eid = eid_range_lo + next_local。 anonymous table は
    /// `entities.next_eid()` (= EntitySet 直) を使うのでこの field は不参照。
    ///
    /// 0.7.0: `&self entity_in` を支えるため AtomicU32 化。 schema crate
    /// (= Arc<Engine> 経由の concurrent mode) で row insert する path が
    /// CAS で並行 safe に払出できる。
    pub next_local: std::sync::atomic::AtomicU32,
    /// 0.8.0: free list = reclaim で解放された local id の reservoir。
    /// `entity_in(table)` は free list が non-empty なら pop で再利用、
    /// 空なら通常通り `next_local.fetch_add(1)`。 `_sync_ops` の長期運用で
    /// eid 空間飽和を防ぐ (= ring buffer 化の本体)。 user table は今のところ
    /// 自動 reclaim path がないので free list は空のまま。
    pub free_locals: std::sync::Arc<std::sync::Mutex<Vec<u32>>>,
    /// 0.8.0: `free_locals` が non-empty かどうかの fast path flag。 user table
    /// (= 自動 reclaim なし) では常に false、 `entity_in` の hot path で mutex を
    /// 取らずに済む。 reclaim が push したら true に上げる、 entity_in が pop
    /// で空になったら false に戻す。 厳密な race は entity_in 内で mutex 取った
    /// あと再 check するので OK (= AtomicBool は fast path の hint)。
    pub free_locals_nonempty: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Clone for TableDef {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            himo_ids: std::sync::RwLock::new(self.himo_ids.read().unwrap().clone()),
            eid_range_lo: self.eid_range_lo,
            eid_range_hi: self.eid_range_hi,
            fk_refs: self.fk_refs.clone(),
            next_local: std::sync::atomic::AtomicU32::new(
                self.next_local.load(std::sync::atomic::Ordering::Relaxed)
            ),
            // free_locals は Arc なので share される (= clone は same reservoir を
            // 指す)。 これは TableDef::clone を「meta read 用 snapshot」 として
            // 使う既存 caller の semantic と整合 (= 並列 race しないように caller
            // 側で coordinate する想定)。
            free_locals: self.free_locals.clone(),
            free_locals_nonempty: self.free_locals_nonempty.clone(),
        }
    }
}

impl TableDef {
    /// 起動時の anonymous table。 全 himo / entity の default 受け皿。
    /// `eid_range_hi == u32::MAX` の間は open-ended (旧 API で `entity()` が
    /// 呼べる)、 `define_table` で初めて非 anon table が切られた瞬間に
    /// 現 `next_eid` で閉じる。
    pub fn anonymous() -> Self {
        Self {
            name: String::new(),
            himo_ids: std::sync::RwLock::new(Vec::new()),
            eid_range_lo: 0,
            eid_range_hi: u32::MAX,
            fk_refs: Vec::new(),
            next_local: std::sync::atomic::AtomicU32::new(0),
            free_locals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            free_locals_nonempty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// 0.7.0: 「reserved table」 = engine / schema 層の internal 用 table。
    /// 命名規約: `_` で始まる名前 (例: `_schema_meta` / `_sync_ops` / `_sync_peers`)。
    /// user 視点では不可視 (= `list_user_tables` から除外、 schema crate も
    /// 公開 API では reject)。 sidecar / wire format に flag は持たず、 名前
    /// だけで判定するので 0.5.0 / 0.6.0 sidecar との forward-compat を維持。
    #[inline]
    pub fn is_reserved(&self) -> bool {
        self.name.starts_with('_')
    }
}

/// 0.7.0: reserved table 命名規約 (= `_` で始まる) を確認する helper。
/// `define_reserved_table` で公開 API に出すバリデーション、 schema crate からも
/// reject 用に呼ばれる。
#[inline]
pub fn is_reserved_table_name(name: &str) -> bool {
    name.starts_with('_')
}

/// `define_table` で size_hint=0 を渡した時の default。 SNS scale (10M+) では
/// 明示指定推奨、 embedded scale (10k-1M) では default で足りる。
pub const DEFAULT_TABLE_RESERVED: u32 = 1_000_000;

pub struct Engine {
    #[allow(dead_code)]
    path: String,
    layout: Layout,
    max_entities: u32,
    max_himos: u32,
    vocab: Vocabulary,
    himo_reg: Vocabulary,
    // 0.9.0 himo dynamic definition: himo の並列配列は AppendVec (固定 capacity
    // + append-only + lock-free read) 化して `&self` から定義追加できるように
    // した (design a: pre-sized slots + atomic len publish)。 reader の hot path
    // (`self.himos[hid]` 等の indexed read) は lock を取らない。
    // 定義追加は `himo_def_lock` で直列化される。
    himo_names: AppendVec<String>,
    value_types: AppendVec<ValueType>,
    himo_max_values: AppendVec<u32>,
    himos: AppendVec<HimoStore>,
    /// β-light step 2: engine が認知する table 一覧。 index 0 は常に
    /// anonymous table (旧 API は全部ここに dispatch)。 step 3+ で
    /// define_table 時に push される。
    tables: Vec<TableDef>,
    /// β-light step 2: himo_id → 所属 table_id の逆引き (himos と同じ index)。
    /// 旧 API (`define_himo`) で追加された himo は全部 ANONYMOUS_TABLE。
    /// 0.9.0: `&self` の define_himo_in 相当 (`ensure_himo_dynamic_in`) が
    /// attach 先を後書きできるよう要素を AtomicU16 (= TableId) にした。
    himo_to_table: AppendVec<std::sync::atomic::AtomicU16>,
    /// 0.9.0: himo 定義 (`ensure_himo_dynamic` 系) の直列化 lock。
    /// 並列配列 5 本 + tables[].himo_ids + header write を 1 定義単位で
    /// atomic に見せる。 read path はこの lock を取らない。
    himo_def_lock: std::sync::Mutex<()>,
    entities: EntitySet,
    contents: ContentStore,
    /// v6 (0.12.0, #88): Leaf 終端ノードの可変長 payload store。 vocab から剥がした
    /// reclaim 対応 store。 pre-v6 DB (leaf region 無し) は None。
    leaf: Option<LeafStore>,
    /// 非同期書き込みキュー。`create_concurrent` で有効化される。
    write_queue: Option<std::sync::Arc<crate::write_queue::WriteQueue>>,
    /// consumer スレッドへの shutdown 通知。`Drop` で true に。
    shutdown_flag: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// consumer スレッドハンドル。Drop で join。
    consumer_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// writer が push した累積件数。tie_async/untie_async/delete_async で +1。
    push_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// consumer が apply 完了した累積件数。apply_count >= push_count が同期点。
    apply_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// v28 WAL。`create_concurrent_with_oplog` or `open_concurrent_with_oplog` で有効化。
    /// Some なら tie_async/untie_async/delete_async は oplog_record_queue 経由で
    /// consumer thread 側に batch flush を委ねる。
    oplog: Option<std::sync::Arc<enchudb_oplog::oplog::OpLog>>,
    /// async path 専用の WAL record queue。 writer は WAL に直接書かず、 ここに
    /// owned record を push。 consumer thread が drain して `wal.append_many` で
    /// 1 flock サイクル N records にまとめる (per-record flock コスト償却)。
    oplog_record_queue: Option<std::sync::Arc<crossbeam_queue::ArrayQueue<enchudb_oplog::oplog::OwnedOp>>>,
    /// writer が `oplog_record_queue` へ push した WAL record の累積件数。
    /// op は queue 先行・record 後追い (#77-H4) なので、 apply_count barrier
    /// だけでは「op は適用済みだが record は未 append」の窓が残る。
    /// `flush_writes` はこの counter 対でも待つ (下の wal_append_count 参照)。
    wal_push_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// consumer が WAL へ append し終えた record の累積件数。
    /// wal_append_count >= wal_push_count が「queue に record が残っていない」
    /// 同期点 — これが無いと `oplog_sync` が record より先に Commit を打ち、
    /// fsync 済みのはずの write が crash で消える。
    wal_append_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// #77-M2: consumer thread が panic すると true。 flush_writes / Drop の
    /// barrier spin と producer の blocking push が「絶対に進まない待ち」に
    /// 陥らないための脱出フラグ。
    consumer_poisoned: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// 0.11 (request10): Ref 値が translated foreign entity を指す write を
    /// bridge から除外した際の一度きり警告フラグ (u32 wire value に世界番号が
    /// 入らないため発送不能、 wire 拡張の follow-up 待ち)。
    warned_ref_to_replica: std::sync::atomic::AtomicBool,
    /// 背景 fsync が最後に completed した LSN。
    durable_lsn: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// v32: この Engine を所有する peer の id。分散時 eid の上位 32bit。
    peer_id: std::sync::atomic::AtomicU32,
    /// v32: LWW 用に (eid, himo) → 最後の HLC を記録。
    hlc_store: std::sync::Arc<crate::hlc_store::HlcStore>,
    /// #9: 受信した foreign eid を自分の eid 空間の local eid に翻訳する写像。
    eid_translator: std::sync::Arc<crate::eid_translator::EidTranslator>,
    /// v32 Phase C: 自 peer の ed25519 鍵ペア。None なら署名しない/検証もしない。
    keypair: std::sync::RwLock<Option<std::sync::Arc<enchudb_oplog::keys::Keypair>>>,
    /// v32 Phase C: 他 peer の pubkey TOFU ストア。Syncer が verify に使う。
    pubkeys: std::sync::Arc<enchudb_oplog::keys::PubkeyStore>,
    /// v32 Phase C: ACL(書き込み許可 peer の集合)。Syncer が enforce する。
    acl: std::sync::Arc<crate::acl::Acl>,
    /// v32 エッジ用: true なら書き込み API が panic、sync 経由 (remote_*_apply) のみ受ける。
    is_replica: std::sync::atomic::AtomicBool,
    /// CRDT mesh mode: true なら `remote_*_apply` で受信した op を **元 HLC/author/署名のまま**
    /// 自分の WAL にも `append_relayed` で記録し、 次の publish で他 peer に gossip する。
    /// relay 側の (peer, hlc) dedupe で同じ record の二度送りはカットされるためループしない。
    /// ホスト/クライアント構成 (= ホストが唯一の集約点) では false のまま使う。
    gossip_remote_apply: std::sync::atomic::AtomicBool,
    /// 大容量 blob 用 store(画像/動画/モデル等)。`set_blob_store` で注入。
    /// 未設定なら `blob_store()` は None を返す。
    blob_store: std::sync::RwLock<Option<std::sync::Arc<dyn crate::blob_store::BlobStore>>>,
    /// changefeed: WAL に durable 化した record を listener に push する。
    /// consumer スレッドが背景 fsync 完了後に発火。
    change_listeners: std::sync::Arc<std::sync::RwLock<Vec<std::sync::Arc<dyn crate::changefeed::ChangeListener>>>>,
    /// changefeed: 次に listener へ emit すべき WAL offset。
    /// add_change_listener 時に wal.head() に同期され、それ以降の commit のみが流れる。
    change_emit_offset: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// 0.7.0 Phase 4: 既に `_sync_ops` table に転送済みの WAL offset。
    /// consumer thread が背景 fsync 後に新規 commit を `_sync_ops` へ転送する。
    /// `enable_sync_tables` 有効化前は使われない (= 0)、 後は単調前進。
    sync_ops_offset: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// 0.8.11 (issue: stress_10k_cycle flaky): `transfer_oplog_to_sync_ops` の
    /// 排他 lock。 0.8.0 で `concurrentize_with_oplog` の background consumer thread
    /// が自動 transfer を呼ぶようになったが、 手動 transfer との並列実行で
    /// `from = sync_ops_offset.load()` → records pull → row insert → offset.store()
    /// の 4 step が race し、 同じ records が複数回 row insert される
    /// (= reclaim 後に残骸が残る) bug。 本 mutex で transfer の全期間を排他化、
    /// 重複転送を根本解消。 lock 競合は per-fsync 頻度 (= 100ms 周期) で頻度低、
    /// hot path 影響は無視できる。
    transfer_lock: std::sync::Arc<std::sync::Mutex<()>>,
    /// 0.8.15: `try_persist_tables` 内の warning emit を rate-limit するための
    /// 最後の emit timestamp (millis since UNIX_EPOCH)。 ENOSPC 等で連続失敗
    /// すると consumer thread が毎 batch eprintln → ターミナル不能なので 1 秒
    /// 1 回に抑える。 0 = 未 emit。
    last_persist_warn_ms: std::sync::atomic::AtomicU64,
    /// 0.7.0 Phase 4: `_sync_ops` row に振る単調 lsn (= u32 の publish_since cursor)。
    /// `entity_in` の eid とは別、 reclaim で row が消えても lsn は単調維持される。
    next_sync_lsn: std::sync::Arc<std::sync::atomic::AtomicU32>,
    /// v33: 受信した Vocab op の `(author_peer, remote_vid) → local_vid` mapping。
    /// Symbol 型 himo の Tie を受信した時に remote_vid を local_vid に変換して apply する。
    /// peer-local に保持(replica でも独立、open 時は空、受信で徐々に埋まる)。
    peer_vocab_map: std::sync::RwLock<std::collections::HashMap<(enchudb_oplog::PeerId, u32), u32>>,
    /// v34: read-only モード。 true なら書き込み API は error/panic。 open_readonly で立つ。
    /// `is_replica` は「直 write 拒否、 Syncer 経由は受ける」、 こちらは「一切 write 不可」。
    is_readonly: std::sync::atomic::AtomicBool,
    /// 0.8.2: build phase の sidecar fsync 抑止。 schema crate が
    /// `Database::create → build×N` 中 true にして、 `finish_*` / Drop で
    /// false に戻して 1 度だけ explicit に `persist_tables()` を呼ぶ。
    /// macOS APFS で N table 宣言時の N×fsync (= 1 fsync 5-7ms) を 1 回に圧縮。
    defer_tables_persist: std::sync::atomic::AtomicBool,
    /// v34: writer lock の保持 fd。 open_writer / create_* で `.db.lock` sidecar を
    /// flock(LOCK_EX)、 Engine drop で fd close = lock release。 readonly では None。
    /// 多 process write の同 .db 競合を防ぐ (sqlite WAL モード相当)。
    #[cfg(not(target_arch = "wasm32"))]
    _writer_lock: Option<WriterLock>,
    backing: Backing, // 最後に drop されるよう最終フィールド
}

impl Engine {
    /// 現行の単独 Engine を作る(WAL 無し、&mut self で mutation)。
    /// v32 以前: `Engine::create` の別名。
    /// v33 以降: `Engine::create` が WAL 付き `Arc<Self>` を返すようになるため、
    /// 明示的に単独 Engine が欲しい場合はこちらを使う。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_standalone(path: &str) -> io::Result<Self> {
        Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)
    }

    /// `create` は WAL 有効 + `Arc<Self>` 返し。
    /// 旧挙動は `create_standalone` で取れる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create(path: &str) -> io::Result<std::sync::Arc<Self>> {
        Self::create_concurrent_with_oplog(path, 16 * 1024 * 1024)
    }

    /// CLI/小規模用途向け。ファイルサイズ数MB。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_compact(path: &str) -> io::Result<Self> {
        Self::create_full_with_cyl(
            path,
            65_536,                    // max_entities: 64K
            Some(16 * 1024 * 1024),    // vocab_data: 16MB
            Some(64),                  // max_himos: 64
            Some(16 * 1024 * 1024),    // content_data: 16MB
            Some(256),                 // cyl_max_values: 256
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_with_capacity(path: &str, max_entities: u32) -> io::Result<Self> {
        Self::create_with_options(path, max_entities, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_with_options(path: &str, max_entities: u32, vocab_data_size: Option<usize>) -> io::Result<Self> {
        Self::create_full(path, max_entities, vocab_data_size, None, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_full(path: &str, max_entities: u32, vocab_data_size: Option<usize>, max_himos: Option<u32>, content_data_size: Option<usize>) -> io::Result<Self> {
        Self::create_full_with_cyl(path, max_entities, vocab_data_size, max_himos, content_data_size, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_full_with_cyl(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        max_himos: Option<u32>,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
    ) -> io::Result<Self> {
        Self::create_full_with_leaf(
            path, max_entities, vocab_data_size, max_himos, content_data_size,
            cyl_max_values, None,
        )
    }

    /// `create_full_with_cyl` の leaf region size を明示できる版。
    /// `leaf_data_size = Some(0)` は leaf region 無し = v5 相当 DB を作る
    /// (#88 migration test / bench の before 生成)。 None は default 予約。
    /// leaf scale は default (`Gb16`)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_full_with_leaf(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        max_himos: Option<u32>,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
        leaf_data_size: Option<usize>,
    ) -> io::Result<Self> {
        Self::create_full_with_leaf_scale(
            path, max_entities, vocab_data_size, max_himos, content_data_size,
            cyl_max_values, leaf_data_size, None,
        )
    }

    /// #90: LeafStore の scale (`LeafScale::Gb16`/`Gb32`/`Gb64` = 16/32/64GB cap) を
    /// 明示する版。 None は default (`Gb16`)。 leaf offset は word 単位で持つので、
    /// scale を上げても列幅は増えず slot alignment (padding) だけ粗くなる。
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    pub fn create_full_with_leaf_scale(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        max_himos: Option<u32>,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
        leaf_data_size: Option<usize>,
        leaf_scale: Option<LeafScale>,
    ) -> io::Result<Self> {
        let off_shift = leaf_scale.map(|s| s.off_shift()).unwrap_or(DEFAULT_LEAF_OFF_SHIFT);
        let vds = vocab_data_size.unwrap_or(DEFAULT_VOCAB_DATA_SIZE);
        let max_himos = max_himos.unwrap_or(DEFAULT_MAX_HIMOS);
        let layout = Layout::compute(max_entities, max_himos, vds, content_data_size, cyl_max_values, leaf_data_size);
        // #90: 予約 leaf region が選んだ scale の cap を超えないか検証。
        let leaf_cap = cap_bytes_for_shift(off_shift);
        if layout.leaf_data_size as u64 > leaf_cap {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "leaf_data_size {} exceeds scale cap {} (off_shift {}) — 大きい LeafScale を選べ",
                    layout.leaf_data_size, leaf_cap, off_shift,
                ),
            ));
        }

        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        // H11: 既存 DB を silent truncate で破壊しない (lock 取得前に判定 —
        // 既存 DB を別 process の writer が掴んでいる場合の flock block も回避)。
        ensure_create_target_absent(path)?;
        // writer lock を先に取る (= 他 writer が居れば block)。 create も書き込みなので必須。
        let writer_lock = acquire_writer_lock(path)?;
        let file = open_new_db_file(path)?;
        file.set_len(layout.total_size as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        mmap[H_MAGIC..H_MAGIC + 4].copy_from_slice(&FILE_MAGIC);
        mmap[H_VERSION..H_VERSION + 4].copy_from_slice(&FILE_VERSION.to_le_bytes());
        mmap[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].copy_from_slice(&max_entities.to_le_bytes());
        mmap[H_MAX_HIMOS..H_MAX_HIMOS + 4].copy_from_slice(&max_himos.to_le_bytes());
        mmap[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&0u32.to_le_bytes());
        mmap[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4].copy_from_slice(&layout.vocab_max_entries.to_le_bytes());
        mmap[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4].copy_from_slice(&layout.vocab_index_cap.to_le_bytes());
        mmap[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8].copy_from_slice(&(layout.vocab_data_size as u64).to_le_bytes());
        mmap[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4].copy_from_slice(&layout.himoreg_max_entries.to_le_bytes());
        mmap[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4].copy_from_slice(&layout.himoreg_index_cap.to_le_bytes());
        mmap[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8].copy_from_slice(&(layout.himoreg_data_size as u64).to_le_bytes());
        mmap[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8].copy_from_slice(&(layout.content_data_size as u64).to_le_bytes());
        mmap[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].copy_from_slice(&layout.cyl_max_values.to_le_bytes());
        mmap[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8].copy_from_slice(&(layout.leaf_data_size as u64).to_le_bytes());

        // v28: ヘッダ整合性 CRC
        write_header_crc(&mut mmap);

        let base = mmap.as_mut_ptr();

        let entities = EntitySet::init(
            unsafe { Region::new(base.add(layout.entities_off), layout.entities_size) },
            max_entities,
        );
        let vocab = Vocabulary::init(
            unsafe { Region::new(base.add(layout.vocab_data_off), layout.vocab_data_size) },
            unsafe { Region::new(base.add(layout.vocab_offsets_off), layout.vocab_offsets_size) },
            unsafe { Region::new(base.add(layout.vocab_index_off), layout.vocab_index_size) },
            layout.vocab_max_entries, layout.vocab_index_cap,
        );
        let himo_reg = Vocabulary::init(
            unsafe { Region::new(base.add(layout.himoreg_data_off), layout.himoreg_data_size) },
            unsafe { Region::new(base.add(layout.himoreg_offsets_off), layout.himoreg_offsets_size) },
            unsafe { Region::new(base.add(layout.himoreg_index_off), layout.himoreg_index_size) },
            layout.himoreg_max_entries, layout.himoreg_index_cap,
        );
        let contents = ContentStore::init(
            unsafe { Region::new(base.add(layout.content_index_off), layout.content_index_size) },
            unsafe { Region::new(base.add(layout.content_data_off), layout.content_data_size) },
        );
        // leaf_data_size == 0 (= v5 相当 create) は leaf region 無し。 load 経路
        // (line ~2281) と同じ gate。
        let leaf = if layout.leaf_data_size > 0 {
            Some(LeafStore::init(unsafe {
                Region::new(base.add(layout.leaf_data_off), layout.leaf_data_size)
            }, off_shift))
        } else {
            None
        };

        Ok(Self {
            path: path.to_string(), layout, max_entities, max_himos,
            vocab, himo_reg,
            himo_names: AppendVec::with_capacity(max_himos as usize),
            value_types: AppendVec::with_capacity(max_himos as usize),
            himo_max_values: AppendVec::with_capacity(max_himos as usize),
            himos: AppendVec::with_capacity(max_himos as usize), entities, contents, leaf,
            tables: vec![TableDef::anonymous()],
            himo_to_table: AppendVec::with_capacity(max_himos as usize),
            himo_def_lock: std::sync::Mutex::new(()),
            write_queue: None,
            shutdown_flag: None,
            consumer_handle: std::sync::Mutex::new(None),
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_append_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            oplog: None,
            oplog_record_queue: None,
            consumer_poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            warned_ref_to_replica: std::sync::atomic::AtomicBool::new(false),
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            eid_translator: std::sync::Arc::new(crate::eid_translator::EidTranslator::new()),
            keypair: std::sync::RwLock::new(None),
            pubkeys: std::sync::Arc::new(enchudb_oplog::keys::PubkeyStore::new()),
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            gossip_remote_apply: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            sync_ops_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            next_sync_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(1)),
            transfer_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            last_persist_warn_ms: std::sync::atomic::AtomicU64::new(0),
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            is_readonly: std::sync::atomic::AtomicBool::new(false),
            defer_tables_persist: std::sync::atomic::AtomicBool::new(false),
            _writer_lock: Some(writer_lock),
            backing: Backing::Mmap(mmap),
        })
    }

    /// growable backing で新規 DB を作る。 通常の `create_full_with_cyl`
    /// が `set_len(layout.total_size)` で sparse な巨大ファイル (88 GB
    /// など) を作るのに対し、 こちらは `GrowableMap` で **virtual address
    /// だけ予約 + 必要分だけ commit** する経路。 fresh DB が page サイズ
    /// から始まり書き込みに応じて拡張する。
    ///
    /// 現状の実装は init 時点で layout 全体まで pre-commit する保守的な
    /// 形 (= ファイルサイズは旧来の `create_*` と同等)。 各 store の write
    /// 境界に `grow_amortized` を仕込むことで段階的に initial commit を
    /// 縮める予定。 まずは Backing 経路の互換性確認を優先。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable(path: &str) -> io::Result<Self> {
        Self::create_growable_with_capacity(path, DEFAULT_MAX_ENTITIES)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_with_capacity(path: &str, max_entities: u32) -> io::Result<Self> {
        let max_himos = DEFAULT_MAX_HIMOS;
        let layout = Layout::compute(max_entities, max_himos, DEFAULT_VOCAB_DATA_SIZE, None, None, None);
        Self::create_growable_full(path, layout, max_entities, max_himos, DEFAULT_LEAF_OFF_SHIFT)
    }

    /// growable backing で開く。`max_entities` と `vocab_data_size` を明示。
    ///
    /// 大規模 Leaf text を持つアプリ (議事録 / 論文 / 全文 archive) で default
    /// 512 MiB の vocab cap に当たる場合に使う。`Leaf` 列も `vocab.insert`
    /// 経由で vocab data に積まれるため、`Tag` 数だけでなく本文の総バイト数
    /// が cap を決める。目安: 1 KB / row × 1 M rows ≒ 1 GiB を見込む。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_with_options(
        path: &str,
        max_entities: u32,
        vocab_data_size: usize,
    ) -> io::Result<Self> {
        let max_himos = DEFAULT_MAX_HIMOS;
        let layout = Layout::compute(max_entities, max_himos, vocab_data_size, None, None, None);
        Self::create_growable_full(path, layout, max_entities, max_himos, DEFAULT_LEAF_OFF_SHIFT)
    }

    /// #90: growable backing で、 leaf region の予約 size と scale (16/32/64GB) を
    /// 明示する。 wikipulse のような大 payload × 巨大 live working set 用途で、
    /// leaf region を 4GB 超に伸ばす場合に使う。 `leaf_data_size` は選んだ scale の
    /// cap 以下であること。 leaf offset は word 単位なので列幅は不変。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_with_leaf(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        leaf_data_size: Option<usize>,
        leaf_scale: LeafScale,
    ) -> io::Result<Self> {
        let max_himos = DEFAULT_MAX_HIMOS;
        let vds = vocab_data_size.unwrap_or(DEFAULT_VOCAB_DATA_SIZE);
        let layout = Layout::compute(max_entities, max_himos, vds, None, None, leaf_data_size);
        let leaf_cap = leaf_scale.cap_bytes();
        if layout.leaf_data_size as u64 > leaf_cap {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "leaf_data_size {} exceeds scale cap {} ({:?}) — 大きい LeafScale を選べ",
                    layout.leaf_data_size, leaf_cap, leaf_scale,
                ),
            ));
        }
        Self::create_growable_full(path, layout, max_entities, max_himos, leaf_scale.off_shift())
    }

    /// Tiny growable preset for app state-logs (matcha-style: a few
    /// hundred rows of dismissed-key / seen-at / etc.). Default
    /// `create_growable` uses gigascale capacities so the layout
    /// total — and thus the on-disk apparent size of a fresh DB —
    /// is hundreds of MB even with growable backing. Apps that just
    /// need a key/value store with timestamps want something much
    /// smaller. This caps:
    /// - 1024 entities
    /// - 16 himos
    /// - 64 KB vocab / himoreg / content data each
    /// → layout total ≈ 250 KB, fresh DB ~ same on disk.
    ///
    /// Hard limit of 1024 rows is fine for most state-log use cases;
    /// callers that may exceed it should use `create_growable` or
    /// `create_growable_with_capacity` with the right size.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_tiny(path: &str) -> io::Result<Self> {
        let max_entities = 1024_u32;
        let max_himos = 16_u32;
        // tiny preset では vocab_max_entries の default ×16 multiplier
        // が過剰 (16384 entries → vocab_offsets 128KB + index 213KB)。
        // matcha のような数百エントリ用途では ×2 で十分 — 2048 entries
        // で offsets 16KB + index ~26KB に収まる。
        let vocab_max_entries = max_entities.saturating_mul(2);
        let layout = Layout::compute_with_caps(
            max_entities,
            max_himos,
            vocab_max_entries,
            64 * 1024,        // vocab_data: 64 KB
            Some(64 * 1024),  // content_data: 64 KB
            Some(64),         // cyl_max_values: small per-himo cylinders
            None,             // leaf_data: default
        );
        Self::create_growable_full(path, layout, max_entities, max_himos, DEFAULT_LEAF_OFF_SHIFT)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_growable_full(
        path: &str,
        layout: Layout,
        max_entities: u32,
        max_himos: u32,
        leaf_off_shift: u32,
    ) -> io::Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        // H11: 既存 DB を silent truncate で破壊しない (create_full_with_cyl と同じガード)
        ensure_create_target_absent(path)?;
        // writer lock を先に取る (= 他 writer が居れば block)
        let writer_lock = acquire_writer_lock(path)?;
        let file = open_new_db_file(path)?;

        // Phase B Step 3: initial_commit は variable cluster の手前 (=
        // 末尾の vocab_data の開始 offset) で打ち切る。 fixed cluster
        // (entities / *_offsets / *_index / content_index / himo_slots)
        // のみコミット、 vocab_data / himoreg_data / content_data は
        // append まで未コミット。 lazy init された Vocabulary /
        // ContentStore が初回 append 時に必要なだけ ensure_committed
        // で伸ばす。
        let initial_commit = layout.vocab_data_off;
        let map = std::sync::Arc::new(crate::growable_map::GrowableMap::new(
            file,
            layout.total_size,
            initial_commit,
        )?);

        // Header init — same byte layout as create_full_with_cyl.
        let base = map.base();
        let header = unsafe { std::slice::from_raw_parts_mut(base, HEADER_SIZE) };
        header[H_MAGIC..H_MAGIC + 4].copy_from_slice(&FILE_MAGIC);
        header[H_VERSION..H_VERSION + 4].copy_from_slice(&FILE_VERSION.to_le_bytes());
        header[H_MAX_ENTITIES..H_MAX_ENTITIES + 4]
            .copy_from_slice(&max_entities.to_le_bytes());
        header[H_MAX_HIMOS..H_MAX_HIMOS + 4].copy_from_slice(&max_himos.to_le_bytes());
        header[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&0u32.to_le_bytes());
        header[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4]
            .copy_from_slice(&layout.vocab_max_entries.to_le_bytes());
        header[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4]
            .copy_from_slice(&layout.vocab_index_cap.to_le_bytes());
        header[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8]
            .copy_from_slice(&(layout.vocab_data_size as u64).to_le_bytes());
        header[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4]
            .copy_from_slice(&layout.himoreg_max_entries.to_le_bytes());
        header[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4]
            .copy_from_slice(&layout.himoreg_index_cap.to_le_bytes());
        header[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8]
            .copy_from_slice(&(layout.himoreg_data_size as u64).to_le_bytes());
        header[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8]
            .copy_from_slice(&(layout.content_data_size as u64).to_le_bytes());
        header[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4]
            .copy_from_slice(&layout.cyl_max_values.to_le_bytes());
        // Phase B: growable backing は file_size < layout.total_size を許容するため、
        // open 側の validate_file_size でこのフラグを見て分岐する。
        header[H_BACKING_KIND..H_BACKING_KIND + 4]
            .copy_from_slice(&BACKING_KIND_GROWABLE.to_le_bytes());
        header[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8]
            .copy_from_slice(&(layout.leaf_data_size as u64).to_le_bytes());
        write_header_crc(header);

        let _ = base; // base ptr is implicit via Region::with_grower from here on
        let entities = EntitySet::init(
            unsafe {
                Region::with_grower(map.clone(), layout.entities_off, layout.entities_size)
            },
            max_entities,
        );
        let vocab = Vocabulary::init(
            unsafe {
                Region::with_grower(map.clone(), layout.vocab_data_off, layout.vocab_data_size)
            },
            unsafe {
                Region::with_grower(
                    map.clone(),
                    layout.vocab_offsets_off,
                    layout.vocab_offsets_size,
                )
            },
            unsafe {
                Region::with_grower(map.clone(), layout.vocab_index_off, layout.vocab_index_size)
            },
            layout.vocab_max_entries,
            layout.vocab_index_cap,
        );
        let himo_reg = Vocabulary::init(
            unsafe {
                Region::with_grower(map.clone(), layout.himoreg_data_off, layout.himoreg_data_size)
            },
            unsafe {
                Region::with_grower(
                    map.clone(),
                    layout.himoreg_offsets_off,
                    layout.himoreg_offsets_size,
                )
            },
            unsafe {
                Region::with_grower(
                    map.clone(),
                    layout.himoreg_index_off,
                    layout.himoreg_index_size,
                )
            },
            layout.himoreg_max_entries,
            layout.himoreg_index_cap,
        );
        let contents = ContentStore::init(
            unsafe {
                Region::with_grower(
                    map.clone(),
                    layout.content_index_off,
                    layout.content_index_size,
                )
            },
            unsafe {
                Region::with_grower(map.clone(), layout.content_data_off, layout.content_data_size)
            },
        );
        let leaf = Some(LeafStore::init(unsafe {
            Region::with_grower(map.clone(), layout.leaf_data_off, layout.leaf_data_size)
        }, leaf_off_shift));

        Ok(Self {
            path: path.to_string(),
            layout,
            max_entities,
            max_himos,
            vocab,
            himo_reg,
            himo_names: AppendVec::with_capacity(max_himos as usize),
            value_types: AppendVec::with_capacity(max_himos as usize),
            himo_max_values: AppendVec::with_capacity(max_himos as usize),
            himos: AppendVec::with_capacity(max_himos as usize),
            entities,
            contents,
            leaf,
            tables: vec![TableDef::anonymous()],
            himo_to_table: AppendVec::with_capacity(max_himos as usize),
            himo_def_lock: std::sync::Mutex::new(()),
            write_queue: None,
            shutdown_flag: None,
            consumer_handle: std::sync::Mutex::new(None),
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_append_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            oplog: None,
            oplog_record_queue: None,
            consumer_poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            warned_ref_to_replica: std::sync::atomic::AtomicBool::new(false),
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            eid_translator: std::sync::Arc::new(crate::eid_translator::EidTranslator::new()),
            keypair: std::sync::RwLock::new(None),
            pubkeys: std::sync::Arc::new(enchudb_oplog::keys::PubkeyStore::new()),
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            gossip_remote_apply: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            sync_ops_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            next_sync_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(1)),
            last_persist_warn_ms: std::sync::atomic::AtomicU64::new(0),
            transfer_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            is_readonly: std::sync::atomic::AtomicBool::new(false),
            defer_tables_persist: std::sync::atomic::AtomicBool::new(false),
            _writer_lock: Some(writer_lock),
            backing: Backing::Growable(map),
        })
    }

    /// 現行の単独 Engine を開く(WAL 無し、&mut self で mutation)。
    /// v32 以前: `Engine::open` の別名。
    /// v33 以降: `Engine::open` が WAL 付き `Arc<Self>` を返すようになるため、
    /// 明示的に単独 Engine が欲しい場合はこちらを使う。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_standalone(path: &str) -> io::Result<Self> {
        Self::open_internal(path, /*verify_region_crc=*/ true, /*take_lock=*/ true, /*readonly=*/ false)
    }

    /// read-only open: writer lock を取らず、 書き込み API は error。
    /// 複数 process で同時に呼んで OK (writer と共存可能、 reader 同士も無制限)。
    /// 用途: GUI の表示専用 process、 監視ツール、 backup-reader 等。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_readonly(path: &str) -> io::Result<Self> {
        let eng = Self::open_internal(path, /*verify_region_crc=*/ true, /*take_lock=*/ false, /*readonly=*/ true)?;
        eng.is_readonly.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// `open` は WAL 有効 + `Arc<Self>` 返し。
    /// 旧挙動は `open_standalone` で取れる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &str) -> io::Result<std::sync::Arc<Self>> {
        Self::open_concurrent_with_oplog(path, 16 * 1024 * 1024)
    }

    /// 内部用: region CRC 検証を skip できる open。WAL ルート用。
    /// `take_lock = true` で `.db.lock` の flock(LOCK_EX) を取得し、 Engine 寿命中保持。
    #[cfg(not(target_arch = "wasm32"))]
    fn open_internal(path: &str, verify_region_crc: bool, take_lock: bool, readonly: bool) -> io::Result<Self> {
        // open path: writer lock を mmap 前に取る (= 他 writer 居れば block)
        let writer_lock = if take_lock {
            Some(acquire_writer_lock(path)?)
        } else {
            None
        };
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        // v29: ファイルサイズ検証 — truncate で SIGBUS を防ぐ
        let file_size = file.metadata()?.len();
        Self::validate_file_size(&file, file_size)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let mut eng = Self::load_from_backing(Backing::Mmap(mmap), readonly)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        eng.path = path.to_string();
        eng._writer_lock = writer_lock;

        // 0.8.15 (issue #52): open 前に残骸 `.tables.tmp` を掃除する self-heal。
        // persist 失敗 (ENOSPC 等) で `.tmp` が残ったケースを次回 reopen で確実に
        // clean できるよう、 next persist の truncate(true) に依存しない明示削除。
        cleanup_tables_tmp(path);

        // β-light step 7: tables sidecar の読み込み。 不在なら anonymous fallback
        // (= load_from_backing が既に行った形のまま) で v4 DB 互換。
        // 0.8.15 (issue #52): InvalidData (= 破損) は **fail-readable** で扱う。
        // sidecar を `.tables.corrupt-<unix_ts>` に rename して退避し、 anonymous
        // tables のまま続行する。 schema crate 側は engine の `list_user_tables`
        // から再合成できるので、 sidecar 破損で全 DB が unreadable になる失敗
        // モードを避ける。
        match load_tables_from_sidecar(path) {
            Ok(Some(persisted)) => eng.adopt_persisted_tables(persisted),
            Ok(None) => {} // 不在: 新規 DB or v4 legacy
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                rename_corrupt_sidecar(path, "tables", &e);
            }
            Err(e) => {
                eprintln!(
                    "warning: failed to read tables sidecar (anonymous fallback): {}",
                    e
                );
            }
        }

        // #9: eid 翻訳テーブルの sidecar 読み込み。 不在 (= sync してない / 旧 DB) なら
        // 空の translator で続行 (additive、 後方互換)。 破損は警告のみで空続行 (= 再
        // sync で mapping は張り直せる)。
        match load_eidmap_from_sidecar(path) {
            Ok(Some(entries)) => {
                // peer_id は上で header から復元済み → translated eid の key が正しく作れる。
                let self_peer = eng.peer_id();
                for (peer, foreign_local, local, tomb) in entries {
                    eng.eid_translator.insert(peer, foreign_local, local);
                    // #9 (H4): `.eidmap` と `.tables` は別々の rename なので crash で
                    // 不整合になりうる。 mapped local を table の next_local が必ず追い
                    // 越すよう前進させ、 stale `.tables` でも mapped slot を再 alloc して
                    // 衝突する事態を防ぐ (= 最悪でも「重複」に留め「衝突」を起こさない)。
                    Self::advance_table_next_local_for(&eng.tables, local);
                    // #9 (C): foreign Delete tombstone を HlcStore に復元する。 これが無いと
                    // reopen で tombstone が消え、 Delete より古い Tie が (Delete 抜きで) 再配送
                    // された時に削除済み entity が復活する。 ZERO は未削除なので skip。
                    if tomb != enchudb_oplog::Hlc::ZERO {
                        eng.hlc_store
                            .force_set(enchudb_oplog::make_eid(self_peer, local), u16::MAX, tomb);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!(
                    "warning: failed to read eidmap sidecar (empty translator): {}",
                    e
                );
            }
        }

        if verify_region_crc {
            // .crc ファイルがあれば全 region CRC 検証
            eng.verify_region_crcs()?;
        }
        Ok(eng)
    }

    /// 全 region の CRC テーブルを計算する。
    ///
    /// v29 issue7 fix: vocab/himoreg 領域の header には index clean flag (offset
    /// 12..16) があり、 open 時に必ず flip される (clean=true → false)。 この 4
    /// バイトを CRC 計算から除外することで、 seal_integrity で焼いた `.crc` が
    /// 次回 open でも変わらず検証 OK になる。 clean flag の値は msync 直後に
    /// 永続化されるので、 CRC 検証範囲から外しても DB データの整合性検証は
    /// 損なわない (clean flag 自体は rebuild の hint であって data ではない)。
    fn compute_region_crc_table(&self) -> crate::integrity::CrcTable {
        use crate::integrity::{CrcTable, RegionKind, fnv1a_region, fnv1a_slices};
        let file_size = self.layout.total_size as u64;
        let mut table = CrcTable::new(self.max_himos, file_size);
        let buf = match &self.backing {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Mmap(m) => &m[..],
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Growable(g) => unsafe {
                std::slice::from_raw_parts(g.base() as *const u8, g.committed())
            },
            Backing::Memory(v) => &v[..],
        };
        // himo columns
        for hid in 0..self.value_types.len() {
            let off = self.layout.himo_col_off(hid);
            let end = off + self.layout.himo_col_size;
            let crc = fnv1a_region(&buf[off..end]);
            table.set(RegionKind::HimoColumn(hid as u32), crc);
        }
        // vocab data (clean flag bytes 12..16 を除外)
        let off = self.layout.vocab_data_off;
        let end = off + self.layout.vocab_data_size;
        table.set(
            RegionKind::Vocab,
            fnv1a_slices(&[&buf[off..off + 12], &buf[off + 16..end]]),
        );
        // himoreg data (clean flag bytes 12..16 を除外)
        let off = self.layout.himoreg_data_off;
        let end = off + self.layout.himoreg_data_size;
        table.set(
            RegionKind::HimoReg,
            fnv1a_slices(&[&buf[off..off + 12], &buf[off + 16..end]]),
        );
        // content data
        let off = self.layout.content_data_off;
        let end = off + self.layout.content_data_size;
        table.set(RegionKind::Content, fnv1a_region(&buf[off..end]));
        // entity set
        let off = self.layout.entities_off;
        let end = off + self.layout.entities_size;
        table.set(RegionKind::EntitySet, fnv1a_region(&buf[off..end]));
        table
    }

    /// open 時の region CRC 検証。`.crc` ファイルがなければスキップ。
    #[cfg(not(target_arch = "wasm32"))]
    fn verify_region_crcs(&self) -> io::Result<()> {
        use crate::integrity::{CrcTable, crc_path_for};
        if self.path.is_empty() { return Ok(()); } // from_bytes 等でパス無い場合スキップ
        let crc_path = crc_path_for(&self.path);
        let stored = match CrcTable::load(&crc_path)? {
            Some(t) => t,
            None => return Ok(()), // v28 以前の DB 互換
        };
        let expected = self.compute_region_crc_table();
        let mismatches = stored.diff(&expected);
        if !mismatches.is_empty() {
            let names: Vec<String> = mismatches.iter().map(|k| k.name()).collect();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("region CRC mismatch in: {} — file may be corrupt", names.join(", ")),
            ));
        }
        Ok(())
    }

    /// 現在の DB state から region CRC を再計算 → `.crc` ファイルに書き出す。
    /// flush() から呼ばれる。
    #[cfg(not(target_arch = "wasm32"))]
    fn persist_region_crcs(&self) -> io::Result<()> {
        if self.path.is_empty() { return Ok(()); }
        let table = self.compute_region_crc_table();
        let crc_path = crate::integrity::crc_path_for(&self.path);
        table.save(&crc_path)?;
        Ok(())
    }

    /// ヘッダを先読みして、ファイルサイズが layout.total_size 以上かチェックする。
    /// 以下だったら truncate されている → mmap すると OOB アクセスで SIGBUS 直行。
    #[cfg(not(target_arch = "wasm32"))]
    fn validate_file_size(file: &std::fs::File, file_size: u64) -> io::Result<()> {
        use std::io::{Read, Seek, SeekFrom};
        if file_size < HEADER_SIZE as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("file too small: {} bytes (need at least {} for header)", file_size, HEADER_SIZE),
            ));
        }
        // ヘッダを同期読みで確認(mmap 前なので普通の read で OK)
        let mut file_clone = file.try_clone()?;
        file_clone.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; HEADER_SIZE];
        file_clone.read_exact(&mut buf)?;

        if buf[H_MAGIC..H_MAGIC + 4] != FILE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not an EnchuDB file"));
        }
        // CRC 検証を先に(fields 改竄で layout 計算が狂うのを防ぐ)
        verify_header_crc(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let max_entities = u32::from_le_bytes(buf[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].try_into().unwrap());
        let max_himos = u32::from_le_bytes(buf[H_MAX_HIMOS..H_MAX_HIMOS + 4].try_into().unwrap());
        let himo_count = u32::from_le_bytes(buf[H_HIMO_COUNT..H_HIMO_COUNT + 4].try_into().unwrap());
        let vocab_max_entries = u32::from_le_bytes(buf[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4].try_into().unwrap());
        let vocab_index_cap = u32::from_le_bytes(buf[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4].try_into().unwrap());
        let vocab_data_size = u64::from_le_bytes(buf[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let himoreg_max_entries = u32::from_le_bytes(buf[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4].try_into().unwrap());
        let himoreg_index_cap = u32::from_le_bytes(buf[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4].try_into().unwrap());
        let himoreg_data_size = u64::from_le_bytes(buf[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let content_data_size = u64::from_le_bytes(buf[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let cyl_max_values = u32::from_le_bytes(buf[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].try_into().unwrap());

        // 0.9.0 (L1): CRC == 0 の legacy header は verify_header_crc を素通しする
        // ため、 field の自己整合を明示チェック (破損値 → panic/OOB の前に InvalidData)。
        sanity_check_header_fields(
            max_himos, himo_count,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // v6 (#88): leaf region size (pre-v6 header は 0 = leaf region 無し)。
        let leaf_data_size = u64::from_le_bytes(buf[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let layout = Layout::try_from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Phase B 以降: backing_kind フラグで「growable 由来で file_size <
        // total_size が正常」 か 「eager 由来で file_size 不足は truncation」 か
        // を分岐する。
        //
        // - EAGER (0、 default、 legacy zero-fill 含む): file_size は
        //   layout.total_size と一致が必須。 不足は accidental truncation /
        //   ファイル破損 → エラーで弾く (v29 crash safety)。
        // - GROWABLE (1): file_size < total_size でも正常 (variable cluster
        //   未コミット)。 ftruncate で sparse 拡張して mmap 用に揃える
        //   (実 disk usage はゼロ、 各 store の load() は MAGIC missing で
        //   lazy fresh 認識)。
        let backing_kind = u32::from_le_bytes(
            buf[H_BACKING_KIND..H_BACKING_KIND + 4].try_into().unwrap(),
        );
        if file_size < layout.total_size as u64 {
            match backing_kind {
                BACKING_KIND_GROWABLE => {
                    file.set_len(layout.total_size as u64)?;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "file truncated: {} bytes (expected at least {}, layout.total_size)",
                            file_size, layout.total_size,
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Vec<u8> からエンジンを構築。WASM ではこれが唯一のエントリポイント。
    /// native でも使える（テスト、ファイル丸読みなど）。
    pub fn from_bytes(data: Vec<u8>) -> Result<Self, String> {
        Self::load_from_backing(Backing::Memory(data), /*readonly=*/ false)
    }

    /// #88 (0.12.0): v5 DB (leaf region 無し = `Leaf` 値が vocab 辞書に単調
    /// append される) の bytes を v6 (`LeafStore` あり = reclaim 対応) へ移送する
    /// 純関数版 (file I/O なし)。 返り値は `(v6 bytes, stats)`。
    ///
    /// 手順: 末尾に `leaf_data_size` の LeafStore region を新設し、 各 `Leaf`
    /// himo の live cell が持つ旧 vocab vid を辿って vocab bytes を LeafStore へ
    /// `insert`、 cell を leaf offset に書換える。 vocab / entity / himo 構造・
    /// content は byte 単位でそのまま引き継ぐ。
    ///
    /// - `leaf_data_size`: 新設 region の予約サイズ (`1..=u32::MAX`)。
    /// - `skip_himos`: vocab に据え置く himo id。 file 経路は reopen で `.tables`
    ///   が復元され reserved-table の Leaf が `leaf_for()==None` に戻るため、 その
    ///   himo を渡して移送から除外し read 整合を保つ。 Memory 経路 (reopen が
    ///   全 anonymous) は空でよい。
    ///
    /// 既知の trade-off: 旧 vocab の Leaf bytes は orphan として残る
    /// (`stats.vocab_orphan_bytes_left`)。 vocab compaction は本 migration の
    /// 対象外で、 目的は「以後の Leaf 書込みを reclaim 対象にする」こと。
    pub fn migrate_bytes_v5_to_v6(
        src: Vec<u8>,
        leaf_data_size: usize,
        skip_himos: &[u16],
    ) -> Result<(Vec<u8>, MigrationStats), String> {
        if src.len() < HEADER_SIZE || src[H_MAGIC..H_MAGIC + 4] != FILE_MAGIC {
            return Err("not an EnchuDB file".into());
        }
        let existing_leaf = u64::from_le_bytes(
            src[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8].try_into().unwrap(),
        ) as usize;
        if existing_leaf > 0 {
            // 既に leaf region あり (v6) — 移送不要。 bytes はそのまま返す。
            return Ok((src, MigrationStats { already_v6: true, ..Default::default() }));
        }
        if leaf_data_size == 0 || leaf_data_size > u32::MAX as usize {
            return Err(format!("leaf_data_size {} out of range (1..=u32::MAX)", leaf_data_size));
        }

        // header fields (load_from_backing と同じ読み出し順)。
        let max_entities = u32::from_le_bytes(src[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].try_into().unwrap());
        let max_himos = u32::from_le_bytes(src[H_MAX_HIMOS..H_MAX_HIMOS + 4].try_into().unwrap());
        let himo_count = u32::from_le_bytes(src[H_HIMO_COUNT..H_HIMO_COUNT + 4].try_into().unwrap());
        let vocab_max_entries = u32::from_le_bytes(src[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4].try_into().unwrap());
        let vocab_index_cap = u32::from_le_bytes(src[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4].try_into().unwrap());
        let vocab_data_size = u64::from_le_bytes(src[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let himoreg_max_entries = u32::from_le_bytes(src[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4].try_into().unwrap());
        let himoreg_index_cap = u32::from_le_bytes(src[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4].try_into().unwrap());
        let himoreg_data_size = u64::from_le_bytes(src[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let content_data_size = u64::from_le_bytes(src[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let cyl_max_values = u32::from_le_bytes(src[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].try_into().unwrap());

        let layout = Layout::try_from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
        )?;

        // src が v5 layout 全域 (= leaf_data_off までのバイト列) をカバーしているか。
        // leaf region は tail 追加なので v6 の leaf_data_off == v5 total_size。
        if src.len() < layout.leaf_data_off {
            return Err(format!(
                "source too small: {} bytes (expected >= {}) — truncated?",
                src.len(), layout.leaf_data_off,
            ));
        }

        // dst = v6 layout 全域。 leaf_data_off までを src からコピー、 leaf region は
        // 0 埋め (LeafStore::init が MAGIC + high_water を書く)。
        let mut dst = vec![0u8; layout.total_size];
        dst[..layout.leaf_data_off].copy_from_slice(&src[..layout.leaf_data_off]);

        // header: version=6 (CRC 範囲内)、 leaf region size (CRC 範囲外)、 CRC 再計算。
        // v5→v6 migration は byte-offset の v6 を出力 (leaf off_shift=0)。 v7 engine は
        // read-through で開ける。 16GB 超が要るなら別途 v7 で create し直す。
        dst[H_VERSION..H_VERSION + 4].copy_from_slice(&FILE_VERSION_LEGACY_V6.to_le_bytes());
        dst[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8]
            .copy_from_slice(&(layout.leaf_data_size as u64).to_le_bytes());
        write_header_crc(&mut dst);

        let type_bytes: Vec<u8> = (0..himo_count as usize).map(|hid| dst[H_HIMO_TYPES + hid]).collect();
        let skip: std::collections::HashSet<u16> = skip_himos.iter().copied().collect();

        // dst 上に region を張って cell を移送する (Region::new は非所有 view なので
        // dst は move 可能なまま)。 vocab は get のみ (readonly=true)、 leaf は新規 init。
        let base = dst.as_mut_ptr();
        let vocab = Vocabulary::load(
            unsafe { Region::new(base.add(layout.vocab_data_off), layout.vocab_data_size) },
            unsafe { Region::new(base.add(layout.vocab_offsets_off), layout.vocab_offsets_size) },
            unsafe { Region::new(base.add(layout.vocab_index_off), layout.vocab_index_size) },
            /*readonly=*/ true,
        );
        // v6 出力なので leaf offset は byte (off_shift=0)。
        let leaf = LeafStore::init(unsafe {
            Region::new(base.add(layout.leaf_data_off), layout.leaf_data_size)
        }, 0);

        let mut stats = MigrationStats::default();
        for (hid, &tb) in type_bytes.iter().enumerate() {
            if ValueType::from_byte(tb) != ValueType::Leaf { continue; }
            if skip.contains(&(hid as u16)) { continue; }
            stats.leaf_himos += 1;
            let col = Column::load(unsafe {
                Region::new(base.add(layout.himo_col_off(hid)), layout.himo_col_size)
            });
            let count = col.count();
            for eid in 0..count {
                // cell = stored 形式 (0 = 未設定、 N = 旧 vocab vid N-1)。
                let stored = u32::from_le_bytes(col.get(eid).try_into().unwrap());
                if stored == 0 { continue; }
                let old_vid = stored - 1;
                let new_off = {
                    let bytes = vocab.get(old_vid);
                    stats.bytes_moved += bytes.len() as u64;
                    leaf.insert(bytes)
                };
                col.set(eid, &(new_off + 1).to_le_bytes());
                stats.cells_moved += 1;
            }
        }
        stats.leaf_footprint = leaf.high_water();
        stats.vocab_orphan_bytes_left = stats.bytes_moved;

        Ok((dst, stats))
    }

    /// #88: v5 DB ファイル (`src_path`) を v6 に移送して `dst_path` に書く
    /// (default leaf region size)。 `src_path` は不変。 詳細は
    /// [`Self::migrate_file_v5_to_v6_with_leaf`]。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn migrate_file_v5_to_v6(src_path: &str, dst_path: &str) -> Result<MigrationStats, String> {
        Self::migrate_file_v5_to_v6_with_leaf(src_path, dst_path, DEFAULT_LEAF_DATA_SIZE)
    }

    /// #88: v5 DB ファイルを v6 に移送 (`dst_path` は `src_path` と別にすること)。
    ///
    /// - main DB (`.ecdb` 相当) を移送。 `.tables` sidecar は himo/table 構造を
    ///   そのまま引き継ぐためコピーし、 reserved-table の Leaf himo は移送から
    ///   除外して vocab に据え置く (reopen で `.tables` 復元 → `leaf_for()==None`
    ///   と整合)。
    /// - 旧 `.oplog` は **引き継がない** (dst の stale `.oplog` は削除)。 v5 の
    ///   Leaf tie op は旧 wire 形 (`Vocab`+`TieNamed`) で、 移送後の cell と
    ///   不整合になり reopen 時の replay が巻き戻すため。 dst は fresh oplog で
    ///   開く (= main file の現在状態を checkpoint とみなす)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn migrate_file_v5_to_v6_with_leaf(
        src_path: &str, dst_path: &str, leaf_data_size: usize,
    ) -> Result<MigrationStats, String> {
        let src = std::fs::read(src_path).map_err(|e| format!("read {src_path}: {e}"))?;
        let skip = Self::reserved_leaf_himos_from_sidecar(src_path, &src);
        let (dst, stats) = Self::migrate_bytes_v5_to_v6(src, leaf_data_size, &skip)?;

        // atomic 書き: tmp → rename (途中 crash で half-written dst を残さない)。
        let tmp = format!("{dst_path}.migrating");
        std::fs::write(&tmp, &dst).map_err(|e| format!("write {tmp}: {e}"))?;
        std::fs::rename(&tmp, dst_path).map_err(|e| format!("rename {tmp} -> {dst_path}: {e}"))?;

        // himo/table 構造は不変なので `.tables` をコピー。 stale な dst sidecar は掃除。
        if let Ok(t) = std::fs::read(tables_path_for(src_path)) {
            let _ = std::fs::write(tables_path_for(dst_path), t);
        } else {
            let _ = std::fs::remove_file(tables_path_for(dst_path));
        }
        let _ = std::fs::remove_file(format!("{dst_path}.oplog"));
        Ok(stats)
    }

    /// `.tables` sidecar から reserved-table 配下の `Leaf` himo id を集める
    /// (migration の skip 集合)。 sidecar が無ければ空 (= 全 anonymous 相当)。
    #[cfg(not(target_arch = "wasm32"))]
    fn reserved_leaf_himos_from_sidecar(path: &str, src: &[u8]) -> Vec<u16> {
        let tables = match load_tables_from_sidecar(path) {
            Ok(Some(t)) => t,
            _ => return Vec::new(),
        };
        let himo_count = u32::from_le_bytes(src[H_HIMO_COUNT..H_HIMO_COUNT + 4].try_into().unwrap()) as usize;
        let mut out = Vec::new();
        for t in &tables {
            if !t.is_reserved() { continue; }
            for &hid in t.himo_ids.read().unwrap().iter() {
                let h = hid as usize;
                if h < himo_count && ValueType::from_byte(src[H_HIMO_TYPES + h]) == ValueType::Leaf {
                    out.push(hid as u16);
                }
            }
        }
        out
    }

    fn load_from_backing(mut backing: Backing, readonly: bool) -> Result<Self, String> {
        let buf = backing.as_slice_mut();

        if buf.len() < HEADER_SIZE || buf[H_MAGIC..H_MAGIC + 4] != FILE_MAGIC {
            return Err("not an EnchuDB file".into());
        }
        let version = u32::from_le_bytes(buf[H_VERSION..H_VERSION + 4].try_into().unwrap());
        // v5 が現行、 v4 は後方互換で受け付ける (= anonymous table 1 個に migrate)。
        // v4 DB を v5 として open しても、 引き続き flat anonymous モードで動作する
        // (table 概念は step 2 以降の実装に伴って活性化)。
        // v7 (現行) / v6 / v5 / v4 を受け付ける。 v6 は leaf offset が byte だが、
        // LeafStore が region header の off_shift=0 で self-describing に read-through。
        if version != FILE_VERSION
            && version != FILE_VERSION_LEGACY_V6
            && version != FILE_VERSION_LEGACY_V5
            && version != FILE_VERSION_LEGACY_V4
        {
            return Err(format!(
                "unsupported EnchuDB file version {} (supported: {}, {}/{}/{} compat). dev phase — recreate the DB.",
                version, FILE_VERSION, FILE_VERSION_LEGACY_V6, FILE_VERSION_LEGACY_V5, FILE_VERSION_LEGACY_V4
            ));
        }
        // v28: ヘッダ整合性 CRC 検証(stored == 0 は v27 以前の DB として許容)
        verify_header_crc(buf)?;
        let max_entities = u32::from_le_bytes(buf[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].try_into().unwrap());
        let max_himos = u32::from_le_bytes(buf[H_MAX_HIMOS..H_MAX_HIMOS + 4].try_into().unwrap());
        let himo_count = u32::from_le_bytes(buf[H_HIMO_COUNT..H_HIMO_COUNT + 4].try_into().unwrap());
        let vocab_max_entries = u32::from_le_bytes(buf[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4].try_into().unwrap());
        let vocab_index_cap = u32::from_le_bytes(buf[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4].try_into().unwrap());
        let vocab_data_size = u64::from_le_bytes(buf[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let himoreg_max_entries = u32::from_le_bytes(buf[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4].try_into().unwrap());
        let himoreg_index_cap = u32::from_le_bytes(buf[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4].try_into().unwrap());
        let himoreg_data_size = u64::from_le_bytes(buf[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let content_data_size = u64::from_le_bytes(buf[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let cyl_max_values = u32::from_le_bytes(buf[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].try_into().unwrap());

        // 0.9.0 (L1): checked arithmetic 版で layout を計算 (破損 header の usize
        // wrap → 過小 total_size → OOB region を防ぐ)。 file 経路は open_internal →
        // validate_file_size で field sanity 済み、 Memory 経路 (from_bytes) は
        // ここが最初の防壁。
        let leaf_data_size = u64::from_le_bytes(buf[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let layout = Layout::try_from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
        )?;

        // backing が layout 全域をカバーしているか (Memory 経路の truncated bytes
        // で Region が OOB を map する事故を防ぐ。 file 経路は validate_file_size
        // が既に同等チェック + growable の set_len 拡張を済ませている)。
        if buf.len() < layout.total_size {
            return Err(format!(
                "backing too small: {} bytes (layout.total_size = {}) — truncated file?",
                buf.len(), layout.total_size,
            ));
        }

        let maxv_base = himo_maxv_base(max_himos);
        let mut type_bytes = Vec::with_capacity(himo_count as usize);
        let mut maxv_values = Vec::with_capacity(himo_count as usize);
        for hid in 0..himo_count as usize {
            type_bytes.push(buf[H_HIMO_TYPES + hid]);
            let mv_off = maxv_base + hid * 4;
            maxv_values.push(u32::from_le_bytes(buf[mv_off..mv_off + 4].try_into().unwrap()));
        }

        let base = backing.as_mut_ptr();

        // ── page-reclaim instrumentation (issue2 調査) ──
        // ENCHU_OPEN_PROFILE=1 で env 有効。 解析後、 削除する一時的計装。
        let profile = std::env::var("ENCHU_OPEN_PROFILE").is_ok();
        let pr = || -> u64 {
            #[cfg(target_os = "macos")]
            unsafe {
                let mut ru: libc::rusage = std::mem::zeroed();
                if libc::getrusage(libc::RUSAGE_SELF, &mut ru) == 0 {
                    return ru.ru_minflt as u64;
                }
                0
            }
            #[cfg(not(target_os = "macos"))]
            { 0 }
        };
        let mut t = Instant::now();
        let mut p = pr();
        let report = |label: &str, t: &mut Instant, p: &mut u64| {
            if profile {
                let np = pr();
                let dp = np - *p;
                eprintln!("[open_profile] {:>22}  Δreclaim={:>7}  Δt={:>5} ms", label, dp, t.elapsed().as_millis());
                *p = np;
                *t = Instant::now();
            }
        };

        let entities = EntitySet::load(
            unsafe { Region::new(base.add(layout.entities_off), layout.entities_size) },
            max_entities,
        );
        report("EntitySet::load", &mut t, &mut p);
        let vocab = Vocabulary::load(
            unsafe { Region::new(base.add(layout.vocab_data_off), layout.vocab_data_size) },
            unsafe { Region::new(base.add(layout.vocab_offsets_off), layout.vocab_offsets_size) },
            unsafe { Region::new(base.add(layout.vocab_index_off), layout.vocab_index_size) },
            readonly, // #77-H1: readonly は共有 index を書き換えず shadow へ rebuild
        );
        report("Vocabulary::load", &mut t, &mut p);
        let himo_reg = Vocabulary::load(
            unsafe { Region::new(base.add(layout.himoreg_data_off), layout.himoreg_data_size) },
            unsafe { Region::new(base.add(layout.himoreg_offsets_off), layout.himoreg_offsets_size) },
            unsafe { Region::new(base.add(layout.himoreg_index_off), layout.himoreg_index_size) },
            readonly,
        );
        report("himo_reg(Vocabulary)", &mut t, &mut p);
        let contents = ContentStore::load(
            unsafe { Region::new(base.add(layout.content_index_off), layout.content_index_size) },
            unsafe { Region::new(base.add(layout.content_data_off), layout.content_data_size) },
        );
        report("ContentStore::load", &mut t, &mut p);
        let leaf = if layout.leaf_data_size > 0 {
            Some(LeafStore::load(unsafe {
                Region::new(base.add(layout.leaf_data_off), layout.leaf_data_size)
            }))
        } else {
            None
        };

        // 0.9.0: capacity は max_himos だが、 header の himo_count が万一それを
        // 超えていても load 自体は落とさない (旧 Vec 実装と同じ寛容さ)。
        let himo_cap = (max_himos as usize).max(himo_count as usize);
        let himo_names: AppendVec<String> = AppendVec::with_capacity(himo_cap);
        let value_types: AppendVec<ValueType> = AppendVec::with_capacity(himo_cap);
        let himo_max_values: AppendVec<u32> = AppendVec::with_capacity(himo_cap);
        let himos: AppendVec<HimoStore> = AppendVec::with_capacity(himo_cap);

        for hid in 0..himo_count as usize {
            let ht = ValueType::from_byte(type_bytes[hid]);
            let mv = maxv_values[hid];
            let name_bytes = himo_reg.get(hid as u32);
            let name = String::from_utf8_lossy(name_bytes).to_string();
            let effective_mv = mv.min(cyl_max_values);

            let hs = HimoStore::load(
                unsafe { Region::new(base.add(layout.himo_col_off(hid)), layout.himo_col_size) },
                ht, effective_mv,
            );

            let _ = himo_names.push(name);
            let _ = value_types.push(ht);
            let _ = himo_max_values.push(mv);
            let _ = himos.push(hs);
        }
        report("HimoStore::load × N", &mut t, &mut p);

        // β-light step 2: load 時は全 himo を anonymous table に attach する。
        // step 3+ で v5 DB の table descriptor 読み出しに置き換える、 v4 DB は
        // 引き続きこの compat 経路で anonymous-only として open される。
        let initial_tables = vec![{
            let anon = TableDef::anonymous();
            *anon.himo_ids.write().unwrap() = (0..himos.len() as u32).collect();
            anon
        }];
        let initial_himo_to_table: AppendVec<std::sync::atomic::AtomicU16> =
            AppendVec::with_capacity(himo_cap);
        for _ in 0..himos.len() {
            let _ = initial_himo_to_table.push(std::sync::atomic::AtomicU16::new(ANONYMOUS_TABLE));
        }

        let eng = Self {
            path: String::new(), layout, max_entities, max_himos,
            vocab, himo_reg,
            himo_names, value_types, himo_max_values,
            himos, entities, contents,
            leaf,
            tables: initial_tables,
            himo_to_table: initial_himo_to_table,
            himo_def_lock: std::sync::Mutex::new(()),
            write_queue: None,
            shutdown_flag: None,
            consumer_handle: std::sync::Mutex::new(None),
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_append_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            oplog: None,
            oplog_record_queue: None,
            consumer_poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            warned_ref_to_replica: std::sync::atomic::AtomicBool::new(false),
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            eid_translator: std::sync::Arc::new(crate::eid_translator::EidTranslator::new()),
            keypair: std::sync::RwLock::new(None),
            pubkeys: std::sync::Arc::new(enchudb_oplog::keys::PubkeyStore::new()),
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            gossip_remote_apply: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            sync_ops_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            last_persist_warn_ms: std::sync::atomic::AtomicU64::new(0),
            next_sync_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(1)),
            transfer_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            is_readonly: std::sync::atomic::AtomicBool::new(false),
            defer_tables_persist: std::sync::atomic::AtomicBool::new(false),
            #[cfg(not(target_arch = "wasm32"))]
            _writer_lock: None, // caller (open_internal) が後から差し替える
            backing,
        };

        // v32: header から peer_id を復元
        {
            let hdr = eng.backing.header_mut();
            let peer = u32::from_le_bytes(hdr[H_PEER_ID..H_PEER_ID + 4].try_into().unwrap());
            eng.peer_id.store(peer, std::sync::atomic::Ordering::Release);
        }

        eng.rebuild();

        // clean flag を 0 に倒し、 即 msync で永続化する。 こうしないと、 この後
        // insert で index 書き換え → crash → 次 open で flag=1 のまま skip → 不整合、
        // という穴が空く。 該当 page (vocab/himo_reg の data header) だけ msync する
        // ことで、 default 25 GB layout でも 1 ms 以下で済む。
        //
        // #56: readonly open は後続 write が無いため flip 自体が不要。 かつ flip +
        // msync は file を物理的に書き換えて DB を dirty 化し、 次回 open で full
        // index rebuild を誘発する (= read-only のはずが DB を太らせる)。 readonly
        // では clean flag を一切触らない (真に非破壊 open)。
        if !readonly {
            eng.vocab.mark_index_clean(false);
            eng.himo_reg.mark_index_clean(false);
            // v6 (#88): routed-Leaf の live cell offset から LeafStore free-list を
            // 再構成 (free-list は非永続)。 これが無いと dead slot が再利用されず
            // footprint が増える。
            eng.rebuild_leaf_free_list();
            #[cfg(not(target_arch = "wasm32"))]
            {
                let _ = eng.backing.flush_range(eng.layout.vocab_data_off, 16);
                let _ = eng.backing.flush_range(eng.layout.himoreg_data_off, 16);
            }
        }

        Ok(eng)
    }

    // ──── v32: エッジ向け read-only replica ────

    /// 既存 DB を read-only replica として開く。
    /// 書き込み API (tie / untie / delete / content / entity 等) は panic する。
    /// Syncer 経由 (remote_*_apply) での書き込みのみ受け付ける。
    /// エッジ node はこちらで起動する。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_replica(path: &str) -> io::Result<Self> {
        let eng = Self::open_standalone(path)?;
        eng.is_replica.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// WAL + 並行 write queue 付きで replica open。通常の Engine と同じく Arc 共有可能。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_concurrent_replica(path: &str, oplog_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::open_concurrent_with_oplog(path, oplog_capacity)?;
        eng.is_replica.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// replica モードの動的切替。true にすると書き込み API が panic する。
    /// false に戻せば通常 DB として書き込み可能に復帰する。
    pub fn set_replica_mode(&self, on: bool) {
        self.is_replica.store(on, std::sync::atomic::Ordering::Release);
    }

    /// 現在 replica モードか。
    pub fn is_replica(&self) -> bool {
        self.is_replica.load(std::sync::atomic::Ordering::Acquire)
    }

    /// 0.7.0: 「writable か」 を panic せず bool で返す。 schema crate の
    /// load_schema 等、 readonly でも続行したい path で使う。
    #[inline]
    pub fn is_readonly(&self) -> bool {
        self.is_readonly.load(std::sync::atomic::Ordering::Acquire)
    }

    /// 書き込み API が呼ばれた時の guard。replica なら panic、 readonly でも panic。
    #[inline(always)]
    fn check_writable(&self) {
        if self.is_readonly.load(std::sync::atomic::Ordering::Acquire) {
            panic!("Engine is opened read-only (open_readonly); writes are not allowed");
        }
        if self.is_replica.load(std::sync::atomic::Ordering::Acquire) {
            panic!("Engine is in replica mode; writes must go through Syncer (remote_*_apply)");
        }
    }

    // ──── table (β-light step 3) ────

    /// 新規 table を定義し、 eid 範囲を予約する。 `size_hint=0` なら
    /// `DEFAULT_TABLE_RESERVED` を採用。
    ///
    /// 振る舞い:
    ///   - 初回の `define_table` 呼び出し時に anonymous table が現 `next_eid`
    ///     で close される。 以降 `entity()` (anonymous 用) は panic する。
    ///   - 新 table の eid 範囲は `[max(全 table の eid_range_hi), +size_hint)`。
    ///   - 範囲が `max_entities` を超えるなら error。
    ///
    /// 戻り値は確保された TableId。 step 4+ で himo を namespacing して attach
    /// する経路と組み合わせて使う。 step 3 段階では table の column 列は空。
    /// 0.7.0 (Phase 3): sync 経路の reserved table を一括定義する。
    ///
    /// - `_sync_ops`: op stream (per-row HLC ordered)、 watermark + reclaim で
    ///   未配送 tail のみが残る。 hlc / peer_id / op_type / eid / himo_id /
    ///   value / signature / pubkey_fp の 8 himo を持つ
    /// - `_sync_peers`: peer ごとの consumed_hlc (= watermark) と last_seen_at
    ///
    /// idempotent (既に存在すれば何もしない)。 Syncer attach 時 / schema crate
    /// `enable_sync` 経由で呼ばれる想定。 sync 不要な単独 DB は呼ばなくて OK
    /// (= reserved table を持たない、 eid 空間も浪費しない)。
    ///
    /// 0.7.0 では opt-in 方針。 一度有効化すると無効化は不可 (= reserved table
    /// は close できない、 ただし himo は最小)。
    pub fn enable_sync_tables(&mut self) -> Result<(), String> {
        self.check_writable();

        // _sync_ops: 未配送 op の tail。 1 row = 1 oplog record、 metadata は
        // 数値 himo (= u32 limit) で query 可、 完全 wire bytes は payload (Leaf)
        // に保存する。 hlc は (wall, logical, peer) で 16 byte なので Number 1 個
        // には収まらない → query 用は lsn (= u32 単調) で代替、 完全な
        // hlc は payload の中に居る。 publish_since の cursor は lsn ベース。
        //
        // size_hint: 平常時は ack-driven reclaim で数 K rows 程度に保たれる想定。
        // 0.7.0 では lazy purge (= 古い row を delete のみ、 eid 空間は再利用せず)、
        // 0.8.0 で ring buffer 化を検討。 max_entities が小さい (= tiny preset)
        // 環境でも overflow しないよう remaining の 1/2 を `_sync_ops` に、
        // 1/16 を `_sync_peers` に割り当てる (= 最大は 1 M / 1 K で cap)。
        let remaining = self.remaining_eid_space();
        let sync_ops_size = (remaining / 2).min(1_048_576).max(64);
        let sync_peers_size = (remaining / 16).min(1024).max(8);
        if !self.has_reserved_table("_sync_ops") {
            self.define_reserved_table("_sync_ops", sync_ops_size)?;
            // lsn: u32 単調 (= publish_since cursor)
            self.define_himo_in("_sync_ops", "lsn", ValueType::Number, 0)?;
            // peer_id: record の author_peer (= filter 用)
            self.define_himo_in("_sync_ops", "peer_id", ValueType::Number, 0)?;
            // op_type: 0=Tie, 1=Untie, 2=Delete, 3=Content, 4=Commit, 5=Schema, 6=Vocab
            self.define_himo_in("_sync_ops", "op_type", ValueType::Number, 0)?;
            // hlc_wall_lo: hlc.wall (u64 ms-since-epoch) の下位 32bit
            // 完全な hlc は payload の中、 これは粗 filter / debug 用。
            self.define_himo_in("_sync_ops", "hlc_wall_lo", ValueType::Number, 0)?;
            // payload: 完全な oplog record wire bytes (header 含む)、 Leaf で dedupe なし
            self.define_himo_in("_sync_ops", "payload", ValueType::Leaf, 0)?;
        }

        // _sync_peers: peer ごと watermark。 100 peer 想定で 1 K の枠 (上で計算済み)。
        if !self.has_reserved_table("_sync_peers") {
            self.define_reserved_table("_sync_peers", sync_peers_size)?;
            // peer_id: PK
            self.define_himo_in("_sync_peers", "peer_id", ValueType::Number, 0)?;
            // consumed_lsn: 当該 peer が ack した最後の _sync_ops.lsn
            self.define_himo_in("_sync_peers", "consumed_lsn", ValueType::Number, 0)?;
            // last_seen_at: 最後の活動時刻 (= 観測用、 unix ms / 2^16 等で u32 に収める想定)
            self.define_himo_in("_sync_peers", "last_seen_at", ValueType::Number, 0)?;
        }

        // #77-H6: 既存 DB の再 enable (reopen 経路) では、 body に残る過去 rows /
        // watermark から lsn 採番を復元する。
        self.rehydrate_next_sync_lsn();

        Ok(())
    }

    /// #77-H6: `next_sync_lsn` を既存 `_sync_ops` rows と `_sync_peers.consumed_lsn`
    /// から復元する。 全 constructor が 1 固定で初期化するため、 reopen 後にこれを
    /// 呼ばないと lsn 採番が 1 に戻り、 peer の ack 済み watermark より小さい lsn
    /// が発行されて `pending_sync_ops(since)` が永久に空 = **再起動後の全 write が
    /// 既存 peer に配信されない** silent loss になっていた。
    fn rehydrate_next_sync_lsn(&self) {
        use std::sync::atomic::Ordering;
        if !self.sync_tables_enabled() { return; }
        let mut max_lsn = 0u32;
        if let Some(lsn_hid) = self.himo_id("_sync_ops.lsn") {
            let hid = lsn_hid as u16;
            for eid in self.entities_with_himo(hid) {
                if let Some(l) = self.get_by_id(eid, hid) {
                    if l > max_lsn { max_lsn = l; }
                }
            }
        }
        if let Some(cons_hid) = self.himo_id("_sync_peers.consumed_lsn") {
            let hid = cons_hid as u16;
            for eid in self.entities_with_himo(hid) {
                if let Some(l) = self.get_by_id(eid, hid) {
                    if l > max_lsn { max_lsn = l; }
                }
            }
        }
        if max_lsn > 0 {
            // 既に進んでいる場合は後退させない (enable → open の二重呼び等)
            let cur = self.next_sync_lsn.load(Ordering::Acquire);
            if max_lsn.saturating_add(1) > cur {
                self.next_sync_lsn.store(max_lsn + 1, Ordering::Release);
            }
        }
    }

    /// 0.7.0 (Phase 3): sync tables が有効化済みか。 schema crate / Syncer が
    /// 「`_sync_ops` に insert すべきか」 判断する fast path。
    #[inline]
    pub fn sync_tables_enabled(&self) -> bool {
        self.has_reserved_table("_sync_ops") && self.has_reserved_table("_sync_peers")
    }

    /// 0.7.0 (Phase 4): oplog の `sync_ops_offset` 以降の record を `_sync_ops`
    /// table へ転送する。 consumer thread が背景 fsync 後に呼ぶ。
    ///
    /// 返り値: 転送した record 数。 enable_sync 未呼出 / oplog 未有効化なら 0。
    pub fn transfer_oplog_to_sync_ops(&self) -> usize {
        use std::sync::atomic::Ordering;
        if !self.sync_tables_enabled() { return 0; }
        let Some(wal) = self.oplog.as_ref() else { return 0; };

        // 0.8.11: background consumer thread と手動呼び出しの race で
        // 同じ records が重複転送される bug fix。 from load → records pull →
        // row insert → offset store の 4 step を排他化。 per-fsync 頻度 (= 100ms
        // 周期) なので lock 競合の hot path 影響なし。
        let _guard = self.transfer_lock.lock().unwrap();

        let from = self.sync_ops_offset.load(Ordering::Acquire);
        // #77-H4: cursor は「読み切った commit 済み group の終端」までしか
        // 進めない。 scan 後の wal.head() 再読は、 書き込み途中 record での
        // break 位置〜head 間の record を恒久 skip していた (#63 と同 class)。
        let (records, committed_end) = wal.iter_committed_from_with_end(from);
        if records.is_empty() {
            // 空 commit group だけ読み進んだ場合も cursor は安全に前進できる
            self.sync_ops_offset.store(committed_end, Ordering::Release);
            return 0;
        }

        // himo_id を 1 度 lookup (= hot path での文字列引きを避ける)
        let lsn_hid = match self.himo_id("_sync_ops.lsn") { Some(h) => h as u16, None => return 0 };
        let peer_id_hid = self.himo_id("_sync_ops.peer_id").unwrap() as u16;
        let op_type_hid = self.himo_id("_sync_ops.op_type").unwrap() as u16;
        let hlc_wall_lo_hid = self.himo_id("_sync_ops.hlc_wall_lo").unwrap() as u16;
        let payload_hid = self.himo_id("_sync_ops.payload").unwrap() as u16;

        // 0.8.11: 自己再帰 sync の循環を断つ filter。
        // `_sync_ops` / `_sync_peers` 配下の write (= ack_sync の watermark
        // update、 transfer 自体の row insert) を queue に積むと、 reclaim 後も
        // lsn が新しくて消えない残骸として蓄積、 stress_10k_cycle の
        // `final_pending < 100` 期待を壊す (= 実測 ~25%)。 これらは local-only
        // state で他 peer に sync 不要なので、 transfer 対象から除外。
        let sync_ops_range = self.table_eid_range("_sync_ops");
        let sync_peers_range = self.table_eid_range("_sync_peers");
        let is_internal_eid = |eid: u64| -> bool {
            let local = enchudb_oplog::eid_local(eid);
            if let Some((lo, hi)) = sync_ops_range {
                if local >= lo && local < hi { return true; }
            }
            if let Some((lo, hi)) = sync_peers_range {
                if local >= lo && local < hi { return true; }
            }
            false
        };

        let mut count = 0usize;
        for rec in &records {
            // 自己再帰 op は skip。 Commit (= barrier marker) / Vocab (= global
            // sync 必須) は eid 持たないのでそのまま通す。
            let skip = match &rec.op {
                enchudb_oplog::oplog::DecodedOp::Tie { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::Untie { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::Delete { eid }
                | enchudb_oplog::oplog::DecodedOp::Content { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::TieNamed { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::TieLeaf { eid, .. } => is_internal_eid(*eid),
                enchudb_oplog::oplog::DecodedOp::Commit => false,
                enchudb_oplog::oplog::DecodedOp::Vocab { .. } => false,
            };
            if skip { continue; }
            // 0.11 (request10 / #76 逆写像): translated foreign entity への
            // **self-authored** write は、 元 entity の世界番号に宛名を書き戻して
            // bridge する (lsn / hlc / author は維持、 eid を含む payload は再署名)。
            // 受信側は eid_peer(eid) をキーに翻訳する (phase 2) ので全 peer で
            // 同一 entity に収束し、 衝突は HLC LWW が裁く。 0.9.0-0.10.x の
            // single-writer guard はこれで撤去。
            //
            // 例外: **Ref 値が translated local を指す write は発送不能**。 wire の
            // value は u32 で世界番号 (u64) を運べない (= wire 拡張が要る、
            // request10 follow-up) ため skip + 一度だけ warn で local-only に
            // 留める。 これは 0.10.x まで silent に断片化していた潜在バグの封鎖。
            let self_peer = wal.peer_id();
            let mut resigned: Option<enchudb_oplog::oplog::ResignedRecord> = None;
            if rec.author_peer == self_peer {
                let ref_to_replica = match &rec.op {
                    enchudb_oplog::oplog::DecodedOp::Tie { himo_id, value, .. } => {
                        self.himo_is_ref(*himo_id)
                            && self.eid_translator.is_translated_local(*value)
                    }
                    enchudb_oplog::oplog::DecodedOp::TieNamed { himo_kind, value, .. } => {
                        *himo_kind == crate::himo_store::ValueType::Ref as u8
                            && self.eid_translator.is_translated_local(*value)
                    }
                    _ => false,
                };
                if ref_to_replica {
                    if !self.warned_ref_to_replica.swap(true, Ordering::Relaxed) {
                        eprintln!(
                            "[enchudb] warning: local Ref write pointing at a replicated \
                             foreign entity is NOT propagated to peers (u32 wire value \
                             cannot carry a foreign entity id, request10 follow-up)."
                        );
                    }
                    continue;
                }
                let row_local = match &rec.op {
                    enchudb_oplog::oplog::DecodedOp::Tie { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::Untie { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::Delete { eid }
                    | enchudb_oplog::oplog::DecodedOp::Content { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::TieNamed { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::TieLeaf { eid, .. } => {
                        Some(enchudb_oplog::eid_local(*eid))
                    }
                    _ => None,
                };
                if let Some(local) = row_local {
                    if let Some((owner, owner_local)) = self.eid_translator.reverse(local) {
                        let world = enchudb_oplog::make_eid(owner, owner_local);
                        let kp_guard = self.keypair.read().unwrap();
                        let kp = kp_guard.as_deref();
                        match enchudb_oplog::oplog::resign_with_eid(rec, world, kp) {
                            Some(r) => resigned = Some(r),
                            // eid 持ち op で None は起きないはずだが、 起きたら
                            // 発送せず local-only に留める (安全側)
                            None => continue,
                        }
                    }
                }
            }
            count += 1;
            let row_eid = match self.entity_in("_sync_ops") {
                Ok(e) => e,
                Err(_) => {
                    // eid_range exhausted: 0.7.0 では追加 row insert を諦める
                    // (= 0.8.0 で ring buffer 化)。 ここまでの転送は維持。
                    // #77-H4: 旧実装は wal.head() まで飛ばして書き込み途中
                    // record まで skip していた。 committed_end 止まりに変更
                    // (残り未転送分はこの group 内のため落ちるが、 in-flight
                    // record の恒久 skip は防ぐ)。
                    self.sync_ops_offset.store(committed_end, Ordering::Release);
                    return count;
                }
            };
            let lsn = self.next_sync_lsn.fetch_add(1, Ordering::AcqRel);
            self.tie_to_by_id(row_eid, lsn_hid, lsn);
            self.tie_to_by_id(row_eid, peer_id_hid, rec.author_peer);
            // DecodedOp variant を tag (Tie=0, Untie=1, Delete=2, Content=3, Commit=4, Vocab=5, TieNamed=6, TieLeaf=7)
            let op_type = match &rec.op {
                enchudb_oplog::oplog::DecodedOp::Tie { .. } => 0,
                enchudb_oplog::oplog::DecodedOp::Untie { .. } => 1,
                enchudb_oplog::oplog::DecodedOp::Delete { .. } => 2,
                enchudb_oplog::oplog::DecodedOp::Content { .. } => 3,
                enchudb_oplog::oplog::DecodedOp::Commit => 4,
                enchudb_oplog::oplog::DecodedOp::Vocab { .. } => 5,
                enchudb_oplog::oplog::DecodedOp::TieNamed { .. } => 6,
                enchudb_oplog::oplog::DecodedOp::TieLeaf { .. } => 7,
            };
            self.tie_to_by_id(row_eid, op_type_hid, op_type);
            // hlc.wall は u64 ms-since-epoch、 下位 32bit のみ保持 (= ~50 日サイクル
            // で wrap するが、 lsn 順序で query するので debug/filter 程度の用途)
            self.tie_to_by_id(row_eid, hlc_wall_lo_hid, rec.hlc.wall as u32);
            // 0.8.0 phase 2: payload は signature(64) + pubkey_fp(8) + signed_bytes(rest)
            // の concat 形式。 0.7.0 では signed_bytes のみだったが、 publish path を
            // _sync_ops 経由にするため signature 込みで保存し、 sync crate で完全な
            // WireRecord に復元できるよう拡張した。 wire format breaking、 0.7.x との
            // 並走 sync は不可。
            // 0.11: 逆写像で宛名を書き戻した record は再署名済み payload を使う
            let (sig, fp, sb): (&[u8; 64], &[u8; 8], &[u8]) = match &resigned {
                Some(r) => (&r.signature, &r.pubkey_fp, &r.signed_bytes[..]),
                None => (&rec.signature, &rec.pubkey_fp, &rec.signed_bytes[..]),
            };
            let mut wire_payload = Vec::with_capacity(72 + sb.len());
            wire_payload.extend_from_slice(sig);
            wire_payload.extend_from_slice(fp);
            wire_payload.extend_from_slice(sb);
            self.tie_bytes_to_by_id(row_eid, payload_hid, &wire_payload);
        }

        // 全部転送できた、 offset を「読み切った commit 済み終端」に進める (#77-H4)
        self.sync_ops_offset.store(committed_end, Ordering::Release);
        count
    }

    /// oplog ring buffer が `try_reset` で head を HEADER_SIZE に巻き戻したとき、
    /// bridge cursor (`sync_ops_offset`) も HEADER_SIZE に戻す。
    ///
    /// #63 fix の regression 対策: try_reset は head/checkpoint しか戻さないため、
    /// これを呼ばないと sync_ops_offset が古い head を指したまま取り残され、
    /// reset 後に append された record が `from > head` で transfer 対象から外れ、
    /// `_sync_ops` に bridge されず sync から無言で欠落する。 reset と同じ
    /// consumer thread から、 transfer 完了 (= offset==旧head) かつ pending==0 の
    /// 直後にのみ呼ぶこと。
    pub fn reset_sync_ops_offset(&self) {
        self.sync_ops_offset.store(
            enchudb_oplog::oplog::HEADER_SIZE as u64,
            std::sync::atomic::Ordering::Release,
        );
    }

    /// 0.7.0 (Phase 4): peer の watermark を更新する。 Syncer が peer から ack を
    /// 受け取ったタイミングで呼ぶ。 既存 row があれば update、 無ければ insert。
    /// `_sync_peers.consumed_lsn` を idempotent に更新。
    pub fn ack_sync(&self, peer: enchudb_oplog::PeerId, consumed_lsn: u32) -> Result<(), String> {
        if !self.sync_tables_enabled() {
            return Err("sync tables not enabled (call enable_sync first)".into());
        }
        let peer_id_hid = self.himo_id("_sync_peers.peer_id")
            .ok_or("missing _sync_peers.peer_id himo")? as u16;
        let consumed_lsn_hid = self.himo_id("_sync_peers.consumed_lsn")
            .ok_or("missing _sync_peers.consumed_lsn himo")? as u16;
        let last_seen_hid = self.himo_id("_sync_peers.last_seen_at")
            .ok_or("missing _sync_peers.last_seen_at himo")? as u16;

        // 既存 peer row を探す: peer_id 値で query
        let existing = self.query_by_id(&[(peer_id_hid, peer)]);
        let row_eid = match existing.into_iter().next() {
            Some(e) => e,
            None => self.entity_in("_sync_peers")
                .map_err(|e| format!("entity_in(_sync_peers): {e}"))?,
        };

        self.tie_to_by_id(row_eid, peer_id_hid, peer);
        self.tie_to_by_id(row_eid, consumed_lsn_hid, consumed_lsn);
        // last_seen_at: 現在時刻 (ms / 65536 で u32 に収める = ~3000 年サイクル)
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.tie_to_by_id(row_eid, last_seen_hid, (now_ms / 65536) as u32);
        Ok(())
    }

    /// 0.7.0 (Phase 4): 全 peer の最小 consumed_lsn (= reclaim 安全点)。
    /// peer 0 件なら 0 を返す (= 「まだ誰も ack してない、 reclaim 不可」)。
    pub fn sync_watermark(&self) -> u32 {
        if !self.sync_tables_enabled() { return 0; }
        let Some(peer_id_hid) = self.himo_id("_sync_peers.peer_id") else { return 0; };
        let Some(consumed_lsn_hid) = self.himo_id("_sync_peers.consumed_lsn") else { return 0; };

        let peer_rows = self.entities_with_himo(peer_id_hid as u16);
        if peer_rows.is_empty() { return 0; }

        let mut min_lsn = u32::MAX;
        for eid in peer_rows {
            if let Some(v) = self.get_by_id(eid, consumed_lsn_hid as u16) {
                if v < min_lsn { min_lsn = v; }
            }
        }
        if min_lsn == u32::MAX { 0 } else { min_lsn }
    }

    /// 0.7.0 (Phase 4): `_sync_ops` の `lsn < watermark` row を削除する。
    /// 0.7.0 lazy purge 方針: entity delete のみ、 eid 空間は再利用しない
    /// (= 0.8.0 で ring buffer 化検討)。 返り値: 削除した row 数。
    pub fn reclaim_sync_ops(&self) -> usize {
        if !self.sync_tables_enabled() { return 0; }
        let watermark = self.sync_watermark();
        if watermark == 0 { return 0; }

        let Some(lsn_hid) = self.himo_id("_sync_ops.lsn") else { return 0; };
        let lsn_hid_u16 = lsn_hid as u16;

        // 0.8.0: _sync_ops table の eid_range_lo を取り、 reclaim 後 free list に
        // local id を push する (= ring buffer 化)。
        let sync_ops_meta = self.tables.iter()
            .find(|t| t.name == "_sync_ops")
            .map(|t| (t.eid_range_lo, t.free_locals.clone(), t.free_locals_nonempty.clone()));

        // _sync_ops 全 row を走査して lsn < watermark を delete
        let rows = self.entities_with_himo(lsn_hid_u16);
        let mut purged = 0;
        for eid in rows {
            if let Some(lsn) = self.get_by_id(eid, lsn_hid_u16) {
                if lsn < watermark {
                    let local = enchudb_oplog::eid_local(eid);
                    self.delete(eid);
                    // 0.8.0: free list に追加 (= 次回 entity_in("_sync_ops") で再利用)
                    if let Some((lo, ref free_list, ref nonempty)) = sync_ops_meta {
                        if local >= lo {
                            free_list.lock().unwrap().push(local - lo);
                            nonempty.store(true, std::sync::atomic::Ordering::Release);
                        }
                    }
                    purged += 1;
                }
            }
        }
        purged
    }

    /// 0.7.0 (Phase 5): 現時点の sync lsn (= 次に転送される record の lsn - 1)。
    /// `transfer_oplog_to_sync_ops` で割当て済みの max lsn。 snapshot 取得時に
    /// 「snapshot 時点でここまで配信済み」 を表すマーカーとして使う。
    pub fn current_sync_lsn(&self) -> u32 {
        self.next_sync_lsn.load(std::sync::atomic::Ordering::Acquire).saturating_sub(1)
    }

    /// 0.7.0 (Phase 4): `_sync_ops` の `lsn > since_lsn` row を全 himo set で
    /// 返す。 Syncer の publish_since が「peer.consumed_lsn より新しい op を
    /// 流す」 用途で呼ぶ。 返り値の各 entry は payload (= 完全 wire bytes)。
    pub fn pending_sync_ops(&self, since_lsn: u32) -> Vec<Vec<u8>> {
        if !self.sync_tables_enabled() { return Vec::new(); }
        let Some(lsn_hid) = self.himo_id("_sync_ops.lsn") else { return Vec::new(); };
        let Some(payload_hid) = self.himo_id("_sync_ops.payload") else { return Vec::new(); };
        let lsn_hid_u16 = lsn_hid as u16;
        let payload_hid_u16 = payload_hid as u16;

        let rows = self.entities_with_himo(lsn_hid_u16);
        let mut pairs: Vec<(u32, Vec<u8>)> = Vec::new();
        for eid in rows {
            let lsn = match self.get_by_id(eid, lsn_hid_u16) {
                Some(l) => l,
                None => continue,
            };
            if lsn <= since_lsn { continue; }
            let payload_vid = match self.get_by_id(eid, payload_hid_u16) {
                Some(v) => v,
                None => continue,
            };
            let bytes = self.vocab.get(payload_vid).to_vec();
            pairs.push((lsn, bytes));
        }
        // lsn 順
        pairs.sort_by_key(|(lsn, _)| *lsn);
        pairs.into_iter().map(|(_, b)| b).collect()
    }

    pub fn define_table(&mut self, name: &str, size_hint: u32) -> Result<TableId, String> {
        // 0.7.0: 公開 API。 `_` 始まりは reserved 命名空間、 user 経路では拒否。
        if is_reserved_table_name(name) {
            return Err(format!(
                "table name '{}' starts with '_' (reserved namespace); use define_reserved_table for internal tables",
                name,
            ));
        }
        self.define_table_inner(name, size_hint)
    }

    /// 0.7.0: engine / schema 層 internal 用の reserved table を作る。
    /// 名前は必ず `_` で始まること (`_schema_meta` / `_sync_ops` / `_sync_peers` 等)。
    /// `list_user_tables()` から除外される、 schema crate も公開 API では非露出。
    ///
    /// 用途:
    /// - schema crate: `_schema_meta` (schema blob / table 名 intern の置き場)
    /// - sync 経路 (issue #11): `_sync_ops` / `_sync_peers` (watermark + reclaim)
    pub fn define_reserved_table(&mut self, name: &str, size_hint: u32) -> Result<TableId, String> {
        if !is_reserved_table_name(name) {
            return Err(format!(
                "reserved table name '{}' must start with '_'",
                name,
            ));
        }
        self.define_table_inner(name, size_hint)
    }

    /// `define_table` / `define_reserved_table` の共通 path。 命名規約 check は
    /// 呼び元 (public API) で済ませる。
    fn define_table_inner(&mut self, name: &str, size_hint: u32) -> Result<TableId, String> {
        self.check_writable();
        if name.is_empty() {
            return Err("table name must be non-empty".into());
        }
        if self.tables.iter().any(|t| t.name == name) {
            return Err(format!("table '{}' already exists", name));
        }
        if self.tables.len() >= TableId::MAX as usize {
            return Err(format!("table count exceeds max ({})", TableId::MAX));
        }
        let size = if size_hint == 0 { DEFAULT_TABLE_RESERVED } else { size_hint };

        // 1) anonymous を現 next_eid で close (まだ open なら)
        let cur_next_eid = self.entities.next_eid();
        let anon = &mut self.tables[ANONYMOUS_TABLE as usize];
        if anon.eid_range_hi == u32::MAX {
            anon.eid_range_hi = cur_next_eid;
        }

        // 2) 新 table の eid 範囲を確保 (= 既存 table 群の hi の max から開始)
        let new_lo = self
            .tables
            .iter()
            .map(|t| t.eid_range_hi)
            .max()
            .unwrap_or(0);
        let new_hi = new_lo
            .checked_add(size)
            .ok_or_else(|| "eid space overflow (u32::MAX)".to_string())?;
        if new_hi > self.max_entities {
            return Err(format!(
                "table '{}' eid range [{}, {}) exceeds max_entities {}",
                name, new_lo, new_hi, self.max_entities,
            ));
        }

        let tid = self.tables.len() as TableId;
        self.tables.push(TableDef {
            name: name.to_string(),
            himo_ids: std::sync::RwLock::new(Vec::new()),
            eid_range_lo: new_lo,
            eid_range_hi: new_hi,
            fk_refs: Vec::new(),
            next_local: std::sync::atomic::AtomicU32::new(0),
            free_locals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            free_locals_nonempty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        self.try_persist_tables();
        Ok(tid)
    }

    /// 指定 table 内に entity を割り当てる。 anonymous table 名は受け付けず、
    /// 旧来の `entity()` を使う必要がある (互換維持)。
    ///
    /// 0.7.0: `&self` 化。 `next_local` は AtomicU32 で CAS-safe に払出される
    /// ので、 schema crate / SQL 層の hot path (Arc<Engine> 経由の concurrent
    /// mode) からも呼べる。 capacity check は load → fetch_add の seq の
    /// rollback 不要 (= 1 ずつ進む単調払出、 overflow は次回 check で弾く)。
    pub fn entity_in(&self, table_name: &str) -> Result<enchudb_oplog::EntityId, String> {
        use std::sync::atomic::Ordering;
        self.check_writable();
        if table_name.is_empty() {
            return Err("entity_in: table name must be non-empty (use entity() for anonymous)".into());
        }
        let tid = self
            .tables
            .iter()
            .position(|t| t.name == table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;
        let table = &self.tables[tid];

        // 0.8.0: free list 優先 — reclaim で解放された local id があれば再利用
        // (= ring buffer 化、 `_sync_ops` の長期運用で eid 飽和を防ぐ)。
        // 再利用時は entities.allocate_at で live mark を戻す + concurrent
        // barrier も走らせる (= 新規 alloc path と同じ orchestration)。
        //
        // fast path: free_locals_nonempty が false (= user table の常態) なら
        // mutex を取らずに通常 alloc に進む。 1M insert hot path で mutex
        // overhead (= 約 50 ns/op) を消す最適化。
        let reused_global = if table.free_locals_nonempty.load(Ordering::Acquire) {
            let mut fl = table.free_locals.lock().unwrap();
            let popped = fl.pop();
            if fl.is_empty() {
                table.free_locals_nonempty.store(false, Ordering::Release);
            }
            popped.map(|local| table.eid_range_lo + local)
        } else {
            None
        };
        if let Some(global) = reused_global {
            self.entities.allocate_at(global);
            if let Some(q) = self.write_queue.as_ref() {
                q.push(crate::write_queue::Op::EntityCreated { local: global });
                self.push_count.fetch_add(1, Ordering::Release);
            }
            let peer = self.peer_id.load(Ordering::Acquire);
            return Ok(enchudb_oplog::make_eid(peer, global));
        }

        // capacity check (= fetch_add 後の load で再 check して overflow 検出)
        let table_size = table.eid_range_hi - table.eid_range_lo;
        let cur = table.next_local.fetch_add(1, Ordering::AcqRel);
        if cur >= table_size {
            // 払出した分を rollback (= overflow 状態を維持しないため厳密には
            // 必要だが、 単調 monotone な next_local なので少々超過しても
            // 次回以降の check で確実に弾ける。 ここは error を返すのみ)。
            return Err(format!(
                "table '{}' eid range exhausted ({} eids reserved)",
                table_name, table_size,
            ));
        }
        let global = table.eid_range_lo + cur;

        // EntitySet で live mark + next_eid 前進 (CAS safe)
        self.entities.allocate_at(global);

        // concurrent mode barrier (entity() と同じ)
        if let Some(q) = self.write_queue.as_ref() {
            q.push(crate::write_queue::Op::EntityCreated { local: global });
            self.push_count.fetch_add(1, Ordering::Release);
        }
        let peer = self.peer_id.load(Ordering::Acquire);
        Ok(enchudb_oplog::make_eid(peer, global))
    }

    /// #9: persist 用に翻訳写像 + 各 entity の foreign tombstone HLC を集める。 tombstone は
    /// HlcStore の `(translated_eid, u16::MAX)` から引く (未削除なら `Hlc::ZERO`)。 reopen 時に
    /// `.eidmap` v2 から復元され、 削除済み foreign entity の resurrection を防ぐ。
    /// 呼ばれるのは persist trigger (consumer tick / 明示 persist) のみで read hot path 外。
    fn eidmap_entries_with_tombstones(&self) -> Vec<EidmapEntry> {
        let self_peer = self.peer_id();
        self.eid_translator
            .snapshot()
            .into_iter()
            .map(|(peer, foreign_local, local)| {
                let tomb = self
                    .hlc_store
                    .get(enchudb_oplog::make_eid(self_peer, local), u16::MAX)
                    .unwrap_or(enchudb_oplog::Hlc::ZERO);
                (peer, foreign_local, local, tomb)
            })
            .collect()
    }

    /// 0.8.1: `&self` で tables sidecar を強制 persist する public API。
    /// `Arc<Engine>` (= concurrent mode) でも `flush(&mut)` を取れない状況で
    /// `next_local` を含む tables 状態を disk に固める用途。
    ///
    /// short-lived CLI (= 1 write → drop) で sinfo 等の embed consumer が
    /// 明示的に呼ぶ想定。 wasm / memory-only (= path 空) では Ok(()) no-op。
    /// persist 失敗時は呼び出し側に io::Error を返す (= `try_persist_tables`
    /// と違って best-effort ではない、 fail-fast)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn persist_tables(&self) -> io::Result<()> {
        if self.path.is_empty() {
            return Ok(());
        }
        persist_tables_to_sidecar(&self.path, &self.tables)?;
        // #9: 翻訳テーブルも同じ trigger で persist (next_local と整合させる)。
        persist_eidmap_to_sidecar(&self.path, &self.eidmap_entries_with_tombstones())
    }

    /// 0.8.7: DB ファイルパスを返す。 schema crate が schema sidecar
    /// (`{path}.schema`) を atomic write するために必要。 memory-only
    /// (= from_bytes) では空文字列。
    pub fn db_path(&self) -> &str {
        &self.path
    }

    /// β-light step 7: 現 tables Vec を sidecar に保存する (best effort)。
    /// memory-only (from_bytes) や wasm では no-op。 path が空なら skip。
    /// 0.8.2: `defer_tables_persist` が立ってる時 (schema crate の build phase)
    /// は no-op。 finish 時に explicit `persist_tables()` で 1 回 fsync する。
    fn try_persist_tables(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if self.defer_tables_persist.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            if !self.path.is_empty() {
                if let Err(e) = persist_tables_to_sidecar(&self.path, &self.tables) {
                    // best effort: panic せずログだけ。 user table の定義は
                    // メモリには反映されてる、 次回 reopen で失われるだけ。
                    // 0.8.15 (issue #52): ENOSPC 等で consumer thread が毎 batch
                    // 失敗 → 同 warning がターミナル不能になる現象を回避するため、
                    // 1 秒 1 行の rate-limit を入れる。
                    self.warn_persist_failure_rate_limited(&e);
                }
                // #9: 翻訳テーブルも tables と同じ trigger で persist (next_local と
                // 整合させる)。 entries 空なら no-op (sync してない DB に file を作らない)。
                if let Err(e) =
                    persist_eidmap_to_sidecar(&self.path, &self.eidmap_entries_with_tombstones())
                {
                    self.warn_persist_failure_rate_limited(&e);
                }
            }
        }
    }

    /// 0.8.15 (issue #52): persist 失敗 warning を 1 秒 1 行に rate-limit。
    /// 高頻度 write hot path で ENOSPC が起きると consumer thread が毎 batch
    /// (= 100ms 周期) eprintln してターミナル使用不能になっていた。
    #[cfg(not(target_arch = "wasm32"))]
    fn warn_persist_failure_rate_limited(&self, e: &io::Error) {
        use std::sync::atomic::Ordering;
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = self.last_persist_warn_ms.load(Ordering::Relaxed);
        // 1 秒以上経過してたら CAS で前進させて emit (= 競合時はもう片方が emit 担当)。
        if now_ms.saturating_sub(last) >= 1000
            && self
                .last_persist_warn_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            eprintln!(
                "warning: failed to persist tables sidecar: {} (rate-limited to 1/s)",
                e
            );
        }
    }

    /// 0.8.2: build phase の sidecar fsync 抑止 toggle。 schema crate の
    /// `Database::create → build×N → finish_*` で N×fsync を 1 回に圧縮する
    /// 内部 hook。 true 中は `try_persist_tables` が no-op、 false に
    /// 戻すタイミングで explicit に `persist_tables()` を呼ぶこと。
    /// 普通の Engine 直利用 (= schema 層なし) で叩く必要は無い。
    pub fn set_defer_tables_persist(&self, defer: bool) {
        self.defer_tables_persist.store(defer, std::sync::atomic::Ordering::Release);
    }

    /// β-light step 7: sidecar から復元した tables を採用、 himo_to_table も
    /// それに合わせて再構築する。 load_from_backing 後 caller で path 設定後に呼ぶ。
    #[cfg(not(target_arch = "wasm32"))]
    fn adopt_persisted_tables(&mut self, tables: Vec<TableDef>) {
        let himo_count = self.himos.len();
        // 全 himo を一旦 anonymous default に戻し、 sidecar に書かれてる
        // attach を上書きする (= sidecar が source of truth)。
        // `&mut self` (= open 時、 共有前) なので AppendVec ごと作り直して OK。
        let rebuilt: AppendVec<std::sync::atomic::AtomicU16> =
            AppendVec::with_capacity(self.himo_to_table.capacity().max(himo_count));
        for _ in 0..himo_count {
            let _ = rebuilt.push(std::sync::atomic::AtomicU16::new(ANONYMOUS_TABLE));
        }
        for (tid, table) in tables.iter().enumerate() {
            for &hid in table.himo_ids.read().unwrap().iter() {
                if (hid as usize) < himo_count {
                    rebuilt[hid as usize]
                        .store(tid as TableId, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        self.himo_to_table = rebuilt;
        self.tables = tables;
    }

    /// 既知 table を `(id, name, eid_range)` で列挙する。 試験用 / debug 用 API。
    /// 0.7.0 以降は **reserved table も含む** (= sync 経路 / schema crate 内部用)。
    /// user code に見せる場合は `list_user_tables` を使うこと。
    pub fn list_tables(&self) -> Vec<(TableId, String, u32, u32)> {
        self.tables
            .iter()
            .enumerate()
            .map(|(i, t)| (i as TableId, t.name.clone(), t.eid_range_lo, t.eid_range_hi))
            .collect()
    }

    /// 0.7.0: user table のみを列挙 (= anonymous と reserved `_*` を除外)。
    /// schema crate `list_tables()` / SQL `SHOW TABLES` 等の user 向け API で
    /// これを使う。
    pub fn list_user_tables(&self) -> Vec<(TableId, String, u32, u32)> {
        self.tables
            .iter()
            .enumerate()
            .filter(|(i, t)| *i != ANONYMOUS_TABLE as usize && !t.is_reserved())
            .map(|(i, t)| (i as TableId, t.name.clone(), t.eid_range_lo, t.eid_range_hi))
            .collect()
    }

    /// 0.8.6: 指定 table 名の `(eid_range_lo, eid_range_hi)` を引く。 schema
    /// crate の `Table::sum/count/group_sum` が table-scoped 集計を
    /// `sum_range` / `count_range` / `group_sum_range` に bind するのに使う。
    /// 未定義 table は None。
    pub fn table_eid_range(&self, name: &str) -> Option<(u32, u32)> {
        self.tables.iter().find(|t| t.name == name).map(|t| (t.eid_range_lo, t.eid_range_hi))
    }

    /// 0.8.7: 登録済 himo の総数。 schema crate の synthesize fallback (= engine 直
    /// DB を `Database::open` した時の schema 復元) で iterate するために使う。
    pub fn himo_count(&self) -> usize {
        self.himo_names.len()
    }

    /// 0.8.7: index 直で himo の full name (= `{table}.{col}` or anonymous) を返す。
    pub fn himo_name_at(&self, idx: usize) -> Option<&str> {
        self.himo_names.get(idx).map(|s| s.as_str())
    }

    /// 0.8.7: index 直で himo の type (Tag/Number/Leaf/Ref) を返す。
    pub fn value_type_at(&self, idx: usize) -> Option<ValueType> {
        self.value_types.get(idx).copied()
    }

    /// 0.8.7: 指定 table の fk_refs を `(from_col_name, to_table_name)` ペアで返す
    /// (= schema synthesize fallback で relation を復元する用)。 himo id → 名前、
    /// table id → 名前を解決する。 未定義 table or fk_refs 空なら Vec::new()。
    pub fn fk_refs_for_table_named(&self, table_name: &str) -> Vec<(String, String)> {
        let Some(table) = self.tables.iter().find(|t| t.name == table_name) else {
            return Vec::new();
        };
        let prefix = format!("{}.", table_name);
        let mut out = Vec::with_capacity(table.fk_refs.len());
        for &(hid, tid) in &table.fk_refs {
            let Some(himo_full) = self.himo_names.get(hid as usize) else { continue; };
            let from_col = himo_full.strip_prefix(&prefix).unwrap_or(himo_full).to_string();
            let Some(target) = self.tables.get(tid as usize) else { continue; };
            out.push((from_col, target.name.clone()));
        }
        out
    }

    /// 0.7.0: 指定 table 名が reserved table として既に存在するか。 schema crate
    /// が `_schema_meta` を auto-define する idempotent path で使う。
    pub fn has_reserved_table(&self, name: &str) -> bool {
        is_reserved_table_name(name)
            && self.tables.iter().any(|t| t.name == name)
    }

    // ──── entity ────

    pub fn entity(&self) -> enchudb_oplog::EntityId {
        use std::sync::atomic::Ordering;
        self.check_writable();
        // β-light step 3: anonymous table が closed (= 既に define_table が
        // 呼ばれた) なら entity() は panic。 entity_in を使うこと。
        let anon_hi = self.tables[ANONYMOUS_TABLE as usize].eid_range_hi;
        if anon_hi != u32::MAX {
            panic!(
                "anonymous table is closed (define_table was called); \
                 use entity_in('<table>') instead of entity()"
            );
        }
        let local = self.entities.allocate();
        // concurrent mode (= consumer thread 稼働) なら barrier 用に空 op を
        // 流す。 issue5: push_count と apply_count を対称に保たないと
        // `flush_writes` が ties drain 前に early return して live query が
        // pending Tie を見落とす。 undo 廃止 (v4) 後は payload なしで
        // counter increment のみが起こる。
        if let Some(q) = self.write_queue.as_ref() {
            q.push(crate::write_queue::Op::EntityCreated { local });
            self.push_count.fetch_add(1, Ordering::Release);
        }
        let peer = self.peer_id.load(Ordering::Acquire);
        enchudb_oplog::make_eid(peer, local)
    }

    pub fn entities(&self) -> Vec<enchudb_oplog::EntityId> {
        let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
        self.entities.iter().into_iter()
            .map(|local| enchudb_oplog::make_eid(peer, local))
            .collect()
    }
    pub fn entity_count(&self) -> u32 { self.entities.count() }

    /// eid が live (= 割当済みで未削除) か。 0.9.0 (M10): query_lang の
    /// update/delete が未割当 slot へ phantom write するのを防ぐ existence check 用。
    /// 範囲外 eid は false。
    pub fn is_live(&self, eid: enchudb_oplog::EntityId) -> bool {
        self.entities.is_live(enchudb_oplog::eid_local(eid))
    }
    pub fn next_eid(&self) -> enchudb_oplog::EntityId {
        let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
        enchudb_oplog::make_eid(peer, self.entities.next_eid())
    }
    /// 0.7.0: 残り eid 空間 (= max_entities - 既存 table の eid_range_hi の max)。
    /// schema crate が `define_table` を呼ぶ前に「table 1 個分にどれだけ割けるか」
    /// 判断する用。
    pub fn remaining_eid_space(&self) -> u32 {
        let used = self.tables.iter().map(|t| {
            if t.eid_range_hi == u32::MAX {
                self.entities.next_eid()  // anonymous open-ended
            } else {
                t.eid_range_hi
            }
        }).max().unwrap_or(0);
        self.max_entities.saturating_sub(used)
    }
    /// 0.7.0: 最大 entity 数 (= layout 確保時の上限)。 schema crate が
    /// size_hint を自動算出する用。
    pub fn max_entities(&self) -> u32 { self.max_entities }

    // ──── v32: peer_id ────

    /// この Engine を所有する peer の id を設定。WAL / DB header にも反映される。
    /// 起動時に 1 回だけ呼ぶ想定。
    pub fn set_peer_id(&self, peer: enchudb_oplog::PeerId) {
        self.peer_id.store(peer, std::sync::atomic::Ordering::Release);
        // mmap の header に即書き込み(CRC 保護外なので再計算不要)
        self.backing.header_mut()[H_PEER_ID..H_PEER_ID + 4]
            .copy_from_slice(&peer.to_le_bytes());
        if let Some(wal) = self.oplog.as_ref() {
            wal.set_peer_id(peer);
        }
    }

    /// 現在の peer id。
    pub fn peer_id(&self) -> enchudb_oplog::PeerId {
        self.peer_id.load(std::sync::atomic::Ordering::Acquire)
    }

    /// CRDT mesh mode の有効化。 true にすると `remote_*_apply` で受信した op を
    /// `append_relayed` で自分の WAL にも記録し、 次の publish で他 peer に gossip する。
    /// ホスト/クライアント構成では false のまま (= ホストの WAL に届いた時点で完結)。
    pub fn set_gossip_remote_apply(&self, on: bool) {
        self.gossip_remote_apply
            .store(on, std::sync::atomic::Ordering::Release);
    }

    pub fn gossip_remote_apply(&self) -> bool {
        self.gossip_remote_apply
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// LWW 用 HlcStore への参照(sync モジュールが使う)。
    pub fn hlc_store(&self) -> &std::sync::Arc<crate::hlc_store::HlcStore> {
        &self.hlc_store
    }

    /// #9: foreign eid 翻訳テーブルへの参照。
    pub fn eid_translator(&self) -> &std::sync::Arc<crate::eid_translator::EidTranslator> {
        &self.eid_translator
    }

    /// #9: 受信 op の foreign eid を「自分の eid 空間の local eid」に翻訳する。
    /// 初見の `(owner, foreign_local)` には `himo_id` が属する closed table 内に
    /// fresh な local entity を払い出す (= local entity と同じ allocator なので衝突しない)。
    /// himo を closed table に解決できない table-less op (anonymous himo / sentinel) は
    /// 安全に確保できる slot が無いので `None` を返す → caller は skip する。
    ///
    /// 0.11 (request10 phase 2): 翻訳キーは **eid に埋まった産みの親**
    /// (`eid_peer(foreign_eid)`) であって record の書き手ではない。 single-writer 下
    /// では両者は常に一致するが、 write-back 解禁後は 「B が書いた A の entity の
    /// record」 で乖離する — 書き手でキーすると受信側で別 entity に断片化する。
    /// `eid_peer(foreign_eid) == self.peer_id` なら identity (自分が産んだ entity)。
    pub fn resolve_remote_eid(
        &self,
        foreign_eid: enchudb_oplog::EntityId,
        himo_id: u16,
    ) -> Option<enchudb_oplog::EntityId> {
        use std::sync::atomic::Ordering;
        let self_peer = self.peer_id.load(Ordering::Acquire);
        let owner = enchudb_oplog::eid_peer(foreign_eid);
        if owner == self_peer {
            return Some(foreign_eid); // identity: 自分が産んだ entity
        }
        let foreign_local = enchudb_oplog::eid_local(foreign_eid);
        // get-or-allocate を atomic に行う (= 並行 apply で同じ foreign entity を
        // double-alloc しない)。 alloc が None (= table-less) なら写像を作らず None。
        let local = self
            .eid_translator
            .get_or_insert_with(owner, foreign_local, || {
                self.alloc_translated_local(himo_id)
            })?;
        Some(enchudb_oplog::make_eid(self_peer, local))
    }

    /// #9: 既存の翻訳のみ引く (払い出しはしない)。 Delete のように himo を持たず
    /// table を導けない op 用。 未登録 (= 一度も Tie されてない foreign entity) なら
    /// None → 呼び出し側で skip。 キーは `resolve_remote_eid` と同じく eid の
    /// 産みの親 (`eid_peer`)、 産みの親 == self なら identity。
    pub fn resolve_remote_eid_existing(
        &self,
        foreign_eid: enchudb_oplog::EntityId,
    ) -> Option<enchudb_oplog::EntityId> {
        use std::sync::atomic::Ordering;
        let self_peer = self.peer_id.load(Ordering::Acquire);
        let owner = enchudb_oplog::eid_peer(foreign_eid);
        if owner == self_peer {
            return Some(foreign_eid);
        }
        let foreign_local = enchudb_oplog::eid_local(foreign_eid);
        self.eid_translator
            .get(owner, foreign_local)
            .map(|local| enchudb_oplog::make_eid(self_peer, local))
    }

    /// #9: foreign entity 用に fresh な local eid を払い出す。 `himo_id` が closed table
    /// 所属なら その table の `entity_in` 経路で確保 (= local entity と同じ allocator を
    /// 使うので衝突しない + `validate_eid_for_himo` の range 内に入る)、 local 部を `Some`
    /// で返す。 table を導けない (anonymous himo / sentinel) / entity_in 失敗 (table 枯渇)
    /// なら **安全に確保できる slot が無い** ので `None` (caller が op を skip)。
    /// `entity()` での anonymous fallback は使わない: sync engine では anonymous table が
    /// 閉じていて panic するし、 `entities.allocate()` は table range と衝突しうる。
    fn alloc_translated_local(&self, himo_id: u16) -> Option<u32> {
        let hid = himo_id as usize;
        if let Some(tid) = self.himo_table_get(hid) {
            let tid = tid as usize;
            if tid < self.tables.len() && self.tables[tid].eid_range_hi != u32::MAX {
                let name = self.tables[tid].name.clone();
                if let Ok(e) = self.entity_in(&name) {
                    return Some(enchudb_oplog::eid_local(e));
                }
            }
        }
        None
    }

    /// #9: himo が Ref 型か。 Ref の value は foreign target eid なので apply 時に
    /// entity eid とは別に value も translate する必要がある。
    pub fn himo_is_ref(&self, himo_id: u16) -> bool {
        let hid = himo_id as usize;
        hid < self.value_types.len() && self.value_types[hid] == ValueType::Ref
    }

    /// #9: Ref himo の value (= foreign target eid の local 部) を自分の eid 空間の
    /// local eid に翻訳する。 target entity が初見なら ref の **target table** に fresh
    /// な local を払い出す。 後で target entity 自身の Tie が来ても同じ key
    /// `(author_peer, foreign_value)` で同じ local に解決されるため整合する (= forward
    /// ref も OK)。 `author_peer == self` なら identity。
    pub fn resolve_remote_ref_value(
        &self,
        author_peer: enchudb_oplog::PeerId,
        foreign_value: u32,
        ref_himo_id: u16,
    ) -> Option<u32> {
        use std::sync::atomic::Ordering;
        let self_peer = self.peer_id.load(Ordering::Acquire);
        if author_peer == self_peer {
            return Some(foreign_value); // identity: 自分が author
        }
        // entity eid 翻訳と同じ key 空間・同じ atomic path。 ref-value 経由と target
        // entity 自身の Tie 経由が同じ foreign を解決しても 1 つの local に収束する。
        // target table を導けなければ None (caller が op を skip)。
        self.eid_translator
            .get_or_insert_with(author_peer, foreign_value, || {
                self.alloc_translated_local_in_target_table(ref_himo_id)
            })
    }

    /// #9: Ref himo の target table (fk_refs で引く) に fresh な local eid を払い出す。
    /// target table を導けない / entity_in 失敗なら `None` (caller が op を skip)。
    fn alloc_translated_local_in_target_table(&self, ref_himo_id: u16) -> Option<u32> {
        let hid = ref_himo_id as usize;
        if let Some(owner_tid) = self.himo_table_get(hid) {
            let owner_tid = owner_tid as usize;
            if owner_tid < self.tables.len() {
                let owner = &self.tables[owner_tid];
                if let Some(&(_, target_tid)) =
                    owner.fk_refs.iter().find(|(h, _)| *h == hid as u32)
                {
                    let target_tid = target_tid as usize;
                    if target_tid < self.tables.len()
                        && self.tables[target_tid].eid_range_hi != u32::MAX
                    {
                        let name = self.tables[target_tid].name.clone();
                        if let Ok(e) = self.entity_in(&name) {
                            return Some(enchudb_oplog::eid_local(e));
                        }
                    }
                }
            }
        }
        None
    }

    /// Phase C: 自 peer の鍵ペアを設定。WAL にも反映される。None で署名 off。
    pub fn set_keypair(&self, kp: Option<std::sync::Arc<enchudb_oplog::keys::Keypair>>) {
        *self.keypair.write().unwrap() = kp.clone();
        if let Some(wal) = self.oplog.as_ref() {
            wal.set_keypair(kp.clone());
        }
        if let Some(ref k) = kp {
            let peer = self.peer_id();
            self.pubkeys.force_register(peer, &k.public_bytes());
        }
    }

    /// Phase C: 他 peer の pubkey ストアへの参照。
    pub fn pubkeys(&self) -> &std::sync::Arc<enchudb_oplog::keys::PubkeyStore> {
        &self.pubkeys
    }

    /// Phase C: ACL への参照。Syncer が受信 op の author を enforce する。
    pub fn acl(&self) -> &std::sync::Arc<crate::acl::Acl> {
        &self.acl
    }

    /// Phase C: 自 peer の鍵ペアを返す。
    pub fn keypair(&self) -> Option<std::sync::Arc<enchudb_oplog::keys::Keypair>> {
        self.keypair.read().unwrap().clone()
    }

    /// WAL への参照(sync モジュールが publish に使う)。
    pub fn oplog_arc(&self) -> Option<std::sync::Arc<enchudb_oplog::oplog::OpLog>> {
        self.oplog.clone()
    }

    // ──── v32: リモート peer から pull したレコードを apply する ────
    // これらは LWW 判定を通った後の無条件 apply。HlcStore は Syncer が先に更新済み。

    /// リモート peer から届いた Tie を apply。
    /// `gossip_remote_apply` 有効かつ `relayed` が `Some` なら、 同じ op を
    /// `append_relayed` で自分の WAL にも記録 (HLC/author/署名は元のまま)。
    /// `relayed` は WAL 受信時の元 header (sync 側で WireRecord から作って渡す)。
    /// v6 (#88): himo が LeafStore routing 対象なら `&LeafStore` を返す。
    /// 対象 = leaf region あり (v6) && Leaf 型 && reserved table 配下でない
    /// (`_sync_ops.payload` 等の内部 Leaf は従来通り vocab)。
    fn leaf_for(&self, hid: usize) -> Option<&LeafStore> {
        if hid < self.value_types.len()
            && self.value_types[hid] == ValueType::Leaf
            && !self.himo_is_in_reserved_table(hid)
        {
            self.leaf.as_ref()
        } else {
            None
        }
    }

    /// v6 (#88): text 型 himo の cell 生値 (raw) を payload に解決。 routed-Leaf は
    /// LeafStore offset として、 それ以外 (Tag / reserved Leaf) は vocab vid として読む。
    #[inline]
    fn text_value(&self, hid: usize, raw: u32) -> &[u8] {
        match self.leaf_for(hid) {
            Some(leaf) => leaf.get(raw),
            None => self.vocab.get(raw),
        }
    }

    /// v6 (#88): routed-Leaf の cell が offset を持っていれば LeafStore に free。
    /// delete / untie / apply_op の remove 直前に呼ぶ (leak 防止)。 非 routed は no-op。
    #[inline]
    fn free_leaf_cell(&self, eid: u32, hid: usize) {
        if let Some(leaf) = self.leaf_for(hid)
            && let Some(off) = self.himos[hid].get_value(eid)
        {
            leaf.free(off);
        }
    }

    /// v6 (#88): open 時に routed-Leaf の live cell offset を集めて LeafStore の
    /// free-list を再構成する (free-list は非永続 = store の派生)。 writable open の
    /// load 末尾で呼ぶ。 これが無いと過去 session で空いた slot が再利用されない。
    fn rebuild_leaf_free_list(&self) {
        let Some(leaf) = self.leaf.as_ref() else { return; };
        let mut live: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for hid in 0..self.himos.len() {
            if self.leaf_for(hid).is_some() {
                for off in self.himos[hid].unique_values() {
                    live.insert(off);
                }
            }
        }
        leaf.rebuild_free_list(&live);
    }

    /// v6 (#88): リモート peer から届いた TieLeaf を apply。 bytes を local
    /// LeafStore に insert して cell に offset を張る (vid mapping 不要)。
    /// `remote_tie_apply` の Leaf 版。
    pub fn remote_tieleaf_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        bytes: &[u8],
        relayed: Option<enchudb_oplog::oplog::RelayedHeader>,
    ) {
        let local = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return; }
        self.entities.ensure_live(local);
        // v6 (#88): remote re-tie 上書きで旧 offset を回収。
        self.free_leaf_cell(local, hid);
        let value = match self.leaf_for(hid) {
            Some(leaf) => leaf.insert(bytes),
            None => self.vocab.insert(bytes), // pre-v6 / reserved: 旧 vocab fallback
        };
        self.himos[hid].set(local, value);
        Self::advance_table_next_local_for(&self.tables, local);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.oplog.as_ref(), relayed) {
                let _ = wal.append_relayed(
                    enchudb_oplog::oplog::Op::TieLeaf {
                        eid,
                        himo_name: &self.himo_names[hid],
                        himo_kind: self.value_types[hid] as u8,
                        bytes,
                    },
                    h,
                );
            }
        }
    }

    pub fn remote_tie_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        value: u32,
        relayed: Option<enchudb_oplog::oplog::RelayedHeader>,
    ) {
        let local = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return; }
        // entity 未確保ならローカル側の EntitySet に登録(eid は peer 側が決めた値)
        self.entities.ensure_live(local);
        self.himos[hid].set(local, value);
        // issue #47 fix: foreign local が我々の table eid_range 内に落ちた場合、
        // 次の `entity_in` が同 local を払出して live entity を上書きしないよう
        // `next_local` を `local + 1` まで前進させる。 これは `apply_oplog_op`
        // (WAL recover 経路、 engine.rs:3790) と対称の処理。
        Self::advance_table_next_local_for(&self.tables, local);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.oplog.as_ref(), relayed) {
                let _ = wal.append_relayed(
                    enchudb_oplog::oplog::Op::Tie { eid, himo_id, value },
                    h,
                );
            }
        }
    }

    /// リモート peer から届いた Untie を apply。
    pub fn remote_untie_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        relayed: Option<enchudb_oplog::oplog::RelayedHeader>,
    ) {
        let local = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return; }
        self.free_leaf_cell(local, hid);
        self.himos[hid].remove(local);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.oplog.as_ref(), relayed) {
                let _ = wal.append_relayed(
                    enchudb_oplog::oplog::Op::Untie { eid, himo_id },
                    h,
                );
            }
        }
    }

    /// リモート peer から届いた Delete を apply。
    pub fn remote_delete_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        relayed: Option<enchudb_oplog::oplog::RelayedHeader>,
    ) {
        let local = enchudb_oplog::eid_local(eid);
        for hid in 0..self.himos.len() {
            self.free_leaf_cell(local, hid);
            self.himos[hid].remove(local);
        }
        self.entities.free(local);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.oplog.as_ref(), relayed) {
                let _ = wal.append_relayed(enchudb_oplog::oplog::Op::Delete { eid }, h);
            }
        }
    }

    /// リモート peer から届いた Content 書き込みを apply。
    pub fn remote_content_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        key: &str,
        data: &[u8],
        relayed: Option<enchudb_oplog::oplog::RelayedHeader>,
    ) {
        let local = enchudb_oplog::eid_local(eid);
        self.entities.ensure_live(local);
        self.contents.set(local, key, data);
        // issue #47 fix: Tie 経路と同じ理由で next_local を前進させる。
        Self::advance_table_next_local_for(&self.tables, local);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.oplog.as_ref(), relayed) {
                let _ = wal.append_relayed(
                    enchudb_oplog::oplog::Op::Content { eid, key, data },
                    h,
                );
            }
        }
    }

    /// v33: リモート peer から届いた Vocab op を apply。
    /// `bytes` を local vocab に insert し、local_vid を取得して
    /// `(author_peer, remote_vid) → local_vid` の mapping を記録する。
    /// 後続の Tie { value: remote_vid } を受信したら `translate_remote_vid` で local_vid に変換。
    /// gossip 有効かつ自 vocab に新規追加された場合のみ Vocab op を `append_relayed` で WAL に
    /// 流す (HLC/author/署名は元のまま)。 既存 vocab に当たれば relay しない (重複防止)。
    pub fn remote_vocab_apply(
        &self,
        author_peer: enchudb_oplog::PeerId,
        remote_vid: u32,
        bytes: &[u8],
        relayed: Option<enchudb_oplog::oplog::RelayedHeader>,
    ) {
        let was_new = self.vocab.lookup(bytes).is_none();
        let local_vid = self.vocab.get_or_insert(bytes);
        let mut map = self.peer_vocab_map.write().unwrap();
        map.insert((author_peer, remote_vid), local_vid);
        if was_new && self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.oplog.as_ref(), relayed) {
                // 自 vocab の vid を貼り直し (Tie 受信側が translate_remote_vid で再翻訳)。
                let _ = wal.append_relayed(
                    enchudb_oplog::oplog::Op::Vocab { vid: local_vid, bytes },
                    h,
                );
            }
        }
    }

    /// 0.8.4 issue #30: 受信 Vocab record の dedupe 用。 `(author_peer, remote_vid)`
    /// が既に `peer_vocab_map` に登録済みで、 かつ map 先 local_vid の bytes が
    /// 受信 bytes と一致するなら true (= 同じ record の再受信、 skip すべき)。
    ///
    /// 旧 behavior: sync apply_one は Vocab を **HLC dedupe せず常に applied 扱い**
    /// していた。 gossip_remote_apply ON 構成で同じ vocab record が無限に往復し、
    /// `entities_live` が膨れる amplification loop が出た (bisquit dogfood 実例)。
    /// 本 method を sync 側で先に叩いて、 既登録なら apply_one が false を返す。
    pub fn has_remote_vocab(
        &self,
        author_peer: enchudb_oplog::PeerId,
        remote_vid: u32,
        bytes: &[u8],
    ) -> bool {
        let map = self.peer_vocab_map.read().unwrap();
        if let Some(&local_vid) = map.get(&(author_peer, remote_vid)) {
            return self.vocab.get(local_vid) == bytes;
        }
        false
    }

    /// v33: Symbol 型 himo の Tie を受信した際、remote vid を local vid に変換する。
    /// Symbol 以外の himo、または mapping 未登録なら元値をそのまま返す。
    pub fn translate_remote_vid(&self, author_peer: enchudb_oplog::PeerId, himo_id: u16, value: u32) -> u32 {
        let hid = himo_id as usize;
        if hid >= self.value_types.len() { return value; }
        // Tag / Leaf どちらも vocab 経由なので vid 翻訳が必要。
        match self.value_types[hid] {
            ValueType::Tag | ValueType::Leaf => {
                let map = self.peer_vocab_map.read().unwrap();
                *map.get(&(author_peer, value)).unwrap_or(&value)
            }
            _ => value,
        }
    }

    // ──── tie ────

    pub fn define_himo(&mut self, himo: &str, ht: ValueType, max_values: u32) {
        self.ensure_himo(himo, ht, max_values);
    }

    /// β-light step 4: 指定 table 配下の column として himo を定義する。
    /// 内部 storage 上は `"table.himo"` (例: `"users.age"`) という完全修飾名で
    /// 1 エントリ。 tie 経路は既存のまま (`himo_id` の vocab 検索で full name
    /// を引く)、 namespacing は単純な命名規約として扱う。
    ///
    /// 旧 API `define_himo` は引き続き bare 名で anonymous table に attach
    /// される。 同名 collision は engine では check しない (step 5+ で
    /// 必要なら追加)。
    pub fn define_himo_in(
        &mut self,
        table_name: &str,
        himo_name: &str,
        ht: ValueType,
        max_values: u32,
    ) -> Result<u32, String> {
        // 0.9.0: 本体は `&self` 版 (ensure_himo_dynamic_in) に移譲。 signature は
        // 互換維持のため `&mut self` / u32 のまま。
        self.ensure_himo_dynamic_in(table_name, himo_name, ht, max_values)
            .map(|hid| hid as u32)
    }

    /// 0.9.0: `define_himo_in` の `&self` 版。 `Arc<Engine>` (= concurrent mode)
    /// から lazy に himo を定義できる。 idempotent: 既に同名 himo が target
    /// table に attach 済みなら既存 hid を返す。 定義 (+ attach 移動) は
    /// `himo_def_lock` で直列化される。
    pub fn ensure_himo_dynamic_in(
        &self,
        table_name: &str,
        himo_name: &str,
        ht: ValueType,
        max_values: u32,
    ) -> Result<u16, String> {
        self.check_writable();
        if table_name.is_empty() {
            return Err("table name must be non-empty (use define_himo for anonymous)".into());
        }
        if himo_name.is_empty() {
            return Err("himo name must be non-empty".into());
        }
        if himo_name.contains('.') {
            return Err(format!(
                "himo name '{}' must not contain '.' (it is reserved as table separator)",
                himo_name,
            ));
        }
        let tid = self
            .tables
            .iter()
            .position(|t| t.name == table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;

        let full_name = format!("{}.{}", table_name, himo_name);

        // 定義 + attach 移動を 1 定義単位として himo_def_lock で直列化する
        // (並行する同名 ensure が同じ hid に収束するように)。
        let _guard = self.himo_def_lock.lock().unwrap();

        // 既存ならそのまま、 新規なら anonymous へ attach で定義。
        // 後者なら anonymous の himo_ids から外して target table へ移す。
        let hid = match self.himo_id(&full_name) {
            Some(idx) => idx,
            None => self.define_himo_slot_locked(&full_name, ht, max_values)? as usize,
        };
        let hid_u32 = hid as u32;

        // 既に target table に attach 済みなら何もしない (重複 define_himo_in)
        let cur_tid = self.himo_to_table[hid].load(std::sync::atomic::Ordering::Relaxed);
        if cur_tid == tid as TableId {
            return Ok(hid as u16);
        }

        // 新規時は ANONYMOUS_TABLE へ attach されているので、 別 table 既属の
        // 場合は migrate する形 (実用上は新規時のみ通る)。
        self.tables[cur_tid as usize]
            .himo_ids
            .write()
            .unwrap()
            .retain(|&h| h != hid_u32);
        self.tables[tid].himo_ids.write().unwrap().push(hid_u32);
        self.himo_to_table[hid].store(tid as TableId, std::sync::atomic::Ordering::Relaxed);

        self.try_persist_tables();
        Ok(hid as u16)
    }

    /// β-light step 5: `Ref` 型 himo を target_table と紐付けて定義する。
    /// 以降の `tie` / `tie_ref` / `tie_async` で target_eid が target_table の
    /// eid 範囲に収まっているか engine が validate する。
    pub fn define_ref_in(
        &mut self,
        table_name: &str,
        himo_name: &str,
        target_table: &str,
    ) -> Result<u32, String> {
        self.check_writable();
        // target_table 存在チェック
        let target_tid = self
            .tables
            .iter()
            .position(|t| t.name == target_table)
            .ok_or_else(|| format!("target table '{}' not found", target_table))?
            as TableId;

        // Ref として himo 登録 (define_himo_in が table_name / himo_name の
        // 各種チェックを行う)
        let hid = self.define_himo_in(table_name, himo_name, ValueType::Ref, 0)?;

        // 所属 table の fk_refs に entry を追加 (idempotent)
        let owner_tid =
            self.himo_to_table[hid as usize].load(std::sync::atomic::Ordering::Relaxed);
        let entry = (hid, target_tid);
        let owner = &mut self.tables[owner_tid as usize];
        if !owner.fk_refs.iter().any(|e| *e == entry) {
            owner.fk_refs.push(entry);
        }
        self.try_persist_tables();
        Ok(hid)
    }

    /// β-light step 6: tie 対象 eid が himo の所属 table eid_range に収まるか
    /// validate。 これが β-light の win の本体: 「table-local positions」 を
    /// BucketCylinder の eid_offset 機構で自然に実現する。
    ///
    /// 動作:
    ///   - anonymous (id=0) かつ open-ended (eid_range_hi == u32::MAX) は
    ///     validation スキップ (= 旧 API 完全互換)
    ///   - それ以外 (= define_table 後 / 非 anonymous table) は eid が
    ///     [eid_range_lo, eid_range_hi) に収まらないと panic
    ///
    /// hot path: define_table が未呼び出しなら `tables.len() == 1` で 1 load で
    /// 抜ける。 これにより legacy 経路の tie_async hot path は ~1 ns コストに収まる。
    /// 一度でも user table を定義したら full validation 経路に入る。
    #[inline(always)]
    fn validate_eid_for_himo(&self, hid: usize, eid_local: u32) {
        // fast path: anonymous のみ存在 (= define_table 未) → 全 eid 受け入れ
        if self.tables.len() <= 1 {
            return;
        }
        let Some(tid) = self.himo_table_get(hid) else {
            return;
        };
        let tid = tid as usize;
        if tid >= self.tables.len() {
            return;
        }
        let table = &self.tables[tid];
        // anonymous は close されてれば eid_range_hi != u32::MAX、 closed 後も
        // tie が来たら validate される
        if table.eid_range_hi == u32::MAX {
            return;
        }
        assert!(
            eid_local >= table.eid_range_lo && eid_local < table.eid_range_hi,
            "tie eid {} not in himo's table '{}' eid_range [{}, {})",
            eid_local, table.name, table.eid_range_lo, table.eid_range_hi,
        );
    }

    /// β-light step 5: Ref tie の FK validation。 himo が Ref 型で fk_refs
    /// entry を持つ場合、 target_eid が target_table の eid 範囲内かを assert。
    ///
    /// hot path 性能:
    ///   - 非 Ref himo は最初の `value_types[hid] != Ref` で即 return (~1 ns)
    ///   - Ref himo は fk_refs (typically 1-5 件) の線形検索 (~5-10 ns)
    #[inline(always)]
    fn validate_ref_tie(&self, hid: usize, target_eid: u32) {
        if hid >= self.value_types.len() {
            return;
        }
        if self.value_types[hid] != ValueType::Ref {
            return;
        }
        let Some(owner_tid) = self.himo_table_get(hid) else {
            return;
        };
        let owner_tid = owner_tid as usize;
        if owner_tid >= self.tables.len() {
            return;
        }
        let owner = &self.tables[owner_tid];
        let target_tid = match owner.fk_refs.iter().find(|(h, _)| *h == hid as u32) {
            Some(&(_, t)) => t as usize,
            // fk_refs に entry なし: Ref 型だが target 不明 (旧 API 経路)、 validation スキップ
            None => return,
        };
        if target_tid >= self.tables.len() {
            return;
        }
        let target = &self.tables[target_tid];
        assert!(
            target_eid >= target.eid_range_lo && target_eid < target.eid_range_hi,
            "FK violation: Ref himo (id {}) points to eid {} outside target table '{}' range [{}, {})",
            hid, target_eid, target.name, target.eid_range_lo, target.eid_range_hi,
        );
    }

    pub fn tie_text(&mut self, eid: enchudb_oplog::EntityId, himo: &str, value: &str) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let hid = self.ensure_himo(himo, ValueType::Tag, 0);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // v6 (#88): Leaf は LeafStore へ。 &mut self (build phase) は WAL emit しない
        // ので leaf.insert + cell set のみ。 re-tie 上書きは旧 offset を free。
        if let Some(leaf) = self.leaf_for(hid) {
            if let Some(old) = self.himos[hid].get_value(eid) { leaf.free(old); }
            let off = leaf.insert(value.as_bytes());
            self.himos[hid].set(eid, off);
            return;
        }
        // Tag は dedupe (get_or_insert)、Leaf は新規 id 発行 (insert)。
        let vid = match self.value_types[hid] {
            ValueType::Tag => self.vocab.get_or_insert(value.as_bytes()),
            ValueType::Leaf => self.vocab.insert(value.as_bytes()),
            ht => panic!("tie_text on non-text himo '{}': {:?}", himo, ht),
        };
        self.himos[hid].set(eid, vid);
    }

    pub fn tie(&mut self, eid: enchudb_oplog::EntityId, himo: &str, value: u32) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, ValueType::Number, 0);
        debug_assert!(self.value_types[hid] == ValueType::Number || self.value_types[hid] == ValueType::Ref, "tie on non-Value himo '{}'", himo);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // β-light step 5: Ref himo は target_table の eid range を validate
        self.validate_ref_tie(hid, value);
        self.himos[hid].set(eid, value);
    }

    pub fn tie_ref(&mut self, eid: enchudb_oplog::EntityId, himo: &str, target_eid: enchudb_oplog::EntityId) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let target_eid = enchudb_oplog::eid_local(target_eid);
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, ValueType::Ref, 0);
        debug_assert!(self.value_types[hid] == ValueType::Ref || self.value_types[hid] == ValueType::Number, "tie_ref on non-Ref himo '{}'", himo);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // β-light step 5: target_eid が target_table の eid range 内か
        self.validate_ref_tie(hid, target_eid);
        self.himos[hid].set(eid, target_eid);
    }

    // ──── tie（定義済み紐、&self で並行書き込み可）────

    /// 定義済みの紐に文字列を張る。&selfで呼べる（Arc共有のまま書き込み可）。
    /// 紐が未定義ならpanic。define_himo を先に呼ぶこと。
    pub fn tie_text_to(&self, eid: enchudb_oplog::EntityId, himo: &str, value: &str) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_text_to_by_id(eid, hid, value);
    }

    /// `tie_text_to` の himo_id 直指定版。 hot path で per-call の HashMap lookup を
    /// 避けたい時に。 起動時に `himo_id(&str)` で解決して u16 を cache しておく。
    pub fn tie_text_to_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: &str) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // v6 (#88): Leaf は LeafStore へ (vocab の vid は使わない)。 re-tie 上書きは
        // 旧 offset を free してから insert (leak 防止)。 sync は bytes 同乗 TieLeaf。
        if let Some(leaf) = self.leaf_for(hid) {
            let bytes = value.as_bytes();
            if let Some(old) = self.himos[hid].get_value(eid) { leaf.free(old); }
            let off = leaf.insert(bytes);
            self.himos[hid].set(eid, off);
            if let Some(wal) = self.oplog.as_ref() {
                let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), eid);
                let _ = wal.append(enchudb_oplog::oplog::Op::TieLeaf {
                    eid: oplog_eid,
                    himo_name: &self.himo_names[hid],
                    himo_kind: self.value_types[hid] as u8,
                    bytes,
                });
            }
            return;
        }
        // Tag は dedupe、Leaf は常に新規 id。
        let vid = match self.value_types[hid] {
            ValueType::Tag => self.vocab.get_or_insert(value.as_bytes()),
            ValueType::Leaf => self.vocab.insert(value.as_bytes()),
            ht => panic!("tie_text_to_by_id on non-text himo_id {}: {:?}", himo_id, ht),
        };
        self.himos[hid].set(eid, vid);
        // WAL に Vocab + Tie を流す。 schema layer (enchudb-schema) は同期版の
        // tie_text_to を経由するため、 ここで append しないと WAL が空のままで
        // peer 同期が成立しない (publish 側が iter_committed で 0 件を見る).
        // 0.7.0: reserved table (`_sync_ops` 等) への write は oplog skip。
        if !self.himo_is_in_reserved_table(hid) {
            if let Some(wal) = self.oplog.as_ref() {
                let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), eid);
                let _ = wal.append(enchudb_oplog::oplog::Op::Vocab { vid, bytes: value.as_bytes() });
                let _ = wal.append(enchudb_oplog::oplog::Op::Tie { eid: oplog_eid, himo_id, value: vid });
            }
        }
    }

    /// 0.7.0: 当該 himo が reserved table 配下か (= `_*` 表)。
    /// reserved table への tie は oplog に再 append しない (= 2 重書き防止)。
    /// hot path で `tie_*_to_by_id` から呼ばれる、 inline で軽量化。
    #[inline]
    fn himo_is_in_reserved_table(&self, himo_id: usize) -> bool {
        match self.himo_table_get(himo_id) {
            Some(tid) => self.tables.get(tid as usize)
                .map(|td| td.is_reserved())
                .unwrap_or(false),
            None => false,
        }
    }

    /// 0.7.0: `tie_text_to_by_id` の binary 版。 任意 byte slice を vocab に
    /// insert (Leaf なら dedupe なし) し、 himo に紐付ける。 `_sync_ops.payload`
    /// (= oplog record の生 wire bytes) 等、 UTF-8 制約を持たない data を書く用。
    pub fn tie_bytes_to_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: &[u8]) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // v6 (#88): Leaf は LeafStore へ。 re-tie 上書きは旧 offset を free。
        if let Some(leaf) = self.leaf_for(hid) {
            if let Some(old) = self.himos[hid].get_value(eid) { leaf.free(old); }
            let off = leaf.insert(value);
            self.himos[hid].set(eid, off);
            if let Some(wal) = self.oplog.as_ref() {
                let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), eid);
                let _ = wal.append(enchudb_oplog::oplog::Op::TieLeaf {
                    eid: oplog_eid,
                    himo_name: &self.himo_names[hid],
                    himo_kind: self.value_types[hid] as u8,
                    bytes: value,
                });
            }
            return;
        }
        let vid = match self.value_types[hid] {
            ValueType::Tag => self.vocab.get_or_insert(value),
            ValueType::Leaf => self.vocab.insert(value),
            ht => panic!("tie_bytes_to_by_id on non-text himo_id {}: {:?}", himo_id, ht),
        };
        self.himos[hid].set(eid, vid);
        // reserved table への write は oplog 再 append を skip (= 2 重書き防止)。
        if !self.himo_is_in_reserved_table(hid) {
            if let Some(wal) = self.oplog.as_ref() {
                let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), eid);
                let _ = wal.append(enchudb_oplog::oplog::Op::Vocab { vid, bytes: value });
                if self.himo_is_content(hid) {
                    // 0.9.0: 動的 content himo は id が peer 間で揃わないため名前で運ぶ
                    let _ = wal.append(enchudb_oplog::oplog::Op::TieNamed {
                        eid: oplog_eid,
                        himo_name: &self.himo_names[hid],
                        himo_kind: self.value_types[hid] as u8,
                        value: vid,
                    });
                } else {
                    let _ = wal.append(enchudb_oplog::oplog::Op::Tie { eid: oplog_eid, himo_id, value: vid });
                }
            }
        }
    }

    /// 定義済みの紐にu32値を張る。&selfで呼べる。
    pub fn tie_to(&self, eid: enchudb_oplog::EntityId, himo: &str, value: u32) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_to_by_id(eid, hid, value);
    }

    /// `tie_to` の himo_id 直指定版。 hot path 用 (string lookup を避ける)。
    pub fn tie_to_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: u32) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // Tag / Leaf 型 (vocab_id を value として持つ) も許可。 schema 層が
        // 起動時に解決済みの table_vid を marker himo に張る hot path 用途で
        // 必要 (request2.md 提案)。 caller 責任で vocab に既に居る id を渡すこと。
        self.himos[hid].set(eid, value);
        // 0.7.0: reserved table への write は oplog skip。
        if !self.himo_is_in_reserved_table(hid) {
            if let Some(wal) = self.oplog.as_ref() {
                let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), eid);
                let _ = wal.append(enchudb_oplog::oplog::Op::Tie { eid: oplog_eid, himo_id, value });
            }
        }
    }

    /// 定義済みの紐にentity参照を張る。&selfで呼べる。
    pub fn tie_ref_to(&self, eid: enchudb_oplog::EntityId, himo: &str, target_eid: enchudb_oplog::EntityId) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_ref_to_by_id(eid, hid, target_eid);
    }

    /// `tie_ref_to` の himo_id 直指定版。 hot path 用。
    pub fn tie_ref_to_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, target_eid: enchudb_oplog::EntityId) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let target_eid = enchudb_oplog::eid_local(target_eid);
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        debug_assert!(
            self.value_types[hid] == ValueType::Ref || self.value_types[hid] == ValueType::Number,
            "tie_ref_to_by_id on non-Ref himo_id {}", himo_id,
        );
        self.himos[hid].set(eid, target_eid);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), eid);
            let _ = wal.append(enchudb_oplog::oplog::Op::Tie { eid: oplog_eid, himo_id, value: target_eid });
        }
    }

    // ──── untie ────

    pub fn untie(&self, eid: enchudb_oplog::EntityId, himo: &str) {
        if let Some(hid) = self.himo_id(himo) {
            self.untie_by_id(eid, hid as u16);
        }
    }

    /// `untie` の himo_id 直指定版。 未定義の himo_id (= range 外) は debug_assert で
    /// panic、 release では silently no-op (string 版が未定義 himo を no-op 扱いするのと
    /// 整合)。
    pub fn untie_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        if hid >= self.himos.len() { return; }
        self.free_leaf_cell(eid, hid);
        self.himos[hid].remove(eid);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), eid);
            let _ = wal.append(enchudb_oplog::oplog::Op::Untie { eid: oplog_eid, himo_id });
        }
    }

    // ──── delete ────

    pub fn delete(&self, eid: enchudb_oplog::EntityId) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        for hid in 0..self.himos.len() {
            self.free_leaf_cell(eid, hid);
            self.himos[hid].remove(eid);
        }
        self.entities.free(eid);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), eid);
            let _ = wal.append(enchudb_oplog::oplog::Op::Delete { eid: oplog_eid });
        }
    }

    // ──── トランザクション ────

    /// WAL 有効時のみ意味あり: Commit marker を WAL に append する。
    /// v4: undo ログ廃止に伴い、 rollback API も廃止 (詳細は CLAUDE.md / lib.rs)。
    pub fn commit(&self) {
        if let Some(wal) = self.oplog.as_ref() {
            let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
        }
    }

    // ──── content ────

    /// eid が属する named table 名。 どの named table の range にも入らなければ
    /// None (= anonymous 扱い)。
    fn table_name_of_local(&self, local: u32) -> Option<String> {
        for t in self.tables.iter() {
            if t.name.is_empty() { continue; }
            if local >= t.eid_range_lo && local < t.eid_range_hi {
                return Some(t.name.clone());
            }
        }
        None
    }

    /// 0.9.0 (#81): content key に対応する `_c_{key}` Leaf himo を lazy 確保。
    /// eid が named table 所属ならその table に、 それ以外は anonymous に定義する。
    fn ensure_content_himo(&self, local: u32, key: &str) -> u16 {
        assert!(
            !key.contains('.'),
            "content key '{}' must not contain '.' (himo 名の table separator と衝突。 \
             0.9.0 で content は Leaf himo 格納に変わった)",
            key,
        );
        let himo_name = format!("_c_{key}");
        let res = match self.table_name_of_local(local) {
            Some(t) => self.ensure_himo_dynamic_in(&t, &himo_name, ValueType::Leaf, 0),
            None => self.ensure_himo_dynamic(&himo_name, ValueType::Leaf, 0),
        };
        res.unwrap_or_else(|e| panic!("content key '{key}': himo allocation failed: {e}"))
    }

    /// full name ("table.himo" or "himo") で himo を lazy 解決。 table 部が既知
    /// table なら所属付きで、 それ以外は anonymous へ定義する (TieNamed replay 用)。
    fn ensure_himo_by_full_name(&self, full_name: &str, ht: ValueType) -> Result<u16, String> {
        if let Some((table, himo)) = full_name.split_once('.') {
            if self.tables.iter().any(|t| t.name == table) {
                return self.ensure_himo_dynamic_in(table, himo, ht, 0);
            }
        }
        self.ensure_himo_dynamic(full_name, ht, 0)
    }

    /// 0.9.0: sync 受信側用の公開版 — TieNamed の himo を名前で解決 (無ければ
    /// lazy 定義) して local hid を返す。 `himo_kind` は wire 上の生 u8。
    pub fn ensure_himo_named(&self, full_name: &str, himo_kind: u8) -> Result<u16, String> {
        self.ensure_himo_by_full_name(full_name, ValueType::from_byte(himo_kind))
    }

    /// hid が content 互換層の himo (`_c_` prefix) か。 これらは動的定義のため
    /// peer 間で himo_id が揃わず、 WAL/wire には Tie ではなく TieNamed で乗せる。
    fn himo_is_content(&self, hid: usize) -> bool {
        self.himo_names.get(hid).map_or(false, |n| {
            n.rsplit('.').next().map_or(false, |leaf| leaf.starts_with("_c_"))
        })
    }

    /// 既存の content himo id を引く (定義はしない)。 read 経路用。
    fn content_himo_id(&self, local: u32, key: &str) -> Option<u16> {
        let full = match self.table_name_of_local(local) {
            Some(t) => format!("{t}._c_{key}"),
            None => format!("_c_{key}"),
        };
        self.himo_id(&full).map(|h| h as u16)
    }

    /// entity に任意 bytes を添付する (sync 版)。
    ///
    /// 0.9.0: 保存先を旧 ContentStore region から **`_c_{key}` Leaf himo**
    /// (bytes は vocab 格納) に変更。 write は Vocab+Tie として WAL / sync /
    /// HLC / changefeed に自然に乗る。 これにより旧経路の既知バグ群
    /// (mod-16 key hash 衝突・index torn read・sync/WAL 非対応・delete 残留)
    /// が構造ごと退役する。 旧 content region は**凍結アーカイブ**:
    /// `get_content` の fallback でのみ読み、 書き込みは一切しない。
    pub fn content(&self, eid: enchudb_oplog::EntityId, key: &str, data: &[u8]) {
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        let hid = self.ensure_content_himo(local, key);
        self.tie_bytes_to_by_id(eid, hid, data);
    }

    pub fn get_content(&self, eid: enchudb_oplog::EntityId, key: &str) -> Option<&[u8]> {
        let local = enchudb_oplog::eid_local(eid);
        // 新経路 (`_c_{key}` Leaf himo) 優先
        if let Some(hid) = self.content_himo_id(local, key) {
            if let Some(vid) = self.get_by_id(eid, hid) {
                return Some(self.text_value(hid as usize, vid));
            }
        }
        // 旧 content region fallback (pre-0.9 data の read-through 互換)
        self.contents.get(local, key)
    }

    // ──── changefeed (WAL 変更通知) ────

    /// WAL に durable 化した record を listener に push する。
    /// consumer スレッドが背景 fsync 完了後に発火、HLC 昇順で渡す。
    /// 詳細は [`crate::changefeed`] のドキュメント参照。
    ///
    /// 初回 listener 追加時に emit cursor を現在の `wal.head()` に揃えるので、
    /// 過去の commit は流れない(必要なら `audit()` で取得して resume すること)。
    pub fn add_change_listener(
        &self,
        listener: std::sync::Arc<dyn crate::changefeed::ChangeListener>,
    ) {
        let was_empty = {
            let mut guard = self.change_listeners.write().unwrap();
            let was_empty = guard.is_empty();
            guard.push(listener);
            was_empty
        };
        // 初回登録時は cursor を現在の wal.head() に揃える(過去 record を流さない)
        if was_empty {
            if let Some(wal) = self.oplog.as_ref() {
                self.change_emit_offset
                    .store(wal.head(), std::sync::atomic::Ordering::Release);
            }
        }
    }

    /// 登録済み listener 数(テスト/監視用)。
    pub fn change_listener_count(&self) -> usize {
        self.change_listeners.read().unwrap().len()
    }

    // ──── blob store (大容量バイナリ: 画像/動画/モデル等) ────

    /// 大容量 blob の外部保管を注入する。通常 `LocalBlobStore` を渡す。
    /// 読み書きは `put_blob`/`get_blob` 経由、または `blob_store()` で直接取得。
    pub fn set_blob_store(&self, store: std::sync::Arc<dyn crate::blob_store::BlobStore>) {
        *self.blob_store.write().unwrap() = Some(store);
    }

    /// 注入済み blob store への Arc を返す。未設定なら None。
    pub fn blob_store(&self) -> Option<std::sync::Arc<dyn crate::blob_store::BlobStore>> {
        self.blob_store.read().unwrap().clone()
    }

    /// blob を書き込んで BlobId(sha-256、32B) を返す。blob store 未設定なら None。
    pub fn put_blob(
        &self,
        data: &[u8],
    ) -> Option<Result<crate::blob_store::BlobId, crate::blob_store::BlobError>> {
        self.blob_store().map(|s| s.put(data))
    }

    /// blob を取得。blob store 未設定なら None、存在しないなら Ok(None)。
    pub fn get_blob(
        &self,
        id: &crate::blob_store::BlobId,
    ) -> Option<Result<Option<Vec<u8>>, crate::blob_store::BlobError>> {
        self.blob_store().map(|s| s.get(id))
    }

    /// blob の存在チェック。blob store 未設定なら false。
    pub fn blob_exists(&self, id: &crate::blob_store::BlobId) -> bool {
        self.blob_store().map(|s| s.exists(id)).unwrap_or(false)
    }

    // ──── get ────

    pub fn get_text(&self, eid: enchudb_oplog::EntityId, himo: &str) -> Option<&[u8]> {
        let eid = enchudb_oplog::eid_local(eid);
        let hid = self.himo_id(himo)?;
        // Tag は vocab、 routed-Leaf (#88) は LeafStore、 reserved Leaf は vocab。
        match self.value_types[hid] {
            ValueType::Tag | ValueType::Leaf => {
                let raw = self.himos[hid].get_value(eid)?;
                Some(self.text_value(hid, raw))
            }
            _ => None,
        }
    }

    pub fn get(&self, eid: enchudb_oplog::EntityId, himo: &str) -> Option<u32> {
        let eid = enchudb_oplog::eid_local(eid);
        let hid = self.himo_id(himo)?;
        self.himos[hid].get_value(eid)
    }

    /// `get` の bindings 版。 schema 等で `himo_id` を起動時に pre-resolve した hot path 用。
    /// 名前 lookup (= himo_names の線形検索) が無くなるので point lookup が最速。
    pub fn get_by_id(&self, eid: enchudb_oplog::EntityId, hid: u16) -> Option<u32> {
        let eid = enchudb_oplog::eid_local(eid);
        self.himos.get(hid as usize)?.get_value(eid)
    }

    /// `get_by_id` の bulk 版。 同 himo の N entity を一括 column scan で `out` に append。
    /// stored 値 (= 内部表現の値+1、 0 = missing) を返す。 呼び出し側で
    /// `s.checked_sub(1)` すると `Option<u32>` に戻せる。
    ///
    /// alloc を呼ぶ側に任せて buffer reuse を可能に (= 集計ヘビーな loop で毎回 Vec を
    /// 作ると alloc が支配する)。 dominance loop は外で 4 buffer 確保 → 毎 lap で clear
    /// + 再 fill するだけ。
    #[inline]
    pub fn pull_himo_stored_many_into(
        &self, hid: u16,
        eids: &[enchudb_oplog::EntityId],
        out: &mut Vec<u32>,
    ) {
        let Some(hs) = self.himos.get(hid as usize) else {
            out.clear();
            out.resize(eids.len(), 0);
            return;
        };
        hs.get_stored_into(eids, out);
    }

    /// alloc 込みの便利版 (= 軽量 callsite 向け)。 hot loop では使わない。
    pub fn pull_himo_stored_many(&self, hid: u16, eids: &[enchudb_oplog::EntityId]) -> Vec<u32> {
        let mut out = Vec::new();
        self.pull_himo_stored_many_into(hid, eids, &mut out);
        out
    }

    /// 指定 himo に値が tie された **全** entity を列挙。 O(next_eid) で重い。
    /// schema layer の `Query::all()` のように「table の任意 column を持つ row」 を
    /// 列挙するための代表 column 経由で使う想定。
    pub fn entities_with_himo(&self, hid: u16) -> Vec<enchudb_oplog::EntityId> {
        let Some(hs) = self.himos.get(hid as usize) else { return Vec::new(); };
        let peer = self.peer_id();
        hs.entities_with_value()
            .into_iter()
            .map(|e| enchudb_oplog::make_eid(peer, e))
            .collect()
    }

    /// 指定 entity 群の紐値を合計
    pub fn sum(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> u64 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                total += v as u64;
            }
        }
        total
    }

    /// 0.8.6: himo 値が `[lo, hi]` (両端含む) の範囲に入る entity 全件を、
    /// column を直線 scan で集める。 既存 `entities_with_himo_range` 等は
    /// BucketCylinder の reverse lookup を range 内 N 値ぶん union するため、
    /// hit 率が高い (= range 内に大量 entity がいる) workload で遅い。 本 path は
    /// 「column を頭から舐めて compare」 だけなので duckdb の `BETWEEN` clause と
    /// 同じ性質、 hit 率に関わらず安定 (= 1M rows / M2 Max で ~1-3ms 目標)。
    ///
    /// 戻り値の eid は昇順、 重複なし、 peer prefix 付き (= make_eid 経由)。
    pub fn range_scan(&self, himo: &str, lo: u32, hi: u32) -> Vec<enchudb_oplog::EntityId> {
        if lo > hi { return Vec::new(); }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return Vec::new() };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let peer = self.peer_id();
        // stored 形式: 0 = missing、 stored - 1 = 値。 範囲 [lo, hi] in 値空間
        // ↔ [lo + 1, hi + 1] in stored 空間。 lo == 0 の場合の境界も自然に扱える。
        let lo_stored = lo.saturating_add(1);
        let hi_stored = hi.saturating_add(1);
        let mut hits: Vec<enchudb_oplog::EntityId> = Vec::new();
        // 22% hit 想定で reserve、 過剰なら trim
        hits.reserve(values.len() / 4);
        for (i, &stored) in values.iter().enumerate() {
            if stored >= lo_stored && stored <= hi_stored {
                hits.push(enchudb_oplog::make_eid(peer, i as u32));
            }
        }
        hits
    }

    /// 0.8.6: himo の `[lo, hi)` eid 範囲を column 直 scan で sum。
    /// schema 層の `Table::sum(col)` の internal primitive — table の eid
    /// 範囲 (= `eid_range_lo..eid_range_hi`) を渡すと、 その table の
    /// その column の合計が出る。
    ///
    /// 1M rows / M2 Max で ~100µs (= DuckDB `SELECT SUM(col) FROM tbl` の
    /// 5-6x 速い)。 eids 配列を経由しない、 mmap u32 slice を sequential に
    /// 舐めるだけの branchless tight loop が auto-vectorize する。
    pub fn sum_range(&self, himo: &str, lo: u32, hi: u32) -> u64 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        if lo >= hi { return 0; }
        let mut total: u64 = 0;
        // stored == 0 が missing、 stored > 0 のとき値は stored - 1。
        // saturating_sub(1) で missing を 0 扱いにできて branchless になる。
        for &stored in &values[lo..hi] {
            total += stored.saturating_sub(1) as u64;
        }
        total
    }

    /// 0.8.6: himo の `[lo, hi)` eid 範囲に値が tie されてる entity 数を返す
    /// (= `COUNT(col)` 相当、 missing を除いた数)。 schema 層の
    /// `Table::count(col)` の internal primitive。
    pub fn count_range(&self, himo: &str, lo: u32, hi: u32) -> u32 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        if lo >= hi { return 0; }
        let mut n: u32 = 0;
        for &stored in &values[lo..hi] {
            // stored > 0 のとき 1、 0 のとき 0 を加算 (= branchless cast)
            n += (stored != 0) as u32;
        }
        n
    }

    /// 0.8.8: himo の `[lo, hi)` eid 範囲の最小値を column 直 scan で求める。
    /// stored 形式: 0 = missing → skip、 stored > 0 のとき値 = stored - 1。
    /// 全 missing なら None。 schema 層の `Table::min(col)` の internal primitive。
    pub fn min_range(&self, himo: &str, lo: u32, hi: u32) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        if lo >= hi { return None; }
        // 最小 stored (1 以上) を探す。 0 (missing) は無視。
        let mut best: u32 = u32::MAX;
        let mut hit: bool = false;
        for &stored in &values[lo..hi] {
            if stored != 0 {
                hit = true;
                if stored < best { best = stored; }
            }
        }
        if hit { Some(best - 1) } else { None }
    }

    /// 0.8.8: himo の `[lo, hi)` eid 範囲の最大値を column 直 scan で求める。
    /// 全 missing なら None。 schema 層の `Table::max(col)` の internal primitive。
    pub fn max_range(&self, himo: &str, lo: u32, hi: u32) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        if lo >= hi { return None; }
        let mut best: u32 = 0;
        let mut hit: bool = false;
        for &stored in &values[lo..hi] {
            // stored > 0 のとき必ず best (== 0 含む) より大きくなりうる、 単純比較で OK。
            if stored > best { best = stored; hit = true; }
        }
        if hit { Some(best - 1) } else { None }
    }

    /// 0.8.8: `[lo, hi)` eid 範囲を 2 column lockstep scan して group_min。
    /// schema 層の `Table::group_min(group, val)` の internal primitive。
    /// `group_sum_range` と同じ dense / sparse 切替。
    pub fn group_min_range(
        &self,
        group_himo: &str,
        val_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(groups.len()).min(vals.len());
        if lo >= hi { return vec![]; }

        if let Some(cap) = self.group_dense_cap(gid) {
            // 値 0 は valid (stored=1 を引いた値) なので、 mins[g] != u32::MAX を
            // 「データ有り」の代用にする。 stored で持ったまま比較すれば mins init=u32::MAX
            // のときは必ず上書きされる (= seen tracking 不要)。
            let mut mins_stored: Vec<u32> = vec![u32::MAX; cap];
            for i in lo..hi {
                let g_stored = groups[i];
                let v_stored = vals[i];
                if g_stored == 0 || v_stored == 0 { continue; }
                let g = (g_stored - 1) as usize;
                if g < cap && v_stored < mins_stored[g] {
                    mins_stored[g] = v_stored;
                }
            }
            (0..cap).filter(|&i| mins_stored[i] != u32::MAX)
                .map(|i| (i as u32, mins_stored[i] - 1)).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for i in lo..hi {
                let g_stored = groups[i];
                let v_stored = vals[i];
                if g_stored == 0 || v_stored == 0 { continue; }
                let g = g_stored - 1;
                let entry = map.entry(g).or_insert(u32::MAX);
                if v_stored < *entry { *entry = v_stored; }
            }
            map.into_iter().map(|(g, v_stored)| (g, v_stored - 1)).collect()
        }
    }

    /// 0.8.8: `[lo, hi)` eid 範囲を 2 column lockstep scan して group_max。
    /// schema 層の `Table::group_max(group, val)` の internal primitive。
    pub fn group_max_range(
        &self,
        group_himo: &str,
        val_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(groups.len()).min(vals.len());
        if lo >= hi { return vec![]; }

        if let Some(cap) = self.group_dense_cap(gid) {
            // maxs_stored の init = 0 (== missing 扱い)。 stored > 0 を見たら必ず
            // 上書きされるので、 結果 filter で maxs_stored > 0 を有効データ判定。
            let mut maxs_stored: Vec<u32> = vec![0; cap];
            for i in lo..hi {
                let g_stored = groups[i];
                let v_stored = vals[i];
                if g_stored == 0 || v_stored == 0 { continue; }
                let g = (g_stored - 1) as usize;
                if g < cap && v_stored > maxs_stored[g] {
                    maxs_stored[g] = v_stored;
                }
            }
            (0..cap).filter(|&i| maxs_stored[i] != 0)
                .map(|i| (i as u32, maxs_stored[i] - 1)).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for i in lo..hi {
                let g_stored = groups[i];
                let v_stored = vals[i];
                if g_stored == 0 || v_stored == 0 { continue; }
                let g = g_stored - 1;
                let entry = map.entry(g).or_insert(0);
                if v_stored > *entry { *entry = v_stored; }
            }
            map.into_iter().map(|(g, v_stored)| (g, v_stored - 1)).collect()
        }
    }

    /// 0.8.8: `[lo, hi)` eid 範囲を column 直 scan して、 値域 `[vmin, vmax]` を
    /// `n_buckets` 等分した頻度ヒストグラムを返す。 値が `[vmin, vmax]` 外の
    /// entity はカウントされない (= clipping ではなく drop)。
    ///
    /// `n_buckets == 0` または `vmin > vmax` の場合は空 Vec。 戻り値長は
    /// `n_buckets` で固定 (= bucket 0 件でも 0 で埋める)。
    ///
    /// bucket index は `((val - vmin) * n_buckets) / (vmax - vmin + 1)`
    /// (= floor division で 0..n_buckets に均等割当)。
    pub fn histogram_range(
        &self,
        himo: &str,
        lo: u32,
        hi: u32,
        vmin: u32,
        vmax: u32,
        n_buckets: u32,
    ) -> Vec<u32> {
        if n_buckets == 0 || vmin > vmax { return vec![]; }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![0; n_buckets as usize] };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        let n = n_buckets as usize;
        let mut hist: Vec<u32> = vec![0; n];
        if lo >= hi { return hist; }
        // 値空間幅 (両端含む、 vmin == vmax のときは 1)
        let span = (vmax as u64 - vmin as u64) + 1;
        let n_u64 = n_buckets as u64;
        for &stored in &values[lo..hi] {
            if stored == 0 { continue; }
            let val = stored - 1;
            if val < vmin || val > vmax { continue; }
            // (val - vmin) * n / span。 span >= 1、 val - vmin < span を保証してる
            // ので idx < n は厳密に成り立つ (= bound check 不要だが safety で min)。
            let idx = (((val - vmin) as u64) * n_u64 / span) as usize;
            hist[idx.min(n - 1)] += 1;
        }
        hist
    }

    // ──── 0.8.9 (#39): bulk column scan の rayon 並列化 ────
    //
    // 既存 `_range` 系を **`_par` suffix で並列版** として複製。 callsite で
    // 「並列で OK (= 大規模 read-only scan)」 と分かってる場面に明示利用させる。
    // 閾値 `PAR_RANGE_THRESHOLD` (= 64k 要素) 以下では seq fallback、
    // thread overhead が利益を上回らないため。
    //
    // 並列化は `par_chunks(CHUNK_SIZE)` 経由で値 slice を区切り、 各 chunk で
    // 局所 accumulator を計算 → `reduce` で合算。 HimoStore は内部に
    // `RwLock<BucketCylinder>` を持つが、 ここで触る `stored_slice` は
    // immutable な mmap view (= read 中は lock 不要) なので thread-safe。

    /// 並列化閾値。 これ未満の `[lo, hi)` 幅では seq fallback (= thread spawn
    /// overhead が利益を上回らない、 実測ベースで 64k 程度が境界)。
    const PAR_RANGE_THRESHOLD: usize = 64_000;

    /// chunk 粒度。 16k 要素 = 64KB (u32 として)、 L2 cache friendly。
    const PAR_RANGE_CHUNK: usize = 16_384;

    /// 0.8.9: `sum_range` の並列版。 chunk ごとに local sum を計算 → reduce。
    /// 閾値以下は seq fallback。 12M row scan で seq 1.95s → par ~600ms 程度
    /// (= 4 thread / M2 Max 想定)。
    pub fn sum_range_par(&self, himo: &str, lo: u32, hi: u32) -> u64 {
        use rayon::prelude::*;
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        if lo_u >= hi_u { return 0; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.sum_range(himo, lo, hi);
        }
        values[lo_u..hi_u]
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut total: u64 = 0;
                for &stored in chunk {
                    total += stored.saturating_sub(1) as u64;
                }
                total
            })
            .sum()
    }

    /// 0.8.9: `count_range` の並列版。
    pub fn count_range_par(&self, himo: &str, lo: u32, hi: u32) -> u32 {
        use rayon::prelude::*;
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        if lo_u >= hi_u { return 0; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.count_range(himo, lo, hi);
        }
        values[lo_u..hi_u]
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut n: u32 = 0;
                for &stored in chunk {
                    n += (stored != 0) as u32;
                }
                n
            })
            .sum()
    }

    /// 0.8.9: `min_range` の並列版。 各 chunk で local min を取り、 全 chunk を
    /// reduce で min 結合。 全 missing なら None。
    pub fn min_range_par(&self, himo: &str, lo: u32, hi: u32) -> Option<u32> {
        use rayon::prelude::*;
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        if lo_u >= hi_u { return None; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.min_range(himo, lo, hi);
        }
        // chunk 内で None / Some(best_stored) を返し、 reduce で min 結合。
        let result = values[lo_u..hi_u]
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut best: u32 = u32::MAX;
                let mut hit = false;
                for &stored in chunk {
                    if stored != 0 {
                        hit = true;
                        if stored < best { best = stored; }
                    }
                }
                if hit { Some(best) } else { None }
            })
            .reduce(|| None, |a, b| match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(x.min(y)),
            });
        result.map(|stored| stored - 1)
    }

    /// 0.8.9: `max_range` の並列版。
    pub fn max_range_par(&self, himo: &str, lo: u32, hi: u32) -> Option<u32> {
        use rayon::prelude::*;
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        if lo_u >= hi_u { return None; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.max_range(himo, lo, hi);
        }
        let result = values[lo_u..hi_u]
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut best: u32 = 0;
                for &stored in chunk {
                    if stored > best { best = stored; }
                }
                if best > 0 { Some(best) } else { None }
            })
            .reduce(|| None, |a, b| match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(x.max(y)),
            });
        result.map(|stored| stored - 1)
    }

    /// 0.8.9: `group_sum_range` の並列版。 dense path のみ並列化、 sparse
    /// (= HashMap merge コスト高) は seq fallback。 chunk ごとに thread-local
    /// `Vec<u64>` を持って scatter add → reduce で要素ごと加算。
    pub fn group_sum_range_par(
        &self,
        group_himo: &str,
        sum_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u64)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let groups = gs.stored_slice();
        let sums = ss.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(groups.len()).min(sums.len());
        if lo_u >= hi_u { return vec![]; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.group_sum_range(group_himo, sum_himo, lo, hi);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            // sparse は HashMap merge が重いので seq に fallback
            return self.group_sum_range(group_himo, sum_himo, lo, hi);
        };
        // chunk 範囲は groups / sums の lo..hi を index 同期で zip するため、
        // index 列を slice しないと indexes が ずれる。 zip 後の slice を chunk する。
        // ただし par_chunks_exact は zip slice を受け付けないので、 chunked range
        // を loop してから内部で indexing する形に。
        let chunk = Self::PAR_RANGE_CHUNK;
        let n = hi_u - lo_u;
        let n_chunks = (n + chunk - 1) / chunk;
        let result: Vec<u64> = (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = lo_u + ci * chunk;
                let e = (s + chunk).min(hi_u);
                let mut acc: Vec<u64> = vec![0; cap];
                for i in s..e {
                    let g_stored = groups[i];
                    let v_stored = sums[i];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap {
                        acc[g] += (v_stored - 1) as u64;
                    }
                }
                acc
            })
            .reduce(|| vec![0u64; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() { a[i] += v; }
                a
            });
        (0..cap).filter(|&i| result[i] > 0).map(|i| (i as u32, result[i])).collect()
    }

    /// 0.8.9: `group_min_range` の並列版。 dense path のみ。
    pub fn group_min_range_par(
        &self,
        group_himo: &str,
        val_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u32)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(groups.len()).min(vals.len());
        if lo_u >= hi_u { return vec![]; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.group_min_range(group_himo, val_himo, lo, hi);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_min_range(group_himo, val_himo, lo, hi);
        };
        let chunk = Self::PAR_RANGE_CHUNK;
        let n = hi_u - lo_u;
        let n_chunks = (n + chunk - 1) / chunk;
        let result: Vec<u32> = (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = lo_u + ci * chunk;
                let e = (s + chunk).min(hi_u);
                let mut mins: Vec<u32> = vec![u32::MAX; cap];
                for i in s..e {
                    let g_stored = groups[i];
                    let v_stored = vals[i];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap && v_stored < mins[g] {
                        mins[g] = v_stored;
                    }
                }
                mins
            })
            .reduce(|| vec![u32::MAX; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() {
                    if v < a[i] { a[i] = v; }
                }
                a
            });
        (0..cap).filter(|&i| result[i] != u32::MAX)
            .map(|i| (i as u32, result[i] - 1)).collect()
    }

    /// 0.8.9: `group_max_range` の並列版。 dense path のみ。
    pub fn group_max_range_par(
        &self,
        group_himo: &str,
        val_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u32)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(groups.len()).min(vals.len());
        if lo_u >= hi_u { return vec![]; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.group_max_range(group_himo, val_himo, lo, hi);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_max_range(group_himo, val_himo, lo, hi);
        };
        let chunk = Self::PAR_RANGE_CHUNK;
        let n = hi_u - lo_u;
        let n_chunks = (n + chunk - 1) / chunk;
        let result: Vec<u32> = (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = lo_u + ci * chunk;
                let e = (s + chunk).min(hi_u);
                let mut maxs: Vec<u32> = vec![0; cap];
                for i in s..e {
                    let g_stored = groups[i];
                    let v_stored = vals[i];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap && v_stored > maxs[g] {
                        maxs[g] = v_stored;
                    }
                }
                maxs
            })
            .reduce(|| vec![0u32; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() {
                    if v > a[i] { a[i] = v; }
                }
                a
            });
        (0..cap).filter(|&i| result[i] != 0)
            .map(|i| (i as u32, result[i] - 1)).collect()
    }

    /// 0.8.9: `range_scan` の並列版。 chunk ごとに local hits → flat_map で連結。
    /// 結果順序は eid 昇順 (= chunk が lo→hi の順で並列実行されて、 各 chunk 内も
    /// 昇順なので、 flat 連結後も全体として昇順を保つ)。
    pub fn range_scan_par(&self, himo: &str, lo: u32, hi: u32) -> Vec<enchudb_oplog::EntityId> {
        use rayon::prelude::*;
        if lo > hi { return Vec::new(); }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return Vec::new() };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        if values.len() < Self::PAR_RANGE_THRESHOLD {
            return self.range_scan(himo, lo, hi);
        }
        let peer = self.peer_id();
        let lo_stored = lo.saturating_add(1);
        let hi_stored = hi.saturating_add(1);
        let chunk = Self::PAR_RANGE_CHUNK;
        let n_chunks = (values.len() + chunk - 1) / chunk;
        // chunk 単位で local Vec を作って flat_map で連結。 chunk 番号順に
        // 結果が並ぶので、 各 chunk 内が昇順なら全体も昇順。
        (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = ci * chunk;
                let e = (s + chunk).min(values.len());
                let mut local: Vec<enchudb_oplog::EntityId> = Vec::new();
                for i in s..e {
                    let stored = values[i];
                    if stored >= lo_stored && stored <= hi_stored {
                        local.push(enchudb_oplog::make_eid(peer, i as u32));
                    }
                }
                local
            })
            .reduce(Vec::new, |mut a, b| { a.extend(b); a })
    }

    /// 0.8.9: `histogram_range` の並列版。 chunk ごとに thread-local
    /// `Vec<u32>` で scatter add → reduce で要素加算。
    pub fn histogram_range_par(
        &self,
        himo: &str,
        lo: u32,
        hi: u32,
        vmin: u32,
        vmax: u32,
        n_buckets: u32,
    ) -> Vec<u32> {
        use rayon::prelude::*;
        if n_buckets == 0 || vmin > vmax { return vec![]; }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![0; n_buckets as usize] };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        let n = n_buckets as usize;
        if lo_u >= hi_u { return vec![0; n]; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.histogram_range(himo, lo, hi, vmin, vmax, n_buckets);
        }
        let span = (vmax as u64 - vmin as u64) + 1;
        let n_u64 = n_buckets as u64;
        let chunk = Self::PAR_RANGE_CHUNK;
        let n_chunks = (hi_u - lo_u + chunk - 1) / chunk;
        (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = lo_u + ci * chunk;
                let e = (s + chunk).min(hi_u);
                let mut hist: Vec<u32> = vec![0; n];
                for i in s..e {
                    let stored = values[i];
                    if stored == 0 { continue; }
                    let val = stored - 1;
                    if val < vmin || val > vmax { continue; }
                    let idx = (((val - vmin) as u64) * n_u64 / span) as usize;
                    hist[idx.min(n - 1)] += 1;
                }
                hist
            })
            .reduce(|| vec![0u32; n], |mut a, b| {
                for (i, &v) in b.iter().enumerate() { a[i] += v; }
                a
            })
    }

    /// 最小値
    pub fn min(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                result = Some(result.map_or(v, |cur: u32| cur.min(v)));
            }
        }
        result
    }

    /// 最大値
    pub fn max(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                result = Some(result.map_or(v, |cur: u32| cur.max(v)));
            }
        }
        result
    }

    /// 平均（整数除算）
    pub fn avg(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u64> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        let mut count: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                total += v as u64;
                count += 1;
            }
        }
        if count == 0 { None } else { Some(total / count) }
    }

    /// 値を持つ entity の数
    pub fn count(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> u32 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut n: u32 = 0;
        for &eid in eids {
            if hs.get_value(enchudb_oplog::eid_local(eid)).is_some() { n += 1; }
        }
        n
    }

    /// GROUP BY + SUM — group_himo の値でグループ化し、sum_himo の値を合計
    ///
    /// group 値の cardinality を見て、 dense (= max_values 小) なら Vec 直 index、
    /// sparse なら HashMap で集計。
    pub fn group_sum(&self, group_himo: &str, sum_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let cap = self.group_dense_cap(gid);
        if let Some(cap) = cap {
            let mut sums: Vec<u64> = vec![0; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), ss.get_value(local)) {
                    let i = group as usize;
                    sums[i] += val as u64;
                    seen[i] = true;
                }
            }
            (0..cap).filter(|&i| seen[i]).map(|i| (i as u32, sums[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), ss.get_value(local)) {
                    *map.entry(group).or_insert(0) += val as u64;
                }
            }
            map.into_iter().collect()
        }
    }

    /// 0.8.6: `[lo, hi)` eid 範囲の 2 column を lockstep scan して group_sum。
    /// schema 層の `Table::group_sum(group, sum)` の internal primitive。
    ///
    /// 1M rows / M2 Max で ~10ms (= 残念ながら DuckDB ~1.5ms には及ばない、
    /// NEON で native scatter が無く、 acc[g] += v が ILP/vector 化困難な
    /// scatter write のため。 algorithmic 工夫は別 work)。
    pub fn group_sum_range(
        &self,
        group_himo: &str,
        sum_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let groups = gs.stored_slice();
        let sums = ss.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(groups.len()).min(sums.len());
        if lo >= hi { return vec![]; }

        if let Some(cap) = self.group_dense_cap(gid) {
            // dense cap での group_sum: seen[] tracking を廃止して
            // hot loop の per-iter store を 2 → 1 に圧縮。
            // 「acc[g] == 0」 が non-empty 判定の代用、 値 0 と「データ無し」を
            // 区別したい時は別 API を使うこと。
            let mut acc: Vec<u64> = vec![0; cap];
            for i in lo..hi {
                let g_stored = groups[i];
                let s_stored = sums[i];
                if g_stored == 0 || s_stored == 0 { continue; }
                let g = (g_stored - 1) as usize;
                if g < cap {
                    acc[g] += (s_stored - 1) as u64;
                }
            }
            (0..cap).filter(|&i| acc[i] > 0).map(|i| (i as u32, acc[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
            for i in lo..hi {
                let g_stored = groups[i];
                let s_stored = sums[i];
                if g_stored == 0 || s_stored == 0 { continue; }
                *map.entry(g_stored - 1).or_insert(0) += (s_stored - 1) as u64;
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + COUNT — group_himo の値でグループ化し、各グループの entity 数
    pub fn group_count(&self, group_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut counts: Vec<u32> = vec![0; cap];
            for &eid in eids {
                if let Some(group) = gs.get_value(enchudb_oplog::eid_local(eid)) {
                    counts[group as usize] += 1;
                }
            }
            (0..cap).filter(|&i| counts[i] > 0).map(|i| (i as u32, counts[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for &eid in eids {
                if let Some(group) = gs.get_value(enchudb_oplog::eid_local(eid)) {
                    *map.entry(group).or_insert(0) += 1;
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + MIN
    pub fn group_min(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut mins: Vec<u32> = vec![u32::MAX; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let i = group as usize;
                    if val < mins[i] { mins[i] = val; }
                    seen[i] = true;
                }
            }
            (0..cap).filter(|&i| seen[i]).map(|i| (i as u32, mins[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let entry = map.entry(group).or_insert(u32::MAX);
                    if val < *entry { *entry = val; }
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + MAX
    pub fn group_max(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut maxs: Vec<u32> = vec![0; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let i = group as usize;
                    if val > maxs[i] { maxs[i] = val; }
                    seen[i] = true;
                }
            }
            (0..cap).filter(|&i| seen[i]).map(|i| (i as u32, maxs[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let entry = map.entry(group).or_insert(0);
                    if val > *entry { *entry = val; }
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + AVG (整数除算)
    pub fn group_avg(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut sums: Vec<u64> = vec![0; cap];
            let mut cnts: Vec<u64> = vec![0; cap];
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let i = group as usize;
                    sums[i] += val as u64;
                    cnts[i] += 1;
                }
            }
            (0..cap).filter(|&i| cnts[i] > 0).map(|i| (i as u32, sums[i] / cnts[i])).collect()
        } else {
            let mut acc: std::collections::HashMap<u32, (u64, u64)> = std::collections::HashMap::new();
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let e = acc.entry(group).or_insert((0, 0));
                    e.0 += val as u64;
                    e.1 += 1;
                }
            }
            acc.into_iter().map(|(k, (s, n))| (k, s / n)).collect()
        }
    }

    /// group 系の dense path 適用判定。 himo の max_values が小さく定義されていれば
    /// その値範囲を Vec 直接 index で集計できる。 閾値は 64K (Vec 確保 256KB 以下) まで。
    fn group_dense_cap(&self, hid: usize) -> Option<usize> {
        let hs = &self.himos[hid];
        let cap = hs.max_values as usize;
        if cap == 0 || cap > 65_536 { None } else { Some(cap) }
    }

    /// 値集合の distinct — eids の中で himo に張られた値のユニーク集合
    pub fn distinct(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<u32> {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![] };
        let hs = &self.himos[hid];
        let mut result: Vec<u32> = Vec::new();
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                if !result.contains(&v) { result.push(v); }
            }
        }
        result
    }

    // ──── 0.8.10 (#43): 不連続 eids 群への集計 (par 版 + histogram_eids) ────
    //
    // 既存 `sum(himo, eids)` / `count` / `min` / `max` / `group_*` は seq 版のみ
    // だった。 sub-set (= `Query::where_*` で絞った eid 群) は連続 range と違って
    // `_range_par` が呼べない (= 不連続)。 そこで eids 版の並列版を追加して、
    // schema の `Query` 終端 method (= 集計 chain) の中で大規模 sub-set 集計を
    // 走らせられるようにする。
    //
    // 並列化方針:
    //   - `eids.par_chunks(PAR_RANGE_CHUNK)` で並列、 各 chunk で `stored_slice`
    //     を indirect access (= `col[eid_local(e)]`) で scatter read
    //   - `_range_par` (= sequential SIMD) と違って cache-unfriendly だが
    //     thread 並列度で稼ぐ
    //   - `eids.len() < PAR_RANGE_THRESHOLD` では seq fallback
    //
    // `histogram_eids` は seq 版が無かったので 0.8.10 で新規追加。

    /// 0.8.10: `sum(himo, eids)` の並列版。 stored_slice の indirect access。
    pub fn sum_eids_par(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> u64 {
        use rayon::prelude::*;
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.sum(himo, eids);
        }
        let values = hs.stored_slice();
        eids.par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut total: u64 = 0;
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local < values.len() {
                        total += values[local].saturating_sub(1) as u64;
                    }
                }
                total
            })
            .sum()
    }

    /// 0.8.10: `count(himo, eids)` の並列版。
    pub fn count_eids_par(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> u32 {
        use rayon::prelude::*;
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.count(himo, eids);
        }
        let values = hs.stored_slice();
        eids.par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut n: u32 = 0;
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local < values.len() && values[local] != 0 {
                        n += 1;
                    }
                }
                n
            })
            .sum()
    }

    /// 0.8.10: `min(himo, eids)` の並列版。
    pub fn min_eids_par(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u32> {
        use rayon::prelude::*;
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.min(himo, eids);
        }
        let values = hs.stored_slice();
        let result = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut best: u32 = u32::MAX;
                let mut hit = false;
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local < values.len() {
                        let stored = values[local];
                        if stored != 0 {
                            hit = true;
                            if stored < best { best = stored; }
                        }
                    }
                }
                if hit { Some(best) } else { None }
            })
            .reduce(|| None, |a, b| match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(x.min(y)),
            });
        result.map(|stored| stored - 1)
    }

    /// 0.8.10: `max(himo, eids)` の並列版。
    pub fn max_eids_par(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u32> {
        use rayon::prelude::*;
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.max(himo, eids);
        }
        let values = hs.stored_slice();
        let result = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut best: u32 = 0;
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local < values.len() {
                        let stored = values[local];
                        if stored > best { best = stored; }
                    }
                }
                if best > 0 { Some(best) } else { None }
            })
            .reduce(|| None, |a, b| match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(x.max(y)),
            });
        result.map(|stored| stored - 1)
    }

    /// 0.8.10: `group_sum(group, sum, eids)` の並列版。 dense path のみ並列、
    /// sparse (= max_values 0 or > 64K) は seq fallback。
    pub fn group_sum_eids_par(
        &self,
        group_himo: &str,
        sum_himo: &str,
        eids: &[enchudb_oplog::EntityId],
    ) -> Vec<(u32, u64)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.group_sum(group_himo, sum_himo, eids);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_sum(group_himo, sum_himo, eids);
        };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let groups = gs.stored_slice();
        let sums = ss.stored_slice();
        let result: Vec<u64> = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut acc: Vec<u64> = vec![0; cap];
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local >= groups.len() || local >= sums.len() { continue; }
                    let g_stored = groups[local];
                    let s_stored = sums[local];
                    if g_stored == 0 || s_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap {
                        acc[g] += (s_stored - 1) as u64;
                    }
                }
                acc
            })
            .reduce(|| vec![0u64; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() { a[i] += v; }
                a
            });
        (0..cap).filter(|&i| result[i] > 0).map(|i| (i as u32, result[i])).collect()
    }

    /// 0.8.10: `group_min(group, val, eids)` の並列版。 dense path のみ。
    pub fn group_min_eids_par(
        &self,
        group_himo: &str,
        val_himo: &str,
        eids: &[enchudb_oplog::EntityId],
    ) -> Vec<(u32, u32)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.group_min(group_himo, val_himo, eids);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_min(group_himo, val_himo, eids);
        };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let result: Vec<u32> = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut mins: Vec<u32> = vec![u32::MAX; cap];
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local >= groups.len() || local >= vals.len() { continue; }
                    let g_stored = groups[local];
                    let v_stored = vals[local];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap && v_stored < mins[g] {
                        mins[g] = v_stored;
                    }
                }
                mins
            })
            .reduce(|| vec![u32::MAX; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() {
                    if v < a[i] { a[i] = v; }
                }
                a
            });
        (0..cap).filter(|&i| result[i] != u32::MAX)
            .map(|i| (i as u32, result[i] - 1)).collect()
    }

    /// 0.8.10: `group_max(group, val, eids)` の並列版。 dense path のみ。
    pub fn group_max_eids_par(
        &self,
        group_himo: &str,
        val_himo: &str,
        eids: &[enchudb_oplog::EntityId],
    ) -> Vec<(u32, u32)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.group_max(group_himo, val_himo, eids);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_max(group_himo, val_himo, eids);
        };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let result: Vec<u32> = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut maxs: Vec<u32> = vec![0; cap];
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local >= groups.len() || local >= vals.len() { continue; }
                    let g_stored = groups[local];
                    let v_stored = vals[local];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap && v_stored > maxs[g] {
                        maxs[g] = v_stored;
                    }
                }
                maxs
            })
            .reduce(|| vec![0u32; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() {
                    if v > a[i] { a[i] = v; }
                }
                a
            });
        (0..cap).filter(|&i| result[i] != 0)
            .map(|i| (i as u32, result[i] - 1)).collect()
    }

    /// 0.8.10: 不連続 eids への histogram (= range scan できない sub-set 向け、
    /// schema `Query::histogram` の bind 先)。 `histogram_range` と同じ semantics
    /// (= 値域外 drop、 戻り値長は常に `n_buckets`)。
    pub fn histogram_eids(
        &self,
        himo: &str,
        eids: &[enchudb_oplog::EntityId],
        vmin: u32,
        vmax: u32,
        n_buckets: u32,
    ) -> Vec<u32> {
        if n_buckets == 0 || vmin > vmax { return vec![]; }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![0; n_buckets as usize] };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let n = n_buckets as usize;
        let mut hist: Vec<u32> = vec![0; n];
        if eids.is_empty() { return hist; }
        let span = (vmax as u64 - vmin as u64) + 1;
        let n_u64 = n_buckets as u64;
        for &eid in eids {
            let local = enchudb_oplog::eid_local(eid) as usize;
            if local >= values.len() { continue; }
            let stored = values[local];
            if stored == 0 { continue; }
            let val = stored - 1;
            if val < vmin || val > vmax { continue; }
            let idx = (((val - vmin) as u64) * n_u64 / span) as usize;
            hist[idx.min(n - 1)] += 1;
        }
        hist
    }

    /// 0.8.10: `histogram_eids` の並列版。
    pub fn histogram_eids_par(
        &self,
        himo: &str,
        eids: &[enchudb_oplog::EntityId],
        vmin: u32,
        vmax: u32,
        n_buckets: u32,
    ) -> Vec<u32> {
        use rayon::prelude::*;
        if n_buckets == 0 || vmin > vmax { return vec![]; }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![0; n_buckets as usize] };
        let hs = &self.himos[hid];
        let n = n_buckets as usize;
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.histogram_eids(himo, eids, vmin, vmax, n_buckets);
        }
        let values = hs.stored_slice();
        let span = (vmax as u64 - vmin as u64) + 1;
        let n_u64 = n_buckets as u64;
        eids.par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut hist: Vec<u32> = vec![0; n];
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local >= values.len() { continue; }
                    let stored = values[local];
                    if stored == 0 { continue; }
                    let val = stored - 1;
                    if val < vmin || val > vmax { continue; }
                    let idx = (((val - vmin) as u64) * n_u64 / span) as usize;
                    hist[idx.min(n - 1)] += 1;
                }
                hist
            })
            .reduce(|| vec![0u32; n], |mut a, b| {
                for (i, &v) in b.iter().enumerate() { a[i] += v; }
                a
            })
    }

    // ──── 範囲クエリ ────

    /// 範囲内の全値に合致する entity を返す（min..=max）
    pub fn pull_range(&self, himo: &str, min: u32, max: u32) -> Vec<enchudb_oplog::EntityId> {
        let idx = match self.himo_id(himo) { Some(h) => h, None => return vec![] };
        let hs = &self.himos[idx];
        let mut result = Vec::new();
        for v in min..=max {
            for local in &hs.pull(v) {
                result.push(*local as enchudb_oplog::EntityId);
            }
        }
        result
    }

    // ──── 日付ヘルパー ────

    /// (year, month, day) → epoch 日数（2000-01-01 = 0）
    pub fn date_to_days(year: u32, month: u32, day: u32) -> u32 {
        let mut y = year as i64;
        let mut m = month as i64;
        if m <= 2 { y -= 1; m += 12; }
        let days = 365 * y + y / 4 - y / 100 + y / 400 + (153 * (m - 3) + 2) / 5 + day as i64 - 1;
        let epoch = {
            let ey: i64 = 2000; let em: i64 = 1;
            let ey2 = ey - 1; // Jan is <= 2, so y-1, m+12
            let em2 = 13i64;
            365 * ey2 + ey2 / 4 - ey2 / 100 + ey2 / 400 + (153 * (em2 - 3) + 2) / 5 + em as i64 - 1
        };
        (days - epoch) as u32
    }

    /// epoch 日数 → (year, month, day)
    pub fn days_to_date(days: u32) -> (u32, u32, u32) {
        // 2000-01-01 の Julian Day Number
        let jdn = days as i64 + 2451545;
        let a = jdn + 32044;
        let b = (4 * a + 3) / 146097;
        let c = a - (146097 * b) / 4;
        let d = (4 * c + 3) / 1461;
        let e = c - (1461 * d) / 4;
        let m = (5 * e + 2) / 153;
        let day = e - (153 * m + 2) / 5 + 1;
        let month = m + 3 - 12 * (m / 10);
        let year = 100 * b + d - 4800 + m / 10;
        (year as u32, month as u32, day as u32)
    }

    /// 日付を epoch 日数として tie
    pub fn tie_date(&mut self, eid: enchudb_oplog::EntityId, himo: &str, year: u32, month: u32, day: u32) {
        self.tie(eid, himo, Self::date_to_days(year, month, day));
    }

    /// epoch 日数から (year, month, day) を返す
    pub fn get_date(&self, eid: enchudb_oplog::EntityId, himo: &str) -> Option<(u32, u32, u32)> {
        self.get(eid, himo).map(Self::days_to_date)
    }

    /// 日付範囲で pull_range
    pub fn pull_date_range(&self, himo: &str, from: (u32, u32, u32), to: (u32, u32, u32)) -> Vec<enchudb_oplog::EntityId> {
        let min = Self::date_to_days(from.0, from.1, from.2);
        let max = Self::date_to_days(to.0, to.1, to.2);
        self.pull_range(himo, min, max)
    }

    pub fn vocab_id(&self, text: &str) -> Option<u32> { self.vocab.lookup(text.as_bytes()) }

    /// 0.7.0: text を vocab に inject して vocab_id を返す (idempotent)。
    /// 既存の `intern_table_name` 系で entity → tie_text → delete の dummy roundtrip
    /// をしていた path を、 vocab 直接 inject に置換するための公開 API。
    /// entity / table 経路を一切触らないので、 anonymous closed 後でも安全に呼べる。
    pub fn vocab_intern_text(&self, text: &str) -> u32 {
        self.vocab.get_or_insert(text.as_bytes())
    }

    /// vocab ID → bytes（vocabulary 経由で文字列復元用）
    pub fn vocab_text(&self, vid: u32) -> &[u8] { self.vocab.get(vid) }

    /// 紐の文脈で文字列のvocab IDを探す。その紐にぶら下がってる値だけ調べる。
    pub fn find_value(&self, himo: &str, text: &str) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let text_bytes = text.as_bytes();
        let vals = self.himos[hid].unique_values();
        for vid in vals {
            if self.vocab.get(vid) == text_bytes {
                return Some(vid);
            }
        }
        None
    }

    pub fn himos_of(&self, eid: enchudb_oplog::EntityId) -> Vec<&str> {
        let eid = enchudb_oplog::eid_local(eid);
        self.himos.iter().enumerate()
            .filter(|(_, ds)| ds.get_value(eid).is_some())
            .map(|(i, _)| self.himo_names[i].as_str())
            .collect()
    }
    /// 1 entity の全フィールドを一括取得。HashMap ルックアップ 0 回。
    pub fn get_entity(&self, eid: enchudb_oplog::EntityId) -> Vec<(&str, EntityValue<'_>)> {
        let eid = enchudb_oplog::eid_local(eid);
        let mut fields = Vec::with_capacity(self.himos.len());
        for (i, hs) in self.himos.iter().enumerate() {
            if let Some(raw) = hs.get_value(eid) {
                let val = match self.value_types[i] {
                    ValueType::Tag | ValueType::Leaf => EntityValue::Text(self.text_value(i, raw)),
                    _ => EntityValue::Num(raw),
                };
                fields.push((self.himo_names[i].as_str(), val));
            }
        }
        fields
    }

    #[allow(dead_code)]
    pub(crate) fn vocab(&self) -> &Vocabulary { &self.vocab }
    pub fn himo_names(&self) -> &[String] { self.himo_names.as_slice() }

    pub fn value_type(&self, himo: &str) -> Option<ValueType> {
        self.himo_id(himo).map(|idx| self.value_types[idx])
    }

    /// 指定紐の現在の unique 値数(非空バケット数)。O(1)。
    /// 紐が未定義なら None。
    ///
    /// v27 の BucketCylinder は tie/untie/remove 時に AtomicU32 を増減させる。
    /// `define_himo` の `max_values` はヒントに過ぎないので、ここで返るのは
    /// 実データ上の cardinality。
    pub fn himo_cardinality(&self, himo: &str) -> Option<u32> {
        let idx = self.himo_id(himo)?;
        Some(self.himos[idx].unique_count())
    }

    /// himo の Cylinder が確保している eid backing の総 bytes（メモリ観測用、#95）。
    /// append-only なので各 eid は 1 度だけ載る = `unique×平均 × pow2 slack`。
    /// double-buffer していれば `>= 2×(eid 数×4)` になるので、その検知にも使える。
    pub fn himo_cylinder_backing_bytes(&self, himo: &str) -> Option<usize> {
        let idx = self.himo_id(himo)?;
        Some(self.himos[idx].cyl_backing_bytes())
    }

    // ──── 紐を引く（Cylinder 経由）────

    /// Cylinder + Bitmap キャッシュを再構築。delta をクリア。
    pub fn rebuild(&self) {
        for ds in &self.himos { ds.rebuild_cylinder(); }
    }

    /// 引く。 EntityId(u64) の Vec を返す。
    pub fn pull_raw(&self, himo: &str, value: u32) -> Vec<enchudb_oplog::EntityId> {
        match self.himo_id(himo) {
            Some(idx) => self.himos[idx].pull(value).into_iter().map(|e| e as enchudb_oplog::EntityId).collect(),
            None => Vec::new(),
        }
    }

    /// 引く。 HimoStore::pull (RwLock + clone) 直。
    pub fn pull(&self, himo: &str, value: u32) -> Vec<u32> {
        match self.himo_id(himo) {
            Some(idx) => self.himos[idx].pull(value),
            None => Vec::new(),
        }
    }

    /// 複数値の bucket を union する。 `WHERE col IN (v1, v2, ...)` / Ravn follow 後の union 用。
    /// 結果は sort + dedup 済み。
    pub fn pull_in(&self, himo: &str, values: &[u32]) -> Vec<enchudb_oplog::EntityId> {
        match self.himo_id(himo) {
            Some(idx) => self.pull_in_by_idx(idx, values),
            None => Vec::new(),
        }
    }

    /// schema 層用: himo_id pre-resolve 済みの `pull_in`。
    pub fn pull_in_by_id(&self, himo_id: u16, values: &[u32]) -> Vec<enchudb_oplog::EntityId> {
        let idx = himo_id as usize;
        if idx >= self.himos.len() { return Vec::new(); }
        self.pull_in_by_idx(idx, values)
    }

    fn pull_in_by_idx(&self, idx: usize, values: &[u32]) -> Vec<enchudb_oplog::EntityId> {
        if values.is_empty() { return Vec::new(); }
        let mut out: Vec<u32> = Vec::new();
        for &v in values {
            out.extend(self.himos[idx].pull(v));
        }
        out.sort_unstable();
        out.dedup();
        out.into_iter().map(|e| e as enchudb_oplog::EntityId).collect()
    }

    /// Cylinder 結果に delta を適用。
    #[allow(dead_code)]
    fn apply_delta(&self, himo_idx: usize, value: u32, cyl_result: &[u32], delta_eids: &[u32]) -> Vec<u32> {
        let hs = &self.himos[himo_idx];

        // delta の eid を集合にする（重複排除 + 高速lookup）
        let mut dirty: Vec<u32> = delta_eids.to_vec();
        dirty.sort_unstable();
        dirty.dedup();

        // Cylinder 結果から dirty eid を除外（Cylinder の値は古い可能性）
        let mut result: Vec<u32> = if dirty.is_empty() {
            cyl_result.to_vec()
        } else {
            cyl_result.iter()
                .filter(|&&eid| dirty.binary_search(&eid).is_err())
                .copied()
                .collect()
        };

        // dirty eid を Column 直読みで補正
        for &eid in &dirty {
            if hs.get_value(eid) == Some(value) {
                result.push(eid);
            }
        }

        result.sort_unstable();
        result.dedup();
        result
    }

    pub fn query(&self, strings: &[(&str, u32)]) -> Vec<enchudb_oplog::EntityId> {
        self.query_u32(strings).into_iter().map(|e| e as enchudb_oplog::EntityId).collect()
    }

    /// schema 層用: himo_id を pre-resolve 済みの場合の高速 path。 名前 lookup を完全に skip。
    /// 同一 entity の AND 条件として扱う。 himo_id が範囲外なら空 Vec。
    pub fn query_by_id(&self, conds: &[(u16, u32)]) -> Vec<enchudb_oplog::EntityId> {
        if conds.is_empty() { return Vec::new(); }
        let himo_count = self.himos.len();
        let mut idx_conds: Vec<(usize, u32)> = Vec::with_capacity(conds.len());
        for &(hid, val) in conds {
            let idx = hid as usize;
            if idx >= himo_count { return Vec::new(); }
            idx_conds.push((idx, val));
        }
        // 0.8.4 issue #32: 旧 `e as EntityId` は u32 → u64 widen で peer prefix が
        // 0 のまま残り、 schema 層の `where_eq().find_one()` 等が壊れた eid を返してた。
        // `entities_with_himo` と同じ make_eid(peer, e) で peer prefix を付ける。
        let peer = self.peer_id();
        self.query_resolved(&idx_conds)
            .into_iter()
            .map(|e| enchudb_oplog::make_eid(peer, e))
            .collect()
    }

    /// 内部版: u32 eid の Vec を返す。互換性のため残す。
    fn query_u32(&self, strings: &[(&str, u32)]) -> Vec<u32> {
        if strings.is_empty() { return vec![]; }
        // 全条件の himo index と value を解決
        let mut conds: Vec<(usize, u32)> = Vec::with_capacity(strings.len());
        for &(himo, val) in strings {
            match self.himo_id(himo) {
                Some(idx) => conds.push((idx, val)),
                None => return vec![],
            }
        }
        self.query_resolved(&conds)
    }

    /// resolved conds (himo_index, value) に対する query 本体。 strategy 自動選択。
    fn query_resolved(&self, conds: &[(usize, u32)]) -> Vec<u32> {
        // delta が溢れた himo があれば rebuild
        for hs in &self.himos {
            if hs.delta_needs_rebuild() { hs.rebuild_cylinder(); }
        }
        if conds.is_empty() { return vec![]; }

        if conds.len() == 1 {
            let (idx, val) = conds[0];
            return self.himos[idx].pull(val);
        }

        // Column直読みフィルタ。 `delta` は常に空なので Column が最新で OK。
        // 旧版は 「全条件 bitmap なら AND」 fast path を持っていたが、
        // `HimoStore::has_bitmaps()` が常に false で到達不能だったため issue #5
        // で撤去した。 0.5+ 系で bitmap を本気で持つなら、 ここで戦略選択を復活させる。
        self.query_column_filter(&conds)
    }

    /// Column直読みフィルタ（delta 補正付き）
    fn query_column_filter(&self, conds: &[(usize, u32)]) -> Vec<u32> {
        let total = self.entities.next_eid() as usize;
        // 各 cond の slice_len を事前計算 (per-eid 呼ばないように外出し)
        let slice_lens: Vec<usize> = conds.iter()
            .map(|&(idx, val)| self.himos[idx].slice_len(val))
            .collect();

        // pivot: 最小スライスを選ぶ
        let mut best = 0;
        let mut best_len = usize::MAX;
        for (i, &len) in slice_lens.iter().enumerate() {
            if len < best_len { best_len = len; best = i; }
        }

        let (pivot_idx, pivot_val) = conds[best];
        let hs = &self.himos[pivot_idx];

        let candidates = hs.pull(pivot_val);

        // 残りの条件を Column 直読みでフィルタ（Column は常に最新）。
        // 全件相当の cond (e.g. schema layer の table marker) は always-true として skip。
        let mut result = Vec::with_capacity(candidates.len());
        for &eid in &candidates {
            let mut pass = true;
            for (i, &(idx, val)) in conds.iter().enumerate() {
                if i == best { continue; }
                if slice_lens[i] >= total { continue; } // 全件 → 確実に true
                if !self.himos[idx].value_eq(eid, val) { pass = false; break; }
            }
            if pass { result.push(eid); }
        }
        result
    }

    #[allow(dead_code)]
    pub(crate) fn query_count(&self, strings: &[(&str, u32)]) -> usize {
        self.query(strings).len()
    }

    // ──── himo 管理 ────

    /// 紐名 → インデックス。線形探索（紐数は高々数百）。
    #[inline]
    pub fn himo_id(&self, himo: &str) -> Option<usize> {
        self.himo_names.iter().position(|n| n == himo)
    }

    /// 0.9.0: himo_to_table の bounds-checked read。 要素は AtomicU16 なので
    /// load して TableId に戻す (attach 変更は himo_def_lock 下の低頻度 path)。
    #[inline(always)]
    fn himo_table_get(&self, hid: usize) -> Option<TableId> {
        self.himo_to_table
            .get(hid)
            .map(|a| a.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn ensure_himo(&mut self, himo: &str, ht: ValueType, max_values: u32) -> usize {
        // 0.9.0: 本体は `&self` 版に移譲 (signature 互換の thin wrapper)。
        // capacity 超過は旧実装の assert! と同様 panic のまま。
        if let Some(idx) = self.himo_id(himo) { return idx; }
        let _guard = self.himo_def_lock.lock().unwrap();
        if let Some(idx) = self.himo_id(himo) { return idx; }
        match self.define_himo_slot_locked(himo, ht, max_values) {
            Ok(hid) => hid as usize,
            Err(e) => panic!("{}", e),
        }
    }

    /// 0.9.0: `ensure_himo` の `&self` 版。 `Arc<Engine>` (= concurrent mode)
    /// から lazy に himo を定義できる。 idempotent: 同名が既にあれば既存 hid を
    /// 返す。 新規定義は `himo_def_lock` で直列化され、 anonymous table に
    /// attach される (旧 `define_himo` と同じ)。 named table に attach するなら
    /// `ensure_himo_dynamic_in` を使う。
    pub fn ensure_himo_dynamic(
        &self,
        full_name: &str,
        ht: ValueType,
        max_values: u32,
    ) -> Result<u16, String> {
        self.check_writable();
        // fast path: 既存名は lock 無しで解決 (himo_names は append-only なので
        // 見つかった idx は安定)。
        if let Some(idx) = self.himo_id(full_name) {
            return Ok(idx as u16);
        }
        let _guard = self.himo_def_lock.lock().unwrap();
        // double-check: lock 待ちの間に他 thread が同名を定義した可能性
        if let Some(idx) = self.himo_id(full_name) {
            return Ok(idx as u16);
        }
        let hid = self.define_himo_slot_locked(full_name, ht, max_values)?;
        // himo → table attach (anonymous) を sidecar に反映 (best effort)。
        self.try_persist_tables();
        Ok(hid)
    }

    /// himo 定義の本体 (0.9.0 で ensure_himo から `&self` 化)。
    ///
    /// 呼び出し規約: **必ず `himo_def_lock` を保持して呼ぶこと** (直列化は
    /// caller 責務)。 lock 下で himoreg 登録 → column region init → 並列配列
    /// への push → header 書き込みまでを行う。 publish 順は `himo_names` を
    /// 最後にする: `himo_id()` (= 名前の線形検索) で hid が見つかった時点で
    /// 他の並列配列 (himos / value_types / himo_max_values / himo_to_table /
    /// tables[anon].himo_ids) は必ず埋まっている、 という不変条件を成す。
    fn define_himo_slot_locked(
        &self,
        himo: &str,
        ht: ValueType,
        max_values: u32,
    ) -> Result<u16, String> {
        let hid = self.himos.len();
        if hid as u32 >= self.max_himos || hid >= u16::MAX as usize {
            return Err(format!("too many himos (max {})", self.max_himos));
        }

        self.himo_reg.get_or_insert(himo.as_bytes());

        let effective_mv = max_values.min(self.layout.cyl_max_values);
        let col_off = self.layout.himo_col_off(hid);
        // himo_slots 領域は固定 cluster 内 (vocab_data_off の手前) にある。
        // Growable backing で initial_commit が縮んだ場合、 ensure_himo が
        // 呼ばれた時点で this slot がまだ uncommitted な可能性がある。
        // grower 経由の Region を作って各 init で ensure_committed が
        // 効くようにする。 static backing の場合は Region::new 経路。
        #[cfg(not(target_arch = "wasm32"))]
        let grower = self.backing.grower();
        let base = self.backing.base_ptr_shared();
        let make_region = |off: usize, size: usize| -> Region {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(g) = &grower {
                return unsafe { Region::with_grower(g.clone(), off, size) };
            }
            unsafe { Region::new(base.add(off), size) }
        };

        let hs = HimoStore::init(
            make_region(col_off, self.layout.himo_col_size),
            ht, effective_mv, self.max_entities,
        );

        // AppendVec は with_capacity(max_himos) 済みなので上の capacity check が
        // 通れば push は失敗しない (万一の防御で明示 panic)。
        let push_ok = self.himos.push(hs).is_ok()
            && self.value_types.push(ht).is_ok()
            && self.himo_max_values.push(max_values).is_ok()
            && self
                .himo_to_table
                .push(std::sync::atomic::AtomicU16::new(ANONYMOUS_TABLE))
                .is_ok();
        assert!(push_ok, "himo parallel arrays out of capacity (max {})", self.max_himos);

        // β-light step 2: 旧 API (define_himo) で追加された himo は anonymous
        // table に attach する。 named table への attach は
        // ensure_himo_dynamic_in / define_himo_in 側で migrate される。
        let hid_u32 = hid as u32;
        self.tables[ANONYMOUS_TABLE as usize]
            .himo_ids
            .write()
            .unwrap()
            .push(hid_u32);

        // ヘッダにメタデータ書き込み (himo_def_lock 下、 reader は runtime に
        // この領域を読まないので &self からの直接書き込みで安全)
        let maxv_base = himo_maxv_base(self.max_himos);
        let buf = self.backing.slice_mut_shared();
        buf[H_HIMO_TYPES + hid] = ht as u8;
        let mv_off = maxv_base + hid * 4;
        buf[mv_off..mv_off + 4].copy_from_slice(&max_values.to_le_bytes());
        let himo_count = (hid + 1) as u32;
        buf[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&himo_count.to_le_bytes());
        // v28: header CRC を再計算(himo_count が変わったため)
        write_header_crc(buf);

        // v29 issue7 fix: schema 変更で region layout が変わったので、 seal_integrity
        // で焼かれた古い `.crc` sidecar は stale。 削除して、 次 open は CRC 検証
        // skip (v28 以前と同じ fallback)、 次 seal_integrity で regenerate させる。
        #[cfg(not(target_arch = "wasm32"))]
        {
            let crc_path = crate::integrity::crc_path_for(&self.path);
            let _ = std::fs::remove_file(&crc_path);
        }

        // 最後に名前を publish (この時点で hid の全 metadata が可視)
        let _ = self.himo_names.push(himo.to_string());

        Ok(hid as u16)
    }

    // ──── v27 並行書き込み ────

    /// 並行対応版の create。
    ///
    /// writer は `tie_async` で WriteQueue に push(~ns オーダー)、
    /// 裏で単一 consumer スレッドが pop して HimoStore に適用する。
    /// reader は Arc<Engine> を複数スレッドで共有し、`pull_raw`/`query` で
    /// RwLock 越しに安全に読める。
    ///
    /// 戻り値は `Arc<Engine>`。define_himo など `&mut self` メソッドは
    /// `Arc::get_mut` 経由(定義の前に呼ぶ前提)で行う、もしくは本関数の前に
    /// `create_with_capacity` で定義済み状態を作ってから `concurrentize` する
    /// パターンを使う。
    ///
    /// WriteQueue は `SegQueue`(unbounded)。cap 指定は不要。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_concurrent(path: &str) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        Ok(Self::spawn_consumer(eng))
    }

    /// v28: WAL 付き create_concurrent。
    /// `{path}.wal` に Write-Ahead Log を作成。
    ///
    /// tie_async / untie_async / delete_async は hot path で WAL append(memcpy)を行い、
    /// consumer スレッドが背景で 100ms 毎に fsync + checkpoint。
    ///
    /// プロセス/OS クラッシュ時は open_concurrent_with_oplog で最後の Commit まで復元される。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_concurrent_with_oplog(path: &str, oplog_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        let oplog_path = oplog_path_for(path);
        let wal = std::sync::Arc::new(enchudb_oplog::oplog::OpLog::create(&oplog_path, oplog_capacity)?);
        eng.rehydrate_next_sync_lsn(); // #77-H6: recovery 後の rows も含めて復元
        Ok(Self::spawn_consumer_with_oplog(eng, Some(wal)))
    }

    /// `create_concurrent_with_oplog` + `queue_capacity` override (issue4)。
    /// - `queue_capacity`: WriteQueue / oplog_record_queue の bounded cap (default 1 M)
    ///
    /// sustained writer (sunsu Docker scenario 03 等) で writer >> consumer rate に
    /// なると、 旧 unbounded queue では RSS 線形成長 → OOM。 bounded 化 + producer
    /// block でこれを cap する。 capacity の選び方:
    /// - 小さい (例: 10 K) → RSS 低い、 latency 不安定
    /// - 大きい (例: 10 M) → latency 安定、 RSS は queue 内 record サイズ × cap
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_concurrent_with_oplog_queue_cap(
        path: &str,
        oplog_capacity: usize,
        queue_capacity: usize,
    ) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        let oplog_path = oplog_path_for(path);
        let wal = std::sync::Arc::new(enchudb_oplog::oplog::OpLog::create(&oplog_path, oplog_capacity)?);
        Ok(Self::spawn_consumer_with_oplog_queue_cap(eng, Some(wal), Some(queue_capacity)))
    }

    /// v28: WAL 付き open_concurrent。既存 WAL があればリカバリする。
    /// v29: region CRC は WAL ルートでは skip(WAL が source of truth)。
    /// 代わりに古い `.crc` ファイルは削除して、次回 flush で regenerate させる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_concurrent_with_oplog(path: &str, oplog_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let mut eng = Self::open_internal(path, /*verify_region_crc=*/ false, /*take_lock=*/ true, /*readonly=*/ false)?;
        // 古い .crc は WAL 活動後に stale になるので削除
        let crc_path = crate::integrity::crc_path_for(path);
        let _ = std::fs::remove_file(&crc_path);
        let oplog_path = oplog_path_for(path);
        let wal = if oplog_path.exists() {
            let w = enchudb_oplog::oplog::OpLog::open(&oplog_path)?;
            // リカバリ: commit されたレコードを本体に適用
            let records = w.recover();
            for rec in &records {
                eng.apply_oplog_op(&rec.op);
            }
            // #77-H2: 適用効果を disk に固めてから checkpoint を前進する。
            // 旧順序 (apply → 即 checkpoint) は kernel が checkpoint header を
            // body より先に writeback すると、 recovery 直後の再 crash で
            // 「一度 durable だった committed record」 が replay 対象外になり
            // 恒久消失した。
            let _ = eng.body_msync();
            let _ = w.fsync();
            w.advance_checkpoint(w.head());
            std::sync::Arc::new(w)
        } else {
            std::sync::Arc::new(enchudb_oplog::oplog::OpLog::create(&oplog_path, oplog_capacity)?)
        };
        eng.rehydrate_next_sync_lsn(); // #77-H6: recovery 後の rows も含めて復元
        Ok(Self::spawn_consumer_with_oplog(eng, Some(wal)))
    }

    /// WAL の 1 op を本体に適用(recover 専用)。
    /// v32: eid は u64 だが Column は local u32 で保持。eid_local() で剥がす。
    ///
    /// 0.8.1: Tie/Content で `entities.ensure_live` + table `next_local` の
    /// max 推進を入れた。 これが無いと short-lived CLI (= sidecar persist
    /// 機会無く drop → 次 open で oplog recover) のとき:
    ///   - entity_set の live bitmap が stale → 次 entity_in が eid 重複払出し
    ///   - table.next_local が 0 のまま → 次 alloc が既存 eid と衝突
    /// になる。 sinfo 連携で表面化したので 0.8.1 patch で根治。
    fn apply_oplog_op(&mut self, op: &enchudb_oplog::oplog::DecodedOp) {
        use enchudb_oplog::oplog::DecodedOp;
        match op {
            DecodedOp::Tie { eid, himo_id, value } => {
                let hid = *himo_id as usize;
                let local = enchudb_oplog::eid_local(*eid);
                if hid < self.himos.len() {
                    self.himos[hid].set(local, *value);
                }
                self.entities.ensure_live(local);
                Self::advance_table_next_local_for(&self.tables, local);
            }
            DecodedOp::Untie { eid, himo_id } => {
                let hid = *himo_id as usize;
                let local = enchudb_oplog::eid_local(*eid);
                if hid < self.himos.len() {
                    self.himos[hid].remove(local);
                }
            }
            DecodedOp::Delete { eid } => {
                let local = enchudb_oplog::eid_local(*eid);
                for hid in 0..self.himos.len() {
                    self.himos[hid].remove(local);
                }
                self.entities.free(local);
            }
            DecodedOp::Content { eid, key, data } => {
                // legacy (pre-0.9 WAL): 旧 content region へ replay。 0.9.0 以降は
                // Op::Content を emit しないので、 旧 DB の WAL 再生でのみ通る。
                let local = enchudb_oplog::eid_local(*eid);
                self.contents.set(local, key, data);
                self.entities.ensure_live(local);
                Self::advance_table_next_local_for(&self.tables, local);
            }
            DecodedOp::TieNamed { eid, himo_name, himo_kind, value } => {
                // 0.9.0: 名前で himo を解決 (無ければ定義) して set。
                let local = enchudb_oplog::eid_local(*eid);
                let ht = ValueType::from_byte(*himo_kind);
                if let Ok(hid) = self.ensure_himo_by_full_name(himo_name, ht) {
                    self.himos[hid as usize].set(local, *value);
                }
                self.entities.ensure_live(local);
                Self::advance_table_next_local_for(&self.tables, local);
            }
            DecodedOp::TieLeaf { .. } => {
                // 0.12.0 (#88): self-authored TieLeaf の recover は no-op。
                // Leaf payload は LeafStore、 cell offset は himo 列、 どちらも mmap
                // body として durable なので「既に local に在る」(Vocab と同思想)。
                // 再 insert すると offset が変わり slot が二重化するため触らない。
                // remote peer からの TieLeaf は sync crate の apply-one 経由で別 apply。
            }
            DecodedOp::Commit => {}
            DecodedOp::Vocab { .. } => {
                // v33: 自プロセスの recover 時は Vocab 個別の apply 不要
                // (author_peer == self の場合は既に local vocab にある)。
                // Sync 経由で他 peer から受信する場合のみ apply_one 側で処理。
            }
        }
    }

    /// recover 中、 与えられた global eid を含む table の next_local を
    /// `(eid - lo) + 1` まで進める (= 次 alloc が衝突しないように)。
    /// global が未登録 table の range なら no-op (= reserved table 等で
    /// recover 順序的に table 定義が先に存在することを期待)。
    fn advance_table_next_local_for(tables: &[TableDef], global: u32) {
        use std::sync::atomic::Ordering;
        for table in tables {
            if global >= table.eid_range_lo && global < table.eid_range_hi {
                let target = global - table.eid_range_lo + 1;
                let mut cur = table.next_local.load(Ordering::Relaxed);
                while cur < target {
                    match table.next_local.compare_exchange_weak(
                        cur,
                        target,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(v) => cur = v,
                    }
                }
                return;
            }
        }
    }

    /// 既存の `Engine`(create や create_with_capacity で作成済み、define_himo も
    /// 済ませたもの)を Arc 化して consumer スレッドを起動する。
    ///
    /// 返り値の `Arc<Engine>` を各 writer/reader スレッドで clone して使う。
    /// Drop(最後の Arc が落ちた時)で consumer スレッドは join される。
    pub fn concurrentize(eng: Self) -> std::sync::Arc<Self> {
        Self::spawn_consumer(eng)
    }

    /// `concurrentize` + WAL 後付け版。 `define_himo` などの build phase を `Engine`
    /// 値所有で終えた後、 既存 schema 状態を保ったまま consumer + WAL を起動して
    /// `Arc<Engine>` に遷移する。 sinfo / enchudb-schema の build → runtime 移行に使う。
    ///
    /// 既存 `.wal` ファイルがあれば recover してから consumer 起動。
    /// (build phase で flush 済みなら本体は最新、 WAL は空のまま start)
    #[cfg(not(target_arch = "wasm32"))]
    pub fn concurrentize_with_oplog(mut eng: Self, oplog_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let path = eng.path.clone();
        let oplog_path = oplog_path_for(&path);
        // 古い .crc は WAL 活動後に stale になるので削除 (open_concurrent_with_oplog と同じ扱い)
        let crc_path = crate::integrity::crc_path_for(&path);
        let _ = std::fs::remove_file(&crc_path);
        let wal = if oplog_path.exists() {
            let w = enchudb_oplog::oplog::OpLog::open(&oplog_path)?;
            let records = w.recover();
            for rec in &records {
                eng.apply_oplog_op(&rec.op);
            }
            // #77-H2: body msync → checkpoint の順 (open_concurrent_with_oplog と同じ)
            let _ = eng.body_msync();
            let _ = w.fsync();
            w.advance_checkpoint(w.head());
            std::sync::Arc::new(w)
        } else {
            std::sync::Arc::new(enchudb_oplog::oplog::OpLog::create(&oplog_path, oplog_capacity)?)
        };
        eng.rehydrate_next_sync_lsn(); // #77-H6: recovery 後の rows も含めて復元
        Ok(Self::spawn_consumer_with_oplog(eng, Some(wal)))
    }

    fn spawn_consumer(eng: Self) -> std::sync::Arc<Self> {
        Self::spawn_consumer_with_oplog(eng, None)
    }

    /// changefeed 内部ヘルパ: WAL から emit_offset 以降の record を取り出して
    /// 全 listener に渡し、cursor を進める。
    fn fire_change_listeners(
        wal: &std::sync::Arc<enchudb_oplog::oplog::OpLog>,
        listeners: &std::sync::RwLock<
            Vec<std::sync::Arc<dyn crate::changefeed::ChangeListener>>,
        >,
        emit_offset: &std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        use std::sync::atomic::Ordering;
        // listener 登録ゼロなら何もしない(cursor は進めない:listener 加入時に
        // wal.head() に同期する設計なので、ここで進めると先行 record が漏れる)
        let guard = listeners.read().unwrap();
        if guard.is_empty() {
            return;
        }
        let start = emit_offset.load(Ordering::Acquire);
        // #77-H4: cursor は commit 済み終端まで。 head 再読は in-flight record の
        // 恒久 skip (listener への通知漏れ) を起こしていた。
        let (recs, committed_end) = wal.iter_committed_from_with_end(start);
        if recs.is_empty() {
            emit_offset.store(committed_end, Ordering::Release);
            return;
        }
        let wires: Vec<crate::transport::WireRecord> =
            recs.into_iter().map(|r| r.into()).collect();
        for listener in guard.iter() {
            listener.on_changes(&wires);
        }
        emit_offset.store(committed_end, Ordering::Release);
    }

    fn spawn_consumer_with_oplog(
        eng: Self,
        oplog: Option<std::sync::Arc<enchudb_oplog::oplog::OpLog>>,
    ) -> std::sync::Arc<Self> {
        Self::spawn_consumer_with_oplog_queue_cap(eng, oplog, None)
    }

    /// `spawn_consumer_with_oplog` + queue capacity 上書き。 None = default。
    /// issue4 backpressure 用 — capacity 大きいほど push 側 latency 安定、
    /// 小さいほど RSS 安定 (writer rate >> consumer rate の差を queue で吸収しない)。
    fn spawn_consumer_with_oplog_queue_cap(
        mut eng: Self,
        oplog: Option<std::sync::Arc<enchudb_oplog::oplog::OpLog>>,
        queue_cap: Option<usize>,
    ) -> std::sync::Arc<Self> {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use crate::write_queue::WriteQueue;

        let qc = queue_cap.unwrap_or(crate::write_queue::DEFAULT_WRITE_QUEUE_CAP);
        let queue = Arc::new(WriteQueue::with_capacity(qc));
        let shutdown = Arc::new(AtomicBool::new(false));
        // WAL 有効時のみ oplog_record_queue を生やす。 writer は直接 wal.append せず
        // ここに owned record を push する → consumer thread が drain して append_many。
        // issue4: bounded `ArrayQueue` で sustained writer の RSS を cap する。
        let oplog_record_queue = oplog.as_ref()
            .map(|_| Arc::new(crossbeam_queue::ArrayQueue::<enchudb_oplog::oplog::OwnedOp>::new(qc)));

        eng.write_queue = Some(queue.clone());
        eng.shutdown_flag = Some(shutdown.clone());
        eng.oplog = oplog.clone();
        eng.oplog_record_queue = oplog_record_queue.clone();

        let arc = Arc::new(eng);

        let engine_ptr: *const Engine = Arc::as_ptr(&arc);
        let engine_addr = engine_ptr as usize;
        let q_for_thread = queue.clone();
        let flag_for_thread = shutdown.clone();
        let apply_count_for_thread = arc.apply_count.clone();
        let wal_append_count_for_thread = arc.wal_append_count.clone();
        let oplog_for_thread = oplog.clone();
        let oplog_record_queue_for_thread = oplog_record_queue.clone();
        let durable_lsn_for_thread = arc.durable_lsn.clone();
        let listeners_for_thread = arc.change_listeners.clone();
        let emit_offset_for_thread = arc.change_emit_offset.clone();
        let poisoned_for_thread = arc.consumer_poisoned.clone();

        // #77-M2: consumer が panic した場合に unwind 経路で poison を立てる
        // guard。 これが無いと apply_count が永久に追いつかず、 flush_writes /
        // Drop の barrier spin と満杯 queue の producer が全員無限待ちになる
        // (旧 behavior: content cap 超過等の panic でプロセス全体が silent hang)。
        struct PoisonOnPanic {
            flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
            queue: std::sync::Arc<crate::write_queue::WriteQueue>,
        }
        impl Drop for PoisonOnPanic {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    self.flag.store(true, std::sync::atomic::Ordering::Release);
                    self.queue.poison();
                    eprintln!("[enchudb] consumer thread panicked — engine poisoned, subsequent writes/flushes will fail fast");
                }
            }
        }

        let handle = std::thread::Builder::new()
            .name("enchudb-consumer".into())
            .spawn(move || {
                use std::sync::atomic::Ordering;
                use std::time::{Duration, Instant};
                let _poison_guard = PoisonOnPanic {
                    flag: poisoned_for_thread,
                    queue: q_for_thread.clone(),
                };
                let engine: &Engine = unsafe { &*(engine_addr as *const Engine) };
                let fsync_interval = Duration::from_millis(100);
                let mut last_fsync = Instant::now();

                loop {
                    let mut drained_any = false;
                    // WAL record queue を batch drain (per-record flock を償却)
                    if let (Some(wq), Some(wal)) = (
                        oplog_record_queue_for_thread.as_ref(),
                        oplog_for_thread.as_ref(),
                    ) {
                        let mut batch: Vec<enchudb_oplog::oplog::OwnedOp> = Vec::new();
                        while let Some(rec) = wq.pop() { batch.push(rec); }
                        if !batch.is_empty() {
                            let _ = wal.append_many(&batch);
                            // append 失敗でも進める: barrier の意味は「queue に
                            // 残っていない」であり、 失敗 record の再送は無い
                            wal_append_count_for_thread
                                .fetch_add(batch.len() as u64, Ordering::Release);
                            drained_any = true;
                        }
                    }
                    while let Some(op) = q_for_thread.pop() {
                        drained_any = true;
                        engine.apply_op(op);
                        apply_count_for_thread.fetch_add(1, Ordering::Release);
                    }

                    // 背景 fsync: WAL 有効 & 前回から fsync_interval 経過 &
                    // head が checkpoint より進んでいる時のみ実行。
                    // 順序厳守: auto-Commit → WAL fsync → body msync → checkpoint 前進。
                    if let Some(wal) = oplog_for_thread.as_ref() {
                        if last_fsync.elapsed() >= fsync_interval {
                            if wal.head() > wal.checkpoint() {
                                let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
                                // #77-H3: checkpoint の上限は「今回の fsync/msync に
                                // 含まれることが確定した位置」= Commit append 直後の
                                // head。 msync 後に head を再読すると、 その間に
                                // append された record が「body 効果が msync に
                                // 含まれないのに checkpoint される」= crash で
                                // fsync 済み write が replay 対象外になり消失した。
                                let durable_head = wal.head();
                                let durable_lsn = wal.next_lsn().saturating_sub(1);
                                let _ = wal.fsync();
                                let _ = engine.body_msync();
                                // 0.8.1: 周期 fsync でも tables sidecar を persist。
                                // checkpoint を前進させる前に必ず sidecar を固める
                                // (= 直後に process kill されても次 open で sidecar
                                // の next_local が oplog の進行と整合する)。
                                engine.try_persist_tables();
                                wal.advance_checkpoint(durable_head);
                                durable_lsn_for_thread.store(durable_lsn, Ordering::Release);

                                // 0.8.0: sync 並走の解消 — durable 化した record を
                                // _sync_ops に自動転送する (= 0.7.0 では user が手動で
                                // transfer_oplog_to_sync_ops() を呼ぶ必要があったが、
                                // 0.8.0 で primary 切替につき自動化)。
                                // enable_sync 未呼出なら no-op、 副作用ゼロ。
                                if engine.sync_tables_enabled() {
                                    engine.transfer_oplog_to_sync_ops();
                                }

                                // changefeed: durable 化した record を listener に push
                                Self::fire_change_listeners(
                                    wal,
                                    &listeners_for_thread,
                                    &emit_offset_for_thread,
                                );
                            }
                            // v30: ring buffer reset を試みる。head == checkpoint &&
                            // pending_writes == 0 のときだけ head/checkpoint を HEADER_SIZE に戻す。
                            // これで WAL 容量を食い切らずに長期運用できる。
                            // ※ auto_reset が発動して offset が後退したら listener cursor もリセット。
                            // 0.9.0: 畳む前に bridge を必ず追いつかせる。 上の transfer は
                            // head > checkpoint の時しか走らないが、 caller thread の
                            // oplog_sync が checkpoint を進めた直後は head == checkpoint の
                            // まま bridge 未了 record が残りえる (cursor 追いつき済みなら
                            // 空 scan で即返るので毎 tick 呼んで無害)。
                            if engine.sync_tables_enabled() {
                                engine.transfer_oplog_to_sync_ops();
                            }
                            if wal.try_reset() {
                                emit_offset_for_thread.store(
                                    enchudb_oplog::oplog::HEADER_SIZE as u64,
                                    Ordering::Release,
                                );
                                // #63 regression fix: bridge cursor も巻き戻す。
                                // これが無いと reset 後の record が sync 欠落する。
                                engine.reset_sync_ops_offset();
                            }
                            last_fsync = Instant::now();
                        }
                    }

                    if flag_for_thread.load(Ordering::Acquire) {
                        // 最終 WAL drain
                        if let (Some(wq), Some(wal)) = (
                            oplog_record_queue_for_thread.as_ref(),
                            oplog_for_thread.as_ref(),
                        ) {
                            let mut batch: Vec<enchudb_oplog::oplog::OwnedOp> = Vec::new();
                            while let Some(rec) = wq.pop() { batch.push(rec); }
                            if !batch.is_empty() {
                                let _ = wal.append_many(&batch);
                                wal_append_count_for_thread
                                    .fetch_add(batch.len() as u64, Ordering::Release);
                            }
                        }
                        while let Some(op) = q_for_thread.pop() {
                            engine.apply_op(op);
                            apply_count_for_thread.fetch_add(1, Ordering::Release);
                        }
                        // shutdown 時の最終 Commit + 順序付き同期
                        if let Some(wal) = oplog_for_thread.as_ref() {
                            let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
                            let durable_head = wal.head(); // #77-H3: msync 前に snapshot
                            let _ = wal.fsync();
                            let _ = engine.body_msync();
                            // 0.8.1: shutdown 時に tables sidecar を強制 persist。
                            // 旧 behavior では body_msync のみで `next_local` が
                            // sidecar に書かれず、 short-lived CLI (sinfo の sf 等)
                            // で次 open 時に eid 衝突が出ていた。 graceful shutdown
                            // 経路では oplog checkpoint も進めてしまうので、 ここで
                            // sidecar を確実に固める必要がある。
                            engine.try_persist_tables();
                            wal.advance_checkpoint(durable_head);
                            // 0.8.0: shutdown 時も最終 sync 転送 (drop で残った record も
                            // _sync_ops に bridge し切ってから抜ける)
                            if engine.sync_tables_enabled() {
                                engine.transfer_oplog_to_sync_ops();
                            }
                            // shutdown 時の最終 emit
                            Self::fire_change_listeners(
                                wal,
                                &listeners_for_thread,
                                &emit_offset_for_thread,
                            );
                            // graceful close でも ring を畳む: head==checkpoint なら
                            // HEADER_SIZE へ巻き戻す。 100ms fsync tick を踏まない
                            // short-lived writer (events.ecdb の log_event 等) でも
                            // 次 open が full のまま始まらないようにするため。
                            if wal.try_reset() {
                                emit_offset_for_thread.store(
                                    enchudb_oplog::oplog::HEADER_SIZE as u64,
                                    std::sync::atomic::Ordering::Release,
                                );
                                // #63 regression fix: bridge cursor も巻き戻す。
                                engine.reset_sync_ops_offset();
                            }
                        }
                        return;
                    }

                    if !drained_any {
                        // yield_now alone is effectively a busy-spin on multi-core
                        // (OS reschedules immediately). Sleep briefly so the consumer
                        // doesn't peg a core when the queue is idle. fsync runs every
                        // 100ms so 1ms tick is plenty fine-grained.
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            })
            .expect("failed to spawn consumer thread");

        *arc.consumer_handle.lock().unwrap() = Some(handle);
        arc
    }

    /// body mmap の msync(WAL 順序と絡むので &self で呼び出し可能)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn body_msync(&self) -> io::Result<()> {
        match &self.backing {
            Backing::Mmap(m) => m.flush(),
            // request3: dirty range tracking。 旧実装は flush(0, committed) で
            // 全 committed 範囲を msync していたが、 sustained workload で
            // committed が伸びるたびに線形に遅くなる (10K user × 100KB で
            // body_msync 6 ms → 3.6 s)。 hot write 経路から `mark_dirty` で
            // 記録された range だけを msync。
            //
            // issue6: writer hot path (`EntitySet::allocate` 等) は `mark_dirty`
            // 経由の atomic union が cache line contention を引き起こすため、
            // 撤廃して entity_set 領域は常時 msync する (= 小サイズ固定領域なので
            // 無条件 msync しても安価)。
            Backing::Growable(g) => {
                // 1. consumer-tracked dirty range (Column::set 等)
                g.flush_dirty()?;
                // 2. entity_set 領域は writer hot path で書かれるので無条件 msync
                //    (small fixed region、 page-aligned で expand)
                g.flush_aligned(self.layout.entities_off, self.layout.entities_size)?;
                Ok(())
            }
            Backing::Memory(_) => Ok(()),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn body_msync(&self) -> io::Result<()> { Ok(()) }

    /// 強制同期: Commit marker 挿入 → WAL fsync → body msync → checkpoint 前進。
    /// Sync mode 相当の待ち。
    pub fn oplog_sync(&self) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        self.flush_writes();
        if let Some(wal) = self.oplog.as_ref() {
            let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
            // #77-H3: checkpoint 上限と durable_lsn は Commit append 直後に
            // snapshot (msync 後の再読は未同期 record まで checkpoint してしまう)
            let durable_head = wal.head();
            let durable_lsn = wal.next_lsn().saturating_sub(1);
            wal.fsync()?;
            self.body_msync()?;
            wal.advance_checkpoint(durable_head);
            self.durable_lsn.store(durable_lsn, Ordering::Release);
            // 0.9.0: checkpoint を進めたら bridge も追いつかせる。 consumer tick の
            // try_reset は head==checkpoint の ring を無条件に畳むため、 ここで
            // transfer しないと「committed だが bridge 未了」の record が wipe され
            // sync から永久に消える (sync lib テスト flaky の第 2 の根)。
            if self.sync_tables_enabled() {
                self.transfer_oplog_to_sync_ops();
            }
            // changefeed: durable 化したので listener へ即時 push
            // (consumer の 100ms tick を待たず caller スレッドで発火)
            Self::fire_change_listeners(wal, &self.change_listeners, &self.change_emit_offset);
        }
        Ok(())
    }

    /// 現在耐久化されている LSN(Commit を含む最後の WAL fsync 到達位置)。
    pub fn durable_lsn(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.durable_lsn.load(Ordering::Acquire)
    }

    /// v29: エンジン状態の一覧。監視・デバッグ用。
    // ──── Phase F: 監査 ────

    /// WAL 監査: commit 済みレコードを filter して返す。
    ///
    /// WAL が有効でない(`create_concurrent_with_oplog` 未使用等)なら空 Vec。
    /// `AuditFilter::default()` = 全件。
    ///
    /// 返る各 `Record` には lsn, hlc, author_peer, op, signature, pubkey_fp が入り、
    /// 「誰がいつ何を書いたか」をそのまま監査できる。
    pub fn audit(&self, filter: &AuditFilter) -> Vec<enchudb_oplog::oplog::Record> {
        let recs = match self.oplog.as_ref() {
            Some(w) => w.iter_committed(),
            None => return Vec::new(),
        };
        recs.into_iter()
            .filter(|r| {
                if let Some(ref from) = filter.from_hlc {
                    if &r.hlc < from {
                        return false;
                    }
                }
                if let Some(ref to) = filter.to_hlc {
                    if &r.hlc > to {
                        return false;
                    }
                }
                if let Some(author) = filter.author_peer {
                    if r.author_peer != author {
                        return false;
                    }
                }
                if let Some(fp) = filter.pubkey_fp {
                    if r.pubkey_fp != fp {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    // ──── Phase E: snapshot export ────

    /// 現在の DB を target パスへ snapshot としてコピーする。
    ///
    /// コピーする:
    /// - main DB(`self.path` → `target`)
    /// - WAL(`self.path.wal` → `target.wal`)(存在時のみ)
    /// - region CRC sidecar(`self.path.crc` → `target.crc`)(存在時のみ)
    ///
    /// **呼び出し前に必ず `oplog_sync()` or `flush()` で durable 化すること**。
    /// mmap 非同期ページが残っていると snapshot に反映されない。
    ///
    /// 返り値: コピーしたパス一覧。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn snapshot_export(&self, target: &str) -> io::Result<SnapshotFiles> {
        // mmap ページを物理ディスクに確実に書き出す
        self.body_msync()?;

        let mut files = SnapshotFiles {
            main: target.to_string(),
            oplog: None,
            crc: None,
        };
        std::fs::copy(&self.path, target)?;

        let oplog_src = format!("{}.oplog", self.path);
        if std::path::Path::new(&oplog_src).exists() {
            let oplog_dst = format!("{}.oplog", target);
            std::fs::copy(&oplog_src, &oplog_dst)?;
            files.oplog = Some(oplog_dst);
        }

        let crc_src = format!("{}.crc", self.path);
        if std::path::Path::new(&crc_src).exists() {
            let crc_dst = format!("{}.crc", target);
            std::fs::copy(&crc_src, &crc_dst)?;
            files.crc = Some(crc_dst);
        }

        // #9 (H2): sidecar (.tables/.eidmap) は source を copy せず、 現在の in-memory
        // 状態を直接 target へ書き出す。 source copy だと consumer tick (≤100ms 周期)
        // 時点の stale sidecar を新しい body に対してコピーしてしまい、 翻訳 entity が
        // .eidmap に載らず restore 後の再 sync で重複する。 直接書き出しなら msync 済 body
        // と必ず整合し、 かつ source を fsync 再 persist しないので snapshot が遅くならない
        // (durability は copy と同じ page-cache level、 backup の fsync は呼び出し側責務)。
        // 0.7.0: .tables には reserved table `_sync_ops` / `_sync_peers` も含む。
        if !self.tables.is_empty() {
            std::fs::write(format!("{}.tables", target), serialize_tables(&self.tables))?;
        }
        let eidmap_entries = self.eidmap_entries_with_tombstones();
        if !eidmap_entries.is_empty() {
            std::fs::write(format!("{}.eidmap", target), serialize_eidmap(&eidmap_entries))?;
        }

        Ok(files)
    }

    pub fn stats(&self) -> EngineStats {
        use std::sync::atomic::Ordering;
        let (oplog_head, oplog_checkpoint, oplog_capacity, oplog_lsn) = match self.oplog.as_ref() {
            Some(w) => (w.head(), w.checkpoint(), w.usage().1, w.next_lsn().saturating_sub(1)),
            None => (0, 0, 0, 0),
        };
        let (peer_id, hlc_entries, max_hlc) = (
            self.peer_id.load(Ordering::Acquire),
            self.hlc_store.len(),
            self.hlc_store.max_hlc(),
        );

        EngineStats {
            entity_count: self.entities.count(),
            himo_count: self.value_types.len() as u32,
            oplog_head,
            oplog_checkpoint,
            oplog_capacity,
            oplog_lag_bytes: oplog_head.saturating_sub(oplog_checkpoint),
            oplog_next_lsn: oplog_lsn,
            durable_lsn: self.durable_lsn.load(Ordering::Acquire),
            queue_len: self.write_queue.as_ref().map(|q| q.len()).unwrap_or(0),
            pushed: self.push_count.load(Ordering::Acquire),
            applied: self.apply_count.load(Ordering::Acquire),
            peer_id,
            hlc_entries,
            max_hlc,
        }
    }

    /// 0.8.16 (issue #54): vocab の orphan (= 死蔵 vid) を read-only scan。
    /// `ValueType::Leaf` の `vocab.insert` 経路は re-tie / remove で旧 vid を
    /// 回収しないため、 long-lived な curated store (= 元ソースから rebuild
    /// しないタイプ) で vocab data が単調増加する。 この API でその実量を計測。
    ///
    /// 計測手順:
    /// 1. 全 himo (Tag / Leaf) の `unique_values()` を union して live vid 集合を作る
    /// 2. `(0..vocab.count())` のうち live 集合に無いものを orphan と判定
    /// 3. orphan vid の `vocab.get(vid).len()` を合計して救出可能 bytes を出す
    ///
    /// O(vocab.count() + Σ himos.unique_values().len())。 vid set は `Vec<bool>` で
    /// vocab.count() bit。 vocab.count() = 1B なら 1GB 食うので注意。 巨大 DB なら
    /// 別 issue で BitVec か stream 化を検討。
    /// v6 (#88): LeafStore の現 footprint (high_water)。 pre-v6 DB (leaf region
    /// 無し) は None。 routing 前 (2.1 scaffolding) は常に HEADER 相当。
    pub fn leaf_footprint(&self) -> Option<u64> {
        self.leaf.as_ref().map(|l| l.high_water())
    }

    /// vocab data 領域の消費 byte 数 (単調・回収なし)。 #88 bench で
    /// 「Leaf を vocab に載せる旧挙動」の footprint 増加を計測する用。
    pub fn vocab_data_footprint(&self) -> u32 {
        self.vocab.data_footprint()
    }

    pub fn vocab_orphan_stats(&self) -> VocabOrphanStats {
        let vocab_total = self.vocab.count();
        if vocab_total == 0 {
            return VocabOrphanStats {
                vocab_total: 0,
                live_vids: 0,
                orphan_vids: 0,
                orphan_bytes: 0,
                live_bytes: 0,
            };
        }
        // live vid 集合 — Tag / Leaf 両方を対象。 Number / Ref は vocab を使わない。
        // 各 himo の unique_values は局所 stored 値 (= cylinder side で管理) を返す。
        // stored は内部表現値 +1 ではなく素の vid なので decode 不要。
        let mut is_live = vec![false; vocab_total as usize];
        for hid in 0..self.value_types.len() {
            // v6 (#88): routed-Leaf の cell は vocab vid でなく LeafStore offset なので
            // vocab の live 判定から除外する (含めると無関係な vid を live 誤判定)。
            if self.leaf_for(hid).is_some() { continue; }
            match self.value_types[hid] {
                ValueType::Tag | ValueType::Leaf => {
                    let vids = self.himos[hid].unique_values();
                    for v in vids {
                        if (v as usize) < is_live.len() {
                            is_live[v as usize] = true;
                        }
                    }
                }
                _ => {}
            }
        }
        let mut live_vids: u32 = 0;
        let mut orphan_vids: u32 = 0;
        let mut live_bytes: u64 = 0;
        let mut orphan_bytes: u64 = 0;
        for vid in 0..vocab_total {
            let len = self.vocab.get(vid).len() as u64;
            if is_live[vid as usize] {
                live_vids += 1;
                live_bytes += len;
            } else {
                orphan_vids += 1;
                orphan_bytes += len;
            }
        }
        VocabOrphanStats {
            vocab_total,
            live_vids,
            orphan_vids,
            orphan_bytes,
            live_bytes,
        }
    }

    /// consumer スレッド内部で呼ぶ。Op 1 個を適用。
    fn apply_op(&self, op: crate::write_queue::Op) {
        use crate::write_queue::Op;
        match op {
            Op::Tie { eid, himo_id, value } => {
                let hid = himo_id as usize;
                if hid >= self.himos.len() { return; }
                // v6 (#88): routed-Leaf の re-tie 上書きは旧 offset を free (async path)。
                self.free_leaf_cell(eid, hid);
                self.himos[hid].set(eid, value);
            }
            Op::Untie { eid, himo_id } => {
                let hid = himo_id as usize;
                if hid >= self.himos.len() { return; }
                self.free_leaf_cell(eid, hid);
                self.himos[hid].remove(eid);
            }
            Op::Delete { eid } => {
                for hid in 0..self.himos.len() {
                    self.free_leaf_cell(eid, hid);
                    self.himos[hid].remove(eid);
                }
                self.entities.free(eid);
            }
            Op::EntityCreated { local: _ } => {
                // v4 (undo 廃止) 以降は no-op。 `entity()` で local slot は writer
                // thread 側で既に allocate 済み。 ここに来るのは `flush_writes` の
                // push_count / apply_count counter を対称に進めるためだけ (issue5)。
            }
        }
    }

    /// 非同期 tie。`create_concurrent`/`concurrentize` で有効化済みの場合のみ使える。
    /// 紐名は事前に `define_himo` で定義されている必要がある。
    /// WriteQueue は SegQueue(unbounded)なので push は必ず成功する。
    ///
    /// WAL が有効な場合: tie_async は WAL append (memcpy) → WriteQueue push の順で実行する。
    /// WAL append は `.wal` ファイルに memcpy 1 回、100ns オーダー。hot path で fsync しない。
    pub fn tie_async(&self, eid: enchudb_oplog::EntityId, himo: &str, value: u32) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_async_by_id(eid, hid, value);
    }

    /// `tie_async` の himo_id 直指定版。 SNS の post / like 投入のように row/sec が KO
    /// 単位の hot path 用 (per-call の `himo_id(&str)` HashMap lookup を消す)。
    /// 起動時に `himo_id(&str)` で u16 を 1 回引いて cache し、 hot loop で繰り返し使う想定。
    pub fn tie_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: u32) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か (anonymous
        // open-ended なら即 return)
        self.validate_eid_for_himo(himo_id as usize, local);
        // β-light step 5: Ref himo の FK validation (非 Ref は即 return で
        // ~1 ns、 Ref で fk_refs entry なしも同じ)
        self.validate_ref_tie(himo_id as usize, value);
        // #77-H4: op を write_queue へ push してから WAL record を push する。
        // 逆順 (record 先) だと 2 push の間で preempt された場合、 consumer が
        // record を fsync + checkpoint した時点で op が未適用となり、 crash で
        // 「fsync 済みの write」が replay 対象外になって消えた。 op 先行なら
        // consumer が wq drain で record を見た時点で op は必ず queue に居て、
        // 同 tick の q drain (wq の後) で適用される。
        let q = self.write_queue.as_ref()
            .expect("tie_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie { eid: local, himo_id, value });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let rec = enchudb_oplog::oplog::OwnedOp::Tie { eid: oplog_eid, himo_id, value };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                push_oplog_record_blocking(wq, rec, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
    }

    /// v33: 非同期 tie_text。text 値を vocab に挿入し、WAL に Vocab + Tie の 2 op を流す。
    ///
    /// 流れ:
    /// 1. local vocab に bytes を get_or_insert → `local_vid`
    /// 2. WAL に `Vocab { vid: local_vid, bytes }` を append
    ///    - receiver 側でこれを受けると `(author_peer, vid) → receiver_local_vid` の mapping が張られる
    /// 3. WAL に `Tie { eid, himo_id, value: local_vid }` を append
    /// 4. write_queue に Tie push で本体に apply
    ///
    /// `Engine::tie_text` の &self 版。`tie_async` と異なり text を sync 対象に乗せる。
    pub fn tie_text_async(&self, eid: enchudb_oplog::EntityId, himo: &str, value: &str) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_text_async_by_id(eid, hid, value);
    }

    /// `tie_text_async` の himo_id 直指定版。 text 本体は vocab 経由なので value はそのまま。
    pub fn tie_text_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: &str) {
        self.tie_bytes_async_by_id(eid, himo_id, value.as_bytes());
    }

    /// `tie_text_async_by_id` の bytes 版 (0.9.0: content 互換層も使う)。
    pub fn tie_bytes_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: &[u8]) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, local);
        // v6 (#88): Leaf は LeafStore へ。 offset を write_queue の value に載せ、
        // WAL には bytes 同乗 TieLeaf を流す。 旧 offset の free は apply_op::Tie 側
        // (= 適用時点の cell を見る。 push 時 free は queue 未適用の二重 free を招く)。
        if let Some(leaf) = self.leaf_for(hid) {
            let off = leaf.insert(value);
            let q = self.write_queue.as_ref()
                .expect("tie_bytes_async requires create_concurrent or concurrentize");
            q.push(crate::write_queue::Op::Tie { eid: local, himo_id, value: off });
            self.push_count.fetch_add(1, Ordering::Release);
            if let Some(wal) = self.oplog.as_ref() {
                let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
                let rec = enchudb_oplog::oplog::OwnedOp::TieLeaf {
                    eid: oplog_eid,
                    himo_name: self.himo_names[hid].clone(),
                    himo_kind: self.value_types[hid] as u8,
                    bytes: value.to_vec(),
                };
                if let Some(wq) = self.oplog_record_queue.as_ref() {
                    push_oplog_record_blocking(wq, rec, &self.consumer_poisoned, &self.wal_push_count);
                } else {
                    let _ = wal.append(rec.as_op());
                }
            }
            return;
        }
        // Tag は dedupe、Leaf は常に新規 id。
        let vid = match self.value_types[hid] {
            ValueType::Tag => self.vocab.get_or_insert(value),
            ValueType::Leaf => self.vocab.insert(value),
            ht => panic!("tie_bytes_async_by_id on non-text himo_id {}: {:?}", himo_id, ht),
        };
        assert!(vid < u32::MAX, "vocab vid must be < u32::MAX (sentinel reserved)");
        // #77-H4: op 先行 push (tie_async_by_id と同じ理由)
        let q = self.write_queue.as_ref()
            .expect("tie_text_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie { eid: local, himo_id, value: vid });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            // Vocab op を先に(sync の receiver 側で Tie より先に mapping が張られるよう)
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let vocab_rec = enchudb_oplog::oplog::OwnedOp::Vocab { vid, bytes: value.to_vec() };
            let tie_rec = if self.himo_is_content(hid) {
                // 0.9.0: 動的 content himo は id が peer 間で揃わないため名前で運ぶ
                enchudb_oplog::oplog::OwnedOp::TieNamed {
                    eid: oplog_eid,
                    himo_name: self.himo_names[hid].clone(),
                    himo_kind: self.value_types[hid] as u8,
                    value: vid,
                }
            } else {
                enchudb_oplog::oplog::OwnedOp::Tie { eid: oplog_eid, himo_id, value: vid }
            };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                // Vocab → Tie の順を保つため同一 thread から連続 push
                push_oplog_record_blocking(wq, vocab_rec, &self.consumer_poisoned, &self.wal_push_count);
                push_oplog_record_blocking(wq, tie_rec, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append(vocab_rec.as_op());
                let _ = wal.append(tie_rec.as_op());
            }
        }
    }

    /// v33: 非同期 tie_ref。target_eid の local 部(u32)を WAL / 本体に運ぶ。
    /// target_eid は u64 [peer|local] だが、現行 WAL は value: u32 しか運べないため
    /// peer_id 部は捨てる。すなわち receiver 側では「author_peer と同一 peer 上の entity」を
    /// 指す ref として再構成される。cross-peer ref (peer A が peer B の entity を指す)は
    /// 現状未対応 (Ref 用 WAL op を別途用意する必要あり、v34 以降)。
    pub fn tie_ref_async(&self, eid: enchudb_oplog::EntityId, himo: &str, target_eid: enchudb_oplog::EntityId) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_ref_async_by_id(eid, hid, target_eid);
    }

    /// `tie_ref_async` の himo_id 直指定版。
    pub fn tie_ref_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, target_eid: enchudb_oplog::EntityId) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        let target_local = enchudb_oplog::eid_local(target_eid);
        assert!(target_local < u32::MAX, "target_local must be < u32::MAX");
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(himo_id as usize, local);
        // β-light step 5: target_eid が target_table の eid range 内か
        self.validate_ref_tie(himo_id as usize, target_local);
        // #77-H4: op 先行 push (tie_async_by_id と同じ理由)
        let q = self.write_queue.as_ref()
            .expect("tie_ref_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie {
            eid: local, himo_id, value: target_local,
        });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let rec = enchudb_oplog::oplog::OwnedOp::Tie {
                eid: oplog_eid, himo_id, value: target_local,
            };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                push_oplog_record_blocking(wq, rec, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
    }

    /// 非同期 untie。
    pub fn untie_async(&self, eid: enchudb_oplog::EntityId, himo: &str) {
        let hid = match self.himo_id(himo) { Some(x) => x as u16, None => return };
        self.untie_async_by_id(eid, hid);
    }

    /// `untie_async` の himo_id 直指定版。
    pub fn untie_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        if (himo_id as usize) >= self.himos.len() { return; }
        // #77-H4: op 先行 push (tie_async_by_id と同じ理由)
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Untie { eid: local, himo_id });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let rec = enchudb_oplog::oplog::OwnedOp::Untie { eid: oplog_eid, himo_id };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                push_oplog_record_blocking(wq, rec, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
    }

    /// 非同期 delete。
    pub fn delete_async(&self, eid: enchudb_oplog::EntityId) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        // #77-H4: op 先行 push (tie_async_by_id と同じ理由)
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Delete { eid: local });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let rec = enchudb_oplog::oplog::OwnedOp::Delete { eid: oplog_eid };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                push_oplog_record_blocking(wq, rec, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
    }

    /// 非同期 content 書き込み。WAL 有効時はクラッシュ後も復元される。
    ///
    /// 0.9.0: 保存先を `_c_{key}` Leaf himo に変更 (`content` の doc 参照)。
    /// WAL には `Op::Content` ではなく通常の `Op::Vocab` + `Op::Tie` が乗る。
    pub fn content_async(&self, eid: enchudb_oplog::EntityId, key: &str, data: &[u8]) {
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        let hid = self.ensure_content_himo(local, key);
        self.tie_bytes_async_by_id(eid, hid, data);
    }

    /// 現在のトランザクションを WAL に commit marker で確定する。
    ///
    /// Async モードでは fsync は consumer スレッドが背景で行う。
    /// Sync モードでは commit 完了まで待つ。
    pub fn oplog_commit(&self) {
        if let Some(wal) = self.oplog.as_ref() {
            let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
        }
    }

    /// oplog への参照(テスト / 内部用)。
    pub fn oplog(&self) -> Option<&std::sync::Arc<enchudb_oplog::oplog::OpLog>> { self.oplog.as_ref() }

    /// push 済みの全 Op が apply 完了するまで spin 待ち。`tie_async` の同期点。
    /// `queue.is_empty()` は pop 直後 / apply 前のウィンドウで true になる race が
    /// あるため、push_count と apply_count の累積カウンタで apply 完了を待つ。
    pub fn flush_writes(&self) {
        use std::sync::atomic::Ordering;
        if self.write_queue.is_none() { return; }
        loop {
            let pushed = self.push_count.load(Ordering::Acquire);
            let applied = self.apply_count.load(Ordering::Acquire);
            // WAL record queue も barrier に含める: op は queue 先行・record
            // 後追い (#77-H4) なので、 apply_count だけで返ると「op は適用済み
            // だが WAL record は queue 内」の窓が残り、 直後の oplog_sync が
            // record を含まない WAL に Commit + fsync してしまう
            // (= fsync 済みのはずの write が crash で消える durability 破れ)。
            let wal_pushed = self.wal_push_count.load(Ordering::Acquire);
            let wal_appended = self.wal_append_count.load(Ordering::Acquire);
            if applied >= pushed && wal_appended >= wal_pushed { return; }
            // #77-M2: consumer が死んでいたら barrier は永遠に成立しない。
            // silent hang ではなく診断可能な panic で失敗する。
            if self.consumer_poisoned.load(Ordering::Acquire) {
                panic!("enchudb consumer thread has panicked — flush_writes cannot complete (#77-M2)");
            }
            std::thread::yield_now();
        }
    }

    /// キューに滞留中の件数(デバッグ用)。
    pub fn pending_writes(&self) -> usize {
        self.write_queue.as_ref().map(|q| q.len()).unwrap_or(0)
    }

    // ──── flush ────

    #[cfg(not(target_arch = "wasm32"))]
    pub fn flush(&mut self) -> io::Result<()> {
        self.commit();

        // β-light step 7: tables sidecar の最新化 (next_local 含む)。
        // schema 変更 (define_*) でも persist してるが、 entity_in による
        // next_local 増加は persist してないので flush 時にまとめて persist する。
        self.try_persist_tables();

        for ds in &self.himos { ds.sync(); }
        self.vocab.sync();
        self.himo_reg.sync();
        self.contents.sync();

        let maxv_base = himo_maxv_base(self.max_himos);
        let hc = self.value_types.len() as u32;
        let buf = self.backing.as_slice_mut();
        for hid in 0..self.value_types.len() {
            buf[H_HIMO_TYPES + hid] = self.value_types[hid] as u8;
            let off = maxv_base + hid * 4;
            buf[off..off + 4].copy_from_slice(&self.himo_max_values[hid].to_le_bytes());
        }
        buf[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&hc.to_le_bytes());
        // v28: ヘッダ整合性 CRC(himo_count 含む固定レイアウト部のみを対象)
        write_header_crc(buf);

        // 全 region を disk に同期した後、 vocab/himo_reg の index 整合性 OK
        // マークを書いてもう一度 msync。 次 open で rebuild_index を skip できる。
        self.sync_and_mark_clean()
    }

    /// #101: 全 region msync → vocab/himo_reg の clean マーク → 再 msync。
    /// mark 前の msync で「flag が指す中身」を先に固める順序が要る (flush() から切り出し)。
    #[cfg(not(target_arch = "wasm32"))]
    fn sync_and_mark_clean(&self) -> io::Result<()> {
        self.backing.flush_to_disk()?;
        self.vocab.mark_index_clean(true);
        self.himo_reg.mark_index_clean(true);
        self.backing.flush_to_disk()?;
        Ok(())
    }

    /// #101: graceful close 用の clean-flush (`&self` 版)。 滞留 write を全 apply して
    /// msync し、 vocab/himo_reg の整合性マーク (clean=1) を永続化する。 次 open は
    /// rebuild_index (O(count)) を skip でき、 readonly open の shadow rebuild も消える。
    ///
    /// `flush()` (&mut) と違い header は書き直さない (himo 定義は define 時に persist 済み)。
    /// readonly open では no-op。 書き込み thread が並行中に呼んだ場合、 mark 後の
    /// insert が flag を 0 に戻すので安全側 (次 open が rebuild) に倒れる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn flush_clean(&self) -> io::Result<()> {
        if self.is_readonly.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        self.flush_writes();
        self.commit();
        self.try_persist_tables();
        for ds in &self.himos {
            ds.sync();
        }
        self.vocab.sync();
        self.himo_reg.sync();
        self.contents.sync();
        self.sync_and_mark_clean()
    }

    /// #101: 観測用 — vocab index の clean flag。 注意: writer open は #56 の保護で
    /// open 直後に flag を 0 へ戻すので、 session 中に true になるのは
    /// 「flush_clean 後 & 次の vocab insert 前」のみ。 「open が rebuild を skip
    /// できたか」は `vocab_index_rebuilt_on_load()` で見る。
    pub fn vocab_index_is_clean(&self) -> bool {
        self.vocab.index_clean_on_disk()
    }

    /// #101: 観測用 — この Engine の open 時に vocab index の rebuild (O(count)) が
    /// 走ったか。 false = 前回 graceful close の clean flag で skip できた。
    pub fn vocab_index_rebuilt_on_load(&self) -> bool {
        self.vocab.rebuilt_on_load
    }

    /// v29: region CRC を計算して `.crc` sidecar に永続化する。
    /// flush() とは別の opt-in API。512MB+ の vocab 走査を含むので秒オーダー。
    /// コールドバックアップを封緘するユースケース向け。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn seal_integrity(&mut self) -> io::Result<()> {
        self.flush()?;
        self.persist_region_crcs()?;
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        // 既に push 済みの全 Op が apply 完了するまで待機(best-effort)。
        // shutdown flag を立てる前に呼ぶことで、writer がまだ活きてる場合でも
        // ここまでに走った tie_async は consumer が反映済みとなる。
        if self.write_queue.is_some() {
            loop {
                let pushed = self.push_count.load(Ordering::Acquire);
                let applied = self.apply_count.load(Ordering::Acquire);
                if applied >= pushed { break; }
                // #77-M2: consumer 死亡時は待っても進まない。 Drop は panic せず
                // 諦めて shutdown へ進む (未 apply 分は失われるが、 hang しない)。
                if self.consumer_poisoned.load(Ordering::Acquire) { break; }
                std::thread::yield_now();
            }
        }
        if let Some(flag) = &self.shutdown_flag {
            flag.store(true, Ordering::Release);
        }
        // consumer スレッドは shutdown flag を検知したら最終 drain を行って exit する。
        if let Ok(mut h) = self.consumer_handle.lock() {
            if let Some(handle) = h.take() {
                let _ = handle.join();
            }
        }
        // #101: graceful close で clean flag を永続化 → 次 open は rebuild_index skip。
        // panic unwinding 中 / consumer 死亡時は「dirty のまま」に倒す (= 次 open で
        // rebuild が走るのが正しい recovery)。 readonly は共有 mmap を書けないので除外。
        // queue は上の drain + consumer join で空なので barrier (flush_writes) は不要。
        // best-effort: msync の Err は握る (dirty のままでも安全側)。
        #[cfg(not(target_arch = "wasm32"))]
        if !std::thread::panicking()
            && !self.is_readonly.load(Ordering::Acquire)
            && !self.consumer_poisoned.load(Ordering::Acquire)
        {
            self.commit();
            self.try_persist_tables();
            for ds in &self.himos {
                ds.sync();
            }
            self.vocab.sync();
            self.himo_reg.sync();
            self.contents.sync();
            let _ = self.sync_and_mark_clean();
        }
    }
}

// ════════════════ テスト ════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let path = format!("/tmp/enchu_v24_{name}.db");
        let _ = std::fs::remove_file(&path);
        path
    }

    /// テスト用: rebuild + query_count を一発で。
    fn qc(eng: &Engine, conds: &[(&str, u32)]) -> usize {
        eng.rebuild();
        eng.query(conds).len()
    }

    // ──── entity ライフサイクル ────

    #[test]
    fn entity_create_and_count() {
        let dir = tmp("ent_create");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        assert_eq!(eng.entity_count(), 0);
        let e0 = eng.entity();
        let e1 = eng.entity();
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.entities(), vec![e0, e1]);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn entity_delete_and_reuse() {
        let dir = tmp("ent_del");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e0 = eng.entity();
        let e1 = eng.entity();
        let e2 = eng.entity();

        eng.delete(e1);
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.entities(), vec![e0, e2]);

        // 上限前は欠番（monotonic）— IDは再利用されない
        let e3 = eng.entity();
        assert_eq!(e3, 3); // e1(=1)ではなく新規ID
        assert_eq!(eng.entity_count(), 3);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── tie / get 全型 ────

    #[test]
    fn tie_text_roundtrip() {
        let dir = tmp("tie_text");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie_text(e, "name", "田中");
        assert_eq!(eng.get_text(e, "name"), Some("田中".as_bytes()));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_value_roundtrip() {
        let dir = tmp("tie_val");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_entity_ref() {
        let dir = tmp("tie_eref");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let parent = eng.entity();
        let child = eng.entity();
        eng.tie_ref(child, "company", parent);
        assert_eq!(eng.get(child, "company"), Some(parent as u32));
        eng.rebuild();
        let result = eng.pull_raw("company", parent as u32);
        assert_eq!(result, vec![child]);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_overwrite() {
        let dir = tmp("tie_ow");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "score", 100);
        eng.tie(e, "score", 200);
        assert_eq!(eng.get(e, "score"), Some(200));
        assert_eq!(qc(&mut eng, &[("score", 100)]), 0);
        assert_eq!(qc(&mut eng, &[("score", 200)]), 1);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_value_zero() {
        let dir = tmp("tie_zero");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "level", 0);
        assert_eq!(eng.get(e, "level"), Some(0));
        assert_eq!(qc(&mut eng, &[("level", 0)]), 1);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── untie ────

    #[test]
    fn untie_removes_value() {
        let dir = tmp("untie");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "X");

        eng.untie(e, "age");
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(qc(&mut eng, &[("age", 30)]), 0);
        assert_eq!(eng.get_text(e, "name"), Some(b"X".as_ref()));
        let _ = std::fs::remove_file(&dir);
    }

    // ──── delete ────

    #[test]
    fn delete_removes_all_ties() {
        let dir = tmp("del_ties");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "田中");

        eng.delete(e);
        assert_eq!(qc(&mut eng, &[("age", 30)]), 0);
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(eng.get_text(e, "name"), None);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── content ────

    #[test]
    fn content_set_get() {
        let dir = tmp("content");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.content(e, "memo", b"hello");
        eng.content(e, "notes", "日本語".as_bytes());
        assert_eq!(eng.get_content(e, "memo"), Some(b"hello".as_ref()));
        assert_eq!(eng.get_content(e, "notes"), Some("日本語".as_bytes()));
        assert_eq!(eng.get_content(e, "none"), None);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── himos_of / himo_names ────

    #[test]
    fn himos_of_entity() {
        let dir = tmp("himos_of");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "X");
        let h = eng.himos_of(e);
        assert!(h.contains(&"age"));
        assert!(h.contains(&"name"));
        assert_eq!(h.len(), 2);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn himo_names_all() {
        let dir = tmp("himo_names");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "x", 1);
        eng.tie_text(e, "y", "a");
        eng.tie_ref(e, "z", e);
        let names = eng.himo_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"x".to_string()));
        assert!(names.contains(&"y".to_string()));
        assert!(names.contains(&"z".to_string()));
        let _ = std::fs::remove_file(&dir);
    }

    // ──── query ────

    #[test]
    fn query_single_condition() {
        let dir = tmp("q_single");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e0 = eng.entity();
        eng.tie(e0, "age", 30);
        let e1 = eng.entity();
        eng.tie(e1, "age", 25);
        let e2 = eng.entity();
        eng.tie(e2, "age", 30);

        eng.rebuild();
        let result = eng.query(&[("age", 30)]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&e0));
        assert!(result.contains(&e2));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn query_multi_condition() {
        let dir = tmp("q_multi");
        let mut eng = Engine::create_standalone(&dir).unwrap();

        let e0 = eng.entity();
        eng.tie(e0, "age", 30);
        eng.tie(e0, "dept", 1);

        let e1 = eng.entity();
        eng.tie(e1, "age", 25);
        eng.tie(e1, "dept", 1);

        let e2 = eng.entity();
        eng.tie(e2, "age", 30);
        eng.tie(e2, "dept", 2);

        eng.rebuild();
        assert_eq!(eng.query(&[("dept", 1), ("age", 30)]), vec![e0]);
        assert_eq!(eng.query(&[("dept", 1), ("age", 25)]), vec![e1]);
        assert_eq!(eng.query(&[("dept", 2), ("age", 30)]), vec![e2]);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn query_empty_result() {
        let dir = tmp("q_empty");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.rebuild();
        assert!(eng.query(&[("age", 99)]).is_empty());
        assert_eq!(qc(&mut eng, &[("age", 99)]), 0);
        assert!(eng.query(&[("nonexistent", 1)]).is_empty());
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn query_count_matches_len() {
        let dir = tmp("q_count");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        for i in 0..10 {
            let e = eng.entity();
            eng.tie(e, "bucket", i % 3);
        }
        eng.rebuild();
        for b in 0..3 {
            let q = eng.query(&[("bucket", b)]);
            let c = qc(&mut eng, &[("bucket", b)]);
            assert_eq!(q.len(), c);
        }
        let _ = std::fs::remove_file(&dir);
    }

    // ──── LazyCylinder ────

    #[test]
    fn lazy_cylinder_pull_observe() {
        let dir = tmp("lazy_cyl");
        let mut eng = Engine::create_standalone(&dir).unwrap();

        let e0 = eng.entity();
        eng.tie(e0, "age", 30);
        eng.tie(e0, "dept", 1);

        let e1 = eng.entity();
        eng.tie(e1, "age", 25);
        eng.tie(e1, "dept", 1);

        eng.rebuild();
        assert_eq!(eng.query(&[("dept", 1), ("age", 30)]), vec![e0]);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── range query ────

    #[test]
    fn pull_range() {
        let dir = tmp("range");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        for age in 20..=40 {
            let e = eng.entity();
            eng.tie(e, "age", age);
        }
        eng.rebuild();
        let mut total = 0;
        for age in 25..=30 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, 6);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn lazy_cylinder_pull_range() {
        let dir = tmp("lc_range");
        let mut eng = Engine::create_standalone(&dir).unwrap();

        for age in 20..=40 {
            let e = eng.entity();
            eng.tie(e, "age", age);
            eng.tie(e, "dept", 1);
        }

        eng.rebuild();
        let mut age_ents: Vec<u64> = Vec::new();
        for age in 25..=30 {
            age_ents.extend(eng.pull_raw("age", age));
        }
        age_ents.sort_unstable();
        let dept1 = eng.pull_raw("dept", 1);
        let mut count = 0;
        let (mut i, mut j) = (0, 0);
        while i < age_ents.len() && j < dept1.len() {
            match age_ents[i].cmp(&dept1[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => { count += 1; i += 1; j += 1; }
            }
        }
        assert_eq!(count, 6);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 永続化 ────

    #[test]
    fn persistence_full_roundtrip() {
        let dir = tmp("persist");

        {
            let mut eng = Engine::create_standalone(&dir).unwrap();
            let e0 = eng.entity();
            eng.tie(e0, "age", 25);
            eng.tie(e0, "dept", 1);

            let e1 = eng.entity();
            eng.tie(e1, "age", 30);
            eng.tie(e1, "dept", 1);
            eng.content(e1, "memo", b"hello");

            eng.flush().unwrap();
        }

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.get(0, "age"), Some(25));
        assert_eq!(eng.get(1, "age"), Some(30));
        assert_eq!(eng.get_content(1, "memo"), Some(b"hello".as_ref()));
        assert_eq!(qc(&mut eng, &[("dept", 1), ("age", 30)]), 1);

        let e2 = eng.entity();
        eng.tie(e2, "age", 35);
        eng.tie(e2, "dept", 1);
        assert_eq!(qc(&mut eng, &[("dept", 1), ("age", 35)]), 1);

        let _ = std::fs::remove_file(&dir);
    }

    // ──── vocab ────

    #[test]
    fn vocab_id_lookup() {
        let dir = tmp("vocab");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie_text(e, "city", "東京");
        eng.tie_text(e, "city2", "大阪");

        assert!(eng.vocab_id("東京").is_some());
        assert!(eng.vocab_id("大阪").is_some());
        assert!(eng.vocab_id("福岡").is_none());
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 境界値 ────

    #[test]
    fn boundary_value_zero() {
        let dir = tmp("bnd_zero");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "x", 0);
        assert_eq!(eng.get(e, "x"), Some(0));
        assert_eq!(qc(&mut eng, &[("x", 0)]), 1);

        eng.untie(e, "x");
        assert_eq!(eng.get(e, "x"), None);
        eng.tie(e, "x", 0);
        assert_eq!(eng.get(e, "x"), Some(0));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn boundary_value_large() {
        let dir = tmp("bnd_large");
        let mut eng = Engine::create_standalone(&dir).unwrap();

        let ts = 1_743_552_000u32;
        let e = eng.entity();
        eng.tie(e, "ts", ts);
        assert_eq!(eng.get(e, "ts"), Some(ts));
        eng.rebuild();
        let result = eng.pull_raw("ts", ts);
        assert_eq!(result, vec![e]);

        // v27 BucketCylinder は動的拡張するので、 u32::MAX-2 だと
        // バケット 40 億本分の Vec を確保してしまう。 ここでは代わりに
        // 100 万オーダーの「大きめ値」で検証する。
        let big = 1_000_000u32;
        let e2 = eng.entity();
        eng.tie(e2, "huge", big);
        assert_eq!(eng.get(e2, "huge"), Some(big));
        eng.rebuild();
        let result2 = eng.pull_raw("huge", big);
        assert_eq!(result2, vec![e2]);

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn boundary_consecutive_values() {
        let dir = tmp("bnd_consec");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        for v in 0..5u32 {
            let e = eng.entity();
            eng.tie(e, "level", v);
        }
        for v in 0..5u32 {
            assert_eq!(qc(&mut eng, &[("level", v)]), 1);
        }
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn boundary_many_dims() {
        let dir = tmp("bnd_dims");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        for d in 0..20u32 {
            eng.tie(e, &format!("dim_{d}"), d * 10);
        }
        for d in 0..20u32 {
            assert_eq!(eng.get(e, &format!("dim_{d}")), Some(d * 10));
        }
        assert_eq!(eng.himos_of(e).len(), 20);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 大量削除 → query整合性 ────

    #[test]
    fn bulk_delete_query_consistency() {
        let dir = tmp("bulk_del");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let n = 1000u32;
        for i in 0..n {
            let e = eng.entity();
            eng.tie(e, "group", i % 5);
            eng.tie(e, "score", (i / 5) % 10);
        }

        eng.rebuild();
        let group0: Vec<u64> = eng.query(&[("group", 0)]);
        assert_eq!(group0.len(), 200);
        for &eid in &group0 {
            eng.delete(eid);
        }
        for g in 1..5u32 {
            assert_eq!(eng.query_count(&[("group", g)]), 200);
        }
        assert_eq!(eng.entity_count(), 800);
        for s in 0..10u32 {
            assert_eq!(eng.query_count(&[("score", s)]), 80);
        }
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn delete_all_then_reinsert() {
        let dir = tmp("del_all");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let n = 100u32;
        for _ in 0..n {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.query_count(&[("val", 42)]), 100);

        let all: Vec<u64> = eng.entities();
        for eid in all {
            eng.delete(eid);
        }
        assert_eq!(eng.entity_count(), 0);
        assert_eq!(eng.query_count(&[("val", 42)]), 0);

        for _ in 0..50 {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.entity_count(), 50);
        assert_eq!(eng.query_count(&[("val", 42)]), 50);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 永続化の堅牢性 ────

    #[test]
    fn persistence_after_delete() {
        let dir = tmp("persist_del");
        {
            let mut eng = Engine::create_standalone(&dir).unwrap();
            for i in 0..100u32 {
                let e = eng.entity();
                eng.tie(e, "val", i % 10);
            }
            eng.rebuild();
            let del_targets: Vec<u64> = eng.query(&[("val", 0)]);
            for &eid in &del_targets {
                eng.delete(eid);
            }
            eng.flush().unwrap();
        }

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.query_count(&[("val", 0)]), 0);
        for v in 1..10u32 {
            assert_eq!(eng.query_count(&[("val", v)]), 10);
        }
        assert_eq!(eng.entity_count(), 90);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 多数 entity ────

    #[test]
    fn many_entities_1k() {
        let dir = tmp("many_1k");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let n = 1000u32;
        for i in 0..n {
            let e = eng.entity();
            eng.tie(e, "val", i % 10);
        }
        assert_eq!(eng.entity_count(), n);
        for b in 0..10 {
            assert_eq!(eng.query_count(&[("val", b)]), 100);
        }
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 100万 entity スケールテスト ────

    const SCALE_N: u32 = 1_000_000;
    const SCALE_COMPANIES: u32 = 100;
    const SCALE_CITIES: u32 = 10;
    const SCALE_AGES: u32 = 50;
    const SCALE_DEPTS: u32 = 8;
    const SCALE_PER_CO: u32 = SCALE_N / SCALE_COMPANIES;

    fn setup_scale(dir: &str) -> Engine {
        let mut eng = Engine::create_standalone(dir).unwrap();

        for c in 0..SCALE_COMPANIES {
            for e in 0..SCALE_PER_CO {
                let eid = eng.entity();
                eng.tie(eid, "age", e % SCALE_AGES);
                eng.tie(eid, "dept", (e / SCALE_AGES) % SCALE_DEPTS);
                eng.tie(eid, "company", c);
                eng.tie_text(eid, "city", &format!("city_{}", c % SCALE_CITIES));
            }
        }
        eng
    }

    #[test]
    #[ignore]
    fn scale_insert_1m() {
        let dir = tmp("scale_insert");
        let eng = setup_scale(&dir);
        assert_eq!(eng.entity_count(), SCALE_N);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_point_query() {
        let dir = tmp("scale_point");
        let mut eng = setup_scale(&dir);
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(eng.query_count(&[("age", 30)]), expected);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_multi_condition() {
        let dir = tmp("scale_multi");
        let mut eng = setup_scale(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let expected = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30)]), expected);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_three_conditions() {
        let dir = tmp("scale_3cond");
        let mut eng = setup_scale(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let per_co = SCALE_PER_CO / SCALE_AGES / SCALE_DEPTS;
        let expected = (SCALE_COMPANIES / SCALE_CITIES * per_co) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30), ("dept", 3)]), expected);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_range_query() {
        let dir = tmp("scale_range");
        let mut eng = setup_scale(&dir);
        eng.rebuild();
        let per_age = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let mut total = 0;
        for age in 25..=34 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, per_age * 10);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_empty_result() {
        let dir = tmp("scale_empty");
        let mut eng = setup_scale(&dir);
        assert_eq!(eng.query_count(&[("age", 99)]), 0);
        assert!(eng.query(&[("age", 99)]).is_empty());
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_delete_reinsert() {
        let dir = tmp("scale_delins");
        let mut eng = setup_scale(&dir);
        let before = eng.query_count(&[("age", 30)]);

        let victims: Vec<u64> = eng.query(&[("age", 30)]).into_iter().take(100).collect();
        for eid in &victims {
            eng.delete(*eid);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before - 100);

        for _ in 0..100 {
            let e = eng.entity();
            eng.tie(e, "age", 30);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_update() {
        let dir = tmp("scale_upd");
        let mut eng = setup_scale(&dir);
        let before_30 = eng.query_count(&[("age", 30)]);
        assert_eq!(eng.query_count(&[("age", 99)]), 0);

        let targets: Vec<u64> = eng.query(&[("age", 30)]).into_iter().take(500).collect();
        for eid in &targets {
            eng.tie(*eid, "age", 99);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before_30 - 500);
        assert_eq!(eng.query_count(&[("age", 99)]), 500);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_persistence() {
        let dir = tmp("scale_persist");
        let city0_vid;
        let expected_age30 = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let expected_city_age = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        {
            let mut eng = setup_scale(&dir);
            city0_vid = eng.vocab_id("city_0").unwrap();
            eng.flush().unwrap();
        }

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.entity_count(), SCALE_N);
        assert_eq!(eng.query_count(&[("age", 30)]), expected_age30);
        assert_eq!(eng.query_count(&[("city", city0_vid), ("age", 30)]), expected_city_age);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_group_by_equivalent() {
        let dir = tmp("scale_grp");
        let mut eng = setup_scale(&dir);
        let mut total = 0usize;
        for c in 0..SCALE_CITIES {
            let vid = eng.vocab_id(&format!("city_{c}")).unwrap();
            total += eng.query_count(&[("city", vid), ("age", 30)]);
        }
        let expected_total = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(total, expected_total);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── トランザクション ────

    #[test]
    fn commit_persists() {
        let dir = tmp("tx_commit");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.commit();
        eng.flush().unwrap();
        drop(eng);

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_file(&dir);
    }

    // v4: rollback / undo log を削除したため `rollback_reverts` /
    //     `rollback_insert` / `crash_recovery_rollback` テストも撤去。 旧 undo
    //     replay 経路の保証 (= flush 済み未 commit 書き込みを open 時に巻き戻す)
    //     はもう存在しない。 crash 中の途中状態を巻き戻したいケースは WAL
    //     (Commit marker 未到達なら recover 時に drop) で代替する。

    // ──── prefix sum O(1) ────

    #[test]
    fn prefix_sum_point_query() {
        let dir = tmp("ps_point");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 100);
        eng.define_himo("dept", ValueType::Number, 20);

        for i in 0..1000u32 {
            let e = eng.entity();
            eng.tie(e, "age", i % 50);
            eng.tie(e, "dept", i % 8);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), 20);
        assert_eq!(eng.query_count(&[("dept", 3)]), 125);
        assert_eq!(eng.query(&[("age", 30), ("dept", 2)]).len(), 5);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_value_zero() {
        let dir = tmp("ps_zero");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("level", ValueType::Number, 10);
        let e = eng.entity();
        eng.tie(e, "level", 0);
        assert_eq!(eng.get(e, "level"), Some(0));
        assert_eq!(eng.query_count(&[("level", 0)]), 1);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_mixed_with_bsearch() {
        let dir = tmp("ps_mixed");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 100);

        for i in 0..100u32 {
            let e = eng.entity();
            eng.tie(e, "age", i % 10);
            eng.tie_text(e, "city", if i < 50 { "東京" } else { "大阪" });
        }
        let tokyo = eng.vocab_id("東京").unwrap();
        assert_eq!(eng.query_count(&[("age", 5)]), 10);
        assert_eq!(eng.query_count(&[("city", tokyo)]), 50);
        assert_eq!(eng.query_count(&[("age", 5), ("city", tokyo)]), 5);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_persistence() {
        let dir = tmp("ps_persist");
        {
            let mut eng = Engine::create_standalone(&dir).unwrap();
            eng.define_himo("score", ValueType::Number, 200);
            for i in 0..100u32 {
                let e = eng.entity();
                eng.tie(e, "score", i % 20);
            }
            assert_eq!(eng.query_count(&[("score", 5)]), 5);
            eng.flush().unwrap();
        }
        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.query_count(&[("score", 5)]), 5);
        assert_eq!(eng.entity_count(), 100);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_untie() {
        let dir = tmp("ps_untie");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 100);
        let e = eng.entity();
        eng.tie(e, "age", 30);
        assert_eq!(eng.query_count(&[("age", 30)]), 1);
        eng.untie(e, "age");
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(eng.query_count(&[("age", 30)]), 0);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_overwrite() {
        let dir = tmp("ps_ow");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("score", ValueType::Number, 1000);
        let e = eng.entity();
        eng.tie(e, "score", 100);
        eng.tie(e, "score", 200);
        assert_eq!(eng.get(e, "score"), Some(200));
        assert_eq!(eng.query_count(&[("score", 100)]), 0);
        assert_eq!(eng.query_count(&[("score", 200)]), 1);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_delete() {
        let dir = tmp("ps_del");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 100);
        eng.define_himo("dept", ValueType::Number, 20);
        for i in 0..100u32 {
            let e = eng.entity();
            eng.tie(e, "age", i % 10);
            eng.tie(e, "dept", i % 5);
        }
        eng.rebuild();
        let victims: Vec<u64> = eng.query(&[("age", 0)]);
        assert_eq!(victims.len(), 10);
        for &eid in &victims { eng.delete(eid); }
        for a in 1..10u32 {
            assert_eq!(eng.query_count(&[("age", a)]), 10);
        }
        assert_eq!(eng.entity_count(), 90);
        let _ = std::fs::remove_file(&dir);
    }


    #[test]
    fn prefix_sum_boundary_max() {
        let dir = tmp("ps_bnd_max");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("x", ValueType::Number, 10);
        let e = eng.entity();
        eng.tie(e, "x", 10);
        assert_eq!(eng.get(e, "x"), Some(10));
        assert_eq!(eng.query_count(&[("x", 10)]), 1);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_bulk_delete_reinsert() {
        let dir = tmp("ps_bulk");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        for _ in 0..500u32 {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.query_count(&[("val", 42)]), 500);

        let all: Vec<u64> = eng.entities();
        for eid in all { eng.delete(eid); }
        assert_eq!(eng.entity_count(), 0);
        assert_eq!(eng.query_count(&[("val", 42)]), 0);

        for _ in 0..200 {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.entity_count(), 200);
        assert_eq!(eng.query_count(&[("val", 42)]), 200);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── prefix sum スケールテスト（100万 entity）────

    fn setup_scale_prefix(dir: &str) -> Engine {
        let mut eng = Engine::create_standalone(dir).unwrap();
        eng.define_himo("age", ValueType::Number, SCALE_AGES);
        eng.define_himo("dept", ValueType::Number, SCALE_DEPTS);
        eng.define_himo("company", ValueType::Number, SCALE_COMPANIES);

        for c in 0..SCALE_COMPANIES {
            for e in 0..SCALE_PER_CO {
                let eid = eng.entity();
                eng.tie(eid, "age", e % SCALE_AGES);
                eng.tie(eid, "dept", (e / SCALE_AGES) % SCALE_DEPTS);
                eng.tie(eid, "company", c);
                eng.tie_text(eid, "city", &format!("city_{}", c % SCALE_CITIES));
            }
        }
        eng
    }

    #[test]
    #[ignore]
    fn ps_scale_insert_1m() {
        let dir = tmp("ps_scale_ins");
        let eng = setup_scale_prefix(&dir);
        assert_eq!(eng.entity_count(), SCALE_N);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_point_query() {
        let dir = tmp("ps_scale_point");
        let mut eng = setup_scale_prefix(&dir);
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(eng.query_count(&[("age", 30)]), expected);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_multi_condition() {
        let dir = tmp("ps_scale_multi");
        let mut eng = setup_scale_prefix(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let expected = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30)]), expected);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_three_conditions() {
        let dir = tmp("ps_scale_3cond");
        let mut eng = setup_scale_prefix(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let per_co = SCALE_PER_CO / SCALE_AGES / SCALE_DEPTS;
        let expected = (SCALE_COMPANIES / SCALE_CITIES * per_co) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30), ("dept", 3)]), expected);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_range_query() {
        let dir = tmp("ps_scale_range");
        let mut eng = setup_scale_prefix(&dir);
        eng.rebuild();
        let per_age = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let mut total = 0;
        for age in 25..=34 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, per_age * 10);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_delete_reinsert() {
        let dir = tmp("ps_scale_delins");
        let mut eng = setup_scale_prefix(&dir);
        let before = eng.query_count(&[("age", 30)]);
        let victims: Vec<u64> = eng.query(&[("age", 30)]).into_iter().take(100).collect();
        for eid in &victims { eng.delete(*eid); }
        assert_eq!(eng.query_count(&[("age", 30)]), before - 100);
        for _ in 0..100 {
            let e = eng.entity();
            eng.tie(e, "age", 30);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_update() {
        let dir = tmp("ps_scale_upd");
        let mut eng = setup_scale_prefix(&dir);
        let before_30 = eng.query_count(&[("age", 30)]);
        let targets: Vec<u64> = eng.query(&[("age", 30)]).into_iter().take(500).collect();
        for eid in &targets { eng.tie(*eid, "age", 49); }
        assert_eq!(eng.query_count(&[("age", 30)]), before_30 - 500);
        assert_eq!(eng.query_count(&[("age", 49)]),
            (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize + 500);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_persistence() {
        let dir = tmp("ps_scale_persist");
        let city0_vid;
        let expected_age30 = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let expected_city_age = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        {
            let mut eng = setup_scale_prefix(&dir);
            city0_vid = eng.vocab_id("city_0").unwrap();
            eng.flush().unwrap();
        }
        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.entity_count(), SCALE_N);
        assert_eq!(eng.query_count(&[("age", 30)]), expected_age30);
        assert_eq!(eng.query_count(&[("city", city0_vid), ("age", 30)]), expected_city_age);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_group_by() {
        let dir = tmp("ps_scale_grp");
        let mut eng = setup_scale_prefix(&dir);
        let mut total = 0usize;
        for c in 0..SCALE_CITIES {
            let vid = eng.vocab_id(&format!("city_{c}")).unwrap();
            total += eng.query_count(&[("city", vid), ("age", 30)]);
        }
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(total, expected);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 1億 entity ────

    #[test]
    #[ignore]
    fn scale_100m_insert_and_query() {
        let dir = tmp("scale_100m");
        let n = 100_000_000u32;
        let ages = 100u32;
        let depts = 20u32;
        let groups = 1000u32;

        let mut eng = Engine::create_with_capacity(&dir, n + 1024).unwrap();
        eng.define_himo("age", ValueType::Number, ages);
        eng.define_himo("dept", ValueType::Number, depts);
        eng.define_himo("group", ValueType::Number, groups);

        for i in 0..n {
            let e = eng.entity();
            eng.tie(e, "age", i % ages);
            eng.tie(e, "dept", i % depts);
            eng.tie(e, "group", i % groups);
            if i % 1_000_000 == 999_999 { eng.commit(); }
        }
        assert_eq!(eng.entity_count(), n);

        assert_eq!(eng.query_count(&[("age", 50)]), (n / ages) as usize);
        assert_eq!(eng.query_count(&[("dept", 10)]), (n / depts) as usize);
        assert_eq!(eng.query_count(&[("age", 50), ("dept", 10)]), (n / ages) as usize);
        assert_eq!(eng.query_count(&[("age", 50), ("group", 500)]), 0);
        assert_eq!(eng.query_count(&[("age", 50), ("group", 50)]), (n / groups) as usize);
        assert_eq!(eng.query_count(&[("age", 30), ("dept", 10), ("group", 30)]), (n / groups) as usize);

        assert_eq!(eng.get(50, "age"), Some(50));
        assert_eq!(eng.get(50, "dept"), Some(50 % depts));

        let victims: Vec<u64> = eng.query(&[("age", 99)]).into_iter().take(1000).collect();
        for &eid in &victims { eng.delete(eid); }
        assert_eq!(eng.query_count(&[("age", 99)]), (n / ages) as usize - 1000);
        assert_eq!(eng.entity_count(), n - 1000);

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_late_himo_on_existing_entities() {
        let dir = tmp("late_himo");
        // Phase 1: 1000 entity作成、nameだけtie
        let mut eng = Engine::create_standalone(&dir).unwrap();
        for i in 0..1000u32 {
            let e = eng.entity();
            eng.tie_text(e, "name", &format!("company_{i}"));
        }
        eng.rebuild();
        eng.flush().unwrap();
        drop(eng);

        // Phase 2: 再open、既存entityに新しいhimoをtie
        let mut eng = Engine::open_standalone(&dir).unwrap();
        eng.rebuild();
        for eid in 0..1000u64 {
            eng.tie_text(eid, "has_flag", "1");
        }
        eng.rebuild();

        // rebuildの後、全件pull_rawで引けるか
        let vid = eng.vocab_id("1").expect("vocab_id");
        let result = eng.pull_raw("has_flag", vid);
        assert_eq!(result.len(), 1000, "expected 1000, got {}", result.len());

        eng.flush().unwrap();
        drop(eng);

        // Phase 3: 再open後も全件引けるか
        let eng = Engine::open_standalone(&dir).unwrap(); // open内でrebuild済み
        let vid = eng.vocab_id("1").expect("vocab_id");
        let result = eng.pull_raw("has_flag", vid);
        assert_eq!(result.len(), 1000, "after reopen: expected 1000, got {}", result.len());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_late_himo_sparse_large_eid() {
        let dir = tmp("late_himo_sparse");
        // Phase 1: 100 entity、大きなeid空間をシミュレート
        let mut eng = Engine::create_with_capacity(&dir, 6_000_000).unwrap();
        // entity 0..99 を作成
        for i in 0..100u32 {
            let e = eng.entity();
            eng.tie_text(e, "name", &format!("company_{i}"));
        }
        // entity 100..5_999_999 も作成（名前なし、eid空間を広げる）
        for _ in 100..1000 {
            eng.entity();
        }
        eng.rebuild();
        eng.flush().unwrap();
        drop(eng);

        // Phase 2: 再open、entity 500 と 999 に新himo をtie
        let mut eng = Engine::open_standalone(&dir).unwrap();
        eng.rebuild();
        eng.tie_text(500, "has_flag", "1");
        eng.tie_text(999, "has_flag", "1");
        eng.tie_text(0, "has_flag", "1");
        eng.rebuild();

        let vid = eng.vocab_id("1").expect("vocab_id");
        let result = eng.pull_raw("has_flag", vid);
        assert_eq!(result.len(), 3, "expected 3, got {}", result.len());

        eng.flush().unwrap();
        drop(eng);

        // Phase 3: 再open後
        let eng = Engine::open_standalone(&dir).unwrap(); // open内でrebuild済み
        let vid = eng.vocab_id("1").expect("vocab_id");
        let result = eng.pull_raw("has_flag", vid);
        assert_eq!(result.len(), 3, "after reopen: expected 3, got {}", result.len());

        let _ = std::fs::remove_file(&dir);
    }


    #[test]
    fn concurrent_tie_async_basic() {
        // tie_async → flush_writes → pull_raw で値が見えること。
        let dir = tmp("concurrent_basic");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 200);
        let eids: Vec<u64> = (0..100).map(|_| eng.entity()).collect();

        let arc = Engine::concurrentize(eng);
        for (i, &e) in eids.iter().enumerate() {
            arc.tie_async(e, "age", (i as u32) % 50);
        }
        arc.flush_writes();

        // 各値ごとに 2 件ずつあるはず
        for v in 0..50u32 {
            let pulled = arc.pull_raw("age", v);
            assert_eq!(pulled.len(), 2, "value {} expected 2 ents", v);
        }
        drop(arc);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn concurrent_multi_reader_writer() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let dir = tmp("concurrent_mrw");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("k", ValueType::Number, 16);
        let eids: Vec<u64> = (0..1_000).map(|_| eng.entity()).collect();
        for (i, &e) in eids.iter().enumerate() {
            eng.tie(e, "k", (i as u32) % 16);
        }

        let arc = Engine::concurrentize(eng);
        let stop = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        // 4 reader threads
        for _ in 0..4 {
            let arc = arc.clone();
            let stop = stop.clone();
            handles.push(thread::spawn(move || {
                let mut ok = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    for v in 0..16u32 {
                        let _ = arc.pull_raw("k", v);
                        let _ = arc.query(&[("k", v)]);
                        ok += 1;
                    }
                }
                ok
            }));
        }

        // 1 writer thread (async)
        {
            let arc = arc.clone();
            let stop = stop.clone();
            let eids = eids.clone();
            handles.push(thread::spawn(move || {
                let mut i = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let e = eids[(i as usize) % eids.len()];
                    arc.tie_async(e, "k", i % 16);
                    i = i.wrapping_add(1);
                }
                i as u64
            }));
        }

        thread::sleep(Duration::from_millis(200));
        stop.store(true, Ordering::Relaxed);
        for h in handles { let _ = h.join(); }

        arc.flush_writes();
        // 最終状態で全 16 値の合計が 1000 に等しいこと(値の分布は分からないが合計は保たれる)
        let mut total = 0;
        for v in 0..16u32 {
            total += arc.pull_raw("k", v).len();
        }
        assert_eq!(total, 1000);

        drop(arc);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── blob store integration ────

    #[test]
    fn engine_blob_store_default_none() {
        let dir = tmp("blob_none");
        let eng = Engine::create_standalone(&dir).unwrap();
        assert!(eng.blob_store().is_none());
        assert!(eng.put_blob(b"data").is_none());
        let fake_id = crate::blob_store::BlobId::from_bytes(b"x");
        assert!(!eng.blob_exists(&fake_id));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn engine_blob_store_injected_put_get() {
        let dir = tmp("blob_inject");
        let eng = Engine::create_standalone(&dir).unwrap();
        let blob_root = std::env::temp_dir().join(format!(
            "enchu_blob_inj_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = std::sync::Arc::new(
            crate::blob_store::LocalBlobStore::new(&blob_root).unwrap(),
        );
        eng.set_blob_store(store);

        let data = b"image bytes";
        let id = eng.put_blob(data).unwrap().unwrap();
        assert!(eng.blob_exists(&id));
        let got = eng.get_blob(&id).unwrap().unwrap();
        assert_eq!(got.as_deref(), Some(&data[..]));

        let _ = std::fs::remove_dir_all(&blob_root);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn engine_blob_store_tie_hash_lookup() {
        // 実運用パターン: blob の hex を tie_text で紐付けて検索できる
        let dir = tmp("blob_tie");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("__blob_id", ValueType::Tag, 0);

        let blob_root = std::env::temp_dir().join(format!(
            "enchu_blob_tie_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = std::sync::Arc::new(
            crate::blob_store::LocalBlobStore::new(&blob_root).unwrap(),
        );
        eng.set_blob_store(store);

        // 3 entity に 2 種類の blob を配る(1 と 3 が同じ blob)
        let img_a = b"pixels-A".to_vec();
        let img_b = b"pixels-B".to_vec();
        let id_a = eng.put_blob(&img_a).unwrap().unwrap();
        let id_b = eng.put_blob(&img_b).unwrap().unwrap();
        assert_ne!(id_a, id_b);

        let e1 = eng.entity();
        eng.tie_text(e1, "__blob_id", &id_a.to_hex());
        let e2 = eng.entity();
        eng.tie_text(e2, "__blob_id", &id_b.to_hex());
        let e3 = eng.entity();
        eng.tie_text(e3, "__blob_id", &id_a.to_hex()); // e1 と同じ画像

        eng.rebuild();

        // blob_id で entity 検索
        let vid = eng.vocab_id(&id_a.to_hex()).unwrap();
        let mut matches = eng.pull_raw("__blob_id", vid);
        matches.sort();
        assert_eq!(matches, vec![e1, e3]);

        // dedup の確認: blob 側のファイル数は 2 個のみ
        let mut file_count = 0;
        let mut stack = vec![blob_root.clone()];
        while let Some(p) = stack.pop() {
            if let Ok(rd) = std::fs::read_dir(&p) {
                for ent in rd.flatten() {
                    let ep = ent.path();
                    if ep.is_dir() {
                        stack.push(ep);
                    } else if ep.is_file() {
                        file_count += 1;
                    }
                }
            }
        }
        assert_eq!(file_count, 2);

        let _ = std::fs::remove_dir_all(&blob_root);
        let _ = std::fs::remove_file(&dir);
    }

    /// `create_growable` が standalone と等価な書き込み / 読み出しを
    /// 提供することを確認する基本テスト。 内部 backing が違うだけで
    /// API 観点では区別がつかないはず。
    #[test]
    fn growable_create_tie_query_roundtrip() {
        let dir = tmp("growable_basic");
        let mut eng = Engine::create_growable(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        eng.define_himo("city", ValueType::Tag, 0);
        for i in 0..50u32 {
            let e = eng.entity();
            eng.tie(e, "age", 20 + (i % 30));
            eng.tie_text(
                e,
                "city",
                if i % 2 == 0 { "Tokyo" } else { "Osaka" },
            );
        }
        eng.rebuild();
        // age = 20 + 5 → 25 was tied to entity 5, 35, etc.
        assert!(eng.query(&[("age", 25)]).len() > 0);
        let _ = std::fs::remove_file(&dir);
    }

    /// `create_growable_tiny` の apparent サイズが state-log 想定の
    /// 数百 KB に収まることを確認する。 dogfood は matcha-shell の
    /// notif_state でやるが、 ここでも基本の roundtrip + サイズを
    /// 押さえる。
    #[test]
    fn growable_tiny_file_is_small() {
        let dir = tmp("growable_tiny");
        {
            let mut eng = Engine::create_growable_tiny(&dir).unwrap();
            eng.define_himo("key", ValueType::Tag, 0);
            eng.define_himo("ts", ValueType::Number, 0);
            // 50 rows of (uuid-like, timestamp) — matcha の notif_state
            // が捌くサイズ感。
            for i in 0..50u32 {
                let e = eng.entity();
                eng.tie_text(e, "key", &format!("uuid-{:08x}", i));
                eng.tie(e, "ts", 1_715_000_000 + i);
            }
            eng.flush().unwrap();
        }
        let meta = std::fs::metadata(&dir).unwrap();
        eprintln!(
            "growable_tiny apparent size after 50 rows: {} bytes ({:.1} KB)",
            meta.len(),
            meta.len() as f64 / 1024.0
        );
        // create_compact 同等シナリオは 305 MB apparent。 tiny は
        // 全 region の上限値の合計でファイルサイズが決まる仕組みで
        // 実測 ~ 5 MB に収まる。 これでも 60× 改善。
        // 真の lazy init で 4 KB クラスにするのは Phase B (issue.md)。
        assert!(
            meta.len() < 8 * 1024 * 1024,
            "tiny growable should be < 8 MB, got {} bytes",
            meta.len()
        );
        // 再オープンしてデータが取れることも確認
        let eng2 = Engine::open_standalone(&dir).unwrap();
        eng2.rebuild();
        assert_eq!(eng2.query(&[("ts", 1_715_000_010)]).len(), 1);
        let _ = std::fs::remove_file(&dir);
    }

    // ─────────────────────────────────────────────────────────────
    // β: writer lock + open_readonly tests
    // ─────────────────────────────────────────────────────────────

    /// readonly で開いた DB は writer lock を取らないので、 writer の存在に関わらず
    /// 開ける + 複数 readonly 同時 open も衝突しない。
    #[test]
    fn readonly_does_not_block_other_opens() {
        let p = tmp("readonly_no_block");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            let e = eng.entity();
            eng.tie(e, "v", 42);
            eng.flush().unwrap();
        }
        // writer 開きっぱなしのまま readonly 多重 open
        let writer = Engine::open_standalone(&p).unwrap();
        let r1 = Engine::open_readonly(&p).unwrap();
        let r2 = Engine::open_readonly(&p).unwrap();
        let r3 = Engine::open_readonly(&p).unwrap();
        // 全 reader が同じ値を見る
        for r in [&r1, &r2, &r3] {
            assert_eq!(r.pull_raw("v", 42).len(), 1);
        }
        drop((writer, r1, r2, r3));
        let _ = std::fs::remove_file(&p);
    }

    /// readonly で開いた engine の書き込み API は panic する。
    #[test]
    #[should_panic(expected = "read-only")]
    fn readonly_write_panics() {
        let p = tmp("readonly_panic");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            eng.flush().unwrap();
        }
        let mut eng = Engine::open_readonly(&p).unwrap();
        let e = eng.entity(); // ← ここで panic
        eng.tie(e, "v", 1);
        let _ = std::fs::remove_file(&p);
    }

    /// #56: readonly open は file を mutate しない (clean flag flip + msync しない)。
    /// 旧 behavior では open するだけで mtime が変わり DB が dirty 化していた。
    #[test]
    fn readonly_open_does_not_mutate_file() {
        let p = tmp("readonly_nomutate");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            let e = eng.entity();
            eng.tie(e, "v", 42);
            eng.flush().unwrap(); // clean=true で確定
        }
        let mtime_before = std::fs::metadata(&p).unwrap().modified().unwrap();
        // 書き込みがあれば mtime 差が必ず出るよう少し待つ
        std::thread::sleep(std::time::Duration::from_millis(20));
        {
            let eng = Engine::open_readonly(&p).unwrap();
            // read path も非破壊であること
            assert_eq!(eng.pull_raw("v", 42).len(), 1);
        }
        let mtime_after = std::fs::metadata(&p).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "readonly open + read must not modify the DB file (#56)"
        );
        let _ = std::fs::remove_file(&p);
    }

    /// 2 process emulation: 1 writer が live な間、 もう一つの open_standalone は
    /// 別 thread から呼ぶと block する。 100 ms 待っても 2nd が returnしないことで
    /// 排他を確認、 1st を drop すると 2nd が unblock。
    #[test]
    fn writer_blocks_concurrent_writer() {
        let p = tmp("writer_block");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            eng.flush().unwrap();
        }
        let eng_a = Engine::open_standalone(&p).unwrap();

        // #80: 同一プロセスの 2nd writer open は flock で block せず即エラー。
        // (別プロセス writer との排他は従来通り blocking flock — そちらは
        //  tests/content_store_cross_process.rs 系の subprocess テストで担保)
        let err = match Engine::open_standalone(&p) {
            Ok(_) => panic!("2nd open_standalone should fail fast in the same process"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
        assert!(err.to_string().contains("already open for writing in this process"));

        // 1 st を drop → 再 open できる
        drop(eng_a);
        let eng_b = Engine::open_standalone(&p)
            .expect("2nd open should succeed after 1st drop");
        drop(eng_b);
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(format!("{}.lock", p));
    }

    // ─────────────────────────────────────────────────────────────
    // _by_id API: string 版と等価な結果を返す
    // ─────────────────────────────────────────────────────────────

    /// `tie_to_by_id` / `tie_text_to_by_id` / `tie_ref_to_by_id` / `untie_by_id`
    /// が、 同等の string 版と同じ Column/vocab 状態を残すことを確認。
    #[test]
    fn by_id_tie_equivalence_sync() {
        let p_str = tmp("by_id_str");
        let p_id = tmp("by_id_id");
        {
            let eng = Engine::create_standalone(&p_str).unwrap();
            // string 版経路
            let mut eng = eng;
            eng.define_himo("year", ValueType::Number, 100);
            eng.define_himo("name", ValueType::Tag, 0);
            eng.define_himo("self_ref", ValueType::Ref, 0);
            let e = eng.entity();
            eng.tie_to(e, "year", 2026);
            eng.tie_text_to(e, "name", "alice");
            eng.tie_ref_to(e, "self_ref", e);
            eng.flush().unwrap();
        }
        {
            let eng = Engine::create_standalone(&p_id).unwrap();
            let mut eng = eng;
            eng.define_himo("year", ValueType::Number, 100);
            eng.define_himo("name", ValueType::Tag, 0);
            eng.define_himo("self_ref", ValueType::Ref, 0);
            let year_id = eng.himo_id("year").unwrap() as u16;
            let name_id = eng.himo_id("name").unwrap() as u16;
            let ref_id = eng.himo_id("self_ref").unwrap() as u16;
            let e = eng.entity();
            eng.tie_to_by_id(e, year_id, 2026);
            eng.tie_text_to_by_id(e, name_id, "alice");
            eng.tie_ref_to_by_id(e, ref_id, e);
            eng.flush().unwrap();
        }
        // 同じ size、 query 結果も同じ
        let m_str = std::fs::metadata(&p_str).unwrap().len();
        let m_id = std::fs::metadata(&p_id).unwrap().len();
        assert_eq!(m_str, m_id, "file size diverges: str={}, id={}", m_str, m_id);

        let eng_str = Engine::open_standalone(&p_str).unwrap();
        let eng_id = Engine::open_standalone(&p_id).unwrap();
        assert_eq!(eng_str.query(&[("year", 2026)]).len(), eng_id.query(&[("year", 2026)]).len());
        let alice = eng_str.vocab_id("alice").unwrap();
        assert_eq!(eng_str.pull_raw("name", alice).len(), eng_id.pull_raw("name", alice).len());

        let _ = std::fs::remove_file(&p_str);
        let _ = std::fs::remove_file(&p_id);
    }

    #[test]
    fn by_id_untie_equivalence_sync() {
        let p = tmp("by_id_untie");
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_himo("year", ValueType::Number, 100);
        let year_id = eng.himo_id("year").unwrap() as u16;
        let e = eng.entity();
        eng.tie_to_by_id(e, year_id, 2026);
        assert_eq!(eng.query(&[("year", 2026)]).len(), 1);
        eng.untie_by_id(e, year_id);
        assert_eq!(eng.query(&[("year", 2026)]).len(), 0);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    #[should_panic]
    fn by_id_out_of_range_panics() {
        let p = tmp("by_id_oor");
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_himo("year", ValueType::Number, 100);
        let e = eng.entity();
        // himo_id = 99 だが define されたのは 1 つだけ (id=0)。
        // debug build では debug_assert! のメッセージ、 release では array indexing の
        // out-of-bounds panic で落ちる。 どちらでも panic することだけ確認。
        eng.tie_to_by_id(e, 99, 0);
        let _ = std::fs::remove_file(&p);
    }

    /// growable backing で作った DB を open_standalone で再オープン
    /// できる (open_standalone は MmapMut で開くが、 ファイル format は
    /// 同一なので問題ないはず)。
    #[test]
    fn growable_then_open_standalone() {
        let dir = tmp("growable_reopen");
        {
            let mut eng = Engine::create_growable(&dir).unwrap();
            eng.define_himo("score", ValueType::Number, 0);
            for i in 0..10u32 {
                let e = eng.entity();
                eng.tie(e, "score", i * 10);
            }
            eng.flush().unwrap();
        }
        let eng2 = Engine::open_standalone(&dir).unwrap();
        eng2.rebuild();
        assert_eq!(eng2.query(&[("score", 30)]).len(), 1);
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 0.9.0: ensure_himo_dynamic (&self himo definition) ────

    /// `Arc<Engine>` (create_concurrent) から 4 thread 並行で
    /// `ensure_himo_dynamic` を叩く。 同名 → 同 hid (idempotent)、 hid 重複
    /// なし、 動的定義 himo の tie_to_by_id / get_by_id round-trip を確認。
    #[test]
    fn ensure_himo_dynamic_concurrent_idempotent() {
        let dir = tmp("himo_dyn_conc");
        let eng = Engine::create_concurrent(&dir).unwrap();

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();
        for t in 0..4u32 {
            let e = eng.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut shared_ids = Vec::new();
                let mut own_ids = Vec::new();
                for i in 0..8u32 {
                    // 全 thread 共通名 (奇偶 2 種) と thread 固有名を混ぜる
                    let shared = e
                        .ensure_himo_dynamic(
                            &format!("dyn_shared_{}", i % 2),
                            ValueType::Number,
                            0,
                        )
                        .unwrap();
                    let own = e
                        .ensure_himo_dynamic(
                            &format!("dyn_own_{}_{}", t, i),
                            ValueType::Number,
                            0,
                        )
                        .unwrap();
                    shared_ids.push((i % 2, shared));
                    own_ids.push(own);
                }
                (shared_ids, own_ids)
            }));
        }

        let mut shared_by_name: std::collections::HashMap<u32, u16> =
            std::collections::HashMap::new();
        let mut all_own: Vec<u16> = Vec::new();
        for h in handles {
            let (shared_ids, own_ids) = h.join().unwrap();
            for (name_key, hid) in shared_ids {
                // idempotent: 同名は全 thread / 全呼び出しで同じ hid
                if let Some(prev) = shared_by_name.insert(name_key, hid) {
                    assert_eq!(prev, hid, "same name resolved to different hids");
                }
            }
            all_own.extend(own_ids);
        }

        // 固有名 4 thread × 8 個 + 共有名 2 個 = 34 hid、 重複なし
        let mut everything: Vec<u16> = all_own.clone();
        everything.extend(shared_by_name.values().copied());
        let total = everything.len();
        assert_eq!(total, 4 * 8 + 2);
        everything.sort_unstable();
        everything.dedup();
        assert_eq!(everything.len(), total, "duplicate hid assigned");
        assert_eq!(eng.himo_count(), 34);

        // 定義済み名の再呼び出しは lock-free fast path で同 hid
        let again = eng
            .ensure_himo_dynamic("dyn_shared_0", ValueType::Number, 0)
            .unwrap();
        assert_eq!(again, shared_by_name[&0]);

        // 動的定義した himo で tie_to_by_id / get_by_id round-trip
        let hid = eng
            .ensure_himo_dynamic("dyn_roundtrip", ValueType::Number, 0)
            .unwrap();
        let e0 = eng.entity();
        eng.tie_to_by_id(e0, hid, 42);
        assert_eq!(eng.get_by_id(e0, hid), Some(42));

        let _ = std::fs::remove_file(&dir);
    }

    /// `ensure_himo_dynamic_in`: named table への lazy 定義も `&self` で通り、
    /// 旧 `define_himo_in` と同じ attach semantics になることを確認。
    #[test]
    fn ensure_himo_dynamic_in_attaches_to_table() {
        let dir = tmp("himo_dyn_in");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_table("users", 100).unwrap();
        let eng = std::sync::Arc::new(eng);

        let hid = eng
            .ensure_himo_dynamic_in("users", "age", ValueType::Number, 0)
            .unwrap();
        // idempotent: 2 回目は同 hid
        let hid2 = eng
            .ensure_himo_dynamic_in("users", "age", ValueType::Number, 0)
            .unwrap();
        assert_eq!(hid, hid2);
        // full name で引ける + table attach 済み
        assert_eq!(eng.himo_id("users.age"), Some(hid as usize));
        let e = eng.entity_in("users").unwrap();
        eng.tie_to_by_id(e, hid, 30);
        assert_eq!(eng.get_by_id(e, hid), Some(30));

        // 未定義 table は Err
        assert!(eng
            .ensure_himo_dynamic_in("nope", "x", ValueType::Number, 0)
            .is_err());

        let _ = std::fs::remove_file(&dir);
    }
}
