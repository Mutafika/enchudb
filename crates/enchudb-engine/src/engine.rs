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
    pub wal_head: u64,
    /// 本体へ反映 + fsync 済みの位置
    pub wal_checkpoint: u64,
    /// WAL ファイル容量(設定値、sparse)
    pub wal_capacity: u64,
    /// head - checkpoint。大きいと未 fsync が溜まっている
    pub wal_lag_bytes: u64,
    /// 発行済みの最大 LSN
    pub wal_next_lsn: u64,
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
    pub max_hlc: Option<enchudb_wal::Hlc>,
}

fn wal_path_for(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.wal", path))
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
        out.extend_from_slice(&t.next_local.to_le_bytes());
        out.extend_from_slice(&(t.himo_ids.len() as u32).to_le_bytes());
        for &h in &t.himo_ids {
            out.extend_from_slice(&h.to_le_bytes());
        }
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
    let mut tables = Vec::with_capacity(table_count);
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
        let next_local = read_u32!(buf, off);
        let himo_count = read_u32!(buf, off) as usize;
        let mut himo_ids = Vec::with_capacity(himo_count);
        for _ in 0..himo_count {
            himo_ids.push(read_u32!(buf, off));
        }
        let fk_count = read_u32!(buf, off) as usize;
        let mut fk_refs = Vec::with_capacity(fk_count);
        for _ in 0..fk_count {
            let hid = read_u32!(buf, off);
            let tid = read_u32!(buf, off);
            fk_refs.push((hid, tid as TableId));
        }
        tables.push(TableDef {
            name,
            himo_ids,
            eid_range_lo,
            eid_range_hi,
            fk_refs,
            next_local,
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

/// writer 排他用 sidecar の path。 `.db.lock`。
#[cfg(not(target_arch = "wasm32"))]
fn writer_lock_path_for(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.lock", path))
}

/// `wal_record_queue` (= bounded ArrayQueue) への blocking push。
/// queue 満杯時は `yield_now` で consumer の進捗を待つ (issue4 backpressure)。
#[inline]
fn push_wal_record_blocking(
    wq: &crossbeam_queue::ArrayQueue<enchudb_wal::wal::WalRecord>,
    rec: enchudb_wal::wal::WalRecord,
) {
    let mut rec = rec;
    loop {
        match wq.push(rec) {
            Ok(()) => return,
            Err(returned) => {
                rec = returned;
                std::thread::yield_now();
            }
        }
    }
}

/// `.db.lock` に flock(LOCK_EX) を取り、 fd を返す。 fd が drop されると lock も解放。
/// 取得できないと **block する** (= sqlite と同様、 取れるまで待つ)。
/// readonly open は呼ばない。 writer 系の open / create だけ呼ぶ。
#[cfg(not(target_arch = "wasm32"))]
fn acquire_writer_lock(path: &str) -> io::Result<std::fs::File> {
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
    let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(f)
}

/// `snapshot_export` の結果。どのファイルを書き出したか。
#[derive(Debug, Clone)]
pub struct SnapshotFiles {
    pub main: String,
    pub wal: Option<String>,
    pub crc: Option<String>,
}

/// `audit()` に渡すフィルタ条件。None は「そのフィールドで絞らない」。
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// HLC 下限(inclusive)。これより古い record は除外。
    pub from_hlc: Option<enchudb_wal::Hlc>,
    /// HLC 上限(inclusive)。これより新しい record は除外。
    pub to_hlc: Option<enchudb_wal::Hlc>,
    /// 書き手 peer_id。これ以外の author を除外。
    pub author_peer: Option<enchudb_wal::PeerId>,
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

    /// growable backing のときに commit を要求サイズまで伸ばす。
    /// 他の variant では no-op (ヘッダ上限で既に確保済み)。
    #[cfg(not(target_arch = "wasm32"))]
    fn ensure_committed(&self, end: usize) -> io::Result<()> {
        match self {
            Backing::Growable(g) => g.grow_amortized(end),
            _ => Ok(()),
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
use crate::vocabulary::Vocabulary;
use crate::entity_set::EntitySet;
use crate::himo_store::{HimoStore, HimoType};
use crate::content_store::ContentStore;
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
const FILE_VERSION: u32 = 5;
/// v4 DB を後方互換で open する識別子。 v4 → v5 migrate は open 時透過。
const FILE_VERSION_LEGACY_V4: u32 = 4;
const HEADER_SIZE: usize = 4096;

const DEFAULT_MAX_ENTITIES: u32 = 16_777_216;
const DEFAULT_MAX_HIMOS: u32 = 256;
const DEFAULT_CYL_MAX_VALUES: u32 = 65536;
const DEFAULT_VOCAB_DATA_SIZE: usize = 512 * 1024 * 1024;

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

const BACKING_KIND_EAGER: u32 = 0;
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
/// himo_types/max_values 領域は runtime で変動するので CRC 範囲外。

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
    himo_base_off: usize,
    himo_col_size: usize,
    #[allow(dead_code)]
    himo_cyl_size: usize,
    himo_slot_size: usize,
    cyl_max_values: u32,
    total_size: usize,
}

impl Layout {
    fn compute(max_entities: u32, max_himos: u32, vocab_data_size: usize, content_data_size: Option<usize>, cyl_max_values: Option<u32>) -> Self {
        let vocab_max_entries = max_entities.saturating_mul(16).min(256_000_000);
        Self::compute_with_caps(
            max_entities, max_himos,
            vocab_max_entries, vocab_data_size,
            content_data_size, cyl_max_values,
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
    ) -> Self {
        let vocab_index_cap = vocab_max_entries.next_power_of_two();
        let himoreg_max_entries = max_himos.max(256);
        let himoreg_index_cap = (himoreg_max_entries * 2).next_power_of_two();
        let himoreg_data_size = 64 * 1024;
        let content_data_size = content_data_size.unwrap_or_else(ContentStore::data_region_size);
        let cyl_max_values = cyl_max_values.unwrap_or(DEFAULT_CYL_MAX_VALUES);

        Self::from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, cyl_max_values,
        )
    }

    fn from_params(
        max_entities: u32, max_himos: u32,
        vocab_max_entries: u32, vocab_index_cap: u32, vocab_data_size: usize,
        himoreg_max_entries: u32, himoreg_index_cap: u32, himoreg_data_size: usize,
        content_data_size: usize, cyl_max_values: u32,
    ) -> Self {
        // v3 layout: 固定上限の region 群を前に、 append-only な
        // variable region 群 (vocab_data / himoreg_data / content_data)
        // を末尾に集める。 これで「ファイル末尾のみ伸びる」 monotonic
        // grow が実現でき、 sparse hole に頼らずに apparent size を
        // 実 usage に追従させられる (Phase B Step 1)。
        let mut off = HEADER_SIZE;

        // ── 固定 cluster: 上限が max_entities / max_himos / *_cap で決まる ──
        let entities_off = off;
        let entities_size = align8(EntitySet::region_size(max_entities));
        off += entities_size;

        let vocab_offsets_off = off;
        let vocab_offsets_size = align8(Vocabulary::offsets_region_size(vocab_max_entries));
        off += vocab_offsets_size;

        let vocab_index_off = off;
        let vocab_index_size = align8(Vocabulary::index_region_size(vocab_index_cap));
        off += vocab_index_size;

        let himoreg_offsets_off = off;
        let himoreg_offsets_size = align8(Vocabulary::offsets_region_size(himoreg_max_entries));
        off += himoreg_offsets_size;

        let himoreg_index_off = off;
        let himoreg_index_size = align8(Vocabulary::index_region_size(himoreg_index_cap));
        off += himoreg_index_size;

        let content_index_off = off;
        let content_index_size = align8(ContentStore::index_region_size_for(max_entities));
        off += content_index_size;

        let himo_col_size = align8(Column::region_size(max_entities, 4));
        let himo_cyl_size = 0usize;
        let himo_slot_size = himo_col_size;

        let himo_base_off = off;
        off += himo_slot_size * (max_himos as usize);

        // ── Variable cluster (tail): append-only で伸びる region 群 ──
        let vocab_data_off = off;
        let vocab_data_size = align8(Vocabulary::data_region_size(vocab_data_size));
        off += vocab_data_size;

        let himoreg_data_off = off;
        let himoreg_data_size = align8(Vocabulary::data_region_size(himoreg_data_size));
        off += himoreg_data_size;

        let content_data_off = off;
        let content_data_size = align8(content_data_size);
        off += content_data_size;

        Layout {
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
            himo_base_off, himo_col_size, himo_cyl_size, himo_slot_size,
            cyl_max_values,
            total_size: off,
        }
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
#[derive(Clone, Debug)]
pub(crate) struct TableDef {
    /// table 名 (anonymous は ""、 user 定義 table は固有名)。
    pub name: String,
    /// この table の column 軸を成す himo の id 列 (engine.himos の index)。
    pub himo_ids: Vec<u32>,
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
    pub next_local: u32,
}

impl TableDef {
    /// 起動時の anonymous table。 全 himo / entity の default 受け皿。
    /// `eid_range_hi == u32::MAX` の間は open-ended (旧 API で `entity()` が
    /// 呼べる)、 `define_table` で初めて非 anon table が切られた瞬間に
    /// 現 `next_eid` で閉じる。
    pub fn anonymous() -> Self {
        Self {
            name: String::new(),
            himo_ids: Vec::new(),
            eid_range_lo: 0,
            eid_range_hi: u32::MAX,
            fk_refs: Vec::new(),
            next_local: 0,
        }
    }
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
    himo_names: Vec<String>,
    himo_types: Vec<HimoType>,
    himo_max_values: Vec<u32>,
    himos: Vec<HimoStore>,
    /// β-light step 2: engine が認知する table 一覧。 index 0 は常に
    /// anonymous table (旧 API は全部ここに dispatch)。 step 3+ で
    /// define_table 時に push される。
    tables: Vec<TableDef>,
    /// β-light step 2: himo_id → 所属 table_id の逆引き (himos と同じ index)。
    /// 旧 API (`define_himo`) で追加された himo は全部 ANONYMOUS_TABLE。
    himo_to_table: Vec<TableId>,
    entities: EntitySet,
    contents: ContentStore,
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
    /// v28 WAL。`create_concurrent_with_wal` or `open_concurrent_with_wal` で有効化。
    /// Some なら tie_async/untie_async/delete_async は wal_record_queue 経由で
    /// consumer thread 側に batch flush を委ねる。
    wal: Option<std::sync::Arc<enchudb_wal::wal::Wal>>,
    /// async path 専用の WAL record queue。 writer は WAL に直接書かず、 ここに
    /// owned record を push。 consumer thread が drain して `wal.append_many` で
    /// 1 flock サイクル N records にまとめる (per-record flock コスト償却)。
    wal_record_queue: Option<std::sync::Arc<crossbeam_queue::ArrayQueue<enchudb_wal::wal::WalRecord>>>,
    /// 背景 fsync が最後に completed した LSN。
    durable_lsn: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// v32: この Engine を所有する peer の id。分散時 eid の上位 32bit。
    peer_id: std::sync::atomic::AtomicU32,
    /// v32: LWW 用に (eid, himo) → 最後の HLC を記録。
    hlc_store: std::sync::Arc<crate::hlc_store::HlcStore>,
    /// v32 Phase C: 自 peer の ed25519 鍵ペア。None なら署名しない/検証もしない。
    keypair: std::sync::RwLock<Option<std::sync::Arc<enchudb_wal::keys::Keypair>>>,
    /// v32 Phase C: 他 peer の pubkey TOFU ストア。Syncer が verify に使う。
    pubkeys: std::sync::Arc<enchudb_wal::keys::PubkeyStore>,
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
    /// v33: 受信した Vocab op の `(author_peer, remote_vid) → local_vid` mapping。
    /// Symbol 型 himo の Tie を受信した時に remote_vid を local_vid に変換して apply する。
    /// peer-local に保持(replica でも独立、open 時は空、受信で徐々に埋まる)。
    peer_vocab_map: std::sync::RwLock<std::collections::HashMap<(enchudb_wal::PeerId, u32), u32>>,
    /// v34: read-only モード。 true なら書き込み API は error/panic。 open_readonly で立つ。
    /// `is_replica` は「直 write 拒否、 Syncer 経由は受ける」、 こちらは「一切 write 不可」。
    is_readonly: std::sync::atomic::AtomicBool,
    /// v34: writer lock の保持 fd。 open_writer / create_* で `.db.lock` sidecar を
    /// flock(LOCK_EX)、 Engine drop で fd close = lock release。 readonly では None。
    /// 多 process write の同 .db 競合を防ぐ (sqlite WAL モード相当)。
    #[cfg(not(target_arch = "wasm32"))]
    _writer_lock: Option<std::fs::File>,
    /// β-heavy phase 1: 各 himo の PositionsRegion を mmap-back する sidecar。
    /// Some なら HimoStore::init_with_positions / load_with_positions を使う
    /// Region mode、 None なら旧来の Heap mode (from_bytes / wasm 等)。
    #[cfg(not(target_arch = "wasm32"))]
    positions_sidecar: Option<std::sync::Arc<crate::positions_sidecar::PositionsSidecar>>,
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
        Self::create_concurrent_with_wal(path, 16 * 1024 * 1024)
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
        let vds = vocab_data_size.unwrap_or(DEFAULT_VOCAB_DATA_SIZE);
        let max_himos = max_himos.unwrap_or(DEFAULT_MAX_HIMOS);
        let layout = Layout::compute(max_entities, max_himos, vds, content_data_size, cyl_max_values);

        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        // writer lock を先に取る (= 他 writer が居れば block)。 create も書き込みなので必須。
        let writer_lock = acquire_writer_lock(path)?;
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
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

        // v28: ヘッダ整合性 CRC
        write_header_crc(&mut mmap);

        // β-heavy phase 1: positions sidecar も同時に新規作成。
        let positions_sidecar = std::sync::Arc::new(
            crate::positions_sidecar::PositionsSidecar::create(path, max_entities, max_himos)?,
        );

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

        Ok(Self {
            path: path.to_string(), layout, max_entities, max_himos,
            vocab, himo_reg,
            himo_names: Vec::new(),
            himo_types: Vec::new(), himo_max_values: Vec::new(),
            himos: Vec::new(), entities, contents,
            tables: vec![TableDef::anonymous()],
            himo_to_table: Vec::new(),
            write_queue: None,
            shutdown_flag: None,
            consumer_handle: std::sync::Mutex::new(None),
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal: None,
            wal_record_queue: None,
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            keypair: std::sync::RwLock::new(None),
            pubkeys: std::sync::Arc::new(enchudb_wal::keys::PubkeyStore::new()),
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            gossip_remote_apply: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_wal::wal::HEADER_SIZE as u64,
            )),
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            is_readonly: std::sync::atomic::AtomicBool::new(false),
            _writer_lock: Some(writer_lock),
            positions_sidecar: Some(positions_sidecar),
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
        let layout = Layout::compute(max_entities, max_himos, DEFAULT_VOCAB_DATA_SIZE, None, None);
        Self::create_growable_full(path, layout, max_entities, max_himos)
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
        );
        Self::create_growable_full(path, layout, max_entities, max_himos)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_growable_full(
        path: &str,
        layout: Layout,
        max_entities: u32,
        max_himos: u32,
    ) -> io::Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        // writer lock を先に取る (= 他 writer が居れば block)
        let writer_lock = acquire_writer_lock(path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

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
        write_header_crc(header);

        // β-heavy phase 1: positions sidecar も同時に新規作成。
        let positions_sidecar = std::sync::Arc::new(
            crate::positions_sidecar::PositionsSidecar::create(path, max_entities, max_himos)?,
        );

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

        Ok(Self {
            path: path.to_string(),
            layout,
            max_entities,
            max_himos,
            vocab,
            himo_reg,
            himo_names: Vec::new(),
            himo_types: Vec::new(),
            himo_max_values: Vec::new(),
            himos: Vec::new(),
            entities,
            contents,
            tables: vec![TableDef::anonymous()],
            himo_to_table: Vec::new(),
            write_queue: None,
            shutdown_flag: None,
            consumer_handle: std::sync::Mutex::new(None),
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal: None,
            wal_record_queue: None,
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            keypair: std::sync::RwLock::new(None),
            pubkeys: std::sync::Arc::new(enchudb_wal::keys::PubkeyStore::new()),
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            gossip_remote_apply: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_wal::wal::HEADER_SIZE as u64,
            )),
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            is_readonly: std::sync::atomic::AtomicBool::new(false),
            _writer_lock: Some(writer_lock),
            positions_sidecar: Some(positions_sidecar),
            backing: Backing::Growable(map),
        })
    }

    /// 現行の単独 Engine を開く(WAL 無し、&mut self で mutation)。
    /// v32 以前: `Engine::open` の別名。
    /// v33 以降: `Engine::open` が WAL 付き `Arc<Self>` を返すようになるため、
    /// 明示的に単独 Engine が欲しい場合はこちらを使う。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_standalone(path: &str) -> io::Result<Self> {
        Self::open_internal(path, /*verify_region_crc=*/ true, /*take_lock=*/ true)
    }

    /// read-only open: writer lock を取らず、 書き込み API は error。
    /// 複数 process で同時に呼んで OK (writer と共存可能、 reader 同士も無制限)。
    /// 用途: GUI の表示専用 process、 監視ツール、 backup-reader 等。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_readonly(path: &str) -> io::Result<Self> {
        let mut eng = Self::open_internal(path, /*verify_region_crc=*/ true, /*take_lock=*/ false)?;
        eng.is_readonly.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// `open` は WAL 有効 + `Arc<Self>` 返し。
    /// 旧挙動は `open_standalone` で取れる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &str) -> io::Result<std::sync::Arc<Self>> {
        Self::open_concurrent_with_wal(path, 16 * 1024 * 1024)
    }

    /// 内部用: region CRC 検証を skip できる open。WAL ルート用。
    /// `take_lock = true` で `.db.lock` の flock(LOCK_EX) を取得し、 Engine 寿命中保持。
    #[cfg(not(target_arch = "wasm32"))]
    fn open_internal(path: &str, verify_region_crc: bool, take_lock: bool) -> io::Result<Self> {
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
        let mut eng = Self::load_from_backing(Backing::Mmap(mmap))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        eng.path = path.to_string();
        eng._writer_lock = writer_lock;

        // β-light step 7: tables sidecar の読み込み。 不在なら anonymous fallback
        // (= load_from_backing が既に行った形のまま) で v4 DB 互換。
        if let Ok(Some(persisted)) = load_tables_from_sidecar(path) {
            eng.adopt_persisted_tables(persisted);
        }

        // β-heavy phase 1: positions sidecar を attach。 既存 HimoStore は
        // Heap mode で load された (cyl_built=false) ので、 ここで Region mode
        // に置き換える。 column scan による lazy rebuild は次の cyl 触りで起きる。
        eng.attach_positions_sidecar()?;

        if verify_region_crc {
            // .crc ファイルがあれば全 region CRC 検証
            eng.verify_region_crcs()?;
        }
        Ok(eng)
    }

    /// β-heavy phase 1: positions sidecar を open_or_create して、 既存
    /// HimoStore (Heap mode で load されたもの) を Region mode に置き換える。
    /// 各 slot は magic check で 「既に valid」 / 「fresh」 を判定し、 fresh なら
    /// PositionsRegion::init() を呼ぶ。 lazy rebuild は次の cyl 触りで走る。
    #[cfg(not(target_arch = "wasm32"))]
    fn attach_positions_sidecar(&mut self) -> io::Result<()> {
        if self.path.is_empty() {
            return Ok(());
        }
        let sidecar = std::sync::Arc::new(
            crate::positions_sidecar::PositionsSidecar::open_or_create(
                &self.path,
                self.max_entities,
                self.max_himos,
            )?,
        );

        // 既存の HimoStore を Region mode で再構築。 col_region は backing から
        // 切り直す (同じ memory を指す)。 type / max_values は self に保存済み。
        #[cfg(not(target_arch = "wasm32"))]
        let grower = self.backing.grower();
        let base = self.backing.as_mut_ptr();
        let cyl_max_values = self.layout.cyl_max_values;

        for hid in 0..self.himos.len() {
            let col_off = self.layout.himo_col_off(hid);
            let col_size = self.layout.himo_col_size;
            let col_region = {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(g) = &grower {
                    unsafe { Region::with_grower(g.clone(), col_off, col_size) }
                } else {
                    unsafe { Region::new(base.add(col_off), col_size) }
                }
                #[cfg(target_arch = "wasm32")]
                unsafe { Region::new(base.add(col_off), col_size) }
            };
            let pos_region = sidecar.region_for(hid as u32);
            let positions =
                if crate::positions_region::PositionsRegion::has_valid_magic(&pos_region) {
                    crate::positions_region::PositionsRegion::load(pos_region)
                } else {
                    crate::positions_region::PositionsRegion::init_with_offset(pos_region, 0)
                };
            let ht = self.himo_types[hid];
            let mv = self.himo_max_values[hid].min(cyl_max_values);
            self.himos[hid] = HimoStore::load_with_positions(col_region, ht, mv, positions);
        }
        self.positions_sidecar = Some(sidecar);
        Ok(())
    }

    /// 全 region の CRC テーブルを計算する。
    fn compute_region_crc_table(&self) -> crate::integrity::CrcTable {
        use crate::integrity::{CrcTable, RegionKind, fnv1a_region};
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
        for hid in 0..self.himo_types.len() {
            let off = self.layout.himo_col_off(hid);
            let end = off + self.layout.himo_col_size;
            let crc = fnv1a_region(&buf[off..end]);
            table.set(RegionKind::HimoColumn(hid as u32), crc);
        }
        // vocab data
        let off = self.layout.vocab_data_off;
        let end = off + self.layout.vocab_data_size;
        table.set(RegionKind::Vocab, fnv1a_region(&buf[off..end]));
        // himoreg data
        let off = self.layout.himoreg_data_off;
        let end = off + self.layout.himoreg_data_size;
        table.set(RegionKind::HimoReg, fnv1a_region(&buf[off..end]));
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
        let vocab_max_entries = u32::from_le_bytes(buf[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4].try_into().unwrap());
        let vocab_index_cap = u32::from_le_bytes(buf[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4].try_into().unwrap());
        let vocab_data_size = u64::from_le_bytes(buf[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let himoreg_max_entries = u32::from_le_bytes(buf[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4].try_into().unwrap());
        let himoreg_index_cap = u32::from_le_bytes(buf[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4].try_into().unwrap());
        let himoreg_data_size = u64::from_le_bytes(buf[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let content_data_size = u64::from_le_bytes(buf[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let cyl_max_values = u32::from_le_bytes(buf[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].try_into().unwrap());

        let layout = Layout::from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, cyl_max_values,
        );

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
        Self::load_from_backing(Backing::Memory(data))
    }

    fn load_from_backing(mut backing: Backing) -> Result<Self, String> {
        let buf = backing.as_slice_mut();

        if buf.len() < HEADER_SIZE || buf[H_MAGIC..H_MAGIC + 4] != FILE_MAGIC {
            return Err("not an EnchuDB file".into());
        }
        let version = u32::from_le_bytes(buf[H_VERSION..H_VERSION + 4].try_into().unwrap());
        // v5 が現行、 v4 は後方互換で受け付ける (= anonymous table 1 個に migrate)。
        // v4 DB を v5 として open しても、 引き続き flat anonymous モードで動作する
        // (table 概念は step 2 以降の実装に伴って活性化)。
        if version != FILE_VERSION && version != FILE_VERSION_LEGACY_V4 {
            return Err(format!(
                "unsupported EnchuDB file version {} (supported: {}, {} compat). dev phase — recreate the DB.",
                version, FILE_VERSION, FILE_VERSION_LEGACY_V4
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

        let layout = Layout::from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, cyl_max_values,
        );

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
        let mut t = std::time::Instant::now();
        let mut p = pr();
        let mut report = |label: &str, t: &mut std::time::Instant, p: &mut u64| {
            if profile {
                let np = pr();
                let dp = np - *p;
                eprintln!("[open_profile] {:>22}  Δreclaim={:>7}  Δt={:>5} ms", label, dp, t.elapsed().as_millis());
                *p = np;
                *t = std::time::Instant::now();
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
        );
        report("Vocabulary::load", &mut t, &mut p);
        let himo_reg = Vocabulary::load(
            unsafe { Region::new(base.add(layout.himoreg_data_off), layout.himoreg_data_size) },
            unsafe { Region::new(base.add(layout.himoreg_offsets_off), layout.himoreg_offsets_size) },
            unsafe { Region::new(base.add(layout.himoreg_index_off), layout.himoreg_index_size) },
        );
        report("himo_reg(Vocabulary)", &mut t, &mut p);
        let contents = ContentStore::load(
            unsafe { Region::new(base.add(layout.content_index_off), layout.content_index_size) },
            unsafe { Region::new(base.add(layout.content_data_off), layout.content_data_size) },
        );
        report("ContentStore::load", &mut t, &mut p);

        let mut himo_names = Vec::new();
        let mut himo_types = Vec::new();
        let mut himo_max_values = Vec::new();
        let mut himos = Vec::new();

        for hid in 0..himo_count as usize {
            let ht = HimoType::from_byte(type_bytes[hid]);
            let mv = maxv_values[hid];
            let name_bytes = himo_reg.get(hid as u32);
            let name = String::from_utf8_lossy(name_bytes).to_string();
            let effective_mv = mv.min(cyl_max_values);

            let hs = HimoStore::load(
                unsafe { Region::new(base.add(layout.himo_col_off(hid)), layout.himo_col_size) },
                ht, effective_mv,
            );

            himo_names.push(name);
            himo_types.push(ht);
            himo_max_values.push(mv);
            himos.push(hs);
        }
        report("HimoStore::load × N", &mut t, &mut p);

        // β-light step 2: load 時は全 himo を anonymous table に attach する。
        // step 3+ で v5 DB の table descriptor 読み出しに置き換える、 v4 DB は
        // 引き続きこの compat 経路で anonymous-only として open される。
        let initial_tables = vec![{
            let mut anon = TableDef::anonymous();
            anon.himo_ids = (0..himos.len() as u32).collect();
            anon
        }];
        let initial_himo_to_table = vec![ANONYMOUS_TABLE; himos.len()];

        let mut eng = Self {
            path: String::new(), layout, max_entities, max_himos,
            vocab, himo_reg,
            himo_names, himo_types, himo_max_values,
            himos, entities, contents,
            tables: initial_tables,
            himo_to_table: initial_himo_to_table,
            write_queue: None,
            shutdown_flag: None,
            consumer_handle: std::sync::Mutex::new(None),
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal: None,
            wal_record_queue: None,
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            keypair: std::sync::RwLock::new(None),
            pubkeys: std::sync::Arc::new(enchudb_wal::keys::PubkeyStore::new()),
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            gossip_remote_apply: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_wal::wal::HEADER_SIZE as u64,
            )),
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            is_readonly: std::sync::atomic::AtomicBool::new(false),
            _writer_lock: None, // caller (open_internal) が後から差し替える
            // β-heavy phase 1: 後から open_internal が attach する。 from_bytes /
            // wasm のような path 無し経路は None のまま (Heap mode で動く)。
            #[cfg(not(target_arch = "wasm32"))]
            positions_sidecar: None,
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
        eng.vocab.mark_index_clean(false);
        eng.himo_reg.mark_index_clean(false);
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = eng.backing.flush_range(eng.layout.vocab_data_off, 16);
            let _ = eng.backing.flush_range(eng.layout.himoreg_data_off, 16);
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
    pub fn open_concurrent_replica(path: &str, wal_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::open_concurrent_with_wal(path, wal_capacity)?;
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
    pub fn define_table(&mut self, name: &str, size_hint: u32) -> Result<TableId, String> {
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
            himo_ids: Vec::new(),
            eid_range_lo: new_lo,
            eid_range_hi: new_hi,
            fk_refs: Vec::new(),
            next_local: 0,
        });
        self.try_persist_tables();
        Ok(tid)
    }

    /// 指定 table 内に entity を割り当てる。 anonymous table 名は受け付けず、
    /// 旧来の `entity()` を使う必要がある (互換維持)。
    ///
    /// 並行性: `&mut self` なので writer は単一。 同時に走る `tie_async` 等
    /// concurrent reader/writer 経路とは EntitySet の CAS で安全に共存する。
    pub fn entity_in(&mut self, table_name: &str) -> Result<enchudb_wal::EntityId, String> {
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
        let table = &mut self.tables[tid];

        // capacity check
        let table_size = table.eid_range_hi - table.eid_range_lo;
        if table.next_local >= table_size {
            return Err(format!(
                "table '{}' eid range exhausted ({} eids reserved)",
                table_name, table_size,
            ));
        }
        let global = table.eid_range_lo + table.next_local;
        table.next_local += 1;

        // EntitySet で live mark + next_eid 前進 (CAS safe)
        self.entities.allocate_at(global);

        // concurrent mode barrier (entity() と同じ)
        if let Some(q) = self.write_queue.as_ref() {
            q.push(crate::write_queue::Op::EntityCreated { local: global });
            self.push_count.fetch_add(1, Ordering::Release);
        }
        let peer = self.peer_id.load(Ordering::Acquire);
        Ok(enchudb_wal::make_eid(peer, global))
    }

    /// β-light step 7: 現 tables Vec を sidecar に保存する (best effort)。
    /// memory-only (from_bytes) や wasm では no-op。 path が空なら skip。
    fn try_persist_tables(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if !self.path.is_empty() {
                if let Err(e) = persist_tables_to_sidecar(&self.path, &self.tables) {
                    // best effort: panic せずログだけ。 user table の定義は
                    // メモリには反映されてる、 次回 reopen で失われるだけ。
                    eprintln!("warning: failed to persist tables sidecar: {}", e);
                }
            }
        }
    }

    /// β-light step 7: sidecar から復元した tables を採用、 himo_to_table も
    /// それに合わせて再構築する。 load_from_backing 後 caller で path 設定後に呼ぶ。
    #[cfg(not(target_arch = "wasm32"))]
    fn adopt_persisted_tables(&mut self, tables: Vec<TableDef>) {
        let himo_count = self.himos.len();
        // 全 himo を一旦 anonymous default に戻し、 sidecar に書かれてる
        // attach を上書きする (= sidecar が source of truth)
        self.himo_to_table = vec![ANONYMOUS_TABLE; himo_count];
        for (tid, table) in tables.iter().enumerate() {
            for &hid in &table.himo_ids {
                if (hid as usize) < himo_count {
                    self.himo_to_table[hid as usize] = tid as TableId;
                }
            }
        }
        self.tables = tables;
    }

    /// 既知 table を `(id, name, eid_range)` で列挙する。 試験用 / debug 用 API。
    pub fn list_tables(&self) -> Vec<(TableId, String, u32, u32)> {
        self.tables
            .iter()
            .enumerate()
            .map(|(i, t)| (i as TableId, t.name.clone(), t.eid_range_lo, t.eid_range_hi))
            .collect()
    }

    // ──── entity ────

    pub fn entity(&self) -> enchudb_wal::EntityId {
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
        enchudb_wal::make_eid(peer, local)
    }

    pub fn entities(&self) -> Vec<enchudb_wal::EntityId> {
        let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
        self.entities.iter().into_iter()
            .map(|local| enchudb_wal::make_eid(peer, local))
            .collect()
    }
    pub fn entity_count(&self) -> u32 { self.entities.count() }
    pub fn next_eid(&self) -> enchudb_wal::EntityId {
        let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
        enchudb_wal::make_eid(peer, self.entities.next_eid())
    }

    // ──── v32: peer_id ────

    /// この Engine を所有する peer の id を設定。WAL / DB header にも反映される。
    /// 起動時に 1 回だけ呼ぶ想定。
    pub fn set_peer_id(&self, peer: enchudb_wal::PeerId) {
        self.peer_id.store(peer, std::sync::atomic::Ordering::Release);
        // mmap の header に即書き込み(CRC 保護外なので再計算不要)
        self.backing.header_mut()[H_PEER_ID..H_PEER_ID + 4]
            .copy_from_slice(&peer.to_le_bytes());
        if let Some(wal) = self.wal.as_ref() {
            wal.set_peer_id(peer);
        }
    }

    /// 現在の peer id。
    pub fn peer_id(&self) -> enchudb_wal::PeerId {
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

    /// Phase C: 自 peer の鍵ペアを設定。WAL にも反映される。None で署名 off。
    pub fn set_keypair(&self, kp: Option<std::sync::Arc<enchudb_wal::keys::Keypair>>) {
        *self.keypair.write().unwrap() = kp.clone();
        if let Some(wal) = self.wal.as_ref() {
            wal.set_keypair(kp.clone());
        }
        if let Some(ref k) = kp {
            let peer = self.peer_id();
            self.pubkeys.force_register(peer, &k.public_bytes());
        }
    }

    /// Phase C: 他 peer の pubkey ストアへの参照。
    pub fn pubkeys(&self) -> &std::sync::Arc<enchudb_wal::keys::PubkeyStore> {
        &self.pubkeys
    }

    /// Phase C: ACL への参照。Syncer が受信 op の author を enforce する。
    pub fn acl(&self) -> &std::sync::Arc<crate::acl::Acl> {
        &self.acl
    }

    /// Phase C: 自 peer の鍵ペアを返す。
    pub fn keypair(&self) -> Option<std::sync::Arc<enchudb_wal::keys::Keypair>> {
        self.keypair.read().unwrap().clone()
    }

    /// WAL への参照(sync モジュールが publish に使う)。
    pub fn wal_arc(&self) -> Option<std::sync::Arc<enchudb_wal::wal::Wal>> {
        self.wal.clone()
    }

    // ──── v32: リモート peer から pull したレコードを apply する ────
    // これらは LWW 判定を通った後の無条件 apply。HlcStore は Syncer が先に更新済み。

    /// リモート peer から届いた Tie を apply。
    /// `gossip_remote_apply` 有効かつ `relayed` が `Some` なら、 同じ op を
    /// `append_relayed` で自分の WAL にも記録 (HLC/author/署名は元のまま)。
    /// `relayed` は WAL 受信時の元 header (sync 側で WireRecord から作って渡す)。
    pub fn remote_tie_apply(
        &self,
        eid: enchudb_wal::EntityId,
        himo_id: u16,
        value: u32,
        relayed: Option<enchudb_wal::wal::RelayedHeader>,
    ) {
        let local = enchudb_wal::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return; }
        // entity 未確保ならローカル側の EntitySet に登録(eid は peer 側が決めた値)
        self.entities.ensure_live(local);
        self.himos[hid].set(local, value);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.wal.as_ref(), relayed) {
                let _ = wal.append_relayed(
                    enchudb_wal::wal::WalOp::Tie { eid, himo_id, value },
                    h,
                );
            }
        }
    }

    /// リモート peer から届いた Untie を apply。
    pub fn remote_untie_apply(
        &self,
        eid: enchudb_wal::EntityId,
        himo_id: u16,
        relayed: Option<enchudb_wal::wal::RelayedHeader>,
    ) {
        let local = enchudb_wal::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return; }
        self.himos[hid].remove(local);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.wal.as_ref(), relayed) {
                let _ = wal.append_relayed(
                    enchudb_wal::wal::WalOp::Untie { eid, himo_id },
                    h,
                );
            }
        }
    }

    /// リモート peer から届いた Delete を apply。
    pub fn remote_delete_apply(
        &self,
        eid: enchudb_wal::EntityId,
        relayed: Option<enchudb_wal::wal::RelayedHeader>,
    ) {
        let local = enchudb_wal::eid_local(eid);
        for hid in 0..self.himos.len() {
            self.himos[hid].remove(local);
        }
        self.entities.free(local);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.wal.as_ref(), relayed) {
                let _ = wal.append_relayed(enchudb_wal::wal::WalOp::Delete { eid }, h);
            }
        }
    }

    /// リモート peer から届いた Content 書き込みを apply。
    pub fn remote_content_apply(
        &self,
        eid: enchudb_wal::EntityId,
        key: &str,
        data: &[u8],
        relayed: Option<enchudb_wal::wal::RelayedHeader>,
    ) {
        let local = enchudb_wal::eid_local(eid);
        self.entities.ensure_live(local);
        self.contents.set(local, key, data);
        if self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.wal.as_ref(), relayed) {
                let _ = wal.append_relayed(
                    enchudb_wal::wal::WalOp::Content { eid, key, data },
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
        author_peer: enchudb_wal::PeerId,
        remote_vid: u32,
        bytes: &[u8],
        relayed: Option<enchudb_wal::wal::RelayedHeader>,
    ) {
        let was_new = self.vocab.lookup(bytes).is_none();
        let local_vid = self.vocab.get_or_insert(bytes);
        let mut map = self.peer_vocab_map.write().unwrap();
        map.insert((author_peer, remote_vid), local_vid);
        if was_new && self.gossip_remote_apply() {
            if let (Some(wal), Some(h)) = (self.wal.as_ref(), relayed) {
                // 自 vocab の vid を貼り直し (Tie 受信側が translate_remote_vid で再翻訳)。
                let _ = wal.append_relayed(
                    enchudb_wal::wal::WalOp::Vocab { vid: local_vid, bytes },
                    h,
                );
            }
        }
    }

    /// v33: Symbol 型 himo の Tie を受信した際、remote vid を local vid に変換する。
    /// Symbol 以外の himo、または mapping 未登録なら元値をそのまま返す。
    pub fn translate_remote_vid(&self, author_peer: enchudb_wal::PeerId, himo_id: u16, value: u32) -> u32 {
        let hid = himo_id as usize;
        if hid >= self.himo_types.len() { return value; }
        // Tag / Leaf どちらも vocab 経由なので vid 翻訳が必要。
        match self.himo_types[hid] {
            HimoType::Tag | HimoType::Leaf => {
                let map = self.peer_vocab_map.read().unwrap();
                *map.get(&(author_peer, value)).unwrap_or(&value)
            }
            _ => value,
        }
    }

    // ──── tie ────

    pub fn define_himo(&mut self, himo: &str, ht: HimoType, max_values: u32) {
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
        ht: HimoType,
        max_values: u32,
    ) -> Result<u32, String> {
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

        // ensure_himo は既に存在ならそのまま、 新規なら anonymous へ attach する。
        // 後者なら anonymous の himo_ids から外して target table へ移す。
        let hid = self.ensure_himo(&full_name, ht, max_values);
        let hid_u32 = hid as u32;

        // 既に target table に attach 済みなら何もしない (重複 define_himo_in)
        if self.himo_to_table[hid] == tid as TableId {
            return Ok(hid_u32);
        }

        // ensure_himo は新規時に ANONYMOUS_TABLE へ attach するので、 別 table
        // 既属の場合は migrate する形 (実用上は新規時のみ通る)。
        let cur_tid = self.himo_to_table[hid];
        self.tables[cur_tid as usize]
            .himo_ids
            .retain(|&h| h != hid_u32);
        self.tables[tid].himo_ids.push(hid_u32);
        self.himo_to_table[hid] = tid as TableId;

        self.try_persist_tables();
        Ok(hid_u32)
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
        let hid = self.define_himo_in(table_name, himo_name, HimoType::Ref, 0)?;

        // 所属 table の fk_refs に entry を追加 (idempotent)
        let owner_tid = self.himo_to_table[hid as usize];
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
        if hid >= self.himo_to_table.len() {
            return;
        }
        let tid = self.himo_to_table[hid] as usize;
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
    ///   - 非 Ref himo は最初の `himo_types[hid] != Ref` で即 return (~1 ns)
    ///   - Ref himo は fk_refs (typically 1-5 件) の線形検索 (~5-10 ns)
    #[inline(always)]
    fn validate_ref_tie(&self, hid: usize, target_eid: u32) {
        if hid >= self.himo_types.len() {
            return;
        }
        if self.himo_types[hid] != HimoType::Ref {
            return;
        }
        if hid >= self.himo_to_table.len() {
            return;
        }
        let owner_tid = self.himo_to_table[hid] as usize;
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

    pub fn tie_text(&mut self, eid: enchudb_wal::EntityId, himo: &str, value: &str) {
        self.check_writable();
        let eid = enchudb_wal::eid_local(eid);
        let hid = self.ensure_himo(himo, HimoType::Tag, 0);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // Tag は dedupe (get_or_insert)、Leaf は新規 id 発行 (insert)。
        let vid = match self.himo_types[hid] {
            HimoType::Tag => self.vocab.get_or_insert(value.as_bytes()),
            HimoType::Leaf => self.vocab.insert(value.as_bytes()),
            ht => panic!("tie_text on non-text himo '{}': {:?}", himo, ht),
        };
        self.himos[hid].set(eid, vid);
    }

    pub fn tie(&mut self, eid: enchudb_wal::EntityId, himo: &str, value: u32) {
        self.check_writable();
        let eid = enchudb_wal::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, HimoType::Number, 0);
        debug_assert!(self.himo_types[hid] == HimoType::Number || self.himo_types[hid] == HimoType::Ref, "tie on non-Value himo '{}'", himo);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // β-light step 5: Ref himo は target_table の eid range を validate
        self.validate_ref_tie(hid, value);
        self.himos[hid].set(eid, value);
    }

    pub fn tie_ref(&mut self, eid: enchudb_wal::EntityId, himo: &str, target_eid: enchudb_wal::EntityId) {
        self.check_writable();
        let eid = enchudb_wal::eid_local(eid);
        let target_eid = enchudb_wal::eid_local(target_eid);
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, HimoType::Ref, 0);
        debug_assert!(self.himo_types[hid] == HimoType::Ref || self.himo_types[hid] == HimoType::Number, "tie_ref on non-Ref himo '{}'", himo);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // β-light step 5: target_eid が target_table の eid range 内か
        self.validate_ref_tie(hid, target_eid);
        self.himos[hid].set(eid, target_eid);
    }

    // ──── tie（定義済み紐、&self で並行書き込み可）────

    /// 定義済みの紐に文字列を張る。&selfで呼べる（Arc共有のまま書き込み可）。
    /// 紐が未定義ならpanic。define_himo を先に呼ぶこと。
    pub fn tie_text_to(&self, eid: enchudb_wal::EntityId, himo: &str, value: &str) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_text_to_by_id(eid, hid, value);
    }

    /// `tie_text_to` の himo_id 直指定版。 hot path で per-call の HashMap lookup を
    /// 避けたい時に。 起動時に `himo_id(&str)` で解決して u16 を cache しておく。
    pub fn tie_text_to_by_id(&self, eid: enchudb_wal::EntityId, himo_id: u16, value: &str) {
        self.check_writable();
        let eid = enchudb_wal::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // Tag は dedupe、Leaf は常に新規 id。
        let vid = match self.himo_types[hid] {
            HimoType::Tag => self.vocab.get_or_insert(value.as_bytes()),
            HimoType::Leaf => self.vocab.insert(value.as_bytes()),
            ht => panic!("tie_text_to_by_id on non-text himo_id {}: {:?}", himo_id, ht),
        };
        self.himos[hid].set(eid, vid);
        // WAL に Vocab + Tie を流す。 schema layer (enchudb-schema) は同期版の
        // tie_text_to を経由するため、 ここで append しないと WAL が空のままで
        // peer 同期が成立しない (publish 側が iter_committed で 0 件を見る).
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), eid);
            let _ = wal.append(enchudb_wal::wal::WalOp::Vocab { vid, bytes: value.as_bytes() });
            let _ = wal.append(enchudb_wal::wal::WalOp::Tie { eid: wal_eid, himo_id, value: vid });
        }
    }

    /// 定義済みの紐にu32値を張る。&selfで呼べる。
    pub fn tie_to(&self, eid: enchudb_wal::EntityId, himo: &str, value: u32) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_to_by_id(eid, hid, value);
    }

    /// `tie_to` の himo_id 直指定版。 hot path 用 (string lookup を避ける)。
    pub fn tie_to_by_id(&self, eid: enchudb_wal::EntityId, himo_id: u16, value: u32) {
        self.check_writable();
        let eid = enchudb_wal::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // Tag / Leaf 型 (vocab_id を value として持つ) も許可。 schema 層が
        // 起動時に解決済みの table_vid を marker himo に張る hot path 用途で
        // 必要 (request2.md 提案)。 caller 責任で vocab に既に居る id を渡すこと。
        self.himos[hid].set(eid, value);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), eid);
            let _ = wal.append(enchudb_wal::wal::WalOp::Tie { eid: wal_eid, himo_id, value });
        }
    }

    /// 定義済みの紐にentity参照を張る。&selfで呼べる。
    pub fn tie_ref_to(&self, eid: enchudb_wal::EntityId, himo: &str, target_eid: enchudb_wal::EntityId) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_ref_to_by_id(eid, hid, target_eid);
    }

    /// `tie_ref_to` の himo_id 直指定版。 hot path 用。
    pub fn tie_ref_to_by_id(&self, eid: enchudb_wal::EntityId, himo_id: u16, target_eid: enchudb_wal::EntityId) {
        self.check_writable();
        let eid = enchudb_wal::eid_local(eid);
        let target_eid = enchudb_wal::eid_local(target_eid);
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        debug_assert!(
            self.himo_types[hid] == HimoType::Ref || self.himo_types[hid] == HimoType::Number,
            "tie_ref_to_by_id on non-Ref himo_id {}", himo_id,
        );
        self.himos[hid].set(eid, target_eid);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), eid);
            let _ = wal.append(enchudb_wal::wal::WalOp::Tie { eid: wal_eid, himo_id, value: target_eid });
        }
    }

    // ──── untie ────

    pub fn untie(&self, eid: enchudb_wal::EntityId, himo: &str) {
        if let Some(hid) = self.himo_id(himo) {
            self.untie_by_id(eid, hid as u16);
        }
    }

    /// `untie` の himo_id 直指定版。 未定義の himo_id (= range 外) は debug_assert で
    /// panic、 release では silently no-op (string 版が未定義 himo を no-op 扱いするのと
    /// 整合)。
    pub fn untie_by_id(&self, eid: enchudb_wal::EntityId, himo_id: u16) {
        self.check_writable();
        let eid = enchudb_wal::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        if hid >= self.himos.len() { return; }
        self.himos[hid].remove(eid);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), eid);
            let _ = wal.append(enchudb_wal::wal::WalOp::Untie { eid: wal_eid, himo_id });
        }
    }

    // ──── delete ────

    pub fn delete(&self, eid: enchudb_wal::EntityId) {
        self.check_writable();
        let eid = enchudb_wal::eid_local(eid);
        for hid in 0..self.himos.len() {
            self.himos[hid].remove(eid);
        }
        self.entities.free(eid);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), eid);
            let _ = wal.append(enchudb_wal::wal::WalOp::Delete { eid: wal_eid });
        }
    }

    // ──── トランザクション ────

    /// WAL 有効時のみ意味あり: Commit marker を WAL に append する。
    /// v4: undo ログ廃止に伴い、 rollback API も廃止 (詳細は CLAUDE.md / lib.rs)。
    pub fn commit(&self) {
        if let Some(wal) = self.wal.as_ref() {
            let _ = wal.append(enchudb_wal::wal::WalOp::Commit);
        }
    }

    // ──── content ────

    pub fn content(&self, eid: enchudb_wal::EntityId, key: &str, data: &[u8]) {
        self.check_writable();
        self.contents.set(enchudb_wal::eid_local(eid), key, data);
    }

    pub fn get_content(&self, eid: enchudb_wal::EntityId, key: &str) -> Option<&[u8]> {
        self.contents.get(enchudb_wal::eid_local(eid), key)
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
            if let Some(wal) = self.wal.as_ref() {
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

    pub fn get_text(&self, eid: enchudb_wal::EntityId, himo: &str) -> Option<&[u8]> {
        let eid = enchudb_wal::eid_local(eid);
        let hid = self.himo_id(himo)?;
        // Tag と Leaf は両方 vocab 経由で bytes を持つ (Leaf は dedupe なし、Tag は dedupe あり)。
        match self.himo_types[hid] {
            HimoType::Tag | HimoType::Leaf => {
                let vid = self.himos[hid].get_value(eid)?;
                Some(self.vocab.get(vid))
            }
            _ => None,
        }
    }

    pub fn get(&self, eid: enchudb_wal::EntityId, himo: &str) -> Option<u32> {
        let eid = enchudb_wal::eid_local(eid);
        let hid = self.himo_id(himo)?;
        self.himos[hid].get_value(eid)
    }

    /// `get` の bindings 版。 schema 等で `himo_id` を起動時に pre-resolve した hot path 用。
    /// 名前 lookup (= himo_names の線形検索) が無くなるので point lookup が最速。
    pub fn get_by_id(&self, eid: enchudb_wal::EntityId, hid: u16) -> Option<u32> {
        let eid = enchudb_wal::eid_local(eid);
        self.himos.get(hid as usize)?.get_value(eid)
    }

    /// 指定 himo に値が tie された **全** entity を列挙。 O(next_eid) で重い。
    /// schema layer の `Query::all()` のように「table の任意 column を持つ row」 を
    /// 列挙するための代表 column 経由で使う想定。
    pub fn entities_with_himo(&self, hid: u16) -> Vec<enchudb_wal::EntityId> {
        let Some(hs) = self.himos.get(hid as usize) else { return Vec::new(); };
        let peer = self.peer_id();
        hs.entities_with_value()
            .into_iter()
            .map(|e| enchudb_wal::make_eid(peer, e))
            .collect()
    }

    /// 指定 entity 群の紐値を合計
    pub fn sum(&self, himo: &str, eids: &[enchudb_wal::EntityId]) -> u64 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_wal::eid_local(eid)) {
                total += v as u64;
            }
        }
        total
    }

    /// 最小値
    pub fn min(&self, himo: &str, eids: &[enchudb_wal::EntityId]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_wal::eid_local(eid)) {
                result = Some(result.map_or(v, |cur: u32| cur.min(v)));
            }
        }
        result
    }

    /// 最大値
    pub fn max(&self, himo: &str, eids: &[enchudb_wal::EntityId]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_wal::eid_local(eid)) {
                result = Some(result.map_or(v, |cur: u32| cur.max(v)));
            }
        }
        result
    }

    /// 平均（整数除算）
    pub fn avg(&self, himo: &str, eids: &[enchudb_wal::EntityId]) -> Option<u64> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        let mut count: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_wal::eid_local(eid)) {
                total += v as u64;
                count += 1;
            }
        }
        if count == 0 { None } else { Some(total / count) }
    }

    /// 値を持つ entity の数
    pub fn count(&self, himo: &str, eids: &[enchudb_wal::EntityId]) -> u32 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut n: u32 = 0;
        for &eid in eids {
            if hs.get_value(enchudb_wal::eid_local(eid)).is_some() { n += 1; }
        }
        n
    }

    /// GROUP BY + SUM — group_himo の値でグループ化し、sum_himo の値を合計
    ///
    /// group 値の cardinality を見て、 dense (= max_values 小) なら Vec 直 index、
    /// sparse なら HashMap で集計。
    pub fn group_sum(&self, group_himo: &str, sum_himo: &str, eids: &[enchudb_wal::EntityId]) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let cap = self.group_dense_cap(gid);
        if let Some(cap) = cap {
            let mut sums: Vec<u64> = vec![0; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_wal::eid_local(eid);
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
                let local = enchudb_wal::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), ss.get_value(local)) {
                    *map.entry(group).or_insert(0) += val as u64;
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + COUNT — group_himo の値でグループ化し、各グループの entity 数
    pub fn group_count(&self, group_himo: &str, eids: &[enchudb_wal::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut counts: Vec<u32> = vec![0; cap];
            for &eid in eids {
                if let Some(group) = gs.get_value(enchudb_wal::eid_local(eid)) {
                    counts[group as usize] += 1;
                }
            }
            (0..cap).filter(|&i| counts[i] > 0).map(|i| (i as u32, counts[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for &eid in eids {
                if let Some(group) = gs.get_value(enchudb_wal::eid_local(eid)) {
                    *map.entry(group).or_insert(0) += 1;
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + MIN
    pub fn group_min(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_wal::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut mins: Vec<u32> = vec![u32::MAX; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_wal::eid_local(eid);
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
                let local = enchudb_wal::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let entry = map.entry(group).or_insert(u32::MAX);
                    if val < *entry { *entry = val; }
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + MAX
    pub fn group_max(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_wal::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut maxs: Vec<u32> = vec![0; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_wal::eid_local(eid);
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
                let local = enchudb_wal::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let entry = map.entry(group).or_insert(0);
                    if val > *entry { *entry = val; }
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + AVG (整数除算)
    pub fn group_avg(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_wal::EntityId]) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut sums: Vec<u64> = vec![0; cap];
            let mut cnts: Vec<u64> = vec![0; cap];
            for &eid in eids {
                let local = enchudb_wal::eid_local(eid);
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
                let local = enchudb_wal::eid_local(eid);
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
    pub fn distinct(&self, himo: &str, eids: &[enchudb_wal::EntityId]) -> Vec<u32> {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![] };
        let hs = &self.himos[hid];
        let mut result: Vec<u32> = Vec::new();
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_wal::eid_local(eid)) {
                if !result.contains(&v) { result.push(v); }
            }
        }
        result
    }

    // ──── 範囲クエリ ────

    /// 範囲内の全値に合致する entity を返す（min..=max）
    pub fn pull_range(&self, himo: &str, min: u32, max: u32) -> Vec<enchudb_wal::EntityId> {
        let idx = match self.himo_id(himo) { Some(h) => h, None => return vec![] };
        let hs = &self.himos[idx];
        let mut result = Vec::new();
        for v in min..=max {
            for local in &hs.pull(v) {
                result.push(*local as enchudb_wal::EntityId);
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
    pub fn tie_date(&mut self, eid: enchudb_wal::EntityId, himo: &str, year: u32, month: u32, day: u32) {
        self.tie(eid, himo, Self::date_to_days(year, month, day));
    }

    /// epoch 日数から (year, month, day) を返す
    pub fn get_date(&self, eid: enchudb_wal::EntityId, himo: &str) -> Option<(u32, u32, u32)> {
        self.get(eid, himo).map(Self::days_to_date)
    }

    /// 日付範囲で pull_range
    pub fn pull_date_range(&self, himo: &str, from: (u32, u32, u32), to: (u32, u32, u32)) -> Vec<enchudb_wal::EntityId> {
        let min = Self::date_to_days(from.0, from.1, from.2);
        let max = Self::date_to_days(to.0, to.1, to.2);
        self.pull_range(himo, min, max)
    }

    pub fn vocab_id(&self, text: &str) -> Option<u32> { self.vocab.lookup(text.as_bytes()) }

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

    pub fn himos_of(&self, eid: enchudb_wal::EntityId) -> Vec<&str> {
        let eid = enchudb_wal::eid_local(eid);
        self.himos.iter().enumerate()
            .filter(|(_, ds)| ds.get_value(eid).is_some())
            .map(|(i, _)| self.himo_names[i].as_str())
            .collect()
    }
    /// 1 entity の全フィールドを一括取得。HashMap ルックアップ 0 回。
    pub fn get_entity(&self, eid: enchudb_wal::EntityId) -> Vec<(&str, EntityValue<'_>)> {
        let eid = enchudb_wal::eid_local(eid);
        let mut fields = Vec::with_capacity(self.himos.len());
        for (i, hs) in self.himos.iter().enumerate() {
            if let Some(raw) = hs.get_value(eid) {
                let val = match self.himo_types[i] {
                    HimoType::Tag | HimoType::Leaf => EntityValue::Text(self.vocab.get(raw)),
                    _ => EntityValue::Num(raw),
                };
                fields.push((self.himo_names[i].as_str(), val));
            }
        }
        fields
    }

    #[allow(dead_code)]
    pub(crate) fn vocab(&self) -> &Vocabulary { &self.vocab }
    pub fn himo_names(&self) -> &[String] { &self.himo_names }

    pub fn himo_type(&self, himo: &str) -> Option<HimoType> {
        self.himo_id(himo).map(|idx| self.himo_types[idx])
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

    // ──── 紐を引く（Cylinder 経由）────

    /// Cylinder + Bitmap キャッシュを再構築。delta をクリア。
    pub fn rebuild(&self) {
        for ds in &self.himos { ds.rebuild_cylinder(); }
    }

    /// 引く。 EntityId(u64) の Vec を返す。
    pub fn pull_raw(&self, himo: &str, value: u32) -> Vec<enchudb_wal::EntityId> {
        match self.himo_id(himo) {
            Some(idx) => self.himos[idx].pull(value).into_iter().map(|e| e as enchudb_wal::EntityId).collect(),
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
    pub fn pull_in(&self, himo: &str, values: &[u32]) -> Vec<enchudb_wal::EntityId> {
        match self.himo_id(himo) {
            Some(idx) => self.pull_in_by_idx(idx, values),
            None => Vec::new(),
        }
    }

    /// schema 層用: himo_id pre-resolve 済みの `pull_in`。
    pub fn pull_in_by_id(&self, himo_id: u16, values: &[u32]) -> Vec<enchudb_wal::EntityId> {
        let idx = himo_id as usize;
        if idx >= self.himos.len() { return Vec::new(); }
        self.pull_in_by_idx(idx, values)
    }

    fn pull_in_by_idx(&self, idx: usize, values: &[u32]) -> Vec<enchudb_wal::EntityId> {
        if values.is_empty() { return Vec::new(); }
        let mut out: Vec<u32> = Vec::new();
        for &v in values {
            out.extend(self.himos[idx].pull(v));
        }
        out.sort_unstable();
        out.dedup();
        out.into_iter().map(|e| e as enchudb_wal::EntityId).collect()
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

    pub fn query(&self, strings: &[(&str, u32)]) -> Vec<enchudb_wal::EntityId> {
        self.query_u32(strings).into_iter().map(|e| e as enchudb_wal::EntityId).collect()
    }

    /// schema 層用: himo_id を pre-resolve 済みの場合の高速 path。 名前 lookup を完全に skip。
    /// 同一 entity の AND 条件として扱う。 himo_id が範囲外なら空 Vec。
    pub fn query_by_id(&self, conds: &[(u16, u32)]) -> Vec<enchudb_wal::EntityId> {
        if conds.is_empty() { return Vec::new(); }
        let himo_count = self.himos.len();
        let mut idx_conds: Vec<(usize, u32)> = Vec::with_capacity(conds.len());
        for &(hid, val) in conds {
            let idx = hid as usize;
            if idx >= himo_count { return Vec::new(); }
            idx_conds.push((idx, val));
        }
        self.query_resolved(&idx_conds).into_iter().map(|e| e as enchudb_wal::EntityId).collect()
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

    fn ensure_himo(&mut self, himo: &str, ht: HimoType, max_values: u32) -> usize {
        if let Some(idx) = self.himo_id(himo) { return idx; }
        let hid = self.himos.len();
        assert!((hid as u32) < self.max_himos, "too many himos (max {})", self.max_himos);

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
        let base = self.backing.as_mut_ptr();
        let make_region = |off: usize, size: usize| -> Region {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(g) = &grower {
                return unsafe { Region::with_grower(g.clone(), off, size) };
            }
            unsafe { Region::new(base.add(off), size) }
        };

        // β-heavy phase 1: sidecar が attached なら Region mode で init、
        // そうでなければ旧来の Heap mode。
        #[cfg(not(target_arch = "wasm32"))]
        let hs = if let Some(sidecar) = &self.positions_sidecar {
            let pos_region = sidecar.region_for(hid as u32);
            // β-heavy phase 1: eid_offset=0 で init すると、 任意の eid 順での
                    // tie (= 大→小 順含む) を panic 無しで扱える。 commit
                    // 空間は max_eid_seen × 8 byte に限定されるので、 大半が
                    // 0 近辺で tie されるワークロードでは waste 少。
                    let positions = crate::positions_region::PositionsRegion::init_with_offset(pos_region, 0);
            HimoStore::init_with_positions(
                make_region(col_off, self.layout.himo_col_size),
                ht, effective_mv, self.max_entities, positions,
            )
        } else {
            HimoStore::init(
                make_region(col_off, self.layout.himo_col_size),
                ht, effective_mv, self.max_entities,
            )
        };
        #[cfg(target_arch = "wasm32")]
        let hs = HimoStore::init(
            make_region(col_off, self.layout.himo_col_size),
            ht, effective_mv, self.max_entities,
        );

        self.himos.push(hs);
        self.himo_names.push(himo.to_string());
        self.himo_types.push(ht);
        self.himo_max_values.push(max_values);

        // β-light step 2: 旧 API (define_himo) で追加された himo は anonymous
        // table に attach する。 step 3+ で define_table 経由なら target table
        // を指定して attach する形に拡張。
        let hid_u32 = hid as u32;
        self.tables[ANONYMOUS_TABLE as usize].himo_ids.push(hid_u32);
        self.himo_to_table.push(ANONYMOUS_TABLE);

        // ヘッダにメタデータ書き込み
        let maxv_base = himo_maxv_base(self.max_himos);
        self.backing.as_slice_mut()[H_HIMO_TYPES + hid] = ht as u8;
        let mv_off = maxv_base + hid * 4;
        self.backing.as_slice_mut()[mv_off..mv_off + 4].copy_from_slice(&max_values.to_le_bytes());
        let himo_count = (hid + 1) as u32;
        self.backing.as_slice_mut()[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&himo_count.to_le_bytes());
        // v28: header CRC を再計算(himo_count が変わったため)
        write_header_crc(self.backing.as_slice_mut());

        hid
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
    /// プロセス/OS クラッシュ時は open_concurrent_with_wal で最後の Commit まで復元される。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_concurrent_with_wal(path: &str, wal_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        let wal_path = wal_path_for(path);
        let wal = std::sync::Arc::new(enchudb_wal::wal::Wal::create(&wal_path, wal_capacity)?);
        Ok(Self::spawn_consumer_with_wal(eng, Some(wal)))
    }

    /// `create_concurrent_with_wal` + `queue_capacity` override (issue4)。
    /// - `queue_capacity`: WriteQueue / wal_record_queue の bounded cap (default 1 M)
    ///
    /// sustained writer (sunsu Docker scenario 03 等) で writer >> consumer rate に
    /// なると、 旧 unbounded queue では RSS 線形成長 → OOM。 bounded 化 + producer
    /// block でこれを cap する。 capacity の選び方:
    /// - 小さい (例: 10 K) → RSS 低い、 latency 不安定
    /// - 大きい (例: 10 M) → latency 安定、 RSS は queue 内 record サイズ × cap
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_concurrent_with_wal_queue_cap(
        path: &str,
        wal_capacity: usize,
        queue_capacity: usize,
    ) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        let wal_path = wal_path_for(path);
        let wal = std::sync::Arc::new(enchudb_wal::wal::Wal::create(&wal_path, wal_capacity)?);
        Ok(Self::spawn_consumer_with_wal_queue_cap(eng, Some(wal), Some(queue_capacity)))
    }

    /// v28: WAL 付き open_concurrent。既存 WAL があればリカバリする。
    /// v29: region CRC は WAL ルートでは skip(WAL が source of truth)。
    /// 代わりに古い `.crc` ファイルは削除して、次回 flush で regenerate させる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_concurrent_with_wal(path: &str, wal_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let mut eng = Self::open_internal(path, /*verify_region_crc=*/ false, /*take_lock=*/ true)?;
        // 古い .crc は WAL 活動後に stale になるので削除
        let crc_path = crate::integrity::crc_path_for(path);
        let _ = std::fs::remove_file(&crc_path);
        let wal_path = wal_path_for(path);
        let wal = if wal_path.exists() {
            let w = enchudb_wal::wal::Wal::open(&wal_path)?;
            // リカバリ: commit されたレコードを本体に適用
            let records = w.recover();
            for rec in &records {
                eng.apply_wal_op(&rec.op);
            }
            // 本体に適用し終えたので checkpoint を head まで前進
            w.advance_checkpoint(w.head());
            std::sync::Arc::new(w)
        } else {
            std::sync::Arc::new(enchudb_wal::wal::Wal::create(&wal_path, wal_capacity)?)
        };
        Ok(Self::spawn_consumer_with_wal(eng, Some(wal)))
    }

    /// WAL の 1 op を本体に適用(recover 専用)。
    /// v32: eid は u64 だが Column は local u32 で保持。eid_local() で剥がす。
    fn apply_wal_op(&mut self, op: &enchudb_wal::wal::DecodedOp) {
        use enchudb_wal::wal::DecodedOp;
        match op {
            DecodedOp::Tie { eid, himo_id, value } => {
                let hid = *himo_id as usize;
                let local = enchudb_wal::eid_local(*eid);
                if hid < self.himos.len() {
                    self.himos[hid].set(local, *value);
                }
            }
            DecodedOp::Untie { eid, himo_id } => {
                let hid = *himo_id as usize;
                let local = enchudb_wal::eid_local(*eid);
                if hid < self.himos.len() {
                    self.himos[hid].remove(local);
                }
            }
            DecodedOp::Delete { eid } => {
                let local = enchudb_wal::eid_local(*eid);
                for hid in 0..self.himos.len() {
                    self.himos[hid].remove(local);
                }
                self.entities.free(local);
            }
            DecodedOp::Content { eid, key, data } => {
                let local = enchudb_wal::eid_local(*eid);
                self.contents.set(local, key, data);
            }
            DecodedOp::Commit => {}
            DecodedOp::Vocab { .. } => {
                // v33: 自プロセスの recover 時は Vocab 個別の apply 不要
                // (author_peer == self の場合は既に local vocab にある)。
                // Sync 経由で他 peer から受信する場合のみ apply_one 側で処理。
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
    pub fn concurrentize_with_wal(mut eng: Self, wal_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let path = eng.path.clone();
        let wal_path = wal_path_for(&path);
        // 古い .crc は WAL 活動後に stale になるので削除 (open_concurrent_with_wal と同じ扱い)
        let crc_path = crate::integrity::crc_path_for(&path);
        let _ = std::fs::remove_file(&crc_path);
        let wal = if wal_path.exists() {
            let w = enchudb_wal::wal::Wal::open(&wal_path)?;
            let records = w.recover();
            for rec in &records {
                eng.apply_wal_op(&rec.op);
            }
            w.advance_checkpoint(w.head());
            std::sync::Arc::new(w)
        } else {
            std::sync::Arc::new(enchudb_wal::wal::Wal::create(&wal_path, wal_capacity)?)
        };
        Ok(Self::spawn_consumer_with_wal(eng, Some(wal)))
    }

    fn spawn_consumer(eng: Self) -> std::sync::Arc<Self> {
        Self::spawn_consumer_with_wal(eng, None)
    }

    /// `wal_record_queue` capacity の default (= write_queue と同じ 1 M)。
    /// issue4: 旧 unbounded SegQueue では sustained writer で RSS 線形成長 → OOM。
    /// caller は `create_concurrent_with_wal_queue_cap` で上書きできる。
    pub(crate) const DEFAULT_WAL_RECORD_QUEUE_CAP: usize =
        crate::write_queue::DEFAULT_WRITE_QUEUE_CAP;

    /// changefeed 内部ヘルパ: WAL から emit_offset 以降の record を取り出して
    /// 全 listener に渡し、cursor を進める。
    fn fire_change_listeners(
        wal: &std::sync::Arc<enchudb_wal::wal::Wal>,
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
        let recs = wal.iter_committed_from(start);
        if recs.is_empty() {
            return;
        }
        let wires: Vec<crate::transport::WireRecord> =
            recs.into_iter().map(|r| r.into()).collect();
        for listener in guard.iter() {
            listener.on_changes(&wires);
        }
        emit_offset.store(wal.head(), Ordering::Release);
    }

    fn spawn_consumer_with_wal(
        eng: Self,
        wal: Option<std::sync::Arc<enchudb_wal::wal::Wal>>,
    ) -> std::sync::Arc<Self> {
        Self::spawn_consumer_with_wal_queue_cap(eng, wal, None)
    }

    /// `spawn_consumer_with_wal` + queue capacity 上書き。 None = default。
    /// issue4 backpressure 用 — capacity 大きいほど push 側 latency 安定、
    /// 小さいほど RSS 安定 (writer rate >> consumer rate の差を queue で吸収しない)。
    fn spawn_consumer_with_wal_queue_cap(
        mut eng: Self,
        wal: Option<std::sync::Arc<enchudb_wal::wal::Wal>>,
        queue_cap: Option<usize>,
    ) -> std::sync::Arc<Self> {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use crate::write_queue::WriteQueue;

        let qc = queue_cap.unwrap_or(crate::write_queue::DEFAULT_WRITE_QUEUE_CAP);
        let queue = Arc::new(WriteQueue::with_capacity(qc));
        let shutdown = Arc::new(AtomicBool::new(false));
        // WAL 有効時のみ wal_record_queue を生やす。 writer は直接 wal.append せず
        // ここに owned record を push する → consumer thread が drain して append_many。
        // issue4: bounded `ArrayQueue` で sustained writer の RSS を cap する。
        let wal_record_queue = wal.as_ref()
            .map(|_| Arc::new(crossbeam_queue::ArrayQueue::<enchudb_wal::wal::WalRecord>::new(qc)));

        eng.write_queue = Some(queue.clone());
        eng.shutdown_flag = Some(shutdown.clone());
        eng.wal = wal.clone();
        eng.wal_record_queue = wal_record_queue.clone();

        let arc = Arc::new(eng);

        let engine_ptr: *const Engine = Arc::as_ptr(&arc);
        let engine_addr = engine_ptr as usize;
        let q_for_thread = queue.clone();
        let flag_for_thread = shutdown.clone();
        let apply_count_for_thread = arc.apply_count.clone();
        let wal_for_thread = wal.clone();
        let wal_record_queue_for_thread = wal_record_queue.clone();
        let durable_lsn_for_thread = arc.durable_lsn.clone();
        let listeners_for_thread = arc.change_listeners.clone();
        let emit_offset_for_thread = arc.change_emit_offset.clone();

        let handle = std::thread::Builder::new()
            .name("enchudb-consumer".into())
            .spawn(move || {
                use std::sync::atomic::Ordering;
                use std::time::{Duration, Instant};
                let engine: &Engine = unsafe { &*(engine_addr as *const Engine) };
                let fsync_interval = Duration::from_millis(100);
                let mut last_fsync = Instant::now();

                loop {
                    let mut drained_any = false;
                    // WAL record queue を batch drain (per-record flock を償却)
                    if let (Some(wq), Some(wal)) = (
                        wal_record_queue_for_thread.as_ref(),
                        wal_for_thread.as_ref(),
                    ) {
                        let mut batch: Vec<enchudb_wal::wal::WalRecord> = Vec::new();
                        while let Some(rec) = wq.pop() { batch.push(rec); }
                        if !batch.is_empty() {
                            let _ = wal.append_many(&batch);
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
                    if let Some(wal) = wal_for_thread.as_ref() {
                        if last_fsync.elapsed() >= fsync_interval {
                            if wal.head() > wal.checkpoint() {
                                let _ = wal.append(enchudb_wal::wal::WalOp::Commit);
                                let _ = wal.fsync();
                                let _ = engine.body_msync();
                                let head = wal.head();
                                wal.advance_checkpoint(head);
                                let lsn = wal.next_lsn().saturating_sub(1);
                                durable_lsn_for_thread.store(lsn, Ordering::Release);

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
                            if wal.try_reset() {
                                emit_offset_for_thread.store(
                                    enchudb_wal::wal::HEADER_SIZE as u64,
                                    Ordering::Release,
                                );
                            }
                            last_fsync = Instant::now();
                        }
                    }

                    if flag_for_thread.load(Ordering::Acquire) {
                        // 最終 WAL drain
                        if let (Some(wq), Some(wal)) = (
                            wal_record_queue_for_thread.as_ref(),
                            wal_for_thread.as_ref(),
                        ) {
                            let mut batch: Vec<enchudb_wal::wal::WalRecord> = Vec::new();
                            while let Some(rec) = wq.pop() { batch.push(rec); }
                            if !batch.is_empty() {
                                let _ = wal.append_many(&batch);
                            }
                        }
                        while let Some(op) = q_for_thread.pop() {
                            engine.apply_op(op);
                            apply_count_for_thread.fetch_add(1, Ordering::Release);
                        }
                        // shutdown 時の最終 Commit + 順序付き同期
                        if let Some(wal) = wal_for_thread.as_ref() {
                            let _ = wal.append(enchudb_wal::wal::WalOp::Commit);
                            let _ = wal.fsync();
                            let _ = engine.body_msync();
                            wal.advance_checkpoint(wal.head());
                            // shutdown 時の最終 emit
                            Self::fire_change_listeners(
                                wal,
                                &listeners_for_thread,
                                &emit_offset_for_thread,
                            );
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
    pub fn wal_sync(&self) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        self.flush_writes();
        if let Some(wal) = self.wal.as_ref() {
            let _ = wal.append(enchudb_wal::wal::WalOp::Commit);
            wal.fsync()?;
            self.body_msync()?;
            let head = wal.head();
            wal.advance_checkpoint(head);
            let lsn = wal.next_lsn().saturating_sub(1);
            self.durable_lsn.store(lsn, Ordering::Release);
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
    /// WAL が有効でない(`create_concurrent_with_wal` 未使用等)なら空 Vec。
    /// `AuditFilter::default()` = 全件。
    ///
    /// 返る各 `RecoveredRecord` には lsn, hlc, author_peer, op, signature, pubkey_fp が入り、
    /// 「誰がいつ何を書いたか」をそのまま監査できる。
    pub fn audit(&self, filter: &AuditFilter) -> Vec<enchudb_wal::wal::RecoveredRecord> {
        let recs = match self.wal.as_ref() {
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
    /// **呼び出し前に必ず `wal_sync()` or `flush()` で durable 化すること**。
    /// mmap 非同期ページが残っていると snapshot に反映されない。
    ///
    /// 返り値: コピーしたパス一覧。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn snapshot_export(&self, target: &str) -> io::Result<SnapshotFiles> {
        // mmap ページを物理ディスクに確実に書き出す(v27 のみ)
        self.body_msync()?;

        let mut files = SnapshotFiles {
            main: target.to_string(),
            wal: None,
            crc: None,
        };
        std::fs::copy(&self.path, target)?;

        let wal_src = format!("{}.wal", self.path);
        if std::path::Path::new(&wal_src).exists() {
            let wal_dst = format!("{}.wal", target);
            std::fs::copy(&wal_src, &wal_dst)?;
            files.wal = Some(wal_dst);
        }

        let crc_src = format!("{}.crc", self.path);
        if std::path::Path::new(&crc_src).exists() {
            let crc_dst = format!("{}.crc", target);
            std::fs::copy(&crc_src, &crc_dst)?;
            files.crc = Some(crc_dst);
        }

        Ok(files)
    }

    pub fn stats(&self) -> EngineStats {
        use std::sync::atomic::Ordering;
        let (wal_head, wal_checkpoint, wal_capacity, wal_lsn) = match self.wal.as_ref() {
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
            himo_count: self.himo_types.len() as u32,
            wal_head,
            wal_checkpoint,
            wal_capacity,
            wal_lag_bytes: wal_head.saturating_sub(wal_checkpoint),
            wal_next_lsn: wal_lsn,
            durable_lsn: self.durable_lsn.load(Ordering::Acquire),
            queue_len: self.write_queue.as_ref().map(|q| q.len()).unwrap_or(0),
            pushed: self.push_count.load(Ordering::Acquire),
            applied: self.apply_count.load(Ordering::Acquire),
            peer_id,
            hlc_entries,
            max_hlc,
        }
    }

    /// consumer スレッド内部で呼ぶ。Op 1 個を適用。
    fn apply_op(&self, op: crate::write_queue::Op) {
        use crate::write_queue::Op;
        match op {
            Op::Tie { eid, himo_id, value } => {
                let hid = himo_id as usize;
                if hid >= self.himos.len() { return; }
                self.himos[hid].set(eid, value);
            }
            Op::Untie { eid, himo_id } => {
                let hid = himo_id as usize;
                if hid >= self.himos.len() { return; }
                self.himos[hid].remove(eid);
            }
            Op::Delete { eid } => {
                for hid in 0..self.himos.len() {
                    self.himos[hid].remove(eid);
                }
                self.entities.free(eid);
            }
            Op::Content { eid, key, data } => {
                self.contents.set(eid, &key, &data);
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
    pub fn tie_async(&self, eid: enchudb_wal::EntityId, himo: &str, value: u32) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_async_by_id(eid, hid, value);
    }

    /// `tie_async` の himo_id 直指定版。 SNS の post / like 投入のように row/sec が KO
    /// 単位の hot path 用 (per-call の `himo_id(&str)` HashMap lookup を消す)。
    /// 起動時に `himo_id(&str)` で u16 を 1 回引いて cache し、 hot loop で繰り返し使う想定。
    pub fn tie_async_by_id(&self, eid: enchudb_wal::EntityId, himo_id: u16, value: u32) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_wal::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か (anonymous
        // open-ended なら即 return)
        self.validate_eid_for_himo(himo_id as usize, local);
        // β-light step 5: Ref himo の FK validation (非 Ref は即 return で
        // ~1 ns、 Ref で fk_refs entry なしも同じ)
        self.validate_ref_tie(himo_id as usize, value);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), local);
            let rec = enchudb_wal::wal::WalRecord::Tie { eid: wal_eid, himo_id, value };
            if let Some(wq) = self.wal_record_queue.as_ref() {
                push_wal_record_blocking(wq, rec);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
        let q = self.write_queue.as_ref()
            .expect("tie_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie { eid: local, himo_id, value });
        self.push_count.fetch_add(1, Ordering::Release);
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
    pub fn tie_text_async(&self, eid: enchudb_wal::EntityId, himo: &str, value: &str) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_text_async_by_id(eid, hid, value);
    }

    /// `tie_text_async` の himo_id 直指定版。 text 本体は vocab 経由なので value はそのまま。
    pub fn tie_text_async_by_id(&self, eid: enchudb_wal::EntityId, himo_id: u16, value: &str) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_wal::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, local);
        // Tag は dedupe、Leaf は常に新規 id。
        let vid = match self.himo_types[hid] {
            HimoType::Tag => self.vocab.get_or_insert(value.as_bytes()),
            HimoType::Leaf => self.vocab.insert(value.as_bytes()),
            ht => panic!("tie_text_async_by_id on non-text himo_id {}: {:?}", himo_id, ht),
        };
        assert!(vid < u32::MAX, "vocab vid must be < u32::MAX (sentinel reserved)");
        if let Some(wal) = self.wal.as_ref() {
            // Vocab op を先に(sync の receiver 側で Tie より先に mapping が張られるよう)
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), local);
            let vocab_rec = enchudb_wal::wal::WalRecord::Vocab { vid, bytes: value.as_bytes().to_vec() };
            let tie_rec = enchudb_wal::wal::WalRecord::Tie { eid: wal_eid, himo_id, value: vid };
            if let Some(wq) = self.wal_record_queue.as_ref() {
                // Vocab → Tie の順を保つため同一 thread から連続 push
                push_wal_record_blocking(wq, vocab_rec);
                push_wal_record_blocking(wq, tie_rec);
            } else {
                let _ = wal.append(vocab_rec.as_op());
                let _ = wal.append(tie_rec.as_op());
            }
        }
        let q = self.write_queue.as_ref()
            .expect("tie_text_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie { eid: local, himo_id, value: vid });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// v33: 非同期 tie_ref。target_eid の local 部(u32)を WAL / 本体に運ぶ。
    /// target_eid は u64 [peer|local] だが、現行 WAL は value: u32 しか運べないため
    /// peer_id 部は捨てる。すなわち receiver 側では「author_peer と同一 peer 上の entity」を
    /// 指す ref として再構成される。cross-peer ref (peer A が peer B の entity を指す)は
    /// 現状未対応 (Ref 用 WAL op を別途用意する必要あり、v34 以降)。
    pub fn tie_ref_async(&self, eid: enchudb_wal::EntityId, himo: &str, target_eid: enchudb_wal::EntityId) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_ref_async_by_id(eid, hid, target_eid);
    }

    /// `tie_ref_async` の himo_id 直指定版。
    pub fn tie_ref_async_by_id(&self, eid: enchudb_wal::EntityId, himo_id: u16, target_eid: enchudb_wal::EntityId) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_wal::eid_local(eid);
        let target_local = enchudb_wal::eid_local(target_eid);
        assert!(target_local < u32::MAX, "target_local must be < u32::MAX");
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(himo_id as usize, local);
        // β-light step 5: target_eid が target_table の eid range 内か
        self.validate_ref_tie(himo_id as usize, target_local);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), local);
            let rec = enchudb_wal::wal::WalRecord::Tie {
                eid: wal_eid, himo_id, value: target_local,
            };
            if let Some(wq) = self.wal_record_queue.as_ref() {
                push_wal_record_blocking(wq, rec);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
        let q = self.write_queue.as_ref()
            .expect("tie_ref_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie {
            eid: local, himo_id, value: target_local,
        });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 非同期 untie。
    pub fn untie_async(&self, eid: enchudb_wal::EntityId, himo: &str) {
        let hid = match self.himo_id(himo) { Some(x) => x as u16, None => return };
        self.untie_async_by_id(eid, hid);
    }

    /// `untie_async` の himo_id 直指定版。
    pub fn untie_async_by_id(&self, eid: enchudb_wal::EntityId, himo_id: u16) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_wal::eid_local(eid);
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        if (himo_id as usize) >= self.himos.len() { return; }
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), local);
            let rec = enchudb_wal::wal::WalRecord::Untie { eid: wal_eid, himo_id };
            if let Some(wq) = self.wal_record_queue.as_ref() {
                push_wal_record_blocking(wq, rec);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Untie { eid: local, himo_id });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 非同期 delete。
    pub fn delete_async(&self, eid: enchudb_wal::EntityId) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_wal::eid_local(eid);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), local);
            let rec = enchudb_wal::wal::WalRecord::Delete { eid: wal_eid };
            if let Some(wq) = self.wal_record_queue.as_ref() {
                push_wal_record_blocking(wq, rec);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Delete { eid: local });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 非同期 content 書き込み。WAL 有効時はクラッシュ後も復元される。
    /// key と data は Box に move される(消費される)。
    pub fn content_async(&self, eid: enchudb_wal::EntityId, key: &str, data: &[u8]) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_wal::eid_local(eid);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = enchudb_wal::make_eid(wal.peer_id(), local);
            let rec = enchudb_wal::wal::WalRecord::Content {
                eid: wal_eid, key: key.to_string(), data: data.to_vec(),
            };
            if let Some(wq) = self.wal_record_queue.as_ref() {
                push_wal_record_blocking(wq, rec);
            } else {
                let _ = wal.append(rec.as_op());
            }
        }
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Content {
            eid: local,
            key: key.to_string().into_boxed_str(),
            data: data.to_vec().into_boxed_slice(),
        });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 現在のトランザクションを WAL に commit marker で確定する。
    ///
    /// Async モードでは fsync は consumer スレッドが背景で行う。
    /// Sync モードでは commit 完了まで待つ。
    pub fn wal_commit(&self) {
        if let Some(wal) = self.wal.as_ref() {
            let _ = wal.append(enchudb_wal::wal::WalOp::Commit);
        }
    }

    /// WAL への参照(テスト / 内部用)。
    pub fn wal(&self) -> Option<&std::sync::Arc<enchudb_wal::wal::Wal>> { self.wal.as_ref() }

    /// push 済みの全 Op が apply 完了するまで spin 待ち。`tie_async` の同期点。
    /// `queue.is_empty()` は pop 直後 / apply 前のウィンドウで true になる race が
    /// あるため、push_count と apply_count の累積カウンタで apply 完了を待つ。
    pub fn flush_writes(&self) {
        use std::sync::atomic::Ordering;
        if self.write_queue.is_none() { return; }
        loop {
            let pushed = self.push_count.load(Ordering::Acquire);
            let applied = self.apply_count.load(Ordering::Acquire);
            if applied >= pushed { return; }
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
        let hc = self.himo_types.len() as u32;
        let buf = self.backing.as_slice_mut();
        for hid in 0..self.himo_types.len() {
            buf[H_HIMO_TYPES + hid] = self.himo_types[hid] as u8;
            let off = maxv_base + hid * 4;
            buf[off..off + 4].copy_from_slice(&self.himo_max_values[hid].to_le_bytes());
        }
        buf[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&hc.to_le_bytes());
        // v28: ヘッダ整合性 CRC(himo_count 含む固定レイアウト部のみを対象)
        write_header_crc(buf);

        // 全 region を disk に同期した後、 vocab/himo_reg の index 整合性 OK
        // マークを書いてもう一度 msync。 次 open で rebuild_index を skip できる。
        self.backing.flush_to_disk()?;
        self.vocab.mark_index_clean(true);
        self.himo_reg.mark_index_clean(true);
        self.backing.flush_to_disk()?;

        // β-heavy phase 1: positions sidecar の msync (dirty range のみ)。
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(sidecar) = &self.positions_sidecar {
            sidecar.flush()?;
        }
        Ok(())
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
        eng.define_himo("age", HimoType::Number, 100);
        eng.define_himo("dept", HimoType::Number, 20);

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
        eng.define_himo("level", HimoType::Number, 10);
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
        eng.define_himo("age", HimoType::Number, 100);

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
            eng.define_himo("score", HimoType::Number, 200);
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
        eng.define_himo("age", HimoType::Number, 100);
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
        eng.define_himo("score", HimoType::Number, 1000);
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
        eng.define_himo("age", HimoType::Number, 100);
        eng.define_himo("dept", HimoType::Number, 20);
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
        eng.define_himo("x", HimoType::Number, 10);
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
        eng.define_himo("val", HimoType::Number, 100);
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
        eng.define_himo("age", HimoType::Number, SCALE_AGES);
        eng.define_himo("dept", HimoType::Number, SCALE_DEPTS);
        eng.define_himo("company", HimoType::Number, SCALE_COMPANIES);

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
        eng.define_himo("age", HimoType::Number, ages);
        eng.define_himo("dept", HimoType::Number, depts);
        eng.define_himo("group", HimoType::Number, groups);

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
        eng.define_himo("age", HimoType::Number, 200);
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
        eng.define_himo("k", HimoType::Number, 16);
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
        eng.define_himo("__blob_id", HimoType::Tag, 0);

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
        eng.define_himo("age", HimoType::Number, 0);
        eng.define_himo("city", HimoType::Tag, 0);
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
            eng.define_himo("key", HimoType::Tag, 0);
            eng.define_himo("ts", HimoType::Number, 0);
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
            eng.define_himo("v", HimoType::Number, 100);
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
            eng.define_himo("v", HimoType::Number, 100);
            eng.flush().unwrap();
        }
        let mut eng = Engine::open_readonly(&p).unwrap();
        let e = eng.entity(); // ← ここで panic
        eng.tie(e, "v", 1);
        let _ = std::fs::remove_file(&p);
    }

    /// 2 process emulation: 1 writer が live な間、 もう一つの open_standalone は
    /// 別 thread から呼ぶと block する。 100 ms 待っても 2nd が returnしないことで
    /// 排他を確認、 1st を drop すると 2nd が unblock。
    #[test]
    fn writer_blocks_concurrent_writer() {
        use std::sync::mpsc;
        let p = tmp("writer_block");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", HimoType::Number, 100);
            eng.flush().unwrap();
        }
        let eng_a = Engine::open_standalone(&p).unwrap();

        // 別 thread で 2 nd open_standalone を試みる。 block するはず。
        let (tx, rx) = mpsc::channel::<()>();
        let p_clone = p.clone();
        let h = std::thread::spawn(move || {
            let _eng_b = Engine::open_standalone(&p_clone).unwrap();
            tx.send(()).unwrap();
        });

        // 200 ms 以内に取れちゃったら lock が効いてない
        match rx.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(_) => panic!("2nd open_standalone should have been blocked"),
            Err(mpsc::RecvTimeoutError::Timeout) => {} // expected
            Err(e) => panic!("unexpected: {:?}", e),
        }

        // 1 st を drop → 2 nd が unblock
        drop(eng_a);
        rx.recv_timeout(std::time::Duration::from_secs(2))
            .expect("2nd open should succeed after 1st drop");
        h.join().unwrap();
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
            eng.define_himo("year", HimoType::Number, 100);
            eng.define_himo("name", HimoType::Tag, 0);
            eng.define_himo("self_ref", HimoType::Ref, 0);
            let e = eng.entity();
            eng.tie_to(e, "year", 2026);
            eng.tie_text_to(e, "name", "alice");
            eng.tie_ref_to(e, "self_ref", e);
            eng.flush().unwrap();
        }
        {
            let eng = Engine::create_standalone(&p_id).unwrap();
            let mut eng = eng;
            eng.define_himo("year", HimoType::Number, 100);
            eng.define_himo("name", HimoType::Tag, 0);
            eng.define_himo("self_ref", HimoType::Ref, 0);
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
        eng.define_himo("year", HimoType::Number, 100);
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
        eng.define_himo("year", HimoType::Number, 100);
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
            eng.define_himo("score", HimoType::Number, 0);
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
}
