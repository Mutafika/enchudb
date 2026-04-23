//! v24 Engine — 量子円柱。単一ファイル。全コンポーネントが1つのmmapを共有。
//!
//!   entity() → ID 振る
//!   tie_text → 文字列を紐で張る（Vocabulary 経由）
//!   tie     → u32 値を紐で張る
//!   untie → 紐を外す
//!   content/get_content → 非索引テキスト
//!   query → 円柱の重なりを一発で返す
//!   delete → entity 削除
//!   commit/rollback → トランザクション（undo ログ）
//!   open/flush → 永続化（mmap なので open は即利用可）

#[cfg(not(target_arch = "wasm32"))]
use std::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
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
#[cfg(feature = "v27")]
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
    pub max_hlc: Option<crate::Hlc>,
}

#[cfg(feature = "v27")]
fn wal_path_for(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.wal", path))
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
    pub from_hlc: Option<crate::Hlc>,
    /// HLC 上限(inclusive)。これより新しい record は除外。
    pub to_hlc: Option<crate::Hlc>,
    /// 書き手 peer_id。これ以外の author を除外。
    pub author_peer: Option<crate::PeerId>,
    /// 署名者 pubkey の指紋(8B)。一致しない record は除外。
    pub pubkey_fp: Option<[u8; 8]>,
}

// ════════════════ バッキングストア ════════════════

/// mmap (native) または Vec<u8> (wasm/テスト) のどちらかを保持。
/// Engine が drop されるまでポインタが安定している。
enum Backing {
    #[cfg(not(target_arch = "wasm32"))]
    Mmap(MmapMut),
    Memory(Vec<u8>),
}

impl Backing {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Mmap(m) => m.as_mut_ptr(),
            Backing::Memory(v) => v.as_mut_ptr(),
        }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Mmap(m) => &mut m[..],
            Backing::Memory(v) => &mut v[..],
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn flush_to_disk(&self) -> io::Result<()> {
        match self {
            Backing::Mmap(m) => m.flush(),
            Backing::Memory(_) => Ok(()),
        }
    }

    /// v32: &self 経由で header 領域に unsafe で書き込む。
    /// mmap 経由の並行書き込みは atomic スライス更新でガードされる前提。
    #[cfg(feature = "v32")]
    #[allow(clippy::mut_from_ref)]
    fn header_mut(&self) -> &mut [u8] {
        unsafe {
            match self {
                #[cfg(not(target_arch = "wasm32"))]
                Backing::Mmap(m) => {
                    let ptr = m.as_ptr() as *mut u8;
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
use crate::undo::UndoLog;
use crate::column::Column;
#[cfg(not(feature = "v27"))]
use crate::cylinder::Cylinder;

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
const FILE_VERSION: u32 = 2;
const HEADER_SIZE: usize = 4096;

/// 観測窓(view)の永続化制限。
#[cfg(feature = "v27")]
const MAX_PERSISTED_VIEWS: usize = 32;
/// 1 view あたりの最大紐数(固定長レコード制約)。
#[cfg(feature = "v27")]
const MAX_VIEW_HIMOS: usize = 8;

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
#[cfg(feature = "v32")]
const H_PEER_ID: usize = 68; // u32
const H_HIMO_TYPES: usize = 256;

// v27: 観測窓(n-tuple 仮想テーブル定義)領域。
// ヘッダ 2048.. の後半 2KB を使う。既存の himo_types/max_values 領域(256..1536, max_himos=256 時)と衝突しない。
// フォーマット:
//   [H_VIEW_COUNT]    u32    view 数(上限 MAX_PERSISTED_VIEWS)
//   [H_VIEWS_OFF + i*R..] : 各 view のレコード (R = 1 + MAX_VIEW_HIMOS*2 = 17 bytes)
//     - length: u8   (紐数、2..=MAX_VIEW_HIMOS)
//     - himo_ids: u16 × MAX_VIEW_HIMOS  (使われない末尾は 0 埋め、length で切る)
#[cfg(feature = "v27")]
const H_VIEW_COUNT: usize = 2048;
#[cfg(feature = "v27")]
const H_VIEWS_OFF: usize = 2052;
#[cfg(feature = "v27")]
const VIEW_RECORD_SIZE: usize = 1 + MAX_VIEW_HIMOS * 2;

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
    undo_off: usize,
    undo_size: usize,
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
        let mut off = HEADER_SIZE;

        let entities_off = off;
        let entities_size = align8(EntitySet::region_size(max_entities));
        off += entities_size;

        let undo_off = off;
        let undo_size = align8(UndoLog::region_size());
        off += undo_size;

        let vocab_data_off = off;
        let vocab_data_size = align8(Vocabulary::data_region_size(vocab_data_size));
        off += vocab_data_size;

        let vocab_offsets_off = off;
        let vocab_offsets_size = align8(Vocabulary::offsets_region_size(vocab_max_entries));
        off += vocab_offsets_size;

        let vocab_index_off = off;
        let vocab_index_size = align8(Vocabulary::index_region_size(vocab_index_cap));
        off += vocab_index_size;

        let himoreg_data_off = off;
        let himoreg_data_size = align8(Vocabulary::data_region_size(himoreg_data_size));
        off += himoreg_data_size;

        let himoreg_offsets_off = off;
        let himoreg_offsets_size = align8(Vocabulary::offsets_region_size(himoreg_max_entries));
        off += himoreg_offsets_size;

        let himoreg_index_off = off;
        let himoreg_index_size = align8(Vocabulary::index_region_size(himoreg_index_cap));
        off += himoreg_index_size;

        let content_index_off = off;
        let content_index_size = align8(ContentStore::index_region_size_for(max_entities));
        off += content_index_size;

        let content_data_off = off;
        let content_data_size = align8(content_data_size);
        off += content_data_size;

        let himo_col_size = align8(Column::region_size(max_entities, 4));
        #[cfg(not(feature = "v27"))]
        let himo_cyl_size = align8(Cylinder::region_size(max_entities, cyl_max_values));
        #[cfg(feature = "v27")]
        let himo_cyl_size = 0usize;
        #[cfg(not(feature = "v27"))]
        let himo_slot_size = himo_col_size + himo_cyl_size * 2;
        #[cfg(feature = "v27")]
        let himo_slot_size = himo_col_size;

        let himo_base_off = off;
        off += himo_slot_size * (max_himos as usize);

        Layout {
            entities_off, entities_size,
            undo_off, undo_size,
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

/// v26 ペアテーブル: ソート済み Vec<u32> セル。delta で直接操作。
#[cfg(all(feature = "v26", not(feature = "v27")))]
struct PairEntry {
    himo_a: usize,
    himo_b: usize,
    card_a: u32,
    card_b: u32,
    cells: Vec<Vec<u32>>,
}

#[cfg(all(feature = "v26", not(feature = "v27")))]
struct PairTable {
    pairs: Vec<PairEntry>,
}

#[cfg(all(feature = "v26", not(feature = "v27")))]
impl PairTable {
    fn new() -> Self {
        Self { pairs: vec![] }
    }

    /// 条件リストから最小候補のペアを引く。
    fn best_lookup<'a>(&'a self, conds: &[(usize, u32)]) -> Option<(&'a [u32], Vec<(usize, u32)>)> {
        if conds.len() < 2 { return None; }
        let mut best: Option<(&[u32], Vec<(usize, u32)>)> = None;
        let mut best_len = usize::MAX;

        for pair in &self.pairs {
            let mut va = None;
            let mut vb = None;
            for &(idx, val) in conds {
                if idx == pair.himo_a { va = Some(val); }
                if idx == pair.himo_b { vb = Some(val); }
            }
            if let (Some(a), Some(b)) = (va, vb) {
                let cell_id = a as usize * pair.card_b as usize + b as usize;
                if cell_id < pair.cells.len() {
                    let cell = &pair.cells[cell_id];
                    if cell.is_empty() { continue; }
                    if cell.len() < best_len {
                        best_len = cell.len();
                        let remaining: Vec<(usize, u32)> = conds.iter()
                            .filter(|&&(idx, _)| idx != pair.himo_a && idx != pair.himo_b)
                            .copied()
                            .collect();
                        best = Some((cell, remaining));
                    }
                }
            }
        }
        best
    }

    /// delta 反映: セルに entity を追加
    fn cell_add(&mut self, pair_idx: usize, cell_id: usize, eid: u32) {
        let cell = &mut self.pairs[pair_idx].cells[cell_id];
        match cell.binary_search(&eid) {
            Ok(_) => {},
            Err(pos) => cell.insert(pos, eid),
        }
    }

    /// delta 反映: セルから entity を除去
    fn cell_remove(&mut self, pair_idx: usize, cell_id: usize, eid: u32) {
        let cell = &mut self.pairs[pair_idx].cells[cell_id];
        if let Ok(pos) = cell.binary_search(&eid) {
            cell.remove(pos);
        }
    }

    /// delta 伝播: 紐の値が変わった時に該当セルを更新
    fn apply_delta(&mut self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32, other_values: &[(usize, u32)]) {
        for pi in 0..self.pairs.len() {
            let (is_a, other_himo) = if self.pairs[pi].himo_a == himo_idx {
                (true, self.pairs[pi].himo_b)
            } else if self.pairs[pi].himo_b == himo_idx {
                (false, self.pairs[pi].himo_a)
            } else {
                continue;
            };

            let other_val = match other_values.iter().find(|&&(idx, _)| idx == other_himo) {
                Some(&(_, v)) => v,
                None => continue,
            };

            let card_b = self.pairs[pi].card_b;

            let old_cell_id = if is_a {
                old_val as usize * card_b as usize + other_val as usize
            } else {
                other_val as usize * card_b as usize + old_val as usize
            };
            if old_cell_id < self.pairs[pi].cells.len() {
                self.cell_remove(pi, old_cell_id, eid);
            }

            let new_cell_id = if is_a {
                new_val as usize * card_b as usize + other_val as usize
            } else {
                other_val as usize * card_b as usize + new_val as usize
            };
            if new_cell_id < self.pairs[pi].cells.len() {
                self.cell_add(pi, new_cell_id, eid);
            }
        }
    }
}

// ─── v27 PairTable (v26 のコピー) ───

#[cfg(feature = "v27")]
struct PairEntry {
    himo_a: usize,
    himo_b: usize,
    #[allow(dead_code)]
    card_a: u32,
    card_b: u32,
    cells: Vec<Vec<u32>>,
}

#[cfg(feature = "v27")]
struct PairTable {
    pairs: Vec<PairEntry>,
}

// ─── v27 NTupleView (任意 n 次元の観測窓) ───

#[cfg(feature = "v27")]
struct NTupleEntry {
    himos: Vec<usize>,
    cards: Vec<u32>,
    strides: Vec<usize>,
    cells: Vec<Vec<u32>>,
}

#[cfg(feature = "v27")]
impl NTupleEntry {
    fn cell_id_for(&self, values: &[u32]) -> Option<usize> {
        let mut id = 0usize;
        for i in 0..self.himos.len() {
            let v = values[i];
            if v >= self.cards[i] { return None; }
            id += v as usize * self.strides[i];
        }
        Some(id)
    }

    fn cell_add(&mut self, cell_id: usize, eid: u32) {
        let cell = &mut self.cells[cell_id];
        match cell.binary_search(&eid) {
            Ok(_) => {},
            Err(pos) => cell.insert(pos, eid),
        }
    }

    fn cell_remove(&mut self, cell_id: usize, eid: u32) {
        let cell = &mut self.cells[cell_id];
        if let Ok(pos) = cell.binary_search(&eid) {
            cell.remove(pos);
        }
    }
}

#[cfg(feature = "v27")]
struct NTupleTable {
    views: Vec<NTupleEntry>,
}

#[cfg(feature = "v27")]
impl NTupleTable {
    fn new() -> Self { Self { views: vec![] } }

    /// conds から該当する観測窓を選ぶ。
    /// 戻り値: (セルの entity スライス参照, 残りの条件)
    /// 優先: 条件紐をすべて含む view のうち、紐数が最大(残り条件が少ない)のもの。
    ///
    /// v27: read lock 下で借用したスライスをそのまま返す(ゼロコピー)。
    fn best_lookup_ref<'a>(
        &'a self,
        conds: &[(usize, u32)],
    ) -> Option<(&'a [u32], Vec<(usize, u32)>)> {
        if conds.len() < 2 { return None; }
        let mut best: Option<(&'a [u32], Vec<(usize, u32)>, usize)> = None;
        // best = (cell, remaining, covered_count)

        for v in &self.views {
            // view の全紐が conds で指定されているか?
            let mut vals_for_view: Vec<u32> = Vec::with_capacity(v.himos.len());
            let mut ok = true;
            for &h in &v.himos {
                match conds.iter().find(|&&(idx, _)| idx == h) {
                    Some(&(_, val)) => vals_for_view.push(val),
                    None => { ok = false; break; }
                }
            }
            if !ok { continue; }

            let cell_id = match v.cell_id_for(&vals_for_view) {
                Some(id) => id,
                None => continue,
            };
            if cell_id >= v.cells.len() { continue; }
            let cell = v.cells[cell_id].as_slice();
            let covered = v.himos.len();

            let remaining: Vec<(usize, u32)> = conds.iter()
                .filter(|&&(idx, _)| !v.himos.contains(&idx))
                .copied()
                .collect();

            let better = match &best {
                None => true,
                Some((_, _, c)) => covered > *c,
            };
            if better {
                best = Some((cell, remaining, covered));
            }
        }

        best.map(|(c, r, _)| (c, r))
    }

    fn apply_delta(&mut self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32, all_values: &[(usize, u32)]) {
        for view in &mut self.views {
            let pos_in_view = match view.himos.iter().position(|&h| h == himo_idx) {
                Some(p) => p,
                None => continue,
            };

            // view の他紐の値を収集。いずれか未 tie なら skip。
            let mut old_vals: Vec<u32> = Vec::with_capacity(view.himos.len());
            let mut new_vals: Vec<u32> = Vec::with_capacity(view.himos.len());
            let mut missing = false;
            for (i, &h) in view.himos.iter().enumerate() {
                if i == pos_in_view {
                    old_vals.push(old_val);
                    new_vals.push(new_val);
                } else {
                    match all_values.iter().find(|&&(idx, _)| idx == h) {
                        Some(&(_, v)) => {
                            old_vals.push(v);
                            new_vals.push(v);
                        }
                        None => { missing = true; break; }
                    }
                }
            }
            if missing { continue; }

            if old_val != u32::MAX {
                if let Some(old_id) = view.cell_id_for(&old_vals) {
                    view.cell_remove(old_id, eid);
                }
            }
            if new_val != u32::MAX {
                if let Some(new_id) = view.cell_id_for(&new_vals) {
                    view.cell_add(new_id, eid);
                }
            }
        }
    }
}

#[cfg(feature = "v27")]
impl PairTable {
    fn new() -> Self {
        Self { pairs: vec![] }
    }

    /// v27: 最小セルへの参照と残り条件を返す(read lock 下で借用)。
    /// スライス参照を返すので呼び出し側で read guard を保持すること。
    /// 内部で Vec を clone しないのでホットパスでもゼロコピー。
    fn best_lookup_ref<'a>(
        &'a self,
        conds: &[(usize, u32)],
    ) -> Option<(&'a [u32], Vec<(usize, u32)>)> {
        if conds.len() < 2 { return None; }
        let mut best: Option<(&'a [u32], Vec<(usize, u32)>)> = None;
        let mut best_len = usize::MAX;

        for pair in &self.pairs {
            let mut va = None;
            let mut vb = None;
            for &(idx, val) in conds {
                if idx == pair.himo_a { va = Some(val); }
                if idx == pair.himo_b { vb = Some(val); }
            }
            if let (Some(a), Some(b)) = (va, vb) {
                let cell_id = a as usize * pair.card_b as usize + b as usize;
                if cell_id < pair.cells.len() {
                    let cell = pair.cells[cell_id].as_slice();
                    if cell.is_empty() { continue; }
                    if cell.len() < best_len {
                        best_len = cell.len();
                        let remaining: Vec<(usize, u32)> = conds.iter()
                            .filter(|&&(idx, _)| idx != pair.himo_a && idx != pair.himo_b)
                            .copied()
                            .collect();
                        best = Some((cell, remaining));
                    }
                }
            }
        }
        best
    }

    fn cell_add(&mut self, pair_idx: usize, cell_id: usize, eid: u32) {
        let cell = &mut self.pairs[pair_idx].cells[cell_id];
        match cell.binary_search(&eid) {
            Ok(_) => {},
            Err(pos) => cell.insert(pos, eid),
        }
    }

    fn cell_remove(&mut self, pair_idx: usize, cell_id: usize, eid: u32) {
        let cell = &mut self.pairs[pair_idx].cells[cell_id];
        if let Ok(pos) = cell.binary_search(&eid) {
            cell.remove(pos);
        }
    }

    fn apply_delta(&mut self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32, other_values: &[(usize, u32)]) {
        for pi in 0..self.pairs.len() {
            let (is_a, other_himo) = if self.pairs[pi].himo_a == himo_idx {
                (true, self.pairs[pi].himo_b)
            } else if self.pairs[pi].himo_b == himo_idx {
                (false, self.pairs[pi].himo_a)
            } else {
                continue;
            };

            let other_val = match other_values.iter().find(|&&(idx, _)| idx == other_himo) {
                Some(&(_, v)) => v,
                None => continue,
            };

            let card_b = self.pairs[pi].card_b;

            let old_cell_id = if is_a {
                old_val as usize * card_b as usize + other_val as usize
            } else {
                other_val as usize * card_b as usize + old_val as usize
            };
            if old_cell_id < self.pairs[pi].cells.len() {
                self.cell_remove(pi, old_cell_id, eid);
            }

            let new_cell_id = if is_a {
                new_val as usize * card_b as usize + other_val as usize
            } else {
                other_val as usize * card_b as usize + new_val as usize
            };
            if new_cell_id < self.pairs[pi].cells.len() {
                self.cell_add(pi, new_cell_id, eid);
            }
        }
    }
}

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
    entities: EntitySet,
    contents: ContentStore,
    undo: UndoLog,
    #[cfg(all(feature = "v26", not(feature = "v27")))]
    pairs: PairTable,
    #[cfg(feature = "v27")]
    pairs: std::sync::RwLock<PairTable>,
    #[cfg(feature = "v27")]
    tuples: std::sync::RwLock<NTupleTable>,
    /// 非同期書き込みキュー。`create_concurrent` で有効化される。
    #[cfg(feature = "v27")]
    write_queue: Option<std::sync::Arc<crate::write_queue::WriteQueue>>,
    /// consumer スレッドへの shutdown 通知。`Drop` で true に。
    #[cfg(feature = "v27")]
    shutdown_flag: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// consumer スレッドハンドル。Drop で join。
    #[cfg(feature = "v27")]
    consumer_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// writer が push した累積件数。tie_async/untie_async/delete_async で +1。
    #[cfg(feature = "v27")]
    push_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// consumer が apply 完了した累積件数。apply_count >= push_count が同期点。
    #[cfg(feature = "v27")]
    apply_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// v28 WAL。`create_concurrent_with_wal` or `open_concurrent_with_wal` で有効化。
    /// Some なら tie_async/untie_async/delete_async は WAL append を先に実行する。
    #[cfg(feature = "v27")]
    wal: Option<std::sync::Arc<crate::wal::Wal>>,
    /// 背景 fsync が最後に completed した LSN。
    #[cfg(feature = "v27")]
    durable_lsn: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// v32: この Engine を所有する peer の id。分散時 eid の上位 32bit。
    #[cfg(feature = "v32")]
    peer_id: std::sync::atomic::AtomicU32,
    /// v32: LWW 用に (eid, himo) → 最後の HLC を記録。
    #[cfg(feature = "v32")]
    hlc_store: std::sync::Arc<crate::hlc_store::HlcStore>,
    /// v32 Phase C: 自 peer の ed25519 鍵ペア。None なら署名しない/検証もしない。
    #[cfg(feature = "v32")]
    keypair: std::sync::RwLock<Option<std::sync::Arc<crate::keys::Keypair>>>,
    /// v32 Phase C: 他 peer の pubkey TOFU ストア。Syncer が verify に使う。
    #[cfg(feature = "v32")]
    pubkeys: std::sync::Arc<crate::keys::PubkeyStore>,
    /// v32 Phase C: ACL(書き込み許可 peer の集合)。Syncer が enforce する。
    #[cfg(feature = "v32")]
    acl: std::sync::Arc<crate::acl::Acl>,
    /// v32 エッジ用: true なら書き込み API が panic、sync 経由 (remote_*_apply) のみ受ける。
    #[cfg(feature = "v32")]
    is_replica: std::sync::atomic::AtomicBool,
    /// 大容量 blob 用 store(画像/動画/モデル等)。`set_blob_store` で注入。
    /// 未設定なら `blob_store()` は None を返す。
    blob_store: std::sync::RwLock<Option<std::sync::Arc<dyn crate::blob_store::BlobStore>>>,
    /// changefeed: WAL に durable 化した record を listener に push する。
    /// consumer スレッドが背景 fsync 完了後に発火。
    #[cfg(feature = "v32")]
    change_listeners: std::sync::Arc<std::sync::RwLock<Vec<std::sync::Arc<dyn crate::changefeed::ChangeListener>>>>,
    /// changefeed: 次に listener へ emit すべき WAL offset。
    /// add_change_listener 時に wal.head() に同期され、それ以降の commit のみが流れる。
    #[cfg(feature = "v32")]
    change_emit_offset: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// v33: 受信した Vocab op の `(author_peer, remote_vid) → local_vid` mapping。
    /// Symbol 型 himo の Tie を受信した時に remote_vid を local_vid に変換して apply する。
    /// peer-local に保持(replica でも独立、open 時は空、受信で徐々に埋まる)。
    #[cfg(feature = "v33")]
    peer_vocab_map: std::sync::RwLock<std::collections::HashMap<(crate::PeerId, u32), u32>>,
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

    #[cfg(all(not(target_arch = "wasm32"), not(feature = "v33")))]
    pub fn create(path: &str) -> io::Result<Self> {
        Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)
    }

    /// v33: `create` は WAL 有効 + `Arc<Self>` 返しに統合。
    /// 旧挙動は `create_standalone` で取れる。
    #[cfg(all(not(target_arch = "wasm32"), feature = "v33"))]
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
    pub fn create_full_with_cyl(path: &str, max_entities: u32, vocab_data_size: Option<usize>, max_himos: Option<u32>, content_data_size: Option<usize>, cyl_max_values: Option<u32>) -> io::Result<Self> {
        let vds = vocab_data_size.unwrap_or(DEFAULT_VOCAB_DATA_SIZE);
        let max_himos = max_himos.unwrap_or(DEFAULT_MAX_HIMOS);
        let layout = Layout::compute(max_entities, max_himos, vds, content_data_size, cyl_max_values);

        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
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

        let base = mmap.as_mut_ptr();

        let entities = EntitySet::init(
            unsafe { Region::new(base.add(layout.entities_off), layout.entities_size) },
            max_entities,
        );
        let undo = UndoLog::init(
            unsafe { Region::new(base.add(layout.undo_off), layout.undo_size) },
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
            himos: Vec::new(), entities, contents, undo,
            #[cfg(all(feature = "v26", not(feature = "v27")))]
            pairs: PairTable::new(),
            #[cfg(feature = "v27")]
            pairs: std::sync::RwLock::new(PairTable::new()),
            #[cfg(feature = "v27")]
            tuples: std::sync::RwLock::new(NTupleTable::new()),
            #[cfg(feature = "v27")]
            write_queue: None,
            #[cfg(feature = "v27")]
            shutdown_flag: None,
            #[cfg(feature = "v27")]
            consumer_handle: std::sync::Mutex::new(None),
            #[cfg(feature = "v27")]
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "v27")]
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "v27")]
            wal: None,
            #[cfg(feature = "v27")]
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "v32")]
            peer_id: std::sync::atomic::AtomicU32::new(0),
            #[cfg(feature = "v32")]
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            #[cfg(feature = "v32")]
            keypair: std::sync::RwLock::new(None),
            #[cfg(feature = "v32")]
            pubkeys: std::sync::Arc::new(crate::keys::PubkeyStore::new()),
            #[cfg(feature = "v32")]
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            #[cfg(feature = "v32")]
            is_replica: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            #[cfg(feature = "v32")]
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            #[cfg(feature = "v32")]
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                crate::wal::HEADER_SIZE as u64,
            )),
            #[cfg(feature = "v33")]
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            backing: Backing::Mmap(mmap),
        })
    }

    /// 現行の単独 Engine を開く(WAL 無し、&mut self で mutation)。
    /// v32 以前: `Engine::open` の別名。
    /// v33 以降: `Engine::open` が WAL 付き `Arc<Self>` を返すようになるため、
    /// 明示的に単独 Engine が欲しい場合はこちらを使う。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_standalone(path: &str) -> io::Result<Self> {
        Self::open_internal(path, /*verify_region_crc=*/ true)
    }

    #[cfg(all(not(target_arch = "wasm32"), not(feature = "v33")))]
    pub fn open(path: &str) -> io::Result<Self> {
        Self::open_internal(path, /*verify_region_crc=*/ true)
    }

    /// v33: `open` は WAL 有効 + `Arc<Self>` 返しに統合。
    /// 旧挙動は `open_standalone` で取れる。
    #[cfg(all(not(target_arch = "wasm32"), feature = "v33"))]
    pub fn open(path: &str) -> io::Result<std::sync::Arc<Self>> {
        Self::open_concurrent_with_wal(path, 16 * 1024 * 1024)
    }

    /// 内部用: region CRC 検証を skip できる open。WAL ルート用。
    #[cfg(not(target_arch = "wasm32"))]
    fn open_internal(path: &str, verify_region_crc: bool) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        // v29: ファイルサイズ検証 — truncate で SIGBUS を防ぐ
        let file_size = file.metadata()?.len();
        Self::validate_file_size(&file, file_size)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let mut eng = Self::load_from_backing(Backing::Mmap(mmap))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        eng.path = path.to_string();
        if verify_region_crc {
            // .crc ファイルがあれば全 region CRC 検証
            eng.verify_region_crcs()?;
        }
        Ok(eng)
    }

    /// 全 region の CRC テーブルを計算する。
    fn compute_region_crc_table(&self) -> crate::integrity::CrcTable {
        use crate::integrity::{CrcTable, RegionKind, fnv1a_region};
        let file_size = self.layout.total_size as u64;
        let mut table = CrcTable::new(self.max_himos, file_size);
        let buf = match &self.backing {
            Backing::Mmap(m) => &m[..],
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

        if file_size < layout.total_size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "file truncated: {} bytes, expected {} — backup からリストアが必要",
                    file_size, layout.total_size,
                ),
            ));
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
        if version != FILE_VERSION {
            return Err(format!(
                "unsupported EnchuDB file version {} (expected {}). dev phase — recreate the DB.",
                version, FILE_VERSION
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

        let entities = EntitySet::load(
            unsafe { Region::new(base.add(layout.entities_off), layout.entities_size) },
            max_entities,
        );
        let undo = UndoLog::load(
            unsafe { Region::new(base.add(layout.undo_off), layout.undo_size) },
        );
        let vocab = Vocabulary::load(
            unsafe { Region::new(base.add(layout.vocab_data_off), layout.vocab_data_size) },
            unsafe { Region::new(base.add(layout.vocab_offsets_off), layout.vocab_offsets_size) },
            unsafe { Region::new(base.add(layout.vocab_index_off), layout.vocab_index_size) },
        );
        let himo_reg = Vocabulary::load(
            unsafe { Region::new(base.add(layout.himoreg_data_off), layout.himoreg_data_size) },
            unsafe { Region::new(base.add(layout.himoreg_offsets_off), layout.himoreg_offsets_size) },
            unsafe { Region::new(base.add(layout.himoreg_index_off), layout.himoreg_index_size) },
        );
        let contents = ContentStore::load(
            unsafe { Region::new(base.add(layout.content_index_off), layout.content_index_size) },
            unsafe { Region::new(base.add(layout.content_data_off), layout.content_data_size) },
        );

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

            #[cfg(not(feature = "v27"))]
            let hs = HimoStore::load(
                unsafe { Region::new(base.add(layout.himo_col_off(hid)), layout.himo_col_size) },
                unsafe { Region::new(base.add(layout.himo_cyl_a_off(hid)), layout.himo_cyl_size) },
                unsafe { Region::new(base.add(layout.himo_cyl_b_off(hid)), layout.himo_cyl_size) },
                ht, effective_mv,
            );
            #[cfg(feature = "v27")]
            let hs = HimoStore::load(
                unsafe { Region::new(base.add(layout.himo_col_off(hid)), layout.himo_col_size) },
                ht, effective_mv,
            );

            himo_names.push(name);
            himo_types.push(ht);
            himo_max_values.push(mv);
            himos.push(hs);
        }

        let mut eng = Self {
            path: String::new(), layout, max_entities, max_himos,
            vocab, himo_reg,
            himo_names, himo_types, himo_max_values,
            himos, entities, contents, undo,
            #[cfg(all(feature = "v26", not(feature = "v27")))]
            pairs: PairTable::new(),
            #[cfg(feature = "v27")]
            pairs: std::sync::RwLock::new(PairTable::new()),
            #[cfg(feature = "v27")]
            tuples: std::sync::RwLock::new(NTupleTable::new()),
            #[cfg(feature = "v27")]
            write_queue: None,
            #[cfg(feature = "v27")]
            shutdown_flag: None,
            #[cfg(feature = "v27")]
            consumer_handle: std::sync::Mutex::new(None),
            #[cfg(feature = "v27")]
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "v27")]
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "v27")]
            wal: None,
            #[cfg(feature = "v27")]
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "v32")]
            peer_id: std::sync::atomic::AtomicU32::new(0),
            #[cfg(feature = "v32")]
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            #[cfg(feature = "v32")]
            keypair: std::sync::RwLock::new(None),
            #[cfg(feature = "v32")]
            pubkeys: std::sync::Arc::new(crate::keys::PubkeyStore::new()),
            #[cfg(feature = "v32")]
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            #[cfg(feature = "v32")]
            is_replica: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            #[cfg(feature = "v32")]
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            #[cfg(feature = "v32")]
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                crate::wal::HEADER_SIZE as u64,
            )),
            #[cfg(feature = "v33")]
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            backing,
        };

        // v32: header から peer_id を復元
        #[cfg(feature = "v32")]
        {
            let hdr = eng.backing.header_mut();
            let peer = u32::from_le_bytes(hdr[H_PEER_ID..H_PEER_ID + 4].try_into().unwrap());
            eng.peer_id.store(peer, std::sync::atomic::Ordering::Release);
        }

        if eng.undo.pending_count() > 0 {
            eng.recover();
        }
        eng.rebuild();

        #[cfg(feature = "v27")]
        {
            eng.rebuild_pairs();
            eng.restore_persisted_views()?;
        }

        Ok(eng)
    }

    // ──── v32: エッジ向け read-only replica ────

    /// 既存 DB を read-only replica として開く。
    /// 書き込み API (tie / untie / delete / content / entity 等) は panic する。
    /// Syncer 経由 (remote_*_apply) での書き込みのみ受け付ける。
    /// エッジ node はこちらで起動する。
    #[cfg(feature = "v32")]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_replica(path: &str) -> io::Result<Self> {
        let eng = Self::open_standalone(path)?;
        eng.is_replica.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// WAL + 並行 write queue 付きで replica open。通常の Engine と同じく Arc 共有可能。
    #[cfg(feature = "v32")]
    #[cfg(feature = "v27")]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_concurrent_replica(path: &str, wal_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::open_concurrent_with_wal(path, wal_capacity)?;
        eng.is_replica.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// replica モードの動的切替。true にすると書き込み API が panic する。
    /// false に戻せば通常 DB として書き込み可能に復帰する。
    #[cfg(feature = "v32")]
    pub fn set_replica_mode(&self, on: bool) {
        self.is_replica.store(on, std::sync::atomic::Ordering::Release);
    }

    /// 現在 replica モードか。
    #[cfg(feature = "v32")]
    pub fn is_replica(&self) -> bool {
        self.is_replica.load(std::sync::atomic::Ordering::Acquire)
    }

    /// 書き込み API が呼ばれた時の guard。replica なら panic。
    #[cfg(feature = "v32")]
    #[inline(always)]
    fn check_writable(&self) {
        if self.is_replica.load(std::sync::atomic::Ordering::Acquire) {
            panic!("Engine is in replica mode; writes must go through Syncer (remote_*_apply)");
        }
    }

    /// v32 無効時は no-op。
    #[cfg(not(feature = "v32"))]
    #[inline(always)]
    fn check_writable(&self) {}

    // ──── entity ────

    pub fn entity(&self) -> crate::EntityId {
        self.check_writable();
        let local = self.entities.allocate();
        self.undo.record(local, 0xFFFF, &[1, 0, 0, 0]); // entity created
        #[cfg(feature = "v32")]
        {
            let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
            crate::make_eid(peer, local)
        }
        #[cfg(not(feature = "v32"))]
        {
            local as crate::EntityId
        }
    }

    pub fn entities(&self) -> Vec<crate::EntityId> {
        #[cfg(feature = "v32")]
        let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
        #[cfg(not(feature = "v32"))]
        let peer: u32 = 0;
        self.entities.iter().into_iter()
            .map(|local| crate::make_eid(peer, local))
            .collect()
    }
    pub fn entity_count(&self) -> u32 { self.entities.count() }
    pub fn next_eid(&self) -> crate::EntityId {
        #[cfg(feature = "v32")]
        let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
        #[cfg(not(feature = "v32"))]
        let peer: u32 = 0;
        crate::make_eid(peer, self.entities.next_eid())
    }

    // ──── v32: peer_id ────

    /// この Engine を所有する peer の id を設定。WAL / DB header にも反映される。
    /// 起動時に 1 回だけ呼ぶ想定。
    #[cfg(feature = "v32")]
    pub fn set_peer_id(&self, peer: crate::PeerId) {
        self.peer_id.store(peer, std::sync::atomic::Ordering::Release);
        // mmap の header に即書き込み(CRC 保護外なので再計算不要)
        self.backing.header_mut()[H_PEER_ID..H_PEER_ID + 4]
            .copy_from_slice(&peer.to_le_bytes());
        #[cfg(feature = "v27")]
        if let Some(wal) = self.wal.as_ref() {
            wal.set_peer_id(peer);
        }
    }

    /// 現在の peer id。
    #[cfg(feature = "v32")]
    pub fn peer_id(&self) -> crate::PeerId {
        self.peer_id.load(std::sync::atomic::Ordering::Acquire)
    }

    /// LWW 用 HlcStore への参照(sync モジュールが使う)。
    #[cfg(feature = "v32")]
    pub fn hlc_store(&self) -> &std::sync::Arc<crate::hlc_store::HlcStore> {
        &self.hlc_store
    }

    /// Phase C: 自 peer の鍵ペアを設定。WAL にも反映される。None で署名 off。
    #[cfg(feature = "v32")]
    pub fn set_keypair(&self, kp: Option<std::sync::Arc<crate::keys::Keypair>>) {
        *self.keypair.write().unwrap() = kp.clone();
        #[cfg(feature = "v27")]
        if let Some(wal) = self.wal.as_ref() {
            wal.set_keypair(kp.clone());
        }
        if let Some(ref k) = kp {
            let peer = self.peer_id();
            self.pubkeys.force_register(peer, &k.public_bytes());
        }
    }

    /// Phase C: 他 peer の pubkey ストアへの参照。
    #[cfg(feature = "v32")]
    pub fn pubkeys(&self) -> &std::sync::Arc<crate::keys::PubkeyStore> {
        &self.pubkeys
    }

    /// Phase C: ACL への参照。Syncer が受信 op の author を enforce する。
    #[cfg(feature = "v32")]
    pub fn acl(&self) -> &std::sync::Arc<crate::acl::Acl> {
        &self.acl
    }

    /// Phase C: 自 peer の鍵ペアを返す。
    #[cfg(feature = "v32")]
    pub fn keypair(&self) -> Option<std::sync::Arc<crate::keys::Keypair>> {
        self.keypair.read().unwrap().clone()
    }

    /// WAL への参照(sync モジュールが publish に使う)。
    #[cfg(all(feature = "v27", feature = "v32"))]
    pub fn wal_arc(&self) -> Option<std::sync::Arc<crate::wal::Wal>> {
        self.wal.clone()
    }

    // ──── v32: リモート peer から pull したレコードを apply する ────
    // これらは LWW 判定を通った後の無条件 apply。HlcStore は Syncer が先に更新済み。

    /// リモート peer から届いた Tie を apply。WAL には書き戻さない(2 重送信防止)。
    #[cfg(feature = "v32")]
    pub fn remote_tie_apply(&self, eid: crate::EntityId, himo_id: u16, value: u32) {
        let local = crate::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return; }
        // entity 未確保ならローカル側の EntitySet に登録(eid は peer 側が決めた値)
        self.entities.ensure_live(local);
        self.himos[hid].set(local, value);
    }

    /// リモート peer から届いた Untie を apply。
    #[cfg(feature = "v32")]
    pub fn remote_untie_apply(&self, eid: crate::EntityId, himo_id: u16) {
        let local = crate::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return; }
        self.himos[hid].remove(local);
    }

    /// リモート peer から届いた Delete を apply。
    #[cfg(feature = "v32")]
    pub fn remote_delete_apply(&self, eid: crate::EntityId) {
        let local = crate::eid_local(eid);
        for hid in 0..self.himos.len() {
            self.himos[hid].remove(local);
        }
        self.entities.free(local);
    }

    /// リモート peer から届いた Content 書き込みを apply。
    #[cfg(feature = "v32")]
    pub fn remote_content_apply(&self, eid: crate::EntityId, key: &str, data: &[u8]) {
        let local = crate::eid_local(eid);
        self.entities.ensure_live(local);
        self.contents.set(local, key, data);
    }

    /// v33: リモート peer から届いた Vocab op を apply。
    /// `bytes` を local vocab に insert し、local_vid を取得して
    /// `(author_peer, remote_vid) → local_vid` の mapping を記録する。
    /// 後続の Tie { value: remote_vid } を受信したら `translate_remote_vid` で local_vid に変換。
    #[cfg(feature = "v33")]
    pub fn remote_vocab_apply(&self, author_peer: crate::PeerId, remote_vid: u32, bytes: &[u8]) {
        let local_vid = self.vocab.get_or_insert(bytes);
        let mut map = self.peer_vocab_map.write().unwrap();
        map.insert((author_peer, remote_vid), local_vid);
    }

    /// v33: Symbol 型 himo の Tie を受信した際、remote vid を local vid に変換する。
    /// Symbol 以外の himo、または mapping 未登録なら元値をそのまま返す。
    #[cfg(feature = "v33")]
    pub fn translate_remote_vid(&self, author_peer: crate::PeerId, himo_id: u16, value: u32) -> u32 {
        let hid = himo_id as usize;
        if hid >= self.himo_types.len() { return value; }
        if self.himo_types[hid] != HimoType::Symbol { return value; }
        let map = self.peer_vocab_map.read().unwrap();
        *map.get(&(author_peer, value)).unwrap_or(&value)
    }

    // ──── tie ────

    pub fn define_himo(&mut self, himo: &str, ht: HimoType, max_values: u32) {
        self.ensure_himo(himo, ht, max_values);
    }

    pub fn tie_text(&mut self, eid: crate::EntityId, himo: &str, value: &str) {
        self.check_writable();
        let eid = crate::eid_local(eid);
        let vid = self.vocab.get_or_insert(value.as_bytes());
        let hid = self.ensure_himo(himo, HimoType::Symbol, 0);
        debug_assert_eq!(self.himo_types[hid], HimoType::Symbol, "tie_text on non-Symbol himo '{}'", himo);
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, vid);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), vid);
    }

    pub fn tie(&mut self, eid: crate::EntityId, himo: &str, value: u32) {
        self.check_writable();
        let eid = crate::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, HimoType::Value, 0);
        debug_assert!(self.himo_types[hid] == HimoType::Value || self.himo_types[hid] == HimoType::Ref, "tie on non-Value himo '{}'", himo);
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, value);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), value);
    }

    pub fn tie_ref(&mut self, eid: crate::EntityId, himo: &str, target_eid: crate::EntityId) {
        self.check_writable();
        let eid = crate::eid_local(eid);
        let target_eid = crate::eid_local(target_eid);
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, HimoType::Ref, 0);
        debug_assert!(self.himo_types[hid] == HimoType::Ref || self.himo_types[hid] == HimoType::Value, "tie_ref on non-Ref himo '{}'", himo);
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, target_eid);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), target_eid);
    }

    // ──── tie（定義済み紐、&self で並行書き込み可）────

    /// 定義済みの紐に文字列を張る。&selfで呼べる（Arc共有のまま書き込み可）。
    /// 紐が未定義ならpanic。define_himo を先に呼ぶこと。
    pub fn tie_text_to(&self, eid: crate::EntityId, himo: &str, value: &str) {
        self.check_writable();
        let eid = crate::eid_local(eid);
        let vid = self.vocab.get_or_insert(value.as_bytes());
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        debug_assert_eq!(self.himo_types[hid], HimoType::Symbol, "tie_text_to on non-Symbol himo '{}'", himo);
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, vid);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), vid);
    }

    /// 定義済みの紐にu32値を張る。&selfで呼べる。
    pub fn tie_to(&self, eid: crate::EntityId, himo: &str, value: u32) {
        self.check_writable();
        let eid = crate::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        debug_assert!(self.himo_types[hid] == HimoType::Value || self.himo_types[hid] == HimoType::Ref, "tie_to on non-Value himo '{}'", himo);
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, value);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), value);
    }

    /// 定義済みの紐にentity参照を張る。&selfで呼べる。
    pub fn tie_ref_to(&self, eid: crate::EntityId, himo: &str, target_eid: crate::EntityId) {
        self.check_writable();
        let eid = crate::eid_local(eid);
        let target_eid = crate::eid_local(target_eid);
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        debug_assert!(self.himo_types[hid] == HimoType::Ref || self.himo_types[hid] == HimoType::Value, "tie_ref_to on non-Ref himo '{}'", himo);
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, target_eid);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), target_eid);
    }

    // ──── untie ────

    pub fn untie(&self, eid: crate::EntityId, himo: &str) {
        self.check_writable();
        let eid = crate::eid_local(eid);
        if let Some(hid) = self.himo_id(himo) {
            self.record_undo(eid, hid);
            #[cfg(feature = "v27")]
            let old_val = self.himos[hid].get_value(eid);
            self.himos[hid].remove(eid);
            #[cfg(feature = "v27")]
            if let Some(ov) = old_val {
                self.apply_pair_delta_internal(eid, hid, ov, u32::MAX);
            }
        }
    }

    // ──── delete ────

    pub fn delete(&self, eid: crate::EntityId) {
        self.check_writable();
        let eid = crate::eid_local(eid);
        self.undo.record(eid, 0xFFFF, &[2, 0, 0, 0]); // entity deleted
        for hid in 0..self.himos.len() {
            self.record_undo(eid, hid);
            #[cfg(feature = "v27")]
            let old_val = self.himos[hid].get_value(eid);
            self.himos[hid].remove(eid);
            #[cfg(feature = "v27")]
            if let Some(ov) = old_val {
                self.apply_pair_delta_internal(eid, hid, ov, u32::MAX);
            }
        }
        self.entities.free(eid);
    }

    // ──── トランザクション ────

    /// v32 以前: undo ログを clear(WAL には触らない)。
    /// v33 以降: undo clear + WAL Commit marker append を同時に行う。
    /// 片方だけ呼ぶと状態が割れる現状 (BUGS.md 層 2) を解消。
    pub fn commit(&self) {
        self.undo.commit();
        #[cfg(all(feature = "v33", feature = "v27"))]
        if let Some(wal) = self.wal.as_ref() {
            let _ = wal.append(crate::wal::WalOp::Commit);
        }
    }

    pub fn rollback(&self) {
        self.replay_undo();
        self.undo.commit();
    }

    fn record_undo(&self, eid: u32, hid: usize) {
        if hid < self.himos.len() {
            let old = self.himos[hid].get_raw_bytes(eid);
            self.undo.record(eid, hid as u16, &old);
        }
    }

    fn recover(&mut self) {
        self.replay_undo();
        self.undo.commit();
    }

    fn replay_undo(&self) {
        for (eid, dim_id, old_value) in self.undo.entries_reverse() {
            if dim_id == 0xFFFF {
                // entity lifecycle marker
                match old_value[0] {
                    1 => self.entities.free(eid),   // undo create → free
                    2 => self.entities.revive(eid),  // undo delete → revive
                    _ => {}
                }
            } else {
                let hid = dim_id as usize;
                if hid < self.himos.len() {
                    self.himos[hid].restore(eid, &old_value);
                }
            }
        }
    }

    // ──── content ────

    pub fn content(&self, eid: crate::EntityId, key: &str, data: &[u8]) {
        self.check_writable();
        self.contents.set(crate::eid_local(eid), key, data);
    }

    pub fn get_content(&self, eid: crate::EntityId, key: &str) -> Option<&[u8]> {
        self.contents.get(crate::eid_local(eid), key)
    }

    // ──── changefeed (WAL 変更通知) ────

    /// WAL に durable 化した record を listener に push する。
    /// consumer スレッドが背景 fsync 完了後に発火、HLC 昇順で渡す。
    /// 詳細は [`crate::changefeed`] のドキュメント参照。
    ///
    /// 初回 listener 追加時に emit cursor を現在の `wal.head()` に揃えるので、
    /// 過去の commit は流れない(必要なら `audit()` で取得して resume すること)。
    #[cfg(feature = "v32")]
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
            #[cfg(feature = "v27")]
            if let Some(wal) = self.wal.as_ref() {
                self.change_emit_offset
                    .store(wal.head(), std::sync::atomic::Ordering::Release);
            }
        }
    }

    /// 登録済み listener 数(テスト/監視用)。
    #[cfg(feature = "v32")]
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

    pub fn get_text(&self, eid: crate::EntityId, himo: &str) -> Option<&[u8]> {
        let eid = crate::eid_local(eid);
        let hid = self.himo_id(himo)?;
        if self.himo_types[hid] != HimoType::Symbol { return None; }
        let vid = self.himos[hid].get_value(eid)?;
        Some(self.vocab.get(vid))
    }

    pub fn get(&self, eid: crate::EntityId, himo: &str) -> Option<u32> {
        let eid = crate::eid_local(eid);
        let hid = self.himo_id(himo)?;
        self.himos[hid].get_value(eid)
    }

    /// 指定 entity 群の紐値を合計
    pub fn sum(&self, himo: &str, eids: &[crate::EntityId]) -> u64 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(crate::eid_local(eid)) {
                total += v as u64;
            }
        }
        total
    }

    /// 最小値
    pub fn min(&self, himo: &str, eids: &[crate::EntityId]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(crate::eid_local(eid)) {
                result = Some(result.map_or(v, |cur: u32| cur.min(v)));
            }
        }
        result
    }

    /// 最大値
    pub fn max(&self, himo: &str, eids: &[crate::EntityId]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(crate::eid_local(eid)) {
                result = Some(result.map_or(v, |cur: u32| cur.max(v)));
            }
        }
        result
    }

    /// 平均（整数除算）
    pub fn avg(&self, himo: &str, eids: &[crate::EntityId]) -> Option<u64> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        let mut count: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(crate::eid_local(eid)) {
                total += v as u64;
                count += 1;
            }
        }
        if count == 0 { None } else { Some(total / count) }
    }

    /// 値を持つ entity の数
    pub fn count(&self, himo: &str, eids: &[crate::EntityId]) -> u32 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut n: u32 = 0;
        for &eid in eids {
            if hs.get_value(crate::eid_local(eid)).is_some() { n += 1; }
        }
        n
    }

    /// GROUP BY + SUM — group_himo の値でグループ化し、sum_himo の値を合計
    pub fn group_sum(&self, group_himo: &str, sum_himo: &str, eids: &[crate::EntityId]) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let mut map: Vec<(u32, u64)> = Vec::new();
        for &eid in eids {
            let local = crate::eid_local(eid);
            if let (Some(group), Some(val)) = (gs.get_value(local), ss.get_value(local)) {
                if let Some(entry) = map.iter_mut().find(|(k, _)| *k == group) {
                    entry.1 += val as u64;
                } else {
                    map.push((group, val as u64));
                }
            }
        }
        map
    }

    // ──── 範囲クエリ ────

    /// 範囲内の全値に合致する entity を返す（min..=max）
    pub fn pull_range(&self, himo: &str, min: u32, max: u32) -> Vec<crate::EntityId> {
        let idx = match self.himo_id(himo) { Some(h) => h, None => return vec![] };
        let hs = &self.himos[idx];
        let mut result = Vec::new();
        for v in min..=max {
            #[cfg(feature = "v27")]
            {
                for local in &hs.pull(v) {
                    result.push(*local as crate::EntityId);
                }
            }
            #[cfg(not(feature = "v27"))]
            {
                for local in hs.pull(v) {
                    result.push(*local as crate::EntityId);
                }
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
    pub fn tie_date(&mut self, eid: crate::EntityId, himo: &str, year: u32, month: u32, day: u32) {
        self.tie(eid, himo, Self::date_to_days(year, month, day));
    }

    /// epoch 日数から (year, month, day) を返す
    pub fn get_date(&self, eid: crate::EntityId, himo: &str) -> Option<(u32, u32, u32)> {
        self.get(eid, himo).map(Self::days_to_date)
    }

    /// 日付範囲で pull_range
    pub fn pull_date_range(&self, himo: &str, from: (u32, u32, u32), to: (u32, u32, u32)) -> Vec<crate::EntityId> {
        let min = Self::date_to_days(from.0, from.1, from.2);
        let max = Self::date_to_days(to.0, to.1, to.2);
        self.pull_range(himo, min, max)
    }

    pub fn vocab_id(&self, text: &str) -> Option<u32> { self.vocab.lookup(text.as_bytes()) }

    /// 紐の文脈で文字列のvocab IDを探す。その紐にぶら下がってる値だけ調べる。
    pub fn find_value(&self, himo: &str, text: &str) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let text_bytes = text.as_bytes();
        #[cfg(not(feature = "v27"))]
        let vals = {
            let cyl = self.himos[hid].cylinder();
            let n = cyl.total();
            if n == 0 {
                let delta = self.himos[hid].delta_eids();
                for &eid in delta {
                    if let Some(vid) = self.himos[hid].get_value(eid) {
                        if self.vocab.get(vid) == text_bytes {
                            return Some(vid);
                        }
                    }
                }
                return None;
            }
            cyl.unique_values(n)
        };
        #[cfg(feature = "v27")]
        let vals = self.himos[hid].unique_values();
        for vid in vals {
            if self.vocab.get(vid) == text_bytes {
                return Some(vid);
            }
        }
        #[cfg(not(feature = "v27"))]
        {
            let delta = self.himos[hid].delta_eids();
            for &eid in delta {
                if let Some(vid) = self.himos[hid].get_value(eid) {
                    if self.vocab.get(vid) == text_bytes {
                        return Some(vid);
                    }
                }
            }
        }
        None
    }

    pub fn himos_of(&self, eid: crate::EntityId) -> Vec<&str> {
        let eid = crate::eid_local(eid);
        self.himos.iter().enumerate()
            .filter(|(_, ds)| ds.get_value(eid).is_some())
            .map(|(i, _)| self.himo_names[i].as_str())
            .collect()
    }
    /// 1 entity の全フィールドを一括取得。HashMap ルックアップ 0 回。
    pub fn get_entity(&self, eid: crate::EntityId) -> Vec<(&str, EntityValue<'_>)> {
        let eid = crate::eid_local(eid);
        let mut fields = Vec::with_capacity(self.himos.len());
        for (i, hs) in self.himos.iter().enumerate() {
            if let Some(raw) = hs.get_value(eid) {
                let val = match self.himo_types[i] {
                    HimoType::Symbol => EntityValue::Text(self.vocab.get(raw)),
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
    #[cfg(feature = "v27")]
    pub fn himo_cardinality(&self, himo: &str) -> Option<u32> {
        let idx = self.himo_id(himo)?;
        Some(self.himos[idx].unique_count())
    }

    // ──── 紐を引く（Cylinder 経由）────

    /// Cylinder + Bitmap キャッシュを再構築。delta をクリア。
    pub fn rebuild(&self) {
        for ds in &self.himos { ds.rebuild_cylinder(); }
    }

    /// v26: ペアテーブルを構築。全紐ペアの二次元テーブルをデルタシンクで事前計算。
    /// rebuild() の後に呼ぶ。
    #[cfg(all(feature = "v26", not(feature = "v27")))]
    pub fn rebuild_pairs(&mut self) {
        let n_himos = self.himos.len();
        let next_eid = self.entities.next_eid();
        let mut pairs = Vec::new();

        for a in 0..n_himos {
            let card_a = if self.himo_max_values[a] > 0 {
                self.himo_max_values[a] + 1
            } else {
                continue;
            };
            for b in (a + 1)..n_himos {
                let card_b = if self.himo_max_values[b] > 0 {
                    self.himo_max_values[b] + 1
                } else {
                    continue;
                };

                let cell_count = card_a as u64 * card_b as u64;
                if cell_count > 1_000_000 { continue; }

                let table_size = cell_count as usize;
                let mut raw: Vec<Vec<u32>> = vec![vec![]; table_size];

                for eid in 0..next_eid {
                    if !self.entities.is_live(eid) { continue; }
                    let va = match self.himos[a].get_value(eid) {
                        Some(v) if v < card_a => v,
                        _ => continue,
                    };
                    let vb = match self.himos[b].get_value(eid) {
                        Some(v) if v < card_b => v,
                        _ => continue,
                    };
                    raw[va as usize * card_b as usize + vb as usize].push(eid);
                }

                pairs.push(PairEntry { himo_a: a, himo_b: b, card_a, card_b, cells: raw });
            }
        }

        self.pairs = PairTable { pairs };
    }

    /// v26: delta 伝播 — 紐の値が変わった時にペアテーブルに反映。
    #[cfg(all(feature = "v26", not(feature = "v27")))]
    pub fn apply_pair_delta(&mut self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32) {
        let other_values: Vec<(usize, u32)> = (0..self.himos.len())
            .filter(|&i| i != himo_idx)
            .filter_map(|i| self.himos[i].get_value(eid).map(|v| (i, v)))
            .collect();
        self.pairs.apply_delta(eid, himo_idx, old_val, new_val, &other_values);
    }

    /// v27: ペアテーブルを構築。v26 のコピー。
    #[cfg(feature = "v27")]
    pub fn rebuild_pairs(&mut self) {
        let n_himos = self.himos.len();
        let next_eid = self.entities.next_eid();
        let mut pairs = Vec::new();

        for a in 0..n_himos {
            let card_a = if self.himo_max_values[a] > 0 {
                self.himo_max_values[a] + 1
            } else {
                continue;
            };
            for b in (a + 1)..n_himos {
                let card_b = if self.himo_max_values[b] > 0 {
                    self.himo_max_values[b] + 1
                } else {
                    continue;
                };

                let cell_count = card_a as u64 * card_b as u64;
                if cell_count > 1_000_000 { continue; }

                let table_size = cell_count as usize;
                let mut raw: Vec<Vec<u32>> = vec![vec![]; table_size];

                for eid in 0..next_eid {
                    if !self.entities.is_live(eid) { continue; }
                    let va = match self.himos[a].get_value(eid) {
                        Some(v) if v < card_a => v,
                        _ => continue,
                    };
                    let vb = match self.himos[b].get_value(eid) {
                        Some(v) if v < card_b => v,
                        _ => continue,
                    };
                    raw[va as usize * card_b as usize + vb as usize].push(eid);
                }

                pairs.push(PairEntry { himo_a: a, himo_b: b, card_a, card_b, cells: raw });
            }
        }

        *self.pairs.write().unwrap() = PairTable { pairs };
    }

    /// v27: delta 伝播 — 紐の値が変わった時にペアテーブルに反映。
    #[cfg(feature = "v27")]
    pub fn apply_pair_delta(&mut self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32) {
        let other_values: Vec<(usize, u32)> = (0..self.himos.len())
            .filter(|&i| i != himo_idx)
            .filter_map(|i| self.himos[i].get_value(eid).map(|v| (i, v)))
            .collect();
        self.pairs.write().unwrap().apply_delta(eid, himo_idx, old_val, new_val, &other_values);
    }

    /// v27: ペアテーブルへの即時反映(内部用、&self)。
    /// himos[hid].set の前に呼んで old_val を取得する必要がある。
    ///
    /// PairTable と NTupleTable は RwLock で保護。consumer スレッド(単一)と
    /// 並行 reader の間で、cell(Vec) への insert/remove が torn read にならない
    /// ように write lock 下で排他する。reader 側 (best_lookup) は read lock で
    /// cell を clone してから返す。
    #[cfg(feature = "v27")]
    fn apply_pair_delta_internal(&self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32) {
        let other_values: Vec<(usize, u32)> = (0..self.himos.len())
            .filter(|&i| i != himo_idx)
            .filter_map(|i| self.himos[i].get_value(eid).map(|v| (i, v)))
            .collect();
        self.pairs.write().unwrap().apply_delta(eid, himo_idx, old_val, new_val, &other_values);

        // 観測窓への伝播(登録されている場合のみコスト発生)
        let has_views = !self.tuples.read().unwrap().views.is_empty();
        if has_views {
            self.tuples.write().unwrap().apply_delta(eid, himo_idx, old_val, new_val, &other_values);
        }
    }

    /// 観測窓(n-tuple 仮想テーブル定義)をスキーマとして永続化登録。
    /// 以降 tie で自動反映、query で自動利用、open 後も自動復元する。
    ///
    /// 各紐は define_himo 済み + max_values > 0 必須。セル数が 100万超ならエラー。
    /// view 数上限 32、1 view あたり紐数上限 8(ヘッダ固定長制約)。
    #[cfg(feature = "v27")]
    pub fn define_view(&mut self, himos: &[&str]) -> Result<(), String> {
        if himos.len() < 2 {
            return Err("view requires at least 2 himos".into());
        }
        if himos.len() > MAX_VIEW_HIMOS {
            return Err(format!("view has too many himos (max {})", MAX_VIEW_HIMOS));
        }

        let mut idxs: Vec<usize> = Vec::with_capacity(himos.len());
        for &name in himos {
            let idx = self.himo_id(name)
                .ok_or_else(|| format!("himo '{}' not defined", name))?;
            idxs.push(idx);
        }
        idxs.sort();

        // 冪等: ソート正規化後に既存 view と一致すれば no-op
        {
            let tuples = self.tuples.read().unwrap();
            for v in &tuples.views {
                let mut existing = v.himos.clone();
                existing.sort();
                if existing == idxs {
                    return Ok(());
                }
            }
        }

        if self.tuples.read().unwrap().views.len() >= MAX_PERSISTED_VIEWS {
            return Err(format!("too many views (max {})", MAX_PERSISTED_VIEWS));
        }

        // 内部構築 → 成功したらヘッダに永続化 + 追加
        self.build_and_push_view(&idxs)?;
        self.persist_view_record(&idxs);
        Ok(())
    }

    /// idxs から NTupleEntry を組み立てて tuples.views に push。永続化は呼び元。
    #[cfg(feature = "v27")]
    fn build_and_push_view(&mut self, idxs: &[usize]) -> Result<(), String> {
        let mut cards: Vec<u32> = Vec::with_capacity(idxs.len());
        for &idx in idxs {
            let mv = self.himo_max_values[idx];
            if mv == 0 {
                let name = self.himo_names.get(idx).cloned().unwrap_or_default();
                return Err(format!("himo '{}' has no max_values (define_himo first)", name));
            }
            cards.push(mv + 1);
        }

        // セル数チェック(オーバーフロー対策)
        let mut cell_count: u64 = 1;
        for &c in &cards {
            cell_count = cell_count.saturating_mul(c as u64);
            if cell_count > 1_000_000 {
                return Err("tuple view cell count exceeds 1M (got card product > 1M)".into());
            }
        }
        let cell_count = cell_count as usize;

        // ストライド(row-major): strides[i] = Π_{j>i} cards[j]
        let mut strides: Vec<usize> = vec![0; idxs.len()];
        let mut acc: usize = 1;
        for i in (0..idxs.len()).rev() {
            strides[i] = acc;
            acc *= cards[i] as usize;
        }

        let mut cells: Vec<Vec<u32>> = vec![vec![]; cell_count];

        // 既存データから cell を埋める
        let next_eid = self.entities.next_eid();
        for eid in 0..next_eid {
            if !self.entities.is_live(eid) { continue; }
            let mut vals: Vec<u32> = Vec::with_capacity(idxs.len());
            let mut all = true;
            for &hi in idxs {
                match self.himos[hi].get_value(eid) {
                    Some(v) if v < self.himo_max_values[hi] + 1 => vals.push(v),
                    _ => { all = false; break; }
                }
            }
            if !all { continue; }
            let mut id = 0usize;
            let mut ok = true;
            for i in 0..idxs.len() {
                if vals[i] >= cards[i] { ok = false; break; }
                id += vals[i] as usize * strides[i];
            }
            if ok && id < cells.len() {
                cells[id].push(eid);
            }
        }
        for c in &mut cells {
            c.sort_unstable();
        }

        self.tuples.write().unwrap().views.push(NTupleEntry {
            himos: idxs.to_vec(),
            cards,
            strides,
            cells,
        });
        Ok(())
    }

    /// ヘッダに 1 view レコードを append(H_VIEW_COUNT を +1)。
    #[cfg(feature = "v27")]
    fn persist_view_record(&mut self, idxs: &[usize]) {
        let buf = self.backing.as_slice_mut();
        let count = u32::from_le_bytes(buf[H_VIEW_COUNT..H_VIEW_COUNT + 4].try_into().unwrap());
        let slot = count as usize;
        let off = H_VIEWS_OFF + slot * VIEW_RECORD_SIZE;
        buf[off] = idxs.len() as u8;
        for (i, &idx) in idxs.iter().enumerate() {
            let o = off + 1 + i * 2;
            buf[o..o + 2].copy_from_slice(&(idx as u16).to_le_bytes());
        }
        // 末尾 padding は 0 のまま
        for i in idxs.len()..MAX_VIEW_HIMOS {
            let o = off + 1 + i * 2;
            buf[o..o + 2].copy_from_slice(&0u16.to_le_bytes());
        }
        let new_count = count + 1;
        buf[H_VIEW_COUNT..H_VIEW_COUNT + 4].copy_from_slice(&new_count.to_le_bytes());
    }

    /// open 時: ヘッダから view を読み出して NTupleEntry を再構築。
    #[cfg(feature = "v27")]
    fn restore_persisted_views(&mut self) -> Result<(), String> {
        let buf = self.backing.as_slice_mut();
        let count = u32::from_le_bytes(buf[H_VIEW_COUNT..H_VIEW_COUNT + 4].try_into().unwrap()) as usize;
        if count == 0 { return Ok(()); }
        if count > MAX_PERSISTED_VIEWS {
            return Err(format!("persisted view count {} exceeds max {}", count, MAX_PERSISTED_VIEWS));
        }

        let mut all_idxs: Vec<Vec<usize>> = Vec::with_capacity(count);
        for slot in 0..count {
            let off = H_VIEWS_OFF + slot * VIEW_RECORD_SIZE;
            let length = buf[off] as usize;
            if length < 2 || length > MAX_VIEW_HIMOS {
                return Err(format!("persisted view {} has invalid length {}", slot, length));
            }
            let mut idxs: Vec<usize> = Vec::with_capacity(length);
            for i in 0..length {
                let o = off + 1 + i * 2;
                let hid = u16::from_le_bytes(buf[o..o + 2].try_into().unwrap()) as usize;
                if hid >= self.himos.len() {
                    return Err(format!("persisted view {} references unknown himo id {}", slot, hid));
                }
                idxs.push(hid);
            }
            all_idxs.push(idxs);
        }

        for idxs in all_idxs {
            self.build_and_push_view(&idxs)?;
        }
        Ok(())
    }

    /// 引く。v32 で EntityId(u64) の Vec を返す形に統一。
    /// 将来の u64 local eid / peer 跨ぎ対応のため、常にコピー。
    #[cfg(not(feature = "v27"))]
    pub fn pull_raw(&self, himo: &str, value: u32) -> Vec<crate::EntityId> {
        match self.himo_id(himo) {
            Some(idx) => {
                let hs = &self.himos[idx];
                if hs.delta_needs_rebuild() {
                    hs.rebuild_cylinder();
                }
                hs.cylinder().slice_one(value).iter().map(|&e| e as crate::EntityId).collect()
            }
            None => Vec::new(),
        }
    }

    /// v27 版: EntityId の Vec を返す(内部 u32 から u64 に昇格)。
    #[cfg(feature = "v27")]
    pub fn pull_raw(&self, himo: &str, value: u32) -> Vec<crate::EntityId> {
        match self.himo_id(himo) {
            Some(idx) => self.himos[idx].pull(value).into_iter().map(|e| e as crate::EntityId).collect(),
            None => Vec::new(),
        }
    }

    /// 引く（delta 補正付き、Vec を返す）。
    #[cfg(not(feature = "v27"))]
    pub fn pull(&self, himo: &str, value: u32) -> Vec<u32> {
        let idx = match self.himo_id(himo) {
            Some(idx) => idx,
            None => return vec![],
        };
        let hs = &self.himos[idx];

        if hs.delta_needs_rebuild() {
            hs.rebuild_cylinder();
        }

        if hs.delta_is_empty() {
            return hs.cylinder().slice_one(value).to_vec();
        }

        // Cylinder 結果 + delta 補正
        let cyl_result = hs.cylinder().slice_one(value);
        let delta = hs.delta_eids();
        self.apply_delta(idx, value, cyl_result, delta)
    }

    /// v27 版: delta なし。HimoStore::pull (RwLock + clone) 直。
    #[cfg(feature = "v27")]
    pub fn pull(&self, himo: &str, value: u32) -> Vec<u32> {
        match self.himo_id(himo) {
            Some(idx) => self.himos[idx].pull(value),
            None => Vec::new(),
        }
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

    pub fn query(&self, strings: &[(&str, u32)]) -> Vec<crate::EntityId> {
        self.query_u32(strings).into_iter().map(|e| e as crate::EntityId).collect()
    }

    /// 内部版: u32 eid の Vec を返す。互換性のため残す。
    fn query_u32(&self, strings: &[(&str, u32)]) -> Vec<u32> {
        // delta が溢れた himo があれば rebuild
        for hs in &self.himos {
            if hs.delta_needs_rebuild() { hs.rebuild_cylinder(); }
        }
        if strings.is_empty() { return vec![]; }

        // 全条件の himo index と value を解決
        let mut conds: Vec<(usize, u32)> = Vec::with_capacity(strings.len());
        for &(himo, val) in strings {
            match self.himo_id(himo) {
                Some(idx) => conds.push((idx, val)),
                None => return vec![],
            }
        }

        if conds.len() == 1 {
            let (idx, val) = conds[0];
            let hs = &self.himos[idx];
            #[cfg(not(feature = "v27"))]
            {
                let cyl_result = hs.cylinder().slice_one(val);
                if hs.delta_is_empty() {
                    return cyl_result.to_vec();
                }
                return self.apply_delta(idx, val, cyl_result, hs.delta_eids());
            }
            #[cfg(feature = "v27")]
            {
                return hs.pull(val);
            }
        }

        // 最小 Cylinder スライスのサイズを調べる(実体は下の戦略選択で必要)
        let mut min_slice_len = usize::MAX;
        for (_, &(idx, val)) in conds.iter().enumerate() {
            #[cfg(not(feature = "v27"))]
            let len = self.himos[idx].cylinder().slice_one(val).len();
            #[cfg(feature = "v27")]
            let len = self.himos[idx].slice_len(val);
            if len < min_slice_len { min_slice_len = len; }
        }

        // v27: 観測窓(n-tuple)とペアテーブル、最小 Cylinder スライスを比べて最小候補を選ぶ。
        // read lock を iterate+filter の間保持して torn read を防ぐ(cell への insert/remove は write lock)。
        #[cfg(feature = "v27")]
        {
            // 先に長さだけ測って最適経路を決定(どのロックも保持しない)。
            let (tuple_len, pair_len) = {
                let t = self.tuples.read().unwrap();
                let p = self.pairs.read().unwrap();
                let tl = t.best_lookup_ref(&conds).map(|(c, _)| c.len()).unwrap_or(usize::MAX);
                let pl = p.best_lookup_ref(&conds).map(|(c, _)| c.len()).unwrap_or(usize::MAX);
                (tl, pl)
            };

            if tuple_len <= pair_len && tuple_len <= min_slice_len {
                let guard = self.tuples.read().unwrap();
                if let Some((candidates, remaining)) = guard.best_lookup_ref(&conds) {
                    if remaining.is_empty() {
                        return candidates.to_vec();
                    }
                    let mut result = Vec::new();
                    for &eid in candidates {
                        let mut pass = true;
                        for &(idx, val) in &remaining {
                            if !self.himos[idx].value_eq(eid, val) {
                                pass = false;
                                break;
                            }
                        }
                        if pass { result.push(eid); }
                    }
                    return result;
                }
            }
            if pair_len <= min_slice_len {
                let guard = self.pairs.read().unwrap();
                if let Some((candidates, remaining)) = guard.best_lookup_ref(&conds) {
                    if remaining.is_empty() {
                        return candidates.to_vec();
                    }
                    let mut result = Vec::new();
                    for &eid in candidates {
                        let mut pass = true;
                        for &(idx, val) in &remaining {
                            if !self.himos[idx].value_eq(eid, val) {
                                pass = false;
                                break;
                            }
                        }
                        if pass { result.push(eid); }
                    }
                    return result;
                }
            }
        }

        // v26 のみ: ペアテーブルで最小候補を O(1) ルックアップ
        #[cfg(all(feature = "v26", not(feature = "v27")))]
        {
            if let Some((candidates, remaining)) = self.pairs.best_lookup(&conds) {
                if candidates.len() <= min_slice_len {
                    if remaining.is_empty() {
                        return candidates.to_vec();
                    }
                    let mut result = Vec::new();
                    for &eid in candidates {
                        let mut pass = true;
                        for &(idx, val) in &remaining {
                            if !self.himos[idx].value_eq(eid, val) {
                                pass = false;
                                break;
                            }
                        }
                        if pass { result.push(eid); }
                    }
                    return result;
                }
            }
        }

        // 全条件bitmapならAND（delta空 + 値がbitmap範囲内の時のみ）
        let all_bitmap = conds.len() >= 2
            && conds.iter().all(|&(idx, val)| {
                let hs = &self.himos[idx];
                hs.has_bitmaps() && val <= hs.max_values && hs.delta_is_empty()
            });

        if all_bitmap {
            return self.query_bitmap_and(&conds);
        }

        // Column直読みフィルタ（Columnは常に最新なのでdelta不要）
        self.query_column_filter(&conds)
    }

    /// 実entity範囲のbitmap word数（max_entitiesではなく実際のnext_eid基準）。
    fn effective_bitmap_words(&self) -> usize {
        (self.entities.next_eid() as usize + 63) / 64
    }

    /// Case A: 全条件bitmap有 → AND + extract を1パスで。alloc最小。
    fn query_bitmap_and(&self, conds: &[(usize, u32)]) -> Vec<u32> {
        let words = self.effective_bitmap_words();
        let bitmaps: Vec<&[u64]> = conds.iter()
            .filter_map(|&(idx, val)| self.himos[idx].bitmap(val))
            .collect();
        if bitmaps.len() != conds.len() { return vec![]; }

        let mut result = Vec::new();
        for i in 0..words {
            let mut w = bitmaps[0][i];
            for bm in &bitmaps[1..] { w &= bm[i]; }
            while w != 0 {
                let bit = w.trailing_zeros();
                result.push((i * 64 + bit as usize) as u32);
                w &= w - 1;
            }
        }
        result
    }

    /// Column直読みフィルタ（delta 補正付き）
    fn query_column_filter(&self, conds: &[(usize, u32)]) -> Vec<u32> {
        // pivot: 最小スライスを選ぶ
        let mut best = 0;
        let mut best_len = usize::MAX;
        for (i, &(idx, val)) in conds.iter().enumerate() {
            #[cfg(not(feature = "v27"))]
            let len = self.himos[idx].cylinder().slice_one(val).len();
            #[cfg(feature = "v27")]
            let len = self.himos[idx].slice_len(val);
            if len < best_len { best_len = len; best = i; }
        }

        let (pivot_idx, pivot_val) = conds[best];
        let hs = &self.himos[pivot_idx];

        // pivot の候補を delta 補正して取得
        #[cfg(not(feature = "v27"))]
        let candidates = {
            let cyl_result = hs.cylinder().slice_one(pivot_val);
            if hs.delta_is_empty() {
                cyl_result.to_vec()
            } else {
                self.apply_delta(pivot_idx, pivot_val, cyl_result, hs.delta_eids())
            }
        };
        #[cfg(feature = "v27")]
        let candidates = hs.pull(pivot_val);

        // 残りの条件を Column 直読みでフィルタ（Column は常に最新）
        let mut result = Vec::with_capacity(candidates.len());
        for &eid in &candidates {
            let mut pass = true;
            for (i, &(idx, val)) in conds.iter().enumerate() {
                if i == best { continue; }
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
        let base = self.backing.as_mut_ptr();
        let col_off = self.layout.himo_col_off(hid);

        #[cfg(not(feature = "v27"))]
        let hs = {
            let cyl_a_off = self.layout.himo_cyl_a_off(hid);
            let cyl_b_off = self.layout.himo_cyl_b_off(hid);
            HimoStore::init(
                unsafe { Region::new(base.add(col_off), self.layout.himo_col_size) },
                unsafe { Region::new(base.add(cyl_a_off), self.layout.himo_cyl_size) },
                unsafe { Region::new(base.add(cyl_b_off), self.layout.himo_cyl_size) },
                ht, effective_mv, self.max_entities,
            )
        };
        #[cfg(feature = "v27")]
        let hs = HimoStore::init(
            unsafe { Region::new(base.add(col_off), self.layout.himo_col_size) },
            ht, effective_mv, self.max_entities,
        );

        self.himos.push(hs);
        self.himo_names.push(himo.to_string());
        self.himo_types.push(ht);
        self.himo_max_values.push(max_values);

        // ヘッダにメタデータ書き込み
        let maxv_base = himo_maxv_base(self.max_himos);
        self.backing.as_slice_mut()[H_HIMO_TYPES + hid] = ht as u8;
        let mv_off = maxv_base + hid * 4;
        self.backing.as_slice_mut()[mv_off..mv_off + 4].copy_from_slice(&max_values.to_le_bytes());
        let himo_count = (hid + 1) as u32;
        self.backing.as_slice_mut()[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&himo_count.to_le_bytes());
        // v28: header CRC を再計算(himo_count が変わったため)
        write_header_crc(self.backing.as_slice_mut());

        // v27: 新 himo と既存 himo のペアを空セルで登録。以降 tie で自動更新。
        #[cfg(feature = "v27")]
        {
            if max_values > 0 {
                let card_b = max_values + 1;
                let mut pairs_w = self.pairs.write().unwrap();
                for a in 0..hid {
                    if self.himo_max_values[a] == 0 { continue; }
                    let card_a = self.himo_max_values[a] + 1;
                    let cell_count = card_a as u64 * card_b as u64;
                    if cell_count > 1_000_000 { continue; }
                    let cells: Vec<Vec<u32>> = vec![vec![]; cell_count as usize];
                    pairs_w.pairs.push(PairEntry {
                        himo_a: a,
                        himo_b: hid,
                        card_a,
                        card_b,
                        cells,
                    });
                }
            }
        }

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
    #[cfg(all(feature = "v27", not(target_arch = "wasm32")))]
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
    #[cfg(all(feature = "v27", not(target_arch = "wasm32")))]
    pub fn create_concurrent_with_wal(path: &str, wal_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        let wal_path = wal_path_for(path);
        let wal = std::sync::Arc::new(crate::wal::Wal::create(&wal_path, wal_capacity)?);
        Ok(Self::spawn_consumer_with_wal(eng, Some(wal)))
    }

    /// v28: WAL 付き open_concurrent。既存 WAL があればリカバリする。
    /// v29: region CRC は WAL ルートでは skip(WAL が source of truth)。
    /// 代わりに古い `.crc` ファイルは削除して、次回 flush で regenerate させる。
    #[cfg(all(feature = "v27", not(target_arch = "wasm32")))]
    pub fn open_concurrent_with_wal(path: &str, wal_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let mut eng = Self::open_internal(path, /*verify_region_crc=*/ false)?;
        // 古い .crc は WAL 活動後に stale になるので削除
        let crc_path = crate::integrity::crc_path_for(path);
        let _ = std::fs::remove_file(&crc_path);
        let wal_path = wal_path_for(path);
        let wal = if wal_path.exists() {
            let w = crate::wal::Wal::open(&wal_path)?;
            // リカバリ: commit されたレコードを本体に適用
            let records = w.recover();
            for rec in &records {
                eng.apply_wal_op(&rec.op);
            }
            // 本体に適用し終えたので checkpoint を head まで前進
            w.advance_checkpoint(w.head());
            std::sync::Arc::new(w)
        } else {
            std::sync::Arc::new(crate::wal::Wal::create(&wal_path, wal_capacity)?)
        };
        Ok(Self::spawn_consumer_with_wal(eng, Some(wal)))
    }

    /// WAL の 1 op を本体に適用(recover 専用)。
    /// v32: eid は u64 だが Column は local u32 で保持。eid_local() で剥がす。
    #[cfg(feature = "v27")]
    fn apply_wal_op(&mut self, op: &crate::wal::DecodedOp) {
        use crate::wal::DecodedOp;
        match op {
            DecodedOp::Tie { eid, himo_id, value } => {
                let hid = *himo_id as usize;
                let local = crate::eid_local(*eid);
                if hid < self.himos.len() {
                    self.himos[hid].set(local, *value);
                }
            }
            DecodedOp::Untie { eid, himo_id } => {
                let hid = *himo_id as usize;
                let local = crate::eid_local(*eid);
                if hid < self.himos.len() {
                    self.himos[hid].remove(local);
                }
            }
            DecodedOp::Delete { eid } => {
                let local = crate::eid_local(*eid);
                for hid in 0..self.himos.len() {
                    self.himos[hid].remove(local);
                }
                self.entities.free(local);
            }
            DecodedOp::Content { eid, key, data } => {
                let local = crate::eid_local(*eid);
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
    #[cfg(feature = "v27")]
    pub fn concurrentize(eng: Self) -> std::sync::Arc<Self> {
        Self::spawn_consumer(eng)
    }

    #[cfg(feature = "v27")]
    fn spawn_consumer(eng: Self) -> std::sync::Arc<Self> {
        Self::spawn_consumer_with_wal(eng, None)
    }

    /// changefeed 内部ヘルパ: WAL から emit_offset 以降の record を取り出して
    /// 全 listener に渡し、cursor を進める。
    #[cfg(feature = "v32")]
    fn fire_change_listeners(
        wal: &std::sync::Arc<crate::wal::Wal>,
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

    #[cfg(feature = "v27")]
    fn spawn_consumer_with_wal(
        mut eng: Self,
        wal: Option<std::sync::Arc<crate::wal::Wal>>,
    ) -> std::sync::Arc<Self> {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use crate::write_queue::WriteQueue;

        let queue = Arc::new(WriteQueue::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        eng.write_queue = Some(queue.clone());
        eng.shutdown_flag = Some(shutdown.clone());
        eng.wal = wal.clone();

        let arc = Arc::new(eng);

        let engine_ptr: *const Engine = Arc::as_ptr(&arc);
        let engine_addr = engine_ptr as usize;
        let q_for_thread = queue.clone();
        let flag_for_thread = shutdown.clone();
        let apply_count_for_thread = arc.apply_count.clone();
        let wal_for_thread = wal.clone();
        let durable_lsn_for_thread = arc.durable_lsn.clone();
        #[cfg(feature = "v32")]
        let listeners_for_thread = arc.change_listeners.clone();
        #[cfg(feature = "v32")]
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
                                let _ = wal.append(crate::wal::WalOp::Commit);
                                let _ = wal.fsync();
                                let _ = engine.body_msync();
                                let head = wal.head();
                                wal.advance_checkpoint(head);
                                let lsn = wal.next_lsn().saturating_sub(1);
                                durable_lsn_for_thread.store(lsn, Ordering::Release);
                                engine.undo.commit();

                                // changefeed: durable 化した record を listener に push
                                #[cfg(feature = "v32")]
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
                            #[cfg(feature = "v32")]
                            {
                                if wal.try_reset() {
                                    emit_offset_for_thread.store(
                                        crate::wal::HEADER_SIZE as u64,
                                        Ordering::Release,
                                    );
                                }
                            }
                            #[cfg(not(feature = "v32"))]
                            {
                                wal.try_reset();
                            }
                            last_fsync = Instant::now();
                        }
                    }

                    if flag_for_thread.load(Ordering::Acquire) {
                        while let Some(op) = q_for_thread.pop() {
                            engine.apply_op(op);
                            apply_count_for_thread.fetch_add(1, Ordering::Release);
                        }
                        // shutdown 時の最終 Commit + 順序付き同期
                        if let Some(wal) = wal_for_thread.as_ref() {
                            let _ = wal.append(crate::wal::WalOp::Commit);
                            let _ = wal.fsync();
                            let _ = engine.body_msync();
                            wal.advance_checkpoint(wal.head());
                            engine.undo.commit();
                            // shutdown 時の最終 emit
                            #[cfg(feature = "v32")]
                            Self::fire_change_listeners(
                                wal,
                                &listeners_for_thread,
                                &emit_offset_for_thread,
                            );
                        }
                        return;
                    }

                    if !drained_any {
                        std::thread::yield_now();
                    }
                }
            })
            .expect("failed to spawn consumer thread");

        *arc.consumer_handle.lock().unwrap() = Some(handle);
        arc
    }

    /// body mmap の msync(WAL 順序と絡むので &self で呼び出し可能)。
    #[cfg(all(feature = "v27", not(target_arch = "wasm32")))]
    pub fn body_msync(&self) -> io::Result<()> {
        match &self.backing {
            Backing::Mmap(m) => m.flush(),
            Backing::Memory(_) => Ok(()),
        }
    }

    #[cfg(all(feature = "v27", target_arch = "wasm32"))]
    pub fn body_msync(&self) -> io::Result<()> { Ok(()) }

    /// 強制同期: Commit marker 挿入 → WAL fsync → body msync → checkpoint 前進。
    /// Sync mode 相当の待ち。undo も clear される。
    #[cfg(feature = "v27")]
    pub fn wal_sync(&self) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        self.flush_writes();
        if let Some(wal) = self.wal.as_ref() {
            let _ = wal.append(crate::wal::WalOp::Commit);
            wal.fsync()?;
            self.body_msync()?;
            let head = wal.head();
            wal.advance_checkpoint(head);
            let lsn = wal.next_lsn().saturating_sub(1);
            self.durable_lsn.store(lsn, Ordering::Release);
            // changefeed: durable 化したので listener へ即時 push
            // (consumer の 100ms tick を待たず caller スレッドで発火)
            #[cfg(feature = "v32")]
            Self::fire_change_listeners(wal, &self.change_listeners, &self.change_emit_offset);
        }
        // undo も clear(transaction 確定)
        self.undo.commit();
        Ok(())
    }

    /// 現在耐久化されている LSN(Commit を含む最後の WAL fsync 到達位置)。
    #[cfg(feature = "v27")]
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
    #[cfg(feature = "v27")]
    pub fn audit(&self, filter: &AuditFilter) -> Vec<crate::wal::RecoveredRecord> {
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
        #[cfg(feature = "v27")]
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

    #[cfg(feature = "v27")]
    pub fn stats(&self) -> EngineStats {
        use std::sync::atomic::Ordering;
        let (wal_head, wal_checkpoint, wal_capacity, wal_lsn) = match self.wal.as_ref() {
            Some(w) => (w.head(), w.checkpoint(), w.usage().1, w.next_lsn().saturating_sub(1)),
            None => (0, 0, 0, 0),
        };
        #[cfg(feature = "v32")]
        let (peer_id, hlc_entries, max_hlc) = (
            self.peer_id.load(Ordering::Acquire),
            self.hlc_store.len(),
            self.hlc_store.max_hlc(),
        );
        #[cfg(not(feature = "v32"))]
        let (peer_id, hlc_entries, max_hlc) = (0u32, 0usize, None);

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
    #[cfg(feature = "v27")]
    fn apply_op(&self, op: crate::write_queue::Op) {
        use crate::write_queue::Op;
        match op {
            Op::Tie { eid, himo_id, value } => {
                let hid = himo_id as usize;
                if hid >= self.himos.len() { return; }
                let old_val = self.himos[hid].get_value(eid);
                self.himos[hid].set(eid, value);
                self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), value);
            }
            Op::Untie { eid, himo_id } => {
                let hid = himo_id as usize;
                if hid >= self.himos.len() { return; }
                let old_val = self.himos[hid].get_value(eid);
                self.himos[hid].remove(eid);
                if let Some(ov) = old_val {
                    self.apply_pair_delta_internal(eid, hid, ov, u32::MAX);
                }
            }
            Op::Delete { eid } => {
                for hid in 0..self.himos.len() {
                    let old_val = self.himos[hid].get_value(eid);
                    self.himos[hid].remove(eid);
                    if let Some(ov) = old_val {
                        self.apply_pair_delta_internal(eid, hid, ov, u32::MAX);
                    }
                }
                self.entities.free(eid);
            }
            Op::Content { eid, key, data } => {
                self.contents.set(eid, &key, &data);
            }
        }
    }

    /// 非同期 tie。`create_concurrent`/`concurrentize` で有効化済みの場合のみ使える。
    /// 紐名は事前に `define_himo` で定義されている必要がある。
    /// WriteQueue は SegQueue(unbounded)なので push は必ず成功する。
    ///
    /// WAL が有効な場合: tie_async は WAL append (memcpy) → WriteQueue push の順で実行する。
    /// WAL append は `.wal` ファイルに memcpy 1 回、100ns オーダー。hot path で fsync しない。
    #[cfg(feature = "v27")]
    pub fn tie_async(&self, eid: crate::EntityId, himo: &str, value: u32) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = crate::eid_local(eid);
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = crate::make_eid(wal.peer_id(), local);
            let _ = wal.append(crate::wal::WalOp::Tie { eid: wal_eid, himo_id: hid as u16, value });
        }
        let q = self.write_queue.as_ref()
            .expect("tie_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie { eid: local, himo_id: hid as u16, value });
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
    #[cfg(feature = "v33")]
    pub fn tie_text_async(&self, eid: crate::EntityId, himo: &str, value: &str) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = crate::eid_local(eid);
        let vid = self.vocab.get_or_insert(value.as_bytes());
        assert!(vid < u32::MAX, "vocab vid must be < u32::MAX (sentinel reserved)");
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        if let Some(wal) = self.wal.as_ref() {
            // Vocab op を先に(sync の receiver 側で Tie より先に mapping が張られるよう)
            let _ = wal.append(crate::wal::WalOp::Vocab { vid, bytes: value.as_bytes() });
            let wal_eid = crate::make_eid(wal.peer_id(), local);
            let _ = wal.append(crate::wal::WalOp::Tie { eid: wal_eid, himo_id: hid as u16, value: vid });
        }
        let q = self.write_queue.as_ref()
            .expect("tie_text_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie { eid: local, himo_id: hid as u16, value: vid });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// v33: 非同期 tie_ref。target_eid の local 部(u32)を WAL / 本体に運ぶ。
    /// target_eid は u64 [peer|local] だが、現行 WAL は value: u32 しか運べないため
    /// peer_id 部は捨てる。すなわち receiver 側では「author_peer と同一 peer 上の entity」を
    /// 指す ref として再構成される。cross-peer ref (peer A が peer B の entity を指す)は
    /// 現状未対応 (Ref 用 WAL op を別途用意する必要あり、v34 以降)。
    #[cfg(feature = "v33")]
    pub fn tie_ref_async(&self, eid: crate::EntityId, himo: &str, target_eid: crate::EntityId) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = crate::eid_local(eid);
        let target_local = crate::eid_local(target_eid);
        assert!(target_local < u32::MAX, "target_local must be < u32::MAX");
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = crate::make_eid(wal.peer_id(), local);
            let _ = wal.append(crate::wal::WalOp::Tie {
                eid: wal_eid, himo_id: hid as u16, value: target_local,
            });
        }
        let q = self.write_queue.as_ref()
            .expect("tie_ref_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie {
            eid: local, himo_id: hid as u16, value: target_local,
        });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 非同期 untie。
    #[cfg(feature = "v27")]
    pub fn untie_async(&self, eid: crate::EntityId, himo: &str) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = crate::eid_local(eid);
        let hid = match self.himo_id(himo) { Some(x) => x, None => return };
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = crate::make_eid(wal.peer_id(), local);
            let _ = wal.append(crate::wal::WalOp::Untie { eid: wal_eid, himo_id: hid as u16 });
        }
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Untie { eid: local, himo_id: hid as u16 });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 非同期 delete。
    #[cfg(feature = "v27")]
    pub fn delete_async(&self, eid: crate::EntityId) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = crate::eid_local(eid);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = crate::make_eid(wal.peer_id(), local);
            let _ = wal.append(crate::wal::WalOp::Delete { eid: wal_eid });
        }
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Delete { eid: local });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 非同期 content 書き込み。WAL 有効時はクラッシュ後も復元される。
    /// key と data は Box に move される(消費される)。
    #[cfg(feature = "v27")]
    pub fn content_async(&self, eid: crate::EntityId, key: &str, data: &[u8]) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = crate::eid_local(eid);
        if let Some(wal) = self.wal.as_ref() {
            let wal_eid = crate::make_eid(wal.peer_id(), local);
            let _ = wal.append(crate::wal::WalOp::Content { eid: wal_eid, key, data });
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
    /// 同時に undo ログもクリア(WAL Commit 後は rollback できないため)。
    ///
    /// Async モードでは fsync は consumer スレッドが背景で行う。
    /// Sync モードでは commit 完了まで待つ。
    #[cfg(feature = "v27")]
    pub fn wal_commit(&self) {
        if let Some(wal) = self.wal.as_ref() {
            let _ = wal.append(crate::wal::WalOp::Commit);
        }
        // WAL Commit = transaction 確定。undo は不要になる。
        // これを忘れると re-open 時に undo 再生で entity が free される。
        self.undo.commit();
    }

    /// WAL への参照(テスト / 内部用)。
    #[cfg(feature = "v27")]
    pub fn wal(&self) -> Option<&std::sync::Arc<crate::wal::Wal>> { self.wal.as_ref() }

    /// push 済みの全 Op が apply 完了するまで spin 待ち。`tie_async` の同期点。
    /// `queue.is_empty()` は pop 直後 / apply 前のウィンドウで true になる race が
    /// あるため、push_count と apply_count の累積カウンタで apply 完了を待つ。
    #[cfg(feature = "v27")]
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
    #[cfg(feature = "v27")]
    pub fn pending_writes(&self) -> usize {
        self.write_queue.as_ref().map(|q| q.len()).unwrap_or(0)
    }

    // ──── flush ────

    #[cfg(not(target_arch = "wasm32"))]
    pub fn flush(&mut self) -> io::Result<()> {
        self.commit();

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

        self.backing.flush_to_disk()?;
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

#[cfg(feature = "v27")]
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

        // v24/v26 は Cylinder が max_values で clamp するため u32::MAX-2 も OK。
        // v27 は silent clamp を廃止して動的拡張するので、u32::MAX-2 だと
        // バケット 40 億本分の Vec を確保してしまう。ここでは代わりに
        // 100 万オーダーの「大きめ値」で検証する。
        #[cfg(not(feature = "v27"))]
        let big = u32::MAX - 2;
        #[cfg(feature = "v27")]
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

    #[test]
    fn rollback_reverts() {
        let dir = tmp("tx_rollback");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.commit();

        eng.tie(e, "age", 99);
        assert_eq!(eng.get(e, "age"), Some(99));
        eng.rollback();
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn rollback_insert() {
        let dir = tmp("tx_rb_ins");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.commit();

        let e = eng.entity();
        eng.tie(e, "age", 30);
        assert_eq!(eng.query_count(&[("age", 30)]), 1);
        eng.rollback();
        assert_eq!(eng.query_count(&[("age", 30)]), 0);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn crash_recovery_rollback() {
        let dir = tmp("tx_crash");
        {
            let mut eng = Engine::create_standalone(&dir).unwrap();
            let e = eng.entity();
            eng.tie(e, "age", 30);
            eng.commit();
            eng.flush().unwrap();

            eng.tie(e, "age", 99);
            // undo だけ flush（クラッシュシミュレーション）
            eng.backing.flush_to_disk().unwrap();
        }

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.get(0, "age"), Some(30));
        let _ = std::fs::remove_file(&dir);
    }

    // ──── prefix sum O(1) ────

    #[test]
    fn prefix_sum_point_query() {
        let dir = tmp("ps_point");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", HimoType::Value, 100);
        eng.define_himo("dept", HimoType::Value, 20);

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
        eng.define_himo("level", HimoType::Value, 10);
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
        eng.define_himo("age", HimoType::Value, 100);

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
            eng.define_himo("score", HimoType::Value, 200);
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
        eng.define_himo("age", HimoType::Value, 100);
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
        eng.define_himo("score", HimoType::Value, 1000);
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
        eng.define_himo("age", HimoType::Value, 100);
        eng.define_himo("dept", HimoType::Value, 20);
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
    fn prefix_sum_rollback() {
        let dir = tmp("ps_rollback");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("val", HimoType::Value, 50);
        let e = eng.entity();
        eng.tie(e, "val", 10);
        eng.commit();

        eng.tie(e, "val", 40);
        assert_eq!(eng.get(e, "val"), Some(40));
        eng.rollback();
        assert_eq!(eng.get(e, "val"), Some(10));
        assert_eq!(eng.query_count(&[("val", 10)]), 1);
        assert_eq!(eng.query_count(&[("val", 40)]), 0);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_boundary_max() {
        let dir = tmp("ps_bnd_max");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("x", HimoType::Value, 10);
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
        eng.define_himo("val", HimoType::Value, 100);
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
        eng.define_himo("age", HimoType::Value, SCALE_AGES);
        eng.define_himo("dept", HimoType::Value, SCALE_DEPTS);
        eng.define_himo("company", HimoType::Value, SCALE_COMPANIES);

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
        eng.define_himo("age", HimoType::Value, ages);
        eng.define_himo("dept", HimoType::Value, depts);
        eng.define_himo("group", HimoType::Value, groups);

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

    // ──── v27: NTupleView (観測窓) ────

    #[cfg(feature = "v27")]
    #[test]
    fn ntuple_view_register_and_query() {
        let dir = tmp("ntuple_basic");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("tenant", HimoType::Value, 8);
        eng.define_himo("dept", HimoType::Value, 8);
        eng.define_himo("status", HimoType::Value, 4);

        for i in 0u32..100 {
            let e = eng.entity();
            eng.tie(e, "tenant", i % 8);
            eng.tie(e, "dept", (i * 3) % 8);
            eng.tie(e, "status", (i * 5) % 4);
        }
        eng.define_view(&["tenant", "dept", "status"]).unwrap();

        // 登録後も tie で自動更新
        let e = eng.entity();
        eng.tie(e, "tenant", 3);
        eng.tie(e, "dept", 5);
        eng.tie(e, "status", 1);

        let baseline: Vec<u32> = (0..100u32).filter(|i| i % 8 == 3 && (i * 3) % 8 == 5 && (i * 5) % 4 == 1).collect();
        let expected_count = baseline.len() + 1;

        let r = eng.query(&[("tenant", 3), ("dept", 5), ("status", 1)]);
        assert_eq!(r.len(), expected_count);
        assert!(r.contains(&e));

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn ntuple_view_partial_match() {
        let dir = tmp("ntuple_partial");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("a", HimoType::Value, 8);
        eng.define_himo("b", HimoType::Value, 8);
        eng.define_himo("c", HimoType::Value, 4);
        eng.define_himo("d", HimoType::Value, 4);

        for i in 0u32..200 {
            let e = eng.entity();
            eng.tie(e, "a", i % 8);
            eng.tie(e, "b", (i * 3) % 8);
            eng.tie(e, "c", (i * 5) % 4);
            eng.tie(e, "d", (i * 7) % 4);
        }
        // 3紐登録、クエリは4条件(1つは残り条件として Column フィルタ)
        eng.define_view(&["a", "b", "c"]).unwrap();

        let r = eng.query(&[("a", 2), ("b", 6), ("c", 2), ("d", 0)]);
        let expected: Vec<u32> = (0..200u32)
            .filter(|i| i % 8 == 2 && (i * 3) % 8 == 6 && (i * 5) % 4 == 2 && (i * 7) % 4 == 0)
            .collect();
        assert_eq!(r.len(), expected.len());

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn ntuple_view_untie_propagates() {
        let dir = tmp("ntuple_untie");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("x", HimoType::Value, 8);
        eng.define_himo("y", HimoType::Value, 8);

        let e0 = eng.entity();
        eng.tie(e0, "x", 2);
        eng.tie(e0, "y", 3);
        let e1 = eng.entity();
        eng.tie(e1, "x", 2);
        eng.tie(e1, "y", 3);

        eng.define_view(&["x", "y"]).unwrap();
        assert_eq!(eng.query(&[("x", 2), ("y", 3)]).len(), 2);

        eng.untie(e0, "x");
        let r = eng.query(&[("x", 2), ("y", 3)]);
        assert_eq!(r, vec![e1]);

        // 再 tie
        eng.tie(e0, "x", 2);
        let r = eng.query(&[("x", 2), ("y", 3)]);
        assert_eq!(r.len(), 2);

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn ntuple_view_delete_propagates() {
        let dir = tmp("ntuple_delete");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("x", HimoType::Value, 8);
        eng.define_himo("y", HimoType::Value, 8);

        let e0 = eng.entity();
        eng.tie(e0, "x", 1);
        eng.tie(e0, "y", 1);
        let e1 = eng.entity();
        eng.tie(e1, "x", 1);
        eng.tie(e1, "y", 1);

        eng.define_view(&["x", "y"]).unwrap();
        assert_eq!(eng.query(&[("x", 1), ("y", 1)]).len(), 2);

        eng.delete(e0);
        let r = eng.query(&[("x", 1), ("y", 1)]);
        assert_eq!(r, vec![e1]);

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn ntuple_view_cell_overflow_rejected() {
        let dir = tmp("ntuple_overflow");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("a", HimoType::Value, 1000);
        eng.define_himo("b", HimoType::Value, 1000);
        eng.define_himo("c", HimoType::Value, 100);
        // 1001 * 1001 * 101 = ~101M > 1M
        let r = eng.define_view(&["a", "b", "c"]);
        assert!(r.is_err());

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn ntuple_view_requires_max_values() {
        let dir = tmp("ntuple_nomv");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "foo", 1); // max_values=0 のまま自動作成
        eng.tie(e, "bar", 2);
        let r = eng.define_view(&["foo", "bar"]);
        assert!(r.is_err());

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn ntuple_view_exact_match_vs_pair() {
        let dir = tmp("ntuple_exact");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("a", HimoType::Value, 4);
        eng.define_himo("b", HimoType::Value, 4);
        eng.define_himo("c", HimoType::Value, 4);

        for i in 0u32..50 {
            let e = eng.entity();
            eng.tie(e, "a", i % 4);
            eng.tie(e, "b", (i / 4) % 4);
            eng.tie(e, "c", (i / 16) % 4);
        }
        eng.define_view(&["a", "b", "c"]).unwrap();

        // 全条件がマッチする観測窓(3紐)を使うケース
        let r = eng.query(&[("a", 1), ("b", 2), ("c", 0)]);
        let expected: Vec<u64> = (0..50u64)
            .filter(|i| i % 4 == 1 && (i / 4) % 4 == 2 && (i / 16) % 4 == 0)
            .collect();
        assert_eq!(r.len(), expected.len());
        for &e in &expected {
            assert!(r.contains(&e));
        }

        let _ = std::fs::remove_file(&dir);
    }

    // ──── v27: 観測窓の永続化 ────

    #[cfg(feature = "v27")]
    #[test]
    fn define_view_persists() {
        let dir = tmp("view_persist");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("tenant", HimoType::Value, 8);
        eng.define_himo("dept", HimoType::Value, 8);
        eng.define_himo("status", HimoType::Value, 4);

        for i in 0u32..80 {
            let e = eng.entity();
            eng.tie(e, "tenant", i % 8);
            eng.tie(e, "dept", (i * 3) % 8);
            eng.tie(e, "status", (i * 5) % 4);
        }
        eng.define_view(&["tenant", "dept", "status"]).unwrap();
        // tenant=3 → dept=(3*3)%8=1, status=(3*5)%4=3。10 件ヒット。
        let pre = eng.query(&[("tenant", 3), ("dept", 1), ("status", 3)]).len();
        eng.flush().unwrap();
        drop(eng);

        // 再 open → define_view なしで query が動くこと
        let eng2 = Engine::open_standalone(&dir).unwrap();
        let post = eng2.query(&[("tenant", 3), ("dept", 1), ("status", 3)]).len();
        assert_eq!(pre, post);
        assert!(pre > 0, "expected matches, got {}", pre);

        // open 後も tie で自動反映される
        // (新しい entity を追加して、観測窓経由で見えること)
        // 注: tie は &mut self なので Drop 前にもう一度可変参照が必要
        drop(eng2);
        let mut eng3 = Engine::open_standalone(&dir).unwrap();
        let new_e = eng3.entity();
        eng3.tie(new_e, "tenant", 3);
        eng3.tie(new_e, "dept", 1);
        eng3.tie(new_e, "status", 3);
        let post2 = eng3.query(&[("tenant", 3), ("dept", 1), ("status", 3)]).len();
        assert_eq!(post2, post + 1);

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn define_view_cell_count_error() {
        let dir = tmp("view_overflow");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("a", HimoType::Value, 1000);
        eng.define_himo("b", HimoType::Value, 1000);
        eng.define_himo("c", HimoType::Value, 100);
        let r = eng.define_view(&["a", "b", "c"]);
        assert!(r.is_err());

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn multiple_views_persist() {
        let dir = tmp("view_multi");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("a", HimoType::Value, 4);
        eng.define_himo("b", HimoType::Value, 4);
        eng.define_himo("c", HimoType::Value, 4);
        eng.define_himo("d", HimoType::Value, 4);

        for i in 0u32..60 {
            let e = eng.entity();
            eng.tie(e, "a", i % 4);
            eng.tie(e, "b", (i / 4) % 4);
            eng.tie(e, "c", (i / 16) % 4);
            eng.tie(e, "d", (i * 7) % 4);
        }
        eng.define_view(&["a", "b", "c"]).unwrap();
        eng.define_view(&["a", "b", "d"]).unwrap();
        eng.define_view(&["c", "d"]).unwrap();

        let q1_pre = eng.query(&[("a", 1), ("b", 2), ("c", 0)]).len();
        let q2_pre = eng.query(&[("a", 1), ("b", 2), ("d", 3)]).len();
        let q3_pre = eng.query(&[("c", 0), ("d", 1)]).len();
        eng.flush().unwrap();
        drop(eng);

        let eng2 = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng2.query(&[("a", 1), ("b", 2), ("c", 0)]).len(), q1_pre);
        assert_eq!(eng2.query(&[("a", 1), ("b", 2), ("d", 3)]).len(), q2_pre);
        assert_eq!(eng2.query(&[("c", 0), ("d", 1)]).len(), q3_pre);

        let _ = std::fs::remove_file(&dir);
    }

    #[cfg(feature = "v27")]
    #[test]
    fn concurrent_tie_async_basic() {
        // tie_async → flush_writes → pull_raw で値が見えること。
        let dir = tmp("concurrent_basic");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", HimoType::Value, 200);
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

    #[cfg(feature = "v27")]
    #[test]
    fn concurrent_multi_reader_writer() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let dir = tmp("concurrent_mrw");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("k", HimoType::Value, 16);
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
        eng.define_himo("__blob_id", HimoType::Symbol, 0);

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
}
