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
                    if cell.is_empty() {
                        return Some((&[], conds.to_vec()));
                    }
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
    /// 戻り値: (セルの entity スライス, 残りの条件)
    /// 優先: 条件紐をすべて含む view のうち、紐数が最大(残り条件が少ない)のもの。
    fn best_lookup<'a>(&'a self, conds: &[(usize, u32)]) -> Option<(&'a [u32], Vec<(usize, u32)>)> {
        if conds.len() < 2 { return None; }
        let mut best: Option<(&[u32], Vec<(usize, u32)>, usize)> = None;
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
            let cell = &v.cells[cell_id];
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
                best = Some((cell.as_slice(), remaining, covered));
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
                    if cell.is_empty() {
                        return Some((&[], conds.to_vec()));
                    }
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
    pairs: PairTable,
    #[cfg(feature = "v27")]
    tuples: NTupleTable,
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
    backing: Backing, // 最後に drop されるよう最終フィールド
}

impl Engine {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create(path: &str) -> io::Result<Self> {
        Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)
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
            pairs: PairTable::new(),
            #[cfg(feature = "v27")]
            tuples: NTupleTable::new(),
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
            backing: Backing::Mmap(mmap),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Self::load_from_backing(Backing::Mmap(mmap))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
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
            pairs: PairTable::new(),
            #[cfg(feature = "v27")]
            tuples: NTupleTable::new(),
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
            backing,
        };

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

    // ──── entity ────

    pub fn entity(&self) -> u32 {
        let eid = self.entities.allocate();
        self.undo.record(eid, 0xFFFF, &[1, 0, 0, 0]); // entity created
        eid
    }

    pub(crate) fn entities(&self) -> Vec<u32> { self.entities.iter() }
    pub fn entity_count(&self) -> u32 { self.entities.count() }
    pub fn next_eid(&self) -> u32 { self.entities.next_eid() }

    // ──── tie ────

    pub fn define_himo(&mut self, himo: &str, ht: HimoType, max_values: u32) {
        self.ensure_himo(himo, ht, max_values);
    }

    pub fn tie_text(&mut self, eid: u32, himo: &str, value: &str) {
        let vid = self.vocab.get_or_insert(value.as_bytes());
        let hid = self.ensure_himo(himo, HimoType::Symbol, 0);
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, vid);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), vid);
    }

    pub fn tie(&mut self, eid: u32, himo: &str, value: u32) {
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, HimoType::Value, 0);
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, value);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), value);
    }

    pub fn tie_ref(&mut self, eid: u32, himo: &str, target_eid: u32) {
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, HimoType::Ref, 0);
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
    pub fn tie_text_to(&self, eid: u32, himo: &str, value: &str) {
        let vid = self.vocab.get_or_insert(value.as_bytes());
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, vid);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), vid);
    }

    /// 定義済みの紐にu32値を張る。&selfで呼べる。
    pub fn tie_to(&self, eid: u32, himo: &str, value: u32) {
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, value);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), value);
    }

    /// 定義済みの紐にentity参照を張る。&selfで呼べる。
    pub fn tie_ref_to(&self, eid: u32, himo: &str, target_eid: u32) {
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        self.record_undo(eid, hid);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, target_eid);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), target_eid);
    }

    // ──── untie ────

    pub fn untie(&self, eid: u32, himo: &str) {
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

    pub fn delete(&self, eid: u32) {
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

    pub fn commit(&self) {
        self.undo.commit();
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

    pub fn content(&self, eid: u32, key: &str, data: &[u8]) {
        self.contents.set(eid, key, data);
    }

    pub fn get_content(&self, eid: u32, key: &str) -> Option<&[u8]> {
        self.contents.get(eid, key)
    }

    // ──── get ────

    pub fn get_text(&self, eid: u32, himo: &str) -> Option<&[u8]> {
        let hid = self.himo_id(himo)?;
        if self.himo_types[hid] != HimoType::Symbol { return None; }
        let vid = self.himos[hid].get_value(eid)?;
        Some(self.vocab.get(vid))
    }

    pub fn get(&self, eid: u32, himo: &str) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        self.himos[hid].get_value(eid)
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

    pub fn himos_of(&self, eid: u32) -> Vec<&str> {
        self.himos.iter().enumerate()
            .filter(|(_, ds)| ds.get_value(eid).is_some())
            .map(|(i, _)| self.himo_names[i].as_str())
            .collect()
    }
    /// 1 entity の全フィールドを一括取得。HashMap ルックアップ 0 回。
    pub fn get_entity(&self, eid: u32) -> Vec<(&str, EntityValue)> {
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

    pub(crate) fn vocab(&self) -> &Vocabulary { &self.vocab }
    pub fn himo_names(&self) -> &[String] { &self.himo_names }

    pub fn himo_type(&self, himo: &str) -> Option<HimoType> {
        self.himo_id(himo).map(|idx| self.himo_types[idx])
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

        self.pairs = PairTable { pairs };
    }

    /// v27: delta 伝播 — 紐の値が変わった時にペアテーブルに反映。
    #[cfg(feature = "v27")]
    pub fn apply_pair_delta(&mut self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32) {
        let other_values: Vec<(usize, u32)> = (0..self.himos.len())
            .filter(|&i| i != himo_idx)
            .filter_map(|i| self.himos[i].get_value(eid).map(|v| (i, v)))
            .collect();
        self.pairs.apply_delta(eid, himo_idx, old_val, new_val, &other_values);
    }

    /// v27: ペアテーブルへの即時反映(内部用、&self)。
    /// himos[hid].set の前に呼んで old_val を取得する必要がある。
    #[cfg(feature = "v27")]
    fn apply_pair_delta_internal(&self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32) {
        let other_values: Vec<(usize, u32)> = (0..self.himos.len())
            .filter(|&i| i != himo_idx)
            .filter_map(|i| self.himos[i].get_value(eid).map(|v| (i, v)))
            .collect();
        // SAFETY: v27 では Engine を Arc 共有しない前提(BucketCylinder 自体が &mut self)。
        // 単一スレッド。
        let pairs_ptr = &self.pairs as *const PairTable as *mut PairTable;
        unsafe { (*pairs_ptr).apply_delta(eid, himo_idx, old_val, new_val, &other_values); }

        // 観測窓への伝播(登録されている場合のみコスト発生)
        if !self.tuples.views.is_empty() {
            let tuples_ptr = &self.tuples as *const NTupleTable as *mut NTupleTable;
            unsafe { (*tuples_ptr).apply_delta(eid, himo_idx, old_val, new_val, &other_values); }
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
        if self.tuples.views.len() >= MAX_PERSISTED_VIEWS {
            return Err(format!("too many views (max {})", MAX_PERSISTED_VIEWS));
        }

        let mut idxs: Vec<usize> = Vec::with_capacity(himos.len());
        for &name in himos {
            let idx = self.himo_id(name)
                .ok_or_else(|| format!("himo '{}' not defined", name))?;
            idxs.push(idx);
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

        self.tuples.views.push(NTupleEntry {
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

    /// 引く。delta が空なら Cylinder 直返し（ゼロコピー）。
    /// delta があれば補正した Vec を返す。
    ///
    /// v27: `Vec<u32>` を返す(内部 RwLock 下で clone、Arc<Engine> で並行 read 可能)。
    /// v24/v26: `&[u32]` を返す(ゼロコピー)。
    #[cfg(not(feature = "v27"))]
    pub fn pull_raw(&self, himo: &str, value: u32) -> &[u32] {
        match self.himo_id(himo) {
            Some(idx) => {
                let hs = &self.himos[idx];
                if hs.delta_needs_rebuild() {
                    hs.rebuild_cylinder();
                }
                // delta が空なら Cylinder そのまま（ゼロコピー）
                hs.cylinder().slice_one(value)
            }
            None => &[],
        }
    }

    /// v27 版: 並行 read 安全のため Vec<u32> を返す。
    #[cfg(feature = "v27")]
    pub fn pull_raw(&self, himo: &str, value: u32) -> Vec<u32> {
        match self.himo_id(himo) {
            Some(idx) => self.himos[idx].pull(value),
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

    pub fn query(&self, strings: &[(&str, u32)]) -> Vec<u32> {
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

        // 最小 Cylinder スライスのサイズを調べる
        let mut min_slice_idx = 0;
        let mut min_slice_len = usize::MAX;
        for (i, &(idx, val)) in conds.iter().enumerate() {
            #[cfg(not(feature = "v27"))]
            let len = self.himos[idx].cylinder().slice_one(val).len();
            #[cfg(feature = "v27")]
            let len = self.himos[idx].slice_len(val);
            if len < min_slice_len { min_slice_len = len; min_slice_idx = i; }
        }

        // v27: 観測窓(n-tuple)とペアテーブル、最小 Cylinder スライスを比べて最小候補を選ぶ。
        #[cfg(feature = "v27")]
        {
            let tuple = self.tuples.best_lookup(&conds);
            let pair = self.pairs.best_lookup(&conds);

            // 候補数最小のものを選ぶ。残り条件を Column フィルタ。
            let tuple_len = tuple.as_ref().map(|(c, _)| c.len()).unwrap_or(usize::MAX);
            let pair_len = pair.as_ref().map(|(c, _)| c.len()).unwrap_or(usize::MAX);

            if tuple_len <= pair_len && tuple_len <= min_slice_len {
                if let Some((candidates, remaining)) = tuple {
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
                if let Some((candidates, remaining)) = pair {
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

        // v27: 新 himo と既存 himo のペアを空セルで登録。以降 tie で自動更新。
        #[cfg(feature = "v27")]
        {
            if max_values > 0 {
                let card_b = max_values + 1;
                for a in 0..hid {
                    if self.himo_max_values[a] == 0 { continue; }
                    let card_a = self.himo_max_values[a] + 1;
                    let cell_count = card_a as u64 * card_b as u64;
                    if cell_count > 1_000_000 { continue; }
                    let cells: Vec<Vec<u32>> = vec![vec![]; cell_count as usize];
                    self.pairs.pairs.push(PairEntry {
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
    #[cfg(all(feature = "v27", not(target_arch = "wasm32")))]
    pub fn create_concurrent(path: &str, queue_cap: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        Ok(Self::spawn_consumer(eng, queue_cap))
    }

    /// 既存の `Engine`(create や create_with_capacity で作成済み、define_himo も
    /// 済ませたもの)を Arc 化して consumer スレッドを起動する。
    ///
    /// 返り値の `Arc<Engine>` を各 writer/reader スレッドで clone して使う。
    /// Drop(最後の Arc が落ちた時)で consumer スレッドは join される。
    #[cfg(feature = "v27")]
    pub fn concurrentize(eng: Self, queue_cap: usize) -> std::sync::Arc<Self> {
        Self::spawn_consumer(eng, queue_cap)
    }

    #[cfg(feature = "v27")]
    fn spawn_consumer(mut eng: Self, queue_cap: usize) -> std::sync::Arc<Self> {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use crate::write_queue::WriteQueue;

        let queue = Arc::new(WriteQueue::new(queue_cap));
        let shutdown = Arc::new(AtomicBool::new(false));

        eng.write_queue = Some(queue.clone());
        eng.shutdown_flag = Some(shutdown.clone());

        let arc = Arc::new(eng);

        // consumer thread は Engine の raw pointer を持つ。Engine の fields は
        // Drop::drop の本体実行中(handle.join を待っている間)は生きているので、
        // consumer は shutdown_flag を見て終了する。flag が立った後は
        // Engine にアクセスしない。
        let engine_ptr: *const Engine = Arc::as_ptr(&arc);
        // 生ポインタを Send させるため usize にキャスト。
        let engine_addr = engine_ptr as usize;
        let q_for_thread = queue.clone();
        let flag_for_thread = shutdown.clone();
        let apply_count_for_thread = arc.apply_count.clone();

        let handle = std::thread::Builder::new()
            .name("enchudb-consumer".into())
            .spawn(move || {
                use std::sync::atomic::Ordering;
                let engine: &Engine = unsafe { &*(engine_addr as *const Engine) };
                loop {
                    let mut drained_any = false;
                    while let Some(op) = q_for_thread.pop() {
                        drained_any = true;
                        engine.apply_op(op);
                        // apply 完了を公開(Release)。flush_writes が Acquire で読む。
                        apply_count_for_thread.fetch_add(1, Ordering::Release);
                    }

                    if flag_for_thread.load(Ordering::Acquire) {
                        // shutdown → 最後の drain
                        while let Some(op) = q_for_thread.pop() {
                            engine.apply_op(op);
                            apply_count_for_thread.fetch_add(1, Ordering::Release);
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
        }
    }

    /// 非同期 tie。`create_concurrent`/`concurrentize` で有効化済みの場合のみ使える。
    /// 紐名は事前に `define_himo` で定義されている必要がある。
    /// queue 満杯なら spin + yield でリトライ(back-pressure)。
    #[cfg(feature = "v27")]
    pub fn tie_async(&self, eid: u32, himo: &str, value: u32) {
        use std::sync::atomic::Ordering;
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        let q = self.write_queue.as_ref()
            .expect("tie_async requires create_concurrent or concurrentize");
        let mut op = crate::write_queue::Op::Tie { eid, himo_id: hid as u16, value };
        loop {
            match q.push(op) {
                Ok(()) => {
                    // push 成功を公開。flush_writes の同期点。
                    self.push_count.fetch_add(1, Ordering::Release);
                    return;
                }
                Err(returned) => {
                    op = returned;
                    std::thread::yield_now();
                }
            }
        }
    }

    /// 非同期 untie。
    #[cfg(feature = "v27")]
    pub fn untie_async(&self, eid: u32, himo: &str) {
        use std::sync::atomic::Ordering;
        let hid = match self.himo_id(himo) { Some(x) => x, None => return };
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        let mut op = crate::write_queue::Op::Untie { eid, himo_id: hid as u16 };
        loop {
            match q.push(op) {
                Ok(()) => {
                    self.push_count.fetch_add(1, Ordering::Release);
                    return;
                }
                Err(returned) => {
                    op = returned;
                    std::thread::yield_now();
                }
            }
        }
    }

    /// 非同期 delete。
    #[cfg(feature = "v27")]
    pub fn delete_async(&self, eid: u32) {
        use std::sync::atomic::Ordering;
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        let mut op = crate::write_queue::Op::Delete { eid };
        loop {
            match q.push(op) {
                Ok(()) => {
                    self.push_count.fetch_add(1, Ordering::Release);
                    return;
                }
                Err(returned) => {
                    op = returned;
                    std::thread::yield_now();
                }
            }
        }
    }

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

        self.backing.flush_to_disk()?;
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie_text(e, "name", "田中");
        assert_eq!(eng.get_text(e, "name"), Some("田中".as_bytes()));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_value_roundtrip() {
        let dir = tmp("tie_val");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_entity_ref() {
        let dir = tmp("tie_eref");
        let mut eng = Engine::create(&dir).unwrap();
        let parent = eng.entity();
        let child = eng.entity();
        eng.tie(child, "company", parent);
        assert_eq!(eng.get(child, "company"), Some(parent));
        eng.rebuild();
        let result = eng.pull_raw("company", parent);
        assert_eq!(result, vec![child]);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_overwrite() {
        let dir = tmp("tie_ow");
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "x", 1);
        eng.tie_text(e, "y", "a");
        eng.tie(e, "z", e);
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();

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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();

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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();

        for age in 20..=40 {
            let e = eng.entity();
            eng.tie(e, "age", age);
            eng.tie(e, "dept", 1);
        }

        eng.rebuild();
        let mut age_ents: Vec<u32> = Vec::new();
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
            let mut eng = Engine::create(&dir).unwrap();
            let e0 = eng.entity();
            eng.tie(e0, "age", 25);
            eng.tie(e0, "dept", 1);

            let e1 = eng.entity();
            eng.tie(e1, "age", 30);
            eng.tie(e1, "dept", 1);
            eng.content(e1, "memo", b"hello");

            eng.flush().unwrap();
        }

        let mut eng = Engine::open(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();

        let ts = 1_743_552_000u32;
        let e = eng.entity();
        eng.tie(e, "ts", ts);
        assert_eq!(eng.get(e, "ts"), Some(ts));
        eng.rebuild();
        let result = eng.pull_raw("ts", ts);
        assert_eq!(result, vec![e]);

        let big = u32::MAX - 2;
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
        let n = 1000u32;
        for i in 0..n {
            let e = eng.entity();
            eng.tie(e, "group", i % 5);
            eng.tie(e, "score", (i / 5) % 10);
        }

        eng.rebuild();
        let group0: Vec<u32> = eng.query(&[("group", 0)]);
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
        let mut eng = Engine::create(&dir).unwrap();
        let n = 100u32;
        for _ in 0..n {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.query_count(&[("val", 42)]), 100);

        let all: Vec<u32> = eng.entities();
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
            let mut eng = Engine::create(&dir).unwrap();
            for i in 0..100u32 {
                let e = eng.entity();
                eng.tie(e, "val", i % 10);
            }
            eng.rebuild();
            let del_targets: Vec<u32> = eng.query(&[("val", 0)]);
            for &eid in &del_targets {
                eng.delete(eid);
            }
            eng.flush().unwrap();
        }

        let mut eng = Engine::open(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(dir).unwrap();

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

        let victims: Vec<u32> = eng.query(&[("age", 30)]).into_iter().take(100).collect();
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

        let targets: Vec<u32> = eng.query(&[("age", 30)]).into_iter().take(500).collect();
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

        let mut eng = Engine::open(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.commit();
        eng.flush().unwrap();
        drop(eng);

        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn rollback_reverts() {
        let dir = tmp("tx_rollback");
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
            let mut eng = Engine::create(&dir).unwrap();
            let e = eng.entity();
            eng.tie(e, "age", 30);
            eng.commit();
            eng.flush().unwrap();

            eng.tie(e, "age", 99);
            // undo だけ flush（クラッシュシミュレーション）
            eng.backing.flush_to_disk().unwrap();
        }

        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.get(0, "age"), Some(30));
        let _ = std::fs::remove_file(&dir);
    }

    // ──── prefix sum O(1) ────

    #[test]
    fn prefix_sum_point_query() {
        let dir = tmp("ps_point");
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
            let mut eng = Engine::create(&dir).unwrap();
            eng.define_himo("score", HimoType::Value, 200);
            for i in 0..100u32 {
                let e = eng.entity();
                eng.tie(e, "score", i % 20);
            }
            assert_eq!(eng.query_count(&[("score", 5)]), 5);
            eng.flush().unwrap();
        }
        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.query_count(&[("score", 5)]), 5);
        assert_eq!(eng.entity_count(), 100);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_untie() {
        let dir = tmp("ps_untie");
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("age", HimoType::Value, 100);
        eng.define_himo("dept", HimoType::Value, 20);
        for i in 0..100u32 {
            let e = eng.entity();
            eng.tie(e, "age", i % 10);
            eng.tie(e, "dept", i % 5);
        }
        eng.rebuild();
        let victims: Vec<u32> = eng.query(&[("age", 0)]);
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        for _ in 0..500u32 {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.query_count(&[("val", 42)]), 500);

        let all: Vec<u32> = eng.entities();
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
        let mut eng = Engine::create(dir).unwrap();
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
        let victims: Vec<u32> = eng.query(&[("age", 30)]).into_iter().take(100).collect();
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
        let targets: Vec<u32> = eng.query(&[("age", 30)]).into_iter().take(500).collect();
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
        let mut eng = Engine::open(&dir).unwrap();
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

        let victims: Vec<u32> = eng.query(&[("age", 99)]).into_iter().take(1000).collect();
        for &eid in &victims { eng.delete(eid); }
        assert_eq!(eng.query_count(&[("age", 99)]), (n / ages) as usize - 1000);
        assert_eq!(eng.entity_count(), n - 1000);

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_late_himo_on_existing_entities() {
        let dir = tmp("late_himo");
        // Phase 1: 1000 entity作成、nameだけtie
        let mut eng = Engine::create(&dir).unwrap();
        for i in 0..1000u32 {
            let e = eng.entity();
            eng.tie_text(e, "name", &format!("company_{i}"));
        }
        eng.rebuild();
        eng.flush().unwrap();
        drop(eng);

        // Phase 2: 再open、既存entityに新しいhimoをtie
        let mut eng = Engine::open(&dir).unwrap();
        eng.rebuild();
        for eid in 0..1000u32 {
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
        let eng = Engine::open(&dir).unwrap(); // open内でrebuild済み
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
        let mut eng = Engine::open(&dir).unwrap();
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
        let eng = Engine::open(&dir).unwrap(); // open内でrebuild済み
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let expected: Vec<u32> = (0..50u32)
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let eng2 = Engine::open(&dir).unwrap();
        let post = eng2.query(&[("tenant", 3), ("dept", 1), ("status", 3)]).len();
        assert_eq!(pre, post);
        assert!(pre > 0, "expected matches, got {}", pre);

        // open 後も tie で自動反映される
        // (新しい entity を追加して、観測窓経由で見えること)
        // 注: tie は &mut self なので Drop 前にもう一度可変参照が必要
        drop(eng2);
        let mut eng3 = Engine::open(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
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

        let eng2 = Engine::open(&dir).unwrap();
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
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("age", HimoType::Value, 200);
        let eids: Vec<u32> = (0..100).map(|_| eng.entity()).collect();

        let arc = Engine::concurrentize(eng, 1024);
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
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("k", HimoType::Value, 16);
        let eids: Vec<u32> = (0..1_000).map(|_| eng.entity()).collect();
        for (i, &e) in eids.iter().enumerate() {
            eng.tie(e, "k", (i as u32) % 16);
        }

        let arc = Engine::concurrentize(eng, 8192);
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
}
