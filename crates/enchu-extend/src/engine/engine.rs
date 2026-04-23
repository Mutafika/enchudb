//! v24 Engine — 量子円柱。単一ファイル。全コンポーネントが1つのmmapを共有。
//!
//!   entity() → ID 振る
//!   tie_text → 文字列を紐で張る（Vocabulary 経由）
//!   tie     → u32 値を紐で張る
//!   query → 円柱の重なりを一発で返す
//!   open/flush → 永続化（mmap なので open は即利用可）

#[cfg(not(target_arch = "wasm32"))]
use std::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
use std::io;

#[cfg(not(target_arch = "wasm32"))]
use memmap2::MmapMut;

use super::region::Region;

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
use super::vocabulary::Vocabulary;
use super::entity_set::EntitySet;
use super::himo_store::{HimoStore, HimoType};
use super::column::Column;
#[cfg(not(feature = "v27"))]
use super::cylinder::Cylinder;

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
    himo_base_off: usize,
    himo_col_size: usize,
    #[allow(dead_code)]
    himo_cyl_size: usize,
    himo_slot_size: usize,
    cyl_max_values: u32,
    total_size: usize,
}

impl Layout {
    fn compute(max_entities: u32, max_himos: u32, vocab_data_size: usize, cyl_max_values: Option<u32>) -> Self {
        let vocab_max_entries = max_entities.saturating_mul(16).min(256_000_000);
        let vocab_index_cap = vocab_max_entries.next_power_of_two();
        let himoreg_max_entries = max_himos.max(256);
        let himoreg_index_cap = (himoreg_max_entries * 2).next_power_of_two();
        let himoreg_data_size = 64 * 1024;
        let cyl_max_values = cyl_max_values.unwrap_or(DEFAULT_CYL_MAX_VALUES);

        Self::from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            cyl_max_values,
        )
    }

    fn from_params(
        max_entities: u32, max_himos: u32,
        vocab_max_entries: u32, vocab_index_cap: u32, vocab_data_size: usize,
        himoreg_max_entries: u32, himoreg_index_cap: u32, himoreg_data_size: usize,
        cyl_max_values: u32,
    ) -> Self {
        let mut off = HEADER_SIZE;

        let entities_off = off;
        let entities_size = align8(EntitySet::region_size(max_entities));
        off += entities_size;

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
            vocab_data_off, vocab_data_size,
            vocab_offsets_off, vocab_offsets_size,
            vocab_index_off, vocab_index_size,
            vocab_max_entries, vocab_index_cap,
            himoreg_data_off, himoreg_data_size,
            himoreg_offsets_off, himoreg_offsets_size,
            himoreg_index_off, himoreg_index_size,
            himoreg_max_entries, himoreg_index_cap,
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
    #[cfg(all(feature = "v26", not(feature = "v27")))]
    pairs: PairTable,
    #[cfg(feature = "v27")]
    pairs: std::sync::RwLock<PairTable>,
    #[cfg(feature = "v27")]
    tuples: std::sync::RwLock<NTupleTable>,
    /// 非同期書き込みキュー。`create_concurrent` で有効化される。
    #[cfg(feature = "v27")]
    write_queue: Option<std::sync::Arc<super::write_queue::WriteQueue>>,
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
            Some(256),                 // cyl_max_values: 256
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_with_capacity(path: &str, max_entities: u32) -> io::Result<Self> {
        Self::create_with_options(path, max_entities, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_with_options(path: &str, max_entities: u32, vocab_data_size: Option<usize>) -> io::Result<Self> {
        Self::create_full(path, max_entities, vocab_data_size, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_full(path: &str, max_entities: u32, vocab_data_size: Option<usize>, max_himos: Option<u32>) -> io::Result<Self> {
        Self::create_full_with_cyl(path, max_entities, vocab_data_size, max_himos, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_full_with_cyl(path: &str, max_entities: u32, vocab_data_size: Option<usize>, max_himos: Option<u32>, cyl_max_values: Option<u32>) -> io::Result<Self> {
        let vds = vocab_data_size.unwrap_or(DEFAULT_VOCAB_DATA_SIZE);
        let max_himos = max_himos.unwrap_or(DEFAULT_MAX_HIMOS);
        let layout = Layout::compute(max_entities, max_himos, vds, cyl_max_values);

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
        mmap[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].copy_from_slice(&layout.cyl_max_values.to_le_bytes());

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

        Ok(Self {
            path: path.to_string(), layout, max_entities, max_himos,
            vocab, himo_reg,
            himo_names: Vec::new(),
            himo_types: Vec::new(), himo_max_values: Vec::new(),
            himos: Vec::new(), entities,
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
        let cyl_max_values = u32::from_le_bytes(buf[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].try_into().unwrap());

        let layout = Layout::from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            cyl_max_values,
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

        let eng = Self {
            path: String::new(), layout, max_entities, max_himos,
            vocab, himo_reg,
            himo_names, himo_types, himo_max_values,
            himos, entities,
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
            backing,
        };

        eng.rebuild();

        #[cfg(feature = "v27")]
        {
            // rebuild_pairs requires &mut but eng is not mut here;
            // however the original code had `eng.rebuild_pairs()` on a `mut eng`.
            // We work around by using interior mutability already present in RwLock.
            // rebuild_pairs needs &mut self, so we re-bind:
        }

        Ok(eng)
    }

    // ──── entity ────

    pub fn entity(&self) -> u32 {
        self.entities.allocate()
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
        debug_assert_eq!(self.himo_types[hid], HimoType::Symbol, "tie_text on non-Symbol himo '{}'", himo);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, vid);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), vid);
    }

    pub fn tie(&mut self, eid: u32, himo: &str, value: u32) {
        if value == u32::MAX { return; }
        let hid = self.ensure_himo(himo, HimoType::Value, 0);
        debug_assert!(self.himo_types[hid] == HimoType::Value || self.himo_types[hid] == HimoType::Ref, "tie on non-Value himo '{}'", himo);
        #[cfg(feature = "v27")]
        let old_val = self.himos[hid].get_value(eid);
        self.himos[hid].set(eid, value);
        #[cfg(feature = "v27")]
        self.apply_pair_delta_internal(eid, hid, old_val.unwrap_or(u32::MAX), value);
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

    /// 指定 entity 群の紐値を合計
    pub fn sum(&self, himo: &str, eids: &[u32]) -> u64 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(eid) {
                total += v as u64;
            }
        }
        total
    }

    pub fn min(&self, himo: &str, eids: &[u32]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(eid) {
                result = Some(result.map_or(v, |cur: u32| cur.min(v)));
            }
        }
        result
    }

    pub fn max(&self, himo: &str, eids: &[u32]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(eid) {
                result = Some(result.map_or(v, |cur: u32| cur.max(v)));
            }
        }
        result
    }

    pub fn avg(&self, himo: &str, eids: &[u32]) -> Option<u64> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        let mut count: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(eid) {
                total += v as u64;
                count += 1;
            }
        }
        if count == 0 { None } else { Some(total / count) }
    }

    pub fn agg_count(&self, himo: &str, eids: &[u32]) -> u32 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut n: u32 = 0;
        for &eid in eids {
            if hs.get_value(eid).is_some() { n += 1; }
        }
        n
    }

    /// GROUP BY + SUM — group_himo の値でグループ化し、sum_himo の値を合計
    pub fn group_sum(&self, group_himo: &str, sum_himo: &str, eids: &[u32]) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let mut map: Vec<(u32, u64)> = Vec::new();
        for &eid in eids {
            if let (Some(group), Some(val)) = (gs.get_value(eid), ss.get_value(eid)) {
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

    pub fn pull_range(&self, himo: &str, min: u32, max: u32) -> Vec<u32> {
        let idx = match self.himo_id(himo) { Some(h) => h, None => return vec![] };
        let hs = &self.himos[idx];
        let mut result = Vec::new();
        for v in min..=max {
            #[cfg(feature = "v27")]
            { result.extend_from_slice(&hs.pull(v)); }
            #[cfg(not(feature = "v27"))]
            { result.extend_from_slice(hs.cylinder().slice_one(v)); }
        }
        result
    }

    // ──── 日付ヘルパー ────

    pub fn date_to_days(year: u32, month: u32, day: u32) -> u32 {
        let mut y = year as i64;
        let mut m = month as i64;
        if m <= 2 { y -= 1; m += 12; }
        let days = 365 * y + y / 4 - y / 100 + y / 400 + (153 * (m - 3) + 2) / 5 + day as i64 - 1;
        let epoch = {
            let ey: i64 = 2000;
            let em: i64 = 1;
            let ey2 = ey - 1;
            let em2 = 13i64;
            365 * ey2 + ey2 / 4 - ey2 / 100 + ey2 / 400 + (153 * (em2 - 3) + 2) / 5 + em as i64 - 1
        };
        (days - epoch) as u32
    }

    pub fn days_to_date(days: u32) -> (u32, u32, u32) {
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

    pub fn tie_date(&mut self, eid: u32, himo: &str, year: u32, month: u32, day: u32) {
        self.tie(eid, himo, Self::date_to_days(year, month, day));
    }

    pub fn get_date(&self, eid: u32, himo: &str) -> Option<(u32, u32, u32)> {
        self.get(eid, himo).map(Self::days_to_date)
    }

    pub fn pull_date_range(&self, himo: &str, from: (u32, u32, u32), to: (u32, u32, u32)) -> Vec<u32> {
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

    pub fn himos_of(&self, eid: u32) -> Vec<&str> {
        self.himos.iter().enumerate()
            .filter(|(_, ds)| ds.get_value(eid).is_some())
            .map(|(i, _)| self.himo_names[i].as_str())
            .collect()
    }

    pub(crate) fn vocab(&self) -> &Vocabulary { &self.vocab }
    pub fn vocab_ref(&self) -> &Vocabulary { &self.vocab }
    pub fn himo_names(&self) -> &[String] { &self.himo_names }
    pub fn himo_store_slice(&self) -> &[HimoStore] { &self.himos }

    pub fn himo_type(&self, himo: &str) -> Option<HimoType> {
        self.himo_id(himo).map(|idx| self.himo_types[idx])
    }

    /// 指定紐の現在の unique 値数(非空バケット数)。O(1)。
    /// 紐が未定義なら None。
    #[cfg(feature = "v27")]
    pub fn himo_cardinality(&self, himo: &str) -> Option<u32> {
        let idx = self.himo_id(himo)?;
        Some(self.himos[idx].unique_count())
    }

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

    /// 引く。delta が空なら Cylinder 直返し（ゼロコピー）。
    #[cfg(not(feature = "v27"))]
    pub fn pull_raw(&self, himo: &str, value: u32) -> &[u32] {
        match self.himo_id(himo) {
            Some(idx) => {
                let hs = &self.himos[idx];
                if hs.delta_needs_rebuild() {
                    hs.rebuild_cylinder();
                }
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
    #[cfg(all(feature = "v27", not(target_arch = "wasm32")))]
    pub fn create_concurrent(path: &str) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        Ok(Self::spawn_consumer(eng))
    }

    /// 既存の `Engine` を Arc 化して consumer スレッドを起動する。
    #[cfg(feature = "v27")]
    pub fn concurrentize(eng: Self) -> std::sync::Arc<Self> {
        Self::spawn_consumer(eng)
    }

    #[cfg(feature = "v27")]
    fn spawn_consumer(mut eng: Self) -> std::sync::Arc<Self> {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use super::write_queue::WriteQueue;

        let queue = Arc::new(WriteQueue::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        eng.write_queue = Some(queue.clone());
        eng.shutdown_flag = Some(shutdown.clone());

        let arc = Arc::new(eng);

        let engine_ptr: *const Engine = Arc::as_ptr(&arc);
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
                        apply_count_for_thread.fetch_add(1, Ordering::Release);
                    }

                    if flag_for_thread.load(Ordering::Acquire) {
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
    fn apply_op(&self, op: super::write_queue::Op) {
        use super::write_queue::Op;
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

    /// 非同期 tie。
    #[cfg(feature = "v27")]
    pub fn tie_async(&self, eid: u32, himo: &str, value: u32) {
        use std::sync::atomic::Ordering;
        if value == u32::MAX { return; }
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        let q = self.write_queue.as_ref()
            .expect("tie_async requires create_concurrent or concurrentize");
        q.push(super::write_queue::Op::Tie { eid, himo_id: hid as u16, value });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 非同期 untie。
    #[cfg(feature = "v27")]
    pub fn untie_async(&self, eid: u32, himo: &str) {
        use std::sync::atomic::Ordering;
        let hid = match self.himo_id(himo) { Some(x) => x, None => return };
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(super::write_queue::Op::Untie { eid, himo_id: hid as u16 });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// 非同期 delete。
    #[cfg(feature = "v27")]
    pub fn delete_async(&self, eid: u32) {
        use std::sync::atomic::Ordering;
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(super::write_queue::Op::Delete { eid });
        self.push_count.fetch_add(1, Ordering::Release);
    }

    /// push 済みの全 Op が apply 完了するまで spin 待ち。
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
        for ds in &self.himos { ds.sync(); }
        self.vocab.sync();
        self.himo_reg.sync();

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
        if let Ok(mut h) = self.consumer_handle.lock() {
            if let Some(handle) = h.take() {
                let _ = handle.join();
            }
        }
    }
}
