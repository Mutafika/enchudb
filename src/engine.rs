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

use std::collections::HashMap;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
use std::io;

#[cfg(not(target_arch = "wasm32"))]
use memmap2::MmapMut;

use crate::region::Region;

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
const FILE_VERSION: u32 = 1;
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
const H_HIMO_TYPES: usize = 256;

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
    himo_cyl_size: usize,
    himo_slot_size: usize,
    cyl_max_values: u32,
    total_size: usize,
}

impl Layout {
    fn compute(max_entities: u32, max_himos: u32, vocab_data_size: usize, content_data_size: Option<usize>) -> Self {
        let vocab_max_entries = max_entities.saturating_mul(16).min(256_000_000);
        let vocab_index_cap = vocab_max_entries.next_power_of_two();
        let himoreg_max_entries = max_himos.max(256);
        let himoreg_index_cap = (himoreg_max_entries * 2).next_power_of_two();
        let himoreg_data_size = 64 * 1024;
        let content_data_size = content_data_size.unwrap_or_else(ContentStore::data_region_size);
        let cyl_max_values = DEFAULT_CYL_MAX_VALUES;

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
        let himo_cyl_size = align8(Cylinder::region_size(max_entities, cyl_max_values));
        let himo_slot_size = himo_col_size + himo_cyl_size * 2;

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
    fn himo_cyl_a_off(&self, hid: usize) -> usize {
        self.himo_base_off + hid * self.himo_slot_size + self.himo_col_size
    }
    fn himo_cyl_b_off(&self, hid: usize) -> usize {
        self.himo_base_off + hid * self.himo_slot_size + self.himo_col_size + self.himo_cyl_size
    }
}

// ════════════════ Engine ════════════════

pub struct Engine {
    #[allow(dead_code)]
    path: String,
    layout: Layout,
    max_entities: u32,
    max_himos: u32,
    vocab: Vocabulary,
    himo_reg: Vocabulary,
    himo_to_id: HashMap<String, usize>,
    himo_names: Vec<String>,
    himo_types: Vec<HimoType>,
    himo_max_values: Vec<u32>,
    himos: Vec<HimoStore>,
    entities: EntitySet,
    contents: ContentStore,
    undo: UndoLog,
    backing: Backing, // 最後に drop されるよう最終フィールド
}

impl Engine {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create(path: &str) -> io::Result<Self> {
        Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)
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
        let vds = vocab_data_size.unwrap_or(DEFAULT_VOCAB_DATA_SIZE);
        let max_himos = max_himos.unwrap_or(DEFAULT_MAX_HIMOS);
        let layout = Layout::compute(max_entities, max_himos, vds, content_data_size);

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
            himo_to_id: HashMap::new(), himo_names: Vec::new(),
            himo_types: Vec::new(), himo_max_values: Vec::new(),
            himos: Vec::new(), entities, contents, undo,
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

        let mut himo_to_id = HashMap::new();
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
                unsafe { Region::new(base.add(layout.himo_cyl_a_off(hid)), layout.himo_cyl_size) },
                unsafe { Region::new(base.add(layout.himo_cyl_b_off(hid)), layout.himo_cyl_size) },
                ht, effective_mv,
            );

            himo_to_id.insert(name.clone(), hid);
            himo_names.push(name);
            himo_types.push(ht);
            himo_max_values.push(mv);
            himos.push(hs);
        }

        let mut eng = Self {
            path: String::new(), layout, max_entities, max_himos,
            vocab, himo_reg,
            himo_to_id, himo_names, himo_types, himo_max_values,
            himos, entities, contents, undo,
            backing,
        };

        if eng.undo.pending_count() > 0 {
            eng.recover();
        }
        eng.rebuild();

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
        self.himos[hid].set(eid, vid);
    }

    pub fn tie(&mut self, eid: u32, himo: &str, value: u32) {
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, HimoType::Value, 0);
        self.record_undo(eid, hid);
        self.himos[hid].set(eid, value);
    }

    pub fn tie_ref(&mut self, eid: u32, himo: &str, target_eid: u32) {
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = self.ensure_himo(himo, HimoType::Ref, 0);
        self.record_undo(eid, hid);
        self.himos[hid].set(eid, target_eid);
    }

    // ──── tie（定義済み紐、&self で並行書き込み可）────

    /// 定義済みの紐に文字列を張る。&selfで呼べる（Arc共有のまま書き込み可）。
    /// 紐が未定義ならpanic。define_himo を先に呼ぶこと。
    pub fn tie_text_to(&self, eid: u32, himo: &str, value: &str) {
        let vid = self.vocab.get_or_insert(value.as_bytes());
        let hid = *self.himo_to_id.get(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        self.record_undo(eid, hid);
        self.himos[hid].set(eid, vid);
    }

    /// 定義済みの紐にu32値を張る。&selfで呼べる。
    pub fn tie_to(&self, eid: u32, himo: &str, value: u32) {
        assert!(value < u32::MAX, "value must be < u32::MAX (sentinel reserved)");
        let hid = *self.himo_to_id.get(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        self.record_undo(eid, hid);
        self.himos[hid].set(eid, value);
    }

    /// 定義済みの紐にentity参照を張る。&selfで呼べる。
    pub fn tie_ref_to(&self, eid: u32, himo: &str, target_eid: u32) {
        assert!(target_eid < u32::MAX, "target_eid must be < u32::MAX (sentinel reserved)");
        let hid = *self.himo_to_id.get(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo));
        self.record_undo(eid, hid);
        self.himos[hid].set(eid, target_eid);
    }

    // ──── untie ────

    pub fn untie(&self, eid: u32, himo: &str) {
        if let Some(&hid) = self.himo_to_id.get(himo) {
            self.record_undo(eid, hid);
            self.himos[hid].remove(eid);
        }
    }

    // ──── delete ────

    pub fn delete(&self, eid: u32) {
        self.undo.record(eid, 0xFFFF, &[2, 0, 0, 0]); // entity deleted
        for hid in 0..self.himos.len() {
            self.record_undo(eid, hid);
            self.himos[hid].remove(eid);
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
        let hid = *self.himo_to_id.get(himo)?;
        if self.himo_types[hid] != HimoType::Symbol { return None; }
        let vid = self.himos[hid].get_value(eid)?;
        Some(self.vocab.get(vid))
    }

    pub fn get(&self, eid: u32, himo: &str) -> Option<u32> {
        let hid = *self.himo_to_id.get(himo)?;
        self.himos[hid].get_value(eid)
    }

    pub fn vocab_id(&self, text: &str) -> Option<u32> { self.vocab.lookup(text.as_bytes()) }

    pub fn himos_of(&self, eid: u32) -> Vec<&str> {
        self.himos.iter().enumerate()
            .filter(|(_, ds)| ds.get_value(eid).is_some())
            .map(|(i, _)| self.himo_names[i].as_str())
            .collect()
    }
    pub(crate) fn vocab(&self) -> &Vocabulary { &self.vocab }
    pub fn himo_names(&self) -> &[String] { &self.himo_names }

    pub fn himo_type(&self, himo: &str) -> Option<HimoType> {
        self.himo_to_id.get(himo).map(|&idx| self.himo_types[idx])
    }

    // ──── 紐を引く（Cylinder 経由）────

    /// Cylinder + Bitmap キャッシュを再構築。並行read安全。
    pub fn rebuild(&self) {
        for ds in &self.himos { ds.rebuild_cylinder(); }
    }

    pub fn pull_raw(&self, himo: &str, value: u32) -> &[u32] {
        match self.himo_to_id.get(himo) {
            Some(&idx) => self.himos[idx].cylinder().slice_one(value),
            None => &[],
        }
    }

    pub fn query(&self, strings: &[(&str, u32)]) -> Vec<u32> {
        self.rebuild();
        if strings.is_empty() { return vec![]; }

        // 全条件の himo index と value を解決
        let mut conds: Vec<(usize, u32)> = Vec::with_capacity(strings.len());
        for &(himo, val) in strings {
            match self.himo_to_id.get(himo) {
                Some(&idx) => conds.push((idx, val)),
                None => return vec![],
            }
        }

        if conds.len() == 1 {
            return self.himos[conds[0].0].cylinder().slice_one(conds[0].1).to_vec();
        }

        // 全条件bitmapならAND、それ以外はColumn直読み
        let all_bitmap = conds.len() >= 2
            && conds.iter().all(|&(idx, _)| self.himos[idx].has_bitmaps());

        if all_bitmap {
            return self.query_bitmap_and(&conds);
        }

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

    /// Column直読みフィルタ
    fn query_column_filter(&self, conds: &[(usize, u32)]) -> Vec<u32> {
        let mut best = 0;
        let mut best_len = usize::MAX;
        for (i, &(idx, val)) in conds.iter().enumerate() {
            let len = self.himos[idx].cylinder().slice_one(val).len();
            if len == 0 { return vec![]; }
            if len < best_len { best_len = len; best = i; }
        }

        let (pivot_idx, pivot_val) = conds[best];
        let candidates = self.himos[pivot_idx].cylinder().slice_one(pivot_val);
        let mut result = Vec::with_capacity(best_len);
        for &eid in candidates {
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

    fn ensure_himo(&mut self, himo: &str, ht: HimoType, max_values: u32) -> usize {
        if let Some(&idx) = self.himo_to_id.get(himo) { return idx; }
        let hid = self.himos.len();
        assert!((hid as u32) < self.max_himos, "too many himos (max {})", self.max_himos);

        self.himo_reg.get_or_insert(himo.as_bytes());

        let effective_mv = max_values.min(self.layout.cyl_max_values);
        let base = self.backing.as_mut_ptr();
        let col_off = self.layout.himo_col_off(hid);
        let cyl_a_off = self.layout.himo_cyl_a_off(hid);
        let cyl_b_off = self.layout.himo_cyl_b_off(hid);

        let hs = HimoStore::init(
            unsafe { Region::new(base.add(col_off), self.layout.himo_col_size) },
            unsafe { Region::new(base.add(cyl_a_off), self.layout.himo_cyl_size) },
            unsafe { Region::new(base.add(cyl_b_off), self.layout.himo_cyl_size) },
            ht, effective_mv, self.max_entities,
        );

        self.himos.push(hs);
        self.himo_to_id.insert(himo.to_string(), hid);
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

        hid
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
}
