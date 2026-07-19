//! ContentStore — 非索引コンテンツ格納。Region経由。
//!
//! entity の自由テキスト（備考、メモ等）を格納。紐を張らない。検索対象外。
//!
//! Layout:
//!   index Region: (eid * MAX_KEYS + key_hash) × 8B → [offset:u32, len:u32]
//!   data Region:  [MAGIC: 4B][data_end: AtomicU32 LE][pad: 4B][content bytes...]
//!
//! data_end は **mmap 上の AtomicU32** (region byte 4..8)。
//! MAP_SHARED で map された region を複数プロセスが同時に開いても、
//! fetch_add が atomic に動くので offset 衝突 / data 上書き が起きない。
//! (BUGS.md の cross-process content overwrite bug の修正。)

use std::sync::atomic::Ordering;
use crate::region::Region;

const MAGIC: [u8; 4] = [b'C', b'N', b'T', b'1'];
const INDEX_HEADER: usize = 16;
const MAX_KEYS: u32 = 16;
// index_region_size で実際の max_entities を受け取る
const DEFAULT_MAX_ENTITIES: u32 = 16_777_216;
const DATA_HEADER: usize = 12;
/// data region 内の data_end フィールド offset (AtomicU32)。
const DATA_END_OFFSET: usize = 4;

pub struct ContentStore {
    index: Region,
    data: Region,
}

unsafe impl Sync for ContentStore {}
unsafe impl Send for ContentStore {}

impl ContentStore {
    pub fn index_region_size_for(max_entities: u32) -> usize {
        INDEX_HEADER + (max_entities as usize) * (MAX_KEYS as usize) * 8
    }

    pub fn index_region_size() -> usize {
        Self::index_region_size_for(DEFAULT_MAX_ENTITIES)
    }

    pub fn data_region_size() -> usize {
        512 * 1024 * 1024 // 512MB
    }

    /// 新規領域を初期化。
    ///
    /// `index` 領域 (固定 cluster) は eager init。 `data` 領域 (variable
    /// cluster、 v3 layout で末尾) は **何も書かない** — `set` の初回呼び
    /// 出しで lazy 初期化する (Phase B Step 2)。 これで growable backing
    /// の `initial_commit` が data 領域を含まなくて済む。
    pub fn init(index: Region, data: Region) -> Self {
        index.write_at(0, &MAGIC);
        Self { index, data }
    }

    /// 既存領域をロード。data_end は region 上の atomic をそのまま使う(持ち出さない)。
    pub fn load(index: Region, data: Region) -> Self {
        Self { index, data }
    }

    /// data_end atomic への参照 (mmap 上の同一場所、cross-process 整合)。
    #[inline]
    fn data_end(&self) -> &std::sync::atomic::AtomicU32 {
        self.data.as_atomic_u32(DATA_END_OFFSET)
    }

    fn key_hash(key: &str) -> u32 {
        let mut h = 0x811c9dc5u32;
        for b in key.bytes() { h ^= b as u32; h = h.wrapping_mul(0x01000193); }
        h % MAX_KEYS
    }

    fn index_offset(eid: u32, key_hash: u32) -> usize {
        INDEX_HEADER + ((eid as usize) * (MAX_KEYS as usize) + key_hash as usize) * 8
    }

    pub fn set(&self, eid: u32, key: &str, content: &[u8]) {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);

        let len = content.len() as u32;
        // Lazy init guard: `init` は data 領域に何も書かないので、 fresh
        // region の data_end は 0 のまま。 この CAS で「最初の writer」が
        // DATA_HEADER に bump する。 idempotent — 既に non-zero なら
        // CAS は失敗してそのまま (race も MAGIC 書きで吸収される)。
        let _ = self.data.ensure_committed(DATA_HEADER);
        let _ = self.data_end().compare_exchange(
            0,
            DATA_HEADER as u32,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        // cross-process atomic fetch_add。mmap shared page なので別プロセスと整合。
        let data_off = self.data_end().fetch_add(len, Ordering::Relaxed);
        // Extend the file-backed commit before writing — no-op on
        // static backings, real grow on Backing::Growable.
        let _ = self
            .data
            .ensure_committed((data_off + len) as usize);
        let _ = self.index.ensure_committed(off + 8);
        let data_len = self.data.len();
        if (data_off + len) as usize > data_len {
            // data領域溢れ — fetch_addを巻き戻してパニック
            self.data_end().fetch_sub(len, Ordering::Relaxed);
            panic!("ContentStore data overflow: {} + {} > {}", data_off, len, data_len);
        }
        // MAGIC は lazy — ここで毎回書く (idempotent、 4 byte copy)。
        self.data.write_at(0, &MAGIC);
        self.data.write_at(data_off as usize, content);
        // request3: MAGIC + content 範囲を dirty
        self.data.mark_dirty(0, 4);
        self.data.mark_dirty(data_off as usize, len as usize);

        if off + 8 <= self.index.len() {
            self.index.write_at(off, &data_off.to_le_bytes());
            self.index.write_at(off + 4, &len.to_le_bytes());
            self.index.mark_dirty(off, 8);
        }
    }

    pub fn get(&self, eid: u32, key: &str) -> Option<&[u8]> {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);

        let im = self.index.slice();
        if off + 8 > im.len() { return None; }

        let data_off = u32::from_le_bytes(im[off..off + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(im[off + 4..off + 8].try_into().unwrap()) as usize;
        if data_off == 0 || len == 0 { return None; }

        let dm = self.data.slice();
        if data_off + len > dm.len() { return None; }
        Some(&dm[data_off..data_off + len])
    }

    pub fn remove(&self, eid: u32, key: &str) {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);
        if off + 8 <= self.index.len() {
            self.index.fill_at(off, 8, 0);
            self.index.mark_dirty(off, 8);
        }
    }

    /// data_end は mmap 上 AtomicU32 で常時永続化されているので、sync は no-op。
    /// 互換性のため残す(flush 経路が呼んでる)。
    pub fn sync(&self) {
        // no-op: data_end lives in the mapped region directly
    }
}
