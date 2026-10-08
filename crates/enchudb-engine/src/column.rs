//! Column — 固定長カラム。Region経由で単一mmapの一部を使う。
//!
//! remap なし。ロックなし。ensure_capacity なし。
//! 仮想アドレス空間だけ確保、物理メモリは書いたページ分だけ。

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const HEADER: usize = 16;

/// Column region 先頭のメタ領域 (count / value_size / max_entities / cell の開始位置)。
/// growable backing で「header を読む前にどこまで commit すべきか」を
/// 呼び側 (engine の v9 version column) が知るために公開する。
pub const HEADER_BYTES: usize = HEADER;

/// #400: cell を header から離して置く時の、 cell の開始位置 (region 先頭から)。
///
/// APFS は、 書いた所に隣り合う **16 MiB 未満の穴**を書いた時に 0 で埋めて実体化する (手元の計測: 64 MiB の file でも
/// 3 MiB 地点に書くと先頭から 3 MiB が実体になる。 穴が 16 MiB をわずかに越えると埋めない)。 cell の位置は DB 全体の
/// 通し eid なので、 後ろの table の列は header (先頭) と自分の行の間が全部穴で、 eid が約 400 万 (u32 の cell) までは
/// その穴が実ディスクになっていた。 cell を header から 17 MiB 離すと、 その穴が埋まらない。 離した分は file の上の穴と
/// 仮想の予約だけで、 実ディスクも RAM も食わない。
pub const CELLS_PAD: usize = 17 << 20;

/// header の cell の開始位置の欄 (u32)。 0 = 旧形式 (`HEADER` の直後から)。
const CELLS_AT: usize = 12;

pub struct Column {
    region: Region,
    count: AtomicU32,
    pub(crate) value_size: u32,
    pub(crate) max_entities: u32,
    /// cell の開始位置 (region 先頭から)。 `HEADER` (旧形式) か `CELLS_PAD` (#400)。
    cells: usize,
}

unsafe impl Sync for Column {}
unsafe impl Send for Column {}

impl Column {
    /// `padded` = cell を header から `CELLS_PAD` 離して置く (#400)。 region は `CELLS_PAD - HEADER` だけ長く取ること。
    pub fn init(region: Region, value_size: u32, max_entities: u32, padded: bool) -> Self {
        let cells = if padded { CELLS_PAD } else { HEADER };
        region.write_at(0, &0u32.to_le_bytes());
        region.write_at(4, &value_size.to_le_bytes());
        region.write_at(8, &max_entities.to_le_bytes());
        region.write_at(CELLS_AT, &Self::cells_field(cells).to_le_bytes());
        // #414: concurrent の書き出し (`flush_dirty`) は印の付いた範囲だけ msync する。 印が無いと、 実行中に
        // 足した列の header は書き出されない (電源断の後に value_size 0 の列になる)
        region.mark_dirty(0, HEADER);
        Self::with_cells(region, 0, value_size, max_entities, cells)
    }

    /// open 時の load。 header の value_size が 0 = header が一度もディスクに届いていない (作った直後に
    /// 電源が落ちた、 #414) なら、 書かれていない列として組む (`init_lazy`、 header は最初の書き込みが書く)。
    /// そのまま `load` すると value_size 0 の列になり、 読みの assert (`values_u32`) で落ちる。
    pub fn load_or_unwritten(region: Region, value_size: u32, max_entities: u32, padded: bool) -> Self {
        let stored_vs = u32::from_le_bytes(region.slice()[4..8].try_into().unwrap());
        if stored_vs == 0 {
            Self::init_lazy(region, value_size, max_entities, padded)
        } else {
            Self::load(region)
        }
    }

    fn cells_field(cells: usize) -> u32 {
        if cells == HEADER { 0 } else { cells as u32 }
    }

    fn with_cells(region: Region, count: u32, value_size: u32, max_entities: u32, cells: usize) -> Self {
        if cells != HEADER {
            // 伸ばす歩幅を cell の量で決める (file の長さで決めると、 離した分で最初から 16 MB ずつ伸ばし、 末尾の穴が
            // 埋められる)
            region.set_grow_base(cells);
        }
        Self { region, count: AtomicU32::new(count), value_size, max_entities, cells }
    }

    /// region を **1 byte も触らずに** 空の column を組み立てる (request18)。
    ///
    /// growable backing では commit が単調 high-water なので、 variable cluster
    /// 末尾に置かれた region (v9 version column / tombstone column) の header を
    /// 読み書きするだけで **手前の vocab_data / content_data / leaf_data が丸ごと
    /// commit される** (100K entity の growable DB で create 直後 1.7 GB)。
    /// header は最初の実書き込み直前に `ensure_header` が書く。
    pub fn init_lazy(region: Region, value_size: u32, max_entities: u32, padded: bool) -> Self {
        let cells = if padded { CELLS_PAD } else { HEADER };
        Self::with_cells(region, 0, value_size, max_entities, cells)
    }

    /// `init_lazy` で作った column の header を確定させる。 既に書かれていれば no-op。
    /// 呼び側は先に `ensure_committed_for` で region 先頭を commit しておくこと。
    #[inline]
    pub fn ensure_header(&self) {
        let stored = u32::from_le_bytes(self.region.slice()[4..8].try_into().unwrap());
        if stored != self.value_size {
            self.region.write_at(4, &self.value_size.to_le_bytes());
            self.region.write_at(8, &self.max_entities.to_le_bytes());
            self.region.write_at(CELLS_AT, &Self::cells_field(self.cells).to_le_bytes());
            self.region.mark_dirty(0, HEADER);
        }
    }

    pub fn load(region: Region) -> Self {
        let mm = region.slice();
        let count = u32::from_le_bytes(mm[0..4].try_into().unwrap());
        let value_size = u32::from_le_bytes(mm[4..8].try_into().unwrap());
        let max_entities = u32::from_le_bytes(mm[8..12].try_into().unwrap());
        let cells = match u32::from_le_bytes(mm[CELLS_AT..CELLS_AT + 4].try_into().unwrap()) {
            0 => HEADER,
            c => c as usize,
        };
        Self::with_cells(region, count, value_size, max_entities, cells)
    }

    pub fn region_size(max_entities: u32, value_size: u32) -> usize {
        HEADER + (max_entities as usize) * (value_size as usize)
    }

    /// cell を `CELLS_PAD` 離して置いているか (#400)。
    pub fn is_padded(&self) -> bool {
        self.cells != HEADER
    }

    /// file の先頭 16 B (column の header) から、 cell の開始位置を読む (#400、 0 = 旧形式の `HEADER`)。
    pub fn cells_offset_of(header: &[u8]) -> usize {
        match header.get(CELLS_AT..CELLS_AT + 4).map(|b| u32::from_le_bytes(b.try_into().unwrap())) {
            Some(c) if c != 0 => c as usize,
            _ => HEADER,
        }
    }

    #[inline]
    pub fn set(&self, entity_id: u32, value: &[u8]) {
        let vs = self.value_size as usize;
        let off = self.cells + (entity_id as usize) * vs;
        let len = value.len().min(vs);
        self.region.write_at(off, &value[..len]);
        self.region.mark_dirty(off, len);
    }

    /// growable backing 用: `entity_id` の cell まで file の commit を伸ばす。
    /// 静的 backing (通常の mmap / memory) では no-op。
    ///
    /// variable cluster の末尾に置かれる region (v9 version column / tombstone
    /// column) は初期 commit の外にあるので、 **書く前に必ず呼ぶこと** —
    /// 呼ばずに触ると未コミット page への書き込みで SIGBUS する。
    #[inline]
    pub fn ensure_committed_for(&self, entity_id: u32) -> std::io::Result<()> {
        let vs = self.value_size.max(1) as usize;
        self.region.ensure_committed(self.cells + (entity_id as usize + 1) * vs)
    }

    /// `entity_id` の cell が commit 済みか (伸ばさない、 atomic の読み 1 回)。
    #[inline]
    pub fn is_committed_for(&self, entity_id: u32) -> bool {
        let vs = self.value_size.max(1) as usize;
        self.region.is_committed(self.cells + (entity_id as usize + 1) * vs)
    }

    #[inline]
    pub fn get(&self, entity_id: u32) -> &[u8] {
        let vs = self.value_size as usize;
        let off = self.cells + (entity_id as usize) * vs;
        let mm = self.region.slice();
        &mm[off..off + vs]
    }

    /// #106: 4B 値を Release で publish する (leaf offset 用)。 `load_u32_acquire` と対で、
    /// この offset を書く *前* の書込 (= LeafStore slot の payload/gen) の可視性を保証する。
    /// これが無いと reader が「新 offset は見えるが leaf slot はまだ stale」を掴み、
    /// seqlock (gen) を stale 値で誤通過して torn read になる。 value_size==4 前提。
    #[inline]
    pub fn store_u32_release(&self, entity_id: u32, v: u32) {
        let off = self.cells + (entity_id as usize) * (self.value_size as usize);
        self.region.as_atomic_u32(off).store(v, Ordering::Release);
        self.region.mark_dirty(off, 4);
    }

    /// #106: 4B 値を Acquire で読む (`store_u32_release` と対)。 value_size==4 前提。
    #[inline]
    pub fn load_u32_acquire(&self, entity_id: u32) -> u32 {
        let off = self.cells + (entity_id as usize) * (self.value_size as usize);
        self.region.as_atomic_u32(off).load(Ordering::Acquire)
    }

    /// 64 bit 列の cell を Release で書く (`store_u32_release` の 8B 版)。 value_size==8 前提。
    #[inline]
    pub fn store_u64_release(&self, entity_id: u32, v: u64) {
        debug_assert_eq!(self.value_size, 8);
        let off = self.cells + (entity_id as usize) * 8;
        self.region.as_atomic_u64(off).store(v, Ordering::Release);
        self.region.mark_dirty(off, 8);
    }

    /// 64 bit 列の cell を Acquire で読む。 value_size==8 前提。
    #[inline]
    pub fn load_u64_acquire(&self, entity_id: u32) -> u64 {
        debug_assert_eq!(self.value_size, 8);
        let off = self.cells + (entity_id as usize) * 8;
        self.region.as_atomic_u64(off).load(Ordering::Acquire)
    }

    /// 0.8.6: u32 packed value slice。 SIMD 集計 / 全件 scan 用の fast path。
    /// `value_size == 4` (= Number / Tag / Leaf 等の通常 himo) でのみ意味あり。
    /// 戻り値の長さは `count()`、 stored 形式 (= 0 = 未設定、 N = 値 N-1)。
    ///
    /// HEADER (= 16 bytes) は u32 alignment、 mmap region は page-aligned なので
    /// アライン安全。 LE 前提 (aarch64 / x86_64 等の supported target で OK)。
    #[inline]
    pub fn values_u32(&self) -> &[u32] {
        // 64 bit 列を 4B ずつ読むと値が化ける (黙った破損) ので release でも止める
        assert_eq!(self.value_size, 4, "values_u32 requires value_size == 4");
        let n = self.count() as usize;
        let mm = self.region.slice();
        // SAFETY: HEADER (16) は u32 アラインで、 mmap region は page-aligned。
        // n * 4 <= max_entities * 4 (= region 内 packed 領域の上限)。
        // u32 LE は aarch64 / x86_64 で native u32 と一致。
        let base = unsafe { mm.as_ptr().add(self.cells) as *const u32 };
        unsafe { std::slice::from_raw_parts(base, n) }
    }

    #[inline]
    pub fn clear(&self, entity_id: u32) {
        let vs = self.value_size as usize;
        let off = self.cells + (entity_id as usize) * vs;
        // v10: 未 commit の cell は zero page で既に 0 (= 未設定)。 書くと fault するので触らない。
        if !self.region.is_committed(off + vs) {
            return;
        }
        self.region.fill_at(off, vs, 0);
        self.region.mark_dirty(off, vs);
    }

    pub fn count(&self) -> u32 { self.count.load(Ordering::Relaxed) }

    /// count を eid+1 まで引き上げる（atomic、並列安全）。
    pub fn ensure_count(&self, eid: u32) {
        let needed = eid + 1;
        self.count.fetch_max(needed, Ordering::Relaxed);
        // mmapヘッダにも書く。 count は rebuild/flush 時に確定するので、ここでは最大値。
        let current = u32::from_le_bytes(self.region.slice()[0..4].try_into().unwrap());
        if needed > current {
            self.region.write_at(0, &needed.to_le_bytes());
        }
    }

    /// flush用: 現在のcountをmmapヘッダに書き出す。
    pub fn sync_count(&self) {
        let c = self.count.load(Ordering::Relaxed);
        self.region.write_at(0, &c.to_le_bytes());
    }
}
