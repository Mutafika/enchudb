//! PositionsRegion — BucketCylinder の positions (eid → (value, idx)) を
//! Region (mmap-back 可) に置く。 β-heavy phase 1。
//!
//! # 動機
//!
//! 旧 BucketCylinder.positions は `Vec<(u32, u32)>` の anonymous heap。 100M
//! entity × 100 himo scale で 80 GB anon → process RSS 主因。 Region に
//! 置くと page cache 経由で working set 比例 RSS にできる。
//!
//! # Layout
//!
//! ```text
//! offset  size  field
//! 0       4     MAGIC = "POS1"
//! 4       4     eid_offset: u32          (= u32::MAX → "empty")
//! 8       4     max_offset: u32          (= 最大の (eid - eid_offset)、 + 1 が論理 len)
//! 12      4     reserved
//! 16      N×8   entries[i] = (value: u32, idx: u32)、 i = eid - eid_offset
//! ```
//!
//! - `value == EMPTY_VALUE (= u32::MAX)` は 「その eid は未 tie」。 (BucketCylinder
//!   の sentinel と同じ。 valid value は < EMPTY_VALUE。)
//! - entry index は `eid - eid_offset`。 `eid < eid_offset` は範囲外 (None)。
//! - region capacity (= region.len()) が許す範囲で `max_offset` を伸ばす。 grower
//!   付き region なら `set` で `ensure_committed` が走って page commit が広がる。
//!
//! # eid_offset の決まり方
//!
//! 第一回 `set(eid, ...)` で `eid_offset = eid` を確定。 以降より小さい eid の
//! set が来た場合は **panic**: 旧 Vec ベースの prepend と違って、 mmap 上での
//! データ shift は高コスト + 単純化のため許容しない。
//!
//! 呼び出し側は次のいずれかで保証する:
//! - β-light の `entity_in(table)` は table の eid_range 内で monotonic に
//!   eid を払い出す → 自然に satisfied。
//! - anonymous table の `entity()` も engine global の next_eid を monotonic
//!   increment → satisfied。
//! - sync 経由の `ensure_live(eid)` は外から飛んでくる eid を accept するが、
//!   peer のローカル空間で monotonic なはずなので順序が大きく崩れない限り
//!   問題にならない (= 同 peer の old eid が先に届くと panic はする)。
//!   実運用で問題になったら別途 sparse fallback を検討する。
//!
//! # 並行性
//!
//! 現状 BucketCylinder 全体が `RwLock` で保護されているため、 PositionsRegion
//! 単体での atomic 操作は要求しない。 region への書き込みは write lock 下で
//! のみ起きる前提。

use crate::region::Region;

const MAGIC: [u8; 4] = [b'P', b'O', b'S', b'1'];
const HEADER: usize = 16;

const H_EID_OFFSET: usize = 4;
const H_MAX_OFFSET: usize = 8;

const EMPTY_EID_OFFSET: u32 = u32::MAX;
pub const EMPTY_VALUE: u32 = u32::MAX;

const ENTRY_SIZE: usize = 8;

pub struct PositionsRegion {
    region: Region,
}

unsafe impl Send for PositionsRegion {}
unsafe impl Sync for PositionsRegion {}

impl PositionsRegion {
    /// 必要 region サイズ (bytes)。 max_entries は positions に乗りうる
    /// (eid - eid_offset) の最大値 + 1。
    pub const fn region_size(max_entries: u32) -> usize {
        HEADER + (max_entries as usize) * ENTRY_SIZE
    }

    /// 新規領域を初期化。 region.len() は 16 byte 以上必須。
    pub fn init(region: Region) -> Self {
        assert!(region.len() >= HEADER, "PositionsRegion init: region too small");
        let _ = region.ensure_committed(HEADER);
        let mm = region.slice_mut();
        mm[0..4].copy_from_slice(&MAGIC);
        mm[H_EID_OFFSET..H_EID_OFFSET + 4]
            .copy_from_slice(&EMPTY_EID_OFFSET.to_le_bytes());
        mm[H_MAX_OFFSET..H_MAX_OFFSET + 4].copy_from_slice(&0u32.to_le_bytes());
        mm[12..16].copy_from_slice(&0u32.to_le_bytes());
        Self { region }
    }

    /// 既存領域をロード。 magic 不一致なら panic。
    pub fn load(region: Region) -> Self {
        assert!(region.len() >= HEADER, "PositionsRegion load: region too small");
        let mm = region.slice();
        assert_eq!(&mm[0..4], &MAGIC, "PositionsRegion: bad magic");
        Self { region }
    }

    /// magic が一致するか覗き見 (load 前判定用)。
    pub fn has_valid_magic(region: &Region) -> bool {
        if region.len() < HEADER {
            return false;
        }
        let mm = region.slice();
        mm[0..4] == MAGIC
    }

    /// 「初期化されていない / 空」 状態か。 lazy migration の判定用。
    pub fn is_empty(&self) -> bool {
        self.eid_offset() == EMPTY_EID_OFFSET
    }

    #[inline]
    pub fn eid_offset(&self) -> u32 {
        let mm = self.region.slice();
        u32::from_le_bytes(mm[H_EID_OFFSET..H_EID_OFFSET + 4].try_into().unwrap())
    }

    #[inline]
    fn max_offset(&self) -> u32 {
        let mm = self.region.slice();
        u32::from_le_bytes(mm[H_MAX_OFFSET..H_MAX_OFFSET + 4].try_into().unwrap())
    }

    fn set_eid_offset(&self, v: u32) {
        let mm = self.region.slice_mut();
        mm[H_EID_OFFSET..H_EID_OFFSET + 4].copy_from_slice(&v.to_le_bytes());
        self.region.mark_dirty(H_EID_OFFSET, 4);
    }

    fn set_max_offset(&self, v: u32) {
        let mm = self.region.slice_mut();
        mm[H_MAX_OFFSET..H_MAX_OFFSET + 4].copy_from_slice(&v.to_le_bytes());
        self.region.mark_dirty(H_MAX_OFFSET, 4);
    }

    /// 論理長 (= 既知の eid range の幅)。 entries[0..len()] が valid index 範囲。
    /// 空なら 0。
    pub fn len(&self) -> u32 {
        if self.is_empty() {
            0
        } else {
            self.max_offset() + 1
        }
    }

    /// eid に対応する (value, idx) を返す。 範囲外 / 空 / sentinel なら None。
    #[inline]
    pub fn get(&self, eid: u32) -> Option<(u32, u32)> {
        let off = self.eid_offset();
        if off == EMPTY_EID_OFFSET || eid < off {
            return None;
        }
        let idx_in_arr = eid - off;
        if idx_in_arr > self.max_offset() {
            return None;
        }
        let (val, idx) = self.read_entry(idx_in_arr);
        if val == EMPTY_VALUE {
            None
        } else {
            Some((val, idx))
        }
    }

    /// (value, idx) を書き込む。 eid_offset 未設定なら eid に確定。
    /// eid < eid_offset の場合は panic (旧 prepend は許容しない)。
    pub fn set(&self, eid: u32, value: u32, idx: u32) {
        debug_assert!(value != EMPTY_VALUE, "set: value must be < EMPTY_VALUE");
        let off = self.eid_offset();
        let (idx_in_arr, off) = if off == EMPTY_EID_OFFSET {
            self.set_eid_offset(eid);
            (0u32, eid)
        } else if eid < off {
            panic!(
                "PositionsRegion::set: eid {} < eid_offset {} (prepend not supported)",
                eid, off
            );
        } else {
            (eid - off, off)
        };

        self.ensure_offset_in_range(idx_in_arr);
        self.write_entry(idx_in_arr, value, idx);

        if idx_in_arr > self.max_offset() || (idx_in_arr == 0 && off == eid && self.max_offset() == 0) {
            // max_offset が小さければ伸ばす (= 0 のままなら 0 として残す)
            if idx_in_arr > self.max_offset() {
                self.set_max_offset(idx_in_arr);
            }
        }
    }

    /// 既存 entry の idx だけ更新 (swap_remove で末尾と入れ替わった entity 用)。
    /// 範囲外なら no-op。
    pub fn update_idx(&self, eid: u32, new_idx: u32) {
        let off = self.eid_offset();
        if off == EMPTY_EID_OFFSET || eid < off {
            return;
        }
        let idx_in_arr = eid - off;
        if idx_in_arr > self.max_offset() {
            return;
        }
        let (val, _) = self.read_entry(idx_in_arr);
        if val == EMPTY_VALUE {
            return;
        }
        self.write_entry(idx_in_arr, val, new_idx);
    }

    /// eid の entry を空にする (sentinel に戻す)。 範囲外なら no-op。
    pub fn clear(&self, eid: u32) {
        let off = self.eid_offset();
        if off == EMPTY_EID_OFFSET || eid < off {
            return;
        }
        let idx_in_arr = eid - off;
        if idx_in_arr > self.max_offset() {
            return;
        }
        self.write_entry(idx_in_arr, EMPTY_VALUE, 0);
    }

    /// 全体を空にリセット。 rebuild の最初に呼ぶ。
    pub fn clear_all(&self) {
        self.set_eid_offset(EMPTY_EID_OFFSET);
        self.set_max_offset(0);
        // entries は触らない (lazy clear)。 eid_offset reset で論理的に到達不能。
    }

    // ──── internal ────

    #[inline]
    fn read_entry(&self, idx_in_arr: u32) -> (u32, u32) {
        let off = HEADER + (idx_in_arr as usize) * ENTRY_SIZE;
        let mm = self.region.slice();
        let val = u32::from_le_bytes(mm[off..off + 4].try_into().unwrap());
        let idx = u32::from_le_bytes(mm[off + 4..off + 8].try_into().unwrap());
        (val, idx)
    }

    #[inline]
    fn write_entry(&self, idx_in_arr: u32, value: u32, idx: u32) {
        let off = HEADER + (idx_in_arr as usize) * ENTRY_SIZE;
        let mm = self.region.slice_mut();
        mm[off..off + 4].copy_from_slice(&value.to_le_bytes());
        mm[off + 4..off + 8].copy_from_slice(&idx.to_le_bytes());
        self.region.mark_dirty(off, ENTRY_SIZE);
    }

    /// idx_in_arr が region 範囲内で書き込めるよう、 必要なら commit を伸ばす +
    /// 新しい range 内の entry を EMPTY_VALUE で埋める。
    fn ensure_offset_in_range(&self, idx_in_arr: u32) {
        let need_bytes = HEADER + ((idx_in_arr as usize) + 1) * ENTRY_SIZE;
        assert!(
            need_bytes <= self.region.len(),
            "PositionsRegion: idx {} 超過 region capacity (need {} bytes, have {})",
            idx_in_arr, need_bytes, self.region.len()
        );
        let _ = self.region.ensure_committed(need_bytes);

        // 既存 max_offset + 1 .. idx_in_arr の間を sentinel で埋める。
        // (commit したばかりの page は zero-init なので value=0 になっている →
        // valid な value 0 と区別がつかないので sentinel に書き換える必要あり。)
        let cur_max = if self.is_empty() {
            // 初期化直後: idx 0 含めて埋める対象は idx_in_arr 以下全部 (新規 region)。
            // ただし set 側で entry[idx_in_arr] を直後に上書きするので、
            // [0..idx_in_arr) の範囲を埋める。 idx_in_arr 自身は呼び出し側が書く。
            if idx_in_arr == 0 {
                return;
            }
            0u32
        } else {
            self.max_offset() + 1
        };
        if cur_max <= idx_in_arr {
            // [cur_max..idx_in_arr) (※ idx_in_arr 自身は除く: set 側で書く) を埋める
            // ただし、 max_offset = 0 で is_empty=false (= idx 0 が valid) のとき、
            // cur_max = 1。 idx_in_arr=1 なら range は空。 idx_in_arr=5 なら 1..5 を埋める。
            for i in cur_max..idx_in_arr {
                self.write_entry(i, EMPTY_VALUE, 0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// テスト用: heap 上に bytes を確保して Region を作る。 Vec データポインタは
    /// Vec 自体を move しても安定 (heap 上の allocation は動かない) なので、
    /// caller が Vec を leak するか保持し続ければ Region は有効。
    fn make_heap_region(capacity: usize) -> (Box<[u8]>, Region) {
        let mut bytes = vec![0u8; capacity].into_boxed_slice();
        let ptr = bytes.as_mut_ptr();
        let len = bytes.len();
        let region = unsafe { Region::new(ptr, len) };
        (bytes, region)
    }

    #[test]
    fn init_load_roundtrip() {
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);
        assert!(pr.is_empty());
        assert_eq!(pr.len(), 0);
        assert_eq!(pr.eid_offset(), EMPTY_EID_OFFSET);

        // 同じ memory に Region を再貼り付けて load
        let region2 = unsafe { Region::new(_owner.as_ptr() as *mut u8, _owner.len()) };
        let pr2 = PositionsRegion::load(region2);
        assert!(pr2.is_empty());
    }

    #[test]
    fn set_then_get() {
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);

        pr.set(5, 42, 0);
        assert_eq!(pr.get(5), Some((42, 0)));
        assert_eq!(pr.get(4), None);
        assert_eq!(pr.get(20), None);
        assert_eq!(pr.eid_offset(), 5);
        assert_eq!(pr.len(), 1);
    }

    #[test]
    fn set_extends_range() {
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);

        pr.set(2, 100, 0);
        pr.set(5, 200, 1);
        assert_eq!(pr.eid_offset(), 2);
        assert_eq!(pr.len(), 4); // 2..=5
        assert_eq!(pr.get(2), Some((100, 0)));
        assert_eq!(pr.get(3), None); // 隙間は sentinel
        assert_eq!(pr.get(4), None);
        assert_eq!(pr.get(5), Some((200, 1)));
    }

    #[test]
    fn update_idx_changes_only_idx() {
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);

        pr.set(3, 7, 0);
        pr.update_idx(3, 5);
        assert_eq!(pr.get(3), Some((7, 5)));
    }

    #[test]
    fn clear_removes_entry() {
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);

        pr.set(3, 7, 0);
        pr.clear(3);
        assert_eq!(pr.get(3), None);
    }

    #[test]
    fn clear_all_resets() {
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);

        pr.set(3, 7, 0);
        pr.set(5, 11, 1);
        pr.clear_all();
        assert!(pr.is_empty());
        assert_eq!(pr.get(3), None);
        assert_eq!(pr.get(5), None);

        // 再 set 可能
        pr.set(10, 99, 0);
        assert_eq!(pr.eid_offset(), 10);
        assert_eq!(pr.get(10), Some((99, 0)));
    }

    #[test]
    #[should_panic(expected = "prepend not supported")]
    fn prepend_panics() {
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);

        pr.set(5, 1, 0);
        pr.set(2, 1, 0); // < eid_offset → panic
    }

    #[test]
    fn sentinel_value_is_empty_after_init() {
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);

        // 大きめの eid を tie してから 0 番目 (eid_offset 直上の隣) を get
        pr.set(0, 5, 0);
        pr.set(10, 7, 0);
        // 隙間 1..10 は EMPTY のはず
        for e in 1..10 {
            assert_eq!(pr.get(e), None, "gap eid {} should be empty", e);
        }
    }

    #[test]
    fn value_zero_is_valid() {
        // value = 0 は valid (sentinel は u32::MAX)
        let cap = PositionsRegion::region_size(16);
        let (_owner, region) = make_heap_region(cap);
        let pr = PositionsRegion::init(region);

        pr.set(7, 0, 0);
        assert_eq!(pr.get(7), Some((0, 0)));
    }

    #[test]
    fn has_valid_magic_works() {
        let cap = PositionsRegion::region_size(4);
        let (mut owner, region) = make_heap_region(cap);
        // 未初期化 region は magic 不一致
        assert!(!PositionsRegion::has_valid_magic(&region));
        drop(region);

        let region = unsafe { Region::new(owner.as_mut_ptr(), owner.len()) };
        let _pr = PositionsRegion::init(region);
        let region2 = unsafe { Region::new(owner.as_mut_ptr(), owner.len()) };
        assert!(PositionsRegion::has_valid_magic(&region2));
    }
}
