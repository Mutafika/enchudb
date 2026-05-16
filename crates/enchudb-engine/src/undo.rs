//! Undo ログ — 書き込み前の値を記録。COMMIT でクリア、クラッシュ時に自動復旧。
//!
//! Layout (Region内):
//!   Header (16B): [magic:4][count:u32][reserved:8]
//!   Entries (10B each): [eid:u32][dim_id:u16][old_value:4B]
//!
//! Backpressure (issue3): `record()` は `max_entries` の 90% に近づくと
//! `force_commit` を立てて consumer の即時 fsync→commit を要求し、 count が
//! reset されるまで yield する。 これで sustained 並列 sync writer (256
//! thread × 100K writes 等) で 16M cap を踏み抜いて OOB panic していたのを
//! 防ぐ。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use crate::region::Region;

const MAGIC: [u8; 4] = [b'U', b'N', b'D', b'1'];
const HEADER: usize = 16;
const ENTRY_SIZE: usize = 10;
pub(crate) const DEFAULT_MAX_ENTRIES: u32 = 16_777_216;

/// backpressure 発動 threshold (max_entries に対する比率、 1/10 単位)。
/// 90% = 9 で「max の 90% 超えたら force_commit を立てて yield 待ち」。
const BACKPRESSURE_NUM: u32 = 9;
const BACKPRESSURE_DEN: u32 = 10;

pub struct UndoLog {
    region: Region,
    count: AtomicU32,
    max_entries: u32,
    /// consumer thread に「即 fsync→commit 走って欲しい」 と伝える signal。
    /// writer thread が backpressure threshold 超で立てる。 consumer が
    /// fsync 完了後に clear する。
    force_commit: Arc<AtomicBool>,
}

unsafe impl Sync for UndoLog {}
unsafe impl Send for UndoLog {}

impl UndoLog {
    pub fn region_size() -> usize {
        Self::region_size_with(DEFAULT_MAX_ENTRIES)
    }

    /// Tunable variant — small-footprint engines (state-log presets,
    /// embedded apps) pass a much smaller `max_entries` so the undo
    /// region doesn't dominate the layout. The default 16 M entries
    /// × 10 bytes = 160 MB which is the bulk of `create_compact`'s
    /// 305 MB apparent size.
    pub fn region_size_with(max_entries: u32) -> usize {
        HEADER + max_entries as usize * ENTRY_SIZE
    }

    /// 新規領域を初期化。
    pub fn init(region: Region, max_entries: u32) -> Self {
        let mm = region.slice_mut();
        mm[0..4].copy_from_slice(&MAGIC);
        mm[4..8].copy_from_slice(&0u32.to_le_bytes());
        Self {
            region,
            count: AtomicU32::new(0),
            max_entries,
            force_commit: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 既存領域をロード。
    pub fn load(region: Region, max_entries: u32) -> Self {
        let mm = region.slice();
        let count = if mm.len() >= HEADER && mm[0..4] == MAGIC {
            u32::from_le_bytes(mm[4..8].try_into().unwrap())
        } else {
            // 初期化されていない場合はクリア状態で返す
            let mm = region.slice_mut();
            mm[0..4].copy_from_slice(&MAGIC);
            mm[4..8].copy_from_slice(&0u32.to_le_bytes());
            0
        };
        Self {
            region,
            count: AtomicU32::new(count),
            max_entries,
            force_commit: Arc::new(AtomicBool::new(false)),
        }
    }

    /// consumer thread にこの signal を共有させる用 (engine 側で
    /// `force_commit_signal()` を持ち回って loop 先頭で check する)。
    pub fn force_commit_signal(&self) -> Arc<AtomicBool> {
        self.force_commit.clone()
    }

    /// backpressure threshold。 これを超えると `record()` は force_commit
    /// を立てて待つ。
    #[inline]
    fn backpressure_threshold(&self) -> u32 {
        ((self.max_entries as u64 * BACKPRESSURE_NUM as u64) / BACKPRESSURE_DEN as u64) as u32
    }

    #[inline]
    pub fn record(&self, eid: u32, dim_id: u16, old_value: &[u8; 4]) {
        let threshold = self.backpressure_threshold();
        // backpressure: count >= threshold なら consumer の commit を待つ。
        // force_commit signal で consumer に「100ms 待たずに今すぐ fsync」 と要求。
        // commit() で count が 0 に戻ったら抜ける。
        //
        // ※ consumer thread 自身は本関数を呼ばないこと (self-deadlock になる)。
        //   consumer 側は `record_unchecked` + ループ内 cap 監視で commit する。
        loop {
            let cur = self.count.load(Ordering::Acquire);
            if cur < threshold {
                break;
            }
            self.force_commit.store(true, Ordering::Release);
            std::thread::yield_now();
        }
        self.record_unchecked(eid, dim_id, old_value);
    }

    /// backpressure check 抜きの record。 consumer thread (apply_op) は
    /// commit する責務側なので backpressure spin に入ると self-deadlock する。
    /// consumer ループ側で `pending_count()` を監視して必要なら自発 commit する。
    #[inline]
    pub fn record_unchecked(&self, eid: u32, dim_id: u16, old_value: &[u8; 4]) {
        let idx = self.count.fetch_add(1, Ordering::Relaxed);
        let mm = self.region.slice_mut();
        let off = HEADER + idx as usize * ENTRY_SIZE;
        mm[off..off + 4].copy_from_slice(&eid.to_le_bytes());
        mm[off + 4..off + 6].copy_from_slice(&dim_id.to_le_bytes());
        mm[off + 6..off + 10].copy_from_slice(old_value);
        mm[4..8].copy_from_slice(&(idx + 1).to_le_bytes());
        // request3: header (count 更新) + entry の両方を dirty として記録。
        // header 4 byte + entry 10 byte の union range をマーク (一括で記録)。
        self.region.mark_dirty(4, 4);
        self.region.mark_dirty(off, ENTRY_SIZE);
    }

    /// backpressure 閾値を超えてるか (consumer ループの自発 commit 用)。
    #[inline]
    pub fn over_threshold(&self) -> bool {
        self.count.load(Ordering::Acquire) >= self.backpressure_threshold()
    }

    #[inline]
    pub fn commit(&self) {
        self.count.store(0, Ordering::Release);
        let mm = self.region.slice_mut();
        mm[4..8].copy_from_slice(&0u32.to_le_bytes());
        self.region.mark_dirty(4, 4);
        self.force_commit.store(false, Ordering::Release);
    }

    pub fn pending_count(&self) -> u32 {
        self.count.load(Ordering::Acquire)
    }

    pub fn max_entries(&self) -> u32 {
        self.max_entries
    }

    pub fn entries_reverse(&self) -> Vec<(u32, u16, [u8; 4])> {
        let count = self.count.load(Ordering::Acquire) as usize;
        let mm = self.region.slice();
        let mut result = Vec::with_capacity(count);
        for i in (0..count).rev() {
            let off = HEADER + i * ENTRY_SIZE;
            let eid = u32::from_le_bytes(mm[off..off + 4].try_into().unwrap());
            let dim_id = u16::from_le_bytes(mm[off + 4..off + 6].try_into().unwrap());
            let mut old_value = [0u8; 4];
            old_value.copy_from_slice(&mm[off + 6..off + 10]);
            result.push((eid, dim_id, old_value));
        }
        result
    }
}
