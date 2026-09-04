//! EntitySet — ビットセット + 空きID管理。Region経由。
//!
//! entity の生存管理。allocate / free / is_live / iter。
//! AtomicU32 で next_eid を管理。ロック不要。
//!
//! Layout:
//!   Header (16B): [magic:4][next_eid:AtomicU32][live_count:AtomicU32][bitset_cap:u32]
//!   Bitset: ceil(bitset_cap / 8) bytes — bit=1 で live
//!   Free stack: [count:4][eid0:4][eid1:4]...
//!
//! v10 (request20 / request21 Phase 3): entity の上限 (`cap` = header の `max_entities`) は
//! **伸ばせる**。 bitset は `bitset_cap` (= create 時の reservation、 header offset 12。 0 なら
//! legacy = cap) 分の場所を最初から取り、 free stack はその後ろに固定する。 cap を伸ばしても
//! offset は動かない。 bitset / free stack は触った page だけ commit する (lazy)。

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const MAGIC: [u8; 4] = [b'E', b'N', b'T', b'1'];
const HEADER: usize = 16;
/// region 先頭の header 長 (= bitset の開始 offset)。 migrate 側の検査用 (#257)。
pub(crate) const HEADER_BYTES: usize = HEADER;
const FREE_STACK_MAX: u32 = 1_048_576;

pub struct EntitySet {
    region: Region,
    /// 現在の上限 (exclusive)。 `grow` で伸びる。 bound check は全部ここを見る。
    max_entities: AtomicU32,
    /// bitset の物理容量 (= 伸ばせる上限)。 header offset 12。
    bitset_cap: u32,
    bitset_offset: usize,
    free_offset: usize,
    /// #77-H7: free stack の push/pop 直列化。 push (count 読み → eid 書き →
    /// count 書き) は非 atomic な複合操作のため、 並行 free / saturation 時の
    /// allocate と混ざると slot 破壊・二重払い出しが起きる。 writer は
    /// .db.lock で 1 プロセスに限定されるため in-process Mutex で足りる。
    free_lock: std::sync::Mutex<()>,
}

unsafe impl Sync for EntitySet {}
unsafe impl Send for EntitySet {}

impl EntitySet {
    /// region の大きさ。 `bitset_cap` = bitset が持てる entity 数 (v10 は reservation、
    /// legacy は max_entities)。
    pub fn region_size(bitset_cap: u32) -> usize {
        Self::free_offset_for(bitset_cap) + 4 + Self::free_cap_for(bitset_cap) as usize * 4
    }

    /// free stack に積める eid 数。 「これまで allocate された eid」 しか積めないので論理上限は
    /// bitset_cap。 FREE_STACK_MAX (= 1 M) は default preset 想定の上限で、 tiny では 4 KB。
    pub(crate) fn free_cap_for(bitset_cap: u32) -> u32 {
        FREE_STACK_MAX.min(bitset_cap)
    }

    pub(crate) fn free_offset_for(bitset_cap: u32) -> usize {
        let bitset_size = bitset_cap.div_ceil(8) as usize;
        (HEADER + bitset_size + 3) & !3 // AtomicU32 alignment
    }

    /// legacy (v8 / v9、 free stack が `old_cap` 基準の位置にある) region を、 bitset 容量
    /// `new_cap` の v10 layout に組み直す (migration 用、 in-memory)。 中身 (bit / free stack)
    /// は同じ。
    pub fn relayout(old: &[u8], old_cap: u32, new_cap: u32) -> Vec<u8> {
        assert!(new_cap >= old_cap, "relayout: cannot shrink {old_cap} -> {new_cap}");
        let old_bits = ((old_cap + 7) / 8) as usize;
        let old_free = Self::free_offset_for(old_cap);
        let old_free_cap = Self::free_cap_for(old_cap) as usize;
        let mut out = vec![0u8; Self::region_size(new_cap)];
        let hdr_end = HEADER.min(old.len());
        out[..hdr_end].copy_from_slice(&old[..hdr_end]);
        out[12..16].copy_from_slice(&new_cap.to_le_bytes());
        let bits_end = (HEADER + old_bits).min(old.len());
        if bits_end > HEADER {
            out[HEADER..bits_end].copy_from_slice(&old[HEADER..bits_end]);
        }
        let new_free = Self::free_offset_for(new_cap);
        let free_bytes = (4 + old_free_cap * 4).min(old.len().saturating_sub(old_free));
        if free_bytes > 0 {
            out[new_free..new_free + free_bytes].copy_from_slice(&old[old_free..old_free + free_bytes]);
        }
        out
    }

    /// 新規領域を初期化。
    ///
    /// growable backing で initial_commit が region の手前で打ち切られて
    /// いる可能性があるため、 region 全体 (header + bitset + free stack)
    /// を init 時点で commit する。 EntitySet 全体は max_entities が
    /// 制限されてれば十分小さい (tiny preset で ~4 KB)。
    pub fn init(region: Region, max_entities: u32, bitset_cap: u32) -> Self {
        let bitset_cap = bitset_cap.max(max_entities);
        // #167: init 時の commit は header だけ。 bitset / free stack は触る page ごとに
        // commit する (v10: reservation 分の場所を取るので、 全域 commit すると default で
        // 36 MB が実体化する)。 失敗 = そもそも DB を作れない状況で、 直後の write で気付く。
        let _ = region.ensure_committed(HEADER);
        region.write_at(0, &MAGIC);
        region.write_at(12, &bitset_cap.to_le_bytes());
        // next_eid = 0, live_count = 0 (already zero from fresh region)

        Self {
            region,
            max_entities: AtomicU32::new(max_entities),
            bitset_cap,
            bitset_offset: HEADER,
            free_offset: Self::free_offset_for(bitset_cap),
            free_lock: std::sync::Mutex::new(()),
        }
    }

    /// 既存領域をロード。 `max_entities` は DB header の現在の上限。 bitset 容量は region
    /// 自身の header (offset 12) から。 0 (legacy) なら max_entities。
    ///
    /// 壊れた region では **panic せず `InvalidData`** を返す (`corrupt_header_open` と同じ方針)。
    /// v10 では `entities.seg` だけが欠けた / 短い状態が外から作れる (部分 copy、 rsync 中断)。
    pub fn load(region: Region, max_entities: u32) -> std::io::Result<Self> {
        let mm = region.slice();
        if mm.len() < HEADER || mm[0..4] != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "entity set region is corrupt or truncated (len {}, expected magic at 0)",
                    mm.len()
                ),
            ));
        }
        let stored = u32::from_le_bytes(mm[12..16].try_into().unwrap());
        let bitset_cap = if stored == 0 { max_entities } else { stored };
        if bitset_cap < max_entities {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "entity set bitset_cap {bitset_cap} < max_entities {max_entities} — corrupt header?"
                ),
            ));
        }
        Ok(Self {
            region,
            max_entities: AtomicU32::new(max_entities),
            bitset_cap,
            bitset_offset: HEADER,
            free_offset: Self::free_offset_for(bitset_cap),
            free_lock: std::sync::Mutex::new(()),
        })
    }

    /// bitset が持てる entity 数 (= `grow` の上限)。
    pub fn bitset_cap(&self) -> u32 {
        self.bitset_cap
    }

    /// 上限を `new_max` に伸ばす (縮めない)。 `bitset_cap` を超えると Err。 offset は
    /// 動かないので、 既存の bit / free stack はそのまま。
    pub fn grow(&self, new_max: u32) -> Result<(), String> {
        if new_max > self.bitset_cap {
            return Err(format!(
                "entity cap {new_max} exceeds the reservation {} made at create", self.bitset_cap
            ));
        }
        self.max_entities.fetch_max(new_max, Ordering::AcqRel);
        Ok(())
    }

    fn next_eid_atomic(&self) -> &AtomicU32 {
        let mm = self.region.slice();
        unsafe { &*(mm.as_ptr().add(4) as *const AtomicU32) }
    }

    fn live_count_atomic(&self) -> &AtomicU32 {
        let mm = self.region.slice();
        unsafe { &*(mm.as_ptr().add(8) as *const AtomicU32) }
    }

    /// 現在の entity 上限 (exclusive)。
    #[inline]
    pub fn max_entities(&self) -> u32 {
        self.max_entities.load(Ordering::Relaxed)
    }

    /// slot を払い出す。 **満杯 (max_entities 到達 + free stack 空) なら `None`**。
    ///
    /// #59: 旧実装は `assert!` で panic していた。 embedded DB は他人の process に
    /// 埋め込まれるので、 「DB が一杯」 という想定内事象で host を殺してはいけない。
    pub fn allocate(&self) -> Option<u32> {
        self.allocate_tracked().map(|(eid, _)| eid)
    }

    /// `allocate` + 「その slot が free stack からの **再利用**か」。
    ///
    /// 再利用 slot は前の住人が書いた per-cell 版数 (v9 の version / tombstone
    /// column) を持ち得るので、 払い出す側が落とす必要がある
    /// (`Engine::clear_cell_versions`)。 monotonic 払い出し (= 新品 slot) では
    /// 消す物が無いので、 hot path に消去コストを持ち込まないための戻り値。
    pub fn allocate_tracked(&self) -> Option<(u32, bool)> {
        // 高速パス: monotonic increment（欠番方式、ロックフリー）
        let eid = self.next_eid_atomic().fetch_add(1, Ordering::Relaxed);
        if eid < self.max_entities() {
            self.set_bit(eid, true);
            self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
            // issue6 (perf 退化対策): writer hot path から mark_dirty を撤廃。
            // EntitySet 全領域は body_msync 内で常時 msync される (固定サイズ
            // で cheap な小領域)。
            return Some((eid, false));
        }
        // 上限到達 → fetch_addを巻き戻してfree stackから再利用
        self.next_eid_atomic().fetch_sub(1, Ordering::Relaxed);
        self.allocate_from_free_stack().map(|eid| (eid, true))
    }

    /// free stack から pop。上限到達時のみ呼ばれる。
    /// #77-H7: `free_lock` で push と直列化 (旧 CAS pop は push 側の
    /// 「slot 書き → count 書き」非 atomic 複合と混ざると、 eid 未書き込みの
    /// slot を読み得た)。
    fn allocate_from_free_stack(&self) -> Option<u32> {
        let _g = self.free_lock.lock().unwrap_or_else(|p| p.into_inner());
        let mm = self.region.slice();
        let free_count_off = self.free_offset;
        let fc = u32::from_le_bytes(mm[free_count_off..free_count_off + 4].try_into().unwrap());
        // #59: 満杯は 「想定内だが続行不能」。 panic せず None を返し、 呼び出し側
        // (Engine) が fault として記録 + 報告する。
        if fc == 0 {
            return None;
        }
        let eid_off = self.free_offset + 4 + ((fc - 1) as usize) * 4;
        let eid = u32::from_le_bytes(mm[eid_off..eid_off + 4].try_into().unwrap());
        self.region.write_at(free_count_off, &(fc - 1).to_le_bytes());
        self.set_bit(eid, true);
        self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        // EntitySet 全領域は body_msync 内で常時 msync (issue6 perf 対策)。
        Some(eid)
    }

    pub fn free(&self, eid: u32) {
        // #77-H7: was-live 判定を bit の atomic 遷移そのもので行う。
        // 旧実装の is_live → set_bit の 2 段は並行 free が両方通過し、
        // free stack への二重 push → 同一 eid の二重払い出しに繋がった。
        let was_live = self.set_bit(eid, false);
        if !was_live { return; }
        self.live_count_atomic().fetch_sub(1, Ordering::Relaxed);

        let _g = self.free_lock.lock().unwrap_or_else(|p| p.into_inner());
        let free_count_off = self.free_offset;
        let fc = u32::from_le_bytes(
            self.region.slice()[free_count_off..free_count_off + 4].try_into().unwrap(),
        );
        let free_cap = Self::free_cap_for(self.bitset_cap);
        if fc < free_cap {
            let eid_off = self.free_offset + 4 + (fc as usize) * 4;
            // v10: free stack は lazy commit。 失敗 (ENOSPC) なら積まずに欠番にする (安全側)。
            if self.region.ensure_committed(eid_off + 4).is_err() {
                return;
            }
            self.region.write_at(eid_off, &eid.to_le_bytes());
            self.region.write_at(free_count_off, &(fc + 1).to_le_bytes());
        }
        // EntitySet 全領域は body_msync 内で常時 msync (issue6 perf 対策)。
    }

    /// rollback用: 削除されたentityを復活させる。
    pub fn revive(&self, eid: u32) {
        if eid >= self.max_entities() { return; }
        // bit 遷移で was-live を判定 (並行 revive の二重 count 防止)
        if !self.set_bit(eid, true) {
            self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        }
    }

    /// リモート peer から届いた eid を「存在する」ことにする。
    /// 既に live なら no-op。next_eid / live_count / live bitmap を整合的に更新する。
    pub fn ensure_live(&self, eid: u32) {
        if eid >= self.max_entities() { return; }
        if self.set_bit(eid, true) { return; } // 既に live
        self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        // next_eid は「これまで allocate した最大 +1」の概念。local を超えていたら進める。
        let cur = self.next_eid_atomic().load(Ordering::Acquire);
        if eid >= cur {
            self.next_eid_atomic().store(eid + 1, Ordering::Release);
        }
    }

    #[inline]
    pub fn is_live(&self, eid: u32) -> bool {
        if eid >= self.max_entities() { return false; }
        let mm = self.region.slice();
        let byte_off = self.bitset_offset + (eid / 8) as usize;
        let bit = 1u8 << (eid % 8);
        (mm[byte_off] & bit) != 0
    }

    /// #77-H7: bit を atomic に立てる/落とす。 変更前にその bit が立っていたか
    /// を返す。 旧実装の `mm[byte_off] |= bit` は非 atomic RMW で、 隣接 eid を
    /// 払い出された並行スレッドと同一バイトを書き合うと片方の bit が消えた。
    fn set_bit(&self, eid: u32, live: bool) -> bool {
        if eid >= self.max_entities() { return false; }
        let byte_off = self.bitset_offset + (eid / 8) as usize;
        // v10: bitset は lazy commit。 未 commit の page は読むと 0 = 全部 dead なので、 消す
        // 側 (live = false) は触らなくてよい。 立てる側の失敗 (ENOSPC) は「立たなかった」
        // として false を返す (呼び出し側は live_count を進めない)。
        if !self.region.is_committed(byte_off + 1) {
            if !live {
                return false;
            }
            if self.region.ensure_committed(byte_off + 1).is_err() {
                return false;
            }
        }
        let mm = self.region.slice();
        let byte = unsafe {
            &*(mm.as_ptr().add(byte_off) as *const std::sync::atomic::AtomicU8)
        };
        let bit = 1u8 << (eid % 8);
        let prev = if live {
            byte.fetch_or(bit, Ordering::AcqRel)
        } else {
            byte.fetch_and(!bit, Ordering::AcqRel)
        };
        (prev & bit) != 0
        // EntitySet 全領域は body_msync 内で常時 msync (issue6 perf 対策)。
    }

    pub fn count(&self) -> u32 {
        self.live_count_atomic().load(Ordering::Relaxed)
    }

    /// `[lo, hi)` 範囲の live 数。 table 単位の枠使用量 (`Engine::table_eid_usage`)
    /// を出すのに使う。 bitset を byte 単位で popcount するので 1M entity でも
    /// 128KB の線形走査で済む (呼ばれるのは診断時のみで hot path 外)。
    pub fn live_count_in(&self, lo: u32, hi: u32) -> u32 {
        let hi = hi.min(self.max_entities());
        if lo >= hi {
            return 0;
        }
        let mm = self.region.slice();
        let mut n = 0u32;
        // 端の byte は bit 単位、 中間は byte 単位で数える。
        let first_full = (lo + 7) & !7;
        let last_full = hi & !7;
        if first_full >= hi {
            for e in lo..hi {
                if mm[self.bitset_offset + (e / 8) as usize] & (1u8 << (e % 8)) != 0 {
                    n += 1;
                }
            }
            return n;
        }
        for e in lo..first_full {
            if mm[self.bitset_offset + (e / 8) as usize] & (1u8 << (e % 8)) != 0 {
                n += 1;
            }
        }
        for b in (first_full / 8)..(last_full / 8) {
            n += mm[self.bitset_offset + b as usize].count_ones();
        }
        for e in last_full..hi {
            if mm[self.bitset_offset + (e / 8) as usize] & (1u8 << (e % 8)) != 0 {
                n += 1;
            }
        }
        n
    }

    /// #117: `[lo, hi)` 範囲内で最上位の live eid を返す (無ければ None)。
    /// open 時に per-table `next_local` を live bitmap から自己修復するために使う。
    /// bitset を上から逆走査し、 空 byte は byte 単位で skip するので append-mostly
    /// なら top 数 byte で早期 exit する。 bitset は body に mmap 永続され `flush` で
    /// msync 済 = sidecar (next_local) が失われても ground truth はここに残る。
    pub fn highest_live_in(&self, lo: u32, hi: u32) -> Option<u32> {
        let hi = hi.min(self.max_entities());
        if lo >= hi {
            return None;
        }
        let mm = self.region.slice();
        let mut e = hi; // exclusive 上端
        while e > lo {
            e -= 1;
            let byte = mm[self.bitset_offset + (e / 8) as usize];
            if byte == 0 {
                // この byte に live 無し → byte 先頭へ飛ばす (次周回の e-=1 で前 byte 最上位へ)。
                let byte_first = e & !7;
                if byte_first <= lo {
                    break;
                }
                e = byte_first;
                continue;
            }
            if byte & (1u8 << (e % 8)) != 0 {
                return Some(e);
            }
        }
        None
    }

    pub fn next_eid(&self) -> u32 {
        self.next_eid_atomic().load(Ordering::Relaxed)
    }

    /// 指定 eid を live としてマークし、 必要なら global next_eid を `eid + 1`
    /// まで進める。 β-light の table-aware allocation 用 (`entity_in`)。
    ///
    /// 並行性: CAS で next_eid を進めるため、 同時に走る `allocate()` とは
    /// 安全に共存できる (重複 eid を返さない)。 ただし呼び出し側で eid 範囲を
    /// 分離する責務を持つ — 例えば table A の range が [0, 1M), B の range が
    /// [1M, 2M) のように互いに disjoint であれば、 並行 allocate_at は安全。
    pub fn allocate_at(&self, eid: u32) {
        assert!(
            eid < self.max_entities(),
            "allocate_at: eid {} exceeds max_entities {}",
            eid, self.max_entities(),
        );
        if !self.set_bit(eid, true) {
            self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        }
        // next_eid を max(current, eid + 1) まで進める (CAS で torn write 防止)
        let mut cur = self.next_eid_atomic().load(Ordering::Relaxed);
        while cur <= eid {
            match self.next_eid_atomic().compare_exchange_weak(
                cur,
                eid + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    pub fn iter(&self) -> Vec<u32> {
        let mm = self.region.slice();
        let next = self.next_eid();
        let mut result = Vec::with_capacity(self.count() as usize);
        for eid in 0..next {
            let byte_off = self.bitset_offset + (eid / 8) as usize;
            let bit = 1u8 << (eid % 8);
            if (mm[byte_off] & bit) != 0 {
                result.push(eid);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_set(max_entities: u32) -> Arc<EntitySet> {
        let size = EntitySet::region_size(max_entities);
        let buf: Box<[u8]> = vec![0u8; size].into_boxed_slice();
        let ptr = Box::leak(buf).as_mut_ptr();
        let region = unsafe { Region::new(ptr, size) };
        Arc::new(EntitySet::init(region, max_entities, max_entities))
    }

    /// #77-H7 regression: 並行 allocate で隣接 eid の live bit が消えないこと。
    /// 旧実装は `mm[byte] |= bit` の非 atomic RMW だったため、同一バイトを
    /// 共有する eid (7/8 の確率) の bit が並行書きで消失した。
    #[test]
    fn concurrent_allocate_no_lost_bits() {
        let set = make_set(100_000);
        const THREADS: usize = 8;
        const PER: usize = 500;
        let handles: Vec<_> = (0..THREADS).map(|_| {
            let s = set.clone();
            std::thread::spawn(move || {
                (0..PER)
                    .map(|_| s.allocate().expect("枠は足りているはず"))
                    .collect::<Vec<u32>>()
            })
        }).collect();
        let mut all: Vec<u32> = handles.into_iter()
            .flat_map(|h| h.join().unwrap()).collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), THREADS * PER, "eid が重複払い出しされた");
        for &eid in &all {
            assert!(set.is_live(eid), "eid {eid} の live bit が消失");
        }
        assert_eq!(set.count(), (THREADS * PER) as u32);
        assert_eq!(set.iter().len(), THREADS * PER);
    }

    /// #77-H7 regression: 同一 eid の並行 free が free stack に二重 push
    /// しないこと。旧実装は is_live → set_bit の 2 段で両者が通過し、
    /// 飽和後の allocate が同一 eid を二重払い出しした。
    #[test]
    fn concurrent_double_free_no_double_push() {
        let set = make_set(4);
        for _ in 0..4 { assert!(set.allocate().is_some()); } // 0..3 で飽和
        let handles: Vec<_> = (0..8).map(|_| {
            let s = set.clone();
            std::thread::spawn(move || s.free(2))
        }).collect();
        for h in handles { h.join().unwrap(); }
        assert_eq!(set.count(), 3, "live_count が二重減算された");

        // free stack には 2 が 1 回だけ積まれているはず
        assert_eq!(set.allocate(), Some(2), "stack から 2 が出るはず");
        // #59: stack 空 + 飽和は panic ではなく None (embedded DB は host を殺さない)。
        assert_eq!(
            set.allocate(),
            None,
            "二重 push された 2 が再払い出しされた (満杯なら None であるべき)"
        );
    }
}
