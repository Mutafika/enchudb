//! `LeafStore` — Leaf 終端ノードの可変長 payload を格納する **単一所有・reclaim
//! 対応** の value store。 #88 / [[request11]]。
//!
//! # 位置づけ
//!
//! Tag は共有 dedup 辞書 (`Vocabulary`) が本来の用途だが、 Leaf は「先 (outgoing
//! edge) の無い終端ノード」で `vocab.insert` (dedup 無し・単一所有) を使うだけ =
//! 共有辞書性を使っていない。 それが append-only-never-reclaim の vocab に乗ると、
//! delete しても回収されず単調増加する (wikipulse の 512MB 壁)。 本 store は Leaf を
//! vocab から剥がし、 delete/untie で **free-list に返して再利用** する。
//!
//! # 設計 (reclaim ≠ compaction)
//!
//! - **reclaim (footprint 有界化)**: 死んだ slot を free-list に積み、 次の insert は
//!   空きに緩く置く (best-fit)。 隣接空きは coalesce。 末尾に達した空きは high_water を
//!   後退させて自由空間へ戻す。 **live は一切動かさない。**
//! - **compaction は本 store の責務外**: live を動かして file を OS へ縮めるのは
//!   別レイヤの rare 保険 (steady churn では不要)。
//!
//! # レイアウト (自己記述的 = 領域を walk できる)
//!
//! ```text
//! [MAGIC "LEF1": 4B][high_water: AtomicU32 LE @4][reserved: 4B @8]
//! slot (live/free 問わず): [slot_size: u32 LE][len: u32 LE][bytes ... padding]
//!   - slot_size = この slot が占める総 byte (header 8B 込み、 4B aligned)
//!   - len       = 実 payload 長 (<= slot_size - 8)。 free slot では未使用
//!   - cell (himo) が持つ参照 = slot 先頭 offset (u32)
//! ```
//!
//! 全 slot が先頭に `slot_size` を持つので、 `HEADER..high_water` を slot_size 刻みで
//! walk できる。 これにより reopen 時に「live cell 集合」を渡すだけで free-list を
//! rebuild できる (free-list 自体は永続化しない = store の派生)。

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use crate::region::Region;

const MAGIC: [u8; 4] = [b'L', b'E', b'F', b'1'];
/// header: MAGIC(4) + high_water(4) + reserved(4)
const HEADER: usize = 12;
const HIGH_WATER_OFF: usize = 4;
/// slot 先頭の [slot_size:4][len:4]
const SLOT_HEADER: usize = 8;
/// 最小 slot (空 payload = header のみ)。 これ未満の余りは hole にできない。
const MIN_SLOT: usize = SLOT_HEADER;

#[inline]
fn align4(x: usize) -> usize { (x + 3) & !3 }

/// Leaf payload の store。 単一所有・reclaim 対応。
pub struct LeafStore {
    region: Region,
    /// free hole: offset -> slot_size。 offset 昇順 (coalesce に必須)。
    /// **永続化しない** — reopen 時は `rebuild_free_list` で live cell から再構成。
    /// insert/free/rebuild を直列化する (writer は `.db.lock` で 1 プロセスだが
    /// in-process では複数スレッドが consumer 経由で触りうる)。
    holes: Mutex<BTreeMap<u32, u32>>,
}

// Region は raw ptr を持つため。 vocab / entity_set と同じ扱い (writer 排他は
// `.db.lock` + in-process の `holes` Mutex で担保)。
unsafe impl Sync for LeafStore {}
unsafe impl Send for LeafStore {}

impl LeafStore {
    /// 新規領域を初期化。 high_water = HEADER。
    pub fn init(region: Region) -> Self {
        let _ = region.ensure_committed(HEADER);
        let mm = region.slice_mut();
        mm[0..4].copy_from_slice(&MAGIC);
        region.as_atomic_u32(HIGH_WATER_OFF).store(HEADER as u32, Ordering::Relaxed);
        Self { region, holes: Mutex::new(BTreeMap::new()) }
    }

    /// 既存領域をロード。 free-list は空 (= 全空間を使用中扱い)。 呼び出し側が
    /// `rebuild_free_list(live)` を呼ぶまで reclaim は効かない (fresh append のみ)。
    pub fn load(region: Region) -> Self {
        let mm = region.slice();
        assert!(mm.len() >= HEADER && mm[0..4] == MAGIC, "bad leaf store magic");
        Self { region, holes: Mutex::new(BTreeMap::new()) }
    }

    #[inline]
    fn high_water_atomic(&self) -> &AtomicU32 {
        self.region.as_atomic_u32(HIGH_WATER_OFF)
    }

    /// 現在の footprint (= 使用領域の上端)。
    pub fn high_water(&self) -> u32 {
        self.high_water_atomic().load(Ordering::Relaxed)
    }

    #[inline]
    fn slot_size_at(&self, off: u32) -> u32 {
        let mm = self.region.slice();
        let o = off as usize;
        u32::from_le_bytes(mm[o..o + 4].try_into().unwrap())
    }

    /// slot 先頭 offset から payload を読む。
    pub fn get(&self, off: u32) -> &[u8] {
        let mm = self.region.slice();
        let o = off as usize;
        let len = u32::from_le_bytes(mm[o + 4..o + 8].try_into().unwrap()) as usize;
        &mm[o + SLOT_HEADER..o + SLOT_HEADER + len]
    }

    /// payload を格納し slot 先頭 offset を返す。 free-list に嵌まる空きがあれば
    /// そこへ、 無ければ high_water を伸ばす。
    pub fn insert(&self, bytes: &[u8]) -> u32 {
        let blen = bytes.len();
        assert!(blen <= (u32::MAX as usize - SLOT_HEADER), "leaf value too large");
        let need = align4(SLOT_HEADER + blen);

        let mut holes = self.holes.lock().unwrap_or_else(|p| p.into_inner());
        let (off, slot_size) = self.alloc_locked(&mut holes, need);
        drop(holes);

        let _ = self.region.ensure_committed(off as usize + slot_size);
        let mm = self.region.slice_mut();
        let o = off as usize;
        mm[o..o + 4].copy_from_slice(&(slot_size as u32).to_le_bytes());
        mm[o + 4..o + 8].copy_from_slice(&(blen as u32).to_le_bytes());
        mm[o + SLOT_HEADER..o + SLOT_HEADER + blen].copy_from_slice(bytes);
        // header 再書き込みを保証 (fresh region の magic は init 済みだが、
        // high_water 更新を dirty hint に載せる)
        self.region.mark_dirty(o, slot_size);
        self.region.mark_dirty(HIGH_WATER_OFF, 4);
        off
    }

    /// `need` (4B aligned, header 込み) を確保。 返り値 = (offset, 実 slot_size)。
    /// best-fit は O(hole 数) の線形走査 (Phase 1)。 hole が定常で少なければ十分、
    /// 多数化するなら size-index 併設は後段の最適化。
    fn alloc_locked(&self, holes: &mut BTreeMap<u32, u32>, need: usize) -> (u32, usize) {
        // best-fit: need 以上で最小の hole
        let best = holes
            .iter()
            .filter(|(_, sz)| **sz as usize >= need)
            .min_by_key(|(_, sz)| **sz)
            .map(|(&off, &sz)| (off, sz as usize));

        if let Some((hoff, hsize)) = best {
            holes.remove(&hoff);
            let remainder = hsize - need;
            if remainder >= MIN_SLOT {
                // split: 余りを hole として登録 (header を書いて walkable に)
                let roff = hoff + need as u32;
                let mm = self.region.slice_mut();
                mm[roff as usize..roff as usize + 4]
                    .copy_from_slice(&(remainder as u32).to_le_bytes());
                holes.insert(roff, remainder as u32);
                (hoff, need)
            } else {
                // 余りが hole にならない → 丸ごと払い出し (内部断片 < MIN_SLOT)
                (hoff, hsize)
            }
        } else {
            // fresh: high_water を伸ばす
            let hw = self.high_water_atomic().load(Ordering::Relaxed);
            let end = hw as usize + need;
            let _ = self.region.ensure_committed(end);
            self.high_water_atomic().store(end as u32, Ordering::Relaxed);
            (hw, need)
        }
    }

    /// slot を解放。 隣接する free hole と coalesce し、 末尾なら high_water を後退。
    pub fn free(&self, off: u32) {
        let mut hsize = self.slot_size_at(off);
        let mut hoff = off;
        let mut holes = self.holes.lock().unwrap_or_else(|p| p.into_inner());

        // 直前の隣接 hole と結合
        if let Some((poff, psize)) = holes.range(..hoff).next_back().map(|(&k, &v)| (k, v))
            && poff + psize == hoff
        {
            holes.remove(&poff);
            hoff = poff;
            hsize += psize;
        }
        // 直後の隣接 hole と結合
        let end = hoff + hsize;
        if let Some(&nsize) = holes.get(&end) {
            holes.remove(&end);
            hsize += nsize;
        }

        let hw = self.high_water_atomic().load(Ordering::Relaxed);
        if hoff + hsize == hw {
            // 末尾に達した → 自由空間へ返す (footprint 後退)
            self.high_water_atomic().store(hoff, Ordering::Relaxed);
            self.region.mark_dirty(HIGH_WATER_OFF, 4);
        } else {
            // merged header を書いて walkable 維持 + free-list 登録
            let mm = self.region.slice_mut();
            mm[hoff as usize..hoff as usize + 4].copy_from_slice(&hsize.to_le_bytes());
            self.region.mark_dirty(hoff as usize, 4);
            holes.insert(hoff, hsize);
        }
    }

    /// reopen 用: live な slot offset 集合を渡し、 領域を walk して free-list を
    /// 再構成する。 free-list は永続化しないので open 時に必ず呼ぶ (呼ばないと
    /// 死んだ slot が再利用されず fresh append のみになる)。
    pub fn rebuild_free_list(&self, live: &HashSet<u32>) {
        let hw = self.high_water_atomic().load(Ordering::Relaxed);
        let mut holes = self.holes.lock().unwrap_or_else(|p| p.into_inner());
        holes.clear();

        // 連続する free run を溜めて coalesce する
        let mut pending: Option<(u32, u32)> = None; // (offset, size)
        let mut off = HEADER as u32;
        while off < hw {
            let ss = self.slot_size_at(off);
            assert!(
                ss as usize >= MIN_SLOT && off as u64 + ss as u64 <= hw as u64,
                "leaf store corrupt: slot_size {} at off {} (hw {})", ss, off, hw
            );
            if live.contains(&off) {
                if let Some((poff, psize)) = pending.take() {
                    self.flush_run(&mut holes, poff, psize, hw);
                }
            } else {
                pending = match pending {
                    Some((poff, psize)) if poff + psize == off => Some((poff, psize + ss)),
                    Some((poff, psize)) => {
                        self.flush_run(&mut holes, poff, psize, hw);
                        Some((off, ss))
                    }
                    None => Some((off, ss)),
                };
            }
            off += ss;
        }
        if let Some((poff, psize)) = pending.take() {
            self.flush_run(&mut holes, poff, psize, hw);
        }
    }

    /// rebuild 中: coalesce 済み free run を確定。 末尾なら high_water 後退。
    fn flush_run(&self, holes: &mut BTreeMap<u32, u32>, off: u32, size: u32, hw: u32) {
        if off + size == hw {
            self.high_water_atomic().store(off, Ordering::Relaxed);
        } else {
            let mm = self.region.slice_mut();
            mm[off as usize..off as usize + 4].copy_from_slice(&size.to_le_bytes());
            holes.insert(off, size);
        }
    }

    /// free-list が抱える回収可能 byte 数 (test / stats 用)。
    pub fn free_bytes(&self) -> u64 {
        let holes = self.holes.lock().unwrap_or_else(|p| p.into_inner());
        holes.values().map(|&s| s as u64).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// heap buffer 上に static backing の Region を作る (grower 無し = 全 commit 済み)。
    fn make_store(bytes: usize) -> LeafStore {
        let buf: Box<[u8]> = vec![0u8; bytes].into_boxed_slice();
        let ptr = Box::leak(buf).as_mut_ptr();
        let region = unsafe { Region::new(ptr, bytes) };
        LeafStore::init(region)
    }

    #[test]
    fn round_trip() {
        let s = make_store(64 * 1024);
        let a = s.insert(b"hello");
        let b = s.insert(b"");
        let c = s.insert("終端ノード".as_bytes());
        assert_eq!(s.get(a), b"hello");
        assert_eq!(s.get(b), b"");
        assert_eq!(s.get(c), "終端ノード".as_bytes());
    }

    #[test]
    fn free_reuses_hole_no_growth() {
        let s = make_store(64 * 1024);
        let _keep = s.insert(b"AAAA"); // 末尾後退を避けるための番人
        let a = s.insert(b"0123456789"); // 10B
        let hw_after_a = s.high_water();
        s.free(a);
        // 同サイズを入れ直す → 同じ穴を再利用、 high_water は伸びない
        let b = s.insert(b"9876543210");
        assert_eq!(s.high_water(), hw_after_a, "hole 再利用されず footprint が伸びた");
        assert_eq!(s.get(b), b"9876543210");
    }

    #[test]
    fn coalesce_adjacent_holes() {
        let s = make_store(64 * 1024);
        let _keep = s.insert(b"K");
        let a = s.insert(b"aaaa"); // 隣接 3 件
        let b = s.insert(b"bbbb");
        let c = s.insert(b"cccc");
        let hw = s.high_water();
        // a, b を解放 (隣接) → 1 つの大きい hole に coalesce
        s.free(a);
        s.free(b);
        // a+b の合計に収まる大きめ値が、 high_water を伸ばさず入る
        let big = s.insert(b"aaaabbbb_pad"); // a slot + b slot 分に収まる
        assert_eq!(s.high_water(), hw, "coalesce した hole に収まらず footprint が伸びた");
        assert_eq!(s.get(big), b"aaaabbbb_pad");
        // c は無傷
        assert_eq!(s.get(c), b"cccc");
    }

    #[test]
    fn tail_free_retracts_high_water() {
        let s = make_store(64 * 1024);
        let base = s.high_water();
        let a = s.insert(b"trailing");
        assert!(s.high_water() > base);
        s.free(a); // 末尾なので high_water が戻る
        assert_eq!(s.high_water(), base, "末尾解放で footprint が後退しなかった");
    }

    /// 本命: churn 下で footprint が有界 ⇔ reclaim (free) がそれを効かせている、
    /// を falsify 付きで示す。 free 無し (append のみ) だと footprint は round 数に
    /// 比例して増える = 「free を止めれば growth が再現する」ことを先に見せる。
    #[test]
    fn churn_footprint_bounded_and_falsified() {
        const ROUNDS: usize = 50;
        const PER: usize = 100;
        let val = vec![b'x'; 40];

        // (A) falsify: free しない → footprint は単調増加
        let no_reclaim = make_store(4 * 1024 * 1024);
        for _ in 0..ROUNDS {
            for _ in 0..PER {
                no_reclaim.insert(&val);
            }
        }
        let grow = no_reclaim.high_water();

        // (B) 本番: 各 round で全 free → footprint は 1 round 分に有界
        let reclaim = make_store(4 * 1024 * 1024);
        for _ in 0..ROUNDS {
            let refs: Vec<u32> = (0..PER).map(|_| reclaim.insert(&val)).collect();
            for r in refs {
                reclaim.free(r);
            }
        }
        let bounded = reclaim.high_water();

        // falsify: reclaim を止めた (A) は round 数倍に膨れる
        assert!(
            grow as f64 > bounded as f64 * (ROUNDS as f64 / 4.0),
            "free 無効時に growth が再現しない (A={grow}, B={bounded})",
        );
        // 本番: footprint は ~1 round 分に収まる (working set の小定数倍)
        let one_round = (align4(SLOT_HEADER + val.len()) * PER) as u32;
        assert!(
            bounded <= one_round * 2 + HEADER as u32,
            "reclaim 下で footprint が有界化してない (B={bounded}, 1round≈{one_round})",
        );
    }

    #[test]
    fn rebuild_free_list_reconstructs_holes() {
        let s = make_store(64 * 1024);
        let a = s.insert(b"keep-a");
        let _b = s.insert(b"drop-b");
        let c = s.insert(b"keep-c");
        let _d = s.insert(b"drop-d");
        let hw = s.high_water();

        // b, d が dead という前提で reopen を模擬: free-list を捨てて live から rebuild
        let live: HashSet<u32> = [a, c].into_iter().collect();
        s.rebuild_free_list(&live);

        // b の穴が回収されている (free_bytes > 0)、 かつ b サイズが再利用可能
        assert!(s.free_bytes() > 0, "rebuild で dead slot が free-list に載っていない");
        // d は末尾なので high_water 後退 (hw より縮む)
        assert!(s.high_water() < hw, "末尾 dead slot が rebuild で後退していない");

        // live は無傷で読める
        assert_eq!(s.get(a), b"keep-a");
        assert_eq!(s.get(c), b"keep-c");

        // b の穴に同サイズ (6B) を入れ直すと再利用される
        let hw2 = s.high_water();
        let e = s.insert(b"reuse!");
        assert_eq!(s.get(e), b"reuse!");
        assert_eq!(s.high_water(), hw2, "rebuild した hole が再利用されず伸びた");
    }
}
