//! Vocabulary — ユニーク値辞書。Symbol の値を value_id (u32) に変換。
//! 3つのRegion（data, offsets, index）で構成。

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const MAGIC: [u8; 4] = [b'V', b'O', b'C', b'1'];
const HEADER: usize = 16;
/// data header byte 12 に書く「index は data と consistent」 マーカー。
/// 0 = dirty (rebuild 要)、 1 = clean (rebuild skip 可)。
/// 残り 13-15 byte は reserved。
const CLEAN_FLAG_OFF: usize = 12;
const INDEX_MAGIC: [u8; 4] = [b'V', b'I', b'X', b'2'];
const INDEX_HEADER: usize = 16;
const INDEX_SLOT_SIZE: usize = 13;

pub struct Vocabulary {
    data: Region,
    offsets: Region,
    index: Region,
    /// #77-H1: readonly open で index が dirty (clean_flag≠1) な場合、 共有
    /// mmap の index を書き換えずに **heap 上のここへ**再構築する。 Some の
    /// とき lookup はこちらを参照。 writer open では常に None。
    /// 旧実装は readonly でも `rebuild_index` が共有 index をゼロクリアして
    /// おり、 併走中の writer の index entry を恒久消失させていた。
    shadow_index: Option<Box<[u8]>>,
    /// #77-M1: disk 上の clean_flag のキャッシュ。 flush() が 1 を書いた後の
    /// 最初の insert で 0 に戻すための判定に使う (旧実装は open 時の 1 回
    /// しか 0 に倒さず、 flush 後の追加 write 中 crash で破損 index を
    /// 次 open が無検証採用した)。
    clean_on_disk: std::sync::atomic::AtomicBool,
    count: AtomicU32,
    data_end: AtomicU32,
    max_entries: u32,
    index_cap: u32,
}

unsafe impl Sync for Vocabulary {}
unsafe impl Send for Vocabulary {}

impl Vocabulary {
    pub fn data_region_size(data_size: usize) -> usize { data_size.max(HEADER) }
    pub fn offsets_region_size(max_entries: u32) -> usize { (max_entries as usize) * 8 }
    pub fn index_region_size(index_cap: u32) -> usize { INDEX_HEADER + (index_cap as usize) * INDEX_SLOT_SIZE }

    /// 新規領域を初期化。
    ///
    /// `data` 領域 (variable cluster、 v3 layout で末尾) の MAGIC + count
    /// + data_end は **書かない** — `insert` の初回 append で初めて書く
    /// (lazy init)。 これで growable backing の `initial_commit` が
    /// data 領域の末尾までコミットしなくて済むようになる (Phase B Step 2)。
    /// `index` 領域 (固定 cluster) は eager init のまま — 固定上限なので
    /// initial_commit に含めるオーバーヘッドが小さい。
    pub fn init(data: Region, offsets: Region, index: Region, max_entries: u32, index_cap: u32) -> Self {
        let index_cap = index_cap.next_power_of_two();

        // index header — eager init は固定 cluster なのでコスト低
        let xm = index.slice_mut();
        xm[0..4].copy_from_slice(&INDEX_MAGIC);
        xm[4..8].copy_from_slice(&index_cap.to_le_bytes());

        Self {
            data, offsets, index,
            shadow_index: None,
            clean_on_disk: std::sync::atomic::AtomicBool::new(false),
            count: AtomicU32::new(0),
            data_end: AtomicU32::new(HEADER as u32),
            max_entries, index_cap,
        }
    }

    /// 既存領域をロード。ハッシュインデックスを再構築する。
    ///
    /// data の先頭 4 バイトが MAGIC でない (= 全 0 = lazy fresh、 一度も
    /// insert されてない) 場合は count=0 / data_end=HEADER の fresh
    /// state を返す。 これで `insert` が遅延書き込みする MAGIC を待たずに
    /// open できる。
    pub fn load(data: Region, offsets: Region, index: Region, readonly: bool) -> Self {
        let dm = data.slice();
        let is_fresh = dm[0..4] != MAGIC;
        let (count, data_end, clean_flag) = if is_fresh {
            (0u32, HEADER as u32, 0u32)
        } else {
            (
                u32::from_le_bytes(dm[4..8].try_into().unwrap()),
                u32::from_le_bytes(dm[8..12].try_into().unwrap()),
                u32::from_le_bytes(dm[CLEAN_FLAG_OFF..CLEAN_FLAG_OFF + 4].try_into().unwrap()),
            )
        };

        let om = offsets.slice();
        let max_entries = (om.len() / 8) as u32;

        let xm = index.slice();
        let index_cap = u32::from_le_bytes(xm[4..8].try_into().unwrap());

        let mut v = Self {
            data, offsets, index,
            shadow_index: None,
            clean_on_disk: std::sync::atomic::AtomicBool::new(clean_flag == 1),
            count: AtomicU32::new(count),
            data_end: AtomicU32::new(data_end),
            max_entries, index_cap,
        };
        // clean_flag == 1 なら前回 graceful close で「index と data は consistent」と
        // 保証されているので rebuild skip。 それ以外 (= 初期 / crash 後 / 未対応の旧 DB) は
        // 全部 rebuild。
        // #77-H1: readonly open は共有 mmap を書き換えず heap の shadow へ。
        if !is_fresh && clean_flag != 1 {
            if readonly {
                let size = Self::index_region_size(v.index_cap);
                let mut shadow = vec![0u8; size].into_boxed_slice();
                shadow[0..4].copy_from_slice(&INDEX_MAGIC);
                shadow[4..8].copy_from_slice(&v.index_cap.to_le_bytes());
                Self::rebuild_index_into(&v, &mut shadow);
                v.shadow_index = Some(shadow);
            } else {
                v.rebuild_index();
            }
        }
        v
    }

    /// ハッシュインデックスを data/offsets から再構築する (writer 専用、
    /// 共有 mmap 上の index を in-place で書き直す)。
    fn rebuild_index(&self) {
        Self::rebuild_index_into(self, self.index.slice_mut());
    }

    /// `xm` (index layout のバイト列、 mmap でも heap でも可) へ index を
    /// 再構築する。 open 時の単一スレッド実行前提なので atomic は使わない。
    fn rebuild_index_into(v: &Self, xm: &mut [u8]) {
        let count = v.count.load(Ordering::Relaxed);
        if count == 0 { return; }

        // インデックス領域をゼロクリア（ヘッダは保持）
        for b in &mut xm[INDEX_HEADER..] { *b = 0; }

        let mask = (v.index_cap - 1) as u64;
        for id in 0..count {
            let value = v.get(id);
            let h = fxhash(value);
            let mut idx = (h & mask) as usize;
            loop {
                let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
                if xm[off] == 0 {
                    xm[off] = 1;
                    xm[off + 1..off + 9].copy_from_slice(&h.to_le_bytes());
                    xm[off + 9..off + 13].copy_from_slice(&id.to_le_bytes());
                    break;
                }
                let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
                if slot_hash == h {
                    let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                    if v.get(vid) == value { break; } // Leaf 二重 append 等の真の重複
                }
                idx = ((idx as u64 + 1) & mask) as usize;
            }
        }
    }

    pub fn get_or_insert(&self, value: &[u8]) -> u32 {
        if let Some(id) = self.lookup(value) { return id; }
        let id = self.insert(value);
        // 並列挿入の競合チェック: 別スレッドが先に同じ値を挿入した場合、先着のidを使う
        if let Some(winner) = self.lookup(value) {
            if winner != id { return winner; }
        }
        id
    }

    #[inline]
    pub fn get(&self, id: u32) -> &[u8] {
        let om = self.offsets.slice();
        let off_pos = (id as usize) * 8;
        let offset = u32::from_le_bytes(om[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(om[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
        let dm = self.data.slice();
        &dm[offset..offset + len]
    }

    #[inline]
    pub fn lookup(&self, value: &[u8]) -> Option<u32> {
        let mask = (self.index_cap - 1) as u64;
        // #77-H1: readonly open で dirty だった場合は heap の shadow を参照
        let xm: &[u8] = match &self.shadow_index {
            Some(s) => s,
            None => self.index.slice(),
        };
        let h = fxhash(value);
        let mut idx = (h & mask) as usize;
        loop {
            let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
            if xm[off] == 0 { return None; }
            let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
            if slot_hash == h {
                let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                let stored = self.get(vid);
                if stored == value { return Some(vid); }
            }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
    }

    /// 重複検査なしで常に新規 id を発行して append する。
    /// `ValueType::Leaf` (終端タグ・dedupe なし) の書き込み path で使う。
    /// 既に同じ bytes が登録済みでも気にせず新 id を払い出す。index_insert は走るが、
    /// 既存スロットがあれば early-return するため index 領域は dedup される副作用がある
    /// (data/offsets のみ完全に増分)。
    pub fn insert(&self, value: &[u8]) -> u32 {
        let id = self.count.fetch_add(1, Ordering::Relaxed);
        assert!(id < self.max_entries, "vocabulary full");
        let len = value.len() as u32;
        let offset = self.data_end.fetch_add(len, Ordering::Relaxed);
        // Growable backing: extend the file-backed window before
        // writing past the current commit. No-op for static backings.
        // We also grow the offsets region in case `id` advanced
        // past its committed footprint (offsets have id × 8 layout).
        let _ = self
            .data
            .ensure_committed((offset + len) as usize);
        let _ = self
            .offsets
            .ensure_committed(((id as usize) + 1) * 8);
        let dm = self.data.slice_mut();
        dm[offset as usize..offset as usize + len as usize].copy_from_slice(value);
        dm[0..4].copy_from_slice(&MAGIC);
        let new_count = id + 1;
        let new_end = offset + len;
        dm[4..8].copy_from_slice(&new_count.to_le_bytes());
        dm[8..12].copy_from_slice(&new_end.to_le_bytes());
        self.data.mark_dirty(0, 12);
        // #77-M1: flush() が clean=1 を書いた後の最初の insert で 0 に戻す。
        // これが無いと flush 後の write 中 crash で、 次 open が部分 writeback
        // された index を rebuild なしで信用してしまう。
        if self.clean_on_disk.swap(false, Ordering::AcqRel) {
            dm[CLEAN_FLAG_OFF..CLEAN_FLAG_OFF + 4].copy_from_slice(&0u32.to_le_bytes());
            self.data.mark_dirty(CLEAN_FLAG_OFF, 4);
        }
        self.data.mark_dirty(offset as usize, len as usize);
        let om = self.offsets.slice_mut();
        let off_pos = (id as usize) * 8;
        om[off_pos..off_pos + 4].copy_from_slice(&offset.to_le_bytes());
        om[off_pos + 4..off_pos + 8].copy_from_slice(&len.to_le_bytes());
        self.offsets.mark_dirty(off_pos, 8);
        self.index_insert(value, id);
        id
    }

    fn index_insert(&self, value: &[u8], id: u32) {
        let mask = (self.index_cap - 1) as u64;
        let xm = self.index.slice_mut();
        let h = fxhash(value);
        let mut idx = (h & mask) as usize;
        loop {
            let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
            let flag = unsafe {
                &*(xm.as_ptr().add(off) as *const std::sync::atomic::AtomicU8)
            };
            let f = flag.load(Ordering::Acquire);
            if f == 0 {
                match flag.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Relaxed) {
                    Ok(_) => {
                        xm[off + 1..off + 9].copy_from_slice(&h.to_le_bytes());
                        xm[off + 9..off + 13].copy_from_slice(&id.to_le_bytes());
                        flag.store(1, Ordering::Release);
                        self.index.mark_dirty(off, INDEX_SLOT_SIZE);
                        return;
                    }
                    Err(_) => continue,
                }
            }
            if f == 2 {
                while flag.load(Ordering::Acquire) == 2 { std::hint::spin_loop(); }
                continue;
            }
            let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
            if slot_hash == h {
                // ハッシュ一致 → 実際の値を比較して本当に重複か確認
                let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                if self.get(vid) == value { return; } // 本当の重複
                // ハッシュ衝突 → linear probe 続行
            }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
    }

    pub fn count(&self) -> u32 { self.count.load(Ordering::Relaxed) }

    /// data 領域の append pointer (= 消費済み byte 数)。 単調増加・回収なし。
    /// #88 bench: Leaf を vocab に載せた場合の「回収されない footprint」計測用。
    pub fn data_footprint(&self) -> u32 { self.data_end.load(Ordering::Relaxed) }

    /// 内部状態をRegionヘッダに書き戻す（flushの前に呼ぶ）。
    pub fn sync(&self) {
        let dm = self.data.slice_mut();
        dm[4..8].copy_from_slice(&self.count.load(Ordering::Relaxed).to_le_bytes());
        dm[8..12].copy_from_slice(&self.data_end.load(Ordering::Relaxed).to_le_bytes());
    }

    /// index と data の整合性マーカーを書く。
    ///
    /// `clean = true`: 直前に全 msync が完了 → 次回 open で rebuild skip 可
    /// `clean = false`: insert が走った／crash 検知用 → 次回 open で rebuild 強制
    ///
    /// 自身では msync しない。 caller (Engine) が body_msync で永続化する責任を負う。
    pub fn mark_index_clean(&self, clean: bool) {
        // data 領域は variable cluster (lazy commit) なので、 先頭 header を
        // 確実に commit してから書く。
        let _ = self.data.ensure_committed(HEADER);
        let dm = self.data.slice_mut();
        let val: u32 = if clean { 1 } else { 0 };
        dm[CLEAN_FLAG_OFF..CLEAN_FLAG_OFF + 4].copy_from_slice(&val.to_le_bytes());
        self.clean_on_disk.store(clean, Ordering::Release); // #77-M1 キャッシュ追従
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Regions {
        data_ptr: *mut u8,
        offsets_ptr: *mut u8,
        index_ptr: *mut u8,
        data_len: usize,
        offsets_len: usize,
        index_len: usize,
    }

    fn make_regions(max_entries: u32, index_cap: u32, data_size: usize) -> Regions {
        let leak = |size: usize| -> *mut u8 {
            Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr()
        };
        Regions {
            data_ptr: leak(Vocabulary::data_region_size(data_size)),
            offsets_ptr: leak(Vocabulary::offsets_region_size(max_entries)),
            index_ptr: leak(Vocabulary::index_region_size(index_cap.next_power_of_two())),
            data_len: Vocabulary::data_region_size(data_size),
            offsets_len: Vocabulary::offsets_region_size(max_entries),
            index_len: Vocabulary::index_region_size(index_cap.next_power_of_two()),
        }
    }

    impl Regions {
        fn vocab_init(&self, max_entries: u32, index_cap: u32) -> Vocabulary {
            Vocabulary::init(
                unsafe { Region::new(self.data_ptr, self.data_len) },
                unsafe { Region::new(self.offsets_ptr, self.offsets_len) },
                unsafe { Region::new(self.index_ptr, self.index_len) },
                max_entries, index_cap,
            )
        }
        fn vocab_load(&self, readonly: bool) -> Vocabulary {
            Vocabulary::load(
                unsafe { Region::new(self.data_ptr, self.data_len) },
                unsafe { Region::new(self.offsets_ptr, self.offsets_len) },
                unsafe { Region::new(self.index_ptr, self.index_len) },
                readonly,
            )
        }
        fn index_bytes(&self) -> Vec<u8> {
            unsafe { std::slice::from_raw_parts(self.index_ptr, self.index_len) }.to_vec()
        }
    }

    /// #77-H1 regression: dirty (clean_flag≠1) な DB の readonly load が
    /// 共有 index を 1 byte も書き換えず、それでも lookup が正しく動くこと。
    /// 旧実装は readonly でも rebuild_index が共有 index をゼロクリアしていた。
    #[test]
    fn readonly_load_does_not_touch_shared_index() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        let a = w.get_or_insert(b"alpha");
        let b = w.get_or_insert(b"beta");
        w.sync();
        // clean_flag は書かない (= 0 のまま) → load は dirty 扱いで rebuild 経路へ

        let before = r.index_bytes();
        let ro = r.vocab_load(/*readonly=*/ true);
        assert_eq!(ro.lookup(b"alpha"), Some(a), "shadow index で lookup できる");
        assert_eq!(ro.lookup(b"beta"), Some(b));
        assert_eq!(ro.lookup(b"gamma"), None);
        assert_eq!(r.index_bytes(), before, "共有 index が書き換えられた (#77-H1)");

        // writer 側の index はそのまま生きている (get_or_insert が dedupe できる)
        assert_eq!(w.get_or_insert(b"alpha"), a, "writer の dedupe が壊れた");
    }

    /// #77-M1 regression: mark_index_clean(true) (= flush) 後の最初の insert が
    /// clean_flag を 0 に戻すこと。旧実装は open 時の 1 回しか倒さなかった。
    #[test]
    fn insert_after_clean_re_dirties_flag() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        w.get_or_insert(b"first");
        w.sync();
        w.mark_index_clean(true);
        let flag = |r: &Regions| -> u32 {
            let dm = unsafe { std::slice::from_raw_parts(r.data_ptr, r.data_len) };
            u32::from_le_bytes(dm[CLEAN_FLAG_OFF..CLEAN_FLAG_OFF + 4].try_into().unwrap())
        };
        assert_eq!(flag(&r), 1, "flush 直後は clean=1");
        w.get_or_insert(b"second");
        assert_eq!(flag(&r), 0, "flush 後の insert で clean=0 に戻るはず (#77-M1)");
    }
}

#[inline(always)]
fn fxhash(data: &[u8]) -> u64 {
    const SEED: u64 = 0x517cc1b727220a95;
    let mut h: u64 = 0;
    let mut i = 0;
    while i + 8 <= data.len() {
        let word = u64::from_le_bytes([
            data[i], data[i+1], data[i+2], data[i+3],
            data[i+4], data[i+5], data[i+6], data[i+7],
        ]);
        h = (h.rotate_left(5) ^ word).wrapping_mul(SEED);
        i += 8;
    }
    while i < data.len() {
        h = (h.rotate_left(5) ^ data[i] as u64).wrapping_mul(SEED);
        i += 1;
    }
    h
}
