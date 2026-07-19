//! Region — 単一 mmap ファイル内のスライス。 ロックなし。
//!
//! 通常モードでは `ptr + len` だけ持って raw deref する単純な view だが、
//! `with_grower` で生成した Region は **GrowableMap への back-reference** と
//! **ファイル内オフセット** を保持し、 `ensure_committed(end)` で書き込み境界
//! までの commit を要求できる。 store 側 (vocabulary / content_store / cylinder)
//! が data_end を進める前に呼ぶのが期待されるパターン。

use std::sync::atomic::AtomicU32;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use crate::growable_map::GrowableMap;

pub struct Region {
    ptr: *mut u8,
    len: usize,
    /// Optional growth backref. Set only for regions that live in
    /// a `Backing::Growable` and may need to extend the file-backed
    /// commit. `None` for static `MmapMut` / `Memory` backings (in
    /// those, the whole reservation is already committed).
    /// Not present on wasm32 (no `GrowableMap` there).
    #[cfg(not(target_arch = "wasm32"))]
    grower: Option<Arc<GrowableMap>>,
    /// Offset of this region's start within the underlying file.
    /// Required to translate "end within region" → "end within file"
    /// before calling `GrowableMap::grow_amortized`.
    #[cfg(not(target_arch = "wasm32"))]
    file_offset: usize,
}

unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    /// # Safety
    /// ptr must point to a valid mutable memory region of at least `len`
    /// bytes that outlives this Region.
    pub unsafe fn new(ptr: *mut u8, len: usize) -> Self {
        Self {
            ptr,
            len,
            #[cfg(not(target_arch = "wasm32"))]
            grower: None,
            #[cfg(not(target_arch = "wasm32"))]
            file_offset: 0,
        }
    }

    /// Growable backing 用 ctor。 `grower` の `base() + file_offset` が
    /// この region の先頭。 `len` は region の **論理上限** (= layout 上の
    /// セクション幅)、 実際に commit されているのは `grower.committed()`
    /// と `file_offset` の関係で決まる。 store 側が region 内オフセット
    /// `end_in_region` まで使いたいときに `ensure_committed(end_in_region)`
    /// を呼ぶ。
    ///
    /// # Safety
    /// `grower.base() + file_offset` から `len` バイトが reservation 内に
    /// 収まっていること (caller responsibility)。
    #[cfg(not(target_arch = "wasm32"))]
    pub unsafe fn with_grower(
        grower: Arc<GrowableMap>,
        file_offset: usize,
        len: usize,
    ) -> Self {
        let ptr = unsafe { grower.base().add(file_offset) };
        Self {
            ptr,
            len,
            grower: Some(grower),
            file_offset,
        }
    }

    #[inline(always)]
    pub fn slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    // `mut_from_ref` (deny by default): `&self` から `&mut [u8]` を返すのは通常 aliasing
    // UB の温床。 本 store は「単一 writer プロセス + in-process は各 store の Mutex /
    // append-only + epoch 規約」で同時可変 alias を出さない運用不変式で回している
    // (`unsafe impl Sync` と同じ前提)。 これは *型* 不変式ではなく運用規約なので、
    // long-lived な mutable alias を実体化しない API (`write_at` / `ptr`) への置換が
    // 本筋 (issue #83 の option 2、 P2)。 それまでは理由明記で lint を局所 allow する。
    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    pub fn slice_mut(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// region 内 [off_in_region, off_in_region+len) に書き込んだ事を grower に
    /// 通知 (= dirty range 記録)。 grower が無い場合は no-op。
    ///
    /// request3 dirty range tracking: `flush_dirty` の msync 範囲を絞る hint。
    /// 呼び忘れても correctness には影響しない (= OS の遅延 flush に任せる)
    /// が、 hot write 経路 (HimoStore::set / Vocabulary::insert /
    /// EntitySet::allocate / ContentStore::set) からは呼ぶこと。
    #[inline]
    pub fn mark_dirty(&self, off_in_region: usize, len: usize) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(g) = &self.grower {
            g.mark_dirty(self.file_offset + off_in_region, len);
        }
        let _ = off_in_region;
        let _ = len;
    }

    /// Region 内の `end_in_region` byte までを書き込み可能にする (= ファイル
    /// 内 commit を `file_offset + end_in_region` まで進める)。 grower が
    /// 設定されていなければ no-op (静的 backing は既に全コミット済み)。
    ///
    /// 呼び出し側は `data_end.fetch_add` の **直前** で必要 end を渡す。
    /// `grow_amortized` の doubling で次回以降の小さい advance では
    /// 何もせず帰る。
    pub fn ensure_committed(&self, end_in_region: usize) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(g) = &self.grower {
            g.grow_amortized(self.file_offset + end_in_region)?;
        }
        let _ = end_in_region;
        Ok(())
    }

    /// mmap 上の 4 byte を `AtomicU32` として参照する。
    ///
    /// `MAP_SHARED` で map された region は複数プロセスで **同じ物理ページ**
    /// を共有するので、 ここに atomic u32 を置くと `fetch_add` 等の操作が
    /// プロセス跨ぎで整合する。 cross-process counter (ContentStore::data_end
    /// 等) 用。
    ///
    /// # Panics
    /// - `offset + 4 > len` なら panic
    /// - `offset % 4 != 0` なら panic (AtomicU32 は 4 byte aligned 要求)
    #[inline(always)]
    pub fn as_atomic_u32(&self, offset: usize) -> &AtomicU32 {
        assert!(
            offset + 4 <= self.len,
            "atomic_u32 out of range: {} + 4 > {}",
            offset,
            self.len
        );
        assert!(
            offset % 4 == 0,
            "atomic_u32 offset {} not 4-byte aligned",
            offset
        );
        unsafe { &*(self.ptr.add(offset) as *const AtomicU32) }
    }
}
