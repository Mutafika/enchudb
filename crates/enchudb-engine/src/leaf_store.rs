//! `LeafStore` — Leaf 終端ノードの可変長 payload を格納する **単一所有・reclaim
//! 対応** の value store。 #88 / [[request11]]。 offset の word 化は #90。
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
//! # cell offset は word 単位 (#90)
//!
//! himo 列は 4B/entity 固定なので Leaf 参照は u32 に収める必要がある。 slot は
//! `2^off_shift` aligned で確保されるので **offset の下位 off_shift bit は常に 0** =
//! **word offset (`byte_off >> off_shift`)** で持てば、 列幅も indirection も増やさず
//! region cap を `u32::MAX << off_shift` byte まで伸ばせる:
//!
//! | off_shift | slot align | region cap |
//! |-----------|-----------|------------|
//! | 0 (v6)    | 4B        | ~4 GB (byte offset そのまま) |
//! | 2         | 4B        | ~16 GB |
//! | 3         | 8B        | ~32 GB |
//! | 4         | 16B       | ~64 GB |
//!
//! `off_shift` は region header に self-describing に書くので (byte 8)、 **v6 region は
//! 予約ゼロ = off_shift 0 = byte offset** として無改変で読める (migration 不要)。
//! cell handle / high_water / free-list の key/size は **word 単位**、 slot header の
//! `slot_size` / `len` は **byte 単位** のまま (payload 長は任意 byte)。
//!
//! # レイアウト (自己記述的 = 領域を walk できる)
//!
//! ```text
//! [MAGIC "LEF1": 4B][high_water(word): AtomicU32 LE @4][off_shift: u8 @8][reserved: 3B @9]
//! slot (live/free 問わず): [slot_size(byte): u32 LE][len(byte): u32 LE][bytes ... padding]
//!   - slot_size = この slot が占める総 byte (header 8B 込み、 2^off_shift aligned)
//!   - len       = 実 payload 長 (<= slot_size - 8)。 free slot では未使用
//!   - cell (himo) が持つ参照 = slot 先頭の **word offset** (= byte_off >> off_shift)
//! ```
//!
//! 全 slot が先頭に `slot_size` を持つので、 `data_start..high_water` を slot 刻みで
//! walk できる。 これにより reopen 時に「live cell 集合」を渡すだけで free-list を
//! rebuild できる (free-list 自体は永続化しない = store の派生)。

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use crate::region::Region;

/// v6/v7 の leaf region magic (8B header slot)。 read は今も受理。
const MAGIC: [u8; 4] = [b'L', b'E', b'F', b'1'];
/// #106 (gen seqlock) を書ける region の magic (12B header slot を含みうる)。
/// 新規 DB はこちら。 旧 engine は magic assert で clean refuse (forward-incompat)。
const MAGIC_GEN: [u8; 4] = [b'L', b'E', b'F', b'2'];
/// header: MAGIC(4) + high_water word(4) + off_shift(1) + reserved(3)
const HEADER: usize = 12;
const HIGH_WATER_OFF: usize = 4;
/// off_shift を書く byte (v6 region は予約ゼロ = 0 = byte offset)。
const SHIFT_OFF: usize = 8;
/// legacy slot 先頭の [slot_size:4][len:4] (どちらも byte)。 slot_size は 4-align で
/// bit0 は常に 0 = legacy 標識。
const SLOT_HEADER: usize = 8;
/// #106: gen 付き slot 先頭の [slot_size|HAS_GEN:4][len:4][gen:4]。 slot_size の
/// bit0(HAS_GEN) が 1 の slot は gen 付き = 12B header。
const SLOT_HEADER_GEN: usize = 12;
/// gen field の slot 内 byte offset (= legacy payload の開始位置に重なる)。
const GEN_OFF: usize = 8;
/// slot_size の bit0。 4-align で本来 0 なので、 1 を「gen 付き 12B header」の標識に使う。
const HAS_GEN: u32 = 1;
/// 最小 slot (空 payload = header のみ、 byte)。 これ未満の余りは hole にできない。
const MIN_SLOT: usize = SLOT_HEADER;
/// off_shift の上限 (16B align = 64GB)。 これ以上は padding 過大で不許可。
pub const MAX_OFF_SHIFT: u32 = 4;

#[inline]
fn align_up(x: usize, a: usize) -> usize { (x + a - 1) & !(a - 1) }

/// 指定 `off_shift` で addressable な leaf region の最大 byte 数 (= u32::MAX word)。
#[inline]
pub fn cap_bytes_for_shift(off_shift: u32) -> u64 {
    (u32::MAX as u64) << off_shift
}

/// Leaf payload の store。 単一所有・reclaim 対応。 offset は word 単位 (#90)。
pub struct LeafStore {
    region: Region,
    /// free hole: **word** offset -> **word** slot_size。 offset 昇順 (coalesce 必須)。
    /// **永続化しない** — reopen 時は `rebuild_free_list` で live cell から再構成。
    /// insert/free/rebuild を直列化する (writer は `.db.lock` で 1 プロセスだが
    /// in-process では複数スレッドが consumer 経由で触りうる)。
    holes: Mutex<BTreeMap<u32, u32>>,
    /// byte_off = word_off << off_shift。 0 = byte offset (v6 互換)。
    off_shift: u32,
    /// #106: slot gen seqlock 用の store-wide 単調カウンタ。 insert ごとに +2 した
    /// **偶数** を新 slot の gen に焼く。 offset ごとに再利用のたび必ず異なる値になるので、
    /// 旧 offset を掴んだ reader が seqlock で再利用/torn を検出できる。 payload バイトから
    /// 導出すると 'x' filler 等で衝突する (それが #106 の torn 残渣だった) ため、
    /// 独立カウンタにする。 in-memory (writer 単一プロセス内で単調)。 wrap は 2^31 insert
    /// 毎 = reader の read 窓 (µs) では衝突しない。
    gen_seq: std::sync::atomic::AtomicU32,
}

// Region は raw ptr を持つため。 vocab / entity_set と同じ扱い (writer 排他は
// `.db.lock` + in-process の `holes` Mutex で担保)。
unsafe impl Sync for LeafStore {}
unsafe impl Send for LeafStore {}

/// #106: `try_read` の結果。 `Retry` は torn/stale の可能性 = 呼び側で column
/// offset を再読して loop する合図。
pub enum LeafRead {
    Ok(Vec<u8>),
    Retry,
}

impl LeafStore {
    /// 新規領域を初期化。 `off_shift` (0/2/3/4) で region cap を選ぶ。
    /// high_water = 最初の aligned slot 位置 (word)。
    pub fn init(region: Region, off_shift: u32) -> Self {
        assert!(off_shift <= MAX_OFF_SHIFT, "off_shift {} > {}", off_shift, MAX_OFF_SHIFT);
        let s = Self {
            region,
            holes: Mutex::new(BTreeMap::new()),
            off_shift,
            gen_seq: std::sync::atomic::AtomicU32::new(0),
        };
        let data_start = s.data_start_byte();
        let _ = s.region.ensure_committed(data_start);
        s.region.write_at(0, &MAGIC_GEN); // #106: 新規は gen 対応 magic
        s.region.write_at(SHIFT_OFF, &[off_shift as u8]);
        s.high_water_atomic().store(s.b2w(data_start), Ordering::Relaxed);
        s
    }

    /// 既存領域をロード。 `off_shift` は header (byte 8) から読む (v6 = 0)。
    /// free-list は空 (= 全空間を使用中扱い)。 呼び出し側が `rebuild_free_list(live)`
    /// を呼ぶまで reclaim は効かない (fresh append のみ)。
    pub fn load(region: Region) -> Self {
        let mm = region.slice();
        assert!(
            mm.len() >= HEADER && (mm[0..4] == MAGIC || mm[0..4] == MAGIC_GEN),
            "bad leaf store magic"
        );
        let off_shift = mm[SHIFT_OFF] as u32;
        assert!(off_shift <= MAX_OFF_SHIFT, "leaf store corrupt: off_shift {}", off_shift);
        Self {
            region,
            holes: Mutex::new(BTreeMap::new()),
            off_shift,
            gen_seq: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// この store の cell offset shift (0 = byte / v6 互換、 2/3/4 = 16/32/64GB)。
    #[inline]
    pub fn off_shift(&self) -> u32 { self.off_shift }

    // ── word ↔ byte 変換 (byte_off は常に 2^off_shift の倍数) ──
    #[inline]
    fn w2b(&self, w: u32) -> usize { (w as usize) << self.off_shift }
    #[inline]
    fn b2w(&self, b: usize) -> u32 { (b >> self.off_shift) as u32 }

    /// slot の物理 byte alignment (= max(4, 2^off_shift))。
    #[inline]
    fn slot_align(&self) -> usize { std::cmp::max(4, 1usize << self.off_shift) }
    #[inline]
    fn align_slot(&self, x: usize) -> usize { align_up(x, self.slot_align()) }
    /// 最初の slot の byte offset (HEADER を alignment まで押し上げた位置)。
    #[inline]
    fn data_start_byte(&self) -> usize { self.align_slot(HEADER) }
    /// 最小 slot の word 数 (padding 込みで header を収める最小)。 常に >= 1。
    #[inline]
    fn min_slot_words(&self) -> u32 { self.b2w(self.align_slot(MIN_SLOT)) }

    #[inline]
    fn high_water_atomic(&self) -> &AtomicU32 {
        self.region.as_atomic_u32(HIGH_WATER_OFF)
    }
    #[inline]
    fn hw_words(&self) -> u32 { self.high_water_atomic().load(Ordering::Relaxed) }
    #[inline]
    fn set_hw_words(&self, w: u32) { self.high_water_atomic().store(w, Ordering::Relaxed); }

    /// 現在の footprint (使用領域の上端) を **byte** で返す。 16GB 超も表せるよう u64。
    pub fn high_water(&self) -> u64 {
        (self.hw_words() as u64) << self.off_shift
    }

    /// word offset の slot が占める **byte** slot_size。 gen 付き slot は bit0(HAS_GEN)
    /// が立つので必ず mask して実 size を返す (real slot_size は 4-align で bit0=0)。
    #[inline]
    fn slot_size_bytes_at(&self, word_off: u32) -> u32 {
        let mm = self.region.slice();
        let o = self.w2b(word_off);
        u32::from_le_bytes(mm[o..o + 4].try_into().unwrap()) & !HAS_GEN
    }

    /// この slot の header 長 (gen 付き = 12B、 legacy = 8B) を bit0 で判定。
    #[inline]
    fn header_len(ss_raw: u32) -> usize {
        if ss_raw & HAS_GEN != 0 { SLOT_HEADER_GEN } else { SLOT_HEADER }
    }

    /// slot 先頭 (word offset) から payload を読む。 **single-thread / quiesce 前提**
    /// (live mmap への参照を返す)。 read-while-write する経路は `try_read` を使う。
    pub fn get(&self, word_off: u32) -> &[u8] {
        let mm = self.region.slice();
        let o = self.w2b(word_off);
        let ss_raw = u32::from_le_bytes(mm[o..o + 4].try_into().unwrap());
        let hdr = Self::header_len(ss_raw);
        let len = u32::from_le_bytes(mm[o + 4..o + 8].try_into().unwrap()) as usize;
        &mm[o + hdr..o + hdr + len]
    }

    /// #106: read-while-write でも安全に payload の一貫 snapshot を **copy** して返す
    /// (seqlock read)。 live mmap への参照を返さないので aliasing UB が無い。
    ///
    /// - **gen 付き slot (12B)**: gen を Acquire で読み (偶数=stable)、 copy 後に
    ///   fence(Acquire) + gen 再読して一致を確認。 writer が同 slot を上書き/再利用
    ///   していれば gen が変わる (odd 中 or 別の even) ので `Retry` を返す。
    /// - **legacy slot (8B)**: writer 側は今後 gen 付き slot しか書かないので、 legacy
    ///   slot は不変 (freed→再利用時に gen 付き 12B に化ける)。 ss を copy 前後で
    ///   再読し、 変化 (= bit0 が立った = 再利用) なら `Retry`。
    /// - bounds を全て検証し、 破れた len を掴んでも panic せず `Retry`。
    ///
    /// `Retry` は「stale/torn の可能性」= 呼び側 (engine `get_text_owned`) が column
    /// offset を再読して loop する合図。
    pub fn try_read(&self, word_off: u32) -> LeafRead {
        let mm = self.region.slice();
        let o = self.w2b(word_off);
        let Some(h8) = o.checked_add(SLOT_HEADER) else { return LeafRead::Retry };
        if h8 > mm.len() {
            return LeafRead::Retry;
        }
        // header field は **atomic load** で読む。 plain な `&[u8]` slice 読みは
        // 「不変」とコンパイラに誤認され、 並行 writer 下 (data race) で再順序化/
        // 重複除去され seqlock を破る。 as_atomic_u32 で volatile 相当にする。
        let ss0 = self.region.as_atomic_u32(o).load(Ordering::Relaxed);

        if ss0 & HAS_GEN != 0 {
            // ── gen 付き 12B slot: seqlock ──
            // 鉄則: gen(g0) を **先に** 読み、 slot_size / len / payload は **すべて g0 の
            // 後・g1 の前** に読む。 こうしないと slot field が seqlock の外に漏れ、
            // 世代跨ぎ (slot_size と len の不整合) で torn を通す。
            let Some(h12) = o.checked_add(SLOT_HEADER_GEN) else { return LeafRead::Retry };
            if h12 > mm.len() {
                return LeafRead::Retry;
            }
            let gen_at = self.region.as_atomic_u32(o + GEN_OFF);
            let g0 = gen_at.load(Ordering::Acquire);
            if g0 & 1 != 0 {
                return LeafRead::Retry; // odd = writer が書込中
            }
            // ここから全 field を g0 の後で atomic load する。
            let ss = self.region.as_atomic_u32(o).load(Ordering::Relaxed);
            if ss & HAS_GEN == 0 {
                return LeafRead::Retry; // g0 直後に legacy 化 = 再利用途中
            }
            let slot_size = (ss & !HAS_GEN) as usize;
            let len = self.region.as_atomic_u32(o + 4).load(Ordering::Relaxed) as usize;
            if len > slot_size.saturating_sub(SLOT_HEADER_GEN) {
                return LeafRead::Retry;
            }
            let Some(end) = h12.checked_add(len) else { return LeafRead::Retry };
            if end > mm.len() {
                return LeafRead::Retry;
            }
            // payload は memcpy (opaque) で copy。
            let bytes = mm[h12..end].to_vec();
            // data read が gen 再読より前で確定するよう Acquire fence を挟む。
            std::sync::atomic::fence(Ordering::Acquire);
            let g1 = gen_at.load(Ordering::Acquire);
            if g1 != g0 {
                return LeafRead::Retry; // copy 中に上書き/再利用された
            }
            LeafRead::Ok(bytes)
        } else {
            // ── legacy 8B slot (不変・再利用時のみ 12B へ化ける) ──
            let slot_size = ss0 as usize;
            let len = self.region.as_atomic_u32(o + 4).load(Ordering::Relaxed) as usize;
            if len > slot_size.saturating_sub(SLOT_HEADER) {
                return LeafRead::Retry;
            }
            let Some(end) = h8.checked_add(len) else { return LeafRead::Retry };
            if end > mm.len() {
                return LeafRead::Retry;
            }
            let bytes = mm[h8..end].to_vec();
            std::sync::atomic::fence(Ordering::Acquire);
            let ss1 = self.region.as_atomic_u32(o).load(Ordering::Relaxed);
            if ss1 != ss0 {
                return LeafRead::Retry; // 再利用されて 12B 化 = stale
            }
            LeafRead::Ok(bytes)
        }
    }

    /// payload を格納し slot 先頭 **word offset** を返す。 free-list に嵌まる空きが
    /// あればそこへ、 無ければ high_water を伸ばす。
    pub fn insert(&self, bytes: &[u8]) -> u32 {
        let blen = bytes.len();
        assert!(blen <= (u32::MAX as usize - SLOT_HEADER_GEN), "leaf value too large");
        // #106: 新規 slot は常に gen 付き 12B header。
        let need_bytes = self.align_slot(SLOT_HEADER_GEN + blen);
        let need_words = self.b2w(need_bytes);

        let mut holes = self.holes.lock().unwrap_or_else(|p| p.into_inner());
        let (word_off, slot_words) = self.alloc_locked(&mut holes, need_words);
        drop(holes);

        let byte_off = self.w2b(word_off);
        let slot_bytes = self.w2b(slot_words);
        let _ = self.region.ensure_committed(byte_off + slot_bytes);

        // #106: store-wide 単調カウンタから新 gen (偶数) を採る。 offset ごとに再利用の
        // たび必ず異なる値になるので、 旧 offset を掴んだ reader が seqlock で torn/再利用を
        // 検出できる。 payload バイト由来だと 'x' filler 等で衝突する (torn 残渣の原因)。
        let gen_atomic = self.region.as_atomic_u32(byte_off + GEN_OFF);
        let g = self.gen_seq.fetch_add(2, Ordering::Relaxed).wrapping_add(2); // even

        // #83: 書込は全て `write_at` (raw ptr, `&mut [u8]` を作らない)。 gen_atomic と
        // 同じ `&self` で interior-mutate するので aliasing 参照が発生しない。 順序は
        // plain store のままなので下の Release fence / gen store の seqlock 契約は不変。
        // 1. slot_size に HAS_GEN を立てて「gen 付き 12B」と標識 (+ 実 size)。
        self.region.write_at(byte_off, &((slot_bytes as u32) | HAS_GEN).to_le_bytes());
        // 2. gen を odd に (書込開始マーカ)。
        gen_atomic.store(g | 1, Ordering::Relaxed);
        // 3. Release fence: odd + ss の書込を payload 書込より前に順序づける。 これが
        //    無いと weak-memory (ARM) で payload の plain write が odd store より先に
        //    reader へ可視化し、 reader が「g0=旧even, torn payload, g1=旧even」を掴んで
        //    torn を通す穴になる (標準 seqlock の begin fence)。
        std::sync::atomic::fence(Ordering::Release);
        // 4. len + payload。
        self.region.write_at(byte_off + 4, &(blen as u32).to_le_bytes());
        self.region.write_at(byte_off + SLOT_HEADER_GEN, bytes);
        // 5. gen を even=g に (Release publish)。 reader の Acquire fence と対で payload の
        //    可視性を保証。
        gen_atomic.store(g, Ordering::Release);

        // legacy magic の DB に初めて gen slot を書いたら magic を upgrade (forward-incompat)。
        if self.region.slice()[0..4] != MAGIC_GEN {
            self.region.write_at(0, &MAGIC_GEN);
            self.region.mark_dirty(0, 4);
        }
        self.region.mark_dirty(byte_off, slot_bytes);
        self.region.mark_dirty(HIGH_WATER_OFF, 4);
        word_off
    }

    /// `need_words` を確保。 返り値 = (word offset, 実 slot_words)。 best-fit は
    /// O(hole 数) の線形走査 (Phase 1)。 hole が定常で少なければ十分。
    fn alloc_locked(&self, holes: &mut BTreeMap<u32, u32>, need_words: u32) -> (u32, u32) {
        // best-fit: need 以上で最小の hole
        let best = holes
            .iter()
            .filter(|(_, sz)| **sz >= need_words)
            .min_by_key(|(_, sz)| **sz)
            .map(|(&off, &sz)| (off, sz));

        if let Some((hoff, hsize)) = best {
            holes.remove(&hoff);
            let remainder = hsize - need_words;
            if remainder >= self.min_slot_words() {
                // split: 余りを hole として登録 (byte slot_size header を書いて walkable に)
                let roff = hoff + need_words;
                let rbyte = self.w2b(roff);
                self.region.write_at(rbyte, &(self.w2b(remainder) as u32).to_le_bytes());
                holes.insert(roff, remainder);
                (hoff, need_words)
            } else {
                // 余りが hole にならない → 丸ごと払い出し (内部断片 < MIN_SLOT)
                (hoff, hsize)
            }
        } else {
            // fresh: high_water を伸ばす
            let hw = self.hw_words();
            let end = hw + need_words;
            let _ = self.region.ensure_committed(self.w2b(end));
            self.set_hw_words(end);
            (hw, need_words)
        }
    }

    /// slot を解放 (word offset)。 隣接 free hole と coalesce し、 末尾なら high_water 後退。
    pub fn free(&self, word_off: u32) {
        let mut hsize = self.b2w(self.slot_size_bytes_at(word_off) as usize);
        let mut hoff = word_off;
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

        let hw = self.hw_words();
        if hoff + hsize == hw {
            // 末尾に達した → 自由空間へ返す (footprint 後退)
            self.set_hw_words(hoff);
            self.region.mark_dirty(HIGH_WATER_OFF, 4);
        } else {
            // merged header (byte slot_size) を書いて walkable 維持 + free-list 登録
            let hbyte = self.w2b(hoff);
            self.region.write_at(hbyte, &(self.w2b(hsize) as u32).to_le_bytes());
            self.region.mark_dirty(hbyte, 4);
            holes.insert(hoff, hsize);
        }
    }

    /// reopen 用: live な slot **word offset** 集合を渡し、 領域を walk して free-list を
    /// 再構成する。 free-list は永続化しないので open 時に必ず呼ぶ。
    pub fn rebuild_free_list(&self, live: &HashSet<u32>) {
        let hw = self.hw_words();
        let mut holes = self.holes.lock().unwrap_or_else(|p| p.into_inner());
        holes.clear();

        // 連続する free run を溜めて coalesce する (word 単位)
        let mut pending: Option<(u32, u32)> = None; // (word offset, word size)
        let mut off = self.b2w(self.data_start_byte());
        while off < hw {
            let ss = self.b2w(self.slot_size_bytes_at(off) as usize);
            assert!(
                ss >= self.min_slot_words() && (off as u64 + ss as u64) <= hw as u64,
                "leaf store corrupt: slot_words {} at word_off {} (hw {})", ss, off, hw
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

    /// rebuild 中: coalesce 済み free run を確定 (word 単位)。 末尾なら high_water 後退。
    fn flush_run(&self, holes: &mut BTreeMap<u32, u32>, off: u32, size: u32, hw: u32) {
        if off + size == hw {
            self.set_hw_words(off);
        } else {
            let obyte = self.w2b(off);
            self.region.write_at(obyte, &(self.w2b(size) as u32).to_le_bytes());
            holes.insert(off, size);
        }
    }

    /// free-list が抱える回収可能 **byte** 数 (test / stats 用)。
    pub fn free_bytes(&self) -> u64 {
        let holes = self.holes.lock().unwrap_or_else(|p| p.into_inner());
        holes.values().map(|&w| (w as u64) << self.off_shift).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// heap buffer 上に static backing の Region を作る (grower 無し = 全 commit 済み)。
    /// default は #90 の新 default = off_shift 2 (16GB cap、 slot align4 で v6 と byte 等価)。
    fn make_store(bytes: usize) -> LeafStore {
        make_store_shift(bytes, 2)
    }
    fn make_store_shift(bytes: usize, off_shift: u32) -> LeafStore {
        let buf: Box<[u8]> = vec![0u8; bytes].into_boxed_slice();
        let ptr = Box::leak(buf).as_mut_ptr();
        let region = unsafe { Region::new(ptr, bytes) };
        LeafStore::init(region, off_shift)
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

    /// #90: cell handle が **word offset** (= byte_off >> off_shift) であることを確認。
    /// shift 0 は byte offset そのまま、 shift 2 は 1/4。
    #[test]
    fn handle_is_word_offset() {
        // shift 0 (v6): 最初の slot は byte 12、 handle も 12
        let s0 = make_store_shift(64 * 1024, 0);
        assert_eq!(s0.off_shift(), 0);
        let a0 = s0.insert(b"x");
        assert_eq!(a0, 12, "shift0 は byte offset そのまま");

        // shift 2: data_start = align4(12)=12 → word 3。 handle = 3 (12 ではない)
        let s2 = make_store_shift(64 * 1024, 2);
        let a2 = s2.insert(b"x");
        assert_eq!(a2, 3, "shift2 は word offset (12>>2=3)");
        // 2 個目の handle 差分は need_words 単位。 #106: gen 付き 12B header なので
        // "x" (1B) → slot = align4(12+1) = 16B = 4 word。
        let b2 = s2.insert(b"y");
        assert_eq!(b2 - a2, 4, "handle 増分は word 単位");
        // それでも payload は正しく往復
        assert_eq!(s2.get(a2), b"x");
        assert_eq!(s2.get(b2), b"y");
    }

    /// #90: shift による region cap (= u32::MAX word × 2^shift byte)。
    #[test]
    fn cap_scales_with_shift() {
        assert_eq!(cap_bytes_for_shift(0), u32::MAX as u64); // ~4 GB
        assert_eq!(cap_bytes_for_shift(2), (u32::MAX as u64) * 4); // ~16 GB
        assert_eq!(cap_bytes_for_shift(3), (u32::MAX as u64) * 8); // ~32 GB
        assert_eq!(cap_bytes_for_shift(4), (u32::MAX as u64) * 16); // ~64 GB
        assert!(cap_bytes_for_shift(2) > 15 * 1024 * 1024 * 1024);
        assert!(cap_bytes_for_shift(4) > 63 * 1024 * 1024 * 1024);
    }

    /// #90: byte offset が u32::MAX を超える領域 (sparse) でも word handle で往復できる
    /// = cap 拡張の実証。 sparse mmap を使い物理は 1 page 程度しか触らない。
    #[test]
    fn addresses_beyond_4gb_via_word_offset() {
        // 5 GiB の sparse region。 mmap は virtual、 書いた page だけ物理化。
        let bytes: usize = 5 * 1024 * 1024 * 1024;
        let mm = match memmap2::MmapMut::map_anon(bytes) {
            Ok(m) => m,
            Err(_) => return, // 環境が anon 5GiB を map できなければ skip
        };
        let ptr = mm.as_ptr() as *mut u8;
        let region = unsafe { Region::new(ptr, bytes) };
        let s = LeafStore::init(region, 2); // 16GB cap
        // high_water を byte ~4.2GB (u32::MAX 近傍) 相当の word まで手で進める
        let near_4gb_word = (u32::MAX / 4) + 16; // byte ≈ 4.29GB > u32::MAX
        s.set_hw_words(near_4gb_word);
        let h = s.insert(b"beyond-4gb");
        // handle が示す byte offset は u32::MAX を超えている
        assert!(s.w2b(h) as u64 > u32::MAX as u64, "byte offset が 4GB を超えていない");
        assert_eq!(s.get(h), b"beyond-4gb", "4GB 超の word handle で往復できない");
        drop(mm);
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
    /// shift 2/4 両方で回して word 化しても reclaim が効くことを確認。
    #[test]
    fn churn_footprint_bounded_and_falsified() {
        for shift in [2u32, 4u32] {
            const ROUNDS: usize = 50;
            const PER: usize = 100;
            let val = vec![b'x'; 40];

            // (A) falsify: free しない → footprint は単調増加
            let no_reclaim = make_store_shift(4 * 1024 * 1024, shift);
            for _ in 0..ROUNDS {
                for _ in 0..PER {
                    no_reclaim.insert(&val);
                }
            }
            let grow = no_reclaim.high_water();

            // (B) 本番: 各 round で全 free → footprint は 1 round 分に有界
            let reclaim = make_store_shift(4 * 1024 * 1024, shift);
            for _ in 0..ROUNDS {
                let refs: Vec<u32> = (0..PER).map(|_| reclaim.insert(&val)).collect();
                for r in refs {
                    reclaim.free(r);
                }
            }
            let bounded = reclaim.high_water();

            assert!(
                grow as f64 > bounded as f64 * (ROUNDS as f64 / 4.0),
                "shift{shift}: free 無効時に growth が再現しない (A={grow}, B={bounded})",
            );
            let slot = align_up(SLOT_HEADER + val.len(), std::cmp::max(4, 1usize << shift));
            let one_round = (slot * PER) as u64;
            assert!(
                bounded <= one_round * 2 + HEADER as u64,
                "shift{shift}: reclaim 下で footprint が有界化してない (B={bounded}, 1round≈{one_round})",
            );
        }
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

    /// v6 互換: off_shift 0 (byte offset) でも従来通り動く。
    #[test]
    fn shift0_byte_offset_compat() {
        let s = make_store_shift(64 * 1024, 0);
        let a = s.insert(b"legacy");
        let b = s.insert(b"bytes!");
        assert_eq!(s.get(a), b"legacy");
        s.free(a);
        let c = s.insert(b"reused"); // 同 6B → a の穴を再利用
        assert_eq!(s.get(c), b"reused");
        assert_eq!(s.get(b), b"bytes!");
    }
}
