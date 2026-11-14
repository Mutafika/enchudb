//! #731 / #863: eid の local = **世代 | 場所** (計画は repo 直下の `EID_REUSE_PLAN.md`、 段 1)。
//!
//! 場所 (slot) = 列・生存ビットマップ・版数の列の添字 (前の local そのもの)。 消えた行の場所を使い回した行は、 世代の違う
//! eid になる。 アプリが持ち続けた古い eid は入口の照合で 「消えた行」 になる (読みは None、 書き込み / 消すは何もしない)。
//!
//! - 場所の bit `S` = max(24, max_entities の bit 数) を、 最初に場所を使い回す時に header (`H_SLOT_BITS`) に焼く。 世代の
//!   bit `G` = 32 − S。 世代は 0..=2^G − 2。 2^G − 1 は 「引退した場所」 の印 (世代を使い切った場所は二度と渡さない)。
//!   いちばん上の世代を使わないので、 local の最大は 2^32 − 2^S − 1 で、 cell の空の印 (u32::MAX) や写像の番兵に届かない
//! - 世代は場所ごとに 1 B (`gen.seg` = 先頭 [`GEN_HEADER`] byte の header + 1 B / 場所)。 header には表ごとの 「世代 > 0 の場所が
//!   ある」 印 (出口で、 使い回しの無い表は世代を読まない)
//! - 上げる時: 世代を持たない DB (file version 14 以下) は最初の使い回しで v15 に移る ([`crate::engine::Engine`] の
//!   `move_to_generations`)。 その時に、 世代を持つ表の死んだ場所 (v14 の間に消した場所) を全部 1 にする。 v15 では行を消す時に
//!   上げる (死んだ場所の値 = 次の住人の世代)。 使い回す時・位置から払い出す時は今の値で渡す
//! - 予約の表 (`_` で始まる、 `_sync_ops` の ring など) は世代を持たない (0 のまま使い回す)

use crate::region::Region;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// [`Generations::gate`] の 「消す時に上げる」 印 (最上位 bit)。 下位 32 bit = 消している書き手 (`enter_free` 〜 `exit_free`) の数。
const GATE_ON: u64 = 1 << 63;
/// 本体の書き出しが生きている印 (entities) と場所の世代 (`gen.seg`) を書き出している間 (F1)。 消す書き手は入らずに待つ。
const GATE_FREEZE: u64 = 1 << 62;
const GATE_COUNT: u64 = u32::MAX as u64;

/// `gen.seg` の先頭の header の長さ。 [`MAGIC`] / 場所の bit / 表ごとの印 (TableId は u16 = 8 KiB) / page ごとの印。
pub(crate) const GEN_HEADER: usize = 16 * 1024;
const MAGIC: [u8; 4] = *b"GEN1";
const OFF_BITS: usize = 4;
const OFF_TABLES: usize = 64;
/// page (2^[`PAGE_SHIFT`] 場所) ごとの 「世代 > 0 の場所がある」 印。 header の残り (8128 B = 65024 page = 2^29 場所まで)。 印の
/// 無い page の場所は世代を読まない (出口・照合で、 使い回しの無い所は gen.seg の 1 B / 場所 を読まない)。 印の範囲の外の場所は
/// いつも読む。
const OFF_PAGES: usize = OFF_TABLES + 8 * 1024;
const PAGE_SHIFT: u32 = 13;
const PAGES: u32 = ((GEN_HEADER - OFF_PAGES) * 8) as u32;

/// [`Generations::gate`] を他の欄と別の cache line に置く (v14 では消すたびに 2 回 RMW する語。 `get` のたびに読む `bits` / `region`
/// と同じ line にあると、 消す書き手が読み手の line を奪う)。
#[repr(align(128))]
struct Gate(AtomicU64);
/// 場所の bit の下限 (既定の `max_entities` 2^24 の DB の場所を全部表せる)。
pub(crate) const MIN_SLOT_BITS: u32 = 24;

/// `max_entities` の DB の場所の bit。
pub(crate) fn slot_bits_for(max_entities: u32) -> u32 {
    let need = 32 - max_entities.saturating_sub(1).leading_zeros();
    need.max(MIN_SLOT_BITS)
}

/// `gen.seg` の大きさ (header + 1 B / 場所)。
pub(crate) fn segment_len(entities: u32) -> usize {
    GEN_HEADER + entities as usize
}

pub(crate) struct Generations {
    /// 場所の bit `S`。 0 = 世代を持たない DB (local = 場所)。
    bits: AtomicU32,
    region: std::sync::OnceLock<Region>,
    /// v15 に移る手順と消す道の揃え (1 語): 最上位 bit = 「消す時に上げる」 (v15)、 下位 = 印の立つ前に入って、 上げずに消している
    /// 書き手の数。 消す道は入る時の RMW で印を読み (`enter_free`)、 生きている印を下ろしてから出る (`exit_free`)。 移る側は印を
    /// 立てた (RMW) 後、 数が 0 になるのを待ってから死んだ場所をなめる (`wait_frees`)。 同じ語の RMW の順で、 印を見ずに消した書き手の
    /// 場所は、 なめる時には死んでいる (なめる側が上げる)。 印を見た書き手は自分で上げる。 揃えの model は
    /// `tests/loom_eid_generation_move.rs`
    gate: Gate,
    /// v15 に移る手順 (`Engine::move_to_generations`) を 1 本に並べる。
    pub(crate) move_lock: std::sync::Mutex<()>,
    /// 本体の書き出しの 「生きている印と世代を一緒に書き出す」 段 ([`Self::freeze`]) を 1 本に並べる (書き出しは重なる)。
    freeze_lock: std::sync::Mutex<()>,
    /// WAL の再生中 (`Engine::apply_oplog_op`)。 消す道が場所の世代を上げない (再生が record の世代で上げる)。
    pub(crate) replaying: std::sync::atomic::AtomicBool,
    /// v15 に移り終えた (`Engine::move_to_generations` の最後 / v15 の DB を開いた時)。 `active` (照合を始めた) とは別: 移る途中
    /// (照合は始めたが、 v14 の間に消した場所をまだ上げていない) に使い回しを始めると、 世代 0 の場所を前と同じ eid で渡す。
    moved: std::sync::atomic::AtomicBool,
}

impl Generations {
    pub(crate) fn off() -> Self {
        Self {
            bits: AtomicU32::new(0),
            region: std::sync::OnceLock::new(),
            gate: Gate(AtomicU64::new(0)),
            move_lock: std::sync::Mutex::new(()),
            freeze_lock: std::sync::Mutex::new(()),
            replaying: std::sync::atomic::AtomicBool::new(false),
            moved: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// 開いた DB の `gen.seg` (header に場所の bit が焼いてある)。 中身の header が合わなければ Err。
    pub(crate) fn attach(&self, region: Region, bits: u32) -> Result<(), String> {
        let s = region.slice();
        if region.committed_len() < GEN_HEADER || s[..4] != MAGIC {
            return Err("gen.seg: not a generation segment".into());
        }
        let stored = u32::from_le_bytes(s[OFF_BITS..OFF_BITS + 4].try_into().unwrap());
        if stored != bits {
            return Err(format!("gen.seg: slot bits {stored} do not match the header ({bits})"));
        }
        let _ = self.region.set(region);
        self.bits.store(bits, Ordering::Release);
        self.gate.0.fetch_or(GATE_ON, Ordering::AcqRel);
        self.moved.store(true, Ordering::Release);
        Ok(())
    }

    /// v15 に移り終えたか ([`Self::finish_move`] / [`Self::attach`] の後)。
    #[inline]
    pub(crate) fn moved(&self) -> bool {
        self.moved.load(Ordering::Acquire)
    }

    /// v15 に移り終えた印 (v14 の間に消した場所を上げ、 header を書き出した後)。
    pub(crate) fn finish_move(&self) {
        self.moved.store(true, Ordering::Release);
    }

    /// v15 に移る時: `region` を 0 から初めて (前の残りは消す)、 header を書く。 まだ照合は始めない ([`Self::start`])。
    pub(crate) fn prepare(&self, region: Region, bits: u32) -> std::io::Result<()> {
        region.ensure_committed(GEN_HEADER)?;
        let used = region.committed_len();
        if used > 0 {
            region.fill_at(0, used, 0);
            region.mark_dirty(0, used);
        }
        region.write_at(0, &MAGIC);
        region.write_at(OFF_BITS, &bits.to_le_bytes());
        region.mark_dirty(0, GEN_HEADER);
        let _ = self.region.set(region);
        Ok(())
    }

    /// 照合と 「消す時に上げる」 を始める (`prepare` の後)。 この後 [`Self::wait_frees`] を待ってから死んだ場所をなめる。
    pub(crate) fn start(&self, bits: u32) {
        self.bits.store(bits, Ordering::Release);
        self.gate.0.fetch_or(GATE_ON, Ordering::AcqRel);
    }

    /// 消す道の入口: (消す時に上げるか, 出る時に [`Self::exit_free`] を呼ぶか)。 世代を持つ DB (v15) では、 本体の書き出しが
    /// 生きている印と世代を書き出している間 ([`Self::freeze`]) は入らずに待つ (F1: 間で消すと、 生きている印は消す前・世代は消した
    /// 後の像が disk に届く)。 世代を持たない DB (v14) は待たない (世代を上げないので像は揃っている)。
    #[inline]
    pub(crate) fn enter_free(&self) -> (bool, bool) {
        loop {
            let v = self.gate.0.fetch_add(1, Ordering::AcqRel);
            if v & GATE_FREEZE == 0 || v & GATE_ON == 0 {
                return (v & GATE_ON != 0, true);
            }
            self.gate.0.fetch_sub(1, Ordering::AcqRel);
            while self.gate.0.load(Ordering::Acquire) & GATE_FREEZE != 0 {
                std::thread::yield_now();
            }
        }
    }

    /// 本体の書き出しの 「生きている印 (entities) と世代 (`gen.seg`) を書き出す」 段 `f` を、 消す書き手を止めて走らせる。 中に居る
    /// 書き手が出るのを待ってから書き出す = 書き出す 2 つの像は、 どの削除についても 「両方前」 か 「両方後」。 世代を持たない DB は
    /// 止めない。
    pub(crate) fn freeze<R>(&self, f: impl FnOnce() -> R) -> R {
        if !self.active() {
            return f();
        }
        let _one = self.freeze_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.gate.0.fetch_or(GATE_FREEZE, Ordering::AcqRel);
        self.wait_frees();
        let r = f();
        self.gate.0.fetch_and(!GATE_FREEZE, Ordering::AcqRel);
        r
    }

    /// 消す道の出口 (生きている印を下ろした後)。
    #[inline]
    pub(crate) fn exit_free(&self) {
        self.gate.0.fetch_sub(1, Ordering::Release);
    }

    /// 印の立つ前に入った消す書き手が全部出るのを待つ (移る手順、 印を立てた後)。 中の書き手は lock を待たない短い道
    /// (生きている印を下ろすだけ) なので回って待つ。
    pub(crate) fn wait_frees(&self) {
        while self.gate.0.load(Ordering::Acquire) & GATE_COUNT != 0 {
            std::thread::yield_now();
        }
    }

    /// 場所の bit。 0 = 世代を持たない。
    #[inline]
    pub(crate) fn bits(&self) -> u32 {
        self.bits.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn active(&self) -> bool {
        self.bits() != 0
    }

    /// local → (場所, 世代)。 世代を持たない DB は (local, 0)。
    #[inline]
    pub(crate) fn split(&self, local: u32) -> (u32, u32) {
        match self.bits() {
            0 | 32.. => (local, 0),
            b => (local & ((1u32 << b) - 1), local >> b),
        }
    }

    /// 場所の今の世代 (死んだ場所は次の住人の世代)。 世代を持たない DB は 0。
    #[inline]
    pub(crate) fn get(&self, slot: u32) -> u32 {
        let Some(r) = self.region.get() else { return 0 };
        read_gen(r, slot)
    }

    /// 場所 → 今の世代の local。
    #[inline]
    pub(crate) fn local(&self, slot: u32) -> u32 {
        self.reader().local(slot)
    }

    /// 場所 → local の写しを、 場所の bit と region を 1 回だけ読んで作る (出口の loop の外で)。
    #[inline]
    pub(crate) fn reader(&self) -> GenReader<'_> {
        match self.bits() {
            0 | 32.. => GenReader { bits: 0, region: None },
            b => GenReader { bits: b, region: self.region.get() },
        }
    }

    /// page の印 (場所 `slot` の page に世代 > 0 の場所がある)。 印の範囲の外はいつも true。 (読みの道は `read_gen` / `GenReader`
    /// が同じ印を直接見る。 これは試験用)
    #[cfg(test)]
    pub(crate) fn page_has(&self, slot: u32) -> bool {
        self.region.get().is_none_or(|r| page_marked(r, slot))
    }

    fn mark_page(&self, r: &Region, slot: u32) {
        let p = slot >> PAGE_SHIFT;
        if p >= PAGES {
            return;
        }
        let off = OFF_PAGES + (p / 8) as usize;
        let bit = 1u8 << (p % 8);
        if r.as_atomic_u8(off).load(Ordering::Acquire) & bit == 0 {
            r.as_atomic_u8(off).fetch_or(bit, Ordering::AcqRel);
            r.mark_dirty(off, 1);
        }
    }

    /// きれいに閉じなかった後に開いた時: 表と page の印を、 場所ごとの世代から立て直す。 印と世代の byte は gen.seg の別の page なので、
    /// 電源断で世代の byte だけ届いた像がありうる (印が無いと、 出口がその場所の世代を読まず、 使い回した行に前の住人の eid を渡す)。
    /// 印は立てるだけ (余分な印は読む手間が増えるだけ)。 `table_of` = 場所の表。
    pub(crate) fn heal_marks(&self, table_of: impl Fn(u32) -> Option<u16>) {
        let Some(r) = self.region.get() else { return };
        let len = r.committed_len();
        if len <= GEN_HEADER {
            return;
        }
        let s = r.slice();
        for (i, &g) in s[GEN_HEADER..len].iter().enumerate() {
            if g != 0 {
                let slot = i as u32;
                self.mark_page(r, slot);
                if let Some(t) = table_of(slot) {
                    self.mark_table(t);
                }
            }
        }
    }

    /// 使える世代の最大 (2^G − 2)。 G ≤ 1 の DB は 0 = 使い回さない。
    #[inline]
    pub(crate) fn max_gen(&self) -> u32 {
        match 32 - self.bits() {
            0 | 1 => 0,
            g => (1u32 << g.min(8)) - 2,
        }
    }

    /// 引退した場所の印 (2^G − 1)。
    #[inline]
    pub(crate) fn retired_mark(&self) -> u32 {
        self.max_gen() + 1
    }

    /// 場所が引退している (世代を使い切った = 二度と渡さない) か。
    #[inline]
    pub(crate) fn retired(&self, slot: u32) -> bool {
        self.active() && self.get(slot) > self.max_gen()
    }

    /// 場所の世代を `g` 以上にする (`g` が使える世代を越えれば引退の印)。 上げたか。 書けなければ (ディスクの空き) Err。
    pub(crate) fn raise(&self, slot: u32, g: u32) -> std::io::Result<bool> {
        let Some(r) = self.region.get() else { return Ok(false) };
        let g = g.min(self.retired_mark());
        let off = GEN_HEADER + slot as usize;
        if off >= r.len() || self.get(slot) >= g {
            return Ok(false);
        }
        r.ensure_committed(off + 1)?;
        // page の印を先に (印の無い page の場所は世代を読まない。 印 → 世代 の順なら、 印を読まずに 0 と答えた読みは上げる前の読み)
        self.mark_page(r, slot);
        r.as_atomic_u8(off).fetch_max(g as u8, Ordering::AcqRel);
        r.mark_dirty(off, 1);
        Ok(true)
    }

    /// 表 `tid` に世代 > 0 の場所があるか (出口で世代を読むか)。
    #[inline]
    pub(crate) fn table_has(&self, tid: u16) -> bool {
        let Some(r) = self.region.get() else { return false };
        r.as_atomic_u8(OFF_TABLES + tid as usize / 8).load(Ordering::Acquire) & (1 << (tid % 8)) != 0
    }

    /// 表 `tid` に世代 > 0 の場所ができた (世代を上げる前に呼ぶ)。
    pub(crate) fn mark_table(&self, tid: u16) {
        let Some(r) = self.region.get() else { return };
        let off = OFF_TABLES + tid as usize / 8;
        let bit = 1u8 << (tid % 8);
        if r.as_atomic_u8(off).load(Ordering::Acquire) & bit == 0 {
            r.as_atomic_u8(off).fetch_or(bit, Ordering::AcqRel);
            r.mark_dirty(off, 1);
        }
    }
}

#[inline]
fn page_marked(r: &Region, slot: u32) -> bool {
    let p = slot >> PAGE_SHIFT;
    p >= PAGES || r.as_atomic_u8(OFF_PAGES + (p / 8) as usize).load(Ordering::Acquire) & (1 << (p % 8)) != 0
}

#[inline]
fn read_gen(r: &Region, slot: u32) -> u32 {
    if !page_marked(r, slot) {
        return 0;
    }
    let off = GEN_HEADER + slot as usize;
    if off >= r.len() {
        return 0;
    }
    r.as_atomic_u8(off).load(Ordering::Acquire) as u32
}

/// 場所 → 今の世代の local の写し ([`Generations::reader`])。 場所の bit と region を 1 回読んだ後は、 page の印と世代の byte だけ読む。
#[derive(Clone, Copy)]
pub(crate) struct GenReader<'a> {
    bits: u32,
    region: Option<&'a Region>,
}

impl GenReader<'_> {
    #[inline]
    pub(crate) fn local(&self, slot: u32) -> u32 {
        match self.region {
            Some(r) if self.bits != 0 => (read_gen(r, slot) << self.bits) | slot,
            _ => slot,
        }
    }

    /// 場所の並び → eid (出口の写し)。 256 個ずつ、 その範囲の page に印が 1 つも無ければ世代を読まずに写す (使い回しの無い所は
    /// 1 個ずつの印の読みも無い = master と同じ形の loop)。 範囲が広い (並んでいない) 塊は 1 個ずつ。
    pub(crate) fn eids(&self, peer: u32, slots: &[u32]) -> Vec<u64> {
        let plain = |s: u32| enchudb_oplog::make_eid(peer, s);
        let Some(r) = self.region.filter(|_| self.bits != 0) else {
            return slots.iter().map(|&s| plain(s)).collect();
        };
        let mut out = Vec::with_capacity(slots.len());
        for chunk in slots.chunks(256) {
            let (lo, hi) = chunk.iter().fold((u32::MAX, 0u32), |(lo, hi), &s| (lo.min(s), hi.max(s)));
            if !any_page_marked(r, lo, hi) {
                out.extend(chunk.iter().map(|&s| plain(s)));
            } else {
                out.extend(chunk.iter().map(|&s| plain((read_gen(r, s) << self.bits) | s)));
            }
        }
        out
    }
}

/// 場所 `lo..=hi` の page のどれかに印があるか (page が 64 を越える広い範囲は 「ある」 と答える = 1 個ずつ読む)。
#[inline]
fn any_page_marked(r: &Region, lo: u32, hi: u32) -> bool {
    let (p0, p1) = (lo >> PAGE_SHIFT, hi >> PAGE_SHIFT);
    if p1 >= PAGES || p1 - p0 > 64 {
        return true;
    }
    (p0..=p1).any(|p| r.as_atomic_u8(OFF_PAGES + (p / 8) as usize).load(Ordering::Acquire) & (1 << (p % 8)) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_bits_cover_max_entities() {
        assert_eq!(slot_bits_for(1), 24);
        assert_eq!(slot_bits_for(1 << 24), 24);
        assert_eq!(slot_bits_for((1 << 24) + 1), 25);
        assert_eq!(slot_bits_for(100_000_000), 27);
        assert_eq!(slot_bits_for(u32::MAX), 32);
    }

    #[test]
    fn the_top_local_stays_below_the_sentinels() {
        let mut v = vec![0u8; GEN_HEADER + 16];
        let g = Generations::off();
        let r = unsafe { Region::new(v.as_mut_ptr(), v.len()) };
        g.prepare(r, 24).unwrap();
        g.start(24);
        assert_eq!(g.max_gen(), 254);
        assert_eq!(g.retired_mark(), 255);
        let top = (g.max_gen() << 24) | ((1 << 24) - 1);
        assert!(top < u32::MAX - 1, "{top:#x}");
        assert!(g.raise(3, 1).unwrap());
        assert_eq!(g.local(3), (1 << 24) | 3);
        assert_eq!(g.split((1 << 24) | 3), (3, 1));
        assert!(!g.raise(3, 1).unwrap(), "同じ世代には上げない");
        assert!(g.raise(3, 300).unwrap());
        assert!(g.retired(3), "使える世代を越えたら引退");
    }

    #[test]
    fn a_page_without_a_raised_slot_reads_generation_zero_without_its_byte() {
        let mut v = vec![0u8; GEN_HEADER + (3 << PAGE_SHIFT)];
        let g = Generations::off();
        let r = unsafe { Region::new(v.as_mut_ptr(), v.len()) };
        g.prepare(r, 24).unwrap();
        g.start(24);
        let far = (2 << PAGE_SHIFT) + 5;
        assert!(g.raise(far, 1).unwrap());
        assert!(g.page_has(far) && !g.page_has(5), "上げた場所の page だけに印");
        // 印の無い page の byte は読まない (書き込みは試験のため直接)
        unsafe { *v.as_mut_ptr().add(GEN_HEADER + 5) = 7 };
        assert_eq!(g.get(5), 0, "印の無い page の世代の byte を読んだ");
        assert_eq!(g.local(far), (1 << 24) | far);
        // 印を消した像 (電源断で世代の byte だけ届いた) は heal_marks で立て直す
        unsafe { *v.as_mut_ptr().add(OFF_PAGES + 0) = 0 };
        g.heal_marks(|_| None);
        assert!(g.page_has(5), "世代の byte から page の印を立て直さない");
        assert_eq!(g.get(5), 7);
    }
}
