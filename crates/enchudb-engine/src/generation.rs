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

/// [`Generations::gate`] の 「消す時に上げる」 印 (最上位 bit)。 下位 32 bit = 印を見ずに消している書き手の数。
const GATE_ON: u64 = 1 << 63;
const GATE_COUNT: u64 = u32::MAX as u64;

/// `gen.seg` の先頭の header の長さ。 [`MAGIC`] / 場所の bit / 表ごとの印 (TableId は u16 = 8 KiB)。
pub(crate) const GEN_HEADER: usize = 16 * 1024;
const MAGIC: [u8; 4] = *b"GEN1";
const OFF_BITS: usize = 4;
const OFF_TABLES: usize = 64;
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
    gate: AtomicU64,
}

impl Generations {
    pub(crate) fn off() -> Self {
        Self { bits: AtomicU32::new(0), region: std::sync::OnceLock::new(), gate: AtomicU64::new(0) }
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
        self.gate.fetch_or(GATE_ON, Ordering::AcqRel);
        Ok(())
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
        self.gate.fetch_or(GATE_ON, Ordering::AcqRel);
    }

    /// 消す道の入口: (消す時に上げるか, 出る時に [`Self::exit_free`] を呼ぶか)。 v15 は読むだけ。
    #[inline]
    pub(crate) fn enter_free(&self) -> (bool, bool) {
        if self.gate.load(Ordering::Acquire) & GATE_ON != 0 {
            return (true, false);
        }
        let v = self.gate.fetch_add(1, Ordering::AcqRel);
        (v & GATE_ON != 0, true)
    }

    /// 消す道の出口 (生きている印を下ろした後)。
    #[inline]
    pub(crate) fn exit_free(&self) {
        self.gate.fetch_sub(1, Ordering::Release);
    }

    /// 印の立つ前に入った消す書き手が全部出るのを待つ (移る手順、 印を立てた後)。 中の書き手は lock を待たない短い道
    /// (生きている印を下ろすだけ) なので回って待つ。
    pub(crate) fn wait_frees(&self) {
        while self.gate.load(Ordering::Acquire) & GATE_COUNT != 0 {
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
        let off = GEN_HEADER + slot as usize;
        if off >= r.len() {
            return 0;
        }
        r.as_atomic_u8(off).load(Ordering::Acquire) as u32
    }

    /// 場所 → 今の世代の local。
    #[inline]
    pub(crate) fn local(&self, slot: u32) -> u32 {
        match self.bits() {
            0 | 32.. => slot,
            b => (self.get(slot) << b) | slot,
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
}
