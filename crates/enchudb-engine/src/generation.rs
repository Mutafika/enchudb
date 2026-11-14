//! #731 / #863: eid の local = **世代 | 場所** (計画は repo 直下の `EID_REUSE_PLAN.md`、 段 1)。
//!
//! 場所 (slot) = 列・生存ビットマップ・版数の列の添字 (前の local そのもの)。 消えた行の場所を使い回した行は、 世代の違う
//! eid になる。 アプリが持ち続けた古い eid は入口の照合で 「消えた行」 になる (読みは None、 書き込み / 消すは何もしない)。
//!
//! - 場所の bit `S` = max(24, max_entities の bit 数) を、 最初に場所を使い回す時に header (`H_SLOT_BITS`) に焼く。 世代の
//!   bit `G` = 32 − S。 世代は 0..=2^G − 2。 2^G − 1 は 「引退した場所」 の印 (世代を使い切った場所は二度と渡さない)。
//!   いちばん上の世代を使わないので、 local の最大は 2^32 − 2^S − 1 で、 cell の空の印 (u32::MAX) や写像の番兵に届かない
//! - 場所ごとに 2 B (`gen.seg` = 先頭 [`GEN_HEADER`] byte の header + 2 B / 場所): 下の byte = 世代、 上の byte の bit 0 = 「死んで
//!   いる」 印 ([`DEAD`])。 header には表ごとの 「世代 > 0 の場所がある」 印 (出口で、 使い回しの無い表は世代を読まない) と page
//!   ごとの印
//! - 上げる時: 世代を持たない DB (file version 14 以下) は最初の使い回しで v15 に移る ([`crate::engine::Engine`] の
//!   `move_to_generations`)。 その時に、 世代を持つ表の死んだ場所 (v14 の間に消した場所) を全部 「世代 1、 死んでいる」 にする。
//!   v15 では行を消す時に上げる (死んだ場所の値 = 次の住人の世代)。 使い回す時・位置から払い出す時は今の値で渡す
//! - 予約の表 (`_` で始まる、 `_sync_ops` の ring など) は世代を持たない (0 のまま使い回す)
//!
//! ## 生きている印 (entities) と世代 (gen.seg) は別の segment (PR #885 の読み直し F1)
//!
//! 本体の書き出しは 2 つを別々に書き出し、 OS も page を勝手な順で先に書き出す。 電源断の像では、 消す道の 2 つの書き込み (世代を
//! 上げる / 生きている印を下ろす) の片方だけが届きうる。 消す書き手を止めずに像を揃えるため、 世代と同じ 2 B に 「死んでいる」 印を置く:
//!
//! - 消す: 印と世代 (+1) を **1 回の store** で書いてから ([`Generations::kill`])、 生きている印を下ろす
//! - 使い回す / 位置から払い出す / 再生や受信で生かす: 印を落としてから ([`Generations::revive`]) 生きている印を立てる
//! - きれいに閉じなかった後に開いた時 (`Engine::reconcile_generations`、 WAL の再生の前): **死が勝つ**。 gen.seg だけ死んでいる
//!   (生きている印は残る) 場所も、 entities だけ死んでいる (印なし) 場所も、 削除を完了させる (cell を落として生きている印を下ろす /
//!   世代を上げて印を立てる)。 生き返らせる向きは無いので、 どの像でも消えた行の eid は戻らない。 「gen.seg だけ死 = 削除が届いていない
//!   → 世代を戻す」 にすると、 使い回しの像 (生きている印は使い回しの後、 gen.seg は前の削除の 「g + 1、 死んでいる」 のまま) を
//!   削除の途中と読み違え、 消えた行 (g) の eid を生きている行にする。 本体の書き出しは entities → gen.seg の順なので、 間で使い回して
//!   落ちれば OS の先行書き出しなしで出る (`tests/power_loss.rs` の F1 の試験)。 完了させる時は世代をもう 1 つ上げる (生きている
//!   印が使い回しの物なら、 その世代の eid はアプリに渡っている)。 失うのは WAL に届いていない操作だけ (届いていれば再生がやり直す:
//!   Delete は冪等、 行を生かす record は場所をその行の世代に戻す [`Generations::restore`])
//! - 順の model は `tests/loom_eid_generation_move.rs`
//!
//! 前の形 (本体の書き出しが 2 つを書き出す間、 消す書き手を止める) は、 消す 1 回の最大の待ちが msync に比例して伸びた
//! (msync 50 ms で 363〜366 ms)。

use crate::region::Region;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

/// [`Generations::gate`] の 「消す時に上げる」 印 (最上位 bit)。 下位 32 bit = 消している書き手 (`enter_free` 〜 `exit_free`) の数。
const GATE_ON: u64 = 1 << 63;
const GATE_COUNT: u64 = u32::MAX as u64;

/// 場所ごとの 2 B の 「死んでいる」 印 (上の byte の bit 0)。 下の byte が世代。
const DEAD: u16 = 1 << 8;
const GEN_MASK: u16 = 0xFF;

/// `gen.seg` の先頭の header の長さ。 [`MAGIC`] / 場所の bit / 表ごとの印 (TableId は u16 = 8 KiB) / page ごとの印。
pub(crate) const GEN_HEADER: usize = 16 * 1024;
/// 場所ごとの値が 2 B (世代 + 「死んでいる」 印) の形。 1 B だった形 (`GEN1`、 release していない) は読まない。
const MAGIC: [u8; 4] = *b"GEN2";
const OFF_BITS: usize = 4;
const OFF_TABLES: usize = 64;
/// page (2^[`PAGE_SHIFT`] 場所) ごとの 「世代 > 0 の場所がある」 印。 header の残り (8128 B = 65024 page = 2^29 場所まで)。 印の
/// 無い page の場所は世代を読まない (出口・照合で、 使い回しの無い所は gen.seg の 2 B / 場所 を読まない)。 印の範囲の外の場所は
/// いつも読む。 「死んでいる」 印は世代 > 0 と一緒にしか立たない (`kill` は page の印を先に立てる) ので、 印の無い page には死んでいる
/// 印も無い。
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

/// `gen.seg` の大きさ (header + 2 B / 場所)。
pub(crate) fn segment_len(entities: u32) -> usize {
    GEN_HEADER + 2 * entities as usize
}

/// 場所の 2 B の位置。
#[inline]
fn slot_off(slot: u32) -> usize {
    GEN_HEADER + 2 * slot as usize
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
    /// WAL の再生中 (`Engine::apply_oplog_op`)。 消す道が場所の世代を上げない (再生が record の世代で上げる)。
    pub(crate) replaying: std::sync::atomic::AtomicBool,
    /// v15 に移り終えた (`Engine::move_to_generations` の最後 / v15 の DB を開いた時)。 `active` (照合を始めた) とは別: 移る途中
    /// (照合は始めたが、 v14 の間に消した場所をまだ上げていない) に使い回しを始めると、 世代 0 の場所を前と同じ eid で渡す。
    moved: std::sync::atomic::AtomicBool,
    /// きれいに閉じなかった後に開いた時に削除を完了させた場所の数 (観測・試験用): 0 = gen.seg だけ死んでいた、 1 = entities だけ。
    repaired: [AtomicU32; 2],
}

impl Generations {
    pub(crate) fn off() -> Self {
        Self {
            bits: AtomicU32::new(0),
            region: std::sync::OnceLock::new(),
            gate: Gate(AtomicU64::new(0)),
            move_lock: std::sync::Mutex::new(()),
            replaying: std::sync::atomic::AtomicBool::new(false),
            moved: std::sync::atomic::AtomicBool::new(false),
            repaired: [AtomicU32::new(0), AtomicU32::new(0)],
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

    /// 消す道の入口: 消す時に上げるか (v15 か)。 出る時に [`Self::exit_free`] を必ず呼ぶ。
    #[inline]
    pub(crate) fn enter_free(&self) -> bool {
        self.gate.0.fetch_add(1, Ordering::AcqRel) & GATE_ON != 0
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

    /// 場所に 「死んでいる」 印が立っているか (消す道が世代と一緒に立て、 使い回す時に落とす)。
    #[inline]
    pub(crate) fn is_dead(&self, slot: u32) -> bool {
        let Some(r) = self.region.get() else { return false };
        read_raw(r, slot) & DEAD != 0
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

    /// 場所 `slot` に世代 > 0 がありうるか (page の印、 世代を持たない DB は false)。 開く時の突き合わせが印の無い page を飛ばす。
    #[inline]
    pub(crate) fn may_have_generation(&self, slot: u32) -> bool {
        self.region.get().is_some_and(|r| page_marked(r, slot))
    }

    /// 場所 `slot` の次の page の先頭の場所。
    #[inline]
    pub(crate) fn next_page_start(&self, slot: u32) -> u32 {
        ((((slot >> PAGE_SHIFT) as u64) + 1) << PAGE_SHIFT).min(u32::MAX as u64) as u32
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

    /// きれいに閉じなかった後に開いた時の 1 つ目のなめ (gen.seg の commit 済みの全部): 世代 > 0 の場所の page と表の印を立て直し、
    /// 「死んでいる」 印が立っているのに生きている場所を返す (呼び手が削除を完了させる)。 印と世代は gen.seg の別の page なので、 世代
    /// だけ届いた像がありうる (印が無いと、 出口がその場所の世代を読まず、 使い回した行に前の住人の eid を渡す)。 印は立てるだけ (余分な
    /// 印は読む手間が増えるだけ)。 `table_of` = 場所の表、 `is_live` = 生きている印。 開く道 (1 本の thread) からだけ呼ぶ。
    pub(crate) fn heal(&self, is_live: impl Fn(u32) -> bool, mut table_of: impl FnMut(u32) -> Option<u16>) -> Vec<u32> {
        let Some(r) = self.region.get() else { return Vec::new() };
        let len = r.committed_len().min(r.len());
        if len <= GEN_HEADER {
            return Vec::new();
        }
        let mut dead_but_live = Vec::new();
        let n = ((len - GEN_HEADER) / 2) as u32;
        for slot in 0..n {
            let v = r.as_atomic_u16(slot_off(slot)).load(Ordering::Relaxed);
            if v == 0 {
                continue;
            }
            self.mark_page(r, slot);
            if let Some(t) = table_of(slot) {
                self.mark_table(t);
            }
            if v & DEAD != 0 && is_live(slot) {
                dead_but_live.push(slot);
            }
        }
        dead_but_live
    }

    /// 開いた時に削除を完了させた場所を数える (観測・試験用)。 `which` 0 = gen.seg だけ死んでいた、 1 = entities だけ死んでいた。
    pub(crate) fn note_repaired(&self, which: usize, n: u32) {
        self.repaired[which].fetch_add(n, Ordering::Relaxed);
    }

    /// きれいに閉じなかった後に開いた時に削除を完了させた場所の数 (gen.seg だけ死んでいた, entities だけ死んでいた)。
    pub(crate) fn repaired(&self) -> (u32, u32) {
        (self.repaired[0].load(Ordering::Relaxed), self.repaired[1].load(Ordering::Relaxed))
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

    /// 場所の 2 B を書く準備 (commit を伸ばし、 page の印を先に立てる)。 書く場所の atomic。 範囲の外は None。
    fn cell_for_write<'r>(&self, r: &'r Region, slot: u32) -> std::io::Result<Option<&'r AtomicU16>> {
        let off = slot_off(slot);
        if off + 2 > r.len() {
            return Ok(None);
        }
        // page の場所を全部 commit する (印の立った page の場所は、 commit を確かめずに読む = `read_raw`)
        let page_end = slot_off(((slot >> PAGE_SHIFT) + 1) << PAGE_SHIFT).min(r.len());
        r.ensure_committed(page_end.max(off + 2))?;
        // page の印を先に (印の無い page の場所は世代を読まない。 印 → 世代 の順なら、 印を読まずに 0 と答えた読みは上げる前の読み)
        self.mark_page(r, slot);
        Ok(Some(r.as_atomic_u16(off)))
    }

    /// 場所の世代を `g` 以上にする (`g` が使える世代を越えれば引退の印)。 「死んでいる」 印は変えない。 上げたか。 書けなければ
    /// (ディスクの空き) Err。
    pub(crate) fn raise(&self, slot: u32, g: u32) -> std::io::Result<bool> {
        let Some(r) = self.region.get() else { return Ok(false) };
        let g = g.min(self.retired_mark()) as u16;
        if self.get(slot) >= g as u32 {
            return Ok(false);
        }
        let Some(c) = self.cell_for_write(r, slot)? else { return Ok(false) };
        let raised = c
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| (v & GEN_MASK < g).then_some((v & !GEN_MASK) | g))
            .is_ok();
        if raised {
            r.mark_dirty(slot_off(slot), 2);
        }
        Ok(raised)
    }

    /// 行が消える: 世代を `g` 以上にし、 「死んでいる」 印を立てる (1 回の store)。 消す道はこの後に生きている印を下ろす。 書けなければ
    /// (ディスクの空き) Err。
    pub(crate) fn kill(&self, slot: u32, g: u32) -> std::io::Result<()> {
        let Some(r) = self.region.get() else { return Ok(()) };
        let g = g.min(self.retired_mark()) as u16;
        let Some(c) = self.cell_for_write(r, slot)? else { return Ok(()) };
        let _ = c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
            let new = (v & !GEN_MASK) | (v & GEN_MASK).max(g) | DEAD;
            (new != v).then_some(new)
        });
        r.mark_dirty(slot_off(slot), 2);
        Ok(())
    }

    /// 場所を生かす前に 「死んでいる」 印を落とす (世代は変えない)。 生きている印を立てる前に呼ぶ (逆の順の像 = 生きている・死んで
    /// いる は、 開く時に削除が届いていないと読まれて世代が戻り、 新しい行が消えた行の eid になる)。
    #[inline]
    pub(crate) fn revive(&self, slot: u32) {
        let Some(r) = self.region.get() else { return };
        if !page_marked(r, slot) {
            return;
        }
        let off = slot_off(slot);
        if off + 2 > r.len() || !r.is_committed(off + 2) {
            return;
        }
        let c = r.as_atomic_u16(off);
        if c.load(Ordering::Acquire) & DEAD != 0 {
            c.fetch_and(!DEAD, Ordering::AcqRel);
            r.mark_dirty(off, 2);
        }
    }

    /// WAL の再生が行 (世代 `g`) を生かす record を当てる時、 場所の世代が `g` より新しい (本体が WAL より先 = その後の削除や使い回しは
    /// WAL に届いていない) なら、 場所を 「世代 `g`、 生きている」 に戻す (`Engine::replay_slot`)。 新しい世代の行は WAL に届いていない
    /// (届いていれば、 その前の Delete(g) も届いて、 この record は checkpoint の手前 = 再生しない)。 戻したか。
    pub(crate) fn restore(&self, slot: u32, g: u32) -> bool {
        let Some(r) = self.region.get() else { return false };
        let off = slot_off(slot);
        if off + 2 > r.len() || !r.is_committed(off + 2) {
            return false;
        }
        let g = g.min(self.retired_mark()) as u16;
        let done = r
            .as_atomic_u16(off)
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                (v & GEN_MASK > g).then_some((v & !(GEN_MASK | DEAD)) | g)
            })
            .is_ok();
        if done {
            r.mark_dirty(off, 2);
        }
        done
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

/// 場所の 2 B (印の無い page / 範囲の外は 0)。
#[inline]
fn read_raw(r: &Region, slot: u32) -> u16 {
    if !page_marked(r, slot) {
        return 0;
    }
    let off = slot_off(slot);
    // 印の範囲の外の場所 (2^29 以上、 page の印が無い = commit を page ごとに揃えていない) は commit を確かめる
    if off + 2 > r.len() || (slot >> PAGE_SHIFT >= PAGES && !r.is_committed(off + 2)) {
        return 0;
    }
    r.as_atomic_u16(off).load(Ordering::Acquire)
}

#[inline]
fn read_gen(r: &Region, slot: u32) -> u32 {
    (read_raw(r, slot) & GEN_MASK) as u32
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

    /// 試験用の region (u16 の atomic が揃う並びの buffer)。
    fn buffer(bytes: usize) -> Vec<u64> {
        vec![0u64; bytes.div_ceil(8)]
    }

    fn region(v: &mut [u64]) -> Region {
        unsafe { Region::new(v.as_mut_ptr() as *mut u8, v.len() * 8) }
    }

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
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24).unwrap();
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

    /// 消す (印と世代を 1 回で) → 使い回す (印だけ落とす) → 再生が古い行を生かす (その世代に戻す)。
    #[test]
    fn kill_revive_and_restore_move_the_mark_with_the_generation() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24).unwrap();
        g.start(24);
        g.kill(5, 1).unwrap();
        assert_eq!((g.get(5), g.is_dead(5)), (1, true), "消す時は世代と印を一緒に");
        g.revive(5);
        assert_eq!((g.get(5), g.is_dead(5)), (1, false), "使い回す時は印だけ落とす");
        g.kill(5, 3).unwrap();
        assert!(!g.restore(5, 3), "同じ世代には戻さない");
        assert!(g.restore(5, 2));
        assert_eq!((g.get(5), g.is_dead(5)), (2, false), "再生した行の世代に戻して生かす");
        g.kill(5, 1).unwrap();
        assert_eq!((g.get(5), g.is_dead(5)), (2, true), "世代は下げない (印だけ立てる)");
        g.kill(7, 300).unwrap();
        assert!(g.retired(7) && g.is_dead(7), "使い切った場所は引退の印 + 死んでいる");
    }

    #[test]
    fn a_page_without_a_raised_slot_reads_generation_zero_without_its_byte() {
        let mut v = buffer(segment_len(3 << PAGE_SHIFT));
        let g = Generations::off();
        g.prepare(region(&mut v), 24).unwrap();
        g.start(24);
        let far = (2 << PAGE_SHIFT) + 5;
        assert!(g.raise(far, 1).unwrap());
        assert!(g.page_has(far) && !g.page_has(5), "上げた場所の page だけに印");
        // 印の無い page の値は読まない (書き込みは試験のため直接)
        let base = v.as_mut_ptr() as *mut u8;
        unsafe { *base.add(slot_off(5)) = 7 };
        assert_eq!(g.get(5), 0, "印の無い page の世代の byte を読んだ");
        assert_eq!(g.local(far), (1 << 24) | far);
        // 印を消した像 (電源断で世代の byte だけ届いた) は heal で立て直す
        unsafe { *base.add(OFF_PAGES) = 0 };
        g.heal(|_| false, |_| None);
        assert!(g.page_has(5), "世代の byte から page の印を立て直さない");
        assert_eq!(g.get(5), 7);
    }

    /// 開く時の突き合わせの 1 つ目: 死んでいる印のある生きている場所 (削除の 印と世代 だけ届いた像 / 使い回しの生きている印だけ届いた像)
    /// を返す (呼び手が削除を完了させる)。 死んでいる場所は返さない。
    #[test]
    fn heal_reports_a_live_slot_with_the_dead_mark() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24).unwrap();
        g.start(24);
        g.raise(3, 2).unwrap();
        g.kill(3, 3).unwrap(); // 世代 2 の行を消す → 3 + 死んでいる (生きている印は下ろす前)
        g.kill(4, 2).unwrap(); // 世代 1 の行を消した (生きている印も下りた)
        assert_eq!(g.heal(|s| s == 3, |_| None), vec![3]);
        assert_eq!((g.get(3), g.is_dead(3)), (3, true), "heal は世代を変えない");
    }
}
