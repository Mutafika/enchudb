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
//! - #970 (案 P): 生かす場所の gen.seg の page は、 生かす前に commit しておく ([`Generations::ensure_slot`]、 払い出しの全部の道。
//!   commit 済みなら atomic の load 1 回、 伸ばす時は場所の page と先回り ([`COMMIT_AHEAD_MIN`]〜[`COMMIT_AHEAD_MAX`]) まで)。 v15 に移る時は払い出し済みの
//!   場所まで ([`Generations::ensure_slots`]、 公開する前に [`Generations::commit_unpublished`])、 書き手として開く時も払い出し済みの
//!   場所まで commit し直す。 commit は縮まないので、 生かした場所の kill / raise は gen.seg を伸ばさない = ディスクの空きの不足で
//!   落ちない。 前は消す時に場所の page を commit していて、 伸ばせないと世代も 「死んでいる」 印も書けないまま場所を空け、 次の
//!   insert が消えた行と同じ eid で使い回した。 commit できない時は払い出す側 (insert / 受信の払い出し / 移る手順 / 開く) を断る。
//!   gen.seg の大きさは払い出した場所の桁 (2 B / 場所の 2 倍か + 先回りの 1 MiB まで)。 伸ばす時は伸ばす分の全部を空きの数え (#317)
//!   に入れる (後で消す時に初めて書く page も、 PR #977 の読み直し 4 の [5])。 表の予約した枠の全部は commit しない (PR #977 の
//!   読み直し 3: 枠の全部では見かけの大きさと開く時のなめが予約に比例し、 APFS は header を書いた所から 16 MiB を実ディスクにした)
//!
//! ## 生きている印 (entities) と世代 (gen.seg) は別の segment (PR #885 の読み直し F1)
//!
//! 本体の書き出しは 2 つを別々に書き出し、 OS も page を勝手な順で先に書き出す。 電源断の像では、 消す道の 2 つの書き込み (世代を
//! 上げる / 生きている印を下ろす) の片方だけが届きうる。 消す書き手を止めずに像を揃えるため、 世代と同じ 2 B に 「死んでいる」 印を置く:
//!
//! - 消す: 印と世代 (+1) を **1 回の store** で書いてから ([`Generations::kill`])、 生きている印を下ろす
//! - 使い回す / 位置から払い出す / 再生や受信で生かす: 印を落としてから生きている印を立てる。 落とすのは 「死んでいる・生きていない」
//!   と読んだ 2 B のままの時だけ ([`Generations::revive_from`] の CAS、 #901: 読んだ後に別の書き手が使い回して消し始めた場所の印を
//!   落とさない)。 払い出した自分の場所 (eid を返す前) は [`Generations::revive`]
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
/// 「きれいに閉じた後に kill / revive した」 印 (1 B)。 紐名の辞書の clean の印は WAL の無い engine では session の途中の flush でも
/// 立つ (`sync_and_mark_clean_with`) ので、 その後の kill / revive の像を開く時に突き合わせるための印。 最初の kill / revive の前に
/// 立てて header を書き出し (`Engine::mark_gen_session_dirty`)、 clean にする書き出しの前に倒す。
const OFF_DIRTY: usize = 8;
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

/// #970 (案 P): 場所を書く前の commit で、 場所の page の後ろに先回りして commit する量の下限 (3 page = 48 KiB = 24576 場所) と上限
/// (1 MiB = 524288 場所)。 間は commit 済みの長さ (header の後ろ) と同じ (×2)。 `Region::ensure_committed` の先回り (1 回 16 MiB まで)
/// は使わない: gen.seg の見かけの大きさを払い出した場所の桁 (最大で 2 倍か +1 MiB) に保つ (APFS は書いた page に隣り合う 16 MiB 未満の
/// 穴を実ディスクにする、 #400)。 上限を 1 page ずつにしない理由: 伸ばすたびに (ftruncate + mmap) 並んで払い出す書き手が全部待つ。
/// 3 page 固定では書き手 8 本の insert が master の 1.17〜1.22 倍だった (伸ばしが 8192 × 4 場所ごと)
const COMMIT_AHEAD_MIN: usize = 3 * (2 << PAGE_SHIFT);
const COMMIT_AHEAD_MAX: usize = 1 << 20;

/// 場所 `slot` の 2 B を書けるように `r` を commit する ([`Generations::ensure_slot`])。 commit 済みなら atomic の load 1 回。
#[inline]
fn commit_slot_in(r: &Region, slot: u32) -> std::io::Result<()> {
    let off = slot_off(slot);
    if off + 2 > r.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("gen.seg: slot {slot} is past the generation segment ({} bytes)", r.len()),
        ));
    }
    let page = (slot >> PAGE_SHIFT) as u64;
    let page_off = |p: u64| -> usize { (GEN_HEADER as u64 + 2 * (p << PAGE_SHIFT)).min(r.len() as u64) as usize };
    let need = page_off(page + 1).max(off + 2);
    if r.is_committed(need) {
        return Ok(());
    }
    // 空きは伸ばす分の全部を数える (PR #977 の読み直し 4 の [5])。 手前の穴 (払い出していない場所の page) と先回りの page も、 後で
    // 払い出した場所を消す時に初めて書く (block を取る) = 伸ばす時に数えておかないと、 その書き込みが #317 の数えの外になる (APFS は
    // 書き出しの時に確保、 Linux の ext4 は mmap の穴への書き込みが空きの無い時に SIGBUS)。 前は場所の page から先だけ数え、 移る時・
    // 開く時に 0 から伸ばす commit は最後の page と先回りだけだった
    let ahead = r.committed_len().saturating_sub(GEN_HEADER).clamp(COMMIT_AHEAD_MIN, COMMIT_AHEAD_MAX);
    r.ensure_committed_window((need + ahead).min(r.len()), 0)
}

/// 場所 `0..end` の 2 B を全部書けるように `r` を commit する。 範囲の外は gen.seg の終わりで切る。
fn commit_slots_in(r: &Region, end: u32) -> std::io::Result<()> {
    r.ensure_committed_window(GEN_HEADER, 0)?;
    if end == 0 || r.len() < GEN_HEADER + 2 {
        return Ok(());
    }
    let last_in_region = ((r.len() - GEN_HEADER) / 2 - 1).min(u32::MAX as usize) as u32;
    commit_slot_in(r, (end - 1).min(last_in_region))
}

pub(crate) struct Generations {
    /// 場所の bit `S`。 0 = 世代を持たない DB (local = 場所)。
    bits: AtomicU32,
    /// #899: 世代を持つ前の、 移る時に決める `S` の下限 (`slot_bits_for(今の max_entities)`。 max_entities は伸びるだけなので
    /// `S` ≥ これ)。 Ref の紐の円柱が、 移った後に場所で切ると別の値になりうる値 (2^これ 以上 = 自分の entity でない値) を見分ける
    /// (`HimoStore::put_bits`)。 engine が開く時と `grow_entity_cap` で上げる。
    floor: AtomicU32,
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
    /// header の [`OFF_DIRTY`] が立っている (この session で立てた / 開いた時に立っていた)。 立てるのは `dirty_lock` の下。
    session_dirty: std::sync::atomic::AtomicBool,
    pub(crate) dirty_lock: std::sync::Mutex<()>,
    /// 開く時の突き合わせで削除を完了させた場所 (場所, 消えた行の世代, gen.seg だけ死だったか)。 cell を落とすのは再生の後も死んで
    /// いる場所だけ (`Engine::finish_reconcile`)。
    pending: std::sync::Mutex<Vec<(u32, u32, bool)>>,
    /// 再生の後も死んでいる完了させた場所の 「消えた行の世代」 (#451 の作り直しの Delete が使う、 作り直しの後に忘れる)。
    relog_gens: std::sync::Mutex<std::collections::BTreeMap<u32, u32>>,
    has_relog_gens: std::sync::atomic::AtomicBool,
    missing_tombs: std::sync::Mutex<Vec<u32>>,
    /// 場所を使い回した時の前の住人の記録 (`prev.seg`、 `crate::prev_slots`)。 `gen.seg` と一緒に持つ (v15 の DB だけ)。
    pub(crate) prev: crate::prev_slots::PrevSlots,
    /// #979: 開く時の WAL の再生が場所の世代を上げられなかった ([`Self::note_replay_unraised`])。
    replay_unraised: std::sync::atomic::AtomicBool,
    /// #970 (PR #977 の読み直し 4 の [3]): 消す道の kill を書けなかった場所 (約束の破れの守り、 [`Self::note_unkilled`])。 この session
    /// では二度と渡さない ([`Self::retired`] が真)。 普段は空 (atomic の load 1 回)。
    unkilled: std::sync::Mutex<Vec<u32>>,
    has_unkilled: std::sync::atomic::AtomicBool,
}

impl Generations {
    pub(crate) fn off() -> Self {
        Self {
            bits: AtomicU32::new(0),
            floor: AtomicU32::new(MIN_SLOT_BITS),
            region: std::sync::OnceLock::new(),
            gate: Gate(AtomicU64::new(0)),
            move_lock: std::sync::Mutex::new(()),
            replaying: std::sync::atomic::AtomicBool::new(false),
            moved: std::sync::atomic::AtomicBool::new(false),
            repaired: [AtomicU32::new(0), AtomicU32::new(0)],
            session_dirty: std::sync::atomic::AtomicBool::new(false),
            dirty_lock: std::sync::Mutex::new(()),
            pending: std::sync::Mutex::new(Vec::new()),
            relog_gens: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            has_relog_gens: std::sync::atomic::AtomicBool::new(false),
            missing_tombs: std::sync::Mutex::new(Vec::new()),
            prev: crate::prev_slots::PrevSlots::off(),
            replay_unraised: std::sync::atomic::AtomicBool::new(false),
            unkilled: std::sync::Mutex::new(Vec::new()),
            has_unkilled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// #970 (案 P): 場所 `slot` の 2 B を書ける (この後の [`Self::kill`] / [`Self::raise`] が commit で落ちない) ように、 場所の page
    /// の終わりまで gen.seg を commit する (伸ばす時は [`COMMIT_AHEAD_MIN`]〜[`COMMIT_AHEAD_MAX`] 先回り)。 commit 済みなら atomic の load 1 回。 場所を
    /// 生かす前に呼ぶ (払い出しの道)。 commit は縮まないので、 生かした場所の kill / raise は空きの不足で落ちない。 gen.seg の無い DB
    /// (v14、 移る手順が公開する前) は何もしない。 gen.seg の範囲の外 (場所の上限の先) と、 書けない時 (ディスクの空き) は Err。
    #[inline]
    pub(crate) fn ensure_slot(&self, slot: u32) -> std::io::Result<()> {
        match self.region.get() {
            Some(r) => commit_slot_in(r, slot),
            None => Ok(()),
        }
    }

    /// 場所 `slot` の page が commit 済みか (atomic の load 1 回)。 gen.seg の無い DB は true。
    #[inline]
    pub(crate) fn slot_committed(&self, slot: u32) -> bool {
        self.region.get().is_none_or(|r| {
            let page_end = GEN_HEADER as u64 + 2 * ((u64::from(slot >> PAGE_SHIFT) + 1) << PAGE_SHIFT);
            r.is_committed((page_end.min(r.len() as u64)) as usize)
        })
    }

    /// #970: 場所 `0..end` の 2 B を全部書けるように commit する (移る手順・開く時の、 払い出し済みの範囲)。 範囲の外は gen.seg の
    /// 終わりで切る (読み直し 3 の [5]: 前は黙って何もしなかった)。 gen.seg の無い DB は何もしない。
    pub(crate) fn ensure_slots(&self, end: u32) -> std::io::Result<()> {
        match self.region.get() {
            Some(r) => commit_slots_in(r, end),
            None => Ok(()),
        }
    }

    /// [`Self::ensure_slots`] の、 まだ公開していない gen.seg 用 (移る手順の 1.: 公開する前に commit し、 できなければ何も変えずに
    /// 断る、 読み直し 3 の [4])。
    pub(crate) fn commit_unpublished(region: &Region, end: u32) -> std::io::Result<()> {
        commit_slots_in(region, end)
    }

    /// gen.seg を公開したか (v15 の DB、 または v15 に移る手順が `prepare` した後)。 公開した後に生かす場所は、 生かす前に場所の page を
    /// commit する ([`Self::ensure_slot`])。
    #[inline]
    pub(crate) fn has_region(&self) -> bool {
        self.region.get().is_some()
    }

    /// 試験用: gen.seg の commit 済みの長さ (byte)。 gen.seg の無い DB は 0。
    #[cfg(test)]
    pub(crate) fn committed_bytes(&self) -> usize {
        self.region.get().map_or(0, |r| r.committed_len())
    }

    /// #979: 開く時の WAL の再生が場所の世代を上げられなかった (gen.seg を伸ばせない) 印を立てる / 取る (開く道が見て、 開くのを断る)。
    pub(crate) fn note_replay_unraised(&self) {
        self.replay_unraised.store(true, Ordering::Release);
    }

    pub(crate) fn take_replay_unraised(&self) -> bool {
        self.replay_unraised.swap(false, Ordering::AcqRel)
    }

    /// PR #977 の読み直し 4 の [3]: 消す道が場所 `slot` の kill (世代と 「死んでいる」 印) を書けなかった (生かす前に page を commit する
    /// 約束が破れた時だけ)。 場所は今の世代のまま 「生きていない・死の印なし」 = 状態からは位置から取って断った穴と見分けられない (前の
    /// 形は状態で推して上げ直し、 払い出し中の場所を殺した = 読み直し 1 の [1])。 覚えた場所はこの session の使い回し (空きの一覧・
    /// 消した行の一覧・全部なめ) が [`Self::retired`] で飛ばす。 閉じる時にもう一度書き、 書けなければ汚れた印を残す (次に開く時の
    /// 突き合わせ = `Engine::reconcile_generations` の 2. が、 払い出し済みの範囲の 「生きていない・死の印なし」 を完了させる)。
    pub(crate) fn note_unkilled(&self, slot: u32) {
        let mut l = self.unkilled.lock().unwrap_or_else(|p| p.into_inner());
        if !l.contains(&slot) {
            l.push(slot);
        }
        self.has_unkilled.store(true, Ordering::Release);
    }

    /// kill を書けなかった場所が残っているか。
    #[inline]
    pub(crate) fn has_unkilled(&self) -> bool {
        self.has_unkilled.load(Ordering::Acquire)
    }

    #[inline]
    fn is_unkilled(&self, slot: u32) -> bool {
        self.has_unkilled() && self.unkilled.lock().unwrap_or_else(|p| p.into_inner()).contains(&slot)
    }

    /// 覚えた場所 (書き直す前の一覧)。
    pub(crate) fn unkilled_slots(&self) -> Vec<u32> {
        if !self.has_unkilled() {
            return Vec::new();
        }
        self.unkilled.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// 場所 `slot` の kill が後から書けた (覚えを外す)。
    pub(crate) fn forget_unkilled(&self, slot: u32) {
        if !self.has_unkilled() {
            return;
        }
        let mut l = self.unkilled.lock().unwrap_or_else(|p| p.into_inner());
        l.retain(|&s| s != slot);
        if l.is_empty() {
            self.has_unkilled.store(false, Ordering::Release);
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
        let dirty = s[OFF_DIRTY] != 0;
        let _ = self.region.set(region);
        self.session_dirty.store(dirty, Ordering::Release);
        self.bits.store(bits, Ordering::Release);
        self.gate.0.fetch_or(GATE_ON, Ordering::AcqRel);
        self.moved.store(true, Ordering::Release);
        Ok(())
    }

    /// header の 「きれいに閉じた後に kill / revive した」 印が立っているか (開いた時の値 / この session で立てた)。
    #[inline]
    pub(crate) fn session_dirty(&self) -> bool {
        self.session_dirty.load(Ordering::Acquire)
    }

    /// header の印を書く (`dirty_lock` の下で呼ぶ)。 立てる時は呼び手が header を書き出してから [`Self::set_session_dirty`]。
    pub(crate) fn write_dirty_flag(&self, on: bool) {
        if let Some(r) = self.region.get() {
            r.as_atomic_u8(OFF_DIRTY).store(on as u8, Ordering::Release);
            r.mark_dirty(OFF_DIRTY, 1);
        }
    }

    pub(crate) fn set_session_dirty(&self, on: bool) {
        self.session_dirty.store(on, Ordering::Release);
    }

    /// 開く時の突き合わせで削除を完了させた場所を覚える (cell は再生の後に落とす)。
    pub(crate) fn note_pending(&self, slot: u32, row_gen: u32, gen_only: bool) {
        self.pending.lock().unwrap_or_else(|p| p.into_inner()).push((slot, row_gen, gen_only));
    }

    pub(crate) fn take_pending(&self) -> Vec<(u32, u32, bool)> {
        std::mem::take(&mut *self.pending.lock().unwrap_or_else(|p| p.into_inner()))
    }

    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.lock().unwrap_or_else(|p| p.into_inner()).is_empty()
    }

    /// #451 の作り直しが使う 「消えた行の世代」 を覚える / 引く / 忘れる。
    pub(crate) fn note_relog_gen(&self, slot: u32, row_gen: u32) {
        self.relog_gens.lock().unwrap_or_else(|p| p.into_inner()).insert(slot, row_gen);
        self.has_relog_gens.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn relog_gen(&self, slot: u32) -> Option<u32> {
        if !self.has_relog_gens.load(Ordering::Acquire) {
            return None;
        }
        self.relog_gens.lock().unwrap_or_else(|p| p.into_inner()).get(&slot).copied()
    }

    /// 開く時に完了させた削除で、 消えた行の tombstone が届いていない場所 (#451 の作り直しが新しい HLC で置く)。
    pub(crate) fn note_missing_tombstone(&self, slot: u32) {
        self.missing_tombs.lock().unwrap_or_else(|p| p.into_inner()).push(slot);
    }

    pub(crate) fn take_missing_tombstones(&self) -> Vec<u32> {
        std::mem::take(&mut *self.missing_tombs.lock().unwrap_or_else(|p| p.into_inner()))
    }

    pub(crate) fn forget_relog_gens(&self) {
        self.has_relog_gens.store(false, Ordering::Release);
        self.relog_gens.lock().unwrap_or_else(|p| p.into_inner()).clear();
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

    /// v15 に移る時: `region` を 0 から初めて (前の残りは消す)、 header を書いて公開する。 まだ照合は始めない ([`Self::start`])。
    ///
    /// #970: 前に移りかけて公開した後に断った (`prepare` の後の commit が空きの不足で落ちた) 時は、 作り直さずに場所の bit だけ書き
    /// 直す (読み直し 3 の [4]: 公開した gen.seg の header を非 atomic に書き直すと、 消す道の page の印の atomic の読みと並ぶ)。 公開した
    /// 後・照合を始める前は誰も gen.seg に書かない (kill / raise / 印は照合を始めた後だけ) ので、 中身は前の `prepare` のまま。
    ///
    /// `leftover`: 前の残りのありうる長さ (移る手順が 1a で伸ばす前の file の長さ)。 その先は伸ばした所 (ftruncate の 0) なので読まない
    /// (PR #977 の読み直し 4 の [10]: 前は commit 済みの全部 = 払い出し済みの場所と先回りを読み、 1 億場所の DB なら 200 MiB の穴を移る
    /// 手順の lock を持ったまま読んだ)。
    pub(crate) fn prepare(&self, region: Region, bits: u32, leftover: usize) -> std::io::Result<()> {
        if let Some(r) = self.region.get() {
            r.write_at(OFF_BITS, &bits.to_le_bytes());
            r.mark_dirty(OFF_BITS, 4);
            return Ok(());
        }
        region.ensure_committed_window(GEN_HEADER, 0)?;
        // 0 でない 4 KiB だけ 0 に戻す (移る手順は払い出し済みの場所まで先に commit するので、 前の残りの無い所は穴のまま。 全部を 0 で
        // 埋めると疎な file の穴を全部書いて実ディスクにする)
        const CHUNK: usize = 4096;
        let used = region.committed_len().min(leftover.max(GEN_HEADER));
        let s = region.slice();
        let mut at = 0;
        while at < used {
            let end = (at + CHUNK).min(used);
            if s[at..end].iter().any(|&b| b != 0) {
                region.fill_at(at, end - at, 0);
                region.mark_dirty(at, end - at);
            }
            at = end;
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

    /// 消す道の入口: 消す時に上げるか (v15 か)。 出る時に [`Self::exit_free`] を必ず呼ぶ。 移り終えた後は呼ばない (数を待つ移る手順は
    /// もう無い。 書き手全部が 1 語を取り合わない)。
    #[inline]
    pub(crate) fn enter_free(&self) -> bool {
        self.gate.0.fetch_add(1, Ordering::AcqRel) & GATE_ON != 0
    }

    /// 払い出しの道 (生きている印を立てた後) と移る手順の揃え (PR #885 の読み直し 3a): 移り終える前は、 gate の 1 語を RMW で読む。
    /// 「消す時に上げる」 の印が立っていれば移る途中 (または移りかけて失敗した) = 移る側の死んだ場所のなめが、 生きている印を立てる前の
    /// この場所に死の印を立てたかもしれない → 呼び手は `move_lock` を待ってから死の印を落とす。 印が無ければ、 移る側の印を立てる RMW
    /// はこの RMW の後 = 移る側のなめはこの場所を生きていると読む。
    #[inline]
    pub(crate) fn claim_overlaps_move(&self) -> bool {
        !self.moved() && self.gate.0.fetch_add(0, Ordering::AcqRel) & GATE_ON != 0
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

    /// #899: 世代を持つ前の `S` の下限を `max_entities` に合わせて上げる (下げない)。
    pub(crate) fn raise_floor(&self, max_entities: u32) {
        self.floor.fetch_max(slot_bits_for(max_entities), Ordering::Release);
    }

    /// #899: 世代を持つ前の `S` の下限 ([`raise_floor`](Self::raise_floor))。
    #[inline]
    pub(crate) fn floor_bits(&self) -> u32 {
        self.floor.load(Ordering::Acquire)
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

    /// 場所の (今の世代, 「死んでいる」 印) を 2 B の 1 回の読みで ([`Self::get`] + [`Self::is_dead`] は同じ 2 B と page の印を 2 回読む)。
    /// 世代を持たない DB は (0, false)。
    #[inline]
    pub(crate) fn state(&self, slot: u32) -> (u32, bool) {
        let Some(r) = self.region.get() else { return (0, false) };
        let v = read_raw(r, slot);
        ((v & GEN_MASK) as u32, v & DEAD != 0)
    }

    /// 場所の (世代, 「死んでいる」 印) を page の印を見ずに読む (commit 済みの範囲の 2 B そのまま)。 開く時の突き合わせ
    /// (`heal`、 page の印を立て直す) より前に読む所 (`prev.seg` の記録と比べる `Engine::prev_evicted`) 用: 電源断の像では世代だけが
    /// 届いて page の印が届いていないことがある。 世代を持たない DB は (0, false)。
    pub(crate) fn state_unmarked(&self, slot: u32) -> (u32, bool) {
        let Some(r) = self.region.get() else { return (0, false) };
        let off = slot_off(slot);
        if off + 2 > r.len() || !r.is_committed(off + 2) {
            return (0, false);
        }
        let v = r.as_atomic_u16(off).load(Ordering::Acquire);
        ((v & GEN_MASK) as u32, v & DEAD != 0)
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
        let mut next = 0u32;
        while next < n {
            // 0 の並び (使い回していない場所、 先回りした所) は 4 場所 (8 B) ずつ飛ばす
            if next.is_multiple_of(4) && next + 4 <= n && r.as_atomic_u64(slot_off(next)).load(Ordering::Relaxed) == 0 {
                next += 4;
                continue;
            }
            let slot = next;
            next += 1;
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

    /// 場所が引退している (世代を使い切った = 二度と渡さない) か。 PR #977 の読み直し 4 の [3]: この session で kill を書けなかった場所
    /// ([`Self::note_unkilled`]) も渡さない。
    #[inline]
    pub(crate) fn retired(&self, slot: u32) -> bool {
        self.active() && (self.get(slot) > self.max_gen() || self.is_unkilled(slot))
    }

    /// 場所の 2 B を書く準備 (commit を確かめ、 page の印を先に立てる)。 書く場所の atomic。 #970: 生かした場所の page は生かす前に
    /// commit してある ([`Self::ensure_slot`]) ので、 ここは atomic の load で済む。 範囲の外・書けない時は Err (読み直し 3 の [5]: 前は
    /// 範囲の外を黙って書いたことにした)。
    fn cell_for_write<'r>(&self, r: &'r Region, slot: u32) -> std::io::Result<&'r AtomicU16> {
        // page の場所を全部 commit する (印の立った page の場所は、 commit を確かめずに読む = `read_raw`)
        commit_slot_in(r, slot)?;
        // page の印を先に (印の無い page の場所は世代を読まない。 印 → 世代 の順なら、 印を読まずに 0 と答えた読みは上げる前の読み)
        self.mark_page(r, slot);
        Ok(r.as_atomic_u16(slot_off(slot)))
    }

    /// 場所の世代を `g` 以上にする (`g` が使える世代を越えれば引退の印)。 「死んでいる」 印は変えない。 上げたか。 書けなければ
    /// (ディスクの空き / 範囲の外) Err。
    pub(crate) fn raise(&self, slot: u32, g: u32) -> std::io::Result<bool> {
        let Some(r) = self.region.get() else { return Ok(false) };
        let g = g.min(self.retired_mark()) as u16;
        if self.get(slot) >= g as u32 {
            return Ok(false);
        }
        let c = self.cell_for_write(r, slot)?;
        let raised = c
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| (v & GEN_MASK < g).then_some((v & !GEN_MASK) | g))
            .is_ok();
        if raised {
            r.mark_dirty(slot_off(slot), 2);
        }
        Ok(raised)
    }

    /// 行が消える: 世代を `g` 以上にし、 「死んでいる」 印を立てる (1 回の store)。 消す道はこの後に生きている印を下ろす。 書けなければ
    /// (ディスクの空き / 範囲の外) Err。 #970: 生かした場所の page は生かす前に commit してあるので、 生かした場所の kill は落ちない。
    pub(crate) fn kill(&self, slot: u32, g: u32) -> std::io::Result<()> {
        let Some(r) = self.region.get() else { return Ok(()) };
        let g = g.min(self.retired_mark()) as u16;
        let c = self.cell_for_write(r, slot)?;
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

    /// 場所の 2 B (「死んでいる」 印が立っていて、 引退していない時だけ)。 `Engine::revive_slot` が生きている印を読む **前に** 読み、
    /// [`Self::revive_from`] に渡す。 引退した場所 (世代 = 引退の印) は None (印を落とさない): 引退の印は消す道の kill が上限で止める
    /// 値なので、 使い回して消した後も同じ 2 B に戻る (CAS が変化を見分けられない唯一の値、 #901)。 引退した場所は生かさないので
    /// 印を持ったままでよい。
    #[inline]
    pub(crate) fn dead_cell(&self, slot: u32) -> Option<u16> {
        let r = self.region.get()?;
        let v = read_raw(r, slot);
        (v & DEAD != 0 && u32::from(v & GEN_MASK) <= self.max_gen()).then_some(v)
    }

    /// 場所の 2 B が `seen` ([`Self::dead_cell`] で読んだ値) のままなら 「死んでいる」 印を落とす (1 回の CAS)。 変わっていれば
    /// 何もせず false (呼び手が生きている印から読み直す)。 2 B は使い回すたびに変わり (印を落とす)、 消すたびに変わる (世代を上げる)
    /// ので、 `seen` のまま = 読んでから使い回しも消す道の kill も無い。 #901: 前は読み直して印を落としていて、 読んだ後に別の書き手が
    /// 使い回してその行を消し始める (kill の後・生きている印を下ろす前) と、 消す途中の場所の印を落とした (= 「次の世代、 印なし、
    /// 生きている」 の像)。
    pub(crate) fn revive_from(&self, slot: u32, seen: u16) -> bool {
        let Some(r) = self.region.get() else { return true };
        let off = slot_off(slot);
        if off + 2 > r.len() || !r.is_committed(off + 2) {
            return true;
        }
        let done = r
            .as_atomic_u16(off)
            .compare_exchange(seen, seen & !DEAD, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if done {
            r.mark_dirty(off, 2);
        }
        done
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
    /// 世代を持つ DB か (写した時の場所の bit)。
    #[inline]
    pub(crate) fn active(&self) -> bool {
        self.bits != 0
    }

    /// 写した時の場所の bit (読みの間に v15 に移ったかを見る)。
    #[inline]
    pub(crate) fn bits(&self) -> u32 {
        self.bits
    }

    /// local → (場所, 世代) ([`Generations::split`] を写した bit で)。
    #[inline]
    pub(crate) fn split(&self, local: u32) -> (u32, u32) {
        match self.bits {
            0 => (local, 0),
            b => (local & ((1u32 << b) - 1), local >> b),
        }
    }

    /// 場所の今の世代 ([`Generations::get`] を写した region で)。
    #[inline]
    pub(crate) fn current(&self, slot: u32) -> u32 {
        match self.region {
            Some(r) => read_gen(r, slot),
            None => 0,
        }
    }

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

    /// v15 に移る途中で落ちた像の gen.seg (前の残り) を作り直す時、 0 でない所を全部 0 に戻して header を書く。 #970: 0 でない 4 KiB
    /// だけを 0 に戻す (commit した穴を全部書かない) ので、 4 KiB の境目をまたぐ残り・最後の半端な 4 KiB の残りも戻すことを見る。
    #[test]
    fn prepare_clears_every_leftover_byte() {
        let n = 3 << PAGE_SHIFT;
        let mut v = buffer(segment_len(n));
        let len = v.len() * 8;
        {
            let base = v.as_mut_ptr() as *mut u8;
            for off in [OFF_DIRTY, OFF_PAGES + 1, slot_off(5), slot_off(2047) + 1, slot_off(2048), len - 1] {
                unsafe { *base.add(off) = 0xA5 };
            }
        }
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        let bytes = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, len) };
        assert_eq!(&bytes[..4], &MAGIC);
        assert_eq!(u32::from_le_bytes(bytes[OFF_BITS..OFF_BITS + 4].try_into().unwrap()), 24);
        let left: Vec<usize> = (OFF_BITS + 4..len).filter(|&i| bytes[i] != 0).collect();
        assert!(left.is_empty(), "前の残りを 0 に戻していない: {left:?}");
    }

    #[test]
    fn the_top_local_stays_below_the_sentinels() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
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

    /// PR #977 の読み直し 3 の [5]: gen.seg の範囲の外の場所の kill / raise は Err (前は黙って書いたことにした = 呼び手は書けたと
    /// 読んで場所を使い回しの一覧に積んだ)。 範囲の中は書ける。
    #[test]
    fn kill_and_raise_past_the_segment_are_errors() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        g.start(24);
        let past = ((v.len() * 8 - GEN_HEADER) / 2) as u32;
        assert!(g.kill(past, 1).is_err(), "範囲の外の kill を書けたことにした");
        assert!(g.raise(past, 1).is_err(), "範囲の外の raise を書けたことにした");
        assert!(g.kill(past - 1, 1).is_ok() && g.is_dead(past - 1));
    }

    /// 消す (印と世代を 1 回で) → 使い回す (印だけ落とす) → 再生が古い行を生かす (その世代に戻す)。
    #[test]
    fn kill_revive_and_restore_move_the_mark_with_the_generation() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
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

    /// #901: 生かす道は読んだ 2 B のままの時だけ印を落とす。 読んだ後に使い回して消し始めた場所 (印を落とした後に kill = 世代が
    /// 上がる) は落とさない。 引退した場所は読まない (kill が上限で止めるので、 使い回して消しても同じ 2 B に戻る)。
    #[test]
    fn revive_from_drops_the_mark_only_on_the_value_it_read() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        g.start(24);
        g.kill(5, 2).unwrap();
        let seen = g.dead_cell(5).expect("消えた場所の 2 B");
        g.revive(5); // 別の書き手が使い回す
        g.kill(5, 3).unwrap(); // その行を消し始める (生きている印はまだ)
        assert!(!g.revive_from(5, seen), "読んだ後に変わった 2 B の印を落とした");
        assert_eq!((g.get(5), g.is_dead(5)), (3, true));
        let seen = g.dead_cell(5).unwrap();
        assert!(g.revive_from(5, seen));
        assert_eq!((g.get(5), g.is_dead(5)), (3, false), "読んだままの 2 B は印だけ落とす");
        g.kill(7, 300).unwrap();
        assert_eq!(g.dead_cell(7), None, "引退した場所の印を落とす値として読んだ");
        assert_eq!(g.dead_cell(6), None, "印の無い場所");
    }

    #[test]
    fn a_page_without_a_raised_slot_reads_generation_zero_without_its_byte() {
        let mut v = buffer(segment_len(3 << PAGE_SHIFT));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
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
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        g.start(24);
        g.raise(3, 2).unwrap();
        g.kill(3, 3).unwrap(); // 世代 2 の行を消す → 3 + 死んでいる (生きている印は下ろす前)
        g.kill(4, 2).unwrap(); // 世代 1 の行を消した (生きている印も下りた)
        assert_eq!(g.heal(|s| s == 3, |_| None), vec![3]);
        assert_eq!((g.get(3), g.is_dead(3)), (3, true), "heal は世代を変えない");
    }
}
