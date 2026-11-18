//! #731 / #863: eid の local = **世代 | 場所** (計画は repo 直下の `EID_REUSE_PLAN.md`、 段 1)。
//!
//! 場所 (slot) = 列・生存ビットマップ・版数の列の添字 (前の local そのもの)。 消えた行の場所を使い回した行は、 世代の違う
//! eid になる。 アプリが持ち続けた古い eid は入口の照合で 「消えた行」 になる (読みは None、 書き込み / 消すは何もしない)。
//!
//! - 場所の bit `S` = max(24, max_entities の bit 数) を、 最初に場所を使い回す時に header (`H_SLOT_BITS`) に焼く。 世代の
//!   bit `G` = 32 − S。 世代は 0..=2^G − 2。 2^G − 1 は 「引退した場所」 の印 (世代を使い切った場所は二度と渡さない)。
//!   いちばん上の世代を使わないので、 local の最大は 2^32 − 2^S − 1 で、 cell の空の印 (u32::MAX) や写像の番兵に届かない
//! - 場所ごとに 2 B (`gen.seg` = 先頭 [`GEN_HEADER`] byte の header + 2 B / 場所): 下の byte = 世代、 上の byte の bit 0 = 「死んで
//!   いる」 印 ([`DEAD`])、 bit 1 = 「場所の最後の行は 今 − 2」 ([`TWO_UP`]、 #1008)、 bit 2 = 「後始末待ち」 ([`PENDING`]、 #992)。
//!   bit 3 = 「場所の最後の行は使い回しの途中の行だった」 ([`MID_REUSE`])。 bit 1〜3 は開く時の突き合わせが削除を完了させる時にだけ立ち、
//!   死んでいる場所でだけ意味を持つ (magic `GEN3`)。 header には表ごとの 「世代 > 0 の場所がある」 印 (出口で、 使い回しの無い表は世代を読まない) と page
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
/// #1008 (`GEN3`、 上の byte の bit 1): 「場所の最後の行は 今 − 2」。 開く時の突き合わせが gen.seg だけ死の場所を世代を 2 つ上げて
/// 完了させた時に立てる (死んでいる場所でだけ意味を持つ)。 生かす (印を落とす) / 世代を上げる時に落とす。 前は最後の行を memory の覚え
/// (`relog_gens`) にだけ持ち、 開いた後は 今 − 1 (実在しない行) と読んだ (Delete の宛名など)
const TWO_UP: u16 = 1 << 9;
/// #992 (`GEN3`、 上の byte の bit 2): 「後始末待ち」。 開く時の突き合わせが削除を完了させた時に、 世代と死の印と同じ 1 回の store で
/// 立て、 仕上げ (残った cell を落とす・tombstone・#451 の作り直しの Delete) を disk に届けた後に落とす ([`Generations::clear_pending`])。
/// 次の open の突き合わせは、 この印の立った死んだ場所を仕上げに回す (前は後始末の覚えを memory にだけ持ち、 仕上げの前に落ちる / 断る /
/// engine を捨てると二度と走らなかった)。 生かす時に落とす
const PENDING: u16 = 1 << 10;
/// #992 の読み直し 3 の S3 / PR #1012 の残り 1 (`GEN3`、 上の byte の bit 3、 user の決定 11-18): 「場所の最後の行は使い回しの途中の行だった
/// (相手に配っていない)」。 開く時の突き合わせ 1. が使い回しの枝 (`Engine::reconcile_target` の 1・2 つ目) で場所を完了させた時に、 世代・
/// 死・後始末待ちと同じ 1 回の store で立てる。 仕上げはこの場所に tombstone を作り直さず (Delete を配らない)、 受信の書き込みは tombstone
/// の無いこの死んだ場所を死んだ行として弾く (`Engine::slot_is_dead`)。 生かす時に落とす (場所を使い回すまで残す)
const MID_REUSE: u16 = 1 << 11;

/// `gen.seg` の先頭の header の長さ。 [`MAGIC`] / 場所の bit / 表ごとの印 (TableId は u16 = 8 KiB) / page ごとの印。
pub(crate) const GEN_HEADER: usize = 16 * 1024;
/// 場所ごとの値が 2 B (世代 + 「死んでいる」 印 + 「最後の行は 今 − 2」 [`TWO_UP`] + 「後始末待ち」 [`PENDING`] + 「最後の行は使い回しの
/// 途中の行」 [`MID_REUSE`]) の形 (#992 / #1008、 release していない)。 1 B だった形 (`GEN1`) は読まない。 `GEN2` (上の 3 つの bit を使わない前の
/// 形、 その bit は 0) は読めて、
/// 書き手として開く時に `GEN3` に上げる ([`Generations::upgrade_magic`])。 `GEN2` だけを読む前の binary は `GEN3` を断る。
const MAGIC: [u8; 4] = *b"GEN3";
const MAGIC_GEN2: [u8; 4] = *b"GEN2";
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
    // 空きは伸ばす分の全部で確かめる (PR #977 の読み直し 4 の [5])。 手前の穴 (払い出していない場所の page) と先回りの page も、 後で
    // 払い出した場所を消す時に初めて書く (block を取る)。 前は場所の page から先だけで確かめ、 移る時・開く時に 0 から伸ばす commit は
    // 最後の page と先回りだけだった。
    // 読み直し 5 の [J]: 守るのは伸ばす時の確かめ (その時に伸ばす分の全部の空きがあるか) だけ。 block は取らない (穴のまま): 伸ばした後に
    // 他の書き込み・他の process が空きを使えば、 後で穴に書く時に足りないことはある (APFS は書き出しの時に確保して #317 の数えの外、
    // Linux の ext4 は空きの無い時の mmap の穴への書き込みが SIGBUS = 前からの列の疎な書き込みと同じ)。 数え (#317 の未書き出しの量) は
    // 伸ばした全部の分だけ増え、 次の gen.seg の書き出しで戻る (APFS では書き出すまでの間、 1 億場所の表が移る時なら 200 MiB 多く見積もる)
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

/// #994 [C4]: `eidalias` の別名が上書きした `.eidmap` の写像 (identity, 場所, その場所の住人の世代, 場所の削除の版数)。
type AliasOverride = ((enchudb_oplog::PeerId, u32), u32, u32, enchudb_oplog::Hlc);

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
    /// きれいに閉じなかった後に開いた時に削除を完了させた場所の数 (観測・試験用): 0 = gen.seg だけ死んでいた、 1 = entities だけ、
    /// 2 = 前の open が立てた 「後始末待ち」 を拾った (#992、 仕上げに回した)。
    repaired: [AtomicU32; 3],
    /// header の [`OFF_DIRTY`] が立っている (この session で立てた / 開いた時に立っていた)。 立てるのは `dirty_lock` の下。
    session_dirty: std::sync::atomic::AtomicBool,
    pub(crate) dirty_lock: std::sync::Mutex<()>,
    /// 開く時の突き合わせで削除を完了させた場所 (場所, 消えた行の世代)。 cell を落とすのは再生の後も死んでいる場所だけ
    /// (`Engine::finish_reconcile`)。 #992: 同じ場所は gen.seg の 「後始末待ち」 ([`PENDING`]) でも disk に残る (この覚えを失っても次の
    /// open が拾う)。
    pending: std::sync::Mutex<Vec<(u32, u32)>>,
    /// #992: 仕上げを済ませた (cell を落とし、 tombstone と作り直しを頼んだ) 場所。 開く道が書き出した後に 「後始末待ち」 を落とす
    /// ([`Self::take_finished`])。
    finished: std::sync::Mutex<Vec<u32>>,
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
    /// #994 [C4]: きれいに閉じなかった後に開いた時、 `eidalias` の別名が上書きした `.eidmap` の写像 (面) (identity → (場所, その場所の
    /// 住人の世代))。 開く道が古い世代の別名を外す時に写像を戻す (`Engine::drop_aliases_where`)。 開く道の仕上げで捨てる。
    alias_overrides: std::sync::Mutex<Vec<AliasOverride>>,
    /// #992: 「後始末待ち」 ([`PENDING`]) の立っている場所の数 (開いた時に見つけた分と、 この session で立てた / 落とした分)。
    pending_marks: AtomicU32,
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
            repaired: [AtomicU32::new(0), AtomicU32::new(0), AtomicU32::new(0)],
            session_dirty: std::sync::atomic::AtomicBool::new(false),
            dirty_lock: std::sync::Mutex::new(()),
            pending: std::sync::Mutex::new(Vec::new()),
            finished: std::sync::Mutex::new(Vec::new()),
            relog_gens: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            has_relog_gens: std::sync::atomic::AtomicBool::new(false),
            missing_tombs: std::sync::Mutex::new(Vec::new()),
            prev: crate::prev_slots::PrevSlots::off(),
            replay_unraised: std::sync::atomic::AtomicBool::new(false),
            unkilled: std::sync::Mutex::new(Vec::new()),
            has_unkilled: std::sync::atomic::AtomicBool::new(false),
            alias_overrides: std::sync::Mutex::new(Vec::new()),
            pending_marks: AtomicU32::new(0),
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

    /// 印が立っているか (取らずに見る。 PR #977 の読み直し 5 の [A]: 再生は最初の断りで止める)。
    pub(crate) fn replay_refused(&self) -> bool {
        self.replay_unraised.load(Ordering::Acquire)
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
        if region.committed_len() < GEN_HEADER || (s[..4] != MAGIC && s[..4] != MAGIC_GEN2) {
            return Err("gen.seg: not a generation segment".into());
        }
        if !region_fits_fast_reads(&region) {
            return Err("gen.seg: the region is not 2-byte aligned".into());
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

    /// #992 / #1008: `GEN2` の gen.seg の magic を `GEN3` に書き換える (書き手として開く時)。 書き換えたか (呼び手が header を書き出す)。
    /// `GEN2` は上の bit ([`TWO_UP`] / [`PENDING`] / [`MID_REUSE`]) を使わない形なので、 中身はそのまま。
    pub(crate) fn upgrade_magic(&self) -> bool {
        let Some(r) = self.region.get() else { return false };
        if r.slice()[..4] != MAGIC_GEN2 {
            return false;
        }
        r.write_at(0, &MAGIC);
        r.mark_dirty(0, 4);
        true
    }

    /// [`Self::upgrade_magic`] の取り消し (`GEN3` の header を書き出せずに開くのを断る時)。
    pub(crate) fn downgrade_magic(&self) {
        if let Some(r) = self.region.get()
            && r.slice()[..4] == MAGIC
        {
            r.write_at(0, &MAGIC_GEN2);
            r.mark_dirty(0, 4);
        }
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

    /// 開く時の突き合わせで削除を完了させた場所 (と前の open の後始末待ちの印の残る場所) を、 消えた行の世代 `row_gen` と覚える (仕上げ
    /// `finish_reconcile` が再生の後に取り出す)。
    pub(crate) fn note_pending(&self, slot: u32, row_gen: u32) {
        self.pending.lock().unwrap_or_else(|p| p.into_inner()).push((slot, row_gen));
    }

    pub(crate) fn take_pending(&self) -> Vec<(u32, u32)> {
        std::mem::take(&mut *self.pending.lock().unwrap_or_else(|p| p.into_inner()))
    }

    /// #992: 仕上げを済ませた場所を覚える / 取り出す (開く道が書き出した後に 「後始末待ち」 を落とす)。
    pub(crate) fn note_finished(&self, slot: u32) {
        self.finished.lock().unwrap_or_else(|p| p.into_inner()).push(slot);
    }

    pub(crate) fn has_finished(&self) -> bool {
        !self.finished.lock().unwrap_or_else(|p| p.into_inner()).is_empty()
    }

    pub(crate) fn take_finished(&self) -> Vec<u32> {
        std::mem::take(&mut *self.finished.lock().unwrap_or_else(|p| p.into_inner()))
    }

    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.lock().unwrap_or_else(|p| p.into_inner()).is_empty()
    }

    /// #994 [C4]: 開く時に `eidalias` の別名 `key` が `.eidmap` の写像 (場所 `slot`、 その住人の世代 `agen`、 場所の削除の版数 `tomb`) を
    /// 上書きした。
    pub(crate) fn note_alias_override(&self, key: (enchudb_oplog::PeerId, u32), slot: u32, agen: u32, tomb: enchudb_oplog::Hlc) {
        self.alias_overrides.lock().unwrap_or_else(|p| p.into_inner()).push((key, slot, agen, tomb));
    }

    /// #994 [C4]: 別名 `key` が上書きした `.eidmap` の写像 (場所, 住人の世代) を見る (取り出さない)。
    pub(crate) fn alias_override(&self, key: (enchudb_oplog::PeerId, u32)) -> Option<(u32, u32)> {
        let g = self.alias_overrides.lock().unwrap_or_else(|p| p.into_inner());
        g.iter().find(|e| e.0 == key).map(|&(_, slot, agen, _)| (slot, agen))
    }

    /// #994 [C4]: 別名 `key` が上書きした `.eidmap` の写像 (場所, 住人の世代) を取り出す。
    pub(crate) fn take_alias_override(&self, key: (enchudb_oplog::PeerId, u32)) -> Option<(u32, u32)> {
        let mut g = self.alias_overrides.lock().unwrap_or_else(|p| p.into_inner());
        if g.is_empty() {
            return None;
        }
        let i = g.iter().position(|e| e.0 == key)?;
        let (_, slot, agen, _) = g.swap_remove(i);
        Some((slot, agen))
    }

    /// PR #1006 の 3 回目の読み直し [2]: 開く時の再生が場所 `slot` を世代 `row_gen` の行で生かした (使い回しの移り目)。 その場所への
    /// 写像の覚えのうち、 面の住人が別の世代の物を取り出す (identity, 場所の削除の版数)。 写像の面の行は場所を離れた (次の住人が来た)
    /// ので、 写像はもう戻さない (戻すと次の住人がその identity の翻訳先になる)。
    pub(crate) fn take_alias_overrides_left(&self, slot: u32, row_gen: u32) -> Vec<((enchudb_oplog::PeerId, u32), enchudb_oplog::Hlc)> {
        let mut g = self.alias_overrides.lock().unwrap_or_else(|p| p.into_inner());
        if !g.iter().any(|e| e.1 == slot && e.2 != row_gen) {
            return Vec::new();
        }
        let mut out = Vec::new();
        g.retain(|&(key, s, agen, tomb)| {
            let left = s == slot && agen != row_gen;
            if left {
                out.push((key, tomb));
            }
            !left
        });
        out
    }

    /// #994 [C4]: 開く道の仕上げで、 残った上書きの覚えを捨てる (別名の方が新しかった)。
    pub(crate) fn take_alias_overrides(&self) {
        self.alias_overrides.lock().unwrap_or_else(|p| p.into_inner()).clear();
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
        if !region_fits_fast_reads(&region) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "gen.seg: the region is shorter than the header or not 2-byte aligned",
            ));
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

    /// [`Self::bits`] を Relaxed で読む (#1015: 読みの入口 `Engine::read_app_in` / `Engine::app_slot`)。 0 でない値を見ても region が見える
    /// とは限らない (region を置いた後に bit を Release で置く順と対にならない) ので、 region は [`Self::reader_for`] で Acquire で引く。
    #[inline]
    pub(crate) fn bits_relaxed(&self) -> u32 {
        self.bits.load(Ordering::Relaxed)
    }

    /// [`Self::bits_relaxed`] で読んだ 0 でない場所の bit `bits` の写し。 None なら呼び手は #885 の形 ([`Self::split`] / [`Self::active`] /
    /// [`Self::get`]、 bit を Acquire で読み直す) で照合する:
    /// - region がまだ見えない (bit を Relaxed で読んだので、 移る手順の公開と並んだ読みでは起きうる)
    /// - 場所の bit が 32 (`max_entities > 2^31`): eid に世代の bit が無い。 消した場所は引退の印 (世代 1) になるので、 世代 0 の eid
    ///   を gen.seg の 2 B と照合して消えた行を弾く (#1020 の読み直し D: 前は世代の無い写しを返して照合を飛ばし、 消えた行の eid への
    ///   書き込みが死んだ場所に通った)
    #[inline]
    pub(crate) fn reader_for(&self, bits: u32) -> Option<GenReader<'_>> {
        debug_assert!(bits != 0);
        match bits {
            32.. => None,
            b => self.region.get().map(|r| GenReader { bits: b, region: Some(r) }),
        }
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
    ///
    /// #992: 2 つ目の戻り値は 「後始末待ち」 ([`PENDING`]) の立った場所 (前の open の突き合わせが削除を完了させ、 仕上げが disk に届く前に
    /// 落ちた / 断った / engine を捨てた)。 数えを [`Self::has_pending_marks`] に足す。
    pub(crate) fn heal(
        &self,
        is_live: impl Fn(u32) -> bool,
        mut table_of: impl FnMut(u32) -> Option<u16>,
    ) -> (Vec<u32>, Vec<u32>) {
        let Some(r) = self.region.get() else { return (Vec::new(), Vec::new()) };
        let len = r.committed_len().min(r.len());
        if len <= GEN_HEADER {
            return (Vec::new(), Vec::new());
        }
        let mut dead_but_live = Vec::new();
        let mut pending = Vec::new();
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
            if v & (PENDING | MID_REUSE) != 0 && v & DEAD == 0 {
                // 印の約束 (後始末待ち / 使い回しの途中 ⇒ 死んでいる) の破れ: 印を知らない binary (`GEN2` だけを読む dev の binary、 bit 3 を
                // 知らない前の GEN3 の head) が生かす時に死の印だけを落とした。 生きている場所の印は意味を持たないので落とす (数えない。
                // 数えると 3. が生きている場所を飛ばして数えが下がらず、 DB が 「きれいに閉じた」 に戻らない、 #992 の読み直し 2 の細かい 6)
                let c = r.as_atomic_u16(slot_off(slot));
                let _ = c.compare_exchange(v, v & !(PENDING | TWO_UP | MID_REUSE), Ordering::AcqRel, Ordering::Acquire);
                r.mark_dirty(slot_off(slot), 2);
            } else if v & PENDING != 0 {
                pending.push(slot);
            }
        }
        self.pending_marks.fetch_add(pending.len() as u32, Ordering::AcqRel);
        (dead_but_live, pending)
    }

    /// 開いた時に削除を完了させた場所を数える (観測・試験用)。 `which` 0 = gen.seg だけ死んでいた、 1 = entities だけ死んでいた、
    /// 2 = 前の open が立てた 「後始末待ち」 ([`PENDING`]) を拾った (#992: 前の open の仕上げが disk に届かなかった像、 仕上げに回した)。
    pub(crate) fn note_repaired(&self, which: usize, n: u32) {
        self.repaired[which].fetch_add(n, Ordering::Relaxed);
    }

    /// きれいに閉じなかった後に開いた時に削除を完了させた場所の数 (gen.seg だけ死んでいた, entities だけ死んでいた, 後始末待ちを拾った)。
    pub(crate) fn repaired(&self) -> (u32, u32, u32) {
        (
            self.repaired[0].load(Ordering::Relaxed),
            self.repaired[1].load(Ordering::Relaxed),
            self.repaired[2].load(Ordering::Relaxed),
        )
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

    /// 世代を使い切ったか (引退の印) だけを見る ([`Self::retired`] から kill を書けなかった場所の覚えを除く)。 PR #977 の読み直し 5 の
    /// [G]: 払い出し位置から取る場所は払い出し位置の先 = 生きたことが無い (覚えた場所ではありえない) ので、 覚えの Mutex と線形探索を
    /// 通さない。
    #[inline]
    pub(crate) fn used_up(&self, slot: u32) -> bool {
        self.active() && self.get(slot) > self.max_gen()
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

    /// 場所の世代を `g` 以上にする (`g` が使える世代を越えれば引退の印)。 「死んでいる」 印と 「後始末待ち」 は変えない。 上げたら
    /// 「最後の行は 今 − 2」 ([`TWO_UP`]) を落とす (今が変わる)。 上げたか。 書けなければ (ディスクの空き / 範囲の外) Err。
    pub(crate) fn raise(&self, slot: u32, g: u32) -> std::io::Result<bool> {
        let Some(r) = self.region.get() else { return Ok(false) };
        let g = g.min(self.retired_mark()) as u16;
        if self.get(slot) >= g as u32 {
            return Ok(false);
        }
        let c = self.cell_for_write(r, slot)?;
        let raised = c
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| (v & GEN_MASK < g).then_some((v & !(GEN_MASK | TWO_UP)) | g))
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
            let raised = (v & GEN_MASK).max(g);
            // 世代が変われば 「最後の行は 今 − 2」 は成り立たない (上の bit の他は保つ: 「後始末待ち」 は仕上げが落とす)
            let keep = if raised != v & GEN_MASK { v & !(GEN_MASK | TWO_UP) } else { v & !GEN_MASK };
            let new = keep | raised | DEAD;
            (new != v).then_some(new)
        });
        r.mark_dirty(slot_off(slot), 2);
        Ok(())
    }

    /// #992 / #1008: 開く時の突き合わせが削除を完了させる: [`Self::kill`] と同じ 1 回の store で 「後始末待ち」 ([`PENDING`]) を立て、
    /// 書いた世代が場所の最後の行 `row` の 2 つ上なら 「最後の行は 今 − 2」 ([`TWO_UP`]) も立てる (でなければ落とす)。 印は引退の印で切った
    /// 後の世代で決める (#992 の読み直し 2 の S1: 引退する場所は世代を 1 つしか上げられない = 最後の行は 今 − 1)。
    ///
    /// `mid_reuse`: 場所の最後の行は使い回しの途中の行 ([`MID_REUSE`] を立てる。 偽なら落とす)。
    pub(crate) fn kill_reconciled(&self, slot: u32, g: u32, row: u32, mid_reuse: bool) -> std::io::Result<()> {
        let Some(r) = self.region.get() else { return Ok(()) };
        let g = g.min(self.retired_mark()) as u16;
        let c = self.cell_for_write(r, slot)?;
        let before = c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
            let raised = (v & GEN_MASK).max(g);
            let two_up = u32::from(raised) == row + 2;
            let marks = DEAD | PENDING | if two_up { TWO_UP } else { 0 } | if mid_reuse { MID_REUSE } else { 0 };
            let new = (v & !(GEN_MASK | TWO_UP | MID_REUSE)) | raised | marks;
            (new != v).then_some(new)
        });
        if let Ok(v) = before
            && v & PENDING == 0
        {
            self.pending_marks.fetch_add(1, Ordering::AcqRel);
        }
        r.mark_dirty(slot_off(slot), 2);
        Ok(())
    }

    /// #992: 場所の 「後始末待ち」 を落とす (仕上げを disk に届けた後)。 立っていなければ何もしない。 落としたか。
    pub(crate) fn clear_pending(&self, slot: u32) -> bool {
        let Some(r) = self.region.get() else { return false };
        let off = slot_off(slot);
        if off + 2 > r.len() || !r.is_committed(off + 2) {
            return false;
        }
        let c = r.as_atomic_u16(off);
        if c.load(Ordering::Acquire) & PENDING == 0 {
            return false;
        }
        let cleared = c.fetch_and(!PENDING, Ordering::AcqRel) & PENDING != 0;
        if cleared {
            self.unmark_pending();
        }
        r.mark_dirty(off, 2);
        cleared
    }

    /// #992 の読み直し 4 の B4: [`Self::clear_pending`] の取り消し (落とした印を書き出せなかった時)。 死んでいる場所にだけ立て直す。
    pub(crate) fn remark_pending(&self, slot: u32) {
        let Some(r) = self.region.get() else { return };
        let off = slot_off(slot);
        if off + 2 > r.len() || !r.is_committed(off + 2) {
            return;
        }
        let c = r.as_atomic_u16(off);
        if c.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| (v & DEAD != 0 && v & PENDING == 0).then_some(v | PENDING)).is_ok() {
            self.pending_marks.fetch_add(1, Ordering::AcqRel);
            r.mark_dirty(off, 2);
        }
    }

    /// #992 の読み直し 4 の B4 (A1): 場所を払い出しの候補から外すか: 引退した場所 ([`Self::retired`]) と、 後始末待ちの場所 (前の open の
    /// 突き合わせが答えを決め、 後始末が disk に届いていない。 使い回すと、 その書き出しが生きている印と gen.seg の間で切れた像が 「前の open
    /// の窓」 と同じ形になり、 次の open が世代を上げずに使い回した行の eid をもう一度渡す)。
    pub(crate) fn withheld(&self, slot: u32) -> bool {
        self.retired(slot) || self.pending_mark(slot)
    }

    /// 場所に 「最後の行は 今 − 2」 ([`TWO_UP`]) が立っているか。
    pub(crate) fn two_up_mark(&self, slot: u32) -> bool {
        self.region.get().is_some_and(|r| read_raw(r, slot) & TWO_UP != 0)
    }

    /// #992: 「後始末待ち」 の数えを 1 つ下げる (0 では止める: 開いた時に数えていない印 (きれいに閉じた後の open は `heal` を通らない) を
    /// 生かす道が落とした時、 #992 の読み直し 2 の細かい 7)。
    fn unmark_pending(&self) {
        let _ = self.pending_marks.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1));
    }

    /// #1008: 開く時の突き合わせが場所の最後の行の削除を完了させた (「最後の行は 今 − 2」 の印がある) か、 場所が引退している (使い回さない
    /// = 最後の行は消した行)。 仕上げが消えた行の tombstone を作り直すかを決める (`Engine::finish_reconcile`)。
    pub(crate) fn last_row_was_deleted(&self, slot: u32) -> bool {
        let v = self.region.get().map_or(0, |r| read_raw(r, slot));
        // #992 の読み直し 3 の S3: 引退する場所でも、 使い回しの途中の行 ([`MID_REUSE`]) は消した行でない
        (v & TWO_UP != 0 || self.used_up(slot)) && v & MID_REUSE == 0
    }

    /// 場所の最後の行は使い回しの途中の行だった ([`MID_REUSE`]、 開く時の突き合わせが立てた)。
    pub(crate) fn mid_reuse(&self, slot: u32) -> bool {
        self.region.get().is_some_and(|r| read_raw(r, slot) & MID_REUSE != 0)
    }

    /// #992 の読み直し 3 の B3: 場所に 「後始末待ち」 が立っているか (前の open の突き合わせが答えを決めた)。
    pub(crate) fn pending_mark(&self, slot: u32) -> bool {
        self.region.get().is_some_and(|r| read_raw(r, slot) & PENDING != 0)
    }

    /// #992 の読み直し 2 の B2: 場所の 2 B の gen.seg の中の範囲 (書き出す範囲)。
    pub(crate) fn slot_range(slot: u32) -> (usize, usize) {
        (slot_off(slot), 2)
    }

    /// #992: 「後始末待ち」 の立った場所が残っているか (開いた時に見つけた分 + この session で立てた分 − 落とした分)。 残っている間は
    /// gen.seg の 「きれいに閉じた後に kill / revive した」 印を倒さない (次の open が突き合わせて拾う)。
    #[inline]
    pub(crate) fn has_pending_marks(&self) -> bool {
        self.pending_marks.load(Ordering::Acquire) != 0
    }

    /// #1008: 死んだ場所の最後の行の世代 (`now` = 場所の今の世代)。 「最後の行は 今 − 2」 の印があれば 今 − 2、 なければ 今 − 1。
    #[inline]
    pub(crate) fn dead_row_gen(&self, slot: u32, now: u32) -> u32 {
        let two_up = self.region.get().is_some_and(|r| read_raw(r, slot) & TWO_UP != 0);
        now.saturating_sub(if two_up { 2 } else { 1 })
    }

    /// [`Self::dead_row_gen`] の、 page の印を見ずに読む形 (開く時の突き合わせが印を立て直す前、 [`Self::state_unmarked`] と同じ)。
    pub(crate) fn dead_row_gen_unmarked(&self, slot: u32, now: u32) -> u32 {
        let two_up = self.region.get().is_some_and(|r| {
            let off = slot_off(slot);
            off + 2 <= r.len() && r.is_committed(off + 2) && r.as_atomic_u16(off).load(Ordering::Acquire) & TWO_UP != 0
        });
        now.saturating_sub(if two_up { 2 } else { 1 })
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
            // 生かす = 死んだ場所の印 (最後の行は 今 − 2・後始末待ち・使い回しの途中) も意味を失う
            if c.fetch_and(!(DEAD | TWO_UP | PENDING | MID_REUSE), Ordering::AcqRel) & PENDING != 0 {
                self.unmark_pending();
            }
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
            .compare_exchange(seen, seen & !(DEAD | TWO_UP | PENDING | MID_REUSE), Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if done {
            if seen & PENDING != 0 {
                self.unmark_pending();
            }
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
        let before = r.as_atomic_u16(off).fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
            (v & GEN_MASK > g).then_some((v & !(GEN_MASK | DEAD | TWO_UP | PENDING | MID_REUSE)) | g)
        });
        if let Ok(v) = before {
            if v & PENDING != 0 {
                self.unmark_pending();
            }
            r.mark_dirty(off, 2);
        }
        before.is_ok()
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

/// region が照合の速い道の読み ([`read_raw`] / [`GenReader::eids`] の範囲を確かめない読み) の前提を満たすか: header (page の印) が
/// 全部 region の中にあり、 先頭が 2 byte に揃っている。 [`Generations::attach`] / [`Generations::prepare`] が region を置く前に
/// 確かめる (置いた region は変わらない)。
fn region_fits_fast_reads(r: &Region) -> bool {
    r.len() >= GEN_HEADER && r.base_aligned(2)
}

/// page `p` (< [`PAGES`]) の印。
#[inline(always)]
fn page_bit(r: &Region, p: u32) -> bool {
    page_bit_with(r, p, Ordering::Acquire)
}

#[inline(always)]
fn page_bit_with(r: &Region, p: u32, order: Ordering) -> bool {
    debug_assert!(p < PAGES);
    // SAFETY: p < PAGES なので OFF_PAGES + p / 8 < GEN_HEADER <= r.len() (`region_fits_fast_reads`、 置いた region だけを読む)
    unsafe { r.as_atomic_u8_unchecked(OFF_PAGES + (p / 8) as usize) }.load(order) & (1 << (p % 8)) != 0
}

#[inline]
fn page_marked(r: &Region, slot: u32) -> bool {
    let p = slot >> PAGE_SHIFT;
    p >= PAGES || page_bit(r, p)
}

/// 場所の 2 B を範囲を確かめずに読む。
///
/// # Safety
/// `slot_off(slot) + 2 <= r.len()`、 `r` は置いた region (`region_fits_fast_reads`)。
#[inline(always)]
unsafe fn load_cell_with(r: &Region, slot: u32, order: Ordering) -> u16 {
    // SAFETY: 範囲は呼び手が確かめる。 slot_off は偶数で region の先頭は 2 byte に揃う
    unsafe { r.as_atomic_u16_unchecked(slot_off(slot)) }.load(order)
}

/// 場所の 2 B (印の無い page / 範囲の外は 0)。
///
/// #1015: get / 書き込みの入口 / 出口のたびに通る。 page の印 (header の中) と場所の 2 B を範囲の確かめ 1 回で読む (前は印と 2 B の
/// それぞれに範囲と揃いの確かめ、 その panic の文の引数を hot path で stack に積んでいた)。
#[inline(always)]
fn read_raw(r: &Region, slot: u32) -> u16 {
    read_raw_with(r, slot, Ordering::Acquire)
}

/// [`read_raw`] の、 page の印と場所の 2 B の load の順 (`order`、 両方に使う) を選ぶ版。 印の範囲の外 ([`read_raw_far`]) は
/// いつも Acquire。
#[inline(always)]
fn read_raw_with(r: &Region, slot: u32, order: Ordering) -> u16 {
    let p = slot >> PAGE_SHIFT;
    if p >= PAGES {
        return read_raw_far(r, slot);
    }
    if !page_bit_with(r, p, order) {
        return 0;
    }
    if slot_off(slot) + 2 > r.len() {
        return 0;
    }
    // SAFETY: 範囲は直前に確かめた
    unsafe { load_cell_with(r, slot, order) }
}

/// 印の範囲の外の場所 (2^29 以上、 page の印が無い = commit を page ごとに揃えていない) は commit を確かめて読む。
#[cold]
#[inline(never)]
fn read_raw_far(r: &Region, slot: u32) -> u16 {
    let off = slot_off(slot);
    if off + 2 > r.len() || !r.is_committed(off + 2) {
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

    /// [`Self::current`] の、 page の印と場所の 2 B を Relaxed で読む版。 lock を取らない読み (`Engine::read_app_in`) の、 世代 0 の eid の
    /// 読む前の照合と、 読んだ後の照合で使う:
    /// - 読んだ後の照合: 順は読みの後の fence か、 列の cell の Acquire の load が付ける (使い回した後の住人の値を読んだ読みは、 その値を
    ///   書いた書き手と同期するので、 後の照合で消す道の印と世代の書きを見る)
    /// - 世代 0 の eid の読む前の照合: 世代 0 の行には前の住人が居ない (v15 に移るのは最初の使い回しの時、 v14 の間に消した場所は移る
    ///   時に世代 1 にする) ので、 順は要らない
    ///
    /// 世代 ≠ 0 の eid の読む前の照合には使わない (#1020 の読み直し A): その eid を同期の無い渡し方で受けた読み手が、 2 B の新しい
    /// 世代を Relaxed で読んで通ると、 cell の load は前の住人の値 (消す道が落とす前の値) を読んでよく、 読んだ後の照合も通る。
    /// [`Self::current`] (Acquire) で読めば、 2 B を書いた消す道 / 使い回す道の RMW と同期し、 その前に落とした cell が見える。
    /// #1015: Acquire で読むより get 1 回が 0.7〜1.3 ns 速い (M4 Max、 100 万行)。
    #[inline]
    pub(crate) fn current_relaxed(&self, slot: u32) -> u32 {
        match self.region {
            Some(r) => (read_raw_with(r, slot, Ordering::Relaxed) & GEN_MASK) as u32,
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

    /// 場所の並び → eid (出口の写し)。 256 個ずつ、 その範囲の page に印が 1 つも無ければ世代を読まない (使い回しの無い所は 1 個ずつの
    /// 印の読みも無い)。 全部の page に印のある塊は印を見ずに 2 B を読み、 印が混ざる塊は塊で 1 回読んだ印の並びで印のある page の
    /// 場所だけ 2 B を読む。 範囲が広い (並んでいない) 塊は 1 個ずつ。 2 B を読んだら返す前に fence(Acquire) (返した eid の後の読みは、
    /// 付けた世代を書いた書き手の前の書きを見る)。
    pub(crate) fn eids(&self, peer: u32, slots: &[u32]) -> Vec<u64> {
        // #1015: 先に世代 0 の eid で全部を写し (この写しは vector の命令になる)、 印のある塊だけ世代の bit を足す。 前は塊ごとに
        // `extend` していて、 使い回しの無い所の写しも 1 個ずつになり、 1 行使い回した表の pull_raw (1 万件) が 2 倍かかった。
        // eid = peer << 32 | 世代 << S | 場所 で、 場所は S bit に収まるので、 世代の bit は足すだけ (`(g << S) | s` と同じ値)
        let mut out: Vec<u64> = slots.iter().map(|&s| enchudb_oplog::make_eid(peer, s)).collect();
        let Some(r) = self.region.filter(|_| self.bits != 0) else {
            return out;
        };
        let bits = self.bits;
        // 場所の 2 B は Relaxed で読み、 2 B を読んだ塊があれば写し終えた後に fence(Acquire) を 1 回置く。 出口が付けるのは 「読んだ時の
        // 今の世代」 で、 並んで消した / 使い回した書き手とはどちらの世代でもよい (前と同じ)。 ただし新しい世代を付けて返した eid は、
        // その 2 B を書いた消す道 / 使い回す道と同期していないと、 呼び手がその eid で読む行に前の住人の値が見えうる (#1020 の読み直し
        // A。 #885 の形は 2 B を Acquire で読んでいて、 出口がこの同期を渡していた)。 Relaxed の load の後の fence(Acquire) は、 読んだ
        // 値を書いた Release (RMW) と同期する (load ごとの Acquire と同じ同期を、 返した後の読み全部に付ける)。 load ごとの Acquire は
        // 全部の page に印のある表の pull_raw (1 万件) が 1.1 倍かかった。 page の印は塊ごとに Acquire で読む (印を見た page の 2 B は、
        // その後の読み = 印を立てた書き手が commit した後)
        let mut read_any = false;
        for (chunk, o) in slots.chunks(256).zip(out.chunks_mut(256)) {
            let (lo, hi) = chunk.iter().fold((u32::MAX, 0u32), |(lo, hi), &s| (lo.min(s), hi.max(s)));
            let m = pages_marked(r, lo, hi);
            read_any |= m.is_none_or(|m| m.bits != 0);
            match m {
                Some(m) if m.bits == 0 => {}
                // 範囲の page が全部印付き (使い回しの多い表): 場所の 2 B を印を見ずに読む (範囲の確かめは塊に 1 回)。 印の立った page の
                // 場所は commit 済み (`cell_for_write` が印より先に commit する)
                Some(m) if m.bits == m.full && slot_off(hi) + 2 <= r.len() => {
                    // 先頭は loop の外で 1 回だけ読む (`r` を通すと out への store の後に毎回読み直す)
                    let cells = r.base_ptr().wrapping_add(GEN_HEADER) as *const AtomicU16;
                    let hi_peer = u64::from(peer) << 32;
                    for (e, &s) in o.iter_mut().zip(chunk) {
                        // SAFETY: s <= hi で、 slot_off(hi) + 2 <= r.len() を直前に確かめた。 region の先頭は 2 byte に揃う
                        // (`region_fits_fast_reads`)、 GEN_HEADER は偶数
                        let g = u32::from(unsafe { &*cells.add(s as usize) }.load(Ordering::Relaxed) & GEN_MASK);
                        *e = hi_peer | u64::from((g << bits) | s);
                    }
                }
                // 印のある page と無い page が混ざる (使い回しの少ない表、 1 行使い回しただけでも塊の範囲に入る): 塊で 1 回読んだ印の並びで、
                // 印のある page の場所だけ 2 B を読む (前は場所ごとに page の印の byte を読み直していた)
                Some(m) if slot_off(hi) + 2 <= r.len() => {
                    for (e, &s) in o.iter_mut().zip(chunk) {
                        if m.has(s) {
                            // SAFETY: s <= hi で、 slot_off(hi) + 2 <= r.len() を直前に確かめた
                            let g = u32::from(unsafe { load_cell_with(r, s, Ordering::Relaxed) } & GEN_MASK);
                            *e |= u64::from(g << bits);
                        }
                    }
                }
                _ => {
                    for (e, &s) in o.iter_mut().zip(chunk) {
                        let g = u32::from(read_raw_with(r, s, Ordering::Relaxed) & GEN_MASK);
                        *e |= u64::from(g << bits);
                    }
                }
            }
        }
        if read_any {
            std::sync::atomic::fence(Ordering::Acquire);
        }
        out
    }
}

/// [`pages_marked`] の答え: page `p0..=p1` の印の並び (bit i = page p0 + i)。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct PageMarks {
    p0: u32,
    /// 印の並び
    bits: u128,
    /// 範囲の page が全部印付きの時の並び
    full: u128,
}

impl PageMarks {
    /// 場所 `s` (範囲の中) の page に印があるか。
    #[inline(always)]
    fn has(&self, s: u32) -> bool {
        (self.bits >> ((s >> PAGE_SHIFT) - self.p0)) & 1 != 0
    }
}

/// 場所 `lo..=hi` の page の印。 範囲が広い (page が 64 を越える) / 印の範囲の外は None (1 個ずつ読む)。
#[inline]
fn pages_marked(r: &Region, lo: u32, hi: u32) -> Option<PageMarks> {
    let (p0, p1) = (lo >> PAGE_SHIFT, hi >> PAGE_SHIFT);
    if p1 >= PAGES || p1 - p0 > 64 {
        return None;
    }
    // 印は 1 byte に 8 page。 byte ごとに読んで並びに入れる (#1015: page ごとに 1 byte ずつ読むと、 1000 件の pull で塊ごとに 30 page
    // ほどを読んでいた)。 page p は bit p − p0 (p − p0 <= 64 < 128)
    let mut bits = 0u128;
    for b in p0 / 8..=p1 / 8 {
        // SAFETY: b <= p1 / 8 < PAGES / 8 なので OFF_PAGES + b < GEN_HEADER <= r.len() (`region_fits_fast_reads`)
        let m = u128::from(unsafe { r.as_atomic_u8_unchecked(OFF_PAGES + b as usize) }.load(Ordering::Acquire));
        let first = b * 8;
        bits |= if first >= p0 { m << (first - p0) } else { m >> (p0 - first) };
    }
    let full = (1u128 << (p1 - p0 + 1)) - 1;
    Some(PageMarks { p0, bits: bits & full, full })
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
        let _ = g.heal(|_| false, |_| None);
        assert!(g.page_has(5), "世代の byte から page の印を立て直さない");
        assert_eq!(g.get(5), 7);
    }

    /// #1015: 塊の印の並び (`pages_marked`) は、 byte の境目をまたぐ範囲でも範囲の page の印だけを、 page の順に並べる。 page が 64 を
    /// 越える範囲と印の範囲の外は並べない (None = 1 個ずつ読む)。
    #[test]
    fn pages_marked_looks_only_at_the_pages_in_range() {
        let pages = 70;
        let mut v = buffer(segment_len(pages << PAGE_SHIFT));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        g.start(24);
        let r = g.region.get().unwrap();
        let at = |p: u32| p << PAGE_SHIFT;
        // 印: page 7 と 8 (byte の境目の両側)、 20..=23
        for p in [7, 8, 20, 21, 22, 23] {
            g.raise(at(p) + 1, 1).unwrap();
        }
        let shape = |lo: u32, hi: u32| match pages_marked(r, lo, hi) {
            None => "wide",
            Some(m) if m.bits == 0 => "none",
            Some(m) if m.bits == m.full => "all",
            Some(_) => "some",
        };
        let bits = |lo: u32, hi: u32| pages_marked(r, lo, hi).map(|m| (m.p0, m.bits, m.full));
        assert_eq!(shape(0, at(6) + 5), "none");
        assert_eq!(shape(at(7), at(8) + 100), "all", "byte の境目をまたぐ全部印");
        assert_eq!(shape(at(7) + 3, at(7) + 9), "all", "1 page の中");
        assert_eq!(shape(at(6), at(7)), "some");
        assert_eq!(shape(at(8), at(9)), "some");
        assert_eq!(shape(at(9), at(19) + 8191), "none", "byte をまたいで印なし");
        assert_eq!(shape(at(20), at(23) + 8191), "all");
        assert_eq!(shape(at(16), at(23)), "some", "同じ byte の印の無い page を含む");
        assert_eq!(shape(at(24), at(39)), "none");
        assert_eq!(shape(0, at(64)), "some");
        assert_eq!(shape(at(24), at(24 + 64)), "none", "64 page の幅は読む");
        assert_eq!(shape(at(1), at(66)), "wide", "64 page を越える幅は 1 個ずつ");
        assert_eq!(shape(at(PAGES - 1), at(PAGES)), "wide", "印の範囲の外");
        // 印の並び (bit i = page p0 + i): byte の途中から始まる範囲・byte をまたぐ範囲
        assert_eq!(bits(at(6), at(9)), Some((6, 0b0110, 0b1111)));
        assert_eq!(bits(at(16), at(23) + 1), Some((16, 0b1111_0000, 0xFF)));
        assert_eq!(bits(at(5) + 9, at(21)), Some((5, (1 << 2) | (1 << 3) | (1 << 15) | (1 << 16), (1 << 17) - 1)));
        let m = pages_marked(r, 0, at(64)).unwrap();
        assert_eq!(m.full, (1u128 << 65) - 1);
        for p in 0..=64u32 {
            assert_eq!(m.has(at(p) + 77), [7, 8, 20, 21, 22, 23].contains(&p), "page {p}");
        }
    }

    /// #1015: 出口の写し (`eids`) は、 塊の印の形 (無い / 全部 / 混ざる / 広い・並んでいない) によらず、 1 個ずつ `local` で写した eid と
    /// 同じ (印の無い page の 2 B は読まない)。 lock を取らない読みの照合の読み (`current_relaxed`) は `current` と同じ値。
    #[test]
    fn eids_match_the_per_slot_local_for_every_mark_shape() {
        let n: u32 = 70 << PAGE_SHIFT;
        let mut v = buffer(segment_len(n));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        g.start(24);
        let at = |p: u32| p << PAGE_SHIFT;
        // page 3..=5 は全部に印 (場所は 3 つに 1 つだけ世代 > 0)、 page 10 は 1 場所、 page 69 は 1 場所
        for s in (at(3)..at(6)).filter(|s| s % 3 == 0) {
            g.raise(s, s % 200 + 1).unwrap();
        }
        g.raise(at(10) + 17, 5).unwrap();
        g.raise(at(69) + 1, 254).unwrap();
        // 印の無い page (11) に残った 2 B (電源断で世代の byte だけ届いた像、 開く時の heal が印を立て直すまでは読まない) は、 印の
        // 混ざる塊でも世代 0 と読む (`local` と同じ)
        g.region.get().unwrap().as_atomic_u16(slot_off(at(11) + 5)).store(9, Ordering::Relaxed);
        let r = g.reader();
        let peer = 7;
        let mut wide: Vec<u32> = (0..n).step_by(4099).collect();
        wide.reverse();
        let shapes: Vec<(&str, Vec<u32>)> = vec![
            ("印なし", (0..at(3)).step_by(37).collect()),
            ("全部印", (at(3)..at(6)).step_by(5).collect()),
            ("全部印の境目", (at(3)..at(3) + 300).collect()),
            ("混ざる", (at(9)..at(12)).step_by(11).collect()),
            ("混ざる (印の無い page の 2 B)", (at(10)..at(10) + 128).chain(at(11)..at(11) + 128).collect()),
            ("広い", wide),
            ("1 個", vec![at(10) + 17]),
            ("空", vec![]),
            ("終わりの page", (at(69)..n).step_by(3).collect()),
        ];
        for (name, slots) in shapes {
            let want: Vec<u64> = slots.iter().map(|&s| enchudb_oplog::make_eid(peer, r.local(s))).collect();
            assert_eq!(r.eids(peer, &slots), want, "{name}");
            for &s in &slots {
                assert_eq!(r.current_relaxed(s), r.current(s), "{name}: 場所 {s}");
            }
        }
        // 世代を持たない写し (region の無い Generations) は場所のまま
        let off = Generations::off();
        assert!(off.reader_for(24).is_none(), "region の無い写しを返した");
        assert_eq!(off.reader().eids(peer, &[1, 2]), vec![enchudb_oplog::make_eid(peer, 1), enchudb_oplog::make_eid(peer, 2)]);
    }

    /// #1015 (#1020 の読み直し): 印の範囲の外 (場所 ≥ 2^29、 page の印が無い) の場所は、 速い道 (`current_relaxed` / `eids`) でも
    /// commit を確かめる道 (`read_raw_far`) で 2 B を読む (header の外を page の印として読まない)。 region は 1 GiB を越えるので
    /// MAP_NORESERVE の匿名の mmap (触った page だけが実体になる)。
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[test]
    fn slots_past_the_page_marks_read_their_own_generation() {
        struct Unmap(*mut libc::c_void, usize);
        impl Drop for Unmap {
            fn drop(&mut self) {
                unsafe { libc::munmap(self.0, self.1) };
            }
        }
        let first = PAGES << PAGE_SHIFT;
        let len = segment_len(first + (2 << PAGE_SHIFT));
        let flags = libc::MAP_PRIVATE | libc::MAP_ANON | libc::MAP_NORESERVE;
        let ptr = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, flags, -1, 0) };
        assert_ne!(ptr, libc::MAP_FAILED, "前提: 匿名の mmap");
        let _unmap = Unmap(ptr, len);
        let g = Generations::off();
        // 場所 2^29 より先を持つ場所の bit は 30 (使える世代は 1..=2)
        g.prepare(unsafe { Region::new(ptr as *mut u8, len) }, 30, 0).unwrap();
        g.start(30);
        let far = [first, first + 1, first + (1 << PAGE_SHIFT) - 1, first + (1 << PAGE_SHIFT) + 77];
        for (i, &s) in far.iter().enumerate() {
            assert!(g.raise(s, i as u32 % 2 + 1).unwrap());
        }
        g.raise(first - 3, 2).unwrap(); // 印の範囲の最後の page
        let r = g.reader();
        for (i, &s) in far.iter().enumerate() {
            assert_eq!(r.current(s), i as u32 % 2 + 1, "前提: 場所 {s}");
            assert_eq!(r.current_relaxed(s), r.current(s), "印の範囲の外の場所 {s} の世代 (照合)");
        }
        assert_eq!(r.current_relaxed(first + 2), 0);
        let peer = 3;
        let slots: Vec<u32> = (first - 8..first + 8).chain(far).collect();
        let want: Vec<u64> = slots.iter().map(|&s| enchudb_oplog::make_eid(peer, r.local(s))).collect();
        assert_eq!(r.eids(peer, &slots), want, "印の範囲の外の場所の出口");
        assert_eq!(r.eids(peer, &far), far.iter().map(|&s| enchudb_oplog::make_eid(peer, r.local(s))).collect::<Vec<_>>());
    }

    /// #1015: 照合の速い道は page の印と場所の 2 B を範囲を確かめずに読むので、 header より短い / 2 byte に揃っていない region は置かない
    /// (作る道 `prepare` も開く道 `attach` も。 開く道は magic を確かめた後、 #1012 の `GEN2` → `GEN3` の書き換えより前)。
    #[test]
    fn a_short_or_misaligned_region_is_refused() {
        let mut v = buffer(segment_len(16) + 8);
        let base = v.as_mut_ptr() as *mut u8;
        let g = Generations::off();
        let odd = unsafe { Region::new(base.add(1), segment_len(16)) };
        assert!(g.prepare(odd, 24, usize::MAX).is_err(), "揃っていない region を置いた");
        let short = unsafe { Region::new(base, GEN_HEADER - 2) };
        assert!(g.prepare(short, 24, usize::MAX).is_err(), "header より短い region を置いた");
        assert!(!g.has_region());
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        assert!(g.has_region());
        // 開く道 (`attach`): magic (GEN3 / GEN2) の合う header でも、 揃っていない region は置かない。 置かなかった region の magic は
        // `upgrade_magic` が書き換えない (GEN2 のまま)
        for magic in [MAGIC, MAGIC_GEN2] {
            let mut w = buffer(segment_len(16) + 8);
            let base = w.as_mut_ptr() as *mut u8;
            let header = |at: *mut u8| unsafe {
                std::ptr::copy_nonoverlapping(magic.as_ptr(), at, 4);
                std::ptr::copy_nonoverlapping(24u32.to_le_bytes().as_ptr(), at.add(OFF_BITS), 4);
            };
            header(unsafe { base.add(1) });
            let g = Generations::off();
            assert!(g.attach(unsafe { Region::new(base.add(1), segment_len(16)) }, 24).is_err(), "揃っていない {magic:?} の region を置いた");
            assert!(!g.has_region() && !g.upgrade_magic(), "置かなかった region の magic を書き換えた");
            assert_eq!(unsafe { std::slice::from_raw_parts(base.add(1), 4) }, &magic);
            header(base);
            assert!(g.attach(region(&mut w), 24).is_ok(), "揃った {magic:?} の region");
            assert_eq!(g.upgrade_magic(), magic == MAGIC_GEN2);
        }
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
        assert_eq!(g.heal(|s| s == 3, |_| None).0, vec![3]);
        assert_eq!((g.get(3), g.is_dead(3)), (3, true), "heal は世代を変えない");
    }

    /// #992 / #1008: 突き合わせの完了は 「後始末待ち」 と (2 つ上げた時) 「最後の行は 今 − 2」 を世代と死の印と同じ store で立てる。
    /// 開く時のなめ (`heal`) が後始末待ちを拾う。 後始末を落とすのは `clear_pending` だけで、 生かす道 (`revive` / `revive_from` /
    /// `restore`) は両方を落とす。 世代を上げる道 (`kill` / `raise`) は 「今 − 2」 を落として後始末待ちを保つ。
    #[test]
    fn reconciled_marks_live_until_the_slot_is_finished_or_revived() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        g.start(24);
        // 場所 3: gen.seg だけ死 (世代 2 = 世代 1 の行を消す途中) を 2 つ上げて完了
        g.kill(3, 2).unwrap();
        g.kill_reconciled(3, 3, 1, false).unwrap();
        assert_eq!((g.get(3), g.is_dead(3), g.dead_row_gen(3, 3)), (3, true, 1), "2 つ上げた場所の最後の行は 今 − 2");
        assert!(g.has_pending_marks());
        // 場所 4: entities だけ死を 1 つ上げて完了
        g.kill_reconciled(4, 1, 0, false).unwrap();
        assert_eq!(g.dead_row_gen(4, 1), 0);
        // 開き直した形: 数えを 0 に戻してなめる
        g.pending_marks.store(0, Ordering::Release);
        let (_, pending) = g.heal(|_| false, |_| None);
        assert_eq!(pending, vec![3, 4], "後始末待ちを拾わない");
        // 同じ世代の kill (再生の Delete) は印を保つ。 世代を上げる kill は 「今 − 2」 を落として後始末待ちを保つ
        g.kill(3, 2).unwrap();
        assert_eq!(g.dead_row_gen(3, 3), 1, "世代の変わらない kill が 「今 − 2」 を落とした");
        g.kill(3, 4).unwrap();
        assert_eq!(g.dead_row_gen(3, 4), 3, "世代を上げた kill が 「今 − 2」 を残した");
        g.clear_pending(3);
        assert!(g.has_pending_marks(), "前提: 場所 4 は残る");
        // 生かす道は後始末待ちを落とす
        let seen = g.dead_cell(4).unwrap();
        assert!(g.revive_from(4, seen));
        assert!(!g.has_pending_marks(), "生かした場所の後始末待ちが数えに残った");
        assert_eq!(g.heal(|_| false, |_| None).1, Vec::<u32>::new(), "落とした / 生かした場所の後始末待ちが残った");
    }

    /// #992 の読み直し 2 の S1: 引退する場所 (最後の行が世代を使い切った) は引退の印より上に上げられないので、 突き合わせが 「2 つ上げる」
    /// と頼んでも 1 つしか上がらない = 「最後の行は 今 − 2」 を立てない (立てると最後の行を 1 つ下に読む)。 細かい 6 / 7: 印の約束 (後始末
    /// 待ち ⇒ 死) の破れた場所 (`GEN2` だけを読む binary が生かした) は `heal` が落として数えない。 数えは 0 で止まる。
    #[test]
    fn a_retired_slot_gets_no_two_up_mark_and_heal_drops_a_live_pending_mark() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        g.start(24);
        let max = g.max_gen();
        // 場所 6: 最後の行 (世代 max) の削除の途中 = 引退の印・死
        g.raise(6, max).unwrap();
        g.kill(6, max + 1).unwrap();
        assert!(g.used_up(6), "前提: 引退の印");
        g.kill_reconciled(6, max + 2, max, false).unwrap();
        assert_eq!((g.get(6), g.dead_row_gen(6, g.get(6))), (max + 1, max), "引退した場所の最後の行を 1 つ下に読んだ");
        assert!(g.last_row_was_deleted(6), "引退した場所の最後の行は消した行");
        // 場所 8 (読み直し 3 の S3): 最後の世代で使い回しの途中の行 (使い回しの印) を引退の印にした = 消した行でない
        g.raise(8, max).unwrap();
        g.kill_reconciled(8, max + 1, max, true).unwrap();
        assert!(g.used_up(8) && g.mid_reuse(8), "前提: 引退の印・使い回しの途中");
        assert!(!g.last_row_was_deleted(8), "引退した場所の使い回しの途中の行を消した行と読んだ");
        // 場所 9: 使い回しの印は世代の変わる kill でも残り、 生かす道が落とす
        g.kill_reconciled(9, 2, 1, true).unwrap();
        g.kill(9, 3).unwrap();
        assert!(g.mid_reuse(9), "kill が使い回しの印を落とした");
        let seen = g.dead_cell(9).unwrap();
        assert!(g.revive_from(9, seen));
        assert!(!g.mid_reuse(9), "生かした場所に使い回しの印が残った");
        // 他の 2 つの生かす道 (`revive` / 再生の `restore`) も落とす
        g.kill_reconciled(11, 2, 1, true).unwrap();
        g.revive(11);
        assert!(!g.mid_reuse(11), "revive した場所に使い回しの印が残った");
        g.kill_reconciled(12, 3, 2, true).unwrap();
        assert!(g.restore(12, 1));
        assert!(!g.mid_reuse(12), "restore した場所に使い回しの印が残った");
        // 場所 10: 使い回しの印だけが残る (後始末を済ませた) まま、 印を知らない binary が死の印だけを落として生かした
        g.kill_reconciled(10, 2, 1, true).unwrap();
        g.clear_pending(10);
        let c10 = g.cell_for_write(g.region.get().unwrap(), 10).unwrap();
        c10.fetch_and(!DEAD, Ordering::AcqRel);
        // 場所 7: 後始末待ち・使い回しの途中だが死んでいない (印を知らない binary が死の印だけを落として生かした)
        g.kill_reconciled(7, 2, 0, true).unwrap();
        let c = g.cell_for_write(g.region.get().unwrap(), 7).unwrap();
        c.fetch_and(!DEAD, Ordering::AcqRel);
        g.pending_marks.store(0, Ordering::Release);
        let (_, pending) = g.heal(|s| s == 7 || s == 10, |_| None);
        assert_eq!(pending, vec![6, 8], "生きている場所の後始末待ちを拾った");
        assert_eq!(c.load(Ordering::Acquire) & (PENDING | TWO_UP | MID_REUSE), 0, "生きている場所の後始末待ち / 使い回しの印を落とさない");
        assert_eq!(c10.load(Ordering::Acquire) & MID_REUSE, 0, "生きている場所の使い回しの印を落とさない");
        assert!(g.has_pending_marks());
        // 数えていない印を落としても 0 で止まる
        g.pending_marks.store(0, Ordering::Release);
        g.clear_pending(6);
        assert!(!g.has_pending_marks(), "数えが 0 を下回った");
    }

    /// #992 / #1008: `GEN2` (上の bit を使わない前の形) の gen.seg も開け、 `upgrade_magic` が `GEN3` に上げる。 他の magic は断る。
    #[test]
    fn a_gen2_segment_is_read_and_upgraded() {
        let mut v = buffer(segment_len(16));
        let g = Generations::off();
        g.prepare(region(&mut v), 24, usize::MAX).unwrap();
        g.start(24);
        g.kill(5, 1).unwrap();
        {
            let bytes = unsafe { std::slice::from_raw_parts_mut(v.as_mut_ptr() as *mut u8, v.len() * 8) };
            bytes[..4].copy_from_slice(&MAGIC_GEN2);
        }
        let old = Generations::off();
        old.attach(region(&mut v), 24).expect("GEN2 の gen.seg を断った");
        assert_eq!((old.get(5), old.is_dead(5), old.dead_row_gen(5, 1)), (1, true, 0));
        assert!(old.upgrade_magic(), "GEN2 を GEN3 に上げない");
        assert!(!old.upgrade_magic(), "GEN3 をもう一度書き換えた");
        let bytes = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, 4) };
        assert_eq!(bytes, &MAGIC);
        let mut w = buffer(segment_len(16));
        {
            let bytes = unsafe { std::slice::from_raw_parts_mut(w.as_mut_ptr() as *mut u8, w.len() * 8) };
            bytes[..4].copy_from_slice(b"GEN9");
        }
        assert!(Generations::off().attach(region(&mut w), 24).is_err(), "知らない magic を読んだ");
    }
}
