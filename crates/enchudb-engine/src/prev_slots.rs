//! 場所を使い回した時の前の住人の記録 (`prev.seg`、 file version 15 の DB だけ。 計画は repo 直下の `EID_REUSE_PLAN.md` の
//! 「決まった事 (2026-11-15 夜)」 = 案 C)。
//!
//! 使い回す時 (`Engine::prepare_reuse`、 場所を生かす前) は、 前の住人 (消えた行) の削除の版数と名前を identity に移す (自分の行は
//! #914 / #915 の (自分の peer, 世代 | 場所)、 翻訳した行は #166 の写像を外した (相手の peer, 相手の番号))。 移した先 (`foreign_tombs`
//! と名前の記録) は sidecar の次の書き出し (`.eidmap` / `pktombs` / `pkformer` / `eidalias`) まで memory にだけあり、 場所の削除の
//! 版数 (tomb.seg) は使い回しがその場で 0 にする。 その間に落ちると前の住人の削除がどこにも残らない (kill = page cache は残るので、
//! 本体には 0 にした tomb.seg が届く。 review921 [4] / review930 [Q4])。
//!
//! そこで使い回す時に、 tomb.seg を 0 にする前に前の住人を書く (mmap に書くだけ、 system call なし)。 log の 1 件は 48 B:
//!
//! ```text
//! 語 0     前の住人の行の番号 (この DB の 世代 | 場所)
//! 語 1     identity の peer (自分の行 = 使い回した時の自分の peer、 翻訳した行 = 相手の peer)
//! 語 2     identity の番号 (自分の行 = 語 0 と同じ、 翻訳した行 = 相手の番号)
//! 語 3..7  削除の版数 (HLC: wall u64 / logical u32 / peer u32)
//! 語 7     場所
//! 語 8     周期 (epoch、 下) の下位 32 bit
//! 語 9     印 (1 = 自分の行、 2 = 翻訳した行)
//! 語 10    周期の上位 32 bit
//! 語 11    語 0..11 の check (0 にしない。 書きかけ / 書き出しの途中の page を記録と読まない)
//! ```
//!
//! 場所ごとの配列の 1 件は 32 B (場所は並びの位置、 行の番号は世代だけを持つ。 世代 | 場所の場所の bit は header に置く):
//!
//! ```text
//! 語 0     世代 (上 8 bit) | 印 (2 bit) | check (下 22 bit、 0 にしない。 場所と語 0..8 (check を除く) の check)
//! 語 1     identity の peer
//! 語 2     identity の番号
//! 語 3..7  削除の版数
//! 語 7     周期の下位 32 bit (開く時に配列をなめるのは、 書き出した周期の次から一杯だった周期までの窓の中の件だけ = 窓との
//!          差で比べる)
//! ```
//!
//! header (4 KiB、 `PRV4`): 0..4 magic、 4..8 行の番号の場所の bit、 8..16 書き出した周期 (u64)、 16..24 log の区画が一杯だった
//! 周期の最大 (u64)。 周期は u64 (review946c [F]: u32 は受けた batch ごとに書き出すホストで数か月〜年であふれ、 黙って記録を
//! 戻さなくなった。 前の形 `PRV3` は件の形が同じなので、 書ける open が header を写して続ける (review946d [N3]。 `PRV1` / `PRV2` は
//! 0 から始める))。
//!
//! check は語ごとに別の奇数を掛けて足す (掛け算が互いに独立 = 依存の鎖にならない。 前の 「語ごとに混ぜる」 は 11 段の鎖で、 使い回し
//! 1 回の値段の 1/4 ほどだった)。
//!
//! 置き場は 2 つ (file の中の並び: header 4 KiB → log の区画 A / B (各 1 MiB) → 場所ごとの配列):
//!
//! - **log** (開く時に戻す元): 使い回しのたびに今の周期の区画に 1 件積む。 周期は sidecar の書き出しごとに進む
//!   ([`PrevSlots::begin_persist`]: 門を閉じて中の使い回しを待ってから進める = 周期 e の記録の memory の効果は、 e + 1 に進めた後に
//!   取る sidecar の写しに全部入る)。 sidecar は WAL に書き込みのある consumer の周期で書くほか、 書き出していない記録が残っていれば
//!   暇な周期でも書く (`Engine::persist_prev_if_pending`。 `oplog_sync()` の後に暇になった DB で周期が止まらない。 1 秒に 1 回まで、
//!   review946c [G])。 sidecar を書けたら DB の directory を fsync し (rename を disk へ、 review946c [C])、 header に 「書き出した周期」
//!   を置いて header の page を書き出す ([`PrevSlots::end_persist`])。 開く時は書き出した周期より後の記録だけ戻す = 書き出し済みの記録・忘れてよい線 (`forget_deletes_before`)
//!   で忘れた記録は戻さない (review946 [2])。 同じ場所を 1 つの周期で 2 回使い回しても、 2 件とも順に残る (場所ごとの配列は上書き)。
//!   区画は周期の偶奇で交互に使い、 前の周期の区画を頭から使い直すのは、 その周期が書き出し済みの時だけ。 開く時になめるのは 2 つの
//!   区画の commit 済みの所 (場所の数によらない、 review946 [4]。 全部 0 の件 = 並んだ書き手の穴は飛ばす、 review946c [B])
//! - **場所ごとの配列** (引く元): 場所の最後の使い回しの記録 (bridge の宛名、 #924 の Ref、 再生の守り、 開く時の写像の見分け)。
//!   区画が一杯の周期 (1 周期 (consumer の 100 ms) に 21845 回より多く使い回した / 書き出しが続けて失敗した間など) は配列にだけ書き、
//!   header に 「一杯だった周期」 を置く (開く時はその周期が書き出し済みでなければ配列もなめる。 なめるのは場所ごとに語 0 と周期の
//!   2 語、 書き出した周期の記録は残りを読まない)
//!
//! - きれいに閉じなかった後に開く時:
//!   - `.eidmap` の写像の面が、 配列の記録か書き出していない周期の記録 (`.eidmap` を読む前に読む、 review946c [A]) の identity
//!     (翻訳した行) のまま場所を指していて、 使い回しが本体に届いている (場所の最後の行が記録の行でない) なら、 写像を戻さず削除の
//!     版数を identity に退避する (`Engine::prev_evicted`、 #925 の推測より先に決める)
//!   - 書き出していない記録の identity の削除が退避表に無ければ戻す (`Engine::restore_prev_residents`。 前の住人の削除が本体に届いて
//!     いない像 = 場所の最後の行が記録の行で生きている、 は戻さない。 名前は今まで通り sidecar から、 `move_stale_slot_names`)
//!   - WAL の再生は、 記録の行 (使い回した前の住人) 宛ての record を当てない (`Engine::replay_slot`)。 引くのは配列と log に残る
//!     記録の全部 (書き出した周期の記録も = sidecar の書き出しが WAL の checkpoint より先に周期を進めた像、 review946c [D])。 死んだ
//!     場所を次の住人の record で生かす所で使い回しの後始末をやり直す (`Engine::redo_reuse_eviction`、 記録が届いていない像も)
//! - bridge は、 覚え (`FormerAddresses`) の無い前の住人の record の宛名を配列と log に残る記録から引く (`Engine::bridge_row_address`)
//! - 書き出しの順: 本体の書き出し (`write_out_body`) は `prev.seg` を生きている印 (entities / gen.seg) と tomb.seg より先に書き出す。
//!   使い回しは記録を書いてから門 ([`PrevSlots::enter`]) の中で tomb.seg を 0 にし、 書き出しは門を閉じて (門を通った書き手が
//!   居れば) `prev.seg` をもう一度書き出してから tomb.seg を書き出す (#656 の 「中身 → 列」 と同じ並べ方)。 0 にした tomb.seg が
//!   届いた像には記録も届いている。 生きている印より先なのはこの書き出しの前の使い回しだけで、 `prev.seg` の書き出しの後・entities /
//!   gen.seg の書き出しの前の使い回しは、 生きている印だけが届いて記録が無い像がある (review946 [3]、 残す)
//! - commit は書いた所まで歩幅で伸ばす (file は疎、 gen.seg と同じ)
//! - 記録は sync する DB (`sync_tables_on`) の使い回しで、 移した削除がある時だけ書く。 `prev.seg` を作るのも sync する v15 の DB
//!   だけ (review946c [I])。 世代を持たない DB (v14 以下 / packed / wasm / 予約の表) は作らない・書かない
//! - 0 から始める時 (header の合わない `prev.seg`) は、 0 に戻した中身を書き出してから header を書く (前の形の件を新しい header の
//!   下で読まない、 review946c [I])。 前の中身があれば warning を出す

use crate::region::Region;
use enchudb_oplog::{Hlc, PeerId};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// `prev.seg` の先頭の header の長さ。
pub(crate) const PREV_HEADER: usize = 4096;
/// `PRV4`: 周期を u64 に (review946c [F]。 この PR の前の commit の `PRV1` / `PRV2` は開く時に 0 から始め、 `PRV3` は header を写す)
const MAGIC: [u8; 4] = *b"PRV4";
/// header: 行の番号 (世代 | 場所) の場所の bit (配列の記録は世代だけを持つ)
const H_BITS: usize = 4;
/// header: 書き出した周期 (u64。 この周期までの記録の効果は sidecar にある)
const H_PERSISTED: usize = 8;
/// header: log の区画が一杯で配列にだけ書いた周期の最大 (u64)
const H_OVERFLOW: usize = 16;
/// 前の形 `PRV3` (周期 u32。 件の形は `PRV4` と同じ = 開く時に header を写す、 review946d [N3]): 4 = 書き出した周期、 8 = 一杯だった
/// 周期、 12 = 場所の bit
const MAGIC_PRV3: [u8; 4] = *b"PRV3";
const PRV3_PERSISTED: usize = 4;
const PRV3_OVERFLOW: usize = 8;
const PRV3_BITS: usize = 12;
/// log の 1 件の長さ。
pub(crate) const ENTRY: usize = 48;
const WORDS: usize = ENTRY / 4;
/// 場所ごとの配列の 1 件の長さ。
const SLOT_ENTRY: usize = 32;
const SLOT_WORDS: usize = SLOT_ENTRY / 4;
/// 配列の語 0 の check の bit
const SLOT_CHECK: u32 = (1 << 22) - 1;
/// log の 1 区画の長さ。
const LOG_HALF: usize = 1 << 20;
/// 1 区画の件数。
const LOG_ENTRIES: u32 = (LOG_HALF / ENTRY) as u32;
/// 場所ごとの配列の始まり。
const ARRAY_OFF: usize = PREV_HEADER + 2 * LOG_HALF;

const KIND_OWN: u32 = 1;
const KIND_TRANSLATED: u32 = 2;

/// `prev.seg` の大きさ (header + log の 2 区画 + 32 B / 場所)。
pub(crate) fn segment_len(entities: u32) -> usize {
    ARRAY_OFF + SLOT_ENTRY * entities as usize
}

#[inline]
fn record_off(slot: u32) -> usize {
    ARRAY_OFF + SLOT_ENTRY * slot as usize
}

#[inline]
fn log_off(half: u32, i: u32) -> usize {
    PREV_HEADER + half as usize * LOG_HALF + ENTRY * i as usize
}

/// (場所, 記録) の並び。
pub(crate) type Records = Vec<(u32, PrevRecord)>;

/// 開く時に戻すかを WAL の再生の後に回した記録 (場所, 記録, 退避が書き出し済みか)。
pub(crate) type Deferred = (u32, PrevRecord, bool);

/// 前の住人が自分の行か、 翻訳した行 (受けた他の peer の行) か。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PrevKind {
    Own,
    Translated,
}

/// 場所の前の住人の記録。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PrevRecord {
    pub(crate) kind: PrevKind,
    /// 前の住人の行の番号 (この DB の 世代 | 場所)
    pub(crate) gone: u32,
    /// identity の peer
    pub(crate) peer: PeerId,
    /// identity の番号 (自分の行 = `gone`、 翻訳した行 = 相手の番号)
    pub(crate) number: u32,
    /// 削除の版数
    pub(crate) tomb: Hlc,
}

/// 語の並びを 64 bit に混ぜる: 語ごとに別の奇数を掛けて足し (掛け算は互いに独立 = 依存の鎖にならない。 前の 「語ごとに xor して
/// 掛ける」 は 11 段の鎖で、 使い回し 1 回の値段の 1/4 ほどだった)、 最後に混ぜる。 1 語だけ違う並びの和は必ず違う (奇数を掛けるのは
/// 2^64 を法に可逆。 語に 1 を足すので 0 の語も数える)。 最後の混ぜは可逆なので、 違いが残るのは取り出す bit の衝突だけ。
#[inline]
fn mix(w: &[u32]) -> u64 {
    const K: [u64; 12] = [
        0x9e37_79b9_7f4a_7c15,
        0xbf58_476d_1ce4_e5b9,
        0x94d0_49bb_1331_11eb,
        0xd6e8_feb8_6659_fd93,
        0xa076_1d64_78bd_642f,
        0xe703_7ed1_a0b4_28db,
        0x8ebc_6af0_9c88_c6e3,
        0x5899_65cc_7537_4cc3,
        0x1d8e_4e27_c47d_124f,
        0xc2b2_ae3d_27d4_eb4f,
        0x1656_67b1_9e37_79f9,
        0x27d4_eb2f_1656_67c5,
    ];
    debug_assert!(w.len() <= K.len());
    let mut h = 0u64;
    for (&x, k) in w.iter().zip(K) {
        h = h.wrapping_add((u64::from(x) + 1).wrapping_mul(k));
    }
    h ^= h >> 31;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^ (h >> 27)
}

/// log の語 0..11 の check (0 にしない)。
fn check(w: &[u32]) -> u32 {
    ((mix(w) >> 32) as u32).max(1)
}

fn kind_word(k: PrevKind) -> u32 {
    match k {
        PrevKind::Own => KIND_OWN,
        PrevKind::Translated => KIND_TRANSLATED,
    }
}

/// 配列の check (22 bit、 0 にしない): 場所と、 語 0 (check を除く) .. 語 7。
fn slot_check(slot: u32, w: &[u32; SLOT_WORDS]) -> u32 {
    let mut v = [0u32; SLOT_WORDS + 1];
    v[0] = slot;
    v[1] = w[0] & !SLOT_CHECK;
    v[2..].copy_from_slice(&w[1..]);
    ((mix(&v) >> 42) as u32 & SLOT_CHECK).max(1)
}

/// 配列の 1 件 (場所 `slot`、 行の番号の場所の bit `bits`)。
fn encode_slot(slot: u32, bits: u32, epoch: u64, r: &PrevRecord) -> [u32; SLOT_WORDS] {
    debug_assert_eq!(r.gone & low_mask(bits), slot, "前の住人の行の番号の場所が記録の場所と違う");
    let generation = r.gone.checked_shr(bits).unwrap_or(0);
    debug_assert!(generation < 256, "世代が 8 bit を越えた");
    let mut w = [
        (generation << 24) | (kind_word(r.kind) << 22),
        r.peer,
        r.number,
        r.tomb.wall as u32,
        (r.tomb.wall >> 32) as u32,
        r.tomb.logical,
        r.tomb.peer,
        epoch as u32,
    ];
    w[0] |= slot_check(slot, &w);
    w
}

/// 配列の 1 件を読む (周期の下位 32 bit, 記録)。 書いていない / 書きかけ / 版数 0 は None。
fn decode_slot(slot: u32, bits: u32, w: &[u32; SLOT_WORDS]) -> Option<(u32, PrevRecord)> {
    if w[0] & SLOT_CHECK == 0 || w[0] & SLOT_CHECK != slot_check(slot, w) {
        return None;
    }
    let kind = match (w[0] >> 22) & 3 {
        KIND_OWN => PrevKind::Own,
        KIND_TRANSLATED => PrevKind::Translated,
        _ => return None,
    };
    let tomb = Hlc { wall: u64::from(w[3]) | (u64::from(w[4]) << 32), logical: w[5], peer: w[6] };
    if tomb == Hlc::ZERO {
        return None;
    }
    let gone = (w[0] >> 24).checked_shl(bits).unwrap_or(0) | slot;
    Some((w[7], PrevRecord { kind, gone, peer: w[1], number: w[2], tomb }))
}

#[inline]
fn low_mask(bits: u32) -> u32 {
    1u32.checked_shl(bits).map_or(u32::MAX, |b| b - 1)
}

fn encode(slot: u32, epoch: u64, r: &PrevRecord) -> [u32; WORDS] {
    let kind = kind_word(r.kind);
    let mut w = [
        r.gone,
        r.peer,
        r.number,
        r.tomb.wall as u32,
        (r.tomb.wall >> 32) as u32,
        r.tomb.logical,
        r.tomb.peer,
        slot,
        epoch as u32,
        kind,
        (epoch >> 32) as u32,
        0,
    ];
    w[WORDS - 1] = check(&w[..WORDS - 1]);
    w
}

/// (場所, 周期, 記録)。 書いていない / 書きかけ / 版数 0 は None。
fn decode(w: &[u32; WORDS]) -> Option<(u32, u64, PrevRecord)> {
    if w[WORDS - 1] != check(&w[..WORDS - 1]) {
        return None;
    }
    let kind = match w[9] {
        KIND_OWN => PrevKind::Own,
        KIND_TRANSLATED => PrevKind::Translated,
        _ => return None,
    };
    let tomb = Hlc { wall: u64::from(w[3]) | (u64::from(w[4]) << 32), logical: w[5], peer: w[6] };
    if tomb == Hlc::ZERO {
        return None;
    }
    Some((w[7], u64::from(w[8]) | (u64::from(w[10]) << 32), PrevRecord { kind, gone: w[0], peer: w[1], number: w[2], tomb }))
}

/// `prev.seg` (世代を持たない DB / 開けなかった DB は off = 書かない・読むと None)。 [`crate::generation::Generations`] が持つ
/// (Engine に欄を足すと書き手の欄と同じ cache line に並びうる、 generation.rs の `Engine::generations` の doc)。
pub(crate) struct PrevSlots {
    region: OnceLock<Region>,
    /// 使い回しの tomb.seg を 0 にする書き込みと本体の書き出しの順 (列は 0 の 1 本だけ使う)
    gate: crate::publish_gate::PublishGate,
    /// 使い回しの後始末 (memory の効果と記録) と周期を進める sidecar の書き出しの順 (列は 0 の 1 本だけ使う)。 tomb.seg の門と
    /// 別に持つ (門ごとに閉じる側を 1 本にする lock があり、 本体の書き出しが tomb.seg を書き出す間、 周期を進められなくなる)
    epoch_gate: crate::publish_gate::PublishGate,
    /// 今の周期 (1 から)。 門の中で読む (門を閉じている間だけ進む)
    epoch: AtomicU64,
    /// 行の番号 (世代 | 場所) の場所の bit (header と同じ。 配列の記録は世代だけを持つ)
    bits: AtomicU32,
    /// 今の周期の区画に次に積む場所
    head: AtomicU32,
    /// 区画ごとの記録の周期の最大
    half_max: [AtomicU64; 2],
    /// 書き出した周期 (header と同じ)
    persisted: AtomicU64,
    /// 開く時に記録から退避を戻した数 (観測・試験用)
    restored: AtomicU32,
    /// log の区画が一杯で配列にだけ書いた数 (観測用)
    log_full: AtomicU64,
    /// 開く時に読んだ記録 ((場所, 前の住人の行) の順、 記録の周期と。 log に残る全部と、 書き出していない周期の配列の件)。 WAL の
    /// 再生・bridge が、 場所ごとの配列で上書きされた前の住人 (同じ場所を 2 回使い回した 1 回目) も引く ([`Self::record_of`])
    replay: std::sync::Mutex<Vec<(u32, PrevRecord, u64)>>,
    /// 開く時に記録から戻した自分の行の削除 (前の住人の行, 削除の版数)。 #451 の作り直しが WAL に無い Delete を載せる
    /// ([`Self::take_restored_own`]、 review946c [E])
    restored_own: std::sync::Mutex<Vec<(u32, Hlc)>>,
    /// 開く時に戻すかを WAL の再生の後に回した記録 (場所, 記録, 退避が書き出し済みか)。 review946d [N1]
    /// (`Engine::finish_prev_restores`)
    deferred: std::sync::Mutex<Vec<Deferred>>,
    /// 読み直し 2 [B2] (#969 / #968 の PR): きれいに閉じなかった後に開く道が、 前の住人の後始末 (記録から戻す・WAL の再生のやり直し・
    /// 後に回した戻し) を memory に作り終えるまで立てる ([`Self::hold_open`] / [`Self::release_open`])。 立っている間の
    /// [`Self::begin_persist`] は周期を進めない (書き出した周期の記録の効果が sidecar に全部ある、 という約束を開く道の途中の書き出し
    /// = 再生する TieNamed / TieLeaf の紐の定義が破らない)
    open_hold: std::sync::atomic::AtomicBool,
    /// [R2] (#969 / #968 の PR): WAL の再生の間だけ持つ、 退避表 (`Engine::foreign_tombs`) の削除の版数 → それを持つ identity の数。
    /// 再生の後始末のやり直し (`redo_reuse_eviction`) が 「退避表に同じ版数の identity があるか」 を record ごとに引く ([`Self::tomb_held`])。
    /// 前は record ごとに退避表の全部をなめた (開く時 O(record の数 × 退避表の大きさ))。 None = 作っていない (最初に引く時に作る)
    tomb_index: std::sync::Mutex<Option<std::collections::HashMap<Hlc, u32>>>,
    /// `tomb_index` を持っている間 (退避表を書く道が数を合わせる)。 持っていない間の書く道は atomic 1 回
    tomb_index_on: std::sync::atomic::AtomicBool,
}

impl PrevSlots {
    pub(crate) fn off() -> Self {
        Self {
            region: OnceLock::new(),
            gate: crate::publish_gate::PublishGate::new(),
            epoch_gate: crate::publish_gate::PublishGate::new(),
            epoch: AtomicU64::new(1),
            bits: AtomicU32::new(0),
            head: AtomicU32::new(0),
            half_max: [AtomicU64::new(0), AtomicU64::new(0)],
            persisted: AtomicU64::new(0),
            restored: AtomicU32::new(0),
            log_full: AtomicU64::new(0),
            replay: std::sync::Mutex::new(Vec::new()),
            restored_own: std::sync::Mutex::new(Vec::new()),
            deferred: std::sync::Mutex::new(Vec::new()),
            open_hold: std::sync::atomic::AtomicBool::new(false),
            tomb_index: std::sync::Mutex::new(None),
            tomb_index_on: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// 読み直し 2 [B2]: きれいに閉じなかった後に開く道の初め (`prev.seg` の記録を読む前)。 [`Self::release_open`] まで周期を進めない。
    pub(crate) fn hold_open(&self) {
        self.open_hold.store(true, Ordering::Release);
    }

    /// 読み直し 2 [B2]: 開く道が前の住人の後始末を memory に作り終えた (`Engine::finish_open_restores`)。 この後の sidecar の書き出しが
    /// 周期を進める (その写しは開く道が作った後始末を全部含む)。
    pub(crate) fn release_open(&self) {
        self.open_hold.store(false, Ordering::Release);
    }

    /// [`Self::hold_open`] から [`Self::release_open`] までの間か。
    pub(crate) fn opening(&self) -> bool {
        self.open_hold.load(Ordering::Acquire)
    }

    /// [R2]: 退避表に削除の版数 `h` を持つ identity があるか。 開く道の後始末の間だけ (記録から戻す道・WAL の再生・その後の別名の外し。
    /// 1 本の thread、 退避表を書く者は他に居ない) 呼ぶ。 索引が無ければ `values` (退避表の今の版数の全部、 退避表の read lock を取る)
    /// で作り、 開く道の仕上げ ([`Self::drop_tomb_index`]) まで退避表を書く道 ([`Self::tomb_index_moved`]) と数を合わせる。 `values` は
    /// 索引の lock の外で呼ぶ (書く道の lock の順は 退避表 → 索引)。
    pub(crate) fn tomb_held(&self, h: Hlc, values: impl FnOnce() -> Vec<Hlc>) -> bool {
        if let Some(idx) = self.tomb_index.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
            return idx.contains_key(&h);
        }
        let vs = values();
        let mut g = self.tomb_index.lock().unwrap_or_else(|p| p.into_inner());
        let idx = g.get_or_insert_with(|| {
            let mut m = std::collections::HashMap::with_capacity(vs.len());
            for v in vs {
                if v != Hlc::ZERO {
                    *m.entry(v).or_insert(0u32) += 1;
                }
            }
            self.tomb_index_on.store(true, Ordering::Release);
            m
        });
        idx.contains_key(&h)
    }

    /// [R2]: 退避表の 1 identity の削除の版数が `old` から `new` に変わった (ZERO = 無い)。 索引を持っていない間は何もしない。
    /// 呼び手は退避表の write lock を握っている (lock の順は 退避表 → 索引、 索引を作る側は退避表の lock を放してから索引の lock)。
    #[inline]
    pub(crate) fn tomb_index_moved(&self, old: Hlc, new: Hlc) {
        if old == new || !self.tomb_index_on.load(Ordering::Acquire) {
            return;
        }
        let mut g = self.tomb_index.lock().unwrap_or_else(|p| p.into_inner());
        let Some(idx) = g.as_mut() else { return };
        if old != Hlc::ZERO
            && let std::collections::hash_map::Entry::Occupied(mut e) = idx.entry(old)
        {
            *e.get_mut() -= 1;
            if *e.get() == 0 {
                e.remove();
            }
        }
        if new != Hlc::ZERO {
            *idx.entry(new).or_insert(0) += 1;
        }
    }

    /// [R2]: 開く道の仕上げ (`Engine::finish_open_restores`) で索引を捨てる (その後は引かない)。
    pub(crate) fn drop_tomb_index(&self) {
        self.tomb_index_on.store(false, Ordering::Release);
        *self.tomb_index.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }

    /// 開く時に読んだ記録 ([`Self::open_records`] の 2 つ目) を、 WAL の再生・bridge・#924 の Ref が引けるように置く
    /// ([`Self::record_of`])。 `.eidmap` を読む前に置く (review946c [A])。
    pub(crate) fn keep_for_replay(&self, records: &[(u32, PrevRecord, u64)]) {
        let mut v = records.to_vec();
        v.sort_unstable_by_key(|&(slot, r, epoch)| (slot, r.gone, epoch));
        *self.replay.lock().unwrap_or_else(|p| p.into_inner()) = v;
    }

    /// 場所 `slot` の前の住人が行 `gone` だった使い回しの記録: 場所ごとの配列 (最後の使い回し) か、 開く時に読んだ記録 (同じ場所の
    /// 前の使い回し。 log に残る全部 = 書き出した周期の記録も、 review946c [D])。
    pub(crate) fn record_of(&self, slot: u32, gone: u32) -> Option<PrevRecord> {
        self.record_at(slot, gone).map(|(r, _)| r)
    }

    /// [`Self::record_of`] と、 その記録の周期が書き出し済みか (= その使い回しの memory の効果は sidecar に全部ある、
    /// [`Self::end_persist`])。 読み直し [A1] (#969 / #968 の PR): 再生の後始末のやり直し (`Engine::redo_reuse_eviction`) は、
    /// 書き出し済みの記録の後始末をやり直さない。
    ///
    /// 同じ (場所, 前の住人の行) の記録が配列と log / 周期の違う log の件に 2 つ以上ある時 (開いた後にもう一度落ちて、 開く道が前の
    /// session の記録を書き直していた像など) は、 1 件でも書き出した周期の物なら書き出し済み (読み直し 2 [B7]: 前は並べ方で任意の
    /// 1 件を引いた)。 返す記録は配列の件 (無ければ周期のいちばん新しい件)。
    pub(crate) fn record_at(&self, slot: u32, gone: u32) -> Option<(PrevRecord, bool)> {
        let array = self.read_slot(slot).filter(|(_, r)| r.gone == gone).map(|(lo, r)| (r, self.slot_epoch_written(lo)));
        let persisted = self.persisted.load(Ordering::Relaxed);
        let v = self.replay.lock().unwrap_or_else(|p| p.into_inner());
        let i = v.partition_point(|&(s, r, _)| (s, r.gone) < (slot, gone));
        // 並びは (場所, 前の住人の行, 周期) の順 = 最後が周期のいちばん新しい件
        let (last, logged_written) = v[i..]
            .iter()
            .take_while(|&&(s, r, _)| s == slot && r.gone == gone)
            .fold((None, false), |(_, w), &(_, r, epoch)| (Some(r), w || epoch <= persisted));
        match (array, last) {
            (Some((r, w)), _) => Some((r, w || logged_written)),
            (None, Some(r)) => Some((r, logged_written)),
            (None, None) => None,
        }
    }

    /// 配列の件の周期 (下位 32 bit `lo`) が書き出した周期以下か: 書き出した周期より後・今の周期以下の窓の中なら書き出していない
    /// (窓との差で比べる = [`Self::open_records`] の配列の窓と同じ。 窓の外の件は書き出した周期の物)。
    fn slot_epoch_written(&self, lo: u32) -> bool {
        let persisted = self.persisted.load(Ordering::Relaxed);
        let width = self.epoch.load(Ordering::Acquire).saturating_sub(persisted);
        let d = lo.wrapping_sub(persisted as u32);
        d == 0 || u64::from(d) > width
    }

    /// 場所 `slot` の記録を全部 (場所ごとの配列と、 `sorted` ((場所, 前の住人の行) の順に並べた記録) の場所 `slot` の件。 同じ記録が
    /// 2 度来うる)。 review946c [A]: 開く時の写像の見分けは、 同じ場所を 1 つの周期で 2 回使い回した 1 回目 (配列は 2 回目に上書き)
    /// の記録も見る。
    pub(crate) fn for_each_record_at(&self, sorted: &[(u32, PrevRecord)], slot: u32, mut f: impl FnMut(PrevRecord)) {
        if let Some(r) = self.read(slot) {
            f(r);
        }
        let i = sorted.partition_point(|&(s, _)| s < slot);
        for &(_, r) in sorted[i..].iter().take_while(|&&(s, _)| s == slot) {
            f(r);
        }
    }

    /// 開く時に記録から退避を戻した数を足す / 読む (観測・試験用)。
    pub(crate) fn note_restored(&self, n: u32) {
        self.restored.fetch_add(n, Ordering::Relaxed);
    }

    /// 開く時に記録から自分の行 `gone` の削除 (版数 `tomb`) を戻した。
    pub(crate) fn note_restored_own(&self, gone: u32, tomb: Hlc) {
        self.restored_own.lock().unwrap_or_else(|p| p.into_inner()).push((gone, tomb));
    }

    /// 開く時に記録から戻した自分の行の削除 (取り出す)。 tomb.seg のなめと同じ (版数, 行) を含みうる (呼び手の #451 の作り直しが
    /// 重ねを落とす、 review946d [N2])。
    pub(crate) fn take_restored_own(&self) -> Vec<(u32, Hlc)> {
        std::mem::take(&mut *self.restored_own.lock().unwrap_or_else(|p| p.into_inner()))
    }

    /// 開く時に戻すかを WAL の再生の後に回す (review946d [N1])。 外す別名は戻す時に世代で決める (#993 / #994。 前は再生の前の場所の
    /// 別名を控えた、 読み直し 2 [B3])。
    pub(crate) fn defer_restore(&self, slot: u32, rec: PrevRecord, written: bool) {
        self.deferred.lock().unwrap_or_else(|p| p.into_inner()).push((slot, rec, written));
    }

    /// 後に回した記録 (取り出す、 回した順)。
    pub(crate) fn take_deferred(&self) -> Vec<Deferred> {
        std::mem::take(&mut *self.deferred.lock().unwrap_or_else(|p| p.into_inner()))
    }

    pub(crate) fn restored(&self) -> u32 {
        self.restored.load(Ordering::Relaxed)
    }

    /// log の区画が一杯で配列にだけ書いた数 (観測用)。
    pub(crate) fn log_full(&self) -> u64 {
        self.log_full.load(Ordering::Relaxed)
    }

    /// 開いた v15 の DB の `prev.seg` (行の番号の場所の bit は `bits`)。 header が合わない (作った直後に落ちた / 前の binary の残り /
    /// 前の形 `PRV1` / `PRV2` / 場所の bit が違う) 時は、 `writable` なら中身を 0 に戻して使う (記録なし、 [`Self::prepare`])、 readonly
    /// なら使わない。 前の形 `PRV3` (場所の bit が合う) は、 `writable` なら header を写して記録ごと使う (review946d [N3])。 使えない
    /// 時は None、 header を書き換えた (0 に戻した / 写した) 時は Some(true) (呼び手は header の page を書き出す)。 `flush` は
    /// `prev.seg` の範囲 (off, len) を書き出す。
    pub(crate) fn attach(
        &self,
        region: Region,
        writable: bool,
        bits: u32,
        flush: impl Fn(usize, usize) -> std::io::Result<()>,
    ) -> Option<bool> {
        let committed = region.committed_len() >= PREV_HEADER;
        let magic = if committed { region.read_atomic(0, 4) } else { Vec::new() };
        let ok = magic == MAGIC && region.as_atomic_u32(H_BITS).load(Ordering::Relaxed) == bits;
        if ok {
            let _ = self.region.set(region);
            self.bits.store(bits, Ordering::Relaxed);
            self.load_state();
            return Some(false);
        }
        if !writable {
            return None;
        }
        // review946d [N3]: `PRV3` は件の形 (log の 48 B・配列の 32 B・check) が `PRV4` と同じで、 header の 3 欄 (周期 u32 → u64 と
        // 場所の bit の位置) だけ違う。 header を写して続ける (0 から始めると、 その binary で書いてきれいに閉じなかった DB の記録を
        // 全部落とした)
        if magic == MAGIC_PRV3 && region.as_atomic_u32(PRV3_BITS).load(Ordering::Relaxed) == bits {
            Self::upgrade_prv3_header(&region, bits);
            let _ = self.region.set(region);
            self.bits.store(bits, Ordering::Relaxed);
            self.load_state();
            return Some(true);
        }
        // review946c [I]: 前の中身 (前の形の header / 場所の bit の違う header) を捨てる時は言う
        if committed && region.as_atomic_u32(0).load(Ordering::Relaxed) != 0 {
            // 場所の bit の位置は形ごと (`PRV1` / `PRV2` は持たない)
            let their_bits = match magic.as_slice() {
                m if m == MAGIC => Some(H_BITS),
                m if m == MAGIC_PRV3 => Some(PRV3_BITS),
                _ => None,
            }
            .map_or_else(String::new, |off| format!(", slot bits {}", region.as_atomic_u32(off).load(Ordering::Relaxed)));
            eprintln!(
                "warning: prev.seg ({}{their_bits}) does not match this binary ({}, slot bits {bits}); starting it over \
                 (reuse records of an unclean shutdown before this open are dropped)",
                String::from_utf8_lossy(&magic),
                String::from_utf8_lossy(&MAGIC),
            );
        }
        self.prepare(region, bits, flush).is_ok().then_some(true)
    }

    /// review946d [N3]: `PRV3` の header (4 = 書き出した周期 u32、 8 = 一杯だった周期 u32、 12 = 場所の bit) を `PRV4` の形に写す。
    /// 3 欄を読んでから新しい位置に置き、 magic を最後に書く (header の page は呼び手が書き出す)。 書き換えの途中の page が先に disk に
    /// 届いた像 (magic は `PRV3`、 場所の bit の位置に周期の上位 = 0) は、 次に開く時に場所の bit が合わず 0 から始める (前と同じ)。
    fn upgrade_prv3_header(region: &Region, bits: u32) {
        let persisted = u64::from(region.as_atomic_u32(PRV3_PERSISTED).load(Ordering::Relaxed));
        let overflow = u64::from(region.as_atomic_u32(PRV3_OVERFLOW).load(Ordering::Relaxed));
        region.as_atomic_u64(H_OVERFLOW).store(overflow, Ordering::Relaxed);
        region.as_atomic_u64(H_PERSISTED).store(persisted, Ordering::Relaxed);
        region.as_atomic_u32(H_BITS).store(bits, Ordering::Relaxed);
        region.write_atomic(0, &MAGIC);
        region.mark_dirty(0, PREV_HEADER);
    }

    /// 0 から始める (前の残りは消す): v15 に移る時 / 開いた `prev.seg` の header が合わない時。 header の page は呼び手が書き出す
    /// (書き出す前に落ちると、 記録の page だけ届いて header の無い像になり、 開く時に記録を捨てる、 review946 [10])。
    ///
    /// review946c [I]: 0 に戻した中身 (header の page も) を `flush` で書き出してから header を書く。 前は header の page だけを
    /// 呼び手が書き出し、 0 に戻した区画と配列は次の本体の書き出しまで disk に前の中身が残った (その間に落ちると、 新しい header の
    /// 下で前の形の件を読みえた)。 書き出せなければ Err (header を書かない = 次に開く時にもう一度 0 から)。
    pub(crate) fn prepare(
        &self,
        region: Region,
        bits: u32,
        flush: impl Fn(usize, usize) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        region.ensure_committed(PREV_HEADER)?;
        let used = region.committed_len();
        if used > 0 {
            region.fill_at(0, used, 0);
            region.mark_dirty(0, used);
            flush(0, used)?;
        }
        region.write_atomic(0, &MAGIC);
        region.as_atomic_u32(H_BITS).store(bits, Ordering::Relaxed);
        region.mark_dirty(0, PREV_HEADER);
        let _ = self.region.set(region);
        self.bits.store(bits, Ordering::Relaxed);
        self.epoch.store(1, Ordering::Relaxed);
        self.head.store(0, Ordering::Relaxed);
        self.persisted.store(0, Ordering::Relaxed);
        for m in &self.half_max {
            m.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    /// `prev.seg` を持つか。
    #[inline]
    pub(crate) fn active(&self) -> bool {
        self.region.get().is_some()
    }

    fn header_u64(&self, off: usize) -> &AtomicU64 {
        self.region.get().expect("prev.seg").as_atomic_u64(off)
    }

    /// 1 件 (`N` 語) を読む (範囲の外 / commit の外は None)。
    fn words_at<const N: usize>(r: &Region, off: usize) -> Option<[u32; N]> {
        if off + 4 * N > r.len() || !r.is_committed(off + 4 * N) {
            return None;
        }
        let mut w = [0u32; N];
        for (i, x) in w.iter_mut().enumerate() {
            *x = r.as_atomic_u32(off + 4 * i).load(Ordering::Relaxed);
        }
        Some(w)
    }

    fn put_words<const N: usize>(r: &Region, off: usize, w: &[u32; N]) {
        for (i, &x) in w.iter().enumerate() {
            r.as_atomic_u32(off + 4 * i).store(x, Ordering::Relaxed);
        }
        r.mark_dirty(off, 4 * N);
    }

    /// 区画 `half` の書いた所 (最後の 0 でない件の次) と記録の周期の最大 (件数, 周期)。 区画の commit 済みの所を全部なめる: 全部の語が
    /// 0 の件 (一度も書いていない) と書きかけの件は飛ばして続ける。
    ///
    /// review946c [B]: 前は最初の全部 0 の件で止まった。 書き手は位置を取ってから (`head.fetch_add`) commit を伸ばして書くので、 別の表の
    /// 使い回しが並ぶと、 前の位置が書かれる前に後ろの位置が書き終わる (commit を伸ばせなかった位置は区画を使い直すまで 0 のまま)。
    /// その穴で止まると、 kill / 電源断の像で穴の後ろの記録を戻さず、 開いた後はその上に積んだ。
    fn scan_half(r: &Region, half: u32, mut each: impl FnMut(u32, u32, u64, PrevRecord)) -> (u32, u64) {
        let (mut end, mut max) = (0, 0);
        for i in 0..LOG_ENTRIES {
            let Some(w) = Self::words_at::<WORDS>(r, log_off(half, i)) else { break };
            if w.iter().all(|&x| x == 0) {
                continue;
            }
            end = i + 1;
            if let Some((slot, epoch, rec)) = decode(&w) {
                max = max.max(epoch);
                each(i, slot, epoch, rec);
            }
        }
        (end, max)
    }

    /// 開いた `prev.seg` の周期を決める: 今の周期 = 書き出した周期と記録の周期の最大の次。 今の周期の区画は書いた所の後ろから積む
    /// (書き出していない前の記録を上書きしない。 区画を頭から使い直すのは、 次の sidecar の書き出しがそれを覆ってから)。
    fn load_state(&self) {
        let Some(r) = self.region.get() else { return };
        let persisted = self.header_u64(H_PERSISTED).load(Ordering::Relaxed);
        let overflow = self.header_u64(H_OVERFLOW).load(Ordering::Relaxed);
        let mut seen = persisted.max(overflow);
        let mut ends = [0u32; 2];
        for half in 0..2u32 {
            let (end, max) = Self::scan_half(r, half, |_, _, _, _| {});
            ends[half as usize] = end;
            self.half_max[half as usize].store(max, Ordering::Relaxed);
            seen = seen.max(max);
        }
        let epoch = seen + 1;
        self.persisted.store(persisted, Ordering::Relaxed);
        self.epoch.store(epoch, Ordering::Relaxed);
        self.head.store(ends[(epoch % 2) as usize], Ordering::Relaxed);
    }

    /// 記録のありうる場所の上限 (配列の commit 済みの範囲)。
    pub(crate) fn slots_end(&self) -> u32 {
        let Some(r) = self.region.get() else { return 0 };
        let len = r.committed_len().min(r.len());
        (len.saturating_sub(ARRAY_OFF) / SLOT_ENTRY).min(u32::MAX as usize) as u32
    }

    /// 使い回しの後始末 (memory の効果と [`Self::write`]) の間持つ ([`Self::begin_persist`] が周期を進める間は待つ)。 `prev.seg` が
    /// 無ければ None。
    #[inline]
    pub(crate) fn enter_epoch(&self, slot: u32) -> Option<crate::publish_gate::Pass<'_>> {
        self.active().then(|| self.epoch_gate.enter(0, slot))
    }

    /// 場所 `slot` に前の住人を書く: 場所ごとの配列 (上書き) と、 今の周期の log の区画 (積む。 一杯なら配列にだけ書いて header に
    /// 一杯だった周期を置く)。 mmap に書くだけ (commit を伸ばす時だけ file を伸ばす、 歩幅は `grow_amortized`)。 書けたか (書けない =
    /// `prev.seg` が無い / 範囲の外 / 空き不足)。 使い回しは [`Self::enter_epoch`] の中で呼ぶ (開く時の再生は 1 本)。 呼び手は場所を
    /// 握っている (行の lock と空きの一覧から取り出した場所 = 同じ場所の書き手は 1 本)。
    pub(crate) fn write(&self, slot: u32, rec: &PrevRecord) -> bool {
        let Some(r) = self.region.get() else { return false };
        // 門の中: 周期は動かない (閉じている間だけ進む)
        let epoch = self.epoch.load(Ordering::Acquire);
        // commit は gen.seg と同じく歩幅で伸ばす (`ensure_committed`)。 page ごとに伸ばす `ensure_committed_sparse` は、 使い回す
        // 場所が上へ進む間 page ごとに file を伸ばして空きを見に行き、 名前のある表の 「消して足す」 が 1 回 +0.5 µs だった
        let off = record_off(slot);
        let in_array = off + SLOT_ENTRY <= r.len() && r.ensure_committed(off + SLOT_ENTRY).is_ok();
        if in_array {
            Self::put_words(r, off, &encode_slot(slot, self.bits.load(Ordering::Relaxed), epoch, rec));
        }
        let half = (epoch % 2) as u32;
        // 一杯の区画は読むだけ (一杯の後に書き手ごとに積む位置を取り合わない)
        let i = if self.head.load(Ordering::Relaxed) < LOG_ENTRIES {
            self.head.fetch_add(1, Ordering::Relaxed)
        } else {
            LOG_ENTRIES
        };
        let in_log = i < LOG_ENTRIES && r.ensure_committed(log_off(half, i) + ENTRY).is_ok();
        if in_log {
            Self::put_words(r, log_off(half, i), &encode(slot, epoch, rec));
            let m = &self.half_max[half as usize];
            if m.load(Ordering::Relaxed) != epoch {
                m.store(epoch, Ordering::Relaxed);
            }
        } else {
            // 観測用の数 (書き手が並ぶと数え落としうる。 一杯の後の書き込みごとに 1 語の RMW を取り合わない)
            self.log_full.store(self.log_full.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
            // 開く時に配列もなめる (この周期を書き出す前に落ちた時)。 一杯の周期の残りの書き込みは読むだけ
            let w = self.header_u64(H_OVERFLOW);
            if in_array && w.load(Ordering::Relaxed) < epoch {
                w.fetch_max(epoch, Ordering::Relaxed);
                r.mark_dirty(0, PREV_HEADER);
            }
        }
        in_array || in_log
    }

    /// 場所 `slot` の配列の記録 (無い / 書きかけ / 範囲の外は None)。
    pub(crate) fn read(&self, slot: u32) -> Option<PrevRecord> {
        self.read_slot(slot).map(|(_, rec)| rec)
    }

    /// [`Self::read`] と、 記録の周期の下位 32 bit。
    fn read_slot(&self, slot: u32) -> Option<(u32, PrevRecord)> {
        let r = self.region.get()?;
        let off = record_off(slot);
        if off + SLOT_ENTRY > r.len() || !r.is_committed(off + SLOT_ENTRY) {
            return None;
        }
        // 書いていない場所 (check の欄が 0) は残りを読まない
        if r.as_atomic_u32(off).load(Ordering::Relaxed) & SLOT_CHECK == 0 {
            return None;
        }
        decode_slot(slot, self.bits.load(Ordering::Relaxed), &Self::words_at::<SLOT_WORDS>(r, off)?)
    }

    /// 書き出していない周期の記録 (場所, 記録) を周期の順 (同じ周期は積んだ順、 配列の分は後) に ([`Self::open_records`] の 1 つ目)。
    #[cfg(test)]
    pub(crate) fn unpersisted(&self) -> Vec<(u32, PrevRecord)> {
        self.open_records().0
    }

    /// 開く時 (きれいに閉じなかった後だけ) に読む記録: (書き出していない周期の記録を周期の順 (同じ周期は積んだ順、 配列の分は後) に、
    /// log に残る記録の全部 (周期によらない) と書き出していない周期の配列の件 (記録の周期と))。 1 つ目は退避を戻す元、 2 つ目は WAL の
    /// 再生の守り・bridge が引く元 ([`Self::keep_for_replay`]。 review946c [D]: 書き出した周期の記録も、 区画を使い直すまで log に残る。
    /// pull の sidecar の書き出しが WAL の checkpoint より先に周期を進めた像で、 再生する前の住人宛ての record を守る)。
    ///
    /// 配列をなめるのは、 log の区画が一杯で配列にだけ書いた周期が書き出されていない時だけ。 配列の件は周期の下位 32 bit しか持た
    /// ないので、 書き出した周期の次から一杯だった周期までの窓の中の件だけ取る (窓との差で比べる = 周期の下位 bit が一周しても比べ
    /// 違えない。 窓の外の古い件と下位 32 bit が窓の中で重なるのは、 2^32 周期の間その場所を使い回さなかった件だけ)。
    pub(crate) fn open_records(&self) -> (Records, Vec<(u32, PrevRecord, u64)>) {
        let Some(r) = self.region.get() else { return (Vec::new(), Vec::new()) };
        let persisted = self.persisted.load(Ordering::Relaxed);
        // 並びの鍵 = (周期, 置き場 (0 = log、 1 = 配列), 区画, 順)
        type Order = (u64, u32, u32, u32);
        let mut out: Vec<(Order, u32, PrevRecord)> = Vec::new();
        let mut logged: Vec<(u32, PrevRecord, u64)> = Vec::new();
        for half in 0..2u32 {
            Self::scan_half(r, half, |i, slot, epoch, rec| {
                logged.push((slot, rec, epoch));
                if epoch > persisted {
                    out.push(((epoch, 0, half, i), slot, rec));
                }
            });
        }
        let overflow = self.header_u64(H_OVERFLOW).load(Ordering::Relaxed);
        if overflow > persisted {
            let bits = self.bits.load(Ordering::Relaxed);
            let width = (overflow - persisted).min(u64::from(u32::MAX)) as u32;
            // 周期の下位 32 bit `lo` が窓 (書き出した周期, 一杯だった周期] の中なら、 その周期
            let in_window = |lo: u32| {
                let d = lo.wrapping_sub(persisted as u32);
                (d != 0 && d <= width).then(|| persisted + u64::from(d))
            };
            for slot in 0..self.slots_end() {
                let off = record_off(slot);
                // 書いていない場所 (check の欄が 0) と窓の外の周期の記録は残りを読まない
                if r.as_atomic_u32(off).load(Ordering::Relaxed) & SLOT_CHECK == 0
                    || in_window(r.as_atomic_u32(off + SLOT_ENTRY - 4).load(Ordering::Relaxed)).is_none()
                {
                    continue;
                }
                if let Some((epoch, rec)) = Self::words_at::<SLOT_WORDS>(r, off)
                    .and_then(|w| decode_slot(slot, bits, &w))
                    .and_then(|(lo, rec)| in_window(lo).map(|e| (e, rec)))
                {
                    out.push(((epoch, 1, 0, slot), slot, rec));
                    logged.push((slot, rec, epoch));
                }
            }
        }
        out.sort_unstable_by_key(|&(k, _, _)| k);
        (out.into_iter().map(|(_, slot, rec)| (slot, rec)).collect(), logged)
    }

    /// 書き出していない周期の記録があるか (sidecar を書けば周期が進む)。
    pub(crate) fn pending(&self) -> bool {
        if self.region.get().is_none() {
            return false;
        }
        let p = self.persisted.load(Ordering::Relaxed);
        self.half_max.iter().any(|m| m.load(Ordering::Relaxed) > p) || self.header_u64(H_OVERFLOW).load(Ordering::Relaxed) > p
    }

    /// sidecar (`.eidmap` / `eidalias` / `pktombs` / `pkformer`) を書き出す前 (呼び手は書き出しを 1 本に並べる lock を握っている)。
    /// 門を閉じて中の使い回しを待ち、 前の周期の区画が書き出し済みなら周期を進める (その区画を頭から使い直す)。 返すのは、 この後に
    /// 取る sidecar の写しが覆う周期 (書けたら [`Self::end_persist`] に渡す)。 書き出していない記録が無ければ None。
    ///
    /// 進めた時は今の周期まで覆う (門を閉じて待ったので、 今の周期の使い回しの memory の効果は全部見える)。 前の周期の区画に書き出して
    /// いない記録が残っている時 (前の書き出しが失敗した / 開いた直後) は進めず、 前の周期まで覆う。
    ///
    /// 読み直し 2 [B2]: きれいに閉じなかった後に開く道の途中 ([`Self::hold_open`] 〜 [`Self::release_open`]) は None (覆わない)。
    /// 開く道は書き出していない周期の記録の効果を memory に作り直している途中 (記録から戻す道・再生のやり直し・後に回した戻し) で、
    /// その途中の写しが周期を覆うと、 まだ作り直していない記録が 「書き出し済み」 になる: 再生のやり直しが後始末を飛ばし (前の住人の
    /// 削除がどこにも残らない)、 途中の書き出しの後に落ちた次の open も記録から戻さない。 前は再生する TieNamed / TieLeaf が本体に無い
    /// 紐を定義する時の書き出し (`ensure_himo_dynamic_in` → `try_persist_tables`) が周期を進めた。
    pub(crate) fn begin_persist(&self) -> Option<u64> {
        self.region.get()?;
        if self.opening() {
            return None;
        }
        let _closed = self.epoch_gate.close([0]);
        let epoch = self.epoch.load(Ordering::Acquire);
        let persisted = self.persisted.load(Ordering::Relaxed);
        let (cur, other) = ((epoch % 2) as usize, ((epoch + 1) % 2) as usize);
        let overflow = self.header_u64(H_OVERFLOW).load(Ordering::Relaxed);
        let other_pending = self.half_max[other].load(Ordering::Relaxed) > persisted;
        if !other_pending && self.half_max[cur].load(Ordering::Relaxed) <= persisted && overflow <= persisted {
            return None;
        }
        if other_pending {
            return Some(epoch - 1);
        }
        self.head.store(0, Ordering::Relaxed);
        self.epoch.store(epoch + 1, Ordering::Release);
        Some(epoch)
    }

    /// sidecar を全部書けた (覆う周期 `covered` = [`Self::begin_persist`] の返り)。 header の書き出した周期を進めたか (呼び手は header
    /// の page を書き出す: 忘れてよい線で外した identity の `.eidmap` が、 その記録の周期より前の header と一緒に届かないように)。
    pub(crate) fn end_persist(&self, covered: u64) -> bool {
        if self.region.get().is_none() || covered <= self.persisted.load(Ordering::Relaxed) {
            return false;
        }
        self.persisted.store(covered, Ordering::Relaxed);
        self.header_u64(H_PERSISTED).fetch_max(covered, Ordering::Relaxed);
        self.region.get().expect("prev.seg").mark_dirty(0, PREV_HEADER);
        true
    }

    /// 使い回しが tomb.seg を 0 にする間持つ (記録を書いた後)。 書き出しが門を閉じていれば開くまで待つ。
    #[inline]
    pub(crate) fn enter(&self, slot: u32) -> crate::publish_gate::Pass<'_> {
        self.gate.enter(0, slot)
    }

    /// 書き出しが tomb.seg を書き出す間閉じる。 返った後は、 門の中で 0 にした tomb.seg の前に書いた記録が全部見える。 印
    /// ([`Self::take_written`]) が立っていれば `prev.seg` をもう一度書き出してから tomb.seg を書き出す。
    pub(crate) fn close(&self) -> crate::publish_gate::Closed<'_> {
        self.gate.close([0])
    }

    /// 前に閉じてから門を通った使い回しが居たか (印を下ろす。 閉じている間に呼ぶ)。
    pub(crate) fn take_written(&self) -> bool {
        self.gate.take_written(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [R2]: 再生の間の退避表の版数の索引は、 作った時の版数の全部を数え、 退避表を書く道の動き (上げる / 外す) に数を合わせる。
    /// 作る前と捨てた後は動きを無視する (atomic 1 回)。
    #[test]
    fn the_tomb_index_follows_the_moves_while_it_is_held() {
        let p = PrevSlots::off();
        let h = |w: u64| Hlc { wall: w, logical: 0, peer: 1 };
        p.tomb_index_moved(Hlc::ZERO, h(9)); // 作る前: 無視
        assert!(p.tomb_held(h(1), || vec![h(1), h(1), h(2), Hlc::ZERO]));
        assert!(!p.tomb_held(h(9), || unreachable!("作った後は作り直さない")));
        assert!(!p.tomb_held(Hlc::ZERO, || unreachable!()));
        p.tomb_index_moved(h(1), h(3)); // 2 つの identity の 1 つが h(1) → h(3)
        assert!(p.tomb_held(h(1), || unreachable!()), "h(1) を持つ identity がまだ 1 つある");
        assert!(p.tomb_held(h(3), || unreachable!()));
        p.tomb_index_moved(h(1), Hlc::ZERO); // 残りの 1 つを外す (忘れてよい線)
        assert!(!p.tomb_held(h(1), || unreachable!()), "h(1) を持つ identity は無い");
        p.tomb_index_moved(Hlc::ZERO, h(4)); // 新しい identity
        assert!(p.tomb_held(h(4), || unreachable!()));
        p.drop_tomb_index();
        p.tomb_index_moved(Hlc::ZERO, h(5)); // 捨てた後: 無視
        assert!(!p.tomb_held(h(4), || vec![h(5)]), "捨てた後は今の版数で作り直す");
        assert!(p.tomb_held(h(5), || unreachable!()));
    }

    fn region(v: &mut [u64]) -> Region {
        unsafe { Region::new(v.as_mut_ptr() as *mut u8, v.len() * 8) }
    }

    fn no_flush(_: usize, _: usize) -> std::io::Result<()> {
        Ok(())
    }

    fn own(slot: u32, g: u32, wall: u64) -> PrevRecord {
        PrevRecord {
            kind: PrevKind::Own,
            gone: (g << 24) | slot,
            peer: 7,
            number: (g << 24) | slot,
            tomb: Hlc { wall, logical: 0, peer: 7 },
        }
    }

    #[test]
    fn a_record_reads_back_and_a_torn_one_does_not() {
        let mut buf = vec![0u64; segment_len(64).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf), 24, no_flush).unwrap();
        assert_eq!(p.read(3), None, "書いていない場所は記録なし");
        let rec = own(3, 2, 1_234);
        assert!(p.write(3, &rec));
        assert_eq!(p.read(3), Some(rec));
        let tr = PrevRecord {
            kind: PrevKind::Translated,
            gone: (1 << 24) | 3,
            peer: 9,
            number: 11,
            tomb: Hlc { wall: 99, logical: 0, peer: 9 },
        };
        assert!(p.write(3, &tr), "上書き");
        assert_eq!(p.read(3), Some(tr));
        assert_eq!(p.unpersisted(), vec![(3, rec), (3, tr)], "log には 2 件とも順に残る");
        // 書きかけ (版数の 1 byte だけ違う) は記録と読まない
        let r = p.region.get().unwrap();
        r.write_at(record_off(3) + 13, &[0xAA]);
        assert_eq!(p.read(3), None, "check の合わない記録を読んだ");
        r.write_at(log_off(1, 0) + 13, &[0xAA]);
        assert_eq!(p.unpersisted(), vec![(3, tr)], "check の合わない log の件を読んだ");
        assert!(p.write(64, &tr), "範囲の外の場所も log には積む");
        assert_eq!(p.read(64), None);
    }

    /// 読み直し [A1] (#969 / #968 の PR): 記録は周期が書き出し済みかを言う。 配列の件は周期の下位 32 bit を 「書き出した周期より後・
    /// 今の周期以下」 の窓と比べ、 開く時に読んだ log の件 (配列は後の使い回しで上書きされた) は周期で比べる。
    #[test]
    fn a_record_says_whether_its_epoch_was_written_out() {
        let mut buf = vec![0u64; segment_len(64).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf), 24, no_flush).unwrap();
        let (a, b) = (own(3, 1, 10), own(4, 1, 20));
        assert!(p.write(3, &a));
        assert_eq!(p.record_at(3, a.gone), Some((a, false)), "書き出していない周期の記録");
        assert_eq!(p.record_at(3, own(3, 2, 10).gone), None, "前の住人の違う記録");
        let covered = p.begin_persist().expect("書き出していない記録がある");
        assert!(p.write(4, &b), "次の周期の使い回し");
        assert!(p.end_persist(covered));
        assert_eq!(p.record_at(3, a.gone), Some((a, true)), "書き出した周期の記録");
        assert_eq!(p.record_at(4, b.gone), Some((b, false)), "書き出した周期の後の記録");
        // 開く時に読んだ log の件 (場所 5 の配列は後の使い回し d で上書き、 場所 6 は配列に無い)
        let (c, d, e) = (own(5, 1, 30), own(5, 2, 40), own(6, 1, 50));
        assert!(p.write(5, &d));
        p.keep_for_replay(&[(5, c, covered), (6, e, covered + 1)]);
        assert_eq!(p.record_at(5, c.gone), Some((c, true)), "log の書き出した周期の件");
        assert_eq!(p.record_at(6, e.gone), Some((e, false)), "log の書き出していない周期の件");
        assert_eq!(p.record_at(5, d.gone), Some((d, false)), "配列の件が先");
        // 読み直し 2 [B6]: 配列の件の周期の下位 32 bit が書き出した周期より前 (窓 (書き出した周期, 今の周期] との差が折り返す) なら
        // 書き出し済み。 場所 3 の a は周期 1、 2 回書き出して書き出した周期は 2 以上
        let e2 = p.begin_persist().expect("書き出していない記録がある");
        assert!(p.end_persist(e2));
        assert!(p.persisted.load(Ordering::Relaxed) > 1, "前提: 書き出した周期が a の周期より後");
        assert_eq!(p.record_at(3, a.gone), Some((a, true)), "書き出した周期より前の配列の件を書き出していないと読んだ");
        // 読み直し 2 [B7]: 同じ (場所, 前の住人の行) の記録が周期違いで 2 件 (log の書き出した周期の件と書き出していない周期の件) あれば、
        // 1 件でも書き出し済みなら書き出し済み (並べ方によらない)
        let f = own(7, 1, 60);
        let persisted = p.persisted.load(Ordering::Relaxed);
        for logged in [[(7, f, persisted), (7, f, persisted + 1)], [(7, f, persisted + 1), (7, f, persisted)]] {
            p.keep_for_replay(&logged);
            assert_eq!(p.record_at(7, f.gone), Some((f, true)), "書き出した周期の件があるのに書き出していないと読んだ: {logged:?}");
        }
        p.keep_for_replay(&[(7, f, persisted + 1), (7, f, persisted + 2)]);
        assert_eq!(p.record_at(7, f.gone), Some((f, false)), "どの件も書き出していない");
        // 配列の件が書き出していない周期でも、 log に書き出した周期の同じ記録があれば書き出し済み
        assert!(p.write(7, &f));
        p.keep_for_replay(&[(7, f, persisted)]);
        assert_eq!(p.record_at(7, f.gone), Some((f, true)), "log の書き出した周期の件を配列の件が隠した");
    }

    /// 読み直し 2 [B2]: きれいに閉じなかった後に開く道の途中 (`hold_open` 〜 `release_open`) の書き出しは周期を覆わない (書き出した
    /// 周期を進めない = 開く道がまだ作り直していない記録を 「書き出し済み」 にしない)。 放した後は今まで通り覆う。
    #[test]
    fn a_persist_during_the_open_does_not_cover_the_records() {
        let mut buf = vec![0u64; segment_len(16).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf), 24, no_flush).unwrap();
        let a = own(3, 1, 10);
        assert!(p.write(3, &a));
        p.hold_open();
        assert!(p.opening());
        assert_eq!(p.begin_persist(), None, "開く道の途中の書き出しが周期を覆った");
        assert_eq!(p.record_at(3, a.gone), Some((a, false)));
        assert_eq!(p.unpersisted(), vec![(3, a)]);
        p.release_open();
        assert!(!p.opening());
        let e = p.begin_persist().expect("放した後は覆う");
        assert!(p.end_persist(e));
        assert_eq!(p.record_at(3, a.gone), Some((a, true)));
    }

    /// 配列の 1 件 (32 B) は行の番号を世代だけで持ち、 場所は並びの位置: 世代と場所の bit が変わっても読み戻せる。 check は場所も
    /// 覆う (別の場所の位置に写した件を読まない)。 どの語の書き換えも記録と読まない (log の件も)。
    #[test]
    fn an_array_record_keeps_the_generation_and_any_changed_word_is_rejected() {
        for bits in [24u32, 26] {
            let mut buf = vec![0u64; segment_len(64).div_ceil(8)];
            let p = PrevSlots::off();
            p.prepare(region(&mut buf), bits, no_flush).unwrap();
            let top = (1u32 << (32 - bits)) - 1;
            let tr = PrevRecord {
                kind: PrevKind::Translated,
                gone: (top << bits) | 5,
                peer: 9,
                number: u32::MAX - 1,
                tomb: Hlc { wall: u64::MAX - 7, logical: 3, peer: 9 },
            };
            assert!(p.write(5, &tr));
            assert_eq!(p.read(5), Some(tr), "場所の bit {bits}");
            let r = p.region.get().unwrap();
            // 別の場所の位置に写した件
            let entry = r.read_atomic(record_off(5), SLOT_ENTRY);
            r.write_at(record_off(6), &entry);
            assert_eq!(p.read(6), None, "別の場所に写した件を読んだ");
            // 語ごとに 1 bit 変えた件
            for i in 0..SLOT_WORDS {
                let w = r.as_atomic_u32(record_off(5) + 4 * i);
                let old = w.load(Ordering::Relaxed);
                w.store(old ^ (1 << 7), Ordering::Relaxed);
                assert_eq!(p.read(5), None, "配列の語 {i} を変えた件を読んだ");
                w.store(old, Ordering::Relaxed);
            }
            assert_eq!(p.read(5), Some(tr));
            for i in 0..WORDS {
                let w = r.as_atomic_u32(log_off(1, 0) + 4 * i);
                let old = w.load(Ordering::Relaxed);
                w.store(old ^ (1 << 7), Ordering::Relaxed);
                assert_eq!(p.unpersisted(), vec![], "log の語 {i} を変えた件を読んだ");
                w.store(old, Ordering::Relaxed);
            }
            assert_eq!(p.unpersisted(), vec![(5, tr)]);
        }
    }

    #[test]
    fn attach_starts_over_a_segment_without_the_header_only_when_writable() {
        let mut buf = vec![0u64; segment_len(8).div_ceil(8)];
        let p = PrevSlots::off();
        assert_eq!(p.attach(region(&mut buf), false, 24, no_flush), None, "readonly で header の無い segment を使った");
        assert!(!p.active());
        let q = PrevSlots::off();
        assert_eq!(q.attach(region(&mut buf), true, 24, no_flush), Some(true));
        let rec = PrevRecord { kind: PrevKind::Translated, gone: 1, peer: 2, number: 5, tomb: Hlc { wall: 1, logical: 0, peer: 2 } };
        assert!(q.write(1, &rec));
        // もう一度開く (header がある) と記録が読める
        let again = PrevSlots::off();
        assert_eq!(again.attach(region(&mut buf), false, 24, no_flush), Some(false));
        assert_eq!(again.read(1), Some(rec));
        assert_eq!(again.unpersisted(), vec![(1, rec)]);
    }

    /// 周期: sidecar を書けた周期の記録は開く時に戻さない。 前の周期の区画は、 その周期を書き出した後にだけ頭から使い直す。 書けな
    /// かった周期の記録は残る。 開き直すと周期は記録の最大の次から、 今の区画は書いた所の後ろから積む。
    #[test]
    fn only_records_after_the_persisted_epoch_come_back() {
        let mut buf = vec![0u64; segment_len(16).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf), 24, no_flush).unwrap();
        assert_eq!(p.begin_persist(), None, "記録が無ければ覆う物は無い");
        let (a, b, c, d) = (own(1, 1, 10), own(1, 2, 20), own(2, 1, 30), own(3, 1, 40));
        assert!(p.write(1, &a)); // 周期 1
        let e1 = p.begin_persist().unwrap();
        assert_eq!(e1, 1, "周期 1 を覆って 2 に進む");
        assert!(p.write(1, &b)); // 周期 2
        assert!(p.end_persist(e1));
        assert_eq!(p.unpersisted(), vec![(1, b)], "書き出した周期 1 の記録を戻す");
        // 周期 2 の sidecar は書けなかった (end_persist を呼ばない)。 次の書き出しは区画 1 (周期 1、 書き出し済み) を使い直して 3 に進む
        assert_eq!(p.begin_persist(), Some(2));
        assert!(p.write(2, &c)); // 周期 3 (区画 1 を頭から)
        // 周期 3 も書けない。 次は区画 0 (周期 2、 書き出していない) を使い直せない = 進めず、 周期 2 まで覆う
        assert_eq!(p.begin_persist(), Some(2));
        assert!(p.write(3, &d)); // 周期 3 のまま
        assert_eq!(p.unpersisted(), vec![(1, b), (2, c), (3, d)]);
        // 開き直す: 周期は記録の最大 (3) の次 = 4 (区画 0)。 区画 0 の書き出していない周期 2 の記録の後ろから積む
        let again = PrevSlots::off();
        assert_eq!(again.attach(region(&mut buf), true, 24, no_flush), Some(false));
        assert_eq!(again.unpersisted(), vec![(1, b), (2, c), (3, d)]);
        let e = own(4, 1, 50);
        assert!(again.write(4, &e));
        assert_eq!(again.unpersisted(), vec![(1, b), (2, c), (3, d), (4, e)], "開いた後に積んで前の記録を消した");
        // 区画 1 (周期 3) が書き出されるまで進めない
        assert_eq!(again.begin_persist(), Some(3));
        assert!(again.end_persist(3));
        assert_eq!(again.unpersisted(), vec![(4, e)]);
        assert_eq!(again.begin_persist(), Some(4), "区画 1 は書き出し済み = 進む");
        assert!(again.end_persist(4));
        assert_eq!(again.unpersisted(), vec![]);
    }

    /// review946c [B]: 区画の途中に一度も書いていない件 (並んだ書き手が位置を取ってまだ書いていない / commit を伸ばせなかった位置) が
    /// あっても、 その後ろの記録を戻し、 開き直した後は後ろの記録の後から積む (穴で止まって後ろを上書きしない)。
    #[test]
    fn a_hole_in_the_log_does_not_hide_the_records_behind_it() {
        let mut buf = vec![0u64; segment_len(16).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf), 24, no_flush).unwrap();
        let recs: Vec<PrevRecord> = (0..4).map(|s| own(s, 1, u64::from(s) + 10)).collect();
        for (s, r) in recs.iter().enumerate() {
            assert!(p.write(s as u32, r));
        }
        // 件 1 を一度も書いていない件にする (周期 1 = 区画 1)
        let r = p.region.get().unwrap();
        r.fill_at(log_off(1, 1), ENTRY, 0);
        let mut want: Vec<(u32, PrevRecord)> = [0usize, 2, 3].iter().map(|&s| (s as u32, recs[s])).collect();
        assert_eq!(p.unpersisted(), want, "穴の後ろの記録を戻さない");
        // 周期 2 (区画 0) に 1 件積んで、 書き出す前に開き直す: 周期 3 = 区画 1 (穴のある区画) の続きから積む
        assert_eq!(p.begin_persist(), Some(1));
        let x = own(8, 1, 80);
        assert!(p.write(8, &x));
        want.push((8, x));
        let again = PrevSlots::off();
        assert_eq!(again.attach(region(&mut buf), true, 24, no_flush), Some(false));
        assert_eq!(again.unpersisted(), want);
        assert_eq!(again.head.load(Ordering::Relaxed), 4, "開き直した後に積む位置が穴の後ろの記録より前");
        for s in [5u32, 6] {
            let e = own(s, 1, u64::from(s) * 10);
            assert!(again.write(s, &e));
            want.push((s, e));
        }
        assert_eq!(again.unpersisted(), want, "開き直した後に穴の後ろの記録の上に積んだ");
    }

    /// review946c [F]: 周期は u64 (u32 の下位 bit が一周しても記録を戻す)。 前は `epoch + 1` が u32 であふれた (debug は panic、
    /// release は 0 に戻り、 書き出した周期との比べが全部偽 = 記録を戻さなくなった)。 配列は周期の下位 32 bit だけ持つので、 開く時に
    /// 配列をなめる窓 (書き出した周期, 一杯だった周期] は差で比べる: 一周をまたぐ窓の件を取り、 窓の外の古い件は取らない。
    #[test]
    fn epochs_past_u32_still_come_back() {
        let n = LOG_ENTRIES + 2;
        let mut buf = vec![0u64; segment_len(n + 8).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf), 24, no_flush).unwrap();
        // 古い件: 周期 1 の配列の記録 (書き出し済み)
        let old = own(n + 1, 1, 5);
        assert!(p.write(n + 1, &old));
        assert_eq!(p.begin_persist(), Some(1));
        assert!(p.end_persist(1));
        // 周期を u32::MAX - 1 まで進めた像
        let top = u64::from(u32::MAX) - 1;
        p.epoch.store(top, Ordering::Relaxed);
        assert!(p.write(1, &own(1, 1, 10))); // 周期 u32::MAX - 1
        assert_eq!(p.begin_persist(), Some(top));
        assert!(p.end_persist(top));
        assert!(p.write(2, &own(2, 1, 20))); // 周期 u32::MAX
        assert_eq!(p.begin_persist(), Some(top + 1), "周期を 2^32 へ");
        assert!(p.end_persist(top + 1));
        // 周期 2^32 (下位 32 bit は 0 = 書き出した周期の下位 bit より小さい) の区画を一杯にして配列にだけ書く
        for s in 0..n {
            assert!(p.write(s, &own(s, 2, u64::from(s) + 100)));
        }
        assert_eq!(p.log_full(), 2, "前提: 区画が一杯で配列にだけ書いた");
        let check = |q: &PrevSlots| {
            let back = q.unpersisted();
            assert_eq!(back.len(), (LOG_ENTRIES + n) as usize, "u32 の一周をまたいだ周期の記録 (log の全部 + 配列の全部)");
            assert!(back.contains(&(n - 1, own(n - 1, 2, u64::from(n) + 99))), "一周をまたいだ周期の配列の記録を戻さない");
            assert!(!back.contains(&(n + 1, old)), "窓の外の古い配列の記録を戻した");
        };
        check(&p);
        let again = PrevSlots::off();
        assert_eq!(again.attach(region(&mut buf), true, 24, no_flush), Some(false));
        check(&again);
        assert_eq!(again.epoch.load(Ordering::Relaxed), (1 << 32) + 1, "開き直した周期");
        let e = again.begin_persist().unwrap();
        assert!(again.end_persist(e));
        assert_eq!(again.unpersisted(), vec![], "書き出した後に戻した");
    }

    /// review946c [I]: 0 から始める時は、 0 に戻した中身を書き出してから header を書く (前の形の件が新しい header の下で残らない)。
    #[test]
    fn starting_over_writes_out_the_cleared_content_before_the_header() {
        let mut buf = vec![0u64; segment_len(8).div_ceil(8)];
        {
            let p = PrevSlots::off();
            p.prepare(region(&mut buf), 24, no_flush).unwrap();
            assert!(p.write(3, &own(3, 1, 7)));
        }
        // 前の形の header (`PRV2`、 件の形が違う) と前の件
        let r = region(&mut buf);
        r.write_atomic(0, b"PRV2");
        let base = buf.as_ptr() as usize;
        let len = buf.len() * 8;
        let flushed = std::cell::Cell::new(0usize);
        let flush = |off: usize, n: usize| {
            // SAFETY: buf は生きている (この test の中)
            let bytes = unsafe { std::slice::from_raw_parts(base as *const u8, len) };
            assert_eq!(off, 0);
            assert!(bytes[..4] != MAGIC[..], "header を書いてから中身を書き出した");
            assert!(bytes[..n].iter().all(|&b| b == 0), "0 に戻す前に書き出した");
            flushed.set(n);
            Ok(())
        };
        let q = PrevSlots::off();
        assert_eq!(q.attach(r, true, 24, flush), Some(true));
        assert!(flushed.get() > log_off(1, 0), "前提: 前の件の所まで書き出した");
        assert_eq!(q.unpersisted(), vec![], "前の形の件を読んだ");
        assert_eq!(q.read(3), None);
    }

    /// review946d [N3]: 前の形 `PRV3` (この PR の途中の binary、 周期 u32) の file は、 書ける open が header を写して記録ごと使う
    /// (件の形は `PRV4` と bit まで同じ = 2^32 より前の周期の件は今の binary が書く物と同じ)。 場所の bit が違う `PRV3` と readonly は
    /// 今まで通り (0 から始める / 使わない)。
    #[test]
    fn a_prv3_file_keeps_its_records() {
        let mut buf = vec![0u64; segment_len(8).div_ceil(8)];
        let want = {
            let p = PrevSlots::off();
            p.prepare(region(&mut buf), 24, no_flush).unwrap();
            assert!(p.write(3, &own(3, 1, 7))); // 周期 1 (書き出す)
            let e = p.begin_persist().unwrap();
            assert!(p.end_persist(e));
            assert!(p.write(5, &own(5, 1, 9))); // 周期 2 (書き出していない = 落ちた像で戻す)
            p.unpersisted()
        };
        assert_eq!(want, vec![(5, own(5, 1, 9))], "前提: 書き出していない記録が 1 つ");
        // header を `PRV3` の形に書き直す (4 = 書き出した周期 u32、 8 = 一杯だった周期 u32、 12 = 場所の bit)
        let to_prv3 = |buf: &mut [u64], bits: u32| {
            let r = region(buf);
            let persisted = r.as_atomic_u64(H_PERSISTED).load(Ordering::Relaxed);
            let overflow = r.as_atomic_u64(H_OVERFLOW).load(Ordering::Relaxed);
            r.as_atomic_u64(H_OVERFLOW).store(0, Ordering::Relaxed);
            r.as_atomic_u32(PRV3_PERSISTED).store(persisted as u32, Ordering::Relaxed);
            r.as_atomic_u32(PRV3_OVERFLOW).store(overflow as u32, Ordering::Relaxed);
            r.as_atomic_u32(PRV3_BITS).store(bits, Ordering::Relaxed);
            r.write_atomic(0, &MAGIC_PRV3);
        };
        let saved = buf.clone();
        to_prv3(&mut buf, 24);
        assert_eq!(PrevSlots::off().attach(region(&mut buf), false, 24, no_flush), None, "readonly で `PRV3` を使った");
        let q = PrevSlots::off();
        assert_eq!(q.attach(region(&mut buf), true, 24, no_flush), Some(true), "写した header を書き出させない");
        assert_eq!(q.unpersisted(), want, "`PRV3` の書き出していない記録を落とした");
        assert_eq!(q.read(3), Some(own(3, 1, 7)), "`PRV3` の配列の記録を落とした");
        assert_eq!(q.epoch.load(Ordering::Relaxed), 3, "写した後の周期");
        assert_eq!(buf, saved, "写した header が `PRV4` の header と違う");
        // 開き直しても同じ (`PRV4` として開く)
        let again = PrevSlots::off();
        assert_eq!(again.attach(region(&mut buf), true, 24, no_flush), Some(false));
        assert_eq!(again.unpersisted(), want);
        // 場所の bit が違う `PRV3` は 0 から始める
        to_prv3(&mut buf, 20);
        let other = PrevSlots::off();
        assert_eq!(other.attach(region(&mut buf), true, 24, no_flush), Some(true));
        assert_eq!(other.unpersisted(), vec![], "場所の bit が違う `PRV3` の記録を読んだ");
        assert_eq!(other.read(3), None);
    }

    /// log の区画が一杯の周期は配列にだけ書き、 その周期を書き出す前に開くと配列から戻す。
    #[test]
    fn a_full_log_half_falls_back_to_the_array() {
        let n = LOG_ENTRIES + 2;
        let mut buf = vec![0u64; segment_len(n).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf), 24, no_flush).unwrap();
        for s in 0..n {
            assert!(p.write(s, &own(s, 1, u64::from(s) + 1)));
        }
        assert_eq!(p.log_full(), 2);
        let back = p.unpersisted();
        assert_eq!(back.len(), (LOG_ENTRIES + n) as usize, "log の全部 + 配列の全部 (戻すのは冪等)");
        assert!(back.contains(&(n - 1, own(n - 1, 1, u64::from(n)))), "一杯の後の記録が戻らない");
        let e = p.begin_persist().unwrap();
        assert!(p.end_persist(e));
        assert_eq!(p.unpersisted(), vec![], "書き出した後に配列をなめた");
    }
}
