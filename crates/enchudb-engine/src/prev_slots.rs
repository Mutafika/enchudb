//! 場所を使い回した時の前の住人の記録 (`prev.seg`、 file version 15 の DB だけ。 計画は repo 直下の `EID_REUSE_PLAN.md` の
//! 「決まった事 (2026-11-15 夜)」 = 案 C)。
//!
//! 使い回す時 (`Engine::prepare_reuse`、 場所を生かす前) は、 前の住人 (消えた行) の削除の版数と名前を identity に移す (自分の行は
//! #914 / #915 の (自分の peer, 世代 | 場所)、 翻訳した行は #166 の写像を外した (相手の peer, 相手の番号))。 移した先 (`foreign_tombs`
//! と名前の記録) は sidecar の次の書き出し (`.eidmap` / `pktombs` / `pkformer` / `eidalias`) まで memory にだけあり、 場所の削除の
//! 版数 (tomb.seg) は使い回しがその場で 0 にする。 その間に落ちると前の住人の削除がどこにも残らない (kill = page cache は残るので、
//! 本体には 0 にした tomb.seg が届く。 review921 [4] / review930 [Q4])。
//!
//! そこで使い回す時に、 tomb.seg を 0 にする前に前の住人を書く (mmap に書くだけ、 system call なし)。 1 件 48 B:
//!
//! ```text
//! 語 0     前の住人の行の番号 (この DB の 世代 | 場所)
//! 語 1     identity の peer (自分の行 = 使い回した時の自分の peer、 翻訳した行 = 相手の peer)
//! 語 2     identity の番号 (自分の行 = 語 0 と同じ、 翻訳した行 = 相手の番号)
//! 語 3..7  削除の版数 (HLC: wall u64 / logical u32 / peer u32)
//! 語 7     場所
//! 語 8     周期 (epoch、 下)
//! 語 9     印 (1 = 自分の行、 2 = 翻訳した行)
//! 語 10    0
//! 語 11    語 0..11 の check (0 にしない。 書きかけ / 書き出しの途中の page を記録と読まない)
//! ```
//!
//! 置き場は 2 つ (file の中の並び: header 4 KiB → log の区画 A / B (各 1 MiB) → 場所ごとの配列):
//!
//! - **log** (開く時に戻す元): 使い回しのたびに今の周期の区画に 1 件積む。 周期は sidecar の書き出しごとに進む
//!   ([`PrevSlots::begin_persist`]: 門を閉じて中の使い回しを待ってから進める = 周期 e の記録の memory の効果は、 e + 1 に進めた後に
//!   取る sidecar の写しに全部入る)。 sidecar を書けたら header に 「書き出した周期」 を置いて header の page を書き出す
//!   ([`PrevSlots::end_persist`])。 開く時は書き出した周期より後の記録だけ戻す = 書き出し済みの記録・忘れてよい線 (`forget_deletes_before`)
//!   で忘れた記録は戻さない (review946 [2])。 同じ場所を 1 つの周期で 2 回使い回しても、 2 件とも順に残る (場所ごとの配列は上書き)。
//!   区画は周期の偶奇で交互に使い、 前の周期の区画を頭から使い直すのは、 その周期が書き出し済みの時だけ。 開く時になめるのは 2 つの
//!   区画の書いた所まで (場所の数によらない、 review946 [4])
//! - **場所ごとの配列** (引く元): 場所の最後の使い回しの記録 (bridge の宛名、 #924 の Ref、 再生の守り、 開く時の写像の見分け)。
//!   区画が一杯の周期 (書き出しが続けて失敗した間など) は配列にだけ書き、 header に 「一杯だった周期」 を置く (開く時はその周期が
//!   書き出し済みでなければ配列もなめる)
//!
//! - きれいに閉じなかった後に開く時:
//!   - `.eidmap` の写像の面が、 配列の記録の identity (翻訳した行) のまま場所を指していて、 使い回しが本体に届いている (場所の最後の行が
//!     記録の行でない) なら、 写像を戻さず削除の版数を identity に退避する (`Engine::prev_evicted`、 #925 の推測より先に決める)
//!   - 書き出していない記録の identity の削除が退避表に無ければ戻す (`Engine::restore_prev_residents`。 前の住人の削除が本体に届いて
//!     いない像 = 場所の最後の行が記録の行で生きている、 は戻さない。 名前は今まで通り sidecar から、 `move_stale_slot_names`)
//!   - WAL の再生は、 記録の行 (使い回した前の住人) 宛ての record を当てない (`Engine::replay_slot`)。 死んだ場所を次の住人の record で
//!     生かす所で使い回しの後始末をやり直す (`Engine::redo_reuse_eviction`、 記録が届いていない像も)
//! - bridge は、 覚え (`FormerAddresses`) の無い前の住人の record の宛名を配列から引く (`Engine::bridge_row_address`)
//! - 書き出しの順: 本体の書き出し (`write_out_body`) は `prev.seg` を生きている印 (entities / gen.seg) と tomb.seg より先に書き出す。
//!   使い回しは記録を書いてから門 ([`PrevSlots::enter`]) の中で tomb.seg を 0 にし、 書き出しは門を閉じて (門を通った書き手が
//!   居れば) `prev.seg` をもう一度書き出してから tomb.seg を書き出す (#656 の 「中身 → 列」 と同じ並べ方)。 0 にした tomb.seg が
//!   届いた像には記録も届いている。 生きている印より先なのはこの書き出しの前の使い回しだけで、 `prev.seg` の書き出しの後・entities /
//!   gen.seg の書き出しの前の使い回しは、 生きている印だけが届いて記録が無い像がある (review946 [3]、 残す)
//! - commit は書いた所まで歩幅で伸ばす (file は疎、 gen.seg と同じ)
//! - 記録は sync する DB (`sync_tables_on`) の使い回しで、 移した削除がある時だけ書く。 世代を持たない DB (v14 以下 / packed /
//!   wasm / 予約の表) は作らない・書かない

use crate::region::Region;
use enchudb_oplog::{Hlc, PeerId};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// `prev.seg` の先頭の header の長さ。
pub(crate) const PREV_HEADER: usize = 4096;
const MAGIC: [u8; 4] = *b"PRV2";
/// header: 書き出した周期 (この周期までの記録の効果は sidecar にある)
const H_PERSISTED: usize = 4;
/// header: log の区画が一杯で配列にだけ書いた周期の最大
const H_OVERFLOW: usize = 8;
/// 1 件の長さ (log と配列で同じ形)。
pub(crate) const ENTRY: usize = 48;
const WORDS: usize = ENTRY / 4;
/// log の 1 区画の長さ。
const LOG_HALF: usize = 1 << 20;
/// 1 区画の件数。
const LOG_ENTRIES: u32 = (LOG_HALF / ENTRY) as u32;
/// 場所ごとの配列の始まり。
const ARRAY_OFF: usize = PREV_HEADER + 2 * LOG_HALF;

const KIND_OWN: u32 = 1;
const KIND_TRANSLATED: u32 = 2;

/// `prev.seg` の大きさ (header + log の 2 区画 + 48 B / 場所)。
pub(crate) fn segment_len(entities: u32) -> usize {
    ARRAY_OFF + ENTRY * entities as usize
}

#[inline]
fn record_off(slot: u32) -> usize {
    ARRAY_OFF + ENTRY * slot as usize
}

#[inline]
fn log_off(half: u32, i: u32) -> usize {
    PREV_HEADER + half as usize * LOG_HALF + ENTRY * i as usize
}

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

/// 語 0..11 の check (0 にしない)。 語ごとに混ぜる (byte ごとの FNV は 1 件 40 段の依存の鎖になる)。
fn check(w: &[u32]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &x in w {
        h = (h ^ x).wrapping_mul(0x0100_0193);
        h ^= h >> 13;
    }
    h.max(1)
}

fn encode(slot: u32, epoch: u32, r: &PrevRecord) -> [u32; WORDS] {
    let kind = match r.kind {
        PrevKind::Own => KIND_OWN,
        PrevKind::Translated => KIND_TRANSLATED,
    };
    let mut w = [
        r.gone,
        r.peer,
        r.number,
        r.tomb.wall as u32,
        (r.tomb.wall >> 32) as u32,
        r.tomb.logical,
        r.tomb.peer,
        slot,
        epoch,
        kind,
        0,
        0,
    ];
    w[WORDS - 1] = check(&w[..WORDS - 1]);
    w
}

/// (場所, 周期, 記録)。 書いていない / 書きかけ / 版数 0 は None。
fn decode(w: &[u32; WORDS]) -> Option<(u32, u32, PrevRecord)> {
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
    Some((w[7], w[8], PrevRecord { kind, gone: w[0], peer: w[1], number: w[2], tomb }))
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
    epoch: AtomicU32,
    /// 今の周期の区画に次に積む場所
    head: AtomicU32,
    /// 区画ごとの記録の周期の最大
    half_max: [AtomicU32; 2],
    /// 書き出した周期 (header と同じ)
    persisted: AtomicU32,
    /// 開く時に記録から退避を戻した数 (観測・試験用)
    restored: AtomicU32,
    /// log の区画が一杯で配列にだけ書いた数 (観測用)
    log_full: AtomicU64,
    /// 開く時に読んだ書き出していない記録 ((場所, 前の住人の行) の順)。 WAL の再生が、 場所ごとの配列で上書きされた前の住人 (同じ
    /// 場所を 1 つの周期で 2 回使い回した 1 回目) も引く ([`Self::record_of`])
    replay: std::sync::Mutex<Vec<(u32, PrevRecord)>>,
}

impl PrevSlots {
    pub(crate) fn off() -> Self {
        Self {
            region: OnceLock::new(),
            gate: crate::publish_gate::PublishGate::new(),
            epoch_gate: crate::publish_gate::PublishGate::new(),
            epoch: AtomicU32::new(1),
            head: AtomicU32::new(0),
            half_max: [AtomicU32::new(0), AtomicU32::new(0)],
            persisted: AtomicU32::new(0),
            restored: AtomicU32::new(0),
            log_full: AtomicU64::new(0),
            replay: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// 開く時に読んだ書き出していない記録を、 WAL の再生が引けるように置く ([`Self::record_of`])。
    pub(crate) fn keep_for_replay(&self, records: &[(u32, PrevRecord)]) {
        let mut v = records.to_vec();
        v.sort_unstable_by_key(|&(slot, r)| (slot, r.gone));
        *self.replay.lock().unwrap_or_else(|p| p.into_inner()) = v;
    }

    /// 場所 `slot` の前の住人が行 `gone` だった使い回しの記録: 場所ごとの配列 (最後の使い回し) か、 開く時に読んだ書き出していない
    /// 記録 (同じ場所の前の使い回し)。
    pub(crate) fn record_of(&self, slot: u32, gone: u32) -> Option<PrevRecord> {
        if let Some(r) = self.read(slot).filter(|r| r.gone == gone) {
            return Some(r);
        }
        let v = self.replay.lock().unwrap_or_else(|p| p.into_inner());
        let i = v.partition_point(|&(s, r)| (s, r.gone) < (slot, gone));
        v.get(i).filter(|&&(s, r)| s == slot && r.gone == gone).map(|&(_, r)| r)
    }

    /// 開く時に記録から退避を戻した数を足す / 読む (観測・試験用)。
    pub(crate) fn note_restored(&self, n: u32) {
        self.restored.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn restored(&self) -> u32 {
        self.restored.load(Ordering::Relaxed)
    }

    /// log の区画が一杯で配列にだけ書いた数 (観測用)。
    pub(crate) fn log_full(&self) -> u64 {
        self.log_full.load(Ordering::Relaxed)
    }

    /// 開いた v15 の DB の `prev.seg`。 header が合わない (作った直後に落ちた / 前の binary の残り / 前の形 `PRV1`) 時は、 `writable`
    /// なら中身を 0 に戻して使う (記録なし)、 readonly なら使わない。 使えない時は None、 0 に戻した時は Some(true) (呼び手は header の
    /// page を書き出す)。
    pub(crate) fn attach(&self, region: Region, writable: bool) -> Option<bool> {
        let ok = region.committed_len() >= PREV_HEADER && region.read_atomic(0, 4) == MAGIC;
        if ok {
            let _ = self.region.set(region);
            self.load_state();
            return Some(false);
        }
        (writable && self.prepare(region).is_ok()).then_some(true)
    }

    /// 0 から始める (前の残りは消す): v15 に移る時 / 開いた `prev.seg` の header が合わない時。 header の page は呼び手が書き出す
    /// (書き出す前に落ちると、 記録の page だけ届いて header の無い像になり、 開く時に記録を捨てる、 review946 [10])。
    pub(crate) fn prepare(&self, region: Region) -> std::io::Result<()> {
        region.ensure_committed(PREV_HEADER)?;
        let used = region.committed_len();
        if used > 0 {
            region.fill_at(0, used, 0);
            region.mark_dirty(0, used);
        }
        region.write_atomic(0, &MAGIC);
        region.mark_dirty(0, PREV_HEADER);
        let _ = self.region.set(region);
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

    fn header_word(&self, off: usize) -> &AtomicU32 {
        self.region.get().expect("prev.seg").as_atomic_u32(off)
    }

    /// 1 件を読む (範囲の外 / commit の外は None)。
    fn words_at(r: &Region, off: usize) -> Option<[u32; WORDS]> {
        if off + ENTRY > r.len() || !r.is_committed(off + ENTRY) {
            return None;
        }
        let mut w = [0u32; WORDS];
        for (i, x) in w.iter_mut().enumerate() {
            *x = r.as_atomic_u32(off + 4 * i).load(Ordering::Relaxed);
        }
        Some(w)
    }

    fn put_words(r: &Region, off: usize, w: &[u32; WORDS]) {
        for (i, &x) in w.iter().enumerate() {
            r.as_atomic_u32(off + 4 * i).store(x, Ordering::Relaxed);
        }
        r.mark_dirty(off, ENTRY);
    }

    /// 区画 `half` の書いた所 (一度も書いていない件 = 全部の語が 0 の所まで) の (件数, 記録の周期の最大)。 書きかけの件は飛ばして続ける。
    fn scan_half(r: &Region, half: u32, mut each: impl FnMut(u32, u32, u32, PrevRecord)) -> (u32, u32) {
        let mut max = 0;
        for i in 0..LOG_ENTRIES {
            let Some(w) = Self::words_at(r, log_off(half, i)) else { return (i, max) };
            if w.iter().all(|&x| x == 0) {
                return (i, max);
            }
            if let Some((slot, epoch, rec)) = decode(&w) {
                max = max.max(epoch);
                each(i, slot, epoch, rec);
            }
        }
        (LOG_ENTRIES, max)
    }

    /// 開いた `prev.seg` の周期を決める: 今の周期 = 書き出した周期と記録の周期の最大の次。 今の周期の区画は書いた所の後ろから積む
    /// (書き出していない前の記録を上書きしない。 区画を頭から使い直すのは、 次の sidecar の書き出しがそれを覆ってから)。
    fn load_state(&self) {
        let Some(r) = self.region.get() else { return };
        let persisted = self.header_word(H_PERSISTED).load(Ordering::Relaxed);
        let overflow = self.header_word(H_OVERFLOW).load(Ordering::Relaxed);
        let mut seen = persisted.max(overflow);
        let mut ends = [0u32; 2];
        for half in 0..2u32 {
            let (end, max) = Self::scan_half(r, half, |_, _, _, _| {});
            ends[half as usize] = end;
            self.half_max[half as usize].store(max, Ordering::Relaxed);
            seen = seen.max(max);
        }
        let epoch = seen.saturating_add(1);
        self.persisted.store(persisted, Ordering::Relaxed);
        self.epoch.store(epoch, Ordering::Relaxed);
        self.head.store(ends[(epoch % 2) as usize], Ordering::Relaxed);
    }

    /// 記録のありうる場所の上限 (配列の commit 済みの範囲)。
    pub(crate) fn slots_end(&self) -> u32 {
        let Some(r) = self.region.get() else { return 0 };
        let len = r.committed_len().min(r.len());
        (len.saturating_sub(ARRAY_OFF) / ENTRY).min(u32::MAX as usize) as u32
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
        let w = encode(slot, epoch, rec);
        // commit は gen.seg と同じく歩幅で伸ばす (`ensure_committed`)。 page ごとに伸ばす `ensure_committed_sparse` は、 使い回す
        // 場所が上へ進む間 page ごとに file を伸ばして空きを見に行き、 名前のある表の 「消して足す」 が 1 回 +0.5 µs だった
        let off = record_off(slot);
        let in_array = off + ENTRY <= r.len() && r.ensure_committed(off + ENTRY).is_ok();
        if in_array {
            Self::put_words(r, off, &w);
        }
        let half = epoch % 2;
        let i = self.head.fetch_add(1, Ordering::Relaxed);
        let in_log = i < LOG_ENTRIES && r.ensure_committed(log_off(half, i) + ENTRY).is_ok();
        if in_log {
            Self::put_words(r, log_off(half, i), &w);
            let m = &self.half_max[half as usize];
            if m.load(Ordering::Relaxed) != epoch {
                m.store(epoch, Ordering::Relaxed);
            }
        } else {
            self.log_full.fetch_add(1, Ordering::Relaxed);
            // 開く時に配列もなめる (この周期を書き出す前に落ちた時)。 一杯の周期の残りの書き込みは読むだけ
            let w = self.header_word(H_OVERFLOW);
            if in_array && w.load(Ordering::Relaxed) < epoch {
                w.fetch_max(epoch, Ordering::Relaxed);
                r.mark_dirty(0, PREV_HEADER);
            }
        }
        in_array || in_log
    }

    /// 場所 `slot` の配列の記録 (無い / 書きかけ / 範囲の外は None)。
    pub(crate) fn read(&self, slot: u32) -> Option<PrevRecord> {
        let r = self.region.get()?;
        let off = record_off(slot);
        if off + ENTRY > r.len() || !r.is_committed(off + ENTRY) {
            return None;
        }
        // 書いていない場所 (check の欄が 0) は残りを読まない
        if r.as_atomic_u32(off + ENTRY - 4).load(Ordering::Relaxed) == 0 {
            return None;
        }
        decode(&Self::words_at(r, off)?).map(|(_, _, rec)| rec)
    }

    /// 書き出していない周期の記録 (場所, 記録) を周期の順 (同じ周期は積んだ順、 配列の分は後) に。 開く時 (きれいに閉じなかった後)
    /// だけ。 配列をなめるのは、 log の区画が一杯で配列にだけ書いた周期が書き出されていない時だけ。
    pub(crate) fn unpersisted(&self) -> Vec<(u32, PrevRecord)> {
        let Some(r) = self.region.get() else { return Vec::new() };
        let persisted = self.persisted.load(Ordering::Relaxed);
        // 並びの鍵 = (周期, 置き場 (0 = log、 1 = 配列), 区画, 順)
        type Order = (u32, u32, u32, u32);
        let mut out: Vec<(Order, u32, PrevRecord)> = Vec::new();
        for half in 0..2u32 {
            Self::scan_half(r, half, |i, slot, epoch, rec| {
                if epoch > persisted {
                    out.push(((epoch, 0, half, i), slot, rec));
                }
            });
        }
        if self.header_word(H_OVERFLOW).load(Ordering::Relaxed) > persisted {
            for slot in 0..self.slots_end() {
                let off = record_off(slot);
                if r.as_atomic_u32(off + ENTRY - 4).load(Ordering::Relaxed) == 0 {
                    continue;
                }
                if let Some((s, epoch, rec)) = Self::words_at(r, off).as_ref().and_then(decode)
                    && epoch > persisted
                    && s == slot
                {
                    out.push(((epoch, 1, 0, slot), slot, rec));
                }
            }
        }
        out.sort_unstable_by_key(|&(k, _, _)| k);
        out.into_iter().map(|(_, slot, rec)| (slot, rec)).collect()
    }

    /// sidecar (`.eidmap` / `eidalias` / `pktombs` / `pkformer`) を書き出す前 (呼び手は書き出しを 1 本に並べる lock を握っている)。
    /// 門を閉じて中の使い回しを待ち、 前の周期の区画が書き出し済みなら周期を進める (その区画を頭から使い直す)。 返すのは、 この後に
    /// 取る sidecar の写しが覆う周期 (書けたら [`Self::end_persist`] に渡す)。 書き出していない記録が無ければ None。
    ///
    /// 進めた時は今の周期まで覆う (門を閉じて待ったので、 今の周期の使い回しの memory の効果は全部見える)。 前の周期の区画に書き出して
    /// いない記録が残っている時 (前の書き出しが失敗した / 開いた直後) は進めず、 前の周期まで覆う。
    pub(crate) fn begin_persist(&self) -> Option<u32> {
        self.region.get()?;
        let _closed = self.epoch_gate.close([0]);
        let epoch = self.epoch.load(Ordering::Acquire);
        let persisted = self.persisted.load(Ordering::Relaxed);
        let (cur, other) = ((epoch % 2) as usize, ((epoch + 1) % 2) as usize);
        let overflow = self.header_word(H_OVERFLOW).load(Ordering::Relaxed);
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
    pub(crate) fn end_persist(&self, covered: u32) -> bool {
        if self.region.get().is_none() || covered <= self.persisted.load(Ordering::Relaxed) {
            return false;
        }
        self.persisted.store(covered, Ordering::Relaxed);
        self.header_word(H_PERSISTED).fetch_max(covered, Ordering::Relaxed);
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

    fn region(v: &mut [u64]) -> Region {
        unsafe { Region::new(v.as_mut_ptr() as *mut u8, v.len() * 8) }
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
        p.prepare(region(&mut buf)).unwrap();
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

    #[test]
    fn attach_starts_over_a_segment_without_the_header_only_when_writable() {
        let mut buf = vec![0u64; segment_len(8).div_ceil(8)];
        let p = PrevSlots::off();
        assert_eq!(p.attach(region(&mut buf), false), None, "readonly で header の無い segment を使った");
        assert!(!p.active());
        let q = PrevSlots::off();
        assert_eq!(q.attach(region(&mut buf), true), Some(true));
        let rec = PrevRecord { kind: PrevKind::Translated, gone: 1, peer: 2, number: 5, tomb: Hlc { wall: 1, logical: 0, peer: 2 } };
        assert!(q.write(1, &rec));
        // もう一度開く (header がある) と記録が読める
        let again = PrevSlots::off();
        assert_eq!(again.attach(region(&mut buf), false), Some(false));
        assert_eq!(again.read(1), Some(rec));
        assert_eq!(again.unpersisted(), vec![(1, rec)]);
    }

    /// 周期: sidecar を書けた周期の記録は開く時に戻さない。 前の周期の区画は、 その周期を書き出した後にだけ頭から使い直す。 書けな
    /// かった周期の記録は残る。 開き直すと周期は記録の最大の次から、 今の区画は書いた所の後ろから積む。
    #[test]
    fn only_records_after_the_persisted_epoch_come_back() {
        let mut buf = vec![0u64; segment_len(16).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf)).unwrap();
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
        assert_eq!(again.attach(region(&mut buf), true), Some(false));
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

    /// log の区画が一杯の周期は配列にだけ書き、 その周期を書き出す前に開くと配列から戻す。
    #[test]
    fn a_full_log_half_falls_back_to_the_array() {
        let n = LOG_ENTRIES + 2;
        let mut buf = vec![0u64; segment_len(n).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf)).unwrap();
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
