//! Vocabulary — ユニーク値辞書。Symbol の値を value_id (u32) に変換。
//! 3つのRegion（data, offsets, index）で構成。
//!
//! ## 索引 (index) = 語数に合わせて伸びるハッシュ表 (#374)
//!
//! index 領域は作成時に `index_cap` slot (= `vocab_max_entries` の次の 2 の冪、 1 slot 13 B) の大きさで予約する
//! (entity cap 2 億なら 3.49 GB、 sparse)。 昔はこの全域を 1 つのハッシュ表にしていたので、 語が少なくても slot は
//! 全域に散り、 引くたび・入れるたびに別のページを触った (語 2 万個で 367 MB のページ)。 メモリの小さい箱では
//! page cache が溢れ、 `lookup` のたびに読み直しになった。
//!
//! 今は表を**語数に合わせて**持つ:
//! - 表 (gen) は領域の中の連続した `2^bits` slot (`base` から)。 最初は `FIRST_GEN_CAP` slot
//! - 語数が表の半分を超えたら、 倍の表を**今の表の直後**に作り、 今の表の slot を移して切り替える (`grow`)。
//!   古い表は書き換えないので、 切り替えの間も読み手は止まらない (古い表を読んだ読み手は、 その後に入った語を
//!   見落とすだけ — `try_get_or_insert` の重複検査が拾う)
//! - 直後に置けない (語数が領域の約 1/4 を超えた) 時だけ、 領域全体を 1 つの表に組み直す (`full`)。 この時は
//!   読み手を待たせる (header の版が奇数の間、 読み手は待って読み直す)。 辞書の上限は昔と同じ
//! - 今の表は index header の 8..16 に 1 語 (`Gen`) で置く。 別 process の読み手 (readonly open) も毎回ここを読む
//!
//! 形式: magic `VIX4`。 graceful close の印は data header の clean flag = `CLEAN_GEN` (2)。 旧 binary は
//! clean flag が 1 でなければ全域の表として作り直すので、 `VIX4` の DB を開いても正しく引ける (全域の表に
//! 入れ直す)。 旧 binary が閉じた (clean flag = 1) `VIX4` の DB は、 表が全域の形になっているので、 新 binary は
//! 作り直す。 `VIX3` (全域の表、 0.14 〜 0.28.5) / `VIX2` (slot が hash 下位ビット、 0.14 以前) の DB は、 書き手が
//! 開いた時に data から作り直して `VIX4` にする。

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use crate::region::Region;

/// #381: 番号 (vid) = 世代 (上位 2 bit) | 場所 (下位 30 bit、 offsets の添字)。 免許証番号の末尾の再交付回数と同じで、
/// 語を回収して場所を別の語に渡すたびにその場所の世代を 1 つ進める。 古い番号は世代が合わず該当なしになる
/// (別の語に当たらない)。 世代は offsets の長さの上位 2 bit に置く (世代 0 = 今までの番号そのもの)。
pub const VID_GEN_SHIFT: u32 = 30;
/// 番号の場所の部分。
pub const VID_SLOT_MASK: u32 = (1 << VID_GEN_SHIFT) - 1;
/// offsets の長さの欄のうち長さの部分 (上位 2 bit は世代)。 1 語の長さの上限。
const LEN_MASK: u32 = VID_SLOT_MASK;
/// 場所の数の上限。 世代 3 の最後の場所は `u32::MAX` (空の印) になるので使わない。
pub const MAX_SLOTS: u32 = VID_SLOT_MASK;
/// 参照数の欄の印: 回収してこの場所を別の語に渡している最中 (#381)。
const CLAIMED: u32 = u32::MAX;

/// 番号の場所 (offsets の添字)。
#[inline]
pub fn vid_slot(vid: u32) -> u32 {
    vid & VID_SLOT_MASK
}

/// 番号の世代。
#[inline]
pub fn vid_gen(vid: u32) -> u32 {
    vid >> VID_GEN_SHIFT
}

#[inline]
fn make_vid(generation: u32, slot: u32) -> u32 {
    (generation & 3) << VID_GEN_SHIFT | slot
}

/// #381: 語の回収 (`Vocabulary::enable_reclaim` で有効)。 場所ごとの参照数 (Tag の cell + 書き手が押さえている分) と、
/// 参照数が 0 になった場所の待ち行列 (いちばん昔に空いたものから使い回す)。
struct Reclaim {
    /// 場所ごとの参照数。 `CLAIMED` = 回収して別の語に渡している最中。
    refs: RefStore,
    /// 参照数が 0 になった場所 (先入れ先出し)。 取り出した時に参照数が 0 でなければ (誰かが使い直した) 捨てる。
    free: std::sync::Mutex<std::collections::VecDeque<u32>>,
    /// 既存の cell を数え終えたか。 数え終わるまでは回収しない (数えていない参照があるので)。
    ready: AtomicBool,
    /// 使い回した回数 (観測用)。
    claims: AtomicU64,
}

/// #385: 参照数の置き場。 file (`vocab.refs.seg`) に置けば閉じても残り、 次に開いた時に数え直さずに使える。
/// file を持てない backing (packed / wasm) は heap に置き、 開くたびに数える。
enum RefStore {
    Heap(Box<[AtomicU32]>),
    /// 先頭 `REFS_HEADER` byte の後ろに場所ごとの u32。 commit した所までしか触らない。
    Mapped(Region),
}

impl RefStore {
    /// 場所 `slot` の参照数。 範囲外 (file なら commit していない所) は None。
    #[inline]
    fn at(&self, slot: u32) -> Option<&AtomicU32> {
        match self {
            RefStore::Heap(b) => b.get(slot as usize),
            RefStore::Mapped(r) => {
                let off = REFS_HEADER + slot as usize * 4;
                (off + 4 <= r.committed_len()).then(|| r.as_atomic_u32(off))
            }
        }
    }

    /// 場所 `slot` まで参照数を置けるようにする (file は commit を伸ばす)。
    fn ensure(&self, slot: u32) -> std::io::Result<()> {
        match self {
            RefStore::Heap(b) if (slot as usize) < b.len() => Ok(()),
            RefStore::Heap(_) => Err(std::io::Error::new(std::io::ErrorKind::OutOfMemory, "vocab refs: slot out of range")),
            RefStore::Mapped(r) => r.ensure_committed(REFS_HEADER + (slot as usize + 1) * 4),
        }
    }
}

/// #385: 参照数の file の header (magic 4 + 数えた場所の数 4 + 予約 8)。
const REFS_HEADER: usize = 16;
const REFS_MAGIC: [u8; 4] = [b'V', b'R', b'F', b'1'];

/// 0 で埋まった `n` 個の参照数 (calloc の 0 ページのまま確保する — 触った分だけ RAM を食う)。
fn zeroed_counters(n: usize) -> Box<[AtomicU32]> {
    let v = std::mem::ManuallyDrop::new(vec![0u32; n]);
    // SAFETY: AtomicU32 は u32 と大きさ・整列が同じで、 0 は正しい値。 `vec![0; n]` は len == capacity
    unsafe { Vec::from_raw_parts(v.as_ptr() as *mut AtomicU32, v.len(), v.capacity()) }.into_boxed_slice()
}

const MAGIC: [u8; 4] = [b'V', b'O', b'C', b'1'];
const HEADER: usize = 16;
/// data header byte 12 に書く「index は data と consistent」 マーカー。
/// 0 = dirty (rebuild 要)、 `CLEAN_GEN` = clean (rebuild skip 可)。 1 は旧 binary の clean (全域の表、 #374)。
/// 残り 13-15 byte は reserved。
const CLEAN_FLAG_OFF: usize = 12;
/// #374: 伸びる表の形式で graceful close した印。 旧 binary は `!= 1` を dirty と見て作り直す。
const CLEAN_GEN: u32 = 2;
/// #385: `CLEAN_GEN` に加えて、 参照数の file (`vocab.refs.seg`) も閉じた時の cell と合っている印。 回収する DB だけが
/// 書く。 0.29.0 は `CLEAN_GEN` でないので索引を作り直す (遅いだけ) うえ、 閉じる時に `CLEAN_GEN` を書く = 0.29.0 が
/// 書いた後の参照数は信じない (数え直す)。
const CLEAN_REFS: u32 = 3;
/// #374: 伸びる表 (今の表の位置と大きさを header の `GEN_OFF` に持つ)。
const INDEX_MAGIC: [u8; 4] = [b'V', b'I', b'X', b'4'];
/// 全域の表 (0.14 〜 0.28.5)。 書き手は開いた時に `VIX4` へ作り直す。 readonly はそのまま全域の表として読む。
const INDEX_MAGIC_V3: [u8; 4] = [b'V', b'I', b'X', b'3'];
/// #123: slot 選択を hash 下位ビット (`h & mask`) から **上位ビット** に変えた前の index。
/// 0.14 以前の DB はこの magic を持つ。 書き手は開いた時に作り直す、 readonly は data から shadow を組む
/// (どちらも `VIX4` でも clean な `VIX3` でもない、 で決まるので、 code では test が旧 index を再現する時だけ使う)。
#[cfg_attr(not(test), allow(dead_code))]
const INDEX_MAGIC_V2: [u8; 4] = [b'V', b'I', b'X', b'2'];
const INDEX_HEADER: usize = 16;
const INDEX_SLOT_SIZE: usize = 13;
/// index header の中の今の表 (`Gen`) の位置 (8 byte、 atomic)。
const GEN_OFF: usize = 8;
/// 最初の表の slot 数 (13 KB)。
const FIRST_GEN_CAP: u32 = 1024;

#[cfg(test)]
thread_local! {
    static FAIL_AFTER_TAKE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// test 用の割り込み点 (#374): 読み手が表を読んだ直後に 1 回だけ走らせる処理。 書き手が表を組み直す窓を、
    /// thread の運に頼らず踏むため。
    static AFTER_TABLE_READ: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    /// test 用の割り込み点 (#381): insert が data の上限の先取りを抜けた直後に 1 回だけ走らせる処理。 並行 insert が
    /// 同時に先取りを抜ける窓を踏むため。
    static AFTER_PRECHECK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

/// 読み手が表を読んだ直後の割り込み点 (test 以外では何もしない)。
#[inline(always)]
fn hook_after_table_read() {
    #[cfg(test)]
    if let Some(f) = AFTER_TABLE_READ.with(|h| h.borrow_mut().take()) {
        f();
    }
}

/// insert が data の上限の先取りを抜けた直後の割り込み点 (test 以外では何もしない)。
#[inline(always)]
fn hook_after_precheck() {
    #[cfg(test)]
    if let Some(f) = AFTER_PRECHECK.with(|h| h.borrow_mut().take()) {
        f();
    }
}

/// `Vocabulary::try_insert` が値を入れられなかった理由 (#316)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VocabFail {
    /// `vocab_max_entries` / 索引の天井に着いた (この先も入らない)
    Full,
    /// ディスクの空き不足で領域を伸ばせない (#167、 空けば入る)
    Space,
}

/// 今の表: 領域の slot `base .. base + 2^bits` + 版 (#374)。 header の `GEN_OFF` に 1 語で置く
/// (`base` 32 bit | `bits` 8 bit | 版 24 bit)。 版が奇数の間は表を組み直している (読み手は待つ)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Gen {
    base: u32,
    bits: u8,
    ver: u32,
}

impl Gen {
    const VER_MASK: u32 = (1 << 24) - 1;

    #[inline]
    fn cap(self) -> u32 {
        1u32 << self.bits
    }

    #[inline]
    fn word(self) -> u64 {
        self.base as u64 | (self.bits as u64) << 32 | ((self.ver & Self::VER_MASK) as u64) << 40
    }

    #[inline]
    fn from_word(w: u64) -> Self {
        Gen { base: w as u32, bits: (w >> 32) as u8, ver: (w >> 40) as u32 & Self::VER_MASK }
    }

    #[inline]
    fn busy(self) -> bool {
        self.ver & 1 == 1
    }

    /// 表の `i` 番目の slot の、 index 領域の中の位置。
    #[inline]
    fn slot_off(self, i: usize) -> usize {
        INDEX_HEADER + (self.base as usize + i) * INDEX_SLOT_SIZE
    }

    /// 表の領域 (byte、 `[start, end)`)。
    #[inline]
    fn span(self) -> (usize, usize) {
        (self.slot_off(0), self.slot_off(self.cap() as usize))
    }
}

pub struct Vocabulary {
    data: Region,
    offsets: Region,
    index: Region,
    /// #77-H1: readonly open で index が dirty (clean_flag≠1) な場合、 共有
    /// mmap の index を書き換えずに **heap 上のここへ**再構築する。 Some の
    /// とき lookup はこちらを参照。 writer open では常に None。
    /// 旧実装は readonly でも `rebuild_index` が共有 index をゼロクリアして
    /// おり、 併走中の writer の index entry を恒久消失させていた。
    ///
    /// #127: 形式は index layout の byte 複製 (`Box<[u8]>`、 index_cap × 13B) から
    /// **count 比例の compact 形式** `(fxhash(value), vid)` sorted に変更。 旧形式は
    /// 確保こそ calloc (仮想) だが、 #123 で hash が一様分散になったため rebuild が
    /// shadow の全ページに live slot を書いて **index_cap 比例の anon RSS** を
    /// Engine 寿命の間占有していた (dirty DB の readonly open 1 回ごと。 全文検索の消費側アプリ
    /// 1GB VPS の boot +~300MB の正体)。 lookup は hash の binary search + 同 hash
    /// 内の値比較で、 旧 probe と同じ「最小 vid が勝つ」 解決を保つ。
    shadow_index: Option<Vec<(u64, u32)>>,
    /// #374: readonly open した全域の表 (`VIX3`、 clean)。 header に今の表を持たないので、 ここに全域の表を置く。
    /// 別 process の書き手が `VIX4` に作り直したら (magic が変わったら) header の表を読む。
    legacy_gen: Option<Gen>,
    /// #374: 表を伸ばす間、 書き込み (`index_insert`) を止める。 読み (`lookup`) は取らない。
    grow_lock: std::sync::RwLock<()>,
    /// #77-M1: disk 上の clean_flag のキャッシュ。 flush() が 1 を書いた後の
    /// 最初の insert で 0 に戻すための判定に使う (旧実装は open 時の 1 回
    /// しか 0 に倒さず、 flush 後の追加 write 中 crash で破損 index を
    /// 次 open が無検証採用した)。
    clean_on_disk: std::sync::atomic::AtomicBool,
    /// #101: この instance の load で rebuild_index が走ったか (= dirty open だったか)。
    /// 観測専用 (graceful close の regression test / 診断)。 init (新規) は false。
    pub rebuilt_on_load: bool,
    count: AtomicU32,
    data_end: AtomicU32,
    /// 語数の上限 (header の `vocab_max_entries`)。 `grow` で伸びる (#381)。
    max_entries: AtomicU32,
    /// offsets 領域に入る語数 (= 予約の長さ / 8)。 `max_entries` はこの範囲で伸びる。 破損 slot の vid を
    /// 読み飛ばす境界にも使う (`max_entries` は他 process の書き手が伸ばしうるので、 読み手の境界にしない)。
    offsets_cap: u32,
    /// data の byte 数の上限 (header の `vocab_data_size`)。 `grow` で伸びる (#381)。
    data_limit: AtomicU32,
    /// 索引の領域の slot 数 (2 の冪)。 表はこの中に置く (#374)。 `grow` で伸びる (#381)。
    index_cap: AtomicU32,
    /// readonly open (領域は書けない)。
    readonly: bool,
    /// #381: 語の回収 (有効な時だけ Some)。
    reclaim: std::sync::OnceLock<Reclaim>,
    /// #385: 開いた時の clean flag が `CLEAN_REFS` だった (= 参照数の file を数え直さずに使える)。
    refs_clean_on_load: bool,
    /// #381: 今の表の中の、 語が入れ替わって死んだ entry の数 (回収で場所を別の語に渡すたびに 1)。 表の埋まりに
    /// 数え、 増えたら表を作り直して落とす。
    stale_entries: AtomicU32,
}

unsafe impl Sync for Vocabulary {}
unsafe impl Send for Vocabulary {}

/// #123: slot は **hash の上位ビット**から採る。
///
/// `fxhash` は乗算で終わる (`h = (h.rotate_left(5) ^ word).wrapping_mul(SEED)`) ため、
/// **積の下位 k bit は両オペランドの下位 k bit だけで決まり**、 上位側の entropy が下位に
/// 伝播しない。 旧実装 (VIX2) は `h & mask` で下位を採っていたので、 構造の似たキーが同一
/// slot に集まって linear probe のクラスタが伸びていた (実例: `第10条` / `第11条` / `第12条`
/// は 1 word ちょうどで `h = word * SEED`、 下位 32 bit が完全一致)。 上位ビットなら乗算の
/// 桁上がりが載る。
///
/// `cap` は 2^n。 cap == 1 の退化ケースは 0。
#[inline]
fn home_slot(h: u64, cap: u32) -> usize {
    let bits = cap.trailing_zeros();
    if bits == 0 { 0 } else { (h >> (64 - bits)) as usize }
}

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
    /// `index` は header だけ書く。 表は最初 `FIRST_GEN_CAP` slot (#374)。
    pub fn init(data: Region, offsets: Region, index: Region, max_entries: u32, index_cap: u32) -> Self {
        let index_cap = index_cap.next_power_of_two();
        let first = Gen { base: 0, bits: FIRST_GEN_CAP.min(index_cap).trailing_zeros() as u8, ver: 0 };
        index.write_at(0, &INDEX_MAGIC);
        index.write_at(4, &index_cap.to_le_bytes());
        index.write_at(GEN_OFF, &first.word().to_le_bytes());

        let (offsets_cap, data_limit) = Self::region_caps(&offsets, &data);
        let offsets_cap = offsets_cap.min(MAX_SLOTS);
        Self {
            data, offsets, index,
            shadow_index: None,
            legacy_gen: None,
            grow_lock: std::sync::RwLock::new(()),
            clean_on_disk: std::sync::atomic::AtomicBool::new(false),
            rebuilt_on_load: false,
            count: AtomicU32::new(0),
            data_end: AtomicU32::new(HEADER as u32),
            max_entries: AtomicU32::new(max_entries.min(offsets_cap)),
            offsets_cap,
            data_limit: AtomicU32::new(data_limit),
            index_cap: AtomicU32::new(index_cap),
            readonly: false,
            reclaim: std::sync::OnceLock::new(),
            refs_clean_on_load: false,
            stale_entries: AtomicU32::new(0),
        }
    }

    /// 既存領域をロード。
    ///
    /// data の先頭 4 バイトが MAGIC でない (= 全 0 = lazy fresh、 一度も
    /// insert されてない) 場合は count=0 / data_end=HEADER の fresh
    /// state を返す。 これで `insert` が遅延書き込みする MAGIC を待たずに
    /// open できる。
    ///
    /// 索引をそのまま使えるのは `VIX4` + clean flag `CLEAN_GEN` の時だけ。 それ以外 (crash 後 / 旧 binary が閉じた /
    /// `VIX3`・`VIX2`) は、 書き手なら data から語数に合った表を作り直し (`rebuild_from_data`)、 readonly なら
    /// heap に shadow を組む (`VIX3` + clean 1 の readonly だけは全域の表をそのまま読む)。
    ///
    /// #327: 索引を作り直すのに要るページを書く空きが無ければ、 書かずにエラー。
    pub fn load(data: Region, offsets: Region, index: Region, readonly: bool) -> std::io::Result<Self> {
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

        let (offsets_cap, data_limit) = Self::region_caps(&offsets, &data);
        let offsets_cap = offsets_cap.min(MAX_SLOTS);

        let xm = index.slice();
        let index_cap = u32::from_le_bytes(xm[4..8].try_into().unwrap());
        let magic: [u8; 4] = xm[0..4].try_into().unwrap();
        let usable = magic == INDEX_MAGIC && (is_fresh || clean_flag == CLEAN_GEN || clean_flag == CLEAN_REFS);
        // readonly で全域の表 (VIX3) を graceful close のまま読む
        let legacy_clean = readonly && magic == INDEX_MAGIC_V3 && (is_fresh || clean_flag == 1);

        let mut v = Self {
            data, offsets, index,
            shadow_index: None,
            legacy_gen: None,
            grow_lock: std::sync::RwLock::new(()),
            clean_on_disk: std::sync::atomic::AtomicBool::new(usable && !is_fresh),
            rebuilt_on_load: false,
            count: AtomicU32::new(count),
            data_end: AtomicU32::new(data_end),
            // 上限は領域から決めた仮の値。 engine が header の値を `set_limits` で入れる (#381)
            max_entries: AtomicU32::new(offsets_cap),
            offsets_cap,
            data_limit: AtomicU32::new(data_limit),
            index_cap: AtomicU32::new(index_cap),
            readonly,
            reclaim: std::sync::OnceLock::new(),
            refs_clean_on_load: usable && !is_fresh && clean_flag == CLEAN_REFS,
            stale_entries: AtomicU32::new(0),
        };
        if usable {
            return Ok(v);
        }
        if legacy_clean {
            v.legacy_gen = Some(Gen { base: 0, bits: index_cap.trailing_zeros() as u8, ver: 0 });
            return Ok(v);
        }
        v.rebuilt_on_load = !is_fresh;
        if readonly {
            // #77-H1 / #127: 共有 mmap は書かない。 count 比例の compact shadow を data/offsets (= ground truth)
            // から構築する。 count は破損 header 対策で offsets 領域に clamp する。
            let count = v.count.load(Ordering::Relaxed).min(v.offsets_cap);
            let mut shadow: Vec<(u64, u32)> = Vec::with_capacity(count as usize);
            for slot in 0..count {
                let (value, generation) = read_slot(&v.offsets, &v.data, slot);
                shadow.push((fxhash(value), make_vid(generation, slot)));
            }
            // (hash, vid) 昇順 = 同 hash 内は vid 昇順。 lookup の線形走査が
            // 最小 vid から当たるので、 旧 probe の dup 解決 (先着 vid) と一致。
            shadow.sort_unstable();
            v.shadow_index = Some(shadow);
        } else {
            v.rebuild_from_data()?;
        }
        Ok(v)
    }

    /// 今の表。 header から読む (別 process の書き手が伸ばしても追える)。
    #[inline]
    fn table(&self) -> Gen {
        if let Some(g) = self.legacy_gen {
            // 別 process の書き手が VIX4 に作り直すまでは全域の表
            if self.index.as_atomic_u32(0).load(Ordering::Acquire) != u32::from_le_bytes(INDEX_MAGIC) {
                return g;
            }
        }
        Gen::from_word(self.index.as_atomic_u64(GEN_OFF).load(Ordering::Acquire))
    }

    /// 書き手専用 (開いた時、 単一スレッド): data/offsets (= ground truth) から、 語数に合った表を領域の先頭に
    /// 作り直して `VIX4` にする (#374)。 同じ値が 2 つの id にある時は小さい id が勝つ (旧 `plan_rebuild` と同じ)。
    /// 語を一度に集めず、 表に入れながら重複を見る (語数に比例するヒープを取らない)。
    ///
    /// #327: 書く表の領域の空きを先に確かめる。 空きが無ければ何も書かずにエラー。
    fn rebuild_from_data(&mut self) -> std::io::Result<()> {
        let count = self.count.load(Ordering::Relaxed).min(self.offsets_cap);
        let bits = Self::bits_for(count as usize, self.index_cap());
        let old = self.gen_on_disk();
        let next = Gen { base: 0, bits, ver: old.ver.wrapping_add(2) & !1 };
        let (offsets, data) = (&self.offsets, &self.data);
        let ids = (0..count).map(|slot| {
            let (value, generation) = read_slot(offsets, data, slot);
            (fxhash(value), make_vid(generation, slot))
        });
        self.write_gen(Some(old), next, ids, true)?;
        self.index.write_at(0, &INDEX_MAGIC);
        self.index.mark_dirty(0, 4);
        Ok(())
    }

    /// header の今の表 (`VIX4` 以外なら、 版 0 の空の表)。
    fn gen_on_disk(&self) -> Gen {
        if self.index.slice()[0..4] == INDEX_MAGIC {
            Gen::from_word(self.index.as_atomic_u64(GEN_OFF).load(Ordering::Acquire))
        } else {
            Gen { base: 0, bits: 0, ver: 0 }
        }
    }

    /// `n` 語を半分以下の埋まり方で持つ表の大きさ (`FIRST_GEN_CAP` 以上、 領域以下、 2 の冪の指数)。
    fn bits_for(n: usize, index_cap: u32) -> u8 {
        let want = (n.saturating_mul(2).max(1) as u64).next_power_of_two().max(FIRST_GEN_CAP as u64);
        want.min(index_cap as u64).trailing_zeros() as u8
    }

    /// 表 `next` を書いて切り替える (書き手、 `grow_lock` の write か開いた時)。 `next` の領域を 0 にしてから
    /// `entries` (hash, vid) を入れ、 header を `next` に。 `stall` (= 今の表 `old` と領域が重なる) なら、 書く間は
    /// header の版を奇数にして読み手を待たせる。 `dedup` なら、 同じ値が既に入っていれば入れない (先に来た vid が勝つ)。
    ///
    /// 空きは先に確かめる (書く表の全域、 #327)。 足りなければ何も書かずに Err。
    fn write_gen(
        &self,
        stall: Option<Gen>,
        next: Gen,
        entries: impl IntoIterator<Item = (u64, u32)>,
        dedup: bool,
    ) -> std::io::Result<()> {
        let (start, end) = next.span();
        self.index.ensure_committed_sparse(end, end - start)?;
        let word = self.index.as_atomic_u64(GEN_OFF);
        if let Some(old) = stall {
            word.store(Gen { ver: old.ver | 1, ..old }.word(), Ordering::Release);
        }
        self.index.fill_at(start, end - start, 0);
        let mask = (next.cap() - 1) as u64;
        // slot の読みは毎回取り直す (書き込みと借用を重ねない、 #83)
        let slot = |off: usize| -> (u8, u64, u32) {
            let xm = self.index.slice();
            (
                xm[off],
                u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap()),
                u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap()),
            )
        };
        'entries: for (h, vid) in entries {
            let mut idx = home_slot(h, next.cap());
            for _ in 0..next.cap() as usize {
                let off = next.slot_off(idx);
                let (flag, slot_h, other) = slot(off);
                if flag == 0 {
                    self.index.write_at(off + 1, &h.to_le_bytes());
                    self.index.write_at(off + 9, &vid.to_le_bytes());
                    self.index.as_atomic_u8(off).store(1, Ordering::Release);
                    continue 'entries;
                }
                if dedup && slot_h == h && self.get(other) == self.get(vid) {
                    continue 'entries; // 同じ値 (先の vid が勝つ)
                }
                idx = ((idx as u64 + 1) & mask) as usize;
            }
            // 表が満杯 (語数より大きい表を選ぶので来ない): 残りは入れない
            break;
        }
        self.index.mark_dirty(start, end - start);
        word.store(next.word(), Ordering::Release);
        self.index.mark_dirty(GEN_OFF, 8);
        Ok(())
    }

    /// 語数が今の表の半分を超えていたら、 表を伸ばす (書き手、 `try_insert` の後で)。 空き不足で伸ばせなければ
    /// 何もしない (今の表で続け、 次の insert でまた試す)。
    ///
    /// #381: 回収で場所を別の語に渡すと、 古い語の entry は死んだまま表に残る (`stale_entries`)。 死んだ entry が
    /// 表の 1/4 を超えたら (`force` なら 1 つでもあれば)、 同じ大きさで作り直して落とす。
    fn maybe_grow(&self) {
        self.rebuild_table(false);
    }

    fn rebuild_table(&self, force: bool) {
        let needs = |g: Gen, index_cap: u32| {
            let (count, stale) =
                (self.count.load(Ordering::Relaxed) as u64, self.stale_entries.load(Ordering::Relaxed) as u64);
            (g.cap() < index_cap && count * 2 > g.cap() as u64) || stale * 4 > g.cap() as u64 || (force && stale > 0)
        };
        if self.legacy_gen.is_some() || self.shadow_index.is_some() || !needs(self.table(), self.index_cap()) {
            return;
        }
        let _w = self.grow_lock.write().unwrap_or_else(|p| p.into_inner());
        // 上限が伸びて (#381) 領域が広がったかもしれないので、 lock の中で読み直す
        let index_cap = self.index_cap();
        let g = self.table();
        if !needs(g, index_cap) {
            return;
        }
        // 今の表の slot を集める (表は書き込みを止めているので確定している)。 語が入れ替わった entry は落とす
        let count = self.count.load(Ordering::Relaxed);
        let mut entries = Vec::with_capacity(count as usize);
        let xm = self.index.slice();
        for i in 0..g.cap() as usize {
            let off = g.slot_off(i);
            if xm[off] == 1 {
                let h = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
                let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                if self.get_checked(vid).is_some() {
                    entries.push((h, vid));
                }
            }
        }
        // 語数で伸ばす時だけ倍にする (死んだ entry を落とすだけなら同じ大きさ)
        let grow = (g.cap() < index_cap && count as u64 * 2 > g.cap() as u64) as u8;
        let bits = Self::bits_for(count as usize, index_cap).max(g.bits + grow);
        let ver = g.ver.wrapping_add(2);
        let after = Gen { base: g.base + g.cap(), bits, ver };
        let written = if after.base as u64 + after.cap() as u64 <= index_cap as u64 {
            // 今の表の直後に置く: 今の表は書き換えないので、 読み手は止めない
            self.write_gen(None, after, entries, false)
        } else {
            // 直後に置けない: 領域の先頭から組み直す (今の表と重なるので読み手を待たせる)
            let full = Gen { base: 0, bits, ver };
            self.write_gen(Some(g), full, entries, false)
        };
        if written.is_ok() {
            self.stale_entries.store(0, Ordering::Relaxed);
        }
    }

    /// 満杯なら **`u32::MAX` (予約 sentinel)** を返す (#59: panic しない)。
    /// 理由 (一杯 / 空き不足) が要る時は `try_get_or_insert`。
    pub fn get_or_insert(&self, value: &[u8]) -> u32 {
        self.try_get_or_insert(value).unwrap_or(u32::MAX)
    }

    /// `get_or_insert` の理由付き版 (#316)。
    ///
    /// #381: 回収が有効な辞書で、 返した番号を cell に書くなら `try_get_or_insert_pinned` を使う (こちらは押さえない
    /// ので、 書くまでの間に回収されうる — 書く時に世代で弾かれる)。
    pub fn try_get_or_insert(&self, value: &[u8]) -> Result<u32, VocabFail> {
        if let Some(id) = self.lookup(value) { return Ok(id); }
        let id = self.try_insert(value)?;
        // 並列挿入の競合チェック: 別スレッドが先に同じ値を挿入した場合、先着のidを使う
        if let Some(winner) = self.lookup(value) {
            if winner != id { return Ok(winner); }
        }
        Ok(id)
    }

    /// `try_get_or_insert` の押さえる版 (#381): 返した番号の参照を 1 つ持った状態で返す (cell に書くまでの間に
    /// 回収されない)。 書いた後 (書けても書けなくても) `release` で返すこと。 回収が無効なら `try_get_or_insert` と同じ。
    pub fn try_get_or_insert_pinned(&self, value: &[u8]) -> Result<u32, VocabFail> {
        loop {
            if let Some(id) = self.lookup(value) {
                if self.acquire(id) {
                    return Ok(id);
                }
                // 引いた後に回収された: 引き直す (次は見つからないか、 別の書き手が入れ直した番号)
                continue;
            }
            let id = self.try_insert_with(value, true)?;
            // 並列挿入の競合: 先着の番号を使う。 自分の番号は押さえを返す (参照 0 = 回収の候補、 #378 の負け番号)
            if let Some(winner) = self.lookup(value)
                && winner != id
            {
                self.release(id);
                if self.acquire(winner) {
                    return Ok(winner);
                }
                continue;
            }
            return Ok(id);
        }
    }

    // ──── #381: 語の回収 ────

    /// 参照数の file (`vocab.refs.seg`) の予約長 (#385)。 offsets に入る語数ぶん。
    pub fn refs_region_size(&self) -> usize {
        REFS_HEADER + self.offsets_cap as usize * 4
    }

    /// 語の回収を有効にする (書き手、 開いた時に 1 回)。 `refs` は参照数の file (#385、 None = heap に置く)。
    ///
    /// 戻り値は参照数がもう揃っているか。 前回きれいに閉じた (`CLEAN_REFS`) file なら true で、 すぐ回収を始める。
    /// false なら参照数は 0 から — 既存の cell を数え (`count_ref`)、 `finish_refs` を呼んでから回収が始まる。
    pub fn enable_reclaim(&self, refs: Option<Region>) -> bool {
        // file を今の語数まで伸ばせない (空き不足) なら heap に置く (file のまま参照を置けないと Tag を書けない)
        let refs = refs.filter(|r| r.ensure_committed(REFS_HEADER + self.count() as usize * 4).is_ok());
        let (store, trusted) = match refs {
            Some(r) => {
                let m = r.slice();
                let trusted = self.refs_clean_on_load
                    && m[0..4] == REFS_MAGIC
                    && u32::from_le_bytes(m[4..8].try_into().unwrap()) == self.count();
                if !trusted {
                    // 前の数は使わない (数え直す)
                    let len = r.committed_len();
                    if len > REFS_HEADER {
                        r.fill_at(REFS_HEADER, len - REFS_HEADER, 0);
                    }
                }
                r.write_at(0, &REFS_MAGIC);
                (RefStore::Mapped(r), trusted)
            }
            None => (RefStore::Heap(zeroed_counters(self.offsets_cap as usize)), false),
        };
        let set = self.reclaim.set(Reclaim {
            refs: store,
            free: std::sync::Mutex::new(std::collections::VecDeque::new()),
            ready: AtomicBool::new(false),
            claims: AtomicU64::new(0),
        });
        if set.is_ok() && trusted {
            self.finish_refs();
        }
        trusted
    }

    /// #385: 閉じる時、 参照数の file に今の語数を書く (engine が file を msync した後で `mark_index_clean_refs`)。
    pub fn seal_refs(&self) -> bool {
        match self.reclaim.get().map(|r| &r.refs) {
            Some(RefStore::Mapped(r)) if r.committed_len() >= REFS_HEADER => {
                r.write_at(4, &self.count().to_le_bytes());
                true
            }
            _ => false,
        }
    }

    /// 回収が有効か。
    pub fn reclaim_enabled(&self) -> bool {
        self.reclaim.get().is_some()
    }

    /// 番号 `vid` の参照を 1 つ取る (cell に書く / 書くまで押さえる)。 世代が合わない (回収された) / 回収している
    /// 最中なら false (取らない)。 回収が無効なら常に true。
    pub fn acquire(&self, vid: u32) -> bool {
        let Some(r) = self.reclaim.get() else { return true };
        let slot = vid_slot(vid);
        if slot >= self.count() {
            return false;
        }
        let Some(c) = r.refs.at(slot) else { return false };
        let mut cur = c.load(Ordering::Acquire);
        loop {
            if cur == CLAIMED {
                return false;
            }
            match c.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(x) => cur = x,
            }
        }
        // 取った後で世代を見る: 回収は 「参照 0 → CLAIMED → 世代を進める → 参照を戻す」 の順なので、 取る前に回収が
        // 終わっていれば世代はもう進んでいる
        if self.slot_gen(slot) != vid_gen(vid) {
            self.release_slot(r, slot);
            return false;
        }
        true
    }

    /// 番号 `vid` の参照を 1 つ返す。 0 になったら (既存の cell を数え終えていれば) 回収の待ち行列へ。
    pub fn release(&self, vid: u32) {
        if let Some(r) = self.reclaim.get() {
            self.release_slot(r, vid_slot(vid));
        }
    }

    fn release_slot(&self, r: &Reclaim, slot: u32) {
        let Some(c) = r.refs.at(slot) else { return };
        let mut cur = c.load(Ordering::Acquire);
        loop {
            // 0 / CLAIMED から返すのは数え違い (取っていない参照を返した)。 0 を割らない
            if cur == 0 || cur == CLAIMED {
                debug_assert!(false, "vocab slot {slot}: release without acquire (refs {cur})");
                return;
            }
            match c.compare_exchange_weak(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break,
                Err(x) => cur = x,
            }
        }
        if cur == 1 && r.ready.load(Ordering::Acquire) {
            r.free.lock().unwrap_or_else(|p| p.into_inner()).push_back(slot);
        }
    }

    /// 既存の cell が持つ番号を 1 つ数える (`Engine` が開いた後に列を読んで呼ぶ)。 世代が合わない cell は数えない。
    pub fn count_ref(&self, vid: u32) -> bool {
        self.acquire(vid)
    }

    /// 既存の cell を数え終えた: 参照 0 の場所を待ち行列に入れ (場所の順)、 回収を始める。
    pub fn finish_refs(&self) {
        let Some(r) = self.reclaim.get() else { return };
        // 先に ready を立てる: 以後に 0 になった場所は release が入れる (下の走査と重なっても、 取り出す時に
        // 参照 0 を CAS で確かめるので二重は害が無い)
        r.ready.store(true, Ordering::Release);
        let count = self.count();
        let mut q = r.free.lock().unwrap_or_else(|p| p.into_inner());
        for slot in 0..count {
            // 置けなかった場所 (#328 で捨てた番号の commit 失敗) は誰も持たないが、 使い回しもしない
            if r.refs.at(slot).is_some_and(|c| c.load(Ordering::Acquire) == 0) {
                q.push_back(slot);
            }
        }
    }

    /// 回収を始めたか (既存の cell を数え終えたか)。
    pub fn reclaim_ready(&self) -> bool {
        self.reclaim.get().is_some_and(|r| r.ready.load(Ordering::Acquire))
    }

    /// (待ち行列の長さ、 使い回した回数)。 観測用。
    pub fn reclaim_stats(&self) -> (usize, u64) {
        match self.reclaim.get() {
            Some(r) => (r.free.lock().unwrap_or_else(|p| p.into_inner()).len(), r.claims.load(Ordering::Relaxed)),
            None => (0, 0),
        }
    }

    /// 番号の場所の参照数 (test / 観測用)。
    pub fn ref_count(&self, vid: u32) -> Option<u32> {
        self.reclaim.get().and_then(|r| r.refs.at(vid_slot(vid))).map(|c| c.load(Ordering::Acquire))
    }

    /// 待ち行列からいちばん昔に空いた場所を取る (参照 0 → CLAIMED)。 取り出した時に参照が戻っていた場所は捨てる。
    fn claim(&self) -> Option<u32> {
        let r = self.reclaim.get()?;
        if !r.ready.load(Ordering::Acquire) {
            return None;
        }
        let mut q = r.free.lock().unwrap_or_else(|p| p.into_inner());
        while let Some(slot) = q.pop_front() {
            if r.refs.at(slot).is_some_and(|c| c.compare_exchange(0, CLAIMED, Ordering::AcqRel, Ordering::Relaxed).is_ok()) {
                return Some(slot);
            }
        }
        None
    }

    /// 取った場所を使えなかった (data の上限 / 空き不足): 元に戻して待ち行列の先頭へ。
    fn unclaim(&self, slot: u32) {
        if let Some(r) = self.reclaim.get() {
            if let Some(c) = r.refs.at(slot) {
                c.store(0, Ordering::Release);
            }
            r.free.lock().unwrap_or_else(|p| p.into_inner()).push_front(slot);
        }
    }

    #[inline]
    pub fn get(&self, vid: u32) -> &[u8] {
        self.get_checked(vid).unwrap_or(&[])
    }

    /// 番号 `vid` の語。 世代が合わない (語が回収されて場所が別の語に渡った、 #381) / 場所が範囲外なら None。
    #[inline]
    pub fn get_checked(&self, vid: u32) -> Option<&[u8]> {
        let slot = vid_slot(vid);
        if slot >= self.offsets_cap {
            return None;
        }
        let (value, generation) = read_slot(&self.offsets, &self.data, slot);
        (generation == vid_gen(vid)).then_some(value)
    }

    /// 場所 `slot` の今の語 (世代は見ない)。 辞書全体を走査する観測用 (#381)。
    pub fn get_slot(&self, slot: u32) -> &[u8] {
        if slot >= self.offsets_cap {
            return &[];
        }
        read_slot(&self.offsets, &self.data, slot).0
    }

    /// 場所 `slot` の今の世代。
    #[inline]
    fn slot_gen(&self, slot: u32) -> u32 {
        ((self.offsets.as_atomic_u64(slot as usize * 8).load(Ordering::Acquire) >> 32) as u32) >> VID_GEN_SHIFT
    }

    #[inline]
    pub fn lookup(&self, value: &[u8]) -> Option<u32> {
        // #77-H1 / #127: readonly open で dirty だった場合は compact shadow を参照。
        // hash の partition_point → 同 hash 区間を vid 昇順に値比較 (probe と同じ
        // 「先着 vid が勝つ」 解決)。
        if let Some(shadow) = &self.shadow_index {
            let h = fxhash(value);
            let mut i = shadow.partition_point(|&(sh, _)| sh < h);
            while let Some(&(sh, vid)) = shadow.get(i) {
                if sh != h { break; }
                if self.get_checked(vid) == Some(value) { return Some(vid); }
                i += 1;
            }
            return None;
        }
        let h = fxhash(value);
        loop {
            let g = self.table();
            if g.busy() {
                // 全域の表へ組み直している (#374): 終わるまで待つ
                std::thread::yield_now();
                continue;
            }
            hook_after_table_read();
            let found = self.lookup_in(g, h, value);
            // 読む間に表が組み直された (重なる領域を書き換えた) なら読み直す
            if self.table() == g {
                return found;
            }
        }
    }

    /// 表 `g` を引く。
    fn lookup_in(&self, g: Gen, h: u64, value: &[u8]) -> Option<u32> {
        let mask = (g.cap() - 1) as u64;
        let xm: &[u8] = self.index.slice();
        let mut idx = home_slot(h, g.cap()); // #123
        // #59: 表が 100% 埋まると 「空 slot に当たる」 終了条件が成立せず、 素の `loop` は **永久に回る**。
        // 走査上限を表の slot 数にする — 全 slot を見て見つからなければ不在。
        for _ in 0..g.cap() as usize {
            let off = g.slot_off(idx);
            if xm[off] == 0 { return None; }
            let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
            if slot_hash == h {
                let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                // #92: 実 insert は必ず vid < max_entries を assert する。 残りうる **vid >= max_entries の
                // 破損 slot** は get(vid) が offsets region を溢れて OOB するので読み飛ばす。
                if self.get_checked(vid) == Some(value) { return Some(vid); }
            }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
        None
    }

    /// 重複検査なしで常に新規 id を発行して append する。
    /// `ValueType::Leaf` (終端タグ・dedupe なし) の書き込み path で使う。
    /// 既に同じ bytes が登録済みでも気にせず新 id を払い出す。index_insert は走るが、
    /// 既存スロットがあれば early-return するため index 領域は dedup される副作用がある
    /// (data/offsets のみ完全に増分)。
    /// 今後 1 件も insert できないか (= 天井 hit)。
    pub fn is_full(&self) -> bool {
        self.count.load(Ordering::Relaxed) >= self.max_entries.load(Ordering::Relaxed)
            && self.reclaim_stats().0 == 0
    }

    /// 満杯なら **`u32::MAX` (予約 sentinel)** を返す (#59: panic しない)。
    /// 理由 (一杯 / 空き不足) が要る時は `try_insert`。
    pub fn insert(&self, value: &[u8]) -> u32 {
        self.try_insert(value).unwrap_or(u32::MAX)
    }

    /// `insert` の理由付き版 (#316)。 `Full` は `vocab_max_entries` / 索引の天井 (この先も入らない)、
    /// `Space` はディスクの空き不足で伸ばせない (空けば入る)。
    pub fn try_insert(&self, value: &[u8]) -> Result<u32, VocabFail> {
        self.try_insert_with(value, false)
    }

    /// `try_insert` の本体。 `pinned` なら返す番号の参照を 1 つ持った状態で返す (#381)。
    fn try_insert_with(&self, value: &[u8], pinned: bool) -> Result<u32, VocabFail> {
        // #381: 1 語の長さは 30 bit まで (offsets の長さの上位 2 bit は世代)
        if value.len() > LEN_MASK as usize {
            return Err(VocabFail::Full);
        }
        // #381: 回収した場所があれば、 新しい番号より先に使い回す
        if let Some(slot) = self.claim() {
            return self.insert_into_claimed(slot, value, pinned);
        }
        // 索引の home slot のページを先に確保する。 空き不足で断られるのは大抵ここなので、 採番・data の書き込みの
        // 前に止めて orphan を作らない (表はこの後で伸びうるので、 確かめるのは今の表)
        let g = self.table();
        let home = g.slot_off(home_slot(fxhash(value), g.cap()));
        if self.index.ensure_committed_sparse(home + INDEX_SLOT_SIZE, INDEX_SLOT_SIZE).is_err() {
            return Err(VocabFail::Space);
        }
        let len = value.len() as u32;
        // #328: data / offsets も番号を取る前に伸ばせるか見る (取った後に伸ばせないと番号を捨てることになる)。
        // 並行 insert で位置は先へずれうるので、 ここは捨てる番号を減らすための先取りで、 保証は下の確認
        let (count_now, end_now) = (self.count.load(Ordering::Relaxed), self.data_end.load(Ordering::Relaxed));
        let max_entries = self.max_entries.load(Ordering::Relaxed);
        // #381: data の上限 (`vocab_data_size`) に着いた。 番号を取る前に止める (先取り、 保証は下の確認)
        if count_now < max_entries && end_now as u64 + len as u64 > self.data_limit.load(Ordering::Relaxed) as u64 {
            return Err(VocabFail::Full);
        }
        if count_now < max_entries
            && (self.data.ensure_committed((end_now + len) as usize).is_err()
                || self.offsets.ensure_committed(((count_now as usize) + 1) * 8).is_err())
        {
            return Err(VocabFail::Space);
        }
        hook_after_precheck();
        let id = self.count.fetch_add(1, Ordering::Relaxed);
        // #122: vocab_max_entries が公開 knob になったので、 天井 hit を actionable に
        // する (#118 の `too many himos` と同じ扱い)。 既存 DB は header 焼き込みなので
        // 引き上げには rebuild が必要、 という点まで伝える。
        // #59: 天井 hit は 「想定内だが続行不能」。 embedded DB は他人の process に
        // 埋め込まれるので panic で host を殺してはいけない。 採番を巻き戻して
        // **予約 sentinel `u32::MAX`** を返し、 呼び出し側 (Engine) が fault として
        // 記録 + 報告し、 write を拒否する。 `u32::MAX` は元々 「無効値」 として
        // engine 側の guard が見ている値なので、 新しい規約を増やしていない。
        if id >= max_entries {
            // 天井より先の番号は誰も使わないので戻してよい (戻す間に取られる番号も天井より先)
            self.count.fetch_sub(1, Ordering::Relaxed);
            return Err(VocabFail::Full);
        }
        // #381: data は上限の内側でだけ取る (並行 insert が上の先取りを同時に抜けても越えない)。 取れなければ番号は
        // 捨てる (#328 と同じく巻き戻さない、 offsets が 0 = 空の値)
        let limit = self.data_limit.load(Ordering::Relaxed) as u64;
        let Ok(offset) = self.data_end.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |end| {
            (end as u64 + len as u64 <= limit).then_some(end + len)
        }) else {
            return Err(VocabFail::Full);
        };
        // Growable backing: extend the file-backed window before
        // writing past the current commit. No-op for static backings.
        // We also grow the offsets region in case `id` advanced
        // past its committed footprint (offsets have id × 8 layout).
        // #167: commit を伸ばせなければ **書かずに諦める** (予約 sentinel を返す)。
        // 未 commit page への書き込みは (ディスク満杯なら) SIGBUS でプロセスごと
        // 落ちるので、 error を捨てて書き進めてはいけない。
        // #328: 番号は巻き戻さずに捨てる。 巻き戻すと、 その間に次の番号を取った insert が居た時に次の
        // fetch_add がその番号をもう一度配り、 offsets を上書きする。 捨てた番号は offsets が 0 = 空の値として読める
        if Self::fail_after_take()
            || self.data.ensure_committed((offset + len) as usize).is_err()
            || self.offsets.ensure_committed(((id as usize) + 1) * 8).is_err()
            || self.reclaim.get().is_some_and(|r| r.refs.ensure(id).is_err())
        {
            return Err(VocabFail::Space);
        }
        self.data.write_at(offset as usize, value);
        self.data.write_at(0, &MAGIC);
        let new_count = id + 1;
        let new_end = offset + len;
        // #328: 並行 insert は逆順に終わりうるので、 header の件数 / data の終わりは戻さない (max で書く)。
        // 素の上書きだと後から終わった小さい番号が件数を戻し、 開き直した後に使用済みの番号を配り直す
        self.data.as_atomic_u32(4).fetch_max(new_count, Ordering::AcqRel);
        self.data.as_atomic_u32(8).fetch_max(new_end, Ordering::AcqRel);
        self.data.mark_dirty(0, 12);
        // #77-M1: flush() が clean を書いた後の最初の insert で 0 に戻す。
        // これが無いと flush 後の write 中 crash で、 次 open が部分 writeback
        // された index を rebuild なしで信用してしまう。
        if self.clean_on_disk.swap(false, Ordering::AcqRel) {
            self.data.write_at(CLEAN_FLAG_OFF, &0u32.to_le_bytes());
            self.data.mark_dirty(CLEAN_FLAG_OFF, 4);
        }
        self.data.mark_dirty(offset as usize, len as usize);
        let off_pos = (id as usize) * 8;
        self.offsets.as_atomic_u64(off_pos).store(slot_entry(offset, len, 0), Ordering::Release);
        self.offsets.mark_dirty(off_pos, 8);
        // #381: 索引に載せる (= 他の書き手に見える) 前に押さえる
        if pinned && let Some(c) = self.reclaim.get().and_then(|r| r.refs.at(id)) {
            c.store(1, Ordering::Release);
        }
        // #59: index が満杯で登録できないなら 「vocab 満杯」 と同じ扱いにする
        // (dedup が黙って壊れるより、 write を拒否させる方が安全)。 data/offsets に
        // 書いた分は orphan になる (満杯なら terminal、 空き不足は probe が home のページを
        // 越えた時だけ)。
        if let Err(e) = self.index_insert_healing(value, id) {
            if pinned {
                self.release(id);
            }
            return Err(e);
        }
        // #374: 語数が表の半分を超えたら伸ばす
        self.maybe_grow();
        Ok(id)
    }

    /// 回収した場所 `slot` (参照 CLAIMED) に `value` を入れ、 世代を 1 つ進めた番号を返す (#381)。 data は今までと
    /// 同じく末尾に足す (前の語の byte は残る — 借用で読んでいる読み手の中身は変わらない)。
    fn insert_into_claimed(&self, slot: u32, value: &[u8], pinned: bool) -> Result<u32, VocabFail> {
        let len = value.len() as u32;
        let limit = self.data_limit.load(Ordering::Relaxed) as u64;
        let Ok(offset) = self.data_end.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |end| {
            (end as u64 + len as u64 <= limit).then_some(end + len)
        }) else {
            self.unclaim(slot);
            return Err(VocabFail::Full);
        };
        if self.data.ensure_committed((offset + len) as usize).is_err() {
            self.unclaim(slot);
            return Err(VocabFail::Space);
        }
        self.data.write_at(offset as usize, value);
        self.data.as_atomic_u32(8).fetch_max(offset + len, Ordering::AcqRel);
        self.data.mark_dirty(0, 12);
        if self.clean_on_disk.swap(false, Ordering::AcqRel) {
            self.data.write_at(CLEAN_FLAG_OFF, &0u32.to_le_bytes());
            self.data.mark_dirty(CLEAN_FLAG_OFF, 4);
        }
        self.data.mark_dirty(offset as usize, len as usize);
        let generation = (self.slot_gen(slot) + 1) & 3;
        let off_pos = slot as usize * 8;
        // 位置・長さ・世代を 1 回で書く (読み手は 1 回で読む)。 data を書いた後 (Release)
        self.offsets.as_atomic_u64(off_pos).store(slot_entry(offset, len, generation), Ordering::Release);
        self.offsets.mark_dirty(off_pos, 8);
        let r = self.reclaim.get().expect("claimed without reclaim");
        // 世代を進めた後で参照を戻す (`acquire` は取った後に世代を見る)
        if let Some(c) = r.refs.at(slot) {
            c.store(pinned as u32, Ordering::Release);
        }
        r.claims.fetch_add(1, Ordering::Relaxed);
        // 前の語の entry は死んだまま表に残る
        self.stale_entries.fetch_add(1, Ordering::Relaxed);
        let vid = make_vid(generation, slot);
        if let Err(e) = self.index_insert_healing(value, vid) {
            if pinned {
                self.release(vid);
            }
            return Err(e);
        }
        self.maybe_grow();
        Ok(vid)
    }

    /// `index_insert`、 表が死んだ entry で埋まって入らなければ作り直してもう 1 回 (#381)。
    fn index_insert_healing(&self, value: &[u8], vid: u32) -> Result<(), VocabFail> {
        match self.index_insert(value, vid) {
            Err(_) if self.stale_entries.load(Ordering::Relaxed) > 0 => {
                self.rebuild_table(true);
                self.index_insert(value, vid)
            }
            r => r,
        }
    }

    /// テストで 「番号を取った直後に伸ばせなかった」 を起こす (#328)。 thread_local なので他のテストに効かない。
    #[cfg(test)]
    fn fail_after_take() -> bool {
        FAIL_AFTER_TAKE.with(|f| f.get())
    }
    #[cfg(not(test))]
    #[inline(always)]
    fn fail_after_take() -> bool {
        false
    }

    /// index に (hash, id) を登録する。 **表が満杯なら `Err(Full)`、 伸ばせなければ `Err(Space)`** (#59 / #316)。
    ///
    /// 旧実装は空 slot が見つかるまで無条件に linear probe しており、 index が 100%
    /// 埋まると永久に回った (= 満杯が hang)。 走査は表の slot 数で打ち切る。
    /// 登録できなくても data/offsets 側の値は書けているので、 dedup が効かなくなる
    /// だけで read は壊れない (呼び出し側が fault として報告する)。
    ///
    /// #374: 表を伸ばしている間は待つ (`grow_lock` の read)。 全域の表でない表が満杯 (= 空き不足で伸ばせなかった)
    /// なら `Space` (空けば伸びて入る)。
    fn index_insert(&self, value: &[u8], id: u32) -> Result<(), VocabFail> {
        let _r = self.grow_lock.read().unwrap_or_else(|p| p.into_inner());
        let g = self.table();
        let mask = (g.cap() - 1) as u64;
        let h = fxhash(value);
        let mut idx = home_slot(h, g.cap()); // #123
        let mut probes = 0usize;
        loop {
            if probes >= g.cap() as usize {
                return Err(if g.cap() >= self.index_cap() { VocabFail::Full } else { VocabFail::Space });
            }
            probes += 1;
            let off = g.slot_off(idx);
            // v10: index segment は書いた分だけ commit される (旧 fixed cluster の eager
            // commit ではない)。 slot の atomic CAS は write なので、 触る前に伸ばす。
            // 伸ばせない (#167) なら挿入失敗として返す (呼び側が満杯扱いする)。
            if self.index.ensure_committed_sparse(off + INDEX_SLOT_SIZE, INDEX_SLOT_SIZE).is_err() {
                return Err(VocabFail::Space);
            }
            // #83: slot flag は Region 経由の AtomicU8 で直接触る (`&mut [u8]` を
            // 実体化しない)。 hash/id の書込も write_at (raw ptr)。
            let flag = self.index.as_atomic_u8(off);
            let f = flag.load(Ordering::Acquire);
            if f == 0 {
                match flag.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Relaxed) {
                    Ok(_) => {
                        self.index.write_at(off + 1, &h.to_le_bytes());
                        self.index.write_at(off + 9, &id.to_le_bytes());
                        flag.store(1, Ordering::Release);
                        self.index.mark_dirty(off, INDEX_SLOT_SIZE);
                        return Ok(());
                    }
                    Err(_) => continue,
                }
            }
            if f == 2 {
                while self.index.as_atomic_u8(off).load(Ordering::Acquire) == 2 {
                    std::hint::spin_loop();
                }
                continue;
            }
            // f == 1 (committed): hash/id は flag=1 の Release publish より前に書かれ、
            // 上の flag Acquire load と対で可視。 slice() で読む。
            let (slot_hash, vid) = {
                let xm = self.index.slice();
                (
                    u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap()),
                    u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap()),
                )
            };
            if slot_hash == h {
                // ハッシュ一致 → 実際の値を比較して本当に重複か確認。
                // #92: vid >= offsets_cap の破損 slot は get(vid) が OOB するので
                // 読み飛ばす (実 insert は vid < max_entries <= offsets_cap を保証 = 通常運用では常に
                // 通過。 offsets_cap は不変で並行 insert を skip しない = dedup race 無)。
                if self.get_checked(vid) == Some(value) {
                    return Ok(()); // 本当の重複
                }
                // ハッシュ衝突 or 破損 slot → linear probe 続行
            }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
    }

    pub fn count(&self) -> u32 { self.count.load(Ordering::Relaxed) }

    /// 語数の上限 (#381)。 回収されない語 (行を消した値) も `count` に入っている。
    pub fn max_entries(&self) -> u32 { self.max_entries.load(Ordering::Relaxed) }

    /// data の byte 数の上限 (#381)。 使った分は `data_footprint`。
    pub fn data_limit(&self) -> u32 { self.data_limit.load(Ordering::Relaxed) }

    /// 領域 (予約) に入る語数と data の byte 数。 上限はこの範囲でしか伸ばせない (#381)。
    pub fn region_limits(&self) -> (u32, u32) {
        let (offsets_cap, data_cap) = Self::region_caps(&self.offsets, &self.data);
        let index_slots = (self.index.len().saturating_sub(INDEX_HEADER) / INDEX_SLOT_SIZE) as u64;
        // 索引は上限の次の 2 の冪の slot を要る
        let by_index = if index_slots == 0 { 0 } else { 1u64 << (63 - index_slots.leading_zeros()) };
        (offsets_cap.min(by_index.min(u32::MAX as u64) as u32).min(MAX_SLOTS), data_cap)
    }

    fn region_caps(offsets: &Region, data: &Region) -> (u32, u32) {
        ((offsets.len() / 8).min(u32::MAX as usize) as u32, data.len().min(u32::MAX as usize) as u32)
    }

    #[inline]
    fn index_cap(&self) -> u32 {
        self.index_cap.load(Ordering::Acquire)
    }

    /// 上限を入れる (#381)。 開いた時は header の値を、 `Engine::grow_vocab` は伸ばした値を渡す。 語数・data は今
    /// 使っている分より下げない。 索引の領域 (`vocab_max_entries` の次の 2 の冪) が今より広がる時は index header の
    /// 領域の大きさも書く (書き手のみ。 readonly の領域は書けないので、 readonly で伸ばす値を渡さないこと)。
    ///
    /// 領域 (予約) に入らない値は `Err` (何も変えない)。
    pub fn set_limits(&self, max_entries: u32, data_size: usize) -> Result<(), String> {
        let (entries_room, data_room) = self.region_limits();
        if max_entries > entries_room {
            return Err(format!(
                "vocab_max_entries {max_entries} does not fit the reservation ({entries_room} entries)"
            ));
        }
        if data_size > data_room as usize {
            return Err(format!("vocab_data_size {data_size} does not fit the reservation ({data_room} bytes)"));
        }
        let index_cap = max_entries.max(1).next_power_of_two();
        let widened = {
            // 表を伸ばす途中 (`maybe_grow`) と重ねない
            let _w = self.grow_lock.write().unwrap_or_else(|p| p.into_inner());
            // 呼び手 (open / `Engine::grow_vocab`) は直列なので store でよい (開いた時の仮の値は下げる)
            self.max_entries.store(max_entries.max(self.count()), Ordering::Release);
            self.data_limit.store((data_size as u32).max(self.data_footprint()), Ordering::Release);
            let widened = index_cap > self.index_cap() && !self.readonly;
            if widened {
                self.index.write_at(4, &index_cap.to_le_bytes());
                self.index.mark_dirty(4, 4);
                self.index_cap.store(index_cap, Ordering::Release);
            }
            widened
        };
        // 表が領域いっぱい (= 上限まで語が入った) なら、 広がった領域へ今のうちに伸ばす。 伸ばすのは insert の後
        // だけなので、 ここで伸ばさないと次の insert が満杯の表に当たる
        if widened {
            self.maybe_grow();
        }
        Ok(())
    }

    /// 今の表の slot 数 (観測用、 #374)。 語数の 2〜4 倍 (最小 `FIRST_GEN_CAP`、 最大は領域の全域)。
    pub fn index_table_slots(&self) -> u32 {
        if let Some(s) = &self.shadow_index {
            return s.len() as u32;
        }
        self.table().cap()
    }

    /// data 領域の append pointer (= 消費済み byte 数)。 単調増加・回収なし。
    /// #88 bench: Leaf を vocab に載せた場合の「回収されない footprint」計測用。
    pub fn data_footprint(&self) -> u32 { self.data_end.load(Ordering::Relaxed) }

    /// 内部状態をRegionヘッダに書き戻す（flushの前に呼ぶ）。
    pub fn sync(&self) {
        self.data.write_at(4, &self.count.load(Ordering::Relaxed).to_le_bytes());
        self.data.write_at(8, &self.data_end.load(Ordering::Relaxed).to_le_bytes());
    }

    /// #101: 観測用 — clean flag の現在値。 open 直後に true なら「前回 graceful close
    /// 済みで rebuild を skip した」。 insert が走ると false に戻る (#77-M1)。
    pub fn index_clean_on_disk(&self) -> bool {
        self.clean_on_disk.load(Ordering::Acquire)
    }

    /// index と data の整合性マーカーを書く。
    ///
    /// `clean = true`: 直前に全 msync が完了 → 次回 open で rebuild skip 可 (`CLEAN_GEN` を書く、 #374)
    /// `clean = false`: insert が走った／crash 検知用 → 次回 open で rebuild 強制
    ///
    /// 自身では msync しない。 caller (Engine) が body_msync で永続化する責任を負う。
    /// `mark_index_clean(true)` の、 参照数の file も閉じた時の cell と合っている版 (#385、 `seal_refs` + msync の後)。
    /// 参照数を file に置いていなければ `mark_index_clean(true)` と同じ。
    pub fn mark_index_clean_refs(&self) {
        let mapped = matches!(self.reclaim.get().map(|r| &r.refs), Some(RefStore::Mapped(_)));
        if !mapped || self.data.ensure_committed(HEADER).is_err() {
            return self.mark_index_clean(true);
        }
        self.data.write_at(CLEAN_FLAG_OFF, &CLEAN_REFS.to_le_bytes());
        self.clean_on_disk.store(true, Ordering::Release);
    }

    pub fn mark_index_clean(&self, clean: bool) {
        // data 領域は variable cluster (lazy commit) なので、 先頭 header を
        // 確実に commit してから書く。
        // #167: commit を伸ばせなければ flag を書かない (未 commit page への write は
        // ディスク満杯なら SIGBUS)。 flag を落とせないと次回 open が rebuild する =
        // 遅くなるだけで壊れない、 安全側。
        if self.data.ensure_committed(HEADER).is_err() {
            return;
        }
        let val: u32 = if clean { CLEAN_GEN } else { 0 };
        self.data.write_at(CLEAN_FLAG_OFF, &val.to_le_bytes());
        self.clean_on_disk.store(clean, Ordering::Release); // #77-M1 キャッシュ追従
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// VIX2 (0.14 以前) の slot 選択 (hash 下位ビット)。 旧 index を再現する test のためだけに残す。
    fn home_slot_legacy(h: u64, index_cap: u32) -> usize {
        (h & (index_cap - 1) as u64) as usize
    }

    struct Regions {
        data_ptr: *mut u8,
        offsets_ptr: *mut u8,
        index_ptr: *mut u8,
        data_len: usize,
        offsets_len: usize,
        index_len: usize,
    }

    fn make_regions(max_entries: u32, index_cap: u32, data_size: usize) -> Regions {
        // offsets は 8 byte の atomic で読み書きする (#381) ので u64 で確保して整列させる
        let leak = |size: usize| -> *mut u8 {
            Box::leak(vec![0u64; size.div_ceil(8)].into_boxed_slice()).as_mut_ptr() as *mut u8
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
            .unwrap()
        }
        fn index_bytes(&self) -> Vec<u8> {
            unsafe { std::slice::from_raw_parts(self.index_ptr, self.index_len) }.to_vec()
        }
        /// index region 内の「使用中」 slot 数 (flag != 0)。 migration で旧 slot が
        /// 残っていないか (= 占有率が二重になっていないか) を見るのに使う。
        fn occupied_slots(&self, index_cap: u32) -> usize {
            let xm = unsafe { std::slice::from_raw_parts(self.index_ptr, self.index_len) };
            (0..index_cap as usize)
                .filter(|i| xm[INDEX_HEADER + i * INDEX_SLOT_SIZE] != 0)
                .count()
        }
        /// 0.14 (VIX2) が書いた index を再現する — 全ゼロ化して旧 magic + **旧 slot 選択**
        /// (hash 下位ビット) で live entry を植える。
        fn plant_legacy_index(&self, index_cap: u32, entries: &[(&[u8], u32)]) {
            let xm = unsafe { std::slice::from_raw_parts_mut(self.index_ptr, self.index_len) };
            xm.fill(0);
            xm[0..4].copy_from_slice(&INDEX_MAGIC_V2);
            xm[4..8].copy_from_slice(&index_cap.to_le_bytes());
            for (value, vid) in entries {
                let h = fxhash(value);
                let mut idx = home_slot_legacy(h, index_cap);
                loop {
                    let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
                    if xm[off] == 0 {
                        xm[off] = 1;
                        xm[off + 1..off + 9].copy_from_slice(&h.to_le_bytes());
                        xm[off + 9..off + 13].copy_from_slice(&vid.to_le_bytes());
                        break;
                    }
                    idx = (idx + 1) & (index_cap as usize - 1);
                }
            }
        }
    }

    /// #123: `fxhash` は乗算で終わるので **下位ビットに上位の entropy が届かない**。
    /// issue の実キー (`第N条` は UTF-8 で 8 byte = 1 word ちょうどなので
    /// `h = word * SEED` そのもの) で、 旧 slot 選択が全滅し新 slot 選択が散ることを固定する。
    #[test]
    fn issue123_low_bits_collide_high_bits_spread() {
        let keys: Vec<&[u8]> = vec![
            "第10条".as_bytes(),
            "第11条".as_bytes(),
            "第12条".as_bytes(),
            "第13条".as_bytes(),
        ];
        assert!(keys.iter().all(|k| k.len() == 8), "1 word ちょうどの前提");
        let cap = 1u32 << 12;

        let legacy: std::collections::HashSet<usize> =
            keys.iter().map(|k| home_slot_legacy(fxhash(k), cap)).collect();
        let modern: std::collections::HashSet<usize> =
            keys.iter().map(|k| home_slot(fxhash(k), cap)).collect();

        assert_eq!(
            legacy.len(), 1,
            "旧 slot 選択 (下位ビット) は同一 slot に潰れる — issue #123 の実例が再現しない: {legacy:?}"
        );
        assert_eq!(
            modern.len(), keys.len(),
            "新 slot 選択 (上位ビット) が散っていない: {modern:?}"
        );
    }

    /// #122 / #59: 天井 hit は **panic ではなく予約 sentinel `u32::MAX`** を返すこと。
    ///
    /// #122 は 「天井 hit のメッセージがどの knob を上げればよいか示すこと」 を
    /// `#[should_panic]` で担保していた。 が #59 で方針が変わった — embedded DB は
    /// 他人の process に埋め込まれるので、 「vocab が一杯」 という想定内事象で host を
    /// 殺してはいけない。 actionable な案内は panic message ではなく
    /// `Engine::record_fault` の warning + `Engine::fault_count(FaultKind::VocabSpace)`
    /// で行う (`engine.rs` / `tests/issue59_no_host_kill.rs`)。
    #[test]
    fn issue122_vocab_full_returns_sentinel_instead_of_panicking() {
        let r = make_regions(4, 8, 64 * 1024);
        let v = r.vocab_init(4, 8);
        let ids: Vec<u32> = (0..8).map(|i| v.insert(format!("v{i}").as_bytes())).collect();
        assert!(
            ids.iter().any(|&id| id != u32::MAX),
            "天井前の insert まで失敗している: {ids:?}"
        );
        assert!(
            ids.contains(&u32::MAX),
            "天井を越えたのに sentinel が返っていない (panic していた旧挙動?): {ids:?}"
        );
        assert!(v.is_full(), "is_full が天井を報告していない");
    }

    /// #59: index が 100% 埋まった状態の `lookup` が **有限時間で** 返ること。
    ///
    /// 旧実装の probe は 「空 slot に当たる」 だけが終了条件で、 満杯かつ不在の
    /// 値を引くと永久に回った (= 満杯が hang。 embedded DB としては panic と同程度に
    /// 悪い)。 index_cap 回で打ち切る。
    #[test]
    fn vocab_lookup_on_full_index_terminates() {
        let r = make_regions(8, 8, 64 * 1024);
        let v = r.vocab_init(8, 8);
        // index_cap = 8 を埋める
        for i in 0..8 {
            v.insert(format!("v{i}").as_bytes());
        }
        // 不在の値: 空 slot が無いので旧実装はここで無限ループした
        assert_eq!(v.lookup(b"absent"), None);
    }

    /// #123 (F6): migration の途中で kill された状態 = **tombstone (flag = 3) が残った
    /// index** を次の open が回収できること。 旧実装は `xm[off] == 1` しか同一 entry と
    /// 見なさず、 rebuild は no-clear (#92) なので flag 3 が恒久ゴミとして残り、 占有率と
    /// probe 長が永久に劣化していた。 crash は「open の単一スレッド区間で必ず消える」
    /// という前提を破る。
    #[test]
    fn issue123_crash_left_tombstones_are_reclaimed() {
        let cap = 1u32 << 8;
        let r = make_regions(1024, cap, 64 * 1024);
        let keys: Vec<String> = (0..24).map(|i| format!("第{}条", 10 + i)).collect();

        let ids: Vec<u32> = {
            let w = r.vocab_init(1024, cap);
            let ids = keys.iter().map(|k| w.get_or_insert(k.as_bytes())).collect();
            w.sync();
            ids
        };
        let entries: Vec<(&[u8], u32)> =
            keys.iter().zip(&ids).map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_legacy_index(cap, &entries);

        // migration ① (tombstone を立てた) 直後に kill された状態を再現する:
        // 先頭 8 entry の flag を 3 にし、 payload (hash/vid) は残す。
        {
            let xm = unsafe {
                std::slice::from_raw_parts_mut(r.index_ptr, r.index_len)
            };
            let mut done = 0;
            for i in 0..cap as usize {
                let off = INDEX_HEADER + i * INDEX_SLOT_SIZE;
                if xm[off] == 1 {
                    xm[off] = 3;
                    done += 1;
                    if done == 8 { break; }
                }
            }
            assert_eq!(done, 8, "tombstone を立てられていない");
        }

        let v = r.vocab_load(/*readonly=*/ false);

        for (k, id) in keys.iter().zip(&ids) {
            assert_eq!(v.lookup(k.as_bytes()), Some(*id), "crash 復帰後に {k} が引けない");
        }
        assert_eq!(
            r.occupied_slots(cap),
            keys.len(),
            "crash で残った tombstone が回収されず恒久ゴミになっている (F6)"
        );
        // flag 3 が 1 つも残っていないこと (lookup/insert は 0/非 0 でしか見ないので
        // 正しさは保たれるが、 占有と probe 長が劣化し続ける)
        let xm = r.index_bytes();
        let leftovers = (0..cap as usize)
            .filter(|i| xm[INDEX_HEADER + i * INDEX_SLOT_SIZE] == 3)
            .count();
        assert_eq!(leftovers, 0, "flag=3 の tombstone が {leftovers} 個残っている");
    }

    /// #123: migrate 後に graceful close (clean flag) すると、 次の open は rebuild を
    /// **skip** する (= VIX3 magic が永続化されている)。 magic を書き忘れると毎 open
    /// rebuild し続けるので、 その回帰を止める。
    #[test]
    fn issue123_migrated_index_is_clean_on_next_open() {
        let cap = 1u32 << 8;
        let r = make_regions(1024, cap, 64 * 1024);
        let keys: Vec<String> = (0..16).map(|i| format!("第{}条", 10 + i)).collect();

        let ids: Vec<u32> = {
            let w = r.vocab_init(1024, cap);
            let ids = keys.iter().map(|k| w.get_or_insert(k.as_bytes())).collect();
            w.sync();
            ids
        };
        let entries: Vec<(&[u8], u32)> =
            keys.iter().zip(&ids).map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_legacy_index(cap, &entries);

        // 1 回目: VIX2 → migrate される
        {
            let v = r.vocab_load(false);
            assert!(v.rebuilt_on_load, "1 回目は migrate されるべき");
            v.sync();
            v.mark_index_clean(true); // graceful close 相当
        }
        // 2 回目: VIX3 + clean なので rebuild しない
        let v2 = r.vocab_load(false);
        assert!(
            !v2.rebuilt_on_load,
            "migrate 後も rebuild され続けている (VIX4 magic が永続化されていない)"
        );
        for (k, id) in keys.iter().zip(&ids) {
            assert_eq!(v2.lookup(k.as_bytes()), Some(*id), "{k} が引けない");
        }
    }

    /// #123: readonly open で VIX2 を掴んだ場合、 共有 index を **1 byte も書かず**に
    /// shadow (新 slot 関数) で正しく引けること。 別 process の writer と併走する
    /// 別 process の readonly reader 型の構成が該当する。
    #[test]
    fn issue123_readonly_open_of_legacy_index_uses_shadow() {
        let cap = 1u32 << 8;
        let r = make_regions(1024, cap, 64 * 1024);
        let keys: Vec<String> = (0..16).map(|i| format!("第{}条", 10 + i)).collect();

        let ids: Vec<u32> = {
            let w = r.vocab_init(1024, cap);
            let ids = keys.iter().map(|k| w.get_or_insert(k.as_bytes())).collect();
            w.sync();
            w.mark_index_clean(true);
            ids
        };
        let entries: Vec<(&[u8], u32)> =
            keys.iter().zip(&ids).map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_legacy_index(cap, &entries);

        let before = r.index_bytes();
        let ro = r.vocab_load(/*readonly=*/ true);
        for (k, id) in keys.iter().zip(&ids) {
            assert_eq!(ro.lookup(k.as_bytes()), Some(*id), "shadow で {k} が引けない");
        }
        assert_eq!(r.index_bytes(), before, "readonly open が共有 index を書き換えた");
    }

    /// #123: cap == 1 の退化ケースで `home_slot` が shift overflow しない。
    #[test]
    fn issue123_home_slot_handles_degenerate_cap() {
        assert_eq!(home_slot(u64::MAX, 1), 0);
        assert_eq!(home_slot(0, 1), 0);
        assert!(home_slot(u64::MAX, 2) < 2);
    }

    /// #123: VIX2 index を持つ DB を開くと、 clean_flag が立っていても index を作り直し、
    /// **旧 slot を残さない** (占有 slot 数 == live 値数)。 旧 slot を消さずに新 slot 関数で
    /// 再挿入すると (rebuild は #92 で no-clear) 占有が二重になる。
    #[test]
    fn issue123_vix2_index_migrates_without_stale_slots() {
        let cap = 1u32 << 8;
        let r = make_regions(1024, cap, 64 * 1024);
        let keys: Vec<String> = (0..40).map(|i| format!("第{}条", 10 + i)).collect();

        let ids: Vec<u32> = {
            let w = r.vocab_init(1024, cap);
            let ids = keys.iter().map(|k| w.get_or_insert(k.as_bytes())).collect();
            w.sync();
            w.mark_index_clean(true); // clean = 「rebuild 不要」 マーカーを立てておく
            ids
        };

        // 0.14 が書いた状態を再現 (VIX2 magic + 旧 slot)。 clean_flag は 1 のまま。
        let entries: Vec<(&[u8], u32)> =
            keys.iter().zip(&ids).map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_legacy_index(cap, &entries);
        assert_eq!(r.occupied_slots(cap), keys.len(), "植え付けが失敗している");

        let v = r.vocab_load(/*readonly=*/ false);

        assert!(v.rebuilt_on_load, "VIX2 は clean_flag が立っていても rebuild されるべき");
        for (k, id) in keys.iter().zip(&ids) {
            assert_eq!(v.lookup(k.as_bytes()), Some(*id), "migration 後に {k} が引けない");
        }
        assert_eq!(v.lookup("第999条".as_bytes()), None, "無い値が引けてしまう");
        assert_eq!(
            &r.index_bytes()[0..4], &INDEX_MAGIC,
            "migration 後の index magic が VIX4 になっていない (次 open で毎回 rebuild する)"
        );
        assert_eq!(
            r.occupied_slots(cap), keys.len(),
            "旧 slot が残って占有が二重になっている (#123 migration の取り残し)"
        );
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
        assert_eq!(flag(&r), CLEAN_GEN, "flush 直後は clean (#374 で 2)");
        w.get_or_insert(b"second");
        assert_eq!(flag(&r), 0, "flush 後の insert で clean=0 に戻るはず (#77-M1)");
    }

    /// 回収を有効にして既存の語を数え終えた辞書 (#381)。
    fn reclaiming(max: u32) -> (Regions, Vocabulary) {
        let r = make_regions(max, max, 1 << 20);
        let v = r.vocab_init(max, max);
        v.enable_reclaim(None);
        (r, v)
    }

    /// #381: 参照が 0 になった語の場所を使い回し、 世代を進める。 古い番号は該当なしになる (別の語に当たらない)。
    #[test]
    fn released_slot_is_reused_with_the_next_generation() {
        let (_r, v) = reclaiming(64);
        let a = v.try_get_or_insert_pinned(b"alpha").unwrap();
        v.finish_refs();
        assert_eq!(v.reclaim_stats().0, 0, "押さえている語は待ち行列に入らない");
        v.release(a);
        assert_eq!(v.reclaim_stats().0, 1);
        let b = v.try_get_or_insert_pinned(b"beta").unwrap();
        assert_eq!(vid_slot(b), vid_slot(a), "空いた場所を使い回す");
        assert_eq!(vid_gen(b), vid_gen(a) + 1, "世代を進める");
        assert_eq!(v.count(), 1, "新しい番号を取らない");
        assert_eq!(v.get_checked(a), None, "古い番号は該当なし");
        assert_eq!(v.get(a), b"", "古い番号は空");
        assert_eq!(v.get_checked(b), Some(&b"beta"[..]));
        assert_eq!(v.lookup(b"alpha"), None, "回収した語は引けない");
        assert_eq!(v.lookup(b"beta"), Some(b));
        assert!(!v.acquire(a), "古い番号は取れない");
        assert_eq!(v.ref_count(b), Some(1));
        // 回収した語を入れ直すと、 新しい番号になる
        let a2 = v.try_get_or_insert_pinned(b"alpha").unwrap();
        assert_ne!(a2, a);
        assert_eq!(v.lookup(b"alpha"), Some(a2));
    }

    /// #381: いちばん昔に空いた場所から使い回す (同じ場所がすぐ戻ってこない = 世代 2 bit で足りる根拠)。
    #[test]
    fn reuse_is_first_in_first_out() {
        let (_r, v) = reclaiming(64);
        let ids: Vec<u32> = (0..4).map(|i| v.try_get_or_insert_pinned(format!("w{i}").as_bytes()).unwrap()).collect();
        v.finish_refs();
        for &i in [2, 0, 3, 1].iter() {
            v.release(ids[i]);
        }
        let got: Vec<u32> =
            (0..4).map(|i| vid_slot(v.try_get_or_insert_pinned(format!("n{i}").as_bytes()).unwrap())).collect();
        assert_eq!(got, vec![vid_slot(ids[2]), vid_slot(ids[0]), vid_slot(ids[3]), vid_slot(ids[1])]);
    }

    /// #381: 既存の cell を数え終えるまでは回収しない / 待ち行列に入った後に使い直された語は回収しない。
    #[test]
    fn no_reuse_before_counting_or_after_revival() {
        let (_r, v) = reclaiming(64);
        let a = v.try_get_or_insert_pinned(b"alpha").unwrap();
        v.release(a);
        let b = v.try_get_or_insert_pinned(b"beta").unwrap();
        assert_ne!(vid_slot(b), vid_slot(a), "数え終える前は使い回さない");
        v.finish_refs();
        assert_eq!(v.reclaim_stats().0, 1, "参照 0 の alpha が待ち行列に入る");
        // 待ち行列に居る alpha を使い直す
        assert_eq!(v.try_get_or_insert_pinned(b"alpha").unwrap(), a);
        let c = v.try_get_or_insert_pinned(b"gamma").unwrap();
        assert_ne!(vid_slot(c), vid_slot(a), "使い直された場所は回収しない");
        assert_eq!(v.get_checked(a), Some(&b"alpha"[..]));
        assert_eq!(v.reclaim_stats().0, 0);
    }

    /// #381: 使い回しを繰り返しても、 死んだ entry で表が埋まらない (作り直して落とす)。 引きは常に正しい。
    #[test]
    fn churn_does_not_fill_the_index_with_dead_entries() {
        let (_r, v) = reclaiming(4096);
        v.finish_refs();
        let mut live = std::collections::VecDeque::new();
        for i in 0..50_000u32 {
            let id = v.try_get_or_insert_pinned(format!("id-{i}").as_bytes()).expect("insert");
            live.push_back((i, id));
            if live.len() > 100 {
                let (_, old) = live.pop_front().unwrap();
                v.release(old);
            }
        }
        assert!(v.count() <= 200, "使い回しで場所が増えない: {}", v.count());
        for &(i, id) in &live {
            assert_eq!(v.lookup(format!("id-{i}").as_bytes()), Some(id), "{i}");
        }
        assert_eq!(v.lookup(b"id-0"), None);
        assert!(v.index_table_slots() <= 4096, "表が伸び続けない: {}", v.index_table_slots());
    }

    /// #381: 世代は offsets に残るので、 開き直しても古い番号は該当なし。 索引も世代込みで作り直す。
    #[test]
    fn generation_survives_reload() {
        let (r, v) = reclaiming(64);
        let a = v.try_get_or_insert_pinned(b"alpha").unwrap();
        v.finish_refs();
        v.release(a);
        let b = v.try_get_or_insert_pinned(b"beta").unwrap();
        drop(v);
        for readonly in [false, true] {
            let w = r.vocab_load(readonly);
            assert_eq!(w.get_checked(a), None, "readonly {readonly}");
            assert_eq!(w.get_checked(b), Some(&b"beta"[..]));
            assert_eq!(w.lookup(b"beta"), Some(b), "readonly {readonly}");
            assert_eq!(w.lookup(b"alpha"), None, "readonly {readonly}");
        }
    }

    /// #381: 回収が無効なら今までどおり (押さえる・返すは何もしない、 使い回さない)。
    #[test]
    fn without_reclaim_nothing_changes() {
        let r = make_regions(64, 64, 1 << 20);
        let v = r.vocab_init(64, 64);
        let a = v.try_get_or_insert_pinned(b"alpha").unwrap();
        v.release(a);
        v.finish_refs();
        let b = v.try_get_or_insert_pinned(b"beta").unwrap();
        assert_eq!((a, b), (0, 1));
        assert_eq!(v.ref_count(a), None);
    }

    /// #381: data の上限は並行 insert が同時に先取りを抜けても越えない (越える側は番号を捨てて `Full`)。
    /// 割り込み点で、 先取りを抜けた insert の間に別の insert が残りの data を使い切る窓を作る。
    #[test]
    fn data_limit_holds_when_another_insert_slips_past_the_precheck() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w: &'static Vocabulary = Box::leak(Box::new(r.vocab_init(1024, 1024)));
        // data は header 16 B + 値 2 つ分 (10 B ずつ)
        w.set_limits(1024, HEADER + 20).unwrap();
        assert_eq!(w.try_insert(b"aaaaaaaaaa"), Ok(0));
        AFTER_PRECHECK.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                assert_eq!(w.try_insert(b"bbbbbbbbbb"), Ok(1), "割り込んだ insert は残りに入る");
            }))
        });
        assert_eq!(w.try_insert(b"cccccccccc"), Err(VocabFail::Full), "先取りを抜けても上限を越えて書かない");
        assert_eq!(w.data_footprint() as usize, HEADER + 20);
        assert_eq!((w.get(0), w.get(1)), (&b"aaaaaaaaaa"[..], &b"bbbbbbbbbb"[..]));
        assert_eq!(w.lookup(b"cccccccccc"), None);
        // 上限を伸ばせば入る
        w.set_limits(1024, HEADER + 30).unwrap();
        assert!(w.try_insert(b"cccccccccc").is_ok());
    }

    /// #328: 番号を取った直後に伸ばせなかった insert の番号は、 次の insert に配り直さない (巻き戻すと、 並行に
    /// 次の番号を取った insert が居た時に同じ番号を 2 回配り、 offsets を上書きする)。 捨てた番号は空の値として
    /// 読め、 開き直しても件数は戻らない。
    #[test]
    fn failed_take_is_not_reissued() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        assert_eq!(w.try_insert(b"a"), Ok(0));
        FAIL_AFTER_TAKE.with(|f| f.set(true));
        assert_eq!(w.try_insert(b"b"), Err(VocabFail::Space));
        FAIL_AFTER_TAKE.with(|f| f.set(false));
        assert_eq!(w.try_insert(b"c"), Ok(2), "捨てた番号 1 を配り直した");
        assert_eq!((w.get(0), w.get(1), w.get(2)), (&b"a"[..], &b""[..], &b"c"[..]));
        drop(w);
        let v = r.vocab_load(false);
        assert_eq!(v.count(), 3);
        assert_eq!((v.lookup(b"a"), v.lookup(b"c")), (Some(0), Some(2)));
    }

    /// index 領域を直接叩くための helper。
    impl Regions {
        // test 専用 helper。 単一 writer 運用不変式の下で torn write を模す。 #83 で
        // `slice_mut` を廃したので、 本番と同じ Region write API 経由で書く。
        fn index_region(&self) -> Region {
            unsafe { Region::new(self.index_ptr, self.index_len) }
        }
        /// vid を持つ live slot を探して 0 クリア (= torn write で slot 欠落を模す)。
        fn clear_slot_of(&self, vid: u32) {
            let region = self.index_region();
            let mut off = INDEX_HEADER;
            while off + INDEX_SLOT_SIZE <= self.index_len {
                let (flag, slot_vid) = {
                    let xm = region.slice();
                    (
                        xm[off],
                        u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap()),
                    )
                };
                if flag != 0 && slot_vid == vid {
                    region.fill_at(off, INDEX_SLOT_SIZE, 0);
                }
                off += INDEX_SLOT_SIZE;
            }
        }
        /// value の probe home 以降で最初の空 slot に (hash, vid) を植える
        /// (= torn write で count より先行した「未来」slot を模す)。
        fn plant_slot(&self, index_cap: u32, value: &[u8], vid: u32) {
            let region = self.index_region();
            let mask = (index_cap - 1) as u64;
            let h = fxhash(value);
            let mut idx = home_slot(h, index_cap); // #123
            loop {
                let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
                if region.slice()[off] == 0 {
                    region.write_at(off, &[1]);
                    region.write_at(off + 1, &h.to_le_bytes());
                    region.write_at(off + 9, &vid.to_le_bytes());
                    return;
                }
                idx = ((idx as u64 + 1) & mask) as usize;
            }
        }
    }

    /// #92: dirty reopen (no-clear rebuild) が全 live value を保持し、 dedup も
    /// 効くこと。 on-disk index が data と consistent (通常の落ち方) な標準ケース。
    #[test]
    fn dirty_rebuild_preserves_all_values() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        let mut ids = Vec::new();
        for i in 0..300 {
            ids.push(w.get_or_insert(format!("v{i}").as_bytes()));
        }
        w.sync(); // count/data_end を書き戻す。 clean_flag は 0 のまま = dirty。
        drop(w);

        let w2 = r.vocab_load(/*readonly=*/ false); // no-clear rebuild
        for i in 0..300 {
            assert_eq!(
                w2.lookup(format!("v{i}").as_bytes()),
                Some(ids[i]),
                "v{i} が dirty rebuild 後に消えた"
            );
        }
        assert_eq!(w2.lookup(b"absent"), None);
        assert_eq!(w2.get_or_insert(b"v0"), ids[0], "既存値の dedup が壊れた");
        let fresh = w2.get_or_insert(b"brand-new");
        assert_eq!(fresh, 300, "新規値は次の id を取るはず");
        assert_eq!(w2.lookup(b"brand-new"), Some(300));
    }

    /// #92: torn-behind (index が count より遅れて slot 欠落) を dirty rebuild が
    /// self-heal すること。
    #[test]
    fn dirty_rebuild_self_heals_missing_slot() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(w.get_or_insert(format!("k{i}").as_bytes()));
        }
        w.sync();
        r.clear_slot_of(ids[7]); // torn: k7 の slot が flush されず消失
        drop(w);

        let w2 = r.vocab_load(false);
        assert_eq!(
            w2.lookup(b"k7"),
            Some(ids[7]),
            "torn-behind の欠落 slot が self-heal されない"
        );
        for i in 0..20 {
            assert_eq!(w2.lookup(format!("k{i}").as_bytes()), Some(ids[i]));
        }
    }

    /// #92: **vid >= max_entries の破損 slot** (bit-rot 等) があっても rebuild / lookup / insert が offsets region を
    /// 溢れて OOB せず正しく振る舞うこと。 破損 slot の hash がクエリと衝突する配置にして guard 経路を必ず踏ませる。
    /// #374 から dirty open の rebuild は表を 0 から作り直すので破損 slot は消える。 lookup / insert の guard は、
    /// 開いた後の表に植えた破損 slot で踏む。
    #[test]
    fn dirty_rebuild_tolerates_corrupt_slot_no_oob() {
        let max_entries = 1024u32;
        let r = make_regions(max_entries, 1024, 64 * 1024);
        let index_cap = 1024u32; // 1024.next_power_of_two()
        let w = r.vocab_init(max_entries, index_cap);
        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(w.get_or_insert(format!("t{i}").as_bytes()));
        }
        w.sync();
        let count = w.count(); // 10
        // 破損 slot: value "probe-me" の home 以降に slot_hash=fxhash("probe-me")、
        // vid=9999 (>= max_entries=1024) を植える。 offsets region は max_entries×8 しか
        // 無いので guard 無しで get(9999) を呼ぶと offsets region 自体を OOB する配置。
        r.plant_slot(index_cap, b"probe-me", 9999);
        drop(w);

        let w2 = r.vocab_load(false); // rebuild は破損 slot で OOB してはならない
        assert_eq!(w2.lookup(b"probe-me"), None, "rebuild 後に破損 slot が誤 hit");
        // 開いた後の表に、 もう一度破損 slot を植える (lookup / insert の guard を踏ませる)
        r.plant_slot(index_cap, b"probe-me", 9999);
        // probe-me は未挿入 → 破損 slot を skip して None (誤 hit / OOB しない)。
        assert_eq!(w2.lookup(b"probe-me"), None, "破損 slot が誤 hit / OOB");
        for i in 0..10 {
            assert_eq!(w2.lookup(format!("t{i}").as_bytes()), Some(ids[i]));
        }
        // 挿入も破損 slot を skip して新 id を取れること。
        let np = w2.get_or_insert(b"probe-me");
        assert_eq!(np, count, "破損 slot を跨いだ挿入が新 id を取れない");
        assert_eq!(w2.lookup(b"probe-me"), Some(count));
    }

    /// #92: guard を `vid < max_entries` (不変) にしたので、 並行 `get_or_insert` が
    /// valid slot を stale count で取りこぼして dedup を壊す race が無いこと。 8 thread で
    /// 重複する値集合を叩き、 全 distinct 値が lost せず findable であること + OOB
    /// panic しないことを確認する (index_insert / lookup 双方を contention 下で踏む)。
    #[test]
    fn concurrent_get_or_insert_stays_findable() {
        use std::sync::Arc;
        let max_entries = 16384u32;
        let r = make_regions(max_entries, 16384, 1024 * 1024);
        let w = Arc::new(r.vocab_init(max_entries, 16384));
        let n_distinct = 128usize;
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let w = w.clone();
                std::thread::spawn(move || {
                    for i in 0..1000usize {
                        let v = format!("v{:05}", (i * 7 + t) % n_distinct);
                        let id = w.get_or_insert(v.as_bytes());
                        assert!(id < max_entries, "vocab full / 不正 id");
                    }
                })
            })
            .collect();
        for h in threads {
            h.join().unwrap();
        }
        // 全 distinct 値が findable (取りこぼし無し) かつ返る vid の実体が一致。
        for k in 0..n_distinct {
            let v = format!("v{:05}", k);
            let got = w.lookup(v.as_bytes());
            assert!(got.is_some(), "並行 insert 後に {v} が lost (dedup race)");
            assert_eq!(w.get(got.unwrap()), v.as_bytes(), "{v} の vid 実体不一致");
        }
    }

    impl Regions {
        fn set_clean_flag(&self, v: u32) {
            let dm = unsafe { std::slice::from_raw_parts_mut(self.data_ptr, self.data_len) };
            dm[CLEAN_FLAG_OFF..CLEAN_FLAG_OFF + 4].copy_from_slice(&v.to_le_bytes());
        }
        /// 0.14 〜 0.28.5 (VIX3) が書いた index を再現する — 全ゼロ化して VIX3 magic + 全域の表 (今と同じ slot
        /// 選択) で live entry を植える。
        fn plant_full_index(&self, index_cap: u32, entries: &[(&[u8], u32)]) {
            let xm = unsafe { std::slice::from_raw_parts_mut(self.index_ptr, self.index_len) };
            xm.fill(0);
            xm[0..4].copy_from_slice(&INDEX_MAGIC_V3);
            xm[4..8].copy_from_slice(&index_cap.to_le_bytes());
            for (value, vid) in entries {
                let h = fxhash(value);
                let mut idx = home_slot(h, index_cap);
                loop {
                    let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
                    if xm[off] == 0 {
                        xm[off] = 1;
                        xm[off + 1..off + 9].copy_from_slice(&h.to_le_bytes());
                        xm[off + 9..off + 13].copy_from_slice(&vid.to_le_bytes());
                        break;
                    }
                    idx = (idx + 1) & (index_cap as usize - 1);
                }
            }
        }
        /// 今の表 (`v.table()`) の中の使用中 slot の数。
        fn table_occupied(&self, v: &Vocabulary) -> usize {
            let g = v.table();
            let xm = unsafe { std::slice::from_raw_parts(self.index_ptr, self.index_len) };
            (0..g.cap() as usize).filter(|&i| xm[g.slot_off(i)] != 0).count()
        }
        /// index 領域の中で 0 でない最後の byte の位置 (書いた所の上限)。
        fn last_written(&self) -> usize {
            let xm = unsafe { std::slice::from_raw_parts(self.index_ptr, self.index_len) };
            xm.iter().rposition(|&b| b != 0).unwrap_or(0)
        }
    }

    /// #374: 表は語数に合わせて伸びる (語数の 2〜4 倍、 最小 `FIRST_GEN_CAP`)。 書くのは領域の先頭の、 語数に比例する
    /// 範囲だけ — 大きな領域 (entity cap の大きい DB) でも、 語が少なければ触るページは少ない。
    #[test]
    fn issue374_table_grows_with_the_number_of_values() {
        let cap = 1u32 << 20; // 13 MB の領域
        let r = make_regions(cap, cap, 1 << 20);
        let w = r.vocab_init(cap, cap);
        assert_eq!(w.index_table_slots(), FIRST_GEN_CAP);
        let n = 20_000u32;
        let ids: Vec<u32> = (0..n).map(|i| w.get_or_insert(format!("value-{i}").as_bytes())).collect();
        let slots = w.index_table_slots();
        assert!(slots >= 2 * n && slots <= 4 * n, "表の大きさ {slots} が語数 {n} の 2〜4 倍でない");
        // 書いたのは今までの表 (倍々なので合わせて今の表の 2 倍まで) の範囲だけ
        let limit = INDEX_HEADER + 2 * slots as usize * INDEX_SLOT_SIZE;
        assert!(r.last_written() < limit, "表の外 ({} > {limit}) を書いた", r.last_written());
        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(w.lookup(format!("value-{i}").as_bytes()), Some(id), "value-{i}");
        }
        assert_eq!(w.lookup(b"absent"), None);
        assert_eq!(w.get_or_insert(b"value-7"), ids[7], "伸ばした後の重複検査");
    }

    /// #374: 今の表の直後に倍の表を置けなくなったら、 領域の先頭から全域の表に組み直す。 辞書の上限 (`max_entries`)
    /// まで入り、 その先は `Full` (昔と同じ)。
    #[test]
    fn issue374_table_falls_back_to_the_whole_region_and_keeps_the_capacity() {
        let cap = 4096u32; // 表は 1024 → 2048 (1024 の直後) → 全域 4096 (直後に置けない)
        let r = make_regions(cap, cap, 1 << 20);
        let w = r.vocab_init(cap, cap);
        let mut seen = vec![w.index_table_slots()];
        let mut ids = Vec::new();
        for i in 0..cap {
            ids.push(w.try_insert(format!("k{i}").as_bytes()).unwrap_or_else(|e| panic!("k{i}: {e:?}")));
            if *seen.last().unwrap() != w.index_table_slots() {
                seen.push(w.index_table_slots());
            }
        }
        assert_eq!(seen, vec![1024, 2048, 4096], "表の大きさの移り方");
        assert_eq!(w.table(), Gen { base: 0, bits: 12, ver: w.table().ver }, "全域の表は領域の先頭から");
        assert_eq!(w.try_insert(b"one-more"), Err(VocabFail::Full), "上限の先は Full");
        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(w.lookup(format!("k{i}").as_bytes()), Some(id), "k{i}");
        }
    }

    /// #374: graceful close (clean) の後は表をそのまま使う (作り直さない)。 crash 後 (dirty) は data から語数に
    /// 合った表を作り直す。 どちらも全部引ける。
    #[test]
    fn issue374_reopen_keeps_or_rebuilds_the_table() {
        let cap = 1u32 << 16;
        let r = make_regions(cap, cap, 1 << 20);
        let ids: Vec<u32> = {
            let w = r.vocab_init(cap, cap);
            let ids = (0..3000).map(|i| w.get_or_insert(format!("x{i}").as_bytes())).collect();
            // 同じ値を別の id で (Leaf の書き込みは重複を見ない)。 作り直しでは先の id だけが表に載る
            for i in 0..50 {
                w.insert(format!("x{i}").as_bytes());
            }
            w.sync();
            w.mark_index_clean(true);
            ids
        };
        let check = |v: &Vocabulary| {
            for (i, &id) in ids.iter().enumerate() {
                assert_eq!(v.lookup(format!("x{i}").as_bytes()), Some(id), "x{i}");
            }
        };
        let clean = r.vocab_load(false);
        assert!(!clean.rebuilt_on_load, "clean なのに作り直した");
        let kept = clean.table();
        assert!(kept.base > 0, "前提: 伸ばした表 (先頭でない) を持っている");
        check(&clean);
        drop(clean);
        r.set_clean_flag(0); // crash 相当
        let dirty = r.vocab_load(false);
        assert!(dirty.rebuilt_on_load);
        assert_eq!(dirty.table().base, 0, "作り直す表は領域の先頭から");
        assert!(dirty.index_table_slots() <= 4 * 3000);
        assert_eq!(r.table_occupied(&dirty), 3000, "同じ値を 2 度載せた");
        check(&dirty);
        assert_eq!(dirty.get_or_insert(b"x5"), ids[5]);
        assert_eq!(dirty.get_or_insert(b"new"), 3050);
    }

    /// #374: 旧 binary (全域の表しか知らない) が閉じた `VIX4` の DB (clean flag 1) は、 表が全域の形になって
    /// いるかもしれないので作り直す。 clean flag 1 を 「clean」 と信じると、 旧 binary が入れた語を見落とし、
    /// 同じ値に別の id を配る。
    #[test]
    fn issue374_vix4_closed_by_an_old_binary_is_rebuilt() {
        let cap = 1u32 << 12;
        let r = make_regions(cap, cap, 1 << 20);
        let ids: Vec<u32> = {
            let w = r.vocab_init(cap, cap);
            let ids = (0..100).map(|i| w.get_or_insert(format!("y{i}").as_bytes())).collect();
            w.sync();
            ids
        };
        // 旧 binary が全域の表に作り直して閉じた状態: magic は VIX4 のまま、 表は全域、 clean flag 1
        let entries: Vec<(String, u32)> = ids.iter().enumerate().map(|(i, &id)| (format!("y{i}"), id)).collect();
        let refs: Vec<(&[u8], u32)> = entries.iter().map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_full_index(cap, &refs);
        let xm = unsafe { std::slice::from_raw_parts_mut(r.index_ptr, r.index_len) };
        xm[0..4].copy_from_slice(&INDEX_MAGIC);
        r.set_clean_flag(1);
        let v = r.vocab_load(false);
        assert!(v.rebuilt_on_load, "旧 binary の clean (1) を信じた");
        for (k, id) in &entries {
            assert_eq!(v.lookup(k.as_bytes()), Some(*id), "{k}");
        }
        // readonly でも同じ (shadow で引く)
        let ro = r.vocab_load(true);
        assert_eq!(ro.lookup(b"y42"), Some(ids[42]));
    }

    /// #374: `VIX3` (全域の表、 0.14 〜 0.28.5) の DB。 readonly はそのまま全域の表として読み、 書き手が開くと
    /// 語数に合った表に作り直して `VIX4` にする。 先に開いていた readonly は、 magic が変わったら新しい表を読む。
    #[test]
    fn issue374_vix3_index_is_converted_by_the_writer() {
        let cap = 1u32 << 16;
        let r = make_regions(cap, cap, 1 << 20);
        let ids: Vec<u32> = {
            let w = r.vocab_init(cap, cap);
            let ids = (0..500).map(|i| w.get_or_insert(format!("z{i}").as_bytes())).collect();
            w.sync();
            ids
        };
        let entries: Vec<(String, u32)> = ids.iter().enumerate().map(|(i, &id)| (format!("z{i}"), id)).collect();
        let refs: Vec<(&[u8], u32)> = entries.iter().map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_full_index(cap, &refs);
        r.set_clean_flag(1); // 0.28.5 の graceful close

        let ro = r.vocab_load(true);
        assert!(!ro.rebuilt_on_load && ro.shadow_index.is_none(), "clean な VIX3 は readonly がそのまま読む");
        assert_eq!(ro.lookup(b"z7"), Some(ids[7]));

        let w = r.vocab_load(false);
        assert!(w.rebuilt_on_load, "書き手は VIX3 を作り直す");
        assert_eq!(&r.index_bytes()[0..4], &INDEX_MAGIC);
        assert!(w.index_table_slots() <= 4 * 500, "作り直した表が語数に合っていない: {}", w.index_table_slots());
        assert_eq!(r.table_occupied(&w), 500, "作り直した表に旧形式の slot が残っている");
        for (k, id) in &entries {
            assert_eq!(w.lookup(k.as_bytes()), Some(*id), "{k}");
            assert_eq!(ro.lookup(k.as_bytes()), Some(*id), "先に開いた readonly が {k} を引けない");
        }
        let fresh = w.get_or_insert(b"after-convert");
        assert_eq!(ro.lookup(b"after-convert"), Some(fresh), "readonly が新しい表を読んでいない");
    }

    /// #374: 表を伸ばしている最中 (直後に置く / 全域へ組み直す) も、 読み手は入れ終えた語を必ず見つける。
    /// 書き手 1 本が語を入れ続け、 読み手 3 本が入れ終えた語を引き続ける。
    #[test]
    fn issue374_readers_never_miss_values_while_the_table_grows() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicU32;
        let cap = 1u32 << 14; // 1024 → … → 8192 (直後) → 16384 (全域へ組み直す)
        let r = make_regions(cap, cap, 1 << 20);
        let w = Arc::new(r.vocab_init(cap, cap));
        let done = Arc::new(AtomicU32::new(0));
        let n = cap - 100;
        let readers: Vec<_> = (0..3u32)
            .map(|k| {
                let (w, done) = (w.clone(), done.clone());
                std::thread::spawn(move || {
                    let mut reads = 0u64;
                    loop {
                        let d = done.load(Ordering::Acquire);
                        if d >= n {
                            return reads;
                        }
                        if d == 0 {
                            std::hint::spin_loop();
                            continue;
                        }
                        let i = (reads as u32).wrapping_mul(2_654_435_761).wrapping_add(k) % d;
                        assert_eq!(w.lookup(format!("g{i}").as_bytes()), Some(i), "入れ終えた g{i} を見落とした");
                        reads += 1;
                    }
                })
            })
            .collect();
        for i in 0..n {
            assert_eq!(w.get_or_insert(format!("g{i}").as_bytes()), i);
            done.store(i + 1, Ordering::Release);
        }
        let reads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(reads > 0);
        assert_eq!(w.index_table_slots(), cap, "前提: 全域の表まで伸びた");
        for i in (0..n).step_by(37) {
            assert_eq!(w.lookup(format!("g{i}").as_bytes()), Some(i));
        }
    }

    /// #374: 読み手が表を読んだ直後に、 書き手が全域の表へ組み直す (今の表と重なる領域を書き換える)。 読み手は
    /// 引き終えた後で表が変わっていないかを見て、 変わっていたら読み直すので、 見落とさない。
    #[test]
    fn issue374_reader_rereads_when_the_table_is_rebuilt_under_it() {
        use std::sync::Arc;
        let cap = 4096u32; // 1024@0 → 2048@1024 → (語 1025 個目で) 全域 4096@0
        let r = make_regions(cap, cap, 1 << 20);
        let w = Arc::new(r.vocab_init(cap, cap));
        let ids: Vec<u32> = (0..1024).map(|i| w.get_or_insert(format!("q{i}").as_bytes())).collect();
        assert_eq!((w.table().base, w.index_table_slots()), (1024, 2048), "前提: 直後に置いた表");
        let writer = w.clone();
        AFTER_TABLE_READ.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                writer.get_or_insert(b"q-trigger");
                assert_eq!(writer.index_table_slots(), cap, "前提: この insert で全域へ組み直した");
            }))
        });
        for (i, &id) in ids.iter().enumerate() {
            assert_eq!(w.lookup(format!("q{i}").as_bytes()), Some(id), "組み直しの最中に読んだ q{i} を見落とした");
        }
    }

    /// #374: 書き手が多数でも、 表を伸ばす間に入れた語は失われず、 同じ値に 2 つの id を返さない
    /// (`grow_lock` で書き込みを止め、 切り替えた後の表に入れる)。
    ///
    /// 同じ値を同時に入れると、 負けた側の番号は使われずに残る (data にも残る)。 この書き方 (8 thread が 143 歩ずつ
    /// ずれて同じ値を追う) では値の 4〜5 倍の番号を使う — 0.28.5 でも同じなので、 番号の上限は広く取る。
    #[test]
    fn issue374_concurrent_writers_across_growth() {
        use std::sync::Arc;
        let cap = 1u32 << 17;
        let r = make_regions(cap, cap, 1 << 22);
        let w = Arc::new(r.vocab_init(cap, cap));
        let distinct = 12_000usize;
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let w = w.clone();
                std::thread::spawn(move || {
                    (0..distinct)
                        .map(|i| {
                            let k = (i * 7 + t * 1_001) % distinct;
                            let got = w.try_get_or_insert(format!("c{k}").as_bytes());
                            let id = got.unwrap_or_else(|e| panic!("c{k}: {e:?} (count {}, table {})", w.count(), w.index_table_slots()));
                            (k, id)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut first: Vec<Option<u32>> = vec![None; distinct];
        for h in threads {
            for (k, id) in h.join().unwrap() {
                assert!(id < cap, "c{k}: 入らなかった");
                match first[k] {
                    None => first[k] = Some(id),
                    Some(prev) => assert_eq!(prev, id, "c{k} に 2 つの id を配った"),
                }
            }
        }
        for (k, id) in first.iter().enumerate() {
            assert_eq!(w.lookup(format!("c{k}").as_bytes()), *id, "c{k}");
        }
        assert!(w.index_table_slots() >= 2 * distinct as u32, "前提: 表が伸びている");
    }
}

/// offsets/data region から id の value slice を読む。 `get` と `rebuild_index_into`
/// で共有する (後者は index を `&mut` 借用中に呼ぶため `&self` メソッドではなく
/// region を直接受ける free fn にして分割借用を可能にする)。
#[inline]
/// 場所 `slot` の語と世代。 offsets の 8 byte (位置 u32 | 長さ u32 の上位 2 bit = 世代) を 1 回で読む (回収で
/// 場所を別の語に渡す書き手と、 位置と長さが食い違わないように、 #381)。
fn read_slot<'a>(offsets: &'a Region, data: &'a Region, slot: u32) -> (&'a [u8], u32) {
    let e = offsets.as_atomic_u64(slot as usize * 8).load(Ordering::Acquire);
    let offset = e as u32 as usize;
    let len_gen = (e >> 32) as u32;
    let len = (len_gen & LEN_MASK) as usize;
    let dm = data.slice();
    (&dm[offset..offset + len], len_gen >> VID_GEN_SHIFT)
}

/// offsets の 8 byte の値 (位置、 長さ、 世代)。
#[inline]
fn slot_entry(offset: u32, len: u32, generation: u32) -> u64 {
    offset as u64 | ((len | (generation & 3) << VID_GEN_SHIFT) as u64) << 32
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
