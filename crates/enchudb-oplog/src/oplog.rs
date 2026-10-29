//! Operation log — append-only op stream for peer sync + audit + recovery.
//!
//! 0.6.0 で `enchudb-wal` から rename (issue #8)。 実態は write-ahead log では
//! なく oplog (MongoDB oplog と同パターン): mmap が primary state、 oplog は
//! 「何が起きたか」 の正準ストリーム。 wire format は v2 で不変、 在野の
//! file magic は歴史的経緯で `EWAL` のまま (= 既存 file binary 互換のため)。
//!
//! # レイアウト (oplog v2)
//!
//! - **eid を u64 化**(分散の [peer|local] 合成 ID)
//! - **HLC スロット**: `(wall:8, logical:4, peer:4)` 全順序用
//! - **author_peer スロット**: record を書いた peer(中継 peer 対応)
//! - **署名スロット(64B)**: ed25519(Phase C で埋める、現状 zeros)
//! - **pubkey_fp(8B)**: 署名検証に使う pubkey の先頭 8B(現状 zeros)
//!
//! 破壊変更: v1 record は開けない。v2 magic で判別しエラー。
//!
//! # 設計原則
//!
//! - **読みは oplog に触らない**(pull_raw / query の 10〜70ns は影響ゼロ)
//! - **書きは oplog append を memcpy 1 回で済ます**(tie_async の ~1μs を維持)
//! - **fsync は hot path から外す**(非同期、consumer スレッド側で定期実行)
//!
//! # レコード形式(v3)
//!
//! ```text
//! [magic: 2B "WL"]
//! [version: 1B]         = 3 (2 も読む)
//! [op_type: 1B]         0=Tie, 1=Untie, 2=Delete, 3=Content, 4=Commit, 5=Schema
//! [len: 4B LE]          payload bytes
//! [lsn: 8B LE]          Log Sequence Number(ローカル単調増加、recover 用)
//! [hlc_wall: 8B LE]     HLC wall clock(ms since epoch)
//! [hlc_logical: 4B LE]  HLC logical counter
//! [hlc_peer: 4B LE]     HLC peer id
//! [author_peer: 4B LE]  record を書いた peer の id
//! [crc32: 4B LE]        FNV-1a over (op_type 〜 author_peer ‖ payload)。 v2 は payload だけ
//! [signature: 64B]      ed25519 over (header fixed ‖ payload)。現状 zeros。
//! [pubkey_fp: 8B]       署名に使った pubkey の先頭 8B。現状 zeros。
//! [payload: len B]
//! ```
//!
//! ヘッダ固定部: 2+1+1+4+8 + 8+4+4 + 4 + 4 + 64 + 8 = **112B / record**
//!
//! ```text
//! [周の印: 8B]          (file v3、 #608) payload の後ろ。 [lap: 4B LE][lap ^ LAP_SALT: 4B LE]。 record を書いた時の ring の
//!                       周 (file header の lap)。 署名 / CRC の外 (中継した record にも自分の ring の周を付ける)。 scan は
//!                       header の周と違う record で止まる = 畳んだ後に page に残っている前の周の record を読まない
//! ```
//!
//! Tie payload(v2):    [eid: 8B][himo_id: 2B][_pad: 2B][value: 4B]  = 16B
//! Tie64 payload:      [eid: 8B][himo_id: 2B][_pad: 6B][value: 8B]  = 24B (値が u32 に入らない時)
//! Untie payload(v2):  [eid: 8B][himo_id: 2B][_pad: 6B]             = 16B
//! Delete payload(v2): [eid: 8B]                                     = 8B
//! Content payload(v2):[eid: 8B][key_len: 2B][_pad: 2B][data_len: 4B][key][data]
//! Commit payload:     なし(len = 0)
//!
//! # ファイル配置
//!
//! 別ファイル `{db_path}.oplog` (0.5.0 までは `.wal`)。 sparse mmap、 初期 256MB。
//!
//! ```text
//! [File header 32B]
//!   [magic: 4B "EWAL"]   = 歴史的経緯で wire 不変。
//!   [version: 4B LE]     = 3 (2 も読める)
//!   [head: 8B LE]        writer が atomic 前進
//!   [checkpoint: 8B LE]  consumer が前進(ここまで本体に適用済み)
//!   [capacity: 4B LE]    buffer size (v3。 ring は 4 GiB 未満)
//!   [lap: 4B LE]         ring の周 (v3、 #608)。 畳むたびに +1。 record の周の印と突き合わせる
//! [Records starting at offset 32]
//! ```
//!
//! v2 (0.35 以前) の header は capacity が 8B で周が無く、 record に周の印が無い。 v2 の file は読めて、 最初に畳む時に
//! v3 に変わる (ring が空の時に header を書き換えるだけ)。 v3 の file は 0.35 以前の binary では開けない (`WAL version 3
//! unsupported`) — 下げるには、 きれいに閉じてから `.oplog` を消す (閉じた時点で中身は全部本体に当たっている)。
//!
//! # 2 つの区画 (file v4、 0.37)
//!
//! 1 つの区画は頭から書き足して丸ごと巻き戻すしかないので、 書き手が休まず書くと畳める瞬間が来ず、 空きを作るには append を
//! 止めて全部を書き出すしかなかった (#388 の空き作り。 msync の遅いディスクで書き手が全員 170〜190 ms 止まった)。 v4 は
//! file を 2 つの区画に分けて交互に使う:
//!
//! ```text
//! [区画 A の header 32B][区画 A の record ...]    0 から half
//! [区画 B の header 32B][区画 B の record ...]    half から 2 * half (half = capacity / 2 を HALF_ALIGN に切り下げ)
//! ```
//!
//! - 区画の header は v3 と同じ形 (version = 4、 head / checkpoint は区画の頭からの offset、 capacity は区画の大きさ、 lap)
//! - 書き手は今の区画 (active) に書き足し、 載らなくなったら、 もう一方が空なら開いている group を Commit で閉じてそちらへ
//!   移る (切り替え)。 移った後の古い区画には誰も書き足さないので、 書き出し (fsync + 本体の msync) を append を止めずに
//!   済ませて畳める。 書き手が待つのは、 もう一方をまだ畳めていないのに今の区画も埋まった時だけ
//! - 周 (lap) は 2 つの区画を通して 1 本の数で、 区画を使い始める (畳む / 切り替えで移る) たびに新しい数を置く。 大きい方が
//!   新しい (`lap_newer`、 0 は v2 の 「周なし」)。 どちらが active かは保存せず、 開く時に header から決める
//!   (record のある区画のうち周の新しい方、 どちらも空なら周の新しい方)
//! - 位置 = `(周 << 32) | 区画の中の offset` ([`wal_pos`])。 位置の大小は WAL に積んだ順。 位置の周がどの区画にも無い = その
//!   区画は畳まれた後 (読み手はもう一方を頭から読む)。 v2 の file (周 0) の位置は offset そのもの
//! - 1 件の record は 1 区画に収まること (区画より大きい record は断る)
//!
//! v3 の file は 1 区画の形のまま読み書きし、 最初に畳む時に v4 になる (B の header を書いて fsync → A の capacity → version の
//! 順。 どの途中の像も v3 か v4 として開ける)。 2 区画にできない小さい WAL (`2 * HALF_ALIGN` 未満) は v3 の 1 区画の形で作る。
//! v4 の file は 0.36 以前の binary では開けない — 下げるには、 きれいに閉じてから `{db}/oplog` を消す。

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use memmap2::MmapMut;

use crate::wal_lock::WalGuard;
use crate::{Hlc, PeerId};

const FILE_MAGIC: &[u8; 4] = b"EWAL";
/// 書く file の版。 v4 (0.37): file を 2 つの区画に分けて交互に使う (module doc の 「2 つの区画」)。
pub const OPLOG_FILE_VERSION: u32 = 4;
/// 1 区画の版 (#608): header の capacity を u32 にして空いた 4 byte に ring の周 (lap) を置き、 record ごとに周の印
/// ([`LAP_TRAILER`]) を付ける。 読めて、 最初に畳む時に v4 になる。 2 区画にできない小さい WAL もこの形で作る。
pub const OPLOG_FILE_VERSION_V3: u32 = 3;
/// 読める旧版 (周の印なし、 capacity は u64)。 最初に畳む時に v3 → v4 になる。
pub const OPLOG_FILE_VERSION_V2: u32 = 2;
pub const HEADER_SIZE: usize = 32;
/// v4 の区画の大きさの単位 (区画の境を page にそろえる。 Apple Silicon の page は 16 KiB)。 区画がこれより小さくなる WAL は
/// 1 区画 (v3) の形。
pub const HALF_ALIGN: u64 = 16 * 1024;
/// 開いた時、 次に置く周を header の周の最大からこれだけ先にする。 前の process が置いて header が届かずに落ちた周 (その周の
/// record は page に残りうる) を使い直さない。 書き出し (fsync) の間に置く周は高々 1 つ。
const LAP_SKIP_ON_OPEN: u32 = 16;

/// WAL の中の位置: `(周 << 32) | 区画の中の offset`。 大小は WAL に積んだ順 (周が 2^32 で回るまで)。 周 0 (v2 の file) は
/// offset そのもの。 0 と [`HEADER_SIZE`] は 「どの区画よりも前」 (先頭から読む)。
#[inline]
pub fn wal_pos(lap: u32, offset: u64) -> u64 {
    ((lap as u64) << 32) | offset
}

/// 位置の周。
#[inline]
pub fn wal_pos_lap(pos: u64) -> u32 {
    (pos >> 32) as u32
}

/// 位置の区画の中の offset。
#[inline]
pub fn wal_pos_offset(pos: u64) -> u64 {
    pos & u32::MAX as u64
}

/// 周 `a` が `b` より新しいか (2^32 で回る数の比べ)。 0 (v2 の 「周なし」 / まだ使っていない区画) はどの周よりも古い。
#[inline]
fn lap_newer(a: u32, b: u32) -> bool {
    match (a, b) {
        (0, _) => false,
        (_, 0) => true,
        _ => (a.wrapping_sub(b) as i32) > 0,
    }
}

/// 次の周 (0 は 「周なし」 の印なので飛ばす)。
#[inline]
fn next_lap(l: u32) -> u32 {
    l.wrapping_add(1).max(1)
}

/// 1 つの区画 (v3 / v2 の file と小さい WAL は file 全体で 1 区画)。 head / written / checkpoint は区画の頭 (`base`) からの offset。
/// 位置と大きさは 4 GiB 未満 (WAL は 4 GiB 未満) なので 32 bit (2 区画と採番の欄を 1 本の cache line に置くため、 [`Hot`])。
struct Half {
    /// 区画の header の位置 (file の中の byte offset)。 v3 → v4 で B ができる時に置く (`append_lock` の下)
    base: std::sync::atomic::AtomicU32,
    /// 区画の大きさ (header 込み)。 0 = 使わない区画 (1 区画の形の B)
    size: std::sync::atomic::AtomicU32,
    /// 次に書く位置 (writer が前進)。 header の head にも置く
    head: AtomicU64,
    /// #268: **書き終えた** record の終端。 append は head を先に進めてから record を書くので、 head と
    /// この値の間は書いている最中 = 前の周 (fold 前) の record がまだ残っている。 scan はここまでしか読まない
    /// (読むと前の周の Commit で group を閉じたつもりになり、 cursor を record の境目でない位置へ進めて
    /// bridge が開き直すまで止まった)。 append は append_lock の下で書き終えてから Release で置く。
    written: AtomicU64,
    /// ここまで本体に書き出した (consumer が前進)
    checkpoint: AtomicU64,
    /// #608: この区画の今の周。 record の周の印に書き、 scan はこれと違う record で止まる。 v2 の file は 0
    lap: std::sync::atomic::AtomicU32,
}

impl Half {
    fn new(base: u64, size: u64, head: u64, checkpoint: u64, lap: u32) -> Self {
        Half {
            base: std::sync::atomic::AtomicU32::new(base as u32),
            size: std::sync::atomic::AtomicU32::new(size as u32),
            head: AtomicU64::new(head),
            written: AtomicU64::new(head),
            checkpoint: AtomicU64::new(checkpoint),
            lap: std::sync::atomic::AtomicU32::new(lap),
        }
    }

    #[inline]
    fn base(&self) -> usize {
        self.base.load(Ordering::Acquire) as usize
    }

    #[inline]
    fn size(&self) -> u64 {
        self.size.load(Ordering::Acquire) as u64
    }

    #[inline]
    fn lap(&self) -> u32 {
        self.lap.load(Ordering::Acquire)
    }

    /// record が 1 件も無い (畳んだ後 / まだ使っていない)。
    #[inline]
    fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) <= HEADER_SIZE as u64
    }
}

/// append のたびに `append_lock` の持ち手が読み書きする欄 (区画 2 つ、 今の区画の番号、 LSN、 HLC) を 1 本の cache line (Apple
/// Silicon の 128 B) に置く。 持ち手が別の core に替わるたびに取りに行く line が 1 本で済む (区画の欄と採番の欄が別の line だった
/// 間、 書き手 8 本の append の合計が 0.36 の 0.80〜0.84 倍だった)。
#[repr(C, align(128))]
struct Hot {
    /// 区画 A / B。 1 区画の形 (v3 / v2 の file、 小さい WAL) は A だけ (B は大きさ 0)
    halves: [Half; 2],
    next_lsn: AtomicU64,
    /// 今書き足している区画の番号 (0 / 1)。 `append_lock` の下で変える
    active: std::sync::atomic::AtomicUsize,
    /// 最後に払い出した HLC の (wall(ms), logical)。 logical は wall が進まない時の tiebreaker。
    /// 2 つを 1 つの lock で読み書きする: 別々の atomic だと、 同じ ms の中では wall の CAS が
    /// 同じ値への CAS になって必ず成功し、 並行した採番が同じ logical を読んで同じ HLC を払い出す
    /// (同期経路の `append` と async 経路の `mint_hlc` は別の lock の中から呼ばれる)。
    hlc_state: parking_lot::Mutex<(u64, u32)>,
    /// 自分の、 中身のある record (Commit を除く、 中継を除く) を積んだ回数。 append の直列化の内側で進める (`quiet_since`)
    own_data: AtomicU64,
}

const _: () = assert!(std::mem::size_of::<Hot>() == 128);

/// `append_lock` だけで 1 本の cache line。 lock を取れなかった書き手はこれを回って読む (`lock_or_request`) ので、 持ち手が append
/// のたびに書く欄 (`Hot`、 数) と同じ line にあると、 書くたびに待つ書き手の読みとぶつかる (OpLog の欄の並びが変わって同じ line に
/// 入った間、 書き手 8 本の append の合計が 0.36 の 0.80〜0.84 倍だった)。
#[repr(align(128))]
struct LockLine(crate::wal_lock::WalLock);

impl std::ops::Deref for LockLine {
    type Target = crate::wal_lock::WalLock;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// 区画の header を書く (v3 / v4 の形)。
fn write_half_header(mmap: &mut [u8], base: usize, version: u32, size: u64, head: u64, checkpoint: u64, lap: u32) {
    let h = &mut mmap[base..base + HEADER_SIZE];
    h[0..4].copy_from_slice(FILE_MAGIC);
    h[4..8].copy_from_slice(&version.to_le_bytes());
    h[8..16].copy_from_slice(&head.to_le_bytes());
    h[16..24].copy_from_slice(&checkpoint.to_le_bytes());
    h[OFF_CAPACITY..OFF_CAPACITY + 4].copy_from_slice(&(size as u32).to_le_bytes());
    h[OFF_LAP..OFF_LAP + 4].copy_from_slice(&lap.to_le_bytes());
}

/// capacity の file を 2 区画にした時の区画の大きさ (2 区画にできなければ None)。
fn half_size_for(capacity: u64) -> Option<u64> {
    let h = (capacity / 2) / HALF_ALIGN * HALF_ALIGN;
    (h >= HALF_ALIGN && capacity < u32::MAX as u64).then_some(h)
}

/// append の場所 (`OpLog::alloc`): 区画、 その header の位置、 区画の中の offset、 周 (`append_lock` の下で読んだ値)。
struct Slot<'a> {
    half: &'a Half,
    base: usize,
    off: u64,
    lap: u32,
}

/// [`OpLog::read_plan`] の 1 区画分: 区画の header の位置、 周、 読み始める offset、 書き終えた終端。
#[derive(Clone, Copy)]
struct ReadPart {
    base: usize,
    lap: u32,
    start: u64,
    end: u64,
}

/// 作る / 開く時に決める区画と周の形 ([`OpLog::assemble`] に渡す)。
struct Layout {
    halves: [Half; 2],
    active: usize,
    two: bool,
    next_lap: u32,
    trailer: usize,
}

/// 新しい WAL の header を書いて形を返す。 2 区画にできれば v4 (A を周 1 で使い始め、 B は周 0 = まだ使っていない)、
/// できなければ v3 の 1 区画。
fn fresh_layout(mmap: &mut [u8], capacity: u64) -> Layout {
    let hs = HEADER_SIZE as u64;
    match half_size_for(capacity) {
        Some(h) => {
            write_half_header(mmap, 0, OPLOG_FILE_VERSION, h, hs, hs, 1);
            write_half_header(mmap, h as usize, OPLOG_FILE_VERSION, h, hs, hs, 0);
            Layout {
                halves: [Half::new(0, h, hs, hs, 1), Half::new(h, h, hs, hs, 0)],
                active: 0,
                two: true,
                next_lap: 2,
                trailer: LAP_TRAILER,
            }
        }
        None => {
            write_half_header(mmap, 0, OPLOG_FILE_VERSION_V3, capacity, hs, hs, 1);
            Layout {
                halves: [Half::new(0, capacity, hs, hs, 1), Half::new(0, 0, hs, hs, 0)],
                active: 0,
                two: false,
                next_lap: 2,
                trailer: LAP_TRAILER,
            }
        }
    }
}

/// 開く時に header を読んで形を決める。 v4 の B の header が読めない (v3 → v4 の途中の像で B がまだ無い、 は B を先に書いて
/// fsync するので起きないが、 壊れた file) なら、 B はまだ使っていない区画として header を書き直す。
fn read_layout(mmap: &mut [u8]) -> io::Result<Layout> {
    let bad = |m: String| io::Error::new(io::ErrorKind::InvalidData, m);
    if mmap.len() < HEADER_SIZE {
        return Err(bad("WAL too small".into()));
    }
    if &mmap[0..4] != FILE_MAGIC {
        return Err(bad("bad WAL magic".into()));
    }
    let r64 = |m: &[u8], o: usize| u64::from_le_bytes(m[o..o + 8].try_into().unwrap());
    let r32 = |m: &[u8], o: usize| u32::from_le_bytes(m[o..o + 4].try_into().unwrap());
    let version = r32(mmap, 4);
    let file_len = mmap.len() as u64;
    let hs = HEADER_SIZE as u64;
    // 0.9.0 (L1): header の capacity / head / checkpoint を実 file 長と突き合わせる。 旧実装は無検証だったため、 truncated
    // .oplog (crash / 不完全 copy) が正常に open された後、 append の mmap[offset..] で OOB panic した。
    let in_range = |name: &str, v: u64, capacity: u64| -> io::Result<()> {
        if v < hs || v > capacity {
            return Err(bad(format!("WAL header corrupt: {name} = {v} out of range [{hs}, capacity {capacity}]")));
        }
        Ok(())
    };
    let truncated = |capacity: u64| {
        bad(format!(
            "WAL truncated: header capacity {capacity} exceeds file size {file_len} — file was cut short (crash / partial copy?)"
        ))
    };
    match version {
        OPLOG_FILE_VERSION_V2 | OPLOG_FILE_VERSION_V3 => {
            let (head, checkpoint) = (r64(mmap, 8), r64(mmap, 16));
            // v3: capacity は u32 で、 その後ろが周 (#608)。 v2: capacity は u64、 周の印なし (最初に畳む時に v3 へ)
            let (capacity, lap, trailer) = if version == OPLOG_FILE_VERSION_V3 {
                (r32(mmap, OFF_CAPACITY) as u64, r32(mmap, OFF_LAP), LAP_TRAILER)
            } else {
                (r64(mmap, 24), 0, 0)
            };
            if capacity >= u32::MAX as u64 {
                // 位置 (`wal_pos`) は区画の中の offset を 32 bit に置く
                return Err(bad(format!(
                    "a v2 WAL of {capacity} bytes (4 GiB or more) cannot be opened by this version: close the DB cleanly with \
                     the version that wrote it and delete the oplog file"
                )));
            }
            if capacity > file_len {
                return Err(truncated(capacity));
            }
            in_range("head", head, capacity)?;
            in_range("checkpoint", checkpoint, capacity)?;
            Ok(Layout {
                halves: [Half::new(0, capacity, head, checkpoint, lap), Half::new(0, 0, hs, hs, 0)],
                active: 0,
                two: false,
                next_lap: lap.wrapping_add(LAP_SKIP_ON_OPEN).max(1),
                trailer,
            })
        }
        OPLOG_FILE_VERSION => {
            let h = r32(mmap, OFF_CAPACITY) as u64;
            if h < HALF_ALIGN {
                return Err(bad(format!("WAL header corrupt: half size {h} is below {HALF_ALIGN}")));
            }
            if 2 * h > file_len {
                return Err(truncated(2 * h));
            }
            let (a_head, a_cp, a_lap) = (r64(mmap, 8), r64(mmap, 16), r32(mmap, OFF_LAP));
            in_range("head", a_head, h)?;
            in_range("checkpoint", a_cp, h)?;
            let b = h as usize;
            let b_ok = &mmap[b..b + 4] == FILE_MAGIC
                && r32(mmap, b + 4) == OPLOG_FILE_VERSION
                && r32(mmap, b + OFF_CAPACITY) as u64 == h;
            let (b_head, b_cp, b_lap) = if b_ok {
                let (bh, bc) = (r64(mmap, b + 8), r64(mmap, b + 16));
                in_range("head", bh, h)?;
                in_range("checkpoint", bc, h)?;
                (bh, bc, r32(mmap, b + OFF_LAP))
            } else {
                write_half_header(mmap, b, OPLOG_FILE_VERSION, h, hs, hs, 0);
                (hs, hs, 0)
            };
            // record のある区画のうち周の新しい方 (どちらも空 / どちらにもあるなら周の新しい方)
            let active = match (a_head > hs, b_head > hs) {
                (true, false) => 0,
                (false, true) => 1,
                _ => usize::from(lap_newer(b_lap, a_lap)),
            };
            let top = if lap_newer(b_lap, a_lap) { b_lap } else { a_lap };
            Ok(Layout {
                halves: [Half::new(0, h, a_head, a_cp, a_lap), Half::new(h, h, b_head, b_cp, b_lap)],
                active,
                two: true,
                next_lap: top.wrapping_add(LAP_SKIP_ON_OPEN).max(1),
                trailer: LAP_TRAILER,
            })
        }
        v => Err(bad(format!(
            "WAL version {v} unsupported (expected {OPLOG_FILE_VERSION}, {OPLOG_FILE_VERSION_V3} or {OPLOG_FILE_VERSION_V2})"
        ))),
    }
}
/// v3: header の capacity (u32)。
const OFF_CAPACITY: usize = 24;
/// v3: header の lap (ring の周)。
const OFF_LAP: usize = 28;
/// 周の印 (v3): record の payload の後ろの 8 byte `[lap: u32 LE][lap ^ LAP_SALT: u32 LE]`。
pub const LAP_TRAILER: usize = 8;
const LAP_SALT: u32 = 0xC3A5_5A3C;

/// #451 (段階 1): 切れ目 ([`OpLog::durable_cut_for`])。
#[derive(Clone, Copy, Debug)]
pub struct DurableCutPoint {
    /// 時計の今 (author の record で HLC ≤ hlc のものは全部 ring に書き終えている)
    pub hlc: Hlc,
    /// author の落ちた record (#57) がまだ覚えられたままか (= floor がまだ無い)
    pub dropped_pending: bool,
    /// author の落ちた record を覚えた回数 ([`OpLog::dropped_gen`])。 置く時に変わっていれば、 切れ目の後に落ちた record がある
    /// (畳む時の孤児 (#450) は古い HLC を持つので、 時計では分からない)
    pub dropped_gen: u64,
    /// ring の周と、 切れ目の時の終わり (この周でこれより前の record は全部切れ目より前)
    pub lap: u32,
    pub offset: u64,
    /// 切れ目の時の、 自分の中身のある record を積んだ回数 ([`OpLog::quiet_since`])
    pub own_data: u64,
}

/// 周の印を書く。
#[inline]
fn write_lap_trailer(t: &mut [u8], lap: u32) {
    t[0..4].copy_from_slice(&lap.to_le_bytes());
    t[4..8].copy_from_slice(&(lap ^ LAP_SALT).to_le_bytes());
}

/// 1 record の大きさの見積もり (header + payload + 周の印)。 `append_lock` の外で使う (空きの待ちなど)。 v2 の file (最初に
/// 畳むまで) は印が無いので 8 byte 小さいが、 見積もりは安全側。
#[inline]
fn record_size_hint(payload_size: usize) -> usize {
    REC_HEADER_SIZE + payload_size + LAP_TRAILER
}

const REC_MAGIC: &[u8; 2] = b"WL";
/// 書く record の版。 v3 (#58): CRC が payload だけでなく op / len / lsn / hlc / author も覆う。
/// 旧: CRC は payload だけで、 header が壊れても (ring の前の周の bytes が一部残る torn write 等)
/// magic が残っていれば通り、 ゴミの HLC / author が recovery・bridge・audit に流れた。
const REC_VERSION: u8 = 3;
/// 読める旧版 (CRC は payload だけ)。 旧 binary が書いた WAL と、 旧版の peer が署名した中継 record。
const REC_VERSION_V2: u8 = 2;
/// CRC が覆う header の範囲 (op / len / lsn / hlc / author)。 magic / version は見分けに使い、
/// crc / 署名は覆えない。
const CRC_FIELDS: std::ops::Range<usize> = OFF_OP..OFF_CRC;

fn known_version(v: u8) -> bool {
    v == REC_VERSION || v == REC_VERSION_V2
}

/// record の CRC。 `fields` は header の `CRC_FIELDS` (signed_bytes の同じ位置)。
///
/// v3 は payload の FNV-1a に header の値を 4 byte ずつ混ぜる (xor → 奇数の掛け算は 1 対 1 なので、
/// どの 1 bit が反転しても値が変わる)。 1 byte ずつの FNV-1a で 33 byte 足すと append 1 回が
/// +39 ns (528 → 567 ns) だった。
fn record_crc(version: u8, fields: &[u8], payload: &[u8]) -> u32 {
    let h = fnv1a(payload);
    if version == REC_VERSION_V2 {
        return h;
    }
    // fields は常に 33 byte (最後の塊だけ 1 byte、 残りを 0 で埋める)
    fields.chunks(4).fold(h, |h, c| {
        let mut w = [0u8; 4];
        w[..c.len()].copy_from_slice(c);
        (h ^ u32::from_le_bytes(w)).wrapping_mul(0x01000193)
    })
}

/// 書く record の CRC (v3) を header の値から。
fn new_record_crc(op_byte: u8, payload_len: u32, lsn: u64, hlc: Hlc, author_peer: PeerId, payload: &[u8]) -> u32 {
    let mut f = [0u8; OFF_CRC - OFF_OP];
    f[0] = op_byte;
    f[1..5].copy_from_slice(&payload_len.to_le_bytes());
    f[5..13].copy_from_slice(&lsn.to_le_bytes());
    f[13..21].copy_from_slice(&hlc.wall.to_le_bytes());
    f[21..25].copy_from_slice(&hlc.logical.to_le_bytes());
    f[25..29].copy_from_slice(&hlc.peer.to_le_bytes());
    f[29..33].copy_from_slice(&author_peer.to_le_bytes());
    record_crc(REC_VERSION, &f, payload)
}

/// signed_bytes (header 固定部 ‖ payload) の CRC をその版の規則で計算し直す。
fn signed_bytes_crc(sb: &[u8]) -> u32 {
    record_crc(sb[OFF_VERSION], &sb[CRC_FIELDS], &sb[SIGNED_PAYLOAD_HEADER_SIZE..])
}
/// v2 レコード固定ヘッダ: 2+1+1+4+8 + 8+4+4 + 4 + 4 + 64 + 8 = 112
const REC_HEADER_SIZE: usize = 112;

// ヘッダ内オフセット
const OFF_MAGIC: usize = 0;       // 2
const OFF_VERSION: usize = 2;     // 1
const OFF_OP: usize = 3;          // 1
const OFF_LEN: usize = 4;         // 4
const OFF_LSN: usize = 8;         // 8
const OFF_HLC_WALL: usize = 16;   // 8
const OFF_HLC_LOGICAL: usize = 24;// 4
const OFF_HLC_PEER: usize = 28;   // 4
const OFF_AUTHOR_PEER: usize = 32;// 4
const OFF_CRC: usize = 36;        // 4
const OFF_SIGNATURE: usize = 40;  // 64
const OFF_PUBKEY_FP: usize = 104; // 8
const _: () = assert!(OFF_PUBKEY_FP + 8 == REC_HEADER_SIZE);
// header の署名より前は signed_bytes の固定部と同じ並び (scan はそこを切り出して signed_bytes にする)
const _: () = assert!(OFF_SIGNATURE == SIGNED_PAYLOAD_HEADER_SIZE);

/// Phase C で keypair が無い場合のデフォルト署名(zeros)。
const ZERO_SIGNATURE: [u8; 64] = [0u8; 64];
const ZERO_PUBKEY_FP: [u8; 8] = [0u8; 8];

/// 署名の対象メッセージを組み立てる。
/// header の固定フィールド(magic, version, op, len, lsn, hlc, author, crc)+ payload。
/// signature, pubkey_fp は含めない(署名対象自身を除く)。
fn signed_payload(
    op_byte: u8, payload_len: u32, lsn: u64, hlc: Hlc,
    author_peer: PeerId, crc: u32, payload: &[u8],
) -> Vec<u8> {
    let mut m = Vec::with_capacity(40 + payload.len());
    m.extend_from_slice(REC_MAGIC);
    m.push(REC_VERSION);
    m.push(op_byte);
    m.extend_from_slice(&payload_len.to_le_bytes());
    m.extend_from_slice(&lsn.to_le_bytes());
    m.extend_from_slice(&hlc.wall.to_le_bytes());
    m.extend_from_slice(&hlc.logical.to_le_bytes());
    m.extend_from_slice(&hlc.peer.to_le_bytes());
    m.extend_from_slice(&author_peer.to_le_bytes());
    m.extend_from_slice(&crc.to_le_bytes());
    m.extend_from_slice(payload);
    m
}

/// signed_payload の固定 header 長 (= magic 2 + version 1 + op_byte 1 +
/// payload_len 4 + lsn 8 + hlc 16 + author_peer 4 + crc 4)。
pub const SIGNED_PAYLOAD_HEADER_SIZE: usize = 40;

/// 0.8.0: `_sync_ops.payload` の wire layout — signature + pubkey_fp + signed_bytes。
/// signed_bytes 単独だと署名検証ができないので、 publish path で復元できるよう
/// 全 metadata を concat 形式で持つ。
pub const SYNC_OPS_PAYLOAD_PREFIX: usize = 64 + 8; // signature(64) + pubkey_fp(8)

/// `_sync_ops.payload` (= signature + pubkey_fp + signed_bytes concat) から
/// `Record` を復元する。 0.8.0 で sync publish path が `_sync_ops` 経由になった
/// ことで、 signed_bytes だけでは足りない (= 署名検証用の signature/pubkey_fp が
/// 必要)、 wire payload に concat 形式で格納してある前提。
///
/// 戻り値の `Record.lsn` は `_sync_ops.lsn` ではなく oplog 元 record の lsn
/// (= signed_bytes header に埋め込まれてた値)。 caller は必要なら別途
/// `_sync_ops.lsn` を取る。
pub fn decode_sync_ops_payload(payload: &[u8]) -> Option<Record> {
    if payload.len() < SYNC_OPS_PAYLOAD_PREFIX + SIGNED_PAYLOAD_HEADER_SIZE {
        return None;
    }
    let mut signature = [0u8; 64];
    signature.copy_from_slice(&payload[0..64]);
    let mut pubkey_fp = [0u8; 8];
    pubkey_fp.copy_from_slice(&payload[64..72]);
    let signed_bytes = &payload[SYNC_OPS_PAYLOAD_PREFIX..];
    let (lsn, hlc, author_peer, op) = decode_signed(signed_bytes)?;
    Some(Record {
        lsn, hlc, author_peer, op, signature, pubkey_fp,
        signed_bytes: signed_bytes.to_vec(),
    })
}

/// #568: 署名の対象の bytes (record の header 40 B ‖ payload) から HLC・author・op を読む。 受け手は、 署名を確かめた
/// bytes と、 当てる中身 (wire の欄) が同じ record かをこれで見る。 読めない bytes は None。
pub fn decode_signed_bytes(signed_bytes: &[u8]) -> Option<(Hlc, PeerId, DecodedOp)> {
    decode_signed(signed_bytes).map(|(_, hlc, author, op)| (hlc, author, op))
}

/// signed_bytes の header と payload を読む (lsn, HLC, author, op)。
fn decode_signed(signed_bytes: &[u8]) -> Option<(u64, Hlc, PeerId, DecodedOp)> {
    if signed_bytes.len() < SIGNED_PAYLOAD_HEADER_SIZE { return None; }
    if &signed_bytes[0..2] != REC_MAGIC { return None; }
    if !known_version(signed_bytes[OFF_VERSION]) { return None; }
    let op_byte = signed_bytes[3];
    let payload_len = u32::from_le_bytes(signed_bytes[4..8].try_into().ok()?) as usize;
    let lsn = u64::from_le_bytes(signed_bytes[8..16].try_into().ok()?);
    let hlc_wall = u64::from_le_bytes(signed_bytes[16..24].try_into().ok()?);
    let hlc_logical = u32::from_le_bytes(signed_bytes[24..28].try_into().ok()?);
    let hlc_peer = u32::from_le_bytes(signed_bytes[28..32].try_into().ok()?);
    let author_peer = u32::from_le_bytes(signed_bytes[32..36].try_into().ok()?);
    let payload_off = SIGNED_PAYLOAD_HEADER_SIZE;
    let payload_end = payload_off + payload_len;
    if payload_end > signed_bytes.len() { return None; }
    let op = decode_op(op_byte, &signed_bytes[payload_off..payload_end])?;
    let hlc = Hlc { wall: hlc_wall, logical: hlc_logical, peer: hlc_peer };
    Some((lsn, hlc, author_peer, op))
}

/// #451: `_sync_ops` の payload から author と HLC だけを読む ([`decode_sync_ops_payload`] と同じ形の検査、 record は
/// 組み立てない)。 読めない payload は None。
pub fn sync_ops_payload_author_hlc(payload: &[u8]) -> Option<(PeerId, Hlc)> {
    let sb = payload.get(SYNC_OPS_PAYLOAD_PREFIX..)?;
    if sb.len() < SIGNED_PAYLOAD_HEADER_SIZE || &sb[0..2] != REC_MAGIC || !known_version(sb[OFF_VERSION]) {
        return None;
    }
    let u32_at = |o: usize| u32::from_le_bytes(sb[o..o + 4].try_into().unwrap());
    let wall = u64::from_le_bytes(sb[OFF_HLC_WALL..OFF_HLC_WALL + 8].try_into().unwrap());
    Some((u32_at(OFF_AUTHOR_PEER), Hlc { wall, logical: u32_at(OFF_HLC_LOGICAL), peer: u32_at(OFF_HLC_PEER) }))
}

/// 0.11 (request10 / #76 逆写像): eid を書き換えて re-sign した record。
/// bridge が `_sync_ops.payload` を組み立てるのに必要な 3 点セット。
pub struct ResignedRecord {
    pub signed_bytes: Vec<u8>,
    pub signature: [u8; 64],
    pub pubkey_fp: [u8; 8],
}

/// 0.11 (request10 / #76 逆写像): record の op 内 eid を `new_eid` に書き換えた
/// signed_bytes を再構築し、 `keypair` で re-sign する。 bridge が
/// 「translated foreign entity への self-authored write」 を元 entity の
/// 世界番号で発送するために使う。
///
/// - lsn / hlc / author_peer は元 record の値を**維持**する (= LWW identity と
///   record の由来を保つ。 変わるのは宛名 = eid だけ)
/// - 全 op payload で eid は先頭 8 byte 固定 (v2 layout) なので定位置 patch +
///   crc (その record の版の規則) 再計算 + 全体 re-sign で完結する
/// - eid を持たない op (Commit / Vocab) は None
/// - `keypair` が None なら zero 署名 (= 署名無効運用、 append 時と同じ扱い)
pub fn resign_with_eid(
    rec: &Record,
    new_eid: u64,
    keypair: Option<&crate::keys::Keypair>,
) -> Option<ResignedRecord> {
    match rec.op {
        DecodedOp::Tie { .. }
        | DecodedOp::Untie { .. }
        | DecodedOp::Delete { .. }
        | DecodedOp::Content { .. }
        | DecodedOp::TieNamed { .. }
        | DecodedOp::TieLeaf { .. }
        | DecodedOp::TieRef { .. } => {}
        // #178: Alias の 2 つの eid は書いた時から世界番号 (宛名を書き戻さない)
        DecodedOp::Commit | DecodedOp::Vocab { .. } | DecodedOp::Alias { .. } => return None,
    }
    let mut sb = rec.signed_bytes.clone();
    if sb.len() < SIGNED_PAYLOAD_HEADER_SIZE + 8 {
        return None;
    }
    let payload_len = u32::from_le_bytes(sb[4..8].try_into().ok()?) as usize;
    let payload_off = SIGNED_PAYLOAD_HEADER_SIZE;
    let payload_end = payload_off + payload_len;
    if payload_end > sb.len() || payload_len < 8 {
        return None;
    }
    // eid patch (全 eid 持ち op で payload 先頭 8 byte)
    sb[payload_off..payload_off + 8].copy_from_slice(&new_eid.to_le_bytes());
    // crc はその版の規則で (v3 は header の値も覆う)
    let crc = signed_bytes_crc(&sb);
    sb[36..40].copy_from_slice(&crc.to_le_bytes());
    let (signature, pubkey_fp) = match keypair {
        Some(kp) => (kp.sign(&sb), kp.pubkey_fp()),
        None => (ZERO_SIGNATURE, ZERO_PUBKEY_FP),
    };
    Some(ResignedRecord { signed_bytes: sb, signature, pubkey_fp })
}

/// #183: `Tie` record を **`TieRef` (target 世界番号同乗) に書き換えて** re-sign する。
///
/// bridge が「Ref 値が translated foreign entity を指す self-authored write」を
/// 発送するために使う。wire の `Tie.value` (u32) は元 entity の世界番号 (u64) を
/// 運べないため、op ごと組み替える。`resign_with_eid` と同じく lsn / hlc /
/// author_peer は**維持** (LWW identity 不変、変わるのは op 表現と宛名だけ)。
///
/// - `new_eid`: 行 eid (自行なら元のまま、translated 行なら世界番号へ書き戻し済みの値)
/// - `target_world`: Ref が指す元 entity の世界番号 (`make_eid(owner, owner_local)`)
/// - `Tie` 以外の op は None (TieNamed の Ref は従来どおり呼び元で skip)
pub fn resign_as_tie_ref(
    rec: &Record,
    new_eid: u64,
    target_world: u64,
    keypair: Option<&crate::keys::Keypair>,
) -> Option<ResignedRecord> {
    let himo_id = match rec.op {
        DecodedOp::Tie { himo_id, .. } => himo_id,
        _ => return None,
    };
    let old = &rec.signed_bytes;
    if old.len() < SIGNED_PAYLOAD_HEADER_SIZE {
        return None;
    }
    // header は元 record から流用し、op_byte / payload_len / crc を差し替える
    let mut sb = Vec::with_capacity(SIGNED_PAYLOAD_HEADER_SIZE + 20);
    sb.extend_from_slice(&old[0..SIGNED_PAYLOAD_HEADER_SIZE]);
    sb[3] = op_type::TIE_REF;
    sb[4..8].copy_from_slice(&20u32.to_le_bytes());
    // payload: eid(8) + himo_id(2) + pad(2) + target(8) — Tie と同じ 4-align 規約
    sb.extend_from_slice(&new_eid.to_le_bytes());
    sb.extend_from_slice(&himo_id.to_le_bytes());
    sb.extend_from_slice(&[0u8; 2]);
    sb.extend_from_slice(&target_world.to_le_bytes());
    // op / len を差し替えたので、 v3 の crc (header の値も覆う) はここで計算し直す
    let crc = signed_bytes_crc(&sb);
    sb[36..40].copy_from_slice(&crc.to_le_bytes());
    let (signature, pubkey_fp) = match keypair {
        Some(kp) => (kp.sign(&sb), kp.pubkey_fp()),
        None => (ZERO_SIGNATURE, ZERO_PUBKEY_FP),
    };
    Some(ResignedRecord { signed_bytes: sb, signature, pubkey_fp })
}

/// WAL 初期サイズ(256MB、sparse なので実ディスク消費は書いた分のみ)。
pub const DEFAULT_OPLOG_SIZE: usize = 256 * 1024 * 1024;

/// op_type バイト値。
pub mod op_type {
    pub const TIE: u8 = 0;
    pub const UNTIE: u8 = 1;
    pub const DELETE: u8 = 2;
    pub const CONTENT: u8 = 3;
    pub const COMMIT: u8 = 4;
    pub const SCHEMA: u8 = 5; // 予約(Phase D 以降で使う)
    pub const VOCAB: u8 = 6;  // text vid → bytes 対応を peer 間で運ぶ
    /// 0.9.0: himo を **名前で** 運ぶ Tie。 動的定義される content himo
    /// (`_c_{key}`) は peer 間で himo_id が揃わないため、 id ではなく
    /// full name を self-describing に運び、 受信側が ensure_himo_dynamic
    /// で解決する。 id 変換表が不要 = 再起動でも壊れない。
    pub const TIE_NAMED: u8 = 7;
    /// 0.12.0 (#88): Leaf 終端ノードの payload を **bytes 同乗** で運ぶ Tie。
    /// Leaf は共有辞書 vocab でなく LeafStore に格納するので vid を持たない。
    /// himo は名前で運ぶ (content `_c_{key}` 等 動的 himo と同じ理由)。 受信側は
    /// ensure_himo_named → leaf.insert(bytes) → cell に offset を set する。
    pub const TIE_LEAF: u8 = 8;
    /// #183: Ref 値の target を世界番号 (u64) 同乗で運ぶ Tie。bridge が
    /// 「translated foreign entity への Ref write」を発送するときだけ合成する
    /// (author のローカル oplog には現れない)。
    pub const TIE_REF: u8 = 9;
    /// FILE_VERSION 11: 値が u32 に入らない Tie (64 bit 列)。 payload 24B。 値が u32 に入る Tie は
    /// 列の幅によらず `TIE` のまま (decode 後はどちらも `DecodedOp::Tie { value: u64 }`)。
    pub const TIE64: u8 = 10;
    /// `TIE_NAMED` の 64 bit 値版: eid(8) + value(8) + kind(1) + pad(1) + name_len(2) + name。
    pub const TIE_NAMED64: u8 = 11;

    /// 上の bit が立った op は 「知らない版は読み飛ばしてよい」 (#178)。 oplog の op の種類と wire の op の印の両方で同じ。
    /// 知らない op は、 この bit が無ければ今まで通り止まる (oplog の scan は打ち切り、 wire の batch は丸ごと Err)。 bit が
    /// あれば、 その record だけを飛ばして先を読む (record は長さで区切られているので、 中身を知らなくても飛ばせる)。
    ///
    /// 足す op にこの bit を付けてよいのは、 読み飛ばされても複製が食い違わない op だけ (たとえば 「この 2 つの entity は同じ」
    /// の知らせ: 知らない peer は今まで通り 2 つのまま持つ)。 値を運ぶ op に付けると、 古い peer が黙って値を落とす。
    /// この bit を読める版より前の peer は、 bit の付いた op でも止まる。
    pub const SKIPPABLE: u8 = 0x80;

    /// #178: 「`eid` と `same_as` は同じ entity」 の知らせ。 payload は eid(8) + same_as(8)。 読み飛ばされても複製は今まで通り
    /// (同じ entity が 2 行のまま) なので [`SKIPPABLE`] の印を付ける。
    pub const ALIAS: u8 = SKIPPABLE | 1;

    /// `op` が [`SKIPPABLE`] の印を持つか。
    #[inline]
    pub const fn skippable(op: u8) -> bool {
        op & SKIPPABLE != 0
    }
}

/// WAL に書く所有型 op。 `Op` の owned 版で、 queue 渡し用 (consumer 側で
/// batch して `append_many` に流す経路で使う)。
#[derive(Debug, Clone)]
pub enum OwnedOp {
    Tie { eid: u64, himo_id: u16, value: u64 },
    Untie { eid: u64, himo_id: u16 },
    Delete { eid: u64 },
    Content { eid: u64, key: String, data: Vec<u8> },
    Commit,
    Vocab { vid: u32, bytes: Vec<u8> },
    /// 0.9.0: himo を full name で運ぶ Tie (content 互換層用)。
    /// `himo_kind` は ValueType の生 u8 (受信側の ensure 用)。
    TieNamed { eid: u64, himo_name: String, himo_kind: u8, value: u64 },
    /// 0.12.0 (#88): Leaf payload を bytes 同乗で運ぶ (vid 無し、 himo は名前)。
    TieLeaf { eid: u64, himo_name: String, himo_kind: u8, bytes: Vec<u8> },
}

impl OwnedOp {
    /// #388: WAL に載せた時の record の byte 数 (header + payload + 周の印)。 v2 の file (最初に畳むまで) は周の印が
    /// 無いので 8 byte 小さい = 空きの見積もりは安全側。
    pub fn record_size(&self) -> usize {
        record_size_hint(self.as_op().payload_size())
    }

    /// borrow 版 `Op` への変換 (append 用)。
    pub fn as_op(&self) -> Op<'_> {
        match self {
            OwnedOp::Tie { eid, himo_id, value } =>
                Op::Tie { eid: *eid, himo_id: *himo_id, value: *value },
            OwnedOp::Untie { eid, himo_id } =>
                Op::Untie { eid: *eid, himo_id: *himo_id },
            OwnedOp::Delete { eid } => Op::Delete { eid: *eid },
            OwnedOp::Content { eid, key, data } =>
                Op::Content { eid: *eid, key, data },
            OwnedOp::Commit => Op::Commit,
            OwnedOp::Vocab { vid, bytes } =>
                Op::Vocab { vid: *vid, bytes },
            OwnedOp::TieNamed { eid, himo_name, himo_kind, value } =>
                Op::TieNamed { eid: *eid, himo_name, himo_kind: *himo_kind, value: *value },
            OwnedOp::TieLeaf { eid, himo_name, himo_kind, bytes } =>
                Op::TieLeaf { eid: *eid, himo_name, himo_kind: *himo_kind, bytes },
        }
    }
}

/// WAL に書く op。eid は u64。
#[derive(Debug, Clone)]
pub enum Op<'a> {
    Tie { eid: u64, himo_id: u16, value: u64 },
    Untie { eid: u64, himo_id: u16 },
    Delete { eid: u64 },
    Content { eid: u64, key: &'a str, data: &'a [u8] },
    Commit,
    /// text 文字列を peer 間で運ぶ。`vid` は author_peer ローカルの vocab ID。
    /// receiver 側は `(author_peer, vid) → local_vid` の mapping を持ち、
    /// 後続の Tie { value: vid } を受けたら local_vid に変換して適用する。
    Vocab { vid: u32, bytes: &'a [u8] },
    /// 0.9.0: himo を full name で運ぶ Tie。 動的 himo (content `_c_{key}`) 用。
    TieNamed { eid: u64, himo_name: &'a str, himo_kind: u8, value: u64 },
    /// 0.12.0 (#88): Leaf payload を bytes 同乗で運ぶ Tie (vid 無し、 himo は名前)。
    TieLeaf { eid: u64, himo_name: &'a str, himo_kind: u8, bytes: &'a [u8] },
    /// #183/#209: Ref 値の target を世界番号 (u64) 同乗で運ぶ Tie。 bridge は
    /// resign (byte patch) で合成するが、 relay の verbatim 転送 (#209) は受信
    /// record からこの variant で `append_relayed` する。 layout は decode 側
    /// (op_type::TIE_REF) と同一: eid(8) + himo_id(2) + pad(2) + target(8)。
    TieRef { eid: u64, himo_id: u16, target: u64 },
    /// #178: 「`eid` と `same_as` は同じ entity」 (どちらも世界番号)。 2 台が同じ自然キーの行を別々に作ってから束ねた側が書く
    /// (束ねる前に出て行った書き込みが、 相手で別の行になっている)。 受け手は 2 つを 1 行に畳む。 op の種類は
    /// [`op_type::ALIAS`] (読み飛ばしてよい印付き)。
    Alias { eid: u64, same_as: u64 },
}

impl<'a> Op<'a> {
    /// payload の byte 数(固定 or 動的)。v2 layout。
    #[inline]
    fn payload_size(&self) -> usize {
        match self {
            Op::Tie { value, .. } if *value > u32::MAX as u64 => 24, // eid(8) + himo_id(2) + pad(6) + value(8)
            Op::Tie { .. } => 16,       // eid(8) + himo_id(2) + pad(2) + value(4)
            Op::Untie { .. } => 16,     // eid(8) + himo_id(2) + pad(6)
            Op::Delete { .. } => 8,     // eid(8)
            Op::Content { key, data, .. } => 8 + 2 + 2 + 4 + key.len() + data.len(),
            Op::Commit => 0,
            Op::Vocab { bytes, .. } => 4 + 4 + bytes.len(), // vid(4) + len(4) + bytes
            // eid(8) + value(4) + kind(1) + pad(1) + name_len(2) + name
            Op::TieNamed { himo_name, value, .. } if *value > u32::MAX as u64 => 20 + himo_name.len(),
            Op::TieNamed { himo_name, .. } => 16 + himo_name.len(),
            // eid(8) + kind(1) + pad(1) + name_len(2) + bytes_len(4) + name + bytes
            Op::TieLeaf { himo_name, bytes, .. } => 16 + himo_name.len() + bytes.len(),
            Op::TieRef { .. } => 20, // eid(8) + himo_id(2) + pad(2) + target(8)
            Op::Alias { .. } => 16,  // eid(8) + same_as(8)
        }
    }

    fn op_byte(&self) -> u8 {
        match self {
            Op::Tie { value, .. } if *value > u32::MAX as u64 => op_type::TIE64,
            Op::Tie { .. } => op_type::TIE,
            Op::Untie { .. } => op_type::UNTIE,
            Op::Delete { .. } => op_type::DELETE,
            Op::Content { .. } => op_type::CONTENT,
            Op::Commit => op_type::COMMIT,
            Op::Vocab { .. } => op_type::VOCAB,
            Op::TieNamed { value, .. } if *value > u32::MAX as u64 => op_type::TIE_NAMED64,
            Op::TieNamed { .. } => op_type::TIE_NAMED,
            Op::TieLeaf { .. } => op_type::TIE_LEAF,
            Op::TieRef { .. } => op_type::TIE_REF,
            Op::Alias { .. } => op_type::ALIAS,
        }
    }

    fn write_payload(&self, buf: &mut [u8]) {
        match self {
            Op::Tie { eid, himo_id, value } if *value > u32::MAX as u64 => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..10].copy_from_slice(&himo_id.to_le_bytes());
                buf[10..16].copy_from_slice(&[0u8; 6]);
                buf[16..24].copy_from_slice(&value.to_le_bytes());
            }
            Op::Tie { eid, himo_id, value } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..10].copy_from_slice(&himo_id.to_le_bytes());
                buf[10..12].copy_from_slice(&[0, 0]);
                buf[12..16].copy_from_slice(&(*value as u32).to_le_bytes());
            }
            Op::Untie { eid, himo_id } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..10].copy_from_slice(&himo_id.to_le_bytes());
                buf[10..16].copy_from_slice(&[0u8; 6]);
            }
            Op::Delete { eid } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
            }
            Op::Content { eid, key, data } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                let klen = key.len() as u16;
                let dlen = data.len() as u32;
                buf[8..10].copy_from_slice(&klen.to_le_bytes());
                buf[10..12].copy_from_slice(&[0, 0]);
                buf[12..16].copy_from_slice(&dlen.to_le_bytes());
                let ko = 16;
                let do_ = ko + key.len();
                buf[ko..do_].copy_from_slice(key.as_bytes());
                buf[do_..do_ + data.len()].copy_from_slice(data);
            }
            Op::Commit => {}
            Op::Vocab { vid, bytes } => {
                buf[0..4].copy_from_slice(&vid.to_le_bytes());
                let blen = bytes.len() as u32;
                buf[4..8].copy_from_slice(&blen.to_le_bytes());
                buf[8..8 + bytes.len()].copy_from_slice(bytes);
            }
            Op::TieNamed { eid, himo_name, himo_kind, value } if *value > u32::MAX as u64 => {
                assert!(himo_name.len() <= u16::MAX as usize, "himo name too long");
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..16].copy_from_slice(&value.to_le_bytes());
                buf[16] = *himo_kind;
                buf[17] = 0;
                let nlen = himo_name.len() as u16;
                buf[18..20].copy_from_slice(&nlen.to_le_bytes());
                buf[20..20 + himo_name.len()].copy_from_slice(himo_name.as_bytes());
            }
            Op::TieNamed { eid, himo_name, himo_kind, value } => {
                assert!(himo_name.len() <= u16::MAX as usize, "himo name too long");
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..12].copy_from_slice(&(*value as u32).to_le_bytes());
                buf[12] = *himo_kind;
                buf[13] = 0;
                let nlen = himo_name.len() as u16;
                buf[14..16].copy_from_slice(&nlen.to_le_bytes());
                buf[16..16 + himo_name.len()].copy_from_slice(himo_name.as_bytes());
            }
            Op::TieLeaf { eid, himo_name, himo_kind, bytes } => {
                assert!(himo_name.len() <= u16::MAX as usize, "himo name too long");
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8] = *himo_kind;
                buf[9] = 0;
                let nlen = himo_name.len() as u16;
                buf[10..12].copy_from_slice(&nlen.to_le_bytes());
                let blen = bytes.len() as u32;
                buf[12..16].copy_from_slice(&blen.to_le_bytes());
                buf[16..16 + himo_name.len()].copy_from_slice(himo_name.as_bytes());
                let bo = 16 + himo_name.len();
                buf[bo..bo + bytes.len()].copy_from_slice(bytes);
            }
            Op::TieRef { eid, himo_id, target } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..10].copy_from_slice(&himo_id.to_le_bytes());
                buf[10..12].copy_from_slice(&[0, 0]);
                buf[12..20].copy_from_slice(&target.to_le_bytes());
            }
            Op::Alias { eid, same_as } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..16].copy_from_slice(&same_as.to_le_bytes());
            }
        }
    }
}

/// デコード後の op(読み戻し用、所有型)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedOp {
    Tie { eid: u64, himo_id: u16, value: u64 },
    Untie { eid: u64, himo_id: u16 },
    Delete { eid: u64 },
    Content { eid: u64, key: String, data: Vec<u8> },
    Commit,
    /// peer 間で vocab の (vid, bytes) を運ぶ。`vid` は record の author_peer ローカル。
    Vocab { vid: u32, bytes: Vec<u8> },
    /// 0.9.0: himo full name 付き Tie (動的 content himo 用)。
    TieNamed { eid: u64, himo_name: String, himo_kind: u8, value: u64 },
    /// 0.12.0 (#88): Leaf payload を bytes 同乗で運ぶ Tie (vid 無し)。
    TieLeaf { eid: u64, himo_name: String, himo_kind: u8, bytes: Vec<u8> },
    /// #183: Ref 値を **target の世界番号 (u64) 同乗**で運ぶ Tie。
    ///
    /// wire の `Tie.value` (u32) は translated foreign target の元 entity
    /// (世界番号) を表現できないため、bridge が発送時に `Tie` から書き換える。
    /// author の**ローカル oplog にはこの op は現れない** (bridge 合成専用)。
    /// 受信側は `target` を「産みの親」key (0.11 semantics) で ref target table
    /// 空間の local eid へ翻訳する。
    TieRef { eid: u64, himo_id: u16, target: u64 },
    /// #178: [`Op::Alias`]。
    Alias { eid: u64, same_as: u64 },
}

/// リカバリ結果の 1 レコード(HLC + 署名込み)。
/// signature と pubkey_fp も含めて返し、Syncer/PubkeyStore で検証できる。
#[derive(Debug, Clone)]
pub struct Record {
    pub lsn: u64,
    pub hlc: Hlc,
    pub author_peer: PeerId,
    pub op: DecodedOp,
    pub signature: [u8; 64],
    pub pubkey_fp: [u8; 8],
    /// 署名検証に使う生データ(header 固定部 + payload、署名フィールドを除いたもの)。
    /// これを keypair.sign()/verify() にかける。
    pub signed_bytes: Vec<u8>,
}

/// #268: scan が **なぜそこで止まったか**。
///
/// bridge cursor が進むのは Commit に到達したときだけなので、 「空 scan が続く」 の
/// 原因は **cursor 位置の record が読めない** (`BadMagic` 以下) か **record は在るが
/// Commit が付いていない** (`Head` + 未 commit 件数 > 0) の 2 つに割れる。 従来は
/// どちらも 「0 件」 としか観測できず、 実機の恒久停止 (#268) を切り分けられなかった。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanStop {
    /// head まで読み切った (= 打ち切りではない)。
    Head,
    /// record magic 不一致 = その offset に record が無い (未書き込み / 別用途)。
    BadMagic,
    /// record version 不一致。
    BadVersion,
    /// payload が file 末尾を越える (truncated)。
    OutOfBounds,
    /// payload CRC 不一致 (破損 tail)。
    BadCrc,
    /// 新しい version が書いた未知 op_type。
    UndecodableOp,
    /// #608: 周の印が書きかけ (torn)。
    BadLap,
    /// #608: 前の周の record (畳んだ後も page に残っている中身。 header の周と違う) = この周の終わり。
    StaleLap,
}

impl ScanStop {
    /// ログ / counter 用の短い識別子。
    pub fn as_str(&self) -> &'static str {
        match self {
            ScanStop::Head => "reached-head",
            ScanStop::BadMagic => "bad-magic",
            ScanStop::BadVersion => "bad-version",
            ScanStop::OutOfBounds => "out-of-bounds",
            ScanStop::BadCrc => "bad-crc",
            ScanStop::UndecodableOp => "undecodable-op",
            ScanStop::BadLap => "bad-lap",
            ScanStop::StaleLap => "stale-lap",
        }
    }
}

/// `scan_from_offset` の結果。
struct ScanOut {
    /// Commit で閉じた group の record 群。
    out: Vec<(Record, u64)>,
    max_lsn: u64,
    max_hlc: Hlc,
    /// 最後に読み切った Commit の直後 offset (1 つも無ければ start_offset)。
    committed_end: u64,
    /// 末尾の **未 commit** batch (Commit で閉じられなかった分)。
    tail: Vec<(Record, u64)>,
    stop: ScanStop,
    /// 読んだ Commit の直後の offset (古い順)。
    commit_ends: Vec<u64>,
}

/// #152: scan の内部表現 `(Record, 終端 offset)` から offset を落とす。
/// partial advance が要らない既存 caller 用。
#[inline]
fn strip_offsets(v: Vec<(Record, u64)>) -> Vec<Record> {
    v.into_iter().map(|(r, _)| r).collect()
}

/// WAL 本体。
/// `append_relayed` で受信レコードの header フィールド (HLC/author/署名) を
/// 引き継ぐためのバンドル。 LSN と payload は自分で発行/再計算する。
/// 受信 record (WireRecord) からそのまま埋めて渡す想定。
#[derive(Clone, Copy)]
pub struct RelayedHeader {
    pub hlc: Hlc,
    pub author: PeerId,
    pub signature: [u8; 64],
    pub pubkey_fp: [u8; 8],
}

/// 排他 advisory lock 解放用 RAII guard。 drop で unlock する。
/// `std::fs::File::lock` は unix で flock、 Windows で LockFileEx に落ちる
/// (Rust 1.89 で安定化)。 素の `libc::flock` は Windows に fd 自体が無く使えない。
/// #280: std が flock を持たない target (Android/bionic) は `crate::filelock`
/// が libc の flock に落とす。 unlock も同じ経路を通す (std の `unlock` も
/// 同じ target で `Unsupported` を返すため、 そのままでは **一度取った lock が
/// 永久に解放されず、 別プロセスの append が無限に待つ**)。
#[cfg(not(target_arch = "wasm32"))]
struct OpLogLockGuard<'a> {
    /// None = 開いている間の lock ([`OpLog::lock_session`]) を持っているので、 取りも外しもしない。
    file: Option<&'a File>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for OpLogLockGuard<'_> {
    fn drop(&mut self) {
        if let Some(file) = self.file {
            let _ = crate::filelock::unlock(file);
        }
    }
}

pub struct OpLog {
    #[cfg(not(target_arch = "wasm32"))]
    _file: File,
    #[cfg(not(target_arch = "wasm32"))]
    mmap: MmapMut,
    #[cfg(target_arch = "wasm32")]
    buf: Vec<u8>,
    /// 区画 2 つと採番の欄 (1 本の cache line)
    hot: Hot,
    /// 2 区画の形か (v4)。 v3 → v4 で立てる (`append_lock` の下)
    two: std::sync::atomic::AtomicBool,
    /// 次に区画に置く周 (畳む / 切り替えで移る時に取る)。 `append_lock` の下で進める
    next_lap: std::sync::atomic::AtomicU32,
    /// 区画を切り替えた回数 (観測用、 [`OpLog::switches`])
    switches: AtomicU64,
    /// この OpLog を持つ peer の id(header には書かず Engine から設定)。
    peer_id: std::sync::atomic::AtomicU32,
    /// ed25519 鍵ペア。set_keypair で設定。None なら署名は zeros。
    keypair: std::sync::RwLock<Option<std::sync::Arc<crate::keys::Keypair>>>,
    /// `keypair` が Some か (鍵を持たない時は、 append のたびに RwLock を読まない、 #506)。
    signing: std::sync::atomic::AtomicBool,
    /// #75: 同一プロセス内 append の直列化。 flock は open file description
    /// 単位のため、 同じ File を共有するスレッド間では排他にならない (2 本目の
    /// LOCK_EX が「既保持の変換」として即成功する)。 head 採番 (read-modify-
    /// write) と `next_hlc` はこの lock の下でのみ安全。 cross-process 排他は
    /// 従来通り flock が担う。
    ///
    /// std の `Mutex` は macOS では pthread の mutex で、 書き手を並べると取り合うたびに、 待つのも起こすのも system call
    /// だった (書き手 8 本の sample で、 書き手の時間の 5〜9 割、 #506)。 parking_lot の `Mutex` と同じ手順 (少し回って待ってから寝て、
    /// 外す時は寝ている待ち手が居る時だけ起こす) で、 try_lock が CAS 1 回の lock ([`crate::wal_lock`]、 まとめ書きの入口が使う)。
    append_lock: LockLine,
    /// [`OpLog::lock_session`] で、 開いている間ずっと flock を持っている。 立っている間の append は record ごとに
    /// flock を取らない (取ると外す時に、 開いている間の lock ごと外れる: flock は開いた file ごとの lock で数えない)。
    /// `append_lock` の下で立て、 append も `append_lock` の下で見る。
    session_locked: std::sync::atomic::AtomicBool,
    /// append 中の writer 数 (観測用)。 満杯で空きを待つ writer も数に入る。 畳むのとの直列化は `append_lock` で、
    /// この数は畳む条件に使わない (`try_reset_if`)。 まとめ書きの置き場のある thread の分は `pending_shards` に数える。
    pending_writes: std::sync::atomic::AtomicU32,
    /// `pending_writes` の、 置き場のある thread の分 (置き場の番号ごと、 その thread だけが足し引きする line)。 #506: 1 つの数を
    /// 書き手 8 本が append のたびに 2 回足し引きすると、 その line の取り合いでまとめ書きの 8 本が 5.5 → 3.4 M/s に落ちた。
    pending_shards: Box<[PendingShard]>,
    /// auto reset を許可するか。default false。
    /// true にすると consumer が head==checkpoint の tick で ring buffer を空に戻し
    /// 長期運用で WAL 容量を食い切らない動きになるが、audit/iter_committed/publish_since
    /// で読む前に消える race があるので opt-in。
    auto_reset: std::sync::atomic::AtomicBool,
    /// #268: テスト用 fault injection。 残り回数ぶん Commit の append を満杯として
    /// 失敗させる。 0 (既定) で何もしない。 `fail_next_commits` の doc を参照。
    fail_next_commits: std::sync::atomic::AtomicU32,
    /// group を閉じない record が残す、 閉じの Commit の分の空き (`alloc`)。 普段は `REC_HEADER_SIZE`。 test だけが 0 にする
    /// ([`OpLog::keep_commit_room_for_test`])。
    commit_room: std::sync::atomic::AtomicU64,
    /// 畳んだ (ring を頭に戻した) 印を、 次の record を書く前に書き出すか ([`OpLog::set_sync_on_reset`])。
    sync_on_reset: std::sync::atomic::AtomicBool,
    /// #57: append に失敗して WAL に載らなかった record の (author, その author の落ちた HLC の max)
    /// (Commit は除く)。 engine が [`OpLog::take_dropped`] で取り出し、 その author の配布履歴に穴が
    /// あったことを puller に知らせる (history floor を上げて bootstrap に回す)。
    dropped: std::sync::Mutex<Vec<(PeerId, Hlc)>>,
    /// #451 (段階 1): 自分 (`peer_id`) の落ちた record を覚えた回数 (`dropped` の lock の下で上げる)。
    dropped_gen: std::sync::atomic::AtomicU64,
    /// `quiet_without_mark` が WAL を読んで自分の中身のある record が無かった author + 1 (0 = まだ読んでいない)。 前の process が
    /// 残した record は増えず、 この process の分は `own_data` / `dropped_gen` で見るので、 一度無ければ読み直さない
    quiet_scanned: std::sync::atomic::AtomicU64,
    /// #178: scan が読み飛ばした record の累計 (知らない版が書いた [`op_type::SKIPPABLE`] の op。 同じ record を読み直すと
    /// また数える)。 [`OpLog::skipped_ops`]。
    skipped_ops: AtomicU64,
    /// #608: record ごとの周の印の大きさ: v3 / v4 の file は [`LAP_TRAILER`]、 v2 (最初に畳むまで) は 0。 `append_lock` の下で変える。
    trailer: std::sync::atomic::AtomicUsize,
    /// #388: append が満杯にぶつかった時に呼ぶ待ち手 (engine が consumer に畳ませて、 空くまで待つ)。 true を返したら
    /// 1 回だけ書き直す。 None なら今までどおりすぐ落とす。
    room_waiter: std::sync::RwLock<Option<RoomWaiter>>,
    /// #391: append が載った後に呼ぶ (engine が consumer を起こす — 書き出しの tick を回すため)。
    append_hook: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>>,
    /// #506 まとめ書き: `append_lock` を取れなかった書き手が頼みを置く場所 (書き手の thread ごとに 1 つ、 [`combine_slot_index`])。
    /// lock を持つ書き手が全部を見て積む ([`OpLog::serve_slots`])。
    combine_slots: Box<[CombineSlot]>,
    /// 頼みを置いたことのある置き場の番号の最大 + 1 と、 頼みが置かれているかもしれない印 ([`CombineHint`])。
    combine: CombineHint,
    /// 置き場の頼みとして積んだ record の数。 `append_lock` の下でだけ進める ([`OpLog::combined_appends`])。
    combined: AtomicU64,
}

/// #388: [`OpLog::set_room_waiter`] の待ち手。 引数は載せたい byte 数、 戻り値は空いたか。 append_lock を持たずに呼ぶ。
pub type RoomWaiter = std::sync::Arc<dyn Fn(u64) -> bool + Send + Sync>;

unsafe impl Send for OpLog {}
unsafe impl Sync for OpLog {}

// テスト専用: 次の `append_inner` 呼び出しを panic させるフラグ (issue #58② 検証用)。
// thread-local なので並行テストでも干渉しない。 release build には残らない。
#[cfg(test)]
thread_local! {
    static FAULT_INJECT_APPEND_PANIC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// test 用: この thread が record ごとの flock を実際に取った回数 ([`OpLog::lock_session`] の test)。
    static FLOCK_TAKEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// test 用: この thread が lock を持って置き場の頼みを積む時、 次の 1 つを積む前に panic する (#506)。
    static FAULT_INJECT_SERVE_PANIC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// #506 まとめ書き: 頼みの置き場の数。 置き場は書き手の thread ごとに決まる ([`combine_slot_index`]) ので、 番号がこれを越えた
/// thread は頼まずに lock を待って自分で積む。
const COMBINE_SLOTS: usize = 64;
const SLOT_EMPTY: u32 = 0;
const SLOT_PENDING: u32 = 1;
const SLOT_DONE: u32 = 2;
/// 1 回 lock を持つ間に置き場を見回る回数の上限 (積む書き手が自分の書き込みに戻れるように)。
const APPEND_SERVE_ROUNDS: usize = 2;
/// 頼みを置いた書き手が、 積まれるのを回って待つ回数 (この間 64 回ごとに、 lock が空いていれば取りに行く)。 越えたら lock を寝て
/// 待ち、 取れたら置かれた頼み (自分の分を含む) を積む。
const APPEND_REQ_SPINS: u32 = 1 << 12;

/// まとめ書きの頼み 1 つの置き場 (cache line 1 本 = 128 B)。 頼む書き手が中身を書いて `state` を PENDING にし、 lock を持つ書き手が
/// 積んで結果を置いて DONE にし、 頼んだ書き手が結果を取って EMPTY に戻す。 頼みと結果が同じ line なので、 頼み 1 つで core を渡る
/// line は行きと帰りの 1 本ずつ。 置き場の持ち主は 1 つの thread だけ (番号は同時に生きている thread の間で重ならない) なので、
/// 頼む側に取り合いは無い (10-17 の試作は頼みを 1 本の list の先頭に CAS で積み、 そこを全員が取り合った)。
#[repr(C, align(128))]
struct CombineSlot {
    state: std::sync::atomic::AtomicU32,
    req: std::cell::UnsafeCell<std::mem::MaybeUninit<SlotReq<'static>>>,
    result: std::cell::UnsafeCell<Option<io::Result<(u64, Hlc)>>>,
}

const _: () = assert!(std::mem::size_of::<CombineSlot>() == 128);

impl CombineSlot {
    fn new_slots() -> Box<[CombineSlot]> {
        (0..COMBINE_SLOTS)
            .map(|_| CombineSlot {
                state: std::sync::atomic::AtomicU32::new(SLOT_EMPTY),
                req: std::cell::UnsafeCell::new(std::mem::MaybeUninit::uninit()),
                result: std::cell::UnsafeCell::new(None),
            })
            .collect()
    }
}

/// まとめ書きの置き場を見るかの手がかり (cache line 1 本、 書かれるのはまれ)。
#[repr(align(128))]
struct CombineHint {
    /// 頼みを置いたことのある置き場の番号の最大 + 1 (lock を持つ書き手はここまで見る)。 置く書き手が上げるだけで下げない。
    hw: std::sync::atomic::AtomicU32,
    /// 頼みが置かれているかもしれない。 頼んだ書き手が (倒れていれば) 立て、 見回って何も無かった持ち手が倒す。 倒れている間、 lock を
    /// 持つ書き手は置き場を読まない: 一度でも頼みが置かれた後に毎回 `hw` 個の置き場を見ると、 書き手 1 本の append が遅くなった (#506、
    /// 書き手 1 本でも裏の consumer と lock がぶつかれば頼む)。 倒すのと立てるのが入れ違って頼みが印なしで残っても、 頼んだ書き手は
    /// 回って待つ間に自分で lock を取って積む (止まらない)。
    wanted: std::sync::atomic::AtomicBool,
}

/// 1 record の頼み (`append_lock` の下で積む中身)。 置き場には `'static` として置く: 頼んだ書き手は DONE を見るまで戻らない。
struct SlotReq<'a> {
    op: Op<'a>,
    payload_size: usize,
    /// group を閉じる record か (= 閉じの Commit の分の空きを残さなくてよい、 `alloc`)
    closes_group: bool,
    /// 中継の header (大きい) と事前採番の HLC は、 頼んだ書き手の stack を指す
    relay: Option<&'a RelayedHeader>,
    hlc_override: Option<&'a Hlc>,
}

/// `pending_writes` の thread ごとの分 (cache line 1 本)。
#[repr(align(128))]
struct PendingShard(std::sync::atomic::AtomicU32);

/// lock を持つ書き手が置き場の頼みを積む間の印。 積む途中で panic したら、 その頼みを失敗で返す (頼んだ書き手が待ち続けないように)。
struct ServeGuard<'a>(&'a CombineSlot);

impl ServeGuard<'_> {
    fn finish(self, out: io::Result<(u64, Hlc)>) {
        let slot = self.0;
        std::mem::forget(self);
        // SAFETY: PENDING の置き場の結果は、 lock を持つ書き手 1 本だけが書く (頼んだ書き手は DONE を見るまで読まない)
        unsafe { *slot.result.get() = Some(out) };
        // Release: 頼んだ書き手が結果を見る。 この後は置き場に触らない
        slot.state.store(SLOT_DONE, Ordering::Release);
    }
}

impl Drop for ServeGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: `finish` と同じ
        unsafe { *self.0.result.get() = Some(Err(io::Error::other("WAL append panicked in the writer that appended this record"))) };
        self.0.state.store(SLOT_DONE, Ordering::Release);
    }
}

/// #506: この thread の、 まとめ書きの置き場の番号。 thread が終わると番号を返し、 後に始まった thread が使い回す (同時に生きている
/// thread の間で番号は重ならない = 置き場の持ち主は 1 つ)。 thread の終わりの片付けの最中 (thread-local がもう無い) は None。
#[cfg(not(target_arch = "wasm32"))]
fn combine_slot_index() -> Option<usize> {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    static FREE: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());
    struct Index(u32);
    impl Drop for Index {
        fn drop(&mut self) {
            FREE.lock().unwrap_or_else(|e| e.into_inner()).push(self.0);
        }
    }
    thread_local! {
        static MINE: Index = Index(
            FREE.lock().unwrap_or_else(|e| e.into_inner()).pop().unwrap_or_else(|| NEXT.fetch_add(1, Ordering::Relaxed)),
        );
    }
    MINE.try_with(|i| i.0 as usize).ok()
}

/// `pending_writes` を RAII で減算するガード。
///
/// issue #58: 旧コードは `fetch_add` → `append_inner` → `fetch_sub` を直列で
/// 並べていたため、 `append_inner` が panic すると `fetch_sub` が skip され
/// counter が +1 のまま残り、 `try_reset`(当時は `pending_writes == 0` 条件)が
/// 永久に発火しなくなった。 drop で必ず減算することで panic 経路でも均衡する。
/// (今は畳む条件に使わない。 観測用 [`OpLog::pending_writes`] の数を合わせるため)
struct PendingGuard<'a> {
    counter: &'a std::sync::atomic::AtomicU32,
    n: u32,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.n, Ordering::AcqRel);
    }
}

impl OpLog {
    /// `pending_writes` を `n` 増やし、 drop 時に同量減らす RAII ガードを返す。
    fn pending_guard(&self, n: u32) -> PendingGuard<'_> {
        #[cfg(not(target_arch = "wasm32"))]
        let counter = match combine_slot_index().filter(|&i| i < COMBINE_SLOTS) {
            Some(i) => &self.pending_shards[i].0,
            None => &self.pending_writes,
        };
        #[cfg(target_arch = "wasm32")]
        let counter = &self.pending_writes;
        counter.fetch_add(n, Ordering::AcqRel);
        PendingGuard { counter, n }
    }

    /// oplog 容量到達時の `OutOfMemory` エラーを生成する。
    ///
    /// issue #57: caller(engine の tie/untie/delete 経路)はこの Err を
    /// `let _ =` で握り潰すため、 append 失敗が **silent な op 欠落**になっていた。
    /// 少なくとも検知だけは可能にするため、 エラー生成の単一地点で警告を
    /// emit する。 0.8.15 の persist warning と同じく **1 秒 1 行**に
    /// rate-limit してターミナルを潰さない。 完全な伝播 / 修復経路は別 issue。
    fn wal_full_err(&self) -> io::Error {
        io::Error::new(io::ErrorKind::OutOfMemory, "WAL full — consumer reset behind")
    }

    /// #388: 満杯で載らなかった (待っても空かなかった) ことを 1 秒 1 行で警告する。
    fn warn_wal_full(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            use std::sync::atomic::AtomicU64;
            static LAST_WARN_MS: AtomicU64 = AtomicU64::new(0);
            if let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                let now_ms = now.as_millis() as u64;
                let last = LAST_WARN_MS.load(Ordering::Relaxed);
                if now_ms.saturating_sub(last) >= 1000
                    && LAST_WARN_MS
                        .compare_exchange(last, now_ms, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                {
                    eprintln!(
                        "enchudb oplog: WAL full (capacity={} bytes) — append dropped, \
                         op NOT recorded to stream; advance checkpoint or enable auto_reset \
                         (rate-limited to 1/s)",
                        self.capacity()
                    );
                }
            }
        }
    }

    /// #391: append が載るたびに呼ぶ処理を置く (1 回だけ、 2 回目以降は無視)。 append_lock を持たずに呼ぶ。
    pub fn set_append_hook(&self, hook: Box<dyn Fn() + Send + Sync>) {
        let _ = self.append_hook.set(hook);
    }

    #[inline]
    fn appended(&self) {
        if let Some(h) = self.append_hook.get() {
            h();
        }
    }

    /// #388: 満杯の待ち手を置く (None で外す)。
    pub fn set_room_waiter(&self, waiter: Option<RoomWaiter>) {
        *self.room_waiter.write().unwrap_or_else(|p| p.into_inner()) = waiter;
    }

    /// #388: `size` byte の append が `err` で載らなかった後に呼ぶ。 書き直してよければ true (呼び手は書き直し、 その間に
    /// 他の書き手が空きを埋めて載らなければまた呼ぶ、 #552)。
    ///
    /// - 満杯 (`OutOfMemory`) でない失敗 (fault injection など): 何もせず false
    /// - もう空いている (載らなかった後に他の thread が畳んだ): true。 前は入口で空きを見て 「満杯でない失敗」 と読み、
    ///   畳めているのに落としていた (書き手が多いと、 満杯にぶつかってから空きを見るまでの間に畳まれる)
    /// - 空いていない: 待ち手に頼む。 待ち手が空けたら true、 空かなければ警告を出して false
    fn wait_for_room(&self, err: &io::Error, size: u64) -> bool {
        if err.kind() != io::ErrorKind::OutOfMemory {
            return false;
        }
        // #553: 空の ring にも入らない record は、 畳んでも載らないので待たない (前は上限の 5 秒まで consumer を起こし続けた)
        if self.larger_than_ring(size) {
            return false;
        }
        if self.free_bytes() >= size {
            return true;
        }
        let waiter = self.room_waiter.read().unwrap_or_else(|p| p.into_inner()).clone();
        if waiter.is_some_and(|w| w(size)) {
            return true;
        }
        self.warn_wal_full();
        false
    }

    /// #553: `size` byte (record と閉じの Commit の分) が空の区画にも入らないか (2 区画の形は 1 区画の大きさで見る)。
    fn larger_than_ring(&self, size: u64) -> bool {
        size > self.hot.halves[0].size().saturating_sub(HEADER_SIZE as u64)
    }

    /// 新規 WAL ファイル作成。capacity は初期サイズ(bytes)。 2 区画にできる大きさ (`2 * HALF_ALIGN` 以上) なら v4、 できなければ
    /// v3 の 1 区画。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create(path: &Path, capacity: usize) -> io::Result<Self> {
        crate::io_point("oplog.create")?;
        if capacity as u64 >= u32::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("oplog capacity {capacity} must be below 4 GiB (the header keeps it in 32 bits)"),
            ));
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;
        crate::io_point("oplog.set_len")?;
        file.set_len(capacity as u64)?;
        crate::io_point("oplog.mmap")?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let layout = fresh_layout(&mut mmap, capacity as u64);
        let wal = Self::assemble(file, mmap, layout);
        // #415: header をすぐ書き出す。 書き出さないと、 consumer の最初の fsync (100 ms) より前に落ちた時に header が
        // 0 のまま残り、 `bad WAL magic` で開けない (開く側は 0 のままの header を作り直す: `was_never_written`)
        wal.fsync()?;
        Ok(wal)
    }

    /// #415: header が一度もディスクに届いていない oplog か (作った直後、 最初の書き出しより前に落ちた)。 そういう
    /// file には record も無い (record は header と一緒に書き出す) ので、 開く側は無いものとして作り直してよい。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn was_never_written(path: &Path) -> io::Result<bool> {
        use std::io::Read;
        let mut head = [0u8; HEADER_SIZE];
        let mut f = std::fs::File::open(path)?;
        let mut n = 0;
        while n < head.len() {
            match f.read(&mut head[n..])? {
                0 => return Ok(true), // header より短い
                k => n += k,
            }
        }
        Ok(head.iter().all(|&b| b == 0))
    }

    /// 既存 WAL を開く。 v4 (2 区画)、 v3 (1 区画、 周の印あり)、 v2 (周の印なし。 最初に畳む時に v3 → v4 になる) を読む。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &Path) -> io::Result<Self> {
        crate::io_point("oplog.open")?;
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let layout = read_layout(&mut mmap)?;
        Ok(Self::assemble(file, mmap, layout))
    }

    /// 作る / 開く時に決めた形から組み立てる。
    fn assemble(
        #[cfg(not(target_arch = "wasm32"))] file: File,
        #[cfg(not(target_arch = "wasm32"))] mmap: MmapMut,
        #[cfg(target_arch = "wasm32")] buf: Vec<u8>,
        l: Layout,
    ) -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            _file: file,
            #[cfg(not(target_arch = "wasm32"))]
            mmap,
            #[cfg(target_arch = "wasm32")]
            buf,
            hot: Hot {
                halves: l.halves,
                next_lsn: AtomicU64::new(1),
                active: std::sync::atomic::AtomicUsize::new(l.active),
                hlc_state: parking_lot::Mutex::new((0, 0)),
                own_data: AtomicU64::new(0),
            },
            two: std::sync::atomic::AtomicBool::new(l.two),
            next_lap: std::sync::atomic::AtomicU32::new(l.next_lap),
            switches: AtomicU64::new(0),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            keypair: std::sync::RwLock::new(None),
            signing: std::sync::atomic::AtomicBool::new(false),
            append_lock: LockLine(crate::wal_lock::WalLock::new(())),
            combine_slots: CombineSlot::new_slots(),
            combine: CombineHint { hw: std::sync::atomic::AtomicU32::new(0), wanted: std::sync::atomic::AtomicBool::new(false) },
            combined: AtomicU64::new(0),
            session_locked: std::sync::atomic::AtomicBool::new(false),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
            pending_shards: (0..COMBINE_SLOTS).map(|_| PendingShard(std::sync::atomic::AtomicU32::new(0))).collect(),
            auto_reset: std::sync::atomic::AtomicBool::new(false),
            fail_next_commits: std::sync::atomic::AtomicU32::new(0),
            commit_room: std::sync::atomic::AtomicU64::new((REC_HEADER_SIZE + l.trailer) as u64),
            sync_on_reset: std::sync::atomic::AtomicBool::new(false),
            room_waiter: std::sync::RwLock::new(None),
            append_hook: std::sync::OnceLock::new(),
            dropped: std::sync::Mutex::new(Vec::new()),
            dropped_gen: std::sync::atomic::AtomicU64::new(0),
            quiet_scanned: std::sync::atomic::AtomicU64::new(0),
            skipped_ops: AtomicU64::new(0),
            trailer: std::sync::atomic::AtomicUsize::new(l.trailer),
        }
    }

    /// #178: scan が読み飛ばした record の累計 (知らない版が書いた 「読み飛ばしてよい」 op、 [`op_type::SKIPPABLE`])。
    /// 0 が常態。 0 でなければ、 この binary より新しい版の peer の record を読んでいる。
    pub fn skipped_ops(&self) -> u64 {
        self.skipped_ops.load(Ordering::Relaxed)
    }

    /// この WAL を所有する peer の id を設定(Engine 初期化時に 1 回)。
    pub fn set_peer_id(&self, peer: PeerId) {
        self.peer_id.store(peer, Ordering::Release);
    }

    /// Phase C: 署名鍵を設定。None で署名 off(slot は zeros で埋める)。
    pub fn set_keypair(&self, kp: Option<std::sync::Arc<crate::keys::Keypair>>) {
        let some = kp.is_some();
        let mut k = self.keypair.write().unwrap();
        *k = kp;
        self.signing.store(some, Ordering::Release);
    }

    /// 現在の peer id。
    #[inline]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id.load(Ordering::Acquire)
    }

    /// 今の head (次の append の位置、 [`wal_pos`])。
    #[inline]
    pub fn head(&self) -> u64 {
        let h = &self.hot.halves[self.hot.active.load(Ordering::Acquire)];
        wal_pos(h.lap(), h.head.load(Ordering::Acquire))
    }

    /// 書き終えた record の終わり ([`wal_pos`])。 `head` は場所を取った所で、 書いている最中の record を含みうる (append は
    /// 場所を取ってから中身を書く)。 書き出し (fsync) の前に覆う位置を取るのはこちら (#663: head で取ると、 中身を写す前の
    /// record まで checkpoint が覆い、 changefeed の記録に書き出していない record の番号を振った)。
    #[inline]
    pub fn written_head(&self) -> u64 {
        let h = &self.hot.halves[self.hot.active.load(Ordering::Acquire)];
        wal_pos(h.lap(), h.written.load(Ordering::Acquire))
    }

    /// 今の checkpoint の位置 (ここまで本体に書き出した、 [`wal_pos`])。 古い区画に書き出していない record があればその区画の
    /// checkpoint、 無ければ今の区画の checkpoint。
    pub fn checkpoint(&self) -> u64 {
        let ai = self.hot.active.load(Ordering::Acquire);
        let (a, o) = (&self.hot.halves[ai], &self.hot.halves[ai ^ 1]);
        if o.size() > 0 && !o.is_empty() {
            let cp = o.checkpoint.load(Ordering::Acquire);
            if cp < o.head.load(Ordering::Acquire) {
                return wal_pos(o.lap(), cp);
            }
        }
        wal_pos(a.lap(), a.checkpoint.load(Ordering::Acquire))
    }

    /// #608: 今の区画の周 (畳む / 切り替えで移るたびに新しい数)。 v2 の file (まだ一度も畳んでいない) は 0。
    #[inline]
    pub fn lap(&self) -> u32 {
        self.hot.halves[self.hot.active.load(Ordering::Acquire)].lap()
    }

    /// file の版 (header): 2 区画の v4、 1 区画の v3、 周の印の無い v2 (最初に畳む時に v3 → v4 になる)。
    pub fn file_version(&self) -> u32 {
        if self.trailer.load(Ordering::Acquire) == 0 {
            OPLOG_FILE_VERSION_V2
        } else if self.two.load(Ordering::Acquire) {
            OPLOG_FILE_VERSION
        } else {
            OPLOG_FILE_VERSION_V3
        }
    }

    /// 2 区画の形 (v4) か。
    pub fn two_halves(&self) -> bool {
        self.two.load(Ordering::Acquire)
    }

    /// record が 1 件も無い (どの区画も空)。
    pub fn is_empty(&self) -> bool {
        self.hot.halves.iter().all(|h| h.size() == 0 || h.is_empty())
    }

    /// 区画を切り替えた回数 (観測用)。
    pub fn switches(&self) -> u64 {
        self.switches.load(Ordering::Relaxed)
    }

    /// 今この WAL に載せた時の 1 record の大きさ (header + payload + 周の印。 v2 の file は印なし)。
    pub fn record_size_of(&self, payload_size: usize) -> usize {
        REC_HEADER_SIZE + payload_size + self.trailer.load(Ordering::Acquire)
    }

    /// 今書き足している区画 (`append_lock` の下で読む)。
    #[inline]
    fn active_half(&self) -> &Half {
        &self.hot.halves[self.hot.active.load(Ordering::Relaxed)]
    }

    /// 区画 `h` の **採番に使う head**。 native では mmap 上の永続値を真実とし、 process-local
    /// atomic と突き合わせる — 別 process が直前に append していると `head` は
    /// 古いため。
    ///
    /// #268: `append_dead` / `free_bytes` (= 「もう書けないか」 の観測) もこの値で
    /// 判定する。 in-memory head だけを見ていた旧実装は、 **実際の append は満杯で
    /// 失敗し続けているのに 「まだ余裕がある」 と答える**ことがあり、 その乖離は
    /// `wal_fold_safe` の死区間例外 (= brick の唯一の出口) まで塞いでいた。
    #[inline]
    fn alloc_head(&self, h: &Half) -> u64 {
        self.alloc_head_at(h, h.base())
    }

    /// `alloc_head` の、 区画の header の位置 `b` を呼び手が読んだ版 (append の道は 1 回だけ読む)。
    #[inline]
    fn alloc_head_at(&self, h: &Half, b: usize) -> u64 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let on_disk = u64::from_le_bytes(self.mmap_slice()[b + 8..b + 16].try_into().unwrap());
            on_disk.max(h.head.load(Ordering::Acquire))
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = b;
            h.head.load(Ordering::Acquire)
        }
    }

    /// 区画 `h` (header は `b`) の header の head と `written` を `end` に (record を書き終えた後、 `append_lock` の下)。
    #[inline]
    fn put_head(&self, h: &Half, b: usize, end: u64) {
        self.mmap_mut_slice()[b + 8..b + 16].copy_from_slice(&end.to_le_bytes());
        h.written.store(end, Ordering::Release);
    }

    /// `size` bytes を予約して (区画, 区画の中の開始 offset) を返す。 **`append_lock` 保持下で呼ぶこと**。 今の区画に載らなければ、
    /// もう一方へ移ってから取る (`switch_locked`)。
    ///
    /// group を閉じない record (`closes_group` = false) は、 閉じの Commit 1 つ分 (`REC_HEADER_SIZE`) の空きを残してしか
    /// 場所を取らない。 満杯でも、 開いている group を必ず閉じられる (engine はそれを閉じ、 bridge に渡してから畳む。 区画を
    /// 切り替える時もこの空きで閉じる)。 前は Commit も入らない所まで埋まると開いた group を閉じられず、 sync の DB では畳む時に
    /// 孤児の group として配りから落としていた (#511。 #450 の死区間の例外。 WAL 256 KiB に書き手 1 本で 20 万回書くと、 満杯の
    /// 後のほぼ全部)。
    ///
    /// head は record 本体を書く **前** に bump される (scan は Commit で閉じた
    /// group しか `out` に載せないので、 書き込み途中の領域が読まれることはない)。
    ///
    /// 戻り値は区画の番号 (上位 32 bit) と区画の中の offset (下位、 4 GiB 未満) を 1 語に (`Slot` のような struct を返すと register に
    /// 載らず、 書き手 1 本の append が 2.5 ns 遅くなった)。 区画は [`OpLog::slot`] で引く。
    #[inline]
    fn alloc(&self, size: u64, closes_group: bool) -> io::Result<u64> {
        let i = self.hot.active.load(Ordering::Relaxed);
        match self.alloc_off(&self.hot.halves[i], size, closes_group) {
            Some(off) => Ok(((i as u64) << 32) | off),
            None => self.alloc_after_switch(size, closes_group),
        }
    }

    #[cold]
    #[inline(never)]
    fn alloc_after_switch(&self, size: u64, closes_group: bool) -> io::Result<u64> {
        // 空の区画にも入らない record では移らない (移ると閉じの Commit だけが古い区画に残り、 次に移れなくなる)
        let need = size + if closes_group { 0 } else { self.commit_room.load(Ordering::Relaxed) };
        if !self.larger_than_ring(need) && self.switch_locked() {
            let i = self.hot.active.load(Ordering::Relaxed);
            if let Some(off) = self.alloc_off(&self.hot.halves[i], size, closes_group) {
                return Ok(((i as u64) << 32) | off);
            }
        }
        Err(self.wal_full_err())
    }

    /// `alloc` の戻り値から場所を引く (`append_lock` の下、 区画の位置と周は Relaxed で読む)。
    #[inline]
    fn slot(&self, at: u64) -> Slot<'_> {
        let half = &self.hot.halves[(at >> 32) as usize];
        Slot { half, base: half.base.load(Ordering::Relaxed) as usize, off: at & u32::MAX as u64, lap: half.lap.load(Ordering::Relaxed) }
    }

    /// 区画 `h` に `size` bytes を取れれば offset。 区画の位置・大きさ・周は `append_lock` の下でしか変えないので、 ここ (lock の下)
    /// では Relaxed で読む。
    #[inline]
    fn alloc_off(&self, h: &Half, size: u64, closes_group: bool) -> Option<u64> {
        let cur = self.alloc_head_at(h, h.base.load(Ordering::Relaxed) as usize);
        let new = cur + size;
        let need = if closes_group { new } else { new + self.commit_room.load(Ordering::Relaxed) };
        if need > h.size.load(Ordering::Relaxed) as u64 {
            return None;
        }
        h.head.store(new, Ordering::Release);
        Some(cur)
    }

    /// 区画 `h` に `size` bytes を取れればその場所。 区画の位置・大きさ・周は `append_lock` の下でしか変えないので、 ここ (lock の下)
    /// では Relaxed で読む (append 1 回に Acquire の読みを足すと、 書き手 1 本の append が 36 → 38.5 ns に遅くなった)。
    fn alloc_in<'a>(&self, h: &'a Half, size: u64, closes_group: bool) -> Option<Slot<'a>> {
        let off = self.alloc_off(h, size, closes_group)?;
        Some(Slot { half: h, base: h.base.load(Ordering::Relaxed) as usize, off, lap: h.lap.load(Ordering::Relaxed) })
    }

    /// #663: 区画 `h` の header の page を書き出す (`append_lock` の下、 移る時)。 区画の頭は [`HALF_ALIGN`] にそろっているので、
    /// そこから [`HALF_ALIGN`] を書く (page が 4 KiB でも 16 KiB でも header の page を含む)。 失敗しても移る (次の fsync が書く)。
    fn sync_half_header(&self, h: &Half) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let b = h.base();
            let len = (HALF_ALIGN as usize).min(self.mmap.len() - b);
            #[cfg(all(feature = "crashsim", unix))]
            let sim = crate::crashsim::active()
                .then(|| crate::crashsim::copy_for_sync(crate::crashsim::Copied::Oplog, b as u64, &self.mmap[b..b + len]));
            if crate::io_point("oplog.switch_header").is_ok() && self.mmap.flush_range(b, len).is_ok() {
                #[cfg(all(feature = "crashsim", unix))]
                if let Some(copy) = sim {
                    crate::crashsim::data_synced(&self._file, copy);
                }
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = h;
    }

    /// 今の区画が埋まった時、 もう一方が空なら移る (`append_lock` の下)。 開いている group は今の区画に閉じの Commit を置いて
    /// 閉じる (その分の空きは `alloc` が残している)。 もう一方の周が今の区画より新しくなければ新しい周を置く (畳んだ時に置いて
    /// あればそのまま)。 移った後の古い区画には誰も書き足さない (書き出して畳むのは engine の consumer)。
    fn switch_locked(&self) -> bool {
        if !self.two.load(Ordering::Acquire) {
            return false;
        }
        let ai = self.hot.active.load(Ordering::Relaxed);
        let (a, o) = (&self.hot.halves[ai], &self.hot.halves[ai ^ 1]);
        if !o.is_empty() || self.alloc_head(o) > HEADER_SIZE as u64 {
            return false;
        }
        self.close_group_for_switch(a);
        // #663: 移った先に書く前に、 古い区画の header (最後の head) を書き出す。 新しい区画の record が読める像では、 古い区画の
        // header が最後の head を持つ = 古い区画の record が欠けていれば読みは古い区画の途中で止まり、 新しい区画を読まない。 書き
        // 出さないと、 header だけ前の head のまま古い区画の終わりの record が届かず、 新しい区画の record は届いた像で、 読みが古い
        // 区画を前の head で読み終えて新しい区画へ進み、 間を飛ばして当てた (再生と changefeed の記録の続きが穴の後から)
        self.sync_half_header(a);
        if !lap_newer(o.lap(), a.lap()) {
            let lap = self.take_next_lap();
            let b = o.base();
            self.mmap_mut_slice()[b + OFF_LAP..b + OFF_LAP + 4].copy_from_slice(&lap.to_le_bytes());
            o.lap.store(lap, Ordering::Release);
        }
        self.hot.active.store(ai ^ 1, Ordering::Release);
        self.switches.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// 切り替えの前に、 今の区画に Commit を置いて開いている group を閉じる (`append_lock` の下)。 入らなければ (閉じの Commit の
    /// 分の空きを残さない test の形) 置かない — 読み手は group を区画をまたいで読む。
    fn close_group_for_switch(&self, h: &Half) {
        // #268 の test の fault injection (Commit の append が失敗する) は、 この Commit にも効かせる (閉じられない group は区画を
        // またいで続く)
        if self.take_commit_fault() {
            return;
        }
        let trailer = self.trailer.load(Ordering::Acquire);
        let size = (REC_HEADER_SIZE + trailer) as u64;
        let Some(Slot { base, off, lap, .. }) = self.alloc_in(h, size, true) else { return };
        let lsn = self.hot.next_lsn.load(Ordering::Relaxed);
        self.hot.next_lsn.store(lsn + 1, Ordering::Release);
        let hlc = self.next_hlc();
        let author = hlc.peer;
        let op_byte = op_type::COMMIT;
        let crc = new_record_crc(op_byte, 0, lsn, hlc, author, &[]);
        let keypair = if self.signing.load(Ordering::Acquire) { self.keypair.read().unwrap().clone() } else { None };
        let (signature, pubkey_fp) = match keypair {
            Some(kp) => (kp.sign(&signed_payload(op_byte, 0, lsn, hlc, author, crc, &[])), kp.pubkey_fp()),
            None => (ZERO_SIGNATURE, ZERO_PUBKEY_FP),
        };
        let at = base + off as usize;
        let mmap = self.mmap_mut_slice();
        let header = &mut mmap[at..at + REC_HEADER_SIZE];
        header[OFF_MAGIC..OFF_MAGIC + 2].copy_from_slice(REC_MAGIC);
        header[OFF_VERSION] = REC_VERSION;
        header[OFF_OP] = op_byte;
        header[OFF_LEN..OFF_LEN + 4].copy_from_slice(&0u32.to_le_bytes());
        header[OFF_LSN..OFF_LSN + 8].copy_from_slice(&lsn.to_le_bytes());
        header[OFF_HLC_WALL..OFF_HLC_WALL + 8].copy_from_slice(&hlc.wall.to_le_bytes());
        header[OFF_HLC_LOGICAL..OFF_HLC_LOGICAL + 4].copy_from_slice(&hlc.logical.to_le_bytes());
        header[OFF_HLC_PEER..OFF_HLC_PEER + 4].copy_from_slice(&hlc.peer.to_le_bytes());
        header[OFF_AUTHOR_PEER..OFF_AUTHOR_PEER + 4].copy_from_slice(&author.to_le_bytes());
        header[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
        header[OFF_SIGNATURE..OFF_SIGNATURE + 64].copy_from_slice(&signature);
        header[OFF_PUBKEY_FP..OFF_PUBKEY_FP + 8].copy_from_slice(&pubkey_fp);
        if trailer != 0 {
            let t = at + REC_HEADER_SIZE;
            write_lap_trailer(&mut mmap[t..t + trailer], lap);
        }
        self.put_head(h, base, off + size);
    }

    /// 次に置く周を取る (`append_lock` の下)。
    fn take_next_lap(&self) -> u32 {
        let l = self.next_lap.load(Ordering::Relaxed);
        self.next_lap.store(next_lap(l), Ordering::Relaxed);
        l
    }

    /// 次に発行する LSN。
    #[inline]
    pub fn next_lsn(&self) -> u64 { self.hot.next_lsn.load(Ordering::Acquire) }

    /// 次の HLC を払い出す。
    /// wall が前回より進んでいれば logical=0 にリセット、同じ/戻っていれば logical+1。
    fn next_hlc(&self) -> Hlc {
        let peer = self.peer_id.load(Ordering::Acquire);
        let now = current_wall_ms();
        let mut st = self.hot.hlc_state.lock();
        let (last, logical) = *st;
        *st = if now > last { (now, 0) } else { (last, logical.wrapping_add(1)) };
        Hlc { wall: st.0, logical: st.1, peer }
    }

    /// request17-A3: HLC を **1 個だけ先に払い出す**。 採番したら clock は進むので、
    /// 呼んだ側は必ずその HLC で record を書くこと (`append_at_hlc` /
    /// `append_many_with_hlcs`)。
    ///
    /// async write 経路 (`tie_async`) は「op の適用」と「WAL への append」が別 queue で
    /// 非同期に進むため、 append の戻り値を待っていては適用時点で版数を書けない。
    /// 事前採番して **cell の version column と WAL record に同じ HLC を載せる**ための入口。
    /// 両者がずれると、 peer 間で「自分が持つ版数」と「相手に配った版数」が食い違い、
    /// その隙間に入った並行 write が peer ごとに別々の勝者を選ぶ (= divergence)。
    pub fn mint_hlc(&self) -> Hlc {
        self.next_hlc()
    }

    /// op を append。新しいレコードの LSN を返す。
    ///
    /// writer は append 中 `pending_writes` を +1、 完了時 -1 (観測用。 畳むのとの直列化は `append_lock`)。
    pub fn append(&self, op: Op<'_>) -> io::Result<u64> {
        self.append_with_hlc(op).map(|(lsn, _)| lsn)
    }

    /// request17-A3: `append` + **払い出した HLC も返す**版。 同期 write 経路が
    /// 「WAL に載せた HLC」をそのまま cell の version column に書けるようにする。
    pub fn append_with_hlc(&self, op: Op<'_>) -> io::Result<(u64, Hlc)> {
        let payload_size = op.payload_size();
        let record_size = record_size_hint(payload_size);

        let _guard = self.pending_guard(1);
        self.append_inner(op, payload_size, record_size, None, None)
    }

    /// request17-A3: `mint_hlc` で事前採番した HLC を載せて append する。 署名は
    /// 自鍵で行う (= 他 peer 由来の record をそのまま中継する `append_relayed` とは違い、
    /// これは **自分の write**)。
    ///
    /// 注意: 事前採番と append の間に別の write が挟まると、 WAL 上で HLC の並びが
    /// LSN の並びと一致しなくなる。 LWW は HLC の全順序だけを見る (LSN は cursor 用) ので
    /// 判定には影響しない。
    pub fn append_at_hlc(&self, op: Op<'_>, hlc: Hlc) -> io::Result<u64> {
        let payload_size = op.payload_size();
        let record_size = record_size_hint(payload_size);

        let _guard = self.pending_guard(1);
        self.append_inner(op, payload_size, record_size, None, Some(hlc))
            .map(|(lsn, _)| lsn)
    }

    /// 複数 record を **1 回の flock サイクル** で連続 append。
    /// per-record で `append` を回すと flock(LOCK_EX) syscall が record 数ぶん走るので、
    /// consumer thread が queue を drain して呼ぶことで flock コストを償却できる。
    /// 戻り値は各 record の LSN (順序対応)。
    pub fn append_many(&self, records: &[OwnedOp]) -> io::Result<Vec<u64>> {
        self.append_many_impl(records, None)
    }

    /// request17-A3: `append_many` の **HLC 事前採番**版。 `hlcs[i]` が `records[i]` の
    /// HLC になる (採番し直さない)。 async write 経路が、 適用時に `mint_hlc` した HLC を
    /// cell の version column と WAL record の両方へ載せるために使う。
    ///
    /// # Panics
    /// - `records.len() != hlcs.len()`
    pub fn append_many_with_hlcs(&self, records: &[OwnedOp], hlcs: &[Hlc]) -> io::Result<Vec<u64>> {
        assert_eq!(
            records.len(), hlcs.len(),
            "append_many_with_hlcs: records {} と hlcs {} の数が違う",
            records.len(), hlcs.len(),
        );
        self.append_many_impl(records, Some(hlcs))
    }

    fn append_many_impl(&self, records: &[OwnedOp], hlcs: Option<&[Hlc]>) -> io::Result<Vec<u64>> {
        if records.is_empty() { return Ok(Vec::new()); }
        // 見積もり (周の印込み)。 本当の大きさは `append_many_inner` が `append_lock` の下で決める (v2 の file は印なし)
        let sizes: Vec<usize> = records.iter()
            .map(|r| record_size_hint(r.as_op().payload_size()))
            .collect();
        let total: usize = sizes.iter().sum();

        let _guard = self.pending_guard(records.len() as u32);
        let mut r = self.append_many_inner(records, &sizes, total, hlcs);
        let reserve = if records.last().is_some_and(|r| matches!(r.as_op(), Op::Commit)) { 0 } else { self.commit_room.load(Ordering::Relaxed) };
        while r.as_ref().is_err_and(|e| self.wait_for_room(e, total as u64 + reserve)) {
            r = self.append_many_inner(records, &sizes, total, hlcs);
        }
        if r.is_ok() {
            self.appended();
        }
        // #57: 自分の write の束 (Commit しか無い束は record を運ばない)。 HLC は事前採番の max
        if r.is_err() {
            let mut carried = records.iter().enumerate().filter(|(_, x)| !matches!(x.as_op(), Op::Commit));
            match hlcs {
                Some(h) => {
                    if let Some(max) = carried.map(|(i, _)| h[i]).max() {
                        self.note_dropped(self.peer_id(), max);
                    }
                }
                None => {
                    if carried.next().is_some() {
                        self.note_dropped_minting(self.peer_id(), None);
                    }
                }
            }
        }
        r
    }

    fn append_many_inner(
        &self,
        records: &[OwnedOp],
        _sizes_hint: &[usize],
        _total_hint: usize,
        // request17-A3: Some なら採番せず与えられた HLC を載せる (index 対応)。
        hlcs: Option<&[Hlc]>,
    ) -> io::Result<Vec<u64>> {
        // #75: 同一プロセス内の直列化 (flock は同一 fd 共有スレッド間で no-op)
        let _in_proc = self.append_lock.lock();
        #[cfg(not(target_arch = "wasm32"))]
        let _lock = self.flock_exclusive()?;

        // #608: 本当の大きさは lock の下で (周の印の有無は畳む時に変わる)
        let trailer = self.trailer.load(Ordering::Acquire);
        let sizes: Vec<usize> = records.iter().map(|r| REC_HEADER_SIZE + r.as_op().payload_size() + trailer).collect();
        let total: usize = sizes.iter().sum();

        // 一括 allocate (束は 1 つの区画に置く。 周は場所を取った区画の周 = 切り替えた後なら移った先)
        let closes = records.last().is_some_and(|r| matches!(r.as_op(), Op::Commit));
        let Slot { half, base, off: start_offset, lap } = self.slot(self.alloc(total as u64, closes)?);
        let base = base as u64;

        let mut lsns = Vec::with_capacity(records.len());
        let keypair = self.keypair.read().unwrap().clone();
        let mut offset = base + start_offset;
        let mmap = self.mmap_mut_slice();

        for (i, (rec, &record_size)) in records.iter().zip(sizes.iter()).enumerate() {
            let op = rec.as_op();
            let payload_size = record_size - REC_HEADER_SIZE - trailer;
            let lsn = self.hot.next_lsn.fetch_add(1, Ordering::AcqRel);
            // request17-A3: 事前採番された HLC があればそれを使う (採番し直すと
            // cell の version column と record の HLC がずれる)。
            let hlc = match hlcs {
                Some(h) => h[i],
                None => self.next_hlc(),
            };
            let author_peer = hlc.peer;
            if !matches!(op, Op::Commit) {
                self.hot.own_data.fetch_add(1, Ordering::AcqRel);
            }

            let op_byte = op.op_byte();
            let payload_offset = offset as usize + REC_HEADER_SIZE;
            op.write_payload(&mut mmap[payload_offset..payload_offset + payload_size]);
            let crc = new_record_crc(
                op_byte, payload_size as u32, lsn, hlc, author_peer,
                &mmap[payload_offset..payload_offset + payload_size],
            );

            let (signature, pubkey_fp) = match &keypair {
                Some(kp) => {
                    let msg = signed_payload(
                        op_byte, payload_size as u32, lsn, hlc,
                        author_peer, crc,
                        &mmap[payload_offset..payload_offset + payload_size],
                    );
                    (kp.sign(&msg), kp.pubkey_fp())
                }
                None => (ZERO_SIGNATURE, ZERO_PUBKEY_FP),
            };

            let header = &mut mmap[offset as usize..offset as usize + REC_HEADER_SIZE];
            header[OFF_MAGIC..OFF_MAGIC + 2].copy_from_slice(REC_MAGIC);
            header[OFF_VERSION] = REC_VERSION;
            header[OFF_OP] = op_byte;
            header[OFF_LEN..OFF_LEN + 4].copy_from_slice(&(payload_size as u32).to_le_bytes());
            header[OFF_LSN..OFF_LSN + 8].copy_from_slice(&lsn.to_le_bytes());
            header[OFF_HLC_WALL..OFF_HLC_WALL + 8].copy_from_slice(&hlc.wall.to_le_bytes());
            header[OFF_HLC_LOGICAL..OFF_HLC_LOGICAL + 4].copy_from_slice(&hlc.logical.to_le_bytes());
            header[OFF_HLC_PEER..OFF_HLC_PEER + 4].copy_from_slice(&hlc.peer.to_le_bytes());
            header[OFF_AUTHOR_PEER..OFF_AUTHOR_PEER + 4].copy_from_slice(&author_peer.to_le_bytes());
            header[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
            header[OFF_SIGNATURE..OFF_SIGNATURE + 64].copy_from_slice(&signature);
            header[OFF_PUBKEY_FP..OFF_PUBKEY_FP + 8].copy_from_slice(&pubkey_fp);
            if trailer != 0 {
                let t = payload_offset + payload_size;
                write_lap_trailer(&mut mmap[t..t + trailer], lap);
            }

            lsns.push(lsn);
            offset += record_size as u64;
        }

        // 区画の header の head を一括更新 (batch 全体が書き終わったあと)
        self.put_head(half, base as usize, start_offset + total as u64);

        Ok(lsns)
    }

    /// 受信した他 peer 由来の WAL op を **元の HLC / author / 署名のまま** 自分の WAL に
    /// 記録する gossip 用 API。 通常の `append` が `next_hlc()` で新規 HLC を発行する
    /// のに対し、 こちらは引数の `origin_hlc` / `origin_author` をそのまま乗せる。
    /// LSN だけは自分で発行 (local-monotonic に保つため)。 CRC は payload から再計算。
    ///
    /// これで「Mac が Android Delete を受信 → 自分の WAL に Android author/HLC のまま
    /// Delete record を記録 → 次の publish で他 peer も pull できる」 hop が成立し、
    /// relay 側の (peer, hlc) dedupe が同じ record の二度送りをカットする (= ループ防止)。
    ///
    /// 自プロセスで HLC が後退しないよう、 受信 HLC でローカル HLC clock も merge する。
    pub fn append_relayed(&self, op: Op<'_>, header: RelayedHeader) -> io::Result<u64> {
        let payload_size = op.payload_size();
        let record_size = record_size_hint(payload_size);
        let _guard = self.pending_guard(1);
        let result = self.append_inner(op, payload_size, record_size, Some(header), None);
        // ローカル HLC clock を受信 HLC で merge (後退防止)
        self.merge_external_hlc(header.hlc);
        result.map(|(lsn, _)| lsn)
    }

    /// #209: 受信 record を **byte 単位でそのまま** WAL に載せる relay append。
    ///
    /// `append_relayed` (op を再 encode) は署名対象領域 (fixed header 40B +
    /// payload、**LSN を含む**) を自分の値で書き直すため、 author の署名と一致
    /// しなくなる。 署名を素通しするには author が署名した bytes をそのまま格納
    /// するしかない — `sb` は `WireRecord::signed_bytes` (= 元 record の
    /// fixed header ‖ payload)、 disk 上の record は
    /// `sb[0..40] ‖ signature(64) ‖ pubkey_fp(8) ‖ sb[40..]` として復元される。
    ///
    /// LSN も author のものが残る (自分の `next_lsn` は消費しない)。 WAL 内 LSN の
    /// 単調性は relayed record では成立しないが、 recover / bridge / audit は
    /// 位置ベース走査で LSN 単調性に依存しない。
    ///
    /// 検証: magic / version / len 整合 / payload CRC。 壊れた bytes は Err。
    pub fn append_relayed_verbatim(
        &self,
        sb: &[u8],
        signature: &[u8; 64],
        pubkey_fp: &[u8; 8],
    ) -> io::Result<u64> {
        let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
        if sb.len() < SIGNED_PAYLOAD_HEADER_SIZE {
            return Err(bad("relayed signed_bytes too short"));
        }
        if &sb[0..2] != REC_MAGIC {
            return Err(bad("relayed signed_bytes: bad magic"));
        }
        if !known_version(sb[OFF_VERSION]) {
            return Err(bad("relayed signed_bytes: unsupported record version"));
        }
        let payload_len = u32::from_le_bytes(sb[4..8].try_into().unwrap()) as usize;
        if sb.len() != SIGNED_PAYLOAD_HEADER_SIZE + payload_len {
            return Err(bad("relayed signed_bytes: len field mismatch"));
        }
        let crc_stored = u32::from_le_bytes(sb[36..40].try_into().unwrap());
        if signed_bytes_crc(sb) != crc_stored {
            return Err(bad("relayed signed_bytes: crc mismatch"));
        }
        let record_size = record_size_hint(payload_len);
        let mut r = self.append_verbatim_checked(sb, signature, pubkey_fp, record_size);
        while r.as_ref().is_err_and(|e| self.wait_for_room(e, record_size as u64 + self.commit_room.load(Ordering::Relaxed))) {
            r = self.append_verbatim_checked(sb, signature, pubkey_fp, record_size);
        }
        if r.is_ok() {
            self.appended();
        }
        // #57: 検証を通った record が載らなかった = 中継の配布履歴に穴 (壊れた bytes の拒否は数えない)
        if r.is_err() {
            let hlc = Hlc {
                wall: u64::from_le_bytes(sb[16..24].try_into().unwrap()),
                logical: u32::from_le_bytes(sb[24..28].try_into().unwrap()),
                peer: u32::from_le_bytes(sb[28..32].try_into().unwrap()),
            };
            self.note_dropped(u32::from_le_bytes(sb[32..36].try_into().unwrap()), hlc);
        }
        r
    }

    fn append_verbatim_checked(
        &self,
        sb: &[u8],
        signature: &[u8; 64],
        pubkey_fp: &[u8; 8],
        _record_size_hint: usize,
    ) -> io::Result<u64> {
        let _guard = self.pending_guard(1);
        let _in_proc = self.append_lock.lock();
        #[cfg(not(target_arch = "wasm32"))]
        let _lock = self.flock_exclusive()?;

        // #608: 本当の大きさは lock の下で (周の印の有無は畳む時に変わる)
        let trailer = self.trailer.load(Ordering::Acquire);
        let payload_len = sb.len() - SIGNED_PAYLOAD_HEADER_SIZE;
        let record_size = REC_HEADER_SIZE + payload_len + trailer;
        // head 採番は append_inner と同じ規則 (mmap 上の永続値を真実とする)。
        let Slot { half, base, off: offset, lap } = self.slot(self.alloc(record_size as u64, false)?);

        let mmap = self.mmap_mut_slice();
        let off = base + offset as usize;
        mmap[off..off + SIGNED_PAYLOAD_HEADER_SIZE]
            .copy_from_slice(&sb[0..SIGNED_PAYLOAD_HEADER_SIZE]);
        mmap[off + OFF_SIGNATURE..off + OFF_SIGNATURE + 64].copy_from_slice(signature);
        mmap[off + OFF_PUBKEY_FP..off + OFF_PUBKEY_FP + 8].copy_from_slice(pubkey_fp);
        mmap[off + REC_HEADER_SIZE..off + REC_HEADER_SIZE + payload_len]
            .copy_from_slice(&sb[SIGNED_PAYLOAD_HEADER_SIZE..]);
        if trailer != 0 {
            let t = off + REC_HEADER_SIZE + payload_len;
            write_lap_trailer(&mut mmap[t..t + trailer], lap);
        }
        // 区画の header の head も更新
        self.put_head(half, base, offset + record_size as u64);

        // ローカル HLC clock を受信 HLC で merge (後退防止)
        let hlc = Hlc {
            wall: u64::from_le_bytes(sb[16..24].try_into().unwrap()),
            logical: u32::from_le_bytes(sb[24..28].try_into().unwrap()),
            peer: u32::from_le_bytes(sb[28..32].try_into().unwrap()),
        };
        self.merge_external_hlc(hlc);
        Ok(u64::from_le_bytes(sb[8..16].try_into().unwrap()))
    }

    /// request17 step 5: **受信 HLC でローカル clock を進める** (HLC の merge 規則)。
    ///
    /// これが無いと、 相手の wall clock が先行している間ずっと「自分が次に採番する
    /// HLC < 既に適用した remote の版数」になり、 版数を storage に置いた途端に
    /// **自分のローカル write が自分の DB で負ける**。 適用した record は必ず
    /// これを通すこと (`append_relayed` は内部で呼ぶので二重呼び出し不要)。
    pub fn observe_hlc(&self, recv: Hlc) {
        self.merge_external_hlc(recv);
    }

    fn merge_external_hlc(&self, recv: Hlc) {
        let mut st = self.hot.hlc_state.lock();
        if (recv.wall, recv.logical) > *st {
            *st = (recv.wall, recv.logical);
        }
    }

    fn append_inner(
        &self,
        op: Op<'_>,
        payload_size: usize,
        record_size: usize,
        relay: Option<RelayedHeader>,
        // request17-A3: Some なら採番せず与えられた HLC を載せる (`mint_hlc` 済み)。
        hlc_override: Option<Hlc>,
    ) -> io::Result<(u64, Hlc)> {
        self.append_inner_noting(op, payload_size, record_size, relay, hlc_override)
            .map_err(|(e, _)| e)
    }

    /// `append_inner` の本体。 載らなかった時は、 その record の HLC (採番前なら今採番) も返す。
    fn append_inner_noting(
        &self,
        op: Op<'_>,
        payload_size: usize,
        record_size: usize,
        relay: Option<RelayedHeader>,
        hlc_override: Option<Hlc>,
    ) -> Result<(u64, Hlc), (io::Error, Hlc)> {
        let is_commit = matches!(op, Op::Commit);
        let mut r = self.append_inner_raw(&op, payload_size, record_size, relay.as_ref(), hlc_override, is_commit);
        let reserve = if is_commit { 0 } else { self.commit_room.load(Ordering::Relaxed) };
        // 空いた後に他の書き手が先に埋めたら、 また待つ (#552: 前は 1 回しか書き直さず、 畳めているのに落とした)
        while r.as_ref().is_err_and(|e| self.wait_for_room(e, record_size as u64 + reserve)) {
            r = self.append_inner_raw(&op, payload_size, record_size, relay.as_ref(), hlc_override, is_commit);
        }
        if r.is_ok() {
            self.appended();
        }
        r.map_err(|e| {
            // #57: 落ちた record の author と HLC を覚える。 Commit は record を運ばない (落ちても group が
            // 閉じるのが遅れるだけ、 #268) ので覚えない (採番もしない)。
            if is_commit {
                return (e, Hlc::ZERO);
            }
            // #553: WAL より大きい record は落ちた record として覚えない (呼び手が断る。 覚えると、 書いていない write の分まで
            // 相手に取り直させる)。 種類を `InvalidInput` に分ける (満杯 = `OutOfMemory` は畳めば載る)
            if e.kind() == io::ErrorKind::OutOfMemory && self.larger_than_ring(record_size as u64 + reserve) {
                return (io::Error::new(io::ErrorKind::InvalidInput, "oplog: the record is larger than the WAL"), Hlc::ZERO);
            }
            let author = relay.map(|h| h.author).unwrap_or_else(|| self.peer_id());
            let hlc = self.note_dropped_minting(author, relay.map(|h| h.hlc).or(hlc_override));
            (e, hlc)
        })
    }

    /// #57: `append_with_hlc` と同じ。 載らなかった時は Err に **落ちた record の HLC** を添える
    /// (採番して [`OpLog::take_dropped`] に覚えた値)。 同期 write はそれを cell の版数に使う —
    /// 覚えるより後に採番し直すと、 その間に engine の bridge が floor を決めて、 落ちた write の
    /// 版数が floor を越える。
    pub fn append_or_dropped(&self, op: Op<'_>) -> Result<(u64, Hlc), (io::Error, Hlc)> {
        let payload_size = op.payload_size();
        let record_size = record_size_hint(payload_size);
        let _guard = self.pending_guard(1);
        self.append_inner_noting(op, payload_size, record_size, None, None)
    }

    /// op と中継の header は参照で受ける (#506: 値で受けると、 取り合いの道に参照を渡すだけで、 呼び手が append のたびに写しを
    /// 作って渡すようになり、 書き手 1 本の append が 36 → 42 ns に遅くなった)。
    fn append_inner_raw(
        &self,
        op: &Op<'_>,
        payload_size: usize,
        // lock の外の見積もり (空きの待ちに使う)。 本当の大きさは lock の下で決める (#608)
        _record_size_hint: usize,
        relay: Option<&RelayedHeader>,
        hlc_override: Option<Hlc>,
        // group を閉じる record か (= 閉じの Commit の分の空きを残さなくてよい、 `alloc`)
        closes_group: bool,
    ) -> io::Result<(u64, Hlc)> {
        // テスト専用 fault injection: pending_writes の RAII ガード (issue #58②) が
        // panic-unwind 経路でも均衡することを deterministic に検証するため、 ここで
        // panic させられるようにする。 release build には一切残らない。
        #[cfg(test)]
        FAULT_INJECT_APPEND_PANIC.with(|c| {
            if c.get() {
                c.set(false);
                panic!("fault-injected append panic (issue #58② test)");
            }
        });

        // #268: テスト用 fault injection (既定では atomic load すら踏まない)。
        if matches!(op, Op::Commit) && self.take_commit_fault() {
            // 容量の失敗 (`OutOfMemory`) とは種類を分ける: 呼び出し側は `OutOfMemory` を 「満杯 = 畳めば回復」
            // と読む (#407)。 これは満杯でない WAL で Commit が打てない場合を作るためのもの
            return Err(io::Error::other("injected commit failure (test)"));
        }

        // #75: 同一プロセス内は append_lock、 プロセス間は flock で直列化。
        // flock は open file description 単位なので、 同じ File を共有する
        // スレッド間では排他にならない — append_lock が必須。
        //
        // lock を取れた時は自分の分を積み、 置き場の頼みも積んでから離す。 取れない時はまとめ書き (#506、 `lock_or_request`): 頼みを
        // 置いて待ち、 他の書き手が積めばその結果を返す。 自分で lock を取れたら頼みを下げて、 ここから自分で積む。
        //
        // 頼み (`SlotReq`) は取り合った時だけ作る (1 本の道で頼みを struct に詰めて参照で渡すと 36 → 49 ns)
        let _in_proc = match self.append_lock.try_lock() {
            Some(g) => g,
            None => match self.lock_or_request(op, payload_size, closes_group, relay, hlc_override) {
                Ok(g) => g,
                Err(appended) => return appended,
            },
        };
        let r = self.append_locked(op, payload_size, closes_group, relay, hlc_override);
        self.serve_slots();
        r
    }

    /// `append_lock` を取れなかった書き手 (まとめ書き、 #506): 頼みを自分の置き場に置き、 lock を持つ書き手に積んでもらう。 積まれたら
    /// その結果を `Err` で、 待つ間に自分で lock を取れたら頼みを下げて lock を `Ok` で返す (呼び手が自分で積み、 置き場の頼みも積む)。
    /// 置き場の無い thread (番号が `COMBINE_SLOTS` 以上 / thread の終わりの片付けの最中) は lock を待つ。
    ///
    /// 書き手 8 本が record ごとに lock を受け渡すと、 受け渡し 1 回 (lock と WAL の状態が core を渡る) が積む仕事より重く、 合計は
    /// 1 本の 0.35 倍だった (M4 Max、 書き手ごとに 110 ns の仕事を挟んだ append)。 lock の種類 (MCS の queue lock も) でも、 lock の
    /// 中で書く line を減らしても、 受け渡しが record ごとである限り変わらない (`WAL_MULTI_WRITER_PLAN.md`)。 持つ書き手がまとめて
    /// 積めば、 受け渡しは束ごとで、 頼み 1 つの手間は置き場の line の行き帰りだけになる。 積む順 = LSN・HLC の順はそのまま (lock を
    /// 持つ書き手が積んだ順に振る)。 同じ cell の書き手は行の lock を持ったまま頼むので、 WAL の順と当てる順もそろったまま (#503)。
    #[cold]
    #[inline(never)]
    fn lock_or_request<'a>(
        &'a self,
        op: &Op<'_>,
        payload_size: usize,
        closes_group: bool,
        relay: Option<&RelayedHeader>,
        hlc_override: Option<Hlc>,
    ) -> Result<WalGuard<'a>, io::Result<(u64, Hlc)>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(i) = combine_slot_index().filter(|&i| i < COMBINE_SLOTS) {
            return self.append_by_request(
                i,
                SlotReq { op: op.clone(), payload_size, closes_group, relay, hlc_override: hlc_override.as_ref() },
            );
        }
        let _ = (op, payload_size, closes_group, relay, hlc_override);
        Ok(self.append_lock.lock())
    }

    /// まとめ書き: 頼みを置き場 `i` (この thread の) に置き、 lock を持つ書き手に積んでもらう ([`OpLog::lock_or_request`])。
    #[cfg(not(target_arch = "wasm32"))]
    fn append_by_request(&self, i: usize, req: SlotReq<'_>) -> Result<WalGuard<'_>, io::Result<(u64, Hlc)>> {
        let slot = &self.combine_slots[i];
        debug_assert_eq!(slot.state.load(Ordering::Relaxed), SLOT_EMPTY);
        // SAFETY: 置き場の持ち主はこの thread だけで、 EMPTY の間は誰も読まない。 頼みの借用は、 DONE を見るか頼みを下げるまで
        // この関数から戻らないので 'static に延ばしてよい (積む書き手は DONE の後に触らない)
        unsafe { (*slot.req.get()).write(std::mem::transmute::<SlotReq<'_>, SlotReq<'static>>(req)) };
        if i as u32 >= self.combine.hw.load(Ordering::Relaxed) {
            self.combine.hw.fetch_max(i as u32 + 1, Ordering::Relaxed);
        }
        // Release: 積む書き手が頼みの中身を見る
        slot.state.store(SLOT_PENDING, Ordering::Release);
        // 印は倒れている時だけ書く (取り合いの続く間は立ったままなので、 頼むたびに line を書かない)。 Release: 印を見た持ち手は
        // 上の PENDING も見る
        if !self.combine.wanted.load(Ordering::Relaxed) {
            self.combine.wanted.store(true, Ordering::Release);
        }
        // 積まれた結果を取って置き場を空に戻す。 Acquire: 積んだ書き手が書いた結果を見る
        let take = || {
            (slot.state.load(Ordering::Acquire) == SLOT_DONE).then(|| {
                // SAFETY: DONE の後は、 積んだ書き手は置き場に触らない
                let r = unsafe { (*slot.result.get()).take() }.expect("まとめ書きの結果が無い");
                slot.state.store(SLOT_EMPTY, Ordering::Relaxed);
                r
            })
        };
        let mut spins = 0u32;
        loop {
            if let Some(r) = take() {
                return Err(r);
            }
            let lock = if spins < APPEND_REQ_SPINS {
                spins += 1;
                if spins.is_multiple_of(64) && !self.append_lock.is_locked() {
                    self.append_lock.try_lock()
                } else {
                    std::hint::spin_loop();
                    None
                }
            } else {
                Some(self.append_lock.lock())
            };
            if let Some(g) = lock {
                // lock を持った: 前の持ち手が積んでいれば結果を返す (持っている間に他の頼みも積む)。 まだなら頼みを下げて、 呼び手が
                // 自分で積む (lock を持つのは自分だけなので、 PENDING のままの置き場を読む書き手はいない)
                if let Some(r) = take() {
                    self.serve_slots();
                    return Err(r);
                }
                slot.state.store(SLOT_EMPTY, Ordering::Relaxed);
                return Ok(g);
            }
        }
    }

    /// `append_lock` を持って呼ぶ。 置き場の頼みを積んで結果を返す。 頼みが置かれている印が倒れていれば (書き手 1 本) 1 回読むだけ。
    #[inline(always)]
    fn serve_slots(&self) {
        if self.combine.wanted.load(Ordering::Acquire) {
            self.serve_slots_from((self.combine.hw.load(Ordering::Relaxed) as usize).min(COMBINE_SLOTS));
        }
    }

    /// `serve_slots` の本体: 置き場 `0..n` を見る。 先に全部の置き場の印を読んでから積む (印の読みが重なる)。
    #[inline(never)]
    fn serve_slots_from(&self, n: usize) {
        // 積んだ数は見回りの終わりにまとめて足す (record ごとに書くと、 頼む書き手が読む欄と line を取り合う)
        let mut served = 0u64;
        for _ in 0..APPEND_SERVE_ROUNDS {
            let mut pending = 0u64;
            for (i, s) in self.combine_slots[..n].iter().enumerate() {
                if s.state.load(Ordering::Relaxed) == SLOT_PENDING {
                    pending |= 1 << i;
                }
            }
            if pending == 0 {
                // 見回って何も無かった: 印を倒す (この後に置かれた頼みは、 置いた書き手が印を立て直す)
                self.combine.wanted.store(false, Ordering::Relaxed);
                break;
            }
            // Acquire: PENDING を読んだ置き場の頼みの中身 (頼んだ書き手の Release の前に書いた) を見る
            std::sync::atomic::fence(Ordering::Acquire);
            while pending != 0 {
                let i = pending.trailing_zeros() as usize;
                pending &= pending - 1;
                let slot = &self.combine_slots[i];
                let guard = ServeGuard(slot);
                #[cfg(test)]
                FAULT_INJECT_SERVE_PANIC.with(|c| {
                    if c.replace(false) {
                        panic!("fault-injected panic while appending a request (#506 test)");
                    }
                });
                // SAFETY: PENDING の頼みは、 DONE にするまで頼んだ書き手が書き換えない (その書き手は DONE を見るまで戻らない)
                let req = unsafe { (*slot.req.get()).assume_init_ref() };
                let out = self.append_locked(&req.op, req.payload_size, req.closes_group, req.relay, req.hlc_override.copied());
                served += 1;
                guard.finish(out);
            }
        }
        if served > 0 {
            self.combined.store(self.combined.load(Ordering::Relaxed) + served, Ordering::Relaxed);
        }
    }

    /// #506: lock を持つ書き手が、 他の書き手が置き場に置いた頼みとして積んだ record の数 (観測用。 頼んだ書き手が待つ間に自分で lock を
    /// 取った時は頼みを下げて自分で積むので数えない)。
    pub fn combined_appends(&self) -> u64 {
        self.combined.load(Ordering::Relaxed)
    }

    /// `append_lock` を持って呼ぶ。 1 record を積む。 呼び元 (1 本の道・置き場の頼み・置き場の無い thread) ごとに中へ入れる:
    /// 外の関数のままだと 1 本の append が 38 → 48 ns に遅くなった (#506)。
    #[inline(always)]
    fn append_locked(
        &self,
        op: &Op<'_>,
        payload_size: usize,
        closes_group: bool,
        relay: Option<&RelayedHeader>,
        hlc_override: Option<Hlc>,
    ) -> io::Result<(u64, Hlc)> {
        #[cfg(not(target_arch = "wasm32"))]
        let _lock = self.flock_exclusive()?;

        // #608: 本当の大きさは lock の下で (周の印の有無は畳む時に変わる)。 引数の `record_size` は lock の外の見積もり
        let trailer = self.trailer.load(Ordering::Acquire);
        let record_size = REC_HEADER_SIZE + payload_size + trailer;
        // ── head を mmap 上の値から読み直して採番 ──
        // 別 process が直前に append した場合、 区画の head (process-local atomic) は
        // 古い値を持ってるので、 lock 中に mmap 上の永続値を真実とする。
        // append_lock + flock の両方を保持しているので単純 store で OK。 周は場所を取った区画の周 (切り替えた後なら移った先)
        let Slot { half, base, off: offset, lap } = self.slot(self.alloc(record_size as u64, closes_group)?);
        let at = base + offset as usize;

        // append_lock の下でだけ進める (読み手は lock を取らずに読むので atomic のまま)
        let lsn = self.hot.next_lsn.load(Ordering::Relaxed);
        self.hot.next_lsn.store(lsn + 1, Ordering::Release);
        // relay 経路では受信 HLC/author/署名をそのまま乗せる (gossip 用)。
        // 通常経路では next_hlc + 自鍵で署名。
        let (hlc, author_peer) = match relay {
            Some(o) => (o.hlc, o.author),
            None => {
                let h = hlc_override.unwrap_or_else(|| self.next_hlc());
                let a = h.peer;
                if !matches!(op, Op::Commit) {
                    self.hot.own_data.fetch_add(1, Ordering::AcqRel);
                }
                (h, a)
            }
        };

        let op_byte = op.op_byte();
        let payload_offset = at + REC_HEADER_SIZE;
        let mmap = self.mmap_mut_slice();
        op.write_payload(&mut mmap[payload_offset..payload_offset + payload_size]);
        let crc = new_record_crc(
            op_byte, payload_size as u32, lsn, hlc, author_peer,
            &mmap[payload_offset..payload_offset + payload_size],
        );

        // Phase C: 鍵があれば署名。無ければ zeros。 relay 経路では元の署名を保持。
        let (signature, pubkey_fp) = match &relay {
            Some(o) => (o.signature, o.pubkey_fp),
            None => {
                let keypair = if self.signing.load(Ordering::Acquire) { self.keypair.read().unwrap().clone() } else { None };
                match keypair {
                    Some(kp) => {
                        let msg = signed_payload(
                            op_byte, payload_size as u32, lsn, hlc,
                            author_peer, crc,
                            &mmap[payload_offset..payload_offset + payload_size],
                        );
                        (kp.sign(&msg), kp.pubkey_fp())
                    }
                    None => (ZERO_SIGNATURE, ZERO_PUBKEY_FP),
                }
            }
        };

        let header = &mut mmap[at..at + REC_HEADER_SIZE];
        header[OFF_MAGIC..OFF_MAGIC + 2].copy_from_slice(REC_MAGIC);
        header[OFF_VERSION] = REC_VERSION;
        header[OFF_OP] = op_byte;
        header[OFF_LEN..OFF_LEN + 4].copy_from_slice(&(payload_size as u32).to_le_bytes());
        header[OFF_LSN..OFF_LSN + 8].copy_from_slice(&lsn.to_le_bytes());
        header[OFF_HLC_WALL..OFF_HLC_WALL + 8].copy_from_slice(&hlc.wall.to_le_bytes());
        header[OFF_HLC_LOGICAL..OFF_HLC_LOGICAL + 4].copy_from_slice(&hlc.logical.to_le_bytes());
        header[OFF_HLC_PEER..OFF_HLC_PEER + 4].copy_from_slice(&hlc.peer.to_le_bytes());
        header[OFF_AUTHOR_PEER..OFF_AUTHOR_PEER + 4].copy_from_slice(&author_peer.to_le_bytes());
        header[OFF_CRC..OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
        header[OFF_SIGNATURE..OFF_SIGNATURE + 64].copy_from_slice(&signature);
        header[OFF_PUBKEY_FP..OFF_PUBKEY_FP + 8].copy_from_slice(&pubkey_fp);
        if trailer != 0 {
            let t = payload_offset + payload_size;
            write_lap_trailer(&mut mmap[t..t + trailer], lap);
        }

        // 区画の header の head も更新
        self.put_head(half, base, offset + record_size as u64);

        Ok((lsn, hlc))
    }

    /// ring buffer reset。
    /// head == checkpoint のときのみ実行可能 (`try_reset_if` の doc)。
    ///
    /// fix: auto_reset ゲートを撤去。 元は「Syncer attached の engine は ring を
    /// reset させない」ために入れた gate だが、 0.8.0 で sync publish path が `_sync_ops`
    /// 経由になり oplog ring を直接読まなくなった (sync.rs 参照) ので gate の存在理由は
    /// 消えていた。 にもかかわらず default が false のままだったため production では
    /// try_reset が常時 no-op になり、 ring が一度も畳まれず 16MB を使い切ると WAL full
    /// で append が全 drop される状態に陥っていた (#57 でようやく可視化)。 reset 条件
    /// (head==checkpoint ⇒ body へ msync 済) を満たす領域は crash recovery
    /// にも sync にも不要なので、 無条件に畳んでよい。 `auto_reset` フラグは vestigial。
    pub fn try_reset(&self) -> bool {
        self.try_reset_if(|_| true)
    }

    /// `try_reset` の述語つき版。 畳める区画 (`fold_target_locked`) が 1 つあれば、 `fold_safe(through)` が true の時に畳む。
    /// `through` は、 読み手 (sync の bridge / changefeed) がそこまで読み終えていればその区画を畳んでよい位置 (その区画の
    /// head、 [`wal_pos`])。 `fold_safe` は **`append_lock` 保持中に、 fold の直前に**評価される。 1 回に畳むのは 1 区画。
    ///
    /// 呼び出し側が lock の外で 「畳んでよいか」 を判定してから `try_reset` を呼ぶと、
    /// 判定と fold の間に他 thread の append + `advance_checkpoint` が割り込める
    /// (check-then-act)。 その窓を通ると 「判定時は bridge 済みだったが fold 時には
    /// 未 bridge record が居る」 状態で ring を畳んでしまい、 その record は WAL ごと
    /// 消えて **sync から無言で欠落**する (engine の `wal_fold_safe` が防いでいる
    /// はずのもの)。 述語を lock 下で再評価することで窓を閉じる。
    ///
    /// `fold_safe` は `append_lock` を保持したまま呼ばれるので、 この OpLog の
    /// append 系 API を再入的に呼んではならない (`head` / `checkpoint` /
    /// `pending_writes` の読みと scan 系は lock を取らないので安全)。
    pub fn try_reset_if(&self, fold_safe: impl FnOnce(u64) -> bool) -> bool {
        // #77-M8: append_lock で append と直列化。 旧実装は pending_writes
        // チェックと head CAS の間に窓があり、 その間に進入した append の
        // record が reset で head 外に落ちて喪失 + stale record 復活が起きた。
        // append_lock 保持中は append_inner が進入できないため窓が閉じる。
        let _in_proc = self.append_lock.lock();
        // `pending_writes` は見ない (`checkpoint_and_reset` と同じ)。 全 append は `append_lock` の下で場所を取って
        // 書き終え、 head も進めるので、 ここで数に入っているのは lock を待っている append と、 満杯で空きを待っている
        // append (`wait_for_room`、 record はまだ載っていない) だけ。 前は見ていたので、 空きを待つ書き手がいる間は畳めず、
        // 空きを作れないまま書き手が諦めて record が落ちた (sync の DB で満杯になると、 その後の record がほぼ全部落ちた、 #511)。
        let Some((i, through)) = self.fold_target_locked() else { return false };
        // lock 下での再評価。 ここで false なら畳まない (= 未 bridge record を守る)。
        if !fold_safe(through) { return false; }
        self.fold_half_locked(i)
    }

    /// 今畳める区画があれば、 読み手がそこまで読み終えていればよい位置 (`try_reset_if` が `fold_safe` に渡す値)。 lock を取らない
    /// 見積もり (呼び手が lock の外で先に確かめる用、 本当の判定は `try_reset_if` の中)。
    pub fn fold_hint(&self) -> Option<u64> {
        self.fold_target_locked().map(|(_, through)| through)
    }

    /// 位置 `pos` ([`wal_pos`]) の周の区画がまだあるか。 無い = その区画は畳まれた後 (畳むと新しい周を置く) で、 その位置から
    /// 読むと残った区画を頭から読む。
    pub fn holds(&self, pos: u64) -> bool {
        let l = wal_pos_lap(pos);
        self.hot.halves.iter().any(|h| h.size() > 0 && h.lap() == l)
    }

    /// 畳める区画と、 読み手がそこまで読み終えていればよい位置 (`append_lock` の下)。
    ///
    /// - 古い区画 (今書き足していない方) に record があれば、 全部を本体に書き出してある (checkpoint == head) 時にその区画。
    ///   書き出していなければ何も畳まない (今の区画は古い区画が空になるまで畳まない)
    /// - 古い区画が空 (1 区画の形も) なら、 今の区画を全部書き出してある時に、 その場で (周を進めて頭から書き直す)
    fn fold_target_locked(&self) -> Option<(usize, u64)> {
        let ai = self.hot.active.load(Ordering::Relaxed);
        if self.two.load(Ordering::Acquire) {
            let o = &self.hot.halves[ai ^ 1];
            if !o.is_empty() {
                let head = o.head.load(Ordering::Acquire);
                return (o.checkpoint.load(Ordering::Acquire) == head).then(|| (ai ^ 1, wal_pos(o.lap(), head)));
            }
        }
        let a = &self.hot.halves[ai];
        let head = a.head.load(Ordering::Acquire);
        (head > HEADER_SIZE as u64 && a.checkpoint.load(Ordering::Acquire) == head).then(|| (ai, wal_pos(a.lap(), head)))
    }

    /// 区画 `i` を頭に戻す (`append_lock` を持った呼び手から、 その区画の head == checkpoint を確かめた後)。 新しい周を置くので、
    /// 前の周の record は読まれない。 古い区画を畳んだ後は、 次に切り替えで移る時に周を置き直さない (今の区画より新しい周)。
    /// 1 区画の形 (v3 / v2) は、 畳んだ後に 2 区画にできれば v4 にする (`split_locked`)。
    fn fold_half_locked(&self, i: usize) -> bool {
        let h = &self.hot.halves[i];
        let head = h.head.load(Ordering::Acquire);
        if head <= HEADER_SIZE as u64
            || h.head.compare_exchange(head, HEADER_SIZE as u64, Ordering::AcqRel, Ordering::Acquire).is_err()
        {
            return false;
        }
        let mmap = self.mmap_mut_slice();
        // #608: 周を進める。 header には head / checkpoint より先に書く: header の page の像がどの瞬間に撮られても
        // (周, head, checkpoint) は 「新しい周と 32」 か 「前の周と前の位置」 のどちらか。 「新しい周と前の位置」 は、 前の周の
        // record (全部適用済みで、 本体も書き出してある) を周の違いで読まないだけで、 何も失わない
        self.advance_lap_locked(mmap, h);
        h.written.store(HEADER_SIZE as u64, Ordering::Release);
        h.checkpoint.store(HEADER_SIZE as u64, Ordering::Release);
        let b = h.base();
        mmap[b + 8..b + 16].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        mmap[b + 16..b + 24].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        self.split_locked();
        if self.sync_on_reset.load(Ordering::Acquire) {
            // append_lock の下: 次の record が前の周の page を上書きする前に、 頭に戻した header を届ける
            let _ = self.fsync();
        }
        true
    }

    /// 畳んだ印 (head / checkpoint を頭に戻した header) を、 次の record を書く前に書き出す (append_lock の下で fsync 1 回。
    /// 畳む前に全部を書き出してあるので、 書くのは header の page だけ)。 既定は書き出さない: 電源が落ちると、 前の周の header
    /// のまま、 新しい周の page だけが OS に先に書かれた姿がありうる。 本体は前の周を当て終えているので困らないが、 WAL の中で
    /// 記録した record の続きを探す changefeed の記録 (engine の `changefeed.seg`) は、 前の周を途中までしか読めずに取り違える。
    pub fn set_sync_on_reset(&self, on: bool) {
        self.sync_on_reset.store(on, Ordering::Release);
    }

    /// #608: 区画 `h` に新しい周を置いて header に書く (`append_lock` の下、 区画が空の時)。 v2 の file はここで v3 になる
    /// (version → 3、 capacity を u32 に、 周、 以後の record に周の印)。 書く順は version → capacity → 周: v2 の
    /// 読み方では周の場所が capacity の上位 4 byte なので、 version を先に 3 にしないと、 途中の像を v2 として読んだ時に
    /// capacity が 4 GiB 増えて開けない (4 GiB 以上の v2 の file は開かない、 `read_layout`)。
    fn advance_lap_locked(&self, mmap: &mut [u8], h: &Half) {
        let lap = self.take_next_lap();
        let b = h.base();
        if self.trailer.load(Ordering::Acquire) == 0 {
            mmap[4..8].copy_from_slice(&OPLOG_FILE_VERSION_V3.to_le_bytes());
            mmap[OFF_CAPACITY..OFF_CAPACITY + 4].copy_from_slice(&(h.size() as u32).to_le_bytes());
            mmap[OFF_LAP..OFF_LAP + 4].copy_from_slice(&lap.to_le_bytes());
            h.lap.store(lap, Ordering::Release);
            self.trailer.store(LAP_TRAILER, Ordering::Release);
            // 閉じの Commit の分の空きも印の分だけ増える (test が 0 にしていればそのまま)
            let _ = self.commit_room.compare_exchange(
                REC_HEADER_SIZE as u64,
                (REC_HEADER_SIZE + LAP_TRAILER) as u64,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        } else {
            mmap[b + OFF_LAP..b + OFF_LAP + 4].copy_from_slice(&lap.to_le_bytes());
            h.lap.store(lap, Ordering::Release);
        }
    }

    /// v3 の 1 区画を v4 の 2 区画にする (`append_lock` の下、 区画を畳んだ直後 = 空の時)。 B の header を書いて fsync してから、
    /// A の capacity → version の順に書く: 途中のどの像も、 v3 (capacity が file 全体か半分、 空) か v4 として開ける。 fsync に
    /// 失敗したら 1 区画のまま (B の header は v3 の区画の中の空き)。
    fn split_locked(&self) {
        if self.two.load(Ordering::Acquire) || self.trailer.load(Ordering::Acquire) == 0 {
            return;
        }
        let (a, b) = (&self.hot.halves[0], &self.hot.halves[1]);
        let Some(hs) = half_size_for(a.size()) else { return };
        if !a.is_empty() {
            return;
        }
        let empty = HEADER_SIZE as u64;
        let mmap = self.mmap_mut_slice();
        write_half_header(mmap, hs as usize, OPLOG_FILE_VERSION, hs, empty, empty, 0);
        if self.fsync().is_err() {
            return;
        }
        mmap[OFF_CAPACITY..OFF_CAPACITY + 4].copy_from_slice(&(hs as u32).to_le_bytes());
        mmap[4..8].copy_from_slice(&OPLOG_FILE_VERSION.to_le_bytes());
        a.size.store(hs as u32, Ordering::Release);
        b.base.store(hs as u32, Ordering::Release);
        b.size.store(hs as u32, Ordering::Release);
        self.two.store(true, Ordering::Release);
    }

    /// #388: checkpoint を head まで進め、 畳めれば 1 区画畳む。 `sync(head)` は head までの record の効果を本体に書き出す
    /// (oplog の fsync + 本体の msync) 役で、 false なら何も進めない。 戻り値は (checkpoint を進めたか、 畳んだか)。 `fold_safe`
    /// は `try_reset_if` と同じ (lock の下で、 checkpoint を進めた後に呼ぶ)。 `sync` / `fold_safe` から この OpLog の append 系を
    /// 呼んではならない。
    ///
    /// - 2 区画の形: 書き出しの間も append を止めない。 古い区画には誰も書き足さないので、 書き出した後に古い区画を畳める
    ///   (今の区画に書き足した分は次の書き出しに入る)
    /// - 1 区画の形: append を止めたまま (`append_lock` を持ったまま) 書き出して畳む (= 書き手への back-pressure)。 `try_reset`
    ///   は書き出してから畳むまでの間に次の record が入ると畳めず、 書き手が休まず書く間は ring が満杯まで埋まっていた。
    ///   `pending_writes` は見ない — 全 append は `append_lock` の下で場所を取るので、 数に入っているのは lock を待っている
    ///   (まだ場所を取っていない) append だけ
    pub fn checkpoint_and_reset(&self, mut sync: impl FnMut(u64) -> bool, mut fold_safe: impl FnMut(u64) -> bool) -> (bool, bool) {
        if self.two.load(Ordering::Acquire) {
            // 書き出している間に書き手が今の区画を埋めて移ると、 読んだ head は古い区画の中 = 古い区画の checkpoint がその head に
            // 届かず畳めない (空きを待つ書き手が 「畳めない」 を見て諦めた)。 移っていたら新しい区画の head まで書き出し直す
            let mut advanced = false;
            for _ in 0..3 {
                let head = self.written_head();
                if self.is_empty() || !sync(head) {
                    return (advanced, false);
                }
                self.advance_checkpoint(head);
                advanced = true;
                if self.try_reset_if(&mut fold_safe) {
                    return (true, true);
                }
                if wal_pos_lap(head) == self.lap() {
                    break;
                }
            }
            return (advanced, false);
        }
        let _in_proc = self.append_lock.lock();
        let head = self.head();
        if self.is_empty() || !sync(head) {
            return (false, false);
        }
        self.advance_checkpoint_locked(head);
        let Some((i, through)) = self.fold_target_locked() else { return (true, false) };
        if !fold_safe(through) {
            return (true, false);
        }
        (true, self.fold_half_locked(i))
    }

    /// WAL の容量 (byte、 2 区画の和)。
    pub fn capacity(&self) -> u64 {
        self.hot.halves.iter().map(Half::size).sum()
    }

    /// 1 区画の大きさ (1 区画の形は file 全体)。 1 件の record はこれより小さいこと。
    pub fn half_capacity(&self) -> u64 {
        self.hot.halves[0].size()
    }

    /// pending_writes を返す(テスト/観測用)。 thread ごとの分を足す。
    pub fn pending_writes(&self) -> u32 {
        self.pending_shards.iter().fold(self.pending_writes.load(Ordering::Acquire), |n, s| n + s.0.load(Ordering::Acquire))
    }

    /// これ以上**いかなる record も** append できない満杯か (今の区画に Commit も入らず、 移れる空の区画も無い)。
    ///
    /// 最小の record は payload 0 の Commit（= REC_HEADER_SIZE bytes）なので、
    /// それすら入らない残量なら、 この WAL は畳まれるまで一切前進できない。
    /// commit group の途中でこの状態に達すると、 閉じの Commit も書けず tail は
    /// **永久に未 commit** のまま残る（recovery からも sync bridge からも不可視）。
    /// その tail を「畳んでよい死区間」と判定するために使う
    /// （engine の `wal_fold_safe` 参照）。
    ///
    /// #268: 判定は `alloc` と同じ `alloc_head()` で行う。 in-memory head だけを
    /// 見ていると、 別 fd の append で on-disk head だけが進んだ DB で
    /// 「append は全滅しているのに append_dead() は false」 になり、
    /// 出口 (fold の死区間例外) が閉じたまま無音で止まる。
    pub fn append_dead(&self) -> bool {
        let ai = self.hot.active.load(Ordering::Acquire);
        let a = &self.hot.halves[ai];
        let full = self.alloc_head(a) + self.record_size_of(0) as u64 > a.size();
        full && !(self.two.load(Ordering::Acquire) && self.hot.halves[ai ^ 1].is_empty())
    }

    /// 残り append 可能バイト数（観測用）。 `append_dead` と対で、 呼び出し側が
    /// 「WAL がどれだけ逼迫しているか」を可視化するのに使う。 今の区画の残りと、 移れる空の区画の大きさ。
    pub fn free_bytes(&self) -> u64 {
        let ai = self.hot.active.load(Ordering::Acquire);
        let a = &self.hot.halves[ai];
        let mut free = a.size().saturating_sub(self.alloc_head(a));
        if self.two.load(Ordering::Acquire) {
            let o = &self.hot.halves[ai ^ 1];
            if o.is_empty() {
                free += o.size().saturating_sub(HEADER_SIZE as u64);
            }
        }
        free
    }

    /// auto_reset を切り替え(Syncer attached の engine では false にする)。
    /// false にすると ring buffer reset が発火しなくなり、WAL は使い切るまで線形成長する。
    /// record が peer sync 前に消える race を防ぐ用途。
    pub fn set_auto_reset(&self, enabled: bool) {
        self.auto_reset.store(enabled, Ordering::Release);
    }

    /// auto_reset の現在値。
    pub fn auto_reset_enabled(&self) -> bool {
        self.auto_reset.load(Ordering::Acquire)
    }

    /// #268: **テスト専用** fault injection。 次の `n` 回の Commit append を
    /// 失敗させる (WAL は満杯でないまま、 error は `OutOfMemory` 以外 = 容量の失敗と見分ける、 #407)。
    ///
    /// 「Commit が打てない」 状態は実機では満杯 / 別 fd の head 先行でしか起きず、
    /// test から自然に作れない。 一方でそこは **恒久停止の入口** (閉じられない
    /// group が checkpoint の後ろに取り残されると、 bridge からも recovery からも
    /// 見えなくなる) なので、 gate を張れないままにしたくない。 判定は Commit の
    /// append だけで行うので、 通常 record の append には分岐も atomic load も
    /// 増えない。
    #[doc(hidden)]
    pub fn fail_next_commits(&self, n: u32) {
        self.fail_next_commits.store(n, Ordering::Release);
    }

    /// test 用: この WAL で、 group を閉じない record が閉じの Commit の分の空きを残すか (`alloc`、 既定は残す)。 残さないと、
    /// group の途中で Commit も入らない満杯 (`append_dead`) を作れる。 その形は普通の append では作れなくなったが、 前の版の
    /// binary が書いた WAL や別の fd の append では残りうるので、 畳む時の死区間の例外 (engine の `wal_fold_safe`、 #450) の
    /// test がこれで作る。 製品の道からは呼ばない。
    #[doc(hidden)]
    pub fn keep_commit_room_for_test(&self, keep: bool) {
        self.commit_room.store(if keep { self.record_size_of(0) as u64 } else { 0 }, Ordering::Relaxed);
    }

    fn note_dropped(&self, author: PeerId, hlc: Hlc) {
        self.note_dropped_minting(author, Some(hlc));
    }

    /// #451 (段階 1): 落ちた record を、 `dropped` の lock を持ったまま採番して覚える (`hlc` が無ければここで採番)。
    /// 切れ目 ([`OpLog::durable_cut_for`]) は同じ lock の下で時計を覗くので、 切れ目に見えなかった落ちた record の HLC は
    /// 切れ目より大きい (lock の外で採番してから覚えると、 採番と覚えるの間に切れ目が入って、 切れ目以下の HLC の落ちた
    /// record を切れ目が見逃す)
    fn note_dropped_minting(&self, author: PeerId, hlc: Option<Hlc>) -> Hlc {
        let mut g = self.dropped.lock().unwrap_or_else(|p| p.into_inner());
        let hlc = hlc.unwrap_or_else(|| self.next_hlc());
        match g.iter_mut().find(|(a, _)| *a == author) {
            Some((_, h)) => *h = (*h).max(hlc),
            None => g.push((author, hlc)),
        }
        if author == self.peer_id() {
            self.dropped_gen.fetch_add(1, Ordering::Release);
        }
        hlc
    }

    /// #451 (段階 1): 自分の落ちた record を覚えた回数 (切れ目と置く時で比べる)。
    pub fn dropped_gen(&self) -> u64 {
        self.dropped_gen.load(Ordering::Acquire)
    }

    /// #451 (段階 1、 配り済みの印): 切れ目。 `append_lock` と `dropped` の lock を持って時計を覗き、
    /// `(c, author の落ちた record が覚えられたままか)` を返す。
    ///
    /// - `author` の record で HLC ≤ c のものは、 返った時点で全部 ring に書き終えている (採番は append の中で `append_lock`
    ///   の下。 事前採番の record (`append_at_hlc` / `append_many_with_hlcs`) は開く時の作り直しだけで、 切れ目と並走しない)
    ///   ので、 この後に始めた fsync に入る。 この後に採番する record は全部 c より大きい
    /// - 落ちた record (#57) は `dropped` の lock の下で採番して覚える ([`OpLog::note_dropped_minting`]) ので、 2 つ目が false
    ///   なら author の落ちた record に HLC ≤ c のものは無い (後から落ちるものは c より大きい)。 畳む時の孤児の group (#450) は
    ///   閉じていない group の record = 呼び手が切れ目の後に打つ Commit より前に載った record は入らない (呼び手は Commit の前に切る)
    /// - 3 つ目・4 つ目 = ring の周と、 切れ目の時の終わり (append_lock の下なので書きかけは無い): この周でこれより前の record は
    ///   全部切れ目より前に載った = `author` のものは HLC ≤ c。 開く時の header の走査 ([`OpLog::visit_record_headers_with_tail`])
    ///   はここから読めばよい (周が違えば畳んだ後 = 先頭から)
    pub fn durable_cut_for(&self, author: PeerId) -> DurableCutPoint {
        let _in_proc = self.append_lock.lock();
        let dropped = self.dropped.lock().unwrap_or_else(|p| p.into_inner());
        let st = *self.hot.hlc_state.lock();
        DurableCutPoint {
            hlc: Hlc { wall: st.0, logical: st.1, peer: author },
            dropped_pending: dropped.iter().any(|(a, _)| *a == author),
            dropped_gen: self.dropped_gen.load(Ordering::Acquire),
            lap: self.active_half().lap(),
            offset: self.active_half().head.load(Ordering::Acquire),
            own_data: self.hot.own_data.load(Ordering::Acquire),
        }
    }

    /// ホストの形 (`HOST_PLAN.md`): 切れ目 ([`OpLog::durable_cut_for`] の `own_data`) の後に自分の中身のある record (Commit と中継を
    /// 除く) を積んでいなければ、 時計を今まで進めてその HLC を返す (以後の採番はこれより後なので、 HLC がこれ以下の `author` の
    /// 中身のある record は切れ目より前に積んだ物だけ)。 積んでいれば `None`。 満杯で落ちた自分の record (`_sync_ops` に無い) を
    /// 切れ目の後に覚えていても `None` (`dropped_gen`、 印もその間は進まない)。 採番は append の直列化の内側なので、 見る間に積まれる
    /// ことはない
    pub fn quiet_since(&self, own_data: u64, dropped_gen: u64, author: PeerId) -> Option<Hlc> {
        let _in_proc = self.append_lock.lock();
        // 落ちた record は `dropped` の lock の下で採番して覚える (`note_dropped_minting`、 append_lock の外): 同じ lock を持って
        // 回数を比べ、 持ったまま今を採番する = この後に落ちる record の HLC は今より後
        let _dropped = self.dropped.lock().unwrap_or_else(|p| p.into_inner());
        if self.hot.own_data.load(Ordering::Acquire) != own_data || self.dropped_gen.load(Ordering::Acquire) != dropped_gen {
            return None;
        }
        Some(Hlc { peer: author, ..self.next_hlc() })
    }

    /// ホストの形: 印の無い (自分の record を `_sync_ops` に載せたことの無い) DB が、 WAL にも自分の中身のある record (Commit を除く)
    /// を持たず、 落ちた record も覚えていなければ、 時計を今まで進めてその HLC を返す (書いたことの無い client = 読むだけの client が
    /// 配り終えの線を止めないように)。 WAL を頭から読むのは、 その author で最初に無かった時まで (読むだけの client は印ができない)
    pub fn quiet_without_mark(&self, author: PeerId) -> Option<Hlc> {
        let _in_proc = self.append_lock.lock();
        let dropped = self.dropped.lock().unwrap_or_else(|p| p.into_inner());
        if self.hot.own_data.load(Ordering::Acquire) != 0 || self.dropped_gen.load(Ordering::Acquire) != 0 || !dropped.is_empty() {
            return None;
        }
        let scanned = author as u64 + 1;
        if self.quiet_scanned.load(Ordering::Acquire) != scanned {
            let s = self.scan_from_offset(HEADER_SIZE as u64);
            if s.out.iter().chain(s.tail.iter()).any(|(r, _)| r.author_peer == author && !matches!(r.op, DecodedOp::Commit)) {
                return None;
            }
            self.quiet_scanned.store(scanned, Ordering::Release);
        }
        Some(Hlc { peer: author, ..self.next_hlc() })
    }

    /// #57: 前回から append に失敗して WAL に載らなかった record を、 author ごとに落ちた HLC の max で
    /// 取り出す (空にする)。
    ///
    /// 載らなかった record は `_sync_ops` にも入らないので、 差分 pull では二度と届かない。
    /// engine の bridge がこれを見て、 その author の history floor を上げる。
    pub fn take_dropped(&self) -> Vec<(PeerId, Hlc)> {
        std::mem::take(&mut *self.dropped.lock().unwrap_or_else(|p| p.into_inner()))
    }

    /// #451: ring にある record を全部 (Commit で閉じた group と、 閉じていない末尾) 返す。 読むだけ (clock を戻さない)。
    pub fn records_with_tail(&self) -> Vec<Record> {
        let s = self.scan_from_offset(HEADER_SIZE as u64);
        s.out.into_iter().chain(s.tail).map(|(r, _)| r).collect()
    }

    /// #451 (段階 1): [`OpLog::records_with_tail`] と同じ範囲 (Commit で閉じた group と閉じていない末尾) の record の (author, HLC) を、
    /// header だけ読んで渡す (payload を組まない・写さない。 開く時の照合は HLC だけ要る: 100 万 record で 340 ms → header の並びを
    /// 読むだけ)。 record ごとに CRC と周の印を確かめ、 `records_with_tail` と同じ所で止まる。 渡す op は `records_with_tail` と少し
    /// 違う (安全側): Commit も渡し、 読み飛ばしてよい op (#178、 ALIAS) は渡さない。
    ///
    /// `from` = 読み始める位置 (周, offset) ([`OpLog::durable_cut_for`] の 3 つ目・4 つ目 = 配り済みの印の位置)。 その周の区画は
    /// offset から (それより前の record は印より前)、 それより新しい周の区画は頭から、 古い周の区画は読まない (全部印より前)。
    /// 周がどの区画にも無く全部より古い (畳んだ後) / 周なし (v2) / offset が書き終えた終端より先 なら先頭から (`read_plan`)。
    pub fn visit_record_headers_with_tail(&self, from: Option<(u32, u64)>, mut f: impl FnMut(PeerId, Hlc)) {
        let trailer = self.trailer.load(Ordering::Acquire);
        let start = match from {
            Some((l, off)) if l != 0 => wal_pos(l, off),
            _ => 0,
        };
        let mmap = self.mmap_slice();
        for part in self.read_plan(start, true).into_iter().flatten() {
            let (base, lap) = (part.base, part.lap);
            let mut offset = part.start;
            while offset < part.end {
                let at = base + offset as usize;
                let rec_end = at + REC_HEADER_SIZE;
                if rec_end > mmap.len() {
                    return;
                }
                let header = &mmap[at..rec_end];
                if &header[OFF_MAGIC..OFF_MAGIC + 2] != REC_MAGIC {
                    return;
                }
                let version = header[OFF_VERSION];
                if !known_version(version) {
                    return;
                }
                let op_byte = header[OFF_OP];
                let payload_len = u32::from_le_bytes(header[OFF_LEN..OFF_LEN + 4].try_into().unwrap()) as usize;
                let payload_end = rec_end + payload_len;
                if payload_end > mmap.len() {
                    return;
                }
                let next_off = payload_end + trailer;
                if trailer != 0 {
                    if next_off > mmap.len() {
                        return;
                    }
                    let t = &mmap[payload_end..next_off];
                    let rec_lap = u32::from_le_bytes(t[0..4].try_into().unwrap());
                    let check = u32::from_le_bytes(t[4..8].try_into().unwrap());
                    if check != rec_lap ^ LAP_SALT || rec_lap != lap {
                        return;
                    }
                }
                let stored_crc = u32::from_le_bytes(header[OFF_CRC..OFF_CRC + 4].try_into().unwrap());
                if stored_crc != record_crc(version, &header[CRC_FIELDS], &mmap[rec_end..payload_end]) {
                    return;
                }
                let known_op = matches!(
                    op_byte,
                    op_type::TIE | op_type::UNTIE | op_type::DELETE | op_type::CONTENT | op_type::COMMIT | op_type::VOCAB
                        | op_type::TIE_NAMED | op_type::TIE_LEAF | op_type::TIE_REF | op_type::TIE64 | op_type::TIE_NAMED64
                );
                if known_op {
                    let hlc = Hlc {
                        wall: u64::from_le_bytes(header[OFF_HLC_WALL..OFF_HLC_WALL + 8].try_into().unwrap()),
                        logical: u32::from_le_bytes(header[OFF_HLC_LOGICAL..OFF_HLC_LOGICAL + 4].try_into().unwrap()),
                        peer: u32::from_le_bytes(header[OFF_HLC_PEER..OFF_HLC_PEER + 4].try_into().unwrap()),
                    };
                    f(u32::from_le_bytes(header[OFF_AUTHOR_PEER..OFF_AUTHOR_PEER + 4].try_into().unwrap()), hlc);
                } else if !op_type::skippable(op_byte) {
                    return;
                }
                offset = (next_off - base) as u64;
            }
        }
    }

    /// 位置 `from` ([`wal_pos`]) から読む区画の並び (周の古い順)。 `from` の周の区画はその offset から、 それより新しい周の区画は
    /// 頭から、 古い周の区画は読まない (読み終えている = 読み手の位置はその区画より後)。 `from` がどの区画よりも新しい (どの区画にも
    /// 同じ周も新しい周も無い = 壊れた位置) なら全部を頭から。 `from` の offset が区画の書き終えた終端より先なら、
    /// `beyond_from_start` で頭から (false なら何も読まない = cursor の 「失った更新」 は呼び手が見る)。
    ///
    /// 周 → 書き終えた終端の順に読む (Acquire)。 畳む側は周を置いてから終端を頭に戻し、 切り替えでは移る先の周を置いてから
    /// record を書くので、 読んだ組は 「新しい周と古い終端」 になっても逆は無い (前の周の record を新しい周で読んで止まるだけ)。
    /// 終端は **新しい区画を先に** 読む: 古い区画の終端を先に読むと、 その後に書き手が古い区画に足してから移って新しい区画に
    /// 書いた時、 古い区画の残りを飛ばして新しい区画を読み、 位置がその先へ進む (changefeed の受け手に区画 1 つ分の record が
    /// 届かなかった)。 新しい区画の終端を読んでから古い区画の終端を読めば、 新しい区画に record があれば古い区画の終端は最後の
    /// 値 (切り替えの前に置いた終端 → 移る → 新しい区画の終端、 の順に Release で置く)。
    fn read_plan(&self, from: u64, beyond_from_start: bool) -> [Option<ReadPart>; 2] {
        let (fl, foff) = (wal_pos_lap(from), wal_pos_offset(from));
        let hs = HEADER_SIZE as u64;
        let lap_of = |h: &Half| (h.size() > 0).then(|| (h.base(), h.lap()));
        let (a, b) = (lap_of(&self.hot.halves[0]), lap_of(&self.hot.halves[1]));
        // (古い方, 新しい方) の区画の番号
        let order: [Option<usize>; 2] = match (a, b) {
            (Some((_, la)), Some((_, lb))) => {
                if lap_newer(la, lb) {
                    [Some(1), Some(0)]
                } else {
                    [Some(0), Some(1)]
                }
            }
            (Some(_), None) => [Some(0), None],
            (None, Some(_)) => [Some(1), None],
            (None, None) => [None, None],
        };
        let laps = [a, b];
        let part = |i: Option<usize>, end: u64| {
            i.and_then(|i| laps[i]).map(|(base, lap)| ReadPart { base, lap, start: hs, end })
        };
        let newer_end = order[1].map_or(0, |i| self.hot.halves[i].written.load(Ordering::Acquire));
        #[cfg(all(test, not(target_arch = "wasm32")))]
        read_plan_hook::call();
        let older_end = order[0].map_or(0, |i| self.hot.halves[i].written.load(Ordering::Acquire));
        let parts = [part(order[0], older_end), part(order[1], newer_end)];
        let known = parts.iter().flatten().any(|p| p.lap == fl || lap_newer(p.lap, fl));
        parts.map(|p| {
            let mut p = p?;
            if !known {
                return Some(p);
            }
            if p.lap == fl {
                p.start = if foff > p.end {
                    if beyond_from_start { hs } else { foff }
                } else {
                    foff.max(hs)
                };
                Some(p)
            } else if lap_newer(p.lap, fl) {
                Some(p)
            } else {
                None
            }
        })
    }

    /// #450: `start_offset` から先の、 Commit で閉じられていない record (閉じの Commit が満杯で入らなかった孤児の group) を、
    /// WAL に載らなかった record と同じに覚える ([`OpLog::take_dropped`] が返す)。 満杯の死区間で ring を畳む直前に呼ぶ —
    /// 畳むと二度と配れないので、 engine の bridge がその author の floor を上げる。 戻り値は覚えた record の数。
    pub fn note_uncommitted_tail_dropped(&self, start_offset: u64) -> usize {
        let tail = self.scan_from_offset(start_offset).tail;
        for (r, _) in &tail {
            self.note_dropped(r.author_peer, r.hlc);
        }
        tail.len()
    }

    /// fault injection の残数を 1 消費する。 消費できたら true。
    fn take_commit_fault(&self) -> bool {
        self.fail_next_commits
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n == 0 { None } else { Some(n - 1) }
            })
            .is_ok()
    }

    /// fsync(WAL 本体のみ)。consumer スレッドが定期実行。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn fsync(&self) -> io::Result<()> {
        #[cfg(all(feature = "crashsim", unix))]
        let sim = crate::crashsim::active().then(|| crate::crashsim::copy_for_sync(crate::crashsim::Copied::Oplog, 0, &self.mmap));
        crate::io_point("oplog.fsync")?;
        self.mmap.flush()?;
        #[cfg(all(feature = "crashsim", unix))]
        if let Some(copy) = sim {
            crate::crashsim::data_synced(&self._file, copy);
        }
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    pub fn fsync(&self) -> io::Result<()> { Ok(()) }

    /// checkpoint を前進。本体 mmap に反映済み LSN の位置まで進める。
    ///
    /// #77-M8: checkpoint は head を越えられない (clamp)。 caller が reset 前に
    /// 読んだ stale head を渡すと checkpoint > head の壊れ状態になり、 head が
    /// その値を再突破するまで背景 fsync / sync 転送が止まっていた。
    pub fn advance_checkpoint(&self, new_checkpoint: u64) {
        // 畳む (`append_lock` の下) と並ばないように: 畳む前の head を読んで畳んだ後の区画に書くと checkpoint > head になる
        let _in_proc = self.append_lock.lock();
        self.advance_checkpoint_locked(new_checkpoint);
    }

    /// `advance_checkpoint` の本体 (`append_lock` の下)。 位置の周の区画はその offset まで (head で切る)、 それより古い周の区画は
    /// head まで (全部その位置より前に積んだ)、 新しい周の区画は進めない。 前にだけ進める。
    fn advance_checkpoint_locked(&self, new_checkpoint: u64) {
        let (l, off) = (wal_pos_lap(new_checkpoint), wal_pos_offset(new_checkpoint));
        let mmap = self.mmap_mut_slice();
        for h in &self.hot.halves {
            if h.size() == 0 {
                continue;
            }
            let (hl, head) = (h.lap(), h.head.load(Ordering::Acquire));
            let target = if hl == l {
                off.min(head)
            } else if lap_newer(l, hl) {
                head
            } else {
                continue;
            };
            if target <= h.checkpoint.load(Ordering::Acquire) {
                continue;
            }
            h.checkpoint.store(target, Ordering::Release);
            let b = h.base();
            mmap[b + 16..b + 24].copy_from_slice(&target.to_le_bytes());
        }
    }

    /// HEADER_SIZE から head までを読んで、Commit で挟まれた全レコードを返す。
    /// checkpoint 位置は無視(既に apply 済みの記録もまだ WAL file 上にあれば拾う)。
    /// Syncer.publish_since で使う。ring buffer reset 済みの記録は取れない。
    pub fn iter_committed(&self) -> Vec<Record> {
        strip_offsets(self.scan_from_offset(HEADER_SIZE as u64).out)
    }

    /// 指定 offset 以降の commit 済みレコードを返す。changefeed の差分発火用。
    /// `start_offset` は前回 emit 時の `wal.checkpoint()` を渡す想定。
    pub fn iter_committed_from(&self, start_offset: u64) -> Vec<Record> {
        strip_offsets(self.scan_from_offset(start_offset).out)
    }

    /// #152: `iter_committed_from_with_end` に **各 record の終端 offset** を添えた版。
    ///
    /// 途中で処理を打ち切った consumer が「どこまで処理し切ったか」を cursor に落とせる
    /// ようにする (= partial advance)。 record の終端 offset は次 record の先頭でもあるので、
    /// そのまま次回の `start_offset` として渡せる。
    ///
    /// group の途中を指す offset から再開しても取りこぼさない: `out` に入る record は
    /// 必ず Commit で閉じられた group の一員なので、 再 scan は残りの record を読んでから
    /// その Commit に到達して flush する。
    pub fn iter_committed_from_with_offsets(&self, start_offset: u64) -> (Vec<(Record, u64)>, u64) {
        let s = self.scan_from_offset(start_offset);
        (s.out, s.committed_end)
    }

    /// #268: `iter_committed_from_with_offsets` に **scan の診断**を添えた版。
    ///
    /// 返り値の 3 番目は 「読めたが Commit で閉じられていない record 数」、
    /// 4 番目は 「scan がなぜ止まったか」。 bridge が 0 を返し続けるとき、
    /// この 2 つで原因が 2 分される:
    ///
    /// - `(0, BadMagic | BadCrc | ..)` — cursor 位置の record が読めない
    /// - `(n > 0, Head)` — record は在るが Commit が付いていない (= 閉じ待ち)
    pub fn iter_committed_from_with_diag(
        &self,
        start_offset: u64,
    ) -> (Vec<(Record, u64)>, u64, usize, ScanStop) {
        let s = self.scan_from_offset(start_offset);
        (s.out, s.committed_end, s.tail.len(), s.stop)
    }

    /// #77-H4: `iter_committed_from` + 「読み切った commit 済み group の終端
    /// offset」を返す版。 cursor (changefeed emit_offset / _sync_ops bridge) は
    /// **この終端までしか進めてはいけない** — head は record 本体の書き込み前に
    /// bump されるため、 scan 後に `head()` を再読して cursor にすると、 scan が
    /// 書き込み途中 record で break した位置〜head 間の record を恒久 skip する。
    pub fn iter_committed_from_with_end(&self, start_offset: u64) -> (Vec<Record>, u64) {
        let s = self.scan_from_offset(start_offset);
        (strip_offsets(s.out), s.committed_end)
    }

    /// changefeed の溜め用 (engine が ring を畳む直前、 `append_lock` の下で呼ぶ): `iter_committed_from_with_end` に加えて、
    /// 末尾の Commit で閉じていない record と、 読み終えた終端 offset を返す。 読むだけ。 返り値は
    /// (Commit で閉じた record, committed_end, 閉じていない末尾の record, 読み終えた終端)。
    pub fn iter_from_with_tail(&self, start_offset: u64) -> (Vec<Record>, u64, Vec<Record>, u64) {
        self.iter_from_with_tail_upto(start_offset, u64::MAX)
    }

    /// `iter_from_with_tail` を位置 `upto` ([`wal_pos`]、 畳む区画の終わり) で止める版 (#662: 今の区画は畳まないので読まない)。
    pub fn iter_from_with_tail_upto(&self, start_offset: u64, upto: u64) -> (Vec<Record>, u64, Vec<Record>, u64) {
        let s = self.scan_range(start_offset, upto);
        let end = s.tail.last().map_or(s.committed_end, |(_, e)| *e);
        (strip_offsets(s.out), s.committed_end, strip_offsets(s.tail), end)
    }

    /// changefeed の記録 (engine の `changefeed.seg`) 用: `start` から読んで、 (record, その終端 offset) と読み終えた offset を返す。
    /// `with_tail` が false なら `limit` (書き出し済みの位置 = checkpoint) までに Commit で閉じた group だけ。 true なら `limit`
    /// (畳む区画の終わり) までの record を閉じていない末尾も含めて全部 (畳む直前に呼ぶ。 `limit` より先は読まない、 #662)。 読むだけ。
    pub fn scan_for_change_log(&self, start: u64, limit: u64, with_tail: bool) -> (Vec<(Record, u64)>, u64) {
        if with_tail {
            let s = self.scan_range(start, limit);
            let end = s.tail.last().map_or(s.committed_end, |(_, e)| *e);
            let mut out = s.out;
            out.extend(s.tail);
            return (out, end);
        }
        let s = self.scan_from_offset(start);
        let end = s.commit_ends.iter().rev().find(|&&c| c <= limit).copied().unwrap_or(start);
        (s.out.into_iter().filter(|(_, e)| *e <= end).collect(), end)
    }

    /// リカバリ: checkpoint から head までを読んで、Commit で挟まれたグループだけ返す。
    /// CRC 破損レコードに到達したらそこで打ち切り(uncommitted tail の切り捨て)。
    ///
    /// #77-H5: LSN / HLC clock の復元 (`next_lsn` / `hlc_*` の store) は
    /// **recover だけ**が行う (単一スレッドの起動時実行前提)。 以前は
    /// iter_committed* (= changefeed / sync publish の並行 read 経路) も
    /// 共通実装の副作用で clock を巻き戻しており、 scan 中に発行された
    /// LSN/HLC を潰して重複発行 → sync dedupe による silent loss を招いた。
    pub fn recover(&self) -> Vec<Record> {
        self.recover_inner().0
    }

    /// recovery 専用: commit 済み group に加えて **末尾の未 commit batch も返す**。
    ///
    /// body が primary state で、 WAL は **body より先に書かれる** (concurrent 経路の
    /// consumer は 1 tick の中で WAL append → body 適用 の順に流す)。 crash がその間に
    /// 入ると 「WAL には在るが body には無い record」 が末尾に残る。
    ///
    /// これを replay せずに checkpoint で越えると、 その write は body に反映されない
    /// まま**恒久的に失われる** — 以後どの scan も checkpoint より前は見ないため。
    /// `advance_checkpoint` を committed_end に留めるだけでは足りない: 走行中の engine
    /// は誰もその record を body に適用しないので、 次の周期 fsync が Commit を打って
    /// checkpoint を再び越え、 しかも Commit が付いた時点で `_sync_ops` へ bridge される
    /// (= body に無いものを相手に配る)。 だから **recovery で適用しきる**。
    ///
    /// 再適用は冪等: cell 版数 (v9) / LWW が同じ HLC を弾くので、 既に body へ効いて
    /// いる record を含んでいても二重適用にならない。
    ///
    /// CRC 破損 record に当たった時点で scan は打ち切るので (`scan_from_offset`)、
    /// 書きかけの tail が混ざることはない。 畳んだ後も page に残っている前の周の record (CRC は合う) は周の印で
    /// 止まる (#608)。
    pub fn recover_with_tail(&self) -> Vec<Record> {
        let (mut out, tail) = self.recover_inner();
        out.extend(tail);
        out
    }

    /// 開いた時の再生 (`recover_with_tail`、 checkpoint から読む) が head より手前で止まったら (読めない record = 届かなかった
    /// page)、 そこから先の record を捨てる。 再生は前だけを当てるので、 先の record は本体に当てていない。 開いた直後 (書き手の
    /// 居ない間) に、 再生と同じ所から読んで決める。 戻り値は捨てたか。
    ///
    /// - #663: 古い区画の途中で止まったのに新しい区画に record があれば、 新しい区画を畳む (周を進めて頭へ)。 残すと、 開いた後に
    ///   古い区画を畳んだ時に読めるようになり、 本体に当てていない record を changefeed の記録 (と bridge・audit) に渡した
    /// - #669: 止まった区画の head (と書き終えた所) を読めた所まで戻す。 戻さないと、 開いた後の書き込みは読めない record の後ろに
    ///   積まれ、 読み (changefeed の記録・bridge・audit・次に開いた時の再生) が届かなかった。 区画を移った時の閉じの Commit
    ///   (`switch_locked`) は header の page だけを先に書き出すので、 閉じの Commit の中身が届かずに head だけがその後ろを指す像が
    ///   出る (#665)
    pub fn drop_records_after_a_hole(&self) -> bool {
        let _in_proc = self.append_lock.lock();
        let start = self.checkpoint();
        let s = self.scan_from_offset(start);
        if s.stop == ScanStop::Head {
            return false;
        }
        // 読めた所 (最後に読めた record の終端。 1 つも読めなければ checkpoint)
        let end = s.tail.last().map_or(s.committed_end, |(_, e)| *e);
        let ai = self.hot.active.load(Ordering::Relaxed);
        let Some(i) = [ai, ai ^ 1].into_iter().find(|&i| {
            let h = &self.hot.halves[i];
            h.size() > 0 && !h.is_empty() && h.lap() == wal_pos_lap(end)
        }) else {
            return false;
        };
        let h = &self.hot.halves[i];
        let mut dropped = false;
        if wal_pos_offset(end) < h.head.load(Ordering::Acquire) {
            self.trim_half_locked(h, wal_pos_offset(end));
            dropped = true;
        }
        // 古い区画で止まった: 新しい区画の record は全部穴の後
        if i != ai && !self.hot.halves[ai].is_empty() {
            dropped |= self.fold_half_locked(ai);
        }
        if dropped {
            let _ = self.fsync();
        }
        dropped
    }

    /// #669: 区画 `h` の head・書き終えた所・checkpoint を `end` (区画の中の offset) までに戻す (`append_lock` の下、 開いた直後)。
    fn trim_half_locked(&self, h: &Half, end: u64) {
        let b = h.base();
        h.head.store(end, Ordering::Release);
        self.put_head(h, b, end);
        if h.checkpoint.load(Ordering::Acquire) > end {
            h.checkpoint.store(end, Ordering::Release);
            self.mmap_mut_slice()[b + 16..b + 24].copy_from_slice(&end.to_le_bytes());
        }
    }

    /// `recover` / `recover_with_tail` の共通部。 clock 復元 (#77-H5) はここだけで行う。
    fn recover_inner(&self) -> (Vec<Record>, Vec<Record>) {
        let start = self.checkpoint();
        let s = self.scan_from_offset(start);
        if s.max_lsn > 0 {
            self.hot.next_lsn.store(s.max_lsn + 1, Ordering::Release);
        }
        if s.max_hlc.wall > 0 {
            *self.hot.hlc_state.lock() = (s.max_hlc.wall, s.max_hlc.logical);
        }
        (strip_offsets(s.out), strip_offsets(s.tail))
    }

    /// 指定 offset から head までを Commit グループ単位で読む。共通実装。
    /// 副作用なし (読むだけ)。返り値は (records, max_lsn, max_hlc,
    /// committed_end)。 committed_end = 最後に読み切った Commit record の
    /// 直後 offset (1 つも無ければ start_offset)。
    ///
    /// #152: records は `(Record, その record の終端 offset)` の組。 終端 offset は
    /// partial advance (= 途中まで処理した consumer の cursor) に使う。 offset 不要な
    /// caller は `strip_offsets` で落とす。
    /// 戻り値の 5 番目は **末尾の未 commit batch** (Commit で閉じられなかった分)。
    /// `iter_committed*` は捨てるが、 recovery だけは replay に使う
    /// (`recover_with_tail` の doc 参照)。
    fn scan_from_offset(
        &self,
        start_offset: u64,
    ) -> ScanOut {
        self.scan_range(start_offset, u64::MAX)
    }

    /// `scan_from_offset` を位置 `upto` ([`wal_pos`]、 record の境目 = 区画の終わりか head) で止める版: `upto` から先の record は
    /// 読まない (`upto` で読み終えたのと同じに扱う)。 畳む区画の終わりまでを読む時に、 今の区画を読まないため (#662)。
    fn scan_range(&self, start_offset: u64, upto: u64) -> ScanOut {
        let mut out = Vec::new();
        let mut batch = Vec::new();
        let mut commit_ends = Vec::new();
        let mut committed_end = start_offset;
        let trailer = self.trailer.load(Ordering::Acquire);
        let mut max_lsn = 0;
        let mut max_hlc = Hlc::ZERO;
        // #268: 打ち切り理由。 while を抜け切れば「head まで読んだ」。
        let mut stop = ScanStop::Head;

        let mmap = self.mmap_slice();

        // 区画を周の古い順に。 group は区画をまたいで続く (切り替えは閉じの Commit を置くので普段はまたがない)。 区画の途中で
        // 止まったら (壊れた record / 前の周の record) 後ろの区画も読まない: 後ろの区画の record は全部その後に積んだ物
        // #608: 区画ごとに周を `written` より先に読む (`read_plan`)。 #268: head ではなく書き終えた終端まで (head との間は書いている
        // 最中 = 前の周の record が見える)
        // 最後まで読んだ区画の終わり (閉じていない record が無ければ、 ここまで読み終えた位置を返す)
        let mut read_end: Option<u64> = None;
        let hs = HEADER_SIZE as u64;
        'parts: for part in self.read_plan(start_offset, false).into_iter().flatten() {
            let (base, lap, head) = (part.base, part.lap, part.end);
            let mut offset = part.start;
            while offset < head {
                if wal_pos(lap, offset) >= upto {
                    break 'parts;
                }
                let at = base + offset as usize;
                let rec_end = at + REC_HEADER_SIZE;
                if rec_end > mmap.len() { stop = ScanStop::OutOfBounds; break 'parts; }
                let header = &mmap[at..rec_end];
                if &header[OFF_MAGIC..OFF_MAGIC + 2] != REC_MAGIC {
                    stop = ScanStop::BadMagic;
                    break 'parts;
                }
                let version = header[OFF_VERSION];
                if !known_version(version) {
                    stop = ScanStop::BadVersion;
                    break 'parts;
                }

                let op_byte = header[OFF_OP];
                let payload_len = u32::from_le_bytes(header[OFF_LEN..OFF_LEN + 4].try_into().unwrap()) as usize;
                let lsn = u64::from_le_bytes(header[OFF_LSN..OFF_LSN + 8].try_into().unwrap());
                let hlc_wall = u64::from_le_bytes(header[OFF_HLC_WALL..OFF_HLC_WALL + 8].try_into().unwrap());
                let hlc_logical = u32::from_le_bytes(header[OFF_HLC_LOGICAL..OFF_HLC_LOGICAL + 4].try_into().unwrap());
                let hlc_peer = u32::from_le_bytes(header[OFF_HLC_PEER..OFF_HLC_PEER + 4].try_into().unwrap());
                let author_peer = u32::from_le_bytes(header[OFF_AUTHOR_PEER..OFF_AUTHOR_PEER + 4].try_into().unwrap());
                let stored_crc = u32::from_le_bytes(header[OFF_CRC..OFF_CRC + 4].try_into().unwrap());
                let mut signature = [0u8; 64];
                signature.copy_from_slice(&header[OFF_SIGNATURE..OFF_SIGNATURE + 64]);
                let mut pubkey_fp = [0u8; 8];
                pubkey_fp.copy_from_slice(&header[OFF_PUBKEY_FP..OFF_PUBKEY_FP + 8]);
                let hlc = Hlc { wall: hlc_wall, logical: hlc_logical, peer: hlc_peer };

                let payload_off = rec_end;
                let payload_end = payload_off + payload_len;
                if payload_end > mmap.len() { stop = ScanStop::OutOfBounds; break 'parts; }

                // v3 は header の値も覆う (壊れた header のゴミの HLC / author を通さない、 #58)
                let computed_crc = record_crc(version, &header[CRC_FIELDS], &mmap[payload_off..payload_end]);
                if stored_crc != computed_crc {
                    stop = ScanStop::BadCrc; // 破損 tail
                    break 'parts;
                }
                // #608: 周の印。 この周でない record は、 畳んだ後も page に残っている前の周の中身 (header の page だけ先に書き
                // 出された像では、 新しい周の数件の後ろにそれが続く)。 CRC は合うので印で見分ける
                let next_off = payload_end + trailer;
                if trailer != 0 {
                    if next_off > mmap.len() { stop = ScanStop::OutOfBounds; break 'parts; }
                    let t = &mmap[payload_end..next_off];
                    let rec_lap = u32::from_le_bytes(t[0..4].try_into().unwrap());
                    let check = u32::from_le_bytes(t[4..8].try_into().unwrap());
                    if check != rec_lap ^ LAP_SALT {
                        stop = ScanStop::BadLap; // 書きかけの印 (torn)
                        break 'parts;
                    }
                    if rec_lap != lap {
                        stop = ScanStop::StaleLap;
                        break 'parts;
                    }
                }

                let payload_slice = &mmap[payload_off..payload_end];
                let op = decode_op(op_byte, payload_slice);
                if lsn > max_lsn { max_lsn = lsn; }
                if hlc > max_hlc { max_hlc = hlc; }

                // 署名はその record の版の bytes に掛かっている。 header の先頭は signed_bytes と同じ並びなので
                // そのまま切り出す (v2 の record を v3 として組み直さない)
                let mut signed_bytes = Vec::with_capacity(SIGNED_PAYLOAD_HEADER_SIZE + payload_len);
                signed_bytes.extend_from_slice(&header[..SIGNED_PAYLOAD_HEADER_SIZE]);
                signed_bytes.extend_from_slice(payload_slice);

                match op {
                    Some(DecodedOp::Commit) => {
                        out.append(&mut batch);
                        committed_end = wal_pos(lap, (next_off - base) as u64);
                        commit_ends.push(committed_end);
                    }
                    Some(other) => {
                        batch.push((
                            Record {
                                lsn, hlc, author_peer, op: other,
                                signature, pubkey_fp, signed_bytes,
                            },
                            wal_pos(lap, (next_off - base) as u64),
                        ));
                    }
                    // #178: 知らない版が書いた 「読み飛ばしてよい」 op は、 その record だけ飛ばして先を読む (下の打ち切りと違い、
                    // 後ろの record を捨てない)
                    None if op_type::skippable(op_byte) => {
                        self.skipped_ops.fetch_add(1, Ordering::Relaxed);
                    }
                    None => {
                        // forward-compat 可視化 (0.9.0 L1): 新しい version が書いた
                        // 未知 op_type (or 不正 payload) は従来通りここで scan を打ち
                        // 切るが、 silent だと以降の正常 record 落ちに気付けない。
                        // changefeed 経路は cursor が進めず毎 tick 再 scan するため、
                        // 0.8.15 の persist warning と同様に 1 秒 1 回に rate-limit。
                        static LAST_WARN_MS: AtomicU64 = AtomicU64::new(0);
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let last = LAST_WARN_MS.load(Ordering::Relaxed);
                        if now_ms.saturating_sub(last) >= 1000
                            && LAST_WARN_MS
                                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                                .is_ok()
                        {
                            eprintln!(
                                "enchudb oplog: undecodable op (op_type={}) at offset {} — \
                                 stopping scan; later records in this WAL are ignored \
                                 (written by a newer version?)",
                                op_byte, offset,
                            );
                        }
                        stop = ScanStop::UndecodableOp;
                        break 'parts;
                    }
                }

                offset = (next_off - base) as u64;
            }
            // 空の区画の頭へは、 前に読んだ区画も空の時だけ進める: 新しい区画が空なら古い区画が今書き足している区画 (今の区画が空
            // なのは、 その場で畳んだ = 周がいちばん新しい時だけ) で、 その頭へ進むと古い区画にこの後足される record を飛ばす
            if part.end > hs || read_end.is_none_or(|e| wal_pos_offset(e) <= hs) {
                read_end = Some(wal_pos(lap, offset.max(part.start)));
            }
        }
        // 全部を読み切って閉じていない record も無ければ、 読み終えた位置まで進めてよい (区画を畳んだ後の空の区画 / 読み飛ばした
        // op の後。 位置を進めないと、 畳んだ区画を指す位置 (= 先頭) のまま `head` より小さく見え、 読み手が追いついたと分からない)
        if stop == ScanStop::Head
            && batch.is_empty()
            && let Some(e) = read_end
        {
            committed_end = committed_end.max(e);
        }

        // 未 commit batch は `out` には混ぜず、 呼び手が選べるよう別枠で返す。
        ScanOut { out, max_lsn, max_hlc, committed_end, tail: batch, stop, commit_ends }
    }

    /// head を checkpoint に戻す(WAL truncate 相当、uncommitted も全捨て)。
    pub fn reset_to_checkpoint(&self) {
        let _in_proc = self.append_lock.lock();
        let h = self.active_half();
        let cp = h.checkpoint.load(Ordering::Acquire);
        h.head.store(cp, Ordering::Release);
        self.put_head(h, h.base(), cp);
    }

    /// WAL 使用量(bytes)と容量を返す(監視用)。
    pub fn usage(&self) -> (u64, u64) {
        let used = self.hot.halves.iter().filter(|h| h.size() > 0).map(|h| {
            h.head.load(Ordering::Acquire).saturating_sub(h.checkpoint.load(Ordering::Acquire))
        });
        (used.sum(), self.capacity())
    }

    // ---- flock (multi-process write 排他) ----

    /// WAL ファイルに対する LOCK_EX を取る。 戻り値の guard が drop された時点で
    /// 解放される。 同じ .wal を別 process が同時に append しようとした場合、 lock
    /// 取得まで block する (典型的に数 µs〜ms)。 read 経路は lock 取らない。
    #[cfg(not(target_arch = "wasm32"))]
    fn flock_exclusive(&self) -> io::Result<OpLogLockGuard<'_>> {
        // 開いている間の lock を持っていれば、 record ごとには取らない (呼び手は `append_lock` の下)
        if self.session_locked.load(Ordering::Relaxed) {
            return Ok(OpLogLockGuard { file: None });
        }
        // #280: advisory lock を持たない FS では **排他なしで続行**する
        // (エラーにすると platform / FS ごと書き込み不能になる)。 同一プロセス内の
        // 直列化は呼び出し側の `append_lock` が担うので、 単一プロセス構成
        // (モバイル) は安全側に倒れる。 guard の unlock は no-op になる。
        crate::io_point("oplog.flock")?;
        let _ = crate::filelock::lock_exclusive(&self._file)?;
        #[cfg(test)]
        FLOCK_TAKEN.with(|c| c.set(c.get() + 1));
        Ok(OpLogLockGuard { file: Some(&self._file) })
    }

    /// この OpLog を開いている間、 WAL の flock を持ちっぱなしにする (閉じると OS が外す)。 以後の append は record ごとに
    /// flock を取らない (system call 2 回を省く)。 engine が書き手の lock (`.db.lock`、 書き手の process は 1 つ) を持って WAL を
    /// 開いた時に呼ぶ: その間、 同じ WAL に追記する別の process は居ない。 何度呼んでもよい。 別の process が record ごとの
    /// flock で追記する使い方 (この crate を直に使う時) では呼ばない。
    ///
    /// 待たずに試す (#561): 他が持っていれば (別の process / 同じ process で同じ file を開いた別の `OpLog` がまだ生きている —
    /// engine より長く生きた `Arc<OpLog>` など) `ErrorKind::WouldBlock` を返し、 record ごとに取るままにする。 前は `append_lock`
    /// を持ったまま待ち、 同じ process の前の `OpLog` が残っていると開き直しが永久に止まった。 lock を持たない FS は排他なしで
    /// 開いている間の lock を持ったことにする (`flock_exclusive` と同じ)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn lock_session(&self) -> io::Result<()> {
        let _in_proc = self.append_lock.lock();
        if !self.session_locked.load(Ordering::Relaxed) {
            crate::io_point("oplog.lock_session")?;
            if crate::filelock::try_lock_exclusive_outcome(&self._file)?.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "oplog: the WAL is locked by another open (another process, or an OpLog of this process that outlived its engine)",
                ));
            }
            self.session_locked.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    /// wasm には flock が無い (`flock_exclusive` も無い) ので何もしない。
    #[cfg(target_arch = "wasm32")]
    pub fn lock_session(&self) -> io::Result<()> {
        Ok(())
    }

    /// [`OpLog::lock_session`] を呼んだか (test 用)。
    pub fn session_locked(&self) -> bool {
        self.session_locked.load(Ordering::Relaxed)
    }

    // ---- internal mmap helpers ----

    #[cfg(not(target_arch = "wasm32"))]
    #[inline]
    fn mmap_slice(&self) -> &[u8] { &self.mmap[..] }

    #[cfg(target_arch = "wasm32")]
    #[inline]
    fn mmap_slice(&self) -> &[u8] { &self.buf[..] }

    #[cfg(not(target_arch = "wasm32"))]
    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn mmap_mut_slice(&self) -> &mut [u8] {
        unsafe {
            let ptr = self.mmap.as_ptr() as *mut u8;
            std::slice::from_raw_parts_mut(ptr, self.mmap.len())
        }
    }

    #[cfg(target_arch = "wasm32")]
    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn mmap_mut_slice(&self) -> &mut [u8] {
        unsafe {
            let ptr = self.buf.as_ptr() as *mut u8;
            std::slice::from_raw_parts_mut(ptr, self.buf.len())
        }
    }

    /// wasm32 用スタブ。
    #[cfg(target_arch = "wasm32")]
    pub fn create_in_memory(capacity: usize) -> Self {
        let mut buf = vec![0u8; capacity];
        let layout = fresh_layout(&mut buf, capacity as u64);
        Self::assemble(buf, layout)
    }
}

/// 現在時刻を ms since UNIX epoch で返す。wasm32 では 0。
///
/// HLC の採番 (WAL に積む 1 回ごと、 `append_lock` の中) で読む。 Apple の aarch64 (macOS / iOS) の `SystemTime::now()` は
/// `mach_absolute_time` が Apple 独自の counter の register を読む所が重く、 書き手 1 本の sample でも書き手の時間の 2〜5 割を
/// 占めていた (#506)。 そこでは thread ごとに、 前に読んだ時刻に CPU の counter (`cntvct_el0`: 全 core でそろった一定の周波数で、
/// 眠っている間も進む。 読みは数 ns) の進みを足して出し、 1 ms ごとに読み直す (時刻を合わせ直した時の飛びは 1 ms 以内に拾う)。
#[inline]
fn current_wall_ms() -> u64 {
    #[cfg(all(target_arch = "aarch64", target_vendor = "apple", not(miri)))]
    {
        apple_wall_clock::now_ms()
    }
    #[cfg(all(not(target_arch = "wasm32"), not(all(target_arch = "aarch64", target_vendor = "apple", not(miri)))))]
    {
        system_wall_ns() / 1_000_000
    }
    #[cfg(target_arch = "wasm32")]
    { 0 }
}

/// `SystemTime::now()` を ns since UNIX epoch で (epoch より前なら 0)。
#[cfg(not(target_arch = "wasm32"))]
fn system_wall_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(all(target_arch = "aarch64", target_vendor = "apple", not(miri)))]
mod apple_wall_clock {
    use std::cell::Cell;

    /// counter の今の値。 `isb` を挟まない (前後の命令と少し入れ違ってよい: 1 ms の窓の判定にだけ使う)。
    #[inline]
    fn ticks() -> u64 {
        let t: u64;
        // SAFETY: cntvct_el0 は EL0 から読める (`mach_absolute_time` も同じ register を読む)。 読むだけで副作用は無い
        unsafe { std::arch::asm!("mrs {}, cntvct_el0", out(reg) t, options(nomem, nostack, preserves_flags)) };
        t
    }

    /// counter の周波数 (Hz)。
    #[inline]
    fn freq() -> u64 {
        let f: u64;
        // SAFETY: 同上
        unsafe { std::arch::asm!("mrs {}, cntfrq_el0", out(reg) f, options(nomem, nostack, preserves_flags)) };
        f
    }

    thread_local! {
        /// 最後に読み直した時の (counter, 時刻 ns)。 counter 0 は未読
        static BASE: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
    }

    pub(super) fn now_ms() -> u64 {
        now_ns() / 1_000_000
    }

    /// 今の時刻 (ns since UNIX epoch)。 控えた時刻からの足しは、 本当の時刻より先に進まない (控えは時刻を読んだ後の counter)。
    pub(super) fn now_ns() -> u64 {
        let f = freq();
        BASE.with(|b| {
            let (t0, ns0) = b.get();
            let d = ticks().wrapping_sub(t0);
            if t0 != 0 && f >= 1000 && d < f / 1000 {
                return ns0 + d * 1_000_000_000 / f;
            }
            let before = ticks();
            let ns = super::system_wall_ns();
            let after = ticks();
            // 時刻の読みの間に止められた (組がずれた) 時は控えない: 足した時刻がその間だけ遅れる
            if after.wrapping_sub(before) < f / 100_000 {
                b.set((after, ns));
            }
            ns
        })
    }
}

fn decode_op(op_byte: u8, payload: &[u8]) -> Option<DecodedOp> {
    match op_byte {
        op_type::TIE if payload.len() >= 16 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let himo_id = u16::from_le_bytes(payload[8..10].try_into().unwrap());
            let value = u32::from_le_bytes(payload[12..16].try_into().unwrap()) as u64;
            Some(DecodedOp::Tie { eid, himo_id, value })
        }
        op_type::TIE64 if payload.len() >= 24 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let himo_id = u16::from_le_bytes(payload[8..10].try_into().unwrap());
            let value = u64::from_le_bytes(payload[16..24].try_into().unwrap());
            Some(DecodedOp::Tie { eid, himo_id, value })
        }
        op_type::UNTIE if payload.len() >= 16 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let himo_id = u16::from_le_bytes(payload[8..10].try_into().unwrap());
            Some(DecodedOp::Untie { eid, himo_id })
        }
        op_type::DELETE if payload.len() >= 8 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            Some(DecodedOp::Delete { eid })
        }
        op_type::CONTENT if payload.len() >= 16 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let klen = u16::from_le_bytes(payload[8..10].try_into().unwrap()) as usize;
            let dlen = u32::from_le_bytes(payload[12..16].try_into().unwrap()) as usize;
            if payload.len() < 16 + klen + dlen { return None; }
            let key = String::from_utf8(payload[16..16 + klen].to_vec()).ok()?;
            let data = payload[16 + klen..16 + klen + dlen].to_vec();
            Some(DecodedOp::Content { eid, key, data })
        }
        op_type::COMMIT => Some(DecodedOp::Commit),
        op_type::TIE_NAMED if payload.len() >= 16 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let value = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as u64;
            let himo_kind = payload[12];
            let nlen = u16::from_le_bytes(payload[14..16].try_into().unwrap()) as usize;
            if payload.len() < 16 + nlen { return None; }
            let himo_name = String::from_utf8(payload[16..16 + nlen].to_vec()).ok()?;
            Some(DecodedOp::TieNamed { eid, himo_name, himo_kind, value })
        }
        op_type::TIE_NAMED64 if payload.len() >= 20 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let value = u64::from_le_bytes(payload[8..16].try_into().unwrap());
            let himo_kind = payload[16];
            let nlen = u16::from_le_bytes(payload[18..20].try_into().unwrap()) as usize;
            if payload.len() < 20 + nlen { return None; }
            let himo_name = String::from_utf8(payload[20..20 + nlen].to_vec()).ok()?;
            Some(DecodedOp::TieNamed { eid, himo_name, himo_kind, value })
        }
        op_type::TIE_LEAF if payload.len() >= 16 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let himo_kind = payload[8];
            let nlen = u16::from_le_bytes(payload[10..12].try_into().unwrap()) as usize;
            let blen = u32::from_le_bytes(payload[12..16].try_into().unwrap()) as usize;
            if payload.len() < 16 + nlen + blen { return None; }
            let himo_name = String::from_utf8(payload[16..16 + nlen].to_vec()).ok()?;
            let bytes = payload[16 + nlen..16 + nlen + blen].to_vec();
            Some(DecodedOp::TieLeaf { eid, himo_name, himo_kind, bytes })
        }
        op_type::VOCAB if payload.len() >= 8 => {
            let vid = u32::from_le_bytes(payload[0..4].try_into().unwrap());
            let blen = u32::from_le_bytes(payload[4..8].try_into().unwrap()) as usize;
            if payload.len() < 8 + blen { return None; }
            let bytes = payload[8..8 + blen].to_vec();
            Some(DecodedOp::Vocab { vid, bytes })
        }
        op_type::TIE_REF if payload.len() >= 20 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let himo_id = u16::from_le_bytes(payload[8..10].try_into().unwrap());
            let target = u64::from_le_bytes(payload[12..20].try_into().unwrap());
            Some(DecodedOp::TieRef { eid, himo_id, target })
        }
        op_type::ALIAS if payload.len() >= 16 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let same_as = u64::from_le_bytes(payload[8..16].try_into().unwrap());
            Some(DecodedOp::Alias { eid, same_as })
        }
        _ => None,
    }
}

/// FNV-1a 32bit(v10 互換)。torn write 検出用。
#[inline]
fn fnv1a(data: &[u8]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// test 用: 読み手が新しい区画の終端を読んでから古い区画の終端を読むまでの間に 1 度だけ呼ぶ (この thread の次の読みだけ)。
#[cfg(all(test, not(target_arch = "wasm32")))]
mod read_plan_hook {
    use std::cell::RefCell;
    thread_local! {
        static HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }
    pub(super) fn set(f: Box<dyn FnOnce()>) {
        HOOK.with(|h| *h.borrow_mut() = Some(f));
    }
    pub(super) fn call() {
        if let Some(f) = HOOK.with(|h| h.borrow_mut().take()) {
            f();
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("enchudb-wal-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
        p
    }

    /// ホストの形: 切れ目の後に Commit と中継しか積んでいなければ 「中身のある自分の record は積んでいない」 (時計を今まで進め、
    /// 以後の採番はそれより後)。 中身のある自分の record を積んだら、 次の切れ目までは言わない。
    #[test]
    fn quiet_since_ignores_commits_and_relays_but_not_own_data() {
        let p = tmp("quiet_since");
        let wal = OpLog::create(&p, 1 << 20).unwrap();
        wal.set_peer_id(7);
        let cut = wal.durable_cut_for(7);
        wal.append(Op::Commit).unwrap();
        let other = Hlc { wall: 5, logical: 0, peer: 9 };
        let hdr = RelayedHeader { hlc: other, author: 9, signature: [0; 64], pubkey_fp: [0; 8] };
        wal.append_relayed(Op::Tie { eid: 1, himo_id: 0, value: 1 }, hdr).unwrap();
        let q = wal.quiet_since(cut.own_data, cut.dropped_gen, 7).expect("Commit と中継だけなら中身は積んでいない");
        assert_eq!(q.peer, 7);
        let (_, next) = wal.append_with_hlc(Op::Tie { eid: 2, himo_id: 0, value: 1 }).unwrap();
        assert!(next > q, "後の採番が言った時刻より後でない: {next:?} {q:?}");
        assert!(wal.quiet_since(cut.own_data, cut.dropped_gen, 7).is_none(), "中身のある自分の record を積んだ後に言った");
        let (_, _) = wal.append_with_hlc(Op::Commit).unwrap();
        let cut2 = wal.durable_cut_for(7);
        assert!(wal.quiet_since(cut2.own_data, cut2.dropped_gen, 7).is_some(), "次の切れ目からはまた言える");
        let _ = std::fs::remove_file(&p);
    }

    /// 印の無い DB: WAL に Commit と中継しか無ければ 「今」 を言い (読むだけの client)、 この process で中身のある自分の record を
    /// 積んだら言わない。 前の process が積んだ自分の record が WAL に残っていても言わない (開き直すと `own_data` は 0 から)。
    #[test]
    fn quiet_without_mark_sees_own_records_of_this_and_earlier_processes() {
        let p = tmp("quiet_without_mark");
        {
            let wal = OpLog::create(&p, 1 << 20).unwrap();
            wal.set_peer_id(7);
            wal.append(Op::Commit).unwrap();
            let hdr = RelayedHeader { hlc: Hlc { wall: 5, logical: 0, peer: 9 }, author: 9, signature: [0; 64], pubkey_fp: [0; 8] };
            wal.append_relayed(Op::Tie { eid: 1, himo_id: 0, value: 1 }, hdr).unwrap();
            let q = wal.quiet_without_mark(7).expect("Commit と中継だけなら読むだけの client");
            assert_eq!(q.peer, 7);
            assert!(wal.quiet_without_mark(7).is_some(), "2 回目 (読み直さない) も言う");
            assert!(wal.quiet_without_mark(9).is_none(), "7 で読んだ結果を 9 に使った (9 の record が WAL にある)");
            let (_, next) = wal.append_with_hlc(Op::Tie { eid: 2, himo_id: 0, value: 1 }).unwrap();
            assert!(next > q, "後の採番が言った時刻より後でない: {next:?} {q:?}");
            assert!(wal.quiet_without_mark(7).is_none(), "中身のある自分の record を積んだ後に言った");
            wal.append(Op::Commit).unwrap();
        }
        let wal = OpLog::open(&p).unwrap();
        wal.set_peer_id(7);
        assert!(wal.quiet_without_mark(7).is_none(), "前の process の自分の record が WAL に残っているのに言った");
        assert!(wal.quiet_without_mark(9).is_none(), "9 の record が WAL にあるのに 9 として言った");
        let _ = std::fs::remove_file(&p);
    }

    /// 満杯で落ちた自分の record は `own_data` に数えない (alloc が通った後に数える) が、 切れ目 (= 置いた印) の後に落ちていれば
    /// `quiet_since` は 「今」 を言わない (落ちた record は `_sync_ops` に無い)。 floor を書いて次の印を置いた後はまた言える
    /// (enchudb-76 のレビューで見つけた)
    #[test]
    fn quiet_since_does_not_see_a_dropped_record() {
        let p = tmp("quiet_since_dropped");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        wal.set_peer_id(7);
        let mut fell = false;
        for i in 0..4000u32 {
            if wal.append(Op::Tie { eid: 3, himo_id: 0, value: i as u64 }).is_err() {
                fell = true;
                break;
            }
        }
        assert!(fell, "前提: 満杯で落ちる");
        let _ = wal.take_dropped();                 // engine が持ち越しへ移して floor を書いた所
        let cut = wal.durable_cut_for(7);           // = 置いた印の切れ目
        assert!(!cut.dropped_pending);
        let (_, h) = wal.append_or_dropped(Op::Tie { eid: 4, himo_id: 0, value: 1 }).expect_err("前提: 満杯のまま落ちる");
        assert!(h > cut.hlc, "落ちた record の HLC は切れ目より後: {h:?} {:?}", cut.hlc);
        assert_ne!(wal.dropped_gen(), cut.dropped_gen, "落ちた record は覚えた回数には見える");
        let q = wal.quiet_since(cut.own_data, cut.dropped_gen, 7);
        assert!(q.is_none(), "落ちた record (HLC {h:?}、 _sync_ops に無い) を越えて今を言った: {q:?}");
        let _ = wal.take_dropped();
        let cut2 = wal.durable_cut_for(7);
        let q2 = wal.quiet_since(cut2.own_data, cut2.dropped_gen, 7).expect("floor を書いて次の印を置いた後はまた言える");
        assert!(q2 > h);
        let _ = std::fs::remove_file(&p);
    }

    /// #451 (段階 1): 切れ目は、 その前に載った record の HLC 以上で、 その後に採番する HLC より小さい。 満杯で落ちた record は
    /// 切れ目に見え (`dropped` の lock の下で採番して覚える)、 取り出した落ちた record の HLC は切れ目以下
    #[test]
    fn durable_cut_bounds_the_records_and_sees_dropped_ones() {
        let p = tmp("durable_cut");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        wal.set_peer_id(7);
        let (_, h1) = wal.append_with_hlc(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        let cut = wal.durable_cut_for(7);
        let (c, lap, off) = (cut.hlc, cut.lap, cut.offset);
        assert!(h1 <= c, "切れ目は前の record 以上: {h1:?} {c:?}");
        assert!(!cut.dropped_pending);
        assert_eq!(cut.dropped_gen, 0);
        assert_eq!((lap, off), (wal.lap(), wal_pos_offset(wal.head())), "切れ目の位置 = この周の終わり");
        let (_, h2) = wal.append_with_hlc(Op::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();
        assert!(h2 > c, "切れ目の後の record は切れ目より大きい: {c:?} {h2:?}");
        let mut after = Vec::new();
        wal.visit_record_headers_with_tail(Some((lap, off)), |_, h| after.push(h));
        assert_eq!(after, vec![h2], "切れ目の位置から読むと切れ目の後の record だけ");
        // 満杯にして落とす (空きの待ち手なし = その場で落ちる)
        let mut fell = 0;
        for i in 0..2000u32 {
            if wal.append(Op::Tie { eid: 3, himo_id: 0, value: i as u64 }).is_err() {
                fell += 1;
                if fell == 3 {
                    break;
                }
            }
        }
        assert_eq!(fell, 3, "前提: 満杯で落ちる");
        let cut2 = wal.durable_cut_for(7);
        let c2 = cut2.hlc;
        assert!(cut2.dropped_pending, "落ちた record は切れ目に見える");
        assert_eq!(cut2.dropped_gen, 3, "自分の落ちた record を覚えた回数");
        assert!(!wal.durable_cut_for(8).dropped_pending, "他の author の落ちた record では無い");
        let d = wal.take_dropped();
        assert!(d.iter().all(|(a, h)| *a == 7 && *h <= c2), "落ちた record の HLC は切れ目以下: {d:?} {c2:?}");
        assert!(!wal.durable_cut_for(7).dropped_pending, "取り出した後は見えない");
        assert_eq!(wal.durable_cut_for(7).dropped_gen, 3, "取り出しても回数は戻らない (置く時に切れ目と比べる)");
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
    }

    /// #451 (段階 1): header だけ読む走査は `records_with_tail` と同じ (author, HLC) の並びを返し (閉じた group + 閉じていない末尾)、
    /// CRC の合わない record に当たればそこで止まる (前の record までは返す)
    #[test]
    fn header_scan_matches_the_record_scan_and_stops_at_a_bad_crc_after_the_checkpoint() {
        let p = tmp("header_scan");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        wal.set_peer_id(3);
        for i in 0..20u64 {
            wal.append(Op::Tie { eid: i, himo_id: 0, value: i }).unwrap();
            if i % 5 == 4 {
                wal.append(Op::Commit).unwrap();
            }
        }
        let cp = wal.head();
        wal.advance_checkpoint(cp);
        // 閉じていない末尾
        let (off_tail, _) = (wal_pos_offset(wal.head()), wal.append(Op::Tie { eid: 100, himo_id: 0, value: 100 }).unwrap());
        wal.append(Op::Tie { eid: 101, himo_id: 0, value: 101 }).unwrap();
        let expect: Vec<(PeerId, Hlc)> = wal.records_with_tail().iter().map(|r| (r.author_peer, r.hlc)).collect();
        let mut got = Vec::new();
        wal.visit_record_headers_with_tail(None, |a, h| got.push((a, h)));
        assert_eq!(got.len(), 22 + 4, "Commit も数える (Tie 22 + Commit 4)");
        let mut from_tail = Vec::new();
        wal.visit_record_headers_with_tail(Some((wal.lap(), off_tail)), |a, h| from_tail.push((a, h)));
        assert_eq!(from_tail, got[24..].to_vec(), "位置を渡せばそこから");
        let mut other_lap = Vec::new();
        wal.visit_record_headers_with_tail(Some((wal.lap() + 1, off_tail)), |a, h| other_lap.push((a, h)));
        assert_eq!(other_lap, got, "周が違えば先頭から");
        let mut no_lap = Vec::new();
        wal.visit_record_headers_with_tail(Some((0, off_tail)), |a, h| no_lap.push((a, h)));
        assert_eq!(no_lap, got, "周なし (v2) は先頭から");
        let ties: Vec<(PeerId, Hlc)> = got.iter().copied().filter(|e| expect.contains(e)).collect();
        assert_eq!(ties, expect, "Tie の (author, HLC) は record の走査と同じ並び");
        // record の payload を壊す (CRC が合わない) → そこで止まる
        let mmap = wal.mmap_mut_slice();
        mmap[off_tail as usize + REC_HEADER_SIZE] ^= 0xFF;
        let mut got2 = Vec::new();
        wal.visit_record_headers_with_tail(None, |a, h| got2.push((a, h)));
        assert_eq!(got2.len(), 24, "壊れた record の手前まで");
        assert_eq!(wal.records_with_tail().len(), 20, "record の走査も同じ所で止まる (Commit は数えない)");
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn create_and_append() {
        let p = tmp("basic");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let lsn1 = wal.append(Op::Tie { eid: 1, himo_id: 0, value: 42 }).unwrap();
        let lsn2 = wal.append(Op::Untie { eid: 2, himo_id: 1 }).unwrap();
        let lsn3 = wal.append(Op::Commit).unwrap();
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// 0.12.0 (#88): Op::TieLeaf の wire payload write ↔ decode round-trip。
    #[test]
    fn tie_leaf_wire_roundtrip() {
        let name = "table.body";
        let payload = "終端ノードの本文\0bin".as_bytes(); // 非 UTF8 境界も含む binary
        let op = Op::TieLeaf { eid: 0xABCD_1234_5678, himo_name: name, himo_kind: 3, bytes: payload };
        let mut buf = vec![0u8; op.payload_size()];
        op.write_payload(&mut buf);
        match decode_op(op.op_byte(), &buf).expect("decode TieLeaf") {
            DecodedOp::TieLeaf { eid, himo_name, himo_kind, bytes } => {
                assert_eq!(eid, 0xABCD_1234_5678);
                assert_eq!(himo_name, name);
                assert_eq!(himo_kind, 3);
                assert_eq!(bytes, payload);
            }
            other => panic!("expected TieLeaf, got {:?}", other),
        }
        // 空 name / 空 bytes の edge
        let op0 = Op::TieLeaf { eid: 1, himo_name: "", himo_kind: 3, bytes: b"" };
        let mut b0 = vec![0u8; op0.payload_size()];
        op0.write_payload(&mut b0);
        assert!(matches!(decode_op(op0.op_byte(), &b0), Some(DecodedOp::TieLeaf { .. })));
        // truncated payload は None (out-of-bounds を返さない)
        assert!(decode_op(op_type::TIE_LEAF, &buf[..8]).is_none());
    }

    /// issue #57: 容量到達時、 `append` は panic せず `OutOfMemory` の `Err` を
    /// graceful に返すこと (= wal_full_err 経路)。 旧来の挙動を回帰で固定する。
    #[test]
    fn append_returns_err_when_full_not_panic() {
        let p = tmp("full_graceful");
        // HEADER_SIZE(32) + 数レコードぶんしか入らない極小 capacity。
        let wal = OpLog::create(&p, HEADER_SIZE + 256).unwrap();
        let mut hit_full = false;
        for i in 0..10_000u64 {
            match wal.append(Op::Tie { eid: i, himo_id: 0, value: i }) {
                Ok(_) => {}
                Err(e) => {
                    assert_eq!(e.kind(), io::ErrorKind::OutOfMemory, "full は OutOfMemory で返る");
                    hit_full = true;
                    break;
                }
            }
        }
        assert!(hit_full, "極小 capacity なら必ず満杯に到達して Err を返すはず");
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// issue #58②: `append_inner` が panic しても `pending_writes` が RAII ガードで
    /// 必ず減算され、 counter が +1 のまま leak しないこと。 当時は leak すると
    /// `try_reset`(pending == 0 条件)が永久に発火しなくなった (今は観測用の数)。 fault injection で
    /// deterministic に panic させて検証する。
    #[test]
    fn pending_writes_balanced_on_append_panic() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let p = tmp("panic_balance");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        assert_eq!(wal.pending_writes(), 0);

        // 次の append_inner を panic させる。 期待された panic なので、 backtrace で
        // テスト出力を汚さないよう hook を一時無効化する。
        FAULT_INJECT_APPEND_PANIC.with(|c| c.set(true));
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let r = catch_unwind(AssertUnwindSafe(|| {
            wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 })
        }));
        std::panic::set_hook(prev);
        assert!(r.is_err(), "fault injection で append は panic するはず");

        // ガードが効いていれば counter は 0 に戻っている (leak なし)。
        assert_eq!(wal.pending_writes(), 0, "panic 後も pending_writes が均衡している");

        // 後続の正常 append が通り、 pending も 0 に戻ることを確認。
        wal.append(Op::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();
        assert_eq!(wal.pending_writes(), 0);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn hlc_monotonic() {
        let p = tmp("hlc");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        wal.set_peer_id(7);
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        wal.append(Op::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();
        wal.append(Op::Commit).unwrap();
        wal.fsync().unwrap();

        let wal = OpLog::open(&p).unwrap();
        let recs = wal.recover();
        assert_eq!(recs.len(), 2);
        // HLC は単調増加
        assert!(recs[0].hlc <= recs[1].hlc);
        // peer が 7 で記録されている
        assert_eq!(recs[0].hlc.peer, 7);
        assert_eq!(recs[0].author_peer, 7);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn recover_committed_only() {
        let p = tmp("recover");
        {
            let wal = OpLog::create(&p, 1024 * 1024).unwrap();
            wal.append(Op::Tie { eid: 1, himo_id: 0, value: 100 }).unwrap();
            wal.append(Op::Tie { eid: 2, himo_id: 0, value: 200 }).unwrap();
            wal.append(Op::Commit).unwrap();
            // uncommitted batch
            wal.append(Op::Tie { eid: 3, himo_id: 0, value: 300 }).unwrap();
            wal.fsync().unwrap();
        }

        let wal = OpLog::open(&p).unwrap();
        let recs = wal.recover();
        assert_eq!(recs.len(), 2);
        match &recs[0].op {
            DecodedOp::Tie { eid, value, .. } => {
                assert_eq!(*eid, 1);
                assert_eq!(*value, 100);
            }
            _ => panic!(),
        }
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn recover_content() {
        let p = tmp("content");
        {
            let wal = OpLog::create(&p, 1024 * 1024).unwrap();
            wal.append(Op::Content { eid: 7, key: "memo", data: b"hello world" }).unwrap();
            wal.append(Op::Commit).unwrap();
            wal.fsync().unwrap();
        }
        let wal = OpLog::open(&p).unwrap();
        let recs = wal.recover();
        assert_eq!(recs.len(), 1);
        match &recs[0].op {
            DecodedOp::Content { eid, key, data } => {
                assert_eq!(*eid, 7);
                assert_eq!(key, "memo");
                assert_eq!(data, b"hello world");
            }
            _ => panic!(),
        }
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn signature_slot_is_zero() {
        // Phase A: 署名スロットは確保されるが zeros。
        let p = tmp("sig_zero");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 42 }).unwrap();
        wal.append(Op::Commit).unwrap();
        wal.fsync().unwrap();

        // WAL の最初のレコード header を直接読む
        let bytes = std::fs::read(&p).unwrap();
        // File header 32B + Record header からの signature 位置
        let sig_off = 32 + OFF_SIGNATURE;
        let sig = &bytes[sig_off..sig_off + 64];
        assert_eq!(sig, &[0u8; 64]);
        let pubkey_fp = &bytes[32 + OFF_PUBKEY_FP..32 + OFF_PUBKEY_FP + 8];
        assert_eq!(pubkey_fp, &[0u8; 8]);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn checksum_catches_torn_write() {
        let p = tmp("corrupt");
        {
            let wal = OpLog::create(&p, 1024 * 1024).unwrap();
            wal.append(Op::Tie { eid: 1, himo_id: 0, value: 100 }).unwrap();
            wal.append(Op::Commit).unwrap();
            wal.append(Op::Tie { eid: 2, himo_id: 0, value: 999 }).unwrap();
            wal.append(Op::Commit).unwrap();
            wal.fsync().unwrap();
        }

        // 2 つ目の Tie payload を壊す。
        // ファイルレイアウト: File header 32 + Tie record(112+16+8=136) + Commit(112+0+8=120) + Tie record... (+8 は周の印、 #608)
        {
            use std::io::Seek;
            use std::io::Write;
            use std::io::Read;
            use std::io::SeekFrom;
            let mut f = OpenOptions::new().read(true).write(true).open(&p).unwrap();
            // 2 つ目の Tie の payload 内の value バイトを破壊
            // 32 (file hdr) + 136 (Tie1) + 120 (Commit) + 112 (Tie2 header) + 12 (value offset in payload) = 412
            let at = HEADER_SIZE + (REC_HEADER_SIZE + 16 + LAP_TRAILER) + (REC_HEADER_SIZE + LAP_TRAILER) + REC_HEADER_SIZE + 12;
            f.seek(SeekFrom::Start(at as u64)).unwrap();
            let mut b = [0u8; 1]; f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Current(-1)).unwrap();
            f.write_all(&[b[0] ^ 0xFF]).unwrap();
        }

        let wal = OpLog::open(&p).unwrap();
        let recs = wal.recover();
        // 壊れた地点で打ち切るので、最初の Tie (commit 済み) だけ返る
        assert_eq!(recs.len(), 1);
        match &recs[0].op {
            DecodedOp::Tie { eid, value, .. } => {
                assert_eq!(*eid, 1);
                assert_eq!(*value, 100);
            }
            _ => panic!(),
        }
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn v1_file_rejected() {
        // v1 WAL file を偽造して、開けないことを確認
        let p = tmp("v1_reject");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&p).unwrap();
            let mut hdr = [0u8; 32];
            hdr[0..4].copy_from_slice(b"EWAL");
            hdr[4..8].copy_from_slice(&1u32.to_le_bytes()); // v1
            hdr[8..16].copy_from_slice(&32u64.to_le_bytes());
            hdr[16..24].copy_from_slice(&32u64.to_le_bytes());
            hdr[24..32].copy_from_slice(&1024u64.to_le_bytes());
            f.write_all(&hdr).unwrap();
            f.set_len(1024).unwrap();
        }
        assert!(OpLog::open(&p).is_err(), "v1 WAL should be rejected");
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn advance_and_reset_checkpoint() {
        let p = tmp("checkpoint");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 50 }).unwrap();
        wal.append(Op::Commit).unwrap();
        let head_after = wal.head();
        wal.advance_checkpoint(head_after);
        assert_eq!(wal.checkpoint(), head_after);

        wal.reset_to_checkpoint();
        assert_eq!(wal.head(), head_after);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn concurrent_append() {
        use std::sync::Arc;
        use std::thread;
        let p = tmp("concurrent");
        let wal = Arc::new(OpLog::create(&p, 32 * 1024 * 1024).unwrap());
        let mut handles = Vec::new();
        for t in 0..4 {
            let w = wal.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000u64 {
                    w.append(Op::Tie {
                        eid: t * 10000 + i,
                        himo_id: 0,
                        value: i,
                    }).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(wal.next_lsn(), 4001);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// #75 regression: 同一プロセス並行 append で offset 衝突 / HLC 重複が
    /// 起きないこと。旧実装は flock (同一 fd 間で no-op) + 非 CAS の head 採番
    /// だったため、2 スレッドが同じ offset に書いて record を破壊し得た。
    /// 既存の `concurrent_append` は next_lsn しか見ないので検出できなかった。
    #[test]
    fn concurrent_append_no_offset_collision_or_dup_hlc() {
        use std::sync::Arc;
        use std::thread;
        let p = tmp("concurrent_offset");
        let wal = Arc::new(OpLog::create(&p, 32 * 1024 * 1024).unwrap());
        const THREADS: u64 = 8;
        const PER: u64 = 500;
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let w = wal.clone();
            handles.push(thread::spawn(move || {
                for i in 0..PER {
                    w.append(Op::Tie {
                        eid: t * 10_000 + i,
                        himo_id: 0,
                        value: i,
                    }).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        wal.append(Op::Commit).unwrap();

        // 全 record が破壊されずに読めて、eid が完全に揃っていること
        let records = wal.iter_committed();
        assert_eq!(records.len(), (THREADS * PER) as usize,
                   "offset 衝突で record が破壊された");
        let mut eids: Vec<u64> = records.iter().map(|r| match &r.op {
            DecodedOp::Tie { eid, .. } => *eid,
            other => panic!("unexpected op: {other:?}"),
        }).collect();
        eids.sort_unstable();
        eids.dedup();
        assert_eq!(eids.len(), (THREADS * PER) as usize, "eid が欠落 / 重複");

        // HLC が全 record で一意であること (旧 next_hlc は重複を発行し得た)
        let mut hlcs: Vec<(u64, u32)> = records.iter()
            .map(|r| (r.hlc.wall, r.hlc.logical)).collect();
        hlcs.sort_unstable();
        let before = hlcs.len();
        hlcs.dedup();
        assert_eq!(hlcs.len(), before, "HLC が重複発行された");

        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// `lock_session` の後の append は record ごとに flock を取らない (取る前は record の数だけ取る = 対照)。 開いている間の
    /// lock は本当に持っている: 同じ WAL を別に開いた file (= 別の process と同じく別の lock の持ち主) からは取れず、 閉じると取れる。
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn session_lock_skips_per_record_flock_and_holds_the_file_lock() {
        let p = tmp("session_lock");
        let wal = OpLog::create(&p, 1 << 20).unwrap();
        let taken = || FLOCK_TAKEN.with(|c| c.get());
        let before = taken();
        for i in 0..10u64 {
            wal.append(Op::Tie { eid: i, himo_id: 0, value: i }).unwrap();
        }
        assert_eq!(taken() - before, 10, "対照: lock_session の前は record ごとに取る");
        assert!(!wal.session_locked());
        wal.lock_session().unwrap();
        wal.lock_session().unwrap(); // 何度呼んでもよい
        assert!(wal.session_locked());
        let before = taken();
        for i in 10..20u64 {
            wal.append(Op::Tie { eid: i, himo_id: 0, value: i }).unwrap();
        }
        wal.append_many(&[OwnedOp::Tie { eid: 20, himo_id: 0, value: 20 }, OwnedOp::Commit]).unwrap();
        assert_eq!(taken() - before, 0, "lock_session の後は取らない");
        let other = std::fs::OpenOptions::new().read(true).write(true).open(&p).unwrap();
        assert!(!crate::filelock::try_lock_exclusive(&other).unwrap(), "開いている間の lock を持っている");
        drop(wal);
        assert!(crate::filelock::try_lock_exclusive(&other).unwrap(), "閉じると外れる");
        let _ = crate::filelock::unlock(&other);
        let ties = OpLog::open(&p).unwrap().recover().iter().filter(|r| matches!(r.op, DecodedOp::Tie { .. })).count();
        assert_eq!(ties, 21);
        let _ = std::fs::remove_file(&p);
    }

    /// #561: 同じ file を開いた前の `OpLog` が開いている間の lock を持ったまま生きていても、 後から開いた `OpLog` の
    /// `lock_session` は待たずに Err を返す (前は永久に待った)。 前のが閉じれば取れる。
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn session_lock_held_by_a_live_oplog_fails_fast() {
        let p = tmp("session_held");
        let first = OpLog::create(&p, 1 << 20).unwrap();
        first.lock_session().unwrap();
        let second = OpLog::open(&p).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let second = std::sync::Arc::new(second);
        let s2 = second.clone();
        std::thread::spawn(move || {
            let _ = tx.send(s2.lock_session().map_err(|e| e.kind()));
        });
        let r = rx.recv_timeout(std::time::Duration::from_secs(5)).expect("lock_session が待ち続けた");
        assert_eq!(r, Err(io::ErrorKind::WouldBlock));
        assert!(!second.session_locked());
        drop(first);
        second.lock_session().unwrap();
        assert!(second.session_locked());
        drop(second);
        let _ = std::fs::remove_file(&p);
    }

    /// `lock_session` の後に 8 本の thread で append しても、 場所と LSN はぶつからない (process の中の直列化は `append_lock` のまま)。
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn session_lock_appends_from_threads_do_not_collide() {
        let p = tmp("session_threads");
        let wal = OpLog::create(&p, 8 << 20).unwrap();
        wal.lock_session().unwrap();
        std::thread::scope(|s| {
            for t in 0..8u64 {
                let wal = &wal;
                s.spawn(move || {
                    for i in 0..1000u64 {
                        wal.append(Op::Tie { eid: t * 1000 + i, himo_id: 0, value: i }).unwrap();
                    }
                });
            }
        });
        wal.append(Op::Commit).unwrap();
        drop(wal);
        let recs = OpLog::open(&p).unwrap().recover();
        let mut eids: Vec<u64> = recs.iter().filter_map(|r| match r.op { DecodedOp::Tie { eid, .. } => Some(eid), _ => None }).collect();
        let mut lsns: Vec<u64> = recs.iter().map(|r| r.lsn).collect();
        assert_eq!(eids.len(), 8000);
        eids.sort_unstable();
        eids.dedup();
        assert_eq!(eids.len(), 8000, "どの record も 1 回ずつ読める");
        let n = lsns.len();
        lsns.sort_unstable();
        lsns.dedup();
        assert_eq!(lsns.len(), n, "LSN が重ならない");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn multi_process_append_no_offset_collision() {
        // 2 つの OpLog インスタンスを **同じ** .wal ファイルに対して開き、
        // それぞれから append する。 flock 排他が効いていれば、 record の
        // 物理 offset は衝突せず重ならない。 1 process 内の 2 OpLog インスタンスは
        // 別 process と同じく別 process-local atomic を持つので、 flock なしだと
        // 同じ offset を奪い合って record が破壊される。
        let p = tmp("multi_proc");
        let cap = 4 * 1024 * 1024;
        let wal_a = OpLog::create(&p, cap).unwrap();
        // 同じ path を別 OpLog で再 open (= 別 process emulation)
        let wal_b = OpLog::open(&p).unwrap();

        // 交互に append、 lock が無いと head が両 instance で衝突
        for i in 0..200u32 {
            if i % 2 == 0 {
                wal_a.append(Op::Tie { eid: i as u64, himo_id: 0, value: i as u64 }).unwrap();
            } else {
                wal_b.append(Op::Tie { eid: i as u64, himo_id: 1, value: i as u64 }).unwrap();
            }
        }
        wal_a.append(Op::Commit).unwrap();
        wal_b.append(Op::Commit).unwrap();

        // どちらか経由で recover、 record が 202 個全部見える事を確認
        let wal_c = OpLog::open(&p).unwrap();
        let recs = wal_c.recover();
        let tie_count = recs.iter().filter(|r| matches!(r.op, DecodedOp::Tie { .. })).count();
        assert_eq!(tie_count, 200, "all Tie records should survive flock-serialized append, got {}", tie_count);
        // record header magic がどれも正常であることを暗黙検証 (recover 自身が壊れた
        // record を見つけたら panic / 早期 break する)
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn try_reset_recycles_wal_space() {
        let p = tmp("ring_reset");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        wal.set_auto_reset(true);
        for i in 0..100u32 {
            wal.append(Op::Tie { eid: i as u64, himo_id: 0, value: i as u64 }).unwrap();
        }
        wal.append(Op::Commit).unwrap();
        let head_before = wal.head();
        assert!(head_before > HEADER_SIZE as u64);

        assert!(!wal.try_reset(), "should not reset when checkpoint < head");

        wal.advance_checkpoint(head_before);
        assert!(wal.try_reset(), "should reset when head == checkpoint");
        assert!(wal.is_empty());
        assert_eq!(wal_pos_offset(wal.head()), HEADER_SIZE as u64);
        assert_eq!(wal_pos_offset(wal.checkpoint()), HEADER_SIZE as u64);

        let lsn = wal.append(Op::Tie { eid: 999, himo_id: 0, value: 99 }).unwrap();
        assert!(lsn > 100);

        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// 回帰 (#57 follow-up): production では `set_auto_reset(true)` を呼ばない。
    /// 旧実装は `auto_reset` ゲートで `try_reset` を常時 no-op にしていたため、
    /// ring が一度も畳まれず 16MB を使い切ると WAL full で全 append を drop していた。
    /// フラグ未設定 (= create 直後の本番初期状態) でも head==checkpoint なら
    /// reclaim されることを保証する。 このテストはゲートが復活したら fail する。
    #[test]
    fn try_reset_recycles_without_auto_reset_flag() {
        let p = tmp("ring_reset_no_flag");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        // ※ set_auto_reset(true) は意図的に呼ばない (本番と同じ初期状態)。
        for i in 0..100u32 {
            wal.append(Op::Tie { eid: i as u64, himo_id: 0, value: i as u64 }).unwrap();
        }
        wal.append(Op::Commit).unwrap();
        let head_before = wal.head();
        assert!(head_before > HEADER_SIZE as u64);

        wal.advance_checkpoint(head_before);
        assert!(
            wal.try_reset(),
            "ring must reclaim even without set_auto_reset(true)"
        );
        assert!(wal.is_empty());
        assert_eq!(wal_pos_offset(wal.head()), HEADER_SIZE as u64);
        assert_eq!(wal_pos_offset(wal.checkpoint()), HEADER_SIZE as u64);

        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn ring_buffer_long_run_does_not_exhaust() {
        let p = tmp("ring_longrun");
        // v2 はヘッダ 112B / record なので、v1 より大きい容量が必要。
        let wal = OpLog::create(&p, 512 * 1024).unwrap();
        wal.set_auto_reset(true);

        for batch in 0..50u32 {
            for i in 0..100u32 {
                let v = batch * 100 + i;
                wal.append(Op::Tie { eid: v as u64, himo_id: 0, value: v as u64 }).unwrap();
            }
            wal.append(Op::Commit).unwrap();
            wal.advance_checkpoint(wal.head());
            wal.try_reset();
        }
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn full_wal_returns_error() {
        let p = tmp("full");
        // v2: Tie 1 つ = REC_HEADER 112 + payload 16 = 128B
        // File header 32 + 128 + 閉じの Commit の空き 112 (`alloc`) + 余白だけ確保
        let wal = OpLog::create(&p, HEADER_SIZE + 128 + 112 + 50).unwrap();
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        let r = wal.append(Op::Tie { eid: 2, himo_id: 0, value: 2 });
        assert!(r.is_err());
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// group を閉じない record は閉じの Commit 1 つ分の空きを残す (`alloc`)。 data が入らなくなった後も、 開いている group を
    /// 閉じる Commit は入る (1 件でも、 Commit で終わる束でも)。 Commit で終わらない束は空きを残す。
    #[test]
    fn data_records_leave_room_for_a_closing_commit() {
        let p = tmp("commit_room");
        let wal = OpLog::create(&p, 4096).unwrap();
        while wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).is_ok() {}
        let free = wal.free_bytes();
        assert!(free >= REC_HEADER_SIZE as u64, "data で閉じの Commit の分まで埋めた (free={free})");
        assert!(!wal.append_dead(), "閉じの Commit が入るのに dead と言う");
        assert!(wal.append_many(&[OwnedOp::Tie { eid: 1, himo_id: 0, value: 1 }]).is_err(), "Commit で終わらない束が空きを使った");
        assert!(wal.append(Op::Commit).is_ok(), "data が入らなくなった後に閉じの Commit が入らない (free={free})");
        // Commit で終わる束も、 その空きを使える
        let p2 = tmp("commit_room_many");
        let wal2 = OpLog::create(&p2, 4096).unwrap();
        while wal2.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).is_ok() {}
        assert!(wal2.append_many(&[OwnedOp::Commit]).is_ok(), "Commit だけの束が入らない");
        // 対照: test 用に残さない形にすると積める
        let p3 = tmp("commit_room_fill");
        let wal3 = OpLog::create(&p3, HEADER_SIZE + 128 + 50).unwrap();
        assert!(wal3.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).is_err(), "空きを残せないのに積めた");
        wal3.keep_commit_room_for_test(false);
        assert!(wal3.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).is_ok());
        for q in [&p, &p2, &p3] {
            let _ = std::fs::remove_dir_all(q);
            let _ = std::fs::remove_file(q);
        }
    }

    // ──── request17-A3: HLC の採番責務を呼び出し側に開く ────
    //
    // engine が cell の version column と WAL record に **同じ HLC** を載せられる
    // ようにするための API 群。 ずれると peer 間で「自分が持つ版数」と「配った版数」が
    // 食い違い、 その隙間の並行 write が peer ごとに別々の勝者を選ぶ。

    /// Commit で閉じて、 Tie record だけを HLC 付きで拾う。
    fn tie_hlcs(wal: &OpLog) -> Vec<(u64, Hlc)> {
        wal.append(Op::Commit).unwrap();
        wal.iter_committed()
            .into_iter()
            .filter(|r| matches!(r.op, DecodedOp::Tie { .. }))
            .map(|r| (r.lsn, r.hlc))
            .collect()
    }

    /// #58: v3 の CRC は header の値 (op / len / lsn / hlc / author) も覆う。 旧 (payload だけ) は
    /// header が壊れても magic が残っていれば通し、 ゴミの lsn / HLC / author を recovery・bridge・
    /// audit に流した。 各 field の 1 bit を反転すると scan はその record で止まる。
    #[test]
    fn a_corrupted_header_field_stops_the_scan() {
        for (field, off) in [
            ("op", OFF_OP),
            ("lsn", OFF_LSN),
            ("hlc_wall", OFF_HLC_WALL),
            ("hlc_logical", OFF_HLC_LOGICAL),
            ("hlc_peer", OFF_HLC_PEER),
            ("author", OFF_AUTHOR_PEER),
        ] {
            let p = tmp(&format!("hdr_crc_{field}"));
            let wal = OpLog::create(&p, 1024 * 1024).unwrap();
            wal.set_peer_id(1);
            wal.append(Op::Untie { eid: 1, himo_id: 0 }).unwrap();
            wal.append(Op::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();
            wal.append(Op::Commit).unwrap();
            assert_eq!(wal.iter_committed().len(), 2, "前提");
            // 1 本目の record (file header の直後) の field を壊す。 op は Untie ↔ Delete 等の別の op に化ける
            wal.mmap_mut_slice()[HEADER_SIZE + off] ^= 0x01;
            let (recs, _, _, stop) = wal.iter_committed_from_with_diag(HEADER_SIZE as u64);
            assert!(recs.is_empty(), "{field} が壊れた record を通した: {:?}", recs.first().map(|(r, _)| (r.lsn, r.hlc, r.author_peer)));
            assert_eq!(stop.as_str(), ScanStop::BadCrc.as_str(), "{field}");
            let _ = std::fs::remove_dir_all(&p);
            let _ = std::fs::remove_file(&p);
        }
    }

    /// #58: v2 (CRC は payload だけ) の record も読める — 旧 binary が書いた WAL と、 旧版の peer が
    /// 署名した中継 record。 署名は v2 の bytes のまま検証できる (v3 として組み直さない)。
    #[test]
    fn v2_records_are_still_read_and_their_signatures_still_verify() {
        let p = tmp("v2_compat");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        wal.set_peer_id(5);
        let kp = crate::keys::Keypair::generate();
        wal.set_keypair(Some(std::sync::Arc::new(crate::keys::Keypair::from_bytes(&kp.secret_bytes()))));
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 7 }).unwrap();
        // 旧 binary が書いた形に書き換える: version 2、 CRC は payload だけ、 署名は v2 の bytes に
        {
            let m = wal.mmap_mut_slice();
            let h = HEADER_SIZE;
            let len = u32::from_le_bytes(m[h + OFF_LEN..h + OFF_LEN + 4].try_into().unwrap()) as usize;
            let payload = m[h + REC_HEADER_SIZE..h + REC_HEADER_SIZE + len].to_vec();
            m[h + OFF_VERSION] = REC_VERSION_V2;
            m[h + OFF_CRC..h + OFF_CRC + 4].copy_from_slice(&fnv1a(&payload).to_le_bytes());
            let mut sb = m[h..h + SIGNED_PAYLOAD_HEADER_SIZE].to_vec();
            sb.extend_from_slice(&payload);
            let sig = kp.sign(&sb);
            m[h + OFF_SIGNATURE..h + OFF_SIGNATURE + 64].copy_from_slice(&sig);
        }
        wal.append(Op::Tie { eid: 2, himo_id: 0, value: 8 }).unwrap(); // こちらは v3
        wal.append(Op::Commit).unwrap();
        let recs = wal.iter_committed();
        assert_eq!(recs.len(), 2, "v2 と v3 が混ざった WAL を読めない");
        assert_eq!(recs[0].signed_bytes[OFF_VERSION], REC_VERSION_V2);
        assert_eq!(recs[1].signed_bytes[OFF_VERSION], REC_VERSION);
        let keys = crate::keys::PubkeyStore::new();
        keys.force_register(5, &kp.public_bytes());
        for r in &recs {
            assert!(keys.verify(5, &r.signed_bytes, &r.signature), "署名が合わない (版 {})", r.signed_bytes[OFF_VERSION]);
        }

        // 中継 (bytes のまま): v2 も受ける。 v3 は header を壊すと弾く (旧: payload しか見ず通した)
        let q = tmp("v2_compat_relay");
        let relay = OpLog::create(&q, 1024 * 1024).unwrap();
        relay.append_relayed_verbatim(&recs[0].signed_bytes, &recs[0].signature, &recs[0].pubkey_fp).unwrap();
        let mut bad = recs[1].signed_bytes.clone();
        bad[OFF_HLC_WALL] ^= 0x01;
        assert!(relay.append_relayed_verbatim(&bad, &recs[1].signature, &recs[1].pubkey_fp).is_err());
        relay.append_relayed_verbatim(&recs[1].signed_bytes, &recs[1].signature, &recs[1].pubkey_fp).unwrap();
        relay.append(Op::Commit).unwrap();
        assert_eq!(relay.iter_committed().len(), 2);

        // 宛名の書き換え (bridge の逆写像) も版の規則で CRC を付け直す
        for r in &recs {
            let rs = resign_with_eid(r, 99, None).unwrap();
            assert_eq!(signed_bytes_crc(&rs.signed_bytes).to_le_bytes(), rs.signed_bytes[36..40]);
        }
        let rs = resign_as_tie_ref(&recs[1], 99, 100, None).unwrap();
        assert_eq!(signed_bytes_crc(&rs.signed_bytes).to_le_bytes(), rs.signed_bytes[36..40]);
        for x in [&p, &q] {
            let _ = std::fs::remove_dir_all(x);
            let _ = std::fs::remove_file(x);
        }
    }

    /// #57: 載らなかった record の author を覚える (bridge が history floor を上げる材料)。
    /// 自分の write は自分、 中継 (2 経路) は元の author。 Commit は record を運ばないので覚えない。
    #[test]
    fn a_full_wal_remembers_whose_records_were_dropped() {
        let p = tmp("dropped_authors");
        let src_p = tmp("dropped_authors_src");
        let wal = OpLog::create(&p, 4096).unwrap();
        wal.set_peer_id(1);
        let src = OpLog::create(&src_p, 1024 * 1024).unwrap();
        src.set_peer_id(7);
        src.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        let rec = tie_record(&src);
        while wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).is_ok() {}
        // data の record は閉じの Commit の分を残すので、 その空きも Commit で埋めて満杯にする (Commit は覚えない)
        while wal.append(Op::Commit).is_ok() {}
        let got = wal.take_dropped();
        assert!(got.len() == 1 && got[0].0 == 1 && got[0].1 != Hlc::ZERO, "自分の write: {got:?}");
        assert!(wal.take_dropped().is_empty(), "取り出したら空");

        // 同期 write が版数に使う HLC = 覚えた HLC
        let (_, h) = wal.append_or_dropped(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap_err();
        assert_eq!(wal.take_dropped(), vec![(1, h)], "返した HLC と覚えた HLC が違う");

        assert!(wal.append(Op::Commit).is_err(), "前提: 満杯");
        assert!(wal.take_dropped().is_empty(), "Commit は覚えない");

        let hdr = RelayedHeader { hlc: rec.hlc, author: 7, signature: rec.signature, pubkey_fp: rec.pubkey_fp };
        assert!(wal.append_relayed(Op::Tie { eid: 1, himo_id: 0, value: 1 }, hdr).is_err());
        assert_eq!(wal.take_dropped(), vec![(7, rec.hlc)], "中継 (再 encode) は元の author と HLC");

        assert!(wal.append_relayed_verbatim(&rec.signed_bytes, &rec.signature, &rec.pubkey_fp).is_err());
        assert_eq!(wal.take_dropped(), vec![(7, rec.hlc)], "中継 (bytes のまま) は元の author と HLC");
        // 壊れた bytes の拒否は載らなかった record ではない
        let mut bad = rec.signed_bytes.clone();
        bad[0] ^= 0xff;
        assert!(wal.append_relayed_verbatim(&bad, &rec.signature, &rec.pubkey_fp).is_err());
        assert!(wal.take_dropped().is_empty(), "壊れた bytes の拒否");

        let (h1, h2) = (wal.mint_hlc(), wal.mint_hlc());
        let batch = [OwnedOp::Tie { eid: 1, himo_id: 0, value: 1 }, OwnedOp::Commit, OwnedOp::Tie { eid: 2, himo_id: 0, value: 1 }];
        let h3 = wal.mint_hlc();
        assert!(wal.append_many_with_hlcs(&batch, &[h1, h3, h2]).is_err());
        assert_eq!(wal.take_dropped(), vec![(1, h2)], "まとめた append は record の HLC の max (Commit は除く)");
        assert!(wal.append_many(&[OwnedOp::Commit]).is_err());
        assert!(wal.take_dropped().is_empty(), "Commit だけの束");
        for q in [&p, &src_p] {
            let _ = std::fs::remove_dir_all(q);
            let _ = std::fs::remove_file(q);
        }
    }

    fn tie_record(wal: &OpLog) -> Record {
        wal.append(Op::Commit).unwrap();
        wal.iter_committed().into_iter().find(|r| matches!(r.op, DecodedOp::Tie { .. })).unwrap()
    }

    #[test]
    fn append_with_hlc_returns_exactly_what_the_record_carries() {
        let p = tmp("append_with_hlc");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let (lsn1, h1) = wal.append_with_hlc(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        let (lsn2, h2) = wal.append_with_hlc(Op::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();
        assert!(h1 < h2, "連続 append で HLC が単調増加していない: {h1:?} → {h2:?}");

        let on_disk = tie_hlcs(&wal);
        assert_eq!(on_disk, vec![(lsn1, h1), (lsn2, h2)], "返した HLC が record と違う");
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// 事前採番した HLC は **そのまま** record に載る (append 側で採番し直さない)。
    #[test]
    fn append_at_hlc_uses_the_minted_hlc_verbatim() {
        let p = tmp("append_at_hlc");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let minted1 = wal.mint_hlc();
        let minted2 = wal.mint_hlc();
        assert!(minted1 < minted2, "mint_hlc が単調増加していない");

        // 採番と逆順に append しても、 record が持つのは採番時の HLC。
        let lsn2 = wal.append_at_hlc(Op::Tie { eid: 2, himo_id: 0, value: 2 }, minted2).unwrap();
        let lsn1 = wal.append_at_hlc(Op::Tie { eid: 1, himo_id: 0, value: 1 }, minted1).unwrap();

        assert_eq!(tie_hlcs(&wal), vec![(lsn2, minted2), (lsn1, minted1)]);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// u32 に入らない値の Tie / TieNamed (FILE_VERSION 11) は TIE64 / TIE_NAMED64 で載り、 u64 のまま
    /// 読み戻せる。 入る値は従来の TIE / TIE_NAMED (16B) のまま。
    #[test]
    fn wide_tie_values_round_trip() {
        let p = tmp("tie64");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let vals = [7u64, u32::MAX as u64, u32::MAX as u64 + 1, 1 << 40, u64::MAX - 1];
        for (i, &v) in vals.iter().enumerate() {
            wal.append(Op::Tie { eid: i as u64, himo_id: 3, value: v }).unwrap();
            wal.append(Op::TieNamed { eid: i as u64, himo_name: "_c_big", himo_kind: 4, value: v }).unwrap();
        }
        wal.append(Op::Commit).unwrap();
        let recs = wal.iter_committed();
        let ties: Vec<u64> = recs.iter().filter_map(|r| match r.op { DecodedOp::Tie { value, .. } => Some(value), _ => None }).collect();
        let named: Vec<u64> = recs.iter().filter_map(|r| match r.op { DecodedOp::TieNamed { value, .. } => Some(value), _ => None }).collect();
        assert_eq!(ties, vals);
        assert_eq!(named, vals);
        assert_eq!(Op::Tie { eid: 0, himo_id: 0, value: u32::MAX as u64 }.op_byte(), op_type::TIE, "入る値は従来の形式");
        assert_eq!(Op::Tie { eid: 0, himo_id: 0, value: u32::MAX as u64 + 1 }.op_byte(), op_type::TIE64);
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
    }

    /// 並行した採番 (同期経路の `append` と async 経路の `mint_hlc` が混ざる) でも HLC は重複しない。
    /// 同じ HLC の record は relay の (peer, hlc) の重複除去で落ち、 `hlc > cursor` の cursor でも
    /// 読み飛ばされる。
    #[test]
    fn concurrent_hlcs_are_unique() {
        let p = tmp("concurrent_hlcs");
        let wal = std::sync::Arc::new(OpLog::create(&p, 64 * 1024 * 1024).unwrap());
        let hs: Vec<_> = (0..8u64)
            .map(|t| {
                let w = wal.clone();
                std::thread::spawn(move || {
                    (0..20_000u64)
                        .map(|i| {
                            if t % 2 == 0 {
                                w.mint_hlc()
                            } else {
                                w.append_with_hlc(Op::Tie { eid: i, himo_id: 0, value: 1 }).unwrap().1
                            }
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut all: Vec<_> = hs.into_iter().flat_map(|h| h.join().unwrap()).map(|h| h.cmp_key()).collect();
        let n = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), n, "同じ HLC が {} 回払い出された", n - all.len());
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
    }

    /// Apple の aarch64 で、 控えた時刻に CPU の counter の進みを足して出す時刻 (`apple_wall_clock`) が、 その都度読む時刻と
    /// ずれない。 1 ms ごとの読み直しをまたぐように、 ときどき寝る。
    #[cfg(all(target_arch = "aarch64", target_vendor = "apple", not(miri)))]
    #[test]
    fn counted_wall_clock_tracks_the_system_clock() {
        for i in 0..20_000u32 {
            if i % 1000 == 999 {
                std::thread::sleep(std::time::Duration::from_micros(1500));
            }
            let before = system_wall_ns();
            let got = apple_wall_clock::now_ns();
            let after = system_wall_ns();
            // 先には進まない (時刻を合わせる遅れ・進みの分だけ許す)。 遅れは、 控えの組を読む間 (10 µs 未満) だけ
            assert!(got <= after + 2_000, "{i} 回目: 読んだ時刻より {} ns 先", got - after);
            assert!(got + 20_000 >= before, "{i} 回目: 読んだ時刻より {} ns 遅れた", before - got);
        }
    }

    #[test]
    fn append_many_with_hlcs_uses_the_given_hlcs() {
        let p = tmp("append_many_hlcs");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let batch = vec![
            OwnedOp::Tie { eid: 1, himo_id: 0, value: 1 },
            OwnedOp::Tie { eid: 2, himo_id: 0, value: 2 },
        ];
        let minted: Vec<Hlc> = (0..2).map(|_| wal.mint_hlc()).collect();
        let lsns = wal.append_many_with_hlcs(&batch, &minted).unwrap();

        let on_disk = tie_hlcs(&wal);
        assert_eq!(
            on_disk,
            vec![(lsns[0], minted[0]), (lsns[1], minted[1])],
            "batch append が HLC を採番し直している",
        );
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// HLC 無し (従来の `append_many`) は今までどおり自前で採番し、 単調増加する。
    #[test]
    fn append_many_still_mints_monotonic_hlcs_without_override() {
        let p = tmp("append_many_mint");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let batch = vec![
            OwnedOp::Tie { eid: 1, himo_id: 0, value: 1 },
            OwnedOp::Tie { eid: 2, himo_id: 0, value: 2 },
        ];
        wal.append_many(&batch).unwrap();

        let on_disk = tie_hlcs(&wal);
        assert_eq!(on_disk.len(), 2);
        assert!(on_disk[0].1 < on_disk[1].1, "batch 内で HLC が単調増加していない");
        assert_ne!(on_disk[0].1, Hlc::ZERO, "HLC が採番されていない");
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    #[should_panic(expected = "数が違う")]
    fn append_many_with_hlcs_rejects_length_mismatch() {
        let p = tmp("append_many_mismatch");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let batch = vec![OwnedOp::Tie { eid: 1, himo_id: 0, value: 1 }];
        let _ = wal.append_many_with_hlcs(&batch, &[]);
    }

    /// #268: 「bridge が 0 を返し続ける」 の原因は 2 つに割れる — cursor 位置の
    /// record が読めないのか、 record は在るが Commit が付いていないのか。
    /// 従来はどちらも 「0 件」 としか見えず、 実機 26.4 時間の停止を後から特定
    /// できなかった。 診断値がその 2 つを名指しで分けることを固定する。
    /// #178: Alias は書いて読み戻せる (2 つの世界番号)、 op の種類は読み飛ばしてよい印付き、 宛名の書き戻し (`resign_with_eid`)
    /// をしない。
    #[test]
    fn alias_round_trips_and_is_skippable() {
        assert!(op_type::skippable(op_type::ALIAS));
        let p = tmp("alias-rt");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let (eid, same_as) = (crate::make_eid(2, 9), crate::make_eid(1, 4));
        wal.append(Op::Alias { eid, same_as }).unwrap();
        wal.append(Op::Commit).unwrap();
        let recs = wal.recover();
        assert_eq!(recs.len(), 1);
        match recs[0].op {
            DecodedOp::Alias { eid: e, same_as: s } => assert_eq!((e, s), (eid, same_as)),
            ref o => panic!("Alias でない: {o:?}"),
        }
        assert!(resign_with_eid(&recs[0], crate::make_eid(3, 3), None).is_none(), "Alias の宛名を書き戻した");
        let _ = std::fs::remove_file(&p);
    }

    /// #178: 知らない op は、 上の bit (`op_type::SKIPPABLE`) があればその record だけ読み飛ばして後ろを読む (数える)。
    /// bit が無ければ今まで通り scan を打ち切る (後ろの record は出ない)。 3 つの Tie の真ん中の op の種類を書き換え、
    /// CRC を付け直して開き直す。
    #[test]
    fn unknown_skippable_op_is_skipped_but_unknown_critical_op_stops() {
        for (op_byte, want, stop, skipped) in [
            (op_type::SKIPPABLE | 0x7f, vec![1u64, 3], ScanStop::Head, 1u64),
            (0x41, vec![1u64], ScanStop::UndecodableOp, 0u64),
        ] {
            let p = tmp(&format!("skippable-{op_byte}"));
            {
                let wal = OpLog::create(&p, 1024 * 1024).unwrap();
                for eid in 1..=3u64 {
                    wal.append(Op::Tie { eid, himo_id: 0, value: 9 }).unwrap();
                }
                wal.append(Op::Commit).unwrap();
            }
            // 2 つ目の record (Tie の payload は 16 byte) の op の種類を書き換えて CRC を付け直す
            let mut bytes = std::fs::read(&p).unwrap();
            let at = HEADER_SIZE + REC_HEADER_SIZE + 16 + LAP_TRAILER;
            let h = &bytes[at..at + REC_HEADER_SIZE];
            let rd8 = |o: usize| u64::from_le_bytes(h[o..o + 8].try_into().unwrap());
            let rd4 = |o: usize| u32::from_le_bytes(h[o..o + 4].try_into().unwrap());
            let (len, lsn, author) = (rd4(OFF_LEN), rd8(OFF_LSN), rd4(OFF_AUTHOR_PEER));
            let hlc = Hlc { wall: rd8(OFF_HLC_WALL), logical: rd4(OFF_HLC_LOGICAL), peer: rd4(OFF_HLC_PEER) };
            let payload = bytes[at + REC_HEADER_SIZE..at + REC_HEADER_SIZE + len as usize].to_vec();
            let crc = new_record_crc(op_byte, len, lsn, hlc, author, &payload);
            bytes[at + OFF_OP] = op_byte;
            bytes[at + OFF_CRC..at + OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
            std::fs::write(&p, &bytes).unwrap();

            let wal = OpLog::open(&p).unwrap();
            let (out, _, _, got_stop) = wal.iter_committed_from_with_diag(HEADER_SIZE as u64);
            let eids: Vec<u64> = out
                .iter()
                .map(|(r, _)| match r.op {
                    DecodedOp::Tie { eid, .. } => eid,
                    ref o => panic!("Tie 以外が出た: {o:?}"),
                })
                .collect();
            if stop == ScanStop::Head {
                assert_eq!(eids, want, "op {op_byte:#x}: 読み飛ばした後ろの record が出ない");
            } else {
                // 打ち切ると Commit に届かないので、 閉じた group として出るものは無い
                assert!(eids.is_empty(), "op {op_byte:#x}: 打ち切ったのに record が出た: {eids:?}");
                let tail: Vec<u64> = wal
                    .recover_with_tail()
                    .iter()
                    .map(|r| match r.op {
                        DecodedOp::Tie { eid, .. } => eid,
                        ref o => panic!("Tie 以外が出た: {o:?}"),
                    })
                    .collect();
                assert_eq!(tail, want, "op {op_byte:#x}: 打ち切りより前の record");
            }
            assert_eq!(got_stop, stop, "op {op_byte:#x}");
            assert!(wal.skipped_ops() >= skipped && (skipped > 0) == (wal.skipped_ops() > 0), "op {op_byte:#x}: 読み飛ばした数 {}", wal.skipped_ops());
            let _ = std::fs::remove_file(&p);
        }
    }

    #[test]
    fn scan_diag_separates_unclosed_group_from_unreadable_cursor() {
        let p = tmp("scan-diag");
        let start = HEADER_SIZE as u64;
        {
            let wal = OpLog::create(&p, 1024 * 1024).unwrap();
            wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
            wal.append(Op::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();

            // ① Commit が付いていない = record は読めているが閉じていない
            let (out, end, pending, stop) = wal.iter_committed_from_with_diag(start);
            assert!(out.is_empty(), "未 commit の record が commit 済みとして出ている");
            assert_eq!(end, start, "commit されていないのに cursor が進んでいる");
            assert_eq!(pending, 2, "未 commit 件数が観測できない");
            assert_eq!(stop, ScanStop::Head, "打ち切りではなく head まで読んだはず");

            // 閉じれば同じ cursor から一括で出る (= 実機の 「復帰の瞬間に全部出る」)
            wal.append(Op::Commit).unwrap();
            let (out, end, pending, stop) = wal.iter_committed_from_with_diag(start);
            assert_eq!(out.len(), 2, "Commit 後も record が出てこない");
            assert!(end > start, "Commit を読んでも cursor 終端が進まない");
            assert_eq!(pending, 0, "閉じたのに未 commit 件数が残っている");
            assert_eq!(stop, ScanStop::Head);
        }

        // ② cursor 位置の record が読めない = 打ち切り。 pending は 0 のまま。
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&p).unwrap();
            f.seek(SeekFrom::Start(start)).unwrap();
            f.write_all(b"??").unwrap(); // record magic を潰す
            f.flush().unwrap();
        }
        let wal = OpLog::open(&p).unwrap();
        let (out, end, pending, stop) = wal.iter_committed_from_with_diag(start);
        assert!(out.is_empty());
        assert_eq!(end, start, "読めない位置を越えて cursor が進んでいる");
        assert_eq!(pending, 0, "1 件も読めていないのに未 commit 件数が立っている");
        assert_eq!(stop, ScanStop::BadMagic, "打ち切り理由が名指しされていない");

        let _ = std::fs::remove_file(&p);
    }

    /// #268: `append_dead()` は **実際の採番規則と同じ値**で判定しなければならない。
    ///
    /// 採番は `max(on-disk head, in-memory head)` を使うので、 別 fd が append した
    /// 後の handle は 「in-memory head はまだ小さいが、 append は満杯で必ず失敗する」
    /// 状態になる。 ここで `append_dead()` が false を返すと、 host の tripwire
    /// (`wal_append_dead`) も fold の死区間例外 (= 唯一の出口) も揃って黙る。
    #[test]
    fn append_dead_agrees_with_a_real_append_failure_after_another_fd_filled_it() {
        let p = tmp("append-dead-sync");
        let cap = 1024;
        let writer = OpLog::create(&p, cap).unwrap();
        // 別 fd (= 別 open file description)。 この時点の in-memory head は HEADER_SIZE。
        let stale = OpLog::open(&p).unwrap();
        assert_eq!(wal_pos_offset(stale.head()), HEADER_SIZE as u64);

        while writer.append(Op::Commit).is_ok() {}
        assert!(writer.append_dead(), "満杯にした本人が dead と言っていない");

        assert_eq!(
            wal_pos_offset(stale.head()),
            HEADER_SIZE as u64,
            "前提が崩れている: stale handle の in-memory head が更新されている",
        );
        assert!(
            stale.append(Op::Commit).is_err(),
            "前提が崩れている: 満杯の WAL に append できてしまった",
        );
        assert!(
            stale.append_dead(),
            "append は必ず失敗するのに append_dead() が false (in-memory head だけを見ている)",
        );
        assert_eq!(
            stale.free_bytes(),
            writer.free_bytes(),
            "同じ file なのに残量の答えが handle ごとに違う",
        );
        assert!(
            stale.free_bytes() < REC_HEADER_SIZE as u64,
            "record 1 本も入らない残量なのに free_bytes がそう言っていない ({})",
            stale.free_bytes(),
        );

        let _ = std::fs::remove_file(&p);
    }

    /// #268: fault injection は **Commit だけ**を落とす (通常 record は素通し)。
    #[test]
    fn commit_fault_injection_fails_only_commits() {
        let p = tmp("commit-fault");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        wal.fail_next_commits(1);
        assert!(wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).is_ok(), "通常 record まで落ちている");
        assert!(wal.append(Op::Commit).is_err(), "Commit が落ちていない");
        assert!(wal.append(Op::Commit).is_ok(), "残数 0 になっても落ち続けている");
        let _ = std::fs::remove_file(&p);
    }

    /// #552 の残り: 満杯で載らなかった後、 空きを見る前に他の thread が畳んで空いていたら、 書き直す (前は入口で空きを見て
    /// 「満杯でない失敗」 と読み、 落としていた。 書き手 64 本の試験で CI の debug で 1 件落ちた)。 満杯でない失敗 (fault
    /// injection) は今まで通り書き直さない。
    #[test]
    fn room_freed_before_waiting_is_retried_but_other_failures_are_not() {
        let p = tmp("room_freed");
        let wal = OpLog::create(&p, 8192).unwrap();
        let size = REC_HEADER_SIZE as u64 + 16;
        let full = loop {
            match wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }) {
                Ok(_) => continue,
                Err(e) => break e,
            }
        };
        assert_eq!(full.kind(), io::ErrorKind::OutOfMemory, "前提: 満杯で載らない");
        // 載らなかった後に他の thread が畳んだ
        wal.checkpoint_and_reset(|_| true, |_| true);
        assert!(wal.free_bytes() >= size, "前提: 畳んで空いた");
        assert!(wal.wait_for_room(&full, size), "空いているのに書き直さない (満杯でない失敗と読んだ)");
        assert!(!wal.wait_for_room(&io::Error::other("injected"), size), "満杯でない失敗を書き直す");
        let _ = std::fs::remove_file(&p);
    }

    /// #552: 待ち手が空けた後、 書き直す前に他の書き手が空きを埋めても、 また待って載せる (前は 1 回しか書き直さず、 畳めて
    /// いるのに落とした)。 append / append_many / append_relayed_verbatim の 3 つの道で。
    #[test]
    fn append_waits_again_when_another_writer_takes_the_room() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicU32};
        fn fill(w: &OpLog) {
            while w.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).is_ok() {}
        }
        let src_p = tmp("room_again_src");
        let src = OpLog::create(&src_p, 1024 * 1024).unwrap();
        src.append(Op::Tie { eid: 3, himo_id: 0, value: 3 }).unwrap();
        src.append(Op::Commit).unwrap();
        let relayed = src.iter_committed().remove(0);
        type Append<'a> = &'a dyn Fn(&OpLog) -> bool;
        let paths: [(&str, Append); 3] = [
            ("append", &|w| w.append(Op::Tie { eid: 2, himo_id: 0, value: 2 }).is_ok()),
            ("append_many", &|w| w.append_many(&[OwnedOp::Tie { eid: 2, himo_id: 0, value: 2 }]).is_ok()),
            ("verbatim", &|w| {
                w.append_relayed_verbatim(&relayed.signed_bytes, &relayed.signature, &relayed.pubkey_fp).is_ok()
            }),
        ];
        for (name, append) in paths {
            let p = tmp(&format!("room_again_{name}"));
            let wal = Arc::new(OpLog::create(&p, 8192).unwrap());
            fill(&wal);
            let calls = Arc::new(AtomicU32::new(0));
            let filling = Arc::new(AtomicBool::new(false));
            let (weak, c, f) = (Arc::downgrade(&wal), calls.clone(), filling.clone());
            wal.set_room_waiter(Some(Arc::new(move |size| {
                // 他の書き手が埋めている最中の満杯は空けない (その書き手は落ちる)
                if f.load(Ordering::Acquire) {
                    return false;
                }
                let w = weak.upgrade().unwrap();
                let n = c.fetch_add(1, Ordering::AcqRel);
                // engine の consumer と同じく、 書き出して畳む
                w.checkpoint_and_reset(|_| true, |_| true);
                let room = w.free_bytes() >= size;
                if n == 0 {
                    // 空いたと答えた後、 呼び手が書き直す前に他の書き手が空きを埋める
                    f.store(true, Ordering::Release);
                    fill(&w);
                    f.store(false, Ordering::Release);
                }
                room
            })));
            assert!(append(&wal), "{name}: 空きを他の書き手に取られた後、 待ち直さずに落とした");
            assert_eq!(calls.load(Ordering::Acquire), 2, "{name}: 前提: 1 回目に空けた空きを他の書き手が埋め、 2 回目に空けた");
            let _ = std::fs::remove_file(&p);
        }
        let _ = std::fs::remove_file(&src_p);
    }
    /// #608: 畳むと周が 1 つ進み、 header (OFF_LAP) と、 その後の record の周の印に載る。 作った時は 1 周目。 開き直すと
    /// header から戻る。
    #[test]
    fn fold_advances_the_lap_and_stamps_records_with_it() {
        let p = tmp("lap-fold");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        assert_eq!((wal.lap(), wal.file_version()), (1, OPLOG_FILE_VERSION));
        let header = |w: &OpLog| {
            let m = w.mmap_slice();
            (
                u32::from_le_bytes(m[4..8].try_into().unwrap()),
                u32::from_le_bytes(m[OFF_CAPACITY..OFF_CAPACITY + 4].try_into().unwrap()),
                u32::from_le_bytes(m[OFF_LAP..OFF_LAP + 4].try_into().unwrap()),
            )
        };
        let first_trailer = |w: &OpLog| {
            let m = w.mmap_slice();
            let len = u32::from_le_bytes(m[HEADER_SIZE + OFF_LEN..HEADER_SIZE + OFF_LEN + 4].try_into().unwrap()) as usize;
            let t = HEADER_SIZE + REC_HEADER_SIZE + len;
            (u32::from_le_bytes(m[t..t + 4].try_into().unwrap()), u32::from_le_bytes(m[t + 4..t + 8].try_into().unwrap()))
        };
        assert_eq!(header(&wal), (OPLOG_FILE_VERSION, 32 * 1024, 1), "2 区画 (A の header の capacity は区画の大きさ)");
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        wal.append(Op::Commit).unwrap();
        assert_eq!(first_trailer(&wal), (1, 1 ^ LAP_SALT), "1 周目の印");
        assert_eq!(wal.record_size_of(16), REC_HEADER_SIZE + 16 + LAP_TRAILER);
        assert_eq!(wal.head(), wal_pos(1, (HEADER_SIZE + 2 * REC_HEADER_SIZE + 16 + 2 * LAP_TRAILER) as u64), "印の分だけ進む");
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset());
        assert_eq!((wal.lap(), header(&wal).2), (2, 2));
        wal.append(Op::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();
        wal.append(Op::Commit).unwrap();
        assert_eq!(first_trailer(&wal), (2, 2 ^ LAP_SALT), "2 周目の印");
        drop(wal);
        let wal = OpLog::open(&p).unwrap();
        assert_eq!((wal.lap(), wal.file_version(), wal.capacity()), (2, OPLOG_FILE_VERSION, 64 * 1024));
        let recs = wal.recover_with_tail();
        assert_eq!(recs.len(), 1);
        assert!(matches!(recs[0].op, DecodedOp::Tie { eid: 2, .. }), "{:?}", recs[0].op);
        let _ = std::fs::remove_file(&p);
    }

    /// #608 の像を手で作る: 前の周の record を ring に残したまま畳んで新しい周を 1 group 書き、 header の head だけ前の周の
    /// 終わりまで進める (OS が header の page を data の page より先に書き出した像)。 record の大きさがそろっているので前の
    /// 周の 2 group 目からは CRC の合う続きとして読めるが、 周の印が違うので止まる (印を見なければ前の周の 35 件が出る)。
    #[test]
    fn records_of_a_previous_lap_left_in_the_ring_stop_the_scan() {
        let p = tmp("lap-stale");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        for i in 0..40u64 {
            wal.append(Op::Tie { eid: 1, himo_id: 0, value: i }).unwrap();
            if i % 5 == 4 {
                wal.append(Op::Commit).unwrap();
            }
        }
        let old_head = wal.head();
        wal.advance_checkpoint(old_head);
        assert!(wal.try_reset());
        for i in 100..105u64 {
            wal.append(Op::Tie { eid: 1, himo_id: 0, value: i }).unwrap();
        }
        wal.append(Op::Commit).unwrap();
        let old_head = wal_pos_offset(old_head);
        let new_head = wal_pos_offset(wal.head());
        assert_eq!(new_head, (HEADER_SIZE + 5 * (REC_HEADER_SIZE + 16 + LAP_TRAILER) + REC_HEADER_SIZE + LAP_TRAILER) as u64);
        // 前の周の 2 group 目の先頭と new_head がそろっている (= 前提: 印が無ければ続きとして読める)
        {
            let m = wal.mmap_slice();
            assert_eq!(&m[new_head as usize..new_head as usize + 2], REC_MAGIC, "前提: 前の周の record が続きの位置に残っている");
        }
        wal.mmap_mut_slice()[8..16].copy_from_slice(&old_head.to_le_bytes());
        wal.fsync().unwrap();
        drop(wal);
        let wal = OpLog::open(&p).unwrap();
        assert_eq!(wal_pos_offset(wal.head()), old_head);
        let (out, _, _, stop) = wal.iter_committed_from_with_diag(HEADER_SIZE as u64);
        let values: Vec<u64> = out
            .iter()
            .map(|(r, _)| match r.op {
                DecodedOp::Tie { value, .. } => value,
                ref o => panic!("Tie 以外: {o:?}"),
            })
            .collect();
        assert_eq!(values, vec![100, 101, 102, 103, 104], "前の周の record が新しい周の続きとして読まれた");
        assert_eq!(stop, ScanStop::StaleLap);
        assert_eq!(wal.recover_with_tail().len(), 5, "再生も新しい周だけ");
        let _ = std::fs::remove_file(&p);
    }

    /// v2 (周の印なし) の file は読めて、 最初に畳む時に v3 になる (header の version / capacity / 周、 以後の record に印)。
    /// 畳む前の record は印なしで読み、 畳んだ後の record は印つきで読む。
    #[test]
    fn v2_file_reads_without_lap_marks_and_becomes_v3_at_the_first_fold() {
        let p = tmp("lap-v2");
        let cap = 64 * 1024u64;
        {
            // 旧 binary が作った形: header は v2 (capacity は u64)、 record に印なし
            let wal = OpLog::create(&p, cap as usize).unwrap();
            {
                let m = wal.mmap_mut_slice();
                m[4..8].copy_from_slice(&OPLOG_FILE_VERSION_V2.to_le_bytes());
                m[24..32].copy_from_slice(&cap.to_le_bytes());
            }
            wal.trailer.store(0, Ordering::Release);
            // 1 区画 (file 全体)、 周なし
            wal.two.store(false, Ordering::Release);
            wal.hot.halves[0].size.store(cap as u32, Ordering::Release);
            wal.hot.halves[1].size.store(0, Ordering::Release);
            wal.hot.halves[0].lap.store(0, Ordering::Release);
            wal.commit_room.store(REC_HEADER_SIZE as u64, Ordering::Relaxed);
            for i in 0..3u64 {
                wal.append(Op::Tie { eid: 1, himo_id: 0, value: i }).unwrap();
            }
            wal.append(Op::Commit).unwrap();
            assert_eq!(wal.head(), (HEADER_SIZE + 3 * (REC_HEADER_SIZE + 16) + REC_HEADER_SIZE) as u64, "印なしの大きさ");
            wal.fsync().unwrap();
        }
        let wal = OpLog::open(&p).unwrap();
        assert_eq!((wal.file_version(), wal.lap(), wal.capacity()), (OPLOG_FILE_VERSION_V2, 0, cap));
        assert_eq!(wal.record_size_of(16), REC_HEADER_SIZE + 16, "v2 のうちは印なし");
        let recs = wal.recover_with_tail();
        assert_eq!(recs.len(), 3, "v2 の record が読める");
        // 畳む → v3 → 2 区画にできるので v4 (周は開いた時の周 0 の 16 先)
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset());
        let lap = LAP_SKIP_ON_OPEN;
        assert_eq!((wal.file_version(), wal.lap(), wal.capacity()), (OPLOG_FILE_VERSION, lap, cap));
        {
            let m = wal.mmap_slice();
            assert_eq!(u32::from_le_bytes(m[4..8].try_into().unwrap()), OPLOG_FILE_VERSION);
            assert_eq!(u32::from_le_bytes(m[OFF_CAPACITY..OFF_CAPACITY + 4].try_into().unwrap()) as u64, cap / 2);
            assert_eq!(u32::from_le_bytes(m[OFF_LAP..OFF_LAP + 4].try_into().unwrap()), lap);
        }
        assert_eq!(wal.record_size_of(16), REC_HEADER_SIZE + 16 + LAP_TRAILER, "v3 になったら印つき");
        assert_eq!(wal.commit_room.load(Ordering::Relaxed), (REC_HEADER_SIZE + LAP_TRAILER) as u64);
        wal.append(Op::Tie { eid: 2, himo_id: 0, value: 9 }).unwrap();
        wal.append(Op::Commit).unwrap();
        wal.fsync().unwrap();
        drop(wal);
        let wal = OpLog::open(&p).unwrap();
        assert_eq!((wal.file_version(), wal.lap(), wal.capacity()), (OPLOG_FILE_VERSION, lap, cap));
        let recs = wal.recover_with_tail();
        assert_eq!(recs.len(), 1);
        assert!(matches!(recs[0].op, DecodedOp::Tie { eid: 2, .. }));
        let _ = std::fs::remove_file(&p);
    }

    /// 4 GiB 以上の WAL は作れない (v3 の header は capacity を u32 で持つ)。
    #[test]
    fn capacity_at_or_above_4gib_is_rejected() {
        let p = tmp("lap-cap");
        let e = match OpLog::create(&p, u32::MAX as usize) {
            Err(e) => e,
            Ok(_) => panic!("4 GiB の WAL が作れてしまった"),
        };
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert!(!p.exists(), "file を作る前に断る");
    }
    /// #506: 置き場の印が PENDING の数 (試験用)。
    fn pending_slots(wal: &OpLog) -> usize {
        wal.combine_slots.iter().filter(|s| s.state.load(Ordering::Acquire) == SLOT_PENDING).count()
    }

    fn wait_pending(wal: &OpLog, n: usize) {
        let t0 = std::time::Instant::now();
        while pending_slots(wal) < n {
            assert!(t0.elapsed() < std::time::Duration::from_secs(60), "頼みが置き場に置かれない ({} / {n})", pending_slots(wal));
            std::thread::yield_now();
        }
    }

    /// #506 まとめ書き: lock を持つ書き手は、 lock を待つ書き手が置き場に置いた頼みを積み、 それぞれに自分の record の (LSN, HLC) を
    /// 返す。 頼みの中身 (借用した bytes、 事前採番の HLC、 中継の header) も、 積む書き手が頼んだ書き手の stack から読む。
    #[test]
    fn holder_appends_requests_left_in_slots() {
        let p = tmp("combine_slots");
        let wal = OpLog::create(&p, 4 << 20).unwrap();
        wal.set_peer_id(1);
        let data = vec![7u8; 300];
        let pre = Hlc { wall: 1, logical: 5, peer: 1 };
        let relay = RelayedHeader { hlc: Hlc { wall: 2, logical: 0, peer: 9 }, author: 9, signature: [3; 64], pubkey_fp: [4; 8] };
        let (wal, data) = (&wal, &data);
        let got: Vec<(u64, Hlc)> = std::thread::scope(|s| {
            let held = wal.append_lock.lock();
            let hs = vec![
                s.spawn(move || wal.append_with_hlc(Op::Tie { eid: 1, himo_id: 2, value: 3 })),
                s.spawn(move || wal.append_with_hlc(Op::Content { eid: 2, key: "k", data })),
                s.spawn(move || wal.append_at_hlc(Op::Delete { eid: 3 }, pre).map(|l| (l, pre))),
                s.spawn(move || wal.append_relayed(Op::Untie { eid: 4, himo_id: 5 }, relay).map(|l| (l, relay.hlc))),
            ];
            wait_pending(wal, 4);
            wal.serve_slots();
            assert_eq!(pending_slots(wal), 0, "lock を持つ書き手が、 置かれた頼みを全部積む");
            assert_eq!(wal.combined_appends(), 4);
            drop(held);
            hs.into_iter().map(|h| h.join().unwrap().unwrap()).collect()
        });
        wal.append(Op::Commit).unwrap();
        let recs: Vec<Record> = wal.iter_committed().into_iter().filter(|r| !matches!(r.op, DecodedOp::Commit)).collect();
        assert_eq!(recs.len(), 4);
        for w in recs.windows(2) {
            assert!(w[0].lsn < w[1].lsn, "WAL の順に LSN が増える");
        }
        let rec = |i: usize| recs.iter().find(|r| r.lsn == got[i].0).unwrap_or_else(|| panic!("書き手 {i} の LSN の record が無い"));
        for (i, want) in [
            DecodedOp::Tie { eid: 1, himo_id: 2, value: 3 },
            DecodedOp::Content { eid: 2, key: "k".into(), data: vec![7; 300] },
            DecodedOp::Delete { eid: 3 },
            DecodedOp::Untie { eid: 4, himo_id: 5 },
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(rec(i).op, want, "書き手 {i} が受け取った LSN の record が、 その書き手の op");
            assert_eq!(rec(i).hlc, got[i].1, "書き手 {i} が受け取った HLC が、 その record の HLC");
        }
        assert_eq!(rec(2).hlc, pre, "事前採番の HLC が載る");
        assert_eq!((rec(3).author_peer, rec(3).signature, rec(3).pubkey_fp), (9, [3; 64], [4; 8]), "中継の header が載る");
        assert_eq!(rec(0).author_peer, 1);
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
    }

    /// #506: 置き場の頼みを積む途中で panic したら、 その頼みだけが失敗で返る (頼んだ書き手が待ち続けない)。 残りの頼みは置き場に
    /// 残り、 次に lock を持つ書き手が積む。 panic した頼みの record は WAL に無い。
    #[test]
    fn panic_while_appending_a_request_fails_only_that_request() {
        let p = tmp("combine_panic");
        let wal = OpLog::create(&p, 4 << 20).unwrap();
        let wal = &wal;
        let out: Vec<io::Result<u64>> = std::thread::scope(|s| {
            let held = wal.append_lock.lock();
            let hs: Vec<_> = (0..3u64).map(|i| s.spawn(move || wal.append(Op::Tie { eid: i, himo_id: 0, value: i }))).collect();
            wait_pending(wal, 3);
            FAULT_INJECT_SERVE_PANIC.with(|c| c.set(true));
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wal.serve_slots()));
            assert!(r.is_err(), "fault が効いていない");
            assert_eq!(pending_slots(wal), 2, "panic で積めなかった残りの頼みは置き場に残る");
            wal.serve_slots();
            drop(held);
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let errs: Vec<_> = out.iter().filter_map(|r| r.as_ref().err()).collect();
        assert_eq!(errs.len(), 1, "panic した頼みだけが失敗で返る: {out:?}");
        assert!(errs[0].to_string().contains("panicked"), "{}", errs[0]);
        wal.append(Op::Commit).unwrap();
        assert_eq!(wal.iter_committed().iter().filter(|r| !matches!(r.op, DecodedOp::Commit)).count(), 2);
        assert_eq!(wal.pending_writes(), 0);
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
    }

    /// #506: 置き場の番号は同時に生きている thread の間で重ならず (置き場の持ち主は 1 つ)、 終わった thread の番号は後の thread が
    /// 使い回す (1 本ずつ 200 本作っても番号が増え続けない)。
    #[test]
    fn slot_index_is_unique_among_live_threads_and_reused() {
        let b = std::sync::Barrier::new(8);
        let live: Vec<usize> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..8)
                .map(|_| {
                    s.spawn(|| {
                        let i = combine_slot_index().unwrap();
                        b.wait();
                        assert_eq!(combine_slot_index(), Some(i), "番号は thread の間ずっと同じ");
                        i
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let mut sorted = live.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 8, "同時に生きている thread の番号が重なった: {live:?}");
        let seq: Vec<usize> = (0..200).map(|_| std::thread::spawn(|| combine_slot_index().unwrap()).join().unwrap()).collect();
        assert!(seq.iter().all(|&i| i < 128), "終わった thread の番号が使い回されていない: max {}", seq.iter().max().unwrap());
    }

    /// まとめ書き: 書き手 8 本が同時に積む時、 各書き手が受け取った (LSN, HLC) は、 その LSN の record に載った自分の値と HLC。
    /// record は WAL の順に LSN・HLC とも増える (lock を持つ書き手が、 頼みの record を順に積む)。 append 中の writer の数は 0 に戻る。
    #[test]
    fn concurrent_appends_get_back_their_own_record() {
        use std::sync::Arc;
        let p = tmp("combine_results");
        let wal = Arc::new(OpLog::create(&p, 64 * 1024 * 1024).unwrap());
        const THREADS: u64 = 8;
        const PER: u64 = 20_000;
        let hs: Vec<_> = (0..THREADS)
            .map(|t| {
                let w = wal.clone();
                std::thread::spawn(move || {
                    (0..PER)
                        .map(|i| {
                            let eid = t * 1_000_000 + i;
                            let (lsn, hlc) = w.append_with_hlc(Op::Tie { eid, himo_id: 0, value: i }).unwrap();
                            (lsn, hlc, eid)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let got: Vec<_> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
        assert_eq!(wal.pending_writes(), 0);
        wal.append(Op::Commit).unwrap();
        let recs = wal.iter_committed();
        let recs: Vec<_> = recs.iter().filter(|r| !matches!(r.op, DecodedOp::Commit)).collect();
        assert_eq!(recs.len(), (THREADS * PER) as usize);
        for w in recs.windows(2) {
            assert!(w[0].lsn < w[1].lsn && w[0].hlc < w[1].hlc, "WAL の順と LSN / HLC の順が違う");
        }
        let by_lsn: std::collections::HashMap<u64, (Hlc, u64)> = recs
            .iter()
            .map(|r| match r.op {
                DecodedOp::Tie { eid, .. } => (r.lsn, (r.hlc, eid)),
                ref other => panic!("unexpected op: {other:?}"),
            })
            .collect();
        for (lsn, hlc, eid) in got {
            assert_eq!(by_lsn.get(&lsn), Some(&(hlc, eid)), "書き手が受け取った LSN {lsn} の record が、 その書き手の値と HLC でない");
        }
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
    }

    // ---- 2 つの区画 (file v4) ----

    fn tie_values(recs: &[Record]) -> Vec<u64> {
        recs.iter()
            .filter_map(|r| match r.op {
                DecodedOp::Tie { value, .. } => Some(value),
                _ => None,
            })
            .collect()
    }

    /// 1 group (Tie 4 件 + Commit) を書く。
    fn append_group(wal: &OpLog, v: &mut u64) {
        for _ in 0..4 {
            wal.append(Op::Tie { eid: 1, himo_id: 0, value: *v }).unwrap();
            *v += 1;
        }
        wal.append(Op::Commit).unwrap();
    }

    /// #669: 区画を移った時の閉じの Commit が header の head だけ届いて中身の届かなかった像 (新しい区画の record も届かなかった) を
    /// 開くと、 head を閉じの Commit の前に戻す。 戻さないと、 書き足した record は読めない record の後ろに積まれ、 読みが届かない
    /// (区画に空きが残る形: 入らない大きな record で移る)。 再生は今までと同じ。 閉じの Commit の前の閉じていない record (再生した)
    /// は残し、 書き足した Commit で閉じる。
    #[test]
    fn a_torn_closing_commit_is_trimmed_on_open() {
        let p = tmp("torn-closing");
        let mut v = 0u64;
        let closing_at;
        {
            let wal = OpLog::create(&p, 64 * 1024).unwrap();
            while wal.hot.halves[0].head.load(Ordering::Acquire) < 30 * 1024 {
                append_group(&wal, &mut v);
            }
            // 閉じていない record 2 件 (閉じの Commit が閉じる)
            for _ in 0..2 {
                wal.append(Op::Tie { eid: 1, himo_id: 0, value: v }).unwrap();
                v += 1;
            }
            closing_at = wal.hot.halves[0].head.load(Ordering::Acquire);
            wal.append(Op::Content { eid: 1, key: "k", data: &[7u8; 4000] }).unwrap();
            assert_eq!(wal.switches(), 1, "前提: 入らない record で移る");
            let a_head = wal.hot.halves[0].head.load(Ordering::Acquire);
            assert_eq!(a_head - closing_at, REC_HEADER_SIZE as u64 + LAP_TRAILER as u64, "前提: A の終わりは閉じの Commit");
            assert!(32 * 1024 - a_head > 1024, "前提: A に空きが残る");
            wal.fsync().unwrap();
        }
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&p).unwrap();
            // B の record は届かなかった (B は空)
            f.seek(SeekFrom::Start(32 * 1024 + 8)).unwrap();
            f.write_all(&(HEADER_SIZE as u64).to_le_bytes()).unwrap();
            f.write_all(&(HEADER_SIZE as u64).to_le_bytes()).unwrap();
            // A の閉じの Commit の中身は届かなかった (head だけが届いた)
            f.seek(SeekFrom::Start(closing_at)).unwrap();
            f.write_all(&[0u8; 16]).unwrap();
        }
        let wal = OpLog::open(&p).unwrap();
        assert_eq!(tie_values(&wal.recover_with_tail()), (0..v).collect::<Vec<_>>(), "再生は閉じの Commit の前まで全部");
        assert!(wal.drop_records_after_a_hole(), "閉じの Commit から先を捨てる");
        assert_eq!(wal.head(), wal_pos(wal.lap(), closing_at), "head は閉じの Commit の前");
        let before = v;
        append_group(&wal, &mut v);
        assert_eq!(tie_values(&wal.iter_committed()), (0..v).collect::<Vec<_>>(), "書き足した {before}.. が読める");
        drop(wal);
        // 開き直しても書き足した record から読む
        let wal = OpLog::open(&p).unwrap();
        assert_eq!(tie_values(&wal.recover_with_tail()).last(), Some(&(v - 1)));
        assert!(!wal.drop_records_after_a_hole(), "穴は無い");
        let _ = std::fs::remove_file(&p);
    }

    /// #663: 開いた時の再生が古い区画の穴 (読めない record) で止まったら、 穴の後の新しい区画の record (再生していない) を捨てる
    /// (`drop_records_after_a_hole`)。 残すと、 開いた後に古い区画を畳んだ時に読めるようになる。 穴が無ければ捨てない。 古い区画を
    /// 全部書き出してあれば再生は新しい区画から始まるので、 古い区画の穴があっても捨てない。
    #[test]
    fn a_hole_in_the_old_half_drops_the_new_half_on_open() {
        // (穴を作る, 古い区画を書き出しておく, 新しい区画を捨てる)
        for (hole, written, dropped) in [(true, false, true), (false, false, false), (true, true, false)] {
            let p = tmp(&format!("halves-hole-{hole}-{written}"));
            let mut v = 0u64;
            {
                let wal = OpLog::create(&p, 64 * 1024).unwrap();
                while wal.switches() == 0 {
                    append_group(&wal, &mut v);
                }
                for _ in 0..3 {
                    append_group(&wal, &mut v);
                }
                assert_eq!(wal.hot.active.load(Ordering::Acquire), 1, "前提: 古い区画は 0");
                let old = &wal.hot.halves[0];
                if written {
                    wal.advance_checkpoint(wal_pos(old.lap(), old.head.load(Ordering::Acquire)));
                }
                if hole {
                    // 6 つ目の group の最初の Tie (Tie 4 件 + Commit = 664 byte) の payload を壊す = 古い区画の途中の読めない record
                    let at = old.base() + HEADER_SIZE + 5 * 664 + REC_HEADER_SIZE;
                    wal.mmap_mut_slice()[at] ^= 0xFF;
                }
                wal.fsync().unwrap();
            }
            let wal = OpLog::open(&p).unwrap();
            let recovered = tie_values(&wal.recover_with_tail());
            if hole && !written {
                assert_eq!(recovered, (0..20).collect::<Vec<_>>(), "前提: 再生は穴の前まで");
            }
            assert_eq!(wal.drop_records_after_a_hole(), dropped, "穴 {hole} / 書き出し済み {written}");
            assert_eq!(wal.hot.halves[1].is_empty(), dropped, "新しい区画を捨てたか");
            if dropped {
                assert_eq!(tie_values(&wal.iter_committed()), (0..20).collect::<Vec<_>>(), "残るのは穴の前の record だけ");
            }
            let _ = std::fs::remove_file(&p);
        }
    }

    /// #662: 畳む時の写し (`scan_for_change_log(.., with_tail = true)` / `iter_from_with_tail_upto`) は `limit` (畳む区画の終わり)
    /// より先を読まない: 古い区画の record は全部、 今の区画の record (閉じていない record も) は返さない。
    #[test]
    fn copies_for_a_fold_stop_at_the_end_of_the_old_half() {
        let p = tmp("halves-upto");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        let lap0 = wal.lap();
        let mut v = 0u64;
        while wal.switches() == 0 {
            append_group(&wal, &mut v);
        }
        // 今の区画の閉じていない record
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 << 40 }).unwrap();
        let old_end = wal_pos(lap0, wal.hot.halves[0].head.load(Ordering::Acquire));
        let strip = |r: Vec<(Record, u64)>| r.into_iter().map(|(r, _)| r).collect::<Vec<_>>();
        let (all, _) = wal.scan_for_change_log(HEADER_SIZE as u64, u64::MAX, true);
        let all = tie_values(&strip(all));
        assert_eq!(all.last(), Some(&(1 << 40)), "前提: 上限なしなら今の区画の閉じていない record まで読む");
        let (old, end) = wal.scan_for_change_log(HEADER_SIZE as u64, old_end, true);
        assert_eq!(end, old_end, "畳む区画の終わりまで読み終える");
        assert!(old.iter().all(|(_, e)| *e <= old_end));
        let old = tie_values(&strip(old));
        let k = old.len();
        assert!(k > 0 && k < all.len() && old == all[..k], "古い区画の record だけ ({k} / {} 件)", all.len());
        let (committed, committed_end, tail, end) = wal.iter_from_with_tail_upto(HEADER_SIZE as u64, old_end);
        assert_eq!((committed_end, end), (old_end, old_end));
        assert!(tail.is_empty());
        assert_eq!(tie_values(&committed), old);
        // 上限は record の境目ならどこでも: その位置から始まる record は読まない
        let (recs, _) = wal.scan_for_change_log(HEADER_SIZE as u64, old_end, true);
        let mid = recs[k / 2].1;
        let (head_part, end) = wal.scan_for_change_log(HEADER_SIZE as u64, mid, true);
        assert_eq!(end, mid);
        assert_eq!(tie_values(&strip(head_part)), old[..=k / 2].to_vec(), "上限の位置から始まる record を読んだ");
    }

    /// 今の区画が埋まると、 開いている group を閉じてもう一方の区画へ移る (周は新しい数)。 2 つの区画を積んだ順に読む。 古い区画
    /// は全部書き出すまで畳まず、 畳めるのは古い区画 (読み手がその終わりまで読んでいればよい)。 今の区画は古い区画が空になって
    /// からその場で畳む。
    #[test]
    fn halves_switch_when_full_and_scans_keep_the_wal_order() {
        let p = tmp("halves-switch");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        assert!(wal.two_halves());
        assert_eq!((wal.file_version(), wal.half_capacity(), wal.capacity()), (OPLOG_FILE_VERSION, 32 * 1024, 64 * 1024));
        let lap0 = wal.lap();
        let mut v = 0u64;
        while wal.switches() == 0 {
            append_group(&wal, &mut v);
        }
        assert!(lap_newer(wal.lap(), lap0), "移った区画は新しい周");
        assert_eq!(tie_values(&wal.iter_committed()), (0..v).collect::<Vec<_>>(), "2 つの区画を積んだ順に読む");
        assert!(!wal.try_reset(), "書き出していない古い区画は畳まない");
        let old_end = wal_pos(lap0, wal.hot.halves[0].head.load(Ordering::Acquire));
        assert_eq!(wal.checkpoint(), wal_pos(lap0, HEADER_SIZE as u64), "checkpoint は古い区画の中");
        wal.advance_checkpoint(wal.head());
        assert_eq!(wal.checkpoint(), wal.head(), "今の区画の head まで書き出すと古い区画も全部");
        let mut through = 0;
        assert!(wal.try_reset_if(|t| {
            through = t;
            true
        }));
        assert_eq!(through, old_end, "畳んでよい位置 = 古い区画の終わり");
        assert!(wal.hot.halves[0].is_empty() && !wal.hot.halves[1].is_empty(), "畳んだのは古い区画だけ");
        let rest = tie_values(&wal.iter_committed());
        let k = v - rest.len() as u64;
        assert!(k > 0 && rest == (k..v).collect::<Vec<_>>(), "今の区画の record だけが残る: {rest:?}");
        // 今の区画はその場で (周を進めて頭から)
        let lap_b = wal.lap();
        assert!(wal.try_reset());
        assert!(wal.is_empty() && lap_newer(wal.lap(), lap_b) && wal.hot.active.load(Ordering::Acquire) == 1);
        // また埋めると A へ (A の周は今の区画より古いので置き直す)
        let start = v;
        while wal.switches() == 1 {
            append_group(&wal, &mut v);
        }
        assert_eq!(wal.hot.active.load(Ordering::Acquire), 0);
        assert!(lap_newer(wal.hot.halves[0].lap(), wal.hot.halves[1].lap()));
        assert_eq!(tie_values(&wal.iter_committed()), (start..v).collect::<Vec<_>>());
        let _ = std::fs::remove_file(&p);
    }

    /// 両方の区画に record があるまま閉じて開くと、 record のある区画のうち周の新しい方に書き足し、 再生は古い区画から。
    /// 開いた後に置く周は header の周の最大の [`LAP_SKIP_ON_OPEN`] 先 (前の process が置いて header の届かなかった周を使わない)。
    #[test]
    fn reopening_with_both_halves_written_replays_the_older_half_first() {
        let p = tmp("halves-reopen");
        let mut v = 0u64;
        {
            let wal = OpLog::create(&p, 64 * 1024).unwrap();
            while wal.switches() == 0 || wal.hot.halves[1].head.load(Ordering::Acquire) < 8 * 1024 {
                append_group(&wal, &mut v);
            }
            wal.fsync().unwrap();
        }
        let wal = OpLog::open(&p).unwrap();
        assert_eq!(wal.hot.active.load(Ordering::Acquire), 1, "record のある区画のうち周の新しい方");
        assert_eq!(tie_values(&wal.recover_with_tail()), (0..v).collect::<Vec<_>>(), "古い区画 → 新しい区画の順に再生");
        let top = wal.hot.halves[1].lap();
        append_group(&wal, &mut v);
        assert_eq!(tie_values(&wal.iter_committed()).last(), Some(&(v - 1)), "今の区画 (B) に書き足す");
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset(), "古い区画 (A) を畳む");
        assert_eq!(wal.hot.halves[0].lap(), top.wrapping_add(LAP_SKIP_ON_OPEN), "開いた後の周は先へ飛ばす");
        let _ = std::fs::remove_file(&p);
    }

    /// 読み手の位置が畳んだ区画を指していたら (その区画は読み終えている)、 もう一方の区画を頭から読む。 位置は WAL に積んだ順。
    #[test]
    fn a_cursor_in_a_folded_half_reads_the_other_half_from_its_start() {
        let p = tmp("halves-cursor");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        let mut v = 0u64;
        while wal.switches() == 0 {
            append_group(&wal, &mut v);
        }
        let in_b = |w: &OpLog| tie_values(&strip_offsets(w.iter_committed_from_with_offsets(wal_pos(w.hot.halves[1].lap(), HEADER_SIZE as u64)).0));
        let b_values = in_b(&wal);
        append_group(&wal, &mut v);
        let b_values = [b_values, vec![v - 4, v - 3, v - 2, v - 1]].concat();
        let a_end = wal_pos(wal.hot.halves[0].lap(), wal.hot.halves[0].head.load(Ordering::Acquire));
        assert!(a_end < wal_pos(wal.hot.halves[1].lap(), HEADER_SIZE as u64), "位置は積んだ順");
        let (recs, end) = wal.iter_committed_from_with_offsets(a_end);
        assert_eq!(tie_values(&strip_offsets(recs)), b_values, "A の終わりからは B の頭から");
        assert_eq!(end, wal.head());
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset_if(|through| a_end >= through), "A を読み終えた読み手なら A を畳める");
        let (recs, end2) = wal.iter_committed_from_with_offsets(a_end);
        assert_eq!(tie_values(&strip_offsets(recs)), b_values, "畳んだ区画の位置からも B の頭から (重複も取りこぼしも無い)");
        assert_eq!(end2, end);
        assert!(wal.iter_committed_from_with_offsets(end).0.is_empty(), "読み終えた位置からは何も無い");
        assert_eq!(in_b(&wal), b_values);
        let _ = std::fs::remove_file(&p);
    }

    /// v3 (0.36 の 1 区画) の file は 1 区画のまま読み書きし (区画の半分を越えて書ける)、 最初に畳む時に v4 の 2 区画になる
    /// (B の header、 A の capacity と version)。 B に前の周の record が残っていても読まない。
    #[test]
    fn a_v3_file_becomes_two_halves_at_the_first_fold() {
        let p = tmp("halves-v3");
        let cap = 64 * 1024u64;
        let half = (cap / 2) as usize;
        let mut v = 0u64;
        {
            let wal = OpLog::create(&p, cap as usize).unwrap();
            {
                // 0.36 が作った形: 1 区画、 header の capacity は file 全体、 B の header は無い
                let m = wal.mmap_mut_slice();
                m[4..8].copy_from_slice(&OPLOG_FILE_VERSION_V3.to_le_bytes());
                m[OFF_CAPACITY..OFF_CAPACITY + 4].copy_from_slice(&(cap as u32).to_le_bytes());
                m[half..half + HEADER_SIZE].fill(0);
            }
            wal.two.store(false, Ordering::Release);
            wal.hot.halves[0].size.store(cap as u32, Ordering::Release);
            wal.hot.halves[1].size.store(0, Ordering::Release);
            while wal_pos_offset(wal.head()) < 40 * 1024 {
                append_group(&wal, &mut v);
            }
            assert_eq!(wal.switches(), 0);
            wal.fsync().unwrap();
        }
        let wal = OpLog::open(&p).unwrap();
        assert_eq!((wal.file_version(), wal.two_halves(), wal.capacity()), (OPLOG_FILE_VERSION_V3, false, cap));
        assert_eq!(tie_values(&wal.recover_with_tail()), (0..v).collect::<Vec<_>>(), "区画の半分を越えた record も読める");
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset());
        assert_eq!(
            (wal.file_version(), wal.two_halves(), wal.half_capacity(), wal.capacity()),
            (OPLOG_FILE_VERSION, true, cap / 2, cap)
        );
        {
            let m = wal.mmap_slice();
            let r32 = |o: usize| u32::from_le_bytes(m[o..o + 4].try_into().unwrap());
            assert_eq!((r32(4), r32(OFF_CAPACITY) as u64), (OPLOG_FILE_VERSION, cap / 2), "A の header");
            assert_eq!(&m[half..half + 4], FILE_MAGIC, "B の header");
            assert_eq!((r32(half + 4), r32(half + OFF_CAPACITY) as u64), (OPLOG_FILE_VERSION, cap / 2));
            assert_eq!(u64::from_le_bytes(m[half + 8..half + 16].try_into().unwrap()), HEADER_SIZE as u64);
        }
        // A を埋めて B (前の周の record が残っている) へ移る
        let start = v;
        while wal.switches() == 0 {
            append_group(&wal, &mut v);
        }
        append_group(&wal, &mut v);
        assert_eq!(tie_values(&wal.iter_committed()), (start..v).collect::<Vec<_>>(), "B に残った前の周の record は読まない");
        wal.fsync().unwrap();
        drop(wal);
        let wal = OpLog::open(&p).unwrap();
        assert_eq!((wal.file_version(), wal.two_halves()), (OPLOG_FILE_VERSION, true));
        assert_eq!(tie_values(&wal.recover_with_tail()), (start..v).collect::<Vec<_>>());
        let _ = std::fs::remove_file(&p);
    }

    /// 2 区画にできない小さい WAL は v3 の 1 区画で作る (今までと同じ形)。
    #[test]
    fn a_wal_too_small_for_two_halves_stays_one_v3_half() {
        let p = tmp("halves-small");
        let wal = OpLog::create(&p, 16 * 1024).unwrap();
        assert_eq!((wal.file_version(), wal.two_halves(), wal.capacity()), (OPLOG_FILE_VERSION_V3, false, 16 * 1024));
        drop(wal);
        let wal = OpLog::open(&p).unwrap();
        assert_eq!((wal.file_version(), wal.two_halves()), (OPLOG_FILE_VERSION_V3, false));
        let _ = std::fs::remove_file(&p);
    }

    /// 1 件の record は 1 区画に収まること: 区画より大きい record は断る (`InvalidInput`、 畳んでも載らない)。
    #[test]
    fn a_record_larger_than_a_half_is_refused() {
        let p = tmp("halves-big");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        let big = vec![7u8; 40 * 1024];
        let e = wal.append(Op::Content { eid: 1, key: "k", data: &big }).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput, "{e}");
        let fits = vec![7u8; 20 * 1024];
        wal.append(Op::Content { eid: 1, key: "k", data: &fits }).unwrap();
        wal.append(Op::Content { eid: 1, key: "k", data: &fits }).unwrap();
        assert_eq!(wal.switches(), 1, "2 件目は移った区画に");
        let _ = std::fs::remove_file(&p);
    }

    /// 書き手が載せられないのは、 今の区画が埋まって、 もう一方をまだ畳めていない時だけ。 古い区画を畳めばまた移れる。
    #[test]
    fn appends_fail_only_while_both_halves_are_full() {
        let p = tmp("halves-full");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        let mut v = 0u64;
        let e = loop {
            if let Err(e) = wal.append(Op::Tie { eid: 1, himo_id: 0, value: v }) {
                break e;
            }
            v += 1;
            wal.append(Op::Commit).unwrap();
        };
        assert_eq!(e.kind(), io::ErrorKind::OutOfMemory);
        assert_eq!(wal.switches(), 1, "1 度移ってから埋まった");
        while wal.append(Op::Commit).is_ok() {}
        assert!(wal.append_dead(), "今の区画に Commit も入らず、 移れる空の区画も無い");
        assert!(wal.free_bytes() < wal.record_size_of(0) as u64);
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset(), "古い区画を畳む");
        assert!(!wal.append_dead());
        assert!(wal.free_bytes() >= wal.half_capacity() - HEADER_SIZE as u64);
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: v }).unwrap();
        assert_eq!(wal.switches(), 2, "畳んだ区画へ移った");
        let _ = std::fs::remove_file(&p);
    }

    /// 配り済みの印の位置 (切れ目) からの header の走査は、 区画をまたいで切れ目の後の record を全部読む。 切れ目の区画を畳んだ
    /// 後は、 残った区画を頭から (全部切れ目の後)。
    #[test]
    fn the_header_visit_from_a_durable_cut_crosses_halves() {
        let p = tmp("halves-cut");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        let mut v = 0u64;
        for _ in 0..10 {
            append_group(&wal, &mut v);
        }
        let mut before = 0;
        wal.visit_record_headers_with_tail(None, |_, _| before += 1);
        let cut = wal.durable_cut_for(0);
        while wal.switches() == 0 {
            append_group(&wal, &mut v);
        }
        for _ in 0..3 {
            append_group(&wal, &mut v);
        }
        let mut all = Vec::new();
        wal.visit_record_headers_with_tail(None, |_, h| all.push(h));
        let mut after = Vec::new();
        wal.visit_record_headers_with_tail(Some((cut.lap, cut.offset)), |_, h| after.push(h));
        assert_eq!(after, all[before..].to_vec(), "切れ目の後 = A の残り + B 全部");
        let in_b = {
            let mut n = Vec::new();
            wal.visit_record_headers_with_tail(Some((wal.hot.halves[1].lap(), HEADER_SIZE as u64)), |_, h| n.push(h));
            n
        };
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset());
        let mut after_fold = Vec::new();
        wal.visit_record_headers_with_tail(Some((cut.lap, cut.offset)), |_, h| after_fold.push(h));
        assert_eq!(after_fold, in_b, "切れ目の区画を畳んだ後は B を頭から");
        let _ = std::fs::remove_file(&p);
    }

    /// 書き手 4 本 (束ねる道も通る) が小さい WAL に書き続け、 何度も区画を切り替える横で、 読み手が位置から読んで進め、 畳む係が
    /// 読み手の読み終えた区画を畳む (engine の bridge と consumer の形、 読むのと畳むのは同じ lock の下)。 読み手はどの record も
    /// 1 回ずつ、 書き手ごとに積んだ順に受け取る。
    #[test]
    fn readers_see_every_record_once_while_writers_switch_halves() {
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};
        let p = tmp("halves-stress");
        let wal = Arc::new(OpLog::create(&p, 64 * 1024).unwrap());
        const W: u64 = 4;
        const N: u64 = 3000;
        let cursor = Arc::new(AtomicU64::new(HEADER_SIZE as u64));
        let seen: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let transfer = Arc::new(Mutex::new(()));
        let done = Arc::new(AtomicBool::new(false));
        let read = {
            let (wal, cursor, seen, transfer) = (wal.clone(), cursor.clone(), seen.clone(), transfer.clone());
            move || {
                let _g = transfer.lock().unwrap();
                let from = cursor.load(Ordering::Acquire);
                let (recs, end) = wal.iter_committed_from_with_offsets(from);
                let mut s = seen.lock().unwrap();
                for (r, _) in recs {
                    if let DecodedOp::Tie { eid, value, .. } = r.op {
                        s.push((eid, value));
                    }
                }
                assert!(end >= from, "位置が戻った: {from:#x} → {end:#x}");
                cursor.store(end, Ordering::Release);
            }
        };
        let consumer = {
            let (wal, cursor, transfer, done, read) = (wal.clone(), cursor.clone(), transfer.clone(), done.clone(), read.clone());
            std::thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    read();
                    wal.advance_checkpoint(wal.head());
                    let _g = transfer.lock().unwrap();
                    wal.try_reset_if(|through| cursor.load(Ordering::Acquire) >= through);
                }
            })
        };
        let writers: Vec<_> = (0..W)
            .map(|w| {
                let wal = wal.clone();
                std::thread::spawn(move || {
                    let put = |op: Op<'_>| {
                        while let Err(e) = wal.append(op.clone()) {
                            assert_eq!(e.kind(), io::ErrorKind::OutOfMemory, "{e}");
                            std::thread::yield_now();
                        }
                    };
                    for i in 0..N {
                        put(Op::Tie { eid: w, himo_id: 0, value: i });
                        if i % 3 == 2 {
                            put(Op::Commit);
                        }
                    }
                    put(Op::Commit);
                })
            })
            .collect();
        for t in writers {
            t.join().unwrap();
        }
        done.store(true, Ordering::Release);
        consumer.join().unwrap();
        wal.append(Op::Commit).unwrap();
        read();
        assert!(wal.switches() >= 10, "前提: 何度も切り替えた ({})", wal.switches());
        let seen = seen.lock().unwrap();
        for w in 0..W {
            let mine: Vec<u64> = seen.iter().filter(|(e, _)| *e == w).map(|(_, v)| *v).collect();
            assert_eq!(mine, (0..N).collect::<Vec<_>>(), "書き手 {w} の record が 1 回ずつ積んだ順に届いていない");
        }
        let _ = std::fs::remove_file(&p);
    }

    /// 読み手が 2 つの区画の終端を読む間に、 書き手が今の区画に足して移り、 移った先にも書く。 読み手は今の区画の残りを飛ばさない
    /// (前は区画の番号の順に終端を読み、 今の区画の終端を先に読むと、 その後に足された record を飛ばして移った先を読み、 位置が
    /// その先へ進んだ: changefeed の受け手に区画 1 つ分が届かなかった)。
    #[test]
    fn a_reader_does_not_skip_records_added_while_it_reads_the_half_ends() {
        let p = tmp("halves-readrace");
        let wal = std::sync::Arc::new(OpLog::create(&p, 64 * 1024).unwrap());
        let mut v = 0u64;
        // B に移って A を畳む (A は空で、 周は B より新しい) → B を少し書いて、 読み手が B の途中まで読む
        while wal.switches() == 0 {
            append_group(&wal, &mut v);
        }
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset());
        assert!(wal.hot.halves[0].is_empty() && lap_newer(wal.hot.halves[0].lap(), wal.hot.halves[1].lap()), "前提: A は空で周が新しい");
        append_group(&wal, &mut v);
        let (recs, cursor) = wal.iter_committed_from_with_offsets(HEADER_SIZE as u64);
        let first = tie_values(&strip_offsets(recs));
        // 次の読みの、 終端を 2 つ読む間に: B を埋めて A へ移り、 A にも書く
        let (w, start) = (wal.clone(), v);
        let written = std::sync::Arc::new(std::sync::Mutex::new(v));
        let wr = written.clone();
        read_plan_hook::set(Box::new(move || {
            let mut v = start;
            while w.switches() == 1 {
                append_group(&w, &mut v);
            }
            append_group(&w, &mut v);
            *wr.lock().unwrap() = v;
        }));
        let (recs, end) = wal.iter_committed_from_with_offsets(cursor);
        let got = tie_values(&strip_offsets(recs));
        let (recs2, _) = wal.iter_committed_from_with_offsets(end);
        let got2 = tie_values(&strip_offsets(recs2));
        let all = [first, got, got2].concat();
        // A にあった分は畳んだので、 残っているのは B の頭から
        let k = all[0];
        assert!(k > 0, "前提: A を畳んだ");
        assert_eq!(all, (k..*written.lock().unwrap()).collect::<Vec<_>>(), "読み手が record を飛ばした / 重ねた");
        let _ = std::fs::remove_file(&p);
    }

    /// 書き出し (`checkpoint_and_reset` の sync) の最中に書き手が今の区画を埋めて移っても、 古い区画を畳む (書き出す前に読んだ head は
    /// 古い区画の中なので、 1 回目は古い区画の checkpoint がその head までしか進まない → 新しい区画の head まで書き出し直す)。 前は
    /// 畳めずに返り、 空きを待つ書き手が 「畳めない」 を見て書き込みを断った。
    #[test]
    fn a_switch_during_the_write_out_still_folds_the_old_half() {
        let p = tmp("halves-syncrace");
        let wal = OpLog::create(&p, 64 * 1024).unwrap();
        let mut v = 0u64;
        while wal.switches() == 0 {
            append_group(&wal, &mut v);
        }
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset(), "A を畳む");
        // B に書く。 書き出しの最中に B を埋めて A へ移る
        append_group(&wal, &mut v);
        let mut calls = 0;
        let (advanced, folded) = wal.checkpoint_and_reset(
            |_head| {
                calls += 1;
                if calls == 1 {
                    while wal.switches() == 1 {
                        append_group(&wal, &mut v);
                    }
                    append_group(&wal, &mut v);
                }
                true
            },
            |_| true,
        );
        assert_eq!((advanced, folded), (true, true), "書き出しの最中に移っても古い区画 (B) を畳む");
        assert!(calls >= 2, "移った後の head まで書き出し直した");
        assert!(wal.hot.halves[1].is_empty() && !wal.hot.halves[0].is_empty());
        let _ = std::fs::remove_file(&p);
    }
}
