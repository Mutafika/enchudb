//! Write-Ahead Log (v28→v32) — crash consistency for op-based writes.
//!
//! # v32 変更点(WAL v2)
//!
//! - **eid を u64 化**(分散の [peer|local] 合成 ID)
//! - **HLC スロット**: `(wall:8, logical:4, peer:4)` 全順序用
//! - **author_peer スロット**: WAL を書いた peer(中継 peer 対応)
//! - **署名スロット(64B)**: ed25519(Phase C で埋める、現状 zeros)
//! - **pubkey_fp(8B)**: 署名検証に使う pubkey の先頭 8B(現状 zeros)
//!
//! 破壊変更: v1 WAL は開けない。v2 magic で判別しエラー。
//!
//! # 設計原則
//!
//! - **読みは WAL に触らない**(pull_raw / query の 10〜70ns は影響ゼロ)
//! - **書きは WAL append を memcpy 1 回で済ます**(tie_async の ~1μs を維持)
//! - **fsync は hot path から外す**(非同期、consumer スレッド側で定期実行)
//!
//! # レコード形式(v2)
//!
//! ```text
//! [magic: 2B "WL"]
//! [version: 1B]         = 2
//! [op_type: 1B]         0=Tie, 1=Untie, 2=Delete, 3=Content, 4=Commit, 5=Schema
//! [len: 4B LE]          payload bytes
//! [lsn: 8B LE]          Log Sequence Number(ローカル単調増加、recover 用)
//! [hlc_wall: 8B LE]     HLC wall clock(ms since epoch)
//! [hlc_logical: 4B LE]  HLC logical counter
//! [hlc_peer: 4B LE]     HLC peer id
//! [author_peer: 4B LE]  WAL を書いた peer の id
//! [crc32: 4B LE]        payload の FNV-1a
//! [signature: 64B]      ed25519 over (header fixed ‖ payload)。現状 zeros。
//! [pubkey_fp: 8B]       署名に使った pubkey の先頭 8B。現状 zeros。
//! [payload: len B]
//! ```
//!
//! ヘッダ固定部: 2+1+1+4+8 + 8+4+4 + 4 + 4 + 64 + 8 = **112B / record**
//!
//! Tie payload(v2):    [eid: 8B][himo_id: 2B][_pad: 2B][value: 4B]  = 16B
//! Untie payload(v2):  [eid: 8B][himo_id: 2B][_pad: 6B]             = 16B
//! Delete payload(v2): [eid: 8B]                                     = 8B
//! Content payload(v2):[eid: 8B][key_len: 2B][_pad: 2B][data_len: 4B][key][data]
//! Commit payload:     なし(len = 0)
//!
//! # ファイル配置
//!
//! 別ファイル `{db_path}.wal`。sparse mmap、初期 256MB。
//!
//! ```text
//! [File header 32B]
//!   [magic: 4B "EWAL"]
//!   [version: 4B LE]  = 2
//!   [head: 8B LE]     writer が atomic 前進
//!   [checkpoint: 8B LE] consumer が前進(ここまで本体に適用済み)
//!   [capacity: 8B LE] buffer size
//! [Records starting at offset 32]
//! ```

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use memmap2::MmapMut;

use crate::{Hlc, PeerId};

const FILE_MAGIC: &[u8; 4] = b"EWAL";
pub const WAL_FILE_VERSION: u32 = 2;
const HEADER_SIZE: usize = 32;

const REC_MAGIC: &[u8; 2] = b"WL";
const REC_VERSION: u8 = 2;
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

/// 署名/pubkey_fp は Phase C で埋める。現状 zeros。
const ZERO_SIGNATURE: [u8; 64] = [0u8; 64];
const ZERO_PUBKEY_FP: [u8; 8] = [0u8; 8];

/// WAL 初期サイズ(256MB、sparse なので実ディスク消費は書いた分のみ)。
pub const DEFAULT_WAL_SIZE: usize = 256 * 1024 * 1024;

/// op_type バイト値。
pub mod op_type {
    pub const TIE: u8 = 0;
    pub const UNTIE: u8 = 1;
    pub const DELETE: u8 = 2;
    pub const CONTENT: u8 = 3;
    pub const COMMIT: u8 = 4;
    pub const SCHEMA: u8 = 5; // v32 予約(Phase D 以降で使う)
}

/// WAL に書く op。eid は u64(v32)。
#[derive(Debug, Clone)]
pub enum WalOp<'a> {
    Tie { eid: u64, himo_id: u16, value: u32 },
    Untie { eid: u64, himo_id: u16 },
    Delete { eid: u64 },
    Content { eid: u64, key: &'a str, data: &'a [u8] },
    Commit,
}

impl<'a> WalOp<'a> {
    /// payload の byte 数(固定 or 動的)。v2 layout。
    #[inline]
    fn payload_size(&self) -> usize {
        match self {
            WalOp::Tie { .. } => 16,       // eid(8) + himo_id(2) + pad(2) + value(4)
            WalOp::Untie { .. } => 16,     // eid(8) + himo_id(2) + pad(6)
            WalOp::Delete { .. } => 8,     // eid(8)
            WalOp::Content { key, data, .. } => 8 + 2 + 2 + 4 + key.len() + data.len(),
            WalOp::Commit => 0,
        }
    }

    fn op_byte(&self) -> u8 {
        match self {
            WalOp::Tie { .. } => op_type::TIE,
            WalOp::Untie { .. } => op_type::UNTIE,
            WalOp::Delete { .. } => op_type::DELETE,
            WalOp::Content { .. } => op_type::CONTENT,
            WalOp::Commit => op_type::COMMIT,
        }
    }

    fn write_payload(&self, buf: &mut [u8]) {
        match self {
            WalOp::Tie { eid, himo_id, value } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..10].copy_from_slice(&himo_id.to_le_bytes());
                buf[10..12].copy_from_slice(&[0, 0]);
                buf[12..16].copy_from_slice(&value.to_le_bytes());
            }
            WalOp::Untie { eid, himo_id } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..10].copy_from_slice(&himo_id.to_le_bytes());
                buf[10..16].copy_from_slice(&[0u8; 6]);
            }
            WalOp::Delete { eid } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
            }
            WalOp::Content { eid, key, data } => {
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
            WalOp::Commit => {}
        }
    }
}

/// デコード後の op(読み戻し用、所有型)。
#[derive(Debug, Clone)]
pub enum DecodedOp {
    Tie { eid: u64, himo_id: u16, value: u32 },
    Untie { eid: u64, himo_id: u16 },
    Delete { eid: u64 },
    Content { eid: u64, key: String, data: Vec<u8> },
    Commit,
}

/// リカバリ結果の 1 レコード(HLC + author_peer 込み)。
#[derive(Debug, Clone)]
pub struct RecoveredRecord {
    pub lsn: u64,
    pub hlc: Hlc,
    pub author_peer: PeerId,
    pub op: DecodedOp,
}

/// WAL 本体。
pub struct Wal {
    #[cfg(not(target_arch = "wasm32"))]
    _file: File,
    #[cfg(not(target_arch = "wasm32"))]
    mmap: MmapMut,
    #[cfg(target_arch = "wasm32")]
    buf: Vec<u8>,
    capacity: u64,
    /// writer 側のキャッシュ(atomic CAS で前進)。mmap ヘッダにも反映。
    head: AtomicU64,
    checkpoint: AtomicU64,
    next_lsn: AtomicU64,
    /// v32: HLC logical counter(wall が進まない時の tiebreaker 単調増加)。
    hlc_logical: std::sync::atomic::AtomicU32,
    /// v32: 最後に書いた HLC wall(ms)。
    hlc_last_wall: AtomicU64,
    /// v32: この Wal を持つ peer の id(header には書かず Engine から設定)。
    peer_id: std::sync::atomic::AtomicU32,
    /// v30: append 中の writer 数。try_reset はこれが 0 のときだけ実行できる。
    pending_writes: std::sync::atomic::AtomicU32,
}

unsafe impl Send for Wal {}
unsafe impl Sync for Wal {}

impl Wal {
    /// 新規 WAL ファイル作成。capacity は初期サイズ(bytes)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create(path: &Path, capacity: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.set_len(capacity as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        // ヘッダ初期化
        mmap[0..4].copy_from_slice(FILE_MAGIC);
        mmap[4..8].copy_from_slice(&WAL_FILE_VERSION.to_le_bytes());
        mmap[8..16].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes()); // head
        mmap[16..24].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes()); // checkpoint
        mmap[24..32].copy_from_slice(&(capacity as u64).to_le_bytes()); // capacity

        Ok(Self {
            _file: file,
            mmap,
            capacity: capacity as u64,
            head: AtomicU64::new(HEADER_SIZE as u64),
            checkpoint: AtomicU64::new(HEADER_SIZE as u64),
            next_lsn: AtomicU64::new(1),
            hlc_logical: std::sync::atomic::AtomicU32::new(0),
            hlc_last_wall: AtomicU64::new(0),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// 既存 WAL を開く。v2 のみ対応。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if mmap.len() < HEADER_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "WAL too small"));
        }
        if &mmap[0..4] != FILE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad WAL magic"));
        }
        let version = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
        if version != WAL_FILE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL version {} unsupported (expected {})", version, WAL_FILE_VERSION),
            ));
        }
        let head = u64::from_le_bytes(mmap[8..16].try_into().unwrap());
        let checkpoint = u64::from_le_bytes(mmap[16..24].try_into().unwrap());
        let capacity = u64::from_le_bytes(mmap[24..32].try_into().unwrap());

        Ok(Self {
            _file: file,
            mmap,
            capacity,
            head: AtomicU64::new(head),
            checkpoint: AtomicU64::new(checkpoint),
            next_lsn: AtomicU64::new(1),
            hlc_logical: std::sync::atomic::AtomicU32::new(0),
            hlc_last_wall: AtomicU64::new(0),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// この WAL を所有する peer の id を設定(Engine 初期化時に 1 回)。
    pub fn set_peer_id(&self, peer: PeerId) {
        self.peer_id.store(peer, Ordering::Release);
    }

    /// 現在の peer id。
    #[inline]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id.load(Ordering::Acquire)
    }

    /// 現在の head(次の append 位置)。
    #[inline]
    pub fn head(&self) -> u64 { self.head.load(Ordering::Acquire) }

    /// 現在の checkpoint(ここまで本体に反映済み)。
    #[inline]
    pub fn checkpoint(&self) -> u64 { self.checkpoint.load(Ordering::Acquire) }

    /// 次に発行する LSN。
    #[inline]
    pub fn next_lsn(&self) -> u64 { self.next_lsn.load(Ordering::Acquire) }

    /// 次の HLC を払い出す。
    /// wall が前回より進んでいれば logical=0 にリセット、同じ/戻っていれば logical+1。
    fn next_hlc(&self) -> Hlc {
        let peer = self.peer_id.load(Ordering::Acquire);
        let now = current_wall_ms();
        loop {
            let last = self.hlc_last_wall.load(Ordering::Acquire);
            let logical = self.hlc_logical.load(Ordering::Acquire);
            let (new_wall, new_logical) = if now > last {
                (now, 0u32)
            } else {
                (last, logical.wrapping_add(1))
            };
            // last_wall, logical を同時更新(CAS 的に)
            if self.hlc_last_wall.compare_exchange(last, new_wall, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                self.hlc_logical.store(new_logical, Ordering::Release);
                return Hlc { wall: new_wall, logical: new_logical, peer };
            }
            // race したらリトライ
        }
    }

    /// op を append。新しいレコードの LSN を返す。
    ///
    /// v30: pending_writes カウンタで try_reset との race を防ぐ。
    /// writer は append 中 +1、完了時 -1。consumer reset は pending==0 時のみ。
    pub fn append(&self, op: WalOp<'_>) -> io::Result<u64> {
        let payload_size = op.payload_size();
        let record_size = REC_HEADER_SIZE + payload_size;

        self.pending_writes.fetch_add(1, Ordering::AcqRel);
        let result = self.append_inner(op, payload_size, record_size);
        self.pending_writes.fetch_sub(1, Ordering::AcqRel);
        result
    }

    fn append_inner(&self, op: WalOp<'_>, payload_size: usize, record_size: usize) -> io::Result<u64> {
        let offset = loop {
            let cur = self.head.load(Ordering::Acquire);
            let new = cur + record_size as u64;
            if new > self.capacity {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "WAL full — consumer reset behind",
                ));
            }
            match self.head.compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break cur,
                Err(_) => continue,
            }
        };

        let lsn = self.next_lsn.fetch_add(1, Ordering::AcqRel);
        let hlc = self.next_hlc();
        let author_peer = hlc.peer;

        let op_byte = op.op_byte();
        let payload_offset = offset as usize + REC_HEADER_SIZE;
        let mmap = self.mmap_mut_slice();
        op.write_payload(&mut mmap[payload_offset..payload_offset + payload_size]);
        let crc = fnv1a(&mmap[payload_offset..payload_offset + payload_size]);

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
        header[OFF_SIGNATURE..OFF_SIGNATURE + 64].copy_from_slice(&ZERO_SIGNATURE);
        header[OFF_PUBKEY_FP..OFF_PUBKEY_FP + 8].copy_from_slice(&ZERO_PUBKEY_FP);

        // ファイルヘッダの head も更新
        mmap[8..16].copy_from_slice(&(offset + record_size as u64).to_le_bytes());

        Ok(lsn)
    }

    /// v30: ring buffer reset。
    /// head == checkpoint && pending_writes == 0 のときのみ実行可能。
    pub fn try_reset(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let cp = self.checkpoint.load(Ordering::Acquire);
        if head != cp || head <= HEADER_SIZE as u64 { return false; }
        if self.pending_writes.load(Ordering::Acquire) > 0 { return false; }
        if self.head.compare_exchange(head, HEADER_SIZE as u64, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return false;
        }
        self.checkpoint.store(HEADER_SIZE as u64, Ordering::Release);
        let mmap = self.mmap_mut_slice();
        mmap[8..16].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        mmap[16..24].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        true
    }

    /// pending_writes を返す(テスト/観測用)。
    pub fn pending_writes(&self) -> u32 {
        self.pending_writes.load(Ordering::Acquire)
    }

    /// fsync(WAL 本体のみ)。consumer スレッドが定期実行。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn fsync(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    #[cfg(target_arch = "wasm32")]
    pub fn fsync(&self) -> io::Result<()> { Ok(()) }

    /// checkpoint を前進。本体 mmap に反映済み LSN の位置まで進める。
    pub fn advance_checkpoint(&self, new_checkpoint: u64) {
        let cur = self.checkpoint.load(Ordering::Acquire);
        if new_checkpoint <= cur { return; }
        self.checkpoint.store(new_checkpoint, Ordering::Release);
        self.mmap_mut_slice()[16..24].copy_from_slice(&new_checkpoint.to_le_bytes());
    }

    /// リカバリ: checkpoint から head までを読んで、Commit で挟まれたグループだけ返す。
    /// CRC 破損レコードに到達したらそこで打ち切り(uncommitted tail の切り捨て)。
    pub fn recover(&self) -> Vec<RecoveredRecord> {
        let mut out = Vec::new();
        let mut batch = Vec::new();
        let mut offset = self.checkpoint.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        let mut max_lsn = 0;
        let mut max_hlc = Hlc::ZERO;

        let mmap = self.mmap_slice();

        while offset < head {
            let rec_end = (offset as usize) + REC_HEADER_SIZE;
            if rec_end > mmap.len() { break; }
            let header = &mmap[offset as usize..rec_end];
            if &header[OFF_MAGIC..OFF_MAGIC + 2] != REC_MAGIC { break; }
            if header[OFF_VERSION] != REC_VERSION { break; }

            let op_byte = header[OFF_OP];
            let payload_len = u32::from_le_bytes(header[OFF_LEN..OFF_LEN + 4].try_into().unwrap()) as usize;
            let lsn = u64::from_le_bytes(header[OFF_LSN..OFF_LSN + 8].try_into().unwrap());
            let hlc_wall = u64::from_le_bytes(header[OFF_HLC_WALL..OFF_HLC_WALL + 8].try_into().unwrap());
            let hlc_logical = u32::from_le_bytes(header[OFF_HLC_LOGICAL..OFF_HLC_LOGICAL + 4].try_into().unwrap());
            let hlc_peer = u32::from_le_bytes(header[OFF_HLC_PEER..OFF_HLC_PEER + 4].try_into().unwrap());
            let author_peer = u32::from_le_bytes(header[OFF_AUTHOR_PEER..OFF_AUTHOR_PEER + 4].try_into().unwrap());
            let stored_crc = u32::from_le_bytes(header[OFF_CRC..OFF_CRC + 4].try_into().unwrap());
            let hlc = Hlc { wall: hlc_wall, logical: hlc_logical, peer: hlc_peer };

            let payload_off = rec_end;
            let payload_end = payload_off + payload_len;
            if payload_end > mmap.len() { break; }

            let computed_crc = fnv1a(&mmap[payload_off..payload_end]);
            if stored_crc != computed_crc { break; } // 破損 tail

            let op = decode_op(op_byte, &mmap[payload_off..payload_end]);
            if lsn > max_lsn { max_lsn = lsn; }
            if hlc > max_hlc { max_hlc = hlc; }

            match op {
                Some(DecodedOp::Commit) => {
                    out.append(&mut batch);
                }
                Some(other) => {
                    batch.push(RecoveredRecord { lsn, hlc, author_peer, op: other });
                }
                None => break,
            }

            offset = (payload_end) as u64;
        }

        // uncommitted batch は破棄。recover の定義通り。
        if max_lsn > 0 {
            self.next_lsn.store(max_lsn + 1, Ordering::Release);
        }
        if max_hlc.wall > 0 {
            self.hlc_last_wall.store(max_hlc.wall, Ordering::Release);
            self.hlc_logical.store(max_hlc.logical, Ordering::Release);
        }

        out
    }

    /// head を checkpoint に戻す(WAL truncate 相当、uncommitted も全捨て)。
    pub fn reset_to_checkpoint(&self) {
        let cp = self.checkpoint.load(Ordering::Acquire);
        self.head.store(cp, Ordering::Release);
        self.mmap_mut_slice()[8..16].copy_from_slice(&cp.to_le_bytes());
    }

    /// WAL 使用量(bytes)と容量を返す(監視用)。
    pub fn usage(&self) -> (u64, u64) {
        let head = self.head.load(Ordering::Acquire);
        let cp = self.checkpoint.load(Ordering::Acquire);
        (head.saturating_sub(cp), self.capacity)
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
        buf[0..4].copy_from_slice(FILE_MAGIC);
        buf[4..8].copy_from_slice(&WAL_FILE_VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        buf[16..24].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        buf[24..32].copy_from_slice(&(capacity as u64).to_le_bytes());
        Self {
            buf,
            capacity: capacity as u64,
            head: AtomicU64::new(HEADER_SIZE as u64),
            checkpoint: AtomicU64::new(HEADER_SIZE as u64),
            next_lsn: AtomicU64::new(1),
            hlc_logical: std::sync::atomic::AtomicU32::new(0),
            hlc_last_wall: AtomicU64::new(0),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

/// 現在時刻を ms since UNIX epoch で返す。wasm32 では 0。
#[inline]
fn current_wall_ms() -> u64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
    #[cfg(target_arch = "wasm32")]
    { 0 }
}

fn decode_op(op_byte: u8, payload: &[u8]) -> Option<DecodedOp> {
    match op_byte {
        op_type::TIE if payload.len() >= 16 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let himo_id = u16::from_le_bytes(payload[8..10].try_into().unwrap());
            let value = u32::from_le_bytes(payload[12..16].try_into().unwrap());
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("enchudb-wal-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn create_and_append() {
        let p = tmp("basic");
        let wal = Wal::create(&p, 1024 * 1024).unwrap();
        let lsn1 = wal.append(WalOp::Tie { eid: 1, himo_id: 0, value: 42 }).unwrap();
        let lsn2 = wal.append(WalOp::Untie { eid: 2, himo_id: 1 }).unwrap();
        let lsn3 = wal.append(WalOp::Commit).unwrap();
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn hlc_monotonic() {
        let p = tmp("hlc");
        let wal = Wal::create(&p, 1024 * 1024).unwrap();
        wal.set_peer_id(7);
        wal.append(WalOp::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        wal.append(WalOp::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();
        wal.append(WalOp::Commit).unwrap();
        wal.fsync().unwrap();

        let wal = Wal::open(&p).unwrap();
        let recs = wal.recover();
        assert_eq!(recs.len(), 2);
        // HLC は単調増加
        assert!(recs[0].hlc <= recs[1].hlc);
        // peer が 7 で記録されている
        assert_eq!(recs[0].hlc.peer, 7);
        assert_eq!(recs[0].author_peer, 7);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn recover_committed_only() {
        let p = tmp("recover");
        {
            let wal = Wal::create(&p, 1024 * 1024).unwrap();
            wal.append(WalOp::Tie { eid: 1, himo_id: 0, value: 100 }).unwrap();
            wal.append(WalOp::Tie { eid: 2, himo_id: 0, value: 200 }).unwrap();
            wal.append(WalOp::Commit).unwrap();
            // uncommitted batch
            wal.append(WalOp::Tie { eid: 3, himo_id: 0, value: 300 }).unwrap();
            wal.fsync().unwrap();
        }

        let wal = Wal::open(&p).unwrap();
        let recs = wal.recover();
        assert_eq!(recs.len(), 2);
        match &recs[0].op {
            DecodedOp::Tie { eid, value, .. } => {
                assert_eq!(*eid, 1);
                assert_eq!(*value, 100);
            }
            _ => panic!(),
        }
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn recover_content() {
        let p = tmp("content");
        {
            let wal = Wal::create(&p, 1024 * 1024).unwrap();
            wal.append(WalOp::Content { eid: 7, key: "memo", data: b"hello world" }).unwrap();
            wal.append(WalOp::Commit).unwrap();
            wal.fsync().unwrap();
        }
        let wal = Wal::open(&p).unwrap();
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
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn signature_slot_is_zero() {
        // Phase A: 署名スロットは確保されるが zeros。
        let p = tmp("sig_zero");
        let wal = Wal::create(&p, 1024 * 1024).unwrap();
        wal.append(WalOp::Tie { eid: 1, himo_id: 0, value: 42 }).unwrap();
        wal.append(WalOp::Commit).unwrap();
        wal.fsync().unwrap();

        // WAL の最初のレコード header を直接読む
        let bytes = std::fs::read(&p).unwrap();
        // File header 32B + Record header からの signature 位置
        let sig_off = 32 + OFF_SIGNATURE;
        let sig = &bytes[sig_off..sig_off + 64];
        assert_eq!(sig, &[0u8; 64]);
        let pubkey_fp = &bytes[32 + OFF_PUBKEY_FP..32 + OFF_PUBKEY_FP + 8];
        assert_eq!(pubkey_fp, &[0u8; 8]);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn checksum_catches_torn_write() {
        let p = tmp("corrupt");
        {
            let wal = Wal::create(&p, 1024 * 1024).unwrap();
            wal.append(WalOp::Tie { eid: 1, himo_id: 0, value: 100 }).unwrap();
            wal.append(WalOp::Commit).unwrap();
            wal.append(WalOp::Tie { eid: 2, himo_id: 0, value: 999 }).unwrap();
            wal.append(WalOp::Commit).unwrap();
            wal.fsync().unwrap();
        }

        // 2 つ目の Tie payload を壊す。
        // ファイルレイアウト: File header 32 + Tie record(112+16=128) + Commit(112+0=112) + Tie record...
        {
            use std::io::Seek;
            use std::io::Write;
            use std::io::Read;
            use std::io::SeekFrom;
            let mut f = OpenOptions::new().read(true).write(true).open(&p).unwrap();
            // 2 つ目の Tie の payload 内の value バイトを破壊
            // 32 (file hdr) + 128 (Tie1) + 112 (Commit) + 112 (Tie2 header) + 12 (value offset in payload) = 396
            f.seek(SeekFrom::Start(32 + 128 + 112 + 112 + 12)).unwrap();
            let mut b = [0u8; 1]; f.read_exact(&mut b).unwrap();
            f.seek(SeekFrom::Current(-1)).unwrap();
            f.write_all(&[b[0] ^ 0xFF]).unwrap();
        }

        let wal = Wal::open(&p).unwrap();
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
        assert!(Wal::open(&p).is_err(), "v1 WAL should be rejected");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn advance_and_reset_checkpoint() {
        let p = tmp("checkpoint");
        let wal = Wal::create(&p, 1024 * 1024).unwrap();
        wal.append(WalOp::Tie { eid: 1, himo_id: 0, value: 50 }).unwrap();
        wal.append(WalOp::Commit).unwrap();
        let head_after = wal.head();
        wal.advance_checkpoint(head_after);
        assert_eq!(wal.checkpoint(), head_after);

        wal.reset_to_checkpoint();
        assert_eq!(wal.head(), head_after);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn concurrent_append() {
        use std::sync::Arc;
        use std::thread;
        let p = tmp("concurrent");
        let wal = Arc::new(Wal::create(&p, 32 * 1024 * 1024).unwrap());
        let mut handles = Vec::new();
        for t in 0..4 {
            let w = wal.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000u64 {
                    w.append(WalOp::Tie {
                        eid: t * 10000 + i,
                        himo_id: 0,
                        value: i as u32,
                    }).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(wal.next_lsn(), 4001);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn try_reset_recycles_wal_space() {
        let p = tmp("ring_reset");
        let wal = Wal::create(&p, 1024 * 1024).unwrap();
        for i in 0..100u32 {
            wal.append(WalOp::Tie { eid: i as u64, himo_id: 0, value: i }).unwrap();
        }
        wal.append(WalOp::Commit).unwrap();
        let head_before = wal.head();
        assert!(head_before > HEADER_SIZE as u64);

        assert!(!wal.try_reset(), "should not reset when checkpoint < head");

        wal.advance_checkpoint(head_before);
        assert!(wal.try_reset(), "should reset when head == checkpoint");
        assert_eq!(wal.head(), HEADER_SIZE as u64);
        assert_eq!(wal.checkpoint(), HEADER_SIZE as u64);

        let lsn = wal.append(WalOp::Tie { eid: 999, himo_id: 0, value: 99 }).unwrap();
        assert!(lsn > 100);

        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn ring_buffer_long_run_does_not_exhaust() {
        let p = tmp("ring_longrun");
        // v2 はヘッダ 112B / record なので、v1 より大きい容量が必要。
        let wal = Wal::create(&p, 512 * 1024).unwrap();

        for batch in 0..50u32 {
            for i in 0..100u32 {
                let v = batch * 100 + i;
                wal.append(WalOp::Tie { eid: v as u64, himo_id: 0, value: v }).unwrap();
            }
            wal.append(WalOp::Commit).unwrap();
            wal.advance_checkpoint(wal.head());
            wal.try_reset();
        }
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn full_wal_returns_error() {
        let p = tmp("full");
        // v2: Tie 1 つ = REC_HEADER 112 + payload 16 = 128B
        // File header 32 + 128 + 余白だけ = 200 確保
        let wal = Wal::create(&p, HEADER_SIZE + 128 + 50).unwrap();
        wal.append(WalOp::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        let r = wal.append(WalOp::Tie { eid: 2, himo_id: 0, value: 2 });
        assert!(r.is_err());
        let _ = std::fs::remove_file(&p);
    }
}
