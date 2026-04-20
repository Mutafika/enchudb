//! Write-Ahead Log (v28) — crash consistency for v27 op-based writes.
//!
//! # 設計原則
//!
//! - **読みは WAL に触らない**(pull_raw / query の 10〜70ns は影響ゼロ)
//! - **書きは WAL append を memcpy 1 回で済ます**(tie_async の ~1μs を維持)
//! - **fsync は hot path から外す**(非同期、consumer スレッド側で定期実行)
//!
//! # レコード形式
//!
//! ```text
//! [magic: 2B "WL"]
//! [op_type: 1B]    0=Tie, 1=Untie, 2=Delete, 3=Content, 4=Commit
//! [_pad: 1B]
//! [len: 4B LE]     payload bytes(header + crc は除く)
//! [lsn: 8B LE]     Log Sequence Number(単調増加)
//! [crc32: 4B LE]   payload の FNV-1a(v10 互換)
//! [payload: len B] op 固有
//! ```
//!
//! Tie payload: [eid: 4B][himo_id: 2B][_pad: 2B][value: 4B]     = 12B
//! Untie payload: [eid: 4B][himo_id: 2B][_pad: 2B]              = 8B
//! Delete payload: [eid: 4B]                                     = 4B
//! Content payload: [eid: 4B][key_len: 2B][data_len: 4B][key][data]
//! Commit payload: なし(len = 0)
//!
//! # ファイル配置
//!
//! 別ファイル `{db_path}.wal`。sparse mmap、初期 256MB。
//!
//! ```text
//! [File header 32B]
//!   [magic: 4B "EWAL"]
//!   [version: 4B LE]  = 1
//!   [head: 8B LE]     writer が atomic 前進
//!   [checkpoint: 8B LE] consumer が前進(ここまで本体に適用済み)
//!   [capacity: 8B LE] buffer size
//! [Records starting at offset 32]
//! ```
//!
//! head, checkpoint は**ファイル内のオフセット**(byte 単位)。
//! atomic なのは mmap 経由での読み書き。プロセス内では AtomicU64 でキャッシュ。

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use memmap2::MmapMut;

const FILE_MAGIC: &[u8; 4] = b"EWAL";
const FILE_VERSION: u32 = 1;
const HEADER_SIZE: usize = 32;

const REC_MAGIC: &[u8; 2] = b"WL";
const REC_HEADER_SIZE: usize = 20; // magic(2) + op(1) + pad(1) + len(4) + lsn(8) + crc(4)

/// WAL 初期サイズ(256MB、sparse なので実ディスク消費は書いた分のみ)。
pub const DEFAULT_WAL_SIZE: usize = 256 * 1024 * 1024;

/// op_type バイト値。
pub mod op_type {
    pub const TIE: u8 = 0;
    pub const UNTIE: u8 = 1;
    pub const DELETE: u8 = 2;
    pub const CONTENT: u8 = 3;
    pub const COMMIT: u8 = 4;
}

/// WAL に書く op。
#[derive(Debug, Clone)]
pub enum WalOp<'a> {
    Tie { eid: u32, himo_id: u16, value: u32 },
    Untie { eid: u32, himo_id: u16 },
    Delete { eid: u32 },
    Content { eid: u32, key: &'a str, data: &'a [u8] },
    Commit,
}

impl<'a> WalOp<'a> {
    /// payload の byte 数(固定 or 動的)。
    #[inline]
    fn payload_size(&self) -> usize {
        match self {
            WalOp::Tie { .. } => 12,
            WalOp::Untie { .. } => 8,
            WalOp::Delete { .. } => 4,
            WalOp::Content { key, data, .. } => 4 + 2 + 4 + key.len() + data.len(),
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
                buf[0..4].copy_from_slice(&eid.to_le_bytes());
                buf[4..6].copy_from_slice(&himo_id.to_le_bytes());
                buf[6..8].copy_from_slice(&[0, 0]);
                buf[8..12].copy_from_slice(&value.to_le_bytes());
            }
            WalOp::Untie { eid, himo_id } => {
                buf[0..4].copy_from_slice(&eid.to_le_bytes());
                buf[4..6].copy_from_slice(&himo_id.to_le_bytes());
                buf[6..8].copy_from_slice(&[0, 0]);
            }
            WalOp::Delete { eid } => {
                buf[0..4].copy_from_slice(&eid.to_le_bytes());
            }
            WalOp::Content { eid, key, data } => {
                buf[0..4].copy_from_slice(&eid.to_le_bytes());
                let klen = key.len() as u16;
                let dlen = data.len() as u32;
                buf[4..6].copy_from_slice(&klen.to_le_bytes());
                buf[6..10].copy_from_slice(&dlen.to_le_bytes());
                let ko = 10;
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
    Tie { eid: u32, himo_id: u16, value: u32 },
    Untie { eid: u32, himo_id: u16 },
    Delete { eid: u32 },
    Content { eid: u32, key: String, data: Vec<u8> },
    Commit,
}

/// リカバリ結果の 1 レコード。
#[derive(Debug, Clone)]
pub struct RecoveredRecord {
    pub lsn: u64,
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
        mmap[4..8].copy_from_slice(&FILE_VERSION.to_le_bytes());
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
            pending_writes: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// 既存 WAL を開く。
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
        let head = u64::from_le_bytes(mmap[8..16].try_into().unwrap());
        let checkpoint = u64::from_le_bytes(mmap[16..24].try_into().unwrap());
        let capacity = u64::from_le_bytes(mmap[24..32].try_into().unwrap());

        // next_lsn は recover 後に update されるので、暫定 1。
        Ok(Self {
            _file: file,
            mmap,
            capacity,
            head: AtomicU64::new(head),
            checkpoint: AtomicU64::new(checkpoint),
            next_lsn: AtomicU64::new(1),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
        })
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
                // capacity 超過 — consumer が reset してくれるのを待つ
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

        let op_byte = op.op_byte();
        let payload_offset = offset as usize + REC_HEADER_SIZE;
        let mmap = self.mmap_mut_slice();
        op.write_payload(&mut mmap[payload_offset..payload_offset + payload_size]);
        let crc = fnv1a(&mmap[payload_offset..payload_offset + payload_size]);

        let header = &mut mmap[offset as usize..offset as usize + REC_HEADER_SIZE];
        header[0..2].copy_from_slice(REC_MAGIC);
        header[2] = op_byte;
        header[3] = 0;
        header[4..8].copy_from_slice(&(payload_size as u32).to_le_bytes());
        header[8..16].copy_from_slice(&lsn.to_le_bytes());
        header[16..20].copy_from_slice(&crc.to_le_bytes());

        mmap[8..16].copy_from_slice(&(offset + record_size as u64).to_le_bytes());

        Ok(lsn)
    }

    /// v30: ring buffer reset。
    /// head == checkpoint && pending_writes == 0 のときのみ実行可能。
    /// 成功したら head と checkpoint を HEADER_SIZE に戻す(全領域再利用可能に)。
    /// consumer 周期 fsync 内で periodic に呼ばれる想定。
    pub fn try_reset(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let cp = self.checkpoint.load(Ordering::Acquire);
        if head != cp || head <= HEADER_SIZE as u64 { return false; }
        if self.pending_writes.load(Ordering::Acquire) > 0 { return false; }
        // もう一度 head == checkpoint を確認(race でずれた場合の保険)
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
    /// 引数は新しい checkpoint offset(bytes)。
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

        let mmap = self.mmap_slice();

        while offset < head {
            let rec_end = (offset as usize) + REC_HEADER_SIZE;
            if rec_end > mmap.len() { break; }
            let header = &mmap[offset as usize..rec_end];
            if &header[0..2] != REC_MAGIC { break; }

            let op_byte = header[2];
            let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
            let lsn = u64::from_le_bytes(header[8..16].try_into().unwrap());
            let stored_crc = u32::from_le_bytes(header[16..20].try_into().unwrap());

            let payload_off = rec_end;
            let payload_end = payload_off + payload_len;
            if payload_end > mmap.len() { break; }

            let computed_crc = fnv1a(&mmap[payload_off..payload_end]);
            if stored_crc != computed_crc { break; } // 破損 tail

            // デコード
            let op = decode_op(op_byte, &mmap[payload_off..payload_end]);
            if lsn > max_lsn { max_lsn = lsn; }

            match op {
                Some(DecodedOp::Commit) => {
                    out.append(&mut batch);
                }
                Some(other) => {
                    batch.push(RecoveredRecord { lsn, op: other });
                }
                None => break, // 未知 op → 安全側に倒して停止
            }

            offset = (payload_end) as u64;
        }

        // uncommitted batch は破棄(drop)。recover の定義通り。
        // 次回 append のために next_lsn を更新。
        if max_lsn > 0 {
            self.next_lsn.store(max_lsn + 1, Ordering::Release);
        }

        out
    }

    /// head を checkpoint に戻す(WAL truncate 相当、uncommitted も全捨て)。
    /// チェックポイント完了後、WAL を空に戻す時に使う。
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
        // mmap 経由の書き込みは CAS + atomic で管理するため、unsafe で可変化。
        // Engine の設計(concurrent writer)と整合。
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
        buf[4..8].copy_from_slice(&FILE_VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        buf[16..24].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        buf[24..32].copy_from_slice(&(capacity as u64).to_le_bytes());
        Self {
            buf,
            capacity: capacity as u64,
            head: AtomicU64::new(HEADER_SIZE as u64),
            checkpoint: AtomicU64::new(HEADER_SIZE as u64),
            next_lsn: AtomicU64::new(1),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

fn decode_op(op_byte: u8, payload: &[u8]) -> Option<DecodedOp> {
    match op_byte {
        op_type::TIE if payload.len() >= 12 => {
            let eid = u32::from_le_bytes(payload[0..4].try_into().unwrap());
            let himo_id = u16::from_le_bytes(payload[4..6].try_into().unwrap());
            let value = u32::from_le_bytes(payload[8..12].try_into().unwrap());
            Some(DecodedOp::Tie { eid, himo_id, value })
        }
        op_type::UNTIE if payload.len() >= 8 => {
            let eid = u32::from_le_bytes(payload[0..4].try_into().unwrap());
            let himo_id = u16::from_le_bytes(payload[4..6].try_into().unwrap());
            Some(DecodedOp::Untie { eid, himo_id })
        }
        op_type::DELETE if payload.len() >= 4 => {
            let eid = u32::from_le_bytes(payload[0..4].try_into().unwrap());
            Some(DecodedOp::Delete { eid })
        }
        op_type::CONTENT if payload.len() >= 10 => {
            let eid = u32::from_le_bytes(payload[0..4].try_into().unwrap());
            let klen = u16::from_le_bytes(payload[4..6].try_into().unwrap()) as usize;
            let dlen = u32::from_le_bytes(payload[6..10].try_into().unwrap()) as usize;
            if payload.len() < 10 + klen + dlen { return None; }
            let key = String::from_utf8(payload[10..10 + klen].to_vec()).ok()?;
            let data = payload[10 + klen..10 + klen + dlen].to_vec();
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
        // Commit 済みの 2 件のみ。eid=3 は uncommitted で破棄。
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

        // 2つ目のレコードの payload を壊す
        {
            use std::io::Seek;
            use std::io::Write;
            use std::io::Read;
            use std::io::SeekFrom;
            let mut f = OpenOptions::new().read(true).write(true).open(&p).unwrap();
            // 最初のレコード: header(20) + payload(12) = 32B、Commit: 20B = 52
            // 2つ目 Tie: payload 先頭にオフセット HEADER_SIZE(32) + 52 + 20(header) = 104
            f.seek(SeekFrom::Start(HEADER_SIZE as u64 + 20 + 12 + 20 + 20 + 5)).unwrap();
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
        let wal = Arc::new(Wal::create(&p, 16 * 1024 * 1024).unwrap());
        let mut handles = Vec::new();
        for t in 0..4 {
            let w = wal.clone();
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    w.append(WalOp::Tie {
                        eid: (t * 10000 + i) as u32,
                        himo_id: 0,
                        value: i as u32,
                    }).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        // 4 * 1000 = 4000 レコード
        // LSN は 4001 まで発行されてる
        assert_eq!(wal.next_lsn(), 4001);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn try_reset_recycles_wal_space() {
        let p = tmp("ring_reset");
        let wal = Wal::create(&p, 1024 * 1024).unwrap();
        // 100 件書く → head 前進
        for i in 0..100u32 {
            wal.append(WalOp::Tie { eid: i, himo_id: 0, value: i }).unwrap();
        }
        wal.append(WalOp::Commit).unwrap();
        let head_before = wal.head();
        assert!(head_before > HEADER_SIZE as u64);

        // checkpoint がまだ追いついてない → reset 失敗
        assert!(!wal.try_reset(), "should not reset when checkpoint < head");

        // checkpoint を head まで進める
        wal.advance_checkpoint(head_before);
        // これで head == checkpoint && pending == 0 → reset 成功
        assert!(wal.try_reset(), "should reset when head == checkpoint");
        assert_eq!(wal.head(), HEADER_SIZE as u64);
        assert_eq!(wal.checkpoint(), HEADER_SIZE as u64);

        // reset 後も append できる
        let lsn = wal.append(WalOp::Tie { eid: 999, himo_id: 0, value: 99 }).unwrap();
        assert!(lsn > 100);

        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn ring_buffer_long_run_does_not_exhaust() {
        let p = tmp("ring_longrun");
        // 容量は小さめ(128KB)にして、何度もリセット必要な状態を作る
        let wal = Wal::create(&p, 128 * 1024).unwrap();

        // 合計で容量の何十倍も書く(周期的に reset する)
        for batch in 0..50u32 {
            // 100 件書く = ~3KB。50 バッチで 150KB 書いて、容量 128KB
            for i in 0..100u32 {
                let v = batch * 100 + i;
                wal.append(WalOp::Tie { eid: v, himo_id: 0, value: v }).unwrap();
            }
            wal.append(WalOp::Commit).unwrap();
            // checkpoint 前進 + reset
            wal.advance_checkpoint(wal.head());
            wal.try_reset();
        }
        // エラー無しで完走できれば ring 動作 OK
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn full_wal_returns_error() {
        let p = tmp("full");
        let wal = Wal::create(&p, HEADER_SIZE + 50).unwrap(); // 極小
        // tie 1 つ = 20 + 12 = 32B
        wal.append(WalOp::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        // 次の tie はもう入らない
        let r = wal.append(WalOp::Tie { eid: 2, himo_id: 0, value: 2 });
        assert!(r.is_err());
        let _ = std::fs::remove_file(&p);
    }
}
