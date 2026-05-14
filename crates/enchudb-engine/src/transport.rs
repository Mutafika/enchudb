//! Transport — peer 間で WAL レコードをやりとりする抽象。
//!
//! # 責務
//!
//! - リモート peer の WAL を「指定 LSN 以降」で引く(pull)
//! - 自 peer の WAL をリモートに送る(push)。push-based は Phase C 以降で。
//!
//! # 実装
//!
//! - `InMemoryTransport`: テスト用、2+ peer の WAL を Arc<Mutex<HashMap>> で共有
//! - `WebSocketTransport`: Phase C 以降
//! - `HttpTransport`: Phase C 以降
//!
//! # 同期プロトコル(Phase B 初期版)
//!
//! ```text
//! Peer A                            Peer B
//!   │                                 │
//!   │── pull(from_peer=B, since=L)──▶│
//!   │                                 │
//!   │◀── records[] ──────────────────│
//!   │                                 │
//!   │  (LWW で apply、HlcStore 更新)    │
//! ```
//!
//! records は commit 済みの WAL レコードのみ(uncommitted は送らない)。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use enchudb_wal::wal::{DecodedOp, RecoveredRecord};
use enchudb_wal::{Hlc, PeerId};

/// peer 間で交換する 1 件の op。
/// Phase C: signature と pubkey_fp + 署名対象 bytes を同梱する。
#[derive(Debug, Clone)]
pub struct WireRecord {
    pub hlc: Hlc,
    pub author_peer: PeerId,
    pub op: DecodedOp,
    /// ed25519 署名(64B)。zeros なら未署名。
    pub signature: [u8; 64],
    /// 署名した公開鍵の先頭 8B(TOFU で識別に使う)。
    pub pubkey_fp: [u8; 8],
    /// 署名対象の生 bytes(WAL header 固定部 + payload)。
    /// peer 間通信では wire 上で再生成する設計でもいいが、簡便に同梱。
    pub signed_bytes: Vec<u8>,
}

impl From<RecoveredRecord> for WireRecord {
    fn from(r: RecoveredRecord) -> Self {
        Self {
            hlc: r.hlc,
            author_peer: r.author_peer,
            op: r.op,
            signature: r.signature,
            pubkey_fp: r.pubkey_fp,
            signed_bytes: r.signed_bytes,
        }
    }
}

impl WireRecord {
    /// テスト用: 未署名(署名 slot zero)で WireRecord を作る。
    /// 本番経路では Wal::iter_committed() 経由で signed な record が来るべき。
    pub fn unsigned(hlc: Hlc, author_peer: PeerId, op: DecodedOp) -> Self {
        Self {
            hlc, author_peer, op,
            signature: [0u8; 64],
            pubkey_fp: [0u8; 8],
            signed_bytes: Vec::new(),
        }
    }

    /// Wire binary format にエンコード。HTTP transport や WebSocket で流す時に使う。
    /// serde 依存なしで、手動で framing する。
    ///
    /// 形式 (little-endian):
    /// ```text
    /// [version: u8 = 1]
    /// [hlc.wall: u64] [hlc.logical: u32] [hlc.peer: u32]
    /// [author_peer: u32]
    /// [signature: 64B] [pubkey_fp: 8B]
    /// [signed_bytes.len: u32] [signed_bytes: N]
    /// [op_tag: u8]
    ///   0 = Tie:     [eid: u64] [himo_id: u16] [value: u32]
    ///   1 = Untie:   [eid: u64] [himo_id: u16]
    ///   2 = Delete:  [eid: u64]
    ///   3 = Content: [eid: u64] [key_len: u32] [key: N] [data_len: u32] [data: M]
    ///   4 = Commit:  (empty)
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(128 + self.signed_bytes.len());
        out.push(1u8); // version
        out.extend_from_slice(&self.hlc.wall.to_le_bytes());
        out.extend_from_slice(&self.hlc.logical.to_le_bytes());
        out.extend_from_slice(&self.hlc.peer.to_le_bytes());
        out.extend_from_slice(&self.author_peer.to_le_bytes());
        out.extend_from_slice(&self.signature);
        out.extend_from_slice(&self.pubkey_fp);
        out.extend_from_slice(&(self.signed_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.signed_bytes);
        match &self.op {
            DecodedOp::Tie { eid, himo_id, value } => {
                out.push(0);
                out.extend_from_slice(&eid.to_le_bytes());
                out.extend_from_slice(&himo_id.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
            }
            DecodedOp::Untie { eid, himo_id } => {
                out.push(1);
                out.extend_from_slice(&eid.to_le_bytes());
                out.extend_from_slice(&himo_id.to_le_bytes());
            }
            DecodedOp::Delete { eid } => {
                out.push(2);
                out.extend_from_slice(&eid.to_le_bytes());
            }
            DecodedOp::Content { eid, key, data } => {
                out.push(3);
                out.extend_from_slice(&eid.to_le_bytes());
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key.as_bytes());
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(data);
            }
            DecodedOp::Commit => {
                out.push(4);
            }
            DecodedOp::Vocab { vid, bytes } => {
                out.push(5);
                out.extend_from_slice(&vid.to_le_bytes());
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
        out
    }

    /// `encode` の逆関数。`(record, bytes_consumed)` を返す。
    /// format 違反なら `Err`。
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), WireDecodeError> {
        let mut p = 0;
        let need = |p: usize, n: usize, buf: &[u8]| -> Result<(), WireDecodeError> {
            if p + n > buf.len() { Err(WireDecodeError::Truncated) } else { Ok(()) }
        };
        need(p, 1, buf)?;
        let ver = buf[p]; p += 1;
        if ver != 1 { return Err(WireDecodeError::UnsupportedVersion(ver)); }

        need(p, 16, buf)?;
        let wall = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
        let logical = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()); p += 4;
        let hlc_peer = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()); p += 4;

        need(p, 4, buf)?;
        let author_peer = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()); p += 4;

        need(p, 64, buf)?;
        let mut signature = [0u8; 64];
        signature.copy_from_slice(&buf[p..p+64]); p += 64;

        need(p, 8, buf)?;
        let mut pubkey_fp = [0u8; 8];
        pubkey_fp.copy_from_slice(&buf[p..p+8]); p += 8;

        need(p, 4, buf)?;
        let sb_len = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()) as usize; p += 4;
        need(p, sb_len, buf)?;
        let signed_bytes = buf[p..p+sb_len].to_vec(); p += sb_len;

        need(p, 1, buf)?;
        let op_tag = buf[p]; p += 1;
        let op = match op_tag {
            0 => {
                need(p, 14, buf)?;
                let eid = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
                let himo_id = u16::from_le_bytes(buf[p..p+2].try_into().unwrap()); p += 2;
                let value = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()); p += 4;
                DecodedOp::Tie { eid, himo_id, value }
            }
            1 => {
                need(p, 10, buf)?;
                let eid = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
                let himo_id = u16::from_le_bytes(buf[p..p+2].try_into().unwrap()); p += 2;
                DecodedOp::Untie { eid, himo_id }
            }
            2 => {
                need(p, 8, buf)?;
                let eid = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
                DecodedOp::Delete { eid }
            }
            3 => {
                need(p, 12, buf)?;
                let eid = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
                let key_len = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()) as usize; p += 4;
                need(p, key_len, buf)?;
                let key = std::str::from_utf8(&buf[p..p+key_len])
                    .map_err(|_| WireDecodeError::InvalidUtf8)?
                    .to_string();
                p += key_len;
                need(p, 4, buf)?;
                let data_len = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()) as usize; p += 4;
                need(p, data_len, buf)?;
                let data = buf[p..p+data_len].to_vec(); p += data_len;
                DecodedOp::Content { eid, key, data }
            }
            4 => DecodedOp::Commit,
            5 => {
                need(p, 8, buf)?;
                let vid = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()); p += 4;
                let blen = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()) as usize; p += 4;
                need(p, blen, buf)?;
                let bytes = buf[p..p+blen].to_vec(); p += blen;
                DecodedOp::Vocab { vid, bytes }
            }
            other => return Err(WireDecodeError::UnknownOpTag(other)),
        };

        Ok((
            Self {
                hlc: Hlc { wall, logical, peer: hlc_peer },
                author_peer,
                op,
                signature,
                pubkey_fp,
                signed_bytes,
            },
            p,
        ))
    }
}

/// WireRecord::decode のエラー。
#[derive(Debug)]
pub enum WireDecodeError {
    Truncated,
    UnsupportedVersion(u8),
    UnknownOpTag(u8),
    InvalidUtf8,
}

impl std::fmt::Display for WireDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "wire record truncated"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported wire version: {}", v),
            Self::UnknownOpTag(t) => write!(f, "unknown op tag: {}", t),
            Self::InvalidUtf8 => write!(f, "invalid utf-8 in content key"),
        }
    }
}

impl std::error::Error for WireDecodeError {}

/// 複数 WireRecord を長さ前置で framing して 1 バイト列にまとめる。
/// HTTP body や file 等に乗せる用。
///
/// 形式: `[count: u32] [rec_len: u32] [rec...] [rec_len: u32] [rec...] ...`
pub fn encode_batch(records: &[WireRecord]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for r in records {
        let enc = r.encode();
        out.extend_from_slice(&(enc.len() as u32).to_le_bytes());
        out.extend_from_slice(&enc);
    }
    out
}

/// `encode_batch` の逆。
pub fn decode_batch(buf: &[u8]) -> Result<Vec<WireRecord>, WireDecodeError> {
    if buf.len() < 4 { return Err(WireDecodeError::Truncated); }
    let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut p = 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if p + 4 > buf.len() { return Err(WireDecodeError::Truncated); }
        let rec_len = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()) as usize;
        p += 4;
        if p + rec_len > buf.len() { return Err(WireDecodeError::Truncated); }
        let (rec, consumed) = WireRecord::decode(&buf[p..p+rec_len])?;
        if consumed != rec_len {
            return Err(WireDecodeError::Truncated);
        }
        out.push(rec);
        p += rec_len;
    }
    Ok(out)
}

/// 同期 transport。
///
/// ブロッキング API。将来的に async trait 化予定(Phase C)。
pub trait Transport: Send + Sync {
    /// `from` peer の、HLC が `since` より後のレコードを取得。
    /// 結果は HLC 昇順。
    fn pull(&self, from: PeerId, since: Hlc) -> Vec<WireRecord>;

    /// 自 peer の commit 済みレコードを broadcast(publish 相当)。
    /// Phase B InMemoryTransport では「共有 log に append する」だけ。
    fn publish(&self, peer: PeerId, records: Vec<WireRecord>);
}

/// テスト用: プロセス内で peer 間の WAL を共有する。
///
/// peer ごとに `(ordered log of WireRecord)` を持つ。HLC 昇順で入れる想定。
#[derive(Default, Clone)]
pub struct InMemoryTransport {
    inner: Arc<Mutex<HashMap<PeerId, Vec<WireRecord>>>>,
}

impl InMemoryTransport {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// 現在 `from` peer が持っている全レコード数(テスト用)。
    pub fn len_of(&self, from: PeerId) -> usize {
        self.inner.lock().unwrap().get(&from).map(|v| v.len()).unwrap_or(0)
    }
}

impl Transport for InMemoryTransport {
    fn pull(&self, from: PeerId, since: Hlc) -> Vec<WireRecord> {
        let guard = self.inner.lock().unwrap();
        let log = match guard.get(&from) {
            Some(l) => l,
            None => return Vec::new(),
        };
        log.iter()
            .filter(|r| r.hlc > since)
            .cloned()
            .collect()
    }

    fn publish(&self, peer: PeerId, mut records: Vec<WireRecord>) {
        if records.is_empty() { return; }
        records.sort_by_key(|r| r.hlc);
        let mut guard = self.inner.lock().unwrap();
        let log = guard.entry(peer).or_insert_with(Vec::new);
        log.extend(records);
        log.sort_by_key(|r| r.hlc);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enchudb_wal::wal::DecodedOp;

    fn rec(hlc_wall: u64, peer: PeerId, eid: u64, value: u32) -> WireRecord {
        WireRecord {
            hlc: Hlc { wall: hlc_wall, logical: 0, peer },
            author_peer: peer,
            op: DecodedOp::Tie { eid, himo_id: 0, value },
            signature: [0u8; 64],
            pubkey_fp: [0u8; 8],
            signed_bytes: Vec::new(),
        }
    }

    #[test]
    fn pull_returns_records_after_since() {
        let t = InMemoryTransport::new();
        t.publish(0, vec![
            rec(100, 0, 1, 10),
            rec(200, 0, 2, 20),
            rec(300, 0, 3, 30),
        ]);
        let since = Hlc { wall: 150, logical: 0, peer: 0 };
        let out = t.pull(0, since);
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0].op, DecodedOp::Tie { eid: 2, .. }));
        assert!(matches!(out[1].op, DecodedOp::Tie { eid: 3, .. }));
    }

    #[test]
    fn pull_unknown_peer_empty() {
        let t = InMemoryTransport::new();
        let out = t.pull(42, Hlc::ZERO);
        assert!(out.is_empty());
    }

    #[test]
    fn publish_sorts_by_hlc() {
        let t = InMemoryTransport::new();
        t.publish(0, vec![
            rec(300, 0, 3, 30),
            rec(100, 0, 1, 10),
            rec(200, 0, 2, 20),
        ]);
        let out = t.pull(0, Hlc::ZERO);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].hlc.wall, 100);
        assert_eq!(out[1].hlc.wall, 200);
        assert_eq!(out[2].hlc.wall, 300);
    }

    #[test]
    fn pull_with_same_hlc_excluded() {
        let t = InMemoryTransport::new();
        let h1 = Hlc { wall: 100, logical: 0, peer: 0 };
        t.publish(0, vec![rec(100, 0, 1, 10)]);
        // since = 記録されている HLC と同じ → exclusive なので空
        let out = t.pull(0, h1);
        assert!(out.is_empty());
    }

    #[test]
    fn encode_decode_tie_roundtrip() {
        let orig = WireRecord {
            hlc: Hlc { wall: 12345, logical: 7, peer: 42 },
            author_peer: 42,
            op: DecodedOp::Tie { eid: 0x1234_5678_9abc_def0, himo_id: 99, value: 777 },
            signature: {
                let mut s = [0u8; 64];
                for i in 0..64 { s[i] = i as u8; }
                s
            },
            pubkey_fp: [1, 2, 3, 4, 5, 6, 7, 8],
            signed_bytes: b"hello world signed".to_vec(),
        };
        let enc = orig.encode();
        let (dec, used) = WireRecord::decode(&enc).unwrap();
        assert_eq!(used, enc.len());
        assert_eq!(dec.hlc, orig.hlc);
        assert_eq!(dec.author_peer, orig.author_peer);
        assert_eq!(dec.signature, orig.signature);
        assert_eq!(dec.pubkey_fp, orig.pubkey_fp);
        assert_eq!(dec.signed_bytes, orig.signed_bytes);
        match (dec.op, orig.op) {
            (DecodedOp::Tie { eid: e1, himo_id: h1, value: v1 },
             DecodedOp::Tie { eid: e2, himo_id: h2, value: v2 }) => {
                assert_eq!(e1, e2); assert_eq!(h1, h2); assert_eq!(v1, v2);
            }
            _ => panic!("op mismatch"),
        }
    }

    #[test]
    fn encode_decode_all_ops() {
        let variants = vec![
            DecodedOp::Tie { eid: 1, himo_id: 2, value: 3 },
            DecodedOp::Untie { eid: 4, himo_id: 5 },
            DecodedOp::Delete { eid: 6 },
            DecodedOp::Content {
                eid: 7,
                key: "memo".to_string(),
                data: b"binary \x00\x01\xff payload".to_vec(),
            },
            DecodedOp::Commit,
        ];
        for op in variants {
            let orig = WireRecord::unsigned(Hlc { wall: 1, logical: 0, peer: 1 }, 1, op);
            let enc = orig.encode();
            let (dec, used) = WireRecord::decode(&enc).unwrap();
            assert_eq!(used, enc.len());
            // op 比較は Debug 文字列で代用 (PartialEq 無いため)
            assert_eq!(format!("{:?}", dec.op), format!("{:?}", orig.op));
        }
    }

    #[test]
    fn decode_rejects_truncated() {
        let orig = WireRecord::unsigned(
            Hlc { wall: 1, logical: 0, peer: 1 },
            1,
            DecodedOp::Delete { eid: 99 },
        );
        let enc = orig.encode();
        for cut in 0..enc.len() {
            let err = WireRecord::decode(&enc[..cut]);
            assert!(err.is_err(), "truncated at {} must fail", cut);
        }
    }

    #[test]
    fn batch_roundtrip() {
        let records = vec![
            rec(100, 1, 10, 100),
            rec(200, 1, 20, 200),
            rec(300, 2, 30, 300),
        ];
        let enc = encode_batch(&records);
        let dec = decode_batch(&enc).unwrap();
        assert_eq!(dec.len(), 3);
        assert_eq!(dec[0].hlc.wall, 100);
        assert_eq!(dec[1].hlc.wall, 200);
        assert_eq!(dec[2].hlc.wall, 300);
    }

    #[test]
    fn batch_empty() {
        let enc = encode_batch(&[]);
        let dec = decode_batch(&enc).unwrap();
        assert!(dec.is_empty());
    }
}
