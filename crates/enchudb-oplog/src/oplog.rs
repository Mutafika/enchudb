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
//! [author_peer: 4B LE]  record を書いた peer の id
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
//! 別ファイル `{db_path}.oplog` (0.5.0 までは `.wal`)。 sparse mmap、 初期 256MB。
//!
//! ```text
//! [File header 32B]
//!   [magic: 4B "EWAL"]   = 歴史的経緯で wire 不変。
//!   [version: 4B LE]     = 2
//!   [head: 8B LE]        writer が atomic 前進
//!   [checkpoint: 8B LE]  consumer が前進(ここまで本体に適用済み)
//!   [capacity: 8B LE]    buffer size
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
pub const OPLOG_FILE_VERSION: u32 = 2;
pub const HEADER_SIZE: usize = 32;

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

    // signed_bytes header parse
    if &signed_bytes[0..2] != REC_MAGIC { return None; }
    if signed_bytes[2] != REC_VERSION { return None; }
    let op_byte = signed_bytes[3];
    let payload_len = u32::from_le_bytes(signed_bytes[4..8].try_into().ok()?) as usize;
    let lsn = u64::from_le_bytes(signed_bytes[8..16].try_into().ok()?);
    let hlc_wall = u64::from_le_bytes(signed_bytes[16..24].try_into().ok()?);
    let hlc_logical = u32::from_le_bytes(signed_bytes[24..28].try_into().ok()?);
    let hlc_peer = u32::from_le_bytes(signed_bytes[28..32].try_into().ok()?);
    let author_peer = u32::from_le_bytes(signed_bytes[32..36].try_into().ok()?);
    let _stored_crc = u32::from_le_bytes(signed_bytes[36..40].try_into().ok()?);
    let payload_off = SIGNED_PAYLOAD_HEADER_SIZE;
    let payload_end = payload_off + payload_len;
    if payload_end > signed_bytes.len() { return None; }
    let op_payload = &signed_bytes[payload_off..payload_end];
    let op = decode_op(op_byte, op_payload)?;
    let hlc = Hlc { wall: hlc_wall, logical: hlc_logical, peer: hlc_peer };

    Some(Record {
        lsn, hlc, author_peer, op, signature, pubkey_fp,
        signed_bytes: signed_bytes.to_vec(),
    })
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
///   crc (payload の FNV-1a) 再計算 + 全体 re-sign で完結する
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
        DecodedOp::Commit | DecodedOp::Vocab { .. } => return None,
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
    // crc は payload のみが対象 (append 時と同じ規約)
    let crc = fnv1a(&sb[payload_off..payload_end]);
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
    let crc = fnv1a(&sb[SIGNED_PAYLOAD_HEADER_SIZE..]);
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
}

/// WAL に書く所有型 op。 `Op` の owned 版で、 queue 渡し用 (consumer 側で
/// batch して `append_many` に流す経路で使う)。
#[derive(Debug, Clone)]
pub enum OwnedOp {
    Tie { eid: u64, himo_id: u16, value: u32 },
    Untie { eid: u64, himo_id: u16 },
    Delete { eid: u64 },
    Content { eid: u64, key: String, data: Vec<u8> },
    Commit,
    Vocab { vid: u32, bytes: Vec<u8> },
    /// 0.9.0: himo を full name で運ぶ Tie (content 互換層用)。
    /// `himo_kind` は ValueType の生 u8 (受信側の ensure 用)。
    TieNamed { eid: u64, himo_name: String, himo_kind: u8, value: u32 },
    /// 0.12.0 (#88): Leaf payload を bytes 同乗で運ぶ (vid 無し、 himo は名前)。
    TieLeaf { eid: u64, himo_name: String, himo_kind: u8, bytes: Vec<u8> },
}

impl OwnedOp {
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
    Tie { eid: u64, himo_id: u16, value: u32 },
    Untie { eid: u64, himo_id: u16 },
    Delete { eid: u64 },
    Content { eid: u64, key: &'a str, data: &'a [u8] },
    Commit,
    /// text 文字列を peer 間で運ぶ。`vid` は author_peer ローカルの vocab ID。
    /// receiver 側は `(author_peer, vid) → local_vid` の mapping を持ち、
    /// 後続の Tie { value: vid } を受けたら local_vid に変換して適用する。
    Vocab { vid: u32, bytes: &'a [u8] },
    /// 0.9.0: himo を full name で運ぶ Tie。 動的 himo (content `_c_{key}`) 用。
    TieNamed { eid: u64, himo_name: &'a str, himo_kind: u8, value: u32 },
    /// 0.12.0 (#88): Leaf payload を bytes 同乗で運ぶ Tie (vid 無し、 himo は名前)。
    TieLeaf { eid: u64, himo_name: &'a str, himo_kind: u8, bytes: &'a [u8] },
}

impl<'a> Op<'a> {
    /// payload の byte 数(固定 or 動的)。v2 layout。
    #[inline]
    fn payload_size(&self) -> usize {
        match self {
            Op::Tie { .. } => 16,       // eid(8) + himo_id(2) + pad(2) + value(4)
            Op::Untie { .. } => 16,     // eid(8) + himo_id(2) + pad(6)
            Op::Delete { .. } => 8,     // eid(8)
            Op::Content { key, data, .. } => 8 + 2 + 2 + 4 + key.len() + data.len(),
            Op::Commit => 0,
            Op::Vocab { bytes, .. } => 4 + 4 + bytes.len(), // vid(4) + len(4) + bytes
            // eid(8) + value(4) + kind(1) + pad(1) + name_len(2) + name
            Op::TieNamed { himo_name, .. } => 16 + himo_name.len(),
            // eid(8) + kind(1) + pad(1) + name_len(2) + bytes_len(4) + name + bytes
            Op::TieLeaf { himo_name, bytes, .. } => 16 + himo_name.len() + bytes.len(),
        }
    }

    fn op_byte(&self) -> u8 {
        match self {
            Op::Tie { .. } => op_type::TIE,
            Op::Untie { .. } => op_type::UNTIE,
            Op::Delete { .. } => op_type::DELETE,
            Op::Content { .. } => op_type::CONTENT,
            Op::Commit => op_type::COMMIT,
            Op::Vocab { .. } => op_type::VOCAB,
            Op::TieNamed { .. } => op_type::TIE_NAMED,
            Op::TieLeaf { .. } => op_type::TIE_LEAF,
        }
    }

    fn write_payload(&self, buf: &mut [u8]) {
        match self {
            Op::Tie { eid, himo_id, value } => {
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..10].copy_from_slice(&himo_id.to_le_bytes());
                buf[10..12].copy_from_slice(&[0, 0]);
                buf[12..16].copy_from_slice(&value.to_le_bytes());
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
            Op::TieNamed { eid, himo_name, himo_kind, value } => {
                assert!(himo_name.len() <= u16::MAX as usize, "himo name too long");
                buf[0..8].copy_from_slice(&eid.to_le_bytes());
                buf[8..12].copy_from_slice(&value.to_le_bytes());
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
    /// peer 間で vocab の (vid, bytes) を運ぶ。`vid` は record の author_peer ローカル。
    Vocab { vid: u32, bytes: Vec<u8> },
    /// 0.9.0: himo full name 付き Tie (動的 content himo 用)。
    TieNamed { eid: u64, himo_name: String, himo_kind: u8, value: u32 },
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
#[cfg(not(target_arch = "wasm32"))]
struct OpLogLockGuard<'a> {
    file: &'a File,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for OpLogLockGuard<'_> {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub struct OpLog {
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
    /// HLC logical counter(wall が進まない時の tiebreaker 単調増加)。
    hlc_logical: std::sync::atomic::AtomicU32,
    /// 最後に書いた HLC wall(ms)。
    hlc_last_wall: AtomicU64,
    /// この OpLog を持つ peer の id(header には書かず Engine から設定)。
    peer_id: std::sync::atomic::AtomicU32,
    /// ed25519 鍵ペア。set_keypair で設定。None なら署名は zeros。
    keypair: std::sync::RwLock<Option<std::sync::Arc<crate::keys::Keypair>>>,
    /// #75: 同一プロセス内 append の直列化。 flock は open file description
    /// 単位のため、 同じ File を共有するスレッド間では排他にならない (2 本目の
    /// LOCK_EX が「既保持の変換」として即成功する)。 head 採番 (read-modify-
    /// write) と `next_hlc` はこの lock の下でのみ安全。 cross-process 排他は
    /// 従来通り flock が担う。
    append_lock: std::sync::Mutex<()>,
    /// append 中の writer 数。try_reset はこれが 0 のときだけ実行できる。
    pending_writes: std::sync::atomic::AtomicU32,
    /// auto reset を許可するか。default false。
    /// true にすると consumer が head==checkpoint の tick で ring buffer を空に戻し
    /// 長期運用で WAL 容量を食い切らない動きになるが、audit/iter_committed/publish_since
    /// で読む前に消える race があるので opt-in。
    auto_reset: std::sync::atomic::AtomicBool,
}

unsafe impl Send for OpLog {}
unsafe impl Sync for OpLog {}

// テスト専用: 次の `append_inner` 呼び出しを panic させるフラグ (issue #58② 検証用)。
// thread-local なので並行テストでも干渉しない。 release build には残らない。
#[cfg(test)]
thread_local! {
    static FAULT_INJECT_APPEND_PANIC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// `pending_writes` を RAII で減算するガード。
///
/// issue #58: 旧コードは `fetch_add` → `append_inner` → `fetch_sub` を直列で
/// 並べていたため、 `append_inner` が panic すると `fetch_sub` が skip され
/// counter が +1 のまま残り、 `try_reset`(`pending_writes == 0` 条件)が
/// 永久に発火しなくなった。 drop で必ず減算することで panic 経路でも均衡する。
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
        self.pending_writes.fetch_add(n, Ordering::AcqRel);
        PendingGuard { counter: &self.pending_writes, n }
    }

    /// oplog 容量到達時の `OutOfMemory` エラーを生成する。
    ///
    /// issue #57: caller(engine の tie/untie/delete 経路)はこの Err を
    /// `let _ =` で握り潰すため、 append 失敗が **silent な op 欠落**になっていた。
    /// 少なくとも検知だけは可能にするため、 エラー生成の単一地点で警告を
    /// emit する。 0.8.15 の persist warning と同じく **1 秒 1 行**に
    /// rate-limit してターミナルを潰さない。 完全な伝播 / 修復経路は別 issue。
    fn wal_full_err(&self) -> io::Error {
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
                        self.capacity
                    );
                }
            }
        }
        io::Error::new(io::ErrorKind::OutOfMemory, "WAL full — consumer reset behind")
    }

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
        mmap[4..8].copy_from_slice(&OPLOG_FILE_VERSION.to_le_bytes());
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
            keypair: std::sync::RwLock::new(None),
            append_lock: std::sync::Mutex::new(()),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
            auto_reset: std::sync::atomic::AtomicBool::new(false),
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
        if version != OPLOG_FILE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL version {} unsupported (expected {})", version, OPLOG_FILE_VERSION),
            ));
        }
        let head = u64::from_le_bytes(mmap[8..16].try_into().unwrap());
        let checkpoint = u64::from_le_bytes(mmap[16..24].try_into().unwrap());
        let capacity = u64::from_le_bytes(mmap[24..32].try_into().unwrap());

        // 0.9.0 (L1): header の capacity / head / checkpoint を実 file 長と突き合わせる。
        // 旧実装は無検証だったため、 truncated .oplog (crash / 不完全 copy) が
        // 正常に open された後、 append の mmap[offset..] で OOB panic した。
        let file_len = mmap.len() as u64;
        if capacity > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "WAL truncated: header capacity {} exceeds file size {} — \
                     file was cut short (crash / partial copy?)",
                    capacity, file_len,
                ),
            ));
        }
        for (name, v) in [("head", head), ("checkpoint", checkpoint)] {
            if v < HEADER_SIZE as u64 || v > capacity {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL header corrupt: {} = {} out of range [{}, capacity {}]",
                        name, v, HEADER_SIZE, capacity,
                    ),
                ));
            }
        }

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
            keypair: std::sync::RwLock::new(None),
            append_lock: std::sync::Mutex::new(()),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
            auto_reset: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// この WAL を所有する peer の id を設定(Engine 初期化時に 1 回)。
    pub fn set_peer_id(&self, peer: PeerId) {
        self.peer_id.store(peer, Ordering::Release);
    }

    /// Phase C: 署名鍵を設定。None で署名 off(slot は zeros で埋める)。
    pub fn set_keypair(&self, kp: Option<std::sync::Arc<crate::keys::Keypair>>) {
        *self.keypair.write().unwrap() = kp;
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
    /// pending_writes カウンタで try_reset との race を防ぐ。
    /// writer は append 中 +1、完了時 -1。consumer reset は pending==0 時のみ。
    pub fn append(&self, op: Op<'_>) -> io::Result<u64> {
        self.append_with_hlc(op).map(|(lsn, _)| lsn)
    }

    /// request17-A3: `append` + **払い出した HLC も返す**版。 同期 write 経路が
    /// 「WAL に載せた HLC」をそのまま cell の version column に書けるようにする。
    pub fn append_with_hlc(&self, op: Op<'_>) -> io::Result<(u64, Hlc)> {
        let payload_size = op.payload_size();
        let record_size = REC_HEADER_SIZE + payload_size;

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
        let record_size = REC_HEADER_SIZE + payload_size;

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
        let sizes: Vec<usize> = records.iter()
            .map(|r| REC_HEADER_SIZE + r.as_op().payload_size())
            .collect();
        let total: usize = sizes.iter().sum();

        let _guard = self.pending_guard(records.len() as u32);
        self.append_many_inner(records, &sizes, total, hlcs)
    }

    fn append_many_inner(
        &self,
        records: &[OwnedOp],
        sizes: &[usize],
        total: usize,
        // request17-A3: Some なら採番せず与えられた HLC を載せる (index 対応)。
        hlcs: Option<&[Hlc]>,
    ) -> io::Result<Vec<u64>> {
        // #75: 同一プロセス内の直列化 (flock は同一 fd 共有スレッド間で no-op)
        let _in_proc = self.append_lock.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(not(target_arch = "wasm32"))]
        let _lock = self.flock_exclusive()?;

        // 一括 allocate
        let start_offset = {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let mm = self.mmap_slice();
                let on_disk = u64::from_le_bytes(mm[8..16].try_into().unwrap());
                let cur = on_disk.max(self.head.load(Ordering::Acquire));
                let new = cur + total as u64;
                if new > self.capacity {
                    return Err(self.wal_full_err());
                }
                self.head.store(new, Ordering::Release);
                cur
            }
            #[cfg(target_arch = "wasm32")]
            {
                let cur = self.head.load(Ordering::Acquire);
                let new = cur + total as u64;
                if new > self.capacity {
                    return Err(self.wal_full_err());
                }
                self.head.store(new, Ordering::Release);
                cur
            }
        };

        let mut lsns = Vec::with_capacity(records.len());
        let keypair = self.keypair.read().unwrap().clone();
        let mut offset = start_offset;
        let mmap = self.mmap_mut_slice();

        for (i, (rec, &record_size)) in records.iter().zip(sizes.iter()).enumerate() {
            let op = rec.as_op();
            let payload_size = record_size - REC_HEADER_SIZE;
            let lsn = self.next_lsn.fetch_add(1, Ordering::AcqRel);
            // request17-A3: 事前採番された HLC があればそれを使う (採番し直すと
            // cell の version column と record の HLC がずれる)。
            let hlc = match hlcs {
                Some(h) => h[i],
                None => self.next_hlc(),
            };
            let author_peer = hlc.peer;

            let op_byte = op.op_byte();
            let payload_offset = offset as usize + REC_HEADER_SIZE;
            op.write_payload(&mut mmap[payload_offset..payload_offset + payload_size]);
            let crc = fnv1a(&mmap[payload_offset..payload_offset + payload_size]);

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

            lsns.push(lsn);
            offset += record_size as u64;
        }

        // ファイルヘッダの head を一括更新 (batch 全体が書き終わったあと)
        mmap[8..16].copy_from_slice(&(start_offset + total as u64).to_le_bytes());

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
        let record_size = REC_HEADER_SIZE + payload_size;
        let _guard = self.pending_guard(1);
        let result = self.append_inner(op, payload_size, record_size, Some(header), None);
        // ローカル HLC clock を受信 HLC で merge (後退防止)
        self.merge_external_hlc(header.hlc);
        result.map(|(lsn, _)| lsn)
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
        loop {
            let last_wall = self.hlc_last_wall.load(Ordering::Acquire);
            let last_logical = self.hlc_logical.load(Ordering::Acquire);
            if recv.wall < last_wall || (recv.wall == last_wall && recv.logical <= last_logical) {
                return;
            }
            if self
                .hlc_last_wall
                .compare_exchange(last_wall, recv.wall, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.hlc_logical.store(recv.logical, Ordering::Release);
                return;
            }
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

        // #75: 同一プロセス内は append_lock、 プロセス間は flock で直列化。
        // flock は open file description 単位なので、 同じ File を共有する
        // スレッド間では排他にならない — append_lock が必須。
        let _in_proc = self.append_lock.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(not(target_arch = "wasm32"))]
        let _lock = self.flock_exclusive()?;

        // ── head を mmap 上の値から読み直して採番 ──
        // 別 process が直前に append した場合、 self.head (process-local atomic) は
        // 古い値を持ってるので、 lock 中に mmap 上の永続値を真実とする。
        let offset = {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let mm = self.mmap_slice();
                let on_disk = u64::from_le_bytes(mm[8..16].try_into().unwrap());
                let cur = on_disk.max(self.head.load(Ordering::Acquire));
                let new = cur + record_size as u64;
                if new > self.capacity {
                    return Err(self.wal_full_err());
                }
                // append_lock + flock の両方を保持しているので単純 store で OK
                self.head.store(new, Ordering::Release);
                cur
            }
            #[cfg(target_arch = "wasm32")]
            {
                let cur = self.head.load(Ordering::Acquire);
                let new = cur + record_size as u64;
                if new > self.capacity {
                    return Err(self.wal_full_err());
                }
                self.head.store(new, Ordering::Release);
                cur
            }
        };

        let lsn = self.next_lsn.fetch_add(1, Ordering::AcqRel);
        // relay 経路では受信 HLC/author/署名をそのまま乗せる (gossip 用)。
        // 通常経路では next_hlc + 自鍵で署名。
        let (hlc, author_peer) = match &relay {
            Some(o) => (o.hlc, o.author),
            None => {
                let h = hlc_override.unwrap_or_else(|| self.next_hlc());
                let a = h.peer;
                (h, a)
            }
        };

        let op_byte = op.op_byte();
        let payload_offset = offset as usize + REC_HEADER_SIZE;
        let mmap = self.mmap_mut_slice();
        op.write_payload(&mut mmap[payload_offset..payload_offset + payload_size]);
        let crc = fnv1a(&mmap[payload_offset..payload_offset + payload_size]);

        // Phase C: 鍵があれば署名。無ければ zeros。 relay 経路では元の署名を保持。
        let (signature, pubkey_fp) = match &relay {
            Some(o) => (o.signature, o.pubkey_fp),
            None => {
                let keypair = self.keypair.read().unwrap().clone();
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

        // ファイルヘッダの head も更新
        mmap[8..16].copy_from_slice(&(offset + record_size as u64).to_le_bytes());

        Ok((lsn, hlc))
    }

    /// ring buffer reset。
    /// head == checkpoint && pending_writes == 0 のときのみ実行可能。
    ///
    /// fix: auto_reset ゲートを撤去。 元は「Syncer attached の engine は ring を
    /// reset させない」ために入れた gate だが、 0.8.0 で sync publish path が `_sync_ops`
    /// 経由になり oplog ring を直接読まなくなった (sync.rs 参照) ので gate の存在理由は
    /// 消えていた。 にもかかわらず default が false のままだったため production では
    /// try_reset が常時 no-op になり、 ring が一度も畳まれず 16MB を使い切ると WAL full
    /// で append が全 drop される状態に陥っていた (#57 でようやく可視化)。 reset 条件
    /// (head==checkpoint ⇒ body へ msync 済 && pending==0) を満たす領域は crash recovery
    /// にも sync にも不要なので、 無条件に畳んでよい。 `auto_reset` フラグは vestigial。
    pub fn try_reset(&self) -> bool {
        self.try_reset_if(|| true)
    }

    /// `try_reset` の述語つき版。 `fold_safe` は **`append_lock` 保持中に、 fold の
    /// 直前に**評価される。
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
    pub fn try_reset_if(&self, fold_safe: impl FnOnce() -> bool) -> bool {
        // #77-M8: append_lock で append と直列化。 旧実装は pending_writes
        // チェックと head CAS の間に窓があり、 その間に進入した append の
        // record が reset で head 外に落ちて喪失 + stale record 復活が起きた。
        // append_lock 保持中は append_inner が進入できないため窓が閉じる。
        let _in_proc = self.append_lock.lock().unwrap_or_else(|p| p.into_inner());
        let head = self.head.load(Ordering::Acquire);
        let cp = self.checkpoint.load(Ordering::Acquire);
        if head != cp || head <= HEADER_SIZE as u64 { return false; }
        if self.pending_writes.load(Ordering::Acquire) > 0 { return false; }
        // lock 下での再評価。 ここで false なら畳まない (= 未 bridge record を守る)。
        if !fold_safe() { return false; }
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

    /// これ以上**いかなる record も** append できない満杯か。
    ///
    /// 最小の record は payload 0 の Commit（= REC_HEADER_SIZE bytes）なので、
    /// それすら入らない残量なら、 この WAL は畳まれるまで一切前進できない。
    /// commit group の途中でこの状態に達すると、 閉じの Commit も書けず tail は
    /// **永久に未 commit** のまま残る（recovery からも sync bridge からも不可視）。
    /// その tail を「畳んでよい死区間」と判定するために使う
    /// （engine の `wal_fold_safe` 参照）。
    pub fn append_dead(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        head + REC_HEADER_SIZE as u64 > self.capacity
    }

    /// 残り append 可能バイト数（観測用）。 `append_dead` と対で、 呼び出し側が
    /// 「WAL がどれだけ逼迫しているか」を可視化するのに使う。
    pub fn free_bytes(&self) -> u64 {
        self.capacity
            .saturating_sub(self.head.load(Ordering::Acquire))
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

    /// fsync(WAL 本体のみ)。consumer スレッドが定期実行。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn fsync(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    #[cfg(target_arch = "wasm32")]
    pub fn fsync(&self) -> io::Result<()> { Ok(()) }

    /// checkpoint を前進。本体 mmap に反映済み LSN の位置まで進める。
    ///
    /// #77-M8: checkpoint は head を越えられない (clamp)。 caller が reset 前に
    /// 読んだ stale head を渡すと checkpoint > head の壊れ状態になり、 head が
    /// その値を再突破するまで背景 fsync / sync 転送が止まっていた。
    pub fn advance_checkpoint(&self, new_checkpoint: u64) {
        let head = self.head.load(Ordering::Acquire);
        let target = new_checkpoint.min(head);
        let cur = self.checkpoint.load(Ordering::Acquire);
        if target <= cur { return; }
        self.checkpoint.store(target, Ordering::Release);
        self.mmap_mut_slice()[16..24].copy_from_slice(&target.to_le_bytes());
    }

    /// HEADER_SIZE から head までを読んで、Commit で挟まれた全レコードを返す。
    /// checkpoint 位置は無視(既に apply 済みの記録もまだ WAL file 上にあれば拾う)。
    /// Syncer.publish_since で使う。ring buffer reset 済みの記録は取れない。
    pub fn iter_committed(&self) -> Vec<Record> {
        strip_offsets(self.scan_from_offset(HEADER_SIZE as u64).0)
    }

    /// 指定 offset 以降の commit 済みレコードを返す。changefeed の差分発火用。
    /// `start_offset` は前回 emit 時の `wal.checkpoint()` を渡す想定。
    pub fn iter_committed_from(&self, start_offset: u64) -> Vec<Record> {
        strip_offsets(self.scan_from_offset(start_offset).0)
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
        let (out, _, _, committed_end, _) = self.scan_from_offset(start_offset);
        (out, committed_end)
    }

    /// #77-H4: `iter_committed_from` + 「読み切った commit 済み group の終端
    /// offset」を返す版。 cursor (changefeed emit_offset / _sync_ops bridge) は
    /// **この終端までしか進めてはいけない** — head は record 本体の書き込み前に
    /// bump されるため、 scan 後に `head()` を再読して cursor にすると、 scan が
    /// 書き込み途中 record で break した位置〜head 間の record を恒久 skip する。
    pub fn iter_committed_from_with_end(&self, start_offset: u64) -> (Vec<Record>, u64) {
        let (out, _, _, committed_end, _) = self.scan_from_offset(start_offset);
        (strip_offsets(out), committed_end)
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
    /// 書きかけの tail が混ざることはない。
    pub fn recover_with_tail(&self) -> Vec<Record> {
        let (mut out, tail) = self.recover_inner();
        out.extend(tail);
        out
    }

    /// `recover` / `recover_with_tail` の共通部。 clock 復元 (#77-H5) はここだけで行う。
    fn recover_inner(&self) -> (Vec<Record>, Vec<Record>) {
        let start = self.checkpoint.load(Ordering::Acquire);
        let (out, max_lsn, max_hlc, _, tail) = self.scan_from_offset(start);
        if max_lsn > 0 {
            self.next_lsn.store(max_lsn + 1, Ordering::Release);
        }
        if max_hlc.wall > 0 {
            self.hlc_last_wall.store(max_hlc.wall, Ordering::Release);
            self.hlc_logical.store(max_hlc.logical, Ordering::Release);
        }
        (strip_offsets(out), strip_offsets(tail))
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
    ) -> (Vec<(Record, u64)>, u64, Hlc, u64, Vec<(Record, u64)>) {
        let mut out = Vec::new();
        let mut batch = Vec::new();
        let mut offset = start_offset;
        let mut committed_end = start_offset;
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
            let mut signature = [0u8; 64];
            signature.copy_from_slice(&header[OFF_SIGNATURE..OFF_SIGNATURE + 64]);
            let mut pubkey_fp = [0u8; 8];
            pubkey_fp.copy_from_slice(&header[OFF_PUBKEY_FP..OFF_PUBKEY_FP + 8]);
            let hlc = Hlc { wall: hlc_wall, logical: hlc_logical, peer: hlc_peer };

            let payload_off = rec_end;
            let payload_end = payload_off + payload_len;
            if payload_end > mmap.len() { break; }

            let computed_crc = fnv1a(&mmap[payload_off..payload_end]);
            if stored_crc != computed_crc { break; } // 破損 tail

            let payload_slice = &mmap[payload_off..payload_end];
            let op = decode_op(op_byte, payload_slice);
            if lsn > max_lsn { max_lsn = lsn; }
            if hlc > max_hlc { max_hlc = hlc; }

            let signed_bytes = signed_payload(
                op_byte, payload_len as u32, lsn, hlc,
                author_peer, stored_crc, payload_slice,
            );

            match op {
                Some(DecodedOp::Commit) => {
                    out.append(&mut batch);
                    committed_end = payload_end as u64;
                }
                Some(other) => {
                    batch.push((
                        Record {
                            lsn, hlc, author_peer, op: other,
                            signature, pubkey_fp, signed_bytes,
                        },
                        payload_end as u64,
                    ));
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
                    break;
                }
            }

            offset = (payload_end) as u64;
        }

        // 未 commit batch は `out` には混ぜず、 呼び手が選べるよう別枠で返す。
        (out, max_lsn, max_hlc, committed_end, batch)
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

    // ---- flock (multi-process write 排他) ----

    /// WAL ファイルに対する LOCK_EX を取る。 戻り値の guard が drop された時点で
    /// 解放される。 同じ .wal を別 process が同時に append しようとした場合、 lock
    /// 取得まで block する (典型的に数 µs〜ms)。 read 経路は lock 取らない。
    #[cfg(not(target_arch = "wasm32"))]
    fn flock_exclusive(&self) -> io::Result<OpLogLockGuard<'_>> {
        self._file.lock()?;
        Ok(OpLogLockGuard { file: &self._file })
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
        buf[4..8].copy_from_slice(&OPLOG_FILE_VERSION.to_le_bytes());
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
            keypair: std::sync::RwLock::new(None),
            append_lock: std::sync::Mutex::new(()),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
            auto_reset: std::sync::atomic::AtomicBool::new(false),
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
        op_type::TIE_NAMED if payload.len() >= 16 => {
            let eid = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let value = u32::from_le_bytes(payload[8..12].try_into().unwrap());
            let himo_kind = payload[12];
            let nlen = u16::from_le_bytes(payload[14..16].try_into().unwrap()) as usize;
            if payload.len() < 16 + nlen { return None; }
            let himo_name = String::from_utf8(payload[16..16 + nlen].to_vec()).ok()?;
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
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let lsn1 = wal.append(Op::Tie { eid: 1, himo_id: 0, value: 42 }).unwrap();
        let lsn2 = wal.append(Op::Untie { eid: 2, himo_id: 1 }).unwrap();
        let lsn3 = wal.append(Op::Commit).unwrap();
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);
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
            match wal.append(Op::Tie { eid: i, himo_id: 0, value: i as u32 }) {
                Ok(_) => {}
                Err(e) => {
                    assert_eq!(e.kind(), io::ErrorKind::OutOfMemory, "full は OutOfMemory で返る");
                    hit_full = true;
                    break;
                }
            }
        }
        assert!(hit_full, "極小 capacity なら必ず満杯に到達して Err を返すはず");
        let _ = std::fs::remove_file(&p);
    }

    /// issue #58②: `append_inner` が panic しても `pending_writes` が RAII ガードで
    /// 必ず減算され、 counter が +1 のまま leak しないこと。 leak すると
    /// `try_reset`(pending == 0 条件)が永久に発火しなくなる。 fault injection で
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
                        value: i as u32,
                    }).unwrap();
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(wal.next_lsn(), 4001);
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
                        value: i as u32,
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
                wal_a.append(Op::Tie { eid: i as u64, himo_id: 0, value: i }).unwrap();
            } else {
                wal_b.append(Op::Tie { eid: i as u64, himo_id: 1, value: i }).unwrap();
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
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn try_reset_recycles_wal_space() {
        let p = tmp("ring_reset");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        wal.set_auto_reset(true);
        for i in 0..100u32 {
            wal.append(Op::Tie { eid: i as u64, himo_id: 0, value: i }).unwrap();
        }
        wal.append(Op::Commit).unwrap();
        let head_before = wal.head();
        assert!(head_before > HEADER_SIZE as u64);

        assert!(!wal.try_reset(), "should not reset when checkpoint < head");

        wal.advance_checkpoint(head_before);
        assert!(wal.try_reset(), "should reset when head == checkpoint");
        assert_eq!(wal.head(), HEADER_SIZE as u64);
        assert_eq!(wal.checkpoint(), HEADER_SIZE as u64);

        let lsn = wal.append(Op::Tie { eid: 999, himo_id: 0, value: 99 }).unwrap();
        assert!(lsn > 100);

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
            wal.append(Op::Tie { eid: i as u64, himo_id: 0, value: i }).unwrap();
        }
        wal.append(Op::Commit).unwrap();
        let head_before = wal.head();
        assert!(head_before > HEADER_SIZE as u64);

        wal.advance_checkpoint(head_before);
        assert!(
            wal.try_reset(),
            "ring must reclaim even without set_auto_reset(true)"
        );
        assert_eq!(wal.head(), HEADER_SIZE as u64);
        assert_eq!(wal.checkpoint(), HEADER_SIZE as u64);

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
                wal.append(Op::Tie { eid: v as u64, himo_id: 0, value: v }).unwrap();
            }
            wal.append(Op::Commit).unwrap();
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
        let wal = OpLog::create(&p, HEADER_SIZE + 128 + 50).unwrap();
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        let r = wal.append(Op::Tie { eid: 2, himo_id: 0, value: 2 });
        assert!(r.is_err());
        let _ = std::fs::remove_file(&p);
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

    #[test]
    fn append_with_hlc_returns_exactly_what_the_record_carries() {
        let p = tmp("append_with_hlc");
        let wal = OpLog::create(&p, 1024 * 1024).unwrap();
        let (lsn1, h1) = wal.append_with_hlc(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        let (lsn2, h2) = wal.append_with_hlc(Op::Tie { eid: 2, himo_id: 0, value: 2 }).unwrap();
        assert!(h1 < h2, "連続 append で HLC が単調増加していない: {h1:?} → {h2:?}");

        let on_disk = tie_hlcs(&wal);
        assert_eq!(on_disk, vec![(lsn1, h1), (lsn2, h2)], "返した HLC が record と違う");
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
        let _ = std::fs::remove_file(&p);
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
}
