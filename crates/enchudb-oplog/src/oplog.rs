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

    // signed_bytes header parse
    if &signed_bytes[0..2] != REC_MAGIC { return None; }
    if !known_version(signed_bytes[OFF_VERSION]) { return None; }
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
    /// #388: WAL に載せた時の record の byte 数 (header 込み)。
    pub fn record_size(&self) -> usize {
        REC_HEADER_SIZE + self.as_op().payload_size()
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
        }
    }
}

/// デコード後の op(読み戻し用、所有型)。
#[derive(Debug, Clone)]
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
    file: &'a File,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for OpLogLockGuard<'_> {
    fn drop(&mut self) {
        let _ = crate::filelock::unlock(self.file);
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
    /// #268: **書き終えた** record の終端。 append は head を先に進めてから record を書くので、 head と
    /// この値の間は書いている最中 = 前の周 (fold 前) の record がまだ残っている。 scan はここまでしか読まない
    /// (読むと前の周の Commit で group を閉じたつもりになり、 cursor を record の境目でない位置へ進めて
    /// bridge が開き直すまで止まった)。 append は append_lock の下で書き終えてから Release で置く。
    written: AtomicU64,
    checkpoint: AtomicU64,
    next_lsn: AtomicU64,
    /// 最後に払い出した HLC の (wall(ms), logical)。 logical は wall が進まない時の tiebreaker。
    /// 2 つを 1 つの lock で読み書きする: 別々の atomic だと、 同じ ms の中では wall の CAS が
    /// 同じ値への CAS になって必ず成功し、 並行した採番が同じ logical を読んで同じ HLC を払い出す
    /// (同期経路の `append` と async 経路の `mint_hlc` は別の lock の中から呼ばれる)。
    hlc_state: std::sync::Mutex<(u64, u32)>,
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
    /// #268: テスト用 fault injection。 残り回数ぶん Commit の append を満杯として
    /// 失敗させる。 0 (既定) で何もしない。 `fail_next_commits` の doc を参照。
    fail_next_commits: std::sync::atomic::AtomicU32,
    /// #57: append に失敗して WAL に載らなかった record の (author, その author の落ちた HLC の max)
    /// (Commit は除く)。 engine が [`OpLog::take_dropped`] で取り出し、 その author の配布履歴に穴が
    /// あったことを puller に知らせる (history floor を上げて bootstrap に回す)。
    dropped: std::sync::Mutex<Vec<(PeerId, Hlc)>>,
    /// #388: append が満杯にぶつかった時に呼ぶ待ち手 (engine が consumer に畳ませて、 空くまで待つ)。 true を返したら
    /// 1 回だけ書き直す。 None なら今までどおりすぐ落とす。
    room_waiter: std::sync::RwLock<Option<RoomWaiter>>,
    /// #391: append が載った後に呼ぶ (engine が consumer を起こす — 書き出しの tick を回すため)。
    append_hook: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>>,
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
                        self.capacity
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

    /// #388: `size` byte の append が満杯で載らなかった後に呼ぶ。 待ち手が空けたら true (呼び手は 1 回書き直す)。
    /// 空かなければ警告を出して false。 満杯でない失敗 (fault injection など) は何もせず false。
    fn wait_for_room(&self, size: u64) -> bool {
        if self.free_bytes() >= size {
            return false;
        }
        let waiter = self.room_waiter.read().unwrap_or_else(|p| p.into_inner()).clone();
        if waiter.is_some_and(|w| w(size)) {
            return true;
        }
        self.warn_wal_full();
        false
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

        let wal = Self {
            _file: file,
            mmap,
            capacity: capacity as u64,
            head: AtomicU64::new(HEADER_SIZE as u64),
            written: AtomicU64::new(HEADER_SIZE as u64),
            checkpoint: AtomicU64::new(HEADER_SIZE as u64),
            next_lsn: AtomicU64::new(1),
            hlc_state: std::sync::Mutex::new((0, 0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            keypair: std::sync::RwLock::new(None),
            append_lock: std::sync::Mutex::new(()),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
            auto_reset: std::sync::atomic::AtomicBool::new(false),
            fail_next_commits: std::sync::atomic::AtomicU32::new(0),
            room_waiter: std::sync::RwLock::new(None),
            append_hook: std::sync::OnceLock::new(),
            dropped: std::sync::Mutex::new(Vec::new()),
        };
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
            written: AtomicU64::new(head),
            checkpoint: AtomicU64::new(checkpoint),
            next_lsn: AtomicU64::new(1),
            hlc_state: std::sync::Mutex::new((0, 0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            keypair: std::sync::RwLock::new(None),
            append_lock: std::sync::Mutex::new(()),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
            auto_reset: std::sync::atomic::AtomicBool::new(false),
            fail_next_commits: std::sync::atomic::AtomicU32::new(0),
            room_waiter: std::sync::RwLock::new(None),
            append_hook: std::sync::OnceLock::new(),
            dropped: std::sync::Mutex::new(Vec::new()),
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

    /// **採番に使う head**。 native では mmap 上の永続値を真実とし、 process-local
    /// atomic と突き合わせる — 別 process が直前に append していると `self.head` は
    /// 古いため。
    ///
    /// #268: `append_dead` / `free_bytes` (= 「もう書けないか」 の観測) もこの値で
    /// 判定する。 in-memory head だけを見ていた旧実装は、 **実際の append は満杯で
    /// 失敗し続けているのに 「まだ余裕がある」 と答える**ことがあり、 その乖離は
    /// `wal_fold_safe` の死区間例外 (= brick の唯一の出口) まで塞いでいた。
    #[inline]
    fn alloc_head(&self) -> u64 {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mm = self.mmap_slice();
            let on_disk = u64::from_le_bytes(mm[8..16].try_into().unwrap());
            on_disk.max(self.head.load(Ordering::Acquire))
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.head.load(Ordering::Acquire)
        }
    }

    /// `size` bytes を予約して開始 offset を返す。 **`append_lock` 保持下で呼ぶこと**。
    ///
    /// head は record 本体を書く **前** に bump される (scan は Commit で閉じた
    /// group しか `out` に載せないので、 書き込み途中の領域が読まれることはない)。
    #[inline]
    fn alloc(&self, size: u64) -> io::Result<u64> {
        let cur = self.alloc_head();
        let new = cur + size;
        if new > self.capacity {
            return Err(self.wal_full_err());
        }
        self.head.store(new, Ordering::Release);
        Ok(cur)
    }

    /// 次に発行する LSN。
    #[inline]
    pub fn next_lsn(&self) -> u64 { self.next_lsn.load(Ordering::Acquire) }

    /// 次の HLC を払い出す。
    /// wall が前回より進んでいれば logical=0 にリセット、同じ/戻っていれば logical+1。
    fn next_hlc(&self) -> Hlc {
        let peer = self.peer_id.load(Ordering::Acquire);
        let now = current_wall_ms();
        let mut st = self.hlc_state.lock().unwrap_or_else(|p| p.into_inner());
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
        let mut r = self.append_many_inner(records, &sizes, total, hlcs);
        if r.is_err() && self.wait_for_room(total as u64) {
            r = self.append_many_inner(records, &sizes, total, hlcs);
        }
        if r.is_ok() {
            self.appended();
        }
        // #57: 自分の write の束 (Commit しか無い束は record を運ばない)。 HLC は事前採番の max
        if r.is_err() {
            let carried = records.iter().enumerate().filter(|(_, x)| !matches!(x.as_op(), Op::Commit));
            let max = match hlcs {
                Some(h) => carried.map(|(i, _)| h[i]).max(),
                None => carried.map(|_| ()).next().map(|_| self.next_hlc()),
            };
            if let Some(h) = max {
                self.note_dropped(self.peer_id(), h);
            }
        }
        r
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
        let start_offset = self.alloc(total as u64)?;

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

            lsns.push(lsn);
            offset += record_size as u64;
        }

        // ファイルヘッダの head を一括更新 (batch 全体が書き終わったあと)
        mmap[8..16].copy_from_slice(&(start_offset + total as u64).to_le_bytes());
        self.written.store(start_offset + total as u64, Ordering::Release);

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
        let record_size = REC_HEADER_SIZE + payload_len;
        let mut r = self.append_verbatim_checked(sb, signature, pubkey_fp, record_size);
        if r.is_err() && self.wait_for_room(record_size as u64) {
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
        record_size: usize,
    ) -> io::Result<u64> {
        let _guard = self.pending_guard(1);
        let _in_proc = self.append_lock.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(not(target_arch = "wasm32"))]
        let _lock = self.flock_exclusive()?;

        // head 採番は append_inner と同じ規則 (mmap 上の永続値を真実とする)。
        let offset = self.alloc(record_size as u64)?;

        let mmap = self.mmap_mut_slice();
        let off = offset as usize;
        mmap[off..off + SIGNED_PAYLOAD_HEADER_SIZE]
            .copy_from_slice(&sb[0..SIGNED_PAYLOAD_HEADER_SIZE]);
        mmap[off + OFF_SIGNATURE..off + OFF_SIGNATURE + 64].copy_from_slice(signature);
        mmap[off + OFF_PUBKEY_FP..off + OFF_PUBKEY_FP + 8].copy_from_slice(pubkey_fp);
        mmap[off + REC_HEADER_SIZE..off + record_size]
            .copy_from_slice(&sb[SIGNED_PAYLOAD_HEADER_SIZE..]);
        // ファイルヘッダの head も更新
        mmap[8..16].copy_from_slice(&(offset + record_size as u64).to_le_bytes());
        self.written.store(offset + record_size as u64, Ordering::Release);

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
        let mut st = self.hlc_state.lock().unwrap_or_else(|p| p.into_inner());
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
        let mut r = self.append_inner_raw(op.clone(), payload_size, record_size, relay, hlc_override);
        if r.is_err() && self.wait_for_room(record_size as u64) {
            r = self.append_inner_raw(op, payload_size, record_size, relay, hlc_override);
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
            let hlc = relay.map(|h| h.hlc).or(hlc_override).unwrap_or_else(|| self.next_hlc());
            self.note_dropped(relay.map(|h| h.author).unwrap_or_else(|| self.peer_id()), hlc);
            (e, hlc)
        })
    }

    /// #57: `append_with_hlc` と同じ。 載らなかった時は Err に **落ちた record の HLC** を添える
    /// (採番して [`OpLog::take_dropped`] に覚えた値)。 同期 write はそれを cell の版数に使う —
    /// 覚えるより後に採番し直すと、 その間に engine の bridge が floor を決めて、 落ちた write の
    /// 版数が floor を越える。
    pub fn append_or_dropped(&self, op: Op<'_>) -> Result<(u64, Hlc), (io::Error, Hlc)> {
        let payload_size = op.payload_size();
        let record_size = REC_HEADER_SIZE + payload_size;
        let _guard = self.pending_guard(1);
        self.append_inner_noting(op, payload_size, record_size, None, None)
    }

    fn append_inner_raw(
        &self,
        op: Op<'_>,
        payload_size: usize,
        record_size: usize,
        relay: Option<RelayedHeader>,
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

        // #268: テスト用 fault injection (既定では atomic load すら踏まない)。
        if matches!(op, Op::Commit) && self.take_commit_fault() {
            // 容量の失敗 (`OutOfMemory`) とは種類を分ける: 呼び出し側は `OutOfMemory` を 「満杯 = 畳めば回復」
            // と読む (#407)。 これは満杯でない WAL で Commit が打てない場合を作るためのもの
            return Err(io::Error::other("injected commit failure (test)"));
        }

        // #75: 同一プロセス内は append_lock、 プロセス間は flock で直列化。
        // flock は open file description 単位なので、 同じ File を共有する
        // スレッド間では排他にならない — append_lock が必須。
        let _in_proc = self.append_lock.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(not(target_arch = "wasm32"))]
        let _lock = self.flock_exclusive()?;

        // ── head を mmap 上の値から読み直して採番 ──
        // 別 process が直前に append した場合、 self.head (process-local atomic) は
        // 古い値を持ってるので、 lock 中に mmap 上の永続値を真実とする。
        // append_lock + flock の両方を保持しているので単純 store で OK。
        let offset = self.alloc(record_size as u64)?;

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
        let crc = new_record_crc(
            op_byte, payload_size as u32, lsn, hlc, author_peer,
            &mmap[payload_offset..payload_offset + payload_size],
        );

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
        self.written.store(offset + record_size as u64, Ordering::Release);

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
        self.reset_ring_locked(head)
    }

    /// ring を先頭に戻す (`append_lock` を持った呼び手から、 head == checkpoint を確かめた後)。
    fn reset_ring_locked(&self, head: u64) -> bool {
        if self.head.compare_exchange(head, HEADER_SIZE as u64, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return false;
        }
        self.written.store(HEADER_SIZE as u64, Ordering::Release);
        self.checkpoint.store(HEADER_SIZE as u64, Ordering::Release);
        let mmap = self.mmap_mut_slice();
        mmap[8..16].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        mmap[16..24].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
        true
    }

    /// #388: append を止めたまま (`append_lock` を持ったまま) checkpoint を head まで進め、 そのまま畳めれば畳む。
    ///
    /// `sync(head)` は head までの record の効果を本体に書き出す (oplog の fsync + 本体の msync) 役で、 false なら何も
    /// 進めない。 その間 append は待たされる (= 書き手への back-pressure)。 `try_reset` は書き出してから畳むまでの間に
    /// 次の record が入ると畳めず、 書き手が休まず書く間は ring が満杯まで埋まっていた。
    ///
    /// 戻り値は (checkpoint を進めたか、 畳んだか)。 `fold_safe` は lock の下で、 checkpoint を進めた後に呼ぶ
    /// (`try_reset_if` と同じ)。 `pending_writes` は見ない — 全 append は `append_lock` の下で場所を取るので、 ここで
    /// 数に入っているのは lock を待っている (まだ場所を取っていない) append だけ。 `sync` / `fold_safe` から
    /// この OpLog の append 系を呼んではならない (自分の lock で止まる)。
    pub fn checkpoint_and_reset(&self, sync: impl FnOnce(u64) -> bool, fold_safe: impl FnOnce() -> bool) -> (bool, bool) {
        let _in_proc = self.append_lock.lock().unwrap_or_else(|p| p.into_inner());
        let head = self.head.load(Ordering::Acquire);
        if head <= HEADER_SIZE as u64 || !sync(head) {
            return (false, false);
        }
        self.advance_checkpoint(head);
        if !fold_safe() {
            return (true, false);
        }
        (true, self.reset_ring_locked(head))
    }

    /// WAL の容量 (byte)。
    pub fn capacity(&self) -> u64 {
        self.capacity
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
    ///
    /// #268: 判定は `alloc` と同じ `alloc_head()` で行う。 in-memory head だけを
    /// 見ていると、 別 fd の append で on-disk head だけが進んだ DB で
    /// 「append は全滅しているのに append_dead() は false」 になり、
    /// 出口 (fold の死区間例外) が閉じたまま無音で止まる。
    pub fn append_dead(&self) -> bool {
        self.alloc_head() + REC_HEADER_SIZE as u64 > self.capacity
    }

    /// 残り append 可能バイト数（観測用）。 `append_dead` と対で、 呼び出し側が
    /// 「WAL がどれだけ逼迫しているか」を可視化するのに使う。
    pub fn free_bytes(&self) -> u64 {
        self.capacity.saturating_sub(self.alloc_head())
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

    fn note_dropped(&self, author: PeerId, hlc: Hlc) {
        let mut g = self.dropped.lock().unwrap_or_else(|p| p.into_inner());
        match g.iter_mut().find(|(a, _)| *a == author) {
            Some((_, h)) => *h = (*h).max(hlc),
            None => g.push((author, hlc)),
        }
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
        let s = self.scan_from_offset(start);
        if s.max_lsn > 0 {
            self.next_lsn.store(s.max_lsn + 1, Ordering::Release);
        }
        if s.max_hlc.wall > 0 {
            *self.hlc_state.lock().unwrap_or_else(|p| p.into_inner()) = (s.max_hlc.wall, s.max_hlc.logical);
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
        let mut out = Vec::new();
        let mut batch = Vec::new();
        let mut offset = start_offset;
        let mut committed_end = start_offset;
        // #268: head ではなく書き終えた終端まで (head との間は書いている最中 = 前の周の record が見える)
        let head = self.written.load(Ordering::Acquire);
        let mut max_lsn = 0;
        let mut max_hlc = Hlc::ZERO;
        // #268: 打ち切り理由。 while を抜け切れば「head まで読んだ」。
        let mut stop = ScanStop::Head;

        let mmap = self.mmap_slice();

        while offset < head {
            let rec_end = (offset as usize) + REC_HEADER_SIZE;
            if rec_end > mmap.len() { stop = ScanStop::OutOfBounds; break; }
            let header = &mmap[offset as usize..rec_end];
            if &header[OFF_MAGIC..OFF_MAGIC + 2] != REC_MAGIC {
                stop = ScanStop::BadMagic;
                break;
            }
            let version = header[OFF_VERSION];
            if !known_version(version) {
                stop = ScanStop::BadVersion;
                break;
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
            if payload_end > mmap.len() { stop = ScanStop::OutOfBounds; break; }

            // v3 は header の値も覆う (壊れた header のゴミの HLC / author を通さない、 #58)
            let computed_crc = record_crc(version, &header[CRC_FIELDS], &mmap[payload_off..payload_end]);
            if stored_crc != computed_crc {
                stop = ScanStop::BadCrc; // 破損 tail
                break;
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
                    stop = ScanStop::UndecodableOp;
                    break;
                }
            }

            offset = (payload_end) as u64;
        }

        // 未 commit batch は `out` には混ぜず、 呼び手が選べるよう別枠で返す。
        ScanOut { out, max_lsn, max_hlc, committed_end, tail: batch, stop }
    }

    /// head を checkpoint に戻す(WAL truncate 相当、uncommitted も全捨て)。
    pub fn reset_to_checkpoint(&self) {
        let cp = self.checkpoint.load(Ordering::Acquire);
        self.head.store(cp, Ordering::Release);
        self.written.store(cp, Ordering::Release);
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
        // #280: advisory lock を持たない FS では **排他なしで続行**する
        // (エラーにすると platform / FS ごと書き込み不能になる)。 同一プロセス内の
        // 直列化は呼び出し側の `append_lock` が担うので、 単一プロセス構成
        // (モバイル) は安全側に倒れる。 guard の unlock は no-op になる。
        let _ = crate::filelock::lock_exclusive(&self._file)?;
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
            written: AtomicU64::new(HEADER_SIZE as u64),
            checkpoint: AtomicU64::new(HEADER_SIZE as u64),
            next_lsn: AtomicU64::new(1),
            hlc_state: std::sync::Mutex::new((0, 0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            keypair: std::sync::RwLock::new(None),
            append_lock: std::sync::Mutex::new(()),
            pending_writes: std::sync::atomic::AtomicU32::new(0),
            auto_reset: std::sync::atomic::AtomicBool::new(false),
            fail_next_commits: std::sync::atomic::AtomicU32::new(0),
            room_waiter: std::sync::RwLock::new(None),
            append_hook: std::sync::OnceLock::new(),
            dropped: std::sync::Mutex::new(Vec::new()),
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
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
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
        assert_eq!(wal.head(), HEADER_SIZE as u64);
        assert_eq!(wal.checkpoint(), HEADER_SIZE as u64);

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
        assert_eq!(wal.head(), HEADER_SIZE as u64);
        assert_eq!(wal.checkpoint(), HEADER_SIZE as u64);

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
        // File header 32 + 128 + 余白だけ = 200 確保
        let wal = OpLog::create(&p, HEADER_SIZE + 128 + 50).unwrap();
        wal.append(Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
        let r = wal.append(Op::Tie { eid: 2, himo_id: 0, value: 2 });
        assert!(r.is_err());
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
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
        assert_eq!(stale.head(), HEADER_SIZE as u64);

        while writer.append(Op::Commit).is_ok() {}
        assert!(writer.append_dead(), "満杯にした本人が dead と言っていない");

        assert_eq!(
            stale.head(),
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
}
