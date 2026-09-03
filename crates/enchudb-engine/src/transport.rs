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

use enchudb_oplog::oplog::{DecodedOp, Record};
use enchudb_oplog::{Hlc, PeerId};

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

impl From<Record> for WireRecord {
    fn from(r: Record) -> Self {
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
    /// 本番経路では OpLog::iter_committed() 経由で signed な record が来るべき。
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
            DecodedOp::TieNamed { eid, himo_name, himo_kind, value } => {
                out.push(6);
                out.extend_from_slice(&eid.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
                out.push(*himo_kind);
                out.extend_from_slice(&(himo_name.len() as u16).to_le_bytes());
                out.extend_from_slice(himo_name.as_bytes());
            }
            DecodedOp::TieLeaf { eid, himo_name, himo_kind, bytes } => {
                out.push(7);
                out.extend_from_slice(&eid.to_le_bytes());
                out.push(*himo_kind);
                out.extend_from_slice(&(himo_name.len() as u16).to_le_bytes());
                out.extend_from_slice(himo_name.as_bytes());
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
            DecodedOp::TieRef { eid, himo_id, target } => {
                // #183: Ref target の世界番号 (u64) 同乗版 Tie
                out.push(8);
                out.extend_from_slice(&eid.to_le_bytes());
                out.extend_from_slice(&himo_id.to_le_bytes());
                out.extend_from_slice(&target.to_le_bytes());
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
            6 => {
                need(p, 15, buf)?;
                let eid = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
                let value = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()); p += 4;
                let himo_kind = buf[p]; p += 1;
                let nlen = u16::from_le_bytes(buf[p..p+2].try_into().unwrap()) as usize; p += 2;
                need(p, nlen, buf)?;
                let himo_name = String::from_utf8(buf[p..p+nlen].to_vec())
                    .map_err(|_| WireDecodeError::UnknownOpTag(6))?;
                p += nlen;
                DecodedOp::TieNamed { eid, himo_name, himo_kind, value }
            }
            7 => {
                need(p, 8, buf)?;
                let eid = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
                need(p, 3, buf)?;
                let himo_kind = buf[p]; p += 1;
                let nlen = u16::from_le_bytes(buf[p..p+2].try_into().unwrap()) as usize; p += 2;
                need(p, nlen, buf)?;
                let himo_name = String::from_utf8(buf[p..p+nlen].to_vec())
                    .map_err(|_| WireDecodeError::InvalidUtf8)?;
                p += nlen;
                need(p, 4, buf)?;
                let blen = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()) as usize; p += 4;
                need(p, blen, buf)?;
                let bytes = buf[p..p+blen].to_vec(); p += blen;
                DecodedOp::TieLeaf { eid, himo_name, himo_kind, bytes }
            }
            8 => {
                need(p, 18, buf)?;
                let eid = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
                let himo_id = u16::from_le_bytes(buf[p..p+2].try_into().unwrap()); p += 2;
                let target = u64::from_le_bytes(buf[p..p+8].try_into().unwrap()); p += 8;
                DecodedOp::TieRef { eid, himo_id, target }
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

    /// `from` peer から **指定 `to` peer のみ** に publish (request4 partial sync 用)。
    /// `SubscriptionFilter` で peer 別に絞った record を届けるための single-target
    /// 経路。 default 実装は `publish` (broadcast) にフォールバック — 既存 transport
    /// は何もしなくても backward compatible に動くが、 partial sync を機能させたい
    /// transport (HTTP/WS push) は **必ず override** すること (broadcast fallback だと
    /// per-peer filter が無視される)。
    fn publish_to(&self, from: PeerId, _to: PeerId, records: Vec<WireRecord>) {
        self.publish(from, records);
    }

    /// `to` peer 視点で `from` peer から HLC `since` 以降の records を pull
    /// (request4 partial sync 用)。 broadcast log + (from, to) targeted log を
    /// merge して返す想定。 default は `pull(from, since)` フォールバック (=
    /// partial sync 非対応 transport では broadcast log のみ)。
    fn pull_as(&self, _to: PeerId, from: PeerId, since: Hlc) -> Vec<WireRecord> {
        self.pull(from, since)
    }

    /// 現在この transport が観測している peer 一覧 (request4 partial sync 用)。
    /// `Syncer::publish_since` から「全 peer に per-peer publish する」 ために
    /// 使う。 default 実装は空 — `Syncer::publish_since` 側は空が返ったら
    /// broadcast 経路 (`publish`) にフォールバックする (= 旧挙動 backward compat)。
    fn known_peers(&self) -> Vec<PeerId> {
        Vec::new()
    }

    /// #216: **author 別 cursor** での pull。 relay (gossip) の stream は
    /// 「自分の row (自 clock)」と「relay された row (原 author の HLC 素通し、
    /// #209)」の merge で **全体としては HLC 非単調** — scalar cursor の
    /// `hlc > since` filter は、 cursor が別 author の新しい row で先に進んだ後に
    /// relay された古い HLC の record を永久に落とす (silent data loss)。
    ///
    /// author ごとの substream は relay を何 hop 挟んでも HLC 単調なので、
    /// cursor を (author → Hlc) の vector にすれば健全になる。 `since` に無い
    /// author は **Hlc::ZERO 起点** (= 全量) — 新しく relay され始めた author の
    /// 古い record を落とさないための必須条件で、 「既知 author の min」への
    /// 短絡は同じ穴を一段下で再現する。
    ///
    /// default 実装は `pull_as(to, from, Hlc::ZERO)` の全量 fetch。 データの正しさは
    /// 保つ (既知分は受信側 `Syncer::pull_once` の author 別 filter が落とす) が、
    /// **cursor を transport に運ばない**。 `Syncer::pull_once` は 0.23.1 (#216) から
    /// **常にこの経路**を呼び、 scalar `pull_as` は Syncer からは呼ばれない。 そのため
    /// request の cursor を到達証明として ack に写す transport (HTTP gateway 等) では、
    /// **override しないと ack が前進せず `_sync_ops` の reclaim が止まり、 #149 の
    /// backpressure が再発する** (#242)。 「override すれば効率が上がる」 ではなく
    /// 「override しないと運用が壊れる」。 default 経路は初回に stderr へ 1 回だけ警告する。
    ///
    /// override の形は 2 つ:
    /// - relay (gossip) を運ぶ transport: author ごとに cursor を引く per-author filter
    ///   (`InMemoryTransport` / ws の実装)
    /// - relay を使わない直 pull 構成 (各 peer の `_sync_ops` に自分が author した record
    ///   しか無い): [`Transport::pull_as_multi_link_author`] を 1 行で呼ぶ
    fn pull_as_multi(
        &self,
        to: PeerId,
        from: PeerId,
        _since: &[(PeerId, Hlc)],
    ) -> Vec<WireRecord> {
        static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            eprintln!(
                "[enchudb] warning: Transport::pull_as_multi is not overridden — the pull cursor is \
                 not delivered to the transport (pull_as is called with Hlc::ZERO). if the server \
                 derives acks from the request cursor, _sync_ops will never be reclaimed (#242). \
                 override it with a per-author filter, or call pull_as_multi_link_author for a \
                 direct-pull (no relay) link"
            );
        }
        self.pull_as(to, from, Hlc::ZERO)
    }

    /// #242: **直 pull 構成 (relay / gossip なし) 向け**の `pull_as_multi` 実装。 `since` の
    /// vector を link author (`from`) の entry 1 本に畳んで scalar `pull_as` に渡す。
    /// `from` の entry が無ければ `Hlc::ZERO` (= 全量)。
    ///
    /// これが正しいのは、 **`from` の `_sync_ops` に `from` 自身が author した record しか
    /// 無い**とき (= `Engine::gossip_remote_apply()` が false、 3 constructor の既定)。 第三
    /// author の record が relay されて混ざる stream では、 他 author の cursor を捨てるので
    /// 使ってはいけない (per-author filter を実装すること)。
    ///
    /// ```ignore
    /// fn pull_as_multi(&self, to: PeerId, from: PeerId, since: &[(PeerId, Hlc)]) -> Vec<WireRecord> {
    ///     self.pull_as_multi_link_author(to, from, since)
    /// }
    /// ```
    fn pull_as_multi_link_author(
        &self,
        to: PeerId,
        from: PeerId,
        since: &[(PeerId, Hlc)],
    ) -> Vec<WireRecord> {
        let cursor = since
            .iter()
            .find(|(p, _)| *p == from)
            .map(|(_, h)| *h)
            .unwrap_or(Hlc::ZERO);
        self.pull_as(to, from, cursor)
    }

    /// #140: publisher が「自分の履歴は `floor` 以下が reclaim 済み」と広告する。
    ///
    /// `_sync_ops` は ring buffer なので、 publisher 側で reclaim が走ると **配れる履歴に
    /// 下限ができる**。 これを伝えないと、 cursor がその下限より古い puller は部分履歴を
    /// 「全履歴」として受け取って黙って不完全な store になる (#140)。
    ///
    /// default は no-op — 広告を運べない transport では `history_floor` が None を返し、
    /// puller 側の判定は従来どおり (= 検知なし) にフォールバックする。
    fn set_history_floor(&self, _peer: PeerId, _floor: Hlc) {}

    /// #140: `peer` が広告した履歴の下限。 `None` は「下限なし (= 全履歴あり)」または
    /// 「この transport は広告を運べない」。 puller は自分の cursor がこれより古ければ
    /// 差分では追いつけないと判断する。
    fn history_floor(&self, _peer: PeerId) -> Option<Hlc> {
        None
    }

    /// #216: author 別の history floor 広告。 relay 混在 ring では scalar floor が
    /// 「author a の cursor は新しいのに author b の reclaim で恒常 truncation」の
    /// false positive を作るため、 publisher は author 別に広告し、 puller は
    /// `cursor[a] < floor[a]` で判定する。
    ///
    /// default 実装は scalar `set_history_floor(peer, max)` への退化 (保守側 —
    /// 判定の粒度が落ちるだけで安全方向)。
    fn set_history_floor_multi(&self, peer: PeerId, floors: &[(PeerId, Hlc)]) {
        if let Some(max) = floors.iter().map(|(_, h)| *h).max() {
            self.set_history_floor(peer, max);
        }
    }

    /// #216: `peer` の author 別 history floor。 `None` = この transport は author 別
    /// 広告を運べない (または未広告) — puller は scalar `history_floor` の保守的
    /// 判定に fall back する。
    fn history_floor_multi(&self, _peer: PeerId) -> Option<Vec<(PeerId, Hlc)>> {
        None
    }

    /// #149: puller が「`author` の履歴を HLC `cursor` まで消化した」ことを記録する。
    ///
    /// pull の since cursor は durable barrier (`Syncer::pull_once` の
    /// persist_applied_state) を通過した後にしか前進しないので、 それ自体が
    /// **消化の到達証明**になっている。 author は `take_pull_acks` で回収して
    /// `Engine::ack_sync_up_to_hlc` に写し、 `_sync_ops` の reclaim を回す。
    ///
    /// default は no-op — ack を運べない transport では従来どおり (= reclaim は
    /// caller の明示 `ack_sync` 頼み、 ring はいずれ満杯で backpressure)。
    fn record_pull_ack(&self, _author: PeerId, _by: PeerId, _cursor: Hlc) {}

    /// #149: `author` 宛に溜まった pull ack を drain して返す。
    /// puller ごとに 1 エントリ (最大 cursor のみ保持)。 default は空。
    fn take_pull_acks(&self, _author: PeerId) -> Vec<(PeerId, Hlc)> {
        Vec::new()
    }

    /// #216: author 別 cursor (vector) での pull ack。 relay 混在 ring の reclaim を
    /// 健全に回す完全形 — publisher 側は [`Engine::ack_sync_up_to_cursors`] の
    /// per-row 述語 (`consumed(row) = row.hlc <= cursors[row.author]`、 未知 author
    /// は ZERO) に直結する。 scalar ack は relayed row を消化と証明できないため、
    /// relay 経路の reclaim は vector ack でしか前進しない。
    ///
    /// default 実装は退化形: `cursors` から **author = link (`author` 引数) の
    /// entry だけ**を scalar `record_pull_ack` に落とす。 「他 author は証明なし」の
    /// 保守側解釈で、 scalar 側の self-only 述語と意味論が揃う (min に潰すのは
    /// 未知 author の row を消化済みと誤判定する over-ack になるので不可)。
    fn record_pull_ack_multi(
        &self,
        author: PeerId,
        by: PeerId,
        cursors: &[(PeerId, Hlc)],
    ) {
        if let Some((_, h)) = cursors.iter().find(|(p, _)| *p == author) {
            self.record_pull_ack(author, by, *h);
        }
    }

    /// #216: `author` 宛に溜まった vector ack を drain して返す。
    /// puller ごとに 1 エントリ (author 別 max cursor)。 default は空 —
    /// 未対応 transport では `record_pull_ack_multi` の default が scalar 経路に
    /// 落としているので、 `take_pull_acks` 側で回収される。
    fn take_pull_acks_multi(&self, _author: PeerId) -> Vec<(PeerId, Vec<(PeerId, Hlc)>)> {
        Vec::new()
    }

    /// #140: `peer` の live-state 配布元を登録する。 truncated puller が
    /// `fetch_state` で author の現在状態を取得する経路 (`Syncer::serve_state`
    /// が author 側で呼ぶ)。 default は no-op — 運べない transport では
    /// `fetch_state` が None を返し、 bootstrap は成立しない (従来どおり)。
    fn register_state_provider(&self, _peer: PeerId, _provider: StateProvider) {}

    /// #226: `by` が `author` の live state を配れると名乗る (relay/replica 用)。
    ///
    /// `author == by` は #140 の author 直配布と同じ。 `author != by` は
    /// **replica 配布** — relay は author の行を translated local として保持して
    /// いるので、 `Engine::state_records_for(author)` で原型に戻して配れる。
    /// relay topology では author に直接届かない follower の唯一の回復経路。
    ///
    /// default は `author == by` の時だけ既存の scalar 登録に落とす
    /// (= replica 配布を運べない transport では従来どおり author 直のみ)。
    /// 同じ `(author, by)` の再登録は置換すること (`serve_state` は繰り返し
    /// 呼ばれる)。
    fn register_state_provider_for(&self, author: PeerId, by: PeerId, provider: StateProvider) {
        if author == by {
            self.register_state_provider(author, provider);
        }
    }

    /// #140: `author` の live state を取得する。 None = この transport は
    /// state 配布を運べない、 または author が provider 未登録。
    ///
    /// #226: replica が名乗っている場合、 実装は **author 本人の provider を
    /// 優先**すること (本人発だけが `complete: true` = ghost sweep 可)。
    fn fetch_state(&self, _author: PeerId) -> Option<StateBatch> {
        None
    }
}

/// #140: live-state 配布元。 呼ばれるたびに author の現在状態を合成して返す。
/// None = 配布元が既に閉じている (restart で engine が drop 済み等)。
/// **実装は engine を `Weak` で持つこと** — transport は peer より長生きするので、
/// 強参照だと drop 済みのはずの engine (consumer thread 込み) が生き続け、
/// 同一 DB file を再 open した新 engine と衝突する。
pub type StateProvider = Arc<dyn Fn() -> Option<StateBatch> + Send + Sync>;

/// #140: author の live state 一式 (`Engine::state_records` の出力)。
///
/// ring (`_sync_ops`) が「最近の差分」を担保するのに対し、 これは「現在状態の
/// 転写」。 truncated puller は records を通常の apply 経路 (LWW、 冪等) で
/// 適用し、 cursor を `as_of` に合わせて差分 pull に接続する。
#[derive(Clone)]
pub struct StateBatch {
    /// author の live cell を bridge と同語彙で合成した record 列。
    /// v1 制約: 署名なし (signature = zeros、 signed_bytes 空) — require_signature
    /// な受信側では reject される。 content blob は含まない。
    pub records: Vec<WireRecord>,
    /// 合成開始時点の HLC。 これ以降の op は ring に必ず居る (floor は必ず
    /// これより古い) ので、 適用後の pull cursor はここに設定できる。
    pub as_of: Hlc,
    /// false = 部分的な合成 (転送打ち切り等)。 受信側は ghost sweep を skip する。
    pub complete: bool,
}

/// テスト用: プロセス内で peer 間の WAL を共有する。
///
/// peer ごとに `(ordered log of WireRecord)` を持つ。HLC 昇順で入れる想定。
///
/// request4: partial sync 対応 — 「from peer → to peer」 で per-target log を
/// 持つ場合のために `targeted` field を別途持つ。 `publish_to(from, to, recs)`
/// は `targeted[(from, to)]` に追記 (broadcast の `inner[from]` とは独立)。
/// `pull(from, since)` は両方の log を merge して返す (= subscriber は broadcast
/// も targeted も両方受信できる)。
#[derive(Default, Clone)]
pub struct InMemoryTransport {
    inner: Arc<Mutex<HashMap<PeerId, Vec<WireRecord>>>>,
    /// (from, to) → records — partial sync 用 targeted log
    targeted: Arc<Mutex<HashMap<(PeerId, PeerId), Vec<WireRecord>>>>,
    /// #140: peer → 広告された履歴の下限 (これ以下は publisher 側で reclaim 済み)。
    floors: Arc<Mutex<HashMap<PeerId, Hlc>>>,
    /// #216: peer → (author → 履歴下限)。 author 別 floor 広告。
    floors_multi: Arc<Mutex<HashMap<PeerId, HashMap<PeerId, Hlc>>>>,
    /// #149: author → (puller → 消化済み max HLC)。 `take_pull_acks` で drain。
    pull_acks: Arc<Mutex<HashMap<PeerId, HashMap<PeerId, Hlc>>>>,
    /// #216: author → (puller → (author 別 max cursor))。 `take_pull_acks_multi` で
    /// drain。 relay 混在 ring の reclaim を回す vector ack 用。
    pull_acks_multi:
        Arc<Mutex<HashMap<PeerId, HashMap<PeerId, HashMap<PeerId, Hlc>>>>>,
    /// #140/#226: author → 配布元 `(by, provider)` の一覧。 `by == author` が
    /// 本人発 (complete)、 それ以外は replica 発 (partial)。 `fetch_state` は
    /// 本人発を優先する。
    state_providers: StateProviderRegistry,
}

/// #226: author → 配布元 `(by, provider)` の一覧。
type StateProviderRegistry = Arc<Mutex<HashMap<PeerId, Vec<(PeerId, StateProvider)>>>>;

impl InMemoryTransport {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            targeted: Arc::new(Mutex::new(HashMap::new())),
            floors: Arc::new(Mutex::new(HashMap::new())),
            floors_multi: Arc::new(Mutex::new(HashMap::new())),
            pull_acks: Arc::new(Mutex::new(HashMap::new())),
            pull_acks_multi: Arc::new(Mutex::new(HashMap::new())),
            state_providers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 現在 `from` peer が持っている全レコード数(テスト用)。
    pub fn len_of(&self, from: PeerId) -> usize {
        self.inner.lock().unwrap().get(&from).map(|v| v.len()).unwrap_or(0)
    }

    /// テスト用に peer を登録 (= `known_peers()` に出すため)。
    /// publish 経由でも自動登録されるが、 まだ 1 度も publish してない peer を
    /// `Syncer::publish_since` の per-peer 経路に含めたい場合に使う。
    pub fn register_peer(&self, peer: PeerId) {
        let mut guard = self.inner.lock().unwrap();
        guard.entry(peer).or_insert_with(Vec::new);
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
        // CRDT 不変式: (peer, hlc) で record は一意。 gossip 経路で同 hlc を
        // 重複受信するため dedupe しないと publish のたびに log が増殖する。
        let existing: std::collections::HashSet<Hlc> =
            log.iter().map(|r| r.hlc).collect();
        for r in records {
            if !existing.contains(&r.hlc) {
                log.push(r);
            }
        }
        log.sort_by_key(|r| r.hlc);
    }

    /// request4: `from` → `to` 専用 log に append。 broadcast 用 `inner` とは
    /// 別の領域なので、 `to` 以外の peer は pull で見えない。
    fn publish_to(&self, from: PeerId, to: PeerId, mut records: Vec<WireRecord>) {
        if records.is_empty() { return; }
        records.sort_by_key(|r| r.hlc);
        // peer 一覧に to を register (= known_peers() で出るように)
        {
            let mut guard = self.inner.lock().unwrap();
            guard.entry(from).or_insert_with(Vec::new);
            guard.entry(to).or_insert_with(Vec::new);
        }
        let mut guard = self.targeted.lock().unwrap();
        let log = guard.entry((from, to)).or_insert_with(Vec::new);
        let existing: std::collections::HashSet<Hlc> =
            log.iter().map(|r| r.hlc).collect();
        for r in records {
            if !existing.contains(&r.hlc) {
                log.push(r);
            }
        }
        log.sort_by_key(|r| r.hlc);
    }

    /// request4: `to` peer 視点での pull。 broadcast log + (from, to) targeted log
    /// を merge し、 HLC > since で filter、 HLC 昇順で返す。
    fn pull_as(&self, to: PeerId, from: PeerId, since: Hlc) -> Vec<WireRecord> {
        let bcast: Vec<WireRecord> = {
            let guard = self.inner.lock().unwrap();
            guard.get(&from).cloned().unwrap_or_default()
        };
        let targeted: Vec<WireRecord> = {
            let guard = self.targeted.lock().unwrap();
            guard.get(&(from, to)).cloned().unwrap_or_default()
        };
        let mut merged: Vec<WireRecord> = bcast.into_iter()
            .chain(targeted)
            .filter(|r| r.hlc > since)
            .collect();
        merged.sort_by_key(|r| r.hlc);
        // dedupe by HLC (broadcast と targeted に同 record があった場合)
        merged.dedup_by_key(|r| r.hlc);
        merged
    }

    fn known_peers(&self) -> Vec<PeerId> {
        let guard = self.inner.lock().unwrap();
        guard.keys().copied().collect()
    }

    /// #216: author 別 filter — record の `author_peer` ごとに cursor を引き、
    /// 未知 author は Hlc::ZERO 起点 (= 全量)。
    fn pull_as_multi(
        &self,
        to: PeerId,
        from: PeerId,
        since: &[(PeerId, Hlc)],
    ) -> Vec<WireRecord> {
        let cursor_of = |author: PeerId| {
            since
                .iter()
                .find(|(p, _)| *p == author)
                .map(|(_, h)| *h)
                .unwrap_or(Hlc::ZERO)
        };
        let bcast: Vec<WireRecord> = {
            let guard = self.inner.lock().unwrap();
            guard.get(&from).cloned().unwrap_or_default()
        };
        let targeted: Vec<WireRecord> = {
            let guard = self.targeted.lock().unwrap();
            guard.get(&(from, to)).cloned().unwrap_or_default()
        };
        let mut merged: Vec<WireRecord> = bcast
            .into_iter()
            .chain(targeted)
            .filter(|r| r.hlc > cursor_of(r.author_peer))
            .collect();
        merged.sort_by_key(|r| r.hlc);
        merged.dedup_by_key(|r| r.hlc);
        merged
    }

    // #140: 広告は「後退させない」— reclaim は進む一方なので floor も単調増加。
    fn set_history_floor(&self, peer: PeerId, floor: Hlc) {
        let mut g = self.floors.lock().unwrap();
        let e = g.entry(peer).or_insert(floor);
        if floor > *e { *e = floor; }
    }

    fn history_floor(&self, peer: PeerId) -> Option<Hlc> {
        self.floors.lock().unwrap().get(&peer).copied()
    }

    // #216: author 別 floor — 単調 max で merge (後退させない)。
    fn set_history_floor_multi(&self, peer: PeerId, floors: &[(PeerId, Hlc)]) {
        let mut g = self.floors_multi.lock().unwrap();
        let slot = g.entry(peer).or_default();
        for (a, h) in floors {
            let e = slot.entry(*a).or_insert(Hlc::ZERO);
            if *h > *e {
                *e = *h;
            }
        }
    }

    fn history_floor_multi(&self, peer: PeerId) -> Option<Vec<(PeerId, Hlc)>> {
        let g = self.floors_multi.lock().unwrap();
        g.get(&peer)
            .map(|m| m.iter().map(|(a, h)| (*a, *h)).collect())
    }

    // #149: ack は puller ごとに max cursor だけ保持 (再送・巻き戻りは無視)。
    fn record_pull_ack(&self, author: PeerId, by: PeerId, cursor: Hlc) {
        let mut g = self.pull_acks.lock().unwrap();
        let slot = g.entry(author).or_default().entry(by).or_insert(Hlc::ZERO);
        if cursor > *slot {
            *slot = cursor;
        }
    }

    fn take_pull_acks(&self, author: PeerId) -> Vec<(PeerId, Hlc)> {
        let mut g = self.pull_acks.lock().unwrap();
        g.remove(&author).map(|m| m.into_iter().collect()).unwrap_or_default()
    }

    // #216: vector ack — puller ごとに author 別 max cursor を merge して保持。
    fn record_pull_ack_multi(
        &self,
        author: PeerId,
        by: PeerId,
        cursors: &[(PeerId, Hlc)],
    ) {
        let mut g = self.pull_acks_multi.lock().unwrap();
        let slot = g.entry(author).or_default().entry(by).or_default();
        for (a, h) in cursors {
            let e = slot.entry(*a).or_insert(Hlc::ZERO);
            if *h > *e {
                *e = *h;
            }
        }
    }

    fn take_pull_acks_multi(&self, author: PeerId) -> Vec<(PeerId, Vec<(PeerId, Hlc)>)> {
        let mut g = self.pull_acks_multi.lock().unwrap();
        g.remove(&author)
            .map(|m| {
                m.into_iter()
                    .map(|(by, cursors)| (by, cursors.into_iter().collect()))
                    .collect()
            })
            .unwrap_or_default()
    }

    // #140: in-process なので provider をそのまま持って fetch 時に呼ぶ。
    fn register_state_provider(&self, peer: PeerId, provider: StateProvider) {
        self.register_state_provider_for(peer, peer, provider);
    }

    // #226: replica 発も受け付ける。 同じ `(author, by)` は置換 (serve_state は
    // 新しい author が増えるたびに呼び直される)。
    fn register_state_provider_for(&self, author: PeerId, by: PeerId, provider: StateProvider) {
        let mut g = self.state_providers.lock().unwrap();
        let slot = g.entry(author).or_default();
        match slot.iter_mut().find(|(p, _)| *p == by) {
            Some(e) => e.1 = provider,
            None => slot.push((by, provider)),
        }
    }

    fn fetch_state(&self, author: PeerId) -> Option<StateBatch> {
        // 本人発 (complete、 ghost sweep 可) を先に。 次に replica 発を登録順に。
        let candidates: Vec<StateProvider> = {
            let g = self.state_providers.lock().unwrap();
            let slot = g.get(&author)?;
            slot.iter()
                .filter(|(by, _)| *by == author)
                .chain(slot.iter().filter(|(by, _)| *by != author))
                .map(|(_, p)| p.clone())
                .collect()
        };
        candidates.into_iter().find_map(|p| p())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enchudb_oplog::oplog::DecodedOp;

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

    /// #242: `pull_as` に渡された cursor を記録するだけの transport。 `pull_as_multi` は
    /// link-author helper に委譲する (直 pull 構成の transport の形)。
    struct LinkAuthorProbe(Mutex<Vec<(PeerId, PeerId, Hlc)>>);

    impl Transport for LinkAuthorProbe {
        fn pull(&self, _from: PeerId, _since: Hlc) -> Vec<WireRecord> {
            Vec::new()
        }
        fn publish(&self, _peer: PeerId, _records: Vec<WireRecord>) {}
        fn pull_as(&self, to: PeerId, from: PeerId, since: Hlc) -> Vec<WireRecord> {
            self.0.lock().unwrap().push((to, from, since));
            Vec::new()
        }
        fn pull_as_multi(&self, to: PeerId, from: PeerId, since: &[(PeerId, Hlc)]) -> Vec<WireRecord> {
            self.pull_as_multi_link_author(to, from, since)
        }
    }

    /// 同じく記録するだけだが `pull_as_multi` を override しない = default 実装のまま。
    struct DefaultProbe(Mutex<Vec<(PeerId, PeerId, Hlc)>>);

    impl Transport for DefaultProbe {
        fn pull(&self, _from: PeerId, _since: Hlc) -> Vec<WireRecord> {
            Vec::new()
        }
        fn publish(&self, _peer: PeerId, _records: Vec<WireRecord>) {}
        fn pull_as(&self, to: PeerId, from: PeerId, since: Hlc) -> Vec<WireRecord> {
            self.0.lock().unwrap().push((to, from, since));
            Vec::new()
        }
    }

    /// #242: 直 pull 構成向け helper は `since` を link author (`from`) の entry に畳んで
    /// scalar `pull_as` に渡す。 entry が無ければ ZERO。
    #[test]
    fn pull_as_multi_link_author_carries_link_cursor() {
        let t = LinkAuthorProbe(Mutex::new(Vec::new()));
        let c3 = Hlc { wall: 300, logical: 1, peer: 3 };
        let c7 = Hlc { wall: 700, logical: 0, peer: 7 };
        t.pull_as_multi(1, 3, &[(7, c7), (3, c3)]);
        t.pull_as_multi(1, 3, &[(7, c7)]);
        let seen = t.0.lock().unwrap();
        assert_eq!(seen[0], (1, 3, c3), "link author's cursor must reach pull_as");
        assert_eq!(seen[1], (1, 3, Hlc::ZERO), "unknown link author falls back to ZERO");
    }

    /// #242 の症状の固定: default 実装は cursor を運ばない (ZERO で pull_as を呼ぶ)。
    /// これが「override しないと ack が前進しない」の根拠。
    #[test]
    fn pull_as_multi_default_drops_cursor() {
        let t = DefaultProbe(Mutex::new(Vec::new()));
        let c3 = Hlc { wall: 300, logical: 1, peer: 3 };
        t.pull_as_multi(1, 3, &[(3, c3)]);
        assert_eq!(t.0.lock().unwrap()[0], (1, 3, Hlc::ZERO));
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
