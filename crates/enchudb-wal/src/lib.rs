//! EnchuDB WAL crate — wire-level primitives shared across engine / sync / transport.
//!
//! このクレートは以下を提供する:
//! - **Hlc / PeerId / EntityId**: 分散 op に必要な全順序・peer 識別の primitive 型
//! - **Wal**: append-only operation log (v2 format)
//! - **Keypair / PubkeyStore**: WAL 署名検証用の ed25519 鍵管理
//!
//! `enchudb-engine` から切り出すことで「WAL は engine の内部詳細」ではなく
//! 「分散 DB の正準パケットフォーマット」として独立リリース可能にする。
//! 既存の `enchudb_engine::wal::*` / `enchudb_engine::keys::*` / `enchudb_engine::Hlc`
//! などのパスは engine 側で re-export して後方互換を保つ。

pub mod keys;
pub mod wal;

/// Content key 用の簡易 hash → 15bit。MSB は LWW key namespace で占有。
/// engine 側の recover hydrate と sync 側の apply_one で同じハッシュを使うため
/// wal crate に共通化してある。 変更すると HlcStore キーが分岐するので注意。
#[inline]
pub fn content_key_hash15(s: &str) -> u16 {
    let mut h: u32 = 0x811c9dc5;
    for &b in s.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    (h as u16) & 0x7fff
}

/// v32 → v3 phase 3: Entity ID は u64。
///
/// **bit layout (β-heavy phase 3 以降)**:
/// ```text
/// [63..40]  peer_id        (24 bit、 1600万 peer)
/// [39..24]  table_id       (16 bit、 65k tables、 0 = anonymous)
/// [23..0]   table_local    (24 bit、 1600万 row/table)
/// ```
///
/// 旧 layout は `[peer: 32][local: 32]`。 互換性: `peer < 2^24` かつ
/// `table_id = 0` (= anonymous-only DB) かつ `local < 2^24` の範囲なら、
/// 旧/新 で numerical value が一致するため、 0.5.0 (file format v6) で
/// 作った single-peer DB はそのまま decode 可能。 multi-peer (peer >= 2^24)
/// または table_id != 0 の DB は新 layout を前提とする (v7 file format)。
pub type EntityId = u64;

/// v32: Peer ID。 phase 3 以降は実質 24 bit のみ使用 (上位 8 bit は予約)。
pub type PeerId = u32;

/// Table ID。 0 = anonymous table、 1.. が named table の sequential id。
/// engine の `TableId` (= u16) と同 alias。
pub type TableId = u16;

/// PeerId の最大値 (24 bit = 16M)。 これを超える peer 番号は EntityId に
/// 詰めて返した時に上位 bit が欠ける。
pub const MAX_PEER_ID: PeerId = (1 << 24) - 1;
/// Table 内 local id の最大値 (24 bit = 16M)。
pub const MAX_TABLE_LOCAL: u32 = (1 << 24) - 1;

const TABLE_LOCAL_BITS: u32 = 24;
const TABLE_ID_BITS: u32 = 16;
const TABLE_LOCAL_MASK: u64 = (1u64 << TABLE_LOCAL_BITS) - 1;
const TABLE_ID_MASK: u64 = (1u64 << TABLE_ID_BITS) - 1;

/// (peer, table_id, table_local) から EntityId を合成 (phase 3 新 API)。
///
/// debug_assert で各フィールドの上限超過を検出。 release では high bits が
/// 黙って捨てられる (= mod 2^N で truncate)。
#[inline]
pub const fn make_eid_in_table(peer: PeerId, table_id: TableId, table_local: u32) -> EntityId {
    debug_assert!(peer <= MAX_PEER_ID, "peer id exceeds 24 bit");
    debug_assert!(table_local <= MAX_TABLE_LOCAL, "table_local exceeds 24 bit");
    ((peer as u64 & ((1u64 << 24) - 1)) << (TABLE_LOCAL_BITS + TABLE_ID_BITS))
        | ((table_id as u64 & TABLE_ID_MASK) << TABLE_LOCAL_BITS)
        | (table_local as u64 & TABLE_LOCAL_MASK)
}

/// 旧 `(peer, local)` 互換 API。 `local` を anonymous table の table_local
/// として扱う (= table_id = 0)。
///
/// **互換性**: peer < 2^24 かつ local < 2^24 の範囲では、 旧 layout の
/// `((peer << 32) | local)` と一致する 「ように見える」 が、 厳密には
/// 異なる (= peer ≥ 2^24 で differ)。 旧 layout に依存するコードは
/// `make_eid_in_table` を直接使うこと。
#[inline]
pub const fn make_eid(peer: PeerId, local: u32) -> EntityId {
    make_eid_in_table(peer, 0, local)
}

/// EntityId から peer 部分を取得 (24 bit)。 旧 32 bit peer の上位 8 bit は捨てる。
#[inline]
pub const fn eid_peer(eid: EntityId) -> PeerId {
    (eid >> (TABLE_LOCAL_BITS + TABLE_ID_BITS)) as u32 & MAX_PEER_ID
}

/// EntityId から table_id 部分を取得 (16 bit)。 0 = anonymous。
#[inline]
pub const fn eid_table_id(eid: EntityId) -> TableId {
    ((eid >> TABLE_LOCAL_BITS) & TABLE_ID_MASK) as u16
}

/// EntityId から table_local 部分を取得 (24 bit)。 engine の column / positions
/// index として使う。
#[inline]
pub const fn eid_table_local(eid: EntityId) -> u32 {
    (eid & TABLE_LOCAL_MASK) as u32
}

/// 旧 `eid_local` 互換: anonymous table 想定で table_local 部分を返す。
/// 新コードは `eid_table_local` を直接呼ぶ方が intent が明確。
#[inline]
pub const fn eid_local(eid: EntityId) -> u32 {
    eid_table_local(eid)
}

/// v32: Hybrid Logical Clock。分散 peer 間で全順序を確立する。
/// wall: 物理時刻(ms since epoch)、logical: 同時刻内のカウンタ、peer: tiebreaker。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hlc {
    pub wall: u64,
    pub logical: u32,
    pub peer: PeerId,
}

impl Hlc {
    pub const ZERO: Hlc = Hlc { wall: 0, logical: 0, peer: 0 };

    /// 比較キー。`(wall, logical, peer)` 辞書順で全順序。
    #[inline]
    pub fn cmp_key(&self) -> (u64, u32, u32) {
        (self.wall, self.logical, self.peer)
    }

    /// 受信した HLC をもとに自 peer の次 HLC を計算する(HLC の merge ルール)。
    /// - wall = max(local_wall, received_wall, now_wall)
    /// - logical = 新 wall が全部一致なら max(local_logical, received_logical)+1、でなければ 0
    pub fn merge_recv(local: Hlc, recv: Hlc, now_wall: u64, local_peer: PeerId) -> Hlc {
        let new_wall = local.wall.max(recv.wall).max(now_wall);
        let new_logical = if new_wall == local.wall && new_wall == recv.wall {
            local.logical.max(recv.logical).wrapping_add(1)
        } else if new_wall == local.wall {
            local.logical.wrapping_add(1)
        } else if new_wall == recv.wall {
            recv.logical.wrapping_add(1)
        } else {
            0
        };
        Hlc { wall: new_wall, logical: new_logical, peer: local_peer }
    }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp_key().cmp(&other.cmp_key())
    }
}
