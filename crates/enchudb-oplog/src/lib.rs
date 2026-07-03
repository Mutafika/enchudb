//! EnchuDB oplog crate — wire-level primitives shared across engine / sync / transport.
//!
//! このクレートは以下を提供する:
//! - **Hlc / PeerId / EntityId**: 分散 op に必要な全順序・peer 識別の primitive 型
//! - **OpLog**: append-only operation log (v2 format)
//! - **Keypair / PubkeyStore**: oplog 署名検証用の ed25519 鍵管理
//!
//! `enchudb-engine` から切り出すことで「oplog は engine の内部詳細」ではなく
//! 「分散 DB の正準パケットフォーマット」として独立リリース可能にする。
//!
//! 命名: 旧 `enchudb-wal` から rename (issue #8、 0.6.0 で実施)。 実態が
//! write-ahead log ではなく oplog (operation log、 MongoDB oplog と同パターン)
//! なため。 wire format は不変、 file 拡張子は `.wal` → `.oplog` (clean break)。
//!
//! # 0.7.0 以降の役割 (issue #11 / request7)
//!
//! 0.7.0 で engine 直下に `_sync_ops` / `_sync_peers` reserved table を導入し、
//! 「peer 配信 stream」 の役割を table 側に移すための土台が出来た。 ただし
//! 0.7.0 時点では並走 (= 既存 oplog 経路と新 `_sync_ops` 経路の両方が動く):
//!
//! - **本 crate (`enchudb-oplog`)**: local crash recovery + peer 配信 (legacy)
//! - **`Engine::transfer_oplog_to_sync_ops`**: oplog から `_sync_ops` への転送
//! - **`Engine::pending_sync_ops`**: SQL から見える新 publish 経路 (将来主体)
//!
//! 0.8.0 以降で:
//! - `_sync_ops` 経路を sync の primary publish source に
//! - oplog crate は local crash recovery 専用に shrink (= file 拡張子 / wire
//!   format / record encoding は維持、 peer publish は移行)
//! - もしくは `_sync_ops` で local recovery も兼任する deprecate 路線
//!
//! 詳細: `notes/requests/request7.md` (Phase 7 discussion)。

pub mod keys;
pub mod oplog;

/// Content key 用の簡易 hash → 15bit。MSB は LWW key namespace で占有。
/// engine 側の recover hydrate と sync 側の apply_one で同じハッシュを使うため
/// oplog crate に共通化してある。 変更すると HlcStore キーが分岐するので注意。
#[inline]
pub fn content_key_hash15(s: &str) -> u16 {
    let mut h: u32 = 0x811c9dc5;
    for &b in s.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    // #78: `% 0x7fff` で値域を 0..=0x7FFE に制限する。 旧 `& 0x7fff` は 0x7FFF を
    // 返し得て、 caller の `hash | 0x8000` が 0xFFFF = Delete tombstone sentinel
    // (u16::MAX) と衝突 — 該当 key への Content 書き込みが偽 tombstone を作り、
    // 以後の op が「削除済み」として skip / 正規 Delete が適用されない / .eidmap
    // に偽 tombstone が永続化されていた。 0.9.0 以降この hash は legacy Content
    // (pre-0.9 WAL) の HLC slot にのみ使われる。
    (h as u16) % 0x7fff
}

/// v32: Entity ID。u64 = `[peer_id: 32bit][local_id: 32bit]`。
/// 単独 peer 運用時は peer_id = 0、実質 local_id のみ使われる。
pub type EntityId = u64;

/// v32: Peer ID。各 peer が固有 u32 ID を持ち、上位 32bit を EntityId に埋め込む。
pub type PeerId = u32;

/// local_id と peer_id から EntityId を合成。
#[inline]
pub const fn make_eid(peer: PeerId, local: u32) -> EntityId {
    ((peer as u64) << 32) | (local as u64)
}

/// EntityId から peer 部分を取得。
#[inline]
pub const fn eid_peer(eid: EntityId) -> PeerId {
    (eid >> 32) as u32
}

/// EntityId から local 部分を取得。
#[inline]
pub const fn eid_local(eid: EntityId) -> u32 {
    eid as u32
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
