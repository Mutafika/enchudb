//! Shard routing — himo 名で owner peer を決め、跨 peer で `pull_raw` する。
//!
//! 各 peer が「担当 himo」だけ `define_himo` して write する構成で使う。
//! v33 の Syncer は WAL record を全 peer に撒くが、受信側で himo 未定義なら
//! silently drop するため、storage は自然に sharded される。
//! このモジュールはその上で **query の routing** だけを足す薄い層。
//!
//! # 使い方
//!
//! ```no_run
//! use std::sync::Arc;
//! use enchudb_engine::Engine;
//! use enchudb_sync::shard::{HashRouter, InMemoryShardTransport, ShardQuery};
//!
//! let eng_a = Engine::create_concurrent_with_wal("/tmp/a.db", 4 * 1024 * 1024).unwrap();
//! eng_a.set_peer_id(1);
//!
//! // 3 peer で 分散
//! let router = Arc::new(HashRouter::new(vec![1, 2, 3]));
//! let transport = Arc::new(InMemoryShardTransport::new());
//! // transport.register(peer_id, engine) で全 peer の engine を登録する
//!
//! let q = ShardQuery::new(eng_a.clone(), router, transport);
//! let hits = q.pull_raw("color", 5);
//! ```
//!
//! # 設計原則
//!
//! - **router は pure function**: `himo 名 → peer id` を返すだけ、状態持たない
//! - **transport は I/O**: remote peer から column slice を fetch する。
//!   in-memory / HTTP / WS は別 crate で実装
//! - **ShardQuery は薄い wrapper**: local なら engine.pull_raw、remote なら
//!   transport.pull_column。そのどちらかだけ

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use enchudb_engine::{Engine};
use enchudb_wal::{EntityId, PeerId};

/// himo 名からその himo の owner peer を決める。
pub trait ShardRouter: Send + Sync {
    /// `himo` を担当する peer の id を返す。
    fn owner_of(&self, himo: &str) -> PeerId;

    /// クラスタに所属する全 peer の一覧。
    fn peers(&self) -> &[PeerId];

    /// `self_peer` が `himo` の owner か。
    #[inline]
    fn is_mine(&self, himo: &str, self_peer: PeerId) -> bool {
        self.owner_of(himo) == self_peer
    }
}

/// FNV-1a ベースの consistent-ish hash router。
///
/// 単純な `hash(himo) % n_peers` で peer を決める。
/// peer の増減で大半の assignment が変わるが、静的クラスタ (niente 辞書用途)
/// ではこれで十分。rebalance 必要な動的クラスタは別 router を書く。
pub struct HashRouter {
    peers: Vec<PeerId>,
}

impl HashRouter {
    /// `peers` は空でないこと。
    pub fn new(peers: Vec<PeerId>) -> Self {
        assert!(!peers.is_empty(), "HashRouter requires at least one peer");
        Self { peers }
    }
}

impl ShardRouter for HashRouter {
    fn owner_of(&self, himo: &str) -> PeerId {
        let h = fnv1a(himo.as_bytes()) as usize;
        self.peers[h % self.peers.len()]
    }

    fn peers(&self) -> &[PeerId] {
        &self.peers
    }
}

/// 手動で `himo → peer` mapping を指定する router。
/// テスト用 / rebalance 後の固定 assignment を持ちたい場合に使う。
pub struct ExplicitRouter {
    map: HashMap<String, PeerId>,
    peers: Vec<PeerId>,
    fallback: PeerId,
}

impl ExplicitRouter {
    /// `fallback` は map に無い himo が来た時の owner。
    pub fn new(map: HashMap<String, PeerId>, fallback: PeerId) -> Self {
        let mut peers: Vec<PeerId> = map.values().copied().collect();
        peers.push(fallback);
        peers.sort_unstable();
        peers.dedup();
        Self { map, peers, fallback }
    }
}

impl ShardRouter for ExplicitRouter {
    fn owner_of(&self, himo: &str) -> PeerId {
        self.map.get(himo).copied().unwrap_or(self.fallback)
    }
    fn peers(&self) -> &[PeerId] {
        &self.peers
    }
}

/// remote peer から column slice (`himo = value` に合致する entity 群) を取得する I/O。
pub trait ShardTransport: Send + Sync {
    /// `from` peer に `himo = value` を query、合致する EntityId を返す。
    /// 返り値の eid は [from peer_id | local_id] に composite 済みが望ましい
    /// (peer 跨ぎで混ざっても区別できるように)。
    fn pull_column(&self, from: PeerId, himo: &str, value: u32) -> Vec<EntityId>;
}

/// in-memory で複数 Engine を持ち、ローカル呼び出しで `pull_column` を解決する。
/// テスト + 同一プロセス内 sharding 実験用。
#[derive(Default)]
pub struct InMemoryShardTransport {
    engines: Mutex<HashMap<PeerId, Arc<Engine>>>,
}

impl InMemoryShardTransport {
    pub fn new() -> Self {
        Self { engines: Mutex::new(HashMap::new()) }
    }

    /// クラスタに engine を登録。sharded query が届くよう ShardQuery::new 前に全 peer 登録する。
    pub fn register(&self, peer: PeerId, engine: Arc<Engine>) {
        self.engines.lock().unwrap().insert(peer, engine);
    }
}

impl ShardTransport for InMemoryShardTransport {
    fn pull_column(&self, from: PeerId, himo: &str, value: u32) -> Vec<EntityId> {
        let engines = self.engines.lock().unwrap();
        let Some(eng) = engines.get(&from) else { return Vec::new() };
        // engine.pull_raw は local u32 eid を Vec<EntityId> (u64) で返すが、
        // 上位 32bit は 0。sharded では from peer を上位に乗せる。
        eng.pull_raw(himo, value)
            .into_iter()
            .map(|e| enchudb_wal::make_eid(from, e as u32))
            .collect()
    }
}

/// 跨 peer で pull_raw / query を発火する薄い wrapper。
pub struct ShardQuery {
    engine: Arc<Engine>,
    router: Arc<dyn ShardRouter>,
    transport: Arc<dyn ShardTransport>,
    self_peer: PeerId,
}

impl ShardQuery {
    pub fn new(
        engine: Arc<Engine>,
        router: Arc<dyn ShardRouter>,
        transport: Arc<dyn ShardTransport>,
    ) -> Self {
        let self_peer = engine.peer_id();
        Self { engine, router, transport, self_peer }
    }

    /// `himo = value` に合致する EntityId を返す。owner peer が自分ならローカル、
    /// そうでなければ transport 経由で remote fetch。
    pub fn pull_raw(&self, himo: &str, value: u32) -> Vec<EntityId> {
        let owner = self.router.owner_of(himo);
        if owner == self.self_peer {
            // 自 peer が owner、ローカル column から
            self.engine.pull_raw(himo, value)
                .into_iter()
                .map(|e| enchudb_wal::make_eid(self.self_peer, e as u32))
                .collect()
        } else {
            // remote
            self.transport.pull_column(owner, himo, value)
        }
    }

    /// 複数条件 AND。各 himo の owner から並列に (sync 的には逐次)、sorted intersect。
    /// 条件が 0 件なら `Vec::new()` を返す。
    pub fn query(&self, conditions: &[(&str, u32)]) -> Vec<EntityId> {
        if conditions.is_empty() { return Vec::new(); }
        let mut sets: Vec<Vec<EntityId>> = conditions.iter()
            .map(|(h, v)| {
                let mut hits = self.pull_raw(h, *v);
                hits.sort_unstable();
                hits.dedup();
                hits
            })
            .collect();
        // 最小サイズを pivot に (intersect の古典的最適化)
        sets.sort_by_key(|s| s.len());
        let mut acc = sets.remove(0);
        for s in sets {
            acc = sorted_intersect(&acc, &s);
            if acc.is_empty() { break; }
        }
        acc
    }

    /// router への生アクセス (テスト用 / 他の routing 判定用)。
    pub fn router(&self) -> &dyn ShardRouter {
        &*self.router
    }

    /// 自分の担当 himo かどうか (write 側で `tie_async` してよいか判定に使う)。
    pub fn is_mine(&self, himo: &str) -> bool {
        self.router.is_mine(himo, self.self_peer)
    }
}

fn sorted_intersect(a: &[EntityId], b: &[EntityId]) -> Vec<EntityId> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            out.push(a[i]);
            i += 1; j += 1;
        } else if a[i] < b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

#[inline]
fn fnv1a(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_router_determinism() {
        let r = HashRouter::new(vec![1, 2, 3]);
        // 同じ himo は常に同じ peer に
        for _ in 0..100 {
            assert_eq!(r.owner_of("color"), r.owner_of("color"));
            assert_eq!(r.owner_of("age"), r.owner_of("age"));
        }
    }

    #[test]
    fn hash_router_distributes() {
        let r = HashRouter::new(vec![1, 2, 3, 4, 5]);
        let himos = (0..1000).map(|i| format!("h_{i}")).collect::<Vec<_>>();
        let mut counts: HashMap<PeerId, usize> = HashMap::new();
        for h in &himos {
            *counts.entry(r.owner_of(h)).or_default() += 1;
        }
        // 5 peer に大体均等に散る(1000/5 = 200、±50% くらい)
        for (_, c) in &counts {
            assert!(*c > 100 && *c < 300, "uneven distribution: {:?}", counts);
        }
    }

    #[test]
    fn explicit_router_map_and_fallback() {
        let mut m = HashMap::new();
        m.insert("a".to_string(), 1);
        m.insert("b".to_string(), 2);
        let r = ExplicitRouter::new(m, 99);
        assert_eq!(r.owner_of("a"), 1);
        assert_eq!(r.owner_of("b"), 2);
        assert_eq!(r.owner_of("unknown"), 99);
    }

    #[test]
    fn sorted_intersect_basic() {
        assert_eq!(sorted_intersect(&[1, 3, 5, 7], &[2, 3, 5, 8]), vec![3, 5]);
        assert_eq!(sorted_intersect(&[1, 2, 3], &[4, 5, 6]), Vec::<EntityId>::new());
        assert_eq!(sorted_intersect(&[], &[1, 2]), Vec::<EntityId>::new());
    }
}
