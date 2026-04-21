//! Chaos シミュレータ — N peer の分散動作にネットワーク故障を注入して観察。
//!
//! In-process で複数 peer を動かし、以下の故障を挿入できる:
//! - **Partition**: 特定 peer 間の通信遮断 (片方向 or 両方向)
//! - **Message loss**: 配送時にドロップ (確率的、seed で deterministic)
//! - **Delay**: メッセージが N tick 後に着くように遅らせる
//! - **Churn**: peer の活性化/停止
//!
//! CRDT (crate::crdt) と組み合わせて「partition heal 後に全 peer が convergent か」を
//! 実機で観察する。
//!
//! # 設計
//!
//! tick-based synchronous simulator。各 tick で:
//! 1. SimNetwork が pending messages を配送 (partition / drop / delay を考慮)
//! 2. 各 peer が受信 msg を apply
//! 3. 各 peer が local 操作 (optional) + broadcast
//!
//! 非同期/並行性は扱わないが、分散アルゴリズムの検証には十分。

#![cfg(feature = "chaos")]

use std::collections::{HashMap, HashSet, VecDeque};

use crate::PeerId;

// ─────────────────────────────────────────────────────────────
// SimNetwork: メッセージ配送 + 故障注入
// ─────────────────────────────────────────────────────────────

/// 配送待ち 1 件。
struct Envelope<M> {
    from: PeerId,
    to: PeerId,
    msg: M,
    deliver_at: u64,
}

/// 全 peer が共有する transport simulator。
pub struct SimNetwork<M: Clone> {
    pending: VecDeque<Envelope<M>>,
    tick: u64,
    /// (from, to) で通信不可 (片方向)。heal で除去。
    partitions: HashSet<(PeerId, PeerId)>,
    /// 0.0〜1.0。deterministic random via simple LCG。
    drop_rate: f64,
    max_delay: u64,
    rng_state: u64,
    /// 統計: dropped / delivered カウント
    pub stat_sent: u64,
    pub stat_dropped: u64,
    pub stat_delivered: u64,
}

impl<M: Clone> SimNetwork<M> {
    pub fn new(seed: u64) -> Self {
        Self {
            pending: VecDeque::new(),
            tick: 0,
            partitions: HashSet::new(),
            drop_rate: 0.0,
            max_delay: 0,
            rng_state: seed.max(1),
            stat_sent: 0,
            stat_dropped: 0,
            stat_delivered: 0,
        }
    }

    /// from → to の通信を両方向遮断する。
    pub fn partition(&mut self, a: PeerId, b: PeerId) {
        self.partitions.insert((a, b));
        self.partitions.insert((b, a));
    }

    /// partition を解除。
    pub fn heal(&mut self, a: PeerId, b: PeerId) {
        self.partitions.remove(&(a, b));
        self.partitions.remove(&(b, a));
    }

    /// メッセージがドロップされる確率 (0.0〜1.0)。
    pub fn set_drop_rate(&mut self, r: f64) { self.drop_rate = r.clamp(0.0, 1.0); }

    /// 配送遅延の最大 tick 数 (0 なら即時)。
    pub fn set_max_delay(&mut self, d: u64) { self.max_delay = d; }

    /// peer から peer へ msg を送る (pending に入れる)。
    /// partition 中 or ドロップ判定でそもそも入らない場合もある。
    pub fn send(&mut self, from: PeerId, to: PeerId, msg: M) {
        self.stat_sent += 1;
        if self.partitions.contains(&(from, to)) {
            self.stat_dropped += 1;
            return;
        }
        if self.drop_rate > 0.0 && self.next_rand() < self.drop_rate {
            self.stat_dropped += 1;
            return;
        }
        let delay = if self.max_delay == 0 { 0 } else { (self.next_rand() * (self.max_delay as f64 + 1.0)) as u64 };
        self.pending.push_back(Envelope {
            from, to, msg,
            deliver_at: self.tick + delay,
        });
    }

    /// 1 tick 進める。今 tick で配送されるべき msg を返す (peer_to, msg)。
    pub fn advance(&mut self) -> Vec<(PeerId, M)> {
        self.tick += 1;
        let mut delivered: Vec<(PeerId, M)> = Vec::new();
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].deliver_at <= self.tick {
                let env = self.pending.remove(i).unwrap();
                // 配送直前にも partition 再チェック (advance 前に partition したケース)
                if self.partitions.contains(&(env.from, env.to)) {
                    self.stat_dropped += 1;
                    continue;
                }
                delivered.push((env.to, env.msg));
                self.stat_delivered += 1;
            } else {
                i += 1;
            }
        }
        delivered
    }

    /// 現在 tick 数。
    pub fn tick(&self) -> u64 { self.tick }

    /// 配送待ちメッセージ数。
    pub fn pending_count(&self) -> usize { self.pending.len() }

    /// LCG ベースの疑似乱数 (0.0〜1.0)。
    fn next_rand(&mut self) -> f64 {
        self.rng_state = self.rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.rng_state as f64 / u64::MAX as f64).abs()
    }
}

// ─────────────────────────────────────────────────────────────
// Tests: chaos シナリオ × CRDT で convergence 検証
// ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::{OrSet, GCounter, LwwRegister};

    /// OrSet<i32> の broadcast message は「現 state 丸ごとコピー」で簡易化
    /// (state-based CRDT の素朴実装、 op-based じゃなく)
    type Msg = OrSet<i32>;

    /// 指定 N peer を作って、各自 OrSet を持つ simulation。
    fn run_scenario<F>(n: u32, max_ticks: u64, step_fn: F) -> (Vec<OrSet<i32>>, u64)
    where F: FnMut(u64, &mut [OrSet<i32>], &mut SimNetwork<Msg>),
    {
        let mut peers: Vec<OrSet<i32>> = (1..=n).map(OrSet::new).collect();
        let mut net = SimNetwork::new(42);
        let mut step = step_fn;

        for tick in 0..max_ticks {
            // 配送される msg を各 peer に apply
            let delivered = net.advance();
            for (to, msg) in delivered {
                let idx = (to - 1) as usize;
                if idx < peers.len() {
                    peers[idx].merge(&msg);
                }
            }
            // シナリオ step
            step(tick, &mut peers, &mut net);
        }
        (peers, net.tick)
    }

    /// 全 peer が同じ要素集合を持つか。
    fn all_converged(peers: &[OrSet<i32>]) -> bool {
        if peers.len() < 2 { return true; }
        let first: HashSet<i32> = peers[0].iter().cloned().collect();
        peers.iter().all(|p| {
            let s: HashSet<i32> = p.iter().cloned().collect();
            s == first
        })
    }

    fn broadcast<M: Clone>(peer_id: PeerId, msg: M, net: &mut SimNetwork<M>, n_peers: u32) {
        for other in 1..=n_peers {
            if other != peer_id {
                net.send(peer_id, other, msg.clone());
            }
        }
    }

    #[test]
    fn healthy_3_peer_converge() {
        // 3 peer が 1 tick 目に各自 add、以降 broadcast、convergent 確認
        let (peers, _) = run_scenario(3, 10, |tick, peers, net| {
            if tick == 1 {
                peers[0].add(1);
                peers[1].add(2);
                peers[2].add(3);
                let m0 = peers[0].clone();
                let m1 = peers[1].clone();
                let m2 = peers[2].clone();
                broadcast(1, m0, net, 3);
                broadcast(2, m1, net, 3);
                broadcast(3, m2, net, 3);
            }
        });
        assert!(all_converged(&peers));
        let set: HashSet<i32> = peers[0].iter().cloned().collect();
        assert_eq!(set, [1, 2, 3].iter().cloned().collect());
    }

    #[test]
    fn partition_heal_converges() {
        // peer 1, 2, 3: 最初 1-{2,3} に partition、それぞれ local add
        // partition 中は相互 merge できない。heal 後に broadcast、convergent に。
        let (peers, _) = run_scenario(3, 30, |tick, peers, net| {
            match tick {
                0 => {
                    net.partition(1, 2);
                    net.partition(1, 3);
                }
                1 => {
                    peers[0].add(10);
                    peers[1].add(20);
                    peers[2].add(30);
                    let m0 = peers[0].clone();
                    let m1 = peers[1].clone();
                    let m2 = peers[2].clone();
                    broadcast(1, m0, net, 3);
                    broadcast(2, m1, net, 3);
                    broadcast(3, m2, net, 3);
                }
                10 => {
                    // partition 中の add (2 と 3 は繋がってる、1 は孤立)
                    peers[0].add(11);
                    peers[1].add(21);
                    let m0 = peers[0].clone();
                    let m1 = peers[1].clone();
                    broadcast(1, m0, net, 3);
                    broadcast(2, m1, net, 3);
                }
                15 => {
                    // heal
                    net.heal(1, 2);
                    net.heal(1, 3);
                    let m0 = peers[0].clone();
                    let m1 = peers[1].clone();
                    let m2 = peers[2].clone();
                    broadcast(1, m0, net, 3);
                    broadcast(2, m1, net, 3);
                    broadcast(3, m2, net, 3);
                }
                _ => {}
            }
        });
        assert!(all_converged(&peers), "partition heal 後に convergent になるはず");
        let set: HashSet<i32> = peers[0].iter().cloned().collect();
        // 全 add: {10, 11, 20, 21, 30}
        assert_eq!(set, [10, 11, 20, 21, 30].iter().cloned().collect());
    }

    #[test]
    fn high_drop_rate_still_converges_with_retry() {
        // drop_rate 60%、定期 re-broadcast で最終的に全 peer が揃う
        let mut net = SimNetwork::new(99);
        net.set_drop_rate(0.6);
        let mut peers: Vec<OrSet<i32>> = (1..=3).map(OrSet::new).collect();
        peers[0].add(1); peers[0].add(2);
        peers[1].add(3);
        peers[2].add(4); peers[2].add(5);

        for _tick in 0..200 {
            for (to, msg) in net.advance() {
                let idx = (to - 1) as usize;
                if idx < peers.len() {
                    peers[idx].merge(&msg);
                }
            }
            // 毎 tick 全 peer が broadcast (retry に相当)
            let m0 = peers[0].clone();
            let m1 = peers[1].clone();
            let m2 = peers[2].clone();
            broadcast(1, m0, &mut net, 3);
            broadcast(2, m1, &mut net, 3);
            broadcast(3, m2, &mut net, 3);
        }
        assert!(all_converged(&peers));
        assert_eq!(peers[0].len(), 5);
        // ドロップされた割合を確認
        assert!(net.stat_dropped > 0);
        assert!(net.stat_dropped as f64 / net.stat_sent as f64 > 0.3);
    }

    #[test]
    fn delayed_delivery_preserves_convergence() {
        // max_delay=5 で delivery に 0〜5 tick 遅延、convergence に影響なし
        let mut net = SimNetwork::new(7);
        net.set_max_delay(5);
        let mut peers: Vec<OrSet<i32>> = (1..=3).map(OrSet::new).collect();
        peers[0].add(1);
        peers[1].add(2);
        peers[2].add(3);
        let m0 = peers[0].clone(); let m1 = peers[1].clone(); let m2 = peers[2].clone();
        broadcast(1, m0, &mut net, 3);
        broadcast(2, m1, &mut net, 3);
        broadcast(3, m2, &mut net, 3);
        for _ in 0..20 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(all_converged(&peers));
    }

    #[test]
    fn large_n_convergence_time() {
        // 20 peer で random add、convergence までの tick 数を計測
        let mut net = SimNetwork::new(123);
        let n = 20u32;
        let mut peers: Vec<OrSet<i32>> = (1..=n).map(OrSet::new).collect();

        // 各 peer が 1 個ずつ add
        for i in 0..n {
            peers[i as usize].add(i as i32);
            let m = peers[i as usize].clone();
            broadcast(i + 1, m, &mut net, n);
        }

        let mut converged_at: Option<u64> = None;
        for tick in 1..200 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
            // 毎 tick 全員再 broadcast (gossip っぽく)
            if tick % 5 == 0 {
                for i in 0..n {
                    let m = peers[i as usize].clone();
                    broadcast(i + 1, m, &mut net, n);
                }
            }
            if all_converged(&peers) && peers[0].len() == n as usize {
                converged_at = Some(tick);
                break;
            }
        }
        let tick = converged_at.expect("should converge within 200 ticks");
        // 印刷して可視化 (cargo test -- --nocapture で見える)
        println!("large_n_convergence_time: 20 peer converged in {} ticks", tick);
        assert!(tick < 100);
    }

    #[test]
    fn lww_vs_crdt_under_partition_data_loss_comparison() {
        // Alice と Bob、partition 中に両方同じ register に書く。heal 後:
        //  LWW: 片方しか残らない (データロス)
        //  OR-Set: 両方残る
        let mut net: SimNetwork<()> = SimNetwork::new(1);
        net.partition(1, 2);

        let mut lww_a: LwwRegister<&str> = LwwRegister::new();
        let mut lww_b: LwwRegister<&str> = LwwRegister::new();
        let mut set_a: OrSet<&str> = OrSet::new(1);
        let mut set_b: OrSet<&str> = OrSet::new(2);

        // 並行 write during partition
        lww_a.set("msg_from_alice", 100);
        lww_b.set("msg_from_bob", 150);
        set_a.add("msg_from_alice");
        set_b.add("msg_from_bob");

        // heal + merge 両方向
        net.heal(1, 2);
        lww_a.merge(&lww_b);
        lww_b.merge(&lww_a);
        let set_a_before = set_a.clone();
        let set_b_before = set_b.clone();
        set_a.merge(&set_b_before);
        set_b.merge(&set_a_before);

        // LWW は片方 (timestamp 大の方)
        assert_eq!(lww_a.get(), Some(&"msg_from_bob"));
        assert_eq!(lww_b.get(), Some(&"msg_from_bob"));
        // Alice のメッセージは消失

        // OR-Set は両方残る
        assert_eq!(set_a.len(), 2);
        assert!(set_a.contains(&"msg_from_alice"));
        assert!(set_a.contains(&"msg_from_bob"));
        assert_eq!(set_b.len(), 2);
    }

    #[test]
    fn gcounter_under_chaos_converges() {
        // 5 peer が各 10 回 inc、50% drop、遅延あり、最終的に全 peer が value=50 になる
        let mut net: SimNetwork<GCounter> = SimNetwork::new(777);
        net.set_drop_rate(0.5);
        net.set_max_delay(3);

        let n = 5u32;
        let mut peers: Vec<GCounter> = (0..n).map(|_| GCounter::new()).collect();
        for i in 0..n {
            for _ in 0..10 { peers[i as usize].inc(i + 1); }
        }

        for _tick in 0..300 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
            // 毎 tick 全員 broadcast
            for i in 0..n {
                let m = peers[i as usize].clone();
                broadcast(i + 1, m, &mut net, n);
            }
        }
        for p in &peers {
            assert_eq!(p.value(), 50);
        }
    }
}
