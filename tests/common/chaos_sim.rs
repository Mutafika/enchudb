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

#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use enchudb::PeerId;

// ─────────────────────────────────────────────────────────────
// SimNetwork: メッセージ配送 + 故障注入
// ─────────────────────────────────────────────────────────────

/// 配送待ち 1 件。BinaryHeap の順序用に seq を持つ。
struct Envelope<M> {
    from: PeerId,
    to: PeerId,
    msg: M,
    deliver_at: u64,
    /// 挿入順のタイブレーカー (BinaryHeap で stable に)。
    seq: u64,
}

// BinaryHeap は max-heap なので、deliver_at の **小さい方** を優先したい →
// Ord を逆向きに定義する。seq も同様。
impl<M> PartialEq for Envelope<M> {
    fn eq(&self, o: &Self) -> bool { self.deliver_at == o.deliver_at && self.seq == o.seq }
}
impl<M> Eq for Envelope<M> {}
impl<M> PartialOrd for Envelope<M> { fn partial_cmp(&self, o: &Self) -> Option<Ordering> { Some(self.cmp(o)) } }
impl<M> Ord for Envelope<M> {
    fn cmp(&self, o: &Self) -> Ordering {
        // max-heap を min-heap として使うため逆順
        o.deliver_at.cmp(&self.deliver_at).then_with(|| o.seq.cmp(&self.seq))
    }
}

/// 全 peer が共有する transport simulator。
pub struct SimNetwork<M: Clone> {
    /// BinaryHeap ベースの priority queue (deliver_at 昇順)。push/pop が O(log n)。
    pending: BinaryHeap<Envelope<M>>,
    /// send 順のグローバル seq (BinaryHeap のタイブレーク用)。
    next_seq: u64,
    tick: u64,
    /// (from, to) で通信不可 (片方向)。heal で除去。
    partitions: HashSet<(PeerId, PeerId)>,
    /// crash した peer 集合。crash 中は send/recv 両方とも drop。
    crashed: HashSet<PeerId>,
    /// 0.0〜1.0。deterministic random via simple LCG。
    drop_rate: f64,
    max_delay: u64,
    /// (from, to) → max_delay override。無ければグローバル max_delay を使う。
    pair_delays: HashMap<(PeerId, PeerId), u64>,
    rng_state: u64,
    /// 統計: dropped / delivered カウント
    pub stat_sent: u64,
    pub stat_dropped: u64,
    pub stat_delivered: u64,
}

impl<M: Clone> SimNetwork<M> {
    pub fn new(seed: u64) -> Self {
        Self {
            pending: BinaryHeap::new(),
            next_seq: 0,
            tick: 0,
            partitions: HashSet::new(),
            crashed: HashSet::new(),
            drop_rate: 0.0,
            max_delay: 0,
            pair_delays: HashMap::new(),
            rng_state: seed.max(1),
            stat_sent: 0,
            stat_dropped: 0,
            stat_delivered: 0,
        }
    }

    /// a ⇄ b を両方向遮断。
    pub fn partition(&mut self, a: PeerId, b: PeerId) {
        self.partitions.insert((a, b));
        self.partitions.insert((b, a));
    }

    /// from → to のみ遮断 (逆方向は通る、非対称 partition)。
    pub fn partition_one_way(&mut self, from: PeerId, to: PeerId) {
        self.partitions.insert((from, to));
    }

    /// partition を解除 (両方向)。
    pub fn heal(&mut self, a: PeerId, b: PeerId) {
        self.partitions.remove(&(a, b));
        self.partitions.remove(&(b, a));
    }

    /// peer を crash させる。以降 send/recv 両方 drop される。state は caller 側で管理。
    pub fn crash(&mut self, peer: PeerId) {
        self.crashed.insert(peer);
    }

    /// crash した peer を復帰。state リセットは caller 責任。
    pub fn restore(&mut self, peer: PeerId) {
        self.crashed.remove(&peer);
    }

    /// peer が crash 中か。
    pub fn is_crashed(&self, peer: PeerId) -> bool {
        self.crashed.contains(&peer)
    }

    /// メッセージがドロップされる確率 (0.0〜1.0)。
    pub fn set_drop_rate(&mut self, r: f64) { self.drop_rate = r.clamp(0.0, 1.0); }

    /// 配送遅延の最大 tick 数 (0 なら即時)。
    pub fn set_max_delay(&mut self, d: u64) { self.max_delay = d; }

    /// (from, to) の最大遅延を個別設定。グローバル `max_delay` より優先。
    pub fn set_pair_delay(&mut self, from: PeerId, to: PeerId, max: u64) {
        self.pair_delays.insert((from, to), max);
    }

    /// 統計カウンタを 0 に戻す (partition や crashed の状態はそのまま)。
    pub fn reset_stats(&mut self) {
        self.stat_sent = 0;
        self.stat_dropped = 0;
        self.stat_delivered = 0;
    }

    /// peer から peer へ msg を送る (pending に入れる)。
    /// partition / crashed / ドロップ判定で入らない場合もある。
    pub fn send(&mut self, from: PeerId, to: PeerId, msg: M) {
        self.stat_sent += 1;
        if self.crashed.contains(&from) || self.crashed.contains(&to) {
            self.stat_dropped += 1;
            return;
        }
        if self.partitions.contains(&(from, to)) {
            self.stat_dropped += 1;
            return;
        }
        if self.drop_rate > 0.0 && self.next_rand() < self.drop_rate {
            self.stat_dropped += 1;
            return;
        }
        let effective_max = self.pair_delays.get(&(from, to)).copied().unwrap_or(self.max_delay);
        let delay = if effective_max == 0 { 0 } else { (self.next_rand() * (effective_max as f64 + 1.0)) as u64 };
        let seq = self.next_seq;
        self.next_seq += 1;
        self.pending.push(Envelope {
            from, to, msg,
            deliver_at: self.tick + delay,
            seq,
        });
    }

    /// 1 tick 進める。今 tick で配送されるべき msg を返す (peer_to, msg)。
    /// BinaryHeap から top (最早 deliver_at) を順次 pop、deliver_at > tick で停止。
    pub fn advance(&mut self) -> Vec<(PeerId, M)> {
        self.tick += 1;
        let mut delivered: Vec<(PeerId, M)> = Vec::new();
        while let Some(top) = self.pending.peek() {
            if top.deliver_at > self.tick { break; }
            let env = self.pending.pop().unwrap();
            // 配送直前の再チェック: partition installed after send、crash after send
            if self.partitions.contains(&(env.from, env.to))
                || self.crashed.contains(&env.from)
                || self.crashed.contains(&env.to)
            {
                self.stat_dropped += 1;
                continue;
            }
            delivered.push((env.to, env.msg));
            self.stat_delivered += 1;
        }
        delivered
    }

    /// 現在 tick 数。
    pub fn tick(&self) -> u64 { self.tick }

    /// 配送待ちメッセージ数。
    pub fn pending_count(&self) -> usize { self.pending.len() }

    /// LCG ベースの疑似乱数 (0.0〜1.0)。mantissa 53bit 幅に入れる。
    fn next_rand(&mut self) -> f64 {
        self.rng_state = self.rng_state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // top 53bit を使う (f64 mantissa に収まる)
        ((self.rng_state >> 11) as f64) / ((1u64 << 53) as f64)
    }
}

// ─────────────────────────────────────────────────────────────
// Tests: chaos シナリオ × CRDT で convergence 検証
// ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::crdt::{OrSet, GCounter, LwwRegister};

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

    // ─── 追加シナリオ: crash/restart、一方向 partition、majority split、100 peer ───

    #[test]
    fn crashed_peer_drops_all_traffic() {
        // peer 2 を crash、他 peer からの broadcast は peer 2 には届かない
        let mut net: SimNetwork<OrSet<i32>> = SimNetwork::new(3);
        let mut peers: Vec<OrSet<i32>> = (1..=3).map(OrSet::new).collect();
        net.crash(2);

        peers[0].add(10);
        let m = peers[0].clone();
        broadcast(1, m, &mut net, 3);

        for _ in 0..5 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(peers[2].contains(&10), "peer 3 は受け取ってる");
        assert!(!peers[1].contains(&10), "peer 2 は crash で受け取ってない");

        // restore 後は届く
        net.restore(2);
        let m = peers[0].clone();
        broadcast(1, m, &mut net, 3);
        for _ in 0..5 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(peers[1].contains(&10), "restore 後は届く");
    }

    #[test]
    fn peer_crash_loses_state_then_rejoins_with_sync() {
        // peer 3 が crash、state ロスト、restore 後 merge で他 peer から同期
        let mut net: SimNetwork<OrSet<i32>> = SimNetwork::new(5);
        let mut peers: Vec<OrSet<i32>> = (1..=3).map(OrSet::new).collect();

        peers[0].add(1);
        peers[1].add(2);
        peers[2].add(3);
        let m0 = peers[0].clone(); let m1 = peers[1].clone(); let m2 = peers[2].clone();
        broadcast(1, m0, &mut net, 3);
        broadcast(2, m1, &mut net, 3);
        broadcast(3, m2, &mut net, 3);
        for _ in 0..5 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        // この時点で全員 convergent
        assert!(all_converged(&peers));

        // peer 3 crash + state ロスト
        net.crash(3);
        peers[2] = OrSet::new(3);
        assert!(!peers[2].contains(&1));

        // peer 1, 2 はその間も動く
        peers[0].add(100);
        let m0 = peers[0].clone();
        broadcast(1, m0, &mut net, 3);
        for _ in 0..5 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(peers[1].contains(&100));
        assert!(!peers[2].contains(&100), "crash 中は届かない");

        // restore + 全員再 broadcast (bootstrap 相当)
        net.restore(3);
        let m0 = peers[0].clone(); let m1 = peers[1].clone();
        broadcast(1, m0, &mut net, 3);
        broadcast(2, m1, &mut net, 3);
        for _ in 0..10 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(peers[2].contains(&1), "rejoin 後に過去 add を受け取る");
        assert!(peers[2].contains(&100));
        assert!(all_converged(&peers));
    }

    #[test]
    fn one_way_partition_allows_reverse_direction() {
        // peer 1 → peer 2 のみ遮断。peer 2 → peer 1 は通る。
        let mut net: SimNetwork<OrSet<i32>> = SimNetwork::new(11);
        let mut peers: Vec<OrSet<i32>> = (1..=2).map(OrSet::new).collect();
        net.partition_one_way(1, 2);

        // peer 1 からの送信は drop
        peers[0].add(10);
        let m = peers[0].clone();
        broadcast(1, m, &mut net, 2);
        for _ in 0..3 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(!peers[1].contains(&10), "1→2 遮断、届かない");

        // peer 2 からの送信は通る
        peers[1].add(20);
        let m = peers[1].clone();
        broadcast(2, m, &mut net, 2);
        for _ in 0..3 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(peers[0].contains(&20), "2→1 は通る");
    }

    #[test]
    fn majority_minority_split_scenario() {
        // 5 peer を {1,2,3} / {4,5} に分離、write を両側でやる、heal で convergent 復帰
        let mut net: SimNetwork<OrSet<i32>> = SimNetwork::new(17);
        let mut peers: Vec<OrSet<i32>> = (1..=5).map(OrSet::new).collect();

        // partition: {1,2,3} と {4,5} を相互遮断
        for a in [1u32, 2, 3] {
            for b in [4u32, 5] {
                net.partition(a, b);
            }
        }

        // majority: 1,2,3 が add
        peers[0].add(100);
        peers[1].add(200);
        peers[2].add(300);
        // minority: 4,5 が add
        peers[3].add(400);
        peers[4].add(500);

        for _ in 0..10 {
            for i in 0..5 {
                let m = peers[i as usize].clone();
                broadcast((i + 1) as u32, m, &mut net, 5);
            }
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }

        // 分離中は majority / minority が各自 convergent
        let maj: Vec<i32> = peers[0].iter().cloned().collect();
        let min_: Vec<i32> = peers[3].iter().cloned().collect();
        assert!(maj.contains(&100) && maj.contains(&200) && maj.contains(&300));
        assert!(!maj.contains(&400), "分離中、majority は minority の add 見えず");
        assert!(min_.contains(&400) && min_.contains(&500));
        assert!(!min_.contains(&100));

        // heal
        for a in [1u32, 2, 3] {
            for b in [4u32, 5] {
                net.heal(a, b);
            }
        }
        for _ in 0..20 {
            for i in 0..5 {
                let m = peers[i as usize].clone();
                broadcast((i + 1) as u32, m, &mut net, 5);
            }
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(all_converged(&peers));
        let final_set: HashSet<i32> = peers[0].iter().cloned().collect();
        assert_eq!(final_set, [100, 200, 300, 400, 500].iter().cloned().collect());
    }

    #[test]
    fn per_pair_delay_overrides_global() {
        // peer 1→2 のみ遅延 10 tick、他は即配送
        let mut net: SimNetwork<OrSet<i32>> = SimNetwork::new(21);
        net.set_max_delay(0);
        net.set_pair_delay(1, 2, 10);
        let mut peers: Vec<OrSet<i32>> = (1..=3).map(OrSet::new).collect();

        peers[0].add(1);
        let m = peers[0].clone();
        broadcast(1, m, &mut net, 3);

        // 1 tick 後: peer 3 には届いてる、peer 2 はまだ
        for (to, msg) in net.advance() {
            peers[(to - 1) as usize].merge(&msg);
        }
        assert!(peers[2].contains(&1), "peer 3 には即配送");
        assert!(!peers[1].contains(&1), "peer 2 は 10 tick 遅延中");

        // 10 tick 後: peer 2 にも届く
        for _ in 0..10 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        assert!(peers[1].contains(&1));
    }

    #[test]
    fn hundred_peer_convergence_time() {
        // 100 peer を gossip-like に動かして convergence までの tick 数を測る
        let mut net: SimNetwork<OrSet<i32>> = SimNetwork::new(101);
        let n = 100u32;
        let mut peers: Vec<OrSet<i32>> = (1..=n).map(OrSet::new).collect();

        // 各 peer が 1 個 add
        for i in 0..n {
            peers[i as usize].add(i as i32);
        }

        // gossip: 5 tick ごとに全員 broadcast
        let mut converged_at: Option<u64> = None;
        for tick in 1..500 {
            if tick % 5 == 0 {
                for i in 0..n {
                    let m = peers[i as usize].clone();
                    broadcast(i + 1, m, &mut net, n);
                }
            }
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
            if peers.iter().all(|p| p.len() == n as usize) {
                converged_at = Some(tick);
                break;
            }
        }
        let tick = converged_at.expect("should converge within 500 ticks");
        println!("hundred_peer_convergence_time: 100 peer converged in {} ticks (stat_sent={}, stat_delivered={})",
            tick, net.stat_sent, net.stat_delivered);
        assert!(tick < 200);
    }

    #[test]
    fn byzantine_peer_injects_corrupted_lww_damages_other() {
        // 悪意 peer が LwwRegister に HLC=MAX 相当 (timestamp=u64::MAX) で注入
        // merge 後、honest peer の legitimate writes は全部 LWW で上書きできない
        let mut a: LwwRegister<&str> = LwwRegister::new();
        let byzantine: LwwRegister<&str> = {
            let mut b = LwwRegister::new();
            b.set("corrupt", u64::MAX);
            b
        };
        a.set("honest_msg", 100);
        a.merge(&byzantine);
        assert_eq!(a.get(), Some(&"corrupt"));

        // honest の後続 write も上書きできない (LWW の既知限界)
        a.set("another_honest", 200);
        assert_eq!(a.get(), Some(&"corrupt"), "LWW では Byzantine からのロック解除不可");
    }

    #[test]
    fn out_of_order_delivery_preserves_crdt_convergence() {
        // 同じ (from, to) pair でも delay がランダムなので、後で送った msg が先着する
        // ケースがある。OR-Set は順序不変なので convergent のはず。
        let mut net: SimNetwork<OrSet<i32>> = SimNetwork::new(31);
        net.set_max_delay(10); // 0〜10 tick ランダム遅延
        let mut peers: Vec<OrSet<i32>> = (1..=2).map(OrSet::new).collect();

        // peer 1 が連続 3 add、各 tick ごとに broadcast
        for val in 0..3 {
            peers[0].add(val);
            let m = peers[0].clone();
            broadcast(1, m, &mut net, 2);
        }

        // 20 tick 回す (max_delay=10 なので全 msg 届くはず)
        for _ in 0..20 {
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }

        // 配送順が時刻順でないことを確認するため、pending 内部が違う順序で発生してるはず
        // (deliver_at ベース配送で delay ランダムなので前後する)
        // → それでも peer 2 は peer 1 と同じ集合に収束
        assert!(all_converged(&peers));
        assert_eq!(peers[1].len(), 3);
        for v in 0..3 { assert!(peers[1].contains(&v)); }
    }

    #[test]
    fn byzantine_during_partition_does_not_corrupt_honest_majority() {
        // partition で Byzantine peer (4) が minority 側 (peer 5) と分離、独自に noise を撒く。
        // majority {1, 2, 3} は影響受けない。heal 後、Byzantine のデータも一応 union に入る
        // (OR-Set は誰の add でも受け入れる、noise 混じりだが honest data は残る)
        let mut net: SimNetwork<OrSet<i32>> = SimNetwork::new(41);
        let mut peers: Vec<OrSet<i32>> = (1..=5).map(OrSet::new).collect();

        // partition: {1,2,3} と {4,5} を分離、peer 4 が Byzantine
        for a in [1u32, 2, 3] {
            for b in [4u32, 5] {
                net.partition(a, b);
            }
        }

        // honest majority の write
        peers[0].add(100);
        peers[1].add(200);
        peers[2].add(300);

        // Byzantine (peer 4) が大量 noise
        for i in 9000..9050 {
            peers[3].add(i);
        }
        // minority honest (peer 5)
        peers[4].add(500);

        for _ in 0..15 {
            for i in 0..5 {
                let m = peers[i as usize].clone();
                broadcast((i + 1) as u32, m, &mut net, 5);
            }
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }

        // 分離中: majority は byzantine noise を見てない
        let maj: HashSet<i32> = peers[0].iter().cloned().collect();
        assert!(maj.contains(&100));
        assert!(!maj.contains(&9000));
        assert_eq!(maj.len(), 3, "majority は 3 つの honest data だけ");

        // heal
        for a in [1u32, 2, 3] {
            for b in [4u32, 5] {
                net.heal(a, b);
            }
        }
        for _ in 0..25 {
            for i in 0..5 {
                let m = peers[i as usize].clone();
                broadcast((i + 1) as u32, m, &mut net, 5);
            }
            for (to, msg) in net.advance() {
                peers[(to - 1) as usize].merge(&msg);
            }
        }
        // heal 後: 全員 convergent、honest data は全部残る、byzantine noise も union で入ってる
        // (OR-Set は signature 層と分離なので noise を CRDT 層だけじゃ弾けない、上位で検出要)
        assert!(all_converged(&peers));
        let fin: HashSet<i32> = peers[0].iter().cloned().collect();
        assert!(fin.contains(&100));
        assert!(fin.contains(&200));
        assert!(fin.contains(&300));
        assert!(fin.contains(&500));
        // noise も残る (CRDT は素朴、honest data を消せないのがポイント)
        assert!(fin.contains(&9000));
        // 要点: honest data は **絶対に消えない**、noise が混じっても
    }

    #[test]
    fn byzantine_peer_on_orset_cannot_suppress_others() {
        // OR-Set では Byzantine が無限 tag を注入しても他 peer の add を消せない
        let mut honest: OrSet<&str> = OrSet::new(1);
        let mut byzantine: OrSet<&str> = OrSet::new(99);

        // Byzantine が 1000 件の noise を add
        for i in 0..1000 {
            byzantine.add(Box::leak(format!("noise_{}", i).into_boxed_str()));
        }
        honest.add("real_data");

        honest.merge(&byzantine);
        assert!(honest.contains(&"real_data"),
            "Byzantine の noise 下でも honest data は残る (OR-Set の強み)");
    }
}
