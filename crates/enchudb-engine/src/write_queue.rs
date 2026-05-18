//! WriteQueue — 並行書き込み用 bounded MPSC キュー (issue4 対応)。
//!
//! 複数 writer スレッドが `push` でオペレーションを投げ、 単一 consumer
//! スレッドが `pop` で取り出して HimoStore に適用する。
//!
//! v0.2.3 以前は `crossbeam_queue::SegQueue` (unbounded) を使っていた。
//! consumer が drain しきれない rate で writer が push し続けると、 queue
//! 内 record が線形成長 → RSS 線形成長 → OOM kill (sunsu Docker scenario
//! 03 で 14s / 8M posts / 3.38 GB → 4 GB cap 突破)。
//!
//! v0.2.4 から `crossbeam_queue::ArrayQueue` (bounded、 lock-free) に変更。
//! push が満杯なら `yield_now` ループで consumer の進捗を待つ (自然な
//! backpressure)。 capacity は `Engine::create_concurrent_with_wal_queue_cap`
//! で指定可能 (default は緩めの値で latency への影響を抑える)。

use crossbeam_queue::ArrayQueue;

/// queue capacity の default。 既存 caller 互換のため、 sustained writer の
/// peak rate を捌ける程度の余裕を持たせる。 SNS 系 hot path (sunsu の
/// concurrent_posts 等、 1 M posts/sec 級) でも、 consumer 進捗との rate 差が
/// 数百 ms 程度なら飲み込める。
pub(crate) const DEFAULT_WRITE_QUEUE_CAP: usize = 1_048_576; // 1 M ops

/// 非同期オペレーション。
#[derive(Clone, Debug)]
pub enum Op {
    /// 値の紐づけ。himo_id は事前に解決済み。
    Tie { eid: u32, himo_id: u16, value: u32 },
    /// 紐を外す。
    Untie { eid: u32, himo_id: u16 },
    /// entity ごと削除。
    Delete { eid: u32 },
    /// 非索引コンテンツ。WAL に載せるため owned 型で保持。
    Content { eid: u32, key: Box<str>, data: Box<[u8]> },
    /// entity 作成 marker。 writer thread の `entity()` は slot allocation のみで戻り、
    /// この op を consumer 経由で空回しさせて `flush_writes` の barrier counter
    /// (`push_count` / `apply_count`) と整合を取る (issue5)。 payload は無し、
    /// `apply_op` 側で counter increment のみが起こる。
    EntityCreated { local: u32 },
}

pub struct WriteQueue {
    queue: ArrayQueue<Op>,
}

impl WriteQueue {
    /// default capacity (= 1 M ops) で生成。
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_WRITE_QUEUE_CAP)
    }

    /// capacity 指定。 0 は無効 (panic)。
    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap > 0, "WriteQueue capacity must be > 0");
        Self { queue: ArrayQueue::new(cap) }
    }

    /// blocking push。 queue が満杯なら consumer の進捗を yield ループで待つ。
    /// producer のここでの待ち時間が backpressure として throughput を
    /// consumer 上限に張り付かせる。
    #[inline]
    pub fn push(&self, op: Op) {
        let mut op = op;
        loop {
            match self.queue.push(op) {
                Ok(()) => return,
                Err(returned) => {
                    op = returned;
                    std::thread::yield_now();
                }
            }
        }
    }

    /// pop。空なら None。
    #[inline]
    pub fn pop(&self) -> Option<Op> {
        self.queue.pop()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// queue の cap (= 受け入れ可能な最大 pending op 数)。
    #[inline]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }
}

impl Default for WriteQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_basic() {
        let q = WriteQueue::new();
        assert!(q.is_empty());
        q.push(Op::Tie { eid: 1, himo_id: 0, value: 10 });
        q.push(Op::Untie { eid: 2, himo_id: 1 });
        assert_eq!(q.len(), 2);

        match q.pop() {
            Some(Op::Tie { eid, himo_id, value }) => {
                assert_eq!(eid, 1);
                assert_eq!(himo_id, 0);
                assert_eq!(value, 10);
            }
            _ => panic!("expected Tie"),
        }
        match q.pop() {
            Some(Op::Untie { eid, himo_id }) => {
                assert_eq!(eid, 2);
                assert_eq!(himo_id, 1);
            }
            _ => panic!("expected Untie"),
        }
        assert!(q.pop().is_none());
    }

    #[test]
    fn capacity_default_accepts_many() {
        let q = WriteQueue::new();
        // default 1 M cap、 そこまでは block せず push できる
        for i in 0..100_000u32 {
            q.push(Op::Delete { eid: i });
        }
        assert_eq!(q.len(), 100_000);
        for i in 0..100_000u32 {
            match q.pop() {
                Some(Op::Delete { eid }) => assert_eq!(eid, i),
                _ => panic!("expected Delete({i})"),
            }
        }
        assert!(q.is_empty());
    }

    #[test]
    fn bounded_push_blocks_until_drained() {
        // cap=4 の queue を満杯にして、 別 thread が drain したら push 成功する事を確認
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let q = Arc::new(WriteQueue::with_capacity(4));
        for i in 0..4 {
            q.push(Op::Delete { eid: i });
        }
        assert_eq!(q.len(), 4);
        // drainer thread を起動して 50 ms 後に 1 個 pop
        let q2 = q.clone();
        let drained = Arc::new(AtomicBool::new(false));
        let drained2 = drained.clone();
        let h = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let _ = q2.pop();
            drained2.store(true, Ordering::Release);
        });
        // この push は block するはず、 drainer が動くまで返らない
        q.push(Op::Delete { eid: 99 });
        assert!(drained.load(Ordering::Acquire), "push should have waited for drainer");
        h.join().unwrap();
    }
}
