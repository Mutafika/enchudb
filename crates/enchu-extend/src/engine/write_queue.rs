//! WriteQueue — v27 並行書き込み用の lock-free キュー。
//!
//! 複数 writer スレッドが `push` でオペレーションを投げ、
//! 単一 consumer スレッドが `pop` で取り出して HimoStore に適用する。
//!
//! SegQueue(crossbeam-queue の unbounded lock-free queue) を採用しているので、
//! push は常に成功する。back-pressure もない。

use crossbeam_queue::SegQueue;

/// 非同期オペレーション。
#[derive(Clone, Copy, Debug)]
pub enum Op {
    /// 値の紐づけ。himo_id は事前に解決済み。
    Tie { eid: u32, himo_id: u16, value: u32 },
    /// 紐を外す。
    Untie { eid: u32, himo_id: u16 },
    /// entity ごと削除。
    Delete { eid: u32 },
}

pub struct WriteQueue {
    queue: SegQueue<Op>,
}

impl WriteQueue {
    pub fn new() -> Self {
        Self { queue: SegQueue::new() }
    }

    /// push。SegQueue は unbounded なので常に成功する。
    #[inline]
    pub fn push(&self, op: Op) {
        self.queue.push(op);
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
    fn unbounded_push_accepts_many() {
        let q = WriteQueue::new();
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
}
