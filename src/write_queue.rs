//! WriteQueue — v27 並行書き込み用の lock-free キュー。
//!
//! 複数 writer スレッドが `push` でオペレーションを投げ、
//! 単一 consumer スレッドが `pop` で取り出して HimoStore に適用する。
//!
//! 満杯なら push は Err(op) を返す(back-pressure)。
//! 呼び出し側(Engine::tie_async) は spin + yield でリトライする。

use crossbeam_queue::ArrayQueue;

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
    queue: ArrayQueue<Op>,
}

impl WriteQueue {
    pub fn new(cap: usize) -> Self {
        Self { queue: ArrayQueue::new(cap.max(1)) }
    }

    /// push。満杯なら Err(op) を返す。
    #[inline]
    pub fn push(&self, op: Op) -> Result<(), Op> {
        self.queue.push(op)
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

    #[inline]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_basic() {
        let q = WriteQueue::new(4);
        assert!(q.is_empty());
        q.push(Op::Tie { eid: 1, himo_id: 0, value: 10 }).unwrap();
        q.push(Op::Untie { eid: 2, himo_id: 1 }).unwrap();
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
    fn push_full_returns_err() {
        let q = WriteQueue::new(2);
        q.push(Op::Delete { eid: 1 }).unwrap();
        q.push(Op::Delete { eid: 2 }).unwrap();
        assert!(q.push(Op::Delete { eid: 3 }).is_err());
    }
}
