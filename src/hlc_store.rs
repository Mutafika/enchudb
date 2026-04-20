//! HlcStore — (eid, himo_id) ごとの「最後に書いた HLC」を保持する。
//!
//! # 用途
//!
//! LWW(Last-Write-Wins)衝突解決のために必要。peer A と peer B が同じ
//! (eid, himo) に違う値を書いたとき、HLC が大きい方を採用する。
//!
//! 受信 op を apply する前に、HlcStore の現在 HLC と比較:
//! - 既存なし、または既存 < 受信 HLC → apply、HlcStore を受信 HLC に更新
//! - 既存 >= 受信 HLC → skip(受信 op は古い)
//!
//! # 永続化
//!
//! v32 Phase B では in-memory HashMap のみ。再起動後は最初の sync で再構築する。
//! Phase D で mmap region として永続化予定。
//!
//! # メモリ
//!
//! (eid: u64, himo_id: u16) → Hlc(16B) のエントリ。100万 (eid, himo) 組で ~40MB。
//! 紐ごとに RwLock を分ければ concurrent apply も並列化できるが、Phase B は単一 Mutex。

use std::collections::HashMap;
use std::sync::RwLock;

use crate::Hlc;

type Key = (u64, u16);

/// LWW 用の HLC ストア。
pub struct HlcStore {
    inner: RwLock<HashMap<Key, Hlc>>,
}

impl Default for HlcStore {
    fn default() -> Self {
        Self::new()
    }
}

impl HlcStore {
    pub fn new() -> Self {
        Self { inner: RwLock::new(HashMap::new()) }
    }

    /// 現在の HLC を取得。未記録なら None。
    pub fn get(&self, eid: u64, himo_id: u16) -> Option<Hlc> {
        let guard = self.inner.read().unwrap();
        guard.get(&(eid, himo_id)).copied()
    }

    /// 新しい HLC を記録。
    /// 既存が新規より大きければ何もしない(monotonic)。
    /// 「更新した (=新規 HLC が採用された)」場合のみ true。
    pub fn try_set(&self, eid: u64, himo_id: u16, hlc: Hlc) -> bool {
        let mut guard = self.inner.write().unwrap();
        let key = (eid, himo_id);
        match guard.get(&key).copied() {
            Some(existing) if existing >= hlc => false,
            _ => {
                guard.insert(key, hlc);
                true
            }
        }
    }

    /// 強制上書き(recovery 用)。
    pub fn force_set(&self, eid: u64, himo_id: u16, hlc: Hlc) {
        let mut guard = self.inner.write().unwrap();
        guard.insert((eid, himo_id), hlc);
    }

    /// エントリ削除(Delete op 用)。
    pub fn remove(&self, eid: u64, himo_id: u16) {
        let mut guard = self.inner.write().unwrap();
        guard.remove(&(eid, himo_id));
    }

    /// eid の全 himo エントリを削除(Delete op で entity ごと消える場合)。
    pub fn remove_entity(&self, eid: u64) {
        let mut guard = self.inner.write().unwrap();
        guard.retain(|(e, _), _| *e != eid);
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(wall: u64, logical: u32, peer: u32) -> Hlc {
        Hlc { wall, logical, peer }
    }

    #[test]
    fn try_set_first_time_succeeds() {
        let s = HlcStore::new();
        assert!(s.try_set(1, 0, hlc(100, 0, 0)));
        assert_eq!(s.get(1, 0), Some(hlc(100, 0, 0)));
    }

    #[test]
    fn try_set_rejects_older_hlc() {
        let s = HlcStore::new();
        s.try_set(1, 0, hlc(200, 0, 0));
        // 古い HLC は reject される
        assert!(!s.try_set(1, 0, hlc(100, 0, 0)));
        assert_eq!(s.get(1, 0), Some(hlc(200, 0, 0)));
    }

    #[test]
    fn try_set_accepts_newer_hlc() {
        let s = HlcStore::new();
        s.try_set(1, 0, hlc(100, 0, 0));
        assert!(s.try_set(1, 0, hlc(200, 0, 0)));
        assert_eq!(s.get(1, 0), Some(hlc(200, 0, 0)));
    }

    #[test]
    fn hlc_ordering_breaks_ties_by_peer() {
        let s = HlcStore::new();
        // 同じ wall/logical、peer だけ違う → peer 大きい方が勝つ
        s.try_set(1, 0, hlc(100, 0, 1));
        assert!(s.try_set(1, 0, hlc(100, 0, 2))); // peer 2 > peer 1
        assert!(!s.try_set(1, 0, hlc(100, 0, 1))); // peer 1 < peer 2
    }

    #[test]
    fn remove_entity_drops_all_himos() {
        let s = HlcStore::new();
        s.try_set(1, 0, hlc(100, 0, 0));
        s.try_set(1, 5, hlc(100, 0, 0));
        s.try_set(1, 9, hlc(100, 0, 0));
        s.try_set(2, 0, hlc(100, 0, 0));
        assert_eq!(s.len(), 4);
        s.remove_entity(1);
        assert_eq!(s.len(), 1);
        assert!(s.get(1, 0).is_none());
        assert_eq!(s.get(2, 0), Some(hlc(100, 0, 0)));
    }
}
