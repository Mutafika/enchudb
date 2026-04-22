//! Sync — peer 間で WAL レコードを pull して本体に LWW apply する。
//!
//! # 使い方
//!
//! ```ignore
//! use enchudb::{Engine, sync::Syncer, transport::InMemoryTransport};
//!
//! let transport = InMemoryTransport::new();
//! let syncer = Syncer::new(eng_a.clone(), transport.clone());
//!
//! // 他 peer の ops を pull して apply
//! let applied = syncer.pull_once(peer_b_id);
//! println!("applied {} ops from peer {}", applied, peer_b_id);
//! ```
//!
//! # LWW 規則
//!
//! 受信 op を `HlcStore` の既存 HLC と比較して:
//!
//! - `(eid, himo)` ペアの既存 HLC より受信 HLC が**厳密に大きい** → apply
//! - それ以外(等しい、または既存が大きい) → skip
//!
//! Delete は特殊: entity ごとすべての himo HLC を比較せず、
//! 「Delete の HLC が、その entity の全 himo 最大 HLC 以上」なら apply。

use std::sync::Arc;

use crate::engine::Engine;
use crate::hlc_store::HlcStore;
use crate::transport::{Transport, WireRecord};
use crate::wal::DecodedOp;
use crate::{Hlc, PeerId};

pub struct Syncer {
    engine: Arc<Engine>,
    transport: Arc<dyn Transport>,
    /// 各 peer から最後に pull した地点(HLC)。次回はここより後だけ取る。
    last_pulled: std::sync::Mutex<std::collections::HashMap<PeerId, Hlc>>,
    /// Phase C: true なら署名検証を強制。未署名 or 検証失敗 op は reject。
    require_signature: std::sync::atomic::AtomicBool,
}

/// 1 回の pull-apply サイクルの結果。
#[derive(Debug, Clone, Default)]
pub struct SyncOutcome {
    /// 受信総数。
    pub received: usize,
    /// LWW で新規/上書きされた op 数。
    pub applied: usize,
    /// LWW で古いと判定して skip した op 数。
    pub skipped: usize,
    /// Phase C: 署名検証で reject した op 数。
    pub rejected_signature: usize,
    /// Phase C: ACL で reject した op 数。
    pub rejected_acl: usize,
}

impl Syncer {
    pub fn new(engine: Arc<Engine>, transport: Arc<dyn Transport>) -> Self {
        // Syncer が attach された engine の WAL は auto_reset を off にする。
        // publish_since は iter_committed で WAL を読むので、consumer が
        // try_reset で WAL を空にすると sync 記録が消える race がある。
        // 正式には「全 peer が replicate 済みの地点まで reset」する watermark が要るが、
        // 未実装なので一旦 auto_reset off で記録を残す方針。
        if let Some(wal) = engine.wal_arc() {
            wal.set_auto_reset(false);
        }
        Self {
            engine,
            transport,
            last_pulled: std::sync::Mutex::new(std::collections::HashMap::new()),
            require_signature: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Phase C: 署名検証を必須にする。未署名 or 検証失敗 op は reject される。
    pub fn set_require_signature(&self, on: bool) {
        self.require_signature.store(on, std::sync::atomic::Ordering::Release);
    }

    /// 指定 peer から未取得レコードを 1 回 pull して本体に apply。
    pub fn pull_once(&self, from: PeerId) -> SyncOutcome {
        let since = {
            let guard = self.last_pulled.lock().unwrap();
            guard.get(&from).copied().unwrap_or(Hlc::ZERO)
        };
        let records = self.transport.pull(from, since);
        let outcome = self.apply_records(&records);

        // last_pulled を進める(空 pull でも既存のままで OK)
        if let Some(last) = records.iter().map(|r| r.hlc).max() {
            let mut guard = self.last_pulled.lock().unwrap();
            let cur = guard.get(&from).copied().unwrap_or(Hlc::ZERO);
            if last > cur {
                guard.insert(from, last);
            }
        }

        outcome
    }

    /// 自 peer の commit 済み ops を transport に publish。
    /// `iter_committed` は checkpoint を無視して WAL 全体を列挙するので、
    /// 既に本体に apply 済みでも WAL ring buffer 内にあれば拾える。
    /// 戻り値は publish したレコード数。
    pub fn publish_since(&self, since: Hlc) -> usize {
        let wal = match self.engine.wal_arc() {
            Some(w) => w,
            None => return 0,
        };
        let recs = wal.iter_committed();
        let peer = self.engine.peer_id();
        let filtered: Vec<WireRecord> = recs
            .into_iter()
            .filter(|r| r.hlc > since)
            .map(|r| r.into())
            .collect();
        let count = filtered.len();
        self.transport.publish(peer, filtered);
        count
    }

    /// 受信レコードを LWW で apply する。Phase C: 署名検証 + ACL も通す。
    fn apply_records(&self, records: &[WireRecord]) -> SyncOutcome {
        let mut out = SyncOutcome::default();
        let store = self.engine.hlc_store().clone();
        let require_sig = self.require_signature.load(std::sync::atomic::Ordering::Acquire);
        let pubkeys = self.engine.pubkeys().clone();
        let acl = self.engine.acl().clone();
        for rec in records {
            out.received += 1;

            // ACL チェック(未定義なら全員通す)
            if !acl.is_writer(rec.author_peer) {
                out.rejected_acl += 1;
                continue;
            }

            if require_sig {
                if rec.signature == [0u8; 64] {
                    out.rejected_signature += 1;
                    continue;
                }
                if pubkeys.get(rec.author_peer).is_none() {
                    out.rejected_signature += 1;
                    continue;
                }
                if !pubkeys.verify(rec.author_peer, &rec.signed_bytes, &rec.signature) {
                    out.rejected_signature += 1;
                    continue;
                }
            }
            if self.apply_one(&store, rec) {
                out.applied += 1;
            } else {
                out.skipped += 1;
            }
        }
        out
    }

    fn apply_one(&self, store: &HlcStore, rec: &WireRecord) -> bool {
        match &rec.op {
            DecodedOp::Tie { eid, himo_id, value } => {
                if !store.try_set(*eid, *himo_id, rec.hlc) {
                    return false;
                }
                self.engine.remote_tie_apply(*eid, *himo_id, *value);
                true
            }
            DecodedOp::Untie { eid, himo_id } => {
                if !store.try_set(*eid, *himo_id, rec.hlc) {
                    return false;
                }
                self.engine.remote_untie_apply(*eid, *himo_id);
                true
            }
            DecodedOp::Delete { eid } => {
                // Delete は全 himo に波及。sentinel himo_id = 0xFFFF で HLC を記録。
                if !store.try_set(*eid, u16::MAX, rec.hlc) {
                    return false;
                }
                store.remove_entity(*eid);
                self.engine.remote_delete_apply(*eid);
                true
            }
            DecodedOp::Content { eid, key, data } => {
                // Content は key 単位で LWW。himo_id を使えないので hash で代用。
                let key_hash = fnv_hash_u16(key);
                if !store.try_set(*eid, key_hash | 0x8000, rec.hlc) {
                    return false;
                }
                self.engine.remote_content_apply(*eid, key, data);
                true
            }
            DecodedOp::Commit => true, // boundary marker、apply は不要
        }
    }
}

/// Content key 用の簡易 hash → 15bit。MSB は LWW key namespace で占有。
fn fnv_hash_u16(s: &str) -> u16 {
    let mut h: u32 = 0x811c9dc5;
    for &b in s.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    (h as u16) & 0x7fff
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HimoType, PeerId};
    use crate::transport::InMemoryTransport;

    fn new_eng(path: &str, peer: PeerId) -> Arc<Engine> {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.wal", path));
        let _ = std::fs::remove_file(format!("{}.crc", path));
        let mut eng = Engine::create(path).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        let eng = Arc::new(eng);
        eng.set_peer_id(peer);
        eng
    }

    #[test]
    fn lww_newer_wins() {
        let path_a = "/tmp/enchudb_sync_a.db";
        let eng_a = new_eng(path_a, 1);
        let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_a.clone(), transport.clone());

        // peer 2 からの古い op
        let eid = crate::make_eid(2, 7);
        let rec_old = WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid, himo_id: 0, value: 10 });
        let rec_new = WireRecord::unsigned(Hlc { wall: 200, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid, himo_id: 0, value: 20 });
        let out = syncer.apply_records(&[rec_new.clone(), rec_old.clone()]);
        assert_eq!(out.applied, 1);
        assert_eq!(out.skipped, 1);

        // より古い HLC で再送しても skip
        let out2 = syncer.apply_records(&[rec_old.clone()]);
        assert_eq!(out2.applied, 0);
        assert_eq!(out2.skipped, 1);

        let _ = std::fs::remove_file(path_a);
    }

    #[test]
    fn two_peer_pull_and_apply() {
        let path_a = "/tmp/enchudb_sync_2peer_a.db";
        let eng_a = new_eng(path_a, 1);
        let transport = Arc::new(InMemoryTransport::new());

        // peer 2 が tie した体で transport に直接 publish
        let eid_b = crate::make_eid(2, 3);
        transport.publish(2, vec![
            WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid: eid_b, himo_id: 0, value: 42 }),
        ]);

        let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
        let out = syncer.pull_once(2);
        assert_eq!(out.applied, 1);

        // peer A 側から peer 2 の eid を get できる(local=3 で引く)
        let v = eng_a.get(eid_b, "val");
        assert_eq!(v, Some(42));

        let _ = std::fs::remove_file(path_a);
    }

    #[test]
    fn pull_incremental_advances_cursor() {
        let path_a = "/tmp/enchudb_sync_cursor_a.db";
        let eng_a = new_eng(path_a, 1);
        let transport = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);

        // 1st round
        transport.publish(2, vec![WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid: crate::make_eid(2, 1), himo_id: 0, value: 10 })]);
        let out1 = syncer.pull_once(2);
        assert_eq!(out1.received, 1);

        // 2nd pull should see only new records
        transport.publish(2, vec![WireRecord::unsigned(Hlc { wall: 200, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid: crate::make_eid(2, 2), himo_id: 0, value: 20 })]);
        let out2 = syncer.pull_once(2);
        assert_eq!(out2.received, 1);
        assert_eq!(out2.applied, 1);

        let _ = std::fs::remove_file(path_a);
    }
}
