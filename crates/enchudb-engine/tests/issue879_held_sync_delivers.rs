//! #879: changefeed の記録 (`changefeed.seg`) のある DB で、 #857 の止め (消した行が `tables` の sidecar より先にある間 checkpoint を
//! 進めない) が掛かった `oplog_sync` も、 返る時に新しい record を listener に配る。
//!
//! 前は記録へ写す上限が checkpoint で、 止めた `oplog_sync` は何も配らず、 次の consumer の周期 (`tables` を書いて止めを解く) まで
//! 待った (`tables` を書けない間は配らないまま)。 今は書き出しの返った位置まで写す。

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{Engine, ValueType};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Recorder(Mutex<Vec<u64>>);

impl ChangeListener for Recorder {
    fn on_changes(&self, recs: &[WireRecord]) {
        let mut seen = self.0.lock().unwrap();
        for r in recs {
            if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                seen.push(value);
            }
        }
    }
}

#[test]
fn a_held_oplog_sync_delivers_new_records() {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let path = std::env::temp_dir().join(format!("enchudb-issue879-{}-{nanos}", std::process::id()));
    let mut eng = Engine::create_growable_with_capacity(path.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let a = eng.himo_id("t.a").unwrap() as u16;
    eng.enable_change_log(1 << 20).unwrap();
    let rec = Arc::new(Recorder::default());
    eng.add_change_listener(rec.clone());
    // consumer の周期は `tables` を書いて止めを解くので止める (配るのが `oplog_sync` だけの形)
    let hold = eng.hold_consumer();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 1u32);
    eng.oplog_sync().unwrap();
    // x は sidecar より先 (足した後に `tables` を書いていない) なので、 消すと止めが掛かる
    eng.delete(x);
    eng.oplog_sync().unwrap();
    let y = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(y, a, 9u32);
    eng.oplog_sync().unwrap();
    let wal = eng.oplog().unwrap().clone();
    let held = wal.checkpoint() != wal.written_head();
    let seen = rec.0.lock().unwrap().clone();
    drop(hold);
    drop((wal, eng));
    let _ = std::fs::remove_dir_all(&path);
    assert!(held, "前提: `oplog_sync` が checkpoint を止めている");
    assert!(seen.contains(&9), "止めた `oplog_sync` が返った時に、 新しい record が listener に届いていない (届いた値 {seen:?})");
}
