//! ChangeListener — WAL に durable 化された record を listener に push する。
//!
//! # 動機
//!
//! peer 間 sync を pull polling 主体でやると RTT + interval の遅延が乗る。
//! 「engine が record を確定 → 自動で listener にイベント発火」できれば、
//! WS push hub / metrics / external replication 等を **manual broadcast 呼び出しなし**で書ける。
//!
//! # 発火タイミング
//!
//! consumer スレッドが背景 fsync を成功させた直後に、新しく durable 化した
//! WAL レコード群を listener に渡す。
//!
//! - **順序保証**: HLC 昇順、WAL append の順序とそのまま一致
//! - **at-least-once**: 重複は無いが、listener 側で再起動跨ぎを担保したいなら
//!   HLC を控えて resume する設計にすること
//! - **durability 後**: 渡された record はディスクに fsync 済み。
//!   listener crash → restart で `audit(AuditFilter { from_hlc: ..., .. })` で
//!   resume できる
//!
//! # 注意
//!
//! listener.on_changes は consumer スレッドから直接呼ばれる。長時間ブロックすると
//! 後続の WAL 書き込みも詰まる。重い処理は別スレッドに forward すること。
//!
//! # 使い方
//!
//! ```
//! use std::sync::{Arc, Mutex};
//! use enchudb_engine::{Engine, HimoType};
//! use enchudb_engine::changefeed::ChangeListener;
//! use enchudb_engine::transport::WireRecord;
//!
//! struct CountSink(Arc<Mutex<usize>>);
//! impl ChangeListener for CountSink {
//!     fn on_changes(&self, records: &[WireRecord]) {
//!         *self.0.lock().unwrap() += records.len();
//!     }
//! }
//!
//! let path = format!("/tmp/enchudb-cf-doc-{}.db", std::process::id());
//! # let _ = std::fs::remove_file(&path);
//! # let _ = std::fs::remove_file(format!("{}.wal", path));
//! {
//!     let mut eng = Engine::create_standalone(&path).unwrap();
//!     eng.define_himo("v", HimoType::Value, 100);
//!     eng.flush().unwrap();
//! }
//! let eng = Engine::open_concurrent_with_wal(&path, 4 * 1024 * 1024).unwrap();
//! let counter = Arc::new(Mutex::new(0usize));
//! eng.add_change_listener(Arc::new(CountSink(counter.clone())));
//!
//! let e = eng.entity();
//! eng.tie_async(e, "v", 1);
//! eng.wal_commit();
//! eng.flush_writes();
//! eng.wal_sync().unwrap();
//! // この時点で counter には少なくとも 1 件入る
//! assert!(*counter.lock().unwrap() >= 1);
//! drop(eng);
//! # let _ = std::fs::remove_file(&path);
//! # let _ = std::fs::remove_file(format!("{}.wal", path));
//! ```

use crate::transport::WireRecord;

/// WAL に durable 化された record を受け取る listener。
///
/// `on_changes` は consumer スレッドから直接呼ばれるので、長時間ブロックしないこと。
/// 重い処理は別スレッドに forward する設計を推奨。
pub trait ChangeListener: Send + Sync {
    /// 新たに durable 化したレコード群。HLC 昇順。
    fn on_changes(&self, records: &[WireRecord]);
}
