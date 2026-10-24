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
//! WAL の書き出し係 (consumer スレッド) が背景 fsync を成功させた後に、 通知係のスレッド (`enchudb-notifier`、
//! listener を登録した WAL 付きの並行の engine にだけ居る) が、 新しく durable 化した WAL レコード群を listener に渡す。
//!
//! - **順序**: WAL に積んだ順。 自分の書き込みだけなら HLC 昇順。 中継した record (別の author の record を自分の WAL に
//!   積む) が混ざると HLC の順にはならない
//! - **重複・欠け**: 1 つの process の中で同じ record を 2 度は渡さない。 WAL が満杯で載らなかった write (#57) は渡らない
//!   (sync の DB は相手に floor で知らせて取り直させる。 listener には知らせない)。 再起動を跨いで漏れなく受けたいなら、
//!   渡された最後の HLC を控えて `audit` で resume する (再起動の前後で同じ record をもう一度受けうる = at-least-once)
//! - **durability 後**: 渡された record はディスクに fsync 済み。
//!   listener crash → restart で `audit(AuditFilter { from_hlc: ..., .. })` で
//!   resume できる
//!
//! # 注意
//!
//! listener.on_changes は通知係のスレッド (と、 `oplog_sync()` を呼んだスレッド) から呼ばれる。 遅くても WAL の書き出しと
//! 畳みは止まらない: まだ渡していない record は、 ring を畳む時にメモリの溜めへ移し、 溜めから先に順に 1 回ずつ渡す
//! ([`crate::engine::Engine::change_backlog_len`] で数が見える)。 0.34 までは consumer が直接呼んでいたので、 遅い・止まった
//! listener の間は WAL を畳めず、 満杯になって書き込みが待たされたり落ちたりした。 溜めには上限がある (#560、
//! [`crate::engine::Engine::set_change_backlog_limit`]、 既定 64 MiB): 畳む時に移す分で越えたら、 溜めを捨てて listener を全部
//! 外し ([`crate::engine::Engine::change_listeners_detached`] で数える)、 [`ChangeListener::on_detached`] で知らせる。 書き手は
//! 止めない。 外れた listener に渡らなかった record は WAL から畳まれているので、 続けて受けるなら今の状態から読み直して付け直す。
//! 重い処理は別スレッドに forward すること。
//!
//! 配信は 1 本ずつ (#337): `oplog_sync()` は、 他の thread が配っている最中ならそれを待ってから自分の commit を配り、
//! 配り終えてから返る。 ただし待つのは 5 秒まで (#560): 配っている listener がそれより長く返らなければ、 配らずに返る
//! (残りは通知係が配る)。 Drop は通知係が残りを配り終えるのを待つ。 ただし配っている listener が 5 秒の間 1 度も返らなければ待たずに
//! 閉じ (#636、 前は永久に止まる listener なら Drop も返らなかった)、 残りはその listener が戻った後に通知係が配る (記録がある時は
//! 記録に残り、 開き直した後に続きから受ける)。 listener の中で engine の最後の参照を落としてもよい (その Drop は通知係を待たない)。
//! listener の中から `oplog_sync()` を呼んでもよい (その呼び出しは配らずに返る、 残りは
//! 呼び出し元の配信か consumer が配る)。 listener の中で **別の thread の `oplog_sync()` を待たない** こと
//! (その thread はこちらの配信が終わるのを待っているので、 互いに待って止まる)。 WAL は配り終えるのを待たずに畳む
//! (渡していない分は溜めへ移す)。
//!
//! # 記録 (開き直しを越えて続きから受ける)
//!
//! [`crate::engine::Engine::enable_change_log`] で `{dir}/changefeed.seg` を作ると、 listener に渡す record を番号付きで disk に
//! 残す (以後は開くたびに使う)。 番号は [`ChangeListener::on_changes_at`] で渡る。 処理と同じ commit で 「次の番号」 を DB に
//! 書いておき、 開き直した後に [`crate::engine::Engine::add_change_listener_from`] に渡すと、 落ちる前に処理した所の続きから
//! 受け直す。 記録が捨てた番号 (記録の大きさを越えて遅れた) は `Err(Truncated { from })` = 今の状態から読み直す。
//!
//! - 写すのは書き出した record だけ (WAL の checkpoint まで)。 番号は落ちても同じ record に同じ番号
//! - WAL を畳む前に記録へ写す (listener が居ない間に畳んだ record も残る)。 記録がある間はメモリの溜めを使わず、 記録が
//!   listener の位置を越えて捨てたら外して `on_detached` で知らせる
//! - 記録の 1/4 より大きい record は番号だけ使って中身を残さない (その番号を読む listener は外れる)
//!
//! # 使い方
//!
//! ```
//! use std::sync::{Arc, Mutex};
//! use enchudb_engine::{Engine, ValueType};
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
//! # let _ = enchudb_engine::db_files::remove_db(&path);
//! {
//!     let mut eng = Engine::create_standalone(&path).unwrap();
//!     eng.define_himo("v", ValueType::Number, 100);
//!     eng.flush().unwrap();
//! }
//! let eng = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
//! let counter = Arc::new(Mutex::new(0usize));
//! eng.add_change_listener(Arc::new(CountSink(counter.clone())));
//!
//! let e = eng.entity().unwrap();
//! eng.tie_async(e, "v", 1);
//! eng.oplog_commit();
//! eng.flush_writes();
//! eng.oplog_sync().unwrap();
//! // この時点で counter には少なくとも 1 件入る
//! assert!(*counter.lock().unwrap() >= 1);
//! drop(eng);
//! # let _ = enchudb_engine::db_files::remove_db(&path);
//! ```

use crate::transport::WireRecord;

/// WAL に durable 化された record を受け取る listener。
///
/// `on_changes` は通知係のスレッド (と `oplog_sync()` を呼んだスレッド) から呼ばれる。 遅くても書き込みは止まらないが、
/// 渡していない record はメモリに溜まるので、 重い処理は別スレッドに forward する設計を推奨。
pub trait ChangeListener: Send + Sync {
    /// 新たに durable 化したレコード群。HLC 昇順。 既定は何もしない ([`Self::on_changes_at`] だけを実装してもよい)。
    fn on_changes(&self, records: &[WireRecord]) {
        let _ = records;
    }

    /// [`Self::on_changes`] に、 `records[0]` の番号 (`first`、 続く record は 1 つずつ増える) を添えたもの。 engine が呼ぶのは
    /// こちらで、 既定は `on_changes` を呼ぶ。 changefeed の記録 (`Engine::enable_change_log`) がある時の番号は開き直しても続き、
    /// 処理した最後の番号 + 1 を `Engine::add_change_listener_from` に渡すと、 その続きから受け直せる (処理と同じ commit で番号を
    /// DB に書いておけば、 落ちても処理した所の続きから)。 記録の無い時の番号は process の中だけ (開き直すと 0 から)。
    fn on_changes_at(&self, first: u64, records: &[WireRecord]) {
        let _ = first;
        self.on_changes(records);
    }

    /// #560: 渡していない record が溜めの上限 (`Engine::set_change_backlog_limit`、 既定 64 MiB) を越えたので外された。
    /// `lost_from` (渡せなかった最初の record の HLC) から先は、 この listener には渡らない。 WAL はもう畳んでいるので
    /// `audit` でも取り直せないことがある: 続けて受けたいなら今の状態から読み直し、 `add_change_listener` で付け直す。
    /// 既定は何もしない。
    fn on_detached(&self, lost_from: enchudb_oplog::Hlc) {
        let _ = lost_from;
    }
}
