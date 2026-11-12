//! #742: 記録 (`changefeed.seg`) のある DB で、 listener の中から書いて `oplog_sync()` した record が、 次に誰かが書くまで
//! listener に渡らなかった。
//!
//! listener の中の `oplog_sync()` は checkpoint を進めるが、 配信は再入なので何もせず返り、 通知係も起こさない。 進めた分を
//! 記録に写すのは次の畳み (`try_fold_with_changes` の先写し) で、 そこで数えた `moved` を畳む前の写し (0 件) が上書きして
//! いたので、 通知係を起こす条件 `moved > 0` も立たなかった。 consumer の tick が通知係を起こすのは head が checkpoint より
//! 先にある時 (= 次の書き込み) だけなので、 書き込みの無い DB では渡らないまま (次に 1 件書くと、 その分と一緒に数 ms で渡る)。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue742-{tag}-{}", std::process::id()));
        let p = p.to_string_lossy().into_owned();
        let _ = db_files::remove_db(&p);
        Tmp(p)
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = db_files::remove_db(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 値 1 を受けたら、 callback の中で値 2 を書いて `oplog_sync()` する listener。
struct Echo {
    eng: Mutex<Weak<Engine>>,
    himo: u16,
    seen: Mutex<Vec<u64>>,
    /// callback の中の `oplog_sync()` が Ok で返った
    echoed: AtomicBool,
}
impl ChangeListener for Echo {
    fn on_changes(&self, recs: &[WireRecord]) {
        let mut first = false;
        {
            let mut s = self.seen.lock().unwrap();
            for r in recs {
                if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                    first |= value == 1 && !s.contains(&1);
                    s.push(value);
                }
            }
        }
        if !first {
            return;
        }
        let Some(eng) = self.eng.lock().unwrap().upgrade() else { return };
        let e = eng.entity().unwrap();
        eng.tie_to_by_id(e, self.himo, 2u64);
        eng.oplog_sync().unwrap();
        self.echoed.store(true, Ordering::SeqCst);
    }
}

fn wait_until(mut f: impl FnMut() -> bool, secs: u64) -> bool {
    let until = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < until {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    f()
}

#[test]
fn a_record_synced_inside_a_listener_is_delivered_without_another_write() {
    let tmp = Tmp::new("fold");
    let eng = Engine::create_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
    eng.ensure_himo_dynamic("v", ValueType::Number64, 0).unwrap();
    let himo = eng.himo_id("v").unwrap() as u16;
    eng.enable_change_log(1 << 20).unwrap();
    let l = Arc::new(Echo {
        eng: Mutex::new(Arc::downgrade(&eng)),
        himo,
        seen: Mutex::new(Vec::new()),
        echoed: AtomicBool::new(false),
    });
    eng.add_change_listener(l.clone());

    let e = eng.entity().unwrap();
    eng.tie_to_by_id(e, himo, 1u64);
    eng.oplog_sync().unwrap();

    // 前提: listener が値 1 を受け、 その中で値 2 を書いて書き出しまで返った
    assert!(wait_until(|| l.echoed.load(Ordering::SeqCst), 30), "前提: listener の中の oplog_sync() が返らない");
    // 前提: 書き出しは済んでいるので consumer が WAL を畳む (畳みの先写しが記録に写す所まで通る)
    let wal = eng.oplog().unwrap().clone();
    assert!(wait_until(|| wal.is_empty(), 30), "前提: consumer が WAL を畳まない");

    // 値 2 は書き出しの返った record。 この後は誰も書かないので、 起こされなかった通知係は起きない
    let got = wait_until(|| l.seen.lock().unwrap().contains(&2), 10);
    let seen = l.seen.lock().unwrap().clone();
    assert!(got, "listener の中で書いて oplog_sync() した record が、 畳んだ後 10 秒待っても渡らない (渡った値 {seen:?})");
    assert_eq!(seen, vec![1, 2], "渡った値の並び");
}
