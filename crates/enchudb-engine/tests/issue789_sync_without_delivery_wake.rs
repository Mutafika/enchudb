//! #789: `oplog_sync()` が自分の commit を配らずに返る道が 2 つある: listener の中からの再入と、 止まった listener を待つのを
//! 諦めた時 (5 秒)。 どちらも通知係を起こさなかった。 書き出しは済んでいる (checkpoint = head) ので consumer の周期も起こさず、
//! 渡るのは次に WAL を畳む時 (畳む前に溜めへ移した分で起こす) だった: 畳める DB なら次の周期 (実測 103〜105 ms)、 畳めない間は
//! 次に誰かが書くまで。 #742 (記録のある DB) の残り。 ここは記録 (`changefeed.seg`) の無い DB。
//!
//! 畳めない状態は 2 通りで作る: sync の ring が満杯で bridge が止まっている (実際に起きる形)、 周期の畳みを試験の口で切る。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue789-{tag}-{}", std::process::id()));
        let p = p.to_string_lossy().into_owned();
        let _ = std::fs::remove_dir_all(&p);
        Tmp(p)
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 試験が書く値 (bridge を止めるための書き込みはこれより小さい)。
const FIRST: u64 = 1_000_001;
const ECHO: u64 = 1_000_002;
const SECOND: u64 = 1_000_003;

/// entity cap 1024、 `notes` (枠 8)。 `sync` なら sync の表も (ring は 508 行)。
fn open(path: &str, sync: bool) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap()
}

fn wait_until(mut f: impl FnMut() -> bool, secs: u64) -> bool {
    let until = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < until {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    f()
}

/// 渡った試験の値を覚える。 `FIRST` を受けた時に `on_first` を 1 回呼ぶ (callback の中)。
struct Seen<F: Fn(&Engine) + Send + Sync> {
    eng: Mutex<Weak<Engine>>,
    seen: Mutex<Vec<u64>>,
    on_first: F,
    /// `on_first` が返った
    done: AtomicBool,
}
impl<F: Fn(&Engine) + Send + Sync> ChangeListener for Seen<F> {
    fn on_changes(&self, recs: &[WireRecord]) {
        let mut first = false;
        {
            let mut s = self.seen.lock().unwrap();
            for r in recs {
                if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op
                    && value >= FIRST
                {
                    first |= value == FIRST && !s.contains(&FIRST);
                    s.push(value);
                }
            }
        }
        if first {
            if let Some(eng) = self.eng.lock().unwrap().upgrade() {
                (self.on_first)(&eng);
            }
            self.done.store(true, Ordering::SeqCst);
        }
    }
}
impl<F: Fn(&Engine) + Send + Sync> Seen<F> {
    fn has(&self, v: u64) -> bool {
        self.seen.lock().unwrap().contains(&v)
    }
}

fn listen<F: Fn(&Engine) + Send + Sync + 'static>(eng: &Arc<Engine>, on_first: F) -> Arc<Seen<F>> {
    let l = Arc::new(Seen { eng: Mutex::new(Arc::downgrade(eng)), seen: Mutex::new(Vec::new()), on_first, done: AtomicBool::new(false) });
    eng.add_change_listener(l.clone());
    l
}

/// ack なしで書いて、 bridge が ring の満杯で止まった所まで進める (= WAL を畳めない)。
fn stall_bridge(eng: &Engine, row: u64) {
    let cap = eng.table_eid_usage("_sync_ops").unwrap().capacity;
    for i in 0..cap + 200 {
        eng.tie_to(row, "notes.note", i);
        if i % 32 == 31 {
            eng.oplog_commit();
        }
    }
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    assert!(bridge_stalled(eng), "前提: bridge が ring の満杯で止まっている");
}

fn bridge_stalled(eng: &Engine) -> bool {
    eng.sync_ops_bridge_offset() < eng.oplog_head()
}

/// sync の ring が満杯で bridge が止まっている DB (WAL を畳めない)。 通知係が配っている listener の中で書いて `oplog_sync()` した
/// record は、 次の書き込みなしで渡る。
#[test]
fn a_record_synced_inside_a_listener_is_delivered_while_the_bridge_is_stalled() {
    let tmp = Tmp::new("stalled");
    let eng = open(&tmp.0, true);
    let row = eng.entity_in("notes").unwrap();
    stall_bridge(&eng, row);
    let l = listen(&eng, move |eng: &Engine| {
        eng.tie_to(row, "notes.note", ECHO as u32);
        eng.oplog_sync().unwrap();
    });
    eng.tie_to(row, "notes.note", FIRST as u32);
    // 書き出しは consumer に任せる (配るのは通知係 = 再入は通知係の thread の上)
    eng.oplog_commit();
    assert!(wait_until(|| l.done.load(Ordering::SeqCst), 30), "前提: listener の中の oplog_sync() が返らない");
    let got = wait_until(|| l.has(ECHO), 30);
    assert!(bridge_stalled(&eng), "前提: 待っている間も bridge は止まったまま (畳めない)");
    let seen = l.seen.lock().unwrap().clone();
    assert!(got, "listener の中で書いて oplog_sync() した record が、 30 秒待っても渡らない (渡った値 {seen:?})");
    assert_eq!(seen, vec![FIRST, ECHO]);
}

/// 周期の畳みを切った DB。 `oplog_sync()` を呼んだ thread が配っている listener の中で書いて `oplog_sync()` した record も渡る
/// (配っているのが通知係でも呼んだ thread でも)。
#[test]
fn a_record_synced_inside_a_listener_is_delivered_without_a_fold() {
    let tmp = Tmp::new("nofold");
    let eng = open(&tmp.0, false);
    eng.set_wal_fold_on_tick(false);
    let row = eng.entity_in("notes").unwrap();
    let l = listen(&eng, move |eng: &Engine| {
        eng.tie_to(row, "notes.note", ECHO as u32);
        eng.oplog_sync().unwrap();
    });
    eng.tie_to(row, "notes.note", FIRST as u32);
    eng.oplog_sync().unwrap();
    assert!(wait_until(|| l.done.load(Ordering::SeqCst), 30), "前提: listener の中の oplog_sync() が返らない");
    let got = wait_until(|| l.has(ECHO), 30);
    assert!(!eng.oplog().unwrap().is_empty(), "前提: WAL は畳まれていない");
    let seen = l.seen.lock().unwrap().clone();
    assert!(got, "listener の中で書いて oplog_sync() した record が、 30 秒待っても渡らない (渡った値 {seen:?})");
    assert_eq!(seen, vec![FIRST, ECHO]);
}

/// 止まった listener を待つのを諦めて返った `oplog_sync()` の record は、 listener が戻った後に渡る。
///
/// 止まる listener は別の thread の `oplog_sync()` の中 (通知係は空いている)。 直す前に落ちるのは、 「通知係が、 consumer に
/// 起こされて配ろうとし、 同じ 5 秒を待って諦めた後」 に listener を放した時 (諦めた通知係をもう誰も起こさない)。 その順は時間で
/// 作っている (諦めた後に 2 秒置く): 直しの無い code で落ちる割合は module の下の注記。 直しがあれば順によらず渡る。
#[test]
fn a_record_synced_while_a_listener_is_stuck_is_delivered_after_it_returns() {
    let tmp = Tmp::new("stuck");
    let eng = open(&tmp.0, false);
    eng.set_wal_fold_on_tick(false);
    let row = eng.entity_in("notes").unwrap();
    let release = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicBool::new(false));
    let (r, e) = (release.clone(), entered.clone());
    let l = listen(&eng, move |_: &Engine| {
        e.store(true, Ordering::SeqCst);
        while !r.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(2));
        }
    });
    // 別の thread が FIRST を書いて oplog_sync(): その中で listener が止まる (通知係が先に配った時は通知係の中で止まる)
    let eng2 = eng.clone();
    let first = std::thread::spawn(move || {
        eng2.tie_to(row, "notes.note", FIRST as u32);
        eng2.oplog_sync().unwrap();
    });
    assert!(wait_until(|| entered.load(Ordering::SeqCst), 30), "前提: listener が FIRST を受けて止まった");
    eng.tie_to(row, "notes.note", SECOND as u32);
    let t0 = Instant::now();
    eng.oplog_sync().unwrap();
    assert!(t0.elapsed() >= Duration::from_secs(4), "前提: oplog_sync() は止まった listener を待ってから返った ({:?})", t0.elapsed());
    assert!(!l.has(SECOND), "前提: oplog_sync() は SECOND を配らずに返った");
    // 起こされていた通知係も同じ上限で諦めるのを待つ
    std::thread::sleep(Duration::from_secs(2));
    assert!(!l.has(SECOND));
    release.store(true, Ordering::SeqCst);
    first.join().unwrap();
    let got = wait_until(|| l.has(SECOND), 30);
    let seen = l.seen.lock().unwrap().clone();
    assert!(got, "待つのを諦めた oplog_sync() の record が、 listener が戻った後 30 秒待っても渡らない (渡った値 {seen:?})");
    assert_eq!(seen, vec![FIRST, SECOND]);
}
