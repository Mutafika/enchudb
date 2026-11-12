//! #719: engine A の listener の中で engine B に書いて `B.oplog_sync()` を呼ぶと、 B の commit を配らずに返った
//! (「戻った時点で自分の commit を配り終えている」 が B で破れる。 届くのは B の consumer の次の周期の後)。
//!
//! 配信中の印 (`EMITTING`) が thread ごとで、 engine ごとでなかった。 同じ engine の入れ子 (listener の中で同じ engine の
//! `oplog_sync()`) は今まで通り配らずに返る (待つと自分を待って止まる)。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue719-{tag}-{}", std::process::id()));
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

fn tie_value(r: &WireRecord) -> Option<u64> {
    match r.op {
        enchudb_oplog::oplog::DecodedOp::Tie { value, .. } => Some(value),
        _ => None,
    }
}

#[derive(Default)]
struct Rec {
    seen: Mutex<Vec<u64>>,
}
impl Rec {
    fn values(&self) -> Vec<u64> {
        self.seen.lock().unwrap().clone()
    }
}
impl ChangeListener for Rec {
    fn on_changes(&self, recs: &[WireRecord]) {
        self.seen.lock().unwrap().extend(recs.iter().filter_map(tie_value));
    }
}

fn wait_until(mut f: impl FnMut() -> bool, secs: u64) -> bool {
    let until = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < until {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    f()
}

fn engine(tag: &str) -> (Tmp, Arc<Engine>) {
    let tmp = Tmp::new(tag);
    let eng = Engine::create_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    (tmp, eng)
}

/// A の listener: 受けた Tie を B に書き、 `B.oplog_sync()` を呼ぶ。 返った時点で B の listener が受けていた値と、 それまでに
/// B に書いた値を控える。
struct Forward {
    b: Arc<Engine>,
    b_rec: Arc<Rec>,
    /// (B.oplog_sync() が返った時点で B の listener が受けていた値, それまでに B に書いた値)
    at_return: Mutex<Vec<(Vec<u64>, Vec<u64>)>>,
    wrote: Mutex<Vec<u64>>,
    /// `B.oplog_sync()` にかかった最長の時間
    slowest: Mutex<Duration>,
}
impl ChangeListener for Forward {
    fn on_changes(&self, recs: &[WireRecord]) {
        let ts = self.b.himo_id("ts").unwrap() as u16;
        let e = self.b.entity().unwrap();
        let mut any = false;
        for v in recs.iter().filter_map(tie_value) {
            self.b.tie_to_by_id(e, ts, v);
            self.wrote.lock().unwrap().push(v);
            any = true;
        }
        if !any {
            return;
        }
        let started = Instant::now();
        self.b.oplog_sync().unwrap();
        let took = started.elapsed();
        let mut slowest = self.slowest.lock().unwrap();
        *slowest = (*slowest).max(took);
        self.at_return.lock().unwrap().push((self.b_rec.values(), self.wrote.lock().unwrap().clone()));
    }
}

#[test]
fn oplog_sync_on_another_engine_inside_a_listener_delivers_before_it_returns() {
    let (_ta, a) = engine("a");
    let (_tb, b) = engine("b");
    let b_rec = Arc::new(Rec::default());
    b.add_change_listener(b_rec.clone());
    let fwd = Arc::new(Forward {
        b: b.clone(),
        b_rec: b_rec.clone(),
        at_return: Mutex::new(Vec::new()),
        wrote: Mutex::new(Vec::new()),
        slowest: Mutex::new(Duration::ZERO),
    });
    a.add_change_listener(fwd.clone());

    // 対照: listener の外から B に書いて oplog_sync → 返った時点で届いている
    {
        let ts = b.himo_id("ts").unwrap() as u16;
        let e = b.entity().unwrap();
        b.tie_to_by_id(e, ts, 7u64);
        b.oplog_sync().unwrap();
        assert_eq!(b_rec.values(), vec![7], "対照: listener の外からの oplog_sync は返る前に配る");
    }

    let ts = a.himo_id("ts").unwrap() as u16;
    let e = a.entity().unwrap();
    for round in 0..20u64 {
        for v in 0..5u64 {
            a.tie_to_by_id(e, ts, 100 + round * 10 + v);
        }
        a.oplog_sync().unwrap();
    }
    assert!(wait_until(|| fwd.wrote.lock().unwrap().len() == 100, 60), "前提: A の listener が 100 件を B に書いた");
    let at_return = fwd.at_return.lock().unwrap().clone();
    assert!(!at_return.is_empty(), "前提: A の listener の中で B.oplog_sync() を呼んだ");
    for (k, (got, wrote)) in at_return.iter().enumerate() {
        let mut want = vec![7u64];
        want.extend(wrote);
        assert_eq!(
            got, &want,
            "{k} 回目: A の listener の中の B.oplog_sync() が、 B の commit を配らずに返った (B の listener は {} 件、 B に書いたのは対照の 1 件 + {} 件)",
            got.len(),
            wrote.len()
        );
    }
    // 配信の待ち (5 秒) に当たって配らずに返った回が無い
    assert!(*fwd.slowest.lock().unwrap() < Duration::from_secs(4), "B.oplog_sync() に {:?}", fwd.slowest.lock().unwrap());
    // 循環 (fwd → b → b_rec → fwd) を切ってから閉じる
    drop(a);
    drop(b);
}

/// 同じ engine の入れ子は今まで通り: listener の中で同じ engine の `oplog_sync()` を呼んでも止まらず、 待たずに返る
/// (その呼び出しは配らない。 書いた分は後の配信で届く)。
#[test]
fn oplog_sync_on_the_same_engine_inside_a_listener_still_returns_at_once() {
    struct Echo {
        eng: Mutex<Option<Arc<Engine>>>,
        seen: Mutex<Vec<u64>>,
        nested: AtomicUsize,
        slowest: Mutex<Duration>,
        /// 入れ子の oplog_sync が返った時点で、 中で書いた値をもう受けていた回数 (入れ子が配っていれば 0 でない)
        delivered_inside: AtomicUsize,
    }
    impl ChangeListener for Echo {
        fn on_changes(&self, recs: &[WireRecord]) {
            let vals: Vec<u64> = recs.iter().filter_map(tie_value).collect();
            self.seen.lock().unwrap().extend(&vals);
            let Some(eng) = self.eng.lock().unwrap().clone() else { return };
            // 1000 未満の値を受けたら、 +1000 した値を同じ engine に書いて oplog_sync
            let echo: Vec<u64> = vals.iter().filter(|v| **v < 1000).map(|v| v + 1000).collect();
            if echo.is_empty() {
                return;
            }
            let ts = eng.himo_id("ts").unwrap() as u16;
            let e = eng.entity().unwrap();
            for v in &echo {
                eng.tie_to_by_id(e, ts, *v);
            }
            let started = Instant::now();
            eng.oplog_sync().unwrap();
            let took = started.elapsed();
            let mut slowest = self.slowest.lock().unwrap();
            *slowest = (*slowest).max(took);
            self.nested.fetch_add(1, Ordering::SeqCst);
            // この callback は `seen` の lock を持っていないので、 入れ子が配っていればここで見える
            if self.seen.lock().unwrap().iter().any(|v| echo.contains(v)) {
                self.delivered_inside.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    let (_t, eng) = engine("same");
    let echo = Arc::new(Echo {
        eng: Mutex::new(Some(eng.clone())),
        seen: Mutex::new(Vec::new()),
        nested: AtomicUsize::new(0),
        slowest: Mutex::new(Duration::ZERO),
        delivered_inside: AtomicUsize::new(0),
    });
    eng.add_change_listener(echo.clone());
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    for v in 1..=5u64 {
        eng.tie_to_by_id(e, ts, v);
        eng.oplog_sync().unwrap();
    }
    // 中で書いた分も後の配信で届く
    assert!(
        wait_until(|| echo.seen.lock().unwrap().iter().filter(|v| **v >= 1000).count() == 5, 60),
        "入れ子で書いた分が届かない: {:?}",
        echo.seen.lock().unwrap()
    );
    assert!(echo.nested.load(Ordering::SeqCst) >= 1, "前提: listener の中で同じ engine の oplog_sync を呼んだ");
    assert!(*echo.slowest.lock().unwrap() < Duration::from_secs(4), "入れ子の oplog_sync が配信を待った: {:?}", echo.slowest.lock().unwrap());
    assert_eq!(echo.delivered_inside.load(Ordering::SeqCst), 0, "同じ engine の入れ子の oplog_sync が配った (callback の入れ子)");
    let mut seen = echo.seen.lock().unwrap().clone();
    seen.sort_unstable();
    assert_eq!(seen, vec![1, 2, 3, 4, 5, 1001, 1002, 1003, 1004, 1005], "1 回ずつ");
    // 循環 (engine → listener → engine) を切ってから閉じる
    *echo.eng.lock().unwrap() = None;
}
