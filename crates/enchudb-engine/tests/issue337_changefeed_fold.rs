//! #337: `oplog_sync` が checkpoint を進めてから listener に配るまでの間に consumer の tick が ring を畳むと、
//! 配っていない record が ring ごと消え、 listener の cursor も巻き戻しを古い終端で上書きされて新しい ring の
//! record を飛ばした (2 万回の tie + oplog_sync で 1,184 件が届かなかった)。
//!
//! listener の処理を少し遅くして (配っている最中を長くして) fold と重ならせ、 書いた Tie が全部届くかを見る。
//! 重複は許す (at-least-once)。

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

struct Seen(Mutex<BTreeSet<u64>>);
impl ChangeListener for Seen {
    fn on_changes(&self, recs: &[WireRecord]) {
        std::thread::sleep(std::time::Duration::from_micros(200));
        let mut s = self.0.lock().unwrap();
        for r in recs {
            if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                s.insert(value);
            }
        }
    }
}

#[test]
fn listener_gets_every_committed_record_across_folds() {
    let p = format!("/tmp/enchudb-issue337-{}", std::process::id());
    let _ = db_files::remove_db(&p);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&p, 16 << 20).unwrap();
    let ts = eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    let seen = Arc::new(Seen(Mutex::new(BTreeSet::new())));
    eng.add_change_listener(seen.clone());
    let a = eng.entity().unwrap();
    const N: u64 = 5000;
    for i in 0..N {
        eng.tie_to_by_id(a, ts, i);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
    }
    // 最後の分は consumer の次の tick でも配られうる
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while seen.0.lock().unwrap().len() < N as usize && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let got = seen.0.lock().unwrap().clone();
    let missing: Vec<u64> = (0..N).filter(|v| !got.contains(v)).collect();
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert!(missing.is_empty(), "{} / {N} 件が listener に届かない (先頭 {:?})", missing.len(), &missing[..missing.len().min(5)]);
}

/// 書き手が 2 本 + sync tables あり。 `oplog_sync` は checkpoint を進めてから bridge を挟んで listener に配るので、
/// その間に consumer が畳める窓が広い。 片方の配信がもう片方の配信中で飛ばされた分は、 書き込みが止んだ後に
/// consumer が配る (配り終えるまで畳まない)。
#[test]
fn two_writers_with_sync_tables_lose_nothing() {
    let p = format!("/tmp/enchudb-issue337-sync-{}", std::process::id());
    let _ = db_files::remove_db(&p);
    {
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_table("rows", 100_000).unwrap();
        eng.define_himo_in("rows", "val", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }
    let eng: Arc<Engine> = Engine::open_concurrent_with_oplog(&p, 16 << 20).unwrap();
    eng.set_peer_id(1);
    let hid = eng.himo_id("rows.val").unwrap() as u16;
    let seen = Arc::new(Seen(Mutex::new(BTreeSet::new())));
    eng.add_change_listener(seen.clone());
    const N: u64 = 2000;
    let hs: Vec<_> = (0..2u64)
        .map(|t| {
            let eng = eng.clone();
            std::thread::spawn(move || {
                let e = eng.entity_in("rows").unwrap();
                for i in 0..N {
                    eng.tie_to_by_id(e, hid, (t * N + i) as u32);
                    eng.oplog_commit();
                    eng.oplog_sync().unwrap();
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while seen.0.lock().unwrap().len() < 2 * N as usize && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let got = seen.0.lock().unwrap().clone();
    let missing: Vec<u64> = (0..2 * N).filter(|v| !got.contains(v)).collect();
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert!(missing.is_empty(), "{} / {} 件が listener に届かない (先頭 {:?})", missing.len(), 2 * N, &missing[..missing.len().min(5)]);
}

/// consumer の thread から呼ばれた時だけ遅い listener (consumer が配っている最中を長くする)。
struct SlowOnConsumer(Mutex<BTreeSet<u64>>);
impl ChangeListener for SlowOnConsumer {
    fn on_changes(&self, recs: &[WireRecord]) {
        if std::thread::current().name() == Some("enchudb-consumer") {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let mut s = self.0.lock().unwrap();
        for r in recs {
            if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                s.insert(value);
            }
        }
    }
}

/// `oplog_sync()` から戻った時点で、 その commit は listener に配り終えている (changefeed の module doc の約束)。
/// consumer の tick が同じ record を遅い listener に配っている最中でも、 `oplog_sync` はそれを待ってから返る
/// (待たずに返ると、 まだ配り終えていない)。
#[test]
fn oplog_sync_returns_after_its_records_are_delivered() {
    let p = format!("/tmp/enchudb-issue337-promise-{}", std::process::id());
    let _ = db_files::remove_db(&p);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&p, 16 << 20).unwrap();
    let ts = eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    let seen = Arc::new(SlowOnConsumer(Mutex::new(BTreeSet::new())));
    eng.add_change_listener(seen.clone());
    let a = eng.entity().unwrap();
    let mut late = Vec::new();
    for i in 0..3000u64 {
        eng.tie_to_by_id(a, ts, i);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        if !seen.0.lock().unwrap().contains(&i) {
            late.push(i);
        }
    }
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert!(late.is_empty(), "{} 回、 oplog_sync から戻った時点で届いていない (先頭 {:?})", late.len(), &late[..late.len().min(5)]);
}

/// listener の中から `oplog_sync()` を呼んでも止まらない (配っている最中の再入は配らずに返る)。
#[test]
fn oplog_sync_inside_a_listener_does_not_block() {
    struct Reenter(Mutex<Option<Arc<Engine>>>, std::sync::atomic::AtomicU64);
    impl ChangeListener for Reenter {
        fn on_changes(&self, _: &[WireRecord]) {
            self.1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(eng) = self.0.lock().unwrap().as_ref() {
                eng.oplog_sync().unwrap();
            }
        }
    }
    let p = format!("/tmp/enchudb-issue337-reenter-{}", std::process::id());
    let _ = db_files::remove_db(&p);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&p, 16 << 20).unwrap();
    let ts = eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    let l = Arc::new(Reenter(Mutex::new(Some(eng.clone())), Default::default()));
    eng.add_change_listener(l.clone());
    let a = eng.entity().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let eng2 = eng.clone();
    std::thread::spawn(move || {
        for i in 0..50u64 {
            eng2.tie_to_by_id(a, ts, i);
            eng2.oplog_commit();
            eng2.oplog_sync().unwrap();
        }
        tx.send(()).unwrap();
    });
    rx.recv_timeout(std::time::Duration::from_secs(30)).expect("listener の中の oplog_sync で止まった");
    assert!(l.1.load(std::sync::atomic::Ordering::Relaxed) > 0);
    *l.0.lock().unwrap() = None; // engine との循環参照を切る
    drop(eng);
    let _ = db_files::remove_db(&p);
}
