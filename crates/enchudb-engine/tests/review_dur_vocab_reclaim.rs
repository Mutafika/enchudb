//! review (0.37.4): 語の回収 (`vocab_reclaim`、 #381) と本体の書き出しの順。
//!
//! 書き出しは 中身 (辞書の data / offsets) → 列 の順 (#419: cell が中身より先に届かない)。 回収のある辞書は、 参照 0 に
//! なった場所の offsets を **その場で** 次の語 (世代 +1) に書き換える。 その書き換えは 「中身」 として列より先に届くので、
//! cell を別の値に書き換えた write (まだ列は届いていない) の前の値 = disk の cell がまだ指している語が、 列より先に
//! 潰れる。 その間に電源が落ちると、 前の書き出しで durable だった値が読めなくなる (cell の番号は世代が合わない)。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::engine::write_out_hook::{self, Phase};
use enchudb_engine::{Engine, GrowableOptions, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::OpLog;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

static SERIAL: Mutex<()> = Mutex::new(());
const OPLOG_CAP: usize = 4 * 1024 * 1024;

fn scratch(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("enchu_review_dur_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn text(eng: &Engine, e: u64) -> Option<String> {
    eng.get_text_owned(e, "t.k").map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn build(db: &str) -> (Engine, u64, u64, u16) {
    let mut eng = Engine::create_growable_opts(
        db,
        GrowableOptions { max_entities: 10_000, vocab_reclaim: true, ..Default::default() },
    )
    .unwrap();
    eng.define_table("t", 1_000).unwrap();
    let k = eng.define_himo_in("t", "k", ValueType::Tag, 0).unwrap() as u16;
    let e0 = eng.entity_in("t").unwrap();
    let e1 = eng.entity_in("t").unwrap();
    eng.try_tie_text_to_by_id(e0, k, "AAAA-the-durable-value").unwrap();
    (eng, e0, e1, k)
}

/// standalone: `flush()` が返った値 A。 その後 e0 を B に書き換え (A の参照 0)、 別の行に新しい語 C (A の場所を使い回す)。
/// 次の `flush()` の途中 (中身を書き出した後・列の前) で電源が落ちた像で、 e0 は A (か B) で読めること。
#[test]
fn a_flushed_tag_value_survives_power_loss_in_the_next_flush() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("reclaim_flush");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let (mut eng, e0, e1, k) = build(db.to_str().unwrap());
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    assert_eq!(text(&eng, e0).as_deref(), Some("AAAA-the-durable-value"));
    let before = eng.vocab_usage();
    eng.try_tie_text_to_by_id(e0, k, "BBBB-not-yet-flushed").unwrap();
    assert_eq!(eng.vocab_usage().reclaimable_entries, before.reclaimable_entries + 1, "前提: A の場所が空いた");
    eng.try_tie_text_to_by_id(e1, k, "CCCC-reuses-the-slot-of-A").unwrap();
    assert_eq!(eng.vocab_usage().reclaimable_entries, before.reclaimable_entries, "前提: C が A の場所を使い回した");
    let img = root.join("img");
    {
        let (live, img) = (live.clone(), img.clone());
        let once = Mutex::new(true);
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && std::mem::take(&mut *once.lock().unwrap()) {
                crashsim::capture(&live, &img, Mode::Lost).unwrap();
            }
        })));
    }
    eng.flush().unwrap();
    write_out_hook::set(None);
    crashsim::stop();
    drop(eng);
    let back = Engine::open_standalone(img.join("db").to_str().unwrap()).unwrap();
    let got = text(&back, e0);
    let raw = back.get(e0, "t.k");
    let other = text(&back, e1);
    drop(back);
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[review_dur] standalone: e0 = {got:?} (cell {raw:?}), e1 = {other:?}");
    assert!(
        matches!(got.as_deref(), Some("AAAA-the-durable-value") | Some("BBBB-not-yet-flushed")),
        "flush() が返った値が、 次の flush() の途中の電源断で読めなくなった: e0 = {got:?} (cell {raw:?})"
    );
}

struct ConsumerHeld(Option<std::sync::mpsc::Sender<()>>);
impl Drop for ConsumerHeld {
    fn drop(&mut self) {
        if let Some(r) = self.0.take() {
            let _ = r.send(());
        }
        crashsim::set_after_copy_hook(None);
    }
}

/// power_loss.rs の `with_stalled_sync` と同じ。
fn with_stalled_sync(eng: &Arc<Engine>, during: impl FnOnce()) -> ConsumerHeld {
    let (copied_tx, copied_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let stall = Mutex::new(Some((copied_tx, go_rx)));
    let hold = Mutex::new(Some(release_rx));
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
        if what != Copied::Oplog {
            return;
        }
        match std::thread::current().name() {
            Some("stalled-sync") => {
                let taken = stall.lock().unwrap().take();
                if let Some((copied, go)) = taken {
                    copied.send(()).unwrap();
                    go.recv().unwrap();
                }
            }
            Some("enchudb-consumer") => {
                let taken = hold.lock().unwrap().take();
                if let Some(release) = taken {
                    let _ = release.recv();
                }
            }
            _ => {}
        }
    })));
    let stalled = {
        let eng = eng.clone();
        std::thread::Builder::new().name("stalled-sync".into()).spawn(move || eng.oplog_sync().unwrap()).unwrap()
    };
    copied_rx.recv().unwrap();
    during();
    go_tx.send(()).unwrap();
    stalled.join().unwrap();
    ConsumerHeld(Some(release_tx))
}

/// concurrent + oplog: `oplog_sync()` が返った値 A。 別の thread の `oplog_sync` が oplog を fsync した後・本体を書き出す前に、
/// e0 を B に書き換えて別の行に新しい語 C を書く (record は oplog に届いていない)。 その書き出しが中身を書き出した所で電源断。
fn oplog_once(attempt: usize) -> Option<(Option<String>, Option<u64>)> {
    let root = scratch(&format!("reclaim_oplog{attempt}"));
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let (mut eng, e0, e1, k) = build(db.to_str().unwrap());
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    let img = root.join("img");
    {
        let (live, img) = (live.clone(), img.clone());
        let once = Mutex::new(true);
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate
                && std::thread::current().name() == Some("stalled-sync")
                && std::mem::take(&mut *once.lock().unwrap())
            {
                crashsim::capture(&live, &img, Mode::Lost).unwrap();
            }
        })));
    }
    let held = with_stalled_sync(&eng, || {
        eng.try_tie_text_to_by_id(e0, k, "BBBB-not-yet-synced").unwrap();
        eng.try_tie_text_to_by_id(e1, k, "CCCC-reuses-the-slot-of-A").unwrap();
    });
    write_out_hook::set(None);
    crashsim::stop();
    drop(held);
    let claims = eng.vocab_usage();
    drop(eng);
    assert!(img.exists(), "像を撮れなかった");
    let img_db = img.join("db");
    // 前提: 像の oplog に B / C の record が無い (あれば再生が直す)
    let recs = OpLog::open(&img_db.join("oplog")).unwrap().records_with_tail();
    let has_new = recs.iter().any(|r| format!("{:?}", r.op).contains("BBBB") || format!("{:?}", r.op).contains("66, 66, 66, 66"));
    if has_new {
        let _ = std::fs::remove_dir_all(&root);
        return None;
    }
    let back = Engine::open_concurrent_with_oplog(img_db.to_str().unwrap(), OPLOG_CAP).unwrap();
    let got = text(&back, e0);
    let raw = back.get(e0, "t.k");
    drop(back);
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[review_dur] oplog: e0 = {got:?} (cell {raw:?}), usage {claims:?}, 像の oplog の record {} 件", recs.len());
    Some((got, raw))
}

#[test]
fn an_oplog_synced_tag_value_survives_power_loss_in_the_next_write_out() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for attempt in 0..8 {
        let Some((got, raw)) = oplog_once(attempt) else { continue };
        assert!(
            matches!(got.as_deref(), Some("AAAA-the-durable-value") | Some("BBBB-not-yet-synced")),
            "oplog_sync() が返った値が、 次の書き出しの途中の電源断で読めなくなった: e0 = {got:?} (cell {raw:?})"
        );
        return;
    }
    panic!("前提を作れなかった (8 回とも像の oplog に新しい record があった)");
}
