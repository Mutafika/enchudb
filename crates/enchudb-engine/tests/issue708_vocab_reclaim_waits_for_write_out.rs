//! #708: 語の回収 (`vocab_reclaim`、 #381) は、 参照 0 になった場所を、 その後に始まった本体の書き出しが成功するまで
//! 使い回さないこと。
//!
//! 書き出しは 中身 (辞書の data / offsets) → 列 の順 (#419)。 回収は、 参照 0 になった場所の offsets を **その場で** 次の語
//! (世代 +1) に書き換える。 前はすぐ使い回していたので、 cell を別の値に書き換えた write (列はまだ届いていない) の前の値 =
//! ディスクの cell がまだ指している語が、 列より先に潰れた。 その間に電源が落ちると、 書き出しの返った値が読めなくなった。
//!
//! 像は書き出しを `Phase::BeforeGate` (中身を書き出した後・列の前) で撮る (時間に頼らない)。 どの試験も、 像を開いた後に
//! 「ディスクの cell は前の値の番号のまま」 を確かめる (前提を手渡さない)。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::engine::write_out_hook::{self, Phase};
use enchudb_engine::{Engine, GrowableOptions, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::OpLog;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// crashsim の控えと hook は process に 1 つなので、 試験は 1 本ずつ。
static SERIAL: Mutex<()> = Mutex::new(());
const OPLOG_CAP: usize = 4 * 1024 * 1024;

const A: &str = "AAAA-the-durable-value";
const B: &str = "BBBB-not-yet-written-out";
const C: &str = "CCCC-a-new-word";

/// 一時 DB の置き場 (Drop で必ず消す)。
struct Scratch(PathBuf);
impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu_issue708_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(p.join("live")).unwrap();
        Scratch(p)
    }
    fn db(&self) -> PathBuf {
        self.0.join("live").join("db")
    }
    fn img(&self) -> PathBuf {
        self.0.join("img")
    }
    /// 次に `thread` (None = どの thread でも) が `BeforeGate` を通った時に、 電源断の像を 1 回撮る。
    fn capture_at_before_gate(&self, thread: Option<&'static str>) {
        let (live, img) = (self.0.join("live"), self.img());
        let once = Mutex::new(true);
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate
                && thread.is_none_or(|t| std::thread::current().name() == Some(t))
                && std::mem::take(&mut *once.lock().unwrap())
            {
                crashsim::capture(&live, &img, Mode::Lost).unwrap();
            }
        })));
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        write_out_hook::set(None);
        crashsim::set_after_copy_hook(None);
        crashsim::stop();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn text(eng: &Engine, e: u64) -> Option<String> {
    eng.get_text_owned(e, "t.k").map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// 回収する辞書の DB。 表 t (Tag の列 k)、 行 e0 = A、 行 e1 (空)。
fn build(db: &str, vocab_max_entries: Option<u32>) -> (Engine, u64, u64, u16) {
    let mut eng = Engine::create_growable_opts(
        db,
        GrowableOptions { max_entities: 10_000, vocab_reclaim: true, vocab_max_entries, ..Default::default() },
    )
    .unwrap();
    eng.define_table("t", 1_000).unwrap();
    let k = eng.define_himo_in("t", "k", ValueType::Tag, 0).unwrap() as u16;
    let e0 = eng.entity_in("t").unwrap();
    let e1 = eng.entity_in("t").unwrap();
    eng.try_tie_text_to_by_id(e0, k, A).unwrap();
    (eng, e0, e1, k)
}

/// standalone: `flush()` が返った値 A。 その後 e0 を B に書き換え (A の参照 0)、 別の行に新しい語 C。 次の `flush()` の途中
/// (中身を書き出した後・列の前) で電源が落ちた像で、 e0 は A で読めること。
#[test]
fn a_flushed_tag_value_survives_power_loss_in_the_next_flush() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new("flush");
    crashsim::start();
    let (mut eng, e0, e1, k) = build(s.db().to_str().unwrap(), None);
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let vid_a = eng.get(e0, "t.k").unwrap();
    let before = eng.vocab_usage();
    eng.try_tie_text_to_by_id(e0, k, B).unwrap();
    assert_eq!(eng.vocab_usage().reclaimable_entries, before.reclaimable_entries + 1, "前提: A の場所が空いた");
    eng.try_tie_text_to_by_id(e1, k, C).unwrap();
    let reused_early = eng.vocab_usage().reclaimed != before.reclaimed;
    s.capture_at_before_gate(None);
    eng.flush().unwrap();
    write_out_hook::set(None);
    crashsim::stop();
    drop(eng);
    assert!(s.img().exists(), "前提: 像を撮れた");

    let back = Engine::open_standalone(s.img().join("db").to_str().unwrap()).unwrap();
    assert_eq!(back.get(e0, "t.k"), Some(vid_a), "前提: 像の cell は前の値 A の番号のまま (列は届いていない)");
    assert_eq!(text(&back, e0).as_deref(), Some(A), "flush() が返った値が、 次の flush() の途中の電源断で読めなくなった");
    drop(back);
    assert!(!reused_early, "書き出しの前に、 空いたばかりの場所を使い回した");
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

/// `power_loss.rs` の `with_stalled_sync` と同じ: 別の thread の `oplog_sync` を oplog の fsync の直前 (写した直後) で止め、
/// その間に `during` を呼ぶ (= `during` の write の record は、 その fsync に入らない)。 consumer も次の周期で止める。
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

/// concurrent + oplog の 1 回分。 `oplog_sync()` が返った値 A。 別の thread の `oplog_sync` が oplog を写した後・本体を
/// 書き出す前に、 e0 を B に書き換えて別の行に新しい語 C を書く (record はその fsync に入らない)。 その書き出しが中身を
/// 書き出した所で電源断。 前提を作れなかった (consumer が先に record を届かせた) 時は None。
fn oplog_once(attempt: usize) -> Option<(Option<String>, Option<u64>, u64)> {
    let s = Scratch::new(&format!("oplog{attempt}"));
    crashsim::start();
    let (mut eng, e0, e1, k) = build(s.db().to_str().unwrap(), None);
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    let vid_a = eng.get(e0, "t.k").unwrap();
    s.capture_at_before_gate(Some("stalled-sync"));
    let held = with_stalled_sync(&eng, || {
        eng.try_tie_text_to_by_id(e0, k, B).unwrap();
        eng.try_tie_text_to_by_id(e1, k, C).unwrap();
    });
    write_out_hook::set(None);
    crashsim::stop();
    drop(held);
    drop(eng);
    assert!(s.img().exists(), "前提: 像を撮れた");
    let img_db = s.img().join("db");
    // 前提: 像の oplog に B の record が無い (あれば再生が e0 を B にする)
    let recs = OpLog::open(&img_db.join("oplog")).unwrap().records_with_tail();
    if recs.iter().any(|r| format!("{:?}", r.op).contains("66, 66, 66, 66")) {
        return None;
    }
    let back = Engine::open_concurrent_with_oplog(img_db.to_str().unwrap(), OPLOG_CAP).unwrap();
    let got = (text(&back, e0), back.get(e0, "t.k"), vid_a);
    drop(back);
    Some(got)
}

#[test]
fn an_oplog_synced_tag_value_survives_power_loss_in_the_next_write_out() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for attempt in 0..8 {
        let Some((got, raw, vid_a)) = oplog_once(attempt) else { continue };
        assert_eq!(raw, Some(vid_a), "前提: 像の cell は前の値 A の番号のまま");
        assert_eq!(got.as_deref(), Some(A), "oplog_sync() が返った値が、 次の書き出しの途中の電源断で読めなくなった");
        return;
    }
    panic!("前提を作れなかった (8 回とも像の oplog に新しい record があった)");
}

/// 回収は止まっていない: 書き換えが書き出された後は、 空いた場所を使い回す (standalone の flush / oplog 付きの oplog_sync)。
#[test]
fn a_released_slot_is_reused_after_the_write_out() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for oplog in [false, true] {
        let s = Scratch::new(&format!("reuse{}", oplog as u8));
        let (mut eng, e0, e1, k) = build(s.db().to_str().unwrap(), None);
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let vid_a = eng.get(e0, "t.k").unwrap() as u32;
        let before = eng.vocab_usage();
        let check = |eng: &Engine, write_out: &dyn Fn()| {
            eng.try_tie_text_to_by_id(e0, k, B).unwrap();
            write_out();
            eng.try_tie_text_to_by_id(e1, k, C).unwrap();
            let u = eng.vocab_usage();
            assert_eq!(u.reclaimed, before.reclaimed + 1, "書き出しの後も場所を使い回さない (oplog = {oplog})");
            assert_eq!(u.entries, before.entries + 1, "B の分だけ増え、 C は A の場所に入る (oplog = {oplog})");
            assert_ne!(eng.vocab_id(C), Some(vid_a), "使い回した場所の番号は世代が違う");
            assert_eq!(text(eng, e0).as_deref(), Some(B));
            assert_eq!(text(eng, e1).as_deref(), Some(C));
        };
        if oplog {
            let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
            check(&eng, &|| eng.oplog_sync().unwrap());
        } else {
            check(&eng, &|| eng.body_msync().unwrap());
            eng.flush().unwrap();
        }
    }
}

/// 待っている間に使い直された場所: e0 を A → B (A は待つ)、 別の行 e1 に A (使い直す)、 書き出し (待っていた entry は済む)、
/// e1 を外す (A はまた参照 0、 待ち行列には古い entry が残る。 外したことはまだ届いていない)。 ここで新しい語 E が A の場所を取ってはいけない
/// (ディスクの e1 はまだ A を指す)。 次の書き出しの途中の電源断の像で e1 は A で読めること。
#[test]
fn a_slot_revived_while_waiting_waits_again() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new("revived");
    crashsim::start();
    let (mut eng, e0, e1, k) = build(s.db().to_str().unwrap(), None);
    let e2 = eng.entity_in("t").unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let vid_a = eng.get(e0, "t.k").unwrap();
    eng.try_tie_text_to_by_id(e0, k, B).unwrap();
    eng.try_tie_text_to_by_id(e1, k, A).unwrap();
    assert_eq!(eng.get(e1, "t.k"), Some(vid_a), "前提: 待っている A を同じ番号で使い直した");
    eng.flush().unwrap();
    let before = eng.vocab_usage();
    // 新しい語を挟まずに外す (挟むと、 待ち行列の古い entry はその語が取り出して捨てる)
    eng.untie(e1, "t.k");
    eng.try_tie_text_to_by_id(e2, k, "EEEE-must-not-take-the-slot-of-A").unwrap();
    let reused_early = eng.vocab_usage().reclaimed != before.reclaimed;
    s.capture_at_before_gate(None);
    eng.flush().unwrap();
    write_out_hook::set(None);
    crashsim::stop();
    drop(eng);
    assert!(s.img().exists(), "前提: 像を撮れた");

    let back = Engine::open_standalone(s.img().join("db").to_str().unwrap()).unwrap();
    assert_eq!(back.get(e1, "t.k"), Some(vid_a), "前提: 像の e1 は A の番号のまま");
    assert_eq!(text(&back, e1).as_deref(), Some(A));
    assert_eq!(text(&back, e0).as_deref(), Some(B), "前の flush が返った e0 = B も読める");
    drop(back);
    assert!(!reused_early, "付け替えの書き出しの前に、 使い直された場所を使い回した");
}

/// flush しない書き手が Tag の値を書き換え続けても、 待つ場所で辞書を使い切らない (溜まったら自分で本体を書き出す)。
/// 辞書は 64 語、 1 つの cell を 2000 回違う値に書き換える。
#[test]
fn a_writer_that_never_flushes_does_not_exhaust_the_dictionary() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new("settle");
    let (mut eng, e0, _e1, k) = build(s.db().to_str().unwrap(), Some(64));
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    for i in 0..2000 {
        let word = format!("w{i}");
        eng.try_tie_text_to_by_id(e0, k, &word).unwrap_or_else(|e| panic!("{i} 回目の書き換えが断られた: {e:?}"));
        assert_eq!(text(&eng, e0).as_deref(), Some(word.as_str()));
    }
    let u = eng.vocab_usage();
    assert!(u.entries <= 64 && u.reclaimed > 1000, "{u:?}");
}
