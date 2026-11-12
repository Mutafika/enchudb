//! #709: oplog 付きの engine の session の途中の `flush_clean()` / `flush()` は、 「きれいに閉じた」 の印 (紐名の辞書の
//! clean flag) を立てないこと。
//!
//! 開く時、 その印が立っていると 「本体にあって oplog / `_sync_ops` に無い自分の write」 の照合 (#451
//! `relog_unlogged_writes`) を飛ばす。 前は `flush_clean()` が session の途中で印を立て直し、 その後の普通の書き込みは
//! 倒さなかった (倒すのは新しい紐を作った時だけ)。 その後に電源が落ちると、 本体にだけ届いた write が相手に永久に届かない。
//! `flush_clean()` は oplog を fsync しないので、 その後に何も書かなくても同じだった (2 本目)。
//!
//! 像は crashsim で撮る。 どの試験も、 開く前に 「像の本体に write があり、 像の oplog に record が無い」 を確かめる。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::{decode_sync_ops_payload, DecodedOp, OpLog};
use enchudb_oplog::eid_local;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const OPLOG_CAP: usize = 4 * 1024 * 1024;
/// crashsim の控えと hook は process に 1 つなので、 試験は 1 本ずつ。
static SERIAL: Mutex<()> = Mutex::new(());

/// 一時 DB の置き場 (Drop で必ず消す)。
struct Scratch(PathBuf);
impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu_issue709_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(p.join("live")).unwrap();
        Scratch(p)
    }
    fn live(&self) -> PathBuf {
        self.0.join("live")
    }
    fn img(&self) -> PathBuf {
        self.0.join("img")
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        crashsim::set_after_copy_hook(None);
        crashsim::stop();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// sync する DB (表 notes、 Number の列 note)、 concurrent + oplog。
fn notes_engine(s: &Scratch) -> Arc<Engine> {
    let db = s.live().join("db");
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("notes", 1_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    eng
}

struct ConsumerHeld(Option<mpsc::Sender<()>>);
impl Drop for ConsumerHeld {
    fn drop(&mut self) {
        if let Some(r) = self.0.take() {
            let _ = r.send(());
        }
        crashsim::set_after_copy_hook(None);
    }
}

/// `power_loss.rs` の `with_stalled_sync` と同じ: 別の thread の `oplog_sync` を oplog の fsync の直前 (写した直後) で止め、
/// その間に `during` を呼ぶ = `during` の write は、 record は届かず (fsync に入らない)、 本体には続く書き出しで届く。
/// consumer も次の周期で止める。
fn with_stalled_sync(eng: &Arc<Engine>, during: impl FnOnce()) -> ConsumerHeld {
    let (copied_tx, copied_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
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

/// `power_loss.rs` の `hold_consumer` と同じ: consumer を周期の oplog の書き出し (写した直後) で止める。
fn hold_consumer(eng: &Arc<Engine>) -> ConsumerHeld {
    let (parked_tx, parked_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let hold = Mutex::new(Some((parked_tx, release_rx)));
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
        if what == Copied::Oplog && std::thread::current().name() == Some("enchudb-consumer") {
            let taken = hold.lock().unwrap().take();
            if let Some((parked, release)) = taken {
                let _ = parked.send(());
                let _ = release.recv();
            }
        }
    })));
    eng.oplog_commit();
    parked_rx.recv_timeout(Duration::from_secs(60)).expect("前提: consumer が止まる");
    ConsumerHeld(Some(release_tx))
}

/// どの形の像を作るか。
#[derive(Clone, Copy, PartialEq, Debug)]
enum Shape {
    /// 対照: `flush_clean` を呼ばない。 oplog の fsync の後・本体の書き出しの前に書く
    Control,
    /// `flush_clean` の後に、 oplog の fsync の後・本体の書き出しの前に書く (issue の形)
    WriteAfterFlushClean,
    /// 書いた後に `flush_clean` (本体は届く、 oplog は fsync しない)。 その後は何も書かない
    FlushCleanAfterWrite,
}

/// 像を開いた結果。
struct Outcome {
    relogged: u64,
    /// 配る分に write (b.note = 22) がある
    distributed: bool,
    /// 相手に取り直させる floor が上がった
    floored: bool,
}

/// 1 回分。 前提を作れなかった (像の oplog に write の record が届いていた) 時は None。
fn once(shape: Shape, attempt: usize) -> Option<Outcome> {
    let s = Scratch::new(&format!("{shape:?}_{attempt}"));
    crashsim::start();
    let eng = notes_engine(&s);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let [a, b] = [(); 2].map(|_| eng.entity_in("notes").unwrap());
    eng.tie_to(a, "notes.note", 1);
    eng.oplog_sync().unwrap();
    let held = match shape {
        Shape::Control => with_stalled_sync(&eng, || eng.tie_to(b, "notes.note", 22)),
        Shape::WriteAfterFlushClean => {
            // session の途中の clean-flush (公開 API)。 この後も書き続ける
            eng.flush_clean().unwrap();
            with_stalled_sync(&eng, || eng.tie_to(b, "notes.note", 22))
        }
        Shape::FlushCleanAfterWrite => {
            eng.oplog_sync().unwrap();
            let held = hold_consumer(&eng);
            eng.tie_to(b, "notes.note", 22);
            eng.flush_clean().unwrap();
            held
        }
    };
    crashsim::capture(&s.live(), &s.img(), Mode::Lost).unwrap();
    crashsim::stop();
    drop(held);
    let written = eng.cell_hlc(b, note);
    drop(eng);

    let db = s.img().join("db");
    // 前提: 像の oplog に write の record が無い (あれば再生と bridge が配る)
    if OpLog::open(&db.join("oplog")).unwrap().records_with_tail().iter().any(|r| r.hlc == written) {
        return None;
    }
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    // 前提: 像の本体に write がある
    assert_eq!(eng.get(b, "notes.note"), Some(22), "前提: write は像の本体に届いている ({shape:?})");
    let relogged = eng.unlogged_writes_relogged();
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    let recs: Vec<_> = eng.pending_sync_ops(0).iter().filter_map(|p| decode_sync_ops_payload(p)).collect();
    let distributed = recs.iter().any(
        |r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, value } if eid_local(*eid) == eid_local(b) && *himo_id == note && *value == 22),
    );
    let floored = !eng.sync_reclaimed_floors().unwrap_or_default().is_empty();
    drop(eng);
    Some(Outcome { relogged, distributed, floored })
}

fn run(shape: Shape) -> Outcome {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for attempt in 0..8 {
        if let Some(o) = once(shape, attempt) {
            return o;
        }
    }
    panic!("前提を作れなかった (8 回とも像の oplog に write の record があった)");
}

/// 対照: `flush_clean` を呼ばなければ、 本体にだけ届いた write は開く時に作り直される。
#[test]
fn control_unlogged_write_is_relogged() {
    let o = run(Shape::Control);
    assert!(o.relogged == 1 && o.distributed, "対照が崩れた: relogged={} distributed={}", o.relogged, o.distributed);
}

/// `flush_clean()` の後に書いた write が本体にだけ届いた像: 作り直されて配られること (か、 floor で覆われること)。
#[test]
fn unlogged_write_after_flush_clean_is_still_relogged() {
    let o = run(Shape::WriteAfterFlushClean);
    assert!(
        o.distributed || o.floored,
        "flush_clean() の後に書いて電源が落ちた: 本体にある write が配る分にも floor にも無い (relogged={}) — 相手に永久に届かない",
        o.relogged
    );
}

/// `flush_clean()` 自身が届かせた write (本体は書き出す、 oplog は fsync しない): その直後の電源断でも同じ。
#[test]
fn write_made_durable_only_by_flush_clean_is_still_relogged() {
    let o = run(Shape::FlushCleanAfterWrite);
    assert!(
        o.distributed || o.floored,
        "flush_clean() が本体にだけ届かせた write が、 配る分にも floor にも無い (relogged={}) — 相手に永久に届かない",
        o.relogged
    );
}

/// #101 は保つ: `flush_clean()` は辞書の索引の clean flag を立て、 落とさずに (Drop を通らずに) 終わった process の次の
/// open は辞書の索引を作り直さない。
#[test]
fn flush_clean_still_lets_the_next_open_skip_the_vocab_index_rebuild() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new("vocab_flag");
    let db = s.live().join("db");
    crashsim::start();
    {
        let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
        eng.define_table("notes", 1_000).unwrap();
        eng.define_himo_in("notes", "tag", ValueType::Tag, 0).unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        let e = eng.entity_in("notes").unwrap();
        eng.tie_text_to(e, "notes.tag", "a-word");
        eng.oplog_sync().unwrap();
        eng.flush_clean().unwrap();
        assert!(eng.vocab_index_is_clean(), "flush_clean の直後に辞書の clean flag が立っていない");
        // Drop を通らずに終わる (process の exit の代わりに、 書き出しの済んだ分の像を撮る)
        crashsim::capture(&s.live(), &s.img(), Mode::Lost).unwrap();
        crashsim::stop();
    }
    let back = Engine::open_concurrent_with_oplog(s.img().join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert!(!back.vocab_index_rebuilt_on_load(), "flush_clean の後の open が辞書の索引を作り直した (#101)");
    drop(back);
}
