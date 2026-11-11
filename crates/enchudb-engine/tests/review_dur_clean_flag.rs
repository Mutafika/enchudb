//! review (0.37.4): 「きれいに閉じた」 の印 (紐名の辞書の clean flag) に頼る開く時の短縮が、 飛ばしてはいけない物を飛ばす道。
//!
//! 1. (bug) `flush_clean()` (`&self`、 公開 API) は session の途中で紐名の辞書に clean の印を立て直す。 その後に書いて電源が
//!    落ちると、 次の open は 「きれいに閉じた」 と読み、 本体にあって oplog に無い write の照合 (#451 `relog_unlogged_writes`) を飛ばす。
//! 2. (正しかった) きれいに閉じた後に開く時の HLC の種。 時計の進んだ相手の record を受けた直後に書いて、 次の切れ目の前に閉じても、
//!    閉じる時の最後の Commit marker が時計を WAL に残すので、 開き直した後のローカルの書き込みは弾かれない。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::{decode_sync_ops_payload, DecodedOp, OpLog};
use enchudb_oplog::{eid_local, Hlc};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const OPLOG_CAP: usize = 4 * 1024 * 1024;
static SERIAL: Mutex<()> = Mutex::new(());

fn scratch(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("enchu_review_dur_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn notes_engine(db: &Path, sync: bool) -> Arc<Engine> {
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("notes", 1_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    eng
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

/// 1 回分。 `Some(relogged, distributed)`、 前提を作れなかった (consumer が先に oplog を届かせた) 時は None。
fn unlogged_write_once(call_flush_clean: bool, attempt: usize) -> Option<(u64, bool)> {
    let root = scratch(&format!("fc{}_{attempt}", call_flush_clean as u8));
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let eng = notes_engine(&live.join("db"), true);
    let [a, b] = [(); 2].map(|_| eng.entity_in("notes").unwrap());
    eng.tie_to(a, "notes.note", 1);
    eng.oplog_sync().unwrap();
    if call_flush_clean {
        // session の途中の clean-flush (公開 API)。 この後も書き続ける
        eng.flush_clean().unwrap();
    }
    let held = with_stalled_sync(&eng, || {
        // oplog の fsync の後・本体の書き出しの前の write = 本体にだけ届く
        eng.tie_to(b, "notes.note", 22);
    });
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(held);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let before = eng.cell_hlc(b, note);
    drop(eng);
    captured.unwrap();
    let db = img.join("db");
    if OpLog::open(&db.join("oplog")).unwrap().records_with_tail().iter().any(|r| r.hlc == before) {
        let _ = std::fs::remove_dir_all(&root);
        return None;
    }
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.get(b, "notes.note"), Some(22), "前提: 本体に届いている");
    let relogged = eng.unlogged_writes_relogged();
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    let recs: Vec<_> = eng.pending_sync_ops(0).iter().filter_map(|p| decode_sync_ops_payload(p)).collect();
    let distributed = recs.iter().any(
        |r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, value } if eid_local(*eid) == eid_local(b) && *himo_id == note && *value == 22),
    );
    let floors = eng.sync_reclaimed_floors().unwrap_or_default();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[review_dur] flush_clean={call_flush_clean}: relogged={relogged} distributed={distributed} floors={floors:?}");
    Some((relogged, distributed || !floors.is_empty()))
}

fn unlogged_write(call_flush_clean: bool) -> (u64, bool) {
    for attempt in 0..8 {
        if let Some(r) = unlogged_write_once(call_flush_clean, attempt) {
            return r;
        }
    }
    panic!("前提を作れなかった");
}

/// 対照: flush_clean を呼ばなければ作り直す。
#[test]
fn control_unlogged_write_is_relogged() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (relogged, covered) = unlogged_write(false);
    assert!(relogged == 1 && covered, "対照が崩れた: relogged={relogged} covered={covered}");
}

/// flush_clean の後に書いた write が本体にだけ届いた像: 作り直されるか floor で覆われること。
#[test]
fn unlogged_write_after_flush_clean_is_still_relogged() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (relogged, covered) = unlogged_write(true);
    assert!(
        covered,
        "flush_clean() の後に書いて電源が落ちた: 本体にある write (b.note = 22) が配る分にも floor にも無い (relogged={relogged}) — 相手に永久に届かない"
    );
}

/// power_loss.rs の `hold_consumer` と同じ: consumer を周期の oplog の書き出し (写した直後) で止める。
fn hold_consumer(eng: &Arc<Engine>) -> ConsumerHeld {
    let (parked_tx, parked_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
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
    parked_rx.recv_timeout(std::time::Duration::from_secs(20)).expect("前提: consumer が止まる");
    ConsumerHeld(Some(release_tx))
}

/// 時計を戻さない形 (set_cell で偽らない): 時計が 1 時間進んだ相手の record を受けた (= `observe_hlc`、 受信の apply が必ず通す)
/// 直後にローカルで書き、 次の周期の切れ目が置かれる前にきれいに閉じる。 開き直した後、 同じ cell へのローカルの書き込み。
/// consumer は周期の書き出しの途中 (切れ目を取った後) で止めておき、 閉じ始めてから放す = 書いた後に新しい切れ目は取られない。
#[test]
fn local_write_after_clean_close_following_a_peer_with_a_fast_clock() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("seed_peer_clock");
    let db = root.join("db");
    crashsim::start();
    let eng = notes_engine(&db, true);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", 1);
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    let held = hold_consumer(&eng);
    let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    // 相手 (peer 2) の時計は 1 時間先。 その record を当てた時の時計の merge
    eng.oplog().unwrap().observe_hlc(Hlc { wall: wall + 3_600_000, logical: 0, peer: 2 });
    eng.tie_to(e, "notes.note", 5);
    let written = eng.cell_hlc(e, note);
    assert!(written.wall >= wall + 3_600_000 && written.peer == 1, "前提: ローカルの書き込みは merge した時計で採番: {written:?}");
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(1));
        drop(held);
    });
    drop(eng); // きれいに閉じる (consumer の最後の書き出し + clean の印)
    releaser.join().unwrap();
    crashsim::stop();
    let back = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    let mark = back.durable_mark();
    assert_eq!(back.get(e, "notes.note"), Some(5), "前提: 閉じる前の書き込みは残っている");
    assert_eq!(back.cell_hlc(e, note), written);
    let r = back.try_tie_to_by_id(e, note, 6u32).map_err(|e| format!("{e:?}"));
    let got = back.get(e, "notes.note");
    drop(back);
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[review_dur] fast peer clock: 印 {mark:?}, cell {written:?}, tie = {r:?}, get = {got:?}");
    assert!(r.is_ok() && got == Some(6), "きれいに閉じて開き直した後、 自分が最後に書いた cell を書き換えられない: {r:?} (get {got:?}、 印 {mark:?}、 cell {written:?})");
}
