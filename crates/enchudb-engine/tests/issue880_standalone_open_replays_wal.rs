//! #880: WAL 付きで書いて落ちた DB を単独 (`Engine::open_standalone` = schema の `Database::open`) で開いても、 WAL 付きで開いた時と
//! 同じ中身になる (先に WAL 付きの open で再生して閉じてから開き直す)。
//!
//! 前は単独の open が WAL を見ず:
//! - A: WAL の fsync の後・本体の列を書き出す前に落ちた像で、 WAL にだけある値を黙って失った
//! - B: #857 の止め (消した行が sidecar より先、 checkpoint は止まったまま) の像で、 消した行の eid をもう一度払い出した
//! - D: sync する DB をきれいに閉じずに落とした後、 WAL に再生する record が無いと往復せず、 単独で閉じると clean の印が立って、
//!   次の WAL 付きの open が本体にあって WAL に無い write の作り直し (#451) を飛ばした (その write が相手に配られない)
//! - E: D の往復が閉じる時に clean の印を立てられなかったら (I/O の故障)、 単独の open は断る (開いて閉じると印が立って D に戻る)

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::engine::write_out_hook::{self, Phase};
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::eid_local;
use enchudb_oplog::oplog::{decode_sync_ops_payload, DecodedOp, OpLog};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// crashsim の控えと書き出しの hook は process に 1 つなので、 試験を 1 本ずつ回す。
static SERIAL: Mutex<()> = Mutex::new(());

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(tag: &str) -> Scratch {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = std::env::temp_dir().join(format!("enchudb-issue880-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(p.join("live")).unwrap();
    Scratch(p)
}

fn copy(from: &Path, to: &Path) {
    assert!(std::process::Command::new("cp").arg("-R").arg(from).arg(to).status().unwrap().success());
}

/// sync しない DB、 表 `t` (列 a)。
fn engine(db: &Path) -> (Arc<Engine>, u16) {
    let mut eng = Engine::create_growable_with_capacity(db.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let a = eng.himo_id("t.a").unwrap() as u16;
    (eng, a)
}

/// 像を写して 2 通りで開き、 (行 x の値, 次に払い出す eid) を返す。
fn open_both(image: &Path, x: u64, a: u16) -> ((Option<u64>, u64), (Option<u64>, u64)) {
    let other = image.with_extension("concurrent");
    copy(image, &other);
    let mut s = Engine::open_standalone(image.join("db").to_str().unwrap()).unwrap();
    let standalone = (s.get_by_id(x, a), s.entity_in("t").unwrap());
    s.flush().unwrap();
    drop(s);
    let c = Engine::open_concurrent_with_oplog(other.join("db").to_str().unwrap(), 4 << 20).unwrap();
    let concurrent = (c.get_by_id(x, a), c.entity_in("t").unwrap());
    (standalone, concurrent)
}

/// A + C: WAL にだけある値を、 単独の open も見る。 その後に単独で書いた値は、 次の WAL 付きの open で古い record に戻らない。
#[test]
fn a_value_only_in_the_wal_survives_a_standalone_open() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("walonly");
    let live = s.0.join("live");
    crashsim::start();
    let (eng, a) = engine(&live.join("db"));
    let hold = eng.hold_consumer();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 1u32);
    eng.oplog_sync().unwrap();
    eng.tie_to_by_id(x, a, 2u32);
    // 次の書き出しの、 WAL を fsync した後・本体の列を書き出す前の像
    let image = s.0.join("img");
    let shot = Arc::new(AtomicBool::new(false));
    {
        let (live, image, shot) = (live.clone(), image.clone(), shot.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !shot.swap(true, Ordering::AcqRel) {
                crashsim::capture(&live, &image, Mode::Lost).unwrap();
            }
        })));
    }
    eng.oplog_sync().unwrap();
    write_out_hook::set(None);
    drop(hold);
    drop(eng);
    crashsim::stop();
    assert!(shot.load(Ordering::Acquire), "前提: 像を撮った");
    let c_image = s.0.join("img_c");
    copy(&image, &c_image);
    let (standalone, concurrent) = open_both(&image, x, a);
    assert_eq!(concurrent.0, Some(2), "前提: WAL 付きの open は WAL にだけある 2 を再生する");
    assert_eq!(standalone.0, Some(2), "単独の open が WAL にだけある値を失った");
    // C: 単独で開いて 5 を書いて閉じ、 その後 WAL 付きで開く
    {
        let mut st = Engine::open_standalone(c_image.join("db").to_str().unwrap()).unwrap();
        assert_eq!(st.get_by_id(x, a), Some(2));
        st.tie(x, "t.a", 5u32);
        st.flush().unwrap();
    }
    let c = Engine::open_concurrent_with_oplog(c_image.join("db").to_str().unwrap(), 4 << 20).unwrap();
    assert_eq!(c.get_by_id(x, a), Some(5), "単独で書いた値が、 次の WAL 付きの open で戻った");
}

/// B: #857 の止めで残した Delete を、 単独の open も再生する (消した行の eid をもう一度払い出さない)。
#[test]
fn a_held_delete_is_replayed_by_a_standalone_open() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("held");
    let live = s.0.join("live");
    let (eng, a) = engine(&live.join("db"));
    let hold = eng.hold_consumer();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 1u32);
    eng.delete(x);
    eng.oplog_sync().unwrap();
    let image = s.0.join("img");
    copy(&live, &image);
    drop(hold);
    drop(eng);
    let (standalone, concurrent) = open_both(&image, x, a);
    assert_ne!(concurrent.1, x, "前提: WAL 付きの open は消した行の eid を払い出さない");
    assert_eq!(standalone, concurrent, "単独の open が WAL 付きの open と違う (消した行 {x:#x})");
}

/// きれいに閉じた DB は、 単独の open が WAL 付きの open を通らない (WAL が空)。 中身も今までどおり。
#[test]
fn a_cleanly_closed_db_opens_standalone_as_before() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("clean");
    let db = s.0.join("live").join("db");
    let x = {
        let (eng, a) = engine(&db);
        let x = eng.entity_in("t").unwrap();
        eng.tie_to_by_id(x, a, 3u32);
        x
    };
    let oplog = db.join("oplog");
    let before = std::fs::metadata(&oplog).unwrap().modified().unwrap();
    let st = Engine::open_standalone(db.to_str().unwrap()).unwrap();
    assert_eq!(st.get_by_id(x, st.himo_id("t.a").unwrap() as u16), Some(3));
    drop(st);
    assert_eq!(std::fs::metadata(&oplog).unwrap().modified().unwrap(), before, "きれいに閉じた DB で WAL 付きの open を通った (WAL が書かれた)");
}

/// 再生して閉じても WAL に record が残る時 (`tables` を書けず #870 の止めで checkpoint を進められない) は、 単独の open は断る
/// (黙って WAL を見ずに開かない)。 書けるように戻せば開ける。
#[test]
fn a_standalone_open_refuses_when_the_wal_cannot_be_written_out() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("refuse");
    let live = s.0.join("live");
    let (eng, a) = engine(&live.join("db"));
    let hold = eng.hold_consumer();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 1u32);
    eng.delete(x);
    eng.oplog_sync().unwrap();
    let image = s.0.join("img");
    copy(&live, &image);
    drop(hold);
    drop(eng);
    // `tables` を書けない (tmp の置き場に directory)
    let db = image.join("db");
    std::fs::create_dir(db.join("tables.tmp")).unwrap();
    match Engine::open_standalone(db.to_str().unwrap()) {
        Err(e) => assert!(e.to_string().contains("#880"), "断った理由: {e}"),
        Ok(_) => panic!("WAL を書き出せないのに単独で開いた"),
    }
    std::fs::remove_dir(db.join("tables.tmp")).unwrap();
    let st = Engine::open_standalone(db.to_str().unwrap()).expect("書けるように戻したのに開けない");
    assert_ne!(st.entity_in("t").unwrap(), x);
}

/// sync する DB (版数の列あり)、 表 `notes` (列 note)、 peer 1。
fn sync_engine(db: &Path) -> (Arc<Engine>, u16) {
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("notes", 1_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    eng.set_peer_id(1);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    (eng, note)
}

/// `oplog_sync` を別の thread で、 oplog を写した (fsync) 直後・本体を書き出す前に止め、 その間に `during` を走らせてから放す
/// (止めた方の本体の書き出しが `during` の write を本体に届かせる。 oplog の写しには載らない)。 consumer は呼び手が止めておく。
fn with_stalled_sync(eng: &Arc<Engine>, during: impl FnOnce()) {
    let (copied_tx, copied_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let stall = Mutex::new(Some((copied_tx, go_rx)));
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
        if what == Copied::Oplog && std::thread::current().name() == Some("stalled-sync") {
            let taken = stall.lock().unwrap().take();
            if let Some((copied, go)) = taken {
                copied.send(()).unwrap();
                go.recv().unwrap();
            }
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
    crashsim::set_after_copy_hook(None);
}

/// 配る分 (bridge し直した後の `_sync_ops`) にある、 行 `x` の列 `note` に `value` を置く Tie の HLC。
fn distributed_tie(eng: &Engine, x: u64, note: u16, value: u64) -> Option<enchudb_oplog::Hlc> {
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    eng.pending_sync_ops(0).iter().filter_map(|p| decode_sync_ops_payload(p)).find_map(|r| match r.op {
        DecodedOp::Tie { eid, himo_id, value: v } if eid_local(eid) == eid_local(x) && himo_id == note && v == value => Some(r.hlc),
        _ => None,
    })
}

/// sync する DB の、 本体に届いて WAL に無い write (oplog の fsync と本体の書き出しの間に書いた行 b の note = 22) を持つ電源断の像。
/// 行 a の note = 1 は書き出し済み。 像の WAL に再生する record は無い。
struct Unlogged {
    _s: Scratch,
    /// 像の DB
    db: PathBuf,
    a: u64,
    b: u64,
    note: u16,
    /// b の write の版数 (像を撮った時)
    written_at: enchudb_oplog::Hlc,
}

/// [`Unlogged`] を撮る。 前提を像の状態で確かめる (WAL に再生する record が無い = 前の形の往復の条件に当たらない・b の write の record も
/// 無い・単独を挟まずに WAL 付きで開けば作り直す)。 consumer の書き出しが先に WAL を届かせた回は撮り直す (5 回まで)。
fn unlogged_image(tag: &str) -> Unlogged {
    for attempt in 0..5 {
        let s = scratch(&format!("{tag}{attempt}"));
        let live = s.0.join("live");
        crashsim::start();
        let (eng, note) = sync_engine(&live.join("db"));
        let hold = eng.hold_consumer();
        let [a, b] = [(); 2].map(|_| eng.entity_in("notes").unwrap());
        eng.tie_to_by_id(a, note, 1u32);
        eng.oplog_sync().unwrap();
        with_stalled_sync(&eng, || eng.tie_to_by_id(b, note, 22u32));
        let image = s.0.join("img");
        let captured = crashsim::capture(&live, &image, Mode::Lost);
        crashsim::stop();
        let written_at = eng.cell_hlc(b, note);
        drop(hold);
        drop(eng);
        captured.unwrap();
        let db = image.join("db");
        let wal = OpLog::open(&db.join("oplog")).unwrap();
        let (to_replay, b_in_wal) =
            (wal.recover_with_tail().len(), wal.records_with_tail().iter().any(|r| r.hlc == written_at));
        drop(wal);
        if to_replay > 0 || b_in_wal {
            eprintln!("[{tag}] 撮り直す ({attempt} 回目): 像の WAL に record が残った ({to_replay} 件、 b: {b_in_wal})");
            continue;
        }
        let direct = s.0.join("img_direct");
        copy(&image, &direct);
        {
            let c = Engine::open_concurrent_with_oplog(direct.join("db").to_str().unwrap(), 4 << 20).unwrap();
            assert_eq!(c.unlogged_writes_relogged(), 1, "前提: 本体にあって WAL に無い write が 1 つある像");
            assert!(distributed_tie(&c, b, note, 22).is_some(), "前提: 単独を挟まなければ配る分に載る");
        }
        std::fs::remove_dir_all(&direct).unwrap();
        return Unlogged { _s: s, db, a, b, note, written_at };
    }
    panic!("5 回とも像の WAL に record が残り、 前提を作れなかった");
}

/// 本体にだけあった b の write が相手に届く道。
#[derive(Debug)]
enum Delivery {
    /// 作り直した record が配る分 (`_sync_ops`) にある (HLC は cell の版数と同じ)
    Relogged(enchudb_oplog::Hlc),
    /// WAL に載せられず、 floor を b の版数より上に上げた (相手は取り直して live state ごと受ける。 `relog_unlogged_writes` の逃げ道)
    Floor(enchudb_oplog::Hlc),
}

/// `db` を WAL 付きで開き、 何も失っていない (a = 1・b = 22、 b の write は配る分にあるか floor が覆う) ことを見る。
fn assert_nothing_lost(db: &Path, u: &Unlogged, what: &str) -> Delivery {
    let c = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), 4 << 20).unwrap();
    assert_eq!((c.get_by_id(u.a, u.note), c.get_by_id(u.b, u.note)), (Some(1), Some(22)), "{what}: 値を失った");
    let version = c.cell_hlc(u.b, u.note);
    if let Some(hlc) = distributed_tie(&c, u.b, u.note, 22) {
        assert_eq!(hlc, version, "{what}: 配る record の HLC が cell の版数と違う");
        return Delivery::Relogged(hlc);
    }
    let floor = c.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == 1).map(|(_, h)| h);
    match floor {
        Some(f) if f >= version => Delivery::Floor(f),
        _ => panic!("{what}: 本体にだけあった write が配る分に無く、 floor ({floor:?}) も覆わない (版数 {version:?})"),
    }
}

/// D: sync する DB で、 本体に届いて WAL に無い write (oplog の fsync と本体の書き出しの間に書いた) を持つ電源断の像を、 単独で
/// 開いて閉じた後に WAL 付きで開いても、 その write が配る分 (`_sync_ops`) に載る。 像の WAL に再生する record は無い (前の形の
/// 往復の条件に当たらない)。 前は単独で閉じると clean の印が立ち、 WAL 付きの open が #451 の作り直しを飛ばした。
#[test]
fn a_write_only_on_disk_is_relogged_after_a_standalone_session() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let u = unlogged_image("relog");
    // 単独で開いて閉じる
    {
        let st = Engine::open_standalone(u.db.to_str().unwrap()).unwrap();
        assert_eq!(st.get_by_id(u.b, u.note), Some(22), "前提: 本体に届いている");
    }
    match assert_nothing_lost(&u.db, &u, "単独で開いて閉じた後") {
        Delivery::Relogged(hlc) => assert!(hlc > u.written_at, "作り直した record は新しい HLC"),
        d => panic!("作り直さずに floor で済ませた: {d:?}"),
    }
}

/// きれいに閉じた sync する DB は、 単独の open が WAL 付きの open を通らない (印が立っている)。
#[test]
fn a_cleanly_closed_sync_db_opens_standalone_as_before() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("cleansync");
    let db = s.0.join("live").join("db");
    let (x, note) = {
        let (eng, note) = sync_engine(&db);
        let x = eng.entity_in("notes").unwrap();
        eng.tie_to_by_id(x, note, 3u32);
        (x, note)
    };
    let oplog = db.join("oplog");
    let before = std::fs::metadata(&oplog).unwrap().modified().unwrap();
    let st = Engine::open_standalone(db.to_str().unwrap()).unwrap();
    assert_eq!(st.get_by_id(x, note), Some(3));
    drop(st);
    assert_eq!(std::fs::metadata(&oplog).unwrap().modified().unwrap(), before, "きれいに閉じた sync の DB で WAL 付きの open を通った");
}

/// 子の DB (親が [`a_standalone_open_refuses_when_the_round_trip_cannot_close_cleanly`] で渡す)
const CHILD_DB_ENV: &str = "ENCHU_ISSUE880_CHILD_DB";

/// 子: `ENCHU_ISSUE880_CHILD_DB` の DB を単独で開き (I/O の故障は親が `ENCHU_FAULT_*` で選ぶ)、 結果を 1 行出す。 開けたら閉じる
/// (利用者と同じ)。 親が呼ばない時は何もしない。
#[test]
fn child_open_standalone() {
    let Ok(db) = std::env::var(CHILD_DB_ENV) else { return };
    let line = match Engine::open_standalone(&db) {
        Ok(st) => format!("OK {}", st.himo_index_rebuilt_on_load()),
        Err(e) => format!("ERR {e}"),
    };
    println!("ISSUE880 {line}");
}

/// [`child_open_standalone`] を別の process で回す。 戻り値: (結果の行, 起こした故障の行 (`ENCHU_FAULT` の後ろ))。
fn run_child(db: &Path, envs: &[(&str, String)]) -> (String, Option<String>) {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_open_standalone", "--nocapture", "--test-threads=1"])
        .env(CHILD_DB_ENV, db)
        .envs(envs.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .unwrap();
    let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let fired = stderr.lines().find_map(|l| l.split_once("ENCHU_FAULT ").map(|(_, r)| r.to_string()));
    if !out.status.success() {
        // 開いた後に初めて触る segment を開けない時は panic する (`SegmentSet::lazy_slot` の doc、 決めてある形)
        assert!(stderr.contains("could not be opened on first use"), "子が落ちた ({envs:?}): {stderr}");
        return ("PANIC lazy segment open".to_string(), fired);
    }
    let line = stdout
        .lines()
        .find_map(|l| l.split_once("ISSUE880 ").map(|(_, r)| r))
        .unwrap_or_else(|| panic!("子の結果が無い: {stdout}\n{stderr}"))
        .to_string();
    (line, fired)
}

/// 往復 (D の WAL 付きの open) が閉じる時に clean の印を立てられなかったら、 単独の open は断る (開いたまま閉じると印が立ち、 次の WAL
/// 付きの open が #451 の作り直しを飛ばす形に戻る)。 断った後も何も失っておらず、 開き直せば開ける。
///
/// 印を立てられない像は状態では作れない (`tables` を書けなくする #870 の形では、 往復は印を立てて閉じる: 閉じる時の `tables` の書き込みは
/// 失敗しても印を止めない。 印を止めるのは本体の書き出し (`write_out_body` / payload の ring) の失敗と consumer の panic だけ)。 なので
/// I/O の故障 (crashsim の `fault`、 子 process) で作る: 往復の WAL 付きの open が WAL を開いた後から、 往復の後に WAL を見直す前まで
/// (数えるだけの回の `oplog.open` の 2 回目と 3 回目の間) の点ごとに 1 回 EIO を起こす。 consumer の thread も同じ数を進めるので、
/// 回ごとに当たる点は少しずれうる (窓の点を全部回す)。
#[test]
fn a_standalone_open_refuses_when_the_round_trip_cannot_close_cleanly() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let u = unlogged_image("still");
    let root = u._s.0.clone();
    // 数えるだけの回 (故障なし)
    let counted = root.join("counted");
    copy(&u.db, &counted);
    let trace = root.join("trace.txt");
    let (line, _) = run_child(&counted, &[("ENCHU_FAULT_TRACE", trace.to_str().unwrap().to_string())]);
    assert_eq!(line, "OK false", "前提: 故障が無ければ往復して開ける");
    let sites: Vec<(u64, String)> = std::fs::read_to_string(&trace)
        .unwrap()
        .lines()
        .filter_map(|l| l.split_once('\t').map(|(n, s)| (n.parse().unwrap(), s.to_string())))
        .collect();
    let opens: Vec<u64> = sites.iter().filter(|(_, s)| s == "oplog.open").map(|(n, _)| *n).collect();
    assert!(opens.len() >= 3, "前提: 単独の open が WAL を 3 回開く (往復の前の見直し・往復・往復の後の見直し): {opens:?}");
    let window = opens[1] + 1..opens[2];
    let (mut refused, mut opened_unclean, mut other) = (Vec::new(), Vec::new(), Vec::new());
    for n in window.clone() {
        let img = root.join(format!("n{n}"));
        copy(&u.db, &img);
        let (line, fired) = run_child(&img, &[("ENCHU_FAULT_AT", n.to_string())]);
        if line == "OK true" {
            opened_unclean.push((n, fired.clone()));
        } else if line.contains("still marked as not cleanly closed") {
            refused.push((n, fired.clone()));
            // 断った後は、 開き直せば開ける
            let st = Engine::open_standalone(img.to_str().unwrap()).expect("断った後に開き直せない");
            assert!(!st.himo_index_rebuilt_on_load());
        } else {
            other.push((n, line.clone()));
        }
        assert_nothing_lost(&img, &u, &format!("点 {n} ({fired:?}、 {line})"));
        std::fs::remove_dir_all(&img).unwrap();
    }
    println!("[still] 窓 {window:?}: 断った {} / それ以外 {other:?}", refused.len());
    assert!(
        opened_unclean.is_empty(),
        "往復が clean の印を立てられなかったのに、 単独の open が開いた (閉じると印が立つ): {opened_unclean:?}"
    );
    assert!(!refused.is_empty(), "前提: 窓のどの点の故障でも、 往復が clean の印を立てられない形にならなかった");
}
