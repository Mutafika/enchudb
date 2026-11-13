//! #813: process が落ちた (OS は生きている) 後に開いた DB は、 前の session が書き出さずに残した page も、 開いてから最初の
//! 本体の書き出しでディスクに届かせること。
//!
//! process が落ちても、 mmap に書いた page は page cache に残る (OS がいつか書く)。 開き直した process はその中身を読むが、
//! 書き出し (`body_msync`) は自分が印を付けた範囲だけを msync する。 版数の列のある DB (sync する DB) では、 再生は同じ HLC・同じ値の
//! Tie を書かない (#705 の規則) ので印も付かない。 前は、 その page を誰も msync しないまま WAL の checkpoint を進めていた:
//!
//! - 開き直した後に `oplog_sync()` が返っても、 OS が書き戻す前に電源が落ちると、 前の session の書き込み (WAL に載っていて、
//!   開いた後に読めていた値) が前の値に戻る (record は checkpoint の前 = もう再生されない)
//! - 語を回収する DB (`vocab_reclaim`) では、 落ちた後の数え直しが page cache の cell を数えるので、 ディスクの cell がまだ指して
//!   いる語の場所が 「参照 0」 で使い回され、 電源断の後その cell は読めない (前の値でもない)
//!
//! 像の作り方: crashsim の控えは process の中だけ。 そこで (1) 子 process が DB を作ってきれいに閉じ、 (2) 親が今の file の中身を
//! 全部 「書き出し済み」 として控え (`crashsim::file_synced`)、 (3) 別の子 process が開いて 1 行書き換え、 書き出さずに `_exit`
//! する (page cache には新しい値、 親の控えは前の値のまま = 「前の session が書き出さなかった page」)、 (4) 親が開いて書いて
//! `oplog_sync()` し、 電源断の像 (`Mode::Lost`) を撮って開く。 前提 (控えの上では前の値 / 開いた process からは新しい値 /
//! 像の WAL に、 その行の再生される record が無い) は試験の中で確かめる。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use enchudb_oplog::crashsim::{self, Mode};
use enchudb_oplog::oplog::{DecodedOp, OpLog};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;

/// crashsim の控えは process に 1 つなので、 試験は 1 本ずつ。
static SERIAL: Mutex<()> = Mutex::new(());
const ROLE_ENV: &str = "ENCHU_ISSUE813_ROLE";
const DB_ENV: &str = "ENCHU_ISSUE813_DB";
const OPLOG_CAP: usize = 4 * 1024 * 1024;
const OLD: &str = "old-value-written-out";
const NEW: &str = "new-value-left-in-the-page-cache";
const THIRD: &str = "a-third-word";
/// #841 の試験: 大きい表の数の列の数 (並べて書き出す thread の数 8 より多く) と、 小さい表の中の行の位置 (4 B の cell で 2 page 目)。
const NUM_COLS: usize = 10;
const S_ROW: usize = 4_500;
const NUM_OLD: u32 = 1;
const NUM_NEW: u32 = 2;
/// e0 と e1 の間の行の数 (cell 8 B × 16 KiB の page でも別の page になる)。
const FAR: usize = 20_000;

struct Scratch(PathBuf);
impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu_issue813_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(p.join("live")).unwrap();
        Scratch(p)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        crashsim::stop();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn child(role: &str, db: &Path) {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["child_entry", "--exact", "--nocapture", "--test-threads=1", "-q"])
        .env(ROLE_ENV, role)
        .env(DB_ENV, db)
        .output()
        .expect("spawn child");
    assert!(out.status.success(), "子 process ({role}) が落ちた: {}", String::from_utf8_lossy(&out.stderr));
}

fn ids_file(db: &str) -> String {
    format!("{db}.ids")
}

fn read_ids(db: &str) -> (u64, u64, u16) {
    let s = std::fs::read_to_string(ids_file(db)).unwrap();
    let mut it = s.split_whitespace().map(|x| x.parse::<u64>().unwrap());
    (it.next().unwrap(), it.next().unwrap(), it.next().unwrap() as u16)
}

fn read_all_ids(db: &str) -> Vec<u64> {
    std::fs::read_to_string(ids_file(db)).unwrap().split_whitespace().map(|x| x.parse::<u64>().unwrap()).collect()
}

/// 子 process の入口 (親が環境変数を付けて呼ぶ。 普通に回した時は何もしない)。
#[test]
fn child_entry() {
    let Ok(role) = std::env::var(ROLE_ENV) else { return };
    let db = std::env::var(DB_ENV).unwrap();
    match role.as_str() {
        // 版数の列のある DB (sync の表)。 行 e0 = OLD を書いて、 きれいに閉じる
        "build" | "build-reclaim" => {
            let mut eng = Engine::create_growable_opts(
                &db,
                GrowableOptions {
                    max_entities: 100_000,
                    vocab_reclaim: role == "build-reclaim",
                    column_pad: Some(false),
                    ..Default::default()
                },
            )
            .unwrap();
            eng.enable_sync_tables().unwrap();
            eng.define_table("t", 30_000).unwrap();
            let k = eng.define_himo_in("t", "k", ValueType::Tag, 0).unwrap() as u16;
            // e0 は e1 より後ろの、 離れた page に置く (列の書いた範囲の印は 1 本の区間なので、 親が e1 に書いた時の印が
            // e0 の page を覆わないように)
            let e1 = eng.entity_in("t").unwrap();
            let e0 = (0..FAR).map(|_| eng.entity_in("t").unwrap()).last().unwrap();
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
            eng.try_tie_text_to_by_id(e0, k, OLD).unwrap();
            eng.oplog_sync().unwrap();
            std::fs::write(ids_file(&db), format!("{e0} {e1} {k}")).unwrap();
            drop(eng);
        }
        // 開いて e0 を NEW に書き換え、 書き出さずに落ちる (page cache に残る)
        "write" => {
            let (e0, _, k) = read_ids(&db);
            let eng = Engine::open_concurrent_with_oplog(&db, OPLOG_CAP).unwrap();
            eng.try_tie_text_to_by_id(e0, k, NEW).unwrap();
            unsafe { libc::_exit(0) }
        }
        // #841: 中身を指さない列 (数の列) と、 その版数の列。 大きい表 t の列 `NUM_COLS` 本 (離れた行) と、 表 s の列 1 本
        // (segment が 2 page で、 header と別の page の行)
        "build-num" => {
            let mut eng = Engine::create_growable_opts(
                &db,
                GrowableOptions { max_entities: 100_000, column_pad: Some(false), ..Default::default() },
            )
            .unwrap();
            eng.enable_sync_tables().unwrap();
            // s を先に作る (eid が小さい = s の列の segment は小さいまま)
            eng.define_table("s", 6_000).unwrap();
            let m = eng.define_himo_in("s", "m", ValueType::Number, 0).unwrap();
            eng.define_table("t", 30_000).unwrap();
            let ns: Vec<u32> =
                (0..NUM_COLS).map(|c| eng.define_himo_in("t", &format!("n{c}"), ValueType::Number, 0).unwrap()).collect();
            // x0 は s の列の 2 page 目 (16 KiB の page でも 4 KiB の page でも、 header の page ではない)
            let x0 = (0..S_ROW).map(|_| eng.entity_in("s").unwrap()).last().unwrap();
            let e1 = eng.entity_in("t").unwrap();
            let e0 = (0..FAR).map(|_| eng.entity_in("t").unwrap()).last().unwrap();
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
            for &n in &ns {
                eng.try_tie_to_by_id(e0, n as u16, NUM_OLD).unwrap();
            }
            eng.try_tie_to_by_id(x0, m as u16, NUM_OLD).unwrap();
            eng.oplog_sync().unwrap();
            let ns: Vec<String> = ns.iter().map(|n| n.to_string()).collect();
            std::fs::write(ids_file(&db), format!("{e0} {e1} {x0} {m} {}", ns.join(" "))).unwrap();
            drop(eng);
        }
        "write-num" => {
            let ids = read_all_ids(&db);
            let eng = Engine::open_concurrent_with_oplog(&db, OPLOG_CAP).unwrap();
            for &n in &ids[4..] {
                eng.try_tie_to_by_id(ids[0], n as u16, NUM_NEW).unwrap();
            }
            eng.try_tie_to_by_id(ids[2], ids[3] as u16, NUM_NEW).unwrap();
            unsafe { libc::_exit(0) }
        }
        other => panic!("知らない役: {other}"),
    }
}

fn baseline(dir: &Path) {
    for ent in std::fs::read_dir(dir).unwrap() {
        let ent = ent.unwrap();
        if ent.file_type().unwrap().is_dir() {
            baseline(&ent.path());
        } else {
            crashsim::file_synced(&ent.path());
        }
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for ent in std::fs::read_dir(from).unwrap() {
        let ent = ent.unwrap();
        let dst = to.join(ent.file_name());
        if ent.file_type().unwrap().is_dir() {
            copy_dir(&ent.path(), &dst);
        } else {
            std::fs::copy(ent.path(), dst).unwrap();
        }
    }
}

fn text(eng: &Engine, e: u64) -> Option<String> {
    eng.get_text_owned(e, "t.k").map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn run(reclaim: bool) {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new(if reclaim { "reclaim" } else { "plain" });
    let live = s.0.join("live");
    let db = live.join("db");
    let db_str = db.to_str().unwrap();
    child(if reclaim { "build-reclaim" } else { "build" }, &db);
    let (e0, e1, k) = read_ids(db_str);

    // 今の file の中身 = ディスクに届いている中身、 として控える
    crashsim::start();
    baseline(&live);
    child("write", &db);

    // 前提 1: 控えの上では e0 は OLD (子の書き換えは 「書き出していない page」)
    let img0 = s.0.join("img0");
    crashsim::capture(&live, &img0, Mode::Lost).unwrap();
    {
        let probe = Engine::open_standalone(img0.join("db").to_str().unwrap()).unwrap();
        assert_eq!(text(&probe, e0).as_deref(), Some(OLD), "前提: 控え (ディスク) の上の e0 は前の値");
    }

    // 落ちた後に開く。 前提 2: 開いた process からは NEW が読める (page cache)
    let eng = Engine::open_concurrent_with_oplog(db_str, OPLOG_CAP).unwrap();
    assert_eq!(text(&eng, e0).as_deref(), Some(NEW), "前提: 開いた process からは新しい値が読める");
    let before = eng.vocab_usage();
    eng.try_tie_text_to_by_id(e1, k, THIRD).unwrap();
    if reclaim {
        assert_eq!(eng.vocab_usage().reclaimed, before.reclaimed + 1, "前提: 新しい語が、 空いた場所 (OLD の場所) を使い回した");
    }
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    assert_eq!(text(&eng, e0).as_deref(), Some(NEW));

    // oplog_sync() が返った後の電源断
    let img = s.0.join("img");
    crashsim::capture(&live, &img, Mode::Lost).unwrap();
    crashsim::stop();
    drop(eng);

    // 前提 3: 像の WAL に、 e0 の再生される record が無い (あれば再生が NEW を当て直す)
    let probe_dir = s.0.join("probe");
    copy_dir(&img, &probe_dir);
    let recs = OpLog::open(&probe_dir.join("db").join("oplog")).unwrap().recover_with_tail();
    let e0_local = e0 & 0xffff_ffff;
    assert!(
        !recs.iter().any(|r| matches!(&r.op, DecodedOp::Tie { eid, .. } if eid & 0xffff_ffff == e0_local)),
        "前提: 像の WAL に e0 の record が残っている (checkpoint が進んでいない)"
    );

    let back = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(
        text(&back, e0).as_deref(),
        Some(NEW),
        "落ちた後に開いて oplog_sync() が返った後の電源断で、 前の session の書き込みが消えた (回収 = {reclaim})"
    );
    assert_eq!(text(&back, e1).as_deref(), Some(THIRD));
    drop(back);
}

#[test]
fn previous_sessions_pages_reach_disk_before_the_checkpoint_moves() {
    run(false);
}

#[test]
fn previous_sessions_pages_reach_disk_before_a_reclaimed_slot_is_reused() {
    run(true);
}

/// #841: 中身を指さない列 (数の列) と版数の列も同じ。 #813 の直しは、 きれいに閉じなかった後の最初の書き出しで全部の segment を
/// 全域で書き出す。 上の 2 本は Tag の列 (中身を指す列の段) しか通らないので、 残りの段 (中身を指さない列 → 版数の列) を通す。
/// その段は大きい segment を thread に分けて書き出す (`SegmentSet::flush_all_spread`) ので、 thread の数より多い大きい segment
/// (t の列 `NUM_COLS` 本とその版数の列) と、 呼んだ thread で書く小さい segment (s の列。 16 KiB の page の OS で 2 page) の
/// 両方に、 前の session の page を置く。
#[test]
fn previous_sessions_number_cells_and_versions_reach_disk_before_the_checkpoint_moves() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new("num");
    let live = s.0.join("live");
    let db = live.join("db");
    let db_str = db.to_str().unwrap();
    child("build-num", &db);
    let ids = read_all_ids(db_str);
    let (e0, e1, x0, m) = (ids[0], ids[1], ids[2], ids[3] as u16);
    let ns: Vec<u16> = ids[4..].iter().map(|&n| n as u16).collect();
    assert_eq!(ns.len(), NUM_COLS);

    crashsim::start();
    baseline(&live);
    child("write-num", &db);

    // 前提 1: 控えの上では、 どれも前の値
    let img0 = s.0.join("img0");
    crashsim::capture(&live, &img0, Mode::Lost).unwrap();
    {
        let probe = Engine::open_standalone(img0.join("db").to_str().unwrap()).unwrap();
        for &n in &ns {
            assert_eq!(probe.get_by_id(e0, n), Some(NUM_OLD as u64), "前提: 控えの上の e0 は前の値");
        }
        assert_eq!(probe.get_by_id(x0, m), Some(NUM_OLD as u64), "前提: 控えの上の x0 は前の値");
    }

    // 前提 2: 落ちた後に開いた process からは新しい値が読める (page cache)
    let eng = Engine::open_concurrent_with_oplog(db_str, OPLOG_CAP).unwrap();
    for &n in &ns {
        assert_eq!(eng.get_by_id(e0, n), Some(NUM_NEW as u64), "前提: 開いた process からは新しい値が読める");
    }
    assert_eq!(eng.get_by_id(x0, m), Some(NUM_NEW as u64));
    let v_e0: Vec<_> = ns.iter().map(|&n| eng.cell_hlc(e0, n)).collect();
    let v_x0 = eng.cell_hlc(x0, m);
    assert!(v_e0.iter().all(|v| *v != enchudb_oplog::Hlc::ZERO) && v_x0 != enchudb_oplog::Hlc::ZERO, "前提: 版数の列がある");
    eng.try_tie_to_by_id(e1, ns[0], 7u32).unwrap();
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();

    let img = s.0.join("img");
    crashsim::capture(&live, &img, Mode::Lost).unwrap();
    crashsim::stop();
    drop(eng);

    // 前提 3: 像の WAL に、 e0 / x0 の再生される record が無い (あれば再生が当て直す)
    let probe_dir = s.0.join("probe");
    copy_dir(&img, &probe_dir);
    let recs = OpLog::open(&probe_dir.join("db").join("oplog")).unwrap().recover_with_tail();
    let locals = [e0 & 0xffff_ffff, x0 & 0xffff_ffff];
    assert!(
        !recs.iter().any(|r| matches!(&r.op, DecodedOp::Tie { eid, .. } if locals.contains(&(eid & 0xffff_ffff)))),
        "前提: 像の WAL に e0 / x0 の record が残っている (checkpoint が進んでいない)"
    );

    let back = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    for (c, &n) in ns.iter().enumerate() {
        assert_eq!(back.get_by_id(e0, n), Some(NUM_NEW as u64), "大きい表の数の列 n{c}: 前の session の書き込みが消えた");
        assert_eq!(back.cell_hlc(e0, n), v_e0[c], "大きい表の版数の列 n{c}: 前の session の版数が消えた");
    }
    assert_eq!(back.get_by_id(x0, m), Some(NUM_NEW as u64), "小さい表の数の列: 前の session の書き込みが消えた");
    assert_eq!(back.cell_hlc(x0, m), v_x0, "小さい表の版数の列: 前の session の版数が消えた");
    assert_eq!(back.get_by_id(e1, ns[0]), Some(7));
    drop(back);
}
