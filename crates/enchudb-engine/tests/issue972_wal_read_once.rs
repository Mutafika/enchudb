//! #972: きれいに閉じなかった後に開く時、 WAL を全部解くのは本当の再生 (WAL 付きの open の `recover_with_tail`) の 1 回だけ。
//! 「再生する record があるか」 を聞く道は、 最初の当てる record で止まる: 単独の open が writer lock の下で聞く所 (#880 の往復の前と後)
//! は WAL を読むだけで開いて聞き (`OpLog::probe_replay`)、 WAL 付きの open は開いた WAL に聞く (`OpLog::has_records_to_replay`、
//! `reconcile_generations` の仕上げと `restore_prev_residents` が要る時に 1 回。 PR #986 の読み直し 2 の [2])。
//!
//! 前は聞くたびに WAL を読み書きで開いて全部を解き (`OpLog::open` + `recover_with_tail`)、 単独の open (`open_standalone`) は
//! 1 回の open で 2〜5 回 WAL を全部読んだ (256 MiB の WAL で 1 回 0.3〜1 秒)。
//!
//! 回数は `enchudb_oplog::oplog::wal_reads` (thread ごとの数え口。 開く道は呼んだ thread で読む) で数える。 像は consumer を
//! 止めて書いた DB の写し (page cache ごと = kill の後の形)。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::{WalReads, wal_reads};
use std::path::Path;
use std::sync::Arc;

const WAL: usize = 4 * 1024 * 1024;
/// 像の WAL に積む行の数 (全部を解く読みと、 最初の 1 件で止まる読みを見分けられる数)
const ROWS: u32 = 2000;
/// 聞く読み 1 回が読んでよい record の数の上限 (Commit 数件と、 見つけた 1 件)
const PROBE_RECORDS_MAX: u64 = 4;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir()
        .join(format!("enchudb-issue972-{tag}-{}-{nanos}", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

struct Cleanup(Vec<String>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_dir_all(p);
        }
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

/// `f` の間にこの thread が WAL を読んだ回数。
fn reads<T>(f: impl FnOnce() -> T) -> (T, WalReads) {
    let a = wal_reads();
    let out = f();
    let b = wal_reads();
    let d = WalReads {
        replays: b.replays - a.replays,
        hole_scans: b.hole_scans - a.hole_scans,
        probes: b.probes - a.probes,
        asks: b.asks - a.asks,
        probe_records: b.probe_records - a.probe_records,
        ..WalReads::default()
    };
    (out, d)
}

/// 全部を解いたのは本当の再生の 1 回、 再生する record があるかを WAL の file に聞いたのは `probes` 回 (単独の open が lock の下で)、
/// 開いた WAL に聞いたのは全部で `asks` 回 (file に聞く 1 回ごとの 1 回と、 WAL 付きの open が開いた WAL に聞く回) で、 どれも最初の数件で
/// 止まった。
fn assert_read_once(r: WalReads, probes: u64, asks: u64, what: &str) {
    assert_eq!(r.replays, 1, "{what}: WAL を全部解いた回数 ({r:?})");
    assert_eq!(r.probes, probes, "{what}: 再生する record があるかを WAL の file に聞いた回数 ({r:?})");
    assert_eq!(r.asks, asks, "{what}: 再生する record があるかを聞いた回数 ({r:?})");
    assert!(r.probe_records <= PROBE_RECORDS_MAX * asks, "{what}: 聞く読みが最初の数件で止まらない ({r:?})");
}

/// 表 `t` (枠 `rows`、 列 a)。
fn create(path: &str, sync: bool, rows: u32) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 1 << 16).unwrap();
    eng.define_table("t", rows).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(1);
    eng
}

/// 行を `ROWS` 個書いて、 consumer を止めたまま (checkpoint は書く前) 写す = WAL に再生する record が `ROWS` 件ある kill の像。
/// 写しの置き場と、 最後の行と値。
fn image_with_records(sync: bool, made: &str, img: &str) -> (u64, u32) {
    let eng = create(made, sync, ROWS + 16);
    let e0 = eng.entity_in("t").unwrap();
    eng.tie_to(e0, "t.a", 1u32);
    eng.oplog_sync().unwrap();
    let hold = eng.hold_consumer();
    let mut last = e0;
    for i in 0..ROWS {
        last = eng.entity_in("t").unwrap();
        eng.tie_to(last, "t.a", 100 + i);
    }
    eng.oplog_commit();
    copy_dir(Path::new(made), Path::new(img));
    drop(hold);
    (last, 100 + ROWS - 1)
}

/// WAL に再生する record がある kill の像: WAL 付きの open は聞かずに 1 回だけ解く。 単独の open は往復の前と後に 1 回ずつ (lock の下で)
/// 聞いて、 解くのは往復の中の再生の 1 回だけ (前は前と中の 2 回 + 後の見直し)。 sync する DB / しない DB。
#[test]
fn an_open_after_a_kill_decodes_the_wal_once() {
    for sync in [false, true] {
        let (made, img, w, s) = (tmp_path("made"), tmp_path("img"), tmp_path("w"), tmp_path("s"));
        let _c = Cleanup(vec![made.clone(), img.clone(), w.clone(), s.clone()]);
        let (last, v) = image_with_records(sync, &made, &img);
        copy_dir(Path::new(&img), Path::new(&w));
        copy_dir(Path::new(&img), Path::new(&s));

        let (eng, r) = reads(|| Engine::open_concurrent_with_oplog(&w, WAL).unwrap());
        assert_eq!(eng.get(last, "t.a"), Some(v as u64), "前提: 再生した (sync = {sync})");
        drop(eng);
        assert_read_once(r, 0, 0, &format!("WAL 付きの open (sync = {sync})"));

        let (eng, r) = reads(|| Engine::open_standalone(&s).unwrap());
        assert_eq!(eng.get(last, "t.a"), Some(v as u64), "前提: 往復が再生した (sync = {sync})");
        drop(eng);
        assert_read_once(r, 2, 2, &format!("単独の open (sync = {sync})"));
    }
}

/// sync する DB をきれいに閉じずに落とし、 WAL に再生する record が無い像: 単独の open は #451 の作り直しのために往復する
/// (`relog_before_standalone_session`)。 往復の前と後に聞き、 往復の中の再生 (解く物は無い) が 1 回。
#[test]
fn the_relog_round_trip_of_an_unclean_sync_db_reads_the_wal_once() {
    let (made, img) = (tmp_path("relog-made"), tmp_path("relog-img"));
    let _c = Cleanup(vec![made.clone(), img.clone()]);
    let e = {
        let eng = create(&made, true, 64);
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.a", 5u32);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        let hold = eng.hold_consumer();
        copy_dir(Path::new(&made), Path::new(&img));
        drop(hold);
        e
    };
    let (eng, r) = reads(|| Engine::open_standalone(&img).unwrap());
    assert_eq!(eng.get(e, "t.a"), Some(5));
    assert!(!eng.himo_index_rebuilt_on_load(), "前提: 往復した (往復が閉じて clean の印を立てた)");
    drop(eng);
    assert_read_once(r, 2, 2, "往復する単独の open");
}

/// 表 `notes` (枠 2、 列 note) と、 使い回しを起こすための表 `full` (枠 1)。 (`eid_generation.rs` と同じ形)
fn create_v15(path: &str, sync: bool) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 2).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_table("full", 1).unwrap();
    eng.define_himo_in("full", "x", ValueType::Number, 0).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(1);
    eng
}

/// v15 の DB で、 開く時の突き合わせが削除を完了させる像 (`eid_generation.rs` の `reconcile_through_standalone_open` と同じ:
/// gen.seg は削除の後、 生きている印は削除の前) に、 再生する record を `ROWS` 件足す。 突き合わせは仕上げを再生の後に回すかを
/// 聞く (`reconcile_generations`)。 WAL 付きの open は開いた WAL に 1 回聞いて 1 回解く。 単独の open は往復の前と後に WAL の file に
/// 聞き (lock の下、 突き合わせの前)、 往復の WAL 付きの open が開いた WAL に 1 回聞いて、 1 回解く (#972 の前は 3 回全部を解いた)。
#[test]
fn an_open_that_reconciles_generations_decodes_the_wal_once() {
    for sync in [false, true] {
        let made = tmp_path(&format!("v15-{sync}"));
        let (before, after, img, w, s) =
            (format!("{made}-before"), format!("{made}-after"), format!("{made}-img"), format!("{made}-w"), format!("{made}-s"));
        let _c = Cleanup(vec![made.clone(), before.clone(), after.clone(), img.clone(), w.clone(), s.clone()]);
        let (b, c) = {
            let eng = create_v15(&made, sync);
            let a = eng.entity_in("notes").unwrap();
            eng.tie_to(a, "notes.note", 1u32);
            let b = eng.entity_in("notes").unwrap();
            eng.tie_to(b, "notes.note", 2u32);
            eng.delete(a);
            let c = eng.entity_in("notes").unwrap();
            assert_eq!(eng.eid_slot(c), eng.eid_slot(a), "前提: v15 に移って使い回す");
            eng.tie_to(c, "notes.note", 3u32);
            eng.oplog_sync().unwrap();
            let hold = eng.hold_consumer();
            copy_dir(Path::new(&made), Path::new(&before));
            eng.delete(c);
            // 削除の record を checkpoint の手前に (#870 の止めを sidecar で外す = 再生が削除をやり直さない)
            eng.persist_tables().unwrap();
            eng.oplog_sync().unwrap();
            for i in 0..ROWS {
                eng.tie_to(b, "notes.note", 10 + i); // 再生する record (削除とは関係の無い行)
            }
            eng.oplog_commit();
            copy_dir(Path::new(&made), Path::new(&after));
            drop(hold);
            (b, c)
        };
        // 生きている印 (entities.seg) だけ削除の前
        copy_dir(Path::new(&after), Path::new(&img));
        std::fs::copy(Path::new(&before).join("entities.seg"), Path::new(&img).join("entities.seg")).unwrap();
        copy_dir(Path::new(&img), Path::new(&w));
        copy_dir(Path::new(&img), Path::new(&s));

        let (eng, r) = reads(|| Engine::open_concurrent_with_oplog(&w, WAL).unwrap());
        assert_eq!(eng.generation_repairs(), (1, 0), "前提: 突き合わせが削除を完了させた (sync = {sync})");
        assert_eq!(eng.get(b, "notes.note"), Some(10 + ROWS as u64 - 1), "前提: 再生した");
        assert!(!eng.is_live(c));
        drop(eng);
        assert_read_once(r, 0, 1, &format!("突き合わせのある WAL 付きの open (sync = {sync})"));

        let (eng, r) = reads(|| Engine::open_standalone(&s).unwrap());
        assert_eq!(eng.get(b, "notes.note"), Some(10 + ROWS as u64 - 1), "前提: 往復が再生した");
        assert!(!eng.is_live(c));
        drop(eng);
        assert_read_once(r, 2, 3, &format!("突き合わせのある単独の open (sync = {sync})"));
    }
}
