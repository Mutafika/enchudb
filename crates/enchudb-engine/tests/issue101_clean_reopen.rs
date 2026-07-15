//! #101 regression: graceful close (Drop / flush_clean) が vocab/himo_reg の clean flag を
//! 永続化し、 次 open が rebuild_index (O(count)) を skip する。
//!
//! 観測点:
//! - `vocab_index_rebuilt_on_load()` — open 時に rebuild が走ったか (直接の争点)
//! - `vocab_index_is_clean()` — disk 上の clean flag。 writer open は #56 の保護で
//!   open 直後に 0 へ戻すため、 session 中に true になるのは flush_clean 直後のみ。
//!
//! crash 相当は「writer open 中 (= dirty マーク済み) の DB file を copy」で作る
//! (実 crash と同じく clean flag 0 のまま残った状態)。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;

const CAP: usize = 8 * 1024 * 1024;

fn fresh(path: &str) {
    for suf in ["", ".oplog", ".lock", ".tables", ".crc"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn define_tag(eng: &Arc<Engine>, name: &str) {
    let eng_mut = unsafe { &mut *(Arc::as_ptr(eng) as *mut Engine) };
    eng_mut.define_himo(name, ValueType::Tag, 0);
}

fn copy_db(src: &str, dst: &str) {
    fresh(dst);
    std::fs::copy(src, dst).expect("copy body");
    let _ = std::fs::copy(format!("{src}.oplog"), format!("{dst}.oplog"));
    let _ = std::fs::copy(format!("{src}.tables"), format!("{dst}.tables"));
}

/// graceful Drop → 次 open は rebuild skip。 crash (dirty copy) → rebuild が走る。
#[test]
fn graceful_close_skips_rebuild_crash_does_not() {
    let path = "/tmp/test_issue101_clean.db";
    let crashed = "/tmp/test_issue101_crashed.db";
    fresh(path);

    // 1) 新規作成 + vocab 書き込み。 open 中に copy = crash 相当 (flag は 0 のまま)。
    {
        let eng = Engine::create_concurrent_with_oplog(path, CAP).expect("create");
        define_tag(&eng, "tag");
        let e = eng.entity();
        eng.tie_text_async(e, "tag", "hello");
        eng.flush_writes();
        eng.oplog_sync().expect("durable");
        copy_db(path, crashed); // ← crash 相当のスナップショット
        // scope end = graceful Drop → #101 の clean-flush
    }

    // 2) graceful close した DB: rebuild は走らない
    {
        let eng = Engine::open_concurrent_with_oplog(path, CAP).expect("reopen");
        assert!(
            !eng.vocab_index_rebuilt_on_load(),
            "graceful close 後の open で rebuild が走った (= #101 の毎 open rebuild が再発)"
        );
        let vid = eng.find_value("tag", "hello").expect("hello が消えた");
        assert_eq!(eng.pull_raw("tag", vid).len(), 1);
    }

    // 3) crash 相当の copy: dirty なので rebuild が走る (= 正しい recovery、観測の teeth)
    {
        let eng = Engine::open_concurrent_with_oplog(crashed, CAP).expect("open crashed copy");
        assert!(
            eng.vocab_index_rebuilt_on_load(),
            "dirty DB なのに rebuild が走っていない (crash recovery が壊れている)"
        );
        let vid = eng.find_value("tag", "hello").expect("crash copy から hello が引けない");
        assert_eq!(eng.pull_raw("tag", vid).len(), 1);
    }

    // 4) crash copy も graceful close したので、 次 open は skip に戻る
    {
        let eng = Engine::open_concurrent_with_oplog(crashed, CAP).expect("reopen crashed");
        assert!(!eng.vocab_index_rebuilt_on_load());
    }

    fresh(path);
    fresh(crashed);
}

/// 明示 `flush_clean(&self)` はプロセス生存中に clean flag を立てられる
/// (sinfo のような「commit 後に checkpoint」用途)。 次の insert で dirty に戻る。
#[test]
fn explicit_flush_clean_marks_clean() {
    let path = "/tmp/test_issue101_explicit.db";
    fresh(path);

    let eng = Engine::create_concurrent_with_oplog(path, CAP).expect("create");
    define_tag(&eng, "tag");
    let e = eng.entity();
    eng.tie_text_async(e, "tag", "alpha");
    eng.flush_writes();
    assert!(!eng.vocab_index_is_clean(), "insert 後に clean のままはおかしい");

    eng.flush_clean().expect("flush_clean");
    assert!(
        eng.vocab_index_is_clean(),
        "flush_clean 直後に clean flag が立っていない"
    );

    // 続けて vocab insert すると dirty に戻る (#77-M1 保護)
    let e2 = eng.entity();
    eng.tie_text_async(e2, "tag", "beta");
    eng.flush_writes();
    assert!(!eng.vocab_index_is_clean());

    drop(eng);
    fresh(path);
}

/// readonly open では flush_clean / Drop とも何も書かない (真に非破壊 open、#56)。
#[test]
fn readonly_close_stays_nondestructive() {
    let path = "/tmp/test_issue101_readonly.db";
    fresh(path);

    {
        let eng = Engine::create_concurrent_with_oplog(path, CAP).expect("create");
        define_tag(&eng, "tag");
        let e = eng.entity();
        eng.tie_text_async(e, "tag", "hello");
        eng.flush_writes();
    } // graceful close → clean

    {
        let eng = Engine::open_readonly(path).expect("readonly open");
        assert!(
            !eng.vocab_index_rebuilt_on_load(),
            "clean DB の readonly open で rebuild (shadow) が走った"
        );
        eng.flush_clean().expect("readonly flush_clean は no-op で Err らない");
        drop(eng); // Drop も readonly guard で書かない
    }

    // readonly close が flag を壊していない = writer open はまだ rebuild skip できる
    let eng = Engine::open_concurrent_with_oplog(path, CAP).expect("writer reopen");
    assert!(!eng.vocab_index_rebuilt_on_load());
    drop(eng);
    fresh(path);
}
