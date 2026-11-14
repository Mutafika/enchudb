//! #884: `tables` の sidecar の書き出しの小さい穴。
//!
//! 1. `persisted_next` (sidecar に置いた `next_local`) を下げなかった。 `next_local` を 0 に戻す道 (`clear_local_only_tables`) の後も
//!    disk の値より大きく残り、 その表で sidecar より先の行を消しても #857 の止めが掛からず、 落ちた後に消した行の eid をもう一度
//!    払い出した (local-only の表だけ)
//! 2. `Engine::flush` は `tables` を書けたかを見なかった (書けない時も、 build phase の defer の時も Ok)。 standalone で足して消して
//!    `flush` した後に落ちると、 消した行の eid をもう一度払い出した
//!
//! `tables` を書けない状態は #870 と同じく `{db}/tables.tmp` に directory を置いて作る。

#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};
use std::path::{Path, PathBuf};

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(tag: &str) -> Scratch {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = std::env::temp_dir().join(format!("enchudb-issue884-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    Scratch(p)
}

fn copy(from: &Path, to: &Path) {
    assert!(std::process::Command::new("cp").arg("-R").arg(from).arg(to).status().unwrap().success());
}

/// 1: local-only の表を空にして (`next_local` を 0 に戻して) `tables` を置いた後、 足して消した行は #857 の止めに掛かり、 落ちた後に
/// その eid を払い出さない。
#[test]
fn a_cleared_local_only_table_holds_the_checkpoint_for_a_deleted_row() {
    let s = scratch("cleared");
    let db = s.0.join("db");
    let mut eng = Engine::create_growable_with_capacity(db.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_reserved_table("_obs", 100).unwrap();
    eng.define_himo_in("_obs", "a", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let a = eng.himo_id("_obs.a").unwrap() as u16;
    for v in 0..3u32 {
        let e = eng.entity_in("_obs").unwrap();
        eng.tie_to_by_id(e, a, v);
    }
    eng.oplog_sync().unwrap();
    eng.persist_tables().unwrap();
    // 空にして、 `next_local` = 0 の sidecar を置く
    assert_eq!(eng.clear_local_only_tables(), 3);
    eng.persist_tables().unwrap();
    // consumer の周期は `tables` を書いて止めを解くので止める
    let hold = eng.hold_consumer();
    let r = eng.entity_in("_obs").unwrap();
    eng.tie_to_by_id(r, a, 9u32);
    eng.delete(r);
    eng.oplog_sync().unwrap();
    let image = s.0.join("image");
    copy(&db, &image);
    drop(hold);
    drop(eng);
    let reopened = Engine::open_concurrent_with_oplog(image.to_str().unwrap(), 4 << 20).unwrap();
    let y = reopened.entity_in("_obs").unwrap();
    assert_ne!(y, r, "落ちた後に、 消した行の eid {r:#x} をもう一度払い出した");
}

/// 2a: `tables` を書けない時、 `flush` は Err を返す (前は Ok)。
#[test]
fn flush_fails_when_tables_cannot_be_written() {
    let s = scratch("flushfail");
    let db = s.0.join("db");
    let mut eng = Engine::create_growable_with_capacity(db.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    let e = eng.entity_in("t").unwrap();
    eng.tie(e, "t.a", 1u32);
    std::fs::create_dir(db.join("tables.tmp")).unwrap();
    let r = eng.flush();
    std::fs::remove_dir(db.join("tables.tmp")).unwrap();
    assert!(r.is_err(), "`tables` を書けないのに flush が Ok を返した");
    eng.flush().expect("書けるように戻した後の flush");
}

/// 2b: build phase の defer (`set_defer_tables_persist(true)`、 `Database::create` の後) でも、 `flush` は `tables` を置く。 足して消して
/// `flush` した後に落ちた像を開いても、 消した行の eid を払い出さない。
#[test]
fn flush_writes_tables_even_while_deferred() {
    let s = scratch("defer");
    let db = s.0.join("db");
    let mut eng = Engine::create_growable_with_capacity(db.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();
    eng.set_defer_tables_persist(true);
    let x = eng.entity_in("t").unwrap();
    eng.tie(x, "t.a", 1u32);
    let y = eng.entity_in("t").unwrap();
    eng.tie(y, "t.a", 2u32);
    eng.delete(y);
    eng.flush().unwrap();
    let image = s.0.join("image");
    copy(&db, &image);
    drop(eng);
    let mut reopened = Engine::open_standalone(image.to_str().unwrap()).unwrap();
    let z = reopened.entity_in("t").unwrap();
    assert_ne!(z, y, "flush の後に落ちて、 消した行の eid {y:#x} をもう一度払い出した");
    let _ = reopened.flush();
}
