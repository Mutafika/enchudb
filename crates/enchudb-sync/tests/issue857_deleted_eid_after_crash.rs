//! #857: 消した行の eid を、 落ちた後に開いた DB がもう一度払い出さない。
//!
//! 払い出していた道: `oplog_sync()` は checkpoint を進めるが `tables` (払い出した数 = `next_local`) を書かなかった。 行 X を
//! 足して消して `oplog_sync()` した後に落ちると、 X の Tie も Delete も checkpoint より前で再生されず、 開いた `next_local` は
//! 生きている行の最大 + 1 までしか戻らない (X は生きていない)。 次に足した行が X の eid になる。
//! sync の DB では、 Delete(X) を受けた相手が、 消えた slot 宛ての書き込みを版数によらず捨てる = 新しい行が相手に現れない。
//!
//! 直し: sidecar の `next_local` より先の行を消した後の `oplog_sync()` は、 checkpoint を進める前に `tables` を書く。 版数の列の
//! ある DB (sync の DB) は、 開く時に tombstone のある local も払い出し済みと数える (0.37.4 以前が残した像)。
#![cfg(unix)]

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;

const WAL: usize = 4 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("enchudb-issue857-{tag}-{}-{seq}-{nanos}", std::process::id())).to_str().unwrap().to_string()
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
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

fn create(path: &str, sync: bool, peer: u32, capacity: u32) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", capacity).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// 行 A を残し、 行 X を足して消して `oplog_sync()` した所で落ちた像を作る。 consumer の周期の書き出しは止めておく (周期が
/// 先に `tables` を書くかは時間の当たり方による)。 返すのは (A, X)。
fn crash_after_delete(eng: &Arc<Engine>, made: &str, image: &str) -> (u64, u64) {
    let hold = eng.hold_consumer();
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1u32);
    eng.oplog_sync().unwrap();
    let x = eng.entity_in("notes").unwrap();
    eng.tie_to(x, "notes.note", 2u32);
    eng.oplog_sync().unwrap();
    eng.delete(x);
    eng.oplog_sync().unwrap();
    assert!(eng.pull_raw("notes.note", 2u32).is_empty(), "前提: X は消えた");
    copy_dir(std::path::Path::new(made), std::path::Path::new(image));
    drop(hold);
    (a, x)
}

/// `tables` の sidecar の、 表 `table` の `next_local` を `value` に書き換える (足して消した行が sidecar に届く前に落ちた、
/// 0.37.4 以前の像の形)。 表ごとの並びは 名前の長さ (u32) / 名前 / eid の範囲 (u32 × 2) / `next_local` (u32)。
fn set_next_local(db: &str, table: &str, value: u32) {
    let path = std::path::Path::new(db).join("tables");
    let mut bytes = std::fs::read(&path).unwrap();
    let mut name = (table.len() as u32).to_le_bytes().to_vec();
    name.extend_from_slice(table.as_bytes());
    let at = bytes.windows(name.len()).position(|w| w == name.as_slice()).expect("sidecar に表がある") + name.len() + 8;
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();
}

/// sync しない DB / sync の DB のどちらでも、 開き直した後に足した行は、 消した行の eid にならない。
///
/// falsify: `oplog_sync` が `tables` を書かないと、 sync しない DB で消した行の eid が出る (sync の DB は開く時の tombstone で
/// 防ぐ)。 開く時に tombstone を数える所も外すと sync の DB でも出る。
#[test]
fn a_deleted_eid_is_not_handed_out_again_after_a_crash() {
    for (sync, older) in [(false, false), (true, false), (true, true)] {
        let made = tmp_path(if sync { "sync" } else { "plain" });
        let image = format!("{made}-image");
        let (_c1, _c2) = (Cleanup(made.clone()), Cleanup(image.clone()));
        let (a, x) = {
            let eng = create(&made, sync, 1, 100);
            crash_after_delete(&eng, &made, &image)
        };
        if older {
            // 0.37.4 以前が残した像: sidecar は X を足す前のまま (A だけ)
            set_next_local(&image, "notes", 1);
        }
        let eng = Engine::open_concurrent_with_oplog(&image, WAL).unwrap();
        eng.set_peer_id(1);
        assert_eq!(eng.get(a, "notes.note"), Some(1), "前提: 残した行は読める");
        let y = eng.entity_in("notes").unwrap();
        assert_ne!(y, x, "消した行の eid を、 落ちた後にもう一度払い出した (sync = {sync}、 古い sidecar = {older})");
        assert_ne!(y, a);
    }
}

/// 2 peer: 落ちた後に足した行が、 X の削除を受けた相手にも現れる。
#[test]
fn a_row_added_after_a_crash_reaches_a_peer_that_saw_the_delete() {
    let (pa, pb) = (tmp_path("a"), tmp_path("b"));
    let image = format!("{pa}-image");
    let (_c1, _c2, _c3) = (Cleanup(pa.clone()), Cleanup(pb.clone()), Cleanup(image.clone()));
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let eng_b = create(&pb, true, 2, 100);
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    {
        let eng_a = create(&pa, true, 1, 100);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        crash_after_delete(&eng_a, &pa, &image);
        sync_a.publish_since(Hlc::ZERO);
        let out = sync_b.pull_once(1);
        assert!(out.applied > 0, "前提: 相手が A の record を受けた ({out:?})");
        assert_eq!(eng_b.pull_raw("notes.note", 1u32).len(), 1, "前提: 相手に行 A がある");
        assert!(eng_b.pull_raw("notes.note", 2u32).is_empty(), "前提: 相手で X は消えている");
    }
    let eng_a = Engine::open_concurrent_with_oplog(&image, WAL).unwrap();
    eng_a.set_peer_id(1);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let y = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(y, "notes.note", 3u32);
    eng_a.oplog_sync().unwrap();
    sync_a.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sync_b.pull_once(1);
    }
    assert_eq!(eng_b.pull_raw("notes.note", 3u32).len(), 1, "落ちた後に足した行が、 削除を受けた相手に現れない");
}

/// 確かめ: 枠を使い切った表が、 消した行の slot を使い回した時 (落ちていない)、 相手に新しい行が現れるか。 現れない (#863、
/// この PR では直していない)。
#[test]
#[ignore = "#863: 枠を使い切った表が使い回した slot の行は、 削除を受けた相手に現れない (未修正)"]
fn a_reused_slot_of_a_full_table_reaches_the_peer() {
    let (pa, pb) = (tmp_path("fa"), tmp_path("fb"));
    let (_c1, _c2) = (Cleanup(pa.clone()), Cleanup(pb.clone()));
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let make = |path: &str, peer: u32| {
        // 枠 4 の表で eid 空間を使い切る (足せない)
        let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
        eng.define_table("notes", 4).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.define_table("pad", eng.remaining_eid_capacity() / 2).unwrap();
        eng.enable_sync_tables().unwrap();
        let rest = eng.remaining_eid_capacity();
        if rest > 0 {
            eng.define_table("pad2", rest).unwrap();
        }
        let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
        eng.set_peer_id(peer);
        eng
    };
    let (eng_a, eng_b) = (make(&pa, 1), make(&pb, 2));
    let (sync_a, sync_b) = (Syncer::new(eng_a.clone(), transport.clone()), Syncer::new(eng_b.clone(), transport.clone()));
    let rows: Vec<u64> = (0..4u32)
        .map(|i| {
            let e = eng_a.entity_in("notes").unwrap();
            eng_a.tie_to(e, "notes.note", 10 + i);
            e
        })
        .collect();
    eng_a.delete(rows[1]);
    eng_a.oplog_sync().unwrap();
    sync_a.publish_since(Hlc::ZERO);
    sync_b.pull_once(1);
    assert!(eng_b.pull_raw("notes.note", 11u32).is_empty(), "前提: 相手で消えている");
    let y = eng_a.entity_in("notes").expect("前提: 消した slot を使い回して払い出せる");
    assert_eq!(y, rows[1], "前提: 使い回した slot は消した行の eid");
    eng_a.tie_to(y, "notes.note", 99u32);
    eng_a.oplog_sync().unwrap();
    sync_a.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sync_b.pull_once(1);
    }
    assert_eq!(eng_b.pull_raw("notes.note", 99u32).len(), 1, "枠を使い切った表が使い回した slot の行が、 相手に現れない");
}

/// sidecar より先の行を消した後の `oplog_sync()` は checkpoint を進めず、 consumer の周期が `tables` を書いてから進める
/// (WAL が止まったままにならない)。
///
/// falsify: consumer の周期が `tables` を書いても、 置いた `next_local` を覚えなければ、 checkpoint は進まないまま。
#[test]
fn the_checkpoint_catches_up_after_a_freed_slot() {
    let path = tmp_path("catchup");
    let _c = Cleanup(path.clone());
    let eng = create(&path, false, 1, 100);
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1u32);
    eng.oplog_sync().unwrap();
    let held = {
        let _hold = eng.hold_consumer();
        let x = eng.entity_in("notes").unwrap();
        eng.tie_to(x, "notes.note", 2u32);
        eng.oplog_sync().unwrap();
        eng.delete(x);
        eng.oplog_sync().unwrap();
        let s = eng.stats();
        s.oplog_checkpoint < s.oplog_head
    };
    assert!(held, "前提: sidecar より先の行を消した後の oplog_sync は checkpoint を進めない");
    let t0 = std::time::Instant::now();
    loop {
        let s = eng.stats();
        if s.oplog_checkpoint == s.oplog_head {
            break;
        }
        assert!(t0.elapsed() < std::time::Duration::from_secs(5), "consumer の周期が checkpoint を進めない (5 秒)");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    // 進んだ後の oplog_sync は止めない
    eng.tie_to(a, "notes.note", 3u32);
    eng.oplog_sync().unwrap();
    let s = eng.stats();
    assert_eq!(s.oplog_checkpoint, s.oplog_head, "tables を書いた後の oplog_sync が checkpoint を進めない");
}
