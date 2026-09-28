//! #57: WAL が満杯で載らなかった write も、 相手に届くこと。
//!
//! 載らなかった record は `_sync_ops` にも入らないので、 差分 pull では二度と届かない (ローカルには
//! 書けているので、 相手とだけ永久にずれる)。 engine の bridge は、 載らなかった record の author の
//! history floor を 「今」 に上げる。 それより前の cursor の puller は差分 pull で `history_truncated`
//! になり、 bootstrap (#140) で live state (載らなかった write を含む) を受け取る。
//!
//! WAL は 64 KiB にして、 consumer の 1 周 (100 ms) より速く書いて溢れさせる (注入なし)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue57-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn make_engine(path: &str, peer: PeerId, oplog_capacity: usize) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_cell_version(path, 65_536).unwrap();
    eng.define_table("notes", 10_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, oplog_capacity).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// `async_` なら consumer が WAL にまとめて載せる経路 (`tie_async`)、 でなければ書く thread が載せる経路。
fn write_notes(eng: &Arc<Engine>, from: u32, n: u32, async_: bool) -> Vec<u64> {
    (from..from + n)
        .map(|i| {
            let e = eng.entity_in("notes").unwrap();
            if async_ {
                eng.tie_async(e, "notes.note", i);
            } else {
                eng.tie_to(e, "notes.note", i);
                eng.tie_text_to(e, "notes.body", &format!("body {i}"));
            }
            e
        })
        .collect()
}

fn settle(eng: &Arc<Engine>) {
    eng.oplog_commit();
    eng.flush_writes();
    // WAL が満杯なら Commit も載らず Err (#268、 group は空いた後の Commit で閉じる)。 ここでは見ない
    let _ = eng.oplog_sync();
    eng.transfer_oplog_to_sync_ops();
}

fn has_note(eng: &Arc<Engine>, i: u32, async_: bool) -> bool {
    let Some(e) = eng.pull_raw("notes.note", i).first().copied() else { return false };
    async_ || eng.get_text_owned(e, "notes.body").is_some_and(|b| b == format!("body {i}").as_bytes())
}

#[test]
fn writes_dropped_from_a_full_wal_reach_the_peer_through_bootstrap() {
    dropped_writes_reach_the_peer(false, false);
}

#[test]
fn async_writes_dropped_from_a_full_wal_reach_the_peer_through_bootstrap() {
    dropped_writes_reach_the_peer(true, false);
}

/// `publish_since_for_peer` を直に呼ぶ publisher (`publish_since` の先頭の広告を通らない) にも
/// floor が届く: 集めた record と一緒に広告する。 旧: この経路は広告せず、 落ちた後の record を
/// 古い floor のまま配って、 puller の cursor が floor を越えた。
#[test]
fn a_per_peer_publish_carries_the_raised_floor() {
    dropped_writes_reach_the_peer(false, true);
}

fn dropped_writes_reach_the_peer(async_: bool, per_peer: bool) {
    let tag = format!("{}{}", if async_ { "async" } else { "sync" }, if per_peer { "-pp" } else { "" });
    let (pa, pb) = (tmp_path(&format!("a-{tag}")), tmp_path(&format!("b-{tag}")));
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let eng_a = make_engine(&pa, 1, 64 * 1024);
    let eng_b = make_engine(&pb, 2, 16 * 1024 * 1024);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    sync_a.serve_state();

    // B は差分 pull で追従している
    write_notes(&eng_a, 0, 10, async_);
    settle(&eng_a);
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.applied > 0 && !out.history_truncated, "{out:?}");

    // 64 KiB の WAL を 1 周より速く溢れさせる
    let burst = write_notes(&eng_a, 10, 5_000, async_);
    // 載らなかった write にも版数が付いている (ZERO = 版数不明だと、 後から届く古い write に負ける)
    let hid = eng_a.himo_id("notes.note").unwrap() as u16;
    settle(&eng_a);
    assert!(burst.iter().all(|e| eng_a.cell_hlc(*e, hid) != Hlc::ZERO), "版数の無い cell がある");
    assert!(eng_a.wal_drop_floor_bumps() > 0, "前提: WAL が溢れて record が落ちること");
    if per_peer {
        sync_a.publish_since_for_peer(2, Hlc::ZERO);
    } else {
        sync_a.publish_since(Hlc::ZERO);
    }

    let out = sync_b.pull_once(1);
    assert!(out.history_truncated, "落ちた write の穴を知らされない: {out:?}");
    let boot = sync_b.bootstrap_pull(1).expect("serve_state 済み");
    assert_eq!(boot.outcome.dropped_vocab, 0, "{boot:?}");
    let missing: Vec<u32> = (0..5_010).filter(|i| !has_note(&eng_b, *i, async_)).collect();
    assert!(missing.is_empty(), "B に届いていない note {} 件 (先頭 {:?})", missing.len(), &missing[..missing.len().min(5)]);

    // bootstrap の後は差分 pull に戻る。 WAL が畳まれて空くまで待つ (空く前に書くと、 また落ちて
    // floor が上がり、 もう一度 bootstrap に回るのが正しい動き)
    let wal = eng_a.oplog().unwrap().clone();
    let end = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while wal.head() > 16 * 1024 {
        assert!(std::time::Instant::now() < end, "WAL が 10 秒で畳まれない (head {})", wal.head());
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let bumps = eng_a.wal_drop_floor_bumps();
    write_notes(&eng_a, 5_010, 3, async_);
    settle(&eng_a);
    assert_eq!(eng_a.wal_drop_floor_bumps(), bumps, "前提: 空いた WAL には載ること");
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(!out.history_truncated, "{out:?}");
    assert!((5_010..5_013).all(|i| has_note(&eng_b, i, async_)));

    drop((sync_a, sync_b, eng_a, eng_b));
    cleanup(&pa);
    cleanup(&pb);
}
