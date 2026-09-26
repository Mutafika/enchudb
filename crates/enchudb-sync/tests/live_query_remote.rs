//! live query (クエリ購読) が **sync で届いた書き込み** を拾うこと。
//!
//! peer B が購読している条件に、 peer A の書き込みが Syncer 経由で入ってくる / 出ていく。
//! Tag の値は B の vocab に写像されて届く (`translate_remote_vid`) ので、 B で購読した時点で
//! B の vocab に無い文字列でも、 届いた時点で一致し始めること (EqText の遅延 resolve) も見る。
//!
//! oracle は B 側の `query_by_id` / Column 直読み (購読の bitset / route を通らない)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::{LiveDelta, LivePred, ValueType};
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-live-remote-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    for ext in ["oplog", "tables", "crc", "db.lock", "eidmap"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "label", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("notes", "score", ValueType::Number, 100).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn integrate(set: &mut BTreeSet<u64>, d: LiveDelta) {
    for e in d.removed {
        assert!(set.remove(&e), "removed に未報告の eid {e:#x}");
    }
    for e in d.added {
        assert!(set.insert(e), "added に報告済みの eid {e:#x}");
    }
}

/// A の commit 済み op を publish して B が pull する (docs の手順: `oplog_sync` →
/// `publish_since` → 相手が `pull_once`)。 bridge は background consumer の仕事なので、
/// 何か適用されるまで待つ。
fn ship(eng_a: &Arc<Engine>, syncer_a: &Syncer, syncer_b: &Syncer) {
    eng_a.oplog_commit();
    eng_a.oplog_sync().unwrap();
    let t0 = std::time::Instant::now();
    while t0.elapsed() < Duration::from_secs(5) {
        syncer_a.publish_since(Hlc::ZERO);
        if syncer_b.pull_once(1).applied > 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("5 秒以内に A の op が B に適用されなかった");
}

#[test]
fn remote_writes_enter_and_leave_subscription() {
    let path_a = tmp_path("a");
    let path_b = tmp_path("b");
    let eng_a = make_engine(&path_a, 1);
    let eng_b = make_engine(&path_b, 2);
    // 全 peer を最初から register する (issue149 の harness と同じ — publish の宛先が
    // registered peer 別に切り替わる仕様のため)
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem;
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());

    let label = eng_b.himo_id("notes.label").unwrap() as u16;
    let score = eng_b.himo_id("notes.score").unwrap() as u16;
    assert!(eng_b.vocab_id("hello").is_none(), "前提: B は 'hello' を知らない");
    let hello = eng_b
        .subscribe(vec![LivePred::EqText { himo_id: label, text: "hello".into() }])
        .unwrap();
    let high = eng_b
        .subscribe(vec![LivePred::Range { himo_id: score, lo: 50, hi: 99 }])
        .unwrap();
    let (mut seen_hello, mut seen_high) = (BTreeSet::new(), BTreeSet::new());
    let oracle = |seen_hello: &BTreeSet<u64>, seen_high: &BTreeSet<u64>, what: &str| {
        let want_hello: BTreeSet<u64> = match eng_b.vocab_id("hello") {
            Some(v) => eng_b.query_by_id(&[(label, v)]).into_iter().collect(),
            None => BTreeSet::new(),
        };
        let want_high: BTreeSet<u64> = eng_b
            .entities_with_himo(score)
            .into_iter()
            .filter(|&e| matches!(eng_b.get(e, "notes.score"), Some(s) if (50..=99).contains(&s)))
            .collect();
        assert_eq!(seen_hello, &want_hello, "{what}: label=hello");
        assert_eq!(seen_high, &want_high, "{what}: 50<=score<=99");
    };

    // 1. A が 3 row 書く → B に届く
    let rows: Vec<u64> = (0..3).map(|_| eng_a.entity_in("notes").unwrap()).collect();
    eng_a.tie_text_to(rows[0], "notes.label", "hello");
    eng_a.tie_to(rows[0], "notes.score", 70);
    eng_a.tie_text_to(rows[1], "notes.label", "hello");
    eng_a.tie_to(rows[1], "notes.score", 10);
    eng_a.tie_text_to(rows[2], "notes.label", "bye");
    eng_a.tie_to(rows[2], "notes.score", 90);
    ship(&eng_a, &syncer_a, &syncer_b);
    integrate(&mut seen_hello, hello.poll());
    integrate(&mut seen_high, high.poll());
    assert_eq!(seen_hello.len(), 2, "hello の 2 row が sync で届く");
    assert_eq!(seen_high.len(), 2, "score 70 / 90 の 2 row が sync で届く");
    oracle(&seen_hello, &seen_high, "初回 sync");

    // 2. A が値を変える / 消す → B で出入りする
    eng_a.tie_text_to(rows[0], "notes.label", "bye"); // hello から出る
    eng_a.tie_to(rows[1], "notes.score", 55); // high に入る
    eng_a.delete(rows[2]); // high から出る
    ship(&eng_a, &syncer_a, &syncer_b);
    let d_hello = hello.poll();
    let d_high = high.poll();
    assert_eq!((d_hello.added.len(), d_hello.removed.len()), (0, 1), "hello: {d_hello:?}");
    assert_eq!((d_high.added.len(), d_high.removed.len()), (1, 1), "high: {d_high:?}");
    integrate(&mut seen_hello, d_hello);
    integrate(&mut seen_high, d_high);
    oracle(&seen_hello, &seen_high, "更新 / 削除の sync");

    drop((hello, high, syncer_a, syncer_b));
    drop(eng_a);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}
