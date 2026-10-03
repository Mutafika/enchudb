//! #381: 辞書の語を回収する peer 同士の sync。 送り手が場所を使い回すと番号 (世代込み) 自体が変わるので、 受け手の
//! `(送り手, 番号) → 手元の番号` の対応は混ざらない。 受け手も自分の辞書で使い回す (送り手の削除が届いて参照 0 に
//! なった語)。 作っては消すを何周しても、 受け手の生きている行の値は送り手と同じ。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::{GrowableOptions, ValueType};
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-381-sync-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for ext in ["oplog", "tables", "crc", "db.lock", "eidmap", "vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}.{ext}"));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_growable_opts(
        path,
        GrowableOptions { max_entities: 65_536, vocab_reclaim: true, ..Default::default() },
    )
    .unwrap();
    eng.define_table("notes", 4096).unwrap();
    eng.define_himo_in("notes", "label", ValueType::Tag, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 64 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn ship(eng_a: &Arc<Engine>, syncer_a: &Syncer, syncer_b: &Syncer) {
    eng_a.oplog_commit();
    eng_a.oplog_sync().unwrap();
    let t0 = std::time::Instant::now();
    while t0.elapsed() < Duration::from_secs(5) {
        syncer_a.publish_since(Hlc::ZERO);
        if syncer_b.pull_once(1).applied > 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("5 秒以内に A の op が B に適用されなかった");
}

#[test]
fn rolling_labels_sync_while_both_peers_reuse_slots() {
    let (path_a, path_b) = (tmp_path("a"), tmp_path("b"));
    let eng_a = make_engine(&path_a, 1);
    let eng_b = make_engine(&path_b, 2);
    // 全 peer を最初から register する (publish の宛先が registered peer 別に切り替わる、 live_query_remote と同じ)
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem;
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());

    let mut live: VecDeque<(u64, String)> = VecDeque::new();
    let mut gone: Vec<String> = Vec::new();
    let mut n = 0u32;
    for _round in 0..30 {
        for _ in 0..40 {
            let e = eng_a.entity_in("notes").unwrap();
            let label = format!("label-{n}");
            eng_a.tie_text_to(e, "notes.label", &label);
            live.push_back((e, label));
            n += 1;
            if live.len() > 60 {
                let (old, label) = live.pop_front().unwrap();
                eng_a.delete(old);
                gone.push(label);
            }
        }
        ship(&eng_a, &syncer_a, &syncer_b);
    }
    let (ua, ub) = (eng_a.vocab_usage(), eng_b.vocab_usage());
    assert!(ua.reclaimed > 500, "送り手が使い回している: {ua:?}");
    assert!(ub.reclaimed > 500, "受け手も使い回している: {ub:?}");
    for (_, label) in &live {
        let vid = eng_b.vocab_id(label).unwrap_or_else(|| panic!("B に {label} が無い"));
        assert_eq!(eng_b.pull_raw("notes.label", vid).len(), 1, "{label}");
    }
    for label in &gone {
        let rows = eng_b.vocab_id(label).map(|v| eng_b.pull_raw("notes.label", v).len()).unwrap_or(0);
        assert_eq!(rows, 0, "消した {label} が B に残っている");
    }
    drop((syncer_a, syncer_b, eng_a, eng_b));
    cleanup(&path_a);
    cleanup(&path_b);
}

/// `_sync_ops` の payload (engine 内部の table の Leaf = 辞書に入る) も参照に数える。 開き直して既存の cell を数え直す時に
/// payload を数え漏らすと、 payload の語が参照 0 として使い回され、 配布前の op が別の値になる。
#[test]
fn sync_ops_payload_survives_recount_after_reopen() {
    let (path_a, path_b) = (tmp_path("ra"), tmp_path("rb"));
    let labels: Vec<String> = (0..200).map(|i| format!("before-reopen-{i}")).collect();
    {
        let eng_a = make_engine(&path_a, 1);
        for l in &labels {
            let e = eng_a.entity_in("notes").unwrap();
            eng_a.tie_text_to(e, "notes.label", l);
        }
        eng_a.oplog_commit();
        eng_a.oplog_sync().unwrap();
        // bridge が _sync_ops に写すのを待つ
        std::thread::sleep(Duration::from_millis(400));
    }
    let eng_a = Engine::open_concurrent_with_oplog(&path_a, 64 * 1024 * 1024).unwrap();
    eng_a.set_peer_id(1);
    assert!(eng_a.vocab_usage().reclaim);
    eng_a.build_vocab_refs();
    // 他の値を作っては消して、 参照 0 の場所を使い回す
    for i in 0..3_000 {
        let e = eng_a.entity_in("notes").unwrap();
        eng_a.tie_text_to(e, "notes.label", &format!("churn-{i}"));
        eng_a.delete(e);
    }
    assert!(eng_a.vocab_usage().reclaimed > 1_000, "{:?}", eng_a.vocab_usage());
    let eng_b = make_engine(&path_b, 2);
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem;
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    ship(&eng_a, &syncer_a, &syncer_b);
    for _ in 0..20 {
        syncer_a.publish_since(Hlc::ZERO);
        syncer_b.pull_once(1);
    }
    for l in &labels {
        let vid = eng_b.vocab_id(l).unwrap_or_else(|| panic!("B に {l} が届いていない"));
        assert_eq!(eng_b.pull_raw("notes.label", vid).len(), 1, "{l}");
    }
    drop((syncer_a, syncer_b, eng_a, eng_b));
    cleanup(&path_a);
    cleanup(&path_b);
}
