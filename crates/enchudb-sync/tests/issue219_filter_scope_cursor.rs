//! #219 — publisher 側 `SubscriptionFilter` は pull cursor を **scope 依存**にする。
//!
//! puller の cursor は 「受け取った record の author 別 max HLC」 で前進するので、
//! 同一 author の record を filter が間引くと **cursor は落とされた分を飛び越える**。
//! cursor は author 粒度であって scope 粒度ではないので、 #216 の per-author 化でも
//! これは閉じない。
//!
//! これは 「直せるバグ」 ではなく **契約**として明文化した (issue の design review 合意)。
//! 自動で truncation 扱いにするには target 別の scope 世代を transport に載せる必要が
//! あり、 実際の app の follow 変更フローを見てから形を決める。 本 file は:
//!
//! 1. hazard を**実際に踏んで**固定する (characterization) — 「後で誰かが差分 pull の
//!    完全性を仮定する」 のを防ぐ。 挙動が変わったらここが落ちて、 契約 doc を
//!    直す機会になる
//! 2. publisher 側で **何をどこまで配らなかったかが観測できる**こと
//! 3. doc が指す回復手段 (bootstrap) が実際に効くこと — 契約に実効性を持たせる

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_engine::ValueType;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::{SubscriptionFilter, Syncer};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue219-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn author_note(eng: &Arc<Engine>, v: u32) {
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", v);
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

fn tick() {
    std::thread::sleep(std::time::Duration::from_millis(15));
}

/// 「note の値が `drop` に一致する Tie だけ落とす」 filter。 scope の出し入れを
/// atomic 1 個で表す (`NONE` = 全送り = scope を広げた状態)。
struct DropNoteValue {
    drop: AtomicU32,
}
const NONE: u32 = u32::MAX;

impl SubscriptionFilter for DropNoteValue {
    fn should_send(&self, _target_peer: PeerId, record: &WireRecord) -> bool {
        let d = self.drop.load(Ordering::Relaxed);
        if d == NONE {
            return true;
        }
        !matches!(&record.op, DecodedOp::Tie { value, .. } if *value == d)
    }
}

/// 落とした record の HLC を A の ring から拾う (assert 用)。
fn hlc_of_note(eng: &Arc<Engine>, v: u32) -> Hlc {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .find(|r| matches!(&r.op, DecodedOp::Tie { value, .. } if *value == v))
        .map(|r| r.hlc)
        .expect("note が ring にある")
}

#[test]
fn filtered_record_is_skipped_by_cursor_and_widening_scope_does_not_bring_it_back() {
    let pa = tmp_path("a");
    let pb = tmp_path("b");
    let mem = Arc::new(InMemoryTransport::new());
    for p in [1u32, 2] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();

    let eng_a = make_engine(&pa, 1);
    let eng_b = make_engine(&pb, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    sync_a.serve_state();

    let filter = Arc::new(DropNoteValue { drop: AtomicU32::new(20) });
    sync_a.set_subscription_filter(filter.clone());

    // HLC 順に 10 → 20 → 30。 落とすのは**中間**の 20 — 後続の 30 が cursor を
    // 20 の上へ押し上げる、 が hazard の本体。
    author_note(&eng_a, 10);
    tick();
    author_note(&eng_a, 20);
    tick();
    author_note(&eng_a, 30);
    let hlc20 = hlc_of_note(&eng_a, 20);

    let sent = sync_a.publish_since_for_peer(2, Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.applied > 0, "10 と 30 は届くこと: {out:?}");
    assert!(
        !out.history_truncated,
        "filter による欠落は truncation として現れない — これが #219 の要点: {out:?}"
    );

    assert!(!eng_b.pull_raw("notes.note", 10).is_empty(), "10 は届いている");
    assert!(!eng_b.pull_raw("notes.note", 30).is_empty(), "30 は届いている");
    assert!(
        eng_b.pull_raw("notes.note", 20).is_empty(),
        "20 は filter で落としたので届かない (前提)"
    );

    // publisher 側から 「何をどこまで配らなかったか」 が見えること。
    assert_eq!(
        sync_a.suppressed_since(2),
        Some(vec![(1, hlc20)]),
        "落とした record の author 別最大 HLC が記録されていない"
    );
    assert_eq!(sync_a.suppressed_records(), 1, "落とした record の累計");
    assert!(
        sync_a.suppressed_since(3).is_none(),
        "publish していない target に entry が付いている"
    );
    assert!(sent >= 2, "配ったのは 10 と 30 (と Commit 等): {sent}");

    // ── ここからが hazard 本体 ──
    // scope を広げて (= 20 も送る filter に変えて) 再 publish しても、 B の
    // cursor[1] は既に hlc(30) なので 20 は receive 側 filter で落ちる。
    filter.drop.store(NONE, Ordering::Relaxed);
    sync_a.publish_since_for_peer(2, Hlc::ZERO);
    let out2 = sync_b.pull_once(1);
    assert!(
        eng_b.pull_raw("notes.note", 20).is_empty(),
        "scope を広げたら過去分が差分 pull で届いた — 挙動が変わったなら \
         `SubscriptionFilter` の契約 doc と #219 を更新すること: {out2:?}"
    );
    assert!(
        !out2.history_truncated,
        "この欠落は truncation flag でも現れない (floor は 「reclaim で消えた」 分しか \
         表さず、 「その peer には配らなかった」 分は表さない): {out2:?}"
    );

    // ── doc が指す回復手段が実際に効くこと ──
    let boot = sync_b
        .bootstrap_pull_via(1, 1)
        .expect("state provider が居ること (sync_a.serve_state)");
    assert!(boot.outcome.applied > 0, "bootstrap で state が入ること: {boot:?}");
    assert!(
        !eng_b.pull_raw("notes.note", 20).is_empty(),
        "契約が指す唯一の回復手段 (bootstrap) で過去分が戻らない = 行き止まり: {boot:?}"
    );

    drop(sync_a);
    drop(sync_b);
    drop(eng_a);
    drop(eng_b);
    cleanup(&pa);
    cleanup(&pb);
}

/// default の `AllRecords` は何も落とさないので、 観測窓は常に空のままであること
/// (counter が背景値を持つと、 本物の欠落を閾値で拾えなくなる)。
#[test]
fn all_records_filter_suppresses_nothing() {
    let pa = tmp_path("a2");
    let pb = tmp_path("b2");
    let mem = Arc::new(InMemoryTransport::new());
    for p in [1u32, 2] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();

    let eng_a = make_engine(&pa, 1);
    let eng_b = make_engine(&pb, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());

    for v in [10u32, 20, 30] {
        author_note(&eng_a, v);
    }
    sync_a.publish_since_for_peer(2, Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.applied > 0, "{out:?}");

    for v in [10u32, 20, 30] {
        assert!(!eng_b.pull_raw("notes.note", v).is_empty(), "{v} が届いていない");
    }
    assert_eq!(sync_a.suppressed_records(), 0, "AllRecords は何も落とさない");
    assert!(sync_a.suppressed_since(2).is_none(), "観測窓は空のまま");

    drop(sync_a);
    drop(sync_b);
    drop(eng_a);
    drop(eng_b);
    cleanup(&pa);
    cleanup(&pb);
}
