//! **容量が足りず apply できなかった record が、恒久喪失にならない** (#210)。
//!
//! #200 (#167) で 「ディスク満杯 / content 天井では書かずに拒否する」 を入れたが、
//! engine 側の戻り値が `bool` だったため sync 受信側は
//! `ApplyResult::from_lww(false)` = **`SkippedOlder` (doc に 「再配送は不要」)** として
//! 計上していた。 `SkippedOlder` は `min_rejected_hlc` を立てないので **pull cursor が
//! その record を越え、 空きが出ても二度と再配送されない**。
//!
//! 「LWW で古い」 と 「今は置く場所が無い」 は cursor を進めてよいかの判断が真逆なので、
//! `RemoteApply` (`Applied` / `Stale` / `RejectedCapacity`) で型として分けた。
//!
//! ここでは実際にディスクを埋める代わりに、 #200 の `set_space_margin`
//! (必要空き量を水増しする fault injection) で 「空きが足りない」 状態を決定的に作る。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_engine::{FaultKind, ValueType};
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue210-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    cleanup(&p);
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

#[test]
fn capacity_reject_holds_the_cursor_and_redelivers() {
    let pa = tmp("A");
    let pb = tmp("B");

    // author 側は通常の engine。
    let mut a = Engine::create_with_capacity(&pa, 65_536).unwrap();
    a.define_table("notes", 64).unwrap();
    a.define_himo_in("notes", "n", ValueType::Number, 0).unwrap();
    a.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    a.enable_sync_tables().unwrap();
    let a: Arc<Engine> = Engine::concurrentize_with_oplog(a, 16 * 1024 * 1024).unwrap();
    a.set_peer_id(1);

    // 受信側は growable backing (= grow が起きる) にしておく。 static backing では
    // 「伸ばせない」 という状態を作れないので、 この test の前提が成立しない。
    let mut b = Engine::create_growable_tiny(&pb).unwrap();
    b.define_table("notes", 64).unwrap();
    b.define_himo_in("notes", "n", ValueType::Number, 0).unwrap();
    b.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    b.enable_sync_tables().unwrap();
    let b: Arc<Engine> = Engine::concurrentize_with_oplog(b, 16 * 1024 * 1024).unwrap();
    b.set_peer_id(2);
    assert!(
        b.disk_free_bytes().is_some(),
        "前提: 受信側が growable backing であること"
    );

    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let sa = Syncer::new(a.clone(), transport.clone());
    let sb = Syncer::new(b.clone(), transport.clone());

    // ── 1) まず普通に 1 件通す (eid 写像と himo を B 側に作る) ──
    let ea = a.entity_in("notes").unwrap();
    a.tie_to(ea, "notes.n", 7);
    a.tie_text_to(ea, "notes.body", "first");
    a.oplog_sync().unwrap();
    assert!(sa.publish_since(Hlc::ZERO) > 0, "A が publish できていない");
    let out = sb.pull_once(1);
    // Number + Leaf の 2 op。
    assert_eq!(out.applied, 2, "前提の 1 行が入っていない: {out:?}");

    // Leaf 値は vocab に載らないので、 Number himo を lookup key にして行を引く。
    let eb = *b.pull_raw("notes.n", 7).first().expect("B に行が来ていない");

    // ── 2) 空きが足りない状態で、同じ cell の新しい値を受ける ──
    b.set_space_margin(u64::MAX / 2);
    let denials_before = b.space_denials();

    // **commit 済み領域に収まらない大きさ**にする — 収まってしまうと grow が起きず、
    // 空き確認が呼ばれないので fault injection が効かない。
    let big = "x".repeat(2 * 1024 * 1024);
    a.tie_text_to(ea, "notes.body", &big);
    a.oplog_sync().unwrap();
    sa.publish_since(Hlc::ZERO);
    let out = sb.pull_once(1);

    assert_eq!(
        out.rejected_capacity, 1,
        "容量拒否として数えていない: {out:?}"
    );
    assert_eq!(
        out.skipped, 0,
        "容量拒否を SkippedOlder (= 再配送不要) に潰している: {out:?}"
    );
    assert_eq!(out.applied, 0, "書けないのに applied になっている: {out:?}");
    assert!(
        out.min_rejected_hlc.is_some(),
        "cursor を止めていない = この record は二度と来ない: {out:?}"
    );
    assert!(
        b.fault_count(FaultKind::DiskSpace) > 0,
        "engine 側で DiskSpace として計上されていない"
    );
    assert!(
        b.space_denials() > denials_before,
        "grow の空き確認が働いていない (fault injection が効いていない)"
    );
    assert_ne!(
        b.get_text_owned(eb, "notes.body").as_deref(),
        Some(big.as_bytes()),
        "拒否したはずの値が書かれている"
    );

    // ── 3) 空きが戻れば、同じ record が再配送されて入る ──
    b.set_space_margin(0);
    sa.publish_since(Hlc::ZERO);
    let out = sb.pull_once(1);
    assert_eq!(
        out.applied, 1,
        "空きが戻っても再配送されない = 恒久喪失: {out:?}"
    );
    assert_eq!(
        b.get_text_owned(eb, "notes.body").as_deref(),
        Some(big.as_bytes()),
        "再配送後の値が入っていない"
    );

    // ── 4) 本当に古い record は従来どおり skipped (回帰確認) ──
    //
    // pull cursor は既に全部を越えているので、 A の oplog から直接 record を取って
    // 二度目の apply をかける (= 全件 「既に適用済み」)。
    // A の ring は bridge 後に畳まれているので、 publish と同じ source
    // (`_sync_ops`) から取る。
    let dup: Vec<WireRecord> = a
        .pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(WireRecord::from)
        .collect();
    assert!(!dup.is_empty(), "_sync_ops が空 (前提が崩れている)");
    let out = sb.apply_records(&dup);
    assert_eq!(out.applied, 0, "冪等でない: {out:?}");
    assert_eq!(
        out.rejected_capacity, 0,
        "古い record を容量拒否に混ぜている: {out:?}"
    );
    assert!(out.skipped > 0, "古い record が skipped に数えられていない: {out:?}");

    cleanup(&pa);
    cleanup(&pb);
}
