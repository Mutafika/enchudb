//! #236 — `state_records_for` が cell を黙って落とす (#226 follow-up)。
//!
//! replica 経路は 4 つの条件で cell を配らない: 版数が `Hlc::ZERO` / Tag の vid を
//! author の空間へ逆引きできない / Ref の target を `reverse` できない / Ref の値が
//! translated local でない。 **どれも個別には正しい判断**で、 batch は
//! `complete: false` なので受信側の ghost sweep も走らない。
//!
//! 危険なのは判断ではなく **落とした事実がどこにも残らない**こと。 replica が系統的に
//! 不完全な state を配っていても、 呼び出し側からは 「bootstrap は成功した」 としか
//! 見えない — #140 / #216 / #218 で繰り返し 「defect の class」 と扱ってきた
//! silent partial そのもの。 `complete: false` は row 単位の話としか読まれないし、
//! 欠けた cell は batch に現れないだけなので **受信側からは原理的に見えない**。
//!
//! 挙動は変えず、 観測できるようにした ([`Engine::state_records_dropped`] +
//! provider の once-warn + doc)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue236-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in [
        "", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".cursors",
        ".cursors.tmp",
    ] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "city", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn make_transport(peers: &[u32]) -> Arc<dyn Transport> {
    let mem = Arc::new(InMemoryTransport::new());
    for p in peers {
        mem.register_peer(*p);
    }
    mem
}

fn author_note(eng: &Arc<Engine>, i: u32, city: &str) {
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", i);
    eng.tie_text_to(e, "notes.city", city);
    eng.tie_text_to(e, "notes.body", &format!("body {i}"));
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

fn relay_cycle(eng: &Arc<Engine>, sync: &Syncer, from: PeerId) {
    sync.pull_once(from);
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    sync.publish_since(Hlc::ZERO);
}

/// A (peer 1) の note 3 件を R (peer 2) へ relay した状態を作る。
fn relayed_pair(pa: &str, pr: &str) -> (Arc<Engine>, Arc<Engine>, Syncer, Syncer) {
    let transport = make_transport(&[1, 2]);
    let eng_a = make_engine(pa, 1);
    let eng_r = make_engine(pr, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    // relay 側に先に自分の語彙を作って vid 空間をずらす (#226 と同じ理由)。
    for (i, city) in [(900u32, "Osaka"), (901, "Kyoto")] {
        author_note(&eng_r, i, city);
    }
    for i in 1..=3 {
        author_note(&eng_a, i, "Tokyo");
    }
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    (eng_a, eng_r, sync_a, sync_r)
}

/// 基本の relay 経路では **1 件も落ちない**こと。
///
/// counter が背景値を持つと 「増えていたら異常」 という読み方ができなくなる。
/// #226 の `replica_state_matches_author_state` は author 本人の wire 形との集合一致で
/// 同じことを間接的に見ているが、 counter という別の観測面でも押さえておく
/// (「落とさない」 と 「落としたと記録しない」 を取り違えないため)。
#[test]
fn basic_relay_replica_state_drops_nothing() {
    let (pa, pr) = (tmp_path("a1"), tmp_path("r1"));
    let (eng_a, eng_r, sync_a, sync_r) = relayed_pair(&pa, &pr);

    let (recs, _) = eng_r.state_records_for(1);
    assert!(!recs.is_empty(), "前提: replica が author 1 の state を合成できている");
    assert_eq!(
        eng_r.state_records_dropped(),
        0,
        "基本の relay 経路で cell が落ちている — replica の state が cell 単位で \
         欠けたまま配られる (受信側からは見えない)"
    );

    drop(sync_a);
    drop(sync_r);
    drop(eng_a);
    drop(eng_r);
    cleanup(&pa);
    cleanup(&pr);
}

/// relay 自身が author の行へ Tag を write-back すると、 その cell の vid は
/// author の vid 空間へ逆引きできない → **配らない**。 その事実が counter に載ること。
///
/// 落とす判断自体は正しい (relay の local vid を `(author, vid)` として配ると、
/// author 直 pull で来る同じ key の別テキストと衝突して vocab 写像が壊れる、#209 と
/// 同種)。 見たいのは 「正しく落としたことが観測できるか」 の方。
#[test]
fn relay_write_back_to_a_replicated_row_is_counted_as_dropped() {
    let (pa, pr) = (tmp_path("a2"), tmp_path("r2"));
    let (eng_a, eng_r, sync_a, sync_r) = relayed_pair(&pa, &pr);

    // 基準点 (ここまでで落ちていないこと自体は上の test が見ている)。
    let (before_recs, _) = eng_r.state_records_for(1);
    let base = eng_r.state_records_dropped();

    // relay が author 1 の行に自分の語彙で write-back する (#76 / #178 の経路)。
    let target = *eng_r.pull_raw("notes.note", 2).first().expect("relay に届いている");
    eng_r.tie_text_to(target, "notes.city", "Sapporo");
    eng_r.flush_writes();

    let (after_recs, _) = eng_r.state_records_for(1);
    let dropped = eng_r.state_records_dropped() - base;

    assert!(
        dropped > 0,
        "author の vid 空間へ戻せない cell を落としたのに counter に載っていない — \
         これが #236 の症状 (呼び出し側からは 「bootstrap は成功した」 としか見えない)"
    );
    assert!(
        after_recs.len() < before_recs.len(),
        "落ちた分だけ record が減っていること (before={}, after={})",
        before_recs.len(),
        after_recs.len()
    );

    drop(sync_a);
    drop(sync_r);
    drop(eng_a);
    drop(eng_r);
    cleanup(&pa);
    cleanup(&pr);
}

/// self 版 (`state_records`) は版数 ZERO を `as_of` で stamp して配るので、
/// counter を汚さないこと — #236 は replica 経路の話。
#[test]
fn self_state_records_do_not_count_as_dropped() {
    let pa = tmp_path("a3");
    let eng_a = make_engine(&pa, 1);
    for i in 1..=3 {
        author_note(&eng_a, i, "Tokyo");
    }
    let (recs, as_of) = eng_a.state_records();
    assert!(!recs.is_empty(), "self の state が空");
    assert!(as_of > Hlc::ZERO, "self の as_of は採番される");
    assert_eq!(eng_a.state_records_dropped(), 0, "self 経路は cell を落とさない");

    drop(eng_a);
    cleanup(&pa);
}
