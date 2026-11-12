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
//!
//! #711: Tag の cell は、 author の空間に戻せなくても、 書いた peer (版数の peer) の空間に戻せればその peer の record の形で
//! 配るようになった (relay 自身の書き戻しも)。 どの空間にも戻せない cell だけが落ちて counter に載る。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
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

/// 試験の終わり (落ちた時も) に DB の directory ごと消す (#688: 名前が run ごとに違うので、 消さないと回すたびに /tmp に増える)。 path を
/// 作った直後に置き、 engine と transport より後に drop させる (engine を持つ物が残ったまま消すと、 後の drop が directory を作り直す)。
struct Cleanup(Vec<String>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_dir_all(p);
            let _ = std::fs::remove_file(p);
        }
    }
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // 0.26 から DB は directory (#688)
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
    let _cleanup = Cleanup(vec![pa.clone(), pr.clone()]);
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

/// relay 自身が author の行へ Tag を write-back した cell は、 author の vid 空間へ逆引きできない。 前は **配らなかった** (#236 は
/// それを counter で見えるようにした)。 #711 から、 その cell は **書いた peer (ここでは relay 自身) の record の形**で配る:
/// 書き手は relay、 宛名は author の世界番号、 vid は relay の空間。 author の写像は汚さない。 受けた follower に値が入ること。
#[test]
fn relay_write_back_to_a_replicated_row_is_served_as_the_relays_own_record() {
    let (pa, pr, pf) = (tmp_path("a2"), tmp_path("r2"), tmp_path("f2"));
    let _cleanup = Cleanup(vec![pa.clone(), pr.clone(), pf.clone()]);
    let (eng_a, eng_r, sync_a, sync_r) = relayed_pair(&pa, &pr);

    let (before_recs, _) = eng_r.state_records_for(1);
    let base = eng_r.state_records_dropped();

    // relay が author 1 の行に自分の語彙で write-back する (#76 / #178 の経路)。
    let target = *eng_r.pull_raw("notes.note", 2).first().expect("relay に届いている");
    eng_r.tie_text_to(target, "notes.city", "Sapporo");
    eng_r.flush_writes();
    let city = eng_r.himo_id("notes.city").unwrap() as u16;
    assert_eq!(eng_r.cell_hlc(target, city).peer, 2, "前提: cell の版数は relay (peer 2) の時計");

    let (after_recs, _) = eng_r.state_records_for(1);
    assert_eq!(eng_r.state_records_dropped() - base, 0, "relay の書き戻しを落とした");
    // 行 2 の city は relay の record (Vocab + Tie)、 他は author 1 の record のまま
    let by_relay: Vec<_> = after_recs.iter().filter(|r| r.author_peer == 2).collect();
    assert_eq!(by_relay.len(), 2, "relay の record は Vocab と Tie の 2 件: {by_relay:?}");
    assert!(by_relay.iter().any(|r| matches!(&r.op, DecodedOp::Vocab { bytes, .. } if bytes == b"Sapporo")));
    assert!(by_relay.iter().any(|r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, .. }
        if enchudb_oplog::eid_peer(*eid) == 1 && *himo_id == city)), "宛名は author 1 の世界番号: {by_relay:?}");
    assert_eq!(after_recs.len(), before_recs.len() + 1, "Tie は書き手が替わり、 Vocab が 1 件増える");

    // follower (peer 3) が state を当てると、 行 2 の city は relay の書いた値
    let eng_f = make_engine(&pf, 3);
    let sync_f = Syncer::new(eng_f.clone(), make_transport(&[3]));
    let out = sync_f.apply_records(&after_recs);
    assert_eq!(out.dropped_vocab + out.dropped_unresolved, 0, "{out:?}");
    let row = *eng_f.pull_raw("notes.note", 2).first().expect("follower に行 2");
    assert_eq!(eng_f.get_text_owned(row, "notes.city").as_deref(), Some(&b"Sapporo"[..]));
    let other = *eng_f.pull_raw("notes.note", 1).first().expect("follower に行 1");
    assert_eq!(eng_f.get_text_owned(other, "notes.city").as_deref(), Some(&b"Tokyo"[..]));

    drop((sync_a, sync_r, sync_f, eng_a, eng_r, eng_f));
    cleanup(&pa);
    cleanup(&pr);
    cleanup(&pf);
}

/// どの vid 空間にも戻せない Tag の cell (版数の peer の語彙を relay が持っていない) は今も配らず、 counter に載る。
#[test]
fn a_tag_cell_no_vid_space_can_carry_is_counted_as_dropped() {
    let (pa, pr) = (tmp_path("a4"), tmp_path("r4"));
    let _cleanup = Cleanup(vec![pa.clone(), pr.clone()]);
    let (eng_a, eng_r, sync_a, sync_r) = relayed_pair(&pa, &pr);
    let (before_recs, _) = eng_r.state_records_for(1);
    let base = eng_r.state_records_dropped();

    // peer 9 が中継した体の record: 語彙は peer 9 の空間、 版数は peer 5 の時計 (relay は peer 5 の語彙を知らない)
    let target = *eng_r.pull_raw("notes.note", 2).first().expect("relay に届いている");
    let city = eng_r.himo_id("notes.city").unwrap() as u16;
    let world = eng_r
        .translated_locals_of(1)
        .into_iter()
        .find(|(_, local)| *local == enchudb_oplog::eid_local(target))
        .map(|(foreign, _)| enchudb_oplog::make_eid(1, foreign))
        .expect("行 2 の世界番号");
    let later = Hlc { wall: eng_r.cell_hlc(target, city).wall + 10_000, logical: 0, peer: 5 };
    let out = sync_r.apply_records(&[
        WireRecord::unsigned(later, 9, DecodedOp::Vocab { vid: 777, bytes: b"Nagoya".to_vec() }),
        WireRecord::unsigned(later, 9, DecodedOp::Tie { eid: world, himo_id: city, value: 777 }),
    ]);
    assert_eq!(out.applied, 2, "前提: 書き換えが relay に当たった: {out:?}");
    assert_eq!(eng_r.get_text_owned(target, "notes.city").as_deref(), Some(&b"Nagoya"[..]));

    let (after_recs, _) = eng_r.state_records_for(1);
    assert_eq!(eng_r.state_records_dropped() - base, 1, "戻せない cell を落としたのに counter に載っていない (#236)");
    assert_eq!(after_recs.len(), before_recs.len() - 1, "落ちた分だけ record が減る");

    drop((sync_a, sync_r, eng_a, eng_r));
    cleanup(&pa);
    cleanup(&pr);
}

/// self 版 (`state_records`) は版数 ZERO を `as_of` で stamp して配るので、
/// counter を汚さないこと — #236 は replica 経路の話。
#[test]
fn self_state_records_do_not_count_as_dropped() {
    let pa = tmp_path("a3");
    let _cleanup = Cleanup(vec![pa.clone()]);
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
