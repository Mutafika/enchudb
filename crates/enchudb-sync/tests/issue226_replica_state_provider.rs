//! #226 — relay/replica が author の live state を配れない。
//!
//! `Engine::state_records()` は translated local を全部 skip するので、 author の行を
//! まさに translated local として持っている relay の state batch は**常に空**だった
//! (実測: note 3 件を relay 済みの R で `state_records()` = 0 records)。 加えて
//! `serve_state` は self_peer の 1 key でしか provider を登録しないので、
//! `fetch_state(A)` が relay に当たらない。 結果、 relay 経由でしか author に届かない
//! follower は `history_truncated` から回復できなかった (#140 は author 直結専用)。
//!
//! fix: `state_records_for(author)` (原 eid / 原 author / 原 HLC / author の vid 空間に
//! 戻して合成) + `register_state_provider_for(author, by, ..)` + `bootstrap_pull_via`
//! (cursor は link の下、 author の下ではない)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue226-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in
        ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".cursors", ".cursors.tmp"]
    {
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

fn note_body(eng: &Arc<Engine>, i: u32) -> Option<String> {
    let hits = eng.pull_raw("notes.note", i);
    let eid = *hits.first()?;
    eng.get_text_owned(eid, "notes.body").map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn note_city(eng: &Arc<Engine>, i: u32) -> Option<String> {
    let hits = eng.pull_raw("notes.note", i);
    let eid = *hits.first()?;
    eng.get_text_owned(eid, "notes.city").map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// relay が bridge して publish するまでの 1 サイクル。
fn relay_cycle(eng: &Arc<Engine>, sync: &Syncer, from: PeerId) {
    sync.pull_once(from);
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    sync.publish_since(Hlc::ZERO);
}

/// wire record を HLC 抜きで比較できる形に落とす (合成順序に依存しない集合比較用)。
fn shape(op: &DecodedOp) -> String {
    match op {
        DecodedOp::Tie { eid, himo_id, value } => format!("Tie {eid} {himo_id} {value}"),
        DecodedOp::TieRef { eid, himo_id, target } => format!("TieRef {eid} {himo_id} {target}"),
        DecodedOp::TieLeaf { eid, himo_name, bytes, .. } => {
            format!("TieLeaf {eid} {himo_name} {}", String::from_utf8_lossy(bytes))
        }
        DecodedOp::Vocab { vid, bytes } => {
            format!("Vocab {vid} {}", String::from_utf8_lossy(bytes))
        }
        other => format!("{other:?}"),
    }
}

/// replica が合成する wire 形は、 author 本人が合成する wire 形と一致すること。
///
/// これが崩れると relay 経由 bootstrap は「別物」を配る。 特に Tag は **author の
/// vid 空間**に戻す必要がある — relay の local vid を `(author, vid)` として配ると、
/// author 直 pull で来る同じ key の別テキストと衝突して vocab 写像が壊れる (#209 と同種)。
#[test]
fn replica_state_matches_author_state() {
    let (pa, pr) = (tmp_path("a1"), tmp_path("r1"));
    let transport = make_transport(&[1, 2]);
    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    // relay 側に**先に**自分の語彙を作って vid 空間をずらす — これが無いと
    // 「たまたま vid が一致していただけ」の test になる。
    for (i, city) in [(900u32, "Osaka"), (901, "Kyoto"), (902, "Nara")] {
        author_note(&eng_r, i, city);
    }

    for i in 1..=3 {
        author_note(&eng_a, i, "Tokyo");
    }
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    assert_eq!(note_body(&eng_r, 2).as_deref(), Some("body 2"), "前提: R に relay 済み");

    let (a_recs, _a_as_of) = eng_a.state_records();
    let (r_recs, r_as_of) = eng_r.state_records_for(1);

    assert!(!r_recs.is_empty(), "replica の state が空 (#226 の症状そのもの)");
    assert!(
        r_recs.iter().all(|r| r.author_peer == 1),
        "replica の record は author 本人の author_peer を名乗ること"
    );
    assert!(
        r_recs.iter().all(|r| r.hlc != Hlc::ZERO && r.hlc <= r_as_of),
        "replica の HLC は原 HLC (非 ZERO) で、 as_of = その max"
    );

    let a_shapes: std::collections::BTreeSet<String> =
        a_recs.iter().map(|r| shape(&r.op)).collect();
    let r_shapes: std::collections::BTreeSet<String> =
        r_recs.iter().map(|r| shape(&r.op)).collect();
    assert_eq!(
        a_shapes, r_shapes,
        "replica の wire 形が author 本人と一致しない (eid / vid 空間 / himo のいずれかがズレている)"
    );

    // Tag の vid が relay の local vid のままなら、 R の vocab 上で "Osaka"/"Kyoto"/
    // "Nara" に割り当てられた vid が漏れる。 上の集合一致がそれを禁じている。
    let vocab_bytes: Vec<String> = r_recs
        .iter()
        .filter_map(|r| match &r.op {
            DecodedOp::Vocab { bytes, .. } => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        })
        .collect();
    assert_eq!(vocab_bytes, vec!["Tokyo".to_string()], "author の Tag 語彙だけを配ること");
}

/// author に直接届かない follower が、 relay 経由で author の state を bootstrap し、
/// **その後の差分 pull が link の下の cursor で継続する**こと。
#[test]
fn truncated_follower_recovers_via_replica() {
    let (pa, pr, pc) = (tmp_path("a2"), tmp_path("r2"), tmp_path("c2"));
    let transport = make_transport(&[1, 2, 3]);
    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let eng_c = make_engine(&pc, 3);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    for i in 1..=3 {
        author_note(&eng_a, i, "Tokyo");
    }
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    // A 本人は provider を出さない (落ちている / 直接届かない)。 R だけが名乗る。
    sync_r.serve_state();

    // C は R 経由で author 1 の state を取る。
    let out = sync_c.bootstrap_pull_via(2, 1).expect("replica 発の state を受け取れること");
    assert!(out.outcome.applied > 0, "何も適用されていない: {:?}", out.outcome);
    assert_eq!(out.swept, 0, "replica 発は complete=false なので ghost sweep しない");
    for i in 1..=3 {
        assert_eq!(note_body(&eng_c, i).as_deref(), Some(&*format!("body {i}")), "note {i}");
        assert_eq!(note_city(&eng_c, i).as_deref(), Some("Tokyo"), "note {i} の Tag");
    }

    // cursor が **link (=2) の下の author 1** に載っていること。 author の下に
    // 書いていると、 C は link 2 から永久に author 1 の過去分を再受信し続ける。
    let again = sync_c.pull_once(2);
    assert_eq!(again.applied, 0, "bootstrap 済み分を再適用している: {again:?}");

    // 以降の差分が relay 経由で普通に流れること。
    author_note(&eng_a, 4, "Tokyo");
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    let fresh = sync_c.pull_once(2);
    assert!(fresh.applied > 0, "bootstrap 後の差分が届かない: {fresh:?}");
    assert_eq!(note_body(&eng_c, 4).as_deref(), Some("body 4"));
}

/// author 本人と replica の両方が名乗っている時、 `fetch_state` は **本人**を選ぶこと
/// (本人発だけが complete = ghost sweep を許せる)。
#[test]
fn author_provider_is_preferred_over_replica() {
    let (pa, pr, pc) = (tmp_path("a3"), tmp_path("r3"), tmp_path("c3"));
    let transport = make_transport(&[1, 2, 3]);
    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let eng_c = make_engine(&pc, 3);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    for i in 1..=2 {
        author_note(&eng_a, i, "Tokyo");
    }
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    sync_r.serve_state();

    // R が relay した**後**に A が書いた行 — R は持っていない。
    author_note(&eng_a, 42, "Tokyo");
    sync_a.serve_state();

    let out = sync_c.bootstrap_pull(1).expect("state を受け取れること");
    assert!(
        note_body(&eng_c, 42).is_some(),
        "replica 発 (note 42 を含まない) が選ばれている — 本人優先が効いていない"
    );
    assert!(out.outcome.applied > 0);
}

/// relay でない peer は他 author の replica 配布元として名乗らないこと
/// (持っていても転送経路が無いので、 名乗ると回復できない先を指す)。
#[test]
fn non_relay_peer_serves_only_itself() {
    let (pa, pr) = (tmp_path("a4"), tmp_path("r4"));
    let transport = make_transport(&[1, 2]);
    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    // gossip OFF = ただの follower。

    author_note(&eng_a, 1, "Tokyo");
    sync_a.publish_since(Hlc::ZERO);
    sync_r.pull_once(1);
    assert_eq!(note_body(&eng_r, 1).as_deref(), Some("body 1"), "前提: R に届いている");

    sync_r.serve_state();
    assert!(
        transport.fetch_state(1).is_none(),
        "follower が author 1 の配布元を名乗ってしまっている"
    );
    assert!(transport.fetch_state(2).is_some(), "自分自身の state は配れること");
}

/// truncation は **どの author の穴か**を返すこと。 relay link には複数 author の
/// stream が乗るので、 bool だけでは `bootstrap_pull_via` の対象が決まらない。
#[test]
fn truncation_names_the_author() {
    let (pa, pr, pc) = (tmp_path("a5"), tmp_path("r5"), tmp_path("c5"));
    let transport = make_transport(&[1, 2, 3]);
    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let eng_c = make_engine(&pc, 3);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    // C は relay R から A と R 両方の row を取り込んで cursor を作る。
    author_note(&eng_r, 900, "Osaka");
    author_note(&eng_a, 1, "Tokyo");
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    assert!(sync_c.pull_once(2).applied > 0, "前提: C が link 2 の cursor を持つ");

    // author 1 の分だけ floor が cursor を追い越した状況を作る。
    transport
        .set_history_floor_multi(2, &[(1, Hlc { wall: u64::MAX, logical: 0, peer: 1 })]);
    let out = sync_c.pull_once(2);
    assert!(out.history_truncated, "truncation が立っていない: {out:?}");
    assert_eq!(
        out.truncated_authors,
        vec![1],
        "穴を空けた author (1) だけを名指しすること — R (2) は無傷"
    );
}
