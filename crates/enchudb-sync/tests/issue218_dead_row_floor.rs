//! #218 — decode 不能な `_sync_ops` row を purge しても history floor が上がらない。
//!
//! purge した record は ring から永久に消えるのに、 floor (= 「差分 pull で配れない
//! 履歴の上限」) は **decode できた row からしか**作っていなかった。 floor の過少申告 =
//! その HLC 帯を必要としていた puller が「cursor >= floor だから差分で追える」と
//! 誤判定する = **#140 で塞いだ silent partial の復活**。
//!
//! 穴は 2 箇所あった:
//!
//! - `reclaim_sync_ops` — decode 不能でも `lsn < watermark` なら消すが、
//!   `reclaimed_max` は decode できた row でしか更新しない
//! - `ack_sync_prefix` の dead-row 分岐 (#217 で追加) — 消して計数して warn するだけで、
//!   floor に一切触らない
//!
//! fix は purge 直前に `(author, HLC 上界)` を作って floor 候補に積む。 author は
//! **payload とは別の列** (`_sync_ops.peer_id`) から取れるので、 無帰属 baseline
//! (`u32::MAX` = 全 author の follower が bootstrap) に倒さずに済む。 HLC は
//! `mint_local_hlc()` (= 今の HLC、 観測済み HLC 全部の上界)。 詳細は
//! `Engine::dead_row_floor_candidate` の doc。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue218-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
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

/// note を 1 件 author して ring まで bridge する。
fn author_note(eng: &Arc<Engine>, i: u32) {
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", i);
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

/// HLC の wall を確実に進める (同 wall 内の logical 順は engine 間で比較不能)。
fn tick() {
    std::thread::sleep(std::time::Duration::from_millis(15));
}

/// ring の生存 row を `(ring_lsn, author, hlc)` で lsn 昇順に。
/// relayed row の payload 内 lsn は **author の lsn** (#209 素通し) なので、
/// ring 位置は lsn column から読む。
fn ring_rows(eng: &Arc<Engine>) -> Vec<(u32, PeerId, Hlc)> {
    let lsn_hid = eng.himo_id("_sync_ops.lsn").unwrap() as u16;
    let payload_hid = eng.himo_id("_sync_ops.payload").unwrap() as u16;
    let mut out = Vec::new();
    for eid in eng.entities_with_himo(lsn_hid) {
        let Some(lsn) = eng.get_by_id(eid, lsn_hid) else { continue };
        let Some(vid) = eng.get_by_id(eid, payload_hid) else { continue };
        let bytes = eng.vocab_text(vid).to_vec();
        let Some(rec) = enchudb_oplog::oplog::decode_sync_ops_payload(&bytes) else { continue };
        out.push((lsn, rec.author_peer, rec.hlc));
    }
    out.sort_by_key(|r| r.0);
    out
}

fn wire_records(eng: &Arc<Engine>) -> Vec<WireRecord> {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(WireRecord::from)
        .collect()
}

fn row_eid(eng: &Arc<Engine>, lsn: u32) -> u64 {
    *eng.pull_raw("_sync_ops.lsn", lsn).first().expect("row exists")
}

fn floor_of(eng: &Arc<Engine>, author: u32) -> Option<Hlc> {
    eng.sync_reclaimed_floors()?
        .into_iter()
        .find(|(a, _)| *a == author)
        .map(|(_, h)| h)
}

/// relay R (peer 2) の ring: [R own(古)] [A relayed ×2] [R own(新)]。
/// 戻り値は `(eng_r, eng_a, relayed_lsns 昇順, 最後の R own row の lsn)`。
///
/// 「relayed が最新 lsn ではない」ようにしてあるのは #235 の guard
/// (未完成 row = 最新 lsn は dead 判定しない) を避けるため — ここで見たいのは
/// **消した row の floor 意味論**であって、 未完成 row の保護ではない。
fn build_relay_ring(pa: &str, pr: &str) -> (Arc<Engine>, Arc<Engine>, Vec<u32>, u32) {
    let eng_a = make_engine(pa, 1);
    let eng_r = make_engine(pr, 2);

    author_note(&eng_r, 900);
    tick();
    author_note(&eng_a, 1);
    tick();
    author_note(&eng_a, 2);

    for rec in wire_records(&eng_a) {
        eng_r.relay_record(&rec);
    }
    eng_r.oplog_sync().unwrap();
    eng_r.transfer_oplog_to_sync_ops();

    tick();
    author_note(&eng_r, 901);

    let rows = ring_rows(&eng_r);
    let relayed: Vec<u32> = rows
        .iter()
        .filter(|(_, a, _)| *a == 1)
        .map(|(l, _, _)| *l)
        .collect();
    let own_max = rows
        .iter()
        .filter(|(_, a, _)| *a == 2)
        .map(|(l, _, _)| *l)
        .max()
        .unwrap();
    assert!(
        relayed.len() >= 2,
        "前提: relayed row が 2 本以上 (rows: {rows:?})"
    );
    assert!(
        own_max > *relayed.last().unwrap(),
        "前提: relayed row は最新 lsn ではない (rows: {rows:?})"
    );
    // #209 の verbatim relay は eid を翻訳しないので、 relay は
    // `replicated_authors()` (eid_translator 由来) では author 1 を知らない。
    // 帰属の材料は ring 内の decodable row が名乗る author の方。
    assert!(
        !eng_r.replicated_authors().contains(&1),
        "前提が変わった: verbatim relay が eid を翻訳するようになったなら \
         `Engine::ring_authors` の doc を見直すこと"
    );
    (eng_r, eng_a, relayed, own_max)
}

/// `reclaim_sync_ops` が decode 不能 row を消したら、 **その row の author の**
/// floor が上がること。
///
/// 壊すのは author 1 の relayed row のうち **HLC 最大の方**。 これが要点で、
/// 壊すのが中間の row だと 「上にある decodable な同 author row」 が結果的に floor を
/// 押し上げてしまい、 fix の有無で差が出ない (= test が何も見ていない)。
/// 「消えた最大 HLC が floor」 という #191 の意味論を、 その最大が decode 不能な
/// ときに保てるか、 がこの issue。
#[test]
fn reclaim_purge_of_undecodable_row_raises_floor_for_its_author() {
    let pa = tmp_path("a1");
    let pr = tmp_path("r1");
    let (eng_r, eng_a, relayed, own_max) = build_relay_ring(&pa, &pr);

    let dead_lsn = *relayed.last().unwrap();
    let dead_hlc = ring_rows(&eng_r)
        .into_iter()
        .find(|(l, _, _)| *l == dead_lsn)
        .map(|(_, _, h)| h)
        .unwrap();
    eng_r.untie(row_eid(&eng_r, dead_lsn), "_sync_ops.payload");

    // dead row までを purge 対象に (最後の R own row は残す = #235 guard を避ける)。
    eng_r.ack_sync(9, dead_lsn + 1).unwrap();
    assert!(eng_r.reclaim_sync_ops() > 0, "reclaim が走っていること");
    assert!(
        eng_r.pull_raw("_sync_ops.lsn", dead_lsn).is_empty(),
        "dead row 本体は消えていること (残っているなら前提が崩れている)"
    );
    assert!(
        !eng_r.pull_raw("_sync_ops.lsn", own_max).is_empty(),
        "watermark より上の row は残っていること"
    );

    let f1 = floor_of(&eng_r, 1).expect("author 1 の floor entry");
    assert!(
        f1 > dead_hlc,
        "消した dead row ({dead_hlc:?}) より floor[1] ({f1:?}) が低い = 過少申告。 \
         この帯を必要とする puller が truncation 通知を受け取れない"
    );
    assert!(
        floor_of(&eng_r, u32::MAX).is_none(),
        "author が `_sync_ops.peer_id` から取れているのに無帰属 baseline に倒れている \
         (= 無関係な author の follower まで bootstrap する)"
    );

    drop(eng_a);
    drop(eng_r);
    cleanup(&pa);
    cleanup(&pr);
}

/// `ack_sync_prefix` の dead-row 分岐 (#217) も同じ穴を持っていた — こちらは
/// floor に**一切**触っていなかった。
#[test]
fn ack_prefix_purge_of_dead_row_raises_floor() {
    let pa = tmp_path("a2");
    let pr = tmp_path("r2");
    let (eng_r, eng_a, relayed, _own_max) = build_relay_ring(&pa, &pr);

    let dead_lsn = *relayed.last().unwrap();
    let dead_hlc = ring_rows(&eng_r)
        .into_iter()
        .find(|(l, _, _)| *l == dead_lsn)
        .map(|(_, _, h)| h)
        .unwrap();
    eng_r.untie(row_eid(&eng_r, dead_lsn), "_sync_ops.payload");

    // 全 author の cursor を「全部消化済み」で渡す → prefix walk が dead row を
    // 削除して越える。
    let far = Hlc { wall: u64::MAX, logical: u32::MAX, peer: u32::MAX };
    eng_r.ack_sync_up_to_cursors(9, &[(1, far), (2, far)]).unwrap();
    assert_eq!(eng_r.sync_dead_rows_purged(), 1, "dead row を 1 件消していること");

    let f1 = floor_of(&eng_r, 1).expect(
        "ack prefix walk が消した dead row の分の floor entry \
         (#217 の分岐は floor に一切触っていなかった)",
    );
    assert!(f1 > dead_hlc, "floor[1] ({f1:?}) が dead row ({dead_hlc:?}) を覆っていない");

    drop(eng_a);
    drop(eng_r);
    cleanup(&pa);
    cleanup(&pr);
}

/// `_sync_ops.peer_id` が読めない row は帰属できない → 無帰属 baseline
/// (`u32::MAX`、 puller が全 author に畳み込む) に落ちること。
#[test]
fn dead_row_without_peer_id_falls_back_to_baseline() {
    let pa = tmp_path("a3");
    let pr = tmp_path("r3");
    let (eng_r, eng_a, relayed, _own_max) = build_relay_ring(&pa, &pr);

    let dead_lsn = *relayed.last().unwrap();
    let eid = row_eid(&eng_r, dead_lsn);
    eng_r.untie(eid, "_sync_ops.payload");
    eng_r.untie(eid, "_sync_ops.peer_id");

    eng_r.ack_sync(9, dead_lsn + 1).unwrap();
    eng_r.reclaim_sync_ops();

    assert!(
        floor_of(&eng_r, u32::MAX).is_some(),
        "帰属不能な dead row が floor に反映されていない = silent gap"
    );

    drop(eng_a);
    drop(eng_r);
    cleanup(&pa);
    cleanup(&pr);
}

/// row 全体が壊れると `peer_id` も**ゴミ値**になり得る。 ゴミを信じると
/// (a) 無関係な author の follower が余計に bootstrap し、 (b) 本当の author の
/// follower は silent gap のまま、 という最悪の組み合わせになるので、
/// **知らない author なら baseline に落とす**こと。
#[test]
fn dead_row_with_implausible_peer_id_falls_back_to_baseline() {
    let pa = tmp_path("a4");
    let pr = tmp_path("r4");
    let (eng_r, eng_a, relayed, _own_max) = build_relay_ring(&pa, &pr);

    let dead_lsn = *relayed.last().unwrap();
    let eid = row_eid(&eng_r, dead_lsn);
    eng_r.untie(eid, "_sync_ops.payload");
    // ring のどの decodable row も名乗っていない peer id。
    eng_r.tie_to(eid, "_sync_ops.peer_id", 777);
    assert!(ring_rows(&eng_r).iter().all(|(_, a, _)| *a != 777));

    eng_r.ack_sync(9, dead_lsn + 1).unwrap();
    eng_r.reclaim_sync_ops();

    assert!(
        floor_of(&eng_r, 777).is_none(),
        "ゴミ値の peer id をそのまま author として信じている"
    );
    assert!(
        floor_of(&eng_r, u32::MAX).is_some(),
        "帰属できないなら baseline に落ちること (落ちないと本当の author の \
         follower が silent gap のまま)"
    );

    drop(eng_a);
    drop(eng_r);
    cleanup(&pa);
    cleanup(&pr);
}

/// 逆向きの歯止め: **健全な reclaim で無帰属 baseline を立てない**こと。
///
/// baseline は全 author に畳み込まれるので、 平常運転で立ててしまうと reclaim の
/// たびに全 follower が bootstrap へ飛ぶ (= #191 で潰した挙動への静かな退化)。
///
/// **この 1 本だけは fix の前後どちらでも緑** — 他の 5 本と違って回帰の再現ではなく、
/// 「保守側に倒す」 fix が行き過ぎていないことの歯止めだから。 帰属を諦めて全部
/// baseline に倒す実装に将来書き換わったら、 ここが落ちる。
#[test]
fn healthy_reclaim_does_not_raise_baseline() {
    let pa = tmp_path("a5");
    let pr = tmp_path("r5");
    let (eng_r, eng_a, _relayed, own_max) = build_relay_ring(&pa, &pr);

    eng_r.ack_sync(9, own_max).unwrap();
    assert!(eng_r.reclaim_sync_ops() > 0, "reclaim が走っていること");
    assert_eq!(eng_r.sync_dead_rows_purged(), 0, "健全な系で dead row は出ない");

    let floors = eng_r.sync_reclaimed_floors().expect("floor は記録される");
    assert!(
        floors.iter().all(|(a, _)| *a != u32::MAX),
        "健全な reclaim で無帰属 baseline が立っている ({floors:?}) — \
         全 author の follower が毎回 bootstrap に飛ぶ"
    );
    assert!(
        floors.iter().any(|(a, _)| *a == 1) && floors.iter().any(|(a, _)| *a == 2),
        "author 別 floor が両方立っていること ({floors:?})"
    );

    drop(eng_a);
    drop(eng_r);
    cleanup(&pa);
    cleanup(&pr);
}

/// **本題** — puller から見える帰結。
///
/// 上の 4 本は engine 内部の field を見ているだけで、 #217 の test と同じ弱さ
/// (前提を手で作って同じ仮定を assert する) を持つ。 floor が存在する理由は
/// 「差分で埋まらない穴を puller に伝える」ことなので、 pull の戻り値で駆動する。
///
/// 構成: A の ring の**最古の row だけ**を decode 不能にして purge する。 purge 対象が
/// その 1 行だけなので、 fix 前は `reclaimed_max` が空 = **floor がそもそも記録されない**。
/// B は cursor 無しで pull し、 穴の空いた batch を 「正常」 として受け取る。
#[test]
fn puller_below_a_purged_dead_row_is_told_truncated() {
    let pa = tmp_path("a6");
    let pb = tmp_path("b6");
    let mem = Arc::new(InMemoryTransport::new());
    for p in [1u32, 2] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();

    let eng_a = make_engine(&pa, 1);
    let eng_b = make_engine(&pb, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());

    author_note(&eng_a, 1);
    tick();
    for i in 2..7u32 {
        author_note(&eng_a, i);
    }

    let rows = ring_rows(&eng_a);
    let oldest = rows.first().map(|(l, _, _)| *l).expect("ring に row がある");
    eng_a.untie(row_eid(&eng_a, oldest), "_sync_ops.payload");

    // 最古の 1 行だけを purge 対象にする。
    eng_a.ack_sync(2, oldest + 1).unwrap();
    assert_eq!(
        eng_a.reclaim_sync_ops(),
        1,
        "purge されるのは壊した 1 行だけ (これが崩れると fix 前後で差が出ない)"
    );

    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);

    assert!(
        out.history_truncated,
        "配れない record ができたのに puller が truncation を知らされていない — \
         B は穴の空いた batch を正常として受け取る (#140 の silent partial): {out:?}"
    );
    assert_eq!(out.applied, 0, "truncation 時は一切適用しない: {out:?}");
    assert_eq!(
        out.truncated_authors,
        vec![1],
        "bootstrap 対象の author が名指しされていること: {out:?}"
    );

    drop(sync_a);
    drop(sync_b);
    drop(eng_a);
    drop(eng_b);
    cleanup(&pa);
    cleanup(&pb);
}
