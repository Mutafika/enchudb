//! #217 — ack の walk は longest-consumed-prefix であること。
//!
//! 旧 `ack_sync_up_to_hlc` は lsn 降順の first-match で、 relay 混在 ring
//! (relayed append が原 HLC 素通しで乗る = lsn 順で HLC 非単調) では高 lsn の
//! 古い HLC row に match し、 その下の未消化 row (hlc > cursor) を watermark の
//! 下に巻き込んで reclaim していた (over-ack)。 doc の「安全側に落ちる」は逆。
//!
//! 検証 (Syncer を使わず engine API 直叩きの決定論構成):
//! 1. `scalar_ack_stops_at_first_unconsumed_row` — ring [R@old.., R@new.., relayed@mid]
//!    に scalar cursor=mid を当てても、 R@new (未消化) を越えない。 scalar は
//!    self-author 行の証明としてのみ解釈され (退化形)、 relayed 行は vector ack が
//!    来るまで prefix を止める。
//! 2. `vector_ack_consumes_per_author` — author 別 cursor で prefix が正しく前進し、
//!    未知 author は ZERO 起点で必ず止まる。 前回 ack からの再開も兼ねて検証。
//! 3. `dead_row_is_purged_not_a_permanent_blocker` — payload 欠落 row は削除して
//!    越える (走査停止だと誰の ack も永久にそこで止まり ring が満杯になる)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue217-{}-{}-{}",
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

/// note=i を 1 件 author して ring まで bridge する。
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

/// ring の生存 row を (ring_lsn, author, hlc) で昇順に。
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

/// A の ring から WireRecord 列を取り出す (relay 入力用)。
fn wire_records(eng: &Arc<Engine>) -> Vec<WireRecord> {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(WireRecord::from)
        .collect()
}

/// [R own (古い)] → [A の記録 (中間 HLC)] → [R own (新しい)] → [relay append] の
/// ring を組む。 返り値 (eng_r, t_mid, batch2_min_lsn, batch2_max_lsn, relayed_max_lsn)。
fn build_mixed_ring(
    pa: &str,
    pr: &str,
) -> (Arc<Engine>, Hlc, u32, u32, u32) {
    let eng_a = make_engine(pa, 1);
    let eng_r = make_engine(pr, 2);

    author_note(&eng_r, 900); // batch1: R own、最古
    tick();
    author_note(&eng_a, 1); // A の記録 (HLC は batch1 と batch2 の間)
    tick();
    author_note(&eng_r, 901); // batch2: R own、最新 HLC

    // relay: A の record を原型のまま R の WAL → ring へ (batch2 より高い lsn、
    // 古い HLC)。
    for rec in wire_records(&eng_a) {
        eng_r.relay_record(&rec);
    }
    eng_r.oplog_sync().unwrap();
    eng_r.transfer_oplog_to_sync_ops();

    let rows = ring_rows(&eng_r);
    let t_mid = rows
        .iter()
        .filter(|(_, a, _)| *a == 1)
        .map(|(_, _, h)| *h)
        .max()
        .expect("relayed rows exist");
    let batch2: Vec<u32> = rows
        .iter()
        .filter(|(_, a, h)| *a == 2 && *h > t_mid)
        .map(|(l, _, _)| *l)
        .collect();
    let relayed_max = rows
        .iter()
        .filter(|(_, a, _)| *a == 1)
        .map(|(l, _, _)| *l)
        .max()
        .unwrap();
    let (b2_min, b2_max) =
        (*batch2.iter().min().unwrap(), *batch2.iter().max().unwrap());
    assert!(
        relayed_max > b2_max,
        "前提: relayed row は batch2 より高い lsn に乗る (rows: {rows:?})"
    );
    (eng_r, t_mid, b2_min, b2_max, relayed_max)
}

#[test]
fn scalar_ack_stops_at_first_unconsumed_row() {
    let pa = tmp_path("a1");
    let pr = tmp_path("r1");
    let (eng_r, t_mid, b2_min, b2_max, relayed_max) = build_mixed_ring(&pa, &pr);

    // scalar cursor = t_mid: batch1 (R own, hlc < t_mid) は消化済み扱い、
    // batch2 (R own, hlc > t_mid) は未消化 → prefix はそこで止まる。
    // 旧実装は降順走査で relayed@t_mid (最高 lsn) に即 match し、 未消化の
    // batch2 を watermark の下に巻き込んでいた。
    let ack = eng_r.ack_sync_up_to_hlc(3, t_mid).unwrap();
    assert!(ack > 0, "batch1 分は ack されること");
    assert!(
        ack < b2_min,
        "#217 over-ack: 未消化の batch2 (lsn {b2_min}..={b2_max}) を越えて \
         lsn {ack} まで ack した (relayed_max = {relayed_max})"
    );

    // reclaim しても batch2 と relayed row は生存すること。
    eng_r.reclaim_sync_ops();
    let after: Vec<u32> = ring_rows(&eng_r).iter().map(|(l, _, _)| *l).collect();
    assert!(
        after.contains(&b2_min) && after.contains(&b2_max) && after.contains(&relayed_max),
        "未消化 row が reclaim で消えた: {after:?}"
    );

    cleanup(&pa);
    cleanup(&pr);
}

#[test]
fn vector_ack_consumes_per_author() {
    let pa = tmp_path("a2");
    let pr = tmp_path("r2");
    let (eng_r, t_mid, _b2_min, b2_max, relayed_max) = build_mixed_ring(&pa, &pr);
    let t_new = ring_rows(&eng_r)
        .iter()
        .filter(|(_, a, _)| *a == 2)
        .map(|(_, _, h)| *h)
        .max()
        .unwrap();

    // step 1: R own は全消化、 A は未知 (cursor 無し) → relayed row で止まる。
    let ack1 = eng_r.ack_sync_up_to_cursors(3, &[(2, t_new)]).unwrap();
    assert_eq!(
        ack1, b2_max,
        "未知 author の relayed row は prefix を止めること (ZERO 起点)"
    );

    // step 2: A の cursor も渡す → 前回 ack からの再開で relayed row を消化。
    let ack2 = eng_r.ack_sync_up_to_cursors(3, &[(2, t_new), (1, t_mid)]).unwrap();
    assert_eq!(ack2, relayed_max, "vector ack で relayed row まで前進すること");

    // step 3: reclaim で ring が空になり、 それ以上 ack するものが無い。
    eng_r.reclaim_sync_ops();
    let ack3 = eng_r.ack_sync_up_to_cursors(3, &[(2, t_new), (1, t_mid)]).unwrap();
    assert_eq!(ack3, 0, "生存 row が無ければ ack しない (先端への嘘 ack をしない)");

    cleanup(&pa);
    cleanup(&pr);
}

/// author 0 = peer identity 設定前の local 著作 (単独 peer 運用 → 後から sync
/// 参加、 の正規 path)。 scalar ack が self 限定だと author=0 row は dead row でも
/// ないのに永久 prefix blocker になる — self と同じ扱いで越えられること。
#[test]
fn author_zero_rows_are_consumable_by_scalar_ack() {
    let p = tmp_path("z");
    cleanup(&p);
    let mut eng = Engine::create_with_capacity(&p, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    // set_peer_id 前に書く → ring に author=0 の row が入る
    author_note(&eng, 1);
    tick();
    eng.set_peer_id(2);
    author_note(&eng, 2);

    let rows = ring_rows(&eng);
    assert!(
        rows.iter().any(|(_, a, _)| *a == 0),
        "前提: author=0 row が ring にある (rows: {rows:?})"
    );
    let head_hlc = rows.iter().map(|(_, _, h)| *h).max().unwrap();
    let head_lsn = rows.iter().map(|(l, _, _)| *l).max().unwrap();
    let ack = eng.ack_sync_up_to_hlc(3, head_hlc).unwrap();
    assert_eq!(
        ack, head_lsn,
        "author=0 row が prefix blocker になっている (ack {ack} / head {head_lsn})"
    );

    cleanup(&p);
}

/// 0.23.x 以前の降順 first-match が over-ack した `consumed_lsn` が残る DB を
/// 模す。 walk は stored を再開点に信用せず 0 から検証し、 下方修正 (heal)
/// する — 信用すると「真の prefix と膨張値の間の row」が未検査のまま次の
/// reclaim で消える。
#[test]
fn inflated_stored_consumed_lsn_is_not_trusted() {
    let pa = tmp_path("a4");
    let pr = tmp_path("r4");
    let (eng_r, t_mid, b2_min, b2_max, relayed_max) = build_mixed_ring(&pa, &pr);

    // 旧実装の over-ack を再現: stored が ring 先端まで膨らんでいる。
    eng_r.ack_sync(3, relayed_max).unwrap();

    // session 最初の walk = 全 ring 検証 → 真の prefix (batch1 末尾) へ下方修正。
    let ack = eng_r.ack_sync_up_to_hlc(3, t_mid).unwrap();
    assert!(
        ack > 0 && ack < b2_min,
        "heal が効いていない (ack {ack}, batch2 {b2_min}..={b2_max})"
    );

    // reclaim しても未消化 row (batch2 / relayed) は生存すること。
    eng_r.reclaim_sync_ops();
    let after: Vec<u32> = ring_rows(&eng_r).iter().map(|(l, _, _)| *l).collect();
    assert!(
        after.contains(&b2_min) && after.contains(&b2_max) && after.contains(&relayed_max),
        "膨張 stored を信用して未消化 row が reclaim された: {after:?}"
    );

    cleanup(&pa);
    cleanup(&pr);
}

#[test]
fn dead_row_is_purged_not_a_permanent_blocker() {
    let pa = tmp_path("a3");
    let pr = tmp_path("r3");
    let (eng_r, t_mid, _b2_min, b2_max, relayed_max) = build_mixed_ring(&pa, &pr);
    let t_new = ring_rows(&eng_r)
        .iter()
        .filter(|(_, a, _)| *a == 2)
        .map(|(_, _, h)| *h)
        .max()
        .unwrap();

    // batch2 の最終 row の payload を欠落させる = decode 不能な dead row。
    let dead_eid = *eng_r.pull_raw("_sync_ops.lsn", b2_max).first().expect("row exists");
    eng_r.untie(dead_eid, "_sync_ops.payload");

    // 全 author の cursor を渡す → dead row は削除して越え、 relayed row まで届く。
    let ack = eng_r.ack_sync_up_to_cursors(3, &[(2, t_new), (1, t_mid)]).unwrap();
    assert_eq!(
        ack, relayed_max,
        "dead row が permanent blocker になっている (ack = {ack})"
    );
    assert_eq!(eng_r.sync_dead_rows_purged(), 1, "dead row の計数");
    assert!(
        eng_r.pull_raw("_sync_ops.lsn", b2_max).is_empty(),
        "dead row 本体が削除されていること"
    );

    cleanup(&pa);
    cleanup(&pr);
}
