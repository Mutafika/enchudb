//! #970 (PR #977 の読み直し 3 の [1]): 受信の払い出しが、 場所の世代を書く gen.seg を伸ばせない (ディスクの空き不足) 時、 record を
//! 落とさず (`Dropped` = cursor が越えて二度と届かない)、 容量の断り (`RejectedCapacity`) にして再配送させる。 空きが戻れば同じ record
//! が届き直して当たる (#210 と同じ扱い)。
//!
//! 2 つの形:
//!
//! - v15 の受け手で、 新しい行の場所が gen.seg の commit の外の page にある (案 P: 生かす前にその page を commit する)
//! - v14 の受け手で、 満杯の表に v14 の間に消した場所がある (受けた行を置くには使い回す = 先に v15 に移る。 移る手順が gen.seg を
//!   commit できない)。 前は移る手順の断りが確保先なし (`Skip`) に潰れて `Dropped` になった (master では移って当たっていた)
//!
//! 空きの不足は `set_space_margin` / `set_gen_space_margin` で作る (#200 の fault injection)。 v15 の受け手は gen.seg だけの余裕
//! (`set_gen_space_margin`、 PR #977 の読み直し 4 の [11]: 全部の segment の余裕では、 列の cell の commit の断り (#210 の前からの道)
//! でも `RejectedCapacity` になって通る)。
//!
//! 読み直し 4 の [1]: 名前で束ねた行の切り離し (#581 / #714 の削除、 #751 の改名) が、 移す / 消えた slot にする場所を払い出せない時も
//! 同じ (前は 「確保先なし」 に潰れて、 削除は後の identity ごと行を消す / 消える identity を付け替えずに束ねから外す、 改名は切り離さずに
//! 束ねた行へ当てた。 どれも `Applied` で cursor が進み、 再配送されずに恒久に食い違った)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

const NO_SPACE: u64 = u64::MAX / 2;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-issue970-sync-{tag}-{}-{nanos}.db", std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(format!("{path}.oplog"));
}

/// 両方の peer の表: `u` (枠 1、 最初の使い回し用)、 `pad` (払い出さない、 `notes` を gen.seg の先の page に置く)、 `notes` (列 `n`)。
/// 紐の番号を揃えるため、 定義の順は両方同じ。 `fill` なら残りの eid 空間を表 `rest` で使い切る (`notes` は枠を足せない)。
fn make(path: &str, peer: PeerId, notes: u32, fill: bool) -> Arc<Engine> {
    let mut e = Engine::create_with_capacity(path, 400_000).unwrap();
    e.define_table("u", 1).unwrap();
    e.define_himo_in("u", "x", ValueType::Number, 0).unwrap();
    e.define_table("pad", 100_000).unwrap();
    e.define_table("notes", notes).unwrap();
    e.define_himo_in("notes", "n", ValueType::Number, 0).unwrap();
    e.enable_sync_tables().unwrap();
    if fill {
        let rest = e.remaining_eid_capacity();
        e.define_table("rest", rest).unwrap();
        assert_eq!(e.remaining_eid_capacity(), 0, "前提: 枠を足せない");
    }
    let e = Engine::concurrentize_with_oplog(e, 16 * 1024 * 1024).unwrap();
    e.set_peer_id(peer);
    e
}

/// A が `notes` に n = 7 の行を作って配る。 B は空きの無い間に受けて断り (再配送させる)、 空きが戻った後に同じ record を受けて当てる。
/// `margin`: B の空きを絞る / 戻す。
fn refused_then_redelivered(a: &Arc<Engine>, b: &Arc<Engine>, margin: fn(&Engine, u64)) {
    let v15 = b.slot_bits() > 0;
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let sa = Syncer::new(a.clone(), transport.clone());
    let sb = Syncer::new(b.clone(), transport.clone());

    let ea = a.entity_in("notes").unwrap();
    a.tie_to(ea, "notes.n", 7u32);
    a.oplog_sync().unwrap();
    assert!(sa.publish_since(Hlc::ZERO) > 0, "前提: A が配れた");

    margin(b, NO_SPACE);
    let out = sb.pull_once(1);
    assert_eq!(out.dropped_unresolved, 0, "空きの不足で払い出せない record を落とした (二度と届かない): {out:?}");
    assert!(out.rejected_capacity >= 1, "容量の断りとして数えていない: {out:?}");
    assert!(out.min_rejected_hlc.is_some(), "cursor を止めていない = この record は二度と届かない: {out:?}");
    assert!(b.pull_raw("notes.n", 7).is_empty(), "断ったのに行が当たった");
    // 読み直し 4 の [11]: 断ったのは場所の世代の道 (v14 の受け手は移る手順。 列の commit で断った形なら移っている)
    if !v15 {
        assert_eq!(b.slot_bits(), 0, "前提: 断ったのは v15 に移る手順 (移った後の列の commit ではない)");
    }

    margin(b, 0);
    sa.publish_since(Hlc::ZERO);
    let out = sb.pull_once(1);
    assert!(out.applied >= 1, "空きが戻っても届き直さない = 失われた: {out:?}");
    let rows = b.pull_raw("notes.n", 7);
    assert_eq!(rows.len(), 1, "届き直した行が当たっていない: {rows:?}");
    assert!(b.is_live_unchecked(rows[0]));
}

/// v15 の受け手: 新しい行の場所 (`notes` の先頭、 場所 100001) は移る時の commit (`u` の場所 0 の page と先回り) の外。
#[test]
fn a_received_row_whose_gen_seg_page_cannot_be_committed_is_redelivered() {
    let (pa, pb) = (tmp("v15-a"), tmp("v15-b"));
    let a = make(&pa, 1, 64, false);
    let b = make(&pb, 2, 64, false);
    let u = b.entity_in("u").unwrap();
    b.delete(u);
    let u2 = b.entity_in("u").unwrap();
    assert!(b.slot_bits() > 0 && u2 != u, "前提: B は v15");
    refused_then_redelivered(&a, &b, Engine::set_gen_space_margin);
    drop((a, b));
    cleanup(&pa);
    cleanup(&pb);
}

/// v14 の受け手: 満杯の `notes` (枠 2) に v14 の間に消した場所がある。 受けた行は使い回すので、 先に v15 に移る (移る手順は gen.seg を
/// commit できない)。
#[test]
fn a_received_row_that_needs_the_move_to_v15_is_redelivered_when_the_move_cannot_commit() {
    let (pa, pb) = (tmp("v14-a"), tmp("v14-b"));
    let a = make(&pa, 1, 2, false);
    let b = make(&pb, 2, 2, true);
    let mine: Vec<u64> = (0..2u32)
        .map(|i| {
            let e = b.entity_in("notes").unwrap();
            b.tie_to(e, "notes.n", 100 + i);
            e
        })
        .collect();
    b.delete(mine[0]);
    assert_eq!(b.slot_bits(), 0, "前提: B は v14");
    // v14 の受け手には gen.seg がまだ無い (移る手順が作る) ので全部の segment の余裕で。 断ったのが移る手順か (列の commit でないか) は
    // 断った後も v14 のままかで見る
    refused_then_redelivered(&a, &b, Engine::set_space_margin);
    assert!(b.slot_bits() > 0, "前提: 空きが戻った後に移った");
    assert!(b.is_live(mine[1]) && !b.is_live(mine[0]));
    drop((a, b));
    cleanup(&pa);
    cleanup(&pb);
}

// ---- 読み直し 4 の [1]: 名前の束ねの切り離しが場所を払い出せない ----

/// 名前 (主キー) のある表 `notes` (列 `n`) を持つ peer。 表の並びは `make` と同じ (`u` 枠 1、 `pad`、 `notes`)。 受け手 (`notes` を
/// 小さくする) は `notes` を使い切ると枠を全部の表の後ろに足す = gen.seg の commit の外の page。
fn make_named(path: &str, peer: PeerId, notes: u32) -> Arc<Engine> {
    let mut e = Engine::create_with_capacity(path, 400_000).unwrap();
    e.define_table("u", 1).unwrap();
    e.define_himo_in("u", "x", ValueType::Number, 0).unwrap();
    e.define_table("pad", 100_000).unwrap();
    e.define_table("notes", notes).unwrap();
    let k = e.define_himo_in("notes", "n", ValueType::Number, 0).unwrap() as u16;
    e.set_table_pk("notes", k).unwrap();
    e.enable_sync_tables().unwrap();
    let e = Engine::concurrentize_with_oplog(e, 16 * 1024 * 1024).unwrap();
    e.set_peer_id(peer);
    e
}

/// v15 にする (表 `u` で 1 回使い回す)。
fn to_v15(e: &Engine) {
    let u = e.entity_in("u").unwrap();
    e.delete(u);
    let u2 = e.entity_in("u").unwrap();
    assert!(e.slot_bits() > 0 && u2 != u, "前提: v15");
}

/// 壁時計 (ms) が `wall` を越えるまで待つ (HLC の時刻の順を作る。 同じ ms の中では論理時計と peer の番号で決まるので越えてから書く)。
fn wait_past(wall: u64) {
    while (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64) <= wall {
        std::hint::spin_loop();
    }
}

struct Net {
    _mem: Arc<InMemoryTransport>,
    syncers: Vec<(PeerId, Syncer)>,
}

impl Net {
    fn new(peers: &[(PeerId, &Arc<Engine>)]) -> Self {
        let mem = Arc::new(InMemoryTransport::new());
        for (p, _) in peers {
            mem.register_peer(*p);
        }
        let transport: Arc<dyn Transport> = mem.clone();
        let syncers = peers.iter().map(|(p, e)| (*p, Syncer::new((*e).clone(), transport.clone()))).collect();
        Net { _mem: mem, syncers }
    }

    fn syncer(&self, p: PeerId) -> &Syncer {
        &self.syncers.iter().find(|(q, _)| *q == p).unwrap().1
    }

    fn publish(&self, p: PeerId, e: &Engine) {
        e.oplog_sync().unwrap();
        self.syncer(p).publish_since(Hlc::ZERO);
    }

    fn pull(&self, to: PeerId, from: PeerId) -> enchudb_sync::SyncOutcome {
        self.syncer(to).pull_once(from)
    }
}

/// 空きの不足の間の pull: 容量の断り (cursor を止める) で、 落としていない。
fn assert_refused(out: &enchudb_sync::SyncOutcome, what: &str) {
    assert_eq!(out.dropped_unresolved, 0, "{what}: 落とした: {out:?}");
    assert!(out.rejected_capacity >= 1 && out.min_rejected_hlc.is_some(), "{what}: 容量の断りにしていない (再配送されない): {out:?}");
}

const A: PeerId = 1;
const B: PeerId = 2;
const R: PeerId = 3;
const C: PeerId = 4;

/// 受け手 R の行 L は A の X (名前 5) の行。 A が X を消した後に B が同じ名前で Y を作る (削除の後に作られた別のデッキ)。 R は削除より
/// 先に Y を受けて L に束ねる。 削除を受けると元の identity (X) が消えるので、 Y を新しい slot に移して L を消す (#581 の元の行が消える枝)。
/// 移す slot を払い出せない (gen.seg だけ空きが無い) 間は、 何も変えずに断って再配送させる。 前は払い出せないと 「切り離さない」 と読み、
/// 行を丸ごと消した (Y の行がこの peer で消え、 cursor は進んだ)。
///
/// falsify: 消える枝の `Alloc::Retry` を `Split::NotBound` に戻すと、 空きの無い pull が L を消して `applied` になる。
#[test]
fn a_delete_that_moves_a_later_identity_is_redelivered_while_the_slot_cannot_be_given_out() {
    let (pa, pb, pr) = (tmp("split-del-a"), tmp("split-del-b"), tmp("split-del-r"));
    let a = make_named(&pa, A, 64);
    let b = make_named(&pb, B, 64);
    let r = make_named(&pr, R, 1);
    to_v15(&r);
    let net = Net::new(&[(A, &a), (B, &b), (R, &r)]);
    let x = a.entity_in("notes").unwrap();
    a.tie_to(x, "notes.n", 5u32);
    net.publish(A, &a);
    assert!(net.pull(R, A).applied >= 1, "前提: R が X を受けた");
    let l = r.resolve_remote_eid_existing(x).expect("前提: X の行");
    a.delete(x);
    net.publish(A, &a);
    wait_past(a.tombstone_hlc(x).wall);
    let y = b.entity_in("notes").unwrap();
    b.tie_to(y, "notes.n", 5u32);
    net.publish(B, &b);
    assert!(net.pull(R, B).applied >= 1, "前提: R が Y を受けた");
    assert_eq!(r.resolve_remote_eid_existing(y), Some(l), "前提: R は Y を同じ名前の L に束ねた");

    r.set_gen_space_margin(NO_SPACE);
    let out = net.pull(R, A);
    assert_refused(&out, "削除の切り離し");
    assert!(r.is_live_unchecked(l), "断ったのに L (Y の行) を消した");
    assert_eq!(r.resolve_remote_eid_existing(y), Some(l), "断ったのに Y を動かした");

    r.set_gen_space_margin(0);
    net.publish(A, &a);
    let out = net.pull(R, A);
    assert!(out.applied >= 1 && out.rejected_capacity == 0, "空きが戻っても削除が届き直さない: {out:?}");
    let ry = r.resolve_remote_eid_existing(y).expect("Y の行が無い");
    assert!(ry != l && r.is_live_unchecked(ry), "Y が新しい slot に移っていない ({ry:#x}、 L {l:#x})");
    assert!(!r.is_live_unchecked(l), "X の行 L が残った");
    assert_eq!(r.get(ry, "notes.n"), Some(5));
    drop((net, a, b, r));
    for p in [&pa, &pb, &pr] {
        cleanup(p);
    }
}

/// 改名の切り離し (#751): R の行 L は A の X (名前 5)。 A が X を 6 に改名した後に、 B が名前 5 で Y を作る。 R は改名より先に Y を受けて
/// L に束ねる。 改名を受けると、 改名より後に名前 5 を持った Y を新しい slot に移してから L に改名を当てる。 移す slot を払い出せない
/// 間は、 改名を当てずに断って再配送させる。 前は 「切り離さない」 と読み、 束ねた L に改名を当てた (Y の名前 5 の行がこの peer で消えた)。
///
/// falsify: `split_for_rename` の `Alloc::Retry` を `Ok(None)` に戻すと、 空きの無い pull が L を 6 に改名して `applied` になる。
#[test]
fn a_rename_that_moves_a_later_identity_is_redelivered_while_the_slot_cannot_be_given_out() {
    let (pa, pb, pr) = (tmp("split-ren-a"), tmp("split-ren-b"), tmp("split-ren-r"));
    let a = make_named(&pa, A, 64);
    let b = make_named(&pb, B, 64);
    let r = make_named(&pr, R, 1);
    to_v15(&r);
    let net = Net::new(&[(A, &a), (B, &b), (R, &r)]);
    let x = a.entity_in("notes").unwrap();
    a.tie_to(x, "notes.n", 5u32);
    net.publish(A, &a);
    assert!(net.pull(R, A).applied >= 1, "前提: R が X を受けた");
    let l = r.resolve_remote_eid_existing(x).expect("前提: X の行");
    let k = r.himo_id("notes.n").unwrap() as u16;
    a.tie_to(x, "notes.n", 6u32);
    net.publish(A, &a);
    wait_past(a.cell_hlc(x, k).wall);
    let y = b.entity_in("notes").unwrap();
    b.tie_to(y, "notes.n", 5u32);
    net.publish(B, &b);
    assert!(net.pull(R, B).applied >= 1, "前提: R が Y を受けた");
    assert_eq!(r.resolve_remote_eid_existing(y), Some(l), "前提: R は Y を同じ名前の L に束ねた");

    r.set_gen_space_margin(NO_SPACE);
    let out = net.pull(R, A);
    assert_refused(&out, "改名の切り離し");
    assert_eq!(r.get(l, "notes.n"), Some(5), "断ったのに L に改名を当てた");
    assert_eq!(r.resolve_remote_eid_existing(y), Some(l), "断ったのに Y を動かした");

    r.set_gen_space_margin(0);
    net.publish(A, &a);
    let out = net.pull(R, A);
    assert!(out.applied >= 1 && out.rejected_capacity == 0, "空きが戻っても改名が届き直さない: {out:?}");
    let ry = r.resolve_remote_eid_existing(y).expect("Y の行が無い");
    assert!(ry != l && r.is_live_unchecked(ry), "Y が新しい slot に移っていない");
    assert_eq!((r.get(l, "notes.n"), r.get(ry, "notes.n")), (Some(6), Some(5)), "X は 6、 Y は 5 の別の行");
    drop((net, a, b, r));
    for p in [&pa, &pb, &pr] {
        cleanup(p);
    }
}

/// 元の行が残る枝 (#581): R の行 L は B の Y (名前 5、 A の削除の後に作られた)。 R は A の X と C の Z (どちらも削除より前に名前 5 で
/// 作られた) を L に束ねてから、 A の X の削除を受ける。 X と Z は消えたデッキ = それぞれ消えた slot に付け替える (2 つ払い出す)。 1 つ目は
/// 表の空き (commit 済みの page)、 2 つ目は枠を足す (gen.seg の commit の外) ので払い出せない。 払い出した 1 つ目を返し、 何も変えずに
/// 断って再配送させる。 前は払い出せない Z を付け替えずに束ねから外し、 Z の写像が L を指したまま (削除を知らない C の書き込みが L に当たる)。
///
/// falsify: 先にまとめて払い出す形をやめて、 払い出せない identity を飛ばすと (前の形)、 空きの無い pull が当たって Z の写像が L に残る。
/// 返す `live_free` を外すと、 断った後の表の行の数が 1 つ多い (払い出した空の行が残る)。
#[test]
fn a_delete_that_keeps_the_row_is_refused_whole_when_one_of_the_dead_slots_cannot_be_given_out() {
    let (pa, pb, pc, pr) = (tmp("split-keep-a"), tmp("split-keep-b"), tmp("split-keep-c"), tmp("split-keep-r"));
    let a = make_named(&pa, A, 64);
    let b = make_named(&pb, B, 64);
    let c = make_named(&pc, C, 64);
    let r = make_named(&pr, R, 2);
    to_v15(&r);
    let net = Net::new(&[(A, &a), (B, &b), (C, &c), (R, &r)]);
    let k = r.himo_id("notes.n").unwrap() as u16;
    let z = c.entity_in("notes").unwrap();
    c.tie_to(z, "notes.n", 5u32);
    net.publish(C, &c);
    let x = a.entity_in("notes").unwrap();
    a.tie_to(x, "notes.n", 5u32);
    net.publish(A, &a);
    wait_past(c.cell_hlc(z, k).wall.max(a.cell_hlc(x, k).wall));
    a.delete(x); // A は削除をまだ配らない (R は X の作成だけを先に受ける)
    wait_past(a.tombstone_hlc(x).wall);
    let y = b.entity_in("notes").unwrap();
    b.tie_to(y, "notes.n", 5u32);
    net.publish(B, &b);
    assert!(net.pull(R, B).applied >= 1, "前提: R が Y を受けた");
    let l = r.resolve_remote_eid_existing(y).expect("前提: Y の行");
    // X / Z の名前の書き込みは L の名前 (Y の、 後の版数) に LWW で負ける (束ねるだけ)
    assert!(net.pull(R, A).received >= 1, "前提: R が X の作成を受けた");
    assert!(net.pull(R, C).received >= 1, "前提: R が Z を受けた");
    assert_eq!(
        (r.resolve_remote_eid_existing(x), r.resolve_remote_eid_existing(z)),
        (Some(l), Some(l)),
        "前提: R は X と Z を Y の行 L に束ねた"
    );
    let live_before = r.table_eid_usage("notes").unwrap().live;

    net.publish(A, &a);
    r.set_gen_space_margin(NO_SPACE);
    let out = net.pull(R, A);
    assert_refused(&out, "残る枝の切り離し");
    assert_eq!(
        (r.resolve_remote_eid_existing(x), r.resolve_remote_eid_existing(z)),
        (Some(l), Some(l)),
        "断ったのに X / Z を付け替えた (一部だけ付け替えると、 残りの写像が L を指したまま束ねから外れる)"
    );
    assert!(r.is_live_unchecked(l));
    assert_eq!(r.table_eid_usage("notes").unwrap().live, live_before, "断った時に払い出した消えた slot を返していない");

    r.set_gen_space_margin(0);
    net.publish(A, &a);
    let out = net.pull(R, A);
    assert!(out.applied >= 1 && out.rejected_capacity == 0, "空きが戻っても削除が届き直さない: {out:?}");
    for (who, id) in [("X", x), ("Z", z)] {
        let m = r.resolve_remote_eid_existing(id);
        assert!(m.is_some_and(|m| m != l && !r.is_live_unchecked(m)), "{who} が消えた slot に付け替わっていない ({m:?}、 L {l:#x})");
    }
    assert!(r.is_live_unchecked(l) && r.get(l, "notes.n") == Some(5), "Y の行 L が残っていない");
    drop((net, a, b, c, r));
    for p in [&pa, &pb, &pc, &pr] {
        cleanup(p);
    }
}

const D: PeerId = 5;

/// PR #977 の読み直し 5 の [B]: 当たった名前の書き込みの後に、 名前の削除優先 (その名前がこの書き込みより後に消された) で行を消す
/// 切り離しが場所を払い出せない (空きの不足) 時、 record は当たっているので、 ホストでない中継 R はそれを配る (cursor だけを止めて
/// 再配送させ、 届き直した record で消し直す)。 前は `RejectedCapacity` (= 何も当てていない、 #210) を返し、 中継の一覧に積まず、 届き
/// 直した record は LWW で飛ばして配らない = 下流 D がその行の名前の書き込みを一度も受けなかった (他の列だけが届いた行は、 名前の無い
/// 行として名前の削除を逃れて生き残る)。
///
/// この形は今の束ねの規則では起きない (束ねた identity の作成はその名前の書き込みの版数 = 束ねた行の名前の cell の版数以下なので、
/// 当たった名前の書き込みより後に消された名前の行に、 その削除より後に作られた identity は居ない)。 守りなので、 束ねを手で作る: R の
/// X の行 L を名前より先に払い出し (他の列が別の配りで先に届いた形)、 L に削除より後に作られた identity W を束ねる
/// (`note_bound_identity`)。
///
/// falsify: 当てた後の切り離しの断りを `RejectedCapacity` に戻すと (前の形)、 D が X の名前の書き込みを受けない。
#[test]
fn a_name_write_applied_before_its_split_is_refused_is_still_relayed() {
    let (pa, pc, pr, pd) = (tmp("relay-a"), tmp("relay-c"), tmp("relay-r"), tmp("relay-d"));
    let a = make_named(&pa, A, 64);
    let c = make_named(&pc, C, 64);
    // R の `notes` は 1 枠: Z の消えた場所を X の行 L が使い回し、 切り離しが移す先は枠を足す (gen.seg の commit の外)
    let r = make_named(&pr, R, 1);
    let d = make_named(&pd, D, 64);
    to_v15(&r);
    r.set_gossip_remote_apply(true);
    let net = Net::new(&[(A, &a), (C, &c), (R, &r), (D, &d)]);
    let k = a.himo_id("notes.n").unwrap() as u16;
    // A の X (名前 5) は、 C が Z (名前 5) を消すより前に作る
    let x = a.entity_in("notes").unwrap();
    a.tie_to(x, "notes.n", 5u32);
    wait_past(a.cell_hlc(x, k).wall);
    let z = c.entity_in("notes").unwrap();
    c.tie_to(z, "notes.n", 5u32);
    wait_past(c.cell_hlc(z, k).wall);
    c.delete(z);
    let t = c.tombstone_hlc(z);
    net.publish(C, &c);
    assert!(net.pull(R, C).applied >= 1, "前提: R が Z の作成と削除を受けた");
    let l = r.resolve_remote_eid(x, k).expect("前提: R が X の行を払い出した");
    r.note_bound_identity(l, enchudb_oplog::make_eid(7, 1), Hlc { wall: t.wall + 1000, logical: 0, peer: 7 });

    r.set_gen_space_margin(NO_SPACE);
    net.publish(A, &a);
    let out = net.pull(R, A);
    assert_refused(&out, "当てた後の名前の削除の切り離し");
    assert!(r.is_live_unchecked(l), "前提: 断った (L はまだ消していない)");
    r.set_gen_space_margin(0);
    net.publish(R, &r);
    net.pull(D, R);
    let got = d.resolve_remote_eid_existing(x).is_some();

    // 空きが戻ると届き直して、 R が L を消す
    net.publish(A, &a);
    let again = net.pull(R, A);
    let l_live = r.is_live_unchecked(l);
    drop((net, a, c, r, d));
    for p in [&pa, &pc, &pr, &pd] {
        cleanup(p);
    }
    assert!(got, "下流 D が X の名前の書き込みを受けていない (中継 R が当たった record を配らなかった)");
    assert_eq!(again.rejected_capacity, 0, "空きが戻っても断った: {again:?}");
    assert!(!l_live, "届き直した名前の書き込みで、 名前の削除優先が L を消していない");
}
