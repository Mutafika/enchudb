//! #1046 の (2) / #1047 (user の決定 11-23): 開く時の突き合わせが完了させた削除 (1. も 2. も) は、 tombstone を削除の時刻の下限
//! (行の cell の版数の最大の直後) で置き、 その版数の Delete を #451 が作り直す (相手の cursor が越えていれば floor を上げて state で
//! 取り直させる)。 ここでは 2. (生きている印だけ死 = 削除の前の写しに、 削除の後の `entities.seg` だけを置いた像) を、 自分の行 X を
//! 受けた相手 C との間で見る:
//!
//! - X 宛ての遅い受信は捨てる (前は tombstone が無く、 次の世代で X を生き返らせた = #1046)
//! - C が A の state で取り直しても、 A が開いた後に同じ名前で足し直した行 X2 を消さない (前は state に X も X の削除も無く、 C の掃除が
//!   今の時刻で X を消し、 名前の削除優先で X2 も消した = #923 の形)
//! - 削除が C に既に届いていれば、 下限の Delete は C で何もしない (C の tombstone はそのまま)
#![cfg(unix)]

use enchudb_engine::ValueType;
use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::path::Path;
use std::sync::Arc;

const WAL: usize = 4 * 1024 * 1024;
const SLOT_MASK: u32 = (1 << 24) - 1;

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("enchudb-issue1046-{tag}-{}-{seq}-{nanos}", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

struct Cleanup(Vec<String>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_dir_all(p);
        }
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

fn transport(peers: &[PeerId]) -> Arc<dyn Transport> {
    let mem = Arc::new(InMemoryTransport::new());
    for &p in peers {
        mem.register_peer(p);
    }
    mem
}

/// 表 notes (名前 `name`、 列 `v`) と、 v15 に移るための枠 1 の表 u。 v15 に移り終えた形で返す。
fn make(path: &str, peer: PeerId) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("u", 1).unwrap();
    eng.define_table("notes", 100).unwrap();
    let name = eng.define_himo_in("notes", "name", ValueType::Number, 0).unwrap() as u16;
    eng.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
    eng.set_table_pk("notes", name).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(peer);
    let u = eng.entity_in("u").unwrap();
    eng.delete(u);
    let u2 = eng.entity_in("u").unwrap();
    assert_ne!(u2, u, "前提: v15");
    eng
}

fn slot(eid: u64) -> u32 {
    (eid as u32) & SLOT_MASK
}

/// A の行 X (名前 `name`) を C に配った後、 A が X を消す。 削除の前の写しに削除の後の生きている印 (`entities.seg`) だけを置いた像
/// (突き合わせの 2.) を `img` に作る。 削除を C に配るか (`deliver`)。 (X, A の X の行の cell の版数の最大) を返す。
fn entities_only_image(
    pa: &str,
    img: &str,
    transport: &Arc<dyn Transport>,
    sync_c: &Syncer,
    name: u32,
    deliver: bool,
) -> (u64, Hlc) {
    let eng_a = make(pa, 1);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let x = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(x, "notes.name", name);
    eng_a.tie_to(x, "notes.v", 1u32);
    // X の後ろに生きた行を置く (払い出し位置が開く時に X の手前へ戻らない = 突き合わせの 2. が X の場所を見る)
    let keep = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(keep, "notes.name", 1000u32);
    eng_a.oplog_sync().unwrap();
    sync_a.publish_since(Hlc::ZERO);
    sync_c.pull_once(1);
    let hv = eng_a.himo_id("notes.v").unwrap() as u16;
    let hn = eng_a.himo_id("notes.name").unwrap() as u16;
    let newest = eng_a.cell_hlc(x, hv).max(eng_a.cell_hlc(x, hn));
    let (before, after) = (format!("{img}-before"), format!("{img}-after"));
    {
        let _hold = eng_a.hold_consumer();
        eng_a.oplog_sync().unwrap();
        copy_dir(Path::new(pa), Path::new(&before));
        eng_a.delete(x);
        // #1047: 下限は本当の削除の版数以下 (名前の削除優先が、 本当の削除の後に同じ名前で足し直した行を消さない)
        let deleted_at = eng_a.tombstone_hlc(x);
        assert!(lower_bound_of(newest) <= deleted_at, "下限 {:?} が本当の削除の版数 {deleted_at:?} より後", lower_bound_of(newest));
        if deliver {
            eng_a.oplog_sync().unwrap();
            eng_a.transfer_oplog_to_sync_ops();
            sync_a.publish_since(Hlc::ZERO);
            sync_c.pull_once(1);
        }
        copy_dir(Path::new(pa), Path::new(&after));
    }
    copy_dir(Path::new(&before), Path::new(img));
    std::fs::copy(Path::new(&after).join("entities.seg"), Path::new(img).join("entities.seg")).unwrap();
    drop(sync_a);
    drop(eng_a);
    let _ = std::fs::remove_dir_all(&before);
    let _ = std::fs::remove_dir_all(&after);
    (x, newest)
}

fn lower_bound_of(newest: Hlc) -> Hlc {
    Hlc { wall: newest.wall, logical: newest.logical + 1, peer: 1 }
}

/// 2. の完了の行 X 宛ての遅い受信は捨てる (#1046)。 X の tombstone は削除の時刻の下限。
///
/// falsify: 2. の完了に tombstone を置かない (前の形) と、 C の Tie が X を次の世代で生き返らせる。
#[test]
fn a_late_write_to_a_row_finished_by_reconcile_step_2_is_dropped() {
    let (pa, pc, img) = (tmp_path("a"), tmp_path("c"), tmp_path("img"));
    let _c = Cleanup(vec![pa.clone(), pc.clone(), img.clone()]);
    let transport = transport(&[1, 3]);
    let eng_c = make(&pc, 3);
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    let (x, newest) = entities_only_image(&pa, &img, &transport, &sync_c, 5, false);
    let eng_a = Engine::open_concurrent_with_oplog(&img, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(eng_a.generation_repairs(), (0, 1), "前提: 突き合わせの 2. が X の場所を完了させた");
    assert!(!eng_a.is_live(x), "前提: X は消えた");
    let tomb = eng_a.tombstone_hlc(x);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    // C が X に書く (版数は X のどの cell より新しい)
    let x_on_c = eng_c.pull_raw("notes.v", 1u32)[0];
    eng_c.tie_to(x_on_c, "notes.v", 2u32);
    eng_c.oplog_sync().unwrap();
    sync_c.publish_since(Hlc::ZERO);
    let out = sync_a.pull_once(3);
    let at_x: Vec<u64> = eng_a.entities().into_iter().filter(|&e| slot(e) == slot(x)).collect();
    drop(sync_a);
    drop(eng_a);
    assert_eq!(at_x, Vec::<u64>::new(), "遅い受信 ({out:?}) が X の場所に行を生き返らせた");
    assert_eq!(tomb, lower_bound_of(newest), "X の tombstone が削除の時刻の下限でない");
}

/// 2. の完了の後に A が同じ名前で足し直した行 X2 は、 C が A の state で取り直しても消えない (#923 の形が起きない)。
///
/// falsify: 2. の完了に tombstone を置かない (前の形) と、 A の state に X も X の削除も無く、 C の掃除が今の時刻で X を消して、 名前の
/// 削除優先で X2 も消す。
#[test]
fn a_row_renamed_after_reconcile_step_2_survives_a_state_refetch() {
    let (pa, pc, img) = (tmp_path("a"), tmp_path("c"), tmp_path("img"));
    let _c = Cleanup(vec![pa.clone(), pc.clone(), img.clone()]);
    let transport = transport(&[1, 3]);
    let eng_c = make(&pc, 3);
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    let (x, _) = entities_only_image(&pa, &img, &transport, &sync_c, 5, false);
    let eng_a = Engine::open_concurrent_with_oplog(&img, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(eng_a.generation_repairs(), (0, 1), "前提: 突き合わせの 2. が X の場所を完了させた");
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    sync_a.serve_state();
    // A が同じ名前 5 で足し直す
    let x2 = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(x2, "notes.name", 5u32);
    eng_a.tie_to(x2, "notes.v", 9u32);
    eng_a.oplog_sync().unwrap();
    assert_eq!(eng_c.pull_raw("notes.name", 5u32).len(), 1, "前提: C に X がある");
    // A は X の削除を下限の版数で置き、 floor を上げた (配った版数以下 = record を載せない)。 C は差分で受けると抜けを言われ、 A の state で
    // 取り直す
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_c.pull_once(1);
    assert!(out.history_truncated, "A の floor で C が抜けを言われない: {out:?}");
    sync_c.bootstrap_pull(1).expect("serve_state 済み");
    let named_on_c: Vec<(u64, Option<u64>)> =
        eng_c.pull_raw("notes.name", 5u32).into_iter().filter(|&e| eng_c.is_live(e)).map(|e| (e, eng_c.get(e, "notes.v"))).collect();
    drop(sync_a);
    drop(eng_a);
    assert_ne!(x2, x);
    assert_eq!(
        named_on_c.iter().map(|&(_, v)| v).collect::<Vec<_>>(),
        vec![Some(9)],
        "C で、 A が足し直した名前 5 の行 X2 が無い / X が残った ({named_on_c:x?})"
    );
}

/// 削除が C に既に届いていれば、 A が開いた時の下限の Delete (C が state で取り直す) は C で何もしない。
#[test]
fn a_lower_bound_delete_does_nothing_where_the_delete_already_arrived() {
    let (pa, pc, img) = (tmp_path("a"), tmp_path("c"), tmp_path("img"));
    let _c = Cleanup(vec![pa.clone(), pc.clone(), img.clone()]);
    let transport = transport(&[1, 3]);
    let eng_c = make(&pc, 3);
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    let (x, newest) = entities_only_image(&pa, &img, &transport, &sync_c, 5, true);
    let x_on_c: Vec<u64> = eng_c.pull_raw("notes.name", 5u32);
    // 削除は C に届いている (行は消え、 名前の cell も落ちている)。 C の X の行は世界番号で引く
    let world = enchudb_oplog::make_eid(1, x as u32);
    let row_on_c = eng_c.resolve_remote_eid_existing(world).expect("前提: C は X を受けた");
    let c_tomb = eng_c.tombstone_hlc(eng_c.slot_eid(eng_c.eid_slot(row_on_c)));
    assert!(x_on_c.iter().all(|&e| !eng_c.is_live(e)), "前提: C で X は消えた");
    assert!(c_tomb > lower_bound_of(newest), "前提: C の tombstone は本当の削除の版数 (下限より新しい)");
    let eng_a = Engine::open_concurrent_with_oplog(&img, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(eng_a.generation_repairs(), (0, 1), "前提: 突き合わせの 2. が X の場所を完了させた");
    assert_eq!(eng_a.tombstone_hlc(x), lower_bound_of(newest), "前提: A の X の tombstone は下限");
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    sync_a.serve_state();
    sync_c.bootstrap_pull(1).expect("serve_state 済み");
    let after = eng_c.tombstone_hlc(eng_c.slot_eid(eng_c.eid_slot(row_on_c)));
    drop(sync_a);
    drop(eng_a);
    assert_eq!(after, c_tomb, "下限の Delete が C の tombstone を変えた");
}

/// #1068 の読み直し F2: 名前で畳んだ翻訳した行 (`fold_rows_with_same_pk` → `merge_entity_into` の src) の、 畳んだ後の生きている印だけが
/// 届いた像 (`.eidmap` は畳む前 = その場所は翻訳した行のまま)。 突き合わせの 2. がその場所を完了させても、 翻訳した行の場所には tombstone を
/// 置かず、 A の state は行の持ち主 B の行の Delete を A の書き込みとして出さない (出すと B が自分の生きている行を消す)。
///
/// falsify: 作り直しを自分の行の場所に限らないと、 畳んだ src の場所に下限の tombstone を置き、 A の state が `Delete (B, x)` を出す。
#[test]
fn a_folded_translated_row_finished_by_reconcile_step_2_is_not_deleted_for_its_owner() {
    let (pa, pb, img) = (tmp_path("a"), tmp_path("b"), tmp_path("img"));
    let (before, after) = (format!("{img}-before"), format!("{img}-after"));
    let _c = Cleanup(vec![pa.clone(), pb.clone(), img.clone(), before.clone(), after.clone()]);
    let transport = transport(&[1, 2]);
    let eng_b = make(&pb, 2);
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    let (s_on_a, x) = {
        let eng_a = make(&pa, 1);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        // B の行 x (名前 7) を A が受ける (翻訳した行 s)
        let x = eng_b.entity_in("notes").unwrap();
        eng_b.tie_to(x, "notes.name", 7u32);
        eng_b.tie_to(x, "notes.v", 1u32);
        eng_b.oplog_sync().unwrap();
        sync_b.publish_since(Hlc::ZERO);
        sync_a.pull_once(2);
        let s_on_a = eng_a.pull_raw("notes.name", 7u32)[0];
        // A の自分の行 d (名前 5)。 s の後ろに生きた行がある (払い出し位置が開く時に s の手前へ戻らない)
        let d = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(d, "notes.name", 5u32);
        eng_a.oplog_sync().unwrap();
        eng_a.persist_sync_state().unwrap();
        {
            let _hold = eng_a.hold_consumer();
            eng_a.oplog_sync().unwrap();
            copy_dir(Path::new(&pa), Path::new(&before));
            // B が x を 5 に改名し、 A が当てて s を d に畳む
            eng_b.tie_to(x, "notes.name", 5u32);
            eng_b.oplog_sync().unwrap();
            sync_b.publish_since(Hlc::ZERO);
            sync_a.pull_once(2);
            assert!(!eng_a.is_live_unchecked(s_on_a), "前提: A は s を d に畳んだ");
            copy_dir(Path::new(&pa), Path::new(&after));
        }
        drop(sync_a);
        (s_on_a, x)
    };
    copy_dir(Path::new(&before), Path::new(&img));
    std::fs::copy(Path::new(&after).join("entities.seg"), Path::new(&img).join("entities.seg")).unwrap();
    let eng_a = Engine::open_concurrent_with_oplog(&img, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(eng_a.generation_repairs(), (0, 1), "前提: 突き合わせの 2. が畳んだ s の場所を完了させた");
    let tomb = eng_a.tombstone_hlc(s_on_a);
    let deletes_x: Vec<Hlc> = eng_a
        .state_records()
        .0
        .into_iter()
        .filter(|r| r.author_peer == 1 && matches!(r.op, enchudb_oplog::oplog::DecodedOp::Delete { eid } if eid == x))
        .map(|r| r.hlc)
        .collect();
    drop(eng_a);
    assert_eq!(tomb, Hlc::ZERO, "畳んだ翻訳した行の場所に tombstone を置いた");
    assert_eq!(deletes_x, Vec::<Hlc>::new(), "A の state が B の行 x の Delete を A の書き込みとして出した");
}

/// #1068 の読み直し 2 (#1071): 自分の行どうしの畳み (#612) の src の、 畳んだ後の生きている印と名前の記録 (`pkformer`、 本体より先に書く) は
/// 届き、 別名 (`eidalias`、 本体の後に書く) は届いていない像 (畳んだ後の最初の本体の書き出しの途中の電源断)。 突き合わせの 2. は src の
/// 場所を自分の行の死として拾うが、 残す行が (自分, src) を束ねているので作り直さない (tombstone を置かず、 state に (自分, src) の Delete を
/// 出さない)。 出すと、 相手でも同じ改名で畳んでいれば相手の残す行 (同じデッキ) が消える。
///
/// falsify: 作り直しの 「(自分, 場所) を別の行が束ねていれば除く」 を外すと、 src の場所に下限の tombstone を置き、 state が Delete を出す。
#[test]
fn an_own_row_folded_into_another_own_row_is_not_deleted_by_reconcile_step_2() {
    let (pa, pb, img) = (tmp_path("a"), tmp_path("b"), tmp_path("img"));
    let (before, after) = (format!("{img}-before"), format!("{img}-after"));
    let _c = Cleanup(vec![pa.clone(), pb.clone(), img.clone(), before.clone(), after.clone()]);
    let transport = transport(&[1, 2]);
    let eng_b = make(&pb, 2);
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    let (src, src_world) = {
        let eng_a = make(&pa, 1);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        // A の自分の行 s (名前 7) と d (名前 5)、 その後ろに生きた行
        let s = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(s, "notes.name", 7u32);
        eng_a.tie_to(s, "notes.v", 1u32);
        let d = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(d, "notes.name", 5u32);
        eng_a.tie_to(d, "notes.v", 2u32);
        let keep = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(keep, "notes.name", 1000u32);
        eng_a.oplog_sync().unwrap();
        sync_a.publish_since(Hlc::ZERO);
        sync_b.pull_once(1);
        let s_on_b = eng_b.pull_raw("notes.name", 7u32)[0];
        eng_a.persist_sync_state().unwrap();
        let src = {
            let _hold = eng_a.hold_consumer();
            eng_a.oplog_sync().unwrap();
            copy_dir(Path::new(&pa), Path::new(&before));
            // B が A の行 s を名前 5 に改名し、 A が当てて s と d を畳む (どちらも自分の行)
            eng_b.tie_to(s_on_b, "notes.name", 5u32);
            eng_b.oplog_sync().unwrap();
            sync_b.publish_since(Hlc::ZERO);
            sync_a.pull_once(2);
            eng_a.persist_sync_state().unwrap();
            let src = [s, d].into_iter().find(|&e| !eng_a.is_live_unchecked(e)).expect("前提: A は s と d を畳んだ");
            copy_dir(Path::new(&pa), Path::new(&after));
            src
        };
        drop(sync_a);
        (src, src)
    };
    // 像: 畳む前の写しに、 畳んだ後の生きている印と名前の記録だけを置く (別名 `eidalias` は畳む前)
    copy_dir(Path::new(&before), Path::new(&img));
    for f in ["entities.seg", "pkformer", "pkformer.log"] {
        if Path::new(&after).join(f).exists() {
            std::fs::copy(Path::new(&after).join(f), Path::new(&img).join(f)).unwrap();
        }
    }
    let eng_a = Engine::open_concurrent_with_oplog(&img, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(eng_a.generation_repairs(), (0, 1), "前提: 突き合わせの 2. が畳んだ src の場所を完了させた");
    let tomb = eng_a.tombstone_hlc(src);
    let deletes: Vec<Hlc> = eng_a
        .state_records()
        .0
        .into_iter()
        .filter(|r| r.author_peer == 1 && matches!(r.op, enchudb_oplog::oplog::DecodedOp::Delete { eid } if eid == src_world))
        .map(|r| r.hlc)
        .collect();
    drop(eng_a);
    assert_eq!(tomb, Hlc::ZERO, "畳んだ自分の行 (src {src:#x}) の場所に tombstone を置いた");
    assert_eq!(deletes, Vec::<Hlc>::new(), "A の state が畳んだ src の Delete を出した");
}

/// #1068 の読み直し 3: 上の逆向き。 A の自分の行 X に B の同じ名前の行を名前で束ねた (X が束ね先 = 名前の記録の束ねに X 自身の identity も
/// 入る) 後、 A が X を本当に消した 2. の像。 X は 「別の行に束ねられた自分の行 (畳んだ src)」 ではないので、 下限の tombstone と Delete を
/// 作り直す。
///
/// falsify: 「別の行が束ねている」 の見分けで、 束ねている行が自分自身の場所の時を除かない (`*s != d` を外す) と、 束ね先の X の削除を
/// 作り直さない。
#[test]
fn a_deleted_own_row_that_binds_a_remote_row_is_still_relogged() {
    let (pa, pb, img) = (tmp_path("a"), tmp_path("b"), tmp_path("img"));
    let (before, after) = (format!("{img}-before"), format!("{img}-after"));
    let _c = Cleanup(vec![pa.clone(), pb.clone(), img.clone(), before.clone(), after.clone()]);
    let transport = transport(&[1, 2]);
    let eng_b = make(&pb, 2);
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    let (x, newest) = {
        let eng_a = make(&pa, 1);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        // A と B が同じ名前 5 の行を別々に作り、 A が B の行を名前で X に束ねる
        let x = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(x, "notes.name", 5u32);
        eng_a.tie_to(x, "notes.v", 1u32);
        let keep = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(keep, "notes.name", 1000u32);
        let y = eng_b.entity_in("notes").unwrap();
        eng_b.tie_to(y, "notes.name", 5u32);
        eng_b.tie_to(y, "notes.v", 2u32);
        eng_a.oplog_sync().unwrap();
        eng_b.oplog_sync().unwrap();
        sync_b.publish_since(Hlc::ZERO);
        sync_a.pull_once(2);
        assert_eq!(eng_a.pull_raw("notes.name", 5u32).into_iter().filter(|&e| eng_a.is_live(e)).count(), 1, "前提: A は B の行を X に束ねた");
        eng_a.persist_sync_state().unwrap();
        let hv = eng_a.himo_id("notes.v").unwrap() as u16;
        let hn = eng_a.himo_id("notes.name").unwrap() as u16;
        let newest = eng_a.cell_hlc(x, hv).max(eng_a.cell_hlc(x, hn));
        {
            let _hold = eng_a.hold_consumer();
            eng_a.oplog_sync().unwrap();
            copy_dir(Path::new(&pa), Path::new(&before));
            eng_a.delete(x);
            copy_dir(Path::new(&pa), Path::new(&after));
        }
        drop(sync_a);
        (x, newest)
    };
    copy_dir(Path::new(&before), Path::new(&img));
    std::fs::copy(Path::new(&after).join("entities.seg"), Path::new(&img).join("entities.seg")).unwrap();
    let eng_a = Engine::open_concurrent_with_oplog(&img, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(eng_a.generation_repairs(), (0, 1), "前提: 突き合わせの 2. が X の場所を完了させた");
    let tomb = eng_a.tombstone_hlc(x);
    let deletes: Vec<Hlc> = eng_a
        .state_records()
        .0
        .into_iter()
        .filter(|r| r.author_peer == 1 && matches!(r.op, enchudb_oplog::oplog::DecodedOp::Delete { eid } if eid == x))
        .map(|r| r.hlc)
        .collect();
    drop(eng_a);
    assert_eq!(tomb, lower_bound_of(newest), "B の行を束ねた自分の行 X の削除を、 下限の tombstone で作り直さない");
    assert_eq!(deletes, vec![lower_bound_of(newest)], "A の state が X の Delete を下限の版数で出さない");
}
