//! `prev.seg` (場所を使い回した時の前の住人の記録、 `crate::prev_slots`) の形と開く時の扱い。 落ちた像の試験は
//! `tests/power_loss.rs` (電源断、 crashsim) と `crates/enchudb-transport/tests/prev_seg_reuse_after_kill.rs` (kill)。
//!
//! 前提は状態で確かめる (場所が同じ・番号が違う、 `prev.seg` の file の有無、 `.eidmap` の disk の中身)。

use enchudb_engine::engine::Engine;
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;
use enchudb_oplog::oplog::DecodedOp;
use std::path::Path;
use std::sync::Arc;

const WAL: usize = 4 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("enchudb-prevseg-{tag}-{}-{seq}", std::process::id()))
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
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(from).unwrap().map(|e| e.unwrap()).collect();
    // `pkformer` のまとめ直し (consumer の周期が別の thread で走らせる、 review987 [1]) は `pkformer` → log の順に置き換える。 log を
    // 先に写せば、 置き換えの途中を写しても 「古い log と新しい pkformer」 (log は読まず、 pkformer に全部ある) にしかならない
    entries.sort_by_key(|e| e.file_name() == "pkformer");
    for entry in entries {
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else if !entry.file_name().to_string_lossy().ends_with(".tmp") {
            // 一時 file (書き出しの途中、 まとめ直しが作って消す) は DB ではない (`db_files::is_copyable_entry`)
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

/// 表 t (枠 `cap`、 名前 k = Number、 v = Number)。 `sync` なら sync する DB (WAL 付き、 peer 1)。
fn create(path: &str, cap: u32, sync: bool) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 1024).unwrap();
    eng.define_table("t", cap).unwrap();
    eng.define_himo_in("t", "k", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    let k = eng.himo_id("t.k").unwrap() as u16;
    eng.set_table_pk("t", k).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(1);
    eng
}

fn open(path: &str) -> Arc<Engine> {
    let eng = Engine::open_concurrent_with_oplog(path, WAL).unwrap();
    eng.set_peer_id(1);
    eng
}

fn add(eng: &Engine, k: u32) -> u64 {
    let e = eng.entity_in("t").unwrap();
    eng.tie_to(e, "t.k", k);
    eng.tie_to(e, "t.v", k * 10);
    eng.oplog_commit();
    e
}

/// 行 x を消して名前 `k` の行で場所を使い回す。 (新しい行, x の削除の版数)。
fn reuse(eng: &Engine, x: u64, k: u32) -> (u64, Hlc) {
    eng.delete(x);
    eng.oplog_commit();
    let tomb = eng.tombstone_hlc(x);
    let y = add(eng, k);
    assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: 場所を使い回した");
    assert_ne!(y, x, "前提: 世代の違う番号");
    (y, tomb)
}

/// 使い回すと、 場所に前の住人 (自分の行: 自分の peer・前の住人の番号・削除の版数) を書く。 sync しない DB は書かない。 v15 の DB は
/// `prev.seg` を持つ。
#[test]
fn a_reuse_records_the_former_own_row_only_in_a_synced_db() {
    let (p, q) = (tmp_path("own"), tmp_path("nosync"));
    let _c = Cleanup(vec![p.clone(), q.clone()]);
    let eng = create(&p, 1, true);
    let x = add(&eng, 7);
    let (y, tomb) = reuse(&eng, x, 8);
    assert!(Path::new(&p).join("prev.seg").exists(), "v15 の DB に `prev.seg` が無い");
    assert_eq!(
        eng.prev_resident_of_slot(eng.eid_slot(y)),
        Some((1, enchudb_oplog::eid_local(x), tomb, true)),
        "使い回した場所の前の住人の記録"
    );
    let plain = create(&q, 1, false);
    let x = add(&plain, 7);
    let (y, _) = reuse(&plain, x, 8);
    assert_eq!(plain.prev_resident_of_slot(plain.eid_slot(y)), None, "sync しない DB が記録を書いた");
    // review946c [I]: 記録を書かない DB には `prev.seg` を作らない (開き直しても)
    assert!(!Path::new(&q).join("prev.seg").exists(), "sync しない v15 の DB に `prev.seg` を作った");
    drop(plain);
    let plain = open(&q);
    assert_eq!(plain.get(y, "t.k"), Some(8));
    assert!(!Path::new(&q).join("prev.seg").exists(), "sync しない v15 の DB を開いて `prev.seg` を作った");
}

/// 翻訳した行 (受けた他の peer の行) の場所を使い回すと、 前の住人の identity (相手の peer, 相手の番号) と削除の版数を書く。
#[test]
fn a_reuse_records_the_former_translated_row() {
    let p = tmp_path("translated");
    let _c = Cleanup(vec![p.clone()]);
    let eng = create(&p, 1, true);
    let world_f = enchudb_oplog::make_eid(3, 9);
    let hid = eng.himo_id("t.k").unwrap() as u16;
    let t = eng.resolve_remote_eid(world_f, hid).unwrap();
    assert!(eng.remote_tie_apply_result(t, hid, 5u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
    let del = Hlc { wall: 2_000, logical: 0, peer: 3 };
    assert!(eng.remote_delete_apply_foreign(world_f, del), "前提: C の削除が当たった");
    let y = add(&eng, 8);
    assert_eq!(eng.eid_slot(y), eng.eid_slot(t), "前提: F の場所を使い回した");
    assert_eq!(eng.prev_resident_of_slot(eng.eid_slot(y)), Some((3, 9, del, false)), "翻訳した行の前の住人の記録");
}

/// 前の binary が作った v15 の DB (`prev.seg` が無い) も開ける: 書ける open は `prev.seg` を作って記録なしで始め、 その後の使い回しを
/// 書く。 readonly の open は作らない。
#[test]
fn a_v15_db_without_prev_seg_opens_and_starts_recording() {
    let p = tmp_path("old-v15");
    let _c = Cleanup(vec![p.clone()]);
    let (x, y) = {
        let eng = create(&p, 1, true);
        let x = add(&eng, 7);
        let (y, _) = reuse(&eng, x, 8);
        (x, y)
    };
    // 前の binary の形: `prev.seg` も manifest の行も無い
    std::fs::remove_file(Path::new(&p).join("prev.seg")).unwrap();
    let manifest = Path::new(&p).join("segments");
    let text = std::fs::read_to_string(&manifest).unwrap();
    assert!(text.contains("prev.seg"), "前提: manifest に prev.seg の行がある");
    let kept: String = text.lines().filter(|l| !l.contains("prev.seg")).map(|l| format!("{l}\n")).collect();
    std::fs::write(&manifest, kept).unwrap();
    {
        let ro = Engine::open_readonly(&p).unwrap();
        assert_eq!(ro.get(y, "t.k"), Some(8), "readonly で開けない");
        assert_eq!(ro.prev_resident_of_slot(ro.eid_slot(y)), None);
    }
    assert!(!Path::new(&p).join("prev.seg").exists(), "readonly の open が `prev.seg` を作った");
    let eng = open(&p);
    assert!(Path::new(&p).join("prev.seg").exists(), "書ける open が `prev.seg` を作らない");
    assert_eq!(eng.get(y, "t.k"), Some(8));
    assert_eq!(eng.prev_resident_of_slot(eng.eid_slot(y)), None, "前の binary の使い回しの記録は無い");
    let (z, tomb) = reuse(&eng, y, 9);
    assert_eq!(eng.prev_resident_of_slot(eng.eid_slot(z)), Some((1, enchudb_oplog::eid_local(y), tomb, true)));
    assert!(!eng.is_live(x));
}

/// 記録は戻す時だけ使う: きれいに閉じた後に開いても、 移した削除が `.eidmap` に書き出し済みの像 (kill) を開いても、 戻さない
/// (退避表と比べて載っていれば何もしない)。 書き出していない像だけ戻す。 どの像でも state に前の住人の Delete は 1 つ。
#[test]
fn only_a_delete_missing_from_the_sidecar_is_restored() {
    let (p, written, unwritten) = (tmp_path("clean"), tmp_path("img-written"), tmp_path("img-unwritten"));
    let _c = Cleanup(vec![p.clone(), written.clone(), unwritten.clone()]);
    let (x, tomb) = {
        let eng = create(&p, 2, true);
        let x = add(&eng, 7);
        add(&eng, 1);
        eng.oplog_sync().unwrap();
        eng.persist_sync_state().unwrap();
        let hold = eng.hold_consumer();
        let (_, tomb) = reuse(&eng, x, 7);
        assert!(eng.eidmap_disk_diff().unwrap().is_some(), "前提: 移した削除は `.eidmap` に無い");
        copy_dir(Path::new(&p), Path::new(&unwritten));
        eng.persist_sync_state().unwrap();
        assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: 移した削除を `.eidmap` に書いた");
        copy_dir(Path::new(&p), Path::new(&written));
        drop(hold);
        (x, tomb)
    };
    let deletes_of = |eng: &Engine| -> Vec<Hlc> {
        let (records, _) = eng.state_records();
        records
            .iter()
            .filter_map(|r| match r.op {
                DecodedOp::Delete { eid } if eid == x => Some(r.hlc),
                _ => None,
            })
            .collect()
    };
    for (path, want) in [(&p, 0), (&written, 0), (&unwritten, 1)] {
        let eng = open(path);
        assert_eq!(eng.prev_residents_restored(), want, "{path}: 戻した数");
        assert_eq!(deletes_of(&eng), vec![tomb], "{path}: state の X の Delete");
    }
}

/// #924: ホスト H が翻訳した行 X (A の行) の場所を使い回すと、 H の state (replica 版、 author A) が、 X を指したままの A の行 W の
/// Ref を出さなかった (Ref の照合で 「場所の今の住人でない」、 退避表を引く道は自分の行しか見ない)。 H から取り直した受け手でだけ
/// W.next が null になる (差分で受けた受け手は消えた X を指したまま)。 `prev.seg` の記録 (場所の前の住人 = (A, x)) から、 A の
/// 行を指す ref として出す (X の削除の版数が A の時計 = 同じ state に X の Delete も入る時だけ)。
#[test]
fn a_replica_state_keeps_a_ref_to_a_translated_row_whose_slot_was_reused() {
    let p = tmp_path("issue924");
    let _c = Cleanup(vec![p.clone()]);
    const AUTHOR: u32 = 2;
    let h = {
        let mut eng = Engine::create_with_capacity(&p, 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.define_ref_in("t", "next", "t").unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
        eng.set_peer_id(9);
        eng
    };
    let (hv, hn) = (h.himo_id("t.v").unwrap() as u16, h.himo_id("t.next").unwrap() as u16);
    let at = |wall: u64| Hlc { wall, logical: 0, peer: AUTHOR };
    // A の行 X (番号 5) と、 X を指す W (番号 6) を受ける
    let (x_world, w_world) = (enchudb_oplog::make_eid(AUTHOR, 5), enchudb_oplog::make_eid(AUTHOR, 6));
    let hx = h.resolve_remote_eid(x_world, hv).unwrap();
    assert!(h.remote_tie_apply_result(hx, hv, 50u32, at(1_000)).applied());
    let hw = h.resolve_remote_eid(w_world, hv).unwrap();
    assert!(h.remote_tie_apply_result(hw, hv, 60u32, at(1_001)).applied());
    let to_x = h.resolve_remote_ref_value(AUTHOR, 5, hn).unwrap();
    assert!(h.remote_tie_apply_result(hw, hn, to_x, at(1_002)).applied());
    // A が X を消し、 H は受ける。 H のアプリが行を足して X の場所を使い回す (表は満杯)
    assert!(h.remote_delete_apply_foreign(x_world, at(2_000)), "前提: A の削除が当たった");
    let y = h.entity_in("t").unwrap();
    h.tie_to(y, "t.v", 70u32);
    assert_eq!(h.eid_slot(y), h.eid_slot(hx), "前提: H は X の場所を使い回した");
    assert_eq!(h.get(hw, "t.next"), Some(u64::from(to_x)), "前提: H の W は消えた X を指したまま");
    let (records, _) = h.state_records_for(AUTHOR);
    let next_of_w: Vec<_> = records
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Tie { eid, himo_id, value } if eid == w_world && himo_id == hn => Some(value),
            DecodedOp::TieRef { eid, himo_id, target } if eid == w_world && himo_id == hn => Some(target),
            _ => None,
        })
        .collect();
    assert_eq!(next_of_w, vec![5], "H の replica 版の state に W.next (A の行 X を指す) が無い / 違う宛先 ({next_of_w:x?})");
    assert!(
        records.iter().any(|r| matches!(r.op, DecodedOp::Delete { eid } if eid == x_world)),
        "前提: 同じ state に X の Delete がある"
    );
}

/// state の Delete (宛名, 版数)。
fn state_deletes(eng: &Engine) -> Vec<(u64, Hlc)> {
    let (records, _) = eng.state_records();
    records
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some((eid, r.hlc)),
            _ => None,
        })
        .collect()
}

/// review946 [2]: 使い回しの記録は場所を次に使い回すまで残る。 前は、 移した削除を sidecar に書き出した後に忘れてよい線
/// (`forget_deletes_before`) がその削除を外すと、 きれいに閉じなかった次の open が 「書き出していない」 と見て記録から戻し、 場所の今の
/// 住人 Y の別名 (名前で畳んだ B の行 b) を外して X の削除の版数で消えた identity にした (B の b 宛ての書き込みが Y に届かず、 state は
/// b の Delete を出した)。 書き出した周期の記録は戻さない。
#[test]
fn a_delete_forgotten_after_its_sidecar_does_not_come_back_over_the_new_rows_alias() {
    let (p, img) = (tmp_path("forget"), tmp_path("forget-img"));
    let _c = Cleanup(vec![p.clone(), img.clone()]);
    const B: u32 = 3;
    let eng = create(&p, 2, true);
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let x = add(&eng, 7);
    // B の行 b を受ける (表は満杯)
    let world_b = enchudb_oplog::make_eid(B, 9);
    let b = eng.resolve_remote_eid(world_b, hk).unwrap();
    assert!(eng.remote_tie_apply_result(b, hk, 8u32, Hlc { wall: 1_000, logical: 0, peer: B }).applied());
    let (y, tomb) = reuse(&eng, x, 7);
    eng.merge_entity_into(b, y).unwrap();
    let slot_of = |e: &Engine| e.resolve_remote_eid_existing(world_b).map(|l| e.eid_slot(l));
    assert_eq!(slot_of(&eng), Some(eng.eid_slot(y)), "前提: b は Y の場所の別名");
    eng.persist_sync_state().unwrap();
    assert!(eng.forget_deletes_before(Hlc { wall: tomb.wall + 1, logical: 0, peer: 0 }) >= 1, "前提: X の削除を忘れた");
    eng.persist_sync_state().unwrap();
    // kill の像
    copy_dir(Path::new(&p), Path::new(&img));
    drop(eng);
    let eng = open(&img);
    assert_eq!(eng.prev_residents_restored(), 0, "書き出して忘れた削除を記録から戻した");
    assert_eq!(slot_of(&eng), Some(eng.eid_slot(y)), "今の住人 Y の別名 b が外れた");
    let deletes = state_deletes(&eng);
    assert!(!deletes.iter().any(|&(e, _)| e == world_b), "生きている Y の別名 b の Delete を state に出した: {deletes:x?}");
}

/// review946 の M6 (再生が記録の行宛ての record を当てる): X (名前 7) を書き出した後、 consumer を止めて X を消し、 名前 8 の Y で場所を
/// 使い回して落ちる (kill)。 再生は X の Delete (場所はもう Y) を当てない。 当てると Y の場所を消して、 Y の名前 8 を X の削除の版数で
/// 消した名前にした (Y を生かし直しても名前の削除は残り、 その後に届いた名前 8 の他の peer の行を消す)。
#[test]
fn the_replayed_delete_of_the_former_resident_does_not_delete_the_new_rows_name() {
    let (p, img) = (tmp_path("m6"), tmp_path("m6-img"));
    let _c = Cleanup(vec![p.clone(), img.clone()]);
    let eng = create(&p, 1, true);
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let x = add(&eng, 7);
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let y = {
        let _hold = eng.hold_consumer();
        let (y, _) = reuse(&eng, x, 8);
        copy_dir(Path::new(&p), Path::new(&img));
        y
    };
    drop(eng);
    let eng = open(&img);
    assert_eq!(eng.get(y, "t.k"), Some(8), "前提: 開いた像で Y が生きている");
    assert!(!eng.is_live(x), "前提: X は消えている");
    assert_eq!(eng.deleted_name_version(hk, 8), None, "再生した X の Delete が Y の名前 8 を消した名前にした");
}

/// review946c [D]: M6 の続き。 X を消して Y で場所を使い回した後に、 pull の sidecar の書き出し (`persist_sync_state`) が周期を進めて
/// 書き出し済みにし (WAL の checkpoint は X の Delete の手前のまま)、 Y を消して Z で場所をもう一度使い回して落ちる (kill)。 開く時に
/// 戻す記録は 2 回目 (前の住人 Y) だけだが、 再生は X の Delete も Z の場所に当てない (log に残る 1 回目の記録で守る)。 前は守りが
/// 書き出していない記録しか引かず、 X の Delete が Z の名前 9 を X の削除の版数で消した名前にした。
#[test]
fn a_replayed_delete_behind_a_persisted_reuse_does_not_delete_the_new_rows_name() {
    let (p, img) = (tmp_path("m6-persisted"), tmp_path("m6-persisted-img"));
    let _c = Cleanup(vec![p.clone(), img.clone()]);
    let eng = create(&p, 1, true);
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let x = add(&eng, 7);
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let z = {
        let _hold = eng.hold_consumer();
        let (y, _) = reuse(&eng, x, 8);
        eng.persist_sync_state().unwrap();
        let (z, _) = reuse(&eng, y, 9);
        copy_dir(Path::new(&p), Path::new(&img));
        z
    };
    drop(eng);
    let eng = open(&img);
    assert_eq!(eng.prev_residents_restored(), 1, "前提: 開く時に戻すのは 2 回目 (書き出していない周期) の記録だけ");
    assert_eq!(eng.get(z, "t.k"), Some(9), "前提: 開いた像で Z が生きている");
    assert!(!eng.is_live(x), "前提: X は消えている");
    assert_eq!(eng.deleted_name_version(hk, 9), None, "再生した X の Delete が Z の名前 9 を消した名前にした");
}

/// review946 の M13: #924 の Ref を出すのは、 前の住人 X の Delete が同じ state に入る時だけ (Delete の無い Ref は受け手に空の生きた行を
/// 作る、 #921 の読み直し [1])。 Delete が入るのは、 削除の版数が author の時計か、 X が author の行の時 (#951)。 ホスト H が X の場所を
/// 使い回した後の、 author の行 W (W.next → X) の replica 版の state:
/// - X が author の行で、 author でない peer が消した: X の Delete が入る = W.next も出す
/// - X が他の peer C の行で、 C が消した: X の Delete は入らない = W.next も出さない
#[test]
fn a_replica_state_refs_a_reused_translated_row_only_with_its_delete() {
    const AUTHOR: u32 = 2;
    const C: u32 = 4;
    for (owner, deleter, want_ref) in [(AUTHOR, C, true), (C, C, false)] {
        let p = tmp_path("issue924-who");
        let _c = Cleanup(vec![p.clone()]);
        let h = {
            let mut eng = Engine::create_with_capacity(&p, 1024).unwrap();
            eng.define_table("t", 2).unwrap();
            eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
            eng.define_ref_in("t", "next", "t").unwrap();
            eng.enable_sync_tables().unwrap();
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
            eng.set_peer_id(9);
            eng
        };
        let (hv, hn) = (h.himo_id("t.v").unwrap() as u16, h.himo_id("t.next").unwrap() as u16);
        let at = |wall: u64, peer: u32| Hlc { wall, logical: 0, peer };
        let (x_world, w_world) = (enchudb_oplog::make_eid(owner, 5), enchudb_oplog::make_eid(AUTHOR, 6));
        let hx = h.resolve_remote_eid(x_world, hv).unwrap();
        assert!(h.remote_tie_apply_result(hx, hv, 50u32, at(1_000, owner)).applied());
        let hw = h.resolve_remote_eid(w_world, hv).unwrap();
        assert!(h.remote_tie_apply_result(hw, hv, 60u32, at(1_001, AUTHOR)).applied());
        let to_x = h.resolve_remote_ref_value(owner, 5, hn).unwrap();
        assert!(h.remote_tie_apply_result(hw, hn, to_x, at(1_002, AUTHOR)).applied());
        let del = at(2_000, deleter);
        assert!(h.remote_delete_apply_foreign(x_world, del), "前提: 削除が当たった");
        let y = h.entity_in("t").unwrap();
        h.tie_to(y, "t.v", 70u32);
        assert_eq!(h.eid_slot(y), h.eid_slot(hx), "前提: H は X の場所を使い回した");
        assert_eq!(h.prev_resident_of_slot(h.eid_slot(y)), Some((owner, 5, del, false)), "前提: 場所の記録は X");
        let (records, _) = h.state_records_for(AUTHOR);
        let next_of_w = records
            .iter()
            .filter(|r| match r.op {
                DecodedOp::Tie { eid, himo_id, .. } | DecodedOp::TieRef { eid, himo_id, .. } => eid == w_world && himo_id == hn,
                _ => false,
            })
            .count();
        let x_delete = records.iter().any(|r| matches!(r.op, DecodedOp::Delete { eid } if eid == x_world));
        assert_eq!(x_delete, want_ref, "前提 (owner {owner}, deleter {deleter}): X の Delete が state に入るか");
        assert_eq!(
            next_of_w,
            usize::from(want_ref),
            "owner {owner}, deleter {deleter}: W.next を X の Delete と揃えて出さない (Delete 無しの Ref / Delete ありで null)"
        );
    }
}

/// review946 3 (i): 同じ場所を 1 つの周期で 2 回使い回した (X1 → Y → Z) 後に落ちる (kill)。 場所ごとの記録は 2 回目 (前の住人 Y) に
/// 上書きされるが、 log には 2 件とも残るので、 開いた state に X1 と Y の両方の Delete がある。 再生は X1 の Delete を Z の場所に当てない。
/// 前は 1 回目の前の住人 X1 の削除が memory だけで失われ、 再生の X1 の Delete が Z の場所を一度消した。
#[test]
fn both_former_residents_of_a_slot_reused_twice_before_a_crash_come_back() {
    let (p, img) = (tmp_path("twice"), tmp_path("twice-img"));
    let _c = Cleanup(vec![p.clone(), img.clone()]);
    let eng = create(&p, 1, true);
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let x1 = add(&eng, 7);
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let (y, t1, z, t2) = {
        let _hold = eng.hold_consumer();
        let (y, t1) = reuse(&eng, x1, 8);
        let (z, t2) = reuse(&eng, y, 9);
        copy_dir(Path::new(&p), Path::new(&img));
        (y, t1, z, t2)
    };
    drop(eng);
    let eng = open(&img);
    assert_eq!(eng.get(z, "t.k"), Some(9), "前提: 開いた像で Z が生きている");
    assert_eq!(eng.deleted_name_version(hk, 9), None, "再生した X1 の Delete が Z の名前 9 を消した名前にした");
    let deletes = state_deletes(&eng);
    assert!(deletes.contains(&(x1, t1)), "1 回目の前の住人 X1 の Delete が state に無い: {deletes:x?}");
    assert!(deletes.contains(&(y, t2)), "2 回目の前の住人 Y の Delete が state に無い: {deletes:x?}");
    assert!(!deletes.iter().any(|&(e, _)| e == z), "生きている Z の Delete を出した: {deletes:x?}");
    assert_eq!(eng.prev_residents_restored(), 2);
}

/// review946c [A]: 翻訳した行 F (C の行) の場所を、 A が F を消してから 1 つの周期の中で 2 回使い回して (F → Y → Z) 落ちる (kill)。
/// 場所ごとの配列は 2 回目 (前の住人 Y = 自分の行) に上書きされるが、 log には F の記録が残る。 開いた A で写像 (C, f) は戻らず
/// (Z は C の行 F ではない)、 state に F の Delete ((C, f) 宛て、 A の削除の版数) があり、 WAL に残った F の Delete の bridge は
/// (C, f) 宛てに出る。 前は開く時の写像の見分け (`prev_evicted`) と bridge の宛名が配列 (最後の 1 件) しか見ず、 写像 (C, f) を
/// 今の住人 Z に戻し (#925 の形)、 F の削除はどこにも残らなかった (F の Delete の bridge は A の世界番号で出た)。
#[test]
fn a_translated_slot_reused_twice_before_a_crash_stays_unmapped() {
    let (p, img) = (tmp_path("translated-twice"), tmp_path("translated-twice-img"));
    let _c = Cleanup(vec![p.clone(), img.clone()]);
    const C: u32 = 3;
    let eng = create(&p, 1, true);
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let world_f = enchudb_oplog::make_eid(C, 9);
    let f = eng.resolve_remote_eid(world_f, hk).unwrap();
    assert!(eng.remote_tie_apply_result(f, hk, 5u32, Hlc { wall: 1_000, logical: 0, peer: C }).applied());
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: `.eidmap` は F の写像 (削除の版数なし) を書き出し済み");
    let (z, tomb) = {
        let _hold = eng.hold_consumer();
        eng.delete(f);
        eng.oplog_commit();
        let tomb = eng.tombstone_hlc(f);
        assert_ne!(tomb, Hlc::ZERO, "前提: A が F を消した");
        let y = add(&eng, 8);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(f), "前提: Y は F の場所を使い回した");
        let (z, _) = reuse(&eng, y, 9);
        assert_eq!(
            eng.prev_resident_of_slot(eng.eid_slot(z)).map(|r| r.3),
            Some(true),
            "前提: 場所の配列は 2 回目 (前の住人 Y = 自分の行) に上書きされた"
        );
        assert!(eng.eidmap_disk_diff().unwrap().is_some(), "前提: 外した写像は `.eidmap` に届いていない");
        copy_dir(Path::new(&p), Path::new(&img));
        (z, tomb)
    };
    drop(eng);
    let eng = open(&img);
    assert_eq!(eng.get(z, "t.k"), Some(9), "前提: 開いた像で Z が生きている");
    assert_eq!(eng.resolve_remote_eid_existing(world_f), None, "開いた A で F の写像 (C, f) が今の住人 Z に戻った");
    let deletes = state_deletes(&eng);
    assert!(deletes.contains(&(world_f, tomb)), "state に F の Delete ((C, f) 宛て) が無い: {deletes:x?}");
    // WAL に残った F の Delete は (C, f) 宛てに出る (A の世界番号で出さない)
    eng.transfer_oplog_to_sync_ops();
    let bridged: Vec<u64> = eng
        .settled_sync_records(|_, _| true)
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } if r.hlc == tomb => Some(eid),
            _ => None,
        })
        .collect();
    assert_eq!(bridged, vec![world_f], "F の Delete の bridge の宛名");
}

/// 使い回しの後に `oplog_sync()` して暇になった DB も、 consumer の暇な周期が sidecar を書いて `prev.seg` の周期を進める: その後に
/// 落ちた (kill) 像を開くと記録から戻す物が無く、 前の住人の削除は sidecar から読む。 前は sidecar を書かず、 次の書き込みまで記録が
/// 書き出されないまま溜まった (1 周期 21845 件の log から溢れて同じ場所の 2 回目が 1 回目を上書きし、 きれいに閉じなかった後の open は
/// 配列をなめて溜まった記録を全部戻した)。 前提: consumer を止めて撮った像は記録から戻す。
#[test]
fn an_idle_db_moves_the_reuse_records_to_the_sidecars() {
    let (p, before, after) = (tmp_path("idle"), tmp_path("idle-before"), tmp_path("idle-after"));
    let _c = Cleanup(vec![p.clone(), before.clone(), after.clone()]);
    let eng = create(&p, 1, true);
    let x = add(&eng, 7);
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let (y, tomb) = {
        let _hold = eng.hold_consumer();
        let r = reuse(&eng, x, 8);
        eng.oplog_sync().unwrap();
        copy_dir(Path::new(&p), Path::new(&before));
        r
    };
    {
        let back = open(&before);
        assert_eq!(back.prev_residents_restored(), 1, "前提: 暇な周期の前の像は記録から戻す");
        assert!(state_deletes(&back).contains(&(x, tomb)), "前提: 戻した X の Delete が state に無い");
    }
    // consumer の暇な周期 (1 秒に 1 回まで) を待って撮る。 写す間は consumer を止める (review946c [J]: 写しの途中に暇な周期の
    // 書き出しが走ると、 書き出し済みと言う `prev.seg` と書き出す前の `eidmap*` の混ざった像になった)
    let mut restored = u32::MAX;
    for _ in 0..50 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        let _ = std::fs::remove_dir_all(&after);
        {
            let _hold = eng.hold_consumer();
            copy_dir(Path::new(&p), Path::new(&after));
        }
        let back = open(&after);
        restored = back.prev_residents_restored();
        if restored == 0 {
            assert!(state_deletes(&back).contains(&(x, tomb)), "sidecar から読んだ X の Delete が state に無い");
            assert_eq!(back.get(y, "t.k"), Some(8));
            break;
        }
    }
    assert_eq!(restored, 0, "暇な DB が使い回しの記録を sidecar に書き出さない (開く時に記録から戻した)");
}
