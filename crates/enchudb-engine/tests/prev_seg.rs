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
