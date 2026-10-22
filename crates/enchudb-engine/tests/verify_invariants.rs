//! `Engine::verify_invariants` (診断): 普通に使った DB では違反も警告も無く、 壊した形はそれぞれ捉える。
//! 壊し方は外からできるものだけ: leaf.free の細工 (空きが live の slot を覆う)、 tombstone だけ書く (途中で切れた delete)、
//! 同じ名前 (主キー) の行を 2 つ作る (警告、 #601 が直るまで)。 「Tag の cell が辞書に無い語を指す」 は外から作れない (`tie` が Tag 列への
//! 生の番号を断る) ので、 その検査には壊した形の test が無い。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::Hlc;
use std::path::PathBuf;

fn tmp(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("enchu-verify-{tag}-{}", std::process::id()));
    let _ = enchudb_engine::db_files::remove_db(&p);
    p.to_str().unwrap().to_string()
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// Number / Tag / Leaf の列と名前付きの表。 書いて、 外して、 消して、 書き直す
fn churn(path: &str) {
    let mut eng = Engine::create_with_cell_version(path, 4096).unwrap();
    eng.define_table("t", 1000).unwrap();
    eng.define_himo_in("t", "id", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "tag", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("t", "body", ValueType::Leaf, 0).unwrap();
    let id_hid = eng.himo_id("t.id").unwrap() as u16;
    eng.set_table_pk("t", id_hid).unwrap();
    let mut rows = Vec::new();
    for i in 0..300u32 {
        let e = eng.entity_in("t").unwrap();
        eng.tie(e, "t.id", i);
        eng.tie_text(e, "t.tag", &format!("w{}", i % 17));
        eng.tie_text(e, "t.body", &format!("本文 {i} {}", "x".repeat((i % 9) as usize * 4)));
        rows.push(e);
    }
    for (i, &e) in rows.iter().enumerate() {
        match i % 5 {
            1 => eng.untie(e, "t.body"),
            2 => eng.delete(e),
            3 => eng.tie_text(e, "t.body", &format!("書き直し {i} {}", "y".repeat((i % 7) * 6))),
            _ => {}
        }
    }
    eng.flush().unwrap();
}

#[test]
fn a_normally_used_db_has_no_violations_and_no_warnings() {
    let path = tmp("ok");
    churn(&path);
    let eng = Engine::open_standalone(&path).unwrap();
    let r = eng.verify_invariants();
    assert!(r.ok() && r.warnings.is_empty(), "{r:?}");
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn a_hole_covering_a_live_slot_is_a_violation() {
    let path = tmp("hole");
    churn(&path);
    // きれいに閉じた時の leaf.free を細工: 最初の空きを 1 word 伸ばして次の live の slot の先頭を覆う (FNV は合わせ直す)。
    // install は構造 (範囲・重なり) しか見ないので通り、 検査が捉える
    let file = PathBuf::from(&path).join(enchudb_engine::db_files::LEAFFREE);
    let good = std::fs::read(&file).unwrap();
    let n = u32::from_le_bytes(good[12..16].try_into().unwrap());
    assert!(n >= 1, "空きが 1 つ以上ある前提");
    let mut bad = good[..good.len() - 4].to_vec();
    let size0 = u32::from_le_bytes(bad[20..24].try_into().unwrap());
    bad[20..24].copy_from_slice(&(size0 + 1).to_le_bytes());
    let crc = fnv1a(&bad);
    bad.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&file, &bad).unwrap();
    let eng = Engine::open_standalone(&path).unwrap();
    assert!(eng.leaf_free_list_from_file(), "細工した file が入っていない (前提が崩れた)");
    let r = eng.verify_invariants();
    assert!(r.violations.iter().any(|v| v.contains("空き") && v.contains("live の slot")), "{r:?}");
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn an_interrupted_delete_is_a_violation() {
    let path = tmp("tomb");
    let mut eng = Engine::create_with_cell_version(&path, 256).unwrap();
    eng.set_peer_id(1);
    eng.define_himo("n", ValueType::Number, 0);
    let e = eng.entity().unwrap();
    eng.tie(e, "n", 5u32);
    // crash 相当: tombstone だけ書いて本体を残す
    assert!(eng.set_tombstone(e, Hlc { wall: 1_000_000, logical: 0, peer: 1 }));
    let r = eng.verify_invariants();
    assert!(r.violations.iter().any(|v| v.contains("途中で切れた delete")), "{r:?}");
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn duplicate_names_in_a_table_are_a_warning_for_now() {
    let path = tmp("pk");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "id", ValueType::Number, 0).unwrap();
    let id_hid = eng.himo_id("t.id").unwrap() as u16;
    eng.set_table_pk("t", id_hid).unwrap();
    for _ in 0..3 {
        let e = eng.entity_in("t").unwrap();
        eng.tie(e, "t.id", 7u32);
    }
    let r = eng.verify_invariants();
    assert!(r.ok(), "違反ではなく警告のはず: {r:?}");
    assert!(r.warnings.iter().any(|w| w.contains("名前 (主キー) が重複") && w.contains("3 個")), "{r:?}");
    let _ = enchudb_engine::db_files::remove_db(&path);
}
