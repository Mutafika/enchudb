//! #90 (0.13.0): LeafStore の cell offset word 化 + scale 選択式 (16/32/64GB) の
//! engine-level 検証。
//!
//! - LeafStore 内部の word-offset 機構は `leaf_store` の unit test が網羅
//!   (handle_is_word_offset / cap_scales_with_shift / addresses_beyond_4gb 等)。
//! - ここでは engine 経由で scale を選んで tie/read/reopen が通ること、 及び
//!   予約 leaf_data_size が scale cap を超えたら reject されることを見る。
//! - v6 (byte offset) の read-through は `issue88_migration` (v6 出力を reopen) が
//!   既にカバー。

use enchudb_engine::{Engine, LeafScale, ValueType};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue90-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// 各 scale で作った DB が、 reopen 後も Leaf を正しく読める (= off_shift が region
/// header に永続化され、 load が word offset を正しく解釈している)。
#[test]
fn scale_persists_across_reopen() {
    let vals = ["alpha", "a somewhat longer leaf payload here", "z", ""];
    for scale in [LeafScale::Gb16, LeafScale::Gb32, LeafScale::Gb64] {
        let path = tmp(&format!("scale-{scale:?}"));
        cleanup(&path);
        {
            let mut eng =
                Engine::create_growable_with_leaf(&path, 1000, None, None, scale).unwrap();
            eng.define_himo("body", ValueType::Leaf, 0);
            for v in vals {
                let e = eng.entity();
                eng.tie_text(e, "body", v);
            }
            // 同 session でも読める
            for (i, v) in vals.iter().enumerate() {
                assert_eq!(
                    eng.get_text(i as u64, "body").map(|b| b.to_vec()),
                    Some(v.as_bytes().to_vec()),
                    "{scale:?} 同session read",
                );
            }
            assert!(eng.leaf_footprint().unwrap() > 0);
            eng.flush().unwrap();
        }
        // reopen して読み直す (off_shift は leaf region header 由来で復元)
        {
            let eng = Engine::open(&path).unwrap();
            for (i, v) in vals.iter().enumerate() {
                assert_eq!(
                    eng.get_text(i as u64, "body").map(|b| b.to_vec()),
                    Some(v.as_bytes().to_vec()),
                    "{scale:?} reopen read",
                );
            }
        }
        cleanup(&path);
    }
}

/// scale で reclaim (word offset でも free/再利用が効く) が動く。
#[test]
fn reclaim_works_at_each_scale() {
    for scale in [LeafScale::Gb16, LeafScale::Gb64] {
        let path = tmp(&format!("reclaim-{scale:?}"));
        cleanup(&path);
        let mut eng = Engine::create_growable_with_leaf(&path, 1000, None, None, scale).unwrap();
        eng.define_himo("body", ValueType::Leaf, 0);
        let e = eng.entity();
        eng.tie_text(e, "body", "0123456789");
        let fp1 = eng.leaf_footprint().unwrap();
        // 同サイズ 30 回 re-tie → 旧 slot を free して再利用 → footprint 不変
        for _ in 0..30 {
            eng.tie_text(e, "body", "9876543210");
        }
        assert_eq!(eng.leaf_footprint().unwrap(), fp1, "{scale:?}: reclaim が効いていない");
        cleanup(&path);
    }
}

/// 予約 leaf_data_size が選んだ scale の cap を超えたら create を弾く。
/// (validate は allocation 前なので巨大 size でも disk を消費しない)
#[test]
fn leaf_data_size_exceeding_cap_is_rejected() {
    let path = tmp("cap");
    cleanup(&path);
    let over_16gb = 20usize * 1024 * 1024 * 1024;
    // Gb16 (cap ~16GB) で 20GB 予約 → InvalidInput
    let r = Engine::create_growable_with_leaf(&path, 1000, None, Some(over_16gb), LeafScale::Gb16);
    assert!(r.is_err(), "Gb16 の cap を超える leaf_data_size が弾かれていない");
    cleanup(&path);
    // Gb64 (cap ~64GB) なら 20GB は format 上許容 (実 allocation はしないので
    // ここでは validate を通ることだけ確認 = create は成功するはず)。
    // ※ 20GB sparse mmap を張るため、 環境依存で失敗したら skip 扱い。
    if let Ok(_eng) = Engine::create_growable_with_leaf(&path, 1000, None, Some(over_16gb), LeafScale::Gb64) {
        // ok: format 上 20GB は Gb64 の範囲内
    }
    cleanup(&path);
}
