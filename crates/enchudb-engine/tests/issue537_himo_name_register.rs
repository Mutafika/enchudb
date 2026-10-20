//! #537: 列の定義が紐名の辞書 (himoreg) に名前を入れられない (辞書の領域を伸ばせない) 時は、 定義を失敗させる。
//! 旧: 入れる失敗を捨てて定義は成功を返し、 その session の中では書けるが、 開き直すと列が名前で引けなかった。
//!
//! 辞書を伸ばすのを決まって断らせるため、 空きの余白 (`set_space_margin`) を空きより大きくする: 辞書は新しい page を
//! 触るたびに空きを確かめる (`SegmentMap::touch_sparse`)。 長い名前の列を足し続けて辞書の page をまたがせ、 定義が Ok を
//! 返した列には全部違う値を書き、 余白を戻して開き直す。 Ok を返した列は全部、 名前で引けて値も合うこと (名前の数と列の
//! 数がずれると、 名前が別の列を指す)。

use enchudb_engine::{Engine, ValueType};

fn tmp(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("enchu_issue537_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p.to_str().unwrap().to_string()
}

#[test]
fn a_column_whose_name_cannot_be_registered_is_not_defined() {
    let path = tmp("reg");
    let mut defined: Vec<(String, u64)> = Vec::new();
    let denied;
    {
        let mut eng = Engine::create_with_capacity(&path, 4096).unwrap();
        eng.define_table("t", 100).unwrap();
        eng.define_himo_in("t", "first", ValueType::Number, 0).unwrap();
        let e = eng.entity_in("t").unwrap();
        // ここから先は辞書の新しい page を触れない
        eng.set_space_margin(u64::MAX / 4);
        for i in 0..120u64 {
            let name = format!("c{i:03}_{}", "x".repeat(500));
            if eng.define_himo_in("t", &name, ValueType::Number, 0).is_ok() {
                defined.push((name, 1000 + i));
            }
        }
        denied = eng.space_denials();
        eng.set_space_margin(0);
        for (name, v) in &defined {
            eng.tie(e, &format!("t.{name}"), *v as u32);
        }
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
    }
    // 前提 (直す前も後も): 辞書の新しい page を触ろうとして断られた (= 名前を入れられなかった定義がある)
    assert!(denied > 0, "前提: 辞書の page をまたがなかった");
    let eng = Engine::open_standalone(&path).unwrap();
    let e = eng.list_tables().iter().find(|t| t.1 == "t").map(|_| 0u64).unwrap();
    for (name, v) in &defined {
        let full = format!("t.{name}");
        assert!(eng.himo_id(&full).is_some(), "定義が Ok を返した列 {} が開き直すと引けない", &full[..8]);
        assert_eq!(eng.get(e, &full), Some(*v), "列 {} の値が違う (名前が別の列を指している)", &full[..8]);
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
