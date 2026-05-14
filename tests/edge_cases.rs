//! v27 エッジケーステスト。
//!
//! 容量・上限、observation views、entity lifecycle、紐型混在、
//! 永続化、concurrent モードの境界条件を確認する。
//!
//! `cargo test --features v27 --test edge_cases`

#![cfg(feature = "v27")]

use enchudb::{Engine, HimoType};
use std::sync::Arc;

fn tmp(name: &str) -> String {
    let path = format!("/tmp/enchu_edge_{name}.db");
    let _ = std::fs::remove_file(&path);
    path
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
}

// ──────────────────────────────────────────────────────────────────
// 1. 容量・上限
// ──────────────────────────────────────────────────────────────────

/// max_himos=16 で 17 本目の define_himo は panic する(ensure_himo の assert)。
#[test]
#[should_panic(expected = "too many himos")]
fn max_himos_limit() {
    let path = tmp("max_himos_limit");
    let mut eng = Engine::create_full(&path, 1024, None, Some(16), None).unwrap();
    for i in 0..16 {
        eng.define_himo(&format!("h{}", i), HimoType::Number, 4);
    }
    // 17 本目 → panic
    eng.define_himo("overflow", HimoType::Number, 4);
}

/// `define_himo("x", Value, 10)` に対して value=9 は OK。
/// max_values は索引サイズのヒント。実際の値制限ではないので、value=10 も
/// value=11 も、BucketCylinder の動的拡張でそれぞれ独立のバケットに入る。
#[test]
fn max_values_boundary() {
    let path = tmp("max_values_boundary");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("x", HimoType::Number, 10);

    let e9 = eng.entity();
    let e10 = eng.entity();
    let e11 = eng.entity();
    eng.tie(e9, "x", 9);
    eng.tie(e10, "x", 10);
    eng.tie(e11, "x", 11); // ヒント超え → 動的拡張

    assert_eq!(eng.get(e9, "x"), Some(9));
    assert_eq!(eng.get(e10, "x"), Some(10));
    assert_eq!(eng.get(e11, "x"), Some(11));

    // clamp されず、それぞれ独立のバケットから返る。
    let p10 = eng.pull_raw("x", 10);
    assert_eq!(p10, vec![e10]);
    let p11 = eng.pull_raw("x", 11);
    assert_eq!(p11, vec![e11]);

    cleanup(&path);
}

/// 空 DB(何も tie してない、紐すら定義してない)に pull_raw / query を呼ぶ
/// → 空の結果が返る。panic しないこと。
#[test]
fn empty_db_query() {
    let path = tmp("empty_db_query");
    let eng = Engine::create_standalone(&path).unwrap();

    // 紐未定義 → 空結果
    let r1 = eng.pull_raw("nothing", 0);
    assert!(r1.is_empty());

    let r2 = eng.query(&[("nothing", 0)]);
    assert!(r2.is_empty());

    let r3 = eng.query(&[("a", 0), ("b", 1)]);
    assert!(r3.is_empty());

    let r4 = eng.query(&[]); // 条件 0 個 → 空
    assert!(r4.is_empty());

    cleanup(&path);
}

/// `tie(e, "x", 0)` → `pull_raw("x", 0)` で引ける。
/// 0 は内部表現で stored=0(未 tie)と区別される(tie 時に value+1 で格納)。
#[test]
fn zero_value_tie() {
    let path = tmp("zero_value_tie");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("x", HimoType::Number, 4);

    let e = eng.entity();
    eng.tie(e, "x", 0);

    assert_eq!(eng.get(e, "x"), Some(0));
    let pulled = eng.pull_raw("x", 0);
    assert_eq!(pulled, vec![e]);

    cleanup(&path);
}

/// 大きめ value(define_himo の max_values ヒント超え)でも tie できる。
/// BucketCylinder は silent clamp せず動的拡張する。
/// u32::MAX-1 相当まで詰めるとバケット数が 40 億になるため現実的ではないので、
/// ここでは 10 万オーダーで動的拡張を確認する。
#[test]
fn tie_large_value_expands_buckets() {
    let path = tmp("large_value_expand");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("x", HimoType::Number, 10); // ヒント 10
    let e = eng.entity();
    eng.tie(e, "x", 100_000);
    assert_eq!(eng.get(e, "x"), Some(100_000));
    let pulled = eng.pull_raw("x", 100_000);
    assert_eq!(pulled, vec![e]);
    // 未使用バケットは空。
    assert!(eng.pull_raw("x", 500).is_empty());
    cleanup(&path);
}

#[test]
#[should_panic(expected = "u32::MAX")]
fn max_value_panics() {
    let path = tmp("max_value_panics");
    let mut eng = Engine::create_standalone(&path).unwrap();
    let e = eng.entity();
    eng.tie(e, "x", u32::MAX);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 2. 観測窓 (define_view) のエッジ
// ──────────────────────────────────────────────────────────────────

/// 未定義の紐名を define_view → Err。
#[test]
fn view_unknown_himo() {
    let path = tmp("view_unknown");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("a", HimoType::Number, 4);
    // "b" は未定義
    let r = eng.define_view(&["a", "b"]);
    assert!(r.is_err(), "unknown himo を含む view は Err になるべき");
    let msg = r.unwrap_err();
    assert!(msg.contains("'b'") || msg.contains("not defined"),
            "エラーメッセージに 'b' か 'not defined' が含まれる: {}", msg);
    cleanup(&path);
}

/// 8 紐 view は OK、9 紐で Err(MAX_VIEW_HIMOS=8 制約)。
#[test]
fn view_max_himos() {
    let path = tmp("view_max_himos");
    let mut eng = Engine::create_standalone(&path).unwrap();
    // 9 本定義(各 max_values=1 にしてセル数を抑える: 2^8=256 / 2^9=512)
    for i in 0..9 {
        eng.define_himo(&format!("h{}", i), HimoType::Number, 1);
    }
    let names8: Vec<String> = (0..8).map(|i| format!("h{}", i)).collect();
    let refs8: Vec<&str> = names8.iter().map(|s| s.as_str()).collect();
    let ok = eng.define_view(&refs8);
    assert!(ok.is_ok(), "8 紐 view は成功するべき: {:?}", ok);

    let names9: Vec<String> = (0..9).map(|i| format!("h{}", i)).collect();
    let refs9: Vec<&str> = names9.iter().map(|s| s.as_str()).collect();
    let err = eng.define_view(&refs9);
    assert!(err.is_err(), "9 紐 view は Err");
    cleanup(&path);
}

/// セル数(card 積)が 100 万超で Err。
#[test]
fn view_cell_count_overflow() {
    let path = tmp("view_cell_overflow");
    let mut eng = Engine::create_standalone(&path).unwrap();
    // (1000+1) × (1000+1) = 1_002_001 > 1M
    eng.define_himo("a", HimoType::Number, 1000);
    eng.define_himo("b", HimoType::Number, 1000);
    let r = eng.define_view(&["a", "b"]);
    assert!(r.is_err(), "セル数 100 万超は Err");
    let msg = r.unwrap_err();
    assert!(msg.contains("1M") || msg.contains("cell"),
            "エラーメッセージに cell 数の情報: {}", msg);
    cleanup(&path);
}

/// 同じ組合せの view を 2 回 define_view → 重複許容(現状の実装は重複チェック無し)。
#[test]
fn duplicate_view_register() {
    let path = tmp("dup_view");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("a", HimoType::Number, 4);
    eng.define_himo("b", HimoType::Number, 4);
    let r1 = eng.define_view(&["a", "b"]);
    assert!(r1.is_ok());
    let r2 = eng.define_view(&["a", "b"]);
    // 実装は重複を許容(エラー無し)。挙動が変わったらここで気付く。
    assert!(r2.is_ok(), "現実装は重複 view を許容: {:?}", r2);
    cleanup(&path);
}

/// 32 views 登録成功、33 個目で Err(MAX_PERSISTED_VIEWS=32)。
#[test]
fn views_max_count() {
    let path = tmp("views_max_count");
    let mut eng = Engine::create_standalone(&path).unwrap();
    // 紐を 33 ペア分用意(各ペアで異なる view)。紐 0..65 を定義。
    for i in 0..66 {
        eng.define_himo(&format!("h{}", i), HimoType::Number, 2);
    }
    for i in 0..32 {
        let r = eng.define_view(&[&format!("h{}", i * 2), &format!("h{}", i * 2 + 1)]);
        assert!(r.is_ok(), "32 個目までは成功: {:?}", r);
    }
    let r33 = eng.define_view(&["h64", "h65"]);
    assert!(r33.is_err(), "33 個目は Err");
    let msg = r33.unwrap_err();
    assert!(msg.contains("too many") || msg.contains("32"),
            "エラーメッセージに上限の情報: {}", msg);
    // 冪等: 既存 view の再登録は OK(カウント増えない)
    let r_dup = eng.define_view(&["h0", "h1"]);
    assert!(r_dup.is_ok(), "重複 view は冪等で Ok");
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 3. entity lifecycle
// ──────────────────────────────────────────────────────────────────

/// entity を delete → entity() で新規 ID を割り当てる。
/// 削除前の eid は live_count から消え、新 eid から古い紐は見えない。
/// (現実装は monotonic、free stack 経由は max_entities 到達後のみ)。
#[test]
fn delete_then_recreate_entity() {
    let path = tmp("del_recreate");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("x", HimoType::Number, 4);

    let e0 = eng.entity();
    eng.tie(e0, "x", 3);
    assert_eq!(eng.get(e0, "x"), Some(3));

    eng.delete(e0);
    assert_eq!(eng.get(e0, "x"), None);
    assert_eq!(eng.entity_count(), 0);

    let e1 = eng.entity();
    // monotonic 方式 → e1 != e0(欠番)
    assert_ne!(e1, e0, "新 entity は古い ID を再利用しない(monotonic)");
    // 古い紐は見えない
    assert_eq!(eng.get(e1, "x"), None);

    cleanup(&path);
}

/// 全紐 untie した entity は pull_raw に出ない、entity 自体は live のまま。
#[test]
fn untie_all_himos_from_entity() {
    let path = tmp("untie_all");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("a", HimoType::Number, 4);
    eng.define_himo("b", HimoType::Number, 4);

    let e = eng.entity();
    eng.tie(e, "a", 1);
    eng.tie(e, "b", 2);
    assert!(eng.pull_raw("a", 1).contains(&e));

    eng.untie(e, "a");
    eng.untie(e, "b");
    assert!(!eng.pull_raw("a", 1).contains(&e));
    assert!(!eng.pull_raw("b", 2).contains(&e));

    // entity 自体は生きている(delete してない)
    assert_eq!(eng.entity_count(), 1);
    cleanup(&path);
}

/// 同じ entity を 2 回 delete、2 回目は noop(panic しない)。
#[test]
fn double_delete() {
    let path = tmp("double_delete");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("x", HimoType::Number, 4);
    let e = eng.entity();
    eng.tie(e, "x", 1);

    eng.delete(e);
    assert_eq!(eng.entity_count(), 0);
    // 2 回目 → panic しない、count はマイナスにならない
    eng.delete(e);
    assert_eq!(eng.entity_count(), 0);
    cleanup(&path);
}

/// 未作成 entity ID を delete しても panic しない(noop)。
#[test]
fn delete_unknown_entity() {
    let path = tmp("delete_unknown");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("x", HimoType::Number, 4);

    // 一度も entity() を呼んでないので 999 は未作成
    eng.delete(999);
    assert_eq!(eng.entity_count(), 0);

    let e = eng.entity();
    assert_eq!(e, 0);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 4. 紐の型混在
// ──────────────────────────────────────────────────────────────────

/// 同じ紐名で tie と tie_text を混在 → 型は最初の define で確定、
/// 以降は元の型として扱われる(get_text は型不一致で None)。
#[cfg(not(debug_assertions))]
#[test]
fn value_then_text_same_himo() {
    let path = tmp("type_mix");
    let mut eng = Engine::create_standalone(&path).unwrap();

    let e1 = eng.entity();
    let e2 = eng.entity();

    // 最初は Value 型として作成
    eng.tie(e1, "x", 42);
    assert!(matches!(eng.himo_type("x"), Some(HimoType::Number)));

    // release build: 型混在は silent に通る(debug_assert が発火しない)
    eng.tie_text(e2, "x", "hello");
    assert!(matches!(eng.himo_type("x"), Some(HimoType::Number)));

    // get_text は Symbol 型でないので None を返す
    assert_eq!(eng.get_text(e2, "x"), None);

    // get は内部の vid を返す(値は取れる)
    assert!(eng.get(e2, "x").is_some());
    assert_eq!(eng.get(e1, "x"), Some(42));

    cleanup(&path);
}

/// `tie_ref(e, "self", e)` で自己参照、panic しない。
#[test]
fn ref_self_reference() {
    let path = tmp("ref_self");
    let mut eng = Engine::create_standalone(&path).unwrap();
    let e = eng.entity();
    eng.tie_ref(e, "self", e);
    assert_eq!(eng.get(e, "self"), Some(e as u32));
    assert!(eng.pull_raw("self", e as u32).contains(&e));
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 5. 永続化
// ──────────────────────────────────────────────────────────────────

/// create → 何も tie せず flush → drop → open → 正常に開ける。
#[test]
fn empty_db_persistence() {
    let path = tmp("empty_persist");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.flush().unwrap();
    }
    let eng = Engine::open_standalone(&path).unwrap();
    assert_eq!(eng.entity_count(), 0);
    cleanup(&path);
}

/// flush → drop → open → tie 追加 → flush → drop → open → 全件見える。
#[test]
fn persist_then_modify() {
    let path = tmp("persist_modify");

    // フェーズ 1: 初期データ
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("x", HimoType::Number, 100);
        let e0 = eng.entity();
        eng.tie(e0, "x", 10);
        assert_eq!(e0, 0);
        eng.flush().unwrap();
    }

    // フェーズ 2: open → 追加データ
    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        assert_eq!(eng.get(0, "x"), Some(10));
        let e1 = eng.entity();
        eng.tie(e1, "x", 20);
        assert_eq!(e1, 1);
        eng.flush().unwrap();
    }

    // フェーズ 3: open → 全件見える
    {
        let eng = Engine::open_standalone(&path).unwrap();
        assert_eq!(eng.get(0, "x"), Some(10));
        assert_eq!(eng.get(1, "x"), Some(20));
        assert_eq!(eng.entity_count(), 2);

        let p10 = eng.pull_raw("x", 10);
        let p20 = eng.pull_raw("x", 20);
        assert_eq!(p10, vec![0]);
        assert_eq!(p20, vec![1]);
    }

    cleanup(&path);
}

/// magic を破壊した DB ファイルを open → Err("not an EnchuDB file")。
#[test]
fn file_version_mismatch() {
    let path = tmp("magic_corrupt");
    {
        let eng = Engine::create_standalone(&path).unwrap();
        drop(eng);
    }

    // magic を 0xFF で上書き
    {
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write};
        let mut f = OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        f.sync_all().unwrap();
    }

    let r = Engine::open_standalone(&path);
    assert!(r.is_err(), "magic 破壊 → open は Err");
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 6. concurrent モードのエッジ
// ──────────────────────────────────────────────────────────────────

/// 空 DB を concurrentize → drop で consumer スレッドが即終了。
#[test]
fn concurrentize_with_empty() {
    let path = tmp("concurrent_empty");
    let eng = Engine::create_standalone(&path).unwrap();
    let arc = Engine::concurrentize(eng);

    // pending は 0
    assert_eq!(arc.pending_writes(), 0);
    assert_eq!(arc.entity_count(), 0);

    // drop → consumer thread join(タイムアウト無し、Drop 内で待つ)
    let start = std::time::Instant::now();
    drop(arc);
    let elapsed = start.elapsed();
    assert!(elapsed < std::time::Duration::from_secs(2),
            "consumer thread は速やかに終了するべき(elapsed={:?})", elapsed);

    cleanup(&path);
}

/// 追加: concurrentize で entity 作成 + tie_async → flush_writes で同期点。
/// (drop 安全性の追加確認)
#[test]
fn concurrentize_basic_lifecycle() {
    let path = tmp("concurrent_basic");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("x", HimoType::Number, 16);
    let arc: Arc<Engine> = Engine::concurrentize(eng);

    let e = arc.entity();
    arc.tie_async(e, "x", 5);
    arc.flush_writes();
    assert_eq!(arc.get(e, "x"), Some(5));

    drop(arc);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 8. 動的拡張 / unique_count / himo_cardinality
// ──────────────────────────────────────────────────────────────────

/// max_values=10 というヒントで定義しても、value=1000 を tie すれば
/// BucketCylinder が動的に resize して引ける。clamp されない。
#[test]
fn test_dynamic_bucket_expansion() {
    let path = tmp("dyn_bucket_expand");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("x", HimoType::Number, 10);
    let e = eng.entity();
    eng.tie(e, "x", 1000);
    let pulled = eng.pull_raw("x", 1000);
    assert_eq!(pulled, vec![e]);
    // clamp で 10 のバケットに落ちてたら bucket[10] に入って、引ける。
    // 逆に 10 では引けないはず。
    assert!(eng.pull_raw("x", 10).is_empty(),
            "value=10 は誰も tie してないので空のはず");
    cleanup(&path);
}

/// tie → untie → delete で unique_count が正しく増減する。
#[test]
fn test_unique_count_basic() {
    let path = tmp("uniq_count_basic");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("k", HimoType::Number, 100);
    assert_eq!(eng.himo_cardinality("k"), Some(0));

    let e1 = eng.entity();
    let e2 = eng.entity();
    let e3 = eng.entity();

    eng.tie(e1, "k", 3);
    assert_eq!(eng.himo_cardinality("k"), Some(1));
    eng.tie(e2, "k", 3); // 同じ値 → カーディナリティ据え置き
    assert_eq!(eng.himo_cardinality("k"), Some(1));
    eng.tie(e3, "k", 7);
    assert_eq!(eng.himo_cardinality("k"), Some(2));

    // untie しても、まだ 3 に 1 件残っている
    eng.untie(e1, "k");
    assert_eq!(eng.himo_cardinality("k"), Some(2));
    // 最後の 1 件を untie → 3 が消える
    eng.untie(e2, "k");
    assert_eq!(eng.himo_cardinality("k"), Some(1));
    // delete で最後の 7 も消える
    eng.delete(e3);
    assert_eq!(eng.himo_cardinality("k"), Some(0));

    cleanup(&path);
}

/// 同じ eid を別値で上書き → unique_count が正しく動く。
#[test]
fn test_unique_count_with_replace() {
    let path = tmp("uniq_count_replace");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("k", HimoType::Number, 10);
    let e1 = eng.entity();
    let e2 = eng.entity();

    eng.tie(e1, "k", 1);
    eng.tie(e2, "k", 1);
    assert_eq!(eng.himo_cardinality("k"), Some(1));

    // e1 を 1 → 2 に上書き。値 1 には e2 が残るので cardinality は +1。
    eng.tie(e1, "k", 2);
    assert_eq!(eng.himo_cardinality("k"), Some(2));

    // e2 を 1 → 3。値 1 が消えて、値 3 が生まれる。±0。
    eng.tie(e2, "k", 3);
    assert_eq!(eng.himo_cardinality("k"), Some(2));
    cleanup(&path);
}

/// himo_cardinality API: 未定義の紐は None、定義済みは 0 から。
#[test]
fn test_himo_cardinality_api() {
    let path = tmp("card_api");
    let mut eng = Engine::create_standalone(&path).unwrap();
    assert_eq!(eng.himo_cardinality("unknown"), None);
    eng.define_himo("k", HimoType::Number, 4);
    assert_eq!(eng.himo_cardinality("k"), Some(0));
    let e = eng.entity();
    eng.tie(e, "k", 0);
    assert_eq!(eng.himo_cardinality("k"), Some(1));
    cleanup(&path);
}
