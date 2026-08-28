//! **embedded DB は 「DB が一杯」 「値が範囲外」 で host process を殺してはいけない** (#59)。
//!
//! enchudb は他人の process に in-process で埋め込まれる。 capacity 到達のような
//! 想定内事象で `panic!` すると、
//!
//! - Rust caller: host app ごと落ちる (DB の一杯は app の死ではない)
//! - FFI caller: panic が `extern "C"` 境界を unwind して **未定義動作**
//!
//! なので engine 内部ではこれらを panic にせず、 **write を拒否 + 種別ごとに計数 +
//! rate-limited warn** に統一する (`FaultKind` / `Engine::fault_count`)。 `Result` を
//! 返せる API では併せて `Err` を返す (`entity()` / `entity_in()` 等)。
//!
//! ここでは 「昔 panic していた入力で panic しないこと」 と 「拒否が観測できること」 を
//! 両方見る。 拒否だけして数えないのは 「黙って落とす」 で、 panic より悪い。

use enchudb_engine::{Engine, FaultKind, ValueType};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue59-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    for suffix in ["", ".oplog", ".tables", ".crc", ".lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{p}{suffix}"));
    }
    p
}

/// entity 枠を使い切っても panic しない。 `entity()` が `Err`、 fault が計上される。
#[test]
fn entity_space_exhaustion_returns_err_instead_of_panicking() {
    let path = tmp("entity-space");
    let eng = Engine::create_with_capacity(&path, 8).expect("create");

    let mut made = 0usize;
    let mut last_err: Option<String> = None;
    for _ in 0..64 {
        match eng.entity() {
            Ok(_) => made += 1,
            Err(e) => {
                last_err = Some(e);
                break;
            }
        }
    }

    assert!(made > 0, "1 個も払い出せていない (前提が壊れている)");
    let err = last_err.expect("枠を使い切っても Err が返っていない (= 無限に払い出せている?)");
    assert!(
        err.contains("entity space exhausted"),
        "枠切れ以外の理由で失敗した: {err}"
    );
    assert!(
        eng.fault_count(FaultKind::EntitySpace) >= 1,
        "拒否したのに fault が計上されていない (黙って落としている)"
    );
    // 拒否後も engine は生きていること (read が通る)
    assert!(eng.entity_count() > 0);
}

/// `u32::MAX` は cell の sentinel 予約値。 渡しても panic せず、 write が拒否される。
#[test]
fn sentinel_value_is_rejected_not_panicked() {
    let path = tmp("sentinel");
    let mut eng = Engine::create_standalone(&path).expect("create");
    eng.define_himo("age", ValueType::Number, 100);
    let e = eng.entity().unwrap();

    eng.tie(e, "age", u32::MAX); // 旧実装は assert! で panic

    assert_eq!(
        eng.get(e, "age"),
        None,
        "sentinel が cell に書かれている (read 側が「値なし」と区別できない)"
    );
    assert!(
        eng.fault_count(FaultKind::ValueOutOfRange) >= 1,
        "拒否したのに fault が計上されていない"
    );

    // 正常値はそのまま通ること (拒否が波及していない)
    eng.tie(e, "age", 30);
    assert_eq!(eng.get(e, "age"), Some(30));
}

/// vocabulary の天井に当たっても panic しない。 text write が拒否され、 fault が計上される。
#[test]
fn vocabulary_full_rejects_text_writes_instead_of_panicking() {
    use enchudb_engine::GrowableOptions;
    let path = tmp("vocab-full");
    let mut eng = Engine::create_growable_opts(
        &path,
        GrowableOptions {
            max_entities: 1_000,
            vocab_max_entries: Some(4), // すぐ天井に当たる
            ..Default::default()
        },
    )
    .expect("create");
    eng.define_himo("city", ValueType::Tag, 0);

    // 天井 (4) を必ず越える数の *別々の* 値を張る
    let mut eids = Vec::new();
    for i in 0..32 {
        let e = eng.entity().unwrap();
        eng.tie_text(e, "city", &format!("city-{i}"));
        eids.push((e, format!("city-{i}")));
    }

    assert!(
        eng.fault_count(FaultKind::VocabSpace) >= 1,
        "天井を越えたのに fault が計上されていない (= panic していないが黙って壊れている?)"
    );

    // 天井前に入った値は読めること。 拒否された分は None (壊れた値ではない)。
    let readable = eids
        .iter()
        .filter(|(e, want)| eng.get_text(*e, "city") == Some(want.as_bytes()))
        .count();
    assert!(readable > 0, "天井前の値も読めなくなっている");
    assert!(
        readable < eids.len(),
        "天井に当たっていない (前提が壊れている: vocab_max_entries が効いていない)"
    );
}
