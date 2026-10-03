//! #381: 辞書 (Tag の値の共有辞書) の上限を開いたまま伸ばす (`Engine::grow_vocab`) と、 使用量を O(1) で見る
//! (`Engine::vocab_usage`)。
//!
//! 語は行を消しても戻らないので、 一意な値を Tag 列に入れる表は、 生きている行が少なくても辞書の上限に着く。
//! 上限は作成時に header に焼かれ、 前は伸ばす口が無かった (DB を作り直すしかなかった)。
//!
//! Windows は予約を広げない (`VOCAB_RESERVE_FACTOR` = 1) ので対象外。

#![cfg(not(windows))]

use enchudb_engine::{Engine, FaultKind, GrowableOptions, TieRejected, ValueType};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue381-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn create(path: &str, vocab_max_entries: u32, vocab_data_size: usize) -> Engine {
    let mut eng = Engine::create_growable_opts(
        path,
        GrowableOptions {
            max_entities: 300_000,
            vocab_max_entries: Some(vocab_max_entries),
            vocab_data_size,
            ..Default::default()
        },
    )
    .expect("create");
    eng.define_himo("id", ValueType::Tag, 0);
    eng
}

/// 一意な値を 1 つ入れる。 `Err` は拒否の種類。
fn put(eng: &Engine, value: &str) -> Result<(), TieRejected> {
    let hid = eng.himo_id("id").unwrap() as u16;
    let e = eng.entity().unwrap();
    eng.try_tie_text_to_by_id(e, hid, value)
}

fn is_vocab_full(r: Result<(), TieRejected>) -> bool {
    matches!(r, Err(TieRejected::Fault(FaultKind::VocabSpace)))
}

#[test]
fn grow_vocab_raises_the_entry_limit_while_open_and_after_reopen() {
    let path = tmp("entries");
    let eng = create(&path, 16, 64 * 1024);
    let u = eng.vocab_usage();
    assert_eq!((u.entries, u.max_entries), (0, 16));
    // 予約は上限の 4 倍以上 (64 KiB 単位の切り上げで増えうる)
    assert!(u.reserved_entries >= 64, "{u:?}");
    let room = u.reserved_entries;

    for i in 0..16 {
        put(&eng, &format!("v{i}")).unwrap();
    }
    assert!(is_vocab_full(put(&eng, "v16")), "17 個目は上限で断られる");
    assert_eq!(eng.vocab_usage().entries, 16);

    // 開いたまま伸ばす
    let u = eng.grow_vocab(40, 0).unwrap();
    assert_eq!((u.entries, u.max_entries), (16, 40));
    for i in 16..40 {
        put(&eng, &format!("v{i}")).unwrap_or_else(|e| panic!("v{i} after grow: {e:?}"));
    }
    assert!(is_vocab_full(put(&eng, "v40")), "伸ばした上限で止まる");

    // 縮めない / 予約を越える値は何も変えずに断る
    assert_eq!(eng.grow_vocab(10, 0).unwrap().max_entries, 40);
    let err = eng.grow_vocab(room + 1, 0).expect_err("beyond the reservation");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{err}");
    assert_eq!(eng.vocab_usage().max_entries, 40);
    // 使い切っていない上限で閉じる (開き直した時に語数から上限を決めていないかを見る)
    eng.grow_vocab(50, 0).unwrap();
    let mut eng = eng;
    eng.flush().unwrap();
    drop(eng);

    // 開き直しても伸ばした上限のまま (領域の長さから決めない)、 予約は伸ばした上限の 4 倍
    let eng = Engine::open_standalone(&path).unwrap();
    let u = eng.vocab_usage();
    assert_eq!((u.entries, u.max_entries), (40, 50), "{u:?}");
    assert!(u.reserved_entries >= 200, "予約は伸ばした上限の 4 倍以上: {u:?}");
    for i in 40..50 {
        put(&eng, &format!("v{i}")).unwrap_or_else(|e| panic!("v{i} after reopen: {e:?}"));
    }
    assert!(is_vocab_full(put(&eng, "v50")), "開き直した後も header の上限で止まる");
    eng.grow_vocab(100, 0).unwrap();
    for i in 50..100 {
        put(&eng, &format!("v{i}")).unwrap_or_else(|e| panic!("v{i} after reopen + grow: {e:?}"));
    }
    for i in 0..100 {
        let vid = eng.vocab_id(&format!("v{i}")).unwrap_or_else(|| panic!("v{i} lost"));
        assert_eq!(eng.pull_raw("id", vid).len(), 1, "v{i}");
    }
}

#[test]
fn grow_vocab_raises_the_data_limit() {
    let path = tmp("data");
    // 予約の切り上げ (64 KiB) に隠れない大きさ
    let eng = create(&path, 10_000, 256 * 1024);
    let value = |i: usize| format!("{i:0>1000}");
    let mut n = 0;
    let rejected = loop {
        match put(&eng, &value(n)) {
            Ok(()) => n += 1,
            Err(e) => break e,
        }
        assert!(n < 1_000, "data の上限で止まらない");
    };
    // data の上限は 「一杯」 (空き不足ではない)
    assert!(matches!(rejected, TieRejected::Fault(FaultKind::VocabSpace)), "{rejected:?}");
    let u = eng.vocab_usage();
    assert!(u.data_bytes + 1000 > u.max_data_bytes && u.max_data_bytes == 256 * 1024, "{u:?}");

    let u = eng.grow_vocab(0, 1 << 20).unwrap();
    assert_eq!(u.max_data_bytes, 1 << 20);
    let before = n;
    while put(&eng, &value(n)).is_ok() {
        n += 1;
    }
    assert!(n >= before + 700, "伸ばした分だけ入る: {before} -> {n}");
    assert!(eng.vocab_usage().data_bytes <= 1 << 20);
    for i in 0..n {
        assert!(eng.vocab_id(&value(i)).is_some(), "{i}");
    }
}

/// 予約は宣言より広いが、 file は宣言 size (の 64 KiB 切り上げ) を越えて伸ばさない — 宣言 size で予約する旧 binary が
/// 「file が予約より大きい」 で開けなくならないように。 宣言 size は 2 の冪からずらす (倍々の伸長が宣言を飛び越える形)。
#[test]
fn vocab_segments_do_not_grow_past_the_declared_size() {
    let align = |n: u64| n.div_ceil(64 * 1024) * 64 * 1024;
    let fill = |path: &str, max_entries: u32, data_size: usize, width: usize| {
        let mut eng = create(path, max_entries, data_size);
        let mut n = 0;
        while put(&eng, &format!("{n:0>width$}")).is_ok() {
            n += 1;
        }
        eng.flush().unwrap();
        let u = eng.vocab_usage();
        drop(eng);
        u
    };
    let len = |path: &str, name: &str| std::fs::metadata(format!("{path}/{name}")).unwrap().len();

    // data を上限まで使う
    let path = tmp("compat-data");
    let u = fill(&path, 200_000, 768 * 1024, 1000);
    assert!(u.data_bytes > 768 * 1024 - 1000, "data を上限まで使う前提: {u:?}");
    let got = len(&path, "vocab.data.seg");
    assert!(got <= align(768 * 1024), "vocab.data.seg {got} > declared");

    // 語数を上限まで使う (offsets)
    let path = tmp("compat-offsets");
    let u = fill(&path, 20_000, 1 << 20, 8);
    assert_eq!(u.entries, 20_000, "語数を上限まで使う前提: {u:?}");
    let got = len(&path, "vocab.offsets.seg");
    assert!(got <= align(20_000 * 8), "vocab.offsets.seg {got} > declared");
}
