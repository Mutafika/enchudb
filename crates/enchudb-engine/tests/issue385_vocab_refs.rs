//! #385: 辞書の語の参照数は file (`vocab.refs.seg`) に残し、 きれいに閉じた後は開く時に数え直さない。 落ちた後 /
//! 0.29.0 が書いた後 / file が無い時だけ、 生きている行の cell を数え直す。
//!
//! 「数え直さなかった」 ことは、 閉じた後に参照数の file を 0 で潰して確かめる: file を使えば潰した数がそのまま
//! 出る (生きている値まで回収できる数に入る)、 数え直せば正しい数に戻る。

#![cfg(not(windows))]

use enchudb_engine::{Engine, GrowableOptions, LivePred, ValueType};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue385-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p);
    p
}

const ROWS: u32 = 100;

/// 回収する DB に 100 行 (値は全部別) を書き、 先頭 40 行を消して閉じる。 回収できる語は 40。
fn build(path: &str) {
    let mut eng = Engine::create_growable_opts(
        path,
        GrowableOptions { max_entities: 10_000, vocab_reclaim: true, ..Default::default() },
    )
    .unwrap();
    eng.define_himo("k", ValueType::Tag, 0);
    let k = eng.himo_id("k").unwrap() as u16;
    let rows: Vec<u64> = (0..ROWS)
        .map(|i| {
            let e = eng.entity().unwrap();
            eng.try_tie_text_to_by_id(e, k, &format!("v{i}")).unwrap();
            e
        })
        .collect();
    for &e in &rows[..40] {
        eng.delete(e);
    }
    assert_eq!(eng.vocab_usage().reclaimable_entries, 40);
}

/// 参照数の file の数を全部 0 にする (header の後ろ)。
fn zero_refs(path: &str) {
    fill_refs(path, 0);
}

/// 参照数の file の数を全部 `n` にする (header の後ろ)。
fn fill_refs(path: &str, n: u32) {
    let f = format!("{path}/vocab.refs.seg");
    let mut b = std::fs::read(&f).unwrap();
    assert_eq!(&b[..4], b"VRF1");
    for c in b[16..].chunks_exact_mut(4) {
        c.copy_from_slice(&n.to_le_bytes());
    }
    std::fs::write(&f, b).unwrap();
}

fn reclaimable_after_reopen(path: &str) -> u32 {
    let eng = Engine::open_standalone(path).unwrap();
    let u = eng.vocab_usage();
    assert!(u.reclaim && u.reclaim_ready, "開いた時から回収している: {u:?}");
    u.reclaimable_entries
}

/// きれいに閉じた後は file の数をそのまま使う (潰した 0 がそのまま出る = 数え直していない)。
#[test]
fn clean_close_reuses_the_persisted_counts() {
    let path = tmp("clean");
    build(&path);
    assert_eq!(reclaimable_after_reopen(&path), 40, "閉じた時の数");
    zero_refs(&path);
    assert_eq!(reclaimable_after_reopen(&path), ROWS, "file の数を使う (数え直していない)");
    let _ = std::fs::remove_dir_all(&path);
}

/// 落ちた後 (閉じる処理が走らない) は数え直す。 開いている最中の directory を写し取ったもの = 落ちた時に disk に
/// 残る形 (開いた時に clean の印を下ろして書き出してある)。
#[test]
fn crash_recounts() {
    let path = tmp("crash");
    let copy = tmp("crash-copy");
    build(&path);
    {
        let eng = Engine::open_standalone(&path).unwrap();
        let st = std::process::Command::new("cp").args(["-R", &path, &copy]).status().unwrap();
        assert!(st.success());
        drop(eng);
    }
    // 前の数が残っていても (0 でなくても) 捨てて数え直す
    fill_refs(&copy, 5);
    assert_eq!(reclaimable_after_reopen(&copy), 40, "落ちた後は数え直す");
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&copy);
}

/// 0.29.0 は参照数の file を知らずに書き、 閉じる時に `CLEAN_GEN` (2) を書く。 その後は数え直す。
#[test]
fn after_an_older_binary_recounts() {
    let path = tmp("older");
    build(&path);
    let data = format!("{path}/vocab.data.seg");
    let mut b = std::fs::read(&data).unwrap();
    assert_eq!(u32::from_le_bytes(b[12..16].try_into().unwrap()), 3, "回収する DB は CLEAN_REFS で閉じる");
    b[12..16].copy_from_slice(&2u32.to_le_bytes());
    std::fs::write(&data, b).unwrap();
    zero_refs(&path);
    assert_eq!(reclaimable_after_reopen(&path), 40, "0.29.0 の後は数え直す");
    let _ = std::fs::remove_dir_all(&path);
}

/// file が無ければ数え直して作る。
#[test]
fn missing_file_recounts() {
    let path = tmp("missing");
    build(&path);
    std::fs::remove_file(format!("{path}/vocab.refs.seg")).unwrap();
    assert_eq!(reclaimable_after_reopen(&path), 40);
    assert!(std::path::Path::new(&format!("{path}/vocab.refs.seg")).exists());
    assert_eq!(reclaimable_after_reopen(&path), 40);
    let _ = std::fs::remove_dir_all(&path);
}

/// 閉じた時に残っている購読が押さえていた番号は file に残さない (残すと、 開き直した後にその値の行が全部消えても
/// 参照 1 のまま回収されない)。
#[test]
fn subscription_pins_are_not_persisted() {
    let path = tmp("pins");
    build(&path);
    let sub = {
        let eng = Engine::open_standalone(&path).unwrap();
        let k = eng.himo_id("k").unwrap() as u16;
        let sub = eng.subscribe(vec![LivePred::EqText { himo_id: k, text: "v50".into() }]).unwrap();
        assert_eq!(eng.vocab_usage().reclaimable_entries, 40);
        sub
        // 購読を持ったまま閉じる
    };
    let eng = Engine::open_standalone(&path).unwrap();
    let rows = eng.pull_raw("k", eng.vocab_id("v50").unwrap());
    assert_eq!(rows.len(), 1);
    eng.delete(rows[0]);
    assert_eq!(eng.vocab_usage().reclaimable_entries, 41, "購読の押さえは閉じた時に返した");
    drop(sub);
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// 消した行の空き slot が多い DB で数え直しても、 生きている行の値は全部数える (空の word を飛ばす境界)。
#[test]
fn recount_skips_dead_slots_but_counts_every_live_row() {
    let path = tmp("sparse");
    let n = 5_000u32;
    let keep: Vec<u32> = vec![0, 1, 63, 64, 65, 127, 128, 1_000, 4_095, 4_096, n - 1];
    {
        // 回収しない DB で全部書いてから消し (消した eid は使い回されず空き slot になる)、 回収する DB にする
        let mut eng = Engine::create_growable_opts(
            &path,
            GrowableOptions { max_entities: 10_000, ..Default::default() },
        )
        .unwrap();
        eng.define_himo("k", ValueType::Tag, 0);
        let k = eng.himo_id("k").unwrap() as u16;
        let rows: Vec<u64> = (0..n)
            .map(|i| {
                let e = eng.entity().unwrap();
                eng.try_tie_text_to_by_id(e, k, &format!("v{i}")).unwrap();
                e
            })
            .collect();
        for (i, &e) in rows.iter().enumerate() {
            if !keep.contains(&(i as u32)) {
                eng.delete(e);
            }
        }
        eng.enable_vocab_reclaim().unwrap();
    }
    let eng = Engine::open_standalone(&path).unwrap();
    let u = eng.vocab_usage();
    assert_eq!(u.reclaimable_entries, n - keep.len() as u32, "{u:?}");
    let k = eng.himo_id("k").unwrap() as u16;
    // 回収した場所を使い回させてから、 残した行の値が変わっていないことを見る
    for i in 0..n {
        let e = eng.entity().unwrap();
        eng.try_tie_text_to_by_id(e, k, &format!("w{i}")).unwrap();
    }
    for &i in &keep {
        let vid = eng.vocab_id(&format!("v{i}")).unwrap_or_else(|| panic!("v{i} が回収された"));
        assert_eq!(eng.pull_raw("k", vid).len(), 1, "v{i}");
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// 前の時点の参照数の file (語数が今と違う) は、 閉じた時の印があっても信じない (部分的な copy / 戻した backup)。
#[test]
fn a_file_from_another_point_in_time_is_not_trusted() {
    let path = tmp("stale-file");
    build(&path);
    let f = format!("{path}/vocab.refs.seg");
    zero_refs(&path);
    let old = std::fs::read(&f).unwrap();
    {
        // 数え直させてから語を 50 足して (空いた 40 か所を使い回し、 10 語は新しい場所)、 きれいに閉じる
        std::fs::remove_file(&f).unwrap();
        let eng = Engine::open_standalone(&path).unwrap();
        let k = eng.himo_id("k").unwrap() as u16;
        for i in 0..50 {
            let e = eng.entity().unwrap();
            eng.try_tie_text_to_by_id(e, k, &format!("added{i}")).unwrap();
        }
        assert_eq!(eng.vocab_usage().entries, ROWS + 10);
    }
    std::fs::write(&f, old).unwrap();
    assert_eq!(reclaimable_after_reopen(&path), 0, "語数が違う file は使わずに数え直す");
    let _ = std::fs::remove_dir_all(&path);
}

/// 1 回の session で新しい語をたくさん入れても、 参照数の file を伸ばして全部数える (伸ばさないと、 file の commit の先の
/// 語は参照を取れず書き込みが古い番号として断られる)。
#[test]
fn many_new_words_in_one_session() {
    let path = tmp("many");
    let n = 60_000u32;
    {
        let mut eng = Engine::create_growable_opts(
            &path,
            GrowableOptions { max_entities: 100_000, vocab_reclaim: true, ..Default::default() },
        )
        .unwrap();
        eng.define_himo("k", ValueType::Tag, 0);
        let k = eng.himo_id("k").unwrap() as u16;
        for i in 0..n {
            let e = eng.entity().unwrap();
            eng.try_tie_text_to_by_id(e, k, &format!("v{i}")).unwrap_or_else(|r| panic!("v{i}: {r:?}"));
        }
        assert_eq!(eng.vocab_usage().reclaimable_entries, 0);
    }
    assert_eq!(reclaimable_after_reopen(&path), 0, "閉じた時の数も全部の語を数えている");
    let _ = std::fs::remove_dir_all(&path);
}
