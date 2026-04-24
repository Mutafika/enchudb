//! ContentStore の cross-process safety テスト。
//!
//! BUGS.md 記載の bug: プロセスが `sync()` を呼ばずに exit すると
//! header の data_end が古いまま残り、次プロセスが同 offset に
//! 上書きしてしまう問題の regression guard。
//!
//! 同一プロセス内で Engine を作って drop(= プロセス終了相当)を繰り返し、
//! 内容が上書きされていないことを確認する。
//!
//! 本当の cross-process test は別 binary 起動が要るが、
//! data_end が mmap region 上 AtomicU32 になったことで
//! 「drop しても header が最新」= 同等の保証が成立する。

use enchudb::Engine;

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-content-xp-{}-{}", tag, std::process::id());
    let _ = std::fs::remove_file(&p);
    p
}

#[test]
fn drop_without_sync_persists_data_end() {
    let path = tmp("drop_no_sync");

    let e1 = {
        let mut eng = Engine::create_standalone(&path).unwrap();
        let e = eng.entity();
        eng.content(e, "body", b"first process data");
        // flush() / sync() は意図的に呼ばない。drop だけ。
        e
    };

    // 2 回目の open、別 entity に異なる content を書く。
    let e2 = {
        let mut eng = Engine::open_standalone(&path).unwrap();
        let e = eng.entity();
        eng.content(e, "body", b"second process data");
        e
    };

    // 3 回目: 両方の content が正しく読めるか。
    let eng = Engine::open_standalone(&path).unwrap();
    let body1 = eng.get_content(e1, "body");
    let body2 = eng.get_content(e2, "body");
    assert_eq!(body1, Some(b"first process data".as_ref()));
    assert_eq!(body2, Some(b"second process data".as_ref()));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn many_short_lived_writes_do_not_corrupt() {
    // oboro hook 的 pattern: 開く→書く→drop を繰り返す。
    // 各書き込みが独立 entity で、最後に全部読めることを保証。
    let path = tmp("many_hooks");
    let mut entities = Vec::new();

    for i in 0..20 {
        let mut eng = Engine::open_standalone(&path).ok()
            .unwrap_or_else(|| Engine::create_standalone(&path).unwrap());
        let e = eng.entity();
        let body = format!("hook event #{i}, timestamp={}", i * 1000);
        eng.content(e, "body", body.as_bytes());
        entities.push((e, body));
        // no flush, drop
    }

    // 最後に全部読む
    let eng = Engine::open_standalone(&path).unwrap();
    for (e, expected) in &entities {
        let got = eng.get_content(*e, "body");
        assert_eq!(
            got.map(|b| std::str::from_utf8(b).unwrap()),
            Some(expected.as_str()),
            "entity {e} content should be '{expected}', got {got:?}"
        );
    }

    let _ = std::fs::remove_file(&path);
}
