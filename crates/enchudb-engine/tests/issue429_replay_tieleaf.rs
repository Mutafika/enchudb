//! #429: 落ちた後の oplog の再生は、 自分の TieLeaf も当て直す。
//!
//! 旧: 再生 (`apply_oplog_op`) は Untie / Delete / Tie を当て直すが、 自分の TieLeaf は 「本体にある」 として何もしな
//! かった。 cell の版数の無い DB は再生する record を全部受け入れるので、 [Untie, TieLeaf] の順に並ぶと Untie だけが
//! 当たり、 本体に入っていた値ごと cell が空になった (電源断でなく process の死で)。
//!
//! 子 process で書いて、 周期の書き出し (100 ms) より前に abort し、 親で開き直して読む。 旧実装での実測 (release、
//! M4 Max): untie の後に書き直す形は 10 回中 10 回 `None`。

use enchudb_engine::{Engine, ValueType};
use std::process::Command;

const CHILD: &str = "ENCHU_ISSUE429_CHILD";
const CELL_VERSION: &str = "ENCHU_ISSUE429_CELL_VERSION";

/// 子: 書いて落ちる。
#[test]
fn issue429_child() {
    let Ok(path) = std::env::var(CHILD) else { return };
    let mut eng = if std::env::var(CELL_VERSION).is_ok() {
        Engine::create_growable_with_cell_version(&path, 1000).unwrap()
    } else {
        Engine::create_growable(&path).unwrap()
    };
    eng.define_himo("body", ValueType::Leaf, 0);
    let e = eng.entity().unwrap();
    eng.flush().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    eng.tie_text_to(e, "body", "v1");
    eng.oplog_sync().unwrap();
    eng.untie(e, "body");
    eng.tie_text_to(e, "body", "v2");
    eng.commit();
    // 周期の書き出し (checkpoint を進める) より先に落ちる = untie と書き直しの record が再生の範囲に残る
    std::process::abort();
}

/// 子を走らせて開き直し、 書き直した値を読む (10 回)。
fn crash_and_reopen(cell_version: bool) -> Vec<Option<String>> {
    (0..10)
        .map(|i| {
            let path = std::env::temp_dir().join(format!("issue429_{cell_version}_{}_{i}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            let p = path.to_str().unwrap().to_string();
            let mut cmd = Command::new(std::env::current_exe().unwrap());
            cmd.args(["issue429_child", "--exact", "--nocapture"]).env(CHILD, &p);
            if cell_version {
                cmd.env(CELL_VERSION, "1");
            }
            let st = cmd.status().unwrap();
            assert!(!st.success(), "子は abort で終わるはず");
            let eng = Engine::open_concurrent_with_oplog(&p, 4 << 20).unwrap();
            let got = eng.get_text_owned(0, "body").map(|b| String::from_utf8_lossy(&b).into_owned());
            drop(eng);
            let _ = std::fs::remove_dir_all(&path);
            got
        })
        .collect()
}

#[test]
fn leaf_rewritten_after_untie_survives_a_crash() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let got = crash_and_reopen(false);
    assert!(got.iter().all(|g| g.as_deref() == Some("v2")), "落ちた後の再生で書き直した値が消えた: {got:?}");
}

/// cell の版数のある DB: 本体にある TieLeaf は同じ HLC で弾かれ、 二重に当たらない (値は書き直した方)。
#[test]
fn leaf_rewritten_after_untie_survives_a_crash_with_cell_versions() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let got = crash_and_reopen(true);
    assert!(got.iter().all(|g| g.as_deref() == Some("v2")), "落ちた後の再生で書き直した値が消えた: {got:?}");
}
