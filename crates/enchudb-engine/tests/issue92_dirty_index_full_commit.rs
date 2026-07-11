//! 回帰テスト: Mutafika/enchudb#92
//! dirty open が vocab index の予約領域を全域 zero-fill + 物理コミット (index_cap 比例) する。
//! live vocab はごく少数なのに、dirty reopen 一発で index 予約全域 (index_cap×13B) を
//! commit してしまう。#56 ③ が close 後も未修正で残存。
//!
//! #92 fix (rebuild を used-slot only touch に) 済 → un-ignore。 dirty reopen が
//! index 予約全域を物理コミットしないことを assert する regression guard。

use std::os::unix::fs::MetadataExt;
use enchudb_engine::{Engine, ValueType};

/// enchu.db + .oplog の実割当バイト (st_blocks×512、sparse hole は数えない)。
fn phys(path: &str) -> u64 {
    let mut total = 0;
    for suffix in ["", ".oplog"] {
        if let Ok(m) = std::fs::metadata(format!("{path}{suffix}")) {
            total += m.blocks() * 512;
        }
    }
    total
}

#[test]
fn dirty_reopen_does_not_commit_full_index_region() {
    // 別プロセス helper mode: dirty write して flush せず process::exit
    // (Drop/flush を回避 = clean_flag=0 の crash 相当。同一プロセス forget は
    //  flock #80 で二重 open 不可のため別プロセスにする)。
    if std::env::var("ENCHU_DIRTY_HELPER").is_ok() {
        let path = std::env::var("ENCHU_DIRTY_PATH").unwrap();
        let mut eng = Engine::open_standalone(&path).unwrap();
        let e = eng.entity();
        eng.tie_text(e, "tag", "x");
        std::process::exit(0);
    }

    let dir = std::env::temp_dir().join(format!("enchu_issue92_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.db");
    let p = path.to_str().unwrap();

    // seed: cap=262144 ⇒ vocab_max=4.19M ⇒ index_cap=4,194,304 ⇒ index region ≈ 54MB。
    // live vocab は 5 個だけ (本来 <1MB で足りる)。clean で確定。
    {
        let mut eng = Engine::create_growable_with_capacity(p, 262_144).unwrap();
        eng.define_himo("tag", ValueType::Tag, 1000);
        for i in 0..5 {
            let e = eng.entity();
            eng.tie_text(e, "tag", &format!("v{i}"));
        }
        eng.flush().unwrap();
    }
    let before = phys(p);

    // 別プロセスで dirty 化 (自身の test binary を helper mode で再実行)。
    let st = std::process::Command::new(std::env::current_exe().unwrap())
        // helper 再実行は ignore 状態に依存しないよう --include-ignored で両対応。
        // (env ENCHU_DIRTY_HELPER 分岐が test body 冒頭で発火する)
        .args(["dirty_reopen_does_not_commit_full_index_region", "--exact", "--include-ignored"])
        .env("ENCHU_DIRTY_HELPER", "1")
        .env("ENCHU_DIRTY_PATH", p)
        .status()
        .unwrap();
    assert!(st.success(), "dirty helper subprocess failed");

    // dirty reopen (write path) → ここで ③ の full rebuild が走る。
    {
        let _e = Engine::open_standalone(p).unwrap();
    }
    let after = phys(p);
    let grew_kb = after.saturating_sub(before) / 1024;

    let _ = std::fs::remove_dir_all(&dir);

    // live vocab 5 個なら本来 <1MB。index 予約全域コミットなら数十MB。
    assert!(
        grew_kb < 4096,
        "dirty reopen が {grew_kb} KB コミット (index 予約全域 zero-fill = #92)。\
         rebuild は live/used slot 分のみ touch すべき。"
    );
}
