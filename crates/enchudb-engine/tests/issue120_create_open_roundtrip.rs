//! #120: `vocab_data_size` が align8 で u32 上限を跨ぐ create が「成功したのに
//! 開けない DB」を作る問題の regression test。
//!
//! 旧挙動: create 側は **整列前** の要求値 (u32::MAX = 4 GiB−1) だけを検証して通し、
//! header には **整列後** の 2^32 が焼かれる。 open 側は header 値を u32 data_end 制約で
//! 検証するため `vocab_data_size 4294967296 exceeds format limit` で恒久的に開けない。
//! naruhodo の 7 分フルビルドが「完走ログを出した後に全損」する形で実踏した。
//!
//! 期待: create が **整列後** のサイズで検証し、 上限超過なら **書く前に** Err。

use enchudb_engine::{Engine, LeafScale};

/// 並行 cargo test で衝突しない一意パス (固定 /tmp パスは偽 flaky の原因)。
fn tmp_path(tag: &str) -> String {
    format!("/tmp/issue120_{}_{}.enchu", tag, std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

/// align8 が u32 上限を跨ぐ要求は create 時点で Err になり、 ファイルを残さない。
#[test]
fn vocab_data_size_u32_max_is_rejected_at_create() {
    let path = tmp_path("u32max");
    cleanup(&path);

    let res = Engine::create_growable_with_leaf(
        &path,
        1000,
        Some(u32::MAX as usize), // align8 → 2^32 = 上限超過
        Some(1 << 30),
        LeafScale::Gb16,
    );

    let err: std::io::Error = match res {
        Ok(_) => panic!("create が成功した — #120 の regression (open 不能な DB が生まれる)"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "err = {err}");
    assert!(
        err.to_string().contains("vocab_data_size"),
        "どの knob が上限超過かを示すこと: {err}"
    );
    // 書く前に落ちる = 壊れたファイルを残さない
    assert!(
        !std::path::Path::new(&path).exists(),
        "create が失敗したのにファイルが残っている (書いてから落ちている)"
    );

    cleanup(&path);
}

/// 上限ちょうど (8-aligned) は通り、 **再 open できる** — 検証を厳しくしすぎて
/// 正常な最大値を弾いていないことの確認。
#[test]
fn vocab_data_size_at_aligned_limit_roundtrips() {
    let path = tmp_path("limit");
    cleanup(&path);

    // u32::MAX を 8 の倍数へ切り下げた値 = align8 が no-op になる最大値。
    let at_limit = (u32::MAX as usize) & !7;
    let eng = Engine::create_growable_with_leaf(&path, 1000, Some(at_limit), Some(1 << 30), LeafScale::Gb16)
        .expect("上限ちょうどの vocab_data_size が create できること");
    drop(eng);

    // #120 の本質はここ: create が通ったなら open も通らなければならない。
    Engine::open_readonly(&path).expect("create できた DB は必ず open できること");

    cleanup(&path);
}
