//! #374: 辞書 (vocab) の索引 (ハッシュ表) は作成時に領域の最大の大きさ (`vocab_max_entries` の次の 2 の冪 × 13 B、
//! entity cap 1,600 万なら 3.49 GB、 sparse) で確保され、 その全域に語が散っていた。 語が少なくても、 引く・入れる
//! たびに別のページを触るので、 page cache が語数 × ページの大きさで膨らみ、 メモリの小さい箱では `lookup` のたびに
//! 読み直しになった (実機で書き込み 1 回が 2〜9 ms → 5〜16 秒)。
//!
//! 今は表を語数に合わせて伸ばす。 ここでは 「触ったページ」 を索引のファイルの実体 (allocated) で見る: 語 2 万個で
//! 0.28.5 は 367 MB (語ごとに別のページ)、 今は 2 MB 未満。

#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};
use std::os::unix::fs::MetadataExt;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue374-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

/// ファイルの実体 (確保されたブロック) の byte 数。
fn allocated(path: &str) -> u64 {
    std::fs::metadata(path).unwrap().blocks() * 512
}

#[test]
fn vocab_index_touches_pages_in_proportion_to_the_values() {
    let path = tmp("alloc");
    let _ = std::fs::remove_dir_all(&path);
    let n = 20_000u32;
    {
        // entity cap 1,600 万 → vocab_max_entries 2.56 億 → 索引の領域 2^28 slot = 3.49 GB
        let mut eng = Engine::create_with_capacity(&path, 16_000_000).unwrap();
        eng.define_himo("tag", ValueType::Tag, 0);
        for i in 0..n {
            let e = eng.entity().unwrap();
            eng.tie_text(e, "tag", &format!("value-{i}"));
        }
        eng.flush().unwrap();
        let index = allocated(&format!("{path}/vocab.index.seg"));
        let data = allocated(&format!("{path}/vocab.data.seg"));
        eprintln!("{n} 語: vocab.index.seg {:.2} MB, vocab.data.seg {:.2} MB", index as f64 / 1e6, data as f64 / 1e6);
        // 表は語数の 2〜4 倍 (1 slot 13 B) で、 倍々に伸ばした古い表も残る: 合わせて語数 × 13 B × 8 程度
        assert!(
            index < 4 * 1024 * 1024,
            "語 {n} 個で索引が {:.1} MB のページを触った (#374: 語ごとに別のページ)",
            index as f64 / 1e6
        );
        for i in (0..n).step_by(997) {
            assert!(eng.vocab_id(&format!("value-{i}")).is_some(), "value-{i}");
        }
    }
    // 開き直しても引ける (graceful close の後は表をそのまま使う)
    let eng = Engine::open_standalone(&path).unwrap();
    assert!(!eng.vocab_index_rebuilt_on_load(), "graceful close の後なのに索引を作り直した");
    for i in (0..n).step_by(991) {
        let vid = eng.vocab_id(&format!("value-{i}")).unwrap_or_else(|| panic!("value-{i}"));
        assert!(!eng.pull_raw("tag", vid).is_empty(), "value-{i} の行");
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
