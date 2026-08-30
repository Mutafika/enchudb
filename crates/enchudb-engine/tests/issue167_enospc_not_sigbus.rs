//! **ディスクが埋まっても SIGBUS でプロセスごと落ちない** (#167)。
//!
//! DB body は sparse ファイルで、 書き込みは mmap 経由。 穴に書くと kernel がその時点で
//! block を割り当てるが、 **割り当てに失敗 (ENOSPC) しても write システムコールが無いので
//! errno の返り先が無く SIGBUS になる**。 `std::fs::write` を使う sidecar は同じ ENOSPC を
//! ちゃんと `No space left on device` で返すので、 **同じ原因が経路によって 「エラー」 と
//! 「即死」 に分かれていた**。
//!
//! enchudb は 「apparent は巨大、 実データはごく一部」 という sparse 前提の設計なので、
//! 一般的な DB より遥かに踏みやすい (既定 apparent は 24 GB 級。 df に空きがあるように
//! 見えても実際には足りない、 が普通に起こる)。
//!
//! 対策は 2 段:
//!
//! 1. **grow の瞬間に空き容量を見る** — `ftruncate` は ENOSPC を返さないので、 伸ばす前に
//!    `statvfs` で 「commit する分 + margin」 を確認し、 足りなければ `StorageFull` を
//!    **Result として**返す (全域 fallocate は sparse 設計と衝突するので採らない)
//! 2. **その Result を捨てない** — 旧実装は `let _ = ensure_committed(..)` で 12 箇所が
//!    error を捨て、 そのまま未 commit page に書いていた。 伸ばせなければ **書かない**
//!    (`FaultKind::DiskSpace` として計上 + rate-limited warn)
//!
//! ここでは実際にディスクを埋める代わりに `set_space_margin` で 「空きが足りない」 状態を
//! 決定的に作る。 **この fault injection 無しでは、 このバグは 20 GB の loopback を
//! 埋め切らないと踏めない** (issue の再現手順がそれ)。

use enchudb_engine::{Engine, FaultKind, ValueType};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue167-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{p}{suf}"));
    }
    p
}

#[test]
fn full_disk_rejects_writes_instead_of_sigbus() {
    let path = tmp("no-sigbus");
    let mut eng = Engine::create_growable_tiny(&path).expect("create");
    eng.define_himo("n", ValueType::Number, 0);
    eng.define_himo("t", ValueType::Leaf, 0);
    assert!(
        eng.disk_free_bytes().is_some(),
        "premise: growable backing であること (static backing では grow が無い)"
    );

    // ここまでは正常に書けること (前提の確認)
    let warm = eng.entity().expect("枠はある");
    eng.tie(warm, "n", 1);
    eng.tie_text(warm, "t", "before-full");
    assert_eq!(eng.get(warm, "n"), Some(1));
    assert_eq!(eng.get_text(warm, "t"), Some(&b"before-full"[..]));

    // 以降 「空きが足りない」 状態にする (実際のディスクは埋めない)
    eng.set_space_margin(u64::MAX / 2);

    let mut attempted = 0usize;
    let mut eids = Vec::new();
    // 枠 (create_growable_tiny = 1024) を使い切らない範囲に留める — 最後の
    // 「空きが戻れば書ける」 確認で 1 個必要なため。
    for i in 0..500u32 {
        let Ok(e) = eng.entity() else { break };
        eng.tie(e, "n", i);
        eng.tie_text(e, "t", &format!("payload-{i}-{}", "x".repeat(512)));
        eids.push(e);
        attempted += 1;
    }

    // ここに到達している = **SIGBUS していない** (旧実装はここで signal: 10 で即死)
    assert!(attempted > 0, "1 件も試行できていない (前提が壊れている)");
    assert!(
        eng.space_denials() > 0,
        "空き容量不足で grow を拒否した記録が無い (fault injection が効いていない)"
    );
    assert!(
        eng.fault_count(FaultKind::DiskSpace) > 0,
        "write を拒否したのに DiskSpace fault が計上されていない (黙って落としている)"
    );

    // 拒否された write が **壊れた値を残していない** こと。
    // sentinel (u32::MAX) が cell に書かれていれば read 側が 「値なし」 と区別できない。
    for &e in &eids {
        let got = eng.get_text(e, "t");
        if let Some(bytes) = got {
            assert!(
                bytes.starts_with(b"payload-"),
                "拒否された cell に壊れた値が残っている: {:?}",
                &bytes[..bytes.len().min(32)]
            );
        }
    }
    // 満杯前に書いた値は読めたままであること
    assert_eq!(eng.get_text(warm, "t"), Some(&b"before-full"[..]));

    // 空きが戻れば書けること (状態が brick していない = sticky にしていない)
    eng.set_space_margin(0);
    let after = eng.entity().expect("枠はある");
    eng.tie_text(after, "t", "after-recovery");
    assert_eq!(
        eng.get_text(after, "t"),
        Some(&b"after-recovery"[..]),
        "空きが戻ったのに書けない (拒否状態が sticky になっている)"
    );

    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}
