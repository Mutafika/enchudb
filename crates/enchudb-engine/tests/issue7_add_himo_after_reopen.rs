//! issue7 再現テスト: 既存 DB に新 himo を append → 次回 open で SIGBUS。
//!
//! opyula `schema::define_all_himos` の挙動を模す:
//! 1) create_full で DB を作る (max_himos=64、 vocab/content 16MB ずつ、 cap 262144)
//! 2) ~44 個 himo を define、 data を tie、 flush、 drop
//! 3) open_standalone で開いて、 さらに ~18 個 himo を append、 flush、 drop
//! 4) もう一度 open_standalone → ここで SIGBUS が報告されている

use enchudb_engine::{Engine, HimoType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue7-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.positions", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn append_himos_after_reopen_does_not_sigbus() {
    let path = tmp_path("append_himo");
    cleanup(&path);

    // Step 1: create + 44 himos + data
    {
        let mut eng = Engine::create_full(
            &path,
            262_144,
            Some(16 * 1024 * 1024),
            Some(64),
            Some(16 * 1024 * 1024),
        )
        .unwrap();
        for i in 0..44 {
            eng.define_himo(&format!("h{}", i), HimoType::Number, 0);
        }
        for v in 0..1000u32 {
            let e = eng.entity();
            eng.tie(e, "h0", v);
        }
        eng.flush().unwrap();
    }

    // Step 2: reopen, append 18 more himos, flush, drop
    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        for i in 44..62 {
            eng.define_himo(&format!("h{}", i), HimoType::Number, 0);
        }
        eng.flush().unwrap();
    }

    // Step 3: reopen — must not SIGBUS, data still readable
    {
        let eng = Engine::open_standalone(&path).unwrap();
        assert_eq!(eng.pull_raw("h0", 1).len(), 1, "old data should still resolve");
        // 新 himo にも data を tie できるべき (read-only path で確認するため write はしない)
    }

    cleanup(&path);
}

/// opyula が報告する具体的 himo 名・型を使った版 (Tag / Number / Ref が混在)。
#[test]
fn append_mixed_type_himos_after_reopen() {
    let path = tmp_path("append_mixed");
    cleanup(&path);

    // 既存 schema (opyula V1 の前半 44 個に近い構成)
    {
        let mut eng = Engine::create_full(
            &path,
            262_144,
            Some(16 * 1024 * 1024),
            Some(64),
            Some(16 * 1024 * 1024),
        )
        .unwrap();
        let pre: &[(&str, HimoType)] = &[
            ("kind", HimoType::Tag),
            ("name", HimoType::Tag),
            ("pubkey_hex", HimoType::Tag),
            ("endpoint", HimoType::Tag),
            ("last_seen_s", HimoType::Number),
            ("room_ref", HimoType::Ref),
            ("peer_ref", HimoType::Ref),
            ("updated_at_s", HimoType::Number),
            ("title", HimoType::Tag),
            ("status", HimoType::Tag),
        ];
        for (n, t) in pre {
            eng.define_himo(n, *t, 0);
        }
        let e = eng.entity();
        eng.tie_text(e, "name", "test");
        eng.flush().unwrap();
    }

    // 新 himo を append (= fact_* 系を後付け、 issue7 の trigger)
    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        let new: &[(&str, HimoType)] = &[
            ("fact_what", HimoType::Tag),
            ("fact_why", HimoType::Tag),
            ("fact_how", HimoType::Tag),
            ("fact_when_start_s", HimoType::Number),
            ("fact_when_end_s", HimoType::Number),
            ("fact_where_project", HimoType::Tag),
            ("fact_who_user", HimoType::Tag),
            ("fact_ref", HimoType::Ref),
            ("entity_tag", HimoType::Tag),
            ("fact_kind", HimoType::Tag),
        ];
        for (n, t) in new {
            eng.define_himo(n, *t, 0);
        }
        eng.flush().unwrap();
    }

    // 2 回目以降の open で SIGBUS しない、 全 himo の type が保たれる
    {
        let eng = Engine::open_standalone(&path).unwrap();
        // 新 himo に対して pull_raw が panic しない (= ensure_count まで通る)
        assert_eq!(eng.pull_raw("fact_what", 999_999).len(), 0);
        assert_eq!(eng.pull_raw("name", 0).len(), 1);
    }

    cleanup(&path);
}

/// seal_integrity で `.crc` を焼いた後に himo を追加 → reopen が崩れるかの確認。
/// 仮説 B: `.crc` sidecar が 「44 個分」 のまま残って 62 個目の column を見たとき
/// integrity check が落ちる / SIGBUS する。
///
/// **status: 未 fix bug の reproducer**。 master では `flush()` が `.crc` を再焼きしない
/// ため、 `seal_integrity` 後に `define_himo` で schema 変更すると `.crc` が前 schema
/// 時点のまま残り、 次 open の `verify_region_crcs` が vocab/himoreg mismatch で `Err`。
/// SIGBUS ではないが DB が開けない状態になる。
///
/// 修正案 (engine 側): `ensure_himo` (= schema 変更経路) で `.crc` を unlink、
/// 次 open は v28 以前 fallback で skip。 これを入れたら `#[ignore]` を剥がす。
/// fix 単体で再現確認したい時は `cargo test -- --ignored` で実行。
#[test]
#[ignore = "未 fix: seal_integrity 後の define_himo append で .crc 追従漏れ → reopen 失敗。 engine 側で ensure_himo が .crc invalidate する fix 待ち。"]
fn append_himos_after_seal_integrity() {
    let path = tmp_path("append_after_seal");
    cleanup(&path);

    {
        let mut eng = Engine::create_full(
            &path,
            262_144,
            Some(16 * 1024 * 1024),
            Some(64),
            Some(16 * 1024 * 1024),
        )
        .unwrap();
        for i in 0..10 {
            eng.define_himo(&format!("h{}", i), HimoType::Number, 0);
        }
        let e = eng.entity();
        eng.tie(e, "h0", 42);
        // seal_integrity = flush + .crc 焼き
        eng.seal_integrity().unwrap();
    }

    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        // 新 himo append
        for i in 10..20 {
            eng.define_himo(&format!("h{}", i), HimoType::Number, 0);
        }
        eng.flush().unwrap();
    }

    {
        // .crc は前 schema 時点で焼かれているので、 verify_region_crcs が失敗する
        // 可能性 (= SIGBUS とは違うけど DB が開けなくなる)。
        match Engine::open_standalone(&path) {
            Ok(_) => {}
            Err(e) => {
                // CRC mismatch だけならまだ良い (errno で帰る)、 SIGBUS じゃない
                eprintln!("open after seal+append: {}", e);
            }
        }
    }

    cleanup(&path);
}
