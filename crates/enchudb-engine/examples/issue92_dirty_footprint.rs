//! #92 footprint bench — dirty open のたびに vocab index 予約全域を zero-fill +
//! 物理コミットしていた挙動 (#56 ③) の before/after を測る。
//!
//! 症状: `create_growable_with_capacity(cap)` は `vocab_max_entries = cap×16` →
//! `index_cap = next_pow2` → index region ≈ index_cap×13B を確保する。 この region
//! は fixed cluster なので mmap 済みだが **sparse** (物理ブロック未確保)。 ところが
//! dirty (= flush せず落ちた) DB を write open すると `rebuild_index_into` が
//! `for b in &mut xm[INDEX_HEADER..] { *b = 0; }` で **予約全域を dirty 化** し、
//! sparse ページが 1 枚残らず物理化する。 live vocab 数と無関係に index_cap 比例。
//!
//! 計測: clean な DB を作り (flush 済) → write open して clean_flag を 0 に倒し
//! flush せず drop (= crash 相当の dirty 化) → もう一度 write open した前後で
//! 物理ブロック (st_blocks×512) の増分を見る。
//!
//! 実行: `cargo run --release -p enchudb-engine --example issue92_dirty_footprint`

use enchudb_engine::{Engine, ValueType};
use std::os::unix::fs::MetadataExt;

/// live vocab に載せる値の数 (index_cap に対して極小)。
const LIVE_VOCAB: usize = 5;

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// enchu.db の物理サイズ (bytes) = st_blocks × 512。 sparse hole は数えない。
fn physical_bytes(path: &str) -> u64 {
    match std::fs::metadata(path) {
        Ok(m) => m.blocks() * 512,
        Err(_) => 0,
    }
}

fn kib(bytes: u64) -> f64 {
    bytes as f64 / 1024.0
}

/// cap で 1 本回して (clean 物理, dirty-reopen 前, dirty-reopen 後) を返す。
fn run(path: &str, cap: u32) -> (u64, u64, u64) {
    cleanup(path);

    // 1) clean な DB を作る: Tag vocab を少数 seed → flush (clean_flag=1) → drop。
    {
        let mut eng = Engine::create_growable_with_capacity(path, cap).unwrap();
        eng.define_himo("kind", ValueType::Tag, 100);
        for i in 0..LIVE_VOCAB {
            let e = eng.entity();
            eng.tie_text(e, "kind", &format!("kind-{i}"));
        }
        eng.flush().unwrap();
    }
    let clean = physical_bytes(path);

    // 2) write open (clean DB なので rebuild は走らないが、open が clean_flag=0 に
    //    倒して msync する) → flush せず drop = dirty on disk。
    {
        let _eng = Engine::open_standalone(path).unwrap();
        // insert も flush もしない。 drop で dirty のまま残す。
    }
    let before = physical_bytes(path);

    // 3) dirty DB を write open → rebuild_index_into が走る。 ここで bloat。
    {
        let _eng = Engine::open_standalone(path).unwrap();
    }
    let after = physical_bytes(path);

    cleanup(path);
    (clean, before, after)
}

fn main() {
    println!("#92 dirty-open footprint bench — live vocab = {LIVE_VOCAB} 件\n");
    println!("| cap | vocab_max (×16) | clean 物理 | dirty-reopen 前 | dirty-reopen 後 | Δ |");
    println!("|----:|----------------:|----------:|----------------:|----------------:|--:|");
    for &cap in &[256_000u32, 1_000_000, 4_000_000] {
        let (clean, before, after) = run("/tmp/enchu_issue92", cap);
        let vocab_max = (cap as u64).saturating_mul(16);
        println!(
            "| {:>7} | {:>13} | {:>7.0} KB | {:>12.0} KB | {:>12.0} KB | +{:>.0} KB |",
            cap,
            vocab_max,
            kib(clean),
            kib(before),
            kib(after),
            kib(after.saturating_sub(before)),
        );
    }
    println!(
        "\n期待 (after-fix): live vocab {LIVE_VOCAB} 件しか無いので dirty reopen の Δ は\
         index_cap 比例ではなく ~0 (触るのは live slot の載る数ページのみ)。"
    );
}
