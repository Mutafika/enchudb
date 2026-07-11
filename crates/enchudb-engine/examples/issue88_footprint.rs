//! #88 footprint bench — Leaf を vocab に載せる旧挙動 (before) と LeafStore に
//! 載せる v6 (after) で、 rolling retention 下の footprint を比較する。
//!
//! wikipulse 型の workload を模す:
//!   round 毎に 1 entity 作成 + content(Leaf) を tie、 `WINDOW` round 前の entity を
//!   delete (= retention)。 live 集合は常に ~WINDOW 件。
//!
//! - before (leaf region 無し = v5): content は vocab に単調 append。 delete は
//!   vocab を回収しない → footprint は round 数に比例して増え続ける。
//! - after (v6): content は LeafStore へ。 delete で slot を free → 再利用で
//!   footprint は live 集合 (~WINDOW) 分で有界。
//!
//! 実行: `cargo run --release -p enchudb-engine --example issue88_footprint`

use enchudb_engine::{Engine, ValueType};

const ROUNDS: usize = 20_000;
const WINDOW: usize = 200;
const CONTENT_LEN: usize = 1024;

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn content_for(i: usize) -> String {
    // 固定長・round 毎に unique (Leaf は dedup しないので現実的に unique 化)。
    let mut s = format!("event-{i:010}-");
    s.push_str(&"x".repeat(CONTENT_LEN.saturating_sub(s.len())));
    s.truncate(CONTENT_LEN);
    s
}

/// workload を回して checkpoint 毎の footprint (bytes) を返す。
/// `leaf_size = Some(0)` は before (leaf region 無し → vocab footprint を計測)。
fn run(path: &str, leaf_size: Option<usize>) -> Vec<(usize, u64)> {
    cleanup(path);
    let mut eng = Engine::create_full_with_leaf(
        path,
        (ROUNDS + WINDOW + 16) as u32,
        Some(64 * 1024 * 1024), // vocab_data: before の単調 append を吸収
        Some(16),               // max_himos
        Some(64 * 1024),        // content_data (未使用)
        None,                   // cyl_max_values
        leaf_size,
    )
    .unwrap();
    eng.define_himo("content", ValueType::Leaf, 0);

    let is_after = leaf_size != Some(0);
    let mut curve = Vec::new();
    for i in 0..ROUNDS {
        let e = eng.entity();
        eng.tie_text(e, "content", &content_for(i));
        if i >= WINDOW {
            eng.delete((i - WINDOW) as u64); // retention: WINDOW 前を削除
        }
        if (i + 1) % 4_000 == 0 || i + 1 == ROUNDS {
            let fp: u64 = if is_after {
                eng.leaf_footprint().unwrap()
            } else {
                eng.vocab_data_footprint() as u64
            };
            curve.push((i + 1, fp));
        }
    }
    drop(eng);
    cleanup(path);
    curve
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    println!(
        "#88 footprint bench — ROUNDS={ROUNDS}, WINDOW={WINDOW}, content={CONTENT_LEN}B/event\n"
    );
    let before = run("/tmp/enchu_issue88_before", Some(0));
    let after = run("/tmp/enchu_issue88_after", Some(64 * 1024 * 1024));

    println!("| rounds | before (vocab, 回収なし) | after (LeafStore, reclaim) |");
    println!("|-------:|-------------------------:|---------------------------:|");
    for (&(r, b), &(_, a)) in before.iter().zip(after.iter()) {
        println!(
            "| {:>6} | {:>10} B ({:>7.2} MiB) | {:>8} B ({:>6.2} MiB) |",
            r, b, mib(b), a, mib(a)
        );
    }

    let (rounds, b_final) = *before.last().unwrap();
    let (_, a_final) = *after.last().unwrap();
    let live_bytes = (WINDOW * CONTENT_LEN) as f64;
    println!(
        "\nsummary @ {rounds} rounds: before {:.2} MiB → after {:.2} MiB ({:.0}x 削減)",
        mib(b_final),
        mib(a_final),
        b_final as f64 / a_final as f64,
    );
    println!(
        "after footprint は live 集合 ~{WINDOW}×{CONTENT_LEN}B = {:.2} MiB 相当で有界",
        live_bytes / (1024.0 * 1024.0),
    );
}
