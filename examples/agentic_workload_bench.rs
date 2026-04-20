//! Agentic Engram 型ワークロード: EnchuDB vs SQLite。
//!
//! 100k セッション(speaker/topic/file Ref + decision text)を投入して、
//! 「Alice × Next.js」的な多条件メタフィルタクエリのレイテンシを測る。
//!
//! Kuzu は upstream archived(2026-04-20 現在 brew で deprecated)かつ
//! kuzu 0.11 Rust bindings が arm64 macOS でリンカ失敗するため採用見送り。
//! LanceDB は Arrow 依存が巨大で組み込み用途にはオーバー。
//!
//! cargo run --release --features v27 --example agentic_workload_bench

#[cfg(feature = "v27")]
fn main() {
    use enchudb::{Engine, HimoType};
    use rusqlite::Connection;
    use std::time::Instant;

    const N_SESSIONS: u32 = 100_000;
    const N_SPEAKERS: u32 = 100;
    const N_TOPICS: u32 = 50;
    const N_FILES: u32 = 1000;
    const QUERY_ITERS: u32 = 1000;

    let decision_text = "middleware で早期 return に統一する";

    println!("=== Agentic Engram 型ワークロード ({} sessions) ===\n", N_SESSIONS);

    // ─────── EnchuDB ───────
    let enchu_path = format!("/tmp/agentic_enchu_{}", std::process::id());
    let _ = std::fs::remove_file(&enchu_path);

    let mut eng = Engine::create_with_capacity(&enchu_path, N_SESSIONS + 2000).unwrap();
    eng.define_himo("speaker", HimoType::Ref, N_SPEAKERS);
    eng.define_himo("topic", HimoType::Ref, N_TOPICS);
    eng.define_himo("file", HimoType::Ref, N_FILES);
    eng.define_himo("decision", HimoType::Symbol, 0);
    eng.flush().unwrap();

    let speaker_eids: Vec<u64> = (0..N_SPEAKERS).map(|_| eng.entity()).collect();
    let topic_eids: Vec<u64> = (0..N_TOPICS).map(|_| eng.entity()).collect();
    let file_eids: Vec<u64> = (0..N_FILES).map(|_| eng.entity()).collect();

    println!("── INSERT ──");
    let t0 = Instant::now();
    for i in 0..N_SESSIONS {
        let sess = eng.entity();
        eng.tie_ref(sess, "speaker", speaker_eids[(i % N_SPEAKERS) as usize]);
        eng.tie_ref(sess, "topic", topic_eids[(i % N_TOPICS) as usize]);
        eng.tie_ref(sess, "file", file_eids[(i % N_FILES) as usize]);
        eng.tie_text(sess, "decision", decision_text);
    }
    eng.rebuild();
    eng.flush().unwrap();
    let enchu_insert = t0.elapsed();
    let enchu_size_virt = std::fs::metadata(&enchu_path).unwrap().len();
    let enchu_size_disk = disk_usage(&enchu_path);
    println!("  EnchuDB: {:>7.2} s ({:>10.0} ties/s, virt {} MB / disk {} MB)",
        enchu_insert.as_secs_f64(),
        (N_SESSIONS * 4) as f64 / enchu_insert.as_secs_f64(),
        enchu_size_virt / 1_048_576,
        enchu_size_disk / 1_048_576);

    // Query 1: 多条件 AND
    // N_SPEAKERS(100) と N_TOPICS(50) は gcd=50 なので、
    // i % 100 == 10 の時に i % 50 == 10 も成立 → overlap あり
    let target_speaker_idx = 10u32;
    let target_topic_idx = 10u32;
    let target_speaker = speaker_eids[target_speaker_idx as usize];
    let target_topic = topic_eids[target_topic_idx as usize];

    let t0 = Instant::now();
    let mut hits = 0;
    for _ in 0..QUERY_ITERS {
        let eids = eng.query(&[("speaker", target_speaker), ("topic", target_topic)]);
        hits = eids.len();
    }
    let enchu_q1 = t0.elapsed();
    println!("  Q1 EnchuDB(query): {:>7} ns/op ({} hits)",
        enchu_q1.as_nanos() / QUERY_ITERS as u128, hits);

    // Query 2: query + decision text 全件抽出
    let t0 = Instant::now();
    for _ in 0..QUERY_ITERS {
        let eids = eng.query(&[("speaker", target_speaker), ("topic", target_topic)]);
        for &e in &eids {
            let _ = eng.get_text(e, "decision");
        }
    }
    let enchu_q2 = t0.elapsed();
    println!("  Q2 EnchuDB(query+extract): {:>7} ns/op",
        enchu_q2.as_nanos() / QUERY_ITERS as u128);

    // Query 3: reverse_follow 風(速やかなグラフ辿り、topic 起点でセッション列挙)
    let t0 = Instant::now();
    let mut rev_hits = 0;
    for _ in 0..QUERY_ITERS {
        let sessions = eng.pull_raw("topic", target_topic);
        rev_hits = sessions.len();
    }
    let enchu_q3 = t0.elapsed();
    println!("  Q3 EnchuDB(pull_raw 逆引き): {:>7} ns/op ({} hits)",
        enchu_q3.as_nanos() / QUERY_ITERS as u128, rev_hits);

    // ─────── SQLite ───────
    println!("\n── SQLite ──");
    let sqlite_path = format!("/tmp/agentic_sqlite_{}", std::process::id());
    let _ = std::fs::remove_file(&sqlite_path);
    let conn = Connection::open(&sqlite_path).unwrap();
    conn.execute_batch("
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        CREATE TABLE session (
            id INTEGER PRIMARY KEY,
            speaker INTEGER NOT NULL,
            topic INTEGER NOT NULL,
            file INTEGER NOT NULL,
            decision TEXT NOT NULL
        );
        CREATE INDEX idx_speaker ON session(speaker);
        CREATE INDEX idx_topic ON session(topic);
        CREATE INDEX idx_speaker_topic ON session(speaker, topic);
    ").unwrap();

    let t0 = Instant::now();
    {
        let tx = conn.unchecked_transaction().unwrap();
        let mut stmt = tx.prepare("INSERT INTO session(speaker, topic, file, decision) VALUES (?,?,?,?)").unwrap();
        for i in 0..N_SESSIONS {
            stmt.execute(rusqlite::params![
                i % N_SPEAKERS,
                i % N_TOPICS,
                i % N_FILES,
                decision_text,
            ]).unwrap();
        }
        drop(stmt);
        tx.commit().unwrap();
    }
    let sqlite_insert = t0.elapsed();
    let sqlite_size = std::fs::metadata(&sqlite_path).unwrap().len();
    println!("  SQLite:  {:>7.2} s ({:>10.0} rows/s, file {} MB)",
        sqlite_insert.as_secs_f64(),
        N_SESSIONS as f64 / sqlite_insert.as_secs_f64(),
        sqlite_size / 1_048_576);
    let _sqlite_virt = sqlite_size; let _sqlite_disk = sqlite_size; // SQLite は sparse ではない

    // Query 1: speaker=5 AND topic=10 の count
    let mut q1_stmt = conn.prepare("SELECT COUNT(*) FROM session WHERE speaker = ? AND topic = ?").unwrap();
    let t0 = Instant::now();
    let mut sqlite_hits = 0i64;
    for _ in 0..QUERY_ITERS {
        sqlite_hits = q1_stmt.query_row(rusqlite::params![target_speaker_idx, target_topic_idx], |r| r.get::<_, i64>(0)).unwrap();
    }
    let sqlite_q1 = t0.elapsed();
    println!("  Q1 SQLite(COUNT 多条件): {:>7} ns/op ({} hits)",
        sqlite_q1.as_nanos() / QUERY_ITERS as u128, sqlite_hits);

    // Query 2: speaker=5 AND topic=10 の decision 抽出
    let mut q2_stmt = conn.prepare("SELECT decision FROM session WHERE speaker = ? AND topic = ?").unwrap();
    let t0 = Instant::now();
    for _ in 0..QUERY_ITERS {
        let rows: Vec<String> = q2_stmt
            .query_map(rusqlite::params![target_speaker_idx, target_topic_idx], |r| r.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        std::hint::black_box(rows);
    }
    let sqlite_q2 = t0.elapsed();
    println!("  Q2 SQLite(SELECT decision): {:>7} ns/op",
        sqlite_q2.as_nanos() / QUERY_ITERS as u128);

    // Query 3: topic=10 だけで pull(逆引き相当)
    let mut q3_stmt = conn.prepare("SELECT id FROM session WHERE topic = ?").unwrap();
    let t0 = Instant::now();
    let mut sqlite_rev_hits = 0;
    for _ in 0..QUERY_ITERS {
        let rows: Vec<i64> = q3_stmt
            .query_map(rusqlite::params![target_topic_idx], |r| r.get::<_, i64>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        sqlite_rev_hits = rows.len();
    }
    let sqlite_q3 = t0.elapsed();
    println!("  Q3 SQLite(topic 単一条件): {:>7} ns/op ({} hits)",
        sqlite_q3.as_nanos() / QUERY_ITERS as u128, sqlite_rev_hits);

    drop(q1_stmt); drop(q2_stmt); drop(q3_stmt);
    drop(conn);

    // ─────── 結果まとめ ───────
    println!("\n=== まとめ ===");
    println!("{:<18} {:>14} {:>14} {:>10}", "操作", "EnchuDB", "SQLite", "倍率");
    println!("{}", "-".repeat(60));
    println!("{:<18} {:>12.2} s {:>12.2} s {:>10.1}x",
        "INSERT",
        enchu_insert.as_secs_f64(), sqlite_insert.as_secs_f64(),
        sqlite_insert.as_secs_f64() / enchu_insert.as_secs_f64());
    println!("{:<18} {:>12} ns {:>12} ns {:>10.1}x",
        "Q1: 多条件 AND",
        enchu_q1.as_nanos() / QUERY_ITERS as u128,
        sqlite_q1.as_nanos() / QUERY_ITERS as u128,
        sqlite_q1.as_nanos() as f64 / enchu_q1.as_nanos() as f64);
    println!("{:<18} {:>12} ns {:>12} ns {:>10.1}x",
        "Q2: 多条件+抽出",
        enchu_q2.as_nanos() / QUERY_ITERS as u128,
        sqlite_q2.as_nanos() / QUERY_ITERS as u128,
        sqlite_q2.as_nanos() as f64 / enchu_q2.as_nanos() as f64);
    println!("{:<18} {:>12} ns {:>12} ns {:>10.1}x",
        "Q3: 単一条件逆引き",
        enchu_q3.as_nanos() / QUERY_ITERS as u128,
        sqlite_q3.as_nanos() / QUERY_ITERS as u128,
        sqlite_q3.as_nanos() as f64 / enchu_q3.as_nanos() as f64);
    println!("{:<18} {:>12} MB {:>12} MB  (EnchuDB virt)",
        "FILE SIZE",
        enchu_size_virt / 1_048_576, sqlite_size / 1_048_576);
    println!("{:<18} {:>12} MB {:>12} MB  (EnchuDB disk、sparse)",
        "",
        enchu_size_disk / 1_048_576, sqlite_size / 1_048_576);

    let _ = std::fs::remove_file(&enchu_path);
    let _ = std::fs::remove_file(format!("{}.wal", enchu_path));
    let _ = std::fs::remove_file(&sqlite_path);
    let _ = std::fs::remove_file(format!("{}-wal", sqlite_path));
    let _ = std::fs::remove_file(format!("{}-shm", sqlite_path));
}

#[cfg(feature = "v27")]
fn disk_usage(path: &str) -> u64 {
    // du 経由で sparse 実ディスク消費を取得(macOS / Linux 両対応)
    let output = std::process::Command::new("du")
        .args(["-k", path])
        .output();
    match output {
        Ok(o) => {
            let s = String::from_utf8_lossy(&o.stdout);
            s.split_whitespace()
                .next()
                .and_then(|n| n.parse::<u64>().ok())
                .map(|kb| kb * 1024)
                .unwrap_or(0)
        }
        Err(_) => 0,
    }
}

#[cfg(not(feature = "v27"))]
fn main() {
    println!("Build with --features v27");
}
