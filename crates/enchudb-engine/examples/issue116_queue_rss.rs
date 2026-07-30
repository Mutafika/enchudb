//! #116 footprint bench — concurrent open ごとに `oplog_record_queue`
//! (`ArrayQueue<OwnedOp>`) を `DEFAULT_WRITE_QUEUE_CAP = 1_048_576` slot 決め打ちで
//! 確保しているため、 中身が空の DB でも per-open ~80MB RSS を固定予約する問題の
//! 再現・計測。
//!
//! **Linux 専用**: RSS は `/proc/self/status` の VmRSS から読む。 macOS/APFS では
//! sparse 予約が phys へ inflate するので判定は必ず Linux (OrbStack container) で行う。
//!
//! 実行 (OrbStack / rust container 内):
//!   MODE=arrayqueue cargo run --release -p enchudb-engine --example issue116_queue_rss
//!   MODE=open       cargo run --release -p enchudb-engine --example issue116_queue_rss
//!   MODE=open QUEUE_CAP=16384 cargo run --release ...   # override 経路 (fix 相当)
//!   MODE=multiopen  N=10 cargo run --release ...        # --memory=1g で OOM 再現
//!
//! env:
//!   MODE       arrayqueue | open | multiopen   (default: arrayqueue)
//!   QUEUE_CAP  oplog_record_queue の slot 数 override (未指定 = default 1M)
//!   OPLOG_MB   oplog WAL バッファの MiB (default 8) — RSS 不感の対照用
//!   N          multiopen で同時に開く DB 数 (default 10)

use enchudb_engine::Engine;
use enchudb_oplog::oplog::OwnedOp;

/// `/proc/self/status` の VmRSS を KiB で返す (Linux 専用)。
fn vm_rss_kib() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            if let Some(num) = rest.split_whitespace().next() {
                return num.parse().unwrap_or(0);
            }
        }
    }
    0
}

fn mib(kib: u64) -> f64 {
    kib as f64 / 1024.0
}

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".wal"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// MODE=arrayqueue — issue の切り分け #2 を直接再現。
/// `ArrayQueue::<OwnedOp>::new(cap)` を単体で確保して RSS Δ を測る。
/// `ArrayQueue::new` は生成時に全 slot を commit するので、 cap に比例して RSS が張り付く。
fn mode_arrayqueue() {
    use crossbeam_queue::ArrayQueue;
    let slot = std::mem::size_of::<OwnedOp>();
    println!("MODE=arrayqueue — ArrayQueue::<OwnedOp>::new(cap) 単体の RSS Δ");
    println!("size_of::<OwnedOp>() = {slot} B/slot\n");
    println!("| cap        | 理論 (cap×slot) | RSS Δ (実測) |");
    println!("|-----------:|----------------:|-------------:|");
    for &cap in &[4_096usize, 16_384, 65_536, 262_144, 1_048_576] {
        // 前の確保を drop しきってから測る。
        let base = vm_rss_kib();
        let q = ArrayQueue::<OwnedOp>::new(cap);
        // 触らなくても new が commit 済みだが、 念のため 1 push して物理化を保証。
        let _ = q.push(OwnedOp::Commit);
        let after = vm_rss_kib();
        let theory_kib = (cap * slot) as u64 / 1024;
        println!(
            "| {:>10} | {:>12.1} MB | {:>9.1} MB |",
            cap,
            mib(theory_kib),
            mib(after.saturating_sub(base)),
        );
        drop(q);
    }
    println!(
        "\n→ cap=1,048,576 (= DEFAULT_WRITE_QUEUE_CAP) で ~80MB。 issue #116 の支配項。"
    );
}

/// MODE=open — concurrent open 直後の RSS を測る。
/// QUEUE_CAP 未指定なら `create_concurrent_with_oplog` (= 内部 default 1M slot)、
/// 指定時は `create_concurrent_with_oplog_queue_cap` (issue4 の既存 override 経路)。
/// この override は schema `finish_with_oplog` からは呼べない (= #116 提案1)。
fn mode_open() {
    let oplog_mb = env_usize("OPLOG_MB").unwrap_or(8);
    let oplog_cap = oplog_mb * 1024 * 1024;
    let queue_cap = env_usize("QUEUE_CAP");
    let path = "/tmp/enchu_issue116_open.db";
    cleanup(path);

    let base = vm_rss_kib();
    let eng = match queue_cap {
        Some(qc) => {
            println!(
                "open: create_concurrent_with_oplog_queue_cap(oplog={oplog_mb}MB, queue_cap={qc})"
            );
            Engine::create_concurrent_with_oplog_queue_cap(path, oplog_cap, qc).unwrap()
        }
        None => {
            println!(
                "open: create_concurrent_with_oplog(oplog={oplog_mb}MB, queue_cap=DEFAULT 1M)"
            );
            Engine::create_concurrent_with_oplog(path, oplog_cap).unwrap()
        }
    };
    let after = vm_rss_kib();
    // 中身は空 (0 entity)。 keep-alive のため使う。
    let _ = eng.entity_count();

    println!("  VmRSS: {:.1} MB → {:.1} MB (Δ {:.1} MB)",
        mib(base), mib(after), mib(after.saturating_sub(base)));
    println!("  oplog_capacity={oplog_mb}MB を振っても Δ は不感 (queue slot 数とは別物)。");
    drop(eng);
    cleanup(path);
}

/// MODE=multiopen — per-tenant=1 DB を N 個同時に open し、 全 Arc を保持したまま
/// RSS の線形成長を測る。 `docker run --memory=1g` 下では N を上げると OOM kill。
fn mode_multiopen() {
    let n = env_usize("N").unwrap_or(10);
    let oplog_cap = env_usize("OPLOG_MB").unwrap_or(8) * 1024 * 1024;
    let queue_cap = env_usize("QUEUE_CAP");
    println!(
        "MODE=multiopen — {n} DB を同時 open (queue_cap={})",
        queue_cap.map_or("DEFAULT 1M".to_string(), |q| q.to_string())
    );
    println!("| opened | VmRSS      | per-DB Δ |");
    println!("|-------:|-----------:|---------:|");

    let base = vm_rss_kib();
    let mut held = Vec::with_capacity(n);
    let mut prev = base;
    for i in 0..n {
        let path = format!("/tmp/enchu_issue116_multi_{i}.db");
        cleanup(&path);
        let eng = match queue_cap {
            Some(qc) => Engine::create_concurrent_with_oplog_queue_cap(&path, oplog_cap, qc).unwrap(),
            None => Engine::create_concurrent_with_oplog(&path, oplog_cap).unwrap(),
        };
        held.push((path, eng));
        let now = vm_rss_kib();
        println!(
            "| {:>6} | {:>7.1} MB | {:>5.1} MB |",
            i + 1,
            mib(now),
            mib(now.saturating_sub(prev)),
        );
        prev = now;
    }
    let total = vm_rss_kib();
    println!(
        "\n合計 Δ {:.1} MB / {} DB = {:.1} MB/DB (base {:.1} MB)",
        mib(total.saturating_sub(base)),
        n,
        mib(total.saturating_sub(base)) / n as f64,
        mib(base),
    );
    // cleanup (Arc を先に drop)。
    for (path, eng) in held {
        drop(eng);
        cleanup(&path);
    }
}

fn main() {
    let mode = std::env::var("MODE").unwrap_or_else(|_| "arrayqueue".to_string());
    println!("#116 queue RSS bench — mode={mode}  (VmRSS from /proc/self/status)\n");
    match mode.as_str() {
        "arrayqueue" => mode_arrayqueue(),
        "open" => mode_open(),
        "multiopen" => mode_multiopen(),
        other => {
            eprintln!("unknown MODE={other} (arrayqueue | open | multiopen)");
            std::process::exit(2);
        }
    }
}
