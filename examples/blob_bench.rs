//! BlobStore の throughput / latency 実測。
//!
//! 実行: `cargo run --release --example blob_bench`

use enchudb::blob_store::{BlobId, BlobStore, LocalBlobStore};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() {
    let root = std::env::temp_dir().join(format!(
        "enchu_blob_bench_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = Arc::new(LocalBlobStore::new(&root).unwrap());

    println!("=== BlobStore micro-bench ===");
    println!("root: {}", root.display());
    println!();

    // 小 blob 単発 put (1KB × 10000)
    {
        let data = vec![0x41u8; 1024];
        let n = 10_000;
        let mut ids = Vec::with_capacity(n);
        let t0 = Instant::now();
        for i in 0..n {
            // ユニーク化のため先頭 4B を counter に
            let mut d = data.clone();
            d[..4].copy_from_slice(&(i as u32).to_le_bytes());
            ids.push(store.put(&d).unwrap());
        }
        let dt = t0.elapsed();
        println!(
            "put 1KB × {:>6} (unique): {:>8.2}ms total, {:>6.2}µs/op, {:>8.0} ops/sec",
            n,
            dt.as_secs_f64() * 1000.0,
            dt.as_micros() as f64 / n as f64,
            n as f64 / dt.as_secs_f64()
        );

        // get
        let t0 = Instant::now();
        for id in &ids {
            let _ = store.get(id).unwrap();
        }
        let dt = t0.elapsed();
        println!(
            "get 1KB × {:>6} (cold) : {:>8.2}ms total, {:>6.2}µs/op, {:>8.0} ops/sec",
            n,
            dt.as_secs_f64() * 1000.0,
            dt.as_micros() as f64 / n as f64,
            n as f64 / dt.as_secs_f64()
        );

        // dedup hit: 同じ blob を再 put
        let t0 = Instant::now();
        for i in 0..n {
            let mut d = data.clone();
            d[..4].copy_from_slice(&(i as u32).to_le_bytes());
            let id = store.put(&d).unwrap();
            debug_assert_eq!(id, ids[i]);
        }
        let dt = t0.elapsed();
        println!(
            "put 1KB × {:>6} (dedup) : {:>8.2}ms total, {:>6.2}µs/op, {:>8.0} ops/sec  (exists → skip)",
            n,
            dt.as_secs_f64() * 1000.0,
            dt.as_micros() as f64 / n as f64,
            n as f64 / dt.as_secs_f64()
        );
    }

    println!();

    // 大 blob 単発 (10MB × 10)
    {
        let n = 10;
        let size = 10 * 1024 * 1024;
        let mut data = vec![0u8; size];
        // 中身をランダム化(dedup 回避)
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i as u32).wrapping_mul(2654435761) as u8;
        }
        let mut ids = Vec::with_capacity(n);
        let t0 = Instant::now();
        for i in 0..n {
            data[0..4].copy_from_slice(&(i as u32).to_le_bytes());
            ids.push(store.put(&data).unwrap());
        }
        let dt = t0.elapsed();
        let mbps = (n * size) as f64 / dt.as_secs_f64() / 1024.0 / 1024.0;
        println!(
            "put 10MB × {:>4}         : {:>8.2}ms total, {:>6.2}ms/op, {:>8.1} MB/s",
            n,
            dt.as_secs_f64() * 1000.0,
            dt.as_millis() as f64 / n as f64,
            mbps
        );

        let t0 = Instant::now();
        for id in &ids {
            let _ = store.get(id).unwrap();
        }
        let dt = t0.elapsed();
        let mbps = (n * size) as f64 / dt.as_secs_f64() / 1024.0 / 1024.0;
        println!(
            "get 10MB × {:>4}         : {:>8.2}ms total, {:>6.2}ms/op, {:>8.1} MB/s  (sha-256 再検証込)",
            n,
            dt.as_secs_f64() * 1000.0,
            dt.as_millis() as f64 / n as f64,
            mbps
        );
    }

    println!();

    // 並行 put (8 スレッド × 1000 blob = 8000 ops)
    {
        let threads = 8;
        let per_thread = 1000;
        let data_size = 4096;
        let t0 = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|tid| {
                let s = store.clone();
                thread::spawn(move || {
                    for i in 0..per_thread {
                        let mut d = vec![0xCCu8; data_size];
                        d[..4].copy_from_slice(&((tid * per_thread + i) as u32).to_le_bytes());
                        let _ = s.put(&d).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let dt = t0.elapsed();
        let total_ops = threads * per_thread;
        println!(
            "put 4KB × {} thr × {} = {:>6}: {:>8.2}ms total, {:>6.2}µs/op, {:>8.0} ops/sec",
            threads,
            per_thread,
            total_ops,
            dt.as_secs_f64() * 1000.0,
            dt.as_micros() as f64 / total_ops as f64,
            total_ops as f64 / dt.as_secs_f64()
        );
    }

    println!();

    // hash 計算のみ(I/O 除外、純 sha-256 速度)
    {
        let size = 10 * 1024 * 1024;
        let data = vec![0x55u8; size];
        let t0 = Instant::now();
        let _id = BlobId::from_bytes(&data);
        let dt = t0.elapsed();
        let mbps = size as f64 / dt.as_secs_f64() / 1024.0 / 1024.0;
        println!(
            "sha-256  10MB           : {:>8.2}ms total, {:>8.1} MB/s  (純ハッシュ、I/O 除外)",
            dt.as_secs_f64() * 1000.0,
            mbps
        );
    }

    // 後片付け
    let _ = std::fs::remove_dir_all(&root);
    println!();
    println!("cleanup: removed {}", root.display());
}
