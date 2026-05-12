//! 破壊テスト用ヘルパーバイナリ。
//!
//! 指定されたシナリオで DB に書き込む。親プロセスが SIGKILL を撃つことで
//! プロセスクラッシュの挙動を検証できる。
//!
//! Usage:
//!   crash_writer <path> <scenario> [count]
//!
//! scenarios:
//!   normal              — 書き込み → wal_sync → 正常終了
//!   no_commit           — 書き込み → wal_commit せずに exit(0)
//!   no_sync             — 書き込み → wal_commit → wal_sync せずに exit(0)
//!   abort_mid           — 半分書いたところで abort()(Drop 走らず)
//!   loop_writes         — 無限に書き込む(親プロセスが kill する前提)
//!   sleep_after         — 書き込み + sync → sleep(親が kill する前提、耐久化確認)
//!   v32_signed_loop     — v32 署名付きで無限書き込み(親が kill する前提)。
//!                         seed 固定の keypair で WAL に署名レコードを残す。

fn main() {
    use enchudb_engine::Engine;
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: crash_writer <path> <scenario> [count]");
        std::process::exit(2);
    }
    let path = &args[1];
    let scenario = args[2].as_str();
    let count: u32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100);

    // DB はすでに定義済み(呼び出し側が準備している前提)
    let eng = Engine::open_concurrent_with_wal(path, 64 * 1024 * 1024).unwrap();

    match scenario {
        "normal" => {
            for i in 0..count {
                let e = eng.entity();
                eng.tie_async(e, "n", i);
            }
            eng.flush_writes();
            eng.wal_sync().unwrap();
        }
        "no_commit" => {
            for i in 0..count {
                let e = eng.entity();
                eng.tie_async(e, "n", i);
            }
            eng.flush_writes();
            // wal_commit 呼ばずに終了 — auto-commit が効くかのテスト
            std::process::exit(0);
        }
        "no_sync" => {
            for i in 0..count {
                let e = eng.entity();
                eng.tie_async(e, "n", i);
            }
            eng.flush_writes();
            eng.wal_commit();
            // wal_sync 呼ばずに終了 — Drop の shutdown fsync に頼る
            std::process::exit(0);
        }
        "abort_mid" => {
            let half = count / 2;
            for i in 0..half {
                let e = eng.entity();
                eng.tie_async(e, "n", i);
            }
            eng.flush_writes();
            eng.wal_commit();
            eng.wal_sync().unwrap();

            // 前半は確実に耐久化されている
            // ここから後半を書いて、fsync せずに abort
            for i in half..count {
                let e = eng.entity();
                eng.tie_async(e, "n", i);
            }
            // abort()は Drop を走らせずにプロセス終了(SIGABRT 相当)
            std::process::abort();
        }
        "loop_writes" => {
            let mut i = 0u32;
            loop {
                let e = eng.entity();
                eng.tie_async(e, "n", i % 1000);
                i += 1;
                if i % 1000 == 0 {
                    eng.flush_writes();
                    eng.wal_commit();
                    eng.wal_sync().unwrap();
                }
                if i % 100 == 0 {
                    // 親に読ませるため stdout に進捗
                    println!("{}", i);
                }
            }
        }
        "sleep_after" => {
            for i in 0..count {
                let e = eng.entity();
                eng.tie_async(e, "n", i);
            }
            eng.flush_writes();
            eng.wal_commit();
            eng.wal_sync().unwrap();
            println!("SYNCED {}", count);
            // stdin を閉じるまで sleep(親が kill する)
            loop {
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
        }
        "v32_signed_loop" => {
            use enchudb_engine::keys::Keypair;
            // seed 固定で親が pubkey を事前登録できるようにする
            let seed = [7u8; 32];
            let kp = std::sync::Arc::new(Keypair::from_bytes(&seed));
            eng.set_peer_id(1);
            eng.set_keypair(Some(kp));

            let mut i = 0u32;
            loop {
                let e = eng.entity();
                eng.tie_async(e, "n", i % 1000);
                i += 1;
                if i % 500 == 0 {
                    eng.flush_writes();
                    eng.wal_commit();
                    eng.wal_sync().unwrap();
                    println!("{}", i);
                }
            }
        }
        _ => {
            eprintln!("unknown scenario: {}", scenario);
            std::process::exit(2);
        }
    }
}

