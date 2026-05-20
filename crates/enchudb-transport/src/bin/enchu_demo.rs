//! enchu-demo — 2-terminal localhost 分散 DB デモ。
//!
//! # 使い方
//!
//! ```bash
//! # ターミナル 1: origin を起動
//! cargo run --features v32 --bin enchu_demo -- origin --db /tmp/origin.db --port 8080
//!
//! # ターミナル 2: 初期 schema 定義 + 初期データ (origin が loop 中でもできる)
//! cargo run --features v32 --bin enchu_demo -- schema --db /tmp/origin.db --himo val
//!
//! # ターミナル 3: publish (replica が見えるように)
//! cargo run --features v32 --bin enchu_demo -- publish \
//!     --origin http://127.0.0.1:8080 --peer 1 --eid 1 --himo-id 0 --value 42 --hlc-wall 1000
//!
//! # ターミナル 4: replica を起動 (bootstrap + 継続 sync)
//! cargo run --features v32 --bin enchu_demo -- replica \
//!     --origin http://127.0.0.1:8080 --db /tmp/replica.db --peer 9
//!
//! # 値を読む
//! cargo run --features v32 --bin enchu_demo -- read --db /tmp/replica.db --eid 1 --himo val
//! ```


use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use enchudb::{Engine, HimoType};
use enchudb_oplog::Hlc;
use enchudb::sync::Syncer;
use enchudb::transport::{Transport, WireRecord};
use enchudb_transport::http::{HttpRelay, HttpTransport};
use enchudb_oplog::oplog::DecodedOp;

fn main() {
    let mut args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }
    let cmd = args.remove(1);
    let opts = parse_opts(&args[1..]);

    match cmd.as_str() {
        "origin" => cmd_origin(&opts),
        "replica" => cmd_replica(&opts),
        "publish" => cmd_publish(&opts),
        "read" => cmd_read(&opts),
        "schema" => cmd_schema(&opts),
        "help" | "--help" | "-h" => print_usage(),
        other => {
            eprintln!("unknown subcommand: {}", other);
            print_usage();
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    println!("enchu-demo — 分散 enchudb のミニデモ");
    println!();
    println!("SUBCOMMANDS:");
    println!("  origin   --db PATH --port N");
    println!("  replica  --origin URL --db PATH [--peer N] [--interval-ms 500]");
    println!("  publish  --origin URL --peer N --eid N --himo-id N --value N [--hlc-wall N]");
    println!("  read     --db PATH --eid N --himo NAME");
    println!("  schema   --db PATH --himo NAME [--type value|symbol|ref] [--max N]");
    println!();
    println!("Start with `origin`, then `schema` on the db to define himos before publishing.");
}

fn parse_opts(args: &[String]) -> HashMap<String, String> {
    let mut m = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        let key = &args[i];
        if key.starts_with("--") {
            let k = key[2..].to_string();
            if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                m.insert(k, args[i + 1].clone());
                i += 2;
            } else {
                m.insert(k, "1".to_string());
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    m
}

fn req<T: std::str::FromStr>(opts: &HashMap<String, String>, key: &str) -> T
where <T as std::str::FromStr>::Err: std::fmt::Debug {
    let v = opts.get(key).unwrap_or_else(|| {
        eprintln!("missing required --{}", key);
        std::process::exit(1);
    });
    v.parse::<T>().unwrap_or_else(|e| {
        eprintln!("invalid --{}={}: {:?}", key, v, e);
        std::process::exit(1);
    })
}

fn opt<T: std::str::FromStr>(opts: &HashMap<String, String>, key: &str) -> Option<T> {
    opts.get(key).and_then(|v| v.parse::<T>().ok())
}

// ─────────────────────────────────────────────────────────────
// origin: Engine + HttpRelay with bootstrap
// ─────────────────────────────────────────────────────────────
fn cmd_origin(opts: &HashMap<String, String>) {
    let db: String = req(opts, "db");
    let port: u16 = opt(opts, "port").unwrap_or(8080);

    // DB ファイルが無ければ作成、あれば open
    if !std::path::Path::new(&db).exists() {
        println!("[origin] creating new DB at {}", db);
        let mut eng = Engine::create_compact(&db).expect("create_compact");
        eng.flush().expect("flush");
        drop(eng);
    }

    let bind = format!("127.0.0.1:{}", port);
    println!("[origin] starting relay + bootstrap server on {}", bind);
    println!("[origin] db = {}", db);

    let relay = HttpRelay::start_with_bootstrap(&bind, &db)
        .unwrap_or_else(|e| {
            eprintln!("[origin] failed to start relay: {}", e);
            std::process::exit(1);
        });

    println!("[origin] listening on http://{}", relay.addr());
    println!("[origin] endpoints:");
    println!("           GET  /pull?from=&wall=&logical=&peer=");
    println!("           POST /publish?peer=");
    println!("           GET  /bootstrap");
    println!("[origin] running. Ctrl-C to stop.");

    // 終了しないで生存ループ。SIGINT で落ちるまで。
    loop {
        std::thread::sleep(Duration::from_secs(10));
        let count = relay.record_count();
        println!("[origin] alive. relay records = {}", count);
    }
}

// ─────────────────────────────────────────────────────────────
// replica: bootstrap + pull loop
// ─────────────────────────────────────────────────────────────
fn cmd_replica(opts: &HashMap<String, String>) {
    let origin_url: String = req(opts, "origin");
    let db: String = req(opts, "db");
    let peer_id: u32 = opt(opts, "peer").unwrap_or(9);
    let interval_ms: u64 = opt(opts, "interval-ms").unwrap_or(500);
    // どの peer から pull するか。デフォルトは 1 (origin peer)。
    let pull_from: u32 = opt(opts, "pull-from").unwrap_or(1);

    // bootstrap: origin から DB を download
    if std::path::Path::new(&db).exists() {
        println!("[replica] removing existing {}", db);
        let _ = std::fs::remove_file(&db);
        let _ = std::fs::remove_file(format!("{}.oplog", db));
        let _ = std::fs::remove_file(format!("{}.crc", db));
    }

    println!("[replica] bootstrapping from {}", origin_url);
    let t0 = Instant::now();
    let client = HttpTransport::new(origin_url.clone());
    let initial_hlc = client.bootstrap_to(&db).unwrap_or_else(|e| {
        eprintln!("[replica] bootstrap failed: {}", e);
        std::process::exit(1);
    });
    let bootstrap_time = t0.elapsed();
    let db_size = std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0);
    println!("[replica] bootstrapped in {:?} (file size {}MB, snapshot HLC {:?})",
        bootstrap_time, db_size / (1 << 20), initial_hlc);

    // replica として open
    let eng = Engine::open_replica(&db).unwrap_or_else(|e| {
        eprintln!("[replica] open_replica failed: {}", e);
        std::process::exit(1);
    });
    let eng = Arc::new(eng);
    eng.set_peer_id(peer_id);
    println!("[replica] opened as replica, peer_id={}", peer_id);

    // pull loop
    let transport: Arc<dyn Transport> = Arc::new(HttpTransport::new(origin_url));
    let syncer = Syncer::new(eng.clone(), transport);

    println!("[replica] starting pull loop (interval {}ms, pull-from peer {})",
        interval_ms, pull_from);
    loop {
        let out = syncer.pull_once(pull_from);
        if out.received > 0 || out.applied > 0 {
            println!("[replica] pulled: received={} applied={} skipped={} rejected_acl={} rejected_sig={}",
                out.received, out.applied, out.skipped, out.rejected_acl, out.rejected_signature);
        }
        std::thread::sleep(Duration::from_millis(interval_ms));
    }
}

// ─────────────────────────────────────────────────────────────
// publish: 手動で WireRecord を 1 件 POST
// ─────────────────────────────────────────────────────────────
fn cmd_publish(opts: &HashMap<String, String>) {
    let origin_url: String = req(opts, "origin");
    let peer: u32 = req(opts, "peer");
    let eid: u64 = req(opts, "eid");
    let himo_id: u16 = req(opts, "himo-id");
    let value: u32 = req(opts, "value");
    let hlc_wall: u64 = opt(opts, "hlc-wall").unwrap_or_else(now_millis);

    let rec = WireRecord::unsigned(
        Hlc { wall: hlc_wall, logical: 0, peer },
        peer,
        DecodedOp::Tie { eid, himo_id, value },
    );

    let client = HttpTransport::new(origin_url);
    client.publish(peer, vec![rec]);

    println!("[publish] ok: peer={} eid={} himo_id={} value={} hlc.wall={}",
        peer, eid, himo_id, value, hlc_wall);
}

// ─────────────────────────────────────────────────────────────
// read: Engine 直接読み (replica mode で open、write しない)
// ─────────────────────────────────────────────────────────────
fn cmd_read(opts: &HashMap<String, String>) {
    let db: String = req(opts, "db");
    let eid: u64 = req(opts, "eid");
    let himo: String = req(opts, "himo");

    let eng = Engine::open_replica(&db).unwrap_or_else(|e| {
        eprintln!("[read] open_replica failed: {}", e);
        std::process::exit(1);
    });

    match eng.get(eid, &himo) {
        Some(v) => println!("{}", v),
        None => {
            println!("(none)");
            std::process::exit(2);
        }
    }
}

// ─────────────────────────────────────────────────────────────
// schema: define_himo を実行して DB に保存
// ─────────────────────────────────────────────────────────────
fn cmd_schema(opts: &HashMap<String, String>) {
    let db: String = req(opts, "db");
    let himo: String = req(opts, "himo");
    let type_str: String = opt(opts, "type").unwrap_or_else(|| "value".to_string());
    let max: u32 = opt(opts, "max").unwrap_or(100);

    let ht = match type_str.as_str() {
        // 新名 (推奨)
        "number" => HimoType::Number,
        "tag" => HimoType::Tag,
        "leaf" => HimoType::Leaf,
        "ref" => HimoType::Ref,
        // 旧名エイリアス (後方互換)
        "value" => HimoType::Number,
        "symbol" => HimoType::Tag,
        other => {
            eprintln!("invalid --type={}, expected number|tag|leaf|ref (legacy: value|symbol)", other);
            std::process::exit(1);
        }
    };

    let mut eng = if std::path::Path::new(&db).exists() {
        Engine::open_standalone(&db).expect("open")
    } else {
        Engine::create_compact(&db).expect("create_compact")
    };
    eng.define_himo(&himo, ht, max);
    let himo_id = eng.himo_id(&himo).unwrap();
    eng.flush().unwrap();
    println!("[schema] defined '{}' as {:?} (max={}), himo_id={}", himo, ht, max, himo_id);
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
