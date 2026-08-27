//! .oplog file の header (head / checkpoint / capacity) と committed record 数を
//! 直接ダンプする診断ツール。engine を通さないので、fold (try_reset) や recovery の
//! 副作用なしに「ファイルに今なにが居るか」を見られる。
//!
//! usage: cargo run -p enchudb-oplog --example oplog_dump -- /path/to/db.oplog
//!
//! #204 (SIGKILL test の負荷依存 flake) の切り分けで「kill 時点で ring が既に
//! fold 済み (head == HEADER_SIZE)」を突き止めた道具。

fn main() {
    let path = std::env::args().nth(1).expect("usage: oplog_dump <path.oplog>");
    let bytes = std::fs::read(&path).unwrap();
    let head = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let checkpoint = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let cap = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    eprintln!("file_len={} head={} checkpoint={} cap={}", bytes.len(), head, checkpoint, cap);
    let wal = enchudb_oplog::oplog::OpLog::open(std::path::Path::new(&path)).unwrap();
    eprintln!("iter_committed: {} records", wal.iter_committed().len());
}
