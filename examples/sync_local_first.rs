//! 0.7.0 pattern C: local-first (privacy-first app、 Ink & Switch 系)。
//! 各 device が 1 DB、 server は blob CDN + signaling relay 役のみ、 metadata
//! は peer-of-peer で `_sync_ops` 経由 publish する。
//!
//! 1 process で alice の 2 device (phone + laptop) を模擬し、 phone 側で
//! `_sync_ops` が populate されて lsn が前進することを示す。 実 wire 経路は
//! transport crate で組む。
//!
//! 実運用では:
//! - peer (device): `~/Documents/myapp/local.ecdb`
//! - sync transport: WebRTC datachannel + signaling relay
//! - blob store: S3 / CDN (= 大 content は別 storage、 metadata だけ sync)
//!
//! 起動: `cargo run --example sync_local_first`

use enchudb::schema::Database;
use std::sync::Arc;

fn open_or_create_local_db(device: &str)
    -> Result<Arc<enchudb::schema::Database>, Box<dyn std::error::Error>>
{
    let path = format!("/tmp/enchudb-pattern-c-{}-{}.ecdb", device, std::process::id());
    let _ = std::fs::remove_file(&path);
    for suf in &[".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }

    let mut db = Database::create(&path)?;
    let _ = db.table("notes")
        .number("id")
        .tag("title")
        .tag("blob_url")  // 大 blob は CDN、 URL だけ DB に
        .number("size_bytes")
        .number("created_s")
        .primary_key("id")
        .build()?;
    db.enable_sync()?;
    // finish_with_oplog で concurrent mode に遷移 → tie が oplog に流れる
    let arc_db = db.finish_with_oplog(16 * 1024 * 1024)?;
    Ok(arc_db)
}

fn cleanup(device: &str) {
    let path = format!("/tmp/enchudb-pattern-c-{}-{}.ecdb", device, std::process::id());
    let _ = std::fs::remove_file(&path);
    for suf in &[".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let phone_db = open_or_create_local_db("phone")?;
    let laptop_db = open_or_create_local_db("laptop")?;

    // phone から note を作成 (= 大 blob は server CDN にアップロード済み想定)。
    // metadata だけ DB に。
    {
        let notes = phone_db.get_table("notes").unwrap();
        notes.insert()
            .set("id", 1i64)
            .set("title", "vacation photos")
            .set("blob_url", "https://cdn.myapp.io/blob/a1b2c3.bin")
            .set("size_bytes", 12_345_678i64)
            .set("created_s", 1716200000i64)
            .commit()?;
        notes.insert()
            .set("id", 2i64)
            .set("title", "shopping list")
            .set("blob_url", "https://cdn.myapp.io/blob/d4e5f6.bin")
            .set("size_bytes", 256i64)
            .set("created_s", 1716200100i64)
            .commit()?;
    }

    println!("phone created 2 notes");

    // peer sync 経路:
    // 1. phone: transfer_oplog_to_sync_ops で `_sync_ops` を populate
    // 2. phone → relay: pending_sync_ops で payload bytes を取り出して送信
    // 3. relay → laptop: 受信した payload を apply_records で取り込む
    // 4. laptop: ack で phone に「ここまで取った」 と返す → phone reclaim
    phone_db.engine().oplog_commit();
    phone_db.engine().flush_writes();
    phone_db.engine().oplog_sync()?;
    let transferred = phone_db.engine().transfer_oplog_to_sync_ops();
    let phone_lsn = phone_db.engine().current_sync_lsn();
    println!("phone _sync_ops populated: transferred={transferred}, lsn={phone_lsn}");

    let pending = phone_db.engine().pending_sync_ops(0);
    println!("phone pending payloads: {} packet(s)", pending.len());

    // 実 peer-of-peer transport は WebRTC datachannel 等で wire する。 ここでは
    // payload bytes を laptop に渡すところは省略 (= transport crate の責務)。

    let notes = phone_db.get_table("notes").unwrap();
    for &eid in &notes.all().find()? {
        let r = notes.entity(eid);
        let title = r.get("title").unwrap();
        let url = r.get("blob_url").unwrap();
        let size = r.get("size_bytes").unwrap();
        println!("phone note: title={title:?}, blob={url:?}, size={size:?}");
    }

    // laptop は schema だけ、 初期 sync 前
    let laptop_notes = laptop_db.get_table("notes").unwrap();
    println!("laptop has {} notes initially (= awaiting initial sync)",
             laptop_notes.all().count()?);
    println!("laptop sync_lsn: {} (= still at zero)",
             laptop_db.engine().current_sync_lsn());

    drop(phone_db);
    drop(laptop_db);
    cleanup("phone");
    cleanup("laptop");
    Ok(())
}
