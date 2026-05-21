//! 0.7.0 pattern C: local-first (privacy-first app、 Ink & Switch 系)。
//! 各 device が 1 DB、 server は blob CDN + relay 役のみ。
//!
//! 1 process で alice の 2 device (phone + laptop) を模擬し、 metadata は
//! peer-of-peer で sync、 大 blob は server CDN に offload する流れを示す。
//!
//! 実運用では:
//! - peer (device): `~/Documents/myapp/local.ecdb`
//! - sync transport: WebRTC datachannel + signaling relay
//! - blob store: S3 / CDN (= 大 content は別 storage、 metadata だけ sync)
//!
//! 起動: `cargo run --example sync_local_first`

use enchudb::schema::Database;

fn open_or_create_local_db(device: &str) -> Result<Database, Box<dyn std::error::Error>> {
    let path = format!("/tmp/enchudb-pattern-c-{}-{}.ecdb", device, std::process::id());
    let _ = std::fs::remove_file(&path);
    for suf in &[".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }

    let mut db = Database::create(&path)?;
    let _ = db.table("notes")
        .number("id")
        .tag("title")
        // blob URL を Tag に持つ (= 実 content は CDN)、 metadata だけ DB に
        .tag("blob_url")
        .number("size_bytes")
        .number("created_s")
        .primary_key("id")
        .build()?;
    db.enable_sync()?;
    Ok(db)
}

fn cleanup(device: &str) {
    let path = format!("/tmp/enchudb-pattern-c-{}-{}.ecdb", device, std::process::id());
    let _ = std::fs::remove_file(&path);
    for suf in &[".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut phone_db = open_or_create_local_db("phone")?;
    let mut laptop_db = open_or_create_local_db("laptop")?;

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
    }

    println!("phone created 1 note");
    println!("phone sync_lsn: {}", phone_db.engine().current_sync_lsn());

    // 実 peer sync 経路:
    // 1. phone: transfer_oplog_to_sync_ops で `_sync_ops` を populate
    // 2. phone → relay: pending_sync_ops で payload bytes を取り出して送信
    // 3. relay → laptop: 受信した payload を apply_records で取り込む
    // 4. laptop: ack で phone に「ここまで取った」 と返す → phone reclaim
    //
    // この example では transport 層を組まないので、 phone DB の snapshot を
    // file copy で laptop の path に持って行く形で「初期 sync」 を模擬する。

    phone_db.engine().transfer_oplog_to_sync_ops();
    let phone_lsn = phone_db.engine().current_sync_lsn();
    println!("phone _sync_ops populated, lsn={phone_lsn}");

    // 実際の peer-of-peer transport は WebRTC datachannel 等で wire しないと
    // 動かないので、 ここでは「phone から取った snapshot を laptop 側で開く」
    // という path にしない (= per_user pattern と被るため)。 metadata blob URL
    // が DB row に居ること = 大 content は別 store という構造だけ示せれば OK。

    let notes = phone_db.get_table("notes").unwrap();
    for &eid in &notes.all().find()? {
        let r = notes.entity(eid);
        let title = r.get("title").unwrap();
        let url = r.get("blob_url").unwrap();
        let size = r.get("size_bytes").unwrap();
        println!("note: title={title:?}, blob={url:?}, size={size:?}");
    }

    // laptop DB は schema だけ持つ (= 初期 sync で phone から取り込む想定)
    let laptop_notes = laptop_db.get_table("notes").unwrap();
    println!("laptop has {} notes initially", laptop_notes.all().count()?);

    drop(phone_db);
    drop(laptop_db);
    cleanup("phone");
    cleanup("laptop");
    Ok(())
}
