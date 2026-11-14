//! #851 (engine、 版数の列の無い既定の DB): WAL の record の中身 (header + payload) だけが前の周の record のまま disk に残り、
//! 後ろの周の印は今の周の物が届いた像。 開く時の再生がその record を続きとして当て、 `oplog_sync` の返った値がそれより古い値に戻る。
//!
//! 像は手で組む: 書き出しの返っていない最後の record (別の行への書き込み) の header + payload を、 同じ大きさの古い record
//! (同じ cell の、 2 つ前の値) の bytes に置き換える (印はそのまま)。 「同じ位置に同じ長さの record が前の周にあった」 像が
//! page / sector の切れでできる事は oplog の単体 (`previous_lap_content_under_a_current_lap_mark_stops_the_scan`) が見る。
//!
//! cargo test -p enchudb-engine --test issue851_stale_content_replay -- --ignored

#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};
use std::path::{Path, PathBuf};

const CAP: usize = 128 * 1024;
const REC_HEADER: usize = 112;

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for ent in std::fs::read_dir(from).unwrap() {
        let ent = ent.unwrap();
        let dst = to.join(ent.file_name());
        if ent.file_type().unwrap().is_dir() {
            copy_dir(&ent.path(), &dst);
        } else {
            std::fs::copy(ent.path(), &dst).unwrap();
        }
    }
}

/// Tie (u32 の値) の record の位置 (payload = `[eid 8][himo 2][pad 2][value 4]`)。
fn find_tie(file: &[u8], eid: u64, himo: u16, value: u32) -> Vec<usize> {
    let mut pat = Vec::new();
    pat.extend_from_slice(&eid.to_le_bytes());
    pat.extend_from_slice(&himo.to_le_bytes());
    pat.extend_from_slice(&[0, 0]);
    pat.extend_from_slice(&value.to_le_bytes());
    (REC_HEADER..file.len() - 16)
        .filter(|&i| file[i..i + 16] == pat[..] && &file[i - REC_HEADER..i - REC_HEADER + 2] == b"WL")
        .map(|i| i - REC_HEADER)
        .collect()
}

#[test]
#[ignore = "#851: 周の印が record の中身に結ばれていない (直しは file の形の変更が要る)"]
fn an_acked_value_does_not_go_back_to_a_previous_lap_record_under_a_current_mark() {
    let root = Scratch(std::env::temp_dir().join(format!("enchu-issue851-engine-{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    let db = root.0.join("live").join("db");
    std::fs::create_dir_all(db.parent().unwrap()).unwrap();
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "x", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    let himo = eng.himo_id("t.x").unwrap() as u16;
    let (e, other) = (eng.entity_in("t").unwrap(), eng.entity_in("t").unwrap());

    eng.tie_to(e, "t.x", 1u32);
    eng.oplog_sync().unwrap();
    let oplog = db.join("oplog");
    let file = std::fs::read(&oplog).unwrap();
    let at = find_tie(&file, e, himo, 1);
    assert_eq!(at.len(), 1, "前提: 値 1 の record が WAL に 1 つ");
    let old: Vec<u8> = file[at[0]..at[0] + REC_HEADER + 16].to_vec();

    eng.tie_to(e, "t.x", 2u32);
    eng.oplog_sync().unwrap(); // ← 2 は電源断の後も残る
    eng.tie_to(other, "t.x", 7u32); // 書き出しの返っていない最後の record

    // 今落ちた像 (全部の page が届いた形) を写し、 最後の record の header + payload だけ前の record の中身にする
    let img = root.0.join("img").join("db");
    copy_dir(&db, &img);
    drop(eng);
    let img_oplog = img.join("oplog");
    let mut image = std::fs::read(&img_oplog).unwrap();
    let last = find_tie(&image, other, himo, 7);
    assert_eq!(last.len(), 1, "前提: 最後の record が像にある");
    let t = last[0] + REC_HEADER + 16;
    let trailer: [u8; 8] = image[t..t + 8].try_into().unwrap();
    image[last[0]..t].copy_from_slice(&old);
    assert_eq!(image[t..t + 8], trailer, "印は今の周のまま");
    std::fs::write(&img_oplog, &image).unwrap();

    let got = Engine::open_concurrent_with_oplog(img.to_str().unwrap(), CAP).map(|eng| eng.get(e, "t.x")).unwrap();
    assert_eq!(got, Some(2), "書き出しの返った 2 が、 前の周の record の値に戻った");
}
