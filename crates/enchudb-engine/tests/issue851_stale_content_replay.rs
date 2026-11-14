//! #851 (engine、 版数の列の無い既定の DB): WAL の record の中身 (header + payload) だけが前の周の record のまま disk に残り、
//! 後ろの周の印は今の周の物が届いた像。 開く時の再生がその record を続きとして当て、 `oplog_sync` の返った値がそれより古い値に戻る。
//!
//! 像は手で組む: 書き出しの返っていない最後の record (別の行への書き込み) の header + payload を、 同じ大きさの古い record
//! (同じ cell の、 2 つ前の値) の bytes に置き換える (印はそのまま)。 「同じ位置に同じ長さの record が前の周にあった」 像が
//! page / sector の切れでできる事は oplog の単体 (`previous_lap_content_under_a_current_lap_mark_stops_the_scan`) が見る。
//!
//! cargo test -p enchudb-engine --test issue851_stale_content_replay

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

/// Tie (u32 の値) の payload = `[eid 8][himo 2][pad 2][value 4]`。
fn tie_payload(eid: u64, himo: u16, value: u32) -> Vec<u8> {
    let mut pat = Vec::new();
    pat.extend_from_slice(&eid.to_le_bytes());
    pat.extend_from_slice(&himo.to_le_bytes());
    pat.extend_from_slice(&[0, 0]);
    pat.extend_from_slice(&value.to_le_bytes());
    pat
}

/// Tie の record の位置。
fn find_tie(file: &[u8], eid: u64, himo: u16, value: u32) -> Vec<usize> {
    let pat = tie_payload(eid, himo, value);
    (REC_HEADER..file.len() - 16)
        .filter(|&i| file[i..i + 16] == pat[..] && &file[i - REC_HEADER..i - REC_HEADER + 2] == b"WL")
        .map(|i| i - REC_HEADER)
        .collect()
}

#[test]
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

/// sync する DB (版数の列あり): 同じ像でも、 再生した古い record は LWW で負けて値は戻らない。 ただし直す前は、 その record が
/// もう 1 回 `_sync_ops` (相手に配る分) に写った = 古い record を余分に配る。 直した後は像の古い record を読まない。
#[test]
fn a_sync_db_does_not_hand_out_a_previous_lap_record_again() {
    let root = Scratch(std::env::temp_dir().join(format!("enchu-issue851-engine-sync-{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    let db = root.0.join("live").join("db");
    std::fs::create_dir_all(db.parent().unwrap()).unwrap();
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "x", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    let himo = eng.himo_id("t.x").unwrap() as u16;
    let (e, other) = (eng.entity_in("t").unwrap(), eng.entity_in("t").unwrap());
    // 配る分の中で、 (e, x) = v の record の数
    let handed = |eng: &Engine, v: u32| {
        let pat = tie_payload(e, himo, v);
        eng.pending_sync_ops(0).iter().filter(|p| p.windows(pat.len()).any(|w| w == &pat[..])).count()
    };

    eng.tie_to(e, "t.x", 1u32);
    eng.oplog_sync().unwrap();
    let oplog = db.join("oplog");
    let file = std::fs::read(&oplog).unwrap();
    let at = find_tie(&file, e, himo, 1);
    assert_eq!(at.len(), 1, "前提: 値 1 の record が WAL に 1 つ");
    let old: Vec<u8> = file[at[0]..at[0] + REC_HEADER + 16].to_vec();
    eng.tie_to(e, "t.x", 2u32);
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    eng.oplog_sync().unwrap(); // 写した行も書き出す
    assert_eq!((handed(&eng, 1), handed(&eng, 2)), (1, 1), "前提: 配る分に 1 回ずつ");
    eng.tie_to(other, "t.x", 7u32);

    let img = root.0.join("img").join("db");
    copy_dir(&db, &img);
    drop(eng);
    let img_oplog = img.join("oplog");
    let mut image = std::fs::read(&img_oplog).unwrap();
    let last = find_tie(&image, other, himo, 7);
    assert_eq!(last.len(), 1, "前提: 最後の record が像にある");
    let t = last[0] + REC_HEADER + 16;
    image[last[0]..t].copy_from_slice(&old);
    std::fs::write(&img_oplog, &image).unwrap();

    let eng = Engine::open_concurrent_with_oplog(img.to_str().unwrap(), CAP).unwrap();
    assert_eq!(eng.get(e, "t.x"), Some(2), "版数のある DB は古い record を当てない");
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    // きれいに閉じなかった後の bridge は WAL を読み直すので、 どの record も配る分にもう 1 回ずつ積まれうる (受け手は
    // (author, HLC) で落とす)。 像の古い record を読むと、 値 1 の record だけ更に 1 回多い
    let (ones, twos) = (handed(&eng, 1), handed(&eng, 2));
    eprintln!("配る分: 値 1 の record {ones} 回、 値 2 の record {twos} 回");
    assert_eq!(ones, twos, "前の周の record (値 1) をもう 1 回配る分に積んだ");
}
