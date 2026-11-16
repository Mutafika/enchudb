//! #993 / #994: `eidalias` (名前で畳んだ他の peer の行の写像、 #178 / #612) の file の形。
//!
//! - 世代を持たない DB (file v14 以下) は 0.37.4 と同じ形 (版 1: magic `EIDA` + 版 1 + 件数 + (peer, 番号, 場所) × 件数) のまま書く
//! - 世代を持つ DB (file v15、 最初の使い回しで移る) は版 2: 別名ごとに指す行の世代 (張った時の場所の住人の世代) を足す
//!   (`(peer, 番号, 場所, 世代)` × 件数)。 開き直すと別名の世代が戻る
//!
//! byte の並びそのものは engine の単体試験 (`eidalias_format_tests`) が見る。 ここは engine が DB の版に合わせて書き分けることを見る。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::{eid_local, make_eid, Hlc};
use std::path::Path;

const CAP: usize = 8 * 1024 * 1024;

fn scratch(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "enchudb-eidalias-gen-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// `eidalias` を読んで (版, [(peer, 番号, 場所, 世代)]) を返す (版 1 の世代は None)。
/// `eidalias` の 1 件 (peer, 番号, 場所, 世代 = 版 1 は None)。
type Row = (u32, u32, u32, Option<u32>);

fn read_eidalias(db: &Path) -> (u32, Vec<Row>) {
    let buf = std::fs::read(db.join("eidalias")).expect("eidalias がある");
    assert_eq!(&buf[0..4], b"EIDA");
    let rd = |o: usize| u32::from_le_bytes(buf[o..o + 4].try_into().unwrap());
    let (version, count) = (rd(4), rd(8) as usize);
    let width = match version {
        1 => 12,
        2 => 16,
        v => panic!("知らない版 {v}"),
    };
    assert_eq!(buf.len(), 12 + count * width, "件数と長さが合わない (版 {version})");
    let rows = (0..count)
        .map(|i| 12 + i * width)
        .map(|o| (rd(o), rd(o + 4), rd(o + 8), (width == 16).then(|| rd(o + 12))))
        .collect();
    (version, rows)
}

/// 名前のある表 t (k = 名前、 v) の sync する DB (peer 1)。
fn engine(db: &Path) -> std::sync::Arc<Engine> {
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 2).unwrap();
    eng.define_himo_in("t", "k", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    let k = eng.himo_id("t.k").unwrap() as u16;
    eng.set_table_pk("t", k).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    eng.set_peer_id(1);
    eng
}

/// peer `peer` の行 `foreign_local` の写しを作り、 自分の行 `into` に畳む (その世界番号は `into` の場所の別名になる)。
fn fold_foreign(eng: &Engine, peer: u32, foreign_local: u32, into: u64) {
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let world = make_eid(peer, foreign_local);
    let b = eng.resolve_remote_eid(world, hv).unwrap();
    assert!(eng.remote_tie_apply_result(b, hv, 8u32, Hlc { wall: 1_000, logical: 0, peer }).applied());
    eng.merge_entity_into(b, into).unwrap();
    assert_eq!(
        eng.resolve_remote_eid_existing(world).map(|e| eng.eid_slot(e)),
        Some(eng.eid_slot(into)),
        "前提: ({peer}, {foreign_local}) は {into:x} の場所の別名"
    );
}

/// 使い回しの無い DB (v14) の `eidalias` は 0.37.4 と同じ版 1 の byte で書く (0.37.4 の binary が開ける)。 最初の使い回しで v15 に
/// 移った後の書き出しは版 2 (別名ごとに指す行の世代): 移る前の行を指す別名は世代 0、 使い回した場所の住人に張った別名はその世代。
/// きれいに閉じて開き直すと、 別名の世代が memory に戻る。
#[test]
fn eidalias_is_written_in_the_form_of_the_db_version() {
    let root = scratch("form");
    let db = root.join("db");
    std::fs::create_dir_all(&root).unwrap();
    let (x_slot, y_slot) = {
        let eng = engine(&db);
        let x = eng.entity_in("t").unwrap();
        eng.tie_to(x, "t.k", 7u32);
        let w = eng.entity_in("t").unwrap();
        eng.tie_to(w, "t.k", 100u32);
        eng.oplog_commit();
        fold_foreign(&eng, 3, 9, x);
        eng.oplog_sync().unwrap();
        eng.persist_tables().unwrap();
        assert_eq!(eng.slot_bits(), 0, "前提: 使い回しが無い = v14");
        let (version, rows) = read_eidalias(&db);
        let x_slot = eng.eid_slot(x);
        assert_eq!((version, rows.clone()), (1, vec![(3, 9, x_slot, None)]), "v14 の DB の `eidalias` が版 1 でない");
        // 0.37.4 の形と byte で同じ
        let mut want = Vec::new();
        for v in [u32::from_le_bytes(*b"EIDA"), 1, 1, 3, 9, x_slot] {
            want.extend_from_slice(&v.to_le_bytes());
        }
        assert_eq!(std::fs::read(db.join("eidalias")).unwrap(), want, "v14 の DB の `eidalias` が 0.37.4 の byte でない");

        // 払い出し位置を枠の終わりまで進めてから W を消し、 消した場所 (W か、 畳んで空いた b の場所) を Y で使い回す (枯渇した表の
        // 使い回しで v15 に移る)
        let mut n = 200u32;
        while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity) {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to(e, "t.k", n);
            n += 1;
        }
        eng.oplog_commit();
        assert_eq!(eng.slot_bits(), 0, "前提: まだ使い回していない");
        eng.delete(w);
        eng.oplog_commit();
        let y = eng.entity_in("t").unwrap();
        assert!(eng.slot_bits() > 0, "前提: 使い回しで v15 に移った");
        eng.tie_to(y, "t.k", 11u32);
        eng.oplog_commit();
        fold_foreign(&eng, 4, 2, y);
        eng.oplog_sync().unwrap();
        eng.persist_tables().unwrap();
        let y_slot = eng.eid_slot(y);
        let y_gen = eid_local(y) >> eng.slot_bits();
        assert!(y_gen > 0, "前提: Y は世代 > 0");
        let (version, mut rows) = read_eidalias(&db);
        rows.sort_unstable();
        assert_eq!(
            (version, rows),
            (2, vec![(3, 9, x_slot, Some(0)), (4, 2, y_slot, Some(y_gen))]),
            "v15 の DB の `eidalias` が版 2 (指す行の世代) でない"
        );
        (x_slot, y_slot)
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), CAP).unwrap();
    let mut got = eng.eid_translator().alias_snapshot();
    got.sort_unstable();
    let y_gen = eng.eid_translator().alias_snapshot().iter().find(|a| a.0 == 4).map(|a| a.3);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(y_gen.is_some_and(|g| g > 0));
    assert_eq!(got, vec![(3, 9, x_slot, 0), (4, 2, y_slot, y_gen.unwrap())], "開き直した別名の世代");
}

/// directory `from` を `to` に丸ごと写す (kill の像: 開いている engine の page cache の中身も届く。 きれいに閉じた印は立っていない)。
/// 写す間に消えた file (背景の名前の記録のまとめ直し (#959) の一時 file など) は像に無いだけ。
fn kill_image(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            kill_image(&entry.path(), &dest);
        } else if let Err(e) = std::fs::copy(entry.path(), &dest) {
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound, "{}: {e}", entry.path().display());
            let _ = std::fs::remove_file(&dest);
        }
    }
}

/// PR #1006 の読み直し [4]: `bind_remote_eid` に消えた行を渡すと、 束ねた場所を生かす (今の世代の行)。 別名の指す行の世代は、 生かした
/// 後の住人の世代 (場所の最後の行)。 前は世代を場所を生かす前に決め (消えた行 = 今 − 1)、 生きている場所の別名としては前の住人の物に
/// 見えた。 きれいに閉じずに開き直しても別名は場所を指す (この像では開く道が X の tombstone の残る場所を消し直す = 死んだ場所の別名)。
#[test]
fn an_alias_bound_to_a_dead_slot_carries_the_generation_of_the_revived_row() {
    let root = scratch("bind_dead");
    let (db, img) = (root.join("db"), root.join("img"));
    std::fs::create_dir_all(&root).unwrap();
    let eng = engine(&db);
    let w = eng.entity_in("t").unwrap();
    eng.tie_to(w, "t.k", 100u32);
    let mut n = 200u32;
    while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity) {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.k", n);
        n += 1;
    }
    eng.delete(w);
    eng.oplog_commit();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to(x, "t.k", 7u32);
    eng.oplog_commit();
    let bits = eng.slot_bits();
    assert!(bits > 0, "前提: 使い回しで v15 に移った");
    eng.delete(x);
    eng.oplog_commit();
    let world = make_eid(2, 5);
    let bound = eng.bind_remote_eid(2, world, x);
    let slot = eng.eid_slot(x);
    assert_eq!(eng.eid_slot(bound), slot, "前提: 消えた X の場所に束ねた");
    let snap = eng.eid_translator().alias_snapshot();
    let agen = snap.iter().find(|a| (a.0, a.1) == (2, 5)).map(|a| a.3);
    let x_gen = eid_local(x) >> bits;
    eng.oplog_sync().unwrap();
    eng.persist_tables().unwrap();
    kill_image(&db, &img);
    drop(eng);
    let eng = Engine::open_concurrent_with_oplog(img.to_str().unwrap(), CAP).unwrap();
    let after = eng.resolve_remote_eid_existing(world).map(|e| eng.eid_slot(e));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(agen, Some(x_gen + 1), "別名の世代が生かした行 (X + 1) でない (束ねる前の消えた行 X のまま)");
    assert_eq!(after, Some(slot), "消えた行に束ねた別名が、 きれいでない open で外れた");
}
