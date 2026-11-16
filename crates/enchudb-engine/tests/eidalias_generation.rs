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
fn kill_image(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            kill_image(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

/// #993 / #994: きれいに閉じなかった後の書ける open は、 `eidalias` (版 2) の別名のうち、 指す行の世代が場所の最後の行の世代と合わない物を
/// 外す。 下の世代 (使い回した場所の前の住人の別名) は再生の前に、 上の世代 (本体にも WAL にも無い行を指す) は開く道の仕上げで。
/// 合う別名は残す。 像は kill (使い回しの記録や退避と関わらない形で、 `eidalias` の世代だけを書き換える)。
#[test]
fn an_alias_whose_generation_does_not_match_the_slot_is_dropped_on_unclean_open() {
    let root = scratch("mismatch");
    let (db, img) = (root.join("db"), root.join("img"));
    std::fs::create_dir_all(&root).unwrap();
    let eng = engine(&db);
    let x = eng.entity_in("t").unwrap();
    eng.tie_to(x, "t.k", 7u32);
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
    let y = eng.entity_in("t").unwrap();
    eng.tie_to(y, "t.k", 11u32);
    eng.oplog_commit();
    let bits = eng.slot_bits();
    assert!(bits > 0 && eid_local(y) >> bits > 0 && eid_local(x) >> bits == 0, "前提: v15、 Y は世代 > 0、 X は世代 0");
    fold_foreign(&eng, 3, 9, x);
    fold_foreign(&eng, 4, 2, y);
    fold_foreign(&eng, 5, 1, y);
    eng.oplog_sync().unwrap();
    eng.persist_tables().unwrap();
    let (x_slot, y_slot, y_gen) = (eng.eid_slot(x), eng.eid_slot(y), eid_local(y) >> bits);
    kill_image(&db, &img);
    drop(eng);
    // 像の `eidalias` の世代を書き換える: (3, 9) → X は上の世代、 (4, 2) → Y は下の世代、 (5, 1) → Y はそのまま
    let path = img.join("eidalias");
    let mut buf = std::fs::read(&path).unwrap();
    let count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    assert_eq!((&buf[4..8], count), (&2u32.to_le_bytes()[..], 3), "前提: 版 2 の 3 件");
    for i in 0..count {
        let o = 12 + i * 16;
        let rd = |o: usize| u32::from_le_bytes(buf[o..o + 4].try_into().unwrap());
        let g = match (rd(o), rd(o + 4)) {
            (3, 9) => 1,
            (4, 2) => y_gen - 1,
            _ => continue,
        };
        buf[o + 12..o + 16].copy_from_slice(&u32::to_le_bytes(g));
    }
    std::fs::write(&path, &buf).unwrap();
    let eng = Engine::open_concurrent_with_oplog(img.to_str().unwrap(), CAP).unwrap();
    let at = |peer: u32, fl: u32| eng.resolve_remote_eid_existing(make_eid(peer, fl)).map(|e| eng.eid_slot(e));
    let got = (at(3, 9), at(4, 2), at(5, 1));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(got.0, None, "上の世代 (本体に無い行) を指す別名 (3, 9) が X の場所 {x_slot} に残った");
    assert_eq!(got.1, None, "下の世代 (前の住人) を指す別名 (4, 2) が Y の場所 {y_slot} に残った");
    assert_eq!(got.2, Some(y_slot), "世代の合う別名 (5, 1) を外した");
}
