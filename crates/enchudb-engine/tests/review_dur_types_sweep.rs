//! review (0.37.4): `oplog_sync()` が返った時点の本体だけで (WAL の再生なしで) 全部の型の列が読めるか。
//!
//! 既存の crashsim の試験 (Kind::Oplog / Sync / LeafSync) は像を WAL 付きで開く = 本体の書き出し漏れ (mark_dirty の漏れ) を
//! 再生が埋めると見えない。 ここは `oplog_sync()` が返った直後の `Lost` の像を **standalone (WAL を読まない)** で開く。
//! `oplog_sync` は checkpoint を進めるので、 その時点の write は全部本体にあるはず。
//! 列: Number / Number64 / Tag / Leaf / Ref / content (`_c_{key}` の紐を走りながら作る)。 書き換え・untie・delete を混ぜる。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Mode};
use enchudb_oplog::Hlc;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

static SERIAL: Mutex<()> = Mutex::new(());
const OPLOG_CAP: usize = 8 * 1024 * 1024;
const ROWS: usize = 3_000;

fn scratch(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("enchu_review_dur_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

#[derive(Clone, Default, PartialEq, Debug)]
struct Row {
    live: bool,
    n: Option<u64>,
    big: Option<u64>,
    tag: Option<String>,
    leaf: Option<String>,
    rf: Option<u64>,
    memo: Option<Vec<u8>>,
}

fn read(eng: &Engine, e: u64) -> Row {
    let s = |h: &str| eng.get_text_owned(e, h).map(|b| String::from_utf8_lossy(&b).into_owned());
    Row {
        live: eng.is_live(e),
        n: eng.get(e, "t.n"),
        big: eng.get(e, "t.big"),
        tag: s("t.tag"),
        leaf: s("t.leaf"),
        rf: eng.get(e, "t.rf"),
        memo: eng.get_content_owned(e, "memo"),
    }
}

fn run(sync: bool, versions: bool) {
    let root = scratch(&format!("types_{}_{}", sync as u8, versions as u8));
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = if versions {
        Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap()
    } else {
        Engine::create_with_capacity(db.to_str().unwrap(), 65_536).unwrap()
    };
    eng.define_table("t", 20_000).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "big", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("t", "tag", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("t", "leaf", ValueType::Leaf, 0).unwrap();
    eng.define_himo_in("t", "rf", ValueType::Ref, 0).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let hids: Vec<u16> = ["t.n", "t.big", "t.tag", "t.leaf", "t.rf"].iter().map(|h| eng.himo_id(h).unwrap() as u16).collect();

    let mut model: BTreeMap<u64, Row> = BTreeMap::new();
    let mut eids: Vec<u64> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for round in 0..6u64 {
        // 行を足す
        if round < 3 {
            for _ in 0..ROWS / 3 {
                let e = eng.entity_in("t").unwrap();
                eids.push(e);
                model.insert(e, Row { live: true, ..Default::default() });
            }
        }
        for (i, &e) in eids.iter().enumerate() {
            let r = mix(round * 1_000_003 + i as u64);
            let row = model.get_mut(&e).unwrap();
            if !row.live {
                continue;
            }
            match r % 16 {
                0 if round > 0 => {
                    eng.delete(e);
                    *row = Row::default();
                    continue;
                }
                1 => {
                    eng.untie(e, "t.n");
                    row.n = None;
                }
                2 => {
                    eng.untie(e, "t.big");
                    row.big = None;
                }
                3 => {
                    eng.untie(e, "t.tag");
                    row.tag = None;
                }
                4 => {
                    eng.untie(e, "t.leaf");
                    row.leaf = None;
                }
                _ => {}
            }
            if r & 0x100 != 0 {
                let v = (r >> 20) as u32 >> 1;
                eng.tie_to(e, "t.n", v);
                row.n = Some(v as u64);
            }
            if r & 0x200 != 0 {
                let v = (r | (1 << 40)) & (u64::MAX >> 1);
                eng.tie_to(e, "t.big", v);
                row.big = Some(v);
            }
            if r & 0x400 != 0 {
                // 半分は使い回す語、 半分は新しい語
                let v = if r & 0x4000 != 0 { format!("shared-{}", r % 50) } else { format!("uniq-{round}-{i}") };
                eng.tie_text_to(e, "t.tag", &v);
                row.tag = Some(v);
            }
            if r & 0x800 != 0 {
                let v = format!("leaf-{round}-{i}-{}", "x".repeat((r % 300) as usize));
                eng.tie_text_to(e, "t.leaf", &v);
                row.leaf = Some(v);
            }
            if r & 0x1000 != 0 {
                let target = eids[(r as usize >> 8) % eids.len()];
                eng.tie_ref_to(e, "t.rf", target);
                row.rf = Some(enchudb_oplog::eid_local(target) as u64);
            }
            if r & 0x2000 != 0 {
                let v = format!("memo-{round}-{i}-{}", "m".repeat((r % 200) as usize)).into_bytes();
                eng.content(e, "memo", &v);
                row.memo = Some(v);
            }
        }
        eng.oplog_sync().unwrap();
        // 生の engine が模型と合っている (試験の前提)
        let mut vers: BTreeMap<(u64, u16), Hlc> = BTreeMap::new();
        for (&e, want) in &model {
            if want.live {
                assert_eq!(&read(&eng, e), want, "前提: 生の engine が模型と違う (round {round}, e {e})");
                if versions {
                    for &h in &hids {
                        vers.insert((e, h), eng.cell_hlc(e, h));
                    }
                }
            }
        }
        let img = root.join(format!("img{round}"));
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
        // WAL を読まずに開く
        let back = Engine::open_standalone(img.join("db").to_str().unwrap()).unwrap();
        let mut bad = 0usize;
        for (&e, want) in &model {
            let got = read(&back, e);
            let same = if want.live { &got == want } else { !got.live };
            if !same {
                bad += 1;
                if bad <= 3 {
                    failures.push(format!("round {round} e {e}: 像 {got:?} / 書き出しの返った値 {want:?}"));
                }
            }
        }
        let mut bad_ver = 0usize;
        for (&(e, h), want) in &vers {
            if back.cell_hlc(e, h) != *want {
                bad_ver += 1;
                if bad_ver <= 3 {
                    failures.push(format!("round {round} e {e} himo {h}: 像の版数 {:?} / 生 {want:?}", back.cell_hlc(e, h)));
                }
            }
        }
        let inv = back.verify_invariants();
        eprintln!(
            "[review_dur] sync={sync} versions={versions} round {round}: 行 {} 値の違い {bad} 版数の違い {bad_ver} 不変量の違反 {:?}",
            model.len(),
            inv.violations
        );
        if !inv.violations.is_empty() {
            failures.push(format!("round {round}: verify_invariants {:?}", inv.violations));
        }
        drop(back);
        let _ = std::fs::remove_dir_all(&img);
    }
    crashsim::stop();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(failures.is_empty(), "oplog_sync() が返った write が本体に無い:\n{}", failures.join("\n"));
}

#[test]
fn body_alone_holds_everything_after_oplog_sync_plain() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    run(false, false);
}

#[test]
fn body_alone_holds_everything_after_oplog_sync_versions() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    run(false, true);
}

#[test]
fn body_alone_holds_everything_after_oplog_sync_sync_tables() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    run(true, true);
}
