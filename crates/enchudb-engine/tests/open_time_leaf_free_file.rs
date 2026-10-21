//! 開く時間: きれいに閉じた時に Leaf の空き一覧を `{db}/leaf.free` に残し、 次の open は列から作り直さずにそれを入れる。
//! 信じるのは紐名の辞書の印が `CLEAN_LEAF` (閉じる時にだけ書く) の時だけ。 kill の後 / 途中の flush の後 / 壊れた file /
//! 今の store と合わない file は列から作り直す (前と同じ)。 どの道でも、 空きの使い回しが live の値を壊さないことを全部の値で
//! 確かめる。 kill は子 process (同じ process で engine を forget すると writer の flock が残る) — `child_entry` が入口。

use enchudb_engine::{Engine, ValueType};
use std::path::PathBuf;
use std::process::Command;

const CHILD_ENV: &str = "ENCHU_LEAF_FREE_CHILD";

fn tmp(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("enchu-leaf-free-{tag}-{}", std::process::id()));
    let _ = enchudb_engine::db_files::remove_db(&p);
    p.to_str().unwrap().to_string()
}

fn body(i: u32) -> String {
    format!("本文 {i}: {}", "x".repeat((i % 7) as usize * 5 + 3))
}

/// Leaf の列 1 本に N 件書き、 3 件に 1 件は外す (空きができる)。 きれいに閉じる (Drop)
fn write_db(path: &str, n: u32) -> Vec<(u64, Option<String>)> {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    let mut rows = Vec::new();
    for i in 0..n {
        let e = eng.entity().unwrap();
        eng.tie_text(e, "body", &body(i));
        rows.push((e, Some(body(i))));
    }
    for (i, (e, v)) in rows.iter_mut().enumerate() {
        if i % 3 == 1 {
            eng.untie(*e, "body");
            *v = None;
        }
    }
    rows
}

fn check(eng: &Engine, rows: &[(u64, Option<String>)]) {
    for (e, v) in rows {
        let got = eng.get_text(*e, "body").map(|b| String::from_utf8_lossy(b).into_owned());
        assert_eq!(got.as_deref(), v.as_deref(), "entity {e} の Leaf の値が変わった");
    }
}

/// 開き直して (writer)、 新しい値を書き足しても前の値が全部無傷なことを確かめる。 `expect_reuse` = 外した分の空きが
/// 使い回されること (開いた直後に空きがあり、 書き足すと減る) も確かめる
fn reopen_and_churn(path: &str, rows: &mut Vec<(u64, Option<String>)>, expect_from_file: bool, expect_reuse: bool, tag: &str) {
    let mut eng = Engine::open_standalone(path).unwrap();
    assert_eq!(eng.leaf_free_list_from_file(), expect_from_file, "{tag}: 空き一覧を file から入れたか");
    check(&eng, rows);
    let free_before = eng.leaf_free_bytes();
    if expect_reuse {
        assert!(free_before > 0, "{tag}: 外した分の空きが無い (作り直し / file のどちらも空きを持つはず)");
    }
    for i in 0..(rows.len() as u32 / 2) {
        let e = eng.entity().unwrap();
        let v = format!("後から {tag} {i} {}", "y".repeat((i % 5) as usize * 7 + 2));
        eng.tie_text(e, "body", &v);
        rows.push((e, Some(v)));
    }
    if expect_reuse {
        assert!(eng.leaf_free_bytes() < free_before, "{tag}: 空きが使い回されていない");
    }
    check(&eng, rows);
}

/// 子 process で書いて閉じずに終わる (= kill)。 書いた変化は stdout の行 (`D <eid>` / `A <eid> <value>`) で親に返す
fn run_child(scenario: &str, path: &str, rows: &mut Vec<(u64, Option<String>)>) {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["child_entry", "--exact", "--nocapture"])
        .env(CHILD_ENV, format!("{scenario};{path}"))
        .output()
        .unwrap();
    assert!(out.status.success(), "子 process が失敗: {}", String::from_utf8_lossy(&out.stderr));
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        if let Some(rest) = line.strip_prefix("D ") {
            let eid: u64 = rest.parse().unwrap();
            rows.iter_mut().find(|(e, _)| *e == eid).unwrap().1 = None;
        } else if let Some(rest) = line.strip_prefix("A ") {
            let (eid, v) = rest.split_once(' ').unwrap();
            rows.push((eid.parse().unwrap(), Some(v.to_string())));
        }
    }
}

#[test]
fn child_entry() {
    let Ok(spec) = std::env::var(CHILD_ENV) else { return };
    let (scenario, path) = spec.split_once(';').unwrap();
    let mut eng = Engine::open_standalone(path).unwrap();
    assert!(eng.leaf_free_list_from_file(), "子: きれいに閉じた後なのに file から入れていない");
    let live: Vec<u64> = (0..eng.entity_count() as u64).filter(|e| eng.get_text(*e, "body").is_some()).collect();
    match scenario {
        // 開いて (印は倒れる) 外してから閉じずに終わる
        "kill" => {
            for (i, e) in live.iter().enumerate() {
                if i % 5 == 0 {
                    eng.untie(*e, "body");
                    println!("D {e}");
                }
            }
            eng.flush().unwrap();
        }
        // 途中の flush (印は立つが CLEAN_LEAF ではない) の後に Leaf を書き換えて、 閉じずに終わる
        "flush" => {
            eng.flush().unwrap();
            for (i, e) in live.iter().enumerate() {
                if i % 4 == 0 {
                    eng.untie(*e, "body");
                    println!("D {e}");
                }
            }
            for i in 0..100u32 {
                let e = eng.entity().unwrap();
                let v = format!("flush_の後_{i}");
                eng.tie_text(e, "body", &v);
                println!("A {e} {v}");
            }
            eng.flush().unwrap();
        }
        other => panic!("知らない scenario {other}"),
    }
    std::process::exit(0);
}

#[test]
fn clean_close_leaves_the_free_list_and_the_next_open_installs_it() {
    let path = tmp("clean");
    let mut rows = write_db(&path, 600);
    let file = PathBuf::from(&path).join(enchudb_engine::db_files::LEAFFREE);
    assert!(file.exists(), "きれいに閉じたのに leaf.free が無い");
    reopen_and_churn(&path, &mut rows, true, true, "1 回目");
    // 2 回目: 閉じ直すと file が書き直され (空きは 1 回目で使った分が減っている)、 また file から入る
    reopen_and_churn(&path, &mut rows, true, false, "2 回目");
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn an_unclean_exit_rebuilds_from_the_columns() {
    let path = tmp("kill");
    let mut rows = write_db(&path, 600);
    run_child("kill", &path, &mut rows);
    reopen_and_churn(&path, &mut rows, false, true, "kill の後");
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn a_flush_during_the_session_does_not_vouch_for_the_file() {
    let path = tmp("flush");
    let mut rows = write_db(&path, 600);
    run_child("flush", &path, &mut rows);
    // 閉じた時の古い file は残っているが、 印が CLEAN_LEAF でないので信じない (flush の後の書き換えで空きは違う)
    reopen_and_churn(&path, &mut rows, false, true, "flush の後");
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn a_damaged_or_mismatched_file_is_ignored() {
    let path = tmp("damaged");
    let mut rows = write_db(&path, 600);
    let file = PathBuf::from(&path).join(enchudb_engine::db_files::LEAFFREE);
    // 化けた: 最初の空きの大きさが 1 word 小さい (構造は正しいままなので FNV でしか分からない)
    let good = std::fs::read(&file).unwrap();
    let n = u32::from_le_bytes(good[12..16].try_into().unwrap());
    assert!(n >= 2, "空きが 2 つ以上ある前提");
    let mut bad = good.clone();
    let size0 = u32::from_le_bytes(bad[20..24].try_into().unwrap());
    assert!(size0 >= 2);
    bad[20..24].copy_from_slice(&(size0 - 1).to_le_bytes());
    std::fs::write(&file, &bad).unwrap();
    reopen_and_churn(&path, &mut rows, false, true, "化けた file");
    // 開き直した後はまた正しい file が残る (閉じた時に書き直す)。 high_water が違う (FNV は合わせ直す) = 別の時点の空き一覧
    let good = std::fs::read(&file).unwrap();
    let mut other = good[..good.len() - 4].to_vec();
    let hw = u32::from_le_bytes(other[8..12].try_into().unwrap());
    other[8..12].copy_from_slice(&(hw + 8).to_le_bytes());
    let crc = fnv1a(&other);
    other.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&file, &other).unwrap();
    reopen_and_churn(&path, &mut rows, false, false, "high_water の違う file");
    // 空きが次の空きに重なる (FNV は合わせ直す): 最初の空きの大きさを次の空きの先頭を越えるまで伸ばす
    let good = std::fs::read(&file).unwrap();
    let n = u32::from_le_bytes(good[12..16].try_into().unwrap());
    assert!(n >= 2, "空きが 2 つ以上ある前提");
    let mut over = good[..good.len() - 4].to_vec();
    let off0 = u32::from_le_bytes(over[16..20].try_into().unwrap());
    let off1 = u32::from_le_bytes(over[24..28].try_into().unwrap());
    over[20..24].copy_from_slice(&(off1 - off0 + 1).to_le_bytes());
    let crc = fnv1a(&over);
    over.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&file, &over).unwrap();
    reopen_and_churn(&path, &mut rows, false, false, "重なる空きの file");
    // 無い
    std::fs::remove_file(&file).unwrap();
    reopen_and_churn(&path, &mut rows, false, false, "file が無い");
    let _ = enchudb_engine::db_files::remove_db(&path);
}

#[test]
fn remove_db_removes_the_file() {
    let path = tmp("remove");
    let _rows = write_db(&path, 60);
    let file = PathBuf::from(&path).join(enchudb_engine::db_files::LEAFFREE);
    assert!(file.exists());
    enchudb_engine::db_files::remove_db(&path).unwrap();
    assert!(!file.exists() && !PathBuf::from(&path).exists());
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}
