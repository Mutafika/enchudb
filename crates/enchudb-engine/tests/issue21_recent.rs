//! #21: 「直近 N 件」 を table の大きさによらずに取る (`Engine::recent_by_id` / `Engine::recent`)。
//!
//! `entities_with_himo` は列を先頭から全部なめる (O(table))。 `recent_by_id` は table の払い出し済みの末尾から
//! 逆にたどり、 `n` 件そろった所で止まる。

use enchudb_engine::{Engine, ValueType};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue21-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".positions"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// table `posts` (列 `ts` / `body`) を持つ standalone の engine。 戻り値は (engine, ts の id, body の id)。
fn posts(tag: &str, size_hint: u32) -> (String, Engine, u16, u16) {
    let path = tmp(tag);
    cleanup(&path);
    let mut eng = Engine::create_standalone(&path).expect("create");
    eng.define_table("posts", size_hint).unwrap();
    let ts = eng.define_himo_in("posts", "ts", ValueType::Number, 0).unwrap() as u16;
    let body = eng.define_himo_in("posts", "body", ValueType::Tag, 0).unwrap() as u16;
    (path, eng, ts, body)
}

/// row を 1 つ足す (ts だけ張る)。
fn add(eng: &Engine, table: &str, ts_hid: u16, ts: u32) -> u64 {
    let e = eng.entity_in(table).unwrap();
    eng.tie_to_by_id(e, ts_hid, ts);
    e
}

#[test]
fn newest_first_and_stops_at_n() {
    let (path, eng, ts, _body) = posts("order", 1000);
    let rows: Vec<u64> = (0..100).map(|i| add(&eng, "posts", ts, i)).collect();

    let want: Vec<u64> = rows.iter().rev().take(10).copied().collect();
    assert_eq!(eng.recent_by_id(ts, 10), want);
    assert_eq!(eng.recent("posts.ts", 10), want, "名前版も同じ");
    // 全部より多く頼んだら全部 (新しい順)
    let all: Vec<u64> = rows.iter().rev().copied().collect();
    assert_eq!(eng.recent_by_id(ts, 100), all);
    assert_eq!(eng.recent_by_id(ts, 1_000_000), all);
    assert_eq!(eng.recent_by_id(ts, 1), vec![*rows.last().unwrap()]);
    assert_eq!(eng.recent_by_id(ts, 0), Vec::<u64>::new());
    // 全走査の列挙 (eid の昇順) を逆にしたものと一致する
    let mut scan = eng.entities_with_himo(ts);
    scan.reverse();
    assert_eq!(scan, all);

    // 未定義の列 / 範囲外の id は空
    assert_eq!(eng.recent("posts.nope", 10), Vec::<u64>::new());
    assert_eq!(eng.recent_by_id(9999, 10), Vec::<u64>::new());
    drop(eng);
    cleanup(&path);
}

#[test]
fn skips_deleted_rows_and_rows_without_the_column() {
    let (path, eng, ts, body) = posts("skip", 1000);
    let rows: Vec<u64> = (0..20).map(|i| add(&eng, "posts", ts, i)).collect();
    // 一番新しい 3 つのうち: 19 は削除、 18 は ts を外す (row は残る)、 17 はそのまま
    eng.delete(rows[19]);
    eng.untie_by_id(rows[18], ts);
    eng.tie_text_to_by_id(rows[18], body, "still here");
    // body だけ持つ row (ts 無し) を末尾に足す
    let only_body = eng.entity_in("posts").unwrap();
    eng.tie_text_to_by_id(only_body, body, "no ts");

    assert_eq!(eng.recent_by_id(ts, 3), vec![rows[17], rows[16], rows[15]]);
    // 列ごとに見る: body を持つ row は新しい順に only_body、 18
    assert_eq!(eng.recent_by_id(body, 5), vec![only_body, rows[18]]);

    // 削除してから足した row は末尾に来る (枠を使い切るまでは、 空いた eid を使い直さない)
    let after = add(&eng, "posts", ts, 99);
    assert_eq!(eng.recent_by_id(ts, 2), vec![after, rows[17]]);
    drop(eng);
    cleanup(&path);
}

#[test]
fn scoped_to_the_table_of_the_column() {
    let path = tmp("scope");
    cleanup(&path);
    let mut eng = Engine::create_standalone(&path).expect("create");
    eng.define_table("posts", 100).unwrap();
    eng.define_table("likes", 100).unwrap();
    let p_ts = eng.define_himo_in("posts", "ts", ValueType::Number, 0).unwrap() as u16;
    let l_ts = eng.define_himo_in("likes", "ts", ValueType::Number, 0).unwrap() as u16;
    let (mut ps, mut ls) = (Vec::new(), Vec::new());
    for i in 0..30 {
        ps.push(add(&eng, "posts", p_ts, i));
        ls.push(add(&eng, "likes", l_ts, i));
        ls.push(add(&eng, "likes", l_ts, i));
    }
    assert_eq!(eng.recent_by_id(p_ts, 5), ps.iter().rev().take(5).copied().collect::<Vec<_>>());
    assert_eq!(eng.recent_by_id(l_ts, 5), ls.iter().rev().take(5).copied().collect::<Vec<_>>());
    assert_eq!(eng.recent_by_id(p_ts, 1000).len(), 30);
    assert_eq!(eng.recent_by_id(l_ts, 1000).len(), 60);
    drop(eng);
    cleanup(&path);
}

/// 枠を足した table (extent が 2 本以上、 間に他の table の eid が挟まる) でも、 払い出しの逆順にたどる。
/// この時、 戻り値は eid の降順にならない (後から足した extent の方が eid は大きいとは限らないが、 ここでは大きい)。
#[test]
fn walks_back_across_extents() {
    let path = tmp("extents");
    cleanup(&path);
    let mut eng = Engine::create_standalone(&path).expect("create");
    eng.define_table("posts", 8).unwrap();
    eng.define_table("likes", 8).unwrap();
    let p_ts = eng.define_himo_in("posts", "ts", ValueType::Number, 0).unwrap() as u16;
    let l_ts = eng.define_himo_in("likes", "ts", ValueType::Number, 0).unwrap() as u16;
    let cap = eng.table_eid_usage("posts").unwrap().capacity;

    // posts の枠を使い切って、 さらに足す (自動で extent が切り足される)
    let mut ps = Vec::new();
    for i in 0..cap + 5 {
        ps.push(add(&eng, "posts", p_ts, i));
        if i % 3 == 0 {
            add(&eng, "likes", l_ts, i);
        }
    }
    assert!(eng.table_eid_extents("posts").unwrap().len() >= 2, "extent が増えていない (test の前提)");

    let all: Vec<u64> = ps.iter().rev().copied().collect();
    assert_eq!(eng.recent_by_id(p_ts, 3), all[..3]);
    // extent の境目をまたぐ件数
    assert_eq!(eng.recent_by_id(p_ts, 7), all[..7]);
    assert_eq!(eng.recent_by_id(p_ts, ps.len()), all);
    drop(eng);
    cleanup(&path);
}

/// table に属さない列 (`define_himo` + `entity()`) も、 削除が無ければ払い出しの逆順。
#[test]
fn column_outside_any_table() {
    let path = tmp("anon");
    cleanup(&path);
    let mut eng = Engine::create_standalone(&path).expect("create");
    eng.define_himo("age", ValueType::Number, 0);
    let rows: Vec<u64> = (0..50u32)
        .map(|i| {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", i);
            e
        })
        .collect();
    assert_eq!(eng.recent("age", 4), rows.iter().rev().take(4).copied().collect::<Vec<_>>());
    assert_eq!(eng.recent("age", 500).len(), 50);

    // 後から table を切っても、 元の列は元の範囲 (table の row は混ざらない)
    eng.define_table("posts", 100).unwrap();
    let p_ts = eng.define_himo_in("posts", "ts", ValueType::Number, 0).unwrap() as u16;
    let p = add(&eng, "posts", p_ts, 1);
    assert_eq!(eng.recent("age", 4), rows.iter().rev().take(4).copied().collect::<Vec<_>>());
    assert_eq!(eng.recent_by_id(p_ts, 4), vec![p]);
    drop(eng);
    cleanup(&path);
}

/// 何も注入しない版: 3 つの table に、 足す / 消す / 列を外す を混ぜて流し、 毎回 「足した順の記録から、 今も値を
/// 持つ row を新しい順に」 と突き合わせる (枠は使い切らない = 空いた eid を使い直さない範囲)。
#[test]
fn matches_the_insertion_log_under_mixed_writes() {
    let path = tmp("model");
    cleanup(&path);
    let mut eng = Engine::create_standalone(&path).expect("create");
    let names = ["a", "b", "c"];
    let mut hids = Vec::new();
    for n in names {
        eng.define_table(n, 4096).unwrap();
        hids.push(eng.define_himo_in(n, "ts", ValueType::Number, 0).unwrap() as u16);
    }
    // table ごとの (eid, 今も ts を持つか)、 足した順
    let mut log: Vec<Vec<(u64, bool)>> = vec![Vec::new(); 3];
    let mut x = 0x2545_F491_4F6C_DD1Du64;
    let mut rnd = |m: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % m
    };
    for step in 0..3000u32 {
        let t = rnd(3) as usize;
        match rnd(10) {
            0..=6 => log[t].push((add(&eng, names[t], hids[t], step), true)),
            7 | 8 if !log[t].is_empty() => {
                let i = rnd(log[t].len() as u64) as usize;
                eng.delete(log[t][i].0);
                log[t][i].1 = false;
            }
            _ if !log[t].is_empty() => {
                let i = rnd(log[t].len() as u64) as usize;
                eng.untie_by_id(log[t][i].0, hids[t]);
                log[t][i].1 = false;
            }
            _ => {}
        }
        if step % 37 == 0 {
            for t in 0..3 {
                let n = 1 + rnd(40) as usize;
                let want: Vec<u64> = log[t].iter().rev().filter(|r| r.1).map(|r| r.0).take(n).collect();
                assert_eq!(eng.recent_by_id(hids[t], n), want, "step {step} table {} n {n}", names[t]);
            }
        }
    }
    for t in 0..3 {
        let want: Vec<u64> = log[t].iter().rev().filter(|r| r.1).map(|r| r.0).collect();
        assert!(want.len() > 100, "test が row をほとんど残していない");
        assert_eq!(eng.recent_by_id(hids[t], usize::MAX), want);
    }
    drop(eng);
    cleanup(&path);
}

/// doc に書いた但し書きの実物: 枠を使い切った table は、 削除済みの eid があればそれを使い直す (無ければ枠を
/// 足す)。 使い直された row は一番新しいのに、 元の位置 (古い側) に出る — 払い出しの順 ≠ 書き込みの順。
/// 直す対象ではなく、 今の挙動の記録。
#[test]
fn reused_eid_after_the_table_is_full_is_not_newest() {
    let (path, eng, ts, _body) = posts("reuse", 8);
    let cap = eng.table_eid_usage("posts").unwrap().capacity;
    let rows: Vec<u64> = (0..cap).map(|i| add(&eng, "posts", ts, i)).collect();

    // 枠を使い切る前の削除 + 追加は末尾に足す (上の test)。 使い切った後は、 空いた eid を使い直す
    eng.delete(rows[2]);
    let reborn = add(&eng, "posts", ts, 999);
    assert_eq!(reborn, rows[2], "枠を使い切った table が、 空いた eid を使い直さなかった (test の前提)");

    let got = eng.recent_by_id(ts, cap as usize);
    assert_eq!(got.len(), cap as usize);
    assert_ne!(got[0], reborn);
    assert_eq!(got.iter().position(|&e| e == reborn), Some(cap as usize - 1 - 2));
    drop(eng);
    cleanup(&path);
}
