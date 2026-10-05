//! #378: 辞書に同じ値を複数の thread が同時に入れても、 番号 (と data) は値 1 つにつき 1 つだけ使う。 旧: 番号と data を
//! 取ってから索引に入れていたので、 索引に入れられなかった (同じ値が先に入った) 側の番号と data が残った (値の 4〜5 倍)。

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use std::sync::{Arc, Barrier};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue378-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

const THREADS: u32 = 8;
const VALUES: u32 = 12_000;

/// issue の再現: 8 thread がそれぞれ 12,000 個の値を、 同じ値を 143 歩ずつずれて追う順で書く
/// (`k = (7i + 1001t) mod 12000`)。 辞書の語数は値の数ちょうど、 同じ値は全 thread で同じ番号。
fn round(reclaim: bool) {
    let path = tmp(if reclaim { "reclaim" } else { "plain" });
    let mut eng = Engine::create_growable_opts(
        &path,
        GrowableOptions { max_entities: 200_000, vocab_reclaim: reclaim, ..Default::default() },
    )
    .unwrap();
    eng.define_table("t", 150_000).unwrap();
    eng.define_himo_in("t", "v", ValueType::Tag, 0).unwrap();
    let before = eng.vocab_usage().entries;
    let eng = Arc::new(eng);
    let hid = eng.himo_id("t.v").unwrap() as u16;
    let start = Arc::new(Barrier::new(THREADS as usize));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let (eng, start) = (eng.clone(), start.clone());
            std::thread::spawn(move || {
                let rows: Vec<u64> = (0..VALUES).map(|_| eng.entity_in("t").unwrap()).collect();
                start.wait();
                for i in 0..VALUES {
                    let k = (7 * i + 1001 * t) % VALUES;
                    eng.tie_text_to_by_id(rows[i as usize], hid, &format!("value-{k}"));
                }
                rows
            })
        })
        .collect();
    let rows: Vec<Vec<u64>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let used = eng.vocab_usage().entries - before;
    assert_eq!(used, VALUES, "reclaim {reclaim}: 値 {VALUES} 個に番号 {used} 個");
    // 書いた値はどれも読め、 同じ値は同じ番号
    for (t, rs) in rows.iter().enumerate() {
        for (i, &e) in rs.iter().enumerate().step_by(97) {
            let k = (7 * i as u32 + 1001 * t as u32) % VALUES;
            let want = format!("value-{k}");
            assert_eq!(eng.get_text_owned(e, "t.v").as_deref(), Some(want.as_bytes()));
            assert_eq!(eng.get(e, "t.v").map(|v| v as u32), eng.vocab_id(&want));
        }
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn racing_writers_use_one_number_per_value() {
    round(false);
}

/// 回収する辞書は負けた番号も (参照 0 なので) いずれ使い回すため、 取りこぼしが語数に出にくい (直す前で 5 回中 1 回)。
/// 5 回繰り返す。
#[test]
fn racing_writers_use_one_number_per_value_with_reclaim() {
    for _ in 0..5 {
        round(true);
    }
}

/// 辞書が一杯で番号を取れなかった書き込みは、 索引に置いた 「入れている途中」 を空に戻す。 戻さないと、 同じ所に
/// 入れようとする次の書き込みが確定を待ち続ける。
#[test]
fn a_rejected_insert_leaves_no_pending_slot() {
    let path = tmp("full");
    let mut eng = Engine::create_growable_opts(
        &path,
        GrowableOptions { max_entities: 10_000, vocab_max_entries: Some(64), ..Default::default() },
    )
    .unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "v", ValueType::Tag, 0).unwrap();
    let eng = Arc::new(eng);
    let hid = eng.himo_id("t.v").unwrap() as u16;
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = {
        let eng = eng.clone();
        std::thread::spawn(move || {
            let mut rejected = 0;
            // 一杯まで入れ、 その先は断られる。 断られた値をもう一度入れても、 入っている値を入れても止まらない
            for _ in 0..3 {
                for k in 0..200u32 {
                    let e = eng.entity_in("t").unwrap();
                    if eng.try_tie_text_to_by_id(e, hid, &format!("v{k}")).is_err() {
                        rejected += 1;
                    }
                }
            }
            tx.send(rejected).unwrap();
        })
    };
    let rejected = rx.recv_timeout(std::time::Duration::from_secs(30)).expect("断られた書き込みの後で次の書き込みが止まった");
    worker.join().unwrap();
    assert!(rejected > 0, "前提: 辞書が一杯になって断られる");
    let usage = eng.vocab_usage();
    assert!(usage.entries <= usage.max_entries, "{usage:?}");
    assert!(eng.vocab_id("v0").is_some());
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
