//! #131: Leaf の値の読み (`get_text_owned`) のやり直しに上限が無かった。
//!
//! #128 で 「進捗の無い連敗だけ数える」 形にしたので、 書き手が同じ cell を書き換え続ける限り
//! 数えが 0 に戻り、 原理的には読みが返らない。 今は揃った版を決まった回数掴めなかったら、
//! 書き手より先に行の lock を握って 1 回で読む (握っている間は column も slot も動かない)。
//!
//! 実際に握るのは稀 (row lock (#135) で同じ cell の書き手は 1 本ずつになり、 揃った版を 64 回続けて
//! 掴めない読みは 9600 万回に 1 回)。 crate 内の test では 1 回で握りに行く (`cfg!(test)`) ので、 ここは
//! crate 内に置いて握る経路を毎回通す。
//!
//! 無制限のまま返らない読みは手元では再現していない (#131 のコメント: CPU 4 倍の奪い合いでも
//! 最大 72 回で抜けた)。 ここで固定するのは、 握って読む経路が書き手と重なっても値を落とさない /
//! 化けないこと (lock 無しにすると None が出る)。 書き手より先に握ること、 別の行を握っている thread は
//! 読みのために握らないことは `row_lock` の test。

use crate::{Engine, GrowableOptions, ValueType};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn leaf_read_under_retie_storm_returns_a_written_value_in_bounded_time() {
    let dir = std::env::temp_dir().join(format!("issue131_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap().to_string();
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    let hid = eng.himo_id("body").unwrap() as u16;
    let eid = eng.entity().unwrap();
    // 長さの違う値 (同じ slot の再利用と別 slot への移動の両方を起こす)
    let bodies: Vec<Vec<u8>> = (0..8).map(|i| vec![b'a' + i as u8; 16 + i * 40]).collect();
    eng.tie_bytes_to_by_id(eid, hid, &bodies[0]);
    let eng = Arc::new(eng);

    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let writers: Vec<_> = (0..cores * 2)
        .map(|w| {
            let (eng, stop, writes, bodies) = (eng.clone(), stop.clone(), writes.clone(), bodies.clone());
            std::thread::spawn(move || {
                let mut n = w;
                while !stop.load(Ordering::Relaxed) {
                    eng.tie_bytes_to_by_id(eid, hid, &bodies[n % bodies.len()]);
                    n += 1;
                    writes.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..cores)
        .map(|_| {
            let (eng, bodies) = (eng.clone(), bodies.clone());
            std::thread::spawn(move || {
                let (mut reads, mut missing, mut corrupt) = (0usize, 0usize, 0usize);
                let mut slowest = Duration::ZERO;
                let end = Instant::now() + Duration::from_millis(2000);
                while Instant::now() < end {
                    let t = Instant::now();
                    match eng.get_text_owned(eid, "body") {
                        Some(b) if bodies.contains(&b) => {}
                        Some(_) => corrupt += 1,
                        None => missing += 1,
                    }
                    slowest = slowest.max(t.elapsed());
                    reads += 1;
                }
                (reads, missing, corrupt, slowest)
            })
        })
        .collect();

    let results: Vec<_> = readers.into_iter().map(|r| r.join().unwrap()).collect();
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    let reads: usize = results.iter().map(|r| r.0).sum();
    let missing: usize = results.iter().map(|r| r.1).sum();
    let corrupt: usize = results.iter().map(|r| r.2).sum();
    let slowest = results.iter().map(|r| r.3).max().unwrap();
    eprintln!(
        "reads {reads} / writes {} / missing {missing} / corrupt {corrupt} / 最も遅い読み {slowest:?}",
        writes.load(Ordering::Relaxed)
    );
    assert!(writes.load(Ordering::Relaxed) > 0 && reads > 0);
    assert_eq!(missing, 0, "値が在るのに None");
    assert_eq!(corrupt, 0, "書いていない bytes を読んだ");
    // 読み 1 回が書き手の数によらず短く返る (握って読むのは今の書き手 1 本を待つだけ)
    assert!(slowest < Duration::from_secs(1), "読み 1 回に {slowest:?}");
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
