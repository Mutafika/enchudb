//! #424: untie / delete は cell を 1 回の atomic store で消す。
//!
//! 旧: `Column::clear` が memset (`ptr::write_bytes`) で消していた。 macOS の memset は 4 B を 1 byte ずつ書くので、
//! 同じ cell を lock 無しで読む読み手が 「一部だけ 0」 = 一度も書いていない値を読んだ (Leaf では、 slot の境目からずれた
//! offset で別の slot の途中を読んだ)。
//!
//! 書き手が値を書いては外し (untie / delete)、 読み手 4 本が読み続けて、 書いた値と None 以外を数える。 旧実装での実測
//! (release、 M4 Max、 1 秒): untie / delete とも 3 回中 3 回、 1 回あたり数百万件 (読みのおよそ 5 %)。
//! 8 B の cell (Number64) も同じ修正だが、 macOS の memset は 8 B を 1 回で書くので旧実装でも読み手に見えず、 test を置いて
//! いない。 1 回で 4 B を書く memset (platform 次第) では、 ここの test も旧実装で落ちない。

use enchudb_engine::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const V: u64 = 0x1122_3344;

/// `cycle(eng, hid)` を回して、 その間 `current` の entity を読み続ける。 戻りは (want の値, None, 他の値)。
fn race(
    eng: Arc<Engine>,
    hid: u16,
    current: Arc<AtomicU64>,
    want: u64,
    cycle: impl Fn(&Engine, u16) + Send + 'static,
) -> (usize, usize, Vec<u64>) {
    let stop = Arc::new(AtomicBool::new(false));
    let w = {
        let (eng, stop) = (eng.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                cycle(&eng, hid);
            }
        })
    };
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (eng, current) = (eng.clone(), current.clone());
            std::thread::spawn(move || {
                let (mut hit, mut none, mut other) = (0usize, 0usize, Vec::new());
                let end = Instant::now() + Duration::from_secs(1);
                while Instant::now() < end {
                    match eng.get_by_id(current.load(Ordering::Relaxed), hid) {
                        Some(v) if v == want => hit += 1,
                        None => none += 1,
                        Some(v) => {
                            if other.len() < 4 {
                                other.push(v);
                            }
                        }
                    }
                }
                (hit, none, other)
            })
        })
        .collect();
    let (mut hit, mut none, mut other) = (0, 0, Vec::new());
    for r in readers {
        let (a, b, c) = r.join().unwrap();
        hit += a;
        none += b;
        other.extend(c);
    }
    stop.store(true, Ordering::Relaxed);
    w.join().unwrap();
    eprintln!("{want:#x}: {hit} / None {none} / 他の値 {:x?}", other);
    (hit, none, other)
}

fn growable(tag: &str) -> (std::path::PathBuf, Engine, u16) {
    let dir = std::env::temp_dir().join(format!("issue424_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut eng = Engine::create_growable(dir.to_str().unwrap()).unwrap();
    eng.define_himo("n", ValueType::Number, 0);
    let hid = eng.himo_id("n").unwrap() as u16;
    (dir, eng, hid)
}

#[test]
fn untie_never_shows_an_unwritten_value() {
    let (dir, eng, hid) = growable("untie");
    let e = eng.entity().unwrap();
    let (hit, _, other) = race(Arc::new(eng), hid, Arc::new(AtomicU64::new(e)), V, move |eng, hid| {
        eng.tie_to_by_id(e, hid, V);
        eng.untie_by_id(e, hid);
    });
    let _ = std::fs::remove_dir_all(&dir);
    assert!(hit > 0, "書いた値を一度も読めていない (前提が崩れた)");
    assert!(other.is_empty(), "untie の途中で書いていない値を読んだ: {other:x?}");
}

#[test]
fn delete_never_shows_an_unwritten_value() {
    let (dir, eng, hid) = growable("delete");
    let first = eng.entity().unwrap();
    let current = Arc::new(AtomicU64::new(first));
    let cur = current.clone();
    let (hit, _, other) = race(Arc::new(eng), hid, current, V, move |eng, hid| {
        let e = eng.entity().unwrap();
        eng.tie_to_by_id(e, hid, V);
        cur.store(e, Ordering::Relaxed);
        eng.delete(e);
    });
    let _ = std::fs::remove_dir_all(&dir);
    assert!(hit > 0, "書いた値を一度も読めていない (前提が崩れた)");
    assert!(other.is_empty(), "delete の途中で書いていない値を読んだ: {other:x?}");
}
