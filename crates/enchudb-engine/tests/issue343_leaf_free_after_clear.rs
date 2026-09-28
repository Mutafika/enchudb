//! #343: Leaf の untie / delete は **列を消してから** slot を返す。
//!
//! 旧: slot を返してから列を消していた。 その間に別の行の書き込みが同じ長さの slot を best-fit で
//! 再利用すると、 lock 無しの読み手は 「旧 offset → 別の行の値で gen が揃った slot → 列はまだ旧 offset」
//! と確定し、 別の行の値を返した (#119 は書き換えだけをこの順に直していた)。
//!
//! 各経路で、 x を書いては外す thread と、 y を同じ長さの別の値で書き換え続ける thread を回し、 x を
//! 読み続ける。 x の値か None 以外が返ったら落とす。 旧順序での実測 (release、 M2、 3 秒):
//! 読み手 4 本で 8 回ずつ、 untie 8/8 回 (1 回 4,931〜62,239 件)、 delete 8/8 回 (215〜12,825 件)。
//! consumer が当てる delete (`apply_op`) は engine の unit test (`issue343_apply_op_delete_clears_before_freeing_leaf`)。

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const X: &[u8] = &[b'x'; 64];

/// `churn(eng, hid)` を回して、 その間 `current` の entity を読み続ける。 戻りは (x の値, None, 他の値)。
fn race(eng: Arc<Engine>, current: Arc<AtomicU64>, churn: impl Fn(&Engine, u16) + Send + 'static) -> (usize, usize, usize) {
    let hid = eng.himo_id("body").unwrap() as u16;
    let y = eng.entity().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let c = {
        let (eng, stop) = (eng.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                churn(&eng, hid);
            }
        })
    };
    let w = {
        let (eng, stop) = (eng.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut n = 0u8;
            while !stop.load(Ordering::Relaxed) {
                eng.tie_bytes_to_by_id(y, hid, &[b'a' + n % 20; 64]);
                n = n.wrapping_add(1);
            }
        })
    };
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (eng, current) = (eng.clone(), current.clone());
            std::thread::spawn(move || {
                let (mut x, mut none, mut other) = (0, 0, 0);
                let end = Instant::now() + Duration::from_secs(3);
                while Instant::now() < end {
                    match eng.get_text_owned(current.load(Ordering::Relaxed), "body") {
                        Some(b) if b == X => x += 1,
                        None => none += 1,
                        Some(_) => other += 1,
                    }
                }
                (x, none, other)
            })
        })
        .collect();
    let (mut x, mut none, mut other) = (0, 0, 0);
    for r in readers {
        let (a, b, c) = r.join().unwrap();
        (x, none, other) = (x + a, none + b, other + c);
    }
    stop.store(true, Ordering::Relaxed);
    c.join().unwrap();
    w.join().unwrap();
    eprintln!("x {x} / None {none} / 他の値 {other}");
    (x, none, other)
}

fn growable(tag: &str) -> (std::path::PathBuf, Engine) {
    let dir = std::env::temp_dir().join(format!("issue343_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut eng = Engine::create_growable_opts(dir.to_str().unwrap(), GrowableOptions::default()).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    (dir, eng)
}

#[test]
fn untie_does_not_expose_another_rows_value() {
    let (dir, eng) = growable("untie");
    let x = eng.entity().unwrap();
    let (xs, _, other) = race(Arc::new(eng), Arc::new(AtomicU64::new(x)), move |eng, hid| {
        eng.tie_bytes_to_by_id(x, hid, X);
        eng.untie_by_id(x, hid);
    });
    assert!(xs > 0, "x を一度も読めていない (前提が崩れた)");
    assert_eq!(other, 0, "untie の途中で別の行の値を読んだ");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 旧順序での実測: 3 秒で数百〜数千回 (entity を作り直すので untie より少ない)。
#[test]
fn delete_does_not_expose_another_rows_value() {
    let (dir, eng) = growable("delete");
    let first = eng.entity().unwrap();
    let current = Arc::new(AtomicU64::new(first));
    let cur = current.clone();
    let (xs, _, other) = race(Arc::new(eng), current, move |eng, hid| {
        let e = eng.entity().unwrap();
        eng.tie_bytes_to_by_id(e, hid, X);
        cur.store(e, Ordering::Relaxed);
        eng.delete(e);
    });
    assert!(xs > 0, "x を一度も読めていない (前提が崩れた)");
    assert_eq!(other, 0, "delete の途中で別の行の値を読んだ");
    let _ = std::fs::remove_dir_all(&dir);
}
