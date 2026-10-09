//! #417: readonly の Engine が、 書き手の伸ばした Leaf 領域に置かれた値を空 (`Some(b"")`) で返した。
//!
//! readonly の写像は開いた時の file 長までで、 その先は予約のゼロ page。 `LeafStore::try_read` はゼロの
//! header を 「slot_size 0 / len 0 の旧形式 slot」 として `Ok(空)` を返していた。 今は写像に無い所は読み直しで、
//! readonly は Leaf の segment を `refresh` して取り込んでから読む。
//!
//! 書き手は同じ cell に毎回長さの違う値を張る (slot を使い回せず、 必ず領域の先に置く)。 修正前は 1.5 秒で
//! 空が 3,300 万回 (正しい値 6.8 万回)。 `tests_issue131` の retie storm は 8 種類の長さを回すだけで、 best-fit
//! が slot を使い回すので領域がほとんど伸びず、 ここを踏まなかった。

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[test]
fn readonly_reader_sees_values_in_grown_leaf_region() {
    let dir = std::env::temp_dir().join(format!("issue417_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap().to_string();
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    let hid = eng.himo_id("body").unwrap() as u16;
    let eid = eng.entity().unwrap();
    eng.tie_bytes_to_by_id(eid, hid, b"seed-value");
    eng.flush().unwrap();
    let ro = Arc::new(Engine::open_readonly(&path).unwrap());
    let eng = Arc::new(eng);
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (eng, stop) = (eng.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) && n < 200_000 {
                eng.tie_bytes_to_by_id(eid, hid, &vec![b'a'; 16 + (n % 50_000)]);
                n += 1;
            }
        })
    };
    let (mut ok, mut bad, mut none) = (0usize, 0usize, 0usize);
    let end = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < end {
        match ro.get_text_owned(eid, "body") {
            Some(b) if b == b"seed-value" || (b.len() >= 16 && b.iter().all(|&c| c == b'a')) => ok += 1,
            Some(_) => bad += 1,
            None => none += 1,
        }
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    eprintln!("readonly: 正しい値 {ok} / 書いていない値 {bad} / None {none}");
    assert_eq!(bad, 0, "書いていない値 (空) を返した");
    assert_eq!(none, 0, "値が在るのに None");
    assert!(ok > 0);
    drop((ro, eng));
    let _ = std::fs::remove_dir_all(&dir);
}

/// 並行の test は、 揃わない読みのたびに `refresh` が先回りで写像を伸ばすので、 ゼロの header を踏む窓が
/// 狭い (header の判定を外しても通る)。 ここは決定的に踏む: readonly を開いた後に書き手が領域の先へ
/// 値を置き、 readonly は 1 回だけ読む。 判定を外すと 1 回目が空を返す。
#[test]
fn readonly_first_read_past_its_mapping_is_not_empty() {
    let dir = std::env::temp_dir().join(format!("issue417_once_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.to_str().unwrap().to_string();
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    let hid = eng.himo_id("body").unwrap() as u16;
    let eid = eng.entity().unwrap();
    eng.tie_bytes_to_by_id(eid, hid, b"seed-value");
    eng.flush().unwrap();
    let ro = Engine::open_readonly(&path).unwrap();
    assert_eq!(ro.get_text_owned(eid, "body").as_deref(), Some(&b"seed-value"[..]), "前提");
    // readonly の写像の先まで Leaf 領域を伸ばす (大きな値を足していく)
    let big = vec![b'z'; 1 << 20];
    for _ in 0..64 {
        eng.tie_bytes_to_by_id(eid, hid, &big);
    }
    eng.flush().unwrap();
    let got = ro.get_text_owned(eid, "body");
    assert_eq!(got.as_ref().map(|b| b.len()), Some(big.len()), "伸ばした先の値を読めない (空 / None)");
    assert!(got.unwrap().iter().all(|&c| c == b'z'));
    drop((ro, eng));
    let _ = std::fs::remove_dir_all(&dir);
}
