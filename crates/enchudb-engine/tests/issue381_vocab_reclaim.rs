//! #381: 辞書の語の回収 (engine)。 番号 = 世代 | 場所、 参照 0 の場所をいちばん昔に空いたものから使い回す。

use enchudb_engine::{Engine, FaultKind, GrowableOptions, TieRejected, ValueType};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue381r-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn create(path: &str, reclaim: bool) -> Engine {
    let mut eng = Engine::create_growable_opts(
        path,
        GrowableOptions { max_entities: 200_000, vocab_reclaim: reclaim, ..Default::default() },
    )
    .unwrap();
    eng.define_himo("k", ValueType::Tag, 0);
    eng.define_himo("g", ValueType::Tag, 0);
    eng
}

/// 回収された値の古い番号で書くと、 別の値を書かずに `StaleValue` で断る。
#[test]
fn stale_vid_is_rejected() {
    let path = tmp("stale");
    let eng = create(&path, true);
    let k = eng.himo_id("k").unwrap() as u16;
    let a = eng.entity().unwrap();
    eng.try_tie_text_to_by_id(a, k, "old").unwrap();
    let old = eng.vocab_id("old").unwrap();
    eng.delete(a);
    let b = eng.entity().unwrap();
    eng.try_tie_text_to_by_id(b, k, "new").unwrap();
    let new = eng.vocab_id("new").unwrap();
    assert_ne!(new, old, "世代が進む");
    assert_eq!(new & 0x3FFF_FFFF, old & 0x3FFF_FFFF, "同じ場所を使い回す");
    assert_eq!(eng.vocab_id("old"), None);
    let c = eng.entity().unwrap();
    assert_eq!(
        eng.try_tie_to_by_id(c, k, old),
        Err(TieRejected::Fault(FaultKind::StaleValue)),
        "古い番号は書かない"
    );
    assert_eq!(eng.get(c, "k"), None);
    assert!(eng.fault_count(FaultKind::StaleValue) >= 1);
    assert_eq!(eng.get_text(b, "k"), Some(&b"new"[..]));
}

/// 既存の cell を数える処理と書き手が並行に走っても、 生きている値の語を回収しない (数え漏らすと、 使い回しで
/// 生きている行の値が消えるか別の値になる)。 数え終えた後も作っては消すを続けて、 使い回しを起こしてから確かめる。
#[test]
fn counting_concurrently_with_writers_keeps_live_values() {
    let path = tmp("concurrent");
    // 回収しない DB で既存の行を作り、 半分消してから回収する DB にする
    let mut survivors: HashMap<u64, (String, String)> = HashMap::new();
    {
        let eng = create(&path, false);
        let (k, g) = (eng.himo_id("k").unwrap() as u16, eng.himo_id("g").unwrap() as u16);
        for i in 0..40_000u32 {
            let e = eng.entity().unwrap();
            let (kv, gv) = (format!("pre{i}"), format!("grp{}", i % 97));
            eng.try_tie_text_to_by_id(e, k, &kv).unwrap();
            eng.try_tie_text_to_by_id(e, g, &gv).unwrap();
            if i % 2 == 0 {
                eng.delete(e);
            } else {
                survivors.insert(e, (kv, gv));
            }
        }
        eng.enable_vocab_reclaim().unwrap();
        let mut eng = eng;
        eng.flush().unwrap();
    }
    let eng = Arc::new(Engine::open_standalone(&path).unwrap());
    assert!(eng.vocab_usage().reclaim && !eng.vocab_usage().reclaim_ready);
    let (k, g) = (eng.himo_id("k").unwrap() as u16, eng.himo_id("g").unwrap() as u16);
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (eng, stop) = (eng.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut mine: Vec<(u64, String, String)> = Vec::new();
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) || i < 20_000 {
                    let e = eng.entity().unwrap();
                    let (kv, gv) = (format!("w{w}-{i}"), format!("grp{}", i % 97));
                    eng.try_tie_text_to_by_id(e, k, &kv).unwrap();
                    eng.try_tie_text_to_by_id(e, g, &gv).unwrap();
                    mine.push((e, kv, gv));
                    if mine.len() > 50 {
                        let (old, ..) = mine.remove(0);
                        eng.delete(old);
                    }
                    i += 1;
                }
                mine
            })
        })
        .collect();
    eng.build_vocab_refs();
    assert!(eng.vocab_usage().reclaim_ready);
    std::thread::sleep(std::time::Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    let mut live: Vec<(u64, String, String)> = writers.into_iter().flat_map(|h| h.join().unwrap()).collect();
    live.extend(survivors.into_iter().map(|(e, (kv, gv))| (e, kv, gv)));
    let u = eng.vocab_usage();
    assert!(u.reclaimed > 10_000, "使い回しが起きている: {u:?}");
    for (e, kv, gv) in &live {
        assert_eq!(eng.get_text(*e, "k"), Some(kv.as_bytes()), "{e} k");
        assert_eq!(eng.get_text(*e, "g"), Some(gv.as_bytes()), "{e} g");
        let vid = eng.vocab_id(kv).unwrap_or_else(|| panic!("{kv} が引けない"));
        assert_eq!(eng.pull_raw("k", vid), vec![*e], "{kv}");
    }
}

/// 書き込みの queue (`tie_text_async`) でも、 積んでから適用するまでの間に番号を回収しない (積む時に押さえ、 適用した
/// 後で返す)。 値を繰り返し使い、 参照 0 になった語を別の値が使い回す時に、 その語の番号を積んだ op が queue に残って
/// いる形にする。 消すのは書き込みを適用済みの行だけ (`delete` は同期、 `tie_text_async` は後から適用されるので、
/// 適用前の行を消すと消した後に値が書かれる)。
#[test]
fn queued_writes_keep_their_vids_until_applied() {
    let path = tmp("async");
    let eng = create(&path, true);
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 64 << 20).unwrap();
    let mut applied: std::collections::VecDeque<(u64, String)> = std::collections::VecDeque::new();
    let mut queued: Vec<(u64, String)> = Vec::new();
    for i in 0..60_000u32 {
        let e = eng.entity().unwrap();
        // 300 種の値を回す + 一意な値を混ぜる (回収と使い回しを常に起こす)
        let v = if i % 3 == 0 { format!("u{i}") } else { format!("r{}", i % 300) };
        eng.tie_text_async(e, "k", &v);
        queued.push((e, v));
        if queued.len() == 500 {
            eng.flush_writes();
            applied.extend(queued.drain(..));
        }
        // 適用済みの行を古い順に消す (queue には常に 500 件まで積まれている)
        while applied.len() > 200 {
            let (old, _) = applied.pop_front().unwrap();
            eng.delete(old);
        }
    }
    eng.flush_writes();
    applied.extend(queued.drain(..));
    let u = eng.vocab_usage();
    assert!(u.reclaimed > 10_000, "{u:?}");
    assert!(u.entries < 2_000, "{u:?}");
    for (e, v) in &applied {
        assert_eq!(eng.get_text(*e, "k"), Some(v.as_bytes()), "{e}");
    }
    assert_eq!(eng.fault_count(FaultKind::StaleValue), 0, "押さえていれば古い番号で書かない");
}
