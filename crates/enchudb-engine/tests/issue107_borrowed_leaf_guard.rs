//! #107: 借用を返す text 読み (`get_text` / `get_content` / `get_entity`) の番人。
//!
//! 借用は live mmap を指すので、 返した後に書き手が同じ slot を書き換える / 再利用すると手元の
//! `&[u8]` の中身が変わる (#106 の torn read / 範囲外 panic)。 copy しない以上 seqlock では守れない。
//! engine は 「どちらを呼ぶか」 を型で強制できないので、 debug build で止める:
//!
//!  - `open_readonly` (= 別 process の書き手と共存する開き方) の Leaf の借用
//!  - 借用の読みが始まった後に、 **別の thread** が Leaf を書き換えた engine での Leaf の借用
//!
//! 止めないもの (ここが広すぎると README の基本の使い方が debug build で動かなくなる):
//!
//!  - 1 本の thread で書いて読む (concurrent な engine でも)
//!  - 書き終えてから、 複数の thread で読むだけ
//!  - Tag (共有辞書は追記だけで、 借用が指す先は動かない)
//!
//! 番人は `debug_assert` 相当で、 release build では何もしない。 この file は debug build でだけ回る。
#![cfg(debug_assertions)]

use enchudb_engine::{Engine, EntityValue, EntityValueOwned, ValueType};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue107-{}-{}-{}",
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

/// build phase 相当: Arc 単一所有のうちに define。
fn define(eng: &Arc<Engine>, name: &str, vt: ValueType) -> u16 {
    let eng_mut = unsafe { &mut *(Arc::as_ptr(eng) as *mut Engine) };
    eng_mut.define_himo(name, vt, 0);
    eng.himo_id(name).unwrap() as u16
}

/// 番人が止めたか (panic の文面まで見る: 別の理由の panic を 「止めた」 と数えない)。
fn stopped<R>(f: impl FnOnce() -> R) -> bool {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(_) => false,
        Err(e) => {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            assert!(msg.contains("#107"), "番人でない panic: {msg}");
            true
        }
    }
}

/// 番人の panic だけ stderr に出さない (この binary の中だけ)。 他の panic (assert の失敗) は元の hook へ。
fn quiet() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let p = info.payload();
            let msg = p.downcast_ref::<String>().map(|s| s.as_str()).or_else(|| p.downcast_ref::<&str>().copied());
            if !msg.is_some_and(|m| m.contains("#107")) {
                default(info);
            }
        }));
    });
}

/// concurrent な engine + Leaf 列 `body` / Tag 列 `city`、 entity 1 つに両方張った状態。
fn concurrent_with_row(tag: &str) -> (String, Arc<Engine>, u16, u16, u64) {
    let path = tmp(tag);
    cleanup(&path);
    let eng: Arc<Engine> = Engine::create_concurrent(&path).expect("create");
    let body = define(&eng, "body", ValueType::Leaf);
    let city = define(&eng, "city", ValueType::Tag);
    let e = eng.entity().unwrap();
    eng.tie_text_to_by_id(e, body, "v0");
    eng.tie_text_to_by_id(e, city, "Tokyo");
    (path, eng, body, city, e)
}

// ───────── 止めないもの ─────────

/// README の基本の使い方: 1 本の thread で書いて借用で読む、 を concurrent な engine で繰り返す。
#[test]
fn single_thread_write_then_borrow_is_allowed_on_concurrent_engine() {
    let (path, eng, body, _city, e) = concurrent_with_row("single");
    for i in 0..50 {
        let v = format!("value-{i}-{}", "x".repeat(i * 7));
        eng.tie_text_to_by_id(e, body, &v);
        eng.content(e, "memo", v.as_bytes());
        assert_eq!(eng.get_text(e, "body"), Some(v.as_bytes()));
        assert_eq!(eng.get_content(e, "memo"), Some(v.as_bytes()));
        let fields = eng.get_entity(e);
        assert!(fields.iter().any(|(n, val)| *n == "body" && *val == EntityValue::Text(v.as_bytes())));
    }
    drop(eng);
    cleanup(&path);
}

/// #414: 書き換えの旧い slot は、 consumer の周期の書き出し (oplog の DB) の後で consumer の thread が空きに戻す。
/// cell を付け替えたのは読み手と同じ thread なので、 1 本の thread で書いて読む使い方は止めない。
#[test]
fn old_slots_released_by_the_consumer_do_not_stop_a_single_thread() {
    let path = tmp("released");
    cleanup(&path);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&path, 4 << 20).expect("create");
    let body = define(&eng, "body", ValueType::Leaf);
    let e = eng.entity().unwrap();
    eng.tie_text_to_by_id(e, body, "value-000");
    let before = eng.leaf_footprint().unwrap();
    for i in 1..=5 {
        let v = format!("value-{i:03}");
        eng.tie_text_to_by_id(e, body, &v);
        // consumer の周期の書き出し (100 ms ごと) を待つ。 その後で consumer の thread が旧い slot を空きに戻す
        std::thread::sleep(std::time::Duration::from_millis(250));
        assert_eq!(eng.get_text(e, "body"), Some(v.as_bytes()));
    }
    // consumer が空きに戻していれば、 同じ長さの書き換えは空いた slot を使い回す (Leaf 領域は 1 slot 分までしか
    // 伸びない)。 戻していなければ 5 slot 分 (24 B × 5) 伸びる
    let grown = eng.leaf_footprint().unwrap() - before;
    assert!(grown < 48, "旧い slot が空きに戻っていない (伸び {grown} B) — consumer が空きに戻す経路を通っていない");
    drop(eng);
    cleanup(&path);
}

/// 書き終えてから複数の thread で読むだけ (読み手が何本でも、 書き手が居なければ借用は動かない)。
#[test]
fn many_reader_threads_after_writes_finished_are_allowed() {
    let (path, eng, _body, _city, e) = concurrent_with_row("readers");
    let hs: Vec<_> = (0..4)
        .map(|_| {
            let eng = eng.clone();
            std::thread::spawn(move || {
                for _ in 0..2000 {
                    assert_eq!(eng.get_text(e, "body"), Some(b"v0".as_ref()));
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    drop(eng);
    cleanup(&path);
}

/// standalone (`&mut self` で書く) は書いて読むを繰り返しても止めない。
#[test]
fn standalone_borrow_is_allowed() {
    let path = tmp("standalone");
    cleanup(&path);
    let mut eng = Engine::create_standalone(&path).expect("create");
    eng.define_himo("body", ValueType::Leaf, 0);
    let e = eng.entity().unwrap();
    for i in 0..20 {
        let v = format!("v{i}");
        eng.tie_text(e, "body", &v);
        eng.content(e, "memo", v.as_bytes());
        assert_eq!(eng.get_text(e, "body"), Some(v.as_bytes()));
        assert_eq!(eng.get_content(e, "memo"), Some(v.as_bytes()));
        assert!(!eng.get_entity(e).is_empty());
    }
    drop(eng);
    cleanup(&path);
}

// ───────── 止めるもの: 別の thread の書き手 ─────────

/// 借用で読んだ後に、 別の thread が Leaf を書き換えたら、 次の借用の読みを止める。
/// 書き換えの 3 つの形 (新しい値を置くだけ / 外すだけ / 置き換え) のどれでも。
#[test]
fn borrow_after_another_thread_wrote_is_stopped() {
    quiet();
    type Write = fn(&Engine, u16, u64);
    let cases: [(&str, Write); 3] = [
        // 別の entity に初めて張る = slot を置くだけ (何も返さない)
        ("insert", |eng, body, _e| {
            let other = eng.entity().unwrap();
            eng.tie_text_to_by_id(other, body, "other");
        }),
        // 外す = slot を返すだけ
        ("free", |eng, body, e| eng.untie_by_id(e, body)),
        // 張り直す = 置いて返す
        ("retie", |eng, body, e| eng.tie_text_to_by_id(e, body, "v1-longer-than-before")),
    ];
    for (name, write) in cases {
        let (path, eng, body, _city, e) = concurrent_with_row(name);
        // 書き換えられない方の entity (外すだけの case では、 外した cell は借用の読みまで行かず None で返る。
        // 番人が見るのは 「Leaf の置き場を借用で読む」 ことなので、 値の残る cell で確かめる)
        let probe = eng.entity().unwrap();
        eng.tie_text_to_by_id(probe, body, "p0");
        // 借用の読みが始まる (ここまでは 1 本の thread)
        assert_eq!(eng.get_text(probe, "body"), Some(b"p0".as_ref()), "{name}");
        let eng2 = eng.clone();
        std::thread::spawn(move || write(&eng2, body, e)).join().unwrap();

        assert!(stopped(|| eng.get_text(probe, "body").map(|b| b.len())), "{name}: get_text が止まらない");
        // Tag は止めない (追記だけの共有辞書)
        assert_eq!(eng.get_text(e, "city"), Some(b"Tokyo".as_ref()), "{name}");
        // copy 版は読める
        assert_eq!(eng.get_text_owned(probe, "body").as_deref(), Some(b"p0".as_ref()), "{name}");
        let want: Option<&[u8]> = match name {
            "free" => None,
            "retie" => Some(b"v1-longer-than-before"),
            _ => Some(b"v0"),
        };
        assert_eq!(eng.get_text_owned(e, "body").as_deref(), want, "{name}");
        // 外した cell は、 借用版でも置き場まで行かずに None (止める対象でない)
        if name == "free" {
            assert_eq!(eng.get_text(e, "body"), None);
        }
        drop(eng);
        cleanup(&path);
    }
}

/// `get_content` / `get_entity` も同じ経路 (同じ slot の借用) なので同じく止める。
#[test]
fn get_content_and_get_entity_are_stopped_too() {
    quiet();
    let (path, eng, _body, _city, e) = concurrent_with_row("others");
    eng.content(e, "memo", b"m0");
    assert_eq!(eng.get_content(e, "memo"), Some(b"m0".as_ref()));
    assert!(!eng.get_entity(e).is_empty());

    let eng2 = eng.clone();
    std::thread::spawn(move || eng2.content(e, "memo", b"m1")).join().unwrap();

    assert!(stopped(|| eng.get_content(e, "memo").map(|b| b.len())), "get_content が止まらない");
    assert!(stopped(|| eng.get_entity(e).len()), "get_entity が止まらない");
    assert_eq!(eng.get_content_owned(e, "memo").as_deref(), Some(b"m1".as_ref()));
    let owned = eng.get_entity_owned(e);
    assert!(owned.iter().any(|(n, v)| *n == "body" && *v == EntityValueOwned::Text(b"v0".to_vec())));
    assert!(owned.iter().any(|(n, v)| *n == "city" && *v == EntityValueOwned::Text(b"Tokyo".to_vec())));
    drop(eng);
    cleanup(&path);
}

/// 読み手が 2 本居て、 その片方が書いた: もう片方の借用が危ないので止める
/// (「最初に借用した thread と書き手が同じ」 だけを見ていると見逃す)。
#[test]
fn write_by_one_of_two_borrowing_threads_is_stopped() {
    quiet();
    let (path, eng, body, _city, e) = concurrent_with_row("two-readers");
    // 1 本目の読み手 = この thread
    assert_eq!(eng.get_text(e, "body"), Some(b"v0".as_ref()));
    // 2 本目の読み手
    let eng2 = eng.clone();
    std::thread::spawn(move || {
        assert_eq!(eng2.get_text(e, "body"), Some(b"v0".as_ref()));
    })
    .join()
    .unwrap();
    // 1 本目が書く (2 本目が借用を持っているかもしれない)
    eng.tie_text_to_by_id(e, body, "v1");
    assert!(stopped(|| eng.get_text(e, "body").map(|b| b.len())));
    drop(eng);
    cleanup(&path);
}

/// 前提を手で並べない版: 読み手 4 本が借用で読み続け、 書き手 1 本が張り直し続ける (#106 の形)。
/// 番人が無いと壊れた中身 / 範囲外 panic が出る読み方 (release build の実測: 100 万回の書き換えで
/// 壊れた中身 487 回 + 範囲外 panic 149 回)。 debug build では読み手が全員、 番人に止められる。
#[test]
fn readers_borrowing_while_a_writer_runs_are_all_stopped() {
    quiet();
    const READERS: usize = 4;
    let (path, eng, body, _city, e) = concurrent_with_row("rww");
    let started = Arc::new(Barrier::new(READERS + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let stopped_readers = Arc::new(AtomicU64::new(0));

    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let (eng, started, stop, stopped_readers) =
                (eng.clone(), started.clone(), stop.clone(), stopped_readers.clone());
            std::thread::spawn(move || {
                // 借用の読みが始まってから書き手を走らせる (書き手が先に終わると、 読むだけの engine と同じ)。
                // ここで panic しても barrier には必ず着く (着かないと test が止まったままになる)
                let first = catch_unwind(AssertUnwindSafe(|| eng.get_text(e, "body").map(|b| b.len())));
                started.wait();
                assert!(first.is_ok(), "書き手が走る前の借用の読みが止められた");
                while !stop.load(Ordering::Relaxed) {
                    if stopped(|| eng.get_text(e, "body").map(|b| b.len())) {
                        stopped_readers.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
            })
        })
        .collect();

    started.wait();
    for g in 0..20_000u64 {
        eng.tie_text_to_by_id(e, body, &format!("{g:016}-{}", "x".repeat((g % 400) as usize)));
    }
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().unwrap();
    }
    assert_eq!(stopped_readers.load(Ordering::Relaxed), READERS as u64, "止まらなかった読み手が居る");
    drop(eng);
    cleanup(&path);
}

// ───────── 止めるもの: readonly (別 process の書き手と共存する開き方) ─────────

#[test]
fn readonly_borrow_of_leaf_is_stopped() {
    quiet();
    let path = tmp("readonly");
    cleanup(&path);
    let e = {
        let mut eng = Engine::create_standalone(&path).expect("create");
        eng.define_himo("body", ValueType::Leaf, 0);
        eng.define_himo("city", ValueType::Tag, 0);
        eng.define_himo("age", ValueType::Number, 0);
        let e = eng.entity().unwrap();
        eng.tie_text(e, "body", "leaf-value");
        eng.tie_text(e, "city", "Tokyo");
        eng.tie(e, "age", 30u32);
        eng.content(e, "memo", b"memo-value");
        eng.flush().unwrap();
        e
    };
    let ro = Engine::open_readonly(&path).expect("open_readonly");

    assert!(stopped(|| ro.get_text(e, "body").map(|b| b.len())), "get_text");
    assert!(stopped(|| ro.get_content(e, "memo").map(|b| b.len())), "get_content");
    assert!(stopped(|| ro.get_entity(e).len()), "get_entity");

    // Tag の借用と、 copy 版は読める
    assert_eq!(ro.get_text(e, "city"), Some(b"Tokyo".as_ref()));
    assert_eq!(ro.get_text_owned(e, "body").as_deref(), Some(b"leaf-value".as_ref()));
    assert_eq!(ro.get_content_owned(e, "memo").as_deref(), Some(b"memo-value".as_ref()));
    let owned = ro.get_entity_owned(e);
    assert!(owned.contains(&("body", EntityValueOwned::Text(b"leaf-value".to_vec()))));
    assert!(owned.contains(&("city", EntityValueOwned::Text(b"Tokyo".to_vec()))));
    assert!(owned.contains(&("age", EntityValueOwned::Num(30))));
    drop(ro);
    cleanup(&path);
}

/// `get_entity_owned` は `get_entity` と同じ列を同じ順で返す (中身を copy で持つだけ)。
#[test]
fn get_entity_owned_matches_get_entity() {
    let path = tmp("owned");
    cleanup(&path);
    let mut eng = Engine::create_standalone(&path).expect("create");
    eng.define_himo("body", ValueType::Leaf, 0);
    eng.define_himo("city", ValueType::Tag, 0);
    eng.define_himo("age", ValueType::Number, 0);
    eng.define_himo("at", ValueType::Number64, 0);
    let e = eng.entity().unwrap();
    eng.tie_text(e, "body", "leaf-value");
    eng.tie_text(e, "city", "Tokyo");
    eng.tie(e, "age", 30u32);
    eng.tie(e, "at", 1_790_000_000_123u64);

    let borrowed = eng.get_entity(e);
    let owned = eng.get_entity_owned(e);
    assert_eq!(borrowed.len(), 4);
    assert_eq!(borrowed.len(), owned.len());
    for ((bn, bv), (on, ov)) in borrowed.iter().zip(owned.iter()) {
        assert_eq!(bn, on);
        let same = match (bv, ov) {
            (EntityValue::Num(a), EntityValueOwned::Num(b)) => a == b,
            (EntityValue::Num64(a), EntityValueOwned::Num64(b)) => a == b,
            (EntityValue::Text(a), EntityValueOwned::Text(b)) => *a == b.as_slice(),
            _ => false,
        };
        assert!(same, "{bn}: {bv:?} vs {ov:?}");
    }
    drop(eng);
    cleanup(&path);
}
