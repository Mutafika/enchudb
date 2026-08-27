//! #119: Leaf の re-tie は **全経路** で `insert → publish → free` の順を守ること。
//!
//! 旧順序 (free 先行) だと、 同サイズ re-tie では best-fit が「たった今 free した hole」を
//! 必ず再利用するため、 旧 offset を掴んでいる並行 reader が再利用済み slot を読む。 実測:
//!
//! | 経路 | 修正前 | 修正後 |
//! |---|---|---|
//! | `tie_bytes_to_by_id` (直接) | None 8,132 / 60,463 | 0 |
//! | `remote_tieleaf_apply` (sync = replica/gossip 受信) | None 59,444 / 6.6 M | 0 |
//! | `apply_op::Tie` (async = concurrent consumer) | corrupt 370,471 + None 20,390 / 8.4 M | 0 |
//!
//! async 経路の corrupt は payload に free-list の hole header (`[36,0,0,0]` 等) が
//! 混ざった **捏造 bytes** で、 legacy slot 経路では seqlock でも検出できない。
//!
//! 独立レビューで sync / async の 2 経路が漏れていたのが判明したため、 3 経路すべてを
//! 固定する。 `vocab_max_entries` の上限 (#122 / #120 同型) も併せて。

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/retie_audit_{}_{}_{}.enchu", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

struct Tally {
    reads: usize,
    missing: usize,
    corrupt: usize,
}

/// writer クロージャを別スレッドで回しつつ get_text_owned で読み続ける共通 harness。
fn churn_and_read<W>(eng: Arc<Engine>, eid: u64, bodies: Vec<String>, writer_fn: W) -> Tally
where
    W: Fn(&Engine, u64, &str) + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let eng = eng.clone();
        let stop = stop.clone();
        let bodies = bodies.clone();
        std::thread::spawn(move || {
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) {
                writer_fn(&eng, eid, &bodies[n % bodies.len()]);
                n += 1;
            }
            n
        })
    };
    // warmup: writer の最初の値が観測されるまで数えない (init 値の残存を除外)
    let warm_deadline = std::time::Instant::now() + std::time::Duration::from_millis(2000);
    loop {
        if let Some(b) = eng.get_text_owned(eid, "body") {
            if bodies.iter().any(|x| x.as_bytes() == b.as_slice()) {
                break;
            }
        }
        assert!(std::time::Instant::now() < warm_deadline, "writer の値が観測できない");
    }
    // 以後 init 値も「既知」に含める (stale 1 世代は corrupt 扱いしない方針)
    let mut known: Vec<Vec<u8>> = bodies.iter().map(|s| s.as_bytes().to_vec()).collect();
    known.push(b"init-0000000000000000".to_vec());
    let mut t = Tally { reads: 0, missing: 0, corrupt: 0 };
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1500);
    while std::time::Instant::now() < deadline {
        match eng.get_text_owned(eid, "body") {
            Some(b) => {
                if !known.iter().any(|x| x[..] == b[..]) {
                    t.corrupt += 1;
                }
            }
            None => t.missing += 1,
        }
        t.reads += 1;
    }
    stop.store(true, Ordering::Relaxed);
    let writes = writer.join().unwrap();
    eprintln!(
        "reads={} missing={} corrupt={} writes={}",
        t.reads, t.missing, t.corrupt, writes
    );
    assert!(writes > 100, "writer が回っていない");
    t
}

fn setup(tag: &str) -> (String, Engine, u64) {
    let path = tmp_path(tag);
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    let eid = eng.entity().unwrap();
    eng.tie_text(eid, "body", "init-0000000000000000");
    (path, eng, eid)
}

fn bodies() -> Vec<String> {
    // 同サイズ re-tie で best-fit が直前 free hole を必ず再利用する形にする
    (0..6).map(|i| format!("b{}-{}", i, "x".repeat(120))).collect()
}

/// 直接経路 (`tie_bytes_to_by_id`)。
#[test]
fn direct_path_no_silent_none_or_torn() {
    let (path, eng, eid) = setup("control");
    let hid = eng.himo_id("body").unwrap() as u16;
    let eng = Arc::new(eng);
    let t = churn_and_read(eng, eid, bodies(), move |e, eid, s| {
        e.tie_bytes_to_by_id(eid, hid, s.as_bytes());
    });
    assert_eq!(t.missing, 0, "対照 (修正済み経路) で silent None");
    assert_eq!(t.corrupt, 0, "対照 (修正済み経路) で torn");
    cleanup(&path);
}

/// sync 経路 (replica / gossip 受信中の並行 read)。
#[test]
fn sync_apply_path_no_silent_none_or_torn() {
    let (path, eng, eid) = setup("remote");
    let hid = eng.himo_id("body").unwrap() as u16;
    let eng = Arc::new(eng);
    let t = churn_and_read(eng, eid, bodies(), move |e, eid, s| {
        e.remote_tieleaf_apply(eid, hid, s.as_bytes(), enchudb_oplog::Hlc::ZERO, None);
    });
    assert_eq!(
        t.missing + t.corrupt,
        0,
        "remote_tieleaf_apply (sync 経路) で silent None {} / torn {}",
        t.missing,
        t.corrupt
    );
    cleanup(&path);
}

/// async consumer 経路 (`create_concurrent*` + `tie_*_async` の全 consumer)。
#[test]
fn async_apply_path_no_silent_none_or_torn() {
    let (path, eng, eid) = setup("async");
    let hid = eng.himo_id("body").unwrap() as u16;
    let eng = Engine::concurrentize(eng);
    let t = churn_and_read(eng.clone(), eid, bodies(), move |e, eid, s| {
        e.tie_bytes_async_by_id(eid, hid, s.as_bytes());
    });
    assert_eq!(
        t.missing + t.corrupt,
        0,
        "apply_op::Tie (async 経路) で silent None {} / torn {}",
        t.missing,
        t.corrupt
    );
    cleanup(&path);
}

/// #122 knob: 2^31 超は next_power_of_two が overflow — create が Err でなく
/// panic (debug) / index_cap=0 の open 不能 DB (release) になる (#120 と同型)。
#[test]
fn vocab_max_entries_over_2p31_is_clean_err() {
    let path = tmp_path("hugeknob");
    cleanup(&path);
    let res = std::panic::catch_unwind(|| {
        Engine::create_growable_opts(
            &path,
            GrowableOptions {
                max_entities: 1000,
                vocab_max_entries: Some(3_000_000_000),
                ..Default::default()
            },
        )
    });
    match res {
        Err(p) => {
            let msg = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            eprintln!("create panicked (debug overflow): {msg}");
            panic!("create が Err でなく panic した — #120 の教訓 (書く前に Err) に反する");
        }
        Ok(Err(e)) => {
            eprintln!("create returned Err (期待通り): {e}");
        }
        Ok(Ok(eng)) => {
            drop(eng);
            let open = Engine::open_readonly(&path);
            eprintln!("create succeeded; open_readonly => {:?}", open.as_ref().err());
            assert!(
                open.is_ok(),
                "create 成功なのに open 不能 — #120 と同型の穴 (release mode)"
            );
        }
    }
    cleanup(&path);
}
