//! 購読の settle が、 根の評価の置き場を根の数だけ一時に膨らませない (評価の置き場は一定の数ずつ使う)。
//!
//! 0.36 を切る前の測りで、 社員 100 万人の行の購読 (`company.city = x` かつ年齢の範囲) の RSS の峰が #519 の後に 58 → 80 MB
//! だった。 常に持つ heap は同じ (最初の poll の後 21.1 MB) で、 最初の poll の間の heap の峰だけが 23.1 → 32.5 MB。 #519 で
//! settle の根の loop を 「全員を評価してから当てる」 にした時、 評価の置き場 (`Settled::nows`) に根の全員を置いていた。
//!
//! 生存ヒープを数える allocator で、 最初の poll の間の峰と、 poll の後の常駐の差 (一時の確保) を測る。 この binary の test は
//! この 1 本だけ (他の test の確保を数えないため)。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let n = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let np = unsafe { System.realloc(p, l, new) };
        if !np.is_null() {
            if new >= l.size() {
                let n = LIVE.fetch_add(new - l.size(), Ordering::Relaxed) + (new - l.size());
                PEAK.fetch_max(n, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(l.size() - new, Ordering::Relaxed);
            }
        }
        np
    }
}

#[global_allocator]
static A: Counting = Counting;

use enchudb_engine::live::LivePred;
use enchudb_engine::{Engine, GrowableOptions, ValueType};

/// 社員の表 20 万人・会社の表 2000 社 (街は 2 つ、 社員 → 会社の ref)。 「会社の街が 1 で 30 歳以上」 の行の購読の最初の poll は、 社員の全員を根として評価する。
/// 一時の確保は根の並び (1 人 4 B) と小さい置き場だけで、 根の全員の評価を一度に置かない。 置き場を poll の後に残しもしない。
#[test]
fn the_first_poll_does_not_hold_every_root_answer_at_once() {
    let (users, companies): (u32, u32) = (200_000, 2_000);
    let path = format!(
        "/tmp/enchudb-settle-peak-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let mut eng = Engine::create_growable_opts(
        &path,
        GrowableOptions { max_entities: (users + (1 << 20) + companies + 1024).next_power_of_two(), ..Default::default() },
    )
    .unwrap();
    eng.define_table("users", users).unwrap();
    eng.define_table("posts", 0).unwrap(); // 空の表 (既定の枠 100 万): 会社の eid を 2^20 の先に置く (w1 の table3 と同じ並べ方)
    eng.define_table("companies", companies).unwrap();
    eng.define_himo_in("companies", "city", ValueType::Number, 4).unwrap();
    eng.define_himo_in("users", "age", ValueType::Number, 0).unwrap();
    eng.define_ref_in("users", "company", "companies").unwrap();
    let us: Vec<u64> = (0..users).map(|_| eng.entity_in("users").unwrap()).collect();
    let cs: Vec<u64> = (0..companies).map(|_| eng.entity_in("companies").unwrap()).collect();
    for (c, &e) in cs.iter().enumerate() {
        eng.tie(e, "companies.city", c as u32 % 2);
    }
    for (u, &e) in us.iter().enumerate() {
        eng.tie(e, "users.company", enchudb_oplog::eid_local(cs[u % companies as usize]));
        eng.tie(e, "users.age", 18 + (u as u32 % 60));
    }
    let eng = Engine::concurrentize(eng);
    let (company, city, age) = (
        eng.himo_id("users.company").unwrap() as u16,
        eng.himo_id("companies.city").unwrap() as u16,
        eng.himo_id("users.age").unwrap() as u16,
    );
    let q = eng
        .subscribe(vec![
            LivePred::Via { path: vec![company], pred: Box::new(LivePred::Eq { himo_id: city, value: 1 }) },
            LivePred::Range { himo_id: age, lo: 30, hi: 1000 },
        ])
        .unwrap();
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let d = q.poll(&eng);
    let after = LIVE.load(Ordering::Relaxed);
    let transient = PEAK.load(Ordering::Relaxed) - after;
    let kept = after - before;
    let want = (0..users).filter(|&u| (u % companies) % 2 == 1 && 18 + u % 60 >= 30).count();
    assert_eq!(d.added.len(), want, "前提: 最初の poll が答えの全部を渡す");
    drop((d, q, eng));
    let _ = std::fs::remove_dir_all(&path);
    let mib = transient as f64 / (1 << 20) as f64;
    let kept_mib = kept as f64 / (1 << 20) as f64;
    eprintln!("first poll transient heap: {mib:.2} MiB, kept {kept_mib:.2} MiB");
    // 一時の確保は直した後 0.5 MiB ほど、 根の全員の評価を一度に置くと 2.5 MiB ほど (#519 の形: poll の後に手放す)。 poll の後に
    // 残るのは答えの状態の 1.8 MiB ほどで、 全員の評価の置き場を残すと 4.9 MiB ほど
    assert!(mib < 1.0, "最初の poll が根の全員の評価を一度に置いた: 一時の確保 {mib:.2} MiB");
    assert!(kept_mib < 2.5, "最初の poll の後に評価の置き場が残った: poll で増えた常駐 {kept_mib:.2} MiB");
}
