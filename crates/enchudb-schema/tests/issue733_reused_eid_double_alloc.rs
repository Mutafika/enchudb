//! #733: 満杯の表への並行の insert は、 同じ消えた eid を 2 回払い出さない。
//!
//! `Engine::entity_in` は、 空きの一覧 (`free_locals`) から穴を取り出した後、 lock を放してから live の印を立てていた。 その間に
//! 枯渇した別の thread の `rebuild_free_locals` が、 まだ live でないその slot を穴として拾い直し、 同じ eid が 2 本に払い出された
//! (2 つの insert の列が 1 つの行に混ざる)。 user の表の delete は空きの一覧に足さないので、 満杯の表の insert はいつも作り直しを
//! 通る = この窓が普通の道だった。
//!
//! 判定は状態で: 各 thread は自分の行を 1 つ持ち、 「消す → 別の名前で insert」 を繰り返す。 insert が返した eid を、 今ほかの thread が
//! 持っていたら二重払い出し。 自分の行の中身 (名前と値) が自分の書いた物かも毎回見る。

use enchudb_schema::{Database, Value};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Barrier, Mutex};

const THREADS: i64 = 4;
const ROUNDS: i64 = 20_000;
const CAP: u32 = 8;

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn num(v: Option<Value>) -> Option<i64> {
    match v {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    }
}

#[test]
fn concurrent_inserts_into_a_full_table_never_get_the_same_eid() {
    let path = format!(
        "{}/enchudb-issue733-{}-{}",
        std::env::temp_dir().display(),
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").with_capacity(CAP).build().unwrap();
    let db = db.finish_with_oplog(4 << 20).unwrap();
    let t = db.get_table("t").unwrap();
    // 表を満杯にする: thread ごとの行 1 つ + 残りは動かさない行
    let mut mine = Vec::new();
    for w in 0..THREADS {
        mine.push(t.insert().set("id", w).set("a", w).commit().unwrap());
    }
    for id in THREADS..CAP as i64 {
        t.insert().set("id", 1_000_000 + id).commit().unwrap();
    }
    // 今だれかが持っている eid
    let held: Mutex<HashSet<u64>> = Mutex::new(mine.iter().copied().collect());
    let seen: Mutex<HashSet<u64>> = Mutex::new(mine.iter().copied().collect());
    let doubled = AtomicU64::new(0);
    let mixed = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let start = Barrier::new(THREADS as usize);
    std::thread::scope(|s| {
        for w in 0..THREADS {
            let (db, held, seen, doubled, mixed, stop, start) = (&db, &held, &seen, &doubled, &mixed, &stop, &start);
            let mut e = mine[w as usize];
            s.spawn(move || {
                let t = db.get_table("t").unwrap();
                start.wait();
                for r in 1..=ROUNDS {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    held.lock().unwrap().remove(&e);
                    t.entity(e).delete().unwrap();
                    // 名前は thread ごと・回ごとに違う (名前の lock では並ばない)
                    let id = w + r * THREADS;
                    e = t.insert().set("id", id).set("a", id).commit().unwrap();
                    if !held.lock().unwrap().insert(e) {
                        doubled.fetch_add(1, Ordering::Relaxed);
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                    seen.lock().unwrap().insert(e);
                    // 自分の行は自分の書いた名前と値 (他の thread は自分の eid にしか書かない)
                    if (num(t.entity(e).get("id")), num(t.entity(e).get("a"))) != (Some(id), Some(id)) {
                        mixed.fetch_add(1, Ordering::Relaxed);
                        stop.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            });
        }
    });
    let distinct = seen.lock().unwrap().len() as i64;
    // 試験の前提: 表は伸びずに穴を使い回した (伸びていたら枯渇の道 = 空きの一覧の作り直しを通っていない)
    assert!(
        distinct <= (CAP as i64) * 4,
        "試験の前提: eid を使い回していない ({distinct} 種類の eid、 枠 {CAP})"
    );
    assert_eq!(doubled.load(Ordering::Relaxed), 0, "同じ eid が 2 本の insert に払い出された");
    assert_eq!(mixed.load(Ordering::Relaxed), 0, "自分の行に他の insert の列が混ざった");
    assert_eq!(t.all().count().unwrap(), CAP as usize, "行の数 (thread ごとに 1 つ + 動かさない行)");
}
