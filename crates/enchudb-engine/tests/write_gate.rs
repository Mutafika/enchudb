//! 円柱を組んでいない紐の書き手の門 (2026-10-11、 `write_gate`)。 書き手は紐の write_lock を取らずに門を通って列を書く。
//!
//! - 同じ列を何本もの書き手が同時に書き、 同時に列の末尾を伸ばしても、 値は全部残り、 開き直しても読める (列の header の
//!   count は fetch_max で上げる。 読んでから書く前の形で count が戻るのを捕まえるのは `column` の単体 test — この test は
//!   前の形でも 5 回とも通った (2026-10-11 実測、 engine の道では終わり際の競合がそろわない))
//! - 書きながら購読を張り、 途中で円柱を組んでも (門を閉じて以後は write_lock の道)、 止まった後の購読と `pull` は列と一致
//!
//! 答えは書き手の写し (shadow) と、 列を `get` で読む走査の 2 経路。 門の約束そのもの (組む側・barrier・並びの索引の構築が
//! 門の中の書き手を待つこと) は loom model (`loom_lazy_cylinder_build` / `loom_live_subscribe` / `loom_order_index_build`) と
//! `HimoStore` の単体 test が見る。

use enchudb_engine::{Engine, GrowableOptions, LiveDelta, LivePred, ValueType};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/write_gate_{}_{}_{}.enchu", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
}

/// 決定論の擬似乱数 (xorshift64*)。
struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

/// 購読の差分を積み上げる。
fn integrate(set: &mut BTreeSet<u64>, d: LiveDelta) {
    for e in d.removed {
        assert!(set.remove(&e), "removed に未報告の eid {e}");
    }
    for e in d.added {
        assert!(set.insert(e), "added に報告済みの eid {e}");
    }
}

const THREADS: usize = 8;
const PER: usize = 3_000;

/// k 番目の entity に書く値 (1..=7)。
fn value(k: usize) -> u32 {
    (k % 7) as u32 + 1
}

/// 全部の entity の値を、 `get` の走査と写し (`value`) で比べ、 値ごとの `pull` とも比べる。
fn check(eng: &Engine, eids: &[u64], what: &str) {
    for (k, &e) in eids.iter().enumerate() {
        assert_eq!(eng.get(e, "score"), Some(u64::from(value(k))), "{what}: entity {k}");
    }
    for v in 1..=7u32 {
        let mut got = eng.pull("score", v);
        got.sort_unstable();
        let mut want: Vec<u32> =
            eids.iter().enumerate().filter(|&(k, _)| value(k) == v).map(|(_, &e)| enchudb_oplog::eid_local(e)).collect();
        want.sort_unstable();
        assert_eq!(got, want, "{what}: pull({v})");
    }
}

/// 8 本の書き手が同じ列の末尾を同時に伸ばす (thread t は t, t+8, t+16, ... 番目を下から順に)。 書いた直後も、 開き
/// 直した後 (header の count から列を組む) も、 値は全部読め、 `pull` (ここで円柱を組む) も一致する。
#[test]
fn parallel_writers_extending_one_column_survive_reopen() {
    for round in 0..5 {
        let path = tmp_path(&format!("grow{round}"));
        cleanup(&path);
        let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
        eng.define_himo("score", ValueType::Number, 0);
        let eids: Vec<u64> = (0..THREADS * PER).map(|_| eng.entity().unwrap()).collect();
        let eng = Engine::concurrentize(eng);
        let start = std::sync::Barrier::new(THREADS);
        std::thread::scope(|s| {
            for t in 0..THREADS {
                let (eng, eids, start) = (&eng, &eids, &start);
                s.spawn(move || {
                    start.wait();
                    for i in 0..PER {
                        let k = i * THREADS + t;
                        eng.tie_to(eids[k], "score", value(k));
                    }
                });
            }
        });
        check(&eng, &eids, &format!("round {round} 書いた直後"));
        eng.flush_writes();
        drop(eng);
        let eng = Engine::open(&path).unwrap();
        check(&eng, &eids, &format!("round {round} 開き直した後"));
        drop(eng);
        cleanup(&path);
    }
}

/// 6 本の書き手が年齢を書き換え続ける (値 0..4 と消す) 間に、 購読 (年齢 == 1) を張り、 途中で `pull` が円柱を組む。
/// 止まった後の購読の集合・`pull`・`find_by` が、 列を `get` で読む走査と一致する。
#[test]
fn parallel_writers_while_subscribing_and_building() {
    for round in 0..10u64 {
        let path = tmp_path(&format!("sub{round}"));
        cleanup(&path);
        let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let age = eng.himo_id("age").unwrap() as u16;
        let eids: Vec<u64> = (0..4_000).map(|_| eng.entity().unwrap()).collect();
        let eng = Engine::concurrentize(eng);
        let stop = AtomicBool::new(false);
        let mut seen = BTreeSet::new();
        let q = std::thread::scope(|s| {
            for t in 0..6u64 {
                let (eng, eids, stop) = (&eng, &eids, &stop);
                s.spawn(move || {
                    let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ (round * 8 + t + 1));
                    while !stop.load(Ordering::Relaxed) {
                        let e = eids[rng.below(eids.len() as u64) as usize];
                        if rng.below(5) == 0 {
                            eng.untie(e, "age");
                        } else {
                            eng.tie_to(e, "age", rng.below(4) as u32);
                        }
                    }
                });
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
            let q = eng.subscribe(vec![LivePred::Eq { himo_id: age, value: 1 }]).unwrap();
            integrate(&mut seen, q.poll(&eng));
            std::thread::sleep(std::time::Duration::from_millis(1 + round % 3));
            // 書いている途中で円柱を組む (以後の書き手は write_lock の道)
            let _ = eng.pull("age", 1u32);
            for _ in 0..3 {
                integrate(&mut seen, q.poll(&eng));
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            stop.store(true, Ordering::Relaxed);
            q
        });
        integrate(&mut seen, q.poll(&eng));
        let want: BTreeSet<u64> = eids.iter().copied().filter(|&e| eng.get(e, "age") == Some(1)).collect();
        assert_eq!(seen, want, "round {round}: 購読が列と違う");
        let mut pulled = eng.pull("age", 1u32);
        pulled.sort_unstable();
        let want_local: Vec<u32> = want.iter().map(|&e| enchudb_oplog::eid_local(e)).collect();
        assert_eq!(pulled, want_local, "round {round}: pull が列と違う");
        let found: BTreeSet<u64> = eng.find_by(vec![LivePred::Eq { himo_id: age, value: 1 }]).unwrap().into_iter().collect();
        assert_eq!(found, want, "round {round}: find_by が列と違う");
        drop(q);
        drop(eng);
        cleanup(&path);
    }
}
