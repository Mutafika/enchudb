//! 鍵付きの購読 (`subscribe_keyed`、 組 / 再帰 / window の購読の土台) が **sync で届いた書き込み** を拾うこと。
//! peer A が row の追加・鍵の列の書き換え・ref の付け替え・ref の先の値の書き換え・row の削除と追加をし、 peer B の
//! 鍵付きの購読 (鍵 = ref 列 / ref の先の列) の積分が、 毎回 B の列を直接読んだ答えと一致すること。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::{LivePred, ValueType};
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!("/tmp/enchudb-keyed-remote-{}-{}-{}", tag, std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    for ext in ["oplog", "tables", "crc", "db.lock", "eidmap"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("companies", 1000).unwrap();
    eng.define_himo_in("companies", "city", ValueType::Number, 0).unwrap();
    eng.define_table("users", 1000).unwrap();
    eng.define_himo_in("users", "age", ValueType::Number, 0).unwrap();
    eng.define_ref_in("users", "company", "companies").unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn ship(eng_a: &Arc<Engine>, sa: &Syncer, sb: &Syncer) {
    eng_a.oplog_commit();
    eng_a.oplog_sync().unwrap();
    let t0 = std::time::Instant::now();
    while t0.elapsed() < Duration::from_secs(5) {
        sa.publish_since(Hlc::ZERO);
        if sb.pull_once(1).applied > 0 {
            std::thread::sleep(Duration::from_millis(100));
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("5 秒以内に A の op が B に適用されなかった");
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

#[test]
fn keyed_subscription_follows_remote_writes() {
    let (pa, pb) = (tmp_path("a"), tmp_path("b"));
    let (a, b) = (make_engine(&pa, 1), make_engine(&pb, 2));
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let tr: Arc<dyn Transport> = mem;
    let (sa, sb) = (Syncer::new(a.clone(), tr.clone()), Syncer::new(b.clone(), tr.clone()));
    let hb = |n: &str| b.himo_id(n).unwrap() as u16;
    let (age, company, city) = (hb("users.age"), hb("users.company"), hb("companies.city"));
    // B: 30 歳より上の user を、 会社の ref / 会社の city を鍵に
    let by_ref = b.subscribe_keyed(vec![LivePred::Range { himo_id: age, lo: 31, hi: 1000 }], vec![], company).unwrap();
    let by_city = b.subscribe_keyed(vec![LivePred::Range { himo_id: age, lo: 31, hi: 1000 }], vec![company], city).unwrap();
    let (mut s_ref, mut s_city): (BTreeMap<u64, u64>, BTreeMap<u64, u64>) = Default::default();
    let mut rng = Rng(0x5eed);
    let comps: Vec<u64> = (0..4).map(|i| { let c = a.entity_in("companies").unwrap(); a.tie_to(c, "companies.city", i as u32); c }).collect();
    let mut users: Vec<u64> = Vec::new();
    for _ in 0..20 {
        let u = a.entity_in("users").unwrap();
        a.tie_to(u, "users.age", rng.below(60) as u32);
        a.tie_ref_to(u, "users.company", comps[rng.below(4) as usize]);
        users.push(u);
    }
    for round in 0..15 {
        ship(&a, &sa, &sb);
        for (live, seen) in [(&by_ref, &mut s_ref), (&by_city, &mut s_city)] {
            let d = live.poll(&b);
            for (e, k) in &d.removed {
                assert_eq!(seen.remove(e), Some(*k), "round {round}: removed {e:#x} {k}");
            }
            for (e, k) in d.added {
                assert!(seen.insert(e, k).is_none(), "round {round}: added dup {e:#x}");
            }
        }
        // oracle: B の列を直接読む
        let mut want_ref = BTreeMap::new();
        let mut want_city = BTreeMap::new();
        for e in b.entities_with_himo(age) {
            if b.get_by_id(e, age).is_some_and(|x| x > 30)
                && let Some(c) = b.get_by_id(e, company)
            {
                want_ref.insert(e, c);
                let ce = enchudb_oplog::make_eid(2, c as u32);
                if let Some(ct) = b.get_by_id(ce, city) {
                    want_city.insert(e, ct);
                }
            }
        }
        assert_eq!(s_ref, want_ref, "round {round}: by_ref");
        assert_eq!(s_city, want_city, "round {round}: by_city");
        // A で書く
        for _ in 0..8 {
            match rng.below(5) {
                0 => a.tie_to(users[rng.below(users.len() as u64) as usize], "users.age", rng.below(60) as u32),
                1 => a.tie_ref_to(users[rng.below(users.len() as u64) as usize], "users.company", comps[rng.below(4) as usize]),
                2 => a.tie_to(comps[rng.below(4) as usize], "companies.city", rng.below(10) as u32),
                3 => {
                    let i = rng.below(users.len() as u64) as usize;
                    a.delete(users[i]);
                    let u = a.entity_in("users").unwrap();
                    a.tie_to(u, "users.age", rng.below(60) as u32);
                    a.tie_ref_to(u, "users.company", comps[rng.below(4) as usize]);
                    users[i] = u;
                }
                _ => {
                    let u = a.entity_in("users").unwrap();
                    a.tie_to(u, "users.age", rng.below(60) as u32);
                    a.tie_ref_to(u, "users.company", comps[rng.below(4) as usize]);
                    users.push(u);
                }
            }
        }
    }
    assert!(s_ref.len() > 10 && s_city.len() > 10, "前提: 集合が空でない ({} / {})", s_ref.len(), s_city.len());
    drop((by_ref, by_city, sa, sb));
    drop(a);
    drop(b);
    cleanup(&pa);
    cleanup(&pb);
}
