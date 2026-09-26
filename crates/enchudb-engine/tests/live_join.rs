//! 値で結ぶ準結合 (`LivePred::ExistsEq`) の engine 直の検査: 書き込みと並行した登録・poll の後、 積分した
//! 集合が総当たりの結果と一致する。 型の違う列 / Leaf を結ぶ購読は断る。

use enchudb_engine::{Engine, GrowableOptions, LiveDelta, LivePred, ValueType};
use std::collections::BTreeSet;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/live_join_{}_{}_{}.enchu", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
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

fn integrate(set: &mut BTreeSet<u64>, d: LiveDelta) {
    for e in d.removed {
        assert!(set.remove(&e), "removed に未報告の eid {e}");
    }
    for e in d.added {
        assert!(set.insert(e), "added に報告済みの eid {e}");
    }
}

/// 値で結ぶ列は同じ型で Leaf でないこと。
#[test]
fn value_join_needs_columns_of_the_same_type() {
    let path = tmp_path("types");
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    for (h, t) in [("u.city", ValueType::Tag), ("s.city", ValueType::Tag), ("s.zip", ValueType::Number), ("u.note", ValueType::Leaf), ("s.note", ValueType::Leaf)] {
        eng.define_himo(h, t, 0);
    }
    let id = |h: &str| eng.himo_id(h).unwrap() as u16;
    let q = |mine: u16, theirs: u16| vec![LivePred::ExistsEq { mine, theirs, preds: vec![LivePred::Present { himo_id: theirs }] }];
    assert!(eng.subscribe(q(id("u.city"), id("s.city"))).is_ok());
    assert!(eng.subscribe(q(id("u.city"), id("s.zip"))).is_err(), "Tag と Number");
    assert!(eng.subscribe(q(id("u.note"), id("s.note"))).is_err(), "Leaf");
    // 否定・ref の先・中身の中でも
    let not = vec![LivePred::Present { himo_id: id("u.city") }, LivePred::Not(Box::new(q(id("u.city"), id("s.zip")).remove(0)))];
    assert!(eng.subscribe(not).is_err(), "否定の中");
    // 件数の閾値 (CountAtLeast) も同じ検査。 min 0 は断る
    let count = |mine: u16, theirs: u16, min: u64| {
        vec![LivePred::CountAtLeast { via: theirs, mine: Some(mine), min, preds: vec![LivePred::Present { himo_id: theirs }] }]
    };
    assert!(eng.subscribe(count(id("u.city"), id("s.city"), 2)).is_ok());
    assert!(eng.subscribe(count(id("u.city"), id("s.zip"), 2)).is_err(), "CountAtLeast: Tag と Number");
    assert!(eng.subscribe(count(id("u.city"), id("s.city"), 0)).is_err(), "CountAtLeast: min 0");
    assert!(eng.find_by(count(id("u.city"), id("s.city"), 0)).is_err(), "CountAtLeast: min 0 (find)");
    drop(eng);
    cleanup(&path);
}

/// 書き込み (住人の街・店の街・店の開閉) と並行して登録・poll し、 止めた後の積分が総当たりと一致する。
#[test]
fn value_join_under_concurrent_writes() {
    let path = tmp_path("race");
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    for h in ["u.city", "s.city", "s.open"] {
        eng.define_himo(h, ValueType::Number, 0);
    }
    let id = |eng: &Engine, h: &str| eng.himo_id(h).unwrap() as u16;
    let (ucity, scity, sopen) = (id(&eng, "u.city"), id(&eng, "s.city"), id(&eng, "s.open"));
    let users: Vec<u64> = (0..500).map(|_| eng.entity().unwrap()).collect();
    let shops: Vec<u64> = (0..30).map(|_| eng.entity().unwrap()).collect();
    for (i, &u) in users.iter().enumerate() {
        eng.tie(u, "u.city", (i % 8) as u32);
    }
    for (i, &s) in shops.iter().enumerate() {
        eng.tie(s, "s.city", (i % 6) as u32);
        eng.tie(s, "s.open", (i % 2) as u32);
    }
    let eng = Engine::concurrentize(eng);
    let join = |open: bool| LivePred::ExistsEq {
        mine: ucity,
        theirs: scity,
        preds: if open { vec![LivePred::Eq { himo_id: sopen, value: 1 }] } else { vec![LivePred::Present { himo_id: scity }] },
    };
    for round in 0..10u64 {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..4u64)
            .map(|t| {
                let eng = eng.clone();
                let (users, shops) = (users.clone(), shops.clone());
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng(0x10e1_0000_0000_0001 ^ (round * 4 + t + 1));
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let u = users[rng.below(users.len() as u64) as usize];
                        let s = shops[rng.below(shops.len() as u64) as usize];
                        match rng.below(6) {
                            0 | 1 => eng.tie_to(u, "u.city", rng.below(8) as u32),
                            2 => eng.tie_to(s, "s.city", rng.below(8) as u32),
                            3 if rng.below(8) == 0 => eng.untie(u, "u.city"),
                            _ => eng.tie_to(s, "s.open", rng.below(2) as u32),
                        }
                    }
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(3));
        // 書き込みと並行して登録: 開いた店がある街の住人 / 店が 1 軒も無い街の住人
        let open_q = eng.subscribe(vec![join(true)]).unwrap();
        let none_q = eng
            .subscribe(vec![LivePred::Present { himo_id: ucity }, LivePred::Not(Box::new(join(false)))])
            .unwrap();
        // 開いた店が 2 軒以上ある街の住人 (件数の閾値)
        let two = || vec![LivePred::CountAtLeast { via: scity, mine: Some(ucity), min: 2, preds: vec![LivePred::Eq { himo_id: sopen, value: 1 }] }];
        let two_q = eng.subscribe(two()).unwrap();
        let mut two_seen = BTreeSet::new();
        let (mut open_seen, mut none_seen) = (BTreeSet::new(), BTreeSet::new());
        let t0 = std::time::Instant::now();
        while t0.elapsed() < std::time::Duration::from_millis(40) {
            integrate(&mut open_seen, open_q.poll(&eng));
            integrate(&mut none_seen, none_q.poll(&eng));
            integrate(&mut two_seen, two_q.poll(&eng));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        integrate(&mut open_seen, open_q.poll(&eng));
        integrate(&mut none_seen, none_q.poll(&eng));
        integrate(&mut two_seen, two_q.poll(&eng));
        let shops_at = |v: u64| {
            shops.iter().filter(|&&s| eng.get(s, "s.city") == Some(v) && eng.get(s, "s.open") == Some(1)).count()
        };
        let want_two: BTreeSet<u64> =
            users.iter().copied().filter(|&u| eng.get(u, "u.city").is_some_and(|v| shops_at(v) >= 2)).collect();
        assert_eq!(two_seen, want_two, "round {round}: 開いた店が 2 軒以上ある街の住人");
        let found_two: BTreeSet<u64> = eng.find_by(two()).unwrap().into_iter().collect();
        assert_eq!(found_two, want_two, "round {round}: find_by (2 軒以上)");
        let shop_at = |v: u64, open: bool| {
            shops.iter().any(|&s| eng.get(s, "s.city") == Some(v) && (!open || eng.get(s, "s.open") == Some(1)))
        };
        let want_open: BTreeSet<u64> =
            users.iter().copied().filter(|&u| eng.get(u, "u.city").is_some_and(|v| shop_at(v, true))).collect();
        let want_none: BTreeSet<u64> =
            users.iter().copied().filter(|&u| eng.get(u, "u.city").is_some_and(|v| !shop_at(v, false))).collect();
        assert_eq!(open_seen, want_open, "round {round}: 開いた店がある街の住人");
        assert_eq!(none_seen, want_none, "round {round}: 店が無い街の住人");
        assert_eq!(open_q.count(&eng), want_open.len());
        let found: BTreeSet<u64> = eng.find_by(vec![join(true)]).unwrap().into_iter().collect();
        assert_eq!(found, want_open, "round {round}: find_by");
    }
    drop(eng);
    cleanup(&path);
}

/// 和の閾値 (`SumAtLeast`): 和の列の書き換え・外しで和が閾値をまたぐと出入りし、 指されている entity の削除で
/// 出る。 行の無い group は偽 (min が 0 以下でも)。
#[test]
fn sum_threshold_follows_the_summed_column() {
    let path = tmp_path("sum");
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("c.name", ValueType::Number, 0);
    eng.define_himo("u.company", ValueType::Ref, 0);
    eng.define_himo("u.salary", ValueType::Number, 0);
    let co = eng.entity().unwrap();
    let empty = eng.entity().unwrap();
    eng.tie(co, "c.name", 1u32);
    eng.tie(empty, "c.name", 2u32);
    let (a, b) = (eng.entity().unwrap(), eng.entity().unwrap());
    for (e, s) in [(a, 1u32), (b, 5u32)] {
        eng.tie(e, "u.company", co as u32);
        eng.tie(e, "u.salary", s);
    }
    let id = |h: &str| eng.himo_id(h).unwrap() as u16;
    let (name, comp, sal) = (id("c.name"), id("u.company"), id("u.salary"));
    let eng = Engine::concurrentize(eng);
    let pred = |min: i128| {
        vec![
            LivePred::Present { himo_id: name },
            LivePred::SumAtLeast { via: comp, mine: None, sum_himo: sal, min, signed: false, preds: vec![LivePred::Present { himo_id: comp }] },
        ]
    };
    let q = eng.subscribe(pred(3)).unwrap();
    assert_eq!(q.poll(&eng).added, vec![co], "1 + 5 >= 3");
    eng.tie_to(b, "u.salary", 0u32);
    assert_eq!(q.poll(&eng).removed, vec![co], "1 + 0 < 3");
    eng.tie_to(b, "u.salary", 9u32);
    assert_eq!(q.poll(&eng).added, vec![co]);
    eng.untie(b, "u.salary");
    assert_eq!(q.poll(&eng).removed, vec![co], "値の無い row は 0 として足す");
    assert_eq!(eng.find_by(pred(1)).unwrap(), vec![co]);
    // 行の無い group (社員の居ない会社) は min <= 0 でも入らない
    assert_eq!(eng.find_by(pred(0)).unwrap(), vec![co]);
    assert_eq!(eng.find_by(pred(-5)).unwrap(), vec![co]);
    let q0 = eng.subscribe(pred(0)).unwrap();
    assert_eq!(q0.poll(&eng).added, vec![co]);
    // 社員が全員抜けた = 行の無い group は min 0 でも偽
    eng.untie(a, "u.company");
    eng.untie(b, "u.company");
    assert_eq!(q0.poll(&eng).removed, vec![co], "行の無い group");
    eng.tie_to(a, "u.company", co as u32);
    assert_eq!(q0.poll(&eng).added, vec![co]);
    eng.delete(co);
    assert_eq!(q0.poll(&eng).removed, vec![co], "指されている entity の削除");
    // 和の列は Number / Number64
    let bad = vec![
        LivePred::Present { himo_id: name },
        LivePred::SumAtLeast { via: comp, mine: None, sum_himo: comp, min: 1, signed: false, preds: vec![LivePred::Present { himo_id: comp }] },
    ];
    assert!(eng.subscribe(bad).is_err(), "Ref の列は足せない");
    drop((q, q0));
    drop(eng);
    cleanup(&path);
}
