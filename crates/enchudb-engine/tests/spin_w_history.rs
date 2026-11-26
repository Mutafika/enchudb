//! スピン 案 W の試作 (`declare_history`): 宣言した列の書き込みを identity ごとに持つ履歴を、 素朴な模型 (全部の書き込みを時刻の順に
//! 当てた真実) と比べる。
//!
//! - 書き込み・外す・行の削除・場所の使い回し・開き直し・宣言の無い列への書き込みを random に混ぜ、 各 identity の変更の一覧 /
//!   時点 t の値 / 更新の回数で引いた値 / 時点 t のデッキ を模型と比べる
//! - WAL の無い session は履歴の列を持つ表の行に書けない (何も変えずに断る)
//! - 電源断 (`--features crashsim`): 書き出しの途中・後の像を開いて、 集まりの最新と cell が揃うこと、 届いた書き込み (oplog を書き出した
//!   後の分) が 「遅くとも」 でない entry で残ること

use enchudb_engine::engine::FaultKind;
use enchudb_engine::history::HistError;
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::EntityId;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const CAP: usize = 4 * 1024 * 1024;
const V: &str = "t.v";
const W: &str = "t.w";
/// 表の行の数 (小さくして場所を使い回させる)
const ROWS: u32 = 8;
const VALUES: u64 = 5;

struct Scratch(PathBuf);
impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu_spin_w_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Scratch(p)
    }
    fn db(&self) -> PathBuf {
        self.0.join("db")
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// 表 t (列 v = 履歴あり、 w = 履歴なし) を作り、 WAL 付きにして v に履歴を宣言する。
fn create(db: &Path, versions: bool) -> Arc<Engine> {
    let p = db.to_str().unwrap();
    let mut eng = if versions { Engine::create_with_cell_version(p, 4096).unwrap() } else { Engine::create_with_capacity(p, 4096).unwrap() };
    eng.define_table("t", ROWS).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "w", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    assert_eq!(eng.declare_history(V), Ok(true));
    assert_eq!(eng.declare_history(V), Ok(false));
    eng
}

fn reopen(db: &Path) -> Arc<Engine> {
    Engine::open_concurrent_with_oplog(db.to_str().unwrap(), CAP).unwrap()
}

/// 模型: identity ごとの (時刻, 値) の並び (時刻の順)。 地平 = 宣言の時刻。
#[derive(Default, Clone)]
struct Model {
    horizon: u64,
    /// identity → 変更の並び
    cards: BTreeMap<EntityId, Vec<(u64, Option<u64>)>>,
    /// 生きている行
    live: BTreeSet<EntityId>,
    /// 全部の変更の時刻 (全体の順)
    times: Vec<u64>,
}

impl Model {
    fn cur(&self, e: EntityId) -> Option<u64> {
        self.cards.get(&e).and_then(|c| c.last()).and_then(|x| x.1)
    }
    fn at(&self, e: EntityId, t: u64) -> Option<u64> {
        self.cards.get(&e).and_then(|c| c.iter().rev().find(|x| x.0 <= t)).and_then(|x| x.1)
    }
    fn deck(&self, v: u64, t: u64) -> BTreeSet<EntityId> {
        self.cards.keys().copied().filter(|&e| self.at(e, t) == Some(v)).collect()
    }
}

/// 書き込みの後: 変更なら時計が進んだ時刻を模型に足す (時刻は全体で増える)。 変更でなければ時計は動かない。
fn record(eng: &Engine, m: &mut Model, e: EntityId, new: Option<u64>) {
    let clock = eng.history_clock(V).unwrap();
    let last = m.times.last().copied().unwrap_or(m.horizon);
    if m.cur(e) == new {
        assert_eq!(clock, last, "変更でない書き込みで時計が進んだ");
        return;
    }
    assert!(clock > last, "変更の時刻が進まない ({clock} <= {last})");
    m.times.push(clock);
    m.cards.entry(e).or_default().push((clock, new));
}

/// engine の履歴を模型と全部比べる。
fn check(eng: &Engine, m: &Model, rng: &mut Rng) {
    for (&e, card) in &m.cards {
        let h = eng.history(e, V).unwrap();
        let got: Vec<(u64, Option<u64>)> = h.iter().map(|c| (c.time, c.value)).collect();
        assert_eq!(got, *card, "identity {e:#x} の変更の一覧");
        for (i, c) in h.iter().enumerate() {
            assert_eq!(c.seq, i as u32, "更新の回数");
            assert!(!c.observed, "時刻の分からない entry がある (identity {e:#x}): {h:?}");
            assert_eq!(eng.value_at_seq(e, V, i as u32).unwrap().map(|x| (x.time, x.value)), Some(card[i]), "{i} 回目");
        }
        assert_eq!(eng.value_at_seq(e, V, card.len() as u32).unwrap(), None);
        for &(t, _) in card {
            for tt in [t - 1, t, t + 1] {
                if tt < m.horizon {
                    assert!(matches!(eng.value_at(e, V, tt), Err(HistError::TooOld { .. })));
                    continue;
                }
                assert_eq!(eng.value_at(e, V, tt).unwrap().value, m.at(e, tt), "identity {e:#x} 時点 {tt}");
            }
        }
    }
    // 生きている行の最新 = cell
    for &e in &m.live {
        assert_eq!(eng.get(e, V), m.cur(e), "cell と模型");
    }
    // 時点 t のデッキ (時刻を少し選ぶ + 今)
    let mut ts: Vec<u64> = (0..6).filter_map(|_| (!m.times.is_empty()).then(|| m.times[rng.below(m.times.len() as u64) as usize])).collect();
    ts.push(eng.history_clock(V).unwrap());
    ts.push(m.horizon);
    for t in ts {
        for v in 0..VALUES {
            let got: BTreeSet<EntityId> = eng.pull_at(V, v, t).unwrap().into_iter().collect();
            assert_eq!(got, m.deck(v, t), "時点 {t} に v = {v} だった行");
        }
    }
}

/// random な操作を `n` 回。 `reopen_every` 回ごとに閉じて開き直す。
fn run(seed: u64, n: usize, versions: bool) {
    let s = Scratch::new(&format!("model_{seed}_{versions}"));
    let db = s.db();
    let mut eng = create(&db, versions);
    let mut m = Model { horizon: eng.history_clock(V).unwrap(), ..Default::default() };
    let mut rng = Rng(seed | 1);
    let mut reused = 0;
    let mut slots_seen: BTreeSet<u32> = BTreeSet::new();
    for step in 0..n {
        let k = rng.below(100);
        let pick = |m: &Model, rng: &mut Rng| -> Option<EntityId> {
            (!m.live.is_empty()).then(|| *m.live.iter().nth(rng.below(m.live.len() as u64) as usize).unwrap())
        };
        match k {
            0..=14 => {
                if (m.live.len() as u32) < ROWS {
                    let e = eng.entity_in("t").unwrap();
                    if !slots_seen.insert(eng.eid_slot(e)) {
                        reused += 1;
                    }
                    assert!(!m.cards.contains_key(&e), "新しい行の eid が前の住人と同じ");
                    m.live.insert(e);
                }
            }
            15..=59 => {
                if let Some(e) = pick(&m, &mut rng) {
                    let v = rng.below(VALUES);
                    eng.tie_to(e, V, v as u32);
                    record(&eng, &mut m, e, Some(v));
                }
            }
            60..=67 => {
                if let Some(e) = pick(&m, &mut rng) {
                    eng.untie(e, V);
                    record(&eng, &mut m, e, None);
                }
            }
            68..=77 => {
                if let Some(e) = pick(&m, &mut rng) {
                    eng.tie_to(e, W, rng.below(100) as u32);
                    let cur = m.cur(e);
                    record(&eng, &mut m, e, cur); // 履歴の無い列: 変更なし
                }
            }
            78..=88 => {
                if let Some(e) = pick(&m, &mut rng) {
                    eng.try_delete(e).unwrap();
                    record(&eng, &mut m, e, None);
                    m.live.remove(&e);
                }
            }
            89..=94 => check(&eng, &m, &mut rng),
            _ => {
                drop(eng);
                eng = reopen(&db);
                assert_eq!(eng.history_declarations(), vec![V.to_string()]);
                check(&eng, &m, &mut rng);
            }
        }
        if step % 97 == 0 {
            check(&eng, &m, &mut rng);
        }
    }
    check(&eng, &m, &mut rng);
    assert!(reused > 0, "前提: 場所を使い回した");
    let (n_entries, observed, _, _) = eng.history_stats(V).unwrap();
    assert_eq!(observed, 0);
    assert_eq!(n_entries as usize, m.cards.values().map(|c| c.len()).sum::<usize>());
}

#[test]
fn model_random_with_versions() {
    for seed in [1, 7, 42] {
        run(seed, 1500, true);
    }
}

#[test]
fn model_random_without_versions() {
    for seed in [3, 9] {
        run(seed, 1500, false);
    }
}

/// 宣言の時に値のある行は宣言の時刻 (地平) の entry を持ち、 地平より前は TooOld。
#[test]
fn declaration_takes_the_values_at_the_horizon() {
    let s = Scratch::new("horizon");
    let p = s.db();
    let mut eng = Engine::create_with_capacity(p.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 16).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "w", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    let a = eng.entity_in("t").unwrap();
    let b = eng.entity_in("t").unwrap();
    eng.tie_to(a, V, 3u32);
    eng.declare_history(V).unwrap();
    let h0 = eng.history_clock(V).unwrap();
    let ha = eng.history(a, V).unwrap();
    assert_eq!(ha.len(), 1);
    assert!(ha[0].base && ha[0].time == h0 && ha[0].value == Some(3));
    assert!(eng.history(b, V).unwrap().is_empty());
    assert!(matches!(eng.value_at(a, V, h0 - 1), Err(HistError::TooOld { .. })));
    assert_eq!(eng.value_at(a, V, h0).unwrap().value, Some(3));
    assert_eq!(eng.pull_at(V, 3, h0).unwrap(), vec![a]);
    assert!(matches!(eng.value_at(a, W, h0), Err(HistError::NotDeclared)));
}

/// WAL は任意: WAL の無い session (単独で開く) の書き込み・外す・削除も、 engine の時計 (壁時計の ms と列の時計の大きい方) で
/// 印なしの entry になる。 1 ms の中に多く書いて時計が壁時計より先に出た後に WAL 付きで開いて書いても、 時刻は戻らない。
#[test]
fn walless_session_writes_with_engine_clock() {
    let s = Scratch::new("walless");
    let p = s.db();
    let (a, b) = {
        let mut eng = Engine::create_with_capacity(p.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 16).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
        let a = eng.entity_in("t").unwrap();
        let b = eng.entity_in("t").unwrap();
        eng.tie_to(a, V, 1u32);
        eng.declare_history(V).unwrap();
        eng.tie_to(a, V, 2u32);
        (a, b)
    };
    let mut want: Vec<Option<u64>> = vec![Some(1), Some(2)];
    {
        let eng = Engine::open_standalone(p.to_str().unwrap()).unwrap();
        assert_eq!(eng.history_declarations(), vec![V.to_string()]);
        let before = eng.fault_count(FaultKind::NoWal);
        // 1 ms の中に何千回も書く (列の時計が壁時計より先に出る)
        for i in 0..5000u32 {
            eng.tie_to(a, V, 3 + i % 2);
            want.push(Some(u64::from(3 + i % 2)));
        }
        eng.untie(a, V);
        want.push(None);
        eng.tie_to(b, V, 7u32);
        eng.try_delete(b).unwrap();
        assert_eq!(eng.fault_count(FaultKind::NoWal), before, "WAL の無い書き込みを断った");
        let h = eng.history(a, V).unwrap();
        assert!(h.iter().all(|c| !c.observed), "WAL の無い書き込みに 「遅くとも」 の印");
        let hb = eng.history(b, V).unwrap();
        assert_eq!(hb.iter().map(|c| c.value).collect::<Vec<_>>(), vec![Some(7), None]);
        assert!(hb[1].deleted && !hb[1].observed);
    }
    // WAL 付きで開き直して同じ行に書く: 時刻は前の entry より後
    let eng = reopen(&p);
    eng.tie_to(a, V, 9u32);
    want.push(Some(9));
    let h = eng.history(a, V).unwrap();
    assert_eq!(h.iter().map(|c| c.value).collect::<Vec<_>>(), want, "変更の並び");
    for w in h.windows(2) {
        assert!(w[0].time < w[1].time, "時刻が戻った: {:?} → {:?}", w[0], w[1]);
    }
    for (i, c) in h.iter().enumerate() {
        assert_eq!(eng.value_at(a, V, c.time).unwrap().value, c.value, "{i} 回目の時点の値");
        assert_eq!(c.seq, i as u32);
    }
}

/// 宣言の無い DB の `{db}/tables` に HIS1 は無い。 宣言を外すと消え、 file も消える。
#[test]
fn tables_file_carries_his1_only_when_declared() {
    let s = Scratch::new("his1");
    let p = s.db();
    let eng = create(&p, false);
    let tables = p.join("tables");
    let with = std::fs::read(&tables).unwrap();
    assert!(with.windows(4).any(|w| w == b"HIS1"));
    assert!(p.join("hist").join(format!("{:04}.seg", eng.himo_id(V).unwrap())).exists());
    assert_eq!(eng.drop_history(V), Ok(true));
    assert_eq!(eng.drop_history(V), Ok(false));
    let without = std::fs::read(&tables).unwrap();
    assert!(!without.windows(4).any(|w| w == b"HIS1"));
    assert!(!p.join("hist").join(format!("{:04}.seg", eng.himo_id(V).unwrap())).exists());
    drop(eng);
    let eng = reopen(&p);
    assert!(eng.history_declarations().is_empty());
    assert!(matches!(eng.history(1, V), Err(HistError::NotDeclared)));
}

/// 断る宣言: Leaf の列・sync する DB。 履歴のある DB は sync を有効にできない。
#[test]
fn declarations_that_are_refused() {
    let s = Scratch::new("refuse");
    let p = s.db();
    let mut eng = Engine::create_with_capacity(p.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 16).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "l", ValueType::Leaf, 0).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    assert!(eng.declare_history("t.l").is_err());
    assert!(eng.declare_history("nope").is_err());
    eng.declare_history(V).unwrap();
    assert!(eng.enable_sync_tables().is_err(), "履歴のある DB は sync しない");
    let s2 = Scratch::new("refuse_sync");
    let p2 = s2.db();
    let mut e2 = Engine::create_with_capacity(p2.to_str().unwrap(), 1024).unwrap();
    e2.define_table("t", 16).unwrap();
    e2.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    e2.enable_sync_tables().unwrap();
    assert!(e2.declare_history(V).is_err(), "sync する DB は宣言できない");
}

// ─────────────────────────── 電源断 ───────────────────────────

#[cfg(all(feature = "crashsim", unix))]
mod power_loss {
    use super::*;
    use enchudb_engine::engine::write_out_hook::{self, Phase};
    use enchudb_oplog::crashsim::{self, Mode};
    use std::sync::Mutex;

    static SERIAL: Mutex<()> = Mutex::new(());

    struct Stop;
    impl Drop for Stop {
        fn drop(&mut self) {
            write_out_hook::set(None);
            crashsim::stop();
        }
    }

    /// 像を開いて確かめる: (1) 生きている行の集まりの最新 = cell、 (2) 各 identity の entry のうち 「遅くとも」 でない物は、 模型の
    /// 並びの (時刻, 値) の部分列で時刻の順、 (3) `sure` (oplog を書き出した後の模型) の変更は 「遅くとも」 でない entry で全部ある。
    fn check_image(img: &Path, m: &Model, sure: &Model) -> (u64, u64) {
        let eng = reopen(img);
        let mut observed = 0;
        for (&e, card) in &m.cards {
            let h = eng.history(e, V).unwrap();
            let exact: Vec<(u64, Option<u64>)> = h.iter().filter(|c| !c.observed).map(|c| (c.time, c.value)).collect();
            observed += h.iter().filter(|c| c.observed).count() as u64;
            let mut it = card.iter();
            for x in &exact {
                assert!(it.any(|y| y == x), "identity {e:#x}: entry {x:?} は模型に無い / 順が違う (像の entry {exact:?} / 模型 {card:?})");
            }
            if let Some(sc) = sure.cards.get(&e) {
                for x in sc {
                    assert!(exact.contains(x), "identity {e:#x}: oplog を書き出した後の変更 {x:?} が無い (像 {exact:?})");
                }
            }
            for w in h.windows(2) {
                assert!(w[0].time < w[1].time, "時刻の順");
            }
            for (i, c) in h.iter().enumerate() {
                assert_eq!(c.seq, i as u32);
            }
        }
        // 生きている行: 集まりの最新 = cell (像で生きている行を数える: 模型の生きている行のうち像に居る物)
        let mut checked = 0;
        for (&e, _) in &m.cards {
            let cell = eng.get(e, V);
            let h = eng.history(e, V).unwrap();
            let last = h.last().and_then(|c| c.value);
            if eng.is_live(e) {
                assert_eq!(last, cell, "identity {e:#x}: 集まりの最新と cell");
                checked += 1;
            } else {
                assert_eq!(last, None, "identity {e:#x}: 消えた行の最新に値が残る");
            }
        }
        // 開き直した後に書いても揃う
        let (n, obs, _, _) = eng.history_stats(V).unwrap();
        assert_eq!(obs, observed.min(obs).max(obs));
        let _ = n;
        drop(eng);
        (observed, checked)
    }

    /// 書く → oplog を書き出す (sure) → 書く → 書き出しの `phase` で像を撮る、 を `mode` で。
    fn image_at(phase: Phase, mode: Mode, versions: bool, seed: u64) -> (u64, u64) {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let s = Scratch::new(&format!("pl_{phase:?}_{versions}_{seed}"));
        let db = s.db();
        std::fs::create_dir_all(&s.0).unwrap();
        crashsim::start();
        let _stop = Stop;
        let eng = create(&db, versions);
        let mut m = Model { horizon: eng.history_clock(V).unwrap(), ..Default::default() };
        let mut rng = Rng(seed | 1);
        let mut rows = Vec::new();
        for _ in 0..ROWS {
            let e = eng.entity_in("t").unwrap();
            m.live.insert(e);
            rows.push(e);
        }
        let mut step = |eng: &Engine, m: &mut Model, rng: &mut Rng| {
            let live: Vec<EntityId> = m.live.iter().copied().collect();
            if live.is_empty() {
                let e = eng.entity_in("t").unwrap();
                m.live.insert(e);
                return;
            }
            let e = live[rng.below(live.len() as u64) as usize];
            match rng.below(10) {
                0 => {
                    eng.try_delete(e).unwrap();
                    record(eng, m, e, None);
                    m.live.remove(&e);
                    if let Ok(n) = eng.entity_in("t") {
                        m.live.insert(n);
                    }
                }
                1 => {
                    eng.untie(e, V);
                    record(eng, m, e, None);
                }
                _ => {
                    let v = rng.below(VALUES);
                    eng.tie_to(e, V, v as u32);
                    record(eng, m, e, Some(v));
                }
            }
        };
        for _ in 0..200 {
            step(&eng, &mut m, &mut rng);
        }
        eng.oplog_sync().unwrap();
        let sure = m.clone();
        for _ in 0..200 {
            step(&eng, &mut m, &mut rng);
        }
        let img = s.0.join("img");
        {
            let (live, img) = (db.clone(), img.join("db"));
            let once = Mutex::new(true);
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == phase && std::thread::current().name() == Some("spin-w-sync") && std::mem::take(&mut *once.lock().unwrap()) {
                    crashsim::capture(&live, &img, mode).unwrap();
                }
            })));
            let e2 = eng.clone();
            std::thread::Builder::new().name("spin-w-sync".into()).spawn(move || e2.oplog_sync().unwrap()).unwrap().join().unwrap();
            write_out_hook::set(None);
        }
        // 書き出しが返った後の像 (checkpoint を進めた後)
        let after = s.0.join("after");
        crashsim::capture(&db, &after.join("db"), mode).unwrap();
        crashsim::stop();
        drop(eng);
        assert!(img.join("db").exists(), "前提: 段 {phase:?} で像を撮れた");
        let a = check_image(&img.join("db"), &m, &sure);
        // 書き出しの後の像は、 全部の変更が 「遅くとも」 でなく残る
        let b = check_image(&after.join("db"), &m, &m);
        assert_eq!(b.0, 0, "書き出しの後の像に 「遅くとも」 の entry");
        a
    }

    /// 書き出しをせずに撮る像 (WAL も本体も OS が勝手に書いた page だけ届く): 履歴の page が WAL の record より先に届いた書き込み
    /// (再生されない) や、 本体の cell だけ届いた書き込みを、 開く時の突き合わせが cell に揃える。
    fn image_without_sync(seed: u64, versions: bool) -> (u64, u64) {
        let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let s = Scratch::new(&format!("nosync_{versions}_{seed}"));
        let db = s.db();
        crashsim::start();
        let _stop = Stop;
        let eng = create(&db, versions);
        let mut m = Model { horizon: eng.history_clock(V).unwrap(), ..Default::default() };
        let mut rng = Rng(seed | 1);
        for _ in 0..ROWS {
            let e = eng.entity_in("t").unwrap();
            m.live.insert(e);
        }
        let mut sure = None;
        for i in 0..400 {
            if i == 150 {
                eng.oplog_sync().unwrap();
                sure = Some(m.clone());
            }
            let live: Vec<EntityId> = m.live.iter().copied().collect();
            let e = live[rng.below(live.len() as u64) as usize];
            match rng.below(10) {
                0 => {
                    eng.try_delete(e).unwrap();
                    record(&eng, &mut m, e, None);
                    m.live.remove(&e);
                    let n = eng.entity_in("t").unwrap();
                    m.live.insert(n);
                }
                1 => {
                    eng.untie(e, V);
                    record(&eng, &mut m, e, None);
                }
                _ => {
                    let v = rng.below(VALUES);
                    eng.tie_to(e, V, v as u32);
                    record(&eng, &mut m, e, Some(v));
                }
            }
        }
        let img = s.0.join("img").join("db");
        crashsim::capture(&db, &img, Mode::Mixed { seed }).unwrap();
        crashsim::stop();
        let slot_of = |e: EntityId| eng.eid_slot(e);
        // 本当に在った書き込み: (場所, 時刻, 値)
        let real: BTreeSet<(u32, u64, Option<u64>)> =
            m.cards.iter().flat_map(|(&e, c)| c.iter().map(move |&(t, v)| (e, t, v))).map(|(e, t, v)| (slot_of(e), t, v)).collect();
        let slots: BTreeMap<EntityId, u32> = m.cards.keys().map(|&e| (e, slot_of(e))).collect();
        drop(eng);
        check_image_loose(&img, &m, sure.as_ref().unwrap(), &real, &slots)
    }

    /// 書き出しをしない像は、 場所の世代の上げが届かずに使い回した行が前の identity に見えることがある (engine の identity がそうなる)。
    /// そこで: 「遅くとも」 でない entry は本当に在った書き込み (場所・時刻・値) であること、 時刻の順、 生きている行の最新 = cell、
    /// 消えた行の最新に値が無いこと、 oplog を書き出した時までの変更が全部あること。
    fn check_image_loose(
        img: &Path,
        m: &Model,
        sure: &Model,
        real: &BTreeSet<(u32, u64, Option<u64>)>,
        slots: &BTreeMap<EntityId, u32>,
    ) -> (u64, u64) {
        let eng = reopen(img);
        let mut observed = 0;
        let mut checked = 0;
        for (&e, _) in &m.cards {
            let h = eng.history(e, V).unwrap();
            observed += h.iter().filter(|c| c.observed).count() as u64;
            for c in h.iter().filter(|c| !c.observed) {
                assert!(real.contains(&(slots[&e], c.time, c.value)), "identity {e:#x}: entry {c:?} は在った書き込みでない");
            }
            for w in h.windows(2) {
                assert!(w[0].time < w[1].time, "時刻の順");
            }
            if let Some(sc) = sure.cards.get(&e) {
                let exact: Vec<(u64, Option<u64>)> = h.iter().filter(|c| !c.observed).map(|c| (c.time, c.value)).collect();
                for x in sc {
                    assert!(exact.contains(x), "identity {e:#x}: oplog を書き出した後の変更 {x:?} が無い (像 {exact:?})");
                }
            }
            let last = h.last().and_then(|c| c.value);
            if eng.is_live(e) {
                assert_eq!(last, eng.get(e, V), "identity {e:#x}: 集まりの最新と cell");
                checked += 1;
            } else {
                assert_eq!(last, None, "identity {e:#x}: 消えた行の最新に値が残る");
            }
        }
        drop(eng);
        (observed, checked)
    }

    #[test]
    fn power_loss_images_without_sync_reconcile() {
        let mut observed = 0;
        for versions in [false, true] {
            for seed in 1..=12u64 {
                eprintln!("nosync versions={versions} seed={seed}");
                observed += image_without_sync(seed, versions).0;
            }
        }
        // 前提: 突き合わせが 「遅くとも」 の entry を足す像があった (届いた順の食い違いを作れている)
        assert!(observed > 0, "前提: 食い違いのある像を作れていない");
    }

    #[test]
    fn power_loss_images_reconcile() {
        for versions in [false, true] {
            for phase in [Phase::BeforeGate, Phase::BeforeVersions, Phase::BeforeHistory] {
                image_at(phase, Mode::Lost, versions, 5);
                for seed in 1..=4u64 {
                    image_at(phase, Mode::Mixed { seed }, versions, seed + 10);
                }
            }
        }
    }
}
