//! 並びの索引の宣言の保存と開き直し (`Engine::declare_order` / `drop_order`)。 宣言は `{db}/tables` の `DIA1` block に
//! 保存され、 開き直すと engine が自分で戻す (中身は最初に読む時に作る)。
//!
//! 答えは shadow (書いた値を test の側でも持つ) と、 列を `get` で直接読む走査の 2 経路で比べる。 索引を本当に読んだか
//! (正の対照: 読んだ回数、 via の紐の円柱を作っていない) と、 宣言しない DB (負の対照: 宣言が戻らない、 `DIA1` を書かない、
//! 同じ読みで円柱ができる) も見る。

use enchudb_engine::{Engine, GrowableOptions, LivePred, ValueType};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/order_persist_{}_{}_{}_{}.enchu", tag, std::process::id(), nanos, n)
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

const VIA: &str = "users.company";
const AGE: &str = "users.age";
const CITY: &str = "companies.city";
const TICKS: [u64; 2] = [30, 1001];

/// 書いた値の写し。 社員 → (会社, 年齢)、 会社 → city。
struct Shadow {
    path: String,
    comps: Vec<u64>,
    users: Vec<u64>,
    rows: BTreeMap<u64, (Option<u64>, Option<u64>)>,
    city: BTreeMap<u64, u64>,
}

impl Drop for Shadow {
    fn drop(&mut self) {
        let _ = enchudb_engine::db_files::remove_db(&self.path);
        let _ = enchudb_engine::db_files::remove_db(format!("{}.snap", self.path));
    }
}

/// users → posts → companies の順に表を作り (会社の eid は 2^20 の先)、 社員 `n_users` 人・会社 24 社を書いて閉じる。
/// `declare` なら閉じる前に並びを宣言する。
fn create(tag: &str, seed: u64, declare: bool, n_users: usize) -> Shadow {
    let path = tmp_path(tag);
    let _ = enchudb_engine::db_files::remove_db(&path);
    let opts = GrowableOptions { max_entities: 1 << 22, ..Default::default() };
    let mut eng = Engine::create_growable_opts(&path, opts).unwrap();
    eng.define_table("users", 0).unwrap();
    eng.define_table("posts", 0).unwrap();
    eng.define_table("companies", 0).unwrap();
    eng.define_himo_in("companies", "city", ValueType::Number, 4).unwrap();
    eng.define_himo_in("users", "age", ValueType::Number, 0).unwrap();
    eng.define_ref_in("users", "company", "companies").unwrap();
    let mut rng = Rng(seed);
    let users: Vec<u64> = (0..n_users).map(|_| eng.entity_in("users").unwrap()).collect();
    let comps: Vec<u64> = (0..24).map(|_| eng.entity_in("companies").unwrap()).collect();
    assert!(comps.iter().all(|&c| enchudb_oplog::eid_local(c) >= 1 << 20), "前提: 会社の eid が 2^20 の先");
    let mut city = BTreeMap::new();
    for &c in &comps {
        let v = rng.below(3);
        eng.tie(c, CITY, v as u32);
        city.insert(c, v);
    }
    let mut rows = BTreeMap::new();
    for &u in &users {
        let c = comps[rng.below(comps.len() as u64) as usize];
        let a = 18 + rng.below(62);
        eng.tie(u, VIA, enchudb_oplog::eid_local(c));
        eng.tie(u, AGE, a as u32);
        rows.insert(u, (Some(c), Some(a)));
    }
    if declare {
        eng.declare_order(VIA, AGE, &TICKS).unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    drop(eng);
    Shadow { path, comps, users, rows, city }
}

/// 書き込みを 1 つ (shadow も同じに直す): 年齢 (1000 / 1001 をまたぐもの含む)・年齢を外す・異動・会社を外す・city・
/// 社員の削除・新しい社員。
fn write_one(eng: &Engine, s: &mut Shadow, rng: &mut Rng) {
    let i = rng.below(s.users.len() as u64) as usize;
    let u = s.users[i];
    match rng.below(16) {
        0..=4 => {
            let a = if rng.below(8) == 0 { 990 + rng.below(110) } else { 18 + rng.below(62) };
            eng.tie_to(u, AGE, a as u32);
            s.rows.get_mut(&u).unwrap().1 = Some(a);
        }
        5 => {
            eng.untie(u, AGE);
            s.rows.get_mut(&u).unwrap().1 = None;
        }
        6..=9 => {
            let c = s.comps[rng.below(s.comps.len() as u64) as usize];
            eng.tie_to(u, VIA, enchudb_oplog::eid_local(c));
            s.rows.get_mut(&u).unwrap().0 = Some(c);
        }
        10 => {
            eng.untie(u, VIA);
            s.rows.get_mut(&u).unwrap().0 = None;
        }
        11..=12 => {
            let c = s.comps[rng.below(s.comps.len() as u64) as usize];
            let v = rng.below(3);
            eng.tie_to(c, CITY, v as u32);
            s.city.insert(c, v);
        }
        13 => {
            eng.delete(u);
            s.rows.remove(&u);
            s.users.swap_remove(i);
        }
        _ => {
            let n = eng.entity_in("users").unwrap();
            let c = s.comps[rng.below(s.comps.len() as u64) as usize];
            let a = 18 + rng.below(62);
            eng.tie_to(n, VIA, enchudb_oplog::eid_local(c));
            eng.tie_to(n, AGE, a as u32);
            s.rows.insert(n, (Some(c), Some(a)));
            s.users.push(n);
        }
    }
}

/// 会社 `c` を指している社員 (local eid、 昇順)。 shadow から。
fn staff_of(s: &Shadow, c: u64) -> Vec<u32> {
    let mut v: Vec<u32> =
        s.rows.iter().filter(|(_, r)| r.0 == Some(c)).map(|(&u, _)| enchudb_oplog::eid_local(u)).collect();
    v.sort_unstable();
    v
}

/// city が `city` の会社の、 年齢が `lo..=hi` の社員: Via + Range の find_by。
fn find_aged(eng: &Engine, city: u64, lo: u64, hi: u64) -> BTreeSet<u64> {
    let h = |n: &str| eng.himo_id(n).unwrap() as u16;
    let preds = vec![
        LivePred::Via { path: vec![h(VIA)], pred: Box::new(LivePred::Eq { himo_id: h(CITY), value: city }) },
        LivePred::Range { himo_id: h(AGE), lo, hi },
    ];
    eng.find_by(preds).unwrap().into_iter().collect()
}

/// 上の答えを shadow から。
fn want_aged(s: &Shadow, city: u64, lo: u64, hi: u64) -> BTreeSet<u64> {
    s.rows
        .iter()
        .filter(|(_, r)| r.0.is_some_and(|c| s.city.get(&c) == Some(&city)) && r.1.is_some_and(|a| lo <= a && a <= hi))
        .map(|(&u, _)| u)
        .collect()
}

/// 全部の会社の `pull` と、 city ごと・範囲ごとの find_by を shadow と比べる。 `scan` なら列を直接読む走査とも比べる
/// (shadow の取り違えを見分ける 2 本目の経路)。
fn check(eng: &Engine, s: &Shadow, what: &str, scan: bool) {
    for &c in &s.comps {
        let mut got = eng.pull(VIA, enchudb_oplog::eid_local(c));
        got.sort_unstable();
        assert_eq!(got, staff_of(s, c), "{what}: pull({c})");
    }
    for city in 0..3 {
        for (lo, hi) in [(30, 1000), (0, 29), (1001, u32::MAX as u64 - 1), (40, 60)] {
            assert_eq!(find_aged(eng, city, lo, hi), want_aged(s, city, lo, hi), "{what}: find_by city {city} age {lo}..={hi}");
        }
    }
    if scan {
        for (&u, r) in &s.rows {
            let c = eng.get(u, VIA).map(|l| s.comps.iter().copied().find(|&c| enchudb_oplog::eid_local(c) as u64 == l).unwrap());
            assert_eq!((c, eng.get(u, AGE)), *r, "{what}: 列の値 ({u})");
        }
        for (&c, &v) in &s.city {
            assert_eq!(eng.get(c, CITY), Some(v), "{what}: city ({c})");
        }
    }
}

/// 索引を読んだ回数の和。
fn order_hits(eng: &Engine) -> u64 {
    eng.order_stats().iter().map(|s| s.2).sum()
}

fn via_cylinder_bytes(eng: &Engine) -> usize {
    eng.himo_cylinder_backing_bytes(VIA).expect("company の紐")
}

fn tables_bytes(path: &str) -> Vec<u8> {
    std::fs::read(enchudb_engine::db_files::path_for(path, enchudb_engine::db_files::TABLES)).unwrap()
}

fn has_dia1(path: &str) -> bool {
    tables_bytes(path).windows(4).any(|w| w == b"DIA1")
}

fn decl() -> Vec<(String, String, Vec<u64>)> {
    vec![(VIA.to_string(), AGE.to_string(), TICKS.to_vec())]
}

/// 閉じて開き直す (WAL 付き)。 他に Arc を持っていないこと。
fn reopen(eng: Arc<Engine>, path: &str) -> Arc<Engine> {
    eng.flush_writes();
    drop(eng);
    Engine::open(path).unwrap()
}

/// 宣言は開き直しても戻る。 戻った直後は作っていない (読んだ回数 0)、 最初の読みで作り、 答えは shadow / 列の走査と一致、
/// via の紐の円柱は作らない。 作る前の書き込み (開き直した直後、 WAL の replay) も見落とさない: 書き込みを 2 回目の
/// 開き直しの前に WAL に積み、 開き直した後 (replay の後) に初めて読む。
#[test]
fn declaration_survives_reopen_and_answers_match() {
    let mut s = create("reopen", 0x5eed_d001, true, 600);
    assert!(has_dia1(&s.path), "宣言を DIA1 に書いていない");
    let eng = Engine::open(&s.path).unwrap();
    assert_eq!(eng.order_declarations(), decl(), "開き直しで宣言が戻らない");
    assert_eq!(order_hits(&eng), 0, "前提: 開いた直後は読んでいない");
    // 作る前 (最初の読みの前) の書き込み
    let mut rng = Rng(0x5eed_d002);
    for _ in 0..300 {
        write_one(&eng, &mut s, &mut rng);
    }
    check(&eng, &s, "1 回目の開き直し", true);
    assert!(order_hits(&eng) > 0, "索引を読んでいない");
    assert_eq!(via_cylinder_bytes(&eng), 0, "宣言した紐の円柱を作った");
    // 作った後の書き込み、 毎回の答え合わせ
    for round in 0..40 {
        for _ in 0..1 + rng.below(6) {
            write_one(&eng, &mut s, &mut rng);
        }
        check(&eng, &s, &format!("round {round}"), false);
    }
    // 2 回目: 閉じる前に書いた分は WAL の replay で戻る (宣言は replay の前に戻るが、 作るのは replay の後の最初の読み)
    for _ in 0..200 {
        write_one(&eng, &mut s, &mut rng);
    }
    let eng = reopen(eng, &s.path);
    assert_eq!(eng.order_declarations(), decl());
    assert_eq!(order_hits(&eng), 0);
    check(&eng, &s, "2 回目の開き直し (replay の後)", true);
    assert!(order_hits(&eng) > 0);
    assert_eq!(via_cylinder_bytes(&eng), 0);
}

/// 負の対照: 宣言しない DB は、 開き直しても宣言が無く、 `{db}/tables` に DIA1 を書かず、 同じ読みで円柱ができる
/// (上の test が 「索引を戻した」 「円柱を作らない」 を見分けられること)。 答えは同じ。
#[test]
fn without_declaration_nothing_is_stored() {
    let mut s = create("plain", 0x5eed_d011, false, 600);
    assert!(!has_dia1(&s.path));
    let eng = Engine::open(&s.path).unwrap();
    assert!(eng.order_declarations().is_empty());
    let mut rng = Rng(0x5eed_d012);
    for _ in 0..300 {
        write_one(&eng, &mut s, &mut rng);
    }
    check(&eng, &s, "宣言なし", true);
    assert_eq!(order_hits(&eng), 0);
    assert!(via_cylinder_bytes(&eng) > 0, "対照: 宣言なしの pull が円柱を作っていない");
    eng.persist_tables().unwrap();
    assert!(!has_dia1(&s.path), "宣言の無い DB に DIA1 を書いた");
}

/// 開くだけでは `{db}/tables` を書き直さない (#259 / #261 と同じ約束: 宣言を戻すのは保存しない道)。 読んで索引を作っても
/// 書き直さない (宣言は変わっていないので)。
#[test]
fn open_and_build_do_not_rewrite_tables_sidecar() {
    let s = create("mtime", 0x5eed_d021, true, 600);
    let file = enchudb_engine::db_files::path_for(&s.path, enchudb_engine::db_files::TABLES);
    let before = (std::fs::metadata(&file).unwrap().modified().unwrap(), tables_bytes(&s.path));
    std::thread::sleep(std::time::Duration::from_millis(30));
    let eng = Engine::open(&s.path).unwrap();
    check(&eng, &s, "開いて読む", false);
    assert!(order_hits(&eng) > 0);
    // 同じ宣言をもう一度しても書き直さない
    eng.declare_order(VIA, AGE, &TICKS).unwrap();
    let after = (std::fs::metadata(&file).unwrap().modified().unwrap(), tables_bytes(&s.path));
    assert_eq!(after, before, "開いた / 作った / 同じ宣言で tables sidecar を書き直した");
}

/// 目盛りを変える (置き換え) と、 新しい索引を作り直して答える。 外すと円柱に戻る。 どちらも保存に効き、 開き直すと
/// 新しい宣言 / 宣言なしが戻る。 置き換え・外すの間も書き込みと答え合わせを続ける。
#[test]
fn redeclare_replaces_and_drop_falls_back() {
    let mut s = create("redeclare", 0x5eed_d031, true, 600);
    let eng = Engine::open(&s.path).unwrap();
    let mut rng = Rng(0x5eed_d032);
    check(&eng, &s, "置き換えの前", false);
    assert!(order_hits(&eng) > 0);
    for _ in 0..100 {
        write_one(&eng, &mut s, &mut rng);
    }
    eng.declare_order(VIA, AGE, &[50]).unwrap();
    assert_eq!(eng.order_declarations(), vec![(VIA.to_string(), AGE.to_string(), vec![50])]);
    assert_eq!(order_hits(&eng), 0, "置き換えた後の一覧は新しい索引だけ (まだ読んでいない)");
    for round in 0..20 {
        for _ in 0..1 + rng.below(6) {
            write_one(&eng, &mut s, &mut rng);
        }
        check(&eng, &s, &format!("置き換えの後 round {round}"), false);
    }
    assert!(order_hits(&eng) > 0, "新しい索引を読んでいない");
    assert_eq!(via_cylinder_bytes(&eng), 0, "置き換えで円柱を作った");
    eng.flush_writes();
    assert!(has_dia1(&s.path), "置き換えた宣言を保存していない");
    let eng = reopen(eng, &s.path);
    assert_eq!(eng.order_declarations(), vec![(VIA.to_string(), AGE.to_string(), vec![50])], "置き換えが戻らない");
    check(&eng, &s, "置き換えの後の開き直し", true);
    // 外す
    assert!(eng.drop_order(VIA).unwrap());
    assert!(!eng.drop_order(VIA).unwrap(), "2 回目は外すものが無い");
    assert!(eng.order_declarations().is_empty());
    for round in 0..20 {
        for _ in 0..1 + rng.below(6) {
            write_one(&eng, &mut s, &mut rng);
        }
        check(&eng, &s, &format!("外した後 round {round}"), false);
    }
    assert_eq!(order_hits(&eng), 0, "外した索引を読んだ");
    assert!(via_cylinder_bytes(&eng) > 0, "外した後は円柱で読むはず");
    assert!(!has_dia1(&s.path), "外した宣言が保存に残っている");
    let eng = reopen(eng, &s.path);
    assert!(eng.order_declarations().is_empty(), "外した宣言が開き直しで戻った");
    check(&eng, &s, "外した後の開き直し", true);
}

/// snapshot (`snapshot_export`) にも宣言が乗り、 写しを開くと戻る。
#[test]
fn snapshot_export_keeps_declarations() {
    let mut s = create("snap", 0x5eed_d041, true, 600);
    let eng = Engine::open(&s.path).unwrap();
    let mut rng = Rng(0x5eed_d042);
    for _ in 0..200 {
        write_one(&eng, &mut s, &mut rng);
    }
    eng.flush_writes();
    let dst = format!("{}.snap", s.path);
    eng.snapshot_export(&dst).unwrap();
    drop(eng);
    let snap = Engine::open(&dst).unwrap();
    assert_eq!(snap.order_declarations(), decl(), "snapshot に宣言が乗っていない");
    check(&snap, &s, "snapshot", true);
    assert!(order_hits(&snap) > 0);
}

/// 開き直した後、 最初の読み (= 作る) と並行に書き手 3 本が走る。 書き手は作る前は鍵を取らずに素通りし、 作る側は
/// via と key の紐の鍵を両方取ってから列をなめる。 止めた後の答えが shadow と列の走査に一致。 書き手ごとに別の社員を
/// 受け持つので、 shadow は書き手ごとに持って最後に合わせる。
///
/// 2 本は年齢だけを書く (30 の境目をまたぐ 20 / 40 と、 年齢を外す)。 異動も書く書き手は、 作る間は会社の紐の鍵で
/// 止まるので、 作る間の年齢の書き込みを重ねるのは年齢だけの書き手 (2026-10-11 の計測: 3 本とも異動を混ぜると、 2 万人を
/// 作る 7 ms の間に書けたのは 0〜1 回)。 社員は 2 万人 (作る時間を書き込み数百回ぶんにする)。 実測 (2026-10-11): 作る側が
/// key の鍵を取らない形に壊すと落ちる。
#[test]
fn concurrent_writers_while_the_first_read_builds() {
    for trial in 0..6u64 {
        let mut s = create("concurrent", 0x5eed_d051 + trial, true, 20_000);
        let eng = Engine::open(&s.path).unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..3usize)
            .map(|t| {
                let eng = eng.clone();
                let mine: Vec<u64> = s.users.iter().copied().skip(t).step_by(3).collect();
                let comps = s.comps.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng(0xc0c0_d000 ^ (trial << 8) ^ (t as u64 + 1));
                    let mut out: BTreeMap<u64, (Option<u64>, Option<u64>)> = BTreeMap::new();
                    let mut n = 0u64;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) || n < 200 {
                        let u = mine[rng.below(mine.len() as u64) as usize];
                        if t < 2 {
                            // 年齢だけ: 帯の境目 (30) をまたぐ値
                            if rng.below(10) == 0 {
                                eng.untie(u, AGE);
                                out.entry(u).or_insert((None, None)).1 = Some(u64::MAX);
                            } else {
                                let a = if rng.below(2) == 0 { 20 } else { 40 };
                                eng.tie_to(u, AGE, a as u32);
                                out.entry(u).or_insert((None, None)).1 = Some(a);
                            }
                            n += 1;
                            continue;
                        }
                        match rng.below(10) {
                            0..=4 => {
                                let a = 18 + rng.below(62);
                                eng.tie_to(u, AGE, a as u32);
                                out.entry(u).or_insert((None, None)).1 = Some(a);
                            }
                            5..=8 => {
                                let c = comps[rng.below(comps.len() as u64) as usize];
                                eng.tie_to(u, VIA, enchudb_oplog::eid_local(c));
                                out.entry(u).or_insert((None, None)).0 = Some(c);
                            }
                            _ => {
                                eng.untie(u, AGE);
                                out.entry(u).or_insert((None, None)).1 = Some(u64::MAX);
                            }
                        }
                        n += 1;
                    }
                    out
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(2));
        // 最初の読み = 作る。 書き手が走っている間に何度か読む
        let t0 = std::time::Instant::now();
        while t0.elapsed() < std::time::Duration::from_millis(60) {
            for &c in &s.comps {
                let _ = eng.pull(VIA, enchudb_oplog::eid_local(c));
            }
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in writers {
            for (u, (c, a)) in w.join().unwrap() {
                let r = s.rows.get_mut(&u).unwrap();
                if c.is_some() {
                    r.0 = c;
                }
                match a {
                    Some(u64::MAX) => r.1 = None,
                    Some(a) => r.1 = Some(a),
                    None => {}
                }
            }
        }
        check(&eng, &s, &format!("trial {trial}"), true);
        assert!(order_hits(&eng) > 0);
    }
}
