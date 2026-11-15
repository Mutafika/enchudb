//! #899: Ref の紐の円柱の鍵を場所にした。 #731 (PR #885) で Ref の cell は 世代 | 場所 を持つので、 使い回した会社 (世代 > 0) を
//! 指す ref の値は円柱の dense の窓の外 (sparse) に入り、 会社ごとの逆引きが 7.8 倍遅かった (#463 の形に戻った)。 今は鍵 = 場所、
//! entry = 値の世代 | 社員 で、 前の世代を指す ref (消えた会社を指したまま) と今の世代を指す ref (場所を使い回した会社を指す) が
//! 同じ bucket に混ざる。
//!
//! 会社の表 (枠 6) の会社を消して足す (= 場所を使い回す) のを繰り返しながら社員の ref を書き換え、 書いた値の写し (shadow) と
//! 次の答えを毎回比べる: 逆引き (`where_ref`、 消えた会社も)、 dotted path の find と購読の差分 (`company.city = Tokyo`、
//! `company.city is null` = 消えた会社を指す ref は中身の無い行を指す ref)、 会社単位の購読の members (並びの索引 `.order` の帯で
//! 読む `age >= 30`)。 開き直した後 (円柱を列から組み直す) も同じ。 使い回した会社を指す ref の値が円柱の dense に置かれている
//! こと (速さの元) も状態で確かめる (前は sparse)。

use enchudb_schema::{Database, Value};
use std::collections::{BTreeMap, BTreeSet};

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "{}/enchudb-issue899-{}-{}-{}-{}",
        std::env::temp_dir().display(),
        tag,
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
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

/// 会社の表の枠 (埋まった後に足すと、 消した会社の場所を使い回す)。
const CAP: u32 = 6;
const CITIES: [&str; 3] = ["Tokyo", "Osaka", "Kyoto"];
const VIA: &str = "users.company";
/// 既定の DB (max_entities 2^24) の場所の bit。
const SLOT_MASK: u64 = (1 << 24) - 1;

/// 書いた値の写し。
#[derive(Default)]
struct Shadow {
    /// 社員 → (会社 (消えた会社も、 指したまま), 年齢)
    users: BTreeMap<u64, (Option<u64>, i64)>,
    /// 生きている会社 → city
    alive: BTreeMap<u64, &'static str>,
    /// 一度でも作った会社 (消えた会社も)
    comps: BTreeSet<u64>,
}

impl Shadow {
    fn pointing_at(&self, c: u64) -> BTreeSet<u64> {
        self.users.iter().filter(|(_, (co, _))| *co == Some(c)).map(|(&u, _)| u).collect()
    }
    fn in_tokyo(&self) -> BTreeSet<u64> {
        self.users.iter().filter(|(_, (co, _))| co.is_some_and(|c| self.alive.get(&c) == Some(&"Tokyo"))).map(|(&u, _)| u).collect()
    }
    /// `company.city is null`: 会社を指していて、 その会社が消えている (生きている会社の city はいつも書いてある)
    fn null_city(&self) -> BTreeSet<u64> {
        self.users.iter().filter(|(_, (co, _))| co.is_some_and(|c| !self.alive.contains_key(&c))).map(|(&u, _)| u).collect()
    }
}

fn build(db: &mut Database) {
    db.table("companies").number("id").tag("city").primary_key("id").with_capacity(CAP).build().unwrap();
    db.table("users")
        .number("id")
        .ref_to("company", "companies")
        .number("age")
        .primary_key("id")
        .order("company", "age", &[30, 65])
        .build()
        .unwrap();
}

/// 逆引きと dotted path の find を shadow と比べ、 生きている会社を指す ref の値が円柱の dense にあるかを確かめる。
fn check_reads(db: &Database, s: &Shadow, when: &str) {
    let users = db.get_table("users").unwrap();
    for &c in &s.comps {
        let got: BTreeSet<u64> = users.where_ref("company", c).find().unwrap().into_iter().collect();
        assert_eq!(got, s.pointing_at(c), "{when}: where_ref 会社 {c:#x} (生きている: {})", s.alive.contains_key(&c));
    }
    let tokyo: BTreeSet<u64> = users.where_eq("company.city", "Tokyo").find().unwrap().into_iter().collect();
    assert_eq!(tokyo, s.in_tokyo(), "{when}: find company.city = Tokyo");
    let null: BTreeSet<u64> = users.all().where_null("company.city").find().unwrap().into_iter().collect();
    assert_eq!(null, s.null_city(), "{when}: find company.city is null");
    // 速さの元: 生きている会社 (使い回した会社も) を指す ref の値は円柱の dense に置く (前は世代 > 0 の値が sparse)
    let eng = db.engine();
    for &c in s.alive.keys() {
        assert_eq!(
            eng.himo_value_is_dense(VIA, eng.ref_value(c)),
            Some(true),
            "{when}: 会社 {c:#x} を指す ref の値が円柱の dense に無い"
        );
    }
}

#[test]
fn refs_to_old_and_new_generations_on_one_slot_read_like_the_shadow() {
    for seed in [1u64, 2, 3] {
        run(seed);
    }
}

fn run(seed: u64) {
    let path = tmp_path("mix");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    build(&mut db);
    let mut s = Shadow::default();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed);
    let (mut next_user, mut next_comp) = (0i64, 0i64);
    {
        let companies = db.get_table("companies").unwrap();
        let users = db.get_table("users").unwrap();
        for _ in 0..CAP {
            let city = CITIES[rng.below(3) as usize];
            let c = companies.insert().set("id", next_comp).set("city", city).commit().unwrap();
            next_comp += 1;
            s.alive.insert(c, city);
            s.comps.insert(c);
        }
        for _ in 0..120 {
            let c = *s.alive.keys().nth(rng.below(s.alive.len() as u64) as usize).unwrap();
            let age = rng.below(90) as i64;
            let u = users.insert().set("id", next_user).set("company", Value::Ref(c)).set("age", age).commit().unwrap();
            next_user += 1;
            s.users.insert(u, (Some(c), age));
        }
    }
    // 円柱を使い回しの前に組む (世代を持たない間に組んだ円柱を、 世代を持った後も使い続ける形)
    check_reads(&db, &s, "最初");
    let users = db.get_table("users").unwrap();
    let companies = db.get_table("companies").unwrap();
    let tokyo_sub = users.where_eq("company.city", "Tokyo").subscribe().unwrap();
    let null_sub = users.all().where_null("company.city").subscribe().unwrap();
    let grouped = users.where_eq("company.city", "Tokyo").where_ge("age", 30i64).subscribe_grouped().unwrap();
    let (mut seen_tokyo, mut seen_null, mut seen_groups) = (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    let integrate = |seen: &mut BTreeSet<u64>, d: enchudb_schema::LiveDelta, what: &str, when: &str| {
        for e in d.removed {
            assert!(seen.remove(&e), "{when}: {what} の removed に渡していない eid {e:#x}");
        }
        for e in d.added {
            assert!(seen.insert(e), "{when}: {what} の added に渡してある eid {e:#x}");
        }
    };
    // 同じ場所に、 前の世代を指す ref と今の世代を指す ref が混ざった回数 (前提)
    let mut mixed_seen = 0usize;
    let mut reused = 0usize;
    for step in 0..400 {
        let when = format!("seed {seed} step {step}");
        let pick_user = |s: &Shadow, rng: &mut Rng| *s.users.keys().nth(rng.below(s.users.len() as u64) as usize).unwrap();
        let pick_alive = |s: &Shadow, rng: &mut Rng| *s.alive.keys().nth(rng.below(s.alive.len() as u64) as usize).unwrap();
        match rng.below(10) {
            // 社員の会社を書き換える (生きている会社へ。 消えた会社を指したままの社員を、 その場所を使い回した会社へ指し直すこともある)
            0..=2 => {
                let u = pick_user(&s, &mut rng);
                let c = pick_alive(&s, &mut rng);
                users.entity(u).set("company", Value::Ref(c)).commit().unwrap();
                s.users.get_mut(&u).unwrap().0 = Some(c);
            }
            // 社員を消して、 新しい社員を足す
            3 => {
                let u = pick_user(&s, &mut rng);
                users.entity(u).delete().unwrap();
                s.users.remove(&u);
                let c = pick_alive(&s, &mut rng);
                let age = rng.below(90) as i64;
                let n = users.insert().set("id", next_user).set("company", Value::Ref(c)).set("age", age).commit().unwrap();
                next_user += 1;
                s.users.insert(n, (Some(c), age));
            }
            // 会社を消して、 新しい会社を足す (= 消した会社の場所を使い回す)。 消した会社を指す社員は指したまま
            4 | 5 => {
                let c = pick_alive(&s, &mut rng);
                companies.entity(c).delete().unwrap();
                s.alive.remove(&c);
                let city = CITIES[rng.below(3) as usize];
                let n = companies.insert().set("id", next_comp).set("city", city).commit().unwrap();
                next_comp += 1;
                assert!(s.comps.insert(n), "{when}: 会社の eid を二度渡した");
                if n & SLOT_MASK == c & SLOT_MASK {
                    reused += 1;
                }
                s.alive.insert(n, city);
            }
            // 会社の city を書き換える
            6 => {
                let c = pick_alive(&s, &mut rng);
                let city = CITIES[rng.below(3) as usize];
                companies.entity(c).set("city", city).commit().unwrap();
                s.alive.insert(c, city);
            }
            // 社員の年齢 (並びの索引の帯) を書き換える
            7 => {
                let u = pick_user(&s, &mut rng);
                let age = rng.below(90) as i64;
                users.entity(u).set("age", age).commit().unwrap();
                s.users.get_mut(&u).unwrap().1 = age;
            }
            // 社員の会社を外す (schema に列を外す書き込みは無いので engine で)
            _ => {
                let u = pick_user(&s, &mut rng);
                db.engine().untie(u, VIA);
                s.users.get_mut(&u).unwrap().0 = None;
            }
        }
        // 前提の数え: 生きている会社の場所に、 その場所の前の住人 (消えた会社) を指したままの社員がいる
        for &c in s.alive.keys() {
            let here = |x: u64| x & SLOT_MASK == c & SLOT_MASK;
            let old = s.users.values().any(|(co, _)| co.is_some_and(|x| here(x) && x != c));
            let now = s.users.values().any(|(co, _)| *co == Some(c));
            if old && now {
                mixed_seen += 1;
            }
        }
        check_reads(&db, &s, &when);
        integrate(&mut seen_tokyo, tokyo_sub.poll(), "company.city = Tokyo", &when);
        assert_eq!(seen_tokyo, s.in_tokyo(), "{when}: 購読 company.city = Tokyo");
        integrate(&mut seen_null, null_sub.poll(), "company.city is null", &when);
        assert_eq!(seen_null, s.null_city(), "{when}: 購読 company.city is null");
        integrate(&mut seen_groups, grouped.poll(), "会社単位", &when);
        // 会社単位の購読の会社 = dotted の条件 (city = Tokyo) を満たす会社 (社員側の条件は members で絞る)
        let want_groups: BTreeSet<u64> = s.alive.iter().filter(|&(_, &city)| city == "Tokyo").map(|(&c, _)| c).collect();
        assert_eq!(seen_groups, want_groups, "{when}: 会社単位の購読の会社");
        for &c in &want_groups {
            let got: BTreeSet<u64> = grouped.members(c).into_iter().collect();
            let want: BTreeSet<u64> = s.users.iter().filter(|(_, (co, age))| *co == Some(c) && *age >= 30).map(|(&u, _)| u).collect();
            assert_eq!(got, want, "{when}: 会社 {c:#x} の members (age >= 30、 並びの索引)");
        }
    }
    assert!(reused > 50, "前提: 会社の場所の使い回しが起きていない ({reused})");
    assert!(mixed_seen > 50, "前提: 同じ場所に前の世代と今の世代の ref が混ざっていない ({mixed_seen})");
    assert!(db.engine().slot_bits() > 0, "前提: 世代を持つ DB (v15)");
    drop(grouped);
    drop(null_sub);
    drop(tokyo_sub);
    drop(db);
    // 開き直す: 円柱を列から組み直す (前の世代と今の世代の entry が混ざった bucket を最初から組む)
    let db = Database::open(&path).unwrap();
    check_reads(&db, &s, &format!("seed {seed} 開き直した後"));
}
