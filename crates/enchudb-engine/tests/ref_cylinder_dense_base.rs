//! ref の円柱の dense の窓 (2026-10-11)。 表を 3 つ作ると後ろの表の eid は 2^20 の先になり、 その表を指す ref の値は
//! 円柱の dense の窓 (0 から 2^20) の外 = 全部 sparse に入っていた (sparse は値ごとの古い印を持たないので、 どこかに
//! 古い entry があると読むたびに全員を列で確かめ直し、 会社ごとの読みが遅かった)。 ref を表に定義した時と、 開いて
//! 表の定義を sidecar から採った時に、 窓の始まりを指す先の表の eid の始まりにする。
//!
//! 答えは shadow (書いた値を test の側でも持つ) と、 列を `get` で読む走査の 2 経路で、 書き込み (異動・会社を外す・
//! 社員の削除・新しい社員) のたびに比べる。 開き直した後も同じ。 対照: 行き先の表を登録しない ref (`define_himo_in` の
//! Ref) は窓の始まり 0 のままで、 同じ値でも sparse。

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use std::collections::BTreeMap;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/ref_dense_base_{}_{}_{}.enchu", tag, std::process::id(), nanos)
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

/// 書いた値の写し: 社員 → 会社 (None = 会社が無い)。
struct Shadow {
    path: String,
    comps: Vec<u64>,
    users: Vec<u64>,
    rows: BTreeMap<u64, Option<u64>>,
}

impl Drop for Shadow {
    fn drop(&mut self) {
        let _ = enchudb_engine::db_files::remove_db(&self.path);
    }
}

/// users → posts → companies の順に表を作り (会社の eid は 2^20 の先)、 社員と会社を書いて閉じる。 `fk` なら ref を
/// `define_ref_in` で会社の表に結ぶ (対照は `define_himo_in` の Ref = 行き先を登録しない)。
fn create(tag: &str, seed: u64, fk: bool, n_users: usize, n_comps: usize) -> Shadow {
    let path = tmp_path(tag);
    let _ = enchudb_engine::db_files::remove_db(&path);
    let opts = GrowableOptions { max_entities: 1 << 22, ..Default::default() };
    let mut eng = Engine::create_growable_opts(&path, opts).unwrap();
    eng.define_table("users", 0).unwrap();
    eng.define_table("posts", 0).unwrap();
    eng.define_table("companies", 0).unwrap();
    if fk {
        eng.define_ref_in("users", "company", "companies").unwrap();
    } else {
        eng.define_himo_in("users", "company", ValueType::Ref, 0).unwrap();
    }
    let mut rng = Rng(seed);
    let users: Vec<u64> = (0..n_users).map(|_| eng.entity_in("users").unwrap()).collect();
    let comps: Vec<u64> = (0..n_comps).map(|_| eng.entity_in("companies").unwrap()).collect();
    assert!(comps.iter().all(|&c| enchudb_oplog::eid_local(c) >= 1 << 20), "前提: 会社の eid が 2^20 の先");
    let mut rows = BTreeMap::new();
    for &u in &users {
        let c = comps[rng.below(comps.len() as u64) as usize];
        eng.tie(u, VIA, enchudb_oplog::eid_local(c));
        rows.insert(u, Some(c));
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    drop(eng);
    Shadow { path, comps, users, rows }
}

/// 書き込みを 1 つ (shadow も同じに直す): 異動・会社を外す・社員の削除・新しい社員。
fn write_one(eng: &Engine, s: &mut Shadow, rng: &mut Rng) {
    let i = rng.below(s.users.len() as u64) as usize;
    let u = s.users[i];
    match rng.below(10) {
        0..=5 => {
            let c = s.comps[rng.below(s.comps.len() as u64) as usize];
            eng.tie_to(u, VIA, enchudb_oplog::eid_local(c));
            s.rows.insert(u, Some(c));
        }
        6 => {
            eng.untie(u, VIA);
            s.rows.insert(u, None);
        }
        7 => {
            eng.delete(u);
            s.rows.remove(&u);
            s.users.swap_remove(i);
        }
        _ => {
            let n = eng.entity_in("users").unwrap();
            let c = s.comps[rng.below(s.comps.len() as u64) as usize];
            eng.tie_to(n, VIA, enchudb_oplog::eid_local(c));
            s.rows.insert(n, Some(c));
            s.users.push(n);
        }
    }
}

/// 全部の会社の `pull` を shadow と比べる。 `scan` なら列を直接読む走査とも比べる (shadow の取り違えを見分ける 2 本目の経路)。
fn check(eng: &Engine, s: &Shadow, what: &str, scan: bool) {
    for &c in &s.comps {
        let mut got = eng.pull(VIA, enchudb_oplog::eid_local(c));
        got.sort_unstable();
        let mut want: Vec<u32> =
            s.rows.iter().filter(|&(_, &r)| r == Some(c)).map(|(&u, _)| enchudb_oplog::eid_local(u)).collect();
        want.sort_unstable();
        assert_eq!(got, want, "{what}: pull({c})");
    }
    if scan {
        for (&u, &r) in &s.rows {
            let c = eng.get(u, VIA).map(|l| s.comps.iter().copied().find(|&c| enchudb_oplog::eid_local(c) as u64 == l).unwrap());
            assert_eq!(c, r, "{what}: 列の値 ({u})");
        }
    }
}

fn reopen(eng: Arc<Engine>, path: &str) -> Arc<Engine> {
    eng.flush_writes();
    drop(eng);
    Engine::open(path).unwrap()
}

/// 会社の表を指す ref は、 円柱を組んだ時に窓の始まりが会社の表の eid の始まりになり、 会社の値は dense。 書き込みの
/// たびに答えが 2 経路で合う。 開き直した後 (表の定義を sidecar から採る) も同じ。
#[test]
fn ref_to_a_far_table_is_dense_and_answers_match() {
    let mut s = create("far", 0x5eed_db01, true, 3_000, 200);
    let eng = Engine::open(&s.path).unwrap();
    let (lo, _) = eng.table_eid_range("companies").unwrap();
    let first = enchudb_oplog::eid_local(s.comps[0]) as u64;
    check(&eng, &s, "開いて最初の読み", true);
    assert_eq!(eng.himo_cylinder_low_base(VIA), Some(u64::from(lo)), "窓の始まりが会社の表の始まりでない");
    assert_eq!(eng.himo_value_is_dense(VIA, first), Some(true), "会社の値が dense でない");
    let mut rng = Rng(0x5eed_db02);
    for round in 0..300 {
        for _ in 0..1 + rng.below(5) {
            write_one(&eng, &mut s, &mut rng);
        }
        check(&eng, &s, &format!("round {round}"), round % 50 == 0);
    }
    // 開き直す: 組む前は窓が 0 (まだ組んでいない)、 最初の読みで表の定義から組む
    let eng = reopen(eng, &s.path);
    assert_eq!(eng.himo_cylinder_low_base(VIA), Some(0), "前提: 開いた直後は円柱を組んでいない");
    check(&eng, &s, "開き直した後", true);
    assert_eq!(eng.himo_cylinder_low_base(VIA), Some(u64::from(lo)));
    assert_eq!(eng.himo_value_is_dense(VIA, first), Some(true));
    for round in 0..100 {
        for _ in 0..1 + rng.below(5) {
            write_one(&eng, &mut s, &mut rng);
        }
        check(&eng, &s, &format!("開き直した後 round {round}"), round % 50 == 0);
    }
}

/// 対照: 行き先の表を登録しない ref (`define_himo_in` の Ref) は窓の始まり 0 のままで、 同じ会社の値でも sparse
/// (上の test が 「窓をずらした」 を見分けられること)。 答えは同じ。
#[test]
fn ref_without_a_target_table_stays_sparse() {
    let mut s = create("plain", 0x5eed_db11, false, 3_000, 200);
    let eng = Engine::open(&s.path).unwrap();
    check(&eng, &s, "開いて最初の読み", true);
    let first = enchudb_oplog::eid_local(s.comps[0]) as u64;
    assert_eq!(eng.himo_cylinder_low_base(VIA), Some(0));
    assert_eq!(eng.himo_value_is_dense(VIA, first), Some(false), "対照: 窓をずらしていないのに dense");
    let mut rng = Rng(0x5eed_db12);
    for round in 0..100 {
        for _ in 0..1 + rng.below(5) {
            write_one(&eng, &mut s, &mut rng);
        }
        check(&eng, &s, &format!("round {round}"), round % 50 == 0);
    }
}

/// 新しく作った DB (開き直さない) でも、 `define_ref_in` の時点で窓が決まり、 最初の読みで dense に組む。
#[test]
fn fresh_db_uses_the_target_table_start() {
    let path = tmp_path("fresh");
    let opts = GrowableOptions { max_entities: 1 << 22, ..Default::default() };
    let mut eng = Engine::create_growable_opts(&path, opts).unwrap();
    eng.define_table("users", 0).unwrap();
    eng.define_table("posts", 0).unwrap();
    eng.define_table("companies", 0).unwrap();
    eng.define_ref_in("users", "company", "companies").unwrap();
    let (lo, _) = eng.table_eid_range("companies").unwrap();
    let c = eng.entity_in("companies").unwrap();
    let us: Vec<u64> = (0..50).map(|_| eng.entity_in("users").unwrap()).collect();
    for &u in &us {
        eng.tie(u, VIA, enchudb_oplog::eid_local(c));
    }
    let mut got = eng.pull(VIA, enchudb_oplog::eid_local(c));
    got.sort_unstable();
    let mut want: Vec<u32> = us.iter().map(|&u| enchudb_oplog::eid_local(u)).collect();
    want.sort_unstable();
    assert_eq!(got, want);
    assert_eq!(eng.himo_cylinder_low_base(VIA), Some(u64::from(lo)));
    assert_eq!(eng.himo_value_is_dense(VIA, enchudb_oplog::eid_local(c) as u64), Some(true));
    drop(eng);
    let _ = enchudb_engine::db_files::remove_db(&path);
}
