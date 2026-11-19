//! #1039: 見直しの購読 (`subscribe_touched`) の `members` / `members_many` / `count_in` は、 group (会社) の eid の行を指す ref で
//! 中身を引く。
//!
//! この購読は 「group が居るか」 を最後の poll で渡した集合で決める (poll の間に入って出た会社の社員をアプリが外せるように)。 なので
//! poll の後に消えた会社の eid も 「居る」。 前は中身を引く時に eid から場所だけを取り、 場所の**今の**世代を付け直して引いたので、
//! poll の後に会社が消えて、 枯渇した会社の表がその場所を使い回すと、 消えた会社の eid で新しい住人 (別の会社) の社員を返した
//! (`count_in` もその人数)。 並びの索引 (`.order(..)` + 範囲の条件) を使う道も同じ。
//!
//! 直し: group の eid の local (世代 | 場所) をそのまま ref の値として引く。 消えた会社の eid では、 その会社を指したままの社員が出る。
//!
//! falsify: `live.rs` の `group_val` を 「場所だけ取って今の世代を付ける」 (`generations().local(split(..).0)`) に戻すと、 4 つの形の
//! どれでも `members` に新しい住人の社員が入り、 `count_in` が 2 → 1 になる。

use enchudb_schema::{Database, Value};

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "{}/enchudb-issue1039-{}-{}-{}-{}",
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

/// 既定の DB (max_entities 2^24) の場所の bit。
const SLOT_MASK: u32 = (1 << 24) - 1;

fn slot(eid: u64) -> u32 {
    (eid as u32) & SLOT_MASK
}

/// `order` = 会社の逆引きを年齢の帯で分ける並びの索引を宣言する、 `ranged` = 根への条件 (年齢 30 以上) を付ける。
fn members_of_a_deleted_and_reused_group(order: bool, ranged: bool) {
    let when = format!("order={order} ranged={ranged}");
    let path = tmp_path("touched");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").with_capacity(2).build().unwrap();
    let mut users = db.table("users").number("id").number("age").ref_to("company", "companies").primary_key("id");
    if order {
        users = users.order("company", "age", &[30, 65]);
    }
    users.build().unwrap();
    let companies = db.get_table("companies").unwrap();
    let users = db.get_table("users").unwrap();
    let c = companies.insert().set("id", 1i64).set("city", "Tokyo").commit().unwrap();
    companies.insert().set("id", 2i64).set("city", "Osaka").commit().unwrap(); // 会社の表を埋める
    let staff: Vec<u64> = (1..=2i64)
        .map(|i| users.insert().set("id", i).set("age", 40i64).set("company", Value::Ref(c)).commit().unwrap())
        .collect();
    let mut q = users.where_eq("company.city", "Tokyo");
    if ranged {
        q = q.where_ge("age", 30i64);
    }
    let t = q.subscribe_touched().unwrap();
    assert_eq!(t.poll().added, vec![c], "{when}: 前提");
    assert_eq!(t.members(c), staff, "{when}: 前提");

    // poll せずに会社を消し、 枯渇した会社の表がその場所を使い回す (東京の別の会社)。 新しい会社に社員を 1 人
    companies.entity(c).delete().unwrap();
    let d = companies.insert().set("id", 3i64).set("city", "Tokyo").commit().unwrap();
    assert_eq!(slot(d), slot(c), "{when}: 前提: 消えた会社の場所を使い回した");
    assert_ne!(d, c, "{when}: 前提: v15 (世代の違う eid)");
    let newcomer = users.insert().set("id", 9i64).set("age", 40i64).set("company", Value::Ref(d)).commit().unwrap();

    // 次の poll の前: 消えた会社 c は最後の poll の集合に居る (型の doc)。 中身は c を指したままの社員で、 d の社員ではない
    let m = t.members(c);
    assert!(!m.contains(&newcomer), "{when}: members(消えた会社) が、 場所を使い回した会社の社員を返した: {m:?}");
    assert_eq!(m, staff, "{when}: members(消えた会社) は、 その会社を指したままの社員");
    assert_eq!(t.members_many(&[c]), vec![staff.clone()], "{when}: members_many");
    assert_eq!(t.count_in(c), staff.len(), "{when}: count_in(消えた会社) が、 場所を使い回した会社の人数になった");
    // 使い回した会社 d はまだ渡していない (次の poll で added)
    assert!(t.members(d).is_empty(), "{when}: まだ渡していない会社の members");

    let delta = t.poll();
    assert_eq!((delta.removed, delta.added), (vec![c], vec![d]), "{when}: 次の poll は出て入り直した");
    assert_eq!(t.members(d), vec![newcomer], "{when}: 使い回した会社の社員");
    assert_eq!(t.count_in(d), 1, "{when}");
}

#[test]
fn touched_members_of_a_deleted_and_reused_group_are_its_own_rows() {
    for order in [false, true] {
        for ranged in [false, true] {
            members_of_a_deleted_and_reused_group(order, ranged);
        }
    }
}
