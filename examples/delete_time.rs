//! 1 行 1 commit の delete の時間 (dbq の churn の delete_row と同じ表・同じ順、 #603 の後の退行を見る道具)。
//!
//! `cargo run --release --example delete_time -- <pk 0|1> [n 1000000] [seed 1]`
//!
//! 表 `t (id, age, score, grp, ts)` に n 行を 1 行ずつ入れ、 id 以外の各列を 1 回ずつ引いて円柱を組み、 10% の行を書き換えてから、
//! 1% の行を乱数の順に 1 行ずつ消す。 pk = 1 は id を主キーにする (insert が名前を引くので id の円柱も組まれる)、 0 は主キーなし (id の円柱は組まれない)。
//! 出力は 1 行の JSON (`delete_row_ns` = 消し 1 行あたり)。

use enchudb::schema::Database;
use std::time::Instant;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let pk = a.get(1).map(|s| s == "1").unwrap_or(true);
    let n: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let seed: u64 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
    let dir = std::env::var("DELETE_TIME_DIR").unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into_owned());
    let path = format!("{dir}/delete_time_{}_{seed}.db", std::process::id());
    let _ = enchudb::db_files::remove_db(&path);
    let mut db = Database::create_growable_with_capacity(&path, (n as u32).saturating_mul(2).max(1 << 16)).unwrap();
    let b = db.table("t").number("id").number("age").number("score").number("grp").bigint("ts");
    let b = if pk { b.primary_key("id") } else { b };
    b.with_capacity((n as u32).saturating_add(n as u32 / 2).max(1 << 16)).build().unwrap();
    let t = db.get_table("t").unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed);
    let row = |rng: &mut Rng| (rng.below(100) as i64, rng.below(10_000) as i64, rng.below(1000) as i64, rng.below(1 << 40) as i64);

    let t0 = Instant::now();
    let mut eid = Vec::with_capacity(n);
    for id in 0..n {
        let (age, score, grp, ts) = row(&mut rng);
        eid.push(t.insert().set("id", id as i64).set("age", age).set("score", score).set("grp", grp).set("ts", ts).commit().unwrap());
    }
    let insert_row = t0.elapsed().as_nanos() as f64 / n as f64;

    // id 以外の列の円柱を組む (dbq の static の問いと同じ。 dbq は id を引かない = 主キーなしなら id の円柱は組まれない)
    let _ = t.where_eq("age", 30i64).find().unwrap();
    let _ = t.where_range("score", 100, 200).find().unwrap();
    let _ = t.where_eq("grp", 7i64).find().unwrap();
    let _ = t.all().where_gt("ts", 1i64 << 39).limit(10).find().unwrap();

    let mut alive = vec![true; n];
    for _ in 0..n / 10 {
        let i = rng.below(n as u64) as usize;
        let (age, score, grp, ts) = row(&mut rng);
        t.entity(eid[i]).update().set("age", age).set("score", score).set("grp", grp).set("ts", ts).commit().unwrap();
    }

    let t1 = Instant::now();
    let mut ndel = 0;
    for _ in 0..n / 100 {
        let i = rng.below(n as u64) as usize;
        if alive[i] {
            t.entity(eid[i]).delete().unwrap();
            alive[i] = false;
            ndel += 1;
        }
    }
    let delete_row = t1.elapsed().as_nanos() as f64 / ndel.max(1) as f64;
    let left = t.all().count().unwrap();
    assert_eq!(left, n - ndel, "消した後の行の数");
    println!(
        "{{\"pk\":{},\"n\":{n},\"seed\":{seed},\"insert_row_ns\":{insert_row:.1},\"deleted\":{ndel},\"delete_row_ns\":{delete_row:.1}}}",
        pk as u8
    );
    drop(db);
    let _ = enchudb::db_files::remove_db(&path);
}
