use enchudb_schema::{Database, Table, LiveQuery};
use enchudb_oplog::eid_local;

fn assert_send_sync<T: Send + Sync>() {}

fn main() {
    assert_send_sync::<Table<'static>>();
    assert_send_sync::<LiveQuery>();
    let dir = std::env::temp_dir().join(format!("schemasem-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut db = Database::create(dir.to_str().unwrap()).unwrap();
    db.table("t").number("id").number("age").number("grp").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    // rows 0..10: id=i, age=7 (row 0 starts at age 6), grp=1
    let mut eids = Vec::new();
    for i in 0..10i64 {
        let e = t.insert().set("id", i).set("age", if i == 0 { 6i64 } else { 7i64 }).set("grp", 1i64).commit().unwrap();
        eids.push(e);
    }
    // build the age index first (lazy build scans the column in eid order), THEN re-tie row 0
    let _ = t.all().order_by("age").limit(1).find().unwrap();
    // row 0: age 6 -> 7  => bucket(7) placement order becomes 1..9,0 while eid order is 0..9
    t.entity(eids[0]).set("age", 7i64).commit().unwrap();
    let loc = |v: Vec<u64>| v.into_iter().map(eid_local).collect::<Vec<_>>();
    println!("== #481 boundary rows: same result set, different plan ==");
    let run = t.all().order_by("age").limit(3).find().unwrap();                       // Walk::Run (no conds)
    let sort = t.where_eq("grp", 1i64).order_by("age").limit(3).find().unwrap();      // Walk::Sort (m^2 <= 2*limit*N)
    let topk = t.all().order_by("age").limit(3).subscribe().unwrap();
    println!("all().order_by(age).limit(3).find()            = {:?}", loc(run.clone()));
    println!("where_eq(grp,1).order_by(age).limit(3).find()  = {:?}", loc(sort.clone()));
    println!("all().order_by(age).limit(3).subscribe().ranked() = {:?}", loc(topk.ranked()));
    println!("full sets equal? {}", {
        let mut a = t.all().order_by("age").find().unwrap(); a.sort();
        let mut b = t.where_eq("grp", 1i64).order_by("age").find().unwrap(); b.sort();
        a == b
    });
    println!("all().order_by(age).find() (no limit, Run path) = {:?}", loc(t.all().order_by("age").find().unwrap()));

    println!("== #475 PK-less rows ==");
    // a row without PK: age=1 (the minimum), grp=1
    let nopk = t.insert().set("age", 1i64).set("grp", 1i64).commit().unwrap();
    println!("inserted PK-less row local={}", eid_local(nopk));
    println!("all().count()                          = {}", t.all().count().unwrap());
    println!("all().find().len()                     = {}", t.all().find().unwrap().len());
    println!("where_range(age,0,1000).count()        = {}", t.where_range("age", 0i64, 1000i64).count().unwrap());
    println!("where_range(age,0,1000).find().len()   = {}", t.where_range("age", 0i64, 1000i64).find().unwrap().len());
    println!("where_eq(grp,1).count()                = {}", t.where_eq("grp", 1i64).count().unwrap());
    println!("where_range(id,0,u32max).count()       = {}", t.where_range("id", 0i64, (u32::MAX - 1) as i64).count().unwrap());
    println!("Table::min(age)                        = {:?}", t.min("age"));
    println!("all().min(age)                         = {:?}", t.all().min("age").unwrap());
    println!("where_range(age,0,1000).min(age)       = {:?}", t.where_range("age", 0i64, 1000i64).min("age").unwrap());
    println!("all().order_by(age).limit(1)           = {:?}", loc(t.all().order_by("age").limit(1).find().unwrap()));
    println!("where_range(age,0,1000).order_by(age).limit(1) = {:?}", loc(t.where_range("age", 0i64, 1000i64).order_by("age").limit(1).find().unwrap()));
    println!("recent(20).len()                       = {}", t.recent(20).len());
    drop(t);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
