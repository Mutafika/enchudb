/// v26 書き込み系ベンチ — クエリ以外の操作が遅くなってないか確認

use std::time::Instant;

fn main() {
    let n = 100_000u32;
    let dir = "/tmp/enchudb_v26_write.db";
    let _ = std::fs::remove_file(dir);

    let mut db = enchudb::Engine::create_standalone(dir).unwrap();
    db.define_himo("city", enchudb::HimoType::Value, 10);
    db.define_himo("dept", enchudb::HimoType::Value, 8);
    db.define_himo("age", enchudb::HimoType::Value, 100);

    // insert
    let t = Instant::now();
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "city", i % 10);
        db.tie(e, "dept", (i * 3) % 8);
        db.tie(e, "age", i % 100);
    }
    println!("insert {}件: {:?}", n, t.elapsed());

    // rebuild
    let t = Instant::now();
    db.rebuild();
    println!("rebuild: {:?}", t.elapsed());

    #[cfg(feature = "v26")]
    {
        let t = Instant::now();
        db.rebuild_pairs();
        println!("rebuild_pairs: {:?}", t.elapsed());
    }

    // get
    let t = Instant::now();
    for eid in 0..n as u64 {
        std::hint::black_box(db.get(eid, "city"));
    }
    println!("get {}件: {:?}", n, t.elapsed());

    // tie (overwrite)
    let t = Instant::now();
    for eid in 0..n as u64 {
        db.tie(eid, "city", ((eid + 1) % 10) as u32);
    }
    println!("tie overwrite {}件: {:?}", n, t.elapsed());

    // untie
    let t = Instant::now();
    for eid in 0..1000u64 {
        db.untie(eid, "age");
    }
    println!("untie 1000件: {:?}", t.elapsed());

    // delete
    let t = Instant::now();
    for eid in 0..1000u64 {
        db.delete(eid);
    }
    println!("delete 1000件: {:?}", t.elapsed());

    // commit
    let t = Instant::now();
    db.commit();
    println!("commit: {:?}", t.elapsed());

    // rollback test
    for i in 0..1000 {
        let e = db.entity();
        db.tie(e, "city", i % 10);
    }
    let t = Instant::now();
    db.rollback();
    println!("rollback: {:?}", t.elapsed());

    // flush
    let t = Instant::now();
    db.flush().unwrap();
    println!("flush: {:?}", t.elapsed());

    // open
    let t = Instant::now();
    let _db2 = enchudb::Engine::open_standalone(dir).unwrap();
    println!("open: {:?}", t.elapsed());

    // tie_text
    let _ = std::fs::remove_file("/tmp/enchudb_v26_text.db");
    let mut db3 = enchudb::Engine::create_standalone("/tmp/enchudb_v26_text.db").unwrap();
    let t = Instant::now();
    for i in 0..10_000 {
        let e = db3.entity();
        db3.tie_text(e, "name", &format!("user_{}", i));
    }
    println!("tie_text 10000件: {:?}", t.elapsed());

    // content
    let t = Instant::now();
    for eid in 0..10_000u64 {
        db3.content(eid, "memo", b"hello world");
    }
    println!("content 10000件: {:?}", t.elapsed());

    // get_content
    let t = Instant::now();
    for eid in 0..10_000u64 {
        std::hint::black_box(db3.get_content(eid, "memo"));
    }
    println!("get_content 10000件: {:?}", t.elapsed());
}
