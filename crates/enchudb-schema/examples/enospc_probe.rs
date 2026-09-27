//! ディスクが一杯の時に、 commit が Ok なのに値が読めない (黙って消える) かを見る。
//! `cargo run --release -p enchudb-schema --example enospc_probe -- <小さいボリューム上の path> [row の数]`

use enchudb_schema::{Database, Value};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args[1].clone();
    let n: i64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2_000_000);
    // verify: 別プロセスで開き直して、 書いた時と同じ規則の中身が残っているかだけ見る (unmount 後に使う)
    if args.get(3).map(String::as_str) == Some("verify") {
        let db = Database::open(&path).unwrap();
        let users = db.get_table("users").unwrap();
        let (mut ok, mut differ, mut missing) = (0usize, 0usize, 0usize);
        for i in 0..n {
            let name = format!("name-{i:08}-{}", "x".repeat(40));
            let memo = format!("memo-{i:08}-{}", "y".repeat(200));
            match users.where_eq("id", i).find_one() {
                Ok(Some(e)) => {
                    let got = (users.entity(e).get("name"), users.entity(e).get("memo"), users.entity(e).get("age"));
                    if got == (Some(Value::Text(name)), Some(Value::Text(memo)), Some(Value::Number(i % 1000))) {
                        ok += 1;
                    } else {
                        if differ == 0 {
                            println!("verify row {i}: {:?}", (&got.0, got.1.as_ref().map(|_| "memo"), &got.2));
                        }
                        differ += 1;
                    }
                }
                _ => missing += 1,
            }
        }
        println!("verify: 一致 {ok}、 中身が違う {differ}、 row が無い {missing} / {n}");
        return;
    }
    let _ = std::fs::remove_dir_all(&path);
    let mut db = match Database::create(&path) {
        Ok(db) => db,
        Err(e) => {
            println!("create: Err {e}");
            return;
        }
    };
    let users = match db.table("users").number("id").tag("name").leaf("memo").number("age").primary_key("id").build() {
        Ok(t) => t,
        Err(e) => {
            println!("build: Err {e}");
            return;
        }
    };
    let mut ok_rows = Vec::new();
    let mut first_err = None;
    let mut silent = 0usize;
    for i in 0..n {
        let name = format!("name-{i:08}-{}", "x".repeat(40));
        let memo = format!("memo-{i:08}-{}", "y".repeat(200));
        let r = users.insert().set("id", i).set("name", name.as_str()).set("memo", memo.as_str()).set("age", i % 1000).commit();
        match r {
            Ok(e) => {
                let got = (users.entity(e).get("name"), users.entity(e).get("memo"), users.entity(e).get("age"));
                let want = (Some(Value::Text(name.clone())), Some(Value::Text(memo.clone())), Some(Value::Number(i % 1000)));
                if got != want {
                    if silent == 0 {
                        println!("row {i}: commit Ok だが読み返しが違う: {:?}", (&got.0, got.1.as_ref().map(|_| "memo"), &got.2));
                    }
                    silent += 1;
                }
                ok_rows.push((i, e, name, memo));
            }
            Err(err) => {
                first_err = Some((i, err.to_string()));
                break;
            }
        }
        if i % 20_000 == 0 {
            eprintln!("row {i}");
        }
    }
    println!("入れた row (Ok): {}、 Ok なのに読み返しが違った row: {silent}", ok_rows.len());
    match &first_err {
        Some((i, e)) => println!("最初の Err: row {i}: {e}"),
        None => println!("Err は一度も出なかった"),
    }
    // Ok で入れた row を全部読み返す
    let mut lost = 0usize;
    for (i, e, name, memo) in &ok_rows {
        let got = (users.entity(*e).get("name"), users.entity(*e).get("memo"), users.entity(*e).get("age"));
        if got != (Some(Value::Text(name.clone())), Some(Value::Text(memo.clone())), Some(Value::Number(i % 1000))) {
            lost += 1;
        }
    }
    println!("最後に読み返して違った row: {lost} / {}", ok_rows.len());
    drop(users);
    match db.engine_mut().expect("単独所有").flush() {
        Ok(()) => println!("flush: Ok"),
        Err(e) => println!("flush: Err {e}"),
    }
    drop(db);
    // 開き直して、 Ok で入れた row の中身が残っているか
    let db = match Database::open(&path) {
        Ok(db) => db,
        Err(e) => {
            println!("reopen: Err {e}");
            return;
        }
    };
    let users = db.get_table("users").unwrap();
    let (mut ok, mut missing, mut differ) = (0usize, 0usize, 0usize);
    for (i, _, name, memo) in &ok_rows {
        match users.where_eq("id", *i).find_one() {
            Ok(Some(e)) => {
                let got = (users.entity(e).get("name"), users.entity(e).get("memo"), users.entity(e).get("age"));
                if got == (Some(Value::Text(name.clone())), Some(Value::Text(memo.clone())), Some(Value::Number(i % 1000))) {
                    ok += 1;
                } else {
                    if differ == 0 {
                        println!("reopen row {i}: {:?}", (&got.0, got.1.as_ref().map(|_| "memo"), &got.2));
                    }
                    differ += 1;
                }
            }
            _ => missing += 1,
        }
    }
    println!("reopen 後: 一致 {ok}、 中身が違う {differ}、 row が無い {missing} / {}", ok_rows.len());
}
