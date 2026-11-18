//! #992: きれいに閉じなかった v15 の DB を開く時の、 gen.seg の突き合わせ (`reconcile_generations`) の時間。 3. (死んだ場所に残った
//! cell を見る) は、 死んだ場所ごとに表の列の cell を読む (場所の数 × 列の数に比例)。 open の全体の時間も出す。
//!
//! 像: 表 t (枠 = 行の数) を Number の列で埋め、 `dead_every` 行に 1 行消して、 1 行足して場所を使い回す (v15)。 書き出して
//! (`oplog_sync`) から閉じずに process を終える (= kill の像、 きれいに閉じていない)。 開く側は像を APFS の clone (`cp -c`) で写して
//! から開く (同じ像を何回でも開ける。 clone は page cache が別 = cold)。 `ENCHU_TRACE_OPEN=1` で突き合わせの内訳を出す。
//!
//! ```text
//! cargo run --release -p enchudb-engine --example issue992_reconcile_cost -- <置き場> <行> <列> <消す間隔> [開く回数]
//! ```

use enchudb_engine::{Engine, ValueType};
use std::process::Command;
use std::time::Instant;

const OPLOG_CAP: usize = 64 * 1024 * 1024;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("make") {
        return make(&args[2], args[3].parse().unwrap(), args[4].parse().unwrap(), args[5].parse().unwrap());
    }
    if args.get(1).map(String::as_str) == Some("open") {
        return open(&args[2]);
    }
    let (dir, rows, cols, every) = (&args[1], &args[2], &args[3], &args[4]);
    let reps: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(5);
    let exe = std::env::current_exe().unwrap();
    std::fs::create_dir_all(dir).unwrap();
    let db = format!("{dir}/db");
    let st = Command::new(&exe).args(["make", &db, rows, cols, every]).status().unwrap();
    assert!(st.success(), "make が失敗");
    for i in 0..reps {
        let img = format!("{dir}/img{i}");
        let _ = std::fs::remove_dir_all(&img);
        assert!(Command::new("cp").args(["-c", "-R", &db, &img]).status().unwrap().success());
        let out = Command::new(&exe).args(["open", &img]).env("ENCHU_TRACE_OPEN", "1").output().unwrap();
        print!("{}", String::from_utf8_lossy(&out.stdout));
        for line in String::from_utf8_lossy(&out.stderr).lines().filter(|l| l.contains("reconcile")) {
            println!("  {line}");
        }
        let _ = std::fs::remove_dir_all(&img);
    }
    let _ = std::fs::remove_dir_all(&db);
}

fn make(db: &str, rows: u32, cols: u32, every: u32) {
    let _ = std::fs::remove_dir_all(db);
    let mut eng = Engine::create_with_capacity(db, rows.saturating_mul(2).max(1 << 21)).unwrap();
    eng.define_table("t", rows).unwrap();
    for c in 0..cols {
        eng.define_himo_in("t", &format!("c{c}"), ValueType::Number, 0).unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    let names: Vec<String> = (0..cols).map(|c| format!("t.c{c}")).collect();
    let mut rows_made = Vec::with_capacity(rows as usize);
    for i in 0..rows {
        let e = eng.entity_in("t").unwrap();
        for (c, n) in names.iter().enumerate() {
            eng.tie_async(e, n, i.wrapping_mul(31).wrapping_add(c as u32) % 1000);
        }
        rows_made.push(e);
        if i % 1000 == 999 {
            eng.oplog_commit();
            eng.oplog_sync().unwrap();
        }
    }
    let mut dead = 0u32;
    for (i, &e) in rows_made.iter().enumerate() {
        if every > 0 && i as u32 % every == every - 1 {
            eng.delete(e);
            dead += 1;
            if dead % 1000 == 0 {
                eng.oplog_commit();
                eng.oplog_sync().unwrap();
            }
        }
    }
    // 1 行足す = 満杯の表が消えた場所を使い回す (v15 に移る)
    let reused = eng.entity_in("t").unwrap();
    assert!(eng.slot_bits() > 0, "前提: v15");
    eng.tie_async(reused, &names[0], 1u32);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    println!("made: rows {rows} cols {cols} dead {} (v15)", dead - 1);
    // 閉じずに終える (きれいに閉じていない像)
    std::process::exit(0);
}

fn open(img: &str) {
    let t0 = Instant::now();
    let eng = Engine::open_concurrent_with_oplog(img, OPLOG_CAP).unwrap();
    let opened = t0.elapsed();
    println!(
        "open {:.2} ms (repairs {:?}, resumed {})",
        opened.as_secs_f64() * 1e3,
        eng.generation_repairs(),
        eng.resumed_cleanups()
    );
    // 閉じる時間は測らない
    std::process::exit(0);
}
