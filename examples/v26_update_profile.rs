/// 差分更新のボトルネック特定

#[cfg(not(feature = "v26"))]
fn main() { println!("--features v26"); }

#[cfg(feature = "v26")]
fn main() {
    use std::time::Instant;

    let dir = "/tmp/v26_update_prof.db";
    let _ = std::fs::remove_file(dir);
    let mut db = enchudb::Engine::create_standalone(dir).unwrap();
    db.define_himo("a", enchudb::HimoType::Number, 10);
    db.define_himo("b", enchudb::HimoType::Number, 8);
    db.define_himo("c", enchudb::HimoType::Number, 5);
    db.define_himo("d", enchudb::HimoType::Number, 4);

    let n = 100_000u32;
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "a", i % 10);
        db.tie(e, "b", (i * 3) % 8);
        db.tie(e, "c", (i * 7) % 5);
        db.tie(e, "d", (i * 11) % 4);
    }
    db.rebuild();
    db.rebuild_pairs();

    // tie だけの速度
    let t = Instant::now();
    for i in 0..1000u32 {
        let old = db.get(i, "a").unwrap();
        db.tie(i, "a", (old + 1) % 10);
    }
    let tie_time = t.elapsed();
    println!("tie 1000件: {:?} ({:?}/件)", tie_time, tie_time / 1000);

    // tie を戻す
    for i in 0..1000u32 {
        let old = db.get(i, "a").unwrap();
        db.tie(i, "a", (old + 10 - 1) % 10);
    }

    // apply_pair_delta だけの速度
    let a_idx = db.himo_id("a").unwrap();
    let t = Instant::now();
    for i in 0..1000u32 {
        let old = db.get(i, "a").unwrap();
        let new_v = (old + 1) % 10;
        db.apply_pair_delta(i, a_idx, old, new_v);
        // Column も更新しないと次のイテレーションで get が古い値返す
        db.tie(i, "a", new_v);
    }
    let update_time = t.elapsed();
    println!("apply_pair_delta + tie 1000件: {:?} ({:?}/件)", update_time, update_time / 1000);

    // rebuild_pairs 全体の速度（比較用）
    let t = Instant::now();
    db.rebuild_pairs();
    let rebuild_time = t.elapsed();
    println!("rebuild_pairs フル: {:?}", rebuild_time);

    // 差分更新 vs フル rebuild のクロスオーバー
    // N件の差分更新が rebuild_pairs より遅くなる点
    println!("\n差分更新コスト: {:.0}μs/件", update_time.as_micros() as f64 / 1000.0);
    println!("フル rebuild: {:.0}μs", rebuild_time.as_micros() as f64);
    let crossover = rebuild_time.as_micros() as f64 / (update_time.as_micros() as f64 / 1000.0);
    println!("クロスオーバー: {:.0}件（これ以上は rebuild の方が速い）", crossover);
}
