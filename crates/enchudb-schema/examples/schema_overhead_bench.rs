//! schema 層が zero-cost か実測。
//!
//! 4 経路を比較:
//!  1. raw (name 経由):    eng.tie / eng.get / eng.query  ← name → himo_id linear search 毎回
//!  2. raw (id 経由):      eng.tie_to_by_id / eng.get_by_id / eng.query_by_id  ← bindings
//!  3. schema DSL:         emp.insert().set() / emp.entity().get() / emp.where_eq().find()
//!  4. schema bindings:    起動時に emp.himo_id() で id を抜き、 以後は 2 と同じ engine 直叩き
//!
//! 仮説:
//!  - 1 と 3 は同等 (両方 name 経由)
//!  - 2 と 4 は完全に同等 (schema は build 時 transpiler、 runtime overhead 0)
//!  - 2/4 < 1/3 by 数 ns/op (linear search 分)

use enchudb_engine::{Engine, HimoType};
use enchudb_schema::{Database, Value};
use std::time::Instant;

const N: u32 = 1_000_000;
const DEPTS: u32 = 100;
const TARGET_DEPT: u32 = 42;

fn xorshift(mut x: u64) -> u64 {
    x ^= x << 13; x ^= x >> 7; x ^= x << 17; x
}

fn main() {
    let raw_path = "/tmp/sch_bench_raw.db";
    let sch_path = "/tmp/sch_bench_sch.db";
    for p in [raw_path, sch_path] {
        let _ = std::fs::remove_file(p);
        let _ = std::fs::remove_file(format!("{}.oplog", p));
    }

    // ──── raw 構築 ────
    let mut eng = Engine::create_standalone(raw_path).unwrap();
    eng.define_himo("user_id", HimoType::Number, N);
    eng.define_himo("dept_id", HimoType::Number, DEPTS);
    eng.define_himo("salary",  HimoType::Number, 800_000);
    let user_hid_r = eng.himo_id("user_id").unwrap() as u16;
    let dept_hid_r = eng.himo_id("dept_id").unwrap() as u16;
    let _sal_hid_r = eng.himo_id("salary").unwrap() as u16;

    let mut eids_raw: Vec<enchudb_oplog::EntityId> = Vec::with_capacity(N as usize);
    let mut rng = 0x9E3779B97F4A7C15u64;
    for i in 0..N {
        rng = xorshift(rng);
        let e = eng.entity();
        eng.tie(e, "user_id", i);
        eng.tie(e, "dept_id", i % DEPTS);
        eng.tie(e, "salary", 200_000u32 + (rng as u32 % 800_000));
        eids_raw.push(e);
    }
    eng.rebuild();

    // ──── schema 構築 ────
    // schema は marker / schema_blob 用に余分 entity を消費するので +1024 余白
    let mut db = Database::create_growable_with_capacity(sch_path, N + 1024).unwrap();
    db.table("emp")
        .number("user_id")
        .number("dept_id")
        .number("salary")
        .build().unwrap();
    let emp_owned = {
        let emp = db.get_table("emp").unwrap();
        let mut eids: Vec<enchudb_oplog::EntityId> = Vec::with_capacity(N as usize);
        let mut rng = 0x9E3779B97F4A7C15u64;
        for i in 0..N {
            rng = xorshift(rng);
            let e = emp.insert()
                .set("user_id", i)
                .set("dept_id", i % DEPTS)
                .set("salary", 200_000u32 + (rng as u32 % 800_000))
                .commit().unwrap();
            eids.push(e);
        }
        eids
    };
    db.engine().rebuild();

    let emp = db.get_table("emp").unwrap();
    // bindings pre-resolve: column 名 → himo_id だけで足りる。
    // table 識別は column 名 prefix で完結してるので marker / table_vid は存在しない。
    let user_hid_s = emp.himo_id("user_id").unwrap();
    let dept_hid_s = emp.himo_id("dept_id").unwrap();
    let eng_sch = db.engine();

    let target_raw = eids_raw[123_456];
    let target_sch = emp_owned[123_456];

    let iters_pt: u32 = 1_000_000;
    let iters_med: u32 = 10_000;

    // ──── Q1: point lookup ────
    // 1. raw name
    let t = Instant::now();
    let mut s = 0u64;
    for _ in 0..iters_pt {
        if let Some(v) = eng.get(target_raw, "user_id") { s = s.wrapping_add(v as u64); }
    }
    let q1_raw_name = t.elapsed().as_nanos() as f64 / iters_pt as f64;

    // 2. raw bindings (get_by_id)
    let t = Instant::now();
    let mut s2 = 0u64;
    for _ in 0..iters_pt {
        if let Some(v) = eng.get_by_id(target_raw, user_hid_r) { s2 = s2.wrapping_add(v as u64); }
    }
    let q1_raw_id = t.elapsed().as_nanos() as f64 / iters_pt as f64;

    // 3. schema DSL: emp.entity().get()
    let t = Instant::now();
    let mut s3 = 0u64;
    for _ in 0..iters_pt {
        if let Some(Value::Number(n)) = emp.entity(target_sch).get("user_id") {
            s3 = s3.wrapping_add(n as u64);
        }
    }
    let q1_sch_dsl = t.elapsed().as_nanos() as f64 / iters_pt as f64;

    // 4. schema bindings: eng_sch.get_by_id(eid, hid)
    let t = Instant::now();
    let mut s4 = 0u64;
    for _ in 0..iters_pt {
        if let Some(v) = eng_sch.get_by_id(target_sch, user_hid_s) {
            s4 = s4.wrapping_add(v as u64);
        }
    }
    let q1_sch_id = t.elapsed().as_nanos() as f64 / iters_pt as f64;

    // ──── Q2: filter list (dept_id = 42) ────
    // 1. raw name
    let t = Instant::now();
    let mut l = 0u64;
    for _ in 0..iters_med {
        let v = eng.pull_raw("dept_id", TARGET_DEPT);
        l = l.wrapping_add(v.len() as u64);
    }
    let q2_raw_name = t.elapsed().as_micros() as f64 / iters_med as f64;

    // 2. raw bindings (query_by_id)
    let t = Instant::now();
    let mut l2 = 0u64;
    for _ in 0..iters_med {
        let v = eng.query_by_id(&[(dept_hid_r, TARGET_DEPT)]);
        l2 = l2.wrapping_add(v.len() as u64);
    }
    let q2_raw_id = t.elapsed().as_micros() as f64 / iters_med as f64;

    // 3. schema DSL
    let t = Instant::now();
    let mut l3 = 0u64;
    for _ in 0..iters_med {
        let v = emp.where_eq("dept_id", TARGET_DEPT).find().unwrap();
        l3 = l3.wrapping_add(v.len() as u64);
    }
    let q2_sch_dsl = t.elapsed().as_micros() as f64 / iters_med as f64;

    // 4. schema bindings (eng_sch.query_by_id 1 cond) ← marker は存在しない
    let t = Instant::now();
    let mut l4 = 0u64;
    for _ in 0..iters_med {
        let v = eng_sch.query_by_id(&[
            (dept_hid_s as u16, TARGET_DEPT),
        ]);
        l4 = l4.wrapping_add(v.len() as u64);
    }
    let q2_sch_id = t.elapsed().as_micros() as f64 / iters_med as f64;

    // ──── 結果 ────
    println!();
    println!("=========================================================================");
    println!("  schema 層の overhead 実測 — 1M rows / 100 dept");
    println!("=========================================================================");
    println!();
    println!("                                |  raw(name) |  raw(id) | sch(DSL) | sch(id)");
    println!("--------------------------------+------------+----------+----------+--------");
    println!("Q1 point lookup    (ns/op)      | {:>10.1} | {:>8.1} | {:>8.1} | {:>6.1}",
        q1_raw_name, q1_raw_id, q1_sch_dsl, q1_sch_id);
    println!("Q2 filter list 10K (us/op)      | {:>10.2} | {:>8.2} | {:>8.2} | {:>6.2}",
        q2_raw_name, q2_raw_id, q2_sch_dsl, q2_sch_id);
    println!();
    println!("仮説検証:");
    println!("  - sch(id) ≈ raw(id) なら schema = zero-cost (= 紐の束 declaration として正しい)");
    println!();
    println!("(_acc 抑止: s={} s2={} s3={} s4={} l={} l2={} l3={} l4={})",
        s, s2, s3, s4, l, l2, l3, l4);

    let _ = std::fs::remove_file(raw_path);
    let _ = std::fs::remove_file(format!("{}.oplog", raw_path));
    let _ = std::fs::remove_file(sch_path);
    let _ = std::fs::remove_file(format!("{}.oplog", sch_path));
}
