//! rw open bench — **writer open / drop を readonly と並べて測る** (#255)。
//!
//! `Engine::open` (rw) は load 末尾で LeafStore の free-list を再構成するが、 それを
//! `HimoStore::unique_values()` で集めていたため **leaf を持つ全 himo の cylinder を open で
//! eager に build し、 drop でそれを free していた**。 消費側 (`sf`) の 「1 行書くだけで
//! ~350 ms、 うち ~250 ms が open + drop」 という報告がこの bench の動機。 `open_split_bench`
//! (readonly) はこの経路を通らないので見えなかった。
//!
//! ```text
//! cargo run --release --example rw_open_bench             # 合成 DB (117 himo の半分が Leaf / 9211 entity)
//! cargo run --release --example rw_open_bench <db_dir>    # 既存 DB (必ず隔離コピーで)
//! ```
use enchudb::{Engine, ValueType};
use std::time::{Duration, Instant};

const HIMOS: u32 = 117; // `sf` の形
const ENTS: u32 = 9211;
const REPS: usize = 20;
const WARMUP: usize = 3;

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn build(path: &str) {
    let _ = enchudb::db_files::remove_db(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("t", 60_000).unwrap();
    for h in 0..HIMOS {
        let vt = if h % 2 == 0 { ValueType::Leaf } else { ValueType::Number };
        eng.define_himo_in("t", &format!("h{h}"), vt, 0).unwrap();
    }
    let hids: Vec<u16> = (0..HIMOS).map(|h| eng.himo_id(&format!("t.h{h}")).unwrap() as u16).collect();
    for i in 0..ENTS {
        let e = eng.entity_in("t").unwrap();
        for h in 0..HIMOS {
            if h % 2 == 0 {
                eng.tie_bytes_to_by_id(e, hids[h as usize], format!("leaf value {i}/{h}").as_bytes());
            } else {
                eng.tie(e, &format!("t.h{h}"), i + h);
            }
        }
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
}

fn min_of(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::INFINITY, f64::min)
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// (open, drop) を REPS 回。 `built` = open 直後に cylinder が組まれていた himo 数。
/// `open` の戻り型は readonly (`Engine`) と rw (`Arc<Engine>`) で違うので generic。
fn measure<T>(open: impl Fn() -> T, built_of: impl Fn(&T) -> usize) -> (Vec<f64>, Vec<f64>, usize) {
    let (mut o, mut d) = (vec![], vec![]);
    let mut built = 0;
    for i in 0..REPS + WARMUP {
        let t = Instant::now();
        let eng = open();
        let t_open = t.elapsed();
        built = built_of(&eng);
        let t = Instant::now();
        drop(eng);
        let t_drop = t.elapsed();
        if i >= WARMUP {
            o.push(ms(t_open));
            d.push(ms(t_drop));
        }
    }
    (o, d, built)
}

fn main() {
    let arg = std::env::args().nth(1);
    let path = match &arg {
        Some(p) => p.clone(),
        None => {
            let p = "/tmp/enchu_rw_open_bench.db".to_string();
            build(&p);
            p
        }
    };
    {
        let eng = Engine::open_readonly(&path).unwrap();
        println!(
            "db: {path}  entities={} himos={} leaf_footprint={:?}",
            eng.entity_count(), eng.himo_count(), eng.leaf_footprint()
        );
    }
    println!("{:<22} {:>10} {:>10} {:>10} {:>10} {:>8}", "mode", "open min", "open med", "drop min", "drop med", "cyl blt");
    let report = |name: &str, (mut o, mut d, built): (Vec<f64>, Vec<f64>, usize)| {
        println!(
            "{:<22} {:>8.2}ms {:>8.2}ms {:>8.2}ms {:>8.2}ms {:>8}",
            name, min_of(&o), median(&mut o), min_of(&d), median(&mut d), built
        );
    };
    report("open_readonly", measure(|| Engine::open_readonly(&path).unwrap(), |e| e.himos_with_cylinder_built()));
    report("open (rw)", measure(|| Engine::open(&path).unwrap(), |e| e.himos_with_cylinder_built()));
    if arg.is_none() {
        let _ = enchudb::db_files::remove_db(&path);
    }
}
