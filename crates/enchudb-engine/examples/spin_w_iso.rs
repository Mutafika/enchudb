//! 案 E の書き手の上乗せの切り分け: 同じ場面で C / E (係を止めた) / E (係が動く、 周期 200 µs、 遅れ 4096 で動き出す) /
//! E (係の周期 5 ms、 遅れ 65536 で動き出す) を組ごとに順を回して交互に。 宣言の無い同じ形の列との差の中央値。
//! 組ごとの process の CPU 時間 (/proc/self/stat、 4 vCPU) も。 欄の offset (false sharing を見る) も出す。
//!
//! `spin_w_iso <dir> [rounds]`。 行 100 万、 値 1000 種、 書き手 1 本、 乱れた行 / 熱い行 (100 行)。
use enchudb_engine::{Engine, ValueType};
use std::time::{Duration, Instant};

const N: usize = 20_000;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn med(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn cpu_ticks() -> u64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap();
    let rest = &s[s.rfind(')').unwrap() + 2..];
    let f: Vec<&str> = rest.split_whitespace().collect();
    f[11].parse::<u64>().unwrap() + f[12].parse::<u64>().unwrap()
}

/// /proc/meminfo の Dirty + Writeback (kB)。
fn dirty_kb() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .unwrap()
        .lines()
        .filter(|l| l.starts_with("Dirty:") || l.starts_with("Writeback:"))
        .map(|l| l.split_whitespace().nth(1).unwrap().parse::<u64>().unwrap())
        .sum()
}

/// 裏 (WAL の係の書き出し・msync) が静まるまで待つ: 200 ms の CPU が 0.15 を切り、 かつ書き出し待ちの page (Dirty + Writeback) が
/// 32 MB を切るまで (60 秒まで)。 msync は I/O 待ちで CPU を使わないので page も見る。
fn quiesce() {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        let c0 = cpu_ticks();
        std::thread::sleep(Duration::from_millis(200));
        if ((cpu_ticks() - c0) as f64 / 100.0) / 0.2 < 0.15 && dirty_kb() < 32 * 1024 {
            return;
        }
    }
    eprintln!("quiesce: gave up (dirty {} kB)", dirty_kb());
}

fn rewrite(eng: &Engine, rows: &[u64], hid: u16, rng: &mut Rng) -> f64 {
    let t = Instant::now();
    for _ in 0..N {
        let e = rows[(rng.next() % rows.len() as u64) as usize];
        eng.tie_to_by_id(e, hid, (rng.next() % 1000) as u32);
    }
    t.elapsed().as_nanos() as f64 / N as f64
}

fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "/tmp/spin_w_iso".into());
    let rounds: usize = std::env::args().nth(2).map_or(30, |s| s.parse().unwrap());
    for (k, off) in Engine::history_layout() {
        println!("layout {k}: offset {off} (line {})", off / 64);
    }
    let _ = std::fs::remove_dir_all(&dir);
    let mut eng = Engine::create_with_capacity(&dir, 1_001_024).unwrap();
    eng.define_table("t", 1_000_016).unwrap();
    for c in ["u", "h"] {
        eng.define_himo_in("t", c, ValueType::Number, 1000).unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 256 * 1024 * 1024).unwrap();
    let rows: Vec<u64> = (0..1_000_000).map(|_| eng.entity_in("t").unwrap()).collect();
    let (uh, hh) = (eng.himo_id("t.u").unwrap() as u16, eng.himo_id("t.h").unwrap() as u16);
    for hid in [uh, hh] {
        for (i, &e) in rows.iter().enumerate() {
            eng.tie_to_by_id(e, hid, (i % 1000) as u32);
        }
    }
    eng.oplog_sync().unwrap();
    eng.declare_history("t.h").unwrap();
    let hot: Vec<u64> = rows[..100].to_vec();
    let names = ["C", "E-stopped", "E-200us", "E-5ms"];
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    for (who, set) in [("random", &rows[..]), ("hot", &hot[..])] {
        let mut d: Vec<Vec<f64>> = vec![Vec::new(); 4];
        let mut cpu: Vec<Vec<f64>> = vec![Vec::new(); 4];
        for r in 0..rounds {
            let only: Option<Vec<usize>> = std::env::var("ISO_ONLY").ok().map(|s| s.split(',').map(|x| x.parse().unwrap()).collect());
            for k in 0..4 {
                let m = (k + r) % 4;
                if only.as_ref().is_some_and(|o| !o.contains(&m)) {
                    continue;
                }
                match m {
                    0 => {
                        eng.history_set_lazy("t.h", true).unwrap();
                    }
                    _ => {
                        eng.history_set_e("t.h", true, None).unwrap();
                        match m {
                            1 => eng.history_tune_linker("t.h", false, None, None).unwrap(),
                            2 => eng.history_tune_linker("t.h", true, Some(200), Some(4096)).unwrap(),
                            _ => eng.history_tune_linker("t.h", true, Some(5000), Some(65536)).unwrap(),
                        }
                    }
                }
                quiesce();
                // 静めた直後の最初の書き込みは遅い (温める)。 宣言の無い列は前と後に取って平均
                rewrite(&eng, set, uh, &mut rng);
                let base1 = rewrite(&eng, set, uh, &mut rng);
                eprintln!("measure {}", names[m]);
                let c0 = cpu_ticks();
                let t0 = Instant::now();
                let x = rewrite(&eng, set, hh, &mut rng);
                let wall = t0.elapsed().as_secs_f64();
                let used = (cpu_ticks() - c0) as f64 / 100.0; // 100 Hz
                let base = (base1 + rewrite(&eng, set, uh, &mut rng)) / 2.0;
                d[m].push(x - base);
                cpu[m].push(used / wall);
                // 元に戻す (繋ぐ)
                if m == 0 {
                    eng.history_set_lazy("t.h", false).unwrap();
                } else {
                    eng.history_set_e("t.h", false, None).unwrap();
                }
                // 切り替えで全体を繋ぐと履歴の file 全体が汚れ、 次の書き出しが大きな msync になる: 次の組の前に静める
                quiesce();
            }
        }
        let mut line: Vec<String> = Vec::new();
        for m in 0..4 {
            if !d[m].is_empty() {
                line.push(format!("{}={:.0} ns (cpu {:.2})", names[m], med(&mut d[m]), med(&mut cpu[m])));
            }
        }
        println!("{who}: {}", line.join(" | "));
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
