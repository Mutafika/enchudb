//! 時点 t の bit の表の戻しを段ごとに測る (ballb r と同じ形: 96 万 entity、 値 100、 10 回 × 9.6 万の書き換え、 t5 = 5 回目の後)。
use std::hint::black_box;
use std::time::Instant;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
}

struct Ledger { eids: Vec<u32>, into: Vec<bool> }

fn main() {
    let total = 960_000usize;
    let mut r = Rng(41);
    let mut cur: Vec<u32> = (0..total).map(|_| (r.next() % 100) as u32).collect();
    let mut logs: Vec<Ledger> = (0..100).map(|_| Ledger { eids: vec![], into: vec![] }).collect();
    let mut cut = vec![0usize; 100];
    for round in 1..=10 {
        for _ in 0..total / 10 {
            let i = (r.next() % total as u64) as usize;
            let a = (r.next() % 100) as u32;
            let o = cur[i];
            if o != a {
                logs[o as usize].eids.push(i as u32); logs[o as usize].into.push(false);
                logs[a as usize].eids.push(i as u32); logs[a as usize].into.push(true);
                cur[i] = a;
            }
        }
        if round == 5 { for v in 0..100 { cut[v] = logs[v].eids.len(); } }
    }
    let nows: Vec<Vec<u32>> = (0..100).map(|v| (0..total as u32).filter(|&e| cur[e as usize] == v).collect()).collect();
    let words = (total >> 6) + 1;
    let mode = std::env::args().nth(1).unwrap_or("all".into());
    let mut bits = vec![0u64; words];
    // 段: clear / now を立てる / 差分を当てる (枝あり / 枝なし) / 走査 (while / 4 個ずつ)
    let mut t = [0f64; 7];
    let n = 2000;
    let mut acc = 0usize;
    for k in 0..n {
        let v = if mode == "hot" { 7 } else { k % 100 };
        let now = &nows[v];
        let lg = &logs[v];
        let (e, into) = (&lg.eids[cut[v]..], &lg.into[cut[v]..]);
        let t0 = Instant::now();
        bits.iter_mut().for_each(|w| *w = 0);
        let t1 = Instant::now();
        for &x in now { bits[(x >> 6) as usize] |= 1 << (x & 63); }
        let t2 = Instant::now();
        if k % 2 == 0 {
            for j in (0..e.len()).rev() {
                let i = e[j] as usize; let m = 1u64 << (i & 63);
                if into[j] { bits[i >> 6] &= !m } else { bits[i >> 6] |= m }
            }
        } else {
            for j in (0..e.len()).rev() {
                let i = e[j] as usize; let m = 1u64 << (i & 63);
                let set = (!into[j] as u64).wrapping_neg() & m;
                let w = &mut bits[i >> 6]; *w = (*w & !m) | set;
            }
        }
        let t3 = Instant::now();
        let mut out: Vec<u32> = Vec::with_capacity(now.len() + e.len() / 2);
        if k % 4 < 2 {
            for (w, &x) in bits.iter().enumerate() {
                let mut x = x;
                while x != 0 { out.push(((w as u32) << 6) + x.trailing_zeros()); x &= x - 1; }
            }
        } else {
            // 4 個ずつ枝なしで書いて、 立っていた数だけ進める
            let cap = bits.len() * 0 + now.len() + e.len() + 64;
            out.reserve(cap);
            let p = out.as_mut_ptr(); let mut len = 0usize;
            for (w, &x) in bits.iter().enumerate() {
                let base = (w as u32) << 6; let c = x.count_ones() as usize; let mut y = x;
                unsafe {
                    *p.add(len) = base + y.trailing_zeros(); y &= y.wrapping_sub(1);
                    *p.add(len + 1) = base + y.trailing_zeros(); y &= y.wrapping_sub(1);
                    *p.add(len + 2) = base + y.trailing_zeros(); y &= y.wrapping_sub(1);
                    *p.add(len + 3) = base + y.trailing_zeros(); y &= y.wrapping_sub(1);
                    if c > 4 { let mut q = len + 4; while y != 0 { *p.add(q) = base + y.trailing_zeros(); y &= y - 1; q += 1; } }
                    len += c;
                }
            }
            unsafe { out.set_len(len) };
        }
        let t4 = Instant::now();
        acc += out.len();
        let ts = [t1 - t0, t2 - t1, t3 - t2, t4 - t3];
        t[0] += ts[0].as_nanos() as f64;
        t[1] += ts[1].as_nanos() as f64;
        t[if k % 2 == 0 { 2 } else { 3 }] += ts[2].as_nanos() as f64;
        t[if k % 4 < 2 { 4 } else { 5 }] += ts[3].as_nanos() as f64;
        t[6] += (t4 - t0).as_nanos() as f64;
    }
    black_box(acc);
    let avg_diffs: f64 = (0..100).map(|v| (logs[v].eids.len() - cut[v]) as f64).sum::<f64>() / 100.0;
    println!("mode {mode}: diffs after t5 avg {avg_diffs:.0}, now avg {:.0}", nows.iter().map(|x| x.len()).sum::<usize>() as f64 / 100.0);
    println!("clear {:.1} µs / set now {:.1} / apply branchy {:.1} / apply branchless {:.1} / scan while {:.1} / scan 4-wide {:.1} / total avg {:.1}",
        t[0] / n as f64 / 1e3, t[1] / n as f64 / 1e3, t[2] / (n / 2) as f64 / 1e3, t[3] / (n / 2) as f64 / 1e3, t[4] / (n / 2) as f64 / 1e3, t[5] / (n / 2) as f64 / 1e3, t[6] / n as f64 / 1e3);
}
