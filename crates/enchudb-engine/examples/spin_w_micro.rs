//! HistCol::note だけの値段 (WAL の揺れを外す)。 spin_w_micro <dir>
use enchudb_engine::history::HistCol;
use std::time::Instant;
fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).unwrap_or("/tmp/spin_w_micro".into()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let n: u32 = 1_000_000;
    let c = HistCol::create(&dir, 1, 1, n).unwrap();
    for s in 0..n { c.note(s, s, Some(s as u64 % 1000), 2 + s as u64, 0).unwrap(); }
    let mut x = 0x9E3779B97F4A7C15u64;
    let mut t0 = 10_000_000u64;
    let every: u64 = std::env::args().nth(2).map(|a| a.parse().unwrap()).unwrap_or(0);
    for round in 0..3 {
        let m = 1_000_000;
        let t = Instant::now();
        for _ in 0..m {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            let s = (x % n as u64) as u32;
            t0 += 1;
            c.note(s, s, Some(x % 1000), t0, 0).unwrap();
            if every > 0 && t0 % every == 0 { c.write_out(false).unwrap(); }
        }
        let ns = t.elapsed().as_nanos() as f64 / m as f64;
        let tw = Instant::now();
        c.write_out(false).unwrap();
        println!("round {round} note_ns={ns:.1} write_out_ms={:.1}", tw.elapsed().as_secs_f64()*1e3);
        if round == 1 { c.pull_at(7, t0).unwrap(); for v in 0..1000 { c.pull_at(v, t0).unwrap(); } println!("decks observed"); }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
