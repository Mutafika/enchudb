#[cfg(test)]
mod tests {
    use std::hint::black_box;
    use std::sync::atomic::AtomicU64;
    use std::sync::OnceLock;
    use std::time::Instant;

    // ball.rs の Chunked と同じ形: 65536 個の OnceLock の並び、 塊は 65536 個の atomic。
    #[test]
    fn dir_get_vs_index() {
        let t0 = Instant::now();
        let dir: Box<[OnceLock<Box<[AtomicU64]>>]> = (0..65536).map(|_| OnceLock::new()).collect();
        eprintln!("PROBE make dir {:?}", t0.elapsed());
        let t0 = Instant::now();
        dir[0].get_or_init(|| (0..65536).map(|_| AtomicU64::new(0)).collect());
        eprintln!("PROBE make chunk {:?}", t0.elapsed());
        for round in 0..2 {
            let t0 = Instant::now();
            for _ in 0..10 {
                black_box(dir.get(0).and_then(|c| c.get()).map(|c| &c[5]));
            }
            eprintln!("PROBE {round} slice::get x10 {:?}", t0.elapsed());
            let t0 = Instant::now();
            for _ in 0..10 {
                black_box(dir[0].get().map(|c| &c[5]));
            }
            eprintln!("PROBE {round} index x10 {:?}", t0.elapsed());
            let t0 = Instant::now();
            for _ in 0..10 {
                black_box(dir.len());
            }
            eprintln!("PROBE {round} len x10 {:?}", t0.elapsed());
            let t0 = Instant::now();
            for _ in 0..10 {
                black_box((&raw const *dir).len());
            }
            eprintln!("PROBE {round} raw len x10 {:?}", t0.elapsed());
        }
    }
}
