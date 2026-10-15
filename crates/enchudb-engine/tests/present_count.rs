//! `Engine::count_present_by_id` (紐に値のある entity の数、 書き手が足し引きした数) が、 列をなめた答え
//! (`entities_with_himo(..).len()`) と合うこと。 書き手 4 本が並んで、 値を書く・書き換える・外す (`untie`)・消す
//! (`delete`) を混ぜ、 cylinder を組む前 (門の中の書き手)・組んだ後 (値の鍵の書き手、 配列の中の値と sparse の大きな値)・
//! 書いている途中で組む時、 開き直した後 (開いた時の数は最初に引かれた時に数える。 その前に書いた分も数に入る) で見る。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::EntityId;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-present-count-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// assert で落ちても DB を消す。 engine より先に作って、 engine の後に drop させる。
struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        cleanup(&self.0);
    }
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

const COLS: [&str; 3] = ["t.a", "t.b", "t.w"];

fn create(path: &str) {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_table("t", 200_000).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "b", ValueType::Number, 16).unwrap();
    eng.define_himo_in("t", "w", ValueType::Number64, 0).unwrap();
    eng.flush().unwrap();
}

fn hids(eng: &Engine) -> Vec<u16> {
    COLS.iter().map(|c| eng.himo_id(c).unwrap() as u16).collect()
}

/// 値: 小さい値 (dense の配列) と、 大きな値 (sparse、 組んだ後なら配列を伸ばさずに sparse へ) を混ぜる。
fn value(rng: &mut Rng) -> u64 {
    if rng.below(5) == 0 { 1_000_000 + rng.below(1_000) } else { rng.below(40) }
}

/// 書き手 1 本の仕事: 新しい row を作って書く・書き換える・外す・消す・外した列を書き直す。
fn writer(eng: &Engine, hs: &[u16], seed: u64, n: usize) {
    let mut rng = Rng(seed);
    let mut mine: Vec<EntityId> = Vec::new();
    for _ in 0..n {
        match rng.below(10) {
            0..=3 => {
                let e = eng.entity_in("t").unwrap();
                for &h in hs {
                    if rng.below(4) != 0 {
                        eng.tie_to_by_id(e, h, value(&mut rng));
                    }
                }
                mine.push(e);
            }
            4..=6 if !mine.is_empty() => {
                let e = mine[rng.below(mine.len() as u64) as usize];
                eng.tie_to_by_id(e, hs[rng.below(hs.len() as u64) as usize], value(&mut rng));
            }
            7 | 8 if !mine.is_empty() => {
                let e = mine[rng.below(mine.len() as u64) as usize];
                eng.untie_by_id(e, hs[rng.below(hs.len() as u64) as usize]);
            }
            9 if !mine.is_empty() => {
                let e = mine.swap_remove(rng.below(mine.len() as u64) as usize);
                eng.delete(e);
            }
            _ => {}
        }
    }
}

fn run_writers(eng: &Arc<Engine>, hs: &[u16], seed: u64, n: usize) {
    std::thread::scope(|s| {
        for t in 0..4u64 {
            let eng = eng.clone();
            s.spawn(move || writer(&eng, hs, seed.wrapping_mul(31).wrapping_add(t * 0x9E37_79B9), n));
        }
    });
}

fn assert_counts(eng: &Engine, hs: &[u16], stage: &str) {
    let mut total = 0;
    for (&h, name) in hs.iter().zip(COLS) {
        let want = eng.entities_with_himo(h).len();
        assert_eq!(eng.count_present_by_id(h), want, "{stage} {name}");
        total += want;
    }
    // 対照: 値のある row がある (数が 0 どうしで合っているのではない)
    assert!(total > 0, "{stage}: 対照 — 値のある row が無い");
}

#[test]
fn present_count_follows_concurrent_writers() {
    let path = tmp_path("writers");
    cleanup(&path);
    let _cleanup = Cleanup(path.clone());
    create(&path);
    let eng = Engine::open(&path).unwrap();
    let hs = hids(&eng);
    let n = 4_000;
    // 組む前 (門の中の書き手)
    run_writers(&eng, &hs, 1, n);
    let built = eng.himos_with_cylinder_built();
    assert_counts(&eng, &hs, "組む前");
    assert_eq!(eng.himos_with_cylinder_built(), built, "数を引いても cylinder を組まない");
    // 組んだ後 (値の鍵の書き手)
    for &h in &hs {
        let _ = eng.count_value_by_id(h, 1);
    }
    assert_eq!(eng.himos_with_cylinder_built(), built + hs.len(), "値の数で 3 本とも組む");
    run_writers(&eng, &hs, 2, n);
    assert_counts(&eng, &hs, "組んだ後");
    drop(eng);

    // 書いている途中で組む: 書き手 4 本と並んで、 組む読み (値の数) と数を引く
    let eng = Engine::open(&path).unwrap();
    let hs2 = hids(&eng);
    std::thread::scope(|s| {
        let e2 = eng.clone();
        let h2 = hs2.clone();
        s.spawn(move || run_writers(&e2, &h2, 3, n));
        for _ in 0..50 {
            for &h in &hs2 {
                let _ = eng.count_present_by_id(h);
                let _ = eng.count_value_by_id(h, 2);
            }
            std::thread::yield_now();
        }
    });
    assert_counts(&eng, &hs2, "書いている途中で組んだ後");
}

#[test]
fn present_count_after_reopen() {
    let path = tmp_path("reopen");
    cleanup(&path);
    let _cleanup = Cleanup(path.clone());
    create(&path);
    {
        let eng = Engine::open(&path).unwrap();
        let hs = hids(&eng);
        run_writers(&eng, &hs, 4, 3_000);
        assert_counts(&eng, &hs, "1 回目");
    }
    // 開き直して、 数を引く前に書く (開いた時の数を数える前の書き込みも数に入る)
    {
        let eng = Engine::open(&path).unwrap();
        let hs = hids(&eng);
        run_writers(&eng, &hs, 5, 2_000);
        assert_counts(&eng, &hs, "開き直して書いた後");
        run_writers(&eng, &hs, 6, 2_000);
        assert_counts(&eng, &hs, "開き直して数えた後にまた書いた後");
    }
    // 開き直して、 先に数を引いてから書く
    {
        let eng = Engine::open(&path).unwrap();
        let hs = hids(&eng);
        assert_counts(&eng, &hs, "開き直した直後");
        run_writers(&eng, &hs, 7, 2_000);
        assert_counts(&eng, &hs, "開き直して数えてから書いた後");
    }
    // 読み取り専用で開いても数える
    {
        let eng = Engine::open_readonly(&path).unwrap();
        let hs = hids(&eng);
        assert_counts(&eng, &hs, "読み取り専用");
    }
}

/// 読み取り専用の engine は、 同じ file に別の engine (別の process と同じ、 共有の写像) の書き手が書いた分も数える
/// (列をなめる。 前と同じ)。 書き手が開いた時に見えている row の値を外すと、 読む側の数も減る。
#[test]
fn readonly_engine_counts_what_another_writer_wrote() {
    let path = tmp_path("readonly");
    cleanup(&path);
    let _cleanup = Cleanup(path.clone());
    create(&path);
    let mut w = Engine::open_standalone(&path).unwrap();
    let h = w.himo_id("t.a").unwrap() as u16;
    let es: Vec<EntityId> = (0..200u64)
        .map(|i| {
            let e = w.entity_in("t").unwrap();
            w.tie_to_by_id(e, h, i % 7);
            e
        })
        .collect();
    w.flush().unwrap();
    let ro = Engine::open_readonly(&path).unwrap();
    assert_eq!(ro.count_present_by_id(h), 200);
    for &e in es.iter().step_by(4) {
        w.untie_by_id(e, h);
    }
    assert_eq!(w.count_present_by_id(h), 150, "書き手の数");
    assert_eq!(ro.entities_with_himo(h).len(), 150, "対照: 読む側の列は書き手の書き込みを見る");
    assert_eq!(ro.count_present_by_id(h), 150, "読む側の数");
}
