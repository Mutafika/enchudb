//! `candidates` (gram の AND) を、doc ごとの gram の集合をなめた答えと比べる（別の道）。eid を順不同で登録（list の順が
//! 崩れる）、同じ gram が何度も出る doc、消しと登録し直し、compact の前後、save して mmap で開いた index で比べる。
//! 答えは eid の昇順・重複なし。

use enchudb_ngram::{gram, NgramIndex};
use std::collections::{HashMap, HashSet};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// 少ない文字 (よく出る gram = 長い list) と、たまに出る文字 (短い list) を混ぜた doc。
fn doc_text(rng: &mut Rng) -> String {
    const COMMON: [char; 5] = ['a', 'b', 'c', '国', '民'];
    const RARE: [char; 8] = ['x', 'y', 'z', '法', '下', '平', '等', 'q'];
    let len = 4 + rng.below(40) as usize;
    (0..len)
        .map(|_| if rng.below(8) == 0 { RARE[rng.below(8) as usize] } else { COMMON[rng.below(5) as usize] })
        .collect()
}

fn scanned(docs: &HashMap<u64, String>, query: &str, n: usize) -> Vec<u64> {
    let q: HashSet<u64> = gram::extract_keys(query, n).into_iter().collect();
    if q.is_empty() {
        return Vec::new();
    }
    let mut out: Vec<u64> = docs
        .iter()
        .filter(|(_, t)| {
            let ks: HashSet<u64> = gram::extract_keys(t, n).into_iter().collect();
            q.iter().all(|k| ks.contains(k))
        })
        .map(|(&e, _)| e)
        .collect();
    out.sort_unstable();
    out
}

fn check(idx: &NgramIndex, docs: &HashMap<u64, String>, n: usize, rng: &mut Rng, stage: &str) {
    let texts: Vec<&String> = docs.values().collect();
    let (mut nonempty, mut partial) = (0, 0);
    for t in 0..400 {
        // 半分は doc の一部 (当たりがある)、半分は乱数の文字列
        let query: String = if t % 2 == 0 {
            let d: Vec<char> = texts[rng.below(texts.len() as u64) as usize].chars().collect();
            let a = rng.below(d.len() as u64) as usize;
            let b = (a + n + rng.below(6) as usize).min(d.len());
            d[a..b].iter().collect()
        } else {
            doc_text(rng).chars().take(n + rng.below(5) as usize).collect()
        };
        let want = scanned(docs, &query, n);
        let got = idx.candidates(&query);
        assert!(got.windows(2).all(|w| w[0] < w[1]), "{stage} n={n} {query:?}: 昇順・重複なしでない {got:?}");
        assert_eq!(got, want, "{stage} n={n} {query:?}");
        nonempty += usize::from(!want.is_empty());
        partial += usize::from(!want.is_empty() && want.len() < docs.len());
    }
    // 対照: 当たる問いと、全部でも空でもない問いが十分にある
    assert!(nonempty >= 150 && partial >= 100, "{stage} n={n}: 当たる {nonempty} / 一部だけ当たる {partial}");
}

#[test]
fn and_matches_a_scan_of_the_grams() {
    for n in [2usize, 3] {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ n as u64);
        let mut idx = NgramIndex::with_n(n).unwrap();
        let mut docs: HashMap<u64, String> = HashMap::new();
        // eid を順不同で (大きい eid の後に小さい eid が来る)。peer の頭付きの大きな eid も混ぜる
        let mut eids: Vec<u64> = (0..300u64).map(|i| if i % 7 == 0 { (1u64 << 32) | i } else { i }).collect();
        for i in (1..eids.len()).rev() {
            let j = rng.below(i as u64 + 1) as usize;
            eids.swap(i, j);
        }
        for &e in &eids {
            let t = doc_text(&mut rng);
            idx.index(e, &t);
            docs.insert(e, t);
        }
        check(&idx, &docs, n, &mut rng, "順不同で登録 (compact の前)");
        // 消しと登録し直し (同じ eid に別の text)
        for &e in eids.iter().step_by(5) {
            idx.remove(e);
            docs.remove(&e);
        }
        for &e in eids.iter().skip(1).step_by(9) {
            if docs.contains_key(&e) {
                let t = doc_text(&mut rng);
                idx.index(e, &t);
                docs.insert(e, t);
            }
        }
        check(&idx, &docs, n, &mut rng, "消しと登録し直しの後");
        idx.compact();
        check(&idx, &docs, n, &mut rng, "compact の後");
        // 昇順に足し続ける (並んだ list に後ろから足す) と、同じ doc に同じ gram が何度も出る
        for e in 1000..1100u64 {
            let t = doc_text(&mut rng);
            let t = format!("{t}{t}");
            idx.index(e, &t);
            docs.insert(e, t);
        }
        check(&idx, &docs, n, &mut rng, "昇順に足した後");
        // save して mmap で開く
        let mut path = std::env::temp_dir();
        path.push(format!("enchudb-ngram-and-{}-{n}.etxt", std::process::id()));
        let p = path.to_str().unwrap();
        idx.save(p).unwrap();
        let mapped = NgramIndex::open(p).unwrap();
        check(&mapped, &docs, n, &mut rng, "mmap");
        let _ = std::fs::remove_file(&path);
    }
}
