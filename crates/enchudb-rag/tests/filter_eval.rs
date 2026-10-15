//! Filter の評価を、手元に覚えた値をなめた答えと比べる（別の道）。値の一致・文字・範囲・その OR / AND / NOT（入れ子、空の
//! AND / OR、無い紐、辞書に無い語を含む）を乱数で組み、書き換え（upsert）と消し（delete）の後にも比べる。
//! search は top_k を全件より大きくして、候補の全部を返させる。

use enchudb_rag::{Chunk, Filter, HybridQuery, Meta, Query, RagStore};
use std::collections::HashMap;
use std::path::PathBuf;

const LANGS: [&str; 3] = ["en", "ja", "fr"];
/// 文字の条件に使う語（"de" は辞書に無い）。
const WORDS: [&str; 4] = ["en", "ja", "fr", "de"];

#[derive(Clone, Copy)]
struct Row {
    tenant: Option<u32>,
    lang: Option<usize>,
    year: Option<u32>,
}

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

fn tmp_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("enchudb-rag-test-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_file(p.with_extension("db"));
    let _ = std::fs::remove_file(p.with_extension("vec"));
    p
}

fn random_row(rng: &mut Rng) -> Row {
    Row {
        tenant: (rng.below(8) != 0).then(|| rng.below(6) as u32),
        lang: (rng.below(5) != 0).then(|| rng.below(3) as usize),
        year: (rng.below(10) != 0).then(|| 2000 + rng.below(30) as u32),
    }
}

fn chunk(rng: &mut Rng, r: &Row, i: usize) -> Chunk {
    let mut meta = Meta::new();
    if let Some(t) = r.tenant {
        meta = meta.value("tenant", t);
    }
    if let Some(l) = r.lang {
        meta = meta.symbol("lang", LANGS[l]);
    }
    if let Some(y) = r.year {
        meta = meta.value("year", y);
    }
    let vector = (0..4).map(|_| (rng.below(1000) as f32 + 1.0) / 1000.0).collect();
    Chunk { text: format!("doc {i} tenant{}", r.tenant.unwrap_or(9)), vector, meta }
}

fn random_filter(rng: &mut Rng, depth: u32) -> Filter {
    let k = if depth == 0 { rng.below(8) } else { rng.below(13) };
    match k {
        // 6 = どの row も持たない値
        0 => Filter::value("tenant", rng.below(7) as u32),
        1 => Filter::symbol("lang", WORDS[rng.below(4) as usize]),
        2 => {
            let (a, b) = (1998 + rng.below(34) as u32, 1998 + rng.below(34) as u32);
            Filter::range("year", a.min(b)..=a.max(b))
        }
        3 => Filter::any_value("tenant", (0..rng.below(4)).map(|_| rng.below(7) as u32).collect()),
        4 => Filter::any_symbol("lang", (0..rng.below(3)).map(|_| WORDS[rng.below(4) as usize].to_string()).collect()),
        5 => Filter::value("year", 2000 + rng.below(30) as u32),
        // 紐が無い
        6 => Filter::value("nosuch", 1),
        7 => Filter::all(),
        8 | 9 => Filter::And((0..rng.below(4)).map(|_| random_filter(rng, depth - 1)).collect()),
        10 | 11 => Filter::Or((0..rng.below(4)).map(|_| random_filter(rng, depth - 1)).collect()),
        _ => random_filter(rng, depth - 1).not(),
    }
}

fn holds(f: &Filter, r: &Row) -> bool {
    match f {
        Filter::All => true,
        Filter::Value { field, value } => match field.as_str() {
            "tenant" => r.tenant == Some(*value),
            "year" => r.year == Some(*value),
            _ => false,
        },
        Filter::Symbol { field, value } => field == "lang" && r.lang.is_some_and(|l| LANGS[l] == value),
        Filter::Range { field, min, max } => field == "year" && r.year.is_some_and(|y| *min <= y && y <= *max),
        Filter::AnyValue { field, values } => {
            values.iter().any(|&v| holds(&Filter::Value { field: field.clone(), value: v }, r))
        }
        Filter::AnySymbol { field, values } => {
            values.iter().any(|v| holds(&Filter::Symbol { field: field.clone(), value: v.clone() }, r))
        }
        Filter::And(fs) => fs.iter().all(|f| holds(f, r)),
        Filter::Or(fs) => fs.iter().any(|f| holds(f, r)),
        Filter::Not(f) => !holds(f, r),
    }
}

#[test]
fn filters_match_a_scan_of_the_written_values() {
    let path = tmp_path("filter-eval");
    let mut store = RagStore::builder()
        .path(&path)
        .dim(4)
        .meta_value("tenant", 0)
        .meta_symbol("lang")
        .meta_value("year", 0)
        .max_entities(2000)
        .build()
        .unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut rows: HashMap<u64, Row> = HashMap::new();
    for i in 0..400 {
        let r = random_row(&mut rng);
        let c = chunk(&mut rng, &r, i);
        let eid = store.insert(c).unwrap();
        rows.insert(eid, r);
    }
    let q = [0.5f32, 0.5, 0.5, 0.5];
    let check = |store: &RagStore, rows: &HashMap<u64, Row>, rng: &mut Rng, stage: &str| {
        let all: usize = rows.len();
        let (mut nonempty, mut partial) = (0, 0);
        for t in 0..300 {
            let f = random_filter(rng, 2 + (t % 2) as u32);
            let mut want: Vec<u64> = rows.iter().filter(|(_, r)| holds(&f, r)).map(|(&e, _)| e).collect();
            want.sort_unstable();
            let mut got: Vec<u64> = store.search(Query::new(&q).filter(f.clone()).top_k(10_000)).unwrap().into_iter().map(|h| h.eid).collect();
            got.sort_unstable();
            assert_eq!(got, want, "{stage} #{t}: {f:?}");
            nonempty += usize::from(!want.is_empty());
            partial += usize::from(!want.is_empty() && want.len() < all);
            // hybrid は候補の中から選ぶ
            if t % 10 == 0 {
                let hits = store.hybrid_search(HybridQuery::new(&q, "doc tenant1").filter(f.clone()).top_k(20)).unwrap();
                assert!(hits.iter().all(|h| want.binary_search(&h.eid).is_ok()), "{stage} hybrid #{t}: {f:?}");
                assert_eq!(hits.is_empty(), want.is_empty(), "{stage} hybrid #{t}: 候補の有無と答えの有無が違う");
            }
        }
        // 対照: 当たる条件と、全部でも空でもない条件が十分にある (比べが空振りしていない)
        assert!(nonempty >= 100 && partial >= 80, "{stage}: 当たる {nonempty} / 一部だけ当たる {partial}");
    };
    check(&store, &rows, &mut rng, "書き換える前");
    // 書き換え (meta を丸ごと置き換え) と消し: bucket に古い entry が残る
    let mut ids: Vec<u64> = rows.keys().copied().collect();
    ids.sort_unstable();
    for i in 0..150 {
        let eid = ids[rng.below(ids.len() as u64) as usize];
        let r = random_row(&mut rng);
        let c = chunk(&mut rng, &r, 1000 + i);
        store.upsert(eid, c).unwrap();
        rows.insert(eid, r);
    }
    for _ in 0..60 {
        let eid = ids[rng.below(ids.len() as u64) as usize];
        if rows.remove(&eid).is_some() {
            store.delete(eid);
        }
    }
    check(&store, &rows, &mut rng, "書き換えと消しの後");
}
