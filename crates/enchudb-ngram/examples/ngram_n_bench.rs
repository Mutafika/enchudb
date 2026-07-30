//! #121 の判断材料 — n = 2 / 3 / 4 を **同じコーパス**で測り比べる。
//!
//! 最適な n はスクリプトとコーパスに依存するので、「trigram の方が良い」は一般には
//! 言えない。この example は各 n について
//!
//! - `.etxt` サイズ（postings-only / 原文込み）と相異なる gram 数
//! - クエリ長ごとの **候補数** と **偽陽性率**（= 上位層が払う `.contains()` 検証コスト）
//! - 候補探索と検証込み検索のレイテンシ
//!
//! を出す。ASCII と CJK でこの表がどう違うかが、この issue の存在価値そのもの。
//!
//! ```sh
//! # コーパスは「1 行 1 doc」のテキストファイル。label= は省略可（既定はファイル名）。
//! cargo run --release -p enchudb-ngram --example ngram_n_bench -- \
//!     ja=corpus_ja.txt en=corpus_en.txt
//!
//! # 引数なしなら /usr/share/dict/words から ASCII コーパスを合成して回す
//! cargo run --release -p enchudb-ngram --example ngram_n_bench
//! ```

use std::collections::HashSet;
use std::time::Instant;

use enchudb_ngram::{gram, NgramIndex};

/// doc としては短すぎる行を捨てる閾値（文字数）。見出しや区切り線を除く。
const MIN_DOC_CHARS: usize = 12;
/// 1 コーパスあたりの最大 doc 数（測定時間を抑える）。
const MAX_DOCS: usize = 40_000;
/// 測るクエリ長（文字数）。
const QUERY_LENS: [usize; 5] = [2, 3, 4, 6, 10];
/// クエリ長ごとのサンプル数。
const QUERIES_PER_LEN: usize = 200;
/// レイテンシ測定の繰り返し回数。
const REPEAT: usize = 5;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let corpora = if args.is_empty() {
        eprintln!("(引数なし: /usr/share/dict/words から ASCII コーパスを合成します)");
        vec![synth_ascii_corpus()]
    } else {
        args.iter().filter_map(|a| load_corpus(a)).collect()
    };

    for (label, docs) in corpora {
        if docs.len() < 100 {
            eprintln!("skip {label}: doc が {} 件しかない", docs.len());
            continue;
        }
        report(&label, &docs);
    }
}

// ── コーパス読み込み ────────────────────────────────────────────────

/// `label=path` または `path`。1 行 1 doc、短すぎる行は捨てる。
fn load_corpus(arg: &str) -> Option<(String, Vec<String>)> {
    let (label, path) = match arg.split_once('=') {
        Some((l, p)) => (l.to_string(), p.to_string()),
        None => (
            std::path::Path::new(arg)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| arg.to_string()),
            arg.to_string(),
        ),
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("skip {path}: {e}");
            return None;
        }
    };
    let docs: Vec<String> = text
        .lines()
        .map(|l| l.trim())
        .filter(|l| l.chars().count() >= MIN_DOC_CHARS)
        .take(MAX_DOCS)
        .map(|l| l.to_string())
        .collect();
    Some((label, docs))
}

/// 単語リストから ASCII コーパスを合成する。単語内の文字分布は実物なので、
/// bigram / trigram のエントロピー比較という目的には足りる。
fn synth_ascii_corpus() -> (String, Vec<String>) {
    let words: Vec<String> = std::fs::read_to_string("/usr/share/dict/words")
        .map(|t| {
            t.lines()
                .map(|w| w.trim().to_ascii_lowercase())
                .filter(|w| w.len() >= 3 && w.is_ascii())
                .collect()
        })
        .unwrap_or_default();
    if words.is_empty() {
        eprintln!("/usr/share/dict/words が無いので合成できません。コーパスを引数で渡してください。");
        std::process::exit(1);
    }
    let mut rng = Rng::new(0x5EED_1234_ABCD_0001);
    let docs: Vec<String> = (0..20_000)
        .map(|_| {
            let n_words = 12 + (rng.next() % 12) as usize;
            (0..n_words)
                .map(|_| words[(rng.next() as usize) % words.len()].as_str())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    ("synth-ascii".to_string(), docs)
}

// ── 測定 ────────────────────────────────────────────────────────────

fn report(label: &str, docs: &[String]) {
    let total_chars: usize = docs.iter().map(|d| d.chars().count()).sum();
    let total_bytes: usize = docs.iter().map(|d| d.len()).sum();
    println!();
    println!("══ corpus: {label}");
    println!(
        "   docs={} chars={} bytes={} (avg {:.0} chars/doc)",
        docs.len(),
        total_chars,
        total_bytes,
        total_chars as f64 / docs.len() as f64
    );

    let queries = sample_queries(docs);

    println!();
    println!("   ── index");
    println!("   {:>2} {:>12} {:>14} {:>14} {:>10}", "n", "distinct", "postings-only", "with text", "build ms");
    let mut indexes = Vec::new();
    for n in gram::MIN_N..=gram::MAX_N {
        let t0 = Instant::now();
        let mut idx = NgramIndex::with_n(n).unwrap();
        for (eid, doc) in docs.iter().enumerate() {
            idx.index(eid as u64, doc);
        }
        idx.compact();
        let build_ms = t0.elapsed().as_secs_f64() * 1e3;

        let mut po = Vec::new();
        idx.write_to_postings_only(&mut po).unwrap();
        let mut full = Vec::new();
        idx.write_to(&mut full).unwrap();

        println!(
            "   {:>2} {:>12} {:>14} {:>14} {:>10.0}",
            n,
            idx.gram_count(),
            human(po.len()),
            human(full.len()),
            build_ms
        );
        indexes.push((n, idx));
    }

    println!();
    println!("   ── query (候補数 / 偽陽性率 / レイテンシ)");
    println!(
        "   {:>2} {:>5} {:>7} {:>10} {:>10} {:>8} {:>11} {:>11}",
        "n", "qlen", "queries", "hits/q", "cand/q", "FP率", "cand µs", "search µs"
    );
    for (n, idx) in &indexes {
        for &qlen in &QUERY_LENS {
            let qs = match queries.get(&qlen) {
                Some(v) if !v.is_empty() => v,
                _ => continue,
            };

            // 正解 = 総当たりの substring 判定（index を通さない独立した基準）
            let truth: Vec<usize> = qs
                .iter()
                .map(|q| docs.iter().filter(|d| d.contains(q.as_str())).count())
                .collect();

            if qlen < *n {
                // index で絞れない = O(N) 全走査に落ちるクエリ長
                let t0 = Instant::now();
                for _ in 0..REPEAT {
                    for q in qs {
                        std::hint::black_box(idx.scan(|t| t.contains(q.as_str())));
                    }
                }
                let us = t0.elapsed().as_secs_f64() * 1e6 / (REPEAT * qs.len()) as f64;
                println!(
                    "   {:>2} {:>5} {:>7} {:>10.1} {:>10} {:>8} {:>11} {:>11.1}",
                    n,
                    qlen,
                    qs.len(),
                    mean(&truth),
                    "-",
                    "scan",
                    "-",
                    us
                );
                continue;
            }

            let cand_counts: Vec<usize> = qs.iter().map(|q| idx.candidates(q).len()).collect();

            let t0 = Instant::now();
            for _ in 0..REPEAT {
                for q in qs {
                    std::hint::black_box(idx.candidates(q));
                }
            }
            let cand_us = t0.elapsed().as_secs_f64() * 1e6 / (REPEAT * qs.len()) as f64;

            // 検証込み = 候補を原文照合で絞る（上位 textsearch の substring ポリシー相当）
            let t0 = Instant::now();
            for _ in 0..REPEAT {
                for q in qs {
                    let c = idx.candidates(q);
                    let verified: Vec<u64> = c
                        .into_iter()
                        .filter(|&eid| idx.get_text(eid).is_some_and(|t| t.contains(q.as_str())))
                        .collect();
                    std::hint::black_box(verified);
                }
            }
            let search_us = t0.elapsed().as_secs_f64() * 1e6 / (REPEAT * qs.len()) as f64;

            let cand_total: usize = cand_counts.iter().sum();
            let truth_total: usize = truth.iter().sum();
            let fp = if cand_total == 0 {
                0.0
            } else {
                (cand_total - truth_total) as f64 * 100.0 / cand_total as f64
            };

            println!(
                "   {:>2} {:>5} {:>7} {:>10.1} {:>10.1} {:>7.2}% {:>11.1} {:>11.1}",
                n,
                qlen,
                qs.len(),
                mean(&truth),
                mean(&cand_counts),
                fp,
                cand_us,
                search_us
            );
        }
    }
}

/// コーパス自身から長さごとのクエリを抜く（実在する部分文字列 = 実際に引かれる形）。
fn sample_queries(docs: &[String]) -> std::collections::HashMap<usize, Vec<String>> {
    let mut rng = Rng::new(0xC0FF_EE00_1234_5678);
    let mut out = std::collections::HashMap::new();
    for &qlen in &QUERY_LENS {
        let mut seen = HashSet::new();
        let mut qs = Vec::new();
        let mut tries = 0;
        while qs.len() < QUERIES_PER_LEN && tries < QUERIES_PER_LEN * 200 {
            tries += 1;
            let doc = &docs[(rng.next() as usize) % docs.len()];
            let chars: Vec<char> = doc.chars().collect();
            if chars.len() < qlen {
                continue;
            }
            let start = (rng.next() as usize) % (chars.len() - qlen + 1);
            let q: String = chars[start..start + qlen].iter().collect();
            // 空白だけ / 空白始まりは検索語として不自然なので捨てる
            if q.trim().chars().count() != qlen {
                continue;
            }
            if seen.insert(q.clone()) {
                qs.push(q);
            }
        }
        out.insert(qlen, qs);
    }
    out
}

fn mean(v: &[usize]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.iter().sum::<usize>() as f64 / v.len() as f64
}

fn human(bytes: usize) -> String {
    const KIB: f64 = 1024.0;
    let b = bytes as f64;
    if b >= KIB * KIB {
        format!("{:.1} MiB", b / (KIB * KIB))
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// 決定的な xorshift64*（測定を再現可能にするため。外部 crate に依存しない）
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}
