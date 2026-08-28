//! #188 の実データ検証 — 本物の `.etxt` を segment に刻んで統合し、
//! **元のバイト列と一致するか** と **ピークメモリがどこまで落ちるか** を見る。
//!
//! 合成データのテストは「形式として正しい」ことしか言わない。実際に効くかは
//! 実コーパス（naruhodo の 494,133 条・1GB）でしか分からない。
//!
//! ```sh
//! # 1) 既存の .etxt を N doc ずつの segment に割る（各 segment の build がピーク）
//! segment_merge_real split <src.etxt> <outdir> <docs_per_segment>
//! # 2) segment を 1 本に統合する（ここが本命 — メモリが本文量から独立しているか）
//! segment_merge_real merge <outdir> <out.etxt>
//! # 3) 元と突合
//! segment_merge_real cmp <a.etxt> <b.etxt>
//! ```
//!
//! 各フェーズを別プロセスにしてあるのは、`/usr/bin/time -l` の Max RSS が
//! フェーズごとに読めるようにするため。

use enchudb_ngram::{MappedIndex, NgramIndex};
use std::path::Path;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("");
    match cmd {
        "split" => split(&args[2], &args[3], args[4].parse().expect("docs_per_segment")),
        "merge" => merge(&args[2], &args[3]),
        "cmp" => cmp(&args[2], &args[3]),
        _ => {
            eprintln!("usage: segment_merge_real split <src.etxt> <outdir> <docs_per_seg>");
            eprintln!("       segment_merge_real merge <segdir> <out.etxt>");
            eprintln!("       segment_merge_real cmp <a.etxt> <b.etxt>");
            std::process::exit(2);
        }
    }
}

/// 既存 index を doc 単位で読み出し、`per_seg` doc ごとに segment を焼く。
///
/// 元 index は mmap から読むので、メモリに載るのは **いま組んでいる segment だけ**。
fn split(src: &str, outdir: &str, per_seg: usize) {
    let t0 = Instant::now();
    std::fs::create_dir_all(outdir).expect("create_dir_all");
    let m = MappedIndex::open(Path::new(src)).expect("open src");
    eprintln!("src: docs={} grams={} n={}", m.doc_count(), m.gram_count(), m.n());

    let mut seg = NgramIndex::with_n(m.n()).expect("with_n");
    let (mut n_in_seg, mut n_seg, mut n_doc) = (0usize, 0usize, 0usize);
    let flush = |seg: &mut NgramIndex, n_seg: &mut usize, n_in_seg: &mut usize| {
        if *n_in_seg == 0 { return; }
        let path = format!("{outdir}/seg{:04}.etxt", *n_seg);
        seg.save(&path).expect("save segment");
        eprintln!("  seg{:04}: {} doc → {}", *n_seg, *n_in_seg, path);
        *n_seg += 1;
        *n_in_seg = 0;
    };
    // for_each_doc は mmap 上の &str をそのまま渡す（原文のコピーを作らない）
    let mut pending: Vec<(u64, String)> = Vec::new();
    m.for_each_doc(|eid, text| {
        pending.push((eid, text.to_string()));
        if pending.len() >= per_seg {
            for (e, t) in pending.drain(..) { seg.index(e, &t); }
            n_in_seg = per_seg;
            n_doc += per_seg;
            flush(&mut seg, &mut n_seg, &mut n_in_seg);
            seg = NgramIndex::with_n(m.n()).expect("with_n");
        }
    });
    if !pending.is_empty() {
        n_in_seg = pending.len();
        n_doc += pending.len();
        for (e, t) in pending.drain(..) { seg.index(e, &t); }
        flush(&mut seg, &mut n_seg, &mut n_in_seg);
    }
    eprintln!("split: {n_doc} doc → {n_seg} segment ({:.1}s)", t0.elapsed().as_secs_f64());
}

fn merge(segdir: &str, out: &str) {
    let t0 = Instant::now();
    let mut segs: Vec<String> = std::fs::read_dir(segdir)
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_string_lossy().into_owned())
        .filter(|p| p.ends_with(".etxt"))
        .collect();
    segs.sort();
    eprintln!("merge: {} segment", segs.len());
    let refs: Vec<&str> = segs.iter().map(|s| s.as_str()).collect();
    let st = NgramIndex::merge_files(&refs, out).expect("merge_files");
    eprintln!(
        "merge: grams={} postings={} docs={} text={}B superseded={} ({:.1}s) → {out}",
        st.grams, st.postings, st.docs, st.text_bytes, st.superseded_docs,
        t0.elapsed().as_secs_f64()
    );
}

fn cmp(a: &str, b: &str) {
    let (ba, bb) = (std::fs::read(a).expect("read a"), std::fs::read(b).expect("read b"));
    if ba == bb {
        println!("IDENTICAL ({} bytes)", ba.len());
        return;
    }
    println!("DIFFERENT: {} vs {} bytes", ba.len(), bb.len());
    let n = ba.len().min(bb.len());
    if let Some(i) = (0..n).find(|&i| ba[i] != bb[i]) {
        println!("  first差分 at byte {i}: {:#04x} vs {:#04x}", ba[i], bb[i]);
    }
    std::process::exit(1);
}
