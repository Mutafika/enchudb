//! loom model — `AppendBucket` の publish protocol を全 interleaving で検証する。#95。
//!
//! ## なぜ de-epoch した model なのか
//! 実 `AppendBucket` は backing の遅延解放に crossbeam-epoch を使うが、**loom は
//! crossbeam-epoch を模擬できない**（loom は使う並行プリミティブ全てが loom 版である
//! 前提。epoch は std atomics + 独自 collector）。よって loom で検証できるのは、単一
//! backing 内の **publish protocol の memory ordering**（俺が手書きした部分）に絞る:
//!
//!   writer: spare slot に書く → `len.store(l+1, Release)` で publish
//!   reader: `len.load(Acquire)` で観測 → published prefix `[0..n]` を読む
//!
//! この Release/Acquire ハンドシェイクが正しければ「reader が len≥i+1 を観測した時、
//! slot i への書き込みは happens-before」で、writer が書き込み中の slot を reader が
//! 読むことはない（= data race なし）。loom は全 interleaving を網羅してこれを証明する。
//! `Release`→`Relaxed` に落とすと loom は torn read の interleaving を見つけて失敗する
//! （= この test が ordering の正しさを実際に gate している）。
//!
//! realloc の epoch 遅延解放は loom 範囲外（Miri Tree Borrows で別途 UB 検証済み）。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_append_publish --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build（CI を遅くしない）。

#![cfg(loom)]

use loom::cell::UnsafeCell;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::Arc;

/// 単一 backing 内の publish protocol の de-epoch model（AppendBucket と同一 ordering）。
struct PubBuf {
    data: Vec<UnsafeCell<u32>>, // 固定 cap（realloc は loom 範囲外）
    len: AtomicUsize,           // publish 済み件数
}

// SAFETY: published [0..len] は不変、writer は spare slot [len] のみ書く。loom が
// UnsafeCell アクセスを追跡して disjoint 性 / race を検査する。
unsafe impl Sync for PubBuf {}
unsafe impl Send for PubBuf {}

impl PubBuf {
    fn with_cap(cap: usize) -> Self {
        Self {
            data: (0..cap).map(|_| UnsafeCell::new(0)).collect(),
            len: AtomicUsize::new(0),
        }
    }

    /// 単一 writer append。spare slot に書いてから len を Release で publish。
    fn push(&self, v: u32) {
        let l = self.len.load(Ordering::Relaxed); // len は writer 専有 = Relaxed で可
        self.data[l].with_mut(|p| unsafe { *p = v });
        self.len.store(l + 1, Ordering::Release); // ← Relaxed に落とすと loom が torn read を検出（検証済み）
    }

    /// 多 reader。len を Acquire で観測してから published prefix を読む。
    fn snapshot(&self) -> Vec<u32> {
        let n = self.len.load(Ordering::Acquire);
        (0..n)
            .map(|i| self.data[i].with(|p| unsafe { *p }))
            .collect()
    }
}

/// 1 writer（2 push）+ 1 reader。どの interleaving でも reader が見る prefix は
/// [10, 20] の切り詰め（torn / 未初期化 slot は観測されない）。
#[test]
fn publish_single_writer_single_reader() {
    loom::model(|| {
        let buf = Arc::new(PubBuf::with_cap(2));
        let w = {
            let b = buf.clone();
            loom::thread::spawn(move || {
                b.push(10);
                b.push(20);
            })
        };

        let snap = buf.snapshot();
        // 単調性 + 値の正しさ: i 番目は必ず [10,20][i]。
        assert!(snap.len() <= 2);
        for (i, &v) in snap.iter().enumerate() {
            assert_eq!(v, [10u32, 20][i], "torn/未初期化 slot を観測 (idx {i})");
        }

        w.join().unwrap();
    });
}

/// 1 writer（2 push）+ 2 reader。reader を増やして interleaving 空間を広げる。
#[test]
fn publish_single_writer_two_readers() {
    loom::model(|| {
        let buf = Arc::new(PubBuf::with_cap(2));
        let w = {
            let b = buf.clone();
            loom::thread::spawn(move || {
                b.push(10);
                b.push(20);
            })
        };
        let r = {
            let b = buf.clone();
            loom::thread::spawn(move || {
                let snap = b.snapshot();
                for (i, &v) in snap.iter().enumerate() {
                    assert_eq!(v, [10u32, 20][i], "reader thread: torn slot (idx {i})");
                }
            })
        };

        let snap = buf.snapshot();
        for (i, &v) in snap.iter().enumerate() {
            assert_eq!(v, [10u32, 20][i], "main reader: torn slot (idx {i})");
        }

        w.join().unwrap();
        r.join().unwrap();
    });
}
