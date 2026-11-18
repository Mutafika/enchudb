//! loom model — #857 / #877 / #1017: 版数なしで消した行の数え (`TableDef::note_freed`) と、 `oplog_sync` の止め。
//!
//! 書き手 (削除) は行 `idx` を数えてから Delete を WAL に積む (#877)。 数えは、 止めの答えが変わらない時 (`idx` < sidecar の値 /
//! `idx` < 数えた所) は読むだけで返り、 それ以外は `fetch_max` (#1017。 前は毎回 `fetch_max` で、 書き手 8 本が表の定義の cache
//! line を取り合った)。 `oplog_sync` は WAL の終端を読んでから止め (`freed_end > persisted_next`) を読み、 止めなければ checkpoint を
//! その終端まで進める。
//!
//! 守る性質: **止めずに checkpoint が越えた Delete の行は、 sidecar の値より前** (落ちた後に開いた `next_local` が覆う = その eid を
//! 払い出し直さない)。 読むだけで返った書き手が、 別の書き手の上げた `freed_end` を読んで返った時も同じ: その削除の Delete を
//! 見た `oplog_sync` は、 WAL の lock の受け渡しの後に読むので、 同じ atomic の読みの順 (coherence) で読んだ値以上を読む。
//! 書き手 A は行 6 を数えて (RMW) 積む。 書き手 B は sidecar が覆う行 2 と、 A の上げた所より前の行 5 を数えて積む (A の後なら読むだけ)。
//!
//! 数えを積んだ後にする形 (#877 の前) は loom が窓を見つける (`count_after_append_misses_the_hold`)。 模型は考え方の写し
//! (実装の行を変えても落ちない)。 実装の数えは engine の `freed_count_tests` (RMW の回数・並んだ書き手の最大・sidecar が覆う行) が見る。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_freed_slot_count --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicU32, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

/// sidecar に置いた `next_local` (この値より前の行は、 落ちた後に開いても払い出し済みと分かる)。
const PERSISTED: u32 = 4;

struct Table {
    persisted_next: AtomicU32,
    freed_end: AtomicU32,
    /// WAL: 積んだ Delete の行 (`append_lock` の下)。
    wal: Mutex<Vec<u32>>,
}

impl Table {
    fn new() -> Self {
        Self { persisted_next: AtomicU32::new(PERSISTED), freed_end: AtomicU32::new(0), wal: Mutex::new(Vec::new()) }
    }

    /// `TableDef::note_freed` の写し。
    fn note_freed(&self, idx: u32) {
        if idx < self.persisted_next.load(Ordering::Acquire) || idx < self.freed_end.load(Ordering::Acquire) {
            return;
        }
        self.freed_end.fetch_max(idx + 1, Ordering::AcqRel);
    }

    /// 削除: 数えてから積む (#877)。 `count_after_append` は直す前の形。
    fn delete(&self, idx: u32, count_after_append: bool) {
        if !count_after_append {
            self.note_freed(idx);
        }
        self.wal.lock().unwrap().push(idx);
        if count_after_append {
            self.note_freed(idx);
        }
    }

    /// `oplog_sync`: WAL の終端まで (積んだ record) を書き出し、 止めを読み、 止めなければ checkpoint をそこまで進める。
    /// 返すのは (checkpoint が越えた Delete の行、 止めを読んだ時の sidecar の値)。 止めた時は越えない。
    fn oplog_sync(&self) -> (Vec<u32>, u32) {
        let written = self.wal.lock().unwrap().clone();
        let freed = self.freed_end.load(Ordering::Acquire);
        let persisted = self.persisted_next.load(Ordering::Acquire);
        if freed > persisted { (Vec::new(), persisted) } else { (written, persisted) }
    }
}

fn model(count_after_append: bool) {
    loom::model(move || {
        let t = Arc::new(Table::new());
        let a = {
            let t = t.clone();
            thread::spawn(move || t.delete(6, count_after_append))
        };
        let b = {
            let t = t.clone();
            thread::spawn(move || {
                t.delete(2, count_after_append);
                t.delete(5, count_after_append);
            })
        };
        let (passed, persisted) = t.oplog_sync();
        for idx in passed {
            assert!(idx < persisted, "止めずに checkpoint が行 {idx} の Delete を越えた (sidecar の値 {persisted})");
        }
        a.join().unwrap();
        b.join().unwrap();
        // 全部積んだ後: 数えた所は消した行のうち sidecar の値以上の最大 + 1
        let end = t.freed_end.load(Ordering::Acquire);
        assert_eq!(end, 7, "数えた所が消した行の最大 + 1 でない");
    });
}

/// 数えてから積めば、 読むだけで返った書き手の Delete も、 止めずに越えるのは sidecar が覆う行だけ。
#[test]
fn count_before_append_holds_for_every_uncovered_row() {
    model(false);
}

/// 対照: 積んでから数える形 (#877 の前) は、 数える前に並んだ `oplog_sync` が Delete を越える (loom が窓を見つける)。
#[test]
#[should_panic]
fn count_after_append_misses_the_hold() {
    model(true);
}
