//! loom model — #850: checkpoint を進める側が、 WAL に積んだだけでまだ本体に当てていない record を越えない
//! (`OpLog::applying` / `OpLog::applied_through`)。
//!
//! 書き手の印は積むまで位置を持たない。 record を積む時に、 **`written` を進める前に** record の位置が置かれる:
//!
//! - 自分で積む (`append_lock` を取れた): record の場所を取った後に、 自分の印 (thread ごとの line) に record の位置を置く
//! - まとめ書き (#506、 lock を持つ別の書き手が積む): 積んだ書き手が、 頼んだ書き手の置き場 (`CombineSlot::served`) に record の
//!   位置を置く。 頼んだ書き手は結果を受け取る時に、 その位置を自分の印へ置いて (Release) から置き場を外す (Release)
//!
//! 書き手は本体 (cell) に当て終えてから、 印を外す。
//!
//! 書き出す側は lock を取らずに、 **`written` を先に読み** (Acquire)、 その後に置き場、 印の順に読んで (Acquire)、 覆う位置を
//! 「`written` と、 置かれている位置の最小」 の小さい方に切る。 その後に本体を書き出す (cell を読む)。 守る性質: **覆う位置が
//! record を越えたなら、 書き出した本体にその cell がある** (越えて cell が無いと、 落ちた後の再生は checkpoint から後なので、
//! その書き込みは本体に当たらない)。
//!
//! 対照は loom が窓を見つける: 位置を見ない形 (直す前)、 `written` を進めてから位置を置く形 (自分で積む道 / まとめ書きの道)、
//! 書き出す側が位置を先に読む形、 `written` の読みを Relaxed にした形、 印を外す store を Relaxed にした形、 同じ書き手の 2 件目の
//! 位置を置く store を Relaxed にした形、 まとめ書きで書き出す側が印を置き場より先に読む形。
//!
//! 印を外す store と、 位置を置く store はどちらも Release: 外れたのを読んだ側にも、 同じ書き手の次の位置を読んだ側にも、 その前に
//! 当てた cell が見える (cell の store 自体は Relaxed でも)。 1 件だけの模型では位置の store が Relaxed でも窓は無い (同じ thread が
//! その後に `written` を Release で進める) が、 2 件続けると窓がある (`a_relaxed_second_write_position_lets_the_cut_pass`)。
//!
//! 模型は考え方の写し (実装の行を変えても落ちない)。 実装の窓は engine の `checkpoint_before_apply_tests` (割り込み点) と
//! oplog の `applied_through_*` / `a_writer_waiting_*` / `checkpoint_and_reset_stops_before_…` が踏む。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_wal_applied_through --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

const IDLE: u64 = u64::MAX;
const ARMED: u64 = u64::MAX - 1;
/// record は位置 1 から 2 まで (1 件)
const START: u64 = 1;
const END: u64 = 2;

struct Wal {
    /// 書き終えた終端
    written: AtomicU64,
    /// 書き手の印 (自分で積む時に位置を置く)
    mark: AtomicU64,
    /// 書き手の置き場 (頼まれた書き手が位置を置く)
    served: AtomicU64,
    /// 本体の cell (0 = 前の値、 1 = この書き込みの値)
    cell: AtomicU64,
    /// まとめ書きの頼み (0 = 無し、 1 = 置いた、 2 = 積んだ)
    request: AtomicU32,
}

#[derive(Clone, Copy)]
struct Shape {
    /// 頼まれた別の書き手が record を書く (まとめ書き)
    combined: bool,
    /// 位置を `written` より前に置く (正しい順)
    pos_first: bool,
    /// 書き出す側が `written` を先に読む (正しい順)
    written_first: bool,
    written_order: Ordering,
    /// 書き出す側が位置を見る
    use_marks: bool,
    /// 書き出す側が置き場を印より先に読む (正しい順)
    served_first: bool,
    /// 当て終えて印を外す store の順序 (Release が正しい)
    clear_order: Ordering,
}

const RIGHT: Shape = Shape {
    combined: false,
    pos_first: true,
    written_first: true,
    written_order: Ordering::Acquire,
    use_marks: true,
    served_first: true,
    clear_order: Ordering::Release,
};

impl Wal {
    fn new() -> Self {
        Self {
            written: AtomicU64::new(START),
            mark: AtomicU64::new(IDLE),
            served: AtomicU64::new(IDLE),
            cell: AtomicU64::new(0),
            request: AtomicU32::new(0),
        }
    }

    /// `append_lock` の下で record を書く所: 位置を `at` に置いて `written` を進める。
    fn append(&self, s: Shape, at: &AtomicU64) {
        // 位置の store は Release (実装と同じ。 1 件だけのこの模型では Relaxed でも窓は無い、 下の 2 件の模型)
        if s.pos_first {
            at.store(START, Ordering::Release);
            self.written.store(END, Ordering::Release);
        } else {
            self.written.store(END, Ordering::Release);
            at.store(START, Ordering::Release);
        }
    }

    /// 書き出す側: 覆う位置を決めてから本体を書き出す。 (覆う位置, 書き出した cell)
    fn write_out(&self, s: Shape) -> (u64, u64) {
        let positions = || {
            let (sv, m) = if s.served_first {
                let sv = self.served.load(Ordering::Acquire);
                (sv, self.mark.load(Ordering::Acquire))
            } else {
                let m = self.mark.load(Ordering::Acquire);
                (self.served.load(Ordering::Acquire), m)
            };
            if !s.use_marks {
                return IDLE;
            }
            (if m < ARMED { m } else { IDLE }).min(sv)
        };
        let through = if s.written_first {
            let w = self.written.load(s.written_order);
            w.min(positions())
        } else {
            let p = positions();
            self.written.load(s.written_order).min(p)
        };
        (through, self.cell.load(Ordering::Acquire))
    }
}

fn model(s: Shape) {
    loom::model(move || {
        let wal = Arc::new(Wal::new());
        let writer = {
            let wal = wal.clone();
            thread::spawn(move || {
                if s.combined {
                    wal.request.store(1, Ordering::Release);
                    while wal.request.load(Ordering::Acquire) != 2 {
                        thread::yield_now();
                    }
                    // 結果を受け取る時に、 置き場の位置を自分の印へ移す
                    let at = wal.served.load(Ordering::Relaxed);
                    if at != IDLE {
                        wal.mark.store(at, Ordering::Release);
                        wal.served.store(IDLE, Ordering::Release);
                    }
                } else {
                    wal.append(s, &wal.mark);
                }
                // 外すのは Release (実装と同じ): 外れたのを Acquire で読んだ側には、 その前に当てた cell が見える
                wal.cell.store(1, Ordering::Relaxed);
                wal.mark.store(IDLE, s.clear_order);
            })
        };
        let combiner = s.combined.then(|| {
            let wal = wal.clone();
            thread::spawn(move || {
                while wal.request.load(Ordering::Acquire) != 1 {
                    thread::yield_now();
                }
                wal.append(s, &wal.served);
                wal.request.store(2, Ordering::Release);
            })
        });
        let (through, cell) = wal.write_out(s);
        assert!(through <= START || cell == 1, "覆う位置 {through} が、 本体に当たっていない record を越えた");
        writer.join().unwrap();
        if let Some(c) = combiner {
            c.join().unwrap();
        }
    });
}

/// 位置を先に置き、 書き出す側が `written` を先に読めば、 覆う位置は当てていない record を越えない。
#[test]
fn the_cut_never_passes_a_record_that_is_not_applied() {
    model(RIGHT);
}

/// まとめ書き (record を書くのが頼まれた別の書き手) でも同じ。
#[test]
fn the_cut_never_passes_a_record_appended_by_another_writer() {
    model(Shape { combined: true, ..RIGHT });
}

/// 対照 (直す前): 位置を見ないと、 積んだだけの record を越える。
#[test]
#[should_panic]
fn without_marks_the_cut_passes_an_unapplied_record() {
    model(Shape { use_marks: false, ..RIGHT });
}

/// 対照: `written` を進めてから位置を置くと、 その間に読んだ書き出しが越える (自分で積む道)。
#[test]
#[should_panic]
fn position_after_written_lets_the_cut_pass() {
    model(Shape { pos_first: false, ..RIGHT });
}

/// 対照: 頼まれた書き手が `written` を進めてから位置を置いても同じ。
#[test]
#[should_panic]
fn position_after_written_in_a_combined_append_lets_the_cut_pass() {
    model(Shape { combined: true, pos_first: false, ..RIGHT });
}

/// 対照: 位置を先に読んでから `written` を読むと、 その間に積まれた record を越える。
#[test]
#[should_panic]
fn positions_read_before_written_lets_the_cut_pass() {
    model(Shape { written_first: false, ..RIGHT });
}

/// 対照: `written` の読みが Relaxed だと、 位置と同期しない。
#[test]
#[should_panic]
fn relaxed_written_lets_the_cut_pass() {
    model(Shape { written_order: Ordering::Relaxed, ..RIGHT });
}

/// 対照: 印を外す store が Relaxed だと、 外れた印と、 当てる前の cell の組を読みうる。
#[test]
#[should_panic]
fn relaxed_clear_lets_the_cut_pass() {
    model(Shape { clear_order: Ordering::Relaxed, ..RIGHT });
}

/// 同じ書き手が 2 件続けて書く形 (1 件目を当てて印を外した後に、 2 件目の位置を置く)。 書き出す側が 2 件目の位置を読んでも、
/// 1 件目の cell が見えていること。 位置の store が Relaxed だと、 2 件目の位置を読んで 1 件目を越えたのに、 1 件目の cell が
/// まだ見えない組を読みうる (2 件目の位置の store は、 1 件目を外した Release の後に置いた値だが、 Relaxed の store を読んでも
/// 同期しない)。
fn model_two_writes(pos_order: Ordering) {
    const END2: u64 = 3;
    loom::model(move || {
        let written = Arc::new(AtomicU64::new(START));
        let mark = Arc::new(AtomicU64::new(IDLE));
        let cell1 = Arc::new(AtomicU64::new(0));
        let writer = {
            let (written, mark, cell1) = (written.clone(), mark.clone(), cell1.clone());
            thread::spawn(move || {
                mark.store(START, pos_order);
                written.store(END, Ordering::Release);
                cell1.store(1, Ordering::Relaxed);
                mark.store(IDLE, Ordering::Release);
                mark.store(END, pos_order);
                written.store(END2, Ordering::Release);
            })
        };
        let w = written.load(Ordering::Acquire);
        let m = mark.load(Ordering::Acquire);
        let through = w.min(m);
        let c = cell1.load(Ordering::Acquire);
        assert!(through <= START || c == 1, "覆う位置 {through} が、 本体に当たっていない 1 件目を越えた");
        writer.join().unwrap();
    });
}

/// 位置の store が Release なら、 2 件目の位置を読んだ書き出しの側には 1 件目の cell が見える。
#[test]
fn a_second_write_position_carries_the_first_apply() {
    model_two_writes(Ordering::Release);
}

/// 対照: 位置の store が Relaxed だと、 2 件目の位置を読んでも 1 件目の cell が見えない組がある。
#[test]
#[should_panic]
fn a_relaxed_second_write_position_lets_the_cut_pass() {
    model_two_writes(Ordering::Relaxed);
}

/// 対照: まとめ書きで、 書き出す側が印を置き場より先に読むと、 頼んだ書き手が位置を印へ移す間に、 古い印と外れた置き場の組を読む。
#[test]
#[should_panic]
fn marks_read_before_served_lets_the_cut_pass() {
    model(Shape { combined: true, served_first: false, ..RIGHT });
}
