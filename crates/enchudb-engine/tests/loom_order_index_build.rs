//! loom model — 並びの索引 (`OrderIndex`、 `Engine::declare_order`) の **遅延の作成と並行の書き込み** の契約を全
//! interleaving で検証する。 `loom_lazy_cylinder_build` (#270) と同じ形で、 違いは書き込みが 2 本の紐から来ること:
//! via (会社の ref) の書き手と、 key (年齢) の書き手。 key の列は via の write_lock の外 (key の紐の write_lock の下) で書く。
//!
//! ## 何を守っているのか
//! 書き手 (`Engine::order_note_slow`、 行の lock の下) は 「列を書く → 作ってあれば (無ければ via の write_lock の下で
//! もう一度見て) 今の via / key の値で置き直す」。 置き直しは置き場の鍵の下 (`OrderIndex::place`)。 作る側
//! (`Engine::order_ready`) は 「via の write_lock を取る → 作っていなければ via の列をなめて今の値で置く → 作った印」。
//!
//!   - 書き手が 「作ってある」 を見た = 作り終えている → 書き手が今の値で置き直す (作る側とは重ならない)
//!   - lock の下で 「作っていない」 を見た = 作る側はまだ lock を取れていない → 後でなめる時に、 lock の前に書いた列を読む
//!
//! どちらかが必ず今の値で置くので、 止まった後の置き場所 (`placed`) と置き場ごとの人数 (bucket の live) は今の列の値と
//! 一致する。 **lock の下で見直さないと壊れる**: key の書き手が lock なしで 「作っていない」 を読む → 作る側が書く前の
//! key の列をなめて印を立てる → 書き手は置かない → 古い帯のまま残る。 実測 (2026-10-10): 見直しを外すと、 key の書き手が
//! 居る 2 本 (`key_write_vs_build`、 `via_and_key_writers_vs_build`) が落ちる。 via の書き手だけの `via_write_vs_build` は
//! 落ちない — via の列は via の write_lock の下で書くので、 作る側がその前になめたなら印は書き手の lock より前に立って
//! いて、 後になめたなら新しい値を読む。 key の列は via の write_lock の外で書くので、 この順序が無い。
//!
//! 同じ entity の置き直しは行の lock で 1 本に並ぶ (記録を読んで、 古い置き場を −1、 新しい置き場を +1、 記録を書く)。
//! 行の lock を外すと、 同じ entity の 2 本の書き手が同じ記録を読んで古い置き場を 2 回 −1 する (実測 2026-10-10:
//! `via_and_key_writers_vs_build` が落ちる)。 engine の書き込みの道は全部、 行の lock の下で `live_set` / `live_remove` を呼ぶ。
//!
//! ## model の範囲
//! 列 (`AtomicUsize`、 0 = 値が無い、 それ以外は値 + 1)、 via / key の write_lock、 行の lock (entity ごと。 本物は eid の
//! 下位 bit の stripe)、 作った印、 置き場所の記録、 置き場ごとの人数を写す。 置き場の鍵は 1 本で写す (本物は値ごとの 64 本で、
//! 2 本取る時は添字の順。 違う値どうしの並びは単体 test が見る)。 bucket の中身 (足すだけの一覧と古い印) は
//! `loom_append_publish` と単体 test が、 epoch の解放は Miri が見ている。
//!
//! ## 帯を 2 本以上読む読み手 (seqlock)
//! 書き手は置き場の鍵の下で、 古い帯の件数を −1 して新しい帯に +1 する。 読み手は鍵を取らずに帯を順に読むので、 間に
//! 書き手が入ると 0 人や 2 人に見える。 鍵ごとの版を書き換えの前後で進め (奇数 = 最中)、 読み手は読む前と後の版が同じ
//! 偶数の時だけ採る (`OrderIndex::consistent`)。 `seqlock_reader_sees_one_move_whole` がそれを見る。 実測 (2026-10-10):
//! 読み手が版を見ないと落ちる。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_order_index_build --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};

/// 目盛り 30 の帯 (0 = 値が無い、 1 = [0, 30)、 2 = [30, ∞))。
const BANDS: usize = 3;
/// 置き場の数 (値 1..=3 × 帯 3 + 「居ない」 の 0)。
const SLOTS: usize = 3 * BANDS + 1;

fn band(k: usize) -> usize {
    match k {
        0 => 0,
        k if k - 1 < 30 => 1,
        _ => 2,
    }
}

/// via の値 + 1 と key の値 + 1 から置き場の番号 + 1 (0 = どこにも居ない)。 `OrderIndex::place` の `want` と同じ。
fn slot(v: usize, k: usize) -> usize {
    if v == 0 { 0 } else { (v - 1) * BANDS + band(k) + 1 }
}

struct Model {
    via: Vec<AtomicUsize>,
    key: Vec<AtomicUsize>,
    via_lock: Mutex<()>,
    key_lock: Mutex<()>,
    /// 行の lock (entity ごと)。
    row: Vec<Mutex<()>>,
    built: AtomicBool,
    /// 置き場の鍵。
    stripe: Mutex<()>,
    /// 置き場所の記録。 読むのは書き手 (行の lock の持ち主) か作る側、 書くのは置き場の鍵の下。
    placed: Vec<AtomicUsize>,
    /// 置き場ごとの人数 (bucket の live)。
    live: Vec<AtomicUsize>,
}

impl Model {
    fn new(init: &[(usize, usize)]) -> Self {
        Model {
            via: init.iter().map(|&(v, _)| AtomicUsize::new(v)).collect(),
            key: init.iter().map(|&(_, k)| AtomicUsize::new(k)).collect(),
            via_lock: Mutex::new(()),
            key_lock: Mutex::new(()),
            row: init.iter().map(|_| Mutex::new(())).collect(),
            built: AtomicBool::new(false),
            stripe: Mutex::new(()),
            placed: init.iter().map(|_| AtomicUsize::new(0)).collect(),
            live: (0..SLOTS).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    /// `OrderIndex::place`: 記録と今の列の値が違えば、 古い置き場を −1、 新しい置き場を +1、 記録を直す。
    fn place(&self, e: usize) {
        let want = slot(self.via[e].load(Ordering::Acquire), self.key[e].load(Ordering::Acquire));
        let cur = self.placed[e].load(Ordering::Relaxed);
        if cur == want {
            return;
        }
        let _s = self.stripe.lock().unwrap();
        if cur != 0 {
            let before = self.live[cur].fetch_sub(1, Ordering::Relaxed);
            assert!(before > 0, "置き場 {cur} の人数が 0 から減った (同じ entity を 2 回外した)");
        }
        if want != 0 {
            self.live[want].fetch_add(1, Ordering::Relaxed);
        }
        self.placed[e].store(want, Ordering::Relaxed);
    }

    /// `Engine::order_note_slow` (行の lock の下で呼ぶ)。
    fn note(&self, e: usize) {
        // ★契約: 作っていなければ via の write_lock の下で見直す。 見直しを外すと、 key の書き手が居る下の 2 本が落ちる。
        if !self.built.load(Ordering::Acquire) {
            let _w = self.via_lock.lock().unwrap();
            if !self.built.load(Ordering::Acquire) {
                return;
            }
        }
        self.place(e);
    }

    /// via の書き手 (`set_cell_local` → `live_set`): 行の lock の下で、 列は via の write_lock の下で書き (`HimoStore::set`)、
    /// 書いた後に note。
    fn write_via(&self, e: usize, v: usize) {
        let _row = self.row[e].lock().unwrap();
        {
            let _w = self.via_lock.lock().unwrap();
            self.via[e].store(v, Ordering::Release);
        }
        self.note(e);
    }

    /// key の書き手: 行の lock の下で、 列は key の write_lock の下で書き、 書いた後に note。
    fn write_key(&self, e: usize, k: usize) {
        let _row = self.row[e].lock().unwrap();
        {
            let _w = self.key_lock.lock().unwrap();
            self.key[e].store(k, Ordering::Release);
        }
        self.note(e);
    }

    /// `Engine::order_ready` (fast path + double-check)。 行の lock は取らない (本物も取らない)。
    fn ensure_ready(&self) {
        if self.built.load(Ordering::Acquire) {
            return;
        }
        let _w = self.via_lock.lock().unwrap();
        if self.built.load(Ordering::Acquire) {
            return;
        }
        for e in 0..self.via.len() {
            if self.via[e].load(Ordering::Acquire) != 0 {
                self.place(e);
            }
        }
        self.built.store(true, Ordering::Release);
    }

    /// 止まった後: 置き場所の記録と置き場ごとの人数が、 今の列の値とちょうど一致する。
    fn assert_matches_columns(&self) {
        assert!(self.built.load(Ordering::Acquire), "作り終えていない");
        let want: Vec<usize> = (0..self.via.len())
            .map(|e| slot(self.via[e].load(Ordering::Acquire), self.key[e].load(Ordering::Acquire)))
            .collect();
        let got: Vec<usize> = self.placed.iter().map(|p| p.load(Ordering::Relaxed)).collect();
        assert_eq!(got, want, "置き場所が今の値と違う (書き込みの見落とし)");
        let mut count = [0usize; SLOTS];
        for &s in want.iter().filter(|&&s| s != 0) {
            count[s] += 1;
        }
        let live: Vec<usize> = self.live.iter().map(|l| l.load(Ordering::Relaxed)).collect();
        assert_eq!(live[1..], count[1..], "置き場ごとの人数が今の値と違う");
    }
}

/// 年齢の書き手 1 本 + 作る側。 年齢が帯をまたいでも、 置き場所は今の帯になる。
#[test]
fn key_write_vs_build() {
    loom::model(|| {
        // 社員 0: 会社 1、 25 歳 (帯 1)
        let m = Arc::new(Model::new(&[(2, 26)]));
        let w = {
            let m = m.clone();
            loom::thread::spawn(move || m.write_key(0, 41))
        };
        let r = {
            let m = m.clone();
            loom::thread::spawn(move || m.ensure_ready())
        };
        w.join().unwrap();
        r.join().unwrap();
        m.assert_matches_columns();
    });
}

/// 会社の書き手 1 本 + 作る側。 会社に入った社員 (作る前は会社が無い) も置かれる。
#[test]
fn via_write_vs_build() {
    loom::model(|| {
        // 社員 0: 会社なし、 40 歳
        let m = Arc::new(Model::new(&[(0, 41)]));
        let w = {
            let m = m.clone();
            loom::thread::spawn(move || m.write_via(0, 2))
        };
        let r = {
            let m = m.clone();
            loom::thread::spawn(move || m.ensure_ready())
        };
        w.join().unwrap();
        r.join().unwrap();
        m.assert_matches_columns();
    });
}

/// 同じ社員に会社と年齢の書き手が 1 本ずつ + 作る側。 書き手どうしは行の lock で並び、 最後に置いた側が両方を見る。
#[test]
fn via_and_key_writers_vs_build() {
    loom::model(|| {
        // 社員 0: 会社 1、 40 歳 → 会社 2 に異動、 20 歳に
        let m = Arc::new(Model::new(&[(2, 41)]));
        let a = {
            let m = m.clone();
            loom::thread::spawn(move || m.write_via(0, 3))
        };
        let b = {
            let m = m.clone();
            loom::thread::spawn(move || m.write_key(0, 21))
        };
        let r = {
            let m = m.clone();
            loom::thread::spawn(move || m.ensure_ready())
        };
        a.join().unwrap();
        b.join().unwrap();
        r.join().unwrap();
        m.assert_matches_columns();
    });
}

/// 作り終えた後、 別の社員の書き手 2 本 (違う置き場の間の行き来。 置き場の鍵だけで並ぶ)。
#[test]
fn writers_on_two_entities_after_build() {
    loom::model(|| {
        // 社員 0: 会社 1・40 歳、 社員 1: 会社 2・20 歳
        let m = Arc::new(Model::new(&[(2, 41), (3, 21)]));
        m.ensure_ready();
        let a = {
            let m = m.clone();
            loom::thread::spawn(move || m.write_via(0, 3))
        };
        let b = {
            let m = m.clone();
            loom::thread::spawn(move || m.write_key(1, 41))
        };
        a.join().unwrap();
        b.join().unwrap();
        m.assert_matches_columns();
    });
}

/// 帯 2 本の件数と版 (`OrderIndex` の置き場の鍵の `seq` と bucket の `live`)。
struct Two {
    lock: Mutex<()>,
    seq: AtomicUsize,
    live: [AtomicUsize; 2],
}

impl Two {
    /// 書き手 (`OrderIndex::place` の帯の行き来): 鍵の下で版を奇数に → 古い帯 −1 → 新しい帯 +1 → 版を偶数に。
    fn move_band(&self, from: usize, to: usize) {
        let _g = self.lock.lock().unwrap();
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s + 1, Ordering::Relaxed);
        loom::sync::atomic::fence(Ordering::Release);
        self.live[from].fetch_sub(1, Ordering::Relaxed);
        self.live[to].fetch_add(1, Ordering::Relaxed);
        self.seq.store(s + 2, Ordering::Release);
    }

    /// 読み手 (`OrderIndex::consistent`): 版が前後で同じ偶数の時だけ採る。 揃わなければ 2 回まで読み直し、 だめなら None。
    fn read(&self) -> Option<usize> {
        for _ in 0..3 {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 == 1 {
                loom::thread::yield_now();
                continue;
            }
            let n = self.live[0].load(Ordering::Relaxed) + self.live[1].load(Ordering::Relaxed);
            loom::sync::atomic::fence(Ordering::Acquire);
            if self.seq.load(Ordering::Relaxed) == s1 {
                return Some(n);
            }
            loom::thread::yield_now();
        }
        None
    }
}

/// 1 人が帯 0 → 1 → 0 と動く間に読む: 採った件数はいつも 1 (0 人や 2 人に見えない)。
#[test]
fn seqlock_reader_sees_one_move_whole() {
    loom::model(|| {
        let t = Arc::new(Two { lock: Mutex::new(()), seq: AtomicUsize::new(0), live: [AtomicUsize::new(1), AtomicUsize::new(0)] });
        let w = {
            let t = t.clone();
            loom::thread::spawn(move || {
                t.move_band(0, 1);
                t.move_band(1, 0);
            })
        };
        if let Some(n) = t.read() {
            assert_eq!(n, 1, "帯の行き来の途中を読んだ");
        }
        w.join().unwrap();
        assert_eq!(t.read(), Some(1));
    });
}
