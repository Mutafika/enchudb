//! loom model — `HimoStore` の **lazy cylinder build と並行 write** の契約を全 interleaving
//! で検証する。#270。
//!
//! ## 何を守っているのか
//! #270 で writer は「cylinder が未 build なら触らない」ようになった。 組むのは読み手が
//! 最初に引いた時 (`ensure_cylinder_built` が Column を scan)。 この 2 つが並走するので、
//! **build が「まだ Column に無い write」を見落とし、 かつ write が「もう組まれた index」に
//! 入れ損なう** 窓があると、 その eid は `pull` から永久に消える (silent lost row)。
//!
//! 契約は「writer は `cyl_built` を **`write_lock` を取った後** に読む」の 1 行:
//!
//!   - `Some` を見た = build は完了済み → writer が自分で index に入れる
//!   - `None` を見た = build はまだ lock を取れていない → その後の scan が此の write を拾う
//!
//! どちらかが必ず index に入れるので取りこぼしが無い。 **判定を lock の前に出すと壊れる**:
//! writer が false を読む → reader が lock を取り write 前の Column を scan → flag を立てる
//! → writer が lock を取り Column に書くが live=false なので入れない → index から消える。
//! この test はその interleaving を検出する — `set()` の load を lock の前に出すと下の
//! 2 本とも落ちる (実測: single = index `[]` に対し期待 `[(0, 7)]`、 two = `[(0, 7)]` に
//! 対し期待 `[(0, 7), (1, 7)]` の非対称な取りこぼし)。 実コードの統合テストはこの窓を
//! 踏めない (同じ hoist を実コードに入れても issue95_lockfree_read 5 run /
//! issue95_stress / engine_model_proptest / issue119_retie_order_all_paths /
//! issue255_rw_open_lazy_cylinder が全 pass した) ので、 **これが唯一の gate**。
//!
//! ## 門 (2026-10-11、 `write_gate`)
//! 組む前の writer は `write_lock` を取らずに門を通って Column だけを書く: shard の数を足し、
//! 閉じていなければ書いて数を引く。 閉じていれば数を戻して上の lock の道へ。 組む側は lock を
//! 取った後で全 shard を閉じ、 数が 0 になるのを待ってから scan する (以後は開けない)。 足す
//! と閉じるは同じ word への RMW なので、 どちらかが先になる:
//!
//!   - writer の足すが先 → 組む側はその writer が書いて出るのを待つ → scan が拾う
//!   - 閉じるが先 → writer は閉じた印を見て lock の道へ → 上の契約
//!
//! **組む側が待たないと壊れる** (`build_that_does_not_wait_for_the_gate_loses_a_write`)。
//! **writer が数を足す前に閉じた印を見ると壊れる** (`writer_that_looks_before_counting_loses_a_write`:
//! 印を見てから足すまでの間に、 組む側が閉じて数 0 を見て scan する)。 どちらも下の
//! `#[should_panic]` の test が loom で窓を見つける = この model が窓を表現できていることの確認。
//!
//! ## 値の鍵 (2026-10-11、 `value_locks`)
//! 組んだ後の writer は `write_lock` を取らずに値ごとの鍵の下で Column と index を書く (`HimoStore::set_indexed`)。
//! writer は最初に flag を Acquire で見て、 立っていれば鍵の道へ。 立っていなければ門か lock の道 (上と同じ) で、 lock を
//! 取った後に立っているのを見たら lock を離して鍵の道へ。 組む側は flag を立てる前に鍵を作る:
//!
//!   - flag を見て鍵の道へ行った writer は build の後 (scan の後) に書く → 自分で index に入れる
//!   - lock の道で flag を見直さないと壊れる (`writer_that_keeps_the_flag_it_saw_before_the_lock_loses_a_write`:
//!     lock の前に見た 「組んでいない」 のまま、 組み終わった後に Column だけを書く = 上の hoist と同じ取りこぼし)
//!
//! ## model の範囲
//! `HimoStore` の flag (`AtomicBool`) / `write_lock` (`Mutex`) / 門の shard (2 本、 writer
//! ごとに別) / Column cell の Release store と、 `ensure_cylinder_built` の double-check build
//! を写す。 index 自体は 「write_lock 下でしか変更されない集合」 として `Mutex<Vec<_>>` で
//! 置く — 実 index (`LockFreeCylinder`) の lock-free publish ordering は `loom_append_publish`
//! が、 epoch 解放は Miri が別途見ているので、 ここでは **build と write の見落とし** だけに絞る。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_lazy_cylinder_build --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};

/// 門の shard の最上位 bit = 閉じている (`write_gate::CLOSED` と同じ形)。
const CLOSED: usize = 1 << 31;

/// 壊した形 (下の `#[should_panic]` の test 用)。
#[derive(Clone, Copy)]
struct Broken {
    /// 組む側が門を閉じた後、 中の writer が出るのを待たない。
    no_wait: bool,
    /// writer が数を足す前に、 閉じた印を load で見る。
    look_first: bool,
    /// lock の道で flag を見直さない (lock の前に見た値のまま決める)。
    stale_flag: bool,
}

const SOUND: Broken = Broken { no_wait: false, look_first: false, stale_flag: false };

/// `HimoStore` の lazy build 契約の de-epoch model。
struct Store {
    /// Column の cell。 0 = 未設定、 それ以外は `value + 1` (実装と同じ +1 encode)。
    col: Vec<AtomicUsize>,
    /// 値→eid 逆索引。 実 cylinder と違い lock-free ではない (上記「model の範囲」)。
    cyl: Mutex<Vec<(usize, usize)>>,
    /// 組み済み = 以後 writer が維持する。
    built: AtomicBool,
    /// writer 直列化 lock。 build も取る。 組んだ後の writer は取らない。
    write_lock: Mutex<()>,
    /// 組んだ後の writer の値の鍵 (`ValueLocks`、 値 1 つなので 1 本)。
    value_lock: Mutex<()>,
    /// 門の shard (`WriteGate`)。 閉じた印 | 中に居る writer の数。
    gate: [AtomicUsize; 2],
    broken: Broken,
}

impl Store {
    fn new(cells: usize, broken: Broken) -> Self {
        Self {
            col: (0..cells).map(|_| AtomicUsize::new(0)).collect(),
            cyl: Mutex::new(Vec::new()),
            built: AtomicBool::new(false),
            write_lock: Mutex::new(()),
            value_lock: Mutex::new(()),
            gate: [AtomicUsize::new(0), AtomicUsize::new(0)],
            broken,
        }
    }

    /// `WriteGate::try_enter` + 門の中の書き込み + `Pass` の drop。 入れなかったら false。
    fn write_in_gate(&self, shard: usize, eid: usize, value: usize) -> bool {
        let s = &self.gate[shard];
        if self.broken.look_first {
            if s.load(Ordering::Acquire) & CLOSED != 0 {
                return false;
            }
            s.fetch_add(1, Ordering::Acquire);
        } else if s.fetch_add(1, Ordering::Acquire) & CLOSED != 0 {
            s.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        self.col[eid].store(value + 1, Ordering::Release);
        s.fetch_sub(1, Ordering::Release);
        true
    }

    /// `HimoStore::set` の model。 `shard` = writer の thread が使う門の shard。
    fn set(&self, shard: usize, eid: usize, value: usize) {
        // 組んだ後は値の鍵の道 (flag は組む側が scan の後に Release で立てる)
        let seen = self.built.load(Ordering::Acquire);
        if seen {
            return self.set_indexed(eid, value);
        }
        // 組んでいなければ門の中で Column だけを書く (閉じていれば lock の道)
        if self.write_in_gate(shard, eid, value) {
            return;
        }
        let w = self.write_lock.lock().unwrap();
        // ★契約: 判定は lock を取った **後**。 lock の前に見た値で決めると下の test が落ちる (`stale_flag`)。
        let live = if self.broken.stale_flag { seen } else { self.built.load(Ordering::Acquire) };
        if live {
            drop(w);
            return self.set_indexed(eid, value);
        }
        self.col[eid].store(value + 1, Ordering::Release);
    }

    /// `HimoStore::set_indexed`: 値の鍵の下で Column と index を書く (write_lock は取らない)。
    fn set_indexed(&self, eid: usize, value: usize) {
        let _k = self.value_lock.lock().unwrap();
        self.col[eid].store(value + 1, Ordering::Release);
        self.cyl.lock().unwrap().push((eid, value));
    }

    /// `HimoStore::ensure_cylinder_built` の model (fast path + double-check)。
    fn ensure_built(&self) {
        if self.built.load(Ordering::Acquire) {
            return;
        }
        let _g = self.write_lock.lock().unwrap();
        if self.built.load(Ordering::Acquire) {
            return;
        }
        // `WriteGate::close`: 全 shard に閉じた印を立ててから、 中の writer が出るのを待つ
        for s in &self.gate {
            s.fetch_or(CLOSED, Ordering::AcqRel);
        }
        if !self.broken.no_wait {
            for s in &self.gate {
                while s.load(Ordering::Acquire) & !CLOSED != 0 {
                    loom::thread::yield_now();
                }
            }
        }
        let mut cyl = self.cyl.lock().unwrap();
        for (eid, cell) in self.col.iter().enumerate() {
            let stored = cell.load(Ordering::Acquire);
            if stored != 0 {
                cyl.push((eid, stored - 1));
            }
        }
        drop(cyl);
        self.built.store(true, Ordering::Release);
    }

    /// quiesce 後の検証: Column に入っている write が index に **ちょうど 1 件ずつ** ある。
    fn assert_index_matches_column(&self) {
        assert!(self.built.load(Ordering::Acquire), "build が完了していない");
        let mut want: Vec<(usize, usize)> = self
            .col
            .iter()
            .enumerate()
            .filter_map(|(eid, c)| match c.load(Ordering::Acquire) {
                0 => None,
                stored => Some((eid, stored - 1)),
            })
            .collect();
        let mut got = self.cyl.lock().unwrap().clone();
        want.sort_unstable();
        got.sort_unstable();
        assert_eq!(got, want, "index が write を取りこぼした / 重複させた (#270)");
    }
}

fn single_writer(broken: Broken) {
    loom::model(move || {
        let s = Arc::new(Store::new(1, broken));
        let w = {
            let s = s.clone();
            loom::thread::spawn(move || s.set(0, 0, 7))
        };
        let r = {
            let s = s.clone();
            loom::thread::spawn(move || s.ensure_built())
        };
        w.join().unwrap();
        r.join().unwrap();
        assert_eq!(s.col[0].load(Ordering::Acquire), 8, "Column への write は必ず残る");
        s.assert_index_matches_column();
    });
}

/// 1 writer + 1 reader。 build と write のどの interleaving でも、 write は index に
/// ちょうど 1 件入る (writer が入れるか、 build の scan が拾うか、 必ずどちらか一方)。
#[test]
fn lost_write_single_writer() {
    single_writer(SOUND);
}

/// 2 writer (別 eid、 別の門の shard) + 1 reader。 build が片方の write だけ拾って片方を
/// 落とす、 という非対称な取りこぼしも許さない。
#[test]
fn lost_write_two_writers() {
    loom::model(|| {
        let s = Arc::new(Store::new(2, SOUND));
        let w0 = {
            let s = s.clone();
            loom::thread::spawn(move || s.set(0, 0, 7))
        };
        let w1 = {
            let s = s.clone();
            loom::thread::spawn(move || s.set(1, 1, 7))
        };
        let r = {
            let s = s.clone();
            loom::thread::spawn(move || s.ensure_built())
        };
        w0.join().unwrap();
        w1.join().unwrap();
        r.join().unwrap();
        s.assert_index_matches_column();
    });
}

/// 1 writer が組む前 (門) と組んだ後 (値の鍵) をまたいで 2 回書く (別 eid)。 1 回目が門、 2 回目が
/// lock の道から鍵の道になる interleaving を含む。
#[test]
fn writer_crossing_the_build() {
    loom::model(|| {
        let s = Arc::new(Store::new(2, SOUND));
        let w = {
            let s = s.clone();
            loom::thread::spawn(move || {
                s.set(0, 0, 7);
                s.set(0, 1, 9);
            })
        };
        s.ensure_built();
        w.join().unwrap();
        s.assert_index_matches_column();
    });
}

/// 対照: 組む側が門を閉じても中の writer を待たないと、 書く前の Column を scan して取りこぼす。
#[test]
#[should_panic(expected = "取りこぼした")]
fn build_that_does_not_wait_for_the_gate_loses_a_write() {
    single_writer(Broken { no_wait: true, ..SOUND });
}

/// 対照: writer が数を足す前に閉じた印を見ると、 その間に組む側が閉じて数 0 を見て scan する。
#[test]
#[should_panic(expected = "取りこぼした")]
fn writer_that_looks_before_counting_loses_a_write() {
    single_writer(Broken { look_first: true, ..SOUND });
}

/// 対照: lock の道で flag を見直さないと、 lock を待つ間に組み終わった索引に入れずに Column だけを書く。
#[test]
#[should_panic(expected = "取りこぼした")]
fn writer_that_keeps_the_flag_it_saw_before_the_lock_loses_a_write() {
    single_writer(Broken { stale_flag: true, ..SOUND });
}
