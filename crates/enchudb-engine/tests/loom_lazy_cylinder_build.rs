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
//! ## model の範囲
//! `HimoStore` の flag (`AtomicBool`) / `write_lock` (`Mutex`) / Column cell の
//! Release store と、 `ensure_cylinder_built` の double-check build を写す。 index 自体は
//! 「write_lock 下でしか変更されない集合」として `Mutex<Vec<_>>` で置く — 実 index
//! (`LockFreeCylinder`) の lock-free publish ordering は `loom_append_publish` が、
//! epoch 解放は Miri が別途見ているので、 ここでは **build と write の見落とし** だけに絞る。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_lazy_cylinder_build --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};

/// `HimoStore` の lazy build 契約の de-epoch model。
struct Store {
    /// Column の cell。 0 = 未設定、 それ以外は `value + 1` (実装と同じ +1 encode)。
    col: Vec<AtomicUsize>,
    /// 値→eid 逆索引。 実 cylinder と違い lock-free ではない (上記「model の範囲」)。
    cyl: Mutex<Vec<(usize, usize)>>,
    /// 組み済み = 以後 writer が維持する。
    built: AtomicBool,
    /// writer 直列化 lock。 build も取る。
    write_lock: Mutex<()>,
}

impl Store {
    fn new(cells: usize) -> Self {
        Self {
            col: (0..cells).map(|_| AtomicUsize::new(0)).collect(),
            cyl: Mutex::new(Vec::new()),
            built: AtomicBool::new(false),
            write_lock: Mutex::new(()),
        }
    }

    /// `HimoStore::set` の model。
    fn set(&self, eid: usize, value: usize) {
        let _w = self.write_lock.lock().unwrap();
        // ★契約: 判定は lock を取った **後**。 ここを `_w` の前に出すと下の test が落ちる。
        let live = self.built.load(Ordering::Acquire);
        self.col[eid].store(value + 1, Ordering::Release);
        if live {
            self.cyl.lock().unwrap().push((eid, value));
        }
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

/// 1 writer + 1 reader。 build と write のどの interleaving でも、 write は index に
/// ちょうど 1 件入る (writer が入れるか、 build の scan が拾うか、 必ずどちらか一方)。
#[test]
fn lost_write_single_writer() {
    loom::model(|| {
        let s = Arc::new(Store::new(1));
        let w = {
            let s = s.clone();
            loom::thread::spawn(move || s.set(0, 7))
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

/// 2 writer (別 eid) + 1 reader。 build が片方の write だけ拾って片方を落とす、
/// という非対称な取りこぼしも許さない。
#[test]
fn lost_write_two_writers() {
    loom::model(|| {
        let s = Arc::new(Store::new(2));
        let w0 = {
            let s = s.clone();
            loom::thread::spawn(move || s.set(0, 7))
        };
        let w1 = {
            let s = s.clone();
            loom::thread::spawn(move || s.set(1, 7))
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
