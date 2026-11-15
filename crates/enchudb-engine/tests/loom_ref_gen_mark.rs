//! loom model — #899: Ref の紐の円柱の鍵を場所にした後の、 「世代 > 0 の entry を置いた」 印 (`HimoStore::has_gen`) と、 それを
//! 見る読み手の順の約束を全 interleaving で検証する。
//!
//! ## 何を守っているのか
//! 世代を持つ DB の Ref の紐は、 円柱の鍵 = 場所、 entry = 値の世代 << S | entity。 印が立っていない紐の entry は全部世代 0
//! (= entity そのもの) なので、 読み手は世代を外す手間を省ける。 印は 1 度だけ立つ:
//!
//! - 書き手 (世代 > 0 の entry を置く) は、 印を Acquire で読み、 立っていなければ Release で立ててから、 値の鍵の下で entry を
//!   bucket に置き (len を Release で)、 bucket の live を Release で +1 する
//! - 値を引く読み (`pull`) は bucket を写した (len を Acquire で読んだ) **後** で印を読む
//! - 数だけの読み (`slice_len`) は bucket の live を Acquire で読んだ **後** で印を読む
//!
//! 読み手が写した entry / 読んだ live を置いた書き手が、 その前に印を立てた (か、 立っているのを Acquire で見た) なら、 読み手は印を
//! 見る。 live は同じ bucket の書き手が値の鍵で並んで書くので、 後の書き手 (印に触らない世代 0 の書き手) の live を読んでも、 鍵の
//! 受け渡しで前の書き手の印まで届く。 印を見なかった読み手は、 世代 > 0 の entry を entity として返す (印の無い紐の速い道) /
//! 世代 > 0 の entity を世代 0 の値の数に数える。
//!
//! ## model の範囲
//! bucket 1 つ (len と live)、 値の鍵 1 本 (Mutex)、 印 1 つ。 書き手 A は世代 1 の entry を置き、 書き手 B は (A の後でも前でも)
//! 世代 0 の entry を置く。 読み手 (別の thread) は live / len を読んでから印を読む。 対照: live の store / load を Relaxed にすると、
//! B の live を読んだ読み手が印を見ない interleaving を loom が見つける (`relaxed_live_misses_the_mark`)。 読み手が 2 件とも数えた
//! interleaving に届いたことも数えて確かめる (読み手を main に置いた最初の形は、 45 の interleaving の全部で読み手が 0 を読み、
//! 約束の形も対照も何も確かめずに通っていた)。
//!
//! ## 場所の印 (PR #907 の読み直し D)
//! 印は紐に 1 つ (`has_gen`) と、 場所ごと (`GenSlots`、 世代 > 0 の entry を置いた場所の bit)。 数の読みは、 世代 0 の値で場所の印が
//! 立っていなければ live をそのまま使う。 書き手は場所の印 → 紐の印の順に立ててから置き、 読み手は live / 写しの後で紐の印 → 場所の印
//! を読む。 model の `slot_mark` がそれ (読み手は両方を見る)。
//!
//! ## 場所の bit の切り替わり (PR #907 の読み直し B、 `bits_switch_*`)
//! 読み手は場所の bit (0 = 値を鍵に) で鍵を決めて bucket を写す。 写す前に DB が世代を持ち (bit 0 → S)、 書き手がその bit を見て世代 > 0
//! の entry を同じ bucket (世代 0 の値 = 場所の bucket) に置くと、 移る前の bit のままの読み手はその entry を entity と読む。 読み手は
//! 写した **後** で bit を読み直し、 違えば新しい bit で写し直す (`HimoStore::with_ref_bits`)。 写した entry を置いた書き手は S を
//! 読んでいる (書き手の bit の読み → bucket に置く (Release) → 読み手が写す (Acquire) → 読み手の bit の読み、 で読み読みの整合)
//! ので、 写した後の読みが 0 なら写した entry は全部世代 0。 対照: 読み直さない読み手は窓を踏む (`bits_switch_without_reread_misses`)。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_ref_gen_mark --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};

struct Model {
    has_gen: AtomicBool,
    /// 場所の印 (`GenSlots` の bit)
    slot_mark: AtomicBool,
    /// bucket の publish 済みの長さ (`Buf::len`) と live (`AppendBucket::live`)
    len: AtomicUsize,
    live: AtomicUsize,
    /// 置いた entry の世代 (len の手前まで publish 済み)
    gens: [AtomicUsize; 2],
    /// 値の鍵
    lock: Mutex<()>,
    live_order: (Ordering, Ordering),
}

impl Model {
    fn push(&self, g: usize) {
        if g != 0 {
            // 場所の印 → 紐の印 (`HimoStore::note_gen_entry`)
            if !self.slot_mark.load(Ordering::Acquire) {
                self.slot_mark.store(true, Ordering::Release);
            }
            if !self.has_gen.load(Ordering::Acquire) {
                self.has_gen.store(true, Ordering::Release);
            }
        }
        let _g = self.lock.lock().unwrap();
        let l = self.len.load(Ordering::Relaxed);
        self.gens[l].store(g, Ordering::Relaxed);
        self.len.store(l + 1, Ordering::Release);
        let v = self.live.load(Ordering::Relaxed);
        self.live.store(v + 1, self.live_order.0);
    }
}

fn run(live_order: (Ordering, Ordering)) {
    // 読み手が 2 件とも数えた interleaving を見た回数 (model が窓に届いている確認。 std の atomic = loom の外)
    let both = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = both.clone();
    loom::model(move || {
        let m = Arc::new(Model {
            has_gen: AtomicBool::new(false),
            slot_mark: AtomicBool::new(false),
            len: AtomicUsize::new(0),
            live: AtomicUsize::new(0),
            gens: [AtomicUsize::new(0), AtomicUsize::new(0)],
            lock: Mutex::new(()),
            live_order,
        });
        // 読み手は別の thread (main は書き手 B)。 数の読み: live を読んでから印。 2 件とも数えたなら、 世代 1 の entry も数えている。
        // 値を引く読み: 写した entry に世代 1 があれば印を見る
        let r = {
            let m = m.clone();
            loom::thread::spawn(move || {
                let n = m.live.load(live_order.1);
                // 紐の印 → 場所の印 (`HimoStore::slot_has_gen`)
                let mark = m.has_gen.load(Ordering::Acquire) && m.slot_mark.load(Ordering::Acquire);
                if n == 2 {
                    assert!(mark, "live を読んだのに世代 > 0 の印を見ない (数の読みが世代 1 の entity を世代 0 に数える)");
                }
                let l = m.len.load(Ordering::Acquire);
                let copied: Vec<usize> = (0..l).map(|i| m.gens[i].load(Ordering::Relaxed)).collect();
                let mark = m.has_gen.load(Ordering::Acquire);
                if copied.contains(&1) {
                    assert!(mark, "世代 1 の entry を写したのに印を見ない (entity として返す)");
                }
                n
            })
        };
        let a = {
            let m = m.clone();
            loom::thread::spawn(move || m.push(1))
        };
        m.push(0);
        a.join().unwrap();
        let n = r.join().unwrap();
        if n == 2 {
            seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    });
    assert!(both.load(std::sync::atomic::Ordering::Relaxed) > 0, "前提: 読み手が 2 件とも数えた interleaving に届いていない");
}

/// 約束の形 (live の store は Release、 数の読みは Acquire) = 全 interleaving で印を見る。
#[test]
fn reads_after_the_bucket_see_the_mark() {
    run((Ordering::Release, Ordering::Acquire));
}

/// 対照: live を Relaxed で書いて読むと、 世代 0 の書き手 B の live (2) を読んだ読み手が A の印を見ない interleaving がある
/// (model が窓を表せていることの確認)。
#[test]
#[should_panic(expected = "印を見ない")]
fn relaxed_live_misses_the_mark() {
    run((Ordering::Relaxed, Ordering::Relaxed));
}

/// 場所の bit の切り替わりの model: bucket 1 つ (世代 0 の値の鍵 = 場所の bucket)、 bit (0 → 1)。 移る側 (`Generations::start`) は
/// bit を Release で立てる。 書き手は bit を Acquire で読み、 立っていれば世代 1 の entry、 立っていなければ世代 0 の entry を値の鍵の下で
/// 置く。 読み手は bit を読んで写し、 `reread` なら写した後で読み直して違えば写し直す。 最後に使った bit が 0 なら、 写した entry に
/// 世代 1 は無いこと。
fn bits_switch(reread: bool) {
    let window = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = window.clone();
    loom::model(move || {
        let bits = Arc::new(AtomicUsize::new(0));
        let len = Arc::new(AtomicUsize::new(0));
        let gens = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let lock = Arc::new(Mutex::new(()));
        let mover = {
            let bits = bits.clone();
            loom::thread::spawn(move || bits.store(1, Ordering::Release))
        };
        let writer = {
            let (bits, len, gens, lock) = (bits.clone(), len.clone(), gens.clone(), lock.clone());
            loom::thread::spawn(move || {
                let g = usize::from(bits.load(Ordering::Acquire) != 0);
                let _l = lock.lock().unwrap();
                let l = len.load(Ordering::Relaxed);
                gens[l].store(g, Ordering::Relaxed);
                len.store(l + 1, Ordering::Release);
            })
        };
        let copy = || -> Vec<usize> { (0..len.load(Ordering::Acquire)).map(|i| gens[i].load(Ordering::Relaxed)).collect() };
        let mut b = bits.load(Ordering::Acquire);
        let mut copied = copy();
        let first = (b, copied.clone());
        if reread {
            let after = bits.load(Ordering::Acquire);
            if after != b {
                b = after;
                copied = copy();
            }
        }
        if first.0 == 0 && first.1.contains(&1) {
            seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if b == 0 {
            assert!(!copied.contains(&1), "bit 0 のまま世代 1 の entry を写した (entry を entity と読む)");
        }
        mover.join().unwrap();
        writer.join().unwrap();
    });
    assert!(window.load(std::sync::atomic::Ordering::Relaxed) > 0, "前提: bit 0 で鍵を決めた後に世代 1 の entry を写す interleaving に届いていない");
}

/// 約束の形: 写した後で bit を読み直す読み手は、 bit 0 のまま世代 1 の entry を使わない。
#[test]
fn bits_switch_reread_after_copy() {
    bits_switch(true);
}

/// 対照: 読み直さない読み手は、 bit 0 で鍵を決めた後に置かれた世代 1 の entry を entity と読む interleaving がある。
#[test]
#[should_panic(expected = "entry を entity と読む")]
fn bits_switch_without_reread_misses() {
    bits_switch(false);
}
