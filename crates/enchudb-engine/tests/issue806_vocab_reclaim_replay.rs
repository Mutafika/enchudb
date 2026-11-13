//! #806: 語の回収 (`vocab_reclaim`、 #381) を有効にした DB の、 落ちた後の WAL の再生。 自分の `Vocab` の record の語は、
//! 後ろの Tie が当たるまで使い回されないこと。
//!
//! 落ちた後に開くと参照数を列から数え直し、 参照 0 の場所は全部すぐ使える待ち行列に入る。 語 b の中身は disk に在り、 b を
//! 指す cell は届かずに落ちた時、 b は参照 0。 WAL が `Vocab b` (書き手 1) `Vocab c` (書き手 2) `Tie(Y = b)` の順なら、 前は
//! `Vocab c` の入れ直しが b の場所を取り (世代 +1)、 `Tie(Y = b)` の番号が古い世代になって書けなかった (`StaleValue`)。
//!
//! 本物の道: 書き手 2 本 (b を張る / 新しい語 c を張る) → oplog の fsync の後・本体の書き出しの前の電源断の像 → 開く。
//! 2 本の record の挟まり方は成り行きなので、 像の WAL と本体から前提 (最初の新しい語の Vocab が、 ある b の Vocab と Tie の
//! 間に挟まり、 その行の cell は disk に無い) を確かめ、 作れなかった回はやり直す。 検出率: 直しを外して 8/8 で落ちる
//! (前提は 1〜7 回目で作れた、 Mac の手元)。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, GrowableOptions, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::{DecodedOp, OpLog};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const OPLOG_CAP: usize = 16 * 1024 * 1024;
const N: usize = 1_000;
const ATTEMPTS: usize = 24;

struct Scratch(PathBuf);
impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu_issue806_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(p.join("live")).unwrap();
        Scratch(p)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        crashsim::set_after_copy_hook(None);
        crashsim::stop();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn b(i: usize) -> String {
    format!("b-word-{i:05}")
}
fn c(i: usize) -> String {
    format!("c-word-{i:05}")
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for ent in std::fs::read_dir(from).unwrap() {
        let ent = ent.unwrap();
        let dst = to.join(ent.file_name());
        if ent.file_type().unwrap().is_dir() {
            copy_dir(&ent.path(), &dst);
        } else {
            std::fs::copy(ent.path(), dst).unwrap();
        }
    }
}

/// 1 回分。 前提を作れなかったら None、 作れたら (前提に当たる行の数、 開いた後に値の違った行)。
fn once(attempt: usize) -> Option<(usize, Vec<String>)> {
    let s = Scratch::new(&format!("a{attempt}"));
    let (live, img) = (s.0.join("live"), s.0.join("img"));
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_growable_opts(
        db.to_str().unwrap(),
        GrowableOptions { max_entities: 100_000, vocab_reclaim: true, ..Default::default() },
    )
    .unwrap();
    eng.define_table("t", 50_000).unwrap();
    let k = eng.define_himo_in("t", "k", ValueType::Tag, 0).unwrap() as u16;
    let rows = |eng: &Engine, n: usize| (0..n).map(|_| eng.entity_in("t").unwrap()).collect::<Vec<u64>>();
    let (pb, pd, ys, zs) = (rows(&eng, N), rows(&eng, N), rows(&eng, N), rows(&eng, N));
    // 語 b (辞書のいちばん低い場所) と、 捨てる語 d を書いて、 d → b の順に外して書き出す: どちらも disk に在って参照 0。
    // 生きている session の待ち行列は外した順 (d が先) なので、 書き手 2 の新しい語は d の場所を取る (b には触らない)。
    // 落ちた後の数え直しの待ち行列は場所の順 = b が先
    for (i, &e) in pb.iter().enumerate() {
        eng.try_tie_text_to_by_id(e, k, &b(i)).unwrap();
    }
    for (i, &e) in pd.iter().enumerate() {
        eng.try_tie_text_to_by_id(e, k, &format!("d-word-{i:05}")).unwrap();
    }
    for &e in pd.iter().chain(&pb) {
        eng.untie(e, "t.k");
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    assert_eq!(eng.vocab_usage().reclaimable_entries as usize, 2 * N, "前提: b と d は参照 0");
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.oplog_sync().unwrap();

    // consumer は次の oplog の fsync の手前で止める (本体を書き出させない)。 "syncer" の oplog_sync が oplog を fsync して、
    // 本体の最初の segment を写した所 (書き出す前) で電源断の像を撮る
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let hold = Mutex::new(Some(release_rx));
    let shot = Mutex::new(true);
    let (live2, img2) = (live.clone(), img.clone());
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| match std::thread::current().name() {
        Some("enchudb-consumer") if what == Copied::Oplog => {
            let taken = hold.lock().unwrap().take();
            if let Some(release) = taken {
                let _ = release.recv();
            }
        }
        Some("syncer") if what == Copied::Segment && std::mem::take(&mut *shot.lock().unwrap()) => {
            crashsim::capture(&live2, &img2, Mode::Lost).unwrap();
        }
        _ => {}
    })));

    // 書き手 2 は、 書き手 1 が回り始めてから書き始める (最初の新しい語の Vocab が、 書き手 1 の Vocab と Tie の間に落ちやすい)
    let done1 = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let w1 = {
        let (eng, ys, done1) = (eng.clone(), ys.clone(), done1.clone());
        std::thread::spawn(move || {
            for (i, &y) in ys.iter().enumerate() {
                eng.try_tie_text_to_by_id(y, k, &b(i)).unwrap();
                done1.store(i + 1, std::sync::atomic::Ordering::Release);
            }
        })
    };
    let w2 = {
        let (eng, zs) = (eng.clone(), zs.clone());
        std::thread::spawn(move || {
            while done1.load(std::sync::atomic::Ordering::Acquire) < N / 10 {
                std::hint::spin_loop();
            }
            for (i, &z) in zs.iter().enumerate() {
                eng.try_tie_text_to_by_id(z, k, &c(i)).unwrap();
            }
        })
    };
    w1.join().unwrap();
    w2.join().unwrap();
    {
        let eng = eng.clone();
        std::thread::Builder::new().name("syncer".into()).spawn(move || eng.oplog_sync().unwrap()).unwrap().join().unwrap();
    }
    crashsim::set_after_copy_hook(None);
    let _ = release_tx.send(());
    crashsim::stop();
    drop(eng);
    assert!(img.exists(), "前提: 像を撮れた");

    // 前提を像から確かめる (像の写しを standalone で開く = 再生しない)
    let recs = OpLog::open(&img.join("db").join("oplog")).unwrap().records_with_tail();
    let probe_dir = s.0.join("probe");
    copy_dir(&img, &probe_dir);
    let probe = Engine::open_standalone(probe_dir.join("db").to_str().unwrap()).unwrap();
    let on_disk = |w: &str| probe.vocab_id(w).is_some();
    // 像の WAL の順に見る。 前提に当たるのは、 **最初の** disk に無い語の Vocab が来た時に 「Vocab は過ぎて Tie がまだ」 の
    // b の行 (その b の場所が、 数え直しの後の待ち行列のいちばん前 = 入れ直しが取る場所。 最初の Vocab が b の Vocab と Tie の
    // 間に挟まらなかった回は、 入れ直しが次の b の場所を先に取り、 以後の b は入れ直されて番号を引き直されるので起きない)
    let mut open_b: std::collections::BTreeMap<u32, usize> = Default::default(); // record の番号 → 行
    let mut hit: Option<Vec<usize>> = None;
    let mut tied: Vec<bool> = vec![false; N];
    for r in &recs {
        match &r.op {
            DecodedOp::Vocab { vid, bytes } => {
                let w = String::from_utf8_lossy(bytes).into_owned();
                if let Some(i) = w.strip_prefix("b-word-").and_then(|x| x.parse::<usize>().ok()) {
                    open_b.insert(*vid, i);
                } else if hit.is_none() && !on_disk(&w) {
                    hit = Some(open_b.values().copied().collect());
                }
            }
            DecodedOp::Tie { eid, value, .. } => {
                if let Some(i) = open_b.remove(&(*value as u32)) {
                    assert_eq!(*eid & 0xffff_ffff, ys[i] & 0xffff_ffff, "b の Tie の行");
                    tied[i] = true;
                }
            }
            _ => {}
        }
    }
    // 前提に当たる行: Tie が WAL に在り、 cell は disk に無く、 語 b は disk に在る
    let hit: Vec<usize> =
        hit.unwrap_or_default().into_iter().filter(|&i| tied[i] && probe.get(ys[i], "t.k").is_none() && on_disk(&b(i))).collect();
    drop(probe);
    if hit.is_empty() {
        return None;
    }

    let back = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    let text = |e: u64| back.get_text_owned(e, "t.k").map(|v| String::from_utf8_lossy(&v).into_owned());
    let mut wrong = Vec::new();
    for i in (0..N).filter(|&i| tied[i]) {
        if text(ys[i]).as_deref() != Some(&b(i)) {
            wrong.push(format!("Y{i}: {:?} (b-word-{i:05} のはず)", text(ys[i])));
        }
    }
    for (i, &z) in zs.iter().enumerate() {
        // c の Tie が WAL に在るかは見ない: 在る行だけ値が在る。 在る値は c(i) であること
        if let Some(got) = text(z).filter(|got| *got != c(i)) {
            wrong.push(format!("Z{i}: {got:?}"));
        }
    }
    drop(back);
    Some((hit.len(), wrong))
}

#[test]
fn replay_after_power_loss_keeps_every_tag_write_of_two_writers() {
    for attempt in 0..ATTEMPTS {
        let Some((hits, wrong)) = once(attempt) else { continue };
        assert!(
            wrong.is_empty(),
            "WAL に載った Tag の書き込みが、 落ちた後の再生で消えた / 違う値になった (前提に当たる行 {hits}、 違った行 {}): {:?}",
            wrong.len(),
            &wrong[..wrong.len().min(5)]
        );
        return;
    }
    panic!("前提を作れなかった ({ATTEMPTS} 回とも、 最初の新しい語の Vocab が b の Vocab と Tie の間に挟まらなかった)");
}
