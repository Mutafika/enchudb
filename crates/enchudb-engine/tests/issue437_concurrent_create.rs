//! #437: 同じ path への create が同時に走っても、 勝つのはちょうど 1 本で、 負けた側は `AlreadyExists` を返して
//! 勝った側の directory に触らない。
//!
//! 旧 (#415): create は作成中の印を置いて fsync してから writer lock を取り、 残骸 (create が作り直してよい
//! directory) に 「印がある」 「lock 以外に何も無い」 を足していた。 lock を取るまでの間の directory を同時の create
//! が残骸と見て消して作り直し、 消された側がその directory の lock を先に取って作り終えると、 lock を待っていた側が
//! 後から勝った側の DB を消した (`SegmentSet::create` の `AlreadyExists` の片付け)。 旧実装での実測 (release、
//! M4 Max、 4 本同時): 別 process は勝者が書いて閉じた DB が 100 回中 50 / 33 回消え、 thread は 1000 回中 930 / 940 回
//! 4 本とも失敗した。
//!
//! 残骸の判定は、 中身が全部 create の置く名前の directory に絞った (旧: 印があれば中身を見ずに directory ごと消した)。

use enchudb_engine::Engine;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CHILD: &str = "ENCHU_ISSUE437_CHILD";

fn tmp(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("issue437_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn now_ns() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

/// 勝者の数と、 勝者が書いて閉じた値を全員の終了後に読めなかった回数。
#[derive(Default, Debug, PartialEq)]
struct Tally {
    no_winner: usize,
    many_winners: usize,
    lost: usize,
}

/// 勝った 1 本 (たち) が書いた値を、 開き直して読めるか。
fn check_winners(path: &str, winners: &[u64], tally: &mut Tally) {
    match winners.len() {
        0 => tally.no_winner += 1,
        1 => {}
        _ => tally.many_winners += 1,
    }
    if winners.is_empty() {
        return;
    }
    match Engine::open_standalone(path) {
        Ok(db) if winners.iter().any(|&e| db.get(e, "x") == Some(7)) => {}
        _ => tally.lost += 1,
    }
}

#[test]
fn concurrent_creates_in_threads_leave_exactly_one_db() {
    let base = tmp("threads");
    std::fs::create_dir_all(&base).unwrap();
    let mut tally = Tally::default();
    for round in 0..100 {
        let path = base.join(format!("db{round}")).to_str().unwrap().to_string();
        let start = Arc::new(Barrier::new(4));
        let racers: Vec<_> = (0..4)
            .map(|_| {
                let (start, path) = (start.clone(), path.clone());
                std::thread::spawn(move || {
                    start.wait();
                    Engine::create_growable_tiny(&path)
                })
            })
            .collect();
        let mut winners = Vec::new();
        for r in racers {
            match r.join().unwrap() {
                Ok(mut eng) => {
                    let e = eng.entity().unwrap();
                    eng.tie(e, "x", 7u32);
                    eng.flush().unwrap();
                    winners.push(e);
                }
                Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists, "負けた create: {err}"),
            }
        }
        check_winners(&path, &winners, &mut tally);
    }
    let _ = std::fs::remove_dir_all(&base);
    assert_eq!(tally, Tally::default());
}

/// 子: 決めた時刻に create し、 勝ったら 1 件書いて flush し、 少し開いたまま居てから閉じる。
#[test]
fn issue437_child() {
    let Ok(spec) = std::env::var(CHILD) else { return };
    let (path, at) = spec.split_once('|').unwrap();
    let at: u128 = at.parse().unwrap();
    while now_ns() < at {
        std::hint::spin_loop();
    }
    match Engine::create_growable_tiny(path) {
        Ok(mut eng) => {
            let e = eng.entity().unwrap();
            eng.tie(e, "x", 7u32);
            eng.flush().unwrap();
            std::thread::sleep(Duration::from_millis(50));
            drop(eng);
            println!("ISSUE437 won {e}");
        }
        Err(err) => println!("ISSUE437 lost {:?}", err.kind()),
    }
}

#[test]
fn concurrent_creates_in_processes_never_remove_the_winners_db() {
    let base = tmp("processes");
    std::fs::create_dir_all(&base).unwrap();
    let mut tally = Tally::default();
    for round in 0..30 {
        let path = base.join(format!("db{round}")).to_str().unwrap().to_string();
        let at = now_ns() + 100_000_000;
        let racers: Vec<_> = (0..4)
            .map(|_| {
                Command::new(std::env::current_exe().unwrap())
                    .args(["issue437_child", "--exact", "--nocapture", "--test-threads=1"])
                    .env(CHILD, format!("{path}|{at}"))
                    .stdout(std::process::Stdio::piped())
                    .spawn()
                    .unwrap()
            })
            .collect();
        let mut winners = Vec::new();
        for r in racers {
            let out = String::from_utf8_lossy(&r.wait_with_output().unwrap().stdout).into_owned();
            let said = out.split("ISSUE437 ").nth(1).expect("子が結果を言わない");
            match said.split_whitespace().collect::<Vec<_>>()[..] {
                ["won", e, ..] => winners.push(e.parse().unwrap()),
                ["lost", kind, ..] => assert_eq!(kind, "AlreadyExists", "負けた create"),
                _ => panic!("子の出力: {out}"),
            }
        }
        check_winners(&path, &winners, &mut tally);
    }
    let _ = std::fs::remove_dir_all(&base);
    assert_eq!(tally, Tally::default());
}

/// create の置かない file がある directory は、 作成中の印 (`creating`) があっても残骸と見なさない (消さない)。
#[test]
fn create_does_not_reclaim_a_directory_with_other_files() {
    let path = tmp("other_files");
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("creating"), b"").unwrap();
    std::fs::write(path.join("notes.txt"), b"keep me").unwrap();
    let err = Engine::create_growable_tiny(path.to_str().unwrap()).err().expect("DB でない directory に作った");
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(path.join("notes.txt")).unwrap(), b"keep me", "DB でない directory の file を消した");
    let _ = std::fs::remove_dir_all(&path);
}

/// 作成中の印のある directory でも、 別の handle が writer lock を持っていれば (作成中) 触らない。
#[test]
fn create_leaves_a_directory_being_created_alone() {
    for with_marker in [false, true] {
        let path = tmp(&format!("being_created_{with_marker}"));
        std::fs::create_dir_all(&path).unwrap();
        let holder = std::fs::File::create(path.join("lock")).unwrap();
        holder.lock().unwrap();
        if with_marker {
            std::fs::write(path.join("creating"), b"").unwrap();
        }
        let err = Engine::create_growable_tiny(path.to_str().unwrap()).err().expect("作成中の directory に作った");
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(path.join("creating").exists(), with_marker, "作成中の directory を触った");
        drop(holder);
        // lock が空けば残骸として作り直せる。 同じ process の別 thread (上の process の試験) が子を起動する瞬間は、 子が
        // fd の写しを exec まで持つので、 手放した lock がまだ取れないことがある (実測: 子を起動し続ける横で 2 万回中 12 回)。
        // create も約 20 ms 取り直すが、 負荷の高い machine ではそれより長いことがあるので、 ここでも取り直す
        let eng = (0..100)
            .find_map(|_| match Engine::create_growable_tiny(path.to_str().unwrap()) {
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    std::thread::sleep(Duration::from_millis(10));
                    None
                }
                r => Some(r),
            })
            .expect("lock が空いても AlreadyExists のまま");
        drop(eng.expect("lock が空いた残骸に作り直せない"));
        assert!(matches!(Engine::probe(&path), enchudb_engine::DbState::Ready));
        let _ = std::fs::remove_dir_all(&path);
    }
}
