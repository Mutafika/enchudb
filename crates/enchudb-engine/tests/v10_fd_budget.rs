//! v10: segment は himo 1 本につき 1 file なので、 writer が fd を持ち続ける戦略は
//! `RLIMIT_NOFILE` を食う。 予算制 (soft の半分まで保持、 超えたら都度 open) が
//! **本当に低い fd 上限で動くか** を子 process で確かめる (`adc0da9` の回帰 gate)。
//!
//! 予算無しの初版はここで `define_himo` が EMFILE で panic した。

use enchudb_engine::{Engine, ValueType};
use std::process::Command;

const CHILD_LIMIT_ENV: &str = "ENCHU_V10_FD_LIMIT";
const CHILD_PATH_ENV: &str = "ENCHU_V10_FD_PATH";
const PROBE_PREFIX: &str = "FD ";

const HIMOS: usize = 200;
const ENTS: u32 = 50;

/// child: 自分の RLIMIT_NOFILE を下げてから himo 200 本の DB を作り、 書いて、 閉じて、
/// 開き直して読む。 どこかで EMFILE になれば panic して非 0 exit する。
#[test]
fn fd_limited_child() {
    let Ok(limit) = std::env::var(CHILD_LIMIT_ENV) else { return };
    let limit: u64 = limit.parse().unwrap();
    let path = std::env::var(CHILD_PATH_ENV).unwrap();

    // soft も hard も下げる (engine の raise_fd_limit が hard まで戻せないように)。
    let lim = libc::rlimit { rlim_cur: limit, rlim_max: limit };
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) }, 0, "setrlimit failed");

    let mut eng = Engine::create_with_capacity(&path, 4096).unwrap();
    eng.define_table("widgets", 1000).unwrap();
    for h in 0..HIMOS {
        eng.define_himo_in("widgets", &format!("h{h}"), ValueType::Number, 1000).unwrap();
    }
    let eids: Vec<u64> = (0..ENTS)
        .map(|i| {
            let e = eng.entity_in("widgets").unwrap();
            for h in 0..HIMOS {
                eng.tie(e, &format!("widgets.h{h}"), i * 1000 + h as u32);
            }
            e
        })
        .collect();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let retained = enchudb_engine::segment_map::retained_fds();
    drop(eng);

    let eng = Engine::open(&path).unwrap();
    let mut sum = 0u64;
    for (i, &e) in eids.iter().enumerate() {
        for h in 0..HIMOS {
            let v = eng
                .get(e, &format!("widgets.h{h}"))
                .unwrap_or_else(|| panic!("値が消えた: eid={e} h{h}"))
                .to_string()
                .parse::<u32>()
                .unwrap();
            assert_eq!(v, i as u32 * 1000 + h as u32, "値が違う: eid={e} h{h}");
        }
        sum += 1;
    }
    println!("{PROBE_PREFIX}limit={limit} himos={} retained={retained} verified_ents={sum}", eng.himo_count());
}

fn run(limit: u64) -> (bool, String) {
    let path = format!("/tmp/enchu_v10_fd_{}_{limit}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["fd_limited_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_LIMIT_ENV, limit.to_string())
        .env(CHILD_PATH_ENV, &path)
        .output()
        .expect("spawn child");
    let _ = std::fs::remove_dir_all(&path);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout
        .lines()
        .find_map(|l| l.find(PROBE_PREFIX).map(|i| l[i + PROBE_PREFIX.len()..].trim().to_string()))
        .unwrap_or_else(|| {
            let err = String::from_utf8_lossy(&out.stderr);
            format!(
                "FAILED status={:?} | {}",
                out.status.code(),
                stdout.lines().chain(err.lines()).filter(|l| l.contains("panicked") || l.contains("Error")).take(2).collect::<Vec<_>>().join(" / ")
            )
        });
    (out.status.success(), line)
}

#[test]
fn himo_200_db_works_under_tight_fd_limits() {
    for limit in [64u64, 128, 512] {
        let (ok, line) = run(limit);
        eprintln!("RLIMIT_NOFILE={limit}: {line}");
        assert!(ok, "RLIMIT_NOFILE={limit} で失敗: {line}");
    }
}
