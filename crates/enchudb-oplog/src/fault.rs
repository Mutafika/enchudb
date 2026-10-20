//! I/O の失敗と process の死を、 決まった I/O の点で起こす (feature `crashsim`、 テスト専用、 #98 ③④)。
//!
//! engine / oplog は filesystem の I/O (open / ftruncate / mmap / msync / fsync / statvfs / flock / rename / 読み込み) の
//! 直前で [`crate::io_point`] を呼ぶ。 ここはその回数を数え、 環境変数で選んだ N 回目の点で:
//!
//! - `error` — その I/O を `EIO` で失敗させる (1 回だけ。 呼び手の失敗の扱いを通す)
//! - `exit` — process を `_exit` で落とす (Drop も atexit も走らない。 page cache は残る = process の死)
//! - `lost` — 電源断の像 ([`crate::crashsim::capture`] の `Lost`) を撮ってから `_exit` (page cache ごと失う)
//!
//! 環境変数 (最初に点を通った時に 1 度だけ読む。 子 process で設定する):
//!
//! - `ENCHU_FAULT_AT` — 何回目の点で起こすか (1 始まり)。 無い / 0 なら起こさない (数えるだけ)
//! - `ENCHU_FAULT_MODE` — `error` (既定) / `exit` / `lost`
//! - `ENCHU_FAULT_ROOT` / `ENCHU_FAULT_OUT` — `lost` の像の元の directory と写し先 (`lost` は呼び手が先に
//!   [`crate::crashsim::start`] を呼んでおくこと)
//! - `ENCHU_FAULT_TRACE` — 通った点の名前を 1 行ずつ足していく file (`n<TAB>名前`)
//!
//! 起こした時は stderr に `ENCHU_FAULT <mode> #<n> <名前>` を出す。 `exit` / `lost` の終了 code は [`EXIT_CODE`]。
//!
//! 数えるのは process 全体の 1 本の数なので、 背景の thread (consumer の周期の書き出し) も同じ数を進める。 背景の
//! thread がある書き手では、 同じ N でも回によって当たる点が違いうる (当たった点は上の stderr で分かる)。

use std::io::{self, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// `exit` / `lost` で落とした時の終了 code。
pub const EXIT_CODE: i32 = 86;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Error,
    Exit,
    Lost,
}

struct Config {
    at: u64,
    mode: Mode,
    root: Option<String>,
    out: Option<String>,
    trace: Option<std::sync::Mutex<std::fs::File>>,
}

static CONFIG: OnceLock<Config> = OnceLock::new();
static COUNT: AtomicU64 = AtomicU64::new(0);

fn config() -> &'static Config {
    CONFIG.get_or_init(|| {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let mode = match var("ENCHU_FAULT_MODE").as_deref() {
            None | Some("error") => Mode::Error,
            Some("exit") => Mode::Exit,
            Some("lost") => Mode::Lost,
            Some(m) => panic!("ENCHU_FAULT_MODE: unknown mode {m}"),
        };
        let trace = var("ENCHU_FAULT_TRACE").map(|p| {
            let f = std::fs::OpenOptions::new().create(true).append(true).open(&p).expect("ENCHU_FAULT_TRACE: open");
            std::sync::Mutex::new(f)
        });
        Config {
            at: var("ENCHU_FAULT_AT").and_then(|v| v.parse().ok()).unwrap_or(0),
            mode,
            root: var("ENCHU_FAULT_ROOT"),
            out: var("ENCHU_FAULT_OUT"),
            trace,
        }
    })
}

/// これまでに通った点の数。
pub fn count() -> u64 {
    COUNT.load(Ordering::Relaxed)
}

/// 点 `site` を通る ([`crate::io_point`] の中身)。
pub fn point(site: &'static str) -> io::Result<()> {
    let c = config();
    let n = COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if let Some(t) = &c.trace {
        let mut f = t.lock().unwrap_or_else(|e| e.into_inner());
        let _ = writeln!(f, "{n}\t{site}");
    }
    if n != c.at {
        return Ok(());
    }
    eprintln!("ENCHU_FAULT {:?} #{n} {site}", c.mode);
    match c.mode {
        Mode::Error => Err(io::Error::from_raw_os_error(libc::EIO)),
        Mode::Exit => exit_now(),
        Mode::Lost => {
            let (Some(root), Some(out)) = (&c.root, &c.out) else {
                panic!("ENCHU_FAULT_MODE=lost needs ENCHU_FAULT_ROOT and ENCHU_FAULT_OUT")
            };
            if let Err(e) = crate::crashsim::capture(std::path::Path::new(root), std::path::Path::new(out), crate::crashsim::Mode::Lost) {
                eprintln!("ENCHU_FAULT capture failed: {e}");
                // SAFETY: 落とすだけ (Drop を走らせないのが目的)
                unsafe { libc::_exit(1) }
            }
            exit_now()
        }
    }
}

fn exit_now() -> ! {
    let _ = io::stderr().flush();
    // SAFETY: process をその場で終える (Drop も atexit も走らせない = 落ちた姿を残す)
    unsafe { libc::_exit(EXIT_CODE) }
}
