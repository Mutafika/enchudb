//! file advisory lock の platform 吸収 (#280)。
//!
//! `std::fs::File::lock` (Rust 1.89 安定化) は unix で `flock(2)`、 Windows で
//! `LockFileEx` に落ちる。 ただし **std は対応 target を列挙で持っており、
//! そこに載っていない platform では `ErrorKind::Unsupported`** を返す
//! ("lock() not supported")。 `target_os = "android"` (bionic) がこれに当たる。
//!
//! bionic 側に flock が無いわけではない — NDK の `sys/file.h` は API level の
//! 制約無しに `int flock(int, int)` を宣言している。 std の列挙漏れなので、
//! `Unsupported` の時だけ libc の flock を直接呼べば、 他 platform と **完全に
//! 同じ意味論** (open file description 単位 / blocking / close で解放) のまま
//! 塞げる。
//!
//! fcntl の record lock (`F_SETLKW`) は代替にならない:
//! - 同じ file への **別 fd を close しただけで、 そのプロセスの lock が全部消える**
//! - **同一プロセス内では常に成功する** (排他にならない)
//! - 32bit target では `flock64` 構造体が要る (OFD lock も同様)
//!
//! 呼び出し側 (engine の writer lock、 WAL の append guard) は flock の意味論を
//! 前提に書かれているので、 ここは flock で揃える。

use std::fs::File;
use std::io;

/// `lock_exclusive` の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockOutcome {
    /// 排他を取れた。 別プロセスは解放まで待たされる。
    Locked,
    /// **この file system は advisory lock を持たない** (一部の FUSE /
    /// ネットワーク FS)。 caller は排他なしで続行するか、 自分で失敗させるかを
    /// 選ぶ。 ここでエラーにすると platform / FS ごと使えなくなるため、
    /// 「取れなかった」 を成功側で返す。
    Unsupported,
}

/// 排他 advisory lock を取る。 他プロセスが保持中は **取れるまで block** する。
/// fd を close すれば解放される (guard を明示的に外す場合は [`unlock`])。
pub fn lock_exclusive(f: &File) -> io::Result<LockOutcome> {
    match f.lock() {
        Ok(()) => Ok(LockOutcome::Locked),
        // std が flock を持たない target (Android/bionic) だけ libc に落とす。
        Err(e) if e.kind() == io::ErrorKind::Unsupported => fallback_lock_exclusive(f),
        Err(e) => Err(e),
    }
}

/// [`lock_exclusive`] で取った lock を明示的に解放する。 `LockOutcome::Unsupported`
/// だった file に対して呼んでも成功扱い (解放するものが無い)。
pub fn unlock(f: &File) -> io::Result<()> {
    match f.unlock() {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::Unsupported => fallback_unlock(f),
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn fallback_lock_exclusive(f: &File) -> io::Result<LockOutcome> {
    match raw_flock(f, libc::LOCK_EX) {
        Ok(()) => Ok(LockOutcome::Locked),
        Err(e) if lock_unavailable(&e) => Ok(LockOutcome::Unsupported),
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn fallback_unlock(f: &File) -> io::Result<()> {
    match raw_flock(f, libc::LOCK_UN) {
        Ok(()) => Ok(()),
        // lock が取れていない file なので、 解放できないのは想定内。
        Err(e) if lock_unavailable(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn raw_flock(f: &File, op: libc::c_int) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    loop {
        if unsafe { libc::flock(f.as_raw_fd(), op) } == 0 {
            return Ok(());
        }
        let e = io::Error::last_os_error();
        // blocking LOCK_EX は signal で中断され得る。 std の cvt と同じく取り直す。
        if e.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(e);
    }
}

/// 「この FS は lock を持たない」 を表す errno か。 flock が無い FS では
/// カーネル/ドライバによって返る errno が割れる。
#[cfg(unix)]
fn lock_unavailable(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::Unsupported
        || matches!(
            e.raw_os_error(),
            Some(libc::ENOSYS) | Some(libc::EOPNOTSUPP) | Some(libc::ENOLCK)
        )
}

/// unix 以外で std が `Unsupported` を返す経路は現状無い (Windows は LockFileEx)。
/// 万一返ってきても open 自体は通す。
#[cfg(not(unix))]
fn fallback_lock_exclusive(_f: &File) -> io::Result<LockOutcome> {
    Ok(LockOutcome::Unsupported)
}

#[cfg(not(unix))]
fn fallback_unlock(_f: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;

    /// 非 blocking で取れるか試すだけの probe (別 fd = 別 open file description)。
    fn probe_locked(f: &File) -> bool {
        unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) != 0 }
    }

    fn open(path: &std::path::Path) -> File {
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(path)
            .unwrap()
    }

    /// Android (bionic) で実際に走る経路を host で固定する。 **排他が効くこと**
    /// (別 fd から取れない) と、 **close / unlock で解放されること** の両方。
    /// no-op 実装に差し替えると 1 つ目の assert が落ちる。
    ///
    /// fcntl record lock ではこの test は通らない (同一プロセスの別 fd から
    /// 常に取れてしまう) — flock を選んだ理由そのもの。
    #[test]
    fn fallback_lock_excludes_other_fd_and_releases() {
        // 並走 cargo test と衝突しないよう pid を混ぜる。
        let dir = std::env::temp_dir().join(format!("enchudb-filelock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("lock");

        let held = open(&path);
        assert_eq!(fallback_lock_exclusive(&held).unwrap(), LockOutcome::Locked);

        let other = open(&path);
        assert!(
            probe_locked(&other),
            "別 fd から取れてしまう = 排他が効いていない"
        );

        // unlock で解放 → 別 fd から取れる。
        fallback_unlock(&held).unwrap();
        assert!(!probe_locked(&other), "unlock 後は取れるはず");

        // close でも解放される (WriterLock は drop で close するだけ)。
        drop(other);
        let after_close = open(&path);
        assert_eq!(
            fallback_lock_exclusive(&after_close).unwrap(),
            LockOutcome::Locked
        );

        drop(held);
        drop(after_close);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 公開 API 経路 (host では std が flock を持つので std 側を通る) も一応。
    #[test]
    fn public_lock_unlock_roundtrip() {
        let dir = std::env::temp_dir().join(format!("enchudb-filelock-pub-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("lock");
        let f = open(&path);
        assert_eq!(lock_exclusive(&f).unwrap(), LockOutcome::Locked);
        unlock(&f).unwrap();
        let other = open(&path);
        assert!(!probe_locked(&other), "unlock 後は別 fd から取れるはず");
        drop(other);
        drop(f);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
