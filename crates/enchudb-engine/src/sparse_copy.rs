//! sparse (穴あき) ファイルを、 穴を潰さずにコピーする。
//!
//! # なぜ要るか
//!
//! enchudb の DB body は 「apparent は巨大、 実データはごく一部」 な sparse
//! ファイルである (eager create は `set_len(total_size)`、 growable は
//! `ftruncate` で伸ばす)。 置いてあるだけなら穴は物理を消費しないので、
//! apparent が何十 GB あっても実消費は数 MB で済む。
//!
//! ところが `std::fs::copy` は **platform によって穴の扱いが違う**:
//!
//! | | 8 GB の穴だけファイルを copy | コピー先の実消費 |
//! |---|---|---|
//! | macOS (APFS) | 0.0003 秒 | 0 MB |
//! | Linux (ext4) | 8.09 秒 | **8192 MB** |
//!
//! macOS は `fcopyfile(COPYFILE_CLONE)` でメタデータだけ触って終わるが、
//! Linux は穴を 0 で埋めて実際に書き出す。 つまり **コピーした瞬間に apparent
//! がまるごと物理化する**。 既定 capacity の DB (apparent 24 GB、 v9 なら
//! 85 GB) を `snapshot_export` すると、 Linux ではその全量が書かれる。
//!
//! CI (ubuntu) が `No space left on device` で落ちたのはこれが原因だった。
//!
//! # どう直すか
//!
//! `SEEK_DATA` / `SEEK_HOLE` で **データが入っている範囲だけ** を写し、 穴は
//! `set_len` で表現する。 macOS は `std::fs::copy` が既に最適 (clonefile) なので
//! そのまま使う。

use std::io;
use std::path::Path;

/// data 範囲を写すときの読み書き単位。
#[cfg(all(unix, any(not(target_os = "macos"), test)))]
const CHUNK: usize = 1 << 20; // 1 MiB

/// 穴を維持したままファイルをコピーする。 戻り値は `std::fs::copy` と同じく
/// コピーしたファイルのサイズ (= src の apparent size)。
///
/// - macOS: `std::fs::copy` (= `fcopyfile` clonefile。 穴を維持したまま最速)
/// - その他の unix: `SEEK_DATA` / `SEEK_HOLE` でデータ範囲だけ写す
/// - Windows / その他: `std::fs::copy` (穴の概念が無いか、 辿る API が無い)
///
/// # `SEEK_DATA` 非対応の FS
///
/// `EINVAL` / `ENOTSUP` / `ENOSYS` が返る環境では素の `std::fs::copy` に落ちる。
/// `SEEK_DATA` を持たない FS は多くの場合そもそも穴を持てない (FAT 等) ので、
/// apparent == 実データで実害は無い。 ただし **穴は持てるが `SEEK_DATA` が無い**
/// 環境 (一部の network FS 等) では元の症状 (= apparent 全量の物理化) に戻る。
///
/// # durability
///
/// **fsync しない。** 書き終わった時点では page cache 止まりなので、 電源断で
/// 消えうる。 durable にしたい呼び側が自分で fsync すること
/// (`Engine::snapshot_export` も同じ方針 — その doc を参照)。
pub fn copy_sparse(src: &Path, dst: &Path) -> io::Result<u64> {
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        return match copy_via_holes(src, dst) {
            Ok(n) => Ok(n),
            Err(e) if is_unsupported(&e) => std::fs::copy(src, dst),
            Err(e) => Err(e),
        };
    }
    #[cfg(not(all(unix, not(target_os = "macos"))))]
    {
        std::fs::copy(src, dst)
    }
}

/// `SEEK_DATA` が使えない環境か (= 素の copy に落ちるべきか)。
#[cfg(all(unix, not(target_os = "macos")))]
fn is_unsupported(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EINVAL) | Some(libc::ENOTSUP) | Some(libc::ENOSYS)
    )
}

/// `SEEK_DATA` / `SEEK_HOLE` でデータ範囲だけを写す本体。
///
/// macOS では `copy_sparse` から呼ばれない (clonefile のほうが速い) が、
/// **アルゴリズムを両 OS のテストで踏ませる**ために test build では常にコンパイルする。
#[cfg(all(unix, any(not(target_os = "macos"), test)))]
fn copy_via_holes(src: &Path, dst: &Path) -> io::Result<u64> {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::io::AsRawFd;

    let mut fin = File::open(src)?;
    let meta = fin.metadata()?;
    let len = meta.len();

    let mut fout = File::create(dst)?;
    // 先に最終サイズを確定させる。 末尾が穴でも apparent が src と一致する。
    fout.set_len(len)?;

    let fd = fin.as_raw_fd();
    let mut buf = vec![0u8; CHUNK];
    let mut off: u64 = 0;
    while off < len {
        // 次にデータが始まる位置。 None = ここから先は全部穴。
        let Some(data) = lseek(fd, off, libc::SEEK_DATA)? else { break };
        if data >= len {
            break;
        }
        // そのデータ範囲の終わり (= 次の穴)。 見つからなければ EOF まで。
        let hole = lseek(fd, data, libc::SEEK_HOLE)?.unwrap_or(len).min(len);
        if hole <= data {
            // 前進しない返り値は無限ループになるので保険で抜ける。
            break;
        }

        fin.seek(SeekFrom::Start(data))?;
        fout.seek(SeekFrom::Start(data))?;
        let mut remain = hole - data;
        while remain > 0 {
            let n = remain.min(CHUNK as u64) as usize;
            fin.read_exact(&mut buf[..n])?;
            fout.write_all(&buf[..n])?;
            remain -= n as u64;
        }
        off = hole;
    }
    // `File::flush` は no-op (userspace buffer が無い)。 durable 化は呼び側責務なので
    // ここでは fsync しない (module doc の durability 節を参照)。
    fout.flush()?;
    // `std::fs::copy` は permission を引き継ぐので合わせる。
    let mode = meta.permissions().mode();
    fout.set_permissions(std::fs::Permissions::from_mode(mode))?;
    Ok(len)
}

/// `lseek(fd, off, whence)`。 `ENXIO` (= もうデータが無い) は `None`。
#[cfg(all(unix, any(not(target_os = "macos"), test)))]
fn lseek(fd: std::os::unix::io::RawFd, off: u64, whence: i32) -> io::Result<Option<u64>> {
    let r = unsafe { libc::lseek(fd, off as libc::off_t, whence) };
    if r < 0 {
        let e = io::Error::last_os_error();
        if e.raw_os_error() == Some(libc::ENXIO) {
            return Ok(None);
        }
        return Err(e);
    }
    Ok(Some(r as u64))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::fs::MetadataExt;

    fn tmp(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "enchudb-sparsecopy-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    /// apparent `len` で、 `spots` の位置に 1 KiB ずつデータを置いた穴あきファイル。
    fn make_sparse(path: &Path, len: u64, spots: &[u64]) {
        let mut f = File::create(path).unwrap();
        f.set_len(len).unwrap();
        for (i, &at) in spots.iter().enumerate() {
            f.seek(SeekFrom::Start(at)).unwrap();
            f.write_all(&vec![(i as u8) | 0x80; 1024]).unwrap();
        }
        f.sync_all().unwrap();
    }

    fn read_all(path: &Path) -> Vec<u8> {
        let mut v = Vec::new();
        File::open(path).unwrap().read_to_end(&mut v).unwrap();
        v
    }

    fn physical_bytes(path: &Path) -> u64 {
        std::fs::metadata(path).unwrap().blocks() * 512
    }

    /// 本題: 穴あきファイルを写しても **穴が穴のまま残る**。
    ///
    /// falsify: `copy_via_holes` を `std::fs::copy` に差し替えると、 Linux では
    /// physical が apparent と同じになって最後の assert が落ちる
    /// (macOS は clonefile なので落ちない — Linux で実演すること)。
    #[test]
    fn copy_via_holes_keeps_the_holes() {
        let src = tmp("src");
        let dst = tmp("dst");
        let len = 256 * 1024 * 1024; // 256 MiB
        make_sparse(&src, len, &[0, 100 * 1024 * 1024, len - 4096]);

        let n = copy_via_holes(&src, &dst).unwrap();

        assert_eq!(n, len, "戻り値が apparent size と違う");
        assert_eq!(
            std::fs::metadata(&dst).unwrap().len(),
            len,
            "コピー先の apparent size が違う",
        );
        assert_eq!(read_all(&src), read_all(&dst), "中身が一致しない");
        // データは 3 KiB しか無いので、 physical は apparent の 1% も要らない。
        let phys = physical_bytes(&dst);
        assert!(
            phys < len / 100,
            "穴が潰れている: physical {} bytes / apparent {} bytes",
            phys,
            len,
        );

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
    }

    /// データが 1 byte も無い (= 全部穴) ファイル。 `SEEK_DATA` が `ENXIO` を返す枝。
    #[test]
    fn copy_via_holes_handles_a_file_that_is_all_hole() {
        let src = tmp("allhole-src");
        let dst = tmp("allhole-dst");
        let len = 64 * 1024 * 1024;
        make_sparse(&src, len, &[]);

        assert_eq!(copy_via_holes(&src, &dst).unwrap(), len);
        assert_eq!(std::fs::metadata(&dst).unwrap().len(), len);
        assert!(read_all(&dst).iter().all(|&b| b == 0), "0 で埋まっていない");
        assert!(physical_bytes(&dst) < len / 100, "全部穴なのに物理を食っている");

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
    }

    /// 穴が無い普通のファイルでも壊さない。
    #[test]
    fn copy_via_holes_handles_a_dense_file() {
        let src = tmp("dense-src");
        let dst = tmp("dense-dst");
        let body: Vec<u8> = (0..(3 * CHUNK + 777)).map(|i| (i % 251) as u8).collect();
        File::create(&src).unwrap().write_all(&body).unwrap();

        assert_eq!(copy_via_holes(&src, &dst).unwrap(), body.len() as u64);
        assert_eq!(read_all(&dst), body, "中身が一致しない");

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
    }

    /// 空ファイル。
    #[test]
    fn copy_via_holes_handles_an_empty_file() {
        let src = tmp("empty-src");
        let dst = tmp("empty-dst");
        File::create(&src).unwrap();

        assert_eq!(copy_via_holes(&src, &dst).unwrap(), 0);
        assert_eq!(std::fs::metadata(&dst).unwrap().len(), 0);

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
    }

    /// 公開 API 側 (platform dispatch 込み) も中身を壊さない。
    #[test]
    fn copy_sparse_roundtrips_content() {
        let src = tmp("pub-src");
        let dst = tmp("pub-dst");
        let len = 32 * 1024 * 1024;
        make_sparse(&src, len, &[4096, len - 8192]);

        assert_eq!(copy_sparse(&src, &dst).unwrap(), len);
        assert_eq!(read_all(&src), read_all(&dst));
        assert!(physical_bytes(&dst) < len / 100, "穴が潰れている");

        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(&dst);
    }
}
