//! BlobStore 拡張 integration tests。
//!
//! 既存 src/blob_store.rs の unit tests で以下はカバー済:
//! - put/get roundtrip, dedup, missing, delete, single-byte tamper → HashMismatch
//! - 同一 blob の並行 put 8 threads
//! - 100MB 大 blob
//!
//! このファイルが追加で検証:
//! - 並行 reader + writer の混在ワークロード(race 下で読み書きが一貫する)
//! - 書き込み先が read-only 時の error 伝搬(ディスクフル相当)
//! - 部分破損(truncate)→ HashMismatch
//! - delete と get の race(get 途中で消えても panic しない)

use enchudb::blob_store::{BlobError, BlobId, BlobStore, LocalBlobStore};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

fn tmp_root(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "enchu-blob-ext-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn cleanup(root: &std::path::Path) {
    // read-only にしてあるディレクトリを消すため、まず戻す
    if let Ok(meta) = std::fs::metadata(root) {
        let mut perms = meta.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        #[cfg(not(unix))]
        {
            perms.set_readonly(false);
        }
        let _ = std::fs::set_permissions(root, perms);
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn concurrent_mixed_reader_writer_workload() {
    // 16 thread が put + get を混在で走らせ、全員一貫した結果を得る。
    let root = tmp_root("mixed_rw");
    let store = Arc::new(LocalBlobStore::new(&root).unwrap());

    // 事前に 8 blob を put して、id をリーダースレッドに共有
    let seed_ids: Vec<BlobId> = (0..8u32)
        .map(|i| {
            let data = format!("seed-blob-{}-{}", i, "x".repeat(100)).into_bytes();
            store.put(&data).unwrap()
        })
        .collect();
    let seed_ids = Arc::new(seed_ids);

    let mut handles = Vec::new();
    // 8 writer: それぞれ 50 新規 blob を put
    for t in 0..8 {
        let s = store.clone();
        handles.push(thread::spawn(move || {
            for i in 0..50u32 {
                let data = format!("writer-{}-item-{}", t, i).into_bytes();
                s.put(&data).unwrap();
            }
        }));
    }
    // 8 reader: seed_ids をぐるぐる get、HashMismatch は出ないことを確認
    for _ in 0..8 {
        let s = store.clone();
        let ids = seed_ids.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..200 {
                for id in ids.iter() {
                    let got = s.get(id).unwrap();
                    assert!(got.is_some());
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // 400 個(8*50) + 8 seed = 408 blob 格納されている
    let mut count = 0usize;
    walk(&root, &mut |p| {
        if p.is_file() && !p.to_string_lossy().contains(".tmp.") {
            count += 1;
        }
    });
    assert_eq!(count, 408);

    cleanup(&root);
}

#[cfg(unix)]
#[test]
fn put_into_readonly_root_returns_io_error() {
    // ディスクフル相当: 書き込み先を read-only にして、put がエラーで止まる。
    use std::os::unix::fs::PermissionsExt;
    let root = tmp_root("readonly");
    let store = LocalBlobStore::new(&root).unwrap();

    // 1 件は事前に書いておく(get は通る確認)
    let pre_id = store.put(b"before lockdown").unwrap();

    // root を read-only にする
    let mut perms = std::fs::metadata(&root).unwrap().permissions();
    perms.set_mode(0o555);
    std::fs::set_permissions(&root, perms).unwrap();

    // 新規 blob の put はエラー
    let r = store.put(b"after lockdown new content");
    match r {
        Err(BlobError::Io(_)) => {}
        Ok(_) => panic!("put should fail on read-only root"),
        Err(other) => panic!("expected Io, got {:?}", other),
    }

    // 既存 blob の get は問題ない
    let got = store.get(&pre_id).unwrap();
    assert_eq!(got.as_deref(), Some(&b"before lockdown"[..]));

    cleanup(&root);
}

#[test]
fn truncated_blob_file_returns_hash_mismatch() {
    let root = tmp_root("truncated");
    let store = LocalBlobStore::new(&root).unwrap();
    let data = vec![0xDE; 4096];
    let id = store.put(&data).unwrap();

    // ファイルを truncate して短くする
    let mut blob_file: Option<PathBuf> = None;
    walk(&root, &mut |p| {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        if p.is_file() && !name.contains(".tmp.") && name == id.to_hex()[4..] {
            blob_file = Some(p.to_path_buf());
        }
    });
    let blob_file = blob_file.expect("blob file should exist");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&blob_file)
        .unwrap();
    f.set_len(1024).unwrap(); // 4096 → 1024 に切り詰め

    match store.get(&id) {
        Err(BlobError::HashMismatch { .. }) => {}
        other => panic!("expected HashMismatch on truncated, got {:?}", other),
    }

    cleanup(&root);
}

#[test]
fn delete_during_get_race_does_not_panic() {
    // delete と get が同時に走っても、get は Ok(Some) or Ok(None) のどちらかで返す。
    // HashMismatch は起きない(削除は atomic な unlink なので部分読みが起きない前提)。
    let root = tmp_root("del_race");
    let store = Arc::new(LocalBlobStore::new(&root).unwrap());

    // 100 blob
    let ids: Vec<BlobId> = (0..100u32)
        .map(|i| store.put(&vec![i as u8; 256]).unwrap())
        .collect();
    let ids = Arc::new(ids);

    let mut handles = Vec::new();
    // reader: get をぐるぐる
    for _ in 0..4 {
        let s = store.clone();
        let ids = ids.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..200 {
                for id in ids.iter() {
                    // Some / None どちらも OK、HashMismatch は NG
                    let _ = s.get(id).unwrap();
                }
            }
        }));
    }
    // deleter: 半分を削除
    {
        let s = store.clone();
        let ids = ids.clone();
        handles.push(thread::spawn(move || {
            for id in ids.iter().take(50) {
                let _ = s.delete(id);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    cleanup(&root);
}

fn walk<F: FnMut(&std::path::Path)>(root: &std::path::Path, f: &mut F) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        if let Ok(rd) = std::fs::read_dir(&p) {
            for entry in rd.flatten() {
                let ep = entry.path();
                if ep.is_dir() {
                    stack.push(ep);
                } else {
                    f(&ep);
                }
            }
        }
    }
}
