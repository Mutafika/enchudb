//! BlobStore — content-addressable な大容量バイナリ保管抽象化。
//!
//! # 動機
//!
//! enchudb 本体の `content()` は main DB ファイル内に bytes を埋める。512MB/blob の上限、
//! 大量投入で DB ファイル肥大・mmap サイズ爆発。画像/動画/モデル/tar.gz 等の大きな blob は
//! **BlobStore に逃がす**のが正しい設計:
//!
//! - 紐の値 or content の値 = `BlobId`(sha-256、32B)
//! - 実体は `LocalBlobStore`(ディスク) or `S3BlobStore`(クラウド、feature 追加時) 等
//! - **content-addressable**: hash が同じ = 内容が同じ → 自動 dedup(entity を 100 個 tie しても blob は 1 個)
//!
//! # レイアウト(LocalBlobStore)
//!
//! ```text
//! <root>/
//!   ab/
//!     cd/
//!       ef0123...<60 hex chars>   <- 実体(atomic write: tmp + rename)
//! ```
//!
//! # 使い方
//!
//! ```
//! use enchudb_engine::blob_store::{BlobStore, LocalBlobStore};
//! let root = format!("/tmp/enchudb-blob-doc-{}", std::process::id());
//! let store = LocalBlobStore::new(&root).unwrap();
//! let id = store.put(b"hello").unwrap();
//! assert_eq!(store.get(&id).unwrap().as_deref(), Some(&b"hello"[..]));
//! # let _ = std::fs::remove_dir_all(&root);
//! ```

use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Blob のコンテンツアドレス(sha-256、32B 固定)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobId(pub [u8; 32]);

impl BlobId {
    /// バイト列から sha-256 を計算して BlobId を作る。
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut h = Sha256::new();
        h.update(data);
        let out: [u8; 32] = h.finalize().into();
        BlobId(out)
    }

    /// 64 文字の小文字 hex 文字列に変換。
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            s.push_str(&format!("{:02x}", b));
        }
        s
    }

    /// 64 文字 hex をパース。形式不正なら None。
    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 64 || !s.is_ascii() {
            return None;
        }
        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(BlobId(out))
    }

    /// 生バイト参照。
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// BlobStore 操作で起きうるエラー。
#[derive(Debug)]
pub enum BlobError {
    Io(io::Error),
    /// 取り出した blob の再計算 hash が BlobId と一致しない(破損 or 改竄)。
    HashMismatch { expected: BlobId, got: BlobId },
}

impl std::fmt::Display for BlobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobError::Io(e) => write!(f, "blob io: {}", e),
            BlobError::HashMismatch { expected, got } => write!(
                f,
                "blob hash mismatch: expected={}, got={}",
                expected, got
            ),
        }
    }
}

impl std::error::Error for BlobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BlobError::Io(e) => Some(e),
            BlobError::HashMismatch { .. } => None,
        }
    }
}

impl From<io::Error> for BlobError {
    fn from(e: io::Error) -> Self {
        BlobError::Io(e)
    }
}

/// 大容量 blob の保管抽象。
///
/// 実装は `LocalBlobStore`(ファイルシステム)、`S3BlobStore`(将来、feature 追加時) 等。
/// 全メソッド `&self` で並行安全。
pub trait BlobStore: Send + Sync {
    /// data を書き込み、BlobId を返す。既存なら追加書き込みせず、同じ BlobId を返す。
    fn put(&self, data: &[u8]) -> Result<BlobId, BlobError>;

    /// BlobId から bytes を取得。無ければ Ok(None)。ハッシュ不一致なら `HashMismatch`。
    fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, BlobError>;

    /// 存在チェック(軽量、I/O 1 回以下)。
    fn exists(&self, id: &BlobId) -> bool;

    /// 削除。消したら true、無ければ false。
    ///
    /// # 注意 (content-addressed store の共有削除ハザード)
    ///
    /// store は content-addressed で **参照カウントを持たない**。 同じ bytes を
    /// put した全 entity が同一 blob を共有しているため、 `delete` は
    /// **その BlobId を参照している全 entity の実体を一括で破壊する**。
    /// 「この entity からだけ外す」 用途では呼ばないこと — 紐/content 側の参照を
    /// 消すだけに留め、 実体の GC は「どの entity からも参照されていない」ことを
    /// caller が確認してから行う。
    fn delete(&self, id: &BlobId) -> Result<bool, BlobError>;
}

/// ローカルファイルシステム上の BlobStore 実装。
///
/// - レイアウト: `<root>/<ab>/<cd>/<remaining_60_hex>`
/// - content-addressable: 同じ hash は同じファイル、dedup 自動
/// - atomic write: `<name>.tmp.<pid>.<counter>` に書いて rename
/// - 読み取り時に sha-256 を再計算して検証(破損検知)
#[derive(Clone)]
pub struct LocalBlobStore {
    root: PathBuf,
}

/// tmp ファイル衝突回避用のグローバル連番。
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

impl LocalBlobStore {
    /// root ディレクトリを用意して開く。無ければ作る。
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// root への参照。
    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    fn path_for(&self, id: &BlobId) -> PathBuf {
        let hex = id.to_hex();
        let mut p = self.root.clone();
        p.push(&hex[0..2]);
        p.push(&hex[2..4]);
        p.push(&hex[4..]);
        p
    }

    fn tmp_path(&self, final_path: &std::path::Path) -> PathBuf {
        let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let name = final_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("blob");
        let tmp_name = format!("{}.tmp.{}.{}", name, pid, counter);
        let mut p = final_path.to_path_buf();
        p.set_file_name(tmp_name);
        p
    }
}

impl LocalBlobStore {
    /// 0.9.0 (M4): crash-durable な tmp + rename 書き込み。
    /// schema sidecar writer (`persist_tables_to_sidecar`) と同じ標準:
    /// tmp へ write → `sync_all` → rename → (unix) 親 dir fsync。
    /// 旧実装は `fs::write` のみで fsync 無し — rename が metadata journal に
    /// 乗る一方 data page が未着だと、 crash 後に final path へ truncated blob
    /// が残った。
    fn write_via_tmp(&self, final_path: &std::path::Path, data: &[u8]) -> Result<(), BlobError> {
        use std::io::Write;
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp_path = self.tmp_path(final_path);
        let write_and_rename = || -> io::Result<()> {
            {
                let mut f = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp_path)?;
                f.write_all(data)?;
                f.sync_all()?;
            }
            // 別スレッドが先に rename してても、この rename は上書きになって安全(内容同じ)
            fs::rename(&tmp_path, final_path)?;
            // rename の directory entry 自体を永続化 (unix のみ、 失敗は無視 —
            // dir の fsync 不可な FS でも blob data 自体は sync 済み)。
            #[cfg(unix)]
            if let Some(parent) = final_path.parent() {
                if let Ok(dir) = fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
            Ok(())
        };
        write_and_rename().map_err(|e| {
            // 失敗時は tmp を片付ける
            let _ = fs::remove_file(&tmp_path);
            BlobError::from(e)
        })
    }

    /// 0.9.0 (M4): 既存 file が id の内容と一致しているか検証する。
    /// path 自体が content hash なので、 size (cheap) → sha-256 (確実) の順。
    /// crash で truncated blob が final path に残った場合の検出用。
    fn existing_blob_is_valid(final_path: &std::path::Path, id: &BlobId, expected_len: usize) -> bool {
        match fs::metadata(final_path) {
            Ok(m) if m.len() == expected_len as u64 => {}
            _ => return false,
        }
        match fs::read(final_path) {
            Ok(bytes) => BlobId::from_bytes(&bytes) == *id,
            Err(_) => false,
        }
    }
}

impl BlobStore for LocalBlobStore {
    fn put(&self, data: &[u8]) -> Result<BlobId, BlobError> {
        let id = BlobId::from_bytes(data);
        let final_path = self.path_for(&id);
        if final_path.exists() {
            // content-addressed dedup — ただし M4: 存在するだけでは信用しない。
            // crash 由来の truncated / 破損 blob なら正しい bytes で上書き修復する
            // (検証は size → 再 hash。 dedup put が read コストになる代わりに
            //  「一度壊れたら二度と直らない」 を解消)。
            if Self::existing_blob_is_valid(&final_path, &id, data.len()) {
                return Ok(id);
            }
        }
        self.write_via_tmp(&final_path, data)?;
        Ok(id)
    }

    fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, BlobError> {
        let path = self.path_for(id);
        match fs::read(&path) {
            Ok(data) => {
                let computed = BlobId::from_bytes(&data);
                if computed != *id {
                    return Err(BlobError::HashMismatch {
                        expected: *id,
                        got: computed,
                    });
                }
                Ok(Some(data))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn exists(&self, id: &BlobId) -> bool {
        self.path_for(id).exists()
    }

    /// 削除。 trait doc の通り content-addressed store に refcount は無く、
    /// この blob を参照している **全 entity** の実体が消える。 caller 側で
    /// 参照ゼロを確認してから呼ぶこと。
    fn delete(&self, id: &BlobId) -> Result<bool, BlobError> {
        let path = self.path_for(id);
        match fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

// ─── AsyncBlobStore (async-blob feature) ──────────────────────────────
//
// 動機:
//   sinfo / sinfohub-server のような async ランタイム上で動くサーバ実装は、
//   S3 / R2 / HTTP リモートへ blob を流す必要がある。これらは原理的に async
//   I/O のほうが効率的なので、enchudb 本体の sync `BlobStore` とは別軸の
//   `AsyncBlobStore` trait を併設する。
//
// 設計判断:
//   - Engine 本体は sync の `BlobStore` のままにする。Engine の hot path は
//     ns オーダーであり、async ランタイムのトランポリンを混ぜたくない。
//   - サーバ側のロジック（push 受信 → S3 アップロード等）が AsyncBlobStore を
//     直接使う。すなわち「sinfo の保存層 = sync BlobStore (engine 経由)」と
//     「サーバの転送層 = AsyncBlobStore (S3 等)」は分離する。
//   - 既存の sync 実装 (LocalBlobStore) は blanket impl で自動的に
//     AsyncBlobStore を満たす (`spawn_blocking` 経由)。専用実装は不要。
//   - 真の async 実装 (S3 等) はこの trait を直接 impl して spawn_blocking を
//     避け、AWS SDK の async I/O をそのまま使う。
//
// 機能フラグ `async-blob` を有効にしない限り tokio / async-trait に依存しない。

/// 非同期版 BlobStore。`async-blob` feature 必須。
///
/// `BlobStore` (sync) との関係:
/// - 既存の任意の `BlobStore` 実装は `spawn_blocking` 経由で自動的に
///   `AsyncBlobStore` を満たす (このファイルの blanket impl)。明示的な
///   ラッパー型は不要。
/// - 真の非同期 I/O を活かす遠隔ストア (S3 / HTTP) は本 trait を直接 impl し、
///   `BlobStore` (sync) は impl しなくてよい。
#[cfg(feature = "async-blob")]
#[async_trait::async_trait]
pub trait AsyncBlobStore: Send + Sync {
    /// `BlobStore::put` の async 版。
    async fn put(&self, data: &[u8]) -> Result<BlobId, BlobError>;

    /// `BlobStore::get` の async 版。
    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, BlobError>;

    /// `BlobStore::exists` の async 版。
    async fn exists(&self, id: &BlobId) -> bool;

    /// `BlobStore::delete` の async 版。
    async fn delete(&self, id: &BlobId) -> Result<bool, BlobError>;
}

/// 同期 `BlobStore` 実装を `AsyncBlobStore` として見せかけるアダプタ。
///
/// `tokio::task::spawn_blocking` で sync コードを blocking thread pool にオフロードし、
/// async ランタイムの実行スレッドをブロックしない。spawn コスト (~10-15µs) が乗るので、
/// 本当に非同期 I/O が欲しい遠隔ストア (S3 等) は `AsyncBlobStore` を直接 impl すべき。
///
/// 同名メソッド (`put` / `get` / `exists` / `delete`) のトレイト衝突を避けるため、
/// blanket impl ではなく明示ラッパとしている: 呼び出し側は
/// `BlockingAsyncBlobStore::new(local_store)` を介して `AsyncBlobStore` 型に持ち上げる。
#[cfg(feature = "async-blob")]
pub struct BlockingAsyncBlobStore<T: BlobStore> {
    inner: T,
}

#[cfg(feature = "async-blob")]
impl<T: BlobStore> BlockingAsyncBlobStore<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

#[cfg(feature = "async-blob")]
#[async_trait::async_trait]
impl<T> AsyncBlobStore for BlockingAsyncBlobStore<T>
where
    T: BlobStore + Clone + Send + Sync + 'static,
{
    async fn put(&self, data: &[u8]) -> Result<BlobId, BlobError> {
        let store = self.inner.clone();
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || store.put(&data))
            .await
            .map_err(|e| BlobError::Io(io::Error::other(format!("join error: {e}"))))?
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, BlobError> {
        let store = self.inner.clone();
        let id = *id;
        tokio::task::spawn_blocking(move || store.get(&id))
            .await
            .map_err(|e| BlobError::Io(io::Error::other(format!("join error: {e}"))))?
    }

    async fn exists(&self, id: &BlobId) -> bool {
        let store = self.inner.clone();
        let id = *id;
        tokio::task::spawn_blocking(move || store.exists(&id))
            .await
            .unwrap_or(false)
    }

    async fn delete(&self, id: &BlobId) -> Result<bool, BlobError> {
        let store = self.inner.clone();
        let id = *id;
        tokio::task::spawn_blocking(move || store.delete(&id))
            .await
            .map_err(|e| BlobError::Io(io::Error::other(format!("join error: {e}"))))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp_root() -> PathBuf {
        // pid + nanos + atomic counter で並列実行内でも完全 unique
        // (nanos が threads で被ると collision して別テストの blob を walk で拾い、
        //  「ファイル数 1 のはず」系 assert が壊れる)
        let n = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "enchu-blob-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            n,
        ))
    }

    #[test]
    fn blob_id_hex_roundtrip() {
        let id = BlobId::from_bytes(b"hello world");
        let hex = id.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(
            hex,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        let back = BlobId::from_hex(&hex).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn blob_id_from_hex_rejects_bad_input() {
        assert!(BlobId::from_hex("short").is_none());
        assert!(BlobId::from_hex(&"z".repeat(64)).is_none());
    }

    #[test]
    fn local_put_get_roundtrip() {
        let root = tmp_root();
        let store = LocalBlobStore::new(&root).unwrap();
        let data = b"hello blob store";
        let id = store.put(data).unwrap();
        let got = store.get(&id).unwrap();
        assert_eq!(got.as_deref(), Some(&data[..]));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_put_dedup() {
        let root = tmp_root();
        let store = LocalBlobStore::new(&root).unwrap();
        let id1 = store.put(b"same bytes").unwrap();
        let id2 = store.put(b"same bytes").unwrap();
        assert_eq!(id1, id2);
        // ファイル数確認: root/ab/cd/... で 1 ファイルのみ
        let mut count = 0;
        for p in walkdir(&root) {
            if p.is_file() {
                count += 1;
            }
        }
        assert_eq!(count, 1);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_get_missing_returns_none() {
        let root = tmp_root();
        let store = LocalBlobStore::new(&root).unwrap();
        let nonexistent = BlobId::from_bytes(b"never written");
        assert_eq!(store.get(&nonexistent).unwrap(), None);
        assert!(!store.exists(&nonexistent));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_delete() {
        let root = tmp_root();
        let store = LocalBlobStore::new(&root).unwrap();
        let id = store.put(b"to be deleted").unwrap();
        assert!(store.exists(&id));
        assert!(store.delete(&id).unwrap());
        assert!(!store.exists(&id));
        assert_eq!(store.delete(&id).unwrap(), false); // 二回目は false
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_detects_corruption() {
        let root = tmp_root();
        let store = LocalBlobStore::new(&root).unwrap();
        let id = store.put(b"original bytes").unwrap();
        // ファイルを壊す
        let path = store.path_for(&id);
        std::fs::write(&path, b"tampered bytes xxxxxxxxxxxxxxx").unwrap();
        match store.get(&id) {
            Err(BlobError::HashMismatch { .. }) => {} // 期待
            other => panic!("expected HashMismatch, got {:?}", other),
        }
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_put_repairs_truncated_blob() {
        // M4 regression: crash 由来の truncated blob が final path に居ても、
        // 正しい bytes の再 put で修復されること (旧実装は exists() 即 return で
        // 「一度壊れたら二度と直らない」)。
        let root = tmp_root();
        let store = LocalBlobStore::new(&root).unwrap();
        let data = b"full blob content that got truncated by a crash";
        let id = store.put(data).unwrap();
        // crash simulate: 前半だけ残った truncated blob
        let path = store.path_for(&id);
        std::fs::write(&path, &data[..10]).unwrap();
        assert!(matches!(store.get(&id), Err(BlobError::HashMismatch { .. })));
        // 再 put → 修復
        let id2 = store.put(data).unwrap();
        assert_eq!(id2, id);
        assert_eq!(store.get(&id).unwrap().as_deref(), Some(&data[..]));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_put_repairs_same_size_corruption() {
        // M4: size が同じでも hash 不一致 (bitrot) なら再 put で修復
        let root = tmp_root();
        let store = LocalBlobStore::new(&root).unwrap();
        let data = b"original content";
        let id = store.put(data).unwrap();
        let path = store.path_for(&id);
        std::fs::write(&path, b"corrupted conten").unwrap(); // 同じ 16 bytes
        let id2 = store.put(data).unwrap();
        assert_eq!(id2, id);
        assert_eq!(store.get(&id).unwrap().as_deref(), Some(&data[..]));
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_concurrent_put_same_blob() {
        use std::sync::Arc;
        use std::thread;
        let root = tmp_root();
        let store = Arc::new(LocalBlobStore::new(&root).unwrap());
        let data = vec![42u8; 1024 * 1024]; // 1MB
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = store.clone();
            let d = data.clone();
            handles.push(thread::spawn(move || s.put(&d).unwrap()));
        }
        let ids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // 全部同じ ID
        for id in &ids[1..] {
            assert_eq!(*id, ids[0]);
        }
        // ファイル 1 個、tmp ファイル残骸なし
        let mut blob_count = 0;
        let mut tmp_count = 0;
        for p in walkdir(&root) {
            if p.is_file() {
                let name = p.file_name().unwrap().to_str().unwrap();
                if name.contains(".tmp.") {
                    tmp_count += 1;
                } else {
                    blob_count += 1;
                }
            }
        }
        assert_eq!(blob_count, 1);
        assert_eq!(tmp_count, 0, "tmp ファイルが残ってる");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn local_large_blob_100mb() {
        let root = tmp_root();
        let store = LocalBlobStore::new(&root).unwrap();
        let data = vec![0xAB; 100 * 1024 * 1024]; // 100MB (512MB 上限を超える blob も問題なし)
        let id = store.put(&data).unwrap();
        let got = store.get(&id).unwrap().unwrap();
        assert_eq!(got.len(), data.len());
        assert_eq!(got, data);
        std::fs::remove_dir_all(&root).ok();
    }

    fn walkdir(root: &std::path::Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(p) = stack.pop() {
            if let Ok(rd) = std::fs::read_dir(&p) {
                for entry in rd.flatten() {
                    let ep = entry.path();
                    if ep.is_dir() {
                        stack.push(ep);
                    } else {
                        out.push(ep);
                    }
                }
            }
        }
        out
    }
}

#[cfg(all(test, feature = "async-blob"))]
mod async_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp_root() -> PathBuf {
        let n = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "enchu-blob-async-test-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            n,
        ))
    }

    /// BlockingAsyncBlobStore 経由で LocalBlobStore が AsyncBlobStore を満たすことの確認。
    #[tokio::test]
    async fn local_via_blocking_adapter_put_get_roundtrip() {
        let root = tmp_root();
        let store: Arc<dyn AsyncBlobStore> = Arc::new(BlockingAsyncBlobStore::new(
            LocalBlobStore::new(&root).unwrap(),
        ));
        let id = store.put(b"hello async blob").await.unwrap();
        let got = store.get(&id).await.unwrap();
        assert_eq!(got.as_deref(), Some(&b"hello async blob"[..]));
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn local_async_exists_and_delete() {
        let root = tmp_root();
        let inner = LocalBlobStore::new(&root).unwrap();
        let id = inner.put(b"transient").unwrap(); // sync put で書く
        let store = BlockingAsyncBlobStore::new(inner);
        assert!(store.exists(&id).await);
        assert!(store.delete(&id).await.unwrap());
        assert!(!store.exists(&id).await);
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn local_async_get_missing_returns_none() {
        let root = tmp_root();
        let store = BlockingAsyncBlobStore::new(LocalBlobStore::new(&root).unwrap());
        let id = BlobId::from_bytes(b"never written async");
        assert_eq!(store.get(&id).await.unwrap(), None);
        std::fs::remove_dir_all(&root).ok();
    }

    /// 並列 spawn から AsyncBlobStore を叩く: spawn_blocking 経由でも race なし。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_async_concurrent_put_dedup() {
        let root = tmp_root();
        let store: Arc<dyn AsyncBlobStore> = Arc::new(BlockingAsyncBlobStore::new(
            LocalBlobStore::new(&root).unwrap(),
        ));
        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = store.clone();
            let d = data.clone();
            handles.push(tokio::spawn(async move { s.put(&d).await.unwrap() }));
        }
        let mut ids = Vec::with_capacity(handles.len());
        for h in handles {
            ids.push(h.await.unwrap());
        }
        for id in &ids[1..] {
            assert_eq!(*id, ids[0]);
        }
        std::fs::remove_dir_all(&root).ok();
    }
}
