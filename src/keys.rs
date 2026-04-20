//! Peer 鍵管理 — ed25519 鍵ペアの生成/保存/署名/検証。
//!
//! # 配置
//!
//! `{db_path}.key` に 32 バイトの ed25519 秘密鍵を書く(rw-------)。
//! ファイルが無ければ生成、あれば読み込む。
//!
//! # セキュリティ
//!
//! - 秘密鍵は平文。OS ファイル権限(0o600)で保護。暗号化は Phase D 以降。
//! - 鍵を失うと、その peer の書き込み権限は失われる(他 peer の ACL に載ってるので、
//!   新しい鍵を再配布しない限り TOFU 扱いになる)。
//!
//! # TOFU (Trust On First Use) pubkey store
//!
//! `PubkeyStore` は peer_id → ed25519 公開鍵 の HashMap。
//! 最初に見た署名で peer_id を公開鍵に bind し、以降変更があれば reject。

use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey, SECRET_KEY_LENGTH, SIGNATURE_LENGTH};

use crate::PeerId;

/// 単一 peer の鍵ペア。
pub struct Keypair {
    signing: SigningKey,
}

impl Keypair {
    /// 新しい鍵ペアを OS の CSPRNG から生成。
    pub fn generate() -> Self {
        let mut rng = rand_core::OsRng;
        let signing = SigningKey::generate(&mut rng);
        Self { signing }
    }

    /// 32B の raw 秘密鍵から復元。
    pub fn from_bytes(bytes: &[u8; SECRET_KEY_LENGTH]) -> Self {
        let signing = SigningKey::from_bytes(bytes);
        Self { signing }
    }

    /// raw 秘密鍵(32B)。
    pub fn secret_bytes(&self) -> [u8; SECRET_KEY_LENGTH] {
        self.signing.to_bytes()
    }

    /// 公開鍵(32B)。
    pub fn public_bytes(&self) -> [u8; 32] {
        self.signing.verifying_key().to_bytes()
    }

    /// 公開鍵 fingerprint(先頭 8B)。WAL ヘッダの pubkey_fp に入る。
    pub fn pubkey_fp(&self) -> [u8; 8] {
        let pk = self.public_bytes();
        let mut fp = [0u8; 8];
        fp.copy_from_slice(&pk[..8]);
        fp
    }

    /// message に署名(64B)。
    pub fn sign(&self, msg: &[u8]) -> [u8; SIGNATURE_LENGTH] {
        self.signing.sign(msg).to_bytes()
    }

    /// 鍵ファイルから読み込む。なければ生成して書き出す。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_or_create(path: &Path) -> std::io::Result<Self> {
        use std::io::Read;
        if path.exists() {
            let mut f = std::fs::File::open(path)?;
            let mut buf = [0u8; SECRET_KEY_LENGTH];
            f.read_exact(&mut buf)?;
            Ok(Self::from_bytes(&buf))
        } else {
            let kp = Self::generate();
            kp.save(path)?;
            Ok(kp)
        }
    }

    /// 鍵ファイルへ書き出し。owner-only 権限で。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(&self.secret_bytes())?;
        Ok(())
    }
}

/// 受信側で pubkey を検証するための peer_id → VerifyingKey の TOFU ストア。
pub struct PubkeyStore {
    inner: RwLock<HashMap<PeerId, VerifyingKey>>,
}

impl Default for PubkeyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustOutcome {
    /// 初めて見る peer、記憶した。
    TrustedOnFirstUse,
    /// 既知 peer、公開鍵一致。
    Verified,
    /// 既知 peer だが公開鍵が変わっている。reject。
    PubkeyMismatch,
    /// 公開鍵のフォーマットが不正。
    InvalidPubkey,
}

impl PubkeyStore {
    pub fn new() -> Self {
        Self { inner: RwLock::new(HashMap::new()) }
    }

    /// TOFU で bind する。既存と衝突したら reject。
    pub fn trust_or_verify(&self, peer: PeerId, pubkey_bytes: &[u8; 32]) -> TrustOutcome {
        let vk = match VerifyingKey::from_bytes(pubkey_bytes) {
            Ok(k) => k,
            Err(_) => return TrustOutcome::InvalidPubkey,
        };
        let mut guard = self.inner.write().unwrap();
        match guard.get(&peer) {
            None => {
                guard.insert(peer, vk);
                TrustOutcome::TrustedOnFirstUse
            }
            Some(existing) if existing.to_bytes() == vk.to_bytes() => TrustOutcome::Verified,
            Some(_) => TrustOutcome::PubkeyMismatch,
        }
    }

    /// peer の VerifyingKey を取得。未登録なら None。
    pub fn get(&self, peer: PeerId) -> Option<VerifyingKey> {
        self.inner.read().unwrap().get(&peer).copied()
    }

    /// signature を検証。VerifyingKey が無ければ Err。
    pub fn verify(&self, peer: PeerId, msg: &[u8], sig_bytes: &[u8; SIGNATURE_LENGTH]) -> bool {
        let vk = match self.get(peer) {
            Some(k) => k,
            None => return false,
        };
        let sig = match Signature::from_slice(sig_bytes) {
            Ok(s) => s,
            Err(_) => return false,
        };
        vk.verify(msg, &sig).is_ok()
    }

    /// 明示的に peer の pubkey を登録/上書き(運用時の鍵ローテーション用)。
    pub fn force_register(&self, peer: PeerId, pubkey_bytes: &[u8; 32]) -> bool {
        match VerifyingKey::from_bytes(pubkey_bytes) {
            Ok(vk) => {
                self.inner.write().unwrap().insert(peer, vk);
                true
            }
            Err(_) => false,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_roundtrip() {
        let kp = Keypair::generate();
        let msg = b"hello distributed world";
        let sig = kp.sign(msg);

        let store = PubkeyStore::new();
        let outcome = store.trust_or_verify(1, &kp.public_bytes());
        assert_eq!(outcome, TrustOutcome::TrustedOnFirstUse);

        assert!(store.verify(1, msg, &sig));
    }

    #[test]
    fn tampered_message_fails_verify() {
        let kp = Keypair::generate();
        let sig = kp.sign(b"original");
        let store = PubkeyStore::new();
        store.trust_or_verify(1, &kp.public_bytes());
        // 違う message は verify 失敗
        assert!(!store.verify(1, b"tampered", &sig));
    }

    #[test]
    fn tofu_rejects_changed_pubkey() {
        let kp1 = Keypair::generate();
        let kp2 = Keypair::generate();
        let store = PubkeyStore::new();
        assert_eq!(store.trust_or_verify(1, &kp1.public_bytes()), TrustOutcome::TrustedOnFirstUse);
        assert_eq!(store.trust_or_verify(1, &kp1.public_bytes()), TrustOutcome::Verified);
        assert_eq!(store.trust_or_verify(1, &kp2.public_bytes()), TrustOutcome::PubkeyMismatch);
    }

    #[test]
    fn verify_unknown_peer_returns_false() {
        let kp = Keypair::generate();
        let sig = kp.sign(b"x");
        let store = PubkeyStore::new();
        // 登録前 → verify 失敗
        assert!(!store.verify(1, b"x", &sig));
    }

    #[test]
    fn pubkey_fp_is_first_8_bytes() {
        let kp = Keypair::generate();
        let pk = kp.public_bytes();
        let fp = kp.pubkey_fp();
        assert_eq!(&fp[..], &pk[..8]);
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn load_or_create_persists_key() {
        let dir = std::env::temp_dir().join(format!("enchudb_key_test_{}", std::process::id()));
        let _ = std::fs::remove_file(&dir);

        let kp1 = Keypair::load_or_create(&dir).unwrap();
        let pk1 = kp1.public_bytes();

        let kp2 = Keypair::load_or_create(&dir).unwrap();
        let pk2 = kp2.public_bytes();
        assert_eq!(pk1, pk2, "load should return same key");

        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn from_bytes_preserves_identity() {
        let kp = Keypair::generate();
        let secret = kp.secret_bytes();
        let kp2 = Keypair::from_bytes(&secret);
        assert_eq!(kp.public_bytes(), kp2.public_bytes());

        let msg = b"identity test";
        // 同じ message に同じ署名(ed25519 は deterministic)
        assert_eq!(kp.sign(msg), kp2.sign(msg));
    }
}
