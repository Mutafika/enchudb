//! Embedder trait。
//!
//! RagStore はベクトル化の詳細には関知しない。呼び出し側が
//! Embedder を用意して、そのベクトルを Chunk に詰めて insert する。
//!
//! 提供されるのはトレイトと、テスト/開発用の Hash ベース擬似エンベッダだけ。
//! 実プロダクトでは OpenAI / Candle / fastembed 等を呼び出す実装を各自書く。

/// 文字列をベクトルに変換する。
pub trait Embedder: Send + Sync {
    fn dim(&self) -> usize;

    fn embed(&self, text: &str) -> Vec<f32>;

    /// バッチ版。デフォルトは単発を回すだけ。SDK 側で batch endpoint がある場合は override。
    fn embed_batch(&self, texts: &[&str]) -> Vec<Vec<f32>> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

/// テスト/開発用。FNV-1a ハッシュを正弦波に通して決定的ベクトルを作る。
/// セマンティック類似は持たないが、同じテキストは常に同じベクトルになるので
/// 単体テスト・統合テストには十分。
pub struct HashEmbedder {
    pub dim: usize,
}

impl HashEmbedder {
    pub fn new(dim: usize) -> Self { Self { dim } }
}

impl Embedder for HashEmbedder {
    fn dim(&self) -> usize { self.dim }

    fn embed(&self, text: &str) -> Vec<f32> {
        let mut v = vec![0.0f32; self.dim];
        // token レベルの FNV-1a。空白区切りトークンごとに異なるシードで成分を足す。
        let tokens: Vec<&str> = text.split_whitespace().collect();
        if tokens.is_empty() {
            // 空文字列は全 0 回避のため 1 次元だけ立てる
            if self.dim > 0 { v[0] = 1.0; }
            return v;
        }
        for tok in tokens {
            let base = fnv1a(tok.as_bytes());
            let mut h = base;
            for i in 0..self.dim {
                // 次元ごとに独立した擬似ランダムを発生させる（xorshift64* でミックス）
                h ^= h >> 12;
                h ^= h << 25;
                h ^= h >> 27;
                let mixed = h.wrapping_mul(0x2545f4914f6cdd1d);
                // [-1, 1]
                let x = ((mixed >> 11) as f64 / (1u64 << 53) as f64) as f32 * 2.0 - 1.0;
                v[i] += x;
            }
        }
        // 正規化（cos 類似が dot に落ちる）
        crate::distance::normalize(&mut v);
        v
    }
}

#[inline]
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

// ---- fastembed 実装 ----

/// fastembed (ONNX 経由) のラッパ。feature flag `fastembed` で有効化。
///
/// モデル例:
/// - `BGESmallENV15` — 384 dim、英語
/// - `MultilingualE5Small` — 384 dim、多言語（日本語可）
/// - `BGELargeENV15` — 1024 dim、高精度
///
/// 初回 `new()` で hf-hub からモデル DL、以後はローカルキャッシュ (`~/.cache/fastembed`)。
#[cfg(feature = "fastembed")]
pub struct FastEmbedder {
    model: std::sync::Mutex<fastembed::TextEmbedding>,
    dim: usize,
}

#[cfg(feature = "fastembed")]
impl FastEmbedder {
    /// デフォルト（BGESmallENV15、384 dim、英語）で作る。
    pub fn new() -> Result<Self, fastembed::Error> {
        Self::with_model(fastembed::EmbeddingModel::BGESmallENV15)
    }

    /// モデル指定で作る。
    pub fn with_model(model: fastembed::EmbeddingModel) -> Result<Self, fastembed::Error> {
        let opts = fastembed::InitOptions::new(model.clone()).with_show_download_progress(true);
        let te = fastembed::TextEmbedding::try_new(opts)?;
        let info = fastembed::TextEmbedding::get_model_info(&model)?;
        Ok(Self {
            model: std::sync::Mutex::new(te),
            dim: info.dim,
        })
    }
}

#[cfg(feature = "fastembed")]
impl Embedder for FastEmbedder {
    fn dim(&self) -> usize { self.dim }

    fn embed(&self, text: &str) -> Vec<f32> {
        let mut m = self.model.lock().unwrap();
        // fastembed は Vec<&str> を受ける
        let out = m.embed(vec![text], None).expect("fastembed embed failed");
        out.into_iter().next().unwrap_or_else(|| vec![0.0; self.dim])
    }

    fn embed_batch(&self, texts: &[&str]) -> Vec<Vec<f32>> {
        if texts.is_empty() { return Vec::new(); }
        let mut m = self.model.lock().unwrap();
        m.embed(texts.to_vec(), None).expect("fastembed embed_batch failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_embedder_deterministic() {
        let e = HashEmbedder::new(64);
        let a = e.embed("hello world");
        let b = e.embed("hello world");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_embedder_different_text() {
        let e = HashEmbedder::new(64);
        let a = e.embed("hello world");
        let b = e.embed("goodbye world");
        assert_ne!(a, b);
    }

    #[test]
    fn hash_embedder_dim() {
        let e = HashEmbedder::new(128);
        assert_eq!(e.embed("x").len(), 128);
    }
}
