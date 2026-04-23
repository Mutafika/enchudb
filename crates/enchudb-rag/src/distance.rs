//! ベクトル距離関数。
//!
//! すべて f32 スライス前提。次元チェックは呼び出し側責任（ホットパスで毎回 len 比較しない）。
//!
//! - `cosine`: 1 - cos類似度。0 が完全一致、2 が真逆。
//! - `cosine_sim`: cos 類似度そのもの。1 が完全一致、-1 が真逆。ランキング用。
//! - `dot`: 内積。事前に正規化されていれば cos 類似度と一致。
//! - `l2`: L2 距離の二乗。sqrt は rank には不要なので省略。
//!
//! # SIMD
//!
//! 現在は autovectorization 任せ（rustc が展開する）。手書き SIMD は
//! feature = "simd" で x86_64 AVX2 を後で足す余地を残してある。

/// cos 類似度（-1..=1、大きいほど近い）。
#[inline]
pub fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        let x = unsafe { *a.get_unchecked(i) };
        let y = unsafe { *b.get_unchecked(i) };
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = (na * nb).sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}

/// cos 距離（1 - cos 類似度、0..=2、小さいほど近い）。
/// 浮動小数誤差で負になるのを避けるため 0 下限でクランプ。
#[inline]
pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    (1.0 - cosine_sim(a, b)).max(0.0)
}

/// 内積（正規化済みなら cos 類似度そのもの）。
#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut acc = 0.0f32;
    for i in 0..a.len() {
        acc += unsafe { *a.get_unchecked(i) * *b.get_unchecked(i) };
    }
    acc
}

/// L2 距離の二乗。
#[inline]
pub fn l2(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut acc = 0.0f32;
    for i in 0..a.len() {
        let d = unsafe { *a.get_unchecked(i) - *b.get_unchecked(i) };
        acc += d * d;
    }
    acc
}

/// ベクトル正規化（in-place）。norm が 0 なら何もしない。
pub fn normalize(v: &mut [f32]) {
    let mut n = 0.0f32;
    for x in v.iter() { n += x * x; }
    let n = n.sqrt();
    if n > 0.0 {
        let inv = 1.0 / n;
        for x in v.iter_mut() { *x *= inv; }
    }
}

/// 距離関数（スコアは「小さいほど近い」）。
#[derive(Clone, Copy, Debug)]
pub enum Metric {
    Cosine,
    L2,
    /// 内積。正規化済みベクトル向け。内部的には -dot に反転して「小さいほど近い」にする。
    Dot,
}

impl Metric {
    #[inline]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::Cosine => cosine(a, b),
            Metric::L2 => l2(a, b),
            Metric::Dot => -dot(a, b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_identity() {
        let a = [1.0, 2.0, 3.0];
        assert!(cosine(&a, &a).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        assert!((cosine(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_opposite() {
        let a = [1.0, 0.0];
        let b = [-1.0, 0.0];
        assert!((cosine(&a, &b) - 2.0).abs() < 1e-6);
    }

    #[test]
    fn l2_zero() {
        let a = [1.0, 2.0, 3.0];
        assert_eq!(l2(&a, &a), 0.0);
    }

    #[test]
    fn normalize_unit() {
        let mut v = vec![3.0, 4.0];
        normalize(&mut v);
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n - 1.0).abs() < 1e-6);
    }
}
