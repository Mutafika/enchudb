//! n-gram の抽出と key encoding（n 可変、#121）。
//!
//! key は **文字を上位から 16bit ずつ詰めた u64**。BMP (U+0000..=U+FFFF) の範囲は
//! 完全一致で表現でき、`n ≤ 4` なら 64bit に収まる = **衝突ゼロ**。hash に潰さない
//! ので「1 gram ちょうどのクエリは候補がそのまま正確一致」という上位層の最適化
//! （`enchudb-textsearch`）が n に関係なくそのまま成立する。
//!
//! n = 2 の key は旧 `bigram::to_key` の u32 値と**同じビット列**（上位 32bit が 0）。
//! これにより version 2 の `.etxt`（key u32）を u64 へゼロ拡張するだけで読めて、
//! 昇順ソート順も保たれる。
//!
//! BMP 外の文字（emoji 等、U+10000 以上）は下位 16bit に切り詰める。切り詰めた者
//! 同士は衝突しうるが、それは n = 2 時代からの既知の挙動で n の一般化とは独立。

use std::io;

/// 対応する n の下限。1-gram は「文字単位の転置」で用途が違うので扱わない。
pub const MIN_N: usize = 2;

/// 対応する n の上限。16bit × 4 = 64bit で u64 key にちょうど収まる限界。
/// これを超えると exact 表現ができず hash に潰す必要があり、偽陽性が生じる。
pub const MAX_N: usize = 4;

/// 既定の n（現行互換）。
pub const DEFAULT_N: usize = 2;

/// n が対応範囲かを検査する。範囲外は `InvalidInput`。
pub fn validate_n(n: usize) -> io::Result<usize> {
    if !(MIN_N..=MAX_N).contains(&n) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "ngram n = {n} は非対応 ({MIN_N}..={MAX_N})。\
                 上限 {MAX_N} は「文字 16bit × n が u64 key に収まる = 衝突ゼロ」の境界"
            ),
        ));
    }
    Ok(n)
}

/// 文字列を 1 個の key に詰める（`chars.len()` が n に相当）。
/// 上位から順に 16bit ずつ。空 slice は 0。
#[inline]
pub fn pack(chars: &[char]) -> u64 {
    let mut acc = 0u64;
    for &c in chars {
        acc = (acc << 16) | ((c as u32 & 0xFFFF) as u64);
    }
    acc
}

/// 文字列を n-gram key 列へ。Unicode 文字単位のスライディングウィンドウ。
/// `"国民は法"` を n=2 で → `["国民", "民は", "は法"]` 相当の 3 key。
///
/// 文字数が n 未満、または n が範囲外なら空。ウィンドウは転がすので O(文字数)。
pub fn extract_keys(text: &str, n: usize) -> Vec<u64> {
    if !(MIN_N..=MAX_N).contains(&n) {
        return vec![];
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() < n {
        return vec![];
    }
    // n = 4 のとき 1 << 64 は overflow するので分岐する
    let mask: u64 = if n == 4 { u64::MAX } else { (1u64 << (16 * n)) - 1 };
    let mut out = Vec::with_capacity(chars.len() - n + 1);
    let mut acc = 0u64;
    for (i, &c) in chars.iter().enumerate() {
        acc = ((acc << 16) | ((c as u32 & 0xFFFF) as u64)) & mask;
        if i + 1 >= n {
            out.push(acc);
        }
    }
    out
}

/// key → gram 文字列（デバッグ / 診断用）。切り詰められた非 BMP 文字を含む key は
/// `char::from_u32` が通れば復元されるが、元の非 BMP 文字には戻らない。
pub fn key_to_string(key: u64, n: usize) -> Option<String> {
    if !(MIN_N..=MAX_N).contains(&n) {
        return None;
    }
    let mut s = String::with_capacity(n);
    for i in (0..n).rev() {
        let cp = ((key >> (16 * i)) & 0xFFFF) as u32;
        s.push(char::from_u32(cp)?);
    }
    Some(s)
}

/// クエリを index で絞り込めるか（= 文字数が n 以上か）。
/// n 未満のクエリは gram を作れないので全走査に落ちる。
#[inline]
pub fn query_len(query: &str) -> usize {
    query.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_matches_window_count() {
        // "国民は法" = 4 文字 → n=2 で 3、n=3 で 2、n=4 で 1
        assert_eq!(extract_keys("国民は法", 2).len(), 3);
        assert_eq!(extract_keys("国民は法", 3).len(), 2);
        assert_eq!(extract_keys("国民は法", 4).len(), 1);
        // 文字数 < n は空
        assert_eq!(extract_keys("国民", 3).len(), 0);
        assert_eq!(extract_keys("", 2).len(), 0);
    }

    #[test]
    fn key_roundtrip_all_n() {
        for (text, n, want) in [
            ("国民", 2, "国民"),
            ("国民は", 3, "国民は"),
            ("国民は法", 4, "国民は法"),
            ("abcd", 4, "abcd"),
        ] {
            let keys = extract_keys(text, n);
            assert_eq!(keys.len(), 1, "{text} / n={n}");
            assert_eq!(key_to_string(keys[0], n).as_deref(), Some(want));
        }
    }

    #[test]
    fn n2_key_is_bit_identical_to_legacy_u32() {
        // v2 の .etxt (key u32) をゼロ拡張して読めることの根拠。
        for text in ["国民", "ab", "法の"] {
            let legacy = crate::bigram::to_key(crate::bigram::extract(text)[0]) as u64;
            assert_eq!(extract_keys(text, 2)[0], legacy, "{text}");
        }
    }

    #[test]
    fn n2_keys_never_exceed_u32() {
        // v2 format へ書き出せることの前提 (key が 32bit に収まる)。
        for k in extract_keys("国民は法の下に平等であってabcXYZ", 2) {
            assert!(k <= u32::MAX as u64, "key {k:#x} が u32 を超えた");
        }
    }

    #[test]
    fn rolling_window_equals_pack() {
        // 転がし実装が「毎回 pack し直す」ナイーブ実装と一致すること。
        let text = "国民は法の下に平等であって";
        let chars: Vec<char> = text.chars().collect();
        for n in MIN_N..=MAX_N {
            let rolled = extract_keys(text, n);
            let naive: Vec<u64> = chars.windows(n).map(pack).collect();
            assert_eq!(rolled, naive, "n={n}");
        }
    }

    #[test]
    fn distinct_keys_across_n() {
        // 別 n の同じ文字列が同じ key にならないこと (n は index 側が固定するので
        // 混ざらないが、key 空間としても分離しているのが望ましい)。
        assert_ne!(extract_keys("国民", 2)[0], extract_keys("国民は", 3)[0]);
    }

    #[test]
    fn validate_n_range() {
        assert!(validate_n(1).is_err());
        assert!(validate_n(5).is_err());
        for n in MIN_N..=MAX_N {
            assert_eq!(validate_n(n).unwrap(), n);
        }
        let e = validate_n(5).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        assert!(e.to_string().contains('4'), "上限を示すこと: {e}");
    }

    #[test]
    fn out_of_range_n_extracts_nothing() {
        assert!(extract_keys("国民は法の下", 1).is_empty());
        assert!(extract_keys("国民は法の下", 5).is_empty());
        assert_eq!(key_to_string(0, 5), None);
    }

    #[test]
    fn non_bmp_truncates_to_low_16_bits() {
        // 既知の挙動 (n=2 時代から)。panic せず key 化されること。
        let keys = extract_keys("😀😀", 2);
        assert_eq!(keys.len(), 1);
        let low = ('😀' as u32 & 0xFFFF) as u64;
        assert_eq!(keys[0], (low << 16) | low);
    }
}
