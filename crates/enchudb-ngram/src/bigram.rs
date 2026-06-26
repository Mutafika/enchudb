/// 文字列を bigram に分割。Unicode 文字単位。
/// "国民は法" → ["国民", "民は", "は法"]
pub fn extract(text: &str) -> Vec<[char; 2]> {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() < 2 {
        return vec![];
    }
    chars.windows(2).map(|w| [w[0], w[1]]).collect()
}

/// bigram → u32 キー。2 文字を 16bit ずつ pack。
/// BMP 外の文字（emoji 等）は下位 16bit に切り詰め。
#[inline]
pub fn to_key(bg: [char; 2]) -> u32 {
    let a = (bg[0] as u32) & 0xFFFF;
    let b = (bg[1] as u32) & 0xFFFF;
    (a << 16) | b
}

/// u32 キー → bigram 文字列（デバッグ用）
pub fn from_key(key: u32) -> Option<String> {
    let a = char::from_u32((key >> 16) & 0xFFFF)?;
    let b = char::from_u32(key & 0xFFFF)?;
    Some(format!("{a}{b}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_basic() {
        let bgs = extract("国民は法");
        assert_eq!(bgs.len(), 3);
        assert_eq!(bgs[0], ['国', '民']);
        assert_eq!(bgs[1], ['民', 'は']);
        assert_eq!(bgs[2], ['は', '法']);
    }

    #[test]
    fn extract_short() {
        assert_eq!(extract("あ").len(), 0);
        assert_eq!(extract("").len(), 0);
        assert_eq!(extract("ab").len(), 1);
    }

    #[test]
    fn key_roundtrip() {
        let bg = ['国', '民'];
        let key = to_key(bg);
        assert_eq!(from_key(key), Some("国民".to_string()));
    }

    #[test]
    fn key_ascii() {
        let bg = ['a', 'b'];
        let key = to_key(bg);
        assert_eq!(from_key(key), Some("ab".to_string()));
    }
}
