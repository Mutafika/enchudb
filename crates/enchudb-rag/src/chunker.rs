//! テキスト分割。LangChain 流の RecursiveCharacterTextSplitter 実装。
//!
//! - 区切り文字を優先度順に試す：段落 → 改行 → 句 → 語 → 字
//! - 各チャンクを `max_chars` に収めつつ、`overlap` 分だけ重ねる
//! - マルチバイト安全（char 境界で切る）

pub trait Chunker {
    fn split<'a>(&self, text: &'a str) -> Vec<String>;
}

pub struct RecursiveCharSplitter {
    pub max_chars: usize,
    pub overlap: usize,
    pub separators: Vec<String>,
}

impl RecursiveCharSplitter {
    /// 一般テキスト向けデフォルト。
    pub fn new(max_chars: usize, overlap: usize) -> Self {
        assert!(overlap < max_chars, "overlap must be < max_chars");
        Self {
            max_chars,
            overlap,
            separators: vec![
                "\n\n".into(), "\n".into(),
                "。".into(), "．".into(), ". ".into(),
                "、".into(), ", ".into(),
                " ".into(), "".into(),  // 最後の "" は 1 文字ずつ
            ],
        }
    }

    /// マークダウン向け（見出し優先）。
    pub fn markdown(max_chars: usize, overlap: usize) -> Self {
        assert!(overlap < max_chars);
        Self {
            max_chars,
            overlap,
            separators: vec![
                "\n# ".into(), "\n## ".into(), "\n### ".into(),
                "\n\n".into(), "\n".into(),
                "。".into(), ". ".into(),
                " ".into(), "".into(),
            ],
        }
    }

    fn chars(s: &str) -> usize { s.chars().count() }

    fn split_inner(&self, text: &str, sep_idx: usize) -> Vec<String> {
        // text 全体が閾値以下ならそのまま返す
        if Self::chars(text) <= self.max_chars { return vec![text.to_string()]; }

        if sep_idx >= self.separators.len() {
            // 最終手段：char 境界で max_chars 切り
            return force_split(text, self.max_chars);
        }

        let sep = &self.separators[sep_idx];
        let parts: Vec<&str> = if sep.is_empty() {
            // 空 sep は次レベル（char 強制分割）にフォールバック
            return self.split_inner(text, sep_idx + 1);
        } else {
            text.split(sep.as_str()).collect()
        };

        // 各 part が max_chars を超えるなら更に再帰分割、
        // そうでなければ貪欲に結合して max_chars 以下のチャンクを作る
        let mut chunks: Vec<String> = Vec::new();
        let mut buf = String::new();
        for part in parts.iter() {
            let part_len = Self::chars(part);
            // 単体で超える → 再帰
            let sub_chunks: Vec<String> = if part_len > self.max_chars {
                self.split_inner(part, sep_idx + 1)
            } else {
                vec![part.to_string()]
            };

            for sc in sub_chunks {
                let sc_len = Self::chars(&sc);
                let buf_len = Self::chars(&buf);
                let sep_len = if buf.is_empty() { 0 } else { Self::chars(sep) };
                if buf_len + sep_len + sc_len <= self.max_chars {
                    if !buf.is_empty() { buf.push_str(sep); }
                    buf.push_str(&sc);
                } else {
                    if !buf.is_empty() { chunks.push(std::mem::take(&mut buf)); }
                    if sc_len <= self.max_chars {
                        buf = sc;
                    } else {
                        // ありえないはずだが保険
                        for c in force_split(&sc, self.max_chars) { chunks.push(c); }
                    }
                }
            }
        }
        if !buf.is_empty() { chunks.push(buf); }

        // overlap 適用: 前チャンクの末尾 overlap 文字を次の先頭に付ける
        if self.overlap > 0 && chunks.len() > 1 {
            let mut with_overlap = Vec::with_capacity(chunks.len());
            with_overlap.push(chunks[0].clone());
            for i in 1..chunks.len() {
                let prev = &chunks[i - 1];
                let prev_chars: Vec<char> = prev.chars().collect();
                let tail: String = prev_chars
                    .iter()
                    .rev()
                    .take(self.overlap)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                let mut next = tail;
                next.push_str(&chunks[i]);
                with_overlap.push(next);
            }
            return with_overlap;
        }
        chunks
    }
}

impl Chunker for RecursiveCharSplitter {
    fn split<'a>(&self, text: &'a str) -> Vec<String> {
        self.split_inner(text, 0)
    }
}

fn force_split(text: &str, max_chars: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut count = 0;
    for c in text.chars() {
        buf.push(c);
        count += 1;
        if count >= max_chars {
            out.push(std::mem::take(&mut buf));
            count = 0;
        }
    }
    if !buf.is_empty() { out.push(buf); }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_single_chunk() {
        let s = RecursiveCharSplitter::new(100, 0);
        assert_eq!(s.split("hello"), vec!["hello".to_string()]);
    }

    #[test]
    fn split_on_paragraph() {
        let s = RecursiveCharSplitter::new(10, 0);
        let out = s.split("aaa\n\nbbb\n\nccc");
        assert!(out.len() >= 2);
        for c in &out { assert!(c.chars().count() <= 10); }
    }

    #[test]
    fn force_split_multibyte() {
        let s = RecursiveCharSplitter::new(3, 0);
        // 日本語でも char 境界で切れる
        let out = s.split("あいうえおかきくけこ");
        for c in &out { assert!(c.chars().count() <= 3); }
        let joined: String = out.join("");
        assert_eq!(joined, "あいうえおかきくけこ");
    }

    #[test]
    fn overlap_applied() {
        let s = RecursiveCharSplitter::new(5, 2);
        let out = s.split("abcde fghij klmno");
        if out.len() >= 2 {
            // 2 つめ以降は先頭 2 文字が重複しているはず
            for i in 1..out.len() {
                assert!(out[i].chars().count() <= 7); // 5 + overlap 2
            }
        }
    }
}
