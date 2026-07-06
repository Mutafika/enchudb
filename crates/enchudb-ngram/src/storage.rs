//! mmap ファイル形式 (version 2)
//!
//! [Header 32 bytes]
//!   magic: "ETXT" (4)
//!   version: u32 (4)  — 2 (eid u64 化に伴う break。v1 は読まない)
//!   bigram_count: u32 (4)
//!   posting_total: u32 (4) — entity ID エントリ総数（バイト数ではない）
//!   doc_count: u32 (4)
//!   text_total: u32 (4) — テキストデータ総バイト数
//!   flags: u8 (1) — bit0 TEXT_OMITTED: Doc Index / Text Data を持たない
//!                   postings-only index (原文は DB 本体が所有、検証は caller 側 #84)
//!   _reserved: [u8; 7]
//!
//! [Bigram Index] — bigram_count × 12 bytes
//!   key: u32, offset: u32, len: u32
//!   key 昇順ソート（二分探索用）
//!   offset/len は Posting Data 内のエントリ単位（byte 単位ではない）
//!
//! [Padding] — 0..=7 bytes
//!   Posting Data の先頭を 8-byte 境界に揃えるための詰め物。
//!   現状の reader は from_le_bytes でアライメント非依存に読むので必須ではないが、
//!   将来 mmap 上で u64 slice cast に戻す余地を残すため format として保持する。
//!
//! [Posting Data] — posting_total × 8 bytes
//!   flat array of u64 entity IDs (little-endian)
//!
//! [Doc Index] — doc_count × 16 bytes
//!   eid: u64, offset: u32, len: u32
//!   eid 昇順ソート
//!
//! [Text Data] — text_total bytes

use std::io;
use std::path::Path;

#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
#[cfg(not(target_arch = "wasm32"))]
use memmap2::Mmap;

use std::io::Write;

const MAGIC: &[u8; 4] = b"ETXT";
const VERSION: u32 = 2;
const HEADER_SIZE: usize = 32;
const BIGRAM_ENTRY: usize = 12;       // key u32 + offset u32 + len u32
const POSTING_ENTRY: usize = 8;       // eid u64
const DOC_ENTRY: usize = 16;          // eid u64 + offset u32 + len u32

/// header の flags byte (`_reserved` の先頭 = buf[24])。 立っていれば Doc Index /
/// Text Data 無しの postings-only index。 旧 v2 file は reserved 全 0 = 原文保持、
/// で自然に後方互換 (#84)。
const FLAG_TEXT_OMITTED: u8 = 0x01;
const FLAGS_OFFSET: usize = 24;

/// Posting Data 先頭を 8-byte 境界に揃えるためのパディング量
#[inline]
fn posting_padding(bigram_count: u32) -> usize {
    let after_bigrams = HEADER_SIZE + (bigram_count as usize) * BIGRAM_ENTRY;
    (8 - (after_bigrams % 8)) % 8
}

/// 永続化バックエンド。native は mmap、wasm は Vec<u8>（fetch 結果を所有）。
enum Backing {
    #[cfg(not(target_arch = "wasm32"))]
    Mmap(Mmap),
    Bytes(Vec<u8>),
}

impl Backing {
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Mmap(m) => m,
            Backing::Bytes(v) => v.as_slice(),
        }
    }
}

/// 読み取り専用インデックス。
pub struct MappedIndex {
    backing: Backing,
    bigram_count: u32,
    posting_total: u32,
    doc_count: u32,
    text_total: u32,
    /// FLAG_TEXT_OMITTED が立っていれば原文非保持 (postings-only)。
    text_omitted: bool,
}

#[inline]
fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

impl MappedIndex {
    /// ファイルを mmap で開く（native のみ）。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Self::from_backing(Backing::Mmap(mmap))
    }

    /// 既存のバイト列から開く。wasm でも動く（fetch 後のレスポンスを直接渡す）。
    pub fn from_bytes(bytes: Vec<u8>) -> io::Result<Self> {
        Self::from_backing(Backing::Bytes(bytes))
    }

    fn from_backing(backing: Backing) -> io::Result<Self> {
        let buf = backing.as_slice();
        if buf.len() < HEADER_SIZE || &buf[0..4] != MAGIC {
            return Err(invalid_data("not an ETXT file"));
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(invalid_data(format!(
                "unsupported ETXT version {version} (expected {VERSION})"
            )));
        }
        let bigram_count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let posting_total = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let doc_count = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let text_total = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        let text_omitted = buf[FLAGS_OFFSET] & FLAG_TEXT_OMITTED != 0;

        // ── 構造検証 ──
        // header のカウント類と各エントリの offset/len を全部バッファサイズに対して
        // 検証する。truncate されたファイルや壊れた header をここで InvalidData として
        // 弾かないと、検索時 (get_posting / get_text) の slice index で panic する。
        // レイアウト計算は u64 で行い、32bit target (wasm32) での桁あふれも防ぐ。
        let bigram_end = HEADER_SIZE as u64 + bigram_count as u64 * BIGRAM_ENTRY as u64;
        let posting_start = bigram_end + posting_padding(bigram_count) as u64;
        let posting_end = posting_start + posting_total as u64 * POSTING_ENTRY as u64;
        let doc_end = posting_end + doc_count as u64 * DOC_ENTRY as u64;
        let total = doc_end + text_total as u64;
        if total > buf.len() as u64 {
            return Err(invalid_data(format!(
                "truncated or corrupt ETXT file: header claims {total} bytes \
                 (bigrams={bigram_count}, postings={posting_total}, docs={doc_count}, \
                 text={text_total}), file has {}",
                buf.len()
            )));
        }

        // bigram index の各エントリ: posting 範囲が Posting Data 内に収まるか。
        for i in 0..bigram_count as usize {
            let base = HEADER_SIZE + i * BIGRAM_ENTRY;
            let entry = &buf[base..base + BIGRAM_ENTRY];
            let offset = u32::from_le_bytes(entry[4..8].try_into().unwrap()) as u64;
            let len = u32::from_le_bytes(entry[8..12].try_into().unwrap()) as u64;
            if offset + len > posting_total as u64 {
                return Err(invalid_data(format!(
                    "corrupt ETXT bigram entry {i}: posting range {offset}+{len} \
                     exceeds posting_total {posting_total}"
                )));
            }
        }

        // doc index の各エントリ: text 範囲が Text Data 内に収まるか。
        for i in 0..doc_count as usize {
            let base = posting_end as usize + i * DOC_ENTRY;
            let entry = &buf[base..base + DOC_ENTRY];
            let offset = u32::from_le_bytes(entry[8..12].try_into().unwrap()) as u64;
            let len = u32::from_le_bytes(entry[12..16].try_into().unwrap()) as u64;
            if offset + len > text_total as u64 {
                return Err(invalid_data(format!(
                    "corrupt ETXT doc entry {i}: text range {offset}+{len} \
                     exceeds text_total {text_total}"
                )));
            }
        }

        Ok(Self { backing, bigram_count, posting_total, doc_count, text_total, text_omitted })
    }

    /// bigram key → posting list (entity IDs)。
    /// アライメント非依存の読み出しで Vec<u64> を返す（slice cast を使わない）。
    pub fn get_posting(&self, key: u32) -> Vec<u64> {
        let idx = self.bigram_index();
        // 二分探索
        let mut lo = 0usize;
        let mut hi = self.bigram_count as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry = &idx[mid * BIGRAM_ENTRY..(mid + 1) * BIGRAM_ENTRY];
            let entry_key = u32::from_le_bytes(entry[0..4].try_into().unwrap());
            if entry_key < key { lo = mid + 1; }
            else if entry_key > key { hi = mid; }
            else {
                let offset = u32::from_le_bytes(entry[4..8].try_into().unwrap()) as usize;
                let len = u32::from_le_bytes(entry[8..12].try_into().unwrap()) as usize;
                let data = self.posting_data();
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    let start = (offset + i) * POSTING_ENTRY;
                    let bytes: [u8; 8] = data[start..start + POSTING_ENTRY].try_into().unwrap();
                    out.push(u64::from_le_bytes(bytes));
                }
                return out;
            }
        }
        Vec::new()
    }

    /// 複数 key の AND
    pub fn intersect(&self, keys: &[u32]) -> Vec<u64> {
        if keys.is_empty() { return vec![]; }

        let postings: Vec<Vec<u64>> = keys.iter().map(|&k| self.get_posting(k)).collect();
        if postings.iter().any(|p| p.is_empty()) { return vec![]; }

        let (shortest_idx, _) = postings.iter().enumerate()
            .min_by_key(|(_, p)| p.len())
            .unwrap();

        let mut result = postings[shortest_idx].clone();
        result.sort_unstable();
        result.dedup();

        for (i, posting) in postings.iter().enumerate() {
            if i == shortest_idx { continue; }
            let mut set = posting.clone();
            set.sort_unstable();
            set.dedup();
            result.retain(|eid| set.binary_search(eid).is_ok());
            if result.is_empty() { return vec![]; }
        }
        result
    }

    /// entity ID → 原文。 postings-only index は原文を持たないので常に None
    /// (呼び出し側が DB 本体の原文を引く前提 #84)。
    pub fn get_text(&self, eid: u64) -> Option<&str> {
        if self.text_omitted { return None; }
        let idx = self.doc_index();
        let mut lo = 0usize;
        let mut hi = self.doc_count as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry = &idx[mid * DOC_ENTRY..(mid + 1) * DOC_ENTRY];
            let entry_eid = u64::from_le_bytes(entry[0..8].try_into().unwrap());
            if entry_eid < eid { lo = mid + 1; }
            else if entry_eid > eid { hi = mid; }
            else {
                let offset = u32::from_le_bytes(entry[8..12].try_into().unwrap()) as usize;
                let len = u32::from_le_bytes(entry[12..16].try_into().unwrap()) as usize;
                let data = self.text_data();
                return std::str::from_utf8(&data[offset..offset + len]).ok();
            }
        }
        None
    }

    /// 全 doc を走査して条件に合う entity を返す
    pub fn search_all(&self, pred: impl Fn(&str) -> bool) -> Vec<u64> {
        let idx = self.doc_index();
        let data = self.text_data();
        let mut result = Vec::new();
        for i in 0..self.doc_count as usize {
            let entry = &idx[i * DOC_ENTRY..(i + 1) * DOC_ENTRY];
            let eid = u64::from_le_bytes(entry[0..8].try_into().unwrap());
            let offset = u32::from_le_bytes(entry[8..12].try_into().unwrap()) as usize;
            let len = u32::from_le_bytes(entry[12..16].try_into().unwrap()) as usize;
            if let Ok(text) = std::str::from_utf8(&data[offset..offset + len]) {
                if pred(text) { result.push(eid); }
            }
        }
        result
    }

    /// 全 doc を (eid, text) で順に callback に渡す。
    /// `NgramIndex::open_mut` / `NgramIndex::from_bytes_mut` で in-memory 再構築するのに使う。
    pub fn for_each_doc<F: FnMut(u64, &str)>(&self, mut f: F) {
        let idx = self.doc_index();
        let data = self.text_data();
        for i in 0..self.doc_count as usize {
            let entry = &idx[i * DOC_ENTRY..(i + 1) * DOC_ENTRY];
            let eid = u64::from_le_bytes(entry[0..8].try_into().unwrap());
            let offset = u32::from_le_bytes(entry[8..12].try_into().unwrap()) as usize;
            let len = u32::from_le_bytes(entry[12..16].try_into().unwrap()) as usize;
            if let Ok(text) = std::str::from_utf8(&data[offset..offset + len]) {
                f(eid, text);
            }
        }
    }

    pub fn bigram_count(&self) -> u32 { self.bigram_count }
    pub fn doc_count(&self) -> u32 { self.doc_count }

    /// この index が原文 (Text Data) を保持しているか。 false = postings-only。
    pub fn has_text(&self) -> bool { !self.text_omitted }

    // ── レイアウト ──

    fn bigram_index(&self) -> &[u8] {
        let buf = self.backing.as_slice();
        let start = HEADER_SIZE;
        let end = start + self.bigram_count as usize * BIGRAM_ENTRY;
        &buf[start..end]
    }

    fn posting_data(&self) -> &[u8] {
        let buf = self.backing.as_slice();
        let start = HEADER_SIZE
            + self.bigram_count as usize * BIGRAM_ENTRY
            + posting_padding(self.bigram_count);
        let end = start + self.posting_total as usize * POSTING_ENTRY;
        &buf[start..end]
    }

    fn doc_index(&self) -> &[u8] {
        let buf = self.backing.as_slice();
        let posting_end = HEADER_SIZE
            + self.bigram_count as usize * BIGRAM_ENTRY
            + posting_padding(self.bigram_count)
            + self.posting_total as usize * POSTING_ENTRY;
        let start = posting_end;
        let end = start + self.doc_count as usize * DOC_ENTRY;
        &buf[start..end]
    }

    fn text_data(&self) -> &[u8] {
        let buf = self.backing.as_slice();
        let start = HEADER_SIZE
            + self.bigram_count as usize * BIGRAM_ENTRY
            + posting_padding(self.bigram_count)
            + self.posting_total as usize * POSTING_ENTRY
            + self.doc_count as usize * DOC_ENTRY;
        // text_total で明示的に区切る (末尾に余計なバイトがあっても晒さない)
        &buf[start..start + self.text_total as usize]
    }
}

/// インメモリの NgramIndex データをファイルに書き出す
#[cfg(not(target_arch = "wasm32"))]
pub fn save(
    path: &Path,
    postings: &std::collections::HashMap<u32, Vec<u64>>,
    originals: &std::collections::HashMap<u64, String>,
) -> io::Result<()> {
    let mut file = File::create(path)?;
    write_to(&mut file, postings, originals)
}

/// 原文非保持 (postings-only) でファイルに書き出す。 Doc Index / Text Data を
/// 省き header に FLAG_TEXT_OMITTED を立てる。 substring 検証は caller が DB 本体の
/// 原文で行う前提 (#84)。 index が store の本文を二重化しなくなる。
#[cfg(not(target_arch = "wasm32"))]
pub fn save_postings_only(
    path: &Path,
    postings: &std::collections::HashMap<u32, Vec<u64>>,
) -> io::Result<()> {
    let mut file = File::create(path)?;
    write_to_postings_only(&mut file, postings)
}

/// 任意の Writer に書き出す。テストや tar/zst パイプラインから使う。
pub fn write_to<W: Write>(
    w: &mut W,
    postings: &std::collections::HashMap<u32, Vec<u64>>,
    originals: &std::collections::HashMap<u64, String>,
) -> io::Result<()> {
    write_index(w, postings, Some(originals))
}

/// `write_to` の postings-only 版 (原文非保持)。
pub fn write_to_postings_only<W: Write>(
    w: &mut W,
    postings: &std::collections::HashMap<u32, Vec<u64>>,
) -> io::Result<()> {
    write_index(w, postings, None)
}

/// 共通の書き出し。 `originals` が `None` なら postings-only
/// (Doc Index / Text Data を省き flag を立てる)。 `Some` のときは従来と
/// **バイト等価** (reserved 先頭が 0 = flag 無し)。
fn write_index<W: Write>(
    w: &mut W,
    postings: &std::collections::HashMap<u32, Vec<u64>>,
    originals: Option<&std::collections::HashMap<u64, String>>,
) -> io::Result<()> {
    // bigram index をキー順にソート
    let mut bigram_entries: Vec<(u32, &Vec<u64>)> = postings.iter().map(|(&k, v)| (k, v)).collect();
    bigram_entries.sort_by_key(|(k, _)| *k);

    let bigram_count = bigram_entries.len() as u32;
    let posting_total: u32 = bigram_entries.iter().map(|(_, v)| v.len() as u32).sum();

    // doc index を eid 順にソート (postings-only なら空)
    let mut doc_entries: Vec<(u64, &String)> = match originals {
        Some(o) => o.iter().map(|(&k, v)| (k, v)).collect(),
        None => Vec::new(),
    };
    doc_entries.sort_by_key(|(k, _)| *k);

    let doc_count = doc_entries.len() as u32;
    let text_total: u32 = doc_entries.iter().map(|(_, v)| v.len() as u32).sum();

    let mut reserved = [0u8; 8];
    if originals.is_none() {
        reserved[0] = FLAG_TEXT_OMITTED;
    }

    // Header
    w.write_all(MAGIC)?;
    w.write_all(&VERSION.to_le_bytes())?;
    w.write_all(&bigram_count.to_le_bytes())?;
    w.write_all(&posting_total.to_le_bytes())?;
    w.write_all(&doc_count.to_le_bytes())?;
    w.write_all(&text_total.to_le_bytes())?;
    w.write_all(&reserved)?;

    // Bigram Index
    let mut offset: u32 = 0;
    for (key, eids) in &bigram_entries {
        let len = eids.len() as u32;
        w.write_all(&key.to_le_bytes())?;
        w.write_all(&offset.to_le_bytes())?;
        w.write_all(&len.to_le_bytes())?;
        offset += len;
    }

    // Padding to 8-byte align Posting Data
    let pad = posting_padding(bigram_count);
    if pad > 0 {
        w.write_all(&[0u8; 8][..pad])?;
    }

    // Posting Data (u64 each)
    for (_, eids) in &bigram_entries {
        for &eid in eids.iter() {
            w.write_all(&eid.to_le_bytes())?;
        }
    }

    // Doc Index
    let mut text_offset: u32 = 0;
    for (eid, text) in &doc_entries {
        let len = text.len() as u32;
        w.write_all(&eid.to_le_bytes())?;
        w.write_all(&text_offset.to_le_bytes())?;
        w.write_all(&len.to_le_bytes())?;
        text_offset += len;
    }

    // Text Data
    for (_, text) in &doc_entries {
        w.write_all(text.as_bytes())?;
    }

    w.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// 有効な index をバイト列で作る helper。
    fn build_valid_bytes() -> Vec<u8> {
        let mut postings: HashMap<u32, Vec<u64>> = HashMap::new();
        postings.insert(1, vec![10, 20]);
        postings.insert(2, vec![10]);
        let mut originals: HashMap<u64, String> = HashMap::new();
        originals.insert(10, "国民は法の下に平等".to_string());
        originals.insert(20, "個人として尊重される".to_string());
        let mut buf = Vec::new();
        write_to(&mut buf, &postings, &originals).unwrap();
        buf
    }

    #[test]
    fn valid_bytes_load_and_search() {
        let buf = build_valid_bytes();
        let idx = MappedIndex::from_bytes(buf).unwrap();
        assert_eq!(idx.doc_count(), 2);
        assert_eq!(idx.get_posting(1), vec![10, 20]);
        assert_eq!(idx.get_text(10), Some("国民は法の下に平等"));
    }

    #[test]
    fn postings_only_omits_text() {
        let mut postings: HashMap<u32, Vec<u64>> = HashMap::new();
        postings.insert(1, vec![10, 20]);
        postings.insert(2, vec![10]);
        let mut buf = Vec::new();
        write_to_postings_only(&mut buf, &postings).unwrap();

        let idx = MappedIndex::from_bytes(buf).unwrap();
        assert!(!idx.has_text(), "postings-only は has_text=false");
        assert_eq!(idx.doc_count(), 0, "Doc Index を持たない");
        // 候補 (posting) は引ける
        assert_eq!(idx.get_posting(1), vec![10, 20]);
        assert_eq!(idx.intersect(&[1, 2]), vec![10]);
        // 原文は持たない
        assert_eq!(idx.get_text(10), None);
    }

    #[test]
    fn text_holding_bytes_are_unchanged() {
        // 原文保持の書き出しは flag 導入後もバイト等価 (reserved[0]==0)。
        let buf = build_valid_bytes();
        assert_eq!(buf[FLAGS_OFFSET] & FLAG_TEXT_OMITTED, 0);
        let idx = MappedIndex::from_bytes(buf).unwrap();
        assert!(idx.has_text());
    }

    #[test]
    fn truncated_bytes_error_instead_of_panic() {
        let buf = build_valid_bytes();
        // header は無傷のまま、あらゆる長さで truncate して panic しないことを確認。
        // (HEADER_SIZE 未満は「not an ETXT file」、それ以上は構造検証で弾かれる)
        for cut in 0..buf.len() {
            let truncated = buf[..cut].to_vec();
            let err = MappedIndex::from_bytes(truncated)
                .err()
                .unwrap_or_else(|| panic!("truncated at {cut}/{} must not load", buf.len()));
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "cut={cut}");
        }
    }

    #[test]
    fn truncated_file_error_instead_of_panic() {
        // 実ファイル経由 (mmap パス) でも truncate が InvalidData になること
        let path = std::env::temp_dir().join(format!(
            "enchu_ngram_truncated_{}.etxt",
            std::process::id()
        ));
        let buf = build_valid_bytes();
        // Text Data の途中でちょん切る
        std::fs::write(&path, &buf[..buf.len() - 5]).unwrap();
        let err = MappedIndex::open(&path).err().expect("truncated file must not load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn corrupt_bigram_offset_rejected() {
        let mut buf = build_valid_bytes();
        // 先頭 bigram エントリの offset (header 直後 +4) を巨大値に書き換え
        let base = HEADER_SIZE + 4;
        buf[base..base + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let err = MappedIndex::from_bytes(buf).err().expect("corrupt index must not load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn corrupt_doc_len_rejected() {
        let mut buf = build_valid_bytes();
        // doc index 先頭エントリの len (eid u64 + offset u32 の後) を巨大値に書き換え
        let bigram_count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let posting_total = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let doc_base = HEADER_SIZE
            + bigram_count as usize * BIGRAM_ENTRY
            + posting_padding(bigram_count)
            + posting_total as usize * POSTING_ENTRY;
        let len_pos = doc_base + 12;
        buf[len_pos..len_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let err = MappedIndex::from_bytes(buf).err().expect("corrupt index must not load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn corrupt_header_count_rejected() {
        let mut buf = build_valid_bytes();
        // doc_count を巨大値に書き換え → レイアウト合計がバッファ超過
        buf[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        let err = MappedIndex::from_bytes(buf).err().expect("corrupt index must not load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
