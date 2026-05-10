/// mmap ファイル形式 (version 2)
///
/// [Header 32 bytes]
///   magic: "ETXT" (4)
///   version: u32 (4)  — 2 (eid u64 化に伴う break。v1 は読まない)
///   bigram_count: u32 (4)
///   posting_total: u32 (4) — entity ID エントリ総数（バイト数ではない）
///   doc_count: u32 (4)
///   text_total: u32 (4) — テキストデータ総バイト数
///   _reserved: [u8; 8]
///
/// [Bigram Index] — bigram_count × 12 bytes
///   key: u32, offset: u32, len: u32
///   key 昇順ソート（二分探索用）
///   offset/len は Posting Data 内のエントリ単位（byte 単位ではない）
///
/// [Padding] — 0..=7 bytes
///   Posting Data の先頭を 8-byte 境界に揃えるための詰め物
///   (Bigram Index は 12 bytes/entry でアライメントが揃わないため)
///
/// [Posting Data] — posting_total × 8 bytes
///   flat array of u64 entity IDs
///
/// [Doc Index] — doc_count × 16 bytes
///   eid: u64, offset: u32, len: u32
///   eid 昇順ソート
///
/// [Text Data] — text_total bytes

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use memmap2::Mmap;

const MAGIC: &[u8; 4] = b"ETXT";
const VERSION: u32 = 2;
const HEADER_SIZE: usize = 32;
const BIGRAM_ENTRY: usize = 12;       // key u32 + offset u32 + len u32
const POSTING_ENTRY: usize = 8;       // eid u64
const DOC_ENTRY: usize = 16;          // eid u64 + offset u32 + len u32

/// Posting Data 先頭を 8-byte 境界に揃えるためのパディング量
#[inline]
fn posting_padding(bigram_count: u32) -> usize {
    let after_bigrams = HEADER_SIZE + (bigram_count as usize) * BIGRAM_ENTRY;
    (8 - (after_bigrams % 8)) % 8
}

/// mmap された読み取り専用インデックス
pub struct MappedIndex {
    mmap: Mmap,
    bigram_count: u32,
    posting_total: u32,
    doc_count: u32,
}

impl MappedIndex {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        if mmap.len() < HEADER_SIZE || &mmap[0..4] != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "not an ETXT file"));
        }
        let version = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported ETXT version {version} (expected {VERSION})"),
            ));
        }
        let bigram_count = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        let posting_total = u32::from_le_bytes(mmap[12..16].try_into().unwrap());
        let doc_count = u32::from_le_bytes(mmap[16..20].try_into().unwrap());
        Ok(Self { mmap, bigram_count, posting_total, doc_count })
    }

    /// bigram key → posting list (entity IDs)
    pub fn get_posting(&self, key: u32) -> &[u64] {
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
                let byte_start = offset * POSTING_ENTRY;
                let byte_end = (offset + len) * POSTING_ENTRY;
                let slice = &data[byte_start..byte_end];
                // Posting Data は 8-byte 境界に揃えてあるので、u64 として安全にキャスト可能。
                debug_assert_eq!(slice.as_ptr() as usize % 8, 0);
                let ptr = slice.as_ptr() as *const u64;
                return unsafe { std::slice::from_raw_parts(ptr, len) };
            }
        }
        &[]
    }

    /// 複数 key の AND
    pub fn intersect(&self, keys: &[u32]) -> Vec<u64> {
        if keys.is_empty() { return vec![]; }

        let mut shortest_idx = 0;
        let mut shortest_len = usize::MAX;
        for (i, &key) in keys.iter().enumerate() {
            let len = self.get_posting(key).len();
            if len == 0 { return vec![]; }
            if len < shortest_len {
                shortest_len = len;
                shortest_idx = i;
            }
        }

        let mut result: Vec<u64> = self.get_posting(keys[shortest_idx]).to_vec();
        result.sort_unstable();
        result.dedup();

        for (i, &key) in keys.iter().enumerate() {
            if i == shortest_idx { continue; }
            let posting = self.get_posting(key);
            let mut set: Vec<u64> = posting.to_vec();
            set.sort_unstable();
            set.dedup();
            result.retain(|eid| set.binary_search(eid).is_ok());
            if result.is_empty() { return vec![]; }
        }
        result
    }

    /// entity ID → 原文
    pub fn get_text(&self, eid: u64) -> Option<&str> {
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
    /// `TextEngine::open_mut` で in-memory 再構築するのに使う。
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

    // ── レイアウト ──

    fn bigram_index(&self) -> &[u8] {
        let start = HEADER_SIZE;
        let end = start + self.bigram_count as usize * BIGRAM_ENTRY;
        &self.mmap[start..end]
    }

    fn posting_data(&self) -> &[u8] {
        let start = HEADER_SIZE
            + self.bigram_count as usize * BIGRAM_ENTRY
            + posting_padding(self.bigram_count);
        let end = start + self.posting_total as usize * POSTING_ENTRY;
        &self.mmap[start..end]
    }

    fn doc_index(&self) -> &[u8] {
        let posting_end = HEADER_SIZE
            + self.bigram_count as usize * BIGRAM_ENTRY
            + posting_padding(self.bigram_count)
            + self.posting_total as usize * POSTING_ENTRY;
        let start = posting_end;
        let end = start + self.doc_count as usize * DOC_ENTRY;
        &self.mmap[start..end]
    }

    fn text_data(&self) -> &[u8] {
        let start = HEADER_SIZE
            + self.bigram_count as usize * BIGRAM_ENTRY
            + posting_padding(self.bigram_count)
            + self.posting_total as usize * POSTING_ENTRY
            + self.doc_count as usize * DOC_ENTRY;
        &self.mmap[start..]
    }
}

/// インメモリの TextEngine データをファイルに書き出す
pub fn save(
    path: &Path,
    postings: &std::collections::HashMap<u32, Vec<u64>>,
    originals: &std::collections::HashMap<u64, String>,
) -> io::Result<()> {
    let mut file = File::create(path)?;

    // bigram index をキー順にソート
    let mut bigram_entries: Vec<(u32, &Vec<u64>)> = postings.iter().map(|(&k, v)| (k, v)).collect();
    bigram_entries.sort_by_key(|(k, _)| *k);

    let bigram_count = bigram_entries.len() as u32;
    let posting_total: u32 = bigram_entries.iter().map(|(_, v)| v.len() as u32).sum();

    // doc index を eid 順にソート
    let mut doc_entries: Vec<(u64, &String)> = originals.iter().map(|(&k, v)| (k, v)).collect();
    doc_entries.sort_by_key(|(k, _)| *k);

    let doc_count = doc_entries.len() as u32;
    let text_total: u32 = doc_entries.iter().map(|(_, v)| v.len() as u32).sum();

    // Header
    file.write_all(MAGIC)?;
    file.write_all(&VERSION.to_le_bytes())?;
    file.write_all(&bigram_count.to_le_bytes())?;
    file.write_all(&posting_total.to_le_bytes())?;
    file.write_all(&doc_count.to_le_bytes())?;
    file.write_all(&text_total.to_le_bytes())?;
    file.write_all(&[0u8; 8])?; // reserved

    // Bigram Index
    let mut offset: u32 = 0;
    for (key, eids) in &bigram_entries {
        let len = eids.len() as u32;
        file.write_all(&key.to_le_bytes())?;
        file.write_all(&offset.to_le_bytes())?;
        file.write_all(&len.to_le_bytes())?;
        offset += len;
    }

    // Padding to 8-byte align Posting Data
    let pad = posting_padding(bigram_count);
    if pad > 0 {
        file.write_all(&[0u8; 8][..pad])?;
    }

    // Posting Data (u64 each)
    for (_, eids) in &bigram_entries {
        for &eid in eids.iter() {
            file.write_all(&eid.to_le_bytes())?;
        }
    }

    // Doc Index
    let mut text_offset: u32 = 0;
    for (eid, text) in &doc_entries {
        let len = text.len() as u32;
        file.write_all(&eid.to_le_bytes())?;
        file.write_all(&text_offset.to_le_bytes())?;
        file.write_all(&len.to_le_bytes())?;
        text_offset += len;
    }

    // Text Data
    for (_, text) in &doc_entries {
        file.write_all(text.as_bytes())?;
    }

    file.flush()?;
    Ok(())
}
