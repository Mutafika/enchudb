/// mmap ファイル形式
///
/// [Header 32 bytes]
///   magic: "ETXT" (4)
///   version: u32 (4)
///   bigram_count: u32 (4)
///   posting_total: u32 (4) — entity ID エントリ総数
///   doc_count: u32 (4)
///   text_total: u32 (4) — テキストデータ総バイト数
///   _reserved: [u8; 8]
///
/// [Bigram Index] — bigram_count × 12 bytes
///   key: u32, offset: u32, len: u32
///   key 昇順ソート（二分探索用）
///
/// [Posting Data] — posting_total × 4 bytes
///   flat array of u32 entity IDs
///
/// [Doc Index] — doc_count × 12 bytes
///   eid: u32, offset: u32, len: u32
///   eid 昇順ソート
///
/// [Text Data] — text_total bytes

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use memmap2::Mmap;

const MAGIC: &[u8; 4] = b"ETXT";
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 32;

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
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported version"));
        }
        let bigram_count = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        let posting_total = u32::from_le_bytes(mmap[12..16].try_into().unwrap());
        let doc_count = u32::from_le_bytes(mmap[16..20].try_into().unwrap());
        Ok(Self { mmap, bigram_count, posting_total, doc_count })
    }

    /// bigram key → posting list (entity IDs)
    pub fn get_posting(&self, key: u32) -> &[u32] {
        let idx = self.bigram_index();
        // 二分探索
        let mut lo = 0usize;
        let mut hi = self.bigram_count as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_key = u32::from_le_bytes(idx[mid * 12..mid * 12 + 4].try_into().unwrap());
            if entry_key < key { lo = mid + 1; }
            else if entry_key > key { hi = mid; }
            else {
                let offset = u32::from_le_bytes(idx[mid * 12 + 4..mid * 12 + 8].try_into().unwrap()) as usize;
                let len = u32::from_le_bytes(idx[mid * 12 + 8..mid * 12 + 12].try_into().unwrap()) as usize;
                let data = self.posting_data();
                let byte_start = offset * 4;
                let byte_end = (offset + len) * 4;
                let ptr = data[byte_start..byte_end].as_ptr() as *const u32;
                return unsafe { std::slice::from_raw_parts(ptr, len) };
            }
        }
        &[]
    }

    /// 複数 key の AND
    pub fn intersect(&self, keys: &[u32]) -> Vec<u32> {
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

        let mut result: Vec<u32> = self.get_posting(keys[shortest_idx]).to_vec();
        result.sort_unstable();
        result.dedup();

        for (i, &key) in keys.iter().enumerate() {
            if i == shortest_idx { continue; }
            let posting = self.get_posting(key);
            let mut set: Vec<u32> = posting.to_vec();
            set.sort_unstable();
            set.dedup();
            result.retain(|eid| set.binary_search(eid).is_ok());
            if result.is_empty() { return vec![]; }
        }
        result
    }

    /// entity ID → 原文
    pub fn get_text(&self, eid: u32) -> Option<&str> {
        let idx = self.doc_index();
        let mut lo = 0usize;
        let mut hi = self.doc_count as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_eid = u32::from_le_bytes(idx[mid * 12..mid * 12 + 4].try_into().unwrap());
            if entry_eid < eid { lo = mid + 1; }
            else if entry_eid > eid { hi = mid; }
            else {
                let offset = u32::from_le_bytes(idx[mid * 12 + 4..mid * 12 + 8].try_into().unwrap()) as usize;
                let len = u32::from_le_bytes(idx[mid * 12 + 8..mid * 12 + 12].try_into().unwrap()) as usize;
                let data = self.text_data();
                return std::str::from_utf8(&data[offset..offset + len]).ok();
            }
        }
        None
    }

    /// 全 doc を走査して条件に合う entity を返す
    pub fn search_all(&self, pred: impl Fn(&str) -> bool) -> Vec<u32> {
        let idx = self.doc_index();
        let data = self.text_data();
        let mut result = Vec::new();
        for i in 0..self.doc_count as usize {
            let eid = u32::from_le_bytes(idx[i * 12..i * 12 + 4].try_into().unwrap());
            let offset = u32::from_le_bytes(idx[i * 12 + 4..i * 12 + 8].try_into().unwrap()) as usize;
            let len = u32::from_le_bytes(idx[i * 12 + 8..i * 12 + 12].try_into().unwrap()) as usize;
            if let Ok(text) = std::str::from_utf8(&data[offset..offset + len]) {
                if pred(text) { result.push(eid); }
            }
        }
        result
    }

    pub fn bigram_count(&self) -> u32 { self.bigram_count }
    pub fn doc_count(&self) -> u32 { self.doc_count }

    // ── レイアウト ──

    fn bigram_index(&self) -> &[u8] {
        let start = HEADER_SIZE;
        let end = start + self.bigram_count as usize * 12;
        &self.mmap[start..end]
    }

    fn posting_data(&self) -> &[u8] {
        let start = HEADER_SIZE + self.bigram_count as usize * 12;
        let end = start + self.posting_total as usize * 4;
        &self.mmap[start..end]
    }

    fn doc_index(&self) -> &[u8] {
        let posting_end = HEADER_SIZE + self.bigram_count as usize * 12 + self.posting_total as usize * 4;
        let start = posting_end;
        let end = start + self.doc_count as usize * 12;
        &self.mmap[start..end]
    }

    fn text_data(&self) -> &[u8] {
        let start = HEADER_SIZE
            + self.bigram_count as usize * 12
            + self.posting_total as usize * 4
            + self.doc_count as usize * 12;
        &self.mmap[start..]
    }
}

/// インメモリの TextEngine データをファイルに書き出す
pub fn save(
    path: &Path,
    postings: &std::collections::HashMap<u32, Vec<u32>>,
    originals: &std::collections::HashMap<u32, String>,
) -> io::Result<()> {
    let mut file = File::create(path)?;

    // bigram index をキー順にソート
    let mut bigram_entries: Vec<(u32, &Vec<u32>)> = postings.iter().map(|(&k, v)| (k, v)).collect();
    bigram_entries.sort_by_key(|(k, _)| *k);

    let bigram_count = bigram_entries.len() as u32;
    let posting_total: u32 = bigram_entries.iter().map(|(_, v)| v.len() as u32).sum();

    // doc index を eid 順にソート
    let mut doc_entries: Vec<(u32, &String)> = originals.iter().map(|(&k, v)| (k, v)).collect();
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

    // Posting Data
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
