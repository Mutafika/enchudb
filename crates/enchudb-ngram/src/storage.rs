//! mmap ファイル形式 (`.etxt`)
//!
//! version 2 と version 3 の 2 系統を読む。違いは **gram key の幅だけ**で、
//! それ以外のセクション構成は共通 (#121)。
//!
//! | | v2 | v3 |
//! |---|---|---|
//! | n | 2 固定 (header に持たない) | header `n` byte (2..=4) |
//! | key | u32 (16bit × 2) | u64 (16bit × n) |
//! | Gram Index entry | 12 bytes | 16 bytes |
//!
//! n = 2 の key は u64 でも上位 32bit が 0 なので、**v2 はゼロ拡張するだけで読める**
//! （昇順ソート順も保たれるので二分探索がそのまま効く）。逆に writer は
//! **n = 2 なら v2 を書く** ので、既定設定での出力バイト列は #121 以前と完全に同じ。
//! v3 が出るのは `with_n(3)` / `with_n(4)` を明示したときだけ。
//!
//! ```text
//! [Header 32 bytes]
//!   magic: "ETXT" (4)
//!   version: u32 (4)  — 2 または 3 (v1 = eid u32 時代は読まない)
//!   gram_count: u32 (4)
//!   posting_total: u32 (4) — entity ID エントリ総数（バイト数ではない）
//!   doc_count: u32 (4)
//!   text_total: u32 (4) — テキストデータ総バイト数
//!   flags: u8 (1) — bit0 TEXT_OMITTED: Doc Index / Text Data を持たない
//!                   postings-only index (原文は DB 本体が所有、検証は caller 側 #84)
//!   n: u8 (1) — v3 のみ有効 (2..=4)。v2 は常に 2 と解釈し、この byte は読まない
//!   _reserved: [u8; 6]
//!
//! [Gram Index] — gram_count × (12 | 16) bytes
//!   v2: key u32, offset u32, len u32
//!   v3: key u64, offset u32, len u32
//!   key 昇順ソート（二分探索用）
//!   offset/len は Posting Data 内のエントリ単位（byte 単位ではない）
//!
//! [Padding] — 0..=7 bytes
//!   Posting Data の先頭を 8-byte 境界に揃えるための詰め物。
//!   現状の reader は from_le_bytes でアライメント非依存に読むので必須ではないが、
//!   将来 mmap 上で u64 slice cast に戻す余地を残すため format として保持する。
//!   (v3 は entry 16B なので常に 0 だが、計算は共通経路に載せる)
//!
//! [Posting Data] — posting_total × 8 bytes
//!   flat array of u64 entity IDs (little-endian)
//!
//! [Doc Index] — doc_count × 16 bytes
//!   eid: u64, offset: u32, len: u32
//!   eid 昇順ソート
//!
//! [Text Data] — text_total bytes
//! ```

use std::io;
use std::path::Path;

#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
#[cfg(not(target_arch = "wasm32"))]
use memmap2::Mmap;

use std::io::Write;

use crate::gram;

const MAGIC: &[u8; 4] = b"ETXT";
/// key u32 / n = 2 固定。#121 以前の唯一の format。
const VERSION_V2: u32 = 2;
/// key u64 / n を header に持つ (#121)。
const VERSION_V3: u32 = 3;
const HEADER_SIZE: usize = 32;
const GRAM_ENTRY_V2: usize = 12;      // key u32 + offset u32 + len u32
const GRAM_ENTRY_V3: usize = 16;      // key u64 + offset u32 + len u32
const POSTING_ENTRY: usize = 8;       // eid u64
const DOC_ENTRY: usize = 16;          // eid u64 + offset u32 + len u32

/// header の flags byte (`_reserved` の先頭 = buf[24])。 立っていれば Doc Index /
/// Text Data 無しの postings-only index。 旧 v2 file は reserved 全 0 = 原文保持、
/// で自然に後方互換 (#84)。
const FLAG_TEXT_OMITTED: u8 = 0x01;
const FLAGS_OFFSET: usize = 24;
/// n を置く byte (v3 のみ)。v2 file はここが 0 だが version で判別するので読まない。
const N_OFFSET: usize = 25;

/// version に対応する Gram Index の 1 エントリ幅。
#[inline]
fn gram_entry(version: u32) -> usize {
    if version == VERSION_V2 { GRAM_ENTRY_V2 } else { GRAM_ENTRY_V3 }
}

/// n に対応する書き出し version。n = 2 は v2 (= #121 以前とバイト等価)。
#[inline]
fn version_for_n(n: usize) -> u32 {
    if n == gram::DEFAULT_N { VERSION_V2 } else { VERSION_V3 }
}

/// Posting Data 先頭を 8-byte 境界に揃えるためのパディング量
#[inline]
fn posting_padding(gram_count: u32, entry: usize) -> usize {
    let after_grams = HEADER_SIZE + (gram_count as usize) * entry;
    (8 - (after_grams % 8)) % 8
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
    /// Gram Index の 1 エントリ幅（version 由来。key の読み方もこれで決まる）
    gram_entry: usize,
    /// この index が使っている n（v2 は常に 2）
    n: usize,
    gram_count: u32,
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
        if version != VERSION_V2 && version != VERSION_V3 {
            return Err(invalid_data(format!(
                "unsupported ETXT version {version} (expected {VERSION_V2} or {VERSION_V3})"
            )));
        }
        let gram_entry = gram_entry(version);
        // n: v2 は 2 固定。v3 は header の byte を検証して採用する。
        let n = if version == VERSION_V2 {
            gram::DEFAULT_N
        } else {
            let raw = buf[N_OFFSET] as usize;
            gram::validate_n(raw).map_err(|_| {
                invalid_data(format!(
                    "corrupt ETXT header: n = {raw} (v3 は {}..={} のみ)",
                    gram::MIN_N,
                    gram::MAX_N
                ))
            })?
        };
        let gram_count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let posting_total = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let doc_count = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let text_total = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        let text_omitted = buf[FLAGS_OFFSET] & FLAG_TEXT_OMITTED != 0;

        // ── 構造検証 ──
        // header のカウント類と各エントリの offset/len を全部バッファサイズに対して
        // 検証する。truncate されたファイルや壊れた header をここで InvalidData として
        // 弾かないと、検索時 (get_posting / get_text) の slice index で panic する。
        // レイアウト計算は u64 で行い、32bit target (wasm32) での桁あふれも防ぐ。
        let gram_end = HEADER_SIZE as u64 + gram_count as u64 * gram_entry as u64;
        let posting_start = gram_end + posting_padding(gram_count, gram_entry) as u64;
        let posting_end = posting_start + posting_total as u64 * POSTING_ENTRY as u64;
        let doc_end = posting_end + doc_count as u64 * DOC_ENTRY as u64;
        let total = doc_end + text_total as u64;
        if total > buf.len() as u64 {
            return Err(invalid_data(format!(
                "truncated or corrupt ETXT file: header claims {total} bytes \
                 (grams={gram_count}, postings={posting_total}, docs={doc_count}, \
                 text={text_total}), file has {}",
                buf.len()
            )));
        }

        // gram index の各エントリ: posting 範囲が Posting Data 内に収まるか。
        for i in 0..gram_count as usize {
            let base = HEADER_SIZE + i * gram_entry;
            let entry = &buf[base..base + gram_entry];
            let (_, offset, len) = decode_gram_entry(entry, gram_entry);
            if offset as u64 + len as u64 > posting_total as u64 {
                return Err(invalid_data(format!(
                    "corrupt ETXT gram entry {i}: posting range {offset}+{len} \
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

        Ok(Self {
            backing,
            gram_entry,
            n,
            gram_count,
            posting_total,
            doc_count,
            text_total,
            text_omitted,
        })
    }

    /// gram key → posting list (entity IDs)。
    /// アライメント非依存の読み出しで Vec<u64> を返す（slice cast を使わない）。
    pub fn get_posting(&self, key: u64) -> Vec<u64> {
        let idx = self.gram_index();
        let w = self.gram_entry;
        // 二分探索
        let mut lo = 0usize;
        let mut hi = self.gram_count as usize;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (entry_key, offset, len) = decode_gram_entry(&idx[mid * w..(mid + 1) * w], w);
            if entry_key < key { lo = mid + 1; }
            else if entry_key > key { hi = mid; }
            else {
                let (offset, len) = (offset as usize, len as usize);
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
    pub fn intersect(&self, keys: &[u64]) -> Vec<u64> {
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

    pub fn gram_count(&self) -> u32 { self.gram_count }
    pub fn doc_count(&self) -> u32 { self.doc_count }

    /// この index が使っている n。v2 file は 2。
    pub fn n(&self) -> usize { self.n }

    /// この index が原文 (Text Data) を保持しているか。 false = postings-only。
    pub fn has_text(&self) -> bool { !self.text_omitted }

    // ── レイアウト ──

    fn gram_index(&self) -> &[u8] {
        let buf = self.backing.as_slice();
        let start = HEADER_SIZE;
        let end = start + self.gram_count as usize * self.gram_entry;
        &buf[start..end]
    }

    /// Gram Index + padding の直後 = Posting Data の開始 offset。
    #[inline]
    fn posting_start(&self) -> usize {
        HEADER_SIZE
            + self.gram_count as usize * self.gram_entry
            + posting_padding(self.gram_count, self.gram_entry)
    }

    fn posting_data(&self) -> &[u8] {
        let buf = self.backing.as_slice();
        let start = self.posting_start();
        let end = start + self.posting_total as usize * POSTING_ENTRY;
        &buf[start..end]
    }

    fn doc_index(&self) -> &[u8] {
        let buf = self.backing.as_slice();
        let start = self.posting_start() + self.posting_total as usize * POSTING_ENTRY;
        let end = start + self.doc_count as usize * DOC_ENTRY;
        &buf[start..end]
    }

    fn text_data(&self) -> &[u8] {
        let buf = self.backing.as_slice();
        let start = self.posting_start()
            + self.posting_total as usize * POSTING_ENTRY
            + self.doc_count as usize * DOC_ENTRY;
        // text_total で明示的に区切る (末尾に余計なバイトがあっても晒さない)
        &buf[start..start + self.text_total as usize]
    }
}

/// Gram Index の 1 エントリを (key, offset, len) に分解する。key 幅は version 由来の
/// `entry` で決まる (v2 = u32 をゼロ拡張、v3 = u64)。読み出しはアライメント非依存。
#[inline]
fn decode_gram_entry(entry: &[u8], gram_entry: usize) -> (u64, u32, u32) {
    if gram_entry == GRAM_ENTRY_V2 {
        (
            u32::from_le_bytes(entry[0..4].try_into().unwrap()) as u64,
            u32::from_le_bytes(entry[4..8].try_into().unwrap()),
            u32::from_le_bytes(entry[8..12].try_into().unwrap()),
        )
    } else {
        (
            u64::from_le_bytes(entry[0..8].try_into().unwrap()),
            u32::from_le_bytes(entry[8..12].try_into().unwrap()),
            u32::from_le_bytes(entry[12..16].try_into().unwrap()),
        )
    }
}

/// インメモリの NgramIndex データをファイルに書き出す
#[cfg(not(target_arch = "wasm32"))]
pub fn save(
    path: &Path,
    postings: &std::collections::HashMap<u64, Vec<u64>>,
    originals: &std::collections::HashMap<u64, String>,
    n: usize,
) -> io::Result<()> {
    let mut file = File::create(path)?;
    write_to(&mut file, postings, originals, n)
}

/// 原文非保持 (postings-only) でファイルに書き出す。 Doc Index / Text Data を
/// 省き header に FLAG_TEXT_OMITTED を立てる。 substring 検証は caller が DB 本体の
/// 原文で行う前提 (#84)。 index が store の本文を二重化しなくなる。
#[cfg(not(target_arch = "wasm32"))]
pub fn save_postings_only(
    path: &Path,
    postings: &std::collections::HashMap<u64, Vec<u64>>,
    n: usize,
) -> io::Result<()> {
    let mut file = File::create(path)?;
    write_to_postings_only(&mut file, postings, n)
}

/// 任意の Writer に書き出す。テストや tar/zst パイプラインから使う。
pub fn write_to<W: Write>(
    w: &mut W,
    postings: &std::collections::HashMap<u64, Vec<u64>>,
    originals: &std::collections::HashMap<u64, String>,
    n: usize,
) -> io::Result<()> {
    write_index(w, postings, Some(originals), n)
}

/// `write_to` の postings-only 版 (原文非保持)。
pub fn write_to_postings_only<W: Write>(
    w: &mut W,
    postings: &std::collections::HashMap<u64, Vec<u64>>,
    n: usize,
) -> io::Result<()> {
    write_index(w, postings, None, n)
}

/// 共通の書き出し。 `originals` が `None` なら postings-only
/// (Doc Index / Text Data を省き flag を立てる)。 `Some` のときは従来と
/// **バイト等価** (reserved 先頭が 0 = flag 無し)。
///
/// `n` が format を決める: n = 2 → v2 (key u32、#121 以前とバイト等価)、
/// n ≥ 3 → v3 (key u64、header に n)。
fn write_index<W: Write>(
    w: &mut W,
    postings: &std::collections::HashMap<u64, Vec<u64>>,
    originals: Option<&std::collections::HashMap<u64, String>>,
    n: usize,
) -> io::Result<()> {
    gram::validate_n(n)?;
    let version = version_for_n(n);
    let entry_width = gram_entry(version);

    // gram index をキー順にソート
    let mut gram_entries: Vec<(u64, &Vec<u64>)> = postings.iter().map(|(&k, v)| (k, v)).collect();
    gram_entries.sort_by_key(|(k, _)| *k);

    // v2 は key u32。n = 2 の key は 32bit に収まるはずだが、format を壊すくらいなら
    // 落とす (build 時の n と postings の n がずれた場合の保険)。
    if version == VERSION_V2 {
        if let Some((bad, _)) = gram_entries.iter().find(|(k, _)| *k > u32::MAX as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("n = 2 (v2 format) なのに key {bad:#x} が u32 を超えている"),
            ));
        }
    }

    let gram_count = gram_entries.len() as u32;
    let posting_total: u32 = gram_entries.iter().map(|(_, v)| v.len() as u32).sum();

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
    if version == VERSION_V3 {
        reserved[N_OFFSET - FLAGS_OFFSET] = n as u8;
    }

    // Header
    w.write_all(MAGIC)?;
    w.write_all(&version.to_le_bytes())?;
    w.write_all(&gram_count.to_le_bytes())?;
    w.write_all(&posting_total.to_le_bytes())?;
    w.write_all(&doc_count.to_le_bytes())?;
    w.write_all(&text_total.to_le_bytes())?;
    w.write_all(&reserved)?;

    // Gram Index
    let mut offset: u32 = 0;
    for (key, eids) in &gram_entries {
        let len = eids.len() as u32;
        if version == VERSION_V2 {
            w.write_all(&(*key as u32).to_le_bytes())?;
        } else {
            w.write_all(&key.to_le_bytes())?;
        }
        w.write_all(&offset.to_le_bytes())?;
        w.write_all(&len.to_le_bytes())?;
        offset += len;
    }

    // Padding to 8-byte align Posting Data
    let pad = posting_padding(gram_count, entry_width);
    if pad > 0 {
        w.write_all(&[0u8; 8][..pad])?;
    }

    // Posting Data (u64 each)
    for (_, eids) in &gram_entries {
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


// ══════════════════════════════════════════════════════════════════════
// segment merge (#188)
// ══════════════════════════════════════════════════════════════════════
//
// 索引を「小さい完結ファイル (segment) を並べて、後から統合する」形で運用できるようにする。
// これが無いと索引の作り直しは常に全 doc をメモリに載せる形しか取れず、build のピークが
// コーパス量に比例する (naruhodo の実測で 494,133 doc = +2.7GB)。
//
// **形式は変えない。** `.etxt` は Gram Index が key 昇順・Doc Index が eid 昇順・
// 各 gram の posting run が eid 昇順 (compact 済み) なので、統合は整列済みリストの
// k-way merge で書ける。
//
// 必要メモリ = Gram Index 相当 (distinct gram 数 × 16B) + Doc Index 相当
// (doc 数 × 14B) + 出力バッファ。**本文量には比例しない**のが要点。

/// `merge_files` の結果統計。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeStats {
    pub grams: u32,
    pub postings: u32,
    pub docs: u32,
    pub text_bytes: u32,
    /// 複数 input に居た eid の数 (= 後勝ちで上書きされた doc 数)。
    pub superseded_docs: u32,
}

/// Gram Index を先頭から舐めるカーソル。
struct GramCursor<'a> {
    idx: &'a [u8],
    width: usize,
    i: usize,
    len: usize,
}

impl<'a> GramCursor<'a> {
    fn new(m: &'a MappedIndex) -> Self {
        Self { idx: m.gram_index(), width: m.gram_entry, i: 0, len: m.gram_count as usize }
    }
    fn peek(&self) -> Option<u64> {
        if self.i >= self.len { return None; }
        let e = &self.idx[self.i * self.width..(self.i + 1) * self.width];
        Some(decode_gram_entry(e, self.width).0)
    }
    fn bump(&mut self) { self.i += 1; }
}

/// Doc Index を先頭から舐めるカーソル。
struct DocCursor<'a> {
    idx: &'a [u8],
    i: usize,
    len: usize,
}

impl<'a> DocCursor<'a> {
    fn new(m: &'a MappedIndex) -> Self {
        Self { idx: m.doc_index(), i: 0, len: m.doc_count as usize }
    }
    /// (eid, text offset, text len)
    fn peek(&self) -> Option<(u64, u32, u32)> {
        if self.i >= self.len { return None; }
        let e = &self.idx[self.i * DOC_ENTRY..(self.i + 1) * DOC_ENTRY];
        Some((
            u64::from_le_bytes(e[0..8].try_into().unwrap()),
            u32::from_le_bytes(e[8..12].try_into().unwrap()),
            u32::from_le_bytes(e[12..16].try_into().unwrap()),
        ))
    }
    fn bump(&mut self) { self.i += 1; }
}

/// 複数の `.etxt` を 1 本に統合する。
///
/// - **後の input が勝つ** (LSM の上書き意味論)。同じ eid が複数の input に居る場合、
///   採用されるのは最後の input の原文で、**それ以前の input が持っていたその eid の
///   posting は落とす** — 統合結果が「全部を一度に索引した場合」と一致するようにするため。
/// - 全 input の `n` が一致していること。原文保持 / postings-only の混在は拒否する
///   (flag を勝手に継承すると、原文を持たない index が原文保持を名乗る)。
/// - postings-only 同士の統合では doc index が無いので上書き判定ができない。
///   同じ eid が複数 input に居ても **union** になる (caller が DB 本体の原文で
///   検証する前提 #84 なので、候補が増えるだけで誤答にはならない)。
#[cfg(not(target_arch = "wasm32"))]
pub fn merge_files(inputs: &[&Path], out: &Path) -> io::Result<MergeStats> {
    if inputs.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "merge_files: input が空"));
    }
    let maps: Vec<MappedIndex> = inputs.iter().map(|p| MappedIndex::open(p)).collect::<io::Result<_>>()?;
    let n = maps[0].n;
    let has_text = maps[0].has_text();
    for (i, m) in maps.iter().enumerate().skip(1) {
        if m.n != n {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("merge_files: n が食い違う (input 0 = {n}, input {i} = {})", m.n),
            ));
        }
        if m.has_text() != has_text {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "merge_files: 原文保持 / postings-only の混在 (input 0 = {}, input {i} = {})",
                    if has_text { "原文保持" } else { "postings-only" },
                    if m.has_text() { "原文保持" } else { "postings-only" },
                ),
            ));
        }
    }

    // ── ① doc の所有権を決める (eid 昇順 k-way merge・後勝ち) ──
    // 保持するのは (eid, src, offset, len) だけ。原文そのものは mmap に置いたまま。
    let mut docs: Vec<(u64, u16, u32, u32)> = Vec::new();
    let mut dup_eids: Vec<u64> = Vec::new(); // 上書きが起きた eid (昇順)
    if has_text {
        let mut cur: Vec<DocCursor> = maps.iter().map(DocCursor::new).collect();
        loop {
            let Some(min) = cur.iter().filter_map(|c| c.peek().map(|(e, _, _)| e)).min() else { break };
            let mut winner: Option<(u16, u32, u32)> = None;
            let mut seen = 0usize;
            for (si, c) in cur.iter_mut().enumerate() {
                if c.peek().map(|(e, _, _)| e) != Some(min) { continue; }
                let (_, off, len) = c.peek().unwrap();
                winner = Some((si as u16, off, len)); // 後の input で上書き = 後勝ち
                seen += 1;
                c.bump();
            }
            let (src, off, len) = winner.unwrap();
            if seen > 1 { dup_eids.push(min); }
            docs.push((min, src, off, len));
        }
    }
    // 上書きが 1 件も無ければ (= segment が doc を分割している通常ケース) posting の
    // 所有権チェックは丸ごと不要。この判定 1 つで hot path の binary search が消える。
    let owner_of = |eid: u64| -> Option<u16> {
        docs.binary_search_by_key(&eid, |(e, _, _, _)| *e).ok().map(|i| docs[i].1)
    };

    // ── ② gram の計画を作る (key 昇順 k-way merge) ──
    // key ごとの統合後 posting 数だけを持つ。posting 本体は ③ で引き直す
    // (全部持つと本文量に比例してしまうため)。
    let mut plan: Vec<(u64, u32)> = Vec::new();
    let mut posting_total: u64 = 0;
    {
        let mut cur: Vec<GramCursor> = maps.iter().map(GramCursor::new).collect();
        let mut buf: Vec<u64> = Vec::new();
        loop {
            let Some(min) = cur.iter().filter_map(|c| c.peek()).min() else { break };
            for c in cur.iter_mut() {
                if c.peek() == Some(min) { c.bump(); }
            }
            merged_posting(&maps, min, &dup_eids, &owner_of, &mut buf);
            posting_total += buf.len() as u64;
            if posting_total > u32::MAX as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "merge_files: posting 総数が u32 を超える (format 上限)",
                ));
            }
            plan.push((min, buf.len() as u32));
        }
    }
    // 所有権で全部落ちた key は index から消す (空 posting の gram entry を残さない)。
    plan.retain(|(_, len)| *len > 0);
    let gram_count = plan.len() as u32;
    let posting_total = posting_total as u32;

    let doc_count = docs.len() as u32;
    let text_total: u64 = docs.iter().map(|(_, _, _, l)| *l as u64).sum();
    if text_total > u32::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "merge_files: text 総量が u32 を超える (format 上限)",
        ));
    }
    let text_total = text_total as u32;

    // ── ③ 書き出し ──
    let version = version_for_n(n);
    let entry_width = gram_entry(version);
    if version == VERSION_V2 {
        if let Some((bad, _)) = plan.iter().find(|(k, _)| *k > u32::MAX as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("n = 2 (v2 format) なのに key {bad:#x} が u32 を超えている"),
            ));
        }
    }
    let mut reserved = [0u8; 8];
    if !has_text {
        reserved[0] = FLAG_TEXT_OMITTED;
    }
    if version == VERSION_V3 {
        reserved[N_OFFSET - FLAGS_OFFSET] = n as u8;
    }

    let file = File::create(out)?;
    let mut w = std::io::BufWriter::with_capacity(1 << 20, file);

    w.write_all(MAGIC)?;
    w.write_all(&version.to_le_bytes())?;
    w.write_all(&gram_count.to_le_bytes())?;
    w.write_all(&posting_total.to_le_bytes())?;
    w.write_all(&doc_count.to_le_bytes())?;
    w.write_all(&text_total.to_le_bytes())?;
    w.write_all(&reserved)?;

    let mut offset: u32 = 0;
    for (key, len) in &plan {
        if version == VERSION_V2 {
            w.write_all(&(*key as u32).to_le_bytes())?;
        } else {
            w.write_all(&key.to_le_bytes())?;
        }
        w.write_all(&offset.to_le_bytes())?;
        w.write_all(len.to_le_bytes().as_ref())?;
        offset += *len;
    }

    let pad = posting_padding(gram_count, entry_width);
    if pad > 0 {
        w.write_all(&[0u8; 8][..pad])?;
    }

    let mut buf: Vec<u64> = Vec::new();
    for (key, len) in &plan {
        merged_posting(&maps, *key, &dup_eids, &owner_of, &mut buf);
        debug_assert_eq!(buf.len() as u32, *len, "merge: 計画と書き出しで posting 数が食い違った");
        for &eid in buf.iter() {
            w.write_all(&eid.to_le_bytes())?;
        }
    }

    let mut text_offset: u32 = 0;
    for (eid, _, _, len) in &docs {
        w.write_all(&eid.to_le_bytes())?;
        w.write_all(&text_offset.to_le_bytes())?;
        w.write_all(&len.to_le_bytes())?;
        text_offset += *len;
    }
    for (_, src, off, len) in &docs {
        let data = maps[*src as usize].text_data();
        w.write_all(&data[*off as usize..(*off + *len) as usize])?;
    }

    w.flush()?;
    Ok(MergeStats {
        grams: gram_count,
        postings: posting_total,
        docs: doc_count,
        text_bytes: text_total,
        superseded_docs: dup_eids.len() as u32,
    })
}

/// 1 gram ぶんの posting を全 input から集めて統合する (昇順・重複除去・所有権フィルタ)。
///
/// `out` は使い回す (key ごとに確保し直さない)。ここが merge の hot path で、
/// **一度に触るのは 1 gram ぶんの posting run だけ** = メモリがコーパス量から独立する。
#[cfg(not(target_arch = "wasm32"))]
fn merged_posting(
    maps: &[MappedIndex],
    key: u64,
    dup_eids: &[u64],
    owner_of: &impl Fn(u64) -> Option<u16>,
    out: &mut Vec<u64>,
) {
    out.clear();
    for (si, m) in maps.iter().enumerate() {
        for eid in m.get_posting(key) {
            // 上書きが起きた eid だけ所有権を見る。上書きが無ければ (通常ケース)
            // `dup_eids` が空なのでこの判定は is_empty() 一発で終わる。
            if !dup_eids.is_empty() && dup_eids.binary_search(&eid).is_ok() {
                if owner_of(eid) != Some(si as u16) { continue; }
            }
            out.push(eid);
        }
    }
    out.sort_unstable();
    out.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// 有効な index をバイト列で作る helper。
    fn build_valid_bytes() -> Vec<u8> {
        let mut postings: HashMap<u64, Vec<u64>> = HashMap::new();
        postings.insert(1, vec![10, 20]);
        postings.insert(2, vec![10]);
        let mut originals: HashMap<u64, String> = HashMap::new();
        originals.insert(10, "国民は法の下に平等".to_string());
        originals.insert(20, "個人として尊重される".to_string());
        let mut buf = Vec::new();
        write_to(&mut buf, &postings, &originals, 2).unwrap();
        buf
    }

    /// n ≥ 3 (= v3、key u64) の index をバイト列で作る helper。
    fn build_v3_bytes(n: usize) -> Vec<u8> {
        let mut postings: HashMap<u64, Vec<u64>> = HashMap::new();
        // 32bit を超える key を必ず含める
        postings.insert(gram::extract_keys("国民は法", n)[0], vec![10, 20]);
        postings.insert(gram::extract_keys("民は法の", n)[0], vec![10]);
        let mut originals: HashMap<u64, String> = HashMap::new();
        originals.insert(10, "国民は法の下に平等".to_string());
        originals.insert(20, "個人として尊重される".to_string());
        let mut buf = Vec::new();
        write_to(&mut buf, &postings, &originals, n).unwrap();
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
        let mut postings: HashMap<u64, Vec<u64>> = HashMap::new();
        postings.insert(1, vec![10, 20]);
        postings.insert(2, vec![10]);
        let mut buf = Vec::new();
        write_to_postings_only(&mut buf, &postings, 2).unwrap();

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
    fn default_n_still_writes_v2() {
        // #121 で n を可変にしても、既定 (n=2) の出力は v2 のまま
        // = 既存の reader / 既存の .etxt 生成パイプラインが一切変わらない。
        let buf = build_valid_bytes();
        assert_eq!(u32::from_le_bytes(buf[4..8].try_into().unwrap()), VERSION_V2);
        assert_eq!(buf[N_OFFSET], 0, "v2 は n byte を使わない");
        let idx = MappedIndex::from_bytes(buf).unwrap();
        assert_eq!(idx.n(), 2, "v2 は n=2 と解釈される");
    }

    #[test]
    fn v3_records_n_and_wide_keys() {
        for n in 3..=gram::MAX_N {
            let buf = build_v3_bytes(n);
            assert_eq!(
                u32::from_le_bytes(buf[4..8].try_into().unwrap()),
                VERSION_V3,
                "n={n}"
            );
            assert_eq!(buf[N_OFFSET] as usize, n, "header に n が焼かれること");

            let idx = MappedIndex::from_bytes(buf).unwrap();
            assert_eq!(idx.n(), n);
            let key = gram::extract_keys("国民は法", n)[0];
            assert!(key > u32::MAX as u64, "n={n} の key が u32 に収まってしまっている");
            assert_eq!(idx.get_posting(key), vec![10, 20], "u64 key を exact に引ける");
            assert_eq!(idx.get_text(10), Some("国民は法の下に平等"));
        }
    }

    #[test]
    fn v3_binary_search_over_many_wide_keys() {
        // 二分探索が u64 key の昇順で正しく効くこと (エントリ幅 16B の読み出し込み)。
        let text: String = (0..500).map(|i| char::from_u32(0x4E00 + i).unwrap()).collect();
        let keys = gram::extract_keys(&text, 3);
        let mut postings: HashMap<u64, Vec<u64>> = HashMap::new();
        for (i, &k) in keys.iter().enumerate() {
            postings.insert(k, vec![i as u64]);
        }
        let mut buf = Vec::new();
        write_to_postings_only(&mut buf, &postings, 3).unwrap();
        let idx = MappedIndex::from_bytes(buf).unwrap();
        assert_eq!(idx.gram_count() as usize, keys.len());
        for (i, &k) in keys.iter().enumerate() {
            assert_eq!(idx.get_posting(k), vec![i as u64], "key #{i}");
        }
        // 存在しない key は空
        assert!(idx.get_posting(u64::MAX).is_empty());
    }

    #[test]
    fn v3_bad_n_in_header_rejected() {
        let mut buf = build_v3_bytes(3);
        buf[N_OFFSET] = 9;
        let err = MappedIndex::from_bytes(buf.clone()).err().expect("n=9 は不正");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("n = 9"), "{err}");

        buf[N_OFFSET] = 0;
        let err = MappedIndex::from_bytes(buf).err().expect("n=0 は不正");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn unknown_version_rejected() {
        let mut buf = build_valid_bytes();
        buf[4..8].copy_from_slice(&4u32.to_le_bytes());
        let err = MappedIndex::from_bytes(buf).err().expect("v4 は未対応");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("version 4"), "{err}");
    }

    #[test]
    fn write_rejects_out_of_range_n() {
        let postings: HashMap<u64, Vec<u64>> = HashMap::new();
        for n in [0usize, 1, 5, 255] {
            let mut buf = Vec::new();
            let err = write_to_postings_only(&mut buf, &postings, n)
                .err()
                .unwrap_or_else(|| panic!("n={n} は書けてはいけない"));
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "n={n}");
        }
    }

    #[test]
    fn v2_write_rejects_wide_key() {
        // n=2 指定なのに 32bit 超の key が混ざっていたら format を壊さず落とす。
        let mut postings: HashMap<u64, Vec<u64>> = HashMap::new();
        postings.insert(1u64 << 40, vec![1]);
        let mut buf = Vec::new();
        let err = write_to_postings_only(&mut buf, &postings, 2).expect_err("落ちること");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn truncated_bytes_error_instead_of_panic() {
        for buf in [build_valid_bytes(), build_v3_bytes(3)] {
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
    fn corrupt_gram_offset_rejected() {
        // v2 (offset は key u32 の直後) / v3 (key u64 の直後) の両方
        for (buf, off) in [
            (build_valid_bytes(), HEADER_SIZE + 4),
            (build_v3_bytes(3), HEADER_SIZE + 8),
        ] {
            let mut buf = buf;
            buf[off..off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            let err = MappedIndex::from_bytes(buf).err().expect("corrupt index must not load");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn corrupt_doc_len_rejected() {
        for (buf, entry) in [
            (build_valid_bytes(), GRAM_ENTRY_V2),
            (build_v3_bytes(4), GRAM_ENTRY_V3),
        ] {
            let mut buf = buf;
            // doc index 先頭エントリの len (eid u64 + offset u32 の後) を巨大値に書き換え
            let gram_count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            let posting_total = u32::from_le_bytes(buf[12..16].try_into().unwrap());
            let doc_base = HEADER_SIZE
                + gram_count as usize * entry
                + posting_padding(gram_count, entry)
                + posting_total as usize * POSTING_ENTRY;
            let len_pos = doc_base + 12;
            buf[len_pos..len_pos + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            let err = MappedIndex::from_bytes(buf).err().expect("corrupt index must not load");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
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
