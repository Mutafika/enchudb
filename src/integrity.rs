//! v29 ページチェックサム — region 単位の FNV-1a CRC。
//!
//! # 設計
//!
//! - 別ファイル `{db_path}.crc` に CRC テーブルを保存
//! - 明示的な `flush()` 時に各 region の CRC を再計算 → `.crc` ファイル更新
//! - open() 時に `.crc` があれば全 region を検証、不一致は region ID 付きエラー
//! - `.crc` 欠損は許容(v28 以前の DB と後方互換)
//! - auto-fsync(100ms 周期) では CRC を計算しない(hot path から外す)
//!
//! # ファイル形式
//!
//! ```text
//! [magic "ECRC" 4B]
//! [version u32 = 1]
//! [db_file_size u64] — 対応する .db のサイズ(mismatch は「古い .crc」検出に使う)
//! [max_himos u32]
//! [crc 表: u32 × (max_himos + 4)]
//!   - [0..max_himos]     : himo column CRCs
//!   - [max_himos + 0]    : vocab data CRC
//!   - [max_himos + 1]    : himoreg data CRC
//!   - [max_himos + 2]    : content data CRC
//!   - [max_himos + 3]    : entity set CRC
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

const CRC_MAGIC: &[u8; 4] = b"ECRC";
const CRC_VERSION: u32 = 1;
const CRC_HEADER_SIZE: usize = 20; // magic(4) + version(4) + file_size(8) + max_himos(4)

/// region 種別。数値はファイル内の table index 計算用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegionKind {
    HimoColumn(u32), // himo_id
    Vocab,
    HimoReg,
    Content,
    EntitySet,
}

impl RegionKind {
    pub fn name(&self) -> String {
        match self {
            RegionKind::HimoColumn(h) => format!("himo_column[{}]", h),
            RegionKind::Vocab => "vocab".into(),
            RegionKind::HimoReg => "himoreg".into(),
            RegionKind::Content => "content".into(),
            RegionKind::EntitySet => "entity_set".into(),
        }
    }
}

/// CRC テーブル。インメモリ表現。
pub struct CrcTable {
    pub max_himos: u32,
    pub db_file_size: u64,
    /// crc[0..max_himos] : himo column CRCs (未使用 himo は 0)
    /// crc[max_himos + 0] : vocab
    /// crc[max_himos + 1] : himoreg
    /// crc[max_himos + 2] : content
    /// crc[max_himos + 3] : entity set
    pub crc: Vec<u32>,
}

impl CrcTable {
    pub fn new(max_himos: u32, db_file_size: u64) -> Self {
        let n = max_himos as usize + 4;
        Self { max_himos, db_file_size, crc: vec![0; n] }
    }

    pub fn set(&mut self, kind: RegionKind, crc: u32) {
        let idx = self.index(kind);
        self.crc[idx] = crc;
    }

    pub fn get(&self, kind: RegionKind) -> u32 {
        self.crc[self.index(kind)]
    }

    fn index(&self, kind: RegionKind) -> usize {
        match kind {
            RegionKind::HimoColumn(h) => h as usize,
            RegionKind::Vocab => self.max_himos as usize,
            RegionKind::HimoReg => self.max_himos as usize + 1,
            RegionKind::Content => self.max_himos as usize + 2,
            RegionKind::EntitySet => self.max_himos as usize + 3,
        }
    }

    /// ファイルに書き出す + fsync。
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        f.write_all(CRC_MAGIC)?;
        f.write_all(&CRC_VERSION.to_le_bytes())?;
        f.write_all(&self.db_file_size.to_le_bytes())?;
        f.write_all(&self.max_himos.to_le_bytes())?;
        for c in &self.crc {
            f.write_all(&c.to_le_bytes())?;
        }
        f.sync_data()?;
        Ok(())
    }

    /// ファイルから読み込む。欠損は None、破損は Err。
    pub fn load(path: &Path) -> io::Result<Option<Self>> {
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut header = [0u8; CRC_HEADER_SIZE];
        f.read_exact(&mut header)?;
        if &header[0..4] != CRC_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad .crc magic"));
        }
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if version != CRC_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported .crc version {}", version),
            ));
        }
        let db_file_size = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let max_himos = u32::from_le_bytes(header[16..20].try_into().unwrap());
        let n = max_himos as usize + 4;
        let mut crc = vec![0u32; n];
        for slot in &mut crc {
            let mut b = [0u8; 4];
            f.read_exact(&mut b)?;
            *slot = u32::from_le_bytes(b);
        }
        Ok(Some(Self { max_himos, db_file_size, crc }))
    }

    /// 既存 table と expected(再計算した) table を比較。不一致な region を返す。
    pub fn diff(&self, expected: &CrcTable) -> Vec<RegionKind> {
        let mut out = Vec::new();
        for h in 0..self.max_himos {
            let k = RegionKind::HimoColumn(h);
            if self.get(k) != expected.get(k) { out.push(k); }
        }
        for k in [RegionKind::Vocab, RegionKind::HimoReg, RegionKind::Content, RegionKind::EntitySet] {
            if self.get(k) != expected.get(k) { out.push(k); }
        }
        out
    }
}

/// FNV-1a 32bit(v28 wal.rs と同じアルゴリズム、共通化候補だが切り出さず重複)
#[inline]
pub fn fnv1a_region(data: &[u8]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// 複数のスライスを連結した扱いで CRC(連結メモリコピー回避)。
#[inline]
pub fn fnv1a_slices(slices: &[&[u8]]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for s in slices {
        for &b in *s {
            h ^= b as u32;
            h = h.wrapping_mul(0x01000193);
        }
    }
    h
}

/// DB ファイルパスから対応する .crc ファイルパスを導出。
pub fn crc_path_for(db_path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.crc", db_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let tmp = std::env::temp_dir().join(format!("enchudb-crc-test-{}", std::process::id()));
        let mut t = CrcTable::new(4, 1024);
        t.set(RegionKind::HimoColumn(0), 0xaaaa);
        t.set(RegionKind::HimoColumn(2), 0xcccc);
        t.set(RegionKind::Vocab, 0xdead);
        t.set(RegionKind::Content, 0xbeef);
        t.save(&tmp).unwrap();

        let loaded = CrcTable::load(&tmp).unwrap().unwrap();
        assert_eq!(loaded.get(RegionKind::HimoColumn(0)), 0xaaaa);
        assert_eq!(loaded.get(RegionKind::HimoColumn(2)), 0xcccc);
        assert_eq!(loaded.get(RegionKind::Vocab), 0xdead);
        assert_eq!(loaded.get(RegionKind::Content), 0xbeef);
        assert_eq!(loaded.db_file_size, 1024);

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn missing_file_returns_none() {
        let path = std::env::temp_dir().join(format!("enchudb-crc-missing-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        assert!(CrcTable::load(&path).unwrap().is_none());
    }

    #[test]
    fn diff_finds_mismatched_regions() {
        let mut a = CrcTable::new(4, 1024);
        let mut b = CrcTable::new(4, 1024);
        a.set(RegionKind::HimoColumn(0), 1);
        a.set(RegionKind::Vocab, 2);
        b.set(RegionKind::HimoColumn(0), 99); // 異なる
        b.set(RegionKind::Vocab, 2);            // 同じ
        let d = a.diff(&b);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0], RegionKind::HimoColumn(0));
    }
}
