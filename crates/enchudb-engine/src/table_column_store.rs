//! TableColumnStore — 名前付き table 用の独立 column data file。 β-heavy phase 2。
//!
//! # 動機
//!
//! v5 layout までは main DB file 1 本に全 himo の column data が並ぶ。 これだと:
//! - table drop が O(unlink) で済まない (= 論理 mark + GC 要)
//! - partial export が table 単位で意味を持たない (= 全 column を含む snapshot)
//! - 巨大 table の cold column が hot table の OS page cache に競合
//!
//! 名前付き table の column を **別 file** に切り出すと、 上記が全部 file 操作で済む。
//! anonymous table (= 旧 `define_himo` 経由の himo) は引き続き main file に残置 (互換維持)。
//!
//! # Layout
//!
//! ```text
//! {db_path}.t.{table_name}.col
//!     header (固定 16 byte): magic = "TBLC" + version + reserved
//!     slots: max_himos × himo_col_size の連続領域
//! ```
//!
//! 各 himo は GLOBAL hid を index にして slot を取る。 anonymous table の himo
//! は table column file には存在しない (main file の slot を使う)。 named table
//! の himo は table file の slot を使い、 main file 側の slot は使われない
//! (= 未 commit のまま、 disk size 増加なし)。
//!
//! GrowableMap で virtual reservation = max_himos × slot_size。 commit は触った
//! slot だけ伸びる (= 5 himo の table なら 5 slot 分のみ disk)。
//!
//! # 命名規則
//!
//! `{db_path}.t.{table_name}.col` の `{table_name}` は user 指定。 `.` や `/`
//! が含まれる場合は別途 validate する (この module 自体は受け取った name を
//! sanitize しない、 caller responsibility)。

#![cfg(not(target_arch = "wasm32"))]

use std::fs::{File, OpenOptions};
use std::io;
use std::sync::Arc;

use crate::growable_map::GrowableMap;
use crate::region::Region;

const MAGIC: [u8; 4] = [b'T', b'B', b'L', b'C'];
const FORMAT_VERSION: u32 = 1;
const HEADER: usize = 16;

pub struct TableColumnStore {
    grower: Arc<GrowableMap>,
    slot_size: usize,
    max_himos: u32,
    #[allow(dead_code)]
    table_name: String,
}

impl TableColumnStore {
    /// `{db_path}.t.{table_name}.col` 形式の file path を作る。
    pub fn file_path(db_path: &str, table_name: &str) -> String {
        format!("{}.t.{}.col", db_path, table_name)
    }

    /// 新規 table column file を作る。 truncate して header だけ書く。
    pub fn create(
        db_path: &str,
        table_name: &str,
        max_entities: u32,
        max_himos: u32,
    ) -> io::Result<Self> {
        let path = Self::file_path(db_path, table_name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Self::from_file(file, table_name, max_entities, max_himos, /*initial=*/ 4096, /*init_header=*/ true)
    }

    /// 既存 table column file を open。 不在なら create 同等。
    pub fn open_or_create(
        db_path: &str,
        table_name: &str,
        max_entities: u32,
        max_himos: u32,
    ) -> io::Result<Self> {
        let path = Self::file_path(db_path, table_name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        let cur_len = file.metadata()?.len() as usize;
        let init_header = cur_len < HEADER;
        let initial = cur_len.max(4096);
        Self::from_file(file, table_name, max_entities, max_himos, initial, init_header)
    }

    fn from_file(
        file: File,
        table_name: &str,
        max_entities: u32,
        max_himos: u32,
        initial: usize,
        init_header: bool,
    ) -> io::Result<Self> {
        let slot_size = align8(crate::column::Column::region_size(max_entities, 4));
        let body = slot_size.saturating_mul(max_himos as usize);
        let reserve = align_page(HEADER + body);
        let initial = initial.min(reserve.max(4096));
        let grower = GrowableMap::new(file, reserve, initial)?;

        if init_header {
            let base = grower.base();
            let header = unsafe { std::slice::from_raw_parts_mut(base, HEADER) };
            header[0..4].copy_from_slice(&MAGIC);
            header[4..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
            header[8..16].fill(0);
        } else {
            // 既存 file: magic check
            let base = grower.base();
            let header = unsafe { std::slice::from_raw_parts(base, HEADER) };
            if header[0..4] != MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("TableColumnStore: bad magic in {}.t.{}.col", "<db>", table_name),
                ));
            }
        }

        Ok(Self {
            grower: Arc::new(grower),
            slot_size,
            max_himos,
            table_name: table_name.to_string(),
        })
    }

    /// hid 番目の himo に対応する column Region を返す (table file 内 slot)。
    pub fn region_for(&self, hid: u32) -> Region {
        assert!(
            hid < self.max_himos,
            "TableColumnStore: hid {} >= max_himos {}",
            hid, self.max_himos
        );
        let off = HEADER + (hid as usize) * self.slot_size;
        unsafe { Region::with_grower(self.grower.clone(), off, self.slot_size) }
    }

    /// 全 dirty range を msync。 engine.flush() から呼ぶ。
    pub fn flush(&self) -> io::Result<()> {
        let committed = self.grower.committed();
        if committed == 0 {
            return Ok(());
        }
        self.grower.flush(0, committed)
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

fn align8(v: usize) -> usize {
    (v + 7) & !7
}

fn align_page(v: usize) -> usize {
    const PAGE: usize = 4096;
    (v + PAGE - 1) & !(PAGE - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_db(tag: &str) -> String {
        let p = format!(
            "/tmp/enchudb-tcs-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        p
    }

    fn cleanup_for(db: &str, table: &str) {
        let _ = std::fs::remove_file(TableColumnStore::file_path(db, table));
    }

    #[test]
    fn create_and_region_for() {
        let db = tmp_db("create");
        let tcs = TableColumnStore::create(&db, "users", 100, 8).unwrap();
        let r0 = tcs.region_for(0);
        // Column::init を試す (header を打って region 化)
        let _col = crate::column::Column::init(r0, 4, 100);
        cleanup_for(&db, "users");
    }

    #[test]
    fn slots_are_disjoint() {
        let db = tmp_db("disjoint");
        let tcs = TableColumnStore::create(&db, "users", 100, 4).unwrap();

        // slot 0 と slot 2 にそれぞれ別の column を init
        let r0 = tcs.region_for(0);
        let r2 = tcs.region_for(2);
        let c0 = crate::column::Column::init(r0, 4, 100);
        let c2 = crate::column::Column::init(r2, 4, 100);
        c0.ensure_count(0);
        c0.set(0, &42u32.to_le_bytes());
        c2.ensure_count(0);
        c2.set(0, &99u32.to_le_bytes());

        assert_eq!(u32::from_le_bytes(c0.get(0).try_into().unwrap()), 42);
        assert_eq!(u32::from_le_bytes(c2.get(0).try_into().unwrap()), 99);

        cleanup_for(&db, "users");
    }

    #[test]
    fn open_or_create_persists_across_session() {
        let db = tmp_db("persist");

        {
            let tcs = TableColumnStore::create(&db, "posts", 100, 4).unwrap();
            let r = tcs.region_for(1);
            let c = crate::column::Column::init(r, 4, 100);
            c.ensure_count(0);
            c.set(0, &77u32.to_le_bytes());
            tcs.flush().unwrap();
        }
        {
            let tcs = TableColumnStore::open_or_create(&db, "posts", 100, 4).unwrap();
            let r = tcs.region_for(1);
            let c = crate::column::Column::load(r);
            assert_eq!(u32::from_le_bytes(c.get(0).try_into().unwrap()), 77);
        }
        cleanup_for(&db, "posts");
    }

    #[test]
    fn open_or_create_makes_new_when_absent() {
        let db = tmp_db("new");
        let tcs = TableColumnStore::open_or_create(&db, "fresh", 100, 4).unwrap();
        // slot 0 に何も書いてないので column.count() == 0
        let r = tcs.region_for(0);
        let c = crate::column::Column::init(r, 4, 100);
        assert_eq!(c.count(), 0);
        cleanup_for(&db, "fresh");
    }
}
