//! PositionsSidecar — 全 himo の PositionsRegion を集約する sidecar mmap file。
//!
//! # 動機
//!
//! BucketCylinder.positions を mmap-back するために、 各 himo 用の Region を
//! どこかから供給する必要がある。 main DB file の layout は v5 でフリーズ済み
//! なので、 後付けで slot を挿入できない。 そこで `{path}.positions` という
//! 別 file を新設し、 そこに max_himos 個の固定 size slot を並べる。
//!
//! sidecar 構造はシンプル:
//! ```text
//! offset       size       内容
//! 0            slot_size  himo 0 の PositionsRegion
//! slot_size    slot_size  himo 1 の PositionsRegion
//! 2*slot_size  slot_size  himo 2 の PositionsRegion
//! ...
//! ```
//!
//! 各 slot は PositionsRegion::region_size(max_entities) で算出した固定幅。
//! sidecar 全体は GrowableMap で virtual reservation + 逐次 commit。 まだ
//! 触ってない slot は file 末尾まで届かない → disk size 比例。
//!
//! # lazy init / migration
//!
//! - 新規 DB: create_* で空 sidecar を作る。 各 himo は最初の `init` で
//!   PositionsRegion::init() を slot に対して呼び、 magic を打って初期化。
//! - 既存 v5 DB: open_* で sidecar が無ければ作る。 各 slot は zero-init で
//!   start、 PositionsRegion::has_valid_magic == false なので
//!   PositionsRegion::init() を呼ばせる。 lazy rebuild_from_column が
//!   sidecar slot に positions を populate する。
//! - v5.5 DB: header marker (step 5) で 「positions は最新」 と分かれば
//!   rebuild_from_column を skip し、 sidecar から直接 load する。
//!
//! # multi-process / 並行性
//!
//! main DB file と同じく writer は flock (`.db.lock`) で排他。 sidecar 自体に
//! 別 lock は持たない (main lock の範囲に含まれる)。 reader-only open
//! (`open_readonly`) は sidecar も read-only でマップする。
//!
//! # 障害時の不整合
//!
//! sidecar が rebuild できる派生データ (column が source of truth) なので、
//! 破損したら捨てて再 build 可能。 open 時に magic 不一致を検出したら slot を
//! init() し直して lazy rebuild に任せる。

#![cfg(not(target_arch = "wasm32"))]

use std::fs::{File, OpenOptions};
use std::io;
use std::sync::Arc;

use crate::growable_map::GrowableMap;
use crate::positions_region::PositionsRegion;
use crate::region::Region;

pub struct PositionsSidecar {
    grower: Arc<GrowableMap>,
    slot_size: usize,
    max_himos: u32,
}

impl PositionsSidecar {
    /// 1 himo あたりの slot size (= PositionsRegion::region_size(max_entities) を
    /// 8 byte align)。
    pub fn slot_size_for(max_entities: u32) -> usize {
        let raw = PositionsRegion::region_size(max_entities);
        (raw + 7) & !7
    }

    /// sidecar file path = `{db_path}.positions`。
    pub fn sidecar_path(db_path: &str) -> String {
        format!("{}.positions", db_path)
    }

    /// db_path に対する sidecar を **新規作成**。 既存 file は truncate される。
    pub fn create(db_path: &str, max_entities: u32, max_himos: u32) -> io::Result<Self> {
        let path = Self::sidecar_path(db_path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Self::from_file(file, max_entities, max_himos, /*initial=*/ 4096)
    }

    /// db_path に対する sidecar を **open**。 存在しなければ create と同等。
    /// v5 DB を 0.5.5 engine で open する経路で呼ぶ。
    pub fn open_or_create(
        db_path: &str,
        max_entities: u32,
        max_himos: u32,
    ) -> io::Result<Self> {
        let path = Self::sidecar_path(db_path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        // 既存 file の長さを initial に使う (zero-fill されてれば magic 不一致で
        // lazy init される)。 0 byte なら最小 4096 で start。
        let cur_len = file.metadata()?.len() as usize;
        let initial = cur_len.max(4096);
        Self::from_file(file, max_entities, max_himos, initial)
    }

    fn from_file(
        file: File,
        max_entities: u32,
        max_himos: u32,
        initial: usize,
    ) -> io::Result<Self> {
        let slot_size = Self::slot_size_for(max_entities);
        let reserve = slot_size.saturating_mul(max_himos as usize);
        let initial = initial.min(reserve.max(4096));
        let grower = GrowableMap::new(file, reserve, initial)?;
        Ok(Self {
            grower: Arc::new(grower),
            slot_size,
            max_himos,
        })
    }

    /// hid 番目の himo に対応する PositionsRegion を返す。
    /// region は grower-backed なので `ensure_committed` で安全に伸びる。
    pub fn region_for(&self, hid: u32) -> Region {
        assert!(
            hid < self.max_himos,
            "positions slot: hid {} >= max_himos {}",
            hid, self.max_himos
        );
        let off = (hid as usize) * self.slot_size;
        unsafe { Region::with_grower(self.grower.clone(), off, self.slot_size) }
    }

    /// 既存 file の slot にすでに valid magic があるかどうか。
    /// migration 判定用。 まだ commit されてない slot は false。
    pub fn slot_initialized(&self, hid: u32) -> bool {
        let off = (hid as usize) * self.slot_size;
        // GrowableMap の committed 範囲外なら絶対 zero (= 未 init)
        if off + 4 > self.grower.committed() {
            return false;
        }
        let region = self.region_for(hid);
        PositionsRegion::has_valid_magic(&region)
    }

    /// flush_dirty を呼んで sidecar を msync。 main file の flush と一緒に呼ぶ。
    pub fn flush(&self) -> io::Result<()> {
        self.grower.flush_dirty()
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    pub fn max_himos(&self) -> u32 {
        self.max_himos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_db(tag: &str) -> String {
        let p = format!(
            "/tmp/enchudb-sidecar-test-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let _ = std::fs::remove_file(format!("{}.positions", p));
        p
    }

    fn cleanup(p: &str) {
        let _ = std::fs::remove_file(format!("{}.positions", p));
    }

    #[test]
    fn create_and_region_for() {
        let p = tmp_db("create");
        let s = PositionsSidecar::create(&p, 1024, 8).unwrap();
        let r0 = s.region_for(0);
        let pr = PositionsRegion::init(r0);
        assert!(pr.is_empty());
        cleanup(&p);
    }

    #[test]
    fn slots_are_disjoint() {
        let p = tmp_db("disjoint");
        let s = PositionsSidecar::create(&p, 1024, 4).unwrap();

        // slot 0 / 1 / 2 / 3 にそれぞれ別の eid_offset を打つ
        for hid in 0..4 {
            let pr = PositionsRegion::init(s.region_for(hid));
            pr.set(hid * 100 + 5, 42, 0);
        }
        // 各 slot から読み戻すと別々の eid_offset
        for hid in 0..4 {
            let pr = PositionsRegion::load(s.region_for(hid));
            assert_eq!(pr.eid_offset(), hid * 100 + 5);
            assert_eq!(pr.get(hid * 100 + 5), Some((42, 0)));
        }
        cleanup(&p);
    }

    #[test]
    fn open_or_create_reuses_existing() {
        let p = tmp_db("reuse");

        {
            let s = PositionsSidecar::create(&p, 1024, 4).unwrap();
            let pr = PositionsRegion::init(s.region_for(2));
            pr.set(10, 99, 0);
            s.flush().unwrap();
        }
        {
            let s = PositionsSidecar::open_or_create(&p, 1024, 4).unwrap();
            assert!(s.slot_initialized(2));
            assert!(!s.slot_initialized(0));
            let pr = PositionsRegion::load(s.region_for(2));
            assert_eq!(pr.get(10), Some((99, 0)));
        }
        cleanup(&p);
    }

    #[test]
    fn open_or_create_makes_new_when_absent() {
        let p = tmp_db("new");
        let s = PositionsSidecar::open_or_create(&p, 1024, 4).unwrap();
        assert!(!s.slot_initialized(0));
        cleanup(&p);
    }
}
