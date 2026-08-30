//! `SegmentSet` — v10 の DB 本体 = **directory + region ごとの segment file** ([[request21]])。
//!
//! ```text
//! foo.db/
//!   header.seg                 ← 今までの 4 KB header と byte 互換 (H_* offset そのまま)
//!   entities.seg
//!   vocab.data.seg  vocab.offsets.seg  vocab.index.seg
//!   himoreg.data.seg  himoreg.offsets.seg  himoreg.index.seg
//!   content.index.seg  content.data.seg
//!   leaf.data.seg              ← leaf_data_size > 0 のとき
//!   tomb.seg                   ← sync 参加 DB (cell_version) のとき
//!   himo/0000.seg ...          ← define_himo 時に作る (himo id で命名、 名前は使わない)
//!   ver/0000.seg ...           ← sync 参加 DB のとき、 himo と同じ id
//! ```
//!
//! 規則は **1 region = 1 segment**、 例外なし。 各 segment の byte 列は旧 1 ファイル layout の
//! region と同一 (Column / EntitySet / … の store format は不変) なので、 旧 v9 file は
//! region 境界で切り出すだけで v10 に移行できる (packed 形式 = 旧 layout、 Phase 2)。
//!
//! `.tables` / `.eidmap` / `.vocabmap` / `.oplog` / `.crc` / `.lock` の sidecar は
//! 今までどおり `{path}.xxx` (directory の隣) に置く (Phase 1 では位置を変えない)。
//!
//! # 何が要らなくなったか
//!
//! - `Layout` の offset 算術: 各 region が独立 file なので offset は常に 0。 `Layout` は
//!   **予約サイズ (= 旧 region size)** と、 packed 形式 (`from_bytes` / wasm) のための
//!   offset 表として残る
//! - 「後ろの region を触ると手前が commit される」 (#172): file 長が segment ごと
//! - 「後から生やせるのは末尾だけ」 (#243 の B-lite): 版数列は `ensure_ver` で
//!   いつでも作れる

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::region::Region;
use crate::segment_map::SegmentMap;

/// segment の種類。 `Himo(hid)` / `Ver(hid)` は himo id で識別する (名前は使わない —
/// rename は辞書の書き換えだけで file に触らない)。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SegmentKind {
    Header,
    Entities,
    VocabData,
    VocabOffsets,
    VocabIndex,
    HimoregData,
    HimoregOffsets,
    HimoregIndex,
    ContentIndex,
    ContentData,
    LeafData,
    Tomb,
    Himo(u32),
    Ver(u32),
}

impl SegmentKind {
    /// 常に存在する固定 segment (header を除く)。 `LeafData` / `Tomb` は条件付き。
    pub const FIXED: [SegmentKind; 10] = [
        SegmentKind::Entities,
        SegmentKind::VocabData,
        SegmentKind::VocabOffsets,
        SegmentKind::VocabIndex,
        SegmentKind::HimoregData,
        SegmentKind::HimoregOffsets,
        SegmentKind::HimoregIndex,
        SegmentKind::ContentIndex,
        SegmentKind::ContentData,
        SegmentKind::LeafData,
    ];

    pub fn rel_path(self) -> PathBuf {
        match self {
            SegmentKind::Header => "header.seg".into(),
            SegmentKind::Entities => "entities.seg".into(),
            SegmentKind::VocabData => "vocab.data.seg".into(),
            SegmentKind::VocabOffsets => "vocab.offsets.seg".into(),
            SegmentKind::VocabIndex => "vocab.index.seg".into(),
            SegmentKind::HimoregData => "himoreg.data.seg".into(),
            SegmentKind::HimoregOffsets => "himoreg.offsets.seg".into(),
            SegmentKind::HimoregIndex => "himoreg.index.seg".into(),
            SegmentKind::ContentIndex => "content.index.seg".into(),
            SegmentKind::ContentData => "content.data.seg".into(),
            SegmentKind::LeafData => "leaf.data.seg".into(),
            SegmentKind::Tomb => "tomb.seg".into(),
            SegmentKind::Himo(h) => format!("himo/{h:04}.seg").into(),
            SegmentKind::Ver(h) => format!("ver/{h:04}.seg").into(),
        }
    }

    pub fn name(self) -> String {
        match self {
            SegmentKind::Himo(h) => format!("himo[{h}]"),
            SegmentKind::Ver(h) => format!("ver[{h}]"),
            other => format!("{other:?}").to_lowercase(),
        }
    }
}

/// kind → 予約サイズ (= 旧 layout の region size)。 engine の `Layout` が実装する。
pub trait SegmentSizes {
    fn segment_size(&self, kind: SegmentKind) -> usize;

    /// mmap の予約長。 既定は `segment_size` と同じ。 entity 比例の segment は cap を伸ばせる
    /// よう reservation 分 (v10 Phase 3、 `Layout::segment_reserve`) を返す。
    fn segment_reserve(&self, kind: SegmentKind) -> usize {
        self.segment_size(kind)
    }
}

/// create 時の初期 commit。 store の header (16〜64 B) が入れば足り、 あとは
/// `ensure_committed` が書きに応じて伸ばす。
const INITIAL_COMMIT: usize = 4096;

pub struct SegmentSet {
    dir: PathBuf,
    fixed: HashMap<SegmentKind, Arc<SegmentMap>>,
    himo: RwLock<Vec<Arc<SegmentMap>>>,
    ver: RwLock<Vec<Arc<SegmentMap>>>,
    tomb: RwLock<Option<Arc<SegmentMap>>>,
    /// 直前に書いた manifest の中身。 segment 長は grow でしか動かないので、 同じなら
    /// 書き直さない (`body_msync` は sync 経路から毎回呼ばれる)。 Mutex は 「同時に
    /// 2 本が manifest を書く」 のを止める役も兼ねる。
    last_manifest: Mutex<Option<Vec<(String, u64)>>>,
}

impl SegmentSet {
    fn path_of(dir: &Path, kind: SegmentKind) -> PathBuf {
        dir.join(kind.rel_path())
    }

    /// 新規 DB directory を作る。 `dir` は存在してはいけない (`AlreadyExists`)。
    /// himo / ver の segment はここでは作らない (`ensure_himo` / `ensure_ver`)。
    pub fn create(
        dir: &Path,
        sizes: &dyn SegmentSizes,
        with_leaf: bool,
        cell_version: bool,
    ) -> io::Result<Self> {
        // directory 自体は呼び出し側が作る (`Engine` は `mkdir` の atomic 性で同時 create を
        // 排他し、 その中に lock を置いてから来る)。 ここでは header.seg の有無で二重 create
        // を拒む。
        std::fs::create_dir_all(dir)?;
        if Self::path_of(dir, SegmentKind::Header).exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "database already exists: \"{}\" — refusing to overwrite (created concurrently?)",
                    dir.display()
                ),
            ));
        }
        std::fs::create_dir_all(dir.join("himo"))?;
        std::fs::create_dir_all(dir.join("ver"))?;
        let mut fixed = HashMap::new();
        let mut kinds: Vec<SegmentKind> = vec![SegmentKind::Header];
        kinds.extend(SegmentKind::FIXED.iter().copied().filter(|k| with_leaf || *k != SegmentKind::LeafData));
        for kind in kinds {
            let size = sizes.segment_size(kind);
            if size == 0 {
                continue;
            }
            // header は表 (himo 型 / max_values) が全域に散るので最初から全 commit
            let initial = if kind == SegmentKind::Header { size } else { INITIAL_COMMIT.min(size) };
            let seg = SegmentMap::create(&Self::path_of(dir, kind), sizes.segment_reserve(kind), initial)?;
            fixed.insert(kind, Arc::new(seg));
        }
        let tomb = if cell_version {
            let size = sizes.segment_size(SegmentKind::Tomb);
            Some(Arc::new(SegmentMap::create(
                &Self::path_of(dir, SegmentKind::Tomb),
                sizes.segment_reserve(SegmentKind::Tomb),
                INITIAL_COMMIT.min(size),
            )?))
        } else {
            None
        };
        let set = Self {
            dir: dir.to_path_buf(),
            fixed,
            himo: RwLock::new(Vec::new()),
            ver: RwLock::new(Vec::new()),
            tomb: RwLock::new(tomb),
            last_manifest: Mutex::new(None),
        };
        // create の最後に manifest を書く。 これで 「manifest が無い = create が完了して
        // いない」 と言い切れる (`Engine::probe` が Incomplete を返す根拠)。
        set.write_manifest()?;
        Ok(set)
    }

    /// 既存 DB directory を開く。 `himo_count` / `cell_version` / `with_leaf` は header
    /// から読んだ値 (`Engine::read_header`)。 header が指す segment が無ければ `InvalidData`。
    pub fn open(
        dir: &Path,
        sizes: &dyn SegmentSizes,
        himo_count: u32,
        with_leaf: bool,
        cell_version: bool,
        readonly: bool,
    ) -> io::Result<Self> {
        let known = verify_manifest(dir)?;
        let open_one = |kind: SegmentKind| -> io::Result<Arc<SegmentMap>> {
            let p = Self::path_of(dir, kind);
            SegmentMap::open(&p, sizes.segment_reserve(kind), readonly)
                .map(Arc::new)
                .map_err(|e| {
                    io::Error::new(
                        e.kind(),
                        format!("segment {} ({}): {}", kind.name(), p.display(), e),
                    )
                })
        };
        let mut fixed = HashMap::new();
        fixed.insert(SegmentKind::Header, open_one(SegmentKind::Header)?);
        for kind in SegmentKind::FIXED {
            if kind == SegmentKind::LeafData && !with_leaf {
                continue;
            }
            if sizes.segment_size(kind) == 0 {
                continue;
            }
            fixed.insert(kind, open_one(kind)?);
        }
        let mut himo = Vec::with_capacity(himo_count as usize);
        let mut ver = Vec::new();
        for hid in 0..himo_count {
            himo.push(open_one(SegmentKind::Himo(hid))?);
            if cell_version {
                ver.push(open_one(SegmentKind::Ver(hid))?);
            }
        }
        let tomb = if cell_version { Some(open_one(SegmentKind::Tomb)?) } else { None };
        Ok(Self {
            dir: dir.to_path_buf(),
            fixed,
            himo: RwLock::new(himo),
            ver: RwLock::new(ver),
            tomb: RwLock::new(tomb),
            last_manifest: Mutex::new(known),
        })
    }

    /// directory の header.seg を先頭 `len` byte 読む (open 前の検証用、 mmap しない)。
    pub fn read_header(dir: &Path, len: usize) -> io::Result<Vec<u8>> {
        use std::io::Read;
        let p = Self::path_of(dir, SegmentKind::Header);
        let mut f = std::fs::File::open(&p)?;
        let mut buf = vec![0u8; len];
        let actual = f.metadata().map(|m| m.len()).unwrap_or(0);
        f.read_exact(&mut buf).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("header segment {} too small: {} bytes (truncated? need {})", p.display(), actual, len),
            )
        })?;
        Ok(buf)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn segment(&self, kind: SegmentKind) -> Option<Arc<SegmentMap>> {
        match kind {
            SegmentKind::Himo(h) => self.himo.read().unwrap().get(h as usize).cloned(),
            SegmentKind::Ver(h) => self.ver.read().unwrap().get(h as usize).cloned(),
            SegmentKind::Tomb => self.tomb.read().unwrap().clone(),
            other => self.fixed.get(&other).cloned(),
        }
    }

    pub fn has(&self, kind: SegmentKind) -> bool {
        self.segment(kind).is_some()
    }

    /// segment 全体の `Region` (offset 0、 長さ = 予約 = 旧 region size)。
    /// 存在しない segment は呼び側の契約違反 (header と directory の不整合は open が弾く)。
    pub fn region(&self, kind: SegmentKind) -> Region {
        let seg = self
            .segment(kind)
            .unwrap_or_else(|| panic!("segment {} is not open in {}", kind.name(), self.dir.display()));
        let len = seg.reserved();
        unsafe { Region::from_segment(seg, len) }
    }

    fn create_or_open(&self, kind: SegmentKind, size: usize) -> io::Result<Arc<SegmentMap>> {
        let p = Self::path_of(&self.dir, kind);
        let seg = if p.exists() {
            // crash で file だけ残った (header の count 更新前) 場合の回収
            SegmentMap::open(&p, size, false)?
        } else {
            SegmentMap::create(&p, size, INITIAL_COMMIT.min(size))?
        };
        Ok(Arc::new(seg))
    }

    /// `himo/{hid}.seg` を用意する。 hid は連番で来る前提 (= `himos.len()`)。
    pub fn ensure_himo(&self, hid: u32, size: usize) -> io::Result<()> {
        let mut v = self.himo.write().unwrap();
        if (hid as usize) < v.len() {
            return Ok(());
        }
        if hid as usize != v.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("himo segment {hid} requested but only {} exist (ids must be sequential)", v.len()),
            ));
        }
        v.push(self.create_or_open(SegmentKind::Himo(hid), size)?);
        Ok(())
    }

    pub fn ensure_ver(&self, hid: u32, size: usize) -> io::Result<()> {
        let mut v = self.ver.write().unwrap();
        if (hid as usize) < v.len() {
            return Ok(());
        }
        if hid as usize != v.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ver segment {hid} requested but only {} exist (ids must be sequential)", v.len()),
            ));
        }
        v.push(self.create_or_open(SegmentKind::Ver(hid), size)?);
        Ok(())
    }

    pub fn ensure_tomb(&self, size: usize) -> io::Result<()> {
        let mut t = self.tomb.write().unwrap();
        if t.is_some() {
            return Ok(());
        }
        *t = Some(self.create_or_open(SegmentKind::Tomb, size)?);
        Ok(())
    }

    /// header segment の先頭 `len` byte を `&mut [u8]` で。 旧 `Backing::header_mut` と
    /// 同じ前提 (書くのは himo_def_lock 下 / open 時のみ)。
    #[allow(clippy::mut_from_ref)]
    pub fn header_slice_mut(&self, len: usize) -> &mut [u8] {
        let seg = &self.fixed[&SegmentKind::Header];
        unsafe { std::slice::from_raw_parts_mut(seg.base(), len.min(seg.committed())) }
    }

    /// 開いている全 segment を (kind, map) で列挙する。 CRC / stats / flush 用。
    pub fn all(&self) -> Vec<(SegmentKind, Arc<SegmentMap>)> {
        let mut out: Vec<(SegmentKind, Arc<SegmentMap>)> =
            self.fixed.iter().map(|(k, s)| (*k, s.clone())).collect();
        for (i, s) in self.himo.read().unwrap().iter().enumerate() {
            out.push((SegmentKind::Himo(i as u32), s.clone()));
        }
        for (i, s) in self.ver.read().unwrap().iter().enumerate() {
            out.push((SegmentKind::Ver(i as u32), s.clone()));
        }
        if let Some(t) = self.tomb.read().unwrap().as_ref() {
            out.push((SegmentKind::Tomb, t.clone()));
        }
        out
    }

    /// commit 済み全域を msync (旧 `Backing::flush_to_disk`)。
    pub fn flush_all(&self) -> io::Result<()> {
        for (_, s) in self.all() {
            s.flush_all()?;
        }
        Ok(())
    }

    /// dirty range だけ msync (旧 growable の `body_msync`)。 `mark_dirty` を通さない
    /// store (EntitySet / header) は全域。
    pub fn flush_dirty_all(&self) -> io::Result<()> {
        for (k, s) in self.all() {
            match k {
                SegmentKind::Header | SegmentKind::Entities => s.flush_all()?,
                _ => s.flush_dirty()?,
            }
        }
        Ok(())
    }

    pub fn flush_kind(&self, kind: SegmentKind, off: usize, len: usize) -> io::Result<()> {
        match self.segment(kind) {
            Some(s) => s.flush_aligned(off, len),
            None => Ok(()),
        }
    }

    /// 別 process の writer が伸ばした分を全 segment で追従する (readonly reader 用)。
    pub fn refresh_all(&self) -> io::Result<()> {
        for (_, s) in self.all() {
            s.refresh()?;
        }
        Ok(())
    }

    // ── #167 観測 ──

    pub fn disk_free_bytes(&self) -> Option<u64> {
        self.fixed.get(&SegmentKind::Header).and_then(|s| s.free_bytes().ok())
    }

    pub fn space_denials(&self) -> u64 {
        self.all().iter().map(|(_, s)| s.space_denials()).sum()
    }

    pub fn set_space_margin(&self, bytes: u64) {
        for (_, s) in self.all() {
            s.set_space_margin(bytes);
        }
    }

    /// 各 segment の (kind, file 長, 予約) — apparent / 物理の観測用。
    pub fn stats(&self) -> Vec<(SegmentKind, u64, usize)> {
        self.all()
            .into_iter()
            .map(|(k, s)| (k, s.file_len().unwrap_or(0), s.reserved()))
            .collect()
    }

    /// 現在の segment 長を manifest sidecar (`segments`) に焼く。
    ///
    /// flush の最後に呼ぶ。 「この時点で各 segment はこの長さだった」 という記録で、
    /// 次の open が **短くなっていないか** を stat だけで確かめられる。 segment file は
    /// 伸びるだけ (`set_len` は grow と page 揃えにしか使わない) なので、 記録は下限として
    /// 常に有効。
    pub fn write_manifest(&self) -> io::Result<()> {
        let mut lens: Vec<(String, u64)> = Vec::new();
        for (k, sgm) in self.all() {
            let rel = k.rel_path().to_string_lossy().to_string();
            let len = sgm.file_len().map_err(|e| {
                io::Error::new(e.kind(), format!("segments manifest: stat {rel}: {e}"))
            })?;
            lens.push((rel, len));
        }
        lens.sort();
        // 書く必要があるか (= 前回から伸びたか) を lock 内で判定する。 sync 経路は
        // `body_msync` を毎回通るが、 segment が伸びるのは稀。
        let mut last = self.last_manifest.lock().unwrap();
        if last.as_deref() == Some(lens.as_slice()) {
            return Ok(());
        }
        write_manifest_entries(&self.dir, &lens).map_err(|e| {
            io::Error::new(e.kind(), format!("segments manifest: {}: {e}", self.dir.display()))
        })?;
        *last = Some(lens);
        Ok(())
    }
}

/// header が指す segment file が全部あるか (mmap せずに存在だけ確認する)。
///
/// 「create が途中で落ちた directory」 と 「segment を消された DB」 を open 前に見分けるため。
/// [`SegmentSet::open`] と同じ集合を、 同じ引数から数える。
pub fn missing_segments(
    dir: &Path,
    sizes: &dyn SegmentSizes,
    himo_count: u32,
    with_leaf: bool,
    cell_version: bool,
) -> Vec<String> {
    let mut want = vec![SegmentKind::Header];
    for kind in SegmentKind::FIXED {
        if kind == SegmentKind::LeafData && !with_leaf {
            continue;
        }
        if sizes.segment_size(kind) == 0 {
            continue;
        }
        want.push(kind);
    }
    for hid in 0..himo_count {
        want.push(SegmentKind::Himo(hid));
        if cell_version {
            want.push(SegmentKind::Ver(hid));
        }
    }
    if cell_version {
        want.push(SegmentKind::Tomb);
    }
    want
        .into_iter()
        .filter(|k| !dir.join(k.rel_path()).is_file())
        .map(|k| k.rel_path().to_string_lossy().to_string())
        .collect()
}

static VERIFY_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static VERIFY_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct VerifyTimer(std::time::Instant);
impl Drop for VerifyTimer {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        VERIFY_COUNT.fetch_add(1, Ordering::Relaxed);
        VERIFY_NANOS.fetch_add(self.0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }
}

/// これまでの `verify_manifest` (全 segment の stat) の (回数, 合計 ns)。 診断用。
pub fn verify_stats() -> (u64, u64) {
    use std::sync::atomic::Ordering;
    (VERIFY_COUNT.load(Ordering::Relaxed), VERIFY_NANOS.load(Ordering::Relaxed))
}

/// 診断 counter を 0 に戻す。
#[doc(hidden)]
pub fn reset_verify_stats() {
    use std::sync::atomic::Ordering;
    VERIFY_COUNT.store(0, Ordering::Relaxed);
    VERIFY_NANOS.store(0, Ordering::Relaxed);
}

/// manifest の 1 行目。 将来 format を変えたら上げる。
const MANIFEST_V1: &str = "enchudb-segments v1";
/// 末尾標識 (`end <行数>`)。 途中で切れた manifest を 「無い」 扱いにするため。
const MANIFEST_END: &str = "end";

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(crate::db_files::SEGMENTS)
}

fn write_manifest_entries(dir: &Path, lens: &[(String, u64)]) -> io::Result<()> {
    let mut out = String::from(MANIFEST_V1);
    out.push('\n');
    let mut sorted: Vec<&(String, u64)> = lens.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    for (rel, len) in &sorted {
        out.push_str(rel);
        out.push(' ');
        out.push_str(&len.to_string());
        out.push('\n');
    }
    // 末尾標識。 これが無い manifest は 「書きかけ」 とみなして検証を飛ばす
    // (fsync しないので、 crash で中途半端な内容が残る可能性がある)。
    out.push_str(&format!("{MANIFEST_END} {}\n", sorted.len()));
    // tmp は書き手ごとに固有名 (別 process の writer が同じ dir を触っても奪い合わない)。
    let path = manifest_path(dir);
    let tmp = dir.join(format!(
        "{}.{}.{:?}.tmp",
        crate::db_files::SEGMENTS,
        std::process::id(),
        std::thread::current().id()
    ));
    let r = (|| -> io::Result<()> {
        use std::io::Write;
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(out.as_bytes())?;
            // **fsync しない**。 manifest は durability の記録ではなく 「前回 flush 時点の
            // 長さ」 のヒントで、 失われても検証を飛ばすだけ (segment は伸びるだけなので、
            // 古い manifest も下限として有効)。 APFS の fsync は 1 回 ~8 ms あり、 flush /
            // drop / snapshot の全部に乗ると 2 倍以上遅くなる。 journaling FS では
            // rename が durable なら先行する ftruncate も durable なので、 順序も保たれる。
        }
        if let Ok(md) = std::fs::metadata(&path) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(
                    &tmp,
                    std::fs::Permissions::from_mode(md.permissions().mode() & 0o777),
                );
            }
            #[cfg(not(unix))]
            let _ = md;
        }
        std::fs::rename(&tmp, &path)
    })();
    if r.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    r
}

/// directory を歩いて manifest を作り直す (migrate / unpack のように SegmentSet を
/// 経由せずに segment を書いた後で使う)。
pub fn write_manifest_from_dir(dir: &Path) -> io::Result<()> {
    let mut lens = Vec::new();
    collect_segments(dir, "", &mut lens)?;
    write_manifest_entries(dir, &lens)
}

fn collect_segments(dir: &Path, prefix: &str, out: &mut Vec<(String, u64)>) -> io::Result<()> {
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().to_string();
        let rel = if prefix.is_empty() { name.clone() } else { format!("{prefix}/{name}") };
        let md = e.metadata()?;
        if md.is_dir() {
            collect_segments(&e.path(), &rel, out)?;
        } else if name.ends_with(".seg") {
            out.push((rel, md.len()));
        }
    }
    Ok(())
}

/// manifest があれば、 記録より短い / 消えた segment が無いか確かめる。
///
/// v10 は 「まだ書いていない segment」 も 0 byte なので、 **file 長だけでは切り詰めと
/// 区別が付かない**。 manifest はその区別を付けるためだけにある (`stat` を十数回するだけで、
/// `.crc` (`seal_integrity`) のような全域走査は要らない)。 manifest が無い DB
/// (v10 以前に作られたもの / 手で組んだもの) は検証を飛ばす。
fn parse_manifest(text: &str) -> Option<Vec<(String, u64)>> {
    let mut lines = text.lines();
    if lines.next() != Some(MANIFEST_V1) {
        return None;
    }
    let mut out = Vec::new();
    for line in lines {
        if let Some(rest) = line.strip_prefix(MANIFEST_END) {
            // `end <行数>` が一致して初めて完全な manifest とみなす。
            return match rest.trim().parse::<usize>() {
                Ok(n) if n == out.len() => Some(out),
                _ => None,
            };
        }
        let (rel, len) = line.rsplit_once(' ')?;
        out.push((rel.to_string(), len.parse().ok()?));
    }
    None // end 行が無い = 書きかけ
}

pub fn verify_manifest(dir: &Path) -> io::Result<Option<Vec<(String, u64)>>> {
    let _t = VerifyTimer(std::time::Instant::now());
    let text = match std::fs::read_to_string(manifest_path(dir)) {
        Ok(t) => t,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    // 壊れている / 途中で切れているなら 「無い」 と同じ扱い (検証を飛ばす)。 誤検知で
    // 健全な DB を開けなくする方が害が大きい。
    let Some(entries) = parse_manifest(&text) else { return Ok(None) };
    for (rel, want) in &entries {
        let want = *want;
        let path = dir.join(rel);
        match std::fs::metadata(&path) {
            Ok(md) if md.len() >= want => {}
            Ok(md) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "segment {rel} is truncated: {} bytes, was {want} at last flush \
                         (partial copy or interrupted transfer?)",
                        md.len()
                    ),
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("segment {rel} is missing (was {want} bytes at last flush)"),
                ));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(Some(entries))
}
