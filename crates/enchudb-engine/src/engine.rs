//! Engine — 量子円柱。単一ファイル。全コンポーネントが1つのmmapを共有。
//!
//!   entity() → ID 振る
//!   tie_text → 文字列を紐で張る（Vocabulary 経由）
//!   tie     → u32 値を紐で張る
//!   untie → 紐を外す
//!   content/get_content → 非索引テキスト
//!   query → 円柱の重なりを一発で返す
//!   delete → entity 削除
//!   commit → WAL Commit marker append (WAL 有効時のみ意味あり)
//!   open/flush → 永続化（mmap なので open は即利用可）

#[cfg(not(target_arch = "wasm32"))]
use std::fs::OpenOptions;
use std::io;

#[cfg(not(target_arch = "wasm32"))]
use crate::segments::{SegmentKind, SegmentSet, SegmentSizes};

// std::time::Instant panics on wasm32-unknown-unknown ("time not implemented").
// load_from_backing's [open_profile] timer is on the wasm read path, so alias
// Instant to the web-time shim there.
#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;
#[cfg(target_arch = "wasm32")]
use web_time::Instant;

use crate::region::Region;

/// get_entity の戻り値。
#[derive(Debug, PartialEq)]
pub enum EntityValue<'a> {
    Num(u32),
    Text(&'a [u8]),
    Content(&'a [u8]),
}

/// table の eid 枠の使用状況。 `Engine::table_eid_usage` の戻り値。
///
/// 枠 (`capacity`) は create 時に固定で、 後から伸ばせない。 溢れると
/// `entity_in` が `Err` を返し、 **アプリの掃引がそこで止まる** — 掃引が
/// 止まると削除も流れなくなり、 削除は枠を空ける唯一の手段なので回復不能に
/// なる。 その手前で気付けるように、 残量を公式に問い合わせられるようにした。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableEidUsage {
    /// 枠の総数 (= `eid_range_hi - eid_range_lo`)。
    pub capacity: u32,
    /// これまでに払い出した最大 (= `next_local`)。 削除で空いた分は含んだまま。
    pub allocated: u32,
    /// いま生きている行数。
    pub live: u32,
    /// あと何行入るか (= `capacity - live`)。 削除で空いた slot は
    /// `entity_in` が free list 経由で再利用するのでここに戻る。
    pub free: u32,
}

/// Engine の実行時状態スナップショット。
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub entity_count: u32,
    pub himo_count: u32,
    /// WAL の次 append 位置(byte offset)
    pub oplog_head: u64,
    /// 本体へ反映 + fsync 済みの位置
    pub oplog_checkpoint: u64,
    /// WAL ファイル容量(設定値、sparse)
    pub oplog_capacity: u64,
    /// head - checkpoint。大きいと未 fsync が溜まっている
    pub oplog_lag_bytes: u64,
    /// 発行済みの最大 LSN
    pub oplog_next_lsn: u64,
    /// fsync/msync 完了済みの LSN(背景 fsync が進めた地点)
    pub durable_lsn: u64,
    /// WriteQueue に滞留中の op 数
    pub queue_len: usize,
    /// writer が push した累計
    pub pushed: u64,
    /// consumer が apply した累計
    pub applied: u64,
    /// 自 peer の peer_id(単独 peer 運用では 0)
    pub peer_id: u32,
    /// **pre-v9 DB の**揮発版数置き場のエントリ数。 v9 DB では版数が
    /// per-cell version column に載るので常に 0 (走査が O(entity × himo) に
    /// なるため column 側の集計は取らない)。
    pub hlc_entries: usize,
    /// 同上 — pre-v9 DB の揮発置き場の最大 HLC。 v9 DB では None。
    pub max_hlc: Option<enchudb_oplog::Hlc>,
    /// #178: 「自分が書いた行が後から foreign identity に束ねられた」 累計。
    /// `> 0` なら相手側に PK 無しの重複行が生えている可能性がある。 0 が常態。
    pub bind_over_local_writes: u64,
}

/// 0.8.16 (issue #54): vocab の orphan (= 死蔵 vid) 検出スナップショット。
/// `ValueType::Leaf` の値は `vocab.insert` (常に新 vid 払出) で書かれるため、
/// re-tie / remove で旧 vid が himo から外れても vocab data 側は残置する。
/// 同 vid を参照する live cell が皆無の vid を orphan として計上する。
///
/// 計測は read-only で、 vocab 自体は変更しない。 `Tag` himo (= `get_or_insert`
/// 経由で dedup) も live set の collection 対象 (= ある vid を Tag が参照中なら
/// Leaf が後から orphan にしても live 判定)。
/// **capacity 到達・edge 値・破損 file のような 「想定内だが続行不能」 な事象** (#59)。
///
/// embedded DB は他人の process に埋め込まれる。 「DB が一杯」 「値が範囲外」 で
/// `panic!` すると host app ごと落ちるし、 FFI 境界を unwind すれば未定義動作になる。
/// そこで engine 内部ではこれらを panic にせず、
///
/// 1. その write を **拒否** し (壊れた値を書かない)
/// 2. 種別ごとに **計数** し (`Engine::fault_count`)
/// 3. **rate-limited に warn** する (黙って落とさない)
///
/// という扱いに統一する。 `Result` を返せる API では併せて `Err` を返す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// entity 枠が満杯 (max_entities 到達 + free stack 空)
    EntitySpace,
    /// content data 領域が満杯 (既定 512 MB)
    ContentSpace,
    /// vocabulary が満杯 (vocab_max_entries 到達)
    VocabSpace,
    /// 値が cell に入らない (`u32::MAX` は sentinel 予約)
    ValueOutOfRange,
    /// #167: filesystem の空き容量不足で DB を伸ばせない
    DiskSpace,
}

impl FaultKind {
    pub(crate) const COUNT: usize = 5;
    pub(crate) fn index(self) -> usize {
        match self {
            FaultKind::EntitySpace => 0,
            FaultKind::ContentSpace => 1,
            FaultKind::VocabSpace => 2,
            FaultKind::ValueOutOfRange => 3,
            FaultKind::DiskSpace => 4,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            FaultKind::EntitySpace => "entity space exhausted",
            FaultKind::ContentSpace => "content space exhausted",
            FaultKind::VocabSpace => "vocabulary full",
            FaultKind::ValueOutOfRange => "value out of range",
            FaultKind::DiskSpace => "filesystem is (nearly) full",
        }
    }
}

/// `remote_*_apply` (sync 受信の apply) の結果 (#210)。
///
/// 旧 `bool` は 「適用した / しなかった」 しか区別できず、 **「LWW で古い」 と
/// 「容量が無くて置けなかった」 が同じ `false` に潰れていた**。 sync 側はそれを
/// `SkippedOlder` (= 再配送不要) として計上するため、 **ディスク満杯や content
/// 天井に当たった record は cursor を越えられて恒久的に失われる**。
///
/// 前者は再配送しても結果が変わらないが、 後者は **空きが出てから再配送しないと
/// 埋まらない**。 cursor を進めてよいかの判断が真逆なので型で分ける
/// (`SyncOutcome::skipped` と `dropped_unresolved` を分けたのと同じ理由)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteApply {
    /// 適用した。
    Applied,
    /// **LWW で古い** / 既に適用済み / tombstone に負けた / 宛先の himo が無い。
    /// 再配送しても結果は変わらない。
    Stale,
    /// **容量が足りず置けなかった** (`FaultKind::DiskSpace` / `ContentSpace`)。
    /// 値は一切書いていない。 **空きが出てからの再配送が必要**。
    RejectedCapacity,
}

impl RemoteApply {
    /// 適用されたか。
    pub fn applied(self) -> bool {
        matches!(self, RemoteApply::Applied)
    }

    /// 再配送が必要か (= cursor をこの record より先に進めてはいけない)。
    pub fn needs_redelivery(self) -> bool {
        matches!(self, RemoteApply::RejectedCapacity)
    }
}

#[derive(Debug, Clone)]
pub struct VocabOrphanStats {
    /// vocab に発行済みの全 vid 数 (= `Vocabulary::count()`)。
    pub vocab_total: u32,
    /// いずれかの himo cell から参照されている vid 数。
    pub live_vids: u32,
    /// `vocab_total - live_vids`。 死蔵の vid 数。
    pub orphan_vids: u32,
    /// orphan vid に対応する vocab data の総 byte 数 (= 救出可能領域)。
    pub orphan_bytes: u64,
    /// live vid に対応する vocab data の総 byte 数 (= 健全に使われてる領域)。
    pub live_bytes: u64,
}

impl VocabOrphanStats {
    /// `orphan_vids / vocab_total` の比率 (`vocab_total == 0` なら 0.0)。
    pub fn dead_ratio(&self) -> f64 {
        if self.vocab_total == 0 { 0.0 }
        else { self.orphan_vids as f64 / self.vocab_total as f64 }
    }
}

/// #88 (0.12.0): v5 (leaf region 無し = Leaf を vocab に格納) DB を v6
/// (LeafStore あり) へ移送した結果。 `Engine::migrate_bytes_v5_to_v6` 等が返す。
#[derive(Debug, Clone, Default)]
pub struct MigrationStats {
    /// 入力が既に v6 (leaf region あり) で移送不要だった。 他フィールドは 0。
    pub already_v6: bool,
    /// 移送対象になった Leaf himo 数。
    pub leaf_himos: u32,
    /// vocab → LeafStore に移した cell (= Leaf 値) の数。
    pub cells_moved: u64,
    /// 移した payload の総 byte 数 (slot header / padding は含まない生 bytes)。
    pub bytes_moved: u64,
    /// 移送後の LeafStore footprint (high_water、 byte)。
    pub leaf_footprint: u64,
    /// 移送後も vocab data に残る旧 Leaf bytes (= 死蔵)。 本 migration は
    /// vocab compaction をしないので、 この分は footprint に残る (既知の trade-off)。
    pub vocab_orphan_bytes_left: u64,
}

fn oplog_path_for(path: &str) -> std::path::PathBuf {
    crate::db_files::path_for(path, crate::db_files::OPLOG)
}

/// β-light step 7: table 定義 metadata の sidecar path (v10: `{db}/tables`)。
/// 中身は binary encoded `TableDef` 配列、 atomic 書き換え (`tables.tmp` →
/// rename) で更新。 不在なら open 時は anonymous fallback。
#[cfg(not(target_arch = "wasm32"))]
fn tables_path_for(path: &str) -> std::path::PathBuf {
    crate::db_files::path_for(path, crate::db_files::TABLES)
}

/// β-light step 7: tables Vec を binary encode。
///
/// layout:
///   magic: "TBL1" (4)
///   version: u32 = 1
///   table_count: u32
///   per table:
///     name_len: u32
///     name: u8[name_len]
///     eid_range_lo: u32
///     eid_range_hi: u32
///     next_local: u32
///     himo_count: u32
///     himo_ids: u32[himo_count]
///     fk_count: u32
///     fk_refs: [(u32 himo_id, u32 (target_table as u32))] × fk_count
///
/// #141 の PK は **全 table を書き切った後ろに optional block** として足す
/// (version は 1 のまま):
///   pk_magic: "PKS1" (4)
///   pk_count: u32
///   [(table_index: u32, pk_himo: u32)] × pk_count
///
/// version を上げずに末尾追記にしているのは **前方互換のため**。 table loop は
/// `table_count` 回で終わって残りを読まないので、 #141 以前のバイナリはこの block を
/// 単に無視して従来どおり開ける (PK 情報が落ちるだけ = PK-aware apply が効かなくなる
/// だけで、 DB が開けなくなることはない)。 version を 2 に上げると古いバイナリが
/// `unsupported version` で **開けなくなる** ため、 同じ DB を旧 enchudb で開く別
/// プロセスを巻き込む。
fn serialize_tables(tables: &[TableDef]) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + tables.len() * 128);
    out.extend_from_slice(b"TBL1");
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&(tables.len() as u32).to_le_bytes());
    for t in tables {
        let name_bytes = t.name.as_bytes();
        out.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(name_bytes);
        out.extend_from_slice(&t.eid_range_lo.to_le_bytes());
        out.extend_from_slice(&t.eid_range_hi.to_le_bytes());
        out.extend_from_slice(&t.next_local.load(std::sync::atomic::Ordering::Relaxed).to_le_bytes());
        let himo_ids = t.himo_ids.read().unwrap();
        out.extend_from_slice(&(himo_ids.len() as u32).to_le_bytes());
        for &h in himo_ids.iter() {
            out.extend_from_slice(&h.to_le_bytes());
        }
        drop(himo_ids);
        out.extend_from_slice(&(t.fk_refs.len() as u32).to_le_bytes());
        for &(hid, tid) in &t.fk_refs {
            out.extend_from_slice(&hid.to_le_bytes());
            out.extend_from_slice(&(tid as u32).to_le_bytes());
        }
    }
    // #141: PK block (optional trailer)。 PK を 1 つも持たないなら書かない
    // (= #141 以前と 1 byte も変わらない出力)。
    let pks: Vec<(u32, u32)> = tables
        .iter()
        .enumerate()
        .filter_map(|(i, t)| t.pk_himo.map(|h| (i as u32, h as u32)))
        .collect();
    if !pks.is_empty() {
        out.extend_from_slice(b"PKS1");
        out.extend_from_slice(&(pks.len() as u32).to_le_bytes());
        for (idx, hid) in pks {
            out.extend_from_slice(&idx.to_le_bytes());
            out.extend_from_slice(&hid.to_le_bytes());
        }
    }
    // v10 Phase 3 (request20 案 B): 追加 extent。 PKS1 と同じ末尾 optional block (version 据え置き)。
    //   ext_magic: "EXT1" (4)
    //   ext_count: u32                         (extent を持つ table の数)
    //   [(table_index: u32, n: u32, [(lo: u32, hi: u32)] × n)] × ext_count
    let ext: Vec<(u32, Vec<(u32, u32)>)> = tables
        .iter()
        .enumerate()
        .filter_map(|(i, t)| {
            let e = t.extra.read().unwrap();
            if e.is_empty() { None } else { Some((i as u32, e.clone())) }
        })
        .collect();
    if !ext.is_empty() {
        out.extend_from_slice(b"EXT1");
        out.extend_from_slice(&(ext.len() as u32).to_le_bytes());
        for (idx, extents) in ext {
            out.extend_from_slice(&idx.to_le_bytes());
            out.extend_from_slice(&(extents.len() as u32).to_le_bytes());
            for (lo, hi) in extents {
                out.extend_from_slice(&lo.to_le_bytes());
                out.extend_from_slice(&hi.to_le_bytes());
            }
        }
    }
    out
}

/// β-light step 7: tables sidecar を decode。 magic 不一致 / 短すぎる buffer
/// は Err。 部分破損は次の field 読みで失敗 → Err として扱う。
fn deserialize_tables(buf: &[u8]) -> Result<Vec<TableDef>, String> {
    if buf.len() < 12 || &buf[0..4] != b"TBL1" {
        return Err("tables sidecar: bad magic".into());
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if version != 1 {
        return Err(format!("tables sidecar: unsupported version {}", version));
    }
    let table_count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    // 破損ガード: 巨大 count をそのまま with_capacity に渡すと OOM abort。 各 table は
    // 最低 24 byte (name_len + lo/hi/next/himo_count/fk_count = 6×u32、 name 空) なので
    // 残りバッファで prealloc を cap する (truncation 自体は下の read_u32! が検出する)。
    let mut tables = Vec::with_capacity(table_count.min((buf.len() - 12) / 24));
    let mut off = 12usize;

    macro_rules! read_u32 {
        ($buf:expr, $off:expr) => {{
            if $off + 4 > $buf.len() {
                return Err("tables sidecar: truncated".into());
            }
            let v = u32::from_le_bytes($buf[$off..$off + 4].try_into().unwrap());
            $off += 4;
            v
        }};
    }

    for _ in 0..table_count {
        let name_len = read_u32!(buf, off) as usize;
        if off + name_len > buf.len() {
            return Err("tables sidecar: truncated (name)".into());
        }
        let name = String::from_utf8_lossy(&buf[off..off + name_len]).to_string();
        off += name_len;
        let eid_range_lo = read_u32!(buf, off);
        let eid_range_hi = read_u32!(buf, off);
        let next_local_u32 = read_u32!(buf, off);
        let next_local = std::sync::atomic::AtomicU32::new(next_local_u32);
        let himo_count = read_u32!(buf, off) as usize;
        let mut himo_ids = Vec::with_capacity(himo_count.min((buf.len() - off) / 4));
        for _ in 0..himo_count {
            himo_ids.push(read_u32!(buf, off));
        }
        let fk_count = read_u32!(buf, off) as usize;
        let mut fk_refs = Vec::with_capacity(fk_count.min((buf.len() - off) / 8));
        for _ in 0..fk_count {
            let hid = read_u32!(buf, off);
            let tid = read_u32!(buf, off);
            fk_refs.push((hid, tid as TableId));
        }
        tables.push(TableDef {
            name,
            himo_ids: std::sync::RwLock::new(himo_ids),
            eid_range_lo,
            eid_range_hi,
            fk_refs,
            next_local,
            free_locals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            free_locals_nonempty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            extra: std::sync::RwLock::new(Vec::new()),
            pk_himo: None,
        });
    }

    // #141: optional PK trailer。 無ければ (= #141 以前が書いた sidecar) PK 無しのまま。
    // 壊れた trailer は「PK 情報が無い」扱いにするだけで、 sidecar 全体は有効とする
    // (PK は再 build で復元できる派生情報であって、 table 定義の本体ではない)。
    // 末尾の optional block 群 (順不同、 未知の magic で打ち切り)
    while off + 8 <= buf.len() {
        let magic = &buf[off..off + 4];
        if magic == b"PKS1" {
            off += 4;
            let pk_count = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            for _ in 0..pk_count {
                if off + 8 > buf.len() {
                    break;
                }
                let idx = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let hid = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                off += 4;
                if let Some(t) = tables.get_mut(idx) {
                    if hid <= u16::MAX as u32 {
                        t.pk_himo = Some(hid as u16);
                    }
                }
            }
        } else if magic == b"EXT1" {
            off += 4;
            let ext_count = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            for _ in 0..ext_count {
                if off + 8 > buf.len() {
                    return Err("tables sidecar: truncated (EXT1)".into());
                }
                let idx = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let n = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                if off + n.saturating_mul(8) > buf.len() {
                    return Err("tables sidecar: truncated (EXT1 extents)".into());
                }
                let mut extents = Vec::with_capacity(n);
                for _ in 0..n {
                    let lo = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                    off += 4;
                    let hi = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
                    off += 4;
                    if lo >= hi {
                        return Err(format!("tables sidecar: bad extent [{lo}, {hi})"));
                    }
                    extents.push((lo, hi));
                }
                if let Some(t) = tables.get_mut(idx) {
                    *t.extra.write().unwrap() = extents;
                }
            }
        } else {
            break;
        }
    }
    Ok(tables)
}

/// sidecar を atomic に置き換える (tmp write → fsync → rename)。
///
/// `.tables` / `.eidmap` / `.vocabmap` が同じ手順を踏むので 1 箇所に寄せてある。
/// tmp 名は `{sidecar}.tmp` (= sidecar ごとに別名) なので、 同時 persist しても
/// 互いの tmp を踏まない。
///
/// rename は **新しい inode** を置くので、 呼び出し側が chmod した mode は放っておくと
/// umask 由来 (典型的には 0644) に戻る。 consumer が DB を締めている前提を壊さないよう、
/// 置き換え前の mode を tmp に写してから rename する (無ければ umask のまま)。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn atomic_write_sidecar(sidecar: &std::path::Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let tmp_path = crate::db_files::tmp_path_for(sidecar);
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    inherit_mode(sidecar, &tmp_path);
    std::fs::rename(&tmp_path, sidecar)?;
    Ok(())
}

/// `from` が既にあればその mode を `to` に写す。 mode が取れない / 設定できない環境
/// (Windows、 権限不足) では黙って諦める — 内容の永続化を mode の都合で失敗させない。
#[cfg(not(target_arch = "wasm32"))]
fn inherit_mode(from: &std::path::Path, to: &std::path::Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(md) = std::fs::metadata(from) {
            let mode = md.permissions().mode() & 0o777;
            let _ = std::fs::set_permissions(to, std::fs::Permissions::from_mode(mode));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (from, to);
    }
}

/// β-light step 7: tables を sidecar に atomic 書き換え。 fsync まで含む。
#[cfg(not(target_arch = "wasm32"))]
fn persist_tables_to_sidecar(db_path: &str, tables: &[TableDef]) -> io::Result<()> {
    atomic_write_sidecar(&tables_path_for(db_path), &serialize_tables(tables))
}

/// 0.8.15 (issue #52): persist 失敗で残った `.tables.tmp` を open 時に明示削除。
/// 通常 persist は `truncate(true)` で上書きするが、 disk full → recovery 後の
/// 状態を確実に clean にするためだけの safety net。 削除失敗は warning だけで続行。
#[cfg(not(target_arch = "wasm32"))]
fn cleanup_tables_tmp(db_path: &str) {
    let sidecar = tables_path_for(db_path);
    let tmp_path = crate::db_files::tmp_path_for(&sidecar);
    if tmp_path.exists() {
        if let Err(e) = std::fs::remove_file(&tmp_path) {
            eprintln!(
                "warning: failed to remove stale tables tmp {}: {}",
                tmp_path.display(),
                e
            );
        }
    }
}

/// 0.8.15 (issue #52): 破損 sidecar を `.tables.corrupt-<unix_ts>` に rename して
/// 退避し、 anonymous fallback で open 続行する。 user 側は schema crate の
/// synthesize 経路で engine 内 table 定義から復元できる。
#[cfg(not(target_arch = "wasm32"))]
fn rename_corrupt_sidecar(db_path: &str, kind: &str, err: &io::Error) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let sidecar = match kind {
        "tables" => tables_path_for(db_path),
        // 将来 schema 用に同 helper を使う場合のため switch (今は tables のみ)
        _ => return,
    };
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let backup = crate::db_files::corrupt_backup_path_for(&sidecar, ts);
    eprintln!(
        "warning: {} sidecar parse failed ({}): renaming to {} (anonymous fallback)",
        kind,
        err,
        backup.display()
    );
    if let Err(e) = std::fs::rename(&sidecar, &backup) {
        eprintln!(
            "warning: failed to rename corrupt sidecar {} -> {}: {}",
            sidecar.display(),
            backup.display(),
            e
        );
    }
}

/// β-light step 7: 既存 sidecar を読む。 不在 (v4 DB) なら Ok(None)。
#[cfg(not(target_arch = "wasm32"))]
fn load_tables_from_sidecar(db_path: &str) -> io::Result<Option<Vec<TableDef>>> {
    let sidecar = tables_path_for(db_path);
    match std::fs::read(&sidecar) {
        Ok(buf) => match deserialize_tables(&buf) {
            Ok(tables) => Ok(Some(tables)),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}


/// #9: eid 翻訳テーブルの sidecar path。 `.eidmap`。 中身は
/// `(author_peer, foreign_local, local)` の binary 配列。 不在 (= sync してない DB
/// や旧 DB) なら open 時に空の translator で続行 (additive、 後方互換)。
#[cfg(not(target_arch = "wasm32"))]
fn eidmap_path_for(path: &str) -> std::path::PathBuf {
    crate::db_files::path_for(path, crate::db_files::EIDMAP)
}

/// #9: eidmap sidecar の 1 entry。 `(author_peer, foreign_local, local, tombstone_hlc)`。
/// `tombstone_hlc == Hlc::ZERO` は「削除されていない」。 v2 (0.8.19) で tombstone を追加し、
/// reopen 後の削除済み entity 復活 (resurrection) を防ぐ。
type EidmapEntry = (enchudb_oplog::PeerId, u32, u32, enchudb_oplog::Hlc);

/// #9: 翻訳 entry を binary encode (v2)。
/// layout: magic "EIDM"(4) + version u32=2 + count u32
///         + (peer u32, foreign u32, local u32, tomb_wall u64, tomb_logical u32, tomb_peer u32) × count
/// #166: `.eidmap` の現行 format 版数。
///
/// - v1: tombstone 無し (12 byte/entry)
/// - v2: tombstone 込み (28 byte/entry)
/// - v3: v2 と **同じ 28 byte/entry**。 `local == NO_LOCAL_SLOT` の entry が
///   「slot を持たない削除記録」 を意味するようになった (`foreign_tombs` の永続化)
///
/// 読み手は 3 版すべてを読める。 v3 を v2 の reader に食わせると
/// `NO_LOCAL_SLOT` を実在 slot として扱ってしまうので版数を上げてある
/// (v9 DB は FILE_VERSION 側で旧 binary を弾くため実害は無いが、 format の
/// 自己記述性として正しい形にしておく)。
const EIDMAP_VERSION: u32 = 3;

/// #166: 「この entry は写像ではなく削除記録だけ」 を表す番兵。
/// `max_entities` は `u32::MAX` 未満なので実在 slot と衝突しない。
const NO_LOCAL_SLOT: u32 = u32::MAX;

fn serialize_eidmap(entries: &[EidmapEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + entries.len() * 28);
    out.extend_from_slice(b"EIDM");
    out.extend_from_slice(&EIDMAP_VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for &(peer, foreign_local, local, tomb) in entries {
        out.extend_from_slice(&peer.to_le_bytes());
        out.extend_from_slice(&foreign_local.to_le_bytes());
        out.extend_from_slice(&local.to_le_bytes());
        out.extend_from_slice(&tomb.wall.to_le_bytes());
        out.extend_from_slice(&tomb.logical.to_le_bytes());
        out.extend_from_slice(&tomb.peer.to_le_bytes());
    }
    out
}

/// #9: eidmap sidecar を decode。 magic 不一致 / truncated は Err。 v1 (tombstone 無し、
/// 12 byte/entry) と v2 (tombstone 込み、 28 byte/entry) の両方を読める。
fn deserialize_eidmap(buf: &[u8]) -> Result<Vec<EidmapEntry>, String> {
    if buf.len() < 12 || &buf[0..4] != b"EIDM" {
        return Err("eidmap sidecar: bad magic".into());
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let entry_size = match version {
        1 => 12usize,
        // v2 と v3 は同じ長さ。 違いは `local == NO_LOCAL_SLOT` の解釈だけで、
        // その判定は読み手 (`load` 側) が行う。
        2 | 3 => 28usize,
        v => return Err(format!("eidmap sidecar: unsupported version {}", v)),
    };
    let count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    // 破損 / torn header ガード: `count` は信用しない。 各 entry は固定長なので上限は
    // 残りバッファで決まる。 bogus な巨大 count をそのまま with_capacity に渡すと数 GB の
    // 確保要求で open 時に abort する (.eidmap は CRC 無し)。 上限超過は破損とみなし Err
    // → load_eidmap_from_sidecar 側で空 translator に graceful fallback。
    let max_entries = (buf.len() - 12) / entry_size;
    if count > max_entries {
        return Err(format!(
            "eidmap sidecar: count {} exceeds buffer capacity {} (corrupt/torn)",
            count, max_entries
        ));
    }
    let mut entries = Vec::with_capacity(count);
    let mut off = 12usize;
    for _ in 0..count {
        if off + entry_size > buf.len() {
            return Err("eidmap sidecar: truncated".into());
        }
        let peer = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
        let foreign_local = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap());
        let local = u32::from_le_bytes(buf[off + 8..off + 12].try_into().unwrap());
        let tomb = if entry_size == 28 {
            let wall = u64::from_le_bytes(buf[off + 12..off + 20].try_into().unwrap());
            let logical = u32::from_le_bytes(buf[off + 20..off + 24].try_into().unwrap());
            let tpeer = u32::from_le_bytes(buf[off + 24..off + 28].try_into().unwrap());
            enchudb_oplog::Hlc { wall, logical, peer: tpeer }
        } else {
            enchudb_oplog::Hlc::ZERO
        };
        off += entry_size;
        entries.push((peer, foreign_local, local, tomb));
    }
    Ok(entries)
}

/// #9: eidmap を sidecar に atomic 書き換え (fsync 込み)。 entries 空なら何もしない
/// (= sync してない DB に空ファイルを作らない)。
#[cfg(not(target_arch = "wasm32"))]
fn persist_eidmap_to_sidecar(db_path: &str, entries: &[EidmapEntry]) -> io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    atomic_write_sidecar(&eidmap_path_for(db_path), &serialize_eidmap(entries))
}

/// #9: eidmap sidecar を読む。 不在なら Ok(None)。
#[cfg(not(target_arch = "wasm32"))]
fn load_eidmap_from_sidecar(db_path: &str) -> io::Result<Option<Vec<EidmapEntry>>> {
    let sidecar = eidmap_path_for(db_path);
    match std::fs::read(&sidecar) {
        Ok(buf) => match deserialize_eidmap(&buf) {
            Ok(entries) => Ok(Some(entries)),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// **peer vocab 写像の sidecar path**。 `.vocabmap`。
///
/// text (Tag/Leaf) の cell 値は `(author_peer, remote_vid) → local_vid` の写像を
/// 通してしか意味を持たない。 この写像は受信した `Vocab` op から組み立てるが、
/// **受信 op は自分の WAL には残らない** (gossip_remote_apply が off なら
/// append_relayed も走らない) ため、 memory から消えると復元手段が無い。
///
/// `(peer, remote_eid) → local` を持つ `.eidmap` と同格の永続先がここ。 両方
/// 揃って初めて「pull cursor が消費した state」が disk 上で再構成できる。
#[cfg(not(target_arch = "wasm32"))]
fn vocabmap_path_for(path: &str) -> std::path::PathBuf {
    crate::db_files::path_for(path, crate::db_files::VOCABMAP)
}

/// vocabmap sidecar の 1 entry。 `(author_peer, remote_vid, local_vid)`。
type VocabmapEntry = (enchudb_oplog::PeerId, u32, u32);

/// `.vocabmap` の現行 format 版数。
///
/// - v1: magic "EVCM"(4) + version u32 + count u32 + (peer u32, remote u32, local u32) × count
const VOCABMAP_VERSION: u32 = 1;

/// vocabmap entry を binary encode (v1)。
fn serialize_vocabmap(entries: &[VocabmapEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + entries.len() * 12);
    out.extend_from_slice(b"EVCM");
    out.extend_from_slice(&VOCABMAP_VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for &(peer, remote_vid, local_vid) in entries {
        out.extend_from_slice(&peer.to_le_bytes());
        out.extend_from_slice(&remote_vid.to_le_bytes());
        out.extend_from_slice(&local_vid.to_le_bytes());
    }
    out
}

/// vocabmap sidecar を decode。 magic 不一致 / truncated は Err。
///
/// `.eidmap` と同じく CRC を持たないので、 header の `count` は信用せず
/// 残りバッファから上限を出す (bogus な巨大 count での確保 abort を防ぐ)。
fn deserialize_vocabmap(buf: &[u8]) -> Result<Vec<VocabmapEntry>, String> {
    if buf.len() < 12 || &buf[0..4] != b"EVCM" {
        return Err("vocabmap sidecar: bad magic".into());
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if version != VOCABMAP_VERSION {
        return Err(format!("vocabmap sidecar: unsupported version {}", version));
    }
    const ENTRY_SIZE: usize = 12;
    let count = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let max_entries = (buf.len() - 12) / ENTRY_SIZE;
    if count > max_entries {
        return Err(format!(
            "vocabmap sidecar: count {} exceeds buffer capacity {} (corrupt/torn)",
            count, max_entries
        ));
    }
    let mut entries = Vec::with_capacity(count);
    let mut off = 12usize;
    for _ in 0..count {
        if off + ENTRY_SIZE > buf.len() {
            return Err("vocabmap sidecar: truncated".into());
        }
        let peer = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
        let remote_vid = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap());
        let local_vid = u32::from_le_bytes(buf[off + 8..off + 12].try_into().unwrap());
        off += ENTRY_SIZE;
        entries.push((peer, remote_vid, local_vid));
    }
    Ok(entries)
}

/// vocabmap を sidecar に atomic 書き換え (fsync 込み)。 entries 空なら何もしない
/// (= sync してない DB に空ファイルを作らない)。
#[cfg(not(target_arch = "wasm32"))]
fn persist_vocabmap_to_sidecar(db_path: &str, entries: &[VocabmapEntry]) -> io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    atomic_write_sidecar(&vocabmap_path_for(db_path), &serialize_vocabmap(entries))
}

/// vocabmap sidecar を読む。 不在なら Ok(None)。
#[cfg(not(target_arch = "wasm32"))]
fn load_vocabmap_from_sidecar(db_path: &str) -> io::Result<Option<Vec<VocabmapEntry>>> {
    match std::fs::read(vocabmap_path_for(db_path)) {
        Ok(buf) => match deserialize_vocabmap(&buf) {
            Ok(entries) => Ok(Some(entries)),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// writer 排他用 sidecar の path (v10: `{db}/lock`)。
#[cfg(not(target_arch = "wasm32"))]
fn writer_lock_path_for(path: &str) -> std::path::PathBuf {
    crate::db_files::path_for(path, crate::db_files::LOCK)
}

/// `oplog_record_queue` (= bounded ArrayQueue) への blocking push。
/// queue 満杯時は `yield_now` で consumer の進捗を待つ (issue4 backpressure)。
/// #77-M2: consumer 死亡 (poisoned) 時は無限待ちせず panic で失敗する。
/// push 成功時に `wal_push_count` を進める (flush_writes の WAL barrier 用)。
#[inline]
fn push_oplog_record_blocking(
    wq: &crossbeam_queue::ArrayQueue<(enchudb_oplog::oplog::OwnedOp, enchudb_oplog::Hlc)>,
    rec: enchudb_oplog::oplog::OwnedOp,
    // request17-A3: push 側が採番した版数。 cell に書いたものと同一。
    hlc: enchudb_oplog::Hlc,
    poisoned: &std::sync::atomic::AtomicBool,
    wal_push_count: &std::sync::atomic::AtomicU64,
) {
    let mut rec = (rec, hlc);
    loop {
        match wq.push(rec) {
            Ok(()) => {
                wal_push_count.fetch_add(1, std::sync::atomic::Ordering::Release);
                return;
            }
            Err(returned) => {
                if poisoned.load(std::sync::atomic::Ordering::Acquire) {
                    panic!("enchudb consumer thread has panicked — WAL record queue is dead (#77-M2)");
                }
                rec = returned;
                std::thread::yield_now();
            }
        }
    }
}

/// 同一プロセス内で writer lock を保持中の lock path の registry (#80)。
/// flock は open file description 単位なので、同一プロセスからの二重 open は
/// block 検知できず無期限ハングになる。flock に入る前にここで fast-fail する。
#[cfg(not(target_arch = "wasm32"))]
static WRITER_LOCK_REGISTRY: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

#[cfg(not(target_arch = "wasm32"))]
fn writer_registry() -> std::sync::MutexGuard<'static, std::collections::HashSet<std::path::PathBuf>>
{
    WRITER_LOCK_REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// writer lock の保持を表す guard。 drop で registry から抜け、 fd close で
/// flock も解放される。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct WriterLock {
    _file: std::fs::File,
    key: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for WriterLock {
    fn drop(&mut self) {
        writer_registry().remove(&self.key);
        // `_file` はこの後 drop され、 close で flock が解放される。
        // registry 解除 → close の順なので、 競合した同一プロセスの open は
        // fast-fail ではなく一瞬 flock で待ってから取得する (spurious error なし)。
    }
}

/// `.db.lock` に flock(LOCK_EX) を取り、 guard を返す。 guard が drop されると
/// lock も解放。 **別プロセス**が保持中は block する (= sqlite と同様、 取れる
/// まで待つ)。 **同一プロセス**が既に保持中は block せず即エラー (#80、
/// `ErrorKind::WouldBlock`)。 readonly open は呼ばない。 writer 系の
/// open / create だけ呼ぶ。
#[cfg(not(target_arch = "wasm32"))]
fn acquire_writer_lock(path: &str) -> io::Result<WriterLock> {
    // v10: lock は DB directory の中。 directory は create 側が作り、 open は存在を
    // `check_db_dir` で確認済み (無い path に空 directory を残さない)。
    let lock_path = writer_lock_path_for(path);
    let f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    // 直前に create 済みなので canonicalize は通常成功する (symlink / 相対 path
    // の表記揺れで registry をすり抜けないための正規化)。
    let key = lock_path.canonicalize().unwrap_or(lock_path);
    if !writer_registry().insert(key.clone()) {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "\"{path}\" is already open for writing in this process \
                 (drop the existing Engine handle first, or use open_readonly)"
            ),
        ));
    }
    // std::fs::File::lock (Rust 1.89 安定化) は unix で flock、 Windows で
    // LockFileEx に落ちる。 素の libc::flock は Windows に fd 自体が無く使えない。
    if let Err(err) = f.lock() {
        writer_registry().remove(&key);
        return Err(err);
    }
    Ok(WriterLock { _file: f, key })
}

/// 0.9.0 (H11): create 系 API の既存ファイルガード。
/// 旧実装は `create(true).truncate(true)` で **既存 DB を無警告で 0 バイトに破壊**
/// していた (typo った path を create しただけで全損)。 FFI 契約
/// (`enchudb_create`: 「既存ファイルがあると Engine 側でエラー」) 通り、
/// 存在する path への create は `AlreadyExists` で拒否する。
/// 既存 DB を開くなら `Engine::open*`、 作り直すなら caller が明示的に削除すること。
#[cfg(not(target_arch = "wasm32"))]
/// v10: DB directory を作る。 `mkdir` の atomic 性で「既存を silent に潰さない」 (H11)
/// と「同時 create の片方だけ勝つ」を兼ねる。 既存が file でも directory でも
/// `AlreadyExists`。
#[cfg(not(target_arch = "wasm32"))]
fn create_db_dir(path: &str) -> io::Result<()> {
    std::fs::create_dir(path).map_err(|e| {
        if e.kind() == io::ErrorKind::AlreadyExists {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "database already exists: \"{path}\" — refusing to overwrite. \
                     use Engine::open* to open the existing DB, or remove it first"
                ),
            )
        } else {
            e
        }
    })
}

/// v10: `[start, end)` の中で data が載っている最後の byte 位置 (SEEK_DATA / SEEK_HOLE)。
/// 無ければ `start`。 sparse な packed file から region を切り出すとき、 穴を読まないため。
#[cfg(all(not(target_arch = "wasm32"), unix))]
fn last_data_end(f: &std::fs::File, start: u64, end: u64) -> u64 {
    use std::os::unix::io::AsRawFd;
    let fd = f.as_raw_fd();
    let mut pos = start;
    let mut last = start;
    while pos < end {
        let d = unsafe { libc::lseek(fd, pos as libc::off_t, libc::SEEK_DATA) };
        if d < 0 || d as u64 >= end {
            break;
        }
        let h = unsafe { libc::lseek(fd, d, libc::SEEK_HOLE) };
        let h = if h < 0 { end } else { (h as u64).min(end) };
        last = h;
        pos = h;
    }
    last
}

#[cfg(all(not(target_arch = "wasm32"), not(unix)))]
fn last_data_end(f: &std::fs::File, start: u64, end: u64) -> u64 {
    // SEEK_DATA が無い環境: 末尾から 64 KB ずつ非 0 を探す (全 0 なら start)。
    use std::io::{Read, Seek, SeekFrom};
    let mut f = f.try_clone().expect("clone");
    let mut buf = vec![0u8; 64 * 1024];
    let mut hi = end;
    while hi > start {
        let lo = hi.saturating_sub(buf.len() as u64).max(start);
        let n = (hi - lo) as usize;
        if f.seek(SeekFrom::Start(lo)).is_err() || f.read_exact(&mut buf[..n]).is_err() {
            return end;
        }
        if buf[..n].iter().any(|b| *b != 0) {
            return hi;
        }
        hi = lo;
    }
    start
}

/// `[src_off, src_off+len)` を `dst_off` へ写す。 全 0 の block (4 KB) は書かずに seek で
/// 飛ばし、 飛ばした範囲 (dst offset, len、 隣接は併合) を返す。 呼び出し側は **全 write と
/// `set_len` の後**に `punch_holes` を呼ぶこと: Linux (ext4 / xfs) は seek で飛ばした範囲が
/// そのまま穴になるが、 **APFS は write のたびに 16 MB 未満の gap を実体化する** ので、
/// 後から `F_PUNCHHOLE` で抜くしかない (実 DB の migrate で 130 MB → 344 MB になった)。
/// 粒度が 4 KB なのは hash index (vocab / himoreg / content) のように data が薄く散る
/// region のため (1 MB 粒度だとほぼ全 chunk に非 0 が混ざる)。
#[cfg(not(target_arch = "wasm32"))]
fn copy_file_range(
    src: &mut std::fs::File, src_off: u64, dst: &mut std::fs::File, dst_off: u64, len: u64,
) -> io::Result<Vec<(u64, u64)>> {
    use std::io::{Read, Seek, SeekFrom, Write};
    src.seek(SeekFrom::Start(src_off))?;
    dst.seek(SeekFrom::Start(dst_off))?;
    const BLOCK: usize = 4096;
    let mut zero_runs: Vec<(u64, u64)> = Vec::new();
    let mut push_zero = |off: u64, n: u64| match zero_runs.last_mut() {
        Some((o, l)) if *o + *l == off => *l += n,
        _ => zero_runs.push((off, n)),
    };
    let mut remaining = len;
    let mut pos = dst_off;
    let mut buf = vec![0u8; 1024 * 1024];
    while remaining > 0 {
        let n = remaining.min(buf.len() as u64) as usize;
        src.read_exact(&mut buf[..n])?;
        let mut i = 0;
        while i < n {
            let blk = BLOCK.min(n - i);
            // 非 0 block の連続 run をまとめて 1 write
            let mut j = i;
            while j < n && buf[j..j + BLOCK.min(n - j)].iter().any(|b| *b != 0) {
                j += BLOCK.min(n - j);
            }
            if j > i {
                dst.write_all(&buf[i..j])?;
                pos += (j - i) as u64;
                i = j;
                continue;
            }
            dst.seek(SeekFrom::Current(blk as i64))?;
            push_zero(pos, blk as u64);
            pos += blk as u64;
            i += blk;
        }
        remaining -= n as u64;
    }
    Ok(zero_runs)
}

/// `copy_file_range` が飛ばした全 0 範囲を穴に戻す (macOS / APFS の `F_PUNCHHOLE`)。
/// 対応しない FS (HFS+ 等) では実体化したままにする = 失敗ではない。 他 OS では seek で
/// 飛ばした範囲が既に穴なので no-op。 4 KB 未満の端数は punch できないので残す。
#[cfg(target_os = "macos")]
fn punch_holes(f: &std::fs::File, runs: &[(u64, u64)]) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    #[repr(C)]
    struct FPunchHole {
        fp_flags: u32,
        reserved: u32,
        fp_offset: libc::off_t,
        fp_length: libc::off_t,
    }
    const F_PUNCHHOLE: libc::c_int = 99;
    for &(off, len) in runs {
        let len = len & !4095;
        if len == 0 || off % 4096 != 0 {
            continue;
        }
        let arg = FPunchHole { fp_flags: 0, reserved: 0, fp_offset: off as libc::off_t, fp_length: len as libc::off_t };
        if unsafe { libc::fcntl(f.as_raw_fd(), F_PUNCHHOLE, &arg as *const FPunchHole) } < 0 {
            let e = io::Error::last_os_error();
            return match e.raw_os_error() {
                Some(libc::ENOTSUP) | Some(libc::EINVAL) | Some(libc::ENOTTY) => Ok(()),
                _ => Err(e),
            };
        }
    }
    Ok(())
}

#[cfg(all(not(target_arch = "wasm32"), not(target_os = "macos")))]
fn punch_holes(_f: &std::fs::File, _runs: &[(u64, u64)]) -> io::Result<()> {
    Ok(())
}

/// `buf` を offset 0 から sparse に書く (全 0 の 4 KB block は穴)。 `copy_file_range` の
/// in-memory 版。
#[cfg(not(target_arch = "wasm32"))]
fn write_sparse(out: &mut std::fs::File, buf: &[u8]) -> io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    const BLOCK: usize = 4096;
    let mut holes: Vec<(u64, u64)> = Vec::new();
    let mut i = 0;
    while i < buf.len() {
        let end = (i + BLOCK).min(buf.len());
        if buf[i..end].iter().any(|b| *b != 0) {
            out.seek(SeekFrom::Start(i as u64))?;
            out.write_all(&buf[i..end])?;
        } else {
            match holes.last_mut() {
                Some((o, l)) if *o + *l == i as u64 => *l += (end - i) as u64,
                _ => holes.push((i as u64, (end - i) as u64)),
            }
        }
        i = end;
    }
    out.set_len(buf.len() as u64)?;
    punch_holes(out, &holes)
}

#[cfg(not(target_arch = "wasm32"))]
impl Engine {
    /// v10: header (Layout / himo_count / cell_version) から、 directory に存在しうる segment の
    /// 一覧を返す。 pack / unpack / migration が共有する。
    fn segment_kinds_for(layout: &Layout, himo_count: u32) -> Vec<SegmentKind> {
        let mut kinds = vec![SegmentKind::Header];
        for k in SegmentKind::FIXED {
            if k == SegmentKind::LeafData && layout.leaf_data_size == 0 {
                continue;
            }
            if layout.segment_size(k) > 0 {
                kinds.push(k);
            }
        }
        for hid in 0..himo_count {
            kinds.push(SegmentKind::Himo(hid));
            if layout.has_cell_version() {
                kinds.push(SegmentKind::Ver(hid));
            }
        }
        if layout.has_cell_version() {
            kinds.push(SegmentKind::Tomb);
        }
        kinds
    }

    /// v10: DB directory を **packed 1 ファイル** (= 旧 v9 の 1 ファイル layout と byte 互換、
    /// sparse) に書き出す。 relay の bootstrap 配布 / 転送 / wasm (`from_bytes`) 用。
    /// 戻り値は packed の総サイズ (= `layout.total_size`、 見かけ)。
    pub fn pack_dir(dir: &str, packed: &std::path::Path) -> io::Result<u64> {
        let (layout, himo_count) = Self::read_header_layout(dir)?;
        let mut out = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(packed)?;
        out.set_len(layout.total_size as u64)?;
        // seek で飛ばした全 0 range + region の未使用尾部。 最後にまとめて穴に戻す (APFS)。
        let mut holes: Vec<(u64, u64)> = Vec::new();
        for kind in Self::segment_kinds_for(&layout, himo_count) {
            let p = std::path::Path::new(dir).join(kind.rel_path());
            let mut f = match std::fs::File::open(&p) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let len = f.metadata()?.len().min(layout.segment_size(kind) as u64);
            let off = layout.region_off(kind) as u64;
            let size = layout.segment_size(kind) as u64;
            holes.extend(copy_file_range(&mut f, 0, &mut out, off, len)?);
            if size > len {
                holes.push((off + len, size - len));
            }
        }
        punch_holes(&out, &holes)?;
        out.sync_all()?;
        Ok(layout.total_size as u64)
    }

    /// v10: packed 1 ファイル (v8 / v9 の単一 file DB も可) を directory `dst` に展開する。
    /// 各 region は data の載っている範囲だけ写す (sparse の穴は読まない)。 v8 / v9 の header
    /// は version を 10 に打ち直す。 `dst` は存在してはいけない。
    pub fn unpack_to_dir(packed: &std::path::Path, dst: &str) -> io::Result<()> {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut src = std::fs::File::open(packed)?;
        let mut fixed = vec![0u8; HEADER_SIZE];
        src.read_exact(&mut fixed).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("packed header too small: {e}"))
        })?;
        let (layout, himo_count) = Self::parse_header(&fixed, true)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let total = src.metadata()?.len();
        if total < layout.total_size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("packed file truncated: {} bytes (layout.total_size = {})", total, layout.total_size),
            ));
        }
        // legacy (v8 / v9): reservation を既定まで広げ、 EntitySet を新 layout に組み直す
        // (free stack の位置が bitset 容量で決まるので、 中身を動かす必要がある)
        let src_version = u32::from_le_bytes(fixed[H_VERSION..H_VERSION + 4].try_into().unwrap());
        let legacy = src_version != FILE_VERSION;
        let new_reserve = if legacy {
            default_reserve_entities(layout.max_entities)
        } else {
            layout.reserve_entities
        };
        let dstp = std::path::Path::new(dst);
        std::fs::create_dir(dstp)?;
        std::fs::create_dir(dstp.join("himo"))?;
        std::fs::create_dir(dstp.join("ver"))?;
        for kind in Self::segment_kinds_for(&layout, himo_count) {
            let off = layout.region_off(kind) as u64;
            let size = layout.segment_size(kind) as u64;
            let end = off + size;
            let mut out = OpenOptions::new().write(true).create_new(true).open(dstp.join(kind.rel_path()))?;
            if kind == SegmentKind::Header {
                // legacy (固定 4096) → v10 (可変長) は zero 拡張。 himo 表は先頭からの
                // 固定 offset なので後ろを伸ばすだけでよい (CRC は先頭 64 byte のみ)。
                let max_himos = u32::from_le_bytes(fixed[H_MAX_HIMOS..H_MAX_HIMOS + 4].try_into().unwrap());
                let v10_size = header_size_for(max_himos);
                let mut hdr = vec![0u8; v10_size.max(layout.header_size)];
                src.seek(SeekFrom::Start(0))?;
                src.read_exact(&mut hdr[..layout.header_size])?;
                if legacy {
                    hdr[H_VERSION..H_VERSION + 4].copy_from_slice(&FILE_VERSION.to_le_bytes());
                    hdr[H_RESERVE_ENTITIES..H_RESERVE_ENTITIES + 4].copy_from_slice(&new_reserve.to_le_bytes());
                    write_header_crc(&mut hdr);
                }
                out.write_all(&hdr)?;
            } else if legacy && kind == SegmentKind::Entities {
                let mut old = vec![0u8; size as usize];
                src.seek(SeekFrom::Start(off))?;
                src.read_exact(&mut old)?;
                let new = EntitySet::relayout(&old, layout.max_entities, new_reserve);
                write_sparse(&mut out, &new)?;
            } else {
                let used = last_data_end(&src, off, end);
                // page 未満でも SegmentMap::open が page に揃える。 ここでは data の末尾まで。
                let len = used.saturating_sub(off).max(4096.min(size));
                let holes = copy_file_range(&mut src, off, &mut out, 0, len)?;
                out.set_len(len)?;
                punch_holes(&out, &holes)?;
            }
            out.sync_all()?;
        }
        // segment を直接書いたので manifest も実長から作る (以後の open が切り詰めを検出できる)。
        crate::segments::write_manifest_from_dir(std::path::Path::new(dst))?;
        Ok(())
    }

    /// v8 / v9 の **1 ファイル DB** を v10 の directory に移行する (offline)。 本体は
    /// `unpack_to_dir`、 sidecar (`{src}.tables` / `.eidmap` / `.vocabmap` / `.oplog` /
    /// `.schema`) は directory の中 (`{dst}/tables` …) に copy。 `.crc` は region 境界が
    /// 変わらないので写すが、 疑わしければ `seal_integrity` で打ち直すこと。 元は触らない。
    pub fn migrate_v9_to_v10(src_file: &str, dst_dir: &str) -> io::Result<()> {
        let sp = std::path::Path::new(src_file);
        if !sp.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("\"{src_file}\" is not a single-file (v9 or older) database"),
            ));
        }
        Self::unpack_to_dir(sp, dst_dir)?;
        for name in crate::db_files::ALL {
            if name == crate::db_files::LOCK || name == crate::db_files::SEGMENTS {
                continue;
            }
            let from = crate::db_files::legacy_path_for(src_file, name);
            if from.exists() {
                crate::sparse_copy::copy_sparse(&from, &crate::db_files::path_for(dst_dir, name))?;
            }
        }
        // v9 の隣には無いので、 書き終えた segment の実長から作る。
        crate::segments::write_manifest_from_dir(std::path::Path::new(dst_dir))?;
        Ok(())
    }
}

/// v10: DB directory を複製する (本体 segment + sidecar。 `lock` と `*.tmp` は除く)。
/// 複製先は普通に `Engine::open*` できる。 segment file は `copy_sparse` で (unix は
/// dense なので素の copy と同じ、 Windows の sparse segment は穴を保つ)。
///
/// **整合性は呼び出し側の責務**: writer が動いている DB の sidecar は本体より遅れて
/// いることがある (consumer tick)。 整合した複製が要るなら `Engine::snapshot_export`。
#[cfg(not(target_arch = "wasm32"))]
pub fn copy_db_dir(src: &std::path::Path, dst: &std::path::Path) -> io::Result<()> {
    copy_db_dir_filtered(src, dst, &crate::db_files::is_copyable_entry)
}

/// v10: DB 本体 (segment file と `himo/` / `ver/`) だけを写す。 sidecar は写さない。
#[cfg(not(target_arch = "wasm32"))]
pub fn copy_db_segments(src: &std::path::Path, dst: &std::path::Path) -> io::Result<()> {
    copy_db_dir_filtered(src, dst, &crate::db_files::is_segment_entry)
}

/// top level の entry を `keep` で選び、 sub directory (`himo/` / `ver/`) は丸ごと再帰。
#[cfg(not(target_arch = "wasm32"))]
fn copy_db_dir_filtered(
    src: &std::path::Path,
    dst: &std::path::Path,
    keep: &dyn Fn(&std::ffi::OsStr) -> bool,
) -> io::Result<()> {
    use crate::sparse_copy::copy_sparse;
    // macOS / APFS: directory ごと 1 syscall で clone できる (file 単位の clonefile は
    // 1 本 ~100 µs で、 himo 200 本の DB を snapshot すると 20 ms 超)。 clone 後に
    // `keep` から漏れる top level entry (lock / sidecar 等) を消す。 dst が既にある /
    // 別 volume / 非 APFS なら file 単位に落ちる。
    #[cfg(target_os = "macos")]
    if clone_dir_apfs(src, dst).is_ok() {
        for entry in std::fs::read_dir(dst)? {
            let entry = entry?;
            if !keep(&entry.file_name()) {
                let p = entry.path();
                if entry.file_type()?.is_dir() {
                    std::fs::remove_dir_all(&p)?;
                } else {
                    std::fs::remove_file(&p)?;
                }
            }
        }
        return Ok(());
    }
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        if !keep(&entry.file_name()) {
            continue;
        }
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_db_dir_filtered(&from, &to, &|_| true)?;
        } else {
            copy_sparse(&from, &to)?;
        }
    }
    Ok(())
}

/// APFS の `clonefile(2)` で directory を丸ごと clone する (dst は存在しないこと)。
#[cfg(target_os = "macos")]
fn clone_dir_apfs(src: &std::path::Path, dst: &std::path::Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let s = std::ffi::CString::new(src.as_os_str().as_bytes())?;
    let d = std::ffi::CString::new(dst.as_os_str().as_bytes())?;
    if unsafe { libc::clonefile(s.as_ptr(), d.as_ptr(), 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}


/// `snapshot_export` の結果。どのファイルを書き出したか。
#[derive(Debug, Clone)]
pub struct SnapshotFiles {
    pub main: String,
    pub oplog: Option<String>,
    pub crc: Option<String>,
}

/// `audit()` に渡すフィルタ条件。None は「そのフィールドで絞らない」。
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// HLC 下限(inclusive)。これより古い record は除外。
    pub from_hlc: Option<enchudb_oplog::Hlc>,
    /// HLC 上限(inclusive)。これより新しい record は除外。
    pub to_hlc: Option<enchudb_oplog::Hlc>,
    /// 書き手 peer_id。これ以外の author を除外。
    pub author_peer: Option<enchudb_oplog::PeerId>,
    /// 署名者 pubkey の指紋(8B)。一致しない record は除外。
    pub pubkey_fp: Option<[u8; 8]>,
}

// ════════════════ バッキングストア ════════════════

/// v10 (request21): DB 本体は **directory + segment file 群** (`SegmentSet`)。
/// wasm / packed 1 blob (`from_bytes`) は `Memory`。 どちらも `region(kind)` で
/// store 用の `Region` を切る (Segments は segment 全体、 Memory は packed offset)。
/// [`Engine::probe`] の結果。 open せずに path の素性を言う。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbState {
    /// 何も無い (新規作成してよい)。
    Missing,
    /// v10 の DB directory で、 segment が揃っている。
    Ready,
    /// directory はあるが初期化が完了していない (create が途中で落ちた等)。
    Incomplete,
    /// segment が欠けている / 前回 flush より短い。 文字列は理由。
    Damaged(String),
    /// v8 / v9 の 1 ファイル DB。 `Engine::migrate_v9_to_v10` で移行する。
    SingleFileLegacy,
}

enum Backing {
    #[cfg(not(target_arch = "wasm32"))]
    Segments(SegmentSet),
    Memory(Vec<u8>),
}

impl Backing {
    fn region(&self, kind: SegmentKind, layout: &Layout) -> Region {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Segments(set) => set.region(kind),
            Backing::Memory(v) => unsafe {
                Region::new(
                    (v.as_ptr() as *mut u8).add(layout.region_off(kind)),
                    layout.segment_size(kind),
                )
            },
        }
    }

    /// packed 1 blob の長さ (Memory のみ)。 `layout.total_size` との突合に使う。
    fn memory_len(&self) -> Option<usize> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Segments(_) => None,
            Backing::Memory(v) => Some(v.len()),
        }
    }

    /// `&self` 経由で header (先頭 HEADER_SIZE) を書く。 書くのは himo_def_lock 下 /
    /// open 時 / set_peer_id のみで、 reader は runtime にこの領域を読まない前提
    /// (旧 `header_mut` / `slice_mut_shared` / `as_slice_mut` を 1 本に統合)。
    #[allow(clippy::mut_from_ref)]
    fn header_mut(&self, len: usize) -> &mut [u8] {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Segments(set) => set.header_slice_mut(len),
            Backing::Memory(v) => unsafe {
                std::slice::from_raw_parts_mut(v.as_ptr() as *mut u8, len.min(v.len()))
            },
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn flush_to_disk(&self) -> io::Result<()> {
        match self {
            Backing::Segments(set) => set.flush_all(),
            Backing::Memory(_) => Ok(()),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn flush_header(&self, len: usize) -> io::Result<()> {
        self.flush_kind(SegmentKind::Header, 0, len)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn flush_kind(&self, kind: SegmentKind, off: usize, len: usize) -> io::Result<()> {
        match self {
            Backing::Segments(set) => set.flush_kind(kind, off, len),
            Backing::Memory(_) => Ok(()),
        }
    }

    /// himo 列 segment を用意する (define_himo)。 Memory は packed に全 slot がある。
    fn ensure_himo(&self, hid: u32, layout: &Layout) -> io::Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Segments(set) => set.ensure_himo(hid, layout.himo_col_size),
            Backing::Memory(_) => {
                let _ = layout;
                Ok(())
            }
        }
    }

    fn ensure_ver(&self, hid: u32, layout: &Layout) -> io::Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Segments(set) => set.ensure_ver(hid, layout.ver_col_size),
            Backing::Memory(_) => {
                let _ = layout;
                Ok(())
            }
        }
    }

    fn ensure_tomb(&self, layout: &Layout) -> io::Result<()> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Segments(set) => set.ensure_tomb(layout.tomb_size),
            Backing::Memory(_) => {
                let _ = layout;
                Ok(())
            }
        }
    }
}
use crate::append_vec::AppendVec;
use crate::vocabulary::Vocabulary;
use crate::entity_set::EntitySet;
use crate::himo_store::{HimoStore, ValueType};
use crate::content_store::ContentStore;
use crate::leaf_store::{LeafRead, LeafStore, cap_bytes_for_shift, MAX_OFF_SHIFT};
use crate::column::Column;

// ════════════════ ギャロッピング交差 ════════════════
// 旧 query 経路で使っていた。将来再利用の余地あり。

#[allow(dead_code)]
#[inline]
fn galloping_intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (small, big) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if small.is_empty() { return vec![]; }
    let mut result = Vec::with_capacity(small.len());
    let mut lo = 0usize;
    for &val in small {
        lo = gallop_ge(big, val, lo);
        if lo >= big.len() { break; }
        if big[lo] == val { result.push(val); lo += 1; }
    }
    result
}

#[allow(dead_code)]
#[inline]
fn gallop_ge(big: &[u32], val: u32, lo: usize) -> usize {
    let n = big.len();
    if lo >= n { return n; }
    if big[lo] >= val { return lo; }
    let mut step = 1usize;
    let mut hi = lo + step;
    while hi < n && big[hi] < val { step *= 2; hi = (lo + step).min(n); }
    let from = lo + step / 2;
    let to = hi.min(n);
    from + big[from..to].partition_point(|&x| x < val)
}

/// bitmap から set bit の entity ID を抽出。
#[allow(dead_code)]
#[inline]
fn extract_bitmap(bitmap: &[u64]) -> Vec<u32> {
    let mut result = Vec::new();
    for (i, &word) in bitmap.iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros();
            result.push((i * 64 + bit as usize) as u32);
            w &= w - 1;
        }
    }
    result
}

// ════════════════ ファイルレイアウト ════════════════

const FILE_MAGIC: [u8; 4] = *b"ECDB";
/// v5 (進行中): engine が table (行型) を認知する。 v4 DB は anonymous table
/// 1 個に migrate されて open 可能 (= 旧 flat 空間と同じ動作)。
///
/// v4: undo region 廃止 — `UndoLog` を撤去し、 `rollback()` API を削除。
/// undo は WAL 有効時には Commit で自動 clear される redundant な層であり、
/// standalone mode では `record()` 内の spin-wait が consumer 不在で
/// permanent hang を起こしていた (GitHub issue #1)。 v3 DB は再作成必要。
/// v7 (0.13.0, #90): LeafStore の cell offset を word 単位化 (16/32/64GB 選択可)。
/// v6 (0.12.0) は leaf offset が byte なので、 v6 region は self-describing な
/// `off_shift == 0` として v7 engine が read-through で扱う (migration 不要)。
/// v8 (0.15.0, #123): vocab index の slot 選択を hash 下位ビット → **上位ビット** に変更
/// (`Vocabulary::home_slot`)。 index は derived data なので open 時に自動 rebuild される
/// (VIX2 magic を検出して in-place migrate) が、 **0.14 以前の binary は index magic を
/// 検証しない**ため、 新 slot で書かれた clean index を旧 slot 関数で読んで silent に
/// lookup miss する。 これを防ぐために file version を上げ、 旧 binary の open を
/// unsupported version で loud に失敗させる (mixed-version 運用は非サポート)。
/// v9 (request17): per-cell version column + tombstone column。 **領域の有無は
/// version 番号ではなく `H_CELL_VERSION` flag が真実** (leaf region の
/// `H_LEAF_DATA_SIZE == 0` と同じ self-describing 方式)。 v8 で作られた DB を v9
/// binary で開くと version stamp だけ 9 に上がるが flag は 0 のままで、 layout は
/// 1 byte も変わらない (= migration 不要)。 version を上げるのは、 v9 領域を持つ DB を
/// **旧 binary が開いて version column を無視したまま書く**のを止めるため。
const FILE_VERSION: u32 = 10;
/// v9 = 1 ファイル固定 layout の最終版 (0.19〜0.25)。 v10 の packed 形式 (`from_bytes`) は
/// byte 互換なので、 Memory backing に限り v9 の blob も受け入れる。
const FILE_VERSION_LEGACY_V9: u32 = 9;
/// v8 (0.15.0〜0.18.x)。 v9 binary で writer open すると version stamp は 9 に上がる
/// (layout は変わらない — v9 領域は `H_CELL_VERSION` flag で管理)。
const FILE_VERSION_LEGACY_V8: u32 = 8;
/// v7 (0.13.0〜0.14.x)。 v10 は v8 以降しか migrate しないので、 「拒否される」 test でだけ使う。
#[cfg(test)]
const FILE_VERSION_LEGACY_V7: u32 = 7;
/// v6 (0.12.0, #88): byte-offset LeafStore。 `migrate_bytes_v5_to_v6` の出力 version
/// (v4 / v5 の定数は v10 で撤去。 v7 以前の file は 0.25.x で v8 に上げてから v10 へ)。
const FILE_VERSION_LEGACY_V6: u32 = 6;
const HEADER_SIZE: usize = 4096;

const DEFAULT_MAX_ENTITIES: u32 = 16_777_216;
/// DB 全体の himo (= table × column の通し) 上限の default。
///
/// #118: **default は 256 据え置き、 raise しない**。 当初 4096 への引き上げを検討したが、
/// himo 領域 = `max_himos × Column::region_size(max_entities, 4)` = **max_entities 比例の
/// per-himo 列領域を max_himos 倍する**構造 (try_from_params L1234-1242)。 16M entity DB で
/// 256→4096 にすると himo 領域が ~16GB→~256GB の apparent (sparse だが macOS/APFS では phys
/// inflate) に膨れる。 → 全 DB の default を上げるのは footprint 的に不可。 代わりに
/// `GrowableOptions { max_himos, .. }` で **必要な consumer (sinfo 等) が明示的に引き上げる**
/// (自分の DB の apparent 増を承知の上で opt-in)。 header 焼き込みなので既存 DB は rebuild
/// するまで旧値のまま。
const DEFAULT_MAX_HIMOS: u32 = 256;
const DEFAULT_CYL_MAX_VALUES: u32 = 65536;
const DEFAULT_VOCAB_DATA_SIZE: usize = 512 * 1024 * 1024;
/// v6 (0.12.0, #88): Leaf payload 用 `LeafStore` の default 予約。 vocab と同等
/// (set_len sparse / growable lazy commit なので実 usage まで物理消費しない)。 tunable。
const DEFAULT_LEAF_DATA_SIZE: usize = 512 * 1024 * 1024;
/// v7 (0.13.0, #90): 新規 DB の LeafStore offset shift の default = 2 (16GB cap、
/// slot align4 で v6 と byte 等価)。
const DEFAULT_LEAF_OFF_SHIFT: u32 = 2;

/// v7 (#90): LeafStore region の addressable 上限を選ぶ (create 時)。 cell 参照を
/// word offset (`byte >> shift`) で持つことで、 列幅・indirection を増やさず cap を
/// 拡げる。 大きいほど slot alignment (= padding) が粗くなるので、 payload が小さい
/// 用途は `Gb16`、 wikipulse のような大 payload × 巨大 working set は `Gb32`/`Gb64`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafScale {
    /// shift 2 / align4 / ~16GB。 default。
    Gb16,
    /// shift 3 / align8 / ~32GB。
    Gb32,
    /// shift 4 / align16 / ~64GB。
    Gb64,
}

impl LeafScale {
    #[inline]
    pub fn off_shift(self) -> u32 {
        match self {
            LeafScale::Gb16 => 2,
            LeafScale::Gb32 => 3,
            LeafScale::Gb64 => 4,
        }
    }
    #[inline]
    pub fn cap_bytes(self) -> u64 { cap_bytes_for_shift(self.off_shift()) }
}

/// #118: growable backing の全 layout knob を 1 struct でまとめて指定する。
///
/// 個別 variant (`create_growable_with_capacity` / `_with_options` / `_with_leaf`) は
/// 部分被覆で組合せ不可 (max_entities と max_himos を同時指定できない等)、 かつ knob が
/// 増えるたび variant が増殖していた。 これを一本化する。 未指定 field は `Default` で
/// 埋まるので、 **気にする knob だけ struct-update で上書き**する:
///
/// ```ignore
/// Engine::create_growable_opts(path, GrowableOptions { max_himos: 8192, ..Default::default() })?;
/// ```
///
/// 将来 knob を足しても `..Default::default()` 利用側は無変更 = variant 爆発が止まる。
#[derive(Debug, Clone)]
pub struct GrowableOptions {
    /// DB 全体の entity (eid) 上限。 default 16 M。
    pub max_entities: u32,
    /// DB 全体の himo (= table × column 通し) 上限。 default 4096。 header 焼き込みなので
    /// 既存 DB は rebuild しないと変わらない。
    pub max_himos: u32,
    /// vocab (Tag/Leaf 値) データ領域の予約 byte。 default 512 MiB (sparse、 未使用なら実消費 0)。
    pub vocab_data_size: usize,
    /// content 領域の予約 byte。 `None` = engine 既定。
    pub content_data_size: Option<usize>,
    /// himo あたり cylinder の値上限。 default 65536。
    pub cyl_max_values: u32,
    /// leaf データ領域の予約 byte。 `None` = engine 既定 (512 MiB)。
    pub leaf_data_size: Option<usize>,
    /// leaf offset scale (16/32/64 GB)。 default `Gb16`。
    pub leaf_scale: LeafScale,
    /// #122: vocab (Tag/Leaf 値) 索引の entry 上限。 `None` = `max_entities × 16`
    /// (上限 256 M) の従来式。
    ///
    /// **vocab に入る値の種類数は entity 数と相関しない**。 グラフのように辺が entity の
    /// 大半を占める形では従来式が実需の 1,000 倍以上を確保する (日本法令コーパスの実測:
    /// entity 44 M → 索引 268 M slot × 13 B = 3.49 GB / on-disk 1,260 MB に対し、
    /// ユニーク Tag 値は 104,971 = 充填率 0.04%)。 実測に基づく値を渡せば索引は実需
    /// サイズに縮む (上の例では 13.6 MB 相当)。
    ///
    /// 値は内部で `next_power_of_two` に丸められ、 header に焼かれる (= 既存 DB は
    /// rebuild しないと変わらない、 `max_himos` と同じ性質)。
    pub vocab_max_entries: Option<u32>,
    /// v10 Phase 3: entity の reservation (= `grow_entity_cap` の上限)。 `None` は既定
    /// (unix: max(max_entities, 2^28)、 Windows: max_entities = 伸ばせない)。
    pub reserve_entities: Option<u32>,
}

impl Default for GrowableOptions {
    fn default() -> Self {
        Self {
            max_entities: DEFAULT_MAX_ENTITIES,
            max_himos: DEFAULT_MAX_HIMOS,
            vocab_data_size: DEFAULT_VOCAB_DATA_SIZE,
            content_data_size: None,
            cyl_max_values: DEFAULT_CYL_MAX_VALUES,
            leaf_data_size: None,
            leaf_scale: LeafScale::Gb16,
            vocab_max_entries: None,
            reserve_entities: None,
        }
    }
}

// ヘッダオフセット
const H_MAGIC: usize = 0;
const H_VERSION: usize = 4;
const H_MAX_ENTITIES: usize = 8;
const H_MAX_HIMOS: usize = 12;
const H_HIMO_COUNT: usize = 16;
const H_VOCAB_MAX_ENTRIES: usize = 20;
const H_VOCAB_INDEX_CAP: usize = 24;
const H_VOCAB_DATA_SIZE: usize = 28;  // u64
const H_HIMOREG_MAX_ENTRIES: usize = 36;
const H_HIMOREG_INDEX_CAP: usize = 40;
const H_HIMOREG_DATA_SIZE: usize = 44; // u64
const H_CONTENT_DATA_SIZE: usize = 52; // u64
const H_CYL_MAX_VALUES: usize = 60;
/// ヘッダ整合性 CRC。[H_MAGIC..H_HEADER_CRC] の CRC32(FNV-1a)。
/// create/flush/define_himo 時に更新、open 時に検証。
const H_HEADER_CRC: usize = 64; // u32
/// この DB を所有する peer の id。0 は「未設定 / single peer」。
/// CRC 保護外(後から set_peer_id で上書き可能)。
const H_PEER_ID: usize = 68; // u32
/// 72..76 は予約済み (v3 まで `H_UNDO_MAX_ENTRIES` が居た跡地)。 v4 で undo 全廃に
/// 伴って read/write 共に廃止、 オフセット安定のためスロットは空のまま (= 0)
/// を保つ。 後続フィールドを追加するなら 80.. を使うこと。
/// Backing kind flag (u32). 0 = EAGER (default, also legacy zero-fill), 1 = GROWABLE.
/// 用途: `validate_file_size` で auto-extend を許すか strict check するかの分岐のみ。
/// CRC 保護外。 accidental truncation 検出が目的、 adversarial tampering は対象外。
#[allow(dead_code)] // v9 header の layout 記録 (offset 76 は予約のまま)。 v10 は backing 種別を持たない
const H_BACKING_KIND: usize = 76; // u32
/// v6 (0.12.0, #88): LeafStore data region size (u64)。 0 = leaf region 無し
/// (pre-v6 DB)。 CRC 保護外 (H_PEER_ID / H_BACKING_KIND と同様、 破損は
/// try_from_params の checked arithmetic + u32::MAX assert で捕捉)。
const H_LEAF_DATA_SIZE: usize = 80; // u64
/// v9 (request17): per-cell version column + tombstone column を持つか (u32、 0 = 無し)。
/// sizes は `max_entities` / `max_himos` から一意に決まるので flag 1 本で足りる。
/// CRC 保護外 (H_LEAF_DATA_SIZE と同様) だが、 破損して 1 に化けても layout の
/// total_size が file size を超えて open 時に `backing too small` で弾かれる。
const H_CELL_VERSION: usize = 88; // u32
/// v10 Phase 3 (request20): entity の reservation = `grow_entity_cap` で伸ばせる上限。 mmap の
/// 予約長と EntitySet の bitset 容量がこれで決まる。 0 (v10 初期 / legacy) は `max_entities`。
/// header CRC の範囲外 (grow で書き換えるのは `H_MAX_ENTITIES` だけ)。
const H_RESERVE_ENTITIES: usize = 92; // u32

/// v10 Phase 3: create 時の reservation 既定。 unix は仮想空間だけなので大きく取る (2^28 entity
/// = Column 4 B で 1 GB / 版数 16 B で 4 GB の仮想)。 Windows は sparse file を reservation
/// 長で作る (`segment_map_windows`) ので apparent に出る → cap と同値 (= 伸ばせない。 伸ばす
/// なら `GrowableOptions::reserve_entities` で明示)。
const DEFAULT_RESERVE_ENTITIES: u32 = 1 << 28;

fn default_reserve_entities(max_entities: u32) -> u32 {
    if cfg!(windows) { max_entities } else { max_entities.max(DEFAULT_RESERVE_ENTITIES) }
}

#[allow(dead_code)] // 同上 (v9 growable の識別値)
const BACKING_KIND_GROWABLE: u32 = 1;
const H_HIMO_TYPES: usize = 256;

// header 2048.. は旧 v0.4.0 までの NTupleView 永続化スロット (H_VIEW_COUNT /
// H_VIEWS_OFF + N × 17 bytes) の跡地。 issue #4 で NTupleTable / define_view を
// 撤去したのに伴い read / write 共に廃止。 file format は v4 のまま (header bytes
// は 0 で残置)、 既存 DB は何もせず読める。

fn align8(n: usize) -> usize { (n + 7) & !7 }

/// v9 (request17-A): version column の 1 cell が持つ HLC のバイト数
/// (`wall: u64` + `logical: u32` + `peer: u32`)。 `Hlc::ZERO` (全 0) が「版数不明」を
/// 意味するので、 zero-fill された region がそのまま正しい初期状態になる。
const HLC_CELL_BYTES: u32 = 16;

/// v9 (request17-A): HLC → version column の 1 cell (16B LE)。
/// layout は `wall: u64` / `logical: u32` / `peer: u32` の順。
#[inline]
fn hlc_to_cell(h: enchudb_oplog::Hlc) -> [u8; HLC_CELL_BYTES as usize] {
    let mut b = [0u8; HLC_CELL_BYTES as usize];
    b[0..8].copy_from_slice(&h.wall.to_le_bytes());
    b[8..12].copy_from_slice(&h.logical.to_le_bytes());
    b[12..16].copy_from_slice(&h.peer.to_le_bytes());
    b
}

/// v9 (request17-A): version column の 1 cell → HLC。 zero-fill された cell は
/// そのまま `Hlc::ZERO` (= 版数不明) になる。
#[inline]
fn hlc_from_cell(b: &[u8]) -> enchudb_oplog::Hlc {
    debug_assert_eq!(b.len(), HLC_CELL_BYTES as usize);
    enchudb_oplog::Hlc {
        wall: u64::from_le_bytes(b[0..8].try_into().unwrap()),
        logical: u32::from_le_bytes(b[8..12].try_into().unwrap()),
        peer: u32::from_le_bytes(b[12..16].try_into().unwrap()),
    }
}

/// v9 (request17-A): version column を region から作る。 fresh な (= zero-fill
/// された) region は Column header が空なので `init`、 既存 v9 DB の region は
/// `load` する。 判定は header の value_size (offset 4..8) を覗くだけ。
fn ver_column_from_region(region: Region, max_entities: u32) -> Column {
    // request18: growable backing では v9 領域は variable cluster の末尾 = 初期
    // commit の外にある。 commit は **単調 high-water** なので、 header 16B を
    // 読むためだけに `ensure_committed` を呼ぶと手前の vocab_data / content_data /
    // leaf_data が丸ごと commit される (100K entity の growable DB で create 直後
    // 1.7 GB、 1M entity で 3.1 GB)。 Phase B Step 3 の lazy commit 設計が
    // v9 で無効化されていた。
    //
    // まだ commit が届いていない region には **書かれた版数が存在し得ない**ので、
    // 触らずに空 column として組み立ててよい。 header は最初の実書き込み直前に
    // `Column::ensure_header` が書く。
    if !region.is_committed(crate::column::HEADER_BYTES) {
        return Column::init_lazy(region, HLC_CELL_BYTES, max_entities);
    }
    let stored_vs = u32::from_le_bytes(region.slice()[4..8].try_into().unwrap());
    if stored_vs == HLC_CELL_BYTES {
        Column::load(region)
    } else {
        Column::init(region, HLC_CELL_BYTES, max_entities)
    }
}

/// ヘッダ整合性 CRC を計算する対象領域。
/// magic, version, max_entities, max_himos, himo_count,
/// vocab_*, himoreg_*, content_data_size, cyl_max_values の固定レイアウト部のみ。
/// value_types/max_values 領域は runtime で変動するので CRC 範囲外。

#[inline]
fn compute_header_crc(buf: &[u8]) -> u32 {
    // [0..H_HEADER_CRC) 範囲を FNV-1a 32bit。
    // 固定レイアウトメタデータ(magic, version, max_entities, max_himos, himo_count,
    // vocab_*, himoreg_*, content_data_size, cyl_max_values)のみを対象。
    // 再現性デバッグ用。
    let mut h: u32 = 0x811c9dc5;
    for &b in &buf[0..H_HEADER_CRC] {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    let _dbg = (h, &buf[0..H_HEADER_CRC]);
    h
}

#[inline]
fn write_header_crc(buf: &mut [u8]) {
    let crc = compute_header_crc(buf);
    buf[H_HEADER_CRC..H_HEADER_CRC + 4].copy_from_slice(&crc.to_le_bytes());
}

/// ヘッダ CRC を検証。不一致なら Err。
fn verify_header_crc(buf: &[u8]) -> Result<(), String> {
    let stored = u32::from_le_bytes(buf[H_HEADER_CRC..H_HEADER_CRC + 4].try_into().unwrap());
    if stored == 0 { return Ok(()); }
    let computed = compute_header_crc(buf);
    if stored != computed {
        return Err(format!(
            "header CRC mismatch: stored={:08x}, computed={:08x} — file may be corrupt",
            stored, computed,
        ));
    }
    Ok(())
}

/// 0.9.0 (L1): header field の自己整合チェック。
/// `verify_header_crc` は stored CRC == 0 (header CRC 導入以前の legacy DB) を素通しするため、
/// 破損した himo_count / *_size / *_cap がそのまま layout 計算へ流れて panic /
/// OOB region を起こし得た。 ここで「header 自身の field 同士の関係」だけを検証
/// する (新しい定数上限は導入しない → 既存の正常 DB を誤って弾かない)。
fn sanity_check_header_fields(
    max_himos: u32, himo_count: u32,
    vocab_max_entries: u32, vocab_index_cap: u32, vocab_data_size: usize,
    himoreg_max_entries: u32, himoreg_index_cap: u32, himoreg_data_size: usize,
    content_data_size: usize,
) -> Result<(), String> {
    if himo_count > max_himos {
        return Err(format!(
            "himo_count {} exceeds max_himos {} — corrupt header", himo_count, max_himos,
        ));
    }
    // himo id は u16 (define_himo が `hid >= u16::MAX` を拒否する)。 それを超える max_himos は
    // header 表のサイズが破綻する (#246) ので破損扱い。
    if max_himos > u16::MAX as u32 {
        return Err(format!(
            "max_himos {} exceeds format limit {} (u16 himo id) — corrupt header", max_himos, u16::MAX,
        ));
    }
    // data_end は mmap 上の AtomicU32 (#77)。 u32::MAX 超は create 時点で拒否
    // されるので、 header にあれば破損。
    for (name, size) in [
        ("vocab_data_size", vocab_data_size),
        ("himoreg_data_size", himoreg_data_size),
        ("content_data_size", content_data_size),
    ] {
        if size > u32::MAX as usize {
            return Err(format!(
                "{} {} exceeds format limit {} (u32 data_end) — corrupt header",
                name, size, u32::MAX,
            ));
        }
    }
    // vocab / himoreg の index は cap-1 を hash mask に使う線形 probe
    // (Vocabulary::lookup)。 cap == 0 は即 OOB / 除算相当、 非 2^n は probe が
    // 全 slot を巡回できず無限 loop。 cap < max_entries は index が先に満杯に
    // なり insert が無限 probe。 create 経路は必ず next_power_of_two(>= entries)
    // で焼くので、 これらを満たさない header は破損。
    for (name, max_entries, index_cap) in [
        ("vocab", vocab_max_entries, vocab_index_cap),
        ("himoreg", himoreg_max_entries, himoreg_index_cap),
    ] {
        if max_entries == 0 || index_cap == 0 {
            return Err(format!(
                "{} max_entries {} / index_cap {} must be nonzero — corrupt header",
                name, max_entries, index_cap,
            ));
        }
        if !index_cap.is_power_of_two() {
            return Err(format!(
                "{} index_cap {} is not a power of two — corrupt header", name, index_cap,
            ));
        }
        if index_cap < max_entries {
            return Err(format!(
                "{} index_cap {} < max_entries {} — corrupt header", name, index_cap, max_entries,
            ));
        }
    }
    Ok(())
}

fn himo_maxv_base(max_himos: u32) -> usize {
    (H_HIMO_TYPES + max_himos as usize + 3) & !3
}

/// v10: header segment の長さ。 固定 field (先頭 `HEADER_SIZE`) の後ろに himo 型表
/// (`max_himos` B) と max_values 表 (`max_himos` × 4 B) が続くので、 max_himos に依存する。
///
/// 旧 1 ファイル layout は header を `HEADER_SIZE` (4096) 固定で entities region を 4096 から
/// 置いていたため、 **max_himos > 約 960 で himo 表が entities region に食い込んでいた**
/// (#246)。 v10 は region が独立 file なので header を必要なだけ伸ばせる。
fn header_size_for(max_himos: u32) -> usize {
    let need = himo_maxv_base(max_himos) + (max_himos as usize) * 4;
    (need.max(HEADER_SIZE) + 4095) & !4095
}

struct Layout {
    /// v10: header segment の長さ (`header_size_for`)。 packed 形式では entities_off と一致。
    header_size: usize,
    entities_off: usize,
    entities_size: usize,
    vocab_data_off: usize,
    vocab_data_size: usize,
    vocab_offsets_off: usize,
    vocab_offsets_size: usize,
    vocab_index_off: usize,
    vocab_index_size: usize,
    vocab_max_entries: u32,
    vocab_index_cap: u32,
    himoreg_data_off: usize,
    himoreg_data_size: usize,
    himoreg_offsets_off: usize,
    himoreg_offsets_size: usize,
    himoreg_index_off: usize,
    himoreg_index_size: usize,
    himoreg_max_entries: u32,
    himoreg_index_cap: u32,
    content_index_off: usize,
    content_index_size: usize,
    content_data_off: usize,
    content_data_size: usize,
    leaf_data_off: usize,
    leaf_data_size: usize,
    /// v9 (request17-A): per-cell version column の base。 himo ごとに **HLC を生で**
    /// (16B: wall u64 / logical u32 / peer u32) 並べる (`Column` と同じ形)。
    /// **variable cluster の末尾**に置くので、 pre-v9 DB の既存 region は 1 byte も
    /// 動かない (`leaf_data` の「size 0 なら region 無し」と同じ)。
    /// `ver_col_size == 0` = version column 無し (pre-v9)。
    ///
    /// `Hlc::ZERO` (全 0) が「版数不明」を意味するので、 zero-fill された region が
    /// そのまま正しい初期状態になる (追加の初期化不要 / A-1)。
    ver_base_off: usize,
    ver_col_size: usize,
    /// v9 (request17-A5): tombstone version column。 削除は himo を持たない
    /// (`Delete { eid }`) ので himo ごとの version column には置けず、 eid 空間に
    /// **1 本だけ**持つ。 `tomb_size == 0` = 無し (pre-v9)。
    tomb_off: usize,
    tomb_size: usize,
    himo_base_off: usize,
    himo_col_size: usize,
    #[allow(dead_code)]
    himo_cyl_size: usize,
    himo_slot_size: usize,
    cyl_max_values: u32,
    total_size: usize,
    /// v10 Phase 3: 現在の entity 上限 (header `max_entities`)。 packed の region size はこれ基準。
    max_entities: u32,
    /// v10 Phase 3: entity の reservation (= `grow_entity_cap` の上限、 header offset 92)。
    /// mmap の予約 (`segment_reserve`) と EntitySet の bitset 容量はこれ基準。 legacy は
    /// `max_entities` と同値。
    reserve_entities: u32,
    content_index_reserve: usize,
    himo_col_reserve: usize,
    ver_col_reserve: usize,
    tomb_reserve: usize,
}

impl Layout {
    /// #120: 上限超過 (align8 後の u32 data_end / total_size overflow) は
    /// **書く前に** Err で返す。 呼び元 (create 経路) は `io::ErrorKind::InvalidInput`
    /// に写して伝播すること — panic にすると caller が握れない。
    fn compute(max_entities: u32, max_himos: u32, vocab_data_size: usize, content_data_size: Option<usize>, cyl_max_values: Option<u32>, leaf_data_size: Option<usize>, vocab_max_entries: Option<u32>, cell_version: bool, reserve_entities: Option<u32>) -> Result<Self, String> {
        // #122: 既定は max_entities × 16 (上限 256 M)。 vocab の値の種類数は entity 数と
        // 相関しないので、 実測で分かっている consumer は GrowableOptions で明示する。
        let vocab_max_entries = match vocab_max_entries {
            // #120 と同じ原則: 上限超過は **書く前に** Err。 2^31 超は下の
            // `next_power_of_two()` が overflow し (release では 0 に化けて header に
            // 焼かれ、 open が "index_cap 0 must be nonzero" で恒久失敗する)。
            Some(v) if v > (1u32 << 31) => {
                return Err(format!(
                    "vocab_max_entries {} exceeds format limit {} (next_power_of_two が u32 を溢れる)",
                    v,
                    1u32 << 31,
                ));
            }
            Some(v) => v.max(1),
            None => max_entities.saturating_mul(16).min(256_000_000),
        };
        Self::compute_with_caps(
            max_entities, max_himos,
            vocab_max_entries, vocab_data_size,
            content_data_size, cyl_max_values, leaf_data_size,
            cell_version,
            reserve_entities,
        )
    }

    /// `compute` のフル版 — vocab_max_entries も override できる。
    /// tiny preset で multiplier ×16 が過剰な場合に ×1 程度に絞る用。
    fn compute_with_caps(
        max_entities: u32,
        max_himos: u32,
        vocab_max_entries: u32,
        vocab_data_size: usize,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
        leaf_data_size: Option<usize>,
        // v9 (request17-A): per-cell version column を持つか。
        cell_version: bool,
        reserve_entities: Option<u32>,
    ) -> Result<Self, String> {
        let vocab_index_cap = vocab_max_entries.next_power_of_two();
        let himoreg_max_entries = max_himos.max(256);
        let himoreg_index_cap = (himoreg_max_entries * 2).next_power_of_two();
        let himoreg_data_size = 64 * 1024;
        let content_data_size = content_data_size.unwrap_or_else(ContentStore::data_region_size);
        let cyl_max_values = cyl_max_values.unwrap_or(DEFAULT_CYL_MAX_VALUES);

        // v6 (#88): create 経路の leaf region 予約サイズ。 None = default。
        // Some(0) は「leaf region 無し」= v5 相当 DB (migration test / bench の
        // before 生成、 及び將来の pre-v6 互換 create に使う)。
        //
        // #120: u32 上限の検証は `try_from_params` 内の **align8 後** の値に対して
        // 行う (整列前の要求値だけを見ていたため、 u32::MAX の create が成功して
        // header に 2^32 が焼かれ、 open 不能な DB ができていた)。
        let leaf_data_size = leaf_data_size.unwrap_or(DEFAULT_LEAF_DATA_SIZE);
        Self::try_from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
            cell_version,
            reserve_entities.unwrap_or_else(|| default_reserve_entities(max_entities)),
        )
    }

    /// 0.9.0 (L1): checked arithmetic 版。 header CRC==0 の legacy DB では
    /// 破損した size field がそのまま流れ込むため、 usize wrap で過小な
    /// total_size を計算 → OOB region を map する事故を Err で防ぐ。
    fn try_from_params(
        max_entities: u32, max_himos: u32,
        vocab_max_entries: u32, vocab_index_cap: u32, vocab_data_size: usize,
        himoreg_max_entries: u32, himoreg_index_cap: u32, himoreg_data_size: usize,
        content_data_size: usize, leaf_data_size: usize, cyl_max_values: u32,
        // v9 (request17-A): per-cell version column を持つか。
        // false = pre-v9 DB、 または v9 を有効化していない create。
        cell_version: bool,
        reserve_entities: u32,
    ) -> Result<Self, String> {
        Self::try_from_params_with_header(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
            cell_version,
            header_size_for(max_himos),
            reserve_entities,
        )
    }

    /// `header_size` を外から与える版。 v10 は `header_size_for(max_himos)` (himo 表が
    /// 溢れない可変長、 #246) だが、 **v8 / v9 の 1 ファイル DB は固定 4096** なので legacy
    /// packed の parse はこちらで offset を出す (可変長で計算すると region が全部ずれる)。
    fn try_from_params_with_header(
        max_entities: u32, max_himos: u32,
        vocab_max_entries: u32, vocab_index_cap: u32, vocab_data_size: usize,
        himoreg_max_entries: u32, himoreg_index_cap: u32, himoreg_data_size: usize,
        content_data_size: usize, leaf_data_size: usize, cyl_max_values: u32,
        cell_version: bool,
        header_size: usize,
        reserve_entities: u32,
    ) -> Result<Self, String> {
        let reserve_entities = reserve_entities.max(max_entities);
        if reserve_entities > u32::MAX - 7 {
            return Err(format!(
                "reserve_entities {} exceeds format limit — corrupt header?", reserve_entities,
            ));
        }
        // EntitySet::region_size 内部の `(max_entities + 7)` が u32 で wrap しない
        // ガード。 この値の DB は create 時点で作れない (debug では overflow panic)。
        if max_entities > u32::MAX - 7 {
            return Err(format!(
                "max_entities {} exceeds format limit — corrupt header?", max_entities,
            ));
        }
        // v7 (#90): LeafStore の high_water は word u32。 addressable 上限は
        // off_shift で決まる (16/32/64GB)。 layout は shift を知らないので、 ここでは
        // 絶対上限 (= MAX_OFF_SHIFT = 64GB) だけ弾く。 shift 個別の cap は create 側で
        // `LeafScale::cap_bytes` に対して検証する。
        let leaf_abs_cap = cap_bytes_for_shift(MAX_OFF_SHIFT);
        if leaf_data_size as u64 > leaf_abs_cap {
            return Err(format!(
                "leaf_data_size {} exceeds absolute format limit {} (64GB) — corrupt header?",
                leaf_data_size, leaf_abs_cap,
            ));
        }
        let ck_add = |a: usize, b: usize| -> Result<usize, String> {
            a.checked_add(b)
                .ok_or_else(|| "layout total_size overflow — corrupt header fields?".to_string())
        };

        // v3 layout: 固定上限の region 群を前に、 append-only な
        // variable region 群 (vocab_data / himoreg_data / content_data)
        // を末尾に集める。 これで「ファイル末尾のみ伸びる」 monotonic
        // grow が実現でき、 sparse hole に頼らずに apparent size を
        // 実 usage に追従させられる (Phase B Step 1)。
        let mut off = header_size;

        // ── 固定 cluster: 上限が max_entities / max_himos / *_cap で決まる ──
        let entities_off = off;
        // v10: EntitySet の bitset は reservation 分の場所を取る (cap を伸ばしても offset 不動)
        let entities_size = align8(EntitySet::region_size(reserve_entities));
        off = ck_add(off, entities_size)?;

        let vocab_offsets_off = off;
        let vocab_offsets_size = align8(Vocabulary::offsets_region_size(vocab_max_entries));
        off = ck_add(off, vocab_offsets_size)?;

        let vocab_index_off = off;
        let vocab_index_size = align8(Vocabulary::index_region_size(vocab_index_cap));
        off = ck_add(off, vocab_index_size)?;

        let himoreg_offsets_off = off;
        let himoreg_offsets_size = align8(Vocabulary::offsets_region_size(himoreg_max_entries));
        off = ck_add(off, himoreg_offsets_size)?;

        let himoreg_index_off = off;
        let himoreg_index_size = align8(Vocabulary::index_region_size(himoreg_index_cap));
        off = ck_add(off, himoreg_index_size)?;

        let content_index_off = off;
        let content_index_size = align8(ContentStore::index_region_size_for(max_entities));
        off = ck_add(off, content_index_size)?;

        let himo_col_size = align8(Column::region_size(max_entities, 4));
        let himo_cyl_size = 0usize;
        let himo_slot_size = himo_col_size;

        let himo_base_off = off;
        let himo_total = himo_slot_size
            .checked_mul(max_himos as usize)
            .ok_or_else(|| "layout himo region overflow — corrupt header fields?".to_string())?;
        off = ck_add(off, himo_total)?;

        // ── Variable cluster (tail): append-only で伸びる region 群 ──
        //
        // #77: data_end (vocab / himoreg / content) は mmap 上の AtomicU32 で、 index の
        // offset/len も u32。 4 GiB 超の data region を許すと append offset が wrap して
        // 先頭から silent 上書きするため拒否する。
        // #120: 検証は **align8 後** の値に対して行う。 align8 は u32::MAX を 2^32 に
        // 切り上げるので、 整列前だけ見ると `vocab_data_size = u32::MAX` の create が
        // 通り、 header には 2^32 が焼かれ、 open 側検証 (validate_header の
        // "u32 data_end") で恒久的に開けない DB ができる (create もビルドも成功した
        // 後に全損する最悪の壊れ方)。
        let ck_u32 = |name: &str, aligned: usize| -> Result<usize, String> {
            if aligned > u32::MAX as usize {
                return Err(format!(
                    "{} {} (align8 後) exceeds format limit {} (u32 data_end)",
                    name, aligned, u32::MAX,
                ));
            }
            Ok(aligned)
        };

        let vocab_data_off = off;
        let vocab_data_size =
            ck_u32("vocab_data_size", align8(Vocabulary::data_region_size(vocab_data_size)))?;
        off = ck_add(off, vocab_data_size)?;

        let himoreg_data_off = off;
        let himoreg_data_size =
            ck_u32("himoreg_data_size", align8(Vocabulary::data_region_size(himoreg_data_size)))?;
        off = ck_add(off, himoreg_data_size)?;

        let content_data_off = off;
        let content_data_size = ck_u32("content_data_size", align8(content_data_size))?;
        off = ck_add(off, content_data_size)?;

        // v6 (#88): Leaf payload store。 append-only variable cluster の末尾に追加。
        // size 0 (pre-v6 header) なら region 無し (off 不変)。
        let leaf_data_off = off;
        let leaf_data_size = align8(leaf_data_size);
        off = ck_add(off, leaf_data_size)?;

        // v9 (request17-A): per-cell version column (himo ごと) + tombstone column (1 本)。
        // leaf と同じく variable cluster の末尾に置くので、 **pre-v9 DB の既存 region は
        // 動かない**。 無効時は両方 size 0 で layout は pre-v9 と byte 単位で同一。
        //
        // cell には HLC を **生で** 置く (16B)。 intern 表案は `next_hlc()` が record ごとに
        // logical を進める実装のため「同じ HLC が繰り返し出る」ことが無く、
        // `4B(id) + 16B(entry)` = 20B/cell で生 HLC (16B) より重くなるので却下した。
        //
        // 並びは **tombstone → himo ごとの version column** の順。 growable backing の
        // commit は単調な high-water mark なので、 後ろの region を触ると手前が丸ごと
        // commit される (= apparent size が跳ねる)。 tombstone は 1 本しか無いので、
        // 「delete しただけで version column 全体 (himo 数 × 16B/cell) が commit される」
        // のを避けるために手前に置く。
        let tomb_off = off;
        let tomb_size = if cell_version {
            align8(Column::region_size(max_entities, HLC_CELL_BYTES))
        } else {
            0
        };
        off = ck_add(off, tomb_size)?;

        let ver_base_off = off;
        let (ver_col_size, ver_total) = if cell_version {
            let s = align8(Column::region_size(max_entities, HLC_CELL_BYTES));
            let total = s
                .checked_mul(max_himos as usize)
                .ok_or_else(|| "layout version column region overflow".to_string())?;
            (s, total)
        } else {
            (0, 0)
        };
        off = ck_add(off, ver_total)?;

        Ok(Layout {
            header_size,
            ver_base_off, ver_col_size,
            tomb_off, tomb_size,
            entities_off, entities_size,
            vocab_data_off, vocab_data_size,
            vocab_offsets_off, vocab_offsets_size,
            vocab_index_off, vocab_index_size,
            vocab_max_entries, vocab_index_cap,
            himoreg_data_off, himoreg_data_size,
            himoreg_offsets_off, himoreg_offsets_size,
            himoreg_index_off, himoreg_index_size,
            himoreg_max_entries, himoreg_index_cap,
            content_index_off, content_index_size,
            content_data_off, content_data_size,
            leaf_data_off, leaf_data_size,
            himo_base_off, himo_col_size, himo_cyl_size, himo_slot_size,
            max_entities, reserve_entities,
            content_index_reserve: align8(ContentStore::index_region_size_for(reserve_entities)),
            himo_col_reserve: align8(Column::region_size(reserve_entities, 4)),
            ver_col_reserve: align8(Column::region_size(reserve_entities, HLC_CELL_BYTES)),
            tomb_reserve: align8(Column::region_size(reserve_entities, HLC_CELL_BYTES)),
            cyl_max_values,
            total_size: off,
        })
    }

    fn himo_col_off(&self, hid: usize) -> usize {
        self.himo_base_off + hid * self.himo_slot_size
    }
    /// v9 (request17-A): himo `hid` の version column 先頭 (HLC 16B の並び)。
    /// `ver_col_size == 0` の DB (pre-v9) には領域が無いので、 呼び側が先に
    /// `has_cell_version()` を確認すること。
    fn ver_col_off(&self, hid: usize) -> usize {
        self.ver_base_off + hid * self.ver_col_size
    }
    /// v9 領域 (version column + tombstone column) を持つ DB か。
    fn has_cell_version(&self) -> bool {
        self.ver_col_size > 0 && self.tomb_size > 0
    }
    /// v10: packed 形式 (= 旧 1 ファイル layout) での `kind` の offset。 `Memory` backing 用。
    fn region_off(&self, kind: SegmentKind) -> usize {
        match kind {
            SegmentKind::Header => 0,
            SegmentKind::Entities => self.entities_off,
            SegmentKind::VocabData => self.vocab_data_off,
            SegmentKind::VocabOffsets => self.vocab_offsets_off,
            SegmentKind::VocabIndex => self.vocab_index_off,
            SegmentKind::HimoregData => self.himoreg_data_off,
            SegmentKind::HimoregOffsets => self.himoreg_offsets_off,
            SegmentKind::HimoregIndex => self.himoreg_index_off,
            SegmentKind::ContentIndex => self.content_index_off,
            SegmentKind::ContentData => self.content_data_off,
            SegmentKind::LeafData => self.leaf_data_off,
            SegmentKind::Tomb => self.tomb_off,
            SegmentKind::Himo(h) => self.himo_col_off(h as usize),
            SegmentKind::Ver(h) => self.ver_col_off(h as usize),
        }
    }
}

/// v10: kind → 予約サイズ (= 旧 region size)。 `SegmentSet` が segment の reserve に使う。
impl SegmentSizes for Layout {
    fn segment_size(&self, kind: SegmentKind) -> usize {
        match kind {
            SegmentKind::Header => self.header_size,
            SegmentKind::Entities => self.entities_size,
            SegmentKind::VocabData => self.vocab_data_size,
            SegmentKind::VocabOffsets => self.vocab_offsets_size,
            SegmentKind::VocabIndex => self.vocab_index_size,
            SegmentKind::HimoregData => self.himoreg_data_size,
            SegmentKind::HimoregOffsets => self.himoreg_offsets_size,
            SegmentKind::HimoregIndex => self.himoreg_index_size,
            SegmentKind::ContentIndex => self.content_index_size,
            SegmentKind::ContentData => self.content_data_size,
            SegmentKind::LeafData => self.leaf_data_size,
            SegmentKind::Tomb => self.tomb_size,
            SegmentKind::Himo(_) => self.himo_col_size,
            SegmentKind::Ver(_) => self.ver_col_size,
        }
    }

    /// v10 Phase 3: entity 比例の segment は reservation 分を予約する (cap を伸ばしても
    /// base pointer が動かない)。 それ以外は size と同じ。
    fn segment_reserve(&self, kind: SegmentKind) -> usize {
        match kind {
            SegmentKind::Entities => self.entities_size,
            SegmentKind::ContentIndex => self.content_index_reserve,
            SegmentKind::Himo(_) => self.himo_col_reserve,
            SegmentKind::Ver(_) => self.ver_col_reserve,
            SegmentKind::Tomb => self.tomb_reserve,
            _ => self.segment_size(kind),
        }
    }
}

impl Layout {
    #[allow(dead_code)]
    fn himo_cyl_a_off(&self, hid: usize) -> usize {
        self.himo_base_off + hid * self.himo_slot_size + self.himo_col_size
    }
    #[allow(dead_code)]
    fn himo_cyl_b_off(&self, hid: usize) -> usize {
        self.himo_base_off + hid * self.himo_slot_size + self.himo_col_size + self.himo_cyl_size
    }
}

// ════════════════ Engine ════════════════

// 旧 v0.4.0 まで居た `NTupleEntry` / `NTupleTable` (n-tuple 観測窓) は issue #4 で
// 撤去。 schema / SQL / RAG / sync / transport いずれの crate からも参照されておらず
// (engine 内テストだけが叩いていた)、 query 経路の \"best_lookup_ref\" branch と
// tie / untie / apply_op / open の hook と合わせて約 350 行の dead weight だった。
// 過去の API は `Engine::define_view` で、 0.4.0 までの DB で永続化された
// header H_VIEW_COUNT は今は無視される (zero 残置で害なし)。

// ════════════════ Table (β-light step 2: 内部 data のみ) ════════════════
//
// 「entity に tie する」 = 「行に値を入れる」 ことの自己認識を engine に
// 与えるための table 概念。 各 table は himo set (列) + eid_range (行範囲)
// を持つ。 step 2 では anonymous table (id=0) 1 個だけが存在し、 全 himo /
// 全 entity が自動でここに属する (旧 API 完全互換)。
//
// step 3+ で define_table / entity_in API を公開し、 非 anonymous table の
// 切り出し + Ref validation + table-local positions へ進む。

/// Table 識別子。 u16 で 65536 table までを表現 (実用上十分)。
/// `ANONYMOUS_TABLE` (= 0) は engine 起動時に必ず存在する default table。
pub type TableId = u16;
pub const ANONYMOUS_TABLE: TableId = 0;

/// 1 つの table の定義。 名前 (anonymous は空文字)、 所属 himo の id 列、
/// eid_range (anonymous は open-ended)、 FK 参照 (Ref himo の target table)。
///
/// step 2 段階では engine 内部にしか露出せず、 一部 field は step 3+ で
/// 使われる。 dead code 警告は段階実装の意図的な姿。
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct TableDef {
    /// table 名 (anonymous は ""、 user 定義 table は固有名)。
    pub name: String,
    /// この table の column 軸を成す himo の id 列 (engine.himos の index)。
    /// 0.9.0: `&self` の himo 定義 (`ensure_himo_dynamic_in`) が attach できる
    /// よう RwLock 化。 write は himo_def_lock 下でのみ発生する低頻度 path。
    pub himo_ids: std::sync::RwLock<Vec<u32>>,
    /// この table が占有する eid 範囲の下限 (inclusive)。
    pub eid_range_lo: u32,
    /// 上限 (exclusive)。 `u32::MAX` は open-ended (= まだ後続 table が
    /// 切られていない、 次の entity 確保で伸びる)。 後続 table が定義された
    /// 瞬間に現在の next_eid に固定される。
    pub eid_range_hi: u32,
    /// Ref himo の target table 一覧 (himo_id, target_table_id)。
    /// step 5 で validation に使う。
    pub fk_refs: Vec<(u32, TableId)>,
    /// 次に entity_in で割り当てる eid の table 内 local offset。
    /// global eid = eid_range_lo + next_local。 anonymous table は
    /// `entities.next_eid()` (= EntitySet 直) を使うのでこの field は不参照。
    ///
    /// 0.7.0: `&self entity_in` を支えるため AtomicU32 化。 schema crate
    /// (= Arc<Engine> 経由の concurrent mode) で row insert する path が
    /// CAS で並行 safe に払出できる。
    pub next_local: std::sync::atomic::AtomicU32,
    /// 0.8.0: free list = reclaim で解放された local id の reservoir。
    /// `entity_in(table)` は free list が non-empty なら pop で再利用、
    /// 空なら通常通り `next_local.fetch_add(1)`。 `_sync_ops` の長期運用で
    /// eid 空間飽和を防ぐ (= ring buffer 化の本体)。 user table は今のところ
    /// 自動 reclaim path がないので free list は空のまま。
    pub free_locals: std::sync::Arc<std::sync::Mutex<Vec<u32>>>,
    /// 0.8.0: `free_locals` が non-empty かどうかの fast path flag。 user table
    /// (= 自動 reclaim なし) では常に false、 `entity_in` の hot path で mutex を
    /// 取らずに済む。 reclaim が push したら true に上げる、 entity_in が pop
    /// で空になったら false に戻す。 厳密な race は entity_in 内で mutex 取った
    /// あと再 check するので OK (= AtomicBool は fast path の hint)。
    pub free_locals_nonempty: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// v10 Phase 3 (request20 案 B): 先頭 range (`eid_range_lo..hi`) の後に足した extent 群。
    /// `entity_in` が先頭を使い切ると空き eid 空間から切り足す (auto-grow)。 local id は
    /// 先頭 → extra の順に連結した offset (= `next_local` / `free_locals` はそのまま)。
    /// grow しない table (常態) は空で、 hot path は先頭 range だけ見る。
    pub extra: std::sync::RwLock<Vec<(u32, u32)>>,
    /// #141: この table の primary key を成す himo の id。 PK 未指定なら `None`。
    ///
    /// PK 自体は schema 層 (`TableBuilder::primary_key`) の概念だが、 **sync の
    /// apply 経路は engine より上を見られない** (`enchudb-sync` と `enchudb-schema`
    /// は兄弟 crate)。 apply 側が「同じ PK の既存 row に束ねる」判断をするために、
    /// schema が build 時に `set_table_pk` で engine へ降ろす。
    pub pk_himo: Option<u16>,
}

impl Clone for TableDef {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            himo_ids: std::sync::RwLock::new(self.himo_ids.read().unwrap().clone()),
            eid_range_lo: self.eid_range_lo,
            eid_range_hi: self.eid_range_hi,
            fk_refs: self.fk_refs.clone(),
            next_local: std::sync::atomic::AtomicU32::new(
                self.next_local.load(std::sync::atomic::Ordering::Relaxed)
            ),
            // free_locals は Arc なので share される (= clone は same reservoir を
            // 指す)。 これは TableDef::clone を「meta read 用 snapshot」 として
            // 使う既存 caller の semantic と整合 (= 並列 race しないように caller
            // 側で coordinate する想定)。
            free_locals: self.free_locals.clone(),
            free_locals_nonempty: self.free_locals_nonempty.clone(),
            extra: std::sync::RwLock::new(self.extra.read().unwrap().clone()),
            pk_himo: self.pk_himo,
        }
    }
}

impl TableDef {
    /// anonymous table がまだ閉じていない (= 後続 table が無い) か。
    #[inline]
    fn is_open_ended(&self) -> bool {
        self.eid_range_hi == u32::MAX
    }

    /// 全 extent (先頭 + `extra`)。 open-ended な anonymous は先頭 1 本。
    pub fn extents(&self) -> Vec<(u32, u32)> {
        let mut v = vec![(self.eid_range_lo, self.eid_range_hi)];
        v.extend(self.extra.read().unwrap().iter().copied());
        v
    }

    /// 払い出せる eid 総数。 open-ended は `u32::MAX`。
    pub fn capacity(&self) -> u32 {
        if self.is_open_ended() {
            return u32::MAX;
        }
        let first = self.eid_range_hi - self.eid_range_lo;
        self.extra.read().unwrap().iter().fold(first, |acc, (lo, hi)| acc.saturating_add(hi - lo))
    }

    /// 最後の extent の上限 (exclusive)。 次の table / extent はここから切る。
    pub fn last_hi(&self) -> u32 {
        self.extra.read().unwrap().last().map(|e| e.1).unwrap_or(self.eid_range_hi)
    }

    /// `global` (local eid) がこの table の extent のどれかに入るか。
    #[inline]
    pub fn contains(&self, global: u32) -> bool {
        self.local_of(global).is_some()
    }

    /// global (local eid) → table 内 local offset。 先頭 range は lock 無し。
    #[inline]
    pub fn local_of(&self, global: u32) -> Option<u32> {
        if global >= self.eid_range_lo && global < self.eid_range_hi {
            return Some(global - self.eid_range_lo);
        }
        if self.is_open_ended() {
            return None;
        }
        let extra = self.extra.read().unwrap();
        let mut base = self.eid_range_hi - self.eid_range_lo;
        for &(lo, hi) in extra.iter() {
            if global >= lo && global < hi {
                return Some(base + (global - lo));
            }
            base += hi - lo;
        }
        None
    }

    /// table 内 local offset → global (local eid)。 extent の外なら `None` (= 枯渇)。
    #[inline]
    pub fn global_of(&self, local: u32) -> Option<u32> {
        let first = self.eid_range_hi.wrapping_sub(self.eid_range_lo);
        if self.is_open_ended() || local < first {
            return self.eid_range_lo.checked_add(local);
        }
        let extra = self.extra.read().unwrap();
        let mut rel = local - first;
        for &(lo, hi) in extra.iter() {
            let len = hi - lo;
            if rel < len {
                return Some(lo + rel);
            }
            rel -= len;
        }
        None
    }

    /// 起動時の anonymous table。 全 himo / entity の default 受け皿。
    /// `eid_range_hi == u32::MAX` の間は open-ended (旧 API で `entity()` が
    /// 呼べる)、 `define_table` で初めて非 anon table が切られた瞬間に
    /// 現 `next_eid` で閉じる。
    pub fn anonymous() -> Self {
        Self {
            name: String::new(),
            himo_ids: std::sync::RwLock::new(Vec::new()),
            eid_range_lo: 0,
            eid_range_hi: u32::MAX,
            fk_refs: Vec::new(),
            next_local: std::sync::atomic::AtomicU32::new(0),
            free_locals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            free_locals_nonempty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            // #141: PK は schema 層が build 後に `set_table_pk` で降ろす。
            extra: std::sync::RwLock::new(Vec::new()),
            pk_himo: None,
        }
    }

    /// 0.7.0: 「reserved table」 = engine / schema 層の internal 用 table。
    /// 命名規約: `_` で始まる名前 (例: `_schema_meta` / `_sync_ops` / `_sync_peers`)。
    /// user 視点では不可視 (= `list_user_tables` から除外、 schema crate も
    /// 公開 API では reject)。 sidecar / wire format に flag は持たず、 名前
    /// だけで判定するので 0.5.0 / 0.6.0 sidecar との forward-compat を維持。
    #[inline]
    pub fn is_reserved(&self) -> bool {
        self.name.starts_with('_')
    }
}

/// 0.7.0: reserved table 命名規約 (= `_` で始まる) を確認する helper。
/// `define_reserved_table` で公開 API に出すバリデーション、 schema crate からも
/// reject 用に呼ばれる。
#[inline]
pub fn is_reserved_table_name(name: &str) -> bool {
    name.starts_with('_')
}

/// request19: engine 自身が持つ内部 table か (= `clear_local_only_tables` の対象外)。
/// アプリが `define_reserved_table` で作った local-only table と区別する。
#[inline]
pub fn is_engine_internal_table(name: &str) -> bool {
    name == "_sync_ops" || name == "_sync_peers"
}

/// `define_table` で size_hint=0 を渡した時の default。 SNS scale (10M+) では
/// 明示指定推奨、 embedded scale (10k-1M) では default で足りる。
pub const DEFAULT_TABLE_RESERVED: u32 = 1_000_000;

pub struct Engine {
    /// #59: 「想定内だが続行不能」 な事象の種別ごとの発生回数。 panic の代替。
    faults: std::sync::Arc<[std::sync::atomic::AtomicU64; FaultKind::COUNT]>,
    /// fault warn の rate limit (unix ms)。 満杯状態では毎 write で warn が出るので絞る。
    last_fault_warn_ms: std::sync::atomic::AtomicU64,
    #[allow(dead_code)]
    path: String,
    layout: std::sync::RwLock<Layout>,
    /// v10 Phase 3: 現在の entity 上限。 `grow_entity_cap` で伸びる (reservation まで)。
    entity_cap: std::sync::atomic::AtomicU32,
    /// v10 Phase 3: table extent の追加 (`grow_table` / auto-grow) を直列化する。
    table_grow_lock: std::sync::Mutex<()>,
    max_himos: u32,
    vocab: Vocabulary,
    himo_reg: Vocabulary,
    // 0.9.0 himo dynamic definition: himo の並列配列は AppendVec (固定 capacity
    // + append-only + lock-free read) 化して `&self` から定義追加できるように
    // した (design a: pre-sized slots + atomic len publish)。 reader の hot path
    // (`self.himos[hid]` 等の indexed read) は lock を取らない。
    // 定義追加は `himo_def_lock` で直列化される。
    himo_names: AppendVec<String>,
    value_types: AppendVec<ValueType>,
    himo_max_values: AppendVec<u32>,
    himos: AppendVec<HimoStore>,
    /// v9 (request17-A): himo ごとの version column (HLC 16B/cell)。 `himos` と
    /// **同じ index** で並ぶ parallel array。 v9 領域を持たない DB (pre-v9 /
    /// v9 未有効 create) では **空のまま**で、 `cell_hlc` は常に `Hlc::ZERO`
    /// (= 版数不明) を返す (A-1 の「現状維持」)。
    ver_cols: AppendVec<Column>,
    /// v9 (request17-A5): tombstone version column。 削除 (`Delete { eid }`) は
    /// himo を持たないので eid 空間に 1 本だけ持つ。 v9 領域が無ければ None。
    tomb_col: Option<Column>,
    /// β-light step 2: engine が認知する table 一覧。 index 0 は常に
    /// anonymous table (旧 API は全部ここに dispatch)。 step 3+ で
    /// define_table 時に push される。
    tables: Vec<TableDef>,
    /// β-light step 2: himo_id → 所属 table_id の逆引き (himos と同じ index)。
    /// 旧 API (`define_himo`) で追加された himo は全部 ANONYMOUS_TABLE。
    /// 0.9.0: `&self` の define_himo_in 相当 (`ensure_himo_dynamic_in`) が
    /// attach 先を後書きできるよう要素を AtomicU16 (= TableId) にした。
    himo_to_table: AppendVec<std::sync::atomic::AtomicU16>,
    /// 0.9.0: himo 定義 (`ensure_himo_dynamic` 系) の直列化 lock。
    /// 並列配列 5 本 + tables[].himo_ids + header write を 1 定義単位で
    /// atomic に見せる。 read path はこの lock を取らない。
    himo_def_lock: std::sync::Mutex<()>,
    entities: EntitySet,
    contents: ContentStore,
    /// v6 (0.12.0, #88): Leaf 終端ノードの可変長 payload store。 vocab から剥がした
    /// reclaim 対応 store。 pre-v6 DB (leaf region 無し) は None。
    leaf: Option<LeafStore>,
    /// 非同期書き込みキュー。`create_concurrent` で有効化される。
    write_queue: Option<std::sync::Arc<crate::write_queue::WriteQueue>>,
    /// consumer スレッドへの shutdown 通知。`Drop` で true に。
    shutdown_flag: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// consumer スレッドハンドル。Drop で join。
    consumer_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
    /// writer が push した累積件数。tie_async/untie_async/delete_async で +1。
    push_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// consumer が apply 完了した累積件数。apply_count >= push_count が同期点。
    apply_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// WAL。`create_concurrent_with_oplog` or `open_concurrent_with_oplog` で有効化。
    /// Some なら tie_async/untie_async/delete_async は oplog_record_queue 経由で
    /// consumer thread 側に batch flush を委ねる。
    oplog: Option<std::sync::Arc<enchudb_oplog::oplog::OpLog>>,
    /// async path 専用の WAL record queue。 writer は WAL に直接書かず、 ここに
    /// owned record を push。 consumer thread が drain して `wal.append_many` で
    /// 1 flock サイクル N records にまとめる (per-record flock コスト償却)。
    /// request17 (A-3): record と **その版数** を対で運ぶ。 consumer は
    /// `append_many_with_hlcs` でこの HLC のまま WAL に載せる (採番し直さない)。
    oplog_record_queue: Option<std::sync::Arc<crossbeam_queue::ArrayQueue<(enchudb_oplog::oplog::OwnedOp, enchudb_oplog::Hlc)>>>,
    /// writer が `oplog_record_queue` へ push した WAL record の累積件数。
    /// op は queue 先行・record 後追い (#77-H4) なので、 apply_count barrier
    /// だけでは「op は適用済みだが record は未 append」の窓が残る。
    /// `flush_writes` はこの counter 対でも待つ (下の wal_append_count 参照)。
    wal_push_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// consumer が WAL へ append し終えた record の累積件数。
    /// wal_append_count >= wal_push_count が「queue に record が残っていない」
    /// 同期点 — これが無いと `oplog_sync` が record より先に Commit を打ち、
    /// fsync 済みのはずの write が crash で消える。
    wal_append_count: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// #77-M2: consumer thread が panic すると true。 flush_writes / Drop の
    /// barrier spin と producer の blocking push が「絶対に進まない待ち」に
    /// 陥らないための脱出フラグ。
    consumer_poisoned: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// 0.11 (request10): Ref 値が translated foreign entity を指す write を
    /// bridge から除外した際の一度きり警告フラグ (u32 wire value に世界番号が
    /// 入らないため発送不能、 wire 拡張の follow-up 待ち)。
    warned_ref_to_replica: std::sync::atomic::AtomicBool,
    /// #178 検知: 「自分が書いた行が、 後から foreign identity に束ねられた」 回数。
    ///
    /// 書き戻しの宛名付け替えは bridge 時に `eid_translator.reverse()` を引くので、
    /// **束ねられる前に書いた分は自分の eid のまま出て行く**。 相手側はそれを別 entity
    /// として払い出すので、 **PK を持たない重複行**が生える (詳細は #178)。
    /// ここでその瞬間 (= bind 時に、 その行に自分が書いた cell が既に在る) を数える。
    /// 静かに壊れる経路なので、 まず観測できるようにするのが目的。
    bind_over_local_writes: std::sync::atomic::AtomicU64,
    warned_bind_over_local_writes: std::sync::atomic::AtomicBool,
    /// 0.18.2: `_sync_ops` 満杯 backpressure の warn を 1 回に抑制（解消で解除）。
    warned_sync_ops_full: std::sync::atomic::AtomicBool,
    /// request17 (v9): ローカル write が cell の版数判定で弾かれた warn の一度きり
    /// フラグ。 構造上起きないはずの事象なので、 起きたら無音にしない。
    warned_cell_version_reject: std::sync::atomic::AtomicBool,
    /// request17 (A-3): **async write の HLC 事前採番と queue push を束ねる lock**。
    ///
    /// async 経路は「op の適用」も「WAL append」も consumer thread が後から行うため、
    /// 版数は push 側で採番して両 queue に同じ値を運ぶしかない。 その採番順と queue
    /// 投入順がずれると WAL 上で HLC が単調増加しなくなり、 **HLC 順に record を
    /// 並べ替えて配る transport** の下で依存順 (Vocab → その vid を使う Tie) が逆転する
    /// (#141 の誤 bind と同じ壊れ方)。 採番と push を 1 単位にして順序を固定する。
    ///
    /// 同期経路はこの lock を取らない (採番が `append` の直列化の内側にあるため
    /// 構造的に単調 — `append_local_op` 参照)。
    hlc_mint_lock: parking_lot::Mutex<()>,
    /// 背景 fsync が最後に completed した LSN。
    durable_lsn: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// この Engine を所有する peer の id。分散時 eid の上位 32bit。
    peer_id: std::sync::atomic::AtomicU32,
    /// LWW 用に (eid, himo) → 最後の HLC を記録。
    hlc_store: std::sync::Arc<crate::hlc_store::HlcStore>,
    /// request18: `sync_tables_enabled()` の cache。 本体は `has_reserved_table`
    /// (= table 名の線形走査) なので write hot path から呼べない。 更新点は
    /// `enable_sync_tables` と open 時の table 復元の 2 箇所だけ (sync tables は
    /// 一度有効にしたら無効化できない = 単調)。
    sync_tables_on: std::sync::atomic::AtomicBool,
    /// #9: 受信した foreign eid を自分の eid 空間の local eid に翻訳する写像。
    eid_translator: std::sync::Arc<crate::eid_translator::EidTranslator>,
    /// #166: **slot から切り離された** foreign entity の削除版数。
    ///
    /// tombstone は普段 local slot 側 (v9 の tombstone column / pre-v9 の
    /// `HlcStore`) に載るが、 slot が別の住人に再利用されると当然そこには残せない。
    /// 一方 「その foreign entity は削除済み」 という事実は **identity に属する**
    /// ので、 slot を手放しても覚えていないと、 削除より古い record が再配送された
    /// ときに新しい slot を確保して復活してしまう (#140 の再来)。
    ///
    /// そこで slot を手放す時点で `(author_peer, foreign_local) -> Hlc` に退避し、
    /// 同じ identity に新しい slot を払い出すとき (`alloc_translated_local`) に
    /// その slot へ書き戻す。 これで 「削除済み」 が slot の寿命から独立する。
    foreign_tombs: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<(enchudb_oplog::PeerId, u32), enchudb_oplog::Hlc>>>,
    /// #166: `foreign_tombs` が空かどうかの fast path flag。 slot 再利用が一度も
    /// 起きていない DB (= 常態) で、 受信 apply の hot path が read lock すら
    /// 取らないようにするためだけのもの。
    foreign_tombs_empty: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// 自 peer の ed25519 鍵ペア。None なら署名しない/検証もしない。
    keypair: std::sync::RwLock<Option<std::sync::Arc<enchudb_oplog::keys::Keypair>>>,
    /// 他 peer の pubkey TOFU ストア。Syncer が verify に使う。
    pubkeys: std::sync::Arc<enchudb_oplog::keys::PubkeyStore>,
    /// ACL(書き込み許可 peer の集合)。Syncer が enforce する。
    acl: std::sync::Arc<crate::acl::Acl>,
    /// エッジ replica 用: true なら書き込み API が panic、sync 経由 (remote_*_apply) のみ受ける。
    is_replica: std::sync::atomic::AtomicBool,
    /// CRDT mesh mode: true なら `remote_*_apply` で受信した op を **元 HLC/author/署名のまま**
    /// 自分の WAL にも `append_relayed` で記録し、 次の publish で他 peer に gossip する。
    /// relay 側の (peer, hlc) dedupe で同じ record の二度送りはカットされるためループしない。
    /// ホスト/クライアント構成 (= ホストが唯一の集約点) では false のまま使う。
    gossip_remote_apply: std::sync::atomic::AtomicBool,
    /// 大容量 blob 用 store(画像/動画/モデル等)。`set_blob_store` で注入。
    /// 未設定なら `blob_store()` は None を返す。
    blob_store: std::sync::RwLock<Option<std::sync::Arc<dyn crate::blob_store::BlobStore>>>,
    /// changefeed: WAL に durable 化した record を listener に push する。
    /// consumer スレッドが背景 fsync 完了後に発火。
    change_listeners: std::sync::Arc<std::sync::RwLock<Vec<std::sync::Arc<dyn crate::changefeed::ChangeListener>>>>,
    /// changefeed: 次に listener へ emit すべき WAL offset。
    /// add_change_listener 時に wal.head() に同期され、それ以降の commit のみが流れる。
    change_emit_offset: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// 0.7.0 Phase 4: 既に `_sync_ops` table に転送済みの WAL offset。
    /// consumer thread が背景 fsync 後に新規 commit を `_sync_ops` へ転送する。
    /// `enable_sync_tables` 有効化前は使われない (= 0)、 後は単調前進。
    sync_ops_offset: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// fold ↔ bridge の check-then-act を lock 下の再検証で弾いた回数。
    /// `wal_fold_safe()` (lock 外) が true を返した後、 `try_reset_if` の述語
    /// (lock 内) が false になった = まさにその窓を踏んだケース。 0 でないことは
    /// 「この race は実在し、 再検証が実際に record を救っている」 の証拠になる。
    fold_race_saves: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// bridge cursor が head を追い越した (= fold ↔ transfer の lost update) のを
    /// 検出して巻き戻した回数。 0 でないなら直列化が破れている。
    sync_ops_cursor_repairs: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// #217: ack prefix walk が dead row (payload 欠落 / decode 不能 = 構造的に
    /// 配送不能) を削除して越えた回数。 0 でないなら bridge が壊れた payload を
    /// 書いたことがある (要調査) — が、 ring を permanent blocker にはしない。
    sync_dead_rows_purged: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// #236: `state_records_for` が **配れないと判断して落とした cell** の累計。
    /// 判断自体はどれも正しい (doc 参照) が、 落とした事実がどこにも残らないと
    /// 「replica が系統的に不完全な state を配っている」 が観測できない。
    state_records_dropped: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// #217: ack prefix walk の再開点 (peer → 検証済み prefix 末尾 lsn)。
    /// **意図的に in-memory** — 永続の `_sync_peers.consumed_lsn` は旧実装
    /// (降順 first-match) が over-ack した値を含みうるので、 session 最初の walk は
    /// これを信用せず lsn 0 から全 ring を検証し直す (移行 heal)。 entry の有無が
    /// 「この session で検証済みか」の marker を兼ねる。
    ack_walk_resume: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<u32, u32>>>,
    /// #221: `_sync_ops` row の purge (delete + free list への slot 返却) を atomic に
    /// する専用 lock。 `Engine::delete` は「実際に消したか」を返さない (冪等) ので、
    /// lock 無しだと並行 purge (`absorb_pull_acks` は複数 peer からの並行 pull で
    /// 並行実行される) が同じ slot を free list に**二重 push** → 後続の
    /// `entity_in("_sync_ops")` が同じ eid を二回払い出し、 bridge row が上書きで
    /// 消える (silent)。 lock 内で「row がまだ生きているか」を再検証してから
    /// delete + push する。
    sync_ops_purge_lock: std::sync::Arc<std::sync::Mutex<()>>,
    /// 0.8.11 (issue: stress_10k_cycle flaky): `transfer_oplog_to_sync_ops` の
    /// 排他 lock。 0.8.0 で `concurrentize_with_oplog` の background consumer thread
    /// が自動 transfer を呼ぶようになったが、 手動 transfer との並列実行で
    /// `from = sync_ops_offset.load()` → records pull → row insert → offset.store()
    /// の 4 step が race し、 同じ records が複数回 row insert される
    /// (= reclaim 後に残骸が残る) bug。 本 mutex で transfer の全期間を排他化、
    /// 重複転送を根本解消。 lock 競合は per-fsync 頻度 (= 100ms 周期) で頻度低、
    /// hot path 影響は無視できる。
    transfer_lock: std::sync::Arc<std::sync::Mutex<()>>,
    /// 0.8.15: `try_persist_tables` 内の warning emit を rate-limit するための
    /// 最後の emit timestamp (millis since UNIX_EPOCH)。 ENOSPC 等で連続失敗
    /// すると consumer thread が毎 batch eprintln → ターミナル不能なので 1 秒
    /// 1 回に抑える。 0 = 未 emit。
    last_persist_warn_ms: std::sync::atomic::AtomicU64,
    /// 0.7.0 Phase 4: `_sync_ops` row に振る単調 lsn (= u32 の publish_since cursor)。
    /// `entity_in` の eid とは別、 reclaim で row が消えても lsn は単調維持される。
    next_sync_lsn: std::sync::Arc<std::sync::atomic::AtomicU32>,
    /// 受信した Vocab op の `(author_peer, remote_vid) → local_vid` mapping。
    /// Symbol 型 himo の Tie を受信した時に remote_vid を local_vid に変換して apply する。
    /// peer-local に保持(replica でも独立、open 時は空、受信で徐々に埋まる)。
    peer_vocab_map: std::sync::RwLock<std::collections::HashMap<(enchudb_oplog::PeerId, u32), u32>>,
    /// `peer_vocab_map` が最後の persist 以降に変化したか。 pull ごとに sidecar を
    /// 書き直さないための dirty flag (写像は単調増加なので「増えたか」で足りる)。
    peer_vocab_map_dirty: std::sync::atomic::AtomicBool,
    /// read-only モード。 true なら書き込み API は error/panic。 open_readonly で立つ。
    /// `is_replica` は「直 write 拒否、 Syncer 経由は受ける」、 こちらは「一切 write 不可」。
    is_readonly: std::sync::atomic::AtomicBool,
    /// 0.8.2: build phase の sidecar fsync 抑止。 schema crate が
    /// `Database::create → build×N` 中 true にして、 `finish_*` / Drop で
    /// false に戻して 1 度だけ explicit に `persist_tables()` を呼ぶ。
    /// macOS APFS で N table 宣言時の N×fsync (= 1 fsync 5-7ms) を 1 回に圧縮。
    defer_tables_persist: std::sync::atomic::AtomicBool,
    /// #190: sidecar persist (`.tables` / `.eidmap` / `.vocabmap`) の直列化 lock。
    /// tmp 名が sidecar ごとに固定なので、 同一 sidecar を 2 thread (consumer の
    /// `try_persist_tables` と pull 側の `persist_sync_state`) が同時に書くと
    /// truncate し合って torn install / rename ENOENT / 新旧逆転 install が起きる。
    /// state の snapshot (serialize) から rename までを丸ごとこの lock 下に置く —
    /// serialize も lock 内なので「後から取った方が必ず新しい状態を書く」が成立する。
    sidecar_persist_lock: std::sync::Mutex<()>,
    /// writer lock の保持 fd。 open_writer / create_* で `.db.lock` sidecar を
    /// flock(LOCK_EX)、 Engine drop で fd close = lock release。 readonly では None。
    /// 多 process write の同 .db 競合を防ぐ (sqlite WAL モード相当)。
    #[cfg(not(target_arch = "wasm32"))]
    _writer_lock: Option<WriterLock>,
    backing: Backing, // 最後に drop されるよう最終フィールド
}

impl Engine {
    /// 現行の単独 Engine を作る(WAL 無し、&mut self で mutation)。
    /// 元は `Engine::create` の別名だったが、現在の `Engine::create` は WAL 付き
    /// `Arc<Self>` を返すため、明示的に単独 Engine が欲しい場合はこちらを使う。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_standalone(path: &str) -> io::Result<Self> {
        Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)
    }

    /// `create` は WAL 有効 + `Arc<Self>` 返し。
    /// 旧挙動は `create_standalone` で取れる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create(path: &str) -> io::Result<std::sync::Arc<Self>> {
        Self::create_concurrent_with_oplog(path, 16 * 1024 * 1024)
    }

    /// CLI/小規模用途向け。ファイルサイズ数MB。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_compact(path: &str) -> io::Result<Self> {
        Self::create_full_with_cyl(
            path,
            65_536,                    // max_entities: 64K
            Some(16 * 1024 * 1024),    // vocab_data: 16MB
            Some(64),                  // max_himos: 64
            Some(16 * 1024 * 1024),    // content_data: 16MB
            Some(256),                 // cyl_max_values: 256
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_with_capacity(path: &str, max_entities: u32) -> io::Result<Self> {
        Self::create_with_options(path, max_entities, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_with_options(path: &str, max_entities: u32, vocab_data_size: Option<usize>) -> io::Result<Self> {
        Self::create_full(path, max_entities, vocab_data_size, None, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_full(path: &str, max_entities: u32, vocab_data_size: Option<usize>, max_himos: Option<u32>, content_data_size: Option<usize>) -> io::Result<Self> {
        Self::create_full_with_cyl(path, max_entities, vocab_data_size, max_himos, content_data_size, None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_full_with_cyl(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        max_himos: Option<u32>,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
    ) -> io::Result<Self> {
        Self::create_full_with_leaf(
            path, max_entities, vocab_data_size, max_himos, content_data_size,
            cyl_max_values, None,
        )
    }

    /// `create_full_with_cyl` の leaf region size を明示できる版。
    /// `leaf_data_size = Some(0)` は leaf region 無し = v5 相当 DB を作る
    /// (#88 migration test / bench の before 生成)。 None は default 予約。
    /// leaf scale は default (`Gb16`)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_full_with_leaf(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        max_himos: Option<u32>,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
        leaf_data_size: Option<usize>,
    ) -> io::Result<Self> {
        Self::create_full_with_leaf_scale(
            path, max_entities, vocab_data_size, max_himos, content_data_size,
            cyl_max_values, leaf_data_size, None,
        )
    }

    /// #90: LeafStore の scale (`LeafScale::Gb16`/`Gb32`/`Gb64` = 16/32/64GB cap) を
    /// 明示する版。 None は default (`Gb16`)。 leaf offset は word 単位で持つので、
    /// scale を上げても列幅は増えず slot alignment (padding) だけ粗くなる。
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    pub fn create_full_with_leaf_scale(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        max_himos: Option<u32>,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
        leaf_data_size: Option<usize>,
        leaf_scale: Option<LeafScale>,
    ) -> io::Result<Self> {
        Self::create_full_with_leaf_scale_v9(
            path, max_entities, vocab_data_size, max_himos, content_data_size,
            cyl_max_values, leaf_data_size, leaf_scale,
            // request18: v9 領域は **sync に参加する DB だけ**が持つ。 create 時点では
            // まだ sync tables が無いので確保しない。 `enable_sync_tables()` が
            // 領域を生やし (`add_v9_regions_for_sync`)、 次の open で version column が
            // 生える。 それまでの版数は揮発 `HlcStore` に置かれる (A-1 の現状維持)。
            //
            // 0.19.0/0.20.0 は無条件に確保していたため、 sync しない DB が
            // apparent ×3.6 (既定 capacity で 26.5 GB → 95.5 GB) を払っていた。
            false,
        )
    }

    /// **v9 領域を持たない** DB を作る (= v8 相当の layout)。
    ///
    /// request18 で create の既定が v9 無しになったため、 これは
    /// `create_with_capacity` と同じ layout を作る (後方互換の別名として残置)。
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn create_without_cell_version(path: &str, max_entities: u32) -> io::Result<Self> {
        Self::create_full_with_leaf_scale_v9(
            path, max_entities, None, None, None, None, None, None, false,
        )
    }

    /// **v9 領域を持つ** DB を最初から作る。 sync する前提が create 時点で判っている
    /// 場合と、 v9 機構そのものの test 用。
    ///
    /// 通常の経路は `create*` → `enable_sync_tables()` で、 そちらは領域を後から
    /// 生やす (request18)。 これはその 1 ステップ版。
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn create_with_cell_version(path: &str, max_entities: u32) -> io::Result<Self> {
        Self::create_full_with_leaf_scale_v9(
            path, max_entities, None, None, None, None, None, None, true,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments)]
    fn create_full_with_leaf_scale_v9(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        max_himos: Option<u32>,
        content_data_size: Option<usize>,
        cyl_max_values: Option<u32>,
        leaf_data_size: Option<usize>,
        leaf_scale: Option<LeafScale>,
        cell_version: bool,
    ) -> io::Result<Self> {
        let off_shift = leaf_scale.map(|s| s.off_shift()).unwrap_or(DEFAULT_LEAF_OFF_SHIFT);
        let vds = vocab_data_size.unwrap_or(DEFAULT_VOCAB_DATA_SIZE);
        let max_himos = max_himos.unwrap_or(DEFAULT_MAX_HIMOS);
        let layout = Layout::compute(max_entities, max_himos, vds, content_data_size, cyl_max_values, leaf_data_size, None, cell_version, None).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // #90: 予約 leaf region が選んだ scale の cap を超えないか検証。
        let leaf_cap = cap_bytes_for_shift(off_shift);
        if layout.leaf_data_size as u64 > leaf_cap {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "leaf_data_size {} exceeds scale cap {} (off_shift {}) — 大きい LeafScale を選べ",
                    layout.leaf_data_size, leaf_cap, off_shift,
                ),
            ));
        }

        Self::create_segments(path, layout, max_entities, max_himos, off_shift)
    }

    /// v10 (request21): directory + segment file 群として DB を新規作成する。 旧 eager /
    /// growable の 2 経路はここに合流した (区別が消えた: 全 segment が書いた分だけ伸びる、
    /// 見かけも物理も)。 store の init は `Backing::region(kind)` から `Region` を受け取る
    /// だけで、 旧 layout offset には触らない。
    #[cfg(not(target_arch = "wasm32"))]
    fn create_segments(
        path: &str,
        layout: Layout,
        max_entities: u32,
        max_himos: u32,
        off_shift: u32,
    ) -> io::Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        // H11: 既存 DB を silent に破壊しない。 directory 作成 (atomic) → lock → segment。
        create_db_dir(path)?;
        // writer lock を先に取る (= 他 writer が居れば block)。 create も書き込みなので必須。
        let writer_lock = acquire_writer_lock(path)?;
        let set = SegmentSet::create(
            std::path::Path::new(path),
            &layout,
            layout.leaf_data_size > 0,
            layout.has_cell_version(),
        )?;
        let backing = Backing::Segments(set);
        {
            let mmap = backing.header_mut(layout.header_size);
        mmap[H_MAGIC..H_MAGIC + 4].copy_from_slice(&FILE_MAGIC);
            mmap[H_VERSION..H_VERSION + 4].copy_from_slice(&FILE_VERSION.to_le_bytes());
            mmap[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].copy_from_slice(&max_entities.to_le_bytes());
            mmap[H_RESERVE_ENTITIES..H_RESERVE_ENTITIES + 4].copy_from_slice(&layout.reserve_entities.to_le_bytes());
            mmap[H_MAX_HIMOS..H_MAX_HIMOS + 4].copy_from_slice(&max_himos.to_le_bytes());
            mmap[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&0u32.to_le_bytes());
            mmap[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4].copy_from_slice(&layout.vocab_max_entries.to_le_bytes());
            mmap[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4].copy_from_slice(&layout.vocab_index_cap.to_le_bytes());
            mmap[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8].copy_from_slice(&(layout.vocab_data_size as u64).to_le_bytes());
            mmap[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4].copy_from_slice(&layout.himoreg_max_entries.to_le_bytes());
            mmap[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4].copy_from_slice(&layout.himoreg_index_cap.to_le_bytes());
            mmap[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8].copy_from_slice(&(layout.himoreg_data_size as u64).to_le_bytes());
            mmap[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8].copy_from_slice(&(layout.content_data_size as u64).to_le_bytes());
            mmap[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].copy_from_slice(&layout.cyl_max_values.to_le_bytes());
            mmap[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8].copy_from_slice(&(layout.leaf_data_size as u64).to_le_bytes());
            // v9 (request17): per-cell version 領域の有無。 open 側はこの flag だけを見る。
            mmap[H_CELL_VERSION..H_CELL_VERSION + 4]
                .copy_from_slice(&(layout.has_cell_version() as u32).to_le_bytes());

            // ヘッダ整合性 CRC
            write_header_crc(mmap);
        }
        backing.flush_header(layout.header_size)?;

        let entities = EntitySet::init(backing.region(SegmentKind::Entities, &layout), max_entities, layout.reserve_entities);
        let vocab = Vocabulary::init(
            backing.region(SegmentKind::VocabData, &layout),
            backing.region(SegmentKind::VocabOffsets, &layout),
            backing.region(SegmentKind::VocabIndex, &layout),
            layout.vocab_max_entries, layout.vocab_index_cap,
        );
        let himo_reg = Vocabulary::init(
            backing.region(SegmentKind::HimoregData, &layout),
            backing.region(SegmentKind::HimoregOffsets, &layout),
            backing.region(SegmentKind::HimoregIndex, &layout),
            layout.himoreg_max_entries, layout.himoreg_index_cap,
        );
        let contents = ContentStore::init(
            backing.region(SegmentKind::ContentIndex, &layout),
            backing.region(SegmentKind::ContentData, &layout),
        );
        // leaf_data_size == 0 (= v5 相当 create) は leaf segment 無し。
        let leaf = if layout.leaf_data_size > 0 {
            Some(LeafStore::init(backing.region(SegmentKind::LeafData, &layout), off_shift))
        } else {
            None
        };
        // v9 (request17-A5): tombstone column は himo に依らないので create 時に確保。
        // himo ごとの version column は define_himo_slot_locked で himo と同時に作る。
        let tomb_col = if layout.has_cell_version() {
            Some(ver_column_from_region(backing.region(SegmentKind::Tomb, &layout), max_entities))
        } else {
            None
        };

        Ok(Self {
            path: path.to_string(), layout: std::sync::RwLock::new(layout), entity_cap: std::sync::atomic::AtomicU32::new(max_entities),
            table_grow_lock: std::sync::Mutex::new(()), max_himos,
            vocab, himo_reg,
            himo_names: AppendVec::with_capacity(max_himos as usize),
            value_types: AppendVec::with_capacity(max_himos as usize),
            himo_max_values: AppendVec::with_capacity(max_himos as usize),
            himos: AppendVec::with_capacity(max_himos as usize),
            ver_cols: AppendVec::with_capacity(max_himos as usize),
            tomb_col,
            entities, contents, leaf,
            tables: vec![TableDef::anonymous()],
            himo_to_table: AppendVec::with_capacity(max_himos as usize),
            himo_def_lock: std::sync::Mutex::new(()),
            write_queue: None,
            shutdown_flag: None,
            consumer_handle: std::sync::Mutex::new(None),
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_append_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            oplog: None,
            oplog_record_queue: None,
            consumer_poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            warned_ref_to_replica: std::sync::atomic::AtomicBool::new(false),
            bind_over_local_writes: std::sync::atomic::AtomicU64::new(0),
            warned_bind_over_local_writes: std::sync::atomic::AtomicBool::new(false),
            warned_sync_ops_full: std::sync::atomic::AtomicBool::new(false),
            warned_cell_version_reject: std::sync::atomic::AtomicBool::new(false),
            hlc_mint_lock: parking_lot::Mutex::new(()),
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            sync_tables_on: std::sync::atomic::AtomicBool::new(false),
            eid_translator: std::sync::Arc::new(crate::eid_translator::EidTranslator::new()),
            foreign_tombs: std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            foreign_tombs_empty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            keypair: std::sync::RwLock::new(None),
            pubkeys: std::sync::Arc::new(enchudb_oplog::keys::PubkeyStore::new()),
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            gossip_remote_apply: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            sync_ops_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            fold_race_saves: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            sync_ops_cursor_repairs: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            sync_dead_rows_purged: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            state_records_dropped: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            ack_walk_resume: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            sync_ops_purge_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            next_sync_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(1)),
            transfer_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            last_persist_warn_ms: std::sync::atomic::AtomicU64::new(0),
            faults: std::sync::Arc::new(std::array::from_fn(|_| {
                std::sync::atomic::AtomicU64::new(0)
            })),
            last_fault_warn_ms: std::sync::atomic::AtomicU64::new(0),
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            peer_vocab_map_dirty: std::sync::atomic::AtomicBool::new(false),
            is_readonly: std::sync::atomic::AtomicBool::new(false),
            defer_tables_persist: std::sync::atomic::AtomicBool::new(false),
            sidecar_persist_lock: std::sync::Mutex::new(()),
            _writer_lock: Some(writer_lock),
            backing,
        })
    }

    /// growable backing で新規 DB を作る。 通常の `create_full_with_cyl`
    /// が `set_len(layout.total_size)` で sparse な巨大ファイル (88 GB
    /// など) を作るのに対し、 こちらは `GrowableMap` で **virtual address
    /// だけ予約 + 必要分だけ commit** する経路。 fresh DB が page サイズ
    /// から始まり書き込みに応じて拡張する。
    ///
    /// 現状の実装は init 時点で layout 全体まで pre-commit する保守的な
    /// 形 (= ファイルサイズは旧来の `create_*` と同等)。 各 store の write
    /// 境界に `grow_amortized` を仕込むことで段階的に initial commit を
    /// 縮める予定。 まずは Backing 経路の互換性確認を優先。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable(path: &str) -> io::Result<Self> {
        Self::create_growable_with_capacity(path, DEFAULT_MAX_ENTITIES)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_with_capacity(path: &str, max_entities: u32) -> io::Result<Self> {
        let max_himos = DEFAULT_MAX_HIMOS;
        let layout = Layout::compute(max_entities, max_himos, DEFAULT_VOCAB_DATA_SIZE, None, None, None, None, false, None).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Self::create_growable_full(path, layout, max_entities, max_himos, DEFAULT_LEAF_OFF_SHIFT)
    }

    /// `create_growable_with_capacity` の **v9 領域あり**版 (request18)。
    /// 通常の経路は create → `enable_sync_tables()` で後から生やす。 これはその
    /// 1 ステップ版で、 v9 機構そのものの test / sync 前提が最初から判っている場合用。
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn create_growable_with_cell_version(path: &str, max_entities: u32) -> io::Result<Self> {
        let max_himos = DEFAULT_MAX_HIMOS;
        let layout = Layout::compute(max_entities, max_himos, DEFAULT_VOCAB_DATA_SIZE, None, None, None, None, true, None).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Self::create_growable_full(path, layout, max_entities, max_himos, DEFAULT_LEAF_OFF_SHIFT)
    }

    /// growable backing で開く。`max_entities` と `vocab_data_size` を明示。
    ///
    /// 大規模 Leaf text を持つアプリ (議事録 / 論文 / 全文 archive) で default
    /// 512 MiB の vocab cap に当たる場合に使う。`Leaf` 列も `vocab.insert`
    /// 経由で vocab data に積まれるため、`Tag` 数だけでなく本文の総バイト数
    /// が cap を決める。目安: 1 KB / row × 1 M rows ≒ 1 GiB を見込む。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_with_options(
        path: &str,
        max_entities: u32,
        vocab_data_size: usize,
    ) -> io::Result<Self> {
        let max_himos = DEFAULT_MAX_HIMOS;
        let layout = Layout::compute(max_entities, max_himos, vocab_data_size, None, None, None, None, false, None).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Self::create_growable_full(path, layout, max_entities, max_himos, DEFAULT_LEAF_OFF_SHIFT)
    }

    /// #90: growable backing で、 leaf region の予約 size と scale (16/32/64GB) を
    /// 明示する。 wikipulse のような大 payload × 巨大 live working set 用途で、
    /// leaf region を 4GB 超に伸ばす場合に使う。 `leaf_data_size` は選んだ scale の
    /// cap 以下であること。 leaf offset は word 単位なので列幅は不変。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_with_leaf(
        path: &str,
        max_entities: u32,
        vocab_data_size: Option<usize>,
        leaf_data_size: Option<usize>,
        leaf_scale: LeafScale,
    ) -> io::Result<Self> {
        let max_himos = DEFAULT_MAX_HIMOS;
        let vds = vocab_data_size.unwrap_or(DEFAULT_VOCAB_DATA_SIZE);
        let layout = Layout::compute(max_entities, max_himos, vds, None, None, leaf_data_size, None, false, None).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let leaf_cap = leaf_scale.cap_bytes();
        if layout.leaf_data_size as u64 > leaf_cap {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "leaf_data_size {} exceeds scale cap {} ({:?}) — 大きい LeafScale を選べ",
                    layout.leaf_data_size, leaf_cap, leaf_scale,
                ),
            ));
        }
        Self::create_growable_full(path, layout, max_entities, max_himos, leaf_scale.off_shift())
    }

    /// #118: `GrowableOptions` から growable DB を作る一本化 API。 全 layout knob
    /// (max_entities / max_himos / vocab / content / cyl / leaf / leaf_scale) を露出する。
    /// 個別 `create_growable_with_*` は本メソッドの部分特化に相当 (後方互換で残置)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_opts(path: &str, opts: GrowableOptions) -> io::Result<Self> {
        let layout = Layout::compute(
            opts.max_entities,
            opts.max_himos,
            opts.vocab_data_size,
            opts.content_data_size,
            Some(opts.cyl_max_values),
            opts.leaf_data_size,
            opts.vocab_max_entries, // #122
            // request18: v9 領域は enable_sync_tables() が生やす (create では確保しない)
            false,
            opts.reserve_entities,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let leaf_cap = opts.leaf_scale.cap_bytes();
        if layout.leaf_data_size as u64 > leaf_cap {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "leaf_data_size {} exceeds leaf_scale cap {} ({:?}) — 大きい LeafScale を選べ",
                    layout.leaf_data_size, leaf_cap, opts.leaf_scale,
                ),
            ));
        }
        Self::create_growable_full(
            path,
            layout,
            opts.max_entities,
            opts.max_himos,
            opts.leaf_scale.off_shift(),
        )
    }

    /// Tiny growable preset for app state-logs (matcha-style: a few
    /// hundred rows of dismissed-key / seen-at / etc.). Default
    /// `create_growable` uses gigascale capacities so the layout
    /// total — and thus the on-disk apparent size of a fresh DB —
    /// is hundreds of MB even with growable backing. Apps that just
    /// need a key/value store with timestamps want something much
    /// smaller. This caps:
    /// - 1024 entities
    /// - 16 himos
    /// - 64 KB vocab / himoreg / content data each
    /// → layout total ≈ 250 KB, fresh DB ~ same on disk.
    ///
    /// Hard limit of 1024 rows is fine for most state-log use cases;
    /// callers that may exceed it should use `create_growable` or
    /// `create_growable_with_capacity` with the right size.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_growable_tiny(path: &str) -> io::Result<Self> {
        let max_entities = 1024_u32;
        let max_himos = 16_u32;
        // tiny preset では vocab_max_entries の default ×16 multiplier
        // が過剰 (16384 entries → vocab_offsets 128KB + index 213KB)。
        // matcha のような数百エントリ用途では ×2 で十分 — 2048 entries
        // で offsets 16KB + index ~26KB に収まる。
        let vocab_max_entries = max_entities.saturating_mul(2);
        let layout = Layout::compute_with_caps(
            max_entities,
            max_himos,
            vocab_max_entries,
            64 * 1024,        // vocab_data: 64 KB
            Some(64 * 1024),  // content_data: 64 KB
            Some(64),         // cyl_max_values: small per-himo cylinders
            None,             // leaf_data: default
            false,            // v9 (request17): 未有効化
            None,             // reserve_entities: 既定
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Self::create_growable_full(path, layout, max_entities, max_himos, DEFAULT_LEAF_OFF_SHIFT)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn create_growable_full(
        path: &str,
        layout: Layout,
        max_entities: u32,
        max_himos: u32,
        leaf_off_shift: u32,
    ) -> io::Result<Self> {
        // v10: growable と eager の区別は消えた (全 segment が書いた分だけ伸びる)。
        Self::create_segments(path, layout, max_entities, max_himos, leaf_off_shift)
    }

    /// 現行の単独 Engine を開く(WAL 無し、&mut self で mutation)。
    /// 元は `Engine::open` の別名だったが、現在の `Engine::open` は WAL 付き
    /// `Arc<Self>` を返すため、明示的に単独 Engine が欲しい場合はこちらを使う。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_standalone(path: &str) -> io::Result<Self> {
        Self::open_internal(path, /*verify_region_crc=*/ true, /*take_lock=*/ true, /*readonly=*/ false)
    }

    /// read-only open: writer lock を取らず、 書き込み API は error。
    /// 複数 process で同時に呼んで OK (writer と共存可能、 reader 同士も無制限)。
    /// 用途: GUI の表示専用 process、 監視ツール、 backup-reader 等。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_readonly(path: &str) -> io::Result<Self> {
        let eng = Self::open_internal(path, /*verify_region_crc=*/ true, /*take_lock=*/ false, /*readonly=*/ true)?;
        eng.is_readonly.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// `open` は WAL 有効 + `Arc<Self>` 返し。
    /// 旧挙動は `open_standalone` で取れる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &str) -> io::Result<std::sync::Arc<Self>> {
        Self::open_concurrent_with_oplog(path, 16 * 1024 * 1024)
    }

    /// 内部用: region CRC 検証を skip できる open。WAL ルート用。
    /// `take_lock = true` で `.db.lock` の flock(LOCK_EX) を取得し、 Engine 寿命中保持。
    #[cfg(not(target_arch = "wasm32"))]
    fn open_internal(path: &str, verify_region_crc: bool, take_lock: bool, readonly: bool) -> io::Result<Self> {
        // open path: writer lock を mmap 前に取る (= 他 writer 居れば block)
        Self::check_db_dir(path)?;
        let writer_lock = if take_lock {
            Some(acquire_writer_lock(path)?)
        } else {
            None
        };
        // v10: header.seg を先に読んで検証 → Layout と himo_count → segment 群を open。
        // 書き込み mapping で開く (readonly engine も旧来 MmapMut と同じく RW map;
        // 「書かない」 は engine 側の契約)。
        let (layout, himo_count) = Self::read_header_layout(path)?;
        let set = SegmentSet::open(
            std::path::Path::new(path),
            &layout,
            himo_count,
            layout.leaf_data_size > 0,
            layout.has_cell_version(),
            false,
        )?;
        let mut eng = Self::load_from_backing(Backing::Segments(set), readonly)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        eng.path = path.to_string();
        eng._writer_lock = writer_lock;

        // 0.8.15 (issue #52): open 前に残骸 `.tables.tmp` を掃除する self-heal。
        // persist 失敗 (ENOSPC 等) で `.tmp` が残ったケースを次回 reopen で確実に
        // clean できるよう、 next persist の truncate(true) に依存しない明示削除。
        cleanup_tables_tmp(path);

        // β-light step 7: tables sidecar の読み込み。 不在なら anonymous fallback
        // (= load_from_backing が既に行った形のまま) で v4 DB 互換。
        // 0.8.15 (issue #52): InvalidData (= 破損) は **fail-readable** で扱う。
        // sidecar を `.tables.corrupt-<unix_ts>` に rename して退避し、 anonymous
        // tables のまま続行する。 schema crate 側は engine の `list_user_tables`
        // から再合成できるので、 sidecar 破損で全 DB が unreadable になる失敗
        // モードを避ける。
        match load_tables_from_sidecar(path) {
            Ok(Some(persisted)) => eng.adopt_persisted_tables(persisted),
            Ok(None) => {} // 不在: 新規 DB or v4 legacy
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                rename_corrupt_sidecar(path, "tables", &e);
            }
            Err(e) => {
                eprintln!(
                    "warning: failed to read tables sidecar (anonymous fallback): {}",
                    e
                );
            }
        }
        // request18: table 定義が確定したので hot path 用 cache を張り直す。
        eng.refresh_sync_tables_flag();

        // v10: sync tables を持つ DB は版数列 (`ver/*.seg` / `tomb.seg`) を持つのが不変条件。
        // `enable_sync_tables()` は segment を作ってから header flag を立てるので、 その間の
        // crash や旧 binary で有効化した DB は flag 無しで残る。 writer open で回収する
        // (segment file が既にあれば open するだけ、 無ければ作る)。 旧 in-place migration
        // (`migrate_v8_to_v9_in_place`) の後継。
        if !readonly && eng.sync_tables_enabled() && !eng.has_cell_version() {
            if let Err(e) = eng.add_v9_regions_for_sync() {
                eprintln!(
                    "warning: failed to add cell-version segments on open (versions stay in-memory): {}",
                    e
                );
            }
        }

        // #9: eid 翻訳テーブルの sidecar 読み込み。 不在 (= sync してない / 旧 DB) なら
        // 空の translator で続行 (additive、 後方互換)。 破損は警告のみで空続行 (= 再
        // sync で mapping は張り直せる)。
        match load_eidmap_from_sidecar(path) {
            Ok(Some(entries)) => {
                for (peer, foreign_local, local, tomb) in entries {
                    // #166: 写像を持たない削除記録 (slot を手放した identity)。
                    // 写像も slot も復元せず、 退避表にだけ載せる。 次にこの
                    // identity へ slot を払い出すとき書き戻される。
                    if local == NO_LOCAL_SLOT {
                        eng.remember_foreign_tombstone(peer, foreign_local, tomb);
                        continue;
                    }
                    eng.eid_translator.insert(peer, foreign_local, local);
                    // #9 (H4): `.eidmap` と `.tables` は別々の rename なので crash で
                    // 不整合になりうる。 mapped local を table の next_local が必ず追い
                    // 越すよう前進させ、 stale `.tables` でも mapped slot を再 alloc して
                    // 衝突する事態を防ぐ (= 最悪でも「重複」に留め「衝突」を起こさない)。
                    Self::advance_table_next_local_for(&eng.tables, local);
                    // #9 (C): foreign Delete tombstone を復元する。 これが無いと
                    // reopen で tombstone が消え、 Delete より古い Tie が (Delete 抜きで) 再配送
                    // された時に削除済み entity が復活する。 ZERO は未削除なので skip。
                    //
                    // request17 step 6: 復元先も `set_tombstone_local` に一本化
                    // (v9 なら tombstone column、 pre-v9 なら HlcStore)。 v9 では既に
                    // 本体側に載っているので monotone-max の no-op になる。
                    if tomb != enchudb_oplog::Hlc::ZERO {
                        eng.set_tombstone_local(local, tomb);
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!(
                    "warning: failed to read eidmap sidecar (empty translator): {}",
                    e
                );
            }
        }

        // text 写像 (`(author_peer, remote_vid) → local_vid`) の sidecar 読み込み。
        //
        // これが無いと、 受信済み `Vocab` を消費した cursor だけが残り、 後続の
        // `Tie` を翻訳できなくなる (旧実装は生の remote vid をそのまま cell に書き、
        // **受信側の無関係な文字列**を指していた)。 `.eidmap` と同じく、 不在なら
        // 空で続行し、 破損は警告のみ (再 sync で張り直せる)。
        match load_vocabmap_from_sidecar(path) {
            Ok(Some(entries)) => {
                let mut map = eng.peer_vocab_map.write().unwrap();
                for (peer, remote_vid, local_vid) in entries {
                    map.insert((peer, remote_vid), local_vid);
                }
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!(
                    "warning: failed to read vocabmap sidecar (empty vocab translation): {}",
                    e
                );
            }
        }

        // #117: sidecar の per-table next_local が未永続 / stale でも、 body に msync
        // 永続された live bitmap を ground truth に next_local を自己修復する。
        // adopt / eidmap で確定した各 table 範囲に対し「max live local + 1」を保証し、
        // 「reopen で next_local 巻き戻り → 生きた eid 再払出 (silent 破壊)」を経路
        // 非依存で塞ぐ (schema Database + raw define + finish_* 無し drop でも安全)。
        // oplog recover 済 entity は apply_oplog_op が別途 advance するので二重でも
        // monotone (max) なため問題ない。
        eng.reconcile_next_local_from_bitmap();

        // crash が delete を途中で切った跡 (tombstone は durable、 本体は残存) を
        // 埋める。 readonly は共有 mmap を書かない契約なので対象外。
        if !readonly {
            eng.finish_interrupted_deletes();
        }

        if verify_region_crc {
            // .crc ファイルがあれば全 region CRC 検証
            eng.verify_region_crcs()?;
        }
        Ok(eng)
    }

    /// 全 region の CRC テーブルを計算する。
    ///
    /// issue7 fix: vocab/himoreg 領域の header には index clean flag (offset
    /// 12..16) があり、 open 時に必ず flip される (clean=true → false)。 この 4
    /// バイトを CRC 計算から除外することで、 seal_integrity で焼いた `.crc` が
    /// 次回 open でも変わらず検証 OK になる。 clean flag の値は msync 直後に
    /// 永続化されるので、 CRC 検証範囲から外しても DB データの整合性検証は
    /// 損なわない (clean flag 自体は rebuild の hint であって data ではない)。
    /// `kind` の region の **commit 済み部分** を `&[u8]` で。 予約全域 (zero page) を
    /// 舐めないための CRC / dump 用。
    fn region_committed_bytes(&self, kind: SegmentKind) -> &[u8] {
        let r = self.backing.region(kind, &*self.layout.read().unwrap());
        let n = r.committed_len();
        // SAFETY: mapping は self.backing が所有し self より長生きする。
        unsafe { std::slice::from_raw_parts(r.slice().as_ptr(), n) }
    }

    fn compute_region_crc_table(&self) -> crate::integrity::CrcTable {
        use crate::integrity::{CrcTable, RegionKind, fnv1a_region, fnv1a_slices};
        let file_size = self.layout.read().unwrap().total_size as u64;
        let mut table = CrcTable::new(self.max_himos, file_size);
        for hid in 0..self.value_types.len() {
            let b = self.region_committed_bytes(SegmentKind::Himo(hid as u32));
            table.set(RegionKind::HimoColumn(hid as u32), fnv1a_region(b));
        }
        let b = self.region_committed_bytes(SegmentKind::VocabData);
        table.set(RegionKind::Vocab, fnv1a_slices(&[&b[..12], &b[16..]]));
        let b = self.region_committed_bytes(SegmentKind::HimoregData);
        table.set(RegionKind::HimoReg, fnv1a_slices(&[&b[..12], &b[16..]]));
        let b = self.region_committed_bytes(SegmentKind::ContentData);
        table.set(RegionKind::Content, fnv1a_region(b));
        let b = self.region_committed_bytes(SegmentKind::Entities);
        table.set(RegionKind::EntitySet, fnv1a_region(b));
        table
    }

    /// open 時の region CRC 検証。`.crc` ファイルがなければスキップ。
    #[cfg(not(target_arch = "wasm32"))]
    fn verify_region_crcs(&self) -> io::Result<()> {
        use crate::integrity::{CrcTable, crc_path_for};
        if self.path.is_empty() { return Ok(()); } // from_bytes 等でパス無い場合スキップ
        let crc_path = crc_path_for(&self.path);
        let stored = match CrcTable::load(&crc_path)? {
            Some(t) => t,
            None => return Ok(()), // `.crc` sidecar の無い DB 互換
        };
        let expected = self.compute_region_crc_table();
        let mismatches = stored.diff(&expected);
        if !mismatches.is_empty() {
            let names: Vec<String> = mismatches.iter().map(|k| k.name()).collect();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("region CRC mismatch in: {} — file may be corrupt", names.join(", ")),
            ));
        }
        Ok(())
    }

    /// 現在の DB state から region CRC を再計算 → `.crc` ファイルに書き出す。
    /// flush() から呼ばれる。
    #[cfg(not(target_arch = "wasm32"))]
    fn persist_region_crcs(&self) -> io::Result<()> {
        if self.path.is_empty() { return Ok(()); }
        let table = self.compute_region_crc_table();
        let crc_path = crate::integrity::crc_path_for(&self.path);
        table.save(&crc_path)?;
        Ok(())
    }

    /// v10: directory の `header.seg` を読んで検証し、 `Layout` と himo_count を返す
    /// (mmap する前)。 旧 1 ファイル DB (regular file) は明示的に弾いて Phase 2 の
    /// `migrate_v9_to_v10` へ誘導する。
    #[cfg(not(target_arch = "wasm32"))]
    /// v10: `path` が DB directory として存在するか。 1 ファイル (v9 以前) なら migrate
    /// への誘導、 無ければ `NotFound`。 writer lock を取る前に呼ぶ (無い path に lock file
    /// だけ作らない)。
    fn check_db_dir(path: &str) -> io::Result<()> {
        let p = std::path::Path::new(path);
        if p.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "\"{path}\" is a single-file (v9 or older) EnchuDB database; this build reads the v10 \
                     directory format. migrate it first (Engine::migrate_v9_to_v10)"
                ),
            ));
        }
        if !p.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("database directory not found: \"{path}\""),
            ));
        }
        Ok(())
    }

    /// open せずに `path` が EnchuDB として何であるかを判定する。
    ///
    /// v10 は DB が directory なので、 「create の途中で落ちた半端な directory」 も
    /// `Path::exists()` は true を返す。 consumer が 「新規作成すべき」 と 「移行すべき」 と
    /// 「壊れている」 を取り違えないための入口。 mmap も lock も取らない (stat だけ)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn probe(path: impl AsRef<std::path::Path>) -> DbState {
        let p = path.as_ref();
        if p.is_file() {
            return DbState::SingleFileLegacy;
        }
        if !p.is_dir() {
            return DbState::Missing;
        }
        // header.seg が無い = create が header を書く前に落ちた。
        if !p.join(crate::segments::SegmentKind::Header.rel_path()).is_file() {
            return DbState::Incomplete;
        }
        // header が読めなければ、 directory の形はしているが DB ではない。
        let (layout, himo_count) = match Self::read_header_layout(path.as_ref().to_str().unwrap_or("")) {
            Ok(v) => v,
            Err(e) => return DbState::Damaged(format!("header: {e}")),
        };
        // header が指す segment が揃っているか (mmap しない)。 manifest があるのに
        // segment が欠けている = 後から消された (Damaged)、 manifest ごと無い = create の
        // 途中 (Incomplete)。
        let missing = crate::segments::missing_segments(
            p,
            &layout,
            himo_count,
            layout.leaf_data_size > 0,
            layout.has_cell_version(),
        );
        let has_manifest = p.join(crate::db_files::SEGMENTS).is_file();
        if !missing.is_empty() {
            let why = format!("missing segment(s): {}", missing.join(", "));
            return if has_manifest { DbState::Damaged(why) } else { DbState::Incomplete };
        }
        if !has_manifest {
            // 完成しているが manifest が無い = flush を 1 度も通っていない (create 直後に
            // 落ちた) か、 manifest を持たない古い build が作った DB。 開けはするので
            // Ready とは言わず、 呼び出し側に判断を渡す。
            return DbState::Incomplete;
        }
        match crate::segments::verify_manifest(p) {
            Ok(_) => DbState::Ready,
            Err(e) => DbState::Damaged(e.to_string()),
        }
    }

    /// `probe(path) == Ready` の糖衣。 「開ける DB がそこにあるか」 だけ聞きたい時に。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn exists(path: impl AsRef<std::path::Path>) -> bool {
        matches!(Self::probe(path), DbState::Ready)
    }

    fn read_header_layout(path: &str) -> io::Result<(Layout, u32)> {
        Self::check_db_dir(path)?;
        let p = std::path::Path::new(path);
        let buf = SegmentSet::read_header(p, HEADER_SIZE)?;
        Self::parse_header(&buf, false).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// header bytes → `(Layout, himo_count)`。 magic / CRC / version / field の整合を検証する。
    /// `allow_legacy_packed` は `from_bytes` / `unpack_to_dir` (packed 1 blob) 用: v8 / v9 の
    /// 1 ファイル layout は packed v10 と byte 互換なので受け入れる (v8 は版数列を持たず、
    /// `H_CELL_VERSION` は予約 0)。 directory open は v10 のみ。
    fn parse_header(buf: &[u8], allow_legacy_packed: bool) -> Result<(Layout, u32), String> {
        if buf.len() < HEADER_SIZE || buf[H_MAGIC..H_MAGIC + 4] != FILE_MAGIC {
            return Err("not an EnchuDB file".into());
        }
        let version = u32::from_le_bytes(buf[H_VERSION..H_VERSION + 4].try_into().unwrap());
        let legacy_packed = allow_legacy_packed
            && (FILE_VERSION_LEGACY_V8..=FILE_VERSION_LEGACY_V9).contains(&version);
        if version != FILE_VERSION && !legacy_packed {
            return Err(format!(
                "unsupported EnchuDB file version {} (this build reads v{}; v8 / v9 single-file \
                 databases must be migrated with Engine::migrate_v9_to_v10, older ones are not supported)",
                version, FILE_VERSION,
            ));
        }
        verify_header_crc(buf)?;
        let max_entities = u32::from_le_bytes(buf[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].try_into().unwrap());
        let max_himos = u32::from_le_bytes(buf[H_MAX_HIMOS..H_MAX_HIMOS + 4].try_into().unwrap());
        let himo_count = u32::from_le_bytes(buf[H_HIMO_COUNT..H_HIMO_COUNT + 4].try_into().unwrap());
        let vocab_max_entries = u32::from_le_bytes(buf[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4].try_into().unwrap());
        let vocab_index_cap = u32::from_le_bytes(buf[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4].try_into().unwrap());
        let vocab_data_size = u64::from_le_bytes(buf[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let himoreg_max_entries = u32::from_le_bytes(buf[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4].try_into().unwrap());
        let himoreg_index_cap = u32::from_le_bytes(buf[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4].try_into().unwrap());
        let himoreg_data_size = u64::from_le_bytes(buf[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let content_data_size = u64::from_le_bytes(buf[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let cyl_max_values = u32::from_le_bytes(buf[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].try_into().unwrap());
        sanity_check_header_fields(
            max_himos, himo_count,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size,
        )?;
        let leaf_data_size = u64::from_le_bytes(buf[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let cell_version = version >= FILE_VERSION_LEGACY_V9
            && u32::from_le_bytes(buf[H_CELL_VERSION..H_CELL_VERSION + 4].try_into().unwrap()) != 0;
        // v8 / v9 の header は固定 4096 (himo 表は溢れ得た = #246)。 v10 は可変長。
        let header_size = if version < FILE_VERSION { HEADER_SIZE } else { header_size_for(max_himos) };
        // v10 Phase 3: reservation。 legacy / 未設定 (0) は max_entities と同値 (= 伸ばせない)。
        let reserve_entities = if version < FILE_VERSION {
            max_entities
        } else {
            u32::from_le_bytes(buf[H_RESERVE_ENTITIES..H_RESERVE_ENTITIES + 4].try_into().unwrap()).max(max_entities)
        };
        let layout = Layout::try_from_params_with_header(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
            cell_version,
            header_size,
            reserve_entities,
        )?;
        Ok((layout, himo_count))
    }

    /// v10: sync tables を有効化した DB に版数列 (`ver/*.seg`) と tombstone 列 (`tomb.seg`)
    /// を **その場で** 生やす。 旧 B-lite (file を伸ばして flag だけ、 column は次の open
    /// から = #243 の窓) は segment 化で不要になった: 版数列は末尾ではなく独立 file なので
    /// mmap を張り替えずに追加できる。 有効化直後から `has_cell_version()` は true。
    ///
    /// packed (`Memory`) backing は永続しないので flag に意味が無く、 no-op。
    #[cfg(not(target_arch = "wasm32"))]
    fn add_v9_regions_for_sync(&mut self) -> io::Result<()> {
        if self.layout.read().unwrap().has_cell_version() {
            return Ok(()); // 既に v9 (明示 create / 有効化済み)
        }
        if self.backing.memory_len().is_some() {
            return Ok(());
        }
        let guard = self.layout.read().unwrap();
        let l = &*guard;
        let v9 = Layout::try_from_params(
            self.max_entities(), self.max_himos,
            l.vocab_max_entries, l.vocab_index_cap, l.vocab_data_size,
            l.himoreg_max_entries, l.himoreg_index_cap, l.himoreg_data_size,
            l.content_data_size, l.leaf_data_size, l.cyl_max_values,
            true,
            l.reserve_entities,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        drop(guard);

        // 順序: segment file を作る → column を組む → header flag。 逆にすると flag だけ
        // 立った DB が crash で残り、 次の open が無い segment を探して失敗する。
        self.backing.ensure_tomb(&v9)?;
        for hid in 0..self.himos.len() {
            self.backing.ensure_ver(hid as u32, &v9)?;
        }
        *self.layout.write().unwrap() = v9;
        for hid in 0..self.himos.len() {
            let ver = ver_column_from_region(
                self.backing.region(SegmentKind::Ver(hid as u32), &*self.layout.read().unwrap()),
                self.max_entities(),
            );
            let _ = self.ver_cols.push(ver);
        }
        self.tomb_col = Some(ver_column_from_region(
            self.backing.region(SegmentKind::Tomb, &*self.layout.read().unwrap()),
            self.max_entities(),
        ));

        let buf = self.backing.header_mut(HEADER_SIZE);
        buf[H_CELL_VERSION..H_CELL_VERSION + 4].copy_from_slice(&1u32.to_le_bytes());
        write_header_crc(buf);
        self.backing.flush_header(HEADER_SIZE)?;
        Ok(())
    }

    /// Vec<u8> からエンジンを構築。WASM ではこれが唯一のエントリポイント。
    /// native でも使える（テスト、ファイル丸読みなど）。
    pub fn from_bytes(data: Vec<u8>) -> Result<Self, String> {
        Self::load_from_backing(Backing::Memory(data), /*readonly=*/ false)
    }

    /// #88 (0.12.0): v5 DB (leaf region 無し = `Leaf` 値が vocab 辞書に単調
    /// append される) の bytes を v6 (`LeafStore` あり = reclaim 対応) へ移送する
    /// 純関数版 (file I/O なし)。 返り値は `(v6 bytes, stats)`。
    ///
    /// 手順: 末尾に `leaf_data_size` の LeafStore region を新設し、 各 `Leaf`
    /// himo の live cell が持つ旧 vocab vid を辿って vocab bytes を LeafStore へ
    /// `insert`、 cell を leaf offset に書換える。 vocab / entity / himo 構造・
    /// content は byte 単位でそのまま引き継ぐ。
    ///
    /// - `leaf_data_size`: 新設 region の予約サイズ (`1..=u32::MAX`)。
    /// - `skip_himos`: vocab に据え置く himo id。 file 経路は reopen で `.tables`
    ///   が復元され reserved-table の Leaf が `leaf_for()==None` に戻るため、 その
    ///   himo を渡して移送から除外し read 整合を保つ。 Memory 経路 (reopen が
    ///   全 anonymous) は空でよい。
    ///
    /// 既知の trade-off: 旧 vocab の Leaf bytes は orphan として残る
    /// (`stats.vocab_orphan_bytes_left`)。 vocab compaction は本 migration の
    /// 対象外で、 目的は「以後の Leaf 書込みを reclaim 対象にする」こと。
    pub fn migrate_bytes_v5_to_v6(
        src: Vec<u8>,
        leaf_data_size: usize,
        skip_himos: &[u16],
    ) -> Result<(Vec<u8>, MigrationStats), String> {
        if src.len() < HEADER_SIZE || src[H_MAGIC..H_MAGIC + 4] != FILE_MAGIC {
            return Err("not an EnchuDB file".into());
        }
        let existing_leaf = u64::from_le_bytes(
            src[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8].try_into().unwrap(),
        ) as usize;
        if existing_leaf > 0 {
            // 既に leaf region あり (v6) — 移送不要。 bytes はそのまま返す。
            return Ok((src, MigrationStats { already_v6: true, ..Default::default() }));
        }
        if leaf_data_size == 0 || leaf_data_size > u32::MAX as usize {
            return Err(format!("leaf_data_size {} out of range (1..=u32::MAX)", leaf_data_size));
        }

        // header fields (load_from_backing と同じ読み出し順)。
        let max_entities = u32::from_le_bytes(src[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].try_into().unwrap());
        let max_himos = u32::from_le_bytes(src[H_MAX_HIMOS..H_MAX_HIMOS + 4].try_into().unwrap());
        let himo_count = u32::from_le_bytes(src[H_HIMO_COUNT..H_HIMO_COUNT + 4].try_into().unwrap());
        let vocab_max_entries = u32::from_le_bytes(src[H_VOCAB_MAX_ENTRIES..H_VOCAB_MAX_ENTRIES + 4].try_into().unwrap());
        let vocab_index_cap = u32::from_le_bytes(src[H_VOCAB_INDEX_CAP..H_VOCAB_INDEX_CAP + 4].try_into().unwrap());
        let vocab_data_size = u64::from_le_bytes(src[H_VOCAB_DATA_SIZE..H_VOCAB_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let himoreg_max_entries = u32::from_le_bytes(src[H_HIMOREG_MAX_ENTRIES..H_HIMOREG_MAX_ENTRIES + 4].try_into().unwrap());
        let himoreg_index_cap = u32::from_le_bytes(src[H_HIMOREG_INDEX_CAP..H_HIMOREG_INDEX_CAP + 4].try_into().unwrap());
        let himoreg_data_size = u64::from_le_bytes(src[H_HIMOREG_DATA_SIZE..H_HIMOREG_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let content_data_size = u64::from_le_bytes(src[H_CONTENT_DATA_SIZE..H_CONTENT_DATA_SIZE + 8].try_into().unwrap()) as usize;
        let cyl_max_values = u32::from_le_bytes(src[H_CYL_MAX_VALUES..H_CYL_MAX_VALUES + 4].try_into().unwrap());

        let layout = Layout::try_from_params(
            max_entities, max_himos,
            vocab_max_entries, vocab_index_cap, vocab_data_size,
            himoreg_max_entries, himoreg_index_cap, himoreg_data_size,
            content_data_size, leaf_data_size, cyl_max_values,
            false, // v9 (request17): 未有効化。 header から読むのは有効化と同時
            max_entities,
        )?;

        // src が v5 layout 全域 (= leaf_data_off までのバイト列) をカバーしているか。
        // leaf region は tail 追加なので v6 の leaf_data_off == v5 total_size。
        if src.len() < layout.leaf_data_off {
            return Err(format!(
                "source too small: {} bytes (expected >= {}) — truncated?",
                src.len(), layout.leaf_data_off,
            ));
        }

        // dst = v6 layout 全域。 leaf_data_off までを src からコピー、 leaf region は
        // 0 埋め (LeafStore::init が MAGIC + high_water を書く)。
        let mut dst = vec![0u8; layout.total_size];
        dst[..layout.leaf_data_off].copy_from_slice(&src[..layout.leaf_data_off]);

        // header: version=6 (CRC 範囲内)、 leaf region size (CRC 範囲外)、 CRC 再計算。
        // v5→v6 migration は byte-offset の v6 を出力 (leaf off_shift=0)。 v7 engine は
        // read-through で開ける。 16GB 超が要るなら別途 v7 で create し直す。
        dst[H_VERSION..H_VERSION + 4].copy_from_slice(&FILE_VERSION_LEGACY_V6.to_le_bytes());
        dst[H_LEAF_DATA_SIZE..H_LEAF_DATA_SIZE + 8]
            .copy_from_slice(&(layout.leaf_data_size as u64).to_le_bytes());
        // v9 (request17): 出力は v6 layout なので per-cell version 領域は無い。
        // src 由来の flag が残ると、 open 側が「有る」前提の layout を組んで
        // `backing too small` になる (src が v9 DB の場合)。
        dst[H_CELL_VERSION..H_CELL_VERSION + 4].copy_from_slice(&0u32.to_le_bytes());
        write_header_crc(&mut dst);

        let type_bytes: Vec<u8> = (0..himo_count as usize).map(|hid| dst[H_HIMO_TYPES + hid]).collect();
        let skip: std::collections::HashSet<u16> = skip_himos.iter().copied().collect();

        // dst 上に region を張って cell を移送する (Region::new は非所有 view なので
        // dst は move 可能なまま)。 vocab は get のみ (readonly=true)、 leaf は新規 init。
        let base = dst.as_mut_ptr();
        let vocab = Vocabulary::load(
            unsafe { Region::new(base.add(layout.vocab_data_off), layout.vocab_data_size) },
            unsafe { Region::new(base.add(layout.vocab_offsets_off), layout.vocab_offsets_size) },
            unsafe { Region::new(base.add(layout.vocab_index_off), layout.vocab_index_size) },
            /*readonly=*/ true,
        );
        // v6 出力なので leaf offset は byte (off_shift=0)。
        let leaf = LeafStore::init(unsafe {
            Region::new(base.add(layout.leaf_data_off), layout.leaf_data_size)
        }, 0);

        let mut stats = MigrationStats::default();
        for (hid, &tb) in type_bytes.iter().enumerate() {
            if ValueType::from_byte(tb) != ValueType::Leaf { continue; }
            if skip.contains(&(hid as u16)) { continue; }
            stats.leaf_himos += 1;
            let col = Column::load(unsafe {
                Region::new(base.add(layout.himo_col_off(hid)), layout.himo_col_size)
            });
            let count = col.count();
            for eid in 0..count {
                // cell = stored 形式 (0 = 未設定、 N = 旧 vocab vid N-1)。
                let stored = u32::from_le_bytes(col.get(eid).try_into().unwrap());
                if stored == 0 { continue; }
                let old_vid = stored - 1;
                let new_off = {
                    let bytes = vocab.get(old_vid);
                    stats.bytes_moved += bytes.len() as u64;
                    leaf.insert(bytes)
                };
                if new_off == u32::MAX {
                    // #167: 移送先を確保できない (commit を伸ばせない)。 cell は旧
                    // offset を指したままにして、 この cell の移送は諦める。
                    continue;
                }
                col.set(eid, &(new_off + 1).to_le_bytes());
                stats.cells_moved += 1;
            }
        }
        stats.leaf_footprint = leaf.high_water();
        stats.vocab_orphan_bytes_left = stats.bytes_moved;

        Ok((dst, stats))
    }

    fn load_from_backing(backing: Backing, readonly: bool) -> Result<Self, String> {
        let hdr: &[u8] = backing.header_mut(HEADER_SIZE);
        let allow_legacy_packed = backing.memory_len().is_some();
        let (layout, himo_count) = Self::parse_header(hdr, allow_legacy_packed)?;
        if let Some(n) = backing.memory_len() {
            if n < layout.total_size {
                return Err(format!(
                    "backing too small: {} bytes (layout.total_size = {}) — truncated file?",
                    n, layout.total_size,
                ));
            }
        }
        let max_entities = u32::from_le_bytes(hdr[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].try_into().unwrap());
        let max_himos = u32::from_le_bytes(hdr[H_MAX_HIMOS..H_MAX_HIMOS + 4].try_into().unwrap());
        let cyl_max_values = layout.cyl_max_values;
        // himo 型 / max_values 表は固定 field の後ろ (max_himos 依存の可変長) にある。
        let hdr: &[u8] = backing.header_mut(layout.header_size);

        let maxv_base = himo_maxv_base(max_himos);
        let mut type_bytes = Vec::with_capacity(himo_count as usize);
        let mut maxv_values = Vec::with_capacity(himo_count as usize);
        for hid in 0..himo_count as usize {
            type_bytes.push(hdr[H_HIMO_TYPES + hid]);
            let mv_off = maxv_base + hid * 4;
            maxv_values.push(u32::from_le_bytes(hdr[mv_off..mv_off + 4].try_into().unwrap()));
        }

        // ── page-reclaim instrumentation (issue2 調査) ──
        // ENCHU_OPEN_PROFILE=1 で env 有効。 解析後、 削除する一時的計装。
        let profile = std::env::var("ENCHU_OPEN_PROFILE").is_ok();
        let pr = || -> u64 {
            #[cfg(target_os = "macos")]
            unsafe {
                let mut ru: libc::rusage = std::mem::zeroed();
                if libc::getrusage(libc::RUSAGE_SELF, &mut ru) == 0 {
                    return ru.ru_minflt as u64;
                }
                0
            }
            #[cfg(not(target_os = "macos"))]
            { 0 }
        };
        let mut t = Instant::now();
        let mut p = pr();
        let report = |label: &str, t: &mut Instant, p: &mut u64| {
            if profile {
                let np = pr();
                let dp = np - *p;
                eprintln!("[open_profile] {:>22}  Δreclaim={:>7}  Δt={:>5} ms", label, dp, t.elapsed().as_millis());
                *p = np;
                *t = Instant::now();
            }
        };

        let entities = EntitySet::load(
            backing.region(SegmentKind::Entities, &layout),
            max_entities,
        )
        .map_err(|e| e.to_string())?;
        report("EntitySet::load", &mut t, &mut p);
        let vocab = Vocabulary::load(
            backing.region(SegmentKind::VocabData, &layout),
            backing.region(SegmentKind::VocabOffsets, &layout),
            backing.region(SegmentKind::VocabIndex, &layout),
            readonly, // #77-H1: readonly は共有 index を書き換えず shadow へ rebuild
        );
        report("Vocabulary::load", &mut t, &mut p);
        let himo_reg = Vocabulary::load(
            backing.region(SegmentKind::HimoregData, &layout),
            backing.region(SegmentKind::HimoregOffsets, &layout),
            backing.region(SegmentKind::HimoregIndex, &layout),
            readonly,
        );
        report("himo_reg(Vocabulary)", &mut t, &mut p);
        let contents = ContentStore::load(
            backing.region(SegmentKind::ContentIndex, &layout),
            backing.region(SegmentKind::ContentData, &layout),
        );
        report("ContentStore::load", &mut t, &mut p);
        let leaf = if layout.leaf_data_size > 0 {
            Some(
                LeafStore::load(backing.region(SegmentKind::LeafData, &layout))
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };

        // 0.9.0: capacity は max_himos だが、 header の himo_count が万一それを
        // 超えていても load 自体は落とさない (旧 Vec 実装と同じ寛容さ)。
        let himo_cap = (max_himos as usize).max(himo_count as usize);
        let himo_names: AppendVec<String> = AppendVec::with_capacity(himo_cap);
        let value_types: AppendVec<ValueType> = AppendVec::with_capacity(himo_cap);
        let himo_max_values: AppendVec<u32> = AppendVec::with_capacity(himo_cap);
        let himos: AppendVec<HimoStore> = AppendVec::with_capacity(himo_cap);
        // v9 (request17-A): version column は himo と 1:1。 v9 領域が無い DB
        // (pre-v9 / 未有効 create) では空のままにする (= 全 cell 版数不明)。
        let ver_cols: AppendVec<Column> = AppendVec::with_capacity(himo_cap);
        let has_cell_version = layout.has_cell_version();

        for hid in 0..himo_count as usize {
            let ht = ValueType::from_byte(type_bytes[hid]);
            let mv = maxv_values[hid];
            let name_bytes = himo_reg.get(hid as u32);
            let name = String::from_utf8_lossy(name_bytes).to_string();
            let effective_mv = mv.min(cyl_max_values);

            let hs = HimoStore::load(
                backing.region(SegmentKind::Himo(hid as u32), &layout),
                ht, effective_mv,
            );
            if has_cell_version {
                let _ = ver_cols.push(ver_column_from_region(
                    backing.region(SegmentKind::Ver(hid as u32), &layout),
                    max_entities,
                ));
            }

            let _ = himo_names.push(name);
            let _ = value_types.push(ht);
            let _ = himo_max_values.push(mv);
            let _ = himos.push(hs);
        }
        let tomb_col = if has_cell_version {
            Some(ver_column_from_region(
                backing.region(SegmentKind::Tomb, &layout),
                max_entities,
            ))
        } else {
            None
        };
        report("HimoStore::load × N", &mut t, &mut p);

        // β-light step 2: load 時は全 himo を anonymous table に attach する。
        // step 3+ で v5 DB の table descriptor 読み出しに置き換える、 v4 DB は
        // 引き続きこの compat 経路で anonymous-only として open される。
        let initial_tables = vec![{
            let anon = TableDef::anonymous();
            *anon.himo_ids.write().unwrap() = (0..himos.len() as u32).collect();
            anon
        }];
        let initial_himo_to_table: AppendVec<std::sync::atomic::AtomicU16> =
            AppendVec::with_capacity(himo_cap);
        for _ in 0..himos.len() {
            let _ = initial_himo_to_table.push(std::sync::atomic::AtomicU16::new(ANONYMOUS_TABLE));
        }

        let eng = Self {
            path: String::new(), layout: std::sync::RwLock::new(layout), entity_cap: std::sync::atomic::AtomicU32::new(max_entities),
            table_grow_lock: std::sync::Mutex::new(()), max_himos,
            vocab, himo_reg,
            himo_names, value_types, himo_max_values,
            himos, ver_cols, tomb_col, entities, contents,
            leaf,
            tables: initial_tables,
            himo_to_table: initial_himo_to_table,
            himo_def_lock: std::sync::Mutex::new(()),
            write_queue: None,
            shutdown_flag: None,
            consumer_handle: std::sync::Mutex::new(None),
            push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            apply_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_push_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            wal_append_count: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            oplog: None,
            oplog_record_queue: None,
            consumer_poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            warned_ref_to_replica: std::sync::atomic::AtomicBool::new(false),
            bind_over_local_writes: std::sync::atomic::AtomicU64::new(0),
            warned_bind_over_local_writes: std::sync::atomic::AtomicBool::new(false),
            warned_sync_ops_full: std::sync::atomic::AtomicBool::new(false),
            warned_cell_version_reject: std::sync::atomic::AtomicBool::new(false),
            hlc_mint_lock: parking_lot::Mutex::new(()),
            durable_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peer_id: std::sync::atomic::AtomicU32::new(0),
            hlc_store: std::sync::Arc::new(crate::hlc_store::HlcStore::new()),
            sync_tables_on: std::sync::atomic::AtomicBool::new(false),
            eid_translator: std::sync::Arc::new(crate::eid_translator::EidTranslator::new()),
            foreign_tombs: std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            foreign_tombs_empty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            keypair: std::sync::RwLock::new(None),
            pubkeys: std::sync::Arc::new(enchudb_oplog::keys::PubkeyStore::new()),
            acl: std::sync::Arc::new(crate::acl::Acl::new()),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            gossip_remote_apply: std::sync::atomic::AtomicBool::new(false),
            blob_store: std::sync::RwLock::new(None),
            change_listeners: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            change_emit_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            sync_ops_offset: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
            )),
            fold_race_saves: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            sync_ops_cursor_repairs: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            sync_dead_rows_purged: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            state_records_dropped: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            ack_walk_resume: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            sync_ops_purge_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            last_persist_warn_ms: std::sync::atomic::AtomicU64::new(0),
            faults: std::sync::Arc::new(std::array::from_fn(|_| {
                std::sync::atomic::AtomicU64::new(0)
            })),
            last_fault_warn_ms: std::sync::atomic::AtomicU64::new(0),
            next_sync_lsn: std::sync::Arc::new(std::sync::atomic::AtomicU32::new(1)),
            transfer_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
            peer_vocab_map: std::sync::RwLock::new(std::collections::HashMap::new()),
            peer_vocab_map_dirty: std::sync::atomic::AtomicBool::new(false),
            is_readonly: std::sync::atomic::AtomicBool::new(false),
            defer_tables_persist: std::sync::atomic::AtomicBool::new(false),
            sidecar_persist_lock: std::sync::Mutex::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            _writer_lock: None, // caller (open_internal) が後から差し替える
            backing,
        };

        // header から peer_id を復元
        {
            let hdr = eng.backing.header_mut(HEADER_SIZE);
            let peer = u32::from_le_bytes(hdr[H_PEER_ID..H_PEER_ID + 4].try_into().unwrap());
            eng.peer_id.store(peer, std::sync::atomic::Ordering::Release);
        }

        eng.rebuild();

        // clean flag を 0 に倒し、 即 msync で永続化する。 こうしないと、 この後
        // insert で index 書き換え → crash → 次 open で flag=1 のまま skip → 不整合、
        // という穴が空く。 該当 page (vocab/himo_reg の data header) だけ msync する
        // ことで、 default 25 GB layout でも 1 ms 以下で済む。
        //
        // #56: readonly open は後続 write が無いため flip 自体が不要。 かつ flip +
        // msync は file を物理的に書き換えて DB を dirty 化し、 次回 open で full
        // index rebuild を誘発する (= read-only のはずが DB を太らせる)。 readonly
        // では clean flag を一切触らない (真に非破壊 open)。
        if !readonly {
            eng.vocab.mark_index_clean(false);
            eng.himo_reg.mark_index_clean(false);
            // v6 (#88): routed-Leaf の live cell offset から LeafStore free-list を
            // 再構成 (free-list は非永続)。 これが無いと dead slot が再利用されず
            // footprint が増える。
            eng.rebuild_leaf_free_list();
            #[cfg(not(target_arch = "wasm32"))]
            {
                let _ = eng.backing.flush_kind(SegmentKind::VocabData, 0, 16);
                let _ = eng.backing.flush_kind(SegmentKind::HimoregData, 0, 16);
            }
        }

        Ok(eng)
    }

    // ──── エッジ向け read-only replica ────

    /// 既存 DB を read-only replica として開く。
    /// 書き込み API (tie / untie / delete / content / entity 等) は panic する。
    /// Syncer 経由 (remote_*_apply) での書き込みのみ受け付ける。
    /// エッジ node はこちらで起動する。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_replica(path: &str) -> io::Result<Self> {
        let eng = Self::open_standalone(path)?;
        eng.is_replica.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// WAL + 並行 write queue 付きで replica open。通常の Engine と同じく Arc 共有可能。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_concurrent_replica(path: &str, oplog_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::open_concurrent_with_oplog(path, oplog_capacity)?;
        eng.is_replica.store(true, std::sync::atomic::Ordering::Release);
        Ok(eng)
    }

    /// replica モードの動的切替。true にすると書き込み API が panic する。
    /// false に戻せば通常 DB として書き込み可能に復帰する。
    pub fn set_replica_mode(&self, on: bool) {
        self.is_replica.store(on, std::sync::atomic::Ordering::Release);
    }

    /// 現在 replica モードか。
    pub fn is_replica(&self) -> bool {
        self.is_replica.load(std::sync::atomic::Ordering::Acquire)
    }

    /// 0.7.0: 「writable か」 を panic せず bool で返す。 schema crate の
    /// load_schema 等、 readonly でも続行したい path で使う。
    #[inline]
    pub fn is_readonly(&self) -> bool {
        self.is_readonly.load(std::sync::atomic::Ordering::Acquire)
    }

    /// 書き込み API が呼ばれた時の guard。replica なら panic、 readonly でも panic。
    #[inline(always)]
    fn check_writable(&self) {
        if self.is_readonly.load(std::sync::atomic::Ordering::Acquire) {
            panic!("Engine is opened read-only (open_readonly); writes are not allowed");
        }
        if self.is_replica.load(std::sync::atomic::Ordering::Acquire) {
            panic!("Engine is in replica mode; writes must go through Syncer (remote_*_apply)");
        }
    }

    // ──── table (β-light step 3) ────

    /// 新規 table を定義し、 eid 範囲を予約する。 `size_hint=0` なら
    /// `DEFAULT_TABLE_RESERVED` を採用。
    ///
    /// 振る舞い:
    ///   - 初回の `define_table` 呼び出し時に anonymous table が現 `next_eid`
    ///     で close される。 以降 `entity()` (anonymous 用) は panic する。
    ///   - 新 table の eid 範囲は `[max(全 table の eid_range_hi), +size_hint)`。
    ///   - 範囲が `max_entities` を超えるなら error。
    ///
    /// 戻り値は確保された TableId。 step 4+ で himo を namespacing して attach
    /// する経路と組み合わせて使う。 step 3 段階では table の column 列は空。
    /// 0.7.0 (Phase 3): sync 経路の reserved table を一括定義する。
    ///
    /// - `_sync_ops`: op stream (per-row HLC ordered)、 watermark + reclaim で
    ///   未配送 tail のみが残る。 hlc / peer_id / op_type / eid / himo_id /
    ///   value / signature / pubkey_fp の 8 himo を持つ
    /// - `_sync_peers`: peer ごとの consumed_hlc (= watermark) と last_seen_at
    ///
    /// idempotent (既に存在すれば何もしない)。 Syncer attach 時 / schema crate
    /// `enable_sync` 経由で呼ばれる想定。 sync 不要な単独 DB は呼ばなくて OK
    /// (= reserved table を持たない、 eid 空間も浪費しない)。
    ///
    /// 0.7.0 では opt-in 方針。 一度有効化すると無効化は不可 (= reserved table
    /// は close できない、 ただし himo は最小)。
    pub fn enable_sync_tables(&mut self) -> Result<(), String> {
        self.check_writable();

        // _sync_ops: 未配送 op の tail。 1 row = 1 oplog record、 metadata は
        // 数値 himo (= u32 limit) で query 可、 完全 wire bytes は payload (Leaf)
        // に保存する。 hlc は (wall, logical, peer) で 16 byte なので Number 1 個
        // には収まらない → query 用は lsn (= u32 単調) で代替、 完全な
        // hlc は payload の中に居る。 publish_since の cursor は lsn ベース。
        //
        // size_hint: 平常時は ack-driven reclaim で数 K rows 程度に保たれる想定。
        // 0.7.0 では lazy purge (= 古い row を delete のみ、 eid 空間は再利用せず)、
        // 0.8.0 で ring buffer 化を検討。 max_entities が小さい (= tiny preset)
        // 環境でも overflow しないよう remaining の 1/2 を `_sync_ops` に、
        // 1/16 を `_sync_peers` に割り当てる (= 最大は 1 M / 1 K で cap)。
        let remaining = self.remaining_eid_space();
        let sync_ops_size = (remaining / 2).min(1_048_576).max(64);
        let sync_peers_size = (remaining / 16).min(1024).max(8);
        if !self.has_reserved_table("_sync_ops") {
            self.define_reserved_table("_sync_ops", sync_ops_size)?;
            // lsn: u32 単調 (= publish_since cursor)
            self.define_himo_in("_sync_ops", "lsn", ValueType::Number, 0)?;
            // peer_id: record の author_peer (= filter 用)
            self.define_himo_in("_sync_ops", "peer_id", ValueType::Number, 0)?;
            // op_type: 0=Tie, 1=Untie, 2=Delete, 3=Content, 4=Commit, 5=Schema, 6=Vocab
            self.define_himo_in("_sync_ops", "op_type", ValueType::Number, 0)?;
            // hlc_wall_lo: hlc.wall (u64 ms-since-epoch) の下位 32bit
            // 完全な hlc は payload の中、 これは粗 filter / debug 用。
            self.define_himo_in("_sync_ops", "hlc_wall_lo", ValueType::Number, 0)?;
            // payload: 完全な oplog record wire bytes (header 含む)、 Leaf で dedupe なし
            self.define_himo_in("_sync_ops", "payload", ValueType::Leaf, 0)?;
        }

        // _sync_peers: peer ごと watermark。 100 peer 想定で 1 K の枠 (上で計算済み)。
        if !self.has_reserved_table("_sync_peers") {
            self.define_reserved_table("_sync_peers", sync_peers_size)?;
            // peer_id: PK
            self.define_himo_in("_sync_peers", "peer_id", ValueType::Number, 0)?;
            // consumed_lsn: 当該 peer が ack した最後の _sync_ops.lsn
            self.define_himo_in("_sync_peers", "consumed_lsn", ValueType::Number, 0)?;
            // last_seen_at: 最後の活動時刻 (= 観測用、 unix ms / 2^16 等で u32 に収める想定)
            self.define_himo_in("_sync_peers", "last_seen_at", ValueType::Number, 0)?;
        }

        // #77-H6: 既存 DB の再 enable (reopen 経路) では、 body に残る過去 rows /
        // watermark から lsn 採番を復元する。
        self.rehydrate_next_sync_lsn();

        // request18: ここが 「この DB は sync に参加する」 と確定する唯一の点。
        // v9 領域 (per-cell version column + tombstone column) はこれ以降に意味を
        // 持つので、 ここで file 上に生やす。 失敗しても enable 自体は成功扱いに
        // する — 版数は `HlcStore` fallback で従来どおり動き、 次の writer open の
        // auto-migration が回収する。
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(e) = self.add_v9_regions_for_sync() {
            eprintln!(
                "[enchudb] warning: failed to add v9 cell-version regions on enable_sync_tables \
                 (versions stay in-memory until the next open): {}",
                e
            );
        }
        self.refresh_sync_tables_flag();

        Ok(())
    }

    /// #77-H6: `next_sync_lsn` を既存 `_sync_ops` rows と `_sync_peers.consumed_lsn`
    /// から復元する。 全 constructor が 1 固定で初期化するため、 reopen 後にこれを
    /// 呼ばないと lsn 採番が 1 に戻り、 peer の ack 済み watermark より小さい lsn
    /// が発行されて `pending_sync_ops(since)` が永久に空 = **再起動後の全 write が
    /// 既存 peer に配信されない** silent loss になっていた。
    fn rehydrate_next_sync_lsn(&self) {
        use std::sync::atomic::Ordering;
        if !self.sync_tables_enabled() { return; }
        let mut max_lsn = 0u32;
        if let Some(lsn_hid) = self.himo_id("_sync_ops.lsn") {
            let hid = lsn_hid as u16;
            for eid in self.entities_with_himo(hid) {
                if let Some(l) = self.get_by_id(eid, hid) {
                    if l > max_lsn { max_lsn = l; }
                }
            }
        }
        if let Some(cons_hid) = self.himo_id("_sync_peers.consumed_lsn") {
            let hid = cons_hid as u16;
            for eid in self.entities_with_himo(hid) {
                if let Some(l) = self.get_by_id(eid, hid) {
                    if l > max_lsn { max_lsn = l; }
                }
            }
        }
        if max_lsn > 0 {
            // 既に進んでいる場合は後退させない (enable → open の二重呼び等)
            let cur = self.next_sync_lsn.load(Ordering::Acquire);
            if max_lsn.saturating_add(1) > cur {
                self.next_sync_lsn.store(max_lsn + 1, Ordering::Release);
            }
        }
    }

    /// 0.7.0 (Phase 3): sync tables が有効化済みか。 schema crate / Syncer が
    /// 「`_sync_ops` に insert すべきか」 判断する fast path。
    #[inline]
    pub fn sync_tables_enabled(&self) -> bool {
        self.has_reserved_table("_sync_ops") && self.has_reserved_table("_sync_peers")
    }

    /// request18: `sync_tables_enabled()` の cache 版。 中身は
    /// `has_reserved_table` の線形走査なので、 **write hot path からはこちらを
    /// 使う**こと。 cache は `refresh_sync_tables_flag` で更新する。
    #[inline]
    pub(crate) fn sync_tables_on(&self) -> bool {
        self.sync_tables_on.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// request18: `sync_tables_on` cache を実体から引き直す。 呼ぶのは
    /// `enable_sync_tables` の末尾と、 open で table 定義を復元し終えた直後。
    pub(crate) fn refresh_sync_tables_flag(&self) {
        self.sync_tables_on
            .store(self.sync_tables_enabled(), std::sync::atomic::Ordering::Relaxed);
    }

    /// 0.7.0 (Phase 4): oplog の `sync_ops_offset` 以降の record を `_sync_ops`
    /// table へ転送する。 consumer thread が背景 fsync 後に呼ぶ。
    ///
    /// 返り値: 転送した record 数。 enable_sync 未呼出 / oplog 未有効化なら 0。
    pub fn transfer_oplog_to_sync_ops(&self) -> usize {
        use std::sync::atomic::Ordering;
        if !self.sync_tables_enabled() { return 0; }
        let Some(wal) = self.oplog.as_ref() else { return 0; };

        // 0.8.11: background consumer thread と手動呼び出しの race で
        // 同じ records が重複転送される bug fix。 from load → records pull →
        // row insert → offset store の 4 step を排他化。 per-fsync 頻度 (= 100ms
        // 周期) なので lock 競合の hot path 影響なし。
        let _guard = self.transfer_lock.lock().unwrap();

        let from = self.sync_ops_offset.load(Ordering::Acquire);
        let trace = Self::trace_bridge_enabled();
        // #77-H4: cursor は「読み切った commit 済み group の終端」までしか
        // 進めない。 scan 後の wal.head() 再読は、 書き込み途中 record での
        // break 位置〜head 間の record を恒久 skip していた (#63 と同 class)。
        // #152: record ごとの終端 offset 付きで読む。 満杯で打ち切ったとき、
        // 「処理し切った最後の record の終端」まで cursor を進める (partial advance)
        // ため。 これが無いと group 全体を retry することになり、 backlog が ring 容量を
        // 超えると先頭 K 件を永久に再挿入し続けて進行しない。
        let (records, committed_end) = wal.iter_committed_from_with_offsets(from);
        if trace {
            eprintln!(
                "[bridge] scan from={from} committed_end={committed_end} head={} cp={} records={}",
                wal.head(),
                wal.checkpoint(),
                records.len(),
            );
        }
        if records.is_empty() {
            // 空 commit group だけ読み進んだ場合も cursor は安全に前進できる
            self.advance_sync_ops_cursor(from, committed_end);
            return 0;
        }

        // himo_id を 1 度 lookup (= hot path での文字列引きを避ける)
        let lsn_hid = match self.himo_id("_sync_ops.lsn") { Some(h) => h as u16, None => return 0 };
        // #59: 直前の lsn は None を graceful に扱っているのに、 ここだけ unwrap で
        // panic していた (非対称)。 sync tables が部分定義な DB で host を殺す。
        let (Some(peer_id_hid), Some(op_type_hid), Some(hlc_wall_lo_hid), Some(payload_hid)) = (
            self.himo_id("_sync_ops.peer_id"),
            self.himo_id("_sync_ops.op_type"),
            self.himo_id("_sync_ops.hlc_wall_lo"),
            self.himo_id("_sync_ops.payload"),
        ) else {
            return 0;
        };
        let (peer_id_hid, op_type_hid, hlc_wall_lo_hid, payload_hid) = (
            peer_id_hid as u16,
            op_type_hid as u16,
            hlc_wall_lo_hid as u16,
            payload_hid as u16,
        );

        // 0.8.11: 自己再帰 sync の循環を断つ filter。
        // `_sync_ops` / `_sync_peers` 配下の write (= ack_sync の watermark
        // update、 transfer 自体の row insert) を queue に積むと、 reclaim 後も
        // lsn が新しくて消えない残骸として蓄積、 stress_10k_cycle の
        // `final_pending < 100` 期待を壊す (= 実測 ~25%)。 これらは local-only
        // state で他 peer に sync 不要なので、 transfer 対象から除外。
        // request19: 除外対象は `_sync_ops` / `_sync_peers` 決め打ちではなく
        // **reserved table (= `_` 始まり) 全部**。 アプリが `define_reserved_table` で
        // 作った table も同じ扱いになる = 「WAL / commit の耐久性は使うが peer には
        // 配らない」 local-only table として使える。 判定は名前だけなので sidecar の
        // format 変更は無く、 reopen を跨いでそのまま効く。
        let local_only_ranges: Vec<(u32, u32)> = self
            .tables
            .iter()
            .filter(|t| t.is_reserved())
            .flat_map(|t| t.extents())
            .collect();
        let is_internal_eid = |eid: u64| -> bool {
            let local = enchudb_oplog::eid_local(eid);
            local_only_ranges.iter().any(|&(lo, hi)| local >= lo && local < hi)
        };

        let mut count = 0usize;
        // #152: 「ここまでは処理し切った」record の終端 offset。 挿入した record だけ
        // でなく、 skip した record も「処理し切った」に含める (再訪しても同じく skip
        // されるので、 進めておく方が backlog を残さない)。
        let mut done_end: Option<u64> = None;
        for (rec, rec_end) in &records {
            // 自己再帰 op は skip。 Commit (= barrier marker) / Vocab (= global
            // sync 必須) は eid 持たないのでそのまま通す。
            let skip = match &rec.op {
                enchudb_oplog::oplog::DecodedOp::Tie { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::Untie { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::Delete { eid }
                | enchudb_oplog::oplog::DecodedOp::Content { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::TieNamed { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::TieLeaf { eid, .. }
                | enchudb_oplog::oplog::DecodedOp::TieRef { eid, .. } => is_internal_eid(*eid),
                enchudb_oplog::oplog::DecodedOp::Commit => false,
                enchudb_oplog::oplog::DecodedOp::Vocab { .. } => false,
            };
            if skip { done_end = Some(*rec_end); continue; }
            // 0.11 (request10 / #76 逆写像): translated foreign entity への
            // **self-authored** write は、 元 entity の世界番号に宛名を書き戻して
            // bridge する (lsn / hlc / author は維持、 eid を含む payload は再署名)。
            // 受信側は eid_peer(eid) をキーに翻訳する (phase 2) ので全 peer で
            // 同一 entity に収束し、 衝突は HLC LWW が裁く。 0.9.0-0.10.x の
            // single-writer guard はこれで撤去。
            //
            // 例外: **Ref 値が translated local を指す write は発送不能**。 wire の
            // value は u32 で世界番号 (u64) を運べない (= wire 拡張が要る、
            // request10 follow-up) ため skip + 一度だけ warn で local-only に
            // 留める。 これは 0.10.x まで silent に断片化していた潜在バグの封鎖。
            let self_peer = wal.peer_id();
            let mut resigned: Option<enchudb_oplog::oplog::ResignedRecord> = None;
            // #183: この record を TieRef へ書き換えて発送したか (op_type metadata 用)
            let mut sent_as_tie_ref = false;
            if rec.author_peer == self_peer {
                // #183: Ref 値が translated foreign entity を指す self-authored Tie は
                // target の世界番号 (逆写像で復元) を同乗させた **TieRef** に書き換えて
                // 発送する — 0.11.0 の「残る制約」(wire の u32 value に世界番号が
                // 入らない) の解消。逆写像が引けない場合と TieNamed の Ref は従来
                // どおり skip + 一度だけ warn (安全側、silent 断片化はさせない)。
                let mut tie_ref: Option<(u64, u64)> = None; // (target_world, 元 row eid)
                let ref_unsendable = match &rec.op {
                    enchudb_oplog::oplog::DecodedOp::Tie { eid, himo_id, value } => {
                        if self.himo_is_ref(*himo_id)
                            && self.eid_translator.is_translated_local(*value)
                        {
                            match self.eid_translator.reverse(*value) {
                                Some((owner, owner_local)) => {
                                    tie_ref = Some((
                                        enchudb_oplog::make_eid(owner, owner_local),
                                        *eid,
                                    ));
                                    false
                                }
                                None => true, // 逆写像なし = 元 entity を導けない
                            }
                        } else {
                            false
                        }
                    }
                    enchudb_oplog::oplog::DecodedOp::TieNamed { himo_kind, value, .. } => {
                        *himo_kind == crate::himo_store::ValueType::Ref as u8
                            && self.eid_translator.is_translated_local(*value)
                    }
                    _ => false,
                };
                if ref_unsendable {
                    if !self.warned_ref_to_replica.swap(true, Ordering::Relaxed) {
                        eprintln!(
                            "[enchudb] warning: local Ref write pointing at a replicated \
                             foreign entity could not be propagated (reverse eid mapping \
                             missing, or named-himo Ref) — kept local-only (#183)."
                        );
                    }
                    done_end = Some(*rec_end);
                    continue;
                }
                let row_local = match &rec.op {
                    enchudb_oplog::oplog::DecodedOp::Tie { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::Untie { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::Delete { eid }
                    | enchudb_oplog::oplog::DecodedOp::Content { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::TieNamed { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::TieLeaf { eid, .. }
                    | enchudb_oplog::oplog::DecodedOp::TieRef { eid, .. } => {
                        Some(enchudb_oplog::eid_local(*eid))
                    }
                    _ => None,
                };
                // 行 eid 自体が translated foreign なら世界番号へ書き戻す (0.11 #76)
                let row_world = row_local.and_then(|local| {
                    self.eid_translator
                        .reverse(local)
                        .map(|(owner, owner_local)| enchudb_oplog::make_eid(owner, owner_local))
                });
                if let Some((target_world, orig_eid)) = tie_ref {
                    let new_eid = row_world.unwrap_or(orig_eid);
                    let kp_guard = self.keypair.read().unwrap();
                    let kp = kp_guard.as_deref();
                    match enchudb_oplog::oplog::resign_as_tie_ref(rec, new_eid, target_world, kp)
                    {
                        Some(r) => {
                            resigned = Some(r);
                            sent_as_tie_ref = true;
                        }
                        // Tie 以外で None だが tie_ref は Tie でしか立たない。
                        // 万一の場合も発送せず local-only に留める (安全側)
                        None => {
                            done_end = Some(*rec_end);
                            continue;
                        }
                    }
                } else if let Some(world) = row_world {
                    let kp_guard = self.keypair.read().unwrap();
                    let kp = kp_guard.as_deref();
                    match enchudb_oplog::oplog::resign_with_eid(rec, world, kp) {
                        Some(r) => resigned = Some(r),
                        // eid 持ち op で None は起きないはずだが、 起きたら
                        // 発送せず local-only に留める (安全側)
                        None => {
                            done_end = Some(*rec_end);
                            continue;
                        }
                    }
                }
            }
            let row_eid = match self.entity_in("_sync_ops") {
                Ok(e) => e,
                Err(_) => {
                    // eid_range exhausted (= ring 満杯)。 #152: **挿入し切った分まで**
                    // cursor を進めて返す (partial advance)。
                    //
                    // 履歴:
                    // - 0.18.1 まで: committed_end まで飛ばして残りを破棄 → ring 満杯が
                    //   続く限り全ての新規変更が配布から無言で欠落する data loss
                    // - 0.18.2 (#150): cursor を一切進めない retry → 損失は消えたが、
                    //   backlog が ring 容量を超えると毎周「先頭 K 件を再挿入 → K+1 件目で
                    //   満杯 → cursor 据置」を繰り返して**永久に前進しない** (#152)
                    // - 本実装: 処理し切った record の終端まで進める。 各 record は
                    //   ちょうど 1 回だけ挿入され、 重複も損失も進行不能も無い。
                    //   ring が空けば必ず続きから再開する。
                    if let Some(end) = done_end {
                        self.advance_sync_ops_cursor(from, end);
                    }
                    if !self.warned_sync_ops_full.swap(true, Ordering::Relaxed) {
                        eprintln!(
                            "[enchudb] warning: _sync_ops ring is full — oplog→sync bridge is \
                             backpressured (transferred records are kept, the rest wait; \
                             nothing is dropped). Consumers must ack (ack_sync) so \
                             reclaim_sync_ops can free the ring."
                        );
                    }
                    return count;
                }
            };
            count += 1;
            let lsn = self.next_sync_lsn.fetch_add(1, Ordering::AcqRel);
            if trace {
                eprintln!(
                    "[bridge]   row lsn={lsn} eid={row_eid} op={:?}",
                    match &rec.op {
                        enchudb_oplog::oplog::DecodedOp::Tie { value, himo_id, .. } =>
                            format!("Tie(himo={himo_id},val={value})"),
                        other => format!("{other:?}"),
                    }
                );
            }
            self.tie_to_by_id(row_eid, peer_id_hid, rec.author_peer);
            // DecodedOp variant を tag (Tie=0, Untie=1, Delete=2, Content=3, Commit=4, Vocab=5, TieNamed=6, TieLeaf=7, TieRef=8)
            // #183: Tie → TieRef へ書き換えて発送した record は payload に合わせて 8。
            let op_type = if sent_as_tie_ref {
                8
            } else {
                match &rec.op {
                    enchudb_oplog::oplog::DecodedOp::Tie { .. } => 0,
                    enchudb_oplog::oplog::DecodedOp::Untie { .. } => 1,
                    enchudb_oplog::oplog::DecodedOp::Delete { .. } => 2,
                    enchudb_oplog::oplog::DecodedOp::Content { .. } => 3,
                    enchudb_oplog::oplog::DecodedOp::Commit => 4,
                    enchudb_oplog::oplog::DecodedOp::Vocab { .. } => 5,
                    enchudb_oplog::oplog::DecodedOp::TieNamed { .. } => 6,
                    enchudb_oplog::oplog::DecodedOp::TieLeaf { .. } => 7,
                    enchudb_oplog::oplog::DecodedOp::TieRef { .. } => 8,
                }
            };
            self.tie_to_by_id(row_eid, op_type_hid, op_type);
            // hlc.wall は u64 ms-since-epoch、 下位 32bit のみ保持 (= ~50 日サイクル
            // で wrap するが、 lsn 順序で query するので debug/filter 程度の用途)
            self.tie_to_by_id(row_eid, hlc_wall_lo_hid, rec.hlc.wall as u32);
            // 0.8.0 phase 2: payload は signature(64) + pubkey_fp(8) + signed_bytes(rest)
            // の concat 形式。 0.7.0 では signed_bytes のみだったが、 publish path を
            // _sync_ops 経由にするため signature 込みで保存し、 sync crate で完全な
            // WireRecord に復元できるよう拡張した。 wire format breaking、 0.7.x との
            // 並走 sync は不可。
            // 0.11: 逆写像で宛名を書き戻した record は再署名済み payload を使う
            let (sig, fp, sb): (&[u8; 64], &[u8; 8], &[u8]) = match &resigned {
                Some(r) => (&r.signature, &r.pubkey_fp, &r.signed_bytes[..]),
                None => (&rec.signature, &rec.pubkey_fp, &rec.signed_bytes[..]),
            };
            let mut wire_payload = Vec::with_capacity(72 + sb.len());
            wire_payload.extend_from_slice(sig);
            wire_payload.extend_from_slice(fp);
            wire_payload.extend_from_slice(sb);
            self.tie_bytes_to_by_id(row_eid, payload_hid, &wire_payload);
            // #235: **`lsn` は最後**。 `_sync_ops` の走査は全部 `entities_with_himo(lsn_hid)`
            // 経由 (`reclaim_sync_ops` / `ack_sync_prefix` / `pending_sync_ops`) なので、
            // lsn を tie した瞬間に row が索引へ載る。 先に tie すると payload の無い
            // row が見えて、 #217 の dead-row purge が **書き込み途中の row を
            // 「壊れている」と誤判定して消す** (実測: 健全な系で 51960 発行 → 51956 行、
            // 差の 4 件は oplog cursor が越えているので二度と bridge されない)。
            //
            // lsn を commit marker にすると 「索引に載っている row は必ず完成している」
            // が成立する。 engine 内の `set_cell` (値 → HLC の順) と同じ規則で、
            // 逆順にすると 「識別子だけ新しい」 窓ができる、 も同じ。
            self.tie_to_by_id(row_eid, lsn_hid, lsn);
            done_end = Some(*rec_end);
        }

        // 全部転送できた、 offset を「読み切った commit 済み終端」に進める (#77-H4)。
        // committed_end は最後の Commit record の直後なので、 最終 record の終端
        // (done_end) より必ず先に居る。
        if trace {
            eprintln!("[bridge] done inserted={count} offset:={committed_end}");
        }
        self.advance_sync_ops_cursor(from, committed_end);
        // 満杯が解消して完走した — 次の満杯では再び warn する
        self.warned_sync_ops_full.store(false, Ordering::Relaxed);
        count
    }

    /// transfer の cursor 前進。 **入口で読んだ `from` からの CAS** で行う。
    ///
    /// transfer は入口で `from` を読み、 scan → row insert のあと最後に cursor を
    /// store する。 その間に fold が cursor を巻き戻していた場合、 素の store は
    /// 巻き戻しを stale 値で上書きしてしまう (= cursor が head を追い越して固定、
    /// 新 ring の record が永久に scan 対象外)。 CAS が外れたら **store しない**:
    /// 巻き戻し後の位置から読み直され、 最悪 重複配布で済む (apply は冪等)。
    ///
    /// 呼び出し側は `transfer_lock` を保持しているので平常時 CAS は必ず成功する。
    /// 失敗は 「fold との直列化が破れた」 の signal なので数える。
    fn advance_sync_ops_cursor(&self, from: u64, to: u64) {
        use std::sync::atomic::Ordering;
        if self
            .sync_ops_offset
            .compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.sync_ops_cursor_repairs.fetch_add(1, Ordering::Relaxed);
            if Self::trace_bridge_enabled() {
                eprintln!(
                    "[bridge] cursor CAS lost ({from} → {to}); keeping {} (fold が巻き戻した)",
                    self.sync_ops_offset.load(Ordering::Acquire),
                );
            }
        }
    }

    /// oplog ring buffer が `try_reset` で head を HEADER_SIZE に巻き戻したとき、
    /// bridge cursor (`sync_ops_offset`) も HEADER_SIZE に戻す。
    ///
    /// #63 fix の regression 対策: try_reset は head/checkpoint しか戻さないため、
    /// これを呼ばないと sync_ops_offset が古い head を指したまま取り残され、
    /// reset 後に append された record が `from > head` で transfer 対象から外れ、
    /// `_sync_ops` に bridge されず sync から無言で欠落する。 reset と同じ
    /// consumer thread から、 transfer 完了 (= offset==旧head) かつ pending==0 の
    /// 直後にのみ呼ぶこと。
    pub fn reset_sync_ops_offset(&self) {
        self.sync_ops_offset.store(
            enchudb_oplog::oplog::HEADER_SIZE as u64,
            std::sync::atomic::Ordering::Release,
        );
    }

    /// 0.18.2: WAL ring を畳んで（`try_reset`）よいか。
    ///
    /// sync bridge（`transfer_oplog_to_sync_ops`）がまだ読んでいない領域が残って
    /// いる間は畳んではならない — 畳むと未 bridge record が WAL ごと消え、 その変更は
    /// **sync から無言で欠落**する。 0.18.1 までの「無条件に畳んでよい」は
    /// 「sync は `_sync_ops` 経由で ring を直接読まない」という誤った前提だった
    /// （bridge 自体が ring の reader）。 bridge が backpressure で止まっている間
    /// （ring 満杯）は fold も止まり、 WAL が保持を引き受ける。
    pub fn wal_fold_safe(&self) -> bool {
        if !self.sync_tables_enabled() {
            return true;
        }
        let Some(wal) = self.oplog.as_ref() else {
            return true;
        };
        let offset = self
            .sync_ops_offset
            .load(std::sync::atomic::Ordering::Acquire);
        let head = wal.head();
        // cursor が head を **追い越している** のは不整合。 起きうる道は fold の
        // 巻き戻しと in-flight transfer の store の race (lost update) で、 この状態を
        // `offset >= head` として 「追いつき済み」 と読むと、 新 ring に積まれた
        // 未 bridge record を畳み続けて無言の恒久欠落になる (実測: 300 iter で 1 件)。
        // 畳まず、 cursor を ring 先頭に戻して拾い直させる (= 最悪 重複配布、
        // apply は冪等なので損失より軽い)。 黙って直さない — 数えて警告する。
        if offset > head {
            self.sync_ops_cursor_repairs
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.sync_ops_offset.store(
                enchudb_oplog::oplog::HEADER_SIZE as u64,
                std::sync::atomic::Ordering::Release,
            );
            eprintln!(
                "[enchudb] warning: sync bridge cursor ({offset}) overtook oplog head ({head}) — \
                 rewound to ring start to avoid dropping un-bridged records \
                 (records already delivered may be re-sent; apply is idempotent)"
            );
            return false;
        }
        if offset >= head {
            return true;
        }
        // offset < head でも、 WAL が「Commit 1 個すら append できない」満杯
        // （append_dead）で、 かつ bridge が最後の committed group まで読み切って
        // いるなら、 残る tail は「閉じの Commit が満杯で書けなかった孤児 group」。
        // 今後 commit される可能性がゼロ（append が全て失敗する）なので、 recovery
        // からも sync からも永久に不可視 = 保持する意味が無く、 畳んでよい。
        //
        // この例外が無いと、 commit group の途中で WAL が満杯に達した瞬間に
        //   閉じ Commit が書けない → committed_end < head 固定 → fold 恒久不能
        //   → 以後の append 全滅（無音 drop）→ 新規変更が sync から永久欠落
        //   → reopen のたび旧 backlog だけを全量再 bridge
        // という**自己修復不能の brick** になる（実機発現）。 tail に未 bridge の
        // committed group が残っている間（= ring 満杯の backpressure 中）は、 従来
        // どおり畳まない。
        if wal.append_dead() {
            let (records, _) = wal.iter_committed_from_with_offsets(offset);
            return records.is_empty();
        }
        false
    }

    /// `ENCHU_TRACE_BRIDGE=1` のときだけ有効になる、 oplog→`_sync_ops` bridge と
    /// ring fold の系列トレース。 「`oplog_sync()` は成功したのに record が
    /// `_sync_ops` に無い」 類の欠落は、 事後状態 (head/checkpoint/offset) だけでは
    /// どの段で落ちたか判別できないため、 段ごとに出す。
    pub fn trace_bridge_enabled() -> bool {
        static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ON.get_or_init(|| std::env::var("ENCHU_TRACE_BRIDGE").is_ok_and(|v| v != "0"))
    }

    /// `try_reset_if` の述語として `append_lock` 保持下で呼ばれる `wal_fold_safe`。
    ///
    /// 判定内容は `wal_fold_safe` と同一。 違いは **false だった回数を数える**点だけ。
    /// lock 外の pre-check が true を返した後にここで false になるのは、 pre-check と
    /// fold の間に append + `advance_checkpoint` が割り込んだ場合だけなので、
    /// この counter は check-then-act の窓を踏んだ回数そのものになる。
    pub fn wal_fold_safe_locked(&self) -> bool {
        let safe = self.wal_fold_safe();
        if !safe {
            self.fold_race_saves
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        safe
    }

    /// fold (`try_reset` + `reset_sync_ops_offset`) を `transfer_oplog_to_sync_ops`
    /// と直列化するための guard。 fold は cursor を巻き戻すので、 cursor を最後に
    /// store する transfer と並走してはならない (lost update → 恒久欠落)。
    pub fn transfer_lock_for_fold(&self) -> std::sync::MutexGuard<'_, ()> {
        self.transfer_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// bridge cursor が head を追い越したのを検出して修復した回数（観測用）。
    /// 平常時は 0。 増えていれば fold と transfer の直列化が破れている。
    pub fn sync_ops_cursor_repairs(&self) -> u64 {
        self.sync_ops_cursor_repairs
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// bridge cursor の現在値（観測用）。
    pub fn sync_ops_bridge_offset(&self) -> u64 {
        self.sync_ops_offset.load(std::sync::atomic::Ordering::Acquire)
    }

    /// fold ↔ bridge の check-then-act を lock 下の再検証で弾いた回数（観測用）。
    /// 増えていれば「その窓を踏んだが record は守られた」。
    pub fn fold_race_saves(&self) -> u64 {
        self.fold_race_saves
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// WAL がこれ以上いかなる record も受け付けない満杯か（観測用）。
    /// oplog 未使用なら false。 `wal_fold_safe` の死区間例外と対で、 呼び出し側が
    /// 「配布が止まっている」を無音にしないための可視化に使う。
    pub fn wal_append_dead(&self) -> bool {
        self.oplog.as_ref().is_some_and(|w| w.append_dead())
    }

    /// WAL の残り append 可能バイト数（観測用）。 oplog 未使用なら `u64::MAX`
    /// （= 逼迫という概念が無い）。
    pub fn wal_free_bytes(&self) -> u64 {
        self.oplog.as_ref().map_or(u64::MAX, |w| w.free_bytes())
    }

    /// 0.7.0 (Phase 4): peer の watermark を更新する。 Syncer が peer から ack を
    /// 受け取ったタイミングで呼ぶ。 既存 row があれば update、 無ければ insert。
    /// `_sync_peers.consumed_lsn` を idempotent に更新。
    pub fn ack_sync(&self, peer: enchudb_oplog::PeerId, consumed_lsn: u32) -> Result<(), String> {
        if !self.sync_tables_enabled() {
            return Err("sync tables not enabled (call enable_sync first)".into());
        }
        let peer_id_hid = self.himo_id("_sync_peers.peer_id")
            .ok_or("missing _sync_peers.peer_id himo")? as u16;
        let consumed_lsn_hid = self.himo_id("_sync_peers.consumed_lsn")
            .ok_or("missing _sync_peers.consumed_lsn himo")? as u16;
        let last_seen_hid = self.himo_id("_sync_peers.last_seen_at")
            .ok_or("missing _sync_peers.last_seen_at himo")? as u16;

        // 既存 peer row を探す: peer_id 値で query
        let existing = self.query_by_id(&[(peer_id_hid, peer)]);
        let row_eid = match existing.into_iter().next() {
            Some(e) => e,
            None => self.entity_in("_sync_peers")
                .map_err(|e| format!("entity_in(_sync_peers): {e}"))?,
        };

        self.tie_to_by_id(row_eid, peer_id_hid, peer);
        self.tie_to_by_id(row_eid, consumed_lsn_hid, consumed_lsn);
        // last_seen_at: 現在時刻 (ms / 65536 で u32 に収める = ~3000 年サイクル)
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.tie_to_by_id(row_eid, last_seen_hid, (now_ms / 65536) as u32);
        Ok(())
    }

    /// #149: pull cursor (HLC) に基づく ack。 relay/gateway 経路には明示 ack が
    /// 無いが、 pull の since カーソルは「適用済み record の max HLC」からしか
    /// 前進しないので、 それ自体が **「この peer はここまで消化済み」の到達証明**に
    /// なっている。 これを consumed_lsn に写して reclaim を回せるようにする。
    ///
    /// #217: scalar cursor は「**自 peer が author の row** に対する消化証明」として
    /// のみ解釈する (= `[(self, cursor)]` + 他 author は ZERO の退化形)。 scalar は
    /// author を判別できないため、 全 author に適用すると relay 経由の「未知 author
    /// の古い HLC の row」を受信していないのに消化済みと誤判定する (over-ack →
    /// 未消化 record の reclaim)。 relay された row (foreign author) は
    /// [`Engine::ack_sync_up_to_cursors`] の vector ack が来るまで prefix を止める —
    /// これは保守側で、 消化の証明が無い row を越えないという原則そのもの。
    /// 該当が無い (cursor が最古の生存 row より古い) 場合は ack せず 0 を返す。
    ///
    /// **ack するのは必ず「実在を確認した生存 row の lsn」**であって、 bridge 先端
    /// (`current_sync_lsn`) ではない。 先端まで ack すると、 生存 row の snapshot を
    /// 取った後に bridge が append した record — cursor より新しい = **まだ pull されて
    /// いない record** — まで「消化済み」と記録され、 `reclaim_sync_ops` が peer に
    /// 届く前に回収してしまう (失うと再著者でしか復旧しない)。 rows[0] の lsn で
    /// 止めても生存リングは同じだけ reclaim でき (`lsn < watermark` の delete なので
    /// rows[0] 未満は全て対象)、 取りこぼした先端は次周回の cursor が拾う。
    ///
    /// #217: walk は **longest-consumed-prefix** — lsn 昇順に走査し、 消化の証明が
    /// ある row が続く間だけ前進、 証明の無い row で停止する。 旧実装の降順
    /// first-match は relay 混在 ring (relayed append が原 HLC 素通しで乗るため
    /// lsn 順で HLC 非単調) で、 高 lsn の古い HLC row に match して未消化 row を
    /// watermark の下に巻き込んでいた (over-ack → 未消化 record の reclaim)。
    pub fn ack_sync_up_to_hlc(
        &self,
        peer: enchudb_oplog::PeerId,
        cursor: enchudb_oplog::Hlc,
    ) -> Result<u32, String> {
        // author 0 は「peer identity 設定前の local 著作」 (単独 peer 運用 →
        // 後から sync 参加、 の正規 path) — 定義上 foreign ではないので self と
        // 同じ扱い。 除外すると set_peer_id 後に永久 prefix blocker になる
        // (dead row ではないので削除もされない)。
        let self_peer = self.peer_id();
        self.ack_sync_prefix(peer, &|author, hlc| {
            (author == self_peer || author == 0) && hlc <= cursor
        })
    }

    /// #217: author 別 cursor (vector) に基づく ack — relay 混在 ring 用の完全形。
    ///
    /// `consumed(row) = row.hlc <= cursors[row.author]`、 **cursors に無い author は
    /// ZERO (= 必ず prefix を止める)**。 未知 author の row は「puller に届いた証明が
    /// 無い」ので越えない — scalar min への短絡はこの row を消化済みと誤判定する
    /// (over-ack)。 puller 側の #216 author 別 pull cursor をそのまま渡す。
    ///
    /// 将来 note: author 別 cursor は「author 別 lsn substream (ring 分割)」への
    /// 足場でもある — 単一 ring の prefix walk は最遅 follower / author 1 つで全体が
    /// pin されるのが celebrity fanout の最終的な scale 限界で、 その時 per-author
    /// ack が throughput にも効くようになる。
    pub fn ack_sync_up_to_cursors(
        &self,
        peer: enchudb_oplog::PeerId,
        cursors: &[(enchudb_oplog::PeerId, enchudb_oplog::Hlc)],
    ) -> Result<u32, String> {
        self.ack_sync_prefix(peer, &|author, hlc| {
            cursors.iter().find(|(p, _)| *p == author).is_some_and(|(_, c)| hlc <= *c)
        })
    }

    /// #217 core: `consumed(author, hlc)` が **先頭から連続して成り立つ最長 prefix**
    /// の末尾 lsn へ `ack_sync(peer, lsn)` する。 前進が無ければ ack せず 0。
    ///
    /// - **前回 walk からの再開**: prefix は peer ごとに単調なので、 走査は前回の
    ///   検証済み末尾 (`ack_walk_resume`、 in-memory) より先だけ。 昇順 walk は
    ///   「全部消化済み = 最も健全な状態で ring 全 row を decode する」コスト反転を
    ///   持つが、 再開により償却で「前回 walk 以降の新規 row 数」に落ちる。
    ///   reclaim 済み row は必ず `lsn < watermark <=` 検証済み末尾なので走査に
    ///   穴は生じない。
    /// - **永続 `consumed_lsn` を再開点に使わない (移行 heal)**: 0.23.x 以前の
    ///   降順 first-match は over-ack した値を `_sync_peers.consumed_lsn` に残して
    ///   いる可能性がある。 これを再開点に信用すると「真の prefix と膨張値の間の
    ///   row」が二度と検査されず、 次の reclaim で未消化のまま消える (#217 が
    ///   塞いだ loss が既存 DB で一度起きる)。 そこで session 最初の walk は lsn 0
    ///   から全 ring を検証し直し、 検証結果が stored より小さければ**下方修正で
    ///   上書き**する (watermark が下がる = 保守側)。 残余窓: この session で一度も
    ///   ack しない peer の膨張 stored は watermark に残り、 pressure reclaim が
    ///   その嘘を purge しうる — ただし purge は `record_reclaimed_floor` で floor を
    ///   上げるので、 当該 peer は次の pull で `history_truncated` → bootstrap で
    ///   回復する (= loss ではなく**余分な bootstrap への縮退**)。 回復経路が無い
    ///   例外は 2 つ: decode 不能 row の purge は floor に現れない (#218) /
    ///   state provider 未登録の構成では truncation が行き止まり (relay topology
    ///   では `serve_state` 必須)。 いずれも 0.23.0 の absorb + reclaim が既に
    ///   持っていた露出で、 heal はそれを ack する peer から順に厳密に縮める。
    /// - **dead row**: payload 欠落 / decode 不能な row は構造的に配送不能
    ///   (`collect_records_since` も skip する = 誰の cursor もそこを消化と証明
    ///   できない) なので、 prefix blocker にすると全 peer の ack が永久にそこで
    ///   止まり ring が満杯になる (#149 で潰した backpressure の復活)。 consumption を
    ///   偽らないまま前進するため、 **削除して越える** (計数 + warn、
    ///   [`Engine::sync_dead_rows_purged`])。 削除 slot は ring free list へ返す。
    fn ack_sync_prefix(
        &self,
        peer: enchudb_oplog::PeerId,
        consumed: &dyn Fn(enchudb_oplog::PeerId, enchudb_oplog::Hlc) -> bool,
    ) -> Result<u32, String> {
        if !self.sync_tables_enabled() {
            return Err("sync tables not enabled (call enable_sync first)".into());
        }
        let lsn_hid = self.himo_id("_sync_ops.lsn").ok_or("missing _sync_ops.lsn")? as u16;
        let payload_hid =
            self.himo_id("_sync_ops.payload").ok_or("missing _sync_ops.payload")? as u16;
        let session_start = {
            let guard = self.ack_walk_resume.lock().unwrap();
            guard.get(&peer).copied()
        };
        let start = session_start.unwrap_or(0);

        // 前回 walk より先の生存 row を lsn 昇順に。
        let mut rows: Vec<(u32, u64)> = self
            .entities_with_himo(lsn_hid)
            .into_iter()
            .filter_map(|eid| self.get_by_id(eid, lsn_hid).map(|lsn| (lsn, eid)))
            .filter(|(lsn, _)| *lsn > start)
            .collect();
        rows.sort_by_key(|r| r.0);

        // 生存 row 無し = reclaim するものが無い。 消化の証明が無いのに先端へ ack すると
        // 「消化した」という嘘の記録になるので、 何もしない (後続の bridge を次の pull の
        // cursor が越えたときに通常経路で ack される)。 heal も不要 — 膨張 stored が
        // 過大申告しうるのは生存 row だけで、 それが無いなら失うものが無い。
        if rows.is_empty() {
            return Ok(0);
        }

        let mut ack_lsn: u32 = 0;
        // #218: dead row を消した分の history floor。 loop 内で
        // `record_reclaimed_floors` を呼ぶと sentinel row の read-modify-write を
        // row 数だけ繰り返すので、 貯めてループ後に 1 回。
        let mut dead_row_floors: std::collections::HashMap<u32, enchudb_oplog::Hlc> =
            std::collections::HashMap::new();
        let mut known_authors: Option<std::collections::HashSet<u32>> = None;
        for (lsn, eid) in rows.iter() {
            let decoded = self
                .get_by_id(*eid, payload_hid)
                .map(|vid| self.vocab.get(vid).to_vec())
                .and_then(|b| enchudb_oplog::oplog::decode_sync_ops_payload(&b));
            match decoded {
                Some(rec) => {
                    if !consumed(rec.author_peer, rec.hlc) {
                        break;
                    }
                    ack_lsn = *lsn;
                }
                None => {
                    // #235: **最新 lsn の row は dead 判定しない**。 bridge は
                    // `transfer_lock` 下で 1 行ずつ書き、 `next_sync_lsn` は ties の
                    // 前に fetch_add されるので、 書き込み途中の row は常に高々 1 つ、
                    // かつ必ず最新 lsn。 「まだ書けていない row」を「壊れた row」と
                    // 取り違えると、 健全な record を消して二度と bridge されない
                    // (実測: 51960 発行 → 51956 行)。
                    //
                    // 本命の fix は bridge 側の書き込み順 (lsn を最後に = commit
                    // marker) だが、 store の並べ替えや将来の書き込み順変更に対する
                    // backstop としてここでも止める。 break なので、 完成後の次の
                    // walk で普通に判定される。
                    if *lsn >= self.current_sync_lsn() {
                        break;
                    }
                    // #218: 消す前に floor 候補を作る (delete 後は peer_id を読めない)。
                    // ここで floor を上げないと、 消えた record を必要としていた puller が
                    // 「cursor >= floor」 と誤判定して差分 pull を続ける = #140 で塞いだ
                    // silent partial の復活。 帰属と上界の作り方は
                    // `dead_row_floor_candidate` の doc を参照。
                    let cand = {
                        let known = known_authors.get_or_insert_with(|| self.ring_authors());
                        self.dead_row_floor_candidate(*eid, known)
                    };
                    // #221: delete + free list push は専用 lock 下で atomic に。
                    if self.purge_sync_ops_row(*eid, lsn_hid, *lsn) {
                        let (author, hlc) = cand;
                        let whose = if author == u32::MAX {
                            "all authors (unattributable row)".to_string()
                        } else {
                            format!("author {author}")
                        };
                        eprintln!(
                            "[enchudb] sync: purging undeliverable _sync_ops row \
                             (lsn {lsn}, missing/undecodable payload) — see #217; \
                             raising history floor for {whose} to {hlc:?} \
                             (followers below it will be told to bootstrap) — see #218"
                        );
                        self.sync_dead_rows_purged
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let e = dead_row_floors
                            .entry(author)
                            .or_insert(enchudb_oplog::Hlc::ZERO);
                        if hlc > *e {
                            *e = hlc;
                        }
                    }
                    ack_lsn = *lsn;
                }
            }
        }

        // #218: 消した dead row の分だけ floor を上げる。 単調 max で merge されるので
        // 呼び直しても下がらない。
        if !dead_row_floors.is_empty() {
            let entries: Vec<(u32, enchudb_oplog::Hlc)> = dead_row_floors.into_iter().collect();
            self.record_reclaimed_floors(&entries);
        }

        // 再開点を更新 (entry の存在 = この session で検証済み、 の marker)。
        {
            let mut guard = self.ack_walk_resume.lock().unwrap();
            let e = guard.entry(peer).or_insert(0);
            let new_resume = start.max(ack_lsn);
            if new_resume > *e {
                *e = new_resume;
            }
        }

        if ack_lsn > 0 {
            // session 最初の walk では stored (膨張の可能性) より小さくても上書き =
            // 下方修正 heal。 2 回目以降は再開走査により単調前進しかしない。
            self.ack_sync(peer, ack_lsn)?;
        } else if session_start.is_none() {
            // session 最初の walk で prefix を 1 row も検証できなかった: 膨張 stored が
            // 生存 row を過大申告している可能性があるので 0 に落とす (reclaim を
            // 止める側 = 保守的)。 正しい値は後続の ack が再構築する。
            let stored = self.sync_consumed_lsn_of(peer);
            if stored > 0 {
                self.ack_sync(peer, 0)?;
            }
        }
        Ok(ack_lsn)
    }

    /// #221: `_sync_ops` の row を 1 件 purge する (delete + ring free list への
    /// slot 返却)。 **専用 lock 下で「snapshot 当時と同じ row か」を再検証してから**
    /// 消す。
    ///
    /// `Engine::delete` は冪等で戻り値を持たないため、 lock 無しで並行 purge すると
    /// 同じ slot が free list に二重 push され、 後続の `entity_in("_sync_ops")` が
    /// 同一 eid を二回払い出して bridge row が silent に上書きされる。 並行経路は
    /// 実在する: `Syncer::absorb_pull_acks` (→ `reclaim_sync_ops` / ack walk の
    /// dead row purge) は複数 peer からの並行 pull で並行実行される。
    ///
    /// **検証は「生存」ではなく `expected_lsn` との一致で行う (ABA)。** 生存だけを
    /// 見ると、 T1 が purge → slot が bridge に再利用されて同一 eid に新 row が乗る
    /// → T2 が stale snapshot で「生存」と判定して**その新 row を消す**、 が成立する
    /// (しかも slot は pop 済みなので dedupe も効かない)。 `next_sync_lsn` は単調
    /// 増加なので、 lsn は「同じ row か」の判別子として機能する (再利用後の row は
    /// 必ず大きい lsn を持つ)。 この窓は ring 再利用が常時走る構成 —
    /// まさに reclaim + bridge を並行で回す運転 — で開く。
    ///
    /// **`free_locals` は `delete` の前に取り、 push まで保持する。** free list への
    /// producer は本 helper だけではない — `entity_in` の枯渇 slow path から呼ばれる
    /// [`Engine::rebuild_free_locals`] が range を線形 scan して**非 live な local を
    /// 穴として push** する。 delete 後 push 前の中間状態でこれが走ると、 同じ slot が
    /// 両者から独立に push されて二重になる (expected_lsn は「同じ row を 2 者が
    /// 消す」しか防げない)。 free list を掴んだまま delete すれば rebuild は中間状態を
    /// 観測できず、 待たされた後は `fl` が非空なので早期 return する。 枯渇は
    /// 「free list を使い切るまで bridge する」運転で常態なので、 この窓は実運用で開く。
    ///
    /// lock 順序は `free_locals` → (delete 内部の) `EntitySet::free_lock` の一方向のみ。
    /// `EntitySet` 側は Engine の table に触らないので逆転しない。
    ///
    /// **コストの注記 (未実測)**: この形で critical section は「Vec への push」から
    /// 「`delete` 全体 (oplog append + column write + tombstone)」に広がる。 その間
    /// `entity_in("_sync_ops")` は待つので、 reclaim sweep 中は bridge の row 払い出しが
    /// row 単位で purge と競合する。 row ごとに解放されるので sweep 全体を止める形では
    /// ないが、 影響は測っていない — fanout で bridge throughput を見るときの観測点。
    ///
    /// 戻り値 `true` = 自分が消した (呼び元は計数して良い)、 `false` = 既に他者が
    /// 消した / slot が再利用されて別 row になっていた。
    fn purge_sync_ops_row(
        &self,
        eid: enchudb_oplog::EntityId,
        lsn_hid: u16,
        expected_lsn: u32,
    ) -> bool {
        let _guard = self.sync_ops_purge_lock.lock().unwrap();
        if self.get_by_id(eid, lsn_hid) != Some(expected_lsn) {
            return false;
        }
        let meta = self
            .tables
            .iter()
            .find(|t| t.name == "_sync_ops")
            .map(|t| (t.local_of(enchudb_oplog::eid_local(eid)), t.free_locals.clone(), t.free_locals_nonempty.clone()));
        match meta {
            Some((Some(local), free_list, nonempty)) => {
                let mut list = free_list.lock().unwrap();
                self.delete(eid);
                list.push(local);
                nonempty.store(true, std::sync::atomic::Ordering::Release);
            }
            _ => self.delete(eid),
        }
        true
    }

    /// #217: `_sync_peers` に記録済みの `peer` の consumed_lsn。 row 無しは 0。
    fn sync_consumed_lsn_of(&self, peer: enchudb_oplog::PeerId) -> u32 {
        let Some(peer_id_hid) = self.himo_id("_sync_peers.peer_id") else { return 0; };
        let Some(consumed_lsn_hid) = self.himo_id("_sync_peers.consumed_lsn") else { return 0; };
        self.query_by_id(&[(peer_id_hid as u16, peer)])
            .into_iter()
            .next()
            .and_then(|eid| self.get_by_id(eid, consumed_lsn_hid as u16))
            .unwrap_or(0)
    }

    /// #217: ack prefix walk が削除した dead row (配送不能 payload) の累計 (観測用)。
    /// 平常時は 0。 増えていれば bridge が壊れた payload を書いたことがある。
    pub fn sync_dead_rows_purged(&self) -> u64 {
        self.sync_dead_rows_purged
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// #236: [`Engine::state_records_for`] が **配れないと判断して落とした cell** の
    /// 累計 (理由は問わない)。 平常時 0。
    ///
    /// 落とす判断はどれも個別には正しく、 batch は `complete: false` なので受信側の
    /// ghost sweep も走らない。 危険なのは判断ではなく **落とした事実がどこにも
    /// 残らない**ことで、 replica が系統的に不完全な state を配っていても呼び出し側
    /// からは 「bootstrap は成功した」 としか見えない — #140 / #216 / #218 で繰り返し
    /// 「defect の class」 と扱ってきた silent partial そのもの。
    ///
    /// 増えていたら 「その replica の state は cell 単位で欠けている」。 到達経路は
    /// 版数 ZERO の cell (pre-v9 DB / #160) と、 relay 自身が translated local へ
    /// write-back した Tag cell (#76 / #178)。 回復手段は author 直 bootstrap。
    pub fn state_records_dropped(&self) -> u64 {
        self.state_records_dropped
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// #218: **decode 不能な `_sync_ops` row を purge する直前**に、 その row が
    /// history floor へ寄与すべき `(author, HLC 上界)` を作る。
    ///
    /// purge した record は ring から永久に消えるので、 floor を上げないと
    /// 「cursor >= floor だから差分 pull を続けてよい」 と puller が誤判定して、
    /// **#140 で塞いだ silent partial がこの経路で復活する**。 decode できた row
    /// だけで floor を作るのは floor の過少申告。
    ///
    /// **帰属は取れる** — bridge は payload とは別の列に author を書いている
    /// (`_sync_ops.peer_id`)。 Column が別なので payload が壊れていても普通に読める。
    /// 無帰属 baseline (`u32::MAX`) に倒すと全 author の follower が bootstrap する
    /// のに対し、 帰属できればその author の follower だけで済む。
    ///
    /// ただし **row 全体が壊れている場合は `peer_id` もゴミ値**になり得る。 ゴミを
    /// 信じると (a) 無関係な author の follower が余計に bootstrap し、 (b) 本当の
    /// author の follower は silent gap のまま、 という最悪の組み合わせになるので、
    /// **実在の裏が取れている author ([`Engine::ring_authors`]) でなければ baseline に
    /// 落とす**。 判定の失敗形は 「厳しすぎる → 余計な bootstrap」 と
    /// 「緩すぎる → silent gap」 で非対称なので、 迷ったら baseline 側。
    ///
    /// HLC 上界は `mint_local_hlc()` (= 今の HLC)。 engine は適用した remote record で
    /// 必ず clock を merge する (`observe_remote_hlc` → `OpLog::observe_hlc`、
    /// `append_relayed` も内部で呼ぶ) ので、 ring に載り得た HLC は全部一度は観測済み
    /// = 今 mint した HLC はその上界。 record を書かない mint は logical を 1 進める
    /// だけで、 `hlc_mint_lock` の規則 (採番順 = WAL 順) は WAL に載る record に
    /// ついての規則なので破らない。
    ///
    /// より tight な上界は採らなかった:
    ///
    /// - `_sync_ops.hlc_wall_lo` は wall の**下位 32bit のみ**。 上位を近傍 row から
    ///   借りる必要があり、 remote の clock skew で外れる。 外れ方が下振れすると
    ///   silent gap そのものなので unsound。
    /// - 「同 author の次の row の HLC」 (per-author lsn 単調性) が一番 tight だが、
    ///   正しさが 「relay が複数 upstream から同一 author を引いても ring 内で HLC 順が
    ///   崩れない」 に依存する。 purge 地点でそれを検証できず、 外れたときの失敗形が
    ///   silent gap = 今直しているバグそのものなので採らない。
    /// - `Hlc::MAX` は論外 — floor は単調 max で下がらないので、 1 件の破損で恒久的に
    ///   全 peer truncation になる。
    ///
    /// 使うのは `ack_sync_prefix` (dead row **だけ**を消すので、 decodable な row は
    /// 全部生き残っており [`Engine::ring_authors`] で裏が取れる)。 `reclaim_sync_ops`
    /// は decodable な row も消すため、 走査後に `reclaimed_max` の key と併せて
    /// 帰属を確定する — そちらは関数内にインラインで書いてある。
    fn dead_row_floor_candidate(
        &self,
        eid: enchudb_oplog::EntityId,
        known_authors: &std::collections::HashSet<u32>,
    ) -> (u32, enchudb_oplog::Hlc) {
        let author = self
            .himo_id("_sync_ops.peer_id")
            .and_then(|h| self.get_by_id(eid, h as u16))
            .filter(|a| known_authors.contains(a))
            .unwrap_or(u32::MAX);
        (author, self.mint_local_hlc())
    }

    /// #218: `dead_row_floor_candidate` の妥当性判定に使う 「実在すると分かっている
    /// author」 の集合。
    ///
    /// - 自分 (自分が author した row)
    /// - ring 内で **payload が decode できた** row が名乗っている author。 payload で
    ///   裏が取れているので、 壊れた row のゴミ値は (偶然一致しない限り) 入らない
    /// - 既存の floor entry の author (無帰属 baseline は除く)。 ring が薄くなっても
    ///   帰属精度を保つため。 floor に載る author は上の 2 経路のどちらかを通ったもの
    ///   だけなので、 ゴミが混ざることはない
    ///
    /// `eid_translator.authors()` (= [`Engine::replicated_authors`]) は使えない —
    /// **#209 の verbatim relay は eid を翻訳しない** (author の eid をそのまま流す) ので、
    /// relay が中継しているだけの author は一度も translator に載らない。 relay こそが
    /// 帰属を一番必要とする側なので、 それでは常に baseline に落ちる。
    fn ring_authors(&self) -> std::collections::HashSet<u32> {
        let mut set = std::collections::HashSet::new();
        set.insert(self.peer_id());
        if let Some(floors) = self.read_reclaimed_floor_entries() {
            for (a, _) in floors {
                if a != u32::MAX {
                    set.insert(a);
                }
            }
        }
        let (Some(lsn_hid), Some(payload_hid)) = (
            self.himo_id("_sync_ops.lsn"),
            self.himo_id("_sync_ops.payload"),
        ) else {
            return set;
        };
        for eid in self.entities_with_himo(lsn_hid as u16) {
            let Some(vid) = self.get_by_id(eid, payload_hid as u16) else { continue };
            let bytes = self.vocab.get(vid).to_vec();
            if let Some(rec) = enchudb_oplog::oplog::decode_sync_ops_payload(&bytes) {
                set.insert(rec.author_peer);
            }
        }
        set
    }

    /// #227: **下流全員が消化し切った位置** を author 別 HLC で返す (transitive
    /// watermark の材料)。
    ///
    /// relay が author に返す pull-as-ack は「自分が apply した位置」ではいけない。
    /// reclaim の安全条件は「全 follower が **apply し切った**」であって「配った」
    /// ではないので、 relay が配った直後に消えると author は「消化済み」と信じて
    /// 履歴を捨て、 下流が永久欠落する (#191 の裏返しで 1 段深い)。
    ///
    /// 導出は既存の永続 state だけで足りる — **新しい記録は増やさない**:
    ///
    /// - `lsn <= sync_watermark()` の `_sync_ops` row = 下流全員が消化済み。
    ///   その範囲の author 別 max HLC がそのまま transitive ack の値。
    /// - 既に purge された分は `sync_reclaimed_floors()` (#216 で author 別) に
    ///   author 別 max HLC として残っているので畳み込む。 無帰属 baseline
    ///   (`u32::MAX`) は author に帰属させられないので**使わない** (保守側)。
    ///
    /// `None` = 下流 peer が 1 つも `_sync_peers` に居ない = 「配り切った」の判定
    /// 材料が無い。 caller は自分の cursor をそのまま使うこと — 葉ノード (中継先の
    /// 居ない peer) がここで ZERO を返すと author の reclaim を永久に止める。
    ///
    /// **既知の残り窓**: 「pull はしたが 1 件も消化していない下流」 は
    /// `_sync_peers` に行を作らない (`ack_sync_prefix` は ack_lsn 0 では
    /// `ack_sync` を呼ばない) ので、 下流ゼロと区別できない。 その間 relay は
    /// full ack する。 塞ぐには pull で行を materialize する必要があるが、
    /// それは relay に限らず全 author の reclaim 挙動を変える (一度 pull して
    /// 消えた peer が watermark を 0 に固定 = #149 の失敗形) ので採らない。
    /// 受け皿は floor + bootstrap (#140 / replica は #226)。
    pub fn sync_delivered_cursors(&self) -> Option<Vec<(enchudb_oplog::PeerId, enchudb_oplog::Hlc)>> {
        if !self.sync_tables_enabled() {
            return None;
        }
        let peer_id_hid = self.himo_id("_sync_peers.peer_id")? as u16;
        let self_peer = self.peer_id();
        // 下流 (self を除く登録済み peer) が 1 つも無ければ判定材料が無い。
        let has_downstream = self.entities_with_himo(peer_id_hid).into_iter().any(|eid| {
            match self.get_by_id(eid, peer_id_hid) {
                Some(pid) => self_peer == 0 || pid != self_peer,
                None => false,
            }
        });
        if !has_downstream {
            return None;
        }

        let mut out: std::collections::HashMap<enchudb_oplog::PeerId, enchudb_oplog::Hlc> =
            std::collections::HashMap::new();
        let mut bump = |author: enchudb_oplog::PeerId, hlc: enchudb_oplog::Hlc| {
            let e = out.entry(author).or_insert(enchudb_oplog::Hlc::ZERO);
            if hlc > *e {
                *e = hlc;
            }
        };
        // purge 済み = 当時の下流全員が消化した範囲。
        if let Some(entries) = self.read_reclaimed_floor_entries() {
            for (author, hlc) in entries {
                if author != u32::MAX {
                    bump(author, hlc);
                }
            }
        }
        // 生存 row のうち watermark 以下 (= 下流全員が消化済み)。
        let watermark = self.sync_watermark();
        if watermark > 0
            && let Some(lsn_hid) = self.himo_id("_sync_ops.lsn")
            && let Some(payload_hid) = self.himo_id("_sync_ops.payload")
        {
            for eid in self.entities_with_himo(lsn_hid as u16) {
                let Some(lsn) = self.get_by_id(eid, lsn_hid as u16) else { continue };
                if lsn > watermark {
                    continue;
                }
                if let Some(rec) = self
                    .get_by_id(eid, payload_hid as u16)
                    .map(|vid| self.vocab.get(vid).to_vec())
                    .and_then(|b| enchudb_oplog::oplog::decode_sync_ops_payload(&b))
                {
                    bump(rec.author_peer, rec.hlc);
                }
            }
        }
        Some(out.into_iter().collect())
    }

    /// 0.7.0 (Phase 4): 全 peer の最小 consumed_lsn (= reclaim 安全点)。
    /// peer 0 件なら 0 を返す (= 「まだ誰も ack してない、 reclaim 不可」)。
    pub fn sync_watermark(&self) -> u32 {
        if !self.sync_tables_enabled() { return 0; }
        let Some(peer_id_hid) = self.himo_id("_sync_peers.peer_id") else { return 0; };
        let Some(consumed_lsn_hid) = self.himo_id("_sync_peers.consumed_lsn") else { return 0; };

        let peer_rows = self.entities_with_himo(peer_id_hid as u16);
        if peer_rows.is_empty() { return 0; }

        // #149: 自分自身の peer row は watermark から除外する。 自分が自分の record を
        // 「持っている」のは自明（著者本人）で、 self row は pull で前進する機会が無い
        // ため、 一度でも作られると（過去の self-ack 経路の残骸等）min を永久に固定して
        // reclaim を殺す（実機発現: watermark が古い self row に張り付いてリング満杯）。
        let self_peer = self.peer_id();

        let mut min_lsn = u32::MAX;
        let mut counted = false;
        for eid in peer_rows {
            if self_peer != 0 {
                if let Some(pid) = self.get_by_id(eid, peer_id_hid as u16) {
                    if pid == self_peer { continue; }
                }
            }
            if let Some(v) = self.get_by_id(eid, consumed_lsn_hid as u16) {
                counted = true;
                if v < min_lsn { min_lsn = v; }
            }
        }
        if !counted { return 0; }
        min_lsn
    }

    /// 0.7.0 (Phase 4): `_sync_ops` の `lsn < watermark` row を削除する。
    /// 0.7.0 lazy purge 方針: entity delete のみ、 eid 空間は再利用しない
    /// (= 0.8.0 で ring buffer 化検討)。 返り値: 削除した row 数。
    pub fn reclaim_sync_ops(&self) -> usize {
        if !self.sync_tables_enabled() { return 0; }
        let watermark = self.sync_watermark();
        if watermark == 0 { return 0; }

        let Some(lsn_hid) = self.himo_id("_sync_ops.lsn") else { return 0; };
        let lsn_hid_u16 = lsn_hid as u16;

        // _sync_ops 全 row を走査して lsn < watermark を delete
        let rows = self.entities_with_himo(lsn_hid_u16);
        let payload_hid = self.himo_id("_sync_ops.payload").map(|h| h as u16);
        let mut purged = 0;
        // #191: purge した record の最大 HLC = 「差分 pull で配れない履歴の上限」。
        // publish 側はこれを history floor として広告する (生存 record の最小 HLC を
        // 使うと、消化完了直後の正常な cursor まで gap と誤認する)。
        // #216: **author 別**に最大を取る — relay 混在 ring では scalar floor が
        // 「author a の cursor は新しいのに、 author b の reclaim で floor が上がって
        // 恒常的に truncation 判定」の false positive を作るため (per-author cursor の
        // 双対として floor も per-author が要る)。
        let mut reclaimed_max: std::collections::HashMap<u32, enchudb_oplog::Hlc> =
            std::collections::HashMap::new();
        // #235: **decode 不能かつ最新 lsn の row** = 書き込み途中の bridge row。
        // 通常は watermark がそこまで届かないので到達しないが、 `ack_sync` は生の
        // lsn を受ける public API なので、 caller が実在より先を ack すると
        // watermark が未完成 row を跨ぐ。 reclaim は decode 可否を見ずに削除して
        // いたので、 そのとき `ack_sync_prefix` と同じ silent loss になった。
        //
        // 条件を 「最新 lsn」 だけにすると、 完成済みの最新 row も永久に残って
        // reclaim の意味論が変わる (`ack_with_future_lsn_does_not_corrupt_reclaim`)。
        // 危険なのは 「未完成」 と 「破損」 を区別できない一点なので、 両方を満たす
        // row だけを避ける — `ack_sync_prefix` の dead-row 分岐と同じ粒度。
        let inflight_lsn = self.current_sync_lsn();
        // #218: dead row の生の `_sync_ops.peer_id`。 帰属の確定は**走査後**
        // (下の resolve ブロック参照) だが、 値は delete 前にしか読めないのでここで拾う。
        let mut dead_raw_authors: Vec<u32> = Vec::new();
        let dead_peer_id_hid = self.himo_id("_sync_ops.peer_id").map(|h| h as u16);
        for eid in rows {
            let Some(lsn) = self.get_by_id(eid, lsn_hid_u16) else { continue };
            if lsn >= watermark {
                continue;
            }
            let decoded = payload_hid
                .and_then(|h| self.get_by_id(eid, h))
                .map(|vid| self.vocab.get(vid).to_vec())
                .and_then(|b| enchudb_oplog::oplog::decode_sync_ops_payload(&b));
            if decoded.is_none() && lsn >= inflight_lsn {
                continue;
            }
            // #218: floor は **decode できた row だけ**で作っていた = 過少申告。
            // decode 不能な row も消す以上、 その分の履歴は差分 pull で配れない。
            match &decoded {
                Some(rec) => {
                    let e = reclaimed_max
                        .entry(rec.author_peer)
                        .or_insert(enchudb_oplog::Hlc::ZERO);
                    if rec.hlc > *e {
                        *e = rec.hlc;
                    }
                }
                None => dead_raw_authors.push(
                    dead_peer_id_hid
                        .and_then(|h| self.get_by_id(eid, h))
                        .unwrap_or(u32::MAX),
                ),
            }
            // 0.8.0: free list に追加 (= 次回 entity_in("_sync_ops") で再利用)。
            // #221: delete + push は専用 lock 下で atomic に (並行 reclaim の二重
            // push = slot 二重払い出し → bridge row の silent 上書き、を塞ぐ)。
            if self.purge_sync_ops_row(eid, lsn_hid_u16, lsn) {
                purged += 1;
            }
        }
        // #218: dead row の帰属は **走査後**に確定する。 reclaim は decodable な row も
        // 消すので、 dead row を見た時点では 「同じ author の裏を取れる row」 が既に
        // 消えていることがある (走査順は lsn 順ではない)。 裏取りの材料は
        // `reclaimed_max` の key (この走査で decode できた author) と、 生き残った row
        // から作る `ring_authors()`。 後者は全走査なので、 生の peer_id が前者だけで
        // 説明できないときにだけ作る。
        if !dead_raw_authors.is_empty() {
            let mut known: std::collections::HashSet<u32> =
                reclaimed_max.keys().copied().collect();
            known.insert(self.peer_id());
            // 読めなかった row (`u32::MAX`) はどのみち baseline なので、 そのためだけに
            // 全走査しない。
            if dead_raw_authors
                .iter()
                .any(|a| *a != u32::MAX && !known.contains(a))
            {
                known.extend(self.ring_authors());
            }
            let hlc = self.mint_local_hlc();
            for raw in dead_raw_authors {
                let author = if known.contains(&raw) { raw } else { u32::MAX };
                let e = reclaimed_max
                    .entry(author)
                    .or_insert(enchudb_oplog::Hlc::ZERO);
                if hlc > *e {
                    *e = hlc;
                }
            }
        }
        if !reclaimed_max.is_empty() {
            let entries: Vec<(u32, enchudb_oplog::Hlc)> =
                reclaimed_max.into_iter().collect();
            self.record_reclaimed_floors(&entries);
        }
        purged
    }

    /// #191: reclaim で `_sync_ops` から消した record の最大 HLC (全 author 横断)。
    ///
    /// `cursor >= floor` の peer は「reclaim で消えた分を全部消化済み」なので
    /// 差分 pull を続けて良い。`cursor < floor` の peer だけが bootstrap 対象。
    /// `_sync_peers` の sentinel row (`reclaimed_floor` Leaf) に保存する。row は
    /// body mmap なので reclaim の delete と durability の運命を共有する
    /// (crash で両方巻き戻れば pre-reclaim 状態として整合する)。
    ///
    /// #216: 内部表現は author 別 map (v2)。 本 API は互換の scalar view
    /// (= 全 entry の max)。 author 別は [`Engine::sync_reclaimed_floors`]。
    pub fn sync_reclaimed_floor(&self) -> Option<enchudb_oplog::Hlc> {
        let entries = self.read_reclaimed_floor_entries()?;
        entries.iter().map(|(_, h)| *h).max()
    }

    /// #216: reclaim floor の author 別 view。 relay 混在 ring では scalar floor が
    /// 「author a の cursor は新しいのに author b の reclaim で恒常 truncation」の
    /// false positive を作るため、 puller は author 別に `cursor[a] < floor[a]` で
    /// 判定する (per-author cursor の双対)。
    ///
    /// **`(u32::MAX, h)` の entry は「無帰属 baseline」** — 旧 (scalar) 書式時代に
    /// purge した row の最大 HLC で、 どの author の分かは失われている。 puller は
    /// これを**全 author に対する下限**として畳み込むこと
    /// (`effective_floor[a] = max(entry[a], baseline)`)。 これは sound
    /// (`cursor[a] >= max` なら upgrade 前分 ≤ baseline も後分 ≤ entry[a] も消化済み)
    /// かつ scalar max fallback より厳密に tight — baseline を越えた cursor の
    /// author から順に per-author 精度が戻るので、 sentinel を消さなくても自然に
    /// retire する。 このため **peer id `u32::MAX` は author として使用不可** (予約)。
    ///
    /// `None` = floor 記録なし (一度も reclaim していない)。
    pub fn sync_reclaimed_floors(&self) -> Option<Vec<(u32, enchudb_oplog::Hlc)>> {
        self.read_reclaimed_floor_entries()
    }

    /// floor row の生 entry 列 (author, hlc)。 legacy 16B scalar は
    /// `author = u32::MAX` (無帰属 sentinel) の 1 entry として読む。
    /// v2 書式: `[0xF2, 0x01, count: u16 LE][author u32 | wall u64 | logical u32 | peer u32 (全て BE)]*`
    fn read_reclaimed_floor_entries(&self) -> Option<Vec<(u32, enchudb_oplog::Hlc)>> {
        if !self.sync_tables_enabled() {
            return None;
        }
        let hid = self.himo_id("_sync_peers.reclaimed_floor")? as u16;
        let row = self.entities_with_himo(hid).into_iter().next()?;
        let vid = self.get_by_id(row, hid)?;
        let bytes = self.vocab.get(vid).to_vec();
        let hlc_at = |b: &[u8]| -> Option<enchudb_oplog::Hlc> {
            Some(enchudb_oplog::Hlc {
                wall: u64::from_be_bytes(b[0..8].try_into().ok()?),
                logical: u32::from_be_bytes(b[8..12].try_into().ok()?),
                peer: u32::from_be_bytes(b[12..16].try_into().ok()?),
            })
        };
        if bytes.len() == 16 {
            // legacy scalar (#191 初版)
            return Some(vec![(u32::MAX, hlc_at(&bytes)?)]);
        }
        if bytes.len() < 4 || bytes[0] != 0xF2 || bytes[1] != 0x01 {
            return None;
        }
        let count = u16::from_le_bytes(bytes[2..4].try_into().ok()?) as usize;
        if bytes.len() != 4 + count * 20 {
            return None;
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let off = 4 + i * 20;
            let author = u32::from_be_bytes(bytes[off..off + 4].try_into().ok()?);
            out.push((author, hlc_at(&bytes[off + 4..off + 20])?));
        }
        Some(out)
    }

    /// #191/#216: reclaimed floor を author 別に単調 max で merge する (下がる
    /// ことは無い)。 himo は lazy に ensure する (fix 前 DB の reopen でも育つ)。
    /// legacy scalar が残っていれば無帰属 sentinel (`u32::MAX`) entry として
    /// 温存する — 帰属不明の上限を落とすと silent gap 側に倒れるため。 puller は
    /// sentinel を全 author への baseline として畳み込む
    /// ([`Engine::sync_reclaimed_floors`] の doc 参照)。 **`u32::MAX` は author の
    /// peer id として予約済み** (実 author に使うと legacy baseline と誤分類される)。
    fn record_reclaimed_floors(&self, candidates: &[(u32, enchudb_oplog::Hlc)]) {
        let mut merged: std::collections::HashMap<u32, enchudb_oplog::Hlc> = self
            .read_reclaimed_floor_entries()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let mut changed = false;
        for (a, h) in candidates {
            let e = merged.entry(*a).or_insert(enchudb_oplog::Hlc::ZERO);
            if *h > *e {
                *e = *h;
                changed = true;
            }
        }
        if !changed {
            return;
        }
        let hid = match self.ensure_himo_dynamic_in(
            "_sync_peers",
            "reclaimed_floor",
            ValueType::Leaf,
            0,
        ) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("[enchudb] warning: reclaimed_floor himo unavailable ({e}) — history floor will over-approximate after restart");
                return;
            }
        };
        let row = match self.entities_with_himo(hid).into_iter().next() {
            Some(r) => r,
            None => match self.entity_in("_sync_peers") {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[enchudb] warning: reclaimed_floor row unavailable ({e})");
                    return;
                }
            },
        };
        let mut entries: Vec<(u32, enchudb_oplog::Hlc)> = merged.into_iter().collect();
        entries.sort_by_key(|(a, _)| *a);
        let mut bytes = Vec::with_capacity(4 + entries.len() * 20);
        bytes.push(0xF2);
        bytes.push(0x01);
        bytes.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        for (a, h) in &entries {
            bytes.extend_from_slice(&a.to_be_bytes());
            bytes.extend_from_slice(&h.wall.to_be_bytes());
            bytes.extend_from_slice(&h.logical.to_be_bytes());
            bytes.extend_from_slice(&h.peer.to_be_bytes());
        }
        self.tie_bytes_to_by_id(row, hid, &bytes);
    }

    /// #140: 自 peer が author した **live state** を bridge と同語彙の wire record
    /// として合成する。 ring (`_sync_ops`) が「最近の差分」なのに対し、 これは
    /// 「現在状態の転写」 — truncated puller の bootstrap 用 (`Syncer::serve_state`
    /// 経由で transport に登録され、 `Syncer::bootstrap_pull` が適用する)。
    ///
    /// - HLC は v9 per-cell version column の真値 (`version_of`)。 版数不明 (ZERO)
    ///   の cell は `as_of` で stamp する (単一 author cell では現在値が最新なので
    ///   LWW 的に安全)
    /// - `as_of` は合成**開始前**に採番する。 合成中の並行 write は HLC > as_of に
    ///   なるので、 適用後の cursor = as_of からの差分 pull で必ず拾える
    /// - 語彙は bridge (`transfer_oplog_to_sync_ops`) と同じ: Number = `Tie`、
    ///   Tag = `Vocab` + `Tie`、 Leaf = `TieLeaf` (bytes 同乗)、 Ref は translated
    ///   target のみ `TieRef` (世界番号同乗、 #183)、 自 entity target は `Tie`
    /// - **含まないもの** (v1): 署名 (signature = zeros — require_signature な受信側
    ///   では使えない)、 content blob、 他 peer 行への self-authored write
    ///   (translated local 行は skip)、 untie 済み cell の tombstone
    ///
    /// 戻り値 `(records, as_of)`。 oplog 無効 (standalone) では `as_of` が採番
    /// できないため空を返す。
    pub fn state_records(
        &self,
    ) -> (Vec<crate::transport::WireRecord>, enchudb_oplog::Hlc) {
        self.state_records_for(self.peer_id())
    }

    /// #226: `author` が author した live state を合成する — `state_records` の
    /// **replica 版**。 relay (gossip) は author の行を translated local として
    /// 保持しているので、 原 eid / 原 author / 原 HLC に**戻して**配れる。
    /// これが無いと relay 経由でしか author に届かない follower は
    /// `history_truncated` から回復できない (#140 は author 直結しか救えなかった)。
    ///
    /// `author == peer_id()` なら `state_records` と同一。 それ以外は:
    ///
    /// - 対象は `translated_locals_of(author)` の local のみ。 eid は原 eid
    ///   (`make_eid(author, foreign_local)`)、 `author_peer` は `author`。
    /// - HLC は cell の版数をそのまま。 remote apply は `set_cell(.., hlc)` 経由で
    ///   **author の HLC を版数に書いている**ので、 relayed cell の版数 = 原 HLC。
    ///   版数不明 (ZERO) の cell は **配らない** — foreign author の cell に自分の
    ///   clock を stamp する権利が無く、 ZERO のまま送ると LWW が順序を付けられない。
    /// - `as_of` は emit した record の **max HLC**。 self 版のように `mint_hlc()`
    ///   すると自分の clock で author の HLC 空間を進めてしまい、 受信側の
    ///   `cursor[author]` が author の後続 record を飛び越す (#216 で cursor が
    ///   author 別になったのでここが直撃する)。
    /// - Tag は **author の vid 空間に戻して**配る (`peer_vocab_map` の逆引き)。
    ///   自分の local vid のまま `(author, vid)` として配ると、 author 直 pull で
    ///   来る同じ key の別テキストと衝突して vocab 写像が壊れる (#209 と同種)。
    ///   逆引きできない cell は配らない。
    ///
    /// 呼び元は `StateBatch.complete` を **false** にすること — relay が author の
    /// live state を全部持っている保証は無い (途中から relay を始めた場合)。
    ///
    /// # replica 発 batch は **cell 単位でも欠けうる** (#236)
    ///
    /// `complete: false` は 「row を全部持っている保証が無い」 だけでなく、
    /// **持っている row の中でも配れない cell がある**ことまで含む。 replica 経路が
    /// cell を落とす条件は 4 つ (どれも上記のとおり個別には正しい判断):
    ///
    /// | 条件 | 理由 |
    /// |---|---|
    /// | 版数が `Hlc::ZERO` | foreign cell に自分の clock を stamp する権利が無い |
    /// | Tag の vid を逆引きできない | author の vid 空間に戻せない |
    /// | Ref の target を `reverse` できない | 元 entity を導けない |
    /// | Ref の値が translated local でない | author の行が自分の entity を指すことはない |
    ///
    /// 落とした件数は [`Engine::state_records_dropped`] に載る。 **基本の relay 経路
    /// では 0 件** (`replica_state_matches_author_state` が author 本人の wire 形との
    /// 集合一致を要求している) なので、 増えていたら到達経路 (版数 ZERO の cell /
    /// relay 自身の write-back) を疑うこと。 欠けた分の回復手段は author 直 bootstrap。
    ///
    /// `himo_id` は自分の番号をそのまま載せる。 これは wire format 全体の前提と
    /// 同じ (`Tie` の himo_id は受信側でそのまま使われる = peer 間で himo 番号が
    /// 一致している前提) なので、 replica 経路が新しく持ち込む仮定ではない。
    pub fn state_records_for(
        &self,
        author: enchudb_oplog::PeerId,
    ) -> (Vec<crate::transport::WireRecord>, enchudb_oplog::Hlc) {
        use enchudb_oplog::oplog::DecodedOp;
        let Some(wal) = self.oplog.as_ref() else {
            return (Vec::new(), enchudb_oplog::Hlc::ZERO);
        };
        let self_peer = self.peer_id();
        let is_self = author == self_peer;
        // self: 合成**開始前**に採番 (合成中の write は HLC > as_of なので差分で拾える)。
        // foreign: 自分の clock を author の HLC 空間に混ぜないため、 emit 後に max を採る。
        let as_of = if is_self { wal.mint_hlc() } else { enchudb_oplog::Hlc::ZERO };

        // foreign author の行 = translated local。 local -> foreign_local の写像。
        let foreign: std::collections::HashMap<u32, u32> = if is_self {
            std::collections::HashMap::new()
        } else {
            self.translated_locals_of(author).into_iter().map(|(f, l)| (l, f)).collect()
        };
        if !is_self && foreign.is_empty() {
            return (Vec::new(), enchudb_oplog::Hlc::ZERO);
        }
        // Tag: local vid -> author の remote vid (逆引き)。
        let vid_back: std::collections::HashMap<u32, u32> =
            if is_self { std::collections::HashMap::new() } else { self.remote_vid_reverse_of(author) };

        let mut records: Vec<crate::transport::WireRecord> = Vec::new();
        // #236: 「配れないと判断して落とした cell」 の件数。 判断はどれも正しいが、
        // 記録が無いと 「この replica の state は cell 単位で欠けている」 が
        // 観測できない ([`Engine::state_records_dropped`])。
        let mut dropped: u64 = 0;
        let mut vocab_sent: std::collections::HashSet<u32> = std::collections::HashSet::new();
        let mk = |op: DecodedOp, hlc: enchudb_oplog::Hlc| crate::transport::WireRecord {
            hlc,
            author_peer: author,
            op,
            signature: [0u8; 64],
            pubkey_fp: [0u8; 8],
            signed_bytes: Vec::new(),
        };

        for hid in 0..self.himos.len() {
            if self.himo_is_in_reserved_table(hid) {
                continue;
            }
            let himo_id = hid as u16;
            let vt = self.value_types[hid];
            for eid in self.entities_with_himo(himo_id) {
                let local = enchudb_oplog::eid_local(eid);
                // 出力する eid。 self 版は自分が author した行だけ (translated local は
                // 他 peer が author なので skip、 self-authored cross-row write は v1
                // 対象外)。 replica 版はその逆で、 author の translated local だけを
                // **原 eid に戻して**配る (#226)。
                let out_eid = if is_self {
                    if self.eid_translator.is_translated_local(local) {
                        continue;
                    }
                    eid
                } else {
                    let Some(&foreign_local) = foreign.get(&local) else { continue };
                    enchudb_oplog::make_eid(author, foreign_local)
                };
                let Some(value) = self.get_by_id(eid, himo_id) else { continue };
                let mut hlc = self.version_of(local, himo_id);
                if hlc == enchudb_oplog::Hlc::ZERO {
                    // self: 現在値が最新なので `as_of` stamp で LWW 的に安全。
                    // replica: foreign cell に自分の clock を stamp する権利が無い
                    // (as_of も ZERO)。 順序を付けられない record は配らない。
                    if !is_self {
                        dropped += 1; // #236
                        continue;
                    }
                    hlc = as_of;
                }
                match vt {
                    ValueType::Number => {
                        records.push(mk(DecodedOp::Tie { eid: out_eid, himo_id, value }, hlc));
                    }
                    ValueType::Ref => {
                        // bridge と同じ規則: translated foreign target は世界番号
                        // 同乗の TieRef、 自 entity target は素の Tie (受信側が
                        // author key で翻訳する)。 replica 版では「author 自身の
                        // entity への ref」も translated local なので、 owner が
                        // author なら素の Tie に戻す (= author 本人が出す形と同じ)。
                        if self.eid_translator.is_translated_local(value) {
                            match self.eid_translator.reverse(value) {
                                Some((owner, owner_local)) if owner == author && !is_self => {
                                    records.push(mk(
                                        DecodedOp::Tie {
                                            eid: out_eid,
                                            himo_id,
                                            value: owner_local,
                                        },
                                        hlc,
                                    ));
                                }
                                Some((owner, owner_local)) => {
                                    records.push(mk(
                                        DecodedOp::TieRef {
                                            eid: out_eid,
                                            himo_id,
                                            target: enchudb_oplog::make_eid(owner, owner_local),
                                        },
                                        hlc,
                                    ));
                                }
                                // 逆写像なし = 元 entity を導けない。 bridge と同じく
                                // 発送しない (silent 断片化させるより欠けを明示)。
                                None => {
                                    dropped += 1; // #236
                                    continue;
                                }
                            }
                        } else if is_self {
                            records.push(mk(DecodedOp::Tie { eid: out_eid, himo_id, value }, hlc));
                        } else {
                            // replica: author の行が「自分が author した entity」を
                            // 指すことはない (それは翻訳先を持つ)。 導けないので skip。
                            dropped += 1; // #236
                            continue;
                        }
                    }
                    ValueType::Tag => {
                        // replica は author の vid 空間に戻す。 戻せない vid は
                        // 配らない — 自分の local vid を `(author, vid)` として送ると
                        // author 直 pull の同 key と衝突して写像が壊れる。
                        let out_vid = if is_self {
                            value
                        } else {
                            match vid_back.get(&value) {
                                Some(v) => *v,
                                None => {
                                    dropped += 1; // #236
                                    continue;
                                }
                            }
                        };
                        if vocab_sent.insert(out_vid) {
                            let bytes = self.vocab.get(value).to_vec();
                            records.push(mk(DecodedOp::Vocab { vid: out_vid, bytes }, hlc));
                        }
                        records
                            .push(mk(DecodedOp::Tie { eid: out_eid, himo_id, value: out_vid }, hlc));
                    }
                    ValueType::Leaf => {
                        let Some(bytes) = self.text_owned_by_id(hid, local) else { continue };
                        let Some(name) = self.himo_names.get(hid) else { continue };
                        records.push(mk(
                            DecodedOp::TieLeaf {
                                eid: out_eid,
                                himo_name: name.clone(),
                                himo_kind: ValueType::Leaf as u8,
                                bytes,
                            },
                            hlc,
                        ));
                    }
                }
            }
        }
        if dropped > 0 {
            // #236: 呼び出し側は差分を見て 「この batch は cell 単位で欠けている」 を
            // 判定する (`Syncer::refresh_state_providers` の provider が前後で読む)。
            self.state_records_dropped
                .fetch_add(dropped, std::sync::atomic::Ordering::Relaxed);
        }
        // replica 版の as_of = emit した record の max HLC (self 版は採番済み)。
        let as_of = if is_self {
            as_of
        } else {
            records.iter().map(|r| r.hlc).max().unwrap_or(enchudb_oplog::Hlc::ZERO)
        };
        (records, as_of)
    }

    /// #226: `author` の remote vid → local vid 写像 (`peer_vocab_map`) の逆引き。
    /// replica が Tag cell を author の vid 空間に戻すために使う。 写像は実質
    /// 単射 (vocab は dedupe 済み) なので衝突は起きない想定だが、 万一重複したら
    /// 小さい remote vid を採って決定的にする。
    fn remote_vid_reverse_of(
        &self,
        author: enchudb_oplog::PeerId,
    ) -> std::collections::HashMap<u32, u32> {
        let map = self.peer_vocab_map.read().unwrap();
        let mut out: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
        for ((peer, remote_vid), local_vid) in map.iter() {
            if *peer != author {
                continue;
            }
            let e = out.entry(*local_vid).or_insert(*remote_vid);
            if *remote_vid < *e {
                *e = *remote_vid;
            }
        }
        out
    }

    /// #226: 自 store に replica (translated local) を持っている author 一覧。
    /// relay が `serve_state` で「どの author の state を配れるか」を決めるのに使う。
    /// pull ごとに引かれるので、 写像全体の走査ではなく translator 側の索引を見る。
    pub fn replicated_authors(&self) -> Vec<enchudb_oplog::PeerId> {
        self.eid_translator.authors()
    }

    /// #140: `author` peer の行として翻訳済みの local slot 一覧
    /// `(foreign_local, local)`。 bootstrap 後の ghost sweep (state に現れなかった
    /// author の行 = author 側で削除済み) の走査に使う。
    pub fn translated_locals_of(&self, author: enchudb_oplog::PeerId) -> Vec<(u32, u32)> {
        self.eid_translator
            .snapshot()
            .into_iter()
            .filter(|(p, _, _)| *p == author)
            .map(|(_, foreign_local, local)| (foreign_local, local))
            .collect()
    }

    /// 0.7.0 (Phase 5): 現時点の sync lsn (= 次に転送される record の lsn - 1)。
    /// `transfer_oplog_to_sync_ops` で割当て済みの max lsn。 snapshot 取得時に
    /// 「snapshot 時点でここまで配信済み」 を表すマーカーとして使う。
    pub fn current_sync_lsn(&self) -> u32 {
        self.next_sync_lsn.load(std::sync::atomic::Ordering::Acquire).saturating_sub(1)
    }

    /// #140: `_sync_ops` から既に reclaim された履歴があるか。
    ///
    /// `reclaim_sync_ops` は `lsn < sync_watermark()` の row を purge する。 watermark は
    /// **登録済み peer** の consumed_lsn の最小値なので、 `_sync_peers` に居ない新規 peer が
    /// 必要とする履歴も落ちる。 その状態で cursor 0 のフル pull を受けると、 部分履歴を
    /// 「全履歴」として配ってしまう (= #140)。
    ///
    /// 判定は `_sync_ops` の内容から導出できるので**新たな永続化は不要**:
    /// lsn は 1 始まりなので、 生存 row の最小 lsn が 1 より大きければ古い分が purge 済み。
    /// row が空でも publish 実績 (`current_sync_lsn() > 0`) があれば全部 purge 済み。
    /// `next_sync_lsn` は open 時に `_sync_ops` / `_sync_peers.consumed_lsn` から
    /// rehydrate されるので、 この判定は reopen をまたいでも成立する。
    pub fn sync_history_reclaimed(&self) -> bool {
        if !self.sync_tables_enabled() { return false; }
        let published = self.current_sync_lsn();
        if published == 0 {
            return false; // 一度も publish していない = 落ちた履歴も無い
        }
        match self.min_sync_ops_lsn() {
            // 生存 row があるなら、 最小 lsn が 1 を超えている分だけ古い履歴が落ちている
            Some(min_lsn) => min_lsn > 1,
            // 空 + publish 実績あり = 全部 reclaim 済み
            None => true,
        }
    }

    /// #140: `_sync_ops` に生存している row の最小 lsn。 空なら None。
    pub fn min_sync_ops_lsn(&self) -> Option<u32> {
        if !self.sync_tables_enabled() { return None; }
        let lsn_hid = self.himo_id("_sync_ops.lsn")? as u16;
        let mut min_lsn: Option<u32> = None;
        for eid in self.entities_with_himo(lsn_hid) {
            if let Some(l) = self.get_by_id(eid, lsn_hid) {
                min_lsn = Some(match min_lsn {
                    Some(m) if m <= l => m,
                    _ => l,
                });
            }
        }
        min_lsn
    }

    /// 0.7.0 (Phase 4): `_sync_ops` の `lsn > since_lsn` row を全 himo set で
    /// 返す。 Syncer の publish_since が「peer.consumed_lsn より新しい op を
    /// 流す」 用途で呼ぶ。 返り値の各 entry は payload (= 完全 wire bytes)。
    pub fn pending_sync_ops(&self, since_lsn: u32) -> Vec<Vec<u8>> {
        if !self.sync_tables_enabled() { return Vec::new(); }
        let Some(lsn_hid) = self.himo_id("_sync_ops.lsn") else { return Vec::new(); };
        let Some(payload_hid) = self.himo_id("_sync_ops.payload") else { return Vec::new(); };
        let lsn_hid_u16 = lsn_hid as u16;
        let payload_hid_u16 = payload_hid as u16;

        let rows = self.entities_with_himo(lsn_hid_u16);
        let mut pairs: Vec<(u32, Vec<u8>)> = Vec::new();
        for eid in rows {
            let lsn = match self.get_by_id(eid, lsn_hid_u16) {
                Some(l) => l,
                None => continue,
            };
            if lsn <= since_lsn { continue; }
            let payload_vid = match self.get_by_id(eid, payload_hid_u16) {
                Some(v) => v,
                None => continue,
            };
            let bytes = self.vocab.get(payload_vid).to_vec();
            pairs.push((lsn, bytes));
        }
        // lsn 順
        pairs.sort_by_key(|(lsn, _)| *lsn);
        pairs.into_iter().map(|(_, b)| b).collect()
    }

    pub fn define_table(&mut self, name: &str, size_hint: u32) -> Result<TableId, String> {
        // 0.7.0: 公開 API。 `_` 始まりは reserved 命名空間、 user 経路では拒否。
        if is_reserved_table_name(name) {
            return Err(format!(
                "table name '{}' starts with '_' (reserved namespace); use define_reserved_table for internal tables",
                name,
            ));
        }
        self.define_table_inner(name, size_hint)
    }

    /// 0.7.0: engine / schema 層 internal 用の reserved table を作る。
    /// 名前は必ず `_` で始まること (`_schema_meta` / `_sync_ops` / `_sync_peers` 等)。
    /// `list_user_tables()` から除外される、 schema crate も公開 API では非露出。
    ///
    /// 用途:
    /// - schema crate: `_schema_meta` (schema blob / table 名 intern の置き場)
    /// - sync 経路 (issue #11): `_sync_ops` / `_sync_peers` (watermark + reclaim)
    pub fn define_reserved_table(&mut self, name: &str, size_hint: u32) -> Result<TableId, String> {
        if !is_reserved_table_name(name) {
            return Err(format!(
                "reserved table name '{}' must start with '_'",
                name,
            ));
        }
        self.define_table_inner(name, size_hint)
    }

    /// `define_table` / `define_reserved_table` の共通 path。 命名規約 check は
    /// 呼び元 (public API) で済ませる。
    fn define_table_inner(&mut self, name: &str, size_hint: u32) -> Result<TableId, String> {
        self.check_writable();
        if name.is_empty() {
            return Err("table name must be non-empty".into());
        }
        if self.tables.iter().any(|t| t.name == name) {
            return Err(format!("table '{}' already exists", name));
        }
        if self.tables.len() >= TableId::MAX as usize {
            return Err(format!("table count exceeds max ({})", TableId::MAX));
        }
        let size = if size_hint == 0 { DEFAULT_TABLE_RESERVED } else { size_hint };

        // 1) anonymous を現 next_eid で close (まだ open なら)
        let cur_next_eid = self.entities.next_eid();
        let anon = &mut self.tables[ANONYMOUS_TABLE as usize];
        if anon.eid_range_hi == u32::MAX {
            anon.eid_range_hi = cur_next_eid;
        }

        // 2) 新 table の eid 範囲を確保 (= 既存 table 群の hi の max から開始)
        let new_lo = self
            .tables
            .iter()
            .map(|t| t.last_hi())
            .max()
            .unwrap_or(0);
        let new_hi = new_lo
            .checked_add(size)
            .ok_or_else(|| "eid space overflow (u32::MAX)".to_string())?;
        if new_hi > self.max_entities() {
            return Err(format!(
                "table '{}' eid range [{}, {}) exceeds max_entities {} (remaining {}; see Engine::remaining_eid_capacity)",
                name, new_lo, new_hi, self.max_entities(),
                self.max_entities().saturating_sub(new_lo),
            ));
        }

        let tid = self.tables.len() as TableId;
        self.tables.push(TableDef {
            name: name.to_string(),
            himo_ids: std::sync::RwLock::new(Vec::new()),
            eid_range_lo: new_lo,
            eid_range_hi: new_hi,
            fk_refs: Vec::new(),
            next_local: std::sync::atomic::AtomicU32::new(0),
            free_locals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            free_locals_nonempty: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            // #141: PK は schema 層が build 後に `set_table_pk` で降ろす。
            extra: std::sync::RwLock::new(Vec::new()),
            pk_himo: None,
        });
        self.try_persist_tables();
        Ok(tid)
    }

    /// 指定 table 内に entity を割り当てる。 anonymous table 名は受け付けず、
    /// 旧来の `entity()` を使う必要がある (互換維持)。
    ///
    /// 0.7.0: `&self` 化。 `next_local` は AtomicU32 で CAS-safe に払出される
    /// ので、 schema crate / SQL 層の hot path (Arc<Engine> 経由の concurrent
    /// mode) からも呼べる。 capacity check は load → fetch_add の seq の
    /// rollback 不要 (= 1 ずつ進む単調払出、 overflow は次回 check で弾く)。
    pub fn entity_in(&self, table_name: &str) -> Result<enchudb_oplog::EntityId, String> {
        use std::sync::atomic::Ordering;
        self.check_writable();
        if table_name.is_empty() {
            return Err("entity_in: table name must be non-empty (use entity() for anonymous)".into());
        }
        let tid = self
            .tables
            .iter()
            .position(|t| t.name == table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;
        let table = &self.tables[tid];

        // 0.8.0: free list 優先 — reclaim で解放された local id があれば再利用
        // (= ring buffer 化、 `_sync_ops` の長期運用で eid 飽和を防ぐ)。
        // 再利用時は entities.allocate_at で live mark を戻す + concurrent
        // barrier も走らせる (= 新規 alloc path と同じ orchestration)。
        //
        // fast path: free_locals_nonempty が false (= user table の常態) なら
        // mutex を取らずに通常 alloc に進む。 1M insert hot path で mutex
        // overhead (= 約 50 ns/op) を消す最適化。
        let reused_global = if table.free_locals_nonempty.load(Ordering::Acquire) {
            let mut fl = table.free_locals.lock().unwrap();
            let popped = fl.pop();
            if fl.is_empty() {
                table.free_locals_nonempty.store(false, Ordering::Release);
            }
            popped.and_then(|local| table.global_of(local))
        } else {
            None
        };
        if let Some(global) = reused_global {
            // 再利用 slot には前の住人の版数が残っている。 払い出す前に落とす
            // (`clear_cell_versions` の doc 参照)。
            self.clear_cell_versions(global);
            self.entities.allocate_at(global);
            // #195: queue に積まず counter 対称 bump (entity() の同所コメント参照)。
            if self.write_queue.is_some() {
                self.push_count.fetch_add(1, Ordering::Release);
                self.apply_count.fetch_add(1, Ordering::Release);
            }
            let peer = self.peer_id.load(Ordering::Acquire);
            return Ok(enchudb_oplog::make_eid(peer, global));
        }

        // capacity check (= fetch_add 後の変換で extent の外なら枯渇)
        let cur = table.next_local.fetch_add(1, Ordering::AcqRel);
        let global = match table.global_of(cur) {
            Some(g) => g,
            None => {
                // 0.18.2: `free_locals` は in-memory のみで reopen で消える。 reclaim 済み
                // slot を持つ store を reopen すると、 range は「満杯」に見えるのに実際は
                // 穴だらけで、 ここが恒久 Err になる。 `_sync_ops` ではこれが
                // 「transfer が row を挿せない → 変更が sync から**無言で欠落**」として
                // 実機発現した (ring ~25K 全 reclaim 後の reopen で bridge 全停止)。
                // 枯渇時に一度 EntitySet の liveness から穴を再構築して自己修復する。
                if self.rebuild_free_locals(tid) {
                    // 穴が見つかった — free list 経由で取り直す。 再帰は一段で止まる:
                    // 再構築後も枯渇なら fl が空のままなので次は grow / Err に落ちる。
                    return self.entity_in(table_name);
                }
                // v10 Phase 3 (request20 案 B): 空き eid 空間があれば extent を切り足す。
                match self.grow_table_extent_for(tid, cur) {
                    Some(g) => g,
                    None => {
                        // 払出した分を rollback (= overflow 状態を維持しないため厳密には
                        // 必要だが、 単調 monotone な next_local なので少々超過しても
                        // 次回以降の check で確実に弾ける。 ここは error を返すのみ)。
                        return Err(format!(
                            "table '{}' eid range exhausted ({} eids reserved, entity cap {} — \
                             Engine::grow_entity_cap to add room)",
                            table_name,
                            table.capacity(),
                            self.max_entities(),
                        ));
                    }
                }
            }
        };

        // EntitySet で live mark + next_eid 前進 (CAS safe)
        self.entities.allocate_at(global);

        // concurrent mode barrier (entity() と同じ)。
        // #195: queue に積まず counter 対称 bump — consumer thread 自身が bridge 中に
        // ここを通る (blocking push だと自縄自縛 livelock)。
        if self.write_queue.is_some() {
            self.push_count.fetch_add(1, Ordering::Release);
            self.apply_count.fetch_add(1, Ordering::Release);
        }
        let peer = self.peer_id.load(Ordering::Acquire);
        Ok(enchudb_oplog::make_eid(peer, global))
    }

    /// 0.18.2: `free_locals` を EntitySet の liveness から再構築する（穴 = 割当済み
    /// range 内で live でない local）。 free list は in-memory のみで reopen で消える
    /// ため、 reclaim 済み slot を持つ store の reopen 後は range が「満杯」に見える —
    /// その枯渇時の自己修復パス（`entity_in` の slow path からのみ呼ばれる想定）。
    ///
    /// 戻り値: 穴が 1 つでも見つかったか。 lock を持ったまま scan するので、 並行する
    /// 枯渇 thread は再構築完了を待ってから free list を pop する（同じ穴の二重払い出し
    /// を防ぐ）。 range 全体の線形 scan だが、 呼ばれるのは枯渇時のみで hot path 外。
    fn rebuild_free_locals(&self, tid: usize) -> bool {
        use std::sync::atomic::Ordering;
        let table = &self.tables[tid];
        let mut fl = table.free_locals.lock().unwrap();
        if !fl.is_empty() {
            return true; // 別 thread が再構築済み
        }
        let allocated = table.next_local.load(Ordering::Acquire).min(table.capacity());
        let mut local = 0u32;
        'outer: for (lo, hi) in table.extents() {
            for global in lo..hi {
                if local >= allocated {
                    break 'outer;
                }
                if !self.entities.is_live(global) {
                    fl.push(local);
                }
                local += 1;
            }
        }
        let found = !fl.is_empty();
        if found {
            table
                .free_locals_nonempty
                .store(true, std::sync::atomic::Ordering::Release);
        }
        found
    }

    /// #9: persist 用に翻訳写像 + 各 entity の foreign tombstone HLC を集める。
    /// reopen 時に `.eidmap` v2 から復元され、 削除済み foreign entity の
    /// resurrection を防ぐ。 呼ばれるのは persist trigger (consumer tick / 明示
    /// persist) のみで read hot path 外。
    ///
    /// request17 step 6: tombstone の出所は `tombstone_version_of` に一本化した
    /// (v9 なら tombstone column、 pre-v9 なら従来の揮発 `HlcStore`)。 v9 では
    /// 版数が本体側に永続しているので sidecar は冗長だが、 pre-v9 DB と同じ
    /// sidecar を書き続けることで **v9 binary で書いた DB を pre-v9 の経路で
    /// 読んでも tombstone が失われない**。
    fn eidmap_entries_with_tombstones(&self) -> Vec<EidmapEntry> {
        let mut out: Vec<EidmapEntry> = self
            .eid_translator
            .snapshot()
            .into_iter()
            .map(|(peer, foreign_local, local)| {
                (peer, foreign_local, local, self.tombstone_version_of(local))
            })
            .collect();
        // #166: slot を手放した identity の削除記録も残す。 これが無いと reopen で
        // 「削除済み」 を忘れ、 削除より古い record の再配送で復活する。
        // 写像を持たないので `local` は番兵。
        out.extend(
            self.orphan_foreign_tombstones()
                .into_iter()
                .map(|(peer, foreign_local, tomb)| (peer, foreign_local, NO_LOCAL_SLOT, tomb)),
        );
        out
    }

    /// 0.8.1: `&self` で sidecar 群を強制 persist する public API。
    /// `Arc<Engine>` (= concurrent mode) でも `flush(&mut)` を取れない状況で
    /// `next_local` を含む tables 状態を disk に固める用途。
    ///
    /// 書くのは `.tables` / `.eidmap` / `.vocabmap` の 3 つ (= 翻訳 state は
    /// `next_local` と整合していないと意味が無いので同じ trigger で落とす)。
    /// **cell 本体は msync しない** — 受信 op を適用した後の barrier が要るなら
    /// [`Engine::persist_sync_state`] を使うこと。
    ///
    /// short-lived CLI (= 1 write → drop) で sinfo 等の embed consumer が
    /// 明示的に呼ぶ想定。 wasm / memory-only (= path 空) では Ok(()) no-op。
    /// persist 失敗時は呼び出し側に io::Error を返す (= `try_persist_tables`
    /// と違って best-effort ではない、 fail-fast)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn persist_tables(&self) -> io::Result<()> {
        if self.path.is_empty() {
            return Ok(());
        }
        // #190: serialize から rename までを直列化 (同一 tmp の truncate 合戦と
        // 新旧逆転 install の防止)。poisoned でも persist は続行して良い
        // (守っているのは file I/O の順序だけで、guard 下の共有 state は無い)。
        let _guard =
            self.sidecar_persist_lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        persist_tables_to_sidecar(&self.path, &self.tables)?;
        // #9: 翻訳テーブルも同じ trigger で persist (next_local と整合させる)。
        persist_eidmap_to_sidecar(&self.path, &self.eidmap_entries_with_tombstones())?;
        self.persist_vocab_map_if_dirty()
    }

    /// **sync 由来 state の durability barrier**。
    ///
    /// `body_msync()` (cell 本体) + [`Engine::persist_tables`] (sidecar 3 つ)。
    ///
    /// 受信 op を適用すると、 cell 本体のほかに 3 つの派生 state が動く:
    ///
    /// - `.tables` — `next_local` (翻訳先 slot の払い出し位置)
    /// - `.eidmap` — `(author_peer, foreign_local) → local` の entity 写像
    /// - `.vocabmap` — `(author_peer, remote_vid) → local_vid` の text 写像
    ///
    /// このうち後ろ 2 つは **memory から消えると復元手段が無い** (受信 op は自分の
    /// WAL に残らない)。 一方 `Syncer` の pull cursor は disk に永続するので、
    /// 「cursor は消費済みと言うが写像は無い」状態を作ると、 差分 pull では
    /// 二度と埋まらない (cursor が越えているので当該 record は再配送されない)。
    ///
    /// よって守るべき順序は **「消費した state を durable にしてから cursor を
    /// 進める」**。 `Syncer::pull_once` はこれを呼んでからでないと cursor を
    /// 前進させない。 失敗したら cursor は進まず、 次の pull で同じ record を
    /// 再適用する (apply は冪等: LWW と `get_or_insert`)。
    ///
    /// `.vocabmap` は dirty のときだけ書く (写像は単調増加なので「増えたか」で足りる)。
    /// memory-only (= path 空) / wasm では no-op。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn persist_sync_state(&self) -> io::Result<()> {
        if self.path.is_empty() {
            return Ok(());
        }
        // cell 本体を先に。 local write 経路が守っている順序 (`oplog_sync`:
        // WAL fsync → body msync → checkpoint 前進) の受信側 counterpart で、
        // pull cursor が受信側の checkpoint に当たる。 dirty range 単位なので
        // 変更量に比例する (request3)。
        self.body_msync()?;
        self.persist_tables()
    }

    /// `.vocabmap` を dirty のときだけ書く。 写像は単調増加なので「増えたか」で足りる。
    #[cfg(not(target_arch = "wasm32"))]
    fn persist_vocab_map_if_dirty(&self) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        if !self.peer_vocab_map_dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        persist_vocabmap_to_sidecar(&self.path, &self.peer_vocab_map_entries())?;
        self.peer_vocab_map_dirty.store(false, Ordering::Release);
        Ok(())
    }

    /// wasm 版 (sidecar を持たないので no-op)。
    #[cfg(target_arch = "wasm32")]
    pub fn persist_sync_state(&self) -> io::Result<()> {
        Ok(())
    }

    /// `.vocabmap` に落とす entry 列。
    fn peer_vocab_map_entries(&self) -> Vec<VocabmapEntry> {
        let map = self.peer_vocab_map.read().unwrap();
        let mut out: Vec<VocabmapEntry> =
            map.iter().map(|(&(peer, remote), &local)| (peer, remote, local)).collect();
        // 決定的な並びにしておく (diff / 再現性のため。 読み手は順序に依存しない)。
        out.sort_unstable();
        out
    }

    /// 0.8.7: DB ファイルパスを返す。 schema crate が schema sidecar
    /// (`{path}.schema`) を atomic write するために必要。 memory-only
    /// (= from_bytes) では空文字列。
    pub fn db_path(&self) -> &str {
        &self.path
    }

    /// β-light step 7: 現 tables Vec を sidecar に保存する (best effort)。
    /// memory-only (from_bytes) や wasm では no-op。 path が空なら skip。
    /// 0.8.2: `defer_tables_persist` が立ってる時 (schema crate の build phase)
    /// は no-op。 finish 時に explicit `persist_tables()` で 1 回 fsync する。
    fn try_persist_tables(&self) {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if self.defer_tables_persist.load(std::sync::atomic::Ordering::Acquire) {
                return;
            }
            if !self.path.is_empty() {
                // #190: persist_tables と同じ直列化 (consumer thread はこちらを通る)。
                let _guard = self
                    .sidecar_persist_lock
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Err(e) = persist_tables_to_sidecar(&self.path, &self.tables) {
                    // best effort: panic せずログだけ。 user table の定義は
                    // メモリには反映されてる、 次回 reopen で失われるだけ。
                    // 0.8.15 (issue #52): ENOSPC 等で consumer thread が毎 batch
                    // 失敗 → 同 warning がターミナル不能になる現象を回避するため、
                    // 1 秒 1 行の rate-limit を入れる。
                    self.warn_persist_failure_rate_limited(&e);
                }
                // #9: 翻訳テーブルも tables と同じ trigger で persist (next_local と
                // 整合させる)。 entries 空なら no-op (sync してない DB に file を作らない)。
                if let Err(e) =
                    persist_eidmap_to_sidecar(&self.path, &self.eidmap_entries_with_tombstones())
                {
                    self.warn_persist_failure_rate_limited(&e);
                }
                // text 写像も同格。 dirty のときだけ書く。
                if let Err(e) = self.persist_vocab_map_if_dirty() {
                    self.warn_persist_failure_rate_limited(&e);
                }
            }
        }
    }

    /// 0.8.15 (issue #52): persist 失敗 warning を 1 秒 1 行に rate-limit。
    /// 高頻度 write hot path で ENOSPC が起きると consumer thread が毎 batch
    /// (= 100ms 周期) eprintln してターミナル使用不能になっていた。
    #[cfg(not(target_arch = "wasm32"))]
    fn warn_persist_failure_rate_limited(&self, e: &io::Error) {
        use std::sync::atomic::Ordering;
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = self.last_persist_warn_ms.load(Ordering::Relaxed);
        // 1 秒以上経過してたら CAS で前進させて emit (= 競合時はもう片方が emit 担当)。
        if now_ms.saturating_sub(last) >= 1000
            && self
                .last_persist_warn_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            eprintln!(
                "warning: failed to persist tables sidecar: {} (rate-limited to 1/s)",
                e
            );
        }
    }

    /// 0.8.2: build phase の sidecar fsync 抑止 toggle。 schema crate の
    /// `Database::create → build×N → finish_*` で N×fsync を 1 回に圧縮する
    /// 内部 hook。 true 中は `try_persist_tables` が no-op、 false に
    /// 戻すタイミングで explicit に `persist_tables()` を呼ぶこと。
    /// 普通の Engine 直利用 (= schema 層なし) で叩く必要は無い。
    pub fn set_defer_tables_persist(&self, defer: bool) {
        self.defer_tables_persist.store(defer, std::sync::atomic::Ordering::Release);
    }

    /// β-light step 7: sidecar から復元した tables を採用、 himo_to_table も
    /// それに合わせて再構築する。 load_from_backing 後 caller で path 設定後に呼ぶ。
    #[cfg(not(target_arch = "wasm32"))]
    fn adopt_persisted_tables(&mut self, tables: Vec<TableDef>) {
        let himo_count = self.himos.len();
        // 全 himo を一旦 anonymous default に戻し、 sidecar に書かれてる
        // attach を上書きする (= sidecar が source of truth)。
        // `&mut self` (= open 時、 共有前) なので AppendVec ごと作り直して OK。
        let rebuilt: AppendVec<std::sync::atomic::AtomicU16> =
            AppendVec::with_capacity(self.himo_to_table.capacity().max(himo_count));
        for _ in 0..himo_count {
            let _ = rebuilt.push(std::sync::atomic::AtomicU16::new(ANONYMOUS_TABLE));
        }
        for (tid, table) in tables.iter().enumerate() {
            for &hid in table.himo_ids.read().unwrap().iter() {
                if (hid as usize) < himo_count {
                    rebuilt[hid as usize]
                        .store(tid as TableId, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        self.himo_to_table = rebuilt;
        self.tables = tables;
    }

    /// 既知 table を `(id, name, eid_range)` で列挙する。 試験用 / debug 用 API。
    /// 0.7.0 以降は **reserved table も含む** (= sync 経路 / schema crate 内部用)。
    /// user code に見せる場合は `list_user_tables` を使うこと。
    pub fn list_tables(&self) -> Vec<(TableId, String, u32, u32)> {
        self.tables
            .iter()
            .enumerate()
            .map(|(i, t)| (i as TableId, t.name.clone(), t.eid_range_lo, t.eid_range_hi))
            .collect()
    }

    /// 0.7.0: user table のみを列挙 (= anonymous と reserved `_*` を除外)。
    /// schema crate `list_tables()` / SQL `SHOW TABLES` 等の user 向け API で
    /// これを使う。
    pub fn list_user_tables(&self) -> Vec<(TableId, String, u32, u32)> {
        self.tables
            .iter()
            .enumerate()
            .filter(|(i, t)| *i != ANONYMOUS_TABLE as usize && !t.is_reserved())
            .map(|(i, t)| (i as TableId, t.name.clone(), t.eid_range_lo, t.eid_range_hi))
            .collect()
    }

    /// 0.8.6: 指定 table 名の `(eid_range_lo, eid_range_hi)` を引く。 schema
    /// crate の `Table::sum/count/group_sum` が table-scoped 集計を
    /// `sum_range` / `count_range` / `group_sum_range` に bind するのに使う。
    /// 未定義 table は None。
    pub fn table_eid_range(&self, name: &str) -> Option<(u32, u32)> {
        self.tables.iter().find(|t| t.name == name).map(|t| (t.eid_range_lo, t.last_hi()))
    }

    /// v10 Phase 3 (request20 案 B): table の eid extent 一覧 (`[lo, hi)` の列、 払出順)。
    /// auto-grow した table は 2 本以上になり、 間に他 table の eid が挟まる。 scan は
    /// `table_eid_range` (= hull) ではなくこちらで。
    pub fn table_eid_extents(&self, name: &str) -> Option<Vec<(u32, u32)>> {
        self.tables.iter().find(|t| t.name == name).map(|t| t.extents())
    }

    /// v10 Phase 3 (request20): table の枠を `extra` 個明示的に足す (末尾の空き eid 空間から)。
    /// 戻り値は新しい capacity。 空きが無ければ Err (`grow_entity_cap` で cap を伸ばす)。
    /// `entity_in` は枯渇時に同じことを自動でやるので、 通常は呼ばなくてよい。
    pub fn grow_table(&self, name: &str, extra: u32) -> Result<u32, String> {
        let tid = self
            .tables
            .iter()
            .position(|t| t.name == name)
            .ok_or_else(|| format!("table '{}' not found", name))?;
        if extra == 0 {
            return Ok(self.tables[tid].capacity());
        }
        let _g = self.table_grow_lock.lock().unwrap_or_else(|p| p.into_inner());
        let got = self.push_table_extent_locked(tid, extra);
        if got == 0 {
            return Err(format!(
                "table '{}': no free eid space (entity cap {} — Engine::grow_entity_cap)",
                name,
                self.max_entities(),
            ));
        }
        Ok(self.tables[tid].capacity())
    }

    /// `entity_in` の枯渇時: `local` (払出済み offset) が入るまで extent を足す。 足せた
    /// なら global を返す。 lock 下で再判定するので並行 thread の二重 grow はしない。
    fn grow_table_extent_for(&self, tid: usize, local: u32) -> Option<u32> {
        let table = &self.tables[tid];
        if table.is_open_ended() {
            return None;
        }
        let _g = self.table_grow_lock.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(g) = table.global_of(local) {
            return Some(g); // 別 thread が足した
        }
        // 既定は先頭 range と同じ大きさ、 最低でも `local` が入る分
        let first = table.eid_range_hi - table.eid_range_lo;
        let need = local.saturating_sub(table.capacity()).saturating_add(1);
        let got = self.push_table_extent_locked(tid, first.max(need).max(1));
        if got < need {
            return None;
        }
        table.global_of(local)
    }

    /// `table_grow_lock` 下で呼ぶ。 末尾の空き eid 空間から最大 `want` 個の extent を
    /// 足し、 実際に足せた数を返す (空きが無ければ 0、 sidecar は persist)。
    fn push_table_extent_locked(&self, tid: usize, want: u32) -> u32 {
        let new_lo = self.tables.iter().map(|t| t.last_hi()).max().unwrap_or(0);
        if new_lo == u32::MAX {
            return 0; // anonymous が open-ended (= named table 無し) — ここには来ない
        }
        let avail = self.max_entities().saturating_sub(new_lo);
        let size = want.min(avail);
        if size == 0 {
            return 0;
        }
        self.tables[tid].extra.write().unwrap().push((new_lo, new_lo + size));
        self.try_persist_tables();
        size
    }

    /// table の eid 枠の使用状況 (未定義 table は `None`)。
    ///
    /// 枠は create 時に固定なので、 **満杯にする前に気付く**のがアプリ側の唯一の
    /// 防御になる。 満杯後は `entity_in` が `Err` を返し、 そこで掃引を止めると
    /// 削除まで流れなくなって回復不能になる (削除は枠を空ける唯一の手段)。
    pub fn table_eid_usage(&self, name: &str) -> Option<TableEidUsage> {
        let t = self.tables.iter().find(|t| t.name == name)?;
        let capacity = t.capacity();
        let allocated = t
            .next_local
            .load(std::sync::atomic::Ordering::Acquire)
            .min(capacity);
        let live = t.extents().into_iter().map(|(lo, hi)| self.entities.live_count_in(lo, hi)).sum::<u32>();
        Some(TableEidUsage { capacity, allocated, live, free: capacity.saturating_sub(live) })
    }

    /// まだどの table にも割り当てていない eid 空間 (= これから `define_table` で
    /// 切り出せる上限)。
    ///
    /// `max_entities` は create 時に header へ焼かれるので、 後から table を足す
    /// アプリ (例: 追加の local-only table) はここを見てから `with_capacity` を
    /// 決める必要がある。 これが無いと 「既知の table 名の range を全部引いて
    /// 自分で引き算する」 しかなかった。
    pub fn remaining_eid_capacity(&self) -> u32 {
        // anonymous table は open (`hi == u32::MAX`) のことがある。 `define_table` は
        // それを現 `next_eid` で閉じてから max を取るので、 ここでも同じ数え方をする。
        let used = self
            .tables
            .iter()
            .map(|t| {
                if t.eid_range_hi == u32::MAX { self.entities.next_eid() } else { t.last_hi() }
            })
            .max()
            .unwrap_or(0);
        self.max_entities().saturating_sub(used)
    }

    /// request19: **local-only table の行を全部落とす** (snapshot / bootstrap の受け側用)。
    /// 戻り値は消した entity 数。
    ///
    /// local-only table (= `define_reserved_table` で作った `_` 始まりの table) は
    /// 「**この端末で観測した事実**」 を置く場所で、 peer には配らない
    /// (`transfer_oplog_to_sync_ops` が bridge から除外する)。 ところが
    /// `snapshot_export` は body を丸ごと写すので、 **snapshot にはその中身も乗る**。
    /// 受け取った側がそれを自分の観測として使うと嘘になるので、 **restore / bootstrap の
    /// 直後にこれを呼んで空にする**。
    ///
    /// engine 自身の sync 内部 table (`_sync_ops` / `_sync_peers`) は**対象外** —
    /// あちらは bootstrap で引き継ぐのが正しい (未配送 backlog と peer watermark)。
    pub fn clear_local_only_tables(&self) -> usize {
        self.check_writable();
        let ranges: Vec<(u32, u32)> = self
            .tables
            .iter()
            .filter(|t| t.is_reserved() && !is_engine_internal_table(&t.name))
            .flat_map(|t| t.extents())
            .collect();
        if ranges.is_empty() {
            return 0;
        }
        let mut cleared = 0usize;
        for local in self.entities.iter() {
            if !ranges.iter().any(|&(lo, hi)| local >= lo && local < hi) {
                continue;
            }
            // 版数を進めずに落とす — local-only なので LWW の相手が居ない。
            for hid in 0..self.himos.len() {
                self.free_leaf_cell(local, hid);
                self.himos[hid].remove(local);
            }
            self.entities.free(local);
            cleared += 1;
        }
        // 払い出し位置も戻す (= 空の table として始める)。
        for t in self.tables.iter() {
            if t.is_reserved() && !is_engine_internal_table(&t.name) {
                t.next_local.store(0, std::sync::atomic::Ordering::Release);
            }
        }
        cleared
    }

    /// #141: table の primary key himo を登録する。 schema 層 (`TableBuilder::build`)
    /// から降ろす専用の API。
    ///
    /// PK は本来 schema 層の概念だが、 sync の apply 経路 (`enchudb-sync`) は schema を
    /// 見られない (兄弟 crate)。 「受信 op の foreign entity を、 同じ PK の既存 row に
    /// 束ねる」判断を apply 側でするために engine が PK を知っている必要がある。
    ///
    /// 未定義 table / 範囲外 himo は `Err`。 `.tables` sidecar (v2) に永続化される。
    pub fn set_table_pk(&mut self, table: &str, himo_id: u16) -> Result<(), String> {
        if himo_id as usize >= self.himos.len() {
            return Err(format!("set_table_pk: himo_id {} out of range", himo_id));
        }
        let Some(t) = self.tables.iter_mut().find(|t| t.name == table) else {
            return Err(format!("set_table_pk: unknown table '{}'", table));
        };
        t.pk_himo = Some(himo_id);
        self.try_persist_tables();
        Ok(())
    }

    /// #141: table の PK himo id。 未設定なら None。
    pub fn table_pk_himo(&self, table: &str) -> Option<u16> {
        self.tables.iter().find(|t| t.name == table).and_then(|t| t.pk_himo)
    }

    /// #141: この himo がどれかの table の PK かどうか。 apply 経路の hot path から
    /// 呼ぶので、 table 数ぶんの線形走査で済む形にしてある (table 数は通常 1 桁)。
    pub fn is_pk_himo(&self, himo_id: u16) -> bool {
        self.tables.iter().any(|t| t.pk_himo == Some(himo_id))
    }

    /// #141: `(author_peer, foreign_eid)` → `local_eid` の翻訳を明示的に張る。
    ///
    /// 通常 `resolve_remote_eid` は未知の foreign entity に **新規 local eid を
    /// 払い出す**が、 PK 一致の既存 row が居る場合はそこへ束ねたい。 apply 側が
    /// PK lookup で行き先を決めてから、 この API で写像を固定する。
    ///
    /// 既に写像がある場合は **変更しない** (先に確定した束ね先を優先)。 戻り値は
    /// 実際に有効な local eid (既存写像があればそちら)。
    pub fn bind_remote_eid(
        &self,
        author_peer: enchudb_oplog::PeerId,
        foreign_eid: enchudb_oplog::EntityId,
        local_eid: enchudb_oplog::EntityId,
    ) -> enchudb_oplog::EntityId {
        use std::sync::atomic::Ordering;
        let self_peer = self.peer_id.load(Ordering::Acquire);
        if enchudb_oplog::eid_peer(foreign_eid) == self_peer {
            return foreign_eid; // identity: 自分が産んだ entity
        }
        let foreign_local = enchudb_oplog::eid_local(foreign_eid);
        let target_local = enchudb_oplog::eid_local(local_eid);
        let local = self
            .eid_translator
            .get_or_insert_with(author_peer, foreign_local, || Some(target_local))
            .unwrap_or(target_local);
        // #178: **束ねる前に自分が書いていた**なら、 その write は既に自分の eid のまま
        // bridge されている (宛名の付け替えは bridge 時に `reverse()` を引くため)。
        // 相手側はそれを別 entity として払い出すので PK 無しの重複行が生える。
        // ここでは直せない (出て行った record は取り消せない) ので、 **観測できるように
        // 数える**。 実地で静かに壊れていた経路。
        self.note_bind_over_local_writes(local, self_peer);
        // 束ね先の entity は live 扱いにしておく (remote_tie_apply と同じ前提)。
        self.entities.ensure_live(local);
        Self::advance_table_next_local_for(&self.tables, local);
        enchudb_oplog::make_eid(self_peer, local)
    }

    /// #178 検知の実体。 `local` の行に **自分が著者の cell** が 1 つでも在れば数える。
    ///
    /// 判定材料は cell の版数 (`Hlc::peer`) だけ — 別の state を持たないので、
    /// bind (= foreign entity ごとに一度) のときだけ O(himo 数) 走るコストで済む。
    fn note_bind_over_local_writes(&self, local: u32, self_peer: enchudb_oplog::PeerId) {
        use std::sync::atomic::Ordering;
        let mine = (0..self.himos.len()).any(|hid| {
            self.himos[hid].get_value(local).is_some()
                && self.version_of(local, hid as u16).peer == self_peer
        });
        if !mine {
            return;
        }
        self.bind_over_local_writes.fetch_add(1, Ordering::Relaxed);
        if !self.warned_bind_over_local_writes.swap(true, Ordering::Relaxed) {
            eprintln!(
                "[enchudb] warning: a row this peer had written was bound to a remote identity \
                 afterwards; writes made before the bind went out under this peer's own eid and \
                 may have created a duplicate row on the other side (#178). \
                 see Engine::bind_over_local_writes()"
            );
        }
    }

    /// #178: 「自分が書いた行が後から foreign identity に束ねられた」 累計回数。
    ///
    /// `> 0` なら、 相手側に **PK を持たない重複行**が生えている可能性がある
    /// (bind 前に出て行った write の分)。 監視用。 0 が常態。
    pub fn bind_over_local_writes(&self) -> u64 {
        self.bind_over_local_writes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 0.8.7: 登録済 himo の総数。 schema crate の synthesize fallback (= engine 直
    /// DB を `Database::open` した時の schema 復元) で iterate するために使う。
    pub fn himo_count(&self) -> usize {
        self.himo_names.len()
    }

    /// 0.8.7: index 直で himo の full name (= `{table}.{col}` or anonymous) を返す。
    pub fn himo_name_at(&self, idx: usize) -> Option<&str> {
        self.himo_names.get(idx).map(|s| s.as_str())
    }

    /// 0.8.7: index 直で himo の type (Tag/Number/Leaf/Ref) を返す。
    pub fn value_type_at(&self, idx: usize) -> Option<ValueType> {
        self.value_types.get(idx).copied()
    }

    /// 0.8.7: 指定 table の fk_refs を `(from_col_name, to_table_name)` ペアで返す
    /// (= schema synthesize fallback で relation を復元する用)。 himo id → 名前、
    /// table id → 名前を解決する。 未定義 table or fk_refs 空なら Vec::new()。
    pub fn fk_refs_for_table_named(&self, table_name: &str) -> Vec<(String, String)> {
        let Some(table) = self.tables.iter().find(|t| t.name == table_name) else {
            return Vec::new();
        };
        let prefix = format!("{}.", table_name);
        let mut out = Vec::with_capacity(table.fk_refs.len());
        for &(hid, tid) in &table.fk_refs {
            let Some(himo_full) = self.himo_names.get(hid as usize) else { continue; };
            let from_col = himo_full.strip_prefix(&prefix).unwrap_or(himo_full).to_string();
            let Some(target) = self.tables.get(tid as usize) else { continue; };
            out.push((from_col, target.name.clone()));
        }
        out
    }

    /// 0.7.0: 指定 table 名が reserved table として既に存在するか。 schema crate
    /// が `_schema_meta` を auto-define する idempotent path で使う。
    pub fn has_reserved_table(&self, name: &str) -> bool {
        is_reserved_table_name(name)
            && self.tables.iter().any(|t| t.name == name)
    }

    // ──── entity ────

    /// anonymous entity を払い出す。
    ///
    /// **`Err` を返す条件は 2 つ** (どちらも旧実装では panic だった、 #59):
    ///
    /// - **entity 枠が満杯** — 「DB が一杯」 は実行時の状態であって使い方の誤りではない。
    ///   embedded DB は他人の process に埋め込まれるので、 これで host を殺してはいけない。
    ///   `FaultKind::EntitySpace` として計数 + rate-limited warn もする。
    ///   空き枠は `remaining_eid_space()` で事前に見られる
    /// - **anonymous table が closed** (= `define_table` 済み) — この DB では
    ///   `entity_in("<table>")` を使うこと
    ///
    /// table 版の [`Engine::entity_in`] と同じ形 (`Result<_, String>`) にしてある。
    /// 同じ 「entity を作る」 操作が、 片方は Err で片方は process 即死、 という
    /// 非対称を無くすための signature 変更 (0.23.0 breaking)。
    pub fn entity(&self) -> Result<enchudb_oplog::EntityId, String> {
        use std::sync::atomic::Ordering;
        self.check_writable();
        // β-light step 3: anonymous table が closed (= 既に define_table が
        // 呼ばれた) なら entity() は使えない。 entity_in を使うこと。
        let anon_hi = self.tables[ANONYMOUS_TABLE as usize].eid_range_hi;
        if anon_hi != u32::MAX {
            return Err(
                "anonymous table is closed (define_table was called); \
                 use entity_in('<table>') instead of entity()"
                    .to_string(),
            );
        }
        // 上限到達後の `allocate` は free stack から slot を再利用する。
        // 再利用なら前の住人の版数を落としてから渡す (entity_in の再利用枝と同じ)。
        let Some((local, reused)) = self.entities.allocate_tracked() else {
            self.record_fault(
                FaultKind::EntitySpace,
                "entity() で払い出す枠が無い (max_entities 到達 + free stack 空)",
            );
            return Err(format!(
                "entity space exhausted: max_entities={} and the free stack is empty — \
                 delete entities to free slots, or recreate the DB with a larger \
                 create_with_capacity (remaining_eid_space() で残量が見られる)",
                self.entities.max_entities(),
            ));
        };
        if reused {
            self.clear_cell_versions(local);
        }
        // concurrent mode (= consumer thread 稼働) なら barrier counter を対称に
        // 進める。 issue5: push_count と apply_count を対称に保たないと
        // `flush_writes` が ties drain 前に early return して live query が
        // pending Tie を見落とす。 undo 廃止 (v4) 後は payload なしで
        // counter increment のみが起こる。
        //
        // #195: 以前は `Op::EntityCreated` を queue に積んでいたが、 drain 側は
        // no-op (counter 対称用のみ) なのに blocking push で、 consumer thread
        // 自身が bridge (`transfer_oplog_to_sync_ops`) 中に `entity_in` する経路で
        // 「満杯 queue の唯一の drainer が push に blocking」する livelock に
        // なった (#116 の小 queue default で顕在化)。 queue を経由せず両 counter を
        // 直接進める (push 先 → apply 後、 apply > push を作らない順序)。
        if self.write_queue.is_some() {
            self.push_count.fetch_add(1, Ordering::Release);
            self.apply_count.fetch_add(1, Ordering::Release);
        }
        let peer = self.peer_id.load(Ordering::Acquire);
        Ok(enchudb_oplog::make_eid(peer, local))
    }

    pub fn entities(&self) -> Vec<enchudb_oplog::EntityId> {
        let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
        self.entities.iter().into_iter()
            .map(|local| enchudb_oplog::make_eid(peer, local))
            .collect()
    }
    pub fn entity_count(&self) -> u32 { self.entities.count() }

    /// eid が live (= 割当済みで未削除) か。 0.9.0 (M10): query_lang の
    /// update/delete が未割当 slot へ phantom write するのを防ぐ existence check 用。
    /// 範囲外 eid は false。
    pub fn is_live(&self, eid: enchudb_oplog::EntityId) -> bool {
        self.entities.is_live(enchudb_oplog::eid_local(eid))
    }
    pub fn next_eid(&self) -> enchudb_oplog::EntityId {
        let peer = self.peer_id.load(std::sync::atomic::Ordering::Acquire);
        enchudb_oplog::make_eid(peer, self.entities.next_eid())
    }
    /// 0.7.0: 残り eid 空間 (= max_entities - 既存 table の eid_range_hi の max)。
    /// schema crate が `define_table` を呼ぶ前に「table 1 個分にどれだけ割けるか」
    /// 判断する用。
    /// #167: この DB が載っている filesystem の空き byte 数。 growable backing のみ。
    ///
    /// **sparse 前提の設計なので 「df に空きがある」 は安全を意味しない** — apparent size
    /// (既定 24 GB、 cell version 有効なら更に大) の全部を書ける空きが必要になり得る。
    /// 監視でこの値を見て、 枯渇前に対処すること。
    pub fn disk_free_bytes(&self) -> Option<u64> {
        match &self.backing {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Segments(set) => set.disk_free_bytes(),
            _ => None,
        }
    }

    /// #167: 空き容量不足で grow を拒否した回数。 0 でなければ write が落とされている。
    pub fn space_denials(&self) -> u64 {
        match &self.backing {
            #[cfg(not(target_arch = "wasm32"))]
            Backing::Segments(set) => set.space_denials(),
            _ => 0,
        }
    }

    /// #167: grow 時に残す空き容量 margin を上書きする (**テスト用**)。
    ///
    /// 巨大な値を渡すと、 実際にディスクを埋めずに 「空きが足りない」 経路を
    /// 決定的に踏める。 production では呼ばないこと。
    pub fn set_space_margin(&self, bytes: u64) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Backing::Segments(set) = &self.backing {
            set.set_space_margin(bytes);
        }
    }

    /// #59: 「想定内だが続行不能」 な事象を記録する。 **panic の代替**。
    ///
    /// 呼び出し側は必ず 「その write を拒否する」 ところまでやること
    /// (記録だけして壊れた値を書いたら、 panic より悪い)。
    pub(crate) fn record_fault(&self, kind: FaultKind, detail: &str) {
        use std::sync::atomic::Ordering;
        self.faults[kind.index()].fetch_add(1, Ordering::Relaxed);
        // 満杯状態では write ごとに来るので 1/s に絞る。 「黙って落とす」 のを
        // 避けるのが目的なので、 完全に消してはいけない。
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = self.last_fault_warn_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= 1000
            && self
                .last_fault_warn_ms
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            eprintln!(
                "[enchudb] warning: {} — write rejected: {detail} \
                 (rate-limited to 1/s; 累計は Engine::fault_count で見られる)",
                kind.as_str(),
            );
        }
    }

    /// #59: 種別ごとの fault 発生回数。 0 でないなら **その分の write が拒否されている**。
    pub fn fault_count(&self, kind: FaultKind) -> u64 {
        self.faults[kind.index()].load(std::sync::atomic::Ordering::Relaxed)
    }

    /// #59: fault 総数 (全種別)。 監視の 1 本目の指標に。
    pub fn fault_total(&self) -> u64 {
        self.faults
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .sum()
    }

    pub fn remaining_eid_space(&self) -> u32 {
        let used = self.tables.iter().map(|t| {
            if t.eid_range_hi == u32::MAX {
                self.entities.next_eid()  // anonymous open-ended
            } else {
                t.last_hi()
            }
        }).max().unwrap_or(0);
        self.max_entities().saturating_sub(used)
    }
    /// 0.7.0: 最大 entity 数 (= layout 確保時の上限)。 schema crate が
    /// size_hint を自動算出する用。
    pub fn max_entities(&self) -> u32 { self.entity_cap.load(std::sync::atomic::Ordering::Relaxed) }

    /// v10 Phase 3: entity の reservation (= `grow_entity_cap` で伸ばせる上限)。 create 時に
    /// 決まり、 後から増やせない (`GrowableOptions::reserve_entities`)。
    pub fn reserve_entities(&self) -> u32 { self.layout.read().unwrap().reserve_entities }

    /// v10 Phase 3 (request20): entity の上限を `new_cap` に伸ばす。 縮めない (現在値以下なら
    /// no-op で現在値を返す)。 `reserve_entities()` を超えると `InvalidInput`、 packed
    /// (`from_bytes`) backing は `Unsupported`。 offset は動かないので既存 data はそのまま、
    /// 伸びた分は次の `entity` / `entity_in` / `define_table` から使える。 header を書いて
    /// flush するので reopen 後も残る。 別 process の readonly reader は次の open から。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn grow_entity_cap(&self, new_cap: u32) -> io::Result<u32> {
        let cur = self.max_entities();
        if new_cap <= cur {
            return Ok(cur);
        }
        if self.backing.memory_len().is_some() {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "grow_entity_cap: packed (in-memory) backing"));
        }
        let mut layout = self.layout.write().unwrap();
        if new_cap > layout.reserve_entities {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "grow_entity_cap: {new_cap} exceeds the reservation {} made at create \
                     (GrowableOptions::reserve_entities)",
                    layout.reserve_entities
                ),
            ));
        }
        let l = &*layout;
        let grown = Layout::try_from_params_with_header(
            new_cap, self.max_himos,
            l.vocab_max_entries, l.vocab_index_cap, l.vocab_data_size,
            l.himoreg_max_entries, l.himoreg_index_cap, l.himoreg_data_size,
            l.content_data_size, l.leaf_data_size, l.cyl_max_values,
            l.has_cell_version(),
            l.header_size,
            l.reserve_entities,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        self.entities.grow(new_cap).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // header が唯一の永続 truth (store は open 時に header から cap を受け取る)
        {
            let buf = self.backing.header_mut(l.header_size);
            buf[H_MAX_ENTITIES..H_MAX_ENTITIES + 4].copy_from_slice(&new_cap.to_le_bytes());
            write_header_crc(buf);
        }
        self.backing.flush_header(l.header_size)?;
        self.entity_cap.store(new_cap, std::sync::atomic::Ordering::Release);
        *layout = grown;
        Ok(new_cap)
    }

    // ──── peer_id ────

    /// この Engine を所有する peer の id を設定。WAL / DB header にも反映される。
    /// 起動時に 1 回だけ呼ぶ想定。
    pub fn set_peer_id(&self, peer: enchudb_oplog::PeerId) {
        self.peer_id.store(peer, std::sync::atomic::Ordering::Release);
        // mmap の header に即書き込み(CRC 保護外なので再計算不要)
        self.backing.header_mut(HEADER_SIZE)[H_PEER_ID..H_PEER_ID + 4]
            .copy_from_slice(&peer.to_le_bytes());
        if let Some(wal) = self.oplog.as_ref() {
            wal.set_peer_id(peer);
        }
    }

    /// 現在の peer id。
    pub fn peer_id(&self) -> enchudb_oplog::PeerId {
        self.peer_id.load(std::sync::atomic::Ordering::Acquire)
    }

    /// CRDT mesh / relay mode の有効化。 true にすると Syncer が受信 record を
    /// **原型のまま** (`Engine::relay_record` #209) 自分の WAL に載せ、 次の
    /// publish で他 peer に配布する = 読み専 replica / gossip の土台。
    /// ホスト/クライアント構成では false のまま (= ホストの WAL に届いた時点で完結)。
    ///
    /// #209: 旧実装は `remote_*_apply` (翻訳後の値しか持たない場所) が翻訳後の
    /// eid/value を append しており、 direct 経路と混在すると row 重複 / vocab
    /// 写像汚染 / 署名不一致を起こした。 relay の append は Syncer 側 (原
    /// WireRecord を持つ場所) に移動済み。
    pub fn set_gossip_remote_apply(&self, on: bool) {
        self.gossip_remote_apply
            .store(on, std::sync::atomic::Ordering::Release);
    }

    pub fn gossip_remote_apply(&self) -> bool {
        self.gossip_remote_apply
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// #209: 受信 record を**素通し** (原 eid / 原 value / 原 HLC / 原 author /
    /// 原署名) で自分の WAL に relay append する。 Syncer が「apply が accept した
    /// record」に限って呼ぶ (LWW gate が cyclic topology の echo を止める唯一の
    /// 栓なので、 skip した record を relay してはいけない)。
    ///
    /// 翻訳 (eid/vid/ref) は body apply 専用で、 relay stream には漏らさない —
    /// 「中継であって作者交代ではない」。 WAL recovery はこの record を
    /// `apply_oplog_op` の relayed 経路 (受信時と同じ翻訳) で replay する。
    ///
    /// 戻り値: append できたか (oplog 無効 / append 失敗で false)。
    pub fn relay_record(&self, rec: &crate::transport::WireRecord) -> bool {
        use enchudb_oplog::oplog::{DecodedOp, Op, RelayedHeader};
        let Some(wal) = self.oplog.as_ref() else { return false };
        // Commit は relay しない: dedupe identity を持たず、 閉路があると無限反響
        // する。 group 境界は relay 自身の commit 周期で足りる。
        if matches!(rec.op, DecodedOp::Commit) {
            return false;
        }
        // 署名対象 bytes を持つ record は **byte 単位の素通し** — 署名は LSN 込みの
        // 領域に掛かっているので、 再 encode すると必ず不一致になる。
        if !rec.signed_bytes.is_empty() {
            return wal
                .append_relayed_verbatim(&rec.signed_bytes, &rec.signature, &rec.pubkey_fp)
                .is_ok();
        }
        // signed_bytes を持たない record (state 転写 #140 等) は op 再 encode で
        // relay する (署名なしなので一致性の問題はない)。
        let header = RelayedHeader {
            hlc: rec.hlc,
            author: rec.author_peer,
            signature: rec.signature,
            pubkey_fp: rec.pubkey_fp,
        };
        let op: Op<'_> = match &rec.op {
            DecodedOp::Tie { eid, himo_id, value } => {
                Op::Tie { eid: *eid, himo_id: *himo_id, value: *value }
            }
            DecodedOp::Untie { eid, himo_id } => Op::Untie { eid: *eid, himo_id: *himo_id },
            DecodedOp::Delete { eid } => Op::Delete { eid: *eid },
            DecodedOp::Content { eid, key, data } => {
                Op::Content { eid: *eid, key, data }
            }
            DecodedOp::Vocab { vid, bytes } => Op::Vocab { vid: *vid, bytes },
            DecodedOp::TieNamed { eid, himo_name, himo_kind, value } => Op::TieNamed {
                eid: *eid,
                himo_name,
                himo_kind: *himo_kind,
                value: *value,
            },
            DecodedOp::TieLeaf { eid, himo_name, himo_kind, bytes } => Op::TieLeaf {
                eid: *eid,
                himo_name,
                himo_kind: *himo_kind,
                bytes,
            },
            DecodedOp::TieRef { eid, himo_id, target } => {
                Op::TieRef { eid: *eid, himo_id: *himo_id, target: *target }
            }
            // Commit は relay しない: dedupe identity を持たず、 閉路があると
            // 無限反響する。 group 境界は relay 自身の commit 周期で足りる。
            DecodedOp::Commit => return false,
        };
        wal.append_relayed(op, header).is_ok()
    }

    /// **pre-v9 DB の版数置き場**への参照 (legacy)。 v9 DB では空のまま
    /// (版数は per-cell version column に永続する)。 判定はすべて engine の
    /// `set_cell` 側で行うので、 新しい呼び出しを増やさないこと。
    /// 残っている用途は sync 側の `hydrate_hlc_store` (pre-v9 専用) と
    /// legacy `Content` op の key 単位 LWW だけ。
    pub fn hlc_store(&self) -> &std::sync::Arc<crate::hlc_store::HlcStore> {
        &self.hlc_store
    }

    // ──── v9 (request17-A): per-cell version — LWW の真実を storage に置く ────
    //
    // 揮発 `HlcStore` (配送バッファからの再構築でしか埋まらない HashMap) が
    // #140 / #154 / #160 の共通の根だった。 ここでは cell の値と HLC を **1 本の
    // 関数の中で**書き、 「判定を呼び忘れると黙って壊れる」構造 (ローカル write が
    // まさにそうだった) を無くす。 request17 Phase 1 step 2: API だけ追加し、
    // まだ engine 内の write 経路からは呼ばない (step 4 / 5 で切替)。

    /// この DB が v9 の per-cell version 領域を持つか。 pre-v9 DB や v9 を
    /// 有効化していない create では false で、 `cell_hlc` は常に `Hlc::ZERO`
    /// (= 版数不明) を返す (A-1 の漸進的移行)。
    pub fn has_cell_version(&self) -> bool {
        self.layout.read().unwrap().has_cell_version()
    }

    #[inline]
    fn ver_col(&self, himo_id: u16) -> Option<&Column> {
        self.ver_cols.get(himo_id as usize)
    }

    /// ローカル write 用に HLC を 1 個 **事前**採番する (A-3)。 oplog 無効 (standalone) は
    /// 採番元が無いので `Hlc::ZERO` = 版数不明のまま書く (A-1 の現状維持。 standalone は
    /// そもそも sync しないので実害は無い)。
    ///
    /// 採番したら **必ずその HLC で WAL record も書く**こと (`append_at_hlc` /
    /// `append_many_with_hlcs`)。 cell の版数と record の HLC がずれると、 peer 間で
    /// 「自分が持つ版数」と「配った版数」が食い違う。
    ///
    /// **事前採番は async 経路専用** — 同期経路は `append_local_op` (WAL 先行) を使う。
    /// 事前採番には「採番順 = WAL 上の並び順」を呼び側が保証する義務が付く
    /// (`hlc_mint_lock` 参照)。
    #[inline]
    fn mint_local_hlc(&self) -> enchudb_oplog::Hlc {
        match self.oplog.as_ref() {
            Some(wal) => wal.mint_hlc(),
            None => enchudb_oplog::Hlc::ZERO,
        }
    }

    /// 同期経路のローカル write を WAL に載せ、 **その record に載った HLC** を返す。
    ///
    /// 採番を `append` の直列化 (append_lock + flock) の内側に置くのが要点で、
    /// これで **WAL 上の HLC が並び順どおり単調増加**する。 transport は record を
    /// HLC 順に並べ替えて配る (`InMemoryTransport::pull_as` 等) ので、 崩すと
    /// 「Vocab → その vid を使う Tie」のような依存順が受信側で逆転し、 vid 翻訳が
    /// 生値に fallback して無関係な row へ誤 bind する (#141 の再来)。
    ///
    /// oplog 無効、 または append 失敗 (WAL 満杯) は `Hlc::ZERO` = 版数不明。
    /// 後者は「本体には適用するが sync には流れない」従来の挙動と揃える。
    /// local eid → WAL に載せる global eid。 oplog 無効なら peer 0 扱い
    /// (record を出さないので値は使われない)。
    #[inline]
    fn oplog_eid(&self, local: u32) -> enchudb_oplog::EntityId {
        let peer = self.oplog.as_ref().map_or(0, |wal| wal.peer_id());
        enchudb_oplog::make_eid(peer, local)
    }

    /// async write 用: 「版数の採番」と「2 本の queue への push」を 1 単位にする guard。
    /// oplog 無効なら版数は常に `Hlc::ZERO` で並び順に意味が無いので lock を取らない。
    #[inline]
    fn mint_guard(&self) -> Option<parking_lot::MutexGuard<'_, ()>> {
        self.oplog.as_ref().map(|_| self.hlc_mint_lock.lock())
    }

    #[inline]
    fn append_local_op(&self, op: enchudb_oplog::oplog::Op<'_>) -> enchudb_oplog::Hlc {
        match self.oplog.as_ref() {
            Some(wal) => wal
                .append_with_hlc(op)
                .map(|(_, h)| h)
                .unwrap_or(enchudb_oplog::Hlc::ZERO),
            None => enchudb_oplog::Hlc::ZERO,
        }
    }

    /// ローカル write が版数判定で弾かれた (= 自分で採番した HLC より新しい版数が
    /// cell に載っていた) ことを 1 回だけ報せる。
    ///
    /// 構造上ここには来ない — ローカル採番は単調増加で、 remote apply は受信 HLC で
    /// ローカル clock を merge するため、 自分の次の HLC は必ず既存版数より大きい。
    /// 来たなら clock の巻き戻り (システム時刻の後退等) を意味し、 **その write は
    /// 落ちている**ので無音にはしない。
    fn warn_local_write_rejected(&self, local: u32, himo_id: u16, mine: enchudb_oplog::Hlc) {
        use std::sync::atomic::Ordering;
        if self.warned_cell_version_reject.swap(true, Ordering::Relaxed) {
            return;
        }
        eprintln!(
            "[enchudb] warning: local write (eid {}, himo {}) was rejected by the cell version \
             guard — stored HLC {:?} >= minted {:?}. clock went backwards? \
             (this write was NOT applied and is NOT published)",
            local, himo_id, self.cell_hlc(local as enchudb_oplog::EntityId, himo_id), mine,
        );
    }

    /// cell `(eid, himo_id)` に最後に書かれた HLC。
    ///
    /// `Hlc::ZERO` は **版数不明** — v9 領域が無い DB、 または まだ一度も
    /// 版数付きで書かれていない cell。 A-1 のとおり版数不明 cell は従来どおり
    /// (= 無条件に上書き) 扱う。
    pub fn cell_hlc(&self, eid: enchudb_oplog::EntityId, himo_id: u16) -> enchudb_oplog::Hlc {
        self.cell_hlc_local(enchudb_oplog::eid_local(eid), himo_id)
    }

    /// `cell_hlc` の local eid 版 (engine 内の write 経路は既に local に落として
    /// いるので、 偽 EntityId を組み立てずに済ませる)。
    #[inline]
    fn cell_hlc_local(&self, local: u32, himo_id: u16) -> enchudb_oplog::Hlc {
        match self.ver_col(himo_id) {
            Some(col) if local < self.max_entities() => {
                // growable backing: 未コミット page は read でも SIGBUS。
                // #167: 伸ばせなければ **読まない** (ZERO 扱い)。 触れば落ちる。
                if col.ensure_committed_for(local).is_err() {
                    self.record_fault(
                        FaultKind::DiskSpace,
                        "cell version の read に必要な commit を伸ばせない — ZERO として扱う",
                    );
                    return enchudb_oplog::Hlc::ZERO;
                }
                hlc_from_cell(col.get(local))
            }
            _ => enchudb_oplog::Hlc::ZERO,
        }
    }

    /// entity `eid` の tombstone HLC (= いつ削除されたか)。 未削除 / 版数不明は
    /// `Hlc::ZERO`。 himo を持たない `Delete { eid }` 用に eid 空間へ 1 本だけ
    /// 持つ column (A-5)。
    pub fn tombstone_hlc(&self, eid: enchudb_oplog::EntityId) -> enchudb_oplog::Hlc {
        self.tombstone_hlc_local(enchudb_oplog::eid_local(eid))
    }

    #[inline]
    fn tombstone_hlc_local(&self, local: u32) -> enchudb_oplog::Hlc {
        match self.tomb_col.as_ref() {
            Some(col) if local < self.max_entities() => {
                // #167: 伸ばせなければ読まない (未 commit page の read も SIGBUS)。
                if col.ensure_committed_for(local).is_err() {
                    self.record_fault(
                        FaultKind::DiskSpace,
                        "tombstone の read に必要な commit を伸ばせない — ZERO として扱う",
                    );
                    return enchudb_oplog::Hlc::ZERO;
                }
                hlc_from_cell(col.get(local))
            }
            _ => enchudb_oplog::Hlc::ZERO,
        }
    }

    /// request17 step 5: 受信 record の HLC でローカル HLC clock を merge する。
    ///
    /// これが無いと、 相手の wall clock が先行している間ずっと「自分が次に採番する
    /// HLC < 既に適用した remote の版数」になり、 **自分のローカル write が自分の DB で
    /// 負ける** (版数を storage に置いた途端に顕在化する。 #161 と同じ「止めた先に
    /// 脱出路が無い」形)。 HLC の merge 規則そのもの。
    #[inline]
    fn observe_remote_hlc(&self, hlc: enchudb_oplog::Hlc) {
        if hlc == enchudb_oplog::Hlc::ZERO {
            return;
        }
        if let Some(wal) = self.oplog.as_ref() {
            wal.observe_hlc(hlc);
        }
    }

    /// pre-v9 DB の版数置き場 (揮発 `HlcStore`) の key。 sync.rs が使っていた key
    /// (`resolve_remote_eid` が返す self peer 付き EntityId) と同一にする。
    #[inline]
    fn version_key(&self, local: u32) -> u64 {
        enchudb_oplog::make_eid(self.peer_id(), local)
    }

    /// cell の現在の版数。 v9 DB は version column、 pre-v9 DB は揮発 `HlcStore`
    /// (= 従来 sync.rs が持っていた記憶) を見る。 どちらも無ければ `Hlc::ZERO`。
    ///
    /// **判定の入口はこの 1 本だけ** — 版数の置き場が column か HashMap かは
    /// ここから先に漏らさない。 pre-v9 の fallback は step 6/7 (v9 既定化) で外す。
    #[inline]
    fn version_of(&self, local: u32, himo_id: u16) -> enchudb_oplog::Hlc {
        if self.has_cell_version() {
            self.cell_hlc_local(local, himo_id)
        } else {
            self.hlc_store
                .get(self.version_key(local), himo_id)
                .unwrap_or(enchudb_oplog::Hlc::ZERO)
        }
    }

    /// entity の tombstone 版数 (`version_of` の delete 版、 sentinel himo = `u16::MAX`)。
    #[inline]
    fn tombstone_version_of(&self, local: u32) -> enchudb_oplog::Hlc {
        if self.has_cell_version() {
            self.tombstone_hlc_local(local)
        } else {
            self.hlc_store
                .get(self.version_key(local), u16::MAX)
                .unwrap_or(enchudb_oplog::Hlc::ZERO)
        }
    }

    /// 削除より古い write か (A-5)。 true なら適用してはいけない —
    /// 適用すると削除済み entity が復活する (#140 の根)。
    ///
    /// 版数不明 (`ZERO`) の write は判定対象外 (A-1 の現状維持)。
    pub fn tombstone_blocks(&self, eid: enchudb_oplog::EntityId, hlc: enchudb_oplog::Hlc) -> bool {
        if hlc == enchudb_oplog::Hlc::ZERO {
            return false;
        }
        let tomb = self.tombstone_version_of(enchudb_oplog::eid_local(eid));
        tomb != enchudb_oplog::Hlc::ZERO && hlc < tomb
    }

    /// cell への write を採用してよいか。 `set_cell` / `clear_cell` の唯一の判定。
    #[inline]
    fn accepts_write(&self, local: u32, himo_id: u16, hlc: enchudb_oplog::Hlc) -> bool {
        if hlc == enchudb_oplog::Hlc::ZERO {
            return true; // 版数不明 (standalone のローカル write) は従来どおり通す
        }
        // request18: 版数の置き場を **構造的に持たない** DB (= sync tables が無く、
        // v9 領域も無い) は判定材料が存在しない。 `store_cell_hlc` /
        // `set_tombstone_local` が記帳を止めているので `HlcStore` は必ず空で、
        // 下の 2 本は必ず ZERO を返す = 必ず true になる。
        //
        // ここで抜けることで **peer を使わない DB の write path が request17 以前と
        // 同じ**になる (空 HashMap の lookup ×2 が消える)。 v9 領域を持つ DB
        // (0.19/0.20 で作られた非 sync DB 含む) は載っている版数を無視しないよう
        // 従来どおり判定に入る。
        if !self.sync_tables_on() && !self.has_cell_version() {
            return true;
        }
        // 削除済み entity を古い Tie/Untie で蘇らせない (A-5)
        let tomb = self.tombstone_version_of(local);
        if tomb != enchudb_oplog::Hlc::ZERO && hlc < tomb {
            return false;
        }
        Self::accepts_hlc(self.version_of(local, himo_id), hlc)
    }

    /// LWW 判定 (A-2)。 true = 採用してよい。
    ///
    /// - 受信 HLC が `ZERO` (= 版数不明。 oplog 無効な standalone のローカル write)
    ///   は判定対象外で常に採用する。 止めると standalone が書けなくなるだけで、
    ///   #161 と同じ「塞いだ先に脱出路が無い」形になる
    /// - 現在値が `ZERO` (版数不明 cell) も常に採用 = 従来挙動の維持 (A-1)
    #[inline]
    fn accepts_hlc(cur: enchudb_oplog::Hlc, incoming: enchudb_oplog::Hlc) -> bool {
        incoming == enchudb_oplog::Hlc::ZERO
            || cur == enchudb_oplog::Hlc::ZERO
            || cur < incoming
    }

    /// version column へ HLC を書く。 `ZERO` は「版数不明」を意味するので書かない
    /// (既に載っている版数を消さない)。 v9 領域が無ければ no-op。
    #[inline]
    fn store_cell_hlc(&self, local: u32, himo_id: u16, hlc: enchudb_oplog::Hlc) {
        if hlc == enchudb_oplog::Hlc::ZERO || local >= self.max_entities() {
            return;
        }
        match self.ver_col(himo_id) {
            Some(col) => {
                // growable backing: 未コミット page への書き込みは SIGBUS。
                // #167: 伸ばせなければ **書かない**。
                if col.ensure_committed_for(local).is_err() {
                    self.record_fault(
                        FaultKind::DiskSpace,
                        "cell version の write に必要な commit を伸ばせない — 版数を記録しない",
                    );
                    return;
                }
                // request18: `init_lazy` で作られた column はここで header を確定する。
                col.ensure_header();
                col.ensure_count(local);
                col.set(local, &hlc_to_cell(hlc));
            }
            // pre-v9: 揮発 HlcStore に置く (= 従来 sync.rs がやっていたこと)。
            // 版数の置き場が違うだけで、 判定は `accepts_write` の 1 本のまま。
            //
            // request18: **sync しない DB では記帳しない**。 `HlcStore` は上限の無い
            // HashMap で、 版数を使う相手 (remote record の LWW 判定) が居ないまま
            // 書き続けると 1 M cell で ~40 MB の純粋な漏れになる。 sync tables を
            // 有効化した直後 (v9 領域はできたが column はまだ生えていない) の窓では
            // `sync_tables_on()` が true なので従来どおり記帳される。
            None => {
                if !self.sync_tables_on() {
                    return;
                }
                self.hlc_store.try_set(self.version_key(local), himo_id, hlc);
            }
        }
    }

    /// request18: v9 領域はあるが **まだ 1 つも版数が載っていない** 状態か。
    ///
    /// `enable_sync_tables()` の窓 (= 領域は生えたが column はまだで、 版数が揮発
    /// `HlcStore` にしか無かったセッション) を経て初めて open した DB がこれに当たる。
    /// 揮発 store はプロセスと共に消えているので、 このときだけは配送バッファ
    /// (`_sync_ops`) からの hydrate が唯一の復元手段になる。
    ///
    /// 一度でも版数が載った DB では false になり、 hydrate は二度と走らない
    /// (request17 step 6 の 「v9 では hydrate しない」 を実質維持する)。
    pub fn cell_versions_are_empty(&self) -> bool {
        self.has_cell_version()
            && self.tomb_col.as_ref().is_none_or(|c| c.count() == 0)
            && self.ver_cols.iter().all(|c| c.count() == 0)
    }

    /// request18: 版数の **再構築** (`Syncer::hydrate_hlc_store`) 用の入口。
    ///
    /// 置き場 (v9 version column / tombstone column / 揮発 `HlcStore`) の選択は
    /// `store_cell_hlc` と同じ判断に委ね、 既に載っている版数より新しいときだけ書く
    /// (monotone-max)。 `himo_id` は hydrate 側の sentinel をそのまま受ける:
    ///
    /// - `u16::MAX` = entity の削除版数 → tombstone column
    /// - `0x8000 | key_hash` = Content key → column を持たないので `HlcStore`
    ///
    /// **なぜ要るか**: `enable_sync_tables()` の窓 (= v9 領域は生えたが column は
    /// 次の open から、 という 1 セッション) で書かれた版数は揮発 `HlcStore` に
    /// しか無い。 次の open で column が生えると `version_of` は column しか見なく
    /// なるので、 hydrate が `HlcStore` へ書いても読まれない = 陳腐 record の再 apply
    /// (#154 の再来) になる。 hydrate をこの 1 本に通すことで、 復元先が常に
    /// 「その DB が今使っている置き場」 に揃う。
    pub fn remember_version(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        hlc: enchudb_oplog::Hlc,
    ) {
        let local = enchudb_oplog::eid_local(eid);
        if hlc == enchudb_oplog::Hlc::ZERO || local >= self.max_entities() {
            return;
        }
        if himo_id == u16::MAX {
            self.set_tombstone_local(local, hlc); // 内部で monotone-max
            return;
        }
        if Self::accepts_hlc(self.version_of(local, himo_id), hlc) {
            self.store_cell_hlc(local, himo_id, hlc);
        }
    }

    /// cell への書き込みと版数の記録を **不可分に**行う (A-2)。
    ///
    /// 戻り値 `false` = 受信 HLC が現在の版数より古いので不採用。 このとき
    /// `Column` も cylinder も version column も 1 byte も触っていない。
    ///
    /// 書く順序は **値 → HLC** (A-4)。 value と HLC は別 region なので 1 命令では
    /// 書けず、 逆順にすると「HLC だけ新しい」窓ができて後続の正しい record が
    /// 永久に負ける。 「値は新しいが HLC は古い」窓は次の write で必ず解消する。
    ///
    /// `value` は `tie_to_by_id` と同じ raw な cell 値。 Leaf himo の旧 payload
    /// 解放 (`take_leaf_cell` / `free_leaf_offset`) は呼び元の責務。
    pub fn set_cell(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        value: u32,
        hlc: enchudb_oplog::Hlc,
    ) -> bool {
        self.check_writable();
        if himo_id as usize >= self.himos.len() {
            debug_assert!(false, "himo_id {} out of range (max {})", himo_id, self.himos.len());
            return false;
        }
        self.set_cell_local(enchudb_oplog::eid_local(eid), himo_id, value, hlc)
    }

    /// `set_cell` の local eid 版 (engine 内の write 経路用。 `check_writable` と
    /// himo_id の範囲チェックは呼び元が済ませている — 範囲外は他の write 経路と
    /// 同じく `himos[hid]` の panic になる)。
    fn set_cell_local(&self, local: u32, himo_id: u16, value: u32, hlc: enchudb_oplog::Hlc) -> bool {
        if !self.accepts_write(local, himo_id, hlc) {
            return false;
        }
        self.himos[himo_id as usize].set(local, value);
        self.store_cell_hlc(local, himo_id, hlc);
        true
    }

    /// `set_cell` の untie 版 — cell を空にして版数を進める。 untie も
    /// 「値の変更」なので LWW 判定を通す (通さないと外した cell に古い tie が
    /// 蘇る)。
    pub fn clear_cell(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        hlc: enchudb_oplog::Hlc,
    ) -> bool {
        self.check_writable();
        if himo_id as usize >= self.himos.len() {
            return false;
        }
        self.clear_cell_local(enchudb_oplog::eid_local(eid), himo_id, hlc)
    }

    /// `clear_cell` の local eid 版。 Leaf payload の解放 (`free_leaf_cell`) は
    /// **採用が決まってから**呼ぶこと (不採用なら cell は変わらないので解放しない)。
    fn clear_cell_local(&self, local: u32, himo_id: u16, hlc: enchudb_oplog::Hlc) -> bool {
        if !self.accepts_write(local, himo_id, hlc) {
            return false;
        }
        self.himos[himo_id as usize].remove(local);
        self.store_cell_hlc(local, himo_id, hlc);
        true
    }

    /// untie 経路用の `clear_cell_local` — 採用が決まった cell の Leaf payload も
    /// 解放する。 不採用 (= 古い untie) のときは payload を **解放しない**
    /// (cell がまだその payload を指しているため)。
    fn clear_cell_local_freeing_leaf(&self, local: u32, himo_id: u16, hlc: enchudb_oplog::Hlc) -> bool {
        if !self.accepts_write(local, himo_id, hlc) {
            return false;
        }
        self.free_leaf_cell(local, himo_id as usize);
        self.himos[himo_id as usize].remove(local);
        self.store_cell_hlc(local, himo_id, hlc);
        true
    }

    /// slot 再利用時に **前の住人の版数を落とす**。
    ///
    /// 版数の置き場は v9 なら version / tombstone column、 pre-v9 なら揮発
    /// `HlcStore` だが、 **どちらも local slot で index される**ので事情は同じ:
    /// free list から払い出された slot には前の住人の版数が残っている。 消さずに
    /// 渡すと新しい住人への write が「前の住人の削除より古い」「前の住人の cell
    /// より古い」と判定されて **無言で落ちる** (`set_cell` は `false` を返すだけ)。
    /// 古い record をまとめて再生する局面 (bootstrap / `Hlc::ZERO` からの pull) で
    /// 顕在化する: 相手が t1 に author した record が、 こちらが t2 (> t1) に消した
    /// **別の** entity の tombstone に負けて適用されない。
    ///
    /// 前の住人の eid は slot が free list に入った時点で到達不能なので、 版数を
    /// 落として困る読み手はいない。
    ///
    /// v9 側で `count() <= local` の column は **その cell を一度も書いていない** =
    /// 中身は zero (= 版数不明) なので触らない。 growable backing の未コミット
    /// page を掴まないための guard でもある (触ると SIGBUS)。
    ///
    /// pre-v9 側は himo 数ぶんの `remove` を回す。 `HlcStore::remove_entity` は
    /// HashMap 全体を `retain` で走査する O(n) なので、 `_sync_ops` の ring 再利用の
    /// ような払い出し hot path から呼ぶと別の事故になる。
    ///
    /// なお **slot に紐づく状態はこれで全部ではない** — `EidTranslator` の
    /// `(author_peer, foreign_local) -> local` 写像も同じ理由で stale になるが、
    /// そちらは remove API 自体が無く master から続く別の穴なので #166 で扱う。
    fn clear_cell_versions(&self, local: u32) {
        // #166: 版数を消す **前** に翻訳写像を外す。 退避する tombstone は
        // まだ slot 上にあるので、 順序を逆にすると読めなくなる。
        self.evict_translation_for_reuse(local);
        if local >= self.max_entities() {
            return;
        }
        // pre-v9: 版数は揮発 HlcStore にある。 sentinel (u16::MAX) が tombstone。
        if !self.has_cell_version() {
            let key = self.version_key(local);
            for hid in 0..self.himos.len() {
                self.hlc_store.remove(key, hid as u16);
            }
            self.hlc_store.remove(key, u16::MAX);
            return;
        }
        for col in self.ver_cols.iter() {
            if col.count() > local {
                col.clear(local);
            }
        }
        if let Some(col) = self.tomb_col.as_ref()
            && col.count() > local
        {
            col.clear(local);
        }
    }

    /// #166: slot を別の住人に渡す前に、 **その slot が持っていた翻訳写像を外し、
    /// 削除版数を identity 側へ退避する**。
    ///
    /// 写像を外さないと、 元の foreign entity 宛の record が新しい住人へ書き込まれる
    /// (無関係な行の silent 破壊)。 かといって外すだけだと、 削除より古い record が
    /// 「初見の foreign entity」として新しい slot を確保し **削除済み entity が復活
    /// する** — 破壊が復活に化けるだけになる。 tombstone は slot ではなく identity に
    /// 属する事実なので、 `foreign_tombs` へ移して slot の寿命から切り離す。
    ///
    /// レプリカでない local (= 自分が産んだ entity) では no-op。
    fn evict_translation_for_reuse(&self, local: u32) {
        let Some(key) = self.eid_translator.remove_local(local) else {
            return;
        };
        let tomb = self.tombstone_version_of(local);
        if tomb == enchudb_oplog::Hlc::ZERO {
            // 削除されずに slot だけ回った (= 到達不能になった) ケース。 覚えることは無い。
            return;
        }
        let mut g = self.foreign_tombs.write().unwrap();
        let e = g.entry(key).or_insert(enchudb_oplog::Hlc::ZERO);
        if *e < tomb {
            *e = tomb; // monotone-max (同 identity が複数回 slot を回った場合)
        }
        self.foreign_tombs_empty
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// #166: `(author_peer, foreign_local)` に **新しい slot** を払い出した直後に、
    /// 退避してあった削除版数を書き戻す。
    ///
    /// これで 「この foreign entity は t で削除済み」 が slot を跨いで生き残り、
    /// t より古い record は新しい slot 上の tombstone で従来どおり弾かれる
    /// (= 判定経路は `set_cell` 1 本のまま。 A-2 を崩さない)。
    fn restore_foreign_tombstone(&self, peer: enchudb_oplog::PeerId, foreign_local: u32, local: u32) {
        if self.foreign_tombs_empty.load(std::sync::atomic::Ordering::Relaxed) {
            return; // 常態 (= 一度も slot が回っていない)。 lock を取らない
        }
        let tomb = {
            let g = self.foreign_tombs.read().unwrap();
            match g.get(&(peer, foreign_local)) {
                Some(&t) if t != enchudb_oplog::Hlc::ZERO => t,
                _ => return,
            }
        };
        self.set_tombstone_local(local, tomb);
    }

    /// #166: slot に載っていない (= 退避済みの) foreign tombstone の snapshot。
    /// `.eidmap` v3 の 「写像を持たない tombstone だけの entry」 として永続化する。
    fn orphan_foreign_tombstones(&self) -> Vec<(enchudb_oplog::PeerId, u32, enchudb_oplog::Hlc)> {
        let g = self.foreign_tombs.read().unwrap();
        g.iter()
            .filter(|(_, t)| **t != enchudb_oplog::Hlc::ZERO)
            .map(|(&(peer, fl), &t)| (peer, fl, t))
            .collect()
    }

    /// #166: 復元 / 再受信で使う。 退避表へ直接載せる (monotone-max)。
    fn remember_foreign_tombstone(
        &self,
        peer: enchudb_oplog::PeerId,
        foreign_local: u32,
        tomb: enchudb_oplog::Hlc,
    ) {
        if tomb == enchudb_oplog::Hlc::ZERO {
            return;
        }
        let mut g = self.foreign_tombs.write().unwrap();
        let e = g.entry((peer, foreign_local)).or_insert(enchudb_oplog::Hlc::ZERO);
        if *e < tomb {
            *e = tomb;
        }
        self.foreign_tombs_empty
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// 削除を **本体まで** 適用する。 `false` = 受信 HLC が既存 tombstone より古いので不採用。
    ///
    /// `set_tombstone_local` が扱うのは 「いつ消えたか」 の版数だけで、 本体の除去は
    /// 呼び元の責務だった。 3 経路 (`delete` / `remote_delete_apply` / WAL replay) が
    /// 同じ手順を書き写しており、 かつ **判定と適用が 1 本の bool に潰れていた**ため、
    /// 次の 2 つが起きていた:
    ///
    /// - **crash が間に落ちると固まる**: tombstone を書いた直後に殺されると
    ///   「tombstone は在るが cell は生きている」 で残る。 query 経路
    ///   (`entities_with_himo` 等) は tombstone を見ないので、 アプリからは
    ///   **生きた行**に見える (= 消したはずのものが復活して見える)
    /// - **再適用で直らない**: 同じ Delete が再配送 / WAL replay で戻ってきても、
    ///   `set_tombstone_local` は同値を `false` で弾く (LWW は真に新しい版だけ通す) ため
    ///   本体除去に到達しない。 **二度と直らない**
    ///
    /// そこで **判定 (LWW) と適用 (本体除去) を分ける**。 拒否するのは受信 HLC が
    /// 既存 tombstone より**真に古い**ときだけで、 それ以外 (新しい / 同値) は
    /// 本体除去を必ず実行する = **冪等**。 順序は tombstone 先行のまま変えていない —
    /// 先に本体を消すと 「tombstone 無しで cell が半端」 な窓ができ、 そこへ届いた
    /// tie が復活させうるため。 crash 窓に残る 「tombstone は在るが本体が残る」 形は
    /// 再配送 (この冪等化) と open 時の sweep (`finish_interrupted_deletes`) の
    /// 両方で埋まる。
    fn apply_delete_local(&self, local: u32, hlc: enchudb_oplog::Hlc) -> bool {
        let tomb = self.tombstone_version_of(local);
        if hlc != enchudb_oplog::Hlc::ZERO
            && tomb != enchudb_oplog::Hlc::ZERO
            && hlc < tomb
        {
            return false; // 既に記録した削除より古い Delete — 巻き戻さない
        }
        // monotone-max。 同値なら版数側は no-op になるが、 本体除去は必ず走らせる。
        self.set_tombstone_local(local, hlc);
        self.remove_entity_body(local, hlc);
        true
    }

    /// 途中で切れた delete の跡を **数えるだけ** (修復しない)。 readonly でも呼べる。
    ///
    /// アプリ側の監査が 「tombstone が在る && 行が生きている」 で数えると、
    /// **削除の後に作り直された行**まで拾ってしまう (LWW 上は正しく生きている)。
    /// 判定は sweep と同一にしておかないと 「直したのに数字が減らない」 になる。
    pub fn interrupted_delete_count(&self) -> usize {
        self.scan_interrupted_deletes(/*repair=*/ false)
    }

    /// 途中で切れた delete の跡を **その場で埋める** (open 時の sweep と同じ)。 戻り値は直した数。
    ///
    /// 通常は writer open のたびに自動で走るので明示的に呼ぶ必要は無い。 長時間
    /// 開きっぱなしの daemon が、 再起動せずに修復したい場合の入口。
    pub fn repair_interrupted_deletes(&self) -> usize {
        self.check_writable();
        self.scan_interrupted_deletes(/*repair=*/ true)
    }

    /// writer open 時の自動 sweep 入口 (`open_internal` から)。
    fn finish_interrupted_deletes(&self) -> usize {
        self.scan_interrupted_deletes(/*repair=*/ true)
    }

    /// crash が delete を途中で切った跡を open 時に掃除する。 戻り値は直した entity 数。
    ///
    /// 対象は **「tombstone が立っているのに、 それより古い cell が生きている」 entity**。
    /// この形は `apply_delete_local` の doc にあるとおり、 tombstone を書いた直後に
    /// 落ちると残る。 query 経路は tombstone を見ないので、 放置するとアプリからは
    /// 生きた行に見え続ける (実地: syncretic の chaos soak で保全した store 3/3 に
    /// 1 件ずつ在った)。 再配送があれば冪等化した apply が直すが、 record が ring から
    /// 落ちた後は再配送が来ないので、 **open 時にこちらでも埋める**。
    ///
    /// 残すのは **tombstone より真に新しい cell** だけ — 削除後に作り直された
    /// entity がこれに当たる (LWW 上は正しく生きている)。 それ以外は本体除去
    /// (`remove_entity_body`) と同じ判断で落とす。 **版数不明 (`ZERO`) も落とす**:
    /// durable な tombstone は v9 領域が生えた後にしか書けない (pre-v9 の tombstone は
    /// 揮発 `HlcStore`、 foreign 分の `.eidmap` 復元先も移行後の tombstone column) ので、
    /// 「durable な tombstone が在る」 ⇒ 「その版数不明 cell は削除より前」 が言える。
    /// ここだけ保守側に倒すと、 **古い版から上げてきた store** (= 移行で全 cell が
    /// 版数不明になる = 実運用で最も修復が要る母集団) だけが永久に直らない。
    ///
    /// 生き残る cell が 1 つも無くなった entity だけ live 登録から外す。
    /// pre-v9 (版数 column が無い) DB では tombstone 版数自体が揮発なので何もしない。
    fn scan_interrupted_deletes(&self, repair: bool) -> usize {
        if !self.has_cell_version() {
            return 0;
        }
        // tombstone column の count = 削除版数を書いた local の上限。 これを越える
        // slot に tombstone は在り得ないので走査を打ち切る (growable backing の
        // 未 commit 領域を触らないためでもある)。
        let tomb_count = match self.tomb_col.as_ref() {
            Some(col) => col.count(),
            None => return 0,
        };
        if tomb_count == 0 {
            return 0;
        }
        let mut repaired = 0usize;
        for local in self.entities.iter() {
            if local >= tomb_count {
                continue;
            }
            let tomb = self.tombstone_hlc_local(local);
            if tomb == enchudb_oplog::Hlc::ZERO {
                continue; // 削除されていない = 常態
            }
            // 削除より後に書かれた cell だけが 「削除後に作り直された行」。
            // それ以外 (古い / 版数不明) が 1 つでも生きていれば、 削除が
            // 途中で切れた跡なので本体除去をやり直す。 判断は
            // `remove_entity_body` と同一 — ここだけ保守側に倒すと、
            // 「本体除去なら消える cell が、 sweep では永久に残る」 になる。
            let mut has_cell = false;
            let mut stale = false;
            for hid in 0..self.himos.len() {
                if self.himos[hid].get_value(local).is_none() {
                    continue;
                }
                has_cell = true;
                if !(self.version_of(local, hid as u16) > tomb) {
                    stale = true;
                    break;
                }
            }
            if !has_cell {
                // tombstone は durable、 cell も落ちきっているのに live 登録だけ
                // 残った形 (= (2) と (3) の間で落ちた)。 slot を返す。
                if repair {
                    self.entities.free(local);
                }
                repaired += 1;
                continue;
            }
            if !stale {
                continue; // 削除後に作り直された行 — LWW 上は正しく生きている
            }
            if repair {
                self.remove_entity_body(local, tomb);
            }
            repaired += 1;
        }
        if repair && repaired > 0 {
            eprintln!(
                "warning: finished {repaired} interrupted delete(s) at open (tombstone was durable but the row was still live)"
            );
        }
        repaired
    }

    /// entity 本体 (全 himo の cell + Leaf payload + live 登録) を落とす。
    ///
    /// 削除より **真に新しい** cell は残す — 削除の後に作り直された entity が
    /// これに当たる (同じ Delete が再配送されたとき、 その後の書き込みまで
    /// 巻き添えにしないため)。 版数不明 (`ZERO`) の cell は従来どおり消す:
    /// pre-v9 の cell や oplog 無効の standalone write は版数を持たないので、
    /// ここで残すと delete が効かなくなる。 `hlc` 自体が `ZERO` (版数を持てない
    /// standalone の delete) のときも全部消す = 従来挙動。
    ///
    /// 版数 column は**触らない** — 「この cell が最後に書かれた版」 は削除後も
    /// LWW 判定に要る (古い tie の復活を弾くのは版数の役目)。
    fn remove_entity_body(&self, local: u32, hlc: enchudb_oplog::Hlc) {
        let mut survivor = false;
        for hid in 0..self.himos.len() {
            if self.himos[hid].get_value(local).is_none() {
                continue;
            }
            if hlc != enchudb_oplog::Hlc::ZERO && self.version_of(local, hid as u16) > hlc {
                survivor = true;
                continue;
            }
            self.free_leaf_cell(local, hid);
            self.himos[hid].remove(local);
        }
        if !survivor {
            self.entities.free(local);
        }
    }

    /// entity の削除版数を記録する (A-5)。 `false` = 受信 HLC が古いので不採用。
    ///
    /// entity 本体の解放はこの関数の責務ではない (呼び元の delete 経路が行う)。
    /// ここが持つのは「いつ消えたか」という版数だけで、 これが永続することで
    /// 配送バッファから tombstone が消えた後も削除済み entity が復活しない
    /// (#140 の根)。
    pub fn set_tombstone(&self, eid: enchudb_oplog::EntityId, hlc: enchudb_oplog::Hlc) -> bool {
        self.check_writable();
        self.set_tombstone_local(enchudb_oplog::eid_local(eid), hlc)
    }

    /// `set_tombstone` の local eid 版。
    fn set_tombstone_local(&self, local: u32, hlc: enchudb_oplog::Hlc) -> bool {
        if !Self::accepts_hlc(self.tombstone_version_of(local), hlc) {
            return false;
        }
        if hlc == enchudb_oplog::Hlc::ZERO || local >= self.max_entities() {
            // 版数不明の delete は記録できないが、 「採用する」判定自体は
            // 変わらない = A-1 の現状維持。
            return true;
        }
        match self.tomb_col.as_ref() {
            Some(col) => {
                // #167: 伸ばせなければ書かない (未 commit page への write は SIGBUS)。
                if col.ensure_committed_for(local).is_err() {
                    self.record_fault(
                        FaultKind::DiskSpace,
                        "tombstone の write に必要な commit を伸ばせない — 記録しない",
                    );
                    return true;
                }
                // request18: `init_lazy` で作られた column はここで header を確定する。
                col.ensure_header();
                col.ensure_count(local);
                col.set(local, &hlc_to_cell(hlc));
            }
            // pre-v9: 従来どおり揮発 HlcStore の sentinel himo に置く。
            // request18: sync しない DB では記帳しない (`store_cell_hlc` と同じ理由)。
            None => {
                if !self.sync_tables_on() {
                    return true;
                }
                self.hlc_store.try_set(self.version_key(local), u16::MAX, hlc);
            }
        }
        true
    }

    /// #9: foreign eid 翻訳テーブルへの参照。
    pub fn eid_translator(&self) -> &std::sync::Arc<crate::eid_translator::EidTranslator> {
        &self.eid_translator
    }

    /// #9: 受信 op の foreign eid を「自分の eid 空間の local eid」に翻訳する。
    /// 初見の `(owner, foreign_local)` には `himo_id` が属する closed table 内に
    /// fresh な local entity を払い出す (= local entity と同じ allocator なので衝突しない)。
    /// himo を closed table に解決できない table-less op (anonymous himo / sentinel) は
    /// 安全に確保できる slot が無いので `None` を返す → caller は skip する。
    ///
    /// 0.11 (request10 phase 2): 翻訳キーは **eid に埋まった産みの親**
    /// (`eid_peer(foreign_eid)`) であって record の書き手ではない。 single-writer 下
    /// では両者は常に一致するが、 write-back 解禁後は 「B が書いた A の entity の
    /// record」 で乖離する — 書き手でキーすると受信側で別 entity に断片化する。
    /// `eid_peer(foreign_eid) == self.peer_id` なら identity (自分が産んだ entity)。
    pub fn resolve_remote_eid(
        &self,
        foreign_eid: enchudb_oplog::EntityId,
        himo_id: u16,
    ) -> Option<enchudb_oplog::EntityId> {
        use std::sync::atomic::Ordering;
        let self_peer = self.peer_id.load(Ordering::Acquire);
        let owner = enchudb_oplog::eid_peer(foreign_eid);
        if owner == self_peer {
            return Some(foreign_eid); // identity: 自分が産んだ entity
        }
        let foreign_local = enchudb_oplog::eid_local(foreign_eid);
        // get-or-allocate を atomic に行う (= 並行 apply で同じ foreign entity を
        // double-alloc しない)。 alloc が None (= table-less) なら写像を作らず None。
        let local = self
            .eid_translator
            .get_or_insert_with(owner, foreign_local, || {
                self.alloc_translated_local(himo_id)
            })?;
        // #166: slot が回った identity なら、 退避してある削除版数を今の slot に
        // 書き戻す。 `set_tombstone_local` は monotone-max なので、 既に載っている
        // 場合も新規 slot の場合も同じ呼びで済む (idempotent)。 退避表が空なら
        // read lock も取らない。
        self.restore_foreign_tombstone(owner, foreign_local, local);
        Some(enchudb_oplog::make_eid(self_peer, local))
    }

    /// #9: 既存の翻訳のみ引く (払い出しはしない)。 Delete のように himo を持たず
    /// table を導けない op 用。 未登録 (= 一度も Tie されてない foreign entity) なら
    /// None → 呼び出し側で skip。 キーは `resolve_remote_eid` と同じく eid の
    /// 産みの親 (`eid_peer`)、 産みの親 == self なら identity。
    pub fn resolve_remote_eid_existing(
        &self,
        foreign_eid: enchudb_oplog::EntityId,
    ) -> Option<enchudb_oplog::EntityId> {
        use std::sync::atomic::Ordering;
        let self_peer = self.peer_id.load(Ordering::Acquire);
        let owner = enchudb_oplog::eid_peer(foreign_eid);
        if owner == self_peer {
            return Some(foreign_eid);
        }
        let foreign_local = enchudb_oplog::eid_local(foreign_eid);
        self.eid_translator
            .get(owner, foreign_local)
            .map(|local| enchudb_oplog::make_eid(self_peer, local))
    }

    /// #9: foreign entity 用に fresh な local eid を払い出す。 `himo_id` が closed table
    /// 所属なら その table の `entity_in` 経路で確保 (= local entity と同じ allocator を
    /// 使うので衝突しない + `validate_eid_for_himo` の range 内に入る)、 local 部を `Some`
    /// で返す。 table を導けない (anonymous himo / sentinel) / entity_in 失敗 (table 枯渇)
    /// なら **安全に確保できる slot が無い** ので `None` (caller が op を skip)。
    /// `entity()` での anonymous fallback は使わない: sync engine では anonymous table が
    /// 閉じていて panic するし、 `entities.allocate()` は table range と衝突しうる。
    fn alloc_translated_local(&self, himo_id: u16) -> Option<u32> {
        let hid = himo_id as usize;
        if let Some(tid) = self.himo_table_get(hid) {
            let tid = tid as usize;
            if tid < self.tables.len() && self.tables[tid].eid_range_hi != u32::MAX {
                let name = self.tables[tid].name.clone();
                if let Ok(e) = self.entity_in(&name) {
                    return Some(enchudb_oplog::eid_local(e));
                }
            }
        }
        None
    }

    /// #9: himo が Ref 型か。 Ref の value は foreign target eid なので apply 時に
    /// entity eid とは別に value も translate する必要がある。
    pub fn himo_is_ref(&self, himo_id: u16) -> bool {
        let hid = himo_id as usize;
        hid < self.value_types.len() && self.value_types[hid] == ValueType::Ref
    }

    /// #9: Ref himo の value (= foreign target eid の local 部) を自分の eid 空間の
    /// local eid に翻訳する。 target entity が初見なら ref の **target table** に fresh
    /// な local を払い出す。 後で target entity 自身の Tie が来ても同じ key
    /// `(author_peer, foreign_value)` で同じ local に解決されるため整合する (= forward
    /// ref も OK)。 `author_peer == self` なら identity。
    pub fn resolve_remote_ref_value(
        &self,
        author_peer: enchudb_oplog::PeerId,
        foreign_value: u32,
        ref_himo_id: u16,
    ) -> Option<u32> {
        use std::sync::atomic::Ordering;
        let self_peer = self.peer_id.load(Ordering::Acquire);
        if author_peer == self_peer {
            return Some(foreign_value); // identity: 自分が author
        }
        // entity eid 翻訳と同じ key 空間・同じ atomic path。 ref-value 経由と target
        // entity 自身の Tie 経由が同じ foreign を解決しても 1 つの local に収束する。
        // target table を導けなければ None (caller が op を skip)。
        self.eid_translator
            .get_or_insert_with(author_peer, foreign_value, || {
                self.alloc_translated_local_in_target_table(ref_himo_id)
            })
    }

    /// #9: Ref himo の target table (fk_refs で引く) に fresh な local eid を払い出す。
    /// target table を導けない / entity_in 失敗なら `None` (caller が op を skip)。
    fn alloc_translated_local_in_target_table(&self, ref_himo_id: u16) -> Option<u32> {
        let hid = ref_himo_id as usize;
        if let Some(owner_tid) = self.himo_table_get(hid) {
            let owner_tid = owner_tid as usize;
            if owner_tid < self.tables.len() {
                let owner = &self.tables[owner_tid];
                if let Some(&(_, target_tid)) =
                    owner.fk_refs.iter().find(|(h, _)| *h == hid as u32)
                {
                    let target_tid = target_tid as usize;
                    if target_tid < self.tables.len()
                        && self.tables[target_tid].eid_range_hi != u32::MAX
                    {
                        let name = self.tables[target_tid].name.clone();
                        if let Ok(e) = self.entity_in(&name) {
                            return Some(enchudb_oplog::eid_local(e));
                        }
                    }
                }
            }
        }
        None
    }

    /// Phase C: 自 peer の鍵ペアを設定。WAL にも反映される。None で署名 off。
    pub fn set_keypair(&self, kp: Option<std::sync::Arc<enchudb_oplog::keys::Keypair>>) {
        *self.keypair.write().unwrap() = kp.clone();
        if let Some(wal) = self.oplog.as_ref() {
            wal.set_keypair(kp.clone());
        }
        if let Some(ref k) = kp {
            let peer = self.peer_id();
            self.pubkeys.force_register(peer, &k.public_bytes());
        }
    }

    /// Phase C: 他 peer の pubkey ストアへの参照。
    pub fn pubkeys(&self) -> &std::sync::Arc<enchudb_oplog::keys::PubkeyStore> {
        &self.pubkeys
    }

    /// Phase C: ACL への参照。Syncer が受信 op の author を enforce する。
    pub fn acl(&self) -> &std::sync::Arc<crate::acl::Acl> {
        &self.acl
    }

    /// Phase C: 自 peer の鍵ペアを返す。
    pub fn keypair(&self) -> Option<std::sync::Arc<enchudb_oplog::keys::Keypair>> {
        self.keypair.read().unwrap().clone()
    }

    /// WAL への参照(sync モジュールが publish に使う)。
    pub fn oplog_arc(&self) -> Option<std::sync::Arc<enchudb_oplog::oplog::OpLog>> {
        self.oplog.clone()
    }

    // ──── リモート peer から pull したレコードを apply する ────
    // これらは LWW 判定を通った後の無条件 apply。HlcStore は Syncer が先に更新済み。

    /// リモート peer から届いた Tie を apply。
    /// `gossip_remote_apply` 有効かつ `relayed` が `Some` なら、 同じ op を
    /// `append_relayed` で自分の WAL にも記録 (HLC/author/署名は元のまま)。
    /// `relayed` は WAL 受信時の元 header (sync 側で WireRecord から作って渡す)。
    /// v6 (#88): himo が LeafStore routing 対象なら `&LeafStore` を返す。
    /// 対象 = leaf region あり (v6) && Leaf 型 && engine 内部 table 配下でない
    /// (`_sync_ops.payload` 等の内部 Leaf は従来通り vocab)。
    ///
    /// request19: 除外は **engine 自身の内部 table だけ**。 アプリが作った
    /// local-only table (`define_reserved_table`) の Leaf 列は通常 table と同じ
    /// LeafStore 経路に載せる — 「配らない」 以外は普通の table として振る舞うべきで、
    /// 内部 table の都合 (vocab 据え置き) を持ち込む理由が無い。
    fn leaf_for(&self, hid: usize) -> Option<&LeafStore> {
        if hid < self.value_types.len()
            && self.value_types[hid] == ValueType::Leaf
            && !self.himo_is_in_engine_internal_table(hid)
        {
            self.leaf.as_ref()
        } else {
            None
        }
    }

    /// v6 (#88): text 型 himo の cell 生値 (raw) を payload に解決。 routed-Leaf は
    /// LeafStore offset として、 それ以外 (Tag / reserved Leaf) は vocab vid として読む。
    #[inline]
    fn text_value(&self, hid: usize, raw: u32) -> &[u8] {
        match self.leaf_for(hid) {
            Some(leaf) => leaf.get(raw),
            None => self.vocab.get(raw),
        }
    }

    /// v6 (#88): routed-Leaf の cell が offset を持っていれば LeafStore に free。
    /// delete / untie / apply_op の remove 直前に呼ぶ (leak 防止)。 非 routed は no-op。
    #[inline]
    /// #119: **publish 後**に旧 offset を free するための 2 段版。 `take_leaf_cell` で
    /// 旧 offset を先に捕まえ、 column を更新してから `free_leaf_offset` に渡す。
    ///
    /// free を先に呼ぶと、 同サイズ re-tie では best-fit が「たった今 free した hole」を
    /// 必ず再利用するため、 旧 offset を掴んでいる並行 reader が再利用済み slot を読む
    /// (silent None / free-list の hole header が payload に混ざった捏造 bytes)。
    fn take_leaf_cell(&self, eid: u32, hid: usize) -> Option<u32> {
        if self.leaf_for(hid).is_some() {
            self.himos[hid].get_value(eid)
        } else {
            None
        }
    }

    fn free_leaf_offset(&self, hid: usize, off: Option<u32>) {
        if let (Some(leaf), Some(off)) = (self.leaf_for(hid), off) {
            leaf.free(off);
        }
    }

    fn free_leaf_cell(&self, eid: u32, hid: usize) {
        if let Some(leaf) = self.leaf_for(hid)
            && let Some(off) = self.himos[hid].get_value(eid)
        {
            leaf.free(off);
        }
    }

    /// v6 (#88): open 時に routed-Leaf の live cell offset を集めて LeafStore の
    /// free-list を再構成する (free-list は非永続 = store の派生)。 writable open の
    /// load 末尾で呼ぶ。 これが無いと過去 session で空いた slot が再利用されない。
    fn rebuild_leaf_free_list(&self) {
        let Some(leaf) = self.leaf.as_ref() else { return; };
        let mut live: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for hid in 0..self.himos.len() {
            if self.leaf_for(hid).is_some() {
                for off in self.himos[hid].unique_values() {
                    live.insert(off);
                }
            }
        }
        leaf.rebuild_free_list(&live);
    }

    /// v6 (#88): リモート peer から届いた TieLeaf を apply。 bytes を local
    /// LeafStore に insert して cell に offset を張る (vid mapping 不要)。
    /// `remote_tie_apply` の Leaf 版。
    pub fn remote_tieleaf_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        bytes: &[u8],
        hlc: enchudb_oplog::Hlc,
    ) -> RemoteApply {
        let local = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return RemoteApply::Stale; }
        // request17 step 5: 受信 HLC でローカル clock を進める (これが無いと、
        // 相手の clock が先行している間ずっと自分のローカル write が負ける)。
        self.observe_remote_hlc(hlc);
        // 値を作る前に判定する — 不採用なら LeafStore に payload を確保しない。
        if !self.accepts_write(local, himo_id, hlc) {
            return RemoteApply::Stale;
        }
        self.entities.ensure_live(local);
        // v6 (#88): remote re-tie 上書きで旧 offset を回収。
        // #119: **insert → publish → free** の順 (逆順だと並行 reader が再利用 slot を読む)。
        let old = self.take_leaf_cell(local, hid);
        let value = match self.leaf_for(hid) {
            Some(leaf) => leaf.insert(bytes),
            None => self.vocab.insert(bytes), // pre-v6 / reserved: 旧 vocab fallback
        };
        if value == u32::MAX {
            // #167 / #59: payload を格納できなかった (commit を伸ばせない = ディスク
            // 満杯、 または vocab 天井)。 sentinel を cell に書くと read が壊れる。
            self.record_fault(
                FaultKind::DiskSpace,
                "受信 TieLeaf の payload を格納できない — apply を拒否 (空きが出てから再配送)",
            );
            return RemoteApply::RejectedCapacity;
        }
        self.set_cell_local(local, himo_id, value, hlc);
        self.free_leaf_offset(hid, old);
        Self::advance_table_next_local_for(&self.tables, local);
        // #209: relay append はここ (翻訳後の値しか持たない場所) から Syncer 側
        // (原 WireRecord を持つ場所、Engine::relay_record) に移動した。
        RemoteApply::Applied
    }

    pub fn remote_tie_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        value: u32,
        hlc: enchudb_oplog::Hlc,
    ) -> bool {
        let local = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return false; }
        self.observe_remote_hlc(hlc);
        // request17 step 5: LWW 判定は `set_cell` の内側だけ (A-2)。 sync 層で
        // 判定してから別関数で適用する形は、 呼び忘れれば黙って壊れる。
        if !self.set_cell_local(local, himo_id, value, hlc) {
            return false;
        }
        // entity 未確保ならローカル側の EntitySet に登録(eid は peer 側が決めた値)
        self.entities.ensure_live(local);
        // issue #47 fix: foreign local が我々の table eid_range 内に落ちた場合、
        // 次の `entity_in` が同 local を払出して live entity を上書きしないよう
        // `next_local` を `local + 1` まで前進させる。 これは `apply_oplog_op`
        // (WAL recover 経路、 engine.rs:3790) と対称の処理。
        Self::advance_table_next_local_for(&self.tables, local);
        // #209: relay append はここ (翻訳後の値しか持たない場所) から Syncer 側
        // (原 WireRecord を持つ場所、Engine::relay_record) に移動した。
        true
    }

    /// リモート peer から届いた Untie を apply。
    pub fn remote_untie_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        himo_id: u16,
        hlc: enchudb_oplog::Hlc,
    ) -> bool {
        let local = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        if hid >= self.himos.len() { return false; }
        self.observe_remote_hlc(hlc);
        if !self.clear_cell_local_freeing_leaf(local, himo_id, hlc) {
            return false;
        }
        // #209: relay append はここ (翻訳後の値しか持たない場所) から Syncer 側
        // (原 WireRecord を持つ場所、Engine::relay_record) に移動した。
        true
    }

    /// リモート peer から届いた Delete を apply。
    pub fn remote_delete_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        hlc: enchudb_oplog::Hlc,
    ) -> bool {
        let local = enchudb_oplog::eid_local(eid);
        self.observe_remote_hlc(hlc);
        // A-5: 削除の版数を残す。 これが永続することで、 配送バッファから
        // tombstone が消えた後も削除済み entity が復活しない (#140 の根)。
        // 版数の記録と本体の除去は `apply_delete_local` で不可分に扱う (再配送でも
        // 本体が必ず落ちる = 冪等)。
        if !self.apply_delete_local(local, hlc) {
            return false;
        }
        // #209: relay append はここ (翻訳後の値しか持たない場所) から Syncer 側
        // (原 WireRecord を持つ場所、Engine::relay_record) に移動した。
        true
    }

    /// リモート peer から届いた Content 書き込みを apply。
    pub fn remote_content_apply(
        &self,
        eid: enchudb_oplog::EntityId,
        key: &str,
        data: &[u8],
        hlc: enchudb_oplog::Hlc,
    ) -> RemoteApply {
        let local = enchudb_oplog::eid_local(eid);
        self.observe_remote_hlc(hlc);
        // legacy op (pre-0.9 WAL のみ)。 cell を持たないので版数は `HlcStore` の
        // key hash entry のまま (sync 側) だが、 tombstone 判定だけは engine に寄せる。
        if self.tombstone_blocks(eid, hlc) {
            return RemoteApply::Stale;
        }
        self.entities.ensure_live(local);
        if !self.contents.set(local, key, data) {
            // #59: content data 領域が満杯。 panic せず拒否 + 計上。
            self.record_fault(
                FaultKind::ContentSpace,
                "content data region is full — content write rejected (空きが出てから再配送)",
            );
            return RemoteApply::RejectedCapacity;
        }
        // issue #47 fix: Tie 経路と同じ理由で next_local を前進させる。
        Self::advance_table_next_local_for(&self.tables, local);
        // #209: relay append はここ (翻訳後の値しか持たない場所) から Syncer 側
        // (原 WireRecord を持つ場所、Engine::relay_record) に移動した。
        RemoteApply::Applied
    }

    /// リモート peer から届いた Vocab op を apply。
    /// `bytes` を local vocab に insert し、local_vid を取得して
    /// `(author_peer, remote_vid) → local_vid` の mapping を記録する。
    /// 後続の Tie { value: remote_vid } を受信したら `translate_remote_vid` で local_vid に変換。
    /// gossip 有効かつ自 vocab に新規追加された場合のみ Vocab op を `append_relayed` で WAL に
    /// 流す (HLC/author/署名は元のまま)。 既存 vocab に当たれば relay しない (重複防止)。
    pub fn remote_vocab_apply(
        &self,
        author_peer: enchudb_oplog::PeerId,
        remote_vid: u32,
        bytes: &[u8],
    ) {
        let local_vid = self.vocab.get_or_insert(bytes);
        {
            let mut map = self.peer_vocab_map.write().unwrap();
            if map.insert((author_peer, remote_vid), local_vid) != Some(local_vid) {
                // 新規 / 張り替え。 sidecar が古くなったので次の barrier で書き直す。
                self.peer_vocab_map_dirty
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        // #209: relay append はここから Syncer 側 (Engine::relay_record、原 vid の
        // まま素通し) に移動した。 旧実装の「local_vid に貼り直して relay」は
        // author 名義の vid namespace を relay namespace で汚染していた。
    }

    /// 0.8.4 issue #30: 受信 Vocab record の dedupe 用。 `(author_peer, remote_vid)`
    /// が既に `peer_vocab_map` に登録済みで、 かつ map 先 local_vid の bytes が
    /// 受信 bytes と一致するなら true (= 同じ record の再受信、 skip すべき)。
    ///
    /// 旧 behavior: sync apply_one は Vocab を **HLC dedupe せず常に applied 扱い**
    /// していた。 gossip_remote_apply ON 構成で同じ vocab record が無限に往復し、
    /// `entities_live` が膨れる amplification loop が出た (bisquit dogfood 実例)。
    /// 本 method を sync 側で先に叩いて、 既登録なら apply_one が false を返す。
    pub fn has_remote_vocab(
        &self,
        author_peer: enchudb_oplog::PeerId,
        remote_vid: u32,
        bytes: &[u8],
    ) -> bool {
        let map = self.peer_vocab_map.read().unwrap();
        if let Some(&local_vid) = map.get(&(author_peer, remote_vid)) {
            return self.vocab.get(local_vid) == bytes;
        }
        false
    }

    /// Symbol 型 himo の Tie を受信した際、remote vid を local vid に変換する。
    /// Symbol 以外の himo、または mapping 未登録なら元値をそのまま返す。
    pub fn translate_remote_vid(&self, author_peer: enchudb_oplog::PeerId, himo_id: u16, value: u32) -> u32 {
        let hid = himo_id as usize;
        if hid >= self.value_types.len() { return value; }
        // Tag / Leaf どちらも vocab 経由なので vid 翻訳が必要。
        match self.value_types[hid] {
            ValueType::Tag | ValueType::Leaf => {
                let map = self.peer_vocab_map.read().unwrap();
                *map.get(&(author_peer, value)).unwrap_or(&value)
            }
            _ => value,
        }
    }

    /// [`translate_remote_vid`] の厳密版: **mapping 未登録なら `None`**（fallback で
    /// 生値を返さない）。 vid は author ローカルな番号なので、 未翻訳の生値で index を
    /// 引くと**無関係な local entity に数値衝突でヒット**する。 PK bind（#141）の
    /// lookup はこちらを使い、 未翻訳なら bind をスキップすること — fresh store 同士は
    /// intern 順が対称で vid 番号がほぼ必ず衝突するため、 fallback 生値での lookup は
    /// 誤 bind → row 上書き → 恒久チャーンに直結する（0.17.0 で実際に発生）。
    /// vocab 翻訳が不要な himo（Number 等）は `Some(value)` を返す。
    pub fn try_translate_remote_vid(
        &self,
        author_peer: enchudb_oplog::PeerId,
        himo_id: u16,
        value: u32,
    ) -> Option<u32> {
        let hid = himo_id as usize;
        if hid >= self.value_types.len() { return Some(value); }
        match self.value_types[hid] {
            ValueType::Tag | ValueType::Leaf => {
                let map = self.peer_vocab_map.read().unwrap();
                map.get(&(author_peer, value)).copied()
            }
            _ => Some(value),
        }
    }

    /// local vocab を **bytes で**引く（`vocab_id` の bytes 版）。 PK bind が
    /// 「同 batch 内の未適用 Vocab record の bytes」から既存 row を照合するのに使う。
    pub fn vocab_id_bytes(&self, bytes: &[u8]) -> Option<u32> {
        self.vocab.lookup(bytes)
    }

    // ──── tie ────

    pub fn define_himo(&mut self, himo: &str, ht: ValueType, max_values: u32) {
        self.ensure_himo(himo, ht, max_values);
    }

    /// β-light step 4: 指定 table 配下の column として himo を定義する。
    /// 内部 storage 上は `"table.himo"` (例: `"users.age"`) という完全修飾名で
    /// 1 エントリ。 tie 経路は既存のまま (`himo_id` の vocab 検索で full name
    /// を引く)、 namespacing は単純な命名規約として扱う。
    ///
    /// 旧 API `define_himo` は引き続き bare 名で anonymous table に attach
    /// される。 同名 collision は engine では check しない (step 5+ で
    /// 必要なら追加)。
    pub fn define_himo_in(
        &mut self,
        table_name: &str,
        himo_name: &str,
        ht: ValueType,
        max_values: u32,
    ) -> Result<u32, String> {
        // 0.9.0: 本体は `&self` 版 (ensure_himo_dynamic_in) に移譲。 signature は
        // 互換維持のため `&mut self` / u32 のまま。
        self.ensure_himo_dynamic_in(table_name, himo_name, ht, max_values)
            .map(|hid| hid as u32)
    }

    /// 0.9.0: `define_himo_in` の `&self` 版。 `Arc<Engine>` (= concurrent mode)
    /// から lazy に himo を定義できる。 idempotent: 既に同名 himo が target
    /// table に attach 済みなら既存 hid を返す。 定義 (+ attach 移動) は
    /// `himo_def_lock` で直列化される。
    pub fn ensure_himo_dynamic_in(
        &self,
        table_name: &str,
        himo_name: &str,
        ht: ValueType,
        max_values: u32,
    ) -> Result<u16, String> {
        self.check_writable();
        if table_name.is_empty() {
            return Err("table name must be non-empty (use define_himo for anonymous)".into());
        }
        if himo_name.is_empty() {
            return Err("himo name must be non-empty".into());
        }
        if himo_name.contains('.') {
            return Err(format!(
                "himo name '{}' must not contain '.' (it is reserved as table separator)",
                himo_name,
            ));
        }
        let tid = self
            .tables
            .iter()
            .position(|t| t.name == table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;

        let full_name = format!("{}.{}", table_name, himo_name);

        // 定義 + attach 移動を 1 定義単位として himo_def_lock で直列化する
        // (並行する同名 ensure が同じ hid に収束するように)。
        let _guard = self.himo_def_lock.lock().unwrap();

        // 既存ならそのまま、 新規なら anonymous へ attach で定義。
        // 後者なら anonymous の himo_ids から外して target table へ移す。
        let hid = match self.himo_id(&full_name) {
            Some(idx) => idx,
            None => self.define_himo_slot_locked(&full_name, ht, max_values)? as usize,
        };
        let hid_u32 = hid as u32;

        // 既に target table に attach 済みなら何もしない (重複 define_himo_in)
        let cur_tid = self.himo_to_table[hid].load(std::sync::atomic::Ordering::Relaxed);
        if cur_tid == tid as TableId {
            return Ok(hid as u16);
        }

        // 新規時は ANONYMOUS_TABLE へ attach されているので、 別 table 既属の
        // 場合は migrate する形 (実用上は新規時のみ通る)。
        self.tables[cur_tid as usize]
            .himo_ids
            .write()
            .unwrap()
            .retain(|&h| h != hid_u32);
        self.tables[tid].himo_ids.write().unwrap().push(hid_u32);
        self.himo_to_table[hid].store(tid as TableId, std::sync::atomic::Ordering::Relaxed);

        self.try_persist_tables();
        Ok(hid as u16)
    }

    /// β-light step 5: `Ref` 型 himo を target_table と紐付けて定義する。
    /// 以降の `tie` / `tie_ref` / `tie_async` で target_eid が target_table の
    /// eid 範囲に収まっているか engine が validate する。
    pub fn define_ref_in(
        &mut self,
        table_name: &str,
        himo_name: &str,
        target_table: &str,
    ) -> Result<u32, String> {
        self.check_writable();
        // target_table 存在チェック
        let target_tid = self
            .tables
            .iter()
            .position(|t| t.name == target_table)
            .ok_or_else(|| format!("target table '{}' not found", target_table))?
            as TableId;

        // Ref として himo 登録 (define_himo_in が table_name / himo_name の
        // 各種チェックを行う)
        let hid = self.define_himo_in(table_name, himo_name, ValueType::Ref, 0)?;

        // 所属 table の fk_refs に entry を追加 (idempotent)
        let owner_tid =
            self.himo_to_table[hid as usize].load(std::sync::atomic::Ordering::Relaxed);
        let entry = (hid, target_tid);
        let owner = &mut self.tables[owner_tid as usize];
        if !owner.fk_refs.iter().any(|e| *e == entry) {
            owner.fk_refs.push(entry);
        }
        self.try_persist_tables();
        Ok(hid)
    }

    /// β-light step 6: tie 対象 eid が himo の所属 table eid_range に収まるか
    /// validate。 これが β-light の win の本体: 「table-local positions」 を
    /// BucketCylinder の eid_offset 機構で自然に実現する。
    ///
    /// 動作:
    ///   - anonymous (id=0) かつ open-ended (eid_range_hi == u32::MAX) は
    ///     validation スキップ (= 旧 API 完全互換)
    ///   - それ以外 (= define_table 後 / 非 anonymous table) は eid が
    ///     [eid_range_lo, eid_range_hi) に収まらないと panic
    ///
    /// hot path: define_table が未呼び出しなら `tables.len() == 1` で 1 load で
    /// 抜ける。 これにより legacy 経路の tie_async hot path は ~1 ns コストに収まる。
    /// 一度でも user table を定義したら full validation 経路に入る。
    #[inline(always)]
    fn validate_eid_for_himo(&self, hid: usize, eid_local: u32) {
        // fast path: anonymous のみ存在 (= define_table 未) → 全 eid 受け入れ
        if self.tables.len() <= 1 {
            return;
        }
        let Some(tid) = self.himo_table_get(hid) else {
            return;
        };
        let tid = tid as usize;
        if tid >= self.tables.len() {
            return;
        }
        let table = &self.tables[tid];
        // anonymous は close されてれば eid_range_hi != u32::MAX、 closed 後も
        // tie が来たら validate される
        if table.eid_range_hi == u32::MAX {
            return;
        }
        assert!(
            table.contains(eid_local),
            "tie eid {} not in himo's table '{}' eid extents {:?}",
            eid_local, table.name, table.extents(),
        );
    }

    /// β-light step 5: Ref tie の FK validation。 himo が Ref 型で fk_refs
    /// entry を持つ場合、 target_eid が target_table の eid 範囲内かを assert。
    ///
    /// hot path 性能:
    ///   - 非 Ref himo は最初の `value_types[hid] != Ref` で即 return (~1 ns)
    ///   - Ref himo は fk_refs (typically 1-5 件) の線形検索 (~5-10 ns)
    #[inline(always)]
    fn validate_ref_tie(&self, hid: usize, target_eid: u32) {
        if hid >= self.value_types.len() {
            return;
        }
        if self.value_types[hid] != ValueType::Ref {
            return;
        }
        let Some(owner_tid) = self.himo_table_get(hid) else {
            return;
        };
        let owner_tid = owner_tid as usize;
        if owner_tid >= self.tables.len() {
            return;
        }
        let owner = &self.tables[owner_tid];
        let target_tid = match owner.fk_refs.iter().find(|(h, _)| *h == hid as u32) {
            Some(&(_, t)) => t as usize,
            // fk_refs に entry なし: Ref 型だが target 不明 (旧 API 経路)、 validation スキップ
            None => return,
        };
        if target_tid >= self.tables.len() {
            return;
        }
        let target = &self.tables[target_tid];
        assert!(
            target.contains(target_eid),
            "FK violation: Ref himo (id {}) points to eid {} outside target table '{}' extents {:?}",
            hid, target_eid, target.name, target.extents(),
        );
    }

    pub fn tie_text(&mut self, eid: enchudb_oplog::EntityId, himo: &str, value: &str) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let hid = self.ensure_himo(himo, ValueType::Tag, 0);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // v6 (#88): Leaf は LeafStore へ。 &mut self (build phase) は WAL emit しない
        // ので leaf.insert + cell set のみ。 re-tie 上書きは旧 offset を free。
        if let Some(leaf) = self.leaf_for(hid) {
            // #119: insert → publish → free に揃える (この経路は `&mut self` = 並行 reader
            // なしなので実害はないが、 3 経路で順序が揃っていないと事故の温床になる)。
            let old = self.himos[hid].get_value(eid);
            let off = leaf.insert(value.as_bytes());
            if off == u32::MAX {
                // #167: leaf payload を書けなかった (commit を伸ばせない = ディスク
                // 満杯)。 sentinel を cell に書くと read が壊れるので write を拒否。
                self.record_fault(
                    FaultKind::DiskSpace,
                    "leaf payload の格納に必要な commit を伸ばせない — text write を拒否",
                );
                return;
            }
            self.himos[hid].set(eid, off);
            if let Some(old) = old { leaf.free(old); }
            return;
        }
        // Tag は dedupe (get_or_insert)、Leaf は新規 id 発行 (insert)。
        let vid = match self.value_types[hid] {
            ValueType::Tag => self.vocab.get_or_insert(value.as_bytes()),
            ValueType::Leaf => self.vocab.insert(value.as_bytes()),
            ht => panic!("tie_text on non-text himo '{}': {:?}", himo, ht),
        };
        if vid == u32::MAX {
            // #59: vocab 満杯 → `insert`/`get_or_insert` が予約 sentinel を返した。
            // panic せず write を拒否 + 計上 (sentinel を cell に書くと read 側が
            // 「値なし」 と区別できない壊れ方をする)。
            self.record_fault(
                FaultKind::VocabSpace,
                "vocabulary is full (vocab_max_entries 到達) — text write rejected. \
                 GrowableOptions { vocab_max_entries: Some(n), .. } で上げられるが、\
                 header 焼き込みなので既存 DB は再作成が必要",
            );
            return;
        }
        self.himos[hid].set(eid, vid);
    }

    pub fn tie(&mut self, eid: enchudb_oplog::EntityId, himo: &str, value: u32) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        if value == u32::MAX {
            // #59: sentinel 値は cell に入らない。 panic せず write を拒否 + 計上。
            self.record_fault(
                FaultKind::ValueOutOfRange,
                "tie value == u32::MAX (sentinel reserved)",
            );
            return;
        }
        let hid = self.ensure_himo(himo, ValueType::Number, 0);
        debug_assert!(self.value_types[hid] == ValueType::Number || self.value_types[hid] == ValueType::Ref, "tie on non-Value himo '{}'", himo);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // β-light step 5: Ref himo は target_table の eid range を validate
        self.validate_ref_tie(hid, value);
        self.himos[hid].set(eid, value);
    }

    pub fn tie_ref(&mut self, eid: enchudb_oplog::EntityId, himo: &str, target_eid: enchudb_oplog::EntityId) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let target_eid = enchudb_oplog::eid_local(target_eid);
        if target_eid == u32::MAX {
            self.record_fault(
                FaultKind::ValueOutOfRange,
                "tie_ref target_eid >= u32::MAX (sentinel reserved)",
            );
            return;
        }
        let hid = self.ensure_himo(himo, ValueType::Ref, 0);
        debug_assert!(self.value_types[hid] == ValueType::Ref || self.value_types[hid] == ValueType::Number, "tie_ref on non-Ref himo '{}'", himo);
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, eid);
        // β-light step 5: target_eid が target_table の eid range 内か
        self.validate_ref_tie(hid, target_eid);
        self.himos[hid].set(eid, target_eid);
    }

    // ──── tie（定義済み紐、&self で並行書き込み可）────

    /// 定義済みの紐に文字列を張る。&selfで呼べる（Arc共有のまま書き込み可）。
    /// 紐が未定義ならpanic。define_himo を先に呼ぶこと。
    pub fn tie_text_to(&self, eid: enchudb_oplog::EntityId, himo: &str, value: &str) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_text_to_by_id(eid, hid, value);
    }

    /// `tie_text_to` の himo_id 直指定版。 hot path で per-call の HashMap lookup を
    /// 避けたい時に。 起動時に `himo_id(&str)` で解決して u16 を cache しておく。
    pub fn tie_text_to_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: &str) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // v6 (#88): Leaf は LeafStore へ (vocab の vid は使わない)。 sync は bytes 同乗 TieLeaf。
        // #106: 書込順は insert(新 slot) → set(column publish) → free(旧 slot) の順。
        //   - 旧: free → insert (best-fit で旧 slot 即再利用) → set だと、 reader が
        //     set 前に握った旧 offset が別世代 bytes に上書きされ torn read になった。
        //   - 新: 新 slot は誰も参照しない状態で payload を書き切ってから column を
        //     publish するので in-place 上書きが消える。 旧 slot は最後に free するので、
        //     旧 offset を掴んだ reader は column 再読 (get_text_owned) で stale を検出。
        if let Some(leaf) = self.leaf_for(hid) {
            let bytes = value.as_bytes();
            let old = self.himos[hid].get_value(eid);
            let off = leaf.insert(bytes);
            if off == u32::MAX {
                // #167: leaf payload を書けなかった (commit を伸ばせない = ディスク
                // 満杯)。 sentinel を cell に書くと read が壊れるので write を拒否。
                self.record_fault(
                    FaultKind::DiskSpace,
                    "leaf payload の格納に必要な commit を伸ばせない — text write を拒否",
                );
                return;
            }
            // request17 step 4: WAL 先行で採番 → 値と版数を不可分に書く。 不採用なら
            // **今 insert した payload** を捨てる (cell は旧 offset を指したままなので
            // 旧 payload は生きている)。
            let oplog_eid = self.oplog_eid(eid);
            let hlc = self.append_local_op(enchudb_oplog::oplog::Op::TieLeaf {
                eid: oplog_eid,
                himo_name: &self.himo_names[hid],
                himo_kind: self.value_types[hid] as u8,
                bytes,
            });
            if !self.set_cell_local(eid, himo_id, off, hlc) {
                leaf.free(off);
                self.warn_local_write_rejected(eid, himo_id, hlc);
                return;
            }
            if let Some(old) = old { leaf.free(old); }
            return;
        }
        // Tag は dedupe、Leaf は常に新規 id。
        let vid = match self.value_types[hid] {
            ValueType::Tag => self.vocab.get_or_insert(value.as_bytes()),
            ValueType::Leaf => self.vocab.insert(value.as_bytes()),
            ht => panic!("tie_text_to_by_id on non-text himo_id {}: {:?}", himo_id, ht),
        };
        if vid == u32::MAX {
            // #59: vocab 満杯 → `insert`/`get_or_insert` が予約 sentinel を返した。
            // panic せず write を拒否 + 計上 (sentinel を cell に書くと read 側が
            // 「値なし」 と区別できない壊れ方をする)。
            self.record_fault(
                FaultKind::VocabSpace,
                "vocabulary is full (vocab_max_entries 到達) — text write rejected. \
                 GrowableOptions { vocab_max_entries: Some(n), .. } で上げられるが、\
                 header 焼き込みなので既存 DB は再作成が必要",
            );
            return;
        }
        // WAL に Vocab + Tie を流す。 schema layer (enchudb-schema) は同期版の
        // tie_text_to を経由するため、 ここで append しないと WAL が空のままで
        // peer 同期が成立しない (publish 側が iter_committed で 0 件を見る).
        // 0.7.0: reserved table (`_sync_ops` 等) への write は oplog skip。
        //
        // request17 step 4: **Vocab → Tie の順に append** し、 Tie に載った HLC で
        // 値と版数を書く。 順序が要るのは transport が record を HLC 順に配るため —
        // Tie が先に採番されると受信側で vid mapping が未登録のまま Tie を適用し、
        // 生の remote vid で誤 bind する (#141)。
        let reserved = self.himo_is_in_engine_internal_table(hid);
        let hlc = if reserved {
            enchudb_oplog::Hlc::ZERO
        } else {
            let oplog_eid = self.oplog_eid(eid);
            if let Some(wal) = self.oplog.as_ref() {
                // Vocab は cell を持たない (= 版数の対象外)。
                let _ = wal.append(enchudb_oplog::oplog::Op::Vocab { vid, bytes: value.as_bytes() });
            }
            self.append_local_op(
                enchudb_oplog::oplog::Op::Tie { eid: oplog_eid, himo_id, value: vid },
            )
        };
        if !self.set_cell_local(eid, himo_id, vid, hlc) {
            self.warn_local_write_rejected(eid, himo_id, hlc);
        }
    }

    /// 0.7.0: 当該 himo が reserved table 配下か (= `_*` 表)。
    /// reserved table への tie は oplog に再 append しない (= 2 重書き防止)。
    /// hot path で `tie_*_to_by_id` から呼ばれる、 inline で軽量化。
    #[inline]
    fn himo_is_in_reserved_table(&self, himo_id: usize) -> bool {
        match self.himo_table_get(himo_id) {
            Some(tid) => self.tables.get(tid as usize)
                .map(|td| td.is_reserved())
                .unwrap_or(false),
            None => false,
        }
    }

    /// request19: その himo が **engine 自身の内部 table** (`_sync_ops` / `_sync_peers`)
    /// に属するか。
    ///
    /// この 2 つへの write を WAL に積んではいけない — `_sync_ops` の行は **WAL record
    /// から作られる**ので、 積むと WAL が自分自身を食う (record を書くたびに record が
    /// 増える)。 一方 **アプリが `define_reserved_table` で作った local-only table は
    /// WAL に積む** — 配送しないだけで、 crash 後に replay されないと
    /// 「本体の行は在るのに、 それに対する観測記録だけ消えた」 が起きる (= request19 の
    /// 動機そのもの)。
    fn himo_is_in_engine_internal_table(&self, himo_id: usize) -> bool {
        match self.himo_table_get(himo_id) {
            Some(tid) => self.tables.get(tid as usize)
                .map(|td| is_engine_internal_table(&td.name))
                .unwrap_or(false),
            None => false,
        }
    }

    /// 0.7.0: `tie_text_to_by_id` の binary 版。 任意 byte slice を vocab に
    /// insert (Leaf なら dedupe なし) し、 himo に紐付ける。 `_sync_ops.payload`
    /// (= oplog record の生 wire bytes) 等、 UTF-8 制約を持たない data を書く用。
    pub fn tie_bytes_to_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: &[u8]) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // v6 (#88): Leaf は LeafStore へ。 re-tie 上書きは旧 offset を free。
        if let Some(leaf) = self.leaf_for(hid) {
            // #119: **insert → publish → free** の順に守る (旧 slot を先に free すると、
            // 同サイズ re-tie では best-fit が「たった今 free した hole」を必ず再利用し、
            // 旧 offset を掴んでいる並行 reader が再利用 slot を読んで seqlock retry を
            // 使い切り None になる — content 経路で実測 8,132/60,463 件)。
            // 既に `tie_text_to_by_id` はこの順序。
            let old = self.himos[hid].get_value(eid);
            let off = leaf.insert(value);
            if off == u32::MAX {
                // #167: leaf payload を書けなかった (commit を伸ばせない = ディスク
                // 満杯)。 sentinel を cell に書くと read が壊れるので write を拒否。
                self.record_fault(
                    FaultKind::DiskSpace,
                    "leaf payload の格納に必要な commit を伸ばせない — text write を拒否",
                );
                return;
            }
            // request17 step 4: WAL 先行で採番。 不採用なら今 insert した payload を捨てる。
            let oplog_eid = self.oplog_eid(eid);
            let hlc = self.append_local_op(enchudb_oplog::oplog::Op::TieLeaf {
                eid: oplog_eid,
                himo_name: &self.himo_names[hid],
                himo_kind: self.value_types[hid] as u8,
                bytes: value,
            });
            if !self.set_cell_local(eid, himo_id, off, hlc) {
                leaf.free(off);
                self.warn_local_write_rejected(eid, himo_id, hlc);
                return;
            }
            if let Some(old) = old { leaf.free(old); }
            return;
        }
        let vid = match self.value_types[hid] {
            ValueType::Tag => self.vocab.get_or_insert(value),
            ValueType::Leaf => self.vocab.insert(value),
            ht => panic!("tie_bytes_to_by_id on non-text himo_id {}: {:?}", himo_id, ht),
        };
        if vid == u32::MAX {
            // #59: vocab 満杯 → `insert`/`get_or_insert` が予約 sentinel を返した。
            // panic せず write を拒否 + 計上 (sentinel を cell に書くと read 側が
            // 「値なし」 と区別できない壊れ方をする)。
            self.record_fault(
                FaultKind::VocabSpace,
                "vocabulary is full (vocab_max_entries 到達) — text write rejected. \
                 GrowableOptions { vocab_max_entries: Some(n), .. } で上げられるが、\
                 header 焼き込みなので既存 DB は再作成が必要",
            );
            return;
        }
        // reserved table への write は oplog 再 append を skip (= 2 重書き防止)。
        // request17 step 4: Vocab → Tie/TieNamed の順に append し (transport は HLC 順に
        // 配るので依存順を崩せない)、 Tie に載った HLC で値と版数を書く。
        let reserved = self.himo_is_in_engine_internal_table(hid);
        let hlc = if reserved {
            enchudb_oplog::Hlc::ZERO
        } else {
            let oplog_eid = self.oplog_eid(eid);
            if let Some(wal) = self.oplog.as_ref() {
                // Vocab は cell を持たない (= 版数の対象外)。
                let _ = wal.append(enchudb_oplog::oplog::Op::Vocab { vid, bytes: value });
            }
            if self.himo_is_content(hid) {
                // 0.9.0: 動的 content himo は id が peer 間で揃わないため名前で運ぶ
                self.append_local_op(enchudb_oplog::oplog::Op::TieNamed {
                    eid: oplog_eid,
                    himo_name: &self.himo_names[hid],
                    himo_kind: self.value_types[hid] as u8,
                    value: vid,
                })
            } else {
                self.append_local_op(
                    enchudb_oplog::oplog::Op::Tie { eid: oplog_eid, himo_id, value: vid },
                )
            }
        };
        if !self.set_cell_local(eid, himo_id, vid, hlc) {
            self.warn_local_write_rejected(eid, himo_id, hlc);
        }
    }

    /// 定義済みの紐にu32値を張る。&selfで呼べる。
    pub fn tie_to(&self, eid: enchudb_oplog::EntityId, himo: &str, value: u32) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_to_by_id(eid, hid, value);
    }

    /// `tie_to` の himo_id 直指定版。 hot path 用 (string lookup を避ける)。
    pub fn tie_to_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: u32) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        if value == u32::MAX {
            // #59: sentinel 値は cell に入らない。 panic せず write を拒否 + 計上。
            self.record_fault(
                FaultKind::ValueOutOfRange,
                "tie value == u32::MAX (sentinel reserved)",
            );
            return;
        }
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // Tag / Leaf 型 (vocab_id を value として持つ) も許可。 schema 層が
        // 起動時に解決済みの table_vid を marker himo に張る hot path 用途で
        // 必要 (request2.md 提案)。 caller 責任で vocab に既に居る id を渡すこと。
        //
        // request17 step 4: 値と版数は `set_cell` で不可分に書き、 **同じ HLC** を
        // WAL record にも載せる (`append_at_hlc`)。 0.7.0: reserved table への write は
        // sync 対象外なので採番しない (版数不明のまま = 従来どおり)。
        let reserved = self.himo_is_in_engine_internal_table(hid);
        let hlc = if reserved {
            enchudb_oplog::Hlc::ZERO
        } else {
            let oplog_eid = self.oplog_eid(eid);
            self.append_local_op(enchudb_oplog::oplog::Op::Tie { eid: oplog_eid, himo_id, value })
        };
        if !self.set_cell_local(eid, himo_id, value, hlc) {
            self.warn_local_write_rejected(eid, himo_id, hlc);
        }
    }

    /// 定義済みの紐にentity参照を張る。&selfで呼べる。
    pub fn tie_ref_to(&self, eid: enchudb_oplog::EntityId, himo: &str, target_eid: enchudb_oplog::EntityId) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_ref_to_by_id(eid, hid, target_eid);
    }

    /// `tie_ref_to` の himo_id 直指定版。 hot path 用。
    pub fn tie_ref_to_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, target_eid: enchudb_oplog::EntityId) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let target_eid = enchudb_oplog::eid_local(target_eid);
        if target_eid == u32::MAX {
            self.record_fault(
                FaultKind::ValueOutOfRange,
                "tie_ref target_eid >= u32::MAX (sentinel reserved)",
            );
            return;
        }
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        debug_assert!(
            self.value_types[hid] == ValueType::Ref || self.value_types[hid] == ValueType::Number,
            "tie_ref_to_by_id on non-Ref himo_id {}", himo_id,
        );
        // request17 step 4: WAL 先行で採番し、 その HLC で値と版数を不可分に書く。
        let oplog_eid = self.oplog_eid(eid);
        let hlc = self.append_local_op(
            enchudb_oplog::oplog::Op::Tie { eid: oplog_eid, himo_id, value: target_eid },
        );
        if !self.set_cell_local(eid, himo_id, target_eid, hlc) {
            self.warn_local_write_rejected(eid, himo_id, hlc);
        }
    }

    // ──── untie ────

    pub fn untie(&self, eid: enchudb_oplog::EntityId, himo: &str) {
        if let Some(hid) = self.himo_id(himo) {
            self.untie_by_id(eid, hid as u16);
        }
    }

    /// `untie` の himo_id 直指定版。 未定義の himo_id (= range 外) は debug_assert で
    /// panic、 release では silently no-op (string 版が未定義 himo を no-op 扱いするのと
    /// 整合)。
    pub fn untie_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        if hid >= self.himos.len() { return; }
        // request17 step 4: untie も版数を進める (進めないと外した cell に古い tie が
        // 蘇る)。 Leaf payload の解放は **採用が決まってから**。
        let oplog_eid = self.oplog_eid(eid);
        let hlc = self.append_local_op(
            enchudb_oplog::oplog::Op::Untie { eid: oplog_eid, himo_id },
        );
        if !self.clear_cell_local_freeing_leaf(eid, himo_id, hlc) {
            self.warn_local_write_rejected(eid, himo_id, hlc);
        }
    }

    // ──── delete ────

    pub fn delete(&self, eid: enchudb_oplog::EntityId) {
        self.check_writable();
        let eid = enchudb_oplog::eid_local(eid);
        // request17 step 4 (A-5): 削除は himo を持たないので eid 空間の tombstone
        // column に版数を記録する。 これが永続することで、 配送バッファから
        // tombstone が消えた後も削除済み entity が復活しない (#140 の根)。
        let oplog_eid = self.oplog_eid(eid);
        let hlc = self.append_local_op(enchudb_oplog::oplog::Op::Delete { eid: oplog_eid });
        if !self.apply_delete_local(eid, hlc) {
            self.warn_local_write_rejected(eid, u16::MAX, hlc);
        }
    }

    // ──── トランザクション ────

    /// WAL 有効時のみ意味あり: Commit marker を WAL に append する。
    /// v4: undo ログ廃止に伴い、 rollback API も廃止 (詳細は CLAUDE.md / lib.rs)。
    pub fn commit(&self) {
        if let Some(wal) = self.oplog.as_ref() {
            let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
        }
    }

    // ──── content ────

    /// eid が属する named table 名。 どの named table の range にも入らなければ
    /// None (= anonymous 扱い)。
    fn table_name_of_local(&self, local: u32) -> Option<String> {
        for t in self.tables.iter() {
            if t.name.is_empty() { continue; }
            if t.contains(local) {
                return Some(t.name.clone());
            }
        }
        None
    }

    /// 0.9.0 (#81): content key に対応する `_c_{key}` Leaf himo を lazy 確保。
    /// eid が named table 所属ならその table に、 それ以外は anonymous に定義する。
    fn ensure_content_himo(&self, local: u32, key: &str) -> u16 {
        assert!(
            !key.contains('.'),
            "content key '{}' must not contain '.' (himo 名の table separator と衝突。 \
             0.9.0 で content は Leaf himo 格納に変わった)",
            key,
        );
        let himo_name = format!("_c_{key}");
        let res = match self.table_name_of_local(local) {
            Some(t) => self.ensure_himo_dynamic_in(&t, &himo_name, ValueType::Leaf, 0),
            None => self.ensure_himo_dynamic(&himo_name, ValueType::Leaf, 0),
        };
        res.unwrap_or_else(|e| panic!("content key '{key}': himo allocation failed: {e}"))
    }

    /// full name ("table.himo" or "himo") で himo を lazy 解決。 table 部が既知
    /// table なら所属付きで、 それ以外は anonymous へ定義する (TieNamed replay 用)。
    fn ensure_himo_by_full_name(&self, full_name: &str, ht: ValueType) -> Result<u16, String> {
        if let Some((table, himo)) = full_name.split_once('.') {
            if self.tables.iter().any(|t| t.name == table) {
                return self.ensure_himo_dynamic_in(table, himo, ht, 0);
            }
        }
        self.ensure_himo_dynamic(full_name, ht, 0)
    }

    /// 0.9.0: sync 受信側用の公開版 — TieNamed の himo を名前で解決 (無ければ
    /// lazy 定義) して local hid を返す。 `himo_kind` は wire 上の生 u8。
    pub fn ensure_himo_named(&self, full_name: &str, himo_kind: u8) -> Result<u16, String> {
        self.ensure_himo_by_full_name(full_name, ValueType::from_byte(himo_kind))
    }

    /// hid が content 互換層の himo (`_c_` prefix) か。 これらは動的定義のため
    /// peer 間で himo_id が揃わず、 WAL/wire には Tie ではなく TieNamed で乗せる。
    fn himo_is_content(&self, hid: usize) -> bool {
        self.himo_names.get(hid).map_or(false, |n| {
            n.rsplit('.').next().map_or(false, |leaf| leaf.starts_with("_c_"))
        })
    }

    /// 既存の content himo id を引く (定義はしない)。 read 経路用。
    fn content_himo_id(&self, local: u32, key: &str) -> Option<u16> {
        let full = match self.table_name_of_local(local) {
            Some(t) => format!("{t}._c_{key}"),
            None => format!("_c_{key}"),
        };
        self.himo_id(&full).map(|h| h as u16)
    }

    /// entity に任意 bytes を添付する (sync 版)。
    ///
    /// 0.9.0: 保存先を旧 ContentStore region から **`_c_{key}` Leaf himo**
    /// (bytes は vocab 格納) に変更。 write は Vocab+Tie として WAL / sync /
    /// HLC / changefeed に自然に乗る。 これにより旧経路の既知バグ群
    /// (mod-16 key hash 衝突・index torn read・sync/WAL 非対応・delete 残留)
    /// が構造ごと退役する。 旧 content region は**凍結アーカイブ**:
    /// `get_content` の fallback でのみ読み、 書き込みは一切しない。
    pub fn content(&self, eid: enchudb_oplog::EntityId, key: &str, data: &[u8]) {
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        let hid = self.ensure_content_himo(local, key);
        self.tie_bytes_to_by_id(eid, hid, data);
    }

    pub fn get_content(&self, eid: enchudb_oplog::EntityId, key: &str) -> Option<&[u8]> {
        let local = enchudb_oplog::eid_local(eid);
        // 新経路 (`_c_{key}` Leaf himo) 優先
        if let Some(hid) = self.content_himo_id(local, key) {
            if let Some(vid) = self.get_by_id(eid, hid) {
                return Some(self.text_value(hid as usize, vid));
            }
        }
        // 旧 content region fallback (pre-0.9 data の read-through 互換)
        self.contents.get(local, key)
    }

    /// #119: read-while-write でも torn read しない content 取得 (所有 copy 返し)。
    ///
    /// `get_content` は借用返しで、 新経路 (`_c_{key}` Leaf himo) では writer 稼働中に
    /// slot 再利用で借用が死ぬ / seqlock verify を通さないので torn bytes を掴む。
    /// 並行 read する consumer は本 API を使う ([`get_text_owned`] の content 版)。
    ///
    /// 旧 content region (pre-0.9) は 0.9.0 以降 **凍結アーカイブで書き込みゼロ** なので
    /// copy のみ (verify 不要)。
    pub fn get_content_owned(&self, eid: enchudb_oplog::EntityId, key: &str) -> Option<Vec<u8>> {
        let local = enchudb_oplog::eid_local(eid);
        if let Some(hid) = self.content_himo_id(local, key) {
            // cell が設定されていれば新経路の結果をそのまま返す。 `text_owned_by_id` の
            // **retry 枯渇 (None)** で legacy region に fallback すると、 0.9.0 以降凍結して
            // いる古いアーカイブ値が蘇ってしまう (= 新しい値を書いたのに古い値が読める)。
            // fallback は「新経路に cell が無い」ときだけに限定する。
            if self.himos[hid as usize].get_value(local).is_some() {
                return self.text_owned_by_id(hid as usize, local);
            }
        }
        self.contents.get(local, key).map(|b| b.to_vec())
    }

    // ──── changefeed (WAL 変更通知) ────

    /// WAL に durable 化した record を listener に push する。
    /// consumer スレッドが背景 fsync 完了後に発火、HLC 昇順で渡す。
    /// 詳細は [`crate::changefeed`] のドキュメント参照。
    ///
    /// 初回 listener 追加時に emit cursor を現在の `wal.head()` に揃えるので、
    /// 過去の commit は流れない(必要なら `audit()` で取得して resume すること)。
    pub fn add_change_listener(
        &self,
        listener: std::sync::Arc<dyn crate::changefeed::ChangeListener>,
    ) {
        let was_empty = {
            let mut guard = self.change_listeners.write().unwrap();
            let was_empty = guard.is_empty();
            guard.push(listener);
            was_empty
        };
        // 初回登録時は cursor を現在の wal.head() に揃える(過去 record を流さない)
        if was_empty {
            if let Some(wal) = self.oplog.as_ref() {
                self.change_emit_offset
                    .store(wal.head(), std::sync::atomic::Ordering::Release);
            }
        }
    }

    /// 登録済み listener 数(テスト/監視用)。
    pub fn change_listener_count(&self) -> usize {
        self.change_listeners.read().unwrap().len()
    }

    // ──── blob store (大容量バイナリ: 画像/動画/モデル等) ────

    /// 大容量 blob の外部保管を注入する。通常 `LocalBlobStore` を渡す。
    /// 読み書きは `put_blob`/`get_blob` 経由、または `blob_store()` で直接取得。
    pub fn set_blob_store(&self, store: std::sync::Arc<dyn crate::blob_store::BlobStore>) {
        *self.blob_store.write().unwrap() = Some(store);
    }

    /// 注入済み blob store への Arc を返す。未設定なら None。
    pub fn blob_store(&self) -> Option<std::sync::Arc<dyn crate::blob_store::BlobStore>> {
        self.blob_store.read().unwrap().clone()
    }

    /// blob を書き込んで BlobId(sha-256、32B) を返す。blob store 未設定なら None。
    pub fn put_blob(
        &self,
        data: &[u8],
    ) -> Option<Result<crate::blob_store::BlobId, crate::blob_store::BlobError>> {
        self.blob_store().map(|s| s.put(data))
    }

    /// blob を取得。blob store 未設定なら None、存在しないなら Ok(None)。
    pub fn get_blob(
        &self,
        id: &crate::blob_store::BlobId,
    ) -> Option<Result<Option<Vec<u8>>, crate::blob_store::BlobError>> {
        self.blob_store().map(|s| s.get(id))
    }

    /// blob の存在チェック。blob store 未設定なら false。
    pub fn blob_exists(&self, id: &crate::blob_store::BlobId) -> bool {
        self.blob_store().map(|s| s.exists(id)).unwrap_or(false)
    }

    // ──── get ────

    pub fn get_text(&self, eid: enchudb_oplog::EntityId, himo: &str) -> Option<&[u8]> {
        let eid = enchudb_oplog::eid_local(eid);
        let hid = self.himo_id(himo)?;
        // Tag は vocab、 routed-Leaf (#88) は LeafStore、 reserved Leaf は vocab。
        match self.value_types[hid] {
            ValueType::Tag | ValueType::Leaf => {
                let raw = self.himos[hid].get_value(eid)?;
                Some(self.text_value(hid, raw))
            }
            _ => None,
        }
    }

    /// #106: read-while-write でも torn read しない安全な text 取得 (所有 copy 返し)。
    ///
    /// - **Leaf (routed)**: LeafStore の live mmap を `try_read` (slot gen seqlock) で
    ///   bounds-safe に copy し、 さらに copy の前後で column offset を再読して一致する
    ///   まで retry する。 これで (a) 別 offset への relocation = column 変化、
    ///   (b) 同 offset 再利用 / in-place torn = slot gen 変化、 の両方を検出する。
    ///   破れた slot を掴んでも `try_read` が `Retry` を返すので panic しない。
    /// - **Tag / reserved Leaf**: append-only vocab は不変なので copy のみ (retry 不要)。
    ///
    /// 借用を返す [`get_text`] は writer 稼働中の Leaf に対しては aliasing UB になる
    /// ため、 並行 read する consumer は本 API を使う。
    pub fn get_text_owned(&self, eid: enchudb_oplog::EntityId, himo: &str) -> Option<Vec<u8>> {
        let eid = enchudb_oplog::eid_local(eid);
        let hid = self.himo_id(himo)?;
        self.text_owned_by_id(hid, eid)
    }

    /// `get_text_owned` / `get_content_owned` 共通の本体 (hid 解決済み・local eid)。
    ///
    /// - **Leaf (routed)**: LeafStore の live mmap を `try_read` (slot gen seqlock) で
    ///   bounds-safe に copy し、 copy の前後で column offset を再読して一致するまで retry。
    ///   (a) 別 offset への relocation = column 変化、 (b) 同 offset 再利用 / in-place torn =
    ///   slot gen 変化、 の両方を検出する。
    /// - **Tag / reserved Leaf**: append-only vocab は不変なので copy のみ (retry 不要)。
    fn text_owned_by_id(&self, hid: usize, eid_local: u32) -> Option<Vec<u8>> {
        match self.value_types[hid] {
            ValueType::Tag | ValueType::Leaf => match self.leaf_for(hid) {
                // routed-Leaf: seqlock verify で torn read / stale を排除。
                Some(leaf) => {
                    // #119: 単一 cell を止めどなく re-tie する writer と競ると、 retry を
                    // **間を置かずに** 64 回消費して「値が無い」と区別できない None を返して
                    // いた (実測 3〜9 件 / 33 万 read)。 spin → yield の backoff と上限
                    // 256 回化で緩和した。
                    //
                    // #128: それでも「一律 256 回で give-up」 は、 CPU contention 下で
                    // writer loop と位相が噛み合う (resonance) と 256 連敗して silent None
                    // を返す (issue119 test の並列実行 flaky、 実測 10/30 run fail)。
                    // 値が存在する限り None は契約違反なので、 **進捗の無い連敗** だけを
                    // 数える方式に変更:
                    // - column offset (raw) か slot stamp (gen/ss) が動いた = writer 前進中
                    //   の生きた race → stall を 0 に戻して続行 (seqlock reader と同じ
                    //   「書き続けられる間は待つ」 契約)
                    // - 同じ (raw, stamp) のまま STALL_LIMIT 連敗 = 誰も動かしていないのに
                    //   検証が通らない (crash 残骸の odd gen / 恒久 stale / 破損) → None
                    // また yield だけでは位相が崩れないことがあるので、 µs sleep の階段
                    // backoff を足して共振を破る。
                    const STALL_LIMIT: usize = 256;
                    const SPIN_TRIES: usize = 16;
                    const YIELD_TRIES: usize = 64;
                    let mut stall = 0usize;
                    let mut last_probe: Option<(u32, u64)> = None;
                    let mut attempt = 0usize;
                    loop {
                        if attempt > 0 {
                            if attempt < SPIN_TRIES {
                                std::hint::spin_loop();
                            } else if attempt < YIELD_TRIES {
                                std::thread::yield_now();
                            } else {
                                let us = ((attempt - YIELD_TRIES + 1) as u64).min(100);
                                std::thread::sleep(std::time::Duration::from_micros(us));
                            }
                        }
                        attempt += 1;
                        let raw = self.himos[hid].get_value(eid_local)?;
                        // slot 内の seqlock (gen) で torn / 同 offset 再利用を検出。
                        let LeafRead::Ok(bytes) = leaf.try_read(raw) else {
                            let probe = (raw, leaf.slot_stamp(raw));
                            if last_probe == Some(probe) {
                                stall += 1;
                                if stall >= STALL_LIMIT {
                                    return None;
                                }
                            } else {
                                stall = 0;
                                last_probe = Some(probe);
                            }
                            continue;
                        };
                        // column offset を再読。 不変なら relocation も無かった = 確定。
                        if self.himos[hid].get_value(eid_local) == Some(raw) {
                            return Some(bytes);
                        }
                        // Ok だが column が動いた = writer 前進 (別 offset へ relocation)。
                        stall = 0;
                        last_probe = None;
                    }
                }
                // Tag / reserved Leaf: 不変な vocab bytes を copy。
                None => {
                    let raw = self.himos[hid].get_value(eid_local)?;
                    Some(self.vocab.get(raw).to_vec())
                }
            },
            _ => None,
        }
    }

    pub fn get(&self, eid: enchudb_oplog::EntityId, himo: &str) -> Option<u32> {
        let eid = enchudb_oplog::eid_local(eid);
        let hid = self.himo_id(himo)?;
        self.himos[hid].get_value(eid)
    }

    /// `get` の bindings 版。 schema 等で `himo_id` を起動時に pre-resolve した hot path 用。
    /// 名前 lookup (= himo_names の線形検索) が無くなるので point lookup が最速。
    pub fn get_by_id(&self, eid: enchudb_oplog::EntityId, hid: u16) -> Option<u32> {
        let eid = enchudb_oplog::eid_local(eid);
        self.himos.get(hid as usize)?.get_value(eid)
    }

    /// `get_by_id` の bulk 版。 同 himo の N entity を一括 column scan で `out` に append。
    /// stored 値 (= 内部表現の値+1、 0 = missing) を返す。 呼び出し側で
    /// `s.checked_sub(1)` すると `Option<u32>` に戻せる。
    ///
    /// alloc を呼ぶ側に任せて buffer reuse を可能に (= 集計ヘビーな loop で毎回 Vec を
    /// 作ると alloc が支配する)。 dominance loop は外で 4 buffer 確保 → 毎 lap で clear
    /// + 再 fill するだけ。
    #[inline]
    pub fn pull_himo_stored_many_into(
        &self, hid: u16,
        eids: &[enchudb_oplog::EntityId],
        out: &mut Vec<u32>,
    ) {
        let Some(hs) = self.himos.get(hid as usize) else {
            out.clear();
            out.resize(eids.len(), 0);
            return;
        };
        hs.get_stored_into(eids, out);
    }

    /// alloc 込みの便利版 (= 軽量 callsite 向け)。 hot loop では使わない。
    pub fn pull_himo_stored_many(&self, hid: u16, eids: &[enchudb_oplog::EntityId]) -> Vec<u32> {
        let mut out = Vec::new();
        self.pull_himo_stored_many_into(hid, eids, &mut out);
        out
    }

    /// 指定 himo に値が tie された **全** entity を列挙。 O(next_eid) で重い。
    /// schema layer の `Query::all()` のように「table の任意 column を持つ row」 を
    /// 列挙するための代表 column 経由で使う想定。
    pub fn entities_with_himo(&self, hid: u16) -> Vec<enchudb_oplog::EntityId> {
        let Some(hs) = self.himos.get(hid as usize) else { return Vec::new(); };
        let peer = self.peer_id();
        hs.entities_with_value()
            .into_iter()
            .map(|e| enchudb_oplog::make_eid(peer, e))
            .collect()
    }

    /// 指定 entity 群の紐値を合計
    pub fn sum(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> u64 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                total += v as u64;
            }
        }
        total
    }

    /// 0.8.6: himo 値が `[lo, hi]` (両端含む) の範囲に入る entity 全件を、
    /// column を直線 scan で集める。 既存 `entities_with_himo_range` 等は
    /// BucketCylinder の reverse lookup を range 内 N 値ぶん union するため、
    /// hit 率が高い (= range 内に大量 entity がいる) workload で遅い。 本 path は
    /// 「column を頭から舐めて compare」 だけなので duckdb の `BETWEEN` clause と
    /// 同じ性質、 hit 率に関わらず安定 (= 1M rows / M2 Max で ~1-3ms 目標)。
    ///
    /// 戻り値の eid は昇順、 重複なし、 peer prefix 付き (= make_eid 経由)。
    pub fn range_scan(&self, himo: &str, lo: u32, hi: u32) -> Vec<enchudb_oplog::EntityId> {
        if lo > hi { return Vec::new(); }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return Vec::new() };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let peer = self.peer_id();
        // stored 形式: 0 = missing、 stored - 1 = 値。 範囲 [lo, hi] in 値空間
        // ↔ [lo + 1, hi + 1] in stored 空間。 lo == 0 の場合の境界も自然に扱える。
        let lo_stored = lo.saturating_add(1);
        let hi_stored = hi.saturating_add(1);
        let mut hits: Vec<enchudb_oplog::EntityId> = Vec::new();
        // 22% hit 想定で reserve、 過剰なら trim
        hits.reserve(values.len() / 4);
        for (i, &stored) in values.iter().enumerate() {
            if stored >= lo_stored && stored <= hi_stored {
                hits.push(enchudb_oplog::make_eid(peer, i as u32));
            }
        }
        hits
    }

    /// 0.8.6: himo の `[lo, hi)` eid 範囲を column 直 scan で sum。
    /// schema 層の `Table::sum(col)` の internal primitive — table の eid
    /// 範囲 (= `eid_range_lo..eid_range_hi`) を渡すと、 その table の
    /// その column の合計が出る。
    ///
    /// 1M rows / M2 Max で ~100µs (= DuckDB `SELECT SUM(col) FROM tbl` の
    /// 5-6x 速い)。 eids 配列を経由しない、 mmap u32 slice を sequential に
    /// 舐めるだけの branchless tight loop が auto-vectorize する。
    pub fn sum_range(&self, himo: &str, lo: u32, hi: u32) -> u64 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        if lo >= hi { return 0; }
        let mut total: u64 = 0;
        // stored == 0 が missing、 stored > 0 のとき値は stored - 1。
        // saturating_sub(1) で missing を 0 扱いにできて branchless になる。
        for &stored in &values[lo..hi] {
            total += stored.saturating_sub(1) as u64;
        }
        total
    }

    /// 0.8.6: himo の `[lo, hi)` eid 範囲に値が tie されてる entity 数を返す
    /// (= `COUNT(col)` 相当、 missing を除いた数)。 schema 層の
    /// `Table::count(col)` の internal primitive。
    pub fn count_range(&self, himo: &str, lo: u32, hi: u32) -> u32 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        if lo >= hi { return 0; }
        let mut n: u32 = 0;
        for &stored in &values[lo..hi] {
            // stored > 0 のとき 1、 0 のとき 0 を加算 (= branchless cast)
            n += (stored != 0) as u32;
        }
        n
    }

    /// 0.8.8: himo の `[lo, hi)` eid 範囲の最小値を column 直 scan で求める。
    /// stored 形式: 0 = missing → skip、 stored > 0 のとき値 = stored - 1。
    /// 全 missing なら None。 schema 層の `Table::min(col)` の internal primitive。
    pub fn min_range(&self, himo: &str, lo: u32, hi: u32) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        if lo >= hi { return None; }
        // 最小 stored (1 以上) を探す。 0 (missing) は無視。
        let mut best: u32 = u32::MAX;
        let mut hit: bool = false;
        for &stored in &values[lo..hi] {
            if stored != 0 {
                hit = true;
                if stored < best { best = stored; }
            }
        }
        if hit { Some(best - 1) } else { None }
    }

    /// 0.8.8: himo の `[lo, hi)` eid 範囲の最大値を column 直 scan で求める。
    /// 全 missing なら None。 schema 層の `Table::max(col)` の internal primitive。
    pub fn max_range(&self, himo: &str, lo: u32, hi: u32) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        if lo >= hi { return None; }
        let mut best: u32 = 0;
        let mut hit: bool = false;
        for &stored in &values[lo..hi] {
            // stored > 0 のとき必ず best (== 0 含む) より大きくなりうる、 単純比較で OK。
            if stored > best { best = stored; hit = true; }
        }
        if hit { Some(best - 1) } else { None }
    }

    /// 0.8.8: `[lo, hi)` eid 範囲を 2 column lockstep scan して group_min。
    /// schema 層の `Table::group_min(group, val)` の internal primitive。
    /// `group_sum_range` と同じ dense / sparse 切替。
    pub fn group_min_range(
        &self,
        group_himo: &str,
        val_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(groups.len()).min(vals.len());
        if lo >= hi { return vec![]; }

        if let Some(cap) = self.group_dense_cap(gid) {
            // 値 0 は valid (stored=1 を引いた値) なので、 mins[g] != u32::MAX を
            // 「データ有り」の代用にする。 stored で持ったまま比較すれば mins init=u32::MAX
            // のときは必ず上書きされる (= seen tracking 不要)。
            let mut mins_stored: Vec<u32> = vec![u32::MAX; cap];
            for i in lo..hi {
                let g_stored = groups[i];
                let v_stored = vals[i];
                if g_stored == 0 || v_stored == 0 { continue; }
                let g = (g_stored - 1) as usize;
                if g < cap && v_stored < mins_stored[g] {
                    mins_stored[g] = v_stored;
                }
            }
            (0..cap).filter(|&i| mins_stored[i] != u32::MAX)
                .map(|i| (i as u32, mins_stored[i] - 1)).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for i in lo..hi {
                let g_stored = groups[i];
                let v_stored = vals[i];
                if g_stored == 0 || v_stored == 0 { continue; }
                let g = g_stored - 1;
                let entry = map.entry(g).or_insert(u32::MAX);
                if v_stored < *entry { *entry = v_stored; }
            }
            map.into_iter().map(|(g, v_stored)| (g, v_stored - 1)).collect()
        }
    }

    /// 0.8.8: `[lo, hi)` eid 範囲を 2 column lockstep scan して group_max。
    /// schema 層の `Table::group_max(group, val)` の internal primitive。
    pub fn group_max_range(
        &self,
        group_himo: &str,
        val_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(groups.len()).min(vals.len());
        if lo >= hi { return vec![]; }

        if let Some(cap) = self.group_dense_cap(gid) {
            // maxs_stored の init = 0 (== missing 扱い)。 stored > 0 を見たら必ず
            // 上書きされるので、 結果 filter で maxs_stored > 0 を有効データ判定。
            let mut maxs_stored: Vec<u32> = vec![0; cap];
            for i in lo..hi {
                let g_stored = groups[i];
                let v_stored = vals[i];
                if g_stored == 0 || v_stored == 0 { continue; }
                let g = (g_stored - 1) as usize;
                if g < cap && v_stored > maxs_stored[g] {
                    maxs_stored[g] = v_stored;
                }
            }
            (0..cap).filter(|&i| maxs_stored[i] != 0)
                .map(|i| (i as u32, maxs_stored[i] - 1)).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for i in lo..hi {
                let g_stored = groups[i];
                let v_stored = vals[i];
                if g_stored == 0 || v_stored == 0 { continue; }
                let g = g_stored - 1;
                let entry = map.entry(g).or_insert(0);
                if v_stored > *entry { *entry = v_stored; }
            }
            map.into_iter().map(|(g, v_stored)| (g, v_stored - 1)).collect()
        }
    }

    /// 0.8.8: `[lo, hi)` eid 範囲を column 直 scan して、 値域 `[vmin, vmax]` を
    /// `n_buckets` 等分した頻度ヒストグラムを返す。 値が `[vmin, vmax]` 外の
    /// entity はカウントされない (= clipping ではなく drop)。
    ///
    /// `n_buckets == 0` または `vmin > vmax` の場合は空 Vec。 戻り値長は
    /// `n_buckets` で固定 (= bucket 0 件でも 0 で埋める)。
    ///
    /// bucket index は `((val - vmin) * n_buckets) / (vmax - vmin + 1)`
    /// (= floor division で 0..n_buckets に均等割当)。
    pub fn histogram_range(
        &self,
        himo: &str,
        lo: u32,
        hi: u32,
        vmin: u32,
        vmax: u32,
        n_buckets: u32,
    ) -> Vec<u32> {
        if n_buckets == 0 || vmin > vmax { return vec![]; }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![0; n_buckets as usize] };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(values.len());
        let n = n_buckets as usize;
        let mut hist: Vec<u32> = vec![0; n];
        if lo >= hi { return hist; }
        // 値空間幅 (両端含む、 vmin == vmax のときは 1)
        let span = (vmax as u64 - vmin as u64) + 1;
        let n_u64 = n_buckets as u64;
        for &stored in &values[lo..hi] {
            if stored == 0 { continue; }
            let val = stored - 1;
            if val < vmin || val > vmax { continue; }
            // (val - vmin) * n / span。 span >= 1、 val - vmin < span を保証してる
            // ので idx < n は厳密に成り立つ (= bound check 不要だが safety で min)。
            let idx = (((val - vmin) as u64) * n_u64 / span) as usize;
            hist[idx.min(n - 1)] += 1;
        }
        hist
    }

    // ──── 0.8.9 (#39): bulk column scan の rayon 並列化 ────
    //
    // 既存 `_range` 系を **`_par` suffix で並列版** として複製。 callsite で
    // 「並列で OK (= 大規模 read-only scan)」 と分かってる場面に明示利用させる。
    // 閾値 `PAR_RANGE_THRESHOLD` (= 64k 要素) 以下では seq fallback、
    // thread overhead が利益を上回らないため。
    //
    // 並列化は `par_chunks(CHUNK_SIZE)` 経由で値 slice を区切り、 各 chunk で
    // 局所 accumulator を計算 → `reduce` で合算。 HimoStore は内部に
    // `RwLock<BucketCylinder>` を持つが、 ここで触る `stored_slice` は
    // immutable な mmap view (= read 中は lock 不要) なので thread-safe。

    /// 並列化閾値。 これ未満の `[lo, hi)` 幅では seq fallback (= thread spawn
    /// overhead が利益を上回らない、 実測ベースで 64k 程度が境界)。
    const PAR_RANGE_THRESHOLD: usize = 64_000;

    /// chunk 粒度。 16k 要素 = 64KB (u32 として)、 L2 cache friendly。
    const PAR_RANGE_CHUNK: usize = 16_384;

    /// 0.8.9: `sum_range` の並列版。 chunk ごとに local sum を計算 → reduce。
    /// 閾値以下は seq fallback。 12M row scan で seq 1.95s → par ~600ms 程度
    /// (= 4 thread / M2 Max 想定)。
    pub fn sum_range_par(&self, himo: &str, lo: u32, hi: u32) -> u64 {
        use rayon::prelude::*;
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        if lo_u >= hi_u { return 0; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.sum_range(himo, lo, hi);
        }
        values[lo_u..hi_u]
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut total: u64 = 0;
                for &stored in chunk {
                    total += stored.saturating_sub(1) as u64;
                }
                total
            })
            .sum()
    }

    /// 0.8.9: `count_range` の並列版。
    pub fn count_range_par(&self, himo: &str, lo: u32, hi: u32) -> u32 {
        use rayon::prelude::*;
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        if lo_u >= hi_u { return 0; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.count_range(himo, lo, hi);
        }
        values[lo_u..hi_u]
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut n: u32 = 0;
                for &stored in chunk {
                    n += (stored != 0) as u32;
                }
                n
            })
            .sum()
    }

    /// 0.8.9: `min_range` の並列版。 各 chunk で local min を取り、 全 chunk を
    /// reduce で min 結合。 全 missing なら None。
    pub fn min_range_par(&self, himo: &str, lo: u32, hi: u32) -> Option<u32> {
        use rayon::prelude::*;
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        if lo_u >= hi_u { return None; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.min_range(himo, lo, hi);
        }
        // chunk 内で None / Some(best_stored) を返し、 reduce で min 結合。
        let result = values[lo_u..hi_u]
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut best: u32 = u32::MAX;
                let mut hit = false;
                for &stored in chunk {
                    if stored != 0 {
                        hit = true;
                        if stored < best { best = stored; }
                    }
                }
                if hit { Some(best) } else { None }
            })
            .reduce(|| None, |a, b| match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(x.min(y)),
            });
        result.map(|stored| stored - 1)
    }

    /// 0.8.9: `max_range` の並列版。
    pub fn max_range_par(&self, himo: &str, lo: u32, hi: u32) -> Option<u32> {
        use rayon::prelude::*;
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        if lo_u >= hi_u { return None; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.max_range(himo, lo, hi);
        }
        let result = values[lo_u..hi_u]
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut best: u32 = 0;
                for &stored in chunk {
                    if stored > best { best = stored; }
                }
                if best > 0 { Some(best) } else { None }
            })
            .reduce(|| None, |a, b| match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(x.max(y)),
            });
        result.map(|stored| stored - 1)
    }

    /// 0.8.9: `group_sum_range` の並列版。 dense path のみ並列化、 sparse
    /// (= HashMap merge コスト高) は seq fallback。 chunk ごとに thread-local
    /// `Vec<u64>` を持って scatter add → reduce で要素ごと加算。
    pub fn group_sum_range_par(
        &self,
        group_himo: &str,
        sum_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u64)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let groups = gs.stored_slice();
        let sums = ss.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(groups.len()).min(sums.len());
        if lo_u >= hi_u { return vec![]; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.group_sum_range(group_himo, sum_himo, lo, hi);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            // sparse は HashMap merge が重いので seq に fallback
            return self.group_sum_range(group_himo, sum_himo, lo, hi);
        };
        // chunk 範囲は groups / sums の lo..hi を index 同期で zip するため、
        // index 列を slice しないと indexes が ずれる。 zip 後の slice を chunk する。
        // ただし par_chunks_exact は zip slice を受け付けないので、 chunked range
        // を loop してから内部で indexing する形に。
        let chunk = Self::PAR_RANGE_CHUNK;
        let n = hi_u - lo_u;
        let n_chunks = (n + chunk - 1) / chunk;
        let result: Vec<u64> = (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = lo_u + ci * chunk;
                let e = (s + chunk).min(hi_u);
                let mut acc: Vec<u64> = vec![0; cap];
                for i in s..e {
                    let g_stored = groups[i];
                    let v_stored = sums[i];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap {
                        acc[g] += (v_stored - 1) as u64;
                    }
                }
                acc
            })
            .reduce(|| vec![0u64; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() { a[i] += v; }
                a
            });
        (0..cap).filter(|&i| result[i] > 0).map(|i| (i as u32, result[i])).collect()
    }

    /// 0.8.9: `group_min_range` の並列版。 dense path のみ。
    pub fn group_min_range_par(
        &self,
        group_himo: &str,
        val_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u32)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(groups.len()).min(vals.len());
        if lo_u >= hi_u { return vec![]; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.group_min_range(group_himo, val_himo, lo, hi);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_min_range(group_himo, val_himo, lo, hi);
        };
        let chunk = Self::PAR_RANGE_CHUNK;
        let n = hi_u - lo_u;
        let n_chunks = (n + chunk - 1) / chunk;
        let result: Vec<u32> = (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = lo_u + ci * chunk;
                let e = (s + chunk).min(hi_u);
                let mut mins: Vec<u32> = vec![u32::MAX; cap];
                for i in s..e {
                    let g_stored = groups[i];
                    let v_stored = vals[i];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap && v_stored < mins[g] {
                        mins[g] = v_stored;
                    }
                }
                mins
            })
            .reduce(|| vec![u32::MAX; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() {
                    if v < a[i] { a[i] = v; }
                }
                a
            });
        (0..cap).filter(|&i| result[i] != u32::MAX)
            .map(|i| (i as u32, result[i] - 1)).collect()
    }

    /// 0.8.9: `group_max_range` の並列版。 dense path のみ。
    pub fn group_max_range_par(
        &self,
        group_himo: &str,
        val_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u32)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(groups.len()).min(vals.len());
        if lo_u >= hi_u { return vec![]; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.group_max_range(group_himo, val_himo, lo, hi);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_max_range(group_himo, val_himo, lo, hi);
        };
        let chunk = Self::PAR_RANGE_CHUNK;
        let n = hi_u - lo_u;
        let n_chunks = (n + chunk - 1) / chunk;
        let result: Vec<u32> = (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = lo_u + ci * chunk;
                let e = (s + chunk).min(hi_u);
                let mut maxs: Vec<u32> = vec![0; cap];
                for i in s..e {
                    let g_stored = groups[i];
                    let v_stored = vals[i];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap && v_stored > maxs[g] {
                        maxs[g] = v_stored;
                    }
                }
                maxs
            })
            .reduce(|| vec![0u32; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() {
                    if v > a[i] { a[i] = v; }
                }
                a
            });
        (0..cap).filter(|&i| result[i] != 0)
            .map(|i| (i as u32, result[i] - 1)).collect()
    }

    /// 0.8.9: `range_scan` の並列版。 chunk ごとに local hits → flat_map で連結。
    /// 結果順序は eid 昇順 (= chunk が lo→hi の順で並列実行されて、 各 chunk 内も
    /// 昇順なので、 flat 連結後も全体として昇順を保つ)。
    pub fn range_scan_par(&self, himo: &str, lo: u32, hi: u32) -> Vec<enchudb_oplog::EntityId> {
        use rayon::prelude::*;
        if lo > hi { return Vec::new(); }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return Vec::new() };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        if values.len() < Self::PAR_RANGE_THRESHOLD {
            return self.range_scan(himo, lo, hi);
        }
        let peer = self.peer_id();
        let lo_stored = lo.saturating_add(1);
        let hi_stored = hi.saturating_add(1);
        let chunk = Self::PAR_RANGE_CHUNK;
        let n_chunks = (values.len() + chunk - 1) / chunk;
        // chunk 単位で local Vec を作って flat_map で連結。 chunk 番号順に
        // 結果が並ぶので、 各 chunk 内が昇順なら全体も昇順。
        (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = ci * chunk;
                let e = (s + chunk).min(values.len());
                let mut local: Vec<enchudb_oplog::EntityId> = Vec::new();
                for i in s..e {
                    let stored = values[i];
                    if stored >= lo_stored && stored <= hi_stored {
                        local.push(enchudb_oplog::make_eid(peer, i as u32));
                    }
                }
                local
            })
            .reduce(Vec::new, |mut a, b| { a.extend(b); a })
    }

    /// 0.8.9: `histogram_range` の並列版。 chunk ごとに thread-local
    /// `Vec<u32>` で scatter add → reduce で要素加算。
    pub fn histogram_range_par(
        &self,
        himo: &str,
        lo: u32,
        hi: u32,
        vmin: u32,
        vmax: u32,
        n_buckets: u32,
    ) -> Vec<u32> {
        use rayon::prelude::*;
        if n_buckets == 0 || vmin > vmax { return vec![]; }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![0; n_buckets as usize] };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let lo_u = lo as usize;
        let hi_u = (hi as usize).min(values.len());
        let n = n_buckets as usize;
        if lo_u >= hi_u { return vec![0; n]; }
        if hi_u - lo_u < Self::PAR_RANGE_THRESHOLD {
            return self.histogram_range(himo, lo, hi, vmin, vmax, n_buckets);
        }
        let span = (vmax as u64 - vmin as u64) + 1;
        let n_u64 = n_buckets as u64;
        let chunk = Self::PAR_RANGE_CHUNK;
        let n_chunks = (hi_u - lo_u + chunk - 1) / chunk;
        (0..n_chunks)
            .into_par_iter()
            .map(|ci| {
                let s = lo_u + ci * chunk;
                let e = (s + chunk).min(hi_u);
                let mut hist: Vec<u32> = vec![0; n];
                for i in s..e {
                    let stored = values[i];
                    if stored == 0 { continue; }
                    let val = stored - 1;
                    if val < vmin || val > vmax { continue; }
                    let idx = (((val - vmin) as u64) * n_u64 / span) as usize;
                    hist[idx.min(n - 1)] += 1;
                }
                hist
            })
            .reduce(|| vec![0u32; n], |mut a, b| {
                for (i, &v) in b.iter().enumerate() { a[i] += v; }
                a
            })
    }

    /// 最小値
    pub fn min(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                result = Some(result.map_or(v, |cur: u32| cur.min(v)));
            }
        }
        result
    }

    /// 最大値
    pub fn max(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut result: Option<u32> = None;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                result = Some(result.map_or(v, |cur: u32| cur.max(v)));
            }
        }
        result
    }

    /// 平均（整数除算）
    pub fn avg(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u64> {
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        let mut total: u64 = 0;
        let mut count: u64 = 0;
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                total += v as u64;
                count += 1;
            }
        }
        if count == 0 { None } else { Some(total / count) }
    }

    /// 値を持つ entity の数
    pub fn count(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> u32 {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        let mut n: u32 = 0;
        for &eid in eids {
            if hs.get_value(enchudb_oplog::eid_local(eid)).is_some() { n += 1; }
        }
        n
    }

    /// GROUP BY + SUM — group_himo の値でグループ化し、sum_himo の値を合計
    ///
    /// group 値の cardinality を見て、 dense (= max_values 小) なら Vec 直 index、
    /// sparse なら HashMap で集計。
    pub fn group_sum(&self, group_himo: &str, sum_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let cap = self.group_dense_cap(gid);
        if let Some(cap) = cap {
            let mut sums: Vec<u64> = vec![0; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), ss.get_value(local)) {
                    let i = group as usize;
                    sums[i] += val as u64;
                    seen[i] = true;
                }
            }
            (0..cap).filter(|&i| seen[i]).map(|i| (i as u32, sums[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), ss.get_value(local)) {
                    *map.entry(group).or_insert(0) += val as u64;
                }
            }
            map.into_iter().collect()
        }
    }

    /// 0.8.6: `[lo, hi)` eid 範囲の 2 column を lockstep scan して group_sum。
    /// schema 層の `Table::group_sum(group, sum)` の internal primitive。
    ///
    /// 1M rows / M2 Max で ~10ms (= 残念ながら DuckDB ~1.5ms には及ばない、
    /// NEON で native scatter が無く、 acc[g] += v が ILP/vector 化困難な
    /// scatter write のため。 algorithmic 工夫は別 work)。
    pub fn group_sum_range(
        &self,
        group_himo: &str,
        sum_himo: &str,
        lo: u32,
        hi: u32,
    ) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let groups = gs.stored_slice();
        let sums = ss.stored_slice();
        let lo = lo as usize;
        let hi = (hi as usize).min(groups.len()).min(sums.len());
        if lo >= hi { return vec![]; }

        if let Some(cap) = self.group_dense_cap(gid) {
            // dense cap での group_sum: seen[] tracking を廃止して
            // hot loop の per-iter store を 2 → 1 に圧縮。
            // 「acc[g] == 0」 が non-empty 判定の代用、 値 0 と「データ無し」を
            // 区別したい時は別 API を使うこと。
            let mut acc: Vec<u64> = vec![0; cap];
            for i in lo..hi {
                let g_stored = groups[i];
                let s_stored = sums[i];
                if g_stored == 0 || s_stored == 0 { continue; }
                let g = (g_stored - 1) as usize;
                if g < cap {
                    acc[g] += (s_stored - 1) as u64;
                }
            }
            (0..cap).filter(|&i| acc[i] > 0).map(|i| (i as u32, acc[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u64> = std::collections::HashMap::new();
            for i in lo..hi {
                let g_stored = groups[i];
                let s_stored = sums[i];
                if g_stored == 0 || s_stored == 0 { continue; }
                *map.entry(g_stored - 1).or_insert(0) += (s_stored - 1) as u64;
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + COUNT — group_himo の値でグループ化し、各グループの entity 数
    pub fn group_count(&self, group_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut counts: Vec<u32> = vec![0; cap];
            for &eid in eids {
                if let Some(group) = gs.get_value(enchudb_oplog::eid_local(eid)) {
                    counts[group as usize] += 1;
                }
            }
            (0..cap).filter(|&i| counts[i] > 0).map(|i| (i as u32, counts[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for &eid in eids {
                if let Some(group) = gs.get_value(enchudb_oplog::eid_local(eid)) {
                    *map.entry(group).or_insert(0) += 1;
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + MIN
    pub fn group_min(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut mins: Vec<u32> = vec![u32::MAX; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let i = group as usize;
                    if val < mins[i] { mins[i] = val; }
                    seen[i] = true;
                }
            }
            (0..cap).filter(|&i| seen[i]).map(|i| (i as u32, mins[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let entry = map.entry(group).or_insert(u32::MAX);
                    if val < *entry { *entry = val; }
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + MAX
    pub fn group_max(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u32)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut maxs: Vec<u32> = vec![0; cap];
            let mut seen: Vec<bool> = vec![false; cap];
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let i = group as usize;
                    if val > maxs[i] { maxs[i] = val; }
                    seen[i] = true;
                }
            }
            (0..cap).filter(|&i| seen[i]).map(|i| (i as u32, maxs[i])).collect()
        } else {
            let mut map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let entry = map.entry(group).or_insert(0);
                    if val > *entry { *entry = val; }
                }
            }
            map.into_iter().collect()
        }
    }

    /// GROUP BY + AVG (整数除算)
    pub fn group_avg(&self, group_himo: &str, val_himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<(u32, u64)> {
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        if let Some(cap) = self.group_dense_cap(gid) {
            let mut sums: Vec<u64> = vec![0; cap];
            let mut cnts: Vec<u64> = vec![0; cap];
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let i = group as usize;
                    sums[i] += val as u64;
                    cnts[i] += 1;
                }
            }
            (0..cap).filter(|&i| cnts[i] > 0).map(|i| (i as u32, sums[i] / cnts[i])).collect()
        } else {
            let mut acc: std::collections::HashMap<u32, (u64, u64)> = std::collections::HashMap::new();
            for &eid in eids {
                let local = enchudb_oplog::eid_local(eid);
                if let (Some(group), Some(val)) = (gs.get_value(local), vs.get_value(local)) {
                    let e = acc.entry(group).or_insert((0, 0));
                    e.0 += val as u64;
                    e.1 += 1;
                }
            }
            acc.into_iter().map(|(k, (s, n))| (k, s / n)).collect()
        }
    }

    /// group 系の dense path 適用判定。 himo の max_values が小さく定義されていれば
    /// その値範囲を Vec 直接 index で集計できる。 閾値は 64K (Vec 確保 256KB 以下) まで。
    fn group_dense_cap(&self, hid: usize) -> Option<usize> {
        let hs = &self.himos[hid];
        let cap = hs.max_values as usize;
        if cap == 0 || cap > 65_536 { None } else { Some(cap) }
    }

    /// 値集合の distinct — eids の中で himo に張られた値のユニーク集合
    pub fn distinct(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Vec<u32> {
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![] };
        let hs = &self.himos[hid];
        let mut result: Vec<u32> = Vec::new();
        for &eid in eids {
            if let Some(v) = hs.get_value(enchudb_oplog::eid_local(eid)) {
                if !result.contains(&v) { result.push(v); }
            }
        }
        result
    }

    // ──── 0.8.10 (#43): 不連続 eids 群への集計 (par 版 + histogram_eids) ────
    //
    // 既存 `sum(himo, eids)` / `count` / `min` / `max` / `group_*` は seq 版のみ
    // だった。 sub-set (= `Query::where_*` で絞った eid 群) は連続 range と違って
    // `_range_par` が呼べない (= 不連続)。 そこで eids 版の並列版を追加して、
    // schema の `Query` 終端 method (= 集計 chain) の中で大規模 sub-set 集計を
    // 走らせられるようにする。
    //
    // 並列化方針:
    //   - `eids.par_chunks(PAR_RANGE_CHUNK)` で並列、 各 chunk で `stored_slice`
    //     を indirect access (= `col[eid_local(e)]`) で scatter read
    //   - `_range_par` (= sequential SIMD) と違って cache-unfriendly だが
    //     thread 並列度で稼ぐ
    //   - `eids.len() < PAR_RANGE_THRESHOLD` では seq fallback
    //
    // `histogram_eids` は seq 版が無かったので 0.8.10 で新規追加。

    /// 0.8.10: `sum(himo, eids)` の並列版。 stored_slice の indirect access。
    pub fn sum_eids_par(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> u64 {
        use rayon::prelude::*;
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.sum(himo, eids);
        }
        let values = hs.stored_slice();
        eids.par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut total: u64 = 0;
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local < values.len() {
                        total += values[local].saturating_sub(1) as u64;
                    }
                }
                total
            })
            .sum()
    }

    /// 0.8.10: `count(himo, eids)` の並列版。
    pub fn count_eids_par(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> u32 {
        use rayon::prelude::*;
        let hid = match self.himo_id(himo) { Some(h) => h, None => return 0 };
        let hs = &self.himos[hid];
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.count(himo, eids);
        }
        let values = hs.stored_slice();
        eids.par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut n: u32 = 0;
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local < values.len() && values[local] != 0 {
                        n += 1;
                    }
                }
                n
            })
            .sum()
    }

    /// 0.8.10: `min(himo, eids)` の並列版。
    pub fn min_eids_par(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u32> {
        use rayon::prelude::*;
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.min(himo, eids);
        }
        let values = hs.stored_slice();
        let result = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut best: u32 = u32::MAX;
                let mut hit = false;
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local < values.len() {
                        let stored = values[local];
                        if stored != 0 {
                            hit = true;
                            if stored < best { best = stored; }
                        }
                    }
                }
                if hit { Some(best) } else { None }
            })
            .reduce(|| None, |a, b| match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(x.min(y)),
            });
        result.map(|stored| stored - 1)
    }

    /// 0.8.10: `max(himo, eids)` の並列版。
    pub fn max_eids_par(&self, himo: &str, eids: &[enchudb_oplog::EntityId]) -> Option<u32> {
        use rayon::prelude::*;
        let hid = self.himo_id(himo)?;
        let hs = &self.himos[hid];
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.max(himo, eids);
        }
        let values = hs.stored_slice();
        let result = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut best: u32 = 0;
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local < values.len() {
                        let stored = values[local];
                        if stored > best { best = stored; }
                    }
                }
                if best > 0 { Some(best) } else { None }
            })
            .reduce(|| None, |a, b| match (a, b) {
                (None, x) | (x, None) => x,
                (Some(x), Some(y)) => Some(x.max(y)),
            });
        result.map(|stored| stored - 1)
    }

    /// 0.8.10: `group_sum(group, sum, eids)` の並列版。 dense path のみ並列、
    /// sparse (= max_values 0 or > 64K) は seq fallback。
    pub fn group_sum_eids_par(
        &self,
        group_himo: &str,
        sum_himo: &str,
        eids: &[enchudb_oplog::EntityId],
    ) -> Vec<(u32, u64)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let sid = match self.himo_id(sum_himo) { Some(h) => h, None => return vec![] };
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.group_sum(group_himo, sum_himo, eids);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_sum(group_himo, sum_himo, eids);
        };
        let gs = &self.himos[gid];
        let ss = &self.himos[sid];
        let groups = gs.stored_slice();
        let sums = ss.stored_slice();
        let result: Vec<u64> = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut acc: Vec<u64> = vec![0; cap];
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local >= groups.len() || local >= sums.len() { continue; }
                    let g_stored = groups[local];
                    let s_stored = sums[local];
                    if g_stored == 0 || s_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap {
                        acc[g] += (s_stored - 1) as u64;
                    }
                }
                acc
            })
            .reduce(|| vec![0u64; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() { a[i] += v; }
                a
            });
        (0..cap).filter(|&i| result[i] > 0).map(|i| (i as u32, result[i])).collect()
    }

    /// 0.8.10: `group_min(group, val, eids)` の並列版。 dense path のみ。
    pub fn group_min_eids_par(
        &self,
        group_himo: &str,
        val_himo: &str,
        eids: &[enchudb_oplog::EntityId],
    ) -> Vec<(u32, u32)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.group_min(group_himo, val_himo, eids);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_min(group_himo, val_himo, eids);
        };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let result: Vec<u32> = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut mins: Vec<u32> = vec![u32::MAX; cap];
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local >= groups.len() || local >= vals.len() { continue; }
                    let g_stored = groups[local];
                    let v_stored = vals[local];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap && v_stored < mins[g] {
                        mins[g] = v_stored;
                    }
                }
                mins
            })
            .reduce(|| vec![u32::MAX; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() {
                    if v < a[i] { a[i] = v; }
                }
                a
            });
        (0..cap).filter(|&i| result[i] != u32::MAX)
            .map(|i| (i as u32, result[i] - 1)).collect()
    }

    /// 0.8.10: `group_max(group, val, eids)` の並列版。 dense path のみ。
    pub fn group_max_eids_par(
        &self,
        group_himo: &str,
        val_himo: &str,
        eids: &[enchudb_oplog::EntityId],
    ) -> Vec<(u32, u32)> {
        use rayon::prelude::*;
        let gid = match self.himo_id(group_himo) { Some(h) => h, None => return vec![] };
        let vid = match self.himo_id(val_himo) { Some(h) => h, None => return vec![] };
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.group_max(group_himo, val_himo, eids);
        }
        let Some(cap) = self.group_dense_cap(gid) else {
            return self.group_max(group_himo, val_himo, eids);
        };
        let gs = &self.himos[gid];
        let vs = &self.himos[vid];
        let groups = gs.stored_slice();
        let vals = vs.stored_slice();
        let result: Vec<u32> = eids
            .par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut maxs: Vec<u32> = vec![0; cap];
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local >= groups.len() || local >= vals.len() { continue; }
                    let g_stored = groups[local];
                    let v_stored = vals[local];
                    if g_stored == 0 || v_stored == 0 { continue; }
                    let g = (g_stored - 1) as usize;
                    if g < cap && v_stored > maxs[g] {
                        maxs[g] = v_stored;
                    }
                }
                maxs
            })
            .reduce(|| vec![0u32; cap], |mut a, b| {
                for (i, &v) in b.iter().enumerate() {
                    if v > a[i] { a[i] = v; }
                }
                a
            });
        (0..cap).filter(|&i| result[i] != 0)
            .map(|i| (i as u32, result[i] - 1)).collect()
    }

    /// 0.8.10: 不連続 eids への histogram (= range scan できない sub-set 向け、
    /// schema `Query::histogram` の bind 先)。 `histogram_range` と同じ semantics
    /// (= 値域外 drop、 戻り値長は常に `n_buckets`)。
    pub fn histogram_eids(
        &self,
        himo: &str,
        eids: &[enchudb_oplog::EntityId],
        vmin: u32,
        vmax: u32,
        n_buckets: u32,
    ) -> Vec<u32> {
        if n_buckets == 0 || vmin > vmax { return vec![]; }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![0; n_buckets as usize] };
        let hs = &self.himos[hid];
        let values = hs.stored_slice();
        let n = n_buckets as usize;
        let mut hist: Vec<u32> = vec![0; n];
        if eids.is_empty() { return hist; }
        let span = (vmax as u64 - vmin as u64) + 1;
        let n_u64 = n_buckets as u64;
        for &eid in eids {
            let local = enchudb_oplog::eid_local(eid) as usize;
            if local >= values.len() { continue; }
            let stored = values[local];
            if stored == 0 { continue; }
            let val = stored - 1;
            if val < vmin || val > vmax { continue; }
            let idx = (((val - vmin) as u64) * n_u64 / span) as usize;
            hist[idx.min(n - 1)] += 1;
        }
        hist
    }

    /// 0.8.10: `histogram_eids` の並列版。
    pub fn histogram_eids_par(
        &self,
        himo: &str,
        eids: &[enchudb_oplog::EntityId],
        vmin: u32,
        vmax: u32,
        n_buckets: u32,
    ) -> Vec<u32> {
        use rayon::prelude::*;
        if n_buckets == 0 || vmin > vmax { return vec![]; }
        let hid = match self.himo_id(himo) { Some(h) => h, None => return vec![0; n_buckets as usize] };
        let hs = &self.himos[hid];
        let n = n_buckets as usize;
        if eids.len() < Self::PAR_RANGE_THRESHOLD {
            return self.histogram_eids(himo, eids, vmin, vmax, n_buckets);
        }
        let values = hs.stored_slice();
        let span = (vmax as u64 - vmin as u64) + 1;
        let n_u64 = n_buckets as u64;
        eids.par_chunks(Self::PAR_RANGE_CHUNK)
            .map(|chunk| {
                let mut hist: Vec<u32> = vec![0; n];
                for &eid in chunk {
                    let local = enchudb_oplog::eid_local(eid) as usize;
                    if local >= values.len() { continue; }
                    let stored = values[local];
                    if stored == 0 { continue; }
                    let val = stored - 1;
                    if val < vmin || val > vmax { continue; }
                    let idx = (((val - vmin) as u64) * n_u64 / span) as usize;
                    hist[idx.min(n - 1)] += 1;
                }
                hist
            })
            .reduce(|| vec![0u32; n], |mut a, b| {
                for (i, &v) in b.iter().enumerate() { a[i] += v; }
                a
            })
    }

    // ──── 範囲クエリ ────

    /// 範囲内の全値に合致する entity を返す（min..=max）
    pub fn pull_range(&self, himo: &str, min: u32, max: u32) -> Vec<enchudb_oplog::EntityId> {
        let idx = match self.himo_id(himo) { Some(h) => h, None => return vec![] };
        let hs = &self.himos[idx];
        let mut result = Vec::new();
        for v in min..=max {
            for local in &hs.pull(v) {
                result.push(*local as enchudb_oplog::EntityId);
            }
        }
        result
    }

    // ──── 日付ヘルパー ────

    /// (year, month, day) → epoch 日数（2000-01-01 = 0）
    pub fn date_to_days(year: u32, month: u32, day: u32) -> u32 {
        let mut y = year as i64;
        let mut m = month as i64;
        if m <= 2 { y -= 1; m += 12; }
        let days = 365 * y + y / 4 - y / 100 + y / 400 + (153 * (m - 3) + 2) / 5 + day as i64 - 1;
        let epoch = {
            let ey: i64 = 2000; let em: i64 = 1;
            let ey2 = ey - 1; // Jan is <= 2, so y-1, m+12
            let em2 = 13i64;
            365 * ey2 + ey2 / 4 - ey2 / 100 + ey2 / 400 + (153 * (em2 - 3) + 2) / 5 + em as i64 - 1
        };
        (days - epoch) as u32
    }

    /// epoch 日数 → (year, month, day)
    pub fn days_to_date(days: u32) -> (u32, u32, u32) {
        // 2000-01-01 の Julian Day Number
        let jdn = days as i64 + 2451545;
        let a = jdn + 32044;
        let b = (4 * a + 3) / 146097;
        let c = a - (146097 * b) / 4;
        let d = (4 * c + 3) / 1461;
        let e = c - (1461 * d) / 4;
        let m = (5 * e + 2) / 153;
        let day = e - (153 * m + 2) / 5 + 1;
        let month = m + 3 - 12 * (m / 10);
        let year = 100 * b + d - 4800 + m / 10;
        (year as u32, month as u32, day as u32)
    }

    /// 日付を epoch 日数として tie
    pub fn tie_date(&mut self, eid: enchudb_oplog::EntityId, himo: &str, year: u32, month: u32, day: u32) {
        self.tie(eid, himo, Self::date_to_days(year, month, day));
    }

    /// epoch 日数から (year, month, day) を返す
    pub fn get_date(&self, eid: enchudb_oplog::EntityId, himo: &str) -> Option<(u32, u32, u32)> {
        self.get(eid, himo).map(Self::days_to_date)
    }

    /// 日付範囲で pull_range
    pub fn pull_date_range(&self, himo: &str, from: (u32, u32, u32), to: (u32, u32, u32)) -> Vec<enchudb_oplog::EntityId> {
        let min = Self::date_to_days(from.0, from.1, from.2);
        let max = Self::date_to_days(to.0, to.1, to.2);
        self.pull_range(himo, min, max)
    }

    pub fn vocab_id(&self, text: &str) -> Option<u32> { self.vocab.lookup(text.as_bytes()) }

    /// 0.7.0: text を vocab に inject して vocab_id を返す (idempotent)。
    /// 既存の `intern_table_name` 系で entity → tie_text → delete の dummy roundtrip
    /// をしていた path を、 vocab 直接 inject に置換するための公開 API。
    /// entity / table 経路を一切触らないので、 anonymous closed 後でも安全に呼べる。
    pub fn vocab_intern_text(&self, text: &str) -> u32 {
        self.vocab.get_or_insert(text.as_bytes())
    }

    /// vocab ID → bytes（vocabulary 経由で文字列復元用）
    pub fn vocab_text(&self, vid: u32) -> &[u8] { self.vocab.get(vid) }

    /// 紐の文脈で文字列のvocab IDを探す。その紐にぶら下がってる値だけ調べる。
    pub fn find_value(&self, himo: &str, text: &str) -> Option<u32> {
        let hid = self.himo_id(himo)?;
        let text_bytes = text.as_bytes();
        let vals = self.himos[hid].unique_values();
        for vid in vals {
            if self.vocab.get(vid) == text_bytes {
                return Some(vid);
            }
        }
        None
    }

    pub fn himos_of(&self, eid: enchudb_oplog::EntityId) -> Vec<&str> {
        let eid = enchudb_oplog::eid_local(eid);
        self.himos.iter().enumerate()
            .filter(|(_, ds)| ds.get_value(eid).is_some())
            .map(|(i, _)| self.himo_names[i].as_str())
            .collect()
    }
    /// 1 entity の全フィールドを一括取得。HashMap ルックアップ 0 回。
    pub fn get_entity(&self, eid: enchudb_oplog::EntityId) -> Vec<(&str, EntityValue<'_>)> {
        let eid = enchudb_oplog::eid_local(eid);
        let mut fields = Vec::with_capacity(self.himos.len());
        for (i, hs) in self.himos.iter().enumerate() {
            if let Some(raw) = hs.get_value(eid) {
                let val = match self.value_types[i] {
                    ValueType::Tag | ValueType::Leaf => EntityValue::Text(self.text_value(i, raw)),
                    _ => EntityValue::Num(raw),
                };
                fields.push((self.himo_names[i].as_str(), val));
            }
        }
        fields
    }

    #[allow(dead_code)]
    pub(crate) fn vocab(&self) -> &Vocabulary { &self.vocab }
    pub fn himo_names(&self) -> &[String] { self.himo_names.as_slice() }

    pub fn value_type(&self, himo: &str) -> Option<ValueType> {
        self.himo_id(himo).map(|idx| self.value_types[idx])
    }

    /// 指定紐の現在の unique 値数(非空バケット数)。O(1)。
    /// 紐が未定義なら None。
    ///
    /// BucketCylinder は tie/untie/remove 時に AtomicU32 を増減させる。
    /// `define_himo` の `max_values` はヒントに過ぎないので、ここで返るのは
    /// 実データ上の cardinality。
    pub fn himo_cardinality(&self, himo: &str) -> Option<u32> {
        let idx = self.himo_id(himo)?;
        Some(self.himos[idx].unique_count())
    }

    /// himo の Cylinder が確保している eid backing の総 bytes（メモリ観測用、#95）。
    /// append-only なので各 eid は 1 度だけ載る = `unique×平均 × pow2 slack`。
    /// double-buffer していれば `>= 2×(eid 数×4)` になるので、その検知にも使える。
    pub fn himo_cylinder_backing_bytes(&self, himo: &str) -> Option<usize> {
        let idx = self.himo_id(himo)?;
        Some(self.himos[idx].cyl_backing_bytes())
    }

    /// 指定 himo の全 bucket を Column 基準で即時 compaction する (request12 P2)。
    /// stale (churn 痕) を除去して backing を縮め、以降の read を verify-free の
    /// fast path に戻す。reader は停止しない (bucket ごとの epoch swap)。
    /// 通常は書き込み時の自動 trigger (stale 率 50%) に任せてよく、これは
    /// 明示的に掃除したい運用・テスト用。紐が未定義なら false。
    /// mutating API なので readonly / replica open では他の write 系と同様 panic。
    pub fn compact_himo(&self, himo: &str) -> bool {
        self.check_writable();
        match self.himo_id(himo) {
            Some(idx) => {
                self.himos[idx].compact_now();
                true
            }
            None => false,
        }
    }

    // ──── 紐を引く（Cylinder 経由）────

    /// Cylinder + Bitmap キャッシュを再構築。delta をクリア。
    pub fn rebuild(&self) {
        for ds in &self.himos { ds.rebuild_cylinder(); }
    }

    /// 引く。 EntityId(u64) の Vec を返す。
    pub fn pull_raw(&self, himo: &str, value: u32) -> Vec<enchudb_oplog::EntityId> {
        match self.himo_id(himo) {
            Some(idx) => self.himos[idx].pull(value).into_iter().map(|e| e as enchudb_oplog::EntityId).collect(),
            None => Vec::new(),
        }
    }

    /// 引く。 HimoStore::pull (RwLock + clone) 直。
    pub fn pull(&self, himo: &str, value: u32) -> Vec<u32> {
        match self.himo_id(himo) {
            Some(idx) => self.himos[idx].pull(value),
            None => Vec::new(),
        }
    }

    /// 複数値の bucket を union する。 `WHERE col IN (v1, v2, ...)` / Ravn follow 後の union 用。
    /// 結果は sort + dedup 済み。
    pub fn pull_in(&self, himo: &str, values: &[u32]) -> Vec<enchudb_oplog::EntityId> {
        match self.himo_id(himo) {
            Some(idx) => self.pull_in_by_idx(idx, values),
            None => Vec::new(),
        }
    }

    /// schema 層用: himo_id pre-resolve 済みの `pull_in`。
    pub fn pull_in_by_id(&self, himo_id: u16, values: &[u32]) -> Vec<enchudb_oplog::EntityId> {
        let idx = himo_id as usize;
        if idx >= self.himos.len() { return Vec::new(); }
        self.pull_in_by_idx(idx, values)
    }

    fn pull_in_by_idx(&self, idx: usize, values: &[u32]) -> Vec<enchudb_oplog::EntityId> {
        if values.is_empty() { return Vec::new(); }
        let mut out: Vec<u32> = Vec::new();
        for &v in values {
            out.extend(self.himos[idx].pull(v));
        }
        out.sort_unstable();
        out.dedup();
        // issue12: 旧 `e as EntityId` は #32 と同型 (peer prefix 抜け)。
        // query_by_id / entities_with_himo と同じ make_eid(peer, e) で揃える。
        let peer = self.peer_id();
        out.into_iter().map(|e| enchudb_oplog::make_eid(peer, e)).collect()
    }

    /// Cylinder 結果に delta を適用。
    #[allow(dead_code)]
    fn apply_delta(&self, himo_idx: usize, value: u32, cyl_result: &[u32], delta_eids: &[u32]) -> Vec<u32> {
        let hs = &self.himos[himo_idx];

        // delta の eid を集合にする（重複排除 + 高速lookup）
        let mut dirty: Vec<u32> = delta_eids.to_vec();
        dirty.sort_unstable();
        dirty.dedup();

        // Cylinder 結果から dirty eid を除外（Cylinder の値は古い可能性）
        let mut result: Vec<u32> = if dirty.is_empty() {
            cyl_result.to_vec()
        } else {
            cyl_result.iter()
                .filter(|&&eid| dirty.binary_search(&eid).is_err())
                .copied()
                .collect()
        };

        // dirty eid を Column 直読みで補正
        for &eid in &dirty {
            if hs.get_value(eid) == Some(value) {
                result.push(eid);
            }
        }

        result.sort_unstable();
        result.dedup();
        result
    }

    pub fn query(&self, strings: &[(&str, u32)]) -> Vec<enchudb_oplog::EntityId> {
        self.query_u32(strings).into_iter().map(|e| e as enchudb_oplog::EntityId).collect()
    }

    /// schema 層用: himo_id を pre-resolve 済みの場合の高速 path。 名前 lookup を完全に skip。
    /// 同一 entity の AND 条件として扱う。 himo_id が範囲外なら空 Vec。
    pub fn query_by_id(&self, conds: &[(u16, u32)]) -> Vec<enchudb_oplog::EntityId> {
        if conds.is_empty() { return Vec::new(); }
        let himo_count = self.himos.len();
        let mut idx_conds: Vec<(usize, u32)> = Vec::with_capacity(conds.len());
        for &(hid, val) in conds {
            let idx = hid as usize;
            if idx >= himo_count { return Vec::new(); }
            idx_conds.push((idx, val));
        }
        // 0.8.4 issue #32: 旧 `e as EntityId` は u32 → u64 widen で peer prefix が
        // 0 のまま残り、 schema 層の `where_eq().find_one()` 等が壊れた eid を返してた。
        // `entities_with_himo` と同じ make_eid(peer, e) で peer prefix を付ける。
        let peer = self.peer_id();
        self.query_resolved(&idx_conds)
            .into_iter()
            .map(|e| enchudb_oplog::make_eid(peer, e))
            .collect()
    }

    /// 内部版: u32 eid の Vec を返す。互換性のため残す。
    fn query_u32(&self, strings: &[(&str, u32)]) -> Vec<u32> {
        if strings.is_empty() { return vec![]; }
        // 全条件の himo index と value を解決
        let mut conds: Vec<(usize, u32)> = Vec::with_capacity(strings.len());
        for &(himo, val) in strings {
            match self.himo_id(himo) {
                Some(idx) => conds.push((idx, val)),
                None => return vec![],
            }
        }
        self.query_resolved(&conds)
    }

    /// resolved conds (himo_index, value) に対する query 本体。 strategy 自動選択。
    fn query_resolved(&self, conds: &[(usize, u32)]) -> Vec<u32> {
        // delta が溢れた himo があれば rebuild
        for hs in &self.himos {
            if hs.delta_needs_rebuild() { hs.rebuild_cylinder(); }
        }
        if conds.is_empty() { return vec![]; }

        if conds.len() == 1 {
            let (idx, val) = conds[0];
            return self.himos[idx].pull(val);
        }

        // Column直読みフィルタ。 `delta` は常に空なので Column が最新で OK。
        // 旧版は 「全条件 bitmap なら AND」 fast path を持っていたが、
        // `HimoStore::has_bitmaps()` が常に false で到達不能だったため issue #5
        // で撤去した。 0.5+ 系で bitmap を本気で持つなら、 ここで戦略選択を復活させる。
        self.query_column_filter(&conds)
    }

    /// Column直読みフィルタ（delta 補正付き）
    fn query_column_filter(&self, conds: &[(usize, u32)]) -> Vec<u32> {
        let total = self.entities.next_eid() as usize;
        // 各 cond の slice_len を事前計算 (per-eid 呼ばないように外出し)
        let slice_lens: Vec<usize> = conds.iter()
            .map(|&(idx, val)| self.himos[idx].slice_len(val))
            .collect();

        // pivot: 最小スライスを選ぶ
        let mut best = 0;
        let mut best_len = usize::MAX;
        for (i, &len) in slice_lens.iter().enumerate() {
            if len < best_len { best_len = len; best = i; }
        }

        let (pivot_idx, pivot_val) = conds[best];
        let hs = &self.himos[pivot_idx];

        let candidates = hs.pull(pivot_val);

        // 残りの条件を Column 直読みでフィルタ（Column は常に最新）。
        // 全件相当の cond (e.g. schema layer の table marker) は always-true として skip。
        let mut result = Vec::with_capacity(candidates.len());
        for &eid in &candidates {
            let mut pass = true;
            for (i, &(idx, val)) in conds.iter().enumerate() {
                if i == best { continue; }
                if slice_lens[i] >= total { continue; } // 全件 → 確実に true
                if !self.himos[idx].value_eq(eid, val) { pass = false; break; }
            }
            if pass { result.push(eid); }
        }
        result
    }

    #[allow(dead_code)]
    pub(crate) fn query_count(&self, strings: &[(&str, u32)]) -> usize {
        self.query(strings).len()
    }

    // ──── himo 管理 ────

    /// 紐名 → インデックス。線形探索（紐数は高々数百）。
    #[inline]
    pub fn himo_id(&self, himo: &str) -> Option<usize> {
        self.himo_names.iter().position(|n| n == himo)
    }

    /// 0.9.0: himo_to_table の bounds-checked read。 要素は AtomicU16 なので
    /// load して TableId に戻す (attach 変更は himo_def_lock 下の低頻度 path)。
    #[inline(always)]
    fn himo_table_get(&self, hid: usize) -> Option<TableId> {
        self.himo_to_table
            .get(hid)
            .map(|a| a.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn ensure_himo(&mut self, himo: &str, ht: ValueType, max_values: u32) -> usize {
        // 0.9.0: 本体は `&self` 版に移譲 (signature 互換の thin wrapper)。
        // capacity 超過は旧実装の assert! と同様 panic のまま。
        if let Some(idx) = self.himo_id(himo) { return idx; }
        let _guard = self.himo_def_lock.lock().unwrap();
        if let Some(idx) = self.himo_id(himo) { return idx; }
        match self.define_himo_slot_locked(himo, ht, max_values) {
            Ok(hid) => hid as usize,
            Err(e) => panic!("{}", e),
        }
    }

    /// 0.9.0: `ensure_himo` の `&self` 版。 `Arc<Engine>` (= concurrent mode)
    /// から lazy に himo を定義できる。 idempotent: 同名が既にあれば既存 hid を
    /// 返す。 新規定義は `himo_def_lock` で直列化され、 anonymous table に
    /// attach される (旧 `define_himo` と同じ)。 named table に attach するなら
    /// `ensure_himo_dynamic_in` を使う。
    pub fn ensure_himo_dynamic(
        &self,
        full_name: &str,
        ht: ValueType,
        max_values: u32,
    ) -> Result<u16, String> {
        self.check_writable();
        // fast path: 既存名は lock 無しで解決 (himo_names は append-only なので
        // 見つかった idx は安定)。
        if let Some(idx) = self.himo_id(full_name) {
            return Ok(idx as u16);
        }
        let _guard = self.himo_def_lock.lock().unwrap();
        // double-check: lock 待ちの間に他 thread が同名を定義した可能性
        if let Some(idx) = self.himo_id(full_name) {
            return Ok(idx as u16);
        }
        let hid = self.define_himo_slot_locked(full_name, ht, max_values)?;
        // himo → table attach (anonymous) を sidecar に反映 (best effort)。
        self.try_persist_tables();
        Ok(hid)
    }

    /// himo 定義の本体 (0.9.0 で ensure_himo から `&self` 化)。
    ///
    /// 呼び出し規約: **必ず `himo_def_lock` を保持して呼ぶこと** (直列化は
    /// caller 責務)。 lock 下で himoreg 登録 → column region init → 並列配列
    /// への push → header 書き込みまでを行う。 publish 順は `himo_names` を
    /// 最後にする: `himo_id()` (= 名前の線形検索) で hid が見つかった時点で
    /// 他の並列配列 (himos / value_types / himo_max_values / himo_to_table /
    /// tables[anon].himo_ids) は必ず埋まっている、 という不変条件を成す。
    fn define_himo_slot_locked(
        &self,
        himo: &str,
        ht: ValueType,
        max_values: u32,
    ) -> Result<u16, String> {
        let hid = self.himos.len();
        if hid as u32 >= self.max_himos || hid >= u16::MAX as usize {
            return Err(format!(
                "too many himos (max {}) — DB 全体の himo (table × column 通し) 上限に達した。 \
                 create 時に GrowableOptions {{ max_himos, .. }} で引き上げよ (既存 DB は rebuild が必要)",
                self.max_himos,
            ));
        }

        self.himo_reg.get_or_insert(himo.as_bytes());

        let effective_mv = max_values.min(self.layout.read().unwrap().cyl_max_values);
        // v10: himo 列 (と版数列) の segment file をここで作る。 crash で file だけ残った
        // 場合 (header の count 更新前) は SegmentSet 側が open で回収する。
        self.backing
            .ensure_himo(hid as u32, &*self.layout.read().unwrap())
            .map_err(|e| format!("cannot create himo segment {hid}: {e}"))?;
        if self.layout.read().unwrap().has_cell_version() {
            self.backing
                .ensure_ver(hid as u32, &*self.layout.read().unwrap())
                .map_err(|e| format!("cannot create version segment {hid}: {e}"))?;
        }

        let hs = HimoStore::init(
            self.backing.region(SegmentKind::Himo(hid as u32), &*self.layout.read().unwrap()),
            ht, effective_mv, self.max_entities(),
        );

        if self.layout.read().unwrap().has_cell_version() {
            let ver = ver_column_from_region(
                self.backing.region(SegmentKind::Ver(hid as u32), &*self.layout.read().unwrap()),
                self.max_entities(),
            );
            assert!(
                self.ver_cols.push(ver).is_ok(),
                "version column array out of capacity (max {})",
                self.max_himos,
            );
        }

        // AppendVec は with_capacity(max_himos) 済みなので上の capacity check が
        // 通れば push は失敗しない (万一の防御で明示 panic)。
        let push_ok = self.himos.push(hs).is_ok()
            && self.value_types.push(ht).is_ok()
            && self.himo_max_values.push(max_values).is_ok()
            && self
                .himo_to_table
                .push(std::sync::atomic::AtomicU16::new(ANONYMOUS_TABLE))
                .is_ok();
        assert!(push_ok, "himo parallel arrays out of capacity (max {})", self.max_himos);

        // β-light step 2: 旧 API (define_himo) で追加された himo は anonymous
        // table に attach する。 named table への attach は
        // ensure_himo_dynamic_in / define_himo_in 側で migrate される。
        let hid_u32 = hid as u32;
        self.tables[ANONYMOUS_TABLE as usize]
            .himo_ids
            .write()
            .unwrap()
            .push(hid_u32);

        // ヘッダにメタデータ書き込み (himo_def_lock 下、 reader は runtime に
        // この領域を読まないので &self からの直接書き込みで安全)
        let maxv_base = himo_maxv_base(self.max_himos);
        let buf = self.backing.header_mut(self.layout.read().unwrap().header_size);
        buf[H_HIMO_TYPES + hid] = ht as u8;
        let mv_off = maxv_base + hid * 4;
        buf[mv_off..mv_off + 4].copy_from_slice(&max_values.to_le_bytes());
        let himo_count = (hid + 1) as u32;
        buf[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&himo_count.to_le_bytes());
        // header CRC を再計算(himo_count が変わったため)
        write_header_crc(buf);

        // issue7 fix: schema 変更で region layout が変わったので、 seal_integrity
        // で焼かれた古い `.crc` sidecar は stale。 削除して、 次 open は CRC 検証
        // skip (`.crc` 無し DB と同じ fallback)、 次 seal_integrity で regenerate させる。
        #[cfg(not(target_arch = "wasm32"))]
        {
            let crc_path = crate::integrity::crc_path_for(&self.path);
            let _ = std::fs::remove_dir_all(&crc_path); // v10: DB は directory
            let _ = std::fs::remove_file(&crc_path);
        }

        // 最後に名前を publish (この時点で hid の全 metadata が可視)
        let _ = self.himo_names.push(himo.to_string());

        Ok(hid as u16)
    }

    // ──── 並行書き込み ────

    /// 並行対応版の create。
    ///
    /// writer は `tie_async` で WriteQueue に push(~ns オーダー)、
    /// 裏で単一 consumer スレッドが pop して HimoStore に適用する。
    /// reader は Arc<Engine> を複数スレッドで共有し、`pull_raw`/`query` で
    /// RwLock 越しに安全に読める。
    ///
    /// 戻り値は `Arc<Engine>`。define_himo など `&mut self` メソッドは
    /// `Arc::get_mut` 経由(定義の前に呼ぶ前提)で行う、もしくは本関数の前に
    /// `create_with_capacity` で定義済み状態を作ってから `concurrentize` する
    /// パターンを使う。
    ///
    /// WriteQueue は `SegQueue`(unbounded)。cap 指定は不要。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_concurrent(path: &str) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        Ok(Self::spawn_consumer(eng))
    }

    /// WAL 付き create_concurrent。
    /// `{path}.wal` に Write-Ahead Log を作成。
    ///
    /// tie_async / untie_async / delete_async は hot path で WAL append(memcpy)を行い、
    /// consumer スレッドが背景で 100ms 毎に fsync + checkpoint。
    ///
    /// プロセス/OS クラッシュ時は open_concurrent_with_oplog で最後の Commit まで復元される。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_concurrent_with_oplog(path: &str, oplog_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        let oplog_path = oplog_path_for(path);
        let wal = std::sync::Arc::new(enchudb_oplog::oplog::OpLog::create(&oplog_path, oplog_capacity)?);
        eng.rehydrate_next_sync_lsn(); // #77-H6: recovery 後の rows も含めて復元
        Ok(Self::spawn_consumer_with_oplog(eng, Some(wal)))
    }

    /// `create_concurrent_with_oplog` + `queue_capacity` override (issue4)。
    /// - `queue_capacity`: WriteQueue / oplog_record_queue の bounded cap (default 1 M)
    ///
    /// sustained writer (sunsu Docker scenario 03 等) で writer >> consumer rate に
    /// なると、 旧 unbounded queue では RSS 線形成長 → OOM。 bounded 化 + producer
    /// block でこれを cap する。 capacity の選び方:
    /// - 小さい (例: 10 K) → RSS 低い、 latency 不安定
    /// - 大きい (例: 10 M) → latency 安定、 RSS は queue 内 record サイズ × cap
    #[cfg(not(target_arch = "wasm32"))]
    pub fn create_concurrent_with_oplog_queue_cap(
        path: &str,
        oplog_capacity: usize,
        queue_capacity: usize,
    ) -> io::Result<std::sync::Arc<Self>> {
        let eng = Self::create_with_capacity(path, DEFAULT_MAX_ENTITIES)?;
        let oplog_path = oplog_path_for(path);
        let wal = std::sync::Arc::new(enchudb_oplog::oplog::OpLog::create(&oplog_path, oplog_capacity)?);
        Ok(Self::spawn_consumer_with_oplog_queue_cap(eng, Some(wal), Some(queue_capacity)))
    }

    /// WAL 付き open_concurrent。既存 WAL があればリカバリする。
    /// region CRC は WAL ルートでは skip(WAL が source of truth)。
    /// 代わりに古い `.crc` ファイルは削除して、次回 flush で regenerate させる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_concurrent_with_oplog(path: &str, oplog_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        Self::open_concurrent_with_oplog_queue_opt(path, oplog_capacity, None)
    }

    /// #116: `open_concurrent_with_oplog` + queue capacity 上書き。多 DB を LRU pool で
    /// open/close する hosted 構成の open 側 knob (説明は `concurrentize_with_oplog_queue`)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_concurrent_with_oplog_queue(
        path: &str,
        oplog_capacity: usize,
        queue_capacity: usize,
    ) -> io::Result<std::sync::Arc<Self>> {
        Self::open_concurrent_with_oplog_queue_opt(path, oplog_capacity, Some(queue_capacity))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open_concurrent_with_oplog_queue_opt(
        path: &str,
        oplog_capacity: usize,
        queue_capacity: Option<usize>,
    ) -> io::Result<std::sync::Arc<Self>> {
        let mut eng = Self::open_internal(path, /*verify_region_crc=*/ false, /*take_lock=*/ true, /*readonly=*/ false)?;
        // 古い .crc は WAL 活動後に stale になるので削除
        let crc_path = crate::integrity::crc_path_for(path);
        let _ = std::fs::remove_dir_all(&crc_path); // v10: DB は directory
        let _ = std::fs::remove_file(&crc_path);
        let oplog_path = oplog_path_for(path);
        let wal = if oplog_path.exists() {
            let w = enchudb_oplog::oplog::OpLog::open(&oplog_path)?;
            // リカバリ: commit されたレコードを本体に適用
            // #77-H2 の順序に加えて: **未 commit tail も replay する**。
            // concurrent 経路は WAL append → body 適用 の順に流すので、 crash が
            // その間に入ると 「WAL には在るが body には無い record」 が末尾に残る。
            // ここで適用せずに checkpoint で越えると恒久消失する
            // (`OpLog::recover_with_tail` の doc 参照)。
            let records = w.recover_with_tail();
            for rec in &records {
                eng.apply_oplog_op(&rec.op, rec.hlc, rec.author_peer);
            }
            // #77-H2: 適用効果を disk に固めてから checkpoint を前進する。
            // 旧順序 (apply → 即 checkpoint) は kernel が checkpoint header を
            // body より先に writeback すると、 recovery 直後の再 crash で
            // 「一度 durable だった committed record」 が replay 対象外になり
            // 恒久消失した。
            let _ = eng.body_msync();
            let _ = w.fsync();
            w.advance_checkpoint(w.head());
            std::sync::Arc::new(w)
        } else {
            std::sync::Arc::new(enchudb_oplog::oplog::OpLog::create(&oplog_path, oplog_capacity)?)
        };
        eng.rehydrate_next_sync_lsn(); // #77-H6: recovery 後の rows も含めて復元
        Ok(Self::spawn_consumer_with_oplog_queue_cap(eng, Some(wal), queue_capacity))
    }

    /// WAL の 1 op を本体に適用(recover 専用)。
    /// eid は u64 だが Column は local u32 で保持。eid_local() で剥がす。
    ///
    /// 0.8.1: Tie/Content で `entities.ensure_live` + table `next_local` の
    /// max 推進を入れた。 これが無いと short-lived CLI (= sidecar persist
    /// 機会無く drop → 次 open で oplog recover) のとき:
    ///   - entity_set の live bitmap が stale → 次 entity_in が eid 重複払出し
    ///   - table.next_local が 0 のまま → 次 alloc が既存 eid と衝突
    /// になる。 sinfo 連携で表面化したので 0.8.1 patch で根治。
    fn apply_oplog_op(
        &mut self,
        op: &enchudb_oplog::oplog::DecodedOp,
        hlc: enchudb_oplog::Hlc,
        author: enchudb_oplog::PeerId,
    ) {
        use enchudb_oplog::oplog::DecodedOp;
        // #209: relay (gossip) が verbatim で積んだ foreign-author record は、
        // 受信時 (Syncer::apply_one) と同じ翻訳経路で replay する。 eid をそのまま
        // local slot に書くと relay の body が壊れる (原 eid は author の番号)。
        // self peer は header (H_PEER_ID) から復元済み。
        let self_peer = self.peer_id();
        if self_peer != 0 && author != 0 && author != self_peer {
            self.replay_relayed_op(op, hlc, author);
            return;
        }
        match op {
            DecodedOp::Tie { eid, himo_id, value } => {
                let hid = *himo_id as usize;
                let local = enchudb_oplog::eid_local(*eid);
                if hid < self.himos.len() {
                    // request17 step 4: replay も版数付きで書く。 不採用 (= cell に
                    // 既に同じか新しい版数が載っている = body に msync 済み) なら
                    // 値を戻さない — 戻すと新しい write を古い record で潰す。
                    self.set_cell_local(local, *himo_id, *value, hlc);
                }
                // eid 空間の整合 (live bitmap / next_local) は適用可否に依らず進める。
                self.entities.ensure_live(local);
                Self::advance_table_next_local_for(&self.tables, local);
            }
            DecodedOp::Untie { eid, himo_id } => {
                let hid = *himo_id as usize;
                let local = enchudb_oplog::eid_local(*eid);
                if hid < self.himos.len() {
                    self.clear_cell_local(local, *himo_id, hlc);
                }
            }
            DecodedOp::Delete { eid } => {
                // replay は冪等でなければならない — 同じ record を二度食っても
                // 本体が落ちる (`apply_delete_local`)。 旧実装は同値 HLC を
                // `set_tombstone_local` で弾いて本体を残していた。
                let local = enchudb_oplog::eid_local(*eid);
                self.apply_delete_local(local, hlc);
            }
            DecodedOp::Content { eid, key, data } => {
                // legacy (pre-0.9 WAL): 旧 content region へ replay。 0.9.0 以降は
                // Op::Content を emit しないので、 旧 DB の WAL 再生でのみ通る。
                let local = enchudb_oplog::eid_local(*eid);
                if !self.contents.set(local, key, data) {
                    // #59: content data 領域が満杯。 panic せず拒否 + 計上。
                    self.record_fault(
                        FaultKind::ContentSpace,
                        "content data region is full — content write rejected",
                    );
                    return;
                }
                self.entities.ensure_live(local);
                Self::advance_table_next_local_for(&self.tables, local);
            }
            DecodedOp::TieNamed { eid, himo_name, himo_kind, value } => {
                // 0.9.0: 名前で himo を解決 (無ければ定義) して set。
                let local = enchudb_oplog::eid_local(*eid);
                let ht = ValueType::from_byte(*himo_kind);
                if let Ok(hid) = self.ensure_himo_by_full_name(himo_name, ht) {
                    self.set_cell_local(local, hid, *value, hlc);
                }
                self.entities.ensure_live(local);
                Self::advance_table_next_local_for(&self.tables, local);
            }
            DecodedOp::TieLeaf { .. } => {
                // 0.12.0 (#88): self-authored TieLeaf の recover は no-op。
                // Leaf payload は LeafStore、 cell offset は himo 列、 どちらも mmap
                // body として durable なので「既に local に在る」(Vocab と同思想)。
                // 再 insert すると offset が変わり slot が二重化するため触らない。
                // remote peer からの TieLeaf は sync crate の apply-one 経由で別 apply。
            }
            DecodedOp::TieRef { .. } => {
                // #183: TieRef は bridge が `_sync_ops` 発送時に合成する op で、
                // ローカル oplog には現れない (author の oplog は Op::Tie のまま)。
                // 万一混入しても local state は Tie で既に durable なので no-op。
            }
            DecodedOp::Commit => {}
            DecodedOp::Vocab { .. } => {
                // 自プロセスの recover 時は Vocab 個別の apply 不要
                // (author_peer == self の場合は既に local vocab にある)。
                // Sync 経由で他 peer から受信する場合のみ apply_one 側で処理。
            }
        }
    }

    /// #209: relay (gossip) が verbatim で WAL に積んだ foreign-author record の
    /// recovery replay。 Syncer::apply_one の翻訳規則をなぞる (recovery 時に
    /// Syncer は存在しない)。 翻訳写像は live 適用の barrier で .eidmap/.vocabmap
    /// に永続しているのが通常で、 crash 窓で欠けていても `get_or_insert` 系が
    /// 受信時と同じ規則で貼り直す。 apply は LWW 冪等。 解決できない record は
    /// skip — relay の役目上、 元 record は author に在り再 pull で埋め直せる。
    fn replay_relayed_op(
        &self,
        op: &enchudb_oplog::oplog::DecodedOp,
        hlc: enchudb_oplog::Hlc,
        author: enchudb_oplog::PeerId,
    ) {
        use enchudb_oplog::oplog::DecodedOp;
        match op {
            DecodedOp::Vocab { vid, bytes } => {
                if !self.has_remote_vocab(author, *vid, bytes) {
                    self.remote_vocab_apply(author, *vid, bytes);
                }
            }
            DecodedOp::Tie { eid, himo_id, value } => {
                let Some(le) = self.resolve_remote_eid(*eid, *himo_id) else { return };
                let v = if self.himo_is_ref(*himo_id) {
                    match self.resolve_remote_ref_value(author, *value, *himo_id) {
                        Some(v) => v,
                        None => return,
                    }
                } else {
                    match self.try_translate_remote_vid(author, *himo_id, *value) {
                        Some(v) => v,
                        None => return,
                    }
                };
                self.remote_tie_apply(le, *himo_id, v, hlc);
            }
            DecodedOp::TieRef { eid, himo_id, target } => {
                let Some(le) = self.resolve_remote_eid(*eid, *himo_id) else { return };
                let v = match self.resolve_remote_ref_value(
                    enchudb_oplog::eid_peer(*target),
                    enchudb_oplog::eid_local(*target),
                    *himo_id,
                ) {
                    Some(v) => v,
                    None => return,
                };
                self.remote_tie_apply(le, *himo_id, v, hlc);
            }
            DecodedOp::TieNamed { eid, himo_name, himo_kind, value } => {
                let Ok(hid) = self.ensure_himo_named(himo_name, *himo_kind) else { return };
                let Some(le) = self.resolve_remote_eid(*eid, hid) else { return };
                let Some(v) = self.try_translate_remote_vid(author, hid, *value) else { return };
                self.remote_tie_apply(le, hid, v, hlc);
            }
            DecodedOp::TieLeaf { eid, himo_name, himo_kind, bytes } => {
                let Ok(hid) = self.ensure_himo_named(himo_name, *himo_kind) else { return };
                let Some(le) = self.resolve_remote_eid(*eid, hid) else { return };
                self.remote_tieleaf_apply(le, hid, bytes, hlc);
            }
            DecodedOp::Untie { eid, himo_id } => {
                let Some(le) = self.resolve_remote_eid(*eid, *himo_id) else { return };
                self.remote_untie_apply(le, *himo_id, hlc);
            }
            DecodedOp::Delete { eid } => {
                let Some(le) = self.resolve_remote_eid_existing(*eid) else { return };
                self.remote_delete_apply(le, hlc);
            }
            DecodedOp::Content { eid, key, data } => {
                let Some(le) = self.resolve_remote_eid_existing(*eid) else { return };
                self.remote_content_apply(le, key, data, hlc);
            }
            DecodedOp::Commit => {}
        }
    }

    /// recover 中、 与えられた global eid を含む table の next_local を
    /// `(eid - lo) + 1` まで進める (= 次 alloc が衝突しないように)。
    /// global が未登録 table の range なら no-op (= reserved table 等で
    /// recover 順序的に table 定義が先に存在することを期待)。
    /// #117: open 時、 各 user table の `next_local` を live bitmap から自己修復する。
    /// sidecar が未永続 / stale で next_local が巻き戻っていても、 body 永続の bitmap
    /// に残る「範囲内 max live eid + 1」まで next_local を前進させ、 生きた eid の
    /// 再払出を防ぐ。 `next_local` は RAM の AtomicU32 (mmap 非依存) なので readonly
    /// open でも安全 (disk は触らない)。 anonymous / reserved も含め全 table を見る。
    fn reconcile_next_local_from_bitmap(&self) {
        for i in 0..self.tables.len() {
            // extent は払出順なので後ろから見て最初に live が居る所が最大 local
            for (lo, hi) in self.tables[i].extents().into_iter().rev() {
                if let Some(highest) = self.entities.highest_live_in(lo, hi) {
                    Self::advance_table_next_local_for(&self.tables, highest);
                    break;
                }
            }
        }
    }

    fn advance_table_next_local_for(tables: &[TableDef], global: u32) {
        use std::sync::atomic::Ordering;
        for table in tables {
            if let Some(local) = table.local_of(global) {
                let target = local + 1;
                let mut cur = table.next_local.load(Ordering::Relaxed);
                while cur < target {
                    match table.next_local.compare_exchange_weak(
                        cur,
                        target,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(v) => cur = v,
                    }
                }
                return;
            }
        }
    }

    /// 既存の `Engine`(create や create_with_capacity で作成済み、define_himo も
    /// 済ませたもの)を Arc 化して consumer スレッドを起動する。
    ///
    /// 返り値の `Arc<Engine>` を各 writer/reader スレッドで clone して使う。
    /// Drop(最後の Arc が落ちた時)で consumer スレッドは join される。
    pub fn concurrentize(eng: Self) -> std::sync::Arc<Self> {
        Self::spawn_consumer(eng)
    }

    /// `concurrentize` + WAL 後付け版。 `define_himo` などの build phase を `Engine`
    /// 値所有で終えた後、 既存 schema 状態を保ったまま consumer + WAL を起動して
    /// `Arc<Engine>` に遷移する。 sinfo / enchudb-schema の build → runtime 移行に使う。
    ///
    /// 既存 `.wal` ファイルがあれば recover してから consumer 起動。
    /// (build phase で flush 済みなら本体は最新、 WAL は空のまま start)
    #[cfg(not(target_arch = "wasm32"))]
    pub fn concurrentize_with_oplog(eng: Self, oplog_capacity: usize) -> io::Result<std::sync::Arc<Self>> {
        Self::concurrentize_with_oplog_queue_opt(eng, oplog_capacity, None)
    }

    /// #116: `concurrentize_with_oplog` + write/oplog-record queue の capacity 上書き。
    /// 多 DB 同居 / 低メモリ host は 4096〜16384 程度に落とすと per-DB の固定 RSS が
    /// ~128MiB → ~1〜2MiB になる (queue は burst 吸収バッファなので、writer rate が
    /// consumer を長時間超えない workload なら小さくて良い)。省略時 (= 既存 API) は
    /// `max_entities` 連動の scaled default。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn concurrentize_with_oplog_queue(
        eng: Self,
        oplog_capacity: usize,
        queue_capacity: usize,
    ) -> io::Result<std::sync::Arc<Self>> {
        Self::concurrentize_with_oplog_queue_opt(eng, oplog_capacity, Some(queue_capacity))
    }

    /// #116: `concurrentize` (oplog なし) + queue capacity 上書き。
    pub fn concurrentize_queue(eng: Self, queue_capacity: usize) -> std::sync::Arc<Self> {
        Self::spawn_consumer_with_oplog_queue_cap(eng, None, Some(queue_capacity))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn concurrentize_with_oplog_queue_opt(
        mut eng: Self,
        oplog_capacity: usize,
        queue_capacity: Option<usize>,
    ) -> io::Result<std::sync::Arc<Self>> {
        let path = eng.path.clone();
        let oplog_path = oplog_path_for(&path);
        // 古い .crc は WAL 活動後に stale になるので削除 (open_concurrent_with_oplog と同じ扱い)
        let crc_path = crate::integrity::crc_path_for(&path);
        let _ = std::fs::remove_dir_all(&crc_path); // v10: DB は directory
        let _ = std::fs::remove_file(&crc_path);
        let wal = if oplog_path.exists() {
            let w = enchudb_oplog::oplog::OpLog::open(&oplog_path)?;
            // #77-H2 の順序に加えて: **未 commit tail も replay する**。
            // concurrent 経路は WAL append → body 適用 の順に流すので、 crash が
            // その間に入ると 「WAL には在るが body には無い record」 が末尾に残る。
            // ここで適用せずに checkpoint で越えると恒久消失する
            // (`OpLog::recover_with_tail` の doc 参照)。
            let records = w.recover_with_tail();
            for rec in &records {
                eng.apply_oplog_op(&rec.op, rec.hlc, rec.author_peer);
            }
            // #77-H2: body msync → checkpoint の順 (open_concurrent_with_oplog と同じ)
            let _ = eng.body_msync();
            let _ = w.fsync();
            w.advance_checkpoint(w.head());
            std::sync::Arc::new(w)
        } else {
            std::sync::Arc::new(enchudb_oplog::oplog::OpLog::create(&oplog_path, oplog_capacity)?)
        };
        eng.rehydrate_next_sync_lsn(); // #77-H6: recovery 後の rows も含めて復元
        Ok(Self::spawn_consumer_with_oplog_queue_cap(eng, Some(wal), queue_capacity))
    }

    fn spawn_consumer(eng: Self) -> std::sync::Arc<Self> {
        Self::spawn_consumer_with_oplog(eng, None)
    }

    /// changefeed 内部ヘルパ: WAL から emit_offset 以降の record を取り出して
    /// 全 listener に渡し、cursor を進める。
    fn fire_change_listeners(
        wal: &std::sync::Arc<enchudb_oplog::oplog::OpLog>,
        listeners: &std::sync::RwLock<
            Vec<std::sync::Arc<dyn crate::changefeed::ChangeListener>>,
        >,
        emit_offset: &std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        use std::sync::atomic::Ordering;
        // listener 登録ゼロなら何もしない(cursor は進めない:listener 加入時に
        // wal.head() に同期する設計なので、ここで進めると先行 record が漏れる)
        let guard = listeners.read().unwrap();
        if guard.is_empty() {
            return;
        }
        let start = emit_offset.load(Ordering::Acquire);
        // #77-H4: cursor は commit 済み終端まで。 head 再読は in-flight record の
        // 恒久 skip (listener への通知漏れ) を起こしていた。
        let (recs, committed_end) = wal.iter_committed_from_with_end(start);
        if recs.is_empty() {
            emit_offset.store(committed_end, Ordering::Release);
            return;
        }
        let wires: Vec<crate::transport::WireRecord> =
            recs.into_iter().map(|r| r.into()).collect();
        for listener in guard.iter() {
            listener.on_changes(&wires);
        }
        emit_offset.store(committed_end, Ordering::Release);
    }

    fn spawn_consumer_with_oplog(
        eng: Self,
        oplog: Option<std::sync::Arc<enchudb_oplog::oplog::OpLog>>,
    ) -> std::sync::Arc<Self> {
        Self::spawn_consumer_with_oplog_queue_cap(eng, oplog, None)
    }

    /// `spawn_consumer_with_oplog` + queue capacity 上書き。 None = default。
    /// issue4 backpressure 用 — capacity 大きいほど push 側 latency 安定、
    /// 小さいほど RSS 安定 (writer rate >> consumer rate の差を queue で吸収しない)。
    fn spawn_consumer_with_oplog_queue_cap(
        mut eng: Self,
        oplog: Option<std::sync::Arc<enchudb_oplog::oplog::OpLog>>,
        queue_cap: Option<usize>,
    ) -> std::sync::Arc<Self> {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        use crate::write_queue::WriteQueue;

        // #116: default を DB の max_entities に連動させる。1M slot 固定は
        // write_queue + oplog_record_queue の 2 本で per-DB ~128MiB を eager 確保し、
        // per-tenant / per-user に DB を分ける構成の host 密度を RAM(GB)×~8 に縛る。
        // 小 DB (max_entities が小さい) は queue も小さく、default 16M DB は従来
        // どおり 1M slot (挙動不変)。floor 4096 は burst 吸収の下限。
        let qc = queue_cap.unwrap_or_else(|| {
            (eng.max_entities() as usize).clamp(4096, crate::write_queue::DEFAULT_WRITE_QUEUE_CAP)
        });
        let queue = Arc::new(WriteQueue::with_capacity(qc));
        let shutdown = Arc::new(AtomicBool::new(false));
        // WAL 有効時のみ oplog_record_queue を生やす。 writer は直接 wal.append せず
        // ここに owned record を push する → consumer thread が drain して append_many。
        // issue4: bounded `ArrayQueue` で sustained writer の RSS を cap する。
        let oplog_record_queue = oplog.as_ref().map(|_| {
            Arc::new(crossbeam_queue::ArrayQueue::<
                (enchudb_oplog::oplog::OwnedOp, enchudb_oplog::Hlc),
            >::new(qc))
        });

        eng.write_queue = Some(queue.clone());
        eng.shutdown_flag = Some(shutdown.clone());
        eng.oplog = oplog.clone();
        eng.oplog_record_queue = oplog_record_queue.clone();

        let arc = Arc::new(eng);

        let engine_ptr: *const Engine = Arc::as_ptr(&arc);
        let engine_addr = engine_ptr as usize;
        let q_for_thread = queue.clone();
        let flag_for_thread = shutdown.clone();
        let apply_count_for_thread = arc.apply_count.clone();
        let wal_append_count_for_thread = arc.wal_append_count.clone();
        let oplog_for_thread = oplog.clone();
        let oplog_record_queue_for_thread = oplog_record_queue.clone();
        let durable_lsn_for_thread = arc.durable_lsn.clone();
        let listeners_for_thread = arc.change_listeners.clone();
        let emit_offset_for_thread = arc.change_emit_offset.clone();
        let poisoned_for_thread = arc.consumer_poisoned.clone();

        // #77-M2: consumer が panic した場合に unwind 経路で poison を立てる
        // guard。 これが無いと apply_count が永久に追いつかず、 flush_writes /
        // Drop の barrier spin と満杯 queue の producer が全員無限待ちになる
        // (旧 behavior: content cap 超過等の panic でプロセス全体が silent hang)。
        struct PoisonOnPanic {
            flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
            queue: std::sync::Arc<crate::write_queue::WriteQueue>,
        }
        impl Drop for PoisonOnPanic {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    self.flag.store(true, std::sync::atomic::Ordering::Release);
                    self.queue.poison();
                    eprintln!("[enchudb] consumer thread panicked — engine poisoned, subsequent writes/flushes will fail fast");
                }
            }
        }

        let handle = std::thread::Builder::new()
            .name("enchudb-consumer".into())
            .spawn(move || {
                use std::sync::atomic::Ordering;
                use std::time::{Duration, Instant};
                let _poison_guard = PoisonOnPanic {
                    flag: poisoned_for_thread,
                    queue: q_for_thread.clone(),
                };
                let engine: &Engine = unsafe { &*(engine_addr as *const Engine) };
                let fsync_interval = Duration::from_millis(100);
                let mut last_fsync = Instant::now();
                // WAL append 失敗（満杯等）の warn-once。 失敗した record は table
                // 本体には apply 済みだが sync には二度と流れない — これが無音だと
                // 「配布だけが死んでいる」を誰も観測できない（実機発現）。
                let mut warned_wal_append = false;

                loop {
                    let mut drained_any = false;
                    // WAL record queue を batch drain (per-record flock を償却)
                    if let (Some(wq), Some(wal)) = (
                        oplog_record_queue_for_thread.as_ref(),
                        oplog_for_thread.as_ref(),
                    ) {
                        let mut batch: Vec<enchudb_oplog::oplog::OwnedOp> = Vec::new();
                        let mut hlcs: Vec<enchudb_oplog::Hlc> = Vec::new();
                        while let Some((rec, hlc)) = wq.pop() {
                            batch.push(rec);
                            hlcs.push(hlc);
                        }
                        if !batch.is_empty() {
                            // append 失敗でも進める: barrier の意味は「queue に
                            // 残っていない」であり、 失敗 record の再送は無い
                            // request17-A3: push 時に採番済みの HLC で書く
                            // (採番し直すと cell の版数と record がずれる)。
                            match wal.append_many_with_hlcs(&batch, &hlcs) {
                                Ok(_) => {
                                    wal_append_count_for_thread
                                        .fetch_add(batch.len() as u64, Ordering::Release);
                                }
                                Err(e) => {
                                    if !warned_wal_append {
                                        warned_wal_append = true;
                                        eprintln!(
                                            "[enchudb] warning: WAL append failed ({e}) — \
                                             {} record(s) dropped from the sync path \
                                             (tables are still updated locally)",
                                            batch.len()
                                        );
                                    }
                                }
                            }
                            drained_any = true;
                        }
                    }
                    while let Some(op) = q_for_thread.pop() {
                        drained_any = true;
                        engine.apply_op(op);
                        apply_count_for_thread.fetch_add(1, Ordering::Release);
                    }

                    // 背景 fsync: WAL 有効 & 前回から fsync_interval 経過 &
                    // head が checkpoint より進んでいる時のみ実行。
                    // 順序厳守: auto-Commit → WAL fsync → body msync → checkpoint 前進。
                    if let Some(wal) = oplog_for_thread.as_ref() {
                        if last_fsync.elapsed() >= fsync_interval {
                            if wal.head() > wal.checkpoint() {
                                let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
                                // #77-H3: checkpoint の上限は「今回の fsync/msync に
                                // 含まれることが確定した位置」= Commit append 直後の
                                // head。 msync 後に head を再読すると、 その間に
                                // append された record が「body 効果が msync に
                                // 含まれないのに checkpoint される」= crash で
                                // fsync 済み write が replay 対象外になり消失した。
                                let durable_head = wal.head();
                                let durable_lsn = wal.next_lsn().saturating_sub(1);
                                let _ = wal.fsync();
                                let _ = engine.body_msync();
                                // 0.8.1: 周期 fsync でも tables sidecar を persist。
                                // checkpoint を前進させる前に必ず sidecar を固める
                                // (= 直後に process kill されても次 open で sidecar
                                // の next_local が oplog の進行と整合する)。
                                engine.try_persist_tables();
                                wal.advance_checkpoint(durable_head);
                                durable_lsn_for_thread.store(durable_lsn, Ordering::Release);

                                // 0.8.0: sync 並走の解消 — durable 化した record を
                                // _sync_ops に自動転送する (= 0.7.0 では user が手動で
                                // transfer_oplog_to_sync_ops() を呼ぶ必要があったが、
                                // 0.8.0 で primary 切替につき自動化)。
                                // enable_sync 未呼出なら no-op、 副作用ゼロ。
                                if engine.sync_tables_enabled() {
                                    engine.transfer_oplog_to_sync_ops();
                                }

                                // changefeed: durable 化した record を listener に push
                                Self::fire_change_listeners(
                                    wal,
                                    &listeners_for_thread,
                                    &emit_offset_for_thread,
                                );
                            }
                            // ring buffer reset を試みる。head == checkpoint &&
                            // pending_writes == 0 のときだけ head/checkpoint を HEADER_SIZE に戻す。
                            // これで WAL 容量を食い切らずに長期運用できる。
                            // ※ auto_reset が発動して offset が後退したら listener cursor もリセット。
                            // 0.9.0: 畳む前に bridge を必ず追いつかせる。 上の transfer は
                            // head > checkpoint の時しか走らないが、 caller thread の
                            // oplog_sync が checkpoint を進めた直後は head == checkpoint の
                            // まま bridge 未了 record が残りえる (cursor 追いつき済みなら
                            // 空 scan で即返るので毎 tick 呼んで無害)。
                            if engine.sync_tables_enabled() {
                                engine.transfer_oplog_to_sync_ops();
                            }
                            // 0.18.2: bridge が未読領域を残している間（ring 満杯の
                            // backpressure 中）は畳まない。 畳むと未 bridge record が
                            // 消えて sync から無言で欠落する（実機発現）。
                            if Engine::trace_bridge_enabled() {
                                eprintln!(
                                    "[fold] try head={} cp={} offset={} safe={}",
                                    wal.head(),
                                    wal.checkpoint(),
                                    engine.sync_ops_bridge_offset(),
                                    engine.wal_fold_safe(),
                                );
                            }
                            // fold は bridge cursor を巻き戻す (`reset_sync_ops_offset`)。
                            // in-flight の `transfer_oplog_to_sync_ops` は入口で読んだ
                            // `from` を元に **最後に** cursor を store するので、 fold と
                            // 並走すると巻き戻しが stale 値で上書きされ、 cursor が head を
                            // 追い越したまま固定する (= 新 ring の record が永久に scan
                            // 対象外 + `wal_fold_safe` が offset>=head を「追いつき済み」と
                            // 誤読して畳み続ける = 無言の恒久欠落)。 transfer と同じ lock を
                            // 取って直列化する。 lock 順は transfer_lock → append_lock で
                            // transfer 自身 (row insert → append) と同じなので deadlock しない。
                            let fold_guard = engine.transfer_lock_for_fold();
                            if engine.wal_fold_safe()
                                && wal.try_reset_if(|| engine.wal_fold_safe_locked())
                            {
                                emit_offset_for_thread.store(
                                    enchudb_oplog::oplog::HEADER_SIZE as u64,
                                    Ordering::Release,
                                );
                                // #63 regression fix: bridge cursor も巻き戻す。
                                // これが無いと reset 後の record が sync 欠落する。
                                engine.reset_sync_ops_offset();
                            }
                            drop(fold_guard);
                            last_fsync = Instant::now();
                        }
                    }

                    if flag_for_thread.load(Ordering::Acquire) {
                        // 最終 WAL drain
                        if let (Some(wq), Some(wal)) = (
                            oplog_record_queue_for_thread.as_ref(),
                            oplog_for_thread.as_ref(),
                        ) {
                            let mut batch: Vec<enchudb_oplog::oplog::OwnedOp> = Vec::new();
                            let mut hlcs: Vec<enchudb_oplog::Hlc> = Vec::new();
                            while let Some((rec, hlc)) = wq.pop() {
                                batch.push(rec);
                                hlcs.push(hlc);
                            }
                            if !batch.is_empty() {
                                // request17-A3: push 時に採番済みの HLC で書く。
                                match wal.append_many_with_hlcs(&batch, &hlcs) {
                                    Ok(_) => {
                                        wal_append_count_for_thread
                                            .fetch_add(batch.len() as u64, Ordering::Release);
                                    }
                                    Err(e) => {
                                        // shutdown 経路は一度しか通らないので gate の
                                        // 再セットは不要（main loop 側と共有の warn-once）
                                        if !warned_wal_append {
                                            eprintln!(
                                                "[enchudb] warning: WAL append failed ({e}) — \
                                                 {} record(s) dropped from the sync path \
                                                 (tables are still updated locally)",
                                                batch.len()
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        while let Some(op) = q_for_thread.pop() {
                            engine.apply_op(op);
                            apply_count_for_thread.fetch_add(1, Ordering::Release);
                        }
                        // shutdown 時の最終 Commit + 順序付き同期
                        if let Some(wal) = oplog_for_thread.as_ref() {
                            let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
                            let durable_head = wal.head(); // #77-H3: msync 前に snapshot
                            let _ = wal.fsync();
                            let _ = engine.body_msync();
                            // 0.8.1: shutdown 時に tables sidecar を強制 persist。
                            // 旧 behavior では body_msync のみで `next_local` が
                            // sidecar に書かれず、 short-lived CLI (sinfo の sf 等)
                            // で次 open 時に eid 衝突が出ていた。 graceful shutdown
                            // 経路では oplog checkpoint も進めてしまうので、 ここで
                            // sidecar を確実に固める必要がある。
                            engine.try_persist_tables();
                            wal.advance_checkpoint(durable_head);
                            // 0.8.0: shutdown 時も最終 sync 転送 (drop で残った record も
                            // _sync_ops に bridge し切ってから抜ける)
                            if engine.sync_tables_enabled() {
                                engine.transfer_oplog_to_sync_ops();
                            }
                            // shutdown 時の最終 emit
                            Self::fire_change_listeners(
                                wal,
                                &listeners_for_thread,
                                &emit_offset_for_thread,
                            );
                            // graceful close でも ring を畳む: head==checkpoint なら
                            // HEADER_SIZE へ巻き戻す。 100ms fsync tick を踏まない
                            // short-lived writer (events.ecdb の log_event 等) でも
                            // 次 open が full のまま始まらないようにするため。
                            // 0.18.2: こちらも bridge 未読領域があるうちは畳まない。
                            if Engine::trace_bridge_enabled() {
                                eprintln!(
                                    "[fold] try head={} cp={} offset={} safe={}",
                                    wal.head(),
                                    wal.checkpoint(),
                                    engine.sync_ops_bridge_offset(),
                                    engine.wal_fold_safe(),
                                );
                            }
                            // fold は bridge cursor を巻き戻す (`reset_sync_ops_offset`)。
                            // in-flight の `transfer_oplog_to_sync_ops` は入口で読んだ
                            // `from` を元に **最後に** cursor を store するので、 fold と
                            // 並走すると巻き戻しが stale 値で上書きされ、 cursor が head を
                            // 追い越したまま固定する (= 新 ring の record が永久に scan
                            // 対象外 + `wal_fold_safe` が offset>=head を「追いつき済み」と
                            // 誤読して畳み続ける = 無言の恒久欠落)。 transfer と同じ lock を
                            // 取って直列化する。 lock 順は transfer_lock → append_lock で
                            // transfer 自身 (row insert → append) と同じなので deadlock しない。
                            let fold_guard = engine.transfer_lock_for_fold();
                            if engine.wal_fold_safe()
                                && wal.try_reset_if(|| engine.wal_fold_safe_locked())
                            {
                                emit_offset_for_thread.store(
                                    enchudb_oplog::oplog::HEADER_SIZE as u64,
                                    std::sync::atomic::Ordering::Release,
                                );
                                // #63 regression fix: bridge cursor も巻き戻す。
                                engine.reset_sync_ops_offset();
                            }
                            drop(fold_guard);
                        }
                        return;
                    }

                    if !drained_any {
                        // yield_now alone is effectively a busy-spin on multi-core
                        // (OS reschedules immediately). Sleep briefly so the consumer
                        // doesn't peg a core when the queue is idle. fsync runs every
                        // 100ms so 1ms tick is plenty fine-grained.
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            })
            .expect("failed to spawn consumer thread");

        *arc.consumer_handle.lock().unwrap() = Some(handle);
        arc
    }

    /// body mmap の msync(WAL 順序と絡むので &self で呼び出し可能)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn body_msync(&self) -> io::Result<()> {
        // v10: segment ごとに dirty range (Column::set 等が mark_dirty した範囲) だけ msync。
        // `mark_dirty` を通さない EntitySet / header は全域 (小さい固定 segment)。
        match &self.backing {
            Backing::Segments(set) => {
                set.flush_dirty_all()?;
                // 「この時点の segment 長」 を記録する。 次の open で切り詰めを検出できる。
                set.write_manifest()
            }
            Backing::Memory(_) => Ok(()),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn body_msync(&self) -> io::Result<()> { Ok(()) }

    /// 強制同期: Commit marker 挿入 → WAL fsync → body msync → checkpoint 前進。
    /// Sync mode 相当の待ち。
    pub fn oplog_sync(&self) -> io::Result<()> {
        use std::sync::atomic::Ordering;
        self.flush_writes();
        if let Some(wal) = self.oplog.as_ref() {
            let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
            // #77-H3: checkpoint 上限と durable_lsn は Commit append 直後に
            // snapshot (msync 後の再読は未同期 record まで checkpoint してしまう)
            let durable_head = wal.head();
            let durable_lsn = wal.next_lsn().saturating_sub(1);
            wal.fsync()?;
            self.body_msync()?;
            wal.advance_checkpoint(durable_head);
            self.durable_lsn.store(durable_lsn, Ordering::Release);
            // 0.9.0: checkpoint を進めたら bridge も追いつかせる。 consumer tick の
            // try_reset は head==checkpoint の ring を無条件に畳むため、 ここで
            // transfer しないと「committed だが bridge 未了」の record が wipe され
            // sync から永久に消える (sync lib テスト flaky の第 2 の根)。
            if self.sync_tables_enabled() {
                if Self::trace_bridge_enabled() {
                    eprintln!(
                        "[sync] after checkpoint: head={} cp={} durable_head={durable_head}",
                        wal.head(),
                        wal.checkpoint(),
                    );
                }
                self.transfer_oplog_to_sync_ops();
            }
            // changefeed: durable 化したので listener へ即時 push
            // (consumer の 100ms tick を待たず caller スレッドで発火)
            Self::fire_change_listeners(wal, &self.change_listeners, &self.change_emit_offset);
        }
        Ok(())
    }

    /// 現在耐久化されている LSN(Commit を含む最後の WAL fsync 到達位置)。
    pub fn durable_lsn(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.durable_lsn.load(Ordering::Acquire)
    }

    /// エンジン状態の一覧。監視・デバッグ用。
    // ──── Phase F: 監査 ────

    /// WAL 監査: commit 済みレコードを filter して返す。
    ///
    /// WAL が有効でない(`create_concurrent_with_oplog` 未使用等)なら空 Vec。
    /// `AuditFilter::default()` = 全件。
    ///
    /// 返る各 `Record` には lsn, hlc, author_peer, op, signature, pubkey_fp が入り、
    /// 「誰がいつ何を書いたか」をそのまま監査できる。
    pub fn audit(&self, filter: &AuditFilter) -> Vec<enchudb_oplog::oplog::Record> {
        let recs = match self.oplog.as_ref() {
            Some(w) => w.iter_committed(),
            None => return Vec::new(),
        };
        recs.into_iter()
            .filter(|r| {
                if let Some(ref from) = filter.from_hlc {
                    if &r.hlc < from {
                        return false;
                    }
                }
                if let Some(ref to) = filter.to_hlc {
                    if &r.hlc > to {
                        return false;
                    }
                }
                if let Some(author) = filter.author_peer {
                    if r.author_peer != author {
                        return false;
                    }
                }
                if let Some(fp) = filter.pubkey_fp {
                    if r.pubkey_fp != fp {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    // ──── Phase E: snapshot export ────

    /// 現在の DB を target パスへ snapshot としてコピーする。
    ///
    /// コピーする:
    /// - main DB(`self.path` → `target`)
    /// - WAL(`self.path.wal` → `target.wal`)(存在時のみ)
    /// - region CRC sidecar(`self.path.crc` → `target.crc`)(存在時のみ)
    ///
    /// **呼び出し前に必ず `oplog_sync()` or `flush()` で durable 化すること**。
    /// mmap 非同期ページが残っていると snapshot に反映されない。
    ///
    /// body の copy は `copy_sparse` で行う (= 穴を穴のまま写す)。 DB body は
    /// apparent が巨大で実データがごく一部の sparse ファイルなので、 素の
    /// `std::fs::copy` だと **Linux では apparent 全量が物理化する** (既定
    /// capacity で 24 GB。 詳細は `sparse_copy` の module doc)。
    ///
    /// # durability
    ///
    /// **この関数は fsync しない。** 書き出しは page cache 止まりで、 直後に電源断が
    /// 起きれば snapshot は残らない。 これは意図した方針で、 source を fsync 再
    /// persist しないことで snapshot を速く保っている (sidecar 側も同じ。 下の #9 (H2)
    /// のコメント参照)。 **backup として残すなら呼び出し側が fsync すること。**
    ///
    /// 返り値: コピーしたパス一覧。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn snapshot_export(&self, target: &str) -> io::Result<SnapshotFiles> {
        use crate::sparse_copy::copy_sparse;
        // mmap ページを物理ディスクに確実に書き出す
        self.body_msync()?;

        let mut files = SnapshotFiles {
            main: target.to_string(),
            oplog: None,
            crc: None,
        };
        // v10: 本体は directory。 segment file だけを 1 つずつ写す (Windows の sparse segment
        // も穴を穴のまま写せるよう copy_sparse を使う)。 sidecar は下で個別に扱う。
        copy_db_segments(std::path::Path::new(&self.path), std::path::Path::new(target))?;

        for (name, slot) in [
            (crate::db_files::OPLOG, &mut files.oplog),
            (crate::db_files::CRC, &mut files.crc),
        ] {
            let src = crate::db_files::path_for(&self.path, name);
            if src.exists() {
                let dst = crate::db_files::path_for(target, name);
                copy_sparse(&src, &dst)?;
                *slot = Some(dst.to_string_lossy().into_owned());
            }
        }

        // #9 (H2): sidecar (.tables/.eidmap) は source を copy せず、 現在の in-memory
        // 状態を直接 target へ書き出す。 source copy だと consumer tick (≤100ms 周期)
        // 時点の stale sidecar を新しい body に対してコピーしてしまい、 翻訳 entity が
        // .eidmap に載らず restore 後の再 sync で重複する。 直接書き出しなら msync 済 body
        // と必ず整合し、 かつ source を fsync 再 persist しないので snapshot が遅くならない
        // (durability は copy と同じ page-cache level、 backup の fsync は呼び出し側責務)。
        // 0.7.0: .tables には reserved table `_sync_ops` / `_sync_peers` も含む。
        if !self.tables.is_empty() {
            std::fs::write(tables_path_for(target), serialize_tables(&self.tables))?;
        }
        let eidmap_entries = self.eidmap_entries_with_tombstones();
        if !eidmap_entries.is_empty() {
            std::fs::write(eidmap_path_for(target), serialize_eidmap(&eidmap_entries))?;
        }
        // text 写像も同じ理由で in-memory から直接書き出す。 これが抜けると restore 後に
        // 受信済み `Vocab` の写像だけが失われ、 後続 `Tie` を翻訳できなくなる。
        let vocabmap_entries = self.peer_vocab_map_entries();
        if !vocabmap_entries.is_empty() {
            std::fs::write(vocabmap_path_for(target), serialize_vocabmap(&vocabmap_entries))?;
        }
        // manifest が無いと snapshot だけ 「切り詰め検出の効かない DB」 になる。 複製は
        // source と同じ長さなので source の manifest をそのまま写せる (記録は下限として
        // 使うので、 複製中に source が伸びていても有害にならない)。 source に無い場合だけ
        // 複製先を walk して作る。
        {
            let from = crate::db_files::path_for(&self.path, crate::db_files::SEGMENTS);
            let to = crate::db_files::path_for(target, crate::db_files::SEGMENTS);
            if std::fs::copy(&from, &to).is_err() {
                crate::segments::write_manifest_from_dir(std::path::Path::new(target))?;
            }
        }

        Ok(files)
    }

    pub fn stats(&self) -> EngineStats {
        use std::sync::atomic::Ordering;
        let (oplog_head, oplog_checkpoint, oplog_capacity, oplog_lsn) = match self.oplog.as_ref() {
            Some(w) => (w.head(), w.checkpoint(), w.usage().1, w.next_lsn().saturating_sub(1)),
            None => (0, 0, 0, 0),
        };
        let (peer_id, hlc_entries, max_hlc) = (
            self.peer_id.load(Ordering::Acquire),
            self.hlc_store.len(),
            self.hlc_store.max_hlc(),
        );

        EngineStats {
            entity_count: self.entities.count(),
            himo_count: self.value_types.len() as u32,
            oplog_head,
            oplog_checkpoint,
            oplog_capacity,
            oplog_lag_bytes: oplog_head.saturating_sub(oplog_checkpoint),
            oplog_next_lsn: oplog_lsn,
            durable_lsn: self.durable_lsn.load(Ordering::Acquire),
            queue_len: self.write_queue.as_ref().map(|q| q.len()).unwrap_or(0),
            pushed: self.push_count.load(Ordering::Acquire),
            applied: self.apply_count.load(Ordering::Acquire),
            peer_id,
            bind_over_local_writes: self.bind_over_local_writes(),
            hlc_entries,
            max_hlc,
        }
    }

    /// 0.8.16 (issue #54): vocab の orphan (= 死蔵 vid) を read-only scan。
    /// `ValueType::Leaf` の `vocab.insert` 経路は re-tie / remove で旧 vid を
    /// 回収しないため、 long-lived な curated store (= 元ソースから rebuild
    /// しないタイプ) で vocab data が単調増加する。 この API でその実量を計測。
    ///
    /// 計測手順:
    /// 1. 全 himo (Tag / Leaf) の `unique_values()` を union して live vid 集合を作る
    /// 2. `(0..vocab.count())` のうち live 集合に無いものを orphan と判定
    /// 3. orphan vid の `vocab.get(vid).len()` を合計して救出可能 bytes を出す
    ///
    /// O(vocab.count() + Σ himos.unique_values().len())。 vid set は `Vec<bool>` で
    /// vocab.count() bit。 vocab.count() = 1B なら 1GB 食うので注意。 巨大 DB なら
    /// 別 issue で BitVec か stream 化を検討。
    /// v6 (#88): LeafStore の現 footprint (high_water)。 pre-v6 DB (leaf region
    /// 無し) は None。 routing 前 (2.1 scaffolding) は常に HEADER 相当。
    pub fn leaf_footprint(&self) -> Option<u64> {
        self.leaf.as_ref().map(|l| l.high_water())
    }

    /// vocab data 領域の消費 byte 数 (単調・回収なし)。 #88 bench で
    /// 「Leaf を vocab に載せる旧挙動」の footprint 増加を計測する用。
    pub fn vocab_data_footprint(&self) -> u32 {
        self.vocab.data_footprint()
    }

    pub fn vocab_orphan_stats(&self) -> VocabOrphanStats {
        let vocab_total = self.vocab.count();
        if vocab_total == 0 {
            return VocabOrphanStats {
                vocab_total: 0,
                live_vids: 0,
                orphan_vids: 0,
                orphan_bytes: 0,
                live_bytes: 0,
            };
        }
        // live vid 集合 — Tag / Leaf 両方を対象。 Number / Ref は vocab を使わない。
        // 各 himo の unique_values は局所 stored 値 (= cylinder side で管理) を返す。
        // stored は内部表現値 +1 ではなく素の vid なので decode 不要。
        let mut is_live = vec![false; vocab_total as usize];
        for hid in 0..self.value_types.len() {
            // v6 (#88): routed-Leaf の cell は vocab vid でなく LeafStore offset なので
            // vocab の live 判定から除外する (含めると無関係な vid を live 誤判定)。
            if self.leaf_for(hid).is_some() { continue; }
            match self.value_types[hid] {
                ValueType::Tag | ValueType::Leaf => {
                    let vids = self.himos[hid].unique_values();
                    for v in vids {
                        if (v as usize) < is_live.len() {
                            is_live[v as usize] = true;
                        }
                    }
                }
                _ => {}
            }
        }
        let mut live_vids: u32 = 0;
        let mut orphan_vids: u32 = 0;
        let mut live_bytes: u64 = 0;
        let mut orphan_bytes: u64 = 0;
        for vid in 0..vocab_total {
            let len = self.vocab.get(vid).len() as u64;
            if is_live[vid as usize] {
                live_vids += 1;
                live_bytes += len;
            } else {
                orphan_vids += 1;
                orphan_bytes += len;
            }
        }
        VocabOrphanStats {
            vocab_total,
            live_vids,
            orphan_vids,
            orphan_bytes,
            live_bytes,
        }
    }

    /// consumer スレッド内部で呼ぶ。Op 1 個を適用。
    fn apply_op(&self, op: crate::write_queue::Op) {
        use crate::write_queue::Op;
        match op {
            Op::Tie { eid, himo_id, value, hlc } => {
                let hid = himo_id as usize;
                if hid >= self.himos.len() { return; }
                // v6 (#88): routed-Leaf の re-tie 上書きは旧 offset を free (async path)。
                // #119: **publish → free** の順 (逆順だと並行 reader が再利用 slot を読む)。
                let old = self.take_leaf_cell(eid, hid);
                // request17 step 4: push 時に採番した版数で値と一緒に書く。
                if self.set_cell_local(eid, himo_id, value, hlc) {
                    self.free_leaf_offset(hid, old);
                } else {
                    // 不採用: cell は旧値のまま = 旧 payload はまだ生きている。
                    // 代わりに push 側が確保済みの **新** payload (= value) を捨てる
                    // (routed-Leaf 以外では no-op)。
                    self.free_leaf_offset(hid, Some(value));
                    self.warn_local_write_rejected(eid, himo_id, hlc);
                }
            }
            Op::Untie { eid, himo_id, hlc } => {
                let hid = himo_id as usize;
                if hid >= self.himos.len() { return; }
                if !self.clear_cell_local_freeing_leaf(eid, himo_id, hlc) {
                    self.warn_local_write_rejected(eid, himo_id, hlc);
                }
            }
            Op::Delete { eid, hlc } => {
                // request17 (A-5): 削除の版数は eid 空間の tombstone column へ。
                if !self.set_tombstone_local(eid, hlc) {
                    self.warn_local_write_rejected(eid, u16::MAX, hlc);
                    return;
                }
                for hid in 0..self.himos.len() {
                    self.free_leaf_cell(eid, hid);
                    self.himos[hid].remove(eid);
                }
                self.entities.free(eid);
            }
            Op::EntityCreated { local: _ } => {
                // v4 (undo 廃止) 以降は no-op。 `entity()` で local slot は writer
                // thread 側で既に allocate 済み。 ここに来るのは `flush_writes` の
                // push_count / apply_count counter を対称に進めるためだけ (issue5)。
                // #195: 新規 code はこの op を queue に積まない (counter 直接 bump に
                // 置換 — blocking push が consumer 自身の bridge 経路で livelock を
                // 起こした)。 variant と本 arm は互換のため残置。
            }
        }
    }

    /// 非同期 tie。`create_concurrent`/`concurrentize` で有効化済みの場合のみ使える。
    /// 紐名は事前に `define_himo` で定義されている必要がある。
    /// WriteQueue は SegQueue(unbounded)なので push は必ず成功する。
    ///
    /// WAL が有効な場合: tie_async は WAL append (memcpy) → WriteQueue push の順で実行する。
    /// WAL append は `.wal` ファイルに memcpy 1 回、100ns オーダー。hot path で fsync しない。
    pub fn tie_async(&self, eid: enchudb_oplog::EntityId, himo: &str, value: u32) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_async_by_id(eid, hid, value);
    }

    /// `tie_async` の himo_id 直指定版。 SNS の post / like 投入のように row/sec が KO
    /// 単位の hot path 用 (per-call の `himo_id(&str)` HashMap lookup を消す)。
    /// 起動時に `himo_id(&str)` で u16 を 1 回引いて cache し、 hot loop で繰り返し使う想定。
    pub fn tie_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: u32) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        if value == u32::MAX {
            // #59: sentinel 値は cell に入らない。 panic せず write を拒否 + 計上。
            self.record_fault(
                FaultKind::ValueOutOfRange,
                "tie value == u32::MAX (sentinel reserved)",
            );
            return;
        }
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か (anonymous
        // open-ended なら即 return)
        self.validate_eid_for_himo(himo_id as usize, local);
        // β-light step 5: Ref himo の FK validation (非 Ref は即 return で
        // ~1 ns、 Ref で fk_refs entry なしも同じ)
        self.validate_ref_tie(himo_id as usize, value);
        // #77-H4: op を write_queue へ push してから WAL record を push する。
        // 逆順 (record 先) だと 2 push の間で preempt された場合、 consumer が
        // record を fsync + checkpoint した時点で op が未適用となり、 crash で
        // 「fsync 済みの write」が replay 対象外になって消えた。 op 先行なら
        // consumer が wq drain で record を見た時点で op は必ず queue に居て、
        // 同 tick の q drain (wq の後) で適用される。
        //
        // request17 step 4 (A-3): 版数は **push 側で 1 回だけ**採番し、 write_queue の
        // op と WAL record の両方に同じ値を運ぶ。 適用も append も consumer thread が
        // 後から別々に行うため、 どちらかで採番し直すと cell の版数と配った版数がずれる。
        // 採番と 2 本の queue push は 1 単位 (`hlc_mint_lock` の doc 参照)。
        let _mint = self.mint_guard();
        let hlc = self.mint_local_hlc();
        let q = self.write_queue.as_ref()
            .expect("tie_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie { eid: local, himo_id, value, hlc });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let rec = enchudb_oplog::oplog::OwnedOp::Tie { eid: oplog_eid, himo_id, value };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                push_oplog_record_blocking(wq, rec, hlc, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append_at_hlc(rec.as_op(), hlc);
            }
        }
    }

    /// 非同期 tie_text。text 値を vocab に挿入し、WAL に Vocab + Tie の 2 op を流す。
    ///
    /// 流れ:
    /// 1. local vocab に bytes を get_or_insert → `local_vid`
    /// 2. WAL に `Vocab { vid: local_vid, bytes }` を append
    ///    - receiver 側でこれを受けると `(author_peer, vid) → receiver_local_vid` の mapping が張られる
    /// 3. WAL に `Tie { eid, himo_id, value: local_vid }` を append
    /// 4. write_queue に Tie push で本体に apply
    ///
    /// `Engine::tie_text` の &self 版。`tie_async` と異なり text を sync 対象に乗せる。
    pub fn tie_text_async(&self, eid: enchudb_oplog::EntityId, himo: &str, value: &str) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_text_async_by_id(eid, hid, value);
    }

    /// `tie_text_async` の himo_id 直指定版。 text 本体は vocab 経由なので value はそのまま。
    pub fn tie_text_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: &str) {
        self.tie_bytes_async_by_id(eid, himo_id, value.as_bytes());
    }

    /// `tie_text_async_by_id` の bytes 版 (0.9.0: content 互換層も使う)。
    pub fn tie_bytes_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, value: &[u8]) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        let hid = himo_id as usize;
        debug_assert!(hid < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(hid, local);
        // v6 (#88): Leaf は LeafStore へ。 offset を write_queue の value に載せ、
        // WAL には bytes 同乗 TieLeaf を流す。 旧 offset の free は apply_op::Tie 側
        // (= 適用時点の cell を見る。 push 時 free は queue 未適用の二重 free を招く)。
        if let Some(leaf) = self.leaf_for(hid) {
            let off = leaf.insert(value);
            if off == u32::MAX {
                // #167: leaf payload を書けなかった (commit を伸ばせない = ディスク
                // 満杯)。 sentinel を cell に書くと read が壊れるので write を拒否。
                self.record_fault(
                    FaultKind::DiskSpace,
                    "leaf payload の格納に必要な commit を伸ばせない — text write を拒否",
                );
                return;
            }
            // request17 step 4: push 側で採番、 op と record に同じ版数を載せる。
            // 採番 → push は `hlc_mint_lock` で 1 単位に。
            let _mint = self.mint_guard();
            let hlc = self.mint_local_hlc();
            let q = self.write_queue.as_ref()
                .expect("tie_bytes_async requires create_concurrent or concurrentize");
            q.push(crate::write_queue::Op::Tie { eid: local, himo_id, value: off, hlc });
            self.push_count.fetch_add(1, Ordering::Release);
            if let Some(wal) = self.oplog.as_ref() {
                let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
                let rec = enchudb_oplog::oplog::OwnedOp::TieLeaf {
                    eid: oplog_eid,
                    himo_name: self.himo_names[hid].clone(),
                    himo_kind: self.value_types[hid] as u8,
                    bytes: value.to_vec(),
                };
                if let Some(wq) = self.oplog_record_queue.as_ref() {
                    push_oplog_record_blocking(wq, rec, hlc, &self.consumer_poisoned, &self.wal_push_count);
                } else {
                    let _ = wal.append_at_hlc(rec.as_op(), hlc);
                }
            }
            return;
        }
        // Tag は dedupe、Leaf は常に新規 id。
        let vid = match self.value_types[hid] {
            ValueType::Tag => self.vocab.get_or_insert(value),
            ValueType::Leaf => self.vocab.insert(value),
            ht => panic!("tie_bytes_async_by_id on non-text himo_id {}: {:?}", himo_id, ht),
        };
        if vid == u32::MAX {
            // #59: vocab 満杯 (insert が sentinel を返した) or sentinel 値。
            self.record_fault(
                FaultKind::VocabSpace,
                "vocab vid == u32::MAX — text write rejected (vocab_max_entries 到達か \
                 sentinel 値)。 GrowableOptions { vocab_max_entries: Some(n), .. } を参照",
            );
            return;
        }
        // #77-H4: op 先行 push (tie_async_by_id と同じ理由)
        // request17 step 4: Vocab は cell を持たない (= 版数の対象外) が、 record queue は
        // 版数付きで運ぶので Tie の手前で 1 個採番しておく (WAL 上の並びと HLC の
        // 並びを揃える)。
        let _mint = self.mint_guard();
        let vocab_hlc = self.mint_local_hlc();
        let hlc = self.mint_local_hlc();
        let q = self.write_queue.as_ref()
            .expect("tie_text_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie { eid: local, himo_id, value: vid, hlc });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            // Vocab op を先に(sync の receiver 側で Tie より先に mapping が張られるよう)
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let vocab_rec = enchudb_oplog::oplog::OwnedOp::Vocab { vid, bytes: value.to_vec() };
            let tie_rec = if self.himo_is_content(hid) {
                // 0.9.0: 動的 content himo は id が peer 間で揃わないため名前で運ぶ
                enchudb_oplog::oplog::OwnedOp::TieNamed {
                    eid: oplog_eid,
                    himo_name: self.himo_names[hid].clone(),
                    himo_kind: self.value_types[hid] as u8,
                    value: vid,
                }
            } else {
                enchudb_oplog::oplog::OwnedOp::Tie { eid: oplog_eid, himo_id, value: vid }
            };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                // Vocab → Tie の順を保つため同一 thread から連続 push
                push_oplog_record_blocking(wq, vocab_rec, vocab_hlc, &self.consumer_poisoned, &self.wal_push_count);
                push_oplog_record_blocking(wq, tie_rec, hlc, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append_at_hlc(vocab_rec.as_op(), vocab_hlc);
                let _ = wal.append_at_hlc(tie_rec.as_op(), hlc);
            }
        }
    }

    /// 非同期 tie_ref。target_eid の local 部(u32)を WAL / 本体に運ぶ。
    /// target_eid は u64 [peer|local] だが、現行 WAL は value: u32 しか運べないため
    /// peer_id 部は捨てる。すなわち receiver 側では「author_peer と同一 peer 上の entity」を
    /// 指す ref として再構成される。cross-peer ref (peer A が peer B の entity を指す)は
    /// 現状未対応 (Ref 用 WAL op を別途用意する必要あり)。
    pub fn tie_ref_async(&self, eid: enchudb_oplog::EntityId, himo: &str, target_eid: enchudb_oplog::EntityId) {
        let hid = self.himo_id(himo)
            .unwrap_or_else(|| panic!("himo '{}' not defined", himo)) as u16;
        self.tie_ref_async_by_id(eid, hid, target_eid);
    }

    /// `tie_ref_async` の himo_id 直指定版。
    pub fn tie_ref_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16, target_eid: enchudb_oplog::EntityId) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        let target_local = enchudb_oplog::eid_local(target_eid);
        if target_local == u32::MAX {
            self.record_fault(
                FaultKind::ValueOutOfRange,
                "tie_ref target_local == u32::MAX (sentinel reserved)",
            );
            return;
        }
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        // β-light step 6: eid が himo の所属 table eid_range 内か
        self.validate_eid_for_himo(himo_id as usize, local);
        // β-light step 5: target_eid が target_table の eid range 内か
        self.validate_ref_tie(himo_id as usize, target_local);
        // #77-H4: op 先行 push (tie_async_by_id と同じ理由)
        // request17 step 4: push 側で採番、 op と record に同じ版数 (採番 → push は 1 単位)。
        let _mint = self.mint_guard();
        let hlc = self.mint_local_hlc();
        let q = self.write_queue.as_ref()
            .expect("tie_ref_async requires create_concurrent or concurrentize");
        q.push(crate::write_queue::Op::Tie {
            eid: local, himo_id, value: target_local, hlc,
        });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let rec = enchudb_oplog::oplog::OwnedOp::Tie {
                eid: oplog_eid, himo_id, value: target_local,
            };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                push_oplog_record_blocking(wq, rec, hlc, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append_at_hlc(rec.as_op(), hlc);
            }
        }
    }

    /// 非同期 untie。
    pub fn untie_async(&self, eid: enchudb_oplog::EntityId, himo: &str) {
        let hid = match self.himo_id(himo) { Some(x) => x as u16, None => return };
        self.untie_async_by_id(eid, hid);
    }

    /// `untie_async` の himo_id 直指定版。
    pub fn untie_async_by_id(&self, eid: enchudb_oplog::EntityId, himo_id: u16) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        debug_assert!((himo_id as usize) < self.himos.len(),
            "himo_id {} out of range (max {})", himo_id, self.himos.len());
        if (himo_id as usize) >= self.himos.len() { return; }
        // #77-H4: op 先行 push (tie_async_by_id と同じ理由)
        // request17 step 4: untie も版数を進める (push 側で採番、 採番 → push は 1 単位)。
        let _mint = self.mint_guard();
        let hlc = self.mint_local_hlc();
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Untie { eid: local, himo_id, hlc });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let rec = enchudb_oplog::oplog::OwnedOp::Untie { eid: oplog_eid, himo_id };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                push_oplog_record_blocking(wq, rec, hlc, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append_at_hlc(rec.as_op(), hlc);
            }
        }
    }

    /// 非同期 delete。
    pub fn delete_async(&self, eid: enchudb_oplog::EntityId) {
        use std::sync::atomic::Ordering;
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        // #77-H4: op 先行 push (tie_async_by_id と同じ理由)
        // request17 step 4 (A-5): delete の版数は tombstone column へ (適用は consumer)。
        let _mint = self.mint_guard();
        let hlc = self.mint_local_hlc();
        let q = match self.write_queue.as_ref() { Some(x) => x, None => return };
        q.push(crate::write_queue::Op::Delete { eid: local, hlc });
        self.push_count.fetch_add(1, Ordering::Release);
        if let Some(wal) = self.oplog.as_ref() {
            let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local);
            let rec = enchudb_oplog::oplog::OwnedOp::Delete { eid: oplog_eid };
            if let Some(wq) = self.oplog_record_queue.as_ref() {
                push_oplog_record_blocking(wq, rec, hlc, &self.consumer_poisoned, &self.wal_push_count);
            } else {
                let _ = wal.append_at_hlc(rec.as_op(), hlc);
            }
        }
    }

    /// 非同期 content 書き込み。WAL 有効時はクラッシュ後も復元される。
    ///
    /// 0.9.0: 保存先を `_c_{key}` Leaf himo に変更 (`content` の doc 参照)。
    /// WAL には `Op::Content` ではなく通常の `Op::Vocab` + `Op::Tie` が乗る。
    pub fn content_async(&self, eid: enchudb_oplog::EntityId, key: &str, data: &[u8]) {
        self.check_writable();
        let local = enchudb_oplog::eid_local(eid);
        let hid = self.ensure_content_himo(local, key);
        self.tie_bytes_async_by_id(eid, hid, data);
    }

    /// 現在のトランザクションを WAL に commit marker で確定する。
    ///
    /// Async モードでは fsync は consumer スレッドが背景で行う。
    /// Sync モードでは commit 完了まで待つ。
    pub fn oplog_commit(&self) {
        if let Some(wal) = self.oplog.as_ref() {
            let _ = wal.append(enchudb_oplog::oplog::Op::Commit);
        }
    }

    /// oplog への参照(テスト / 内部用)。
    pub fn oplog(&self) -> Option<&std::sync::Arc<enchudb_oplog::oplog::OpLog>> { self.oplog.as_ref() }

    /// #116: write queue の slot 数 (観測用)。concurrent mode でなければ None。
    /// oplog_record_queue も同じ capacity で確保される。
    pub fn write_queue_capacity(&self) -> Option<usize> {
        self.write_queue.as_ref().map(|q| q.capacity())
    }

    /// push 済みの全 Op が apply 完了するまで spin 待ち。`tie_async` の同期点。
    /// `queue.is_empty()` は pop 直後 / apply 前のウィンドウで true になる race が
    /// あるため、push_count と apply_count の累積カウンタで apply 完了を待つ。
    pub fn flush_writes(&self) {
        use std::sync::atomic::Ordering;
        if self.write_queue.is_none() { return; }
        loop {
            let pushed = self.push_count.load(Ordering::Acquire);
            let applied = self.apply_count.load(Ordering::Acquire);
            // WAL record queue も barrier に含める: op は queue 先行・record
            // 後追い (#77-H4) なので、 apply_count だけで返ると「op は適用済み
            // だが WAL record は queue 内」の窓が残り、 直後の oplog_sync が
            // record を含まない WAL に Commit + fsync してしまう
            // (= fsync 済みのはずの write が crash で消える durability 破れ)。
            let wal_pushed = self.wal_push_count.load(Ordering::Acquire);
            let wal_appended = self.wal_append_count.load(Ordering::Acquire);
            if applied >= pushed && wal_appended >= wal_pushed { return; }
            // #77-M2: consumer が死んでいたら barrier は永遠に成立しない。
            // silent hang ではなく診断可能な panic で失敗する。
            if self.consumer_poisoned.load(Ordering::Acquire) {
                panic!("enchudb consumer thread has panicked — flush_writes cannot complete (#77-M2)");
            }
            std::thread::yield_now();
        }
    }

    /// キューに滞留中の件数(デバッグ用)。
    pub fn pending_writes(&self) -> usize {
        self.write_queue.as_ref().map(|q| q.len()).unwrap_or(0)
    }

    // ──── flush ────

    #[cfg(not(target_arch = "wasm32"))]
    pub fn flush(&mut self) -> io::Result<()> {
        self.commit();

        // β-light step 7: tables sidecar の最新化 (next_local 含む)。
        // schema 変更 (define_*) でも persist してるが、 entity_in による
        // next_local 増加は persist してないので flush 時にまとめて persist する。
        self.try_persist_tables();

        for ds in &self.himos { ds.sync(); }
        self.vocab.sync();
        self.himo_reg.sync();
        self.contents.sync();

        let maxv_base = himo_maxv_base(self.max_himos);
        let hc = self.value_types.len() as u32;
        let buf = self.backing.header_mut(self.layout.read().unwrap().header_size);
        for hid in 0..self.value_types.len() {
            buf[H_HIMO_TYPES + hid] = self.value_types[hid] as u8;
            let off = maxv_base + hid * 4;
            buf[off..off + 4].copy_from_slice(&self.himo_max_values[hid].to_le_bytes());
        }
        buf[H_HIMO_COUNT..H_HIMO_COUNT + 4].copy_from_slice(&hc.to_le_bytes());
        // ヘッダ整合性 CRC(himo_count 含む固定レイアウト部のみを対象)
        write_header_crc(buf);

        // 全 region を disk に同期した後、 vocab/himo_reg の index 整合性 OK
        // マークを書いてもう一度 msync。 次 open で rebuild_index を skip できる。
        self.sync_and_mark_clean()
    }

    /// #101: 全 region msync → vocab/himo_reg の clean マーク → 再 msync。
    /// mark 前の msync で「flag が指す中身」を先に固める順序が要る (flush() から切り出し)。
    #[cfg(not(target_arch = "wasm32"))]
    fn sync_and_mark_clean(&self) -> io::Result<()> {
        self.backing.flush_to_disk()?;
        self.vocab.mark_index_clean(true);
        self.himo_reg.mark_index_clean(true);
        self.backing.flush_to_disk()?;
        if let Backing::Segments(set) = &self.backing {
            set.write_manifest()?;
        }
        Ok(())
    }

    /// #101: graceful close 用の clean-flush (`&self` 版)。 滞留 write を全 apply して
    /// msync し、 vocab/himo_reg の整合性マーク (clean=1) を永続化する。 次 open は
    /// rebuild_index (O(count)) を skip でき、 readonly open の shadow rebuild も消える。
    ///
    /// `flush()` (&mut) と違い header は書き直さない (himo 定義は define 時に persist 済み)。
    /// readonly open では no-op。 書き込み thread が並行中に呼んだ場合、 mark 後の
    /// insert が flag を 0 に戻すので安全側 (次 open が rebuild) に倒れる。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn flush_clean(&self) -> io::Result<()> {
        if self.is_readonly.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        self.flush_writes();
        self.commit();
        self.try_persist_tables();
        for ds in &self.himos {
            ds.sync();
        }
        self.vocab.sync();
        self.himo_reg.sync();
        self.contents.sync();
        self.sync_and_mark_clean()
    }

    /// #101: 観測用 — vocab index の clean flag。 注意: writer open は #56 の保護で
    /// open 直後に flag を 0 へ戻すので、 session 中に true になるのは
    /// 「flush_clean 後 & 次の vocab insert 前」のみ。 「open が rebuild を skip
    /// できたか」は `vocab_index_rebuilt_on_load()` で見る。
    pub fn vocab_index_is_clean(&self) -> bool {
        self.vocab.index_clean_on_disk()
    }

    /// #101: 観測用 — この Engine の open 時に vocab index の rebuild (O(count)) が
    /// 走ったか。 false = 前回 graceful close の clean flag で skip できた。
    pub fn vocab_index_rebuilt_on_load(&self) -> bool {
        self.vocab.rebuilt_on_load
    }

    /// region CRC を計算して `.crc` sidecar に永続化する。
    /// flush() とは別の opt-in API。512MB+ の vocab 走査を含むので秒オーダー。
    /// コールドバックアップを封緘するユースケース向け。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn seal_integrity(&mut self) -> io::Result<()> {
        self.flush()?;
        self.persist_region_crcs()?;
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        // 既に push 済みの全 Op が apply 完了するまで待機(best-effort)。
        // shutdown flag を立てる前に呼ぶことで、writer がまだ活きてる場合でも
        // ここまでに走った tie_async は consumer が反映済みとなる。
        if self.write_queue.is_some() {
            loop {
                let pushed = self.push_count.load(Ordering::Acquire);
                let applied = self.apply_count.load(Ordering::Acquire);
                if applied >= pushed { break; }
                // #77-M2: consumer 死亡時は待っても進まない。 Drop は panic せず
                // 諦めて shutdown へ進む (未 apply 分は失われるが、 hang しない)。
                if self.consumer_poisoned.load(Ordering::Acquire) { break; }
                std::thread::yield_now();
            }
        }
        if let Some(flag) = &self.shutdown_flag {
            flag.store(true, Ordering::Release);
        }
        // consumer スレッドは shutdown flag を検知したら最終 drain を行って exit する。
        if let Ok(mut h) = self.consumer_handle.lock() {
            if let Some(handle) = h.take() {
                let _ = handle.join();
            }
        }
        // #101: graceful close で clean flag を永続化 → 次 open は rebuild_index skip。
        // panic unwinding 中 / consumer 死亡時は「dirty のまま」に倒す (= 次 open で
        // rebuild が走るのが正しい recovery)。 readonly は共有 mmap を書けないので除外。
        // queue は上の drain + consumer join で空なので barrier (flush_writes) は不要。
        // best-effort: msync の Err は握る (dirty のままでも安全側)。
        #[cfg(not(target_arch = "wasm32"))]
        if !std::thread::panicking()
            && !self.is_readonly.load(Ordering::Acquire)
            && !self.consumer_poisoned.load(Ordering::Acquire)
        {
            self.commit();
            self.try_persist_tables();
            for ds in &self.himos {
                ds.sync();
            }
            self.vocab.sync();
            self.himo_reg.sync();
            self.contents.sync();
            let _ = self.sync_and_mark_clean();
        }
    }
}

// ════════════════ テスト ════════════════

#[cfg(test)]
mod sync_ops_purge_tests {
    use super::*;

    fn tmp_engine(tag: &str) -> Engine {
        let path = format!(
            "{}/enchudb-purge-{}-{}",
            std::env::temp_dir().display(),
            tag,
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        for suf in ["", ".tables", ".oplog"] {
            let _ = std::fs::remove_file(format!("{path}{suf}"));
        }
        let mut eng = Engine::create_with_capacity(&path, 4096).unwrap();
        eng.define_table("t", 100).unwrap();
        eng.enable_sync_tables().unwrap();
        eng
    }

    /// #221 (review 2): free list への producer は purge helper だけではない —
    /// `rebuild_free_locals` (枯渇 slow path) が非 live local を穴として push する。
    /// purge が delete 後 push 前に中断されると同じ slot が両者から入る。
    ///
    /// ここでは逐次順序での不変条件だけを確認する: purge 済み slot に対して rebuild を
    /// 走らせても free list に重複が生まれない (rebuild は非空を見て早期 return)。
    /// **窓そのものは逐次では作れない**ので、 これは回帰検知であって window の
    /// guard ではない (`concurrent_exhaustion_and_purge_do_not_duplicate_slots` の
    /// 注意書きも参照)。
    #[test]
    fn rebuild_does_not_duplicate_purged_slots() {
        let eng = tmp_engine("rebuild");
        let lsn_hid = eng.himo_id("_sync_ops.lsn").unwrap() as u16;
        let tid = eng.tables.iter().position(|t| t.name == "_sync_ops").unwrap();

        let mut eids = Vec::new();
        for i in 0..8u32 {
            let e = eng.entity_in("_sync_ops").unwrap();
            eng.tie_to_by_id(e, lsn_hid, i + 1);
            eids.push((e, i + 1));
        }
        for (e, lsn) in &eids {
            assert!(eng.purge_sync_ops_row(*e, lsn_hid, *lsn));
        }

        // 枯渇 slow path 相当: 非 live local を scan して穴を push する経路。
        eng.rebuild_free_locals(tid);

        let list = eng.tables[tid].free_locals.lock().unwrap().clone();
        let mut uniq = list.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(
            list.len(),
            uniq.len(),
            "#221: free list に slot が二重に入った (purge と rebuild の二重 push): {list:?}"
        );
    }

    /// #221 (review 2): purge と **枯渇 slow path (`rebuild_free_locals`)** を並行
    /// させても free list に slot が二重に入らないこと。
    ///
    /// purge が `free_locals` を delete の前に取らないと、 「delete 済み・push 前」の
    /// 中間状態を rebuild の scan が観測して同じ slot を独立に push する。
    ///
    /// **注意: この test は窓が閉じていることの証明にはならない。** 発火には
    /// 「free list が空 + 枯渇 + purge が delete と push の間」の同時成立が要り、
    /// fix を外した状態でも緑のままだった (3 回試行)。 回帰検知として置いているが、
    /// 正しさの根拠は helper 側の lock 順序 (doc 参照) であってこの test ではない。
    #[test]
    fn concurrent_exhaustion_and_purge_do_not_duplicate_slots() {
        let eng = std::sync::Arc::new(tmp_engine("exhaust"));
        let lsn_hid = eng.himo_id("_sync_ops.lsn").unwrap() as u16;
        let tid = eng.tables.iter().position(|t| t.name == "_sync_ops").unwrap();

        // ring をほぼ埋める (次の alloc が枯渇 → rebuild_free_locals を踏む状態)。
        let range = eng.tables[tid].eid_range_hi - eng.tables[tid].eid_range_lo;
        let fill = range.saturating_sub(4);
        let mut rows = Vec::with_capacity(fill as usize);
        for i in 0..fill {
            let e = eng.entity_in("_sync_ops").unwrap();
            eng.tie_to_by_id(e, lsn_hid, i + 1);
            rows.push((e, i + 1));
        }

        // A: 前半 row を purge (free list に slot を返す)
        // B: 枯渇まで alloc し続ける (= rebuild_free_locals を繰り返し踏む)
        let purger = {
            let eng = eng.clone();
            let rows: Vec<_> = rows.iter().take(rows.len() / 2).copied().collect();
            std::thread::spawn(move || {
                for (e, lsn) in rows {
                    eng.purge_sync_ops_row(e, lsn_hid, lsn);
                }
            })
        };
        let allocator = {
            let eng = eng.clone();
            std::thread::spawn(move || {
                for i in 0..2000u32 {
                    match eng.entity_in("_sync_ops") {
                        Ok(e) => eng.tie_to_by_id(e, lsn_hid, 1_000_000 + i),
                        Err(_) => std::thread::yield_now(),
                    }
                }
            })
        };
        purger.join().unwrap();
        allocator.join().unwrap();

        let list = eng.tables[tid].free_locals.lock().unwrap().clone();
        let mut uniq = list.clone();
        uniq.sort_unstable();
        uniq.dedup();
        assert_eq!(
            list.len(),
            uniq.len(),
            "#221: purge と rebuild_free_locals の並行で slot が二重 push された \
             (free list {} entries, {} unique)",
            list.len(),
            uniq.len()
        );
    }

    /// #221 (review): purge の再検証は「生存」ではなく **`expected_lsn` 一致**で
    /// 行うこと (ABA)。
    ///
    /// 生存だけを見ると、 T1 の purge で解放された slot が bridge に再利用されて
    /// 同一 eid に新 row が乗った後、 T2 が stale snapshot で「生存」と判定して
    /// **その新 row を消す**。 slot は pop 済みなので dedupe も効かない。 この窓は
    /// ring 再利用が常時走る運転 (reclaim + bridge の並行) で開く。
    ///
    /// ここでは interleaving を手で固定して核を検証する:
    /// T1 purge → slot 再利用 (同一 eid に新 lsn) → T2 が stale lsn で purge。
    #[test]
    fn stale_snapshot_does_not_purge_the_reused_slot() {
        let eng = tmp_engine("aba");
        let lsn_hid = eng.himo_id("_sync_ops.lsn").unwrap() as u16;

        // row A: lsn = 5
        let eid = eng.entity_in("_sync_ops").unwrap();
        eng.tie_to_by_id(eid, lsn_hid, 5);
        assert_eq!(eng.get_by_id(eid, lsn_hid), Some(5));

        // T1 相当: snapshot (eid, lsn=5) で purge → slot が free list へ
        assert!(eng.purge_sync_ops_row(eid, lsn_hid, 5), "T1 の purge が成立すること");
        assert_eq!(eng.get_by_id(eid, lsn_hid), None);

        // bridge 相当: 解放 slot を再利用して **同一 eid** に新 row (lsn = 900)
        let reused = eng.entity_in("_sync_ops").unwrap();
        assert_eq!(reused, eid, "前提: 解放 slot が再利用されて同じ eid になる");
        eng.tie_to_by_id(reused, lsn_hid, 900);

        // T2 相当: stale snapshot (lsn=5) のまま purge を試みる。
        // 生存判定だけの実装はここで新 row を消してしまう。
        assert!(
            !eng.purge_sync_ops_row(eid, lsn_hid, 5),
            "#221 ABA: stale snapshot が再利用後の row を purge した"
        );
        assert_eq!(
            eng.get_by_id(eid, lsn_hid),
            Some(900),
            "#221 ABA: bridge されたばかりの row が silent に消えた"
        );

        // 正しい snapshot なら消せる (検証が過剰に厳しくないこと)。
        assert!(eng.purge_sync_ops_row(eid, lsn_hid, 900));
        assert_eq!(eng.get_by_id(eid, lsn_hid), None);
    }
}

#[cfg(test)]
mod reclaimed_floor_tests {
    use super::*;

    fn tmp_engine(tag: &str) -> Engine {
        let path = format!(
            "{}/enchudb-floor-{}-{}",
            std::env::temp_dir().display(),
            tag,
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}.tables"));
        let mut eng = Engine::create_with_capacity(&path, 4096).unwrap();
        eng.define_table("t", 100).unwrap();
        eng.enable_sync_tables().unwrap();
        eng
    }

    fn hlc(wall: u64) -> enchudb_oplog::Hlc {
        enchudb_oplog::Hlc { wall, logical: 0, peer: 0 }
    }

    /// #216 (review): 無帰属 sentinel (`u32::MAX` = legacy scalar 由来) は
    /// v2 entry と共存して**温存**され、 `sync_reclaimed_floors` は sentinel 込みで
    /// Some を返す (None に落とすと legacy floor を持つ既存 DB で per-author 判定が
    /// 一生有効化されない)。 scalar view は全 entry の max。
    #[test]
    fn sentinel_baseline_is_preserved_and_exposed() {
        let eng = tmp_engine("sentinel");
        eng.record_reclaimed_floors(&[(u32::MAX, hlc(100))]);
        eng.record_reclaimed_floors(&[(1, hlc(50)), (2, hlc(200))]);

        let floors = eng.sync_reclaimed_floors().expect("floor 記録済み");
        assert!(
            floors.contains(&(u32::MAX, hlc(100))),
            "sentinel が消えた: {floors:?}"
        );
        assert!(floors.contains(&(1, hlc(50))) && floors.contains(&(2, hlc(200))));
        assert_eq!(eng.sync_reclaimed_floor(), Some(hlc(200)), "scalar view = max");

        // 単調 max merge: 古い値では下がらない
        eng.record_reclaimed_floors(&[(2, hlc(150))]);
        let floors = eng.sync_reclaimed_floors().unwrap();
        assert!(floors.contains(&(2, hlc(200))), "floor が後退した: {floors:?}");
    }
}

#[cfg(test)]
mod layout_v9_tests {
    use super::*;

    fn base(cell_version: bool) -> Layout {
        Layout::compute(1024, 16, 64 * 1024, None, None, None, None, cell_version, None).unwrap()
    }

    /// v9 を有効化していない layout は **pre-v9 と完全に同一**であること。
    /// version column / tombstone column は variable cluster の末尾に置く設計なので、
    /// 無効時は 1 byte も増えてはいけない。
    #[test]
    fn v9_disabled_layout_is_byte_identical() {
        let l = base(false);
        assert_eq!(l.ver_col_size, 0, "無効時に version column を確保している");
        assert_eq!(l.tomb_size, 0, "無効時に tombstone column を確保している");
        assert!(!l.has_cell_version());
    }

    /// v9 を有効化しても **既存 region の offset は 1 つも動かない**こと。
    /// これが崩れると pre-v9 DB を開いた瞬間に全データが別位置を指す。
    #[test]
    fn enabling_v9_does_not_move_existing_regions() {
        let off = base(false);
        let on = base(true);

        assert_eq!(on.entities_off, off.entities_off);
        assert_eq!(on.vocab_data_off, off.vocab_data_off);
        assert_eq!(on.vocab_index_off, off.vocab_index_off);
        assert_eq!(on.himoreg_data_off, off.himoreg_data_off);
        assert_eq!(on.content_index_off, off.content_index_off);
        assert_eq!(on.content_data_off, off.content_data_off);
        assert_eq!(on.leaf_data_off, off.leaf_data_off);
        assert_eq!(on.himo_base_off, off.himo_base_off, "himo region が動いた");
        assert_eq!(on.himo_slot_size, off.himo_slot_size, "himo slot の形が変わった");
        assert!(on.total_size > off.total_size, "v9 領域が確保されていない");
    }

    /// version column は himo ごとに cell 16B (HLC を生で持つ)。 slot が重ならず、
    /// tombstone column も踏まないこと。
    #[test]
    fn version_columns_do_not_overlap_and_leave_room_for_tombstone() {
        let l = base(true);
        assert!(l.has_cell_version());
        assert_eq!(l.ver_col_off(0), l.ver_base_off);

        for hid in 0..15usize {
            let a = l.ver_col_off(hid);
            let b = l.ver_col_off(hid + 1);
            assert_eq!(b - a, l.ver_col_size, "himo {hid} と {} の slot が重なる", hid + 1);
        }

        // 並びは tombstone → version columns (growable の commit が単調なので、
        // 1 本しかない tombstone を手前に置く)。
        assert!(l.tomb_off + l.tomb_size <= l.ver_base_off, "tombstone が version column を踏んでいる");
        let last_end = l.ver_col_off(15) + l.ver_col_size;
        assert!(last_end <= l.total_size, "version column が file 外に出ている");
    }

    /// cell は HLC を生で持つので、 1 himo ぶんの version column は
    /// 値 column (4B/cell) の 4 倍のオーダーになること。 intern 表を採らなかった
    /// 判断 (request17-A) がそのまま layout に出ていることの確認。
    #[test]
    fn version_column_holds_raw_hlc_16b_per_cell() {
        let l = base(true);
        assert_eq!(HLC_CELL_BYTES, 16);
        // himo_col_size は 4B/cell、 ver_col_size は 16B/cell。 header 分を除いて 4 倍。
        let cells = 1024usize;
        assert!(l.ver_col_size >= cells * 16, "version column が 16B/cell 未満");
        assert!(l.ver_col_size < cells * 16 + 64, "version column に余分な確保がある");
        assert_eq!(l.tomb_size, l.ver_col_size, "tombstone column も同じ形 (eid 空間 × 16B)");
    }
}

/// request17 Phase 1 step 2: `set_cell` / `cell_hlc` / `clear_cell` /
/// `set_tombstone` / `tombstone_hlc` の単体。 まだ engine 内の write 経路からは
/// 呼ばれないので、 ここが唯一の caller。
#[cfg(all(test, not(target_arch = "wasm32")))]
mod cell_version_tests {
    use super::*;
    use enchudb_oplog::Hlc;

    fn tmp(name: &str) -> String {
        let path = format!("/tmp/enchu_v9cell_{name}.db");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        path
    }

    fn hlc(wall: u64, peer: u32) -> Hlc {
        Hlc { wall, logical: 0, peer }
    }

    /// request18: header の `H_CELL_VERSION` flag を落として 「v9 化を取りこぼした
    /// DB」 を作る。 `enable_sync_tables` の B-lite が crash / 旧 binary で走らなかった
    /// ケースの再現で、 open 側の回収路 (sidecar-gated auto-migration) を試すのに使う。
    fn clear_cell_version_flag(path: &str) {
        use std::io::{Read, Seek, SeekFrom, Write};
        // v10: header は `{path}/header.seg`
        let hp = format!("{path}/header.seg");
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(hp).unwrap();
        let mut buf = [0u8; HEADER_SIZE];
        f.read_exact(&mut buf).unwrap();
        buf[H_CELL_VERSION..H_CELL_VERSION + 4].copy_from_slice(&0u32.to_le_bytes());
        write_header_crc(&mut buf);
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();
    }

    /// v9 DB + himo 1 本 + entity 1 つ。
    fn v9_engine(name: &str, himo: &str) -> (Engine, u16, enchudb_oplog::EntityId) {
        let mut eng = Engine::create_with_cell_version(&tmp(name), 1024).unwrap();
        eng.define_himo(himo, ValueType::Number, 0);
        let hid = eng.himo_id(himo).unwrap() as u16;
        let eid = eng.entity().unwrap();
        (eng, hid, eid)
    }

    #[test]
    fn v9_db_has_version_region_and_pre_v9_does_not() {
        let (eng, hid, eid) = v9_engine("has_region", "age");
        assert!(eng.has_cell_version(), "v9 create が version 領域を確保していない");
        assert_eq!(eng.cell_hlc(eid, hid), Hlc::ZERO, "未書き込み cell は版数不明");

        let mut old = Engine::create_without_cell_version(&tmp("no_region"), 1024).unwrap();
        old.define_himo("age", ValueType::Number, 0);
        assert!(!old.has_cell_version(), "pre-v9 create が v9 領域を確保している");
    }

    #[test]
    fn set_cell_writes_value_and_version_together() {
        let (eng, hid, eid) = v9_engine("write", "age");
        assert!(eng.set_cell(eid, hid, 42, hlc(100, 1)));
        assert_eq!(eng.get(eid, "age"), Some(42));
        assert_eq!(eng.cell_hlc(eid, hid), hlc(100, 1), "値は入ったが版数が残っていない");
        assert_eq!(eng.pull("age", 42), vec![enchudb_oplog::eid_local(eid)]);
    }

    /// A-2 の核: 古い HLC は **値も版数も cylinder も** 触らずに落とす。
    /// 同値 HLC も不採用 (`cur >= hlc`)。
    #[test]
    fn older_hlc_is_rejected_without_touching_the_cell() {
        let (eng, hid, eid) = v9_engine("reject_old", "age");
        assert!(eng.set_cell(eid, hid, 42, hlc(200, 1)));

        assert!(!eng.set_cell(eid, hid, 7, hlc(100, 1)), "古い HLC を採用した");
        assert_eq!(eng.get(eid, "age"), Some(42), "不採用なのに値が変わった");
        assert_eq!(eng.cell_hlc(eid, hid), hlc(200, 1), "不採用なのに版数が変わった");
        assert!(eng.pull("age", 7).is_empty(), "不採用なのに cylinder に載った");

        assert!(!eng.set_cell(eid, hid, 8, hlc(200, 1)), "同値 HLC を採用した");
        assert_eq!(eng.get(eid, "age"), Some(42));
    }

    #[test]
    fn newer_hlc_wins() {
        let (eng, hid, eid) = v9_engine("newer", "age");
        assert!(eng.set_cell(eid, hid, 42, hlc(100, 1)));
        assert!(eng.set_cell(eid, hid, 43, hlc(101, 1)));
        assert_eq!(eng.get(eid, "age"), Some(43));
        assert_eq!(eng.cell_hlc(eid, hid), hlc(101, 1));
        // wall 同値なら peer が tiebreak (Hlc の全順序)
        assert!(eng.set_cell(eid, hid, 44, hlc(101, 2)));
        assert_eq!(eng.get(eid, "age"), Some(44));
    }

    /// A-1: 版数不明 (`Hlc::ZERO`) の cell は従来どおり無条件に上書きされる。
    /// ここを塞ぐと既存 DB が何も同期できなくなる (#161 の再来)。
    #[test]
    fn unknown_version_cell_accepts_any_write() {
        let (eng, hid, eid) = v9_engine("unknown_cur", "age");
        // 版数を書かない従来経路
        eng.tie_to_by_id(eid, hid, 5);
        assert_eq!(eng.cell_hlc(eid, hid), Hlc::ZERO, "前提: 旧経路は版数を書かない");

        assert!(eng.set_cell(eid, hid, 9, hlc(50, 1)), "版数不明 cell への write を止めた");
        assert_eq!(eng.get(eid, "age"), Some(9));
        assert_eq!(eng.cell_hlc(eid, hid), hlc(50, 1), "以後は版数が付く");
    }

    /// A-3: oplog 無効 (standalone) の write は HLC を採番できず `ZERO` で来る。
    /// 値は従来どおり通し、 既に載っている版数は消さない。
    #[test]
    fn zero_hlc_write_applies_and_keeps_recorded_version() {
        let (eng, hid, eid) = v9_engine("zero_incoming", "age");
        assert!(eng.set_cell(eid, hid, 42, hlc(200, 1)));

        assert!(eng.set_cell(eid, hid, 43, Hlc::ZERO), "版数不明の write を止めた");
        assert_eq!(eng.get(eid, "age"), Some(43));
        assert_eq!(eng.cell_hlc(eid, hid), hlc(200, 1), "ZERO 書き込みが既存の版数を消した");
    }

    /// v10 (request21): `enable_sync_tables()` は版数列 segment を **その場で** 生やす。
    /// 旧 B-lite の 「column は次の open から」 という窓 (#243) は無い。 有効化した
    /// セッションで書いた版数は column に載り、 reopen 後も残る。
    #[test]
    fn enable_sync_tables_grows_version_columns_immediately() {
        let path = tmp("enable_now");
        let (eid, hid) = {
            let mut eng = Engine::create_without_cell_version(&path, 1024).unwrap();
            eng.define_himo("age", ValueType::Number, 0);
            let hid = eng.himo_id("age").unwrap() as u16;
            let eid = eng.entity().unwrap();
            assert!(!eng.has_cell_version(), "前提が崩れた: create 直後は版数列無し");
            // sync tables を足すと anonymous table が closed になるので entity() の後で呼ぶ。
            eng.enable_sync_tables().unwrap();
            assert!(eng.has_cell_version(), "enable_sync_tables が版数列をその場で生やしていない");
            assert!(eng.set_cell(eid, hid, 1, hlc(200, 1)));
            assert_eq!(eng.cell_hlc(eid, hid), hlc(200, 1), "版数が column に載っていない");
            assert!(!eng.set_cell(eid, hid, 2, hlc(100, 1)), "古い HLC を採用した");
            assert!(eng.set_tombstone(eid, hlc(400, 1)));
            assert!(!eng.set_tombstone(eid, hlc(350, 1)), "古い delete を採用した");
            assert_eq!(eng.tombstone_hlc(eid), hlc(400, 1));
            eng.flush().unwrap();
            (eid, hid)
        };
        let eng = Engine::open_standalone(&path).unwrap();
        assert!(eng.has_cell_version());
        assert_eq!(eng.cell_hlc(eid, hid), hlc(200, 1), "有効化セッションの版数が reopen で消えた (#243)");
        assert_eq!(eng.tombstone_hlc(eid), hlc(400, 1));
    }

    /// 版数列を持たない非 sync DB は版数を記帳しない (request18)。 判定材料が無いので
    /// 「版数不明 = 受け入れる」 (A-1) に倒れる。
    #[test]
    fn no_version_column_means_writes_are_always_accepted() {
        let mut eng = Engine::create_without_cell_version(&tmp("prev9_write"), 1024).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;
        let eid = eng.entity().unwrap();
        assert!(eng.set_cell(eid, hid, 1, hlc(200, 1)));
        assert!(eng.set_cell(eid, hid, 2, hlc(100, 1)), "版数列の無い非 sync DB で古い HLC が弾かれた (A-1 違反)");
        assert_eq!(eng.get(eid, "age"), Some(2));
        assert_eq!(eng.cell_hlc(eid, hid), Hlc::ZERO);
    }

    /// untie も版数を進める。 これが無いと外した cell に古い tie が蘇る。
    #[test]
    fn clear_cell_is_versioned() {
        let (eng, hid, eid) = v9_engine("clear", "age");
        assert!(eng.set_cell(eid, hid, 42, hlc(100, 1)));

        assert!(!eng.clear_cell(eid, hid, hlc(50, 1)), "古い untie を採用した");
        assert_eq!(eng.get(eid, "age"), Some(42));

        assert!(eng.clear_cell(eid, hid, hlc(200, 1)));
        assert_eq!(eng.get(eid, "age"), None);
        assert_eq!(eng.cell_hlc(eid, hid), hlc(200, 1));

        assert!(!eng.set_cell(eid, hid, 42, hlc(150, 1)), "untie より古い tie が蘇った");
        assert_eq!(eng.get(eid, "age"), None);
    }

    /// A-5: tombstone は eid 空間の 1 本。 himo の版数とは別空間で、 LWW も別。
    #[test]
    fn tombstone_column_is_lww_and_separate_from_himo_versions() {
        let (eng, hid, eid) = v9_engine("tomb", "age");
        assert_eq!(eng.tombstone_hlc(eid), Hlc::ZERO);

        assert!(eng.set_tombstone(eid, hlc(100, 1)));
        assert_eq!(eng.tombstone_hlc(eid), hlc(100, 1));
        assert!(!eng.set_tombstone(eid, hlc(50, 1)), "古い delete を採用した");
        assert_eq!(eng.tombstone_hlc(eid), hlc(100, 1));
        assert!(eng.set_tombstone(eid, hlc(300, 1)));
        assert_eq!(eng.tombstone_hlc(eid), hlc(300, 1));

        assert_eq!(eng.cell_hlc(eid, hid), Hlc::ZERO, "tombstone が himo の版数を踏んだ");
    }

    // ──── slot 再利用: 前の住人の版数を持ち越さない ────

    /// version / tombstone column は local slot で index されるので、 free list
    /// から再利用された slot には前の住人の版数が残る。 落とさずに渡すと、 新しい
    /// 住人への write が「前の住人の削除より古い」と判定されて **無言で落ちる**。
    ///
    /// 実害が出るのは bootstrap / `Hlc::ZERO` からの pull のように **古い record を
    /// まとめて再生する**局面: 相手が t1 に author した record が、 こちらが
    /// t2 (> t1) に消した無関係な entity の tombstone に負けて適用されない。
    ///
    /// falsify: `entity_in` の再利用枝から `clear_cell_versions` を外すと
    /// 最後の 3 assert が落ちる。
    #[test]
    fn reused_table_slot_does_not_inherit_previous_tenant_version() {
        let mut eng = Engine::create_with_cell_version(&tmp("slot_reuse_table"), 1024).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;
        // slot 1 個だけの table → 2 個目の entity_in は必ず再利用に落ちる
        eng.define_table("t", 1).unwrap();

        let first = eng.entity_in("t").unwrap();
        assert!(eng.set_cell(first, hid, 42, hlc(300, 1)));
        assert!(eng.set_tombstone(first, hlc(300, 1)));
        eng.delete(first); // slot が free list に戻る

        let second = eng.entity_in("t").unwrap();
        assert_eq!(
            enchudb_oplog::eid_local(second),
            enchudb_oplog::eid_local(first),
            "前提が崩れた: slot が再利用されていない",
        );

        // 前の住人の版数 (300) より古い remote record 相当 = 実害が出る形
        assert!(
            eng.set_cell(second, hid, 7, hlc(200, 1)),
            "前の住人の版数が新しい住人への write を無言で落とした",
        );
        assert_eq!(eng.get(second, "age"), Some(7));
        assert_eq!(eng.cell_hlc(second, hid), hlc(200, 1));
        assert_eq!(eng.tombstone_hlc(second), Hlc::ZERO, "前の住人の tombstone を引き継いだ");
    }

    /// anonymous table 側 (`entity()` → `EntitySet` free stack) の同じ穴。
    /// こちらは eid 空間を使い切った時だけ再利用に落ちる。
    #[test]
    fn reused_entity_set_slot_does_not_inherit_previous_tenant_version() {
        let mut eng = Engine::create_with_cell_version(&tmp("slot_reuse_anon"), 2).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;

        let a = eng.entity().unwrap();
        let b = eng.entity().unwrap(); // ここで max_entities (2) を使い切る
        assert!(eng.set_cell(b, hid, 42, hlc(300, 1)));
        assert!(eng.set_tombstone(b, hlc(300, 1)));
        eng.delete(b);

        let reused = eng.entity().unwrap(); // free stack から b の slot を再利用
        assert_ne!(enchudb_oplog::eid_local(reused), enchudb_oplog::eid_local(a));
        assert_eq!(
            enchudb_oplog::eid_local(reused),
            enchudb_oplog::eid_local(b),
            "前提が崩れた: free stack から再利用されていない",
        );

        assert!(
            eng.set_cell(reused, hid, 7, hlc(200, 1)),
            "前の住人の版数が新しい住人への write を無言で落とした",
        );
        assert_eq!(eng.get(reused, "age"), Some(7));
        assert_eq!(eng.tombstone_hlc(reused), Hlc::ZERO, "前の住人の tombstone を引き継いだ");
    }

    /// **pre-v9 DB でも同じ穴が開く。** 版数の置き場が version column ではなく揮発
    /// `HlcStore` になるだけで、 キーは `version_key(local)` = local slot なので
    /// 事情は v9 と変わらない。 しかも `HlcStore` のエントリは production code から
    /// 一度も消されないので、 落とさなければ永久に残る。
    ///
    /// falsify: `clear_cell_versions` の pre-v9 枝を消すと最後の 3 assert が落ちる。
    #[test]
    fn reused_slot_does_not_inherit_previous_tenant_version_on_pre_v9() {
        let mut eng = Engine::create_without_cell_version(&tmp("slot_reuse_prev9"), 1024).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;
        eng.define_table("t", 1).unwrap(); // slot 1 個 → 2 個目は必ず再利用
        assert!(!eng.has_cell_version(), "前提が崩れた: pre-v9 DB でない");

        let first = eng.entity_in("t").unwrap();
        assert!(eng.set_cell(first, hid, 42, hlc(300, 1)));
        assert!(eng.set_tombstone(first, hlc(300, 1)));
        eng.delete(first);

        let second = eng.entity_in("t").unwrap();
        assert_eq!(
            enchudb_oplog::eid_local(second),
            enchudb_oplog::eid_local(first),
            "前提が崩れた: slot が再利用されていない",
        );

        assert!(
            eng.set_cell(second, hid, 7, hlc(200, 1)),
            "前の住人の版数が新しい住人への write を無言で落とした (pre-v9)",
        );
        assert_eq!(eng.get(second, "age"), Some(7));
        assert_eq!(eng.tombstone_hlc(second), Hlc::ZERO, "前の住人の tombstone を引き継いだ");
    }

    // ──── step 4: ローカル write が版数を書く ────

    /// v9 DB + oplog (WAL) + consumer。 async 経路まで通した実構成。
    fn v9_with_wal(name: &str, himo: &str) -> (std::sync::Arc<Engine>, u16) {
        let path = tmp(name);
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let mut eng = Engine::create_with_cell_version(&path, 1024).unwrap();
        eng.define_himo(himo, ValueType::Number, 0);
        let hid = eng.himo_id(himo).unwrap() as u16;
        let arc = Engine::concurrentize_with_oplog(eng, 1024 * 1024).unwrap();
        (arc, hid)
    }

    /// WAL 上の record を (op 種別を問わず) HLC 付きで拾う。
    fn wal_hlcs(eng: &Engine) -> Vec<(enchudb_oplog::oplog::DecodedOp, Hlc)> {
        let wal = eng.oplog().expect("wal");
        wal.append(enchudb_oplog::oplog::Op::Commit).unwrap();
        wal.iter_committed().into_iter().map(|r| (r.op, r.hlc)).collect()
    }

    /// 同期 write が **版数を記録し、 その版数が WAL record の HLC と一致する**こと。
    /// 一致していないと、 自分が持つ版数と peer に配る版数が食い違う。
    #[test]
    fn sync_local_tie_records_the_same_hlc_as_the_record() {
        let (eng, hid) = v9_with_wal("sync_tie", "age");
        let eid = eng.entity().unwrap();
        eng.tie_to(eid, "age", 42);
        eng.flush_writes();

        let cell = eng.cell_hlc(eid, hid);
        assert_ne!(cell, Hlc::ZERO, "ローカル write が版数を書いていない");

        let ties: Vec<Hlc> = wal_hlcs(&eng)
            .into_iter()
            .filter(|(op, _)| matches!(op, enchudb_oplog::oplog::DecodedOp::Tie { .. }))
            .map(|(_, h)| h)
            .collect();
        assert_eq!(ties, vec![cell], "cell の版数と WAL record の HLC が違う");
    }

    /// untie も版数を進める (= 外した cell に古い tie が蘇らない)。
    #[test]
    fn sync_local_untie_advances_the_version() {
        let (eng, hid) = v9_with_wal("sync_untie", "age");
        let eid = eng.entity().unwrap();
        eng.tie_to(eid, "age", 42);
        let after_tie = eng.cell_hlc(eid, hid);
        eng.untie(eid, "age");
        let after_untie = eng.cell_hlc(eid, hid);

        assert!(after_untie > after_tie, "untie が版数を進めていない");
        assert_eq!(eng.get(eid, "age"), None);
        // untie より古い tie は蘇らない
        assert!(!eng.set_cell(eid, hid, 42, after_tie));
        assert_eq!(eng.get(eid, "age"), None);
    }

    /// delete が tombstone 版数を残す (A-5)。
    #[test]
    fn sync_local_delete_records_tombstone() {
        let (eng, _hid) = v9_with_wal("sync_delete", "age");
        let eid = eng.entity().unwrap();
        eng.tie_to(eid, "age", 42);
        assert_eq!(eng.tombstone_hlc(eid), Hlc::ZERO);

        eng.delete(eid);
        let tomb = eng.tombstone_hlc(eid);
        assert_ne!(tomb, Hlc::ZERO, "delete が tombstone 版数を残していない");

        let deletes: Vec<Hlc> = wal_hlcs(&eng)
            .into_iter()
            .filter(|(op, _)| matches!(op, enchudb_oplog::oplog::DecodedOp::Delete { .. }))
            .map(|(_, h)| h)
            .collect();
        assert_eq!(deletes, vec![tomb], "tombstone 版数と Delete record の HLC が違う");
    }

    /// **async 経路** (適用と WAL append が別 queue / 別タイミング) でも、 cell の版数と
    /// record の HLC が一致すること。 ここがずれると peer 間で勝者が分かれる。
    #[test]
    fn async_write_puts_the_same_hlc_on_cell_and_record() {
        let (eng, hid) = v9_with_wal("async_tie", "age");
        let eid = eng.entity().unwrap();
        eng.tie_async(eid, "age", 7);
        eng.tie_async(eid, "age", 8);
        eng.flush_writes();

        assert_eq!(eng.get(eid, "age"), Some(8));
        let cell = eng.cell_hlc(eid, hid);
        assert_ne!(cell, Hlc::ZERO, "async write が版数を書いていない");

        let ties: Vec<Hlc> = wal_hlcs(&eng)
            .into_iter()
            .filter(|(op, _)| matches!(op, enchudb_oplog::oplog::DecodedOp::Tie { .. }))
            .map(|(_, h)| h)
            .collect();
        assert_eq!(ties.len(), 2, "record が 2 本乗っていない");
        assert!(ties[0] < ties[1], "record の HLC が単調増加していない");
        assert_eq!(
            ties[1], cell,
            "最後に適用された cell の版数が、 その write の record の HLC と違う",
        );
    }

    /// async untie / delete も同様に版数を進める。
    #[test]
    fn async_untie_and_delete_are_versioned() {
        let (eng, hid) = v9_with_wal("async_untie", "age");
        let eid = eng.entity().unwrap();
        eng.tie_async(eid, "age", 7);
        eng.flush_writes();
        let after_tie = eng.cell_hlc(eid, hid);

        eng.untie_async(eid, "age");
        eng.flush_writes();
        assert!(eng.cell_hlc(eid, hid) > after_tie, "async untie が版数を進めていない");
        assert_eq!(eng.get(eid, "age"), None);

        let eid2 = eng.entity().unwrap();
        eng.tie_async(eid2, "age", 9);
        eng.delete_async(eid2);
        eng.flush_writes();
        assert_ne!(eng.tombstone_hlc(eid2), Hlc::ZERO, "async delete が tombstone を残していない");
    }

    // ──── step 7: v9 を既定にする (版数が reopen を跨いで残る) ────

    /// **版数が reopen を跨いで残る** — request17 全体の目的そのもの。
    ///
    /// 従来 LWW の真実は揮発 HashMap にしか無く、 プロセスが落ちるか配送バッファが
    /// reclaim されると失われた (#140 / #154 / #160 の共通の根)。 v9 では cell と
    /// 一緒に永続するので、 reopen 後も古い record に負けない。
    #[test]
    fn versions_survive_reopen() {
        let path = tmp("reopen");
        let (eid, other, hid) = {
            let mut eng = Engine::create_with_cell_version(&path, 1024).unwrap();
            eng.define_himo("age", ValueType::Number, 0);
            let hid = eng.himo_id("age").unwrap() as u16;
            let eid = eng.entity().unwrap();
            let other = eng.entity().unwrap();
            assert!(eng.has_cell_version(), "通常 create が v9 領域を確保していない");
            assert!(eng.set_cell(eid, hid, 42, hlc(500, 7)));
            assert!(eng.set_tombstone(other, hlc(600, 7)));
            eng.flush().unwrap();
            (eid, other, hid)
        };

        let eng = Engine::open_standalone(&path).unwrap();
        assert!(eng.has_cell_version(), "reopen で v9 領域を見失った");
        assert_eq!(eng.get(eid, "age"), Some(42), "値が消えた");
        assert_eq!(eng.cell_hlc(eid, hid), hlc(500, 7), "cell の版数が reopen で消えた");
        assert_eq!(eng.tombstone_hlc(other), hlc(600, 7), "tombstone が reopen で消えた");

        // 記憶が残っているので、 reopen 後も古い record は負ける
        assert!(
            !eng.remote_tie_apply(eid, hid, 9, hlc(400, 7)),
            "reopen 後に古い record が勝った (版数が永続していない)",
        );
        assert_eq!(eng.get(eid, "age"), Some(42));
    }

    /// **growable backing で v9 領域を書いても SIGBUS しない**。
    ///
    /// growable の commit は初期値が `vocab_data_off` (= 固定 cluster まで) で、
    /// v9 領域は variable cluster の末尾 = その外側にある。 触る前に commit を
    /// 伸ばさないと未コミット page への read/write で **プロセスが即死** する
    /// (step 7 で実際に踏んだ: bench が exit 138 = SIGBUS)。
    ///
    /// 既存の growable テストが素通ししたのは、 oplog 無しでは版数が `ZERO` で
    /// version column を一度も触らないため。 oplog 付きで書くこと自体が条件。
    #[test]
    fn growable_backing_writes_versions_without_sigbus() {
        let path = tmp("growable_v9");
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let mut eng = Engine::create_growable_with_cell_version(&path, 100_000).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;
        let eng = Engine::concurrentize_with_oplog(eng, 1024 * 1024).unwrap();
        assert!(eng.has_cell_version(), "growable create が v9 領域を確保していない");

        // 末尾寄りの eid ほど commit の外 — 手前だけ触って満足しないように広く書く。
        let mut eids = Vec::new();
        for i in 0..64u32 {
            let e = eng.entity().unwrap();
            eng.tie_to(e, "age", i);
            eids.push(e);
        }
        eng.tie_async(eids[0], "age", 999);
        eng.delete(eids[1]);
        eng.flush_writes();

        assert_ne!(eng.cell_hlc(eids[0], hid), Hlc::ZERO, "版数が記録されていない");
        assert_ne!(eng.tombstone_hlc(eids[1]), Hlc::ZERO, "tombstone が記録されていない");
        assert_eq!(eng.get(eids[0], "age"), Some(999));
    }

    /// pre-v9 の **sync DB** を writer open すると v9 領域が生える (自動 migration)。
    /// 確認するのは (a) 開ける (b) 既存データが無傷 (c) 既存 cell は版数不明のままなので
    /// 従来どおり remote record を受け入れる。
    ///
    /// **仕様変更の記録 (その 1、 0.20.0)**: 0.19.0 までは 「開いても migrate しない」 が
    /// 方針だった。 だが版数を持たない DB は #154 / #160 の穴を抱えたままで anti-entropy
    /// (Phase 2) も効かず、 「新機能の恩恵を受けられない DB が永久に残る」 のは DB として
    /// 筋が悪い。 v9 領域は layout 末尾で手前を 1 byte も動かさないため移行が in-place で
    /// 済むので、 自動化する判断に変えた。
    ///
    /// **仕様変更の記録 (その 2、 request18 / #173)**: 0.20.0 は **無条件に** v9 化して
    /// いたため、 sync しない DB まで apparent ×3.6 (既定 capacity で 26.5 GB → 95.5 GB) を
    /// 払っていた。 版数・tombstone は remote record の LWW 判定にしか使わず、 それは
    /// `Syncer` 経由でしか起きない (`Syncer::new` が `sync_tables_enabled()` を必須
    /// チェックする) ので、 **sync tables を持つ DB だけ**を対象に絞った。
    ///
    /// A-1 (「版数不明 = 現状維持」) は **cell の粒度では維持されている** — 移行しても
    /// 既存 cell の版数は ZERO のままで、 古い record を弾いたりしない。 変わったのは
    /// 「領域を生やすかどうか」 だけ。
    #[test]
    fn pre_v9_sync_db_opens_and_migrates_without_touching_the_data() {
        let path = tmp("prev9_reopen");
        let (eid, hid) = {
            let mut eng = Engine::create_without_cell_version(&path, 1024).unwrap();
            eng.define_himo("age", ValueType::Number, 0);
            let hid = eng.himo_id("age").unwrap() as u16;
            let eid = eng.entity().unwrap();
            eng.tie_to(eid, "age", 7);
            // sync DB にする。 `enable_sync_tables` 自身も file を伸ばして header flag を
            // 立てる (B-lite) が、 ここで確かめたいのは **open 側の回収路**なので
            // 意図的に flag を落として 「B-lite を取りこぼした DB」 を作る。
            eng.enable_sync_tables().unwrap();
            eng.flush().unwrap();
            (eid, hid)
        };
        clear_cell_version_flag(&path);

        let eng = Engine::open_standalone(&path).unwrap();
        assert!(eng.has_cell_version(), "sync DB の writer open で v9 領域が生えていない");
        assert_eq!(eng.get(eid, "age"), Some(7), "移行で既存データが壊れた");
        assert_eq!(
            eng.cell_hlc(eid, hid),
            Hlc::ZERO,
            "移行しただけの cell に版数が付いた (A-1: 版数不明のままであるべき)",
        );
        // 版数不明なので remote record は従来どおり適用される
        assert!(
            eng.remote_tie_apply(eid, hid, 9, hlc(400, 7)),
            "版数不明 cell への remote apply を止めた (既存 DB が同期不能になる)",
        );
        assert_eq!(eng.get(eid, "age"), Some(9));
    }

    /// request18 の主眼: **sync しない DB は writer open しても v9 化されない**。
    /// 0.20.0 はここで無条件に領域を生やしていた。
    #[test]
    fn plain_db_is_not_migrated_on_open() {
        let path = tmp("plain_no_migrate");
        {
            let mut eng = Engine::create_without_cell_version(&path, 1024).unwrap();
            eng.define_himo("age", ValueType::Number, 0);
            let eid = eng.entity().unwrap();
            eng.tie_to(eid, "age", 7);
            eng.flush().unwrap();
        }
        let before = std::fs::metadata(&path).unwrap().len();
        let eng = Engine::open_standalone(&path).unwrap();
        assert!(!eng.has_cell_version(), "sync しない DB が v9 化された");
        drop(eng);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            before,
            "sync しない DB の open でファイルが伸びた",
        );
    }

    /// request18: `accepts_write` の短絡が、 **版数を持っている非 sync DB** まで
    /// 飲み込まないこと。
    ///
    /// 0.19.0 / 0.20.0 は sync の有無に関わらず v9 領域を確保していたので、
    /// 「v9 領域があり、 版数も載っているが sync tables は無い」 DB が現に存在する。
    /// 短絡条件を `!sync_tables_on()` だけにするとその版数を無視してしまい、
    /// 古い record を通す = 静かな巻き戻りになる。
    #[test]
    fn v9_db_without_sync_tables_still_honors_recorded_versions() {
        let (eng, hid, eid) = v9_engine("v9_nosync", "age");
        assert!(!eng.sync_tables_enabled(), "前提が崩れた: sync tables 無しのはず");
        assert!(eng.has_cell_version(), "前提が崩れた: v9 領域ありのはず");

        assert!(eng.set_cell(eid, hid, 42, hlc(200, 1)));
        assert_eq!(eng.cell_hlc(eid, hid), hlc(200, 1), "版数が載っていない");
        assert!(
            !eng.set_cell(eid, hid, 9, hlc(100, 1)),
            "版数を持つ非 sync DB で古い write が通った (accepts_write の短絡が広すぎる)",
        );
        assert_eq!(eng.get(eid, "age"), Some(42));
    }

    /// request18: sync しない DB は **揮発 `HlcStore` にも記帳しない**。
    /// 記帳しても読む相手が居らず、 上限の無い HashMap が漏れるだけ。
    #[test]
    fn no_sync_db_does_not_record_versions_at_all() {
        let mut eng = Engine::create_without_cell_version(&tmp("nosync_nostore"), 1024).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;
        let eid = eng.entity().unwrap();

        assert!(eng.set_cell(eid, hid, 1, hlc(200, 1)));
        assert!(eng.set_tombstone(eid, hlc(400, 1)));
        assert_eq!(eng.hlc_store().len(), 0, "sync しない DB が版数を記帳した");
        // 記帳しない = 判定材料が無い = 従来どおり全部通る (A-1 の現状維持)
        assert!(eng.set_cell(eid, hid, 2, hlc(100, 1)));
        assert_eq!(eng.get(eid, "age"), Some(2));
    }

    // ──── step 5: remote apply も同じ 1 本を通る ────

    /// tombstone より古い remote Tie は **適用されない** (A-5)。 これが永続する
    /// ことで、 配送バッファから tombstone が消えた後も削除済み entity が
    /// 復活しない (#140 の根)。
    #[test]
    fn tombstone_blocks_older_remote_tie() {
        let (eng, hid) = v9_with_wal("tomb_remote", "age");
        let eid = eng.entity().unwrap();

        assert!(eng.remote_delete_apply(eid, hlc(1000, 1)));
        assert!(
            !eng.remote_tie_apply(eid, hid, 5, hlc(900, 1)),
            "削除より古い Tie が蘇った",
        );
        assert_eq!(eng.get(eid, "age"), None);
        assert!(
            eng.remote_tie_apply(eid, hid, 6, hlc(1100, 1)),
            "削除より新しい Tie が入らない",
        );
        assert_eq!(eng.get(eid, "age"), Some(6));
    }

    /// remote apply の LWW も engine の 1 本で判定されること。
    #[test]
    fn remote_apply_is_lww_without_any_sync_layer_help() {
        let (eng, hid) = v9_with_wal("remote_lww", "age");
        let eid = eng.entity().unwrap();

        assert!(eng.remote_tie_apply(eid, hid, 1, hlc(200, 1)));
        assert!(!eng.remote_tie_apply(eid, hid, 2, hlc(100, 1)), "古い record を適用した");
        assert_eq!(eng.get(eid, "age"), Some(1));
        assert!(eng.remote_tie_apply(eid, hid, 3, hlc(300, 1)));
        assert_eq!(eng.get(eid, "age"), Some(3));
        assert_eq!(eng.cell_hlc(eid, hid), hlc(300, 1), "remote の版数が cell に残っていない");
        // untie も同じ判定
        assert!(!eng.remote_untie_apply(eid, hid, hlc(250, 1)));
        assert!(eng.remote_untie_apply(eid, hid, hlc(400, 1)));
        assert_eq!(eng.get(eid, "age"), None);
    }

    /// **受信 HLC でローカル clock を merge する** (step 5)。
    ///
    /// 相手の wall clock が先行していると、 merge しない限り自分の次の HLC は
    /// 相手の版数より小さいままで、 版数を storage に置いた途端に
    /// **自分のローカル write が自分の DB で負ける**。 #161 と同じ「止めた先に
    /// 脱出路が無い」形なので、 構造で塞ぐ。
    #[test]
    fn local_write_still_wins_after_a_far_future_remote_hlc() {
        let (eng, hid) = v9_with_wal("clock_merge", "age");
        let eid = eng.entity().unwrap();

        // 10 年先の clock を持つ peer からの record
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let far_future = Hlc { wall: now_ms + 10 * 365 * 24 * 3600 * 1000, logical: 0, peer: 9 };
        assert!(eng.remote_tie_apply(eid, hid, 7, far_future));
        assert_eq!(eng.get(eid, "age"), Some(7));

        // 直後のローカル write が負けてはいけない
        eng.tie_to(eid, "age", 42);
        assert_eq!(
            eng.get(eid, "age"), Some(42),
            "未来 HLC を受けた後にローカル write が負けた (clock を merge していない)",
        );
        assert!(eng.cell_hlc(eid, hid) > far_future, "ローカル版数が受信 HLC を追い越していない");
    }

    /// **WAL 上の HLC は record の並び順どおり単調増加**すること。
    ///
    /// transport は record を HLC 順に並べ替えて配る (`InMemoryTransport::pull_as`)。
    /// 崩れると「Vocab → その vid を使う Tie」の依存順が受信側で逆転し、 vid 翻訳が
    /// 生の remote vid に fallback して無関係な row へ誤 bind する (#141 の再来)。
    /// step 4 で採番を engine 側に寄せたとき、 一度これを壊して #141 の回帰テストが
    /// 落ちた — その再発防止。
    #[test]
    fn wal_hlcs_are_monotonic_in_record_order() {
        let path = tmp("wal_monotonic");
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let mut eng = Engine::create_with_cell_version(&path, 1024).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        eng.define_himo("name", ValueType::Tag, 0);
        let eng = Engine::concurrentize_with_oplog(eng, 1024 * 1024).unwrap();

        let e1 = eng.entity().unwrap();
        let e2 = eng.entity().unwrap();
        // 同期経路: text tie は Vocab + Tie の 2 record を出す
        eng.tie_text_to(e1, "name", "alice");
        eng.tie_to(e1, "age", 30);
        eng.tie_text_to(e2, "name", "bob");
        eng.untie(e1, "age");
        // async 経路も混ぜる
        eng.tie_async(e2, "age", 20);
        eng.tie_text_async(e2, "name", "bobby");
        eng.delete_async(e1);
        eng.flush_writes();

        let recs = wal_hlcs(&eng);
        assert!(recs.len() >= 8, "record が足りない ({} 本)", recs.len());
        for w in recs.windows(2) {
            assert!(
                w[0].1 < w[1].1,
                "WAL 上で HLC が並び順どおりでない: {:?}({:?}) → {:?}({:?})",
                w[0].0, w[0].1, w[1].0, w[1].1,
            );
        }
    }

    /// pre-v9 DB (= 通常 create) のローカル write は **何も変わらない**。
    /// step 7 まで既定はこちらなので、 これが step 4 の「挙動不変」の担保。
    #[test]
    fn pre_v9_local_writes_are_unchanged() {
        let path = tmp("prev9_local");
        let _ = std::fs::remove_file(format!("{path}.wal"));
        let mut eng = Engine::create_without_cell_version(&path, 1024).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;
        let eng = Engine::concurrentize_with_oplog(eng, 1024 * 1024).unwrap();

        let eid = eng.entity().unwrap();
        eng.tie_to(eid, "age", 1);
        eng.tie_async(eid, "age", 2);
        eng.flush_writes();
        assert_eq!(eng.get(eid, "age"), Some(2));
        assert_eq!(eng.cell_hlc(eid, hid), Hlc::ZERO, "v9 領域が無いのに版数が読めている");

        eng.untie(eid, "age");
        assert_eq!(eng.get(eid, "age"), None);
        eng.delete(eid);
        assert_eq!(eng.tombstone_hlc(eid), Hlc::ZERO);
        // record 自体は従来どおり出ている
        let n = wal_hlcs(&eng).len();
        assert!(n >= 4, "WAL record が減っている ({n} 本)");
    }

    /// 版数は cell (eid × himo) ごとに独立。 himo が動的定義でも
    /// (= define_himo_slot_locked 経由でも) version column が付く。
    #[test]
    fn versions_are_independent_per_cell() {
        let (mut eng, hid_a, e1) = v9_engine("per_cell", "a");
        eng.define_himo("b", ValueType::Number, 0);
        let hid_b = eng.himo_id("b").unwrap() as u16;
        let e2 = eng.entity().unwrap();

        assert!(eng.set_cell(e1, hid_a, 1, hlc(300, 1)));
        assert!(eng.set_cell(e1, hid_b, 2, hlc(100, 1)), "himo a の版数が b を塞いだ");
        assert!(eng.set_cell(e2, hid_a, 3, hlc(100, 1)), "e1 の版数が e2 を塞いだ");

        assert_eq!(eng.cell_hlc(e1, hid_a), hlc(300, 1));
        assert_eq!(eng.cell_hlc(e1, hid_b), hlc(100, 1));
        assert_eq!(eng.cell_hlc(e2, hid_a), hlc(100, 1));
        assert_eq!(eng.cell_hlc(e2, hid_b), Hlc::ZERO);
        assert_eq!(eng.get(e1, "a"), Some(1));
        assert_eq!(eng.get(e1, "b"), Some(2));
        assert_eq!(eng.get(e2, "a"), Some(3));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let path = format!("/tmp/enchu_v24_{name}.db");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        path
    }

    /// テスト用: rebuild + query_count を一発で。
    fn qc(eng: &Engine, conds: &[(&str, u32)]) -> usize {
        eng.rebuild();
        eng.query(conds).len()
    }

    /// **fold の cursor 巻き戻しは、 巻き戻し前の `from` を握った in-flight transfer の
    /// store に上書きされてはならない。**
    ///
    /// `transfer_oplog_to_sync_ops` は入口で `from` を読み、 最後に cursor を進める。
    /// その間に fold (`try_reset` + `reset_sync_ops_offset`) が入ると、 素の store は
    /// 巻き戻しを stale 値で上書きし、 cursor が head を追い越したまま固定する。
    /// そうなると scan は永久に 0 件、 かつ `wal_fold_safe` が `offset >= head` を
    /// 「追いつき済み」 と誤読して畳み続けるので、 新 ring の record は無言で恒久欠落する
    /// (実測: 300 iter の stress で ~15% の run で 1 件消えた)。
    #[test]
    fn stale_transfer_store_cannot_clobber_a_fold_rewind() {
        let path = tmp("bridge_cursor_clobber");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        for suffix in ["", ".oplog", ".tables", ".crc", ".lock"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
        {
            let mut eng = Engine::create_standalone(&path).unwrap();
            eng.define_table("rows", 1_000).unwrap();
            eng.define_himo_in("rows", "val", ValueType::Number, 1_000).unwrap();
            eng.enable_sync_tables().unwrap();
            eng.flush().unwrap();
        }
        let eng = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
        eng.set_peer_id(1);
        let hid = eng.himo_id("rows.val").unwrap() as u16;

        // bridge を 1 回走らせて cursor を進める
        let e = eng.entity_in("rows").unwrap();
        eng.tie_async_by_id(e, hid, 7);
        eng.oplog_sync().unwrap();
        let advanced = eng.sync_ops_bridge_offset();
        assert!(
            advanced > enchudb_oplog::oplog::HEADER_SIZE as u64,
            "premise: bridge が cursor を進めていること (offset={advanced})"
        );

        // fold の巻き戻しを再現
        eng.reset_sync_ops_offset();
        assert_eq!(
            eng.sync_ops_bridge_offset(),
            enchudb_oplog::oplog::HEADER_SIZE as u64,
            "premise: 巻き戻しが効いていること"
        );

        // 巻き戻し前に読んだ `from` を握った transfer が cursor を進めようとする。
        // trace で実際に観測されたのは records 空 (= from == committed_end) の path。
        eng.advance_sync_ops_cursor(advanced, advanced);

        assert_eq!(
            eng.sync_ops_bridge_offset(),
            enchudb_oplog::oplog::HEADER_SIZE as u64,
            "stale store が fold の巻き戻しを上書きした (= 新 ring の record が恒久欠落する)"
        );
        assert_eq!(
            eng.sync_ops_cursor_repairs(),
            1,
            "上書きを弾いたことが観測できること"
        );
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        for suffix in ["", ".oplog", ".tables", ".crc", ".lock"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    /// **cursor が head を追い越した状態は 「畳んでよい」 ではなく 「不整合」。**
    ///
    /// `wal_fold_safe` は `offset >= head` で fold を許可していたため、 上の lost update で
    /// 壊れた cursor を 「bridge 追いつき済み」 と読んで畳み続けていた。 追い越しは
    /// 検出して畳まず、 ring 先頭へ巻き戻す (最悪 重複配布、 apply は冪等)。
    #[test]
    fn cursor_overtaking_head_blocks_the_fold_and_self_heals() {
        let path = tmp("bridge_cursor_overtake");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        for suffix in ["", ".oplog", ".tables", ".crc", ".lock"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
        {
            let mut eng = Engine::create_standalone(&path).unwrap();
            eng.define_table("rows", 1_000).unwrap();
            eng.define_himo_in("rows", "val", ValueType::Number, 1_000).unwrap();
            eng.enable_sync_tables().unwrap();
            eng.flush().unwrap();
        }
        let eng = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
        eng.set_peer_id(1);
        let hid = eng.himo_id("rows.val").unwrap() as u16;
        let e = eng.entity_in("rows").unwrap();
        eng.tie_async_by_id(e, hid, 9);
        eng.oplog_sync().unwrap();

        let head = eng.oplog.as_ref().unwrap().head();
        eng.sync_ops_offset
            .store(head + 4096, std::sync::atomic::Ordering::Release);

        assert!(
            !eng.wal_fold_safe(),
            "cursor が head を追い越しているのに fold を許可した"
        );
        assert_eq!(
            eng.sync_ops_bridge_offset(),
            enchudb_oplog::oplog::HEADER_SIZE as u64,
            "追い越しを検出したら ring 先頭へ巻き戻すこと"
        );
        assert!(eng.sync_ops_cursor_repairs() >= 1, "修復が観測できること");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        for suffix in ["", ".oplog", ".tables", ".crc", ".lock"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    /// #128: 進捗の無い Retry 連発 (= crash 残骸の odd gen) では reader が
    /// hang せず **有限時間で None** に落ちること。 進捗検出付き retry loop
    /// (text_owned_by_id) の escape 経路の regression test。
    #[test]
    fn issue128_stalled_slot_returns_none_without_hang() {
        let dir = tmp("issue128_stall");
        let mut eng = Engine::create_growable(&dir).unwrap();
        eng.define_himo("body", ValueType::Leaf, 0);
        let eid = eng.entity().unwrap();
        eng.tie_text(eid, "body", "hello-leaf-body");
        assert_eq!(
            eng.get_text_owned(eid, "body").as_deref(),
            Some("hello-leaf-body".as_bytes()),
            "premise: 正常読みできること"
        );

        // writer 書込中 crash の残骸を再現: slot gen を odd に汚す。
        let hid = eng.himo_id("body").unwrap();
        let raw = eng.himos[hid].get_value(enchudb_oplog::eid_local(eid)).unwrap();
        eng.leaf_for(hid).expect("routed leaf").poison_gen_odd_for_test(raw);

        let t0 = std::time::Instant::now();
        assert_eq!(
            eng.get_text_owned(eid, "body"),
            None,
            "odd gen (進捗なし) は None に落ちること"
        );
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(10),
            "stall escape が {} ms — hang している",
            t0.elapsed().as_millis()
        );
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── entity ライフサイクル ────

    #[test]
    fn entity_create_and_count() {
        let dir = tmp("ent_create");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        assert_eq!(eng.entity_count(), 0);
        let e0 = eng.entity().unwrap();
        let e1 = eng.entity().unwrap();
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.entities(), vec![e0, e1]);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn entity_delete_and_reuse() {
        let dir = tmp("ent_del");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e0 = eng.entity().unwrap();
        let e1 = eng.entity().unwrap();
        let e2 = eng.entity().unwrap();

        eng.delete(e1);
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.entities(), vec![e0, e2]);

        // 上限前は欠番（monotonic）— IDは再利用されない
        let e3 = eng.entity().unwrap();
        assert_eq!(e3, 3); // e1(=1)ではなく新規ID
        assert_eq!(eng.entity_count(), 3);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── tie / get 全型 ────

    #[test]
    fn tie_text_roundtrip() {
        let dir = tmp("tie_text");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie_text(e, "name", "田中");
        assert_eq!(eng.get_text(e, "name"), Some("田中".as_bytes()));
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_value_roundtrip() {
        let dir = tmp("tie_val");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "age", 30);
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_entity_ref() {
        let dir = tmp("tie_eref");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let parent = eng.entity().unwrap();
        let child = eng.entity().unwrap();
        eng.tie_ref(child, "company", parent);
        assert_eq!(eng.get(child, "company"), Some(parent as u32));
        eng.rebuild();
        let result = eng.pull_raw("company", parent as u32);
        assert_eq!(result, vec![child]);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_overwrite() {
        let dir = tmp("tie_ow");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "score", 100);
        eng.tie(e, "score", 200);
        assert_eq!(eng.get(e, "score"), Some(200));
        assert_eq!(qc(&mut eng, &[("score", 100)]), 0);
        assert_eq!(qc(&mut eng, &[("score", 200)]), 1);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn tie_value_zero() {
        let dir = tmp("tie_zero");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "level", 0);
        assert_eq!(eng.get(e, "level"), Some(0));
        assert_eq!(qc(&mut eng, &[("level", 0)]), 1);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── untie ────

    #[test]
    fn untie_removes_value() {
        let dir = tmp("untie");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "X");

        eng.untie(e, "age");
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(qc(&mut eng, &[("age", 30)]), 0);
        assert_eq!(eng.get_text(e, "name"), Some(b"X".as_ref()));
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── delete ────

    #[test]
    fn delete_removes_all_ties() {
        let dir = tmp("del_ties");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "田中");

        eng.delete(e);
        assert_eq!(qc(&mut eng, &[("age", 30)]), 0);
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(eng.get_text(e, "name"), None);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── content ────

    #[test]
    fn content_set_get() {
        let dir = tmp("content");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.content(e, "memo", b"hello");
        eng.content(e, "notes", "日本語".as_bytes());
        assert_eq!(eng.get_content(e, "memo"), Some(b"hello".as_ref()));
        assert_eq!(eng.get_content(e, "notes"), Some("日本語".as_bytes()));
        assert_eq!(eng.get_content(e, "none"), None);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── himos_of / himo_names ────

    #[test]
    fn himos_of_entity() {
        let dir = tmp("himos_of");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "X");
        let h = eng.himos_of(e);
        assert!(h.contains(&"age"));
        assert!(h.contains(&"name"));
        assert_eq!(h.len(), 2);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn himo_names_all() {
        let dir = tmp("himo_names");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "x", 1);
        eng.tie_text(e, "y", "a");
        eng.tie_ref(e, "z", e);
        let names = eng.himo_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"x".to_string()));
        assert!(names.contains(&"y".to_string()));
        assert!(names.contains(&"z".to_string()));
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── query ────

    #[test]
    fn query_single_condition() {
        let dir = tmp("q_single");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e0 = eng.entity().unwrap();
        eng.tie(e0, "age", 30);
        let e1 = eng.entity().unwrap();
        eng.tie(e1, "age", 25);
        let e2 = eng.entity().unwrap();
        eng.tie(e2, "age", 30);

        eng.rebuild();
        let result = eng.query(&[("age", 30)]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&e0));
        assert!(result.contains(&e2));
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn query_multi_condition() {
        let dir = tmp("q_multi");
        let mut eng = Engine::create_standalone(&dir).unwrap();

        let e0 = eng.entity().unwrap();
        eng.tie(e0, "age", 30);
        eng.tie(e0, "dept", 1);

        let e1 = eng.entity().unwrap();
        eng.tie(e1, "age", 25);
        eng.tie(e1, "dept", 1);

        let e2 = eng.entity().unwrap();
        eng.tie(e2, "age", 30);
        eng.tie(e2, "dept", 2);

        eng.rebuild();
        assert_eq!(eng.query(&[("dept", 1), ("age", 30)]), vec![e0]);
        assert_eq!(eng.query(&[("dept", 1), ("age", 25)]), vec![e1]);
        assert_eq!(eng.query(&[("dept", 2), ("age", 30)]), vec![e2]);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn query_empty_result() {
        let dir = tmp("q_empty");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "age", 30);
        eng.rebuild();
        assert!(eng.query(&[("age", 99)]).is_empty());
        assert_eq!(qc(&mut eng, &[("age", 99)]), 0);
        assert!(eng.query(&[("nonexistent", 1)]).is_empty());
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn query_count_matches_len() {
        let dir = tmp("q_count");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        for i in 0..10 {
            let e = eng.entity().unwrap();
            eng.tie(e, "bucket", i % 3);
        }
        eng.rebuild();
        for b in 0..3 {
            let q = eng.query(&[("bucket", b)]);
            let c = qc(&mut eng, &[("bucket", b)]);
            assert_eq!(q.len(), c);
        }
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── LazyCylinder ────

    #[test]
    fn lazy_cylinder_pull_observe() {
        let dir = tmp("lazy_cyl");
        let mut eng = Engine::create_standalone(&dir).unwrap();

        let e0 = eng.entity().unwrap();
        eng.tie(e0, "age", 30);
        eng.tie(e0, "dept", 1);

        let e1 = eng.entity().unwrap();
        eng.tie(e1, "age", 25);
        eng.tie(e1, "dept", 1);

        eng.rebuild();
        assert_eq!(eng.query(&[("dept", 1), ("age", 30)]), vec![e0]);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── range query ────

    #[test]
    fn pull_range() {
        let dir = tmp("range");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        for age in 20..=40 {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", age);
        }
        eng.rebuild();
        let mut total = 0;
        for age in 25..=30 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, 6);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn lazy_cylinder_pull_range() {
        let dir = tmp("lc_range");
        let mut eng = Engine::create_standalone(&dir).unwrap();

        for age in 20..=40 {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", age);
            eng.tie(e, "dept", 1);
        }

        eng.rebuild();
        let mut age_ents: Vec<u64> = Vec::new();
        for age in 25..=30 {
            age_ents.extend(eng.pull_raw("age", age));
        }
        age_ents.sort_unstable();
        let dept1 = eng.pull_raw("dept", 1);
        let mut count = 0;
        let (mut i, mut j) = (0, 0);
        while i < age_ents.len() && j < dept1.len() {
            match age_ents[i].cmp(&dept1[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => { count += 1; i += 1; j += 1; }
            }
        }
        assert_eq!(count, 6);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 永続化 ────

    #[test]
    fn persistence_full_roundtrip() {
        let dir = tmp("persist");

        {
            let mut eng = Engine::create_standalone(&dir).unwrap();
            let e0 = eng.entity().unwrap();
            eng.tie(e0, "age", 25);
            eng.tie(e0, "dept", 1);

            let e1 = eng.entity().unwrap();
            eng.tie(e1, "age", 30);
            eng.tie(e1, "dept", 1);
            eng.content(e1, "memo", b"hello");

            eng.flush().unwrap();
        }

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.get(0, "age"), Some(25));
        assert_eq!(eng.get(1, "age"), Some(30));
        assert_eq!(eng.get_content(1, "memo"), Some(b"hello".as_ref()));
        assert_eq!(qc(&mut eng, &[("dept", 1), ("age", 30)]), 1);

        let e2 = eng.entity().unwrap();
        eng.tie(e2, "age", 35);
        eng.tie(e2, "dept", 1);
        assert_eq!(qc(&mut eng, &[("dept", 1), ("age", 35)]), 1);

        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── vocab ────

    #[test]
    fn vocab_id_lookup() {
        let dir = tmp("vocab");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie_text(e, "city", "東京");
        eng.tie_text(e, "city2", "大阪");

        assert!(eng.vocab_id("東京").is_some());
        assert!(eng.vocab_id("大阪").is_some());
        assert!(eng.vocab_id("福岡").is_none());
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 境界値 ────

    #[test]
    fn boundary_value_zero() {
        let dir = tmp("bnd_zero");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "x", 0);
        assert_eq!(eng.get(e, "x"), Some(0));
        assert_eq!(qc(&mut eng, &[("x", 0)]), 1);

        eng.untie(e, "x");
        assert_eq!(eng.get(e, "x"), None);
        eng.tie(e, "x", 0);
        assert_eq!(eng.get(e, "x"), Some(0));
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn boundary_value_large() {
        let dir = tmp("bnd_large");
        let mut eng = Engine::create_standalone(&dir).unwrap();

        let ts = 1_743_552_000u32;
        let e = eng.entity().unwrap();
        eng.tie(e, "ts", ts);
        assert_eq!(eng.get(e, "ts"), Some(ts));
        eng.rebuild();
        let result = eng.pull_raw("ts", ts);
        assert_eq!(result, vec![e]);

        // BucketCylinder は動的拡張するので、 u32::MAX-2 だと
        // バケット 40 億本分の Vec を確保してしまう。 ここでは代わりに
        // 100 万オーダーの「大きめ値」で検証する。
        let big = 1_000_000u32;
        let e2 = eng.entity().unwrap();
        eng.tie(e2, "huge", big);
        assert_eq!(eng.get(e2, "huge"), Some(big));
        eng.rebuild();
        let result2 = eng.pull_raw("huge", big);
        assert_eq!(result2, vec![e2]);

        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn boundary_consecutive_values() {
        let dir = tmp("bnd_consec");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        for v in 0..5u32 {
            let e = eng.entity().unwrap();
            eng.tie(e, "level", v);
        }
        for v in 0..5u32 {
            assert_eq!(qc(&mut eng, &[("level", v)]), 1);
        }
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn boundary_many_dims() {
        let dir = tmp("bnd_dims");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        for d in 0..20u32 {
            eng.tie(e, &format!("dim_{d}"), d * 10);
        }
        for d in 0..20u32 {
            assert_eq!(eng.get(e, &format!("dim_{d}")), Some(d * 10));
        }
        assert_eq!(eng.himos_of(e).len(), 20);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 大量削除 → query整合性 ────

    #[test]
    fn bulk_delete_query_consistency() {
        let dir = tmp("bulk_del");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let n = 1000u32;
        for i in 0..n {
            let e = eng.entity().unwrap();
            eng.tie(e, "group", i % 5);
            eng.tie(e, "score", (i / 5) % 10);
        }

        eng.rebuild();
        let group0: Vec<u64> = eng.query(&[("group", 0)]);
        assert_eq!(group0.len(), 200);
        for &eid in &group0 {
            eng.delete(eid);
        }
        for g in 1..5u32 {
            assert_eq!(eng.query_count(&[("group", g)]), 200);
        }
        assert_eq!(eng.entity_count(), 800);
        for s in 0..10u32 {
            assert_eq!(eng.query_count(&[("score", s)]), 80);
        }
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn delete_all_then_reinsert() {
        let dir = tmp("del_all");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let n = 100u32;
        for _ in 0..n {
            let e = eng.entity().unwrap();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.query_count(&[("val", 42)]), 100);

        let all: Vec<u64> = eng.entities();
        for eid in all {
            eng.delete(eid);
        }
        assert_eq!(eng.entity_count(), 0);
        assert_eq!(eng.query_count(&[("val", 42)]), 0);

        for _ in 0..50 {
            let e = eng.entity().unwrap();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.entity_count(), 50);
        assert_eq!(eng.query_count(&[("val", 42)]), 50);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 永続化の堅牢性 ────

    #[test]
    fn persistence_after_delete() {
        let dir = tmp("persist_del");
        {
            let mut eng = Engine::create_standalone(&dir).unwrap();
            for i in 0..100u32 {
                let e = eng.entity().unwrap();
                eng.tie(e, "val", i % 10);
            }
            eng.rebuild();
            let del_targets: Vec<u64> = eng.query(&[("val", 0)]);
            for &eid in &del_targets {
                eng.delete(eid);
            }
            eng.flush().unwrap();
        }

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.query_count(&[("val", 0)]), 0);
        for v in 1..10u32 {
            assert_eq!(eng.query_count(&[("val", v)]), 10);
        }
        assert_eq!(eng.entity_count(), 90);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 多数 entity ────

    #[test]
    fn many_entities_1k() {
        let dir = tmp("many_1k");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let n = 1000u32;
        for i in 0..n {
            let e = eng.entity().unwrap();
            eng.tie(e, "val", i % 10);
        }
        assert_eq!(eng.entity_count(), n);
        for b in 0..10 {
            assert_eq!(eng.query_count(&[("val", b)]), 100);
        }
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 100万 entity スケールテスト ────

    const SCALE_N: u32 = 1_000_000;
    const SCALE_COMPANIES: u32 = 100;
    const SCALE_CITIES: u32 = 10;
    const SCALE_AGES: u32 = 50;
    const SCALE_DEPTS: u32 = 8;
    const SCALE_PER_CO: u32 = SCALE_N / SCALE_COMPANIES;

    fn setup_scale(dir: &str) -> Engine {
        let mut eng = Engine::create_standalone(dir).unwrap();

        for c in 0..SCALE_COMPANIES {
            for e in 0..SCALE_PER_CO {
                let eid = eng.entity().unwrap();
                eng.tie(eid, "age", e % SCALE_AGES);
                eng.tie(eid, "dept", (e / SCALE_AGES) % SCALE_DEPTS);
                eng.tie(eid, "company", c);
                eng.tie_text(eid, "city", &format!("city_{}", c % SCALE_CITIES));
            }
        }
        eng
    }

    #[test]
    #[ignore]
    fn scale_insert_1m() {
        let dir = tmp("scale_insert");
        let eng = setup_scale(&dir);
        assert_eq!(eng.entity_count(), SCALE_N);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_point_query() {
        let dir = tmp("scale_point");
        let mut eng = setup_scale(&dir);
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(eng.query_count(&[("age", 30)]), expected);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_multi_condition() {
        let dir = tmp("scale_multi");
        let mut eng = setup_scale(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let expected = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30)]), expected);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_three_conditions() {
        let dir = tmp("scale_3cond");
        let mut eng = setup_scale(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let per_co = SCALE_PER_CO / SCALE_AGES / SCALE_DEPTS;
        let expected = (SCALE_COMPANIES / SCALE_CITIES * per_co) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30), ("dept", 3)]), expected);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_range_query() {
        let dir = tmp("scale_range");
        let mut eng = setup_scale(&dir);
        eng.rebuild();
        let per_age = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let mut total = 0;
        for age in 25..=34 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, per_age * 10);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_empty_result() {
        let dir = tmp("scale_empty");
        let mut eng = setup_scale(&dir);
        assert_eq!(eng.query_count(&[("age", 99)]), 0);
        assert!(eng.query(&[("age", 99)]).is_empty());
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_delete_reinsert() {
        let dir = tmp("scale_delins");
        let mut eng = setup_scale(&dir);
        let before = eng.query_count(&[("age", 30)]);

        let victims: Vec<u64> = eng.query(&[("age", 30)]).into_iter().take(100).collect();
        for eid in &victims {
            eng.delete(*eid);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before - 100);

        for _ in 0..100 {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", 30);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_update() {
        let dir = tmp("scale_upd");
        let mut eng = setup_scale(&dir);
        let before_30 = eng.query_count(&[("age", 30)]);
        assert_eq!(eng.query_count(&[("age", 99)]), 0);

        let targets: Vec<u64> = eng.query(&[("age", 30)]).into_iter().take(500).collect();
        for eid in &targets {
            eng.tie(*eid, "age", 99);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before_30 - 500);
        assert_eq!(eng.query_count(&[("age", 99)]), 500);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_persistence() {
        let dir = tmp("scale_persist");
        let city0_vid;
        let expected_age30 = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let expected_city_age = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        {
            let mut eng = setup_scale(&dir);
            city0_vid = eng.vocab_id("city_0").unwrap();
            eng.flush().unwrap();
        }

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.entity_count(), SCALE_N);
        assert_eq!(eng.query_count(&[("age", 30)]), expected_age30);
        assert_eq!(eng.query_count(&[("city", city0_vid), ("age", 30)]), expected_city_age);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn scale_group_by_equivalent() {
        let dir = tmp("scale_grp");
        let mut eng = setup_scale(&dir);
        let mut total = 0usize;
        for c in 0..SCALE_CITIES {
            let vid = eng.vocab_id(&format!("city_{c}")).unwrap();
            total += eng.query_count(&[("city", vid), ("age", 30)]);
        }
        let expected_total = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(total, expected_total);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── トランザクション ────

    #[test]
    fn commit_persists() {
        let dir = tmp("tx_commit");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        let e = eng.entity().unwrap();
        eng.tie(e, "age", 30);
        eng.commit();
        eng.flush().unwrap();
        drop(eng);

        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // v4: rollback / undo log を削除したため `rollback_reverts` /
    //     `rollback_insert` / `crash_recovery_rollback` テストも撤去。 旧 undo
    //     replay 経路の保証 (= flush 済み未 commit 書き込みを open 時に巻き戻す)
    //     はもう存在しない。 crash 中の途中状態を巻き戻したいケースは WAL
    //     (Commit marker 未到達なら recover 時に drop) で代替する。

    // ──── prefix sum O(1) ────

    #[test]
    fn prefix_sum_point_query() {
        let dir = tmp("ps_point");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 100);
        eng.define_himo("dept", ValueType::Number, 20);

        for i in 0..1000u32 {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", i % 50);
            eng.tie(e, "dept", i % 8);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), 20);
        assert_eq!(eng.query_count(&[("dept", 3)]), 125);
        assert_eq!(eng.query(&[("age", 30), ("dept", 2)]).len(), 5);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_value_zero() {
        let dir = tmp("ps_zero");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("level", ValueType::Number, 10);
        let e = eng.entity().unwrap();
        eng.tie(e, "level", 0);
        assert_eq!(eng.get(e, "level"), Some(0));
        assert_eq!(eng.query_count(&[("level", 0)]), 1);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_mixed_with_bsearch() {
        let dir = tmp("ps_mixed");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 100);

        for i in 0..100u32 {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", i % 10);
            eng.tie_text(e, "city", if i < 50 { "東京" } else { "大阪" });
        }
        let tokyo = eng.vocab_id("東京").unwrap();
        assert_eq!(eng.query_count(&[("age", 5)]), 10);
        assert_eq!(eng.query_count(&[("city", tokyo)]), 50);
        assert_eq!(eng.query_count(&[("age", 5), ("city", tokyo)]), 5);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_persistence() {
        let dir = tmp("ps_persist");
        {
            let mut eng = Engine::create_standalone(&dir).unwrap();
            eng.define_himo("score", ValueType::Number, 200);
            for i in 0..100u32 {
                let e = eng.entity().unwrap();
                eng.tie(e, "score", i % 20);
            }
            assert_eq!(eng.query_count(&[("score", 5)]), 5);
            eng.flush().unwrap();
        }
        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.query_count(&[("score", 5)]), 5);
        assert_eq!(eng.entity_count(), 100);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_untie() {
        let dir = tmp("ps_untie");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 100);
        let e = eng.entity().unwrap();
        eng.tie(e, "age", 30);
        assert_eq!(eng.query_count(&[("age", 30)]), 1);
        eng.untie(e, "age");
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(eng.query_count(&[("age", 30)]), 0);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_overwrite() {
        let dir = tmp("ps_ow");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("score", ValueType::Number, 1000);
        let e = eng.entity().unwrap();
        eng.tie(e, "score", 100);
        eng.tie(e, "score", 200);
        assert_eq!(eng.get(e, "score"), Some(200));
        assert_eq!(eng.query_count(&[("score", 100)]), 0);
        assert_eq!(eng.query_count(&[("score", 200)]), 1);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_delete() {
        let dir = tmp("ps_del");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 100);
        eng.define_himo("dept", ValueType::Number, 20);
        for i in 0..100u32 {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", i % 10);
            eng.tie(e, "dept", i % 5);
        }
        eng.rebuild();
        let victims: Vec<u64> = eng.query(&[("age", 0)]);
        assert_eq!(victims.len(), 10);
        for &eid in &victims { eng.delete(eid); }
        for a in 1..10u32 {
            assert_eq!(eng.query_count(&[("age", a)]), 10);
        }
        assert_eq!(eng.entity_count(), 90);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }


    #[test]
    fn prefix_sum_boundary_max() {
        let dir = tmp("ps_bnd_max");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("x", ValueType::Number, 10);
        let e = eng.entity().unwrap();
        eng.tie(e, "x", 10);
        assert_eq!(eng.get(e, "x"), Some(10));
        assert_eq!(eng.query_count(&[("x", 10)]), 1);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn prefix_sum_bulk_delete_reinsert() {
        let dir = tmp("ps_bulk");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("val", ValueType::Number, 100);
        for _ in 0..500u32 {
            let e = eng.entity().unwrap();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.query_count(&[("val", 42)]), 500);

        let all: Vec<u64> = eng.entities();
        for eid in all { eng.delete(eid); }
        assert_eq!(eng.entity_count(), 0);
        assert_eq!(eng.query_count(&[("val", 42)]), 0);

        for _ in 0..200 {
            let e = eng.entity().unwrap();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.entity_count(), 200);
        assert_eq!(eng.query_count(&[("val", 42)]), 200);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── prefix sum スケールテスト（100万 entity）────

    fn setup_scale_prefix(dir: &str) -> Engine {
        let mut eng = Engine::create_standalone(dir).unwrap();
        eng.define_himo("age", ValueType::Number, SCALE_AGES);
        eng.define_himo("dept", ValueType::Number, SCALE_DEPTS);
        eng.define_himo("company", ValueType::Number, SCALE_COMPANIES);

        for c in 0..SCALE_COMPANIES {
            for e in 0..SCALE_PER_CO {
                let eid = eng.entity().unwrap();
                eng.tie(eid, "age", e % SCALE_AGES);
                eng.tie(eid, "dept", (e / SCALE_AGES) % SCALE_DEPTS);
                eng.tie(eid, "company", c);
                eng.tie_text(eid, "city", &format!("city_{}", c % SCALE_CITIES));
            }
        }
        eng
    }

    #[test]
    #[ignore]
    fn ps_scale_insert_1m() {
        let dir = tmp("ps_scale_ins");
        let eng = setup_scale_prefix(&dir);
        assert_eq!(eng.entity_count(), SCALE_N);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_point_query() {
        let dir = tmp("ps_scale_point");
        let mut eng = setup_scale_prefix(&dir);
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(eng.query_count(&[("age", 30)]), expected);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_multi_condition() {
        let dir = tmp("ps_scale_multi");
        let mut eng = setup_scale_prefix(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let expected = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30)]), expected);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_three_conditions() {
        let dir = tmp("ps_scale_3cond");
        let mut eng = setup_scale_prefix(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let per_co = SCALE_PER_CO / SCALE_AGES / SCALE_DEPTS;
        let expected = (SCALE_COMPANIES / SCALE_CITIES * per_co) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30), ("dept", 3)]), expected);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_range_query() {
        let dir = tmp("ps_scale_range");
        let mut eng = setup_scale_prefix(&dir);
        eng.rebuild();
        let per_age = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let mut total = 0;
        for age in 25..=34 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, per_age * 10);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_delete_reinsert() {
        let dir = tmp("ps_scale_delins");
        let mut eng = setup_scale_prefix(&dir);
        let before = eng.query_count(&[("age", 30)]);
        let victims: Vec<u64> = eng.query(&[("age", 30)]).into_iter().take(100).collect();
        for eid in &victims { eng.delete(*eid); }
        assert_eq!(eng.query_count(&[("age", 30)]), before - 100);
        for _ in 0..100 {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", 30);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_update() {
        let dir = tmp("ps_scale_upd");
        let mut eng = setup_scale_prefix(&dir);
        let before_30 = eng.query_count(&[("age", 30)]);
        let targets: Vec<u64> = eng.query(&[("age", 30)]).into_iter().take(500).collect();
        for eid in &targets { eng.tie(*eid, "age", 49); }
        assert_eq!(eng.query_count(&[("age", 30)]), before_30 - 500);
        assert_eq!(eng.query_count(&[("age", 49)]),
            (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize + 500);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_persistence() {
        let dir = tmp("ps_scale_persist");
        let city0_vid;
        let expected_age30 = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let expected_city_age = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        {
            let mut eng = setup_scale_prefix(&dir);
            city0_vid = eng.vocab_id("city_0").unwrap();
            eng.flush().unwrap();
        }
        let mut eng = Engine::open_standalone(&dir).unwrap();
        assert_eq!(eng.entity_count(), SCALE_N);
        assert_eq!(eng.query_count(&[("age", 30)]), expected_age30);
        assert_eq!(eng.query_count(&[("city", city0_vid), ("age", 30)]), expected_city_age);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_group_by() {
        let dir = tmp("ps_scale_grp");
        let mut eng = setup_scale_prefix(&dir);
        let mut total = 0usize;
        for c in 0..SCALE_CITIES {
            let vid = eng.vocab_id(&format!("city_{c}")).unwrap();
            total += eng.query_count(&[("city", vid), ("age", 30)]);
        }
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(total, expected);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 1億 entity ────

    #[test]
    #[ignore]
    fn scale_100m_insert_and_query() {
        let dir = tmp("scale_100m");
        let n = 100_000_000u32;
        let ages = 100u32;
        let depts = 20u32;
        let groups = 1000u32;

        let mut eng = Engine::create_with_capacity(&dir, n + 1024).unwrap();
        eng.define_himo("age", ValueType::Number, ages);
        eng.define_himo("dept", ValueType::Number, depts);
        eng.define_himo("group", ValueType::Number, groups);

        for i in 0..n {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", i % ages);
            eng.tie(e, "dept", i % depts);
            eng.tie(e, "group", i % groups);
            if i % 1_000_000 == 999_999 { eng.commit(); }
        }
        assert_eq!(eng.entity_count(), n);

        assert_eq!(eng.query_count(&[("age", 50)]), (n / ages) as usize);
        assert_eq!(eng.query_count(&[("dept", 10)]), (n / depts) as usize);
        assert_eq!(eng.query_count(&[("age", 50), ("dept", 10)]), (n / ages) as usize);
        assert_eq!(eng.query_count(&[("age", 50), ("group", 500)]), 0);
        assert_eq!(eng.query_count(&[("age", 50), ("group", 50)]), (n / groups) as usize);
        assert_eq!(eng.query_count(&[("age", 30), ("dept", 10), ("group", 30)]), (n / groups) as usize);

        assert_eq!(eng.get(50, "age"), Some(50));
        assert_eq!(eng.get(50, "dept"), Some(50 % depts));

        let victims: Vec<u64> = eng.query(&[("age", 99)]).into_iter().take(1000).collect();
        for &eid in &victims { eng.delete(eid); }
        assert_eq!(eng.query_count(&[("age", 99)]), (n / ages) as usize - 1000);
        assert_eq!(eng.entity_count(), n - 1000);

        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn test_late_himo_on_existing_entities() {
        let dir = tmp("late_himo");
        // Phase 1: 1000 entity作成、nameだけtie
        let mut eng = Engine::create_standalone(&dir).unwrap();
        for i in 0..1000u32 {
            let e = eng.entity().unwrap();
            eng.tie_text(e, "name", &format!("company_{i}"));
        }
        eng.rebuild();
        eng.flush().unwrap();
        drop(eng);

        // Phase 2: 再open、既存entityに新しいhimoをtie
        let mut eng = Engine::open_standalone(&dir).unwrap();
        eng.rebuild();
        for eid in 0..1000u64 {
            eng.tie_text(eid, "has_flag", "1");
        }
        eng.rebuild();

        // rebuildの後、全件pull_rawで引けるか
        let vid = eng.vocab_id("1").expect("vocab_id");
        let result = eng.pull_raw("has_flag", vid);
        assert_eq!(result.len(), 1000, "expected 1000, got {}", result.len());

        eng.flush().unwrap();
        drop(eng);

        // Phase 3: 再open後も全件引けるか
        let eng = Engine::open_standalone(&dir).unwrap(); // open内でrebuild済み
        let vid = eng.vocab_id("1").expect("vocab_id");
        let result = eng.pull_raw("has_flag", vid);
        assert_eq!(result.len(), 1000, "after reopen: expected 1000, got {}", result.len());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_late_himo_sparse_large_eid() {
        let dir = tmp("late_himo_sparse");
        // Phase 1: 100 entity、大きなeid空間をシミュレート
        let mut eng = Engine::create_with_capacity(&dir, 6_000_000).unwrap();
        // entity 0..99 を作成
        for i in 0..100u32 {
            let e = eng.entity().unwrap();
            eng.tie_text(e, "name", &format!("company_{i}"));
        }
        // entity 100..5_999_999 も作成（名前なし、eid空間を広げる）
        for _ in 100..1000 {
            eng.entity().unwrap();
        }
        eng.rebuild();
        eng.flush().unwrap();
        drop(eng);

        // Phase 2: 再open、entity 500 と 999 に新himo をtie
        let mut eng = Engine::open_standalone(&dir).unwrap();
        eng.rebuild();
        eng.tie_text(500, "has_flag", "1");
        eng.tie_text(999, "has_flag", "1");
        eng.tie_text(0, "has_flag", "1");
        eng.rebuild();

        let vid = eng.vocab_id("1").expect("vocab_id");
        let result = eng.pull_raw("has_flag", vid);
        assert_eq!(result.len(), 3, "expected 3, got {}", result.len());

        eng.flush().unwrap();
        drop(eng);

        // Phase 3: 再open後
        let eng = Engine::open_standalone(&dir).unwrap(); // open内でrebuild済み
        let vid = eng.vocab_id("1").expect("vocab_id");
        let result = eng.pull_raw("has_flag", vid);
        assert_eq!(result.len(), 3, "after reopen: expected 3, got {}", result.len());

        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }


    #[test]
    fn concurrent_tie_async_basic() {
        // tie_async → flush_writes → pull_raw で値が見えること。
        let dir = tmp("concurrent_basic");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 200);
        let eids: Vec<u64> = (0..100).map(|_| eng.entity().unwrap()).collect();

        let arc = Engine::concurrentize(eng);
        for (i, &e) in eids.iter().enumerate() {
            arc.tie_async(e, "age", (i as u32) % 50);
        }
        arc.flush_writes();

        // 各値ごとに 2 件ずつあるはず
        for v in 0..50u32 {
            let pulled = arc.pull_raw("age", v);
            assert_eq!(pulled.len(), 2, "value {} expected 2 ents", v);
        }
        drop(arc);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn concurrent_multi_reader_writer() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let dir = tmp("concurrent_mrw");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("k", ValueType::Number, 16);
        let eids: Vec<u64> = (0..1_000).map(|_| eng.entity().unwrap()).collect();
        for (i, &e) in eids.iter().enumerate() {
            eng.tie(e, "k", (i as u32) % 16);
        }

        let arc = Engine::concurrentize(eng);
        let stop = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::new();
        // 4 reader threads
        for _ in 0..4 {
            let arc = arc.clone();
            let stop = stop.clone();
            handles.push(thread::spawn(move || {
                let mut ok = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    for v in 0..16u32 {
                        let _ = arc.pull_raw("k", v);
                        let _ = arc.query(&[("k", v)]);
                        ok += 1;
                    }
                }
                ok
            }));
        }

        // 1 writer thread (async)
        {
            let arc = arc.clone();
            let stop = stop.clone();
            let eids = eids.clone();
            handles.push(thread::spawn(move || {
                let mut i = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let e = eids[(i as usize) % eids.len()];
                    arc.tie_async(e, "k", i % 16);
                    i = i.wrapping_add(1);
                }
                i as u64
            }));
        }

        thread::sleep(Duration::from_millis(200));
        stop.store(true, Ordering::Relaxed);
        for h in handles { let _ = h.join(); }

        arc.flush_writes();
        // 最終状態で全 16 値の合計が 1000 に等しいこと(値の分布は分からないが合計は保たれる)
        let mut total = 0;
        for v in 0..16u32 {
            total += arc.pull_raw("k", v).len();
        }
        assert_eq!(total, 1000);

        drop(arc);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── blob store integration ────

    #[test]
    fn engine_blob_store_default_none() {
        let dir = tmp("blob_none");
        let eng = Engine::create_standalone(&dir).unwrap();
        assert!(eng.blob_store().is_none());
        assert!(eng.put_blob(b"data").is_none());
        let fake_id = crate::blob_store::BlobId::from_bytes(b"x");
        assert!(!eng.blob_exists(&fake_id));
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn engine_blob_store_injected_put_get() {
        let dir = tmp("blob_inject");
        let eng = Engine::create_standalone(&dir).unwrap();
        let blob_root = std::env::temp_dir().join(format!(
            "enchu_blob_inj_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = std::sync::Arc::new(
            crate::blob_store::LocalBlobStore::new(&blob_root).unwrap(),
        );
        eng.set_blob_store(store);

        let data = b"image bytes";
        let id = eng.put_blob(data).unwrap().unwrap();
        assert!(eng.blob_exists(&id));
        let got = eng.get_blob(&id).unwrap().unwrap();
        assert_eq!(got.as_deref(), Some(&data[..]));

        let _ = std::fs::remove_dir_all(&blob_root);
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn engine_blob_store_tie_hash_lookup() {
        // 実運用パターン: blob の hex を tie_text で紐付けて検索できる
        let dir = tmp("blob_tie");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_himo("__blob_id", ValueType::Tag, 0);

        let blob_root = std::env::temp_dir().join(format!(
            "enchu_blob_tie_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = std::sync::Arc::new(
            crate::blob_store::LocalBlobStore::new(&blob_root).unwrap(),
        );
        eng.set_blob_store(store);

        // 3 entity に 2 種類の blob を配る(1 と 3 が同じ blob)
        let img_a = b"pixels-A".to_vec();
        let img_b = b"pixels-B".to_vec();
        let id_a = eng.put_blob(&img_a).unwrap().unwrap();
        let id_b = eng.put_blob(&img_b).unwrap().unwrap();
        assert_ne!(id_a, id_b);

        let e1 = eng.entity().unwrap();
        eng.tie_text(e1, "__blob_id", &id_a.to_hex());
        let e2 = eng.entity().unwrap();
        eng.tie_text(e2, "__blob_id", &id_b.to_hex());
        let e3 = eng.entity().unwrap();
        eng.tie_text(e3, "__blob_id", &id_a.to_hex()); // e1 と同じ画像

        eng.rebuild();

        // blob_id で entity 検索
        let vid = eng.vocab_id(&id_a.to_hex()).unwrap();
        let mut matches = eng.pull_raw("__blob_id", vid);
        matches.sort();
        assert_eq!(matches, vec![e1, e3]);

        // dedup の確認: blob 側のファイル数は 2 個のみ
        let mut file_count = 0;
        let mut stack = vec![blob_root.clone()];
        while let Some(p) = stack.pop() {
            if let Ok(rd) = std::fs::read_dir(&p) {
                for ent in rd.flatten() {
                    let ep = ent.path();
                    if ep.is_dir() {
                        stack.push(ep);
                    } else if ep.is_file() {
                        file_count += 1;
                    }
                }
            }
        }
        assert_eq!(file_count, 2);

        let _ = std::fs::remove_dir_all(&blob_root);
        let _ = std::fs::remove_file(&dir);
    }

    /// `create_growable` が standalone と等価な書き込み / 読み出しを
    /// 提供することを確認する基本テスト。 内部 backing が違うだけで
    /// API 観点では区別がつかないはず。
    #[test]
    fn growable_create_tie_query_roundtrip() {
        let dir = tmp("growable_basic");
        let mut eng = Engine::create_growable(&dir).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        eng.define_himo("city", ValueType::Tag, 0);
        for i in 0..50u32 {
            let e = eng.entity().unwrap();
            eng.tie(e, "age", 20 + (i % 30));
            eng.tie_text(
                e,
                "city",
                if i % 2 == 0 { "Tokyo" } else { "Osaka" },
            );
        }
        eng.rebuild();
        // age = 20 + 5 → 25 was tied to entity 5, 35, etc.
        assert!(eng.query(&[("age", 25)]).len() > 0);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    /// `create_growable_tiny` の apparent サイズが state-log 想定の
    /// 数百 KB に収まることを確認する。 dogfood は matcha-shell の
    /// notif_state でやるが、 ここでも基本の roundtrip + サイズを
    /// 押さえる。
    #[test]
    fn growable_tiny_file_is_small() {
        let dir = tmp("growable_tiny");
        {
            let mut eng = Engine::create_growable_tiny(&dir).unwrap();
            eng.define_himo("key", ValueType::Tag, 0);
            eng.define_himo("ts", ValueType::Number, 0);
            // 50 rows of (uuid-like, timestamp) — matcha の notif_state
            // が捌くサイズ感。
            for i in 0..50u32 {
                let e = eng.entity().unwrap();
                eng.tie_text(e, "key", &format!("uuid-{:08x}", i));
                eng.tie(e, "ts", 1_715_000_000 + i);
            }
            eng.flush().unwrap();
        }
        let meta = std::fs::metadata(&dir).unwrap();
        eprintln!(
            "growable_tiny apparent size after 50 rows: {} bytes ({:.1} KB)",
            meta.len(),
            meta.len() as f64 / 1024.0
        );
        // create_compact 同等シナリオは 305 MB apparent。 tiny は
        // 全 region の上限値の合計でファイルサイズが決まる仕組みで
        // 実測 ~ 5 MB に収まる。 これでも 60× 改善。
        // 真の lazy init で 4 KB クラスにするのは Phase B (issue.md)。
        assert!(
            meta.len() < 8 * 1024 * 1024,
            "tiny growable should be < 8 MB, got {} bytes",
            meta.len()
        );
        // 再オープンしてデータが取れることも確認
        let eng2 = Engine::open_standalone(&dir).unwrap();
        eng2.rebuild();
        assert_eq!(eng2.query(&[("ts", 1_715_000_010)]).len(), 1);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ─────────────────────────────────────────────────────────────
    // β: writer lock + open_readonly tests
    // ─────────────────────────────────────────────────────────────

    /// readonly で開いた DB は writer lock を取らないので、 writer の存在に関わらず
    /// 開ける + 複数 readonly 同時 open も衝突しない。
    #[test]
    fn readonly_does_not_block_other_opens() {
        let p = tmp("readonly_no_block");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            let e = eng.entity().unwrap();
            eng.tie(e, "v", 42);
            eng.flush().unwrap();
        }
        // writer 開きっぱなしのまま readonly 多重 open
        let writer = Engine::open_standalone(&p).unwrap();
        let r1 = Engine::open_readonly(&p).unwrap();
        let r2 = Engine::open_readonly(&p).unwrap();
        let r3 = Engine::open_readonly(&p).unwrap();
        // 全 reader が同じ値を見る
        for r in [&r1, &r2, &r3] {
            assert_eq!(r.pull_raw("v", 42).len(), 1);
        }
        drop((writer, r1, r2, r3));
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// readonly で開いた engine の書き込み API は panic する。
    #[test]
    #[should_panic(expected = "read-only")]
    fn readonly_write_panics() {
        let p = tmp("readonly_panic");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            eng.flush().unwrap();
        }
        let mut eng = Engine::open_readonly(&p).unwrap();
        let e = eng.entity().unwrap(); // ← ここで panic
        eng.tie(e, "v", 1);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// #56: readonly open は file を mutate しない (clean flag flip + msync しない)。
    /// 旧 behavior では open するだけで mtime が変わり DB が dirty 化していた。
    #[test]
    fn readonly_open_does_not_mutate_file() {
        let p = tmp("readonly_nomutate");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            let e = eng.entity().unwrap();
            eng.tie(e, "v", 42);
            eng.flush().unwrap(); // clean=true で確定
        }
        let mtime_before = std::fs::metadata(&p).unwrap().modified().unwrap();
        // 書き込みがあれば mtime 差が必ず出るよう少し待つ
        std::thread::sleep(std::time::Duration::from_millis(20));
        {
            let eng = Engine::open_readonly(&p).unwrap();
            // read path も非破壊であること
            assert_eq!(eng.pull_raw("v", 42).len(), 1);
        }
        let mtime_after = std::fs::metadata(&p).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "readonly open + read must not modify the DB file (#56)"
        );
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// 2 process emulation: 1 writer が live な間、 もう一つの open_standalone は
    /// 別 thread から呼ぶと block する。 100 ms 待っても 2nd が returnしないことで
    /// 排他を確認、 1st を drop すると 2nd が unblock。
    #[test]
    fn writer_blocks_concurrent_writer() {
        let p = tmp("writer_block");
        {
            let mut eng = Engine::create_standalone(&p).unwrap();
            eng.define_himo("v", ValueType::Number, 100);
            eng.flush().unwrap();
        }
        let eng_a = Engine::open_standalone(&p).unwrap();

        // #80: 同一プロセスの 2nd writer open は flock で block せず即エラー。
        // (別プロセス writer との排他は従来通り blocking flock — そちらは
        //  tests/content_store_cross_process.rs 系の subprocess テストで担保)
        let err = match Engine::open_standalone(&p) {
            Ok(_) => panic!("2nd open_standalone should fail fast in the same process"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
        assert!(err.to_string().contains("already open for writing in this process"));

        // 1 st を drop → 再 open できる
        drop(eng_a);
        let eng_b = Engine::open_standalone(&p)
            .expect("2nd open should succeed after 1st drop");
        drop(eng_b);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(format!("{}.lock", p));
    }

    // ─────────────────────────────────────────────────────────────
    // _by_id API: string 版と等価な結果を返す
    // ─────────────────────────────────────────────────────────────

    /// `tie_to_by_id` / `tie_text_to_by_id` / `tie_ref_to_by_id` / `untie_by_id`
    /// が、 同等の string 版と同じ Column/vocab 状態を残すことを確認。
    #[test]
    fn by_id_tie_equivalence_sync() {
        let p_str = tmp("by_id_str");
        let p_id = tmp("by_id_id");
        {
            let eng = Engine::create_standalone(&p_str).unwrap();
            // string 版経路
            let mut eng = eng;
            eng.define_himo("year", ValueType::Number, 100);
            eng.define_himo("name", ValueType::Tag, 0);
            eng.define_himo("self_ref", ValueType::Ref, 0);
            let e = eng.entity().unwrap();
            eng.tie_to(e, "year", 2026);
            eng.tie_text_to(e, "name", "alice");
            eng.tie_ref_to(e, "self_ref", e);
            eng.flush().unwrap();
        }
        {
            let eng = Engine::create_standalone(&p_id).unwrap();
            let mut eng = eng;
            eng.define_himo("year", ValueType::Number, 100);
            eng.define_himo("name", ValueType::Tag, 0);
            eng.define_himo("self_ref", ValueType::Ref, 0);
            let year_id = eng.himo_id("year").unwrap() as u16;
            let name_id = eng.himo_id("name").unwrap() as u16;
            let ref_id = eng.himo_id("self_ref").unwrap() as u16;
            let e = eng.entity().unwrap();
            eng.tie_to_by_id(e, year_id, 2026);
            eng.tie_text_to_by_id(e, name_id, "alice");
            eng.tie_ref_to_by_id(e, ref_id, e);
            eng.flush().unwrap();
        }
        // 同じ size、 query 結果も同じ
        let m_str = std::fs::metadata(&p_str).unwrap().len();
        let m_id = std::fs::metadata(&p_id).unwrap().len();
        assert_eq!(m_str, m_id, "file size diverges: str={}, id={}", m_str, m_id);

        let eng_str = Engine::open_standalone(&p_str).unwrap();
        let eng_id = Engine::open_standalone(&p_id).unwrap();
        assert_eq!(eng_str.query(&[("year", 2026)]).len(), eng_id.query(&[("year", 2026)]).len());
        let alice = eng_str.vocab_id("alice").unwrap();
        assert_eq!(eng_str.pull_raw("name", alice).len(), eng_id.pull_raw("name", alice).len());

        let _ = std::fs::remove_dir_all(&p_str); // v10: DB は directory
        let _ = std::fs::remove_file(&p_str);
        let _ = std::fs::remove_dir_all(&p_id); // v10: DB は directory
        let _ = std::fs::remove_file(&p_id);
    }

    #[test]
    fn by_id_untie_equivalence_sync() {
        let p = tmp("by_id_untie");
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_himo("year", ValueType::Number, 100);
        let year_id = eng.himo_id("year").unwrap() as u16;
        let e = eng.entity().unwrap();
        eng.tie_to_by_id(e, year_id, 2026);
        assert_eq!(eng.query(&[("year", 2026)]).len(), 1);
        eng.untie_by_id(e, year_id);
        assert_eq!(eng.query(&[("year", 2026)]).len(), 0);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    #[should_panic]
    fn by_id_out_of_range_panics() {
        let p = tmp("by_id_oor");
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_himo("year", ValueType::Number, 100);
        let e = eng.entity().unwrap();
        // himo_id = 99 だが define されたのは 1 つだけ (id=0)。
        // debug build では debug_assert! のメッセージ、 release では array indexing の
        // out-of-bounds panic で落ちる。 どちらでも panic することだけ確認。
        eng.tie_to_by_id(e, 99, 0);
        let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
        let _ = std::fs::remove_file(&p);
    }

    /// growable backing で作った DB を open_standalone で再オープン
    /// できる (open_standalone は MmapMut で開くが、 ファイル format は
    /// 同一なので問題ないはず)。
    #[test]
    fn growable_then_open_standalone() {
        let dir = tmp("growable_reopen");
        {
            let mut eng = Engine::create_growable(&dir).unwrap();
            eng.define_himo("score", ValueType::Number, 0);
            for i in 0..10u32 {
                let e = eng.entity().unwrap();
                eng.tie(e, "score", i * 10);
            }
            eng.flush().unwrap();
        }
        let eng2 = Engine::open_standalone(&dir).unwrap();
        eng2.rebuild();
        assert_eq!(eng2.query(&[("score", 30)]).len(), 1);
        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    // ──── 0.9.0: ensure_himo_dynamic (&self himo definition) ────

    /// `Arc<Engine>` (create_concurrent) から 4 thread 並行で
    /// `ensure_himo_dynamic` を叩く。 同名 → 同 hid (idempotent)、 hid 重複
    /// なし、 動的定義 himo の tie_to_by_id / get_by_id round-trip を確認。
    #[test]
    fn ensure_himo_dynamic_concurrent_idempotent() {
        let dir = tmp("himo_dyn_conc");
        let eng = Engine::create_concurrent(&dir).unwrap();

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();
        for t in 0..4u32 {
            let e = eng.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut shared_ids = Vec::new();
                let mut own_ids = Vec::new();
                for i in 0..8u32 {
                    // 全 thread 共通名 (奇偶 2 種) と thread 固有名を混ぜる
                    let shared = e
                        .ensure_himo_dynamic(
                            &format!("dyn_shared_{}", i % 2),
                            ValueType::Number,
                            0,
                        )
                        .unwrap();
                    let own = e
                        .ensure_himo_dynamic(
                            &format!("dyn_own_{}_{}", t, i),
                            ValueType::Number,
                            0,
                        )
                        .unwrap();
                    shared_ids.push((i % 2, shared));
                    own_ids.push(own);
                }
                (shared_ids, own_ids)
            }));
        }

        let mut shared_by_name: std::collections::HashMap<u32, u16> =
            std::collections::HashMap::new();
        let mut all_own: Vec<u16> = Vec::new();
        for h in handles {
            let (shared_ids, own_ids) = h.join().unwrap();
            for (name_key, hid) in shared_ids {
                // idempotent: 同名は全 thread / 全呼び出しで同じ hid
                if let Some(prev) = shared_by_name.insert(name_key, hid) {
                    assert_eq!(prev, hid, "same name resolved to different hids");
                }
            }
            all_own.extend(own_ids);
        }

        // 固有名 4 thread × 8 個 + 共有名 2 個 = 34 hid、 重複なし
        let mut everything: Vec<u16> = all_own.clone();
        everything.extend(shared_by_name.values().copied());
        let total = everything.len();
        assert_eq!(total, 4 * 8 + 2);
        everything.sort_unstable();
        everything.dedup();
        assert_eq!(everything.len(), total, "duplicate hid assigned");
        assert_eq!(eng.himo_count(), 34);

        // 定義済み名の再呼び出しは lock-free fast path で同 hid
        let again = eng
            .ensure_himo_dynamic("dyn_shared_0", ValueType::Number, 0)
            .unwrap();
        assert_eq!(again, shared_by_name[&0]);

        // 動的定義した himo で tie_to_by_id / get_by_id round-trip
        let hid = eng
            .ensure_himo_dynamic("dyn_roundtrip", ValueType::Number, 0)
            .unwrap();
        let e0 = eng.entity().unwrap();
        eng.tie_to_by_id(e0, hid, 42);
        assert_eq!(eng.get_by_id(e0, hid), Some(42));

        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }

    /// `ensure_himo_dynamic_in`: named table への lazy 定義も `&self` で通り、
    /// 旧 `define_himo_in` と同じ attach semantics になることを確認。
    #[test]
    fn ensure_himo_dynamic_in_attaches_to_table() {
        let dir = tmp("himo_dyn_in");
        let mut eng = Engine::create_standalone(&dir).unwrap();
        eng.define_table("users", 100).unwrap();
        let eng = std::sync::Arc::new(eng);

        let hid = eng
            .ensure_himo_dynamic_in("users", "age", ValueType::Number, 0)
            .unwrap();
        // idempotent: 2 回目は同 hid
        let hid2 = eng
            .ensure_himo_dynamic_in("users", "age", ValueType::Number, 0)
            .unwrap();
        assert_eq!(hid, hid2);
        // full name で引ける + table attach 済み
        assert_eq!(eng.himo_id("users.age"), Some(hid as usize));
        let e = eng.entity_in("users").unwrap();
        eng.tie_to_by_id(e, hid, 30);
        assert_eq!(eng.get_by_id(e, hid), Some(30));

        // 未定義 table は Err
        assert!(eng
            .ensure_himo_dynamic_in("nope", "x", ValueType::Number, 0)
            .is_err());

        let _ = std::fs::remove_dir_all(&dir); // v10: DB は directory
        let _ = std::fs::remove_file(&dir);
    }
}

/// v10 Phase 2: sidecar の置き場 (DB directory の中) と 1 ファイル DB からの移行。
#[cfg(all(test, not(target_arch = "wasm32")))]
mod v10_dir_tests {
    use super::*;
    use crate::db_files;

    const LEGACY_TEST_MAX_HIMOS: u32 = 2048;

    fn tmp(name: &str) -> String {
        let path = format!("/tmp/enchu_v10dir_{name}.db");
        for base in [path.clone(), format!("{path}.packed"), format!("{path}.dst"), format!("{path}.copy")] {
            let _ = std::fs::remove_dir_all(&base);
            let _ = std::fs::remove_file(&base);
            for n in db_files::ALL {
                let _ = std::fs::remove_file(db_files::legacy_path_for(&base, n));
            }
        }
        path
    }

    /// table 1 つ + himo `n` + entity 3 つを書いて tables を persist した WAL 付き DB。
    fn seed(path: &str) -> Vec<(enchudb_oplog::EntityId, u32)> {
        seed_with(path, None)
    }

    /// `reserve` = entity reservation (None は既定 = 2^28)。 legacy fixture を作るときは
    /// `Some(cap)` (v8 / v9 は reservation の概念が無く、 EntitySet が cap 基準)。
    fn seed_with(path: &str, reserve: Option<u32>) -> Vec<(enchudb_oplog::EntityId, u32)> {
        // max_himos 2048 → v10 の header は可変長 (12 KB)。 legacy 化するときに 4096 へ切り詰めて
        // 「固定 4096 header の v8 / v9」 を再現する (実 DB = sinfohub の shape)。
        let mut eng = Engine::create_growable_opts(
            path,
            GrowableOptions {
                max_entities: 1024,
                max_himos: LEGACY_TEST_MAX_HIMOS,
                vocab_data_size: 1 << 20,
                content_data_size: Some(1 << 20),
                reserve_entities: reserve,
                ..Default::default()
            },
        )
        .unwrap();
        eng.define_table("widgets", 100).unwrap();
        eng.define_himo_in("widgets", "n", ValueType::Number, 100).unwrap();
        let mut rows = Vec::new();
        for i in 0..3u32 {
            let e = eng.entity_in("widgets").unwrap();
            eng.tie(e, "widgets.n", 10 + i);
            rows.push((e, 10 + i));
        }
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        drop(eng);
        // WAL 経路で一度開いて `oplog` を作る (standalone create は WAL を持たない)
        let eng = Engine::open(path).unwrap();
        eng.flush_writes();
        drop(eng);
        rows
    }

    fn assert_seeded(eng: &Engine, rows: &[(enchudb_oplog::EntityId, u32)]) {
        assert_eq!(eng.entity_count(), rows.len() as u32);
        for (e, v) in rows {
            assert_eq!(eng.get(*e, "widgets.n"), Some(*v));
        }
        let tables: Vec<String> = eng.list_user_tables().into_iter().map(|t| t.1).collect();
        assert!(tables.iter().any(|t| t == "widgets"), "tables: {tables:?}");
    }

    #[test]
    fn sidecars_live_inside_db_dir() {
        let path = tmp("inside");
        let rows = seed(&path);
        for n in [db_files::TABLES, db_files::OPLOG, db_files::LOCK] {
            assert!(db_files::path_for(&path, n).exists(), "{n} should be inside the DB dir");
            assert!(!db_files::legacy_path_for(&path, n).exists(), "{n} must not be beside the dir");
        }
        // reopen で同じ sidecar を読む
        let eng = Engine::open(&path).unwrap();
        assert_seeded(&eng, &rows);
    }

    #[test]
    fn open_missing_dir_leaves_no_residue() {
        let path = tmp("missing");
        let err = Engine::open(&path).err().expect("open of a missing DB must fail");
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
        assert!(!std::path::Path::new(&path).exists(), "open must not create an empty DB dir");
        // 1 ファイル (旧 format) は migrate への誘導
        std::fs::write(&path, b"ECDB-not-really").unwrap();
        let err = Engine::open(&path).err().unwrap();
        assert!(err.to_string().contains("migrate_v9_to_v10"), "{err}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn copy_db_dir_clones_sidecars_but_not_lock() {
        let path = tmp("copy");
        let rows = seed(&path);
        let dst = format!("{path}.copy");
        copy_db_dir(std::path::Path::new(&path), std::path::Path::new(&dst)).unwrap();
        assert!(db_files::path_for(&dst, db_files::TABLES).exists());
        assert!(db_files::path_for(&dst, db_files::OPLOG).exists());
        assert!(!db_files::path_for(&dst, db_files::LOCK).exists(), "lock must not be cloned");
        let eng = Engine::open(&dst).unwrap();
        assert_seeded(&eng, &rows);
        drop(eng);

        let seg_only = format!("{path}.dst");
        copy_db_segments(std::path::Path::new(&path), std::path::Path::new(&seg_only)).unwrap();
        assert!(!db_files::path_for(&seg_only, db_files::TABLES).exists());
        assert!(!db_files::path_for(&seg_only, db_files::OPLOG).exists());
        assert!(std::path::Path::new(&seg_only).join("header.seg").exists());
        assert!(std::path::Path::new(&seg_only).join("himo/0000.seg").exists());
    }

    #[test]
    fn snapshot_export_puts_sidecars_in_target_dir() {
        let path = tmp("snap");
        let rows = seed(&path);
        let eng = Engine::open(&path).unwrap();
        let dst = format!("{path}.dst");
        let files = eng.snapshot_export(&dst).unwrap();
        drop(eng);
        assert_eq!(files.oplog.as_deref(), Some(db_files::path_for(&dst, db_files::OPLOG).to_str().unwrap()));
        assert!(db_files::path_for(&dst, db_files::TABLES).exists());
        assert!(!db_files::path_for(&dst, db_files::LOCK).exists());
        let eng = Engine::open(&dst).unwrap();
        assert_seeded(&eng, &rows);
    }

    /// v10 DB を pack して header の version を `version` に書き戻し、 旧 sidecar 配置
    /// (`{file}.tables` …) を作る = 旧 binary が残した 1 ファイル DB の再現。
    fn make_legacy_single_file(path: &str, version: u32) -> String {
        let packed = format!("{path}.packed");
        Engine::pack_dir(path, std::path::Path::new(&packed)).unwrap();
        // v10 packed (可変長 header) → legacy (固定 4096 header): header の余分を切り落とす
        let v10_hs = header_size_for(LEGACY_TEST_MAX_HIMOS);
        assert!(v10_hs > HEADER_SIZE, "test must exercise the variable-length header");
        let bytes = std::fs::read(&packed).unwrap();
        let mut legacy = Vec::with_capacity(bytes.len() - (v10_hs - HEADER_SIZE));
        legacy.extend_from_slice(&bytes[..HEADER_SIZE]);
        legacy.extend_from_slice(&bytes[v10_hs..]);
        legacy[H_VERSION..H_VERSION + 4].copy_from_slice(&version.to_le_bytes());
        if version < FILE_VERSION_LEGACY_V9 {
            legacy[H_CELL_VERSION..H_CELL_VERSION + 4].copy_from_slice(&0u32.to_le_bytes());
        }
        write_header_crc(&mut legacy[..HEADER_SIZE]);
        std::fs::write(&packed, &legacy).unwrap();
        for n in [db_files::TABLES, db_files::OPLOG] {
            std::fs::copy(db_files::path_for(path, n), db_files::legacy_path_for(&packed, n)).unwrap();
        }
        std::fs::write(db_files::legacy_path_for(&packed, db_files::SCHEMA), b"schema-bytes").unwrap();
        packed
    }

    fn migrate_roundtrip(name: &str, version: u32) {
        let path = tmp(name);
        let rows = seed_with(&path, Some(1024));
        let packed = make_legacy_single_file(&path, version);
        let _ = std::fs::remove_dir_all(&path);

        // 旧 1 ファイルはこの build では開けない
        let err = Engine::open(&packed).err().unwrap();
        assert!(err.to_string().contains("migrate_v9_to_v10"), "{err}");

        let dst = format!("{path}.dst");
        Engine::migrate_v9_to_v10(&packed, &dst).unwrap();
        for n in [db_files::TABLES, db_files::OPLOG, db_files::SCHEMA] {
            assert!(db_files::path_for(&dst, n).exists(), "{n} should be migrated into the dir");
        }
        assert_eq!(std::fs::read(db_files::path_for(&dst, db_files::SCHEMA)).unwrap(), b"schema-bytes");
        assert!(!db_files::path_for(&dst, db_files::LOCK).exists());
        let eng = Engine::open(&dst).unwrap();
        assert_seeded(&eng, &rows);
        assert!(!eng.has_cell_version());
        // 元は不変
        assert!(std::path::Path::new(&packed).is_file());
        assert!(db_files::legacy_path_for(&packed, db_files::TABLES).exists());
    }

    #[test]
    fn migrate_v9_single_file_to_dir() {
        migrate_roundtrip("mig9", FILE_VERSION_LEGACY_V9);
    }

    #[test]
    fn migrate_v8_single_file_to_dir() {
        migrate_roundtrip("mig8", FILE_VERSION_LEGACY_V8);
    }

    #[test]
    fn migrate_rejects_older_than_v8() {
        let path = tmp("mig7");
        seed_with(&path, Some(1024));
        let packed = make_legacy_single_file(&path, FILE_VERSION_LEGACY_V7);
        let err = Engine::migrate_v9_to_v10(&packed, &format!("{path}.dst")).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        assert!(err.to_string().contains("version 7"), "{err}");
    }

    fn tiny(path: &str, cap: u32, reserve: u32) -> Engine {
        Engine::create_growable_opts(
            path,
            GrowableOptions {
                max_entities: cap,
                max_himos: 16,
                vocab_data_size: 64 * 1024,
                content_data_size: Some(64 * 1024),
                reserve_entities: Some(reserve),
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn fill(eng: &Engine) -> Vec<enchudb_oplog::EntityId> {
        let mut v = Vec::new();
        while let Ok(e) = eng.entity() {
            v.push(e);
        }
        v
    }

    #[test]
    fn grow_entity_cap_extends_allocation_and_persists() {
        let path = tmp("grow");
        let mut eng = tiny(&path, 16, 64);
        eng.define_himo("n", ValueType::Number, 100);
        assert_eq!((eng.max_entities(), eng.reserve_entities()), (16, 64));
        let first = fill(&eng);
        assert_eq!(first.len(), 16, "cap 16 must hand out exactly 16");
        assert!(eng.entity().is_err(), "17th allocation must fail before grow");
        for &e in &first {
            eng.tie(e, "n", 1);
        }

        assert_eq!(eng.grow_entity_cap(40).unwrap(), 40);
        assert_eq!(eng.grow_entity_cap(20).unwrap(), 40, "shrinking is a no-op");
        let more = fill(&eng);
        assert_eq!(more.len(), 24);
        for &e in &more {
            eng.tie(e, "n", 2);
        }
        let err = eng.grow_entity_cap(65).err().expect("beyond reservation must fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{err}");
        assert_eq!(eng.grow_entity_cap(64).unwrap(), 64);
        eng.flush().unwrap();
        drop(eng);

        let eng = Engine::open_standalone(&path).unwrap();
        assert_eq!((eng.max_entities(), eng.reserve_entities()), (64, 64));
        assert_eq!(eng.entity_count(), 40);
        for &e in &first {
            assert_eq!(eng.get(e, "n"), Some(1));
        }
        for &e in &more {
            assert_eq!(eng.get(e, "n"), Some(2));
        }
        let rest = fill(&eng);
        assert_eq!(rest.len(), 24, "grown cap survives reopen");
    }

    #[test]
    fn free_stack_keeps_working_across_grow() {
        let path = tmp("grow_free");
        let eng = tiny(&path, 8, 32);
        let ids = fill(&eng);
        assert_eq!(ids.len(), 8);
        eng.delete(ids[3]);
        eng.delete(ids[5]);
        assert_eq!(eng.grow_entity_cap(32).unwrap(), 32);
        // 伸びた分 (24) + free stack から戻る 2
        let got = fill(&eng);
        assert_eq!(got.len(), 26);
        assert!(got.contains(&ids[3]) && got.contains(&ids[5]), "freed slots must be reused: {got:?}");
    }

    #[test]
    fn define_table_can_use_grown_cap() {
        let path = tmp("grow_table");
        let mut eng = tiny(&path, 8, 64);
        eng.define_table("a", 8).unwrap();
        let err = eng.define_table("b", 8).err().expect("no room for a second table at cap 8");
        assert!(err.contains("max_entities"), "{err}");
        eng.grow_entity_cap(64).unwrap();
        eng.define_table("b", 8).unwrap();
        let e = eng.entity_in("b").unwrap();
        assert!(enchudb_oplog::eid_local(e) >= 8);
    }

    #[test]
    fn migrated_legacy_db_gets_default_reservation_and_grows() {
        let path = tmp("grow_mig");
        let rows = seed_with(&path, Some(1024));
        let packed = make_legacy_single_file(&path, FILE_VERSION_LEGACY_V9);
        let _ = std::fs::remove_dir_all(&path);
        let dst = format!("{path}.dst");
        Engine::migrate_v9_to_v10(&packed, &dst).unwrap();
        let eng = Engine::open(&dst).unwrap();
        assert_seeded(&eng, &rows);
        assert_eq!(eng.max_entities(), 1024);
        assert_eq!(eng.reserve_entities(), default_reserve_entities(1024));
        // EntitySet も relayout 済み (bitset 容量 = reservation) なので伸ばせる
        eng.grow_entity_cap(4096).unwrap();
        let n_before = eng.entity_count();
        let mut got = 0;
        for _ in 0..2000 {
            eng.entity_in("widgets").ok();
            got += 1;
        }
        assert!(got > 0);
        assert!(eng.entity_count() > n_before);
    }

    #[test]
    fn table_auto_grows_into_free_eid_space_and_persists() {
        let path = tmp("tbl_grow");
        let mut eng = tiny(&path, 64, 128);
        eng.define_table("a", 4).unwrap();
        eng.define_himo_in("a", "n", ValueType::Number, 100).unwrap();
        // a の直後に b を切る = a は連続では伸びられず、 b の後ろに extent を足すしかない
        eng.define_table("b", 4).unwrap();
        let mut ids = Vec::new();
        for i in 0..10u32 {
            let e = eng.entity_in("a").expect("auto-grow must hand out rows beyond the first range");
            eng.tie(e, "a.n", i);
            ids.push(e);
        }
        let ext = eng.table_eid_extents("a").unwrap();
        assert_eq!(ext[0], (0, 4));
        assert!(ext.len() >= 2, "{ext:?}");
        assert_eq!(ext[1].0, 8, "second extent must start after table b [4, 8): {ext:?}");
        let u = eng.table_eid_usage("a").unwrap();
        assert_eq!(u.live, 10);
        assert!(u.capacity >= 10);
        assert_eq!(eng.table_eid_range("a").unwrap(), (0, ext.last().unwrap().1), "hull");
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        drop(eng);

        let eng = Engine::open_standalone(&path).unwrap();
        assert_eq!(eng.table_eid_extents("a").unwrap(), ext, "extents survive reopen (EXT1 block)");
        for (i, &e) in ids.iter().enumerate() {
            assert_eq!(eng.get(e, "a.n"), Some(i as u32));
        }
        // reopen 後の払出は live eid を再利用しない (next_local が bitmap から復元される)
        let fresh = eng.entity_in("a").unwrap();
        assert!(!ids.contains(&fresh), "live row handed out again after reopen");
        // b は自分の range をそのまま使える
        let b = eng.entity_in("b").unwrap();
        assert!((4..8).contains(&enchudb_oplog::eid_local(b)));

        // cap まで使い切ると exhausted、 cap を伸ばせば続く
        while eng.entity_in("a").is_ok() {}
        let err = eng.entity_in("a").unwrap_err();
        assert!(err.contains("exhausted"), "{err}");
        assert_eq!(eng.remaining_eid_capacity(), 0);
        eng.grow_entity_cap(128).unwrap();
        assert!(eng.entity_in("a").is_ok(), "after grow_entity_cap the table must auto-grow again");
    }

    #[test]
    fn deleted_row_in_second_extent_is_reused() {
        let path = tmp("tbl_grow_reuse");
        let mut eng = tiny(&path, 64, 64);
        eng.define_table("a", 2).unwrap();
        eng.define_table("b", 2).unwrap();
        let ids: Vec<_> = (0..6).map(|_| eng.entity_in("a").unwrap()).collect();
        let victim = ids[4]; // 2 本目の extent に居る
        assert!(enchudb_oplog::eid_local(victim) >= 4);
        eng.delete(victim);
        let again = eng.entity_in("a").unwrap();
        assert_eq!(again, victim, "free slot in a later extent must be reused before growing");
    }

    #[test]
    fn grow_table_explicit_and_exhaustion_message() {
        let path = tmp("tbl_grow_explicit");
        let mut eng = tiny(&path, 16, 16);
        eng.define_table("a", 4).unwrap();
        assert_eq!(eng.grow_table("a", 4).unwrap(), 8);
        assert_eq!(eng.table_eid_extents("a").unwrap(), vec![(0, 4), (4, 8)]);
        assert_eq!(eng.grow_table("a", 100).unwrap(), 16, "capped at the entity cap");
        let err = eng.grow_table("a", 1).unwrap_err();
        assert!(err.contains("grow_entity_cap"), "{err}");
        assert!(eng.grow_table("nope", 1).is_err());
    }

    /// 実 DB での確認用 (手動)。 `ENCHU_LEGACY_FIXTURE=/path/to/enchu.db` (1 ファイル、
    /// sidecar は隣) を与えると migrate → open_readonly して要約を出す。 無指定なら no-op。
    #[test]
    fn migrate_real_fixture_if_provided() {
        let Ok(src) = std::env::var("ENCHU_LEGACY_FIXTURE") else { return };
        let dst = tmp("fixture") ;
        let t0 = std::time::Instant::now();
        Engine::migrate_v9_to_v10(&src, &dst).unwrap();
        let eng = Engine::open_readonly(&dst).unwrap();
        let tables: Vec<String> = eng.list_user_tables().into_iter().map(|t| t.1).collect();
        eprintln!(
            "[fixture] migrated in {:?}: entities={} himos={} tables={:?} cell_version={}",
            t0.elapsed(), eng.entity_count(), eng.himo_count(), tables, eng.has_cell_version()
        );
        assert!(eng.entity_count() > 0);
    }
}
