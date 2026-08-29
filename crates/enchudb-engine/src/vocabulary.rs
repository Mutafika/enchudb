//! Vocabulary — ユニーク値辞書。Symbol の値を value_id (u32) に変換。
//! 3つのRegion（data, offsets, index）で構成。

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const MAGIC: [u8; 4] = [b'V', b'O', b'C', b'1'];
const HEADER: usize = 16;
/// data header byte 12 に書く「index は data と consistent」 マーカー。
/// 0 = dirty (rebuild 要)、 1 = clean (rebuild skip 可)。
/// 残り 13-15 byte は reserved。
const CLEAN_FLAG_OFF: usize = 12;
const INDEX_MAGIC: [u8; 4] = [b'V', b'I', b'X', b'3'];
/// #123: slot 選択を hash 下位ビット (`h & mask`) から **上位ビット** に変えた前の index。
/// 0.14 以前の DB はこの magic を持つので、 open 時に in-place migrate する
/// (`migrate_legacy_index`)。
const INDEX_MAGIC_V2: [u8; 4] = [b'V', b'I', b'X', b'2'];
const INDEX_HEADER: usize = 16;
const INDEX_SLOT_SIZE: usize = 13;

pub struct Vocabulary {
    data: Region,
    offsets: Region,
    index: Region,
    /// #77-H1: readonly open で index が dirty (clean_flag≠1) な場合、 共有
    /// mmap の index を書き換えずに **heap 上のここへ**再構築する。 Some の
    /// とき lookup はこちらを参照。 writer open では常に None。
    /// 旧実装は readonly でも `rebuild_index` が共有 index をゼロクリアして
    /// おり、 併走中の writer の index entry を恒久消失させていた。
    ///
    /// #127: 形式は index layout の byte 複製 (`Box<[u8]>`、 index_cap × 13B) から
    /// **count 比例の compact 形式** `(fxhash(value), vid)` sorted に変更。 旧形式は
    /// 確保こそ calloc (仮想) だが、 #123 で hash が一様分散になったため rebuild が
    /// shadow の全ページに live slot を書いて **index_cap 比例の anon RSS** を
    /// Engine 寿命の間占有していた (dirty DB の readonly open 1 回ごと。 naruhodo
    /// 1GB VPS の boot +~300MB の正体)。 lookup は hash の binary search + 同 hash
    /// 内の値比較で、 旧 probe と同じ「最小 vid が勝つ」 解決を保つ。
    shadow_index: Option<Vec<(u64, u32)>>,
    /// #77-M1: disk 上の clean_flag のキャッシュ。 flush() が 1 を書いた後の
    /// 最初の insert で 0 に戻すための判定に使う (旧実装は open 時の 1 回
    /// しか 0 に倒さず、 flush 後の追加 write 中 crash で破損 index を
    /// 次 open が無検証採用した)。
    clean_on_disk: std::sync::atomic::AtomicBool,
    /// #101: この instance の load で rebuild_index が走ったか (= dirty open だったか)。
    /// 観測専用 (graceful close の regression test / 診断)。 init (新規) は false。
    pub rebuilt_on_load: bool,
    count: AtomicU32,
    data_end: AtomicU32,
    max_entries: u32,
    index_cap: u32,
}

unsafe impl Sync for Vocabulary {}
unsafe impl Send for Vocabulary {}

/// #123: slot は **hash の上位ビット**から採る。
///
/// `fxhash` は乗算で終わる (`h = (h.rotate_left(5) ^ word).wrapping_mul(SEED)`) ため、
/// **積の下位 k bit は両オペランドの下位 k bit だけで決まり**、 上位側の entropy が下位に
/// 伝播しない。 旧実装 (VIX2) は `h & mask` で下位を採っていたので、 構造の似たキーが同一
/// slot に集まって linear probe のクラスタが伸びていた (実例: `第10条` / `第11条` / `第12条`
/// は 1 word ちょうどで `h = word * SEED`、 下位 32 bit が完全一致)。 上位ビットなら乗算の
/// 桁上がりが載る。
///
/// `index_cap` は 2^n (open 時に検証済み、 `validate_header`)。 cap == 1 の退化ケースは 0。
#[inline]
fn home_slot(h: u64, index_cap: u32) -> usize {
    let bits = index_cap.trailing_zeros();
    if bits == 0 { 0 } else { (h >> (64 - bits)) as usize }
}

/// VIX2 (0.14 以前) の slot 選択。 index の in-place migration で **旧 slot を正確に消す**
/// ためだけに残す。
#[inline]
fn home_slot_legacy(h: u64, index_cap: u32) -> usize {
    (h & (index_cap - 1) as u64) as usize
}

impl Vocabulary {
    pub fn data_region_size(data_size: usize) -> usize { data_size.max(HEADER) }
    pub fn offsets_region_size(max_entries: u32) -> usize { (max_entries as usize) * 8 }
    pub fn index_region_size(index_cap: u32) -> usize { INDEX_HEADER + (index_cap as usize) * INDEX_SLOT_SIZE }

    /// 新規領域を初期化。
    ///
    /// `data` 領域 (variable cluster、 v3 layout で末尾) の MAGIC + count
    /// + data_end は **書かない** — `insert` の初回 append で初めて書く
    /// (lazy init)。 これで growable backing の `initial_commit` が
    /// data 領域の末尾までコミットしなくて済むようになる (Phase B Step 2)。
    /// `index` 領域 (固定 cluster) は eager init のまま — 固定上限なので
    /// initial_commit に含めるオーバーヘッドが小さい。
    pub fn init(data: Region, offsets: Region, index: Region, max_entries: u32, index_cap: u32) -> Self {
        let index_cap = index_cap.next_power_of_two();

        // index header — eager init は固定 cluster なのでコスト低
        index.write_at(0, &INDEX_MAGIC);
        index.write_at(4, &index_cap.to_le_bytes());

        Self {
            data, offsets, index,
            shadow_index: None,
            clean_on_disk: std::sync::atomic::AtomicBool::new(false),
            rebuilt_on_load: false,
            count: AtomicU32::new(0),
            data_end: AtomicU32::new(HEADER as u32),
            max_entries, index_cap,
        }
    }

    /// 既存領域をロード。ハッシュインデックスを再構築する。
    ///
    /// data の先頭 4 バイトが MAGIC でない (= 全 0 = lazy fresh、 一度も
    /// insert されてない) 場合は count=0 / data_end=HEADER の fresh
    /// state を返す。 これで `insert` が遅延書き込みする MAGIC を待たずに
    /// open できる。
    pub fn load(data: Region, offsets: Region, index: Region, readonly: bool) -> Self {
        let dm = data.slice();
        let is_fresh = dm[0..4] != MAGIC;
        let (count, data_end, clean_flag) = if is_fresh {
            (0u32, HEADER as u32, 0u32)
        } else {
            (
                u32::from_le_bytes(dm[4..8].try_into().unwrap()),
                u32::from_le_bytes(dm[8..12].try_into().unwrap()),
                u32::from_le_bytes(dm[CLEAN_FLAG_OFF..CLEAN_FLAG_OFF + 4].try_into().unwrap()),
            )
        };

        let om = offsets.slice();
        let max_entries = (om.len() / 8) as u32;

        let xm = index.slice();
        let index_cap = u32::from_le_bytes(xm[4..8].try_into().unwrap());
        // #123: VIX2 = slot が hash **下位**ビットの旧 index。 clean_flag が立っていても
        // slot 関数が違うので再利用できない (lookup が全部 miss する) → 必ず作り直す。
        let legacy_index = !is_fresh && xm[0..4] == INDEX_MAGIC_V2;
        let needs_rebuild = !is_fresh && (clean_flag != 1 || legacy_index);

        let mut v = Self {
            data, offsets, index,
            shadow_index: None,
            clean_on_disk: std::sync::atomic::AtomicBool::new(clean_flag == 1 && !legacy_index),
            rebuilt_on_load: needs_rebuild,
            count: AtomicU32::new(count),
            data_end: AtomicU32::new(data_end),
            max_entries, index_cap,
        };
        // clean_flag == 1 なら前回 graceful close で「index と data は consistent」と
        // 保証されているので rebuild skip。 それ以外 (= 初期 / crash 後 / 未対応の旧 DB) は
        // 全部 rebuild。
        // #77-H1: readonly open は共有 mmap を書き換えず heap の shadow へ。
        if needs_rebuild {
            if readonly {
                // #127: count 比例の compact shadow を data/offsets (= ground truth)
                // から構築する。 on-disk index は VIX2 / dirty のまま残す (readonly は
                // 共有 mmap を書かない)。 構築元が data 直なので、 旧 shadow rebuild の
                // 「破損 slot (vid >= max_entries) 読み飛ばし」 は不要 — count 側だけ
                // 破損 header 対策で max_entries に clamp する (旧実装は clamp なしで
                // read_value が offsets を溢れる余地があった、 安全側の強化)。
                let count = v.count.load(Ordering::Relaxed).min(v.max_entries);
                let mut shadow: Vec<(u64, u32)> = Vec::with_capacity(count as usize);
                for id in 0..count {
                    let value = read_value(&v.offsets, &v.data, id);
                    shadow.push((fxhash(value), id));
                }
                // (hash, vid) 昇順 = 同 hash 内は vid 昇順。 lookup の線形走査が
                // 最小 vid から当たるので、 旧 probe の dup 解決 (先着 vid) と一致。
                shadow.sort_unstable();
                v.shadow_index = Some(shadow);
            } else {
                // #123: legacy は旧 slot を消してから (消さないと no-clear rebuild (#92) の
                // 上に新 slot で二重に載って占有率が最大 2 倍になる)。
                if legacy_index {
                    v.migrate_legacy_index();
                }
                v.rebuild_index();
                if legacy_index {
                    v.index.write_at(0, &INDEX_MAGIC);
                    v.index.mark_dirty(0, 4);
                }
            }
        }
        v
    }

    /// #123: VIX2 index (slot = hash 下位ビット) の entry を **正確に消す** (writer 専用)。
    ///
    /// 手順: ① 旧 probe (`home_slot_legacy`) で各 live id の slot を見つけ **tombstone
    /// (flag = 3)** にして位置を記録する。 その場で 0 にすると後続 id の probe chain が
    /// 途切れて取り残しが出るため、 chain を保つ中間状態を使う。 ② 記録した tombstone を
    /// 0 に戻す。 この後 `rebuild_index` が新 slot 関数で再挿入する。
    ///
    /// index 全域の zero-fill は sparse ページを 1 枚残らず物理化する (#92 の回帰) ので
    /// 採らない。 clear / insert とも O(count) で、 触るのは live slot が載るページのみ。
    /// flag = 3 は open 時の単一スレッド区間だけに存在し、 ② で必ず消える。
    fn migrate_legacy_index(&mut self) {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return;
        }
        let index_cap = self.index_cap;
        let max_entries = self.max_entries;
        let mask = (index_cap - 1) as u64;
        let Self { index, offsets, data, .. } = self;
        let xm = index.as_mut_slice();

        let mut tombstones: Vec<usize> = Vec::new();
        for id in 0..count {
            let value = read_value(offsets, data, id);
            let h = fxhash(value);
            let mut idx = home_slot_legacy(h, index_cap);
            for _ in 0..index_cap as usize {
                let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
                if xm[off] == 0 {
                    break; // 旧 index に載っていない (torn write 等) — rebuild が入れ直す
                }
                let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
                let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                // #92 と同じ predicate: vid >= max_entries の破損 slot は read_value が
                // offsets を溢れるので触らない。
                // flag == 3 は「前回の migration が crash で中断して残した tombstone」。
                // 同じ entry なのでここで回収し、 ② で 0 に戻す (migration を idempotent に
                // する。 index 全域の scan は sparse ページ全物理化 = #92 の回帰なので採らない)。
                if (xm[off] == 1 || xm[off] == 3)
                    && slot_hash == h
                    && vid < max_entries
                    && read_value(offsets, data, vid) == value
                {
                    xm[off] = 3;
                    tombstones.push(off);
                    break;
                }
                idx = ((idx as u64 + 1) & mask) as usize;
            }
        }
        for off in tombstones {
            xm[off..off + INDEX_SLOT_SIZE].fill(0);
        }
    }

    /// ハッシュインデックスを data/offsets から再構築する (writer 専用、
    /// 共有 mmap 上の index を in-place で書き直す)。
    ///
    /// #83: `index` を **排他借用** (`&mut self.index` → `as_mut_slice`) して可変
    /// slice を得る。 open 時の単一スレッド実行なので排他が保証され、 `slice_mut(&self)`
    /// のような aliasing 参照を作らない。 `offsets`/`data` は分割借用で不変参照する。
    fn rebuild_index(&mut self) {
        let count = self.count.load(Ordering::Relaxed);
        let index_cap = self.index_cap;
        let max_entries = self.max_entries;
        let Self { index, offsets, data, .. } = self;
        // v10: rebuild は任意 slot に `&mut [u8]` で書くので index segment を全域 commit
        // しておく (open 時 1 回、 index が dirty だった場合のみ)。
        let _ = index.ensure_committed(index.len());
        Self::rebuild_index_into(offsets, data, count, index_cap, max_entries, index.as_mut_slice());
    }

    /// `xm` (index layout のバイト列、 mmap でも heap でも可) へ index を
    /// 再構築する。 open 時の単一スレッド実行前提なので atomic は使わない。
    ///
    /// #92 (#56 ③): **予約全域を zero-fill しない**。 旧実装は先頭で
    /// `for b in &mut xm[INDEX_HEADER..] { *b = 0; }` と index_cap×13B を全ゼロ
    /// 埋めしてから live entry を再挿入していた。 index region は fixed cluster で
    /// mmap 済みだが **sparse** (物理未確保) なので、 この全域書き込みが sparse
    /// ページを 1 枚残らず物理化 → live vocab 数と無関係に index_cap 比例の物理
    /// commit (writer) / RAM commit (readonly shadow) を起こしていた。
    ///
    /// 代わりに **既存の on-disk index の上へ live entry (id 0..count) を再挿入
    /// するだけ** にする (used-slot only touch)。 append-only vocab の count は
    /// 単調なので:
    /// - 通常の落ち方 (page cache 経由の drop / process::exit) では on-disk index は
    ///   data と consistent。 → 各 entry は自分の slot で dup 一致し **書き込みゼロ**
    ///   (触るのは live slot が載る数ページのみ)。
    /// - torn write で index が count より遅れていた (slot 欠落) 場合は空 slot へ
    ///   再挿入して self-heal。
    /// - 旧実装は全域 zero-fill で破損/不整合 slot を全 scrub していた。 no-clear では
    ///   既存 slot が残るので、 **vid >= max_entries の破損 slot** (実 insert は
    ///   `vid < max_entries` を assert するので通常あり得ないが、 bit-rot 等) を
    ///   `slot_hash == h` でも `get(vid)` を呼ばず読み飛ばす guard を入れる。 get が
    ///   offsets region を溢れて OOB するのを防ぐ = 旧 zero-fill と同じ安全性を復元。
    ///   lookup / index_insert も同じ predicate で一貫させる。
    ///
    /// #83: `&Self` ではなく必要な region (`offsets`/`data`) + scalar を直接受ける。
    /// これで呼び出し側が `index` を `&mut` 借用したまま (writer 経路) でも分割借用
    /// で呼べる。 heap shadow (readonly) 経路とも `xm: &mut [u8]` で共有できて DRY。
    fn rebuild_index_into(
        offsets: &Region,
        data: &Region,
        count: u32,
        index_cap: u32,
        max_entries: u32,
        xm: &mut [u8],
    ) {
        if count == 0 { return; }

        let mask = (index_cap - 1) as u64;
        for id in 0..count {
            let value = read_value(offsets, data, id);
            let h = fxhash(value);
            let mut idx = home_slot(h, index_cap); // #123
            loop {
                let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
                if xm[off] == 0 {
                    xm[off] = 1;
                    xm[off + 1..off + 9].copy_from_slice(&h.to_le_bytes());
                    xm[off + 9..off + 13].copy_from_slice(&id.to_le_bytes());
                    break;
                }
                let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
                if slot_hash == h {
                    let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                    // #92: vid >= max_entries の破損 slot は read_value(vid) が offsets region を
                    // 溢れて OOB するので dup 判定に使わず読み飛ばす (lookup / index_insert と
                    // 同じ predicate = 破損 slot の扱いを 3 経路で一貫させる)。
                    if vid < max_entries && read_value(offsets, data, vid) == value { break; } // 真の重複 (Leaf 二重 append 等)
                }
                idx = ((idx as u64 + 1) & mask) as usize;
            }
        }
    }

    /// 満杯なら **`u32::MAX` (予約 sentinel)** を返す (#59: panic しない)。
    pub fn get_or_insert(&self, value: &[u8]) -> u32 {
        if let Some(id) = self.lookup(value) { return id; }
        let id = self.insert(value);
        if id == u32::MAX {
            return u32::MAX;
        }
        // 並列挿入の競合チェック: 別スレッドが先に同じ値を挿入した場合、先着のidを使う
        if let Some(winner) = self.lookup(value) {
            if winner != id { return winner; }
        }
        id
    }

    #[inline]
    pub fn get(&self, id: u32) -> &[u8] {
        read_value(&self.offsets, &self.data, id)
    }

    #[inline]
    pub fn lookup(&self, value: &[u8]) -> Option<u32> {
        // #77-H1 / #127: readonly open で dirty だった場合は compact shadow を参照。
        // hash の partition_point → 同 hash 区間を vid 昇順に値比較 (probe と同じ
        // 「先着 vid が勝つ」 解決)。
        if let Some(shadow) = &self.shadow_index {
            let h = fxhash(value);
            let mut i = shadow.partition_point(|&(sh, _)| sh < h);
            while let Some(&(sh, vid)) = shadow.get(i) {
                if sh != h { break; }
                if self.get(vid) == value { return Some(vid); }
                i += 1;
            }
            return None;
        }
        let mask = (self.index_cap - 1) as u64;
        let xm: &[u8] = self.index.slice();
        let h = fxhash(value);
        let mut idx = home_slot(h, self.index_cap); // #123
        // #59: index が 100% 埋まると 「空 slot に当たる」 終了条件が成立せず、
        // 素の `loop` は **永久に回る** (= 満杯が hang になる。 embedded DB としては
        // panic と同じくらい悪い)。 走査上限を index_cap 回にする — 全 slot を見て
        // 見つからなければ不在。
        for _ in 0..self.index_cap as usize {
            let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
            if xm[off] == 0 { return None; }
            let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
            if slot_hash == h {
                let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                // #92: 実 insert は必ず vid < max_entries を assert する。 no-clear
                // rebuild で残りうる **vid >= max_entries の破損 slot** は get(vid) が
                // offsets region を溢れて OOB するので読み飛ばす。 max_entries は不変
                // なので atomic 不要・並行 insert の valid slot を skip しない (race 無)。
                if vid < self.max_entries && self.get(vid) == value { return Some(vid); }
            }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
        // 全 slot 走査して空きも一致も無かった = index 満杯かつ不在。
        None
    }

    /// 重複検査なしで常に新規 id を発行して append する。
    /// `ValueType::Leaf` (終端タグ・dedupe なし) の書き込み path で使う。
    /// 既に同じ bytes が登録済みでも気にせず新 id を払い出す。index_insert は走るが、
    /// 既存スロットがあれば early-return するため index 領域は dedup される副作用がある
    /// (data/offsets のみ完全に増分)。
    /// 今後 1 件も insert できないか (= 天井 hit)。
    pub fn is_full(&self) -> bool {
        self.count.load(Ordering::Relaxed) >= self.max_entries
    }

    /// 満杯なら **`u32::MAX` (予約 sentinel)** を返す (#59: panic しない)。
    pub fn insert(&self, value: &[u8]) -> u32 {
        let id = self.count.fetch_add(1, Ordering::Relaxed);
        // #122: vocab_max_entries が公開 knob になったので、 天井 hit を actionable に
        // する (#118 の `too many himos` と同じ扱い)。 既存 DB は header 焼き込みなので
        // 引き上げには rebuild が必要、 という点まで伝える。
        // #59: 天井 hit は 「想定内だが続行不能」。 embedded DB は他人の process に
        // 埋め込まれるので panic で host を殺してはいけない。 採番を巻き戻して
        // **予約 sentinel `u32::MAX`** を返し、 呼び出し側 (Engine) が fault として
        // 記録 + 報告し、 write を拒否する。 `u32::MAX` は元々 「無効値」 として
        // engine 側の guard が見ている値なので、 新しい規約を増やしていない。
        if id >= self.max_entries {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return u32::MAX;
        }
        let len = value.len() as u32;
        let offset = self.data_end.fetch_add(len, Ordering::Relaxed);
        // Growable backing: extend the file-backed window before
        // writing past the current commit. No-op for static backings.
        // We also grow the offsets region in case `id` advanced
        // past its committed footprint (offsets have id × 8 layout).
        // #167: commit を伸ばせなければ **書かずに諦める** (予約 sentinel を返す)。
        // 未 commit page への書き込みは (ディスク満杯なら) SIGBUS でプロセスごと
        // 落ちるので、 error を捨てて書き進めてはいけない。 採番は巻き戻す。
        if self.data.ensure_committed((offset + len) as usize).is_err()
            || self.offsets.ensure_committed(((id as usize) + 1) * 8).is_err()
        {
            self.count.fetch_sub(1, Ordering::Relaxed);
            return u32::MAX;
        }
        self.data.write_at(offset as usize, value);
        self.data.write_at(0, &MAGIC);
        let new_count = id + 1;
        let new_end = offset + len;
        self.data.write_at(4, &new_count.to_le_bytes());
        self.data.write_at(8, &new_end.to_le_bytes());
        self.data.mark_dirty(0, 12);
        // #77-M1: flush() が clean=1 を書いた後の最初の insert で 0 に戻す。
        // これが無いと flush 後の write 中 crash で、 次 open が部分 writeback
        // された index を rebuild なしで信用してしまう。
        if self.clean_on_disk.swap(false, Ordering::AcqRel) {
            self.data.write_at(CLEAN_FLAG_OFF, &0u32.to_le_bytes());
            self.data.mark_dirty(CLEAN_FLAG_OFF, 4);
        }
        self.data.mark_dirty(offset as usize, len as usize);
        let off_pos = (id as usize) * 8;
        self.offsets.write_at(off_pos, &offset.to_le_bytes());
        self.offsets.write_at(off_pos + 4, &len.to_le_bytes());
        self.offsets.mark_dirty(off_pos, 8);
        // #59: index が満杯で登録できないなら 「vocab 満杯」 と同じ扱いにする
        // (dedup が黙って壊れるより、 write を拒否させる方が安全)。 data/offsets に
        // 書いた分は orphan になるが、 これは terminal な capacity 状態。
        if !self.index_insert(value, id) {
            return u32::MAX;
        }
        id
    }

    /// index に (hash, id) を登録する。 **index が満杯なら `false`** (#59)。
    ///
    /// 旧実装は空 slot が見つかるまで無条件に linear probe しており、 index が 100%
    /// 埋まると永久に回った (= 満杯が hang)。 走査は index_cap 回で打ち切る。
    /// 登録できなくても data/offsets 側の値は書けているので、 dedup が効かなくなる
    /// だけで read は壊れない (呼び出し側が fault として報告する)。
    fn index_insert(&self, value: &[u8], id: u32) -> bool {
        let mask = (self.index_cap - 1) as u64;
        let h = fxhash(value);
        let mut idx = home_slot(h, self.index_cap); // #123
        let mut probes = 0usize;
        loop {
            if probes >= self.index_cap as usize {
                return false;
            }
            probes += 1;
            let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
            // v10: index segment は書いた分だけ commit される (旧 fixed cluster の eager
            // commit ではない)。 slot の atomic CAS は write なので、 触る前に伸ばす。
            // 伸ばせない (#167) なら挿入失敗として返す (呼び側が満杯扱いする)。
            if self.index.ensure_committed(off + INDEX_SLOT_SIZE).is_err() {
                return false;
            }
            // #83: slot flag は Region 経由の AtomicU8 で直接触る (`&mut [u8]` を
            // 実体化しない)。 hash/id の書込も write_at (raw ptr)。
            let flag = self.index.as_atomic_u8(off);
            let f = flag.load(Ordering::Acquire);
            if f == 0 {
                match flag.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Relaxed) {
                    Ok(_) => {
                        self.index.write_at(off + 1, &h.to_le_bytes());
                        self.index.write_at(off + 9, &id.to_le_bytes());
                        flag.store(1, Ordering::Release);
                        self.index.mark_dirty(off, INDEX_SLOT_SIZE);
                        return true;
                    }
                    Err(_) => continue,
                }
            }
            if f == 2 {
                while self.index.as_atomic_u8(off).load(Ordering::Acquire) == 2 {
                    std::hint::spin_loop();
                }
                continue;
            }
            // f == 1 (committed): hash/id は flag=1 の Release publish より前に書かれ、
            // 上の flag Acquire load と対で可視。 slice() で読む。
            let (slot_hash, vid) = {
                let xm = self.index.slice();
                (
                    u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap()),
                    u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap()),
                )
            };
            if slot_hash == h {
                // ハッシュ一致 → 実際の値を比較して本当に重複か確認。
                // #92: vid >= max_entries の破損 slot は get(vid) が OOB するので
                // 読み飛ばす (実 insert は vid < max_entries を保証 = 通常運用では常に
                // 通過。 max_entries は不変で並行 insert を skip しない = dedup race 無)。
                if vid < self.max_entries && self.get(vid) == value {
                    return true; // 本当の重複
                }
                // ハッシュ衝突 or 破損 slot → linear probe 続行
            }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
    }

    pub fn count(&self) -> u32 { self.count.load(Ordering::Relaxed) }

    /// data 領域の append pointer (= 消費済み byte 数)。 単調増加・回収なし。
    /// #88 bench: Leaf を vocab に載せた場合の「回収されない footprint」計測用。
    pub fn data_footprint(&self) -> u32 { self.data_end.load(Ordering::Relaxed) }

    /// 内部状態をRegionヘッダに書き戻す（flushの前に呼ぶ）。
    pub fn sync(&self) {
        self.data.write_at(4, &self.count.load(Ordering::Relaxed).to_le_bytes());
        self.data.write_at(8, &self.data_end.load(Ordering::Relaxed).to_le_bytes());
    }

    /// index と data の整合性マーカーを書く。
    ///
    /// `clean = true`: 直前に全 msync が完了 → 次回 open で rebuild skip 可
    /// `clean = false`: insert が走った／crash 検知用 → 次回 open で rebuild 強制
    ///
    /// 自身では msync しない。 caller (Engine) が body_msync で永続化する責任を負う。
    /// #101: 観測用 — clean flag の現在値。 open 直後に true なら「前回 graceful close
    /// 済みで rebuild を skip した」。 insert が走ると false に戻る (#77-M1)。
    pub fn index_clean_on_disk(&self) -> bool {
        self.clean_on_disk.load(Ordering::Acquire)
    }

    pub fn mark_index_clean(&self, clean: bool) {
        // data 領域は variable cluster (lazy commit) なので、 先頭 header を
        // 確実に commit してから書く。
        // #167: commit を伸ばせなければ flag を書かない (未 commit page への write は
        // ディスク満杯なら SIGBUS)。 flag を落とせないと次回 open が rebuild する =
        // 遅くなるだけで壊れない、 安全側。
        if self.data.ensure_committed(HEADER).is_err() {
            return;
        }
        let val: u32 = if clean { 1 } else { 0 };
        self.data.write_at(CLEAN_FLAG_OFF, &val.to_le_bytes());
        self.clean_on_disk.store(clean, Ordering::Release); // #77-M1 キャッシュ追従
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Regions {
        data_ptr: *mut u8,
        offsets_ptr: *mut u8,
        index_ptr: *mut u8,
        data_len: usize,
        offsets_len: usize,
        index_len: usize,
    }

    fn make_regions(max_entries: u32, index_cap: u32, data_size: usize) -> Regions {
        let leak = |size: usize| -> *mut u8 {
            Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr()
        };
        Regions {
            data_ptr: leak(Vocabulary::data_region_size(data_size)),
            offsets_ptr: leak(Vocabulary::offsets_region_size(max_entries)),
            index_ptr: leak(Vocabulary::index_region_size(index_cap.next_power_of_two())),
            data_len: Vocabulary::data_region_size(data_size),
            offsets_len: Vocabulary::offsets_region_size(max_entries),
            index_len: Vocabulary::index_region_size(index_cap.next_power_of_two()),
        }
    }

    impl Regions {
        fn vocab_init(&self, max_entries: u32, index_cap: u32) -> Vocabulary {
            Vocabulary::init(
                unsafe { Region::new(self.data_ptr, self.data_len) },
                unsafe { Region::new(self.offsets_ptr, self.offsets_len) },
                unsafe { Region::new(self.index_ptr, self.index_len) },
                max_entries, index_cap,
            )
        }
        fn vocab_load(&self, readonly: bool) -> Vocabulary {
            Vocabulary::load(
                unsafe { Region::new(self.data_ptr, self.data_len) },
                unsafe { Region::new(self.offsets_ptr, self.offsets_len) },
                unsafe { Region::new(self.index_ptr, self.index_len) },
                readonly,
            )
        }
        fn index_bytes(&self) -> Vec<u8> {
            unsafe { std::slice::from_raw_parts(self.index_ptr, self.index_len) }.to_vec()
        }
        /// index region 内の「使用中」 slot 数 (flag != 0)。 migration で旧 slot が
        /// 残っていないか (= 占有率が二重になっていないか) を見るのに使う。
        fn occupied_slots(&self, index_cap: u32) -> usize {
            let xm = unsafe { std::slice::from_raw_parts(self.index_ptr, self.index_len) };
            (0..index_cap as usize)
                .filter(|i| xm[INDEX_HEADER + i * INDEX_SLOT_SIZE] != 0)
                .count()
        }
        /// 0.14 (VIX2) が書いた index を再現する — 全ゼロ化して旧 magic + **旧 slot 選択**
        /// (hash 下位ビット) で live entry を植える。
        fn plant_legacy_index(&self, index_cap: u32, entries: &[(&[u8], u32)]) {
            let xm = unsafe { std::slice::from_raw_parts_mut(self.index_ptr, self.index_len) };
            xm.fill(0);
            xm[0..4].copy_from_slice(&INDEX_MAGIC_V2);
            xm[4..8].copy_from_slice(&index_cap.to_le_bytes());
            for (value, vid) in entries {
                let h = fxhash(value);
                let mut idx = home_slot_legacy(h, index_cap);
                loop {
                    let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
                    if xm[off] == 0 {
                        xm[off] = 1;
                        xm[off + 1..off + 9].copy_from_slice(&h.to_le_bytes());
                        xm[off + 9..off + 13].copy_from_slice(&vid.to_le_bytes());
                        break;
                    }
                    idx = (idx + 1) & (index_cap as usize - 1);
                }
            }
        }
    }

    /// #123: `fxhash` は乗算で終わるので **下位ビットに上位の entropy が届かない**。
    /// issue の実キー (`第N条` は UTF-8 で 8 byte = 1 word ちょうどなので
    /// `h = word * SEED` そのもの) で、 旧 slot 選択が全滅し新 slot 選択が散ることを固定する。
    #[test]
    fn issue123_low_bits_collide_high_bits_spread() {
        let keys: Vec<&[u8]> = vec![
            "第10条".as_bytes(),
            "第11条".as_bytes(),
            "第12条".as_bytes(),
            "第13条".as_bytes(),
        ];
        assert!(keys.iter().all(|k| k.len() == 8), "1 word ちょうどの前提");
        let cap = 1u32 << 12;

        let legacy: std::collections::HashSet<usize> =
            keys.iter().map(|k| home_slot_legacy(fxhash(k), cap)).collect();
        let modern: std::collections::HashSet<usize> =
            keys.iter().map(|k| home_slot(fxhash(k), cap)).collect();

        assert_eq!(
            legacy.len(), 1,
            "旧 slot 選択 (下位ビット) は同一 slot に潰れる — issue #123 の実例が再現しない: {legacy:?}"
        );
        assert_eq!(
            modern.len(), keys.len(),
            "新 slot 選択 (上位ビット) が散っていない: {modern:?}"
        );
    }

    /// #122 / #59: 天井 hit は **panic ではなく予約 sentinel `u32::MAX`** を返すこと。
    ///
    /// #122 は 「天井 hit のメッセージがどの knob を上げればよいか示すこと」 を
    /// `#[should_panic]` で担保していた。 が #59 で方針が変わった — embedded DB は
    /// 他人の process に埋め込まれるので、 「vocab が一杯」 という想定内事象で host を
    /// 殺してはいけない。 actionable な案内は panic message ではなく
    /// `Engine::record_fault` の warning + `Engine::fault_count(FaultKind::VocabSpace)`
    /// で行う (`engine.rs` / `tests/issue59_no_host_kill.rs`)。
    #[test]
    fn issue122_vocab_full_returns_sentinel_instead_of_panicking() {
        let r = make_regions(4, 8, 64 * 1024);
        let v = r.vocab_init(4, 8);
        let ids: Vec<u32> = (0..8).map(|i| v.insert(format!("v{i}").as_bytes())).collect();
        assert!(
            ids.iter().any(|&id| id != u32::MAX),
            "天井前の insert まで失敗している: {ids:?}"
        );
        assert!(
            ids.contains(&u32::MAX),
            "天井を越えたのに sentinel が返っていない (panic していた旧挙動?): {ids:?}"
        );
        assert!(v.is_full(), "is_full が天井を報告していない");
    }

    /// #59: index が 100% 埋まった状態の `lookup` が **有限時間で** 返ること。
    ///
    /// 旧実装の probe は 「空 slot に当たる」 だけが終了条件で、 満杯かつ不在の
    /// 値を引くと永久に回った (= 満杯が hang。 embedded DB としては panic と同程度に
    /// 悪い)。 index_cap 回で打ち切る。
    #[test]
    fn vocab_lookup_on_full_index_terminates() {
        let r = make_regions(8, 8, 64 * 1024);
        let v = r.vocab_init(8, 8);
        // index_cap = 8 を埋める
        for i in 0..8 {
            v.insert(format!("v{i}").as_bytes());
        }
        // 不在の値: 空 slot が無いので旧実装はここで無限ループした
        assert_eq!(v.lookup(b"absent"), None);
    }

    /// #123 (F6): migration の途中で kill された状態 = **tombstone (flag = 3) が残った
    /// index** を次の open が回収できること。 旧実装は `xm[off] == 1` しか同一 entry と
    /// 見なさず、 rebuild は no-clear (#92) なので flag 3 が恒久ゴミとして残り、 占有率と
    /// probe 長が永久に劣化していた。 crash は「open の単一スレッド区間で必ず消える」
    /// という前提を破る。
    #[test]
    fn issue123_crash_left_tombstones_are_reclaimed() {
        let cap = 1u32 << 8;
        let r = make_regions(1024, cap, 64 * 1024);
        let keys: Vec<String> = (0..24).map(|i| format!("第{}条", 10 + i)).collect();

        let ids: Vec<u32> = {
            let w = r.vocab_init(1024, cap);
            let ids = keys.iter().map(|k| w.get_or_insert(k.as_bytes())).collect();
            w.sync();
            ids
        };
        let entries: Vec<(&[u8], u32)> =
            keys.iter().zip(&ids).map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_legacy_index(cap, &entries);

        // migration ① (tombstone を立てた) 直後に kill された状態を再現する:
        // 先頭 8 entry の flag を 3 にし、 payload (hash/vid) は残す。
        {
            let xm = unsafe {
                std::slice::from_raw_parts_mut(r.index_ptr, r.index_len)
            };
            let mut done = 0;
            for i in 0..cap as usize {
                let off = INDEX_HEADER + i * INDEX_SLOT_SIZE;
                if xm[off] == 1 {
                    xm[off] = 3;
                    done += 1;
                    if done == 8 { break; }
                }
            }
            assert_eq!(done, 8, "tombstone を立てられていない");
        }

        let v = r.vocab_load(/*readonly=*/ false);

        for (k, id) in keys.iter().zip(&ids) {
            assert_eq!(v.lookup(k.as_bytes()), Some(*id), "crash 復帰後に {k} が引けない");
        }
        assert_eq!(
            r.occupied_slots(cap),
            keys.len(),
            "crash で残った tombstone が回収されず恒久ゴミになっている (F6)"
        );
        // flag 3 が 1 つも残っていないこと (lookup/insert は 0/非 0 でしか見ないので
        // 正しさは保たれるが、 占有と probe 長が劣化し続ける)
        let xm = r.index_bytes();
        let leftovers = (0..cap as usize)
            .filter(|i| xm[INDEX_HEADER + i * INDEX_SLOT_SIZE] == 3)
            .count();
        assert_eq!(leftovers, 0, "flag=3 の tombstone が {leftovers} 個残っている");
    }

    /// #123: migrate 後に graceful close (clean flag) すると、 次の open は rebuild を
    /// **skip** する (= VIX3 magic が永続化されている)。 magic を書き忘れると毎 open
    /// rebuild し続けるので、 その回帰を止める。
    #[test]
    fn issue123_migrated_index_is_clean_on_next_open() {
        let cap = 1u32 << 8;
        let r = make_regions(1024, cap, 64 * 1024);
        let keys: Vec<String> = (0..16).map(|i| format!("第{}条", 10 + i)).collect();

        let ids: Vec<u32> = {
            let w = r.vocab_init(1024, cap);
            let ids = keys.iter().map(|k| w.get_or_insert(k.as_bytes())).collect();
            w.sync();
            ids
        };
        let entries: Vec<(&[u8], u32)> =
            keys.iter().zip(&ids).map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_legacy_index(cap, &entries);

        // 1 回目: VIX2 → migrate される
        {
            let v = r.vocab_load(false);
            assert!(v.rebuilt_on_load, "1 回目は migrate されるべき");
            v.sync();
            v.mark_index_clean(true); // graceful close 相当
        }
        // 2 回目: VIX3 + clean なので rebuild しない
        let v2 = r.vocab_load(false);
        assert!(
            !v2.rebuilt_on_load,
            "migrate 後も rebuild され続けている (VIX3 magic が永続化されていない)"
        );
        for (k, id) in keys.iter().zip(&ids) {
            assert_eq!(v2.lookup(k.as_bytes()), Some(*id), "{k} が引けない");
        }
    }

    /// #123: readonly open で VIX2 を掴んだ場合、 共有 index を **1 byte も書かず**に
    /// shadow (新 slot 関数) で正しく引けること。 別 process の writer と併走する
    /// oboro / sinfo-studio 型の構成が該当する。
    #[test]
    fn issue123_readonly_open_of_legacy_index_uses_shadow() {
        let cap = 1u32 << 8;
        let r = make_regions(1024, cap, 64 * 1024);
        let keys: Vec<String> = (0..16).map(|i| format!("第{}条", 10 + i)).collect();

        let ids: Vec<u32> = {
            let w = r.vocab_init(1024, cap);
            let ids = keys.iter().map(|k| w.get_or_insert(k.as_bytes())).collect();
            w.sync();
            w.mark_index_clean(true);
            ids
        };
        let entries: Vec<(&[u8], u32)> =
            keys.iter().zip(&ids).map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_legacy_index(cap, &entries);

        let before = r.index_bytes();
        let ro = r.vocab_load(/*readonly=*/ true);
        for (k, id) in keys.iter().zip(&ids) {
            assert_eq!(ro.lookup(k.as_bytes()), Some(*id), "shadow で {k} が引けない");
        }
        assert_eq!(r.index_bytes(), before, "readonly open が共有 index を書き換えた");
    }

    /// #123: cap == 1 の退化ケースで `home_slot` が shift overflow しない。
    #[test]
    fn issue123_home_slot_handles_degenerate_cap() {
        assert_eq!(home_slot(u64::MAX, 1), 0);
        assert_eq!(home_slot(0, 1), 0);
        assert!(home_slot(u64::MAX, 2) < 2);
    }

    /// #123: VIX2 index を持つ DB を開くと、 clean_flag が立っていても index を作り直し、
    /// **旧 slot を残さない** (占有 slot 数 == live 値数)。 旧 slot を消さずに新 slot 関数で
    /// 再挿入すると (rebuild は #92 で no-clear) 占有が二重になる。
    #[test]
    fn issue123_vix2_index_migrates_without_stale_slots() {
        let cap = 1u32 << 8;
        let r = make_regions(1024, cap, 64 * 1024);
        let keys: Vec<String> = (0..40).map(|i| format!("第{}条", 10 + i)).collect();

        let ids: Vec<u32> = {
            let w = r.vocab_init(1024, cap);
            let ids = keys.iter().map(|k| w.get_or_insert(k.as_bytes())).collect();
            w.sync();
            w.mark_index_clean(true); // clean = 「rebuild 不要」 マーカーを立てておく
            ids
        };

        // 0.14 が書いた状態を再現 (VIX2 magic + 旧 slot)。 clean_flag は 1 のまま。
        let entries: Vec<(&[u8], u32)> =
            keys.iter().zip(&ids).map(|(k, id)| (k.as_bytes(), *id)).collect();
        r.plant_legacy_index(cap, &entries);
        assert_eq!(r.occupied_slots(cap), keys.len(), "植え付けが失敗している");

        let v = r.vocab_load(/*readonly=*/ false);

        assert!(v.rebuilt_on_load, "VIX2 は clean_flag が立っていても rebuild されるべき");
        for (k, id) in keys.iter().zip(&ids) {
            assert_eq!(v.lookup(k.as_bytes()), Some(*id), "migration 後に {k} が引けない");
        }
        assert_eq!(v.lookup("第999条".as_bytes()), None, "無い値が引けてしまう");
        assert_eq!(
            &r.index_bytes()[0..4], &INDEX_MAGIC,
            "migration 後の index magic が VIX3 になっていない (次 open で毎回 rebuild する)"
        );
        assert_eq!(
            r.occupied_slots(cap), keys.len(),
            "旧 slot が残って占有が二重になっている (#123 migration の取り残し)"
        );
    }

    /// #77-H1 regression: dirty (clean_flag≠1) な DB の readonly load が
    /// 共有 index を 1 byte も書き換えず、それでも lookup が正しく動くこと。
    /// 旧実装は readonly でも rebuild_index が共有 index をゼロクリアしていた。
    #[test]
    fn readonly_load_does_not_touch_shared_index() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        let a = w.get_or_insert(b"alpha");
        let b = w.get_or_insert(b"beta");
        w.sync();
        // clean_flag は書かない (= 0 のまま) → load は dirty 扱いで rebuild 経路へ

        let before = r.index_bytes();
        let ro = r.vocab_load(/*readonly=*/ true);
        assert_eq!(ro.lookup(b"alpha"), Some(a), "shadow index で lookup できる");
        assert_eq!(ro.lookup(b"beta"), Some(b));
        assert_eq!(ro.lookup(b"gamma"), None);
        assert_eq!(r.index_bytes(), before, "共有 index が書き換えられた (#77-H1)");

        // writer 側の index はそのまま生きている (get_or_insert が dedupe できる)
        assert_eq!(w.get_or_insert(b"alpha"), a, "writer の dedupe が壊れた");
    }

    /// #77-M1 regression: mark_index_clean(true) (= flush) 後の最初の insert が
    /// clean_flag を 0 に戻すこと。旧実装は open 時の 1 回しか倒さなかった。
    #[test]
    fn insert_after_clean_re_dirties_flag() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        w.get_or_insert(b"first");
        w.sync();
        w.mark_index_clean(true);
        let flag = |r: &Regions| -> u32 {
            let dm = unsafe { std::slice::from_raw_parts(r.data_ptr, r.data_len) };
            u32::from_le_bytes(dm[CLEAN_FLAG_OFF..CLEAN_FLAG_OFF + 4].try_into().unwrap())
        };
        assert_eq!(flag(&r), 1, "flush 直後は clean=1");
        w.get_or_insert(b"second");
        assert_eq!(flag(&r), 0, "flush 後の insert で clean=0 に戻るはず (#77-M1)");
    }

    /// index 領域を直接叩くための helper。
    impl Regions {
        // test 専用 helper。 単一 writer 運用不変式の下で torn write を模す。 #83 で
        // `slice_mut` を廃したので、 本番と同じ Region write API 経由で書く。
        fn index_region(&self) -> Region {
            unsafe { Region::new(self.index_ptr, self.index_len) }
        }
        /// vid を持つ live slot を探して 0 クリア (= torn write で slot 欠落を模す)。
        fn clear_slot_of(&self, vid: u32) {
            let region = self.index_region();
            let mut off = INDEX_HEADER;
            while off + INDEX_SLOT_SIZE <= self.index_len {
                let (flag, slot_vid) = {
                    let xm = region.slice();
                    (
                        xm[off],
                        u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap()),
                    )
                };
                if flag != 0 && slot_vid == vid {
                    region.fill_at(off, INDEX_SLOT_SIZE, 0);
                }
                off += INDEX_SLOT_SIZE;
            }
        }
        /// value の probe home 以降で最初の空 slot に (hash, vid) を植える
        /// (= torn write で count より先行した「未来」slot を模す)。
        fn plant_slot(&self, index_cap: u32, value: &[u8], vid: u32) {
            let region = self.index_region();
            let mask = (index_cap - 1) as u64;
            let h = fxhash(value);
            let mut idx = home_slot(h, index_cap); // #123
            loop {
                let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
                if region.slice()[off] == 0 {
                    region.write_at(off, &[1]);
                    region.write_at(off + 1, &h.to_le_bytes());
                    region.write_at(off + 9, &vid.to_le_bytes());
                    return;
                }
                idx = ((idx as u64 + 1) & mask) as usize;
            }
        }
    }

    /// #92: dirty reopen (no-clear rebuild) が全 live value を保持し、 dedup も
    /// 効くこと。 on-disk index が data と consistent (通常の落ち方) な標準ケース。
    #[test]
    fn dirty_rebuild_preserves_all_values() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        let mut ids = Vec::new();
        for i in 0..300 {
            ids.push(w.get_or_insert(format!("v{i}").as_bytes()));
        }
        w.sync(); // count/data_end を書き戻す。 clean_flag は 0 のまま = dirty。
        drop(w);

        let w2 = r.vocab_load(/*readonly=*/ false); // no-clear rebuild
        for i in 0..300 {
            assert_eq!(
                w2.lookup(format!("v{i}").as_bytes()),
                Some(ids[i]),
                "v{i} が dirty rebuild 後に消えた"
            );
        }
        assert_eq!(w2.lookup(b"absent"), None);
        assert_eq!(w2.get_or_insert(b"v0"), ids[0], "既存値の dedup が壊れた");
        let fresh = w2.get_or_insert(b"brand-new");
        assert_eq!(fresh, 300, "新規値は次の id を取るはず");
        assert_eq!(w2.lookup(b"brand-new"), Some(300));
    }

    /// #92: torn-behind (index が count より遅れて slot 欠落) を dirty rebuild が
    /// self-heal すること。
    #[test]
    fn dirty_rebuild_self_heals_missing_slot() {
        let r = make_regions(1024, 1024, 64 * 1024);
        let w = r.vocab_init(1024, 1024);
        let mut ids = Vec::new();
        for i in 0..20 {
            ids.push(w.get_or_insert(format!("k{i}").as_bytes()));
        }
        w.sync();
        r.clear_slot_of(ids[7]); // torn: k7 の slot が flush されず消失
        drop(w);

        let w2 = r.vocab_load(false);
        assert_eq!(
            w2.lookup(b"k7"),
            Some(ids[7]),
            "torn-behind の欠落 slot が self-heal されない"
        );
        for i in 0..20 {
            assert_eq!(w2.lookup(format!("k{i}").as_bytes()), Some(ids[i]));
        }
    }

    /// #92: no-clear rebuild で残りうる **vid >= max_entries の破損 slot** (bit-rot 等)
    /// でも rebuild / lookup / insert が offsets region を溢れて OOB せず正しく振る舞う
    /// こと。 破損 slot の hash がクエリと衝突する配置にして guard 経路を必ず踏ませる。
    #[test]
    fn dirty_rebuild_tolerates_corrupt_slot_no_oob() {
        let max_entries = 1024u32;
        let r = make_regions(max_entries, 1024, 64 * 1024);
        let index_cap = 1024u32; // 1024.next_power_of_two()
        let w = r.vocab_init(max_entries, index_cap);
        let mut ids = Vec::new();
        for i in 0..10 {
            ids.push(w.get_or_insert(format!("t{i}").as_bytes()));
        }
        w.sync();
        let count = w.count(); // 10
        // 破損 slot: value "probe-me" の home 以降に slot_hash=fxhash("probe-me")、
        // vid=9999 (>= max_entries=1024) を植える。 offsets region は max_entries×8 しか
        // 無いので guard 無しで get(9999) を呼ぶと offsets region 自体を OOB する配置。
        r.plant_slot(index_cap, b"probe-me", 9999);
        drop(w);

        let w2 = r.vocab_load(false); // rebuild は破損 slot で OOB してはならない
        // probe-me は未挿入 → 破損 slot を skip して None (誤 hit / OOB しない)。
        assert_eq!(w2.lookup(b"probe-me"), None, "破損 slot が誤 hit / OOB");
        for i in 0..10 {
            assert_eq!(w2.lookup(format!("t{i}").as_bytes()), Some(ids[i]));
        }
        // 挿入も破損 slot を skip して新 id を取れること。
        let np = w2.get_or_insert(b"probe-me");
        assert_eq!(np, count, "破損 slot を跨いだ挿入が新 id を取れない");
        assert_eq!(w2.lookup(b"probe-me"), Some(count));
    }

    /// #92: guard を `vid < max_entries` (不変) にしたので、 並行 `get_or_insert` が
    /// valid slot を stale count で取りこぼして dedup を壊す race が無いこと。 8 thread で
    /// 重複する値集合を叩き、 全 distinct 値が lost せず findable であること + OOB
    /// panic しないことを確認する (index_insert / lookup 双方を contention 下で踏む)。
    #[test]
    fn concurrent_get_or_insert_stays_findable() {
        use std::sync::Arc;
        let max_entries = 16384u32;
        let r = make_regions(max_entries, 16384, 1024 * 1024);
        let w = Arc::new(r.vocab_init(max_entries, 16384));
        let n_distinct = 128usize;
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let w = w.clone();
                std::thread::spawn(move || {
                    for i in 0..1000usize {
                        let v = format!("v{:05}", (i * 7 + t) % n_distinct);
                        let id = w.get_or_insert(v.as_bytes());
                        assert!(id < max_entries, "vocab full / 不正 id");
                    }
                })
            })
            .collect();
        for h in threads {
            h.join().unwrap();
        }
        // 全 distinct 値が findable (取りこぼし無し) かつ返る vid の実体が一致。
        for k in 0..n_distinct {
            let v = format!("v{:05}", k);
            let got = w.lookup(v.as_bytes());
            assert!(got.is_some(), "並行 insert 後に {v} が lost (dedup race)");
            assert_eq!(w.get(got.unwrap()), v.as_bytes(), "{v} の vid 実体不一致");
        }
    }
}

/// offsets/data region から id の value slice を読む。 `get` と `rebuild_index_into`
/// で共有する (後者は index を `&mut` 借用中に呼ぶため `&self` メソッドではなく
/// region を直接受ける free fn にして分割借用を可能にする)。
#[inline]
fn read_value<'a>(offsets: &'a Region, data: &'a Region, id: u32) -> &'a [u8] {
    let om = offsets.slice();
    let off_pos = (id as usize) * 8;
    let offset = u32::from_le_bytes(om[off_pos..off_pos + 4].try_into().unwrap()) as usize;
    let len = u32::from_le_bytes(om[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
    let dm = data.slice();
    &dm[offset..offset + len]
}

#[inline(always)]
fn fxhash(data: &[u8]) -> u64 {
    const SEED: u64 = 0x517cc1b727220a95;
    let mut h: u64 = 0;
    let mut i = 0;
    while i + 8 <= data.len() {
        let word = u64::from_le_bytes([
            data[i], data[i+1], data[i+2], data[i+3],
            data[i+4], data[i+5], data[i+6], data[i+7],
        ]);
        h = (h.rotate_left(5) ^ word).wrapping_mul(SEED);
        i += 8;
    }
    while i < data.len() {
        h = (h.rotate_left(5) ^ data[i] as u64).wrapping_mul(SEED);
        i += 1;
    }
    h
}
