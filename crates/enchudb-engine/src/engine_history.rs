//! engine 側の履歴 (スピン 案 W の試作、 置き場と読みは [`crate::history`])。 宣言・書き手の道からの 1 件・WAL の無い書き込みの
//! 断り・開く時の戻し / 繋ぎ直し / cell との突き合わせ・書き出し・読み。
//!
//! ## 決めたこと (時刻の分からない道)
//! - **WAL の無い session のユーザの書き込みは engine の時計** (`HistCol::now`: 壁時計の ms と、 列の時計 (見た最大) + 1 の大きい方)。
//!   WAL は任意なので、 WAL なしで開くアプリ (`open_standalone` / build phase / schema の `Database::open`) も履歴の列に書ける。
//!   印は付けない (その書き込みの時刻として正しい)。 WAL が無いので電源断の後の足し直しは無く、 履歴は本体と同じく書き出した所まで
//!   (開く時の cell との突き合わせは効く)。 後で WAL 付きで開いた時は WAL の時計を履歴の時計より先へ進める (`observe_history_clock`)
//! - **engine の中の時刻の分からない道は 「遅くとも」 の印** (`OBSERVED`、 呼び手は [`HIST_NO_TIME`] を渡す): 開く時の後始末
//!   (gen.seg の突き合わせで完了させた死・前の住人の残りの cell)、 sync の畳み・改名の分け (sync する DB は宣言を断るので今は通らない)、
//!   開く時の cell との突き合わせ。 時刻は列の時計で、 本当の変更はそれ以前
use super::*;
use crate::history::{self, flag, At, Change, HistCol, HistError};
use enchudb_oplog::Hlc;

/// 突き合わせの足す entry の時刻に立てる 「遅くとも」 の bit (時刻は 2^63 未満)。
const OBSERVED_AT: u64 = 1 << 63;

/// 時刻を運べない engine の中の道が履歴に渡す印 (`ZERO` は WAL の無い session のユーザの書き込み = engine の時計)。
pub(super) const HIST_NO_TIME: Hlc = Hlc { wall: 0, logical: 1, peer: u32::MAX };

impl Engine {
    /// 書き手の道 (`live_set` / `live_remove`、 行の lock の下) から: 列 `h` の場所 `slot` の identity に 1 件足す。 `hlc` が `ZERO`
    /// (時刻を運べない道) なら 「遅くとも」 の印を付けて列の時計で置く。
    pub(super) fn hist_note(&self, h: &HistCol, slot: u32, value: Option<u64>, hlc: Hlc, deleting: bool) {
        // 行の削除は場所の世代を上げる前に cell を落とす (今の住人)。 開く時の後始末 (死んだ場所の残りの cell) は最後の行
        let eid = if deleting { self.last_local_of_slot(slot) } else { self.generations().local(slot) };
        let (time, mut extra) = if hlc == Hlc::ZERO {
            // WAL の無い session のユーザの書き込み: engine の時計 (印なし)
            (h.now(), 0)
        } else if hlc == HIST_NO_TIME {
            let t = h.tick();
            // 後の本当の書き込みの時刻がこれより前にならないように WAL の時計も進める
            if let Some(w) = self.oplog.as_ref() {
                w.observe_hlc(history::unpack(t));
            }
            (t, flag::OBSERVED)
        } else {
            (history::pack(hlc), 0)
        };
        if deleting {
            extra |= flag::DELETE;
        }
        if h.note(slot, eid, value, time, extra).is_err() {
            self.record_fault(FaultKind::DiskSpace, "history: cannot grow the history segment — the change is not in the history");
        }
    }

    /// WAL の再生から: record の identity `eid` (世代 | 場所) の書き込みを、 cell が当てたかによらず足す (cell の版数が先に届いた像では
    /// LWW が弾くが、 書き込みは在った)。 同じ時刻の entry は重複で落ちる。
    pub(super) fn hist_replayed(&self, slot: u32, eid: u32, hid: usize, value: Option<u64>, hlc: Hlc, deleting: bool) {
        let Some(h) = self.hist.get(hid) else { return };
        if hlc == Hlc::ZERO {
            return;
        }
        let _row = self.row_locks.write(slot);
        if h.note(slot, eid, value, history::pack(hlc), if deleting { flag::DELETE } else { 0 }).is_err() {
            self.record_fault(FaultKind::DiskSpace, "history: cannot grow the history segment — the change is not in the history");
        }
    }

    fn hist_table_of(&self, hid: u16) -> Option<&TableDef> {
        self.himo_table_get(hid as usize).filter(|&t| t != ANONYMOUS_TABLE).and_then(|t| self.tables.get(t as usize))
    }

    /// 列 (紐) に履歴を持たせる (スピン 案 W の試作): 以後の書き込み・外す・行の削除を、 行の identity (eid = 世代 | 場所) ごとの
    /// 集まりに (時刻, 値) で足す。 宣言の時に値のある行は、 その値を宣言の時刻 (地平) で入れる。 宣言したら true、 もう宣言して
    /// あれば false。
    ///
    /// - 書き手の居ない時に呼ぶ (宣言の時の値をなめる間の書き込みは、 順序が前後しうる)
    /// - 時刻は書き込みの WAL の record の HLC。 WAL の無い session の書き込みは engine の時計 (壁時計の ms と列の時計の大きい方)
    /// - 断る: sync する DB (`enable_sync_tables`)・Leaf の列・語を回収する DB の Tag の列・表に属さない紐 / 予約の表の紐・
    ///   packed / memory の DB・readonly
    /// - 宣言は `{db}/tables` (`HIS1`、 ANS1 の後ろ) に保存する。 置き場は `{db}/hist/NNNN.seg` と `NNNN.head.seg`
    pub fn declare_history(&self, himo: &str) -> Result<bool, String> {
        let id = self.himo_id(himo).ok_or_else(|| format!("declare_history: unknown himo '{himo}'"))?;
        if self.hist.get(id).is_some() {
            return Ok(false);
        }
        if self.is_readonly() {
            return Err("declare_history: the engine is readonly".into());
        }
        if self.sync_tables_on() || self.sync_tables_enabled() {
            return Err("declare_history: a sync-enabled DB cannot declare history (not in this prototype)".into());
        }
        match self.value_types[id] {
            ValueType::Leaf => return Err(format!("declare_history: '{himo}' is a Leaf himo (slots are reused)")),
            ValueType::Tag if self.vocab.reclaim_enabled() => {
                return Err(format!("declare_history: '{himo}' is a Tag himo of a DB that reclaims words (ids are reused)"));
            }
            _ => {}
        }
        let Some(t) = self.hist_table_of(id as u16) else {
            return Err(format!("declare_history: '{himo}' does not belong to a table"));
        };
        if t.is_reserved() {
            return Err(format!("declare_history: '{himo}' belongs to a reserved table"));
        }
        let Backing::Segments(set) = &self.backing else {
            return Err("declare_history: a packed / memory DB cannot hold history segments".into());
        };
        let horizon = match self.oplog.as_ref() {
            Some(w) => history::pack(w.mint_hlc()),
            None => {
                let ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                history::pack(Hlc { wall: ms, logical: 0, peer: 0 })
            }
        };
        let col = HistCol::create(set.dir(), id as u16, horizon, self.max_entities()).map_err(|e| format!("declare_history: {e}"))?;
        // 宣言の時の値 (地平の entry)
        let mut cells = Vec::new();
        self.himos[id].for_each_set_cell(|slot, v| cells.push((slot, v)));
        for (slot, v) in cells {
            if !self.entities.is_live(slot) {
                continue;
            }
            let _row = self.row_locks.write(slot);
            if let Some(now) = self.himos[id].get_value(slot) {
                let _ = v;
                col.note(slot, self.generations().local(slot), Some(now), horizon, flag::BASE)
                    .map_err(|e| format!("declare_history: {e}"))?;
            }
        }
        col.write_out(true).map_err(|e| format!("declare_history: {e}"))?;
        self.hist.insert(col);
        self.try_persist_tables();
        Ok(true)
    }

    /// 履歴 ([`declare_history`](Self::declare_history)) を外す。 外したら true。 置き場の file は消す (開いている間の map は残る)。
    pub fn drop_history(&self, himo: &str) -> Result<bool, String> {
        let id = self.himo_id(himo).ok_or_else(|| format!("drop_history: unknown himo '{himo}'"))?;
        if !self.hist.remove(id) {
            return Ok(false);
        }
        self.try_persist_tables();
        if let Backing::Segments(set) = &self.backing {
            for p in HistCol::paths(set.dir(), id as u16) {
                let _ = remove_sidecar(&p);
            }
        }
        Ok(true)
    }

    /// 履歴を宣言した紐の名前 (紐の番号の順)。
    pub fn history_declarations(&self) -> Vec<String> {
        self.hist.declared().into_iter().map(|h| self.himo_name_at(h as usize).unwrap_or_default().to_string()).collect()
    }

    /// `{db}/tables` に書く宣言 (紐, 地平)。
    pub(super) fn hist_decls_raw(&self) -> Vec<HistDecl> {
        self.hist.cols().into_iter().map(|c| (c.hid, c.horizon())).collect()
    }

    /// 開く時 (`{db}/tables` を読んだ後、 WAL の再生の前): 列の file を開く。 きれいに閉じなかった後は鎖を entry から繋ぎ直し
    /// (再生の重複落としが鎖を引く)、 再生の後に cell と突き合わせる印を立てる。 開けない宣言は警告して捨てる。
    pub(super) fn restore_history(&self, decls: Vec<HistDecl>, readonly: bool) {
        let Backing::Segments(set) = &self.backing else { return };
        let unclean = self.himo_reg.rebuilt_on_load || self.generations().session_dirty();
        for (h, horizon) in decls {
            if self.himos.get(h as usize).is_none() {
                eprintln!("warning: dropped a stored history declaration (himo {h}): no such himo");
                continue;
            }
            match HistCol::open(set.dir(), h, self.max_entities(), readonly) {
                Ok(c) => {
                    if c.horizon() != horizon {
                        eprintln!("warning: history of himo {h}: the horizon in the file differs from the declaration");
                    }
                    let c = self.hist.insert(c);
                    // heads を file から戻せなかった (きれいに閉じていない / 置く前に落ちた) 時も繋ぎ直す。 readonly は繋ぎ直せない
                    // (entry を書く) ので、 その process の読みは鎖の頭を持たない (試作の限り)
                    if readonly && c.heads_stale() {
                        eprintln!("warning: history of himo {h}: heads were not stored at the last close; readonly reads see no history");
                    }
                    if (unclean || c.heads_stale()) && !readonly {
                        if let Err(e) = c.relink(|eid| self.generations().split(eid).0) {
                            eprintln!("warning: history of himo {h}: relink failed: {e}");
                        }
                    }
                }
                Err(e) => eprintln!("warning: dropped a stored history declaration (himo {h}): {e}"),
            }
        }
        if unclean && !readonly && self.hist.any() {
            self.hist.repair.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// 開く道の終わり (WAL の再生と後始末の後、 書き出しの前): きれいに閉じなかった後なら、 宣言した列ごとに cell と集まりの最新を
    /// 突き合わせて揃える ([`Self::reconcile_history`])。 再生や突き合わせが鎖に時刻の戻る entry を足したら繋ぎ直す。
    pub(super) fn settle_history_at_open(&self) {
        if !self.hist.any() || self.is_readonly() {
            return;
        }
        let repair = self.hist.repair.swap(false, std::sync::atomic::Ordering::AcqRel);
        for h in self.hist.cols() {
            if repair {
                self.reconcile_history(h);
            }
            if h.needs_relink()
                && let Err(e) = h.relink(|eid| self.generations().split(eid).0)
            {
                eprintln!("warning: history of himo {}: relink failed: {e}", h.hid);
            }
        }
    }

    /// cell (今の値) と集まりの最新を場所ごとに突き合わせる (電源断の後。 門に頼らず開く時に揃える)。 cell が正しい:
    ///
    /// - 生きている行: 最新の entry の値 = cell の値 なら何もしない。 食い違えば cell の値を 「遅くとも」 で 1 件足す。 entry は外さない
    ///   (entry だけ届いた書き込みも手元で在った書き込み)。 時刻は cell の版数が最新の entry より後ならそれ、 でなければ時計
    /// - 死んだ場所: 最後の行の最新の entry に値が残っていれば、 削除の 「遅くとも」 の entry を足す (時刻は tombstone の版数があれば
    ///   それ)
    fn reconcile_history(&self, h: &HistCol) {
        let hid = h.hid as usize;
        let Some(t) = self.hist_table_of(h.hid) else { return };
        let has_ver = self.has_cell_version();
        // (場所, eid, 値, 時刻 (None = 遅くとも), 削除)
        let mut appends: Vec<(u32, u32, Option<u64>, Option<u64>, bool)> = Vec::new();
        for slot in 0..self.max_entities() {
            if !t.contains(slot) {
                continue;
            }
            let live = self.entities.is_live(slot);
            let eid = if live { self.generations().local(slot) } else { self.last_local_of_slot(slot) };
            // 前の住人 (今 / 最後の行より前) は消えている: 最新に値が残っていれば削除の 「遅くとも」 を足す (削除の record が再生で
            // 当たらない像: 場所の使い回しまで届いた本体に、 前の住人の削除の entry だけ届かなかった)。 時刻は次の住人の最初の時刻
            let res = h.residents(slot);
            for (i, &(r, v, last, _)) in res.iter().enumerate() {
                if r == eid || v.is_none() {
                    continue;
                }
                let next_first = i.checked_sub(1).map(|j| res[j].3).filter(|&nf| nf > last);
                appends.push((slot, r, None, next_first.map(|nf| nf | OBSERVED_AT), true));
            }
            let want = if live { self.himos[hid].get_value(slot) } else { None };
            let top = h.latest_value(slot, eid);
            match top {
                Some((v, _)) if v == want => continue,
                None if want.is_none() => continue,
                _ => {}
            }
            if !live {
                let tomb = self.tombstone_version_of(slot);
                let at = (tomb != Hlc::ZERO).then(|| history::pack(tomb) | OBSERVED_AT).filter(|&x| top.is_some_and(|(_, tt)| x & !OBSERVED_AT > tt));
                appends.push((slot, eid, None, at, true));
                continue;
            }
            // 外さずに足すだけ: entry だけ届いた書き込みも手元で在った書き込み。 cell の値を 「遅くとも」 で足す (時刻は cell の版数が
            // 最新の entry より後ならそれ、 でなければ時計)。 前は cell の版数より新しい entry / cell と同じ値より新しい entry を外していたが、
            // 書き出しをしない像では cell の版数や値が別の書き込み (使い回しで消した版数・別の住人の値) を指すことがあり、 書き出しの済んだ
            // entry まで外した
            let ver = if has_ver { self.version_of(slot, h.hid) } else { Hlc::ZERO };
            let at = (ver != Hlc::ZERO)
                .then(|| history::pack(ver))
                .filter(|&vt| top.is_none_or(|(_, tt)| vt > tt))
                .map(|vt| vt | OBSERVED_AT);
            appends.push((slot, eid, want, at, false));
        }
        if appends.is_empty() && !h.needs_relink() {
            return;
        }
        if h.needs_relink()
            && let Err(e) = h.relink(|eid| self.generations().split(eid).0)
        {
            eprintln!("warning: history of himo {}: relink failed: {e}", h.hid);
            return;
        }
        for (slot, eid, value, at, deleting) in appends {
            let (time, mut extra) = match at {
                Some(x) if x & OBSERVED_AT != 0 => (x & !OBSERVED_AT, flag::OBSERVED),
                Some(x) => (x, 0),
                None => (h.tick(), flag::OBSERVED),
            };
            if deleting {
                extra |= flag::DELETE;
            }
            let _ = h.note(slot, eid, value, time, extra);
        }
    }

    /// WAL を付ける時 (`spawn_consumer_with_oplog`): WAL の時計を、 宣言した列の時計 (見た最大の時刻) より先へ進める。
    pub(super) fn observe_history_clock(&self, wal: &enchudb_oplog::oplog::OpLog) {
        if !self.hist.any() {
            return;
        }
        let top = self.hist.cols().into_iter().map(|h| h.clock()).max().unwrap_or(0);
        wal.observe_hlc(history::unpack(top));
    }

    /// きれいに閉じる時 (`sync_and_mark_clean_with` の closing、 書き手の居ない所): 宣言した列ごとに場所の最新 (heads) を file に置く。
    /// 置けなければ次に開く時に entry から作り直す (遅くなるだけ)。
    pub(super) fn persist_history_heads(&self) {
        if !self.hist.any() || self.is_readonly() {
            return;
        }
        for h in self.hist.cols() {
            if let Err(e) = h.persist_heads() {
                eprintln!("warning: history of himo {}: heads not stored ({e}); the next open rebuilds them", h.hid);
            }
        }
    }

    /// 本体の書き出しの最後 (`write_out_body`): 宣言した列の置き場を書き出す (`all` = 全域)。
    pub(super) fn write_out_history(&self, all: bool) -> io::Result<()> {
        if !self.hist.any() {
            return Ok(());
        }
        for h in self.hist.cols() {
            if all || h.has_dirty() {
                h.write_out(all)?;
            }
        }
        Ok(())
    }

    fn hist_col(&self, himo: &str) -> Result<&HistCol, HistError> {
        self.himo_id(himo).and_then(|id| self.hist.get(id)).ok_or(HistError::NotDeclared)
    }

    /// 行 `eid` (世代 | 場所) の列 `himo` の、 時点 `t` (`history::pack` した HLC) の値。 地平より前は `TooOld`。 場所が使い回された後も、
    /// 前の住人の eid で前の住人の値を読む。
    pub fn value_at(&self, eid: enchudb_oplog::EntityId, himo: &str, t: u64) -> Result<At, HistError> {
        let h = self.hist_col(himo)?;
        let local = enchudb_oplog::eid_local(eid);
        Ok(h.value_at(self.slot_unchecked(eid), local, t)?)
    }

    /// 行 `eid` の列 `himo` の `n` 回目の変更 (宣言の時の値 / 最初の書き込み = 0)。
    pub fn value_at_seq(&self, eid: enchudb_oplog::EntityId, himo: &str, n: u32) -> Result<Option<Change>, HistError> {
        let h = self.hist_col(himo)?;
        Ok(h.at_seq(self.slot_unchecked(eid), enchudb_oplog::eid_local(eid), n))
    }

    /// 行 `eid` の列 `himo` の変更の一覧 (古い順)。
    pub fn history(&self, eid: enchudb_oplog::EntityId, himo: &str) -> Result<Vec<Change>, HistError> {
        let h = self.hist_col(himo)?;
        Ok(h.history(self.slot_unchecked(eid), enchudb_oplog::eid_local(eid)))
    }

    /// 時点 `t` に列 `himo` が `value` だった行 (eid、 順不同)。 値ごとに、 最初に見た時に差分の並びを作り、 後は書き手が足す。
    pub fn pull_at(&self, himo: &str, value: u64, t: u64) -> Result<Vec<enchudb_oplog::EntityId>, HistError> {
        let h = self.hist_col(himo)?;
        // 今 (列の時計) 以後の時点のデッキは今の円柱そのもの (cell = 集まりの最新)。 差分の並びを読まない
        if t >= h.clock() && t >= h.horizon() {
            return Ok(self.pull_raw(himo, value));
        }
        let peer = self.peer_id();
        Ok(h.pull_at(value, t)?.into_iter().map(|l| enchudb_oplog::make_eid(peer, l)).collect())
    }

    /// 列 `himo` の履歴の時計 (見た最大の時刻)。 「今」 の時点として読みに渡せる。
    pub fn history_clock(&self, himo: &str) -> Result<u64, HistError> {
        Ok(self.hist_col(himo)?.clock())
    }

    /// 列 `himo` の履歴の大きさ: (entry の数 (外した物を除く), 「遅くとも」 の entry の数, entry の segment の byte, heads の byte)。
    pub fn history_stats(&self, himo: &str) -> Result<(u64, u64, u64, u64), HistError> {
        let h = self.hist_col(himo)?;
        let (n, obs) = h.counts();
        let (eb, hb) = h.disk_bytes();
        Ok((n, obs, eb, hb))
    }
}
