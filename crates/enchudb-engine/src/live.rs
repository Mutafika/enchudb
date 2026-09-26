//! Live query (クエリ購読) — 「この条件に当てはまる entity 集合」 の **差分** を購読する。
//!
//! 普通の query は 1 回聞いて 1 回答えが返る。 live query は 1 回登録すると、 以降の
//! 書き込みで答えが変わった分だけを [`LiveQuery::poll`] が返す。 結果集合は空から
//! 始まり、 poll が返す差分 (added / removed) を順に積めば常に 「今 find したら返る集合」
//! に一致する (DBSP の Z-set 積分と同じ見方。 初回 poll は登録時点の全件を added で返す)。
//!
//! # 仕組み
//!
//! 条件は 1 entity の中の AND (`Eq` / `Range` / `In` / `EqText` / `Present`)。 これは
//! DBSP でいう **線形な filter** なので、 差分版は 「書き換わった entity 1 個を条件に
//! 当て直す」 だけで済む — 全件の引き直しは要らない。
//!
//! 1. engine の Column 書き込み (ぶら下げる / 外す / 削除、 local・remote apply・oplog
//!    replay・build phase の全経路) が、 書いた `(himo, eid)` を [`LiveRegistry::touch`]
//!    に通知する
//! 2. registry はその himo を条件に含む購読だけを引き (himo_id 添字の Vec、 hash 無し)、
//!    各購読がその eid を **現在の Column 状態で** 評価し直す
//! 3. 結果は eid 添字の bitset (`current`) に持つ。 最後に poll で返した状態 (`reported`)
//!    と違う eid だけを dirty list に積み、 poll で差分として吐く。 +1 → -1 のように
//!    打ち消し合う変化は poll に出ない (consolidation)
//!
//! 購読が 1 本も無い DB の書き込みコストは atomic load 1 回。
//!
//! # 正しさの根拠 (lock 順序)
//!
//! 評価は常に 「通知された時点の Column」 を読み直す (旧値を引き回さない) ので、 同じ
//! eid への通知が並行しても **最後に購読 mutex を取った評価が全ての書き込みを見ている**
//! (各書き込みは自分の通知より前に Column へ出ている + mutex の release/acquire)。
//!
//! 登録と並行する書き込みの取りこぼしは、 登録側が 「購読を route に載せる → 条件の
//! 各 himo の write_lock を 1 度取って離す (barrier) → 初期集合を数える」 の順で塞ぐ。
//! 書き込み W は 「himo の write_lock 下で Column を書く → lock を離す → route を見る」:
//!
//! - W の lock が barrier より先 → W の Column 書き込みは barrier 経由で初期集合の走査に見える
//! - W の lock が barrier より後 → barrier の unlock が W の lock に同期するので、 W は
//!   route 上の購読を必ず見て、 その eid を評価し直す
//!
//! **この保証は lock 順序が根拠であって、 並行 test の緑が根拠ではない** — 実コードの並行
//! test は barrier を外しても落ちなかった (10 run 中 0 回)。 barrier の gate は loom model
//! `tests/loom_live_subscribe.rs` で、 barrier を外すとそちらは取りこぼしを見つける。
//!
//! # 追うもの / 追わないもの
//!
//! - 追う: **集合への出入り**。 条件に出てくる紐が書き換わって、 条件を満たす/満たさなく
//!   なった entity
//! - 追わない: 集合に入ったままの entity の **中身の変化** (条件に無い紐の書き換え、 条件を
//!   満たしたままの値変更)。 中身が要るなら poll 後に読む
//! - 条件の紐が書かれた瞬間に集合に入る。 row の他の紐はまだ書かれていないことがある
//!   (insert は紐を 1 本ずつ書く / sync は op を順に適用する)。 書き込み呼び出しが返った後の
//!   poll なら揃っている
//! - 状態はメモリ上だけ。 reopen 後は購読し直し、 初回 poll は全件を added で返す
//! - 削除された eid の slot が poll の間に再利用されて再び条件を満たした場合は、 同じ eid が
//!   `removed` と `added` の両方に出る (別 entity になった可能性があるので、 読み直しを促す)。
//!   適用順は removed → added

use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use enchudb_oplog::EntityId;

/// live query の条件 1 個。 全部 **同一 entity 上の AND** として組み合わさる。
///
/// himo は `himo_id` (= `Engine::himo_id`、 schema 層は build 時 resolve 済みの id) で指す。
/// 値の意味は `query_by_id` と同じ (Number は値そのもの、 Tag は vocab id、 Ref は local eid)。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LivePred {
    /// 値が `value` と等しい。
    Eq { himo_id: u16, value: u32 },
    /// Tag 紐の値が文字列 `text`。 **まだ vocab に無い文字列でもよい** — 後から誰かが
    /// その文字列をぶら下げた時点で一致し始める (登録時に vocab を汚さない)。
    EqText { himo_id: u16, text: String },
    /// `lo <= 値 <= hi` (両端含む)。
    Range { himo_id: u16, lo: u32, hi: u32 },
    /// 値が `values` のどれか。
    In { himo_id: u16, values: Vec<u32> },
    /// 値が何か tie されている (= その紐を持つ全 entity)。
    Present { himo_id: u16 },
}

impl LivePred {
    pub fn himo_id(&self) -> u16 {
        match self {
            LivePred::Eq { himo_id, .. }
            | LivePred::EqText { himo_id, .. }
            | LivePred::Range { himo_id, .. }
            | LivePred::In { himo_id, .. }
            | LivePred::Present { himo_id } => *himo_id,
        }
    }
}

/// [`LiveQuery::poll`] の戻り値。 前回 poll からの結果集合の差分。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LiveDelta {
    /// 集合に入った entity (eid 昇順)。
    pub added: Vec<EntityId>,
    /// 集合から出た entity (eid 昇順)。 同じ eid が `added` にも居たら 「出て、 別物として
    /// 入り直した」 (slot 再利用など)。 適用順は removed → added。
    pub removed: Vec<EntityId>,
}

impl LiveDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

/// 評価に要る engine 側の読み口。 engine 本体と unit test の両方が実装する。
pub(crate) trait CellReader {
    fn cell(&self, himo_id: u16, eid: u32) -> Option<u32>;
    fn vocab_lookup(&self, text: &str) -> Option<u32>;
}

/// 登録済みの条件。 `EqText` は vocab id を初めて引けた時点で固定する (vocab id は
/// 一度振られたら変わらない)。
enum Pred {
    Eq(u16, u32),
    EqText(u16, String, OnceLock<u32>),
    Range(u16, u32, u32),
    /// sort + dedup 済み。
    In(u16, Vec<u32>),
    Present(u16),
}

impl Pred {
    fn compile(p: LivePred) -> Self {
        match p {
            LivePred::Eq { himo_id, value } => Pred::Eq(himo_id, value),
            LivePred::EqText { himo_id, text } => Pred::EqText(himo_id, text, OnceLock::new()),
            LivePred::Range { himo_id, lo, hi } => Pred::Range(himo_id, lo, hi),
            LivePred::In { himo_id, mut values } => {
                values.sort_unstable();
                values.dedup();
                Pred::In(himo_id, values)
            }
            LivePred::Present { himo_id } => Pred::Present(himo_id),
        }
    }

    fn matches(&self, r: &impl CellReader, eid: u32) -> bool {
        match self {
            Pred::Eq(h, v) => r.cell(*h, eid) == Some(*v),
            Pred::EqText(h, text, vid) => {
                let want = match vid.get() {
                    Some(v) => *v,
                    None => match r.vocab_lookup(text) {
                        Some(v) => *vid.get_or_init(|| v),
                        // vocab に無い = 誰もこの文字列をぶら下げていない
                        None => return false,
                    },
                };
                r.cell(*h, eid) == Some(want)
            }
            Pred::Range(h, lo, hi) => matches!(r.cell(*h, eid), Some(v) if *lo <= v && v <= *hi),
            Pred::In(h, vs) => matches!(r.cell(*h, eid), Some(v) if vs.binary_search(&v).is_ok()),
            Pred::Present(h) => r.cell(*h, eid).is_some(),
        }
    }
}

/// eid 添字の伸びる bitset。
#[derive(Default)]
struct Bits(Vec<u64>);

impl Bits {
    #[inline]
    fn get(&self, i: u32) -> bool {
        self.0
            .get((i >> 6) as usize)
            .is_some_and(|w| w & (1u64 << (i & 63)) != 0)
    }

    #[inline]
    fn put(&mut self, i: u32, on: bool) {
        let w = (i >> 6) as usize;
        if w >= self.0.len() {
            if !on {
                return;
            }
            self.0.resize(w + 1, 0);
        }
        let m = 1u64 << (i & 63);
        if on {
            self.0[w] |= m;
        } else {
            self.0[w] &= !m;
        }
    }

    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().enumerate().flat_map(|(wi, &w)| {
            let mut w = w;
            std::iter::from_fn(move || {
                if w == 0 {
                    return None;
                }
                let b = w.trailing_zeros();
                w &= w - 1;
                Some(((wi as u32) << 6) | b)
            })
        })
    }
}

#[derive(Default)]
struct Membership {
    /// 今の Column 状態で条件を満たす eid。
    current: Bits,
    /// 最後の poll で呼び手に渡した状態 (= 呼び手が積分済みの集合)。
    reported: Bits,
    /// 最後の poll 以降に一度でも条件を外れた eid。 reported かつ current でも、 これが
    /// 立っていれば 「出て入り直した」 として removed + added の両方を出す。
    left: Bits,
    /// dirty list の重複防止。
    dirty_mark: Bits,
    /// 最後の poll 以降に評価が変わりうる eid。
    dirty: Vec<u32>,
    count: usize,
}

impl Membership {
    fn apply(&mut self, eid: u32, now: bool) {
        let was = self.current.get(eid);
        if was == now {
            return;
        }
        self.current.put(eid, now);
        if now {
            self.count += 1;
        } else {
            self.count -= 1;
            self.left.put(eid, true);
        }
        if !self.dirty_mark.get(eid) {
            self.dirty_mark.put(eid, true);
            self.dirty.push(eid);
        }
    }
}

pub(crate) struct LiveState {
    id: u64,
    preds: Vec<Pred>,
    /// route を張る himo (重複除去済み)。
    himos: Vec<u16>,
    m: Mutex<Membership>,
}

impl LiveState {
    fn eval(&self, r: &impl CellReader, eid: u32) -> bool {
        self.preds.iter().all(|p| p.matches(r, eid))
    }

    /// eid を現在の Column 状態で評価し直して集合に反映する。
    fn reeval(&self, r: &impl CellReader, eid: u32) {
        let mut m = self.m.lock();
        // 評価は mutex の中 — 同じ eid への並行通知で古い評価が新しい評価を上書きしない
        // (module doc 「正しさの根拠」)。
        let now = self.eval(r, eid);
        m.apply(eid, now);
    }
}

/// engine 1 個につき 1 つ。 himo_id → その himo を条件に含む購読。
pub(crate) struct LiveRegistry {
    /// 登録中の購読数。 0 なら touch は即 return (書き込み hot path のコスト = これ 1 回)。
    active: AtomicUsize,
    routes: RwLock<Vec<Vec<Arc<LiveState>>>>,
    next_id: AtomicUsize,
    /// poll が返す EntityId の peer prefix (`Engine::set_peer_id` が追従させる)。
    peer: AtomicU32,
}

impl LiveRegistry {
    pub(crate) fn new(peer: u32) -> Self {
        Self {
            active: AtomicUsize::new(0),
            routes: RwLock::new(Vec::new()),
            next_id: AtomicUsize::new(0),
            peer: AtomicU32::new(peer),
        }
    }

    pub(crate) fn set_peer(&self, peer: u32) {
        self.peer.store(peer, Ordering::Release);
    }

    /// Column の `(himo_id, eid)` を書き換えた **後** (himo の write_lock を離した後) に呼ぶ。
    #[inline]
    pub(crate) fn touch(&self, r: &impl CellReader, himo_id: u16, eid: u32) {
        if self.active.load(Ordering::Acquire) == 0 {
            return;
        }
        self.touch_slow(r, himo_id, eid);
    }

    #[cold]
    fn touch_slow(&self, r: &impl CellReader, himo_id: u16, eid: u32) {
        let routes = self.routes.read();
        if let Some(subs) = routes.get(himo_id as usize) {
            for s in subs {
                s.reeval(r, eid);
            }
        }
    }

    /// 購読を route に載せる (登録手順の 1 段目、 module doc 参照)。 barrier と初期集合の
    /// 走査は呼び手 (engine) がこの後にやる。
    pub(crate) fn register(self: &Arc<Self>, preds: Vec<LivePred>) -> LiveQuery {
        let mut himos: Vec<u16> = preds.iter().map(LivePred::himo_id).collect();
        himos.sort_unstable();
        himos.dedup();
        let state = Arc::new(LiveState {
            id: self.next_id.fetch_add(1, Ordering::Relaxed) as u64,
            preds: preds.into_iter().map(Pred::compile).collect(),
            himos,
            m: Mutex::new(Membership::default()),
        });
        {
            let mut routes = self.routes.write();
            for &h in &state.himos {
                let h = h as usize;
                if routes.len() <= h {
                    routes.resize_with(h + 1, Vec::new);
                }
                routes[h].push(state.clone());
            }
            // route を載せてから active を上げる (Release)。 touch は active (Acquire) →
            // routes.read の順なので、 active を見た touch は必ずこの route を見る。
            self.active.fetch_add(1, Ordering::Release);
        }
        LiveQuery { state, registry: self.clone() }
    }

    fn unregister(&self, id: u64) {
        let mut routes = self.routes.write();
        for subs in routes.iter_mut() {
            subs.retain(|s| s.id != id);
        }
        self.active.fetch_sub(1, Ordering::Release);
    }
}

/// 登録済みの live query。 drop で購読解除。
///
/// engine を借用しない (`Arc` で registry を持つ) ので、 struct に入れて持ち回れる。
/// `Send + Sync` — poll は別 thread からでもよい。
pub struct LiveQuery {
    state: Arc<LiveState>,
    registry: Arc<LiveRegistry>,
}

impl LiveQuery {
    /// 条件の himo 一覧 (engine の barrier 用)。
    pub(crate) fn himos(&self) -> &[u16] {
        &self.state.himos
    }

    /// 初期集合の候補を評価する (登録手順の 3 段目)。 候補に居ない eid は、 barrier 後の
    /// 書き込みなら touch が、 barrier 前の書き込みなら候補の走査が拾っている。
    pub(crate) fn seed(&self, r: &impl CellReader, candidates: impl IntoIterator<Item = u32>) {
        for eid in candidates {
            self.state.reeval(r, eid);
        }
    }

    /// 前回 poll からの差分を返し、 それを 「呼び手に渡した」 状態として記録する。
    /// 初回は登録時点の結果全体が `added` に入る。 eid は `query_by_id` / schema の `find()`
    /// と同じ形 (peer prefix 付き)。
    ///
    /// 他 thread の書き込み途中に呼ぶと、 条件の紐だけ書かれて残りの紐がまだの row が
    /// `added` に出うる (module doc 「追うもの / 追わないもの」)。
    pub fn poll(&self) -> LiveDelta {
        let peer = self.registry.peer.load(Ordering::Acquire);
        let mut m = self.state.m.lock();
        let mut dirty = std::mem::take(&mut m.dirty);
        dirty.sort_unstable();
        let mut delta = LiveDelta::default();
        for eid in dirty {
            m.dirty_mark.put(eid, false);
            let now = m.current.get(eid);
            let was = m.reported.get(eid);
            let left = m.left.get(eid);
            m.left.put(eid, false);
            let e = enchudb_oplog::make_eid(peer, eid);
            match (was, now) {
                (false, true) => delta.added.push(e),
                (true, false) => delta.removed.push(e),
                (true, true) if left => {
                    delta.removed.push(e);
                    delta.added.push(e);
                }
                _ => {}
            }
            m.reported.put(eid, now);
        }
        delta
    }

    /// 未 poll の変化があるか (無ければ `poll` は空を返す)。
    pub fn is_dirty(&self) -> bool {
        !self.state.m.lock().dirty.is_empty()
    }

    /// 現在の結果件数 (poll 済みかどうかに関係なく、 今 find したら返る件数)。
    pub fn count(&self) -> usize {
        self.state.m.lock().count
    }

    /// `eid` が現在の結果に含まれるか。
    pub fn contains(&self, eid: EntityId) -> bool {
        self.state.m.lock().current.get(enchudb_oplog::eid_local(eid))
    }

    /// 現在の結果全体 (eid 昇順)。 poll の状態は変えない。
    pub fn members(&self) -> Vec<EntityId> {
        let peer = self.registry.peer.load(Ordering::Acquire);
        let m = self.state.m.lock();
        m.current.iter().map(|e| enchudb_oplog::make_eid(peer, e)).collect()
    }
}

impl Drop for LiveQuery {
    fn drop(&mut self) {
        self.registry.unregister(self.state.id);
    }
}

impl std::fmt::Debug for LiveQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveQuery")
            .field("himos", &self.state.himos)
            .field("count", &self.count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Column の代わり。 (himo, eid) → value。
    #[derive(Default)]
    struct Fake {
        cells: BTreeMap<(u16, u32), u32>,
        vocab: Vec<String>,
    }

    impl CellReader for Mutex<Fake> {
        fn cell(&self, h: u16, e: u32) -> Option<u32> {
            self.lock().cells.get(&(h, e)).copied()
        }
        fn vocab_lookup(&self, t: &str) -> Option<u32> {
            self.lock().vocab.iter().position(|v| v == t).map(|i| i as u32)
        }
    }

    fn write(reg: &LiveRegistry, f: &Mutex<Fake>, h: u16, e: u32, v: Option<u32>) {
        match v {
            Some(v) => f.lock().cells.insert((h, e), v),
            None => f.lock().cells.remove(&(h, e)),
        };
        reg.touch(f, h, e);
    }

    #[test]
    fn delta_consolidates_and_flags_reentry() {
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        let q = reg.register(vec![LivePred::Eq { himo_id: 0, value: 30 }]);
        write(&reg, &f, 0, 1, Some(30));
        write(&reg, &f, 0, 2, Some(30));
        assert_eq!(q.poll(), LiveDelta { added: vec![1, 2], removed: vec![] });
        // 出て戻る (値変更 → 元に戻す) = 同一 entity の往復 → removed + added
        write(&reg, &f, 0, 1, Some(31));
        write(&reg, &f, 0, 1, Some(30));
        assert_eq!(q.poll(), LiveDelta { added: vec![1], removed: vec![1] });
        // 入って出る (未報告のまま) = 打ち消し → 何も出ない
        write(&reg, &f, 0, 3, Some(30));
        write(&reg, &f, 0, 3, None);
        assert!(q.poll().is_empty());
        assert_eq!(q.count(), 2);
    }

    #[test]
    fn eq_text_matches_after_vocab_appears() {
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        let q = reg.register(vec![LivePred::EqText { himo_id: 4, text: "東京".into() }]);
        f.lock().vocab.push("大阪".into());
        write(&reg, &f, 4, 9, Some(0));
        assert!(q.poll().is_empty());
        f.lock().vocab.push("東京".into());
        write(&reg, &f, 4, 9, Some(1));
        assert_eq!(q.poll().added, vec![9]);
    }

    #[test]
    fn drop_unregisters() {
        let reg = Arc::new(LiveRegistry::new(0));
        let q = reg.register(vec![LivePred::Present { himo_id: 2 }]);
        assert_eq!(reg.active.load(Ordering::Acquire), 1);
        drop(q);
        assert_eq!(reg.active.load(Ordering::Acquire), 0);
        assert!(reg.routes.read().iter().all(Vec::is_empty));
    }
}
