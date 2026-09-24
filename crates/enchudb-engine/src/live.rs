//! Live query (クエリ購読) — 「この条件に当てはまる entity 集合」 の **差分** を購読する。
//!
//! 普通の query は 1 回聞いて 1 回答えが返る。 live query は 1 回登録すると、 以降の
//! 書き込みで答えが変わった分だけを [`LiveQuery::poll`] が返す。 結果集合は空から
//! 始まり、 poll が返す差分 (added / removed) を順に積めば常に 「今 find したら返る集合」
//! に一致する (DBSP の Z-set 積分と同じ見方。 初回 poll は登録時点の全件を added で返す)。
//!
//! # 条件の形
//!
//! 条件は根の entity `x0` についての AND。 各条件は
//!
//! - `x0` 自身の紐 1 本への条件 (`Eq` / `EqText` / `Range` / `In` / `Present`)、 または
//! - ref 紐を順にたどった先の entity の紐への条件 (`Via`)。 例: 「所属会社の所在地が東京」 =
//!   `Via { path: [users.company], pred: Eq(companies.city, 東京) }`
//!
//! `Via` は同じ ref の並びを共有するものが 1 本の道にまとまり、 購読は **ref の木** になる
//! (根 = `x0`、 枝 = ref 紐、 各節 = その先の entity への条件)。
//!
//! `Or` は AND の OR (枝) に展開し (`Via` の中の `Or` は外に出す)、 枝ごとに AND の購読を張る。
//! `Or` の購読は枝の差分を枝ごとの集合に積み、 「どの枝にも居ない ↔ どれかに居る」 をまたいだら出入り
//! (`Union`)。 同じ形の枝 (`city = 東京 OR city = 大阪`) は同じ family の member なので、
//! 書き込みのコストは AND の購読と変わらない。
//!
//! # 仕組み — 書き込みで印、 poll で展開
//!
//! ref は 「その entity からは 1 本だけ」 (関数従属) なので、 会社の所在地の値は会社の entity に
//! 1 個ぶら下がっているだけ。 書き込み時に社員全員へ配る必要は無い:
//!
//! 1. **書き込み** — Column の `(himo, eid)` を書いたら、 その紐を条件 (または枝) に持つ節に
//!    `eid` の印を付けるだけ (O(1)、 評価しない)
//! 2. **poll** (と `count` / `contains` / `members` / [`Engine::poll_live`](crate::engine::Engine::poll_live)) — 深い節から順に、
//!    印の付いた entity を **現在の Column 状態で** 評価し直す。 根でない節は 「その entity から
//!    下の部分条件の答え」 を節ごとに記録し、 **答えが変わった entity だけ** を ref 紐の常設
//!    逆引き索引 (その entity を指している親) で 1 段上に展開して印を付ける。 根では結果集合を
//!    更新し、 最後の poll で返した状態との差を差分として返す
//!
//! join 用の索引は作らない — 逆引きは engine の常設索引 (Cylinder) をそのまま使う。
//!
//! 答えの変わらない書き換え (条件が 「東京」 の時に京都 → 大阪) は展開しない。 根に条件が
//! 無ければ、 展開した数 = 結果の変化数 (出力に比例)。 根にも条件がある (「東京の会社 かつ
//! 30 歳以上」) と、 会社の答えが変わった時に配下全員を評価するので、 展開数は結果の変化数より
//! 多くなりうる。 **ref の先の条件が別々の枝に 2 本以上ある** (会社が東京 かつ 部署が営業) と、
//! 答えの変化が片方の枝だけで起きても配下を評価する — このクラスには既知の下限があり
//! (OMv 予想の下で更新と列挙の両方を劣線形にはできない)、 動くが上の比例は保証しない。
//!
//! # 購読の共有 (family)
//!
//! 形が同じで値だけ違う購読 (`company.city = 東京` と `company.city = 大阪`) は engine の中で
//! 1 本に束ねる。 `Eq` / `EqText` の値と `Range` の両端を **穴** とみなし、 穴以外が同じ条件を
//! 1 つの family にまとめ、 購読 1 本 = family の member (穴の値の並び = 鍵) にする:
//!
//! - 印付け・評価・記録は family で 1 回だけ。 節の答えは真偽でなく **鍵** (その節から下の穴の
//!   値の並びを id にしたもの、 偽 = なし)。 根の鍵が member の鍵と一致する根がその member の結果
//! - 鍵の表には member の鍵の射影だけを載せる。 購読の無い値は表に無い = 偽 と同じに扱うので、
//!   購読の無い値どうしの書き換え (東京だけを購読している時の 京都 → 大阪) は展開しない
//! - 書き込み 1 回のコストは購読の数によらない。 [`Engine::poll_live`](crate::engine::Engine::poll_live) は出入りのあった member
//!   だけを返す (購読を 1 本ずつ poll すると、 その呼び出し自体が購読の数に比例する)
//! - 鍵の表には **今居る** member の鍵だけを載せる (使っている member の数を数えて 0 で外す)。
//!   購読を張り替え続けても表・状態は伸びない。 鍵の id は使い回さない
//! - `In` の値と、 2 本目以降の `Range` は穴にしていない (値が違えば別の family)
//!
//! ## 範囲の穴
//!
//! `Range` は 1 本目 (正規形の順) だけを穴にする。 値の穴と違い、 1 つの値が多数の member の範囲に
//! 同時に入るので、 範囲は節の鍵に入れない:
//!
//! - 範囲の穴の節は **値そのもの** を答えに持ち、 根までの道の上の節がそれを運ぶ (`Family::on_path`)。
//!   根では 「鍵が一致し、 値が範囲に入る」 member の集合に居る
//! - 値の軸を全 member の範囲の端で区切った **帯** (`Slabs`) の中では、 どの member にとっても
//!   値の区別が付かない。 道の上の節は 「鍵か帯が変わった時だけ」 親へ展開する。 どの member の
//!   範囲にも入らない帯の値は偽と同じ
//! - 根で鍵を共有する member は範囲の索引 (`Ivs`) を持つ。 値が同じ鍵の中で動いたら、 端が旧値と
//!   新値の間にある member だけ (か、 両方の値を含む member の突き合わせの安い方) が出入りする
//!   (入れ子の閾値 `age > k` を多数張っても、 1 歳動いた時に触るのはその間の端の member だけ)
//! - 範囲の穴は 1 本まで: 2 本あると member の範囲が 2 次元の箱になり、 根の振り分けが 1 次元の
//!   区間の索引で済まない
//!
//! # 会社単位の購読 ([`GroupedLiveQuery`])
//!
//! `Via` の条件の結果を ref の 1 段目の先 (会社) 単位で持つ。 購読の中身は 「会社への購読」 と 「根
//! (社員) への条件」 の組で、 差分は会社の eid、 社員は必要な時に常設の逆引き索引から引く。 会社の
//! 所在地を 1 個書き換えた時の差分は会社 1 件 (配下が何人でも O(1))、 社員の付け替えは印を付けない。
//!
//! # 集計の購読 ([`LiveCounts`])
//!
//! 結果を group の列の値ごとに数えた件数の live 版。 group の列を 「値を根まで運ぶ穴」 にする
//! (範囲の穴と同じ道。 ただし帯で刈り込まず、 値が変わるたびに運ぶ)。 件数は根の鍵ごとに
//! `値 → 件数` で持ち、 根の答えが変わった時に旧値の件数を減らして新値の件数を増やす — 記録と同じ
//! 所で動かすので、 登録と並行した書き込みでも件数と記録が食い違わない。 同じ鍵の集計の購読は件数を
//! 共有し、 購読ごとには 「動いた group」 と 「最後に渡した件数」 だけを持つ。
//!
//! group の列が ref の先 (`company.city`) なら、 根は group の値でなく 1 段目の先 (会社) を
//! 記録し、 件数は 「会社ごとに、 その会社を指して数えている根の数と、 今数えている group」 の
//! 部分和で持つ (`Partial`)。 会社の所在地が変わったら部分和をまとめて移す = 配下が何人でも
//! O(鍵の数)。 根が出入りした時はその会社の今の group で数える。 登録時に 1 段目の先にも印を
//! 付けて記録を作る (記録の無い会社が最初に変わると配下を全部評価し直すことになるので)。
//!
//! # 状態の大きさ
//!
//! member ごとの状態 (最後に報告した集合など) は、 疎なうちは要素数に比例する集合 (roaring と
//! 同じ形)、 密になったら平らな bitset。 節ごとの記録と根の現在の鍵は family に 1 つ。 購読
//! 1 本あたりのメモリは entity 空間の広さでなく結果の大きさで決まる。
//!
//! # 正しさの根拠
//!
//! **印の付いた entity は現在の Column を読み直して評価する** (旧値を引き回さない) ので、 poll
//! 間で打ち消し合う変化は自動で消える。 各書き込みは Column に出た後に印を付けるので、 印を消費
//! した後の評価は必ずその書き込みを見ている。 印の付いた entity は必ず次の poll で評価される。
//!
//! 親を評価する時、 子の答えは **子の記録を使う** (記録が不明な時だけ Column から評価し直す)。
//! 記録が現在の Column とずれるのは、 記録の後に子が書き換わった時だけで、 その書き込みは子に
//! 印を付けている → 次の poll が子を評価し直し、 答えが変わっていれば親へ展開する。 よって
//! 「答えが変わった時だけ展開」 でも、 親は常に子の最新の記録と整合する。 記録は 「印を消費して
//! 評価した時」 にだけ書く。
//!
//! 記録の意味が変わるもう 1 つの場合は **鍵の表が増えた時** (購読の無かった値が購読された):
//! 「偽」 と記録した entity が今は真でありうる。 member を有効化する時、 その member の各穴の値を
//! 持つ entity から根の手前まで遡って記録を不明に戻す (`Family::forget_stale`)。
//!
//! 範囲の穴では、 道の上の節の記録の値は 「子の今の値と同じ帯のどこか」 (帯の中の変化は運ばない)。
//! member が増えて帯が割れると、 両者が別の帯に分かれうる。 有効化する時、 割れた帯と新しく範囲に
//! 入った値の entity を範囲の穴の節から根の手前まで不明に戻し、 評価し直させる
//! (`Family::forget_regions`、 test `range_split_refreshes_values_carried_up_within_a_slab`)。
//! member が外れて帯が併さる時は、 記録の値は併さった帯の中に居続けるので直さなくてよい。
//!
//! 登録と並行する書き込みの取りこぼしは、 登録側が 「購読を route に載せる → 条件の全紐の
//! write_lock を 1 度取って離す (barrier) → 初期候補を数える」 の順で塞ぐ:
//!
//! - 書き込み W の lock が barrier より先 → W の Column 書き込みは初期候補の走査に見える
//! - W の lock が barrier より後 → barrier の unlock が W の lock に同期するので、 W は
//!   route 上の購読を必ず見て印を付ける
//!
//! **この保証は lock 順序が根拠であって、 並行 test の緑が根拠ではない** — 実コードの並行
//! test は barrier を外しても落ちなかった (10 run 中 0 回)。 barrier の gate は loom model
//! `tests/loom_live_subscribe.rs`。
//!
//! 印付け (書き込み側) と評価 (poll 側) は別の lock なので、 大きな展開をしている poll の
//! 最中も書き込みは待たない。 印の置き場は書き手の thread ごとに分けた shard で、 書き手どうしも
//! 同じ lock を取り合わない。 poll は印の付いた shard だけを開ける — 書き手は印を置いて lock を
//! 離した後に shard の bit を立て、 poll は bit を落としてから shard を開ける (この順序の gate は
//! loom model `tests/loom_live_dirty.rs`)。 書き込み側は route の一覧を epoch で読む (共有の
//! lock の counter に書かない)。
//!
//! # 追うもの / 追わないもの
//!
//! - 追う: **集合への出入り**
//! - 追わない: 集合に入ったままの entity の **中身の変化** (条件に無い紐の書き換え、 条件を
//!   満たしたままの値変更)。 中身が要るなら poll 後に読む
//! - 条件の紐が書かれた瞬間に集合に入る。 row の他の紐はまだ書かれていないことがある
//!   (insert は紐を 1 本ずつ書く / sync は op を順に適用する)。 書き込み呼び出しが返った後の
//!   poll なら揃っている
//! - 状態はメモリ上だけ。 reopen 後は購読し直し、 初回 poll は全件を added で返す
//! - 削除された eid の slot が poll の間に再利用されて再び条件を満たした場合は、 同じ eid が
//!   `removed` と `added` の両方に出る (別 entity になった可能性があるので、 読み直しを促す)。
//!   適用順は removed → added

use crossbeam_epoch as epoch;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use enchudb_oplog::EntityId;

/// live query の条件 1 個。 全部 **根の entity についての AND** として組み合わさる。
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
    /// `lo <= 値 <= hi` (両端含む)。 `lo > hi` は常に偽。
    Range { himo_id: u16, lo: u32, hi: u32 },
    /// 値が `values` のどれか。
    In { himo_id: u16, values: Vec<u32> },
    /// 値が何か tie されている (= その紐を持つ全 entity)。
    Present { himo_id: u16 },
    /// ref 紐 `path` を順にたどった先の entity で `pred` が真。 ref が張られていない / 先の
    /// entity に値が無ければ偽。 `path` の紐は全部 Ref 型であること。
    Via { path: Vec<u16>, pred: Box<LivePred> },
    /// 枝のどれかが真 (各枝は条件の AND)。 `Via` の中にも書ける (`company.city = 東京 OR
    /// company.city = 大阪` = `Via { company, Or([[city = 東京], [city = 大阪]]) }`)。 枝も枝の中の
    /// AND も空は不可。 展開した枝 (AND の OR に直した数) は [`MAX_BRANCHES`] まで。
    Or(Vec<Vec<LivePred>>),
}

/// `Or` を AND の OR に展開した枝の数の上限 (`(a OR b) AND (c OR d) AND ...` は枝が掛け算で増える)。
pub const MAX_BRANCHES: usize = 64;

impl LivePred {
    /// 条件に出てくる全ての紐 (ref の道を含む)。
    pub fn himos(&self) -> Vec<u16> {
        let mut out = Vec::new();
        self.collect_himos(&mut out);
        out
    }

    fn collect_himos(&self, out: &mut Vec<u16>) {
        match self {
            LivePred::Eq { himo_id, .. }
            | LivePred::EqText { himo_id, .. }
            | LivePred::Range { himo_id, .. }
            | LivePred::In { himo_id, .. }
            | LivePred::Present { himo_id } => out.push(*himo_id),
            LivePred::Via { path, pred } => {
                out.extend_from_slice(path);
                pred.collect_himos(out);
            }
            LivePred::Or(branches) => {
                for p in branches.iter().flatten() {
                    p.collect_himos(out);
                }
            }
        }
    }

    /// ref の道 (`Via` の `path`) に出てくる紐。 engine が Ref 型かを検証する用。
    pub fn ref_himos(&self) -> Vec<u16> {
        let mut out = Vec::new();
        self.collect_ref_himos(&mut out);
        out
    }

    fn collect_ref_himos(&self, out: &mut Vec<u16>) {
        match self {
            LivePred::Via { path, pred } => {
                out.extend_from_slice(path);
                pred.collect_ref_himos(out);
            }
            LivePred::Or(branches) => {
                for p in branches.iter().flatten() {
                    p.collect_ref_himos(out);
                }
            }
            _ => {}
        }
    }
}

/// 条件 (AND) を `Or` の無い AND の OR (枝) に展開する。 `Via` の中の `Or` は外に出す
/// (`Via(p, a OR b)` = `Via(p, a) OR Via(p, b)`)。
pub(crate) fn dnf(preds: Vec<LivePred>) -> Result<Vec<Vec<LivePred>>, String> {
    let mut out: Vec<Vec<LivePred>> = vec![Vec::new()];
    for p in preds {
        let alts = alternatives(p)?;
        let mut next = Vec::with_capacity(out.len() * alts.len());
        for base in &out {
            for alt in &alts {
                let mut b = base.clone();
                b.extend(alt.iter().cloned());
                next.push(b);
            }
        }
        if next.len() > MAX_BRANCHES {
            return Err(format!("Or expands to more than {MAX_BRANCHES} branches"));
        }
        out = next;
    }
    Ok(out)
}

/// 条件 1 個の選択肢 (それぞれ AND)。
fn alternatives(p: LivePred) -> Result<Vec<Vec<LivePred>>, String> {
    Ok(match p {
        LivePred::Or(branches) => {
            if branches.is_empty() {
                return Err("Or with no branches".into());
            }
            let mut out = Vec::new();
            for b in branches {
                if b.is_empty() {
                    return Err("Or with an empty branch".into());
                }
                out.extend(dnf(b)?);
                if out.len() > MAX_BRANCHES {
                    return Err(format!("Or expands to more than {MAX_BRANCHES} branches"));
                }
            }
            out
        }
        LivePred::Via { path, pred } => alternatives(*pred)?
            .into_iter()
            .map(|conj| conj.into_iter().map(|q| LivePred::Via { path: path.clone(), pred: Box::new(q) }).collect())
            .collect(),
        leaf => vec![vec![leaf]],
    })
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
    /// `himo_id` の値が `value` である entity (常設逆引き索引)。
    fn pull(&self, himo_id: u16, value: u32) -> Vec<u32>;
    /// `himo_id` に何か値を持つ entity。
    fn with_himo(&self, himo_id: u16) -> Vec<u32>;
    /// `himo_id` の値が `lo..=hi` の entity (順不同)。
    fn pull_range(&self, himo_id: u16, lo: u32, hi: u32) -> Vec<u32>;
    /// `pull(himo_id, value)` の件数 (O(1))。
    fn pull_len(&self, himo_id: u16, value: u32) -> usize;
}

// ─────────────────────────── 疎な状態 ───────────────────────────

/// entity の集合。 疎なうちは [`Roaring`] (大きさが要素数に比例し、 entity 空間の広さに
/// よらない — 購読 1 本の報告状態が結果の大きさで済む)、 要素が範囲の 1/16 を超えたら平らな
/// bitset に切り替える (密な集合は添字 1 回で引ける方が速く、 大きさも同程度)。 1/64 を
/// 下回ったら疎に戻す。
enum Bits {
    Sparse(Roaring, usize),
    Flat(Vec<u64>, usize),
}

impl Default for Bits {
    fn default() -> Self {
        Bits::Sparse(Roaring::default(), 0)
    }
}

impl Bits {
    #[inline]
    fn get(&self, i: u32) -> bool {
        match self {
            Bits::Sparse(r, _) => r.get(i),
            Bits::Flat(w, _) => w.get((i >> 6) as usize).is_some_and(|w| w & (1u64 << (i & 63)) != 0),
        }
    }

    #[inline]
    fn put(&mut self, i: u32, on: bool) {
        match self {
            Bits::Sparse(r, n) => {
                if r.put(i, on) {
                    if on {
                        *n += 1;
                        if *n > 4096 && *n * 16 >= r.span() {
                            self.make_flat();
                        }
                    } else {
                        *n -= 1;
                    }
                }
            }
            Bits::Flat(w, n) => {
                let wi = (i >> 6) as usize;
                let m = 1u64 << (i & 63);
                if wi >= w.len() {
                    if !on {
                        return;
                    }
                    w.resize(wi + 1, 0);
                }
                let was = w[wi] & m != 0;
                if on && !was {
                    w[wi] |= m;
                    *n += 1;
                } else if !on && was {
                    w[wi] &= !m;
                    *n -= 1;
                    // 範囲 (w.len() * 64) の 1/64 を下回ったら
                    if *n < w.len() && *n < 2048 {
                        self.make_sparse();
                    }
                }
            }
        }
    }

    #[cold]
    fn make_flat(&mut self) {
        let Bits::Sparse(r, n) = self else { return };
        let mut w: Vec<u64> = Vec::new();
        for i in r.iter() {
            let wi = (i >> 6) as usize;
            if wi >= w.len() {
                w.resize(wi + 1, 0);
            }
            w[wi] |= 1u64 << (i & 63);
        }
        *self = Bits::Flat(w, *n);
    }

    #[cold]
    fn make_sparse(&mut self) {
        let Bits::Flat(_, n) = self else { return };
        let n = *n;
        let mut r = Roaring::default();
        for i in self.iter() {
            r.put(i, true);
        }
        *self = Bits::Sparse(r, n);
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u32> + '_> {
        match self {
            Bits::Sparse(r, _) => Box::new(r.iter()),
            Bits::Flat(w, _) => Box::new(w.iter().enumerate().flat_map(|(wi, &w)| {
                let mut w = w;
                std::iter::from_fn(move || {
                    if w == 0 {
                        return None;
                    }
                    let b = w.trailing_zeros();
                    w &= w - 1;
                    Some(((wi as u32) << 6) | b)
                })
            })),
        }
    }
}

/// 65536 entity ごとの chunk を、 疎なうちは昇順の下位 16 bit 列、 4096 個を超えたら bitmap
/// で持つ集合 (roaring と同じ形)。
#[derive(Default)]
struct Roaring(Vec<(u16, Chunk)>);

enum Chunk {
    Sparse(Vec<u16>),
    /// bitmap と立っている bit 数。
    Dense(Box<[u64; 1024]>, u32),
}

const SPARSE_MAX: usize = 4096;

impl Chunk {
    #[inline]
    fn get(&self, lo: u16) -> bool {
        match self {
            Chunk::Sparse(v) => v.binary_search(&lo).is_ok(),
            Chunk::Dense(b, _) => b[(lo >> 6) as usize] & (1u64 << (lo & 63)) != 0,
        }
    }

    /// 立てる / 落とす。 (変わったか, 空になったか)。
    fn put(&mut self, lo: u16, on: bool) -> (bool, bool) {
        let mut changed = false;
        match self {
            Chunk::Sparse(v) => match (v.binary_search(&lo), on) {
                (Err(i), true) => {
                    changed = true;
                    v.insert(i, lo);
                    if v.len() > SPARSE_MAX {
                        let mut b = Box::new([0u64; 1024]);
                        for &x in v.iter() {
                            b[(x >> 6) as usize] |= 1u64 << (x & 63);
                        }
                        *self = Chunk::Dense(b, (SPARSE_MAX + 1) as u32);
                    }
                }
                (Ok(i), false) => {
                    changed = true;
                    v.remove(i);
                }
                _ => {}
            },
            Chunk::Dense(b, n) => {
                let (w, m) = ((lo >> 6) as usize, 1u64 << (lo & 63));
                let was = b[w] & m != 0;
                if on && !was {
                    changed = true;
                    b[w] |= m;
                    *n += 1;
                } else if !on && was {
                    changed = true;
                    b[w] &= !m;
                    *n -= 1;
                    if (*n as usize) <= SPARSE_MAX / 2 {
                        let v = Chunk::dense_iter(b).collect();
                        *self = Chunk::Sparse(v);
                    }
                }
            }
        }
        let empty = match self {
            Chunk::Sparse(v) => v.is_empty(),
            Chunk::Dense(_, n) => *n == 0,
        };
        (changed, empty)
    }

    fn dense_iter(b: &[u64; 1024]) -> impl Iterator<Item = u16> + '_ {
        b.iter().enumerate().flat_map(|(wi, &w)| {
            let mut w = w;
            std::iter::from_fn(move || {
                if w == 0 {
                    return None;
                }
                let bit = w.trailing_zeros();
                w &= w - 1;
                Some(((wi as u16) << 6) | bit as u16)
            })
        })
    }
}

impl Roaring {
    /// chunk の位置。 chunk が 0 から隙間なく並んでいる間は添字 = 上位 16 bit なので、 探索
    /// せずに当てる。
    #[inline]
    fn chunk(&self, hi: u16) -> Result<usize, usize> {
        if self.0.get(hi as usize).is_some_and(|(h, _)| *h == hi) {
            return Ok(hi as usize);
        }
        self.0.binary_search_by_key(&hi, |(h, _)| *h)
    }

    /// 要素が入りうる範囲の大きさ (最後の chunk の終わり)。
    fn span(&self) -> usize {
        self.0.last().map_or(0, |(h, _)| (*h as usize + 1) << 16)
    }

    #[inline]
    fn get(&self, i: u32) -> bool {
        match self.chunk((i >> 16) as u16) {
            Ok(k) => self.0[k].1.get(i as u16),
            Err(_) => false,
        }
    }

    /// 変わったら true。
    #[inline]
    fn put(&mut self, i: u32, on: bool) -> bool {
        let hi = (i >> 16) as u16;
        let k = match self.chunk(hi) {
            Ok(k) => k,
            Err(k) => {
                if !on {
                    return false;
                }
                self.0.insert(k, (hi, Chunk::Sparse(Vec::new())));
                k
            }
        };
        let (changed, empty) = self.0[k].1.put(i as u16, on);
        if empty {
            self.0.remove(k);
        }
        changed
    }

    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().flat_map(|(hi, c)| {
            let base = (*hi as u32) << 16;
            let it: Box<dyn Iterator<Item = u16> + '_> = match c {
                Chunk::Sparse(v) => Box::new(v.iter().copied()),
                Chunk::Dense(b, _) => Box::new(Chunk::dense_iter(b)),
            };
            it.map(move |lo| base | lo as u32)
        })
    }
}

/// 1024 entity を 1 page にした u32 配列 (0 = 未設定)。 触った page だけ確保する。
#[derive(Default)]
struct Words(Vec<Option<Box<[u32; 1024]>>>);

impl Words {
    #[inline]
    fn get(&self, i: u32) -> u32 {
        match self.0.get((i >> 10) as usize) {
            Some(Some(p)) => p[(i & 1023) as usize],
            _ => 0,
        }
    }

    #[inline]
    fn put(&mut self, i: u32, v: u32) {
        let pi = (i >> 10) as usize;
        if pi >= self.0.len() {
            if v == 0 {
                return;
            }
            self.0.resize_with(pi + 1, || None);
        }
        let slot = &mut self.0[pi];
        if slot.is_none() && v == 0 {
            return;
        }
        slot.get_or_insert_with(|| Box::new([0; 1024]))[(i & 1023) as usize] = v;
    }

    fn iter(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(pi, p)| p.as_ref().map(|p| (pi as u32, p)))
            .flat_map(|(pi, p)| {
                p.iter().enumerate().filter(|(_, v)| **v != 0).map(move |(i, &v)| ((pi << 10) | i as u32, v))
            })
    }
}

/// 印リスト。 追記するだけで、 重複は取り出す時 (と溜まりすぎた時) に畳む — 印は poll の
/// たびに空になる一時的な集合なので、 付けるたびに整列した集合へ挿入するより安い。 大きさは
/// 印の付いた entity の数の高々 2 倍 + 定数。
#[derive(Default)]
struct Marks {
    list: Vec<u32>,
    /// 最後に畳んだ時の長さ。
    clean: usize,
}

impl Marks {
    #[inline]
    fn add(&mut self, eid: u32) {
        self.list.push(eid);
        if self.list.len() > 2 * self.clean + 64 {
            self.compact();
        }
    }

    fn compact(&mut self) {
        self.list.sort_unstable();
        self.list.dedup();
        self.clean = self.list.len();
    }

    /// 昇順・重複なしで取り出して空にする。
    fn take(&mut self) -> Vec<u32> {
        self.compact();
        self.take_raw()
    }

    /// 整列せずに取り出して空にする (書き込み側の lock の下で呼ぶ — 整列は lock の外で)。
    fn take_raw(&mut self) -> Vec<u32> {
        self.clean = 0;
        std::mem::take(&mut self.list)
    }

    fn is_empty(&self) -> bool {
        self.list.is_empty()
    }
}

/// 根でない節の entity の記録値。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Rec {
    Unknown,
    /// `None` = 部分条件が偽、 `Some(k)` = 真で鍵 id `k`。
    Known(Option<u32>),
}

/// 節ごとの entity → 記録。 鍵 id が 0 しか無い間 (その節から下に値の穴が無い、 または
/// member の値が 1 種類) は 2 bit/entity、 鍵が増えたら u32/entity に広げる。
///
/// 根では 「現在の鍵」 を持つ (`key` / `set_root`)。 根は偽と不明を区別しないので、 偽のために
/// page を確保しない = 根の状態は結果の大きさに比例する。
enum KeyStore {
    Bits { known: Bits, yes: Bits },
    /// 0 = 不明、 1 = 偽、 2 + k = 真で鍵 id k。
    Ids(Words),
}

impl Default for KeyStore {
    fn default() -> Self {
        KeyStore::Bits { known: Bits::default(), yes: Bits::default() }
    }
}

impl KeyStore {
    #[inline]
    fn get(&self, e: u32) -> Rec {
        match self {
            KeyStore::Bits { known, yes } => {
                if known.get(e) {
                    Rec::Known(yes.get(e).then_some(0))
                } else {
                    Rec::Unknown
                }
            }
            KeyStore::Ids(w) => match w.get(e) {
                0 => Rec::Unknown,
                1 => Rec::Known(None),
                v => Rec::Known(Some(v - 2)),
            },
        }
    }

    #[inline]
    fn set(&mut self, e: u32, v: Option<u32>) {
        self.widen(v);
        match self {
            KeyStore::Bits { known, yes } => {
                known.put(e, true);
                yes.put(e, v.is_some());
            }
            KeyStore::Ids(w) => w.put(e, v.map_or(1, |k| k + 2)),
        }
    }

    #[inline]
    fn forget(&mut self, e: u32) {
        match self {
            KeyStore::Bits { known, .. } => known.put(e, false),
            KeyStore::Ids(w) => w.put(e, 0),
        }
    }

    /// 根: 現在の鍵 (偽 / 未評価は `None`)。
    #[inline]
    fn key(&self, e: u32) -> Option<u32> {
        match self {
            KeyStore::Bits { yes, .. } => yes.get(e).then_some(0),
            KeyStore::Ids(w) => w.get(e).checked_sub(2),
        }
    }

    #[inline]
    fn set_root(&mut self, e: u32, v: Option<u32>) {
        self.widen(v);
        match self {
            KeyStore::Bits { yes, .. } => yes.put(e, v.is_some()),
            KeyStore::Ids(w) => w.put(e, v.map_or(0, |k| k + 2)),
        }
    }

    /// 根: 鍵が `k` の entity (昇順)。
    fn with_key(&self, k: u32) -> Vec<u32> {
        match self {
            KeyStore::Bits { yes, .. } if k == 0 => yes.iter().collect(),
            KeyStore::Bits { .. } => Vec::new(),
            KeyStore::Ids(w) => w.iter().filter(|&(_, v)| v == k + 2).map(|(e, _)| e).collect(),
        }
    }

    #[inline]
    fn widen(&mut self, v: Option<u32>) {
        if matches!(v, Some(k) if k >= 1) && matches!(self, KeyStore::Bits { .. }) {
            self.widen_slow();
        }
    }

    #[cold]
    fn widen_slow(&mut self) {
        let KeyStore::Bits { known, yes } = self else { return };
        let mut w = Words::default();
        for e in known.iter() {
            w.put(e, 1);
        }
        for e in yes.iter() {
            w.put(e, 2);
        }
        *self = KeyStore::Ids(w);
    }
}

/// 節の鍵 (穴の値 + 子の鍵 id の並び) → 鍵 id。 **今居る** member の鍵の射影だけを載せる
/// (射影を使う member の数を数え、 0 になったら外す — 購読の無くなった値を表に残すと、 その値
/// どうしの書き換えも展開されるようになる)。 節の鍵の長さは節ごとに一定なので、 鍵を昇順に詰めた
/// 1 本の配列を二分探索する (hash 不使用、 探索が連続したメモリの上で済む)。
///
/// id は使い回さない (記録に残った古い id が別の鍵と取り違えられないように)。 同じ鍵が外れた後に
/// また載ると新しい id になる — 古い id の記録は `Family::forget_stale` と候補の評価し直しが直す。
#[derive(Default)]
struct KeyTable {
    width: usize,
    /// 昇順の鍵を `width` 個ずつ詰めたもの。
    keys: Vec<u32>,
    /// `keys` の i 番目の鍵の id。
    ids: Vec<u32>,
    /// `keys` の i 番目の鍵を使っている member の数。
    refs: Vec<u32>,
    /// 次に振る id (= これまでに振った id の数)。
    next: u32,
}

impl KeyTable {
    fn new(width: usize) -> Self {
        KeyTable { width, ..Default::default() }
    }

    #[inline]
    fn search(&self, k: &[u32]) -> Result<usize, usize> {
        debug_assert_eq!(k.len(), self.width);
        let w = self.width;
        if w == 0 {
            return if self.ids.is_empty() { Err(0) } else { Ok(0) };
        }
        let (mut lo, mut hi) = (0, self.ids.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.keys[mid * w..mid * w + w].cmp(k) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    #[inline]
    fn find(&self, k: &[u32]) -> Option<u32> {
        self.search(k).ok().map(|i| self.ids[i])
    }

    /// 鍵を載せ (使う member を 1 増やし)、 id を返す。
    fn intern(&mut self, k: &[u32]) -> u32 {
        match self.search(k) {
            Ok(i) => {
                self.refs[i] += 1;
                self.ids[i]
            }
            Err(i) => {
                let id = self.next;
                self.next += 1;
                self.keys.splice(i * self.width..i * self.width, k.iter().copied());
                self.ids.insert(i, id);
                self.refs.insert(i, 1);
                id
            }
        }
    }

    /// 使う member を 1 減らし、 0 になったら外す。
    fn release(&mut self, k: &[u32]) {
        let Ok(i) = self.search(k) else { return };
        self.refs[i] -= 1;
        if self.refs[i] == 0 {
            let w = self.width;
            self.keys.drain(i * w..i * w + w);
            self.ids.remove(i);
            self.refs.remove(i);
        }
    }

    /// 載っている鍵の数。
    #[cfg(test)]
    fn len(&self) -> usize {
        self.ids.len()
    }
}

/// 範囲の穴の値の軸を、 member の範囲の端 (lo と hi + 1) で区切った帯。 帯の中の値は全 member に
/// とって区別が付かない (どの member の範囲にも同時に入るか同時に外れる) ので、 値が帯の中で
/// 動いても展開しない。 帯ごとに 「その帯を含む member の数」 を持ち、 0 の帯の値は偽と同じに扱う。
#[derive(Default)]
struct Slabs {
    /// 帯の始まり (昇順)。 帯 i = [starts[i], starts[i + 1])、 最後の帯は上限なし。
    starts: Vec<u32>,
    /// starts[i] を端に持つ member の数 (0 になったら外す)。
    refs: Vec<u32>,
    /// 帯 i を含む member の数。
    cover: Vec<u32>,
}

impl Slabs {
    /// `v` を含む帯 (最初の端より下なら None)。
    #[inline]
    fn slab(&self, v: u32) -> Option<usize> {
        self.starts.partition_point(|&b| b <= v).checked_sub(1)
    }

    /// `v` を範囲に含む member が居るか。
    #[inline]
    fn covered(&self, v: u32) -> bool {
        self.slab(v).is_some_and(|i| self.cover[i] > 0)
    }

    /// 同じ帯か (帯の外どうしも同じとみなす)。
    #[inline]
    fn same(&self, a: u32, b: u32) -> bool {
        self.slab(a) == self.slab(b)
    }

    /// 端 `b` を 1 つ足す。 帯を新しく割ったら、 割られた帯の値の範囲 (両端含む) を返す。
    fn add_end(&mut self, b: u32) -> Option<(u32, u32)> {
        match self.starts.binary_search(&b) {
            Ok(i) => {
                self.refs[i] += 1;
                None
            }
            Err(i) => {
                // 新しい帯 [b, 次の端) は割られた帯と同じ member に含まれる
                let cover = if i > 0 { self.cover[i - 1] } else { 0 };
                self.starts.insert(i, b);
                self.refs.insert(i, 1);
                self.cover.insert(i, cover);
                let hi = self.starts.get(i + 1).map_or(u32::MAX - 1, |&n| n - 1);
                (i > 0).then(|| (self.starts[i - 1], hi))
            }
        }
    }

    /// 端 `b` を 1 つ外す (0 になったら帯を前の帯に併せる)。
    fn remove_end(&mut self, b: u32) {
        let Ok(i) = self.starts.binary_search(&b) else { return };
        self.refs[i] -= 1;
        if self.refs[i] == 0 {
            // この端を持つ member が居ない = 前後の帯は同じ member に含まれる
            debug_assert!(i == 0 || self.cover[i - 1] == self.cover[i]);
            self.starts.remove(i);
            self.refs.remove(i);
            self.cover.remove(i);
        }
    }

    /// `[lo, hi]` の member を 1 つ足す / 外す。
    fn add(&mut self, lo: u32, hi: u32) -> Vec<(u32, u32)> {
        if lo > hi {
            return Vec::new();
        }
        let mut split: Vec<(u32, u32)> = self.add_end(lo).into_iter().collect();
        if hi < u32::MAX - 1 {
            split.extend(self.add_end(hi + 1));
        }
        self.recover(lo, hi, true);
        split
    }

    fn remove(&mut self, lo: u32, hi: u32) {
        if lo > hi {
            return;
        }
        self.recover(lo, hi, false);
        self.remove_end(lo);
        if hi < u32::MAX - 1 {
            self.remove_end(hi + 1);
        }
    }

    fn recover(&mut self, lo: u32, hi: u32, add: bool) {
        let a = self.starts.partition_point(|&b| b < lo);
        let z = self.starts.partition_point(|&b| b <= hi);
        for c in &mut self.cover[a..z] {
            if add {
                *c += 1;
            } else {
                *c -= 1;
            }
        }
    }
}

/// 根の鍵 1 つを共有する member の範囲の索引 (範囲の穴のある family 用)。 member が変わったら
/// 作り直す (`RootKey::ivs`)。
struct Ivs {
    /// (lo, hi, member の slot)、 lo 昇順。 空の範囲 (lo > hi) は載せない。
    items: Vec<(u32, u32, usize)>,
    /// items の添字を hi 昇順に。
    by_hi: Vec<usize>,
    /// items の上の segment tree (葉 = items、 節 = 部分木の hi の最大)。
    max_hi: Vec<u32>,
    leaves: usize,
}

impl Ivs {
    fn new(mut items: Vec<(u32, u32, usize)>) -> Self {
        items.retain(|x| x.0 <= x.1);
        items.sort_unstable();
        let mut by_hi: Vec<usize> = (0..items.len()).collect();
        by_hi.sort_unstable_by_key(|&i| items[i].1);
        let leaves = items.len().next_power_of_two();
        let mut max_hi = vec![0u32; 2 * leaves];
        for (i, x) in items.iter().enumerate() {
            max_hi[leaves + i] = x.1;
        }
        for n in (1..leaves).rev() {
            max_hi[n] = max_hi[2 * n].max(max_hi[2 * n + 1]);
        }
        Ivs { items, by_hi, max_hi, leaves }
    }

    /// `v` を含む member の slot を `f` に渡す (出力の数 × log)。
    fn stab(&self, v: u32, f: &mut impl FnMut(usize)) {
        let k = self.items.partition_point(|x| x.0 <= v);
        if k > 0 {
            self.stab_in(1, 0, self.leaves, k, v, f);
        }
    }

    fn stab_in(&self, n: usize, l: usize, r: usize, k: usize, v: u32, f: &mut impl FnMut(usize)) {
        if l >= k || self.max_hi[n] < v {
            return;
        }
        if r - l == 1 {
            f(self.items[l].2);
            return;
        }
        let m = (l + r) / 2;
        self.stab_in(2 * n, l, m, k, v, f);
        self.stab_in(2 * n + 1, m, r, k, v, f);
    }

    /// `v` を含む区間の数 (O(log))。
    fn count_at(&self, v: u32) -> usize {
        self.items.partition_point(|x| x.0 <= v) - self.by_hi.partition_point(|&i| self.items[i].1 < v)
    }

    /// 値が `a` から `b` に動いた時に出入りする member (`f(slot, 入ったか)`)。 端が `a` と `b` の
    /// 間にある区間だけを見る (間の端の数) か、 `a` を含む区間と `b` を含む区間を突き合わせる
    /// (両方の数) かの安い方 — 入れ子の閾値 (`age > k`) で近くに動くなら前者、 狭い範囲が多数並ぶ所を
    /// 遠くに飛ぶなら後者が小さい。
    fn cross(&self, a: u32, b: u32, f: &mut impl FnMut(usize, bool)) {
        let (x, y) = if a < b { (a, b) } else { (b, a) };
        let between = self.items.partition_point(|it| it.0 <= y) - self.items.partition_point(|it| it.0 <= x)
            + self.by_hi.partition_point(|&i| self.items[i].1 < y)
            - self.by_hi.partition_point(|&i| self.items[i].1 < x);
        if between > self.count_at(a) + self.count_at(b) {
            let (mut sa, mut sb) = (Vec::new(), Vec::new());
            self.stab(a, &mut |i| sa.push(i));
            self.stab(b, &mut |i| sb.push(i));
            sa.sort_unstable();
            sb.sort_unstable();
            let (mut i, mut j) = (0, 0);
            while i < sa.len() || j < sb.len() {
                match (sa.get(i), sb.get(j)) {
                    (Some(p), Some(q)) if p == q => {
                        i += 1;
                        j += 1;
                    }
                    (Some(&p), q) if q.is_none_or(|&q| p < q) => {
                        f(p, false);
                        i += 1;
                    }
                    (_, Some(&q)) => {
                        f(q, true);
                        j += 1;
                    }
                    _ => unreachable!(),
                }
            }
            return;
        }
        let has = |i: usize, v: u32| self.items[i].0 <= v && v <= self.items[i].1;
        let mut visit = |i: usize| {
            let (ia, ib) = (has(i, a), has(i, b));
            if ia != ib {
                f(self.items[i].2, ib);
            }
        };
        // lo が (x, y] = 片方だけを含みうる
        let s = self.items.partition_point(|it| it.0 <= x);
        let e = self.items.partition_point(|it| it.0 <= y);
        for i in s..e {
            visit(i);
        }
        // hi が [x, y) (lo が (x, y] のものは上で見た)
        let s = self.by_hi.partition_point(|&i| self.items[i].1 < x);
        let e = self.by_hi.partition_point(|&i| self.items[i].1 < y);
        for &i in &self.by_hi[s..e] {
            let lo = self.items[i].0;
            if !(x < lo && lo <= y) {
                visit(i);
            }
        }
    }
}

// ─────────────────────────── 条件の形 ───────────────────────────

/// 値を固定した単一紐条件 (家族の形の一部)。
enum Pred {
    Range(u16, u32, u32),
    /// sort + dedup 済み。
    In(u16, Vec<u32>),
    Present(u16),
}

impl Pred {
    fn himo(&self) -> u16 {
        match self {
            Pred::Range(h, ..) | Pred::In(h, _) | Pred::Present(h) => *h,
        }
    }

    #[inline]
    fn matches(&self, r: &impl CellReader, eid: u32) -> bool {
        match self {
            Pred::Range(h, lo, hi) => matches!(r.cell(*h, eid), Some(v) if *lo <= v && v <= *hi),
            Pred::In(h, vs) => matches!(r.cell(*h, eid), Some(v) if vs.binary_search(&v).is_ok()),
            Pred::Present(h) => r.cell(*h, eid).is_some(),
        }
    }
}

/// 穴に入る値。 `Id` / `Text` は `Eq` / `EqText` (`Text` は vocab に現れた時点で id に解決する)、
/// `Range` は `Range` の両端 (family に 1 個まで、 `canonical`)。
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum HoleVal {
    Id(u32),
    Text(String),
    Range(u32, u32),
    /// 集計の group の列 ([`LiveCounts`])。 値は持たない (全ての値が group)。
    Group,
}

enum Leaf {
    Hole(u16, HoleVal),
    Fixed(Pred),
}

/// 根からの ref の道 + 単一紐条件 1 個。
struct Flat {
    path: Vec<u16>,
    /// 形の符号 (穴の値を含まない)。
    sig: Vec<u32>,
    leaf: Leaf,
}

impl Flat {
    fn hole(&self) -> Option<&HoleVal> {
        match &self.leaf {
            Leaf::Hole(_, v) => Some(v),
            Leaf::Fixed(_) => None,
        }
    }
}

fn flatten(p: LivePred, path: &mut Vec<u16>, out: &mut Vec<Flat>) {
    let (tail, leaf) = match p {
        LivePred::Via { path: seg, pred } => {
            let n = path.len();
            path.extend_from_slice(&seg);
            flatten(*pred, path, out);
            path.truncate(n);
            return;
        }
        LivePred::Eq { himo_id, value } => (vec![0, himo_id as u32], Leaf::Hole(himo_id, HoleVal::Id(value))),
        LivePred::EqText { himo_id, text } => (vec![0, himo_id as u32], Leaf::Hole(himo_id, HoleVal::Text(text))),
        // 範囲の穴。 2 本目以降は canonical が値を固定した条件に戻す
        LivePred::Range { himo_id, lo, hi } => (vec![1, himo_id as u32], Leaf::Hole(himo_id, HoleVal::Range(lo, hi))),
        LivePred::In { himo_id, mut values } => {
            values.sort_unstable();
            values.dedup();
            let mut t = vec![2, himo_id as u32, values.len() as u32];
            t.extend_from_slice(&values);
            (t, Leaf::Fixed(Pred::In(himo_id, values)))
        }
        LivePred::Present { himo_id } => (vec![3, himo_id as u32], Leaf::Fixed(Pred::Present(himo_id))),
        LivePred::Or(_) => unreachable!("Or は dnf で枝に展開してから flatten する"),
    };
    let mut sig = Vec::with_capacity(1 + path.len() + tail.len());
    sig.push(path.len() as u32);
    sig.extend(path.iter().map(|&h| h as u32));
    sig.extend(tail);
    out.push(Flat { path: path.clone(), sig, leaf });
}

/// 条件を正規形 (道と条件の辞書順) に並べ、 形の符号と穴の値の並び (= member の鍵) を返す。
/// 同じ条件の集合なら書き順によらず同じ形・同じ鍵になる。
///
/// `Range` を穴にするのは正規形で最初の 1 本だけ (範囲の穴が 2 本あると、 member の範囲が
/// 多次元の箱になり、 根での振り分けが 1 次元の区間の索引で済まなくなる)。 残りは値を固定した
/// 条件 = 範囲が違えば別の family。
///
/// `group` (集計の group の列: ref の道 + 紐) があれば、 その列を値を運ぶ穴にする (範囲の穴は
/// 作らない — 根まで運ぶ値は 1 つ)。
fn canonical(preds: Vec<LivePred>, group: Option<(Vec<u16>, u16)>) -> (Vec<u32>, Vec<Flat>, Vec<HoleVal>) {
    let mut flats = Vec::new();
    for p in preds {
        flatten(p, &mut Vec::new(), &mut flats);
    }
    let grouped = group.is_some();
    if let Some((path, h)) = group {
        let mut sig = vec![path.len() as u32];
        sig.extend(path.iter().map(|&x| x as u32));
        sig.extend([6, h as u32]);
        flats.push(Flat { path, sig, leaf: Leaf::Hole(h, HoleVal::Group) });
    }
    let order = |a: &Flat, b: &Flat| a.sig.cmp(&b.sig).then_with(|| a.hole().cmp(&b.hole()));
    flats.sort_by(order);
    let mut first = !grouped;
    for f in &mut flats {
        if let Leaf::Hole(h, HoleVal::Range(lo, hi)) = f.leaf {
            if !first {
                f.sig.truncate(1 + f.path.len());
                f.sig.extend([5, h as u32, lo, hi]);
                f.leaf = Leaf::Fixed(Pred::Range(h, lo, hi));
            }
            first = false;
        }
    }
    flats.sort_by(order);
    let mut sig = Vec::new();
    for f in &flats {
        sig.push(f.sig.len() as u32);
        sig.extend_from_slice(&f.sig);
    }
    let key = flats.iter().filter_map(|f| f.hole().cloned()).collect();
    (sig, flats, key)
}

/// 購読の木の節。 節 0 が根 (`x0`)。
struct Node {
    /// 親の節 (根は `usize::MAX`)。
    parent: usize,
    /// 親の entity からこの節の entity へ張られた ref 紐 (根は未使用)。
    via: u16,
    depth: u32,
    /// この節の entity 自身への値を固定した条件。
    local: Vec<Pred>,
    /// この節の entity の値の穴 (`Eq` / `EqText`): (紐, member の鍵の添字)。
    holes: Vec<(u16, usize)>,
    /// この節の entity の範囲の穴: (紐, member の鍵の添字)。 family に高々 1 個。 節の鍵には
    /// 入れず、 値そのものを記録に持って根まで運ぶ (`Family::range`)。
    range: Option<(u16, usize)>,
    children: Vec<usize>,
    /// この節から下に穴があるか (無ければ鍵は常に id 0)。
    has_holes: bool,
    /// 節の鍵の長さ = 穴の数 + 穴を持つ子の数。
    key_len: usize,
}

fn build_tree(flats: Vec<Flat>) -> Vec<Node> {
    let node = |parent, via, depth| Node {
        parent,
        via,
        depth,
        local: Vec::new(),
        holes: Vec::new(),
        range: None,
        children: Vec::new(),
        has_holes: false,
        key_len: 0,
    };
    let mut nodes = vec![node(usize::MAX, 0, 0)];
    let mut slot = 0;
    for f in flats {
        let mut cur = 0;
        for &seg in &f.path {
            cur = match nodes[cur].children.iter().copied().find(|&c| nodes[c].via == seg) {
                Some(c) => c,
                None => {
                    let depth = nodes[cur].depth + 1;
                    nodes.push(node(cur, seg, depth));
                    let id = nodes.len() - 1;
                    nodes[cur].children.push(id);
                    id
                }
            };
        }
        match f.leaf {
            Leaf::Hole(h, HoleVal::Range(..) | HoleVal::Group) => {
                nodes[cur].range = Some((h, slot));
                slot += 1;
            }
            Leaf::Hole(h, _) => {
                nodes[cur].holes.push((h, slot));
                slot += 1;
            }
            Leaf::Fixed(p) => nodes[cur].local.push(p),
        }
    }
    // 子は親より後に作られる = 添字の逆順が深い順
    for n in (0..nodes.len()).rev() {
        let keyed = nodes[n].children.iter().filter(|&&c| nodes[c].has_holes).count();
        nodes[n].has_holes = !nodes[n].holes.is_empty() || keyed > 0;
        nodes[n].key_len = nodes[n].holes.len() + keyed;
    }
    nodes
}

// ─────────────────────────── family ───────────────────────────

/// 書き込み側の印の置き場の数。 書き手の thread ごとに 1 つを使う (書き手が多い時に 1 個の
/// lock を取り合わない)。
const SHARDS: usize = 16;

/// この thread が印を置く shard。
#[inline]
fn shard_index() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static IDX: usize = NEXT.fetch_add(1, Ordering::Relaxed) % SHARDS;
    }
    IDX.with(|i| *i)
}

/// shard 1 個 (隣の shard と cache line を共有しない)。
#[repr(align(128))]
struct Shard(Mutex<Pending>);

/// 書き込み側が付ける印。
struct Pending {
    /// 節ごと: 条件の紐が書かれた entity。
    nodes: Vec<Marks>,
    /// 解放された entity (slot 再利用で別 entity になりうる)。
    freed: Marks,
}

/// 節の答え: (鍵 id, 範囲の穴の値)。 値は根と範囲の穴の節を結ぶ道の上の節だけが持つ (他は 0)。
type Ans = (u32, u32);

/// 購読 1 本ぶんの報告状態。
struct Member {
    /// engine 内で一意な購読 id ([`LiveQuery::id`])。
    id: u64,
    key: Vec<HoleVal>,
    /// 有効化 (鍵を表に載せて初期候補を数えた) 済みなら根の鍵 id。
    root_key: Option<u32>,
    /// 範囲の穴の範囲 (family に範囲の穴がある時)。
    range: Option<(u32, u32)>,
    /// 有効化した時に解決した穴の値 (表から外す時に射影を計算し直す)。
    vals: Vec<u32>,
    /// 最後の poll で呼び手に渡した集合 (= 呼び手が積分済みの集合)。
    reported: Bits,
    /// 最後の poll 以降に一度でも集合を出た eid。 報告済みかつ今も集合に居ても、 これが
    /// 立っていれば 「出て入り直した」 として removed + added の両方を出す。
    left: Marks,
    /// 最後の poll 以降に集合への出入りがありえた eid。
    changed: Marks,
    /// family の `ready` に載っているか。
    queued: bool,
    /// `Or` の購読の枝なら、 その購読と枝の番号 (差分は呼び手でなくこちらに積む)。
    union: Option<(std::sync::Weak<Union>, usize)>,
    /// 集計の購読 ([`LiveCounts`]) なら group の報告状態 (entity の出入りは積まない)。
    grp: Option<GroupState>,
}

/// 集計の購読 1 本ぶんの報告状態。 件数そのものは根の鍵が持ち (同じ鍵の購読で共有)、 ここは
/// 「どの group の件数が動いたか」 と 「最後に渡した件数」 だけ。
#[derive(Default)]
struct GroupState {
    changed: Marks,
    reported: std::collections::BTreeMap<u32, u64>,
}

impl GroupState {
    /// 動いた group の今の件数 (最後に渡した件数と違うものだけ、 値の昇順。 0 = group が消えた)。
    fn drain(&mut self, groups: &std::collections::BTreeMap<u32, u64>) -> Vec<(u32, u64)> {
        let mut out = Vec::new();
        for v in self.changed.take() {
            let now = groups.get(&v).copied().unwrap_or(0);
            let was = self.reported.get(&v).copied().unwrap_or(0);
            if now != was {
                out.push((v, now));
                if now == 0 {
                    self.reported.remove(&v);
                } else {
                    self.reported.insert(v, now);
                }
            }
        }
        out
    }
}

impl Member {
    /// 根の答えが `rec` の entity がこの member の集合に居るか。
    #[inline]
    fn has(&self, rec: Option<Ans>) -> bool {
        Member::probe(self.root_key, self.range)(rec)
    }

    /// `eid` の出入りを記録し、 changed が空でなくなったら family の `ready` に載せる。
    #[inline]
    fn note(&mut self, slot: usize, ready: &mut Vec<usize>, eid: u32, left: bool) {
        if left {
            self.left.add(eid);
        }
        self.changed.add(eid);
        if !self.queued {
            self.queued = true;
            ready.push(slot);
        }
    }

    /// changed を消費して差分を返し、 報告済みの集合を進める。 `root` = 根の答え。
    fn drain(&mut self, root: impl Fn(u32) -> Option<Ans>, peer: u32) -> LiveDelta {
        self.queued = false;
        let (key, range) = (self.root_key, self.range);
        let probe = Member::probe(key, range);
        drain_marks(&mut self.changed, &mut self.left, &mut self.reported, |e| probe(root(e)), peer)
    }

    /// `has` の、 member を借用しない版。
    fn probe(key: Option<u32>, range: Option<(u32, u32)>) -> impl Fn(Option<Ans>) -> bool {
        move |rec| match (key, rec) {
            (Some(k), Some((id, v))) => k == id && range.is_none_or(|(lo, hi)| lo <= v && v <= hi),
            _ => false,
        }
    }
}

/// changed を消費して差分を返し、 報告済みの集合を進める (member と `Or` の購読で共通)。
/// `now(e)` = 今集合に居るか。 `left` に居る eid は、 報告済みで今も居ても 「出て入り直した」。
fn drain_marks(changed: &mut Marks, left: &mut Marks, reported: &mut Bits, now: impl Fn(u32) -> bool, peer: u32) -> LiveDelta {
    let mut delta = LiveDelta::default();
    if changed.is_empty() {
        return delta;
    }
    // changed も left も昇順なので、 left は突き合わせで引く
    let left_set = left.take();
    let mut li = 0;
    for eid in changed.take() {
        let now = now(eid);
        let was = reported.get(eid);
        while li < left_set.len() && left_set[li] < eid {
            li += 1;
        }
        let left = left_set.get(li) == Some(&eid);
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
        reported.put(eid, now);
    }
    delta
}

/// 節 `n` の記録 (`None` = 不明)。 範囲の値は `vals` に `v + 1` で持つ (0 = なし)。
#[inline]
fn rec_at(recs: &[KeyStore], vals: &[Words], n: usize, e: u32) -> Option<Option<Ans>> {
    match recs[n].get(e) {
        Rec::Unknown => None,
        Rec::Known(k) => Some(k.map(|id| (id, vals[n].get(e).saturating_sub(1)))),
    }
}

/// 根の答え。
#[inline]
fn root_at(recs: &[KeyStore], vals: &[Words], e: u32) -> Option<Ans> {
    recs[0].key(e).map(|id| (id, vals[0].get(e).saturating_sub(1)))
}

/// poll 側だけが触る状態 (評価 lock の下)。
struct Settled {
    /// 節ごとの記録 (添字 0 = 根の現在の鍵)。
    recs: Vec<KeyStore>,
    /// 節ごとの範囲の穴の値 (範囲の穴から根への道の上の節だけ使う)。
    vals: Vec<Words>,
    /// 範囲の穴の値の帯。
    slabs: Slabs,
    /// 節ごとの鍵の表。
    tables: Vec<KeyTable>,
    /// 今表に載っている根の鍵 (id 昇順)。 id は使い回さないので、 id 添字の配列にすると購読の
    /// 張り替えのたびに伸び続ける — 生きている鍵だけを並べて二分探索で引く。 根の記録に外れた鍵の
    /// id が残っていても、 ここに無ければ無視する。
    keys: Vec<RootKey>,
    members: Vec<Option<Member>>,
    /// 未有効化の member (登録直後 / text が vocab にまだ無い)。
    dormant: Vec<usize>,
    /// changed が空でない (かもしれない) member。 `LiveRegistry::poll_all` はここだけを見る。
    ready: Vec<usize>,
    /// 集計の部分和 (`Family::partial`): 1 段目の先の entity → その entity を指して数えられている根。
    partial: std::collections::BTreeMap<u32, Partial>,
}

/// 集計で 1 段目の先の entity `t` (会社) を指している根の部分和。
struct Partial {
    /// `t` の根を今数えている group の値。
    g: u32,
    /// 根の鍵 id → その鍵で `t` を指して数えている根の数。
    n: Vec<(u32, u64)>,
}

/// 根の答えの当て方。
#[derive(Clone, Copy, PartialEq, Eq)]
enum RootMode {
    /// 根の鍵が一致すれば member の集合に居る。
    Plain,
    /// 範囲の穴: 鍵が一致し、 値が member の範囲に入れば居る。
    Ranged,
    /// 集計: 鍵ごと・値 (group) ごとの件数を数える。
    Grouped,
}

/// 鍵 id → `Settled::keys` の添字の直近 2 件 (`apply_root_cached`)。 `keys` が変わる
/// (install / uninstall) をまたいで使わないこと。
#[derive(Default)]
struct KeyCache {
    slots: [Option<(u32, Option<usize>)>; 2],
    /// 次に上書きする slot。
    next: usize,
}

impl KeyCache {
    #[inline]
    fn find(&mut self, keys: &[RootKey], id: u32) -> Option<usize> {
        if let Some((_, i)) = self.slots.iter().flatten().find(|(k, _)| *k == id) {
            return *i;
        }
        let i = keys.binary_search_by_key(&id, |k| k.id).ok();
        self.slots[self.next] = Some((id, i));
        self.next ^= 1;
        i
    }
}

/// 根の鍵 1 つ: その鍵の member と、 現在その鍵の根の数。
struct RootKey {
    id: u32,
    members: Vec<usize>,
    count: usize,
    /// 範囲の穴のある family: member の範囲の索引 (member が変わったら None に戻して作り直す)。
    ivs: Option<Ivs>,
    /// 集計の family: group の値 → その値の根の数 (0 の group は載せない)。
    groups: std::collections::BTreeMap<u32, u64>,
}

impl RootKey {
    fn new(id: u32) -> Self {
        RootKey { id, members: Vec::new(), count: 0, ivs: None, groups: std::collections::BTreeMap::new() }
    }

    /// 範囲の索引を (無ければ作って) 返す。
    fn ivs(&mut self, members: &[Option<Member>]) -> &Ivs {
        self.ivs.get_or_insert_with(|| {
            Ivs::new(
                self.members
                    .iter()
                    .filter_map(|&slot| members[slot].as_ref()?.range.map(|(lo, hi)| (lo, hi, slot)))
                    .collect(),
            )
        })
    }
}

impl Settled {
    #[inline]
    fn key(&mut self, id: u32) -> Option<&mut RootKey> {
        let i = self.keys.binary_search_by_key(&id, |k| k.id).ok()?;
        Some(&mut self.keys[i])
    }

    /// `widths` = 節ごとの鍵の長さ。
    fn new(widths: &[usize]) -> Self {
        Settled {
            recs: widths.iter().map(|_| KeyStore::default()).collect(),
            vals: widths.iter().map(|_| Words::default()).collect(),
            slabs: Slabs::default(),
            tables: widths.iter().map(|&w| KeyTable::new(w)).collect(),
            keys: Vec::new(),
            members: Vec::new(),
            dormant: Vec::new(),
            ready: Vec::new(),
            partial: std::collections::BTreeMap::new(),
        }
    }

    /// 根の鍵 `k` の group `g` の件数を `d` 動かし、 鍵の member に 「g が動いた」 を積む。
    fn bump_group(keys: &mut [RootKey], members: &mut [Option<Member>], k: u32, g: u32, d: i64) {
        let Ok(i) = keys.binary_search_by_key(&k, |x| x.id) else { return };
        let rk = &mut keys[i];
        let c = rk.groups.entry(g).or_insert(0);
        *c = c.wrapping_add_signed(d);
        if *c == 0 {
            rk.groups.remove(&g);
        }
        for &slot in &rk.members {
            if let Some(gs) = members[slot].as_mut().and_then(|m| m.grp.as_mut()) {
                gs.changed.add(g);
            }
        }
    }

    /// 1 段目の先 `t` の group の値が `g` になった: `t` を指して数えている根を全部まとめて移す
    /// (根を 1 件ずつ評価しない)。
    fn move_partial(&mut self, t: u32, g: u32) {
        let Settled { partial, keys, members, .. } = self;
        let Some(p) = partial.get_mut(&t) else { return };
        if p.g == g {
            return;
        }
        for &(k, n) in &p.n {
            Settled::bump_group(keys, members, k, p.g, -(n as i64));
            Settled::bump_group(keys, members, k, g, n as i64);
        }
        p.g = g;
    }

    /// 集計の部分和の family で、 根 `eid` の答えを `now` (鍵, 1 段目の先 `t`) にする。 `g` = 今の
    /// `t` の group の値 (`t` の部分和がまだ無い時だけ使う)。
    ///
    /// `t` の根は常に部分和の `g` で数える (評価で読んだ値とずれていても — ずれるのは `t` より
    /// 下が書き換わった時で、 その印で `t` を評価し直した時に `move_partial` が全部移す)。
    fn apply_root_partial(&mut self, eid: u32, now: Option<Ans>, g: u32) {
        let was = self.root(eid);
        if was == now {
            return;
        }
        self.recs[0].set_root(eid, now.map(|a| a.0));
        self.vals[0].put(eid, now.map_or(0, |a| a.1 + 1));
        let Settled { partial, keys, members, .. } = self;
        if let Some((k, t)) = was {
            if let Ok(i) = keys.binary_search_by_key(&k, |x| x.id) {
                keys[i].count -= 1;
            }
            if let Some(p) = partial.get_mut(&t) {
                let pg = p.g;
                if let Some(j) = p.n.iter().position(|x| x.0 == k) {
                    p.n[j].1 -= 1;
                    if p.n[j].1 == 0 {
                        p.n.swap_remove(j);
                    }
                }
                if p.n.is_empty() {
                    partial.remove(&t);
                }
                Settled::bump_group(keys, members, k, pg, -1);
            }
        }
        if let Some((k, t)) = now {
            if let Ok(i) = keys.binary_search_by_key(&k, |x| x.id) {
                keys[i].count += 1;
            }
            let p = partial.entry(t).or_insert_with(|| Partial { g, n: Vec::new() });
            match p.n.iter_mut().find(|x| x.0 == k) {
                Some(x) => x.1 += 1,
                None => p.n.push((k, 1)),
            }
            let pg = p.g;
            Settled::bump_group(keys, members, k, pg, 1);
        }
    }

    #[inline]
    fn root(&self, e: u32) -> Option<Ans> {
        root_at(&self.recs, &self.vals, e)
    }

    /// 根 `eid` の答えを `now` にし、 出入りした member に印を付ける (集計の family では group の
    /// 件数を動かす)。
    #[inline]
    fn apply_root_cached(&mut self, eid: u32, now: Option<Ans>, mode: RootMode, cache: &mut KeyCache) {
        let was = self.root(eid);
        if was == now {
            return;
        }
        self.recs[0].set_root(eid, now.map(|a| a.0));
        if mode != RootMode::Plain {
            self.vals[0].put(eid, now.map_or(0, |a| a.1 + 1));
        }
        let ranged = mode == RootMode::Ranged;
        let Settled { keys, members, ready, .. } = self;
        let iw = was.and_then(|a| cache.find(keys, a.0));
        let inw = now.and_then(|a| cache.find(keys, a.0));
        if mode == RootMode::Grouped {
            for (k, ans, enter) in [(iw, was, false), (inw, now, true)] {
                let (Some(i), Some((_, v))) = (k, ans) else { continue };
                let rk = &mut keys[i];
                let c = rk.groups.entry(v).or_insert(0);
                if enter {
                    rk.count += 1;
                    *c += 1;
                } else {
                    rk.count -= 1;
                    *c -= 1;
                    if *c == 0 {
                        rk.groups.remove(&v);
                    }
                }
                for &slot in &rk.members {
                    if let Some(g) = members[slot].as_mut().and_then(|m| m.grp.as_mut()) {
                        g.changed.add(v);
                    }
                }
            }
            return;
        }
        if let (Some(i), Some(j), Some((_, a)), Some((_, b))) = (iw, inw, was, now)
            && i == j
        {
            // 同じ鍵の中で範囲の値だけが動いた: 端をまたいだ member だけが出入りする
            let rk = &mut keys[i];
            rk.ivs(members);
            if let Some(ivs) = &rk.ivs {
                ivs.cross(a, b, &mut |slot, enter| {
                    if let Some(m) = members[slot].as_mut() {
                        m.note(slot, ready, eid, !enter);
                    }
                });
            }
            return;
        }
        for (k, ans, enter) in [(iw, was, false), (inw, now, true)] {
            let (Some(i), Some((_, v))) = (k, ans) else { continue };
            let rk = &mut keys[i];
            if enter {
                rk.count += 1;
            } else {
                rk.count -= 1;
            }
            if ranged {
                rk.ivs(members);
            }
            let mut note = |slot: usize| {
                if let Some(m) = members[slot].as_mut() {
                    m.note(slot, ready, eid, !enter);
                }
            };
            if let Some(ivs) = rk.ivs.as_ref().filter(|_| ranged) {
                ivs.stab(v, &mut note);
            } else {
                for &slot in &rk.members {
                    note(slot);
                }
            }
        }
    }
}

/// 形 (値の穴以外が同じ条件) を共有する購読の束。 書き込みの印付け・評価・記録は family で
/// 1 回だけやり、 結果は根の鍵 (穴の値の並び) ごとに member へ振り分ける。
pub(crate) struct Family {
    id: u64,
    sig: Vec<u32>,
    nodes: Vec<Node>,
    /// 深い節から順に並べた節の添字 (根が最後)。
    order: Vec<usize>,
    /// route を張る (himo, 節) の組。
    routes: Vec<(u16, usize)>,
    /// 書き込み側が付ける印 (shard ごと)。 評価 lock とは別 — poll の最中も書き込みは待たない。
    pending: Box<[Shard]>,
    /// 最後に印を取り出してから印が付いた shard (bit i = shard i)。 書き手は印を置いて lock を
    /// 離した **後** に自分の bit を立て、 poll は全 bit を落としてから立っていた shard だけを
    /// 取り出す (逆順だと、 取り出した後に置かれた印が 「印なし」 のまま残る — gate は
    /// `tests/loom_live_dirty.rs`)。 印の無い shard の lock は取らない。
    dirty: AtomicU32,
    settled: Mutex<Settled>,
    /// ablation 用: 真偽 (鍵) が変わらなくても常に展開する。
    expand_always: AtomicBool,
    /// 値を根まで運ぶ穴 (範囲の穴か集計の group の列): (節, 紐)。
    range: Option<(usize, u16)>,
    /// 運ぶ値が集計の group の列 (帯で刈り込まず、 値が変わるたびに運ぶ)。
    group: bool,
    /// 集計で group の列が根でない時: 根の子のうち道の上の節 (1 段目)。 根は group の値でなく
    /// 1 段目の先の entity を記録し、 件数は 1 段目の先ごとの部分和で持つ — 1 段目の先の group の
    /// 値が変わっても根を評価せず部分和を移すだけ (`Settled::move_partial`)。
    partial: Option<usize>,
    /// 節ごと: 範囲の穴の節から根への道の上か (その節の答えは範囲の値を運ぶ)。
    on_path: Vec<bool>,
}

/// 穴の値を解決する (範囲の穴の位置は 0、 範囲は `range_of`)。 text が vocab に無ければ None。
fn resolve(r: &impl CellReader, key: &[HoleVal]) -> Option<Vec<u32>> {
    key.iter()
        .map(|v| match v {
            HoleVal::Id(x) => Some(*x),
            HoleVal::Text(t) => r.vocab_lookup(t),
            HoleVal::Range(..) | HoleVal::Group => Some(0),
        })
        .collect()
}

fn range_of(key: &[HoleVal]) -> Option<(u32, u32)> {
    key.iter().find_map(|v| match v {
        HoleVal::Range(lo, hi) => Some((*lo, *hi)),
        _ => None,
    })
}

impl Family {
    fn new(id: u64, sig: Vec<u32>, flats: Vec<Flat>) -> Self {
        let group = flats.iter().any(|f| matches!(f.leaf, Leaf::Hole(_, HoleVal::Group)));
        let nodes = build_tree(flats);
        let mut order: Vec<usize> = (0..nodes.len()).collect();
        order.sort_by_key(|&n| std::cmp::Reverse(nodes[n].depth));
        let mut routes = Vec::new();
        for (i, n) in nodes.iter().enumerate() {
            for p in &n.local {
                routes.push((p.himo(), i));
            }
            for &(h, _) in &n.holes {
                routes.push((h, i));
            }
            // 子への ref 紐はこの節の entity に張られている
            for &c in &n.children {
                routes.push((nodes[c].via, i));
            }
        }
        let range = nodes.iter().enumerate().find_map(|(i, n)| n.range.map(|(h, _)| (i, h)));
        let mut on_path = vec![false; nodes.len()];
        if let Some((mut n, h)) = range {
            routes.push((h, n));
            loop {
                on_path[n] = true;
                if n == 0 {
                    break;
                }
                n = nodes[n].parent;
            }
        }
        routes.sort_unstable();
        routes.dedup();
        let nodes_partial = nodes[0].children.iter().copied().find(|&c| on_path[c]);
        let n = nodes.len();
        let widths: Vec<usize> = nodes.iter().map(|x| x.key_len).collect();
        Self {
            id,
            sig,
            nodes,
            order,
            routes,
            pending: (0..SHARDS)
                .map(|_| Shard(Mutex::new(Pending { nodes: (0..n).map(|_| Marks::default()).collect(), freed: Marks::default() })))
                .collect(),
            dirty: AtomicU32::new(0),
            settled: Mutex::new(Settled::new(&widths)),
            expand_always: AtomicBool::new(false),
            partial: if group { nodes_partial } else { None },
            range,
            group,
            on_path,
        }
    }

    #[inline]
    fn mark(&self, node: usize, eid: u32) {
        let i = shard_index();
        self.pending[i].0.lock().nodes[node].add(eid);
        self.set_dirty(i);
    }

    #[inline]
    fn mark_freed(&self, eid: u32) {
        let i = shard_index();
        self.pending[i].0.lock().freed.add(eid);
        self.set_dirty(i);
    }

    /// shard `i` に印を置いて lock を離した後に呼ぶ。 立っていれば書かない (poll の間の書き込みは
    /// 読むだけで済み、 cache line を取り合わない)。 立てるのが `store` でなく RMW なのは loom
    /// model と同じ形にするため (`tests/loom_live_dirty.rs` の doc)。
    #[inline]
    fn set_dirty(&self, i: usize) {
        let bit = 1u32 << i;
        if self.dirty.load(Ordering::Acquire) & bit == 0 {
            self.dirty.fetch_or(bit, Ordering::AcqRel);
        }
    }

    /// member `slot` に未 poll の変化がありうるか (評価しない)。
    fn member_dirty(&self, slot: usize) -> bool {
        if self.dirty.load(Ordering::Acquire) != 0 {
            return true;
        }
        let s = self.settled.lock();
        s.members[slot].as_ref().is_some_and(|m| !m.changed.is_empty() || s.dormant.contains(&slot))
    }

    fn add_member(&self, id: u64, key: Vec<HoleVal>, union: Option<(std::sync::Weak<Union>, usize)>) -> usize {
        let mut s = self.settled.lock();
        let grp = key.contains(&HoleVal::Group).then(GroupState::default);
        let m = Member {
            id,
            key,
            root_key: None,
            range: None,
            vals: Vec::new(),
            reported: Bits::default(),
            left: Marks::default(),
            changed: Marks::default(),
            queued: false,
            grp,
            union,
        };
        let slot = match s.members.iter().position(Option::is_none) {
            Some(i) => {
                s.members[i] = Some(m);
                i
            }
            None => {
                s.members.push(Some(m));
                s.members.len() - 1
            }
        };
        s.dormant.push(slot);
        slot
    }

    /// 残りの member 数を返す。
    fn remove_member(&self, slot: usize) -> usize {
        let mut s = self.settled.lock();
        if let Some(m) = s.members[slot].take()
            && let Some(k) = m.root_key
        {
            if let Some(rk) = s.key(k) {
                rk.members.retain(|&x| x != slot);
                rk.ivs = None;
            }
            self.uninstall(&mut s, &m.vals);
            // 端が減って帯が併さるだけ = 記録の値は同じ帯に居続けるので直さなくてよい
            if let Some((lo, hi)) = m.range {
                s.slabs.remove(lo, hi);
            }
        }
        s.dormant.retain(|&x| x != slot);
        s.ready.retain(|&x| x != slot);
        s.members.iter().filter(|m| m.is_some()).count()
    }

    /// 節 `n` の entity `e` から下の部分条件を評価し、 真なら節の答え (鍵 id, 範囲の値) を返す。
    /// 子は **記録があれば記録を使い**、 不明な時だけ Column から評価し直す (module doc
    /// 「正しさの根拠」)。
    fn eval(&self, r: &impl CellReader, s: &Settled, n: usize, e: u32) -> Option<Ans> {
        self.eval_known(r, s, n, e, None)
    }

    /// `eval` の、 子 1 本の答えが分かっている版。 `known = (子の節, 子の答え)` の子は ref も
    /// Column も読まずにその答えを使う (その子の entity から逆引きで来た `e` 用 — `e` の ref は
    /// その entity を指している)。
    fn eval_known(&self, r: &impl CellReader, s: &Settled, n: usize, e: u32, known: Option<(usize, Option<Ans>)>) -> Option<Ans> {
        let node = &self.nodes[n];
        if !node.local.iter().all(|p| p.matches(r, e)) {
            return None;
        }
        // 範囲の値 (範囲の穴の節では自分の値、 道の上の節では道の子の値)。 どの member の範囲にも
        // 入らない値は偽と同じ (購読の無い値と同じく、 そこでの書き換えを展開しない)
        let mut v = 0;
        if let Some((h, _)) = node.range {
            v = r.cell(h, e)?;
            if !self.group && !s.slabs.covered(v) {
                return None;
            }
        }
        let mut stack = [0u32; 8];
        let mut heap = Vec::new();
        let buf: &mut [u32] = if node.key_len <= stack.len() {
            &mut stack[..node.key_len]
        } else {
            heap.resize(node.key_len, 0);
            &mut heap
        };
        let mut i = 0;
        for &(h, _) in &node.holes {
            buf[i] = r.cell(h, e)?;
            i += 1;
        }
        for &c in &node.children {
            let (id, cv) = match known {
                Some((kc, ans)) if kc == c => ans,
                _ => {
                    let t = r.cell(self.nodes[c].via, e)?;
                    match rec_at(&s.recs, &s.vals, c, t) {
                        Some(ans) => ans,
                        None => self.eval(r, s, c, t),
                    }
                }
            }?;
            if self.on_path[c] {
                v = cv;
            }
            if self.nodes[c].has_holes {
                buf[i] = id;
                i += 1;
            }
        }
        s.tables[n].find(buf).map(|id| (id, v))
    }

    /// member の鍵の射影を各節の表に載せ、 根の鍵 id を返す。
    fn install(&self, s: &mut Settled, vals: &[u32]) -> u32 {
        let mut ids = vec![0u32; self.nodes.len()];
        let mut buf = Vec::new();
        for &n in &self.order {
            let node = &self.nodes[n];
            buf.clear();
            buf.extend(node.holes.iter().map(|&(_, slot)| vals[slot]));
            buf.extend(node.children.iter().filter(|&&c| self.nodes[c].has_holes).map(|&c| ids[c]));
            ids[n] = s.tables[n].intern(&buf);
        }
        if s.key(ids[0]).is_none() {
            // id は昇順に振られるので末尾に足せば並びが保たれる
            debug_assert!(s.keys.last().is_none_or(|k| k.id < ids[0]));
            s.keys.push(RootKey::new(ids[0]));
        }
        ids[0]
    }

    /// `install` の逆: member の鍵の射影を各節の表から 1 つ外す。
    fn uninstall(&self, s: &mut Settled, vals: &[u32]) {
        let mut ids = vec![0u32; self.nodes.len()];
        let mut buf = Vec::new();
        for &n in &self.order {
            let node = &self.nodes[n];
            buf.clear();
            buf.extend(node.holes.iter().map(|&(_, slot)| vals[slot]));
            buf.extend(node.children.iter().filter(|&&c| self.nodes[c].has_holes).map(|&c| ids[c]));
            ids[n] = s.tables[n].find(&buf).unwrap_or(u32::MAX);
            s.tables[n].release(&buf);
            let gone = n == 0 && s.tables[0].find(&buf).is_none();
            if let Some(i) = gone.then(|| s.keys.binary_search_by_key(&ids[0], |k| k.id).ok()).flatten() {
                s.keys.remove(i);
                // 外れた鍵の部分和 (根の記録には外れた鍵の id が残るが、 部分和に無ければ動かさない)
                if self.partial.is_some() {
                    let k = ids[0];
                    s.partial.retain(|_, p| {
                        p.n.retain(|x| x.0 != k);
                        !p.n.is_empty()
                    });
                }
            }
        }
    }

    /// 鍵 `vals` (範囲の穴は `range`) の結果を含む根の上位集合。 値の穴があれば一番浅い穴の値で
    /// 索引を引いて根まで遡る (結果の根は全ての穴で値が一致するので、 どの穴から遡っても漏れない)。
    /// 無ければ範囲の穴の範囲、 それも無ければ索引で引ける条件 (無ければ条件の紐を持つ全 entity)
    /// から遡る。
    fn walk(&self, r: &impl CellReader, vals: &[u32], range: Option<(u32, u32)>) -> Vec<u32> {
        let hole = self
            .order
            .iter()
            .rev()
            .find_map(|&n| self.nodes[n].holes.first().map(|&(h, slot)| (n, r.pull(h, vals[slot]))));
        let pick = hole
            .or_else(|| {
                let ((n, h), (lo, hi)) = (self.range?, range?);
                Some((n, r.pull_range(h, lo, hi)))
            })
            .or_else(|| {
                self.order.iter().rev().find_map(|&n| {
                    self.nodes[n].local.iter().find_map(|p| match p {
                        Pred::In(h, vs) => Some((n, vs.iter().flat_map(|&v| r.pull(*h, v)).collect())),
                        _ => None,
                    })
                })
            })
            .or_else(|| {
                self.order.iter().rev().find_map(|&n| self.nodes[n].local.first().map(|p| (n, r.with_himo(p.himo()))))
            });
        let Some((n, ents)) = pick else { return Vec::new() };
        let (_, mut ents) = self.climb(r, n, ents, 0, |_, _| {});
        ents.sort_unstable();
        ents.dedup();
        ents
    }

    /// 節 `n` の entity `ents` から、 それを (ref の道で) 指している節 `to` の entity まで遡る。
    /// 途中の各節 (`n` を含み `to` を含まない) で `f(節, entity)` を呼ぶ。
    fn climb(&self, r: &impl CellReader, mut n: usize, mut ents: Vec<u32>, to: usize, mut f: impl FnMut(usize, &[u32])) -> (usize, Vec<u32>) {
        while n != to && n != 0 {
            f(n, &ents);
            let via = self.nodes[n].via;
            let mut up: Vec<u32> = ents.iter().flat_map(|&e| r.pull(via, e)).collect();
            up.sort_unstable();
            up.dedup();
            ents = up;
            n = self.nodes[n].parent;
        }
        (n, ents)
    }

    /// 鍵の表が増えると、 増える前に 「偽」 と記録した節の entity が今は真でありうる (購読の
    /// 無かった値が購読された)。 評価は子の記録を信じるので、 各穴の値を持つ entity から根の
    /// 手前まで遡って記録を不明に戻す (test `late_member_sees_hub_recorded_while_unsubscribed`)。
    /// 記録の意味が変わりうるのは、 部分木の穴が全部この member の値と一致する entity だけ =
    /// どの穴から遡っても含まれる。
    fn forget_stale(&self, r: &impl CellReader, s: &mut Settled, vals: &[u32]) {
        for (n, node) in self.nodes.iter().enumerate().skip(1) {
            for &(h, slot) in &node.holes {
                self.climb(r, n, r.pull(h, vals[slot]), 0, |m, ents| {
                    for &e in ents {
                        s.recs[m].forget(e);
                    }
                });
            }
        }
    }

    /// 範囲の穴が根でない節にある時、 帯が変わった値の範囲 `regions` の entity の記録を範囲の穴の
    /// 節から根の手前まで不明に戻し、 範囲の穴の節で評価し直させる。 直すのは 2 つ:
    /// - どの member にも含まれなかった帯が含まれた → 「偽」 と記録した entity が今は真でありうる
    /// - 帯が割れた → 道の上の節は 「帯が変わった時だけ」 親へ展開するので、 親の記録の値は子の値と
    ///   同じ帯のどこかにある (帯の中では区別が要らない)。 帯が割れると両者が別の帯に分かれうる
    ///
    /// 道の上を全部不明に戻すので、 評価し直した値は根まで展開される。
    fn forget_regions(&self, r: &impl CellReader, s: &mut Settled, regions: &[(u32, u32)]) {
        let Some((rn, h)) = self.range.filter(|&(n, _)| n != 0) else { return };
        let mut ents: Vec<u32> = regions.iter().flat_map(|&(lo, hi)| r.pull_range(h, lo, hi)).collect();
        ents.sort_unstable();
        ents.dedup();
        if ents.is_empty() {
            return;
        }
        self.climb(r, rn, ents.clone(), 0, |m, es| {
            for &e in es {
                s.recs[m].forget(e);
            }
        });
        self.push_marks(rn, ents);
    }

    /// 節 `n` に印を積む (poll 側から)。
    fn push_marks(&self, n: usize, ents: Vec<u32>) {
        let i = shard_index();
        {
            let mut p = self.pending[i].0.lock();
            for e in ents {
                p.nodes[n].add(e);
            }
        }
        self.set_dirty(i);
    }

    /// 未有効化の member を有効化する: 鍵を解決して表に載せ、 初期候補を根の印と member の
    /// changed に積む (text が vocab にまだ無い member は dormant のまま)。
    fn activate(&self, r: &impl CellReader, s: &mut Settled) {
        if s.dormant.is_empty() {
            return;
        }
        for slot in std::mem::take(&mut s.dormant) {
            let Some(m) = s.members[slot].as_ref() else { continue };
            let Some(vals) = resolve(r, &m.key) else {
                s.dormant.push(slot);
                continue;
            };
            let range = range_of(&m.key);
            let known_before = s.tables[0].next as usize;
            let id = self.install(s, &vals);
            let new_key = id as usize >= known_before;
            if let Some((lo, hi)) = range {
                let mut regions = s.slabs.add(lo, hi);
                if lo <= hi {
                    regions.push((lo, hi));
                }
                self.forget_regions(r, s, &regions);
            }
            self.forget_stale(r, s, &vals);
            let roots = self.walk(r, &vals, range);
            if let Some(rk) = s.key(id) {
                rk.members.push(slot);
                rk.ivs = None;
            }
            let Settled { members, ready, recs, vals: rvals, keys, .. } = &mut *s;
            if let Some(m) = members[slot].as_mut() {
                m.root_key = Some(id);
                m.range = range;
                m.vals = vals;
                if let Some(g) = m.grp.as_mut() {
                    // 集計: 件数は鍵が持つ。 既にある鍵なら今の group を全部初回の報告に積む
                    if let Ok(i) = keys.binary_search_by_key(&id, |k| k.id) {
                        for &v in keys[i].groups.keys() {
                            g.changed.add(v);
                        }
                    }
                    // 部分和の節 (1 段目) の entity にも印を付けて記録を作っておく。 根の評価は子の記録が
                    // 無いと子を評価し直すだけで記録しない (記録は印を消費した時だけ書く) ので、 記録の無い
                    // 1 段目の先は最初に書き換わった時に 「前が不明」 = 配下の根を全部評価し直す (hacg で
                    // 大きな市区町村の最初の都道府県変更が ms 級)。 登録の時に 1 回払っておく
                    if let Some(c1) = self.partial {
                        let mut ts: Vec<u32> = roots.iter().filter_map(|&e| r.cell(self.nodes[c1].via, e)).collect();
                        ts.sort_unstable();
                        ts.dedup();
                        self.push_marks(c1, ts);
                    }
                    self.push_marks(0, roots);
                    continue;
                }
                // 新しい鍵なら今その鍵の根は居ない — 候補を評価した時の遷移 (apply_root) が
                // この member に届く。 既にある鍵 (同じ条件の購読が他に居る / 居た) なら、 今その鍵の
                // 根はもう遷移しないので、 ここで初回の報告に積む。 候補 (上位集合) を全部積むと、
                // 購読を一度に多数張った時に 購読数 × 候補数 の一時メモリになる
                if !new_key {
                    for &e in &roots {
                        if m.has(root_at(recs, rvals, e)) {
                            m.note(slot, ready, e, false);
                        }
                    }
                }
            }
            self.push_marks(0, roots);
        }
    }

    /// 印を消費して評価し直す。 深い節から: 答えが変わった entity を親へ展開、 根で集合を更新。
    fn settle(&self, r: &impl CellReader, s: &mut Settled) {
        self.activate(r, s);
        let mask = self.dirty.swap(0, Ordering::AcqRel);
        if mask == 0 {
            return;
        }
        let mut work: Vec<Vec<u32>> = vec![Vec::new(); self.nodes.len()];
        let mut freed = Vec::new();
        for (i, sh) in self.pending.iter().enumerate() {
            if mask & (1 << i) == 0 {
                continue;
            }
            let mut p = sh.0.lock();
            for (w, m) in work.iter_mut().zip(p.nodes.iter_mut()) {
                if w.is_empty() {
                    *w = m.take_raw();
                } else {
                    w.append(&mut m.take_raw());
                }
            }
            freed.append(&mut p.freed.take_raw());
        }
        for e in freed {
            // 報告済みの根が解放された = 呼び手の持つ eid は別物になりうる。 今の鍵と違う
            // member の報告済みは、 鍵が変わった時点で left + changed を積まれている
            if let Some(i) = s.recs[0].key(e).and_then(|k| s.keys.binary_search_by_key(&k, |x| x.id).ok()) {
                let Settled { keys, members, ready, .. } = &mut *s;
                for &slot in &keys[i].members {
                    if let Some(m) = members[slot].as_mut().filter(|m| m.reported.get(e)) {
                        m.note(slot, ready, e, true);
                    }
                }
            }
        }
        let always = self.expand_always.load(Ordering::Relaxed);
        let mode = match (self.range.is_some(), self.group) {
            (false, _) => RootMode::Plain,
            (true, false) => RootMode::Ranged,
            (true, true) => RootMode::Grouped,
        };
        // 根の子が 1 本なら、 その子の entity から逆引きした根は 「子の答え = その entity の新しい
        // 記録」 と分かっているので、 評価で ref と子の記録を読み直さない (会社 1 社の移転で配下
        // 数十万人が動く時、 1 人あたりの評価がほぼ根自身の条件だけになる)
        let single_child = (self.nodes[0].children.len() == 1).then(|| self.nodes[0].children[0]);
        let mut via_child: Vec<(Option<Ans>, u32, Vec<u32>)> = Vec::new();
        let root_plain = self.nodes[0].local.is_empty() && self.nodes[0].holes.is_empty() && self.nodes[0].range.is_none();
        for &n in &self.order {
            let mut list = std::mem::take(&mut work[n]);
            list.sort_unstable();
            list.dedup();
            if n == 0 {
                let mut cache = KeyCache::default();
                for (ans, t, ents) in via_child.drain(..) {
                    let known = Some((single_child.unwrap_or(usize::MAX), ans));
                    // 根自身に条件も穴も無ければ答えは子の答えだけで決まる = 全員同じ
                    let same = root_plain.then(|| self.eval_known(r, s, 0, 0, known));
                    for e in ents {
                        let now = same.unwrap_or_else(|| self.eval_known(r, s, 0, e, known));
                        match self.partial {
                            Some(_) => s.apply_root_partial(e, now.map(|a| (a.0, t)), now.map_or(0, |a| a.1)),
                            None => s.apply_root_cached(e, now, mode, &mut cache),
                        }
                    }
                }
                // 印の付いた根 (ref や根の条件の紐が書かれた) は全部読み直し、 上の近道より後に
                // 当てる (逆引きの後に ref が書き換わった根は、 こちらの今の Column の答えが勝つ)
                for e in list {
                    let now = self.eval(r, s, 0, e);
                    match self.partial {
                        Some(c1) => {
                            let g = now.map_or(0, |a| a.1);
                            let t = now.and_then(|_| r.cell(self.nodes[c1].via, e));
                            let now = now.and_then(|a| Some((a.0, t?)));
                            s.apply_root_partial(e, now, g);
                        }
                        None => s.apply_root_cached(e, now, mode, &mut cache),
                    }
                }
                continue;
            }
            let (parent, via, path) = (self.nodes[n].parent, self.nodes[n].via, self.on_path[n]);
            for e in list {
                let now = self.eval(r, s, n, e);
                let prev = rec_at(&s.recs, &s.vals, n, e);
                s.recs[n].set(e, now.map(|a| a.0));
                if path {
                    s.vals[n].put(e, now.map_or(0, |a| a.1 + 1));
                }
                // 範囲の値は帯が変わった時だけ運ぶ (帯の中の違いはどの member にも区別が付かない)。
                // 集計の部分和の節では group の値は運ばない (部分和を移す)
                let partial_node = self.partial == Some(n);
                let changed = match (prev, now) {
                    (Some(Some((pi, pv))), Some((ni, nv))) => {
                        pi != ni
                            || (path
                                && !partial_node
                                && if self.group { pv != nv } else { !s.slabs.same(pv, nv) })
                    }
                    (Some(None), None) => false,
                    _ => true,
                };
                if partial_node && let Some((_, nv)) = now {
                    s.move_partial(e, nv);
                }
                if always || changed {
                    let up = r.pull(via, e);
                    if parent == 0 && single_child == Some(n) {
                        via_child.push((now, e, up));
                    } else {
                        work[parent].extend(up);
                    }
                }
            }
        }
    }
}

/// 購読せずに条件を 1 回だけ評価する (`Engine::find_by`)。 枝 (`dnf` の結果) ごとに候補を
/// 数えて全条件で評価し、 和を取る。
pub(crate) fn find_once(r: &impl CellReader, branches: Vec<Vec<LivePred>>) -> Vec<u32> {
    let mut out: Vec<u32> = branches.into_iter().flat_map(|b| find_branch(r, b)).collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn find_branch(r: &impl CellReader, preds: Vec<LivePred>) -> Vec<u32> {
    let (sig, flats, key) = canonical(preds, None);
    let fam = Family::new(0, sig, flats);
    let Some(vals) = resolve(r, &key) else { return Vec::new() };
    let range = range_of(&key);
    let mut s = Settled::new(&fam.nodes.iter().map(|x| x.key_len).collect::<Vec<_>>());
    let id = fam.install(&mut s, &vals);
    if let Some((lo, hi)) = range {
        s.slabs.add(lo, hi);
    }
    let m = Member {
        id: 0,
        key: Vec::new(),
        root_key: Some(id),
        range,
        vals: Vec::new(),
        reported: Bits::default(),
        left: Marks::default(),
        changed: Marks::default(),
        queued: false,
        union: None,
        grp: None,
    };
    let roots = fam.walk(r, &vals, range);
    roots.into_iter().filter(|&e| m.has(fam.eval(r, &s, 0, e))).collect()
}

type Route = (Arc<Family>, usize);

/// route と family の一覧。 登録・解除のたびに作り直して差し替え (copy-on-write)、 書き込み側は
/// epoch で読む — 書き込みごとに共有の lock (RwLock の read も共有の counter への RMW) を取ると、
/// 書き手が多い時にその cache line を取り合う。
#[derive(Clone, Default)]
struct Snapshot {
    /// himo_id → その紐に印を付ける (family, 節)。
    routes: Vec<Vec<Route>>,
    /// 登録中の全 family (形の照合と entity 解放の通知用)。
    families: Vec<Arc<Family>>,
}

/// engine 1 個につき 1 つ。 himo_id → その紐を条件に含む (family, 節)。
pub(crate) struct LiveRegistry {
    /// 登録中の family 数。 0 なら touch は即 return (書き込み hot path のコスト = これ 1 回)。
    active: AtomicUsize,
    /// 今の [`Snapshot`] (null = まだ登録が無い)。
    snap: epoch::Atomic<Snapshot>,
    /// 登録・解除 (snapshot の作り直し) を 1 本ずつにする。
    edit: Mutex<()>,
    /// 枝の差分を積んだが呼び手にまだ渡していない `Or` の購読 (`count` などが積んだ分。
    /// `poll_all` が拾う)。
    ready_unions: Mutex<Vec<Arc<Union>>>,
    next_id: AtomicUsize,
    next_member_id: std::sync::atomic::AtomicU64,
    /// poll が返す EntityId の peer prefix (`Engine::set_peer_id` が追従させる)。
    peer: AtomicU32,
}

impl Drop for LiveRegistry {
    fn drop(&mut self) {
        // SAFETY: &mut self = 他に snapshot を読んでいる thread は無い
        unsafe {
            let s = self.snap.load(Ordering::Relaxed, epoch::unprotected());
            if !s.is_null() {
                drop(s.into_owned());
            }
        }
    }
}

impl LiveRegistry {
    pub(crate) fn new(peer: u32) -> Self {
        Self {
            active: AtomicUsize::new(0),
            snap: epoch::Atomic::null(),
            edit: Mutex::new(()),
            ready_unions: Mutex::new(Vec::new()),
            next_id: AtomicUsize::new(0),
            next_member_id: std::sync::atomic::AtomicU64::new(0),
            peer: AtomicU32::new(peer),
        }
    }

    pub(crate) fn set_peer(&self, peer: u32) {
        self.peer.store(peer, Ordering::Release);
    }

    /// 今の snapshot を `f` に渡す。
    #[inline]
    fn with_snap<T>(&self, f: impl FnOnce(&Snapshot) -> T) -> Option<T> {
        let guard = epoch::pin();
        let s = self.snap.load(Ordering::Acquire, &guard);
        // SAFETY: snapshot は Owned から作って差し替え、 古いものは defer_destroy で guard の
        // 外まで生きる
        unsafe { s.as_ref() }.map(f)
    }

    /// 登録中の全 family。
    fn families(&self) -> Vec<Arc<Family>> {
        self.with_snap(|s| s.families.clone()).unwrap_or_default()
    }

    /// snapshot を作り直して差し替える (`edit` を持って呼ぶ)。
    fn replace_snap(&self, f: impl FnOnce(&mut Snapshot)) {
        let guard = epoch::pin();
        let cur = self.snap.load(Ordering::Acquire, &guard);
        // SAFETY: with_snap と同じ
        let mut next = unsafe { cur.as_ref() }.cloned().unwrap_or_default();
        f(&mut next);
        let old = self.snap.swap(epoch::Owned::new(next), Ordering::AcqRel, &guard);
        if !old.is_null() {
            // SAFETY: 差し替え済み = 以後この snapshot を新しく読む thread は無い
            unsafe { guard.defer_destroy(old) };
        }
    }

    /// Column の `(himo_id, eid)` を書き換えた **後** (himo の write_lock を離した後) に呼ぶ。
    /// 評価はしない — 印を付けるだけ。
    #[inline]
    pub(crate) fn touch(&self, himo_id: u16, eid: u32) {
        if self.active.load(Ordering::Acquire) == 0 {
            return;
        }
        self.touch_slow(himo_id, eid);
    }

    #[cold]
    fn touch_slow(&self, himo_id: u16, eid: u32) {
        self.with_snap(|s| {
            if let Some(fams) = s.routes.get(himo_id as usize) {
                for (f, node) in fams {
                    f.mark(*node, eid);
                }
            }
        });
    }

    /// entity の slot を解放した **後** に呼ぶ (slot 再利用の検出用)。
    #[inline]
    pub(crate) fn freed(&self, eid: u32) {
        if self.active.load(Ordering::Acquire) == 0 {
            return;
        }
        self.with_snap(|s| {
            for f in &s.families {
                f.mark_freed(eid);
            }
        });
    }

    /// 購読を登録する (登録手順の 1 段目、 module doc 参照)。 同じ形の family があればそこに
    /// member として加わり、 無ければ family を作って route に載せる。 barrier と初期候補は
    /// 呼び手 (engine) がこの後にやる。
    ///
    /// `expand_always` (ablation / 計測用) の購読は、 同じ形でも既定の購読とは別の family になる。
    pub(crate) fn register(self: &Arc<Self>, preds: Vec<LivePred>, expand_always: bool) -> LiveQuery {
        let id = self.next_member_id.fetch_add(1, Ordering::Relaxed);
        let (family, slot) = self.register_member(id, preds, None, expand_always, None);
        LiveQuery { kind: Kind::One { family, slot }, id, registry: self.clone() }
    }

    /// 集計の購読を登録する: `preds` の結果を `group` (ref の道 + 紐) の値ごとに数える。
    pub(crate) fn register_counts(self: &Arc<Self>, preds: Vec<LivePred>, group: (Vec<u16>, u16)) -> LiveCounts {
        let id = self.next_member_id.fetch_add(1, Ordering::Relaxed);
        let (family, slot) = self.register_member(id, preds, Some(group), false, None);
        LiveCounts { family, slot, id, registry: self.clone() }
    }

    /// `Or` の購読を登録する: 枝 (AND) ごとに member を登録し、 枝の差分を積む [`Union`] に束ねる。
    pub(crate) fn register_any(self: &Arc<Self>, branches: Vec<Vec<LivePred>>, expand_always: bool) -> LiveQuery {
        let id = self.next_member_id.fetch_add(1, Ordering::Relaxed);
        let n = branches.len();
        let u = Arc::new_cyclic(|w| Union {
            id,
            branches: branches
                .into_iter()
                .enumerate()
                .map(|(i, b)| {
                    let bid = self.next_member_id.fetch_add(1, Ordering::Relaxed);
                    self.register_member(bid, b, None, expand_always, Some((w.clone(), i)))
                })
                .collect(),
            state: Mutex::new(UnionState::new(n)),
        });
        LiveQuery { kind: Kind::Any(u), id, registry: self.clone() }
    }

    fn register_member(
        &self,
        id: u64,
        preds: Vec<LivePred>,
        group: Option<(Vec<u16>, u16)>,
        expand_always: bool,
        union: Option<(std::sync::Weak<Union>, usize)>,
    ) -> (Arc<Family>, usize) {
        let (mut sig, flats, key) = canonical(preds, group);
        sig.insert(0, expand_always as u32);
        let _edit = self.edit.lock();
        let family = match self.families().into_iter().find(|f| f.sig == sig) {
            Some(f) => f,
            None => {
                let f = Arc::new(Family::new(self.next_id.fetch_add(1, Ordering::Relaxed) as u64, sig, flats));
                f.expand_always.store(expand_always, Ordering::Relaxed);
                self.replace_snap(|s| {
                    for &(h, node) in &f.routes {
                        let h = h as usize;
                        if s.routes.len() <= h {
                            s.routes.resize_with(h + 1, Vec::new);
                        }
                        s.routes[h].push((f.clone(), node));
                    }
                    s.families.push(f.clone());
                });
                // route を載せてから active を上げる (Release)。 touch は active (Acquire) →
                // snapshot (Acquire) の順なので、 active を見た touch は必ずこの route を見る。
                self.active.fetch_add(1, Ordering::Release);
                f
            }
        };
        let slot = family.add_member(id, key, union);
        (family, slot)
    }

    /// 前回の poll から出入りのあった全購読の差分 (`(LiveQuery::id, 差分)`、 id 昇順)。 各購読の
    /// `poll` と同じ報告状態を進める (どちらで受け取っても差分は 1 回だけ届く)。 コストは
    /// family 数 + 出入りの数で、 購読の数によらない。
    pub(crate) fn poll_all(&self, r: &impl CellReader) -> Vec<(u64, LiveDelta)> {
        let peer = self.peer.load(Ordering::Acquire);
        let fams = self.families();
        let mut out = Vec::new();
        // `Or` の枝の差分 (呼び手でなく枝の購読に積む)
        let mut branch: Vec<(Arc<Union>, usize, LiveDelta)> = Vec::new();
        for f in fams {
            let mut guard = f.settled.lock();
            f.settle(r, &mut guard);
            let Settled { recs, vals, members, ready, .. } = &mut *guard;
            for slot in std::mem::take(ready) {
                if let Some(m) = members[slot].as_mut() {
                    let union = m.union.as_ref().map(|(w, i)| (w.upgrade(), *i));
                    let d = m.drain(|e| root_at(recs, vals, e), if union.is_some() { 0 } else { peer });
                    match union {
                        Some((Some(u), i)) if !d.is_empty() => branch.push((u, i, d)),
                        Some(_) => {}
                        None if !d.is_empty() => out.push((m.id, d)),
                        None => {}
                    }
                }
            }
        }
        // family の lock を離してから Or の購読の lock を取る (Or 側は Or → family の順に取らない)
        branch.extend(std::mem::take(&mut *self.ready_unions.lock()).into_iter().map(|u| (u, 0, LiveDelta::default())));
        branch.sort_by_key(|(u, _, _)| u.id);
        let mut i = 0;
        while i < branch.len() {
            let u = branch[i].0.clone();
            let mut st = u.state.lock();
            while i < branch.len() && branch[i].0.id == u.id {
                st.absorb(branch[i].1, std::mem::take(&mut branch[i].2));
                i += 1;
            }
            let d = st.drain(peer);
            if !d.is_empty() {
                out.push((u.id, d));
            }
        }
        out.sort_unstable_by_key(|(id, _)| *id);
        out
    }

    fn unregister(&self, family: &Arc<Family>, slot: usize) {
        let _edit = self.edit.lock();
        if family.remove_member(slot) > 0 {
            return;
        }
        self.replace_snap(|s| {
            for fs in s.routes.iter_mut() {
                fs.retain(|(f, _)| f.id != family.id);
            }
            s.families.retain(|f| f.id != family.id);
        });
        self.active.fetch_sub(1, Ordering::Release);
    }
}

/// `Or` の購読: 枝 (AND の購読、 family の member) の差分を積んで、 どれかの枝に居る entity の
/// 集合を持つ。 枝ごとに枝に居る entity を持ち、 どの枝にも居ない ↔ どれかに居る をまたいだら出入り。
///
/// 枝の差分は枝の member の報告状態として 1 回だけ流れる (このこちら側で積む)。 同じ枝の差分に
/// 同じ eid が removed と added の両方で来たら (枝で出て入り直した = slot 再利用など)、 この購読
/// でも出て入り直したとして扱う。
pub(crate) struct Union {
    id: u64,
    branches: Vec<(Arc<Family>, usize)>,
    state: Mutex<UnionState>,
}

struct UnionState {
    /// 枝ごと: 枝の報告状態で枝に居る entity (枝の差分を積んだもの)。 entity ごとの数を配列で
    /// 持つと entity 空間の広さに比例する (結果が散らばると購読 1 本で page を全部確保する) —
    /// 疎な集合を枝の数だけ持てば結果の大きさに比例する。
    branches: Vec<Bits>,
    /// どれかの枝に居る entity の数。
    size: usize,
    reported: Bits,
    left: Marks,
    changed: Marks,
    /// `LiveRegistry::ready_unions` に載っているか。
    queued: bool,
}

impl UnionState {
    fn new(n: usize) -> Self {
        UnionState {
            branches: (0..n).map(|_| Bits::default()).collect(),
            size: 0,
            reported: Bits::default(),
            left: Marks::default(),
            changed: Marks::default(),
            queued: false,
        }
    }

    /// どれかの枝に居るか。
    #[inline]
    fn has(&self, e: u32) -> bool {
        self.branches.iter().any(|b| b.get(e))
    }

    /// 枝 `b` の差分 (eid は local) を積む。
    fn absorb(&mut self, b: usize, d: LiveDelta) {
        let (mut ai, mut ri) = (0, 0);
        // 同じ枝で removed と added の両方 = 出て入り直した (両方昇順)
        while ai < d.added.len() && ri < d.removed.len() {
            match d.added[ai].cmp(&d.removed[ri]) {
                std::cmp::Ordering::Less => ai += 1,
                std::cmp::Ordering::Greater => ri += 1,
                std::cmp::Ordering::Equal => {
                    self.left.add(d.added[ai] as u32);
                    ai += 1;
                    ri += 1;
                }
            }
        }
        for (list, on) in [(&d.removed, false), (&d.added, true)] {
            for &e in list {
                let e = e as u32;
                let was = self.has(e);
                self.branches[b].put(e, on);
                let now = self.has(e);
                if was != now {
                    if now {
                        self.size += 1;
                    } else {
                        self.size -= 1;
                    }
                }
                self.changed.add(e);
            }
        }
    }

    fn drain(&mut self, peer: u32) -> LiveDelta {
        self.queued = false;
        let branches = &self.branches;
        drain_marks(&mut self.changed, &mut self.left, &mut self.reported, |e| branches.iter().any(|b| b.get(e)), peer)
    }

    /// どれかの枝に居る entity (昇順)。
    fn members(&self) -> Vec<u32> {
        let mut out: Vec<u32> = self.branches.iter().flat_map(|b| b.iter()).collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

impl Union {
    /// 枝を全部 settle して差分を積む。 family の lock と自分の lock を同時に持たない
    /// (`poll_all` は family → 自分 の順に取るので、 逆順に重ねると deadlock)。
    fn settle(self: &Arc<Self>, r: &impl CellReader) -> parking_lot::MutexGuard<'_, UnionState> {
        let mut ds = Vec::with_capacity(self.branches.len());
        for (i, (f, slot)) in self.branches.iter().enumerate() {
            let mut g = f.settled.lock();
            f.settle(r, &mut g);
            let Settled { recs, vals, members, .. } = &mut *g;
            if let Some(m) = members[*slot].as_mut() {
                ds.push((i, m.drain(|e| root_at(recs, vals, e), 0)));
            }
        }
        let mut st = self.state.lock();
        for (i, d) in ds {
            st.absorb(i, d);
        }
        st
    }

    fn is_dirty(&self) -> bool {
        self.branches.iter().any(|(f, slot)| f.member_dirty(*slot)) || !self.state.lock().changed.is_empty()
    }
}

enum Kind {
    /// AND の購読 = family の member 1 つ。
    One { family: Arc<Family>, slot: usize },
    /// `Or` を含む購読。
    Any(Arc<Union>),
}

/// 登録済みの live query。 drop で購読解除。
///
/// 評価は engine の索引を引くので、 読み出し系 (`poll` / `count` / `contains` / `members`)
/// は購読した engine を引数に取る (schema 層の `LiveQuery` は engine を抱えていて引数不要)。
/// engine を借用しないので struct に入れて持ち回れる。 `Send + Sync`。
pub struct LiveQuery {
    kind: Kind,
    id: u64,
    registry: Arc<LiveRegistry>,
}

impl LiveQuery {
    fn parts(&self) -> Vec<(&Arc<Family>, usize)> {
        match &self.kind {
            Kind::One { family, slot } => vec![(family, *slot)],
            Kind::Any(u) => u.branches.iter().map(|(f, s)| (f, *s)).collect(),
        }
    }

    /// 条件に出てくる全ての紐 (engine の barrier 用)。
    pub(crate) fn himos(&self) -> Vec<u16> {
        let mut hs: Vec<u16> = self.parts().into_iter().flat_map(|(f, _)| f.routes.iter().map(|&(h, _)| h)).collect();
        hs.sort_unstable();
        hs.dedup();
        hs
    }

    /// 有効化する (登録手順の 3 段目): 鍵を表に載せ、 初期候補を根に印付けする。 候補に居ない
    /// 根は、 barrier 後の書き込みなら印が、 barrier 前の書き込みなら候補の走査が拾っている。
    /// 呼ばなくても最初の poll が有効化する。
    pub(crate) fn seed(&self, r: &impl CellReader) {
        for (f, _) in self.parts() {
            let mut s = f.settled.lock();
            f.activate(r, &mut s);
        }
    }

    fn check_engine(&self, r: &crate::engine::Engine) {
        assert!(
            Arc::ptr_eq(&self.registry, r.live_registry()),
            "LiveQuery: 購読した engine とは別の engine が渡された"
        );
    }

    /// 前回 poll からの差分を返し、 それを 「呼び手に渡した」 状態として記録する。
    /// 初回は登録時点の結果全体が `added` に入る。 eid は `query_by_id` / schema の `find()`
    /// と同じ形 (peer prefix 付き)。
    ///
    /// 他 thread の書き込み途中に呼ぶと、 条件の紐だけ書かれて残りの紐がまだの row が
    /// `added` に出うる (module doc 「追うもの / 追わないもの」)。
    ///
    /// `eng` は購読した engine (別の engine を渡すと panic)。
    pub fn poll(&self, eng: &crate::engine::Engine) -> LiveDelta {
        self.check_engine(eng);
        self.poll_with(eng)
    }

    pub(crate) fn poll_with(&self, r: &impl CellReader) -> LiveDelta {
        let peer = self.registry.peer.load(Ordering::Acquire);
        match &self.kind {
            Kind::One { family, slot } => {
                let mut guard = family.settled.lock();
                family.settle(r, &mut guard);
                let Settled { recs, vals, members, .. } = &mut *guard;
                match members[*slot].as_mut() {
                    Some(m) => m.drain(|e| root_at(recs, vals, e), peer),
                    None => LiveDelta::default(),
                }
            }
            Kind::Any(u) => u.settle(r).drain(peer),
        }
    }

    /// engine 内で一意な購読 id。 [`Engine::poll_live`](crate::engine::Engine::poll_live) の
    /// 差分がどの購読のものかを表す。
    pub fn id(&self) -> u64 {
        self.id
    }

    /// 未 poll の変化がありうるか (false なら `poll` は空を返す)。 評価はしないので軽い。
    pub fn is_dirty(&self) -> bool {
        match &self.kind {
            Kind::One { family, slot } => family.member_dirty(*slot),
            Kind::Any(u) => u.is_dirty(),
        }
    }

    /// 現在の結果件数 (poll 済みかどうかに関係なく、 今 find したら返る件数)。 O(1)。 ただし
    /// `Range` を含む購読は、 範囲の違う購読と件数を共有できないので結果を数える (`members` と同じ)。
    pub fn count(&self, eng: &crate::engine::Engine) -> usize {
        self.check_engine(eng);
        self.count_with(eng)
    }

    pub(crate) fn count_with(&self, r: &impl CellReader) -> usize {
        let (family, slot) = match &self.kind {
            Kind::One { family, slot } => (family, *slot),
            Kind::Any(u) => return self.settle_union(u, r).size,
        };
        let mut s = family.settled.lock();
        family.settle(r, &mut s);
        let Some(m) = s.members[slot].as_ref() else { return 0 };
        let Some(k) = m.root_key else { return 0 };
        if m.range.is_some() {
            // 根の鍵を範囲の違う member と共有するので、 鍵ごとの件数は使えない
            return s.recs[0].with_key(k).into_iter().filter(|&e| m.has(s.root(e))).count();
        }
        s.keys.binary_search_by_key(&k, |x| x.id).map_or(0, |i| s.keys[i].count)
    }

    /// `Or` の購読の枝を settle して積む。 積んだ差分は `poll_all` が拾えるように登録する。
    fn settle_union<'u>(&self, u: &'u Arc<Union>, r: &impl CellReader) -> parking_lot::MutexGuard<'u, UnionState> {
        let mut st = u.settle(r);
        if !st.changed.is_empty() && !st.queued {
            st.queued = true;
            self.registry.ready_unions.lock().push(u.clone());
        }
        st
    }

    /// `eid` が現在の結果に含まれるか。
    pub fn contains(&self, eng: &crate::engine::Engine, eid: EntityId) -> bool {
        self.check_engine(eng);
        let e = enchudb_oplog::eid_local(eid);
        let (family, slot) = match &self.kind {
            Kind::One { family, slot } => (family, *slot),
            Kind::Any(u) => return self.settle_union(u, eng).has(e),
        };
        let mut s = family.settled.lock();
        family.settle(eng, &mut s);
        s.members[slot].as_ref().is_some_and(|m| m.has(s.root(e)))
    }

    /// 現在の結果全体 (eid 昇順)。 poll の状態は変えない。
    pub fn members(&self, eng: &crate::engine::Engine) -> Vec<EntityId> {
        self.check_engine(eng);
        let peer = self.registry.peer.load(Ordering::Acquire);
        let (family, slot) = match &self.kind {
            Kind::One { family, slot } => (family, *slot),
            Kind::Any(u) => {
                let st = self.settle_union(u, eng);
                return st.members().into_iter().map(|e| enchudb_oplog::make_eid(peer, e)).collect();
            }
        };
        let mut s = family.settled.lock();
        family.settle(eng, &mut s);
        let Some(m) = s.members[slot].as_ref() else { return Vec::new() };
        let Some(k) = m.root_key else { return Vec::new() };
        s.recs[0]
            .with_key(k)
            .into_iter()
            .filter(|&e| m.has(s.root(e)))
            .map(|e| enchudb_oplog::make_eid(peer, e))
            .collect()
    }
}

impl Drop for LiveQuery {
    fn drop(&mut self) {
        for (f, slot) in self.parts() {
            self.registry.unregister(f, slot);
        }
    }
}

impl std::fmt::Debug for LiveQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let branches = self.parts().len();
        f.debug_struct("LiveQuery").field("id", &self.id).field("branches", &branches).finish()
    }
}

// ─────────────────────────── 集計の購読 ───────────────────────────

/// 条件に当てはまる entity を **group の列の値ごとに数えた件数** の live 版
/// ([`Engine::subscribe_counts`](crate::engine::Engine::subscribe_counts))。
///
/// 例: 「営業中の法人の都道府県別の件数」。 [`poll`](Self::poll) は前回から件数が変わった group と
/// 今の件数 (0 = その group が消えた) を返す。 積分 = 値で上書き。 初回は登録時点の全 group。
///
/// - group の列は ref の先でもよい (`company.city` ごと)。 会社の所在地が変わると配下の件数が
///   まとめて移る (社員を 1 人ずつ報告しない)
/// - group の列に値の無い entity は数えない (SQL の `GROUP BY` の NULL の group は無い)
/// - 件数は同じ条件の集計の購読どうしで共有する (条件の値だけ違う購読は同じ family)
pub struct LiveCounts {
    family: Arc<Family>,
    slot: usize,
    id: u64,
    registry: Arc<LiveRegistry>,
}

impl LiveCounts {
    pub(crate) fn himos(&self) -> Vec<u16> {
        let mut hs: Vec<u16> = self.family.routes.iter().map(|&(h, _)| h).collect();
        hs.sort_unstable();
        hs.dedup();
        hs
    }

    pub(crate) fn seed(&self, r: &impl CellReader) {
        let mut s = self.family.settled.lock();
        self.family.activate(r, &mut s);
    }

    fn check_engine(&self, r: &crate::engine::Engine) {
        assert!(
            Arc::ptr_eq(&self.registry, r.live_registry()),
            "LiveCounts: 購読した engine とは別の engine が渡された"
        );
    }

    /// settle して `f(この購読の報告状態, 鍵の group の件数, 鍵の件数)`。
    fn with<T>(&self, r: &impl CellReader, f: impl FnOnce(&mut GroupState, &std::collections::BTreeMap<u32, u64>, usize) -> T) -> T {
        let mut guard = self.family.settled.lock();
        self.family.settle(r, &mut guard);
        let Settled { members, keys, .. } = &mut *guard;
        let Some(m) = members[self.slot].as_mut() else { return f(&mut GroupState::default(), &Default::default(), 0) };
        let empty = std::collections::BTreeMap::new();
        let (groups, count) = match m.root_key.and_then(|k| keys.binary_search_by_key(&k, |x| x.id).ok()) {
            Some(i) => (&keys[i].groups, keys[i].count),
            None => (&empty, 0),
        };
        match m.grp.as_mut() {
            Some(g) => f(g, groups, count),
            None => f(&mut GroupState::default(), groups, count),
        }
    }

    /// 前回 poll から件数が変わった group と今の件数 (値の昇順、 0 = group が消えた)。 値は
    /// `query_by_id` と同じ (Number は値、 Tag は vocab id、 Ref は local eid)。
    pub fn poll(&self, eng: &crate::engine::Engine) -> Vec<(u32, u64)> {
        self.check_engine(eng);
        self.poll_with(eng)
    }

    pub(crate) fn poll_with(&self, r: &impl CellReader) -> Vec<(u32, u64)> {
        self.with(r, |g, groups, _| g.drain(groups))
    }

    /// group `value` の今の件数 (poll の状態は変えない)。
    pub fn get(&self, eng: &crate::engine::Engine, value: u32) -> u64 {
        self.check_engine(eng);
        self.with(eng, |_, groups, _| groups.get(&value).copied().unwrap_or(0))
    }

    /// 今の全 group と件数 (値の昇順)。
    pub fn all(&self, eng: &crate::engine::Engine) -> Vec<(u32, u64)> {
        self.check_engine(eng);
        self.all_with(eng)
    }

    pub(crate) fn all_with(&self, r: &impl CellReader) -> Vec<(u32, u64)> {
        self.with(r, |_, groups, _| groups.iter().map(|(&v, &c)| (v, c)).collect())
    }

    /// 全 group の件数の和 (= 条件に当てはまり、 group の列に値のある entity の数)。
    pub fn total(&self, eng: &crate::engine::Engine) -> usize {
        self.check_engine(eng);
        self.with(eng, |_, _, count| count)
    }

    /// engine 内で一意な購読 id。
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Drop for LiveCounts {
    fn drop(&mut self) {
        self.registry.unregister(&self.family, self.slot);
    }
}

impl std::fmt::Debug for LiveCounts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveCounts").field("id", &self.id).finish()
    }
}

// ─────────────────────────── 会社単位の購読 ───────────────────────────

/// `Via` を含む条件を 「ref の 1 段目の先 (group) への購読」 + 「根への条件」 に分ける。
/// 全ての `Via` が同じ ref 紐から始まること。
pub(crate) fn split_grouped(preds: Vec<LivePred>) -> Result<(u16, Vec<LivePred>, Vec<LivePred>), String> {
    let mut via = None;
    let (mut inner, mut filter) = (Vec::new(), Vec::new());
    for p in preds {
        match p {
            LivePred::Via { path, pred } => {
                let Some((&first, rest)) = path.split_first() else {
                    return Err("Via with an empty path".into());
                };
                match via {
                    None => via = Some(first),
                    Some(v) if v != first => {
                        return Err(format!(
                            "grouped subscription needs every Via to start with the same ref himo ({v} vs {first})"
                        ));
                    }
                    _ => {}
                }
                inner.push(if rest.is_empty() { *pred } else { LivePred::Via { path: rest.to_vec(), pred } });
            }
            LivePred::Or(_) => return Err("grouped subscription does not support Or".into()),
            leaf => filter.push(leaf),
        }
    }
    match via {
        Some(v) => Ok((v, inner, filter)),
        None => Err("grouped subscription needs at least one Via condition".into()),
    }
}

/// 根への単一紐条件 (`Via` 以外) を 1 entity に当てる。
fn matches_leaf(r: &impl CellReader, p: &LivePred, e: u32) -> bool {
    match p {
        LivePred::Eq { himo_id, value } => r.cell(*himo_id, e) == Some(*value),
        LivePred::EqText { himo_id, text } => {
            r.vocab_lookup(text).is_some_and(|v| r.cell(*himo_id, e) == Some(v))
        }
        LivePred::Range { himo_id, lo, hi } => matches!(r.cell(*himo_id, e), Some(v) if *lo <= v && v <= *hi),
        LivePred::In { himo_id, values } => matches!(r.cell(*himo_id, e), Some(v) if values.contains(&v)),
        LivePred::Present { himo_id } => r.cell(*himo_id, e).is_some(),
        LivePred::Via { .. } | LivePred::Or(_) => false,
    }
}

/// ref をたどる条件の結果を **group (ref の 1 段目の先の entity) 単位** で持つ live query。
///
/// 例: 「所属会社の所在地が東京の社員」 を、 差分は 「東京になった会社 / 東京でなくなった会社」、
/// 社員は必要な時に会社から逆引きする形で持つ。 会社の所在地を 1 個書き換えた時の差分は会社
/// 1 件で、 配下の社員が何人いても書き込み + poll は O(1)。 社員の異動 (ref の付け替え) は
/// この購読の書き込みコストに乗らない (group の購読は ref の先の紐にしか印を付けない)。
///
/// - [`poll`](Self::poll) の差分は group の eid。 積分した集合 = 今条件を満たす group
/// - [`members`](Self::members) は group を指している根のうち、 根への条件 (`age > 30` など) を
///   満たすもの。 **いつ引いても今の中身** — 社員の異動や根の条件の変化は差分には出ず、
///   members / count を引いた時点の状態が返る
/// - 平らにした集合 (全 group の members の和) は、 同じ条件の [`LiveQuery`] の結果と同じ
pub struct GroupedLiveQuery {
    inner: LiveQuery,
    via: u16,
    filter: Vec<LivePred>,
}

impl GroupedLiveQuery {
    pub(crate) fn new(inner: LiveQuery, via: u16, filter: Vec<LivePred>) -> Self {
        GroupedLiveQuery { inner, via, filter }
    }

    /// 前回 poll からの group の差分 (初回は登録時点の全 group が `added`)。
    pub fn poll(&self, eng: &crate::engine::Engine) -> LiveDelta {
        self.inner.poll(eng)
    }

    /// 今条件を満たす group (eid 昇順)。 poll の状態は変えない。
    pub fn groups(&self, eng: &crate::engine::Engine) -> Vec<EntityId> {
        self.inner.members(eng)
    }

    /// `group` を指していて根への条件を満たす entity (eid 昇順)。 `group` が今条件を満たして
    /// いなければ空。
    pub fn members(&self, eng: &crate::engine::Engine, group: EntityId) -> Vec<EntityId> {
        if !self.inner.contains(eng, group) {
            return Vec::new();
        }
        self.members_of(eng, enchudb_oplog::eid_local(group))
    }

    fn members_of(&self, eng: &crate::engine::Engine, g: u32) -> Vec<EntityId> {
        let peer = self.inner.registry.peer.load(Ordering::Acquire);
        let mut out: Vec<u32> = CellReader::pull(eng, self.via, g);
        out.retain(|&e| self.filter.iter().all(|p| matches_leaf(eng, p, e)));
        out.sort_unstable();
        out.into_iter().map(|e| enchudb_oplog::make_eid(peer, e)).collect()
    }

    /// 平らにした結果の件数。 根への条件が無ければ group ごとの逆引きの件数の和 (group 数に比例)、
    /// あれば members を数える。
    pub fn count(&self, eng: &crate::engine::Engine) -> usize {
        let groups = self.inner.members(eng);
        if self.filter.is_empty() {
            groups.iter().map(|&g| CellReader::pull_len(eng, self.via, enchudb_oplog::eid_local(g))).sum()
        } else {
            groups.iter().map(|&g| self.members_of(eng, enchudb_oplog::eid_local(g)).len()).sum()
        }
    }

    /// 平らにした結果全体 (eid 昇順)。
    pub fn flatten(&self, eng: &crate::engine::Engine) -> Vec<EntityId> {
        let mut out: Vec<EntityId> = self
            .inner
            .members(eng)
            .into_iter()
            .flat_map(|g| self.members_of(eng, enchudb_oplog::eid_local(g)))
            .collect();
        out.sort_unstable();
        out
    }

    /// 未 poll の group の変化がありうるか。
    pub fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }

    /// engine 内で一意な購読 id (`Engine::poll_live` の差分の宛先。 差分は group の eid)。
    pub fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl std::fmt::Debug for GroupedLiveQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupedLiveQuery").field("inner", &self.inner).field("via", &self.via).finish()
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
        /// 並行書き込みの再現用: この cell の読みは列の値を順に返す (尽きたら通常の値)。
        script: Option<((u16, u32), std::collections::VecDeque<u32>)>,
    }

    impl CellReader for Mutex<Fake> {
        fn cell(&self, h: u16, e: u32) -> Option<u32> {
            let mut f = self.lock();
            if let Some(v) = f.script.as_mut().filter(|(k, _)| *k == (h, e)).and_then(|(_, seq)| seq.pop_front()) {
                return Some(v);
            }
            f.cells.get(&(h, e)).copied()
        }
        fn vocab_lookup(&self, t: &str) -> Option<u32> {
            self.lock().vocab.iter().position(|v| v == t).map(|i| i as u32)
        }
        fn pull(&self, h: u16, v: u32) -> Vec<u32> {
            self.lock().cells.iter().filter(|&(k, vv)| k.0 == h && *vv == v).map(|(k, _)| k.1).collect()
        }
        fn with_himo(&self, h: u16) -> Vec<u32> {
            self.lock().cells.keys().filter(|&&(hh, _)| hh == h).map(|&(_, e)| e).collect()
        }
        fn pull_len(&self, h: u16, v: u32) -> usize {
            self.pull(h, v).len()
        }
        fn pull_range(&self, h: u16, lo: u32, hi: u32) -> Vec<u32> {
            self.lock().cells.iter().filter(|&(k, v)| k.0 == h && lo <= *v && *v <= hi).map(|(k, _)| k.1).collect()
        }
    }

    fn write(reg: &LiveRegistry, f: &Mutex<Fake>, h: u16, e: u32, v: Option<u32>) {
        match v {
            Some(v) => f.lock().cells.insert((h, e), v),
            None => f.lock().cells.remove(&(h, e)),
        };
        reg.touch(h, e);
    }

    #[test]
    fn delta_consolidates_and_flags_reentry() {
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        let q = reg.register(vec![LivePred::Eq { himo_id: 0, value: 30 }], false);
        write(&reg, &f, 0, 1, Some(30));
        write(&reg, &f, 0, 2, Some(30));
        assert_eq!(q.poll_with(&f), LiveDelta { added: vec![1, 2], removed: vec![] });
        // 出て戻る: poll の間に評価されない往復は現在の状態だけを見る = 変化なし
        write(&reg, &f, 0, 1, Some(31));
        write(&reg, &f, 0, 1, Some(30));
        assert!(q.poll_with(&f).is_empty());
        // 出たのを評価 (count が settle する) してから戻る = removed + added
        write(&reg, &f, 0, 1, Some(31));
        assert_eq!(q.count_with(&f), 1);
        write(&reg, &f, 0, 1, Some(30));
        assert_eq!(q.poll_with(&f), LiveDelta { added: vec![1], removed: vec![1] });
        // 入って出る (未報告のまま) = 打ち消し → 何も出ない
        write(&reg, &f, 0, 3, Some(30));
        write(&reg, &f, 0, 3, None);
        assert!(q.poll_with(&f).is_empty());
        assert_eq!(q.count_with(&f), 2);
    }

    /// 解放された報告済みの根が、 poll の間に同じ eid で条件を満たし直したら removed + added。
    #[test]
    fn freed_root_reported_as_leave_and_reenter() {
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        let q = reg.register(vec![LivePred::Eq { himo_id: 0, value: 30 }], false);
        write(&reg, &f, 0, 1, Some(30));
        assert_eq!(q.poll_with(&f).added, vec![1]);
        write(&reg, &f, 0, 1, None);
        reg.freed(1);
        write(&reg, &f, 0, 1, Some(30));
        assert_eq!(q.poll_with(&f), LiveDelta { added: vec![1], removed: vec![1] });
    }

    #[test]
    fn eq_text_matches_after_vocab_appears() {
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        let q = reg.register(vec![LivePred::EqText { himo_id: 4, text: "東京".into() }], false);
        f.lock().vocab.push("大阪".into());
        write(&reg, &f, 4, 9, Some(0));
        assert!(q.poll_with(&f).is_empty());
        f.lock().vocab.push("東京".into());
        write(&reg, &f, 4, 9, Some(1));
        assert_eq!(q.poll_with(&f).added, vec![9]);
    }

    /// user(紐 0 = company ref) → company(紐 1 = city)。 会社の city を書くと配下が出入りし、
    /// 真偽の変わらない書き換えでは展開しない。
    #[test]
    fn via_expands_only_when_truth_flips() {
        const COMPANY: u16 = 0;
        const CITY: u16 = 1;
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        // 会社 100 (city=1) に user 1..=3、 会社 200 (city=2) に user 4
        write(&reg, &f, CITY, 100, Some(1));
        write(&reg, &f, CITY, 200, Some(2));
        for u in 1..=3 {
            write(&reg, &f, COMPANY, u, Some(100));
        }
        write(&reg, &f, COMPANY, 4, Some(200));
        let q = reg.register(vec![LivePred::Via {
            path: vec![COMPANY],
            pred: Box::new(LivePred::Eq { himo_id: CITY, value: 1 }),
        }], false);
        q.seed(&f);
        assert_eq!(q.poll_with(&f).added, vec![1, 2, 3]);
        // 会社 100 が移転 → 3 人とも出る
        write(&reg, &f, CITY, 100, Some(3));
        assert_eq!(q.poll_with(&f).removed, vec![1, 2, 3]);
        // 会社 200 が city 2 → 5 (どちらも偽): 記録値が確定したので展開しない
        write(&reg, &f, CITY, 200, Some(5));
        q.poll_with(&f);
        write(&reg, &f, CITY, 200, Some(6));
        q.poll_with(&f);
        // user 4 を会社 100 に付け替え、 会社 100 が戻る → 4 人入る
        write(&reg, &f, COMPANY, 4, Some(100));
        write(&reg, &f, CITY, 100, Some(1));
        assert_eq!(q.poll_with(&f).added, vec![1, 2, 3, 4]);
        assert_eq!(q.count_with(&f), 4);
    }

    /// 親は子の記録を使う (Column を読み直さない)。 記録の後に子が書き換わっても、 その書き込みの
    /// 印で次の settle が子を評価し直し、 鍵が変わっていれば親へ展開する。
    ///
    /// 決定論再現: poll k の中で hub (会社 100) を評価して記録 = 真にした **後** に並行書き込みで
    /// city が偽の値になる。 同じ poll の根 (user 1) は記録 (真) を使うので集合に残る — その書き込み
    /// の印を消費する poll k+1 で hub が偽になり、 user 1 が出る。
    #[test]
    fn parent_uses_record_and_later_change_expands() {
        const COMPANY: u16 = 0;
        const CITY: u16 = 1;
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        write(&reg, &f, CITY, 100, Some(1));
        write(&reg, &f, COMPANY, 1, Some(100));
        let q = reg.register(vec![LivePred::Via {
            path: vec![COMPANY],
            pred: Box::new(LivePred::Eq { himo_id: CITY, value: 1 }),
        }], false);
        q.seed(&f);
        assert_eq!(q.poll_with(&f).added, vec![1]);
        // hub の記録を確定させる
        write(&reg, &f, CITY, 100, Some(1));
        assert!(q.poll_with(&f).is_empty());

        // poll k: hub と user 1 の両方に印。 hub の評価は 1 (真) を読む。 2 番目の読み (= 並行
        // 書き込み後の値) は、 根が記録を使うので poll k では読まれない
        reg.touch(CITY, 100);
        reg.touch(COMPANY, 1);
        f.lock().script = Some(((CITY, 100), [1, 2].into()));
        assert!(q.poll_with(&f).is_empty(), "根は hub の記録 (真) を使う");
        assert_eq!(f.lock().script.as_ref().map(|(_, v)| v.len()), Some(1), "根が hub の Column を読み直した");
        // 並行書き込みの印が届く → hub が偽 → 配下が出る
        reg.touch(CITY, 100);
        assert_eq!(q.poll_with(&f).removed, vec![1]);
        // 真に戻る
        f.lock().script = None;
        reg.touch(CITY, 100);
        assert_eq!(q.poll_with(&f).added, vec![1]);
        assert_eq!(q.count_with(&f), 1);
    }

    /// 同じ形の購読は 1 本の family に束ねられ、 値ごとに振り分けられる。 購読の無い値は鍵の表に
    /// 無い = hub の記録は 「偽」。 その値を後から購読すると、 記録した 「偽」 は今は真 — 記録を
    /// 直さないと、 hub が別の (購読の無い) 値に移った時に 「偽 → 偽」 で展開されず、 配下が
    /// 後から加わった購読に残り続ける。
    #[test]
    fn late_member_sees_hub_recorded_while_unsubscribed() {
        const COMPANY: u16 = 0;
        const CITY: u16 = 1;
        let via = |v| vec![LivePred::Via { path: vec![COMPANY], pred: Box::new(LivePred::Eq { himo_id: CITY, value: v }) }];
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        write(&reg, &f, COMPANY, 1, Some(100));
        let a = reg.register(via(1), false);
        a.seed(&f);
        assert!(a.poll_with(&f).is_empty());
        // 購読の無い値 5 に移転 → hub を評価して 「偽」 を記録
        write(&reg, &f, CITY, 100, Some(5));
        assert!(a.poll_with(&f).is_empty());
        assert_eq!(reg.families().len(), 1);

        let b = reg.register(via(5), false);
        assert_eq!(reg.families().len(), 1, "同じ形は同じ family");
        assert_eq!(b.poll_with(&f).added, vec![1]);
        // 購読の無い値 7 へ: b から出る
        write(&reg, &f, CITY, 100, Some(7));
        assert_eq!(b.poll_with(&f).removed, vec![1], "hub の古い 「偽」 の記録で展開が止まった");
        assert!(a.poll_with(&f).is_empty());
        // 同じ鍵の重複購読は、 既に集合に居る根を初回 poll で受け取る
        write(&reg, &f, CITY, 100, Some(1));
        assert_eq!(a.poll_with(&f).added, vec![1]);
        let a2 = reg.register(via(1), false);
        assert_eq!(a2.poll_with(&f).added, vec![1]);
        assert_eq!(a2.count_with(&f), 1);
        drop((a, b, a2));
        assert_eq!(reg.active.load(Ordering::Acquire), 0);
        assert!(reg.families().is_empty());
    }

    /// 購読が外れた値は鍵の表からも外れる (購読の無い値どうしの書き換えは展開しない状態に戻る)。
    /// 同じ値をまた購読すると新しい id になるが、 古い id の記録 (hub の記録・根の現在の鍵) が
    /// 残っていても新しい購読に正しく届く。
    #[test]
    fn unsubscribed_key_leaves_table_and_resubscribe_works() {
        const COMPANY: u16 = 0;
        const CITY: u16 = 1;
        let via = |v| vec![LivePred::Via { path: vec![COMPANY], pred: Box::new(LivePred::Eq { himo_id: CITY, value: v }) }];
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        write(&reg, &f, CITY, 100, Some(5));
        write(&reg, &f, COMPANY, 1, Some(100));
        let a = reg.register(via(1), false);
        let b = reg.register(via(5), false);
        assert_eq!(b.poll_with(&f).added, vec![1]);
        // hub の記録を確定させる (記録 = 鍵 (5) の id)
        write(&reg, &f, CITY, 100, Some(5));
        assert!(b.poll_with(&f).is_empty());
        let fam = reg.families()[0].clone();
        assert_eq!(fam.settled.lock().tables[1].len(), 2, "前提: 会社の節に (1) と (5)");
        drop(b);
        assert_eq!(fam.settled.lock().tables[1].len(), 1, "外れた値が表に残っている");
        assert_eq!(fam.settled.lock().tables[0].len(), 1);

        // 書き込み無しで同じ値をまた購読: 根の現在の鍵も hub の記録も古い id のまま
        let c = reg.register(via(5), false);
        assert_eq!(c.poll_with(&f).added, vec![1], "古い id を持つ根が新しい購読に届かない");
        write(&reg, &f, CITY, 100, Some(6));
        assert_eq!(c.poll_with(&f).removed, vec![1], "古い id の hub の記録で展開が止まった");
        assert!(a.poll_with(&f).is_empty());
        write(&reg, &f, CITY, 100, Some(5));
        assert_eq!(c.poll_with(&f).added, vec![1]);
        assert_eq!(c.count_with(&f), 1);
    }

    /// 範囲の違う購読は 1 本の family に束ねられる。 範囲が 2 本ある形は 1 本目だけが穴で、
    /// 2 本目の範囲が違えば別の family。
    #[test]
    fn ranges_share_one_family() {
        let reg = Arc::new(LiveRegistry::new(0));
        let age = |lo, hi| LivePred::Range { himo_id: 0, lo, hi };
        let qs: Vec<LiveQuery> = (0..50).map(|k| reg.register(vec![age(k, k + 10)], false)).collect();
        assert_eq!(reg.families().len(), 1, "範囲の違う購読が別 family になった");
        let two = |k| vec![age(k, k + 5), LivePred::Range { himo_id: 1, lo: 0, hi: k }];
        let a = reg.register(two(1), false);
        let b = reg.register(two(1), false);
        assert_eq!(reg.families().len(), 2, "2 本目の範囲が同じ = 同じ family");
        let c = reg.register(two(2), false);
        assert_eq!(reg.families().len(), 3, "2 本目の範囲が違えば別 family");
        drop((qs, a, b, c));
        assert!(reg.families().is_empty());
    }

    /// ref の先の範囲: 道の上の節は 「帯が変わった時だけ」 範囲の値を親へ運ぶので、 根の記録の値は
    /// 子の今の値と同じ帯のどこか。 購読が増えて帯が割れると両者が別の帯に分かれうる — 割れた帯の
    /// entity を評価し直さないと、 根の古い値で新しい購読の集合が決まる。
    ///
    /// 決定論再現: 売上 75 → 65 ([0, 100] の帯の中なので根は 75 のまま) → [70, 100] を購読
    /// (帯が 70 で割れる。 新しい購読の候補 = 売上 70〜100 の会社には入らないので、 候補の評価
    /// でも直らない)。
    #[test]
    fn range_split_refreshes_values_carried_up_within_a_slab() {
        const COMPANY: u16 = 0;
        const REV: u16 = 1;
        let via = |lo, hi| vec![LivePred::Via { path: vec![COMPANY], pred: Box::new(LivePred::Range { himo_id: REV, lo, hi }) }];
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        write(&reg, &f, REV, 100, Some(75));
        write(&reg, &f, COMPANY, 1, Some(100));
        let a = reg.register(via(0, 100), false);
        assert_eq!(a.poll_with(&f).added, vec![1]);
        write(&reg, &f, REV, 100, Some(65));
        assert!(a.poll_with(&f).is_empty());
        let b = reg.register(via(70, 100), false);
        assert!(b.poll_with(&f).is_empty(), "根の古い値 (75) で新しい購読に入った");
        assert_eq!(b.count_with(&f), 0, "根の古い値 (75) で数えた");
        write(&reg, &f, REV, 100, Some(90));
        assert_eq!(b.poll_with(&f).added, vec![1]);
        assert!(a.poll_with(&f).is_empty());
        write(&reg, &f, REV, 100, Some(69));
        assert_eq!(b.poll_with(&f).removed, vec![1]);
        assert!(a.poll_with(&f).is_empty());
        assert_eq!((a.count_with(&f), b.count_with(&f)), (1, 0));
    }

    #[test]
    fn dnf_distributes_via_and_multiplies() {
        let eq = |h, v| LivePred::Eq { himo_id: h, value: v };
        let or = |a, b| LivePred::Or(vec![vec![a], vec![b]]);
        // Via(1, a OR b) AND (c OR d) = 4 枝、 Via は枝の中に配られる
        let b = dnf(vec![LivePred::Via { path: vec![1], pred: Box::new(or(eq(2, 0), eq(2, 1))) }, or(eq(3, 0), eq(3, 1))]).unwrap();
        assert_eq!(b.len(), 4);
        assert!(b.iter().all(|c| c.len() == 2 && matches!(&c[0], LivePred::Via { path, pred } if path == &vec![1] && matches!(**pred, LivePred::Eq { himo_id: 2, .. }))));
        // 7 個の OR (2 択) の AND = 128 枝 > 上限
        assert!(dnf((0..7).map(|h| or(eq(h, 0), eq(h, 1))).collect()).is_err());
        assert!(dnf((0..6).map(|h| or(eq(h, 0), eq(h, 1))).collect()).is_ok());
        assert!(dnf(vec![LivePred::Or(vec![])]).is_err());
        assert!(dnf(vec![LivePred::Or(vec![vec![]])]).is_err());
    }

    /// `Or`: 両方の枝に居る entity は片方から出ても残り、 両方から出たら出る。 差分は枝の id でなく
    /// `Or` の購読の id で届き、 `count` が先に枝の差分を積んでも `poll_all` に届く。
    #[test]
    fn or_counts_rows_held_by_several_branches() {
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        let q = reg.register_any(
            vec![vec![LivePred::Eq { himo_id: 0, value: 1 }], vec![LivePred::Eq { himo_id: 1, value: 1 }]],
            false,
        );
        write(&reg, &f, 0, 7, Some(1));
        write(&reg, &f, 1, 7, Some(1));
        assert_eq!(q.poll_with(&f), LiveDelta { added: vec![7], removed: vec![] });
        write(&reg, &f, 0, 7, Some(2));
        assert!(q.poll_with(&f).is_empty(), "もう片方の枝に居るのに出た");
        assert_eq!(q.count_with(&f), 1);
        write(&reg, &f, 1, 7, None);
        // count が枝の差分を積む → poll_all は Or の購読の id で返す (枝の id は出さない)
        assert_eq!(q.count_with(&f), 0);
        assert_eq!(reg.poll_all(&f), vec![(q.id(), LiveDelta { added: vec![], removed: vec![7] })]);
        write(&reg, &f, 1, 8, Some(1));
        assert_eq!(reg.poll_all(&f), vec![(q.id(), LiveDelta { added: vec![8], removed: vec![] })]);
        assert!(q.poll_with(&f).is_empty());
    }

    /// `Or`: 報告済みの entity の slot が解放されて同じ eid で入り直したら removed + added
    /// (枝がそう報告するのを、 枝の数が 1 のまま = 0 をまたがなくても伝える)。
    #[test]
    fn or_reports_reentry_of_freed_slot() {
        let reg = Arc::new(LiveRegistry::new(0));
        let f = Mutex::new(Fake::default());
        let q = reg.register_any(
            vec![vec![LivePred::Eq { himo_id: 0, value: 1 }], vec![LivePred::Eq { himo_id: 1, value: 1 }]],
            false,
        );
        write(&reg, &f, 0, 3, Some(1));
        assert_eq!(q.poll_with(&f).added, vec![3]);
        write(&reg, &f, 0, 3, None);
        reg.freed(3);
        write(&reg, &f, 0, 3, Some(1));
        assert_eq!(q.poll_with(&f), LiveDelta { added: vec![3], removed: vec![3] });
        drop(q);
        assert!(reg.families().is_empty());
    }

    /// `Bits` (roaring の疎 ↔ bitmap chunk、 roaring ↔ 平らな bitset の切り替え込み) が
    /// BTreeSet と同じ集合を持つ。
    #[test]
    fn bits_matches_btreeset_across_mode_switches() {
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut modes = std::collections::BTreeSet::new();
        // (範囲, 付けるか)。 A: 広い範囲に疎 → 1 chunk に密 (bitmap chunk) → 落とす。
        // B: 狭い範囲に密 (平ら) → 落として疎に戻る
        let scenarios: [&[(u32, bool)]; 2] = [
            &[(u32::MAX, true), (9_000, true), (9_000, false)],
            &[(200_000, true), (200_000, true), (200_000, false), (200_000, false), (200_000, false)],
        ];
        for (si, rounds) in scenarios.iter().enumerate() {
            let mut b = Bits::default();
            let mut o = std::collections::BTreeSet::new();
            for (round, &(span, on)) in rounds.iter().enumerate() {
                for _ in 0..40_000 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let i = if span == 9_000 { 70_000 + (x >> 8) as u32 % span } else { (x >> 8) as u32 % span };
                    b.put(i, on);
                    if on {
                        o.insert(i);
                    } else {
                        o.remove(&i);
                    }
                }
                modes.insert(match &b {
                    Bits::Sparse(r, _) if r.0.iter().any(|(_, c)| matches!(c, Chunk::Dense(..))) => "sparse+dense-chunk",
                    Bits::Sparse(..) => "sparse",
                    Bits::Flat(..) => "flat",
                });
                let at = format!("scenario {si} round {round}");
                assert!(o.iter().all(|&i| b.get(i)), "{at}");
                assert_eq!(b.iter().collect::<Vec<_>>(), o.iter().copied().collect::<Vec<_>>(), "{at}");
                let n = match &b {
                    Bits::Sparse(r, n) => {
                        assert!(r.0.iter().all(|(_, c)| !matches!(c, Chunk::Sparse(v) if v.is_empty())), "{at}: 空 chunk");
                        *n
                    }
                    Bits::Flat(_, n) => *n,
                };
                assert_eq!(n, o.len(), "{at}: 要素数");
            }
            if si == 1 {
                // 100 個まで落とすと疎に戻る
                assert!(matches!(b, Bits::Flat(..)), "前提: 平ら");
                let drop: Vec<u32> = o.iter().copied().skip(100).collect();
                for i in drop {
                    b.put(i, false);
                    o.remove(&i);
                }
                assert!(matches!(b, Bits::Sparse(_, 100)), "疎に戻っていない");
                assert_eq!(b.iter().collect::<Vec<_>>(), o.iter().copied().collect::<Vec<_>>());
            }
        }
        assert_eq!(modes.len(), 3, "全ての形を通っていない: {modes:?}");
    }

    #[test]
    fn drop_unregisters() {
        let reg = Arc::new(LiveRegistry::new(0));
        let q = reg.register(vec![LivePred::Via {
            path: vec![1, 2],
            pred: Box::new(LivePred::Present { himo_id: 3 }),
        }], false);
        assert_eq!(reg.active.load(Ordering::Acquire), 1);
        drop(q);
        assert_eq!(reg.active.load(Ordering::Acquire), 0);
        assert!(reg.with_snap(|s| s.routes.iter().all(Vec::is_empty)).unwrap_or(true));
    }
}
