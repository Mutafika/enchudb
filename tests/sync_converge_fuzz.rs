//! sync の収束の総当たり: 書き手 3 台が乱数で並べた手順 (作成 / 改名 / 削除 / 書き換え / 相手から pull) を、 受け手が書き手の
//! 全部の pull の順で受け、 書き手どうしも pull し合った後に、 答えが全部同じか (peer ごとに割れないか) を見る。 割れたら proptest が
//! 手順を縮めて最小の形を出す。
//!
//! 名前の列 (`key`) で束ねる表 (#141)。 改名 (#569)・削除優先 (#577)・消した名前 (#582 / #588) の組み合わせを、 人が形を思い付く代わりに
//! 機械に探させる (#547 / #551 / #545 / #581 は全部人が見つけた)。 答えには名前の無い塊 (title だけの entity) も入れる。
//!
//! 普段の試験 (CI) は乱数の種を固定して 32 回回す (毎回同じ手順。 揺れで落ちない)。 新しい形を探す時は `ENCHU_FUZZ_CASES` で回数を
//! 渡すと、 毎回違う種で回す (`ENCHU_FUZZ_SEED` で種を決めて繰り返せる)。 改名を出さずに回すなら `ENCHU_FUZZ_NO_RENAME=1`:
//!
//! ```text
//! ENCHU_FUZZ_CASES=1000 cargo test --release --test sync_converge_fuzz every_order -- --nocapture
//! ```
//!
//! #612 (自分の行どうしを畳む) が入って、 改名を混ぜて 1,000 回ずつ回しても割れなくなった (2026-10-22)。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;
use proptest::prelude::*;

const T: &str = "t";
const OPLOG: usize = 1 << 20;
const AUTHORS: usize = 3;
const RECEIVER_BASE: u32 = 10;
const KEYS: u8 = 3;
const TITLES: u8 = 3;

static RUN: AtomicU64 = AtomicU64::new(0);

/// 1 回の試しの directory。 Drop で消す。
struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        let n = RUN.fetch_add(1, Ordering::Relaxed);
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("converge-fuzz-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }

    fn db(&self, id: u32) -> String {
        self.0.join(format!("p{id}")).to_str().unwrap().to_string()
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Peer {
    db: Arc<Database>,
    sync: Syncer,
}

fn make(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>) -> Peer {
    let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
    b.table(T).tag("key").tag("title").primary_key("key").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    Peer { db, sync }
}

fn key(k: u8) -> String {
    format!("k{k}")
}

fn title(t: u8) -> String {
    format!("t{t}")
}

impl Peer {
    fn publish(&self) {
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        eng.transfer_oplog_to_sync_ops();
        self.sync.publish_since(Hlc::ZERO);
        // HLC の順を作る (同じ process の時計なので、 間を空ければ後に書いた方が新しい)
        std::thread::sleep(Duration::from_millis(2));
    }

    fn row(&self, k: u8) -> Option<u64> {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().where_eq("key", key(k).as_str()).find().unwrap().first().copied()
    }

    /// 手順を 1 つ当てる。 当てられない手順 (無い行の改名 / 削除 / 書き換え、 改名の先が既にある) は何もしない。
    fn step(&self, s: &Step, peers: &[Peer]) {
        let t = self.db.get_table(T).unwrap();
        match *s {
            Step::Upsert { key: k, title: v, .. } => {
                t.upsert().set("key", key(k).as_str()).set("title", title(v).as_str()).commit().unwrap();
            }
            Step::Rename { from, to, .. } => {
                let (Some(e), None) = (self.row(from), self.row(to)) else { return };
                if t.entity(e).set("key", key(to).as_str()).commit().is_err() {
                    return;
                }
            }
            Step::Delete { key: k, .. } => {
                let Some(e) = self.row(k) else { return };
                t.entity(e).delete().unwrap();
            }
            Step::Retitle { key: k, title: v, .. } => {
                let Some(e) = self.row(k) else { return };
                t.entity(e).set("title", title(v).as_str()).commit().unwrap();
            }
            Step::Pull { from, .. } => {
                self.sync.pull_once(peers[from].db.engine().peer_id());
                return;
            }
        }
        self.publish();
    }

    /// engine の不変量の違反 (`verify_invariants`、 書き手を止めてから)。 主キーの重複は警告 (#601 が入るまで)。 #560: 書き出した
    /// `eidmap` + `eidmap.log` を読み直した中身が memory の中身と同じか (変えたのに log に載せない所を拾う)。
    fn violations(&self) -> Vec<String> {
        let eng = self.db.engine();
        let mut out = eng.verify_invariants().violations;
        eng.persist_tables().unwrap();
        if let Some((disk, mem)) = eng.eidmap_disk_diff().unwrap() {
            out.push(format!("eidmap: disk にだけ {disk:?} / memory にだけ {mem:?}"));
        }
        out
    }

    /// 表の行 (key, title)。 名前の無い塊 (title だけの entity) も入れる。
    fn rows(&self) -> Vec<(Option<Value>, Option<Value>)> {
        self.db.engine().rebuild();
        let t = self.db.get_table(T).unwrap();
        let mut eids = t.all().find().unwrap();
        eids.extend(t.all().where_ne("title", "\u{0}").find().unwrap());
        eids.sort_unstable();
        eids.dedup();
        let mut out: Vec<_> = eids
            .into_iter()
            .map(|e| {
                let r = t.entity(e);
                (r.get("key"), r.get("title"))
            })
            .collect();
        out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        out
    }
}

#[derive(Debug, Clone)]
enum Step {
    Upsert { by: usize, key: u8, title: u8 },
    Rename { by: usize, from: u8, to: u8 },
    Delete { by: usize, key: u8 },
    Retitle { by: usize, key: u8, title: u8 },
    Pull { by: usize, from: usize },
}

impl Step {
    fn by(&self) -> usize {
        match *self {
            Step::Upsert { by, .. }
            | Step::Rename { by, .. }
            | Step::Delete { by, .. }
            | Step::Retitle { by, .. }
            | Step::Pull { by, .. } => by,
        }
    }
}

/// `ENCHU_FUZZ_NO_RENAME=1` で改名を出さない (改名の割れ以外を探す時)。
fn rename_weight() -> u32 {
    if std::env::var("ENCHU_FUZZ_NO_RENAME").is_ok_and(|v| v == "1") { 0 } else { 2 }
}

fn step_strategy() -> impl Strategy<Value = Step> {
    let by = 0..AUTHORS;
    prop_oneof![
        3 => (by.clone(), 0..KEYS, 0..TITLES).prop_map(|(by, key, title)| Step::Upsert { by, key, title }),
        rename_weight() => (by.clone(), 0..KEYS, 0..KEYS).prop_filter("改名の先は別の名前", |(_, f, t)| f != t)
            .prop_map(|(by, from, to)| Step::Rename { by, from, to }),
        2 => (by.clone(), 0..KEYS).prop_map(|(by, key)| Step::Delete { by, key }),
        1 => (by.clone(), 0..KEYS, 0..TITLES).prop_map(|(by, key, title)| Step::Retitle { by, key, title }),
        3 => (by, 0..AUTHORS).prop_filter("自分からは引かない", |(b, f)| b != f)
            .prop_map(|(by, from)| Step::Pull { by, from }),
    ]
}

fn permutations(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![vec![]];
    }
    let mut out = Vec::new();
    for p in permutations(n - 1) {
        for i in 0..=p.len() {
            let mut q = p.clone();
            q.insert(i, n - 1);
            out.push(q);
        }
    }
    out
}

type Answer = Vec<(Option<Value>, Option<Value>)>;

/// 手順を当てて、 全部の受け手と書き手の答え。 全部同じなら Ok。
fn run(steps: &[Step]) -> Result<Answer, String> {
    let dir = Dir::new();
    let orders = permutations(AUTHORS);
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=AUTHORS as u32 {
        tr.register_peer(id);
    }
    for k in 0..orders.len() as u32 {
        tr.register_peer(RECEIVER_BASE + k);
    }
    let authors: Vec<Peer> = (1..=AUTHORS as u32).map(|id| make(&dir, id, &tr)).collect();
    for s in steps {
        authors[s.by()].step(s, &authors);
    }
    let mut answers: Vec<(String, Answer)> = Vec::new();
    let mut broken: Vec<String> = Vec::new();
    for (k, order) in orders.iter().enumerate() {
        let r = make(&dir, RECEIVER_BASE + k as u32, &tr);
        for &from in order {
            r.sync.pull_once(from as u32 + 1);
        }
        let who = format!("受け手 {:?}", order.iter().map(|a| a + 1).collect::<Vec<_>>());
        broken.extend(r.violations().into_iter().map(|v| format!("  {who}: {v}")));
        answers.push((who, r.rows()));
    }
    for _ in 0..2 {
        for (i, a) in authors.iter().enumerate() {
            for from in 0..AUTHORS {
                if from != i {
                    a.sync.pull_once(from as u32 + 1);
                }
            }
        }
    }
    for (i, a) in authors.iter().enumerate() {
        broken.extend(a.violations().into_iter().map(|v| format!("  書き手 {}: {v}", i + 1)));
        answers.push((format!("書き手 {}", i + 1), a.rows()));
    }
    drop(authors);
    if !broken.is_empty() {
        return Err(format!("engine の不変量の違反\n{}", broken.join("\n")));
    }
    if answers.iter().all(|(_, x)| *x == answers[0].1) {
        return Ok(answers.swap_remove(0).1);
    }
    let mut msg = String::from("届く順で答えが割れる\n");
    for (who, rows) in &answers {
        msg.push_str(&format!("  {who}: {rows:?}\n"));
    }
    Err(msg)
}

fn cases() -> u32 {
    std::env::var("ENCHU_FUZZ_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(32)
}

/// 種: `ENCHU_FUZZ_SEED` があればそれ、 回数を渡した (手で探す) なら毎回違う種、 どちらも無ければ (CI) 固定。
fn seed() -> proptest::test_runner::RngSeed {
    use proptest::test_runner::RngSeed;
    match std::env::var("ENCHU_FUZZ_SEED").ok().and_then(|v| v.parse().ok()) {
        Some(n) => RngSeed::Fixed(n),
        None if std::env::var("ENCHU_FUZZ_CASES").is_ok() => RngSeed::Random,
        None => RngSeed::Fixed(612),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), rng_seed: seed(), max_shrink_iters: 2000, ..ProptestConfig::default() })]

    #[test]
    fn every_order_converges(steps in proptest::collection::vec(step_strategy(), 1..10)) {
        if let Err(msg) = run(&steps) {
            prop_assert!(false, "手順 {steps:?}\n{msg}");
        }
    }
}

fn text(s: &str) -> Option<Value> {
    Some(Value::Text(s.into()))
}

/// 縮めた形 (#581): B が消した名前で、 A が (削除を知らずに) 削除の後に作る。 A の行を削除より先に受けて束ねた peer でも、
/// 後から届いた削除は A の行を消さない (A の行は削除の後に作られた = 新しいデッキ)。
#[test]
fn a_row_made_after_the_delete_survives_in_every_order() {
    let steps = [
        Step::Upsert { by: 0, key: 0, title: 0 },
        Step::Pull { by: 1, from: 0 },
        Step::Delete { by: 1, key: 0 },
        Step::Upsert { by: 2, key: 0, title: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t1"))]));
}

/// 縮めた形 (総当たりの 2 つ目、 改名なし): 上の形に、 削除の前に別の peer (C) が B の行を受けておく (中継の重ね)。
#[test]
fn a_row_made_after_a_relayed_delete_survives_in_every_order() {
    let steps = [
        Step::Upsert { by: 1, key: 2, title: 0 },
        Step::Pull { by: 2, from: 1 },
        Step::Delete { by: 1, key: 2 },
        Step::Upsert { by: 0, key: 2, title: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k2"), text("t0"))]));
}

/// 縮めた形 (総当たりの 3 つ目、 改名なし): B と A が別々に同じ名前で作り、 A が自分の行を消し、 B が (削除を知らずに) その名前で
/// upsert し直す。 B の行は削除より前に作られた同じデッキなので消える (削除優先、 D5)。 upsert が名前のカードを書き直すと、 B の
/// 行の作成の時刻が分からなくなり、 束ねた peer だけ B の行を残した。
#[test]
fn an_upsert_after_an_unseen_delete_does_not_revive_the_row() {
    let steps = [
        Step::Upsert { by: 1, key: 2, title: 0 },
        Step::Upsert { by: 0, key: 2, title: 0 },
        Step::Delete { by: 0, key: 2 },
        Step::Upsert { by: 1, key: 2, title: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![]));
}

/// 縮めた形 (総当たりの 4 つ目): A の行の作成より先に、 B の (A の行宛ての) 書き換えが別の配りで届く。 受け手は名前の無い slot を
/// 払い出し、 後から届いた名前のカードは写像のある slot に当たる。 その名前は (C の同じ名前の行の) 削除で消えていて、 A の行は
/// 削除より前に作られたので、 同じデッキとして消える。
#[test]
fn a_name_that_arrives_after_its_row_still_meets_the_delete() {
    let steps = [
        Step::Upsert { by: 0, key: 1, title: 0 },
        Step::Pull { by: 1, from: 0 },
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Retitle { by: 1, key: 1, title: 0 },
        Step::Delete { by: 2, key: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![]));
}

/// 縮めた形 (改名の時刻 1): A が改名した後に、 前の名前で作り直す。 作り直した行は改名の後に作られた = 新しいデッキ。 改名を先に
/// 受けた受け手も、 前の名前で改名した行に束ねない (前は束ねて、 A だけ 2 行になった)。
#[test]
fn a_row_made_with_the_former_name_after_the_rename_is_a_new_deck() {
    let steps = [
        Step::Upsert { by: 0, key: 1, title: 0 },
        Step::Rename { by: 0, from: 1, to: 0 },
        Step::Upsert { by: 0, key: 1, title: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t0")), (text("k1"), text("t1"))]));
}

/// 縮めた形 (改名の時刻 2): B が改名の前に同じ名前で作った行は、 改名した行と同じデッキ。 改名の後に A が前の名前で作り直した行
/// (新しいデッキ) を先に受けていても、 B の行はそちらに束ねない (今の名前より、 その時刻にその名前だった行を先に引く)。
#[test]
fn a_row_made_with_the_former_name_before_the_rename_joins_the_renamed_deck() {
    let steps = [
        Step::Upsert { by: 0, key: 1, title: 0 },
        Step::Upsert { by: 1, key: 1, title: 1 },
        Step::Rename { by: 0, from: 1, to: 0 },
        Step::Upsert { by: 0, key: 1, title: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t1")), (text("k1"), text("t2"))]));
}

/// 縮めた形 (改名の時刻 3): C が改名の後に前の名前で作った行を先に受け、 改名した行 (改名の前) をそれに今の名前で束ねた受け手に、
/// 後から改名が届く。 改名より後に作られた C の行を切り離す (それぞれの title も戻す: 束ねた行の title は C の方が新しかった)。
#[test]
fn a_row_made_after_the_rename_splits_off_when_the_rename_arrives_late() {
    let steps = [
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Rename { by: 1, from: 1, to: 2 },
        Step::Upsert { by: 2, key: 1, title: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k1"), text("t1")), (text("k2"), text("t0"))]));
}

/// 改名の時刻 4: 切り離す時、 束ねていた間の自分の書き込みは、 その書き込みの宛名の identity の側に残る。 A は B の k1 を受けた
/// 後に (改名を知らずに) C の k1 (改名の後に作られた) を受けて束ね、 束ねた行の title を書く。 束ねた行への書き込みの宛名は行の元の
/// identity (B の行、 #613: 束ねても宛名は替えない) なので、 他の peer でも B の行に当たる。 その後で B の改名が届くと、 A の title は
/// B の行 (k2) に、 C の行は C の title に戻る (束ねた行の cell は混ざっているので、 identity ごとのカードから戻す)。
#[test]
fn a_split_keeps_the_writes_made_while_bound() {
    let steps = [
        Step::Upsert { by: 1, key: 1, title: 1 },
        Step::Pull { by: 0, from: 1 },
        Step::Rename { by: 1, from: 1, to: 2 },
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Pull { by: 0, from: 2 },
        Step::Retitle { by: 0, key: 1, title: 2 },
        Step::Pull { by: 0, from: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k1"), text("t0")), (text("k2"), text("t2"))]));
}

/// 改名の時刻 4 の続き: 切り離した後も、 行の slot の持ち主 (C の行) は自分の slot に残る。 C は改名の後に k1 を作り、 B の k1 と
/// 改名を受けて切り離した後に、 自分の k1 の title を書く (自分の行の番号で書くので、 他の peer でも C の行に当たる)。
#[test]
fn the_slot_owner_stays_in_its_slot_after_a_split() {
    let steps = [
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Rename { by: 1, from: 1, to: 2 },
        Step::Upsert { by: 2, key: 1, title: 1 },
        Step::Pull { by: 2, from: 1 },
        Step::Retitle { by: 2, key: 1, title: 3 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k1"), text("t3")), (text("k2"), text("t0"))]));
}

/// 改名と削除 (今の形、 前の名前の行も消える): A が k0 を k1 に改名し、 改名の後に C が作った k0 (新しいデッキ) を受けてから、
/// 自分で k1 を消す。 消す前に作られた C の k0 も一緒に消える (削除を先に受けた peer は、 改名を知らずに k0 の行を消している)。
#[test]
fn deleting_a_renamed_row_also_deletes_rows_holding_its_former_name() {
    let steps = [
        Step::Upsert { by: 0, key: 0, title: 0 },
        Step::Rename { by: 0, from: 0, to: 1 },
        Step::Upsert { by: 2, key: 0, title: 1 },
        Step::Pull { by: 0, from: 2 },
        Step::Delete { by: 0, key: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![]));
}

/// 縮めた形 (改名の時刻 5): A が k2 を k0 に改名し、 B の k0 と 1 つに畳む。 畳んだ行の前の名前 k2 も残す行に移す (前は slot と
/// 一緒に捨て、 改名の前に作られた C の k2 が別の行になった)。
#[test]
fn a_row_folded_by_a_rename_keeps_its_former_names() {
    let steps = [
        Step::Upsert { by: 1, key: 0, title: 0 },
        Step::Upsert { by: 2, key: 2, title: 1 },
        Step::Upsert { by: 0, key: 2, title: 2 },
        Step::Rename { by: 0, from: 2, to: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t2"))]));
}

/// 縮めた形 (改名の時刻 6): 同じデッキの 2 つの identity を、 2 人が別々の名前に改名する (C は k1 → k2、 その後で B は k1 → k0)。
/// A が 2 つの改名の間に k1 で作った行は、 その時刻のデッキの名前が k2 なので新しいデッキ。 B の改名を先に受けた受け手でも、
/// C の改名 (LWW で負ける) を名前の年表に載せて、 k1 を離れた時刻を C の改名にする (前は B の改名で k1 を離れたと見て束ねた)。
#[test]
fn concurrent_renames_decide_the_former_name_by_the_earlier_rename() {
    let steps = [
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Upsert { by: 1, key: 1, title: 1 },
        Step::Rename { by: 2, from: 1, to: 2 },
        Step::Upsert { by: 0, key: 1, title: 2 },
        Step::Rename { by: 1, from: 1, to: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t1")), (text("k1"), text("t2"))]));
}

/// 縮めた形 (改名の時刻 7): 上の形で、 A が 2 つの改名の間に作るのが改名の先の名前 k2 (C の改名の後、 B の改名の前)。 その時刻の
/// デッキの名前は k2 なので、 A の行は同じデッキ。 B の改名を先に受けた受け手では C の改名が LWW で負けるが、 名前の年表に載せて
/// k2 を前の名前として覚え、 先に届いていた A の行も畳む。
#[test]
fn a_row_made_with_the_name_of_a_losing_rename_joins_the_deck() {
    let steps = [
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Upsert { by: 1, key: 1, title: 1 },
        Step::Rename { by: 2, from: 1, to: 2 },
        Step::Upsert { by: 0, key: 2, title: 2 },
        Step::Rename { by: 1, from: 1, to: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t2"))]));
}

/// 縮めた形 (改名の時刻 8): B が自分の行の title を書き換えてから改名する (名前も title も作成より後の版数になる)。 B の行に C の行を
/// 束ねる時、 B の行の作成を cell の版数の最小で近似すると C の改名より後に見え、 C の改名で B の行を切り離した。 名前の年表の
/// 最初の書き込み (改名の前の名前の版数) を作成にする。
#[test]
fn a_rewritten_row_keeps_its_creation_time_when_bound() {
    let steps = [
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Rename { by: 2, from: 1, to: 2 },
        Step::Retitle { by: 1, key: 1, title: 1 },
        Step::Rename { by: 1, from: 1, to: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t1"))]));
}

/// 縮めた形 (改名の時刻 9): C が A の k1 を受けて k0 に改名する。 C から先に受ける受け手は、 A の行の作成より先に改名を受け、
/// 別の行として払い出す。 後から届いた A の作成 (k1、 改名より古い) は LWW で負けるが、 その行が改名の前に k1 だったことを
/// 示す: 名前の年表を作って k1 を前の名前にし、 その間に k1 で作られた B の行も畳む。
#[test]
fn a_create_arriving_after_its_rename_reveals_the_former_name() {
    let steps = [
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Upsert { by: 0, key: 1, title: 1 },
        Step::Pull { by: 2, from: 0 },
        Step::Rename { by: 2, from: 1, to: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t1"))]));
}

/// #613 の形 (束ねた後の自分の改名が、 束ねた相手の identity 宛てで出る): A が自分の k2 に C の k2 を束ねてから改名する。 C の行の
/// 作成より先に改名を受けた受け手も、 後から届いた作成で前の名前を知り、 A の行を畳む。
#[test]
fn a_rename_addressed_to_a_bound_identity_still_converges() {
    let steps = [
        Step::Upsert { by: 2, key: 2, title: 0 },
        Step::Upsert { by: 0, key: 2, title: 1 },
        Step::Pull { by: 0, from: 2 },
        Step::Rename { by: 0, from: 2, to: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t1"))]));
}

/// 縮めた形 (改名の時刻 10): B が自分の k1 を k0 に改名した時、 A の行はまだ k0 だった (A は後で k2 に改名する)。 B の行は A の行と
/// 同じデッキ。 A の改名を先に受けた受け手は、 B の改名を当てた時に k0 の行が今は無いが、 その時刻に k0 だった A の行と畳む。
#[test]
fn a_rename_into_a_name_another_row_held_then_joins_that_row() {
    let steps = [
        Step::Upsert { by: 0, key: 0, title: 0 },
        Step::Upsert { by: 1, key: 1, title: 1 },
        Step::Rename { by: 1, from: 1, to: 0 },
        Step::Rename { by: 0, from: 0, to: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k2"), text("t1"))]));
}

/// 縮めた形 (改名の時刻 11): B が k2 を、 C の行が持つ k0 に改名し (C の行と同じデッキ)、 さらに k1 に改名する。 B の 2 つの改名を
/// 先に受けた受け手は、 C の行の作成の時刻には B の行がまだ k2 だが、 その後 C の行と同じ時刻に k0 だった (前の版は時刻を見ずに
/// 前の名前で束ねたので割れなかった)。
#[test]
fn a_row_holding_a_name_another_row_took_later_joins_it() {
    let steps = [
        Step::Upsert { by: 1, key: 2, title: 0 },
        Step::Upsert { by: 2, key: 0, title: 1 },
        Step::Rename { by: 1, from: 2, to: 0 },
        Step::Rename { by: 1, from: 0, to: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k1"), text("t1"))]));
}

/// 改名の時刻 12: A が自分で改名した後に、 B が改名の後に前の名前で作った行を A が受ける。 A の名前の年表に自分の改名が載って
/// いないと、 A だけ B の行を自分の行に束ねる。
#[test]
fn the_renamer_does_not_bind_a_later_row_with_its_former_name() {
    let steps = [
        Step::Upsert { by: 0, key: 1, title: 0 },
        Step::Rename { by: 0, from: 1, to: 0 },
        Step::Upsert { by: 1, key: 1, title: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t0")), (text("k1"), text("t1"))]));
}

/// 改名の時刻 13: C が k1 を k2 に改名した時、 A の行が k2 だった (A は後で k0 に改名する)。 C の改名は B の後の改名で負けるが、
/// その時刻に k2 だった A の行と同じデッキ。 B と A の改名を先に受けた受け手では、 負けた C の改名を年表に載せる時に A の行と畳む。
#[test]
fn a_losing_rename_joins_the_row_that_held_its_name_then() {
    let steps = [
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Upsert { by: 1, key: 1, title: 1 },
        Step::Upsert { by: 0, key: 2, title: 2 },
        Step::Rename { by: 2, from: 1, to: 2 },
        Step::Rename { by: 0, from: 2, to: 0 },
        Step::Rename { by: 1, from: 1, to: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t2"))]));
}

/// #614: A が k1 を作って消す。 B は C の k2 を受け、 A の削除を知らずに k1 に改名する (削除より後)。 C の行は削除の後に k1 に
/// なった行なので生き、 title も C が作った時のまま。 改名を C の行の作成より先に受けた受け手は、 k1 を消した削除の版数を払い出す
/// slot に載せ、 後から届いた C の作成と title (削除より古い) を捨てていた。
#[test]
fn a_rename_after_the_delete_keeps_the_writes_made_before_it() {
    let steps = [
        Step::Upsert { by: 0, key: 1, title: 0 },
        Step::Upsert { by: 2, key: 2, title: 0 },
        Step::Pull { by: 1, from: 2 },
        Step::Delete { by: 0, key: 1 },
        Step::Rename { by: 1, from: 2, to: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k1"), text("t0"))]));
}

/// #614 の続き: A が k1 を作って消す。 C は A の削除を知らずに k1 を作り (削除より前 = A の行と同じデッキ)、 B は C の k1 を受けて
/// 削除の後に k2 に改名する。 C の行は消えたデッキの一員なので、 削除の後の改名も捨てる。 B の改名を C の作成より先に受けた受け手は、
/// 最初に見た名前の書き込み (k2) で生きた slot を払い出す。 後から届いた C の作成 (削除より前に k1、 LWW で負ける) で消す。
#[test]
fn a_late_create_shows_the_row_was_in_the_deleted_deck() {
    let steps = [
        Step::Upsert { by: 0, key: 1, title: 0 },
        Step::Upsert { by: 2, key: 1, title: 1 },
        Step::Pull { by: 1, from: 2 },
        Step::Delete { by: 0, key: 1 },
        Step::Rename { by: 1, from: 1, to: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![]));
}

/// #614 の続き (上の形の裏): A が k1 を消した後に、 C が (削除を知らずに) k1 を作る (削除の後 = 新しいデッキ)。 B は C の k1 を受けて
/// k2 に改名する。 B の改名を先に受けた受け手に後から届く C の作成 (k1、 LWW で負ける) は削除より後なので、 C の行を消さない。
#[test]
fn a_late_create_after_the_delete_keeps_the_row() {
    let steps = [
        Step::Upsert { by: 0, key: 1, title: 0 },
        Step::Delete { by: 0, key: 1 },
        Step::Upsert { by: 2, key: 1, title: 1 },
        Step::Pull { by: 1, from: 2 },
        Step::Rename { by: 1, from: 1, to: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k2"), text("t1"))]));
}

/// 改名の時刻 14: B が k1 を k2 に改名した後に、 A が (B の行を知らずに) k0 を k1 に改名する。 A の行が k1 になった時、 B の行はもう
/// k2 なので、 2 つの行は同じ時刻に同じ名前だったことがない = 別のデッキ。
#[test]
fn a_rename_into_a_name_another_row_already_left_is_a_new_deck() {
    let steps = [
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Upsert { by: 0, key: 0, title: 0 },
        Step::Rename { by: 1, from: 1, to: 2 },
        Step::Rename { by: 0, from: 0, to: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k1"), text("t0")), (text("k2"), text("t0"))]));
}

/// 改名の時刻 15 (#620 の続き): A の k0 と C の k0 は同じデッキ (C が A の行を束ねる)。 B が k1 を k2 に改名した後に、 C がデッキを
/// k0 から k1 に改名する。 デッキは B の改名の後に k1 になったので、 B の行とは別のデッキ。 束ねていた A の identity は、 自分宛ての
/// k1 への書き込みが無くても、 C の改名で k1 を持ち始めた (改名はデッキの全部の identity に当たる)。 B の行の作成 (A の行の作成より
/// 後) は、 A の identity が k1 を持ち始めた時刻ではない。 最後の A の title の書き換え (A の identity 宛て) は、 A の identity と同じ
/// デッキ (k1) に当たる。
#[test]
fn a_deck_renamed_into_a_name_another_row_already_left_stays_together() {
    let steps = [
        Step::Upsert { by: 0, key: 0, title: 1 },
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Upsert { by: 2, key: 0, title: 1 },
        Step::Pull { by: 2, from: 0 },
        Step::Rename { by: 1, from: 1, to: 2 },
        Step::Rename { by: 2, from: 0, to: 1 },
        Step::Retitle { by: 0, key: 0, title: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k1"), text("t2")), (text("k2"), text("t0"))]));
}

/// 削除優先 (#614 で払い出しの道を書き換えたので): C が A の削除を知らずに k1 を作り (削除より前 = 同じデッキ)、 削除の後に title を
/// 書き換える。 C の行を削除の後に受けた受け手は、 死んだ slot を払い出して、 削除の後の書き換えも捨てる。
#[test]
fn a_write_after_the_delete_to_a_row_made_before_it_is_dropped() {
    let steps = [
        Step::Upsert { by: 0, key: 1, title: 0 },
        Step::Upsert { by: 2, key: 1, title: 1 },
        Step::Delete { by: 0, key: 1 },
        Step::Retitle { by: 2, key: 1, title: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![]));
}

/// 改名の時刻 16 (#620 の続き): 上の形で、 C の改名 (A の identity 宛て、 束ねた後の書き込みの宛名) で k1 を持ち始めた C の identity。
/// C の identity は B の行より前に作られ、 B の改名の後に (A の identity 宛ての改名で) k1 になった。 B の行の作成は、 C の identity が
/// k1 を持ち始めた時刻ではない (C の title が残るのは A と C のデッキ)。
#[test]
fn an_identity_renamed_by_another_identity_of_its_deck_stays_with_the_deck() {
    let steps = [
        Step::Upsert { by: 0, key: 0, title: 1 },
        Step::Upsert { by: 2, key: 0, title: 1 },
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Retitle { by: 2, key: 0, title: 2 },
        Step::Pull { by: 2, from: 0 },
        Step::Rename { by: 1, from: 1, to: 2 },
        Step::Rename { by: 2, from: 0, to: 1 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k1"), text("t2")), (text("k2"), text("t0"))]));
}

/// #612: C が自分で作った 2 行 (k1 と k0) のうち、 k1 を B が k0 に改名する (B は C の k0 を知らない)。 2 行は同じ名前 = 同じデッキ。
/// 他の peer は 2 行を 1 つに畳む。 C も自分の 2 行を 1 つに畳む (畳まれた行の eid は残す行に転送する)。
#[test]
fn a_peer_folds_its_own_rows_renamed_into_one_name() {
    let steps = [
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Pull { by: 1, from: 2 },
        Step::Upsert { by: 2, key: 0, title: 0 },
        Step::Rename { by: 1, from: 1, to: 0 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t0"))]));
}

/// #612 の別の形: 上の後に、 B (自分の 2 行を持つ側) がデッキを改名する。 畳んでいないと、 改名は 2 行の片方にしか当たらない。
#[test]
fn a_rename_after_folding_own_rows_renames_the_whole_deck() {
    let steps = [
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Pull { by: 2, from: 1 },
        Step::Upsert { by: 1, key: 0, title: 0 },
        Step::Rename { by: 2, from: 1, to: 0 },
        Step::Rename { by: 1, from: 0, to: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k2"), text("t0"))]));
}

/// #612 の続き (年表で畳む): C の k1 を B が k0 に改名し、 その後 C が (改名を知らずに) k0 を作る。 A は B の改名を受けてデッキを k2 に
/// 改名する。 C は A の改名を先に受けるので、 B の改名は LWW で負けるが、 C の行が k0 だった間に C の k0 が作られた = 同じデッキ。
/// 負けた改名を年表に載せる時に、 C の自分の 2 行を畳む。
#[test]
fn a_losing_rename_folds_own_rows_made_while_it_held_the_name() {
    let steps = [
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Pull { by: 1, from: 2 },
        Step::Rename { by: 1, from: 1, to: 0 },
        Step::Upsert { by: 2, key: 0, title: 0 },
        Step::Pull { by: 0, from: 1 },
        Step::Rename { by: 0, from: 0, to: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k2"), text("t0"))]));
}

/// #612 の続き (切り離し): C の k1 を B が k0 に改名し、 A がさらに k5 に改名する。 その後 C が (どちらも知らずに) k0 を作る (デッキが
/// k0 を離れた後 = 新しいデッキ)。 C は B の改名を先に受けて自分の 2 行を畳み、 A の改名を受けて切り離す。 切り離す C の k0 は
/// 元の slot に戻す (新しい slot に移すと、 その後の C の書き込みが知らない世界番号宛てに出る)。
#[test]
fn a_split_puts_a_folded_own_row_back_in_its_slot() {
    let steps = [
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Pull { by: 1, from: 2 },
        Step::Rename { by: 1, from: 1, to: 0 },
        Step::Pull { by: 0, from: 1 },
        Step::Rename { by: 0, from: 0, to: 5 },
        Step::Upsert { by: 2, key: 0, title: 1 },
        Step::Pull { by: 2, from: 1 },
        Step::Pull { by: 2, from: 0 },
        Step::Retitle { by: 2, key: 0, title: 2 },
    ];
    assert_eq!(run(&steps), Ok(vec![(text("k0"), text("t2")), (text("k5"), text("t0"))]));
}
