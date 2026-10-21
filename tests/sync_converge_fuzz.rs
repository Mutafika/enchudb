//! sync の収束の総当たり: 書き手 3 台が乱数で並べた手順 (作成 / 改名 / 削除 / 書き換え / 相手から pull) を、 受け手が書き手の
//! 全部の pull の順で受け、 書き手どうしも pull し合った後に、 答えが全部同じか (peer ごとに割れないか) を見る。 割れたら proptest が
//! 手順を縮めて最小の形を出す。
//!
//! 名前の列 (`key`) で束ねる表 (#141)。 改名 (#569)・削除優先 (#577)・消した名前 (#582 / #588) の組み合わせを、 人が形を思い付く代わりに
//! 機械に探させる (#547 / #551 / #545 / #581 は全部人が見つけた)。 答えには名前の無い塊 (title だけの entity) も入れる。
//!
//! 既知の割れ (#581: 削除の後に同じ名前で作った行を、 削除より先に受けて束ねた peer は後の削除で消す) があるので、 普段の試験からは外す:
//!
//! ```text
//! ENCHU_FUZZ_CASES=200 cargo test --release --test sync_converge_fuzz -- --ignored --nocapture
//! ```

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

    /// engine の不変量の違反 (`verify_invariants`、 書き手を止めてから)。 主キーの重複は警告 (#601 が入るまで)。
    fn violations(&self) -> Vec<String> {
        self.db.engine().verify_invariants().violations
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

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), max_shrink_iters: 2000, ..ProptestConfig::default() })]

    #[test]
    #[ignore = "既知の割れ (#581) があるので手で回す"]
    fn every_order_converges(steps in proptest::collection::vec(step_strategy(), 1..10)) {
        if let Err(msg) = run(&steps) {
            prop_assert!(false, "手順 {steps:?}\n{msg}");
        }
    }
}

/// 道具の前提: 既知の割れ (#581) の形を渡すと割れを見つける (見つけられない道具では、 通っても何も言えない)。
#[test]
fn the_tool_sees_the_known_split() {
    let steps = [
        Step::Upsert { by: 0, key: 0, title: 0 },
        Step::Pull { by: 1, from: 0 },
        Step::Delete { by: 1, key: 0 },
        Step::Upsert { by: 2, key: 0, title: 1 },
    ];
    assert!(run(&steps).is_err(), "#581 の形で割れを見つけない");
}
