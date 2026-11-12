//! #751: 改名して元の名前に戻す形で、 届く順によって行が 2 つに割れた。
//!
//! 2 台が同じ名前 k2 の行を別々に作る (同じデッキ)。 一方が k1 に改名し、 それを受けた 3 台目が (自分の持つ、 もう一方の行宛てに)
//! k2 に戻す。 戻す改名を先に受け、 その行の作成を最後に受けた受け手は、 戻す改名の時刻をその行の作成と見て 「改名の後に前の
//! 名前で作られた別のデッキ」 として切り離し、 後から本当の作成 (改名より前) が届いても畳み直さなかった。
//!
//! 既存の `tests/sync_converge_fuzz.rs` の種 11 (400 件、 release) を proptest が縮めた 6 手。

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 1 << 20;
const AUTHORS: usize = 3;
const OBSERVERS: usize = 2;
const RECEIVER_BASE: u32 = 10;
const KEYS: u8 = 3;
const TITLES: u8 = 3;

static RUN: AtomicU64 = AtomicU64::new(0);

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let n = RUN.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("enchudb-issue751-{}-{n}", std::process::id()));
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

fn create(dir: &Dir, id: u32) -> Arc<Database> {
    let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
    b.table(T).tag("key").tag("title").primary_key("key").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    db
}

fn attach(dir: &Dir, db: Arc<Database>, tr: &Arc<InMemoryTransport>, keep_cursor: bool) -> Peer {
    let id = db.engine().peer_id();
    let mut sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    if keep_cursor {
        sync = sync.with_cursor_path(format!("{}.cursors", dir.db(id)).into());
    }
    Peer { db, sync }
}

fn key(k: u8) -> String {
    format!("k{k}")
}
fn title(t: u8) -> String {
    format!("t{t}")
}

type Answer = Vec<(Option<Value>, Option<Value>)>;

impl Peer {
    fn settle(&self) {
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        eng.transfer_oplog_to_sync_ops();
    }

    fn publish(&self) {
        self.settle();
        self.sync.publish_since(Hlc::ZERO);
        std::thread::sleep(Duration::from_millis(2));
    }

    fn row(&self, k: u8) -> Option<u64> {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().where_eq("key", key(k).as_str()).find().unwrap().first().copied()
    }

    fn write(&self, s: &Step) {
        let t = self.db.get_table(T).unwrap();
        match *s {
            Step::Upsert { key: k, title: v, .. } => {
                t.upsert().set("key", key(k).as_str()).set("title", title(v).as_str()).commit().unwrap();
            }
            Step::InsertKeyLast { key: k, title: v, .. } => {
                // 名前を最後に set する (その名前の行が無い時だけ: insert は同じ名前を断るかもしれない)
                if self.row(k).is_some() {
                    return;
                }
                let _ = t.insert().set("title", title(v).as_str()).set("key", key(k).as_str()).commit();
            }
            Step::Rename { from, to, .. } => {
                let (Some(e), None) = (self.row(from), self.row(to)) else { return };
                let _ = t.entity(e).set("key", key(to).as_str()).commit();
            }
            Step::Delete { key: k, .. } => {
                let Some(e) = self.row(k) else { return };
                t.entity(e).delete().unwrap();
            }
            Step::Retitle { key: k, title: v, .. } => {
                let Some(e) = self.row(k) else { return };
                t.entity(e).set("title", title(v).as_str()).commit().unwrap();
            }
            _ => unreachable!(),
        }
        self.publish();
    }

    fn violations(&self) -> Vec<String> {
        let eng = self.db.engine();
        let mut out = eng.verify_invariants().violations;
        eng.persist_tables().unwrap();
        if let Some((disk, mem)) = eng.eidmap_disk_diff().unwrap() {
            out.push(format!("eidmap: disk にだけ {disk:?} / memory にだけ {mem:?}"));
        }
        out
    }

    fn rows(&self) -> Answer {
        self.db.engine().rebuild();
        let t = self.db.get_table(T).unwrap();
        let mut eids = t.all().find().unwrap();
        eids.extend(t.all().where_ne("title", "\u{0}").find().unwrap());
        eids.sort_unstable();
        eids.dedup();
        let mut out: Answer = eids
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

#[derive(Debug, Clone, PartialEq)]
enum Step {
    Upsert { by: usize, key: u8, title: u8 },
    InsertKeyLast { by: usize, key: u8, title: u8 },
    Rename { by: usize, from: u8, to: u8 },
    Delete { by: usize, key: u8 },
    Retitle { by: usize, key: u8, title: u8 },
    /// by: 0..AUTHORS は書き手、 その先は観測者
    Pull { by: usize, from: usize },
    Reopen { by: usize },
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

fn run(steps: &[Step]) -> Result<Answer, String> {
    let dir = Dir::new();
    let orders = permutations(AUTHORS);
    let tr = Arc::new(InMemoryTransport::new());
    let n_peers = (AUTHORS + OBSERVERS) as u32;
    for id in 1..=n_peers {
        tr.register_peer(id);
    }
    for k in 0..orders.len() as u32 {
        tr.register_peer(RECEIVER_BASE + k);
    }
    let mut peers: Vec<Option<Peer>> =
        (1..=n_peers).map(|id| Some(attach(&dir, create(&dir, id), &tr, true))).collect();
    for s in steps {
        match *s {
            Step::Pull { by, from } => {
                peers[by].as_ref().unwrap().sync.pull_once(from as u32 + 1);
            }
            Step::Reopen { by } => {
                let p = peers[by].take().unwrap();
                p.settle();
                drop(p);
                let db = Database::open_with_oplog(&dir.db(by as u32 + 1), OPLOG).map_err(|e| format!("reopen: {e:?}"))?;
                db.engine().set_peer_id(by as u32 + 1);
                let p = attach(&dir, db, &tr, true);
                if by < AUTHORS {
                    p.publish();
                }
                peers[by] = Some(p);
            }
            Step::Upsert { by, .. }
            | Step::InsertKeyLast { by, .. }
            | Step::Rename { by, .. }
            | Step::Delete { by, .. }
            | Step::Retitle { by, .. } => peers[by].as_ref().unwrap().write(s),
        }
    }
    let mut answers: Vec<(String, Answer)> = Vec::new();
    let mut broken: Vec<String> = Vec::new();
    for (k, order) in orders.iter().enumerate() {
        let r = attach(&dir, create(&dir, RECEIVER_BASE + k as u32), &tr, false);
        for &from in order {
            r.sync.pull_once(from as u32 + 1);
        }
        let who = format!("受け手 {:?}", order.iter().map(|a| a + 1).collect::<Vec<_>>());
        broken.extend(r.violations().into_iter().map(|v| format!("{who}: {v}")));
        answers.push((who, r.rows()));
    }
    for _ in 0..2 {
        for (i, p) in peers.iter().flatten().enumerate() {
            for from in 0..AUTHORS {
                if from != i {
                    p.sync.pull_once(from as u32 + 1);
                }
            }
        }
    }
    for (i, p) in peers.iter().flatten().enumerate() {
        let who = if i < AUTHORS { format!("書き手 {}", i + 1) } else { format!("観測者 {}", i + 1) };
        broken.extend(p.violations().into_iter().map(|v| format!("{who}: {v}")));
        answers.push((who, p.rows()));
    }
    drop(peers);
    if !broken.is_empty() {
        return Err(format!("不変量の違反\n  {}", broken.join("\n  ")));
    }
    for (who, a) in &answers {
        let mut names: Vec<_> = a.iter().filter_map(|r| r.0.clone()).map(|v| format!("{v:?}")).collect();
        let n = names.len();
        names.dedup();
        if names.len() != n {
            return Err(format!("{who} に同じ名前の行が 2 つ: {a:?}"));
        }
    }
    if answers.iter().all(|(_, x)| *x == answers[0].1) {
        return Ok(answers.swap_remove(0).1);
    }
    let mut msg = String::from("答えが割れる\n");
    for (who, rows) in &answers {
        msg.push_str(&format!("  {who}: {rows:?}\n"));
    }
    Err(msg)
}


fn text(s: &str) -> Option<Value> {
    Some(Value::Text(s.into()))
}

/// 6 手: 2 が k2 を作る → 1 が k2 を作る (別々に、 同じデッキ) → 3 が 1 から受ける → 2 が k2 → k1 に改名 → 3 が 2 から受ける
/// (2 の行を 1 の行に束ね、 改名が当たって k1) → 3 が k1 → k2 に戻す (宛名は 3 の持つ行の元の identity = 1 の行)。
/// 答えは 1 行 (k2, t0)。 `run` は受け手 6 通り・書き手 3 台・観測者 2 台の答えが全部同じ時だけ Ok。
#[test]
fn a_rename_back_to_the_first_name_keeps_one_row_in_every_order() {
    let steps = [
        Step::Upsert { by: 1, key: 2, title: 0 },
        Step::Upsert { by: 0, key: 2, title: 0 },
        Step::Pull { by: 2, from: 0 },
        Step::Rename { by: 1, from: 2, to: 1 },
        Step::Pull { by: 2, from: 1 },
        Step::Rename { by: 2, from: 1, to: 2 },
    ];
    let want: Answer = vec![(text("k2"), text("t0"))];
    for i in 0..3 {
        assert_eq!(run(&steps), Ok(want.clone()), "{} 回目", i + 1);
    }
}

/// 同じ形を、 割れた受け手の順 (3 → 2 → 1) で 1 台ずつ受けて、 途中の形を確かめる。 3 から受けた時点では 1 の行の作成を知らない
/// (戻す改名だけの、 title の無い行)。 2 から受けると、 2 の行は改名で k1 になり、 1 の行は 「改名の後に k2 になった行」 に見える
/// ので別の行 (ここまでは、 知っている事からは正しい)。 1 から本当の作成 (改名より前) を受けたら、 同じ時刻に同じ名前だった
/// 2 つの行は同じデッキなので 1 行に畳む。 直す前は 2 行のまま残った。
#[test]
fn the_creation_arriving_after_the_rename_back_folds_the_two_rows() {
    let dir = Dir::new();
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=4 {
        tr.register_peer(id);
    }
    let p: Vec<Peer> = (1..=3).map(|id| attach(&dir, create(&dir, id), &tr, true)).collect();
    let late = attach(&dir, create(&dir, 4), &tr, true);
    p[1].write(&Step::Upsert { by: 1, key: 2, title: 0 });
    p[0].write(&Step::Upsert { by: 0, key: 2, title: 0 });
    p[2].sync.pull_once(1);
    p[1].write(&Step::Rename { by: 1, from: 2, to: 1 });
    p[2].sync.pull_once(2);
    assert_eq!(p[2].rows(), vec![(text("k1"), text("t0"))], "前提: 3 は 2 つの行を束ね、 2 の改名が当たった");
    p[2].write(&Step::Rename { by: 2, from: 1, to: 2 });
    assert_eq!(p[2].rows(), vec![(text("k2"), text("t0"))], "前提: 3 は元の名前に戻した");

    late.sync.pull_once(3);
    assert_eq!(late.rows(), vec![(text("k2"), None)], "前提: 戻す改名だけを受けた (1 の行の作成はまだ)");
    late.sync.pull_once(2);
    assert_eq!(
        late.rows(),
        vec![(text("k1"), text("t0")), (text("k2"), None)],
        "前提: 1 の行の作成を知らない間は別の行 (この形を踏んでいない)"
    );
    late.sync.pull_once(1);
    assert_eq!(late.rows(), vec![(text("k2"), text("t0"))], "1 の行の作成 (改名より前) が届いても 2 行のまま");
    assert_eq!(late.violations(), Vec::<String>::new());
}
