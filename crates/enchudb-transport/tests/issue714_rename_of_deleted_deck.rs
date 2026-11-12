//! #714: 削除を知らない peer が消えたデッキ (eid) を改名した時、 その名前のカードが、 削除の後に作られた同じ名前のデッキに
//! 写っていた (届く順で `(k0, t0)` と `(k1, t0)` に割れた)。 約束は 「消したデッキ宛ての書き込みは、 後から何が来ても捨てる。
//! 消した後に足した同じ名前のデッキは新しいデッキ」。
//!
//! - `a_rename_of_a_deleted_deck_...`: issue の 5 手を、 受け手の 6 通りの届く順 + 書き手 3 台 + 観測者 2 台で
//! - `the_delete_arrives_after_...`: 同じ形を 1 手ずつ届け、 束ねてから改名が当たった所 (1 行で名前が k0) を確かめてから削除を届ける
//! - `mesh_model_*`: 直に pull する形の乱数 model (途中で受ける観測者 / 開き直し / cursor なしの開き直し = 当て直し / 名前を
//!   最後に set する insert)。 種は固定。 回数と種は環境変数で変えられる:
//!
//! ```text
//! ISSUE714_CASES=200 ISSUE714_SEED=1 ISSUE714_MODE=mesh|reopen|redeliver|redeliver_norename|keylast \
//!   cargo test -p enchudb-transport --test issue714_rename_of_deleted_deck -- --nocapture
//! ```

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
        let dir = std::env::temp_dir().join(format!("enchudb-issue714-{}-{n}", std::process::id()));
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

#[derive(Clone, Copy, Debug)]
struct Mode {
    reopen: bool,
    keep_cursor: bool,
    key_last: bool,
    rename: bool,
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn gen_steps(rng: &mut Rng, mode: Mode) -> Vec<Step> {
    let n = 2 + rng.below(11) as usize;
    let mut out = Vec::new();
    for _ in 0..n {
        let by = rng.below(AUTHORS as u64) as usize;
        let any = rng.below((AUTHORS + OBSERVERS) as u64) as usize;
        let k = rng.below(KEYS as u64) as u8;
        let v = rng.below(TITLES as u64) as u8;
        let from = {
            let f = rng.below(AUTHORS as u64) as usize;
            if f == any { (f + 1) % AUTHORS } else { f }
        };
        let s = match rng.below(20) {
            0..=3 => Step::Upsert { by, key: k, title: v },
            4 if mode.key_last => Step::InsertKeyLast { by, key: k, title: v },
            4 => Step::Upsert { by, key: k, title: v },
            5..=7 if mode.rename => {
                let to = (k + 1 + rng.below(KEYS as u64 - 1) as u8) % KEYS;
                Step::Rename { by, from: k, to }
            }
            5..=7 => Step::Retitle { by, key: k, title: v },
            8..=10 => Step::Delete { by, key: k },
            11 => Step::Retitle { by, key: k, title: v },
            12..=17 => Step::Pull { by: any, from },
            _ if mode.reopen => Step::Reopen { by: any },
            _ => Step::Pull { by: any, from },
        };
        out.push(s);
    }
    out
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

fn run(steps: &[Step], mode: Mode) -> Result<Answer, String> {
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
        (1..=n_peers).map(|id| Some(attach(&dir, create(&dir, id), &tr, mode.keep_cursor))).collect();
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
                let p = attach(&dir, db, &tr, mode.keep_cursor);
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

fn shrink(mut steps: Vec<Step>, mode: Mode) -> Vec<Step> {
    loop {
        let mut smaller = None;
        for i in 0..steps.len() {
            let mut t = steps.clone();
            t.remove(i);
            if !t.is_empty() && run(&t, mode).is_err() && run(&t, mode).is_err() {
                smaller = Some(t);
                break;
            }
        }
        match smaller {
            Some(t) => steps = t,
            None => return steps,
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn fuzz(name: &str, mode: Mode) {
    if std::env::var("ISSUE714_MODE").is_ok_and(|m| m != name) {
        return;
    }
    let cases = env_u64("ISSUE714_CASES", 8);
    let seed = env_u64("ISSUE714_SEED", 1);
    let max_fail = env_u64("ISSUE714_MAX_FAIL", 3);
    let mut fails = 0;
    for case in 0..cases {
        let mut rng = Rng(seed.wrapping_mul(1_000_003).wrapping_add(case));
        let steps = gen_steps(&mut rng, mode);
        if let Err(msg) = run(&steps, mode) {
            let again = run(&steps, mode).is_err();
            println!(
                "=== {name} seed {seed} case {case}: 失敗 (もう 1 回: {})\n手順 {steps:?}\n{msg}",
                if again { "失敗" } else { "通る" }
            );
            if again {
                let small = shrink(steps, mode);
                println!(
                    "--- 縮めた手順 {small:?}\n{}",
                    run(&small, mode).err().unwrap_or_else(|| "(縮めた手順は今回通った)".into())
                );
            }
            fails += 1;
            if fails >= max_fail {
                break;
            }
        }
    }
    assert_eq!(fails, 0, "{name}: {fails} 件の食い違い (seed {seed}, cases {cases})");
}

#[test]
fn mesh_model_observers() {
    fuzz("mesh", Mode { reopen: false, keep_cursor: true, key_last: false, rename: true });
}

#[test]
fn mesh_model_reopen() {
    fuzz("reopen", Mode { reopen: true, keep_cursor: true, key_last: false, rename: true });
}

#[test]
fn mesh_model_redeliver() {
    fuzz("redeliver", Mode { reopen: true, keep_cursor: false, key_last: false, rename: true });
}

#[test]
fn mesh_model_redeliver_no_rename() {
    fuzz("redeliver_norename", Mode { reopen: true, keep_cursor: false, key_last: false, rename: false });
}

#[test]
fn mesh_model_key_last() {
    fuzz("keylast", Mode { reopen: false, keep_cursor: true, key_last: true, rename: true });
}

const PLAIN: Mode = Mode { reopen: false, keep_cursor: true, key_last: false, rename: true };

fn text(s: &str) -> Option<Value> {
    Some(Value::Text(s.into()))
}

/// issue の 5 手 (既存の `tests/sync_converge_fuzz.rs` の種 3741 を proptest が縮めた形): 2 が k1 を作る → 1 が受ける → 2 が k1 を
/// 消す → 3 が (削除の後に) k1 を作る = 新しいデッキ → 1 が (削除を知らずに) 2 の行を k1 → k0 に改名する (消えたデッキ宛て =
/// 捨てる)。 答えは 3 の k1 だけ。 直す前は、 1 の改名を 2 の作成・削除より先に受けた受け手 ([3,1,2] / [1,3,2]) と 3 自身で、
/// 3 の行が k0 になった (3 回中 3 回)。 `run` は受け手 6 通り・書き手 3 台・観測者 2 台の答えが全部同じ時だけ Ok。
#[test]
fn a_rename_of_a_deleted_deck_does_not_rename_the_deck_made_after_the_delete() {
    let steps = [
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Pull { by: 0, from: 1 },
        Step::Delete { by: 1, key: 1 },
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Rename { by: 0, from: 1, to: 0 },
    ];
    let want: Answer = vec![(text("k1"), text("t0"))];
    for i in 0..3 {
        assert_eq!(run(&steps, PLAIN), Ok(want.clone()), "{} 回目", i + 1);
    }
}

/// 同じ形の、 改名を title の書き換えに替えた物: 1 が (削除を知らずに) 消えた 2 の行の title を書き換える。 3 の行には写らない。
#[test]
fn an_edit_of_a_deleted_deck_does_not_land_on_the_deck_made_after_the_delete() {
    let steps = [
        Step::Upsert { by: 1, key: 1, title: 0 },
        Step::Pull { by: 0, from: 1 },
        Step::Delete { by: 1, key: 1 },
        Step::Upsert { by: 2, key: 1, title: 0 },
        Step::Retitle { by: 0, key: 1, title: 2 },
    ];
    assert_eq!(run(&steps, PLAIN), Ok(vec![(text("k1"), text("t0"))]));
}

/// 同じ形を 1 手ずつ届ける (作成と削除が別の配り): 観測者は 2 の作成 → 3 の作成 → 1 の改名 → 2 の削除 の順に受ける。 削除の
/// 前に、 2 つのデッキが 1 行に束ねられて改名が当たっている事 (1 行で名前が k0、 title は 3 の物) を確かめてから、 削除を届ける。
/// 削除は消えたデッキ (2 の行) とその宛ての改名を捨て、 3 のデッキを 3 が書いたとおり (k1, t1) に戻す。 書き込みが書かれた順
/// (2 の作成 → 2 の削除 → 3 の作成 → 1 の改名) に受けた受け手と同じ答え。
#[test]
fn the_delete_arrives_after_the_two_decks_were_bound_and_renamed() {
    let dir = Dir::new();
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=5 {
        tr.register_peer(id);
    }
    let p: Vec<Peer> = (1..=3).map(|id| attach(&dir, create(&dir, id), &tr, true)).collect();
    let watcher = attach(&dir, create(&dir, 4), &tr, true);
    let in_order = attach(&dir, create(&dir, 5), &tr, true);

    p[1].write(&Step::Upsert { by: 1, key: 1, title: 0 });
    watcher.sync.pull_once(2);
    in_order.sync.pull_once(2);
    assert_eq!(watcher.rows(), vec![(text("k1"), text("t0"))], "前提: 観測者は 2 の作成だけを受けた");
    p[0].sync.pull_once(2);
    p[1].write(&Step::Delete { by: 1, key: 1 });
    in_order.sync.pull_once(2);
    assert_eq!(in_order.rows(), vec![], "前提: 順に受ける受け手は、 3 の作成より先に削除を受けた");
    p[2].write(&Step::Upsert { by: 2, key: 1, title: 1 });
    assert_eq!(p[0].rows(), vec![(text("k1"), text("t0"))], "前提: 1 は削除を知らない");
    p[0].write(&Step::Rename { by: 0, from: 1, to: 0 });
    assert_eq!(p[0].rows(), vec![(text("k0"), text("t0"))], "前提: 1 は 2 の行を改名した");

    watcher.sync.pull_once(3);
    assert_eq!(watcher.rows(), vec![(text("k1"), text("t1"))], "前提: 3 の行は同じ名前の 2 の行に束ねられた (1 行)");
    watcher.sync.pull_once(1);
    assert_eq!(
        watcher.rows(),
        vec![(text("k0"), text("t1"))],
        "前提: 削除の前は 1 つのデッキで、 1 の改名が当たっている (この形を踏んでいない)"
    );
    watcher.sync.pull_once(2);
    let want: Answer = vec![(text("k1"), text("t1"))];
    assert_eq!(watcher.rows(), want, "削除の後に作られた 3 のデッキに、 消えたデッキ宛ての改名が残った");
    assert_eq!(watcher.violations(), Vec::<String>::new());

    in_order.sync.pull_once(3);
    in_order.sync.pull_once(1);
    assert_eq!(in_order.rows(), want, "書かれた順に受けた受け手");
    // 書き手どうしも同じ答えに
    for _ in 0..2 {
        for (i, peer) in p.iter().enumerate() {
            for from in 1..=3u32 {
                if from as usize != i + 1 {
                    peer.sync.pull_once(from);
                }
            }
        }
    }
    for (i, peer) in p.iter().enumerate() {
        assert_eq!(peer.rows(), want, "書き手 {}", i + 1);
        assert_eq!(peer.violations(), Vec::<String>::new(), "書き手 {}", i + 1);
    }
}
