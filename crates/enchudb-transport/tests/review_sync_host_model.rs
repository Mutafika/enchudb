//! review (0.37.4): ホスト経由の収束の乱数 model。 client 3 台 + ホスト。 名前 (`key`) で束ねる表に、 乱数で並べた手順
//! (作成 / 改名 / 削除 / 書き換え / 出す / 受ける / 開き直す / ホストの回収) を当て、 最後に全員が出して受けた後、 client と
//! ホストの答え (key, title) が全部同じかを見る。 既存の `tests/sync_converge_fuzz.rs` は相手から直に pull する形だけで、
//! ホスト (中継 + 忘れてよい線) を通らない。
//!
//! ```text
//! REVIEW_CASES=200 REVIEW_SEED=1 REVIEW_MODE=plain|line|reopen cargo test -p enchudb-transport --test review_sync_host_model -- --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const T: &str = "t";
const OPLOG: usize = 1 << 20;
const HOST: PeerId = 100;
const CLIENTS: usize = 3;
const KEYS: u8 = 3;
const TITLES: u8 = 3;

static RUN: AtomicU64 = AtomicU64::new(0);
/// 取り直し (bootstrap) の起きた、 通った case の数
static BOOTS: AtomicU64 = AtomicU64::new(0);

struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let n = RUN.fetch_add(1, Ordering::Relaxed);
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("review-host-model-{}-{n}", std::process::id()));
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

fn create(path: &str, id: u32) -> Arc<Database> {
    let mut b = Database::create_with_capacity(path, 4096).unwrap();
    b.table(T).tag("key").tag("title").primary_key("key").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    db
}

struct Client {
    db: Arc<Database>,
    t: Arc<HttpHostTransport>,
    s: Syncer,
}

fn attach(db: Arc<Database>, url: &str, cursors: &str) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), db.engine().peer_id()).unwrap());
    let s = Syncer::new(db.arc_engine(), t.clone()).with_cursor_path(cursors.into());
    s.serve_state();
    Client { db, t, s }
}

fn key(k: u8) -> String {
    format!("k{k}")
}
fn title(t: u8) -> String {
    format!("t{t}")
}

type Answer = Vec<(Option<Value>, Option<Value>)>;

fn rows(db: &Database) -> Answer {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
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

impl Client {
    fn row(&self, k: u8) -> Option<u64> {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().where_eq("key", key(k).as_str()).find().unwrap().first().copied()
    }

    fn settle(&self) {
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        eng.transfer_oplog_to_sync_ops();
        std::thread::sleep(Duration::from_millis(2));
    }

    fn push(&self) {
        self.settle();
        self.s.publish_since(Hlc::ZERO);
    }

    /// 受ける。 抜けを言われたら取り直して受け直す。 戻り値: 取り直した回数。
    fn pull(&self) -> usize {
        let mut boots = 0;
        for _ in 0..4 {
            let out = self.s.pull_once(HOST);
            if !out.history_truncated {
                return boots;
            }
            let mut authors = out.truncated_authors.clone();
            if authors.is_empty() {
                // 書き手を特定できない抜け: ホストと他の client を全部
                authors = (1..=CLIENTS as u32).filter(|a| *a != self.db.engine().peer_id()).collect();
                authors.push(HOST);
            }
            for a in authors {
                let _ = self.s.bootstrap_pull_via(HOST, a);
                boots += 1;
            }
        }
        boots
    }

    fn write(&self, s: &Step) {
        let t = self.db.get_table(T).unwrap();
        match *s {
            Step::Upsert { key: k, title: v, .. } => {
                t.upsert().set("key", key(k).as_str()).set("title", title(v).as_str()).commit().unwrap();
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
        self.settle();
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Step {
    Upsert { by: usize, key: u8, title: u8 },
    Rename { by: usize, from: u8, to: u8 },
    Delete { by: usize, key: u8 },
    Retitle { by: usize, key: u8, title: u8 },
    Push { by: usize },
    Pull { by: usize },
    Reopen { by: usize },
    Reclaim,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    /// 線なし (cursor の置き場なし)
    Plain,
    /// 線あり + ホストの回収
    Line,
    /// 線なし + client の開き直し
    Reopen,
    /// 線あり + 回収 + 開き直し
    All,
    /// 線あり + 回収 + 開き直し、 client ごとに別の名前だけを作り (他人の行は消す / 書き換えるだけ)、 改名なし。
    /// 「負けた名前の Tie を中継しない」 形 (review_sync_host_relay_losing_name.rs) を避けて、 ホストの線・回収・取り直しだけを見る
    Disjoint,
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
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
    let n = 2 + rng.below(12) as usize;
    let mut out = Vec::new();
    for _ in 0..n {
        let by = rng.below(CLIENTS as u64) as usize;
        let k = rng.below(KEYS as u64) as u8;
        let v = rng.below(TITLES as u64) as u8;
        if mode == Mode::Disjoint {
            let any = rng.below(KEYS as u64 * CLIENTS as u64) as u8;
            out.push(match rng.below(20) {
                0..=3 => Step::Upsert { by, key: by as u8 * KEYS + k, title: v },
                4..=6 => Step::Delete { by, key: any },
                7..=8 => Step::Retitle { by, key: any, title: v },
                9..=12 => Step::Push { by },
                13..=16 => Step::Pull { by },
                17 => Step::Reopen { by },
                _ => Step::Reclaim,
            });
            continue;
        }
        let s = match rng.below(20) {
            0..=3 => Step::Upsert { by, key: k, title: v },
            4..=6 => {
                let to = (k + 1 + rng.below(KEYS as u64 - 1) as u8) % KEYS;
                Step::Rename { by, from: k, to }
            }
            7..=9 => Step::Delete { by, key: k },
            10 => Step::Retitle { by, key: k, title: v },
            11..=14 => Step::Push { by },
            15..=17 => Step::Pull { by },
            18 if matches!(mode, Mode::Reopen | Mode::All) => Step::Reopen { by },
            19 if matches!(mode, Mode::Line | Mode::All) => Step::Reclaim,
            _ => Step::Pull { by },
        };
        out.push(s);
    }
    out
}

fn run(steps: &[Step], mode: Mode) -> Result<Answer, String> {
    let dir = Dir::new();
    let host_db = create(&dir.db(HOST), HOST);
    let line = matches!(mode, Mode::Line | Mode::All | Mode::Disjoint);
    let cursor_path = line.then(|| PathBuf::from(format!("{}.cursors", dir.db(HOST))));
    let host = HttpHost::start("127.0.0.1:0", host_db.arc_engine(), cursor_path).map_err(|e| e.to_string())?;
    let url = format!("http://{}", host.addr());
    let cur = |i: usize| format!("{}.cursors", dir.db(i as u32 + 1));
    let mut cs: Vec<Option<Client>> =
        (0..CLIENTS).map(|i| Some(attach(create(&dir.db(i as u32 + 1), i as u32 + 1), &url, &cur(i)))).collect();
    let mut notes: Vec<String> = Vec::new();
    for s in steps {
        match *s {
            Step::Push { by } => cs[by].as_ref().unwrap().push(),
            Step::Pull { by } => {
                let b = cs[by].as_ref().unwrap().pull();
                if b > 0 {
                    notes.push(format!("client {} が {b} 回取り直した", by + 1));
                }
            }
            Step::Reopen { by } => {
                let c = cs[by].take().unwrap();
                c.settle();
                drop(c);
                let db = Database::open_with_oplog(&dir.db(by as u32 + 1), OPLOG).map_err(|e| format!("reopen: {e:?}"))?;
                db.engine().set_peer_id(by as u32 + 1);
                cs[by] = Some(attach(db, &url, &cur(by)));
            }
            Step::Reclaim => {
                host_db.engine().reclaim_sync_ops();
            }
            Step::Upsert { by, .. } | Step::Rename { by, .. } | Step::Delete { by, .. } | Step::Retitle { by, .. } => {
                cs[by].as_ref().unwrap().write(s)
            }
        }
    }
    // 落ち着くまで: 全員が出して受ける
    for _ in 0..4 {
        for c in cs.iter().flatten() {
            c.push();
            let b = c.pull();
            if b > 0 {
                notes.push(format!("client {} が最後の周で {b} 回取り直した", c.db.engine().peer_id()));
            }
        }
    }
    let mut answers: Vec<(String, Answer)> = Vec::new();
    let mut broken: Vec<String> = Vec::new();
    for (i, c) in cs.iter().flatten().enumerate() {
        if c.t.rebase_required() {
            broken.push(format!("client {} が STALE を受けた", i + 1));
        }
        broken.extend(c.db.engine().verify_invariants().violations.into_iter().map(|v| format!("client {}: {v}", i + 1)));
        answers.push((format!("client {}", i + 1), rows(&c.db)));
    }
    broken.extend(host_db.engine().verify_invariants().violations.into_iter().map(|v| format!("host: {v}")));
    answers.push(("host".into(), rows(&host_db)));
    drop(cs);
    drop(host);
    drop(host_db);
    if !broken.is_empty() {
        return Err(format!("不変量の違反\n  {}\n  notes: {notes:?}", broken.join("\n  ")));
    }
    // 同じ名前の行が 2 つ
    for (who, a) in &answers {
        let mut names: Vec<_> = a.iter().filter_map(|r| r.0.clone()).map(|v| format!("{v:?}")).collect();
        let n = names.len();
        names.dedup();
        if names.len() != n {
            return Err(format!("{who} に同じ名前の行が 2 つ: {a:?}\n  notes: {notes:?}"));
        }
    }
    if answers.iter().all(|(_, x)| *x == answers[0].1) {
        if !notes.is_empty() {
            BOOTS.fetch_add(1, Ordering::Relaxed);
        }
        return Ok(answers.swap_remove(0).1);
    }
    let mut msg = String::from("答えが割れる\n");
    for (who, rows) in &answers {
        msg.push_str(&format!("  {who}: {rows:?}\n"));
    }
    msg.push_str(&format!("  notes: {notes:?}\n"));
    Err(msg)
}

/// 失敗する手順を、 1 つずつ抜いて縮める (同じ種類の失敗でなくてもよい: どれも食い違い)。
fn shrink(mut steps: Vec<Step>, mode: Mode) -> Vec<Step> {
    loop {
        let mut smaller = None;
        for i in 0..steps.len() {
            let mut t = steps.clone();
            t.remove(i);
            // 揺れで落ちた物を縮めない: 2 回とも落ちること
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

fn fuzz(mode: Mode) {
    let cases = env_u64("REVIEW_CASES", 20);
    let seed = env_u64("REVIEW_SEED", 1);
    let max_fail = env_u64("REVIEW_MAX_FAIL", 3);
    let mut fails = 0;
    for case in 0..cases {
        let mut rng = Rng(seed.wrapping_mul(1_000_003).wrapping_add(case));
        let steps = gen_steps(&mut rng, mode);
        if let Err(msg) = run(&steps, mode) {
            let again = run(&steps, mode).is_err();
            println!("=== {mode:?} seed {seed} case {case}: 失敗 (もう 1 回: {})\n手順 {steps:?}\n{msg}", if again { "失敗" } else { "通る" });
            if again {
                let small = shrink(steps, mode);
                println!("--- 縮めた手順 {small:?}\n{}", run(&small, mode).err().unwrap_or_else(|| "(縮めた手順は今回通った)".into()));
            }
            fails += 1;
            if fails >= max_fail {
                break;
            }
        }
    }
    println!("{mode:?}: seed {seed} cases {cases} 失敗 {fails} (取り直しの起きた通った case {})", BOOTS.load(Ordering::Relaxed));
    assert_eq!(fails, 0, "{mode:?}: {fails} 件の食い違い (seed {seed}, cases {cases})");
}

fn want(mode: &str) -> bool {
    std::env::var("REVIEW_MODE").map_or(true, |m| m == mode)
}

#[test]
fn host_model_plain() {
    if want("plain") {
        fuzz(Mode::Plain);
    }
}

#[test]
fn host_model_line() {
    if want("line") {
        fuzz(Mode::Line);
    }
}

#[test]
fn host_model_reopen() {
    if want("reopen") {
        fuzz(Mode::Reopen);
    }
}

#[test]
fn host_model_disjoint() {
    if want("disjoint") {
        fuzz(Mode::Disjoint);
    }
}

#[test]
fn host_model_all() {
    if want("all") {
        fuzz(Mode::All);
    }
}
