//! 主キーの改名 (東京 → 新東京) を混ぜても、 受け手が書き手から pull する順によらず同じ答えになるか (PK_IDENTITY_PLAN.md の 11-1 節)。
//!
//! 答えをどう見せるか (改名を知らずに古い名前で作った行を、 改名した行に束ねるか) はここでは決めず、 順によらないことだけを見る
//! (`sync_delete_before_create::newer_write_after_the_delete_lands_the_same_in_any_order` と同じ)。 書き手どうしも、 互いを
//! pull し合った後に同じ答えになるか見る。
//!
//! 束ねは 「受け取った時の今の名前で、 同じ名前の行を引く」 ので、 名前が変わると届く順で答えが割れうる。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 1 << 20;
/// 書き手は 1..=AUTHORS、 受け手は RECEIVER_BASE 以降
const AUTHORS: u32 = 3;
const RECEIVER_BASE: u32 = 10;

/// test ごとの directory。 Drop で消す (DB と Syncer を落とした後に消えるよう、 最初に作って最後に落とす)。
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("pk-rename-{tag}-{}", std::process::id()));
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
    /// この peer が upsert で最初に作った行 (`row_of_any`)
    first_row: std::sync::OnceLock<u64>,
}

fn make(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>) -> Peer {
    let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
    b.table(T).tag("key").tag("title").primary_key("key").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    Peer { db, sync, first_row: std::sync::OnceLock::new() }
}

impl Peer {
    /// `tests/issue141_pk_dedupe.rs` の `publish_all` と同じ手順。
    fn publish(&self) {
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        eng.transfer_oplog_to_sync_ops();
        self.sync.publish_since(Hlc::ZERO);
        later();
    }

    fn pull(&self, from: u32) {
        self.sync.pull_once(from);
    }

    fn upsert(&self, key: &str, title: &str) {
        let e = self.db.get_table(T).unwrap().upsert().set("key", key).set("title", title).commit().unwrap();
        let _ = self.first_row.set(e);
        self.publish();
    }

    fn row_of(&self, key: &str) -> u64 {
        self.db.engine().rebuild();
        let found = self.db.get_table(T).unwrap().where_eq("key", key).find().unwrap();
        assert_eq!(found.len(), 1, "peer {}: 主キー {key} の行が {} 個", self.db.engine().peer_id(), found.len());
        found[0]
    }

    /// 主キーの値を書き換える (改名)。
    fn rename(&self, from: &str, to: &str) {
        let e = self.row_of(from);
        self.db.get_table(T).unwrap().entity(e).set("key", to).commit().unwrap();
        self.publish();
    }

    fn retitle(&self, key: &str, title: &str) {
        let e = self.row_of(key);
        self.db.get_table(T).unwrap().entity(e).set("title", title).commit().unwrap();
        self.publish();
    }

    /// この peer が upsert で最初に作った行 (消えた後でも番号を引くため、 作った時に覚えておく)。
    fn row_of_any(&self) -> u64 {
        self.first_row.get().copied().expect("まだ行を作っていない")
    }

    fn delete(&self, key: &str) {
        let e = self.row_of(key);
        self.db.get_table(T).unwrap().entity(e).delete().unwrap();
        self.publish();
    }

    /// 表の行 (主キーの無い行も) を (key, title) で並べる。
    fn rows(&self) -> Vec<(Option<Value>, Option<Value>)> {
        self.db.engine().rebuild();
        let t = self.db.get_table(T).unwrap();
        let mut out: Vec<_> = t
            .all()
            .find()
            .unwrap()
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

/// HLC の順を作る (同じ process の時計なので、 間を空ければ後に書いた方が新しい)。
fn later() {
    std::thread::sleep(Duration::from_millis(3));
}

fn permutations(n: u32) -> Vec<Vec<u32>> {
    if n == 0 {
        return vec![vec![]];
    }
    let mut out = Vec::new();
    for p in permutations(n - 1) {
        for i in 0..=p.len() {
            let mut q = p.clone();
            q.insert(i, n);
            out.push(q);
        }
    }
    out
}

/// 書き手 1..=AUTHORS に `script` を書かせ、 受け手を書き手の全部の順に 1 台ずつ作って pull させる。 書き手どうしも互いを pull する。
/// 答えが全部同じなら None、 割れたら (順, 答え) の並び。
fn run(tag: &str, script: impl Fn(&[Peer])) -> Option<Vec<(String, Vec<(Option<Value>, Option<Value>)>)>> {
    let dir = Dir::new(tag);
    let orders = permutations(AUTHORS);
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=AUTHORS {
        tr.register_peer(id);
    }
    for k in 0..orders.len() as u32 {
        tr.register_peer(RECEIVER_BASE + k);
    }
    let authors: Vec<Peer> = (1..=AUTHORS).map(|id| make(&dir, id, &tr)).collect();
    script(&authors);

    let mut answers = Vec::new();
    for (k, order) in orders.iter().enumerate() {
        let r = make(&dir, RECEIVER_BASE + k as u32, &tr);
        for &from in order {
            r.pull(from);
        }
        answers.push((format!("受け手 {order:?}"), r.rows()));
    }
    // 書き手どうし: 2 周 pull し合う (1 周目に受けた分の書き戻しは無いが、 念のため)
    for _ in 0..2 {
        for a in &authors {
            for from in 1..=AUTHORS {
                if from != a.db.engine().peer_id() {
                    a.pull(from);
                }
            }
        }
    }
    for a in &authors {
        answers.push((format!("書き手 {}", a.db.engine().peer_id()), a.rows()));
    }
    let same = answers.iter().all(|(_, x)| *x == answers[0].1);
    drop(authors);
    if same {
        eprintln!("{tag}: 揃う {:?}", answers[0].1);
        None
    } else {
        Some(answers)
    }
}

fn check(tag: &str, script: impl Fn(&[Peer])) {
    if let Some(answers) = run(tag, script) {
        let mut msg = format!("{tag}: 届く順で答えが割れる\n");
        for (who, rows) in &answers {
            msg.push_str(&format!("  {who}: {rows:?}\n"));
        }
        panic!("{msg}");
    }
}

/// 対照: 改名なし。 A と B が別々に z を作る (#141)。
#[test]
fn same_name_from_two_authors() {
    check("same", |p| {
        p[0].upsert("z", "a");
        p[1].upsert("z", "b");
    });
}

/// A が z を作って y に改名。 B は A を見ずに z を作る (改名を知らない作成)。
#[test]
fn unaware_create_after_rename() {
    check("unaware-create", |p| {
        p[0].upsert("z", "a");
        p[0].rename("z", "y");
        p[1].upsert("z", "b");
    });
}

/// B は A の z を受けた後、 A の改名を知らずに z の title を書く (eid で書くので同じ行に届くはず)。
#[test]
fn unaware_write_after_rename() {
    check("unaware-write", |p| {
        p[0].upsert("z", "a");
        p[1].pull(1);
        p[0].rename("z", "y");
        p[1].retitle("z", "b");
    });
}

/// A の z を、 既に B が持つ名前 y に改名する。
#[test]
fn rename_into_an_existing_name() {
    check("into-existing", |p| {
        p[0].upsert("z", "a");
        p[1].upsert("y", "b");
        p[0].rename("z", "y");
    });
}

/// A と B が同じ行を同時に別の名前に改名する。
#[test]
fn concurrent_renames() {
    check("concurrent", |p| {
        p[0].upsert("z", "a");
        p[1].pull(1);
        p[0].rename("z", "y");
        p[1].rename("z", "x");
    });
}

/// A が z を y に改名して消す。 B は A を見ずに z を作る。 C は A の z を受けて (改名を知らずに) title を書く。
#[test]
fn rename_then_delete_with_unaware_writers() {
    check("rename-delete", |p| {
        p[0].upsert("z", "a");
        p[2].pull(1);
        p[0].rename("z", "y");
        p[0].delete("y");
        p[1].upsert("z", "b");
        p[2].retitle("z", "c");
    });
}

/// 改名して戻す (z → y → z)。 B は A を見ずに y を作る。
#[test]
fn rename_back_with_an_unaware_create() {
    check("rename-back", |p| {
        p[0].upsert("z", "a");
        p[0].rename("z", "y");
        p[0].rename("y", "z");
        p[1].upsert("y", "b");
    });
}

/// 前の名前は開き直しても残る (`pkformer`)。 受け手 R が改名を受けてから開き直し、 その後で改名を知らない B の z を受ける。
/// 開き直さない受け手と同じ答え (B の z を改名した行に束ねる)。
#[test]
fn former_names_survive_a_reopen() {
    let dir = Dir::new("reopen");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 10, 11] {
        tr.register_peer(id);
    }
    let (a, b) = (make(&dir, 1, &tr), make(&dir, 2, &tr));
    let (r, plain) = (make(&dir, 10, &tr), make(&dir, 11, &tr));
    a.upsert("z", "a");
    a.rename("z", "y");
    b.upsert("z", "b");
    r.pull(1);
    plain.pull(1);
    let path = dir.db(10);
    drop(r);
    let db = Database::open_with_oplog(&path, OPLOG).unwrap();
    db.engine().set_peer_id(10);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    let r = Peer { db, sync, first_row: std::sync::OnceLock::new() };
    r.pull(2);
    plain.pull(2);
    assert_eq!(plain.rows(), vec![(Some(Value::Text("z".into())), Some(Value::Text("b".into())))], "前提: B の z を改名した行に束ねる");
    assert_eq!(r.rows(), plain.rows(), "開き直した受け手が前の名前を忘れた");
}

/// 消えた行の slot を別の行に使い回したら、 その行の前の名前は外す (外さないと、 改名を知らない peer の古い名前の行が、
/// slot を使い回した無関係な行に束なる)。 受け手 R の表は 4 行分: A の行 (z → y に改名して消す) の slot を、 R が自分で
/// 作る 4 行目が使い回す。 その後で B の z を受けても、 R の行 w は書き換わらない。
#[test]
fn a_reused_slot_drops_its_former_names() {
    let dir = Dir::new("reuse");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 10] {
        tr.register_peer(id);
    }
    let (a, b) = (make(&dir, 1, &tr), make(&dir, 2, &tr));
    let r = {
        let mut bl = Database::create_with_capacity(&dir.db(10), 4096).unwrap();
        bl.table(T).tag("key").tag("title").primary_key("key").with_capacity(4).build().unwrap();
        bl.enable_sync().unwrap();
        let db = bl.finish_with_oplog(OPLOG).unwrap();
        db.engine().set_peer_id(10);
        let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
        Peer { db, sync, first_row: std::sync::OnceLock::new() }
    };
    a.upsert("z", "a");
    a.rename("z", "y");
    a.delete("y");
    b.upsert("z", "b");
    r.pull(1);
    assert_eq!(r.rows(), vec![], "前提: A の行は消えた");
    let a_slot = r.db.engine().resolve_remote_eid_existing(a.row_of_any()).map(enchudb::eid_local);
    assert!(a_slot.is_some(), "前提: R に A の行の写像がある");
    let t = r.db.get_table(T).unwrap();
    let mut mine = Vec::new();
    for k in ["p", "q", "s", "w"] {
        mine.push(t.insert().set("key", k).set("title", "own").commit().unwrap());
    }
    assert_eq!(mine.last().map(|e| enchudb::eid_local(*e)), a_slot, "前提: R の 4 行目 (w) が A の行の slot を使い回す ({mine:?})");
    r.pull(2);
    let rows = r.rows();
    let text = |s: &str| Some(Value::Text(s.into()));
    assert!(rows.contains(&(text("w"), text("own"))), "使い回した slot の行が B の z に書き換わった: {rows:?}");
    assert!(rows.contains(&(text("z"), text("b"))), "B の z が無い: {rows:?}");
}
