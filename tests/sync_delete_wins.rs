//! 削除優先 (ユーザの決め、 2026-10-21): 消した行 (eid) 宛ての書き込みは、 削除より新しくても捨てる。 消した後に新しく足した
//! 行 (新しい eid) は残す。 名前 (主キー) が同じ行は同じ行なので、 削除より前に別々に作られた同じ名前の行も一緒に消える。
//!
//! 書き手 3 台に書かせ、 受け手が書き手から pull する 3! 通りの順と、 書き手どうしの pull し合いで、 答えが決めた形になるかを見る。

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
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("delete-wins-{tag}-{}", std::process::id()));
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

    /// 表の行を (key, title) で並べる。 名前 (主キー) の無いカードの塊も見る (title のある entity も数える。 `all()` は名前の
    /// ある行だけなので、 それだけだと消したはずの行の書き換えが名前の無い塊で残っても見えない)。
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
/// 書き手 1..=AUTHORS に `script` を書かせ、 受け手を書き手の全部の順に 1 台ずつ作って pull させる。 書き手どうしも互いを pull する。
/// (誰の答えか, 答え) の並び。
fn run(tag: &str, script: impl Fn(&[Peer])) -> Vec<(String, Vec<(Option<Value>, Option<Value>)>)> {
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
    drop(authors);
    answers
}

/// 全部の受け手と書き手の答えが `want`。
fn expect(tag: &str, want: Vec<(Option<Value>, Option<Value>)>, script: impl Fn(&[Peer])) {
    let answers = run(tag, script);
    if answers.iter().any(|(_, x)| *x != want) {
        let mut msg = format!("{tag}: 答えが {want:?} にならない\n");
        for (who, rows) in &answers {
            msg.push_str(&format!("  {who}: {rows:?}\n"));
        }
        panic!("{msg}");
    }
}

fn text(s: &str) -> Option<Value> {
    Some(Value::Text(s.into()))
}

/// 買い物リストの形: A が z を作り、 B が受ける。 A が z を消す。 B は (消されたのを知らずに) z の title を書き換える。
/// 書き換えは消した行 (A の eid) 宛てなので、 削除より新しくても捨てる。
#[test]
fn an_unaware_edit_to_a_deleted_row_is_dropped() {
    expect("edit", vec![], |p| {
        p[0].upsert("z", "a");
        p[1].pull(1);
        p[0].delete("z");
        p[1].retitle("z", "b");
    });
}

/// 消したのを見てから足し直した行は新しい行 (新しい eid) なので残る。
#[test]
fn a_row_added_again_after_seeing_the_delete_lives() {
    expect("readd", vec![(text("z"), text("b"))], |p| {
        p[0].upsert("z", "a");
        p[1].pull(1);
        p[0].delete("z");
        p[1].pull(1);
        p[1].upsert("z", "b");
    });
}

/// 削除より前に別々に作られた同じ名前の行 (B の z) も同じ行なので、 A の削除で消える。
#[test]
fn a_same_name_row_made_before_the_delete_goes_with_it() {
    expect("before", vec![], |p| {
        p[0].upsert("z", "a");
        p[1].upsert("z", "b");
        p[0].delete("z");
    });
}

/// 上の形で、 B が削除を知らずに自分の z を書き換えても戻らない (B の z は削除より前に作られた同じ行)。
#[test]
fn an_unaware_edit_to_a_same_name_row_is_dropped_too() {
    expect("before-edit", vec![], |p| {
        p[0].upsert("z", "a");
        p[1].upsert("z", "b");
        p[0].delete("z");
        p[1].retitle("z", "b2");
    });
}

/// 作成より先に削除が届く形 (#490): A が x を作り、 B が受けて消す。 C は A の x を受けた後、 削除を知らずに書き換える。
#[test]
fn an_unaware_edit_after_an_early_delete_is_dropped() {
    expect("early", vec![], |p| {
        p[0].upsert("x", "a");
        p[1].pull(1);
        p[2].pull(1);
        p[1].delete("x");
        p[2].retitle("x", "c");
    });
}

/// 消したのを見てから、 2 人が別々に同じ名前で足し直した行は、 同じ行として残る (名前の削除はその前からある行だけを消す。 足し直した
/// 行どうしを束ねる時に、 前の削除を当てて消さない)。
#[test]
fn two_rows_added_again_after_the_delete_live_as_one() {
    expect("readd-twice", vec![(text("z"), text("c"))], |p| {
        p[0].upsert("z", "a");
        p[1].pull(1);
        p[2].pull(1);
        p[0].delete("z");
        p[1].pull(1);
        p[2].pull(1);
        p[1].upsert("z", "b");
        p[2].upsert("z", "c");
    });
}

/// 消した行への、 削除を知らない Leaf の列 (wire では TieLeaf) の書き込みも捨てる。
#[test]
fn an_unaware_leaf_edit_to_a_deleted_row_is_dropped() {
    let dir = Dir::new("leaf");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 10, 11] {
        tr.register_peer(id);
    }
    let peer = |id: u32| {
        let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
        b.table("notes").tag("key").leaf("memo").primary_key("key").build().unwrap();
        b.enable_sync().unwrap();
        let db = b.finish_with_oplog(OPLOG).unwrap();
        db.engine().set_peer_id(id);
        let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
        Peer { db, sync, first_row: std::sync::OnceLock::new() }
    };
    let (a, b) = (peer(1), peer(2));
    let row = a.db.get_table("notes").unwrap().upsert().set("key", "z").set("memo", "a").commit().unwrap();
    a.publish();
    b.pull(1);
    a.db.get_table("notes").unwrap().entity(row).delete().unwrap();
    a.publish();
    let on_b = b.db.engine().resolve_remote_eid_existing(row).expect("前提: B に A の行の写像");
    b.db.get_table("notes").unwrap().entity(on_b).set("memo", "b").commit().unwrap();
    b.publish();
    let (r1, r2) = (peer(10), peer(11));
    r1.pull(1);
    r1.pull(2);
    r2.pull(2);
    r2.pull(1);
    a.pull(2);
    b.pull(1);
    for (name, p, e) in [
        ("A", &a, Some(row)),
        ("B", &b, Some(on_b)),
        ("R1 (A → B)", &r1, r1.db.engine().resolve_remote_eid_existing(row)),
        ("R2 (B → A)", &r2, r2.db.engine().resolve_remote_eid_existing(row)),
    ] {
        let e = e.expect("A の行の写像が無い");
        let memo = p.db.engine().get_text_owned(e, "notes.memo");
        assert_eq!(memo, None, "{name}: 消した行への Leaf の書き込みが残った");
        assert!(!p.db.engine().is_live(e), "{name}: 消した行が生き返った");
    }
}
