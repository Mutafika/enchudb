//! review (0.37.4): 取り直し (`bootstrap_pull`) の ghost sweep と、 名前で束ねた行。
//!
//! author の state (`Engine::state_records`) は 「自分が author した行だけ (翻訳した行は飛ばす)」。 author が自分の行に相手の
//! 同じ名前の行を束ねた後、 その行が state に出るか。 出なければ、 受け手は 「state に無い author の行 = author が消した」 と見て
//! 消し (`Engine::delete`、 自分の oplog に載る)、 その削除が全員に配られる。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 1 << 20;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("review-boot-sweep-{tag}-{}", std::process::id()));
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
    sync.serve_state();
    Peer { db, sync }
}

type Answer = Vec<(Option<Value>, Option<Value>)>;

impl Peer {
    fn publish(&self) {
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        eng.transfer_oplog_to_sync_ops();
        self.sync.publish_since(Hlc::ZERO);
        std::thread::sleep(Duration::from_millis(3));
    }
    fn upsert(&self, k: &str, v: &str) {
        self.db.get_table(T).unwrap().upsert().set("key", k).set("title", v).commit().unwrap();
        self.publish();
    }
    fn rows(&self) -> Answer {
        self.db.engine().rebuild();
        let t = self.db.get_table(T).unwrap();
        let mut out: Answer = t
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

fn state_of(p: &Peer) -> Vec<String> {
    let (recs, _) = p.db.engine().state_records();
    recs.iter().map(|r| format!("{:?}", r.op)).collect()
}

/// 2 台が同じ名前 k を別々に作り、 互いに受けて束ねる。 その後、 受け手 r が書き手 1 を取り直す。
#[test]
fn a_bootstrap_does_not_sweep_a_row_the_author_bound_by_name() {
    let dir = Dir::new("m");
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=3 {
        tr.register_peer(id);
    }
    let (p1, p2, r) = (make(&dir, 1, &tr), make(&dir, 2, &tr), make(&dir, 3, &tr));
    p1.upsert("k", "from1");
    p1.upsert("other", "only1");
    p2.upsert("k", "from2");
    r.sync.pull_once(1);
    r.sync.pull_once(2);
    let before = r.rows();
    println!("[sweep] 取り直しの前の r: {before:?}");
    println!("[sweep] 束ねる前の書き手 1 の state: {:?}", state_of(&p1));
    // 書き手 1 が 2 の行を受けて、 自分の k に束ねる
    p1.sync.pull_once(2);
    p2.sync.pull_once(1);
    println!("[sweep] 束ねた後の書き手 1 の行: {:?}", p1.rows());
    println!("[sweep] 束ねた後の書き手 1 の state: {:?}", state_of(&p1));
    // r が書き手 1 を取り直す (抜けを言われた時にアプリが呼ぶ道)
    let out = r.sync.bootstrap_pull(1).expect("state を取れない");
    println!("[sweep] bootstrap_pull(1): swept {} / {:?}", out.swept, out.outcome);
    let after = r.rows();
    println!("[sweep] 取り直しの後の r: {after:?}");
    // r の削除が配られた後の全員
    r.publish();
    for _ in 0..2 {
        p1.sync.pull_once(3);
        p2.sync.pull_once(3);
        p1.sync.pull_once(2);
        p2.sync.pull_once(1);
    }
    let (a1, a2) = (p1.rows(), p2.rows());
    println!("[sweep] 配った後: 書き手 1 {a1:?} / 書き手 2 {a2:?}");
    assert_eq!(out.swept, 0, "書き手 1 が消していない行を、 取り直しが消した");
    assert_eq!(after, before, "取り直しで r の行が変わった");
    assert_eq!(a1, before, "r の掃除の削除が書き手 1 に配られた");
}

impl Peer {
    fn rename(&self, from: &str, to: &str) {
        self.db.engine().rebuild();
        let t = self.db.get_table(T).unwrap();
        let e = t.where_eq("key", from).find().unwrap()[0];
        t.entity(e).set("key", to).commit().unwrap();
        self.publish();
    }
}

/// 自分の 2 行が 1 つに畳まれた書き手 (#612、 `sync_converge_fuzz.rs` の `a_peer_folds_its_own_rows_renamed_into_one_name` の形) を
/// 取り直す。 書き手の state は残した行の世界番号だけを出す。 受け手は畳まれた方の世界番号も同じ行に写しているので、 「state に
/// 無い世界番号 = 消えた」 と見ると、 生きている行を消す。
#[test]
fn a_bootstrap_does_not_sweep_a_row_folded_from_two_of_the_authors_rows() {
    let dir = Dir::new("f");
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=4 {
        tr.register_peer(id);
    }
    let (p1, p2, p3, r) = (make(&dir, 1, &tr), make(&dir, 2, &tr), make(&dir, 3, &tr), make(&dir, 4, &tr));
    let _ = &p1;
    p3.upsert("k1", "t0");
    p2.sync.pull_once(3);
    p3.upsert("k0", "t0");
    p2.rename("k1", "k0"); // p2 は p3 の k0 を知らない
    for _ in 0..2 {
        p3.sync.pull_once(2);
        p2.sync.pull_once(3);
    }
    p3.publish();
    r.sync.pull_once(3);
    r.sync.pull_once(2);
    let before = r.rows();
    println!("[fold] 書き手 3 の行: {:?}", p3.rows());
    println!("[fold] 書き手 3 の state: {:?}", state_of(&p3));
    println!("[fold] 取り直しの前の r: {before:?}");
    assert_eq!(before, p3.rows(), "前提: r と書き手 3 は同じ答え");
    let out = r.sync.bootstrap_pull(3).expect("state を取れない");
    let after = r.rows();
    println!("[fold] bootstrap_pull(3): swept {} / 取り直しの後の r: {after:?}", out.swept);
    r.publish();
    for _ in 0..2 {
        p3.sync.pull_once(4);
        p2.sync.pull_once(4);
    }
    println!("[fold] r の掃除を配った後: 書き手 3 {:?} / 書き手 2 {:?}", p3.rows(), p2.rows());
    assert_eq!(out.swept, 0, "書き手 3 が消していない行を、 取り直しが消した");
    assert_eq!(after, before, "取り直しで r の行が変わった");
}
