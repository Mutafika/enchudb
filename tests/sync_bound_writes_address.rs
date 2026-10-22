//! #613: 名前で束ねた後の書き込みの宛名は、 行の元の identity のまま (束ねた相手の identity に替えない)。
//!
//! 前は束ねると行の逆写像 (書き込みの宛名) が束ねた identity に替わり、 束ねた後の書き込みが、 その identity の作成を受けていない
//! 受け手 (束ねた相手を pull しない受け手) で別の行になった。 元の identity の作成は同じ書き手の流れの前にあるので、 書き手を pull する
//! 受け手は必ず先に受けている。

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
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("bound-addr-{tag}-{}", std::process::id()));
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

    fn upsert(&self, key: &str, title: &str) {
        self.db.get_table(T).unwrap().upsert().set("key", key).set("title", title).commit().unwrap();
        self.publish();
    }

    fn row_of(&self, key: &str) -> u64 {
        self.db.engine().rebuild();
        let found = self.db.get_table(T).unwrap().where_eq("key", key).find().unwrap();
        assert_eq!(found.len(), 1, "peer {}: 名前 {key} の行が {} 個", self.db.engine().peer_id(), found.len());
        found[0]
    }

    /// 表の行 (名前の無い塊も) を (key, title) で並べる。
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

fn text(s: &str) -> Option<Value> {
    Some(Value::Text(s.into()))
}

/// A が自分の k2 に C の k2 を束ねてから、 title を書き換えて改名する。 A だけを pull する受け手 R (C の作成を受けない) も、 A の行
/// 1 つだけを持つ。 前は A の書き込みが C の世界番号宛てに出て、 R で名前の無い行 / 改名だけの行が別にできた。
#[test]
fn writes_after_a_bind_reach_a_receiver_that_never_sees_the_bound_peer() {
    let dir = Dir::new("own");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 3, 10] {
        tr.register_peer(id);
    }
    let (a, c, r) = (make(&dir, 1, &tr), make(&dir, 3, &tr), make(&dir, 10, &tr));
    c.upsert("k2", "c");
    a.upsert("k2", "a");
    a.sync.pull_once(3);
    let e = a.row_of("k2");
    a.db.get_table(T).unwrap().entity(e).set("title", "after-bind").commit().unwrap();
    a.publish();
    a.db.get_table(T).unwrap().entity(e).set("key", "k0").commit().unwrap();
    a.publish();
    r.sync.pull_once(1);
    assert_eq!(r.rows(), vec![(text("k0"), text("after-bind"))], "A だけを pull する受け手で、 束ねた後の書き込みが別の行になった");
}

/// A が B の k2 (翻訳した行) に C の k2 を束ねてから書き換える。 A と B だけを pull する受け手 R も 1 行 (宛名は B の行のまま)。
#[test]
fn writes_after_binding_into_a_translated_row_keep_its_original_address() {
    let dir = Dir::new("translated");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 3, 10] {
        tr.register_peer(id);
    }
    let (a, b, c, r) = (make(&dir, 1, &tr), make(&dir, 2, &tr), make(&dir, 3, &tr), make(&dir, 10, &tr));
    b.upsert("k2", "b");
    c.upsert("k2", "c");
    a.sync.pull_once(2);
    a.sync.pull_once(3);
    let e = a.row_of("k2");
    a.db.get_table(T).unwrap().entity(e).set("title", "after-bind").commit().unwrap();
    a.publish();
    r.sync.pull_once(2);
    r.sync.pull_once(1);
    assert_eq!(r.rows(), vec![(text("k2"), text("after-bind"))], "A と B だけを pull する受け手で、 束ねた後の書き込みが別の行になった");
}
