//! #755: ホストでない peer が、 LWW で負けた record だけの batch を受けた後、 束ねた写像を書き出さずに cursor を進めていた。
//!
//! 2 台 (A / B) が同じ名前 k1 の行を別々に作る (同じデッキ)。 B の方が後 = A の record は B で全部 LWW に負けるが、 名前のカードは
//! A の世界番号を B の行に束ねる写像を作る。 この batch は `applied == 0` なので、 前は派生 state (eidmap) を書き出さずに cursor を
//! 進めて保存していた。 そこで落ちると 「cursor は先・写像は無い」 = A がその行を後から書き換えた record を B の行に結べない。
//!
//! 落ちた像は、 pull が返った直後の B の directory と cursor の file をそのまま写して作る (process が死んだ時に disk に残る物。
//! 写像の file は書き出した分しか無い)。 時間には頼らない。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 1 << 20;
const A: u32 = 1;
const B: u32 = 2;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue755-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
    fn db(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_string()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dst = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), &dst).unwrap();
        }
    }
}

struct Peer {
    db: Arc<Database>,
    sync: Syncer,
}

fn cursor_path(db_path: &str) -> PathBuf {
    format!("{db_path}.cursors").into()
}

fn attach(db: Arc<Database>, path: &str, id: u32, tr: &Arc<InMemoryTransport>) -> Peer {
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>).with_cursor_path(cursor_path(path));
    Peer { db, sync }
}

fn create(path: &str, id: u32, tr: &Arc<InMemoryTransport>) -> Peer {
    let mut b = Database::create_with_capacity(path, 4096).unwrap();
    // 数の列だけ: 語の record (Vocab) は受け手で「当たった」に数えるので、 Tag の列があると batch の applied が 0 にならない
    b.table(T).number("key").number("title").primary_key("key").build().unwrap();
    b.enable_sync().unwrap();
    attach(b.finish_with_oplog(OPLOG).unwrap(), path, id, tr)
}

impl Peer {
    fn upsert(&self, key: i64, title: i64) {
        self.db.get_table(T).unwrap().upsert().set("key", key).set("title", title).commit().unwrap();
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        while eng.transfer_oplog_to_sync_ops() > 0 {}
        self.sync.publish_since(Hlc::ZERO);
    }

    /// (key, title) の並び (全部の行)。
    fn rows(&self) -> Vec<(Option<Value>, Option<Value>)> {
        self.db.engine().rebuild();
        let t = self.db.get_table(T).unwrap();
        let mut eids = t.all().find().unwrap();
        eids.extend(t.all().where_ge("title", 0i64).find().unwrap());
        eids.sort_unstable();
        eids.dedup();
        let mut out: Vec<_> = eids.into_iter().map(|e| (t.entity(e).get("key"), t.entity(e).get("title"))).collect();
        out.sort_by_key(|r| format!("{r:?}"));
        out
    }
}

fn row(key: i64, title: i64) -> (Option<Value>, Option<Value>) {
    (Some(Value::Number(key)), Some(Value::Number(title)))
}

/// 名前 (key の値)。
const K1: i64 = 1;
const FROM_A: i64 = 10;
const FROM_B: i64 = 20;
const FROM_A_LATER: i64 = 30;

/// B が A の負ける record だけを受けた直後に落ち、 その後の A の書き換えを受ける。 `bootstrap` なら state の転写で受ける。
fn losing_batch_then_crash(bootstrap: bool) {
    let dir = Dir::new(if bootstrap { "bootstrap" } else { "pull" });
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(A);
    tr.register_peer(B);
    let a = create(&dir.db("a"), A, &tr);
    let b = create(&dir.db("b"), B, &tr);
    if bootstrap {
        a.sync.serve_state();
    }

    // A が先に k1 を作り、 B が後から同じ名前の行を作る (B の HLC の方が新しい)
    a.upsert(K1, FROM_A);
    std::thread::sleep(Duration::from_millis(3));
    b.upsert(K1, FROM_B);

    // B が A から受ける: A の record は全部負ける (写像だけができる)
    let out = if bootstrap { b.sync.bootstrap_pull(A).expect("前提: A の state が取れる").outcome } else { b.sync.pull_once(A) };
    assert!(out.received > 0, "前提: A の record が届いた ({out:?})");
    assert_eq!(out.applied, 0, "前提: A の record は全部 LWW で負けた ({out:?})");
    assert_eq!(b.rows(), vec![row(K1, FROM_B)], "前提: B の行は 1 つで、 B の値のまま");
    assert!(cursor_path(&dir.db("b")).exists(), "前提: cursor は保存された");

    // ここで B が落ちた像 (disk にある物だけ)
    copy_dir(Path::new(&dir.db("b")), Path::new(&dir.db("b2")));
    std::fs::copy(cursor_path(&dir.db("b")), cursor_path(&dir.db("b2"))).unwrap();

    // A がその行を書き換える (いちばん新しい)
    std::thread::sleep(Duration::from_millis(3));
    a.upsert(K1, FROM_A_LATER);

    // 対照: 落ちなかった B は、 A の書き換えを自分の行に受ける
    let live = b.sync.pull_once(A);
    assert!(live.applied > 0, "前提: 落ちなかった B は A の書き換えを当てる ({live:?})");
    assert_eq!(b.rows(), vec![row(K1, FROM_A_LATER)], "前提: 落ちなかった B の答え");

    // 落ちた像を開いて、 同じ書き換えを受ける
    let b2 = attach(Database::open_with_oplog(&dir.db("b2"), OPLOG).unwrap(), &dir.db("b2"), B, &tr);
    assert_eq!(b2.rows(), vec![row(K1, FROM_B)], "前提: 像の B は受けた直後の中身");
    let back = b2.sync.pull_once(A);
    assert_eq!(
        b2.rows(),
        vec![row(K1, FROM_A_LATER)],
        "落ちた後の B が A の書き換えを自分の行に結べていない (pull = {back:?})"
    );
}

#[test]
fn mapping_made_by_a_losing_batch_survives_a_crash() {
    losing_batch_then_crash(false);
}

/// state の転写 (`bootstrap_pull`) で受けた時も同じ。
#[test]
fn mapping_made_by_a_losing_state_batch_survives_a_crash() {
    losing_batch_then_crash(true);
}
