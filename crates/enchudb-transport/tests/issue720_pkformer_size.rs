//! #720: 2 台が同じ名前の行を別々に作って束ねた表 (設定の key のように、 どの端末も同じ名前を作る表) で、 `pkformer` が 1 行の
//! 書き換えの pull のたびにまるごと書き直され、 全行を消しても縮まなかった。 `Syncer` を通した形で確かめる (追記の log の
//! 中身と、 落ちた後・閉じた後の読み直しは `crates/enchudb-engine/tests/issue720_pkformer_log.rs`)。
//!
//! 行の数は `ISSUE720_ROWS` (既定 400、 issue の測りは 2000)。 `--nocapture` で大きさと時間を出す。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use enchudb::schema::Database;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 64 << 20;

/// 試験の directory。 Drop で消す。
struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Peer {
    path: String,
    db: Arc<Database>,
    sync: Syncer,
}

fn make(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>) -> Peer {
    let path = dir.0.join(format!("p{id}")).to_str().unwrap().to_string();
    let mut b = Database::create_with_capacity(&path, 200_000).unwrap();
    b.table(T).tag("key").tag("title").leaf("body").primary_key("key").with_capacity(20_000).build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    Peer { path, db, sync }
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
    fn file(&self, name: &str) -> u64 {
        std::fs::metadata(Path::new(&self.path).join(name)).map(|m| m.len()).unwrap_or(0)
    }
}

/// `pkformer` と `pkformer.log` の中身。
fn sidecars(p: &Peer) -> (Vec<u8>, u64) {
    (std::fs::read(Path::new(&p.path).join("pkformer")).unwrap_or_default(), p.file("pkformer.log"))
}

#[test]
fn a_one_row_pull_does_not_rewrite_pkformer_and_deleted_rows_leave_it() {
    let n: usize = std::env::var("ISSUE720_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(400);
    let dir = Dir(std::env::temp_dir().join(format!("enchudb-issue720-size-{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&dir.0);
    std::fs::create_dir_all(&dir.0).unwrap();
    let tr = Arc::new(InMemoryTransport::new());
    tr.register_peer(1);
    tr.register_peer(2);
    let (p1, p2) = (make(&dir, 1, &tr), make(&dir, 2, &tr));
    let body = "x".repeat(200);
    for p in [&p1, &p2] {
        let t = p.db.get_table(T).unwrap();
        for i in 0..n {
            t.insert().set("key", format!("setting-{i}")).set("title", format!("v{i}")).set("body", body.as_str()).commit().unwrap();
        }
        p.publish();
    }
    p2.sync.pull_once(1);
    p1.sync.pull_once(2);
    assert_eq!(p2.db.get_table(T).unwrap().all().count().unwrap(), n, "前提: 同じ名前の行は 1 行に束ねられた");
    let (snapshot, log) = sidecars(&p2);
    let full = snapshot.len() as u64 + log;
    println!("[issue720] {n} 行を束ねた後: pkformer {} B + log {log} B", snapshot.len());
    // 束ねた行はカードを Leaf の中身ごと持つ (2 identity × 200 B)
    assert!(full > 400 * n as u64, "前提: pkformer が束ねた行のカードを持つ ({full} B)");

    // 1 行だけ書き換えて pull する: 足すのはその行の分だけ
    let mut times = Vec::new();
    for round in 0..5 {
        let t = p1.db.get_table(T).unwrap();
        let e = t.where_eq("key", "setting-7").find().unwrap()[0];
        t.entity(e).set("title", format!("edit{round}").as_str()).commit().unwrap();
        p1.publish();
        let (before, before_log) = sidecars(&p2);
        let t0 = Instant::now();
        let out = p2.sync.pull_once(1);
        times.push(t0.elapsed());
        assert!(out.applied >= 1, "前提: 書き換えが当たった ({out:?})");
        let (after, after_log) = sidecars(&p2);
        let wrote = if after == before { after_log - before_log } else { after.len() as u64 + after_log };
        assert!(wrote > 0, "{round} 回目: 変わったカードを書き出していない");
        assert!(wrote < 4_096, "{round} 回目: 1 行の書き換えの pull で pkformer に {wrote} B 書いた (全体 {full} B)");
    }
    println!("[issue720] 1 行の書き換えの pull: {times:?}");

    // 全部消す
    let t = p1.db.get_table(T).unwrap();
    for e in t.all().find().unwrap() {
        t.entity(e).delete().unwrap();
    }
    p1.publish();
    p2.sync.pull_once(1);
    p2.publish();
    p1.sync.pull_once(2);
    assert_eq!(p2.db.get_table(T).unwrap().all().count().unwrap(), 0, "前提: 全部の行が消えた");
    let (snapshot, log) = sidecars(&p2);
    let after = snapshot.len() as u64 + log;
    println!("[issue720] 全部消した後: pkformer {} B + log {log} B", snapshot.len());
    assert!(after < full / 4, "全行を消した後も pkformer + log が {after} B (前は {full} B)");
    drop((p1, p2));
}
