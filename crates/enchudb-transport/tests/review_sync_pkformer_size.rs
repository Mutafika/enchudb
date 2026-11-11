//! review (0.37.4): `pkformer` の大きさの測り (判定なし、 数を出すだけ)。 2 台が同じ名前の行を N 個ずつ別々に作って束ねる
//! (設定の key のように、 どの端末も同じ名前を作る表)。 束ねた行は identity ごとのカード (列ごとの値) を `pkformer` に持ち、
//! `pkformer` は変わるたびにまるごと書き直す。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use enchudb::schema::Database;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 64 << 20;

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

#[test]
fn measure_pkformer_for_rows_bound_by_name() {
    let n: usize = std::env::var("REVIEW_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(2000);
    let dir = Dir(Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("review-pkformer-{}", std::process::id())));
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
    let t0 = Instant::now();
    p2.sync.pull_once(1);
    let first = t0.elapsed();
    p1.sync.pull_once(2);
    println!("[pkformer] {n} 行 (title + 200 B の body) を 2 台が別々に作って束ねた。 最初の pull {first:?}");
    println!("[pkformer] p2: pkformer {} B、 eidmap {} B + log {} B、 pktombs {} B", p2.file("pkformer"), p2.file("eidmap"), p2.file("eidmap.log"), p2.file("pktombs"));
    // 1 行だけ書き換えて pull する時の値段 (pkformer のまるごとの書き直しが入るか)
    let mut times = Vec::new();
    for round in 0..5 {
        let t = p1.db.get_table(T).unwrap();
        let e = t.where_eq("key", "setting-7").find().unwrap()[0];
        t.entity(e).set("title", format!("edit{round}").as_str()).commit().unwrap();
        p1.publish();
        let before = std::fs::metadata(Path::new(&p2.path).join("pkformer")).and_then(|m| m.modified()).ok();
        let t0 = Instant::now();
        let out = p2.sync.pull_once(1);
        let dt = t0.elapsed();
        let after = std::fs::metadata(Path::new(&p2.path).join("pkformer")).and_then(|m| m.modified()).ok();
        times.push((out.applied, dt, before != after));
    }
    println!("[pkformer] 1 行の書き換えの pull (当てた数, 時間, pkformer を書き直したか): {times:?}");
    // 全部消した後
    let t = p1.db.get_table(T).unwrap();
    for e in t.all().find().unwrap() {
        t.entity(e).delete().unwrap();
    }
    p1.publish();
    p2.sync.pull_once(1);
    p2.publish();
    p1.sync.pull_once(2);
    println!(
        "[pkformer] 全部消した後 (行 {} / {}): p2 の pkformer {} B、 pktombs {} B / p1 の pkformer {} B",
        p1.db.get_table(T).unwrap().all().count().unwrap(),
        p2.db.get_table(T).unwrap().all().count().unwrap(),
        p2.file("pkformer"),
        p2.file("pktombs"),
        p1.file("pkformer")
    );
    drop((p1, p2));
}
