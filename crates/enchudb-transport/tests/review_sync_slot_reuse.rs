//! review (0.37.4): 2026-10-20 の S1 (#545: 自分で消した行の slot を使い回した後に、 元の書き手の削除と、 その間に第三者が作った
//! 同じ名前の行が届く) を、 0.37.4 でもう一度。 それに足して: 使い回しの後に届く、 消えた identity 宛ての改名 / 書き換え。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 1 << 20;
const CAP: u32 = 8;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("review-slot-reuse-{tag}-{}", std::process::id()));
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

/// `eids`: DB 全体の entity の数 (小さくして、 表の枠を足せないようにする)。
fn make(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>, eids: u32) -> Peer {
    let mut b = Database::create_with_capacity(&dir.db(id), eids).unwrap();
    b.table(T).tag("key").tag("title").primary_key("key").with_capacity(CAP).build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
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
        std::thread::sleep(Duration::from_millis(4));
    }
    fn upsert(&self, k: &str, v: &str) {
        self.db.get_table(T).unwrap().upsert().set("key", k).set("title", v).commit().unwrap();
        self.publish();
    }
    fn row(&self, k: &str) -> Option<u64> {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().where_eq("key", k).find().unwrap().first().copied()
    }
    fn delete(&self, k: &str) {
        let e = self.row(k).unwrap_or_else(|| panic!("消す行 {k} が無い"));
        self.db.get_table(T).unwrap().entity(e).delete().unwrap();
        self.publish();
    }
    fn rename(&self, from: &str, to: &str) {
        let e = self.row(from).unwrap();
        self.db.get_table(T).unwrap().entity(e).set("key", to).commit().unwrap();
        self.publish();
    }
    fn retitle(&self, k: &str, v: &str) {
        let e = self.row(k).unwrap();
        self.db.get_table(T).unwrap().entity(e).set("title", v).commit().unwrap();
        self.publish();
    }
    /// 表を埋めて (消えた行の slot を使い回させて) から、 埋めた行を全部消す。 戻り値: 埋められた行の数と、 その eid。
    fn churn(&self, rounds: usize) -> Vec<u64> {
        let t = self.db.get_table(T).unwrap();
        let mut seen = Vec::new();
        for r in 0..rounds {
            let mut made = Vec::new();
            for i in 0..CAP {
                match t.insert().set("key", format!("w{r}_{i}")).set("title", "w").commit() {
                    Ok(e) => made.push(e),
                    Err(_) => break,
                }
            }
            seen.extend(made.iter().copied());
            for e in made {
                t.entity(e).delete().unwrap();
            }
        }
        // 埋めた行は配らない世界にしたいが、 配っても答えの比べから外す
        seen
    }
    /// 埋めた行 (title = w) を除いた答え。 名前の無い塊も入れる。
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
            .filter(|r| r.1 != Some(Value::Text("w".into())))
            .collect();
        out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        out
    }
}

fn tr(n: u32) -> Arc<InMemoryTransport> {
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=n {
        tr.register_peer(id);
    }
    tr
}

/// X (使い回す) と X2 (使い回さない対照) と Y (自分では消さない) を比べる。 `late` は、 X / X2 が自分で z を消して (X は slot を
/// 使い回して) から届く、 A と F の書き込み。
fn scenario(tag: &str, late: impl Fn(&Peer, &Peer)) -> (Answer, Answer, Answer, bool) {
    let dir = Dir::new(tag);
    let tr = tr(5);
    // X は entity の枠を小さく (表の枠を足せない = 消えた slot を使い回す)。 他は大きく
    let (a, x, y, f, x2) =
        (make(&dir, 1, &tr, 4096), make(&dir, 2, &tr, 4096), make(&dir, 3, &tr, 4096), make(&dir, 4, &tr, 4096), make(&dir, 5, &tr, 4096));
    a.upsert("z", "a");
    for p in [&x, &y, &x2] {
        p.sync.pull_once(1);
    }
    let z_at_x = x.row("z").expect("前提: X が z を受けた");
    x.delete("z");
    x2.delete("z");
    let churned = x.churn(6);
    let reused = churned.contains(&z_at_x);
    println!("[{tag}] X が埋めた行 {} 個、 z の slot ({z_at_x:#x}) を使い回した: {reused}、 残りの eid の枠 {}", churned.len(), x.db.engine().remaining_eid_capacity());
    late(&a, &f);
    for p in [&x, &x2, &y] {
        p.sync.pull_once(1);
        p.sync.pull_once(4);
    }
    let out = (x.rows(), x2.rows(), y.rows(), reused);
    println!("[{tag}] X (使い回し) {:?}\n[{tag}] X2 (対照)   {:?}\n[{tag}] Y          {:?}", out.0, out.1, out.2);
    out
}

/// S1 そのまま: F が (X の削除と A の削除の間に) z を作り、 A が z を消す。
#[test]
fn s1_a_later_delete_and_a_third_row_after_slot_reuse() {
    let (x, x2, y, reused) = scenario("s1", |a, f| {
        f.upsert("z", "f");
        a.delete("z");
    });
    assert!(reused, "前提: X が z の slot を使い回していない (試験の組み方を直す)");
    assert_eq!(x, y, "slot を使い回した X と Y が割れた");
    assert_eq!(x2, y, "対照の X2 と Y が割れた");
}

/// 使い回しの後に、 A が (X の削除を知らずに) z を改名し、 F がその前の名前 z で作る。
#[test]
fn a_late_rename_of_the_evicted_identity_after_slot_reuse() {
    let (x, x2, y, reused) = scenario("rename", |a, f| {
        a.rename("z", "y");
        f.upsert("z", "f");
        f.upsert("y", "f2");
    });
    assert!(reused, "前提: X が z の slot を使い回していない");
    assert_eq!(x2, y, "対照の X2 と Y が割れた");
    assert_eq!(x, y, "slot を使い回した X と Y が割れた");
}

/// 使い回しの後に、 A が (X の削除を知らずに) z の title を書き換える (消えた行宛ての書き込み = 捨てる)。
#[test]
fn a_late_edit_of_the_evicted_identity_after_slot_reuse() {
    let (x, x2, y, reused) = scenario("edit", |a, _f| {
        a.retitle("z", "a2");
    });
    assert!(reused, "前提: X が z の slot を使い回していない");
    assert_eq!(x, x2, "slot を使い回した X と、 使い回さない X2 が割れた (Y は自分で消していないので別の答え: {y:?})");
}
