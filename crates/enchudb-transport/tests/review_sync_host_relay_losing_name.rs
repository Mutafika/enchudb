//! review (0.37.4): ホスト (中継) は LWW で負けた record を中継しない (`Syncer::apply_records_impl` の `relay_accepted`)。
//! 名前で束ねる表では、 負けた名前の Tie が 「この世界番号はこの名前のデッキ」 を運ぶ唯一の record なので、 ホストの下の client は
//! その世界番号を名前に結べず、 その世界番号宛ての削除が自分の同じ名前の行に当たらない。
//!
//! 形 A: client 3 が k を作る → client 1 が (知らずに) 同じ k を作る (後) → client 3 が自分の k を消す (さらに後)。
//! 削除優先: client 1 の行は削除より前に作られた同じデッキなので全員で消える (直に pull する形の
//! `tests/sync_converge_fuzz.rs` の答え)。 ホスト経由では、 ホストと client 3 は消え、 client 1 と 2 は残る。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const T: &str = "t";
const OPLOG: usize = 1 << 20;
const HOST: PeerId = 100;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("review-relay-losing-{tag}-{}", std::process::id()));
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

type Answer = Vec<(Option<Value>, Option<Value>)>;

fn rows(db: &Database) -> Answer {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
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

fn settle(db: &Database) {
    let eng = db.engine();
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    std::thread::sleep(Duration::from_millis(3));
}

fn upsert(db: &Database, k: &str, v: &str) {
    db.get_table(T).unwrap().upsert().set("key", k).set("title", v).commit().unwrap();
    settle(db);
}

fn delete(db: &Database, k: &str) {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    let e = t.where_eq("key", k).find().unwrap()[0];
    t.entity(e).delete().unwrap();
    settle(db);
}

fn rename(db: &Database, from: &str, to: &str) {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    let e = t.where_eq("key", from).find().unwrap()[0];
    t.entity(e).set("key", to).commit().unwrap();
    settle(db);
}

/// ring の record を (書き手, op の形) で。
fn ring(db: &Database) -> Vec<String> {
    let eng = db.engine();
    let key = eng.himo_id("t.key").map(|h| h as u16);
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(|r| {
            let op = match &r.op {
                DecodedOp::Tie { eid, himo_id, .. } => format!(
                    "Tie {}:{} {}",
                    enchudb_oplog::eid_peer(*eid),
                    enchudb_oplog::eid_local(*eid),
                    if Some(*himo_id) == key { "key" } else { "title" }
                ),
                DecodedOp::Delete { eid } => {
                    format!("Delete {}:{}", enchudb_oplog::eid_peer(*eid), enchudb_oplog::eid_local(*eid))
                }
                DecodedOp::Vocab { bytes, .. } => format!("Vocab {:?}", String::from_utf8_lossy(bytes)),
                DecodedOp::Commit => "Commit".into(),
                other => format!("{other:?}"),
            };
            format!("author {} @{} {op}", r.author_peer, r.hlc.wall % 100_000)
        })
        .collect()
}

struct Client {
    db: Arc<Database>,
    s: Syncer,
}

impl Client {
    fn new(dir: &Dir, id: u32, url: &str) -> Self {
        let db = create(&dir.db(id), id);
        let t = Arc::new(HttpHostTransport::connect(url.to_string(), id).unwrap());
        let s = Syncer::new(db.arc_engine(), t);
        s.serve_state();
        Client { db, s }
    }
    fn push(&self) {
        settle(&self.db);
        self.s.publish_since(Hlc::ZERO);
    }
    fn pull(&self) {
        let out = self.s.pull_once(HOST);
        assert!(!out.history_truncated, "{out:?}");
    }
}

/// 同じ書き込みを、 直に pull する形 (中継なし) で 1 台の受け手に当てた答え (対照)。
fn direct_answer(dir: &Dir, script: impl Fn(&[Arc<Database>])) -> Answer {
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=4 {
        tr.register_peer(id);
    }
    let dbs: Vec<Arc<Database>> = (1..=3).map(|id| create(&dir.db(10 + id), id)).collect();
    script(&dbs);
    let r = create(&dir.db(14), 4);
    let rs = Syncer::new(r.arc_engine(), tr.clone() as Arc<dyn Transport>);
    for db in &dbs {
        let s = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
        settle(db);
        s.publish_since(Hlc::ZERO);
    }
    for id in 1..=3 {
        rs.pull_once(id);
    }
    rows(&r)
}

fn through_host(tag: &str, script: impl Fn(&[Arc<Database>]), push_order: &[usize]) -> (Vec<(String, Answer)>, Answer) {
    let dir = Dir::new(tag);
    let direct = direct_answer(&dir, &script);
    let host_db = create(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_db.arc_engine(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let cs: Vec<Client> = (1..=3).map(|id| Client::new(&dir, id, &url)).collect();
    let dbs: Vec<Arc<Database>> = cs.iter().map(|c| c.db.clone()).collect();
    script(&dbs);
    drop(dbs);
    for &i in push_order {
        cs[i].push();
    }
    println!("[{tag}] ホストの ring (client に配る record):");
    for l in ring(&host_db) {
        println!("    {l}");
    }
    for _ in 0..4 {
        for c in &cs {
            c.push();
            c.pull();
        }
    }
    let mut answers: Vec<(String, Answer)> =
        cs.iter().enumerate().map(|(i, c)| (format!("client {}", i + 1), rows(&c.db))).collect();
    answers.push(("host".into(), rows(&host_db)));
    println!("[{tag}] 直に pull した受け手: {direct:?}");
    for (who, a) in &answers {
        println!("[{tag}] {who}: {a:?}");
    }
    drop(cs);
    drop(host);
    drop(host_db);
    (answers, direct)
}

fn all_same(tag: &str, answers: &[(String, Answer)], direct: &Answer) {
    assert!(
        answers.iter().all(|(_, a)| a == &answers[0].1),
        "[{tag}] ホスト経由で答えが割れた: {answers:?} (直に pull した受け手: {direct:?})"
    );
    assert_eq!(&answers[0].1, direct, "[{tag}] ホスト経由の答えが、 直に pull した答えと違う");
}

/// 形 A: 負けた作成 (名前の Tie) が中継されず、 その世界番号宛ての削除が他の client の同じ名前の行に当たらない。
#[test]
fn a_delete_addressed_to_an_identity_whose_name_tie_lost_lww() {
    let script = |dbs: &[Arc<Database>]| {
        upsert(&dbs[2], "k", "from3");
        upsert(&dbs[0], "k", "from1");
        delete(&dbs[2], "k");
    };
    // client 1 が先に出す (ホストでは client 3 の名前の Tie が負ける)
    let (answers, direct) = through_host("A", script, &[0, 2]);
    all_same("A", &answers, &direct);
}

/// 形 A の対照: client 3 が先に出すと、 client 3 の作成はホストで当たるので中継される。
#[test]
fn control_the_same_writes_pushed_in_the_other_order() {
    let script = |dbs: &[Arc<Database>]| {
        upsert(&dbs[2], "k", "from3");
        upsert(&dbs[0], "k", "from1");
        delete(&dbs[2], "k");
    };
    let (answers, direct) = through_host("A-control", script, &[2, 0]);
    all_same("A-control", &answers, &direct);
}

/// 形 B (削除なし): 負けた改名が中継されない。 client 2 が k1 → k2 に改名 (先)、 client 1 が k1 → k0 に改名 (後、 勝つ)。 client 3 は
/// 2 つの改名の間に k2 を作る (その時刻のデッキの名前は k2 = 同じデッキ、 `a_row_made_with_the_name_of_a_losing_rename_joins_the_deck`)。
#[test]
fn a_rename_that_lost_lww_is_not_relayed() {
    let script = |dbs: &[Arc<Database>]| {
        upsert(&dbs[1], "k1", "from2");
        upsert(&dbs[0], "k1", "from1");
        rename(&dbs[1], "k1", "k2");
        upsert(&dbs[2], "k2", "from3");
        rename(&dbs[0], "k1", "k0");
    };
    let (answers, direct) = through_host("B", script, &[0, 1, 2]);
    all_same("B", &answers, &direct);
}

fn retitle(db: &Database, k: &str, v: &str) {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    let e = t.where_eq("key", k).find().unwrap()[0];
    t.entity(e).set("title", v).commit().unwrap();
    settle(db);
}

/// 形 C (削除も改名も無い、 いちばん普通の形): 2 台がオフラインで同じ名前の行を作り (client 1 が先、 client 2 が後)、 ホストには
/// client 2 が先に出す。 client 1 の作成はホストで負けて中継されない。 その後 client 1 が自分の行の title を書き換える (宛名は
/// 自分の世界番号)。 他の client はその世界番号を知らないので、 書き換えは名前の無い塊に入り、 k の title は古いまま。
#[test]
fn an_edit_addressed_to_an_identity_whose_create_lost_lww() {
    let dir = Dir::new("C");
    let host_db = create(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_db.arc_engine(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let cs: Vec<Client> = (1..=3).map(|id| Client::new(&dir, id, &url)).collect();
    upsert(&cs[0].db, "k", "from1");
    upsert(&cs[1].db, "k", "from2");
    cs[1].push();
    cs[0].push();
    for _ in 0..2 {
        for c in &cs {
            c.push();
            c.pull();
        }
    }
    let before: Vec<Answer> = cs.iter().map(|c| rows(&c.db)).collect();
    println!("[C] 書き換えの前: {before:?} / host {:?}", rows(&host_db));
    retitle(&cs[0].db, "k", "edited-by-1");
    for _ in 0..4 {
        for c in &cs {
            c.push();
            c.pull();
        }
    }
    println!("[C] ホストの ring:");
    for l in ring(&host_db) {
        println!("    {l}");
    }
    let mut answers: Vec<(String, Answer)> =
        cs.iter().enumerate().map(|(i, c)| (format!("client {}", i + 1), rows(&c.db))).collect();
    answers.push(("host".into(), rows(&host_db)));
    for (who, a) in &answers {
        println!("[C] {who}: {a:?}");
    }
    // 名前の無い塊 (title だけの entity) の数
    let clumps: Vec<usize> = cs
        .iter()
        .map(|c| {
            c.db.engine().rebuild();
            let t = c.db.get_table(T).unwrap();
            t.where_eq("title", "edited-by-1").find().unwrap().into_iter().filter(|e| t.entity(*e).get("key").is_none()).count()
        })
        .collect();
    println!("[C] client ごとの、 名前の無い 'edited-by-1' の塊: {clumps:?}");
    drop(cs);
    drop(host);
    drop(host_db);
    assert!(answers.iter().all(|(_, a)| a == &answers[0].1), "[C] ホスト経由で答えが割れた: {answers:?}");
}
