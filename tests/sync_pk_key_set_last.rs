//! #544: 主キーを後に set した行 (`upsert().set("title", ..).set("key", ..)`) を、 受け手が主キーで束ねなかった。
//!
//! schema の `commit()` は set した順に書くので、 行の最初の record が主キーでない Tie になる。 受け手は record を当てる直前に
//! 1 件ずつ主キーを照合する (#508 / #509) が、 主キーでない Tie が最初に来ると、 そこで新しい slot を払い出していた。 後から
//! 来る主キーの Tie は写像済みなので束ねない。 同じ主キーの行が 2 つ残り (#141 の退行)、 #513 の削除の規則も効かなかった。
//! 直し: 行の最初の record を当てる前に、 同じ batch の後ろにある主キーの Tie で束ねる (束ねる時の状態は今までどおり、
//! 前の record を当てた後)。
//!
//! 今までの test (`issue141_pk_dedupe` / `sync_pk_delete_converges` / `sync_pk_recreate_in_one_batch`) は全部主キーを先に
//! set していたので、 この退行を捉えなかった。 ここでは同じ形を主キーを最後に set して回す。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 1 << 20;

/// test ごとの directory。 Drop で消す (DB と Syncer を落とした後に消えるよう、 最初に作って最後に落とす)。
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("pk-key-last-{tag}-{}", std::process::id()));
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

fn transport(peers: u32) -> Arc<InMemoryTransport> {
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=peers {
        tr.register_peer(id);
    }
    tr
}

fn make(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>) -> (Arc<Database>, Syncer) {
    let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
    b.table(T).tag("key").tag("title").number("n").primary_key("key").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    (db, sync)
}

/// `tests/issue141_pk_dedupe.rs` の `publish_all` と同じ手順。
fn publish(db: &Database, s: &Syncer) {
    let eng = db.engine();
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    s.publish_since(Hlc::ZERO);
}

/// 届いた record があり、 全部当たったことを確かめて pull する。
fn pull(s: &Syncer, from: u32) {
    let o = s.pull_once(from);
    assert!(o.received > 0, "{from} から何も届かない: {o:?}");
}

/// HLC の順を作る (同じ process の時計なので、 間を空ければ後に書いた方が新しい)。
fn later() {
    std::thread::sleep(Duration::from_millis(5));
}

type Row = (Option<Value>, Option<Value>, Option<Value>);

/// 表の行 (主キーのある行) を (key, title, n) で並べる。
fn rows(db: &Database) -> Vec<Row> {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    let mut out: Vec<Row> = t
        .all()
        .find()
        .unwrap()
        .into_iter()
        .map(|e| {
            let r = t.entity(e);
            (r.get("key"), r.get("title"), r.get("n"))
        })
        .collect();
    out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    out
}

/// title が `title` の行の数 (主キーの cell の無い行も数える)。
fn titled(db: &Database, title: &str) -> usize {
    db.engine().rebuild();
    db.get_table(T).unwrap().where_eq("title", title).find().unwrap().len()
}

/// 主キーを最後に set して書く。
fn upsert_key_last(db: &Database, key: &str, title: &str) {
    db.get_table(T).unwrap().upsert().set("title", title).set("key", key).commit().unwrap();
}

/// 対照: 主キーを先に set して書く。
fn upsert_key_first(db: &Database, key: &str, title: &str) {
    db.get_table(T).unwrap().upsert().set("key", key).set("title", title).commit().unwrap();
}

fn delete_key(db: &Database, key: &str) {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    let found = t.where_eq("key", key).find().unwrap();
    assert!(!found.is_empty(), "消す行 {key} が無い");
    for e in found {
        t.entity(e).delete().unwrap();
    }
}

fn text(s: &str) -> Option<Value> {
    Some(Value::Text(s.into()))
}

/// #141 の形: A と B が独立に主キー z の行を作る。 どの順に pull しても、 どの peer でも z の行は 1 つ (後に書いた B の title)。
/// 主キーを後に set した行どうし、 先と後の混ざった行どうしの両方で。
#[test]
fn rows_with_the_key_set_last_are_bound_by_the_key() {
    type Write = fn(&Database, &str, &str);
    let shapes: [(&str, Write, Write); 3] = [
        ("last-last", upsert_key_last, upsert_key_last),
        ("first-last", upsert_key_first, upsert_key_last),
        ("last-first", upsert_key_last, upsert_key_first),
    ];
    for (tag, write_a, write_b) in shapes {
        let dir = Dir::new(tag);
        let tr = transport(4);
        let (a, sa) = make(&dir, 1, &tr);
        let (b, sb) = make(&dir, 2, &tr);
        let (c, sc) = make(&dir, 3, &tr);
        let (d, sd) = make(&dir, 4, &tr);

        write_a(&a, "z", "a");
        publish(&a, &sa);
        later();
        write_b(&b, "z", "b");
        publish(&b, &sb);

        pull(&sc, 1);
        pull(&sc, 2);
        pull(&sd, 2);
        pull(&sd, 1);
        pull(&sa, 2);
        pull(&sb, 1);
        for (name, db) in [("A", &a), ("B", &b), ("C (A → B)", &c), ("D (B → A)", &d)] {
            assert_eq!(rows(db), vec![(text("z"), text("b"), None)], "{tag} {name}: 主キー z の行が 1 つにならない");
            assert_eq!(titled(db, "a"), 0, "{tag} {name}: A の z が別の行で残った");
        }

        drop((sa, sb, sc, sd));
        drop((a, b, c, d));
    }
}

/// #513 の形を主キーを後に set して: A が z を作る → B が (A を見ずに) z を作る → A が z を消す。 削除より古い B の z は
/// どの peer でも消える (主キーごとの LWW)。
#[test]
fn delete_of_a_key_set_last_reaches_the_other_authors_row() {
    let dir = Dir::new("delete");
    let tr = transport(4);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);
    let (d, sd) = make(&dir, 4, &tr);

    upsert_key_last(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert_key_last(&b, "z", "b");
    publish(&b, &sb);
    later();
    delete_key(&a, "z");
    publish(&a, &sa);

    pull(&sb, 1);
    pull(&sa, 2);
    pull(&sc, 1);
    pull(&sc, 2);
    pull(&sd, 2);
    pull(&sd, 1);
    for (name, db) in [("A", &a), ("B", &b), ("C (A → B)", &c), ("D (B → A)", &d)] {
        assert_eq!(rows(db), vec![], "{name}: 削除より古い B の z が残った");
        assert_eq!(titled(db, "b"), 0, "{name}: 削除より古い B の z が主キーの無い行で残った");
    }

    drop((sa, sb, sc, sd));
    drop((a, b, c, d));
}

/// #508 の形を主キーを後に set して: A が 1 回の配りの中で z を消して z で作り直す。 作り直した行は、 同じ batch で先に
/// 消える古い行に束ねない (束ねる時の状態は、 前の record を当てた後)。 その後で C が (削除を知らずに) 古い行を更新すると、
/// その書き込みは今 z の名前を持つ行 (作り直した行) に当たる (#547: 名前が同じなら同じ行。 消えた行は名前の無い行として
/// 生き返らない)。 author の A も、 B も、 古い行を持っていた C も同じ答え。
#[test]
fn recreating_a_key_set_last_in_one_batch_converges_with_the_author() {
    let dir = Dir::new("recreate");
    let tr = transport(3);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);

    upsert_key_last(&a, "z", "x");
    publish(&a, &sa);
    pull(&sb, 1);
    pull(&sc, 1);

    delete_key(&a, "z");
    a.get_table(T).unwrap().insert().set("title", "y").set("n", 1i64).set("key", "z").commit().unwrap();
    publish(&a, &sa);

    later();
    c.engine().rebuild();
    let old_on_c = c.get_table(T).unwrap().where_eq("key", "z").find().unwrap();
    assert_eq!(old_on_c.len(), 1, "前提: C に古い z が 1 つ");
    c.get_table(T).unwrap().entity(old_on_c[0]).update().set("n", 2i64).commit().unwrap();
    publish(&c, &sc);

    pull(&sb, 1);
    pull(&sb, 3);
    pull(&sa, 3);
    pull(&sc, 1);
    let (on_a, on_b, on_c) = (rows(&a), rows(&b), rows(&c));
    assert_eq!(on_b, on_a, "B の行の集まりが author の A と違う (A: {on_a:?})");
    assert_eq!(on_c, on_a, "C の行の集まりが author の A と違う (A: {on_a:?})");
    assert_eq!(on_a, vec![(text("z"), text("y"), Some(Value::Number(2)))], "C の書き込みが作り直した z に当たらない");
    for (name, db) in [("A", &a), ("B", &b), ("C", &c)] {
        db.engine().rebuild();
        assert_eq!(db.get_table(T).unwrap().where_eq("n", 2i64).count().unwrap(), 1, "{name}: n = 2 の行が名前の無い行でも残った");
    }

    drop((sa, sb, sc));
    drop((a, b, c));
}

/// 行の最初の record で束ねる時、 後ろの主キーの Tie が署名を通らなければ、 その主キーでは束ねない (通らない record の値で、
/// 通った record の行き先を決めない)。 A の署名付きの n の Tie と、 署名の壊れた主キー 7 の Tie が 1 つの batch に並んでも、
/// C が持つ主キー 7 の行は書き換わらない。
#[test]
fn a_key_that_fails_the_signature_check_does_not_bind() {
    use enchudb::transport::WireRecord;
    use enchudb_oplog::oplog::{DecodedOp, Op, OpLog};

    let dir = Dir::new("forged");
    let mut b = Database::create_with_capacity(&dir.db(3), 4096).unwrap();
    b.table("u").number("id").number("n").primary_key("id").build().unwrap();
    b.enable_sync().unwrap();
    let c = b.finish_with_oplog(OPLOG).unwrap();
    c.engine().set_peer_id(3);
    let u = c.get_table("u").unwrap();
    u.insert().set("id", 7i64).set("n", 1i64).commit().unwrap();
    later();

    // A の署名付きの record (C の行 7 の書き込みより新しい): 行 5 の n、 その後に主キー
    let eng = c.engine();
    let (id, n) = (eng.himo_id("u.id").unwrap() as u16, eng.himo_id("u.n").unwrap() as u16);
    let kp = Arc::new(enchudb::keys::Keypair::generate());
    let author = OpLog::create(&dir.0.join("a.oplog"), 1 << 20).unwrap();
    author.set_peer_id(1);
    author.set_keypair(Some(kp.clone()));
    let row = enchudb::make_eid(1, 5);
    author.append(Op::Tie { eid: row, himo_id: n, value: 5 }).unwrap();
    author.append(Op::Tie { eid: row, himo_id: id, value: 7 }).unwrap();
    author.append(Op::Commit).unwrap();
    let mut recs: Vec<WireRecord> = author.iter_committed().into_iter().map(WireRecord::from).collect();
    let pk_at = recs.iter().position(|r| matches!(r.op, DecodedOp::Tie { himo_id, .. } if himo_id == id));
    assert_eq!((recs.len(), pk_at), (2, Some(1)), "前提: n の Tie の後に主キーの Tie");
    assert!(recs.iter().all(|r| r.signature != [0u8; 64]), "前提: 署名付き");
    // 主キーの Tie の署名を壊す
    recs[1].signature[0] ^= 0xff;

    c.engine().pubkeys().force_register(1, &kp.public_bytes());
    let tr: Arc<dyn Transport> = transport(3);
    let sc = Syncer::new(c.arc_engine(), tr);
    sc.set_require_signature(true);
    let o = sc.apply_records(&recs);
    assert_eq!((o.applied, o.rejected_signature), (1, 1), "前提: 署名の壊れた主キーの Tie だけ弾く: {o:?}");

    eng.rebuild();
    let sevens = u.where_eq("id", 7i64).find().unwrap();
    assert_eq!(sevens.len(), 1);
    assert_eq!(u.entity(sevens[0]).get("n"), Some(Value::Number(1)), "署名の壊れた主キーで C の行 7 に束ねた");
    assert_eq!(u.where_eq("n", 5i64).find().unwrap().len(), 1, "前提: A の n は別の行に当たった");

    drop(sc);
    drop(c);
}

/// 行の最初の record が Leaf の列 (wire では TieLeaf) でも、 後ろの主キーで束ねる。
#[test]
fn a_leaf_column_set_before_the_key_is_bound_too() {
    let dir = Dir::new("leaf");
    let tr = transport(3);
    let peer = |id: u32| {
        let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
        b.table("notes").leaf("body").tag("key").primary_key("key").build().unwrap();
        b.enable_sync().unwrap();
        let db = b.finish_with_oplog(OPLOG).unwrap();
        db.engine().set_peer_id(id);
        let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
        (db, sync)
    };
    let ((a, sa), (b, sb), (c, sc)) = (peer(1), peer(2), peer(3));
    a.get_table("notes").unwrap().upsert().set("body", "from a").set("key", "z").commit().unwrap();
    publish(&a, &sa);
    later();
    b.get_table("notes").unwrap().upsert().set("body", "from b").set("key", "z").commit().unwrap();
    publish(&b, &sb);

    pull(&sc, 1);
    pull(&sc, 2);
    c.engine().rebuild();
    let notes = c.get_table("notes").unwrap();
    let all = notes.all().find().unwrap();
    let bodies: Vec<_> = all.iter().map(|e| notes.entity(*e).get("body")).collect();
    assert_eq!(bodies, vec![text("from b")], "主キー z の行が 1 つにならない");

    drop((sa, sb, sc));
    drop((a, b, c));
}
