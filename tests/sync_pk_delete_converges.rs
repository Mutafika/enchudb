//! #513: 違う author が同じ主キーの行を作り、 片方が消すと、 author どうしが恒久に食い違った。
//!
//! A が主キー z の行を作る → B が (A を見ずに) 主キー z の行を作る → A が z を消す。 B は A の z を自分の z に束ねる
//! (#141、 同じ主キーなら同じ行) ので、 A の削除で B の z も消える。 A は自分の z が死んでいるので、 B の z を束ねずに
//! 新しい行として生かしていた。 A は z を持ち、 B は持たない。
//!
//! 答えの形 (ユーザの決め、 2026-10-18): 主キーが同じなら同じ行。 削除はその主キーの行の、 削除より古い書き込みを
//! どの author のものでも消し、 新しい書き込みは残す (主キーごとの LWW)。 受け手は消した主キーを覚え
//! (`crates/enchudb-engine/src/pk_tombs.rs`)、 後から届いた同じ主キーの行にその削除を当てる。

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
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("pk-delete-{tag}-{}", std::process::id()));
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

/// `make` の、 sync を先に有効にしてから主キーのある表を作る順。
fn make_sync_first(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>) -> (Arc<Database>, Syncer) {
    let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
    b.enable_sync().unwrap();
    b.table(T).tag("key").tag("title").number("n").primary_key("key").build().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    (db, sync)
}

/// 開き直す (Syncer も engine の Arc を持つので、 呼び手が先に両方を落としておく)。
fn reopen(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>) -> (Arc<Database>, Syncer) {
    let db = Database::open_with_oplog(&dir.db(id), OPLOG).unwrap();
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

/// 届いた record があることを確かめて pull する。
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

fn delete_key(db: &Database, key: &str) {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    let found = t.where_eq("key", key).find().unwrap();
    assert!(!found.is_empty(), "消す行 {key} が無い");
    for e in found {
        t.entity(e).delete().unwrap();
    }
}

fn upsert(db: &Database, key: &str, title: &str) {
    db.get_table(T).unwrap().upsert().set("key", key).set("title", title).commit().unwrap();
}

fn text(s: &str) -> Option<Value> {
    Some(Value::Text(s.into()))
}

/// 本題: A が z を作る → B が (A を見ずに) z を作る → A が z を消す。 A と B が互いに pull した後、 同じ答え (z 無し)。
/// B の z は A の削除より古いので、 主キーごとの LWW で消える。 第三の peer C・D も、 A と B を逆の順に pull して同じ答え。
#[test]
fn delete_of_a_primary_key_reaches_the_other_authors_row() {
    let dir = Dir::new("h3");
    let tr = transport(4);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);
    let (d, sd) = make(&dir, 4, &tr);

    upsert(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert(&b, "z", "b");
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
        assert_eq!(titled(db, "b"), 0, "{name}");
    }
    assert!(a.engine().deleted_primary_key_count() >= 1);

    drop((sa, sb, sc, sd));
    drop((a, b, c, d));
}

/// 本題と同じ形を、 sync を先に有効にしてから主キーのある表を作った DB で (主キーを覚えるかの印を、 表に主キーを付けた所で
/// 引き直す)。
#[test]
fn sync_enabled_before_the_table_also_remembers() {
    let dir = Dir::new("order");
    let tr = transport(2);
    let (a, sa) = make_sync_first(&dir, 1, &tr);
    let (b, sb) = make_sync_first(&dir, 2, &tr);

    upsert(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert(&b, "z", "b");
    publish(&b, &sb);
    later();
    delete_key(&a, "z");
    publish(&a, &sa);

    pull(&sb, 1);
    pull(&sa, 2);
    assert_eq!((rows(&a), rows(&b)), (vec![], vec![]));
    assert!(a.engine().deleted_primary_key_count() >= 1, "A が消した主キーを覚えていない");

    drop((sa, sb));
    drop((a, b));
}

/// 対照: 削除が B の作成より古い時は、 B の z が残る (削除より新しい書き込みは消さない)。
#[test]
fn a_delete_older_than_the_other_authors_row_keeps_it() {
    let dir = Dir::new("older");
    let tr = transport(3);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);

    upsert(&a, "z", "a");
    later();
    delete_key(&a, "z");
    publish(&a, &sa);
    later();
    upsert(&b, "z", "b");
    publish(&b, &sb);

    pull(&sb, 1);
    pull(&sa, 2);
    pull(&sc, 2);
    pull(&sc, 1);
    for (name, db) in [("A", &a), ("B", &b), ("C", &c)] {
        assert_eq!(rows(db), vec![(text("z"), text("b"), None)], "{name}");
    }

    drop((sa, sb, sc));
    drop((a, b, c));
}

/// 覚えた主キーは再起動を越える。 消した A (自分の削除) と、 A の削除を受けた B (受け取った削除) を開き直してから、
/// 削除より古い C の z を受ける。 対照: 開き直す前に記録の file (`pktombs`) を消した R は、 C の z を残す
/// (= 再起動を越えているのは file の記録)。
#[test]
fn the_deleted_key_survives_a_restart() {
    let dir = Dir::new("restart");
    let tr = transport(4);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);
    let (r, sr) = make(&dir, 4, &tr);

    upsert(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert(&c, "z", "c");
    publish(&c, &sc);
    later();
    delete_key(&a, "z");
    publish(&a, &sa);
    pull(&sb, 1);
    pull(&sr, 1);
    assert_eq!((rows(&b), rows(&r)), (vec![], vec![]));

    drop((sa, sb, sr));
    drop((a, b, r));
    std::fs::remove_file(Path::new(&dir.db(4)).join("pktombs")).expect("R に記録の file が無い");
    let (a, sa) = reopen(&dir, 1, &tr);
    let (b, sb) = reopen(&dir, 2, &tr);
    let (r, sr) = reopen(&dir, 4, &tr);

    pull(&sa, 3);
    pull(&sb, 3);
    pull(&sr, 3);
    assert_eq!(rows(&r), vec![(text("z"), text("c"), None)], "対照: 記録を消した R は C の z を残す");
    assert_eq!(rows(&a), vec![], "A が再起動で消した主キーを忘れた");
    assert_eq!(rows(&b), vec![], "B が再起動で受け取った削除の主キーを忘れた");

    drop((sa, sb, sc, sr));
    drop((a, b, c, r));
}

/// 削除の後に同じ主キーで作り直した行に、 削除より古い別の author の行を束ねても、 古い書き込みは入らない。
/// A: z (title a1) → B: z (title b, n 2、 A を見ずに) → A: z を消して作り直す (title a2、 n 無し)。 どの peer も
/// (z, a2, n 無し)。 A が作り直した行に B の z を束ねた時に、 B の n (削除より古い) が入らないこと。
#[test]
fn a_row_made_again_after_the_delete_drops_the_older_writes() {
    let dir = Dir::new("again");
    let tr = transport(4);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);
    let (d, sd) = make(&dir, 4, &tr);

    upsert(&a, "z", "a1");
    publish(&a, &sa);
    later();
    b.get_table(T).unwrap().upsert().set("key", "z").set("title", "b").set("n", 2).commit().unwrap();
    publish(&b, &sb);
    later();
    delete_key(&a, "z");
    later();
    upsert(&a, "z", "a2");
    publish(&a, &sa);

    pull(&sb, 1);
    pull(&sa, 2);
    pull(&sc, 2);
    pull(&sc, 1);
    pull(&sd, 1);
    pull(&sd, 2);
    for (name, db) in [("A", &a), ("B", &b), ("C (B → A)", &c), ("D (A → B)", &d)] {
        assert_eq!(rows(db), vec![(text("z"), text("a2"), None)], "{name}: 削除より古い B の n が残った");
    }

    drop((sa, sb, sc, sd));
    drop((a, b, c, d));
}

/// 同じ主キーの行への 2 つの削除 (A の t3 と B の t4) が、 受け手に逆の順で届いても、 主キーの削除の版数は新しい方
/// (t4) になる。 その間 (t3.5) に作った F の z は、 どちらの受け手でも消える。 X は A と B の行を束ねた後に、 A の削除
/// (主キーの cell がある) → B の削除 (主キーの cell はもう無い) の順に受ける。 Y は B → A の順。
#[test]
fn two_deletes_of_one_key_agree_in_either_order() {
    let dir = Dir::new("two");
    let tr = transport(5);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (x, sx) = make(&dir, 3, &tr);
    let (y, sy) = make(&dir, 4, &tr);
    let (f, sf) = make(&dir, 5, &tr);

    upsert(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert(&b, "z", "b");
    publish(&b, &sb);
    pull(&sx, 1);
    pull(&sx, 2);
    pull(&sa, 2);
    pull(&sb, 1);
    assert_eq!(rows(&x), vec![(text("z"), text("b"), None)], "X で A と B の z が 1 行に束ならない");
    later();
    delete_key(&a, "z");
    publish(&a, &sa);
    later();
    upsert(&f, "z", "f");
    publish(&f, &sf);
    later();
    delete_key(&b, "z");
    publish(&b, &sb);

    pull(&sx, 1);
    pull(&sx, 2);
    pull(&sx, 5);
    pull(&sy, 2);
    pull(&sy, 1);
    pull(&sy, 5);
    pull(&sa, 2);
    pull(&sa, 5);
    pull(&sb, 1);
    pull(&sb, 5);
    pull(&sf, 1);
    pull(&sf, 2);
    for (name, db) in [("X (A → B)", &x), ("Y (B → A)", &y), ("A", &a), ("B", &b), ("F", &f)] {
        assert_eq!(rows(db), vec![], "{name}: 2 つの削除の間に作った F の z が残った");
    }

    drop((sa, sb, sx, sy, sf));
    drop((a, b, x, y, f));
}

/// 作成より先に届いた削除 (#490) も、 その主キーの削除として覚える。 B が A の z を消す (t3)。 C は B から削除を先に
/// 受け、 次に A の z (作成) を受け、 最後に D の z (t2、 A も B も見ていない) を受ける。 E は A → D → B の順。 どちらも
/// z 無し (E では D の z が A の z に束なって、 B の削除で消える)。
#[test]
fn an_early_delete_also_covers_the_same_key_from_another_author() {
    let dir = Dir::new("early");
    let tr = transport(5);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);
    let (d, sd) = make(&dir, 4, &tr);
    let (e, se) = make(&dir, 5, &tr);

    upsert(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert(&d, "z", "d");
    publish(&d, &sd);
    later();
    pull(&sb, 1);
    delete_key(&b, "z");
    publish(&b, &sb);

    pull(&sc, 2);
    pull(&sc, 1);
    pull(&sc, 4);
    pull(&se, 1);
    pull(&se, 4);
    pull(&se, 2);
    for (name, db) in [("C (B → A → D)", &c), ("E (A → D → B)", &e)] {
        assert_eq!(rows(db), vec![], "{name}: 削除より古い D の z が残った");
        assert_eq!(titled(db, "d"), 0, "{name}");
    }

    drop((sa, sb, sc, sd, se));
    drop((a, b, c, d, e));
}

/// 削除より新しい書き込みは残り、 行は主キー (名前) ごと生きる (#547: 名前の無い行は表の行でない。 前は主キーの cell
/// だけ消えて、 主キーで引けない行が残り、 同じ主キーの upsert で 2 行目ができた)。 B が (A の削除を見ずに) title を
/// 更新した後: どの peer も (z, b2) の行が 1 つ。 削除が先に届いた peer (C) と、 新しい title を先に持っていた peer (D) で
/// 同じ。 その後で同じ主キーを upsert しても行は増えない。
#[test]
fn a_write_newer_than_the_delete_survives_with_its_key() {
    let dir = Dir::new("newer");
    let tr = transport(4);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);
    let (d, sd) = make(&dir, 4, &tr);

    upsert(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert(&b, "z", "b");
    later();
    delete_key(&a, "z");
    publish(&a, &sa);
    later();
    {
        b.engine().rebuild();
        let t = b.get_table(T).unwrap();
        let found = t.where_eq("key", "z").find().unwrap();
        assert_eq!(found.len(), 1);
        t.entity(found[0]).update().set("title", "b2").commit().unwrap();
    }
    publish(&b, &sb);

    pull(&sb, 1);
    pull(&sa, 2);
    pull(&sc, 1);
    pull(&sc, 2);
    pull(&sd, 2);
    pull(&sd, 1);
    for (name, db) in [("A", &a), ("B", &b), ("C (A → B)", &c), ("D (B → A)", &d)] {
        assert_eq!(rows(db), vec![(text("z"), text("b2"), None)], "{name}: 削除より新しい title の行が主キーごと残らない");
        assert_eq!((titled(db, "b2"), titled(db, "b")), (1, 0), "{name}: 削除より新しい title の行の数");
        let before = db.engine().entity_count();
        db.get_table(T).unwrap().upsert().set("key", "z").set("n", 9i64).commit().unwrap();
        db.engine().rebuild();
        assert_eq!(
            (rows(db), db.engine().entity_count()),
            (vec![(text("z"), text("b2"), Some(Value::Number(9)))], before),
            "{name}: 同じ主キーの upsert で行が増えた"
        );
    }

    drop((sa, sb, sc, sd));
    drop((a, b, c, d));
}

/// 削除より新しい書き込みが Leaf の列 (wire では TieLeaf) でも、 行は主キーごと生きる (#547)。
#[test]
fn a_newer_leaf_write_also_keeps_the_key() {
    let dir = Dir::new("newer-leaf");
    let tr = transport(4);
    let peer = |id: u32| {
        let mut bl = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
        bl.table("notes").tag("key").leaf("body").primary_key("key").build().unwrap();
        bl.enable_sync().unwrap();
        let db = bl.finish_with_oplog(OPLOG).unwrap();
        db.engine().set_peer_id(id);
        let s = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
        (db, s)
    };
    let ((a, sa), (b, sb), (c, sc), (d, sd)) = (peer(1), peer(2), peer(3), peer(4));
    a.get_table("notes").unwrap().upsert().set("key", "z").set("body", "a").commit().unwrap();
    publish(&a, &sa);
    pull(&sb, 1);
    later();
    {
        a.engine().rebuild();
        let t = a.get_table("notes").unwrap();
        let e = t.where_eq("key", "z").find_one().unwrap().unwrap();
        t.entity(e).delete().unwrap();
    }
    publish(&a, &sa);
    later();
    {
        b.engine().rebuild();
        let t = b.get_table("notes").unwrap();
        let e = t.where_eq("key", "z").find_one().unwrap().unwrap();
        t.entity(e).set("body", "b2").commit().unwrap();
    }
    publish(&b, &sb);
    pull(&sb, 1);
    pull(&sa, 2);
    pull(&sc, 1);
    pull(&sc, 2);
    pull(&sd, 2);
    pull(&sd, 1);
    for (name, db) in [("A", &a), ("B", &b), ("C (A → B)", &c), ("D (B → A)", &d)] {
        db.engine().rebuild();
        let t = db.get_table("notes").unwrap();
        let rows: Vec<_> = t.all().find().unwrap().into_iter().map(|e| (t.entity(e).get("key"), t.entity(e).get("body"))).collect();
        assert_eq!(rows, vec![(text("z"), text("b2"))], "{name}: Leaf の新しい書き込みの行が主キーごと残らない");
    }
    drop((sa, sb, sc, sd));
    drop((a, b, c, d));
}
