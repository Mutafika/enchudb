//! #490: 作成より先に届いた Delete で、 受け手が pull する順だけで peer が恒久に食い違う。
//!
//! A が作った行 x を、 それを pull した B が消す (書き込みは直列で、 同時の書き込みは無い)。 3 台目の R1 が A より先に
//! B から pull すると、 R1 には x の写像がまだ無い。 前は B の Delete を捨てていた (`dropped_unresolved`)。 pull の
//! cursor は Delete を越えるので Delete は二度と届かず、 そのあと A から x の作成が届くと、 x は R1 でだけ生きていた。
//! A → B の順に pull した R2 では x は消えている。
//!
//! 直した後は、 写像の無い Delete の削除版数を受け手が覚え (#166 の退避表)、 後から作成が届いて slot を払い出す時に
//! 書き戻す。 削除より古い作成は弾かれる。

use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};

use enchudb::schema::Database;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::Hlc;

const T: &str = "t";
const OPLOG: usize = 1 << 20;

/// test ごとの directory。 Drop で消す (DB と Syncer を落とした後に消えるよう、 最初に作って最後に落とす)。
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("delete-before-create-{tag}-{}", std::process::id()));
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

fn make(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>) -> (Arc<Database>, Syncer) {
    let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
    b.table(T).tag("key").tag("title").primary_key("key").build().unwrap();
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

fn count_key(db: &Database, key: &str) -> usize {
    db.engine().rebuild();
    db.get_table(T).unwrap().where_eq("key", key).count().unwrap()
}

/// 表の生きている行の数。
fn count_rows(db: &Database) -> usize {
    db.engine().rebuild();
    db.get_table(T).unwrap().all().count().unwrap()
}

/// 消す側: 表の行を全部消す。
fn delete_all(db: &Database) {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    for e in t.all().find().unwrap() {
        t.entity(e).delete().unwrap();
    }
}

/// transport が知っている peer ごとの log に配るので、 受け手も先に知らせる。
fn transport(peers: u32) -> Arc<InMemoryTransport> {
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=peers {
        tr.register_peer(id);
    }
    tr
}

/// **本題** (#490 の再現): R1 は B → A、 R2 は A → B の順に pull する。 どちらも x は消えたまま。
#[test]
fn delete_before_create_does_not_revive_the_row() {
    let dir = Dir::new("order");
    let tr = transport(4);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (r1, s1) = make(&dir, 3, &tr);
    let (r2, s2) = make(&dir, 4, &tr);

    // 1. A が x を作る
    a.get_table(T).unwrap().upsert().set("key", "x").set("title", "a").commit().unwrap();
    publish(&a, &sa);

    // 2. B が A から pull して x を見てから消す (直列)
    sb.pull_once(1);
    assert_eq!(count_key(&b, "x"), 1);
    delete_all(&b);
    publish(&b, &sb);

    // 3. A も B から pull する
    sa.pull_once(2);

    // 4. R1 は B → A、 R2 は A → B の順に pull する
    let o1b = s1.pull_once(2);
    let o1a = s1.pull_once(1);
    s2.pull_once(1);
    s2.pull_once(2);

    // 5. R1 にもう一度 pull させる
    s1.pull_once(1);
    s1.pull_once(2);

    assert!(o1b.received > 0 && o1b.applied == o1b.received, "B からの pull: {o1b:?}");
    assert_eq!(o1b.dropped_unresolved, 0, "写像の無い Delete を捨てた: {o1b:?}");
    assert!(o1a.received > 0, "A からの pull: {o1a:?}");
    assert_eq!((count_key(&a, "x"), count_key(&b, "x")), (0, 0));
    assert_eq!(count_key(&r2, "x"), 0, "因果の順に pull した R2 で消えていない");
    assert_eq!(count_key(&r1, "x"), 0, "B → A の順に pull した R1 でだけ x が生き返った");
    assert_eq!(count_rows(&r1), count_rows(&r2), "R1 と R2 の行の数が違う");

    // syncer も engine の Arc を持つので、 両方落としてから directory を消す
    drop((sa, sb, s1, s2));
    drop((a, b, r1, r2));
}

/// 覚えた削除は再起動を越える: R3 は B から Delete だけを受けて落ち、 開き直してから A を pull する。
/// 対照: B から何も受けていない R4 が A を pull すると、 x は 1 行 (= この形で生き返りを見分けられる)。
#[test]
fn early_delete_survives_restart() {
    let dir = Dir::new("restart");
    let tr = transport(4);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (r3, s3) = make(&dir, 3, &tr);
    let (r4, s4) = make(&dir, 4, &tr);

    a.get_table(T).unwrap().upsert().set("key", "x").set("title", "a").commit().unwrap();
    publish(&a, &sa);
    sb.pull_once(1);
    delete_all(&b);
    publish(&b, &sb);

    let o3b = s3.pull_once(2);
    assert!(o3b.applied > 0 && o3b.applied == o3b.received, "B からの pull: {o3b:?}");

    // R3 を落として開き直す (Syncer も engine の Arc を持つので先に落とす)。 cursor は持ち越さない: 開き直した後は
    // A からしか pull しないので、 B の Delete は二度と届かない (= 覚えたものが残っていなければ生き返る)
    drop(s3);
    drop(r3);
    let r3 = Database::open_with_oplog(&dir.db(3), OPLOG).unwrap();
    r3.engine().set_peer_id(3);
    let s3 = Syncer::new(r3.arc_engine(), tr.clone() as Arc<dyn Transport>);

    let o3a = s3.pull_once(1);
    let o4a = s4.pull_once(1);
    assert!(o3a.received > 0 && o4a.received > 0, "A からの pull: {o3a:?} / {o4a:?}");
    assert_eq!(count_key(&r4, "x"), 1, "対照: B の Delete を受けていない R4 には x がある");
    assert_eq!(count_key(&r3, "x"), 0, "再起動で覚えた削除を忘れ、 x が生き返った");

    drop((sa, sb, s3, s4));
    drop((a, b, r3, r4));
}

/// 写像の確かめと退避表への記入が、 写像の払い出しと入れ違わない。 受け手 R の 2 本の thread が、 同じ 300 行の
/// Delete (B の) と作成 (A の) を同時に当てる。 1 行ごとに Delete が先にも後にもなるが、 どの行も残らない。
#[test]
fn concurrent_early_delete_and_create_never_revive() {
    const ROWS: usize = 300;
    const ROUNDS: u32 = 8;
    let dir = Dir::new("race");
    let tr = transport(2 + ROUNDS);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);

    let ta = a.get_table(T).unwrap();
    for i in 0..ROWS {
        ta.upsert().set("key", format!("k{i}")).set("title", "a").commit().unwrap();
    }
    drop(ta);
    publish(&a, &sa);
    sb.pull_once(1);
    assert_eq!(count_rows(&b), ROWS);
    delete_all(&b);
    publish(&b, &sb);

    for round in 0..ROUNDS {
        let id = 3 + round;
        let (r, sr) = make(&dir, id, &tr);
        let from_a: Vec<WireRecord> = tr.pull_as(id, 1, Hlc::ZERO);
        let from_b: Vec<WireRecord> = tr.pull_as(id, 2, Hlc::ZERO);
        let deletes = from_b.iter().filter(|r| matches!(r.op, DecodedOp::Delete { .. })).count();
        assert_eq!(deletes, ROWS, "B の Delete の数");

        let start = Barrier::new(2);
        let (oa, ob) = std::thread::scope(|s| {
            let ha = s.spawn(|| {
                start.wait();
                sr.apply_records(&from_a)
            });
            let hb = s.spawn(|| {
                start.wait();
                sr.apply_records(&from_b)
            });
            (ha.join().unwrap(), hb.join().unwrap())
        });
        assert_eq!(ob.applied, ob.received, "round {round}: B の Delete を当てきれていない: {ob:?}");
        assert_eq!(oa.dropped_unresolved, 0, "round {round}: A の作成: {oa:?}");
        assert_eq!(count_rows(&r), 0, "round {round}: Delete と同時に当てた作成で行が生き返った");
        drop(sr);
        drop(r);
    }

    drop((sa, sb));
    drop((a, b));
}

/// 表の行を (key, title) で並べる。
fn rows(db: &Database) -> Vec<(Option<enchudb::schema::Value>, Option<enchudb::schema::Value>)> {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    let mut out: Vec<_> = t
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

/// 削除より新しい書き込み (B の削除を知らない C の更新) が混ざっても、 受け手が A・B・C を pull する 6 通りの順で
/// 同じ答えになる。 答えは 「行は主キーごと生きる」 (#547: 名前の無い行は表の行でない)。 B → C → A の順 (削除が作成より
/// 先、 C の書き込みが作成より先に行を生き返らせる) では、 後から届いた作成の主キー (削除より古い) が名前として当たる。
#[test]
fn newer_write_after_the_delete_lands_the_same_in_any_order() {
    const ORDERS: [[u32; 3]; 6] = [[1, 2, 3], [1, 3, 2], [2, 1, 3], [2, 3, 1], [3, 1, 2], [3, 2, 1]];
    let dir = Dir::new("newer");
    let tr = transport(3 + ORDERS.len() as u32);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (c, sc) = make(&dir, 3, &tr);

    a.get_table(T).unwrap().upsert().set("key", "x").set("title", "a").commit().unwrap();
    publish(&a, &sa);
    sb.pull_once(1);
    sc.pull_once(1);
    delete_all(&b);
    publish(&b, &sb);
    // B の削除より新しい HLC で、 C が (削除を知らずに) x の title を書き換える
    std::thread::sleep(std::time::Duration::from_millis(5));
    c.engine().rebuild();
    let tc = c.get_table(T).unwrap();
    let x_on_c = tc.where_eq("key", "x").find().unwrap();
    assert_eq!(x_on_c.len(), 1);
    tc.entity(x_on_c[0]).update().set("title", "c").commit().unwrap();
    drop(tc);
    publish(&c, &sc);

    let mut answers = Vec::new();
    for (k, order) in ORDERS.iter().enumerate() {
        let id = 4 + k as u32;
        let (r, sr) = make(&dir, id, &tr);
        for &from in order {
            let o = sr.pull_once(from);
            assert!(o.received > 0 && o.applied + o.skipped == o.received, "{order:?} の {from} から: {o:?}");
        }
        let titled_c = {
            r.engine().rebuild();
            r.get_table(T).unwrap().where_eq("title", "c").count().unwrap()
        };
        answers.push((rows(&r), count_key(&r, "x"), titled_c));
        drop(sr);
        drop(r);
    }
    for (k, ans) in answers.iter().enumerate() {
        assert_eq!(ans, &answers[0], "{:?} の順と {:?} の順で答えが違う", ORDERS[k], ORDERS[0]);
    }
    let text = |s: &str| Some(enchudb::schema::Value::Text(s.into()));
    assert_eq!(answers[0], (vec![(text("x"), text("c"))], 1, 1), "C の書き込みの行が主キーごと残らない");
    eprintln!("答え (6 通り同じ): {:?}", answers[0]);

    drop((sa, sb, sc));
    drop((a, b, c));
}

/// 受け手が自分でも主キー x の行を持っている時 (#141: 同じ主キーの行は 1 つに束ねる)。 A → B の順なら、 A の x を自分の
/// 行に束ねた後で B の Delete が届き、 束ねた行ごと消える (自分の行の値は削除より古い)。 B → A の順でも同じ答え。
/// 対照: B の Delete を受けない受け手では、 束ねた行に A の値が載って残る。
#[test]
fn receiver_with_the_same_primary_key_ends_the_same_in_either_order() {
    let dir = Dir::new("pkbound");
    let tr = transport(5);
    // 受け手が先に自分の行を作る (A の作成・B の削除より古い HLC)
    let (r1, s1) = make(&dir, 3, &tr);
    let (r2, s2) = make(&dir, 4, &tr);
    let (r3, s3) = make(&dir, 5, &tr);
    for r in [&r1, &r2, &r3] {
        r.get_table(T).unwrap().upsert().set("key", "x").set("title", "own").commit().unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(3));
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    a.get_table(T).unwrap().upsert().set("key", "x").set("title", "a").commit().unwrap();
    publish(&a, &sa);
    sb.pull_once(1);
    delete_all(&b);
    publish(&b, &sb);

    s1.pull_once(2);
    s1.pull_once(1);
    s2.pull_once(1);
    s2.pull_once(2);
    s3.pull_once(1);
    let text = |s: &str| Some(enchudb::schema::Value::Text(s.into()));
    assert_eq!(rows(&r3), vec![(text("x"), text("a"))], "対照: 束ねた行に A の値が載る");
    assert_eq!(rows(&r2), vec![], "A → B の順で、 束ねた行が消えていない");
    assert_eq!(rows(&r1), rows(&r2), "B → A の順と A → B の順で答えが違う");

    drop((sa, sb, s1, s2, s3));
    drop((a, b, r1, r2, r3));
}

/// #546: 「2 つの世界番号は同じ entity」 の知らせ (`Op::Alias`、 #178 の `set_emit_alias`) が、 その世界番号の作成より先に、
/// その世界番号への Delete の後に届く。 知らせで別名を張る時も、 先に届いて退避していた削除を行に当てる (前は前向きの写像を
/// 入れるだけで、 その世界番号の record は以後写像の速い道で解決され、 削除が当たらなかった)。
///
/// 形: A が z を作る → P が (A を見ずに) z を作って配る → B が A の z を消す → P が A の z を自分の行に束ねて知らせを配る。
/// R は B → P → A の順 (削除が作成より先、 知らせも作成より先)、 R2 は A → B → P の順に pull する。 どちらも z は無い
/// (B の削除は P の z と A の z の両方より新しい)。
#[test]
fn alias_before_the_create_keeps_the_early_delete() {
    let dir = Dir::new("alias");
    let tr = transport(5);
    let (a, sa) = make(&dir, 1, &tr);
    let (b, sb) = make(&dir, 2, &tr);
    let (p, sp) = make(&dir, 3, &tr);
    let (r, sr) = make(&dir, 4, &tr);
    let (r2, sr2) = make(&dir, 5, &tr);
    p.engine().set_emit_alias(true);
    let later = || std::thread::sleep(std::time::Duration::from_millis(5));

    a.get_table(T).unwrap().upsert().set("key", "z").set("title", "a").commit().unwrap();
    publish(&a, &sa);
    later();
    p.get_table(T).unwrap().upsert().set("key", "z").set("title", "p").commit().unwrap();
    publish(&p, &sp);
    later();
    sb.pull_once(1);
    delete_all(&b);
    publish(&b, &sb);
    later();
    sp.pull_once(1);
    assert_eq!(p.engine().bind_over_local_writes(), 1, "前提: P が自分の書いた行に A の世界番号を束ねて知らせを書く");
    publish(&p, &sp);

    for from in [2, 3, 1] {
        assert!(sr.pull_once(from).received > 0, "R: {from} から何も届かない");
    }
    for from in [1, 2, 3] {
        assert!(sr2.pull_once(from).received > 0, "R2: {from} から何も届かない");
    }
    assert_eq!(rows(&r2), vec![], "前提: A → B → P の順で z は消える");
    assert_eq!(rows(&r), rows(&r2), "知らせが作成より先に届いた R で、 先に届いた削除が当たらない");
    assert_eq!(count_key(&r, "z"), 0);

    drop((sa, sb, sp, sr, sr2));
    drop((a, b, p, r, r2));
}
