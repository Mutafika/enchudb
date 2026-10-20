//! Reproduction harness for the 0.35.0 pk-tombstone review.
//!
//! S1: receiver X locally deletes a replica of A's PK row, its slot gets reused by local inserts
//!     (table range exhausted -> rebuild_free_locals), then A's own (newer) delete arrives for the
//!     evicted identity, then a third author's row with the same PK (dated between the two deletes).
//!     Control X2 does the same without slot reuse.
//! S3: Op::Alias (#178, opt-in) arriving before the aliased identity's create, after an early Delete.
use std::path::PathBuf;
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
        let dir = std::env::temp_dir()
            .join("enchudb-review-2026-10-20")
            .join(format!("{tag}-{}", std::process::id()));
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

fn make(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>, cap: u32) -> (Arc<Database>, Syncer) {
    let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
    b.table(T).tag("key").tag("title").primary_key("key").with_capacity(cap).build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    (db, sync)
}

fn publish(db: &Database, s: &Syncer) {
    let eng = db.engine();
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    s.publish_since(Hlc::ZERO);
}

fn pull(s: &Syncer, from: u32) {
    let o = s.pull_once(from);
    assert!(o.received > 0, "{from} から何も届かない: {o:?}");
}

fn later() {
    std::thread::sleep(Duration::from_millis(5));
}

type Row = (Option<Value>, Option<Value>);

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
            (r.get("key"), r.get("title"))
        })
        .collect();
    out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    out
}

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

/// S1
fn scenario_slot_reuse() {
    println!("=== S1: local delete + slot reuse + later remote delete + third-party row in between ===");
    let dir = Dir::new("s1");
    let tr = transport(5);
    const CAP: u32 = 8;
    let (a, sa) = make(&dir, 1, &tr, CAP);
    let (x, sx) = make(&dir, 2, &tr, CAP); // reuses the slot
    let (y, sy) = make(&dir, 3, &tr, CAP); // never deleted locally
    let (f, sf) = make(&dir, 4, &tr, CAP); // third author
    let (x2, sx2) = make(&dir, 5, &tr, CAP); // control: like X but no slot reuse

    upsert(&a, "z", "a");
    publish(&a, &sa);
    pull(&sx, 1);
    pull(&sy, 1);
    pull(&sx2, 1);
    later();
    // T1: X and X2 delete their replica of z locally
    delete_key(&x, "z");
    delete_key(&x2, "z");
    assert_eq!(x.engine().deleted_primary_key_count(), 1);
    // X: fill the table range so that the freed slot is reused (rebuild_free_locals on exhaustion)
    {
        let t = x.get_table(T).unwrap();
        for i in 0..CAP {
            t.insert().set("key", format!("w{i}")).set("title", "w").commit().unwrap();
        }
        // make room for F's row later: delete one local filler row
        let found = t.where_eq("key", "w0").find().unwrap();
        t.entity(found[0]).delete().unwrap();
    }
    later();
    // T1.5: F creates z independently (between X's delete and A's delete)
    upsert(&f, "z", "f");
    publish(&f, &sf);
    later();
    // T2: A deletes its z
    delete_key(&a, "z");
    publish(&a, &sa);

    for (s, name) in [(&sx, "X"), (&sx2, "X2"), (&sy, "Y")] {
        let o1 = s.pull_once(1);
        let o4 = s.pull_once(4);
        println!("{name}: pull A -> {o1:?}\n{name}: pull F -> {o4:?}");
    }
    let rx = rows(&x).into_iter().filter(|r| r.0 != text("w") && r.1 != text("w")).collect::<Vec<_>>();
    println!("X  rows (non-filler): {rx:?}  titled f = {}", titled(&x, "f"));
    println!("X2 rows: {:?}  titled f = {}", rows(&x2), titled(&x2, "f"));
    println!("Y  rows: {:?}  titled f = {}", rows(&y), titled(&y, "f"));
    println!("A  rows: {:?}", rows(&a));
    println!("deleted_primary_key_count X={} X2={} Y={}", x.engine().deleted_primary_key_count(), x2.engine().deleted_primary_key_count(), y.engine().deleted_primary_key_count());
    let diverged = titled(&x, "f") != titled(&y, "f");
    println!("S1 RESULT: X vs Y {}", if diverged { "DIVERGED" } else { "same" });
    println!("S1 control: X2 vs Y {}", if titled(&x2, "f") != titled(&y, "f") { "DIVERGED" } else { "same" });
    drop((sa, sx, sy, sf, sx2));
    drop((a, x, y, f, x2));
}

/// S3: Alias (#178) before the aliased identity's create, after an early delete of that identity.
fn scenario_alias(emit: bool) {
    println!("=== S3 (emit_alias={emit}): early Delete(A:1), then P's row + Alias(P:p -> A:1), then A's create ===");
    let dir = Dir::new("s3");
    let tr = transport(5);
    let (a, sa) = make(&dir, 1, &tr, 64);
    let (b, sb) = make(&dir, 2, &tr, 64);
    let (p, sp) = make(&dir, 3, &tr, 64);
    let (r, sr) = make(&dir, 4, &tr, 64); // pulls B -> P -> A
    let (r2, sr2) = make(&dir, 5, &tr, 64); // pulls A -> B -> P
    p.engine().set_emit_alias(emit);

    upsert(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert(&p, "z", "p"); // P's own row, before P sees A's
    publish(&p, &sp); // goes out under P's own world id (before the bind)
    later();
    pull(&sb, 1);
    delete_key(&b, "z"); // B deletes A's z (T)
    publish(&b, &sb);
    later();
    pull(&sp, 1); // P binds A:1 onto its own row -> emits Alias(P:p, A:1)
    publish(&p, &sp);

    for (s, order, name) in [(&sr, [2u32, 3, 1], "R"), (&sr2, [1u32, 2, 3], "R2")] {
        for from in order {
            let o = s.pull_once(from);
            let db = if name == "R" { &r } else { &r2 };
            println!("{name}: pull {from} -> applied {} skipped {} dropped {} | rows {:?} titled p={} a={} entity_count={}", o.applied, o.skipped, o.dropped_unresolved, rows(db), titled(db, "p"), titled(db, "a"), db.engine().entity_count());
        }
    }
    println!("R  rows: {:?}  titled p = {} titled a = {}", rows(&r), titled(&r, "p"), titled(&r, "a"));
    println!("R2 rows: {:?}  titled p = {} titled a = {}", rows(&r2), titled(&r2, "p"), titled(&r2, "a"));
    println!("P  rows: {:?}", rows(&p));
    println!("A  rows: {:?}", rows(&a));
    println!("S3 RESULT: R vs R2 {}", if rows(&r) != rows(&r2) { "DIVERGED" } else { "same" });
    drop((sa, sb, sp, sr, sr2));
    drop((a, b, p, r, r2));
}

/// S4: the zombie row (newer non-PK write survives) followed by an upsert of the same PK.
fn scenario_zombie_upsert() {
    println!("=== S4: zombie row + upsert of the same PK ===");
    let dir = Dir::new("s4");
    let tr = transport(3);
    let (a, sa) = make(&dir, 1, &tr, 64);
    let (b, sb) = make(&dir, 2, &tr, 64);
    let (c, sc) = make(&dir, 3, &tr, 64);
    upsert(&a, "z", "a");
    publish(&a, &sa);
    pull(&sb, 1);
    later();
    delete_key(&a, "z");
    publish(&a, &sa);
    later();
    {
        b.engine().rebuild();
        let t = b.get_table(T).unwrap();
        let e = t.where_eq("key", "z").find().unwrap();
        t.entity(e[0]).update().set("title", "b2").commit().unwrap();
    }
    publish(&b, &sb);
    pull(&sb, 1);
    pull(&sa, 2);
    pull(&sc, 1);
    pull(&sc, 2);
    for (n, db) in [("A", &a), ("B", &b), ("C", &c)] {
        println!("{n}: all()={:?} titled b2={} entity_count={}", rows(db), titled(db, "b2"), db.engine().entity_count());
    }
    // now upsert z again on C: does it find the zombie?
    upsert(&c, "z", "c2");
    c.engine().rebuild();
    let t = c.get_table(T).unwrap();
    println!("C after upsert z: all()={:?} where_eq(title,b2)={} where_eq(key,z)={} entity_count={}",
        rows(&c), titled(&c, "b2"), t.where_eq("key", "z").find().unwrap().len(), c.engine().entity_count());
    println!("C: all().count()={} all().where_ne(title,x).count()={} where_eq(title,b2).count()={}",
        t.all().count().unwrap(), t.all().where_ne("title", "x").count().unwrap(), t.where_eq("title", "b2").count().unwrap());
    let live_all = t.all().subscribe().unwrap();
    let live_b2 = t.where_eq("title", "b2").subscribe().unwrap();
    let counts = t.all().where_ne("key", "nope").subscribe_counts("title").unwrap();
    let counts2 = t.where_eq("title", "b2").subscribe_counts("title").unwrap();
    println!("C: subscribe all()={} subscribe where_eq(title,b2)={} counts(where_ne key).all()={:?} counts(where_eq title b2)={:?}",
        live_all.poll().added.len(), live_b2.poll().added.len(), counts.all(), counts2.all());
    drop((sa, sb, sc));
    drop((a, b, c));
}


/// rows inserted with the non-PK column set first (title, then key) -> the title Tie precedes the PK Tie in the batch.
fn upsert_title_first(db: &Database, key: &str, title: &str) {
    db.get_table(T).unwrap().upsert().set("title", title).set("key", key).commit().unwrap();
}

/// S5: #141 binding when the PK Tie is not the first Tie of the entity in the batch (post-#509).
fn scenario_pk_not_first() {
    println!("=== S5: two authors create the same PK; receiver pulls both. (a) PK column set LAST, (b) PK set FIRST ===");
    let dir = Dir::new("s5");
    let tr = transport(6);
    let (a, sa) = make(&dir, 1, &tr, 64);
    let (b, sb) = make(&dir, 2, &tr, 64);
    let (c, sc) = make(&dir, 3, &tr, 64);
    let (p, sp) = make(&dir, 4, &tr, 64);
    let (q, sq) = make(&dir, 5, &tr, 64);
    let (r, sr) = make(&dir, 6, &tr, 64);
    upsert_title_first(&a, "z", "a");
    upsert(&p, "z", "p");
    later();
    upsert_title_first(&b, "z", "b");
    upsert(&q, "z", "q");
    publish(&a, &sa);
    publish(&b, &sb);
    publish(&p, &sp);
    publish(&q, &sq);
    pull(&sc, 1);
    pull(&sc, 2);
    pull(&sr, 4);
    pull(&sr, 5);
    println!("(a) PK set last : C rows = {:?}", rows(&c));
    println!("(b) PK set first: R rows = {:?}", rows(&r));
    drop((sa, sb, sc, sp, sq, sr));
    drop((a, b, c, p, q, r));
}

/// S6: #513 with the PK column set last: A creates z and deletes it; B independently created z (older than the delete).
fn scenario_pk_not_first_delete() {
    println!("=== S6: #513 shape, but rows are written with the PK column set LAST ===");
    let dir = Dir::new("s6");
    let tr = transport(4);
    let (a, sa) = make(&dir, 1, &tr, 64);
    let (b, sb) = make(&dir, 2, &tr, 64);
    let (c, sc) = make(&dir, 3, &tr, 64); // A -> B
    let (d, sd) = make(&dir, 4, &tr, 64); // B -> A
    upsert_title_first(&a, "z", "a");
    publish(&a, &sa);
    later();
    upsert_title_first(&b, "z", "b");
    publish(&b, &sb);
    later();
    delete_key(&a, "z");
    publish(&a, &sa);
    pull(&sc, 1);
    pull(&sc, 2);
    pull(&sd, 2);
    pull(&sd, 1);
    pull(&sb, 1);
    pull(&sa, 2);
    for (n, db) in [("A", &a), ("B", &b), ("C (A->B)", &c), ("D (B->A)", &d)] {
        println!("{n}: rows {:?} titled b={}", rows(db), titled(db, "b"));
    }
    println!("S6 RESULT: C vs D {}", if rows(&c) != rows(&d) { "DIVERGED" } else { "same" });
    drop((sa, sb, sc, sd));
    drop((a, b, c, d));
}

fn main() {
    scenario_slot_reuse();
    scenario_alias(true);
    scenario_alias(false);
    scenario_zombie_upsert();
    scenario_pk_not_first();
    scenario_pk_not_first_delete();
}
