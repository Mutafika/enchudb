//! #612: 相手の改名で、 自分が作った行 2 つが同じ名前 (= 同じデッキ) になった時、 自分の行どうしも 1 つに畳む。
//!
//! 畳まれた行の eid は残す行に転送する (ユーザの決め、 2026-10-22): アプリが持っている eid での読み書きも、 相手がその世界番号宛てに
//! 書いた record も、 残す行に届く。 畳まれた行の slot は使い回さない (自分の行の世界番号は slot の番号そのもの)。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const T: &str = "t";
/// t の行を指す ref を持つ表
const U: &str = "u";
const OPLOG: usize = 1 << 20;

/// test ごとの directory。 Drop で消す。
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("own-fold-{tag}-{}", std::process::id()));
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

fn make(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>, capacity: Option<u32>) -> Peer {
    let mut b = Database::create_with_capacity(&dir.db(id), 4096).unwrap();
    let t = b.table(T).tag("key").tag("title").number("n").primary_key("key");
    let t = match capacity {
        Some(n) => t.with_capacity(n),
        None => t,
    };
    t.build().unwrap();
    b.table(U).tag("name").ref_to("of", T).primary_key("name").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
    Peer { db, sync }
}

/// 開き直す (peer の番号は DB の header に残っているので、 set_peer_id は呼ばない)。
fn reopen(dir: &Dir, id: u32, tr: &Arc<InMemoryTransport>) -> Peer {
    let db = Database::open_with_oplog(&dir.db(id), OPLOG).unwrap();
    assert_eq!(db.engine().peer_id(), id);
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
        // HLC の順を作る (同じ process の時計なので、 間を空ければ後に書いた方が新しい)
        std::thread::sleep(Duration::from_millis(3));
    }

    fn pull(&self, from: u32) {
        self.sync.pull_once(from);
    }

    fn upsert(&self, key: &str, title: &str) -> u64 {
        let e = self.db.get_table(T).unwrap().upsert().set("key", key).set("title", title).commit().unwrap();
        self.publish();
        e
    }

    fn rows_named(&self, key: &str) -> Vec<u64> {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().where_eq("key", key).find().unwrap()
    }

    fn row_of(&self, key: &str) -> u64 {
        let found = self.rows_named(key);
        assert_eq!(found.len(), 1, "peer {}: 名前 {key} の行が {} 個", self.db.engine().peer_id(), found.len());
        found[0]
    }

    fn set_title(&self, e: u64, title: &str) {
        self.db.get_table(T).unwrap().entity(e).set("title", title).commit().unwrap();
        self.publish();
    }

    fn get(&self, e: u64, col: &str) -> Option<Value> {
        self.db.get_table(T).unwrap().entity(e).get(col)
    }

    /// title のある行の数 (名前の無い塊も数える)。
    fn titled_rows(&self) -> usize {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().all().where_ne("title", "\u{0}").find().unwrap().len()
    }
}

fn text(s: &str) -> Option<Value> {
    Some(Value::Text(s.into()))
}

/// C が作った k1 / k0 のうち、 k1 を B が k0 に改名する (B は C の k0 を知らない)。 A は改名の前に C の 2 行を受けておく。
/// C が B の改名を受けると、 C の 2 行は同じ名前 = 同じデッキ。 戻り値は (C の残した行, C の畳まれた行)。
fn fold_setup(a: &Peer, b: &Peer, c: &Peer) -> (u64, u64) {
    let e1 = c.upsert("k1", "t1");
    b.pull(3);
    let e0 = c.upsert("k0", "t0");
    a.pull(3);
    let at_b = b.row_of("k1");
    b.db.get_table(T).unwrap().entity(at_b).set("key", "k0").commit().unwrap();
    b.publish();
    c.pull(2);
    let kept = c.row_of("k0");
    assert!(kept == e1 || kept == e0, "C の残した行は C の 2 行のどちらか");
    let folded = if kept == e1 { e0 } else { e1 };
    (kept, folded)
}

/// 畳まれた行の eid での読み書きは残す行に届く。 書いた分は相手にも配られる。
#[test]
fn a_folded_own_row_eid_forwards_reads_and_writes() {
    let dir = Dir::new("app");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 3] {
        tr.register_peer(id);
    }
    let (a, b, c) = (make(&dir, 1, &tr, None), make(&dir, 2, &tr, None), make(&dir, 3, &tr, None));
    let (kept, folded) = fold_setup(&a, &b, &c);
    assert!(c.db.engine().is_live(folded), "畳まれた行の eid もデッキとして生きている (転送)");
    assert_eq!(c.get(folded, "key"), text("k0"));
    assert_eq!(c.get(folded, "title"), c.get(kept, "title"));
    c.db.get_table(T).unwrap().entity(kept).set("n", 7i64).commit().unwrap();
    assert_eq!(c.get(folded, "n"), Some(Value::Number(7)), "畳まれた eid で数の列を読むと残す行の値でない");
    let eng = c.db.engine();
    let n = eng.himo_id("t.n").unwrap() as u16;
    assert_eq!(eng.get_by_id(folded, n), Some(7), "engine の get_by_id が畳まれた eid を転送しない");
    c.set_title(folded, "x");
    assert_eq!(c.get(kept, "title"), text("x"), "畳まれた eid への書き込みが残す行に当たらない");
    assert_eq!(c.rows_named("k0").len(), 1);
    b.pull(3);
    assert_eq!(b.get(b.row_of("k0"), "title"), text("x"), "畳まれた eid への書き込みが相手に配られない");
}

/// 相手が畳まれた行の世界番号宛てに書いた record も残す行に届く。 開き直しても転送は残る (別名は `.eidalias`)。
#[test]
fn records_to_a_folded_own_row_land_in_the_kept_row_after_a_reopen() {
    let dir = Dir::new("reopen");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 3] {
        tr.register_peer(id);
    }
    let (a, b, c) = (make(&dir, 1, &tr, None), make(&dir, 2, &tr, None), make(&dir, 3, &tr, None));
    let (kept, folded) = fold_setup(&a, &b, &c);
    c.db.engine().persist_tables().unwrap();
    drop(c);
    let c = reopen(&dir, 3, &tr);
    assert_eq!(c.get(folded, "title"), c.get(kept, "title"), "開き直すと畳まれた eid を転送しない");
    // A は改名を知らないので、 A の行 (C の 2 行の写し) はそれぞれ C の 2 つの世界番号宛てに書く
    for key in ["k1", "k0"] {
        let e = a.row_of(key);
        a.set_title(e, &format!("a-{key}"));
    }
    c.pull(1);
    assert_eq!(c.rows_named("k0").len(), 1, "開き直した後に、 畳まれた世界番号宛ての record が別の行を作った");
    assert!(c.rows_named("k1").is_empty());
    assert_eq!(c.titled_rows(), 1, "畳まれた世界番号宛ての record が名前の無い行を作った");
    assert_eq!(c.get(kept, "title"), text("a-k0"), "後に書いた A の title (畳まれた / 残した世界番号宛て) が残す行に当たらない");
}

/// 表の枠が尽きて空いた slot を拾う時も、 畳まれた行の slot は使い回さない。 使い回すと、 新しい行の record が畳まれた世界番号
/// 宛てに出て、 相手では残した行 (別のデッキ) に当たる。
#[test]
fn a_folded_own_slot_is_not_reused_when_the_table_is_full() {
    let dir = Dir::new("reuse");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 3] {
        tr.register_peer(id);
    }
    let (a, b, c) = (make(&dir, 1, &tr, None), make(&dir, 2, &tr, None), make(&dir, 3, &tr, Some(4)));
    let (_, folded) = fold_setup(&a, &b, &c);
    let mut made = Vec::new();
    for i in 5..9 {
        made.push(c.upsert(&format!("k{i}"), "n"));
    }
    assert!(!made.contains(&folded), "畳まれた行の slot {folded:x} を新しい行に使い回した: {made:x?}");
    b.pull(3);
    for i in 5..9 {
        assert_eq!(b.rows_named(&format!("k{i}")).len(), 1);
    }
    assert_eq!(b.rows_named("k0").len(), 1);
}

/// 相手が畳まれた行の世界番号宛てに出した削除は、 残す行 (= 同じデッキ) を消す。 A は改名を知らないまま C の k0 (畳まれた行) を
/// 消す。 削除は改名の後なので、 デッキごと消える (B も同じ答え)。
#[test]
fn a_delete_addressed_to_a_folded_own_row_deletes_the_deck() {
    let dir = Dir::new("delete");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 3] {
        tr.register_peer(id);
    }
    let (a, b, c) = (make(&dir, 1, &tr, None), make(&dir, 2, &tr, None), make(&dir, 3, &tr, None));
    let (_, folded) = fold_setup(&a, &b, &c);
    let at_a = a.row_of("k0");
    a.db.get_table(T).unwrap().entity(at_a).delete().unwrap();
    a.publish();
    c.pull(1);
    b.pull(1);
    b.pull(3);
    assert!(b.rows_named("k0").is_empty(), "前提: B では A の削除がデッキを消す");
    assert!(c.rows_named("k0").is_empty(), "畳まれた行 {folded:x} 宛ての削除が残す行に届かない");
}

/// 相手の行の ref が畳まれた行 (の世界番号) を指していたら、 受けた ref の値は残す行を指す (逆引きに出る)。
#[test]
fn a_ref_to_a_folded_own_row_points_at_the_kept_row() {
    let dir = Dir::new("ref");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 3] {
        tr.register_peer(id);
    }
    let (a, b, c) = (make(&dir, 1, &tr, None), make(&dir, 2, &tr, None), make(&dir, 3, &tr, None));
    let (kept, folded) = fold_setup(&a, &b, &c);
    // A は改名を知らないので、 A の k0 の行は C の k0 (畳まれた行) の写し
    for key in ["k1", "k0"] {
        let target = a.row_of(key);
        a.db.get_table(U).unwrap().insert().set("name", key).set("of", Value::Ref(target)).commit().unwrap();
    }
    a.publish();
    c.pull(1);
    c.db.engine().rebuild();
    let refs = c.db.get_table(U).unwrap().where_ref("of", kept).find().unwrap();
    assert_eq!(refs.len(), 2, "畳まれた行 {folded:x} を指す ref が残す行を指さない");
}

/// 残した行を消した後に表の枠が尽きて、 残した行の slot が使い回されても、 畳まれた行の slot は予約したまま (使い回さない)、 その
/// 世界番号宛ての record は捨てる (消えたデッキ宛て)。 別名を外すだけだと、 A が畳まれた行の写しに書いた title が C で名前の無い行を作る。
#[test]
fn a_folded_own_slot_stays_reserved_after_the_kept_slot_is_reused() {
    let dir = Dir::new("reuse-kept");
    let tr = Arc::new(InMemoryTransport::new());
    for id in [1, 2, 3] {
        tr.register_peer(id);
    }
    let (a, b, c) = (make(&dir, 1, &tr, None), make(&dir, 2, &tr, None), make(&dir, 3, &tr, Some(4)));
    let (kept, folded) = fold_setup(&a, &b, &c);
    c.db.get_table(T).unwrap().entity(kept).delete().unwrap();
    c.publish();
    let mut made = Vec::new();
    for i in 5..10 {
        made.push(c.upsert(&format!("k{i}"), "n"));
    }
    assert!(made.contains(&kept), "前提: 枠が尽きて、 消した残す行の slot {kept:x} を使い回す: {made:x?}");
    assert!(!made.contains(&folded), "畳まれた行の slot {folded:x} を使い回した: {made:x?}");
    // A は削除も改名も知らないまま、 C の 2 行の写しに書く
    for key in ["k1", "k0"] {
        let e = a.row_of(key);
        a.set_title(e, "stale");
    }
    c.pull(1);
    assert_eq!(c.titled_rows(), 5, "消えたデッキの世界番号宛ての record が C で行を作った");
}
