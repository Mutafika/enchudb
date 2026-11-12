//! #756: 束ねた identity の並びが、 きれいに閉じて開き直すと変わり、 `split_later_identities` が元の identity を取り違えていた。
//!
//! `pkformer` のまとめ直し (`PkFormer::serialize`、 きれいに閉じる時) は束ねた identity を (peer, 番号) の順に並べて書く。
//! memory の並びは 「先頭が行の元の identity」 で、 `split_later_identities` は先頭を元の identity として読んでいた。 自分の行に
//! 番号の小さい peer の identity を束ねた DB は、 開き直すと先頭が相手の identity になる。
//!
//! 形: A (peer 1) が k1 を作って消す。 その後 B (peer 2) が同じ名前 k1 の行を作る (削除の後に作られた = 別のデッキ)。 B は A の
//! 作成だけを先に受けて束ね、 削除を後から受ける。 正しくは B の行がそのまま残る (A の identity だけが消えた slot に付く)。
//! 開き直した B は、 A の identity を元の identity と読んで 「元の行は消える」 側に入り、 自分の行を消して中身を新しい slot に
//! 移す = 自分の行の世界番号が変わる (その後の B の書き込みは、 相手の知らない世界番号宛てに出る)。

use std::path::PathBuf;
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
const C: u32 = 3;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue756-{}-{tag}", std::process::id()));
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

    /// 名前 `key` の行を消す。 `publish` でなければ配らない (WAL に置くだけ)。
    fn delete(&self, key: i64, publish: bool) {
        let t = self.db.get_table(T).unwrap();
        let e = self.eid_of(key).expect("消す行がある");
        t.entity(e).delete().unwrap();
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        if publish {
            self.publish();
        }
    }

    fn publish(&self) {
        let eng = self.db.engine();
        eng.oplog_sync().unwrap();
        while eng.transfer_oplog_to_sync_ops() > 0 {}
        self.sync.publish_since(Hlc::ZERO);
    }

    /// 名前 `key` の行の eid。
    fn eid_of(&self, key: i64) -> Option<u64> {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().where_eq("key", key).find().unwrap().first().copied()
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
const FROM_B_LATER: i64 = 30;


/// B の k1 の eid と、 最後の (B の行, C の行)。
fn run(reopen: bool) -> (u64, u64, Vec<(Option<Value>, Option<Value>)>, Vec<(Option<Value>, Option<Value>)>) {
    let dir = Dir::new(if reopen { "reopen" } else { "live" });
    let tr = Arc::new(InMemoryTransport::new());
    for p in [A, B, C] {
        tr.register_peer(p);
    }
    let a = create(&dir.db("a"), A, &tr);
    let mut b = create(&dir.db("b"), B, &tr);
    let c = create(&dir.db("c"), C, &tr);

    // A が k1 を作って配り、 消す (削除はまだ配らない)。 その後に B が同じ名前の行を作る
    a.upsert(K1, FROM_A);
    std::thread::sleep(Duration::from_millis(3));
    a.delete(K1, false);
    std::thread::sleep(Duration::from_millis(3));
    b.upsert(K1, FROM_B);
    let before = b.eid_of(K1).expect("前提: B に k1 の行がある");
    c.sync.pull_once(B);
    assert_eq!(c.rows(), vec![row(K1, FROM_B)], "前提: C は B の行を受けた");

    // B は A の作成だけを受ける (名前で束ねる)
    let out = b.sync.pull_once(A);
    assert!(out.received > 0, "前提: A の作成が届いた ({out:?})");
    assert_eq!(b.rows(), vec![row(K1, FROM_B)], "前提: B の行は 1 つ (A の作成は負けて束ねただけ)");
    assert_eq!(b.eid_of(K1), Some(before));

    if reopen {
        // きれいに閉じて開き直す
        drop(b);
        b = attach(Database::open_with_oplog(&dir.db("b"), OPLOG).unwrap(), &dir.db("b"), B, &tr);
        assert_eq!(b.eid_of(K1), Some(before), "前提: 開き直しても同じ行");
    }

    // A の削除が届く (B の行は削除の後に作られた = 残る)
    a.publish();
    let out = b.sync.pull_once(A);
    assert!(out.received > 0, "前提: A の削除が届いた ({out:?})");
    let after = b.eid_of(K1).expect("B の k1 の行が消えた");

    // B がその行を書き換え、 C が受ける
    b.upsert(K1, FROM_B_LATER);
    c.sync.pull_once(B);
    (before, after, b.rows(), c.rows())
}

/// 対照: 開き直さない B は、 自分の行をそのまま残す。
#[test]
fn live_peer_keeps_its_own_row() {
    let (before, after, b, c) = run(false);
    assert_eq!(after, before, "B の行の世界番号が変わった");
    assert_eq!(b, vec![row(K1, FROM_B_LATER)]);
    assert_eq!(c, vec![row(K1, FROM_B_LATER)]);
}

/// 開き直した B も同じ答え。
#[test]
fn reopened_peer_keeps_its_own_row() {
    let (before, after, b, c) = run(true);
    assert_eq!(after, before, "開き直した B が、 自分の行を消して別の slot に移した (世界番号が変わった)");
    assert_eq!(b, vec![row(K1, FROM_B_LATER)]);
    assert_eq!(c, vec![row(K1, FROM_B_LATER)], "C が B の書き換えを B の行に受けていない");
}
