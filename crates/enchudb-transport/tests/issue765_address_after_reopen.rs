//! #765: 切り離した identity を新しい slot に付け替える時 (`point_identities_to`)、 束ねた identity の並びの 「最初の相手」 を
//! 宛名 (逆写像) にしていた。 並びは、 きれいに閉じて開き直すと (peer, 番号) の順になる (`PkFormer::serialize`、 #756 と同じ根)。
//!
//! 形 (ホストを置かない中継 = 当たった record だけを先へ運ぶ): A (peer 1) が k1 を作って消す (削除はまだ配らない)。 その後に
//! C (peer 3)、 D (peer 4) の順で同じ名前の行を作る (どちらも削除の後 = 削除では消えないデッキ)。 B (peer 2) は A の作成 →
//! D の行 → C の行 の順に受けて 1 行に束ねる。 C の行は D の行より古いので B で負ける = B は先へ運ばない。 F (peer 5) は B から
//! だけ受けるので、 C の identity を知らない。
//!
//! A の削除が B に届くと、 B は C / D の identity を新しい slot に移す。 宛名は、 開き直していない B では先に届いた D の identity、
//! 開き直した B では番号の小さい C の identity。 その後の B の書き換えは宛名宛てに出るので、 開き直した B の書き換えは F の
//! 知らない世界番号宛て = F で別の行になる。

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
const D: u32 = 4;
const F: u32 = 5;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue765-{}-{tag}", std::process::id()));
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
const FROM_C: i64 = 20;
const FROM_D: i64 = 30;
const FROM_B_LATER: i64 = 40;

/// 最後の (B の行, F の行)。
fn run(reopen: bool) -> (Vec<(Option<Value>, Option<Value>)>, Vec<(Option<Value>, Option<Value>)>) {
    let dir = Dir::new(if reopen { "reopen" } else { "live" });
    let tr = Arc::new(InMemoryTransport::new());
    for p in [A, B, C, D, F] {
        tr.register_peer(p);
    }
    let a = create(&dir.db("a"), A, &tr);
    let mut b = create(&dir.db("b"), B, &tr);
    // ホストでない中継: 当たった record だけを先へ運ぶ
    b.db.engine().set_gossip_remote_apply(true);
    let c = create(&dir.db("c"), C, &tr);
    let d = create(&dir.db("d"), D, &tr);
    let f = create(&dir.db("f"), F, &tr);

    // A が k1 を作って配り、 消す (削除はまだ配らない)。 その後に C、 D の順で同じ名前の行を作る
    a.upsert(K1, FROM_A);
    std::thread::sleep(Duration::from_millis(3));
    a.delete(K1, false);
    std::thread::sleep(Duration::from_millis(3));
    c.upsert(K1, FROM_C);
    std::thread::sleep(Duration::from_millis(3));
    d.upsert(K1, FROM_D);

    // B は A の作成 → D の行 → C の行 の順に受ける (名前で 1 行に束ねる。 C の行は D の行より古いので負ける)
    let out = b.sync.pull_once(A);
    assert!(out.applied > 0, "前提: A の作成が B に当たった ({out:?})");
    assert_eq!(b.rows(), vec![row(K1, FROM_A)]);
    let out = b.sync.pull_once(D);
    assert!(out.applied > 0, "前提: D の行が B に当たった ({out:?})");
    let out = b.sync.pull_once(C);
    assert!(out.received > 0, "前提: C の行が B に届いた ({out:?})");
    assert_eq!(b.rows(), vec![row(K1, FROM_D)], "前提: B の行は 1 つ (3 つの identity を束ねた、 値は D の物)");

    // F は B からだけ受ける (B で負けた C の行は来ない)
    b.publish();
    f.sync.pull_once(B);
    assert_eq!(f.rows(), vec![row(K1, FROM_D)], "前提: F は B の行を受けた");

    if reopen {
        // きれいに閉じて開き直す
        drop(b);
        b = attach(Database::open_with_oplog(&dir.db("b"), OPLOG).unwrap(), &dir.db("b"), B, &tr);
        b.db.engine().set_gossip_remote_apply(true);
        assert_eq!(b.rows(), vec![row(K1, FROM_D)], "前提: 開き直しても同じ行");
    }

    // A の削除が届く (C / D の行は削除の後に作られた = 残る。 B は 2 つの identity を新しい slot に移す)
    a.publish();
    let before = b.eid_of(K1).expect("前提: B に k1 の行がある");
    let out = b.sync.pull_once(A);
    assert!(out.received > 0, "前提: A の削除が届いた ({out:?})");
    assert_eq!(b.rows(), vec![row(K1, FROM_D)], "前提: 削除の後も B の行は 1 つ");
    assert_ne!(b.eid_of(K1), Some(before), "前提: B は残る identity を新しい slot に移した");
    b.publish();
    f.sync.pull_once(B);
    assert_eq!(f.rows(), vec![row(K1, FROM_D)], "前提: 削除を受けた後も F の行は 1 つ");

    // B がその行を書き換え、 F が受ける
    b.upsert(K1, FROM_B_LATER);
    f.sync.pull_once(B);
    (b.rows(), f.rows())
}

/// 対照: 開き直さない B の書き換えは、 F の同じ行に届く。
#[test]
fn live_peer_addresses_the_row_the_downstream_knows() {
    let (b, f) = run(false);
    assert_eq!(b, vec![row(K1, FROM_B_LATER)]);
    assert_eq!(f, vec![row(K1, FROM_B_LATER)]);
}

/// 開き直した B も同じ答え。
#[test]
fn reopened_peer_addresses_the_row_the_downstream_knows() {
    let (b, f) = run(true);
    assert_eq!(b, vec![row(K1, FROM_B_LATER)]);
    assert_eq!(f, vec![row(K1, FROM_B_LATER)], "F が B の書き換えを同じ行に受けていない (B の宛名が F の知らない identity)");
}
