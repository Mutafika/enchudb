//! #775: 名前で束ねた行から切り離す側に、 畳んだ自分の行が 2 つ以上ある時、 生き返らせる slot (`revive_folded_own`) を束ねた identity の
//! 並びの 「最初の 1 つ」 で選んでいた。 並びは memory では束ねた順、 きれいに閉じて開き直すと (peer, 番号) の順
//! (`PkFormer::serialize`、 #756 / #765 と同じ根) なので、 開き直すと生き返る slot = その行の世界番号 = 以後の書き込みの宛名が変わる。
//!
//! 形: C (peer 3) が K (名前 1) を作る。 A・B が受け、 A が K を消す (削除はまだ配らない)。 その後に C が x1 (名前 5) を作って D が
//! 受け、 x2 (名前 2) を作って G が受ける。 D が x1 を 2 に、 G が x2 を 7 に、 B (削除を知らない) が K を 7 に改名する。 C は
//! G → D → B の順に受けて、 x1 を x2 に畳み (改名した時に x2 がその名前だった)、 それを K に畳む (束ねた順は K, x2, x1)。
//! A の削除が C に届くと、 K は消え、 削除の後に作った x1 / x2 を切り離して、 畳んだ自分の行の slot に戻す: 開き直していない C は
//! 並びの最初の x2、 開き直した C は番号の小さい x1 だった。
//!
//! 相手で違いが出るのは、 x1 と x2 を別の行に持つ相手。 R (ホストでない中継 = 当たった record だけを先へ運ぶ) が B → G → C → D の
//! 順に受けると、 x2 の作成 (名前 2) は R で負けて先へ運ばれない。 R からだけ受ける F は、 x1 (名前 2) と K / x2 (名前 7) を別の
//! 行に持つ (ホストでない中継の限界、 #698。 この件より前からある)。 C がデッキ (名前 7) に書いた title は、 宛名が x2 なら F の
//! 名前 7 の行に、 x1 なら名前 2 の行に当たる。

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
const R: u32 = 5;
const F: u32 = 6;
const G: u32 = 7;
const O: u32 = 8;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-revive-own-{}-{tag}", std::process::id()));
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
    // 数の列だけ (語の record は受け手で 「当たった」 に数える)
    b.table(T).number("key").number("title").primary_key("key").build().unwrap();
    b.enable_sync().unwrap();
    attach(b.finish_with_oplog(OPLOG).unwrap(), path, id, tr)
}

impl Peer {
    fn publish(&self) {
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        while eng.transfer_oplog_to_sync_ops() > 0 {}
        self.sync.publish_since(Hlc::ZERO);
        // HLC の前後を分ける (同じ process の時計)
        std::thread::sleep(Duration::from_millis(3));
    }

    /// 名前だけの行を作る (他の列を書くと、 その record が中継で当たって identity が先へ運ばれる)。
    fn make(&self, key: i64) -> u64 {
        let e = self.db.get_table(T).unwrap().upsert().set("key", key).commit().unwrap();
        self.publish();
        e
    }

    fn rename(&self, from: i64, to: i64) {
        let e = self.eid_of(from).expect("改名する行がある");
        self.db.get_table(T).unwrap().entity(e).set("key", to).commit().unwrap();
        self.publish();
    }

    fn set_title(&self, key: i64, title: i64) {
        let e = self.eid_of(key).expect("書き換える行がある");
        self.db.get_table(T).unwrap().entity(e).set("title", title).commit().unwrap();
        self.publish();
    }

    /// 名前 `key` の行を消す (配らない)。
    fn delete_unpublished(&self, key: i64) {
        let e = self.eid_of(key).expect("消す行がある");
        self.db.get_table(T).unwrap().entity(e).delete().unwrap();
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        std::thread::sleep(Duration::from_millis(3));
    }

    fn eid_of(&self, key: i64) -> Option<u64> {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().where_eq("key", key).find().unwrap().first().copied()
    }

    /// (key, title) の並び (全部の行、 名前の無い行も)。
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

type Rows = Vec<(Option<Value>, Option<Value>)>;

fn named(key: i64) -> (Option<Value>, Option<Value>) {
    (Some(Value::Number(key)), None)
}

fn row(key: i64, title: i64) -> (Option<Value>, Option<Value>) {
    (Some(Value::Number(key)), Some(Value::Number(title)))
}

/// 名前 (key の値)
const K_NAME: i64 = 1;
const X1_NAME: i64 = 5;
const X2_NAME: i64 = 2;
const DECK: i64 = 7;
const TITLE: i64 = 40;

struct Out {
    /// C が後から作った 2 行の eid (x1 が先 = 番号が小さい)
    x1: u64,
    x2: u64,
    /// 削除を受けた後の、 C のデッキの eid
    deck: u64,
    /// R (C の行を翻訳して持つ peer) が同じ切り離しをした後の、 デッキの宛名
    address_at_r: Option<(u32, u32)>,
    c: Rows,
    r: Rows,
    f: Rows,
    /// 全部を受けた観測者 (削除が先)
    o: Rows,
}

fn run(reopen: bool) -> Out {
    let dir = Dir::new(if reopen { "reopen" } else { "live" });
    let tr = Arc::new(InMemoryTransport::new());
    for p in [A, B, C, D, R, F, G, O] {
        tr.register_peer(p);
    }
    let a = create(&dir.db("a"), A, &tr);
    let b = create(&dir.db("b"), B, &tr);
    let mut c = create(&dir.db("c"), C, &tr);
    let d = create(&dir.db("d"), D, &tr);
    let g = create(&dir.db("g"), G, &tr);
    let o = create(&dir.db("o"), O, &tr);
    let r = create(&dir.db("r"), R, &tr);
    // ホストでない中継: 当たった record だけを先へ運ぶ
    r.db.engine().set_gossip_remote_apply(true);
    let f = create(&dir.db("f"), F, &tr);

    // C が K を作る。 A・B が受け、 A が消す (配らない)。 その後に x1 (D が受ける)、 x2 (G が受ける)
    let k = c.make(K_NAME);
    a.sync.pull_once(C);
    b.sync.pull_once(C);
    a.delete_unpublished(K_NAME);
    let x1 = c.make(X1_NAME);
    d.sync.pull_once(C);
    let x2 = c.make(X2_NAME);
    g.sync.pull_once(C);
    assert!(k < x1 && x1 < x2, "前提: C の slot は作った順 ({k:x} {x1:x} {x2:x})");
    assert_eq!(d.rows(), vec![named(K_NAME), named(X1_NAME)], "前提: D は x2 を知らない");
    d.rename(X1_NAME, X2_NAME);
    g.rename(X2_NAME, DECK);
    b.rename(K_NAME, DECK);

    // C は G → D → B の順に受けて、 自分の 3 行を 1 つに畳む (束ねた順は K, x2, x1)
    c.sync.pull_once(G);
    assert_eq!(c.rows(), vec![named(K_NAME), named(X1_NAME), named(DECK)], "前提: G の改名で x2 の名前が変わった");
    c.sync.pull_once(D);
    assert_eq!(c.rows(), vec![named(K_NAME), named(DECK)], "前提: D の改名で x1 を x2 に畳んだ");
    assert_eq!(c.eid_of(DECK), Some(x2), "前提: 残した行は x2");
    c.sync.pull_once(B);
    assert_eq!(c.rows(), vec![named(DECK)], "前提: C の 3 行は 1 つ");
    assert_eq!(c.eid_of(DECK), Some(k), "前提: 残した行は K (x2 / x1 は畳んだ自分の行)");

    // R は B → G → C → D の順に受ける。 F は R からだけ受ける
    for from in [B, G, C, D] {
        r.sync.pull_once(from);
    }
    assert_eq!(r.rows(), vec![named(DECK)], "前提: R でも 1 行");
    r.publish();
    f.sync.pull_once(R);
    assert_eq!(f.rows(), vec![named(X2_NAME), named(DECK)], "前提: F は x1 (名前 2) と K / x2 (名前 7) を別の行に持つ");

    if reopen {
        // きれいに閉じて開き直す
        drop(c);
        c = attach(Database::open_with_oplog(&dir.db("c"), OPLOG).unwrap(), &dir.db("c"), C, &tr);
        assert_eq!(c.eid_of(DECK), Some(k), "前提: 開き直しても同じ行");
    }

    // A の削除が C に届く: K は消え、 x1 / x2 (削除の後に作った) を畳んだ自分の行の slot に戻す
    a.publish();
    let out = c.sync.pull_once(A);
    assert!(out.received > 0, "前提: A の削除が届いた ({out:?})");
    assert_eq!(c.rows(), vec![named(DECK)], "前提: 削除の後も C の行は 1 つ");
    let deck = c.eid_of(DECK).unwrap();
    assert!(deck == x1 || deck == x2, "前提: 畳んだ自分の行の slot に戻した ({deck:x})");
    assert!(!c.db.engine().is_live(k), "前提: K は消えた");

    // C がデッキを書き換え、 R を通して F が受ける
    c.set_title(DECK, TITLE);
    r.sync.pull_once(C);
    r.publish();
    f.sync.pull_once(R);

    // R も同じ切り離しをする (C の行を翻訳して持つ側の宛名)
    let out = r.sync.pull_once(A);
    assert!(out.received > 0, "前提: A の削除が R に届いた ({out:?})");
    let at_r = r.eid_of(DECK).expect("R にデッキがある");
    let address_at_r = r.db.engine().eid_translator().reverse(enchudb_oplog::eid_local(at_r));

    // 観測者は削除を先に受ける
    for from in [A, C, G, D, B] {
        o.sync.pull_once(from);
    }
    Out { x1, x2, deck, address_at_r, c: c.rows(), r: r.rows(), f: f.rows(), o: o.rows() }
}

fn check(o: &Out) {
    assert_eq!(o.c, vec![row(DECK, TITLE)]);
    assert_eq!(o.r, vec![row(DECK, TITLE)]);
    assert_eq!(o.o, vec![row(DECK, TITLE)], "全部を受けた観測者は、 宛名によらず同じ答え");
    assert_eq!(
        o.address_at_r,
        Some((C, enchudb_oplog::eid_local(o.x2))),
        "前提: C の行を翻訳して持つ peer の宛名は、 作成のいちばん新しい identity (x2、 #768)"
    );
    assert_eq!(o.deck, o.x2, "C が生き返らせた slot が x2 でない (x1 = {:x})", o.x1);
    assert_eq!(o.f, vec![named(X2_NAME), row(DECK, TITLE)], "C の書き換えが、 F の名前 7 の行でない行に当たった");
}

/// 対照: 開き直さない C は、 作成のいちばん新しい自分の行 (x2) の slot を生き返らせる。
#[test]
fn live_peer_revives_the_newest_folded_own_row() {
    check(&run(false));
}

/// 開き直した C も同じ slot (= 同じ世界番号、 同じ宛名)。
#[test]
fn reopened_peer_revives_the_same_folded_own_row() {
    check(&run(true));
}
