//! #779: bridge (`_sync_ops` へ写す) は、 自分が書いた record の宛名 (行の世界番号) を **bridge する時の** 逆写像で決めていた。
//! 書いてから bridge するまでの間に、 その slot の住人か逆写像が変わると、 前の住人宛ての record が別の宛名で出た:
//!
//! - 満杯の表で、 消した他の peer の行の slot を自分の新しい行が使い回す: 削除が自分の世界番号宛てに出る (相手で消えず、
//!   新しい行も 「消えた世界番号」 宛てとして捨てられる)
//! - 同じ slot を、 後から受けた別の peer の行が使い回す: 削除がその行宛てに出る (相手で別の行が消える)
//! - 翻訳した行を自分の行に畳む: 畳む前の書き換えが自分の世界番号宛てに出る (相手で名前の無い行になる)
//!
//! どの試験も 「書く → 変わる → 配る」 を、 間に commit を置かずに続ける。 consumer (裏の書き出し係) は WAL に書かれると起きて、
//! 自分で group を閉じて bridge するので、 「変わる」 より先に bridge が読むことがある (その回は前提が崩れている)。 bridge が写した
//! 行の番号 (`current_sync_lsn`) が動いていない事で確かめ、 動いた回はやり直す (`until_unraced`)。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::{eid_local, Hlc};

const T: &str = "t";
/// t の行を指す ref を持つ表
const U: &str = "u";
const OPLOG: usize = 1 << 20;
/// 表の枠 (P1 だけ)
const CAP: u32 = 4;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue779-{}-{tag}", std::process::id()));
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

fn create(path: &str, id: u32, tr: &Arc<InMemoryTransport>, cap: Option<u32>) -> Peer {
    let mut b = Database::create_with_capacity(path, 4096).unwrap();
    let t = b.table(T).number("key").number("title").primary_key("key");
    let t = match cap {
        Some(n) => t.with_capacity(n),
        None => t,
    };
    t.build().unwrap();
    b.table(U).number("name").ref_to("of", T).primary_key("name").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>).with_cursor_path(format!("{path}.cursors").into());
    Peer { db, sync }
}

impl Peer {
    fn publish(&self) {
        let eng = self.db.engine();
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        while eng.transfer_oplog_to_sync_ops() > 0 {}
        self.sync.publish_since(Hlc::ZERO);
        std::thread::sleep(Duration::from_millis(3));
    }

    fn upsert(&self, key: i64, title: i64) -> u64 {
        self.db.get_table(T).unwrap().upsert().set("key", key).set("title", title).commit().unwrap()
    }

    fn eid_of(&self, key: i64) -> Option<u64> {
        self.db.engine().rebuild();
        self.db.get_table(T).unwrap().where_eq("key", key).find().unwrap().first().copied()
    }

    /// (key, title) の並び (全部の行、 名前の無い行も)。
    fn rows(&self) -> Vec<(Option<i64>, Option<i64>)> {
        self.db.engine().rebuild();
        let t = self.db.get_table(T).unwrap();
        let mut eids = t.all().find().unwrap();
        eids.extend(t.all().where_ge("title", 0i64).find().unwrap());
        eids.sort_unstable();
        eids.dedup();
        let n = |v: Option<Value>| match v {
            Some(Value::Number(n)) => Some(n),
            _ => None,
        };
        let mut out: Vec<_> = eids.into_iter().map(|e| (n(t.entity(e).get("key")), n(t.entity(e).get("title")))).collect();
        out.sort();
        out
    }
}

/// 覚えている 「slot の前の宛名」 の数。
fn remembered(p: &Peer) -> usize {
    p.db.engine().former_addresses_len()
}

/// 配った後 (bridge が新しい住人の record まで読んだ後) は、 次の bridge が覚えを外す。
fn assert_forgotten(p: &Peer) {
    p.db.engine().transfer_oplog_to_sync_ops();
    assert_eq!(remembered(p), 0, "bridge が読み終えた後も、 slot の前の宛名を覚えたまま");
}

/// bridge が写した行の番号 (bridge が 1 件でも写すと進む。 WAL を畳んでも戻らない)。
fn bridged(p: &Peer) -> u32 {
    p.db.engine().current_sync_lsn()
}

/// `body` を、 前提 (slot が変わる前に bridge が読まなかった) が崩れなくなるまで回す。 `body` は前提が崩れた回に None を返す。
fn until_unraced(tag: &str, body: impl Fn(&str) -> Option<()>) {
    for i in 0..20 {
        if body(&format!("{tag}{i}")).is_some() {
            return;
        }
    }
    panic!("前提: 20 回とも、 slot が変わる前に consumer の bridge が record を読んだ");
}

fn r(key: i64, title: i64) -> (Option<i64>, Option<i64>) {
    (Some(key), Some(title))
}

struct World {
    p1: Peer,
    p2: Peer,
    p3: Peer,
    p4: Peer,
    tr: Arc<InMemoryTransport>,
    dir: Dir,
}

/// P2 が行 1..=4 を作る。 P1 (枠 4) と P3 が受ける = P1 の表は P2 の行の写しで満杯。 P4 は行 7 を作り、 P3 だけが受けている。
fn world(tag: &str) -> World {
    let dir = Dir::new(tag);
    let tr = Arc::new(InMemoryTransport::new());
    for p in 1..=4 {
        tr.register_peer(p);
    }
    let p1 = create(&dir.db("p1"), 1, &tr, Some(CAP));
    let p2 = create(&dir.db("p2"), 2, &tr, None);
    let p3 = create(&dir.db("p3"), 3, &tr, None);
    let p4 = create(&dir.db("p4"), 4, &tr, None);
    for k in 1..=CAP as i64 {
        p2.upsert(k, k * 10);
    }
    p2.publish();
    p4.upsert(7, 70);
    p4.publish();
    p1.sync.pull_once(2);
    p3.sync.pull_once(2);
    p3.sync.pull_once(4);
    p1.publish();
    assert_eq!(p1.rows(), vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40)], "前提: P1 の表は P2 の行で満杯");
    assert_eq!(p3.rows(), vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40), r(7, 70)]);
    World { p1, p2, p3, p4, tr, dir }
}

/// #731: eid の場所 (使い回した行の eid は世代が違う。 既定の DB の場所は下 24 bit)。 translator の鍵も場所。
fn slot(e: u64) -> u32 {
    eid_local(e) & 0xFF_FFFF
}

/// P1 が P2 の行 `key` を消す (commit しない)。 消した slot と、 消す前の bridge の行の番号。
fn delete_unbridged(p1: &Peer, key: i64) -> (u32, u32) {
    let at = bridged(p1);
    let e = p1.eid_of(key).expect("消す行がある");
    assert_eq!(p1.db.engine().eid_translator().reverse(slot(e)).map(|x| x.0), Some(2), "前提: 消すのは P2 の行の写し");
    p1.db.get_table(T).unwrap().entity(e).delete().unwrap();
    (slot(e), at)
}

/// 消した他の peer の行の slot を、 自分の新しい行が使い回す。
#[test]
fn a_delete_of_a_foreign_row_keeps_its_address_when_an_own_row_reuses_the_slot() {
    until_unraced("own", |tag| {
        let w = world(tag);
        let (slot, at) = delete_unbridged(&w.p1, 1);
        let e9 = w.p1.upsert(9, 90);
        assert_eq!(crate::slot(e9), slot, "前提: 新しい行が消した行の slot を使い回した");
        // commit の前の bridge (読めるのは閉じた group だけ) は、 覚えを外さない
        w.p1.db.engine().transfer_oplog_to_sync_ops();
        if bridged(&w.p1) != at {
            return None;
        }
        assert_eq!(remembered(&w.p1), 1, "まだ bridge していない record があるのに、 slot の前の宛名を覚えていない");
        w.p1.publish();
        assert_forgotten(&w.p1);
        assert_eq!(w.p1.rows(), vec![r(2, 20), r(3, 30), r(4, 40), r(9, 90)]);
        for p in [&w.p2, &w.p3] {
            p.sync.pull_once(1);
            let rows: Vec<_> = p.rows().into_iter().filter(|x| x.0 != Some(7)).collect();
            assert_eq!(rows, w.p1.rows(), "P1 の削除と新しい行が相手で同じ答えにならない");
        }
        Some(())
    });
}

/// 消した他の peer の行の slot を、 後から受けた別の peer の行が使い回す。
#[test]
fn a_delete_of_a_foreign_row_keeps_its_address_when_another_foreign_row_reuses_the_slot() {
    until_unraced("foreign", |tag| {
        let w = world(tag);
        let (slot, at) = delete_unbridged(&w.p1, 1);
        if bridged(&w.p1) != at {
            return None;
        }
        w.p1.sync.pull_once(4);
        let e7 = w.p1.eid_of(7).expect("P1 が P4 の行を受けた");
        assert_eq!(crate::slot(e7), slot, "前提: P4 の行が消した行の slot を使い回した");
        w.p1.publish();
        assert_forgotten(&w.p1);
        assert_eq!(w.p1.rows(), vec![r(2, 20), r(3, 30), r(4, 40), r(7, 70)]);
        for p in [&w.p2, &w.p3, &w.p4] {
            p.sync.pull_once(1);
        }
        assert_eq!(w.p3.rows(), w.p1.rows(), "P1 の削除が P3 で別の行に当たった / 当たらない");
        assert_eq!(w.p4.rows(), vec![r(7, 70)], "P1 の削除が P4 の行を消した");
        assert_eq!(w.p2.rows(), vec![r(2, 20), r(3, 30), r(4, 40)]);
        Some(())
    });
}

/// 消す前の書き換えも、 消した行宛てのまま出る (使い回した行に写らない)。
#[test]
fn a_write_before_the_delete_keeps_its_address_when_the_slot_is_reused() {
    until_unraced("write", |tag| {
        let w = world(tag);
        let e = w.p1.eid_of(1).unwrap();
        let at = bridged(&w.p1);
        w.p1.db.get_table(T).unwrap().entity(e).set("title", 11i64).commit().unwrap();
        w.p1.db.get_table(T).unwrap().entity(e).delete().unwrap();
        // 新しい行は title を書かない (前の住人宛ての title が写れば残る)
        let e9 = w.p1.db.get_table(T).unwrap().upsert().set("key", 9i64).commit().unwrap();
        assert_eq!(slot(e9), slot(e), "前提: 新しい行が消した行の slot を使い回した");
        if bridged(&w.p1) != at {
            return None;
        }
        w.p1.publish();
        assert_eq!(w.p1.rows(), vec![(Some(2), Some(20)), (Some(3), Some(30)), (Some(4), Some(40)), (Some(9), None)]);
        w.p3.sync.pull_once(1);
        let rows: Vec<_> = w.p3.rows().into_iter().filter(|x| x.0 != Some(7)).collect();
        assert_eq!(rows, w.p1.rows(), "消した行宛ての書き換えが、 使い回した行に写った");
        Some(())
    });
}

/// 翻訳した行を自分の行に畳む前の書き換えも、 畳んだ行の世界番号宛てに出る (満杯の表でなくてよい)。
#[test]
fn a_write_before_a_fold_keeps_its_address() {
    until_unraced("fold", |tag| {
        let dir = Dir::new(tag);
        let tr = Arc::new(InMemoryTransport::new());
        for p in 1..=3 {
            tr.register_peer(p);
        }
        let p1 = create(&dir.db("p1"), 1, &tr, None);
        let p2 = create(&dir.db("p2"), 2, &tr, None);
        let p3 = create(&dir.db("p3"), 3, &tr, None);
        p2.upsert(1, 10);
        p2.publish();
        p1.sync.pull_once(2);
        p3.sync.pull_once(2);
        p1.upsert(5, 50);
        p1.publish();
        p3.sync.pull_once(1);
        // P2 は P1 の行 5 を知らないまま、 自分の行 1 を 5 に改名する
        let e = p2.eid_of(1).unwrap();
        p2.db.get_table(T).unwrap().entity(e).set("key", 5i64).commit().unwrap();
        p2.publish();
        // P1 が P2 の行の写しを書き換え、 bridge の前に P2 の改名を受ける (写しを自分の行に畳む)
        let s = p1.eid_of(1).unwrap();
        let d = p1.eid_of(5).unwrap();
        let at = bridged(&p1);
        p1.db.get_table(T).unwrap().entity(s).set("title", 11i64).commit().unwrap();
        if bridged(&p1) != at {
            return None;
        }
        p1.sync.pull_once(2);
        assert_eq!(p1.rows(), vec![r(5, 11)], "前提: P1 は P2 の行を自分の行に畳んだ");
        assert_eq!(p1.eid_of(5), Some(d), "前提: 残したのは自分の行");
        p1.publish();
        assert_forgotten(&p1);
        p3.sync.pull_once(2);
        p3.sync.pull_once(1);
        assert_eq!(p3.rows(), vec![r(5, 11)], "畳む前の書き換えが P3 で別の行になった");
        drop((p1, p2, p3));
        Some(())
    });
}

/// 消した自分の行の slot を、 後から受けた別の peer の行が使い回す: 削除は自分の世界番号宛てのまま (受けた行宛てにならない)。
#[test]
fn a_delete_of_an_own_row_keeps_its_address_when_a_foreign_row_reuses_the_slot() {
    until_unraced("ownrow", |tag| {
        let dir = Dir::new(tag);
        let tr = Arc::new(InMemoryTransport::new());
        for p in 1..=4 {
            tr.register_peer(p);
        }
        let p1 = create(&dir.db("p1"), 1, &tr, Some(CAP));
        let p3 = create(&dir.db("p3"), 3, &tr, None);
        let p4 = create(&dir.db("p4"), 4, &tr, None);
        for k in 1..=CAP as i64 {
            p1.upsert(k, k * 10);
        }
        p1.publish();
        p4.upsert(7, 70);
        p4.publish();
        p3.sync.pull_once(1);
        p3.sync.pull_once(4);
        assert_eq!(p3.rows(), vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40), r(7, 70)]);
        let at = bridged(&p1);
        let e = p1.eid_of(1).unwrap();
        assert_eq!(p1.db.engine().eid_translator().reverse(slot(e)), None, "前提: 消すのは自分の行");
        p1.db.get_table(T).unwrap().entity(e).delete().unwrap();
        if bridged(&p1) != at {
            return None;
        }
        p1.sync.pull_once(4);
        assert_eq!(p1.eid_of(7).map(slot), Some(slot(e)), "前提: P4 の行が消した行の slot を使い回した");
        p1.publish();
        // 続く自分の record が無くても (受けた行は自分の record でない)、 bridge が追いつけば覚えを外す
        assert_forgotten(&p1);
        assert_eq!(p1.rows(), vec![r(2, 20), r(3, 30), r(4, 40), r(7, 70)]);
        p3.sync.pull_once(1);
        p4.sync.pull_once(1);
        assert_eq!(p3.rows(), p1.rows(), "P1 の自分の行の削除が、 P3 で P4 の行に当たった / 当たらない");
        assert_eq!(p4.rows(), vec![r(2, 20), r(3, 30), r(4, 40), r(7, 70)], "P1 の自分の行の削除が P4 の行を消した");
        drop((p1, p3, p4));
        Some(())
    });
}

/// bridge の前に同じ slot を 2 回使い回す: P2 の行 → 自分の行 9 (作って消す) → P4 の行。 3 人の住人宛ての record がそれぞれの宛名で出る。
#[test]
fn records_keep_their_addresses_when_the_slot_changes_owner_twice() {
    until_unraced("twice", |tag| {
        let w = world(tag);
        let (slot, at) = delete_unbridged(&w.p1, 1);
        let e9 = w.p1.upsert(9, 90);
        assert_eq!(crate::slot(e9), slot, "前提: 自分の行 9 が消した行の slot を使い回した");
        w.p1.db.get_table(T).unwrap().entity(e9).delete().unwrap();
        if bridged(&w.p1) != at {
            return None;
        }
        w.p1.sync.pull_once(4);
        assert_eq!(w.p1.eid_of(7).map(crate::slot), Some(slot), "前提: P4 の行が同じ slot をもう一度使い回した");
        // P4 の行の写しも書き換える (3 人目の住人宛て)
        let e7 = w.p1.eid_of(7).unwrap();
        w.p1.db.get_table(T).unwrap().entity(e7).set("title", 71i64).commit().unwrap();
        w.p1.publish();
        assert_forgotten(&w.p1);
        assert_eq!(w.p1.rows(), vec![r(2, 20), r(3, 30), r(4, 40), r(7, 71)]);
        for p in [&w.p2, &w.p3, &w.p4] {
            p.sync.pull_once(1);
        }
        assert_eq!(w.p3.rows(), w.p1.rows(), "3 人の住人宛ての record が P3 で同じ答えにならない");
        assert_eq!(w.p4.rows(), vec![r(7, 71)], "自分の行 9 の削除か P2 の行の削除が、 P4 の行に当たった");
        // P2 は P4 の行を知らなかった (P1 の書き換えだけ先に届く)。 P4 から受ければそろう
        w.p2.sync.pull_once(4);
        assert_eq!(w.p2.rows(), w.p1.rows());
        Some(())
    });
}

/// ref の指す先も、 書いた時の住人の世界番号で出る: P2 の行の写しを指す ref を書いた後、 その行を消して slot を自分の行が使い回す。
#[test]
fn a_ref_keeps_pointing_at_the_row_it_was_written_for_when_the_target_slot_is_reused() {
    until_unraced("ref", |tag| {
        let w = world(tag);
        let at_p3 = w.p3.eid_of(1).expect("P3 に P2 の行 1 がある");
        let target = w.p1.eid_of(1).unwrap();
        let at = bridged(&w.p1);
        w.p1.db.get_table(U).unwrap().insert().set("name", 100i64).set("of", Value::Ref(target)).commit().unwrap();
        w.p1.db.get_table(T).unwrap().entity(target).delete().unwrap();
        let e9 = w.p1.upsert(9, 90);
        assert_eq!(slot(e9), slot(target), "前提: 新しい行が、 ref の指していた行の slot を使い回した");
        if bridged(&w.p1) != at {
            return None;
        }
        w.p1.publish();
        w.p3.sync.pull_once(1);
        w.p3.db.engine().rebuild();
        let u = w.p3.db.get_table(U).unwrap();
        let found = u.where_eq("name", 100i64).find().unwrap();
        assert_eq!(found.len(), 1, "前提: ref を持つ行が P3 に届いた");
        let nine = w.p3.eid_of(9).expect("P1 の新しい行が P3 に届いた");
        assert_ne!(nine, at_p3);
        let of = u.entity(found[0]).get("of");
        assert_eq!(of, Some(Value::Ref(at_p3)), "ref が、 書いた時に指していた行 (P2 の行 1) を指していない (新しい行 9 は {nine:x})");
        Some(())
    });
}

/// bridge の前にきれいに閉じても、 閉じる時の最後の bridge が前の住人の宛名で写す (覚えはメモリの上だけで、 開き直すと無い)。
#[test]
fn the_address_survives_a_clean_close_before_the_bridge() {
    until_unraced("close", |tag| {
        let World { p1, p2, p3, p4, tr, dir } = world(tag);
        let (slot, at) = delete_unbridged(&p1, 1);
        let e9 = p1.upsert(9, 90);
        assert_eq!(crate::slot(e9), slot, "前提: 新しい行が消した行の slot を使い回した");
        if bridged(&p1) != at {
            return None;
        }
        // commit も publish もせずに閉じて、 開き直す
        drop(p1);
        let path = dir.db("p1");
        let db = Database::open_with_oplog(&path, OPLOG).unwrap();
        assert_eq!(db.engine().peer_id(), 1);
        let sync = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>).with_cursor_path(format!("{path}.cursors").into());
        let p1 = Peer { db, sync };
        assert_eq!(remembered(&p1), 0, "前提: 開き直すと覚えは無い");
        assert_eq!(p1.rows(), vec![r(2, 20), r(3, 30), r(4, 40), r(9, 90)]);
        p1.publish();
        for p in [&p2, &p3] {
            p.sync.pull_once(1);
            let rows: Vec<_> = p.rows().into_iter().filter(|x| x.0 != Some(7)).collect();
            assert_eq!(rows, p1.rows(), "閉じる前の削除と新しい行が、 開き直した後に相手で同じ答えにならない");
        }
        drop((p1, p2, p3, p4));
        Some(())
    });
}
