//! #178: 「2 つの世界番号は同じ entity」 の知らせ (`Op::Alias`) で、 同じ主キーの行が 2 つになったのを 1 つに戻す。
//!
//! 形 (実地の #178 と同じく、 束ねる前に出て行った書き込みが相手で別の行になる):
//!
//! 1. A と B が同じ主キー (key = 7) の行を別々に作る。 B は主キー以外の列 (module / path) を先に commit し、 主キーは後の commit
//! 2. A が B の record を 2 回の配りで受ける: 先の配りの主キーの無い record で B の行の写しを払い出し、 後の配りの主キーは
//!    その写しに当たる (写像が既にあるので主キーでは束ねない)。 前はここで A に key 7 の行が 2 つ残った。 今は主キーを当てた
//!    時に同じ主キーの A の行と 1 つに畳む (`Engine::fold_rows_with_same_pk`、 知らせによらない)。 同じ配りなら、 主キーが
//!    後ろでも最初の record の前に束ねる (#544)
//! 3. B が A の record を受ける: A の行を主キーで B の行に束ねる (`bind_over_local_writes`)。 知らせを書く設定なら
//!    「B の行の B の世界番号 = A の行の世界番号」 を配る
//! 4. A と第三者 C が受ける。 C も B の record を A と同じ 2 回の配りで受ける (主キーの無い record が先)
//!
//! 主キーの Tie が届けば知らせが無くても畳まれる。 知らせが要るのは、 主キーの無い record しか届かない世界番号 (D の形、
//! `alias_maps_the_unseen_identity_onto_the_existing_row`)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-alias178-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    let _ = std::fs::remove_dir_all(path);
    let mut eng = Engine::create_with_cell_version(path, 65_536).unwrap();
    eng.define_table("files", 1000).unwrap();
    for c in ["key", "module", "path"] {
        eng.define_himo_in("files", c, ValueType::Number, 0).unwrap();
    }
    let key = eng.himo_id("files.key").unwrap() as u16;
    eng.set_table_pk("files", key).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// commit して bridge を待ち、 この peer の配る record を全部取る (前に配った分も。 当て直しは LWW で冪等)。
fn records_of(eng: &Arc<Engine>, peer: PeerId) -> Vec<WireRecord> {
    eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(350));
    eng.transfer_oplog_to_sync_ops();
    let t: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    Syncer::new(eng.clone(), t.clone()).publish_since(Hlc::ZERO);
    t.pull(peer, Hlc::ZERO)
}

fn apply(eng: &Arc<Engine>, recs: &[WireRecord]) {
    let t: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    Syncer::new(eng.clone(), t).apply_records(recs);
}

/// key 7 の行の (module, path) の並び。
fn rows_with_key(eng: &Engine) -> Vec<(Option<u64>, Option<u64>)> {
    let mut v: Vec<_> = eng
        .pull_raw("files.key", 7)
        .into_iter()
        .map(|e| (eng.get(e, "files.module"), eng.get(e, "files.path")))
        .collect();
    v.sort();
    v
}

struct Peers {
    a: Arc<Engine>,
    b: Arc<Engine>,
    c: Arc<Engine>,
    paths: Vec<String>,
    /// B の 2 回目の record (A の行を束ねた後。 知らせを書く設定なら知らせを含む)
    recs_b2: Vec<WireRecord>,
    /// A の record
    recs_a: Vec<WireRecord>,
    /// A の行の世界番号
    ea: u64,
}

fn cleanup(paths: &[String]) {
    for p in paths {
        let _ = std::fs::remove_dir_all(p);
    }
}

fn run(tag: &str, emit: bool) -> Peers {
    let paths: Vec<String> = ["a", "b", "c"].iter().map(|p| tmp_path(&format!("{tag}-{p}"))).collect();
    let (a, b, c) = (make_engine(&paths[0], 1), make_engine(&paths[1], 2), make_engine(&paths[2], 3));
    b.set_emit_alias(emit);

    let ea = a.entity_in("files").unwrap();
    a.tie_to(ea, "files.key", 7);
    a.tie_to(ea, "files.module", 1);
    let recs_a = records_of(&a, 1);
    std::thread::sleep(Duration::from_millis(5));

    let eb = b.entity_in("files").unwrap();
    b.tie_to(eb, "files.module", 2);
    b.tie_to(eb, "files.path", 3);
    let recs_b0 = records_of(&b, 2);
    b.tie_to(eb, "files.key", 7);
    let recs_b1 = records_of(&b, 2);

    // 2. 主キーの無い record が先の配りで届く: A に B の行の写しが主キー無しでできる (#178 の形)
    apply(&a, &recs_b0);
    assert_eq!((rows_with_key(&a).len(), a.pull_raw("files.path", 3).len()), (1, 1), "前提: A に B の行の写しが主キー無しでできる");
    // 主キーが後の配りで届き、 写しの主キーが 7 になる (A の行と畳む)
    apply(&a, &recs_b1);
    // 3. B が A の行を主キーで自分の行に束ねる
    apply(&b, &recs_a);
    assert_eq!(b.bind_over_local_writes(), 1, "前提: B が自分の書いた行に A の世界番号を束ねる");
    let recs_b2 = records_of(&b, 2);
    // 4. A と C が受ける
    apply(&a, &recs_b2);
    apply(&c, &recs_a);
    apply(&c, &recs_b0);
    apply(&c, &recs_b2);
    Peers { a, b, c, paths, recs_b2, recs_a, ea }
}

#[test]
fn alias_folds_the_duplicate_rows_on_every_peer() {
    let p = run("on", true);
    // module は B の 2 (A の 1 より後に書いた)、 path は B の 3
    let want = vec![(Some(2), Some(3))];
    assert_eq!(rows_with_key(&p.a), want, "A: 2 行が畳まれない");
    assert_eq!(rows_with_key(&p.b), want, "B");
    assert_eq!(rows_with_key(&p.c), want, "C: 2 行が畳まれない");
    assert!(
        p.recs_b2.iter().any(|r| matches!(r.op, enchudb_oplog::oplog::DecodedOp::Alias { .. })),
        "前提: B が知らせを配った"
    );

    // 同じ record をもう一度当てても (再配送) 2 行に戻らない。 開き直した後も (別名が残る)
    apply(&p.a, &p.recs_b2);
    assert_eq!(rows_with_key(&p.a), want, "再配送で 2 行に戻った");
    let path_a = p.paths[0].clone();
    let Peers { a, b, c, paths, recs_b2, .. } = p;
    drop((a, b, c));
    let a = Engine::open_concurrent_with_oplog(&path_a, 16 * 1024 * 1024).unwrap();
    a.set_peer_id(1);
    apply(&a, &recs_b2);
    assert_eq!(rows_with_key(&a), want, "開き直した後の再配送で 2 行に戻った");
    drop(a);
    cleanup(&paths);
}

/// 知らせを書かない設定 (既定) でも、 主キーの Tie が届いた時に 1 行に畳む (前は 2 行のままだった)。 B は今まで通り数えるだけ。
#[test]
fn without_alias_the_key_folds_the_duplicate_rows() {
    let p = run("off", false);
    let want = vec![(Some(2), Some(3))];
    assert_eq!(rows_with_key(&p.a), want, "知らせ無しで A の 2 行が畳まれない");
    assert_eq!(rows_with_key(&p.c), want, "知らせ無しで C の 2 行が畳まれない");
    assert_eq!(p.a.pull_raw("files.path", 3).len(), 1, "A に B の行の写しが主キー無しで残った");
    assert!(
        !p.recs_b2.iter().any(|r| matches!(r.op, enchudb_oplog::oplog::DecodedOp::Alias { .. })),
        "既定で知らせを書いた"
    );
    let Peers { a, b, c, paths, .. } = p;
    drop((a, b, c));
    cleanup(&paths);
}

/// 知らせを受けた時に片方の世界番号しか行が無ければ、 もう片方をその行の別名にする: D は B の record と知らせだけを先に受け
/// (B の行の写しが 1 行)、 後から A の主キーの無い record (module の書き込み) を受ける。 その record は別名でその行に当たり、
/// 新しい行を作らない (主キーで束ねる道は通らない = 主キーの無い record なので)。
#[test]
fn alias_maps_the_unseen_identity_onto_the_existing_row() {
    let p = run("unseen", true);
    let d_path = tmp_path("unseen-d");
    let d = make_engine(&d_path, 4);
    apply(&d, &p.recs_b2);
    assert_eq!(rows_with_key(&d), vec![(Some(2), Some(3))], "前提: D に B の行の写しが 1 行");
    let row = d.pull_raw("files.key", 7)[0];
    let module_only: Vec<WireRecord> = p
        .recs_a
        .iter()
        .filter(|r| !matches!(r.op, enchudb_oplog::oplog::DecodedOp::Tie { himo_id, .. } if Some(himo_id as usize) == d.himo_id("files.key")))
        .cloned()
        .collect();
    assert!(module_only.len() < p.recs_a.len(), "前提: 主キーの record を除いた");
    apply(&d, &module_only);
    assert_eq!(d.resolve_remote_eid_existing(p.ea), Some(row), "A の世界番号が B の行の写しに当たらない");
    assert!(d.pull_raw("files.module", 1).is_empty(), "A の主キーの無い record が新しい行を作った");
    drop(d);
    let _ = std::fs::remove_dir_all(&d_path);
    let Peers { a, b, c, paths, .. } = p;
    drop((a, b, c));
    cleanup(&paths);
}
