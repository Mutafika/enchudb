//! #720: `pkformer` (前の名前・束ねた identity・identity ごとのカード・名前の年表) の追記 (`pkformer.log`)。
//!
//! 前は、 変わるたびに `pkformer` をまるごと書き直していた。 束ねた行は identity ごとのカード (Leaf は中身ごと) を持つので、
//! 2 台が同じ名前の行を 2,000 個ずつ作って束ねると 1.36 MB になり、 1 行の書き換えの pull のたびに全部を書き直した。 全行を
//! 消しても縮まなかった。
//!
//! - 書き出しは、 変わった slot の今の中身だけを `pkformer.log` に追記する (`pkformer` は書き直さない)
//! - きれいに閉じずに落ちても、 開き直すと `pkformer` + log から戻る。 追記の途中で切れた終わりは捨てて、 次にまとめ直す
//! - きれいに閉じると `pkformer` をまとめ直し、 log は header だけ (log を読まない 0.37.4 以前は `pkformer` だけで全部ある)
//! - log の無い `pkformer` (0.37.4 以前が書いた物) はそのまま読め、 最初の書き出しで log を作る
//! - 消えた行のカードは落とし、 `pkformer` は縮む
//!
//! 束ねた行は、 `Syncer` の束ね (`bind_by_primary_key` → `apply_one`) と同じ engine の呼び出しで作る。 落ちた像は、 開いたままの
//! DB の directory を写して作る (写しは閉じる処理を通らない)。 disk の中身は `Engine::pkformer_for_bootstrap` (`pkformer` に log を
//! 当てた形 = relay の bootstrap が配る物) で比べる。

use enchudb_engine::ValueType;
use enchudb_engine::db_files::{PKFORMER, PKFORMER_LOG, path_for};
use enchudb_engine::engine::Engine;
use enchudb_oplog::{EntityId, Hlc, make_eid};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const PEER: u32 = 2;
const AUTHOR: u32 = 1;
const LOG_HEADER: u64 = 24;
const OPLOG: usize = 8 << 20;
const BODY: usize = 200;

static RUN: AtomicU64 = AtomicU64::new(0);

/// 1 本の試験の directory。 Drop で消す。
struct Dir(PathBuf);

impl Dir {
    fn new(tag: &str) -> Self {
        let n = RUN.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("enchudb-issue720-{tag}-{}-{n}", std::process::id()));
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

struct Cols {
    key: u16,
    title: u16,
    body: u16,
}

fn cols(eng: &Engine) -> Cols {
    let h = |n: &str| eng.himo_id(n).unwrap() as u16;
    Cols { key: h("t.key"), title: h("t.title"), body: h("t.body") }
}

/// 受け手 (peer 2)。 表 `t` に名前 `key`・`title`・Leaf の `body`。
fn create(path: &str) -> (Arc<Engine>, Cols) {
    let mut eng = Engine::create_with_cell_version(path, 16_384).unwrap();
    eng.define_table("t", 8_192).unwrap();
    eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "title", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "body", ValueType::Leaf, 0).unwrap();
    let key = eng.himo_id("t.key").unwrap() as u16;
    eng.set_table_pk("t", key).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG).unwrap();
    eng.set_peer_id(PEER);
    let c = cols(&eng);
    (eng, c)
}

fn open(path: &str) -> (Arc<Engine>, Cols) {
    let eng = Engine::open_concurrent_with_oplog(path, OPLOG).unwrap();
    let c = cols(&eng);
    (eng, c)
}

/// 相手の書き込みの時刻: 呼ぶたびに進み、 それまでの自分の書き込み (受けた時刻を見て進む時計) より新しい。
fn later() -> Hlc {
    static WALL: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let _ = WALL.compare_exchange(0, now + 600_000, Ordering::Relaxed, Ordering::Relaxed);
    Hlc { wall: WALL.fetch_add(10, Ordering::Relaxed), logical: 0, peer: AUTHOR }
}

/// 名前 `i` の行を自分で作り、 相手 (peer 1) が別に作った同じ名前の行を束ねる (`Syncer` の束ねと同じ呼び出し)。 自分の行の eid。
fn bound_row(eng: &Engine, c: &Cols, i: u32) -> EntityId {
    let own = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(own, c.key, i);
    eng.tie_to_by_id(own, c.title, 1_000 + i);
    eng.tie_text_to_by_id(own, c.body, &"x".repeat(BODY));
    let foreign = make_eid(AUTHOR, i);
    let t = later();
    eng.note_bound_identity(own, foreign, t);
    let local = eng.bind_remote_eid_by_pk(AUTHOR, foreign, own, c.key, i as u64);
    assert_eq!(local, own, "前提: 相手の行は自分の行に束ねられた");
    eng.remote_tie_apply(local, c.key, i, t);
    eng.note_bound_write(local, foreign, c.key, Some(i as u64), t);
    eng.remote_tie_apply(local, c.title, 2_000 + i, t);
    eng.note_bound_write(local, foreign, c.title, Some(2_000 + i as u64), t);
    let body = "y".repeat(BODY);
    eng.remote_tieleaf_apply(local, c.body, body.as_bytes(), t);
    eng.note_bound_leaf(local, foreign, c.body, body.as_bytes(), t);
    assert_eq!(eng.get_by_id(own, c.key), Some(i as u64), "前提: 名前");
    assert_eq!(eng.get_by_id(own, c.title), Some(2_000 + i as u64), "前提: 相手の title が当たった");
    assert_eq!(eng.get_text_owned(own, "t.body").as_deref(), Some(body.as_bytes()), "前提: 相手の body が当たった");
    own
}

/// 相手が名前 `i` の行の title を書き換えた 1 件を受ける。
fn retitle(eng: &Engine, c: &Cols, i: u32, round: u64) {
    let foreign = make_eid(AUTHOR, i);
    let local = eng.resolve_remote_eid_existing(foreign).expect("束ねた行の写像");
    let t = later();
    assert!(eng.remote_tie_apply(local, c.title, 50_000 + round as u32, t), "前提: 書き換えが当たった");
    eng.note_bound_write(local, foreign, c.title, Some(50_000 + round), t);
}

fn len(path: &str, name: &str) -> u64 {
    std::fs::metadata(path_for(path, name)).map(|m| m.len()).unwrap_or(0)
}

fn bytes(path: &str, name: &str) -> Vec<u8> {
    std::fs::read(path_for(path, name)).unwrap_or_default()
}

/// disk の中身 (`pkformer` に log を当てた形)。
fn on_disk(path: &str) -> Vec<u8> {
    Engine::pkformer_for_bootstrap(path).unwrap().unwrap_or_default()
}

/// 開いたままの DB の像を撮る (= 閉じる処理を通らない、 落ちた後の像)。 先に自分の書き込みの配り (`_sync_ops` への写し) を
/// 済ませる (consumer が写している最中の列を写すと、 像の segment の長さが合わない)。 `pkformer` は書き出さない。
fn image_of(eng: &Engine, from: &str, to: &str) {
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    eng.oplog_sync().unwrap();
    copy_dir(from, to);
}

fn copy_dir(from: &str, to: &str) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dst = std::path::Path::new(to).join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(e.path().to_str().unwrap(), dst.to_str().unwrap());
        } else if e.file_name() != "lock" && !e.file_name().to_string_lossy().ends_with(".tmp") {
            // consumer が sidecar を置き換えている最中の一時 file は写さない (rename で消える)
            match std::fs::copy(e.path(), &dst) {
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                r => drop(r.unwrap()),
            }
        }
    }
}

/// `n` 行を束ねた DB。 きれいに閉じて開き直した所から始める (`pkformer` が全部を持ち、 log は header だけ。 作っている間の
/// 自分の書き込みを consumer が書き出し終えているので、 以後の書き出しは試験が呼ぶ物だけ)。
fn bound_db(dir: &Dir, n: u32) -> (String, Arc<Engine>, Cols, Vec<EntityId>) {
    let path = dir.db("db");
    let (eng, c) = create(&path);
    let rows: Vec<_> = (0..n).map(|i| bound_row(&eng, &c, i)).collect();
    eng.persist_sync_state().unwrap();
    assert_eq!(eng.pkformer_disk_diff().unwrap(), None);
    drop(eng);
    assert_eq!(len(&path, PKFORMER_LOG), LOG_HEADER, "前提: きれいに閉じた後の log は header だけ");
    let (eng, c) = open(&path);
    assert_eq!(eng.pkformer_disk_diff().unwrap(), None);
    (path, eng, c, rows)
}

/// 1 行の書き換えの書き出しは、 `pkformer` を書き直さず、 その行の record だけを log に足す (束ねた行の数によらない)。
#[test]
fn one_rewritten_row_appends_one_record_and_leaves_pkformer_alone() {
    let dir = Dir::new("append");
    let (path, eng, c, _) = bound_db(&dir, 300);
    let snapshot = bytes(&path, PKFORMER);
    assert!(snapshot.len() > 150_000, "前提: pkformer は束ねた行のカードを Leaf ごと持つ ({} B)", snapshot.len());
    for round in 0..5 {
        let before = len(&path, PKFORMER_LOG);
        retitle(&eng, &c, 7, round);
        eng.persist_sync_state().unwrap();
        let grew = len(&path, PKFORMER_LOG) - before;
        assert!(grew > 0, "{round} 回目: 変わったカードを書き出していない");
        assert!(grew < 2_048, "{round} 回目: 1 行の書き換えで log に {grew} B 足した");
        assert!(bytes(&path, PKFORMER) == snapshot, "{round} 回目: pkformer を書き直した");
        assert_eq!(eng.pkformer_disk_diff().unwrap(), None, "{round} 回目");
    }
    // 何も変わらない書き出しは何も足さない
    let before = len(&path, PKFORMER_LOG);
    eng.persist_sync_state().unwrap();
    assert_eq!(len(&path, PKFORMER_LOG), before);
}

/// 追記は、 きれいに閉じずに落ちても開き直すと戻る。 開き直した後も追記を続ける。
#[test]
fn appended_records_survive_an_unclean_close() {
    let dir = Dir::new("unclean");
    let (path, eng, c, _) = bound_db(&dir, 20);
    let snapshot = bytes(&path, PKFORMER);
    retitle(&eng, &c, 3, 0);
    bound_row(&eng, &c, 100);
    eng.persist_sync_state().unwrap();
    let expect = on_disk(&path);
    assert!(expect != snapshot && bytes(&path, PKFORMER) == snapshot, "前提: 2 回目の書き出しの分は log にだけある");
    assert_eq!(eng.pkformer_disk_diff().unwrap(), None);
    let image = dir.db("image");
    image_of(&eng, &path, &image);
    drop(eng);

    let (back, c) = open(&image);
    // 開いた時の中身 (memory) を書き出させて比べる: 何も変えずにまとめ直した `pkformer` が、 落ちる前の中身と同じ
    assert_eq!(back.pkformer_disk_diff().unwrap(), None, "開き直した中身が disk と違う");
    assert!(on_disk(&image) == expect, "log にだけあった記録が戻らない");
    let before = len(&image, PKFORMER_LOG);
    retitle(&back, &c, 3, 1);
    back.persist_sync_state().unwrap();
    assert!(len(&image, PKFORMER_LOG) > before, "開き直した後の書き出しが追記でない");
    assert!(bytes(&image, PKFORMER) == snapshot, "開き直した後の書き出しが pkformer を書き直した");
    assert_eq!(back.pkformer_disk_diff().unwrap(), None);
}

/// きれいに閉じると `pkformer` をまとめ直し、 log は header だけになる (log を読まない 0.37.4 以前も `pkformer` だけで全部ある)。
#[test]
fn a_clean_close_compacts_the_log_into_pkformer() {
    let dir = Dir::new("clean");
    let (path, eng, c, _) = bound_db(&dir, 20);
    for round in 0..4 {
        retitle(&eng, &c, round as u32, round);
        eng.persist_sync_state().unwrap();
    }
    assert!(len(&path, PKFORMER_LOG) > LOG_HEADER, "前提: 閉じる前は log に追記がある");
    let expect = on_disk(&path);
    assert!(bytes(&path, PKFORMER) != expect, "前提: 閉じる前の pkformer だけでは足りない");
    drop(eng);
    assert_eq!(len(&path, PKFORMER_LOG), LOG_HEADER, "閉じた後の log に record が残った");
    assert!(bytes(&path, PKFORMER) == expect, "閉じた後の pkformer が全部を持たない");
    let (back, _) = open(&path);
    assert_eq!(back.pkformer_disk_diff().unwrap(), None);
}

/// log の終わりが切れていたら (追記の途中で落ちた)、 切れた所から後を捨てて開き、 次の書き出しでまとめ直す。
#[test]
fn a_torn_log_tail_is_dropped_and_rewritten() {
    let dir = Dir::new("torn");
    let (path, eng, c, _) = bound_db(&dir, 20);
    retitle(&eng, &c, 1, 0);
    eng.persist_sync_state().unwrap();
    let kept = on_disk(&path);
    retitle(&eng, &c, 2, 1);
    eng.persist_sync_state().unwrap();
    assert!(on_disk(&path) != kept, "前提: 最後の record は中身を変える");
    let image = dir.db("image");
    image_of(&eng, &path, &image);
    drop(eng);
    let log = path_for(&image, PKFORMER_LOG);
    let n = std::fs::metadata(&log).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&log).unwrap().set_len(n - 5).unwrap();

    assert!(on_disk(&image) == kept, "切れていない record まで読む");
    let (back, c) = open(&image);
    retitle(&back, &c, 5, 2);
    back.persist_sync_state().unwrap();
    assert_eq!(len(&image, PKFORMER_LOG), LOG_HEADER, "切れた跡の後ろに追記した");
    assert_eq!(back.pkformer_disk_diff().unwrap(), None);
    assert!(bytes(&image, PKFORMER) == on_disk(&image));
}

/// log の無い `pkformer` (0.37.4 以前が書いた物) はそのまま読め、 最初の書き出しでまとめ直して log を作る。 `pkformer` を
/// 書き直された後に残った古い log (log を知らない binary が開いて書いた) は読まない。
#[test]
fn a_pkformer_without_a_log_or_with_a_stale_log_is_read_as_it_is() {
    let dir = Dir::new("nolog");
    let (path, eng, c, _) = bound_db(&dir, 20);
    retitle(&eng, &c, 1, 0);
    eng.persist_sync_state().unwrap();
    let stale_log = bytes(&path, PKFORMER_LOG);
    assert!(stale_log.len() as u64 > LOG_HEADER, "前提: 古い log には record がある");
    retitle(&eng, &c, 2, 1);
    drop(eng);
    let closed = bytes(&path, PKFORMER);

    // log が無い
    let old = dir.db("old");
    copy_dir(&path, &old);
    std::fs::remove_file(path_for(&old, PKFORMER_LOG)).unwrap();
    assert!(on_disk(&old) == closed);
    let (back, c) = open(&old);
    assert_eq!(back.pkformer_disk_diff().unwrap(), None);
    retitle(&back, &c, 3, 2);
    back.persist_sync_state().unwrap();
    assert_eq!(len(&old, PKFORMER_LOG), LOG_HEADER, "log の無い所はまとめて書き、 header だけの log を作る");
    assert_eq!(back.pkformer_disk_diff().unwrap(), None);
    drop(back);

    // `pkformer` が書き直された後に、 前の `pkformer` の続きの log が残っている
    let stale = dir.db("stale");
    copy_dir(&path, &stale);
    std::fs::write(path_for(&stale, PKFORMER_LOG), &stale_log).unwrap();
    assert!(on_disk(&stale) == closed, "別の pkformer の続きの log を当てた");
    let (back, _) = open(&stale);
    assert_eq!(back.pkformer_disk_diff().unwrap(), None);
    drop(back);
    assert!(bytes(&stale, PKFORMER) == closed, "何も変えずに閉じたのに pkformer を書き直した");
}

/// 束ねた行を消すと、 その行のカードを落とし、 `pkformer` は縮む (前は全行を消しても同じ大きさのまま)。
#[test]
fn deleting_the_bound_rows_shrinks_pkformer() {
    let dir = Dir::new("shrink");
    let (path, eng, _c, rows) = bound_db(&dir, 300);
    let full = len(&path, PKFORMER) + len(&path, PKFORMER_LOG);
    assert!(full > 150_000, "前提: {full} B");
    for &row in &rows {
        assert!(eng.remote_delete_apply(row, later()), "前提: 削除が当たった");
    }
    eng.persist_sync_state().unwrap();
    let after = len(&path, PKFORMER) + len(&path, PKFORMER_LOG);
    assert!(after < full / 4, "全行を消した後も pkformer + log が {after} B (前は {full} B)");
    assert_eq!(eng.pkformer_disk_diff().unwrap(), None);
    // 消えた行宛ての書き込みは、 カードを積み直さない
    let foreign = make_eid(AUTHOR, 7);
    let local = eng.resolve_remote_eid_existing(foreign).expect("消えた行の写像は残る");
    let body = "z".repeat(BODY);
    eng.note_bound_leaf(local, foreign, cols(&eng).body, body.as_bytes(), later());
    eng.persist_sync_state().unwrap();
    assert_eq!(len(&path, PKFORMER) + len(&path, PKFORMER_LOG), after, "消えた行にカードを積んだ");
}
