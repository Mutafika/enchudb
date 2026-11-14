//! #847: 開いたままの DB の書き出しが、 変わっていない事を書くために sidecar (`tables` / `segments`) を作り直さない。
//!
//! 満杯の WAL を空ける consumer の書き出し 1 回は、 file system が混むと秒単位で止まり、 待つ書き手が待ちの上限 (5 秒) に
//! 当たって書き込みが落ちた。 止まっていた時間の 5 割前後が、 書き出しのたびの sidecar の作り直し (tmp + fsync + rename) だった:
//!
//! - `tables`: sync の ring (`_sync_ops`) は bridge が record を写すたびに 1 行払い出すので、 `next_local` が書き出しのたびに
//!   変わっていた。 満杯の ring への払い出しは失敗のたびに `next_local` を 1 進めるので、 止まった bridge の周ごとにも変わった
//! - `segments` (segment の長さの manifest): 伸びている最中の DB は書き出しのたびにどれかの segment が伸びている
//!
//! 直し: ring の `next_local` はいつも枠を置く (開き直した ring は、 生きていない slot を拾って払い出す。 1 周した ring と同じ)。
//! 枠を越えた `next_local` は枠に丸めて置く。 manifest は、 続けて書き出す間は 1 秒空ける (`flush` / 閉じる時は必ず書く)。
//! 最初の bridge の仕度 (payload の ring、 その場所の列) は、 書き手として並行の形にした時に済ませる。
//!
//! 「作り直さない」 は sidecar の inode で見る (作り直しは rename = 新しい inode)。
#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::{Duration, Instant};

const ME: u32 = 1;
const PULLER: u32 = 77;
const WAL: usize = 16 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("enchudb-issue847-{tag}-{}-{nanos}", std::process::id())).to_str().unwrap().to_string()
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn create(path: &str) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(ME);
    eng
}

/// DB の directory を写す (書き手が開いたままの像 = 閉じずに落ちた後に残る形)。
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

fn drain(eng: &Engine) {
    while eng.transfer_oplog_to_sync_ops() > 0 {}
}

fn inode(path: &str, sidecar: &str) -> u64 {
    std::fs::metadata(std::path::Path::new(path).join(sidecar)).unwrap().ino()
}

fn ring(eng: &Engine) -> enchudb_engine::engine::TableEidUsage {
    eng.table_eid_usage("_sync_ops").unwrap()
}

/// ring を満杯にして止める (書いている間は bridge も畳みも止める)。
fn fill(eng: &Engine, e: u64, extra: u32) {
    {
        let _no_bridge = eng.transfer_lock_for_fold();
        for i in 0..ring(eng).free + extra {
            eng.tie_to(e, "notes.note", i);
        }
        eng.oplog_commit();
    }
    drain(eng);
    assert_eq!(ring(eng).free, 0, "前提: ring が満杯");
}

/// bridge が ring に行を足しても `tables` を作り直さない。
///
/// falsify: ring の `next_local` をそのまま置くと、 行を足した後の persist のたびに作り直す (この試験で 100 回)。
#[test]
fn bridging_does_not_rewrite_tables_every_time() {
    const ROUNDS: u32 = 100;
    let path = tmp_path("bridge");
    let _cleanup = Cleanup(path.clone());
    let eng = create(&path);
    let e = eng.entity_in("notes").unwrap();
    eng.persist_tables().unwrap();
    let capacity = ring(&eng).capacity;
    let (mut last, mut rewrites) = (inode(&path, "tables"), 0u32);
    let lsn0 = eng.current_sync_lsn();
    for i in 0..ROUNDS {
        eng.tie_to(e, "notes.note", i);
        eng.oplog_commit();
        drain(&eng);
        eng.persist_tables().unwrap();
        let now = inode(&path, "tables");
        if now != last {
            rewrites += 1;
            last = now;
        }
    }
    let rows = eng.current_sync_lsn() - lsn0;
    assert!(rows >= ROUNDS, "前提: 周ごとに ring に行が足された ({rows} 行)");
    assert!(rows < capacity, "前提: ring は満杯になっていない");
    assert_eq!(rewrites, 0, "ring に {rows} 行足す間に `tables` を {rewrites} 回作り直した");
}

/// sync の DB の最初の bridge の仕度 (payload の ring、 その場所を置く列) は、 書き手として並行の形にした時に済んでいる。
/// 最初の bridge は file を作らず、 `tables` も作り直さない (前は、 満杯の WAL を空ける 1 回目の中で、 空きを待つ書き手を
/// 止めたまま ring を作って列を足していた)。
///
/// falsify: 並行の形にする時に仕度をしないと、 ring が無く、 最初の bridge が列を足して `tables` を作り直す。
#[test]
fn the_first_bridge_creates_nothing() {
    let path = tmp_path("first");
    let _cleanup = Cleanup(path.clone());
    let eng = create(&path);
    let ring_file = std::path::Path::new(&path).join("sync.payload.seg");
    assert!(ring_file.exists(), "並行の形にした後、 payload の ring がまだ無い");
    assert!(eng.himo_id("_sync_ops.payload_at").is_some(), "並行の形にした後、 payload の場所の列がまだ無い");
    let e = eng.entity_in("notes").unwrap();
    eng.persist_tables().unwrap();
    let (tables, ring_ino) = (inode(&path, "tables"), inode(&path, "sync.payload.seg"));
    eng.tie_to(e, "notes.note", 1u32);
    eng.oplog_commit();
    drain(&eng);
    assert!(ring(&eng).live >= 1, "前提: 最初の bridge が ring に行を足した");
    eng.persist_tables().unwrap();
    assert_eq!(inode(&path, "tables"), tables, "最初の bridge が `tables` を作り直した");
    assert_eq!(inode(&path, "sync.payload.seg"), ring_ino, "最初の bridge が payload の ring を作り直した");
    // 行の payload は ring に置かれている (辞書に落ちていない)
    let at = eng.himo_id("_sync_ops.payload_at").unwrap() as u16;
    let lsn = eng.himo_id("_sync_ops.lsn").unwrap() as u16;
    for row in eng.entities_with_himo(lsn) {
        assert!(eng.get_by_id(row, at).is_some(), "最初の bridge の行の payload が ring に無い");
    }
    assert_eq!(eng.pending_sync_ops(0).len() as u32, ring(&eng).live, "ring の行の payload が読めない");
}

/// 中身が変わった `tables` は作り直し、 変わっていなければ触らない。 開き直すと、 最後に置いた中身が読める。
///
/// falsify: 置いた中身の覚えがあれば飛ばす (中身を比べない) と、 行を足した後の `tables` が古いまま。
#[test]
fn tables_is_rewritten_only_when_it_changes() {
    let path = tmp_path("changed");
    let _cleanup = Cleanup(path.clone());
    {
        let eng = create(&path);
        eng.persist_tables().unwrap();
        let before = inode(&path, "tables");
        eng.persist_tables().unwrap();
        assert_eq!(inode(&path, "tables"), before, "変わっていない `tables` を作り直した");
        for _ in 0..3 {
            eng.entity_in("notes").unwrap();
        }
        eng.persist_tables().unwrap();
        let after = inode(&path, "tables");
        assert_ne!(after, before, "行を払い出した後の `tables` を作り直さなかった");
        eng.persist_tables().unwrap();
        assert_eq!(inode(&path, "tables"), after, "変わっていない `tables` を作り直した");
        // 閉じずに落ちた形で読む (行は 1 つも書いていないので、 払い出した数は sidecar にしか無い)
        copy_dir(std::path::Path::new(&path), std::path::Path::new(&format!("{path}-image")));
    }
    let image = format!("{path}-image");
    let _cleanup_image = Cleanup(image.clone());
    let eng = Engine::open_concurrent_with_oplog(&image, WAL).unwrap();
    assert_eq!(eng.table_eid_usage("notes").unwrap().allocated, 3, "開き直した `tables` に、 最後に置いた払い出しの数が無い");
}

/// 満杯の ring への払い出しが失敗し続けても (止まった bridge の周)、 `tables` を作り直さない。
///
/// falsify: 枠を越えた `next_local` をそのまま置くと、 失敗のたびに値が変わって作り直す。
#[test]
fn a_full_ring_does_not_rewrite_tables_on_every_failed_allocation() {
    let path = tmp_path("full");
    let _cleanup = Cleanup(path.clone());
    let eng = create(&path);
    let e = eng.entity_in("notes").unwrap();
    fill(&eng, e, 50);
    eng.persist_tables().unwrap();
    let before = inode(&path, "tables");
    for i in 0..30u32 {
        // 新しい record のたびに bridge は置こうとして失敗する
        eng.tie_to(e, "notes.note", 1_000_000 + i);
        eng.oplog_commit();
        assert_eq!(eng.transfer_oplog_to_sync_ops(), 0, "前提: ring が満杯のまま");
        assert!(eng.entity_in("_sync_ops").is_err(), "前提: ring への払い出しは失敗する");
        eng.persist_tables().unwrap();
    }
    assert_eq!(inode(&path, "tables"), before, "満杯の ring への払い出しが失敗するたびに `tables` を作り直した");
}

/// 枠を置いた `next_local` で開き直しても、 ring の slot を失わない: 生きていない slot を拾って払い出す。
/// きれいに閉じた後も、 閉じずに落ちた後 (sidecar は最後に書いた時のまま) も、 開き直した ring は枠の数だけ行を持てて、
/// 開き直す前の行と後の行が混ざらない (lsn が重ならない)。
#[test]
fn a_reopened_ring_still_holds_its_whole_capacity() {
    for clean in [true, false] {
        let made = tmp_path(if clean { "reopen-clean" } else { "reopen-kill" });
        let _cleanup = Cleanup(made.clone());
        let image = format!("{made}-image");
        let _cleanup_image = Cleanup(image.clone());
        let path = if clean { made.clone() } else { image.clone() };
        let before;
        {
            let eng = create(&made);
            let e = eng.entity_in("notes").unwrap();
            for i in 0..5u32 {
                eng.tie_to(e, "notes.note", i);
            }
            eng.oplog_sync().unwrap();
            drain(&eng);
            eng.oplog_sync().unwrap();
            eng.persist_tables().unwrap();
            before = ring(&eng).live;
            assert!(before >= 5 && before < ring(&eng).capacity / 2, "前提: ring はまだ空きの方が多い ({before} 行)");
            if !clean {
                // 閉じずに落ちた形: 開いたままの directory を写す (sidecar と本体は上で書き出した所まで)
                copy_dir(std::path::Path::new(&made), std::path::Path::new(&image));
            }
        }
        let eng = Engine::open_concurrent_with_oplog(&path, WAL).unwrap();
        eng.set_peer_id(ME);
        drain(&eng);
        let u = ring(&eng);
        assert!(u.live >= before, "開き直したら ring の行が減った ({} < {before})", u.live);
        assert!(u.allocated >= u.live, "開き直した後の払い出し済みの数が、 生きている行より少ない (払い出し済みの slot をもう一度払い出す)");
        let e = eng.entities_with_himo(eng.himo_id("notes.note").unwrap() as u16).into_iter().next().expect("前提: 行が残っている");
        fill(&eng, e, 20);
        let u = ring(&eng);
        assert_eq!(u.live, u.capacity, "開き直した後、 ring が枠の数だけ行を持てない (clean = {clean})");
        // 行は 1 つの slot に 1 つ (lsn が重ならない)
        let mut lsns: Vec<u32> = Vec::new();
        let lsn_hid = eng.himo_id("_sync_ops.lsn").unwrap() as u16;
        for row in eng.entities_with_himo(lsn_hid) {
            lsns.push(eng.get_by_id(row, lsn_hid).unwrap() as u32);
        }
        let n = lsns.len();
        lsns.sort_unstable();
        lsns.dedup();
        assert_eq!(lsns.len(), n, "ring の 2 つの行が同じ lsn を持っている (clean = {clean})");
        assert_eq!(n as u32, u.capacity, "lsn の列にある行の数が枠と違う (clean = {clean})");
        // ack で空けて、 また満杯まで使える
        let top = eng
            .pending_sync_ops(0)
            .iter()
            .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
            .filter(|r| r.author_peer == ME)
            .map(|r| r.hlc)
            .max()
            .unwrap();
        eng.ack_sync_up_to_cursors(PULLER, &[(ME, top)]).unwrap();
        assert!(eng.reclaim_sync_ops() > 0, "前提: 行を回収した");
        fill(&eng, e, 20);
    }
}

/// 続けて書き出す間、 segment が伸びるたびには manifest (`segments`) を作り直さない (1 秒に 1 回まで)。 `flush` は必ず書く。
///
/// falsify: 間隔を見ないと、 伸びた後の書き出しのたびに作り直す (この試験で 24 回)。
#[test]
fn growing_segments_do_not_rewrite_the_manifest_on_every_write_out() {
    const COLS: usize = 24;
    let path = tmp_path("manifest");
    let _cleanup = Cleanup(path.clone());
    let mut eng = Engine::create_with_capacity(&path, 4096).unwrap();
    eng.define_table("wide", 64).unwrap();
    let names: Vec<String> = (0..COLS).map(|i| format!("c{i}")).collect();
    for n in &names {
        eng.define_himo_in("wide", n, ValueType::Number, 0).unwrap();
    }
    eng.define_himo_in("wide", "last", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    let e = eng.entity_in("wide").unwrap();
    let segment_of = |col: &str| format!("himo/{:04}.seg", eng.himo_id(&format!("wide.{col}")).unwrap());
    let manifest_len = |segment: &str| -> u64 {
        let text = std::fs::read_to_string(std::path::Path::new(&path).join("segments")).unwrap();
        let name = format!("{segment} ");
        text.lines().find_map(|l| l.strip_prefix(name.as_str()).map(|n| n.parse().unwrap())).unwrap_or(0)
    };
    let file_len = |segment: &str| std::fs::metadata(std::path::Path::new(&path).join(segment)).unwrap().len();
    let t0 = Instant::now();
    let (mut last, mut rewrites) = (inode(&path, "segments"), 0u32);
    for n in &names {
        // 列ごとに最初の書き込み = その列の segment が伸びる
        eng.tie_to(e, &format!("wide.{n}"), 7u32);
        eng.body_msync().unwrap();
        let now = inode(&path, "segments");
        if now != last {
            rewrites += 1;
            last = now;
        }
    }
    let bound = 1 + t0.elapsed().as_secs() as u32 + 1;
    assert!(
        rewrites <= bound,
        "segment が伸びた後の書き出し {COLS} 回で manifest を {rewrites} 回作り直した ({:?} の間、 1 秒に 1 回なら {bound} 回まで)",
        t0.elapsed()
    );
    // 間隔が空けば書く
    std::thread::sleep(Duration::from_millis(1100));
    eng.body_msync().unwrap();
    let c = segment_of(names.last().unwrap());
    assert!(file_len(&c) > 0, "前提: 列の segment が伸びた");
    assert_eq!(manifest_len(&c), file_len(&c), "間隔が空いた後の書き出しが、 伸びた segment の長さを manifest に書かなかった");
    // 閉じる時は間隔によらず書く (直前の書き出しが間隔の中で飛ばした伸びも入る)
    let last = segment_of("last");
    eng.tie_to(e, "wide.last", 9u32);
    eng.body_msync().unwrap();
    assert!(file_len(&last) > 0, "前提: 最後の列の segment が伸びた");
    assert_eq!(Arc::strong_count(&eng), 1);
    drop(eng);
    assert_eq!(manifest_len(&last), file_len(&last), "きれいに閉じた後の manifest に、 最後に伸びた segment の長さが無い");
}
