//! #856: 落ちた後に開いた sync の DB で、 まだ配っていない `_sync_ops` の行が、 その次に開いた時に消えない。
//!
//! 消えていた道 (0.37.4 まで): `tables` の sidecar に置いた ring の `next_local` は、 最後に bridge が払い出した分だけ古い
//! (bridge の行は WAL に載らないので、 次の書き出しは `tables` を書き直さない。 `oplog_sync()` は `tables` を書かない)。
//! 相手が ack して行が消えた (tombstone が立つ) 後に落ちると、 開いた `next_local` は生きている行の最大 + 1 までしか戻らず、
//! 次の bridge が tombstone の付いた slot を、 前の住人の版数を落とさない道で払い出す。 その行は、 もう一度開いた時に
//! 「途中で切れた削除」 (tombstone があり、 版数が tombstone より新しくない cell が生きている) と読まれて消えた。 WAL は畳まれて
//! いて record は無く、 floor も上がらない = 相手に黙って届かない。
//!
//! 直し (#847 / #856): ring の `next_local` は sidecar にいつも枠を置き、 開いた ring は sidecar の値によらず 「払い出し
//! 切った ring」 として、 生きていない slot を、 前の住人の版数を落としてから払い出す (0.37.4 以前が残した sidecar は実際の
//! 値なので、 開く側でも進める)。
#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::DecodedOp;
use std::sync::Arc;

const ME: u32 = 1;
const PULLER: u32 = 77;
const WAL: usize = 4 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("enchudb-issue856-{tag}-{}-{nanos}", std::process::id())).to_str().unwrap().to_string()
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
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

/// ring にある、 `notes.note` に書いた値 (lsn の順)。
fn notes(eng: &Engine) -> Vec<u64> {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter_map(|r| match r.op {
            DecodedOp::Tie { value, .. } => Some(value),
            _ => None,
        })
        .collect()
}

fn open(path: &str) -> Arc<Engine> {
    let eng = Engine::open_concurrent_with_oplog(path, WAL).unwrap();
    eng.set_peer_id(ME);
    eng
}

/// 行が ack で消えた後に落ちた像を開いて書いた record が、 きれいに閉じてもう一度開いた後も ring に残る。
///
/// falsify: ring の `next_local` を実際の値で sidecar に置く (0.37.4 までの形) と、 開いた後の最初の行が tombstone の付いた
/// slot に前の住人の版数のまま置かれ、 もう一度開いた時に消える。
#[test]
fn a_row_bridged_after_a_crash_survives_the_next_open() {
    survives_the_next_open(false);
}

/// 0.37.4 以前が落ちて残した像 (sidecar の ring の `next_local` が実際の値) を開いた時も同じ。
///
/// falsify: 開く時に ring の `next_local` を枠まで進めないと、 上と同じに消える。
#[test]
fn a_row_bridged_after_a_crash_left_by_an_older_version_survives_the_next_open() {
    survives_the_next_open(true);
}

/// `tables` の sidecar の、 ring の `next_local` を `value` に書き換える (0.37.4 以前の sidecar の形)。
/// 表ごとの並びは 名前の長さ (u32) / 名前 / eid の範囲 (u32 × 2) / `next_local` (u32)。
fn set_ring_next_local(db: &str, value: u32) {
    let path = std::path::Path::new(db).join("tables");
    let mut bytes = std::fs::read(&path).unwrap();
    let name = b"_sync_ops";
    let at = bytes.windows(name.len()).position(|w| w == name).expect("sidecar に ring の表がある") + name.len() + 8;
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();
}

fn survives_the_next_open(older_sidecar: bool) {
    let made = tmp_path(if older_sidecar { "older" } else { "made" });
    let image = format!("{made}-image");
    let (_c1, _c2) = (Cleanup(made.clone()), Cleanup(image.clone()));
    let e;
    {
        let mut eng = Engine::create_with_capacity(&made, 4096).unwrap();
        eng.define_table("notes", 64).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
        eng.set_peer_id(ME);
        // consumer の周期の書き出しを止める (止めないと、 当たり方によって周期が `tables` を書き直す)
        let hold = eng.hold_consumer();
        e = eng.entity_in("notes").unwrap();
        // 先に 1 回、 書いて ack まで通す (相手の行と列ができる = この後の ack は `tables` を変えない)。 ここで `tables` を書く:
        // sidecar の ring の `next_local` は、 この時までに払い出した数
        eng.tie_to(e, "notes.note", 0u32);
        eng.oplog_sync().unwrap();
        eng.ack_sync(PULLER, eng.current_sync_lsn() + 1).unwrap();
        eng.reclaim_sync_ops();
        eng.persist_tables().unwrap();
        let allocated = eng.table_eid_usage("_sync_ops").unwrap().allocated;
        for v in [1u32, 2, 3] {
            eng.tie_to(e, "notes.note", v);
        }
        eng.oplog_sync().unwrap();
        assert_eq!(notes(&eng), [1, 2, 3], "前提: 最初の 3 件が ring に載った");
        // 相手が全部受けて ack → 行が全部消える (最後の行まで。 1 行でも残ると、 開いた `next_local` はその後ろから始まる)
        eng.ack_sync(PULLER, eng.current_sync_lsn() + 1).unwrap();
        let reclaimed = eng.reclaim_sync_ops();
        assert!(notes(&eng).is_empty(), "前提: ack 済みの行が消えた (消した {reclaimed} 行、 残り {:?})", notes(&eng));
        assert_eq!(eng.table_eid_usage("_sync_ops").unwrap().live, 0, "前提: ring に生きている行が無い");
        eng.oplog_sync().unwrap();
        // 閉じずに落ちた形
        copy_dir(std::path::Path::new(&made), std::path::Path::new(&image));
        if older_sidecar {
            set_ring_next_local(&image, allocated);
        }
        drop(hold);
    }
    // 落ちた像を開いて 1 件書く。 像の WAL は畳まれていないので、 前の 3 件も写し直される (相手は同じ版数として読み捨てる)
    let before;
    {
        let eng = open(&image);
        eng.tie_to(e, "notes.note", 777u32);
        eng.oplog_sync().unwrap();
        before = notes(&eng);
        assert!(before.contains(&777), "前提: 開いた後に書いた record が ring に載った ({before:?})");
        assert_eq!(Arc::strong_count(&eng), 1);
    }
    // きれいに閉じた後は WAL が畳まれている: ring の行が消えると、 写し直す元が無い
    let eng = open(&image);
    assert_eq!(notes(&eng), before, "まだ配っていない行が、 もう一度開いたら消えた");
    assert!(eng.verify_invariants().violations.is_empty(), "{:?}", eng.verify_invariants().violations);
}
