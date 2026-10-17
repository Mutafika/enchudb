//! #502: sync の DB で WAL が満杯の時、 行の lock を握ったまま空きを待つ書き手が、 空きを作る書き出し係を止めていた。
//!
//! 行の lock を握ったまま WAL に積む書き手 (schema の `commit()` = `write_row` の中の `tie_*`、 Leaf の書き込み) は、 WAL が
//! 満杯だと書き出し係 (consumer) が畳むのを待つ。 sync の DB の書き出し係は、 畳む前に bridge で record を `_sync_ops` の行に
//! 写し、 その行を書くのに行の lock を取る。 その行が書き手の行と同じ stripe に当たると書き手を待ち、 書き手は待ちの上限
//! (5 秒) で諦めて record を落とした。 内部の表の行はユーザの行と別の stripe を使う (`row_lock` の module doc)。
//!
//! WAL 256 KiB、 書き手 1 本が 64 行に順に書く (100 回ごとに commit)。 直す前 (行を握って書く): 60 秒で 2041 回しか書けず、
//! 5 秒止まった書き込みが 12 回 (落ち 12)。 止まっている間、 書き出し係は `transfer_oplog_to_sync_ops` → `set_cell_local` の
//! 行の lock で回り続けていた。 満杯で record が落ちるのは別の話 (sync の DB は bridge が追いつかないと落ちる、 #57 の floor で
//! 取り直す) なので、 ここでは見ない。

use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::Arc;
use std::time::{Duration, Instant};

const WRITES: u32 = 50_000;
/// 書き手の待ちの上限 (5 秒) より十分短い
const SLOW: Duration = Duration::from_secs(1);

struct Cleanup(String);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = db_files::remove_db(&self.0);
    }
}

/// sync の DB (WAL 256 KiB) で `write` を `WRITES` 回。 1 秒以上かかった書き込みがあれば、 その回と時間を返す。
fn first_slow_write(tag: &str, ty: ValueType, write: impl Fn(&Engine, u64, u16, u32)) -> Option<(u32, Duration)> {
    let path = format!("/tmp/enchudb-sync-bridge-row-lock-{tag}-{}", std::process::id());
    let _ = db_files::remove_db(&path);
    let mut eng = Engine::create_with_capacity(&path, 1 << 16).unwrap();
    eng.define_table("t", 1 << 12).unwrap();
    eng.define_himo_in("t", "v", ty, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 256 * 1024).unwrap();
    let hid = eng.himo_id("t.v").unwrap() as u16;
    let rows: Vec<_> = (0..64).map(|_| eng.entity_in("t").unwrap()).collect();
    let mut slow = None;
    for i in 0..WRITES {
        let started = Instant::now();
        write(&eng, rows[i as usize % rows.len()], hid, i);
        if started.elapsed() >= SLOW {
            slow = Some((i, started.elapsed()));
            break;
        }
        if i % 100 == 99 {
            eng.oplog_commit();
        }
        // 受け手が ack しないと `_sync_ops` の ring が満杯で bridge が止まる (#368) ので、 ときどき全部 ack する
        if i % 2000 == 1999 {
            eng.ack_sync(7, u32::MAX).unwrap();
        }
    }
    drop(eng);
    drop(Cleanup(path));
    slow
}

/// schema の `commit()` と同じ形: 行を握って数の列を書く。
#[test]
fn a_writer_holding_its_row_does_not_stall_the_bridge() {
    let slow = first_slow_write("number", ValueType::Number, |eng, e, hid, i| {
        let _row = eng.write_row(e);
        eng.tie_to_by_id(e, hid, i);
    });
    assert_eq!(slow, None, "(回, 時間): 行を握った書き込みが、 WAL の空きを待って止まった");
}

/// Leaf の書き込みは中で行を握る。
#[test]
fn a_leaf_writer_does_not_stall_the_bridge() {
    let slow = first_slow_write("leaf", ValueType::Leaf, |eng, e, hid, i| {
        eng.tie_text_to_by_id(e, hid, &format!("memo-{i}"));
    });
    assert_eq!(slow, None, "(回, 時間): Leaf の書き込みが、 WAL の空きを待って止まった");
}
