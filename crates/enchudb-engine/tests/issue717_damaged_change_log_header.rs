//! #717 (engine を通した再現): `changefeed.seg` の header の crc が合わない記録を 「空」 として開いた後、 次に開いた時に、
//! data に残っていた前の世代の entry が新しい番号の続きとして戻った (古い record を新しい番号で渡す。 `Truncated` になるべき
//! 古い位置の読み手が黙って通る)。
//!
//! 直した後: 壊れた header で開いた記録の番号は、 前の世代のどの番号よりも先から始まる。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::change_log::{Truncated, RESTART_GAP};
use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue717-{tag}-{}", std::process::id()));
        let p = p.to_string_lossy().into_owned();
        let _ = db_files::remove_db(&p);
        Tmp(p)
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = db_files::remove_db(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 受けた record の (番号, Tie なら値)。
#[derive(Default)]
struct Rec {
    seen: Mutex<Vec<(u64, Option<u64>)>>,
}
impl ChangeListener for Rec {
    fn on_changes_at(&self, first: u64, recs: &[WireRecord]) {
        let mut s = self.seen.lock().unwrap();
        for (k, r) in recs.iter().enumerate() {
            let v = match r.op {
                enchudb_oplog::oplog::DecodedOp::Tie { value, .. } => Some(value),
                _ => None,
            };
            s.push((first + k as u64, v));
        }
    }
}

fn wait_until(mut f: impl FnMut() -> bool, secs: u64) -> bool {
    let until = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < until {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    f()
}

/// `values` を 1 件ずつ書いて書き出す。
fn write(eng: &Engine, values: std::ops::RangeInclusive<u64>) {
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    for v in values {
        eng.tie_to_by_id(e, ts, v);
    }
    eng.oplog_sync().unwrap();
}

/// header の crc (file の 76..80) を壊す (電源断で header の page がちぎれた像の代わり)。
fn damage_header_crc(db: &str) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let path = std::path::Path::new(db).join("changefeed.seg");
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    let mut b = [0u8; 4];
    f.seek(SeekFrom::Start(76)).unwrap();
    f.read_exact(&mut b).unwrap();
    b[0] ^= 0xff;
    f.seek(SeekFrom::Start(76)).unwrap();
    f.write_all(&b).unwrap();
    f.sync_all().unwrap();
}

#[test]
fn a_damaged_change_log_header_never_hands_old_entries_under_new_numbers() {
    let tmp = Tmp::new("engine");
    // 1 つ目の session: 前の世代 (値 1..=100)
    let old_range = {
        let eng = Engine::create_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
        eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
        eng.enable_change_log(1 << 20).unwrap();
        write(&eng, 1..=100);
        eng.change_log_range().unwrap()
    };
    assert!(old_range.1 - old_range.0 >= 100, "前提: 前の世代の 100 件が記録に在る ({old_range:?})");
    damage_header_crc(&tmp.0);

    // 2 つ目の session: 壊れた header で開く → 新しい世代 (値 1001..=1003)
    let new_range = {
        let eng = Engine::open_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
        let opened = eng.change_log_range().expect("前提: 記録は付いたまま");
        assert!(
            opened.0 >= old_range.1 + RESTART_GAP,
            "壊れた header で開いた記録の番号が、 前の世代 ({old_range:?}) の先から始まっていない: {opened:?}"
        );
        // 前の世代の位置の読み手は Truncated
        for pos in [old_range.0, old_range.0 + 10, old_range.1 - 1, old_range.1] {
            let r = eng.add_change_listener_from(Arc::new(Rec::default()), pos);
            assert_eq!(r, Err(Truncated { from: opened.0 }), "前の世代の位置 {pos}");
        }
        write(&eng, 1001..=1003);
        let r = eng.change_log_range().unwrap();
        assert!(r.1 >= opened.1 + 3, "前提: 新しい世代の 3 件が記録に入った ({opened:?} → {r:?})");
        r
    };

    // 3 つ目の session: 開き直しても、 前の世代の entry が新しい番号の続きとして戻らない
    let eng = Engine::open_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
    let reopened = eng.change_log_range().unwrap();
    assert_eq!(reopened, new_range, "開き直したら記録の範囲が変わった (前の世代の entry を拾い足した)");
    for pos in [old_range.0, old_range.0 + 10, old_range.1 - 1, old_range.1] {
        let r = eng.add_change_listener_from(Arc::new(Rec::default()), pos);
        assert_eq!(r, Err(Truncated { from: reopened.0 }), "前の世代の位置 {pos} の読み手が通った");
    }
    // 最古の番号から受け直す: 番号は続き、 件数は範囲と同じ、 最後の 3 件が新しい世代
    let rec = Arc::new(Rec::default());
    eng.add_change_listener_from(rec.clone(), reopened.0).unwrap();
    let want = (reopened.1 - reopened.0) as usize;
    assert!(wait_until(|| rec.seen.lock().unwrap().len() >= want, 60), "受け直しが届かない");
    let seen = rec.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), want);
    assert!(seen.iter().enumerate().all(|(k, (no, _))| *no == reopened.0 + k as u64), "番号が続いていない");
    let values: Vec<u64> = seen.iter().filter_map(|(_, v)| *v).collect();
    assert_eq!(&values[values.len() - 3..], &[1001, 1002, 1003]);
    // 新しい世代の 3 件より後ろに、 前の世代の値 (1..=100) が出ない
    let first_new = values.iter().position(|v| *v == 1001).unwrap();
    assert!(values[first_new..].iter().all(|v| *v > 1000), "新しい世代の後ろに前の世代の値: {values:?}");
}
