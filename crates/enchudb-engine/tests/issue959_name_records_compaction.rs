//! #959: 名前のある表で 「消して足す」 を続ける DB の、 消した名前 (`pktombs`) と前の名前 (`pkformer` + `pkformer.log`) の書き出し。
//!
//! 前は書き出しが `pk_tombs` / `pk_former` の書き込みの lock を握ったまま記録の全部を並べ直していた (file の無い DB の最初の書き出しと、
//! disk が大きくなった時のまとめ直し)。 その間、 消す / 場所を使い回す / 名前を書く書き手が止まった (使い回し 10 万回の churn で 1 回
//! 20〜36 ms)。 今は lock の下では取り出すだけで、 最初の書き出しは覚えた record をそのまま、 まとめ直しは追記の済んだ disk から作る。
//! 止まりの長さ自体は試験にしない (時間に頼る)。 ここは、 その道を通っても disk の中身が memory と同じで、 まとめ直しが走って disk が
//! 縮み、 開き直すと同じ記録が戻ることを見る。

use enchudb_engine::ValueType;
use enchudb_engine::db_files::{PKFORMER, PKFORMER_LOG, PKTOMBS, path_for};
use enchudb_engine::engine::Engine;
use enchudb_oplog::Hlc;
use std::sync::Arc;

const ROWS: u32 = 512;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-issue959-{tag}-{}-{nanos}", std::process::id())
}

fn len(path: &str, name: &str) -> u64 {
    std::fs::metadata(path_for(path, name)).map(|m| m.len()).unwrap_or(0)
}

fn open(path: &str) -> Arc<Engine> {
    let eng = Engine::open_concurrent_with_oplog(path, 64 << 20).unwrap();
    eng.set_peer_id(1);
    eng
}

/// 満杯の名前のある表で、 行の名前を 2 つの間で行き来させながら消して足す (同じ名前を何度も消す = `pktombs` の record が生きている記録より
/// 速く増える。 使い回すたびに前の住人の名前が identity に移る = `pkformer` が増える)。 書き出しのたびに disk と memory が同じ、
/// `pktombs` は追記だけでなくまとめ直しで縮む、 線より前の削除を忘れると `pkformer` + log も縮む、 開き直しても同じ記録。
#[test]
fn name_records_are_compacted_outside_the_lock_and_read_back_the_same() {
    let path = tmp("churn");
    {
        let mut eng = Engine::create_with_cell_version(&path, 1 << 16).unwrap();
        eng.define_table("t", ROWS).unwrap();
        eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
        let key = eng.himo_id("t.key").unwrap() as u16;
        eng.set_table_pk("t", key).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
    }
    let eng = open(&path);
    let key = eng.himo_id("t.key").unwrap() as u16;
    let mut rows: Vec<u64> = (0..ROWS)
        .map(|i| {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, key, i);
            e
        })
        .collect();
    let mut names: Vec<u32> = (0..ROWS).collect();
    eng.persist_sync_state().unwrap();
    let (mut tombs_shrank, mut prev_tombs) = (0, 0u64);
    // 30 回目の最後に消した名前の削除の版数 (後で、 これより前の削除を忘れる線にする)
    let mut line = None;
    for round in 0..40u32 {
        for i in 0..(ROWS / 2) as usize {
            let at = (i * 2 + round as usize % 2) % ROWS as usize;
            let gone = names[at];
            eng.delete(rows[at]);
            if round == 30 {
                line = eng.deleted_name_version(key, gone as u64);
            }
            // 表は満杯 = 消した場所を使い回す。 名前は i と i + ROWS を行き来する (同じ名前を何度も消す)
            let e = eng.entity_in("t").unwrap();
            names[at] = if names[at] < ROWS { names[at] + ROWS } else { names[at] - ROWS };
            eng.tie_to_by_id(e, key, names[at]);
            rows[at] = e;
        }
        eng.persist_sync_state().unwrap();
        if round == 0 {
            // disk の無い DB の最初の書き出しは log だけ (memory の全部を `pkformer` に並べない)
            assert_eq!(len(&path, PKFORMER), 0, "最初の書き出しで pkformer を並べた");
            assert!(len(&path, PKFORMER_LOG) > 0, "前提: 前の住人の名前を書き出した");
        }
        assert_eq!(eng.pkformer_disk_diff().unwrap(), None, "{round} 回目: pkformer の disk と memory が違う");
        let tombs = len(&path, PKTOMBS);
        if tombs < prev_tombs {
            tombs_shrank += 1;
        }
        prev_tombs = tombs;
    }
    assert!(tombs_shrank >= 2, "pktombs が追記だけで、 まとめ直していない ({tombs_shrank} 回縮んだ、 今 {prev_tombs} B)");
    let deleted = eng.deleted_primary_key_count();
    assert_eq!(deleted, 2 * ROWS as usize, "前提: 2 つの名前を両方とも消した");
    let versions: Vec<Option<Hlc>> = (0..2 * ROWS).map(|n| eng.deleted_name_version(key, n as u64)).collect();
    assert!(versions.iter().all(Option::is_some));
    // 線より前の削除を忘れる = 使い回した場所の前の住人の名前 (identity) の 3/4 ほども外れる → disk が今の中身の 2 倍 + 64 KiB を
    // 越え、 まとめ直して縮む。 残る identity (線より後の使い回し) は log にしか無いので、 まとめ直しが log を読まなければ消える
    let former_before = len(&path, PKFORMER) + len(&path, PKFORMER_LOG);
    let forgotten = eng.forget_deletes_before(line.expect("前提: 30 回目に消した"));
    assert!(forgotten > ROWS as usize * 30 / 2, "前提: 30 回分ほど忘れた ({forgotten})");
    eng.persist_sync_state().unwrap();
    let former_after = len(&path, PKFORMER) + len(&path, PKFORMER_LOG);
    assert_eq!(eng.pkformer_disk_diff().unwrap(), None, "忘れた後の pkformer の disk と memory が違う");
    assert!(len(&path, PKFORMER) > 0, "前提: 忘れた後も残る前の名前がある (pkformer に並べた)");
    assert!(
        former_after * 2 < former_before,
        "忘れた後に pkformer + log をまとめ直していない ({former_before} → {former_after} B)"
    );
    // 忘れる前の記録で開き直す像は作らない (忘れた後の記録が開き直しで戻るか)
    let deleted = eng.deleted_primary_key_count();
    drop(eng);
    let back = open(&path);
    assert_eq!(back.deleted_primary_key_count(), deleted, "開き直すと消した名前の数が違う");
    assert_eq!(back.pkformer_disk_diff().unwrap(), None);
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
}

/// 忘れる前の記録 (まとめ直しを何度か踏んだ `pktombs` と、 追記を重ねた `pkformer.log`) を、 閉じずに写した像 (きれいに閉じていない) から
/// 開いても、 消した名前の版数と前の名前が同じ。
#[test]
fn compacted_name_records_read_back_after_an_unclean_copy() {
    let path = tmp("copy");
    {
        let mut eng = Engine::create_with_cell_version(&path, 1 << 16).unwrap();
        eng.define_table("t", ROWS).unwrap();
        eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
        let key = eng.himo_id("t.key").unwrap() as u16;
        eng.set_table_pk("t", key).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
    }
    let eng = open(&path);
    let key = eng.himo_id("t.key").unwrap() as u16;
    let mut rows: Vec<u64> = (0..ROWS)
        .map(|i| {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, key, i);
            e
        })
        .collect();
    let mut names: Vec<u32> = (0..ROWS).collect();
    for round in 0..24u32 {
        for i in 0..(ROWS / 2) as usize {
            let at = (i * 2 + round as usize % 2) % ROWS as usize;
            eng.delete(rows[at]);
            let e = eng.entity_in("t").unwrap();
            names[at] = if names[at] < ROWS { names[at] + ROWS } else { names[at] - ROWS };
            eng.tie_to_by_id(e, key, names[at]);
            rows[at] = e;
        }
        eng.persist_sync_state().unwrap();
    }
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let versions: Vec<Option<Hlc>> = (0..2 * ROWS).map(|n| eng.deleted_name_version(key, n as u64)).collect();
    let img = format!("{path}-img");
    enchudb_engine::copy_db_dir(std::path::Path::new(&path), std::path::Path::new(&img)).unwrap();
    drop(eng);
    let back = open(&img);
    let got: Vec<Option<Hlc>> = (0..2 * ROWS).map(|n| back.deleted_name_version(key, n as u64)).collect();
    assert_eq!(got, versions, "写した像を開くと消した名前の版数が違う");
    assert_eq!(back.pkformer_disk_diff().unwrap(), None);
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&img);
}
