//! #1046 (review038 の A2): 受けた書き込みが、 tombstone の無い死んだ自分の行を生かすと、 次の世代で生き返る (相手の持つ世界番号と食い違い、
//! 以後の受信は捨てられる)。
//!
//! A (peer 1、 v15) の自分の行 X (世代 g | 場所 s、 世界番号 (1, g | s)) を C (peer 3) が受けている。 A を単独で開いて X を消す (版数なしの
//! 削除 = tombstone 0、 消す道は世代を g + 1 に上げて死の印を立てる)。 WAL 付きで開き直した A が、 C の X 宛ての新しい版数の Tie を 2 つ
//! 受ける:
//! - 1 つ目: `own_world_slot(g | s)` は場所の最後の行 (死んだ X = g | s) で通り、 `slot_is_dead` は偽 (tombstone 0・使い回しの印なし) →
//!   cell に書いて `ensure_live_slot` → 場所は 「g + 1・生きている」 = X が g + 1 の行として生き返る (A のアプリの持つ X の eid は消えた行)
//! - 2 つ目: `own_world_slot(g | s)` は場所の最後の行 (今は g + 1) と合わず `is_gone_own_eid` → SkippedOlder で捨てる
//!
//! = C の X は 2 つ目の値、 A の 「X の場所の行」 は 1 つ目の値で、 食い違ったまま (A がその行に書いた分は (1, g + 1 | s) で配られ、 C には
//! 別の新しい行として現れる)。 v14 (世代なし) では同じ番号で生き返り、 2 つ目も当たっていた。
//!
//! 直し方は LWW の形の決め (死んだ自分の行に受けた書き込みを当てるか捨てるか) なので user に聞く事になっている。 この試験は今の壊れ方を
//! 示すだけ (`#[ignore]`)。 主張は 「1 つ目を当てたなら 2 つ目も同じ行に当てる」 (どちらの直し方でも成り立つ: 当てない形は 1 つ目も
//! 当てない、 生き返らせる形は同じ番号で生き返らせて 2 つ目も当てる)。
#![cfg(unix)]

use enchudb_engine::ValueType;
use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

const WAL: usize = 4 * 1024 * 1024;
const SLOT_MASK: u32 = (1 << 24) - 1;

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("enchudb-review038-a2-{tag}-{}-{seq}-{nanos}", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

struct Cleanup(Vec<String>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_dir_all(p);
        }
    }
}

fn transport(peers: &[PeerId]) -> Arc<dyn Transport> {
    let mem = Arc::new(InMemoryTransport::new());
    for &p in peers {
        mem.register_peer(p);
    }
    mem
}

/// 表 notes (列 note) と、 v15 に移るための枠 1 の表 u。
fn make(path: &str, peer: PeerId) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("u", 1).unwrap();
    eng.define_table("notes", 100).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn slot(eid: u64) -> u32 {
    (eid as u32) & SLOT_MASK
}

#[test]
#[ignore = "#1046 (review038 の A2): 直し方 (LWW の形) は user の決め待ち。 今の壊れ方を示す"]
fn a_remote_write_to_an_own_row_deleted_without_a_tombstone_keeps_one_identity() {
    let (pa, pc) = (tmp_path("a"), tmp_path("c"));
    let _c = Cleanup(vec![pa.clone(), pc.clone()]);
    let transport = transport(&[1, 3]);
    let eng_c = make(&pc, 3);
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    let x = {
        let eng_a = make(&pa, 1);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        // v15 に移る (枠 1 の表で使い回す)
        let u = eng_a.entity_in("u").unwrap();
        eng_a.delete(u);
        let u2 = eng_a.entity_in("u").unwrap();
        assert_ne!(u2, u, "前提: v15");
        let x = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(x, "notes.note", 1u32);
        eng_a.oplog_sync().unwrap();
        sync_a.publish_since(Hlc::ZERO);
        sync_c.pull_once(1);
        assert_eq!(eng_c.pull_raw("notes.note", 1u32).len(), 1, "前提: C に X がある");
        drop(sync_a);
        x
    };
    // A を単独で開いて X を消す (版数なしの削除: tombstone を置かない、 世代は上げる)
    {
        let mut eng = Engine::open_standalone(&pa).unwrap();
        eng.delete(x);
        assert!(!eng.is_live(x), "前提: X を消した");
        eng.flush().unwrap();
    }
    let eng_a = Engine::open_concurrent_with_oplog(&pa, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert!(!eng_a.is_live(x), "前提: X は消えている");
    assert!(eng_a.entities().iter().all(|&e| slot(e) != slot(x)), "前提: X の場所に行は無い");
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    // C が X に 2 回書く (版数は X の cell より新しい)
    let x_on_c = eng_c.pull_raw("notes.note", 1u32)[0];
    eng_c.tie_to(x_on_c, "notes.note", 2u32);
    eng_c.oplog_sync().unwrap();
    sync_c.publish_since(Hlc::ZERO);
    let first = sync_a.pull_once(3);
    let revived: Vec<u64> = eng_a.entities().into_iter().filter(|&e| slot(e) == slot(x)).collect();
    eng_c.tie_to(x_on_c, "notes.note", 3u32);
    eng_c.oplog_sync().unwrap();
    sync_c.publish_since(Hlc::ZERO);
    let second = sync_a.pull_once(3);
    let now: Vec<(u64, Option<u64>)> =
        eng_a.entities().into_iter().filter(|&e| slot(e) == slot(x)).map(|e| (e, eng_a.get(e, "notes.note"))).collect();
    eprintln!(
        "A2: X {x:#x} / 1 つ目 {first:?} の後に X の場所の行 {revived:x?} / 2 つ目 {second:?} の後 {now:x?} (C の X = {:?})",
        eng_c.get(x_on_c, "notes.note")
    );
    // 1 つ目を当てて X の場所に行を作ったなら、 2 つ目も同じ行に当たる (C と同じ値)
    if !revived.is_empty() {
        assert_eq!(
            now.iter().map(|&(_, v)| v).collect::<Vec<_>>(),
            vec![Some(3)],
            "受けた書き込みが死んだ X を別の番号 {revived:x?} で生き返らせ (X は {x:#x})、 次の書き込みはその行に当たらなかった"
        );
    }
}
