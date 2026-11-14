//! #731 (PR #885 の読み直し B2): 翻訳した行 (他の peer の行) が、 こちらで消えた行の場所を使い回して載ると、 その場所の世代は 1 以上。
//! sync の中の道が世代 0 の eid でアプリの入口 (照合あり) を呼ぶと、 その行を 「生きていない」 と読む。 state を取り直した時の掃除
//! (author の state に無い翻訳した行を消す、 #699 / #761) が、 消さずに数えだけ進めた。
//!
//! 前提は状態で確かめる: 翻訳した行の場所 = 消えた行の場所、 その場所の今の eid ≠ 消えた行の eid (世代 > 0)。

use enchudb_engine::ValueType;
use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_oplog::{Hlc, PeerId, make_eid};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "{}/enchudb-issue731-sync-{}-{}-{}",
        std::env::temp_dir().display(),
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

struct Cleanup(Vec<String>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_dir_all(p);
        }
    }
}

fn make_engine(path: &str, peer: PeerId, cap: u32) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", cap).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

const SLOT_MASK: u32 = (1 << 24) - 1;

fn slot(eid: u64) -> u32 {
    (eid as u32) & SLOT_MASK
}

#[test]
fn the_state_sweep_deletes_a_translated_row_on_a_reused_slot() {
    let (pa, pb) = (tmp_path("a"), tmp_path("b"));
    let _c = Cleanup(vec![pa.clone(), pb.clone()]);
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let eng_a = make_engine(&pa, 1, 1000);
    let eng_b = make_engine(&pb, 2, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    sync_a.serve_state();

    // A の行 (state に載る)
    let a1 = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(a1, "notes.note", 1u32);
    eng_a.oplog_commit();
    eng_a.oplog_sync().unwrap();
    eng_a.transfer_oplog_to_sync_ops();

    // B: 自分の表 (枠 2) を埋めて 1 行消す
    let b1 = eng_b.entity_in("notes").unwrap();
    eng_b.tie_to(b1, "notes.note", 10u32);
    let b2 = eng_b.entity_in("notes").unwrap();
    eng_b.tie_to(b2, "notes.note", 11u32);
    eng_b.delete(b1);
    // A の行だった (A の state には無い) 翻訳した行が、 消えた b1 の場所を使い回して載る
    let note = eng_b.himo_id("notes.note").unwrap() as u16;
    let t = eng_b.resolve_remote_eid(make_eid(1, 777), note).unwrap();
    assert_eq!(slot(t), slot(b1), "前提: 翻訳した行は消えた行の場所を使い回した");
    assert_ne!(eng_b.slot_eid(slot(t)), b1, "前提: 使い回した場所の世代は 1 以上");
    let h = Hlc { wall: 1_000, logical: 0, peer: 1 };
    assert!(eng_b.remote_tie_apply_result(t, note, 5u32, h).applied());
    assert_eq!(eng_b.pull_raw("notes.note", 5u32).len(), 1, "前提: 翻訳した行は生きている");

    let out = sync_b.bootstrap_pull(1).expect("A の state");
    assert!(out.swept >= 1, "前提: 掃除が走った ({out:?})");
    assert!(
        eng_b.pull_raw("notes.note", 5u32).is_empty(),
        "A の state に無い翻訳した行が、 使い回した場所に載っていると掃除で消えない"
    );
    assert_eq!(eng_b.pull_raw("notes.note", 1u32).len(), 1, "A の行は state で届く");
    assert_eq!(eng_b.get(b2, "notes.note"), Some(11), "自分の行は残る");
}
