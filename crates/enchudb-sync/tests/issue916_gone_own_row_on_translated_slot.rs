//! #916: 自分の消えた行 X (世代 g | 場所 s) の場所を、 受信で入った相手の行 F1 が使い回した (世代 g + 1、 逆写像 (C, f1) → s) 後に、
//! X の record が F1 に当たる・F1 宛てに配られる。
//!
//! - [B] kill / 電源断の直後に開く: X の record (checkpoint の後ろ) の再生が F1 の場所を X の世代に戻し (`restore`)、 X の Delete が
//!   F1 を消した。 bridge は前の宛名の覚え (`FormerAddresses`、 メモリだけ) が無いので今の逆写像 (C, f1) で X の Tie / Delete を配り、
//!   C の F1 も消えた。
//! - [E] アプリが X の古い eid を的に `tie_ref_to` すると (的の生死は見ない、 cell は g | s)、 bridge が今の逆写像 (C, f1) で TieRef
//!   を出した (相手では F1 を指し、 A では消えた行を指す)。 逆の形 (消えた翻訳した行 F0 を指し、 場所を自分の行が使い回した) は素の
//!   Tie で出て、 相手に幻の行ができた。
//!
//! 前提は状態で確かめる (F1 の場所 = X の場所、 checkpoint が X の record の手前、 F1 の今の eid ≠ X の eid)。
#![cfg(unix)]

use enchudb_engine::ValueType;
use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_oplog::{Hlc, PeerId, eid_local, make_eid};
use enchudb_sync::Syncer;
use std::sync::Arc;

const WAL: usize = 4 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("enchudb-issue916-{tag}-{}-{seq}-{nanos}", std::process::id())).to_str().unwrap().to_string()
}

struct Cleanup(Vec<String>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_dir_all(p);
        }
    }
}

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

const SLOT_MASK: u32 = (1 << 24) - 1;

fn slot(eid: u64) -> u32 {
    (eid as u32) & SLOT_MASK
}

fn transport(peers: &[PeerId]) -> Arc<dyn Transport> {
    let mem = Arc::new(InMemoryTransport::new());
    for &p in peers {
        mem.register_peer(p);
    }
    mem
}

/// 表 notes (枠 `cap`、 列 note)。
fn make_notes(path: &str, peer: PeerId, cap: u32) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", cap).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// 表 companies (枠 `cap`、 列 name) と users (列 uid、 ref company → companies)。
fn make_refs(path: &str, peer: PeerId, cap: u32) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("companies", cap).unwrap();
    eng.define_himo_in("companies", "name", ValueType::Number, 0).unwrap();
    eng.define_table("users", 100).unwrap();
    eng.define_himo_in("users", "uid", ValueType::Number, 0).unwrap();
    eng.define_ref_in("users", "company", "companies").unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// [B]: A (枠 1 の表) が X を作って書き出し、 X に書いて消す (checkpoint は X の Tie の手前)。 C の行 F1 を受けて X の場所を使い回し、
/// そこで落ちた像 (page cache は残る = 写し) を開く。 F1 は A に残り、 写しの A を受けた C でも残る。
///
/// falsify: 再生の 「翻訳した行の場所の古い世代の record は当てない」 を外すと、 写しの A で F1 が消える。 bridge の 「record の世代が
/// 場所の今の行の世代と違えば今の逆写像を使わない」 を外すと、 C で F1 が消える。 宛名の分からない Delete も配らないと、 C に X が残る。
#[test]
fn a_gone_own_rows_records_do_not_hit_a_translated_row_after_a_crash() {
    let (pa, pc) = (tmp_path("a"), tmp_path("c"));
    let image = format!("{pa}-image");
    let _c = Cleanup(vec![pa.clone(), pc.clone(), image.clone()]);
    let transport = transport(&[1, 3]);
    let eng_c = make_notes(&pc, 3, 100);
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    let (x, f1_on_a, f1) = {
        let eng_a = make_notes(&pa, 1, 1);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        let hold = eng_a.hold_consumer();
        let x = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(x, "notes.note", 1u32);
        eng_a.oplog_sync().unwrap();
        // C は X を受けている (落ちた後に配る X の Delete で消える)
        sync_a.publish_since(Hlc::ZERO);
        sync_c.pull_once(1);
        assert_eq!(eng_c.pull_raw("notes.note", 1u32).len(), 1, "前提: C に X がある");
        let checkpoint = eng_a.stats().oplog_checkpoint;
        // checkpoint の後ろ: X の Tie と Delete
        eng_a.tie_to(x, "notes.note", 2u32);
        eng_a.delete(x);
        // C の行 F1 を受けて、 X の場所を使い回す
        let f1 = eng_c.entity_in("notes").unwrap();
        eng_c.tie_to(f1, "notes.note", 50u32);
        eng_c.oplog_sync().unwrap();
        sync_c.publish_since(Hlc::ZERO);
        let out = sync_a.pull_once(3);
        assert!(out.applied > 0, "前提: A が F1 を受けた ({out:?})");
        let on_a = eng_a.pull_raw("notes.note", 50u32);
        assert_eq!(on_a.len(), 1, "前提: A に F1 がある");
        let f1_on_a = on_a[0];
        assert_eq!(slot(f1_on_a), slot(x), "前提: F1 は X の場所を使い回した");
        assert_ne!(f1_on_a, x, "前提: 使い回した場所の世代は X より新しい");
        assert_eq!(eng_a.stats().oplog_checkpoint, checkpoint, "前提: checkpoint は X の Tie / Delete の手前");
        copy_dir(std::path::Path::new(&pa), std::path::Path::new(&image));
        drop(hold);
        drop(sync_a);
        (x, f1_on_a, f1)
    };
    let eng_a = Engine::open_concurrent_with_oplog(&image, WAL).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(eng_a.get(f1_on_a, "notes.note"), Some(50), "落ちた後の再生が、 X の record で F1 を消した / 書き換えた");
    assert_eq!(eng_a.get(x, "notes.note"), None, "X は消えたまま");
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    eng_a.oplog_sync().unwrap();
    sync_a.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sync_c.pull_once(1);
    }
    assert_eq!(eng_c.get(f1, "notes.note"), Some(50), "落ちた後の bridge が、 X の record を F1 宛てに配った");
    assert!(eng_c.pull_raw("notes.note", 1u32).is_empty(), "落ちた後の bridge が X の Delete を配らない (C に X が残る)");
    assert_eq!(eng_a.get(f1_on_a, "notes.note"), Some(50), "A の F1 は残る");
}

/// [E]: A が X を消し、 X の場所を C の行 F1 が使い回した後、 アプリが X の古い eid を的に ref を張る。 C の W の ref は F1 を指さない
/// (A でも W は F1 を指していない)。
///
/// `again`: C が F1 を消した後に ref を張り、 bridge の前に A の行 Z がその場所をもう一度使い回す (ref を張った後の変わり目の覚えは
/// F1 の宛名 = 世代が ref の値と違う)。 consumer を止めて、 Z が使い回した所を bridge が越える前に配る (受信の中継が bridge を
/// 回すので、 ref と使い回しの間に pull を挟まない)。
///
/// `pruned`: ref を張る前に bridge を 2 回回して前の宛名の覚えを外す (覚えの無い道)。
///
/// falsify: bridge の Ref の値の世代の照合を外すと、 C で W が F1 を指す (覚えの無い道は `pruned`)。 覚えの 「書いた後の変わり目の行の
/// 世代が record の世代と違えば使わない」 を外すと、 `again` で C の W が F1 を指す。
#[test]
fn a_ref_to_a_gone_own_row_does_not_point_at_the_translated_row_on_its_slot() {
    for (again, pruned) in [(false, false), (false, true), (true, false)] {
        let (pa, pc) = (tmp_path("ea"), tmp_path("ec"));
        let _c = Cleanup(vec![pa.clone(), pc.clone()]);
        let transport = transport(&[1, 3]);
        let eng_a = make_refs(&pa, 1, 1);
        let eng_c = make_refs(&pc, 3, 100);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        let sync_c = Syncer::new(eng_c.clone(), transport.clone());
        let x = eng_a.entity_in("companies").unwrap();
        eng_a.tie_to(x, "companies.name", 1u32);
        eng_a.delete(x);
        eng_a.oplog_sync().unwrap();
        sync_a.publish_since(Hlc::ZERO);
        sync_c.pull_once(1);
        let f1 = eng_c.entity_in("companies").unwrap();
        eng_c.tie_to(f1, "companies.name", 50u32);
        eng_c.oplog_sync().unwrap();
        sync_c.publish_since(Hlc::ZERO);
        let hold = eng_a.hold_consumer();
        sync_a.pull_once(3);
        let on_a = eng_a.pull_raw("companies.name", 50u32);
        assert_eq!(on_a.len(), 1, "前提: A に F1 がある");
        assert_eq!(slot(on_a[0]), slot(x), "前提: F1 は X の場所を使い回した");
        assert_ne!(on_a[0], x, "前提: 使い回した場所の世代は X より新しい");

        if again {
            eng_c.delete(f1);
            eng_c.oplog_sync().unwrap();
            sync_c.publish_since(Hlc::ZERO);
            sync_a.pull_once(3);
            assert!(eng_a.pull_raw("companies.name", 50u32).is_empty(), "前提: A で F1 が消えた");
        }
        if pruned {
            eng_a.oplog_sync().unwrap();
            eng_a.oplog_sync().unwrap();
            assert_eq!(eng_a.former_addresses_len(), 0, "前提: 前の宛名の覚えが無い");
        }
        let w = eng_a.entity_in("users").unwrap();
        eng_a.tie_to(w, "users.uid", 7u32);
        eng_a.tie_ref_to(w, "users.company", x);
        let a_val = eng_a.get(w, "users.company").expect("前提: A の W に ref がある");
        assert_eq!(a_val as u32, eid_local(x), "前提: A の W は X の番号を指す");
        assert!(!eng_a.is_live(make_eid(1, a_val as u32)), "前提: A の W の的は生きていない");
        if again {
            let z = eng_a.entity_in("companies").unwrap();
            eng_a.tie_to(z, "companies.name", 70u32);
            assert_eq!(slot(z), slot(x), "前提: Z は同じ場所をもう一度使い回した");
            assert!(eng_a.former_addresses_len() > 0, "前提: bridge は使い回しの所の手前 (前の宛名を覚えている)");
        }
        eng_a.oplog_sync().unwrap();
        sync_a.publish_since(Hlc::ZERO);
        drop(hold);
        for _ in 0..3 {
            sync_c.pull_once(1);
        }
        let ws = eng_c.pull_raw("users.uid", 7u32);
        assert_eq!(ws.len(), 1, "前提: C に W が届いた");
        let c_val = eng_c.get(ws[0], "users.company");
        assert_ne!(c_val, Some(u64::from(eid_local(f1))), "C の W が F1 を指す (A では消えた行 X を指す、 again = {again}、 pruned = {pruned})");
    }
}

/// [E] の逆の形: A が C の行 F0 を受けて、 C の削除で消し、 その場所を A の行 Y が使い回した後に、 アプリが F0 の古い eid を的に ref
/// を張る。 C の W は F0 (C の消えた行) を指し、 幻の行 (A の世界番号の空の行) を作らない。
///
/// 前の宛名の覚え (`FormerAddresses`) がある間 (bridge が使い回しの所の後ろの record を読む前) だけ。 読んだ後 / 開き直した後に張った
/// ref は、 前と同じく A の世界番号で出る (前の住人が他の peer の行だったと分かる物が残っていない)。 試験は consumer を止めて覚えのある
/// 間に張る。
///
/// `caught_up`: bridge が WAL の頭に追いついた所 (書き込みの無い間の常の形) で使い回す = 覚えの位置 (使い回す時の WAL の頭) が bridge の
/// 位置と同じ。 偽は bridge が使い回しの所の手前 (使い回しの前の A の書き込みを読んでいない)。 #952: 前は bridge の入口が、 位置が
/// bridge の位置と同じ覚えを外し (使い回しの後ろの record を 1 つも読んでいないのに)、 ref が A の世界番号で出た。 この試験の前の形は
/// consumer を止める前に consumer の最初の周期が来たか (負荷の下で来る) で前提が決まり、 並べると毎回落ちた。
///
/// falsify: bridge の Ref の値の世代の照合を外すと、 C に A の世界番号の行が現れる (W の的が F0 でない)。 覚えを外す境目を 「bridge の
/// 位置 >= 覚えの位置」 に戻すと `caught_up` で同じ。
#[test]
fn a_ref_to_a_gone_translated_row_does_not_make_a_phantom_row() {
    for caught_up in [false, true] {
        let (pa, pc) = (tmp_path("ra"), tmp_path("rc"));
        let _c = Cleanup(vec![pa.clone(), pc.clone()]);
        let transport = transport(&[1, 3]);
        let eng_a = make_refs(&pa, 1, 1);
        let eng_c = make_refs(&pc, 3, 100);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        let sync_c = Syncer::new(eng_c.clone(), transport.clone());
        let f0 = eng_c.entity_in("companies").unwrap();
        eng_c.tie_to(f0, "companies.name", 40u32);
        eng_c.oplog_sync().unwrap();
        sync_c.publish_since(Hlc::ZERO);
        sync_a.pull_once(3);
        let on_a = eng_a.pull_raw("companies.name", 40u32);
        assert_eq!(on_a.len(), 1, "前提: A に F0 がある");
        let f0_on_a = on_a[0];
        eng_c.delete(f0);
        eng_c.oplog_sync().unwrap();
        sync_c.publish_since(Hlc::ZERO);
        sync_a.pull_once(3);
        assert!(eng_a.pull_raw("companies.name", 40u32).is_empty(), "前提: A で F0 が消えた");
        let hold = eng_a.hold_consumer();
        // bridge の位置は状態で決める (consumer の周期が止める前に来たかによらない): W を使い回しの前に書き、 読ませる / 読ませない
        let w = eng_a.entity_in("users").unwrap();
        eng_a.tie_to(w, "users.uid", 7u32);
        if caught_up {
            eng_a.oplog_sync().unwrap();
            assert_eq!(eng_a.sync_ops_bridge_offset(), eng_a.stats().oplog_head, "前提: bridge は WAL の頭に追いついている");
        } else {
            assert!(eng_a.sync_ops_bridge_offset() < eng_a.stats().oplog_head, "前提: bridge は使い回しの所の手前 (W を読んでいない)");
        }
        let y = eng_a.entity_in("companies").unwrap();
        eng_a.tie_to(y, "companies.name", 60u32);
        assert_eq!(slot(y), slot(f0_on_a), "前提: Y は F0 の場所を使い回した");
        assert_ne!(y, f0_on_a, "前提: 使い回した場所の世代は F0 より新しい");

        eng_a.tie_ref_to(w, "users.company", f0_on_a);
        assert!(eng_a.former_addresses_len() > 0, "前提: 前の宛名を覚えている");
        eng_a.oplog_sync().unwrap();
        sync_a.publish_since(Hlc::ZERO);
        drop(hold);
        for _ in 0..3 {
            sync_c.pull_once(1);
        }
        let ws = eng_c.pull_raw("users.uid", 7u32);
        assert_eq!(ws.len(), 1, "前提: C に W が届いた");
        assert_eq!(eng_c.pull_raw("companies.name", 60u32).len(), 1, "前提: C に Y が届いた");
        let c_val = eng_c.get(ws[0], "users.company");
        assert_eq!(c_val, Some(u64::from(eid_local(f0))), "C の W が F0 (C の消えた行) を指さない (caught_up = {caught_up})");
    }
}

/// 0.38 前 review の B1: 上の試験の、 前の宛名の覚え (`FormerAddresses`) が無くなった後に ref を張る形。 `prev.seg` (#946) の使い回しの
/// 記録が F0 (C の行) を前の住人として持つので、 bridge は Ref の値の宛名をそこから引き、 C の W は F0 (C の消えた行) を指す (state の
/// #924 と同じ答え)。 幻の行 (A の世界番号の空の行) を作らない。
///
/// - `pruned`: bridge が使い回しの所の後ろを読んで覚えを外した後 (#952 の境目)
/// - `reopened`: A を閉じて開き直した後 (覚えは memory だけ)
///
/// 前は覚えが無いと Ref の値の宛名を引けず、 素の Tie (A の世界番号) で配った (行の宛名 `bridge_row_address` は記録を引くのに、 値の宛名
/// `bridge_address` は引かなかった)。
///
/// falsify: `bridge_address` の、 覚えの無い時に `prev.seg` の記録を引く所を外すと、 C の W が F0 を指さない (A の世界番号の行を指す)。
#[test]
fn a_ref_to_a_gone_translated_row_without_the_former_address_does_not_make_a_phantom_row() {
    let mut wrong = Vec::new();
    for reopened in [false, true] {
        let (pa, pc) = (tmp_path("b1a"), tmp_path("b1c"));
        let _c = Cleanup(vec![pa.clone(), pc.clone()]);
        let transport = transport(&[1, 3]);
        let mut eng_a = make_refs(&pa, 1, 1);
        let eng_c = make_refs(&pc, 3, 100);
        let mut sync_a = Syncer::new(eng_a.clone(), transport.clone());
        let sync_c = Syncer::new(eng_c.clone(), transport.clone());
        let f0 = eng_c.entity_in("companies").unwrap();
        eng_c.tie_to(f0, "companies.name", 40u32);
        eng_c.oplog_sync().unwrap();
        sync_c.publish_since(Hlc::ZERO);
        sync_a.pull_once(3);
        let on_a = eng_a.pull_raw("companies.name", 40u32);
        assert_eq!(on_a.len(), 1, "前提: A に F0 がある");
        let f0_on_a = on_a[0];
        eng_c.delete(f0);
        eng_c.oplog_sync().unwrap();
        sync_c.publish_since(Hlc::ZERO);
        sync_a.pull_once(3);
        assert!(eng_a.pull_raw("companies.name", 40u32).is_empty(), "前提: A で F0 が消えた");
        let w = eng_a.entity_in("users").unwrap();
        eng_a.tie_to(w, "users.uid", 7u32);
        let y = eng_a.entity_in("companies").unwrap();
        eng_a.tie_to(y, "companies.name", 60u32);
        assert_eq!(slot(y), slot(f0_on_a), "前提: Y は F0 の場所を使い回した");
        assert_ne!(y, f0_on_a, "前提: 使い回した場所の世代は F0 より新しい");
        if reopened {
            drop(sync_a);
            drop(eng_a);
            eng_a = Engine::open_concurrent_with_oplog(&pa, WAL).unwrap();
            eng_a.set_peer_id(1);
            sync_a = Syncer::new(eng_a.clone(), transport.clone());
        } else {
            eng_a.oplog_sync().unwrap();
            eng_a.oplog_sync().unwrap();
        }
        assert_eq!(eng_a.former_addresses_len(), 0, "前提: 前の宛名の覚えが無い (reopened = {reopened})");
        eng_a.tie_ref_to(w, "users.company", f0_on_a);
        eng_a.oplog_sync().unwrap();
        sync_a.publish_since(Hlc::ZERO);
        for _ in 0..3 {
            sync_c.pull_once(1);
        }
        let ws = eng_c.pull_raw("users.uid", 7u32);
        assert_eq!(ws.len(), 1, "前提: C に W が届いた");
        assert_eq!(eng_c.pull_raw("companies.name", 60u32).len(), 1, "前提: C に Y が届いた");
        let c_val = eng_c.get(ws[0], "users.company");
        // 両方の形を見てから落とす
        if c_val != Some(u64::from(eid_local(f0))) {
            wrong.push((reopened, c_val));
        }
    }
    assert!(wrong.is_empty(), "C の W が F0 (C の消えた行) を指さない = A の世界番号の幻の行を指す ((reopened, C の W の値)): {wrong:?}");
}

/// 読み直し A2: A が受けた C の行 F0 に書き (bridge の前)、 C が F0 を消して F1 を作り、 A が受けて F1 が F0 の場所を使い回した所で落ちた
/// 像 (page cache は残る = 写し) を開く。 前の宛名の覚えはメモリだけなので無い。 A の F0 への書き込みは、 宛名が分からない (場所の最後の
/// 行でない) ので配らない。 自分の世界番号で配ると、 C で A の行と読まれて幻の行 (F0 に書いた値の行) ができた。
///
/// 受けるのは `apply_records` で (pull の中継は bridge を回すので、 書き込みが落ちる前に配られる形を避ける)。
///
/// falsify: bridge の 「宛名が分からない書き込みは配らない」 を外すと、 C に値 5 の行が現れる。
#[test]
fn a_write_to_a_gone_translated_row_is_not_sent_under_our_number_after_a_crash() {
    let (pa, pc) = (tmp_path("a2a"), tmp_path("a2c"));
    let image = format!("{pa}-image");
    let _c = Cleanup(vec![pa.clone(), pc.clone(), image.clone()]);
    let transport = transport(&[1, 3]);
    let eng_c = make_notes(&pc, 3, 100);
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    let f1 = {
        let eng_a = make_notes(&pa, 1, 1);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        let f0 = eng_c.entity_in("notes").unwrap();
        eng_c.tie_to(f0, "notes.note", 40u32);
        eng_c.oplog_sync().unwrap();
        sync_c.publish_since(Hlc::ZERO);
        sync_a.apply_records(&transport.pull_as(1, 3, Hlc::ZERO));
        let on_a = eng_a.pull_raw("notes.note", 40u32);
        assert_eq!(on_a.len(), 1, "前提: A に F0 がある");
        let f0_on_a = on_a[0];
        eng_a.persist_sync_state().unwrap();
        eng_a.oplog_sync().unwrap();
        let hold = eng_a.hold_consumer();
        let checkpoint = eng_a.stats().oplog_checkpoint;
        eng_a.tie_to(f0_on_a, "notes.note", 5u32);
        // C が F0 を消して F1 を作る。 A が受けて、 F1 が F0 の場所を使い回す
        eng_c.delete(f0);
        let f1 = eng_c.entity_in("notes").unwrap();
        eng_c.tie_to(f1, "notes.note", 50u32);
        eng_c.oplog_sync().unwrap();
        sync_c.publish_since(Hlc::ZERO);
        sync_a.apply_records(&transport.pull_as(1, 3, Hlc::ZERO));
        assert!(eng_a.pull_raw("notes.note", 5u32).is_empty(), "前提: A で F0 が消えた");
        let on_a = eng_a.pull_raw("notes.note", 50u32);
        assert_eq!(on_a.len(), 1, "前提: A に F1 がある");
        assert_eq!(slot(on_a[0]), slot(f0_on_a), "前提: F1 は F0 の場所を使い回した");
        assert_ne!(on_a[0], f0_on_a, "前提: 使い回した場所の世代は F0 より新しい");
        assert_eq!(eng_a.stats().oplog_checkpoint, checkpoint, "前提: A の書き込みは checkpoint の後ろ (bridge の前)");
        eng_a.persist_sync_state().unwrap();
        copy_dir(std::path::Path::new(&pa), std::path::Path::new(&image));
        drop(hold);
        drop(sync_a);
        f1
    };
    let eng_a = Engine::open_concurrent_with_oplog(&image, WAL).unwrap();
    eng_a.set_peer_id(1);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    eng_a.oplog_sync().unwrap();
    sync_a.publish_since(Hlc::ZERO);
    for _ in 0..3 {
        sync_c.pull_once(1);
    }
    assert_eq!(eng_c.get(f1, "notes.note"), Some(50), "前提: C の F1 は変わらない");
    assert!(eng_c.pull_raw("notes.note", 5u32).is_empty(), "C に、 A が消えた受けた行 F0 に書いた値の幻の行ができた");
}
