//! #731 / #863: eid の local = 「世代 | 場所」 (EID_REUSE の段 1、 engine の入口と出口)。
//!
//! 消えた行の場所 (slot) を使い回した行は、 世代の違う eid になる。 古い eid (アプリが持ち続けた u64) は入口の照合で 「消えた行」:
//! 読みは None、 書き込み / 消すは何もしない。 世代を上げるのは、 一度も使い回していない DB (v14) では使い回す時、 最初の使い回しで
//! file version 15 に移った後は消す時 (払い出し位置が消した場所の手前に戻っても、 消した場所は次の世代で出る)。
//!
//! 前提は状態で確かめる: 使い回した事は場所 (local の下 24 bit) と表の枠で、 version は header.seg の version の欄で。

use enchudb_engine::engine::Engine;
use enchudb_engine::ValueType;
use std::sync::Arc;

const WAL: usize = 4 * 1024 * 1024;
/// 既定 / 小さい DB (max_entities ≤ 2^24) の場所の bit。
const SLOT_MASK: u32 = (1 << 24) - 1;

fn slot(eid: u64) -> u32 {
    (eid as u32) & SLOT_MASK
}

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("enchudb-eidgen-{tag}-{}-{seq}-{nanos}", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
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

/// header.seg の file version の欄 (offset 4、 u32 LE)。
fn file_version(db: &str) -> u32 {
    let h = std::fs::read(std::path::Path::new(db).join("header.seg")).unwrap();
    u32::from_le_bytes(h[4..8].try_into().unwrap())
}

fn has_gen_segment(db: &str) -> bool {
    std::path::Path::new(db).join("gen.seg").is_file()
}

/// 表 `notes` (枠 `cap`、 列 `note`) と、 使い回しを起こすための表 `full` (枠 1) を持つ DB。
fn create_standalone(path: &str, cap: u32) -> Engine {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", cap).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_table("full", 1).unwrap();
    eng.define_himo_in("full", "x", ValueType::Number, 0).unwrap();
    eng
}

fn create_concurrent(path: &str, sync: bool, cap: u32) -> Arc<Engine> {
    let mut eng = create_standalone(path, cap);
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(1);
    eng
}

/// 表 `full` (枠 1) で 1 行足して消して足す = 最初の使い回し (v15 に移る)。
fn reuse_once(eng: &Engine) {
    let a = eng.entity_in("full").unwrap();
    eng.tie_to(a, "full.x", 1u32);
    eng.delete(a);
    let b = eng.entity_in("full").unwrap();
    assert_eq!(slot(a), slot(b), "前提: 枠 1 の表は消えた場所を使い回す");
    assert_ne!(a, b, "使い回した場所に同じ eid を渡した");
    eng.tie_to(b, "full.x", 2u32);
}

#[test]
fn a_stale_eid_neither_reads_nor_writes_the_row_that_reused_its_slot() {
    let path = tmp_path("stale");
    let _c = Cleanup(path.clone());
    let eng = create_standalone(&path, 4);
    let rows: Vec<u64> = (0..4u32)
        .map(|i| {
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
            e
        })
        .collect();
    let old = rows[2];
    eng.delete(old);
    let new = eng.entity_in("notes").unwrap();
    eng.tie_to(new, "notes.note", 100u32);
    assert_eq!(slot(new), slot(old), "前提: 満杯の表は消えた場所を使い回す");
    assert_eq!(eng.table_eid_usage("notes").unwrap().capacity, 4, "前提: 枠は足していない");
    assert_ne!(new, old);
    assert!(!eng.is_live(old));
    assert!(eng.is_live(new));
    assert_eq!(eng.get(old, "notes.note"), None, "古い eid の読みが使い回した行を読んだ");
    eng.tie_to(old, "notes.note", 7u32);
    let hid = eng.himo_id("notes.note").unwrap() as u16;
    assert!(eng.try_tie_to_by_id(old, hid, 8u32).is_err(), "古い eid への書き込みを通した");
    eng.untie(old, "notes.note");
    eng.delete(old);
    assert_eq!(eng.get(new, "notes.note"), Some(100), "古い eid への書き込み / untie / delete が使い回した行に当たった");
    assert!(eng.is_live(new));
    assert_eq!(eng.pull_raw("notes.note", 100u32), vec![new], "出口 (pull_raw) は今の世代の eid");
    assert!(eng.pull_raw("notes.note", 7u32).is_empty() && eng.pull_raw("notes.note", 8u32).is_empty());
}

#[test]
fn an_anonymous_entity_reused_from_the_free_stack_gets_a_new_eid() {
    let path = tmp_path("anon");
    let _c = Cleanup(path.clone());
    let mut eng = Engine::create_with_capacity(&path, 4).unwrap();
    eng.define_himo("v", ValueType::Number, 0);
    let rows: Vec<u64> = (0..4u32)
        .map(|i| {
            let e = eng.entity().unwrap();
            eng.tie(e, "v", i);
            e
        })
        .collect();
    eng.delete(rows[1]);
    let new = eng.entity().unwrap();
    assert_eq!(slot(new), slot(rows[1]), "前提: 満杯の DB の entity() は消えた場所を使い回す");
    assert_ne!(new, rows[1]);
    eng.tie(new, "v", 100u32);
    eng.tie(rows[1], "v", 7u32);
    assert_eq!(eng.get(new, "v"), Some(100));
    assert_eq!(eng.get(rows[1], "v"), None);
}

/// 消しただけ (使い回しなし) の DB は v14 以下のまま (旧 binary で開ける)。 最初の使い回しで v15 になり gen.seg ができる。
#[test]
fn the_file_moves_to_v15_only_when_a_slot_is_reused() {
    let path = tmp_path("v15");
    let _c = Cleanup(path.clone());
    {
        let mut eng = create_standalone(&path, 4);
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        eng.delete(a);
        let b = eng.entity_in("notes").unwrap();
        assert_ne!(slot(a), slot(b), "前提: 枠の残る表は使い回さない");
        eng.flush().unwrap();
    }
    assert!(file_version(&path) < 15, "使い回していない DB を v15 にした ({})", file_version(&path));
    assert!(!has_gen_segment(&path), "使い回していない DB に gen.seg を作った");
    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        reuse_once(&eng);
        eng.flush().unwrap();
    }
    assert_eq!(file_version(&path), 15);
    assert!(has_gen_segment(&path));
    // 開き直しても世代は残る
    let eng = Engine::open_standalone(&path).unwrap();
    let rows = eng.pull_raw("full.x", 2u32);
    assert_eq!(rows.len(), 1);
    assert_ne!(rows[0] as u32, slot(rows[0]), "開き直した後の出口が世代を付けない");
    assert!(eng.pull_raw("full.x", 1u32).is_empty());
}

/// v14 の間に消した場所も、 v15 に移った後に使い回すと新しい世代になる (移る時の穴埋め)。
#[test]
fn a_slot_freed_before_the_move_to_v15_gets_a_new_generation() {
    let path = tmp_path("before");
    let _c = Cleanup(path.clone());
    let eng = create_standalone(&path, 2);
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1u32);
    let b = eng.entity_in("notes").unwrap();
    eng.tie_to(b, "notes.note", 2u32);
    eng.delete(a); // v14 の間に消す (世代はまだ無い)
    reuse_once(&eng); // 別の表で最初の使い回し → v15
    let c = eng.entity_in("notes").unwrap(); // notes は満杯 → a の場所を使い回す
    assert_eq!(slot(c), slot(a), "前提: 消えた場所を使い回す");
    assert_ne!(c, a, "v14 の間に消した場所を、 v15 で同じ eid のまま渡した");
    eng.tie_to(c, "notes.note", 3u32);
    assert_eq!(eng.get(a, "notes.note"), None);
}

/// `tables` の sidecar を書けなくする / 戻す (`tables` は tmp を作って rename する。 tmp の置き場に directory を置くと、 他の書き出し =
/// WAL / 本体は書けるまま `tables` だけ書けない。 #870 の試験と同じ作り方)。
fn block_tables(db: &str, on: bool) {
    let tmp = std::path::Path::new(db).join("tables.tmp");
    if on { std::fs::create_dir(&tmp).unwrap() } else { std::fs::remove_dir(&tmp).unwrap() }
}

/// `tables` の sidecar の、 表 `table` の `next_local` (表ごとの並びは 名前の長さ (u32) / 名前 / eid の範囲 (u32 × 2) / `next_local` (u32))。
fn sidecar_next_local(db: &str, table: &str) -> u32 {
    let bytes = std::fs::read(std::path::Path::new(db).join("tables")).unwrap();
    let mut name = (table.len() as u32).to_le_bytes().to_vec();
    name.extend_from_slice(table.as_bytes());
    let at = bytes.windows(name.len()).position(|w| w == name.as_slice()).expect("sidecar に表がある") + name.len() + 8;
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

/// `tables` を書けない間に行を足して消し、 閉じた像 (`tables` の sidecar は消した行を足す前のまま = 開いた払い出し位置が消した場所の
/// 手前に戻りうる) を開いても、 消した行の eid をもう一度渡さない。 v15 は世代で、 v14 は #870 (消した行が sidecar より先にある間は
/// checkpoint を進めない = WAL の再生が払い出し位置を戻す) で。
fn rewind_case(sync: bool, v15: bool) {
    let made = tmp_path(&format!("rewind-{sync}-{v15}"));
    let image = format!("{made}-image");
    let (_c1, _c2) = (Cleanup(made.clone()), Cleanup(image.clone()));
    let (a, x) = {
        let eng = create_concurrent(&made, sync, 100);
        if v15 {
            reuse_once(&eng);
        }
        // 足して消すまで consumer を止める (間に `tables` を書かれると、 消した行が sidecar より先にならない)
        let hold = eng.hold_consumer();
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        block_tables(&made, true);
        let x = eng.entity_in("notes").unwrap();
        eng.tie_to(x, "notes.note", 2u32);
        eng.delete(x);
        eng.oplog_sync().unwrap();
        drop(hold);
        (a, x)
    }; // `tables` を書けないまま閉じる
    assert_eq!(file_version(&made) == 15, v15, "前提: version");
    assert!(sidecar_next_local(&made, "notes") <= 1, "前提: sidecar は消した行を足す前のまま (払い出し位置が消した場所の手前)");
    copy_dir(std::path::Path::new(&made), std::path::Path::new(&image));
    block_tables(&made, false);
    let _ = std::fs::remove_dir(std::path::Path::new(&image).join("tables.tmp"));
    let eng = Engine::open_concurrent_with_oplog(&image, WAL).unwrap();
    eng.set_peer_id(1);
    assert_eq!(eng.get(a, "notes.note"), Some(1), "前提: 残した行は読める");
    let y = eng.entity_in("notes").unwrap();
    assert_ne!(y, x, "消した行の eid を、 落ちた後にもう一度払い出した (sync = {sync}、 v15 = {v15})");
    assert_ne!(y, a);
}

#[test]
fn a_rewound_allocation_does_not_repeat_a_deleted_eid_v15_plain() {
    rewind_case(false, true);
}

#[test]
fn a_rewound_allocation_does_not_repeat_a_deleted_eid_v15_sync() {
    rewind_case(true, true);
}

#[test]
fn a_rewound_allocation_does_not_repeat_a_deleted_eid_v14_sync() {
    rewind_case(true, false);
}

#[test]
fn a_rewound_allocation_does_not_repeat_a_deleted_eid_v14_plain() {
    rewind_case(false, false);
}

/// WAL 付きで、 使い回しの後に落ちた像 (本体の書き出しの前) を開いても、 世代は WAL の record から戻る。
#[test]
fn generations_come_back_from_the_wal_after_a_crash() {
    let made = tmp_path("crash");
    let image = format!("{made}-image");
    let (_c1, _c2) = (Cleanup(made.clone()), Cleanup(image.clone()));
    let (old, new) = {
        let eng = create_concurrent(&made, false, 2);
        let hold = eng.hold_consumer();
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        let b = eng.entity_in("notes").unwrap();
        eng.tie_to(b, "notes.note", 2u32);
        eng.delete(a);
        let c = eng.entity_in("notes").unwrap();
        assert_eq!(slot(c), slot(a), "前提: 使い回す");
        eng.tie_to(c, "notes.note", 3u32);
        eng.oplog_sync().unwrap();
        copy_dir(std::path::Path::new(&made), std::path::Path::new(&image));
        drop(hold);
        (a, c)
    };
    let eng = Engine::open_concurrent_with_oplog(&image, WAL).unwrap();
    assert_eq!(eng.get(new, "notes.note"), Some(3), "落ちた後に使い回した行の eid が読めない");
    assert_eq!(eng.get(old, "notes.note"), None);
    assert_eq!(eng.pull_raw("notes.note", 3u32), vec![new]);
    // 落ちた後に同じ場所をもう一度使い回しても、 前の 2 つの eid と違う
    eng.delete(new);
    let d = eng.entity_in("notes").unwrap();
    assert_eq!(slot(d), slot(new));
    assert!(d != new && d != old);
}

/// packed (`pack_dir` → `unpack_to_dir`) にしても世代は残る。
#[test]
fn packed_keeps_generations() {
    let path = tmp_path("packed");
    let packed = format!("{path}.packed");
    let out = format!("{path}-out");
    let (_c1, _c2, _c3) = (Cleanup(path.clone()), Cleanup(out.clone()), Cleanup(packed.clone()));
    let (old, new) = {
        let mut eng = create_standalone(&path, 1);
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        eng.delete(a);
        let b = eng.entity_in("notes").unwrap();
        eng.tie_to(b, "notes.note", 2u32);
        assert_eq!(slot(a), slot(b));
        eng.flush().unwrap();
        (a, b)
    };
    let _ = std::fs::remove_file(&packed);
    Engine::pack_dir(&path, std::path::Path::new(&packed)).unwrap();
    Engine::unpack_to_dir(std::path::Path::new(&packed), &out).unwrap();
    let eng = Engine::open_standalone(&out).unwrap();
    assert_eq!(eng.get(new, "notes.note"), Some(2));
    assert_eq!(eng.get(old, "notes.note"), None);
    assert_eq!(eng.pull_raw("notes.note", 2u32), vec![new]);
    let mem = Engine::from_bytes(std::fs::read(&packed).unwrap()).unwrap();
    assert_eq!(mem.get(new, "notes.note"), Some(2));
    assert_eq!(mem.get(old, "notes.note"), None);
}

/// 購読: poll の前に行を消して場所を使い回すと、 removed は渡した eid、 added は新しい eid。
#[test]
fn a_subscription_reports_the_old_eid_as_removed_and_the_new_one_as_added() {
    let path = tmp_path("live");
    let _c = Cleanup(path.clone());
    let eng = create_concurrent(&path, false, 2);
    let hid = eng.himo_id("notes.note").unwrap() as u16;
    let q = eng.subscribe(vec![enchudb_engine::live::LivePred::Present { himo_id: hid }]).unwrap();
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1u32);
    let b = eng.entity_in("notes").unwrap();
    eng.tie_to(b, "notes.note", 2u32);
    let d = q.poll(&eng);
    assert_eq!(d.added.len(), 2);
    assert!(d.added.contains(&a) && d.added.contains(&b));
    eng.delete(a);
    let c = eng.entity_in("notes").unwrap();
    eng.tie_to(c, "notes.note", 3u32);
    assert_eq!(slot(c), slot(a), "前提: poll の前に場所を使い回した");
    assert_ne!(c, a, "使い回した場所に同じ eid を渡した");
    let d = q.poll(&eng);
    assert_eq!(d.removed, vec![a], "removed がアプリに渡した eid でない");
    assert_eq!(d.added, vec![c], "added が今の世代の eid でない");
}
