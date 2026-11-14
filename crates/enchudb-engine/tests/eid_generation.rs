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

/// 世代を持つ前 (v14) は、 消えた行の古い eid への書き込みを断れない (死んだ場所に cell が残る)。 その場所を使い回した行には、
/// その cell が残らない (使い回す時に落とす)。
#[test]
fn a_cell_written_through_a_dead_eid_before_v15_is_not_inherited_by_the_row_that_reuses_the_slot() {
    let path = tmp_path("leftover");
    let _c = Cleanup(path.clone());
    let eng = create_standalone(&path, 2);
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1u32);
    let b = eng.entity_in("notes").unwrap();
    eng.tie_to(b, "notes.note", 2u32);
    eng.delete(b);
    eng.tie_to(b, "notes.note", 99u32);
    assert_eq!(eng.slot_bits(), 0, "前提: まだ世代を持たない");
    assert_eq!(eng.get(b, "notes.note"), Some(99), "前提: 消えた行の eid への書き込みが死んだ場所に残った");
    let c = eng.entity_in("notes").unwrap();
    assert_eq!(slot(c), slot(b), "前提: 消えた場所を使い回した");
    assert_ne!(c, b);
    assert_eq!(eng.get(c, "notes.note"), None, "消えた行の eid で書いた cell が、 場所を使い回した行に残った");
    assert!(eng.pull_raw("notes.note", 99u32).is_empty(), "消えた行の eid で書いた cell が引ける");
    eng.tie_to(c, "notes.note", 3u32);
    assert_eq!(eng.get(c, "notes.note"), Some(3));
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

/// gen.seg の header (表の印 64.. / page の印) と、 場所 `slot` の世代の byte (`Some` の時) を消す。
fn wipe_generation_marks(db: &str, slot: Option<u32>) {
    let p = std::path::Path::new(db).join("gen.seg");
    let mut b = std::fs::read(&p).unwrap();
    b[64..16 * 1024].fill(0);
    if let Some(s) = slot {
        b[16 * 1024 + s as usize] = 0;
    }
    std::fs::write(&p, b).unwrap();
}

/// WAL 付きで、 使い回しの後に落ちた像 (本体の書き出しの前) を開いても、 世代は WAL の record から戻る。
///
/// 像は開いたままの DB の写し (page cache が見える) なので、 gen.seg の世代も印も届いて見える。 「WAL から戻る」 を確かめるため、
/// 像の gen.seg から使い回した場所の世代と印を消す (gen.seg が書き出される前に落ちた像)。 checkpoint は進めない (consumer を止め、
/// `oplog_sync` も呼ばない = record は再生される)。
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
        eng.oplog_commit();
        copy_dir(std::path::Path::new(&made), std::path::Path::new(&image));
        drop(hold);
        (a, c)
    };
    wipe_generation_marks(&image, Some(slot(new)));
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

/// 読み直し A: packed を開いた Memory backing (`from_bytes`、 wasm と同じ) の v14 の DB は v15 に移れない。 満杯の表で消して足すと、
/// 前は使い回しの道が移る手順の Err をそのまま返し、 空きの一覧が残るので以後の insert も全部断った (anonymous の `entity()` も)。
/// 移れない DB は今まで通り世代なしで使い回す (master と同じ)。
#[test]
fn a_packed_v14_db_in_memory_keeps_reusing_slots() {
    let path = tmp_path("memreuse");
    let packed = format!("{path}.packed");
    let (_c1, _c2) = (Cleanup(path.clone()), Cleanup(packed.clone()));
    {
        let mut e = Engine::create_with_capacity(&path, 2).unwrap();
        e.define_himo("x", ValueType::Number, 0);
        let x = e.entity().unwrap();
        e.tie(x, "x", 1u32);
        let y = e.entity().unwrap();
        e.tie(y, "x", 2u32);
        e.flush().unwrap();
    }
    assert!(file_version(&path) < 15, "前提: 使い回していない DB は v15 より前");
    let _ = std::fs::remove_file(&packed);
    Engine::pack_dir(&path, std::path::Path::new(&packed)).unwrap();
    let mem = Engine::from_bytes(std::fs::read(&packed).unwrap()).unwrap();
    assert_eq!(mem.slot_bits(), 0, "前提: 世代を持たない DB");
    for round in 0..3u32 {
        let gone = mem.entities()[0];
        mem.delete(gone);
        let again = mem.entity().unwrap_or_else(|e| panic!("round {round}: 使い回しの entity() を断った: {e}"));
        assert_eq!(slot(again), slot(gone), "前提: 空きの山から使い回す (max_entities 2)");
        mem.tie_to(again, "x", 10 + round);
        assert_eq!(mem.get(again, "x"), Some(10 + round as u64));
    }
    // 名前のある表 (Memory backing の DB に後から定義した表) の満杯の使い回しも
    let path2 = format!("{path}-named");
    let packed2 = format!("{path}-named.packed");
    let (_c3, _c4) = (Cleanup(path2.clone()), Cleanup(packed2.clone()));
    {
        let mut e = Engine::create_with_capacity(&path2, 64).unwrap();
        e.flush().unwrap();
    }
    Engine::pack_dir(&path2, std::path::Path::new(&packed2)).unwrap();
    let mut mem = Engine::from_bytes(std::fs::read(&packed2).unwrap()).unwrap();
    mem.define_table("notes", 2).unwrap();
    mem.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    let mut rows: Vec<u64> = (0..2u32)
        .map(|i| {
            let e = mem.entity_in("notes").unwrap();
            mem.tie_to(e, "notes.note", i);
            e
        })
        .collect();
    assert_eq!(mem.table_eid_usage("notes").unwrap().free, 0, "前提: 表は満杯");
    for round in 0..3u32 {
        let gone = rows.remove(0);
        mem.delete(gone);
        let c = mem.entity_in("notes").unwrap_or_else(|e| panic!("round {round}: 使い回しの insert を断った: {e}"));
        assert_eq!(slot(c), slot(gone), "前提: 消えた場所を使い回す");
        mem.tie_to(c, "notes.note", 10 + round);
        rows.push(c);
    }
}

/// sync の表 (`notes` 枠 `cap`、 Number の `note` / Leaf の `body`、 Ref の `owner` → `owners` 枠 1) を持つ、 peer 1 の DB。
fn create_sync_with_leaf(path: &str, cap: u32) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("owners", 1).unwrap();
    eng.define_himo_in("owners", "name", ValueType::Number, 0).unwrap();
    eng.define_table("notes", cap).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.define_ref_in("notes", "owner", "owners").unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(1);
    eng
}

fn hlc(wall: u64, peer: u32) -> enchudb_oplog::Hlc {
    enchudb_oplog::Hlc { wall, logical: 0, peer }
}

/// 読み直し B1: 翻訳した行 (他の peer の行) が使い回した場所に載ると、 その場所の世代は 1 以上。 中の道 (名前で畳む #178 / #612、
/// 改名の切り離し) が世代 0 の eid でアプリの入口 (`get_text_owned`、 照合あり) を呼んで、 Leaf の列を 「無い」 と読んで移さなかった。
#[test]
fn folding_a_translated_row_on_a_reused_slot_moves_its_leaf_cells() {
    let path = tmp_path("foldleaf");
    let _c = Cleanup(path.clone());
    let eng = create_sync_with_leaf(&path, 2);
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1u32);
    let keep = eng.entity_in("notes").unwrap();
    eng.tie_to(keep, "notes.note", 2u32);
    eng.delete(a);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let body = eng.himo_id("notes.body").unwrap() as u16;
    let t = eng.resolve_remote_eid(enchudb_oplog::make_eid(2, 9), note).unwrap();
    assert_eq!(eng.eid_slot(t), slot(a), "前提: 翻訳した行は消えた場所を使い回した");
    assert_ne!(eng.slot_eid(slot(a)), a, "前提: 使い回した場所の世代は 1 以上");
    assert!(eng.remote_tieleaf_apply(t, body, b"hello", hlc(1_000, 2)).applied());
    assert_eq!(eng.get_text_owned(eng.slot_eid(slot(a)), "notes.body").as_deref(), Some(&b"hello"[..]), "前提");
    eng.merge_entity_into(t, keep).unwrap();
    assert_eq!(eng.get_text_owned(keep, "notes.body").as_deref(), Some(&b"hello"[..]), "畳んだ行の Leaf が残す行へ移らない");
}

/// 読み直し C: 相手から届いた Ref の先が、 こちらで消えた行 (相手の削除を先に当てた / 作成より先に削除が届いた) の時、 Ref の値は
/// 消えた行を指す (前の世代)。 前は場所の今の世代 = 次の住人の世代を返し、 その場所を次の行が使い回すと、 届いた Ref がその行を指した。
#[test]
fn a_remote_ref_to_a_deleted_row_does_not_point_at_the_next_row_on_its_slot() {
    let path = tmp_path("remoteref");
    let _c = Cleanup(path.clone());
    let eng = create_sync_with_leaf(&path, 4);
    reuse_once_in(&eng, "owners", "owners.name");
    let owner = eng.himo_id("notes.owner").unwrap() as u16;
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let x = enchudb_oplog::make_eid(2, 7);
    // 作成より先に削除が届く (#490: 削除の版数を退避)
    eng.remote_delete_apply_foreign(x, hlc(2_000, 2));
    // その後に、 消えた行を指す Ref が届く (Ref の先の行の場所を払い出して削除の版数を書き戻す)
    let w = eng.resolve_remote_eid(enchudb_oplog::make_eid(2, 8), note).unwrap();
    assert!(eng.remote_tie_apply_result(w, note, 5u32, hlc(1_000, 2)).applied());
    let v = eng.resolve_remote_ref_value(2, enchudb_oplog::eid_local(x), owner).unwrap();
    assert!(eng.remote_tie_apply_result(w, owner, v, hlc(1_001, 2)).applied());
    let w_app = eng.pull_raw("notes.note", 5u32)[0];
    let target = eng.ref_eid(eng.get(w_app, "notes.owner").unwrap());
    assert!(!eng.is_live(target), "前提: Ref の先は消えた行");
    // こちらで足した行が、 その場所を使い回す (owners は枠 1)
    let y = eng.entity_in("owners").unwrap();
    eng.tie_to(y, "owners.name", 42u32);
    assert_eq!(eng.eid_slot(y), eng.eid_slot(target), "前提: 消えた行の場所を使い回した");
    let target = eng.ref_eid(eng.get(w_app, "notes.owner").unwrap());
    assert_ne!(target, y, "届いた Ref が、 消えた行でなく次の住人を指した");
    assert_eq!(eng.get(target, "owners.name"), None);
}

/// 表 `table` (枠 1) で 1 行足して消して足す (最初の使い回し = v15 に移る)。 足した行は消して空ける。
fn reuse_once_in(eng: &Engine, table: &str, col: &str) {
    let a = eng.entity_in(table).unwrap();
    eng.tie_to(a, col, 1u32);
    eng.delete(a);
    let b = eng.entity_in(table).unwrap();
    assert_eq!(slot(a), slot(b), "前提: 枠 1 の表は消えた場所を使い回す");
    assert_ne!(a, b, "前提: v15");
    eng.delete(b);
}

/// 読み直し D1 / D2 (#612): 自分の行 X を消し、 その場所を Y が使い回し、 Y を同じ名前の行 Z に畳む (別名 場所 → Z)。
/// D1: 古い X の eid は、 別名があっても世代を見ずに Z へ転送されていた (読み・書き込み・消すが Z に当たる)。
/// D2: 畳む時に場所の世代を上げていた (#775 で生き返らせた行の eid が 「消えた行」 になる)。 畳んだ場所は使い回さないので上げない。
#[test]
fn an_old_eid_is_not_forwarded_by_the_fold_alias_of_the_row_that_reused_its_slot() {
    let path = tmp_path("foldalias");
    let _c = Cleanup(path.clone());
    let eng = create_sync_with_leaf(&path, 2);
    let x = eng.entity_in("notes").unwrap();
    eng.tie_to(x, "notes.note", 1u32);
    let z = eng.entity_in("notes").unwrap();
    eng.tie_to(z, "notes.note", 2u32);
    eng.delete(x);
    let y = eng.entity_in("notes").unwrap();
    eng.tie_to(y, "notes.note", 3u32);
    assert_eq!(slot(y), slot(x), "前提: 使い回した");
    assert_ne!(y, x);
    eng.merge_entity_into(y, z).unwrap();
    // D2: 畳んだ場所の世代は Y のまま (畳んだ場所は使い回さない)
    assert_eq!(eng.slot_eid(slot(y)), y, "畳む時に場所の世代を上げた");
    // #612: Y の eid は Z へ転送される
    assert!(eng.is_live(y), "畳んだ行の eid が残す行へ転送されない");
    assert_eq!(eng.get(y, "notes.note"), eng.get(z, "notes.note"));
    // D1: X の eid は消えた行 (Z に当たらない)
    assert!(!eng.is_live(x), "消えた行の古い eid が畳んだ別名で残す行へ転送された");
    assert_eq!(eng.get(x, "notes.note"), None);
    let before = eng.get(z, "notes.note");
    eng.tie_to(x, "notes.note", 99u32);
    eng.delete(x);
    assert!(eng.is_live(z), "古い eid の delete が残す行を消した");
    assert_eq!(eng.get(z, "notes.note"), before, "古い eid の書き込みが残す行に当たった");
    assert_eq!(eng.ref_value(x), enchudb_oplog::eid_local(x) as u64, "Ref の値も転送しない (消えた行を指す)");
}

/// きれいに閉じなかった後に開くと、 gen.seg の表と page の印を世代の byte から立て直す。 印と世代の byte は gen.seg の別の page なので、
/// 電源断で世代の byte だけ届いた像がありうる。 印が無いと出口 (`pull_raw` / `find`) が世代を読まず、 使い回した行に前の住人の eid を
/// 渡した。 像は開いたままの DB (WAL 付き) の写しで、 checkpoint を進めた後に WAL を外す (再生する record が無い。 再生は上げる世代で
/// 印も立てるので、 再生があると立て直しを確かめられない)。
#[test]
fn generation_marks_are_rebuilt_after_an_unclean_close() {
    let made = tmp_path("marks");
    let image = format!("{made}-image");
    let (_c1, _c2) = (Cleanup(made.clone()), Cleanup(image.clone()));
    let (old, new) = {
        let eng = create_concurrent(&made, false, 2);
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        let b = eng.entity_in("notes").unwrap();
        eng.tie_to(b, "notes.note", 2u32);
        eng.delete(a);
        let c = eng.entity_in("notes").unwrap();
        assert_eq!(slot(c), slot(a), "前提: 使い回す");
        eng.tie_to(c, "notes.note", 3u32);
        eng.oplog_sync().unwrap(); // 本体を書き出して checkpoint を進める
        let hold = eng.hold_consumer();
        copy_dir(std::path::Path::new(&made), std::path::Path::new(&image));
        drop(hold);
        (a, c)
    };
    std::fs::remove_file(std::path::Path::new(&image).join("oplog")).unwrap();
    wipe_generation_marks(&image, None);
    let eng = Engine::open_concurrent_with_oplog(&image, WAL).unwrap();
    assert_eq!(eng.pull_raw("notes.note", 3u32), vec![new], "印の無い像で、 出口が使い回した行に前の住人の eid を渡した");
    assert_eq!(eng.get(new, "notes.note"), Some(3));
    assert_eq!(eng.get(old, "notes.note"), None);
}
