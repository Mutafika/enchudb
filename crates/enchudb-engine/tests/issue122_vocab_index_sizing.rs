//! #122: vocab 索引の entry 上限が `max_entities × 16` から導出され、 entity が多く vocab
//! 値が少ないワークロード (辺が entity の大半を占めるグラフ形) で実需の 1,000 倍以上を
//! 確保していた問題。 `GrowableOptions::vocab_max_entries` で consumer が是正できること。
//!
//! #123: あわせて file version が v8 になり、 v7 DB は writer open で透過 migrate される
//! ことを固定する (旧 binary が新 slot 関数の index を旧 slot で読む silent miss の防止)。

use enchudb_engine::{Engine, GrowableOptions, ValueType};

/// header offset (engine.rs の H_* と対応。 black-box 検証なので値を直書きする)。
const H_VERSION: usize = 4;
const H_VOCAB_MAX_ENTRIES: usize = 20;
const H_VOCAB_INDEX_CAP: usize = 24;
const H_HEADER_CRC: usize = 64;
/// index slot は [flag:1][hash:8][vid:4]。
const INDEX_SLOT_SIZE: usize = 13;

fn tmp_path(tag: &str) -> String {
    format!("/tmp/issue122_{}_{}.enchu", tag, std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

/// header の 4 byte だけを seek して読む。
///
/// **`std::fs::read` を使ってはいけない** — growable DB は apparent が巨大な sparse file
/// なので、 全体をメモリへ読むと Linux では OOM killer に SIGKILL される (macOS では
/// 通ってしまい、 OrbStack の Linux 実行で初めて露見した)。
fn header_u32(path: &str, off: usize) -> u32 {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path).expect("open db");
    f.seek(SeekFrom::Start(off as u64)).expect("seek");
    let mut b = [0u8; 4];
    f.read_exact(&mut b).expect("read header word");
    u32::from_le_bytes(b)
}

/// header の 4 byte だけを seek して書く (同上の理由でファイル全体を読み書きしない)。
fn write_header_u32(path: &str, off: usize, v: u32) {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().write(true).open(path).expect("open db rw");
    f.seek(SeekFrom::Start(off as u64)).expect("seek");
    f.write_all(&v.to_le_bytes()).expect("write header word");
}

/// 明示した `vocab_max_entries` が header に焼かれ、 索引が実需サイズに縮む。
#[test]
fn explicit_vocab_max_entries_shrinks_index() {
    let path = tmp_path("explicit");
    cleanup(&path);

    // 実測ワークロード (法令コーパス) の形: entity は辺で膨らむがユニーク Tag 値は 10 万台。
    let max_entities = 4_000_000;
    let eng = Engine::create_growable_opts(
        &path,
        GrowableOptions {
            max_entities,
            vocab_max_entries: Some(200_000),
            ..Default::default()
        },
    )
    .expect("create with explicit vocab_max_entries");
    drop(eng);

    assert_eq!(header_u32(&path, H_VOCAB_MAX_ENTRIES), 200_000);
    let cap = header_u32(&path, H_VOCAB_INDEX_CAP);
    assert_eq!(cap, 262_144, "index_cap は next_power_of_two(200_000)");

    // 従来式 (max_entities × 16 = 64 M → 上限 256 M 未満なので 64 M、 cap = 2^26) との差。
    let default_cap: u32 = (max_entities.saturating_mul(16)).next_power_of_two();
    let saved = (default_cap as u64 - cap as u64) * INDEX_SLOT_SIZE as u64;
    assert!(
        saved > 800 * 1024 * 1024,
        "索引予約が縮んでいない: cap {} → {} (削減 {} bytes)",
        default_cap, cap, saved
    );

    Engine::open_readonly(&path).expect("縮めた索引の DB が open できること");
    cleanup(&path);
}

/// `None` (既定) は従来式 `max_entities × 16` のまま — 既存 consumer の挙動が変わらない。
#[test]
fn default_keeps_legacy_multiplier() {
    let path = tmp_path("default");
    cleanup(&path);

    let max_entities = 100_000;
    let eng = Engine::create_growable_opts(
        &path,
        GrowableOptions { max_entities, ..Default::default() },
    )
    .expect("create with default");
    drop(eng);

    assert_eq!(
        header_u32(&path, H_VOCAB_MAX_ENTRIES),
        max_entities * 16,
        "既定式 (max_entities × 16) が変わっている"
    );
    cleanup(&path);
}

/// #123: 新規 DB は現行 version。 v7 DB は writer open で現行へ上がる
/// (= 旧 binary が開けなくなる)。
///
/// request17 (v9) で現行は 9。 **version stamp が上がっても v9 領域は生えない**
/// (per-cell version の有無は別 header flag `H_CELL_VERSION` が持つ) ので、
/// 既存 DB の layout は 1 byte も変わらない — その回帰は engine 側の
/// `pre_v9_db_opens_and_behaves_as_before` が見ている。
const CURRENT_FILE_VERSION: u32 = 9;

#[test]
fn v7_db_upgrades_to_current_version_on_writer_open() {
    let path = tmp_path("v7upgrade");
    cleanup(&path);

    {
        let mut eng =
            Engine::create_growable_opts(&path, GrowableOptions::default()).expect("create");
        eng.define_himo("k", ValueType::Tag, 1024);
        drop(eng);
    }
    assert_eq!(
        header_u32(&path, H_VERSION),
        CURRENT_FILE_VERSION,
        "新規 create が現行 version でない",
    );

    // v7 を偽造する。 header CRC == 0 は「v27 以前の DB」として検証を通る経路なので、
    // version を 7 に戻して CRC を 0 にすれば v7 DB として open される。
    write_header_u32(&path, H_VERSION, 7);
    write_header_u32(&path, H_HEADER_CRC, 0);
    assert_eq!(header_u32(&path, H_VERSION), 7, "偽造に失敗");

    // readonly open は据え置き (共有 mmap を書かない)。
    {
        let ro = Engine::open_readonly(&path).expect("v7 は legacy として open できること");
        drop(ro);
    }
    assert_eq!(header_u32(&path, H_VERSION), 7, "readonly open が header を書き換えた");

    // writer open で v8 へ上がる。
    {
        let w = Engine::open(&path).expect("v7 を writer open");
        drop(w);
    }
    assert_eq!(
        header_u32(&path, H_VERSION),
        CURRENT_FILE_VERSION,
        "writer open で現行 version へ migrate されていない (旧 binary が新 index を誤読する穴が残る)"
    );

    Engine::open(&path).expect("migrate 後も open できること");
    cleanup(&path);
}

/// #119 Step 0: `get_content_owned` が `get_content` と同じ値を返し、 content を re-tie し
/// 続ける writer と並行でも silent None / torn を出さないこと (rag の本文読み経路)。
#[test]
fn get_content_owned_matches_borrow_and_survives_rewrite() {
    let path = tmp_path("content_owned");
    cleanup(&path);

    let eng = std::sync::Arc::new(
        Engine::create_growable_opts(&path, GrowableOptions::default()).expect("create"),
    );
    let eid = eng.entity();
    let bodies: Vec<String> = (0..5).map(|i| format!("c{}-{}", i, "y".repeat(48 + i * 61))).collect();
    eng.content(eid, "body", bodies[0].as_bytes());

    // 単独 read: 借用版と owned 版が一致する
    assert_eq!(
        eng.get_content(eid, "body").map(|b| b.to_vec()),
        eng.get_content_owned(eid, "body"),
        "owned 版が借用版と違う値を返した"
    );

    // 並行 re-tie 下でも既知値のどれかが必ず返る
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let eng = eng.clone();
        let stop = stop.clone();
        let bodies = bodies.clone();
        std::thread::spawn(move || {
            let mut n = 0usize;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                eng.content(eid, "body", bodies[n % bodies.len()].as_bytes());
                n += 1;
            }
        })
    };
    let mut reads = 0usize;
    let mut missing = 0usize;
    let mut corrupt = 0usize;
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(800);
    while std::time::Instant::now() < deadline {
        match eng.get_content_owned(eid, "body") {
            Some(b) => {
                if !bodies.iter().any(|x| x.as_bytes() == b.as_slice()) {
                    corrupt += 1;
                }
            }
            None => missing += 1,
        }
        reads += 1;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();

    assert!(reads > 500, "read が回っていない (reads={reads})");
    assert_eq!(missing, 0, "silent None が {missing} 件 / reads={reads}");
    assert_eq!(corrupt, 0, "既知値以外 (torn) が {corrupt} 件 / reads={reads}");

    drop(writer_marker(&eng));
    cleanup(&path);
}

/// drop 順を明示するためのヘルパ (Arc を最後に落とす)。
fn writer_marker<T>(_t: &T) {}
