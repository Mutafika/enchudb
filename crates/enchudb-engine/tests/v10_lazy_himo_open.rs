//! v10 / request23 D2: **触らない himo の segment は open しない**。
//!
//! v10 は himo 1 本 = file 1 本なので、 open 代が himo 数に比例していた。 consumer は
//! 1 コマンド 1 process (償却先が無い) で、 kenning の実測では 1 コマンドが触る himo は
//! 48 本中 2〜13 本しかない。 `HimoStore` の column を遅延 mmap にして、 その差を消した。
//!
//! ここで gate するのは 3 つ:
//! 1. 触っていない himo の segment file は本当に open されないか (counter で数える)
//! 2. 遅延させても **欠けた / 短い segment は open で落ちる** か (安全性を落としていないか)
//! 3. 触っていない himo が manifest から消えないか (次回 open の検証材料を失わないか)

use enchudb_engine::{Engine, ValueType};
use std::sync::Mutex;

const HIMOS: u32 = 40;
const ENTS: u32 = 20;

/// `segment_map` の open counter は process 大域なので、 この binary 内の test を直列化する。
static SERIAL: Mutex<()> = Mutex::new(());

/// 直前の test が panic しても後続を毒で落とさない (本当の失敗が隠れる)。
fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn tmp(tag: &str) -> String {
    format!(
        "{}/enchudb-lazy-himo-{}-{}",
        std::env::temp_dir().display(),
        tag,
        std::process::id()
    )
}

fn build(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("t", 1000).unwrap();
    for h in 0..HIMOS {
        eng.define_himo_in("t", &format!("h{h}"), ValueType::Number, 100).unwrap();
    }
    for i in 0..ENTS {
        let e = eng.entity_in("t").unwrap();
        for h in 0..HIMOS {
            eng.tie(e, &format!("t.h{h}"), i * 100 + h);
        }
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
}

/// open した segment file の数 (この呼び出しの中だけ)。
fn opens_during(f: impl FnOnce()) -> u64 {
    enchudb_engine::segment_map::reset_stats();
    f();
    enchudb_engine::segment_map::open_stats().0
}

#[test]
fn untouched_himo_segments_are_not_opened() {
    let _g = serial();
    let path = tmp("lazy");
    build(&path);

    // 開いて himo を 1 本も触らない
    let mut eng = None;
    let bare = opens_during(|| eng = Some(Engine::open_readonly(&path).unwrap()));
    let eng = eng.unwrap();
    assert!(
        bare < HIMOS as u64,
        "himo を触っていないのに {bare} file 開いた (himo {HIMOS} 本 = 遅延していない)"
    );

    // 1 本触ると、 その 1 本だけ開く
    let first = opens_during(|| {
        assert_eq!(eng.get(0, "t.h3"), Some(3));
    });
    assert_eq!(first, 1, "himo 1 本の read で開いた file 数");

    // 同じ himo をもう一度触っても開かない (mmap は保持される)
    let again = opens_during(|| {
        assert_eq!(eng.get(1, "t.h3"), Some(103));
    });
    assert_eq!(again, 0, "2 回目の read で再 open してはいけない");

    // 別の himo を触ればまた 1 本だけ
    let other = opens_during(|| {
        assert_eq!(eng.get(2, "t.h17"), Some(217));
    });
    assert_eq!(other, 1, "himo 2 本目の read で開いた file 数");

    drop(eng);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 遅延しても 「欠けた segment を黙って開く」 ことにはならない。
#[test]
fn missing_or_short_himo_segment_still_fails_open() {
    let _g = serial();

    // (a) manifest ごと消して file も消す = manifest では検出できない経路
    let path = tmp("missing");
    build(&path);
    std::fs::remove_file(format!("{path}/segments")).unwrap();
    std::fs::remove_file(format!("{path}/himo/0031.seg")).unwrap();
    let err = match Engine::open_readonly(&path) { Ok(_) => panic!("open が成功してしまった"), Err(e) => e };
    assert!(
        err.to_string().contains("himo") || err.to_string().contains("0031"),
        "欠けた himo segment が open で検出されていない: {err}"
    );
    let _ = enchudb_engine::db_files::remove_db(&path);

    // (b) manifest は残したまま short にする = manifest 検証で落ちる経路
    let path = tmp("short");
    build(&path);
    let victim = format!("{path}/himo/0031.seg");
    let len = std::fs::metadata(&victim).unwrap().len();
    assert!(len > 4096);
    std::fs::OpenOptions::new().write(true).open(&victim).unwrap().set_len(len / 2).unwrap();
    let err = match Engine::open_readonly(&path) { Ok(_) => panic!("open が成功してしまった"), Err(e) => e };
    assert!(
        err.to_string().contains("truncated"),
        "短くなった himo segment が open で検出されていない: {err}"
    );
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 1 本も himo を触らない writer session の後でも、 manifest は全 himo を載せ続ける。
/// (`all()` = mmap 済みだけ、 を manifest にそのまま使うと未 mmap 分が消える)
#[test]
fn manifest_keeps_untouched_himo_entries() {
    let _g = serial();
    let path = tmp("manifest");
    build(&path);

    let before = std::fs::read_to_string(format!("{path}/segments")).unwrap();
    // manifest を消してから開く = この session が **ゼロから書き直す** ことを強制する
    // (segment が伸びないと skip されて test が空振りになるため)。 manifest 不在なので
    // slot の長さは stat で埋まる経路も一緒に通る。
    std::fs::remove_file(format!("{path}/segments")).unwrap();
    {
        // himo を 1 本も触らない writer session
        let eng = Engine::open(&path).unwrap();
        let _ = eng.entity_in("t").unwrap();
        eng.commit();
    } // Drop が sync_and_mark_clean → write_manifest
    let after = std::fs::read_to_string(format!("{path}/segments")).unwrap();
    assert_eq!(before, after, "触らない session が書き直した manifest が元と違う");
    for h in 0..HIMOS {
        let rel = format!("himo/{h:04}.seg");
        assert!(before.contains(&rel), "作成直後の manifest に {rel} が無い");
        assert!(after.contains(&rel), "触らない session の後で manifest から {rel} が消えた");
    }

    // その manifest が次の open の検証材料として生きていること
    let victim = format!("{path}/himo/0031.seg");
    let len = std::fs::metadata(&victim).unwrap().len();
    std::fs::OpenOptions::new().write(true).open(&victim).unwrap().set_len(len / 2).unwrap();
    let err = match Engine::open_readonly(&path) { Ok(_) => panic!("open が成功してしまった"), Err(e) => e };
    assert!(err.to_string().contains("truncated"), "manifest 検証が効いていない: {err}");
    let _ = enchudb_engine::db_files::remove_db(&path);
}
