//! v10 の破損耐性を **全 segment × 複数の壊し方 × seal の有無** で網羅する。
//!
//! v10 は DB が directory なので、 「1 本だけ欠ける / 短い / 中身が化ける」 が外から起こる
//! (部分 copy、 rsync 中断、 backup 復元の取りこぼし、 disk error)。 v9 の 1 ファイルでは
//! 作れなかった状態なので、 面ごと押さえる。
//!
//! 分類:
//! - `CleanErr`      — open が Err。 **望ましい**
//! - `Correct`       — 開けて、 全値が期待どおり (壊した所がまだ書かれていない領域だった等)
//! - `SilentlyWrong` — **開けてしまい、 値が欠ける / 違う**。 最も危険
//! - `Panic` / `Crash` — 不可 (assert で落とす)
//!
//! gate している不変条件:
//! 1. **どの壊し方でも signal 死 / panic しない**
//! 2. **`seal_integrity()` した DB は、 CRC が覆う region の破損を必ず `CleanErr` にする**
//!    (= 封緘したバックアップは黙って壊れない)
//! 3. seal していない DB で `SilentlyWrong` になる組合せの一覧を出す (既知の限界の可視化)

use enchudb_engine::{Engine, ValueType};
use std::path::{Path, PathBuf};
use std::process::Command;

const CHILD_PATH_ENV: &str = "ENCHU_DMG_PATH";
const PREFIX: &str = "RES ";
const ENTS: u32 = 8_000;
const HIMOS: u32 = 2;

/// CRC table が覆う region の segment file (`compute_region_crc_table` と対応)。
const CRC_COVERED: &[&str] =
    &["himo/0000.seg", "himo/0001.seg", "entities.seg", "vocab.data.seg", "himoreg.data.seg"];


fn expected_digest() -> u64 {
    let mut d = 0u64;
    for i in 0..ENTS {
        for h in 0..HIMOS {
            d = d.wrapping_mul(31).wrapping_add((i * 10 + h) as u64);
        }
    }
    d
}

/// child: 開いて全値を読み、 digest を出す。 どう転んでも exit 0。
#[test]
fn damage_child() {
    let Ok(path) = std::env::var(CHILD_PATH_ENV) else { return };
    let line = std::panic::catch_unwind(|| {
        let eng = match Engine::open_readonly(&path) {
            Ok(e) => e,
            Err(e) => return format!("CleanErr {:?} {}", e.kind(), first_line(&e.to_string())),
        };
        let mut d = 0u64;
        let mut missing = 0u32;
        for i in 0..ENTS {
            let eid = u64::from(i);
            for h in 0..HIMOS {
                match eng.get(eid, &format!("t.h{h}")).and_then(|v| v.to_string().parse::<u64>().ok())
                {
                    Some(v) => d = d.wrapping_mul(31).wrapping_add(v),
                    None => {
                        missing += 1;
                        d = d.wrapping_mul(31);
                    }
                }
            }
        }
        if d == expected_digest() && missing == 0 {
            "Correct".to_string()
        } else {
            format!("SilentlyWrong missing={missing}")
        }
    })
    .unwrap_or_else(|e| {
        let msg = e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        format!("Panic {}", first_line(&msg))
    });
    println!("{PREFIX}{line}");
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(90).collect()
}

fn probe(path: &Path) -> String {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["damage_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_PATH_ENV, path)
        .output()
        .expect("spawn child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let found =
        stdout.lines().find_map(|l| l.find(PREFIX).map(|i| l[i + PREFIX.len()..].trim().to_string()));
    match (out.status.code(), found) {
        (Some(0), Some(l)) => l,
        (code, _) => format!("Crash status={code:?}"),
    }
}

fn build_db(path: &str, seal: bool) {
    let _ = std::fs::remove_dir_all(path);
    let mut eng = Engine::create_with_capacity(path, 16_384).unwrap();
    eng.define_table("t", 10_000).unwrap();
    for h in 0..HIMOS {
        eng.define_himo_in("t", &format!("h{h}"), ValueType::Number, 10_000).unwrap();
    }
    for i in 0..ENTS {
        let e = eng.entity_in("t").unwrap();
        assert_eq!(e, u64::from(i), "eid が連番でない前提が崩れた");
        for h in 0..HIMOS {
            eng.tie(e, &format!("t.h{h}"), i * 10 + h);
        }
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    if seal {
        eng.seal_integrity().unwrap();
    }
    drop(eng);
}

/// DB directory 内の file を再帰列挙 (lock / tmp を除く)。
fn files_in(db: &Path) -> Vec<String> {
    let mut v = vec![];
    let mut stack = vec![(db.to_path_buf(), String::new())];
    while let Some((dir, prefix)) = stack.pop() {
        for e in std::fs::read_dir(&dir).unwrap() {
            let e = e.unwrap();
            let name = e.file_name().to_string_lossy().to_string();
            let rel = if prefix.is_empty() { name.clone() } else { format!("{prefix}/{name}") };
            if e.path().is_dir() {
                stack.push((e.path(), rel));
            } else if name != "lock" && !name.ends_with(".tmp") {
                v.push(rel);
            }
        }
    }
    v.sort();
    v
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Damage {
    Delete,
    Zero,
    Half,
    FlipMid,
}

fn apply(d: Damage, f: &Path) {
    match d {
        Damage::Delete => std::fs::remove_file(f).unwrap(),
        Damage::Zero => {
            std::fs::OpenOptions::new().write(true).open(f).unwrap().set_len(0).unwrap()
        }
        Damage::Half => {
            let len = std::fs::metadata(f).unwrap().len();
            std::fs::OpenOptions::new().write(true).open(f).unwrap().set_len(len / 2).unwrap();
        }
        Damage::FlipMid => {
            let mut bytes = std::fs::read(f).unwrap();
            if bytes.is_empty() {
                return;
            }
            let mid = bytes.len() / 2;
            let end = (mid + 64).min(bytes.len());
            for b in bytes[mid..end].iter_mut() {
                *b = !*b;
            }
            std::fs::write(f, bytes).unwrap();
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    let st = Command::new("cp").args(["-R".as_ref(), src.as_os_str(), dst.as_os_str()]).status().unwrap();
    assert!(st.success(), "cp -R failed");
    let _ = std::fs::remove_file(dst.join("lock"));
}

fn run_matrix(seal: bool, damages: &[Damage], only: Option<&[&str]>) -> Vec<(String, Damage, String)> {
    let base = format!("/tmp/enchu_dmgmx_{}_{}", std::process::id(), if seal { "sealed" } else { "plain" });
    build_db(&base, seal);
    let files = files_in(Path::new(&base));
    assert!(files.len() >= 10, "segment が少なすぎる: {files:?}");

    let mut out = vec![];
    for f in &files {
        if let Some(list) = only {
            if !list.contains(&f.as_str()) {
                continue;
            }
        }
        for &d in damages {
            let dst = PathBuf::from(format!("{base}.case"));
            let _ = std::fs::remove_dir_all(&dst);
            copy_dir(Path::new(&base), &dst);
            apply(d, &dst.join(f));
            out.push((f.clone(), d, probe(&dst)));
            let _ = std::fs::remove_dir_all(&dst);
        }
    }
    let _ = std::fs::remove_dir_all(&base);
    out
}

#[test]
fn damage_never_crashes_and_sealed_dbs_always_report() {
    // 1) 健全な DB は Correct であること (digest の妥当性確認)。
    let healthy = format!("/tmp/enchu_dmgmx_{}_healthy", std::process::id());
    build_db(&healthy, false);
    assert_eq!(probe(Path::new(&healthy)), "Correct", "健全な DB が Correct でない");
    let _ = std::fs::remove_dir_all(&healthy);

    // 2) seal していない DB: 全 file × 3 通り。 signal 死 / panic しないこと。
    let plain = run_matrix(false, &[Damage::Delete, Damage::Zero, Damage::Half], None);

    // 3) seal した DB: CRC が覆う region を壊したら **必ず** Err になること。
    let sealed = run_matrix(true, &[Damage::Zero, Damage::Half, Damage::FlipMid], Some(CRC_COVERED));

    let mut bad = vec![];
    for (f, d, r) in plain.iter().chain(sealed.iter()) {
        if r.starts_with("Crash") || r.starts_with("Panic") {
            bad.push(format!("{f} {d:?}: {r}"));
        }
    }

    // seal 済みの不変条件は 「CleanErr であること」 ではなく **「黙って壊れないこと」**。
    // 壊した箇所が commit 済み領域の未使用部だった場合は Correct が正しい答えなので。
    let mut sealed_missed = vec![];
    for (f, d, r) in &sealed {
        if r.starts_with("SilentlyWrong") {
            sealed_missed.push(format!("{f} {d:?}: {r}"));
        }
    }

    eprintln!("\n=== seal 無し (全 file × delete / zero / half) ===");
    let mut silent = vec![];
    for (f, d, r) in &plain {
        eprintln!("  {f:24} {:8}  {r}", format!("{d:?}"));
        if r.starts_with("SilentlyWrong") {
            silent.push(format!("{f} {d:?}"));
        }
    }
    eprintln!("\n=== seal_integrity 済み (CRC 対象 region) ===");
    for (f, d, r) in &sealed {
        eprintln!("  {f:24} {:8}  {r}", format!("{d:?}"));
    }
    eprintln!(
        "\n合計 {} ケース / うち seal 無しで黙って壊れたのが {} ケース:\n  {}",
        plain.len() + sealed.len(),
        silent.len(),
        silent.join("\n  ")
    );

    assert!(bad.is_empty(), "signal 死 / panic したケース:\n{}", bad.join("\n"));
    assert!(
        sealed_missed.is_empty(),
        "seal 済みなのに黙って壊れたケース (CRC 検証の穴):\n{}",
        sealed_missed.join("\n")
    );
}
