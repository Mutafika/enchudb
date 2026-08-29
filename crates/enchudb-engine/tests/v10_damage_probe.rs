//! v10 で新しく増えた故障面の探索: DB が directory + segment file 群になったので、
//! 「segment が 1 本欠ける / 短い」 「sidecar が欠ける」 が **外から** 起こりうる
//! (部分 copy、 backup 復元、 disk error、 rsync 中断)。 v9 では 1 ファイルだったので
//! 存在しない状態だった。
//!
//! 期待するのは 「clean な Err か、 正しいデータ」 であって、 **SIGBUS / SIGSEGV で死ぬのは
//! 不可** (reserve 領域は PROT_NONE なので committed を越えて読むと SIGBUS になりうる)。
//! 静かに 0 を返す (= 無言のデータ欠損) も望ましくない。
//!
//! 各ケースは子 process で開く。 親は signal 死かどうかを exit status で見る。

use enchudb_engine::{Engine, ValueType};
use std::path::Path;
use std::process::Command;

const CHILD_PATH_ENV: &str = "ENCHU_V10_DMG_PATH";
const CHILD_MODE_ENV: &str = "ENCHU_V10_DMG_MODE";
const PROBE_PREFIX: &str = "DMG ";

const ENTS: u32 = 40;

/// child: 開いて全 entity を読み、 結果を 1 行で出す。 どう転んでも exit 0
/// (親が signal 死と区別できるように)。
#[test]
fn damage_probe_child() {
    let Ok(path) = std::env::var(CHILD_PATH_ENV) else { return };
    let ro = std::env::var(CHILD_MODE_ENV).as_deref() == Ok("ro");
    let line = std::panic::catch_unwind(|| {
        let eng: std::sync::Arc<Engine> = if ro {
            match Engine::open_readonly(&path) {
                Ok(e) => std::sync::Arc::new(e),
                Err(e) => return format!("ERR open kind={:?} msg={e}", e.kind()),
            }
        } else {
            match Engine::open(&path) {
                Ok(e) => e,
                Err(e) => return format!("ERR open kind={:?} msg={e}", e.kind()),
            }
        };
        let mut sum = 0i64;
        let mut got = 0u32;
        for e in 1..=u64::from(ENTS) {
            if let Some(v) = eng.get(e, "widgets.n") {
                if let Ok(n) = v.to_string().parse::<i64>() {
                    sum += n;
                    got += 1;
                }
            }
        }
        format!("OK ents={} himos={} read={got} sum={sum}", eng.entity_count(), eng.himo_count())
    })
    .unwrap_or_else(|e| {
        let msg = e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        format!("PANIC {}", msg.lines().next().unwrap_or("").chars().take(160).collect::<String>())
    });
    println!("{PROBE_PREFIX}{line}");
}

fn probe(path: &Path, ro: bool) -> String {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["damage_probe_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_PATH_ENV, path)
        .env(CHILD_MODE_ENV, if ro { "ro" } else { "rw" })
        .output()
        .expect("spawn child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let found = stdout
        .lines()
        .find_map(|l| l.find(PROBE_PREFIX).map(|i| l[i + PROBE_PREFIX.len()..].trim().to_string()));
    match (out.status.code(), found) {
        (Some(0), Some(l)) => l,
        // signal 死 (SIGBUS / SIGSEGV) か、 harness ごと落ちた
        (code, l) => format!(
            "CRASH status={code:?} line={l:?} stderr={}",
            String::from_utf8_lossy(&out.stderr).lines().take(3).collect::<Vec<_>>().join(" / ")
        ),
    }
}

fn base_db(tag: &str) -> String {
    let path = format!("/tmp/enchu_v10_dmg_{}_{tag}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("widgets", 200).unwrap();
    eng.define_himo_in("widgets", "n", ValueType::Number, 200).unwrap();
    eng.define_himo_in("widgets", "m", ValueType::Number, 200).unwrap();
    for i in 0..ENTS {
        let e = eng.entity_in("widgets").unwrap();
        eng.tie(e, "widgets.n", i + 1);
        eng.tie(e, "widgets.m", 1000 + i);
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    drop(eng);
    path
}

fn ls(path: &str) -> Vec<String> {
    let mut v = vec![];
    for e in std::fs::read_dir(path).unwrap() {
        let e = e.unwrap();
        let name = e.file_name().to_string_lossy().to_string();
        if e.path().is_dir() {
            for f in std::fs::read_dir(e.path()).unwrap() {
                let f = f.unwrap();
                v.push(format!(
                    "{name}/{} ({})",
                    f.file_name().to_string_lossy(),
                    f.metadata().unwrap().len()
                ));
            }
        } else {
            v.push(format!("{name} ({})", e.metadata().unwrap().len()));
        }
    }
    v.sort();
    v
}

#[test]
fn damaged_v10_dir_reports_cleanly_instead_of_crashing() {
    let path = base_db("base");
    eprintln!("--- 健全な v10 DB の中身 ---");
    for f in ls(&path) {
        eprintln!("  {f}");
    }
    let healthy = probe(Path::new(&path), false);
    eprintln!("healthy rw : {healthy}");
    eprintln!("healthy ro : {}", probe(Path::new(&path), true));
    assert!(healthy.starts_with("OK "), "健全な DB が開けない: {healthy}");

    // 破損ケースごとに copy して壊す。
    let cases: Vec<(&str, Box<dyn Fn(&Path)>)> = vec![
        ("himo segment を 1 本削除", Box::new(|p: &Path| {
            let f = p.join("himo/0001.seg");
            std::fs::remove_file(&f).unwrap_or_else(|e| panic!("{f:?}: {e}"));
        })),
        ("himo segment を半分に truncate", Box::new(|p: &Path| {
            let f = p.join("himo/0000.seg");
            let len = std::fs::metadata(&f).unwrap().len();
            std::fs::OpenOptions::new().write(true).open(&f).unwrap().set_len(len / 2).unwrap();
        })),
        ("himo segment を 0 byte に truncate", Box::new(|p: &Path| {
            let f = p.join("himo/0000.seg");
            std::fs::OpenOptions::new().write(true).open(&f).unwrap().set_len(0).unwrap();
        })),
        ("entities.seg を 0 byte に truncate", Box::new(|p: &Path| {
            let f = p.join("entities.seg");
            std::fs::OpenOptions::new().write(true).open(&f).unwrap().set_len(0).unwrap();
        })),
        ("header.seg を 1 byte 短く", Box::new(|p: &Path| {
            let f = p.join("header.seg");
            let len = std::fs::metadata(&f).unwrap().len();
            std::fs::OpenOptions::new().write(true).open(&f).unwrap().set_len(len - 1).unwrap();
        })),
        ("sidecar tables を削除", Box::new(|p: &Path| {
            let _ = std::fs::remove_file(p.join("tables"));
        })),
    ];

    let mut report = vec![];
    for (i, (name, damage)) in cases.iter().enumerate() {
        let dst = format!("/tmp/enchu_v10_dmg_{}_case{i}.db", std::process::id());
        let _ = std::fs::remove_dir_all(&dst);
        let st = Command::new("cp").args(["-R", &path, &dst]).status().unwrap();
        assert!(st.success(), "cp -R failed");
        let _ = std::fs::remove_file(Path::new(&dst).join("lock"));
        damage(Path::new(&dst));
        let rw = probe(Path::new(&dst), false);
        let ro = probe(Path::new(&dst), true);
        report.push((name, rw, ro));
        let _ = std::fs::remove_dir_all(&dst);
    }
    let _ = std::fs::remove_dir_all(&path);

    eprintln!("\n--- 破損ケース ---");
    let mut bad = vec![];
    for (name, rw, ro) in &report {
        eprintln!("{name}\n    rw: {rw}\n    ro: {ro}");
        for r in [rw, ro] {
            if r.starts_with("CRASH") || r.starts_with("PANIC") {
                bad.push(format!("{name}: {r}"));
            }
        }
    }
    assert!(bad.is_empty(), "signal 死 / panic したケース (clean な Err であるべき):\n{}", bad.join("\n"));

    // 既知の残り: segment を 0 byte / 途中まで truncate しても open は通り、 その region が
    // **静かに空になる** (`himo/0000.seg` を 0 にすると read=0 / sum=0)。 v10 は 「まだ触って
    // いない region」 も 0 byte なので、 file 長だけでは 「切り詰められた」 と区別できない。
    // 検出するには header に per-segment の committed 長を持つ必要がある (GH issue で追跡)。
}
