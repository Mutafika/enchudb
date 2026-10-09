//! 電源断 (page cache ごと失う) の後に、 書き出しが済んだと返したデータが残っているか。
//!
//! `v10_crash_consistency` は SIGKILL = process の死を見る。 mmap の dirty page は page cache に
//! 残って file に届くので、 msync の呼び忘れ・順序の誤りは通ってしまう。 ここは
//! `enchudb_oplog::crashsim` が書き出し (msync / sync_all) の済んだ page を控え、 書き手が走って
//! いる最中の任意の瞬間に 「今電源が落ちた」 姿の像を directory ごと写す:
//!
//! - `Lost` — 書き出しの済んだ page だけ (書き出していない page は前の中身 / ゼロ)
//! - `Mixed` — page ごとに 「書き出しの済んだ中身」 か 「今の中身」 (OS が先に書き出していた) を選ぶ
//!
//! 像は子 process で開く (SIGBUS / panic で親を巻き込まない)。 合格の条件:
//!
//! 1. **開ける** (書き出しの順序が正しければ、 どの瞬間の像も開けるはず)
//! 2. **書き出しが返った batch は全部、 値まで正しい** (durability)
//! 3. **それ以外の entity も、 値があるならその entity に書いた値** (化けた値が出ない)
//!
//! 模型の前提 (metadata は journal で即 durable、 page 単位で書き出す) は crashsim の module doc。
//! 失敗した像は `ENCHU_POWER_LOSS_KEEP=1` で消さずに残る (`ENCHU_POWER_LOSS_VERIFY` に渡して再現)。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Mode};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

const VERIFY_ENV: &str = "ENCHU_POWER_LOSS_VERIFY";
const ACKED_ENV: &str = "ENCHU_POWER_LOSS_ACKED";
const KIND_ENV: &str = "ENCHU_POWER_LOSS_KIND";
const KEEP_ENV: &str = "ENCHU_POWER_LOSS_KEEP";
/// 子の失敗理由のうち 「開けなかった」 の印
const OPEN_ERR: &str = "開けない: ";
/// 子が 「作り終える前の像 (開けないが、 作り直せる)」 を見た印 (stderr、 #415)
const UNBORN: &str = "POWER_LOSS_UNBORN";

const BATCH: u32 = 200;
const BATCHES: u32 = 40;
const HIMOS: u32 = 3;
const OPLOG_CAP: usize = 4 * 1024 * 1024;
/// 1 回の run で撮る像の上限 (子 process で 1 枚ずつ開くので、 run の時間はほぼこれで決まる)
const MAX_IMAGES: usize = 300;

fn value_of(i: u32, h: u32) -> u32 {
    i.wrapping_mul(7).wrapping_add(h * 1_000_003)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    /// standalone: tie → `flush()` が返ったら durable
    Flush,
    /// concurrent + oplog: tie_async → `oplog_sync()` が返ったら durable (open は oplog から recover)
    Oplog,
    /// standalone で Leaf / Tag の値を書き換え・untie・delete する (#414)。 像は `Lost` だけ (下の `LEAF_*`)
    Leaf,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Flush => "flush",
            Kind::Oplog => "oplog",
            Kind::Leaf => "leaf",
        }
    }
    /// 書き出しが返る batch の数
    fn batches(self) -> u32 {
        match self {
            Kind::Flush | Kind::Oplog => BATCHES,
            Kind::Leaf => LEAF_BATCHES,
        }
    }
    fn parse(s: &str) -> Self {
        match s {
            "flush" => Kind::Flush,
            "oplog" => Kind::Oplog,
            "leaf" => Kind::Leaf,
            _ => panic!("unknown kind {s}"),
        }
    }
}

fn scratch(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("enchu_power_loss_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn define_schema(eng: &mut Engine) {
    eng.define_table("t", 40_000).unwrap();
    for h in 0..HIMOS {
        eng.define_himo_in("t", &format!("h{h}"), ValueType::Number, 40_000).unwrap();
    }
}

/// 書き手。 batch を書いて durable にするたびに `acked` を進める。
fn write_workload(kind: Kind, db: &str, acked: &AtomicU32) {
    match kind {
        Kind::Flush => {
            let mut eng = Engine::create_with_capacity(db, 65_536).unwrap();
            define_schema(&mut eng);
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            for b in 0..BATCHES {
                for k in 0..BATCH {
                    let i = b * BATCH + k;
                    let e = eng.entity_in("t").unwrap();
                    assert_eq!(e, u64::from(i), "eid が連番でない");
                    for h in 0..HIMOS {
                        eng.tie(e, &format!("t.h{h}"), value_of(i, h));
                    }
                }
                eng.flush().unwrap();
                eng.persist_tables().unwrap();
                acked.store(b + 1, Ordering::Release);
            }
        }
        Kind::Oplog => {
            let mut eng = Engine::create_with_capacity(db, 65_536).unwrap();
            define_schema(&mut eng);
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
            let hids: Vec<u16> =
                (0..HIMOS).map(|h| eng.himo_id(&format!("t.h{h}")).unwrap() as u16).collect();
            for b in 0..BATCHES {
                for k in 0..BATCH {
                    let i = b * BATCH + k;
                    let e = eng.entity_in("t").unwrap();
                    assert_eq!(e, u64::from(i), "eid が連番でない");
                    for (h, &hid) in hids.iter().enumerate() {
                        eng.tie_async_by_id(e, hid, value_of(i, h as u32));
                    }
                }
                eng.oplog_sync().unwrap();
                acked.store(b + 1, Ordering::Release);
            }
        }
        Kind::Leaf => {
            let mut eng = Engine::create_with_capacity(db, 65_536).unwrap();
            eng.define_table("t", 40_000).unwrap();
            for (name, vt) in LEAF_HIMOS {
                eng.define_himo_in("t", name, vt, 0).unwrap();
            }
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            let full: Vec<String> = LEAF_HIMOS.iter().map(|(n, _)| format!("t.{n}")).collect();
            for (b, ops) in leaf_batches().into_iter().enumerate() {
                for op in ops {
                    match op {
                        Op::New(i) => {
                            let e = eng.entity_in("t").unwrap();
                            assert_eq!(e, u64::from(i), "eid が連番でない");
                        }
                        Op::Set(e, h, Val::N(v)) => eng.tie(u64::from(e), &full[h], v),
                        Op::Set(e, h, Val::T(t)) => eng.tie_text(u64::from(e), &full[h], &t),
                        Op::Untie(e, h) => eng.untie(u64::from(e), &full[h]),
                        Op::Delete(e) => eng.delete(u64::from(e)),
                    }
                }
                eng.flush().unwrap();
                eng.persist_tables().unwrap();
                acked.store(b as u32 + 1, Ordering::Release);
            }
        }
    }
}

// ---- Leaf: standalone で Leaf / Tag の値を書き換える書き手と、 その oracle (#414) ----
//
// Leaf の書き換えは 「新しい slot に書く → cell を付け替える → 旧い slot を空きに戻す」。 旧い slot を同じ batch の
// 新しい値が使い回し、 その中身が cell の付け替えより先にディスクに届くと (flush は Leaf 領域を列より先に msync
// する)、 書き出しの返った値が空 / 別の行の値になる。 書き換えを batch の先に置いて、 空いた slot を同じ batch の
// 新しい値が使い回すようにしている。
//
// 像は `Lost` だけ。 書き手は 1 本で flush の間は書かないので、 `Lost` の像では 「cell が中身より先に届く」 (#419) は
// 起きない (flush は中身の segment を列より先に msync する)。 `Mixed` の像は page ごとに今の中身を混ぜるので #419 が
// 混ざり、 #414 と見分けられない。

const LEAF_BATCH: u32 = 150;
const LEAF_BATCHES: u32 = 30;
const LEAF_HIMOS: [(&str, ValueType); 3] =
    [("n0", ValueType::Number), ("tg", ValueType::Tag), ("lf", ValueType::Leaf)];

#[derive(Clone, PartialEq, Eq, Debug)]
enum Val {
    N(u64),
    T(String),
}

enum Op {
    New(u32),
    Set(u32, usize, Val),
    Untie(u32, usize),
    Delete(u32),
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// batch ごとの操作列 (決定的)。 前の batch の entity を書き換え / untie / delete してから、 新しい entity に 3 列を書く。
/// 消した entity には以後触らない (eid は使い回されない: table の枠に余裕がある)。 Leaf の値は長さを変える。
fn leaf_batches() -> Vec<Vec<Op>> {
    let mut alive: Vec<bool> = Vec::new();
    let mut out = Vec::new();
    for b in 0..LEAF_BATCHES {
        let mut ops = Vec::new();
        let born = b * LEAF_BATCH;
        if b > 0 {
            for j in 0..60u64 {
                let e = (mix((u64::from(b) << 16) | j) % u64::from(born)) as u32;
                if !alive[e as usize] {
                    continue;
                }
                match j % 10 {
                    0..=5 => ops.push(Op::Set(e, 2, Val::T(format!("lf-{e}-{b}-{}", "x".repeat((j % 4) as usize * 20))))),
                    6 => ops.push(Op::Set(e, 1, Val::T(format!("tag{}", (e + b) % 13)))),
                    7 => ops.push(Op::Set(e, 0, Val::N(u64::from(e) * 7 + u64::from(b) * 100_000))),
                    8 => ops.push(Op::Untie(e, 2)),
                    _ => {
                        ops.push(Op::Delete(e));
                        alive[e as usize] = false;
                    }
                }
            }
        }
        for k in 0..LEAF_BATCH {
            let i = born + k;
            alive.push(true);
            ops.push(Op::New(i));
            ops.push(Op::Set(i, 0, Val::N(u64::from(i) * 7 + u64::from(b))));
            ops.push(Op::Set(i, 1, Val::T(format!("tag{}", i % 13))));
            ops.push(Op::Set(i, 2, Val::T(format!("lf-{i}-{b}-{}", "y".repeat((i % 4) as usize * 20)))));
        }
        out.push(ops);
    }
    out
}

/// acked batch までの状態と、 それ以降に書いた値 (untie / delete は None) — 電源断の後に許される値。
fn leaf_allowed(acked: u32) -> (Vec<[Option<Val>; 3]>, Vec<[Vec<Option<Val>>; 3]>) {
    let total = (LEAF_BATCH * LEAF_BATCHES) as usize;
    let mut state: Vec<[Option<Val>; 3]> = vec![Default::default(); total];
    let mut later: Vec<[Vec<Option<Val>>; 3]> = vec![Default::default(); total];
    for (b, ops) in leaf_batches().into_iter().enumerate() {
        let after = b as u32 >= acked;
        for op in ops {
            let (e, h, v) = match op {
                Op::New(_) => continue,
                Op::Set(e, h, v) => (e as usize, h, Some(v)),
                Op::Untie(e, h) => (e as usize, h, None),
                Op::Delete(e) => {
                    for h in 0..3 {
                        if after {
                            later[e as usize][h].push(None);
                        } else {
                            state[e as usize][h] = None;
                        }
                    }
                    continue;
                }
            };
            if after {
                later[e][h].push(v);
            } else {
                state[e][h] = v;
            }
        }
    }
    (state, later)
}

fn verify_leaf(eng: &Engine, acked: u32) -> String {
    let (state, later) = leaf_allowed(acked);
    let full: Vec<String> = LEAF_HIMOS.iter().map(|(n, _)| format!("t.{n}")).collect();
    for (e, cells) in state.iter().enumerate() {
        for (h, want) in cells.iter().enumerate() {
            let got = match LEAF_HIMOS[h].1 {
                ValueType::Number => eng.get(e as u64, &full[h]).map(Val::N),
                _ => eng
                    .get_text_owned(e as u64, &full[h])
                    .map(|b| Val::T(String::from_utf8_lossy(&b).into_owned())),
            };
            if &got == want || later[e][h].contains(&got) {
                continue;
            }
            let what = if want.is_some() && !later[e][h].contains(&None) && got.is_none() {
                "書き出しが返った値が消えた"
            } else {
                "許されない値"
            };
            return format!(
                "{what}: eid={e} {} got={got:?} want={want:?} (以後に書いた値 {:?}、 acked batch {acked})",
                full[h], later[e][h]
            );
        }
    }
    String::new()
}

/// 像を開いて確かめる。 空文字 = 合格。
fn verify_image(kind: Kind, db: &str, acked: u32) -> String {
    let opened = match kind {
        Kind::Flush | Kind::Leaf => Engine::open_standalone(db).map(Arc::new),
        Kind::Oplog => Engine::open_concurrent_with_oplog(db, OPLOG_CAP),
    };
    let eng = match opened {
        Ok(e) => e,
        // #415: 最初の書き出しが返る前の像は、 作り終えていなければ開けなくてよい。 ただし 「作成中」 (directory が
        // 無い) と言うこと、 そして同じ path に作り直せること (旧: 「壊れている」 で開けず、 作り直しも既存として断った)
        Err(e) if acked == 0 && (e.kind() == std::io::ErrorKind::NotFound || e.to_string().contains("incomplete")) => {
            return match Engine::create_with_capacity(db, 65_536) {
                Ok(_) => {
                    eprintln!("{UNBORN}");
                    String::new()
                }
                Err(c) => format!("作り終える前の像を作り直せない: 開く {e} / 作る {c}"),
            };
        }
        Err(e) => return format!("{OPEN_ERR}{e}"),
    };
    if kind == Kind::Leaf {
        return verify_leaf(&eng, acked);
    }
    let must_have = acked * BATCH;
    for i in 0..BATCHES * BATCH {
        for h in 0..HIMOS {
            let got = eng.get(u64::from(i), &format!("t.h{h}"));
            let want = u64::from(value_of(i, h));
            if i < must_have {
                if got != Some(want) {
                    return format!(
                        "書き出しが返った値が違う: eid={i} h{h} got={got:?} want={want} (acked batch {acked})"
                    );
                }
            } else if let Some(v) = got
                && v != want
            {
                return format!("化けた値: eid={i} h{h} got={v} want={want} (acked batch {acked})");
            }
        }
    }
    String::new()
}

/// 子: 像 1 枚を開いて確かめ、 失敗なら理由を stderr に出して非 0 で終わる。
#[test]
fn power_loss_verify_child() {
    let Ok(db) = std::env::var(VERIFY_ENV) else { return };
    let acked: u32 = std::env::var(ACKED_ENV).unwrap().parse().unwrap();
    let kind = Kind::parse(&std::env::var(KIND_ENV).unwrap());
    let err = verify_image(kind, &db, acked);
    if !err.is_empty() {
        eprintln!("POWER_LOSS_FAIL: {err}");
        std::process::exit(3);
    }
}

/// 子で像を確かめる。 (失敗の理由 (空 = 合格), 作り終える前の像だったか)
fn verify_in_child(kind: Kind, db: &Path, acked: u32) -> (String, bool) {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["power_loss_verify_child", "--exact", "--test-threads=1", "--nocapture"])
        .env(VERIFY_ENV, db)
        .env(ACKED_ENV, acked.to_string())
        .env(KIND_ENV, kind.name())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn child");
    let stderr = String::from_utf8_lossy(&out.stderr);
    let unborn = stderr.lines().any(|l| l == UNBORN);
    if out.status.success() {
        return (String::new(), unborn);
    }
    let err = match stderr.lines().find_map(|l| l.strip_prefix("POWER_LOSS_FAIL: ")) {
        Some(msg) => msg.to_string(),
        None => {
            let tail: Vec<&str> = stderr.lines().rev().take(6).collect();
            format!("子が落ちた ({:?}): {}", out.status, tail.into_iter().rev().collect::<Vec<_>>().join(" | "))
        }
    };
    (err, unborn)
}

/// crashsim の控えは process 全体で 1 つ。 同じ binary の test を並べて走らせない
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct Image {
    dir: PathBuf,
    acked: u32,
    mode: Mode,
    divergent: Vec<(String, u64)>,
}

/// 書き手を走らせ、 その横で像を撮り続ける。
fn run(kind: Kind) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());

    let root = scratch(kind.name());
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    let db_str = db.to_str().unwrap().to_string();

    crashsim::start();
    let acked = Arc::new(AtomicU32::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let acked = acked.clone();
        let done = done.clone();
        std::thread::spawn(move || {
            write_workload(kind, &db_str, &acked);
            done.store(true, Ordering::Release);
        })
    };

    let mut images = Vec::new();
    let mut n = 0u64;
    // 書き手が止まった / 落ちた時に撮り続けない: 進捗が STALL の間動かなければ打ち切る
    const STALL: std::time::Duration = std::time::Duration::from_secs(60);
    let (mut last_acked, mut last_move) = (0u32, std::time::Instant::now());
    while !done.load(Ordering::Acquire) && !writer.is_finished() {
        let now_acked = acked.load(Ordering::Acquire);
        if now_acked != last_acked {
            (last_acked, last_move) = (now_acked, std::time::Instant::now());
        } else if last_move.elapsed() > STALL {
            crashsim::stop();
            let _ = std::fs::remove_dir_all(&root);
            panic!(
                "[{}] 書き手が {} 秒進まない (acked batch {now_acked}) — 書き出しが止まった",
                kind.name(),
                STALL.as_secs()
            );
        }
        if images.len() >= MAX_IMAGES {
            std::thread::sleep(std::time::Duration::from_millis(10));
            continue;
        }
        // acked を先に読む: 像の控えはこの時点以降の書き出しを含む (acked は控えた後に進む)
        let a = acked.load(Ordering::Acquire);
        let mode = if kind == Kind::Leaf || n.is_multiple_of(2) { Mode::Lost } else { Mode::Mixed { seed: n } };
        let dir = root.join(format!("img{n:03}"));
        match crashsim::capture(&live, &dir, mode) {
            Ok(st) => images.push(Image { dir, acked: a, mode, divergent: st.divergent }),
            Err(e) => eprintln!("capture {n} 失敗: {e}"),
        }
        n += 1;
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    if let Err(e) = writer.join() {
        crashsim::stop();
        let _ = std::fs::remove_dir_all(&root);
        std::panic::resume_unwind(e);
    }
    crashsim::stop();

    // oracle を確かめる: 電源断の無い live の DB (書き手は drop 済み) は、 全 batch acked で合格すること。
    // ここで落ちるなら検証器か oracle の方が間違っている
    let (sane, _) = verify_in_child(kind, &live.join("db"), kind.batches());
    if !sane.is_empty() {
        crashsim::stop();
        let _ = std::fs::remove_dir_all(&root);
        panic!("[{}] 電源断の無い DB が検証に通らない (oracle の誤り): {sane}", kind.name());
    }

    let keep = std::env::var(KEEP_ENV).is_ok();
    let mut failures = Vec::new();
    let (mut mid, mut unborn) = (0, 0);
    for img in &images {
        if img.acked > 0 && img.acked < kind.batches() {
            mid += 1;
        }
        let (err, was_unborn) = verify_in_child(kind, &img.dir.join("db"), img.acked);
        // 作り終える前の像 (開けないが 「作成中」 と言い、 作り直せた) は数えるだけ
        if was_unborn {
            unborn += 1;
        }
        if err.is_empty() {
            let _ = std::fs::remove_dir_all(&img.dir);
        } else {
            failures.push(format!(
                "{} ({:?}, acked {}): {err}\n    失われうる page: {:?}",
                img.dir.display(),
                img.mode,
                img.acked,
                img.divergent
            ));
            if !keep {
                let _ = std::fs::remove_dir_all(&img.dir);
            }
        }
    }
    eprintln!(
        "[{}] 像 {} 枚 (書き込みの途中 {mid}、 作り終える前 {unborn}) 書き出し {} 回 → 失敗 {}",
        kind.name(),
        images.len(),
        crashsim::events(),
        failures.len()
    );
    if !keep {
        let _ = std::fs::remove_dir_all(&root);
    }
    assert!(mid >= 3, "書き込みの途中で撮れた像が少ない ({mid}) — 検証が空振りしている");
    assert!(failures.is_empty(), "電源断の像で壊れた:\n{}", failures.join("\n"));
}

#[test]
fn power_loss_keeps_flushed_batches() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    run(Kind::Flush);
}

#[test]
fn power_loss_keeps_oplog_synced_batches() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    run(Kind::Oplog);
}

/// #414: Leaf の書き換えで旧い slot を空きに戻すのは、 cell の付け替えが書き出された後。
#[test]
fn power_loss_keeps_rewritten_leaf_values() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    run(Kind::Leaf);
}

/// crashsim の控えが、 rename で置き換えた sidecar の inode 番号を引き継いだ列の segment に写らない。
///
/// Linux (ext4 / overlayfs) は空いた inode の番号をすぐ次に作った file に渡す。 置き換えで控えを捨てて
/// いなかった頃は、 table の定義の sidecar を書き直した直後に作った列の page 0 に古い sidecar の控え
/// (`TBL1`) が写り、 像を開くと列の header (value_size 1) として読んで panic した (CI の Linux だけ、
/// 書き出しが返る前の像)。 番号を使い回さない FS (APFS) では元から起きない — ここは何も確かめずに通る。
#[test]
fn capture_does_not_carry_sidecar_pages_into_reused_inode() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("inode_reuse");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    for h in 0..16 {
        // sidecar を rename で置き換える (前の sidecar の inode が空く) → 列の segment を作る
        eng.persist_tables().unwrap();
        eng.define_himo_in("t", &format!("h{h}"), ValueType::Number, 1_000).unwrap();
    }
    let img = root.join("img");
    let st = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    st.unwrap();
    let mut carried = Vec::new();
    for ent in std::fs::read_dir(img.join("db").join("himo")).unwrap() {
        let p = ent.unwrap().path();
        if std::fs::read(&p).unwrap().starts_with(b"TBL1") {
            carried.push(p.display().to_string());
        }
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(carried.is_empty(), "sidecar の控えが列の segment に写った: {carried:?}");
}
