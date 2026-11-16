//! #972: 開く道が 「WAL に再生する record があるか」 を聞く読み (`OpLog::probe_replay`) は、 全部を解く再生 (#972 の前の engine の
//! 判定 = 読み書きで `OpLog::open` して `is_empty() || recover_with_tail().is_empty()` でないか) と同じ答えを、 file を書かずに出す。
//! 読むのは checkpoint の後の最初の当てる record まで。
//!
//! 突き合わせる像: `fixtures/` の file (0.36.2 の v3、 0.37.4 の v4 2 つ、 v5) と、 開く時に header を直す形 (A = 4・B = 5、 読めない
//! B の header)、 読み飛ばす op / 解けない op、 乱数で積んで畳んで byte を壊した WAL (1 区画 / 2 区画)、 crashsim の電源断の像
//! (feature `crashsim`)。
//!
//! cargo test -p enchudb-oplog --test issue972_replay_probe
//! cargo test -p enchudb-oplog --features crashsim --test issue972_replay_probe -- --test-threads=1   (電源断の像も)

#![cfg(unix)]

use enchudb_oplog::oplog::{Op, OpLog, op_type, wal_pos_offset, wal_reads};
use std::path::{Path, PathBuf};

const HALF: usize = 16 * 1024;
const REC_HEADER: usize = 112;
const OFF_OP: usize = 3;
const OFF_CRC: usize = 36;
/// `oplog.rs` の `LAP_SALT` (file の形の一部)。
const LAP_SALT: u32 = 0xC3A5_5A3C;
/// Tie (payload 16 B) の record の大きさ。
const TIE: usize = REC_HEADER + 16 + 8;

fn fixture_path(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

const FIXTURES: [&str; 4] =
    ["oplog-v3-0.36.2.bin", "oplog-v4-0.37.4.bin", "oplog-v4-0.37.4-b-active.bin", "oplog-v5.bin"];

struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu-issue972-oplog-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Scratch(p)
    }

    fn fixture(&self, name: &str) -> PathBuf {
        let p = self.0.join(name);
        std::fs::copy(fixture_path(name), &p).unwrap();
        p
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// #972 の前の engine の判定: 写しを読み書きで開き、 record が 1 件も無いか再生が空なら false。 開けなければ None。
fn full_answer(path: &Path) -> Option<bool> {
    let copy = path.with_extension("full");
    std::fs::copy(path, &copy).unwrap();
    let r = OpLog::open(&copy).ok().map(|w| !(w.is_empty() || w.recover_with_tail().is_empty()));
    let _ = std::fs::remove_file(&copy);
    r
}

/// 聞く読みの答え (開けなければ None)。 file を 1 byte も書かないことと、 容量が `open` と同じことも確かめる。
fn probe_answer(path: &Path) -> Option<bool> {
    let before = std::fs::read(path).unwrap();
    let r = OpLog::probe_replay(path);
    assert_eq!(std::fs::read(path).unwrap(), before, "聞く読みが file を書いた ({})", path.display());
    let r = r.ok()?;
    let copy = path.with_extension("cap");
    std::fs::copy(path, &copy).unwrap();
    assert_eq!(Some(r.capacity), OpLog::open(&copy).ok().map(|w| w.capacity()), "容量が open と違う");
    let _ = std::fs::remove_file(&copy);
    Some(r.records)
}

/// 2 つの答えが同じ (同じ値を返す)。
fn same(path: &Path, what: &str) -> Option<bool> {
    let full = full_answer(path);
    assert_eq!(probe_answer(path), full, "{what}: 聞く読みと全部を解く再生の答えが違う");
    full
}

fn r32(file: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(file[at..at + 4].try_into().unwrap())
}

/// record v3 の CRC: payload の FNV-1a に、 header の op 〜 author (3..36) を 4 byte ずつ混ぜる (`oplog.rs` の `record_crc`)。
fn record_crc(header: &[u8], payload: &[u8]) -> u32 {
    let fnv = |h: u32, data: &[u8]| data.iter().fold(h, |h, &b| (h ^ b as u32).wrapping_mul(0x0100_0193));
    header[3..36].chunks(4).fold(fnv(0x811c_9dc5, payload), |h, c| {
        let mut w = [0u8; 4];
        w[..c.len()].copy_from_slice(c);
        (h ^ u32::from_le_bytes(w)).wrapping_mul(0x0100_0193)
    })
}

/// file の位置 `at` の record の op の種類を `op` に書き換え、 CRC と周の印を付け直す (印の形 = 結ぶ / 結ばない はそのまま)。
fn retype(file: &mut [u8], at: usize, op: u8) {
    assert_eq!(&file[at..at + 2], b"WL", "record の頭");
    let len = r32(file, at + 4) as usize;
    let t = at + REC_HEADER + len;
    let (lap, check, old_crc) = (r32(file, t), r32(file, t + 4), r32(file, at + OFF_CRC));
    let bound = check != lap ^ LAP_SALT;
    if bound {
        assert_eq!(check, lap ^ LAP_SALT ^ old_crc.max(1), "前提: 印は結んだ形");
    }
    file[at + OFF_OP] = op;
    let crc = record_crc(&file[at..at + REC_HEADER], &file[at + REC_HEADER..at + REC_HEADER + len]);
    file[at + OFF_CRC..at + OFF_CRC + 4].copy_from_slice(&crc.to_le_bytes());
    let check = if bound { lap ^ LAP_SALT ^ crc.max(1) } else { lap ^ LAP_SALT };
    file[t + 4..t + 8].copy_from_slice(&check.to_le_bytes());
}

fn tie(wal: &OpLog, v: u64) {
    wal.append(Op::Tie { eid: 1, himo_id: 0, value: v }).unwrap();
}

/// fixture をそのまま (落ちた後の file = 再生する record あり)、 checkpoint を head へ進めた後 (再生する物なし)、 その後に Commit
/// だけ積んだ後 (再生する物なし)、 Alias を足した後 (再生する物あり)。
#[test]
fn fixtures_answer_like_the_full_replay() {
    let root = Scratch::new("fixtures");
    for name in FIXTURES {
        let p = root.fixture(name);
        assert_eq!(same(&p, name), Some(true), "前提: {name} は再生する record を持つ");
        {
            let wal = OpLog::open(&p).unwrap();
            assert!(!wal.recover_with_tail().is_empty());
            wal.advance_checkpoint(wal.head());
            wal.fsync().unwrap();
        }
        assert_eq!(same(&p, &format!("{name} の checkpoint を head へ")), Some(false));
        {
            let wal = OpLog::open(&p).unwrap();
            for _ in 0..3 {
                wal.append(Op::Commit).unwrap();
            }
            wal.fsync().unwrap();
        }
        assert_eq!(same(&p, &format!("{name} の checkpoint の後に Commit だけ")), Some(false));
        {
            let wal = OpLog::open(&p).unwrap();
            wal.append(Op::Alias { eid: 7, same_as: 8 }).unwrap();
            wal.fsync().unwrap();
        }
        assert_eq!(same(&p, &format!("{name} の Commit の後に Alias")), Some(true));
    }
}

/// 本当の open が header を直す形 (A の版 4・B の版 5 / 読めない B の header) でも、 聞く読みは file を書かずに同じ答えを出す。
/// 前提として、 読み書きの `open` はこの形の file を書く (聞くだけの道が読み書きで開くと、 本当の open の前に header を直していた)。
#[test]
fn a_header_the_open_rewrites_is_not_written_by_the_probe() {
    let root = Scratch::new("rewrite");
    let opened_writes = |p: &Path| {
        let copy = p.with_extension("rw");
        std::fs::copy(p, &copy).unwrap();
        drop(OpLog::open(&copy).unwrap());
        let changed = std::fs::read(&copy).unwrap() != std::fs::read(p).unwrap();
        let _ = std::fs::remove_file(&copy);
        changed
    };
    // A = 4・B = 5 (`issue851_bound_lap_mark` の同じ名前の試験の像): B をその場で畳んで v5 にし、 B に結んだ形の record を書く
    let p = root.fixture("oplog-v4-0.37.4-b-active.bin");
    {
        let wal = OpLog::open(&p).unwrap();
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset());
        for v in 200..=207 {
            tie(&wal, v);
        }
        wal.fsync().unwrap();
    }
    let mut image = std::fs::read(&p).unwrap();
    assert_eq!((r32(&image, 4), r32(&image, HALF + 4)), (5, 5));
    image[4..8].copy_from_slice(&4u32.to_le_bytes());
    std::fs::write(&p, &image).unwrap();
    assert!(opened_writes(&p), "前提: open は B の版を 4 に戻す");
    assert_eq!(same(&p, "A = 4・B = 5"), Some(true));
    // 読めない B の header: B に record のある file (B の record は読まない = 空の区画) と、 B が空の file
    for name in ["oplog-v4-0.37.4-b-active.bin", "oplog-v5.bin"] {
        let p = root.fixture(name);
        let mut image = std::fs::read(&p).unwrap();
        image[HALF..HALF + 4].copy_from_slice(b"XXXX");
        std::fs::write(&p, &image).unwrap();
        assert!(opened_writes(&p), "前提: open は {name} の B の header を書き直す");
        same(&p, &format!("{name} の読めない B の header"));
    }
}

/// checkpoint の後の最初の record が読み飛ばしてよい op (知らない版の `SKIPPABLE` 付き) なら飛ばして先を見る、 解けない op
/// (`SKIPPABLE` の無い知らない op) なら再生と同じくそこで止まる (後ろの Tie は再生されない = 再生する物なし)。
#[test]
fn skipped_and_undecodable_ops_answer_like_the_full_replay() {
    let root = Scratch::new("ops");
    // (先頭の 2 件の op の種類, 3 件目, 答え)
    let cases: [(u8, u8, bool, &str); 5] = [
        (op_type::SKIPPABLE | 0x7f, op_type::COMMIT, false, "読み飛ばす op と Commit だけ"),
        (op_type::SKIPPABLE | 0x7f, op_type::TIE, true, "読み飛ばす op の後に Tie"),
        (op_type::SCHEMA, op_type::TIE, false, "解けない op の後の Tie"),
        (0x50, op_type::TIE, false, "知らない op (読み飛ばせない) の後の Tie"),
        (op_type::COMMIT, op_type::TIE, true, "Commit の後に Tie"),
    ];
    for (i, (first, third, want, what)) in cases.into_iter().enumerate() {
        let p = root.0.join(format!("ops{i}"));
        {
            let wal = OpLog::create(&p, 2 * HALF).unwrap();
            for v in 0..3 {
                tie(&wal, v);
            }
            wal.fsync().unwrap();
        }
        let mut image = std::fs::read(&p).unwrap();
        let at = 32; // A の頭 (header の後)
        retype(&mut image, at, first);
        retype(&mut image, at + TIE, first);
        if third != op_type::TIE {
            retype(&mut image, at + 2 * TIE, third);
        }
        std::fs::write(&p, &image).unwrap();
        assert_eq!(same(&p, what), Some(want), "{what}");
    }
}

/// 小さな決まった乱数 (xorshift64*)。
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// 乱数で WAL を動かす 1 歩: Tie / Commit / Alias を積む、 checkpoint を head へ進める、 畳む、 書き出す。 満杯なら checkpoint を
/// 進めて畳む。
fn step(wal: &OpLog, rng: &mut Rng, v: &mut u64) {
    let r = rng.below(100);
    let res = if r < 55 {
        *v += 1;
        wal.append(Op::Tie { eid: 1 + rng.below(4), himo_id: 0, value: *v }).map(|_| ())
    } else if r < 75 {
        wal.append(Op::Commit).map(|_| ())
    } else if r < 80 {
        wal.append(Op::Alias { eid: 3, same_as: 4 }).map(|_| ())
    } else if r < 90 {
        wal.advance_checkpoint(wal.head());
        Ok(())
    } else if r < 95 {
        wal.try_reset();
        Ok(())
    } else {
        wal.fsync()
    };
    if res.is_err() {
        wal.advance_checkpoint(wal.head());
        wal.try_reset();
    }
}

/// file の写しの byte を `n` 個、 乱数の位置で壊す (header も record も)。
fn corrupt(image: &mut [u8], rng: &mut Rng, n: usize) {
    for _ in 0..n {
        let at = rng.below(image.len() as u64) as usize;
        image[at] ^= 1 << rng.below(8);
    }
}

/// 写しの file の形: (A の版, B の版 (1 区画の形は 0))。
fn versions(file: &[u8]) -> (u32, u32) {
    let a = r32(file, 4);
    if a == 4 || a == 5 {
        let half = r32(file, 24) as usize;
        if file.len() >= half + 8 && &file[half..half + 4] == b"EWAL" {
            return (a, r32(file, half + 4));
        }
    }
    (a, 0)
}

/// 乱数で積んで畳んだ WAL を、 開き直しを挟みながら何度も写して、 そのままと byte を壊した写しで突き合わせる。 始めの形は
/// `OpLog::create` の v5 (1 区画 / 2 区画) と、 fixture 4 つ (0.36.2 の v3、 0.37.4 の v4 2 つ、 v5) を開いて (再生 → 穴の後を捨てて
/// 周を置く) 書き足す物。 v4 の fixture から書き足すと、 結ばない印の record (v4 の間)、 畳んだ A だけ v5 になった A = 5・B = 4 の
/// 混ざり、 古い区画の record を壊した写し (穴のある v4 / v5) が出る。 v3 の fixture は最初に畳む時に 2 区画になる。 v2 の file は
/// 作れない (fixture が無い) ので入れていない。 3 つの答え (true / false / 開けない) と、 形 (v3 / v4 / A = 5・B = 4 / v5) が全部出る
/// ことも確かめる。
#[test]
fn generated_wals_answer_like_the_full_replay() {
    let root = Scratch::new("gen");
    let (mut yes, mut no, mut unreadable) = (0, 0, 0);
    let mut shapes: std::collections::BTreeMap<(u32, u32), u32> = std::collections::BTreeMap::new();
    // (始めの形の名前, 容量 (fixture は None))
    let starts: Vec<(&str, Option<usize>)> = [("create-24k", Some(24 * 1024)), ("create-32k", Some(2 * HALF)), ("create-64k", Some(4 * HALF))]
        .into_iter()
        .chain(FIXTURES.iter().map(|&f| (f, None)))
        .collect();
    for (si, &(start, cap)) in starts.iter().enumerate() {
        for seed in 0..10u64 {
            let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ (seed * 1000 + si as u64 + 1));
            let p = root.0.join(format!("gen{si}-{seed}"));
            let snap = root.0.join(format!("gen{si}-{seed}.snap"));
            let mut wal = match cap {
                Some(cap) => OpLog::create(&p, cap).unwrap(),
                None => {
                    std::fs::copy(fixture_path(start), &p).unwrap();
                    let wal = OpLog::open(&p).unwrap();
                    wal.recover_with_tail();
                    wal.drop_records_after_a_hole();
                    wal
                }
            };
            let mut v = 0;
            for k in 0..240 {
                step(&wal, &mut rng, &mut v);
                if k % 60 == 59 {
                    // 開き直す (engine の開く道と同じ順: 再生 → 穴の後を捨てて周を置く)
                    drop(wal);
                    wal = OpLog::open(&p).unwrap();
                    wal.recover_with_tail();
                    wal.drop_records_after_a_hole();
                }
                if k % 4 != 3 {
                    continue;
                }
                std::fs::copy(&p, &snap).unwrap();
                let image = std::fs::read(&p).unwrap();
                *shapes.entry(versions(&image)).or_default() += 1;
                let what = format!("{start} 種 {seed} 歩 {k}");
                for a in [same(&snap, &what), {
                    let mut image = image.clone();
                    let n = 1 + rng.below(3) as usize;
                    corrupt(&mut image, &mut rng, n);
                    std::fs::write(&snap, &image).unwrap();
                    same(&snap, &format!("{what} の壊した写し"))
                }] {
                    match a {
                        Some(true) => yes += 1,
                        Some(false) => no += 1,
                        None => unreadable += 1,
                    }
                }
            }
        }
    }
    println!("答え: 再生する物あり {yes} / なし {no} / 開けない {unreadable}、 形 (A の版, B の版) ごとの写し {shapes:?}");
    assert!(yes > 100 && no > 100 && unreadable > 0, "前提: 3 つの答えが全部出る ({yes} / {no} / {unreadable})");
    for shape in [(3, 0), (4, 4), (5, 4), (5, 5), (6, 0)] {
        assert!(shapes.contains_key(&shape), "前提: 形 {shape:?} の写しが出る: {shapes:?}");
    }
}

/// 聞く読みは最初の当てる record で止まる: checkpoint の後に Tie が 1000 件あっても、 読むのは 1 件 (全部を解く再生の回数は
/// 増えない)。 数える口は thread ごと。
#[test]
fn the_probe_reads_up_to_the_first_record_to_replay() {
    let root = Scratch::new("count");
    let p = root.0.join("oplog");
    {
        let wal = OpLog::create(&p, 1 << 20).unwrap();
        wal.append(Op::Commit).unwrap();
        for v in 0..1000 {
            tie(&wal, v);
            if v % 10 == 9 {
                wal.append(Op::Commit).unwrap();
            }
        }
        wal.fsync().unwrap();
    }
    let before = wal_reads();
    let probe = OpLog::probe_replay(&p).unwrap();
    let after = wal_reads();
    assert!(probe.records);
    assert_eq!(
        (after.probes - before.probes, after.probe_records - before.probe_records, after.replays - before.replays),
        (1, 2, 0),
        "聞く読みは Commit 1 件と最初の Tie 1 件だけを読む"
    );
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(wal.recover_with_tail().len(), 1000);
    assert_eq!(wal_reads().replays - after.replays, 1);
    let at = wal_pos_offset(wal.head()) as usize;
    assert!(at > 1000 * TIE, "前提: WAL に 1000 件の Tie");
}

/// 電源断の像 (crashsim の控え: 書き出しの済んだ page だけ / page ごとに混ぜた像) を、 積む・書き出す・checkpoint を進める・畳む
/// の途中で撮って突き合わせる。
#[cfg(feature = "crashsim")]
#[test]
fn power_loss_images_answer_like_the_full_replay() {
    use enchudb_oplog::crashsim::{self, Mode};
    let root = Scratch::new("crashsim");
    let img = root.0.join("img");
    crashsim::start();
    let (mut yes, mut no) = (0, 0);
    for (ci, cap) in [24 * 1024, 2 * HALF, 4 * HALF].into_iter().enumerate() {
        for seed in 0..6u64 {
            let mut rng = Rng(0x2545_f491_4f6c_dd1d ^ (seed * 7919 + ci as u64 + 1));
            // 控えは file (inode) ごと: 消して作り直さず、 回ごとに別の置き場
            let live = root.0.join(format!("live{ci}-{seed}"));
            std::fs::create_dir_all(&live).unwrap();
            let p = live.join("oplog");
            let wal = OpLog::create(&p, cap).unwrap();
            let mut v = 0;
            for k in 0..200 {
                step(&wal, &mut rng, &mut v);
                if k % 5 != 4 {
                    continue;
                }
                for mode in [Mode::Lost, Mode::Mixed { seed: rng.next() }] {
                    let _ = std::fs::remove_dir_all(&img);
                    crashsim::capture(&live, &img, mode).unwrap();
                    match same(&img.join("oplog"), &format!("容量 {cap} 種 {seed} 歩 {k} {mode:?}")) {
                        Some(true) => yes += 1,
                        Some(false) => no += 1,
                        None => {}
                    }
                }
            }
        }
    }
    crashsim::stop();
    println!("答え: 再生する物あり {yes} / なし {no}");
    assert!(yes > 50 && no > 50, "前提: 両方の答えが出る ({yes} / {no})");
}
