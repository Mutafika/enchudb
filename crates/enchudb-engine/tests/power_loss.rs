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
//! 失敗した像は `ENCHU_POWER_LOSS_KEEP=1` で消さずに残る (`ENCHU_POWER_LOSS_VERIFY` に渡して再現)。 検証は像を開いて
//! 復旧する (oplog の再生 / bridge) ので、 撮ったままの像は隣の `imgNNN.raw` に写してある。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::engine::write_out_hook::{self, Phase};
use enchudb_engine::sync_payload_ring::PayloadRing;
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Mode};
use enchudb_oplog::oplog::{decode_sync_ops_payload, DecodedOp, OpLog, Record};
use enchudb_oplog::{eid_local, Hlc};
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
    /// `Leaf` と同じ操作を concurrent + oplog の engine に同期の API (`tie_text_to` …) で書く (#419 / #429)。
    /// consumer の周期の書き出しと、 oplog_sync を回し続ける thread が書き手と並んで走る。 像は `Lost` だけ
    LeafSync,
    /// `Oplog` と同じ書き込みを sync する DB (cell の版数 + `_sync_ops`) に書く。 oplog_sync を回し続ける thread が
    /// 書き手と並んで走る。 書き出しが返った書き込みは、 開き直して bridge し直した後の配る分 (`pending_sync_ops`) に
    /// 全部あること (相手の peer に届く)。 像は `Lost` だけ
    Sync,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Flush => "flush",
            Kind::Oplog => "oplog",
            Kind::Leaf => "leaf",
            Kind::LeafSync => "leaf_sync",
            Kind::Sync => "sync",
        }
    }
    /// 書き出しが返る batch の数
    fn batches(self) -> u32 {
        match self {
            Kind::Flush | Kind::Oplog | Kind::Sync => BATCHES,
            Kind::Leaf | Kind::LeafSync => LEAF_BATCHES,
        }
    }
    fn parse(s: &str) -> Self {
        match s {
            "flush" => Kind::Flush,
            "oplog" => Kind::Oplog,
            "leaf" => Kind::Leaf,
            "leaf_sync" => Kind::LeafSync,
            "sync" => Kind::Sync,
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
        Kind::Oplog | Kind::Sync => {
            let mut eng = if kind == Kind::Sync {
                // `_sync_ops` の枠は残りの eid 空間の半分。 全 batch の record (2.4 万) が ack 無しで入る大きさにする
                // (入らない分は ack されるまで oplog で待つ)
                Engine::create_with_cell_version(db, 262_144).unwrap()
            } else {
                Engine::create_with_capacity(db, 65_536).unwrap()
            };
            define_schema(&mut eng);
            if kind == Kind::Sync {
                eng.enable_sync_tables().unwrap();
            }
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
            // sync: 書き手と並んで oplog_sync を回し続ける thread。 bridge が本体の書き出しや畳む所と重なる (#440 の
            // 「畳む前の書き出し」 と 「畳む」 の間に bridge が入る、 #442 の 「行だけ届いた」 行ができる)
            let done = Arc::new(AtomicBool::new(false));
            let syncer = (kind == Kind::Sync).then(|| {
                let (eng, done) = (eng.clone(), done.clone());
                std::thread::spawn(move || {
                    while !done.load(Ordering::Relaxed) {
                        eng.oplog_sync().unwrap();
                    }
                })
            });
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
            done.store(true, Ordering::Relaxed);
            if let Some(s) = syncer {
                s.join().unwrap();
            }
            // WAL に載らずに落ちた record は配る分に無くてよい (floor を上げて相手に取り直させる、 #57)。 ここは落ちない
            // 大きさの oplog で、 書き出しが返った書き込みが全部配る分にあることを見る
            if kind == Kind::Sync {
                assert_eq!(eng.wal_dropped_records(), 0, "WAL に載らずに落ちた record がある (oplog が小さい)");
            }
        }
        Kind::LeafSync => write_leaf_sync(db, acked),
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

fn write_leaf_sync(db: &str, acked: &AtomicU32) {
    let mut eng = Engine::create_with_capacity(db, 65_536).unwrap();
    eng.define_table("t", 40_000).unwrap();
    for (name, vt) in LEAF_HIMOS {
        eng.define_himo_in("t", name, vt, 0).unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    // 書き手と並んで書き出し続ける thread (oplog_sync を呼ぶ別の thread)。 consumer の周期 (100 ms) だけでは書き手と
    // 重なる書き出しが少なく、 #419 の形を 3〜10 run に 1 回しか撮れなかった
    let done = Arc::new(AtomicBool::new(false));
    let syncer = {
        let (eng, done) = (eng.clone(), done.clone());
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                eng.oplog_sync().unwrap();
            }
        })
    };
    let full: Vec<String> = LEAF_HIMOS.iter().map(|(n, _)| format!("t.{n}")).collect();
    for (b, ops) in leaf_batches().into_iter().enumerate() {
        for op in ops {
            match op {
                Op::New(i) => {
                    let e = eng.entity_in("t").unwrap();
                    assert_eq!(e, u64::from(i), "eid が連番でない");
                }
                Op::Set(e, h, Val::N(v)) => eng.tie_to(u64::from(e), &full[h], v),
                Op::Set(e, h, Val::T(t)) => eng.tie_text_to(u64::from(e), &full[h], &t),
                Op::Untie(e, h) => eng.untie(u64::from(e), &full[h]),
                Op::Delete(e) => eng.delete(u64::from(e)),
            }
        }
        eng.oplog_sync().unwrap();
        acked.store(b as u32 + 1, Ordering::Release);
    }
    done.store(true, Ordering::Relaxed);
    syncer.join().unwrap();
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
        Kind::Oplog | Kind::LeafSync | Kind::Sync => Engine::open_concurrent_with_oplog(db, OPLOG_CAP),
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
    if matches!(kind, Kind::Leaf | Kind::LeafSync) {
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
    if kind == Kind::Sync {
        return verify_sync_stream(&eng, acked);
    }
    String::new()
}

/// sync する DB: 書き出しが返った書き込みは、 開き直して bridge し直した後の配る分 (`pending_sync_ops`) に全部ある。
fn verify_sync_stream(eng: &Engine, acked: u32) -> String {
    use enchudb_oplog::oplog::{decode_sync_ops_payload, DecodedOp};
    if acked == 0 {
        return String::new(); // 書き出しが返った書き込みが無い (列もまだ届いていないことがある)
    }
    if let Err(e) = eng.oplog_sync() {
        return format!("開いた後の oplog_sync が失敗: {e}");
    }
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    let hids: Vec<u16> = (0..HIMOS).map(|h| eng.himo_id(&format!("t.h{h}")).unwrap() as u16).collect();
    let mut have = std::collections::HashSet::new();
    let (mut rows, mut unreadable) = (0, 0);
    for payload in eng.pending_sync_ops(0) {
        rows += 1;
        match decode_sync_ops_payload(&payload) {
            Some(rec) => {
                if let DecodedOp::Tie { eid, himo_id, value } = rec.op {
                    have.insert((eid, himo_id, value));
                }
            }
            None => unreadable += 1,
        }
    }
    for i in 0..acked * BATCH {
        for (h, &hid) in hids.iter().enumerate() {
            if !have.contains(&(u64::from(i), hid, u64::from(value_of(i, h as u32)))) {
                return format!(
                    "書き出しが返った書き込みが配る分に無い: eid={i} h{h} (acked batch {acked}、 配る分 {rows} 行 / Tie {} 件、 \
                     読めない payload {unreadable}、 壊れた行の掃除 {}、 sync lsn {})",
                    have.len(),
                    eng.sync_dead_rows_purged(),
                    eng.current_sync_lsn()
                );
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
        let mode = if matches!(kind, Kind::Leaf | Kind::LeafSync | Kind::Sync) || n.is_multiple_of(2) {
            Mode::Lost
        } else {
            Mode::Mixed { seed: n }
        };
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
        // 検証の子は像を開いて復旧する (oplog の再生 / bridge) ので、 残す像は検証の前に写しを取っておく
        let raw = img.dir.with_extension("raw");
        if keep {
            let st = Command::new("cp").arg("-R").arg(&img.dir).arg(&raw).status().expect("cp");
            assert!(st.success(), "像を写せない: {}", img.dir.display());
        }
        let (err, was_unborn) = verify_in_child(kind, &img.dir.join("db"), img.acked);
        // 作り終える前の像 (開けないが 「作成中」 と言い、 作り直せた) は数えるだけ
        if was_unborn {
            unborn += 1;
        }
        if err.is_empty() {
            let _ = std::fs::remove_dir_all(&img.dir);
            let _ = std::fs::remove_dir_all(&raw);
        } else {
            failures.push(format!(
                "{} ({:?}, acked {}): {err}\n    失われうる page: {:?}{}",
                img.dir.display(),
                img.mode,
                img.acked,
                img.divergent,
                if keep { format!("\n    検証の前の像: {}", raw.display()) } else { String::new() }
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

/// 同期の書き手 (`tie_text_to` …) と並んで書き出し (consumer の周期 + oplog_sync を回し続ける thread) が走る。
/// #429 (再生が untie だけを当てて書き直した Leaf を消す) を 3〜10 run に 1 回撮った。 #419 (cell が中身より先に届く)
/// はこの形ではまれにしか撮れない (修正前で 10 run 中 0) — 書き出しの順は `write_out_never_persists_a_cell_without_its_content`
/// が決定的に確かめる。
#[test]
fn power_loss_keeps_values_rewritten_by_sync_writers() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    run(Kind::LeafSync);
}

/// #441: sync の payload の ring は作った時に header を書き出す。 作った直後 (最初の本体の書き出しの前) に電源が落ちても
/// ring を開ける (旧: header の無い file が残り、 以後ずっと bad magic で開けず、 payload を辞書に置いた)。
#[test]
fn sync_payload_ring_header_is_written_out_when_created() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("ring_header");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let ring = PayloadRing::open_or_create(&live, 1 << 20, false).unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(ring);
    captured.unwrap();
    let opened = PayloadRing::open_or_create(&img, 1 << 20, true);
    let _ = std::fs::remove_dir_all(&root);
    assert!(opened.is_ok(), "作った直後の像で ring を開けない: {:?}", opened.err());
}

#[test]
fn power_loss_keeps_sync_records_of_synced_batches() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    run(Kind::Sync);
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

/// 別の thread の `oplog_sync` を oplog を写した直後 (fsync の中、 本体の書き出しの前) で止め、 その間に `during` を
/// 走らせてから続けさせる (返るまで待つ)。
fn with_stalled_sync(eng: &Arc<Engine>, during: impl FnOnce()) {
    let (copied_tx, copied_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let stall = std::sync::Mutex::new(Some((copied_tx, go_rx)));
    crashsim::set_after_copy_hook(Some(Arc::new(move || {
        if std::thread::current().name() != Some("stalled-sync") {
            return;
        }
        let taken = stall.lock().unwrap().take();
        if let Some((copied, go)) = taken {
            copied.send(()).unwrap();
            go.recv().unwrap();
        }
    })));
    let stalled = {
        let eng = eng.clone();
        std::thread::Builder::new().name("stalled-sync".into()).spawn(move || eng.oplog_sync().unwrap()).unwrap()
    };
    copied_rx.recv().unwrap();
    during();
    go_tx.send(()).unwrap();
    stalled.join().unwrap();
    crashsim::set_after_copy_hook(None);
}

/// #446: oplog の書き出しが重なって、 先に写した方が後で終わっても、 控えを古い写しへ戻さない。
///
/// oplog の fsync は consumer の周期 / `oplog_sync` の呼び手が同時に呼ぶ。 crashsim は fsync の前に写した中身を
/// 書き出しが返った後に控えに置くので、 先に写した方が後で終わると、 控えが後の書き出しより前の中身に戻っていた。
/// 像を開くと古い控えの checkpoint から再生し、 書き出しの返った untie / 書き直しの前の値を当て直した (Linux の
/// LeafSync が 54 run 中 5 回落ちた。 本物の msync はディスクの中身を古い方へ戻さない)。 ここは別の thread の
/// `oplog_sync` を oplog を写した直後で止め、 その間に untie して `oplog_sync` を返らせてから、 止めた方を終わらせて
/// 像を撮る。
#[test]
fn overlapping_oplog_writes_do_not_roll_back_what_a_later_write_persisted() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("overlap");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "lf", ValueType::Leaf, 0).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    let e = eng.entity_in("t").unwrap();
    eng.tie_text_to(e, "t.lf", "old");
    eng.oplog_sync().unwrap();
    eng.tie_text_to(e, "t.lf", "new");
    with_stalled_sync(&eng, || {
        eng.untie(e, "t.lf");
        // 書き出しが返った = この untie は電源断の後も残る
        eng.oplog_sync().unwrap();
    });
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    let got = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP)
        .map(|eng| eng.get_text_owned(e, "t.lf").map(|b| String::from_utf8_lossy(&b).into_owned()));
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(got.unwrap(), None, "書き出しの返った untie が電源断の像で消え、 前の値が戻った");
}

/// #451 の DB: sync する (cell の版数あり)、 note (Number) / tag (Tag) / body (Leaf)、 peer 1。
fn notes_engine(db: &Path, oplog_cap: usize) -> Arc<Engine> {
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("notes", 1_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "tag", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, oplog_cap).unwrap();
    eng.set_peer_id(1);
    eng
}

/// 配る分 (bridge し直した後の `pending_sync_ops`) の record。
fn distributed(eng: &Engine) -> Vec<Record> {
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    eng.pending_sync_ops(0).iter().filter_map(|p| decode_sync_ops_payload(p)).collect()
}

fn floor_of(eng: &Engine, author: u32) -> Option<Hlc> {
    eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == author).map(|(_, h)| h)
}

/// #451: oplog の fsync と本体の書き出しの間に書いた write は、 電源断の後に本体にだけ残る (oplog に無い)。 開く時に
/// 作り直して配る分に載せる (旧: 本体にあるのに相手に永久に届かなかった)。 値 (Number / Tag の新しい語 / Leaf)・
/// untie・delete。 値と untie は新しい HLC で作り直して cell の版数もそれに上げ、 delete は元の HLC のまま。
///
/// 別の thread の `oplog_sync` を oplog を写した直後で止め、 その間に書き、 止めた方の本体の書き出しで届かせてから像を
/// 撮る。 その間に consumer の周期の書き出しが oplog も届かせると前提が崩れるので、 像の oplog にその record が無いことを
/// 確かめ、 崩れていたら撮り直す。
#[test]
fn writes_on_disk_before_their_oplog_record_are_relogged_after_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for attempt in 0..5 {
        let root = scratch(&format!("unlogged{attempt}"));
        let live = root.join("live");
        std::fs::create_dir_all(&live).unwrap();
        crashsim::start();
        let eng = notes_engine(&live.join("db"), OPLOG_CAP);
        let [a, b, c, d, e] = [(); 5].map(|_| eng.entity_in("notes").unwrap());
        eng.tie_to(a, "notes.note", 1);
        eng.tie_to(e, "notes.note", 5);
        eng.oplog_sync().unwrap();
        with_stalled_sync(&eng, || {
            eng.tie_to(b, "notes.note", 22);
            eng.tie_text_to(c, "notes.tag", "fresh-word");
            eng.tie_text_to(d, "notes.body", "leaf body");
            eng.untie(a, "notes.note");
            eng.delete(e);
        });
        let img = root.join("img");
        let captured = crashsim::capture(&live, &img, Mode::Lost);
        crashsim::stop();
        let hid = |n: &str| eng.himo_id(n).unwrap() as u16;
        let (note, tag, body) = (hid("notes.note"), hid("notes.tag"), hid("notes.body"));
        let before = [eng.cell_hlc(b, note), eng.cell_hlc(c, tag), eng.cell_hlc(d, body), eng.cell_hlc(a, note)];
        let deleted_at = eng.tombstone_hlc(e);
        drop(eng);
        captured.unwrap();
        let db = img.join("db");
        // 前提: 像の oplog にこの write の record が無い
        let in_oplog = OpLog::open(&db.join("oplog"))
            .unwrap()
            .records_with_tail()
            .iter()
            .filter(|r| before.contains(&r.hlc) || r.hlc == deleted_at)
            .count();
        if in_oplog > 0 {
            eprintln!("[unlogged] 撮り直す ({attempt} 回目): consumer の書き出しが先に oplog を届かせた");
            let _ = std::fs::remove_dir_all(&root);
            continue;
        }
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        let text = |e: u64, n: &str| eng.get_text_owned(e, n).map(|t| String::from_utf8_lossy(&t).into_owned());
        // 前提: 本体には届いている
        assert_eq!(eng.get(b, "notes.note"), Some(22), "前提: 本体に届いている");
        assert_eq!(text(c, "notes.tag").as_deref(), Some("fresh-word"));
        assert_eq!(text(d, "notes.body").as_deref(), Some("leaf body"));
        assert_eq!(eng.get(a, "notes.note"), None);
        assert!(!eng.is_live(e));
        assert_eq!(eng.unlogged_writes_relogged(), 5, "本体にあって oplog に無い write を作り直していない");
        let after = [eng.cell_hlc(b, note), eng.cell_hlc(c, tag), eng.cell_hlc(d, body), eng.cell_hlc(a, note)];
        assert!(before.iter().zip(&after).all(|(x, y)| y > x), "値と untie は新しい HLC: {before:?} → {after:?}");
        let recs = distributed(&eng);
        let find = |pred: &dyn Fn(&DecodedOp) -> bool| recs.iter().find(|r| pred(&r.op)).map(|r| r.hlc);
        let local = |x: u64, y: &u64| eid_local(x) == eid_local(*y);
        let vid = eng.get(c, "notes.tag").unwrap();
        assert_eq!(
            find(&|op| matches!(op, DecodedOp::Tie { eid, himo_id, value } if local(b, eid) && *himo_id == note && *value == 22)),
            Some(after[0]),
            "Number の値が配る分に無い"
        );
        let vocab_hlc = find(&|op| matches!(op, DecodedOp::Vocab { bytes, .. } if bytes == b"fresh-word"));
        let tie_hlc = find(&|op| matches!(op, DecodedOp::Tie { eid, himo_id, value } if local(c, eid) && *himo_id == tag && *value == vid));
        assert!(vocab_hlc.is_some() && tie_hlc == Some(after[1]) && vocab_hlc < tie_hlc, "Tag の語と値: {vocab_hlc:?} {tie_hlc:?}");
        assert_eq!(
            find(&|op| matches!(op, DecodedOp::TieLeaf { eid, himo_name, bytes, .. } if local(d, eid) && himo_name == "notes.body" && bytes == b"leaf body")),
            Some(after[2]),
            "Leaf の値が配る分に無い"
        );
        assert_eq!(
            find(&|op| matches!(op, DecodedOp::Untie { eid, himo_id } if local(a, eid) && *himo_id == note)),
            Some(after[3]),
            "untie が配る分に無い"
        );
        assert_eq!(find(&|op| matches!(op, DecodedOp::Delete { eid } if local(e, eid))), Some(deleted_at), "delete は元の HLC で");
        assert_eq!(floor_of(&eng, 1), None, "作り直せたので floor は上げない");
        drop(eng);
        let _ = std::fs::remove_dir_all(&root);
        return;
    }
    panic!("5 回とも consumer の書き出しが先に oplog を届かせ、 前提を作れなかった");
}

/// #451: 本体にある write が全部 oplog か配る分 (`_sync_ops`) にある電源断の像と、 きれいに閉じた後は、 何も作り直さない
/// (配った record を新しい HLC で重ねて配らない)。 像は 2 枚: `oplog_sync` の直後 (record は oplog にだけある — 写した
/// `_sync_ops` の行はまだ書き出していない) と、 oplog を畳んだ後 (record は `_sync_ops` にだけある)。
#[test]
fn nothing_is_relogged_when_every_write_on_disk_is_in_the_oplog() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("logged");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let eng = notes_engine(&db, OPLOG_CAP);
    let [a, b, c] = [(); 3].map(|_| eng.entity_in("notes").unwrap());
    eng.tie_to(a, "notes.note", 1);
    eng.tie_text_to(b, "notes.tag", "word");
    eng.tie_text_to(c, "notes.body", "leaf");
    eng.oplog_sync().unwrap();
    eng.untie(a, "notes.note");
    eng.delete(b);
    eng.oplog_sync().unwrap();
    let in_oplog = crashsim::capture(&live, &root.join("img_oplog"), Mode::Lost);
    // 畳ませる
    let wal = eng.oplog().unwrap().clone();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while wal.head() != enchudb_oplog::oplog::HEADER_SIZE as u64 {
        assert!(std::time::Instant::now() < deadline, "前提: 5 秒で oplog を畳まない (head {})", wal.head());
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    drop(wal);
    // 畳んだ ring (先頭に戻した header) を書き出す。 書き出すまでディスクの oplog には畳む前の record が残っている
    eng.oplog_sync().unwrap();
    let in_sync_ops = crashsim::capture(&live, &root.join("img_sync_ops"), Mode::Lost);
    crashsim::stop();
    drop(eng);
    for (name, captured) in [("img_oplog", in_oplog), ("img_sync_ops", in_sync_ops)] {
        captured.unwrap();
        let eng = Engine::open_concurrent_with_oplog(root.join(name).join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
        assert_eq!(eng.unlogged_writes_relogged(), 0, "{name}: 届いている write を作り直した");
        assert_eq!(floor_of(&eng, 1), None, "{name}");
    }
    // きれいに閉じた後は調べない
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.unlogged_writes_relogged(), 0, "きれいに閉じた後に作り直した");
    assert_eq!(floor_of(&eng, 1), None);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #451 / #449: WAL が満杯で載らなかった write (#57) は、 floor を上げる前に電源が落ちると本体にだけ残る。 開く時に
/// 作り直そうとしても oplog が満杯なら、 floor を上げて相手に取り直させる (floor = 開いた時の今 は落ちた write を覆う)。
#[test]
fn writes_dropped_from_a_full_wal_are_covered_by_a_floor_after_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("dropped");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let eng = notes_engine(&live.join("db"), 64 * 1024);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let wal = eng.oplog().unwrap().clone();
    // 満杯の書き手を待たせず (#388) 畳ませずに、 WAL に載らない write を作る
    wal.set_room_waiter(None);
    let mut dropped = Vec::new();
    let img = root.join("img");
    let captured = {
        let _no_fold = eng.transfer_lock_for_fold();
        // 閉じた group で WAL を埋め、 残りを 100 B (Tie 128 B も Commit 112 B も入らない) にする。 TieLeaf の record は
        // 文字列の長さ + c B
        let e0 = eng.entity_in("notes").unwrap();
        let h0 = wal.head();
        eng.tie_text_to(e0, "notes.body", "x");
        let c = wal.head() - h0 - 1;
        eng.oplog_commit();
        let pad = wal.free_bytes() - 112 - 100 - c;
        let e1 = eng.entity_in("notes").unwrap();
        eng.tie_text_to(e1, "notes.body", &"p".repeat(pad as usize));
        eng.oplog_commit();
        assert_eq!(wal.free_bytes(), 100, "前提: 残り 100 B");
        for i in 0..5u32 {
            let head = wal.head();
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
            assert_eq!(wal.head(), head, "前提: WAL に載らない");
            dropped.push(eng.cell_hlc(e, note));
        }
        // 落ちた write は本体にだけ届く (floor を上げる bridge は畳む lock で止めている)
        wal.fsync().unwrap();
        eng.body_msync().unwrap();
        crashsim::capture(&live, &img, Mode::Lost)
    };
    crashsim::stop();
    drop((wal, eng));
    captured.unwrap();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), 64 * 1024).unwrap();
    let max_dropped = dropped.into_iter().max().unwrap();
    let floor = floor_of(&eng, 1);
    assert!(floor.is_some_and(|f| f >= max_dropped), "落ちた write {max_dropped:?} を floor {floor:?} が覆わない");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #419: 本体の書き出し (`body_msync`) の途中で並行の書き手が中身と cell を書いても、 cell だけがディスクに届くことは
/// 無い。 書き出しを段の切れ目で止め (`write_out_hook`)、 その間に書き手に書かせて電源断の像 (`Lost`) を撮る (決定的)。
///
/// - 中身の segment を書き出した後 (門を閉じる前) で止めて書かせる: その時点の像と書き出しの後の像の両方で、 cell は
///   書き出す前の値か、 中身ごと届いた新しい値 (旧: 列を中身のすぐ後に書き出していたので、 中身の後で書いた cell だけが
///   届いた)
/// - 門を閉じた後で止めて書かせる: 書き手は門で待つので、 その cell は書き出しに入らない
#[test]
fn write_out_never_persists_a_cell_without_its_content() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for stop_at in [Phase::BeforeGate, Phase::GateClosed] {
        for himo in ["t.lf", "t.tg"] {
            write_during_write_out(stop_at, himo);
        }
    }
}

fn write_during_write_out(stop_at: Phase, himo: &'static str) {
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::time::Duration;
    const NEW: &str = "a new value, longer than the old one";
    let root = scratch(&format!("write_out_{stop_at:?}_{}", &himo[2..]));
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "lf", ValueType::Leaf, 0).unwrap();
    eng.define_himo_in("t", "tg", ValueType::Tag, 0).unwrap();
    let e = eng.entity_in("t").unwrap();
    eng.tie_text(e, himo, "old");
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Arc::new(eng);

    // stop_at で止めて書き手に書かせる。 門を閉じる前で止める時は、 書かせた後 (まだ門の前) と門を閉じた直後の像も撮る
    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (wrote_tx, wrote_rx) = mpsc::channel::<()>();
    let (paused_tx, go_rx, wrote_rx) = (Mutex::new(paused_tx), Mutex::new(go_rx), Mutex::new(wrote_rx));
    let (before_gate, closed) = (root.join("before_gate"), root.join("closed"));
    {
        let (live, before_gate, closed) = (live.clone(), before_gate.clone(), closed.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == stop_at {
                paused_tx.lock().unwrap().send(()).unwrap();
                go_rx.lock().unwrap().recv_timeout(Duration::from_secs(10)).expect("書き出しを進める合図が来ない");
                if p == Phase::BeforeGate {
                    crashsim::capture(&live, &before_gate, Mode::Lost).unwrap();
                }
            }
            if p == Phase::Closed && stop_at == Phase::BeforeGate {
                crashsim::capture(&live, &closed, Mode::Lost).unwrap();
            }
            // 門で待っていた書き手が書き終えてから残りを書き出す (残りに中身を指す列が混ざれば、 その cell が届く)
            if p == Phase::Opened && stop_at == Phase::GateClosed {
                wrote_rx.lock().unwrap().recv_timeout(Duration::from_secs(10)).expect("門を開いた後も書き手が書かない");
            }
        })));
    }
    let syncer = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.body_msync().unwrap())
    };
    paused_rx.recv_timeout(Duration::from_secs(10)).expect("書き出しが止まらない");
    let writer = {
        let eng = eng.clone();
        std::thread::spawn(move || {
            eng.tie_text_to(e, himo, NEW);
            let _ = wrote_tx.send(());
        })
    };
    let mut writer = Some(writer);
    if stop_at == Phase::BeforeGate {
        writer.take().unwrap().join().unwrap(); // 門は開いている = すぐ書ける
    } else {
        std::thread::sleep(Duration::from_millis(50)); // 門で待つ (門が無ければこの間に書く)
    }
    go_tx.send(()).unwrap();
    syncer.join().unwrap();
    let after = root.join("after");
    crashsim::capture(&live, &after, Mode::Lost).unwrap();
    write_out_hook::set(None);
    if let Some(w) = writer {
        w.join().unwrap();
    }
    crashsim::stop();

    for img in [before_gate, closed, after] {
        if !img.exists() {
            continue;
        }
        let got = {
            let opened = Engine::open_standalone(img.join("db").to_str().unwrap()).unwrap();
            opened.get_text_owned(e, himo).map(|b| String::from_utf8_lossy(&b).into_owned())
        };
        assert!(
            got.as_deref() == Some("old") || got.as_deref() == Some(NEW),
            "{stop_at:?} で止めて {himo} を書いた像 {}: cell が中身より先に届いた (got {got:?})",
            img.file_name().unwrap().to_string_lossy()
        );
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #431: oplog の fsync の後・本体の書き出しの前に電源が落ちても、 新しい語を書いた Tag の cell は開き直した後に
/// その語を読む (再生が自分の Vocab を当てる)。 oplog だけを fsync して像を撮る (consumer の周期の書き出しが先に
/// 辞書を書き出していたら撮り直す)。 旧実装: `Some("")` (cell は語の番号を指すが、 辞書に語が無い)。
#[test]
fn new_tag_word_survives_power_loss_before_write_out() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for attempt in 0..20 {
        let root = scratch(&format!("vocab_replay_{attempt}"));
        let live = root.join("live");
        std::fs::create_dir_all(&live).unwrap();
        let db = live.join("db");
        crashsim::start();
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 100).unwrap();
        eng.define_himo_in("t", "tg", ValueType::Tag, 0).unwrap();
        let e = eng.entity_in("t").unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        let word = format!("a-word-only-in-the-oplog-{attempt}");
        eng.tie_text_to(e, "t.tg", &word);
        eng.oplog_commit();
        eng.oplog().unwrap().fsync().unwrap();
        let img = root.join("img");
        let st = crashsim::capture(&live, &img, Mode::Lost);
        crashsim::stop();
        st.unwrap();
        drop(eng);
        let vocab = std::fs::read(img.join("db").join("vocab.data.seg")).unwrap();
        if vocab.windows(word.len()).any(|w| w == word.as_bytes()) {
            // consumer の周期の書き出しが先に辞書を書き出した (前提が作れていない)
            let _ = std::fs::remove_dir_all(&root);
            continue;
        }
        let got = {
            let opened = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
            opened.get_text_owned(e, "t.tg").map(|b| String::from_utf8_lossy(&b).into_owned())
        };
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(got.as_deref(), Some(word.as_str()), "oplog にだけ在った語を再生で辞書に戻していない");
        return;
    }
    panic!("前提を作れなかった (20 回とも、 像を撮る前に辞書が書き出された)");
}
