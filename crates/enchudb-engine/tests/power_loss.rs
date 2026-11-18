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
use enchudb_engine::engine::ConsumerHold;
use enchudb_engine::sync_payload_ring::PayloadRing;
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
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
    /// `Oplog` を小さい WAL (0.37 の 2 区画、 1 区画 64 KiB) で: batch 1 つが区画を越え、 書いている間に区画を何十回も切り替えて
    /// 古い区画を畳む。 どの像でも、 古い区画 → 新しい区画の順に再生して書き出しの返った batch が全部ある
    OplogHalves,
    /// `Sync` を小さい WAL (1 区画 128 KiB) で: bridge が区画をまたいで読み、 読み終えた古い区画だけを畳む
    SyncHalves,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Flush => "flush",
            Kind::Oplog => "oplog",
            Kind::Leaf => "leaf",
            Kind::LeafSync => "leaf_sync",
            Kind::Sync => "sync",
            Kind::OplogHalves => "oplog_halves",
            Kind::SyncHalves => "sync_halves",
        }
    }
    /// sync する DB か
    fn sync(self) -> bool {
        matches!(self, Kind::Sync | Kind::SyncHalves)
    }
    /// 書き手の WAL の大きさ
    fn oplog_cap(self) -> usize {
        match self {
            Kind::OplogHalves => 128 * 1024,
            Kind::SyncHalves => 256 * 1024,
            _ => OPLOG_CAP,
        }
    }
    /// 書き出しが返る batch の数
    fn batches(self) -> u32 {
        match self {
            Kind::Flush | Kind::Oplog | Kind::Sync | Kind::OplogHalves | Kind::SyncHalves => BATCHES,
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
            "oplog_halves" => Kind::OplogHalves,
            "sync_halves" => Kind::SyncHalves,
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

/// sync する DB の書き手が、 行を積む前に WAL の空きを待つ (状態で待つ。 上限は前提の誤りを知らせるだけ)。
///
/// sync する DB は、 満杯の WAL を consumer が畳めないと分かった時 / 5 秒待って空かなかった時に、 書き込みを WAL に載せずに
/// 落として相手に取り直させる (#57)。 この試験が見るのは 「書き出しが返った書き込みが全部配る分にある」 で、 1 件も落ちない事が
/// 前提 — 前は 「consumer が書き手の待ちの上限より速く畳む」 に頼っていて、 load の高い機械で前提の assert が落ちた (#735)。
/// 書き手が自分で空きを待てば、 engine の待ち (と、 その諦め) に入らない。 区画を移る・古い区画を畳む・bridge が区画を
/// またいで読むのは変わらない (`wal_switches() >= 10` の assert はそのまま)。
fn wait_for_wal_room(eng: &Engine) {
    /// 1 行 (Tie 3 つ) と、 並んで積まれる Commit の分より十分大きい空き
    const ROOM: u64 = 16 * 1024;
    let wal = eng.oplog().unwrap();
    let until = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while wal.free_bytes() < ROOM {
        assert!(std::time::Instant::now() < until, "前提: WAL が 120 秒空かない (consumer が古い区画を畳まない)");
        std::thread::sleep(std::time::Duration::from_micros(200));
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
        Kind::Oplog | Kind::Sync | Kind::OplogHalves | Kind::SyncHalves => {
            let mut eng = if kind.sync() {
                // `_sync_ops` の枠は残りの eid 空間の半分。 全 batch の record (2.4 万) が ack 無しで入る大きさにする
                // (入らない分は ack されるまで oplog で待つ)
                Engine::create_with_cell_version(db, 262_144).unwrap()
            } else {
                Engine::create_with_capacity(db, 65_536).unwrap()
            };
            define_schema(&mut eng);
            if kind.sync() {
                eng.enable_sync_tables().unwrap();
            }
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            let eng = Engine::concurrentize_with_oplog(eng, kind.oplog_cap()).unwrap();
            // sync: 書き手と並んで oplog_sync を回し続ける thread。 bridge が本体の書き出しや畳む所と重なる (#440 の
            // 「畳む前の書き出し」 と 「畳む」 の間に bridge が入る、 #442 の 「行だけ届いた」 行ができる)
            let done = Arc::new(AtomicBool::new(false));
            let syncer = kind.sync().then(|| {
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
                    if kind.sync() {
                        wait_for_wal_room(&eng);
                    }
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
            if kind.sync() {
                assert_eq!(eng.wal_dropped_records(), 0, "WAL に載らずに落ちた record がある (oplog が小さい)");
            }
            if matches!(kind, Kind::OplogHalves | Kind::SyncHalves) {
                assert!(eng.wal_switches() >= 10, "前提: 書いている間に区画を何度も切り替える ({} 回)", eng.wal_switches());
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
        Kind::Oplog | Kind::LeafSync | Kind::Sync | Kind::OplogHalves | Kind::SyncHalves => {
            Engine::open_concurrent_with_oplog(db, kind.oplog_cap())
        }
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
    if kind.sync() {
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
        let mode = if matches!(kind, Kind::Leaf | Kind::LeafSync | Kind::Sync | Kind::SyncHalves) || n.is_multiple_of(2) {
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

/// 0.37: 2 区画の WAL を何度も切り替え、 古い区画を書き出して畳みながら書く (切り替え・畳み・区画をまたぐ再生の途中の像)。
#[test]
fn power_loss_keeps_oplog_synced_batches_across_wal_halves() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    run(Kind::OplogHalves);
}

/// #663: 区画を移る時、 新しい区画に書く前に古い区画の header (最後の head) を書き出す。 書き出し (fsync) を作った時の 1 度しか
/// しないまま区画を移り、 新しい区画にも書いた像 (書き出しの済んだ page だけ) でも、 古い区画の header は最後の head を持つ。 前は
/// 最後に書き出した時の header のままで、 古い区画の終わりの record が届かず新しい区画の record は届いた像 (OS が先に書いた page)
/// で、 開いた後の読みが古い区画を前の head で読み終えて新しい区画へ進み、 間を飛ばして当てた (`changefeed_log_power_loss` で値が
/// 飛ぶ形)。
#[test]
fn switching_wal_halves_writes_out_the_old_header_first() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("switch_header");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let path = live.join("oplog");
    let wal = OpLog::create(&path, 64 * 1024).unwrap();
    assert!(wal.two_halves(), "前提: 2 区画の WAL");
    wal.fsync().unwrap();
    let mut v = 0u64;
    while wal.switches() == 0 {
        wal.append(enchudb_oplog::oplog::Op::Tie { eid: 1, himo_id: 0, value: v }).unwrap();
        v += 1;
    }
    for _ in 0..10 {
        wal.append(enchudb_oplog::oplog::Op::Tie { eid: 1, himo_id: 0, value: v }).unwrap();
        v += 1;
    }
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(wal);
    captured.unwrap();
    let head = |p: &Path| u64::from_le_bytes(std::fs::read(p).unwrap()[8..16].try_into().unwrap());
    let (live_head, img_head) = (head(&path), head(&img.join("oplog")));
    let _ = std::fs::remove_dir_all(&root);
    assert!(live_head > enchudb_oplog::oplog::HEADER_SIZE as u64, "前提: 古い区画に書いた");
    assert_eq!(img_head, live_head, "区画を移った後の像で、 古い区画の header が最後の head を持たない");
}

/// 0.37: sync する DB で、 bridge が区画をまたいで読み、 読み終えた古い区画だけを畳む途中の像でも、 書き出しが返った書き込みは
/// 配る分に全部ある。
#[test]
fn power_loss_keeps_sync_records_across_wal_halves() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    run(Kind::SyncHalves);
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
///
/// consumer も、 次に oplog を写した所で止め、 返した [`ConsumerHeld`] を drop するまで止めておく。 止めないと consumer の
/// 周期の書き出しが `during` の write の record を oplog ごと届かせることがある (CI の少ない CPU では毎回届いた)。 止める
/// 所 (周期の oplog の fsync) は lock を持たない。 WAL の空き作り (#388) は append の lock を持ったまま書き出すので、 WAL
/// が埋まる試験では使わないこと。 `during` で queue の write (`tie_async` …) を待つと止まる。
///
/// 止めるのは oplog 全体の fsync の写し (`Copied::Oplog`) だけ。 header を届ける書き出し (`Copied::OplogHeader`) は、 書き手が
/// `append_lock` を持ったまま (畳んだ区画に書く前) か、 consumer が畳んだ直後 (bridge の lock を持ったまま) に写す。 そこで
/// 止めると `during` の書き込みが `append_lock` を待ち続ける (#1000、 [`a_stalled_sync_does_not_wait_for_a_fold_header_being_written_out`])。
fn with_stalled_sync(eng: &Arc<Engine>, during: impl FnOnce()) -> ConsumerHeld {
    let (copied_tx, copied_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let stall = std::sync::Mutex::new(Some((copied_tx, go_rx)));
    let hold = std::sync::Mutex::new(Some(release_rx));
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
        if what != Copied::Oplog {
            return;
        }
        match std::thread::current().name() {
            Some("stalled-sync") => {
                let taken = stall.lock().unwrap().take();
                if let Some((copied, go)) = taken {
                    copied.send(()).unwrap();
                    go.recv().unwrap();
                }
            }
            Some("enchudb-consumer") => {
                let taken = hold.lock().unwrap().take();
                if let Some(release) = taken {
                    let _ = release.recv();
                }
            }
            _ => {}
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
    ConsumerHeld(Some(release_tx))
}

/// [`with_stalled_sync`] が止めた consumer を、 drop で放す (hook も外す)。 像を撮った後、 engine を drop する前に。
struct ConsumerHeld(Option<std::sync::mpsc::Sender<()>>);

impl Drop for ConsumerHeld {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
        crashsim::set_after_copy_hook(None);
    }
}

/// #1000: consumer が暇な WAL を畳み、 畳んだ区画の header を届けている途中 (遅い disk の msync の間) に [`with_stalled_sync`] を
/// 始めても、 互いに待たない。
///
/// 止めた `oplog_sync` の Commit は、 畳んだ区画に書く前にその header を自分で届ける (`alloc_after_switch`、 `append_lock` の
/// 下)。 前はその写しも全体の fsync と同じ `Copied::Oplog` で、 hook がそこで止めた = `append_lock` を持ったまま止まり、 `during`
/// の書き込みが lock を待ち続けた (CI の `writes_after_a_durable_mark_are_still_relogged_after_power_loss` で 2 時間。 consumer
/// は同じ hook で畳んだ header の写しに止まっていた)。 遅い msync の代わりに、 consumer を畳んだ header の写し (WAL が空の時に
/// consumer が oplog を写すのはここだけ、 種類は問わない) で止めて窓を開けておく = 毎回この順になる。 止まった時に落ちるように、
/// 本体は別の thread で回して上限を付ける (止まった thread は放っておく)。
#[test]
fn a_stalled_sync_does_not_wait_for_a_fold_header_being_written_out() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let body = std::thread::spawn(move || {
        let root = scratch("fold_header");
        let live = root.join("live");
        std::fs::create_dir_all(&live).unwrap();
        crashsim::start();
        let eng = notes_engine(&live.join("db"), OPLOG_CAP);
        let wal = eng.oplog().unwrap().clone();
        let [a, b] = [(); 2].map(|_| eng.entity_in("notes").unwrap());
        // consumer を、 暇な WAL を畳んだ後の header の写し (書き出しの前) で止める。 書いた後に周期が書き出して畳む (試験は
        // 何も積まない: 積むと畳んだ区画の header を試験の thread が届けてしまう)。 hook を置く前に畳み終えていたら書き直す
        let mut release = None;
        for i in 0..5u32 {
            eng.tie_to(a, "notes.note", i);
            let lap = wal.lap();
            let (parked_tx, parked_rx) = std::sync::mpsc::channel::<()>();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let fold = std::sync::Mutex::new(Some((parked_tx, release_rx)));
            let w = wal.clone();
            crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
                if what != Copied::Segment
                    && std::thread::current().name() == Some("enchudb-consumer")
                    && w.is_empty()
                    && w.lap() != lap
                {
                    let taken = fold.lock().unwrap().take();
                    if let Some((parked, release)) = taken {
                        let _ = parked.send(());
                        let _ = release.recv();
                    }
                }
            })));
            if parked_rx.recv_timeout(std::time::Duration::from_secs(5)).is_ok() {
                assert!(wal.is_empty() && wal.lap() != lap, "前提: 畳んだ");
                release = Some(release_tx);
                break;
            }
        }
        let release_tx = release.expect("前提: consumer が暇な WAL を畳んで、 その header の写しで止まる");
        let held = with_stalled_sync(&eng, || {
            // 止めた sync の bridge が consumer の持つ lock を待たないように放す (consumer は header を届け終え、 次の周期で
            // with_stalled_sync の hook に止まる)
            release_tx.send(()).unwrap();
            eng.tie_to(b, "notes.note", 2);
        });
        assert_eq!(eng.get(b, "notes.note"), Some(2));
        drop(held);
        crashsim::stop();
        drop(eng);
        let _ = std::fs::remove_dir_all(&root);
        let _ = done_tx.send(());
    });
    match done_rx.recv_timeout(std::time::Duration::from_secs(90)) {
        Ok(()) => body.join().unwrap(),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => std::panic::resume_unwind(body.join().unwrap_err()),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            crashsim::set_after_copy_hook(None);
            crashsim::stop();
            panic!("止めた oplog_sync と試験の書き込みが 90 秒互いに待った (畳んだ header の写しで止めた、 #1000)");
        }
    }
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
    let held = with_stalled_sync(&eng, || {
        eng.untie(e, "t.lf");
        // 書き出しが返った = この untie は電源断の後も残る
        eng.oplog_sync().unwrap();
    });
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(held);
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
/// 撮る (consumer の oplog の書き出しも止めておく)。 それでも像の oplog にその record があれば前提が崩れているので撮り直す。
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
        let held = with_stalled_sync(&eng, || {
            eng.tie_to(b, "notes.note", 22);
            eng.tie_text_to(c, "notes.tag", "fresh-word");
            eng.tie_text_to(d, "notes.body", "leaf body");
            eng.untie(a, "notes.note");
            eng.delete(e);
        });
        let img = root.join("img");
        let captured = crashsim::capture(&live, &img, Mode::Lost);
        crashsim::stop();
        drop(held);
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
    while !wal.is_empty() {
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

/// #669: 区画を移った直後の像 (古い区画の header の page だけが届き、 閉じの Commit の中身と新しい区画の record は届かなかった) を
/// 開いても、 開いた後の書き込みは WAL から読める (audit と、 sync の配る分)。 前は head が届かなかった閉じの Commit の後ろのままで、
/// 書き込みはその後ろに積まれ、 読みが閉じの Commit で止まって届かなかった。
#[test]
fn writes_after_a_torn_closing_commit_are_read_from_the_wal() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn_closing");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let eng = notes_engine(&live.join("db"), 64 * 1024);
    let wal = eng.oplog().unwrap().clone();
    let (img, closing) = {
        // 畳ませない (consumer は書き出しの後、 この lock で止まる = 区画を移った後に書き出さない)
        let _no_fold = eng.transfer_lock_for_fold();
        let e = eng.entity_in("notes").unwrap();
        let mut i = 0u32;
        while enchudb_oplog::oplog::wal_pos_offset(wal.head()) < 30 * 1024 {
            eng.tie_to(e, "notes.note", i);
            eng.oplog_commit();
            i += 1;
        }
        assert_eq!(wal.switches(), 0, "前提: まだ A");
        // consumer の書き出しが済んで lock で止まるのを待つ
        let t = std::time::Instant::now();
        while wal.checkpoint() != wal.head() && t.elapsed() < std::time::Duration::from_secs(5) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        // 入らない Leaf で区画を移る: A の終わりに閉じの Commit を置き、 A の header の page だけを書き出す
        let closing = enchudb_oplog::oplog::wal_pos_offset(wal.head()) as usize;
        let big = eng.entity_in("notes").unwrap();
        eng.tie_text_to(big, "notes.body", &"b".repeat(4000));
        assert_eq!(wal.switches(), 1, "前提: 区画を移った");
        let img = root.join("img");
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
        (img, closing)
    };
    crashsim::stop();
    let live_wal = std::fs::read(live.join("db").join("oplog")).unwrap();
    drop((wal, eng));
    let img_wal = std::fs::read(img.join("db").join("oplog")).unwrap();
    assert_ne!(img_wal[closing..closing + 120], live_wal[closing..closing + 120], "前提: 閉じの Commit の中身が届いていない");
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), 64 * 1024).unwrap();
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", 777_777u32);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    let is_new = |op: &DecodedOp| matches!(op, DecodedOp::Tie { value: 777_777, .. });
    let audited = eng.audit(&enchudb_engine::AuditFilter::default()).iter().any(|r| is_new(&r.op));
    let distributed = distributed(&eng).iter().any(|r| is_new(&r.op));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(audited, "開いた後の書き込みが audit に無い");
    assert!(distributed, "開いた後の書き込みが sync の配る分に無い");
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
    // #667: 区画を移った時に consumer を起こすと、 移った直後 (下で残りを 100 B にする途中) に閉じの Commit が入る。 この試験は
    // 満杯の状態を手で作るので切る
    eng.set_wal_fold_on_switch(false);
    // 満杯の書き手を待たせず (#388) 畳ませずに、 WAL に載らない write を作る
    wal.set_room_waiter(None);
    let mut dropped = Vec::new();
    let img = root.join("img");
    let captured = {
        let _no_fold = eng.transfer_lock_for_fold();
        // 2 区画: A を埋めて B へ移る (A は畳めないまま残るので、 空きは B の残りだけ)
        let filler = eng.entity_in("notes").unwrap();
        let switched = wal.switches();
        let mut i = 0u32;
        while wal.switches() == switched {
            eng.tie_to(filler, "notes.note", i);
            eng.oplog_commit();
            i += 1;
        }
        // 閉じた group で WAL を埋め、 残りを 100 B (Tie 136 B も Commit 120 B も入らない) にする。 TieLeaf の record は
        // 文字列の長さ + c B
        let e0 = eng.entity_in("notes").unwrap();
        let h0 = wal.head();
        eng.tie_text_to(e0, "notes.body", "x");
        let c = wal.head() - h0 - 1;
        eng.oplog_commit();
        let pad = wal.free_bytes() - wal.record_size_of(0) as u64 - 100 - c;
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

/// #656: 書き出しの済んだ語を指す Tag の cell は、 その列の門が閉じていても待たずに書ける (先に届いても語はディスクに
/// ある)。 新しい語を指す cell は門で待つ。 門を閉じた所で書き出しを止め、 その間に両方を書かせて、 書き出しの後の
/// 電源断の像でどの cell も前の値か中身ごと届いた値。 旧: 1 つの門を中身を指す列の全部の msync の間閉じていて、 済んだ語を
/// 書く書き手も待った。
#[test]
fn a_cell_of_a_written_out_word_does_not_wait_at_the_gate() {
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("durable_word");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "tg", ValueType::Tag, 0).unwrap();
    let [e0, e1, e2, e3] = std::array::from_fn(|_| eng.entity_in("t").unwrap());
    eng.tie_text(e0, "t.tg", "old");
    eng.tie_text(e1, "t.tg", "shared");
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Arc::new(eng);
    // 新しい語を書いて列に印を立てる (書き出しはこの列の門を閉じて、 中身をもう一度書き出す)
    eng.tie_text_to(e2, "t.tg", "a-word-written-before-the-write-out");

    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (paused_tx, go_rx) = (Mutex::new(paused_tx), Mutex::new(go_rx));
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::GateClosed {
            paused_tx.lock().unwrap().send(()).unwrap();
            go_rx.lock().unwrap().recv_timeout(Duration::from_secs(10)).expect("書き出しを進める合図が来ない");
        }
    })));
    let syncer = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.body_msync().unwrap())
    };
    paused_rx.recv_timeout(Duration::from_secs(10)).expect("書き出しが門を閉じた所で止まらない");
    let waits = eng.publish_gate_waits();
    let durable = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.tie_text_to(e0, "t.tg", "shared"))
    };
    let fresh = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.tie_text_to(e3, "t.tg", "a-word-written-while-the-gate-is-closed"))
    };
    let end = Instant::now() + Duration::from_secs(5);
    while !durable.is_finished() {
        assert!(Instant::now() < end, "書き出しの済んだ語を書く書き手が門で待った");
        std::thread::sleep(Duration::from_millis(1));
    }
    durable.join().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(!fresh.is_finished(), "新しい語を書く書き手が閉じた門を通った");
    assert_eq!(eng.publish_gate_waits(), waits + 1, "門で待った書き手を数えていない");
    go_tx.send(()).unwrap();
    syncer.join().unwrap();
    fresh.join().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    write_out_hook::set(None);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    let opened = Engine::open_standalone(img.join("db").to_str().unwrap()).unwrap();
    let text = |e| opened.get_text_owned(e, "t.tg").map(|b| String::from_utf8_lossy(&b).into_owned());
    let got = [text(e0), text(e1), text(e2), text(e3)];
    drop(opened);
    let _ = std::fs::remove_dir_all(&root);
    let ok = |g: &Option<String>, allowed: &[Option<&str>]| allowed.contains(&g.as_deref());
    assert!(
        ok(&got[0], &[Some("old"), Some("shared")])
            && ok(&got[1], &[Some("shared")])
            && ok(&got[2], &[Some("a-word-written-before-the-write-out")])
            && ok(&got[3], &[None, Some("a-word-written-while-the-gate-is-closed")]),
        "cell が中身より先に届いた / 書き出しの返った値が消えた: {got:?}"
    );
}

/// #656: 印の無い列 (前に書き出してから新しい語を指す cell が無い列) は 1 本ずつ門を閉じて書き出す。 その間に新しい語を
/// 書く書き手は、 その列なら待ち、 別の列なら待たない。 列を書き出す所で止めて両方に書かせ、 書き出しの後の像でどの cell も
/// 前の値か中身ごと届いた値。
#[test]
fn a_writer_waits_only_while_its_own_column_is_written_out() {
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("own_column");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "tg", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("t", "other", ValueType::Tag, 0).unwrap();
    let tg = eng.himo_id("t.tg").expect("t.tg") as u16;
    // 書き手は別の行に書く (同じ行の書き手は行の lock で並ぶ)
    let [e0, e1, e2] = std::array::from_fn(|_| eng.entity_in("t").unwrap());
    eng.tie_text(e0, "t.tg", "old");
    eng.tie_text(e0, "t.other", "old");
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Arc::new(eng);
    // 書き出しの前に書いた cell (列は書いた範囲を持つので、 1 本ずつの列の書き出しが msync する)
    eng.tie_text_to(e0, "t.tg", "old");

    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (paused_tx, go_rx) = (Mutex::new(paused_tx), Mutex::new(go_rx));
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::Column(tg) {
            paused_tx.lock().unwrap().send(()).unwrap();
            go_rx.lock().unwrap().recv_timeout(Duration::from_secs(10)).expect("書き出しを進める合図が来ない");
        }
    })));
    let syncer = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.body_msync().unwrap())
    };
    paused_rx.recv_timeout(Duration::from_secs(10)).expect("書き出しが列の門を閉じた所で止まらない");
    let same = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.tie_text_to(e1, "t.tg", "a-new-word-in-the-closed-column"))
    };
    let other = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.tie_text_to(e2, "t.other", "a-new-word-in-an-open-column"))
    };
    let end = Instant::now() + Duration::from_secs(5);
    while !other.is_finished() {
        assert!(Instant::now() < end, "閉じていない列に書く書き手が待った");
        std::thread::sleep(Duration::from_millis(1));
    }
    other.join().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(!same.is_finished(), "書き出している列に新しい語を書く書き手が門を通った");
    go_tx.send(()).unwrap();
    syncer.join().unwrap();
    same.join().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    write_out_hook::set(None);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    let opened = Engine::open_standalone(img.join("db").to_str().unwrap()).unwrap();
    let text = |e, h| opened.get_text_owned(e, h).map(|b| String::from_utf8_lossy(&b).into_owned());
    let got = [text(e0, "t.tg"), text(e1, "t.tg"), text(e2, "t.other")];
    drop(opened);
    let _ = std::fs::remove_dir_all(&root);
    assert!(
        got[0].as_deref() == Some("old")
            && matches!(got[1].as_deref(), None | Some("a-new-word-in-the-closed-column"))
            && matches!(got[2].as_deref(), None | Some("a-new-word-in-an-open-column")),
        "cell が中身より先に届いた / 書き出しの返った値が消えた: {got:?}"
    );
}

/// #801 並んだ書き出し: 後から来た書き出しは、 前の書き出しが msync している最中の segment を 「印が無い = 書くものが無い」 と見て
/// 飛ばさない。 `SegmentMap::flush_dirty` は印を取り出してから msync するので、 その間 `has_dirty()` は false。 前は中身を指す
/// 列 (と、 門を閉じた後に書き出し直す中身) を `has_dirty()` で選んでいて、 後の書き出しが前の msync を待たずに `Ok` を返した
/// (#421 の約束 = 「返った時、 呼ぶ前に印を付けた page は書き出してある」 が外から破れる)。
///
/// 2 本目の書き出しを途中 (中身を書き出した後。 列の時は残りの列を見る直前) で止め、 その間に書き手が書き、 1 本目の書き出しを msync の直前 (crashsim が写した後) で
/// 止めてから、 2 本目を進める。 2 本目が 1 本目の msync の終わる前に `Ok` で返ったら、 その時の電源断の像を開く:
///
/// - 列: 書き出しの済んだ語を指す Tag の cell を書く。 1 本目はその列の msync で止まる。 像の cell は前の値のまま
///   (= 書き出しの返った書き込みが消える)
/// - 中身: Leaf の値を書く (slot と cell)。 1 本目は Leaf の slot の msync で止まる。 2 本目は門を閉じた後に slot を
///   書き出し直さずに列を書き出すので、 像の cell は中身の無い slot を指す
///
/// 直した形では 2 本目は 1 本目を待つ (止めている間は返らない)。 1 本目は 2 本目が返るのを 1 秒だけ待ってから進む —
/// 時間が決めるのは 「飛ばす実装を見つけられるか」 だけで、 待つ実装が落ちることは無い。
#[test]
fn a_write_out_waits_for_a_segment_another_write_out_is_still_writing() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for himo in ["t.tg", "t.lf"] {
        overlapping_write_outs(himo);
    }
}

fn overlapping_write_outs(himo: &'static str) {
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::time::Duration;
    const NEW: &str = "a new value, longer than the old one";
    const FIRST: &str = "first-write-out";
    const SECOND: &str = "second-write-out";
    let column_case = himo == "t.tg";
    let root = scratch(&format!("overlap_write_out_{}", &himo[2..]));
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "lf", ValueType::Leaf, 0).unwrap();
    eng.define_himo_in("t", "tg", ValueType::Tag, 0).unwrap();
    let e = eng.entity_in("t").unwrap();
    let other = eng.entity_in("t").unwrap();
    eng.tie_text(e, himo, "old");
    // Tag: 新しい値の語を先に辞書に置いて書き出しておく (その語を指す cell は門を通らず、 列だけが書かれる)
    eng.tie_text(other, "t.tg", NEW);
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let column = eng.himo_id(himo).unwrap() as u16;
    let eng = Arc::new(eng);
    // 開いて最初の書き出し (中身の全域) を済ませ、 書き出しの済んだ語の境目を進める
    eng.body_msync().unwrap();

    let on = |name: &str| std::thread::current().name() == Some(name);
    // 2 本目が止まる所: 中身の時は門を閉じる前。 列の時は門を開いた後 (残りの列を 1 本ずつ見る前) — 門を閉じる所は閉じる側を
    // 1 本に並べるので、 そこより前で止めると 2 本目は 1 本目が列を書き出す間そこで待つ (列を見る所まで来ない)
    let second_stops_at = if column_case { Phase::Opened } else { Phase::BeforeGate };
    // 1 本目が止まる所: 列の時は 「その列の門を閉じた後」 に写した segment、 中身の時は最初に写した segment (Leaf の slot)
    let armed = Arc::new(AtomicBool::new(!column_case));
    let (at_gate_tx, at_gate_rx) = mpsc::channel::<()>();
    let (resume_tx, resume_rx) = mpsc::channel::<()>();
    let (at_gate_tx, resume_rx) = (Mutex::new(Some(at_gate_tx)), Mutex::new(resume_rx));
    {
        let armed = armed.clone();
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            let reached = on(SECOND) && p == second_stops_at;
            if let Some(tx) = reached.then(|| at_gate_tx.lock().unwrap().take()).flatten() {
                tx.send(()).unwrap();
                resume_rx.lock().unwrap().recv_timeout(Duration::from_secs(10)).expect("2 本目を進める合図が来ない");
            }
            if on(FIRST) && p == Phase::Column(column) {
                armed.store(true, Ordering::Release);
            }
        })));
    }
    let (in_msync_tx, in_msync_rx) = mpsc::channel::<()>();
    let (second_done_tx, second_done_rx) = mpsc::channel::<std::io::Result<()>>();
    let (finish_tx, finish_rx) = mpsc::channel::<()>();
    let stall = Mutex::new(Some((in_msync_tx, finish_rx)));
    {
        let armed = armed.clone();
        crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
            if what != Copied::Segment || !on(FIRST) || !armed.load(Ordering::Acquire) {
                return;
            }
            let taken = stall.lock().unwrap().take();
            if let Some((in_msync, finish)) = taken {
                in_msync.send(()).unwrap();
                finish.recv_timeout(Duration::from_secs(10)).expect("1 本目を進める合図が来ない");
            }
        })));
    }

    let second = {
        let eng = eng.clone();
        std::thread::Builder::new()
            .name(SECOND.into())
            .spawn(move || {
                let _ = second_done_tx.send(eng.body_msync());
            })
            .unwrap()
    };
    at_gate_rx.recv_timeout(Duration::from_secs(10)).expect("2 本目の書き出しが途中で止まらない");
    // 2 本目は中身を書き出し終えている。 ここで書いた分は、 2 本目が返る前に誰かが書き出すこと
    eng.tie_text_to(e, himo, NEW);
    let first = {
        let eng = eng.clone();
        std::thread::Builder::new().name(FIRST.into()).spawn(move || eng.body_msync()).unwrap()
    };
    in_msync_rx.recv_timeout(Duration::from_secs(10)).expect("1 本目の書き出しが msync の前で止まらない");
    resume_tx.send(()).unwrap();
    // 1 本目の msync はまだ届いていない。 2 本目がここで返ったら、 待たずに飛ばした
    let early = root.join("early");
    let returned_early = second_done_rx.recv_timeout(Duration::from_secs(1)).ok();
    if returned_early.is_some() {
        crashsim::capture(&live, &early, Mode::Lost).unwrap();
    }
    finish_tx.send(()).unwrap();
    first.join().unwrap().unwrap();
    let second_result = match returned_early {
        Some(r) => r,
        None => second_done_rx.recv_timeout(Duration::from_secs(10)).expect("2 本目の書き出しが返らない"),
    };
    second.join().unwrap();
    let after = root.join("after");
    crashsim::capture(&live, &after, Mode::Lost).unwrap();
    write_out_hook::set(None);
    crashsim::set_after_copy_hook(None);
    crashsim::stop();
    drop(eng);

    let read = |img: &Path| {
        img.exists().then(|| {
            let opened = Engine::open_standalone(img.join("db").to_str().unwrap()).unwrap();
            opened.get_text_owned(e, himo).map(|b| String::from_utf8_lossy(&b).into_owned())
        })
    };
    let (got_early, got_after) = (read(&early), read(&after));
    let _ = std::fs::remove_dir_all(&root);
    second_result.unwrap();
    if let Some(got) = got_early {
        assert_eq!(
            got.as_deref(),
            Some(NEW),
            "{himo}: 2 本目の書き出しが、 1 本目の msync が終わる前に Ok で返った。 その時の電源断の像に、 書き出しの前に書いた値が無い"
        );
    }
    assert_eq!(got_after.unwrap().as_deref(), Some(NEW), "{himo}: 両方の書き出しが返った後の像に値が無い");
}

/// #656: 前の session が書き出さずに終わった (process が落ちた) 語は、 page cache にあってもディスクに無い。 開いた後に
/// その語を指す cell を書いても、 最初の書き出しが辞書の全域を書き出すので、 cell だけが届くことは無い (書き出しの済んだ語の
/// 境目は、 その後でないと進めない)。 前の session は子 process で書いて、 書き出さずに exit する。
#[test]
fn words_left_unwritten_by_a_crashed_session_reach_disk_before_cells() {
    if std::env::var(VERIFY_ENV).is_ok() || std::env::var(UNWRITTEN_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("unwritten_words");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let (e1, e2) = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 100).unwrap();
        eng.define_himo_in("t", "tg", ValueType::Tag, 0).unwrap();
        let e0 = eng.entity_in("t").unwrap();
        eng.tie_text(e0, "t.tg", "flushed");
        let (e1, e2) = (eng.entity_in("t").unwrap(), eng.entity_in("t").unwrap());
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        (e1, e2)
    };
    // 子: 開いて新しい語を書き、 書き出さずに終わる (page cache には残る、 この process の控えには無い)
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["write_words_and_die_child", "--exact", "--test-threads=1", "--nocapture"])
        .env(UNWRITTEN_ENV, &db)
        .env(ACKED_ENV, e1.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn child");
    assert!(out.status.success(), "子が落ちた: {}", String::from_utf8_lossy(&out.stderr));
    let mut eng = Engine::open_standalone(db.to_str().unwrap()).unwrap();
    assert_eq!(eng.get_text_owned(e1, "t.tg").as_deref(), Some(&b"left-unwritten"[..]), "前提: 子の書いた語が page cache にある");
    // 既にある語を指す cell (辞書には書かない)
    eng.tie_text(e2, "t.tg", "left-unwritten");
    eng.body_msync().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    let opened = Engine::open_standalone(img.join("db").to_str().unwrap()).unwrap();
    let got = [e1, e2].map(|e| opened.get_text_owned(e, "t.tg").map(|b| String::from_utf8_lossy(&b).into_owned()));
    drop(opened);
    let _ = std::fs::remove_dir_all(&root);
    for g in &got {
        assert!(
            matches!(g.as_deref(), None | Some("left-unwritten")),
            "辞書に語の無い cell が届いた (開いてから最初の書き出しが辞書の全域を書き出していない): {got:?}"
        );
    }
}

const UNWRITTEN_ENV: &str = "ENCHU_POWER_LOSS_UNWRITTEN";

/// 子: `words_left_unwritten_by_a_crashed_session_reach_disk_before_cells` の前の session。
#[test]
fn write_words_and_die_child() {
    let Ok(db) = std::env::var(UNWRITTEN_ENV) else { return };
    let e1: u64 = std::env::var(ACKED_ENV).unwrap().parse().unwrap();
    let mut eng = Engine::open_standalone(&db).unwrap();
    eng.tie_text(e1, "t.tg", "left-unwritten");
    // 書き出さずに終わる (Drop の flush を通さない)
    std::process::exit(0);
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

/// #550: 主キーのある行を消して `oplog_sync` が返った直後の電源断の像でも、 消した主キー (`pktombs`) が残る。 `oplog_sync` は
/// checkpoint を進めるので、 開く時に WAL の削除は当て直されず、 本体の主キーの cell も消えている (引き直せない)。 前は
/// `pktombs` を consumer の周期 (最大 100 ms 後) か flush でしか書かず、 その間の電源断で失った。
#[test]
fn deleted_primary_keys_survive_power_loss_after_oplog_sync() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("pktombs");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
    let key = eng.himo_id("t.key").expect("t.key") as u16;
    eng.set_table_pk("t", key).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let e = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(e, key, 7u32);
    eng.oplog_sync().unwrap();
    eng.delete(e);
    eng.oplog_sync().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    let back = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    let n = back.deleted_primary_key_count();
    drop(back);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(n, 1, "oplog_sync の返った削除の主キーが電源断の像で消えた");
}

/// #720: `pkformer` の追記 (`pkformer.log`) は、 書き出し (`persist_sync_state`) が返った直後の電源断の像でも残る。 1 回目の
/// 書き出しは log を作り (#959: disk の無い DB の最初の書き出しは log の header + record だけ。 その後、 書き出しの lock の外のまとめ直しが
/// `pkformer` に並べる = review987 [3]。 ここでは待って走らせる)、 2 回目は変わった slot の record だけを log に足す (前提)。 像の
/// `pkformer` + log を読んだ中身が、 電源を落とす前の中身と同じで、 像は開ける。
#[test]
fn appended_pkformer_records_survive_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("pkformer_log");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    let path = db.to_str().unwrap();
    crashsim::start();
    let mut eng = Engine::create_with_cell_version(path, 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "title", ValueType::Number, 0).unwrap();
    let key = eng.himo_id("t.key").expect("t.key") as u16;
    let title = eng.himo_id("t.title").expect("t.title") as u16;
    eng.set_table_pk("t", key).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(2);
    // 自分の行に、 相手 (peer 1) が別に作った同じ名前の行を束ねる (`Syncer` の束ねと同じ呼び出し)
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let at = |n: u64| Hlc { wall: now + 600_000 + n, logical: 0, peer: 1 };
    let own = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(own, key, 7u32);
    eng.tie_to_by_id(own, title, 1u32);
    eng.oplog_sync().unwrap();
    let foreign = enchudb_oplog::make_eid(1, 0);
    eng.note_bound_identity(own, foreign, at(0));
    let local = eng.bind_remote_eid_by_pk(1, foreign, own, key, 7);
    eng.remote_tie_apply(local, key, 7u32, at(0));
    eng.note_bound_write(local, foreign, key, Some(7), at(0));
    eng.persist_sync_state().unwrap();
    eng.compact_name_records_now();
    let pkformer = enchudb_engine::db_files::path_for(path, enchudb_engine::db_files::PKFORMER);
    let log = enchudb_engine::db_files::path_for(path, enchudb_engine::db_files::PKFORMER_LOG);
    let snapshot = std::fs::read(&pkformer).ok();
    assert!(snapshot.is_some(), "前提: 最初の書き出しの後に pkformer にまとめ直した");
    let log_before = std::fs::metadata(&log).expect("前提: 1 回目の書き出しは log を作る").len();
    assert!(eng.remote_tie_apply(local, title, 2u32, at(1)), "前提: 相手の書き換えが当たった");
    eng.note_bound_write(local, foreign, title, Some(2), at(1));
    eng.persist_sync_state().unwrap();
    let appended = std::fs::metadata(&log).unwrap().len() > log_before && std::fs::read(&pkformer).ok() == snapshot;
    let expect = Engine::pkformer_for_bootstrap(path).unwrap().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    let img_db = img.join("db");
    let got = Engine::pkformer_for_bootstrap(img_db.to_str().unwrap()).unwrap();
    let back = Engine::open_concurrent_with_oplog(img_db.to_str().unwrap(), OPLOG_CAP).map(|e| e.pkformer_disk_diff().unwrap());
    let _ = std::fs::remove_dir_all(&root);
    assert!(appended, "前提: 2 回目の書き出しは log への追記 (pkformer は書き直さない)");
    // review987 [7]: 前は最初の書き出しが log だけで snapshot が空になり、 この前提はいつも真だった (今は 1 回目の中身の `pkformer`)
    assert!(expect != snapshot.unwrap_or_default(), "前提: 追記の分は pkformer だけでは読めない");
    assert!(got.as_deref() == Some(&expect[..]), "書き出しの返った追記が電源断の像で消えた");
    assert_eq!(back.expect("像を開けない"), None);
}

/// review987 [1]: 名前の記録 (`pkformer`) のまとめ直しは書き出しの lock の外で作り、 作っている間に log が伸びたら (別の書き出しが
/// 追記した) 伸びた分を作った中身に当てて作り直してから置く。 まとめ直しの途中の電源断の像 (作り終えて置く前 / `pkformer` を置いた後・
/// log を置く前) でも、 書き出しの返った前の名前 (まとめ直しの間に追記した分も) と消した名前の版数が全部ある。 伸びた分を新しい log に
/// 写すだけで置くと、 `pkformer` を置いた後の像は古い log (header が合わない) を読まず、 間に追記した分を失う。
#[test]
fn name_records_compacted_outside_the_lock_survive_power_loss_mid_compaction() {
    use enchudb_engine::engine::name_compact_hook::{self, Phase as CPhase};
    use std::sync::Mutex;
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const ROWS: u32 = 256;
    let root = scratch("name_compact");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    let path = db.to_str().unwrap().to_string();
    crashsim::start();
    // hook を置いている間は consumer の周期はまとめ直さない (この thread の `compact_name_records_now` だけが段を踏む)
    name_compact_hook::set(Some(Arc::new(|_| {})));
    let mut eng = Engine::create_with_cell_version(&path, 65_536).unwrap();
    eng.define_table("t", ROWS).unwrap();
    eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
    let key = eng.himo_id("t.key").expect("t.key") as u16;
    eng.set_table_pk("t", key).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let rows: Vec<u64> = (0..ROWS)
        .map(|i| {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, key, i);
            e
        })
        .collect();
    // 満杯の表で消して足す (使い回すたびに前の住人の名前が identity に移る)。 名前は i と i + ROWS を行き来する
    let state = Arc::new(Mutex::new((rows, (0..ROWS).collect::<Vec<u32>>(), 0u32)));
    let churn = {
        let (eng, state) = (eng.clone(), state.clone());
        move |n: usize| {
            let mut st = state.lock().unwrap();
            let (rows, names, round) = &mut *st;
            for i in 0..n {
                let at = (i * 2 + *round as usize % 2) % ROWS as usize;
                eng.delete(rows[at]);
                let e = eng.entity_in("t").unwrap();
                names[at] = if names[at] < ROWS { names[at] + ROWS } else { names[at] - ROWS };
                eng.tie_to_by_id(e, key, names[at]);
                rows[at] = e;
            }
            *round += 1;
        }
    };
    let versions = {
        let eng = eng.clone();
        move || (0..2 * ROWS).map(|n| eng.deleted_name_version(key, n as u64)).collect::<Vec<Option<Hlc>>>()
    };
    churn(ROWS as usize / 2);
    eng.persist_sync_state().unwrap();
    // 最初の書き出し (log だけ) の後のまとめ直しを、 段ごとに撮りながら走らせる。 作り終えた後 (置く前) に別の書き出しが追記する
    type Shot = (PathBuf, Vec<u8>, Vec<Option<Hlc>>);
    let shots: Arc<Mutex<Vec<(CPhase, Shot)>>> = Arc::new(Mutex::new(Vec::new()));
    let appended = Arc::new(AtomicBool::new(false));
    // 追記の書き出しが返った後の disk の中身 (以後に撮った像は全部これを持つ)
    let after_tail: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    {
        let (eng, shots, appended, live, root, path, after_tail) =
            (eng.clone(), shots.clone(), appended.clone(), live.clone(), root.clone(), path.clone(), after_tail.clone());
        let churn = churn.clone();
        let versions = versions.clone();
        name_compact_hook::set(Some(Arc::new(move |phase| {
            if !matches!(phase, CPhase::FormerPrepared | CPhase::FormerPlaced) {
                return;
            }
            let mut shots = shots.lock().unwrap();
            let n = shots.len();
            if n >= 4 {
                return;
            }
            // 像が持つべき中身: 追記の後は追記の書き出しが返った時の disk の中身。 前は撮る時の disk の中身 (`pkformer` + log)
            let expect = match after_tail.lock().unwrap().clone() {
                Some(t) => t,
                None => Engine::pkformer_for_bootstrap(&path).unwrap().unwrap(),
            };
            let img = root.join(format!("img{n}"));
            crashsim::capture(&live, &img, Mode::Lost).unwrap();
            shots.push((phase, (img, expect, versions())));
            drop(shots);
            if phase == CPhase::FormerPrepared && !appended.swap(true, Ordering::AcqRel) {
                // 作り終えた後・置く前に、 書き手が進んで別の書き出しが log に追記する (置く時に当てて作り直す)
                churn(ROWS as usize / 4);
                eng.persist_sync_state().unwrap();
                *after_tail.lock().unwrap() = Some(Engine::pkformer_for_bootstrap(&path).unwrap().unwrap());
            }
        })));
    }
    eng.compact_name_records_now();
    name_compact_hook::set(None);
    // 置いた disk が memory と同じ (伸びた分を当てずに置くと違う)
    let live_diff = eng.pkformer_disk_diff().unwrap();
    let fin = Engine::pkformer_for_bootstrap(&path).unwrap().unwrap();
    let shots = std::mem::take(&mut *shots.lock().unwrap());
    crashsim::stop();
    let log_len = std::fs::metadata(enchudb_engine::db_files::path_for(&path, enchudb_engine::db_files::PKFORMER_LOG)).unwrap().len();
    drop(eng);
    let phases: Vec<CPhase> = shots.iter().map(|s| s.0).collect();
    let mut failures = Vec::new();
    for (i, (phase, (img, expect, vers))) in shots.iter().enumerate() {
        let img_db = img.join("db");
        let img_path = img_db.to_str().unwrap();
        let got = Engine::pkformer_for_bootstrap(img_path).unwrap();
        let want = expect;
        if got.as_deref() != Some(&want[..]) {
            failures.push(format!("像 {i} ({phase:?}): pkformer の中身が違う ({:?} B, 期待 {} B)", got.map(|g| g.len()), want.len()));
        }
        match Engine::open_concurrent_with_oplog(img_path, OPLOG_CAP) {
            Ok(back) => {
                let got_v: Vec<Option<Hlc>> = (0..2 * ROWS).map(|n| back.deleted_name_version(key, n as u64)).collect();
                if &got_v != vers {
                    failures.push(format!("像 {i} ({phase:?}): 消した名前の版数が違う"));
                }
                if let Some(d) = back.pkformer_disk_diff().unwrap() {
                    failures.push(format!("像 {i} ({phase:?}): 開いた後の disk と memory が違う: {d}"));
                }
            }
            Err(e) => failures.push(format!("像 {i} ({phase:?}): 開けない: {e}")),
        }
    }
    let _ = std::fs::remove_dir_all(&root);
    let after_tail = after_tail.lock().unwrap().clone();
    assert!(after_tail.is_some() && phases.contains(&CPhase::FormerPlaced), "前提: 作り終えた後に追記があり、 置いた ({phases:?})");
    assert!(failures.is_empty(), "{phases:?}: {failures:#?}");
    assert_eq!(live_diff, None, "まとめ直して置いた disk が memory と違う");
    assert_eq!(Some(fin), after_tail, "置いた disk の中身が、 置く前に書き出しの返った中身と違う");
    assert_eq!(
        phases,
        vec![CPhase::FormerPrepared, CPhase::FormerPrepared, CPhase::FormerPlaced],
        "前提: 作り終えた後に追記があり、 当てて作り直してから置いた"
    );
    assert_eq!(log_len, 24, "前提: まとめ直した後の log は header だけ");
}

/// review987 [1]: `pktombs` のまとめ直しは書き出しの lock の外で作り、 作っている間に別の書き出しが追記した分 (伸びた分) を一時 file の
/// 後ろに足して置く。 まとめ直しの後に checkpoint を進めた (= WAL からは引き直せない) 像で、 伸びた分の削除の版数も残る。
#[test]
fn pktombs_compacted_outside_the_lock_keeps_the_appends_made_meanwhile() {
    pktombs_compaction_with_appends_meanwhile(1);
}

/// review987 2 回目 [B]: `pktombs` のまとめ直しの 2 回目 (書き出しの lock の下)。 一時 file を作った後と、 1 回目に伸びた分を足して
/// 書き出した後の 2 度、 別の書き出しが追記すると、 2 回目は書き出しの lock の下で伸びた分を足して書き出してから置く。 置いた file が
/// memory と同じ、 電源断の像の `pktombs` が置いた file と同じ (2 度目に伸びた分を書き出さずに置くと、 像の file の後ろが欠ける)。
#[test]
fn pktombs_compaction_under_the_lock_keeps_the_appends_made_meanwhile() {
    pktombs_compaction_with_appends_meanwhile(2);
}

/// `pktombs` のまとめ直しの回ごとの始め (`TombsPrepared`) のうち、 最初の `appends` 回に別の書き出しが追記する。
fn pktombs_compaction_with_appends_meanwhile(appends: usize) {
    use enchudb_engine::engine::name_compact_hook::{self, Phase as CPhase};
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const ROWS: u32 = 256;
    let root = scratch("tombs_compact");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    let path = db.to_str().unwrap().to_string();
    crashsim::start();
    name_compact_hook::set(Some(Arc::new(|_| {})));
    let mut eng = Engine::create_with_cell_version(&path, 65_536).unwrap();
    eng.define_table("t", ROWS).unwrap();
    eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
    let key = eng.himo_id("t.key").expect("t.key") as u16;
    eng.set_table_pk("t", key).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let rows: Vec<u64> = (0..ROWS)
        .map(|i| {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, key, i);
            e
        })
        .collect();
    // 満杯の表で、 名前を i と i + ROWS の間で行き来させながら消して足す (同じ名前を何度も消す = record が生きている記録より速く増える)
    let state = Arc::new(std::sync::Mutex::new((rows, (0..ROWS).collect::<Vec<u32>>(), 0u32)));
    let churn = {
        let (eng, state) = (eng.clone(), state.clone());
        move |n: usize| {
            let mut st = state.lock().unwrap();
            let (rows, names, round) = &mut *st;
            for i in 0..n {
                let at = (i * 2 + *round as usize % 2) % ROWS as usize;
                eng.delete(rows[at]);
                let e = eng.entity_in("t").unwrap();
                names[at] = if names[at] < ROWS { names[at] + ROWS } else { names[at] - ROWS };
                eng.tie_to_by_id(e, key, names[at]);
                rows[at] = e;
            }
            *round += 1;
        }
    };
    let fired = Arc::new(AtomicU32::new(0));
    {
        let (eng, fired, churn) = (eng.clone(), fired.clone(), churn.clone());
        name_compact_hook::set(Some(Arc::new(move |phase| {
            if phase == CPhase::TombsPrepared && (fired.load(Ordering::Acquire) as usize) < appends {
                // 一時 file を作った後 (2 度目は、 1 回目に伸びた分を足して書き出した後) に、 書き手が進んで別の書き出しが `pktombs` に追記する
                churn(ROWS as usize / 2);
                eng.persist_sync_state().unwrap();
                fired.fetch_add(1, Ordering::AcqRel);
            }
        })));
    }
    let tombs_len = || std::fs::metadata(enchudb_engine::db_files::path_for(&path, enchudb_engine::db_files::PKTOMBS)).map_or(0, |m| m.len());
    let (rounds_before, _) = eng.name_compaction_rounds();
    let mut shrank = false;
    for _ in 0..60 {
        churn(ROWS as usize / 2);
        eng.persist_sync_state().unwrap();
        let before = tombs_len();
        eng.compact_name_records_now();
        if fired.load(Ordering::Acquire) > 0 {
            shrank = tombs_len() < before;
            break;
        }
    }
    name_compact_hook::set(None);
    let (rounds_after, _) = eng.name_compaction_rounds();
    // 置いた file が memory と同じ (作っている間の追記を落とすと違う)
    let live_diff = eng.pktombs_disk_diff().unwrap();
    let live_file = std::fs::read(enchudb_engine::db_files::path_for(&path, enchudb_engine::db_files::PKTOMBS)).unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    // 電源断の像の `pktombs` は置いた file (一時 file の書き出しと rename が disk に届いている)。 像を開いて版数を比べることはしない
    // (閉じずに落ちた像を開くと、 消した名前の版数が新しくなる回がある。 まとめ直しと関係なく起きる)
    let img_file = std::fs::read(img.join("db").join(enchudb_engine::db_files::PKTOMBS)).unwrap_or_default();
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(fired.load(Ordering::Acquire) as usize, appends, "前提: pktombs をまとめ直す間に {appends} 度追記した");
    assert!(shrank, "前提: まとめ直して縮んだ");
    // 前提: 置いた回 ([1] = lock の外で 1 回足した、 [2] = もう一度伸びて lock の下で足して書き出した)
    let placed: Vec<u64> = rounds_after.iter().zip(rounds_before).map(|(a, b)| a - b).collect();
    assert_eq!(placed, if appends == 1 { [0, 1, 0] } else { [0, 0, 1] }, "前提: まとめ直しを置いた回");
    assert_eq!(live_diff, None, "まとめ直しの間に追記した削除が置いた file に無い");
    assert!(img_file == live_file, "電源断の像の pktombs が置いた file と違う ({} B / {} B)", img_file.len(), live_file.len());
}

/// review987 2 回目 [B]: `pkformer` のまとめ直しの 3 回目 (書き出しの lock の下)。 作る間に log が伸び (1 回目の置く前)、 伸びた分を
/// 読む前にもう一度伸び (2 回目で当てる)、 2 回目の伸びた分を読む前にまた伸びる (3 回目で当てる) と、 3 回目は書き出しの lock の下で
/// 伸びた分を当てて作り直して置く。 3 回目で置いた `pkformer` が memory と同じ、 `pkformer` を置いた後・log を置く前の電源断の像も
/// 書き出しの返った追記 (3 回目に当てた分も) を全部持つ。 3 回目の伸びた分を当てずに置くと、 その分の前の名前を失う。
#[test]
fn pkformer_compaction_under_the_lock_applies_the_appends_made_meanwhile() {
    use enchudb_engine::engine::name_compact_hook::{self, Phase as CPhase};
    use std::sync::Mutex;
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const ROWS: u32 = 256;
    let root = scratch("name_compact_locked");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    let path = db.to_str().unwrap().to_string();
    crashsim::start();
    // hook を置いている間は consumer の周期はまとめ直さない (この thread の `compact_name_records_now` だけが段を踏む)
    name_compact_hook::set(Some(Arc::new(|_| {})));
    let mut eng = Engine::create_with_cell_version(&path, 65_536).unwrap();
    eng.define_table("t", ROWS).unwrap();
    eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
    let key = eng.himo_id("t.key").expect("t.key") as u16;
    eng.set_table_pk("t", key).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let rows: Vec<u64> = (0..ROWS)
        .map(|i| {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, key, i);
            e
        })
        .collect();
    let state = Arc::new(Mutex::new((rows, (0..ROWS).collect::<Vec<u32>>(), 0u32)));
    let churn = {
        let (eng, state) = (eng.clone(), state.clone());
        move |n: usize| {
            let mut st = state.lock().unwrap();
            let (rows, names, round) = &mut *st;
            for i in 0..n {
                let at = (i * 2 + *round as usize % 2) % ROWS as usize;
                eng.delete(rows[at]);
                let e = eng.entity_in("t").unwrap();
                names[at] = if names[at] < ROWS { names[at] + ROWS } else { names[at] - ROWS };
                eng.tie_to_by_id(e, key, names[at]);
                rows[at] = e;
            }
            *round += 1;
        }
    };
    churn(ROWS as usize / 2);
    eng.persist_sync_state().unwrap();
    // 追記の書き出しが返った後の disk の中身 (追記ごと)
    let appended: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let shots: Arc<Mutex<Vec<(CPhase, PathBuf, Vec<u8>)>>> = Arc::new(Mutex::new(Vec::new()));
    let prepared = Arc::new(AtomicBool::new(false));
    {
        let (eng, appended, shots, prepared, live, root, path) =
            (eng.clone(), appended.clone(), shots.clone(), prepared.clone(), live.clone(), root.clone(), path.clone());
        let churn = churn.clone();
        name_compact_hook::set(Some(Arc::new(move |phase| {
            let append = match phase {
                // 1 回目の作った後 (置く前の見直しで伸びている)
                CPhase::FormerPrepared => !prepared.swap(true, Ordering::AcqRel),
                // 伸びた分を読む前 (その分は次の回が当てる。 2 回目の後の分は 3 回目 = 書き出しの lock の下が当てる)
                CPhase::FormerGrown => true,
                CPhase::FormerPlaced => {
                    // `pkformer` を置いた後・log を置く前の像。 持つべき中身は最後の追記の書き出しが返った時の disk の中身
                    let expect = appended.lock().unwrap().last().cloned().unwrap_or_default();
                    let mut shots = shots.lock().unwrap();
                    let img = root.join(format!("img{}", shots.len()));
                    crashsim::capture(&live, &img, Mode::Lost).unwrap();
                    shots.push((phase, img, expect));
                    false
                }
                _ => false,
            };
            if append && appended.lock().unwrap().len() < 3 {
                churn(ROWS as usize / 4);
                eng.persist_sync_state().unwrap();
                appended.lock().unwrap().push(Engine::pkformer_for_bootstrap(&path).unwrap().unwrap());
            }
        })));
    }
    let (_, rounds_before) = eng.name_compaction_rounds();
    eng.compact_name_records_now();
    name_compact_hook::set(None);
    let (_, rounds_after) = eng.name_compaction_rounds();
    // 置いた disk が memory と同じ (3 回目に伸びた分を当てずに置くと違う)
    let live_diff = eng.pkformer_disk_diff().unwrap();
    let fin = Engine::pkformer_for_bootstrap(&path).unwrap().unwrap();
    let log_len = std::fs::metadata(enchudb_engine::db_files::path_for(&path, enchudb_engine::db_files::PKFORMER_LOG)).unwrap().len();
    let fin_img = root.join("img-final");
    let captured = crashsim::capture(&live, &fin_img, Mode::Lost);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    // 置いた log (header だけ) は書き出さない: 置き終えた後の像の log には header が無い (読まれず、 `pkformer` だけで全部ある)
    let live_log = std::fs::read(enchudb_engine::db_files::path_for(&path, enchudb_engine::db_files::PKFORMER_LOG)).unwrap();
    let img_log = std::fs::read(fin_img.join("db").join(enchudb_engine::db_files::PKFORMER_LOG)).unwrap_or_default();
    let appended = std::mem::take(&mut *appended.lock().unwrap());
    let mut shots = std::mem::take(&mut *shots.lock().unwrap());
    shots.push((CPhase::FormerPlaced, fin_img, appended.last().cloned().unwrap_or_default()));
    let mut failures = Vec::new();
    for (i, (phase, img, want)) in shots.iter().enumerate() {
        let img_db = img.join("db");
        let img_path = img_db.to_str().unwrap();
        let got = Engine::pkformer_for_bootstrap(img_path).unwrap();
        if got.as_deref() != Some(&want[..]) {
            failures.push(format!("像 {i} ({phase:?}): pkformer の中身が違う ({:?} B, 期待 {} B)", got.map(|g| g.len()), want.len()));
        }
        match Engine::open_concurrent_with_oplog(img_path, OPLOG_CAP) {
            Ok(back) => {
                if let Some(d) = back.pkformer_disk_diff().unwrap() {
                    failures.push(format!("像 {i} ({phase:?}): 開いた後の disk と memory が違う: {d}"));
                }
            }
            Err(e) => failures.push(format!("像 {i} ({phase:?}): 開けない: {e}")),
        }
    }
    let _ = std::fs::remove_dir_all(&root);
    let placed: Vec<u64> = rounds_after.iter().zip(rounds_before).map(|(a, b)| a - b).collect();
    assert_eq!(appended.len(), 3, "前提: まとめ直しの間に 3 度追記した");
    assert!(appended[2] != appended[1], "前提: 3 回目 (書き出しの lock の下) が当てる伸びた分がある");
    assert_eq!(placed, [0, 0, 1], "前提: 3 回目 (書き出しの lock の下) で置いた");
    assert_eq!(shots.len(), 2, "前提: pkformer を置いた後・log を置く前の像と、 置き終えた後の像");
    assert!(failures.is_empty(), "{failures:#?}");
    assert_eq!(live_diff, None, "3 回目に置いた disk が memory と違う");
    assert!(fin == appended[2], "置いた disk の中身が、 最後の追記の書き出しが返った中身と違う");
    assert_eq!(log_len, 24, "前提: まとめ直した後の log は header だけ");
    assert!(img_log != live_log, "前提: 置き終えた後の像の log は header を持たない (書き出していない log を読まずに開く像)");
}

/// review987 2 回目 [F]: まとめ直しの途中で panic しても、 まとめ直しの印 (走っている) を下ろし、 一時 file を消す (次の機会に始め直せる)。
/// 前は印が立ったまま残り、 その記録は二度とまとめ直されず、 試験の `compact_name_records_now` は回り続けた。
#[test]
fn a_compaction_that_panics_can_start_again() {
    use enchudb_engine::engine::name_compact_hook::{self, Phase as CPhase};
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const ROWS: u32 = 256;
    let root = scratch("compact_panic");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("db").to_str().unwrap().to_string();
    // hook を置いている間は consumer の周期はまとめ直さない
    name_compact_hook::set(Some(Arc::new(|_| {})));
    let mut eng = Engine::create_with_cell_version(&path, 65_536).unwrap();
    eng.define_table("t", ROWS).unwrap();
    eng.define_himo_in("t", "key", ValueType::Number, 0).unwrap();
    let key = eng.himo_id("t.key").expect("t.key") as u16;
    eng.set_table_pk("t", key).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let mut rows: Vec<u64> = (0..ROWS)
        .map(|i| {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to_by_id(e, key, i);
            e
        })
        .collect();
    let mut names: Vec<u32> = (0..ROWS).collect();
    for i in 0..(ROWS / 2) as usize {
        eng.delete(rows[i]);
        let e = eng.entity_in("t").unwrap();
        names[i] += ROWS;
        eng.tie_to_by_id(e, key, names[i]);
        rows[i] = e;
    }
    // 最初の書き出し (log だけ) の後の `pkformer` のまとめ直しが、 一時 file を作った後に panic する
    eng.persist_sync_state().unwrap();
    name_compact_hook::set(Some(Arc::new(|phase| {
        if phase == CPhase::FormerPrepared {
            panic!("まとめ直しの途中の panic (試験)");
        }
    })));
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eng.compact_name_records_now())).is_err();
    name_compact_hook::set(Some(Arc::new(|_| {})));
    let pkformer = enchudb_engine::db_files::path_for(&path, enchudb_engine::db_files::PKFORMER);
    let left = std::fs::read_dir(&path).unwrap().filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().into_owned()).filter(|n| n.ends_with(".compact.tmp")).collect::<Vec<_>>();
    let placed_before = pkformer.exists();
    // 次の機会に始め直せる (印が立ったままなら、 待ち合わせが回り続ける = 上限で落とす)
    let (tx, rx) = std::sync::mpsc::channel();
    let again = {
        let eng = eng.clone();
        std::thread::spawn(move || {
            eng.compact_name_records_now();
            let _ = tx.send(());
        })
    };
    let finished = rx.recv_timeout(std::time::Duration::from_secs(30)).is_ok();
    name_compact_hook::set(None);
    let placed = pkformer.exists();
    let diff = if finished { eng.pkformer_disk_diff().unwrap() } else { None };
    if finished {
        again.join().unwrap();
        drop(eng);
        let _ = std::fs::remove_dir_all(&root);
    }
    assert!(panicked && !placed_before, "前提: まとめ直しが置く前に panic した");
    assert!(left.is_empty(), "panic したまとめ直しの一時 file が残った: {left:?}");
    assert!(finished, "panic したまとめ直しの印が立ったまま (始め直せない)");
    assert!(placed, "panic の後にまとめ直さない");
    assert_eq!(diff, None);
}

/// #557: sync の表の無い v9 の DB (cell の版数あり) で、 汚れた終了の後に開いた時、 cell の版数が時計より先にあっても (時計が
/// 戻った = cell に未来の版数を置いて真似る)、 ローカルの書き込みが弾かれない。 開く時の HLC の種は WAL に残った record だけで、
/// WAL に無い版数 (畳んだ後) を知らなかった。
#[test]
fn local_writes_after_power_loss_are_not_older_than_cells() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("hlc_seed");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    let v = eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap() as u16;
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    assert!(eng.has_cell_version() && !eng.sync_tables_enabled(), "前提: sync の表の無い v9 の DB");
    let e = eng.entity_in("t").unwrap();
    let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let future = Hlc { wall: wall + 3_600_000, logical: 0, peer: 1 };
    assert!(eng.set_cell(e, v, 5, future), "前提: 未来の版数で cell を書けた");
    eng.oplog_sync().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    let back = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(back.cell_hlc(e, v), future, "前提: 像の cell に未来の版数がある");
    let r = back.try_tie_to_by_id(e, v, 6u32);
    let got = back.get(e, "t.v");
    drop(back);
    let _ = std::fs::remove_dir_all(&root);
    assert!(r.is_ok(), "汚れた終了の後のローカルの書き込みが cell の版数より古いと弾かれた: {r:?}");
    assert_eq!(got, Some(6));
}

// ── #451 段階 1 (OPEN_TIME_PLAN): 配り済みの印 ──────────────────────────────────────────────────────────────

/// consumer の oplog の書き出しを、 写した直後 (= disk に届く前) で止める。 戻り値の drop で放す (hook も外す)。
/// `with_stalled_sync` の consumer 側だけ (oplog_sync の thread は走らせない)。
///
/// Commit を 1 つ積んで consumer を周期の書き出し (写した直後、 lock を持たない所) に来させ、 止まったのを見てから返す (止まる前に
/// 始まった周期は無い = 返った後に印を置くのは呼び手だけ)。 「次に写した所で止まる」 形にしないこと: 埋まった WAL を見て起きた
/// consumer は空き作り (#388、 `make_wal_room_inner`) に入り、 transfer の lock を持ったまま書き出す = その写しで止まると、 試験の
/// `oplog_sync` の bridge (同じ lock を待つ) と互いに待つ。 Linux の CI で踏んだ (試験の `oplog_sync` の Commit で起きた consumer が、
/// 試験 thread の fsync と本体の書き出しの間に lock を取った。 Mac は書き出しが速く、 bridge が先に済んで consumer は lock を取れない)。
/// 畳んだ直後に header を届ける写しも bridge の lock の下なので、 止めるのは全体の fsync (`Copied::Oplog`) だけ (#1000)。
fn hold_consumer(eng: &Arc<Engine>) -> ConsumerHeld {
    let (parked_tx, parked_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let hold = std::sync::Mutex::new(Some((parked_tx, release_rx)));
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
        if what == Copied::Oplog && std::thread::current().name() == Some("enchudb-consumer") {
            let taken = hold.lock().unwrap().take();
            if let Some((parked, release)) = taken {
                let _ = parked.send(());
                let _ = release.recv();
            }
        }
    })));
    // 書き出すものを渡す (head > checkpoint で周期の書き出しに入る)
    eng.oplog_commit();
    parked_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("前提: consumer が周期の oplog の書き出し (写した直後) で止まる");
    ConsumerHeld(Some(release_tx))
}

/// 0.37 (#440 の 2 区画): 本体の書き出しが届かせた bridge の位置は、 書き出しを始めた時の位置。 書き出し終えた後 (位置を置く前)
/// に bridge が写した行は書き出されていないので、 その行の record を含む WAL は畳んでよいと言わない (写した行の数も、 書き出した
/// 位置も追いつかない)。 もう一度書き出せば言う。 2 区画の形は、 書き手が書き続ける間もこの位置で古い区画を畳む。
#[test]
fn rows_bridged_after_the_write_out_do_not_let_the_wal_fold() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("bridged_after_write_out");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let eng = notes_engine(&live.join("db"), 4 << 20);
    for i in 0..10u32 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_sync().unwrap();
    // consumer を止めておく (周期の書き出しが間に入ると、 写した行が書き出されてしまう)
    let held = hold_consumer(&eng);
    // 止める時に consumer が打った Commit も写してから書き出す
    eng.transfer_oplog_to_sync_ops();
    eng.body_msync().unwrap();
    assert!(eng.wal_fold_safe(), "前提: 写した行を書き出した後は畳める");
    for i in 10..20u32 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    // 書き出し終えた後、 位置を置く前に bridge が写す
    let bridged = Arc::new(AtomicU32::new(0));
    {
        let (eng, bridged) = (eng.clone(), bridged.clone());
        let once = std::sync::Mutex::new(true);
        write_out_hook::set(Some(Arc::new(move |phase| {
            if phase == Phase::WrittenOut && std::mem::take(&mut *once.lock().unwrap()) {
                bridged.store(eng.transfer_oplog_to_sync_ops() as u32, Ordering::Release);
            }
        })));
    }
    eng.body_msync().unwrap();
    write_out_hook::set(None);
    assert!(bridged.load(Ordering::Acquire) > 0, "前提: 書き出しの後に bridge が写した");
    assert!(!eng.wal_fold_safe(), "書き出しの後に写した行を書き出す前に畳んでよいと言った");
    eng.body_msync().unwrap();
    assert!(eng.wal_fold_safe(), "写した行を書き出した後も畳めない");
    drop(held);
    crashsim::stop();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #451 (段階 1): 配り済みの印が disk にあっても、 印の後に書いた write が本体にだけ届いた像では作り直す (印は、 印より前の
/// write だけを飛ばす)。 像には印 (切れ目 = 止めた `oplog_sync` の Commit の前 = 最初の 2 つの write の後) が入り、 その後の
/// 5 つの write は oplog に無い。
#[test]
fn writes_after_a_durable_mark_are_still_relogged_after_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for attempt in 0..5 {
        let root = scratch(&format!("mark_after{attempt}"));
        let live = root.join("live");
        std::fs::create_dir_all(&live).unwrap();
        crashsim::start();
        let eng = notes_engine(&live.join("db"), OPLOG_CAP);
        let [a, b, c, d, e] = [(); 5].map(|_| eng.entity_in("notes").unwrap());
        eng.tie_to(a, "notes.note", 1);
        eng.tie_to(e, "notes.note", 5);
        eng.oplog_sync().unwrap();
        eng.oplog_sync().unwrap();
        let mark0 = eng.durable_mark().expect("前提: 2 回目の書き出しで印が置かれる");
        let held = with_stalled_sync(&eng, || {
            // 書く間に consumer が目を覚まして Commit を打つと、 止めた sync の bridge がこの write を行にし、 下の本体の書き出しで
            // 届く (= 作り直さなくてよい形になる)。 その Commit を 1 回だけ失敗させて、 5 つとも閉じていない group に残す
            eng.oplog().unwrap().fail_next_commits(1);
            eng.tie_to(b, "notes.note", 22);
            eng.tie_text_to(c, "notes.tag", "fresh-word");
            eng.tie_text_to(d, "notes.body", "leaf body");
            eng.untie(a, "notes.note");
            eng.delete(e);
        });
        // 止めた側の本体の書き出しは済んでいる。 止めた sync が置いた印 (切れ目は書く前) も ring の書き出しで届かせる
        eng.body_msync().unwrap();
        let img = root.join("img");
        let captured = crashsim::capture(&live, &img, Mode::Lost);
        crashsim::stop();
        drop(held);
        let hid = |n: &str| eng.himo_id(n).unwrap() as u16;
        let (note, tag, body) = (hid("notes.note"), hid("notes.tag"), hid("notes.body"));
        let before = [eng.cell_hlc(b, note), eng.cell_hlc(c, tag), eng.cell_hlc(d, body), eng.cell_hlc(a, note)];
        let deleted_at = eng.tombstone_hlc(e);
        drop(eng);
        captured.unwrap();
        let db = img.join("db");
        let in_oplog = OpLog::open(&db.join("oplog"))
            .unwrap()
            .records_with_tail()
            .iter()
            .filter(|r| before.contains(&r.hlc) || r.hlc == deleted_at)
            .count();
        if in_oplog > 0 {
            eprintln!("[mark_after] 撮り直す ({attempt} 回目): consumer の書き出しが先に oplog を届かせた");
            let _ = std::fs::remove_dir_all(&root);
            continue;
        }
        // 像の印: 止めた sync の切れ目 = 最初の 2 つの write より後、 5 つの write より前
        let img_mark = PayloadRing::read_durable_mark(&db).expect("像に印が無い (ring の書き出しで届くはず)");
        assert!(img_mark.hlc >= mark0.hlc, "印は進む: {img_mark:?} (前 {mark0:?})");
        assert!(
            before.iter().all(|h| *h > img_mark.hlc) && deleted_at > img_mark.hlc,
            "前提: 5 つの write は印より後 {img_mark:?} {before:?} {deleted_at:?}"
        );
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        assert_eq!(eng.get(b, "notes.note"), Some(22), "前提: 本体に届いている");
        assert_eq!(eng.unlogged_writes_relogged(), 5, "印の後の write を作り直していない (印が覆った)");
        assert_eq!(floor_of(&eng, 1), None, "作り直せたので floor は上げない");
        let recs = distributed(&eng);
        let local = |x: u64, y: &u64| eid_local(x) == eid_local(*y);
        assert!(
            recs.iter().any(|r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, value } if local(b, eid) && *himo_id == note && *value == 22)),
            "作り直した値が配る分に無い"
        );
        drop(eng);
        let _ = std::fs::remove_dir_all(&root);
        return;
    }
    panic!("5 回とも consumer の書き出しが先に oplog を届かせ、 前提を作れなかった");
}

/// #451 (段階 1): WAL が満杯で落ちた write (#57) の floor がまだ無い間は、 配り済みの印を進めない。 consumer の書き出しを止めて
/// (consumer の bridge が先に落ちた record を持ち越さない = 切れ目は WAL が覚えている落ちた record を見る形)、 落とした直後に
/// `oplog_sync` を走らせる (その bridge が持ち越す、 floor はまだ上げない) → 印は前のまま。 本体と ring を書き出してから電源断の像を
/// 撮り、 開いた時に落ちた write が floor に覆われるか作り直されること (印が覆うと、 開く時に見ず相手に永久に届かない)。
#[test]
fn a_mark_never_covers_a_dropped_write_before_its_floor_is_on_disk() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("mark_dropped");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let eng = notes_engine(&live.join("db"), 64 * 1024);
    // #667: 満杯の状態を手で作るので、 区画を移った時の畳みを切る (移った直後に consumer の Commit が入る)
    eng.set_wal_fold_on_switch(false);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let wal = eng.oplog().unwrap().clone();
    // 満杯の書き手を待たせない (#388)
    wal.set_room_waiter(None);
    // 印を 1 つ置いておく
    let e0 = eng.entity_in("notes").unwrap();
    eng.tie_to(e0, "notes.note", 100);
    eng.oplog_sync().unwrap();
    // consumer を周期の oplog の書き出し (写した直後) で止めておく = その bridge は走らず、 埋まった WAL の空き作りにも入らない。
    // 印を読むのは止めた後: 止める前に始まった周期 (切れ目は落とす前) が、 読んだ後に印を置くことがある (遅い disk で踏んだ)
    let held = hold_consumer(&eng);
    let before = eng.durable_mark().expect("前提: 印が置かれている");
    let mut dropped = Vec::new();
    {
        let _no_fold = eng.transfer_lock_for_fold();
        // 2 区画: A を埋めて B へ移る (A は畳めないまま残るので、 空きは B の残りだけ)
        let filler = eng.entity_in("notes").unwrap();
        let switched = wal.switches();
        let mut i = 0u32;
        while wal.switches() == switched {
            eng.tie_to(filler, "notes.note", i);
            eng.oplog_commit();
            i += 1;
        }
        // 閉じた group で WAL を埋め、 残りを 「Commit は入るが Tie (+ Commit の分の空き) は入らない」 大きさにする
        let e1 = eng.entity_in("notes").unwrap();
        let h0 = wal.head();
        eng.tie_text_to(e1, "notes.body", "x");
        let c = wal.head() - h0 - 1;
        eng.oplog_commit();
        let commit = wal.record_size_of(0) as u64;
        let pad = wal.free_bytes() - commit - (commit + 60) - c;
        let e2 = eng.entity_in("notes").unwrap();
        eng.tie_text_to(e2, "notes.body", &"p".repeat(pad as usize));
        eng.oplog_commit();
        assert_eq!(wal.free_bytes(), commit + 60, "前提: Commit は入るが Tie は入らない残り");
        for i in 0..5u32 {
            let head = wal.head();
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
            assert_eq!(wal.head(), head, "前提: WAL に載らない");
            dropped.push(eng.cell_hlc(e, note));
        }
    }
    // 落とした直後の書き出し = 印を置く機会。 切れ目は WAL が覚えている落ちた record を見て候補を出さない。 bridge は落ちた record を
    // 持ち越すが floor はまだ上げない (静かになってから)
    eng.oplog_sync().unwrap();
    assert_eq!(eng.durable_mark(), Some(before), "落ちた write の floor が無いうちは印を進めない");
    // 置いた印があれば ring の書き出しで届く
    eng.body_msync().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(held);
    drop((wal, eng));
    captured.unwrap();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), 64 * 1024).unwrap();
    let max_dropped = dropped.iter().copied().max().unwrap();
    let floor = floor_of(&eng, 1);
    let relogged = eng.unlogged_writes_relogged();
    assert!(
        floor.is_some_and(|f| f >= max_dropped) || relogged == 5,
        "落ちた write {max_dropped:?} が floor {floor:?} に覆われず、 作り直し ({relogged}) もされない (印が覆った)"
    );
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #451 (段階 1): 印より後の `_sync_ops` の行は開く時に読む。 印 (W, S) が disk にある像で、 W より後の write が oplog には
/// 無く (consumer の書き出しを止める)、 行 (lsn > S) にだけある → 作り直さない (旧 #451 と同じ答え。 行を見なければ重ねて配る)。
/// 像は consumer の書き出しを止めて手で bridge した形 (本物では record が届いてから行ができる) で、 開く側の約束だけを見る。
#[test]
fn a_write_known_only_from_a_row_after_the_mark_is_not_relogged() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("mark_row");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let eng = notes_engine(&live.join("db"), OPLOG_CAP);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1);
    // consumer を先に止める: 止める前に始まった周期が、 印を読んだ後に別の印を置くと像の印が前提と違う (遅い disk で踏む形)
    let held = hold_consumer(&eng);
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    let mark = eng.durable_mark().expect("前提: 印");
    // 印を届かせる
    eng.body_msync().unwrap();
    let x = eng.entity_in("notes").unwrap();
    eng.tie_to(x, "notes.note", 7);
    let hx = eng.cell_hlc(x, note);
    assert!(hx > mark.hlc, "前提: x は印より後");
    eng.oplog_commit();
    assert!(eng.transfer_oplog_to_sync_ops() > 0, "前提: x の record を行に写す");
    // 行 (と payload) を届かせる。 oplog は fsync しない (consumer は止めている)
    eng.body_msync().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(held);
    drop(eng);
    captured.unwrap();
    let db = img.join("db");
    let in_oplog = OpLog::open(&db.join("oplog")).unwrap().records_with_tail().iter().filter(|r| r.hlc == hx).count();
    assert_eq!(in_oplog, 0, "前提: x の record は像の oplog に無い");
    assert_eq!(PayloadRing::read_durable_mark(&db), Some(mark), "前提: 像の印");
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.get(x, "notes.note"), Some(7), "前提: 本体に届いている");
    assert_eq!(eng.unlogged_writes_relogged(), 0, "行にある write を作り直した (印より後の行を見ていない)");
    assert_eq!(floor_of(&eng, 1), None);
    let recs = distributed(&eng);
    let ties = recs
        .iter()
        .filter(|r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, value } if eid_local(*eid) == eid_local(x) && *himo_id == note && *value == 7))
        .count();
    assert_eq!(ties, 1, "x の Tie は配る分に 1 つ (重ねて配らない)");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #451 (段階 1): 置いた印は、 書くものの無い周期に consumer が ring だけ書き出す (書き終えて暇になった後に kill されても印が残る)。
#[test]
fn the_mark_reaches_disk_while_idle() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("mark_idle");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let eng = notes_engine(&live.join("db"), OPLOG_CAP);
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1);
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    let m = eng.durable_mark().expect("前提: 印");
    // 置いただけ (ring の書き出しは次の周期)。 暇な周期を待つ (sync の DB の consumer は 1 秒ごとに起きる)
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(eng);
    captured.unwrap();
    // 読んだ後に、 読む前に始まった consumer の周期が (後の切れ目で) 印を進めていてもよい: 届くのは最後に置いた印 = m 以上
    let on_disk = PayloadRing::read_durable_mark(&img.join("db"));
    assert!(on_disk.is_some_and(|d| (d.hlc, d.lsn) >= (m.hlc, m.lsn)), "暇な周期に印が届いていない: {on_disk:?} (置いた印 {m:?})");
    let _ = std::fs::remove_dir_all(&root);
}

/// #451 (段階 1): 印は、 切れ目の後に始めた oplog の fsync (と本体の書き出し) が返ってから置く。 fsync を写した直後で止め
/// (record はまだ disk に無い)、 その間に本体と ring を書き出して像を撮る: 像に印があっても、 止めた fsync に入るはずだった
/// write (切れ目より前) は覆わない (書き出しの前に置くと、 印が disk に届いた後に電源が落ちた時、 覆われた write が
/// oplog に無いのに開く時に見ない = 相手に永久に届かない)。
#[test]
fn a_mark_is_not_on_disk_before_the_records_it_covers() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for attempt in 0..5 {
        let root = scratch(&format!("mark_before{attempt}"));
        let live = root.join("live");
        std::fs::create_dir_all(&live).unwrap();
        crashsim::start();
        let eng = notes_engine(&live.join("db"), OPLOG_CAP);
        let note = eng.himo_id("notes.note").unwrap() as u16;
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1);
        eng.oplog_sync().unwrap();
        eng.oplog_sync().unwrap();
        let mark0 = eng.durable_mark().expect("前提: 印");
        // 止める fsync に入る write (切れ目より前)
        let f = eng.entity_in("notes").unwrap();
        eng.tie_to(f, "notes.note", 6);
        let hf = eng.cell_hlc(f, note);
        let img = root.join("img");
        let mut captured = None;
        let held = with_stalled_sync(&eng, || {
            // fsync が返る前に、 本体と ring (置いた印があれば) を書き出して像を撮る (consumer の Commit は 1 回失敗させる = 上と同じ)
            eng.oplog().unwrap().fail_next_commits(1);
            eng.body_msync().unwrap();
            captured = Some(crashsim::capture(&live, &img, Mode::Lost));
        });
        crashsim::stop();
        drop(held);
        drop(eng);
        captured.unwrap().unwrap();
        let db = img.join("db");
        let in_oplog = OpLog::open(&db.join("oplog")).unwrap().records_with_tail().iter().filter(|r| r.hlc == hf).count();
        if in_oplog > 0 {
            eprintln!("[mark_before] 撮り直す ({attempt} 回目): consumer の書き出しが先に oplog を届かせた");
            let _ = std::fs::remove_dir_all(&root);
            continue;
        }
        let img_mark = PayloadRing::read_durable_mark(&db);
        assert!(
            img_mark.is_some_and(|m| m.hlc < hf),
            "像の印 {img_mark:?} が、 oplog に届いていない write {hf:?} を覆う (書き出しが返る前に置いた)。 前の印 {mark0:?}"
        );
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        assert_eq!(eng.get(f, "notes.note"), Some(6), "前提: 本体に届いている");
        assert_eq!(eng.unlogged_writes_relogged(), 1, "oplog に無い write を作り直していない");
        drop(eng);
        let _ = std::fs::remove_dir_all(&root);
        return;
    }
    panic!("5 回とも consumer の書き出しが先に oplog を届かせ、 前提を作れなかった");
}

/// #451 (段階 1): 印は oplog の位置 (その周の、 切れ目の時の終わり) も持ち、 開く時の header の走査はそこから (それより前の自分の
/// record は全部切れ目以下)。 切れ目の後に載って oplog にだけある (行には無い) record はそこから先で見つかる → 作り直さない。
/// 止めた `oplog_sync` (切れ目の後、 fsync の途中) の間に x を書き、 その後に oplog だけ書き出す (bridge はしない)。
#[test]
fn a_write_known_only_from_the_wal_after_the_mark_is_not_relogged() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("mark_wal");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let eng = notes_engine(&live.join("db"), OPLOG_CAP);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1);
    eng.oplog_sync().unwrap();
    let x = eng.entity_in("notes").unwrap();
    let held = with_stalled_sync(&eng, || {
        // 止めた sync の切れ目の後 (Commit の後) に載る = 閉じていない group、 その sync の bridge は行にしない (consumer が目を覚まして
        // 打つ Commit は 1 回失敗させる: 通ると x が行になり、 下の本体の書き出しで届く)
        eng.oplog().unwrap().fail_next_commits(1);
        eng.tie_to(x, "notes.note", 7);
    });
    let hx = eng.cell_hlc(x, note);
    // x の record を届かせ (bridge はしない)、 止めた sync が置いた印 (位置は x の手前) と本体も届かせる
    eng.oplog().unwrap().fsync().unwrap();
    eng.body_msync().unwrap();
    let img = root.join("img");
    let captured = crashsim::capture(&live, &img, Mode::Lost);
    crashsim::stop();
    drop(held);
    drop(eng);
    captured.unwrap();
    let db = img.join("db");
    let in_oplog = OpLog::open(&db.join("oplog")).unwrap().records_with_tail().iter().filter(|r| r.hlc == hx).count();
    assert_eq!(in_oplog, 1, "前提: x の record は像の oplog にある");
    let mark = PayloadRing::read_durable_mark(&db).expect("前提: 像に印");
    assert!(mark.hlc < hx, "前提: 印の切れ目は x より前: {mark:?} {hx:?}");
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.get(x, "notes.note"), Some(7), "前提: 本体に届いている");
    assert_eq!(eng.unlogged_writes_relogged(), 0, "oplog にある write を作り直した (印の位置から先を読んでいない)");
    assert_eq!(floor_of(&eng, 1), None);
    let recs = distributed(&eng);
    let ties = recs
        .iter()
        .filter(|r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, value } if eid_local(*eid) == eid_local(x) && *himo_id == note && *value == 7))
        .count();
    assert_eq!(ties, 1, "x の Tie は配る分に 1 つ (重ねて配らない)");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #858: `_sync_ops` の行の cell (lsn を含む) は届いたのに、 生きている印 (`entities.seg`) が届かなかった像。 本体の書き出しは
/// 生きている印 → lsn の列 の順なので、 書き出しの順だけでは残らない。 残るのは、 OS が書き出し (msync) の前に lsn の列の
/// page だけを先に書いた時 (`Mixed` の像)。 開いた後、 その slot は 「生きていないのに lsn の列に居る行」 で、 払い出しの
/// 穴拾い (`rebuild_free_locals`) が穴として拾い、 ack の後の回収 (`purge_sync_ops_row`) も行として消して free list に
/// 戻す = 同じ slot が 2 度 free list に載り、 2 つの record が同じ slot に置かれて片方が消える。
#[test]
fn a_ring_row_whose_live_bit_did_not_reach_disk_is_not_handed_out_twice() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("ghost_row");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    // 1 枚の像が 「印の無い行」 を持つ率は Mac で 1/16 前後 (lsn の列の cell の page と列の header の page が今の中身、 生きている
    // 印の page が書き出し済みの中身、 の組)。 160 枚で 1 枚も無い率は (15/16)^160 ≈ 3e-5
    const SEEDS: u64 = 160;
    crashsim::start();
    let eng = notes_engine(&db, OPLOG_CAP);
    let e = eng.entity_in("notes").unwrap();
    {
        // consumer の周期の書き出しを止める (書き出すのは下の `oplog_sync` だけ)
        let _hold = eng.hold_consumer();
        for v in 1..=5u32 {
            eng.tie_to(e, "notes.note", v);
        }
        // 書き出し → bridge: ring の行はこの書き出しの後に置かれる = まだ書き出していない (page cache にだけある)
        eng.oplog_sync().unwrap();
        for seed in 0..SEEDS {
            crashsim::capture(&live, &root.join(format!("img{seed}")), Mode::Mixed { seed }).unwrap();
        }
    }
    crashsim::stop();
    drop(eng);

    let mut ghost_images = 0;
    for seed in 0..SEEDS {
        let img = root.join(format!("img{seed}"));
        let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let lsn_hid = eng.himo_id("_sync_ops.lsn").unwrap() as u16;
        let ghosts = eng.entities_with_himo(lsn_hid).into_iter().filter(|r| !eng.is_live(*r)).count();
        if ghosts == 0 {
            drop(eng);
            let _ = std::fs::remove_dir_all(&img);
            continue;
        }
        ghost_images += 1;
        // 開いた後の流れ: 写し直し → 相手が全部受けて ack → 回収 → 新しい record を写す。 新しい record は 1 つも欠けない
        let _hold = eng.hold_consumer();
        while eng.transfer_oplog_to_sync_ops() > 0 {}
        let old: Vec<u64> = eng
            .pending_sync_ops(0)
            .iter()
            .filter_map(|p| decode_sync_ops_payload(p))
            .filter_map(|r| match r.op {
                DecodedOp::Tie { value, .. } => Some(value),
                _ => None,
            })
            .collect();
        for v in 1..=5u64 {
            assert!(old.contains(&v), "seed {seed}: 落ちる前に書いた record {v} が、 開いた後の ring に無い ({old:?})");
        }
        eng.ack_sync(77, eng.current_sync_lsn() + 1).unwrap();
        eng.reclaim_sync_ops();
        // 穴拾いが 1 回に拾う数 (4096) より多く書く: 2 度載った slot の 2 つ目は、 先に拾った穴を使い切った後に出てくる
        let wrote: Vec<u64> = (100..6_100u64).collect();
        for v in &wrote {
            eng.tie_to(e, "notes.note", *v as u32);
            if v % 500 == 0 {
                eng.oplog_sync().unwrap();
            }
        }
        eng.oplog_sync().unwrap();
        let on_ring: Vec<u64> = eng
            .pending_sync_ops(0)
            .iter()
            .filter_map(|p| decode_sync_ops_payload(p))
            .filter_map(|r| match r.op {
                DecodedOp::Tie { value, .. } if value >= 100 => Some(value),
                _ => None,
            })
            .collect();
        assert_eq!(on_ring, wrote, "seed {seed} (印の無い行 {ghosts} 個): 開いた後に書いた record が ring から欠けた / 重なった");
        let rows = eng.entities_with_himo(lsn_hid);
        let mut lsns: Vec<u64> = rows.iter().filter_map(|r| eng.get_by_id(*r, lsn_hid)).collect();
        let n = lsns.len();
        lsns.sort_unstable();
        lsns.dedup();
        assert_eq!(lsns.len(), n, "seed {seed}: ring の 2 つの行が同じ lsn を持っている");
        assert!(rows.iter().all(|r| eng.is_live(*r)), "seed {seed}: 生きている印の無い行が ring に残っている");
        drop(_hold);
        drop(eng);
        let _ = std::fs::remove_dir_all(&img);
    }
    eprintln!("#858: 印の無い行のある像 {ghost_images} / {SEEDS}");
    assert!(ghost_images > 0, "前提: {SEEDS} 枚の像のどれかに、 cell は届いて生きている印の届いていない ring の行がある");
    let _ = std::fs::remove_dir_all(&root);
}

/// #731: 最初の使い回しで file version 15 に移る途中の電源断。 移る手順は `gen.seg` (v14 の間に消した場所の世代 1) を書き出して
/// から header (場所の bit と version 15) を書き出す。
///
/// - `gen.seg` だけ届いた像 (header を書く直前): v14 のまま開け、 世代を持たない。 そこで使い回すと v15 に移り直し、 使い回した
///   場所は消えた行と違う eid になる (前の残りの `gen.seg` を 0 から作り直す)
/// - 移った後の像: v15 で開け、 消えた行の eid は 「消えた行」 (v14 の間に消した場所も世代 1)
#[test]
fn moving_to_eid_generations_writes_the_generations_before_the_header() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("gen_move");
    let live = root.join("live");
    let (before, after) = (root.join("before"), root.join("after"));
    let version = |dir: &Path| {
        let h = std::fs::read(dir.join("header.seg")).unwrap();
        u32::from_le_bytes(h[4..8].try_into().unwrap())
    };
    crashsim::start();
    let (a, b) = {
        let mut eng = Engine::create_with_capacity(live.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        let a = eng.entity_in("t").unwrap();
        eng.tie_to(a, "t.v", 1u32);
        let b = eng.entity_in("t").unwrap();
        eng.tie_to(b, "t.v", 2u32);
        eng.delete(a); // v14 の間に消す
        eng.flush().unwrap();
        let (l, img) = (live.clone(), before.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::GenerationsBeforeHeader {
                crashsim::capture(&l, &img, Mode::Lost).unwrap();
            }
        })));
        let c = eng.entity_in("t").unwrap(); // 満杯の表で a の場所を使い回す = v15 に移る
        write_out_hook::set(None);
        assert_ne!(c, a, "前提: 使い回した場所は別の eid");
        crashsim::capture(&live, &after, Mode::Lost).unwrap();
        (a, b)
    };
    crashsim::stop();
    assert!(before.join("gen.seg").is_file(), "前提: header の前に gen.seg が届いている像");
    assert!(version(&before) < 15, "header の前の像が v15 になっている ({})", version(&before));
    {
        let eng = Engine::open_standalone(before.to_str().unwrap()).unwrap();
        assert_eq!(eng.slot_bits(), 0, "v14 の像で世代を読んだ");
        assert_eq!(eng.get(b, "t.v"), Some(2));
        assert!(!eng.is_live(a));
        let c = eng.entity_in("t").unwrap();
        assert_eq!(eng.eid_slot(c), eng.eid_slot(a), "前提: もう一度 a の場所を使い回す");
        assert_ne!(c, a, "移り直した後の使い回しが消えた行の eid を渡した");
        assert_eq!(eng.get(a, "t.v"), None);
    }
    assert_eq!(version(&after), 15, "移った後の像が v15 でない");
    {
        let eng = Engine::open_standalone(after.to_str().unwrap()).unwrap();
        assert_eq!(eng.slot_bits(), 24);
        assert_eq!(eng.get(b, "t.v"), Some(2), "世代 0 の生きている行が読めない");
        assert!(!eng.is_live(a), "v14 の間に消した行の eid が生きている");
        assert_eq!(eng.get(a, "t.v"), None);
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// #731 (PR #885 の読み直し F1): 本体の書き出しは生きている印 (entities) を先に、 場所の世代 (`gen.seg`) を後に書き出す。
/// その間に行を消すと 「生きている印は残り、 世代は上がった」 像になる (消す道は 世代を上げる → 印を下ろす)。 消した record は
/// 書き出しの cut の後 = 電源断で失われる。 前は開くと行は生きているのに、 アプリの eid (と、 その行を指す Ref) が 「消えた行」 になった。
/// 直し (案 (a)): 消す道は世代と一緒に 「死んでいる」 印を立て、 開く時の突き合わせが 「死んでいる印のある生きている場所」 の削除を
/// 完了させる (死が勝つ。 消した record は届いていないが、 本体に届いた削除を全部当てる = 「消した後」 の像)。 書き出しは消す書き手を
/// 止めない (前の直し `Generations::freeze` は、 消す 1 回の最大の待ちが msync に比例して伸びた)。
#[test]
fn a_delete_between_the_live_bits_and_the_generations_does_not_change_a_surviving_rows_eid() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("gen_torn");
    let live = root.join("live");
    let img = root.join("img");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let (r, keep) = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        // v15 にする (枠 2 の表で消して足す)
        let a = eng.entity_in("t").unwrap();
        eng.tie_to(a, "t.v", 1u32);
        let keep = eng.entity_in("t").unwrap();
        eng.tie_to(keep, "t.v", 2u32);
        eng.delete(a);
        let r = eng.entity_in("t").unwrap();
        eng.tie_to(r, "t.v", 3u32);
        assert_ne!(r, a, "前提: v15");
        eng.oplog_sync().unwrap();
        let _hold = eng.hold_consumer();
        let (e2, done) = (eng.clone(), Arc::new(AtomicBool::new(false)));
        let done2 = done.clone();
        // 生きている印を書き出した後・世代を書き出す前に、 別の thread で消す (消す書き手は書き出しを待たない。 待つなら 300 ms で
        // 書き出しを進め、 下の前提 (突き合わせが戻した場所が 1 つ) で落ちる)
        let deleter: Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::default();
        let deleter2 = deleter.clone();
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGenerations && !done2.swap(true, Ordering::SeqCst) {
                let (e3, (tx, rx)) = (e2.clone(), std::sync::mpsc::channel::<()>());
                *deleter2.lock().unwrap() = Some(std::thread::spawn(move || {
                    e3.delete(r);
                    let _ = tx.send(());
                }));
                let _ = rx.recv_timeout(std::time::Duration::from_millis(300));
            }
        })));
        eng.tie_to(keep, "t.v", 4u32); // 書き出す物を作る
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(done.load(Ordering::SeqCst), "前提: 書き出しの間に消した");
        deleter.lock().unwrap().take().unwrap().join().unwrap();
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
        (r, keep)
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.get(keep, "t.v"), Some(4), "前提: 書き出した値は届いている");
    assert_eq!(eng.generation_repairs().0, 1, "前提: 世代と死んでいる印だけ届いた (生きている印は消す前の) 場所が 1 つ");
    // 場所が生きていれば、 その eid は r のまま (前は 「生きている、 世代だけ上がった」 で、 生きている行の eid が変わった)。 突き合わせは
    // 削除を完了させる。 checkpoint が #870 で止まっていれば r の Tie を再生して r の世代に戻す (消した record は届いていない = WAL では
    // r は生きている)。 どちらでも、 場所を使い回す行は r と違う eid
    let s = eng.eid_slot(r);
    let now = eng.slot_eid(s);
    assert!(!eng.is_live(now) || now == r, "消した record の届かなかった像で、 生きている行の eid が変わった ({r:#x} → {now:#x}、 世代だけ届いた)");
    if !eng.is_live(r) {
        let n = eng.entity_in("t").unwrap();
        assert_eq!(eng.eid_slot(n), s, "前提: 消した場所を使い回す");
        assert_ne!(n, r, "削除を完了させた場所を、 消えた行の eid で使い回した");
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// #731 F1 (案 (a)) の突き合わせの規則: 削除が書き出し済みの場所 (「次の世代 g + 1、 死んでいる」) を、 本体の書き出しの途中
/// (生きている印を書き出す前) に使い回して落ちる。 像は 「生きている印は使い回しの後 (生きている)、 gen.seg は前の書き出し
/// (g + 1、 死んでいる)」 で、 使い回しの record は cut の後 = 届かない。 「gen.seg だけ死 = 削除が届いていない → 世代を戻す」
/// と読むと、 消えた行 (世代 g、 削除は checkpoint を越えた) の eid が生きている行になる。 死が勝つ規則は場所を死なせる。
#[test]
fn a_reuse_torn_between_the_live_bits_and_the_generations_does_not_bring_back_a_deleted_eid() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("gen_torn_reuse");
    let live = root.join("live");
    let img = root.join("img");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let (r, n) = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        let a = eng.entity_in("t").unwrap();
        eng.tie_to(a, "t.v", 1u32);
        let keep = eng.entity_in("t").unwrap();
        eng.tie_to(keep, "t.v", 2u32);
        eng.delete(a);
        let r = eng.entity_in("t").unwrap(); // v15 に移って a の場所を使い回す (世代 1)
        eng.tie_to(r, "t.v", 3u32);
        assert_ne!(r, a, "前提: v15");
        eng.delete(r); // 場所は 「世代 2、 死んでいる」
        eng.oplog_sync().unwrap(); // r の削除は書き出し済み (checkpoint を越えた)
        let _hold = eng.hold_consumer();
        let (e2, made) = (eng.clone(), Arc::new(std::sync::Mutex::new(None)));
        let made2 = made.clone();
        let (l, img2) = (live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::Opened && made2.lock().unwrap().is_none() {
                // 生きている印を書き出す前に使い回す (record は cut の後)
                let n = e2.entity_in("t").unwrap();
                e2.tie_to(n, "t.v", 5u32);
                *made2.lock().unwrap() = Some(n);
            }
            if p == Phase::BeforeGenerations {
                crashsim::capture(&l, &img2, Mode::Lost).unwrap();
            }
        })));
        eng.tie_to(keep, "t.v", 4u32); // 書き出す物を作る
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        let n = made.lock().unwrap().expect("前提: 書き出しの途中で使い回した");
        assert_eq!(eng.eid_slot(n), eng.eid_slot(r), "前提: r の場所を使い回した");
        (r, n)
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.generation_repairs(), (1, 0), "前提: 生きている印は使い回しの後、 gen.seg は死んでいる像");
    assert!(!eng.is_live(r), "削除の書き出し済みの行 {r:#x} の eid が、 落ちた後に生きている");
    assert!(!eng.is_live(n), "前提: 使い回しの record は届いていない");
    let s = eng.eid_slot(r);
    assert!(!eng.is_live(eng.slot_eid(s)), "使い回しの record の届かなかった場所が生きている");
    // 次の使い回しは、 削除の届いた r とも a とも違う。 届かなかった使い回しの eid (n) は、 再生が r の record で場所を r の世代に
    // 戻すともう一度出うる (位置から払い出した行の record が届かなかった時と同じ = 落ちる前にアプリへ渡った、 WAL に届いていない eid)
    let m = eng.entity_in("t").unwrap();
    assert_eq!(eng.eid_slot(m), s, "前提: 同じ場所を使い回す");
    assert_ne!(m, r, "落ちた後の使い回しが削除の届いた行の eid ({m:#x}) を渡した");
    let _ = std::fs::remove_dir_all(&root);
}

/// #731 (PR #885 の読み直し F2): 本体にあって oplog に無い削除を作り直す (#451) Delete の record は、 消えた行の世代込みの eid
/// (WAL の record は世代込み)。 前は世代 0 の番号で作り直していた (使い回した場所の行を消した record が、 前の住人の番号になる)。
#[test]
fn a_relogged_delete_carries_the_generation_of_the_deleted_row() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for attempt in 0..5 {
        let root = scratch(&format!("relog_gen{attempt}"));
        let live = root.join("live");
        std::fs::create_dir_all(&live).unwrap();
        crashsim::start();
        let db = live.join("db");
        let eng = {
            let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
            eng.define_table("notes", 2).unwrap();
            eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
            eng.enable_sync_tables().unwrap();
            eng.flush().unwrap();
            eng.persist_tables().unwrap();
            let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
            eng.set_peer_id(1);
            eng
        };
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        let b = eng.entity_in("notes").unwrap();
        eng.tie_to(b, "notes.note", 2u32);
        eng.delete(a);
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", 5u32);
        assert_ne!(e, a, "前提: 使い回した場所は世代の違う eid");
        eng.oplog_sync().unwrap();
        let held = with_stalled_sync(&eng, || eng.delete(e));
        let img = root.join("img");
        let captured = crashsim::capture(&live, &img, Mode::Lost);
        crashsim::stop();
        drop(held);
        let deleted_at = eng.tombstone_hlc(e);
        drop(eng);
        captured.unwrap();
        let idb = img.join("db");
        let in_oplog = OpLog::open(&idb.join("oplog")).unwrap().records_with_tail().iter().any(|r| r.hlc == deleted_at);
        if in_oplog {
            eprintln!("[relog_gen] 撮り直す ({attempt} 回目): consumer の書き出しが先に oplog を届かせた");
            let _ = std::fs::remove_dir_all(&root);
            continue;
        }
        let eng = Engine::open_concurrent_with_oplog(idb.to_str().unwrap(), OPLOG_CAP).unwrap();
        assert!(!eng.is_live(e), "前提: 本体には削除が届いている");
        assert!(eng.unlogged_writes_relogged() >= 1, "前提: 削除を作り直した");
        let relogged: Vec<u64> = eng
            .audit(&enchudb_engine::AuditFilter::default())
            .into_iter()
            .filter(|r| r.hlc == deleted_at)
            .filter_map(|r| match r.op {
                DecodedOp::Delete { eid } => Some(eid),
                _ => None,
            })
            .collect();
        assert_eq!(relogged.len(), 1, "前提: 作り直した Delete が 1 つ");
        assert_eq!(eid_local(relogged[0]), eid_local(e), "作り直した Delete が、 消えた行の世代込みの eid でない");
        drop(eng);
        let _ = std::fs::remove_dir_all(&root);
        return;
    }
    panic!("5 回とも consumer が先に oplog を届かせた");
}

/// 書き出しの段 `when` で、 別の thread で `eid` を消す。 消し終えるか、 削除の門で待ち始めたら (#978、 `Phase::DeleteGateWait`) 書き出しを
/// 進める (時間では待たない、 読み直し [9])。 段 `shot` で像を撮る。
fn delete_during_write_out(eng: &Arc<Engine>, eid: u64, when: Phase, shot: Phase, live: &Path, img: &Path) -> Arc<AtomicBool> {
    let (e2, done, (l, i)) = (eng.clone(), Arc::new(AtomicBool::new(false)), (live.to_path_buf(), img.to_path_buf()));
    let (done2, shot_once) = (done.clone(), AtomicBool::new(false));
    let waiting: Arc<std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>> = Arc::default();
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::DeleteGateWait
            && let Some(tx) = waiting.lock().unwrap().take()
        {
            let _ = tx.send(());
        }
        if p == when && !done2.swap(true, Ordering::SeqCst) {
            let (e3, (tx, rx)) = (e2.clone(), std::sync::mpsc::channel::<()>());
            *waiting.lock().unwrap() = Some(tx.clone());
            std::thread::spawn(move || {
                e3.delete(eid);
                let _ = tx.send(());
            });
            rx.recv_timeout(std::time::Duration::from_secs(60))
                .expect("削除が 60 秒終わらず、 削除の門で待ち始めた印 (`Phase::DeleteGateWait`) も来ない");
        }
        // 撮るのは消した書き出しの段で 1 回だけ (`flush_clean` は本体を 2 回書き出す。 2 回目で撮ると揃った像になる)
        if p == shot && done2.load(Ordering::SeqCst) && !shot_once.swap(true, Ordering::SeqCst) {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
        }
    })));
    done
}

/// PR #885 の読み直し 1: 突き合わせが削除を完了させる時に cell を落とすと、 再生がその行を生かし直した時 (行の Tie が WAL にある =
/// 削除の record は届いていない) に、 書き出しの返った列が消える。 行 r の Tag 列は書き出し済み、 Number 列の Tie は checkpoint の後。
/// 書き出しの entities → gen.seg の間に r を消し、 版数の段の前で落ちる。 開くと r は再生で生き返る: その時 Tag 列も残っている事。
/// [`a_row_brought_back_by_the_replay_keeps_its_written_out_columns`] の像 (`root` の下に作る): 行 r の Tag 列 `t.a` ("kept") は
/// 書き出し済み、 Number 列 `t.c` (5) の Tie は checkpoint の後。 書き出しの entities → gen.seg の間に r を消し、 版数の段の前で
/// 落ちた。 戻り値は像の DB と r。
fn kept_columns_image(root: &Path) -> (PathBuf, u64) {
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let r = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "c", ValueType::Number, 0).unwrap();
        eng.define_himo_in("t", "a", ValueType::Tag, 0).unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        let a = eng.entity_in("t").unwrap();
        eng.tie_to(a, "t.c", 1u32);
        let keep = eng.entity_in("t").unwrap();
        eng.tie_to(keep, "t.c", 2u32);
        eng.delete(a);
        let r = eng.entity_in("t").unwrap(); // v15
        assert_ne!(r, a, "前提: v15");
        eng.tie_text_to(r, "t.a", "kept");
        eng.persist_tables().unwrap(); // #870 の止めを外して checkpoint を進める
        eng.oplog_sync().unwrap(); // Tag 列は書き出し済み
        let _hold = eng.hold_consumer();
        eng.tie_to(r, "t.c", 5u32); // checkpoint の後の Tie (再生で r を生かし直す)
        let done = delete_during_write_out(&eng, r, Phase::BeforeGenerations, Phase::BeforeVersions, &live, &img);
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(done.load(Ordering::SeqCst), "前提: 書き出しの間に消した");
        r
    };
    crashsim::stop();
    (img.join("db"), r)
}

#[test]
fn a_row_brought_back_by_the_replay_keeps_its_written_out_columns() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("gen_keep_cells");
    let (db, r) = kept_columns_image(&root);
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.generation_repairs().0, 1, "前提: gen.seg だけ死の場所を突き合わせが完了させた");
    assert!(eng.is_live(r), "前提: 再生が r を生かし直した (削除の record は届いていない)");
    assert_eq!(eng.get(r, "t.c"), Some(5));
    assert_eq!(eng.get_text_owned(r, "t.a").as_deref(), Some(&b"kept"[..]), "再生で生き返った行の、 書き出しの返った列が消えた");
    let _ = std::fs::remove_dir_all(&root);
}

/// 読み直し 2: WAL の無い engine (`concurrentize` = schema の `finish_concurrent`) は、 session の途中の `flush_clean` でも紐名の辞書の
/// clean の印が立つ。 その後の書き出しの entities → gen.seg の間に消して落ちた像は、 開く時に 「きれいに閉じた」 と読まれて突き合わせが
/// 走らなかった (生きている行が次の世代 + 死の印 = 行の eid が変わる)。 gen.seg の header の 「clean の後に kill / revive した」 印で
/// 突き合わせる。
#[test]
fn a_db_without_a_wal_reconciles_a_delete_after_a_clean_flush() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("gen_nowal");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let r = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize(eng);
        let a = eng.entity_in("t").unwrap();
        eng.tie_to(a, "t.v", 1u32);
        let keep = eng.entity_in("t").unwrap();
        eng.tie_to(keep, "t.v", 2u32);
        eng.delete(a);
        let r = eng.entity_in("t").unwrap(); // v15
        assert_ne!(r, a, "前提: v15");
        eng.tie_to(r, "t.v", 3u32);
        eng.flush_clean().unwrap(); // 紐名の辞書の clean の印が立つ
        eng.tie_to(keep, "t.v", 4u32);
        let done = delete_during_write_out(&eng, r, Phase::BeforeGenerations, Phase::BeforeVersions, &live, &img);
        eng.flush_clean().unwrap();
        write_out_hook::set(None);
        assert!(done.load(Ordering::SeqCst), "前提: 書き出しの間に消した");
        r
    };
    crashsim::stop();
    let eng = Engine::open_standalone(img.join("db").to_str().unwrap()).unwrap();
    let s = eng.eid_slot(r);
    let now = eng.slot_eid(s);
    assert_eq!(eng.generation_repairs(), (1, 0), "前提: gen.seg だけ死の場所 (生きている印は消す前) を突き合わせが完了させた");
    assert!(!eng.is_live(now) || now == r, "WAL の無い DB で、 生きている行の eid が変わった ({r:#x} → {now:#x}、 突き合わせが走らない)");
    let _ = std::fs::remove_dir_all(&root);
}

/// 読み直し Q1 / 4: gen.seg だけ死の場所を突き合わせが完了させ、 tombstone は届いていない (sync の DB)。
/// tombstone を新しい HLC で置き、 #451 が Delete を作り直す (相手に配る)。 その Delete の eid は消えた行の世代込み (突き合わせは世代を
/// 2 つ上げるので、 今 − 1 ではない)。 消えた行への相手の書き込みは弾く (前は `slot_is_dead` が偽で、 新しい世代で生き返った)。
///
/// #978 から本体の書き出しは、 生きている印・場所の世代・列の前に削除の門を閉じて tomb.seg を書き出すので、 書き出しの段で撮る像
/// (`Lost`) にはこの形が無い (消した場所の世代が届く像には tombstone も届く)。 残る道は OS が書き出しの前に page を先に書いた像: 書き出して
/// いない r の削除のうち、 gen.seg の page だけが届いた像 (`Mixed`) を seed で探す (r の削除の record の page・tomb.seg・生きている印の
/// page が書き出し済みの中身、 gen.seg の page が今の中身、 の組。 1 枚が当たる率は Mac で 1/9〜1/23 = 3 回の run で 160 枚に 7〜18 枚。
/// 320 枚で 1 枚も無い率は 1e-6 以下)。
#[test]
fn a_completed_delete_without_a_tombstone_is_relogged_with_the_deleted_rows_eid() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    const SEEDS: u64 = 320;
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("gen_tomb");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let (a, r, hid, deleted_at) = {
        let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
        eng.define_table("notes", 2).unwrap();
        let hid = eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap() as u16;
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        let keep = eng.entity_in("notes").unwrap();
        eng.tie_to(keep, "notes.note", 2u32);
        eng.delete(a);
        let r = eng.entity_in("notes").unwrap(); // v15
        eng.tie_to(r, "notes.note", 5u32);
        eng.persist_tables().unwrap();
        eng.oplog_sync().unwrap();
        // もう 1 度: 1 回目が進めた checkpoint (r の Tie の後ろ) を WAL の header ごと書き出す (でないと、 header の page が書き出し済みの
        // 中身の像で再生が r の Tie を当て直して r を生かし直す)
        eng.oplog_sync().unwrap();
        let _hold = eng.hold_consumer();
        eng.tie_to(keep, "notes.note", 7u32);
        // 書き出さない (r の削除は WAL にも本体にも page cache にだけある)
        eng.delete(r);
        let deleted_at = eng.tombstone_hlc(r);
        for seed in 0..SEEDS {
            crashsim::capture(&live, &root.join(format!("img{seed}")), Mode::Mixed { seed }).unwrap();
        }
        (a, r, hid, deleted_at)
    };
    crashsim::stop();
    let (mut hits, mut revived) = (0, 0);
    for seed in 0..SEEDS {
        let img = root.join(format!("img{seed}"));
        let idb = img.join("db");
        // 前提の形: r の削除の record は WAL に無く、 tombstone も無く、 gen.seg だけ死
        let in_wal = OpLog::open(&idb.join("oplog")).unwrap().records_with_tail().iter().any(|x| x.hlc == deleted_at);
        let tomb_in_image = Engine::open_readonly(idb.to_str().unwrap()).unwrap().tombstone_hlc(r);
        if in_wal || tomb_in_image != Hlc::ZERO {
            let _ = std::fs::remove_dir_all(&img);
            continue;
        }
        let eng = Engine::open_concurrent_with_oplog(idb.to_str().unwrap(), OPLOG_CAP).unwrap();
        if eng.generation_repairs().0 != 1 {
            drop(eng);
            let _ = std::fs::remove_dir_all(&img);
            continue;
        }
        let deletes: Vec<u64> = eng
            .audit(&enchudb_engine::AuditFilter::default())
            .into_iter()
            .filter_map(|r| match r.op {
                DecodedOp::Delete { eid } => Some(eid),
                _ => None,
            })
            .collect();
        // a の Delete は前の session の record (WAL が畳まれていれば無い)。 r の Delete は作り直した物
        let relogged: Vec<u64> = deletes.iter().copied().filter(|&d| d != a).collect();
        if eng.is_live(r) {
            // WAL の header の page が古い像 (checkpoint が r の Tie の手前) では、 再生が r の Tie で r を生かし直す = 削除は 「無かった」。
            // その時は Delete を配らないこと (配ると相手でだけ消える)
            assert!(relogged.is_empty(), "再生が生かし直した r の Delete を作り直した (seed {seed}: {deletes:x?})");
            revived += 1;
            drop(eng);
            let _ = std::fs::remove_dir_all(&img);
            continue;
        }
        hits += 1;
        assert_eq!(relogged, vec![r], "作り直した Delete が無い / 消えた行の世代込みの eid でない (seed {seed}: {deletes:x?})");
        let world = enchudb_oplog::make_eid(1, eng.eid_slot(r));
        let later = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
        assert!(!eng.remote_tie_apply(world, hid, 9u32, later), "消えた行への相手の書き込みで行が生き返った (seed {seed})");
        drop(eng);
        let _ = std::fs::remove_dir_all(&img);
    }
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[gen_tomb] gen.seg だけ死で tombstone の無い像: {hits} / {SEEDS} (再生が r を生かし直した像 {revived})");
    assert!(hits >= 1, "前提: gen.seg だけ死で tombstone の届いていない像が {SEEDS} 枚に 1 枚も無い");
}

/// #992 の試験の置き場 (`scratch`) を、 落ちた (panic の巻き戻し) 時も消す guard (`ENCHU_POWER_LOSS_KEEP` の時は残す)。 像は写しを
/// 何枚も持つ (1 回 数百 MB): 変異試験で落ち続けると置き場が disk を埋めた。
struct Torn992Root(PathBuf);

impl Drop for Torn992Root {
    fn drop(&mut self) {
        if std::env::var(KEEP_ENV).is_err() {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// #992 の像の形 ([`torn992_image`])。 どれも sync の DB の行 r (使い回した場所、 世代 1、 Number `note` と Tag `tag`) の削除で、 削除の
/// record は WAL に届いていない (WAL の checkpoint は r の列の後ろ)。
#[derive(Clone, Copy, Debug)]
enum Torn992 {
    /// gen.seg の page だけ削除の後 (OS が msync の前に page を先に書いた像、 #978 の後は書き出しの段では作られない形): 生きている印・
    /// tombstone・列・WAL は削除の前 = 開く時の突き合わせが gen.seg だけ死の場所として削除を完了させ (世代を 2 つ上げる)、 仕上げが
    /// tombstone を新しい HLC で置いて #451 に Delete を作り直させる。 WAL に再生する record は無い
    GenPage,
    /// [`Torn992::GenPage`] の、 tomb.seg も r の使い回しの前 (a の削除の tombstone が残る = 使い回しが 0 にした page が届かなかった、
    /// #992 の読み直し B1 の 「届いていない」 のもう 1 つの形): 場所の tombstone は使い回しの記録 (`prev.seg`) の前の住人 a の削除の版数
    OldTomb,
    /// [`Torn992::GenPage`] の逆: 削除の前の写しに、 削除の後の生きている印 (entities.seg) だけを置く。 生きている印だけ死・gen.seg は
    /// 生きている (突き合わせの 2.)・tombstone 無し
    LivePage,
    /// 本体の書き出しが削除の門を閉じる前に r の削除を門の外まで進め、 生きている印の後・世代の前に消し終えた像 ([`c2_image`]、
    /// `Mode::Lost`): gen.seg だけ死、 tombstone は届いた (門を閉じる時に書き出す)、 Tag の cell は残る (中身を指す列は先に書き出した)。
    /// `records` = WAL に再生する record (残す行の値) を残す、 `relay` = 他の peer (9) の新しい行の record を中継して WAL に残す (開く時の
    /// 再生が `far` に払い出す)
    Gate { records: bool, relay: Option<u64> },
    /// [`Torn992::Gate`] の削除を、 相手 (peer 2) から受けた削除 (版数 h_d は r のどの cell の版数より古い = r の cell は全部 h_d の後に
    /// 書いた) にした像。 削除優先で r は消え、 tombstone は h_d で届く
    GateRemote,
}

/// [`torn992_image`] の r の名前。
const TORN992_NAME: u32 = 5;

/// [`Torn992::GateRemote`] の受けた削除の版数 (r のどの cell より古い)。
const TORN992_REMOTE_DELETE: Hlc = Hlc { wall: 1, logical: 0, peer: 2 };

/// #992 の像を `root` の下に作る。 sync の DB、 表 `notes` (枠 2、 Number `note` (名前) / Tag `tag` / Leaf `body`)・`pad`・`far` (枠 16、 Number `a`、 場所は
/// v15 に移る時の gen.seg の commit の外)。 r の名前は [`TORN992_NAME`]。 戻り値は (像の DB, 先に消した a, r)。
fn torn992_image(root: &Path, kind: Torn992) -> (PathBuf, u64, u64) {
    use enchudb_engine::transport::WireRecord;
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let (a, r) = {
        let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 200_000).unwrap();
        eng.define_table("notes", 2).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.define_himo_in("notes", "tag", ValueType::Tag, 0).unwrap();
        eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
        let note = eng.himo_id("notes.note").unwrap() as u16;
        eng.set_table_pk("notes", note).unwrap();
        eng.define_table("pad", 100_000).unwrap();
        eng.define_table("far", 16).unwrap();
        eng.define_himo_in("far", "a", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        let keep = eng.entity_in("notes").unwrap();
        eng.tie_to(keep, "notes.note", 2u32);
        eng.delete(a);
        if matches!(kind, Torn992::OldTomb) {
            // a の削除を書き出し、 その tomb.seg (a の tombstone) を控える
            eng.oplog_sync().unwrap();
            std::fs::copy(db.join("tomb.seg"), root.join("tomb_a.seg")).unwrap();
        }
        let r = eng.entity_in("notes").unwrap(); // v15 に移って a の場所を使い回す
        assert!(r != a && eng.eid_slot(r) == eng.eid_slot(a), "前提: v15 で a の場所を使い回した");
        eng.tie_to(r, "notes.note", TORN992_NAME);
        eng.tie_text_to(r, "notes.tag", "torn");
        eng.tie_text_to(r, "notes.body", "torn body");
        eng.persist_tables().unwrap();
        eng.oplog_sync().unwrap(); // r の列は書き出し済み (checkpoint を越えた)
        // もう 1 度: 進めた checkpoint を WAL の header ごと書き出す (でないと、 header の page が書き出し済みの中身の像で再生が r の Tie を
        // 当て直して r を生かし直す)
        eng.oplog_sync().unwrap();
        let _hold = eng.hold_consumer();
        match kind {
            Torn992::GenPage | Torn992::OldTomb | Torn992::LivePage => {
                // 削除の前 (書き出し済み) の写しに、 削除の後の gen.seg (LivePage は entities.seg) だけを置く
                let (before, after) = (root.join("before"), root.join("after"));
                copy_tree(&live, &before);
                eng.delete(r);
                copy_tree(&live, &after);
                copy_tree(&before, &img);
                let seg = if matches!(kind, Torn992::LivePage) { "entities.seg" } else { "gen.seg" };
                std::fs::copy(after.join("db").join(seg), img.join("db").join(seg)).unwrap();
                if matches!(kind, Torn992::OldTomb) {
                    std::fs::copy(root.join("tomb_a.seg"), img.join("db").join("tomb.seg")).unwrap();
                }
            }
            Torn992::Gate { records, relay } => {
                if records {
                    eng.tie_to(keep, "notes.note", 7u32);
                }
                if let Some(foreign) = relay {
                    let far = eng.himo_id("far.a").unwrap() as u16;
                    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
                    let rec = WireRecord::unsigned(Hlc { wall: now, logical: 0, peer: 9 }, 9, DecodedOp::Tie { eid: foreign, himo_id: far, value: 7 });
                    assert!(eng.relay_record(&rec), "前提: 中継した record を WAL に載せた");
                }
                eng.oplog_commit();
                let e2 = eng.clone();
                c2_image(&eng, move || e2.delete(r), &live, &img);
            }
            Torn992::GateRemote => {
                let note = eng.himo_id("notes.note").unwrap() as u16;
                assert!(eng.cell_hlc(r, note) > TORN992_REMOTE_DELETE, "前提: r の cell は受ける削除より新しい");
                eng.oplog_commit();
                let e2 = eng.clone();
                c2_image(&eng, move || assert!(e2.remote_delete_apply(r, TORN992_REMOTE_DELETE), "前提: 受けた削除が当たる"), &live, &img);
            }
        }
        assert!(img.join("db").is_dir(), "前提: 像を撮った ({kind:?})");
        (a, r)
    };
    crashsim::stop();
    (img.join("db"), a, r)
}

/// #992: 消えた行 r の後始末が済んだか。
/// - Tag の cell を落とした (死んだ場所の cell が値の引きに出ない)
/// - 消えた行への相手の書き込みを弾く (tombstone がある)
/// - r の名前の削除を覚えた (#992 の読み直しの細かい物 1 / S1: 名前の cell を落とす前に覚える。 前は覚えずに落とし、 後から届いた、 削除
///   より前に作られた同じ名前の行を束ねない / 消さない。 tombstone の届いていない場所で、 tombstone を置く前に cell を落とすと読めない)
/// - r の Delete を r の世代込みの宛名で配る分 (`_sync_ops`) に載せた (相手でも r が消える。 #1008: 前の open が世代を 2 つ上げた
///   場所も、 gen.seg の 「最後の行は 今 − 2」 の印で r の世代を読む)
fn assert_torn_delete_cleaned(eng: &Engine, a: u64, r: u64, named: bool, what: &str) {
    assert_torn_cells_dropped(eng, what);
    let deletes = r_deletes(eng, a, r);
    assert_eq!(deletes.len(), 1, "{what}: 消えた行 r の Delete を作り直して配っていない ({deletes:x?})");
    assert_eq!(eid_local(deletes[0]), eid_local(r), "{what}: 作り直した Delete が r の世代込みの eid でない");
    let note = eng.himo_id("notes.note").unwrap() as u16;
    if named {
        assert!(eng.deleted_name_version(note, TORN992_NAME as u64).is_some(), "{what}: 消えた行 r の名前の削除を覚えていない");
    }
    let later = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
    let slot = eng.eid_slot(r);
    assert!(!eng.remote_tie_apply(enchudb_oplog::make_eid(1, slot), note, 9u32, later), "{what}: 消えた行への相手の書き込みで行が生き返った");
}

/// #992: 消えた行 r の cell が値の引きに出ない: Tag (中身を指す列、 本体の書き出しで生きている印より先に書き出す) と Number の名前
/// (残りの列、 gen.seg の後に書き出す。 #992 の読み直し 2: 前は Tag だけを見て、 書き出しの途中の像で Number だけ残る形を見逃した)。
fn assert_torn_cells_dropped(eng: &Engine, what: &str) {
    let vid = eng.vocab_id("torn").expect("前提: 語は辞書に残る");
    assert_eq!(eng.pull_raw("notes.tag", vid), Vec::<u64>::new(), "{what}: 消えた行 r の Tag の cell が残り、 値の引きに出る");
    assert_eq!(eng.pull_raw("notes.note", TORN992_NAME), Vec::<u64>::new(), "{what}: 消えた行 r の名前 (Number) の cell が残り、 値の引きに出る");
}

/// 配る分 (`_sync_ops`) の、 自分 (peer 1) が author の r の場所の Delete (先に消した a の物は除く)。
fn r_deletes(eng: &Engine, a: u64, r: u64) -> Vec<u64> {
    let slot = eng.eid_slot(r);
    distributed(eng)
        .into_iter()
        .filter(|rec| rec.author_peer == 1)
        .filter_map(|rec| match rec.op {
            DecodedOp::Delete { eid } if eid_local(eid) != eid_local(a) && eng.eid_slot(eid) == slot => Some(eid),
            _ => None,
        })
        .collect()
}

/// 像の r の名前の cell が disk に残っているか ([`Torn992::GenPage`] / [`Torn992::OldTomb`] だけ。 削除の門を通った削除は、 名前の cell を落とした列が届き、
/// 名前の削除の記録 (`pktombs`) は consumer の周期の書き出しの前 = 開く道が名前を読めない。 この PR の外の形)。
fn torn992_named(kind: Torn992) -> bool {
    matches!(kind, Torn992::GenPage | Torn992::OldTomb | Torn992::LivePage)
}

/// 配る分の、 自分 (peer 1) が author の場所 `slot` の Delete の (宛名の local, 版数)。 同じ record の重複 (落ちた後に bridge が WAL から写し
/// 直す = 前からの at-least-once、 相手は中身で重複を落とす) は 1 つにする。
fn slot_deletes(eng: &Engine, slot: u32) -> Vec<(u32, Hlc)> {
    let mut v: Vec<(u32, Hlc)> = distributed(eng)
        .into_iter()
        .filter_map(|rec| match rec.op {
            DecodedOp::Delete { eid } if rec.author_peer == 1 && eng.eid_slot(eid) == slot => Some((eid_local(eid), rec.hlc)),
            _ => None,
        })
        .collect();
    v.sort();
    v.dedup();
    v
}

/// gen.seg だけ死の 3 つの像 (tombstone が届いていない (0 / 前の住人の物) / 届いた)。
const TORN992_GEN_ONLY: [Torn992; 3] = [Torn992::GenPage, Torn992::OldTomb, Torn992::Gate { records: false, relay: None }];

/// #992 の対照: gen.seg だけ死の像を WAL 付きで開くと、 突き合わせの仕上げが同じ engine で走る (前提の確かめ)。 仕上げを書き出した後に
/// 「後始末待ち」 の印を落とす: その open の後に落ちた (kill の) 像を開き直しても、 もう一度仕上げない。
#[test]
fn a_torn_delete_with_a_tag_cell_is_cleaned_up_by_one_open() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for kind in TORN992_GEN_ONLY {
        let root = scratch("torn992_one");
        let _rm = Torn992Root(root.clone());
        let (db, a, r) = torn992_image(&root, kind);
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        // 開いた直後の kill の像 (配る分に写す前: 写した後の kill は bridge が同じ record をもう一度写す = 前からの at-least-once)
        let after = root.join("after_open");
        let hold = eng.hold_consumer();
        copy_tree(&db, &after);
        drop(hold);
        assert_eq!(eng.generation_repairs(), (1, 0), "前提: gen.seg だけ死の場所 (r) を突き合わせが完了させた ({kind:?})");
        assert!(!eng.is_live(r), "前提: 消した record は WAL に無いが、 本体に届いた削除は完了する");
        assert_torn_delete_cleaned(&eng, a, r, torn992_named(kind), &format!("1 回の open ({kind:?})"));
        drop(eng);
        let again = Engine::open_concurrent_with_oplog(after.to_str().unwrap(), OPLOG_CAP).unwrap();
        assert_eq!(again.resumed_cleanups(), 0, "仕上げを書き出した後も後始末待ちの印が残った ({kind:?})");
        assert_eq!(r_deletes(&again, a, r).len(), 1, "開き直した open が r の Delete をもう一度作った ({kind:?})");
        drop(again);
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// #992 の読み直し S4 / 読み直し 2 の細かい 3: 生きている印だけ死の像 ([`Torn992::LivePage`]、 tombstone 無し) は突き合わせの 2. が削除を
/// 完了させる。 2. の完了は tombstone を作り直さない (版数なしの削除・`merge_entity_into` の src・`Split::NoRoom` の空の行も tomb.seg に何も
/// 置かず、 見分けられない。 作り直すと一度も配っていない削除を今の時刻で配る)。 突き合わせと仕上げの間で落ちて次の open が後始末待ちから
/// 拾っても同じ答え (決め方は gen.seg の印から)。 cell はどちらでも落とす。 最初の head は 2. の場所を照合せず、 3. で拾うと照合した (落ちたか
/// で答えが変わった)、 2 つ目の head は両方で作り直した。
#[test]
fn an_entities_only_torn_delete_is_finished_the_same_with_or_without_a_kill() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for kill in [false, true] {
        let root = scratch("torn992_live_page");
        let _rm = Torn992Root(root.clone());
        let (db, a, r) = torn992_image(&root, Torn992::LivePage);
        let open_at = if kill {
            let killed = root.join("killed");
            std::fs::create_dir_all(&killed).unwrap();
            let (d2, k2) = (db.clone(), killed.join("db"));
            let shot = Arc::new(AtomicBool::new(false));
            let shot2 = shot.clone();
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == Phase::OpenBeforeFinishReconcile && !shot2.swap(true, Ordering::SeqCst) {
                    copy_tree(&d2, &k2);
                }
            })));
            let first = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP);
            write_out_hook::set(None);
            let first = first.unwrap();
            assert!(shot.load(Ordering::SeqCst), "前提: 仕上げの前で像を撮った");
            assert_eq!(first.generation_repairs(), (0, 1), "前提: 1 回目の open の突き合わせの 2. が r の場所を完了させた");
            drop(first);
            killed.join("db")
        } else {
            db
        };
        let eng = Engine::open_concurrent_with_oplog(open_at.to_str().unwrap(), OPLOG_CAP).unwrap();
        assert_eq!(eng.generation_repairs(), if kill { (0, 0) } else { (0, 1) }, "前提: 突き合わせの 2. の像 (kill {kill})");
        assert_eq!(eng.resumed_cleanups(), u32::from(kill), "前提: kill の後は後始末待ちから拾う");
        assert!(!eng.is_live(r));
        let what = format!("生きている印だけ死 (kill {kill})");
        assert_torn_cells_dropped(&eng, &what);
        assert_eq!(r_deletes(&eng, a, r), Vec::<u64>::new(), "{what}: 2. の完了で r の Delete を作り直した");
        drop(eng);
    }
}

/// #992 の読み直し B1: 受けた削除 (相手の削除の版数 h_d より新しい自分の cell も落とす削除優先) の像 ([`Torn992::GateRemote`]、 r の
/// cell は全部 h_d の後に書いた)。 tombstone (h_d) は届いている。 突き合わせはこれを 「届いていない」 と読まない: 前の照合 (tombstone が
/// 行の cell の版数より古い) は今の時刻の tombstone を置き、 自分を author とする r の Delete を配った (受け手の名前の削除優先が、 消した
/// 後に同じ名前で足し直した行を消す)。 「tombstone が行の cell の版数の最小より古い」 でも、 cell が全部 h_d の後に書き直されていれば同じに
/// 誤る。 届いていないのは、 tombstone が無いか、 場所の使い回しの記録の前の住人の削除の版数と同じ時だけ。
#[test]
fn a_received_delete_with_newer_own_cells_is_not_relogged_as_missing() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_remote");
    let _rm = Torn992Root(root.clone());
    let (db, a, r) = torn992_image(&root, Torn992::GateRemote);
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.generation_repairs(), (1, 0), "前提: gen.seg だけ死の場所 (r) を突き合わせが完了させた");
    assert!(!eng.is_live(r));
    let vid = eng.vocab_id("torn").expect("前提: 語は辞書に残る");
    assert_eq!(eng.pull_raw("notes.tag", vid), Vec::<u64>::new(), "消えた行 r の Tag の cell が残った");
    assert_eq!(eng.tombstone_hlc(r), TORN992_REMOTE_DELETE, "受けた削除の tombstone を置き換えた");
    assert_eq!(r_deletes(&eng, a, r), Vec::<u64>::new(), "受けた削除を届いていない削除と読み、 自分を author とする r の Delete を配った");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #992 の入口 1 (#880 の往復): 単独の open (`open_standalone`) は、 きれいに閉じなかった sync の DB (WAL に再生する record は無い) を、
/// WAL 付きの open で往復して #451 の作り直しを済ませてから開く。 前は往復の前の単独の engine が突き合わせの直し (死の印・生きている印)
/// を map に書き、 仕上げの後半 (消えた行の世代・tombstone の届いていない完了) を memory に持ったまま捨てた: 往復の open は直した場所を
/// 見つけず、 tombstone の届かなかった像では r の Delete が作り直されず (相手では r が消えない)、 届いた像では作り直した Delete の宛名が
/// r の 1 つ上の世代だった。
#[test]
fn the_standalone_round_trip_finishes_the_reconcile_of_a_torn_delete() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for kind in TORN992_GEN_ONLY {
        let root = scratch("torn992_standalone");
        let _rm = Torn992Root(root.clone());
        let (db, a, r) = torn992_image(&root, kind);
        let probe = OpLog::open(&db.join("oplog")).unwrap();
        assert!(!probe.has_records_to_replay(), "前提: WAL に再生する record が無い (単独の open は往復だけをする) ({kind:?})");
        drop(probe);
        let eng = Engine::open_standalone(db.to_str().unwrap()).unwrap();
        assert!(!eng.is_live(r), "前提: 本体に届いた削除は完了する ({kind:?})");
        assert_torn_delete_cleaned(&eng, a, r, torn992_named(kind), &format!("単独の open の往復 ({kind:?})"));
        drop(eng);
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// #992 の往復の条件の対照: WAL の file の無い sync の DB は往復しない (`relog_before_standalone_session`) ので、 単独の open が自分で
/// 突き合わせる (`defer_repairs` は WAL の file がある時だけ)。 tombstone の作り直し (#451 で Delete を作り直す) は WAL が要るので、
/// 「後始末待ち」 の印を残し、 単独の session をきれいに閉じても DB を 「きれいに閉じた」 にしない: 次の WAL 付きの open が拾って仕上げる。
/// cell は単独の session でその場で落とす (名前の削除は行の cell の版数の最大で仮に覚え、 WAL 付きの仕上げが置く tombstone で覚え直す。
/// #992 の読み直し 2 の細かい 2: 前は cell を WAL 付きの open まで残し、 単独の session で消えた行の cell が値の引きに出た)。
#[test]
fn a_standalone_open_without_a_wal_file_reconciles_by_itself() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_standalone_nowal");
    let _rm = Torn992Root(root.clone());
    let (db, a, r) = torn992_image(&root, Torn992::GenPage);
    std::fs::remove_file(db.join("oplog")).unwrap();
    let eng = Engine::open_standalone(db.to_str().unwrap()).unwrap();
    assert_eq!(eng.generation_repairs(), (1, 0), "WAL の file の無い単独の open が突き合わせなかった");
    assert!(!eng.is_live(r));
    assert_torn_cells_dropped(&eng, "WAL の file の無い単独の session");
    drop(eng); // きれいに閉じる
    let eng = Engine::open_standalone(db.to_str().unwrap()).unwrap();
    assert_torn_cells_dropped(&eng, "WAL の file の無い単独の session (2 回目)");
    drop(eng);
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.resumed_cleanups(), 1, "単独の session の後始末待ちを WAL 付きの open が拾わない");
    assert_torn_delete_cleaned(&eng, a, r, true, "WAL の file の無い単独の open の後の WAL 付きの open");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #992 の入口 2: WAL 付きの open が突き合わせの直しを map に書いた後、 仕上げを書き出して 「後始末待ち」 の印を落とす前に落ちた (kill =
/// page cache は残る) 像を開く。 割り込み点は 3 つ: 仕上げの前 / 仕上げの後・#451 の作り直しの前 (読み直し S1: 前は仕上げが先に cell を
/// 落とし、 tombstone を置く前に落ちると後始末の印が消えた) / 作り直しの後・書き出しの前。 直した場所は 「死の印・生きていない」 で
/// 突き合わせの 1. / 2. に当たらない。 後始末待ちの印から仕上げに回す。 前は仕上げが二度と走らず、 r の Delete が作り直されない / Tag の
/// cell が値の引きに出た。 #1008: 1 回目の open が世代を 2 つ上げた場所の Delete の宛名も r の世代 (「最後の行は 今 − 2」 の印)。
#[test]
fn a_kill_between_the_reconcile_and_its_finish_leaves_the_cleanup_findable() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for kind in [Torn992::GenPage, Torn992::Gate { records: true, relay: None }] {
        for point in [Phase::OpenBeforeFinishReconcile, Phase::OpenAfterFinishReconcile, Phase::OpenBeforeSettle] {
            let what = format!("{kind:?} / {point:?}");
            let root = scratch("torn992_kill");
            let _rm = Torn992Root(root.clone());
            let (db, a, r) = torn992_image(&root, kind);
            let killed = root.join("killed");
            std::fs::create_dir_all(&killed).unwrap();
            let (d2, k2) = (db.clone(), killed.join("db"));
            let shot = Arc::new(AtomicBool::new(false));
            let shot2 = shot.clone();
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == point && !shot2.swap(true, Ordering::SeqCst) {
                    // kill の像: page cache の中身 (開く道が map に書いた直しも) を写す
                    copy_tree(&d2, &k2);
                }
            })));
            let first = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP);
            write_out_hook::set(None);
            let first = first.unwrap();
            assert!(shot.load(Ordering::SeqCst), "前提: 割り込み点で像を撮った ({what})");
            assert_eq!(first.generation_repairs(), (1, 0), "前提: 1 回目の open の突き合わせが r の場所を完了させた ({what})");
            drop(first);
            let eng = Engine::open_concurrent_with_oplog(killed.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
            assert_eq!(eng.generation_repairs(), (0, 0), "前提: 直した場所は突き合わせの 1. / 2. に当たらない ({what})");
            assert!(!eng.is_live(r));
            assert_eq!(eng.resumed_cleanups(), 1, "後始末待ちの印から仕上げに回した場所が r の 1 つでない ({what})");
            // 作り直しが cell を落とした後 (`OpenBeforeSettle`) の kill は、 名前の削除の記録 (`pktombs`、 書き出しは開く道の
            // `persist_name_records`) を失う: 普通の削除の後・consumer の周期の書き出しの前の kill と同じ形 (この PR の外)
            let named = torn992_named(kind) && point != Phase::OpenBeforeSettle;
            assert_torn_delete_cleaned(&eng, a, r, named, &format!("kill ({what})"));
            drop(eng);
            let _ = std::fs::remove_dir_all(&root);
        }
    }
}

/// #992: WAL の file の無い DB を WAL 付きで開き、 突き合わせが直しを map に書いた後に WAL を作れず断る (`OpLog::create` の失敗。 WAL の
/// file の名前を行き先の directory の無い symlink にして起こす)。 断った engine は何も書かない (`abandoned`) = 次の open もきれいに閉じて
/// いないと読み、 後始末待ちの印から仕上げに回す。 前は突き合わせの中の仕上げの前半が cell を落とし、 断った engine の Drop がきれいに
/// 閉じた印を立て、 次の open は突き合わせも #451 の照合もせず、 r の Delete が配られなかった。
#[test]
fn an_open_that_cannot_create_the_wal_leaves_the_cleanup_findable() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_nowal");
    let _rm = Torn992Root(root.clone());
    let (db, a, r) = torn992_image(&root, Torn992::GenPage);
    let wal = db.join("oplog");
    std::fs::remove_file(&wal).unwrap();
    std::os::unix::fs::symlink(root.join("missing").join("oplog"), &wal).unwrap();
    let refused = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).map(|_| ());
    std::fs::remove_file(&wal).unwrap();
    refused.expect_err("前提: WAL を作れずに開くのを断る");
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.generation_repairs(), (0, 0), "前提: 断った open が直した場所は突き合わせの 1. / 2. に当たらない");
    assert!(!eng.is_live(r));
    assert_torn_delete_cleaned(&eng, a, r, true, "WAL を作れなかった open");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #992 の読み直し S2: 単独の open が region の CRC の照合 (`.crc` の file) で断る時も何も書かない。 往復する engine は開く時の直しを書かずに
/// 開いてあるので、 前は Drop がきれいに閉じた印を立て、 直していない状態 (gen.seg だけ死・生きている) がきれいに閉じた DB として残った
/// (次の open は突き合わせも #451 の照合もしない)。 `.crc` を外して開き直せば往復して仕上げる。
#[test]
fn a_standalone_open_refused_by_the_crc_check_leaves_the_db_unclean() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_crc");
    let _rm = Torn992Root(root.clone());
    let (db, a, r) = torn992_image(&root, Torn992::GenPage);
    let crc = db.join("crc");
    std::fs::write(&crc, b"not a crc table").unwrap();
    let refused = Engine::open_standalone(db.to_str().unwrap()).map(|_| ());
    std::fs::remove_file(&crc).unwrap();
    refused.expect_err("前提: 壊れた .crc で断る");
    let eng = Engine::open_standalone(db.to_str().unwrap()).unwrap();
    assert!(!eng.is_live(r));
    assert_torn_delete_cleaned(&eng, a, r, true, "CRC で断った後の単独の open");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #992 の読み直し 2 の S1: 世代を使い切る場所 (最後の行 r の世代がいちばん上) の削除の途中の像 (gen.seg だけ死 = 引退の印)。 突き合わせは
/// 引退の印より上に上げられないので、 「最後の行は 今 − 2」 の印を立てない (立てると次の open が最後の行を r の 1 つ前と読む)。 突き合わせと
/// 仕上げの間の kill の後の open が後始末待ちから拾い、 r の Delete を r の世代で作り直す (前の住人宛てに作らない)。
#[test]
fn a_torn_delete_of_the_last_generation_keeps_the_last_row() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_retired");
    let _rm = Torn992Root(root.clone());
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let (r, prev) = {
        let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
        eng.define_table("t", 1).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        // 世代 = local の上 8 bit (場所の bit 24)。 いちばん上の世代 (254) の行まで使い回す
        let (mut prev, mut i) = (0u64, 0u32);
        let r = loop {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to(e, "t.v", i);
            if eid_local(e) >> 24 == 254 {
                break e;
            }
            assert!(i < 300, "前提: 枠 1 の表は場所を使い回す");
            eng.delete(e);
            prev = e;
            i += 1;
        };
        eng.persist_tables().unwrap();
        eng.oplog_sync().unwrap();
        eng.oplog_sync().unwrap();
        let _hold = eng.hold_consumer();
        // 削除の前 (書き出し済み) の写しに、 削除の後の gen.seg だけを置く (gen.seg だけ死 = 引退の印)
        let (before, after) = (root.join("before"), root.join("after"));
        copy_tree(&live, &before);
        eng.delete(r);
        copy_tree(&live, &after);
        copy_tree(&before, &img);
        std::fs::copy(after.join("db").join("gen.seg"), img.join("db").join("gen.seg")).unwrap();
        (r, prev)
    };
    crashsim::stop();
    let db = img.join("db");
    let killed = root.join("killed");
    std::fs::create_dir_all(&killed).unwrap();
    let (d2, k2) = (db.clone(), killed.join("db"));
    let shot = Arc::new(AtomicBool::new(false));
    let shot2 = shot.clone();
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::OpenBeforeFinishReconcile && !shot2.swap(true, Ordering::SeqCst) {
            copy_tree(&d2, &k2);
        }
    })));
    let first = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP);
    write_out_hook::set(None);
    let first = first.unwrap();
    assert!(shot.load(Ordering::SeqCst), "前提: 仕上げの前で像を撮った");
    assert_eq!(first.generation_repairs(), (1, 0), "前提: 突き合わせが引退する場所を完了させた");
    drop(first);
    let eng = Engine::open_concurrent_with_oplog(killed.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.resumed_cleanups(), 1, "前提: 後始末待ちから拾った");
    let slot = eng.eid_slot(r);
    let deletes: Vec<u32> = slot_deletes(&eng, slot).into_iter().map(|(d, _)| d).collect();
    let count = |e: u64| deletes.iter().filter(|&&d| d == eid_local(e)).count();
    assert_eq!((count(r), count(prev)), (1, 1), "r の Delete が r の世代で 1 つでない / 前の住人宛てを作り直した ({deletes:x?}、 r {r:#x} 前 {prev:#x})");
    drop(eng);
}

/// #992 の読み直し 2 の S2 / 変異 N12: 単独の open が Leaf の空き一覧の作り直し (Leaf の列を読む) で断る時も何も書かない (`abandoned`)。 Leaf の
/// 列の segment の file を行き先の無い symlink にして起こす。 往復する engine は開く時の直しを書いていないので、 前は Drop がきれいに閉じた
/// 印を立て、 直していない状態がきれいに閉じた DB として残った (次の open は突き合わせも #451 の照合もしない)。
#[test]
fn a_standalone_open_refused_by_the_leaf_columns_leaves_the_db_unclean() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_leaf");
    let _rm = Torn992Root(root.clone());
    let (db, a, r) = torn992_image(&root, Torn992::GenPage);
    let body = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        ro.himo_id("notes.body").unwrap()
    };
    let seg = db.join("himo").join(format!("{body:04}.seg"));
    assert!(seg.is_file(), "前提: Leaf の列の segment がある ({})", seg.display());
    let kept = root.join("body.seg");
    std::fs::rename(&seg, &kept).unwrap();
    std::os::unix::fs::symlink(root.join("missing").join("body.seg"), &seg).unwrap();
    let refused = Engine::open_standalone(db.to_str().unwrap()).map(|_| ());
    std::fs::remove_file(&seg).unwrap();
    std::fs::rename(&kept, &seg).unwrap();
    refused.expect_err("前提: Leaf の列を読めずに断る");
    let eng = Engine::open_standalone(db.to_str().unwrap()).unwrap();
    assert!(!eng.is_live(r));
    assert_torn_delete_cleaned(&eng, a, r, true, "Leaf の列で断った後の単独の open");
    drop(eng);
}

/// #992 の入口 3 (PR #977): WAL の再生が空きの不足で他の peer の新しい行を置けず、 開くのを断る (`settle_replayed_generations`)。
/// 断る前に突き合わせは直しを map に書いている (page cache に残り、 OS の書き出しや断る前の `keep_replayed_mappings` の書き出しで disk
/// にも届く)。 空きを作って開き直すと、 直した場所は突き合わせの 1. / 2. に当たらない。 後始末待ちの印から仕上げに回す。 前は空き不足で
/// 断って開き直すだけで必ず後始末を失った (Tag の cell が値の引きに出て、 Delete の宛名が r の 1 つ上の世代)。
#[test]
fn an_open_refused_after_the_replay_leaves_the_cleanup_findable() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_refused");
    let _rm = Torn992Root(root.clone());
    let foreign = enchudb_oplog::make_eid(9, 5);
    let (db, a, r) = torn992_image(&root, Torn992::Gate { records: false, relay: Some(foreign) });
    enchudb_engine::engine::open_space_margin_hook::set(u64::MAX / 2);
    let refused = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).map(|_| ());
    let e = refused.expect_err("前提: 再生が中継した行を置けずに開くのを断る");
    assert_eq!(e.kind(), std::io::ErrorKind::StorageFull, "前提: 空きの不足で断った: {e}");
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.generation_repairs(), (0, 0), "前提: 断った open が直した場所は突き合わせの 1. / 2. に当たらない");
    assert!(eng.resolve_remote_eid_existing(foreign).is_some(), "前提: 空きが戻った open が中継した行を再生した");
    assert!(!eng.is_live(r));
    assert_eq!(eng.resumed_cleanups(), 1, "後始末待ちの印から仕上げに回した場所が r の 1 つでない");
    assert_torn_delete_cleaned(&eng, a, r, false, "再生の後の断り");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #1008 の像 (`root` の下に作る): 突き合わせが世代を 2 つ上げて完了させた場所 (「最後の行は 今 − 2」 の印) を、 開いた後 (A1) に Y で
/// 使い回し、 その本体の書き出しの途中で電源が落ちた像: 使い回しは書き出しの段の間 (`PrevWrittenOut`、 Y の record は WAL の fsync の後 =
/// 届かない) に走り、 使い回しの記録 (前の住人 r) と生きている印 (Y) は届き、 gen.seg (死の印・「今 − 2」 のまま) は届かない
/// (`BeforeGenerations` で撮る)。 戻り値は (像の directory (中に `db`), 先に消した a, r, Y)。
fn two_raises_reuse_image(root: &Path) -> (PathBuf, u64, u64, u64) {
    let (db, a, r) = torn992_image(root, Torn992::GenPage);
    let (img, img2) = (root.join("img"), root.join("img2"));
    crashsim::start();
    // 1 枚目は全部書き出し済み (A1 の書き出しの控えはこの上に積む)
    adopt_as_durable(&img);
    let y = {
        let a1 = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        assert_eq!(a1.generation_repairs(), (1, 0), "前提: A1 の突き合わせが r の場所を完了させた (世代を 2 つ上げた)");
        a1.oplog_sync().unwrap();
        a1.persist_sync_state().unwrap();
        let _hold = a1.hold_consumer();
        let made: Arc<std::sync::Mutex<Option<u64>>> = Arc::default();
        let shot = Arc::new(AtomicBool::new(false));
        let (e2, made2, shot2, l, i) = (a1.clone(), made.clone(), shot.clone(), img.clone(), img2.clone());
        let _off = HookOff;
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::PrevWrittenOut && made2.lock().unwrap().is_none() {
                // 削除の門は開いている: 別の thread で r の場所を Y で使い回す (使い回しの記録は門を閉じる時に書き出し直す)
                let e3 = e2.clone();
                let h = std::thread::spawn(move || {
                    let y = e3.entity_in("notes").unwrap();
                    e3.tie_to(y, "notes.note", TORN992_Y_NAME);
                    y
                });
                let until = std::time::Instant::now() + std::time::Duration::from_secs(60);
                while !h.is_finished() && std::time::Instant::now() < until {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                assert!(h.is_finished(), "前提: 書き出しの途中の使い回しが 60 秒で終わらない");
                *made2.lock().unwrap() = Some(h.join().unwrap());
            }
            if p == Phase::BeforeGenerations && made2.lock().unwrap().is_some() && !shot2.swap(true, Ordering::SeqCst) {
                crashsim::capture(&l, &i, Mode::Lost).unwrap();
            }
        })));
        // 書き出す物を作る
        let f = a1.entity_in("far").unwrap();
        a1.tie_to(f, "far.a", 1u32);
        a1.oplog_sync().unwrap();
        drop(_off);
        assert!(shot.load(Ordering::SeqCst), "前提: 使い回しの後の書き出しの途中で撮った");
        let y = made.lock().unwrap().expect("前提: 書き出しの途中で使い回した");
        assert!(a1.eid_slot(y) == a1.eid_slot(r) && y != r, "前提: Y は r の場所を使い回した");
        y
    };
    crashsim::stop();
    (img2, a, r, y)
}

/// [`two_raises_reuse_image`] の Y の名前。
const TORN992_Y_NAME: u32 = 9;

/// #1008: [`two_raises_reuse_image`] の像を開く。 突き合わせは gen.seg だけ死の場所として完了させる。 最後の行は Y (使い回した行の世代は
/// 場所の今の世代 = r + 2)。 次の使い回しは r / Y の eid を渡さない。 Y の record は WAL に無い (相手は Y を知らない) ので、 Y の Delete は
/// 作らない (#992 の読み直し 2 の細かい 3)。 r の Delete も新しく作らない (A1 が作り直した 1 つだけ)。 前は記録の前の住人 + 1 (r + 1、 無かった
/// 行) を最後の住人と読み、 削除の途中と取り違えて 今 − 1 (= r + 1) 宛ての Delete を作った。
#[test]
fn a_reuse_of_a_slot_finished_with_two_raises_is_its_last_row_after_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_reuse_two_up");
    let _rm = Torn992Root(root.clone());
    let (img2, a, r, y) = two_raises_reuse_image(&root);
    let eng = Engine::open_concurrent_with_oplog(img2.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.generation_repairs(), (1, 0), "前提: 生きている印は Y、 gen.seg は使い回しの前 (死・「今 − 2」) の像");
    assert!(!eng.is_live(y), "前提: Y の record は WAL に届いていない");
    assert_two_raises_reuse_finished(&eng, a, r, y, "1 回の open");
    drop(eng);
}

/// [`two_raises_reuse_image`] の像の後始末: Y の名前の cell が値の引きに出ない、 配る Delete は A1 が作り直した r の 1 つだけ (Y 宛て・
/// 無かった行宛て・r 宛ての 2 つ目を作らない)、 次の使い回しは r / Y の eid を渡さない。
fn assert_two_raises_reuse_finished(eng: &Engine, a: u64, r: u64, y: u64, what: &str) {
    assert_eq!(eng.pull_raw("notes.note", TORN992_Y_NAME), Vec::<u64>::new(), "{what}: 消えた行 Y の名前の cell が値の引きに出る");
    let deletes: Vec<u32> =
        slot_deletes(eng, eng.eid_slot(r)).into_iter().map(|(d, _)| d).filter(|&d| d != eid_local(a)).collect();
    assert_eq!(deletes, vec![eid_local(r)], "{what}: 配る Delete が A1 の作り直した r の 1 つでない ({deletes:x?}、 r {r:#x} Y {y:#x})");
    let n = eng.entity_in("notes").unwrap();
    assert!(n != y && n != r, "{what}: 使い回しが前の住人の eid を渡した ({n:#x})");
}

/// #992 の読み直し 2 の B2 の像: `dir` (中に `db`) の像を開く道の、 突き合わせの後の最初の本体の書き出しの `at` で電源断の像 (`Mode::Lost`) を
/// `out` に撮る。 開いた engine は突き合わせの 1. が場所を 1 つ完了させた事を確かめて捨てる。
fn cut_the_write_out_of_an_open(dir: &Path, out: &Path, at: Phase) {
    crashsim::start();
    adopt_as_durable(dir);
    let (armed, shot) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let (a2, s2, l, i) = (armed.clone(), shot.clone(), dir.to_path_buf(), out.to_path_buf());
    let off = HookOff;
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::OpenBeforeFinishReconcile {
            a2.store(true, Ordering::SeqCst);
        }
        if p == at && a2.load(Ordering::SeqCst) && !s2.swap(true, Ordering::SeqCst) {
            crashsim::capture(&l, &i, Mode::Lost).unwrap();
        }
    })));
    let eng = Engine::open_concurrent_with_oplog(dir.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    drop(off);
    assert!(shot.load(Ordering::SeqCst), "前提: 開く道の突き合わせの後の書き出しの途中で撮った");
    assert_eq!(eng.generation_repairs(), (1, 0), "前提: 開く道の突き合わせが gen.seg だけ死の場所を完了させた");
    drop(eng);
    crashsim::stop();
}

/// #992 の読み直し 2 の B2: 開く道 (O1) の突き合わせが gen.seg だけ死の場所を完了させ (世代を上げて死・後始末待ちの印を gen.seg に書き、
/// 生きている印を下ろす)、 その後の最初の本体の書き出しの生きている印の後・gen.seg の前 (`BeforeGenerations`) で電源が落ちた像を開く (O2)。
/// O1 は gen.seg を書き出してから生きている印を下ろすので、 O2 は後始末待ちの印から仕上げに回す。 前は生きている印 (先に書き出す) だけが
/// 届き、 O2 は 「死んでいて生きていない」 場所として何も拾わなかった:
/// - GenPage (削除の途中の像): 名前 (Number、 gen.seg の後に書き出す列) の cell が値の引きに出続けた
/// - 「今 − 2」 の場所の使い回しの像: O1 が上げた世代を失い、 次の使い回しが Y の番号をもう一度渡した
#[test]
fn a_power_loss_in_the_first_write_out_after_the_reconcile_keeps_the_cleanup_findable() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    {
        let root = scratch("torn992_b2_genpage");
        let _rm = Torn992Root(root.clone());
        let (_, a, r) = torn992_image(&root, Torn992::GenPage);
        let (img, cut) = (root.join("img"), root.join("cut"));
        cut_the_write_out_of_an_open(&img, &cut, Phase::BeforeGenerations);
        let eng = Engine::open_concurrent_with_oplog(cut.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
        assert!(!eng.is_live(r));
        assert_eq!(eng.resumed_cleanups(), 1, "O1 の後始末待ちを O2 が拾わない (GenPage)");
        assert_torn_delete_cleaned(&eng, a, r, true, "開く道の書き出しの途中の電源断 (GenPage)");
        drop(eng);
    }
    {
        let root = scratch("torn992_b2_two_up");
        let _rm = Torn992Root(root.clone());
        let (img2, a, r, y) = two_raises_reuse_image(&root);
        let cut = root.join("cut");
        cut_the_write_out_of_an_open(&img2, &cut, Phase::BeforeGenerations);
        let eng = Engine::open_concurrent_with_oplog(cut.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
        assert!(!eng.is_live(y));
        assert_eq!(eng.resumed_cleanups(), 1, "O1 の後始末待ちを O2 が拾わない (「今 − 2」 の場所の使い回し)");
        assert_two_raises_reuse_finished(&eng, a, r, y, "開く道の書き出しの途中の電源断 (「今 − 2」 の場所の使い回し)");
        drop(eng);
    }
}

/// #992 の読み直し 2 の S2: [`two_raises_reuse_image`] の A1 と同じく 「今 − 2」 の場所を Y で使い回し、 WAL の fsync の後・本体の `prev.seg`
/// の書き出しの前 (`BeforeGate`) で電源が落ちた像を開く。 再生が Y の record で場所を生かす時、 使い回しの後始末のやり直し
/// (`redo_reuse_eviction`) は前の住人を場所の最後の行 r (「今 − 2」 の印) と読み、 記録に r を書く (使い回しの道 `prepare_reuse` と同じ)。
/// 前は 1 つ下 (r + 1、 無かった行) と決め打ちし、 r の削除の版数と名前を無かった行の identity に退避した。
#[test]
fn a_replayed_reuse_of_a_slot_finished_with_two_raises_records_the_last_row_as_the_former_resident() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_redo_two_up");
    let _rm = Torn992Root(root.clone());
    let (db, _a, r) = torn992_image(&root, Torn992::GenPage);
    let (img, img2) = (root.join("img"), root.join("img2"));
    crashsim::start();
    adopt_as_durable(&img);
    let y = {
        let a1 = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        assert_eq!(a1.generation_repairs(), (1, 0), "前提: A1 の突き合わせが r の場所を完了させた (世代を 2 つ上げた)");
        a1.oplog_sync().unwrap();
        a1.persist_sync_state().unwrap();
        let _hold = a1.hold_consumer();
        let y = a1.entity_in("notes").unwrap();
        a1.tie_to(y, "notes.note", TORN992_Y_NAME);
        assert!(a1.eid_slot(y) == a1.eid_slot(r) && y != r, "前提: Y は r の場所を使い回した");
        assert_eq!(a1.prev_resident_of_slot(a1.eid_slot(r)).map(|p| p.1), Some(eid_local(r)), "前提: 使い回しの道は前の住人を r と記録する");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), img.clone(), img2.clone());
        let _off = HookOff;
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !s2.swap(true, Ordering::SeqCst) {
                crashsim::capture(&l, &i, Mode::Lost).unwrap();
            }
        })));
        a1.oplog_sync().unwrap();
        drop(_off);
        assert!(shot.load(Ordering::SeqCst), "前提: WAL の fsync の後・本体の書き出しの初めで撮った");
        y
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img2.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert!(eng.is_live(y), "前提: 再生が Y を生かした");
    let slot = eng.eid_slot(r);
    let rec = eng.prev_resident_of_slot(slot);
    assert_eq!(rec.map(|p| (p.1, p.3)), Some((eid_local(r), true)), "使い回しのやり直しが前の住人を r と記録しない: {rec:x?} (r {r:#x})");
    drop(eng);
}

/// #1008 の付け (gen の書き出しの間に同じ場所を 2 回使い回した像): x を消して書き出した後 (gen.seg = 「次の世代 g、 死」)、 本体の書き出しの
/// 途中 (中身を指す列の後) に、 その場所を y で使い回して消し、 z でもう一度使い回す。 生きている印 (z) は届き、 gen.seg は届かない
/// (`BeforeGenerations` で撮る)。 y と z の record は WAL に届いていない。 開く時の突き合わせは gen.seg だけ死の場所として完了させるが、
/// 前の 「今 + 1」 (= z の世代) まで上げる形は、 次の使い回しに z の番号 (アプリに渡った) をもう一度渡した。 使い回しの記録 (`prev.seg`、
/// z の前の住人 = y) から最後の住人 z を引き、 z の次まで上げる。
#[test]
fn a_slot_reused_twice_during_a_write_out_does_not_hand_out_the_last_residents_eid_again() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("torn992_twice");
    let _rm = Torn992Root(root.clone());
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let (x, y, z) = {
        let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let w = eng.entity_in("t").unwrap();
        eng.tie_to(w, "t.v", 1u32);
        let keep = eng.entity_in("t").unwrap();
        eng.tie_to(keep, "t.v", 2u32);
        eng.delete(w);
        let x = eng.entity_in("t").unwrap(); // v15 に移って w の場所を使い回す (世代 1)
        eng.tie_to(x, "t.v", 3u32);
        eng.delete(x); // 場所は 「世代 2、 死」
        eng.persist_tables().unwrap();
        eng.oplog_sync().unwrap();
        eng.oplog_sync().unwrap(); // x の削除は書き出し済み (checkpoint を越えた、 header も)
        let _hold = eng.hold_consumer();
        let (e2, made) = (eng.clone(), Arc::new(std::sync::Mutex::new(None)));
        let made2 = made.clone();
        let (l, i) = (live.clone(), img.clone());
        let _off = HookOff;
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::PrevWrittenOut && made2.lock().unwrap().is_none() {
                // 削除の門は開いている: 別の thread で y で使い回して消し、 z でもう一度使い回す (record は WAL の fsync の後 = 届かない。
                // 使い回しの記録は門を閉じる時に書き出し直す)
                let e3 = e2.clone();
                let h = std::thread::spawn(move || {
                    let y = e3.entity_in("t").unwrap();
                    e3.tie_to(y, "t.v", 4u32);
                    e3.delete(y);
                    let z = e3.entity_in("t").unwrap();
                    e3.tie_to(z, "t.v", 5u32);
                    (y, z)
                });
                let until = std::time::Instant::now() + std::time::Duration::from_secs(60);
                while !h.is_finished() && std::time::Instant::now() < until {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                assert!(h.is_finished(), "前提: 書き出しの途中の使い回しと削除が 60 秒で終わらない");
                *made2.lock().unwrap() = Some(h.join().unwrap());
            }
            if p == Phase::BeforeGenerations && made2.lock().unwrap().is_some() {
                crashsim::capture(&l, &i, Mode::Lost).unwrap();
            }
        })));
        eng.tie_to(keep, "t.v", 6u32); // 書き出す物を作る
        eng.oplog_sync().unwrap();
        drop(_off);
        let (y, z) = made.lock().unwrap().expect("前提: 書き出しの途中で 2 回使い回した");
        assert!(eng.eid_slot(y) == eng.eid_slot(x) && eng.eid_slot(z) == eng.eid_slot(x), "前提: x の場所を y と z で使い回した");
        assert!(x != y && y != z && x != z, "前提: 世代の違う eid");
        (x, y, z)
    };
    crashsim::stop();
    let db = img.join("db");
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(eng.generation_repairs(), (1, 0), "前提: 生きている印は z、 gen.seg は x の削除の後 (y / z の前) の像");
    assert!(!eng.is_live(z) && !eng.is_live(y), "前提: y と z の record は WAL に届いていない");
    // y / z の record は WAL に無い (相手は知らない) ので Delete を作り直さない。 無かった行宛ての Delete も作らない (#992 の読み直し 2)
    let slot = eng.eid_slot(x);
    let deletes: Vec<u32> = slot_deletes(&eng, slot).into_iter().map(|(d, _)| d).collect();
    assert!(
        deletes.iter().all(|&d| d < eid_local(y)),
        "x の後の行 (y / z / 無かった行) 宛ての Delete を作った ({deletes:x?}、 x {x:#x} y {y:#x} z {z:#x})"
    );
    let n = eng.entity_in("t").unwrap();
    assert_eq!(eng.eid_slot(n), eng.eid_slot(x), "前提: 同じ場所を使い回す");
    assert!(![x, y, z].contains(&n), "使い回しが前の住人の eid ({n:#x}、 x {x:#x} y {y:#x} z {z:#x}) を渡した");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// #916 の読み直し A1: 受けた行 F (他の peer C の行、 写像 (C,f)→場所 s) を A のアプリが消し、 書き出しの版数の段 (tombstone) の前で
/// 落ちる。 開くと再生が F の Delete を当て直して tombstone を戻す (場所は使い回していない = 場所の最後の行は F)。 #919 の最初の形は
/// 「逆写像があり、 record の世代が場所の世代より古い」 だけで F 自身の Delete も捨て、 tombstone を失った F に C の遅い書き込みが当たって
/// A でだけ生き返った (state にも F の Delete が出なかった)。
#[test]
fn a_deleted_translated_row_keeps_its_tombstone_after_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("translated_delete");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let world_f = enchudb_oplog::make_eid(3, 7);
    let hid = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "c", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let hid = eng.himo_id("t.c").unwrap() as u16;
        // 自分の行を消して使い回し、 v15 (世代を持つ DB) にする
        let a = eng.entity_in("t").unwrap();
        eng.tie_to(a, "t.c", 1u32);
        let keep = eng.entity_in("t").unwrap();
        eng.tie_to(keep, "t.c", 2u32);
        eng.delete(a);
        let r = eng.entity_in("t").unwrap();
        assert_ne!(r, a, "前提: v15");
        // C の行 F を受ける
        let t = eng.resolve_remote_eid(world_f, hid).unwrap();
        assert!(eng.remote_tie_apply_result(t, hid, 5u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
        eng.persist_sync_state().unwrap(); // 写像を書き出す
        eng.persist_tables().unwrap();
        eng.oplog_sync().unwrap();
        let f = eng.pull_raw("t.c", 5u32);
        assert_eq!(f.len(), 1, "前提: A に F がある");
        let _hold = eng.hold_consumer();
        eng.delete(f[0]);
        assert!(eng.pull_raw("t.c", 5u32).is_empty(), "前提: F は消えた");
        // 書き出しの、 版数の列と tomb.seg の前で撮る (entities と gen.seg は届いている)
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeVersions && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: 書き出しの間に撮った");
        hid
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert!(eng.pull_raw("t.c", 5u32).is_empty(), "前提: 開いた像で F は消えている");
    let t = eng.resolve_remote_eid_existing(world_f).expect("前提: F の写像は残る");
    let (records, _) = eng.state_records();
    let later = Hlc { wall: u64::MAX / 4, logical: 0, peer: 3 };
    let revived = eng.remote_tie_apply(t, hid, 9u32, later);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(!revived, "消えた受けた行 F に、 削除を知らない相手の遅い書き込みが当たって生き返った (tombstone を失った)");
    assert!(
        records.iter().any(|r| matches!(r.op, DecodedOp::Delete { eid } if eid == world_f)),
        "state に受けた行 F の Delete が無い"
    );
}

/// review930 [1] の DB: sync する (cell の版数あり)、 表 t (枠 2、 c = Number)、 peer A = 1。 C (peer 3) の行 F (`world_f`) を受けて
/// 写像と本体を書き出し、 consumer の周期を止めた engine と c の紐を渡す。
fn received_row_engine(db: &Path, world_f: u64) -> (Arc<Engine>, u16) {
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 2).unwrap();
    eng.define_himo_in("t", "c", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let t = eng.resolve_remote_eid(world_f, hid).unwrap();
    assert!(eng.remote_tie_apply_result(t, hid, 5u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
    eng.persist_sync_state().unwrap(); // 写像と本体を書き出す
    eng.oplog_sync().unwrap();
    assert_eq!(eng.pull_raw("t.c", 5u32).len(), 1, "前提: A に F がある");
    (eng, hid)
}

/// review930 [1] (a): A が受けた行 F (C の行、 写像 (C,f)→場所 s) に C の削除を当てた後、 本体を書き出す前に `.eidmap` だけが disk に
/// 届いた (本体を書き出さずに `.eidmap` を書く公開の `persist_tables()`。 WAL の空け作り `make_wal_room_inner`・新しい紐を足す受信
/// `ensure_himo_dynamic_in` も同じ順) 像を開く。 写像の面には削除の版数があり、 場所 s は生きていて (F の cell は削除より古い)、 場所の
/// 削除の版数は無い。
///
/// 前は #925 の見分けがこれを 「落ちる前に場所を使い回した」 と読んで写像を外し、 F は持ち主の居ない生きた行のまま残った (削除が
/// 届き直しても退避表で止まる。 A の state は F を A の行として出した)。 今は、 場所の行が削除の後に作り直された時 (cell が全部削除より
/// 新しい) だけ外す。 ここは写像と削除の版数を戻し、 開く時の sweep が F を消す。
#[test]
fn a_received_delete_whose_eidmap_reached_disk_before_the_body_still_removes_the_row() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("eidmap_before_body_received");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_f = enchudb_oplog::make_eid(3, 7);
    {
        let (eng, _) = received_row_engine(&live.join("db"), world_f);
        let _hold = eng.hold_consumer();
        assert!(
            eng.remote_delete_apply_foreign(world_f, Hlc { wall: 2_000, logical: 0, peer: 3 }),
            "前提: C の削除が当たった"
        );
        assert!(eng.pull_raw("t.c", 5u32).is_empty(), "前提: F は消えた");
        // 本体を書き出さずに `.eidmap` を書く
        eng.persist_tables().unwrap();
        assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: `.eidmap` に F の削除の版数が届いた");
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
    }
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let rows = eng.pull_raw("t.c", 5u32);
    let mapped = eng.resolve_remote_eid_existing(world_f);
    let (records, _) = eng.state_records();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(rows.is_empty(), "電源断の像で、 受けた削除の F が写像の無い生きた行として残った: {rows:x?}");
    assert!(mapped.is_some(), "F の写像が戻らない (外した)");
    assert!(
        !records.iter().any(|r| matches!(r.op, DecodedOp::Tie { value: 5, .. })),
        "A の state に F の値を持つ行がある"
    );
}

/// review930 [1] (b): A のアプリが受けた行 F を消し、 WAL (Delete) と `.eidmap` (写像の面の削除の版数) だけが disk に届いた像を開く。
/// 再生は F を消す。 前は #925 の見分けが写像を外していて、 bridge は Delete を自分の世界番号 (A, F の場所) で配った = 相手 (C も
/// 他の peer も) は知らない A の行の削除として退避表に載せるだけで、 F は A でだけ消えた。 今は写像を戻し、 Delete は (C, f) 宛て。
#[test]
fn a_local_delete_of_a_translated_row_whose_eidmap_reached_disk_before_the_body_goes_to_its_owner() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("eidmap_before_body_local");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_f = enchudb_oplog::make_eid(3, 7);
    {
        let (eng, _) = received_row_engine(&live.join("db"), world_f);
        let _hold = eng.hold_consumer();
        let f = eng.pull_raw("t.c", 5u32);
        eng.delete(f[0]);
        assert!(eng.pull_raw("t.c", 5u32).is_empty(), "前提: F は消えた");
        eng.oplog_commit();
        // WAL だけ書き出す (本体は書き出さない) 後に、 本体を書き出さずに `.eidmap` を書く
        eng.oplog_arc().unwrap().fsync().unwrap();
        eng.persist_tables().unwrap();
        assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: `.eidmap` に F の削除の版数が届いた");
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
    }
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let rows = eng.pull_raw("t.c", 5u32);
    let deletes: Vec<u64> = distributed(&eng)
        .into_iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some(eid),
            _ => None,
        })
        .collect();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(rows.is_empty(), "前提: 再生が F を消した");
    assert_eq!(deletes, vec![world_f], "F の Delete が持ち主の世界番号 (C, f) 宛てに配られない ({deletes:x?})");
}

// ──── `prev.seg` (場所を使い回した時の前の住人の記録、 EID_REUSE_PLAN.md の 「決まった事 (2026-11-15 夜)」 = 案 C) ────

/// `prev.seg` の試験の DB: sync する (cell の版数あり)、 表 t (枠 2、 名前 k = Number、 v = Number)、 peer A = 1。 1 度使い回して
/// v15 にし、 名前 7 の X と名前 1 の行を置いて全部 (WAL・本体・sidecar) 書き出す。 consumer の周期を止めずに engine と X を渡す。
fn full_named_table_engine(db: &Path) -> (Arc<Engine>, u64) {
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 2).unwrap();
    eng.define_himo_in("t", "k", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    let k = eng.himo_id("t.k").unwrap() as u16;
    eng.set_table_pk("t", k).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let w = eng.entity_in("t").unwrap();
    eng.tie_to(w, "t.k", 100u32);
    let keep = eng.entity_in("t").unwrap();
    eng.tie_to(keep, "t.k", 1u32);
    eng.delete(w);
    let x = eng.entity_in("t").unwrap();
    assert_eq!(eng.eid_slot(x), eng.eid_slot(w), "前提: 使い回した (v15)");
    eng.tie_to(x, "t.k", 7u32);
    eng.tie_to(x, "t.v", 70u32);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: `.eidmap` は memory と同じ");
    (eng, x)
}

/// X を消し、 sidecar (`tables` / `.eidmap` …) と WAL・本体を書き出して checkpoint を X の Delete の先へ進める (落ちた後の再生は
/// X の Delete を当て直さない = X の削除は本体の tomb.seg にだけある)。 X の削除の版数と、 consumer の周期を止めた guard を返す
/// (落とすまで止めたまま。 呼び手はこの後の仕度もこの guard の下でする)。
///
/// consumer は消す前から止める (#981): 周期は head が checkpoint より先にあると自分の Commit を積んで書き出す。 `oplog_sync` の
/// 書き出しの最中 (Commit を積んで覆う位置を取った後・checkpoint を進める前) に周期が来ると、 周期の Commit (payload 0 = 120 B) は
/// `oplog_sync` の覆う位置の先に積まれ、 それを書き出して checkpoint を進めるのは周期の方 = `oplog_sync` が返った時、 checkpoint は
/// head の 120 B 手前にある (X の Delete は checkpoint の手前。 #933 と同じ形)。 書き出しが遅い (CI / fsync を遅らせる shim) と、
/// 書き出しの間に周期 (100 ms) が来る。 止めると WAL に積むのも checkpoint を進めるのもこの thread だけになる。
fn delete_and_checkpoint(eng: &Engine, x: u64) -> (Hlc, ConsumerHold<'_>) {
    let hold = eng.hold_consumer();
    eng.delete(x);
    eng.oplog_commit();
    let tomb = eng.tombstone_hlc(x);
    assert_ne!(tomb, Hlc::ZERO, "前提: 消した行の削除の版数");
    eng.persist_sync_state().unwrap();
    eng.oplog_sync().unwrap();
    let s = eng.stats();
    assert_eq!(s.oplog_checkpoint, s.oplog_head, "前提: checkpoint が消した行の Delete の先へ進んだ");
    (tomb, hold)
}

/// 名前 7 の行 Y を足す (X の場所を使い回す)。
fn add_seven(eng: &Engine, v: u32) -> u64 {
    let y = eng.entity_in("t").unwrap();
    eng.tie_to(y, "t.k", 7u32);
    eng.tie_to(y, "t.v", v);
    eng.oplog_commit();
    y
}

/// 像を開いて、 state の Delete (宛名, 版数) と 名前 7 の行の v を返す。
fn open_and_read_state(img: &Path) -> (Vec<(u64, Hlc)>, Vec<u64>, u32) {
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let (records, _) = eng.state_records();
    let deletes = records
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some((eid, r.hlc)),
            _ => None,
        })
        .collect();
    let mut v: Vec<u64> = eng.pull_raw("t.k", 7u32).into_iter().filter_map(|e| eng.get(e, "t.v")).collect();
    v.sort_unstable();
    let restored = eng.prev_residents_restored();
    (deletes, v, restored)
}

/// A のアプリが X を消して書き出した (checkpoint は X の Delete の先) 後、 同じ名前の Y で場所を使い回し、 `oplog_sync` (WAL・名前の
/// 記録・本体を書き出す。 `.eidmap` は書かない) が返った後、 consumer の次の周期 (`.eidmap` に移した削除を書く) の前に電源が落ちる。
/// 開いた A の state に X の Delete (X の番号と削除の版数) がある。
///
/// 前は使い回しが identity に移した X の削除が memory だけで、 本体には 0 にした tomb.seg が届き、 WAL の再生も (checkpoint の手前)
/// 戻さないので、 X の削除がどこにも残らなかった (#914 の形: 取りこぼした peer が state で取り直すと Y を X に束ねて消す)。
#[test]
fn a_moved_delete_of_a_reused_slot_survives_power_loss_before_the_sidecar() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_own_after_sync");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let (tomb, _hold) = delete_and_checkpoint(&eng, x);
        let y = add_seven(&eng, 77);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        eng.oplog_sync().unwrap();
        assert!(eng.eidmap_disk_diff().unwrap().is_some(), "前提: 移した X の削除は `.eidmap` に届いていない");
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
        (x, tomb)
    };
    crashsim::stop();
    let (deletes, v, restored) = open_and_read_state(&img);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(v, vec![77], "前提: 開いた像に Y がある");
    assert!(deletes.contains(&(x, tomb)), "電源断の像の state に、 使い回した場所の前の住人 X の Delete が無い: {deletes:x?}");
    assert_eq!(restored, 1, "`prev.seg` の記録から X の削除を戻していない");
}

/// X の削除を書き出した (checkpoint は X の Delete の先) 後、 本体の書き出しの途中 (`prev.seg` と生きている印を書き出した後・版数の
/// 列の前) に A のアプリが X の場所を使い回す。 書き出しは使い回しが 0 にした tomb.seg も書き出すので、 書き出しが返った後の電源断の
/// 像には 0 の tomb.seg が届く。 その記録 (書き出しの初めに `prev.seg` を書き出した後に書いた) も、 tomb.seg の前に門を閉じて書き出し
/// 直すので届いている = 開いた A の state に X の Delete がある。
///
/// 書き出し直しが無いと、 0 の tomb.seg だけが届き X の削除がどこにも残らない (記録の無い master も同じ)。
#[test]
fn a_reuse_during_the_write_out_reaches_disk_with_its_record_before_the_cleared_tombstone() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_own_during_write_out");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let (tomb, _hold) = delete_and_checkpoint(&eng, x);
        let reused: Arc<std::sync::Mutex<Option<u64>>> = Arc::new(std::sync::Mutex::new(None));
        let (e2, r2) = (eng.clone(), reused.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeVersions && r2.lock().unwrap().is_none() {
                *r2.lock().unwrap() = Some(add_seven(&e2, 77));
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        let y = reused.lock().unwrap().expect("前提: 書き出しの途中で使い回した");
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
        (x, tomb)
    };
    crashsim::stop();
    let (deletes, _, restored) = open_and_read_state(&img);
    let _ = std::fs::remove_dir_all(&root);
    assert!(deletes.contains(&(x, tomb)), "書き出しの途中で使い回した場所の前の住人 X の Delete が state に無い: {deletes:x?}");
    assert_eq!(restored, 1, "`prev.seg` の記録から X の削除を戻していない");
}

/// X の削除を書き出した (checkpoint は X の Delete の先) 後に X の場所を Y で使い回し、 本体の書き出しの途中で電源が落ちる:
/// - 版数の列の前 (`BeforeVersions`): 生きている印 (Y が生きている) は届き、 tomb.seg には X の削除の版数が残る。 記録は書き出しの
///   初め (生きている印の前) に届いている
/// - tomb.seg の段の直前 (`BeforeTomb`): 門を閉じて記録を書き出し直した後
///
/// どちらの像でも、 開いた A で Y は生き、 state に X の Delete がある。 前は X の削除の版数が生きている Y の場所の tomb.seg に残るだけで、
/// state に X の Delete が無かった。
#[test]
fn a_reuse_cut_before_the_tombstones_keeps_the_reused_row_and_the_delete() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for shot_at in [Phase::BeforeVersions, Phase::BeforeTomb] {
        let root = scratch("prev_own_before_tomb");
        let (live, img) = (root.join("live"), root.join("img"));
        std::fs::create_dir_all(&live).unwrap();
        crashsim::start();
        let (x, tomb) = {
            let (eng, x) = full_named_table_engine(&live.join("db"));
            let (tomb, _hold) = delete_and_checkpoint(&eng, x);
            let y = add_seven(&eng, 77);
            assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
            let shot = Arc::new(AtomicBool::new(false));
            let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == shot_at && !s2.swap(true, Ordering::SeqCst) {
                    let _ = crashsim::capture(&l, &i, Mode::Lost);
                }
            })));
            eng.oplog_sync().unwrap();
            write_out_hook::set(None);
            assert!(shot.load(Ordering::SeqCst), "前提: {shot_at:?} で撮った");
            (x, tomb)
        };
        crashsim::stop();
        let (deletes, v, _) = open_and_read_state(&img);
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(v, vec![77], "{shot_at:?}: 開いた像で使い回した Y が生きていない");
        assert!(deletes.contains(&(x, tomb)), "{shot_at:?}: state に使い回した場所の前の住人 X の Delete が無い: {deletes:x?}");
    }
}

/// #925 の電源断の形: A (枠 1 の表) が C の行 F を受けて写像を書き出した後、 A のアプリが F を消し (`.eidmap` の写像の面に削除の
/// 版数は無い)、 Y で F の場所を使い回して `oplog_sync` が返った後、 consumer の次の周期 (外した写像を書く) の前に電源が落ちる。
/// 開いた A で写像 (C, f) は戻らず、 A の state は Y を A の行として出し、 F の Delete を (C, f) 宛てに出す。
///
/// 前は #925 の推測 (面に削除の版数がある時だけ外す) が写像を戻し、 Y が C の行 F として扱われた。
#[test]
fn a_locally_deleted_translated_slot_reused_before_power_loss_stays_unmapped() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_translated");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_f = enchudb_oplog::make_eid(3, 7);
    let tomb = {
        let db = live.join("db");
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 1).unwrap();
        eng.define_himo_in("t", "c", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let hid = eng.himo_id("t.c").unwrap() as u16;
        let t = eng.resolve_remote_eid(world_f, hid).unwrap();
        assert!(eng.remote_tie_apply_result(t, hid, 5u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
        eng.persist_sync_state().unwrap();
        eng.oplog_sync().unwrap();
        let f = eng.pull_raw("t.c", 5u32);
        assert_eq!(f.len(), 1, "前提: A に F がある");
        let _hold = eng.hold_consumer();
        eng.delete(f[0]);
        eng.oplog_commit();
        let tomb = eng.tombstone_hlc(f[0]);
        let y = eng.entity_in("t").unwrap();
        eng.tie_to(y, "t.c", 9u32);
        eng.oplog_commit();
        assert_eq!(eng.eid_slot(y), eng.eid_slot(f[0]), "前提: Y は F の場所を使い回した");
        eng.oplog_sync().unwrap();
        assert!(eng.eidmap_disk_diff().unwrap().is_some(), "前提: 外した写像は `.eidmap` に届いていない");
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
        tomb
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let mapped = eng.resolve_remote_eid_existing(world_f);
    let (records, _) = eng.state_records();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    let y_rows: Vec<u64> = records
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Tie { eid, value: 9, .. } => Some(eid),
            _ => None,
        })
        .collect();
    assert!(
        y_rows.len() == 1 && enchudb_oplog::eid_peer(y_rows[0]) == 1,
        "使い回した Y が A の行として state に出ない (C の行 F として出た?): {y_rows:x?}"
    );
    assert!(
        records.iter().any(|r| matches!(r.op, DecodedOp::Delete { eid } if eid == world_f) && r.hlc == tomb),
        "state に F の Delete ((C, f) 宛て) が無い"
    );
    assert_eq!(mapped, None, "開いた A で F の写像が戻った");
}

/// `prev.seg` の試験の DB (翻訳した行): sync する、 表 t (枠 1、 c = Number)、 peer A = 1。 C (peer 3) の行 F を受け、 C の削除も受けて
/// 写像の面 (削除の版数あり) と本体を書き出す。 consumer の周期を止めずに engine と F の削除の版数を渡す。
fn received_and_deleted_row_engine(db: &Path, world_f: u64) -> (Arc<Engine>, Hlc) {
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 1).unwrap();
    eng.define_himo_in("t", "c", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let t = eng.resolve_remote_eid(world_f, hid).unwrap();
    assert!(eng.remote_tie_apply_result(t, hid, 5u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
    let del = Hlc { wall: 2_000, logical: 0, peer: 3 };
    assert!(eng.remote_delete_apply_foreign(world_f, del), "前提: C の削除が当たった");
    eng.persist_sync_state().unwrap();
    eng.oplog_sync().unwrap();
    assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: 写像の面 (F の削除の版数あり) を書き出した");
    (eng, del)
}

/// 像の state の (値 9 の行の宛名, F が消えたままか (C の遅い書き込みが当たらない), 自分の行宛ての Delete) と、 開いた時の (C, f) の
/// 写像。
fn translated_image_state(img: &Path, world_f: u64) -> (Vec<u64>, bool, Vec<u64>, Option<u64>) {
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let mapped = eng.resolve_remote_eid_existing(world_f);
    if mapped.is_some() {
        // 写像が戻った像は、 その後に足した行が F の場所を使い回す (使い回しが F の写像を外す)
        let z = eng.entity_in("t").unwrap();
        eng.tie_to(z, "t.c", 8u32);
        eng.oplog_commit();
    }
    let (records, _) = eng.state_records();
    let nine = records
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Tie { eid, value: 9, .. } => Some(eid),
            _ => None,
        })
        .collect();
    let own_deletes = records
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } if enchudb_oplog::eid_peer(eid) == 1 => Some(eid),
            _ => None,
        })
        .collect();
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let later = Hlc { wall: u64::MAX / 4, logical: 0, peer: 3 };
    let f_dead = match eng.resolve_remote_eid(world_f, hid) {
        None => true,
        Some(t) => !eng.remote_tie_apply(t, hid, 6u32, later),
    };
    (nine, f_dead, own_deletes, mapped)
}

/// 受けた行 F (C の削除も受けて書き出した) の場所を Y で使い回し、 本体の書き出しの途中で電源が落ちる (WAL に Y の record):
/// - `prev.seg` の後・生きている印の前 (`PrevWrittenOut`、 本体には記録だけ): 開く時は記録の行が場所の最後の行 (使い回しが本体に
///   届いていない) なので写像を戻し、 再生が Y を生かす所で写像を外す
/// - 生きている印の後・世代の前 (`BeforeGenerations`、 Y の生きている印と F の死の印が届く): 開く時の突き合わせが場所の世代を上げて
///   Y を殺し、 再生の Y の record は場所の世代より古い。 #916 の守り (場所に逆写像がある) の前に、 記録の行 (F) が Y のすぐ前の
///   住人なので写像を外す (review946 [7])。 #978 の後は、 この像には使い回しが 0 にした tomb.seg と `prev.seg` の 2 回目の書き出しも
///   届く (生きている印の前に削除の門を閉じて tomb.seg を書き出す、 その前に使い回しの門の手順で `prev.seg` をもう一度)。 それでも
///   review946 [7] の道を通る (後始末のやり直しを外す変異で、 この像の Y が state に出ずに落ちる、 #978 の読み直し 2 [F])
///
/// どちらも開いた A の state は Y を A の行として出し、 F は (C, f) の削除で消えたまま。 前は写像が戻ったまま再生が Y を生かし (#925 の形)、
/// `BeforeGenerations` では #916 の守りが Y を捨てた。
#[test]
fn a_translated_reuse_cut_after_its_record_is_finished_by_the_replay() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    for shot_at in [Phase::PrevWrittenOut, Phase::BeforeGenerations] {
        let root = scratch("prev_translated_replay");
        let (live, img) = (root.join("live"), root.join("img"));
        std::fs::create_dir_all(&live).unwrap();
        crashsim::start();
        let world_f = enchudb_oplog::make_eid(3, 7);
        {
            let (eng, _) = received_and_deleted_row_engine(&live.join("db"), world_f);
            let _hold = eng.hold_consumer();
            let y = eng.entity_in("t").unwrap();
            eng.tie_to(y, "t.c", 9u32);
            eng.oplog_commit();
            let shot = Arc::new(AtomicBool::new(false));
            let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == shot_at && !s2.swap(true, Ordering::SeqCst) {
                    let _ = crashsim::capture(&l, &i, Mode::Lost);
                }
            })));
            eng.oplog_sync().unwrap();
            write_out_hook::set(None);
            assert!(shot.load(Ordering::SeqCst), "前提: {shot_at:?} で撮った");
        }
        crashsim::stop();
        let (nine, f_deleted, _, mapped) = translated_image_state(&img, world_f);
        let _ = std::fs::remove_dir_all(&root);
        assert!(
            nine.len() == 1 && enchudb_oplog::eid_peer(nine[0]) == 1,
            "{shot_at:?}: 再生が生かした Y が A の行として state に出ない (C の行 F として出た? / 捨てた?): {nine:x?}"
        );
        assert!(f_deleted, "{shot_at:?}: 消えた F に C の遅い書き込みが当たった (F の削除を失った)");
        assert_eq!(mapped, None, "{shot_at:?}: Y を生かした後も F の写像が場所を指す");
    }
}

/// 受けた行 F の場所を、 本体の書き出しの途中 (WAL の書き出しの後) で Y が使い回し、 `prev.seg` の後・生きている印の前に電源が
/// 落ちる (記録は届き、 Y の record は WAL に無い)。 開いた A では Y は無く、 F の写像は戻る (場所に F の削除が残る)。 その後に足した
/// 行 Z が場所を使い回すと F の削除は (C, f) に退避され、 A の行の Delete は出ない。
///
/// 開く時に記録だけで写像を外すと、 Z の使い回しが前の住人 F を A の行と読み、 A の行の Delete (幻) を出す。
#[test]
fn a_translated_reuse_record_without_its_new_row_keeps_the_mapping() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_translated_record_only");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_f = enchudb_oplog::make_eid(3, 7);
    {
        let (eng, _) = received_and_deleted_row_engine(&live.join("db"), world_f);
        let _hold = eng.hold_consumer();
        let shot = Arc::new(AtomicBool::new(false));
        let reused = Arc::new(AtomicBool::new(false));
        let (s2, r2, e2, l, i) = (shot.clone(), reused.clone(), eng.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !r2.swap(true, Ordering::SeqCst) {
                let y = e2.entity_in("t").unwrap();
                e2.tie_to(y, "t.c", 9u32);
                e2.oplog_commit();
            }
            if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(reused.load(Ordering::SeqCst) && shot.load(Ordering::SeqCst), "前提: 書き出しの途中で使い回して撮った");
    }
    crashsim::stop();
    let (nine, f_deleted, own_deletes, mapped) = translated_image_state(&img, world_f);
    let _ = std::fs::remove_dir_all(&root);
    assert!(nine.is_empty(), "前提: 像に Y は無い (WAL に届いていない): {nine:x?}");
    assert!(mapped.is_some(), "使い回しが本体に届いていない像で F の写像を外した");
    assert!(f_deleted, "消えた F に C の遅い書き込みが当たった (F の削除を失った)");
    assert!(own_deletes.is_empty(), "A の行の Delete (前の住人 F を A の行と読んだ幻) を出した: {own_deletes:x?}");
}

/// review946 [1]: 本体の書き出しの WAL の書き出しの後・`prev.seg` の前に A のアプリが X を消して Y で場所を使い回し、 `prev.seg` を
/// 書き出した後 (生きている印の前) に電源が落ちる。 像は 「記録だけが届き、 X の削除 (WAL の Delete も本体も) は届いていない」。
/// 開いた A で X は生きていて、 state に X の Delete は無い。
///
/// 前は開く時に本体を見ずに記録から (A, X) の削除を戻し、 生きている X の Tie と Delete を同じ state に出した (state で取り直した
/// peer でだけ X が消える)。
#[test]
fn a_reuse_record_that_reached_disk_before_the_delete_keeps_the_row_alive() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_record_before_delete");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let x = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let _hold = eng.hold_consumer();
        // WAL に書き出す物を置く (書き出しが走る)
        eng.tie_to(x, "t.v", 71u32);
        eng.oplog_commit();
        let reused: Arc<std::sync::Mutex<Option<u64>>> = Arc::new(std::sync::Mutex::new(None));
        let shot = Arc::new(AtomicBool::new(false));
        let (e2, r2, s2, l, i) = (eng.clone(), reused.clone(), shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && r2.lock().unwrap().is_none() {
                e2.delete(x);
                e2.oplog_commit();
                *r2.lock().unwrap() = Some(add_seven(&e2, 77));
            }
            if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        let y = reused.lock().unwrap().expect("前提: 書き出しの途中で使い回した");
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の後で撮った");
        x
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let alive = eng.is_live(x) && eng.get(x, "t.v") == Some(71);
    let deletes: Vec<u64> = eng
        .state_records()
        .0
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some(eid),
            _ => None,
        })
        .collect();
    let restored = eng.prev_residents_restored();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(alive, "前提: 像の X は生きている (削除は届いていない)");
    assert!(!deletes.contains(&x), "生きている X の Delete を state に出した: {deletes:x?}");
    assert_eq!(restored, 0, "削除の届いていない X を記録から戻した");
}

/// review946c [E]: 本体の書き出しの WAL の書き出しの後に A のアプリが X を消して Y で場所を使い回し、 本体の書き出し (記録・生きている
/// 印・0 にした tomb.seg) が返った後・次の WAL の書き出しの前に電源が落ちる (#451 の前提の像: WAL に X の Delete も Y も無い)。 開いた
/// A は記録から X の削除を戻し (state に入る)、 #451 の作り直しが X の Delete を WAL に載せて bridge が配る (差分で受ける peer にも届く)。
/// 前は作り直しが場所の削除の版数 (tomb.seg、 使い回しが 0 にした) しか見ず、 X の Delete は state にしか無かった (差分で受ける peer
/// では X が生きたまま)。
#[test]
fn a_restored_own_delete_missing_from_the_wal_is_relogged() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_restored_relog");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let _hold = eng.hold_consumer();
        // WAL に書き出す物を置く (書き出しが走る)
        eng.tie_to(x, "t.v", 71u32);
        eng.oplog_commit();
        let reused: Arc<std::sync::Mutex<Option<(u64, Hlc)>>> = Arc::new(std::sync::Mutex::new(None));
        let shot = Arc::new(AtomicBool::new(false));
        let (e2, r2, s2, l, i) = (eng.clone(), reused.clone(), shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && r2.lock().unwrap().is_none() {
                e2.delete(x);
                e2.oplog_commit();
                let tomb = e2.tombstone_hlc(x);
                *r2.lock().unwrap() = Some((add_seven(&e2, 77), tomb));
            }
            if p == Phase::WrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        let (y, tomb) = reused.lock().unwrap().expect("前提: 書き出しの途中で使い回した");
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        assert!(shot.load(Ordering::SeqCst), "前提: 本体の書き出しの後で撮った");
        (x, tomb)
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let alive = eng.is_live(x);
    let ys: Vec<u64> = eng.pull_raw("t.k", 7u32).into_iter().filter_map(|e| eng.get(e, "t.v")).collect();
    let restored = eng.prev_residents_restored();
    let in_state = eng.state_records().0.iter().any(|r| matches!(r.op, DecodedOp::Delete { eid } if eid == x) && r.hlc == tomb);
    eng.transfer_oplog_to_sync_ops();
    let bridged: Vec<(u64, Hlc)> = eng
        .settled_sync_records(|_, _| true)
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some((eid, r.hlc)),
            _ => None,
        })
        .collect();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(!alive && ys == vec![77], "前提: 像では X が消えて Y が生きている (本体が届いた): alive {alive}, Y {ys:?}");
    assert_eq!(restored, 1, "前提: 記録から X の削除を戻した");
    assert!(in_state, "前提: state に X の Delete がある");
    assert!(bridged.contains(&(x, tomb)), "WAL に無い X の Delete を作り直して配らない (差分で受ける peer に届かない): {bridged:x?}");
}

/// 像を開いて、 bridge が配る Delete (宛名, 版数) を返す (WAL の record を `_sync_ops` に写してから読む)。
fn bridged_deletes(eng: &Engine) -> Vec<(u64, Hlc)> {
    eng.transfer_oplog_to_sync_ops();
    eng.settled_sync_records(|_, _| true)
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some((eid, r.hlc)),
            _ => None,
        })
        .collect()
}

/// review946d [N1]: 本体の書き出しの WAL の書き出しの後、 中身の段の後 (`BeforeGate`) に A のアプリが X を消し、 版数の列の前
/// (`BeforeVersions`、 生きている印と世代の書き出しの後) に Y で X の場所を使い回し、 本体の書き出しが返った後・次の WAL の書き出しの
/// 前に電源が落ちる。 像は 「X の削除は本体 (生きている印・世代) に届き、 使い回しは記録 (`prev.seg`) と 0 にした tomb.seg だけが
/// 届いた」 で、 WAL には X の古い Tie がある (X の Delete も Y も無い)。 再生が X の Tie で X を生き返らせる (本体にだけ届いた削除を
/// WAL の record が取り消す) ので、 開いた A の X は生きている = state にも bridge にも X の Delete が無い (A と配る物が一致)。
///
/// 前は戻すかを再生の前に決め (X は死んでいて場所の最後の行 = 戻す)、 再生が X を生き返らせた後も戻したままだった: state に X の
/// Delete があり、 #451 の作り直しも X の Delete を WAL に載せて配った (差分で受ける peer でも X が消え、 A では生きている)。
/// [`a_former_resident_the_replay_revives_is_not_restored`] の像 (`root` の下に作る): X の削除は本体 (生きている印・世代) に届き、
/// 使い回しは記録 (`prev.seg`) と 0 にした tomb.seg だけが届いた。 WAL には X の古い Tie がある。 戻り値は像の DB、 X、 X の削除の版数。
fn revived_former_resident_image(root: &Path) -> (PathBuf, u64, Hlc) {
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let _hold = eng.hold_consumer();
        // WAL に X の record を置く (書き出しが走り、 落ちた後の再生が X を生かす)
        eng.tie_to(x, "t.v", 71u32);
        eng.oplog_commit();
        let deleted: Arc<std::sync::Mutex<Option<Hlc>>> = Arc::new(std::sync::Mutex::new(None));
        let reused: Arc<std::sync::Mutex<Option<u64>>> = Arc::new(std::sync::Mutex::new(None));
        let shot = Arc::new(AtomicBool::new(false));
        let (e2, d2, r2, s2, l, i) = (eng.clone(), deleted.clone(), reused.clone(), shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && d2.lock().unwrap().is_none() {
                e2.delete(x);
                e2.oplog_commit();
                *d2.lock().unwrap() = Some(e2.tombstone_hlc(x));
            }
            if p == Phase::BeforeVersions && r2.lock().unwrap().is_none() {
                *r2.lock().unwrap() = Some(add_seven(&e2, 77));
            }
            if p == Phase::WrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        let y = reused.lock().unwrap().expect("前提: 書き出しの途中で使い回した");
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        assert!(shot.load(Ordering::SeqCst), "前提: 本体の書き出しの後で撮った");
        let tomb = deleted.lock().unwrap().expect("前提: 書き出しの途中で X を消した");
        (x, tomb)
    };
    crashsim::stop();
    (img.join("db"), x, tomb)
}

#[test]
fn a_former_resident_the_replay_revives_is_not_restored() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_revived_by_replay");
    let (db, x, tomb) = revived_former_resident_image(&root);
    // 記録が届いた (再生の前の像。 readonly は WAL を見ない)
    let record = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        ro.prev_resident_of_slot(ro.eid_slot(x))
    };
    let reads_before = enchudb_oplog::oplog::wal_reads();
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    let reads = enchudb_oplog::oplog::wal_reads();
    eng.set_peer_id(1);
    let alive = eng.is_live(x);
    let sevens: Vec<u64> = eng.pull_raw("t.k", 7u32).into_iter().filter_map(|e| eng.get(e, "t.v")).collect();
    let restored = eng.prev_residents_restored();
    let in_state: Vec<Hlc> = eng
        .state_records()
        .0
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } if eid == x => Some(r.hlc),
            _ => None,
        })
        .collect();
    let bridged: Vec<Hlc> = bridged_deletes(&eng).into_iter().filter(|&(e, _)| e == x).map(|(_, h)| h).collect();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(record, Some((1, eid_local(x), tomb, true)), "前提: 像に X の場所の使い回しの記録 (前の住人 X) が届いた");
    assert!(!sevens.contains(&77), "前提: 使い回した Y は本体にも WAL にも届いていない: {sevens:?}");
    assert!(alive, "前提: 再生が X の古い Tie で X を生き返らせた");
    assert!(in_state.is_empty(), "生きている X の Delete を state に出した: {in_state:x?}");
    assert!(bridged.is_empty(), "生きている X の Delete を作り直して配った (差分で受ける peer で X が消える): {bridged:x?}");
    assert_eq!(restored, 0, "再生が生き返らせた X の削除を記録から戻したまま");
    // #972: 戻すかを再生の後に回すために WAL に再生する record があるかを聞く (`restore_prev_residents`) のは、 最初の 1 件で止まる
    // 読み。 全部を解くのは再生の 1 回。 PR #986 の読み直し 2 の [2]: WAL 付きの open は、 開いた WAL に聞く (file を開き直して聞かない)
    let asks = reads.asks - reads_before.asks;
    assert!(asks >= 1, "前提: 記録を戻すかを決める前に WAL に聞いた ({reads:?})");
    assert_eq!(reads.probes - reads_before.probes, 0, "WAL 付きの open が WAL の file を開き直して聞いた ({reads:?})");
    assert_eq!(reads.replays - reads_before.replays, 1, "WAL を全部解いたのが再生の 1 回でない ({reads:?})");
    assert!(reads.probe_records - reads_before.probe_records <= 4 * asks, "聞く読みが最初の数件で止まらない ({reads:?})");
}

// ──── #989 / PR #986 の読み直し 2: 開く時に WAL を聞けない・開けない・WAL の file の stat が失敗した時 ────
//
// 聞く読み (`OpLog::probe_replay`) が Err の時、 #986 の前の engine は 「再生する物が無い」 と読んだ (単独の open は WAL を見ずに
// 開き、 往復の後の見直しは素通り、 WAL 付きの open の突き合わせ / 記録の戻しは再生の前に仕上げた)。 #986 の最初の直しは、 突き合わせが
// 直しを map に書いた後に聞き、 聞けなければ断った: 断った後の開き直しは直した場所を見つけず、 仕上げ (残った cell を落とす) が二度と
// 走らなかった (読み直し 2 の [1])。 WAL 付きの open は突き合わせの後に WAL を開けずに返ると、 Drop が clean の印を立てて後に回した
// 仕上げを失った ([2])。 WAL の file があるかの stat (`Path::exists`) は失敗を 「無い」 と読み、 WAL 付きの open は WAL を作り直した ([3])。
// 今は何かを直す前 (writer lock を取った直後) に決める: 単独の open はそこで一度だけ聞いて、 聞けなければ何も書かずに断り、 WAL 付きの
// open はそこで WAL を開いて、 開いた WAL に聞く。 故障は子 process で起こす (`ENCHU_FAULT_AT`、 数えるだけの回で点の番号を引く)。

const PROBE_CHILD_ENV: &str = "ENCHU_POWER_LOSS_PROBE_CHILD";

/// 子: `ENCHU_POWER_LOSS_PROBE_CHILD` = `<開き方>\t<DB>\t<eid>` を開いて、 結果を 1 行 (`PROBE_CHILD ...`) 出す。 開けたら閉じる。
/// 親が呼ばない時は何もしない。
/// - `standalone` / `writer`: 単独 / WAL 付きで開いて `t.a` の値
/// - `concurrentize`: 単独で開いて `concurrentize_with_oplog` に渡し、 `t.a` の値
/// - `keep`: WAL 付きで開いて、 行が生きているか・`t.c`・`t.a` (Tag)
/// - `prev`: WAL 付きで開いて、 X が生きているか・記録から戻した数・state に出た X の Delete の数
/// - `standalone_gen` / `writer_gen`: 単独 / WAL 付きで開いて、 `notes.note` = eid の欄の値を引いた数と、 開く時の突き合わせが完了させた数
#[test]
fn probe_fault_child() {
    let Ok(spec) = std::env::var(PROBE_CHILD_ENV) else { return };
    let mut it = spec.split('\t');
    let (kind, db, e) = (it.next().unwrap(), it.next().unwrap(), it.next().unwrap().parse::<u64>().unwrap());
    let gen_line = |eng: &Engine| format!("OK {} {:?}", eng.pull_raw("notes.note", e as u32).len(), eng.generation_repairs());
    let line = match kind {
        "standalone" => match Engine::open_standalone(db) {
            Ok(st) => format!("OK {:?}", st.get(e, "t.a")),
            Err(err) => format!("ERR {err}"),
        },
        "writer" => match Engine::open_concurrent_with_oplog(db, OPLOG_CAP) {
            Ok(eng) => format!("OK {:?}", eng.get(e, "t.a")),
            Err(err) => format!("ERR {err}"),
        },
        "concurrentize" => match Engine::open_standalone(db).and_then(|st| Engine::concurrentize_with_oplog(st, OPLOG_CAP)) {
            Ok(eng) => format!("OK {:?}", eng.get(e, "t.a")),
            Err(err) => format!("ERR {err}"),
        },
        "keep" => match Engine::open_concurrent_with_oplog(db, OPLOG_CAP) {
            Ok(eng) => format!(
                "OK {} {:?} {:?}",
                eng.is_live(e),
                eng.get(e, "t.c"),
                eng.get_text_owned(e, "t.a").map(|b| String::from_utf8_lossy(&b).into_owned())
            ),
            Err(err) => format!("ERR {err}"),
        },
        "prev" => match Engine::open_concurrent_with_oplog(db, OPLOG_CAP) {
            Ok(eng) => {
                eng.set_peer_id(1);
                let in_state = eng
                    .state_records()
                    .0
                    .iter()
                    .filter(|r| matches!(r.op, DecodedOp::Delete { eid } if eid == e))
                    .count();
                format!("OK {} {} {in_state}", eng.is_live(e), eng.prev_residents_restored())
            }
            Err(err) => format!("ERR {err}"),
        },
        "standalone_gen" => match Engine::open_standalone(db) {
            Ok(st) => gen_line(&st),
            Err(err) => format!("ERR {err}"),
        },
        "writer_gen" => match Engine::open_concurrent_with_oplog(db, OPLOG_CAP) {
            Ok(eng) => gen_line(&eng),
            Err(err) => format!("ERR {err}"),
        },
        k => panic!("開き方 {k}"),
    };
    println!("PROBE_CHILD {line}");
}

/// [`probe_fault_child`] を別の process で回す。 戻り値: (結果の行 (子が落ちたら `PANIC ...`), 起こした故障 (`ENCHU_FAULT` の後ろ))。
fn run_probe_child(kind: &str, db: &Path, e: u64, envs: &[(&str, String)]) -> (String, Option<String>) {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "probe_fault_child", "--nocapture", "--test-threads=1"])
        .env(PROBE_CHILD_ENV, format!("{kind}\t{}\t{e}", db.display()))
        .envs(envs.iter().map(|(k, v)| (*k, v.as_str())))
        .output()
        .unwrap();
    let (stdout, stderr) = (String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let fired = stderr.lines().find_map(|l| l.split_once("ENCHU_FAULT ").map(|(_, r)| r.to_string()));
    if !out.status.success() {
        // 探している間に別の点 (開いた後に初めて触る segment など) に当たると子は panic しうる
        return (format!("PANIC {}", stderr.lines().last().unwrap_or("")), fired);
    }
    let line = stdout
        .lines()
        .find_map(|l| l.split_once("PROBE_CHILD ").map(|(_, r)| r.to_string()))
        .unwrap_or_else(|| panic!("子の結果が無い: {stdout}\n{stderr}"));
    (line, fired)
}

/// 像 `db` の写しを子で故障なしに開く (数えるだけの回)。 戻り値: (結果, 通った I/O の点 (番号, 名前) の並び)。
fn count_points(root: &Path, db: &Path, kind: &str, e: u64) -> (String, Vec<(u64, String)>) {
    let counted = root.join("counted");
    let _ = std::fs::remove_dir_all(&counted);
    copy_tree(db, &counted);
    let trace = root.join("trace.txt");
    let _ = std::fs::remove_file(&trace);
    let (clean, _) = run_probe_child(kind, &counted, e, &[("ENCHU_FAULT_TRACE", trace.to_str().unwrap().to_string())]);
    let points = std::fs::read_to_string(&trace)
        .unwrap()
        .lines()
        .filter_map(|l| l.split_once('\t'))
        .map(|(n, s)| (n.parse().unwrap(), s.to_string()))
        .collect();
    (clean, points)
}

/// 名前 `site` の点の番号 (通った順)。
fn points_named(points: &[(u64, String)], site: &str) -> Vec<u64> {
    points.iter().filter(|(_, s)| s == site).map(|(n, _)| *n).collect()
}

/// 像 `db` の写し (`after` に置き、 故障の後も残す) を子で開き、 数えた回の `site` の点のうち `nth` 番目 (1 始まり) で EIO を 1 回起こす。
/// consumer の thread も同じ数を進めるので、 当たった点が `site` になるまで、 同じ名前の前後の点の間で近くを探す。 戻り値は故障ありの結果。
fn fault_at(db: &Path, kind: &str, e: u64, points: &[(u64, String)], site: &str, nth: usize, after: &Path) -> String {
    let named = points_named(points, site);
    assert!(named.len() >= nth, "前提: {kind} の open が {site} を {nth} 回以上通る: {points:?}");
    let at = named[nth - 1] as i64;
    let lo = if nth > 1 { named[nth - 2] as i64 } else { 0 };
    let hi = named.get(nth).map_or(i64::MAX, |&n| n as i64);
    for d in 0..=16i64 {
        for n in [at + d, at - d] {
            if n <= lo || n >= hi {
                continue;
            }
            let _ = std::fs::remove_dir_all(after);
            copy_tree(db, after);
            let (line, fired) = run_probe_child(kind, after, e, &[("ENCHU_FAULT_AT", n.to_string())]);
            if fired.as_deref().is_some_and(|f| f.ends_with(&format!(" {site}"))) {
                return line;
            }
        }
    }
    panic!("{kind}: {nth} 番目の {site} の点に故障を当てられなかった (数えた点 {points:?})");
}

/// 像 `db` の写しを子で開き、 `nth` 番目 (1 始まり) の `site` の点で EIO を 1 回起こす ([`fault_at`])。 戻り値: (故障なしの結果, 故障ありの結果)。
fn open_with_fault(root: &Path, db: &Path, kind: &str, e: u64, site: &str, nth: usize, after: &Path) -> (String, String) {
    let (clean, points) = count_points(root, db, kind, e);
    (clean, fault_at(db, kind, e, &points, site, nth, after))
}

/// `src` の directory を `dst` に写す。
fn copy_tree(src: &Path, dst: &Path) {
    assert!(Command::new("cp").arg("-R").arg(src).arg(dst).status().unwrap().success());
}

/// 単独の open が WAL にだけある値を見る像 (#880 の A と同じ): 行 x の `t.a` は 1 が本体に届き、 2 は WAL にだけある (WAL を fsync
/// した後・本体の列を書き出す前に撮った)。 戻り値は像の DB と x。
fn wal_only_value_image(root: &Path) -> (PathBuf, u64) {
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let db = live.join("db");
    let x = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 16).unwrap();
        eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        let hold = eng.hold_consumer();
        let x = eng.entity_in("t").unwrap();
        eng.tie_to(x, "t.a", 1u32);
        eng.oplog_sync().unwrap();
        eng.tie_to(x, "t.a", 2u32);
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: 像を撮った");
        drop(hold);
        x
    };
    crashsim::stop();
    (img.join("db"), x)
}

/// #989 (a): 単独の open の往復の前の見直しが聞けなければ、 WAL を見ずに開かない (断る)。 断った後に開き直せば往復して WAL にだけある値を
/// 見る。 前は 「再生する物が無い」 と読んで WAL を見ずに開き、 `t.a` が 1 (本体の値) だった。
#[test]
fn a_probe_failure_before_the_standalone_round_trip_refuses_to_open() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("probe_fault_before");
    let (db, x) = wal_only_value_image(&root);
    let after = root.join("after");
    let (clean, faulted) = open_with_fault(&root, &db, "standalone", x, "oplog.probe", 1, &after);
    assert_eq!(clean, "OK Some(2)", "前提: 故障が無ければ往復して WAL にだけある値を見る");
    assert!(faulted.starts_with("ERR ") && faulted.contains("#989"), "聞けないのに単独で開いた: {faulted}");
    let (again, _) = run_probe_child("standalone", &after, x, &[]);
    assert_eq!(again, "OK Some(2)", "断った後に開き直せない / 値を失った");
    let _ = std::fs::remove_dir_all(&root);
}

/// #989 (d): 往復の後の見直しが聞けなければ断る (「往復の後も WAL に再生する record が残れば断る」 を確かめられない)。 前は素通りした。
/// 断った後に開き直せば開ける。
#[test]
fn a_probe_failure_after_the_standalone_round_trip_refuses_to_open() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("probe_fault_after");
    let (db, x) = wal_only_value_image(&root);
    let after = root.join("after");
    let (clean, faulted) = open_with_fault(&root, &db, "standalone", x, "oplog.probe", 2, &after);
    assert_eq!(clean, "OK Some(2)");
    assert!(faulted.starts_with("ERR ") && faulted.contains("#989"), "往復の後の見直しを聞けないのに開いた: {faulted}");
    let (again, _) = run_probe_child("standalone", &after, x, &[]);
    assert_eq!(again, "OK Some(2)", "断った後に開き直せない / 値を失った");
    let _ = std::fs::remove_dir_all(&root);
}

/// #989 (b) / 読み直し 2 の [2]: WAL 付きの open は、 突き合わせの前 (writer lock の下) に WAL を開き、 開いた WAL に再生する record があるかを
/// 聞く (file を開き直して聞かない)。 その WAL を開けなければ、 何も直さずに返り、 開き直した open が突き合わせ・再生・仕上げをする
/// (`kept_columns_image`: 再生で生き返った行の書き出しの返った列 `t.a` が残る = #885 読み直し 1 の像)。
#[test]
fn a_wal_open_failure_before_the_reconcile_leaves_it_to_the_next_open() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("wal_open_fault_reconcile");
    let (db, r) = kept_columns_image(&root);
    let after = root.join("after");
    let (clean, points) = count_points(&root, &db, "keep", r);
    assert_eq!(clean, "OK true Some(5) Some(\"kept\")", "前提: 故障が無ければ再生が r を生かし直して列も残る");
    assert!(points_named(&points, "oplog.probe").is_empty(), "WAL 付きの open が WAL の file を開き直して聞いた: {points:?}");
    let faulted = fault_at(&db, "keep", r, &points, "oplog.open", 1, &after);
    assert!(faulted.starts_with("ERR "), "WAL を開けないのに開いた: {faulted}");
    let (again, _) = run_probe_child("keep", &after, r, &[]);
    assert_eq!(again, clean, "WAL を開けなかった後の開き直しが、 故障の無い open と違う");
    let _ = std::fs::remove_dir_all(&root);
}

/// #989 (c) / 読み直し 2 の [2]: WAL 付きの open の `prev.seg` の記録の戻しも、 開いた WAL に聞く。 WAL を開けなければ何も戻さずに返り、
/// 開き直した open が戻すかを再生の後に決める (`revived_former_resident_image`: 再生が生き返らせた X を戻さない = review946d [N1])。
#[test]
fn a_wal_open_failure_before_the_prev_restore_leaves_it_to_the_next_open() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("wal_open_fault_prev");
    let (db, x, _) = revived_former_resident_image(&root);
    let after = root.join("after");
    let (clean, points) = count_points(&root, &db, "prev", x);
    assert_eq!(clean, "OK true 0 0", "前提: 故障が無ければ再生が X を生かし、 記録から戻さない");
    assert!(points_named(&points, "oplog.probe").is_empty(), "WAL 付きの open が WAL の file を開き直して聞いた: {points:?}");
    let faulted = fault_at(&db, "prev", x, &points, "oplog.open", 1, &after);
    assert!(faulted.starts_with("ERR "), "WAL を開けないのに開いた: {faulted}");
    let (again, _) = run_probe_child("prev", &after, x, &[]);
    assert_eq!(again, clean, "WAL を開けなかった後の開き直しが、 故障の無い open と違う");
    let _ = std::fs::remove_dir_all(&root);
}

/// 開く時の突き合わせが削除を完了させる v15 の像 (sync しない): gen.seg は行 c の削除の後、 生きている印 (`entities.seg`) と列 (`himo/`) は
/// 削除の前 (c の場所に note = 3 の cell が残る)。 `replay` なら WAL に再生する record (行 b の note = 9、 削除とは関係の無い行) を 1 つ
/// 残す (無ければ WAL に再生する record は無い = 単独の open は往復しない)。 故障が無ければ、 開く時の突き合わせが削除を完了させて (1, 0)、
/// 残った cell を落とす (note = 3 を引いても 0 件)。 戻り値は像の DB。
fn reconcile_image(root: &Path, replay: bool) -> PathBuf {
    reconcile_image_with(root, replay, false)
}

/// [`reconcile_image`] の、 `sync` なら sync する DB (版数の列と sync の表がある) の形。 sync する DB はきれいに閉じていないので、 単独の open は
/// 開いた後に #451 の往復をする。
fn reconcile_image_with(root: &Path, replay: bool, sync: bool) -> PathBuf {
    let (made, before, after, img) = (root.join("made"), root.join("before"), root.join("after"), root.join("img"));
    {
        let mut eng = if sync {
            Engine::create_with_cell_version(made.to_str().unwrap(), 65_536).unwrap()
        } else {
            Engine::create_with_capacity(made.to_str().unwrap(), 65_536).unwrap()
        };
        eng.define_table("notes", 2).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.define_table("full", 1).unwrap();
        eng.define_himo_in("full", "x", ValueType::Number, 0).unwrap();
        if sync {
            eng.enable_sync_tables().unwrap();
        }
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1u32);
        let b = eng.entity_in("notes").unwrap();
        eng.tie_to(b, "notes.note", 2u32);
        eng.delete(a);
        let c = eng.entity_in("notes").unwrap();
        assert_eq!(eng.eid_slot(c), eng.eid_slot(a), "前提: v15 に移って使い回す");
        eng.tie_to(c, "notes.note", 3u32);
        eng.oplog_sync().unwrap();
        let hold = eng.hold_consumer();
        copy_tree(&made, &before);
        eng.delete(c);
        eng.persist_tables().unwrap(); // #870 の止めを外して checkpoint を削除の先へ
        eng.oplog_sync().unwrap();
        if replay {
            eng.tie_to(b, "notes.note", 9u32);
            eng.oplog_commit();
        }
        copy_tree(&made, &after);
        drop(hold);
    }
    copy_tree(&after, &img);
    std::fs::copy(before.join("entities.seg"), img.join("entities.seg")).unwrap();
    std::fs::remove_dir_all(img.join("himo")).unwrap();
    copy_tree(&before.join("himo"), &img.join("himo"));
    img
}

/// 読み直し 2 の [1]: 単独の open は、 開く時の突き合わせが何かを直す前 (writer lock を取った直後) に WAL に再生する record があるかを聞く。
/// 聞けなければ何も書かずに断り、 開き直した open が突き合わせから全部をやる。 #986 の最初の直しは突き合わせが直しを map に書いた後に
/// 聞いて断ったので、 開き直した open は直した場所を見つけず、 残った cell を落とす仕上げが二度と走らなかった (消えた c が note = 3 で
/// 引けた)。 単独の open が聞く全部の点で確かめる (WAL に再生する record は無い = 往復しない)。
#[test]
fn every_probe_failure_in_a_standalone_open_leaves_the_reconcile_whole() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("probe_fault_standalone_gen");
    let img = reconcile_image(&root, false);
    let (clean, points) = count_points(&root, &img, "standalone_gen", 3);
    assert_eq!(clean, "OK 0 (1, 0)", "前提: 故障が無ければ単独の open の突き合わせが削除を完了させ、 残った cell を落とす");
    let probes = points_named(&points, "oplog.probe").len();
    assert!(probes >= 1, "前提: 単独の open が WAL に聞く: {points:?}");
    let probed = root.join("probed");
    for nth in 1..=probes {
        let faulted = fault_at(&img, "standalone_gen", 3, &points, "oplog.probe", nth, &probed);
        assert!(faulted.starts_with("ERR ") && faulted.contains("#989"), "{nth} 番目の聞く点: 聞けないのに単独で開いた: {faulted}");
        let (again, _) = run_probe_child("standalone_gen", &probed, 3, &[]);
        assert_eq!(again, clean, "{nth} 番目の聞く点: 断った後の開き直しが、 突き合わせの仕上げをしない (消えた行が引ける)");
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// 読み直し 3 の [1]: 単独の open は、 #451 の往復が要るか (WAL の file があるか) も、 writer lock の下で開く時の直しが何かを書く前に聞いた
/// stat の答えで決める。 単独の open 自身の stat (往復の WAL 付きの open が WAL を開く前の stat より前の点) のどれに故障を起こしても、 何も
/// 書かずに断る (開く時の突き合わせが直す gen.seg / entities.seg が像のまま)。 開き直せば故障の無い open と同じ。 v15 の突き合わせのある
/// sync する DB の像 (きれいに閉じていない = 開いた後に往復する、 WAL に再生する record は無い)。
///
/// 読み直し 2 の直しは往復の要否を本体を開いた後にもう一度 stat で聞き、 その stat の失敗で、 突き合わせが直し (`kill_generation` /
/// `live_free`、 仕上げの cell 落とし) を map に書いた後に断った (開き直した突き合わせは直した場所を見つけない)。
#[test]
fn a_stat_failure_in_a_standalone_open_refuses_before_any_repair() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("stat_fault_standalone_relog");
    let img = reconcile_image_with(&root, false, true);
    let (clean, points) = count_points(&root, &img, "standalone_gen", 3);
    assert!(clean.starts_with("OK 0 "), "前提: 故障が無ければ開けて、 消えた c が引けない: {clean}");
    let repaired = ["gen.seg", "entities.seg"];
    let read = |db: &Path, f: &str| std::fs::read(db.join(f)).unwrap();
    for f in repaired {
        assert!(read(&root.join("counted"), f) != read(&img, f), "前提: 単独の open の突き合わせが {f} を直す");
    }
    // 往復の WAL 付きの open は WAL の file を stat してから開く。 その stat より前の stat が単独の open 自身の物
    let open = points.iter().position(|(_, s)| s == "oplog.open");
    let Some(open) = open.filter(|&i| i > 0 && points[i - 1].1 == "oplog.stat") else {
        panic!("前提: 単独の open が #451 の往復で WAL を開く: {points:?}")
    };
    let own = points_named(&points, "oplog.stat").into_iter().filter(|&n| n < points[open - 1].0).count();
    assert!(own >= 1, "前提: 単独の open が WAL の file を stat する: {points:?}");
    let after = root.join("after_stat_fault");
    for nth in 1..=own {
        let faulted = fault_at(&img, "standalone_gen", 3, &points, "oplog.stat", nth, &after);
        assert!(faulted.starts_with("ERR "), "{nth} 番目の stat: 故障したのに開いた: {faulted}");
        for f in repaired {
            assert!(read(&after, f) == read(&img, f), "{nth} 番目の stat: 開く時の直し ({f}) を書いた後に断った: {faulted}");
        }
        let (again, _) = run_probe_child("standalone_gen", &after, 3, &[]);
        assert_eq!(again, clean, "{nth} 番目の stat: 断った後の開き直しが、 故障の無い open と違う");
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// 読み直し 2 の [2]: WAL 付きの open が WAL を開けずに返っても、 開き直した open が突き合わせから全部をやる (WAL を開くのは突き合わせの前)。
/// 前は突き合わせが直しを map に書き、 仕上げを再生の後に回した後に WAL を開けずに返り、 engine の Drop が clean の印を立てた: 開き直した
/// open は突き合わせをせず (きれいに閉じたと読む)、 消えた c が note = 3 で引けた。 WAL に再生する record がある像。
#[test]
fn a_wal_open_failure_leaves_the_reconcile_whole() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("wal_open_fault_gen");
    let img = reconcile_image(&root, true);
    let after = root.join("after_fault");
    let (clean, faulted) = open_with_fault(&root, &img, "writer_gen", 3, "oplog.open", 1, &after);
    assert_eq!(clean, "OK 0 (1, 0)", "前提: 故障が無ければ突き合わせが削除を完了させ、 再生の後に残った cell を落とす");
    assert!(faulted.starts_with("ERR "), "WAL を開けないのに開いた: {faulted}");
    let (again, _) = run_probe_child("writer_gen", &after, 3, &[]);
    assert_eq!(again, clean, "WAL を開けなかった後の開き直しが、 突き合わせの仕上げをしない (消えた行が引ける)");
    let _ = std::fs::remove_dir_all(&root);
}

/// 読み直し 2 の [3]: WAL 付きの open は、 WAL の file があるかの stat が失敗した時に 「無い」 と読まない。 前は `Path::exists` が stat の
/// 失敗を false にし、 `OpLog::create` が WAL を作り直して (truncate) 再生していない record を捨てた (WAL にだけある `t.a` = 2 を失って 1)。
#[test]
fn a_wal_stat_failure_does_not_recreate_the_wal() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("wal_stat_fault_writer");
    let (db, x) = wal_only_value_image(&root);
    let after = root.join("after");
    let (clean, faulted) = open_with_fault(&root, &db, "writer", x, "oplog.stat", 1, &after);
    assert_eq!(clean, "OK Some(2)", "前提: 故障が無ければ再生して WAL にだけある値を見る");
    assert!(faulted.starts_with("ERR "), "WAL の stat が失敗したのに開いた: {faulted}");
    let (again, _) = run_probe_child("writer", &after, x, &[]);
    assert_eq!(again, "OK Some(2)", "WAL の stat が失敗した後に WAL にだけある値を失った (WAL を作り直した)");
    let _ = std::fs::remove_dir_all(&root);
}

/// 読み直し 2 の [3]: 単独の open も、 WAL の file があるかの stat の失敗を 「WAL 無し」 と読まない (#989 と同じく断る)。 前は WAL を見ずに
/// 開いた (`t.a` = 1)。
#[test]
fn a_wal_stat_failure_refuses_a_standalone_open() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("wal_stat_fault_standalone");
    let (db, x) = wal_only_value_image(&root);
    let after = root.join("after");
    let (clean, faulted) = open_with_fault(&root, &db, "standalone", x, "oplog.stat", 1, &after);
    assert_eq!(clean, "OK Some(2)");
    assert!(faulted.starts_with("ERR ") && faulted.contains("#989"), "WAL の stat が失敗したのに単独で開いた: {faulted}");
    let (again, _) = run_probe_child("standalone", &after, x, &[]);
    assert_eq!(again, "OK Some(2)", "断った後に開き直せない / 値を失った");
    let _ = std::fs::remove_dir_all(&root);
}

/// 読み直し 2 の [3]: `concurrentize_with_oplog` も、 WAL の file があるかの stat の失敗で WAL を作り直さない (Err を返し、 WAL の file は
/// 1 byte も変わらない)。 像はきれいに閉じた WAL 付きの DB (単独で開いてから渡す = 利用者の build → runtime の形)。
#[test]
fn a_wal_stat_failure_does_not_recreate_the_wal_in_concurrentize() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("wal_stat_fault_concurrentize");
    let db = root.join("db");
    let x = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 16).unwrap();
        eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        let x = eng.entity_in("t").unwrap();
        eng.tie_to(x, "t.a", 4u32);
        eng.oplog_sync().unwrap();
        x
    };
    let (clean, points) = count_points(&root, &db, "concurrentize", x);
    assert_eq!(clean, "OK Some(4)");
    let (stats, open) = (points_named(&points, "oplog.stat"), points_named(&points, "oplog.open"));
    // 単独の open は往復しない (きれいに閉じた)。 WAL を開くのは concurrentize_with_oplog の 1 回で、 その前の stat が 2 つ目
    // (1 つ目は単独の open が lock の下で聞く所)
    assert_eq!(open.len(), 1, "前提: 単独の open は往復しない: {points:?}");
    assert!(stats.len() >= 2 && stats[1] < open[0], "前提: 単独の open と concurrentize_with_oplog が WAL の file を見る: {points:?}");
    let after = root.join("after");
    let faulted = fault_at(&db, "concurrentize", x, &points, "oplog.stat", 2, &after);
    assert!(faulted.starts_with("ERR "), "WAL の stat が失敗したのに WAL 付きにした: {faulted}");
    assert_eq!(
        std::fs::read(after.join("oplog")).unwrap(),
        std::fs::read(db.join("oplog")).unwrap(),
        "WAL の stat が失敗した concurrentize_with_oplog が WAL を書き換えた (作り直した)"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// review946d [N1] の後に回した戻しも、 再生の前に戻していた時と同じく前の住人の別名を外す: X (名前 7) に B の行 b を畳み (b の世界
/// 番号は X の場所の別名)、 X を消して書き出した (本体に X の死) 後、 Y で X の場所を使い回して `oplog_sync` し (WAL に Y の record)、
/// `prev.seg` を書き出した後・生きている印の前に電源が落ちる。 開く時は場所の最後の行が死んだ X なので戻すかを再生の後に回し、 再生が
/// Y を生かす所で後始末をやり直す (`redo_reuse_eviction`、 X の削除を退避する)。 その後に戻す時、 退避が書き出し済みかを再生の前の
/// 退避表で決めるので、 X の別名 b を外して X の削除で消えた identity にする = b は Y に写らない。
///
/// 再生の後の退避表で決めると、 やり直しが退避した X の削除を 「書き出し済み」 と読んで別名を外さず、 b が Y の別名に残った。
#[test]
fn a_deferred_restore_drops_the_aliases_of_the_former_resident() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_deferred_alias");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_b = enchudb_oplog::make_eid(3, 9);
    let (x, y) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let hk = eng.himo_id("t.k").unwrap() as u16;
        let b = eng.resolve_remote_eid(world_b, hk).unwrap();
        assert!(eng.remote_tie_apply_result(b, hk, 8u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
        eng.merge_entity_into(b, x).unwrap();
        assert_eq!(
            eng.resolve_remote_eid_existing(world_b).map(|e| eng.eid_slot(e)),
            Some(eng.eid_slot(x)),
            "前提: b は X の場所の別名"
        );
        // 畳んで空いた b の場所を埋める (X の場所を使い回させる)
        let z = eng.entity_in("t").unwrap();
        eng.tie_to(z, "t.k", 2u32);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        eng.persist_sync_state().unwrap();
        let _hold = eng.hold_consumer();
        eng.delete(x);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        let y = add_seven(&eng, 77);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の後で撮った");
        (x, y)
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let y_alive = eng.is_live(y) && eng.get(y, "t.v") == Some(77);
    let x_alive = eng.is_live(x);
    let restored = eng.prev_residents_restored();
    let b_now = eng.resolve_remote_eid_existing(world_b);
    let deletes: Vec<u64> = eng
        .state_records()
        .0
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some(eid),
            _ => None,
        })
        .collect();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(y_alive && !x_alive, "前提: 再生が Y を生かした (X は消えている)");
    assert_eq!(restored, 1, "前提: 記録から X の削除を戻した");
    assert_eq!(b_now, None, "消えた X の別名 b が使い回した Y に写った: {b_now:x?}");
    assert!(deletes.contains(&world_b), "state に X の別名 b の Delete が無い: {deletes:x?}");
}

/// review946d [N2]: X を消した書き出し (WAL の書き出しの後に消した = X の Delete は WAL に届かず、 本体には生きている印・世代・tomb.seg
/// の X の削除の版数が届く) の後、 Y で X の場所を使い回し、 WAL を書き出さない本体の書き出し (`body_msync`、 書き手が自分で呼ぶ #414 /
/// #708 の形) が `prev.seg` を書き出した後・生きている印の前に電源が落ちる。 開く時は記録から X の削除を戻し (場所の最後の行は死んだ
/// X)、 #451 の作り直しは tomb.seg のなめでも同じ (版数, X) を見る。 作り直して配る X の Delete は 1 つ。
///
/// 前は同じ Delete を 2 つ WAL に載せて配った。
#[test]
fn a_restored_own_delete_also_in_the_tombstones_is_relogged_once() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_restored_relog_once");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let _hold = eng.hold_consumer();
        // WAL に書き出す物を X でない行に置く (書き出しが走る。 落ちた後の再生は X を生かさない)
        let keep = eng.pull_raw("t.k", 1u32);
        assert_eq!(keep.len(), 1, "前提: 名前 1 の行");
        eng.tie_to(keep[0], "t.v", 5u32);
        eng.oplog_commit();
        let deleted: Arc<std::sync::Mutex<Option<Hlc>>> = Arc::new(std::sync::Mutex::new(None));
        let (e2, d2) = (eng.clone(), deleted.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && d2.lock().unwrap().is_none() {
                e2.delete(x);
                e2.oplog_commit();
                *d2.lock().unwrap() = Some(e2.tombstone_hlc(x));
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        let tomb = deleted.lock().unwrap().expect("前提: 書き出しの途中で X を消した");
        let y = add_seven(&eng, 77);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.body_msync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の後で撮った");
        (x, tomb)
    };
    crashsim::stop();
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let alive = eng.is_live(x);
    let restored = eng.prev_residents_restored();
    let bridged: Vec<Hlc> = bridged_deletes(&eng).into_iter().filter(|&(e, _)| e == x).map(|(_, h)| h).collect();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(!alive, "前提: 像の X は消えている");
    assert_eq!(restored, 1, "前提: 記録から X の削除を戻した");
    assert!(!bridged.is_empty(), "前提: WAL に無い X の Delete を作り直して配った");
    assert_eq!(bridged, vec![tomb], "作り直した X の Delete が 1 つでない");
}

/// review946 [1] の続き: X を消して Y で使い回してから `oplog_sync` し (WAL に X の Delete と Y が届く)、 `prev.seg` を書き出した後・
/// 生きている印の前に電源が落ちる (本体は X の削除の前、 記録あり)。 開く時は記録を戻さず (X は本体で生きている)、 再生が X の Delete の
/// 後に Y を生かす所で使い回しの後始末をやり直す = Y は生き、 state に X の Delete がある。
#[test]
fn a_reuse_with_its_delete_in_the_wal_is_finished_by_the_replay() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_delete_in_wal");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb, y) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let _hold = eng.hold_consumer();
        eng.delete(x);
        eng.oplog_commit();
        let tomb = eng.tombstone_hlc(x);
        let y = add_seven(&eng, 77);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の後で撮った");
        (x, tomb, y)
    };
    crashsim::stop();
    let (deletes, v, _) = open_and_read_state(&img);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(v, vec![77], "前提: 再生が Y を生かした");
    assert!(deletes.contains(&(x, tomb)), "state に X の Delete が無い: {deletes:x?}");
    assert!(!deletes.iter().any(|&(e, _)| e == y), "生きている Y の Delete を出した: {deletes:x?}");
}

/// review946 3 (ii): X の削除を書き出した (checkpoint は X の Delete の先) 後、 Y で場所を使い回して `oplog_sync` し、 WAL の書き出しの
/// 後・`prev.seg` の前に電源が落ちる (使い回しは WAL にだけ届き、 記録は届かない)。 再生が Y を生かす所で、 本体の場所の削除 (X の
/// 削除の版数) を前の住人 X (世代のすぐ前の行) に移す = Y は生き、 state に X の Delete がある。
///
/// 前は記録が無いと何もせず、 X の削除が生きている Y の場所に残り、 state に X の Delete が無かった (#914 の形)。
#[test]
fn a_reuse_only_in_the_wal_is_finished_by_the_replay() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_reuse_wal_only");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb, y) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let (tomb, _hold) = delete_and_checkpoint(&eng, x);
        let y = add_seven(&eng, 77);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の前で撮った");
        (x, tomb, y)
    };
    crashsim::stop();
    let (deletes, v, restored) = open_and_read_state(&img);
    // 再生のやり直しは場所の版数も落とす (生きている Y の場所に X の削除の版数を残すと、 後で Y の cell が全部外れた時に、 きれいに
    // 閉じなかった open の sweep が Y を消す、 review946 [11])
    let left = {
        let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        eng.tombstone_hlc(y)
    };
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(v, vec![77], "前提: 再生が Y を生かした");
    assert_eq!(restored, 0, "前提: 記録は届いていない");
    assert!(deletes.contains(&(x, tomb)), "state に X の Delete が無い: {deletes:x?}");
    assert!(!deletes.iter().any(|&(e, _)| e == y), "生きている Y の Delete を出した: {deletes:x?}");
    assert_eq!(left, Hlc::ZERO, "生きている Y の場所に前の住人 X の削除の版数が残った");
}

/// review946 3 (ii) の翻訳した行: 受けた行 F (C の削除も受けて書き出した) の場所を Y で使い回して `oplog_sync` し、 WAL の書き出しの
/// 後・`prev.seg` の前に電源が落ちる。 写像は戻る (記録が無く、 本体の場所は死んでいる) が、 再生が Y を生かす所で外す = Y は A の行、
/// F は (C, f) の削除で消えたまま。
#[test]
fn a_translated_reuse_only_in_the_wal_is_finished_by_the_replay() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_translated_wal_only");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_f = enchudb_oplog::make_eid(3, 7);
    {
        let (eng, _) = received_and_deleted_row_engine(&live.join("db"), world_f);
        let _hold = eng.hold_consumer();
        let y = eng.entity_in("t").unwrap();
        eng.tie_to(y, "t.c", 9u32);
        eng.oplog_commit();
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の前で撮った");
    }
    crashsim::stop();
    let (nine, f_deleted, _, mapped) = translated_image_state(&img, world_f);
    let _ = std::fs::remove_dir_all(&root);
    assert!(
        nine.len() == 1 && enchudb_oplog::eid_peer(nine[0]) == 1,
        "再生が生かした Y が A の行として state に出ない (C の行 F として出た?): {nine:x?}"
    );
    assert!(f_deleted, "消えた F に C の遅い書き込みが当たった (F の削除を失った)");
    assert_eq!(mapped, None, "Y を生かした後も F の写像が場所を指す");
}

/// review946 3 (ii) の守り: 受けた行 F を A が消して (A の時計の削除) 書き出した後、 F の場所を Y で使い回し、 公開の `persist_tables()`
/// (本体を書き出さない) で外した写像と F の退避 ((C, f) の削除の版数) だけが `.eidmap` に届き、 WAL の書き出しの後・`prev.seg` の前に
/// 電源が落ちる (記録も写像も無く、 本体の場所は F の削除の版数で死んでいる)。 再生が Y を生かす所で、 退避表に同じ版数の identity
/// ((C, f)) があるので前の住人を自分の行と読まない = A の行の Delete (幻、 A の時計なので state に出る) を出さない。
#[test]
fn a_translated_reuse_whose_eidmap_reached_disk_first_is_not_read_as_an_own_row() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_translated_eidmap_first");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_f = enchudb_oplog::make_eid(3, 7);
    {
        let db = live.join("db");
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 1).unwrap();
        eng.define_himo_in("t", "c", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let hid = eng.himo_id("t.c").unwrap() as u16;
        let t = eng.resolve_remote_eid(world_f, hid).unwrap();
        assert!(eng.remote_tie_apply_result(t, hid, 5u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
        let f = eng.pull_raw("t.c", 5u32);
        assert_eq!(f.len(), 1, "前提: A に F がある");
        let (_, _hold) = delete_and_checkpoint(&eng, f[0]);
        let y = eng.entity_in("t").unwrap();
        eng.tie_to(y, "t.c", 9u32);
        eng.oplog_commit();
        assert_eq!(eng.eid_slot(y), eng.eid_slot(f[0]), "前提: Y は F の場所を使い回した");
        eng.persist_tables().unwrap();
        assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: 外した写像と F の退避が `.eidmap` に届いた");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の前で撮った");
    }
    crashsim::stop();
    let (nine, _, own_deletes, mapped) = translated_image_state(&img, world_f);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(mapped, None, "前提: 写像は戻らない");
    assert!(nine.len() == 1 && enchudb_oplog::eid_peer(nine[0]) == 1, "前提: Y は A の行: {nine:x?}");
    assert!(own_deletes.is_empty(), "F の場所の前の住人を A の行と読んで Delete を出した: {own_deletes:x?}");
}

/// #969: X (名前 7) に B の行 b を畳み (b の世界番号は X の場所の別名)、 X を消して書き出した (本体に X の死と tomb.seg の削除の版数) 後、
/// Y で X の場所を使い回して `oplog_sync` し、 WAL の書き出しの後・`prev.seg` の書き出しの前 (`BeforeGate`) に電源が落ちる。 像は
/// 「使い回しは WAL にだけ届き、 記録 (`prev.seg`) も本体の使い回しも無い」。 開く時は再生が Y を生かす所で後始末をやり直す
/// (`redo_reuse_eviction` の記録の無い自分の行の枝、 場所の削除の版数 = X の削除)。 その時、 X の別名 b も外して X の削除で消えた
/// identity にする = b は Y に写らず、 state に b の Delete がある。
///
/// 前は自分の行の枝が別名を外さず、 b が使い回した Y の別名に残った (b 宛ての書き込みが Y に当たる)。
#[test]
fn a_redone_own_reuse_drops_the_aliases_of_the_former_resident() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_redo_alias");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_b = enchudb_oplog::make_eid(3, 9);
    let (x, y, tomb) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let hk = eng.himo_id("t.k").unwrap() as u16;
        let b = eng.resolve_remote_eid(world_b, hk).unwrap();
        assert!(eng.remote_tie_apply_result(b, hk, 8u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
        eng.merge_entity_into(b, x).unwrap();
        assert_eq!(
            eng.resolve_remote_eid_existing(world_b).map(|e| eng.eid_slot(e)),
            Some(eng.eid_slot(x)),
            "前提: b は X の場所の別名"
        );
        // 畳んで空いた b の場所を埋める (X の場所を使い回させる)
        let z = eng.entity_in("t").unwrap();
        eng.tie_to(z, "t.k", 2u32);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        eng.persist_sync_state().unwrap();
        let _hold = eng.hold_consumer();
        eng.delete(x);
        eng.oplog_commit();
        let tomb = eng.tombstone_hlc(x);
        eng.oplog_sync().unwrap();
        let y = add_seven(&eng, 77);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: WAL の書き出しの後・prev.seg の前で撮った");
        (x, y, tomb)
    };
    crashsim::stop();
    let db = img.join("db");
    // 記録が届いていない (再生の前の像。 readonly は WAL を見ない): 場所の記録は X の前の住人の物か無い
    let record = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        ro.prev_resident_of_slot(ro.eid_slot(x))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let y_alive = eng.is_live(y) && eng.get(y, "t.v") == Some(77);
    let x_alive = eng.is_live(x);
    let restored = eng.prev_residents_restored();
    let b_now = eng.resolve_remote_eid_existing(world_b);
    let deletes: Vec<(u64, Hlc)> = eng
        .state_records()
        .0
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some((eid, r.hlc)),
            _ => None,
        })
        .collect();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(record.is_none_or(|r| r.1 != eid_local(x)), "前提: X の場所の使い回しの記録 (前の住人 X) は届いていない: {record:x?}");
    assert!(y_alive && !x_alive, "前提: 再生が Y を生かした (X は消えている)");
    assert_eq!(restored, 0, "前提: 記録から戻していない (後始末は再生のやり直しだけ)");
    assert_eq!(b_now, None, "消えた X の別名 b が使い回した Y に写った: {b_now:x?}");
    assert!(deletes.contains(&(world_b, tomb)), "state に X の別名 b の Delete (X の削除の版数) が無い: {deletes:x?}");
}

/// #968 の像を撮る: X (名前 7、 v = 70) の v を 71 に書き (WAL の checkpoint の後ろに X の Tie)、 本体の書き出しの WAL の書き出しの後、
/// 中身の段の後 (`BeforeGate`) に A のアプリが X を消し、 本体の書き出し (生きている印・世代・列・版数の列・tomb.seg) が返った後・次の WAL の
/// 書き出しの前 (`WrittenOut`) に電源が落ちる。 WAL に X の Delete は無い。 `reuse` = v15 (場所を 1 度使い回した DB) か v14 か。
/// X と削除の版数を返す。
fn body_only_delete_image(live: &Path, img: &Path, reuse: bool) -> (u64, Hlc) {
    let (eng, x) = if reuse {
        full_named_table_engine(&live.join("db"))
    } else {
        let db = live.join("db");
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "k", ValueType::Number, 0).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        let k = eng.himo_id("t.k").unwrap() as u16;
        eng.set_table_pk("t", k).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let x = eng.entity_in("t").unwrap();
        eng.tie_to(x, "t.k", 7u32);
        eng.tie_to(x, "t.v", 70u32);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        eng.persist_sync_state().unwrap();
        (eng, x)
    };
    let _hold = eng.hold_consumer();
    eng.tie_to(x, "t.v", 71u32);
    eng.oplog_commit();
    let deleted: Arc<std::sync::Mutex<Option<Hlc>>> = Arc::new(std::sync::Mutex::new(None));
    let shot = Arc::new(AtomicBool::new(false));
    let (e2, d2, s2, l, i) = (eng.clone(), deleted.clone(), shot.clone(), live.to_path_buf(), img.to_path_buf());
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::BeforeGate && d2.lock().unwrap().is_none() {
            e2.delete(x);
            e2.oplog_commit();
            *d2.lock().unwrap() = Some(e2.tombstone_hlc(x));
        }
        if p == Phase::WrittenOut && !s2.swap(true, Ordering::SeqCst) {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
        }
    })));
    eng.oplog_sync().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: 本体の書き出しの後で撮った");
    let tomb = deleted.lock().unwrap().expect("前提: 書き出しの途中で X を消した");
    assert_eq!(eng.slot_bits() > 0, reuse, "前提: v15 か (場所を使い回した DB)");
    (x, tomb)
}

/// #968: 本体にだけ (tombstone まで) 届いた X の削除を、 WAL の再生が X の古い Tie で生き返らせる像 (`body_only_delete_image`)。 開いた A は
/// 再生の後に削除を完了させる (再生の前に回る開く時の sweep と同じ決め): X は消えていて、 #451 の作り直しが配る X の Delete と state の
/// X の Delete が一致する。 きれいに閉じて開き直しても同じ (2 回目の open の sweep は何もしない)。
///
/// 前は 1 回目の open の X が生きて (名前も値も無く、 tombstone が残る) state に X の Delete が無いのに、 作り直しは X の Delete を配った
/// (差分で受ける peer では X が消え、 state で取り直した peer では消えない)。 2 回目の open の sweep が X を黙って消した。
fn a_body_only_delete_the_replay_revives_is_finished(tag: &str, reuse: bool) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb) = body_only_delete_image(&live, &img, reuse);
    crashsim::stop();
    let db = img.join("db");
    // 再生の前の像 (readonly は WAL を見ない): X は消えていて、 tomb.seg に X の削除の版数がある
    let before = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (ro.is_live(x), ro.tombstone_hlc(x), ro.get(x, "t.k"))
    };
    let deletes_of_x = |eng: &Engine| -> Vec<Hlc> {
        eng.state_records()
            .0
            .iter()
            .filter_map(|r| match r.op {
                DecodedOp::Delete { eid } if eid == x => Some(r.hlc),
                _ => None,
            })
            .collect()
    };
    let first = {
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let out = (
            eng.is_live(x),
            deletes_of_x(&eng),
            bridged_deletes(&eng).into_iter().filter(|&(e, _)| e == x).map(|(_, h)| h).collect::<Vec<_>>(),
            eng.unlogged_writes_relogged(),
        );
        eng.oplog_sync().unwrap();
        out
    };
    let second = {
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        (eng.is_live(x), deletes_of_x(&eng), eng.verify_invariants().violations)
    };
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(before, (false, tomb, None), "前提: 本体に X の削除 (死・tombstone・落とした cell) が届いた");
    let (alive, in_state, bridged, relogged) = first;
    assert!(relogged >= 1, "前提: #451 の作り直しが走った");
    assert!(!alive, "本体に届いた X の削除を、 再生が X の古い Tie で取り消したまま (名前の無い行が生きる)");
    assert_eq!(in_state, vec![tomb], "state の X の Delete が配る物と違う");
    assert_eq!(bridged, vec![tomb], "作り直して配る X の Delete が 1 つでない");
    assert_eq!(second, (false, vec![tomb], Vec::<String>::new()), "開き直すと答えが変わった");
}

#[test]
fn a_body_only_delete_the_replay_revives_is_finished_v15() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_body_only_delete_the_replay_revives_is_finished("body_only_delete_v15", true);
}

#[test]
fn a_body_only_delete_the_replay_revives_is_finished_v14() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_body_only_delete_the_replay_revives_is_finished("body_only_delete_v14", false);
}

/// 場所を 1 度使い回した X (前の住人 W の退避と記録は書き出し済み) に B の行 b を畳み (b の世界番号 (3, 9) は X の場所の別名)、 #968 の
/// 像 (本体にだけ X の削除、 WAL に X の古い Tie) を撮る。 開く時、 再生は突き合わせが世代を上げた場所を X の Tie で生かす所 ([7]) で、
/// 記録 (前の住人 W) の後始末をやり直す道を通る。 `forget` = 像を撮る前に W の削除を忘れてよい線 (`forget_deletes_before`) で忘れて
/// sidecar に書き出す (退避表に W は無く、 場所の記録 (前の住人 W、 書き出した周期) は残る。 読み直し [A1])。 (X, X の削除の版数) を返す。
fn written_eviction_image(live: &Path, img: &Path, world_b: u64, forget: bool) -> (u64, Hlc) {
    let (eng, x) = full_named_table_engine(&live.join("db"));
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let b = eng.resolve_remote_eid(world_b, hk).unwrap();
    assert!(eng.remote_tie_apply_result(b, hk, 8u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
    eng.merge_entity_into(b, x).unwrap();
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    if forget {
        let w_tomb = eng.prev_resident_of_slot(eng.eid_slot(x)).expect("前提: X の場所の記録 (前の住人 W)").2;
        assert!(eng.forget_deletes_before(Hlc { wall: w_tomb.wall + 1, logical: 0, peer: 0 }) >= 1, "前提: W の削除を忘れた");
        eng.persist_sync_state().unwrap();
        assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: W を忘れた退避表が `.eidmap` に届いた");
    }
    let _hold = eng.hold_consumer();
    eng.tie_to(x, "t.v", 71u32);
    eng.oplog_commit();
    let deleted: Arc<std::sync::Mutex<Option<Hlc>>> = Arc::new(std::sync::Mutex::new(None));
    let shot = Arc::new(AtomicBool::new(false));
    let (e2, d2, s2, l, i) = (eng.clone(), deleted.clone(), shot.clone(), live.to_path_buf(), img.to_path_buf());
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::BeforeGate && d2.lock().unwrap().is_none() {
            e2.delete(x);
            e2.oplog_commit();
            *d2.lock().unwrap() = Some(e2.tombstone_hlc(x));
        }
        if p == Phase::WrittenOut && !s2.swap(true, Ordering::SeqCst) {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
        }
    })));
    eng.oplog_sync().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: 本体の書き出しの後で撮った");
    let tomb = deleted.lock().unwrap().expect("前提: 書き出しの途中で X を消した");
    (x, tomb)
}

/// #969 の守り: 再生のやり直しが前の住人の別名を外すのは、 前の住人の後始末が書き出されていない時だけ (`written_eviction_image`)。 W の
/// 退避は書き出し済みなので、 場所の別名 b (X の物) は外さない = b は X の場所を指したまま、 W の削除の版数で消えた identity にならない。
#[test]
fn a_redo_for_a_written_eviction_keeps_the_aliases_of_the_slot() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_redo_written_alias");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_b = enchudb_oplog::make_eid(3, 9);
    let (x, tomb) = written_eviction_image(&live, &img, world_b, false);
    crashsim::stop();
    let db = img.join("db");
    // 場所の記録は前の住人 W (自分の行、 書き出し済みの周期)
    let record = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        ro.prev_resident_of_slot(ro.eid_slot(x))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let slot = eng.eid_slot(x);
    let b_now = eng.resolve_remote_eid_existing(world_b);
    let b_deletes: Vec<Hlc> = eng
        .state_records()
        .0
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } if eid == world_b => Some(r.hlc),
            _ => None,
        })
        .collect();
    let alive = eng.is_live(x);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    let w_tomb = match record {
        Some((1, w, t, true)) if w != eid_local(x) => t,
        r => panic!("前提: 場所の記録は前の住人 W (自分の行): {r:x?}"),
    };
    assert!(!alive, "前提: X は消えている (#968)");
    assert_eq!(b_now, Some(enchudb_oplog::make_eid(1, slot)), "書き出し済みの W の後始末のやり直しが、 X の別名 b を外した");
    assert!(!b_deletes.contains(&w_tomb), "X の別名 b を W の削除の版数で消えた identity にした: {b_deletes:x?} (X の削除 {tomb:x?})");
}

/// directory `from` を `to` に丸ごと写す (kill の像: page cache の中身 = 書き出していない mmap の書き込みも届く)。
///
/// 写す間に消えた file は像に無いだけ (その file を消す / rename する前に kill した像と同じ)。 開いている engine の背景の名前の記録の
/// まとめ直し (#959) が一時 file `pkformer.compact.tmp` を置いて rename するので、 開いた engine の kill の像を撮る試験で起きる。
///
/// PR #1006 の 3 回目の読み直し [4]: file を 1 つずつ順に写すので、 写す間にまとめ直しが `pkformer` と log を置き換えると、 古い
/// `pkformer` と新しい log (header だけ) の組になる (本物の kill ではありえない像。 header が合わず log が読まれない)。 写す前と後で
/// 一覧 (path・inode・mmap でない file の長さ) を比べ、 違えば撮り直す。
fn kill_image(from: &Path, to: &Path) {
    for _ in 0..500 {
        let before = kill_image_state(from);
        let _ = std::fs::remove_dir_all(to);
        kill_image_copy(from, to);
        if kill_image_state(from) == before {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    panic!("kill の像: 写す間に {} の file が置き換わり続けた", from.display());
}

/// [`kill_image`] の 1 回の写し。
fn kill_image_copy(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_dir() {
            kill_image_copy(&entry.path(), &dest);
        } else if let Err(e) = std::fs::copy(entry.path(), &dest) {
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound, "{}: {e}", entry.path().display());
            let _ = std::fs::remove_file(&dest);
        }
    }
}

/// [`kill_image`] の比べる一覧: (path, inode, 長さ)。 mmap で書く file (segment と WAL) の長さは書き手が動かすので比べない
/// (置き換えは inode で分かる)。
fn kill_image_state(dir: &Path) -> Vec<(std::path::PathBuf, u64, Option<u64>)> {
    use std::os::unix::fs::MetadataExt;
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else { continue };
        for entry in rd.flatten() {
            let p = entry.path();
            let Ok(md) = std::fs::symlink_metadata(&p) else { continue };
            if md.is_dir() {
                stack.push(p);
                continue;
            }
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let mapped = name.ends_with(".seg") || name == "oplog";
            out.push((p.strip_prefix(dir).unwrap().to_path_buf(), md.ino(), (!mapped).then_some(md.len())));
        }
    }
    out.sort();
    out
}

/// 読み直し [A1] (#969 / #968 の PR): `a_redo_for_a_written_eviction_keeps_the_aliases_of_the_slot` の像を撮る前に、 W の削除を忘れてよい
/// 線で忘れて sidecar に書き出す (退避表に W は無く、 場所の記録 (前の住人 W) は書き出した周期のまま残る)。 開く時の後始末のやり直しは、
/// 記録の周期が書き出し済みなのでやり直さない: X の別名 b を外さず、 忘れた W の削除も退避表に戻さない (state に W の削除の版数の Delete
/// が無い)。 記録も今の周期に書き直さない: 開く道の本体の書き出しの後 (再生とやり直しの後、 consumer が sidecar を書く前) の kill の像を
/// もう一度開いても、 記録から戻さず (戻した数 0)、 b は X の場所を指したまま。
///
/// 前は、 退避表に W が無いのを 「退避が書き出されていない」 と読んで b を外し、 b を W の削除の版数で消えた identity にした (W も退避表に
/// 戻した)。 別名の判定だけ直して記録を今の周期に書き直すと、 2 回目に落ちた像の open が書き出していない記録として戻し、 同じく b を外した。
#[test]
fn a_redo_for_a_forgotten_eviction_keeps_the_aliases_of_the_slot() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_redo_forgotten_alias");
    let (live, img, again) = (root.join("live"), root.join("img"), root.join("again"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_b = enchudb_oplog::make_eid(3, 9);
    let (x, tomb) = written_eviction_image(&live, &img, world_b, true);
    crashsim::stop();
    let db = img.join("db");
    let record = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        ro.prev_resident_of_slot(ro.eid_slot(x))
    };
    let observe = |eng: &Engine| {
        let deletes: Vec<(u64, Hlc)> = eng
            .state_records()
            .0
            .iter()
            .filter_map(|r| match r.op {
                DecodedOp::Delete { eid } => Some((eid, r.hlc)),
                _ => None,
            })
            .collect();
        (eng.resolve_remote_eid_existing(world_b), deletes, eng.is_live(x), eng.prev_residents_restored())
    };
    // 開く道の最後の本体の書き出し (開く thread の上、 consumer を起こす前) の後で kill の像を撮る
    let opener = std::thread::current().id();
    let shots = Arc::new(AtomicU32::new(0));
    {
        let (n, from, to) = (shots.clone(), db.clone(), again.join("db"));
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::WrittenOut && std::thread::current().id() == opener {
                let _ = std::fs::remove_dir_all(&to);
                kill_image(&from, &to);
                n.fetch_add(1, Ordering::SeqCst);
            }
        })));
    }
    let (slot, first) = {
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        write_out_hook::set(None);
        eng.set_peer_id(1);
        (eng.eid_slot(x), observe(&eng))
    };
    let second = {
        let eng = Engine::open_concurrent_with_oplog(again.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        observe(&eng)
    };
    let _ = std::fs::remove_dir_all(&root);
    let w_tomb = match record {
        Some((1, w, t, true)) if w != eid_local(x) => t,
        r => panic!("前提: 場所の記録は前の住人 W (自分の行): {r:x?}"),
    };
    assert!(shots.load(Ordering::SeqCst) >= 1, "前提: 開く道の本体の書き出しの後で kill の像を撮った");
    let b_at_x = Some(enchudb_oplog::make_eid(1, slot));
    for (when, (b_now, deletes, alive, restored)) in [("開いた時", first), ("開いた後にもう一度落ちた像", second)] {
        assert!(!alive, "{when}: 前提: X は消えている (#968)");
        assert_eq!(b_now, b_at_x, "{when}: 忘れた W の後始末のやり直しが、 X の別名 b を外した");
        assert!(
            !deletes.iter().any(|&(_, h)| h == w_tomb),
            "{when}: 忘れた W の削除の版数の Delete がある (b を消えた identity にした / W を退避表に戻した): {deletes:x?} (X の削除 {tomb:x?})"
        );
        assert_eq!(restored, 0, "{when}: 書き出した周期の W の記録から戻した");
    }
}

/// 読み直し [A2] の像の撮り方。
#[derive(Clone, Copy, Debug)]
enum EidaliasBehind {
    /// sidecar の書き出し (`persist_tables`) を `.eidmap` の後・`eidalias` の前で kill (page cache は残る = 本体に使い回しと記録)
    Kill,
    /// 対照: 使い回しの後に C の行 c を Y に畳み (c は Y の場所の別名)、 sidecar を全部 (`eidalias` も) 書いた後・書き出した周期を
    /// 進める前 (`prev_persist_end`) に落ちる。 記録は書き出していない周期のまま、 `eidalias` は使い回しの後 (b は無く c がある)。
    /// None = kill、 Some(段) = 電源断 (`PowerLoss` と同じく本体の書き出しをその段で止めて sidecar を書き出す)
    After(Option<Phase>),
    /// 使い回しの後の `oplog_sync` の本体の書き出しを段 `.0` で止めて sidecar を書き出し、 `.eidmap` の後・`eidalias` の前で電源断
    /// (WAL に使い回し。 `PrevWrittenOut` = 記録は届き、 生きている印は届かない。 `BeforeGate` = 記録も届かない)
    PowerLoss(Phase),
}

/// X (名前 7) に B の行 b を畳み (b の世界番号は X の場所の別名、 `eidalias` に書き出し済み)、 X を消して Y (名前 7、 v = 77) で X の
/// 場所を使い回し、 sidecar の書き出しが `.eidmap` (X と b の退避 = X の削除の版数) を書いた後・`eidalias` を書く前に落ちた像を撮る。
/// 電源断の像の本体は X の削除も使い回しも持たない (WAL に X の Delete と Y の record): 開く時に記録から戻す道は通らず (場所の最後の行
/// X が生きている)、 再生が X を消して Y を生かす所の後始末のやり直しだけが別名を外す。 (X, Y, X の削除の版数) を返す。
///
/// #993 / #994 (PR #976 の読み直し 2 の [B4]): `forget` = 使い回しの後、 忘れてよい線 (client が pull ごとに当てる
/// `forget_deletes_before`) で X の削除の版数より前を忘れてから sidecar を書き出す (使い回しが退避した (自分, X) と b は退避表から消える)。
fn eidalias_behind_image(live: &Path, img: &Path, world_b: u64, how: EidaliasBehind, forget: bool) -> (u64, u64, Hlc) {
    let (eng, x) = full_named_table_engine(&live.join("db"));
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let b = eng.resolve_remote_eid(world_b, hk).unwrap();
    assert!(eng.remote_tie_apply_result(b, hk, 8u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
    eng.merge_entity_into(b, x).unwrap();
    // 畳んで空いた b の場所を埋める (X の場所を使い回させる)
    let z = eng.entity_in("t").unwrap();
    eng.tie_to(z, "t.k", 2u32);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    assert_eq!(
        eng.resolve_remote_eid_existing(world_b).map(|e| eng.eid_slot(e)),
        Some(eng.eid_slot(x)),
        "前提: b は X の場所の別名"
    );
    let _hold = eng.hold_consumer();
    eng.delete(x);
    eng.oplog_commit();
    let tomb = eng.tombstone_hlc(x);
    let y = add_seven(&eng, 77);
    assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
    assert_eq!(eng.resolve_remote_eid_existing(world_b), None, "前提: 使い回しが b を外した (memory)");
    if forget {
        assert!(eng.forget_deletes_before(Hlc { wall: tomb.wall + 1, logical: 0, peer: 0 }) >= 2, "前提: X と b の退避を忘れた");
    }
    let shot = Arc::new(AtomicBool::new(false));
    let (s2, l, i) = (shot.clone(), live.to_path_buf(), img.to_path_buf());
    match how {
        EidaliasBehind::After(at) => {
            let c = eng.resolve_remote_eid(WORLD_C, hk).unwrap();
            assert!(eng.remote_tie_apply_result(c, hk, 9u32, Hlc { wall: 1_001, logical: 0, peer: 4 }).applied());
            eng.merge_entity_into(c, y).unwrap();
            assert_eq!(
                eng.resolve_remote_eid_existing(WORLD_C).map(|e| eng.eid_slot(e)),
                Some(eng.eid_slot(y)),
                "前提: c は Y の場所の別名"
            );
            if let Some(at) = at {
                let (e2, nested) = (eng.clone(), Arc::new(AtomicBool::new(false)));
                write_out_hook::set(Some(Arc::new(move |p: Phase| {
                    if p == at && !nested.swap(true, Ordering::SeqCst) {
                        e2.persist_tables().unwrap();
                    }
                    if p == Phase::SidecarsWritten && !s2.swap(true, Ordering::SeqCst) {
                        let _ = crashsim::capture(&l, &i, Mode::Lost);
                    }
                })));
                eng.oplog_sync().unwrap();
            } else {
                write_out_hook::set(Some(Arc::new(move |p: Phase| {
                    if p == Phase::SidecarsWritten && !s2.swap(true, Ordering::SeqCst) {
                        kill_image(&l, &i);
                    }
                })));
                eng.persist_tables().unwrap();
            }
        }
        EidaliasBehind::Kill => {
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == Phase::EidmapWritten && !s2.swap(true, Ordering::SeqCst) {
                    kill_image(&l, &i);
                }
            })));
            eng.persist_tables().unwrap();
        }
        EidaliasBehind::PowerLoss(at) => {
            let (e2, nested) = (eng.clone(), Arc::new(AtomicBool::new(false)));
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == at && !nested.swap(true, Ordering::SeqCst) {
                    e2.persist_tables().unwrap();
                }
                if p == Phase::EidmapWritten && !s2.swap(true, Ordering::SeqCst) {
                    let _ = crashsim::capture(&l, &i, Mode::Lost);
                }
            })));
            eng.oplog_sync().unwrap();
        }
    }
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: sidecar の書き出しの途中で撮った");
    (x, y, tomb)
}

/// `EidaliasBehind::After` で Y に畳む C の行 c の世界番号。
const WORLD_C: u64 = (4u64 << 32) | 11;

/// 読み直し [A2] (#969 の窓、 master にも): `eidalias_behind_image` の像を開くと、 Y が生き、 使い回しが外した X の別名 b は Y の別名で
/// ない (退避表に X の削除の版数で載っている identity の別名は、 使い回しの前の `eidalias` の残り)。 b は X の削除の版数で消えた identity。
///
/// 前は 「`.eidmap` に退避が届いた = `eidalias` も使い回しの後」 と読み (書く順は `.eidmap` → `eidalias` で、 言えるのは逆の向きだけ)、
/// b を Y の別名に残した (b 宛ての書き込みが Y に当たる)。 開く時に記録から戻す道 (kill の像: 書き出していない周期の記録、 退避は届いて
/// いる) と、 再生のやり直し (記録の届いた電源断の像: 記録の行 X の退避が届いている。 記録の無い像: 退避表に場所の削除の版数の identity
/// があるので前の住人を読まずに返った) の 3 つの道。
fn a_former_alias_left_in_eidalias_is_dropped(tag: &str, how: EidaliasBehind, forget: bool) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_b = enchudb_oplog::make_eid(3, 9);
    let (x, y, tomb) = eidalias_behind_image(&live, &img, world_b, how, forget);
    crashsim::stop();
    let db = img.join("db");
    // 像の前提 (readonly は記録から戻さず、 WAL も見ない): `eidalias` は使い回しの前 (b は X の場所の別名)、 場所の記録
    let (ro_b, ro_c, ro_slot, record, ro_x_alive) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        let slot = ro.eid_slot(x);
        let alias = |w| ro.resolve_remote_eid_existing(w);
        (alias(world_b), alias(WORLD_C), slot, ro.prev_resident_of_slot(slot), ro.is_live(x))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let y_alive = eng.is_live(y) && eng.get(y, "t.v") == Some(77);
    let b_now = eng.resolve_remote_eid_existing(world_b);
    let c_now = eng.resolve_remote_eid_existing(WORLD_C);
    let deletes: Vec<(u64, Hlc)> = eng
        .state_records()
        .0
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some((eid, r.hlc)),
            _ => None,
        })
        .collect();
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    let at_slot = Some(enchudb_oplog::make_eid(1, ro_slot));
    if matches!(how, EidaliasBehind::After(_)) {
        assert_eq!((ro_b, ro_c), (None, at_slot), "前提: 像の `eidalias` は使い回しの後 (b は無く、 c は Y の場所の別名)");
    } else {
        assert_eq!(ro_b, at_slot, "前提: 像の `eidalias` は使い回しの前 (b は X の場所の別名)");
    }
    let x_record = record.is_some_and(|r| r.1 == eid_local(x));
    match how {
        EidaliasBehind::PowerLoss(Phase::BeforeGate) => assert!(!x_record, "前提: X の場所の使い回しの記録は届いていない: {record:x?}"),
        _ => assert!(x_record, "前提: 場所の記録は前の住人 X: {record:x?}"),
    }
    // 電源断の像は本体に X の削除が無い (記録から戻す道を通らず、 再生のやり直しだけが外す)。 kill の像は本体に使い回しがある
    assert_eq!(
        ro_x_alive,
        matches!(how, EidaliasBehind::PowerLoss(_) | EidaliasBehind::After(Some(_))),
        "前提: 像の本体の X ({how:?})"
    );
    assert!(y_alive, "前提: 開いた A で Y が生きている");
    assert_eq!(b_now, None, "使い回しが外した X の別名 b が、 使い回した Y の別名に残った ({how:?}、 忘れた: {forget})");
    // 忘れた後の b の削除は state に出なくてよい (記録から戻す道が書き出していない周期の記録で戻すことはある)
    if !forget {
        assert!(deletes.contains(&(world_b, tomb)), "state に X の別名 b の Delete (X の削除の版数) が無い ({how:?}): {deletes:x?}");
    }
    if matches!(how, EidaliasBehind::After(_)) {
        assert_eq!(c_now, at_slot, "使い回しの後に Y に畳んだ c の別名を外した ({how:?}、 忘れた: {forget})");
        assert!(!deletes.iter().any(|&(e, _)| e == WORLD_C), "生きている Y の別名 c の Delete を state に出した ({how:?}、 忘れた: {forget}): {deletes:x?}");
    }
}

#[test]
fn a_former_alias_left_in_eidalias_is_dropped_on_restore_after_kill() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_left_in_eidalias_is_dropped("eidalias_behind_kill", EidaliasBehind::Kill, false);
}

/// 読み直し [A2] の対照: 書き出していない周期の記録の後始末 (記録から戻す道 / 再生のやり直し) は、 退避が `.eidmap` に届いている時、
/// 使い回しの後に次の住人 Y へ畳んだ別名 c (退避を持たない) を外さない (`eidalias` は使い回しの後まで届いた像)。 「退避が書き出し済み
/// なら場所の別名を全部外す」 では c を Y から外した。 kill の像は記録から戻す道、 電源断の像 (本体に X の削除も使い回しも無い) は
/// 再生のやり直しの自分の行の枝。
#[test]
fn an_alias_folded_into_the_new_resident_is_kept_by_the_restore() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_left_in_eidalias_is_dropped("eidalias_after_kill", EidaliasBehind::After(None), false);
}

#[test]
fn an_alias_folded_into_the_new_resident_is_kept_by_the_redo() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_left_in_eidalias_is_dropped("eidalias_after_power_loss", EidaliasBehind::After(Some(Phase::PrevWrittenOut)), false);
}

#[test]
fn a_former_alias_left_in_eidalias_is_dropped_on_redo_with_the_record() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_left_in_eidalias_is_dropped("eidalias_behind_record", EidaliasBehind::PowerLoss(Phase::PrevWrittenOut), false);
}

#[test]
fn a_former_alias_left_in_eidalias_is_dropped_on_redo_without_a_record() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_left_in_eidalias_is_dropped("eidalias_behind_no_record", EidaliasBehind::PowerLoss(Phase::BeforeGate), false);
}

/// #993 / #994 (PR #976 の読み直し 2 の [B4]): `an_alias_folded_into_the_new_resident_is_kept_by_the_restore` / `_by_the_redo` の像の、
/// 使い回しの後に忘れてよい線が X の削除を忘れた形 (client は pull ごとに忘れ、 何か忘れれば sidecar を書き出す)。 記録 (前の住人 X) は
/// 書き出していない周期で、 退避表に X が居ないので 「退避が書き出されていない」 と読める。 前は場所の別名を全部外し、 使い回しの後に Y に
/// 畳んだ c を外して X の削除の版数で消えた identity にした。 `eidalias` 版 2 の世代で、 外すのは X とそれより前の行の別名だけ (c は Y の
/// 世代)。
#[test]
fn an_alias_folded_into_the_new_resident_is_kept_after_forgetting() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_left_in_eidalias_is_dropped("eidalias_after_kill_forgot", EidaliasBehind::After(None), true);
    a_former_alias_left_in_eidalias_is_dropped("eidalias_after_power_loss_forgot", EidaliasBehind::After(Some(Phase::PrevWrittenOut)), true);
}

/// #993: `eidalias` が使い回しの前のまま (b が X の場所の別名) の像の、 忘れてよい線が X と b の削除を忘れた形。 b は X の世代なので外す
/// (退避表に居なくても)。
#[test]
fn a_former_alias_left_in_eidalias_is_dropped_after_forgetting() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_left_in_eidalias_is_dropped("eidalias_behind_kill_forgot", EidaliasBehind::Kill, true);
    a_former_alias_left_in_eidalias_is_dropped("eidalias_behind_record_forgot", EidaliasBehind::PowerLoss(Phase::PrevWrittenOut), true);
    a_former_alias_left_in_eidalias_is_dropped("eidalias_behind_no_record_forgot", EidaliasBehind::PowerLoss(Phase::BeforeGate), true);
}

/// [R1] (#969 / #968 の PR): 同じ場所 A を 2 回使い回し (x → R1 → R2)、 その後 R2 も消した像で、 WAL の再生が R1 (記録の前の住人) の
/// 生かす record を捨てる時は、 A の後始末をやり直さない (場所ごとの配列を書き換えない)。
///
/// 周期 e で x → R1 (log の区画 e の 2 件目、 前の住人 x)、 sidecar を書き出して周期 e+1 で R1 → R2 (区画 e+1 の 1 件目、 前の住人
/// R1)、 周期 e+2 と e+3 で別の場所 B を 1 回ずつ使い回して区画の頭を使い直す (区画 e+1 の R1 の記録は消え、 配列にだけ残る。 区画 e の
/// x の記録は 2 件目なので残る)。 R2 を消して、 本体の書き出しの後で撮る (WAL の checkpoint は R1 を作る前、 consumer を止めた)。 開くと
/// 場所 A は死、 WAL に R1 の Tie がある。 再生は R1 宛ての record を捨て (R1 は配列の記録の前の住人)、 配列は R1 のまま。
///
/// 前は捨てる判定の前に後始末をやり直して配列を x で上書きし、 判定が外れて R1 を生かし直した (配列には x が残った)。
#[test]
fn a_dropped_record_of_a_former_resident_keeps_the_slot_record() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_dropped_record_keeps_slot");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (slot_a, r1) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let keep = eng.pull_raw("t.k", 1u32);
        assert_eq!(keep.len(), 1, "前提: 名前 1 の行");
        let _hold = eng.hold_consumer();
        // 場所 B を使い回す (名前 1 のまま)
        let reuse_b = |b: u64| -> u64 {
            eng.delete(b);
            let nb = eng.entity_in("t").unwrap();
            assert_eq!(eng.eid_slot(nb), eng.eid_slot(b), "前提: 場所 B を使い回した");
            eng.tie_to(nb, "t.k", 1u32);
            eng.oplog_commit();
            nb
        };
        // 周期 e: B (1 件目)、 x → R1 (2 件目)
        let b1 = reuse_b(keep[0]);
        eng.delete(x);
        let r1 = eng.entity_in("t").unwrap();
        assert_eq!(eng.eid_slot(r1), eng.eid_slot(x), "前提: R1 は x の場所を使い回した");
        eng.tie_to(r1, "t.k", 7u32);
        eng.tie_to(r1, "t.v", 71u32);
        eng.oplog_commit();
        eng.persist_sync_state().unwrap();
        // 周期 e+1: R1 → R2 (1 件目)
        eng.delete(r1);
        let r2 = eng.entity_in("t").unwrap();
        assert_eq!(eng.eid_slot(r2), eng.eid_slot(x), "前提: R2 は場所 A を使い回した");
        eng.tie_to(r2, "t.k", 7u32);
        eng.oplog_commit();
        eng.persist_sync_state().unwrap();
        // 周期 e+2 / e+3: B を 1 回ずつ (区画の頭を使い直す。 e+3 が区画 e+1 の R1 の記録を上書きする)
        let b2 = reuse_b(b1);
        eng.persist_sync_state().unwrap();
        let _b3 = reuse_b(b2);
        eng.delete(r2);
        eng.oplog_commit();
        let s = eng.stats();
        assert!(s.oplog_checkpoint < s.oplog_head, "前提: WAL に R1 の record が残る");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::WrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: 本体の書き出しの後で撮った");
        (eng.eid_slot(x), r1)
    };
    crashsim::stop();
    let db = img.join("db");
    let before = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        ro.prev_resident_of_slot(slot_a)
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let after = eng.prev_resident_of_slot(slot_a);
    let r1_alive = eng.is_live(r1);
    let sevens: Vec<u64> = eng.pull_raw("t.k", 7u32);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(before.map(|r| r.1), Some(eid_local(r1)), "前提: 場所 A の配列の記録は前の住人 R1");
    assert!(!r1_alive && sevens.is_empty(), "前提: 開いた後 R1 も R2 も消えている");
    assert_eq!(after.map(|r| r.1), Some(eid_local(r1)), "R1 宛ての捨てる record の再生が、 場所 A の配列の記録を前の前の住人で上書きした");
}

/// 読み直し 2 [B1] の像の DB: sync する、 表 t (枠 2、 c = Number)、 peer A = 1。 C (peer 3) の行 F (c = 5、 世界番号 `WORLD_F`) と
/// B (peer 2) の行 b (c = 6、 `WORLD_B2`) を受けて b を F に畳み (b の世界番号は F の場所の別名、 `eidalias` に書き出す)、 畳んで空いた
/// b の場所を A の行 Z で埋めて (v15)、 全部書き出す。 consumer の周期を止めずに engine と F の場所を返す。
fn translated_row_with_alias_engine(db: &Path) -> (Arc<Engine>, u64) {
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 2).unwrap();
    eng.define_himo_in("t", "c", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let f = eng.resolve_remote_eid(WORLD_F, hid).unwrap();
    assert!(eng.remote_tie_apply_result(f, hid, 5u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
    let b = eng.resolve_remote_eid(WORLD_B2, hid).unwrap();
    assert!(eng.remote_tie_apply_result(b, hid, 6u32, Hlc { wall: 1_001, logical: 0, peer: 2 }).applied());
    eng.merge_entity_into(b, f).unwrap();
    let z = eng.entity_in("t").unwrap();
    assert_eq!(eng.eid_slot(z), eng.eid_slot(b), "前提: Z は畳んで空いた b の場所を使い回した (v15)");
    eng.tie_to(z, "t.c", 2u32);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    assert!(eng.slot_bits() > 0, "前提: v15");
    assert_eq!(
        eng.resolve_remote_eid_existing(WORLD_B2).map(|e| eng.eid_slot(e)),
        Some(eng.eid_slot(f)),
        "前提: b は F の場所の別名"
    );
    assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: `.eidmap` は memory と同じ");
    (eng, f)
}

/// 読み直し 2 [B1] の C の行 F の世界番号。
const WORLD_F: u64 = (3u64 << 32) | 7;
/// 読み直し 2 [B1] の、 F に畳む B の行 b の世界番号。
const WORLD_B2: u64 = (2u64 << 32) | 9;
/// 読み直し 2 [B1] の F の削除 (C から受ける。 A の state には出ない = C の行を C が消した)。
const F_DELETE: Hlc = Hlc { wall: 2_000, logical: 0, peer: 3 };

/// 読み直し 2 [B1] の像の撮り方。
#[derive(Clone, Copy, Debug, PartialEq)]
enum TranslatedBehind {
    /// (a) Y が F の場所を使い回した後、 世界番号 `.0` (F の (C, f) か、 b の (B, b)) 宛ての遅れた書き込みが届いてその世界番号を新しい
    /// 死んだ場所 S2 に払い直し、 sidecar の書き出しを `.eidmap` の後・`eidalias` の前で kill (page cache は残る = 本体に使い回しと記録)。
    /// (B, b) の時は `.eidmap` の (B, b) が S2 への写像 (と退避) で、 `eidalias` の (B, b) → F の場所がそれを上書きする
    Remapped(u64),
    /// (b) Y が F の場所を使い回した後、 B の遅れた書き込み (b 宛て、 A では当たらない) を受けて中継し (ホスト / gossip、 WAL に載る)、
    /// `oplog_sync` の本体の書き出しを段 `.0` で止めて sidecar を書き出し、 `.eidmap` の後・`eidalias` の前で電源断 (F の削除は受けた
    /// record = WAL に無い。 本体の F は生きている、 WAL に Y の record と中継した b 宛ての書き込み)
    PowerLoss(Phase),
}

/// 読み直し 2 [B1]: 翻訳した行 F (B の行 b を畳んだ、 b の世界番号は F の場所の別名) を C の削除で消して、 Y で F の場所を使い回し、
/// sidecar の書き出しが `.eidmap` (F と b の退避 = F の削除の版数) を書いた後・`eidalias` を書く前に落ちた像を撮る。 (A での F, Y) を
/// 返す。 #993: `forget` = 使い回しの後、 忘れてよい線 (client が pull ごとに当てる `forget_deletes_before`) で F の削除の版数より前を
/// 忘れてから sidecar を書き出す (使い回しが退避した b は退避表から消える)。 3 つ目は (a) で払い直した死んだ場所 S2。
fn translated_eidalias_behind_image(live: &Path, img: &Path, how: TranslatedBehind, forget: bool) -> (u64, u64, Option<u32>) {
    let (eng, f) = translated_row_with_alias_engine(&live.join("db"));
    let f_slot = eng.eid_slot(f);
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let _hold = eng.hold_consumer();
    assert!(eng.remote_delete_apply_foreign(WORLD_F, F_DELETE), "前提: C の削除が F に当たった");
    let y = eng.entity_in("t").unwrap();
    eng.tie_to(y, "t.c", 9u32);
    eng.oplog_commit();
    assert_eq!(eng.eid_slot(y), f_slot, "前提: Y は F の場所を使い回した");
    assert_eq!(eng.resolve_remote_eid_existing(WORLD_B2), None, "前提: 使い回しが b を外した (memory)");
    let shot = Arc::new(AtomicBool::new(false));
    let (s2, l, i) = (shot.clone(), live.to_path_buf(), img.to_path_buf());
    let mut remapped = None;
    match how {
        TranslatedBehind::Remapped(world) => {
            // 消えた F のデッキ宛ての遅れた書き込み (sync ではよくある): 退避表の削除で新しい場所を死んだまま払い出す (当たらない)
            let late = eng.resolve_remote_eid(world, hid).unwrap();
            assert_ne!(eng.eid_slot(late), f_slot, "前提: {world:x} を別の場所に払い直した");
            remapped = Some(eng.eid_slot(late));
            let at = Hlc { wall: 1_500, logical: 0, peer: enchudb_oplog::eid_peer(world) };
            assert!(!eng.remote_tie_apply(late, hid, 4u32, at), "前提: 消えた F のデッキに当たらない");
            if forget {
                assert!(eng.forget_deletes_before(Hlc { wall: F_DELETE.wall + 1, logical: 0, peer: 0 }) >= 1, "前提: 退避を忘れた");
            }
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == Phase::EidmapWritten && !s2.swap(true, Ordering::SeqCst) {
                    kill_image(&l, &i);
                }
            })));
            eng.persist_tables().unwrap();
        }
        TranslatedBehind::PowerLoss(at) => {
            let later = Hlc { wall: eng.cell_hlc(y, hid).wall + 1_000_000, logical: 0, peer: 2 };
            let late = eng.resolve_remote_eid(WORLD_B2, hid).unwrap();
            assert!(!eng.remote_tie_apply(late, hid, 3u32, later), "前提: 消えた b に当たらない");
            let rec = enchudb_engine::transport::WireRecord::unsigned(later, 2, DecodedOp::Tie { eid: WORLD_B2, himo_id: hid, value: 3 });
            assert!(eng.relay_record(&rec), "前提: b 宛ての書き込みを中継して WAL に載せた");
            eng.oplog_commit();
            if forget {
                assert!(eng.forget_deletes_before(Hlc { wall: F_DELETE.wall + 1, logical: 0, peer: 0 }) >= 1, "前提: 退避を忘れた");
            }
            let (e2, nested) = (eng.clone(), Arc::new(AtomicBool::new(false)));
            write_out_hook::set(Some(Arc::new(move |p: Phase| {
                if p == at && !nested.swap(true, Ordering::SeqCst) {
                    e2.persist_tables().unwrap();
                }
                if p == Phase::EidmapWritten && !s2.swap(true, Ordering::SeqCst) {
                    let _ = crashsim::capture(&l, &i, Mode::Lost);
                }
            })));
            eng.oplog_sync().unwrap();
        }
    }
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: sidecar の書き出しの途中で撮った");
    (f, y, remapped)
}

/// 読み直し 2 [B1] (master にも、 #969 の窓): `translated_eidalias_behind_image` の像を開くと、 Y が A の行として生き、 使い回しが外した
/// F の別名 b は Y の別名でない (退避表に載っている identity の別名は、 使い回しの前の `eidalias` の残り)。 b は消えた identity
/// (B の遅れた書き込みが当たらない)。
///
/// 前の直し ([A2]) は前の住人の記録を見る 3 つの道で別名を外したが、 この 2 つの像はどれも通らず、 b が Y の別名に残った (b 宛ての
/// 書き込みが Y に当たる。 (b) では WAL の再生の中継した b 宛ての書き込みも): (a) 記録から戻す道は (C, f) が写像を持つ (S2) ので何もせずに返った。 (b) 記録から戻す道は場所の最後の行
/// F が生きているので返り、 再生は Y の record で場所の世代を上げるだけ (`replay_slot` の `g > now`、 やり直しを呼ばない)。
/// (a) で b の世界番号を払い直した像 (`.eidmap` に (B, b) → S2 の写像と退避、 `eidalias` の (B, b) → F の場所が上書きする) は前の
/// 直しでも外れた (記録から戻す道を通る) が、 開く道の 1 か所で外す形でも退避表で見分ける (`.eidmap` は写像と退避の 2 つの面を書く)。
fn a_former_alias_of_a_translated_row_is_dropped(tag: &str, how: TranslatedBehind, forget: bool) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (f, y, remapped) = translated_eidalias_behind_image(&live, &img, how, forget);
    crashsim::stop();
    let db = img.join("db");
    // 像の前提 (readonly は記録から戻さず、 WAL も見ない)
    let (ro_b, ro_f, ro_slot, record, ro_live) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        let slot = ro.eid_slot(y);
        let at = |w| ro.resolve_remote_eid_existing(w).map(|e| ro.eid_slot(e));
        let resident = match how {
            TranslatedBehind::Remapped(_) => ro.is_live(y),
            TranslatedBehind::PowerLoss(_) => ro.is_live(f),
        };
        (at(WORLD_B2), at(WORLD_F), slot, ro.prev_resident_of_slot(slot), resident)
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let y_alive = eng.is_live(y);
    let y_opened = eng.get(y, "t.c");
    // b の世界番号の行き先の場所 (消えた identity なら無いか、 遅れた書き込みが払い出した死んだ場所)
    let b_now = eng.resolve_remote_eid_existing(WORLD_B2).map(|e| eng.eid_slot(e));
    let f_now = eng.resolve_remote_eid_existing(WORLD_F).map(|e| eng.eid_slot(e));
    let (records, _) = eng.state_records();
    // B の遅れた書き込み (F の削除を知らない) は b の世界番号宛て: 消えた identity なら当たらない
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let later = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
    let b_late = eng.resolve_remote_eid(WORLD_B2, hid).is_some_and(|t| eng.remote_tie_apply(t, hid, 3u32, later));
    let y_after = eng.get(y, "t.c");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    let nine: Vec<u64> = records
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Tie { eid, value: 9, .. } => Some(eid),
            _ => None,
        })
        .collect();
    assert_eq!(ro_b, Some(ro_slot), "前提: 像の `eidalias` は使い回しの前 (b は F の場所の別名) ({how:?})");
    let f_record = record.is_some_and(|r| r.0 == 3 && r.1 == 7 && !r.3);
    match how {
        TranslatedBehind::Remapped(world) => {
            if world == WORLD_F {
                assert!(ro_f.is_some_and(|s| s != ro_slot), "前提: (C, f) は別の場所に払い直してある: {ro_f:?}");
            }
            assert!(f_record, "前提: 場所の記録は前の住人 F (翻訳した行): {record:x?}");
            assert!(ro_live, "前提: 本体に使い回し (Y が生きている)");
        }
        TranslatedBehind::PowerLoss(at) => {
            assert_eq!(ro_f, None, "前提: (C, f) の退避が `.eidmap` に届いた");
            assert_eq!(f_record, at == Phase::PrevWrittenOut, "前提: 場所の記録 ({at:?}): {record:x?}");
            assert!(ro_live, "前提: 本体の F は生きている (F の削除は本体にも WAL にも無い)");
        }
    }
    assert!(y_alive, "前提: 開いた A で Y が生きている ({how:?})");
    assert_eq!(y_opened, Some(9), "WAL の再生で b 宛ての中継した書き込みが Y に当たった ({how:?})");
    assert!(nine.len() == 1 && enchudb_oplog::eid_peer(nine[0]) == 1, "前提: Y は A の行: {nine:x?}");
    assert_eq!(f_now.filter(|&s| s == ro_slot), None, "前提: (C, f) は Y の場所を指さない ({how:?})");
    assert_ne!(b_now, Some(ro_slot), "使い回しが外した F の別名 b が、 使い回した Y の別名に残った ({how:?}、 忘れた: {forget})");
    // #994 [C4]: `.eidmap` の (B, b) → S2 (使い回しの後に払い直した写像) は、 古い `eidalias` の別名を外した後に戻る
    if how == TranslatedBehind::Remapped(WORLD_B2) {
        assert_eq!(b_now, remapped, "使い回しの後に払い直した b の写像 (`.eidmap`) が戻らない (忘れた: {forget})");
    }
    assert_eq!(y_after, Some(9), "b 宛ての遅れた書き込みが Y に当たった ({how:?}、 忘れた: {forget})");
    // 忘れた後の b は消えた identity でない (退避表に無い) ので、 遅れた書き込みは新しい行に当たってよい
    if !forget {
        assert!(!b_late, "b 宛ての遅れた書き込みが当たった ({how:?})");
    }
}

#[test]
fn a_former_alias_of_a_translated_row_mapped_again_is_dropped_after_kill() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_of_a_translated_row_is_dropped("translated_alias_remapped", TranslatedBehind::Remapped(WORLD_F), false);
    a_former_alias_of_a_translated_row_is_dropped("translated_alias_alias_remapped", TranslatedBehind::Remapped(WORLD_B2), false);
}

/// #993: 上の像の、 使い回しの後に忘れてよい線が F の削除を忘れた形 (client は pull ごとに忘れる)。 退避表に b が居ないので、 前の開く
/// 時の見分け (別名の identity が退避表に居る = 使い回しが外した別名) は b を Y の別名に残した。 `eidalias` 版 2 の世代 (b は F の世代)
/// で外す。
#[test]
fn a_former_alias_of_a_translated_row_mapped_again_is_dropped_after_forgetting() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_of_a_translated_row_is_dropped("translated_alias_remapped_forgot", TranslatedBehind::Remapped(WORLD_F), true);
    a_former_alias_of_a_translated_row_is_dropped("translated_alias_alias_remapped_forgot", TranslatedBehind::Remapped(WORLD_B2), true);
}

#[test]
fn a_former_alias_of_a_translated_row_alive_in_the_body_is_dropped() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_of_a_translated_row_is_dropped("translated_alias_record", TranslatedBehind::PowerLoss(Phase::PrevWrittenOut), false);
    a_former_alias_of_a_translated_row_is_dropped("translated_alias_no_record", TranslatedBehind::PowerLoss(Phase::BeforeGate), false);
}

/// #993: 上の像 (本体の F が生きている電源断) の、 忘れてよい線が F と b の削除を忘れた形。 前は再生の使い回しの移り目 (`g > now`) で
/// 何もせず、 再生の後の見分けも退避表に b が居ないので b を Y の別名に残し、 中継した b 宛ての書き込みが Y に当たった。
#[test]
fn a_former_alias_of_a_translated_row_alive_in_the_body_is_dropped_after_forgetting() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_alias_of_a_translated_row_is_dropped("translated_alias_record_forgot", TranslatedBehind::PowerLoss(Phase::PrevWrittenOut), true);
    a_former_alias_of_a_translated_row_is_dropped("translated_alias_no_record_forgot", TranslatedBehind::PowerLoss(Phase::BeforeGate), true);
}

/// 読み直し 2 [B2]: X (名前 7) に B の行 b を畳み (b の世界番号は X の場所の別名)、 書き出した後、 名前 1 の行に新しい content の列
/// (`t._c_memo`) で書き (WAL に TieLeaf、 列の定義は header / 紐名の辞書と一緒に次の本体の書き出しの残りの段で届く)、 X を消して Y で
/// X の場所を使い回し、 `oplog_sync` の本体の書き出しの `prev.seg` の後・生きている印の前 (`PrevWrittenOut`) に電源が落ちる。 像は
/// 「本体に X が生きていて列の定義が無く、 場所の記録 (前の住人 X) は書き出していない周期、 WAL に [列の書き込み, X の Delete, Y]」。
///
/// 開く時の再生は、 列の TieLeaf で列を定義する時に sidecar を書き出し (`ensure_himo_dynamic_in` → `try_persist_tables`)、 その後の
/// Y の record で後始末をやり直す。 開いた A では X の削除が退避表 (state の X の Delete) にあり、 b は Y の別名でない (X の削除の版数で
/// 消えた identity)。 開く道の途中の書き出し (再生が列を定義した時) の後で kill した像をもう一度開いても同じ。
///
/// 前は再生の途中の書き出しが `prev.seg` の周期を覆って記録を 「書き出し済み」 にし、 やり直しが自分の行の後始末を飛ばした ([A1] の
/// 守り): X の削除がどこにも残らず (場所の版数も消した、 #914 の形)、 b が Y の別名に残った。 header の書き出した周期も進んだので、
/// 途中の書き出しの後に落ちた像の次の open も記録から戻さなかった。
#[test]
fn a_persist_during_the_replay_does_not_skip_the_redo_of_the_reuse() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_persist_during_replay");
    let (live, img, again) = (root.join("live"), root.join("img"), root.join("again"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_b = enchudb_oplog::make_eid(3, 9);
    let (x, y, tomb) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let hk = eng.himo_id("t.k").unwrap() as u16;
        let b = eng.resolve_remote_eid(world_b, hk).unwrap();
        assert!(eng.remote_tie_apply_result(b, hk, 8u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
        eng.merge_entity_into(b, x).unwrap();
        // 畳んで空いた b の場所を埋める (X の場所を使い回させる)
        let z = eng.entity_in("t").unwrap();
        eng.tie_to(z, "t.k", 2u32);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        eng.persist_sync_state().unwrap();
        let keep = eng.pull_raw("t.k", 1u32);
        assert_eq!(keep.len(), 1, "前提: 名前 1 の行");
        let _hold = eng.hold_consumer();
        eng.content(keep[0], "memo", b"hello");
        eng.oplog_commit();
        assert!(eng.himo_id("t._c_memo").is_some(), "前提: 新しい content の列");
        eng.delete(x);
        eng.oplog_commit();
        let tomb = eng.tombstone_hlc(x);
        let y = add_seven(&eng, 77);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の後で撮った");
        (x, y, tomb)
    };
    crashsim::stop();
    let db = img.join("db");
    // 像の前提 (readonly は WAL を見ず、 記録から戻さない)
    let (ro_column, ro_x, record, ro_b) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        let slot = ro.eid_slot(x);
        (
            ro.himo_id("t._c_memo"),
            ro.is_live(x),
            ro.prev_resident_of_slot(slot),
            ro.resolve_remote_eid_existing(world_b).map(|e| ro.eid_slot(e) == slot),
        )
    };
    let observe = |eng: &Engine| {
        let deletes: Vec<(u64, Hlc)> = eng
            .state_records()
            .0
            .iter()
            .filter_map(|r| match r.op {
                DecodedOp::Delete { eid } => Some((eid, r.hlc)),
                _ => None,
            })
            .collect();
        (
            eng.is_live(y) && eng.get(y, "t.v") == Some(77),
            eng.is_live(x),
            eng.himo_id("t._c_memo").is_some(),
            eng.resolve_remote_eid_existing(world_b),
            deletes,
        )
    };
    // 開く道の途中の書き出し (再生が列を定義した時、 開く thread の上) の後で kill の像を撮る
    let opener = std::thread::current().id();
    let shots = Arc::new(AtomicU32::new(0));
    {
        let (n, from, to) = (shots.clone(), db.clone(), again.join("db"));
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::SidecarsWritten && std::thread::current().id() == opener && n.fetch_add(1, Ordering::SeqCst) == 0 {
                kill_image(&from, &to);
            }
        })));
    }
    let first = {
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        write_out_hook::set(None);
        eng.set_peer_id(1);
        observe(&eng)
    };
    let second = {
        let eng = Engine::open_concurrent_with_oplog(again.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        observe(&eng)
    };
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_column, None, "前提: 像の本体に新しい列の定義は無い (再生が定義する)");
    assert!(ro_x, "前提: 像の本体の X は生きている (X の削除も使い回しも WAL にだけ)");
    assert!(
        record.is_some_and(|r| r.1 == eid_local(x) && r.3),
        "前提: 場所の記録は前の住人 X (自分の行): {record:x?}"
    );
    assert_eq!(ro_b, Some(true), "前提: 像の `eidalias` で b は X の場所の別名");
    assert!(shots.load(Ordering::SeqCst) >= 1, "前提: 開く道の途中 (再生が列を定義した時) に sidecar を書き出した");
    for (when, (y_alive, x_alive, column, b_now, deletes)) in [("開いた時", first), ("開く道の途中の書き出しの後に落ちた像", second)] {
        assert!(y_alive && !x_alive && column, "{when}: 前提: 再生が列を定義し、 X を消して Y を生かした");
        assert!(deletes.contains(&(x, tomb)), "{when}: X の削除がどこにも残らない (state に X の Delete が無い): {deletes:x?}");
        assert_eq!(b_now, None, "{when}: 消えた X の別名 b が使い回した Y に写った: {b_now:x?}");
        assert!(deletes.contains(&(world_b, tomb)), "{when}: state に X の別名 b の Delete が無い: {deletes:x?}");
    }
}

/// 読み直し 2 [B3]: `a_deferred_restore_drops_the_aliases_of_the_former_resident` の像 (本体に X の死、 使い回しは記録と WAL にだけ) の、
/// 使い回しの後に D (peer 4) から 「D の行 c と Y は同じ」 (`Alias`) を受けて c を Y の別名にし、 その record を中継して WAL に載せた形
/// (ホスト / gossip)。 開く時は戻すかを再生の後に回し、 再生が Y を生かす所で X の別名 b を外し、 中継した `Alias` の再生が c を Y に
/// 張る。 後に回した戻しは、 再生の前に控えた別名 (b) だけ外す: c は Y の別名のまま、 c の Delete は state に出ない。
///
/// 前は後に回した戻しが再生の後の場所の別名を全部外し、 Y の別名 c を X の削除の版数で消えた identity にした (state に c の Delete =
/// peer では D の行 c = Y のデッキが消える)。
#[test]
fn a_deferred_restore_keeps_an_alias_the_replay_put_on_the_new_resident() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("prev_deferred_keeps_replayed_alias");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_b = enchudb_oplog::make_eid(3, 9);
    let (x, y, tomb) = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let hk = eng.himo_id("t.k").unwrap() as u16;
        let b = eng.resolve_remote_eid(world_b, hk).unwrap();
        assert!(eng.remote_tie_apply_result(b, hk, 8u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
        eng.merge_entity_into(b, x).unwrap();
        let z = eng.entity_in("t").unwrap();
        eng.tie_to(z, "t.k", 2u32);
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        eng.persist_sync_state().unwrap();
        let _hold = eng.hold_consumer();
        eng.delete(x);
        eng.oplog_commit();
        let tomb = eng.tombstone_hlc(x);
        eng.oplog_sync().unwrap();
        let y = add_seven(&eng, 77);
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
        // D から 「c と Y は同じ」 を受けて当て、 中継して WAL に載せる (ホスト / gossip の受け手と同じ)
        let alias = enchudb_engine::transport::WireRecord::unsigned(
            Hlc { wall: tomb.wall + 1_000_000, logical: 0, peer: 4 },
            4,
            DecodedOp::Alias { eid: WORLD_C, same_as: y },
        );
        assert!(eng.apply_alias(WORLD_C, y), "前提: c を Y の別名にした");
        assert!(eng.relay_record(&alias), "前提: `Alias` を中継して WAL に載せた");
        eng.oplog_commit();
        let shot = Arc::new(AtomicBool::new(false));
        let (s2, l, i) = (shot.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot.load(Ordering::SeqCst), "前提: prev.seg の後で撮った");
        (x, y, tomb)
    };
    crashsim::stop();
    let db = img.join("db");
    let (ro_c, ro_x_dead) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (ro.resolve_remote_eid_existing(WORLD_C), !ro.is_live(x) && !ro.is_live(y))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let y_alive = eng.is_live(y) && eng.get(y, "t.v") == Some(77);
    let restored = eng.prev_residents_restored();
    let b_now = eng.resolve_remote_eid_existing(world_b);
    let c_now = eng.resolve_remote_eid_existing(WORLD_C).map(|e| eng.eid_slot(e));
    let deletes: Vec<(u64, Hlc)> = eng
        .state_records()
        .0
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } => Some((eid, r.hlc)),
            _ => None,
        })
        .collect();
    let y_slot = eng.eid_slot(y);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_c, None, "前提: 像の `eidalias` に c は無い (再生が張る)");
    assert!(ro_x_dead, "前提: 像の本体の X は死んでいて Y は無い (使い回しは記録と WAL にだけ)");
    assert!(y_alive, "前提: 再生が Y を生かした");
    assert_eq!(restored, 1, "前提: 後に回した記録から X の削除を戻した");
    assert_eq!(b_now, None, "消えた X の別名 b が使い回した Y に写った: {b_now:x?}");
    assert!(deletes.contains(&(world_b, tomb)), "state に X の別名 b の Delete が無い: {deletes:x?}");
    assert_eq!(c_now, Some(y_slot), "再生が Y に張った別名 c を、 後に回した戻しが外した");
    assert!(!deletes.iter().any(|&(e, _)| e == WORLD_C), "生きている Y の別名 c の Delete を state に出した: {deletes:x?}");
}

/// #994 [C1] の像 (ホスト = 受けた record を中継して WAL に載せる DB): v15 の DB の、 使い回した事の無い場所 (世代 0) の自分の行 X
/// (名前 50) に、 中継した `Alias` で B の世界番号 K を別名に付け (record は WAL の checkpoint の後ろ、 `eidalias` にはその効果 = K → X の
/// 場所 (X の世代) を書き出す)、 X を名前 8 に改名してから消し、 Y (名前 9) で X の場所を使い回す。 その `oplog_sync` の本体の書き出しを
/// 中身の段 (`BeforeGate`) で止めて sidecar を書き出し、 `.eidmap` (K と X の退避) の後・`eidalias` の前で電源断。 像は 「本体の X は
/// 生きている (名前 50)、 WAL に [中継した Alias, X の改名, X の Delete, Y]、 `eidalias` は K → X の場所」。 (X, Y, X の削除の版数, K) を返す。
fn relayed_alias_before_reuse_image(live: &Path, img: &Path) -> (u64, u64, Hlc, u64) {
    let (eng, _) = full_named_table_engine(&live.join("db"));
    // 使い回した事の無い場所の行 X (枠を足した先)。 枠の終わりまで埋めて、 X を消した後の払い出しが X の場所を使い回すようにする
    let x = eng.entity_in("t").unwrap();
    eng.tie_to(x, "t.k", 50u32);
    let mut n = 300u32;
    while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity) {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.k", n);
        n += 1;
    }
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    assert_eq!(eid_local(x) >> eng.slot_bits(), 0, "前提: X は世代 0 (使い回した事の無い場所)");
    let world_k = enchudb_oplog::make_eid(3, 21);
    let _hold = eng.hold_consumer();
    let at = Hlc { wall: eng.cell_hlc(x, eng.himo_id("t.k").unwrap() as u16).wall + 10, logical: 0, peer: 3 };
    assert!(eng.apply_alias(world_k, x), "前提: K を X の別名にした");
    let rec = enchudb_engine::transport::WireRecord::unsigned(at, 3, DecodedOp::Alias { eid: world_k, same_as: x });
    assert!(eng.relay_record(&rec), "前提: 中継した Alias を WAL に載せた");
    eng.oplog_commit();
    // 本体と sidecar (`eidalias` = K → X の場所) を書き出す。 WAL の checkpoint は進めない
    eng.persist_sync_state().unwrap();
    eng.tie_to(x, "t.k", 8u32);
    eng.oplog_commit();
    eng.delete(x);
    eng.oplog_commit();
    let tomb = eng.tombstone_hlc(x);
    let y = eng.entity_in("t").unwrap();
    eng.tie_to(y, "t.k", 9u32);
    eng.tie_to(y, "t.v", 90u32);
    eng.oplog_commit();
    assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y は X の場所を使い回した");
    assert_eq!(eng.resolve_remote_eid_existing(world_k), None, "前提: 使い回しが K を外した (memory)");
    let shot = Arc::new(AtomicBool::new(false));
    let (s2, l, i) = (shot.clone(), live.to_path_buf(), img.to_path_buf());
    let (e2, nested) = (eng.clone(), Arc::new(AtomicBool::new(false)));
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::BeforeGate && !nested.swap(true, Ordering::SeqCst) {
            e2.persist_tables().unwrap();
        }
        if p == Phase::EidmapWritten && !s2.swap(true, Ordering::SeqCst) {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
        }
    })));
    eng.oplog_sync().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: sidecar の書き出しの途中で撮った");
    (x, y, tomb, world_k)
}

/// #994 [C1]: `relayed_alias_before_reuse_image` を開く。 K は X の世代を指す別名なので、 本体の X が生きている再生の前には外さない
/// (中継した `Alias` の再生は K と X が同じ行で何もしない)。 再生は X の改名 (名前 8) と Delete を当て、 Y の record で場所が次の世代に
/// 移る所 (使い回しの移り目) で K を外す。 開いた A では: X は消え、 消えた名前は 8 (X の Delete の版数)、 Y は名前 9 で生き、 K は Y の
/// 別名でない。
///
/// 前は再生の前に K を外し (本体の X は生きていて K は退避表に居る)、 中継した `Alias` の再生が K を X の場所に張り直してその削除 (X の
/// 削除の版数) を当てた = X が自分の Delete より前に消えた (#994 [C1])。 ただしこの像の開いた後の答え (X の生死・消えた名前・K・Y) は
/// 前の形でも同じ (X の改名は削除の後の書き込みとして断られず、 X の Delete がもう一度当たる) = 前の形でも通る。 世代で決める形の守り。
#[test]
fn a_relayed_alias_before_the_reuse_keeps_the_former_resident_until_its_delete() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("relayed_alias_before_reuse");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, y, tomb, world_k) = relayed_alias_before_reuse_image(&live, &img);
    crashsim::stop();
    let db = img.join("db");
    let (ro_k, ro_slot, ro_x_alive) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (ro.resolve_remote_eid_existing(world_k), ro.eid_slot(x), ro.is_live(x))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let x_alive = eng.is_live(x);
    let y_name = eng.get(y, "t.k");
    let k_now = eng.resolve_remote_eid_existing(world_k);
    let (eight, seven) = (eng.deleted_name_version(hk, 8), eng.deleted_name_version(hk, 50));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_k, Some(enchudb_oplog::make_eid(1, ro_slot)), "前提: 像の `eidalias` は K → X の場所");
    assert!(ro_x_alive, "前提: 本体の X は生きている (X の削除と使い回しは WAL にだけ)");
    assert!(!x_alive, "前提: 再生が X を消した");
    assert_eq!(y_name, Some(9), "前提: 再生が Y (名前 9) を生かした");
    assert_ne!(k_now, Some(enchudb_oplog::make_eid(1, ro_slot)), "使い回しの前の X の別名 K が Y の別名に残った");
    assert_eq!(eight, Some(tomb), "X の改名 (名前 8) の後の削除が名前 8 の削除にならない (X が自分の Delete より前に消えた) (名前 50: {seven:x?})");
}

/// hook を外す guard (panic の巻き戻しでも外す)。
struct HookOff;

impl Drop for HookOff {
    fn drop(&mut self) {
        write_out_hook::set(None);
    }
}

thread_local! {
    /// [`c2_image`] の消す thread の印 (その thread の最初の `DeleteGateLeft` だけ止める)
    static C2_DELETER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// [C2] の像 (PR #1006 の 3 つの像の元): 別の thread の `delete` (sync する DB の行を消す) が消した行の場所の世代 (gen.seg の死の印) と
/// tomb.seg は届き、 生きている印は届かない像を、 次の `oplog_sync` の本体の書き出しの終わり (`WrittenOut`) で撮る (`Mode::Lost`)。
/// 呼び手は consumer を止め、 書き出す物を作ってから呼ぶ。 `delete` は撮る前に返る (その後に書いた sidecar も届く)。
///
/// #978 (PR #997) から本体の書き出しは、 生きている印・場所の世代・残りの列を書き出す間、 削除の門を閉じる。 前 (#1006) は生きている
/// 印の後・世代の前 (`BeforeGenerations`、 門の中) に別の thread で消して返るのを 10 秒待ち、 削除が門で待つので hook の中で panic した
/// (PR #997 に master を取り込んだ所。 hook を外す前の panic が engine を持つ hook を残し、 次の試験の `write_out_hook::set` と互いを
/// 待って power_loss が止まり続けた)。 門を閉じた後に届く場所の世代の削除は、 閉じる前に門を出た削除の `live_free` (場所の世代を
/// 上げてから生きている印を下ろす) だけ (`write_out_body` の注釈)。 そこで消す thread を門を閉じる前の段 (`PrevWrittenOut`) で始め、
/// 門を出た所 (`DeleteGateLeft`、 `live_free` の前) で止め、 生きている印を書き出した後 (`BeforeGenerations`) に放して返るのを待つ。
/// 消した行の tombstone は門を閉じる時 (`close_deletes`) に、 落とした cell は残りの列の段で書き出す。
fn c2_image(eng: &Arc<Engine>, delete: impl FnOnce() + Send + 'static, live: &Path, img: &Path) {
    enum Ev {
        /// 消す thread が門を出て止まった
        Left,
        /// 消す thread が終わった (panic でも)
        Done,
    }
    struct DoneOnDrop(std::sync::mpsc::Sender<Ev>);
    impl Drop for DoneOnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(Ev::Done);
        }
    }
    type Deleter = Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>;
    /// 消す thread が終わるのを待って join する (期限付き)。 hook を外した後に落とす = 門を出て止まったままの消す thread は、 放す合図の
    /// channel が切れて抜ける (失敗した時も、 engine と行の lock を持った thread を次の試験に残さない)
    struct JoinDeleter(Deleter);
    impl Drop for JoinDeleter {
        fn drop(&mut self) {
            let Some(h) = self.0.lock().unwrap_or_else(|p| p.into_inner()).take() else { return };
            let until = std::time::Instant::now() + std::time::Duration::from_secs(60);
            while !h.is_finished() && std::time::Instant::now() < until {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            if h.is_finished() {
                let _ = h.join();
            }
        }
    }
    let wait = std::time::Duration::from_secs(60);
    let (ev_tx, ev_rx) = std::sync::mpsc::channel::<Ev>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let (ev_rx, go_rx, delete) = (std::sync::Mutex::new(ev_rx), std::sync::Mutex::new(go_rx), std::sync::Mutex::new(Some(delete)));
    let [left, returned, done, shot] = std::array::from_fn(|_| Arc::new(AtomicBool::new(false)));
    let (left2, returned2, done2, shot2) = (left.clone(), returned.clone(), done.clone(), shot.clone());
    let (l, i) = (live.to_path_buf(), img.to_path_buf());
    // `PrevWrittenOut` を踏んだ書き出しの thread (他の段はこの thread の物だけ見る: 消す thread の中の書き出しや、 前の試験の残した
    // engine の書き出しの段で放す / 撮ると、 前提で落ちるか、 消す thread が `ev_rx` を待って止まる)
    let writer: Arc<std::sync::Mutex<Option<std::thread::ThreadId>>> = Arc::default();
    let deleter: Deleter = Arc::default();
    let (writer2, deleter2) = (writer.clone(), deleter.clone());
    let _join = JoinDeleter(deleter);
    let _off = HookOff;
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        let on_writer = *writer2.lock().unwrap() == Some(std::thread::current().id());
        match p {
            Phase::PrevWrittenOut => {
                let Some(f) = delete.lock().unwrap().take() else { return };
                *writer2.lock().unwrap() = Some(std::thread::current().id());
                let (tx, returned3) = (DoneOnDrop(ev_tx.clone()), returned2.clone());
                *deleter2.lock().unwrap() = Some(std::thread::spawn(move || {
                    let _done = tx;
                    C2_DELETER.with(|d| d.set(true));
                    f();
                    returned3.store(true, Ordering::SeqCst);
                }));
                // 門を出て止まるか、 (門を通らずに) 終わるまで
                if let Ev::Left = ev_rx.lock().unwrap().recv_timeout(wait).expect("消す thread が 60 秒、 門を出ず終わりもしない") {
                    left2.store(true, Ordering::SeqCst);
                }
            }
            Phase::DeleteGateLeft if C2_DELETER.with(|d| d.replace(false)) => {
                let _ = ev_tx.send(Ev::Left);
                go_rx.lock().unwrap().recv_timeout(wait).expect("書き出しが 60 秒、 門を出た消す thread を放さない");
            }
            Phase::BeforeGenerations if on_writer && left2.load(Ordering::SeqCst) && !done2.load(Ordering::SeqCst) => {
                let _ = go_tx.send(());
                let ev = ev_rx.lock().unwrap().recv_timeout(wait).expect("放した消す thread が 60 秒で終わらない");
                assert!(matches!(ev, Ev::Done), "消す thread がもう一度止まった");
                done2.store(true, Ordering::SeqCst);
            }
            Phase::WrittenOut if on_writer && done2.load(Ordering::SeqCst) && !shot2.swap(true, Ordering::SeqCst) => {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
            _ => {}
        }
    })));
    eng.oplog_sync().unwrap();
    drop(_off);
    drop(_join);
    assert!(left.load(Ordering::SeqCst), "前提: 消す thread が削除の門を出た所で止まった (書き出しが門を閉じる前に)");
    assert!(returned.load(Ordering::SeqCst), "前提: 消す thread が (panic せずに) 返った");
    assert!(done.load(Ordering::SeqCst) && shot.load(Ordering::SeqCst), "前提: 生きている印の書き出しの後に消し終え、 書き出しの終わりで撮った");
}

/// PR #997 に master (#1006) を取り込んだ所で power_loss が約 21 時間止まった形: hook を外す前に panic した試験は、 engine の `Arc` を
/// 持つ hook を残す。 次の試験の `write_out_hook::set` がその hook を落とすと engine の最後の `Arc` が落ち、 engine の Drop は consumer を
/// 待つ。 consumer の最後の書き出しは `write_out_hook::call` で hook の lock を取る。 前の `set` は lock を持ったまま前の hook を落とした
/// ので互いを待ち、 後の試験は全部 `SERIAL` で待った。 ここでは engine の最後の `Arc` を持つ hook を置いて外す (前の形では外す thread が
/// 返らない)。 60 秒で返らなければ process ごと落とす (外す thread は hook の lock を持ったまま止まっているので、 試験を落とすだけでは
/// この binary の後の試験の書き出しが全部 `write_out_hook::call` で止まる = 21 時間止まった形を作り直す)。
#[test]
fn removing_a_hook_that_holds_the_last_engine_returns() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("hook_holds_last_engine");
    {
        let (eng, x) = full_named_table_engine(&root.join("db"));
        let e2 = eng.clone();
        write_out_hook::set(Some(Arc::new(move |_p: Phase| {
            let _ = &e2;
        })));
        // consumer の最後の書き出しが段を通るように、 書き出していない物を残す
        eng.tie_to(x, "t.v", 71u32);
        eng.oplog_commit();
    } // ここから hook が engine の最後の `Arc`
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        write_out_hook::set(None);
        let _ = tx.send(());
    });
    if rx.recv_timeout(std::time::Duration::from_secs(60)).is_err() {
        // 試験の出力の受け (`eprintln!`) は abort で捨てられるので、 stderr に直に書く
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr(),
            "removing_a_hook_that_holds_the_last_engine_returns: engine の最後の `Arc` を持つ hook を外す `write_out_hook::set` が 60 秒返らない \
             (engine の consumer の最後の書き出しと互いを待った)。 外す thread が hook の lock を持ったまま止まっていて、 この binary の後の試験も \
             止まるので process ごと落とす"
        );
        std::process::abort();
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// #994 [C2] / Q6 の像: 名前 7 の自分の行 X と、 作成より先に削除 (t_k) を受けた B の世界番号 K (退避表に居る、 #490)。 K を X に束ねる
/// (`apply_alias`: K は X の場所の別名になり、 退避表の削除で X が消える = 死んだ場所の正しい別名)。 `power_loss`: 本体の書き出しの
/// 途中で束ね (束ねの削除は生きている印の書き出しの後に場所の世代を上げる、 [`c2_image`])、 sidecar を書き出してから、 本体の書き出しの
/// 終わり (`WrittenOut`) で電源断 (gen.seg の死の印と tomb.seg は届き、 生きている印は届かない)。 偽なら束ねて書き出した後の kill の像。
/// (X, K, t_k) を返す。
fn bound_dead_alias_image(live: &Path, img: &Path, power_loss: bool) -> (u64, u64, Hlc) {
    let (eng, x) = full_named_table_engine(&live.join("db"));
    let world_k = enchudb_oplog::make_eid(3, 23);
    let t_k = Hlc { wall: eng.cell_hlc(x, eng.himo_id("t.k").unwrap() as u16).wall + 10, logical: 0, peer: 3 };
    eng.remote_delete_apply_foreign(world_k, t_k);
    assert_eq!(eng.deleted_before_create(world_k), Some(t_k), "前提: K の削除は作成より先に退避表へ");
    eng.persist_sync_state().unwrap();
    let bind = {
        let e2 = eng.clone();
        move || {
            assert!(e2.apply_alias(world_k, x), "前提: K を X に束ねた");
            assert!(!e2.is_live(x), "前提: 退避表の削除で X が消えた");
            e2.persist_tables().unwrap();
        }
    };
    if power_loss {
        let _hold = eng.hold_consumer();
        // 書き出す物を作る (本体の書き出しが段を通る)
        let z = eng.entity_in("t").unwrap();
        eng.tie_to(z, "t.k", 3u32);
        eng.oplog_commit();
        c2_image(&eng, bind, live, img);
    } else {
        bind();
        eng.oplog_sync().unwrap();
        kill_image(live, img);
    }
    (x, world_k, t_k)
}

/// #994 [C2] / Q6: `bound_dead_alias_image` を開く。 K は消えた X の正しい別名 (指す行の世代は X の世代 = 場所の最後の行) なので外さず、
/// 場所の名前の記録 (X の名前 7 の削除) も K に移さない。
///
/// 前は再生の前の 1 か所が突き合わせの前の生死で場所を見て、 電源断の像 (生きている印だけ届かない) で S を生きていると読み、 K (退避表に
/// 居る) を外して X の名前の記録を K に移した (#915 の型)。 kill の像は死んだ場所を飛ばしたので通る (変異 Q6 = 死んだ場所も見る形で落ちる)。
///
/// `reopen` (PR #1006 の読み直し [1]): 開いた A の kill の像をもう一度開いても同じ。 電源断の像は、 突き合わせが削除を完了させる時に
/// 場所の世代を 2 つ上げる (X の世代 + 2) ので、 開いた後の場所の読み (`last_local_of_slot`) は X + 1 (無かった行) になった (#1008 の前。
/// 今は gen.seg の 「最後の行は 今 − 2」 の印で X)。 前は
/// 2 回目の open が K (X の世代) を 「場所の最後の行より下 = 前の住人の別名」 と読み、 退避表の K の削除の版数で X の名前 7 の記録を K に
/// 移して外した (K の写像も消えた)。
fn a_bound_alias_of_a_dead_row_is_kept(tag: &str, power_loss: bool, reopen: bool) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img, img2) = (root.join("live"), root.join("img"), root.join("img2"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, world_k, t_k) = bound_dead_alias_image(&live, &img, power_loss);
    crashsim::stop();
    let db = img.join("db");
    let (ro_k, ro_slot) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (ro.resolve_remote_eid_existing(world_k), ro.eid_slot(x))
    };
    let read = |eng: &Engine| {
        let hk = eng.himo_id("t.k").unwrap() as u16;
        (
            eng.is_live(x),
            eng.resolve_remote_eid_existing(world_k),
            eng.deleted_name_version(hk, 7),
            eng.deleted_before_create(world_k),
        )
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let first = read(&eng);
    let repairs = eng.generation_repairs();
    if reopen {
        // 開いた A を kill した像 (page cache に開く道の直しが届く、 きれいに閉じた印は立っていない)
        kill_image(&img, &img2);
    }
    drop(eng);
    let second = reopen.then(|| {
        let eng = Engine::open_concurrent_with_oplog(img2.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        read(&eng)
    });
    let _ = std::fs::remove_dir_all(&root);
    let at_slot = Some(enchudb_oplog::make_eid(1, ro_slot));
    assert_eq!(ro_k, at_slot, "前提: 像の `eidalias` は K → X の場所");
    // 電源断の像: gen.seg の死の印は届き、 生きている印は届いていない (突き合わせが削除を完了させた)
    assert_eq!(repairs.0, u32::from(power_loss), "前提: 本体の生きている印と gen.seg の死の印 ({tag}): {repairs:?}");
    for (n, (x_alive, k_now, seven, k_tomb)) in std::iter::once(first).chain(second).enumerate() {
        let n = n + 1;
        assert!(!x_alive, "前提: 開いた A で X は消えている ({n} 回目)");
        assert_eq!(k_now, at_slot, "消えた X の正しい別名 K を外した ({tag}、 {n} 回目の open)");
        assert_eq!(k_tomb, Some(t_k), "前提: K の削除は退避表に残る ({tag}、 {n} 回目)");
        assert_eq!(seven, Some(t_k), "X の名前 7 の削除が残らない ({tag}、 {n} 回目の open)");
    }
}

#[test]
fn a_bound_alias_of_a_dead_row_is_kept_after_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_bound_alias_of_a_dead_row_is_kept("bound_dead_alias_power_loss", true, false);
}

#[test]
fn a_bound_alias_of_a_dead_row_is_kept_after_kill() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_bound_alias_of_a_dead_row_is_kept("bound_dead_alias_kill", false, false);
}

/// PR #1006 の読み直し [1]: 上の電源断の像を開いた A を kill して、 もう一度開く。
#[test]
fn a_bound_alias_of_a_dead_row_is_kept_when_reopened_after_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_bound_alias_of_a_dead_row_is_kept("bound_dead_alias_power_loss_reopen", true, true);
}

/// PR #1006 の読み直し [1] の、 別名の identity が退避表に居ない形: 翻訳した行 F (C の行、 B の行 b を名前で畳んだ = b の世界番号は
/// F の場所の別名) を C の削除で消す。 消すのは本体の書き出しの途中で、 場所の世代を上げるのは生きている印の書き出しの後
/// ([`c2_image`])。 書き出しの終わり (`WrittenOut`) で電源断 (gen.seg の死の印は届き、 生きている印は届かない)。 F を返す。
fn dead_translated_alias_image(live: &Path, img: &Path) -> u64 {
    let (eng, f) = translated_row_with_alias_engine(&live.join("db"));
    let _hold = eng.hold_consumer();
    // 書き出す物を作る (本体の書き出しが段を通る)
    let z = eng.entity_in("t").unwrap();
    eng.tie_to(z, "t.c", 3u32);
    eng.oplog_commit();
    let e2 = eng.clone();
    c2_image(&eng, move || assert!(e2.remote_delete_apply_foreign(WORLD_F, F_DELETE), "前提: C の削除が F に当たった"), live, img);
    f
}

/// PR #1006 の読み直し [1]: `dead_translated_alias_image` を開き、 開いた A を kill してもう一度開く。 b は消えた F の別名のまま (F の場所を
/// 指す) で、 B の遅れた書き込み (F の削除を知らない、 b 宛て) は死んだ場所に当たって捨てられる (新しい行にならない)。
///
/// 前は 2 回目の open が b (F の世代) を 「場所の最後の行 (F + 1、 突き合わせが世代を 2 つ上げた) より下」 と読んで外し、 b の identity は
/// 退避表に居ないので退避もせず、 B の遅れた書き込みが b に新しい生きた行を払い出した (幽霊行。 master は死んだ場所の別名を見ないので
/// b を残した)。
#[test]
fn an_alias_of_a_dead_translated_row_is_kept_when_reopened_after_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("dead_translated_alias_reopen");
    let (live, img, img2) = (root.join("live"), root.join("img"), root.join("img2"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let f = dead_translated_alias_image(&live, &img);
    crashsim::stop();
    let f_slot = {
        let ro = Engine::open_readonly(img.join("db").to_str().unwrap()).unwrap();
        let s = ro.eid_slot(f);
        assert_eq!(ro.resolve_remote_eid_existing(WORLD_B2).map(|e| ro.eid_slot(e)), Some(s), "前提: 像の `eidalias` は b → F の場所");
        s
    };
    // (b の行き先の場所, F が生きているか, 突き合わせの直した数, b 宛ての遅れた書き込みが当たったか / 生きた行になったか)
    let read = |eng: &Engine, late: bool| {
        let b_at = eng.resolve_remote_eid_existing(WORLD_B2).map(|e| eng.eid_slot(e));
        let repaired = eng.generation_repairs();
        let landed = late.then(|| {
            let hid = eng.himo_id("t.c").unwrap() as u16;
            let at = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
            let t = eng.resolve_remote_eid(WORLD_B2, hid);
            let applied = t.is_some_and(|t| eng.remote_tie_apply(t, hid, 4u32, at));
            (applied, t.is_some_and(|t| eng.is_live(t)))
        });
        (b_at, eng.is_live(f), repaired, landed)
    };
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let first = read(&eng, false);
    kill_image(&img, &img2);
    drop(eng);
    let eng = Engine::open_concurrent_with_oplog(img2.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let second = read(&eng, true);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(first.2 .0, 1, "前提: 突き合わせが F の削除を完了させた (世代を 2 つ上げた): {:?}", first.2);
    assert!(!first.1 && !second.1, "前提: 開いた A で F は消えている");
    assert_eq!(first.0, Some(f_slot), "消えた F の別名 b を外した (1 回目の open)");
    assert_eq!(second.3, Some((false, false)), "b 宛ての遅れた書き込みが当たった / 生きた行になった (幽霊行)");
    assert_eq!(second.0, Some(f_slot), "消えた F の別名 b を外した (2 回目の open)");
}

/// #994 [C3]: `eidalias_behind_image` の電源断の像 (本体の X は生きている、 WAL に X の Delete と Y)。 再生が X の Delete で場所の名前の
/// 記録 (名前 7 を消した) を作り、 Y の record で使い回しの後始末をやり直す時、 名前を前の住人 X の identity (自分, X) に移す (外す別名 b
/// にも)。 開いた後に (自分, X) 宛ての、 より新しい削除を受けると、 名前 7 の削除の時刻が上がる (#915、 identity が名前を持つ)。
///
/// 前は、 記録の無い像 (`BeforeGate`) の後始末のやり直しが 「退避は書き出し済み」 (退避表に X の削除の版数) で前の住人を読まずに返り、
/// 名前を移さなかった (場所の記録が Y の場所に残り、 (自分, X) は名前を持たない)。
fn a_former_residents_name_goes_to_its_identity(tag: &str, how: EidaliasBehind) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let world_b = enchudb_oplog::make_eid(3, 9);
    let (x, y, tomb) = eidalias_behind_image(&live, &img, world_b, how, false);
    crashsim::stop();
    let db = img.join("db");
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let y_alive = eng.is_live(y);
    let before = eng.deleted_name_version(hk, 7);
    let newer = Hlc { wall: tomb.wall + 1_000, logical: 0, peer: 2 };
    eng.remote_delete_apply_foreign(x, newer);
    let after = eng.deleted_name_version(hk, 7);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(y_alive, "前提: 開いた A で Y が生きている ({how:?})");
    assert_eq!(before, Some(tomb), "前提: 名前 7 は X の削除の版数で消えた名前 ({how:?})");
    assert_eq!(after, Some(newer), "(自分, X) 宛ての新しい削除が名前 7 の削除の時刻を上げない = 名前が前の住人の identity に移っていない ({how:?})");
}

#[test]
fn a_former_residents_name_goes_to_its_identity_on_redo_with_the_record() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_residents_name_goes_to_its_identity("names_redo_record", EidaliasBehind::PowerLoss(Phase::PrevWrittenOut));
}

#[test]
fn a_former_residents_name_goes_to_its_identity_on_redo_without_a_record() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_former_residents_name_goes_to_its_identity("names_redo_no_record", EidaliasBehind::PowerLoss(Phase::BeforeGate));
}

/// PR #1006 の読み直し [2] の、 C の行 (受けた行) と B の行の世界番号。
const WORLD_Y2: u64 = (3u64 << 32) | 11;
const WORLD_C2: u64 = (2u64 << 32) | 12;

/// PR #1006 の読み直し [2] の像: 名前 7 の自分の行 X を消して書き出した後、 C の行 Y (受けた行 = 自分の WAL に載らない) が X の場所を
/// 使い回し、 B の行 c を Y に畳む (c の世界番号は Y の場所の別名、 指す行の世代は Y の世代)。 sidecar を書き出してから本体を書き出し、
/// 生きている印の後・世代の前 (`BeforeGenerations`) で電源断 (Y の生きている印は届き、 gen.seg は X の削除のまま = 死の印が残る)。
/// Y を返す。
fn killed_resident_alias_image(live: &Path, img: &Path) -> u64 {
    let (eng, x) = full_named_table_engine(&live.join("db"));
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let _hold = eng.hold_consumer();
    eng.delete(x);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    let y = eng.resolve_remote_eid(WORLD_Y2, hv).unwrap();
    assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: 受けた Y が X の場所を使い回した");
    assert!(eng.remote_tie_apply_result(y, hv, 9u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
    let c = eng.resolve_remote_eid(WORLD_C2, hv).unwrap();
    assert!(eng.remote_tie_apply_result(c, hv, 8u32, Hlc { wall: 1_001, logical: 0, peer: 2 }).applied());
    eng.merge_entity_into(c, y).unwrap();
    assert_eq!(
        eng.resolve_remote_eid_existing(WORLD_C2).map(|e| eng.eid_slot(e)),
        Some(eng.eid_slot(y)),
        "前提: c は Y の場所の別名"
    );
    eng.persist_tables().unwrap();
    let shot = Arc::new(AtomicBool::new(false));
    let (s2, l, i) = (shot.clone(), live.to_path_buf(), img.to_path_buf());
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::BeforeGenerations && !s2.swap(true, Ordering::SeqCst) {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
        }
    })));
    // 書き出す物を作る (本体の書き出しが段を通る)
    let z = eng.entity_in("t").unwrap();
    eng.tie_to(z, "t.k", 3u32);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: 本体の書き出しの途中で撮った");
    y
}

/// PR #1006 の読み直し [2]: `killed_resident_alias_image` を開くと、 突き合わせが Y を消す (生きている印と死の印 = 使い回しの像も削除の
/// 途中の像も 「死が勝つ」、 Y の record は WAL に無い)。 c は消えた Y の別名のまま Y の場所を指し、 B の遅れた書き込み (c 宛て) は新しい
/// 生きた行を払い出さない。 開いた A を kill してもう一度開いても同じ。
///
/// #992 の読み直し 2 の細かい 3 (PR #1012): 突き合わせは使い回しの途中の像 (Y = 場所を使い回して生かす途中の行) に tombstone を作り直さない
/// (Y の Delete を配らない: Y は C の行で C では生きている。 自分の時計の tombstone は自分の state に 「他の peer の行の削除」 (#766) として
/// 出る)。 tombstone の無い死んだ場所なので、 遅れた書き込みは死んだ場所の cell には当たる (版数なしで消した行と同じ形)。 前 (master /
/// PR #1012 の最初の 2 つの head) は tombstone を今の時刻で置き、 Delete を前の住人 X (master) / Y の世界番号 (PR の 2 つ目) 宛てに配った。
///
/// 前は開く道の仕上げが c を 「場所の最後の行 (突き合わせが仕上げで覚えた X の世代) より上の世代 = 本体にも WAL にも無い行」 と読んで
/// 退避なしで外し、 B の遅れた書き込みが c に新しい生きた行を払い出した (master は c を死んだ場所に残し、 書き込みは捨てられた)。
#[test]
fn an_alias_of_a_resident_the_open_killed_is_kept() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("killed_resident_alias");
    let (live, img, img2) = (root.join("live"), root.join("img"), root.join("img2"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let y = killed_resident_alias_image(&live, &img);
    crashsim::stop();
    let y_slot = {
        let ro = Engine::open_readonly(img.join("db").to_str().unwrap()).unwrap();
        let s = ro.eid_slot(y);
        assert_eq!(ro.resolve_remote_eid_existing(WORLD_C2).map(|e| ro.eid_slot(e)), Some(s), "前提: 像の `eidalias` は c → Y の場所");
        s
    };
    // (c の行き先の場所, Y が生きているか, 突き合わせの直した数, c 宛ての遅れた書き込み (当たったか, 生きた行になったか))
    let read = |eng: &Engine, late: bool| {
        let c_at = eng.resolve_remote_eid_existing(WORLD_C2).map(|e| eng.eid_slot(e));
        let landed = late.then(|| {
            let hv = eng.himo_id("t.v").unwrap() as u16;
            let at = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
            let t = eng.resolve_remote_eid(WORLD_C2, hv);
            let applied = t.is_some_and(|t| eng.remote_tie_apply(t, hv, 4u32, at));
            (applied, t.is_some_and(|t| eng.is_live(t)))
        });
        (c_at, eng.is_live(y), eng.generation_repairs(), landed)
    };
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let first = read(&eng, false);
    kill_image(&img, &img2);
    drop(eng);
    let eng = Engine::open_concurrent_with_oplog(img2.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let second = read(&eng, true);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(first.2 .0, 1, "前提: 突き合わせが Y を消した (生きている印と死の印): {:?}", first.2);
    assert!(!first.1 && !second.1, "前提: 開いた A で Y は消えている");
    assert_eq!(first.0, Some(y_slot), "突き合わせが消した Y の別名 c を外した (1 回目の open)");
    assert_eq!(second.3.map(|(_, live)| live), Some(false), "c 宛ての遅れた書き込みが生きた行になった (幽霊行)");
    assert_eq!(second.0, Some(y_slot), "突き合わせが消した Y の別名 c を外した (2 回目の open)");
}

/// 本体より先に `eidalias` が届いた像: 名前 7 の自分の行 X (書き出し済み) を消して、 自分の行 Y で X の場所を使い回し、 B の行 c を Y に
/// 畳んで (c は Y の場所の別名、 指す行の世代は Y の世代 = X + 1) sidecar だけを書き出し (`persist_tables`、 本体と WAL は書き出さない)、
/// 電源断。 X を返す。
fn alias_ahead_of_the_body_image(live: &Path, img: &Path) -> u64 {
    let (eng, x) = full_named_table_engine(&live.join("db"));
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let _hold = eng.hold_consumer();
    eng.delete(x);
    eng.oplog_commit();
    let y = add_seven(&eng, 71);
    assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y が X の場所を使い回した");
    let c = eng.resolve_remote_eid(WORLD_C2, hv).unwrap();
    assert!(eng.remote_tie_apply_result(c, hv, 8u32, Hlc { wall: 1_001, logical: 0, peer: 2 }).applied());
    eng.merge_entity_into(c, y).unwrap();
    eng.persist_tables().unwrap();
    let _ = crashsim::capture(live, img, Mode::Lost);
    x
}

/// `alias_ahead_of_the_body_image` を開くと、 本体の X は生きている (X の削除と Y は本体にも WAL にも無い)。 c (Y の世代を指す) は X の
/// 別名でない: 開く道の仕上げが、 生きている場所の住人より上の世代の別名を外す。 B の遅れた書き込み (c 宛て) は X に当たらない。
/// (読み直し [6]: 前は `eidalias` の世代を手で書き換えた像だけがこの段を確かめていた)
#[test]
fn an_alias_ahead_of_the_body_is_dropped_from_the_live_former_resident() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("alias_ahead_of_body");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let x = alias_ahead_of_the_body_image(&live, &img);
    crashsim::stop();
    let (ro_c, ro_slot) = {
        let ro = Engine::open_readonly(img.join("db").to_str().unwrap()).unwrap();
        (ro.resolve_remote_eid_existing(WORLD_C2).map(|e| ro.eid_slot(e)), ro.eid_slot(x))
    };
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let x_alive = eng.is_live(x);
    let c_at = eng.resolve_remote_eid_existing(WORLD_C2).map(|e| eng.eid_slot(e));
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let at = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
    if let Some(t) = eng.resolve_remote_eid(WORLD_C2, hv) {
        eng.remote_tie_apply(t, hv, 4u32, at);
    }
    let x_v = eng.get(x, "t.v");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_c, Some(ro_slot), "前提: 像の `eidalias` は c → X の場所 (Y の世代)");
    assert!(x_alive, "前提: 本体の X は生きている");
    assert_ne!(c_at, Some(ro_slot), "本体に無い Y の別名 c が、 生きている X の別名に残った");
    assert_eq!(x_v, Some(70), "c 宛ての遅れた書き込みが X に当たった");
}

/// PR #1006 の読み直し [3] の B の行の世界番号。
const WORLD_B3: u64 = (2u64 << 32) | 13;

/// `dir` (DB の directory) の segment のうち、 `prev.seg`・tomb.seg・版数の列 (`ver/`) を除いた全部を 「書き出し済み」 として控える
/// (crashsim。 OS が msync の前にその page を書き出した像)。
fn mark_written_early_but_records(dir: &Path) {
    for ent in std::fs::read_dir(dir).unwrap() {
        let ent = ent.unwrap();
        let (path, name) = (ent.path(), ent.file_name().to_string_lossy().into_owned());
        if ent.file_type().unwrap().is_dir() {
            if name != "ver" {
                mark_written_early_but_records(&path);
            }
        } else if name.ends_with(".seg") && name != "prev.seg" && name != "tomb.seg" {
            crashsim::file_synced(&path);
        }
    }
}

/// review946 [3] の窓の像 (PR #1006 の 3 つの像の元): 次の `oplog_sync` の本体の書き出しで `prev.seg` を書き出した後
/// (`PrevWrittenOut`) に別の thread で `reuse` (場所の使い回し) を走らせ、 返った後に、 OS が `prev.seg`・tomb.seg・版数の列より先に
/// 残りの segment (生きている印・場所の世代・列 …) の page を書き出した像を撮る (`Mode::Lost`)。 本体には使い回した住人が届き、
/// 使い回しの記録 (`prev.seg`)・tomb.seg・版数と、 WAL の書き出しの後の record は届かない。 呼び手は consumer を止め、 書き出す物を
/// 作ってから呼ぶ。 `reuse` の返り値を返す。
///
/// 前 (#1006) は `PrevWrittenOut` で使い回して書き出しの段 `BeforeVersions` (残りの列まで書き出した所) で撮った。 #978 (PR #997) から
/// は、 その間に削除の門を閉じる所 (`close_deletes` → `write_out_tombs`) が `prev.seg` をもう一度書き出すので、 書き出しの段で撮る像
/// には使い回しの記録も届く。 この窓が書き出しの段の像に現れるのは門を閉じた後 (`DeletesClosed`) の使い回しだけで、 その時は前の
/// 住人の tombstone も (門を閉じる時の tomb.seg で) 届いている別の像になる (開くと tombstone で使い回した住人が消え、 古い別名を通る
/// record を見られない)。 前と同じ像 (記録も前の住人の tombstone も届かず、 使い回した住人は届く) は、 この PR の下では OS が msync の
/// 前に page を書いた像でだけ残るので、 その形で撮る (page を乱数で選ぶ `Mode::Mixed` でなく、 前の書き出しの段の像が届けていた
/// segment を決めて書き出し済みにする)。
fn reuse_window_image<T: Send + 'static>(eng: &Arc<Engine>, reuse: impl FnOnce() -> T + Send + 'static, live: &Path, img: &Path) -> T {
    let out: Arc<std::sync::Mutex<Option<T>>> = Arc::new(std::sync::Mutex::new(None));
    let shot = Arc::new(AtomicBool::new(false));
    let (o2, s2, l, i) = (out.clone(), shot.clone(), live.to_path_buf(), img.to_path_buf());
    let reuse = std::sync::Mutex::new(Some(reuse));
    let _off = HookOff;
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p != Phase::PrevWrittenOut {
            return;
        }
        let Some(f) = reuse.lock().unwrap().take() else { return };
        let (tx, rx) = std::sync::mpsc::channel::<std::thread::Result<T>>();
        std::thread::spawn(move || {
            let _ = tx.send(std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)));
        });
        match rx.recv_timeout(std::time::Duration::from_secs(60)).expect("前提: 使い回しが 60 秒で返らない") {
            Ok(v) => *o2.lock().unwrap() = Some(v),
            Err(p) => std::panic::resume_unwind(p),
        }
        mark_written_early_but_records(&l.join("db"));
        let _ = crashsim::capture(&l, &i, Mode::Lost);
        s2.store(true, Ordering::SeqCst);
    })));
    eng.oplog_sync().unwrap();
    drop(_off);
    assert!(shot.load(Ordering::SeqCst), "前提: 書き出しの途中で使い回して撮った");
    let v = out.lock().unwrap().take();
    v.expect("前提: 使い回しが返った")
}

/// PR #1006 の読み直し [3] の像: 名前 7 の自分の行 X に B の行 b を畳み (b は X の場所の別名、 `eidalias` に書き出す)、 X に書いて
/// (WAL の checkpoint の後ろに X の record)、 `oplog_sync` の本体の書き出しの途中で X を消して Y で X の場所を使い回した像
/// ([`reuse_window_image`])。 本体には Y が届き (生きている印と gen.seg)、 使い回しの記録 (`prev.seg`) と X の削除・Y の record (WAL)
/// は届かない (review946 [3] の窓)。 X を返す。
fn reuse_without_record_image(live: &Path, img: &Path) -> u64 {
    let (eng, x) = full_named_table_engine(&live.join("db"));
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let b = eng.resolve_remote_eid(WORLD_B3, hv).unwrap();
    assert!(eng.remote_tie_apply_result(b, hv, 8u32, Hlc { wall: 1_001, logical: 0, peer: 2 }).applied());
    eng.merge_entity_into(b, x).unwrap();
    // b を受けて伸びた表を使い切る (使い切った表は、 消した場所を次の行に使い回す)
    let mut n = 300u32;
    while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity) {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.k", n);
        n += 1;
    }
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let _hold = eng.hold_consumer();
    eng.tie_to(x, "t.v", 71u32);
    eng.oplog_commit();
    let e2 = eng.clone();
    let y = reuse_window_image(
        &eng,
        move || {
            e2.delete(x);
            add_seven(&e2, 72)
        },
        live,
        img,
    );
    assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y が X の場所を使い回した");
    x
}

/// PR #1006 の読み直し [3]: `reuse_without_record_image` を開くと、 WAL の再生が X の record で場所を X に戻す (使い回しの記録が無いので
/// 再生の守りは当てる。 Y は本体にだけある)。 b は生き返った X の別名のまま。
///
/// 前は再生の前の外しが、 本体の場所の住人 Y より下の世代の b を外した (退避もしない): X は生きているのに b だけが写像を失い、 B の
/// 遅れた書き込み (b 宛て) が新しい行になった。
#[test]
fn an_alias_of_a_former_resident_the_replay_brings_back_is_kept() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("reuse_without_record");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let x = reuse_without_record_image(&live, &img);
    crashsim::stop();
    let (ro_b, ro_slot, ro_record, ro_x_alive) = {
        let ro = Engine::open_readonly(img.join("db").to_str().unwrap()).unwrap();
        let s = ro.eid_slot(x);
        (ro.resolve_remote_eid_existing(WORLD_B3).map(|e| ro.eid_slot(e)), s, ro.prev_resident_of_slot(s), ro.is_live(x))
    };
    let eng = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let x_alive = eng.is_live(x);
    let x_v = eng.get(x, "t.v");
    let b_at = eng.resolve_remote_eid_existing(WORLD_B3).map(|e| eng.eid_slot(e));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_b, Some(ro_slot), "前提: 像の `eidalias` は b → X の場所");
    assert!(!ro_x_alive, "前提: 本体の場所の住人は Y (X は使い回された)");
    assert!(ro_record.is_none_or(|r| r.1 != eid_local(x)), "前提: 使い回しの記録 (前の住人 X) は届いていない: {ro_record:x?}");
    assert!(x_alive && x_v == Some(71), "前提: 再生が X を戻した (v = {x_v:?})");
    assert_eq!(b_at, Some(ro_slot), "再生が戻した X の別名 b を、 再生の前に外した");
}

/// PR #1006 の読み直し [1] の死んだ場所の決め方の、 外す側の B の行の世界番号。
const WORLD_B4: u64 = (2u64 << 32) | 14;

/// 名前 7 の自分の行 X に B の行 b を畳み (b は X の場所の別名、 `eidalias` に書き出す)、 X を消して Y で X の場所を使い回し (使い回しが b を
/// 外して X の削除の版数を退避する、 `prev.seg` に前の住人 X の記録)、 Y も消す。 sidecar の書き出しを `.eidmap` の後・`eidalias` の前で
/// kill (page cache は残る: 場所は Y の削除で死んでいる、 `eidalias` は使い回しの前 = b → X の場所)。 (X, X の削除の版数) を返す。
fn dead_slot_former_alias_image(live: &Path, img: &Path) -> (u64, Hlc) {
    let (eng, x) = full_named_table_engine(&live.join("db"));
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let b = eng.resolve_remote_eid(WORLD_B4, hv).unwrap();
    assert!(eng.remote_tie_apply_result(b, hv, 8u32, Hlc { wall: 1_001, logical: 0, peer: 2 }).applied());
    eng.merge_entity_into(b, x).unwrap();
    // b を受けて伸びた表を使い切る (使い切った表は、 消した場所を次の行に使い回す)
    let mut n = 300u32;
    while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity) {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.k", n);
        n += 1;
    }
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let _hold = eng.hold_consumer();
    eng.delete(x);
    eng.oplog_commit();
    let t_x = eng.tombstone_hlc(x);
    assert_ne!(t_x, Hlc::ZERO, "前提: X の削除の版数");
    let y = add_seven(&eng, 72);
    assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y が X の場所を使い回した");
    assert_eq!(eng.resolve_remote_eid_existing(WORLD_B4), None, "前提: 使い回しが b を外した (memory)");
    eng.delete(y);
    eng.oplog_commit();
    let shot = Arc::new(AtomicBool::new(false));
    let (s2, l, i) = (shot.clone(), live.to_path_buf(), img.to_path_buf());
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::EidmapWritten && !s2.swap(true, Ordering::SeqCst) {
            kill_image(&l, &i);
        }
    })));
    eng.persist_tables().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: sidecar の書き出しの途中で撮った");
    (x, t_x)
}

/// PR #1006 の読み直し [1] の死んだ場所の決め方 (外す側): `dead_slot_former_alias_image` を開くと、 場所は死んでいて (Y の削除)、
/// `eidalias` の b は X の世代を指す。 `prev.seg` の使い回しの記録が X を場所から離したので、 b は前の住人の別名として外す (死んだ場所でも)。
/// その後に場所を Z で使い回しても、 b の削除の版数は X の削除のまま (外さないと、 Z の使い回しが b を場所の削除 = Y の削除で退避し直し、
/// b の削除の版数が Y の物に上がる)。
///
/// この像で b を外すのは記録から戻す道 (書き出していない周期の記録、 `restore_prev_resident`) が先で、 死んだ場所のなめ
/// (`alias_row_left_slot`) は同じ別名の受け止め (変異で記録を見ないなめにしても通る)。 `eidalias` が使い回しの前のまま残る像は、 その
/// 記録の周期を書き出していない (周期は `eidalias` を書いた後に進む) ので、 なめだけが外す像は作れていない。
#[test]
fn a_former_alias_at_a_dead_slot_is_dropped_by_the_reuse_record() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("dead_slot_former_alias");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    let (x, t_x) = dead_slot_former_alias_image(&live, &img);
    let db = img.join("db");
    let (ro_b, ro_slot, ro_record) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        let s = ro.eid_slot(x);
        (ro.resolve_remote_eid_existing(WORLD_B4).map(|e| ro.eid_slot(e)), s, ro.prev_resident_of_slot(s))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let b_at = eng.resolve_remote_eid_existing(WORLD_B4).map(|e| eng.eid_slot(e));
    let z = add_seven(&eng, 73);
    let z_slot = eng.eid_slot(z);
    let b_tomb = eng.deleted_before_create(WORLD_B4);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_b, Some(ro_slot), "前提: 像の `eidalias` は b → X の場所 (使い回しの前)");
    assert!(ro_record.is_some_and(|r| r.1 == eid_local(x)), "前提: 場所の記録は前の住人 X: {ro_record:x?}");
    assert_eq!(z_slot, ro_slot, "前提: Z が場所を使い回した");
    assert_ne!(b_at, Some(ro_slot), "使い回しが外した X の別名 b が、 死んだ場所に残った");
    assert_eq!(b_tomb, Some(t_x), "b の削除の版数が X の削除でない (後の使い回しが場所の削除で退避し直した)");
}

/// PR #1006 の 2 回目の読み直し [A] の像: 翻訳した行 F (B の行 b を名前で畳んだ = b の世界番号は F の場所の別名、 b は退避表に居ない) を
/// C の削除で消し、 本体 (F は死) と sidecar (`.eidmap` の (C, f) → F の場所、 `eidalias` の b → F の場所) を書き出す (周期 k)。 次の周期の
/// 前に Y で F の場所を使い回し (`prev.seg` に前の住人 F の記録、 memory では (C, f) と b を外して退避)、 その `oplog_sync` の本体の書き出しで
/// `prev.seg` を書き出した後・生きている印の前 (`PrevWrittenOut`) に電源断。 像は 「本体の場所は死んだ F のまま、 使い回しの記録だけが
/// 届いた、 sidecar は使い回しの前、 WAL に Y の record」 (consumer は sidecar を本体の書き出しの後に書く)。 (F, Y) を返す。
fn translated_reuse_record_only_image(live: &Path, img: &Path) -> (u64, u64) {
    let (eng, f) = translated_row_with_alias_engine(&live.join("db"));
    let f_slot = eng.eid_slot(f);
    let _hold = eng.hold_consumer();
    assert!(eng.remote_delete_apply_foreign(WORLD_F, F_DELETE), "前提: C の削除が F に当たった");
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "前提: `.eidmap` は memory と同じ ((C, f) → F の場所)");
    let y = eng.entity_in("t").unwrap();
    eng.tie_to(y, "t.c", 9u32);
    eng.oplog_commit();
    assert_eq!(eng.eid_slot(y), f_slot, "前提: Y は F の場所を使い回した");
    assert_eq!(eng.resolve_remote_eid_existing(WORLD_B2), None, "前提: 使い回しが b を外した (memory)");
    let shot = Arc::new(AtomicBool::new(false));
    let (s2, l, i) = (shot.clone(), live.to_path_buf(), img.to_path_buf());
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
        }
    })));
    eng.oplog_sync().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: 本体の書き出しの途中で撮った");
    (f, y)
}

/// PR #1006 の 2 回目の読み直し [A]: `translated_reuse_record_only_image` を開くと、 再生が Y の record で場所を Y に生かす。 b は Y の
/// 別名でなく、 B の遅れた書き込み (F の削除を知らない、 b 宛て) は消えた identity に当たって捨てられる (新しい生きた行にならない)。
///
/// 前は記録から戻す道 (`restore_prev_resident`) が、 (C, f) の写像が同じ場所に残っている (`prev_evicted` が記録の行 = 場所の最後の行
/// の時にわざと残す) のを 「落ちる前に別の場所に払い直した」 と同じに扱い、 b を退避なしで外して返った (91a9005 からの退行。 master の
/// この枝は退避表に居る別名しか外さず、 b は死んだ場所に残った)。
#[test]
fn an_alias_of_a_translated_row_is_evicted_when_only_the_reuse_record_reached_disk() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("translated_reuse_record_only");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (f, y) = translated_reuse_record_only_image(&live, &img);
    crashsim::stop();
    let db = img.join("db");
    let (ro_b, ro_f, ro_slot, ro_record, ro_f_dead) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        let s = ro.eid_slot(f);
        let at = |w| ro.resolve_remote_eid_existing(w).map(|e| ro.eid_slot(e));
        (at(WORLD_B2), at(WORLD_F), s, ro.prev_resident_of_slot(s), !ro.is_live(f) && !ro.is_live(y))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let y_alive = eng.is_live(y);
    let b_at = eng.resolve_remote_eid_existing(WORLD_B2).map(|e| eng.eid_slot(e));
    let f_at = eng.resolve_remote_eid_existing(WORLD_F).map(|e| eng.eid_slot(e));
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let late = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
    let t = eng.resolve_remote_eid(WORLD_B2, hid);
    let landed = (t.is_some_and(|t| eng.remote_tie_apply(t, hid, 4u32, late)), t.is_some_and(|t| eng.is_live(t)));
    let y_c = eng.get(y, "t.c");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_b, Some(ro_slot), "前提: 像の `eidalias` は b → F の場所 (使い回しの前)");
    assert_eq!(ro_f, Some(ro_slot), "前提: 像の `.eidmap` は (C, f) → F の場所 (使い回しの前)");
    assert!(ro_record.is_some_and(|r| r.0 == 3 && r.1 == 7 && r.2 == F_DELETE && !r.3), "前提: 場所の記録は前の住人 F: {ro_record:x?}");
    assert!(ro_f_dead, "前提: 本体の場所は死んだ F のまま (使い回しは本体に届いていない)");
    assert!(y_alive && y_c == Some(9), "前提: 再生が Y を生かした (c = {y_c:?})");
    assert_ne!(f_at, Some(ro_slot), "前提: (C, f) は Y の場所を指さない");
    assert_ne!(b_at, Some(ro_slot), "F の別名 b が、 使い回した Y の別名に残った");
    assert_eq!(landed, (false, false), "b 宛ての遅れた書き込みが当たった / 生きた行になった (幽霊行)");
}

/// 2 回目の読み直し [B] の B の行の世界番号。
const WORLD_B5: u64 = (2u64 << 32) | 15;

/// `dir` の下の file を全部 「書き出し済み」 として控える (crashsim。 撮った像を開いた engine で、 もう一度電源断の像を撮る時)。
fn adopt_as_durable(dir: &Path) {
    for ent in std::fs::read_dir(dir).unwrap() {
        let ent = ent.unwrap();
        if ent.file_type().unwrap().is_dir() {
            adopt_as_durable(&ent.path());
        } else {
            crashsim::file_synced(&ent.path());
        }
    }
}

/// PR #1006 の 2 回目の読み直し [B] (b) の像 (2 枚)。 名前 7 の自分の行 X (世代 gx) に B の行 b を畳み (b は X の場所の別名、 退避表に
/// 居ない)、 表を使い切って書き出す。 X を本体の書き出しの途中で消し (場所の世代を上げるのは生きている印の書き出しの後、 [`c2_image`])、
/// 書き出しの終わり (`WrittenOut`) で電源断 (1 枚目 `img`: gen.seg の死の印は届き、 生きている印は届かない = [C2] の像)。 1 枚目を開いた A1 (突き合わせが
/// X の削除を完了させて場所の世代を 2 つ上げる、 b は残る) で書き出してから、 Y で X の場所を使い回す (開いた後の場所の最後の行は X =
/// gen.seg の 「最後の行は 今 − 2」 の印、 #1008。 前は gx + 1 = 無かった行と読み、 使い回しの記録の前の住人も gx + 1 だった)。 その
/// `oplog_sync` の本体の書き出しで `prev.seg` を書き出した後・生きている印の前 (`PrevWrittenOut`) に電源断 (2 枚目 `img2`: 場所は死んだ
/// まま、 記録 (前の住人 X) だけが届き、 `eidalias` は b → X の場所 (gx)、 WAL に Y の record)。 (X, Y) を返す。
fn reuse_after_reopened_power_loss_image(live: &Path, img: &Path, img2: &Path) -> (u64, u64) {
    let x = {
        let (eng, x) = full_named_table_engine(&live.join("db"));
        let hv = eng.himo_id("t.v").unwrap() as u16;
        let b = eng.resolve_remote_eid(WORLD_B5, hv).unwrap();
        assert!(eng.remote_tie_apply_result(b, hv, 8u32, Hlc { wall: 1_001, logical: 0, peer: 2 }).applied());
        eng.merge_entity_into(b, x).unwrap();
        // b を受けて伸びた表を使い切る (使い切った表は、 消した場所を次の行に使い回す)
        let (mut n, mut filler) = (300u32, None);
        while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity) {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to(e, "t.k", n);
            filler = Some(e);
            n += 1;
        }
        let filler = filler.expect("前提: 表を伸ばした (b を受けた)");
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        eng.persist_sync_state().unwrap();
        assert_eq!(
            eng.resolve_remote_eid_existing(WORLD_B5).map(|e| eng.eid_slot(e)),
            Some(eng.eid_slot(x)),
            "前提: b は X の場所の別名"
        );
        let _hold = eng.hold_consumer();
        // 書き出す物を作る (本体の書き出しが段を通る。 X に書くと WAL の X の record で再生が X を戻すので、 他の行に)
        eng.tie_to(filler, "t.v", 5u32);
        eng.oplog_commit();
        let e2 = eng.clone();
        c2_image(&eng, move || e2.delete(x), live, img);
        x
    };
    // 1 枚目を開く (その file は全部書き出し済み = A1 の書き出しの控えはこの上に積む)
    adopt_as_durable(img);
    let db = img.join("db");
    let a1 = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    a1.set_peer_id(1);
    let slot = a1.eid_slot(x);
    assert_eq!(a1.generation_repairs().0, 1, "前提: 突き合わせが X の削除を完了させた (世代を 2 つ上げた)");
    assert!(!a1.is_live(x), "前提: A1 で X は消えている");
    assert_eq!(
        a1.resolve_remote_eid_existing(WORLD_B5).map(|e| a1.eid_slot(e)),
        Some(slot),
        "前提: A1 で b は消えた X の別名のまま"
    );
    a1.oplog_sync().unwrap();
    a1.persist_sync_state().unwrap();
    let _hold = a1.hold_consumer();
    let y = a1.entity_in("t").unwrap();
    a1.tie_to(y, "t.k", 9u32);
    a1.oplog_commit();
    assert_eq!(a1.eid_slot(y), slot, "前提: Y が X の場所を使い回した");
    assert_eq!(a1.resolve_remote_eid_existing(WORLD_B5), None, "前提: 使い回しが b を外した (memory)");
    let shot = Arc::new(AtomicBool::new(false));
    let (s2, l, i) = (shot.clone(), img.to_path_buf(), img2.to_path_buf());
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::PrevWrittenOut && !s2.swap(true, Ordering::SeqCst) {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
        }
    })));
    a1.oplog_sync().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: A1 の本体の書き出しの途中で撮った");
    (x, y)
}

/// PR #1006 の 2 回目の読み直し [B] (b): `reuse_after_reopened_power_loss_image` の 2 枚目を開くと、 再生が Y の record で場所を Y に生かし、
/// その使い回しの移り目のやり直し (`redo_reuse_eviction`、 記録の前の住人は X。 #1008 の前は gx + 1) が b を外して記録の削除の版数で退避する。 b は Y の
/// 別名でなく、 B の遅れた書き込み (b 宛て) は消えた identity に当たって捨てられる (新しい生きた行にならない)。
///
/// 前は再生の前のなめが死んだ場所の b (gx) を 「使い回しの記録 (gx + 1 ≥ gx) が場所から離した行の別名」 として外し、 記録の行が
/// 完全一致しない (`record_at(slot, gx)` が無い) ので版数を引けず退避なしで外した。 自分の行の記録の戻しは再生の後に回している
/// ([N1]) のに先取りした形で、 b 宛ての遅れた書き込みが新しい生きた行 (幽霊行) になった。
#[test]
fn an_alias_of_a_dead_row_is_evicted_by_the_reuse_after_a_reopened_power_loss() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("reuse_after_reopened_power_loss");
    let (live, img, img2) = (root.join("live"), root.join("img"), root.join("img2"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, y) = reuse_after_reopened_power_loss_image(&live, &img, &img2);
    crashsim::stop();
    let db = img2.join("db");
    let (ro_b, ro_slot, ro_record, ro_dead) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        let s = ro.eid_slot(x);
        (ro.resolve_remote_eid_existing(WORLD_B5).map(|e| ro.eid_slot(e)), s, ro.prev_resident_of_slot(s), !ro.is_live(y))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let y_alive = eng.is_live(y);
    let b_at = eng.resolve_remote_eid_existing(WORLD_B5).map(|e| eng.eid_slot(e));
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let late = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
    let t = eng.resolve_remote_eid(WORLD_B5, hv);
    let landed = (t.is_some_and(|t| eng.remote_tie_apply(t, hv, 4u32, late)), t.is_some_and(|t| eng.is_live(t)));
    let b_tomb = eng.deleted_before_create(WORLD_B5);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_b, Some(ro_slot), "前提: 2 枚目の `eidalias` は b → X の場所 (使い回しの前)");
    // 記録の前の住人は X (開いた後の場所の最後の行 = 突き合わせが世代を 2 つ上げ、 「最後の行は 今 − 2」 の印を立てた、 #1008。 Y は X + 2)
    assert!(
        ro_record.is_some_and(|r| r.0 == 1 && r.3 && r.1 == eid_local(x)),
        "前提: 場所の記録の前の住人は X: {ro_record:x?} (X = {:x}, Y = {:x})",
        eid_local(x),
        eid_local(y)
    );
    assert!(ro_dead, "前提: 本体の場所は死んだまま (使い回しは本体に届いていない)");
    assert!(y_alive, "前提: 再生が Y を生かした");
    assert_ne!(b_at, Some(ro_slot), "X の別名 b が、 使い回した Y の別名に残った");
    assert_eq!(landed, (false, false), "b 宛ての遅れた書き込みが当たった / 生きた行になった (幽霊行)");
    assert!(b_tomb.is_some(), "b が消えた identity として退避されていない");
}

/// PR #1006 の 2 回目の読み直しの [3] の引き換え (別案) の B の行の世界番号。
const WORLD_B6: u64 = (2u64 << 32) | 16;

/// 2 回目の読み直しの [3] の引き換えの像 (ホスト = 受けた record を中継して WAL に載せる DB): 名前 7 の自分の行 X に B の行 b を畳み
/// (b は X の場所の別名、 `eidalias` に書き出す)、 表を使い切って書き出す。 B の b 宛ての書き込み (v = 71) と削除を受けて (X に当たる、
/// X は消える) 中継し (WAL の checkpoint の後ろ)、 `oplog_sync` の本体の書き出しの途中で Y (v を持たない) で X の場所を使い回した
/// 像 ([`reuse_window_image`])。 本体には Y が届き (生きている印と gen.seg)、 使い回しの記録・Y の record は届かない (review946 [3]
/// の窓)。 WAL の再生する record は中継した b 宛ての書き込みと削除だけ。 (X, Y, b の削除の版数) を返す。
///
/// `second` (PR #1006 の 3 回目の読み直し [3]): C の行 c (`WORLD_C16`) も X に畳む (c も X の場所の別名)。
fn relayed_write_over_reuse_without_record_image(live: &Path, img: &Path, second: bool) -> (u64, u64, Hlc) {
    let (eng, x) = full_named_table_engine(&live.join("db"));
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let b = eng.resolve_remote_eid(WORLD_B6, hv).unwrap();
    assert!(eng.remote_tie_apply_result(b, hv, 8u32, Hlc { wall: 1_001, logical: 0, peer: 2 }).applied());
    eng.merge_entity_into(b, x).unwrap();
    if second {
        let c = eng.resolve_remote_eid(WORLD_C16, hv).unwrap();
        assert!(eng.remote_tie_apply_result(c, hv, 6u32, Hlc { wall: 1_002, logical: 0, peer: 3 }).applied());
        eng.merge_entity_into(c, x).unwrap();
    }
    let mut n = 300u32;
    // 畳んで空いた場所も埋める (Y は X の場所だけを使い回す)
    while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity || u.free > 0) {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.k", n);
        n += 1;
    }
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let _hold = eng.hold_consumer();
    // B の b 宛ての書き込みを受けて (X に当たる) 中継する
    let at = Hlc { wall: eng.cell_hlc(x, hv).wall + 1_000_000, logical: 0, peer: 2 };
    let to = eng.resolve_remote_eid(WORLD_B6, hv).unwrap();
    assert_eq!(eng.eid_slot(to), eng.eid_slot(x), "前提: b 宛ては X に当たる");
    assert!(eng.remote_tie_apply(to, hv, 71u32, at), "前提: b 宛ての書き込みが X に当たった");
    let rec = enchudb_engine::transport::WireRecord::unsigned(at, 2, DecodedOp::Tie { eid: WORLD_B6, himo_id: hv, value: 71 });
    assert!(eng.relay_record(&rec), "前提: b 宛ての書き込みを中継して WAL に載せた");
    let gone = Hlc { wall: at.wall + 1, logical: 0, peer: 2 };
    assert!(eng.remote_delete_apply_foreign(WORLD_B6, gone), "前提: b 宛ての削除が X を消した");
    let rec = enchudb_engine::transport::WireRecord::unsigned(gone, 2, DecodedOp::Delete { eid: WORLD_B6 });
    assert!(eng.relay_record(&rec), "前提: b 宛ての削除を中継して WAL に載せた");
    eng.oplog_commit();
    assert!(!eng.is_live(x), "前提: X は消えた");
    let e2 = eng.clone();
    let y = reuse_window_image(
        &eng,
        move || {
            let y = e2.entity_in("t").unwrap();
            e2.tie_to(y, "t.k", 9u32);
            e2.oplog_commit();
            y
        },
        live,
        img,
    );
    assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: Y が X の場所を使い回した");
    (x, y, gone)
}

/// PR #1006 の 2 回目の読み直しの [3] の引き換え (別案): `relayed_write_over_reuse_without_record_image` を開くと、 本体の場所の住人は Y
/// (X の record は WAL に無い)。 `eidalias` の b は X の世代を指し、 使い回しの記録が無く再生する record があるので再生の前には外れない
/// (読み直し [3])。 再生する中継の b 宛ての書き込みと削除は、 その古い別名で Y に当てない (Y は生きていて v を持たない)。 削除は消えた
/// identity b の削除として退避表に載る (開いた後の b 宛ての遅れた書き込みは当たらない)。
///
/// 前は再生の全部の間、 古い別名 b で中継の record が Y に当たった (v = 71、 中継された削除が Y を消した。 master と同じ)。
#[test]
fn a_relayed_write_to_a_former_alias_does_not_hit_the_new_resident_on_replay() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("relayed_write_over_reuse_without_record");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, y, gone) = relayed_write_over_reuse_without_record_image(&live, &img, false);
    crashsim::stop();
    let db = img.join("db");
    let (ro_b, ro_slot, ro_record, ro_y_alive, ro_tomb) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        let s = ro.eid_slot(x);
        (ro.resolve_remote_eid_existing(WORLD_B6).map(|e| ro.eid_slot(e)), s, ro.prev_resident_of_slot(s), ro.is_live(y), ro.tombstone_hlc(x))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let (x_alive, y_alive) = (eng.is_live(x), eng.is_live(y));
    let y_v = eng.get(y, "t.v");
    let b_at = eng.resolve_remote_eid_existing(WORLD_B6).map(|e| eng.eid_slot(e));
    let b_tomb = eng.deleted_before_create(WORLD_B6);
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let late = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
    let t = eng.resolve_remote_eid(WORLD_B6, hv);
    let landed = (t.is_some_and(|t| eng.remote_tie_apply(t, hv, 4u32, late)), t.is_some_and(|t| eng.is_live(t)));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_b, Some(ro_slot), "前提: 像の `eidalias` は b → X の場所");
    assert!(ro_y_alive, "前提: 本体の場所の住人は Y");
    assert!(ro_record.is_none_or(|r| r.1 != eid_local(x)), "前提: 使い回しの記録 (前の住人 X) は届いていない: {ro_record:x?}");
    // 届いていると、 開く時の sweep が tombstone で Y を消す (中継の削除が Y に当たった時と同じ落ち方になる)
    assert_eq!(ro_tomb, Hlc::ZERO, "前提: 前の住人 X の tombstone は届いていない (b の削除の版数 {gone:?})");
    assert!(!x_alive, "前提: 開いた A で X は消えている");
    assert!(y_alive, "中継した b 宛ての削除が、 再生で古い別名から使い回した Y を消した");
    assert_eq!(y_v, None, "中継した b 宛ての書き込みが、 再生で古い別名から使い回した Y に当たった");
    assert_ne!(b_at, Some(ro_slot), "前提: 開く道の仕上げが Y より下の世代の b を外した");
    assert_eq!(b_tomb, Some(gone), "再生した b 宛ての削除が b の削除として退避表に載らない");
    assert_eq!(landed, (false, false), "b 宛ての遅れた書き込みが当たった / 生きた行になった (幽霊行)");
}

/// #994 [C4] の死んだ場所 (PR #1006 の 2 回目の読み直し): 翻訳した行 F (B の行 b を畳んだ) を C の削除で消し、 Y で F の場所を使い回す
/// (使い回しが b を外して退避する)。 B の遅れた b 宛ての書き込みが b を新しい死んだ場所 S2 に払い直し (`.eidmap` の (B, b) → S2)、 Y も
/// 消す (F の場所は死んだ場所)。 WAL と本体を書き出してから、 sidecar の書き出しを `.eidmap` の後・`eidalias` の前で kill (`eidalias` は
/// 使い回しの前 = b → F の場所)。 (F の場所, S2) を返す。
fn overridden_alias_at_dead_slot_image(live: &Path, img: &Path) -> (u32, u32) {
    let (eng, f) = translated_row_with_alias_engine(&live.join("db"));
    let f_slot = eng.eid_slot(f);
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let _hold = eng.hold_consumer();
    assert!(eng.remote_delete_apply_foreign(WORLD_F, F_DELETE), "前提: C の削除が F に当たった");
    let y = eng.entity_in("t").unwrap();
    eng.tie_to(y, "t.c", 9u32);
    eng.oplog_commit();
    assert_eq!(eng.eid_slot(y), f_slot, "前提: Y は F の場所を使い回した");
    let late = eng.resolve_remote_eid(WORLD_B2, hid).unwrap();
    let s2 = eng.eid_slot(late);
    assert_ne!(s2, f_slot, "前提: b を別の場所に払い直した");
    assert!(!eng.is_live(late), "前提: 払い直した場所は退避表の削除で死んでいる");
    eng.delete(y);
    eng.oplog_commit();
    // WAL と本体を書き出して checkpoint を Y の record の先へ (`oplog_sync` は `.eidmap` / `eidalias` を書かない)。 再生が Y の record で
    // 場所を生かし直す所 (使い回しの移り目の外し) を通らない = 開く道の仕上げのなめだけが b を見る
    eng.oplog_sync().unwrap();
    let s = eng.stats();
    assert_eq!(s.oplog_checkpoint, s.oplog_head, "前提: checkpoint が Y の削除の先まで進んだ");
    let shot = Arc::new(AtomicBool::new(false));
    let (s3, l, i) = (shot.clone(), live.to_path_buf(), img.to_path_buf());
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::EidmapWritten && !s3.swap(true, Ordering::SeqCst) {
            kill_image(&l, &i);
        }
    })));
    eng.persist_tables().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: sidecar の書き出しの途中で撮った");
    (f_slot, s2)
}

/// #994 [C4] の死んだ場所 (PR #1006 の 2 回目の読み直し): `overridden_alias_at_dead_slot_image` の像から `prev.seg` を外して (使い回しの
/// 記録の無い DB = 前の binary が作った v15 の DB / 記録を書けなかった使い回し) 開くと、 古い `eidalias` の b (死んだ F の場所) が
/// `.eidmap` の写像 (B, b) → S2 を上書きしている。 `.eidmap` を先に書くので上書きした別名の方が古い: 開く道の仕上げが b を外して
/// 写像を S2 に戻す。
///
/// 前は死んだ場所の別名を使い回しの記録でしか外さず、 記録の無い時は古い別名が残って `.eidmap` の新しい写像が戻らなかった。
#[test]
fn an_overridden_alias_at_a_dead_slot_gives_back_the_mapping_without_a_record() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("overridden_alias_at_dead_slot");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    let (f_slot, s2) = overridden_alias_at_dead_slot_image(&live, &img);
    let db = img.join("db");
    std::fs::remove_file(db.join("prev.seg")).expect("前提: `prev.seg` がある");
    let (ro_b, ro_f_dead) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (ro.resolve_remote_eid_existing(WORLD_B2).map(|e| ro.eid_slot(e)), !ro.is_live_unchecked(enchudb_oplog::make_eid(1, f_slot)))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let b_at = eng.resolve_remote_eid_existing(WORLD_B2).map(|e| eng.eid_slot(e));
    let f_at_dead = !eng.is_live_unchecked(enchudb_oplog::make_eid(1, f_slot));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_b, Some(f_slot), "前提: 像の `eidalias` は b → F の場所 (`.eidmap` の写像を上書き)");
    assert!(ro_f_dead && f_at_dead, "前提: F の場所は死んでいる (Y の削除)");
    assert_eq!(b_at, Some(s2), "`.eidmap` の (B, b) → S2 (使い回しの後に払い直した) が戻らない");
}

/// PR #1006 の 3 回目の読み直し [2] の像の、 F の場所の住人 Y の最後。
#[derive(Clone, Copy, Debug, PartialEq)]
enum OverriddenTargetY {
    /// Y は生きたまま (開く道の仕上げが生きている場所の Y より下の世代の b を外す = `drop_aliases_where` の写像の戻し)
    Alive,
    /// `Alive` の、 b を S2 に払い直した後に忘れてよい線が F の削除の版数より前を忘れた形 (退避表に b が無い。 S2 の削除の版数は残る)
    AliveForgotten,
    /// Y は WAL の Z の後ろの Delete で消える (開く道の仕上げの死んだ場所 = `restore_overridden_aliases`)
    DeletedInWal,
}

/// PR #1006 の 3 回目の読み直し [2]: 翻訳した行 F (B の行 b を畳んだ) を C の削除で消し、 Y で F の場所を使い回す (使い回しが b を外して
/// 退避する)。 B の遅れた b 宛ての書き込みが b を新しい死んだ場所 S2 に払い直す (`.eidmap` の (B, b) → S2)。 WAL と本体を書き出して
/// から、 sidecar の書き出し (別の thread) が `.eidmap` を書いた後・`eidalias` を書く前に、 表を使い切った自分の行 Z が S2 を使い回し
/// (`how` = DeletedInWal なら続けて Y を消し)、 `oplog_sync` が WAL を fsync した後・本体を書き出す前に電源断 (本体と `prev.seg` は
/// 使い回しの前、 `eidalias` は b → F の場所、 WAL の checkpoint の後ろに Z (と Y の削除))。 (F の場所, S2, Z) を返す。
fn override_target_reused_on_replay_image(live: &Path, img: &Path, how: OverriddenTargetY) -> (u32, u32, u64) {
    let (eng, f) = translated_row_with_alias_engine(&live.join("db"));
    let f_slot = eng.eid_slot(f);
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let _hold = eng.hold_consumer();
    assert!(eng.remote_delete_apply_foreign(WORLD_F, F_DELETE), "前提: C の削除が F に当たった");
    let y = eng.entity_in("t").unwrap();
    eng.tie_to(y, "t.c", 9u32);
    eng.oplog_commit();
    assert_eq!(eng.eid_slot(y), f_slot, "前提: Y は F の場所を使い回した");
    let late = eng.resolve_remote_eid(WORLD_B2, hid).unwrap();
    let s2 = eng.eid_slot(late);
    assert_ne!(s2, f_slot, "前提: b を別の場所に払い直した");
    assert!(!eng.is_live(late), "前提: 払い直した場所は退避表の削除で死んでいる");
    if how == OverriddenTargetY::AliveForgotten {
        assert!(eng.forget_deletes_before(Hlc { wall: F_DELETE.wall + 1, logical: 0, peer: 0 }) >= 1, "前提: 退避を忘れた");
        assert_eq!(eng.deleted_before_create(WORLD_B2), None, "前提: 退避表に b は無い");
    }
    // 表を使い切る (次の払い出しは死んだ場所を使い回す)
    let mut n = 300u32;
    while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity) {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.c", n);
        n += 1;
    }
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    let s = eng.stats();
    assert_eq!(s.oplog_checkpoint, s.oplog_head, "前提: checkpoint が払い直しの先まで進んだ");
    // sidecar の書き出しを別の thread で、 `.eidmap` の後で止める。 止めている間に Z が S2 を使い回し、 `oplog_sync` が WAL を fsync
    // した後・本体を書き出す前 (`BeforeGate`、 中身の segment だけ書き出した所) で撮る
    let (to_main, from_persist) = std::sync::mpsc::channel::<()>();
    let to_main = std::sync::Mutex::new(to_main);
    let (armed, shot) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let (a2, s3, l, i) = (armed.clone(), shot.clone(), live.to_path_buf(), img.to_path_buf());
    write_out_hook::set(Some(Arc::new(move |p: Phase| match p {
        Phase::EidmapWritten if !s3.load(Ordering::SeqCst) => {
            let _ = to_main.lock().unwrap().send(());
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !s3.load(Ordering::SeqCst) {
                assert!(std::time::Instant::now() < until, "前提: Z の WAL の fsync の後で撮れない");
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        Phase::BeforeGate if a2.load(Ordering::SeqCst) && !s3.load(Ordering::SeqCst) => {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
            s3.store(true, Ordering::SeqCst);
        }
        _ => {}
    })));
    let persist = {
        let e2 = eng.clone();
        std::thread::spawn(move || e2.persist_tables().unwrap())
    };
    from_persist.recv_timeout(std::time::Duration::from_secs(10)).expect("前提: `.eidmap` を書いた");
    let z = eng.entity_in("t").unwrap();
    assert_eq!(eng.eid_slot(z), s2, "前提: Z は S2 を使い回した (表を使い切っている)");
    eng.tie_to(z, "t.c", 12u32);
    if how == OverriddenTargetY::DeletedInWal {
        eng.delete(y);
    }
    eng.oplog_commit();
    armed.store(true, Ordering::SeqCst);
    eng.oplog_sync().unwrap();
    persist.join().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: sidecar の書き出しの途中で撮った");
    (f_slot, s2, z)
}

/// PR #1006 の 3 回目の読み直し [2]: `override_target_reused_on_replay_image` の像から `prev.seg` を外して (使い回しの記録の無い DB) 開くと、
/// 古い `eidalias` の b (F の場所) が `.eidmap` の写像 (B, b) → S2 を上書きしている。 WAL の再生が S2 を自分の行 Z で使い回す (Z の
/// 記録は無い、 S2 の逆写像は上書きで外れている)。 開く道の仕上げが b を外す時、 写像を S2 に戻すと自分の行 Z が B の行 b の翻訳先になる
/// (Z の書き込みは (B, b) 宛てに配られ、 B の b 宛ての書き込みが Z に当たる)。 戻し先の住人が写像の面の住人でなければ戻さない。 再生が
/// S2 を使い回す所で (B, b) を S2 の削除の版数で退避する (使い回しが翻訳した行の写像を外すのと同じ): 忘れてよい線が退避表の b を
/// 忘れた後でも、 b は消えた identity (遅れた書き込みは当たらない)。
fn an_overridden_alias_is_not_given_back_to_a_slot_the_replay_reused(tag: &str, how: OverriddenTargetY) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (f_slot, s2, z) = override_target_reused_on_replay_image(&live, &img, how);
    crashsim::stop();
    let db = img.join("db");
    std::fs::remove_file(db.join("prev.seg")).expect("前提: `prev.seg` がある");
    let (ro_b, ro_z_dead) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (ro.resolve_remote_eid_existing(WORLD_B2).map(|e| ro.eid_slot(e)), !ro.is_live_unchecked(enchudb_oplog::make_eid(1, s2)))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let z_alive = eng.is_live(z);
    let b_rows: Vec<u32> = eng
        .translated_locals_of(2)
        .into_iter()
        .filter(|&(fl, _)| fl == 9)
        .map(|(_, l)| eng.eid_slot(enchudb_oplog::make_eid(1, l)))
        .collect();
    let b_at = eng.resolve_remote_eid_existing(WORLD_B2).map(|e| eng.eid_slot(e));
    let hid = eng.himo_id("t.c").unwrap() as u16;
    let late = Hlc { wall: u64::MAX / 4, logical: 0, peer: 2 };
    let landed = eng.resolve_remote_eid(WORLD_B2, hid).is_some_and(|t| eng.remote_tie_apply(t, hid, 4u32, late));
    let z_c = eng.get(z, "t.c");
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_b, Some(f_slot), "前提: 像の `eidalias` は b → F の場所 (`.eidmap` の写像を上書き) ({how:?})");
    assert!(ro_z_dead, "前提: 本体の S2 は死んでいる (Z の使い回しは WAL にだけある) ({how:?})");
    assert!(z_alive, "前提: 再生が Z を S2 に生かした ({how:?})");
    assert!(
        !b_rows.contains(&s2),
        "自分の行 Z の場所 S2 が B の行 b の翻訳先になった: {b_rows:x?} ({how:?})"
    );
    assert_ne!(b_at, Some(s2), "b の世界番号が Z の場所を指す ({how:?})");
    assert!(!landed && z_c == Some(12), "B の b 宛ての遅れた書き込みが Z に当たった: {landed} {z_c:?} ({how:?})");
}

#[test]
fn an_overridden_alias_is_not_given_back_to_a_slot_the_replay_reused_by_a_live_slot() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    an_overridden_alias_is_not_given_back_to_a_slot_the_replay_reused("override_reused_live", OverriddenTargetY::Alive);
}

#[test]
fn an_overridden_alias_is_not_given_back_to_a_slot_the_replay_reused_after_forgetting() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    an_overridden_alias_is_not_given_back_to_a_slot_the_replay_reused("override_reused_forgot", OverriddenTargetY::AliveForgotten);
}

#[test]
fn an_overridden_alias_is_not_given_back_to_a_slot_the_replay_reused_by_a_dead_slot() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    an_overridden_alias_is_not_given_back_to_a_slot_the_replay_reused("override_reused_dead", OverriddenTargetY::DeletedInWal);
}

/// PR #1006 の 3 回目の読み直し [1] の、 古い別名を通る再生の道。
#[derive(Clone, Copy, Debug, PartialEq)]
enum StaleAliasPath {
    /// C の行 c の Ref 列 `u.r` を B の行 b (X に畳んだ別名) に向ける中継の TieRef
    RefTarget,
    /// C の世界番号 e を b と同じ entity と知らせる中継の Alias (e に行は無い)
    RelayedAlias,
    /// 自分の行 X (場所を使い回した世代 1 の行) を自分の行 K に畳み、 C が X の世界番号 (世代 1) に書いて消す中継の Tie と Delete
    FoldedOwn,
}

/// [1] の像の C の行 c の世界番号 (`RefTarget`)。
const WORLD_C30: u64 = (3u64 << 32) | 30;
/// [1] の像の、 b と同じ entity と知らされる C の世界番号 (`RelayedAlias`)。
const WORLD_C20: u64 = (3u64 << 32) | 20;
/// [1] の像の、 X に畳む B の行 b の世界番号。
const WORLD_B7: u64 = (2u64 << 32) | 17;

/// PR #1006 の 3 回目の読み直し [1] の像 (ホスト = 受けた record を中継して WAL に載せる DB): 表 t (枠 2、 名前 k、 v) と表 u (n、
/// Ref `u.r` → t)。 名前 7 の自分の行 X は場所を使い回した行 (世代 1)。 `RefTarget` / `RelayedAlias` は X に B の行 b を畳み (b は X の
/// 場所の別名)、 `FoldedOwn` は X を名前 1 の自分の行 K に畳む (X の世界番号は K の場所の別名)。 表を使い切って書き出してから、 `path`
/// の中継の record と、 畳んだ先 (X / K) を消す中継の削除を受けて (当たる) 中継し (WAL の checkpoint の後ろ)、 `oplog_sync` の本体の
/// 書き出しの途中で Y で畳んだ先の場所を使い回した像 ([`reuse_window_image`]、 review946 [3] の窓: 本体には Y、 使い回しの記録と
/// Y の record は無い)。 (X, 畳んだ先, Y) を返す。
fn stale_alias_path_image(live: &Path, img: &Path, path: StaleAliasPath) -> (u64, u64, u64) {
    let db = live.join("db");
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 2).unwrap();
    eng.define_himo_in("t", "k", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    let k = eng.himo_id("t.k").unwrap() as u16;
    eng.set_table_pk("t", k).unwrap();
    eng.define_table("u", 8).unwrap();
    eng.define_himo_in("u", "n", ValueType::Number, 0).unwrap();
    eng.define_ref_in("u", "r", "t").unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let hr = eng.himo_id("u.r").unwrap() as u16;
    let w = eng.entity_in("t").unwrap();
    eng.tie_to(w, "t.k", 100u32);
    let keep = eng.entity_in("t").unwrap();
    eng.tie_to(keep, "t.k", 1u32);
    eng.delete(w);
    let x = eng.entity_in("t").unwrap();
    assert_eq!(eng.eid_slot(x), eng.eid_slot(w), "前提: X は w の場所を使い回した (v15、 世代 1)");
    eng.tie_to(x, "t.k", 7u32);
    eng.tie_to(x, "t.v", 70u32);
    let target = if path == StaleAliasPath::FoldedOwn {
        eng.merge_entity_into(x, keep).unwrap();
        keep
    } else {
        let b = eng.resolve_remote_eid(WORLD_B7, hv).unwrap();
        assert!(eng.remote_tie_apply_result(b, hv, 8u32, Hlc { wall: 1_001, logical: 0, peer: 2 }).applied());
        eng.merge_entity_into(b, x).unwrap();
        x
    };
    let mut n = 300u32;
    while eng.table_eid_usage("t").is_some_and(|u| u.allocated < u.capacity) {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.k", n);
        n += 1;
    }
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let _hold = eng.hold_consumer();
    let at = Hlc { wall: eng.cell_hlc(target, hv).wall + 1_000_000, logical: 0, peer: 3 };
    // 畳んだ先を消す世界番号 (b / X の世界番号) と、 その削除を中継する author
    let (gone_world, gone_author) = match path {
        StaleAliasPath::FoldedOwn => (enchudb_oplog::make_eid(1, eid_local(x)), 3),
        _ => (WORLD_B7, 2),
    };
    match path {
        StaleAliasPath::RefTarget => {
            let c = eng.resolve_remote_eid(WORLD_C30, hr).unwrap();
            let v = eng.resolve_remote_ref_value(2, eid_local(WORLD_B7), hr).unwrap();
            assert_eq!(v, eid_local(x), "前提: b を指す ref は X を指す");
            assert!(eng.remote_tie_apply(c, hr, v, at), "前提: c の ref が当たった");
            let rec = enchudb_engine::transport::WireRecord::unsigned(at, 3, DecodedOp::TieRef { eid: WORLD_C30, himo_id: hr, target: WORLD_B7 });
            assert!(eng.relay_record(&rec), "前提: c の ref を中継して WAL に載せた");
        }
        StaleAliasPath::RelayedAlias => {
            assert!(eng.apply_alias(WORLD_C20, WORLD_B7), "前提: e を b (X) の別名にした");
            let rec = enchudb_engine::transport::WireRecord::unsigned(at, 3, DecodedOp::Alias { eid: WORLD_C20, same_as: WORLD_B7 });
            assert!(eng.relay_record(&rec), "前提: 知らせを中継して WAL に載せた");
        }
        StaleAliasPath::FoldedOwn => {
            let to = eng.resolve_remote_eid(gone_world, hv).unwrap();
            assert_eq!(eng.eid_slot(to), eng.eid_slot(keep), "前提: X の世界番号 (世代 1) は K に当たる");
            assert!(eng.remote_tie_apply(to, hv, 71u32, at), "前提: X 宛ての書き込みが K に当たった");
            let rec = enchudb_engine::transport::WireRecord::unsigned(at, 3, DecodedOp::Tie { eid: gone_world, himo_id: hv, value: 71 });
            assert!(eng.relay_record(&rec), "前提: X 宛ての書き込みを中継して WAL に載せた");
        }
    }
    let gone = Hlc { wall: at.wall + 1, logical: 0, peer: gone_author };
    assert!(eng.remote_delete_apply_foreign(gone_world, gone), "前提: 削除が畳んだ先を消した");
    let rec = enchudb_engine::transport::WireRecord::unsigned(gone, gone_author, DecodedOp::Delete { eid: gone_world });
    assert!(eng.relay_record(&rec), "前提: 削除を中継して WAL に載せた");
    eng.oplog_commit();
    assert!(!eng.is_live(target), "前提: 畳んだ先は消えた");
    let e2 = eng.clone();
    let y = reuse_window_image(
        &eng,
        move || {
            let y = e2.entity_in("t").unwrap();
            e2.tie_to(y, "t.k", 9u32);
            e2.oplog_commit();
            y
        },
        live,
        img,
    );
    assert_eq!(eng.eid_slot(y), eng.eid_slot(target), "前提: Y が畳んだ先の場所を使い回した");
    (x, target, y)
}

/// PR #1006 の 3 回目の読み直し [1]: `stale_alias_path_image` を開くと、 本体の畳んだ先の場所の住人は Y (畳んだ先を消す record は
/// WAL の中継の削除だけ)。 `eidalias` の別名 (b / X の世界番号) は畳んだ先の世代を指し、 使い回しの記録が無く再生する record があるので
/// 再生の前には外れない (読み直し [3])。 再生する中継の record はその古い別名を通って Y に当たらない:
/// - `RefTarget`: c の ref は消えた X を指す (Y を指さない)。 前は ref の値を別名の場所の最後の行 (Y) にした
/// - `RelayedAlias`: e は Y の別名にならない。 前は e を Y の別名にした (e 宛ての書き込みが Y に当たる)
/// - `FoldedOwn`: X の世界番号 (世代 1) 宛ての書き込みと削除は Y に当たらない。 前は #612 の別名の鍵 (場所) を世代込みの番号で引いて
///   古い別名と分からず、 K の場所に転送して Y に当て、 中継の削除が Y を消した
fn a_relayed_record_does_not_reach_the_new_resident_through_a_stale_alias(tag: &str, path: StaleAliasPath) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, target, y) = stale_alias_path_image(&live, &img, path);
    crashsim::stop();
    let db = img.join("db");
    let (ro_y_alive, ro_record, ro_tomb) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (ro.is_live(y), ro.prev_resident_of_slot(ro.eid_slot(target)), ro.tombstone_hlc(target))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let (target_alive, y_alive) = (eng.is_live(target), eng.is_live(y));
    let y_v = eng.get(y, "t.v");
    let y_slot = eng.eid_slot(y);
    let c_ref = eng.resolve_remote_eid_existing(WORLD_C30).and_then(|c| eng.get(c, "u.r"));
    let e_at = eng.resolve_remote_eid_existing(WORLD_C20).map(|e| eng.eid_slot(e));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert!(ro_y_alive, "前提: 本体の場所の住人は Y ({path:?})");
    assert!(ro_record.is_none_or(|r| r.1 != eid_local(target)), "前提: 使い回しの記録は届いていない: {ro_record:x?} ({path:?})");
    // 届いていると、 開く時の sweep が tombstone で Y を消す (中継の削除が Y に当たった時と同じ落ち方になる)
    assert_eq!(ro_tomb, Hlc::ZERO, "前提: 畳んだ先の tombstone は届いていない ({path:?})");
    assert!(!target_alive, "前提: 開いた A で畳んだ先は消えている ({path:?})");
    assert!(y_alive, "中継した削除が、 再生で古い別名から使い回した Y を消した ({path:?})");
    assert_eq!(y_v, None, "中継した書き込みが、 再生で古い別名から使い回した Y に当たった ({path:?})");
    match path {
        StaleAliasPath::RefTarget => {
            assert_ne!(c_ref, Some(u64::from(eid_local(y))), "c の ref が、 再生で古い別名から使い回した Y を指した");
            assert_eq!(c_ref, Some(u64::from(eid_local(x))), "c の ref が消えた X を指さない");
        }
        StaleAliasPath::RelayedAlias => {
            assert_ne!(e_at, Some(y_slot), "中継した知らせの再生が、 古い別名から e を使い回した Y の別名にした");
        }
        StaleAliasPath::FoldedOwn => {}
    }
}

#[test]
fn a_relayed_ref_does_not_point_to_the_new_resident_through_a_stale_alias() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_relayed_record_does_not_reach_the_new_resident_through_a_stale_alias("stale_alias_ref", StaleAliasPath::RefTarget);
}

#[test]
fn a_relayed_alias_does_not_bind_the_new_resident_through_a_stale_alias() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_relayed_record_does_not_reach_the_new_resident_through_a_stale_alias("stale_alias_alias", StaleAliasPath::RelayedAlias);
}

#[test]
fn a_relayed_write_to_a_folded_own_row_does_not_reach_the_new_resident() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_relayed_record_does_not_reach_the_new_resident_through_a_stale_alias("stale_alias_folded_own", StaleAliasPath::FoldedOwn);
}

/// PR #1006 の 3 回目の読み直し [3] の像の、 X に畳む 2 つ目の行 (C の行 c) の世界番号。
const WORLD_C16: u64 = (3u64 << 32) | 16;

/// PR #1006 の 3 回目の読み直し [3]: `relayed_write_over_reuse_without_record_image` の X に C の行 c も畳んだ像 (b も c も X の場所の別名)。
/// 再生は中継した b 宛ての削除 (X を消した削除) を古い別名で Y に当てず、 b の削除として退避表に載せる。 同じ削除で消えた X の他の別名
/// c も同じ版数で退避する (開く道の仕上げは、 使い回しの記録が無いので前の住人の削除の版数を引けず、 c を退避なしで外す)。 開いた後の
/// c 宛ての遅れた書き込みは当たらない (新しい生きた行にならない)。
///
/// 前は b だけを退避し、 c は仕上げで退避なしに外れて、 c 宛ての遅れた書き込みが新しい生きた行 (幽霊行) になった。
#[test]
fn a_relayed_delete_through_a_stale_alias_evicts_the_other_aliases_of_the_gone_row() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("relayed_delete_over_reuse_other_aliases");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, y, gone) = relayed_write_over_reuse_without_record_image(&live, &img, true);
    crashsim::stop();
    let db = img.join("db");
    let (ro_c, ro_slot, ro_record, ro_tomb, ro_y_alive) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        let s = ro.eid_slot(x);
        (ro.resolve_remote_eid_existing(WORLD_C16).map(|e| ro.eid_slot(e)), s, ro.prev_resident_of_slot(s), ro.tombstone_hlc(x), ro.is_live(y))
    };
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let y_alive = eng.is_live(y);
    let c_at = eng.resolve_remote_eid_existing(WORLD_C16).map(|e| eng.eid_slot(e));
    let (b_tomb, c_tomb) = (eng.deleted_before_create(WORLD_B6), eng.deleted_before_create(WORLD_C16));
    let hv = eng.himo_id("t.v").unwrap() as u16;
    let late = Hlc { wall: u64::MAX / 4, logical: 0, peer: 3 };
    let t = eng.resolve_remote_eid(WORLD_C16, hv);
    let landed = (t.is_some_and(|t| eng.remote_tie_apply(t, hv, 4u32, late)), t.is_some_and(|t| eng.is_live(t)));
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(ro_c, Some(ro_slot), "前提: 像の `eidalias` は c → X の場所");
    assert!(ro_y_alive, "前提: 本体の場所の住人は Y");
    assert!(ro_record.is_none_or(|r| r.1 != eid_local(x)), "前提: 使い回しの記録 (前の住人 X) は届いていない: {ro_record:x?}");
    // 届いていると、 開く時の sweep が tombstone で Y を消す (中継の削除が Y に当たった時と同じ落ち方になる)
    assert_eq!(ro_tomb, Hlc::ZERO, "前提: 前の住人 X の tombstone は届いていない (b の削除の版数 {gone:?})");
    assert!(y_alive, "前提: 中継した b 宛ての削除は Y を消さない");
    assert_eq!(b_tomb, Some(gone), "前提: 再生した b 宛ての削除が b の削除として退避表に載る");
    assert_ne!(c_at, Some(ro_slot), "前提: 開く道の仕上げが Y より下の世代の c を外した");
    assert_eq!(c_tomb, Some(gone), "X を消した削除で c が退避されない");
    assert_eq!(landed, (false, false), "c 宛ての遅れた書き込みが当たった / 生きた行になった (幽霊行)");
}

// ──── #978: 削除が落とした cell だけ届き、 tombstone (tomb.seg) が届く前に落ちた像 ────

/// #978 の名前の列の型と、 削除が落とした名前の cell が disk に届く書き出しの段 (像を撮る段)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Name978 {
    /// 名前が Number 列 (中身を指さない列 = 残りの段で書き出す)。 像は版数の列の前 (`BeforeVersions`、 issue の像)
    Number,
    /// 名前が Tag 列で、 書き出しの前に別の行 Z の名前に新しい語を書く (列に門の印 = 中身を指す列の最初の段で書き出す)。 像はその段の
    /// 後 (`Opened`)
    TagFresh,
    /// 名前が Tag 列 (印なし = 中身を指す列を 1 本ずつ書き出す段)。 像はその段の後 (`PrevWrittenOut`。 生きている印は書き出す前)
    TagColumn,
}

impl Name978 {
    fn tag(self) -> bool {
        self != Name978::Number
    }
    fn shot(self) -> Phase {
        match self {
            Name978::Number => Phase::BeforeVersions,
            Name978::TagFresh => Phase::Opened,
            Name978::TagColumn => Phase::PrevWrittenOut,
        }
    }
}

/// #978 の DB: sync する (cell の版数あり)、 表 t (枠 3、 名前 k、 v = Number)、 peer A = 1。 名前 7 (Tag は "n7") の X に v = 70 を書き、
/// 名前 1 の行も置いて全部書き出す。 `reuse` = 1 度使い回して v15 にする (X が前の住人 W の場所を使い回す)。
fn named_x_engine_978(db: &Path, name: Name978, reuse: bool) -> (Arc<Engine>, u64) {
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
    eng.define_table("t", 3).unwrap();
    let kt = if name.tag() { ValueType::Tag } else { ValueType::Number };
    eng.define_himo_in("t", "k", kt, 0).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    let k = eng.himo_id("t.k").unwrap() as u16;
    eng.set_table_pk("t", k).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let tag = name.tag();
    let name_it = |e: u64, n: u32| {
        if tag {
            eng.tie_text_to(e, "t.k", &format!("n{n}"));
        } else {
            eng.tie_to(e, "t.k", n);
        }
    };
    let x = if reuse {
        let w = eng.entity_in("t").unwrap();
        name_it(w, 100);
        let keep = eng.entity_in("t").unwrap();
        name_it(keep, 1);
        let fill = eng.entity_in("t").unwrap();
        name_it(fill, 2);
        eng.delete(w);
        let x = eng.entity_in("t").unwrap();
        assert_eq!(eng.eid_slot(x), eng.eid_slot(w), "前提: 使い回した (v15)");
        x
    } else {
        let keep = eng.entity_in("t").unwrap();
        name_it(keep, 1);
        eng.entity_in("t").unwrap()
    };
    name_it(x, 7);
    eng.tie_to(x, "t.v", 70u32);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    assert_eq!(eng.slot_bits() > 0, reuse, "前提: v15 か (場所を使い回した DB)");
    (eng, x)
}

/// X の名前 (Number は数、 Tag は語)。
fn name_978(eng: &Engine, x: u64, name: Name978) -> Option<String> {
    if name.tag() {
        eng.get_text_owned(x, "t.k").map(|b| String::from_utf8_lossy(&b).into_owned())
    } else {
        eng.get(x, "t.k").map(|v| v.to_string())
    }
}

/// state と bridge が配る、 X 宛ての Delete の版数。
fn deletes_of_978(eng: &Engine, x: u64) -> (Vec<Hlc>, Vec<Hlc>) {
    let in_state = eng
        .state_records()
        .0
        .iter()
        .filter_map(|r| match r.op {
            DecodedOp::Delete { eid } if eid == x => Some(r.hlc),
            _ => None,
        })
        .collect();
    let bridged = bridged_deletes(eng).into_iter().filter(|&(e, _)| e == x).map(|(_, h)| h).collect();
    (in_state, bridged)
}

/// #978 の像を撮る: X の v を 71 に書き (WAL の checkpoint の後ろに X の Tie)、 本体の書き出し (WAL の書き出しの後) の中身の段の後
/// (`BeforeGate`) に A のアプリが X を消し、 削除が落とした名前の cell が disk に届いた段 (`Name978::shot`) で撮る。 WAL に X の Delete は
/// 無い。 X と削除の版数を返す。
fn dropped_name_image_978(live: &Path, img: &Path, name: Name978, reuse: bool) -> (u64, Hlc) {
    let (eng, x) = named_x_engine_978(&live.join("db"), name, reuse);
    let _hold = eng.hold_consumer();
    eng.tie_to(x, "t.v", 71u32);
    if name == Name978::TagFresh {
        let z = eng.entity_in("t").unwrap();
        eng.tie_text_to(z, "t.k", "n3"); // 書き出していない語 = 名前の列に門の印
    }
    eng.oplog_commit();
    let deleted: Arc<std::sync::Mutex<Option<Hlc>>> = Arc::new(std::sync::Mutex::new(None));
    let shot = Arc::new(AtomicBool::new(false));
    let (e2, d2, s2, l, i) = (eng.clone(), deleted.clone(), shot.clone(), live.to_path_buf(), img.to_path_buf());
    let at = name.shot();
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::BeforeGate && d2.lock().unwrap().is_none() {
            e2.delete(x);
            e2.oplog_commit();
            *d2.lock().unwrap() = Some(e2.tombstone_hlc(x));
        }
        if p == at && d2.lock().unwrap().is_some() && !s2.swap(true, Ordering::SeqCst) {
            let _ = crashsim::capture(&l, &i, Mode::Lost);
        }
    })));
    eng.oplog_sync().unwrap();
    write_out_hook::set(None);
    assert!(shot.load(Ordering::SeqCst), "前提: 書き出しの {at:?} で撮った");
    let tomb = deleted.lock().unwrap().expect("前提: 書き出しの途中で X を消した");
    (x, tomb)
}

/// #978: 本体の書き出しの途中で消した行 X の、 削除が落とした名前の cell が disk に届き、 削除の record は WAL に無い像
/// (`dropped_name_image_978`)。 開いた A の X は名前の無い行として生きない: 名前の cell は checkpoint の手前に書いたので再生で戻らない =
/// 削除は 「あった」 しか無い。 X は消え、 state の X の Delete と #451 の作り直しが配る X の Delete が一致する。 きれいに閉じて開き直しても同じ。
///
/// 前は tomb.seg を書き出しの最後 (版数の列の後) にだけ書き出していて、 この像に tombstone が無かった。 再生が checkpoint の後ろの X の
/// Tie (v = 71) で X を生かし直し (`k None, v 71`)、 #451 の作り直しも開く時の sweep も削除を知らず、 Delete を配らなかった (相手は
/// `k 7, v 71` のまま = 永久に食い違う)。
fn a_dropped_name_is_not_revived_without_its_delete(tag: &str, name: Name978, reuse: bool) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb) = dropped_name_image_978(&live, &img, name, reuse);
    crashsim::stop();
    let db = img.join("db");
    // 再生の前の像 (readonly は WAL を見ない): 削除が落とした X の名前の cell は disk に届いている
    let before = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (name_978(&ro, x, name), ro.tombstone_hlc(x), ro.is_live(x))
    };
    let first = {
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let out = (
            eng.is_live(x),
            name_978(&eng, x, name),
            eng.get(x, "t.v"),
            deletes_of_978(&eng, x),
            eng.unlogged_writes_relogged(),
        );
        eng.oplog_sync().unwrap();
        out
    };
    let second = {
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        (eng.is_live(x), eng.get(x, "t.v"), deletes_of_978(&eng, x).0, eng.verify_invariants().violations)
    };
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[978 {tag}] 像: 名前 {:?} tomb.seg {:?} live {} / 1 回目: {first:?} / 2 回目: {second:?}", before.0, before.1, before.2);
    assert_eq!(before.0, None, "前提: 削除が落とした X の名前の cell が disk に届いた像");
    let (alive, k, v, (in_state, bridged), _) = first;
    assert!(!alive, "再生が X を名前の無い行として生かし直した (名前 {k:?}、 v {v:?}、 像の tomb.seg {:?}、 state の Delete {in_state:?})", before.1);
    assert_eq!(in_state, vec![tomb], "state の X の Delete が無い / 削除の版数でない");
    assert_eq!(bridged, vec![tomb], "作り直して配る X の Delete が無い / 1 つでない");
    assert_eq!(second, (false, None, vec![tomb], Vec::<String>::new()), "開き直すと答えが変わった");
}

#[test]
fn a_dropped_number_name_is_not_revived_without_its_delete_v14() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_dropped_name_is_not_revived_without_its_delete("dropped_name_number_v14", Name978::Number, false);
}

#[test]
fn a_dropped_number_name_is_not_revived_without_its_delete_v15() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_dropped_name_is_not_revived_without_its_delete("dropped_name_number_v15", Name978::Number, true);
}

#[test]
fn a_dropped_tag_name_in_a_marked_column_is_not_revived_without_its_delete() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_dropped_name_is_not_revived_without_its_delete("dropped_name_tag_fresh", Name978::TagFresh, false);
}

#[test]
fn a_dropped_tag_name_is_not_revived_without_its_delete_v14() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_dropped_name_is_not_revived_without_its_delete("dropped_name_tag_v14", Name978::TagColumn, false);
}

#[test]
fn a_dropped_tag_name_is_not_revived_without_its_delete_v15() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_dropped_name_is_not_revived_without_its_delete("dropped_name_tag_v15", Name978::TagColumn, true);
}

/// #978 の削除の門: 書き出しが生きている印・残りの列の前に削除の門を閉じて tomb.seg を書き出した後 (`DeletesClosed`) に、 別の thread で
/// X を消す。 消す書き手は門が開くまで cell を落とさない (門で待ち始めたら書き出しを進める)。 版数の列の前 (`BeforeVersions`) で撮った像は、
/// 落とした cell が届いていない (X は名前のまま生き、 削除は WAL に無い = 無かった。 配る物にも無い)。
///
/// 門が無いと、 消す書き手が tomb.seg の書き出しの後に tombstone を置いて cell を落とし、 残りの列の msync がその cell を拾う
/// (tombstone は書き出しの最後まで届かない) = 再生が X を名前の無い行として生かし直した。
#[test]
fn a_delete_during_the_column_write_out_waits_for_the_tombstones() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("delete_gate_978");
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, waits) = {
        let (eng, x) = named_x_engine_978(&live.join("db"), Name978::Number, false);
        let _hold = eng.hold_consumer();
        eng.tie_to(x, "t.v", 71u32); // checkpoint の後ろの X の Tie (残りの列に書き出す物)
        eng.oplog_commit();
        let done = delete_during_write_out(&eng, x, Phase::DeletesClosed, Phase::BeforeVersions, &live, &img);
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(done.load(Ordering::SeqCst), "前提: 書き出しの間に消した");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while eng.is_live(x) {
            assert!(std::time::Instant::now() < deadline, "門が開いた後も X が消えない");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        (x, eng.delete_gate_waits())
    };
    crashsim::stop();
    let db = img.join("db");
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let (alive, k, v) = (eng.is_live(x), eng.get(x, "t.k"), eng.get(x, "t.v"));
    let (in_state, bridged) = deletes_of_978(&eng, x);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[978 gate] alive {alive} k {k:?} v {v:?} state {in_state:?} bridged {bridged:?} waits {waits}");
    assert!(!alive || k.is_some(), "再生が X を名前の無い行として生かし直した (v {v:?}、 state の Delete {in_state:?})");
    assert!(alive, "門で待った削除の cell も tombstone も届いていないのに X が消えた");
    assert_eq!((k, v), (Some(7), Some(71)), "X の名前 / 値が書き出しの返った値でない");
    assert!(in_state.is_empty() && bridged.is_empty(), "WAL に無い削除を配る ({in_state:?} / {bridged:?})");
    assert!(waits >= 1, "前提: 消す書き手が門で待った");
}

/// #978 の読み直し [1]: checkpoint の後ろに record の無い行 X (名前 k = 7、 書き出し済み) を、 本体の書き出しの `prev.seg` の後・生きている
/// 印 (entities) の前 (`PrevWrittenOut`) に A のアプリが消し、 段 `shot` (生きている印 / 場所の世代を書き出した後、 残りの列の前) で電源が
/// 落ちる。 削除は WAL の書き出しの後 = WAL に X の Delete は無い。 X の削除は 「あった」 (生きている印に届いた): 開いた A の X は消えていて、
/// state と #451 の作り直しが X の Delete を配る。 きれいに閉じて開き直しても同じ。
///
/// 前は削除の門を残りの列の前 (`BeforeRest` の後) で閉じていて、 生きている印と gen.seg が tomb.seg より先に届いた: 像は 「X は生きて
/// いない (gen は死)、 tomb.seg は 0、 cell は残る」。 再生に X の record は無く、 突き合わせは両方死なので何もせず (v14 は突き合わせ自体が
/// 無い)、 #451 も sweep も tombstone が無いので削除を知らない = X は A でだけ黙って消え、 Delete は state にも作り直しにも出ず、 相手は
/// `k 7` のまま食い違った。
fn a_record_less_delete_torn_before_the_tombstones_is_relogged(tag: &str, reuse: bool, shot: Phase) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let (live, img) = (root.join("live"), root.join("img"));
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let (x, tomb) = {
        let (eng, x) = named_x_engine_978(&live.join("db"), Name978::Number, reuse);
        let _hold = eng.hold_consumer();
        // 書き出す物 (X でない行。 X には checkpoint の後ろの record を作らない)
        let keep = eng.pull_raw("t.k", 1u32)[0];
        eng.tie_to(keep, "t.v", 5u32);
        eng.oplog_commit();
        let deleted: Arc<std::sync::Mutex<Option<Hlc>>> = Arc::default();
        let shot_once = Arc::new(AtomicBool::new(false));
        let (e2, d2, s2, l, i) = (eng.clone(), deleted.clone(), shot_once.clone(), live.clone(), img.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::PrevWrittenOut && d2.lock().unwrap().is_none() {
                e2.delete(x);
                *d2.lock().unwrap() = Some(e2.tombstone_hlc(x));
            }
            if p == shot && d2.lock().unwrap().is_some() && !s2.swap(true, Ordering::SeqCst) {
                let _ = crashsim::capture(&l, &i, Mode::Lost);
            }
        })));
        eng.oplog_sync().unwrap();
        write_out_hook::set(None);
        assert!(shot_once.load(Ordering::SeqCst), "前提: 書き出しの {shot:?} で撮った");
        let tomb = deleted.lock().unwrap().expect("前提: 書き出しの途中で X を消した");
        (x, tomb)
    };
    crashsim::stop();
    let db = img.join("db");
    let in_wal = OpLog::open(&db.join("oplog")).unwrap().records_with_tail().iter().any(|r| r.hlc == tomb);
    // 再生の前の像 (readonly は WAL を見ない)
    let (live_in_image, tomb_in_image, name_in_image) = {
        let ro = Engine::open_readonly(db.to_str().unwrap()).unwrap();
        (ro.is_live(x), ro.tombstone_hlc(x), ro.get(x, "t.k"))
    };
    let first = {
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let out = (eng.is_live(x), eng.get(x, "t.k"), deletes_of_978(&eng, x));
        eng.oplog_sync().unwrap();
        out
    };
    let second = {
        let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        (eng.is_live(x), deletes_of_978(&eng, x).0, eng.verify_invariants().violations)
    };
    let _ = std::fs::remove_dir_all(&root);
    eprintln!(
        "[978 r1 {tag}] 像: live {live_in_image} tomb.seg {tomb_in_image:?} 名前 {name_in_image:?} / 1 回目: {first:?} / 2 回目: {second:?}"
    );
    assert!(!in_wal, "前提: X の Delete は WAL に無い (書き出しの WAL の書き出しの後に消した)");
    assert!(!live_in_image, "前提: X の削除は生きている印に届いた ({shot:?})");
    let (alive, k, (in_state, bridged)) = first;
    assert!(!alive, "前提: 開いた A の X は消えている (名前 {k:?})");
    assert_eq!(
        in_state,
        vec![tomb],
        "生きている印に届いた削除の Delete が state に無い (像の tomb.seg {tomb_in_image:?}: 相手は k 7 のまま食い違う)"
    );
    assert_eq!(bridged, vec![tomb], "生きている印に届いた削除の Delete を作り直して配らない (像の tomb.seg {tomb_in_image:?})");
    assert_eq!(second, (false, vec![tomb], Vec::<String>::new()), "開き直すと答えが変わった");
}

#[test]
fn a_record_less_delete_torn_before_the_rest_is_relogged_v14() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_record_less_delete_torn_before_the_tombstones_is_relogged("r1_rest_v14", false, Phase::BeforeRest);
}

#[test]
fn a_record_less_delete_torn_before_the_rest_is_relogged_v15() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_record_less_delete_torn_before_the_tombstones_is_relogged("r1_rest_v15", true, Phase::BeforeRest);
}

#[test]
fn a_record_less_delete_torn_before_the_generations_is_relogged_v14() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_record_less_delete_torn_before_the_tombstones_is_relogged("r1_gen_v14", false, Phase::BeforeGenerations);
}

#[test]
fn a_record_less_delete_torn_before_the_generations_is_relogged_v15() {
    if std::env::var(VERIFY_ENV).is_ok() {
        return;
    }
    a_record_less_delete_torn_before_the_tombstones_is_relogged("r1_gen_v15", true, Phase::BeforeGenerations);
}
