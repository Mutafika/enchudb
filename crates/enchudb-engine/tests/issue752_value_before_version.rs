//! #752: 値だけがディスクに届いた自分の write (#705 の逆の像) が、 相手に配られないまま本体に残らないこと。
//!
//! sync する DB の書き出しは 「oplog を fsync → 本体 (列 → 版数の列)」 の順。 oplog の fsync の後に書いた write は、 列
//! (`himo/NNNN.seg`) の msync と版数の列 (`ver/NNNN.seg`) の msync の間で電源が落ちると 「値は新しい・版数は古い・record は
//! oplog に無い」 像になる。 開く時の照合 (#451 `relog_unlogged_writes`) は版数で 「oplog に無い write」 を探すので、 版数の
//! 古いこの cell を拾わない。
//!
//! 像は書き出しを段の切れ目で止めて決定的に作る (時間に頼らない): `Phase::BeforeGate` (oplog の fsync の後・列の前) で
//! 書き、 `Phase::BeforeVersions` (列を全部書き出した後・版数の列の前) で像を撮る。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::engine::write_out_hook::{self, Phase};
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::{decode_sync_ops_payload, DecodedOp, OpLog};
use enchudb_oplog::{eid_local, EntityId, Hlc};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const OPLOG_CAP: usize = 4 * 1024 * 1024;

/// crashsim の控えと hook は process に 1 つなので、 試験は 1 本ずつ。
static SERIAL: Mutex<()> = Mutex::new(());

/// 一時 DB の置き場 (Drop で必ず消す)。
struct Scratch(PathBuf);
impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu_issue752_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(p.join("live")).unwrap();
        Scratch(p)
    }
    fn live(&self) -> PathBuf {
        self.0.join("live")
    }
    fn img(&self) -> PathBuf {
        self.0.join("img")
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        write_out_hook::set(None);
        crashsim::set_after_copy_hook(None);
        crashsim::stop();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct ConsumerHeld(Option<mpsc::Sender<()>>);
impl Drop for ConsumerHeld {
    fn drop(&mut self) {
        if let Some(r) = self.0.take() {
            let _ = r.send(());
        }
        crashsim::set_after_copy_hook(None);
    }
}

/// consumer を周期の oplog の書き出し (写した直後) で止める (`power_loss.rs` の `hold_consumer` と同じ)。 周期の書き出しが
/// 試験の作る像に割り込まないように。
fn hold_consumer(eng: &Arc<Engine>) -> ConsumerHeld {
    let (parked_tx, parked_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let hold = Mutex::new(Some((parked_tx, release_rx)));
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
        if what == Copied::Oplog && std::thread::current().name() == Some("enchudb-consumer") {
            let taken = hold.lock().unwrap().take();
            if let Some((parked, release)) = taken {
                let _ = parked.send(());
                let _ = release.recv();
            }
        }
    })));
    eng.oplog_commit();
    parked_rx.recv_timeout(Duration::from_secs(60)).expect("前提: consumer が止まる");
    ConsumerHeld(Some(release_tx))
}

/// 試す列の種類。
#[derive(Clone, Copy, PartialEq, Debug)]
enum Col {
    Tag,
    Number,
    Leaf,
}

struct Rig {
    /// engine より先に落とす (欄は書いた順に落ちる): consumer を放してからでないと、 engine の Drop が consumer を待ち続ける
    _held: ConsumerHeld,
    eng: Arc<Engine>,
    /// 試す cell の行
    a: EntityId,
    hid: u16,
    col: Col,
}

const NAME: &str = "notes.v";

impl Rig {
    /// 表 notes (列 v) を作り、 行 a の v に 「古い値」 を書いて書き出しを返す (= 古い値と古い版数は disk にある)。
    fn new(s: &Scratch, col: Col, sync: bool) -> Self {
        let db = s.live().join("db");
        crashsim::start();
        let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
        eng.define_table("notes", 1_000).unwrap();
        let vt = match col {
            Col::Tag => ValueType::Tag,
            Col::Number => ValueType::Number,
            Col::Leaf => ValueType::Leaf,
        };
        eng.define_himo_in("notes", "v", vt, 0).unwrap();
        if sync {
            eng.enable_sync_tables().unwrap();
        }
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
        eng.set_peer_id(1);
        let hid = eng.himo_id(NAME).unwrap() as u16;
        let [a, b] = [(); 2].map(|_| eng.entity_in("notes").unwrap());
        let rig = Rig { eng: eng.clone(), a, hid, col, _held: ConsumerHeld(None) };
        rig.write_local(a, false);
        // 「新しい値」 の語は先に辞書に入れて書き出しておく (Tag: 版数だけ届く像で、 語が無いせいで読めない形と混ぜない)
        rig.write_local(b, true);
        eng.oplog_sync().unwrap();
        eng.oplog_sync().unwrap();
        let held = hold_consumer(&eng);
        Rig { _held: held, ..rig }
    }

    /// 自分の書き込み: 行 `e` の v を 古い値 / 新しい値 にする。
    fn write_local(&self, e: EntityId, new: bool) {
        match self.col {
            Col::Number => self.eng.tie_to(e, NAME, if new { 2u32 } else { 1u32 }),
            Col::Tag | Col::Leaf => self.eng.tie_text_to(e, NAME, if new { "new" } else { "old" }),
        }
    }
}

/// 行 a の v を文字列で (Number は 10 進)。
fn read(eng: &Engine, a: EntityId, col: Col) -> Option<String> {
    match col {
        Col::Number => eng.get(a, NAME).map(|v| v.to_string()),
        Col::Tag | Col::Leaf => eng.get_text_owned(a, NAME).map(|b| String::from_utf8_lossy(&b).into_owned()),
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dst = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), &dst).unwrap();
        }
    }
}

/// 像の本体だけを見る (oplog を外した写しを、 再生なしで開く): 行 a の (値, 版数)。
fn body_of_image(s: &Scratch, a: EntityId, hid: u16, col: Col) -> (Option<String>, Hlc) {
    let raw = s.0.join("raw");
    let _ = std::fs::remove_dir_all(&raw);
    copy_dir(&s.img().join("db"), &raw);
    let _ = std::fs::remove_file(raw.join("oplog"));
    let eng = Engine::open_standalone(raw.to_str().unwrap()).unwrap();
    let got = (read(&eng, a, col), eng.cell_hlc(a, hid));
    drop(eng);
    let _ = std::fs::remove_dir_all(&raw);
    got
}


/// 配る分 (`_sync_ops`) に載っている、 行 a の列 v の最後の record の (HLC, 値)。
fn last_distributed(eng: &Engine, a: EntityId, hid: u16) -> Option<(Hlc, Option<u64>)> {
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| decode_sync_ops_payload(p))
        .filter_map(|r| match &r.op {
            DecodedOp::Tie { eid, himo_id, value } if eid_local(*eid) == eid_local(a) && *himo_id == hid => {
                Some((r.hlc, Some(*value as u64)))
            }
            DecodedOp::Untie { eid, himo_id } if eid_local(*eid) == eid_local(a) && *himo_id == hid => Some((r.hlc, None)),
            _ => None,
        })
        .max_by_key(|(h, _)| *h)
}

/// oplog の fsync の後に書いた自分の write が、 列だけ書き出された所で電源が落ちる。 開いた後、 本体の値と配る分の最後の
/// record の値が同じであること (新しい値を配り直すか、 古い値に戻すかは問わない)。
#[test]
fn own_number_write_with_value_only_is_not_left_undistributed() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new("own_number");
    let rig = Rig::new(&s, Col::Number, true);
    let (eng, a, hid) = (rig.eng.clone(), rig.a, rig.hid);
    let old_hlc = eng.cell_hlc(a, hid);
    assert_eq!(read(&eng, a, Col::Number).as_deref(), Some("1"));

    // 書き出し (oplog fsync → 本体): oplog の fsync の後・列の前 (BeforeGate) で a を書き換え、 列を書き出した後・版数の列の前
    // (BeforeVersions) で電源が落ちた像を撮る
    let new_hlc = Arc::new(Mutex::new(Hlc::ZERO));
    {
        let (live, img) = (s.live(), s.img());
        let (wrote, shot) = (Mutex::new(false), Mutex::new(false));
        let (e2, got) = (eng.clone(), new_hlc.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if std::thread::current().name() != Some("issue752-sync") {
                return;
            }
            if p == Phase::BeforeGate && !std::mem::replace(&mut *wrote.lock().unwrap(), true) {
                e2.tie_to(a, NAME, 2u32);
                *got.lock().unwrap() = e2.cell_hlc(a, hid);
            }
            if p == Phase::BeforeVersions && !std::mem::replace(&mut *shot.lock().unwrap(), true) {
                crashsim::capture(&live, &img, Mode::Lost).unwrap();
            }
        })));
        let eng = eng.clone();
        std::thread::Builder::new().name("issue752-sync".into()).spawn(move || eng.oplog_sync().unwrap()).unwrap().join().unwrap();
        write_out_hook::set(None);
    }
    crashsim::stop();
    let new_hlc = *new_hlc.lock().unwrap();
    assert!(new_hlc > old_hlc, "前提: 書き出しの途中で a を書き換えた");
    drop(rig);
    drop(eng);
    assert!(s.img().exists(), "前提: 像を撮れた");

    // 前提: 像の本体は 「値は新しい、 版数は古い」。 像の oplog に新しい値の record は無い
    let img_db = s.img().join("db");
    let (raw_value, raw_ver) = body_of_image(&s, a, hid, Col::Number);
    assert_eq!(raw_value.as_deref(), Some("2"), "前提: 像の本体の値は新しい");
    assert_eq!(raw_ver, old_hlc, "前提: 像の本体の版数は古いまま");
    let in_oplog = OpLog::open(&img_db.join("oplog")).unwrap().records_with_tail().iter().any(|r| r.hlc == new_hlc);
    assert!(!in_oplog, "前提: 像の oplog に新しい値の record は無い");

    let back = Engine::open_concurrent_with_oplog(img_db.to_str().unwrap(), OPLOG_CAP).unwrap();
    let local = back.get(a, NAME);
    let ver = back.cell_hlc(a, hid);
    let dist = last_distributed(&back, a, hid);
    eprintln!("#752: 開いた後の本体 = {local:?} (版数 {ver:?})、 配る分の最後の record = {dist:?}");
    let (dist_hlc, dist_value) = dist.expect("配る分に a の record がある");
    assert_eq!(local, dist_value, "本体の値 {local:?} が、 配る分の最後の record の値 {dist_value:?} と違う (相手に届かない write)");
    assert_eq!(ver, dist_hlc, "本体の版数が、 配る分の最後の record の HLC と違う");
    drop(back);
}
