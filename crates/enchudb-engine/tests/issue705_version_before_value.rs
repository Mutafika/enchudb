//! #705: 版数だけがディスクに届いた cell に、 同じ HLC の record を当て直せること。
//!
//! 書き手は 値 → 版数 の順に書き、 本体の書き出し (`write_out_body`) は 列 (`himo/NNNN.seg`) → 版数の列 (`ver/NNNN.seg`) の
//! 順に segment ごとに msync する。 その間に入った write は 「版数だけ disk、 値は前のまま」 になる。 前は同じ HLC の record を
//! 無条件に弾いていたので、 電源断の後の再生 (自分の write) も再配送 (相手の write) もその cell を直せなかった。
//!
//! 像は書き出しを段の切れ目で止めて決定的に作る (時間に頼らない):
//! - `Phase::Opened` (中身を指す列を書き出した後): Tag の列。 hook を足す前の版にもある段なので、 直しを全部外しても試験が組める
//! - `Phase::BeforeVersions` (列を全部書き出した後・版数の列の前、 #705 で足した段): Number / Leaf / untie
//!
//! どの試験も、 開き直す前に 「像が本当に版数だけ届いた形か」 を確かめる (前提を手渡さない)。

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
        let p = std::env::temp_dir().join(format!("enchu_issue705_{}_{tag}", std::process::id()));
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

    /// 相手 (peer 2) の record を当てる: 行 a の v を新しい値にする。 当たったか。
    fn apply_remote(&self, eng: &Engine, hlc: Hlc) -> bool {
        match self.col {
            Col::Number => eng.remote_tie_apply(self.a, self.hid, 2u32, hlc),
            Col::Tag => eng.remote_tie_apply(self.a, self.hid, eng.vocab_id("new").unwrap(), hlc),
            Col::Leaf => eng.remote_tieleaf_apply(self.a, self.hid, b"new", hlc).applied(),
        }
    }

    /// 書き出しを `phase` (列を書き出した後・版数の列を書き出す前) で止め、 その間に `during` を呼んでから書き出しを終える。
    /// = `during` の書いた cell は、 版数だけが書き出される。
    fn write_out_with(&self, phase: Phase, during: impl FnOnce()) {
        if phase == Phase::Opened {
            // 列 v に印を立てる (新しい語) = 書き出しは v を 「門を閉じてまとめて書く列」 に入れる (Opened の前に msync される)
            assert_eq!(self.col, Col::Tag);
            let c = self.eng.entity_in("notes").unwrap();
            self.eng.tie_text_to(c, NAME, "a-brand-new-word-to-mark-the-column");
        }
        let (paused_tx, paused_rx) = mpsc::channel::<()>();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let (paused_tx, go_rx) = (Mutex::new(Some(paused_tx)), Mutex::new(go_rx));
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == phase
                && std::thread::current().name() == Some("issue705-body")
                && let Some(tx) = paused_tx.lock().unwrap().take()
            {
                tx.send(()).unwrap();
                go_rx.lock().unwrap().recv_timeout(Duration::from_secs(60)).expect("合図が来ない");
            }
        })));
        let body = {
            let eng = self.eng.clone();
            std::thread::Builder::new().name("issue705-body".into()).spawn(move || eng.body_msync().unwrap()).unwrap()
        };
        paused_rx.recv_timeout(Duration::from_secs(60)).expect("前提: 書き出しが段の切れ目で止まる");
        during();
        go_tx.send(()).unwrap();
        body.join().unwrap();
        write_out_hook::set(None);
    }
}

/// 行 a の v を文字列で (Number は 10 進)。
fn read(eng: &Engine, a: EntityId, col: Col) -> Option<String> {
    match col {
        Col::Number => eng.get(a, NAME).map(|v| v.to_string()),
        Col::Tag | Col::Leaf => eng.get_text_owned(a, NAME).map(|b| String::from_utf8_lossy(&b).into_owned()),
    }
}

fn old_of(col: Col) -> &'static str {
    if col == Col::Number { "1" } else { "old" }
}

fn new_of(col: Col) -> &'static str {
    if col == Col::Number { "2" } else { "new" }
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

/// 自分の書き込み: 版数だけ届いた後、 次の周期が oplog を fsync して、 本体の列を書き出す前に電源が落ちる。 開く時の再生が
/// その record を当て直すこと。
fn own_write_is_replayed(col: Col, phase: Phase, sync: bool) {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new(&format!("own_{col:?}_{sync}"));
    let rig = Rig::new(&s, col, sync);
    let (eng, a, hid) = (rig.eng.clone(), rig.a, rig.hid);
    let old_hlc = eng.cell_hlc(a, hid);

    // 1) 列を書き出した後・版数の列を書き出す前に a を書き換える (WAL に record → 値 → 版数)
    rig.write_out_with(phase, || rig.write_local(a, true));
    let new_hlc = eng.cell_hlc(a, hid);
    assert!(new_hlc > old_hlc);

    // 2) 次の書き出し: oplog を fsync した後、 本体の列を書き出す前 (BeforeGate) に電源が落ちた像
    {
        let (live, img) = (s.live(), s.img());
        let once = Mutex::new(true);
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate
                && std::thread::current().name() == Some("issue705-sync")
                && std::mem::take(&mut *once.lock().unwrap())
            {
                crashsim::capture(&live, &img, Mode::Lost).unwrap();
            }
        })));
        let eng = eng.clone();
        std::thread::Builder::new().name("issue705-sync".into()).spawn(move || eng.oplog_sync().unwrap()).unwrap().join().unwrap();
        write_out_hook::set(None);
    }
    crashsim::stop();
    let new_vid = (col == Col::Tag).then(|| eng.vocab_id("new").unwrap());
    drop(rig);
    drop(eng);
    assert!(s.img().exists(), "前提: 像を撮れた");

    // 前提: 像の本体は 「値は古い、 版数は新しい」。 像の oplog には新しい値の record がある
    let img_db = s.img().join("db");
    let (raw_value, raw_ver) = body_of_image(&s, a, hid, col);
    assert_eq!(raw_value.as_deref(), Some(old_of(col)), "前提: 像の本体の値は古いまま");
    assert_eq!(raw_ver, new_hlc, "前提: 像の本体の版数は新しい record の物");
    let in_oplog = OpLog::open(&img_db.join("oplog")).unwrap().records_with_tail().iter().any(|r| r.hlc == new_hlc);
    assert!(in_oplog, "前提: 像の oplog に新しい値の record がある");

    let back = Engine::open_concurrent_with_oplog(img_db.to_str().unwrap(), OPLOG_CAP).unwrap();
    let got = read(&back, a, col);
    let ver = back.cell_hlc(a, hid);
    assert_eq!(got.as_deref(), Some(new_of(col)), "再生が record を当て直していない (版数 {ver:?} / record {new_hlc:?})");
    assert_eq!(ver, new_hlc, "当て直した cell の版数は record の HLC のまま");
    if sync {
        // 配る分にも同じ record が (1 回だけ) 載る = 本体と配る分が同じ値
        back.oplog_sync().unwrap();
        while back.transfer_oplog_to_sync_ops() > 0 {}
        let recs: Vec<_> = back.pending_sync_ops(0).iter().filter_map(|p| decode_sync_ops_payload(p)).collect();
        let mine: Vec<_> = recs
            .iter()
            .filter(|r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, .. } if eid_local(*eid) == eid_local(a) && *himo_id == hid))
            .collect();
        let last = mine.iter().max_by_key(|r| r.hlc).expect("配る分に a の record がある");
        assert_eq!(last.hlc, new_hlc, "配る分の a の最後の record は新しい値の record");
        if let (Some(vid), DecodedOp::Tie { value, .. }) = (new_vid, &last.op) {
            assert_eq!(*value, vid as u64);
        }
    }
    drop(back);
}

/// 相手の書き込み: 当てている最中に書き出しが 列 → 版数の列 の間に居て版数だけ届き、 受け手が当てた分を書き出す
/// (`persist_sync_state`、 pull の cursor を進める前) より前に電源が落ちる。 cursor は進んでいないので相手は同じ record を
/// もう一度配る。 それが当たること。 当たった後の同じ record は今までどおり弾くこと。
fn redelivery_is_applied(col: Col, phase: Phase) {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new(&format!("remote_{col:?}"));
    let rig = Rig::new(&s, col, true);
    let (eng, a, hid) = (rig.eng.clone(), rig.a, rig.hid);
    let old_hlc = eng.cell_hlc(a, hid);
    let remote = Hlc { wall: old_hlc.wall + 10_000, logical: 0, peer: 2 };

    rig.write_out_with(phase, || assert!(rig.apply_remote(&eng, remote), "前提: 受信の apply が当たった"));
    crashsim::capture(&s.live(), &s.img(), Mode::Lost).unwrap();
    crashsim::stop();
    drop(eng);

    let back = Engine::open_concurrent_with_oplog(s.img().join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    // 前提: 開いた直後は 「値は古い、 版数は相手の record の物」 (相手の record は自分の oplog に無いので、 再生は直さない)
    assert_eq!(read(&back, a, col).as_deref(), Some(old_of(col)), "前提: 像の値は古いまま");
    assert_eq!(back.cell_hlc(a, hid), remote, "前提: 像の版数は相手の record の物");

    assert!(rig.apply_remote(&back, remote), "再配送した相手の record が弾かれた (版数は record と同じ、 値は古い)");
    assert_eq!(read(&back, a, col).as_deref(), Some(new_of(col)));
    assert_eq!(back.cell_hlc(a, hid), remote);
    // 値がそろった後の同じ record は当てない (再配送の skip は今までどおり)
    assert!(!rig.apply_remote(&back, remote), "値のそろった cell に同じ record が二度当たった");
    assert_eq!(read(&back, a, col).as_deref(), Some(new_of(col)));
    drop(back);
    drop(rig);
}

#[test]
fn own_tag_write_is_replayed_sync() {
    own_write_is_replayed(Col::Tag, Phase::Opened, true);
}

#[test]
fn own_tag_write_is_replayed_plain() {
    own_write_is_replayed(Col::Tag, Phase::Opened, false);
}

#[test]
fn own_number_write_is_replayed() {
    own_write_is_replayed(Col::Number, Phase::BeforeVersions, true);
}

#[test]
fn own_leaf_write_is_replayed() {
    own_write_is_replayed(Col::Leaf, Phase::BeforeVersions, false);
}

#[test]
fn redelivered_tag_record_is_applied() {
    redelivery_is_applied(Col::Tag, Phase::Opened);
}

#[test]
fn redelivered_number_record_is_applied() {
    redelivery_is_applied(Col::Number, Phase::BeforeVersions);
}

#[test]
fn redelivered_leaf_record_is_applied() {
    redelivery_is_applied(Col::Leaf, Phase::BeforeVersions);
}

/// untie も同じ: 版数だけ届いた untie の再配送が、 残っている値を外すこと。
#[test]
fn redelivered_untie_is_applied() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let s = Scratch::new("remote_untie");
    let rig = Rig::new(&s, Col::Number, true);
    let (eng, a, hid) = (rig.eng.clone(), rig.a, rig.hid);
    let old_hlc = eng.cell_hlc(a, hid);
    let remote = Hlc { wall: old_hlc.wall + 10_000, logical: 0, peer: 2 };

    rig.write_out_with(Phase::BeforeVersions, || assert!(eng.remote_untie_apply(a, hid, remote), "前提: 受信の untie が当たった"));
    assert_eq!(eng.get(a, NAME), None);
    crashsim::capture(&s.live(), &s.img(), Mode::Lost).unwrap();
    crashsim::stop();
    drop(eng);

    let back = Engine::open_concurrent_with_oplog(s.img().join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    assert_eq!(back.get(a, NAME), Some(1), "前提: 像の値は残ったまま");
    assert_eq!(back.cell_hlc(a, hid), remote, "前提: 像の版数は相手の untie の物");
    assert!(back.remote_untie_apply(a, hid, remote), "再配送した相手の untie が弾かれた");
    assert_eq!(back.get(a, NAME), None);
    assert!(!back.remote_untie_apply(a, hid, remote), "外れた cell に同じ untie が二度当たった");
    drop(back);
    drop(rig);
}
