//! review (0.37.4): 本体の書き出しは 列 (himo/NNNN.seg) → 版数の列 (ver/NNNN.seg) の順で segment ごとに msync する。
//! その間に書いた write は 「版数だけ disk、 値は前のまま」 になりうる。 その後 oplog の fsync が record を届かせてから
//! (本体の列を書き出す前に) 電源が落ちると、 開く時の再生は 「cell の版数が record と同じ = 本体に届いている」 と見て
//! 値を当てない (`apply_oplog_op` → `set_cell_local` が同じ HLC を弾く)。 結果: oplog (と配る分) には新しい値、 本体は古い値。
//!
//! 書き出しを `Phase::Opened` (中身を指す列を書き出した後・残り = 版数の列を書き出す前) で止めて決定的に作る。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::engine::write_out_hook::{self, Phase};
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::{decode_sync_ops_payload, DecodedOp, OpLog};
use enchudb_oplog::eid_local;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const OPLOG_CAP: usize = 4 * 1024 * 1024;

fn scratch(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("enchu_review_dur_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
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

/// power_loss.rs の `hold_consumer` と同じ: consumer を周期の oplog の書き出し (写した直後) で止める。
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
    parked_rx.recv_timeout(Duration::from_secs(20)).expect("前提: consumer が止まる");
    ConsumerHeld(Some(release_tx))
}

fn run(sync: bool) {
    let root = scratch(if sync { "vbv_sync" } else { "vbv_plain" });
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("notes", 1_000).unwrap();
    eng.define_himo_in("notes", "tag", ValueType::Tag, 0).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let tag = eng.himo_id("notes.tag").unwrap() as u16;
    let [a, b, c] = [(); 3].map(|_| eng.entity_in("notes").unwrap());
    eng.tie_text_to(a, "notes.tag", "old");
    eng.tie_text_to(b, "notes.tag", "new");
    // 書き出しが返った: a = "old" は durable
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    let old_hlc = eng.cell_hlc(a, tag);

    let held = hold_consumer(&eng);
    // 列 tag に印を立てる (新しい語) = 次の書き出しは tag を 「門を閉じてまとめて書く列」 に入れる (Opened の前に msync される)
    eng.tie_text_to(c, "notes.tag", "a-brand-new-word-to-mark-the-column");

    // 1) 本体の書き出しを、 列 tag を書き出した後・版数の列を書き出す前 (Opened) で止める
    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (paused_tx, go_rx) = (Mutex::new(Some(paused_tx)), Mutex::new(go_rx));
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::Opened && std::thread::current().name() == Some("review-body") {
            if let Some(tx) = paused_tx.lock().unwrap().take() {
                tx.send(()).unwrap();
                go_rx.lock().unwrap().recv_timeout(Duration::from_secs(20)).expect("合図が来ない");
            }
        }
    })));
    let body = {
        let eng = eng.clone();
        std::thread::Builder::new().name("review-body".into()).spawn(move || eng.body_msync().unwrap()).unwrap()
    };
    paused_rx.recv_timeout(Duration::from_secs(20)).expect("書き出しが Opened で止まらない");
    // 2) その間に a を書き換える (WAL に record → 値 → 版数)。 列 tag はもう msync 済み、 版数の列はこの後 msync される
    eng.tie_text_to(a, "notes.tag", "new");
    let new_hlc = eng.cell_hlc(a, tag);
    assert!(new_hlc > old_hlc);
    go_tx.send(()).unwrap();
    body.join().unwrap();

    // 3) 次の書き出し: oplog を fsync した後、 本体の列を書き出す前 (BeforeGate) に電源が落ちた像
    let img = root.join("img");
    {
        let (live, img) = (live.clone(), img.clone());
        let once = Mutex::new(true);
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate
                && std::thread::current().name() == Some("review-sync")
                && std::mem::take(&mut *once.lock().unwrap())
            {
                crashsim::capture(&live, &img, Mode::Lost).unwrap();
            }
        })));
    }
    {
        let eng = eng.clone();
        std::thread::Builder::new().name("review-sync".into()).spawn(move || eng.oplog_sync().unwrap()).unwrap().join().unwrap();
    }
    write_out_hook::set(None);
    crashsim::stop();
    drop(held);
    let new_vid = eng.vocab_id("new").unwrap();
    drop(eng);
    assert!(img.exists(), "像を撮れなかった");

    // 前提: 像の oplog に、 a を "new" にする record がある
    let img_db = img.join("db");
    let in_oplog = OpLog::open(&img_db.join("oplog")).unwrap().records_with_tail().iter().any(|r| r.hlc == new_hlc);
    assert!(in_oplog, "前提: 像の oplog に新しい値の record がある");

    let back = Engine::open_concurrent_with_oplog(img_db.to_str().unwrap(), OPLOG_CAP).unwrap();
    let got = back.get_text_owned(a, "notes.tag").map(|b| String::from_utf8_lossy(&b).into_owned());
    let ver = back.cell_hlc(a, tag);
    let mut distributed_new = None;
    if sync {
        back.oplog_sync().unwrap();
        while back.transfer_oplog_to_sync_ops() > 0 {}
        let recs: Vec<_> = back.pending_sync_ops(0).iter().filter_map(|p| decode_sync_ops_payload(p)).collect();
        distributed_new = Some(recs.iter().any(|r| {
            r.hlc == new_hlc
                && matches!(&r.op, DecodedOp::Tie { eid, himo_id, value } if eid_local(*eid) == eid_local(a) && *himo_id == tag && *value == new_vid as u64)
        }));
    }
    drop(back);
    let _ = std::fs::remove_dir_all(&root);
    eprintln!(
        "[review_dur] sync={sync} 開いた後: a.tag = {got:?}, cell の版数 = {ver:?} (old {old_hlc:?} / new {new_hlc:?}), 配る分に new の record = {distributed_new:?}"
    );
    // oplog に record があり cell の版数もその record の物なら、 値もその record の値のはず
    assert!(
        !(ver == new_hlc && got.as_deref() != Some("new")),
        "cell の版数は新しい record ({new_hlc:?}) なのに値は {got:?} (oplog の record は再生で弾かれた。 配る分に new = {distributed_new:?})"
    );
}

static SERIAL: Mutex<()> = Mutex::new(());

#[test]
fn replay_skips_a_durable_record_when_only_the_version_reached_disk_sync() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    run(true);
}

#[test]
fn replay_skips_a_durable_record_when_only_the_version_reached_disk_plain() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    run(false);
}

/// 受け手の側: 相手 (peer 2) の record を当てている最中 (値 → 版数) に周期の書き出しが 列 → 版数の列 の間に居ると、 版数だけ
/// disk に届く。 受け手が 「当てた分を書き出す」 (`persist_sync_state`、 pull の cursor を進める前) より前に電源が落ちると、
/// 開き直した後の再配送 (cursor は進んでいない) は同じ HLC なので LWW で弾かれ、 値は古いまま。
#[test]
fn a_redelivered_remote_record_is_refused_when_only_its_version_reached_disk() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("vbv_remote");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("notes", 1_000).unwrap();
    eng.define_himo_in("notes", "tag", ValueType::Tag, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, OPLOG_CAP).unwrap();
    eng.set_peer_id(1);
    let tag = eng.himo_id("notes.tag").unwrap() as u16;
    let [a, b, c] = [(); 3].map(|_| eng.entity_in("notes").unwrap());
    eng.tie_text_to(a, "notes.tag", "old");
    eng.tie_text_to(b, "notes.tag", "new");
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    let new_vid = eng.vocab_id("new").unwrap();
    let old_hlc = eng.cell_hlc(a, tag);
    let held = hold_consumer(&eng);
    eng.tie_text_to(c, "notes.tag", "a-brand-new-word-to-mark-the-column");

    let (paused_tx, paused_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (paused_tx, go_rx) = (Mutex::new(Some(paused_tx)), Mutex::new(go_rx));
    write_out_hook::set(Some(Arc::new(move |p: Phase| {
        if p == Phase::Opened && std::thread::current().name() == Some("review-body") {
            if let Some(tx) = paused_tx.lock().unwrap().take() {
                tx.send(()).unwrap();
                go_rx.lock().unwrap().recv_timeout(Duration::from_secs(20)).expect("合図が来ない");
            }
        }
    })));
    let body = {
        let eng = eng.clone();
        std::thread::Builder::new().name("review-body".into()).spawn(move || eng.body_msync().unwrap()).unwrap()
    };
    paused_rx.recv_timeout(Duration::from_secs(20)).expect("書き出しが Opened で止まらない");
    // 相手 (peer 2) が a を "new" に書き換えた record を当てる
    let remote = enchudb_oplog::Hlc { wall: old_hlc.wall + 10_000, logical: 0, peer: 2 };
    assert!(eng.remote_tie_apply(a, tag, new_vid, remote), "前提: 受信の apply が当たった");
    go_tx.send(()).unwrap();
    body.join().unwrap();
    write_out_hook::set(None);
    // 受け手が persist_sync_state (本体の書き出し → cursor) を呼ぶ前に電源断
    let img = root.join("img");
    crashsim::capture(&live, &img, Mode::Lost).unwrap();
    crashsim::stop();
    drop(held);
    drop(eng);

    let back = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), OPLOG_CAP).unwrap();
    let text = |e: &Engine| e.get_text_owned(a, "notes.tag").map(|b| String::from_utf8_lossy(&b).into_owned());
    let (before, ver) = (text(&back), back.cell_hlc(a, tag));
    // cursor は進んでいないので、 相手は同じ record をもう一度配る
    let again = back.remote_tie_apply(a, tag, back.vocab_id("new").unwrap(), remote);
    let after = text(&back);
    drop(back);
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[review_dur] remote: 開いた直後 a.tag = {before:?} 版数 {ver:?} (record {remote:?})、 再配送 applied = {again}、 その後 a.tag = {after:?}");
    assert_eq!(after.as_deref(), Some("new"), "再配送した相手の record が弾かれ、 値が古いまま (版数 {ver:?} は record と同じ)");
}
