//! #769: client が送った state (`POST /v2/state`) をホストが中継する道で、 ホストが載せない record (#753: `as_of` より後の版数)
//! の中に Tag の語を運ぶ Vocab があっても、 その語が下の client に届く。
//!
//! state の中で、 語の Vocab は語ごとに 1 回だけ、 **最初に走査した cell の版数**で出る。 その cell が、 書き手が state を
//! 作っている間に書かれた物 (版数が as_of より後) だと、 ホストは Vocab を当てるが載せなかった。 2 つの形で語が届かなかった:
//!
//! 1. 同じ語を持つ他の行 (as_of 以前の版数) の Tie は載る。 下の client は語の写像を持たず、 Tie を捨てる (cursor は進む)。
//! 2. その語を持つのが作っている間に書いた cell だけの時: Tie は後で差分で届いて載るが、 差分の Vocab は、 ホストがもう
//!    写像を持っているので載らない (「前に当てた = 前に載せた」 が state の道で崩れていた)。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{StateBatch, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;
/// c1 が出さずに持つ行は 2..=M
const M: u32 = 3000;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue769-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Dir(dir)
    }
    fn db(&self, id: u32) -> String {
        self.0.join(format!("p{id}")).to_str().unwrap().to_string()
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 名前 `k` (Number)、 `v` (Number)、 `t` (Tag) を持つ表 `notes`。 state は列の順 (k → v → t) に走査する。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 262_144).unwrap();
        e.define_table("notes", 32_768).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 64 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

struct Client {
    eng: Arc<Engine>,
    t: Arc<HttpHostTransport>,
    s: Syncer,
}

fn client(eng: Arc<Engine>, url: &str) -> Client {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t.clone());
    s.serve_state();
    Client { eng, t, s }
}

/// state の配布元が 「engine の state をそのまま」 (`Syncer::serve_state` が自分の分として登録する物と同じ中身) の client。 送った
/// batch を控える (前提を確かめる)。
fn recording_client(eng: Arc<Engine>, url: &str) -> (Client, Arc<Mutex<Vec<StateBatch>>>) {
    let t = Arc::new(HttpHostTransport::connect(url.to_string(), eng.peer_id()).unwrap());
    let s = Syncer::new(eng.clone(), t.clone());
    let sent: Arc<Mutex<Vec<StateBatch>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let (weak, sent) = (Arc::downgrade(&eng), sent.clone());
        t.register_state_provider(
            eng.peer_id(),
            Arc::new(move || {
                let eng = weak.upgrade()?;
                let (records, as_of) = eng.state_records();
                let batch = StateBatch { records, as_of, complete: true };
                sent.lock().unwrap().push(batch.clone());
                Some(batch)
            }),
        );
    }
    (Client { eng, t, s }, sent)
}

impl Client {
    fn write(&self, k: u32, v: u32) -> u64 {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        e
    }
    fn row(&self, k: u32) -> Option<u64> {
        self.eng.pull_raw("notes.k", k).first().copied()
    }
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
    }
    /// 受ける。 抜けを言われたら、 言われた書き手を取り直して受け直す。 戻り値: 取り直した書き手。
    fn pull(&self) -> Vec<PeerId> {
        let mut booted = Vec::new();
        for _ in 0..4 {
            let out = self.s.pull_once(HOST);
            if !out.history_truncated {
                break;
            }
            for a in out.truncated_authors {
                assert!(self.s.bootstrap_pull_via(HOST, a).is_some(), "書き手 {a} の state を取れない");
                booted.push(a);
            }
        }
        booted
    }
    fn sync(&self) -> Vec<PeerId> {
        self.push();
        self.pull()
    }
    fn cursor(&self, author: PeerId) -> Hlc {
        self.s.cursors_of(HOST).into_iter().find(|(a, _)| *a == author).map_or(Hlc::ZERO, |(_, h)| h)
    }
}

/// 名前 `k` の行の `t` の語。
fn text(e: &Engine, k: u32) -> Option<Vec<u8>> {
    e.pull_raw("notes.k", k).first().and_then(|r| e.get_text_owned(*r, "notes.t"))
}

/// `t` が c1 と食い違う名前。
fn differing(c1: &Engine, other: &Engine) -> Vec<u32> {
    (1..=M).filter(|k| text(other, *k) != text(c1, *k)).collect()
}

struct Run {
    /// c1 が送った state
    batch: StateBatch,
    /// state の中継を受けた直後の、 c3 の書き手 1 の cursor
    c3_cursor_after_state: Hlc,
    /// 全部出して受けた後、 `t` が c1 と食い違う名前 (ホスト / c3)
    bad_host: Vec<u32>,
    bad_c3: Vec<u32>,
    /// c1 の `t` に値のある行の数
    with_text: usize,
    /// `t` の列の番号
    t_himo: u16,
}

/// c1 が行 2..=M を出さずに持ち (`shared` があれば全部の `t` にその語)、 「持っている record は配れない」 と言って publish する
/// → ホストが state を求める。 `late` があれば、 c1 が state を作っている間 (as_of を採った後、 列 `t` を走査する直前。
/// `Engine::set_state_scan_hook`) に行 2 の `t` にその語を書く (版数は as_of より後で state に入り、 行 2 は `t` で最初に走査する
/// cell)。 #890: 前は別の thread が 「作り始めた」 印を見てから書き、 `t` の走査に間に合うかは時間任せだった (負荷の下で 6 回とも
/// 間に合わず落ちた)。
///
/// None: この publish でホストが state を求めなかった (呼び手がやり直す)。
fn run(tag: &str, shared: Option<&str>, late: Option<&'static str>) -> Option<Run> {
    let dir = Dir::new(tag);
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, sent) = recording_client(db(&dir.db(1), 1), &url);
    let c3 = client(db(&dir.db(3), 3), &url);
    c1.write(1, 1);
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    assert!(c3.row(1).is_some(), "前提: c3 は c1 の行 1 を受けた");
    assert!(sent.lock().unwrap().is_empty(), "前提: まだ state は送っていない");

    for k in 2..=M {
        let e = c1.write(k, k);
        if let Some(word) = shared {
            c1.eng.tie_text_to(e, "notes.t", word);
        }
    }
    c1.eng.oplog_commit();
    c1.eng.oplog_sync().unwrap();
    // c1 の floor を自分の今の時計の上に置く (= 持っている record は配れない)。 次の publish でホストは c1 の state を求める
    let last = c1.row(M).unwrap();
    let top = ["notes.k", "notes.v", "notes.t"]
        .iter()
        .map(|h| c1.eng.cell_hlc(last, c1.eng.himo_id(h).unwrap() as u16))
        .max()
        .unwrap();
    c1.t.set_history_floor_multi(1, &[(1, Hlc { wall: top.wall, logical: top.logical + 1, peer: 1 })]);

    let t_himo = c1.eng.himo_id("notes.t").unwrap() as u16;
    let wrote = Arc::new(AtomicBool::new(false));
    if let Some(word) = late {
        let (weak, first, wrote) = (Arc::downgrade(&c1.eng), c1.row(2).unwrap(), wrote.clone());
        let write: Box<dyn FnOnce() + Send> = Box::new(move || {
            let Some(eng) = weak.upgrade() else { return };
            eng.tie_text_to(first, "notes.t", word);
            eng.oplog_commit();
            wrote.store(true, Ordering::Release);
        });
        c1.eng.set_state_scan_hook(Some((t_himo, write)));
    }
    c1.push();
    c1.eng.set_state_scan_hook(None);
    let n_sent = sent.lock().unwrap().len();
    if n_sent == 0 {
        println!("[{tag}] この publish でホストは state を求めなかった");
        return None;
    }
    assert_eq!(n_sent, 1, "前提: c1 は state を 1 回だけ送った");
    assert_eq!(wrote.load(Ordering::Acquire), late.is_some(), "前提: state の列 `t` の走査の前に書いた");
    let batch = sent.lock().unwrap()[0].clone();

    // c3 が state の中継を受ける
    assert_eq!(c3.pull(), Vec::<PeerId>::new(), "前提: c3 は取り直しなしで受けた");
    let c3_cursor_after_state = c3.cursor(1);
    // c1 が残り (作っている間の書き込みの差分) を出し、 c3 が受ける
    for _ in 0..3 {
        c1.sync();
        c3.sync();
    }
    let out = Run {
        batch,
        c3_cursor_after_state,
        bad_host: differing(&c1.eng, &host_eng),
        bad_c3: differing(&c1.eng, &c3.eng),
        with_text: (1..=M).filter(|k| text(&c1.eng, *k).is_some()).count(),
        t_himo,
    };
    drop((c1, c3, host));
    Some(out)
}

/// state の中の、 語 `word` の Vocab の (番号, 版数)。
fn vocab_of(batch: &StateBatch, word: &str) -> Option<(u32, Hlc)> {
    batch.records.iter().find_map(|r| match &r.op {
        DecodedOp::Vocab { vid, bytes } if bytes == word.as_bytes() => Some((*vid, r.hlc)),
        _ => None,
    })
}

/// 列 `t` で番号 `vid` を持つ Tie の版数。
fn ties_of(batch: &StateBatch, t_himo: u16, vid: u32) -> Vec<Hlc> {
    batch
        .records
        .iter()
        .filter_map(|r| match &r.op {
            DecodedOp::Tie { himo_id, value, .. } if *himo_id == t_himo && *value == vid as u64 => Some(r.hlc),
            _ => None,
        })
        .collect()
}

/// state を作っている間の書き込みが語 `late` の最初の cell になった回 (state の Vocab の版数が as_of より後)。 ホストが state を
/// 求めなかった回はやり直す。
fn run_with_late_vocab(tag: &str, shared: Option<&str>, late: &'static str) -> Run {
    let r = (0..6).find_map(|i| run(&format!("{tag}{i}"), shared, Some(late))).expect("前提: ホストが c1 の state を求めた");
    assert!(
        vocab_of(&r.batch, late).is_some_and(|(_, h)| h > r.batch.as_of),
        "前提: state を作っている間の書き込みが語 {late:?} の最初の cell (Vocab の版数が as_of より後)"
    );
    r
}

/// 形 1 (issue の形): 行 2..=M が全部同じ語を持ち、 c1 が state を作っている間に行 2 の語を書き直す。 state の Vocab は行 2 の
/// 版数 (as_of より後) で出る。 行 3..=M の Tie (as_of 以前) は載るので、 c3 に語も届かなければならない。
#[test]
fn a_word_whose_vocab_rides_a_cell_written_during_the_build_reaches_the_rows_below_the_host() {
    let r = run_with_late_vocab("a", Some("same-word"), "same-word");
    let (vid, vocab_hlc) = vocab_of(&r.batch, "same-word").unwrap();
    let ties = ties_of(&r.batch, r.t_himo, vid);
    let (before, after) = (ties.iter().filter(|h| **h <= r.batch.as_of).count(), ties.iter().filter(|h| **h > r.batch.as_of).count());
    println!("[a] as_of {:?} / Vocab の版数 {vocab_hlc:?} / その語の Tie: as_of 以前 {before}、 後 {after}", r.batch.as_of);
    assert_eq!((before, after), (M as usize - 2, 1), "前提: 語の Tie は行 2 だけが as_of より後、 行 3..=M は as_of 以前");
    assert_eq!(r.with_text, M as usize - 1, "前提: c1 の行 2..=M が語を持つ");
    assert_eq!(r.bad_host, Vec::<u32>::new(), "前提: ホストは c1 と同じ");
    assert!(
        r.bad_c3.is_empty(),
        "c1 の語が c3 に届かない行が {} 行 (例 k = {:?})",
        r.bad_c3.len(),
        &r.bad_c3[..r.bad_c3.len().min(5)]
    );
    assert!(
        r.c3_cursor_after_state <= r.batch.as_of,
        "#753: state の中継を受けた c3 の、 書き手 1 の cursor が as_of を越えた ({:?} > {:?})",
        r.c3_cursor_after_state,
        r.batch.as_of
    );
}

/// 形 2: 語を持つのが、 c1 が state を作っている間に書いた cell (行 2) だけ。 state の Vocab も Tie も as_of より後で、 ホストは
/// どちらも載せない。 Tie は後で差分で届いて載るが、 差分の Vocab はホストがもう写像を持っていて載らない。 c3 に語が届くこと。
#[test]
fn a_word_only_on_a_cell_written_during_the_build_reaches_a_client_below_the_host() {
    let r = run_with_late_vocab("b", None, "late-word");
    let (vid, vocab_hlc) = vocab_of(&r.batch, "late-word").unwrap();
    let ties = ties_of(&r.batch, r.t_himo, vid);
    println!("[b] as_of {:?} / Vocab の版数 {vocab_hlc:?} / その語の Tie {ties:?}", r.batch.as_of);
    assert_eq!(ties.len(), 1, "前提: 語を持つ cell は行 2 だけ");
    assert!(ties[0] > r.batch.as_of, "前提: その cell の版数は as_of より後 (ホストは Tie を state からは載せない)");
    assert_eq!(r.with_text, 1, "前提: c1 で語を持つのは行 2 だけ");
    assert_eq!(r.bad_host, Vec::<u32>::new(), "前提: ホストは c1 と同じ");
    assert_eq!(r.bad_c3, Vec::<u32>::new(), "c1 の行 2 の語が c3 に届かない");
    assert!(
        r.c3_cursor_after_state <= r.batch.as_of,
        "#753: state の中継を受けた c3 の、 書き手 1 の cursor が as_of を越えた ({:?} > {:?})",
        r.c3_cursor_after_state,
        r.batch.as_of
    );
}

/// 対照: state を作っている間に何も書かなければ、 Vocab は as_of 以前の版数で出て、 そのまま載る (直しの前も通る)。
#[test]
fn control_a_quiet_state_carries_the_word_below_the_host() {
    let r = (0..6).find_map(|i| run(&format!("c{i}"), Some("same-word"), None)).expect("前提: ホストが c1 の state を求めた");
    let (vid, vocab_hlc) = vocab_of(&r.batch, "same-word").unwrap();
    assert!(vocab_hlc <= r.batch.as_of, "前提: Vocab の版数は as_of 以前");
    assert!(r.batch.records.iter().all(|x| x.hlc <= r.batch.as_of), "前提: state に as_of より後の版数は無い");
    assert_eq!(ties_of(&r.batch, r.t_himo, vid).len(), M as usize - 1, "前提: 行 2..=M の語が state にある");
    assert_eq!(r.bad_host, Vec::<u32>::new(), "前提: ホストは c1 と同じ");
    assert_eq!(r.bad_c3, Vec::<u32>::new(), "c3 は c1 と同じ");
}
