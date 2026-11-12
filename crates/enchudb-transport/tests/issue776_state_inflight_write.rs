//! #776: state (`Engine::state_records`) の `as_of` より前に版数を採って、 まだ cell に着いていない書き込みも state に入る。
//!
//! 書き手は行を握ってから WAL に積み (そこで版数 h が決まる)、 それから cell に当てる。 state の `as_of` は作り始めに時計から
//! 採る。 h を採った後・cell に当てる前に `as_of` が採られ、 state の走査がその cell を先に読むと、 state には古い値が入り、
//! record は h < as_of。 ホストは state を当てた後 `cursor[c] = as_of` にして as_of 以下の c の record を受けない (#753) ので、
//! その書き込みは state にも差分にも乗らなかった。 今は、 as_of を採った後、 行を握っている書き手が離すのを待ってから走査する。
#![allow(dead_code)]

use std::collections::HashMap;
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

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue776-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `v` (Number) を持つ表 `notes`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 262_144).unwrap();
        e.define_table("notes", 65_536).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 64 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

/// 書き手 `threads` 本が、 それぞれ自分の行だけを書き続ける横で state を `builds` 回作る。 戻り値: state が落とした書き込み
/// (版数 h < as_of なのに、 state のその cell の版数が h より前) の数と、 見た書き込みの数。
fn stress(eng: &Arc<Engine>, threads: usize, rows_each: usize, writes_each: u32) -> (usize, usize, usize) {
    let v = eng.himo_id("notes.v").unwrap() as u16;
    let mut rows: Vec<Vec<u64>> = Vec::new();
    let mut k = 0u32;
    for _ in 0..threads {
        let mut mine = Vec::new();
        for _ in 0..rows_each {
            k += 1;
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.k", k);
            eng.tie_to(e, "notes.v", 0u32);
            mine.push(e);
        }
        rows.push(mine);
    }
    eng.oplog_commit();
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = rows
        .iter()
        .cloned()
        .map(|mine| {
            let (eng, stop) = (eng.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut log: Vec<(u64, Hlc)> = Vec::new();
                let mut i = 0u32;
                while !stop.load(Ordering::Acquire) && i < writes_each {
                    let row = mine[i as usize % mine.len()];
                    i += 1;
                    eng.try_tie_to_by_id(row, v, i).unwrap();
                    // この行を書くのはこの thread だけ = 今の版数は今の書き込みの版数
                    log.push((row, eng.cell_hlc(row, v)));
                }
                log
            })
        })
        .collect();
    let mut states: Vec<(HashMap<u64, Hlc>, Hlc)> = Vec::new();
    while writers.iter().any(|w| !w.is_finished()) {
        let (records, as_of) = eng.state_records();
        let cells: HashMap<u64, Hlc> = records
            .iter()
            .filter_map(|r| match &r.op {
                DecodedOp::Tie { eid, himo_id, .. } if *himo_id == v => Some((*eid, r.hlc)),
                _ => None,
            })
            .collect();
        states.push((cells, as_of));
    }
    stop.store(true, Ordering::Release);
    let logs: Vec<Vec<(u64, Hlc)>> = writers.into_iter().map(|w| w.join().unwrap()).collect();
    let mut missed = 0;
    let mut seen = 0;
    for log in &logs {
        seen += log.len();
        for (cells, as_of) in &states {
            // as_of より前の、 行ごとの最後の書き込み (log は書いた順 = 版数の昇順)
            let cut = log.partition_point(|(_, h)| h < as_of);
            let mut last: HashMap<u64, Hlc> = HashMap::new();
            for (row, h) in &log[..cut] {
                last.insert(*row, *h);
            }
            for (row, h) in last {
                if cells.get(&row).is_none_or(|s| *s < h) {
                    missed += 1;
                }
            }
        }
    }
    (missed, seen, states.len())
}

/// 書き手 32 本が自分の行を書き続ける横で state を作る。 as_of より前の版数の書き込みは、 全部 state に入っている。
///
/// 直す前は、 版数を採った後・cell に当てる前に止まった書き手 (thread が CPU を外された / WAL の lock を待つ他の書き手に
/// 積んでもらった後) の cell を state の走査が先に読み、 毎回 1〜7 件が抜けた (手元 6 回、 16 core・load 20〜50)。
#[test]
fn a_state_holds_every_write_stamped_before_its_as_of() {
    let dir = Dir::new("stress");
    let eng = db(&dir.db(1), 1);
    let (missed, seen, builds) = stress(&eng, 32, 2, 4000);
    println!("[stress] 書き込み {seen} 件 / state {builds} 回 / 抜け {missed} 件");
    assert!(builds >= 2, "前提: 書き手が動いている間に state を作った ({builds} 回)");
    assert_eq!(missed, 0, "as_of より前の版数の書き込みが state から抜けた (state {builds} 回、 書き込み {seen} 件)");
}

struct Client {
    eng: Arc<Engine>,
    t: Arc<HttpHostTransport>,
    s: Syncer,
}

impl Client {
    fn push(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
    }
    fn sync(&self) {
        self.push();
        let out = self.s.pull_once(HOST);
        assert!(!out.history_truncated, "前提: client {} は抜けを言われない", self.eng.peer_id());
    }
}

fn v_of(eng: &Engine, k: u32) -> Option<u64> {
    eng.pull_raw("notes.k", k).first().and_then(|e| eng.get(*e, "notes.v"))
}

fn host_cursor(host: &HttpHost, c: PeerId) -> Hlc {
    host.syncer().cursors_of(c).into_iter().find(|(a, _)| *a == c).map_or(Hlc::ZERO, |(_, h)| h)
}

/// ホストを置く形で端から端まで: c1 の書き手 32 本が行を 1 回ずつ書いている最中に、 ホストが c1 の state を取り直す。 書き終えて
/// 全部出した後、 ホストと c3 の行は c1 と同じ。
///
/// 直す前は、 as_of の時に cell に当てる前だった書き込みが state から抜け、 版数が as_of より前なのでホストが差分でも受けず、
/// ホストと c3 にその行の古い値が残った。 行は 1 回ずつしか書かないので、 後の書き込みが上書きして隠すことは無い。 窓は
/// 書き手の thread の止まり方で決まるので確率的 (直す前の検出率は PR に書いた実測)。 決まって踏む試験は engine の単体
/// `state_as_of_tests` (割り込み点)。
#[test]
fn writes_in_flight_when_the_state_is_taken_reach_the_host() {
    const THREADS: u32 = 32;
    const EACH: u32 = 100;
    const ROUNDS: u32 = 3;
    let dir = Dir::new("e2e");
    let host_eng = db(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let eng1 = db(&dir.db(1), 1);
    let t1 = Arc::new(HttpHostTransport::connect(url.clone(), 1).unwrap());
    let s1 = Syncer::new(eng1.clone(), t1.clone());
    // 書き手を走らせる印と、 書いた数
    let go = Arc::new(AtomicBool::new(false));
    let written = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let sent: Arc<Mutex<Vec<StateBatch>>> = Arc::new(Mutex::new(Vec::new()));
    {
        // state の配布元: 書き手を走らせ、 書き始めたのを見てから engine の state をそのまま返す (`Syncer::serve_state` が
        // 自分の分として登録する物と同じ中身)
        let (weak, go, written, sent) = (Arc::downgrade(&eng1), go.clone(), written.clone(), sent.clone());
        t1.register_state_provider(
            1,
            Arc::new(move || {
                let eng = weak.upgrade()?;
                let before = written.load(Ordering::Acquire);
                go.store(true, Ordering::Release);
                while written.load(Ordering::Acquire) < before + THREADS * 8 {
                    std::hint::spin_loop();
                }
                let (records, as_of) = eng.state_records();
                let batch = StateBatch { records, as_of, complete: true };
                sent.lock().unwrap().push(batch.clone());
                Some(batch)
            }),
        );
    }
    let c1 = Client { eng: eng1, t: t1, s: s1 };
    let c3 = {
        let eng = db(&dir.db(3), 3);
        let t = Arc::new(HttpHostTransport::connect(url, 3).unwrap());
        let s = Syncer::new(eng.clone(), t.clone());
        s.serve_state();
        Client { eng, t, s }
    };
    let v = c1.eng.himo_id("notes.v").unwrap() as u16;
    let total = THREADS * EACH * ROUNDS;
    let mut rows: Vec<u64> = Vec::new();
    for k in 1..=total + ROUNDS {
        let e = c1.eng.entity_in("notes").unwrap();
        c1.eng.tie_to(e, "notes.k", k);
        c1.eng.tie_to(e, "notes.v", 0u32);
        rows.push(e);
    }
    c1.eng.oplog_commit();
    for _ in 0..2 {
        c1.sync();
        c3.sync();
    }
    assert_eq!((1..=total).filter(|k| v_of(&c3.eng, *k) != Some(0)).count(), 0, "前提: c3 は c1 の行を全部受けた");

    let mut in_window = 0usize;
    for round in 0..ROUNDS {
        let base = (round * THREADS * EACH) as usize;
        go.store(false, Ordering::Release);
        let writers: Vec<_> = (0..THREADS as usize)
            .map(|j| {
                // 行は書き手の間で互い違い (走査の順に、 どの書き手の行も散らばる)
                let mine: Vec<u64> = (0..EACH as usize).map(|i| rows[base + i * THREADS as usize + j]).collect();
                let (eng, go, written) = (c1.eng.clone(), go.clone(), written.clone());
                std::thread::spawn(move || {
                    while !go.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    for row in mine {
                        eng.try_tie_to_by_id(row, v, round + 1).unwrap();
                        written.fetch_add(1, Ordering::AcqRel);
                    }
                })
            })
            .collect();
        // ホストの c1 の cursor のすぐ上に c1 の floor を置く (= c1 が送れずに落とした record がある、 と言う)。 次の publish で
        // ホストは c1 の state を求める
        let cur = host_cursor(&host, 1);
        c1.t.set_history_floor_multi(1, &[(1, Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer })]);
        // 出す record を 1 つ (publish が空だとホストに聞かない)
        c1.eng.try_tie_to_by_id(rows[(total + round) as usize], v, 9u32).unwrap();
        c1.eng.oplog_commit();
        c1.push();
        for w in writers {
            w.join().unwrap();
        }
        let batch = sent.lock().unwrap().last().cloned().expect("前提: ホストが c1 の state を求め、 c1 が送った");
        assert_eq!(sent.lock().unwrap().len(), round as usize + 1, "前提: 1 回の publish で state を 1 回送った");
        // 前提: この回の書き込みに、 as_of より前の版数の物と後の物がある (書き手が動いている最中に as_of を採った)
        let hlcs: Vec<Hlc> = rows[base..base + (THREADS * EACH) as usize].iter().map(|r| c1.eng.cell_hlc(*r, v)).collect();
        let before = hlcs.iter().filter(|h| **h < batch.as_of).count();
        assert!(before > 0 && before < hlcs.len(), "前提: as_of は書き手が動いている最中 (as_of より前 {before} / {})", hlcs.len());
        // as_of より前の版数なのに state に無い書き込み (直した後は 0)
        let in_state: std::collections::HashSet<(u64, Hlc)> = batch
            .records
            .iter()
            .filter_map(|r| match &r.op {
                DecodedOp::Tie { eid, himo_id, .. } if *himo_id == v => Some((*eid, r.hlc)),
                _ => None,
            })
            .collect();
        in_window += rows[base..base + (THREADS * EACH) as usize]
            .iter()
            .zip(&hlcs)
            .filter(|(r, h)| **h < batch.as_of && !in_state.contains(&(**r, **h)))
            .count();
        c1.eng.oplog_commit();
        for _ in 0..2 {
            c1.sync();
            c3.sync();
        }
    }
    let differing = |eng: &Engine| (1..=total).filter(|k| v_of(eng, *k) != v_of(&c1.eng, *k)).collect::<Vec<u32>>();
    let (at_host, at_c3) = (differing(&host_eng), differing(&c3.eng));
    println!("[e2e] as_of より前の版数で state に無い書き込み {in_window} 件 / 食い違う行: ホスト {} / c3 {}", at_host.len(), at_c3.len());
    assert!(
        at_host.is_empty() && at_c3.is_empty(),
        "c1 が書き終えて全部出した後も、 行が食い違う: ホスト {} 行 {:?} / c3 {} 行 (as_of より前の版数で state に無い書き込み {in_window} 件)",
        at_host.len(),
        &at_host[..at_host.len().min(8)],
        at_c3.len()
    );
    assert_eq!(in_window, 0, "as_of より前の版数の書き込みが state から抜けた");
    drop((c1, c3, host));
}
