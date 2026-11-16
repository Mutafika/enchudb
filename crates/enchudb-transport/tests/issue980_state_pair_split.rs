//! #980 の読み直し: ホストが client の state (`POST /v2/state`) を載せている間、 pull は ring を読まない (`state_gate`、 #753)。
//!
//! state の Tag の cell は、 語の Vocab と Tie が同じ (書き手, 版数) の組で載る (`Syncer::bootstrap_pull_via`、 #769)。 ホストの state の
//! 道は 1 つずつ `relay_record` で WAL に載せ、 途中では書き出さない。 その途中で、 並んだ受信の `sync_for_ack` (#980 の後は state と
//! 並ぶ) か consumer の周期が Commit して bridge すると、 ring には組の前半 (Vocab) だけが載る。 bridge は写し終えた周なので
//! (`remaining` が空) その行も 「配ってよい」 になり、 その時に pull した b は Vocab だけを受けて `cursor[d]` がその版数に進む。
//! 後で載る Tie は `hlc > cursor` を通らず、 b はその cell を二度と受けない。 `state_gate` がこの間の pull を止める。
//!
//! 割り込み点が無い (組の間は `Syncer` の中) ので確率の試験: 語を 1 つずつ持つ行を d が多く書き、 state を載せる間、 別の thread が
//! ホストの `oplog_sync` (Commit + bridge) を回し、 b が pull を回す。 最後に b が d の全部の行を持つか。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;
const WORDS: u32 = 1_500;
const ATTEMPTS: u32 = 3;

/// Tag の列 `name` を持つ表 `rows`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 262_144).unwrap();
        e.define_table("rows", 32_768).unwrap();
        e.define_himo_in("rows", "name", ValueType::Tag, 0).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 64 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

fn settle(e: &Engine) {
    e.oplog_commit();
    e.oplog_sync().unwrap();
    while e.transfer_oplog_to_sync_ops() > 0 {}
}

#[test]
fn a_pull_while_the_host_relays_a_state_does_not_split_a_word_from_its_cell() {
    let dir = std::env::temp_dir().join(format!("enchudb-980-pair-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = |name: &str| dir.join(name).to_str().unwrap().to_string();
    let host_eng = db(&path("host"), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let client = |name: &str, peer: PeerId| {
        let e = db(&path(name), peer);
        let s = Syncer::new(e.clone(), Arc::new(HttpHostTransport::connect(url.clone(), peer).unwrap()));
        s.serve_state();
        (e, s)
    };
    let ((d, sd), (b, sb)) = (client("d", 4), client("b", 2));
    let mut words = Vec::new();
    for attempt in 0..ATTEMPTS {
        // d が語を 1 つずつ持つ行を書き、 ホストに出す前に自分の ring を回収する (ホストは次の publish で d の state を求める)
        for i in 0..WORDS {
            let w = format!("w{attempt}-{i}");
            let e = d.entity_in("rows").unwrap();
            d.tie_text_to(e, "rows.name", &w);
            words.push(w);
        }
        settle(&d);
        d.ack_sync(HOST, d.current_sync_lsn() + 1).unwrap();
        assert!(d.reclaim_sync_ops() > 0, "前提: d の ring を回収した");
        let e = d.entity_in("rows").unwrap();
        d.tie_text_to(e, "rows.name", &format!("tail{attempt}"));
        settle(&d);
        // state を載せる間: ホストの書き出し (Commit + bridge) と b の pull を回し続ける
        let stop = AtomicBool::new(false);
        std::thread::scope(|s| {
            s.spawn(|| {
                while !stop.load(Ordering::Acquire) {
                    let _ = host_eng.oplog_sync();
                }
            });
            s.spawn(|| {
                while !stop.load(Ordering::Acquire) {
                    sb.pull_once(HOST);
                }
            });
            sd.publish_since(Hlc::ZERO);
            stop.store(true, Ordering::Release);
        });
        let host_has = words.iter().filter(|w| host_eng.vocab_id(w).is_some_and(|v| host_eng.pull_raw("rows.name", v).len() == 1)).count();
        assert_eq!(host_has, words.len(), "前提: ホストは d の行を state で受けた (attempt {attempt})");
    }
    for _ in 0..3 {
        sd.publish_since(Hlc::ZERO);
        sb.pull_once(HOST);
    }
    let missing: Vec<&String> =
        words.iter().filter(|w| !b.vocab_id(w).is_some_and(|v| b.pull_raw("rows.name", v).len() == 1)).collect();
    drop((sd, sb, host));
    drop((d, b, host_eng));
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        missing.is_empty(),
        "b が d の行を {} / {} 受けない (例 {:?}): state の組の Vocab だけを受けて cursor が Tie を越えた",
        missing.len(),
        words.len(),
        &missing[..missing.len().min(5)]
    );
}
