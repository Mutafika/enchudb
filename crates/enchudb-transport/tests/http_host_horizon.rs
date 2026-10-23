//! ホストの段 2: 削除を忘れてよい線 (`HOST_PLAN.md`)。
//!
//! 消した名前 (`pktombs`) と相手の行の削除 (`foreign_tombs`) の記録は、 前は減らなかった (#560)。 ホストは client ごとに 「自分の
//! 書き込みをどこまで出し終えたか」 と 「ホストの配りをどこまで受け終えたか」 を覚え、 全員が過ぎた線を配る。 線より前の削除の
//! 記録は、 ホストも client も捨てる。 30 日 (試験では短く) 黙った client は数えず、 戻ってきて線より前の record を出したら受けない。

use std::sync::Arc;
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const HOST: PeerId = 100;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-horizon-{tag}-{}-{nanos}", std::process::id())
}

/// 名前 `k` (主キー) と `v` を持つ表 `notes` の sync の DB。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    reopen(path, peer)
}

fn reopen(path: &str, peer: PeerId) -> Arc<Engine> {
    let e = Engine::open_concurrent_with_oplog(path, 16 << 20).unwrap();
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

impl Client {
    fn write(&self, k: u32, v: u32) {
        let e = self.eng.entity_in("notes").unwrap();
        self.eng.tie_to(e, "notes.k", k);
        self.eng.tie_to(e, "notes.v", v);
        self.eng.oplog_commit();
    }

    fn delete(&self, k: u32) {
        let e = self.eng.pull_raw("notes.k", k)[0];
        self.eng.delete(e);
        self.eng.oplog_commit();
    }

    fn sync(&self) {
        self.eng.oplog_sync().unwrap();
        self.s.publish_since(Hlc::ZERO);
        let out = self.s.pull_once(self.t.host_id());
        assert!(!out.history_truncated, "peer {}: {out:?}", self.eng.peer_id());
    }

    fn has(&self, k: u32) -> usize {
        self.eng.pull_raw("notes.k", k).len()
    }
}

fn rounds(cs: &[&Client], n: usize) {
    for _ in 0..n {
        for c in cs {
            c.sync();
        }
    }
}

fn cleanup(paths: &[&String]) {
    for p in paths {
        let _ = std::fs::remove_dir_all(p);
        let _ = std::fs::remove_file(format!("{p}.cursors"));
        let _ = std::fs::remove_file(format!("{p}.cursors.clients"));
    }
}

/// 全員が削除の後まで出し終え・受け終えたら、 ホストも client も消した名前の記録を捨てる。 書き込みの無い client (c2 は最初に
/// 1 行書いただけ) も線を止めない。
#[test]
fn deletes_are_forgotten_once_everyone_is_past_them() {
    let (hp, p1, p2) = (tmp("host"), tmp("c1"), tmp("c2"));
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(format!("{hp}.cursors").into())).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&p1, 1), &url), client(db(&p2, 2), &url));
    c1.write(1, 10);
    c2.write(2, 20);
    rounds(&[&c1, &c2], 2);
    assert_eq!([c1.has(2), c2.has(1)], [1, 1], "前提: 行が届く");
    c1.delete(2);
    rounds(&[&c1, &c2], 2);
    assert_eq!([c1.has(2), c2.has(2), host_eng.pull_raw("notes.k", 2).len()], [0, 0, 0], "前提: 削除が届く");
    assert!(
        c1.eng.deleted_primary_key_count() > 0 && c2.eng.deleted_primary_key_count() > 0,
        "前提: 消した名前を覚えている"
    );
    rounds(&[&c1, &c2], 6);
    assert!(host.forget_line() > Hlc::ZERO, "線が進まない");
    assert_eq!(
        [c1.eng.deleted_primary_key_count(), c2.eng.deleted_primary_key_count(), host_eng.deleted_primary_key_count()],
        [0, 0, 0],
        "線を過ぎた削除の記録が残る (線 {:?})",
        host.forget_line()
    );
    assert_eq!([c1.has(1), c2.has(1)], [1, 1], "残す行まで消えた");
    drop((c1, c2, host));
    cleanup(&[&hp, &p1, &p2]);
}

/// 置き場 (`cursor_path`) の無いホストは線を配らない (開き直した後に誰を待つか分からないので、 忘れない側)。
#[test]
fn a_host_without_a_state_file_forgets_nothing() {
    let (hp, p1) = (tmp("nofile-host"), tmp("nofile-c1"));
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&p1, 1), &url);
    c1.write(1, 10);
    rounds(&[&c1], 2);
    c1.delete(1);
    rounds(&[&c1], 6);
    assert_eq!(host.forget_line(), Hlc::ZERO);
    assert!(c1.eng.deleted_primary_key_count() > 0, "線の無いホストの下で忘れた");
    drop((c1, host));
    cleanup(&[&hp, &p1]);
}

/// 黙った client は線を止めない。 戻ってきて、 線より前の (黙る前に書いて出していない) record を出すと、 ホストは受けずに
/// `rebase_required` を立てる (受けると、 忘れた削除を越えて消した行が生き返りうる)。
#[test]
fn a_silent_client_stops_holding_the_line_and_is_refused_when_it_returns() {
    let (hp, p1, p2) = (tmp("silent-host"), tmp("silent-c1"), tmp("silent-c2"));
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(format!("{hp}.cursors").into())).unwrap();
    host.set_silence_limit(Duration::from_millis(300));
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&p1, 1), &url), client(db(&p2, 2), &url));
    c1.write(1, 10);
    rounds(&[&c1, &c2], 2);
    // c2 は書いて出さずに黙る
    let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    c2.write(5, 50);
    std::thread::sleep(Duration::from_millis(400));
    c1.delete(1);
    for k in 10..30 {
        c1.write(k, k);
    }
    rounds(&[&c1], 6);
    assert!(host.forget_line().wall > before, "黙った client が線を止めた (線 {:?})", host.forget_line());
    // 黙った client はホストの回収も止めない (c1 が受け終えた行は回収できる)
    host_eng.reclaim_sync_ops();
    let left = host_eng.pending_sync_ops(0).len();
    assert!(left <= 2, "黙った client がホストの回収を止めた (残り {left} 行)");
    assert_eq!(host_eng.deleted_primary_key_count(), 0, "ホストが線を過ぎた削除を捨てない");
    c2.eng.oplog_sync().unwrap();
    c2.s.publish_since(Hlc::ZERO);
    assert!(c2.t.rebase_required(), "線より前の record を受けた");
    assert_eq!(host_eng.pull_raw("notes.k", 5).len(), 0, "断った record が当たった");
    drop((c1, c2, host));
    cleanup(&[&hp, &p1, &p2]);
}

/// 初めて来た client がオフラインで書いた record は、 線より前でも受ける (オフラインで書いてから初めて同期するのは常の形)。
#[test]
fn a_new_clients_offline_writes_are_accepted() {
    let (hp, p1, p3) = (tmp("new-host"), tmp("new-c1"), tmp("new-c3"));
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(format!("{hp}.cursors").into())).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&p1, 1), &url);
    c1.write(1, 10);
    rounds(&[&c1], 2);
    // c3 はまだホストを知らないまま書く
    let c3_eng = db(&p3, 3);
    let e = c3_eng.entity_in("notes").unwrap();
    c3_eng.tie_to(e, "notes.k", 7u32);
    c3_eng.oplog_commit();
    let written = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    std::thread::sleep(Duration::from_millis(20));
    rounds(&[&c1], 6);
    assert!(host.forget_line().wall > written, "前提: 線が c3 の書き込みを越えた (線 {:?})", host.forget_line());
    let c3 = client(c3_eng, &url);
    c3.sync();
    assert!(!c3.t.rebase_required(), "初めて来た client を断った");
    assert_eq!(host_eng.pull_raw("notes.k", 7).len(), 1, "初めて来た client の行が当たらない");
    drop((c1, c3, host));
    cleanup(&[&hp, &p1, &p3]);
}

/// ホストを開き直しても線と client ごとの状態を覚えている (線は下がらない、 開き直した直後に誰も居なくても進めない)。
#[test]
fn a_restarted_host_keeps_the_line() {
    let (hp, p1) = (tmp("restart-host"), tmp("restart-c1"));
    let cursors: std::path::PathBuf = format!("{hp}.cursors").into();
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors.clone())).unwrap();
    let url = format!("http://{}", host.addr());
    let c1 = client(db(&p1, 1), &url);
    c1.write(1, 10);
    rounds(&[&c1], 2);
    c1.delete(1);
    rounds(&[&c1], 6);
    let line = host.forget_line();
    assert!(line > Hlc::ZERO, "前提: 線が進んだ");
    drop(host);
    drop(host_eng);

    let host_eng = reopen(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(cursors)).unwrap();
    assert_eq!(host.forget_line(), line, "開き直すと線を忘れた");
    let url = format!("http://{}", host.addr());
    let c1 = client(c1.eng.clone(), &url);
    c1.write(2, 20);
    rounds(&[&c1], 4);
    assert!(host.forget_line() > line, "開き直した後に線が進まない");
    drop((c1, host));
    cleanup(&[&hp, &p1]);
}
