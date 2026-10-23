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

/// `rounds` の各周で最初の client が 1 行足し (書き込みの続く形)、 周の後にホストの `_sync_ops` を回収する (ring が半分埋まった
/// 時の回収の代わり)。 忘れてよい線はホストの ring に残る record を越えず、 回収は全員が受けた最後の行の手前まで (`lsn < watermark`)
/// なので、 削除の記録を忘れるのは、 削除の後の行が来て削除が ring から消えた後。
fn rounds_reclaiming(host: &Engine, cs: &[&Client], n: usize) {
    for i in 0..n {
        cs[0].write(9_000 + i as u32, 0);
        for c in cs {
            c.sync();
        }
        host.reclaim_sync_ops();
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
    rounds(&[&c1, &c2], 3);
    assert!(
        c1.eng.deleted_primary_key_count() > 0 && host_eng.deleted_primary_key_count() > 0,
        "ホストの ring に削除が残っている間に忘れた"
    );
    rounds_reclaiming(&host_eng, &[&c1, &c2], 4);
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
    rounds_reclaiming(&host_eng, &[&c1], 6);
    assert_eq!(host.forget_line(), Hlc::ZERO);
    assert!(c1.eng.deleted_primary_key_count() > 0, "線の無いホストの下で忘れた");
    drop((c1, host));
    cleanup(&[&hp, &p1]);
}

/// 黙った client は線を止めない。 戻ってきて record を出すと、 ホストは受けずに `rebase_required` を立てる: 線より前の (黙る前に
/// 書いて出していない) record でも (受けると、 忘れた削除を越えて消した行が生き返りうる)、 黙る前に全部出していた client の
/// 線より後だけの record でも (留守の間に消えた行を持ったままなので、 その行への書き込みが生き返らせうる)。
#[test]
fn a_silent_client_stops_holding_the_line_and_is_refused_when_it_returns() {
    let (hp, p1, p2, p3) = (tmp("silent-host"), tmp("silent-c1"), tmp("silent-c2"), tmp("silent-c3"));
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(format!("{hp}.cursors").into())).unwrap();
    host.set_silence_limit(Duration::from_millis(300));
    let url = format!("http://{}", host.addr());
    let (c1, c2, c3) = (client(db(&p1, 1), &url), client(db(&p2, 2), &url), client(db(&p3, 3), &url));
    c1.write(1, 10);
    c3.write(3, 30); // c3 は全部出してから黙る
    rounds(&[&c1, &c2, &c3], 2);
    // c2 は書いて出さずに黙る
    let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    c2.write(5, 50);
    std::thread::sleep(Duration::from_millis(400));
    c1.delete(1);
    for k in 10..30 {
        c1.write(k, k);
    }
    rounds_reclaiming(&host_eng, &[&c1], 6);
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
    // 戻った後の新しい書き込みも受けない (留守の間に消えた行を持ったままなので、 線より後でも消えた行を生き返らせうる)
    c2.write(6, 60);
    c2.eng.oplog_sync().unwrap();
    c2.s.publish_since(Hlc::ZERO);
    assert_eq!(host_eng.pull_raw("notes.k", 6).len(), 0, "戻った client の新しい書き込みを受けた");
    // 黙る前に全部出していた c3 の、 戻った後の (線より後だけの) 書き込みも受けない
    assert_eq!(host_eng.pull_raw("notes.k", 3).len(), 1, "前提: c3 は黙る前に出し終えた");
    c3.write(7, 70);
    c3.eng.oplog_sync().unwrap();
    c3.s.publish_since(Hlc::ZERO);
    assert!(c3.t.rebase_required(), "黙る前に出し終えた client の、 戻った後の書き込みを受けた");
    assert_eq!(host_eng.pull_raw("notes.k", 7).len(), 0, "断った record が当たった");
    drop((c1, c2, c3, host));
    cleanup(&[&hp, &p1, &p2, &p3]);
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
    rounds_reclaiming(&host_eng, &[&c1], 6);
    assert!(host.forget_line().wall > written, "前提: 線が c3 の書き込みを越えた (線 {:?})", host.forget_line());
    let c3 = client(c3_eng, &url);
    c3.eng.oplog_sync().unwrap();
    c3.s.publish_since(Hlc::ZERO);
    // ホストは回収済みなので、 初めて来た client は抜けを知って取り直す
    let out = c3.s.pull_once(HOST);
    for a in out.truncated_authors {
        assert!(c3.s.bootstrap_pull_via(HOST, a).is_some());
    }
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
    rounds_reclaiming(&host_eng, &[&c1], 6);
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
    rounds_reclaiming(&host_eng, &[&c1], 4);
    assert!(host.forget_line() > line, "開き直した後に線が進まない");
    drop((c1, host));
    cleanup(&[&hp, &p1]);
}

/// 線を一度も言っていない (publish していない) client は線を止める。 その client がオフラインで作った行 N が、 届いた別の端末の
/// N の作成に名前で束ねられ、 その削除で一緒に消えた (受けた削除なので自分の WAL に載らない) 後で N の作成を出しても、 ホストは
/// まだ削除を覚えているので、 全員で N は消えたまま (enchudb-76 のレビューの形。 線が削除を越えていると、 ホストは N を新しい行と
/// して受けて、 その client だけが消えたままになる)。
#[test]
fn a_client_that_has_not_published_holds_the_line() {
    let (hp, p1, p2) = (tmp("unpub-host"), tmp("unpub-c1"), tmp("unpub-k"));
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(format!("{hp}.cursors").into())).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, k) = (client(db(&p1, 1), &url), client(db(&p2, 2), &url));
    k.write(5, 50); // オフラインで、 出さない
    c1.write(5, 51);
    c1.sync();
    c1.delete(5);
    c1.sync();
    // k は受けるだけ: c1 の 5 が自分の 5 に束ねられ、 削除で一緒に消える
    for _ in 0..3 {
        k.s.pull_once(HOST);
    }
    assert_eq!(k.has(5), 0, "前提: 束ねられて消えた");
    let before_k = host.forget_line(); // k が来る前に c1 だけで進んだ分 (削除より前)
    rounds_reclaiming(&host_eng, &[&c1], 4);
    for _ in 0..2 {
        k.s.pull_once(HOST);
    }
    rounds_reclaiming(&host_eng, &[&c1], 2);
    assert_eq!(host.forget_line(), before_k, "publish していない client が居るのに線が進んだ");
    assert!(host_eng.deleted_primary_key_count() > 0, "ホストが削除を忘れた");
    // k が初めて出す: ホストは削除を覚えているので、 削除より前の作成は消えたまま
    k.sync();
    c1.sync();
    assert_eq!(
        [host_eng.pull_raw("notes.k", 5).len(), c1.has(5), k.has(5)],
        [0, 0, 0],
        "削除より前の作成の答えが割れた"
    );
    // 出した後は k も線を言うので進む
    rounds_reclaiming(&host_eng, &[&c1, &k], 6);
    assert!(host.forget_line() > before_k, "publish した後も線が進まない");
    drop((c1, k, host));
    cleanup(&[&hp, &p1, &p2]);
}

/// client は返事の線で忘れてから当てる: ホストが線で削除を忘れた後に受けた、 削除より前の作成 (初めて来た client のオフラインの
/// 行) を、 まだ線を受けていない client が当てても、 ホストと同じく新しい行として生きる (忘れずに当てると、 その client だけ消える)。
#[test]
fn a_client_forgets_before_it_applies_the_answer() {
    let (hp, p1, p2, p3) = (tmp("order-host"), tmp("order-c1"), tmp("order-c2"), tmp("order-k"));
    let host_eng = db(&hp, HOST);
    let host = HttpHost::start("127.0.0.1:0", host_eng.clone(), Some(format!("{hp}.cursors").into())).unwrap();
    let url = format!("http://{}", host.addr());
    let (c1, c2) = (client(db(&p1, 1), &url), client(db(&p2, 2), &url));
    let k_eng = db(&p3, 3);
    let e = k_eng.entity_in("notes").unwrap();
    k_eng.tie_to(e, "notes.k", 5u32); // まだホストを知らない client がオフラインで
    k_eng.tie_to(e, "notes.v", 50u32);
    k_eng.oplog_commit();
    c1.write(5, 51);
    rounds(&[&c1, &c2], 2);
    c1.delete(5);
    rounds(&[&c1, &c2], 2);
    // c2 が受けた後に c1 が線を進める順で回し、 ホストが忘れて c2 がまだ覚えている所で止める
    let mut reached = false;
    for i in 0..12 {
        c1.write(9_100 + i, 0);
        c2.sync();
        host_eng.reclaim_sync_ops(); // c2 の後・c1 の前に回収 = 線は c1 の時に進む
        c1.sync();
        if host_eng.deleted_primary_key_count() == 0 && c2.eng.deleted_primary_key_count() > 0 {
            reached = true;
            break;
        }
    }
    assert!(reached, "前提: ホストが忘れ、 c2 がまだ覚えている所に来ない");
    // 初めて来た k が削除より前の作成を出す: ホストは忘れた後なので新しい行
    let k = client(k_eng, &url);
    k.eng.oplog_sync().unwrap();
    k.s.publish_since(Hlc::ZERO);
    assert_eq!(host_eng.pull_raw("notes.k", 5).len(), 1, "前提: ホストは新しい行として受けた");
    // c2 は同じ返事で線を受けて忘れてから当てる
    let out = c2.s.pull_once(HOST);
    assert!(!out.history_truncated, "{out:?}");
    assert_eq!(c2.has(5), 1, "ホストは生かした行を、 線を受ける前の client が消した");
    drop((c1, c2, k, host));
    cleanup(&[&hp, &p1, &p2, &p3]);
}
