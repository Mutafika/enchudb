//! #823: ホストでない中継 (`set_gossip_remote_apply` だけ、 A → R → C、 後から D) で、 書き手が前に使った語をもう一度使った時の
//! Vocab と Tie が **別の pull に分かれて** 中継に届いても、 中継の流れでは Tie の前にその語の Vocab がある。
//!
//! #780 は、 登録済みの Vocab (中継がもう写像を持っている語) を 「同じ pull の、 載せる Tie が指す時だけ」 載せた。 Vocab だけが
//! 先に届く (書き手の bridge が Vocab と Tie の間で切れた / その間に pull された / Tie だけ中継が容量で断った = #754) と、
//! Vocab は載らずに中継の cursor がそれを越え、 次の pull で届いた Tie は Vocab なしで載った。 中継の state で取り直していて
//! その語の写像を持たない下の受け手 (D) は、 Tie を翻訳できずに捨てる (cursor は進む = 二度と届かない)。
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};

const A: PeerId = 1;
const R: PeerId = 2;
const C: PeerId = 3;
const D: PeerId = 4;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue823-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `t` (Tag)、 `u` (Tag、 world では書かない) を持つ表 `notes`。 伸びる形 (空きが足りないと書き込みを断る)。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_growable_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        e.define_himo_in("notes", "u", ValueType::Tag, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 16 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

fn t_of(e: &Engine, k: u32) -> Option<String> {
    let row = *e.pull_raw("notes.k", k).first()?;
    e.get_text_owned(row, "notes.t").map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn put_t(e: &Engine, k: u32, word: &str) {
    let row = e.pull_raw("notes.k", k).first().copied().unwrap_or_else(|| {
        let r = e.entity_in("notes").unwrap();
        e.tie_to(r, "notes.k", k);
        r
    });
    e.tie_text_to(row, "notes.t", word);
    e.oplog_commit();
    std::thread::sleep(Duration::from_millis(3));
}

/// ring の中身 (載った順)。
fn ring(e: &Engine) -> Vec<WireRecord> {
    e.pending_sync_ops(0).iter().filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p)).map(WireRecord::from).collect()
}

fn is_vocab(r: &WireRecord, word: &[u8]) -> bool {
    matches!(&r.op, DecodedOp::Vocab { bytes, .. } if bytes == word)
}

/// A → R (中継) → C。 A が "x" を使って全員に届き、 "y" に書き換えて、 R が "x" の Vocab を回収した後に D が来て、 R の state で
/// A を取り直す (D は "x" の写像を持たない)。
struct Mesh {
    sa: Syncer,
    sr: Syncer,
    sc: Syncer,
    sd: Syncer,
    a: Arc<Engine>,
    r: Arc<Engine>,
    c: Arc<Engine>,
    d: Arc<Engine>,
    mem: Arc<InMemoryTransport>,
    _dir: Dir,
}

fn mesh(tag: &str) -> Mesh {
    let dir = Dir::new(tag);
    let mem = Arc::new(InMemoryTransport::new());
    for p in [A, R, C, D] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();
    let (a, r, c) = (db(&dir.db(A), A), db(&dir.db(R), R), db(&dir.db(C), C));
    r.set_gossip_remote_apply(true);
    let (sa, sr, sc) =
        (Syncer::new(a.clone(), transport.clone()), Syncer::new(r.clone(), transport.clone()), Syncer::new(c.clone(), transport.clone()));
    sa.serve_state();
    sr.serve_state();
    let cycle = || {
        a.oplog_commit();
        a.oplog_sync().unwrap();
        sa.publish_since(Hlc::ZERO);
        assert!(!sr.pull_once(A).history_truncated, "前提: R は抜けを言われない");
        r.oplog_sync().unwrap();
        sr.publish_since(Hlc::ZERO);
        assert!(!sc.pull_once(R).history_truncated, "前提: C は抜けを言われない");
    };
    put_t(&a, 1, "x");
    cycle();
    cycle();
    assert_eq!(t_of(&c, 1).as_deref(), Some("x"), "前提: C が A の行を受けた");
    assert!(ring(&r).iter().any(|x| is_vocab(x, b"x")), "前提: 語 x の Vocab は R の ring に載った");
    put_t(&a, 1, "y");
    for k in 8..=11 {
        put_t(&a, k, "pad");
        cycle();
        cycle();
    }
    assert_eq!(t_of(&c, 1).as_deref(), Some("y"), "前提: C に書き換えが届いた");
    assert!(r.reclaim_sync_ops() > 0, "前提: R が回収した");
    assert!(!ring(&r).iter().any(|x| is_vocab(x, b"x")), "前提: 語 x の Vocab は R の ring から消えた");
    r.oplog_sync().unwrap();
    sr.publish_since(Hlc::ZERO);
    let d = db(&dir.db(D), D);
    let sd = Syncer::new(d.clone(), transport);
    let out = sd.pull_once(R);
    assert!(out.history_truncated, "前提: D は抜けを言われた");
    let authors = if out.truncated_authors.is_empty() { vec![A] } else { out.truncated_authors.clone() };
    for author in authors {
        assert!(sd.bootstrap_pull_via(R, author).is_some(), "前提: D は R から書き手 {author} の state を取れる");
    }
    assert!(!sd.pull_once(R).history_truncated, "前提: 取り直した D はもう抜けを言われない");
    assert_eq!(t_of(&d, 1).as_deref(), Some("y"), "前提: D は取り直しで今の語を受けた");
    Mesh { sa, sr, sc, sd, a, r, c, d, mem, _dir: dir }
}

impl Mesh {
    fn r_cursor(&self) -> Hlc {
        self.sr.cursors_of(A).into_iter().find(|(a, _)| *a == A).map_or(Hlc::ZERO, |(_, h)| h)
    }
    /// A が行 `k` の t に "x" を書く。 戻り値: R がまだ受けていない A の record を (Vocab まで, Tie から) に分けた物。
    fn a_reuses_x(&self, k: u32) -> (Vec<WireRecord>, Vec<WireRecord>) {
        put_t(&self.a, k, "x");
        self.a.oplog_sync().unwrap();
        let cur = self.r_cursor();
        let fresh: Vec<WireRecord> = ring(&self.a).into_iter().filter(|r| r.author_peer == A && r.hlc > cur).collect();
        let at = fresh.iter().position(|r| is_vocab(r, b"x")).expect("前提: A の ring に語 x の Vocab がある");
        let t_himo = self.a.himo_id("notes.t").unwrap() as u16;
        assert!(
            matches!(&fresh[at + 1].op, DecodedOp::Tie { himo_id, .. } if *himo_id == t_himo),
            "前提: Vocab のすぐ後ろが t の Tie ({:?})",
            fresh[at + 1].op
        );
        assert!(fresh[at].hlc < fresh[at + 1].hlc, "前提: Vocab の版数 < Tie の版数");
        let tail = fresh[at + 1..].to_vec();
        (fresh[..=at].to_vec(), tail)
    }
    /// A の record を R にだけ配って、 R が受ける。
    fn deliver_to_r(&self, records: Vec<WireRecord>) -> enchudb::sync::SyncOutcome {
        self.mem.publish_to(A, R, records);
        let out = self.sr.pull_once(A);
        assert!(!out.history_truncated, "前提: R は抜けを言われない");
        out
    }
    fn sa_publishes(&self) {
        self.a.oplog_sync().unwrap();
        self.sa.publish_since(Hlc::ZERO);
    }
    fn r_publishes(&self) {
        self.r.oplog_sync().unwrap();
        self.sr.publish_since(Hlc::ZERO);
    }
    fn vocab_x_in_r(&self) -> usize {
        ring(&self.r).iter().filter(|r| is_vocab(r, b"x")).count()
    }
    /// R の ring で、 行 `k` の t の Tie (最後の物) より前に語 x の Vocab があるか。
    fn vocab_precedes_tie_in_r(&self, k: u32) -> bool {
        let row = self.a.pull_raw("notes.k", k)[0];
        let t_himo = self.a.himo_id("notes.t").unwrap() as u16;
        let ring = ring(&self.r);
        let tie = ring
            .iter()
            .rposition(|r| matches!(&r.op, DecodedOp::Tie { eid, himo_id, .. } if *eid == row && *himo_id == t_himo))
            .expect("前提: R の ring に行の t の Tie がある");
        ring[..tie].iter().any(|r| is_vocab(r, b"x"))
    }
}

/// Vocab までを受けた pull と、 Tie からを受けた pull に分かれる。 下の受け手は R が両方を載せ終えてから受ける。
#[test]
fn a_known_vocab_and_its_tie_split_across_two_pulls_both_reach_a_peer_that_bootstrapped_below_the_relay() {
    let m = mesh("split");
    let (head, tail) = m.a_reuses_x(2);
    let vocab_hlc = head.last().unwrap().hlc;
    let out = m.deliver_to_r(head);
    assert_eq!(out.min_rejected_hlc, None, "前提: R は断っていない");
    assert_eq!(m.r_cursor(), vocab_hlc, "前提: R の A の cursor は Vocab まで進んだ");
    assert_eq!(t_of(&m.r, 2), None, "前提: Tie はまだ R に届いていない");
    let out = m.deliver_to_r(tail);
    assert!(out.applied > 0, "前提: Tie が R に当たった");
    assert_eq!(t_of(&m.r, 2).as_deref(), Some("x"), "前提: R は自分の写像で Tie を翻訳できる");
    m.r_publishes();
    assert!(m.vocab_precedes_tie_in_r(2), "R の ring で、 行 2 の t の Tie の前に語 x の Vocab が無い");
    let (oc, od) = (m.sc.pull_once(R), m.sd.pull_once(R));
    assert!(!oc.history_truncated && !od.history_truncated, "前提: 抜けを言われない");
    let got = [t_of(&m.a, 2), t_of(&m.r, 2), t_of(&m.c, 2), t_of(&m.d, 2)];
    let want = Some("x".to_string());
    assert_eq!(od.dropped_vocab, 0, "D が翻訳できずに捨てた Tie");
    assert_eq!(got, [want.clone(), want.clone(), want.clone(), want], "行 2 の t [A, R, C, D]");
}

/// 同じ分かれ方で、 下の受け手が 2 つの pull の間にも受ける (Vocab だけが載った所を受ける)。
#[test]
fn a_peer_below_the_relay_that_pulls_between_the_two_halves_still_gets_the_word() {
    let m = mesh("between");
    let (head, tail) = m.a_reuses_x(2);
    m.deliver_to_r(head);
    m.r_publishes();
    assert!(!m.sd.pull_once(R).history_truncated, "前提: 抜けを言われない");
    assert!(m.deliver_to_r(tail).applied > 0, "前提: Tie が R に当たった");
    m.r_publishes();
    let od = m.sd.pull_once(R);
    assert!(!od.history_truncated, "前提: 抜けを言われない");
    assert_eq!(od.dropped_vocab, 0, "D が翻訳できずに捨てた Tie");
    assert_eq!(t_of(&m.d, 2).as_deref(), Some("x"), "行 2 の t (D)");
    // 同じ語をもう一度 (分かれずに届く): 何度使っても届く
    let (head, tail) = m.a_reuses_x(3);
    m.deliver_to_r(head.into_iter().chain(tail).collect());
    m.r_publishes();
    assert_eq!(m.sd.pull_once(R).dropped_vocab, 0);
    assert_eq!(t_of(&m.d, 3).as_deref(), Some("x"), "行 3 の t (D)");
}

/// 指す Tie が当たらなかった Vocab は載せない (同じ record が別の link から届き直した = 閉路の反響)。
#[test]
fn a_known_vocab_whose_tie_loses_is_not_relayed_again() {
    let m = mesh("echo");
    let (head, tail) = m.a_reuses_x(2);
    let all: Vec<WireRecord> = head.into_iter().chain(tail).collect();
    assert!(m.deliver_to_r(all.clone()).applied > 0, "前提: Tie が R に当たった");
    assert_eq!(m.vocab_x_in_r(), 1, "前提: R は語 x の Vocab を Tie の前に 1 回載せた");
    // 同じ record が C の link から届く (R の C の link の cursor は A の record をまだ受けていない)
    m.mem.publish_to(C, R, all);
    let out = m.sr.pull_once(C);
    assert!(out.received > 0 && out.applied == 0, "前提: R は同じ record を受けて、 どれも当たらない ({out:?})");
    m.r_publishes();
    assert_eq!(m.vocab_x_in_r(), 1, "R の ring の、 語 x の Vocab の数");
}

/// 容量の道 (#754 との組み合わせ): R が Tie だけを容量で断る。 cursor は Vocab まで進み、 Tie は空きが戻ってから届き直す。
/// 断っている間に何度 pull しても、 Vocab は 1 回しか載らない。
#[test]
fn a_known_vocab_whose_tie_is_refused_for_capacity_is_relayed_once_and_the_tie_follows() {
    let m = mesh("cap");
    // A が、 R でまだ 1 度も書かれていない列 u に "x" を書く (R では列を伸ばす書き込み = 空きが足りないと断る)
    let row = m.a.entity_in("notes").unwrap();
    m.a.tie_to(row, "notes.k", 2u32);
    m.a.oplog_commit();
    m.sa_publishes();
    assert!(m.sr.pull_once(A).applied > 0, "前提: 行 2 が R に届いた");
    m.a.tie_text_to(row, "notes.u", "x");
    m.a.oplog_commit();
    m.sa_publishes();
    m.r.set_space_margin(u64::MAX / 2);
    let before = m.r_cursor();
    for round in 0..3 {
        let out = m.sr.pull_once(A);
        assert_eq!(out.rejected_capacity, 1, "前提: R は Tie を容量で断った (round {round}, {out:?})");
        m.r_publishes();
        assert_eq!(m.vocab_x_in_r(), 1, "断っている間の R の ring の、 語 x の Vocab の数 (round {round})");
    }
    assert!(m.r_cursor() > before, "前提: R の A の cursor は Vocab まで進んだ");
    assert!(!m.sd.pull_once(R).history_truncated, "前提: 抜けを言われない");
    m.r.set_space_margin(0);
    let out = m.sr.pull_once(A);
    assert!(out.applied > 0 && out.rejected_capacity == 0, "前提: 空きが戻って Tie が R に当たった ({out:?})");
    m.r_publishes();
    let od = m.sd.pull_once(R);
    assert_eq!(od.dropped_vocab, 0, "D が翻訳できずに捨てた Tie");
    let u_of = |e: &Engine| {
        let row = *e.pull_raw("notes.k", 2).first()?;
        e.get_text_owned(row, "notes.u").map(|b| String::from_utf8_lossy(&b).into_owned())
    };
    let want = Some("x".to_string());
    assert_eq!([u_of(&m.a), u_of(&m.r), u_of(&m.d)], [want.clone(), want.clone(), want], "行 2 の u [A, R, D]");
}

/// cursor が進まずに Vocab が届き直す間 (同じ link の、 別の書き手の手前の record を容量で断っている) も、 1 回しか載せない。
#[test]
fn a_known_vocab_redelivered_under_a_held_cursor_is_relayed_once() {
    let m = mesh("held");
    // C の行 50 が R に届いている
    let row = m.c.entity_in("notes").unwrap();
    m.c.tie_to(row, "notes.k", 50u32);
    m.c.oplog_commit();
    m.c.oplog_sync().unwrap();
    m.mem.publish_to(C, R, ring(&m.c).into_iter().filter(|r| r.author_peer == C).collect());
    assert!(m.sr.pull_once(C).applied > 0, "前提: C の行 50 が R に届いた");
    // C が、 R でまだ 1 度も書かれていない列 u に書く (R は空きが足りず断る)。 その後に A が "x" をもう一度使う
    std::thread::sleep(Duration::from_millis(3));
    m.c.tie_text_to(row, "notes.u", "z");
    m.c.oplog_commit();
    m.c.oplog_sync().unwrap();
    let u_himo = m.c.himo_id("notes.u").unwrap() as u16;
    let from_c: Vec<WireRecord> = ring(&m.c)
        .into_iter()
        .filter(|r| r.author_peer == C && (is_vocab(r, b"z") || matches!(&r.op, DecodedOp::Tie { himo_id, .. } if *himo_id == u_himo)))
        .collect();
    assert_eq!(from_c.len(), 2, "前提: C の Vocab と Tie");
    std::thread::sleep(Duration::from_millis(3));
    let (head, tail) = m.a_reuses_x(2);
    let vocab_hlc = head.last().unwrap().hlc;
    assert!(from_c[1].hlc < vocab_hlc, "前提: C の Tie は A の Vocab より前");
    m.r.set_space_margin(u64::MAX / 2);
    // 同じ link (A) に、 C の record と A の Vocab まで
    m.mem.publish_to(A, R, from_c);
    m.mem.publish_to(A, R, head);
    for round in 0..3 {
        let out = m.sr.pull_once(A);
        assert_eq!(out.rejected_capacity, 1, "前提: R は C の Tie を容量で断った (round {round}, {out:?})");
        assert!(m.r_cursor() < vocab_hlc, "前提: R の A の cursor は Vocab の手前で止まっている (round {round})");
        m.r_publishes();
        assert_eq!(m.vocab_x_in_r(), 1, "cursor が止まっている間の R の ring の、 語 x の Vocab の数 (round {round})");
    }
    m.r.set_space_margin(0);
    m.mem.publish_to(A, R, tail);
    assert!(m.sr.pull_once(A).applied > 0, "前提: 空きが戻って当たった");
    m.r_publishes();
    let od = m.sd.pull_once(R);
    assert_eq!(od.dropped_vocab, 0, "D が翻訳できずに捨てた Tie");
    assert_eq!(t_of(&m.d, 2).as_deref(), Some("x"), "行 2 の t (D)");
}
