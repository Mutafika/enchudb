//! #825 / #786: ホストでない中継の下に中継がある形 (A・P → U → R → D) で、 R が U の replica の state で書き手 A を取り直した時、 state の
//! 中の 「A の行を別の peer P が書き換えた cell」 で、 下の受け手 D の cursor が、 R がまだ載せていない record を越えない。
//!
//! replica の state は、 その cell を 2 つの形で出す:
//! - Tag の、 A の語彙に無い語: P の record の形 (`author_peer = P`、 版数は P の時計、 #711)。 R が当てて載せると、 D の
//!   `cursor[R][P]` がその版数まで進み、 P がそれより前に書いて R にまだ届いていない record が、 後で差分で R に届いて載っても落ちる
//! - それ以外: A の record の形で、 版数は P の時計。 版数が R の A の cursor (取り直した後) より後だと、 D の `cursor[R][A]` が
//!   そこまで進み、 A がそれより前に書いて R にまだ届いていない record が落ちる (#786 の、 中継の下の形)
#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use enchudb::ValueType;
use enchudb::engine::Engine;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};

const A: PeerId = 1;
const P: PeerId = 2;
const U: PeerId = 3;
const R: PeerId = 4;
const D: PeerId = 5;

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("enchudb-issue825-{tag}-{}", std::process::id()));
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

/// 名前 `k` (Number) と `v` (Number)、 `t` (Tag) を持つ表 `notes`。
fn db(path: &str, peer: PeerId) -> Arc<Engine> {
    {
        let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
        e.define_table("notes", 4096).unwrap();
        e.define_himo_in("notes", "k", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "v", ValueType::Number, 0).unwrap();
        e.define_himo_in("notes", "t", ValueType::Tag, 0).unwrap();
        let k = e.himo_id("notes.k").unwrap() as u16;
        e.set_table_pk("notes", k).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let e = Engine::open_concurrent_with_oplog(path, 16 << 20).unwrap();
    e.set_peer_id(peer);
    e
}

fn row(e: &Engine, k: u32) -> Option<u64> {
    e.pull_raw("notes.k", k).first().copied()
}
fn v(e: &Engine, k: u32) -> Option<u64> {
    row(e, k).and_then(|r| e.get(r, "notes.v"))
}
fn t(e: &Engine, k: u32) -> Option<String> {
    row(e, k).and_then(|r| e.get_text_owned(r, "notes.t").map(|b| String::from_utf8_lossy(&b).into_owned()))
}
/// 新しい行 (k, v) を書く。 次の書き込みの HLC の壁時計を進める (別の peer の書き込みとの前後を、 書いた順にする)。
fn write(e: &Engine, k: u32, val: u32) {
    let r = e.entity_in("notes").unwrap();
    e.tie_to(r, "notes.k", k);
    e.tie_to(r, "notes.v", val);
    e.oplog_commit();
    std::thread::sleep(Duration::from_millis(3));
}

/// 書き手 A・P → U (中継) → R (中継) → D。 P は U から A の行を受ける。 state を配るのは U だけ (replica の state)。
struct Mesh {
    sa: Syncer,
    sp: Syncer,
    su: Syncer,
    sr: Syncer,
    sd: Syncer,
    a: Arc<Engine>,
    p: Arc<Engine>,
    u: Arc<Engine>,
    r: Arc<Engine>,
    d: Arc<Engine>,
    mem: Arc<InMemoryTransport>,
    _dir: Dir,
}

fn mesh(tag: &str) -> Mesh {
    let dir = Dir::new(tag);
    let mem = Arc::new(InMemoryTransport::new());
    for p in [A, P, U, R, D] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();
    let [a, p, u, r, d] = [A, P, U, R, D].map(|id| db(&dir.db(id), id));
    u.set_gossip_remote_apply(true);
    r.set_gossip_remote_apply(true);
    let [sa, sp, su, sr, sd] = [&a, &p, &u, &r, &d].map(|e| Syncer::new(e.clone(), transport.clone()));
    su.serve_state();
    let m = Mesh { sa, sp, su, sr, sd, a, p, u, r, d, mem, _dir: dir };
    // A の行 1 と P の行 100 が全員に届く
    let r1 = m.a.entity_in("notes").unwrap();
    m.a.tie_to(r1, "notes.k", 1u32);
    m.a.tie_to(r1, "notes.v", 10u32);
    m.a.tie_text_to(r1, "notes.t", "a1");
    m.a.oplog_commit();
    std::thread::sleep(Duration::from_millis(3));
    write(&m.p, 100, 1);
    for _ in 0..2 {
        m.upper();
        m.lower();
    }
    for e in [&m.p, &m.u, &m.r, &m.d] {
        assert_eq!((v(e, 1), t(e, 1).as_deref(), v(e, 100)), (Some(10), Some("a1"), Some(1)), "前提: 行 1 と行 100 が全員に届いた");
    }
    m
}

impl Mesh {
    fn publish(e: &Engine, s: &Syncer) {
        e.oplog_commit();
        e.oplog_sync().unwrap();
        s.publish_since(Hlc::ZERO);
    }
    fn a_publishes_and_u_pulls(&self) {
        Self::publish(&self.a, &self.sa);
        assert!(!self.su.pull_once(A).history_truncated, "前提: U は抜けを言われない");
    }
    fn p_publishes_and_u_pulls(&self) {
        Self::publish(&self.p, &self.sp);
        assert!(!self.su.pull_once(P).history_truncated, "前提: U は抜けを言われない");
    }
    /// A・P → U、 U → P。
    fn upper(&self) {
        self.a_publishes_and_u_pulls();
        self.p_publishes_and_u_pulls();
        Self::publish(&self.u, &self.su);
        assert!(!self.sp.pull_once(U).history_truncated, "前提: P は抜けを言われない");
    }
    /// U → R → D (R が抜けを言われたら panic)。
    fn lower(&self) {
        Self::publish(&self.u, &self.su);
        let out = self.sr.pull_once(U);
        assert!(!out.history_truncated, "前提: R は抜けを言われない ({:?})", out.truncated_authors);
        self.r_to_d();
    }
    fn r_to_d(&self) {
        Self::publish(&self.r, &self.sr);
        assert!(!self.sd.pull_once(R).history_truncated, "前提: D は抜けを言われない");
    }
    fn cursor(s: &Syncer, link: PeerId, author: PeerId) -> Hlc {
        s.cursors_of(link).into_iter().find(|(a, _)| *a == author).map_or(Hlc::ZERO, |(_, h)| h)
    }
    /// U の A の floor を R の cursor のすぐ上に置き (= U が A の record を回収した、 と言う)、 R が U の replica の state で A を
    /// 取り直す。
    fn r_bootstraps_a_from_u(&self) {
        let cur = Self::cursor(&self.sr, U, A);
        assert!(Self::cursor(&self.su, A, A) > cur, "前提: U は A の record を R より先まで受けている");
        self.mem.set_history_floor_multi(U, &[(A, Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer })]);
        Self::publish(&self.u, &self.su);
        let out = self.sr.pull_once(U);
        assert!(out.history_truncated && out.truncated_authors == vec![A], "前提: R は A だけ抜けを言われた ({out:?})");
        let boot = self.sr.bootstrap_pull_via(U, A).expect("前提: R は U から A の state を取れる");
        assert_eq!(boot.as_of, Self::cursor(&self.su, A, A), "前提: replica の state の as_of は、 U が A の record を受けた位置");
        assert_eq!(Self::cursor(&self.sr, U, A), boot.as_of, "前提: R の A の cursor は as_of");
    }
}

/// Tag の形: R の留守中に P が自分の行を書き、 その後に A の行の Tag を A の語彙に無い語に書き換える。 R が取り直した state には
/// その cell が P の record の形で入っている。 P が留守中に書いた行も D に届く。
#[test]
fn rows_a_third_peer_wrote_before_rewriting_the_authors_tag_reach_a_peer_below_the_second_relay() {
    let m = mesh("tag");
    // R の留守中: A が行 2 を書き (U の A の cursor が進む)、 P が行 101・102 を書いてから A の行 1 の t を書き換える
    write(&m.a, 2, 20);
    m.a_publishes_and_u_pulls();
    write(&m.p, 101, 2);
    write(&m.p, 102, 3);
    let p_row1 = row(&m.p, 1).unwrap();
    m.p.tie_text_to(p_row1, "notes.t", "pw");
    m.p.oplog_commit();
    let t_himo = m.p.himo_id("notes.t").unwrap() as u16;
    let hp = m.p.cell_hlc(p_row1, t_himo);
    m.p_publishes_and_u_pulls();
    assert_eq!((t(&m.u, 1).as_deref(), v(&m.u, 101), v(&m.u, 102)), (Some("pw"), Some(2), Some(3)), "前提: U は P の書き込みを全部当てた");
    let a_row1 = row(&m.a, 1).unwrap();
    let (state, _) = m.u.state_records_for(A);
    assert!(
        state.iter().any(|r| r.author_peer == P && r.hlc == hp && matches!(&r.op, DecodedOp::Tie { eid, himo_id, .. } if *eid == a_row1 && *himo_id == t_himo)),
        "前提: U の A の state に、 行 1 の t が P の record の形 (版数は P の時計) で入っている"
    );
    assert!(Mesh::cursor(&m.sr, U, P) < hp, "前提: R は P の書き換えをまだ受けていない");
    assert_eq!(v(&m.r, 101), None, "前提: R は P の行 101 をまだ受けていない");

    m.r_bootstraps_a_from_u();
    m.r_to_d();
    println!("[tag] hp {hp:?} / D の P の cursor {:?} / R の P の cursor {:?}", Mesh::cursor(&m.sd, R, P), Mesh::cursor(&m.sr, U, P));
    // R が戻る: U から差分を受けて、 D へ
    for _ in 0..3 {
        m.lower();
    }
    assert_eq!((v(&m.r, 101), v(&m.r, 102), t(&m.r, 1).as_deref()), (Some(2), Some(3), Some("pw")), "前提: P の書き込みは差分で R に届いた");
    let got = [v(&m.d, 101), v(&m.d, 102)];
    assert_eq!(got, [Some(2), Some(3)], "D の、 P の行 101・102 の v: D の cursor が P の record を越えた (D の P の cursor {:?})", Mesh::cursor(&m.sd, R, P));
    assert_eq!((t(&m.d, 1).as_deref(), v(&m.d, 2)), (Some("pw"), Some(20)), "D の、 行 1 の t と行 2 の v");
}

/// Number の形: A が行 3 を書き (まだ出していない)、 その後に P が A の行 1 の v を書き換える。 R が取り直した state には
/// その cell が A の record の形・P の時計の版数 (as_of より後) で入っている。 A の行 3 は後で差分で届き、 D にも届く。
#[test]
fn a_row_the_author_wrote_before_a_third_peers_rewrite_reaches_a_peer_below_the_second_relay() {
    let m = mesh("num");
    write(&m.a, 2, 20);
    m.a_publishes_and_u_pulls();
    // A が行 3 を書く (出すのは後)。 その後に P が A の行 1 の v を書き換える
    write(&m.a, 3, 30);
    let v_himo = m.a.himo_id("notes.v").unwrap() as u16;
    let h3 = m.a.cell_hlc(row(&m.a, 3).unwrap(), v_himo);
    let p_row1 = row(&m.p, 1).unwrap();
    m.p.tie_to(p_row1, "notes.v", 77u32);
    m.p.oplog_commit();
    let hp = m.p.cell_hlc(p_row1, v_himo);
    assert!(h3 < hp, "前提: A の行 3 の書き込み {h3:?} < P の書き換え {hp:?}");
    m.p_publishes_and_u_pulls();
    assert_eq!((v(&m.u, 1), v(&m.u, 3)), (Some(77), None), "前提: U は P の書き換えを当てた、 A の行 3 はまだ");
    let a_row1 = row(&m.a, 1).unwrap();
    let (state, _) = m.u.state_records_for(A);
    assert!(
        state.iter().any(|r| r.author_peer == A && r.hlc == hp && matches!(&r.op, DecodedOp::Tie { eid, himo_id, .. } if *eid == a_row1 && *himo_id == v_himo)),
        "前提: U の A の state に、 行 1 の v が A の record の形・P の時計の版数で入っている"
    );
    assert!(Mesh::cursor(&m.su, A, A) < h3, "前提: U が A の record を受けた位置 (= state の as_of) は行 3 の書き込みより前");

    m.r_bootstraps_a_from_u();
    m.r_to_d();
    println!("[num] h3 {h3:?} / hp {hp:?} / D の A の cursor {:?}", Mesh::cursor(&m.sd, R, A));
    // A の行 3 が差分で U → R → D へ
    for _ in 0..3 {
        m.a_publishes_and_u_pulls();
        m.lower();
    }
    assert_eq!((v(&m.r, 3), v(&m.r, 1)), (Some(30), Some(77)), "前提: A の行 3 と P の書き換えは R に届いた");
    assert_eq!(v(&m.d, 3), Some(30), "D の、 A の行 3 の v: D の cursor が A の record を越えた (D の A の cursor {:?})", Mesh::cursor(&m.sd, R, A));
    assert_eq!((v(&m.d, 1), v(&m.d, 2)), (Some(77), Some(20)), "D の、 行 1 と行 2 の v");
}

/// 差分で届かない時 (U が P のその record をもう回収した、 と広告している) は、 今までどおり state から当てて載せる。
#[test]
fn foreign_records_the_link_already_reclaimed_are_still_applied_from_the_state_and_relayed() {
    let m = mesh("reclaimed");
    write(&m.a, 2, 20);
    m.a_publishes_and_u_pulls();
    write(&m.p, 101, 2);
    // P が A の行 1 の v (state では A の record の形) と t (P の record の形) を書き換える
    let p_row1 = row(&m.p, 1).unwrap();
    m.p.tie_to(p_row1, "notes.v", 77u32);
    m.p.tie_text_to(p_row1, "notes.t", "pw");
    m.p.oplog_commit();
    let hv = m.p.cell_hlc(p_row1, m.p.himo_id("notes.v").unwrap() as u16);
    let hp = m.p.cell_hlc(p_row1, m.p.himo_id("notes.t").unwrap() as u16);
    assert!(hv < hp, "前提: v の書き換え < t の書き換え");
    m.p_publishes_and_u_pulls();
    assert!(Mesh::cursor(&m.sr, U, P) < hv, "前提: R は P の書き換えをまだ受けていない");
    assert!(Mesh::cursor(&m.su, A, A) < hv, "前提: v の書き換えの版数は、 取り直した後の R の A の cursor より後");
    // U が、 A の record と、 P の書き換えまでの record を回収した、 と広告する
    let cur = Mesh::cursor(&m.sr, U, A);
    m.mem.set_history_floor_multi(U, &[(A, Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer }), (P, hp)]);
    Mesh::publish(&m.u, &m.su);
    let out = m.sr.pull_once(U);
    assert!(out.history_truncated && out.truncated_authors.contains(&A) && out.truncated_authors.contains(&P), "前提: R は A と P の抜けを言われた ({out:?})");
    assert!(m.sr.bootstrap_pull_via(U, A).is_some(), "前提: R は U から A の state を取れる");
    assert_eq!((v(&m.r, 1), t(&m.r, 1).as_deref()), (Some(77), Some("pw")), "R の行 1 の v と t: 差分では届かない書き換えは、 state から当てる");
    Mesh::publish(&m.r, &m.sr);
    let ring: Vec<_> = m.r.pending_sync_ops(0).iter().filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p)).collect();
    assert!(ring.iter().any(|r| r.hlc == hv && r.author_peer == A), "R の ring: state から当てた v の書き換えは載せる");
    assert!(
        ring.iter().any(|r| r.hlc == hp && r.author_peer == P && matches!(r.op, DecodedOp::Tie { .. })),
        "R の ring: state から当てた t の書き換えは載せる"
    );
    // D が受けるのは v だけ確かめる (t の Vocab と Tie は同じ版数で載り、 `InMemoryTransport` の pull が同じ版数の 2 つ目を落とす = #827)
    assert!(!m.sd.pull_once(R).history_truncated, "前提: D は抜けを言われない");
    assert_eq!(v(&m.d, 1), Some(77), "D の行 1 の v");
}

/// 差分で届かない時 (link が、 書き換えた peer の record を運ばない): A → R → D で、 P は A とだけ同期する。 A 自身の state の、
/// P の時計の版数 (as_of より後) の cell は、 今までどおり当てて載せる (#786 の越えは、 この形では残る)。
#[test]
fn a_foreign_clock_cell_the_link_does_not_carry_is_still_applied_and_relayed() {
    use enchudb::transport::StateBatch;
    let dir = Dir::new("nocarry");
    let mem = Arc::new(InMemoryTransport::new());
    for p in [A, P, R, D] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();
    let [a, p, r, d] = [A, P, R, D].map(|id| db(&dir.db(id), id));
    r.set_gossip_remote_apply(true);
    let [sa, sp, sr, sd] = [&a, &p, &r, &d].map(|e| Syncer::new(e.clone(), transport.clone()));
    write(&a, 1, 10);
    write(&a, 2, 20);
    for _ in 0..2 {
        Mesh::publish(&a, &sa);
        assert!(!sr.pull_once(A).history_truncated);
        Mesh::publish(&r, &sr);
        assert!(!sd.pull_once(R).history_truncated);
    }
    assert!(!sp.pull_once(A).history_truncated);
    assert_eq!((v(&p, 2), v(&d, 2)), (Some(20), Some(20)), "前提: P と D は A の行を受けた");
    let v_himo = a.himo_id("notes.v").unwrap() as u16;
    let row2 = row(&a, 2).unwrap();
    let cur = Mesh::cursor(&sr, A, A);
    mem.set_history_floor_multi(A, &[(A, Hlc { wall: cur.wall, logical: cur.logical + 1, peer: cur.peer })]);
    // A の state の走査が as_of を採った後に、 P が A の行 2 を書いた record が A に当たり、 走査がその cell を読む
    let (scan, as_of) = a.state_records();
    std::thread::sleep(Duration::from_millis(3));
    p.tie_to(row(&p, 2).unwrap(), "notes.v", 77u32);
    Mesh::publish(&p, &sp);
    assert!(sa.pull_once(P).applied > 0, "前提: A は P の書き込みを受けた");
    let hp = a.cell_hlc(row2, v_himo);
    assert!(hp.peer == P && as_of < hp, "前提: as_of {as_of:?} < P の書き込み {hp:?}");
    let is_row2_v = |r: &enchudb::transport::WireRecord| matches!(&r.op, DecodedOp::Tie { eid, himo_id, .. } if *eid == row2 && *himo_id == v_himo);
    let late = a.state_records().0.into_iter().find(|r| is_row2_v(r)).expect("前提: state に行 2 の v がある");
    assert_eq!((late.hlc, late.author_peer), (hp, A), "前提: state の行 2 の v は、 A の record の形で版数は P の時計");
    let records: Vec<_> = scan.into_iter().map(|r| if is_row2_v(&r) { late.clone() } else { r }).collect();
    let batch = StateBatch { records, as_of, complete: true };
    mem.register_state_provider(A, Arc::new(move || Some(batch.clone())));
    assert!(Mesh::cursor(&sr, A, P) == Hlc::ZERO, "前提: R は link A から P の record を受けたことが無い");
    let out = sr.pull_once(A);
    assert!(out.history_truncated && out.truncated_authors == vec![A], "前提: R は A の抜けを言われた ({out:?})");
    assert!(sr.bootstrap_pull_via(A, A).is_some(), "前提: R は A の state を取れる");
    assert_eq!(v(&r, 2), Some(77), "R の行 2 の v: 他に届く道の無い書き換えは、 state から当てる");
    Mesh::publish(&r, &sr);
    assert!(!sd.pull_once(R).history_truncated);
    assert_eq!(v(&d, 2), Some(77), "D の行 2 の v: state から当てた書き換えは載せる");
    drop((sa, sp, sr, sd));
}

/// A の record の形の cell で、 P の時計の版数が取り直した後の R の A の cursor 以下の物は、 今までどおり state から当てて載せる
/// (A の record はもうそこまで載せてあるので、 D の cursor は越えない)。
#[test]
fn a_foreign_clock_cell_at_or_below_the_authors_cursor_is_applied_from_the_state_and_relayed() {
    let m = mesh("below");
    // P が A の行 1 の v を書き換え、 その後に A が行 2 を書く (U の A の cursor = state の as_of は、 P の書き換えより後)
    let p_row1 = row(&m.p, 1).unwrap();
    m.p.tie_to(p_row1, "notes.v", 77u32);
    m.p.oplog_commit();
    let hp = m.p.cell_hlc(p_row1, m.p.himo_id("notes.v").unwrap() as u16);
    std::thread::sleep(Duration::from_millis(3));
    write(&m.a, 2, 20);
    m.p_publishes_and_u_pulls();
    m.a_publishes_and_u_pulls();
    assert!(hp < Mesh::cursor(&m.su, A, A), "前提: P の書き換えの版数 < U が A の record を受けた位置");
    assert!(Mesh::cursor(&m.sr, U, P) < hp, "前提: R は P の書き換えをまだ受けていない");
    m.r_bootstraps_a_from_u();
    assert_eq!(v(&m.r, 1), Some(77), "R の行 1 の v: state から当てる");
    m.r_to_d();
    assert_eq!((v(&m.d, 1), v(&m.d, 2)), (Some(77), Some(20)), "D の行 1・行 2 の v");
    assert!(Mesh::cursor(&m.sd, R, A) <= Mesh::cursor(&m.sr, U, A), "D の A の cursor は、 R の A の cursor を越えない");
}
