//! #698: ホストは、 自分の LWW で負けた record も中継に載せる。
//!
//! 前は当たった record だけ載せていた。 名前で束ねる表では、 負けた名前のカード (主キーの Tie) が 「この世界番号はこの名前の
//! デッキ」 を運ぶ唯一の record なので、 ホストの下の client はその世界番号を名前に結べず、 その世界番号宛ての後の書き込み・
//! 削除・改名が自分の行に当たらなかった (ホストに届く順で、 配られる record の集合が変わった)。
//!
//! - 形 A / B / C: 相手から直に pull する形 (record を全部受ける) と同じ答えに、 全員がなること
//! - 乱数 model (client 3 台 + ホスト): 種を固定した手順で、 全員の答えが同じになること
//! - cursor が進まずに届き直した負けた record を、 2 度載せないこと

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_transport::host::{HttpHost, HttpHostTransport};

const T: &str = "t";
const OPLOG: usize = 1 << 20;
const HOST: PeerId = 100;

static RUN: AtomicU64 = AtomicU64::new(0);

struct Dir(PathBuf);
impl Dir {
    fn new(tag: &str) -> Self {
        let n = RUN.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("enchudb-issue698-{tag}-{}-{n}", std::process::id()));
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

fn create(path: &str, id: u32) -> Arc<Database> {
    let mut b = Database::create_with_capacity(path, 4096).unwrap();
    b.table(T).tag("key").tag("title").primary_key("key").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(OPLOG).unwrap();
    db.engine().set_peer_id(id);
    db
}

type Answer = Vec<(Option<Value>, Option<Value>)>;

/// 表の全部の行 (名前の無い塊も) を (key, title) で。
fn rows(db: &Database) -> Answer {
    db.engine().rebuild();
    let t = db.get_table(T).unwrap();
    let mut eids = t.all().find().unwrap();
    eids.extend(t.all().where_ne("title", "\u{0}").find().unwrap());
    eids.sort_unstable();
    eids.dedup();
    let mut out: Answer = eids
        .into_iter()
        .map(|e| {
            let r = t.entity(e);
            (r.get("key"), r.get("title"))
        })
        .collect();
    out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    out
}

fn settle(db: &Database) {
    let eng = db.engine();
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    // 次の書き込みの HLC の壁時計を進める (別の client の書き込みとの前後を、 書いた順にする)
    std::thread::sleep(Duration::from_millis(3));
}

fn find(db: &Database, k: &str) -> Option<u64> {
    db.engine().rebuild();
    db.get_table(T).unwrap().where_eq("key", k).find().unwrap().first().copied()
}

fn upsert(db: &Database, k: &str, v: &str) {
    db.get_table(T).unwrap().upsert().set("key", k).set("title", v).commit().unwrap();
    settle(db);
}

fn delete(db: &Database, k: &str) {
    let e = find(db, k).unwrap();
    db.get_table(T).unwrap().entity(e).delete().unwrap();
    settle(db);
}

fn rename(db: &Database, from: &str, to: &str) {
    let e = find(db, from).unwrap();
    db.get_table(T).unwrap().entity(e).set("key", to).commit().unwrap();
    settle(db);
}

fn retitle(db: &Database, k: &str, v: &str) {
    let e = find(db, k).unwrap();
    db.get_table(T).unwrap().entity(e).set("title", v).commit().unwrap();
    settle(db);
}

/// ring の 1 件: (書き手, HLC, 名前の Tie ならその世界番号)。
struct Ringed {
    author: PeerId,
    hlc: Hlc,
    name_tie_of: Option<u64>,
    line: String,
}

/// `db` の `_sync_ops` (ホストなら client に配る record)。
fn ring(db: &Database) -> Vec<Ringed> {
    let eng = db.engine();
    let key = eng.himo_id("t.key").map(|h| h as u16);
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(|r| {
            let name_tie_of = match &r.op {
                DecodedOp::Tie { eid, himo_id, .. } if Some(*himo_id) == key => Some(*eid),
                _ => None,
            };
            let op = match &r.op {
                DecodedOp::Tie { eid, .. } => format!(
                    "Tie {}:{} {}",
                    enchudb_oplog::eid_peer(*eid),
                    enchudb_oplog::eid_local(*eid),
                    if name_tie_of.is_some() { "key" } else { "title" }
                ),
                DecodedOp::Delete { eid } => {
                    format!("Delete {}:{}", enchudb_oplog::eid_peer(*eid), enchudb_oplog::eid_local(*eid))
                }
                DecodedOp::Vocab { bytes, .. } => format!("Vocab {:?}", String::from_utf8_lossy(bytes)),
                other => format!("{other:?}"),
            };
            Ringed { author: r.author_peer, hlc: r.hlc, name_tie_of, line: format!("author {} @{:?} {op}", r.author_peer, r.hlc) }
        })
        .collect()
}

fn ring_lines(db: &Database) -> String {
    ring(db).iter().map(|r| format!("    {}\n", r.line)).collect()
}

/// `author` が書いた名前の Tie の HLC (ring に載っている物、 載った順)。
fn name_ties(db: &Database, author: PeerId) -> Vec<Hlc> {
    ring(db).iter().filter(|r| r.author == author && r.name_tie_of.is_some()).map(|r| r.hlc).collect()
}

struct Client {
    db: Arc<Database>,
    t: Arc<HttpHostTransport>,
    s: Syncer,
}

impl Client {
    fn attach(db: Arc<Database>, url: &str, cursors: Option<String>) -> Self {
        let t = Arc::new(HttpHostTransport::connect(url.to_string(), db.engine().peer_id()).unwrap());
        let mut s = Syncer::new(db.arc_engine(), t.clone());
        if let Some(p) = cursors {
            s = s.with_cursor_path(p.into());
        }
        s.serve_state();
        Client { db, t, s }
    }
    fn new(dir: &Dir, id: u32, url: &str) -> Self {
        Self::attach(create(&dir.db(id), id), url, None)
    }
    fn push(&self) {
        settle(&self.db);
        self.s.publish_since(Hlc::ZERO);
    }
    fn pull(&self) {
        let out = self.s.pull_once(HOST);
        assert!(!out.history_truncated, "{out:?}");
    }
}

/// 同じ書き込みを、 直に pull する形 (中継なし = 書き手ごとの record を全部受ける) で 1 台の受け手に当てた答え。
fn direct_answer(dir: &Dir, script: impl Fn(&[Arc<Database>])) -> Answer {
    let tr = Arc::new(InMemoryTransport::new());
    for id in 1..=4 {
        tr.register_peer(id);
    }
    let dbs: Vec<Arc<Database>> = (1..=3).map(|id| create(&dir.db(10 + id), id)).collect();
    script(&dbs);
    let r = create(&dir.db(14), 4);
    let rs = Syncer::new(r.arc_engine(), tr.clone() as Arc<dyn Transport>);
    for db in &dbs {
        let s = Syncer::new(db.arc_engine(), tr.clone() as Arc<dyn Transport>);
        settle(db);
        s.publish_since(Hlc::ZERO);
    }
    for id in 1..=3 {
        rs.pull_once(id);
    }
    rows(&r)
}

struct Through {
    /// client 1..3 とホストの答え。
    answers: Vec<(String, Answer)>,
    /// 直に pull した受け手の答え。
    direct: Answer,
    /// `push_order` の通りに出した直後の、 ホストの ring。
    host_ring: Vec<Ringed>,
}

/// `script` の書き込みを client 3 台にして、 `push_order` の順にホストへ出し、 その後全員が出して受ける。
fn through_host(tag: &str, script: impl Fn(&[Arc<Database>]), push_order: &[usize]) -> Through {
    let dir = Dir::new(tag);
    let direct = direct_answer(&dir, &script);
    let host_db = create(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_db.arc_engine(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let cs: Vec<Client> = (1..=3).map(|id| Client::new(&dir, id, &url)).collect();
    let dbs: Vec<Arc<Database>> = cs.iter().map(|c| c.db.clone()).collect();
    script(&dbs);
    drop(dbs);
    for &i in push_order {
        cs[i].push();
    }
    settle(&host_db);
    let host_ring = ring(&host_db);
    for _ in 0..4 {
        for c in &cs {
            c.push();
            c.pull();
        }
    }
    let mut answers: Vec<(String, Answer)> =
        cs.iter().enumerate().map(|(i, c)| (format!("client {}", i + 1), rows(&c.db))).collect();
    answers.push(("host".into(), rows(&host_db)));
    drop(cs);
    drop(host);
    drop(host_db);
    Through { answers, direct, host_ring }
}

fn all_same(tag: &str, t: &Through) {
    let lines: String = t.host_ring.iter().map(|r| format!("    {}\n", r.line)).collect();
    assert!(
        t.answers.iter().all(|(_, a)| a == &t.answers[0].1),
        "[{tag}] ホスト経由で答えが割れた: {:?} (直に pull した受け手: {:?})\nホストの ring:\n{lines}",
        t.answers,
        t.direct
    );
    assert_eq!(&t.answers[0].1, &t.direct, "[{tag}] ホスト経由の答えが、 直に pull した答えと違う\nホストの ring:\n{lines}");
}

/// `loser` の最初の名前の Tie が、 `winner` の名前の Tie より古い HLC で、 `winner` より後にホストの ring に載っていること
/// (= ホストは `winner` の行を先に持っていて、 `loser` の名前の Tie は LWW で負けた。 それでも中継に載った)。
fn losing_name_tie_was_relayed(tag: &str, ring: &[Ringed], loser: PeerId, winner: PeerId) {
    let lines: String = ring.iter().map(|r| format!("    {}\n", r.line)).collect();
    let pos = |a: PeerId| ring.iter().position(|r| r.author == a && r.name_tie_of.is_some());
    let (Some(w), Some(l)) = (pos(winner), pos(loser)) else {
        panic!("[{tag}] ホストの ring に、 負けた名前の Tie (書き手 {loser}) が無い — 中継されていない\n{lines}");
    };
    // 前提: 本当に負ける形だった (先に当たった方が新しい)
    assert!(w < l, "[{tag}] 前提: 書き手 {winner} の名前の Tie が先にホストに当たっていない\n{lines}");
    assert!(ring[l].hlc < ring[w].hlc, "[{tag}] 前提: 書き手 {loser} の名前の Tie が古くない (負けない形)\n{lines}");
}

/// 形 A: client 3 が k を作る → client 1 が (知らずに) 同じ k を作る (後) → client 3 が自分の k を消す (さらに後)。
/// 削除優先: client 1 の行は削除より前に作られた同じデッキなので、 全員で消える。 client 1 が先に出すと、 ホストでは client 3 の
/// 名前の Tie が負ける。 それを配らないと、 client 3 の世界番号宛ての削除が client 1 / 2 の同じ名前の行に当たらなかった。
#[test]
fn shape_a_a_delete_addressed_to_an_identity_whose_name_tie_lost() {
    let script = |dbs: &[Arc<Database>]| {
        upsert(&dbs[2], "k", "from3");
        upsert(&dbs[0], "k", "from1");
        delete(&dbs[2], "k");
    };
    let t = through_host("A", script, &[0, 2]);
    losing_name_tie_was_relayed("A", &t.host_ring, 3, 1);
    assert_eq!(t.direct, vec![], "前提: 直に pull した受け手では行が消える");
    all_same("A", &t);
}

/// 形 A の対照: client 3 が先に出す (client 3 の作成がホストで当たる)。 前から通る。
#[test]
fn shape_a_control_the_same_writes_pushed_in_the_other_order() {
    let script = |dbs: &[Arc<Database>]| {
        upsert(&dbs[2], "k", "from3");
        upsert(&dbs[0], "k", "from1");
        delete(&dbs[2], "k");
    };
    let t = through_host("A-control", script, &[2, 0]);
    all_same("A-control", &t);
}

/// 形 B (削除なし): 負けた改名。 client 2 が k1 → k2 に改名 (先)、 client 1 が k1 → k0 に改名 (後、 勝つ)。 client 3 は 2 つの
/// 改名の間に k2 を作る (その時刻のデッキの名前は k2 = 同じデッキ)。 client 1 が先に出すと、 client 2 の作成と改名がホストで負ける。
#[test]
fn shape_b_a_rename_that_lost() {
    let script = |dbs: &[Arc<Database>]| {
        upsert(&dbs[1], "k1", "from2");
        upsert(&dbs[0], "k1", "from1");
        rename(&dbs[1], "k1", "k2");
        upsert(&dbs[2], "k2", "from3");
        rename(&dbs[0], "k1", "k0");
    };
    let t = through_host("B", script, &[0, 1, 2]);
    // client 2 の名前の Tie は 2 つ (作成と改名) とも client 1 の最後の改名より古い = 負ける。 2 つとも載っていること
    let lines: String = t.host_ring.iter().map(|r| format!("    {}\n", r.line)).collect();
    let of = |a: PeerId| -> Vec<Hlc> { t.host_ring.iter().filter(|r| r.author == a && r.name_tie_of.is_some()).map(|r| r.hlc).collect() };
    let newest_of_1 = of(1).into_iter().max().expect("client 1 の名前の Tie");
    assert_eq!(of(2).len(), 2, "[B] client 2 の負けた名前の Tie (作成・改名) が中継に載っていない\n{lines}");
    assert!(of(2).iter().all(|h| *h < newest_of_1), "[B] 前提: client 2 の名前の Tie が負ける形でない\n{lines}");
    assert_eq!(t.direct.len(), 1, "前提: 直に pull した受け手では 1 行に束なる: {:?}", t.direct);
    all_same("B", &t);
}

/// 形 C (削除も改名も無い、 いちばん普通の形): 2 台がオフラインで同じ名前の行を作り (client 1 が先、 client 2 が後)、 ホストには
/// client 2 が先に出す。 client 1 の作成はホストで負ける。 その後 client 1 が自分の行の title を書き換える (宛名は自分の
/// 世界番号)。 負けた作成を配らないと、 他の client はその世界番号を知らず、 書き換えは名前の無い塊に入った。
#[test]
fn shape_c_an_edit_addressed_to_an_identity_whose_create_lost() {
    let dir = Dir::new("C");
    let host_db = create(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_db.arc_engine(), None).unwrap();
    let url = format!("http://{}", host.addr());
    let cs: Vec<Client> = (1..=3).map(|id| Client::new(&dir, id, &url)).collect();
    upsert(&cs[0].db, "k", "from1");
    upsert(&cs[1].db, "k", "from2");
    cs[1].push();
    cs[0].push();
    for _ in 0..2 {
        for c in &cs {
            c.push();
            c.pull();
        }
    }
    let k = |v: &str| vec![(Some(Value::Text("k".into())), Some(Value::Text(v.into())))];
    // 前提: client 1 の作成はホストで負けた (ホストの行は client 2 の値のまま)
    assert_eq!(rows(&host_db), k("from2"), "前提: ホストで client 1 の作成が負けていない");
    settle(&host_db);
    losing_name_tie_was_relayed("C", &ring(&host_db), 1, 2);

    retitle(&cs[0].db, "k", "edited-by-1");
    for _ in 0..4 {
        for c in &cs {
            c.push();
            c.pull();
        }
    }
    let mut answers: Vec<(String, Answer)> =
        cs.iter().enumerate().map(|(i, c)| (format!("client {}", i + 1), rows(&c.db))).collect();
    answers.push(("host".into(), rows(&host_db)));
    let lines = ring_lines(&host_db);
    drop(cs);
    drop(host);
    drop(host_db);
    for (who, a) in &answers {
        assert_eq!(a, &k("edited-by-1"), "[C] {who} に client 1 の書き換えが当たっていない: {answers:?}\nホストの ring:\n{lines}");
    }
}

/// cursor が進まずに届き直した record: 負けた record を 2 度載せない。
///
/// ホストは署名必須。 client 1 は鍵を持つ前に 1 行書き (署名なし = ホストが断り、 client 1 の cursor はそこで止まる)、 鍵を持って
/// から k を作る (client 2 の k より古い = 負ける)。 client 1 は ack が進まないので、 出すたびに同じ record を送り直す。 負けた
/// record はホストで毎回 「負け」 なので、 見分けずに載せるとホストの ring が出すたびに伸びる。
#[test]
fn a_losing_record_redelivered_under_a_held_cursor_is_relayed_once() {
    let dir = Dir::new("held");
    let host_db = create(&dir.db(HOST), HOST);
    let host = HttpHost::start("127.0.0.1:0", host_db.arc_engine(), None).unwrap();
    host.syncer().set_require_signature(true);
    let url = format!("http://{}", host.addr());
    let cs: Vec<Client> = (1..=2).map(|id| Client::new(&dir, id, &url)).collect();
    let keys: Vec<Arc<enchudb::keys::Keypair>> = (0..2).map(|_| Arc::new(enchudb::keys::Keypair::generate())).collect();
    for (i, kp) in keys.iter().enumerate() {
        assert!(host_db.engine().pubkeys().force_register(i as u32 + 1, &kp.public_bytes()));
    }
    upsert(&cs[0].db, "unsigned", "x");
    cs[0].db.engine().set_keypair(Some(keys[0].clone()));
    cs[1].db.engine().set_keypair(Some(keys[1].clone()));
    upsert(&cs[0].db, "k", "from1");
    upsert(&cs[1].db, "k", "from2");
    cs[1].push();
    let mut counts = Vec::new();
    for _ in 0..4 {
        cs[0].push();
        settle(&host_db);
        counts.push(ring(&host_db).iter().filter(|r| r.author == 1).count());
    }
    let lines = ring_lines(&host_db);
    // 前提: ホストは client 1 の最初の行を断り、 cursor を進めていない (= 毎回同じ record が届き直した)
    let k = |v: &str| (Some(Value::Text("k".into())), Some(Value::Text(v.into())));
    assert_eq!(rows(&host_db), vec![k("from2")], "前提: 署名の無い行を断り、 client 1 の k が負ける形\n{lines}");
    assert!(
        host.syncer().cursors_of(1).iter().all(|(a, h)| *a != 1 || *h == Hlc::ZERO),
        "前提: ホストの client 1 の cursor が進んだ (届き直していない): {:?}",
        host.syncer().cursors_of(1)
    );
    // 負けた名前の Tie は載っていて、 1 回だけ
    assert_eq!(name_ties(&host_db, 1).len(), 1, "client 1 の負けた名前の Tie が 1 回だけ載っていること\n{lines}");
    assert!(counts.iter().all(|c| *c == counts[0]), "届き直すたびにホストの ring が伸びた: {counts:?}\n{lines}");
    drop(cs);
    drop(host);
}

// ─────────────────────────────────────────────────────────────
// 乱数 model: client 3 台 + ホスト。 乱数で並べた手順 (作成 / 改名 / 削除 / 書き換え / 出す / 受ける / 開き直す / ホストの回収)
// を当て、 最後に全員が出して受けた後、 client とホストの答えが全部同じかを見る
// ─────────────────────────────────────────────────────────────

const CLIENTS: usize = 3;
const KEYS: u8 = 3;
const TITLES: u8 = 3;

fn key(k: u8) -> String {
    format!("k{k}")
}
fn title(t: u8) -> String {
    format!("t{t}")
}

#[derive(Debug, Clone, PartialEq)]
enum Step {
    Upsert { by: usize, key: u8, title: u8 },
    Rename { by: usize, from: u8, to: u8 },
    Delete { by: usize, key: u8 },
    Retitle { by: usize, key: u8, title: u8 },
    Push { by: usize },
    Pull { by: usize },
    Reopen { by: usize },
    Reclaim,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    /// 線なし (ホストの cursor の置き場なし)
    Plain,
    /// 線あり + ホストの回収 + client の開き直し
    All,
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn gen_steps(rng: &mut Rng, mode: Mode) -> Vec<Step> {
    let n = 2 + rng.below(12) as usize;
    let mut out = Vec::new();
    for _ in 0..n {
        let by = rng.below(CLIENTS as u64) as usize;
        let k = rng.below(KEYS as u64) as u8;
        let v = rng.below(TITLES as u64) as u8;
        let s = match rng.below(20) {
            0..=3 => Step::Upsert { by, key: k, title: v },
            4..=6 => {
                let to = (k + 1 + rng.below(KEYS as u64 - 1) as u8) % KEYS;
                Step::Rename { by, from: k, to }
            }
            7..=9 => Step::Delete { by, key: k },
            10 => Step::Retitle { by, key: k, title: v },
            11..=14 => Step::Push { by },
            15..=17 => Step::Pull { by },
            18 if mode == Mode::All => Step::Reopen { by },
            19 if mode == Mode::All => Step::Reclaim,
            _ => Step::Pull { by },
        };
        out.push(s);
    }
    out
}

impl Client {
    /// 受ける。 抜けを言われたら取り直して受け直す。
    fn pull_or_bootstrap(&self) {
        for _ in 0..4 {
            let out = self.s.pull_once(HOST);
            if !out.history_truncated {
                return;
            }
            let mut authors = out.truncated_authors.clone();
            if authors.is_empty() {
                authors = (1..=CLIENTS as u32).filter(|a| *a != self.db.engine().peer_id()).collect();
                authors.push(HOST);
            }
            for a in authors {
                let _ = self.s.bootstrap_pull_via(HOST, a);
            }
        }
    }

    fn write(&self, s: &Step) {
        let t = self.db.get_table(T).unwrap();
        match *s {
            Step::Upsert { key: k, title: v, .. } => {
                t.upsert().set("key", key(k).as_str()).set("title", title(v).as_str()).commit().unwrap();
            }
            Step::Rename { from, to, .. } => {
                let (Some(e), None) = (find(&self.db, &key(from)), find(&self.db, &key(to))) else { return };
                let _ = t.entity(e).set("key", key(to).as_str()).commit();
            }
            Step::Delete { key: k, .. } => {
                let Some(e) = find(&self.db, &key(k)) else { return };
                t.entity(e).delete().unwrap();
            }
            Step::Retitle { key: k, title: v, .. } => {
                let Some(e) = find(&self.db, &key(k)) else { return };
                t.entity(e).set("title", title(v).as_str()).commit().unwrap();
            }
            _ => unreachable!(),
        }
        settle(&self.db);
    }
}

fn run(steps: &[Step], mode: Mode) -> Result<Answer, String> {
    let dir = Dir::new("model");
    let host_db = create(&dir.db(HOST), HOST);
    let cursor_path = (mode == Mode::All).then(|| PathBuf::from(format!("{}.cursors", dir.db(HOST))));
    let host = HttpHost::start("127.0.0.1:0", host_db.arc_engine(), cursor_path).map_err(|e| e.to_string())?;
    let url = format!("http://{}", host.addr());
    let cur = |i: usize| Some(format!("{}.cursors", dir.db(i as u32 + 1)));
    let mut cs: Vec<Option<Client>> =
        (0..CLIENTS).map(|i| Some(Client::attach(create(&dir.db(i as u32 + 1), i as u32 + 1), &url, cur(i)))).collect();
    for s in steps {
        match *s {
            Step::Push { by } => cs[by].as_ref().unwrap().push(),
            Step::Pull { by } => cs[by].as_ref().unwrap().pull_or_bootstrap(),
            Step::Reopen { by } => {
                let c = cs[by].take().unwrap();
                settle(&c.db);
                drop(c);
                let db = Database::open_with_oplog(&dir.db(by as u32 + 1), OPLOG).map_err(|e| format!("reopen: {e:?}"))?;
                db.engine().set_peer_id(by as u32 + 1);
                cs[by] = Some(Client::attach(db, &url, cur(by)));
            }
            Step::Reclaim => {
                host_db.engine().reclaim_sync_ops();
            }
            Step::Upsert { by, .. } | Step::Rename { by, .. } | Step::Delete { by, .. } | Step::Retitle { by, .. } => {
                cs[by].as_ref().unwrap().write(s)
            }
        }
    }
    // 落ち着くまで: 全員が出して受ける
    for _ in 0..4 {
        for c in cs.iter().flatten() {
            c.push();
            c.pull_or_bootstrap();
        }
    }
    let mut answers: Vec<(String, Answer)> = Vec::new();
    let mut broken: Vec<String> = Vec::new();
    for (i, c) in cs.iter().flatten().enumerate() {
        if c.t.rebase_required() {
            broken.push(format!("client {} が STALE を受けた", i + 1));
        }
        broken.extend(c.db.engine().verify_invariants().violations.into_iter().map(|v| format!("client {}: {v}", i + 1)));
        answers.push((format!("client {}", i + 1), rows(&c.db)));
    }
    broken.extend(host_db.engine().verify_invariants().violations.into_iter().map(|v| format!("host: {v}")));
    answers.push(("host".into(), rows(&host_db)));
    drop(cs);
    drop(host);
    drop(host_db);
    if !broken.is_empty() {
        return Err(format!("不変量の違反\n  {}", broken.join("\n  ")));
    }
    for (who, a) in &answers {
        let mut names: Vec<_> = a.iter().filter_map(|r| r.0.clone()).map(|v| format!("{v:?}")).collect();
        let n = names.len();
        names.dedup();
        if names.len() != n {
            return Err(format!("{who} に同じ名前の行が 2 つ: {a:?}"));
        }
    }
    if answers.iter().all(|(_, x)| *x == answers[0].1) {
        return Ok(answers.swap_remove(0).1);
    }
    let mut msg = String::from("答えが割れる\n");
    for (who, rows) in &answers {
        msg.push_str(&format!("  {who}: {rows:?}\n"));
    }
    Err(msg)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// 種 `seed` の case `cases` を回し、 食い違った case を返す。
fn sweep(mode: Mode, seed: u64, cases: impl Iterator<Item = u64>) -> Vec<String> {
    let mut fails = Vec::new();
    for case in cases {
        let mut rng = Rng(seed.wrapping_mul(1_000_003).wrapping_add(case));
        let steps = gen_steps(&mut rng, mode);
        if let Err(msg) = run(&steps, mode) {
            fails.push(format!("{mode:?} seed {seed} case {case}\n手順 {steps:?}\n{msg}"));
        }
    }
    fails
}

/// 総点検 (2026-11-11) の model が縮めた形: client 3 が k2 を作って k0 に改名、 client 1 が (知らずに) k2 を作る。
#[test]
fn model_shrunk_shape_a_row_made_with_a_name_another_client_renamed_away() {
    let steps = [
        Step::Upsert { by: 2, key: 2, title: 2 },
        Step::Rename { by: 2, from: 2, to: 0 },
        Step::Upsert { by: 0, key: 2, title: 0 },
    ];
    for mode in [Mode::Plain, Mode::All] {
        if let Err(msg) = run(&steps, mode) {
            panic!("{mode:?}: {msg}");
        }
    }
}

/// 線なし。 `ISSUE698_SEED` / `ISSUE698_CASES` で広げられる (既定は直す前に食い違った case を含む)。
#[test]
fn model_plain() {
    let fails = match std::env::var("ISSUE698_SEED") {
        Ok(_) => sweep(Mode::Plain, env_u64("ISSUE698_SEED", 1), 0..env_u64("ISSUE698_CASES", 20)),
        Err(_) => PLAIN_CASES.iter().flat_map(|(seed, cases)| sweep(Mode::Plain, *seed, cases.iter().copied())).collect(),
    };
    assert!(fails.is_empty(), "{} 件の食い違い\n{}", fails.len(), fails.join("\n"));
}

/// 線あり + ホストの回収 + client の開き直し。
#[test]
fn model_all() {
    let fails = match std::env::var("ISSUE698_SEED") {
        Ok(_) => sweep(Mode::All, env_u64("ISSUE698_SEED", 9), 0..env_u64("ISSUE698_CASES", 20)),
        Err(_) => ALL_CASES.iter().flat_map(|(seed, cases)| sweep(Mode::All, *seed, cases.iter().copied())).collect(),
    };
    assert!(fails.is_empty(), "{} 件の食い違い\n{}", fails.len(), fails.join("\n"));
}

/// (種, case)。 直す前に食い違った case (線なし: 種 1 の 2、 種 5 の 0 / 6 / 17。 線あり: 種 9 の 28 / 67) と、 直す前から通る case。
const PLAIN_CASES: &[(u64, &[u64])] = &[(1, &[0, 1, 2, 3, 4, 5]), (5, &[0, 6, 17])];
const ALL_CASES: &[(u64, &[u64])] = &[(9, &[0, 1, 2, 3, 4, 5, 28, 67])];
