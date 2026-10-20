//! I/O の点ごとの故障 (#98 ③ fault injection / ④ crash matrix)。
//!
//! engine / oplog は filesystem の I/O の直前で `enchudb_oplog::io_point` を呼ぶ (crashsim feature の時だけ働く、
//! `enchudb_oplog::fault`)。 ここは決まった書き込みの流れを子 process で回し、 N 回目の点で
//!
//! - `error` — その I/O を `EIO` で失敗させる (③。 書き手は続ける)
//! - `exit` — process を `_exit` で落とす (④ process の死。 page cache は残る)
//! - `lost` — 電源断の像 (crashsim の `Lost`) を撮って落とす (④ 電源断)
//!
//! を起こす。 N は、 先に数えるだけの回で通った点を数え、 点の名前ごとに全部 (多ければ間引く) 選ぶ。 親は別の子 process で
//! 開き直して確かめる:
//!
//! 1. 書き手が panic / abort しない (`error`)。 `exit` / `lost` は点で落ちたこと
//! 2. 開ける。 作り終える前に落ちた DB は開けなくてよいが、 同じ path に作り直せること
//! 3. 書き出しが返った (`flush` / `oplog_sync` が Ok を返した) 書き込みは全部、 値まで残る
//! 4. それ以外の値も、 書き手がその cell に書いた (書こうとした) 値のどれか (化けた値・失敗を返した書き込みの値が出ない)
//! 5. 開き直した DB に書いて書き出せる
//!
//! 書き手は操作の前に日誌に 「これから書く」 を、 後に 「済んだ / 失敗した」 を書き (process の死でも page cache に残る。
//! 像は DB の directory だけを写すので、 日誌は電源断でも全部残る)、 確かめる側は日誌から 「書き出しが返った値」 と
//! 「その後に書いた (書きかけた) 値」 を組み立てる。
//!
//! 背景の thread (oplog の形の consumer の周期の書き出し) も同じ数を進めるので、 oplog の形では同じ N でも回によって
//! 当たる点が違いうる (当たった点は子の stderr の `ENCHU_FAULT` 行で分かる)。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

const ROLE_ENV: &str = "ENCHU_FM_ROLE";
const KIND_ENV: &str = "ENCHU_FM_KIND";
const DB_ENV: &str = "ENCHU_FM_DB";
const JOURNAL_ENV: &str = "ENCHU_FM_JOURNAL";
const OPLOG_CAP: usize = 1 << 20;
/// 点の名前ごとに選ぶ回数の上限 (それより多く通る点は、 最初・最後と間を均等に)
const PER_SITE: usize = 6;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    /// standalone: tie → `flush()` が返ったら durable (`seal_integrity` で `.crc` も書く)
    Flush,
    /// concurrent + oplog: tie_async → `oplog_sync()` が返ったら durable
    Oplog,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Flush => "flush",
            Kind::Oplog => "oplog",
        }
    }
    fn parse(s: &str) -> Kind {
        match s {
            "flush" => Kind::Flush,
            "oplog" => Kind::Oplog,
            _ => panic!("unknown kind {s}"),
        }
    }
}

const HIMOS: [(&str, ValueType); 3] = [("n", ValueType::Number), ("g", ValueType::Tag), ("l", ValueType::Leaf)];

/// 値: Number は数、 Tag / Leaf は文字列 (どれも書くたびに違う = どの書き込みの値か分かる)。
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Val {
    N(u64),
    S(String),
}

fn val_for(seq: u64, himo: &str) -> Val {
    match himo {
        "n" | "m" => Val::N(seq * 10 + 7),
        "g" => Val::S(format!("g{seq}")),
        _ => Val::S(format!("leaf-{seq}-{}", "x".repeat((seq % 7) as usize * 9))),
    }
}

// ─────────────────────────── 書き手 (子) ───────────────────────────

/// 日誌。 1 行ずつ書く (process が落ちても page cache に残る)。
struct Journal {
    f: std::fs::File,
    seq: u64,
}

impl Journal {
    fn line(&mut self, s: &str) {
        writeln!(self.f, "{s}").unwrap();
    }
    /// 操作 `what` を書く前に 「これから」 を書き、 `f` の結果で 「済んだ / 失敗した」 を書く。
    fn op<T, E: std::fmt::Debug>(&mut self, what: &str, f: impl FnOnce() -> Result<T, E>) -> Option<T> {
        self.seq += 1;
        let seq = self.seq;
        self.line(&format!("op {seq} {what}"));
        match f() {
            Ok(t) => {
                self.line(&format!("done {seq}"));
                Some(t)
            }
            Err(e) => {
                self.line(&format!("fail {seq} {e:?}"));
                None
            }
        }
    }
    fn next_seq(&self) -> u64 {
        self.seq + 1
    }
}

fn write_child(kind: Kind, db: &str, journal: &Path) {
    let f = std::fs::OpenOptions::new().create(true).append(true).open(journal).unwrap();
    let mut j = Journal { f, seq: 0 };
    let Some(mut eng) = j.op("create", || Engine::create_with_capacity(db, 4096)) else { return };
    j.line("created");
    let schema = (|| -> Result<(), String> {
        eng.define_table("t", 1000)?;
        for (h, vt) in HIMOS {
            eng.define_himo_in("t", h, vt, 0)?;
        }
        Ok(())
    })();
    if let Err(e) = schema {
        j.line(&format!("schema_failed {e}"));
        return;
    }
    j.line("schema");
    if eng.flush().is_ok() && eng.persist_tables().is_ok() {
        j.line("ack");
    }
    match kind {
        Kind::Flush => write_flush(eng, db, &mut j),
        Kind::Oplog => {
            let Some(eng) = j.op("concurrentize", || Engine::concurrentize_with_oplog(eng, OPLOG_CAP)) else { return };
            write_oplog(eng, db, &mut j);
        }
    }
}

fn hid(eng: &Engine, h: &str) -> Option<u16> {
    eng.himo_id(&format!("t.{h}")).map(|i| i as u16)
}

/// cell に値を書く (Number は try_tie、 文字列は try_tie_text。 結果の返る形を使う: `tie_*` / `tie_*_async` は書けなかった
/// 時に結果を捨てて fault を数えるだけなので、 日誌に 「失敗」 を残せない)。 oplog の形でも同じ (WAL にも積む)。
fn set(eng: &Engine, j: &mut Journal, e: u64, h: &str) {
    let seq = j.next_seq();
    let v = val_for(seq, h);
    let what = format!("set {e} {h} {}", enc(&v));
    // 定義の返った列が引けない (開き直した後) なら書かない: 日誌に残し、 確かめる側が 「書き出しの返った列が無い」 と見る
    let Some(id) = hid(eng, h) else {
        j.line(&format!("missing_himo {h}"));
        return;
    };
    match v {
        Val::N(n) => j.op(&what, || eng.try_tie_to_by_id(e, id, n)),
        Val::S(s) => j.op(&what, || eng.try_tie_text_to_by_id(e, id, &s)),
    };
}

/// 結果を返さない操作 (untie / delete): 断られたかは fault の数で見る (`tie_*` と同じ約束 — 書き込みを断った時は fault を
/// 数える)。
fn fault_checked(eng: &Engine, f: impl FnOnce()) -> Result<(), String> {
    let before = eng.fault_total();
    f();
    if eng.fault_total() > before { Err(format!("fault (total {before} → {})", eng.fault_total())) } else { Ok(()) }
}

fn new_entity(eng: &Engine, j: &mut Journal) -> Option<u64> {
    let seq = j.next_seq();
    let e = eng.entity_in("t");
    // eid は返った後にしか分からないので、 「これから」 の行には書かない (作りかけの entity は値が無いので見ない)
    j.line(&format!("op {seq} new"));
    j.seq = seq;
    match e {
        Ok(e) => {
            j.line(&format!("done {seq} {e}"));
            Some(e)
        }
        Err(err) => {
            j.line(&format!("fail {seq} {err}"));
            None
        }
    }
}

fn write_flush(mut eng: Engine, db: &str, j: &mut Journal) {
    let flush = |eng: &mut Engine, j: &mut Journal| {
        if eng.flush().is_ok() && eng.persist_tables().is_ok() {
            j.line("ack");
        }
    };
    // batch 1: entity 4 つに 3 列
    let mut es = Vec::new();
    for _ in 0..4 {
        if let Some(e) = new_entity(&eng, j) {
            for (h, _) in HIMOS {
                set(&eng, j, e, h);
            }
            es.push(e);
        }
    }
    flush(&mut eng, j);
    // batch 2: 列を足す (segment を作る)、 新しい entity、 書き直し、 untie
    let m_ok = j.op("himo m", || eng.define_himo_in("t", "m", ValueType::Number, 0)).is_some();
    for _ in 0..2 {
        if let Some(e) = new_entity(&eng, j) {
            for (h, _) in HIMOS {
                set(&eng, j, e, h);
            }
            if m_ok {
                set(&eng, j, e, "m");
            }
            es.push(e);
        }
    }
    for (i, h) in ["n", "g", "l"].iter().enumerate() {
        if let Some(&e) = es.get(i) {
            set(&eng, j, e, h);
        }
    }
    if let Some(&e) = es.get(3) {
        j.op(&format!("unset {e} g"), || fault_checked(&eng, || eng.untie(e, "t.g")));
    }
    flush(&mut eng, j);
    // 閉じて開き直す (開く時の I/O: header・manifest・sidecar の読み込み)
    drop(eng);
    let Some(mut eng) = j.op("reopen", || Engine::open_standalone(db)) else { return };
    // batch 3: delete、 新しい entity (eid を使い回しうる)、 封緘 (flush + .crc)
    if let Some(&e) = es.get(1) {
        j.op(&format!("del {e}"), || fault_checked(&eng, || eng.delete(e)));
    }
    if let Some(e) = new_entity(&eng, j) {
        for (h, _) in HIMOS {
            set(&eng, j, e, h);
        }
    }
    if eng.seal_integrity().is_ok() && eng.persist_tables().is_ok() {
        j.line("ack");
    }
    drop(eng);
    j.line("dropped");
}

fn write_oplog(eng: std::sync::Arc<Engine>, db: &str, j: &mut Journal) {
    let sync = |eng: &Engine, j: &mut Journal| {
        if eng.oplog_sync().is_ok() {
            j.line("ack");
        }
    };
    let mut es = Vec::new();
    for _ in 0..4 {
        if let Some(e) = new_entity(&eng, j) {
            for (h, _) in HIMOS {
                set(&eng, j, e, h);
            }
            es.push(e);
        }
    }
    eng.oplog_commit();
    sync(&eng, j);
    for (i, h) in ["n", "g", "l"].iter().enumerate() {
        if let Some(&e) = es.get(i) {
            set(&eng, j, e, h);
        }
    }
    if let Some(&e) = es.get(3) {
        j.op(&format!("unset {e} g"), || fault_checked(&eng, || eng.untie_async(e, "t.g")));
    }
    eng.oplog_commit();
    sync(&eng, j);
    // 閉じて開き直す (開く時の I/O と oplog の再生)
    drop(eng);
    let Some(eng) = j.op("reopen", || Engine::open_concurrent_with_oplog(db, OPLOG_CAP)) else { return };
    if let Some(&e) = es.get(1) {
        j.op(&format!("del {e}"), || fault_checked(&eng, || eng.delete_async(e)));
    }
    if let Some(e) = new_entity(&eng, j) {
        for (h, _) in HIMOS {
            set(&eng, j, e, h);
        }
    }
    eng.oplog_commit();
    sync(&eng, j);
    j.line("dropped");
}

fn enc(v: &Val) -> String {
    match v {
        Val::N(n) => format!("n:{n}"),
        Val::S(s) => format!("s:{s}"),
    }
}

fn dec(s: &str) -> Val {
    match s.split_once(':') {
        Some(("n", n)) => Val::N(n.parse().unwrap()),
        Some(("s", t)) => Val::S(t.to_string()),
        _ => panic!("bad value {s}"),
    }
}

// ─────────────────────────── 日誌から組み立てる ───────────────────────────

#[derive(Clone, Debug)]
enum Op {
    Set(u64, String, Val),
    Unset(u64, String),
    Del(u64),
    New(u64),
    Himo,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    Done,
    Failed,
    /// 「これから」 だけ (その操作の途中で落ちた)
    InFlight,
}

#[derive(Default, Debug)]
struct Model {
    created: bool,
    schema_acked: bool,
    /// (操作, 状態, その時点で書き出しが返っていたか)
    ops: Vec<(Op, Status)>,
    /// 書き出しが返った所 (ops の長さ)
    acked_at: usize,
    m_defined_before_ack: bool,
}

fn read_model(journal: &Path) -> Model {
    let text = std::fs::read_to_string(journal).unwrap_or_default();
    let mut m = Model::default();
    let mut idx: BTreeMap<u64, usize> = BTreeMap::new();
    let mut schema = false;
    for line in text.lines() {
        let w: Vec<&str> = line.split(' ').collect();
        match w[0] {
            "created" => m.created = true,
            "schema" => schema = true,
            "ack" => {
                m.acked_at = m.ops.len();
                if schema {
                    m.schema_acked = true;
                }
                if m.ops.iter().any(|(o, s)| matches!(o, Op::Himo) && *s == Status::Done) {
                    m.m_defined_before_ack = true;
                }
            }
            "op" => {
                let seq: u64 = w[1].parse().unwrap();
                let op = match w.get(2).copied() {
                    Some("set") => Op::Set(w[3].parse().unwrap(), w[4].to_string(), dec(&w[5..].join(" "))),
                    Some("unset") => Op::Unset(w[3].parse().unwrap(), w[4].to_string()),
                    Some("del") => Op::Del(w[3].parse().unwrap()),
                    Some("new") => Op::New(u64::MAX),
                    Some("himo") => Op::Himo,
                    _ => Op::Other,
                };
                idx.insert(seq, m.ops.len());
                m.ops.push((op, Status::InFlight));
            }
            "done" => {
                let i = idx[&w[1].parse::<u64>().unwrap()];
                m.ops[i].1 = Status::Done;
                if let (Op::New(e), Some(eid)) = (&mut m.ops[i].0, w.get(2)) {
                    *e = eid.parse().unwrap();
                }
            }
            "fail" => {
                let i = idx[&w[1].parse::<u64>().unwrap()];
                m.ops[i].1 = Status::Failed;
            }
            _ => {}
        }
    }
    m
}

const ALL_HIMOS: [&str; 4] = ["n", "g", "l", "m"];

/// cell ごとに、 開き直した後に在ってよい値 (None = 値が無い)。 書き出しが返った所までの最後の値と、 その後に書いた
/// (書きかけた) 値。
fn allowed(m: &Model) -> BTreeMap<(u64, String), BTreeSet<Option<Val>>> {
    let mut cur: BTreeMap<(u64, String), Option<Val>> = BTreeMap::new();
    let mut later: BTreeMap<(u64, String), BTreeSet<Option<Val>>> = BTreeMap::new();
    for (i, (op, st)) in m.ops.iter().enumerate() {
        if *st == Status::Failed {
            continue;
        }
        let mut effects: Vec<((u64, String), Option<Val>)> = Vec::new();
        match op {
            Op::Set(e, h, v) => effects.push(((*e, h.clone()), Some(v.clone()))),
            Op::Unset(e, h) => effects.push(((*e, h.clone()), None)),
            Op::Del(e) | Op::New(e) if *e != u64::MAX => {
                effects.extend(ALL_HIMOS.iter().map(|h| ((*e, h.to_string()), None)));
            }
            _ => {}
        }
        for (k, v) in effects {
            if i < m.acked_at {
                cur.insert(k, v);
            } else {
                later.entry(k).or_default().insert(v);
            }
        }
    }
    let mut out: BTreeMap<(u64, String), BTreeSet<Option<Val>>> = BTreeMap::new();
    for k in cur.keys().chain(later.keys()) {
        let mut s = later.get(k).cloned().unwrap_or_default();
        s.insert(cur.get(k).cloned().flatten());
        out.insert(k.clone(), s);
    }
    out
}

// ─────────────────────────── 確かめる (子) ───────────────────────────

fn read_val(eng: &Engine, e: u64, h: &str) -> Option<Val> {
    let name = format!("t.{h}");
    eng.himo_id(&name)?;
    match h {
        "n" | "m" => eng.get(e, &name).map(Val::N),
        _ => eng.get_text_owned(e, &name).map(|b| Val::S(String::from_utf8_lossy(&b).into_owned())),
    }
}

fn verify_child(kind: Kind, db: &str, journal: &Path) -> String {
    let m = read_model(journal);
    let opened: std::io::Result<std::sync::Arc<Engine>> = match kind {
        Kind::Flush => Engine::open_standalone(db).map(std::sync::Arc::new),
        Kind::Oplog => Engine::open_concurrent_with_oplog(db, OPLOG_CAP),
    };
    let eng = match opened {
        Ok(e) => e,
        Err(e) if !m.created => {
            // 作り終える前に落ちた / 作るのが失敗した: 開けなくてよいが、 作り直せること
            return match Engine::create_with_capacity(db, 4096) {
                Ok(_) => String::new(),
                Err(e2) => format!("作り終える前の DB を開けない ({e}) うえに作り直せない: {e2}"),
            };
        }
        Err(e) => return format!("開けない: {e}"),
    };
    let mut errs = Vec::new();
    if m.schema_acked {
        if !eng.list_tables().iter().any(|t| t.1 == "t") {
            errs.push("書き出しの返った table t が無い".to_string());
        }
        for (h, _) in HIMOS {
            if eng.himo_id(&format!("t.{h}")).is_none() {
                errs.push(format!("書き出しの返った列 t.{h} が無い"));
            }
        }
    }
    if m.m_defined_before_ack && eng.himo_id("t.m").is_none() {
        errs.push("書き出しの返った列 t.m が無い".to_string());
    }
    for ((e, h), ok) in allowed(&m) {
        let got = read_val(&eng, e, &h);
        if !ok.contains(&got) {
            errs.push(format!("entity {e} の {h} = {got:?}、 在ってよいのは {ok:?}"));
        }
    }
    // 開き直した DB に書いて書き出せる
    if m.schema_acked {
        let smoke = (|| -> Result<(), String> {
            let e = eng.entity_in("t")?;
            let n = hid(&eng, "n").ok_or("t.n が無い")?;
            eng.try_tie_to_by_id(e, n, 424242u32).map_err(|e| format!("{e:?}"))?;
            match kind {
                Kind::Flush => {
                    let mut eng = std::sync::Arc::try_unwrap(eng).map_err(|_| "Arc".to_string())?;
                    eng.flush().map_err(|e| e.to_string())
                }
                Kind::Oplog => eng.oplog_sync().map_err(|e| e.to_string()),
            }
        })();
        if let Err(e) = smoke {
            errs.push(format!("開き直した DB に書けない: {e}"));
        }
    }
    errs.join("; ")
}

#[test]
fn fault_matrix_child() {
    let Ok(role) = std::env::var(ROLE_ENV) else { return };
    let kind = Kind::parse(&std::env::var(KIND_ENV).unwrap());
    let db = std::env::var(DB_ENV).unwrap();
    let journal = PathBuf::from(std::env::var(JOURNAL_ENV).unwrap());
    match role.as_str() {
        "write" => {
            if std::env::var("ENCHU_FAULT_MODE").as_deref() == Ok("lost") {
                enchudb_oplog::crashsim::start();
            }
            write_child(kind, &db, &journal);
            println!("FM_POINTS {}", enchudb_oplog::fault::count());
        }
        "verify" => {
            let r = verify_child(kind, &db, &journal);
            if r.is_empty() {
                println!("FM_RESULT ok");
            } else {
                println!("FM_RESULT fail {r}");
            }
        }
        r => panic!("unknown role {r}"),
    }
}

// ─────────────────────────── 親 ───────────────────────────

/// 今わかっている失敗 (issue ごと)。 当たった失敗は 「既知」 として数えて出し、 試験は落とさない。 表に無い失敗だけ落とす。
/// 直す PR が自分の行を外す。
struct Known {
    /// None = flush / oplog のどちらも
    kind: Option<Kind>,
    /// None = error / exit / lost のどれも
    mode: Option<&'static str>,
    /// 当たった点の名前の一部
    site: &'static str,
    /// 失敗の文の一部
    needle: &'static str,
    issue: &'static str,
    /// macOS と Linux の両方で当たるのを確かめた (点の数は page の大きさなどで違い、 選ぶ N がずれる)。 true の行は、
    /// flush の形で当たらなかったら落とす (直ったなら外す / 確かめる側の見落とし)
    everywhere: bool,
}

const KNOWN: &[Known] = &[
    // 開く時の sidecar の読み込みの EIO で table の定義を捨てて開き、 次の保存で上書きする
    Known { kind: None, mode: Some("error"), site: "sidecar.read_tables", needle: "table t が無い", issue: "#533", everywhere: true },
    // 伸ばす mmap の失敗で file だけ伸び、 封緘した .crc が合わない (macOS で当たる)
    Known { kind: Some(Kind::Flush), mode: Some("error"), site: "segment.remap", needle: "region CRC mismatch", issue: "#535", everywhere: false },
    // .crc をその場で上書きする (書きかけが残る)。 crashsim の hook も無い (電源断の像では一度も書き出していない扱い)
    Known { kind: Some(Kind::Flush), mode: None, site: "crc.", needle: "failed to fill whole buffer", issue: "#536", everywhere: true },
    Known { kind: Some(Kind::Flush), mode: Some("lost"), site: "", needle: "bad .crc magic", issue: "#536", everywhere: true },
    // 列の定義が紐名の辞書を伸ばせなくても成功を返し、 開き直すと列が引けない (Linux = page 4 KiB で当たる)
    Known { kind: None, mode: Some("error"), site: "segment.", needle: "書き出しの返った列 t.", issue: "#537", everywhere: false },
];

fn known(kind: Kind, mode: &str, site: &str, msg: &str) -> Option<usize> {
    KNOWN.iter().position(|k| {
        k.kind.is_none_or(|x| x == kind) && k.mode.is_none_or(|m| m == mode) && site.contains(k.site) && msg.contains(k.needle)
    })
}

fn scratch(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("enchu_fault_matrix_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn child(envs: &[(&str, String)]) -> std::process::Output {
    let mut c = Command::new(std::env::current_exe().unwrap());
    c.args(["fault_matrix_child", "--exact", "--nocapture", "--test-threads=1", "-q"]);
    for v in [ROLE_ENV, KIND_ENV, DB_ENV, JOURNAL_ENV, "ENCHU_FAULT_AT", "ENCHU_FAULT_MODE", "ENCHU_FAULT_ROOT", "ENCHU_FAULT_OUT", "ENCHU_FAULT_TRACE"] {
        c.env_remove(v);
    }
    for (k, v) in envs {
        c.env(k, v);
    }
    c.output().expect("spawn child")
}

/// 書き手を 1 度、 数えるだけで回して、 通った点の名前を順に返す。
fn trace(kind: Kind, root: &Path) -> Vec<String> {
    let dir = root.join("count");
    std::fs::create_dir_all(&dir).unwrap();
    let tr = dir.join("trace");
    let out = child(&[
        (ROLE_ENV, "write".into()),
        (KIND_ENV, kind.name().into()),
        (DB_ENV, dir.join("db").to_str().unwrap().into()),
        (JOURNAL_ENV, dir.join("journal").to_str().unwrap().into()),
        ("ENCHU_FAULT_TRACE", tr.to_str().unwrap().into()),
    ]);
    assert!(out.status.success(), "数える回の書き手が落ちた: {}", String::from_utf8_lossy(&out.stderr));
    let sites: Vec<String> =
        std::fs::read_to_string(&tr).unwrap().lines().map(|l| l.split('\t').nth(1).unwrap().to_string()).collect();
    let _ = std::fs::remove_dir_all(&dir);
    sites
}

/// 点の名前ごとに、 通った回の番号 (1 始まり) を `PER_SITE` まで選ぶ (最初・最後と間を均等に)。
fn pick(sites: &[String]) -> Vec<(u64, String)> {
    let mut by: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    for (i, s) in sites.iter().enumerate() {
        by.entry(s).or_default().push(i as u64 + 1);
    }
    let mut out = Vec::new();
    for (s, ns) in by {
        let chosen: BTreeSet<usize> = if ns.len() <= PER_SITE {
            (0..ns.len()).collect()
        } else {
            (0..PER_SITE).map(|k| k * (ns.len() - 1) / (PER_SITE - 1)).collect()
        };
        out.extend(chosen.into_iter().map(|k| (ns[k], s.to_string())));
    }
    out.sort();
    out
}

fn run(kind: Kind, mode: &str) {
    let root = scratch(&format!("{}_{mode}", kind.name()));
    let sites = trace(kind, &root);
    let picks = pick(&sites);
    let mut failures: Vec<(String, String, String)> = Vec::new();
    let mut fired = 0usize;
    for (n, site) in &picks {
        let dir = root.join(format!("n{n}"));
        let live = dir.join("live");
        std::fs::create_dir_all(&live).unwrap();
        let db = live.join("db");
        let img = dir.join("img");
        let journal = dir.join("journal");
        let out = child(&[
            (ROLE_ENV, "write".into()),
            (KIND_ENV, kind.name().into()),
            (DB_ENV, db.to_str().unwrap().into()),
            (JOURNAL_ENV, journal.to_str().unwrap().into()),
            ("ENCHU_FAULT_AT", n.to_string()),
            ("ENCHU_FAULT_MODE", mode.into()),
            ("ENCHU_FAULT_ROOT", live.to_str().unwrap().into()),
            ("ENCHU_FAULT_OUT", img.to_str().unwrap().into()),
        ]);
        let stderr = String::from_utf8_lossy(&out.stderr);
        let hit = stderr.lines().find(|l| l.starts_with("ENCHU_FAULT ")).map(|l| l.to_string());
        if hit.is_some() {
            fired += 1;
        }
        let label = format!("#{n} {site} (当たった点: {})", hit.as_deref().unwrap_or("無し"));
        // 当たった点の名前 (背景の thread で選んだ点とずれることがある)
        let hit_site = hit.as_deref().and_then(|l| l.rsplit(' ').next()).unwrap_or(site).to_string();
        let code = out.status.code();
        let wrote_ok = match mode {
            "error" => code == Some(0),
            _ => code == Some(enchudb_oplog::fault::EXIT_CODE) || (code == Some(0) && hit.is_none()),
        };
        if !wrote_ok {
            let tail: Vec<&str> = stderr.lines().filter(|l| !l.trim().is_empty()).rev().take(6).collect();
            failures.push((label, hit_site, format!("書き手が {:?} で終わった: {}", out.status, tail.into_iter().rev().collect::<Vec<_>>().join(" | "))));
            let _ = std::fs::remove_dir_all(&dir);
            continue;
        }
        let target = if mode == "lost" && img.exists() { img.join("db") } else { db.clone() };
        let v = child(&[
            (ROLE_ENV, "verify".into()),
            (KIND_ENV, kind.name().into()),
            (DB_ENV, target.to_str().unwrap().into()),
            (JOURNAL_ENV, journal.to_str().unwrap().into()),
        ]);
        let vout = String::from_utf8_lossy(&v.stdout);
        match vout.lines().find(|l| l.starts_with("FM_RESULT ")) {
            Some("FM_RESULT ok") => {}
            Some(l) => failures.push((label, hit_site, l["FM_RESULT fail ".len().min(l.len())..].to_string())),
            None => {
                let err = String::from_utf8_lossy(&v.stderr).into_owned();
                let tail: Vec<&str> = err.lines().filter(|l| !l.trim().is_empty()).rev().take(6).collect();
                failures.push((label, hit_site, format!("確かめる子が {:?} で終わった: {}", v.status, tail.into_iter().rev().collect::<Vec<_>>().join(" | "))));
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
    let _ = std::fs::remove_dir_all(&root);
    let mut used = vec![0usize; KNOWN.len()];
    let mut unknown = Vec::new();
    for (label, site, msg) in &failures {
        match known(kind, mode, site, msg) {
            Some(i) => {
                used[i] += 1;
                eprintln!("  既知 ({}): {label}: {msg}", KNOWN[i].issue);
            }
            None => unknown.push(format!("{label}: {msg}")),
        }
    }
    let distinct: BTreeSet<&String> = sites.iter().collect();
    eprintln!(
        "[{} {mode}] 点 {} 回 ({} 種類) のうち {} 回で起こした (当たった {fired}) → 失敗 {} (既知 {})",
        kind.name(),
        sites.len(),
        distinct.len(),
        picks.len(),
        failures.len(),
        failures.len() - unknown.len()
    );
    assert!(fired * 10 >= picks.len() * 9, "選んだ点の 1 割より多くで何も起きなかった ({fired} / {})", picks.len());
    assert!(unknown.is_empty(), "[{} {mode}] 表に無い失敗 {} 件:\n{}", kind.name(), unknown.len(), unknown.join("\n"));
    // flush の形は背景の thread が無く、 同じ N は毎回同じ点に当たる: 表の失敗が起きなくなったら、 直った (表から外す) か
    // 試験が空振りしている (確かめる側が見落としている)。 oplog の形は当たる点が回によって違いうるので見ない
    if kind == Kind::Flush {
        let stale: Vec<String> = KNOWN
            .iter()
            .zip(&used)
            .filter(|(k, n)| **n == 0 && k.everywhere && k.kind.is_none_or(|x| x == kind) && k.mode.is_none_or(|m| m == mode))
            .map(|(k, _)| format!("{} {} 「{}」", k.issue, k.site, k.needle))
            .collect();
        assert!(stale.is_empty(), "[{} {mode}] 表の失敗が起きなかった (直ったなら表から外す): {}", kind.name(), stale.join(", "));
    }
}

/// 確かめる側の自己試験: 故障なしで書いた DB の、 書き出しの返った値を書き換える / 消す / 書き手が失敗を返した書き込みの値を
/// 入れると、 確かめる側が見つける (見落とすなら、 上の試験は何も見ていない)。
#[test]
fn verifier_catches_wrong_values() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let root = scratch("verifier");
    let (db, journal) = (root.join("db"), root.join("journal"));
    let db_s = db.to_str().unwrap().to_string();
    let w = child(&[
        (ROLE_ENV, "write".into()),
        (KIND_ENV, "flush".into()),
        (DB_ENV, db_s.clone()),
        (JOURNAL_ENV, journal.to_str().unwrap().into()),
    ]);
    assert!(w.status.success());
    let verify = || {
        let v = child(&[
            (ROLE_ENV, "verify".into()),
            (KIND_ENV, "flush".into()),
            (DB_ENV, db_s.clone()),
            (JOURNAL_ENV, journal.to_str().unwrap().into()),
        ]);
        String::from_utf8_lossy(&v.stdout).lines().find(|l| l.starts_with("FM_RESULT ")).unwrap_or("").to_string()
    };
    assert_eq!(verify(), "FM_RESULT ok", "対照: 故障なしの DB が通らない");
    let m = read_model(&journal);
    let e = m.ops.iter().find_map(|(o, st)| match (o, st) {
        (Op::Set(e, h, _), Status::Done) if h == "n" => Some(*e),
        _ => None,
    });
    let e = e.expect("値を書いた entity");
    // 開いて書き換えるたびに .crc を外す (封緘した DB を書き換えると開けない)
    let tamper = |f: &dyn Fn(&mut Engine)| {
        let _ = std::fs::remove_file(enchudb_engine::integrity::crc_path_for(&db_s));
        let mut eng = Engine::open_standalone(&db_s).unwrap();
        f(&mut eng);
        eng.flush().unwrap();
    };
    // 1. 書き出しの返った値を別の値に
    tamper(&|eng| eng.tie(e, "t.n", 999_999u32));
    let r = verify();
    assert!(r.starts_with("FM_RESULT fail") && r.contains(&format!("entity {e} の n")), "書き換えた値を見落とした: {r}");
    // 2. 書き出しの返った値を消す
    tamper(&|eng| eng.untie(e, "t.n"));
    let r = verify();
    assert!(r.starts_with("FM_RESULT fail") && r.contains("None"), "消した値を見落とした: {r}");
    // 3. 書き手が失敗を返した書き込みの値 (日誌に失敗の書き込みを足し、 その値を DB に入れる)
    let fail_val = 777_777u64;
    {
        let mut f = std::fs::OpenOptions::new().append(true).open(&journal).unwrap();
        writeln!(f, "op 99999 set {e} m n:{fail_val}").unwrap();
        writeln!(f, "fail 99999 injected").unwrap();
    }
    tamper(&|eng| eng.tie(e, "t.m", fail_val as u32));
    let r = verify();
    assert!(r.starts_with("FM_RESULT fail") && r.contains(&format!("entity {e} の m")), "失敗を返した書き込みの値を見落とした: {r}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn io_errors_at_every_point_flush() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    run(Kind::Flush, "error");
}

#[test]
fn io_errors_at_every_point_oplog() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    run(Kind::Oplog, "error");
}

#[test]
fn process_exit_at_every_point_flush() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    run(Kind::Flush, "exit");
}

#[test]
fn process_exit_at_every_point_oplog() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    run(Kind::Oplog, "exit");
}

#[test]
fn power_loss_at_every_point_flush() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    run(Kind::Flush, "lost");
}

#[test]
fn power_loss_at_every_point_oplog() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    run(Kind::Oplog, "lost");
}
