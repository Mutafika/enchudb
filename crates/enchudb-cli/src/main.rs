//! enchu — EnchuDB CLI
//!
//! sqlite3 風の REPL で engine を直接叩く。 query 構文は `enchudb-engine`
//! の `query_lang` (`age:30 city:"東京" | group dept | sum salary` 等)、
//! メタ操作は dot command (`.himos`、 `.entity <eid>`、 `.dump` …)。
//! SQL は通さない (= enchudb-sql に依存しない)。

use std::io::{self, BufRead, IsTerminal, Write};
use std::process::ExitCode;

use enchudb_engine::engine::{Engine, EntityValue};
use enchudb_engine::himo_store::HimoType;
use enchudb_engine::query_lang;

const HELP: &str = "\
enchu — EnchuDB CLI (engine 直叩き、 SQL 非依存)

Usage:
  enchu <db>                       既存 DB を開いて REPL
  enchu <db> -e \"<query>\"          one-shot 実行
  enchu --create [<preset>] <db>   新規 DB 作成
  enchu --readonly <db>            read-only で開く (multi-process 安全)
  enchu --help

<preset>: --default | --compact | --growable | --tiny
          (省略時は --growable)

REPL では query_lang 構文をそのまま入力:
  age:30 city:\"東京\"                AND 検索
  age:20..30                         範囲
  age:30 | count                     集計 (pipe)
  age:30 | group dept | sum salary   GROUP BY
  + name:\"alice\" age:30              新規 entity
  ~ 42 age:31                        更新
  - 42                               削除

dot command:
  .help / .quit / .exit
  .himos                             定義済み himo 一覧
  .entities [LIMIT]                  entity ID 一覧 (default 50)
  .entity <eid>                      entity の全 himo を表示
  .define <name> <type> [<max>]      himo 追加 (tag|num|leaf|ref)
  .stats                             engine stats (count / WAL lag)
  .dump [LIMIT]                      全 entity を text dump
  .commit                            明示 commit
";

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match run(&argv) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("enchu: {e}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Debug, Default)]
struct Args {
    db_path: Option<String>,
    one_shot: Option<String>,
    mode: Mode,
}

#[derive(Debug, Default, Clone, Copy)]
enum Mode {
    #[default]
    Open,
    OpenReadonly,
    Create(Preset),
}

#[derive(Debug, Clone, Copy)]
enum Preset { Default, Compact, Growable, Tiny }

impl Default for Preset {
    fn default() -> Self { Preset::Growable }
}

fn parse_args(argv: &[String]) -> Result<Args, String> {
    let mut a = Args::default();
    let mut i = 0;
    let mut create_preset: Option<Preset> = None;
    let mut create_flag = false;
    while i < argv.len() {
        let s = argv[i].as_str();
        match s {
            "--help" | "-h" => { print!("{HELP}"); std::process::exit(0); }
            "--create" => { create_flag = true; }
            "--readonly" | "--ro" => { a.mode = Mode::OpenReadonly; }
            "--default" => { create_preset = Some(Preset::Default); }
            "--compact" => { create_preset = Some(Preset::Compact); }
            "--growable" => { create_preset = Some(Preset::Growable); }
            "--tiny" => { create_preset = Some(Preset::Tiny); }
            "-e" | "--exec" => {
                i += 1;
                let q = argv.get(i).ok_or("-e は引数が必要")?;
                a.one_shot = Some(q.clone());
            }
            _ if s.starts_with('-') => return Err(format!("unknown flag: {s}")),
            _ => {
                if a.db_path.is_some() {
                    return Err(format!("予期せぬ引数: {s}"));
                }
                a.db_path = Some(s.to_string());
            }
        }
        i += 1;
    }
    if create_flag {
        a.mode = Mode::Create(create_preset.unwrap_or_default());
    } else if create_preset.is_some() {
        return Err("preset flag は --create と併用".into());
    }
    Ok(a)
}

fn open_engine(args: &Args) -> io::Result<Engine> {
    let path = args.db_path.as_deref().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "db path required"))?;
    match args.mode {
        Mode::Open => Engine::open_standalone(path),
        Mode::OpenReadonly => Engine::open_readonly(path),
        Mode::Create(p) => match p {
            Preset::Default => Engine::create_standalone(path),
            Preset::Compact => Engine::create_compact(path),
            Preset::Growable => Engine::create_growable(path),
            Preset::Tiny => Engine::create_growable_tiny(path),
        }
    }
}

fn run(argv: &[String]) -> Result<(), String> {
    if argv.is_empty() {
        print!("{HELP}");
        return Ok(());
    }
    let args = parse_args(argv)?;
    if args.db_path.is_none() {
        return Err("db path required (see --help)".into());
    }
    let mut eng = open_engine(&args).map_err(|e| format!("open: {e}"))?;

    if let Some(q) = args.one_shot.as_deref() {
        run_line(&mut eng, q);
        eng.commit();
        return Ok(());
    }

    // --create だけで -e も標準入力 (tty / pipe) もない場合は作って終わる。
    // ただし pipe で SQL を流し込む使い方を壊さないため、 stdin が非 tty なら REPL に進む。
    if matches!(args.mode, Mode::Create(_)) && io::stdin().is_terminal() {
        if let Some(p) = args.db_path.as_deref() {
            eprintln!("created {p}");
        }
        eng.commit();
        return Ok(());
    }

    repl(&mut eng);
    eng.commit();
    Ok(())
}

fn repl(eng: &mut Engine) {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let interactive = stdin.is_terminal();
    if interactive {
        println!("enchu — type .help for commands, .quit to exit");
    }
    let mut handle = stdin.lock();
    let mut line = String::new();
    loop {
        if interactive {
            let mut out = stdout.lock();
            let _ = write!(out, "enchu> ");
            let _ = out.flush();
        }
        line.clear();
        match handle.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        }
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }
        if trimmed == ".quit" || trimmed == ".exit" {
            break;
        }
        if trimmed.starts_with('.') {
            if let Err(e) = run_dot(eng, trimmed) {
                eprintln!("! {e}");
            }
            continue;
        }
        run_line(eng, trimmed);
    }
}

fn run_line(eng: &mut Engine, line: &str) {
    let result = query_lang::execute(eng, line);
    println!("{result}");
}

fn run_dot(eng: &mut Engine, line: &str) -> Result<(), String> {
    let mut parts = line.split_whitespace();
    let cmd = parts.next().unwrap_or("");
    let rest: Vec<&str> = parts.collect();
    match cmd {
        ".help" => { print!("{HELP}"); Ok(()) }
        ".himos" => { cmd_himos(eng); Ok(()) }
        ".entities" => {
            let limit = parse_limit(rest.first().copied(), 50)?;
            cmd_entities(eng, limit);
            Ok(())
        }
        ".entity" => {
            let eid_str = rest.first().ok_or(".entity <eid> が必要")?;
            let eid: u64 = eid_str.parse().map_err(|_| format!("invalid eid: {eid_str}"))?;
            cmd_entity(eng, eid)
        }
        ".define" => cmd_define(eng, &rest),
        ".stats" => { cmd_stats(eng); Ok(()) }
        ".dump" => {
            let limit = parse_limit(rest.first().copied(), usize::MAX)?;
            cmd_dump(eng, limit);
            Ok(())
        }
        ".commit" => { eng.commit(); println!("ok"); Ok(()) }
        _ => Err(format!("unknown dot command: {cmd} (.help)")),
    }
}

fn parse_limit(s: Option<&str>, default: usize) -> Result<usize, String> {
    match s {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| format!("invalid limit: {v}")),
    }
}

fn cmd_himos(eng: &Engine) {
    let names = eng.himo_names();
    if names.is_empty() {
        println!("(no himos defined)");
        return;
    }
    println!("name\ttype");
    for n in names {
        let ty = eng.himo_type(n).map(type_label).unwrap_or("?");
        println!("{n}\t{ty}");
    }
}

fn type_label(t: HimoType) -> &'static str {
    match t {
        HimoType::Tag => "tag",
        HimoType::Number => "num",
        HimoType::Leaf => "leaf",
        HimoType::Ref => "ref",
    }
}

fn parse_type(s: &str) -> Result<HimoType, String> {
    match s {
        "tag" => Ok(HimoType::Tag),
        "num" | "number" => Ok(HimoType::Number),
        "leaf" => Ok(HimoType::Leaf),
        "ref" => Ok(HimoType::Ref),
        _ => Err(format!("unknown type: {s} (tag|num|leaf|ref)")),
    }
}

fn cmd_entities(eng: &Engine, limit: usize) {
    let eids = eng.entities();
    let n = eids.len().min(limit);
    for eid in eids.iter().take(n) {
        println!("{eid}");
    }
    if eids.len() > n {
        println!("... ({} more, total {})", eids.len() - n, eids.len());
    }
}

fn cmd_entity(eng: &Engine, eid: u64) -> Result<(), String> {
    let fields = eng.get_entity(eid);
    if fields.is_empty() {
        return Err(format!("entity {eid} has no fields (or does not exist)"));
    }
    println!("eid={eid}");
    for (name, val) in fields {
        match val {
            EntityValue::Num(n) => println!("  {name}: {n}"),
            EntityValue::Text(b) => match std::str::from_utf8(b) {
                Ok(s) => println!("  {name}: \"{s}\""),
                Err(_) => println!("  {name}: <{} bytes>", b.len()),
            },
            EntityValue::Content(b) => println!("  {name}: <content {} bytes>", b.len()),
        }
    }
    Ok(())
}

fn cmd_define(eng: &mut Engine, rest: &[&str]) -> Result<(), String> {
    let name = rest.first().ok_or(".define <name> <type> [<max_values>] が必要")?;
    let ty_s = rest.get(1).ok_or(".define <name> <type> [<max_values>] が必要")?;
    let ty = parse_type(ty_s)?;
    let max_values: u32 = match rest.get(2) {
        Some(v) => v.parse().map_err(|_| format!("invalid max_values: {v}"))?,
        None => 0,
    };
    eng.define_himo(name, ty, max_values);
    println!("defined {name} ({}, max_values={max_values})", type_label(ty));
    Ok(())
}

fn cmd_stats(eng: &Engine) {
    let s = eng.stats();
    println!("entity_count   {}", s.entity_count);
    println!("himo_count     {}", s.himo_count);
    println!("oplog_head       {}", s.oplog_head);
    println!("oplog_checkpoint {}", s.oplog_checkpoint);
    println!("oplog_lag_bytes  {}", s.oplog_lag_bytes);
    println!("oplog_capacity   {}", s.oplog_capacity);
    println!("next_lsn       {}", s.oplog_next_lsn);
    println!("durable_lsn    {}", s.durable_lsn);
    println!("queue_len      {}", s.queue_len);
    println!("pushed/applied {}/{}", s.pushed, s.applied);
    println!("peer_id        {}", s.peer_id);
}

fn cmd_dump(eng: &Engine, limit: usize) {
    let eids = eng.entities();
    let n = eids.len().min(limit);
    for eid in eids.iter().take(n) {
        let fields = eng.get_entity(*eid);
        if fields.is_empty() { continue; }
        print!("{eid}:");
        for (name, val) in fields {
            match val {
                EntityValue::Num(v) => print!(" {name}={v}"),
                EntityValue::Text(b) => match std::str::from_utf8(b) {
                    Ok(s) => print!(" {name}=\"{s}\""),
                    Err(_) => print!(" {name}=<{}b>", b.len()),
                },
                EntityValue::Content(b) => print!(" {name}=<content {}b>", b.len()),
            }
        }
        println!();
    }
    if eids.len() > n {
        println!("... ({} more)", eids.len() - n);
    }
}
