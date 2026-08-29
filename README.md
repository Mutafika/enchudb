# EnchuDB

**Embedded graph engine with multi-condition AND in nanoseconds.**

An embedded database built around a *himo* (紐, "cord")-based cylinder engine. Single-file mmap, and a LockFreeCylinder (value → eid buckets) makes the **lookup decision ns-scale** with lock-free concurrent reads. Returning results is memcpy-bound and proportional to result size (µs scale — the physical floor).

On top of it you can stack **schema / SQL / FFI / full-text search / RAG / P2P sync / transport**, all in one workspace.

## Why

- **Faster lookups than SQLite**: LockFreeCylinder resolves value → eid in ns, and multi-condition AND is just a bucket intersection.
- **Relations, unlike Rocks / LMDB**: himo + cylinder let you traverse between entities (ref columns + reverse bucket lookup).
- **Append-only oplog + HLC**: P2P sync (`enchudb-sync`) is built in, including partial sync (`SubscriptionFilter`).
- **Small memory footprint**: a single mmap file that leans on the OS page cache.

## Workspace

| Crate | Role | Opt-in from the meta crate |
|---|---|---|
| [`enchudb-oplog`](./crates/enchudb-oplog) | oplog wire primitives (Hlc / PeerId / EntityId + record). Shared primitive for engine / sync / transport | always |
| [`enchudb-engine`](./crates/enchudb-engine) | Core storage engine. Column + LockFreeCylinder + oplog | always |
| [`enchudb-schema`](./crates/enchudb-schema) | **Native API.** Virtual 2D tables + himo_id pre-resolution + persisted schema | always |
| [`enchudb-sync`](./crates/enchudb-sync) | HLC-LWW Syncer, ShardRouter, SubscriptionFilter | always |
| [`enchudb-transport`](./crates/enchudb-transport) | HTTP relay / WebSocket push hub | direct dep |
| [`enchudb-textsearch`](./crates/enchudb-textsearch) | Text-search policy on top of ngram. Candidate doc ids + `.contains()` verification for exact substring matching | direct dep |
| [`enchudb-ngram`](./crates/enchudb-ngram) | bigram inverted-index primitive (posting → intersect → candidate doc ids), mmap-persisted | direct dep |
| [`enchudb-rag`](./crates/enchudb-rag) | RAG store. Metadata filter first + brute-force cosine, no ANN | direct dep |
| [`enchudb-sql`](./crates/enchudb-sql) | SQLite-superset SQL frontend (CRUD + ORDER BY / LIMIT / ranges / IS NULL / INSERT OR REPLACE), **persisted schema** | `features = ["sql"]` |
| [`enchudb-ffi`](./crates/enchudb-ffi) | SQLite-style C ABI (12 functions), `cdylib + staticlib`; the base for calling from Python / Node / Swift | `features = ["ffi"]` |
| [`enchudb-cli`](./crates/enchudb-cli) | The `enchu` REPL. Drives the engine directly via `query_lang` syntax + dot commands (does **not** go through SQL) | `cargo install --path crates/enchudb-cli` |

Each sub-crate has its own `README.md` with details. Node.js bindings live in a separate repo, [`mutafika/enchu-extend`](https://github.com/mutafika/enchu-extend).

## Quick start

The recommended entry point is the **schema layer**. Declare virtual 2D tables and do CRUD; the schema is persisted inside the DB file, so no CREATE is needed on reopen.

```rust
use enchudb::schema::Database;

let mut db = Database::create("/tmp/app.db")?;

let users = db.table("users")
    .number("id")
    .tag("name")
    .number("age")
    .primary_key("id")
    .build()?;

let alice = users.insert()
    .set("id", 1i64).set("name", "Alice").set("age", 30i64)
    .commit()?;

let hits = users.where_eq("age", 30i64)
    .where_eq("name", "Alice")
    .find()?;
```

`build()` pre-resolves col → himo_id, so the hot-path string lookup disappears internally. "Drop to the engine for performance" is normally unnecessary (the schema layer has been zero-cost since v0.3.0).

For low-cardinality columns that you group / filter on (`dept` / `status` / categories), pass a hint of the distinct-value count via `.cardinality(n)`. Aggregations that use that column as the group key (`group_sum` / `group_min` / `group_max` / `histogram`) then take a dense, parallel fast path (without the hint they fall back to a HashMap, [#46](https://github.com/Mutafika/enchudb/issues/46)). It is a hint, not a cap — you can still tie values beyond it.

```rust
db.table("events")
    .number("id")
    .number("kind").cardinality(16)   // low-cardinality column you group / filter on
    .number("ts")
    .primary_key("id")
    .build()?;
```

reopen:

```rust
let db = Database::open("/tmp/app.db")?;
let users = db.get_table("users").unwrap();   // no CREATE needed, schema already restored
```

Apps that store lots of large text (`Leaf` himos — article bodies, tool output, long notes) and would overflow the default 512 MiB leaf region can size it explicitly with `Database::create_growable_with_leaf(path, max_entities, leaf_data_size)` ([#109](https://github.com/Mutafika/enchudb/issues/109)).

See [`crates/enchudb-schema/README.md`](./crates/enchudb-schema/README.md) for the full API.

### Engine layer (for graph ops / custom dispatch)

```rust
use enchudb::{Engine, ValueType};

let db = Engine::create_standalone("/tmp/my.db")?;
db.define_himo("age", ValueType::Number, 100);

let alice = db.entity().unwrap();
db.tie(alice, "age", 30);
db.tie_text(alice, "city", "Tokyo");

db.rebuild();
let result = db.query(&[("age", 30)]);
```

### SQL frontend (`features = ["sql"]`)

```rust
use enchudb_sql::{Database, Output};

let mut db = Database::create_growable_tiny("/tmp/notif.db")?;

db.execute("CREATE TABLE notif (key TEXT PRIMARY KEY, dismissed_at INTEGER)")?;
db.execute("INSERT OR REPLACE INTO notif VALUES ('uuid-abc', 1715174400)")?;

if let Output::Rows { rows, columns } = db.execute("
    SELECT key, dismissed_at FROM notif
    WHERE dismissed_at > 1715000000 AND dismissed_at IS NOT NULL
    ORDER BY dismissed_at DESC
    LIMIT 10
")? {
    // rows[i][j] = Value::Integer | Text | Null
}
drop(db);

let mut db = Database::open("/tmp/notif.db")?;
assert_eq!(db.list_tables().len(), 1);    // schema already restored
```

Non-SQL consumers can read the schema via `Database::list_tables()`.

### C ABI (`features = ["ffi"]`)

```bash
cargo build --release -p enchudb-ffi
# → target/release/libenchudb_ffi.{dylib,so,a}
```

```c
#include "enchudb.h"
enchudb_db* db;
enchudb_open("/tmp/x.db", &db);
enchudb_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

enchudb_result* r;
enchudb_query(db, "SELECT * FROM t", &r);
for (size_t i = 0; i < enchudb_result_rows(r); i++) {
    int64_t id = enchudb_result_int(r, i, 0);
    const char* name = enchudb_result_text(r, i, 1);
}
enchudb_result_free(r);
enchudb_close(db);
```

Header: `crates/enchudb-ffi/include/enchudb.h`; demo: `crates/enchudb-ffi/examples/demo.c`.

### CLI (`enchu`)

A `sqlite3`-style REPL that drives the engine directly. It speaks `query_lang` syntax (`age:30 city:"Tokyo" | group dept | sum salary`) and dot commands (`.himos` / `.entity <eid>` / `.dump` …). It does **not** go through SQL.

```bash
cargo install --path crates/enchudb-cli
# the binary is named `enchu`

enchu --create --tiny /tmp/state.db          # new DB (1024-row preset)
enchu /tmp/state.db                          # enter the REPL
enchu /tmp/state.db -e 'age:30 | count'      # one-shot
enchu --readonly /tmp/state.db               # read-only open
```

A REPL session:

```
enchu> .define name tag
defined name (tag, max_values=0)
enchu> .define age num
defined age (num, max_values=0)
enchu> + name:"alice" age:30
+0
enchu> + name:"bob" age:25
+1
enchu> age:30
0
enchu> age:30 | count
1
enchu> .entity 0
eid=0
  name: "alice"
  age: 30
enchu> .quit
```

create presets: `--default` / `--compact` / `--growable` (default) / `--tiny`.

### Durability (oplog)

```rust
let db = Engine::create_concurrent_with_oplog("/tmp/my.db", 256 * 1024 * 1024)?;

let e = db.entity().unwrap();
db.tie_async(e, "age", 30);
db.oplog_sync()?;     // fsync + msync
// or oplog_commit() for a background fsync (Async mode)
```

### RAG (`enchudb-rag`)

```rust
use enchudb_rag::{RagStore, Chunk, Meta, Query, Filter};

let mut store = RagStore::builder()
    .path("./rag")
    .dim(384)
    .meta_value("tenant", 100)
    .meta_symbol("lang")
    .build()?;

let hits = store.search(
    Query::new(&query_vec)
        .filter(Filter::symbol("lang", "en").and(Filter::value("tenant", 3)))
        .top_k(10)
)?;
```

Sub-ms RAG at personal scale (~1M chunks) by filtering on metadata first. For measured numbers see [`crates/enchudb-rag/examples/`](./crates/enchudb-rag/examples).

## Benchmarks

Reproduction commands, measured numbers, and hardware are collected in [`benches/README.md`](./benches/README.md). We keep inflated numbers out of the README — anyone curious can run them and verify.

```bash
cargo bench --bench core
cargo run --release --example vs_db
```

## Architecture

- **himo**: an attribute attached to an entity — the unit of indexing, named by any UTF-8 string.
- **Column**: the source of truth, mmap-persisted, `Column[himo][eid] = value`.
- **LockFreeCylinder**: per-value eid buckets, value → bucket in O(1), append-only + epoch for lock-free concurrent reads; a cylinder slice + a Column filter gives multi-condition AND.
- **oplog** (`enchudb-oplog`): crash consistency, background fsync, ring-buffer reuse.
- **Virtual 2D table layer** (`enchudb-schema`): bundles N himos under one table name, pre-resolves col → himo_id, and persists the schema inside the DB file. The SQL frontend and FFI sit on top of this.

Mental model: the whole engine is one big tangle of vines — the vines are himo, and an entity is the body hidden where vines cross. A single himo's contents form a 2D table (value × eid buckets).

Each sub-crate has its own `README.md` with details, and `docs/` holds architecture / concurrency / migration notes. For a quick term lookup, see [`docs/glossary.md`](docs/glossary.md) (a per-layer index plus disambiguation of easily-confused homonyms).

## File layout

Since 0.26.0 (format v10) **a database is a directory**: one file per region, so each
region grows independently and only what you write is materialised. Everything that
belongs to the DB lives inside it — `mv`, `rm -r` and `cp -r` move the whole thing.

```
{path}/                     the database (FILE_VERSION 10)
  header.seg                header: capacities, himo table, flags
  entities.seg              live bitmap + free stack
  vocab.{data,offsets,index}.seg
  himoreg.{data,offsets,index}.seg
  content.{index,data}.seg
  leaf.data.seg             LeafStore (variable-length text)
  himo/NNNN.seg             one Column per himo (id = NNNN, never reused)
  ver/NNNN.seg              per-cell HLC version column (syncing DBs only)
  tomb.seg                  tombstone column (syncing DBs only)
  oplog                     WAL (when enabled)
  tables                    table definitions (+ PK / extent blocks)
  eidmap, vocabmap          foreign-eid / vocab translation (only when syncing)
  crc                       region CRC (only when seal_integrity is used)
  schema                    enchudb-schema's table schema
  lock                      writer exclusion (flock on writer open, released on close)
<blob_root>/                BlobStore (separate directory, content-addressed)
```

Every segment file is mapped on top of a large private reservation, so its base
pointer never moves while it grows: a segment starts at 4 KB and is committed page by
page as you write. The per-cell version regions are created by `enable_sync_tables()`
**immediately** (they are separate files, so nothing has to be remapped) and a DB that
never syncs simply does not have them.

**Single-file databases (v8 / v9, up to 0.25.x) are not opened by this build.** Convert
them once, offline, with

```rust
enchudb::Engine::migrate_v9_to_v10("old.db", "new.db")?;   // new.db is a directory
```

which copies only the data extents (a 10 GB-apparent v8 store becomes a few MB on
disk) and moves the old `old.db.tables` / `.oplog` / `.schema` sidecars inside. The
packed single-file layout is still produced by `Engine::pack_dir` for transfer /
bootstrap and read back by `unpack_to_dir` / `from_bytes`.

## Disk space

Segment files are committed lazily: a fresh default-capacity store is ~6 MB in
`du -sh` (it was 26 GB *apparent* as a single file up to 0.25, 95 GB with sync), and
both apparent and physical size grow with what you actually write. On Windows the
segments are sparse files sized to their reservation, so `dir` still shows the reserved
size there. Three consequences remain worth knowing.

**1. A full disk is refused, not crashed — but only on a best-effort basis.** Writes go
through `mmap`, so when the kernel cannot allocate a block for a hole it raises
**SIGBUS** rather than returning `ENOSPC`: there is no write syscall for the errno to
come back from. Two mitigations are in place
([#167](https://github.com/Mutafika/enchudb/issues/167)):

- **The grow path checks free space first.** `ftruncate` never reports `ENOSPC` (it just
  extends sparsely), so before extending, the engine calls `statvfs` and requires
  *(bytes to commit + a 32 MB margin)* to be available. If they are not, it returns
  `io::ErrorKind::StorageFull` as a normal `Result`.
- **That result is no longer discarded.** When the commit cannot be extended, the write
  is **refused** rather than performed against an uncommitted page — counted as
  `FaultKind::DiskSpace` and reported through a rate-limited warning.

```rust
eng.disk_free_bytes();  // Option<u64> — growable backings only; watch this
eng.space_denials();    // grow refusals; non-zero means writes are being dropped
eng.fault_count(enchudb::FaultKind::DiskSpace);
```

This is deliberately best-effort: a write into a hole **inside an already-committed
range** does not pass through the check (the margin exists to absorb exactly that).
Reserving the whole file with `fallocate` would close the gap but defeats the sparse
design, so it is not done. `create` still succeeds regardless of free space (it only
does `set_len`), so the failure surfaces later, at write time. **Provision for the
apparent size, not the current physical usage** — "`df` says there is room" is not a
safety property here.

**2. Copying the DB.** Use `enchudb_engine::copy_db_dir` (a clone that opens: segments
plus sidecars, minus the lock) or `Engine::snapshot_export` (a consistent copy). Both
walk `SEEK_DATA` / `SEEK_HOLE` per file and copy only data. Note that **APFS fills any
hole smaller than 16 MB on write**, so `pack_dir` / `unpack_to_dir` punch the zero
ranges back out with `F_PUNCHHOLE` after writing; Linux keeps seek-created holes as is.

**3. External backup tools have the same problem.** `rsync` without `--sparse`, naive
`cp`, and apparent-size-based tools (Time Machine) will expand the file. Prefer
`snapshot_export`, or pass the sparse-aware flags. If the reserved size is a problem
(Windows), create with a smaller `max_entities` — since 0.26.0 the entity cap can be
raised later with `grow_entity_cap`, up to the reservation chosen at create time.

Note that `snapshot_export` does **not** fsync: the copy lands in the page cache, so a
power loss right after can lose it. That is deliberate (it keeps snapshots fast by not
re-persisting the source); fsync it yourself if the snapshot is a backup you rely on.

## Capacity limits

enchudb is embedded — it runs inside someone else's process — so "the DB is full" and
"that value does not fit" must not take the host down with them. Since 0.21.0+ these
never panic ([#59](https://github.com/Mutafika/enchudb/issues/59)): the write is
**refused**, counted per kind, and reported through a rate-limited warning. APIs that
can carry an error return one.

```rust
use enchudb::FaultKind;

eng.entity();                                 // Result — Err when the eid space is
                                              // exhausted, or when the anonymous table
                                              // is closed (entity_in is the one to use
                                              // then). Same shape as entity_in().
eng.fault_count(FaultKind::EntitySpace);      // refused: no eid slots left
                                              // (grow_entity_cap raises the cap; tables
                                              // auto-grow into free eid space first)
eng.fault_count(FaultKind::VocabSpace);       // refused: vocab_max_entries reached
eng.fault_count(FaultKind::ContentSpace);     // refused: content region full
eng.fault_count(FaultKind::ValueOutOfRange);  // refused: value == u32::MAX (sentinel)
eng.fault_count(FaultKind::DiskSpace);        // refused: cannot grow the file
eng.fault_total();
```

A non-zero count means writes were dropped on purpose. Watch it the way you would watch
a queue depth: the DB stays readable and usable, but it is telling you it could not
accept everything.

## Concurrency

A SQLite-WAL-style model: **one writer process + unlimited readers**.

| Goal | API | lock | concurrent processes |
|---|---|---|---|
| Write + read | `Engine::open_concurrent_with_oplog` / `Engine::open_standalone` | exclusive | 1 |
| Read only | `Engine::open_readonly` / `Database::open_readonly` | none | unlimited |

The writer holds `flock(LOCK_EX)` on `{path}/lock` for the engine's lifetime. A second writer blocks until the first is dropped (same as sqlite's default). Readonly opens take no lock, so they coexist with the writer; calling a write API on one panics so you notice immediately.

Reading variable-length text (`Leaf` / text himos) while a writer is live should go through `Engine::get_text_owned`, which returns an owned `Vec<u8>` via a per-slot gen-seqlock (the borrowing `get_text` is for single-threaded / quiesced access). This is what makes cross-process readonly reads of live text torn-read-safe ([#106](https://github.com/Mutafika/enchudb/issues/106) / [#113](https://github.com/Mutafika/enchudb/issues/113)).

For a GUI app + CLI sharing one DB, the recommended pattern is **the GUI opens `open_readonly` and the CLI opens as a writer subprocess**. See [`docs/concurrency.md`](./docs/concurrency.md).

### Writes between peers (sync)

Since 0.11, EnchuDB is **multi-writer**: any peer can write to any entity, and conflicts are resolved per card (himo) by HLC LWW (concurrent writes are settled by time). Convergence is **logical** (exchanging ops converges the contents) rather than physical replication. Refs that point at a replica of a foreign entity propagate too — the `TieRef` op carries the world id (peer-prefixed eid) since 0.22.0. Up to 0.10.x it was per-entity single-writer (only the author could write).

#### What a differential pull does *not* promise

A puller's cursor advances to the **max HLC it received, per author**. Two things follow, and both are contracts rather than bugs:

- **A publisher-side `SubscriptionFilter` makes the cursor scope-dependent.** Records the filter drops are skipped by the cursor, so widening a subscription later does **not** bring the past back through a differential pull — and no truncation is signalled either. Recover the widened range with `Syncer::bootstrap_pull_via`. `Syncer::suppressed_since(target)` shows what a publisher declined to send, per author; `suppressed_records()` is the running total (always 0 with the default `AllRecords`).
- **A state batch served by a *replica* can be incomplete at cell granularity**, not just row granularity — a relay cannot always map a cell back into the author's namespace. `Engine::state_records_dropped()` counts the cells it declined to serve (0 on the ordinary relay path), and the replica warns once when it serves such a batch.

Neither is silent any more, but neither is fixed by retrying: both recover through a direct bootstrap from the author.

## Testing

```bash
# unit + integration (~400 tests, ~1 min)
cargo test --workspace

# heavy scaling / stress tests (run manually)
cargo test --workspace -- --ignored

# bench
cargo bench --bench core
```

## Project status

Still 0.x. Not at SemVer 1.0 yet; breaking changes to the API / on-disk format are possible. Use in production at your own risk.

## License

Licensed under the [Functional Source License, Version 1.1, Apache 2.0 Future License](LICENSE.md) (FSL-1.1-Apache-2.0).

In short:

- You may **use, modify, and redistribute** EnchuDB for any purpose **other than offering it as a competing product or service**.
- **Each released version converts to Apache 2.0 two years after that version's release.** The project as a whole stays under FSL while continuously updated; only the specific past releases roll into Apache 2.0.

See [`LICENSE.md`](LICENSE.md) for the full text, and <https://fsl.software/> for background on the FSL.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you shall be licensed under FSL-1.1-Apache-2.0 as above, without any additional terms or conditions.
