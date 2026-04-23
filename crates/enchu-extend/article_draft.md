# I built a search engine that's 130x faster than Elasticsearch. No server required.

I needed fast filtering for a multi-tenant SaaS. Tried Elasticsearch, Meilisearch — both were overkill. So I built a local engine that sits on top of PostgreSQL.

## The numbers

1 million products. 100 tenants × 10,000 products each. Filter by tenant, category, color, size.

### vs Elasticsearch 8.17 (pure query time, no HTTP overhead)

Measured with ES `profile` API — this is the fairest possible comparison:

| Query | Enchu | Elasticsearch | Faster |
|---|---|---|---|
| 1 condition (tenant=42) | 2.3µs | 95µs | **41x** |
| 2 conditions (tenant+category) | 760ns | 320µs | **421x** |
| 4 conditions (+color, +size) | 19.7µs | 196µs | **10x** |

2-condition queries hit the pair table — a precomputed O(1) lookup that returns results in 760 nanoseconds.

### vs Meilisearch v1.13

| Query | Enchu | Meilisearch | Faster |
|---|---|---|---|
| 1 condition | 2.3µs | 5.9ms | **2,565x** |
| 2 conditions | 760ns | 3.1ms | **4,079x** |
| 4 conditions | 19.7µs | 274µs | **14x** |

### At 100 million rows

Same machine. 100M products, 1,000 tenants.

| Query | Hits | Enchu |
|---|---|---|
| 1 condition (tenant=42) | 100K | **6µs** |
| 2 conditions (tenant+category) | 5K | **101µs** |
| 3 conditions (+color) | ~500 | **103µs** |
| 1 condition (color=red) | 10M | **597µs** |

Rebuild time for all indexes: 13.7 seconds. Still microsecond queries at scale.

## What it is

**Enchu Extend** is a transparent cache that sits on top of your PostgreSQL. No separate server. No configuration. No schema definition.

```ts
import { enchu } from 'enchu-extend'

const db = await enchu('postgresql://user:pass@localhost:5432/mydb')

// That's it. Schema auto-detected from PG, data auto-loaded.

// Filter
const results = db.products.filter({ color: 'red', size: 'M' })
// → [{ _id: 0, color: 'red', size: 'M', price: 2000, ... }, ...]

// Count — microseconds, not milliseconds
db.products.count({ category: 'shirt' })  // → 42

// Pagination
db.products.filter({ color: 'red' }, { limit: 20, offset: 40 })
```

Three lines to set up. Your app keeps writing to PostgreSQL. Enchu handles the reads.

## Why not just use Elasticsearch?

| | Enchu Extend | Elasticsearch |
|---|---|---|
| Setup | `npm install` + 3 lines | JVM + cluster config + mappings |
| Infrastructure | None (in-process) | Separate server, 512MB+ RAM |
| Latency | 760ns–20µs | 95–320µs (query) + 600µs (HTTP) |
| Data sync | Auto from PG, 1 second | You build the pipeline |
| Schema changes | Automatic | Reindex |

Elasticsearch is great when you need full-text search, fuzzy matching, or aggregations across distributed clusters. But for filtered queries on structured data? It's a lot of infrastructure for something that should be instant.

## Dynamic queries

Conditions are just `Record<string, string | number>`. Build them however you want:

```ts
// From user input
const conditions: Record<string, string | number> = {}
if (req.query.color) conditions.color = req.query.color
if (req.query.size) conditions.size = req.query.size
db.products.filter(conditions)

// From variables
const key = 'color'
db.products.filter({ [key]: 'red' })
```

## Your data stays local

Algolia and Meilisearch Cloud require you to send data to their servers. That's a problem for healthcare, finance, government — any company that cares about data privacy.

Enchu runs in your process. Your data never leaves your machine.

## Live sync

Enchu auto-syncs from PostgreSQL every second. When your app writes to Postgres, Enchu picks up the changes via `updated_at` polling. If the connection drops, it auto-reconnects.

```ts
const db = await enchu(pgConn, { syncInterval: 1000 })  // default
```

Search performance doesn't degrade during sync.

## How it works

```
PostgreSQL  ──(1s polling)──▶  Enchu Extend (mmap cache)
                                     │
                              Pair Table (O(1) 2-condition)
                              + Cylinder Index (prefix sum)
                              + Column filter (fallback)
                                     │
                            db.products.filter({ ... })
                                  760ns–20µs
```

The engine uses three query strategies, auto-selected per query:

1. **Pair table** — precomputed 2D lookup for every dimension pair. O(1). Returns in ~100ns (760ns including FFI).
2. **Bitmap AND** — bitwise intersection for dimensions with known cardinality. O(n/64).
3. **Column filter** — direct column read for remaining conditions. O(candidates).

For a 4-condition query, it might use pair table for the first two, then column filter for the rest — combining O(1) + O(small) for the best of both worlds.

Everything runs through mmap, so the cache persists across process restarts.

## Get started

```bash
npm install enchu-extend
```

```ts
import { enchu } from 'enchu-extend'

const db = await enchu(process.env.DATABASE_URL!)
const results = db.products.filter({ color: 'red' })
```

That's it. No Elasticsearch cluster. No Meilisearch server. No Redis. Just your PostgreSQL and three lines of code.

---

*Built with Rust. Quantum cylinder engine (v27). Available on npm.*
