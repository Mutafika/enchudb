// Enchu Extend v3 vs PostgreSQL — 公平比較（TCP直接接続）
// PGもnode-postgres経由でTCP接続。docker execのプロセス起動コストなし。

import * as enchu from './native'
import { Client } from 'pg'

const CACHE_DIR = '/tmp/enchu_extend_bench_fair'
const ENTITIES = 1000

const SCHEMA = JSON.stringify({
  name: "entities",
  fields: [{ name: "entity_id", type: "int" }],
  tables: [{
    name: "members",
    fields: [
      { name: "text_val", type: "symbol" },
      { name: "int_val", type: "int" },
    ]
  }, {
    name: "tasks",
    fields: [
      { name: "text_val", type: "symbol" },
      { name: "int_val", type: "int" },
    ]
  }]
})

function fmt(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

async function benchAsync(warmup: number, iters: number, fn: () => Promise<void>): Promise<number> {
  for (let i = 0; i < warmup; i++) await fn()
  const t = performance.now()
  for (let i = 0; i < iters; i++) await fn()
  return (performance.now() - t) / iters * 1_000_000 // ns
}

function bench(warmup: number, iters: number, fn: () => void): number {
  for (let i = 0; i < warmup; i++) fn()
  const t = performance.now()
  for (let i = 0; i < iters; i++) fn()
  return (performance.now() - t) / iters * 1_000_000 // ns
}

async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) fs.rmSync(CACHE_DIR, { recursive: true })

  // PG直接接続
  const pg = new Client({
    host: 'localhost',
    port: 5433,
    user: 'postgres',
    password: 'enchu',
    database: 'enchu_bench',
  })
  await pg.connect()
  console.log('PG connected (TCP direct)\n')

  console.log('╔══════════════════════════════════════════════════════════════╗')
  console.log('║  Enchu Extend v3 vs PostgreSQL (TCP直接)                   ║')
  console.log(`║  ${ENTITIES} entities × 50 members                             ║`)
  console.log('╚══════════════════════════════════════════════════════════════╝\n')

  // Init Enchu + Load from PG
  enchu.init(CACHE_DIR, SCHEMA, '')

  console.log(`Loading ${ENTITIES} entities...`)
  const t0 = performance.now()

  for (let eid = 0; eid < ENTITIES; eid++) {
    const cid = enchu.insert('entities')
    enchu.writeInt('entities', cid, 'entity_id', eid)

    const members = await pg.query(
      'SELECT text_val, int_val FROM members WHERE entity_id = $1', [eid]
    )
    for (const row of members.rows) {
      const mid = enchu.insertChild('members', cid)
      enchu.writeText('members', mid, 'text_val', row.text_val || '')
      enchu.writeInt('members', mid, 'int_val', parseInt(row.int_val) || 0)
    }

    const tasks = await pg.query(
      'SELECT text_val, int_val FROM tasks WHERE entity_id = $1', [eid]
    )
    for (const row of tasks.rows) {
      const tid = enchu.insertChild('tasks', cid)
      enchu.writeText('tasks', tid, 'text_val', row.text_val || '')
      enchu.writeInt('tasks', tid, 'int_val', parseInt(row.int_val) || 0)
    }
  }

  enchu.flush()
  const loadTime = performance.now() - t0
  console.log(`Loaded in ${(loadTime / 1000).toFixed(1)}s (members: ${enchu.count('members')}, tasks: ${enchu.count('tasks')})\n`)

  // Prepared statements
  const pgLookup = 'SELECT * FROM members WHERE entity_id = $1 LIMIT 1'
  const pgText = "SELECT * FROM members WHERE text_val = $1"
  const pgRange = 'SELECT count(*) FROM members WHERE int_val BETWEEN $1 AND $2'
  const pgChildren = 'SELECT * FROM members WHERE entity_id = $1'
  const pgColumnInt = 'SELECT int_val FROM members WHERE entity_id = $1'
  const pgComposite = 'SELECT count(*) FROM members WHERE entity_id = $1 AND int_val > $2'

  const W = 500
  const R = 10000
  const PG_W = 50
  const PG_R = 1000

  console.log('  ┌──────────────────────────┬──────────┬──────────┬──────────┐')
  console.log('  │ クエリ                   │  PG(TCP) │   Extend │     倍率 │')
  console.log('  ├──────────────────────────┼──────────┼──────────┼──────────┤')

  // 1. 完全一致 (entity_id)
  const pg1 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query(pgLookup, [42])
  })
  const en1 = bench(W, R, () => {
    enchu.slice('entities', 'entity_id', 42)
  })
  row('完全一致(entity_id)', pg1, en1)

  // 2. 完全一致 (text_val)
  const sampleText = 'entity_0_members_row_0'
  const textVid = enchu.vocabLookup(sampleText)
  const pg2 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query(pgText, [sampleText])
  })
  const en2 = textVid != null ? bench(W, R, () => {
    enchu.slice('members', 'text_val', textVid!)
  }) : 0
  row('完全一致(text_val)', pg2, en2)

  // 3. 範囲検索 (int_val 100-200)
  const pg3 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query(pgRange, [100, 200])
  })
  const en3 = bench(W, R, () => {
    enchu.sliceRange('members', 'int_val', 100, 200)
  })
  row('範囲(int 100-200)', pg3, en3)

  // 4. parent
  const pg4 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query(pgLookup, [42])
  })
  const en4 = bench(W, R, () => {
    enchu.parent('members', 42)
  })
  row('親辿り(parent)', pg4, en4)

  // 5. 子一覧
  const pg5 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query(pgChildren, [42])
  })
  const en5 = bench(W, R, () => {
    enchu.children('members', 42)
  })
  row('子一覧(children)', pg5, en5)

  // 6. カラム読み (int_val × 50件)
  const pg6 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query(pgColumnInt, [42])
  })
  const kidsBuf = enchu.children('members', 42)
  const en6 = bench(W, R, () => {
    enchu.getColumnInt('members', 'int_val', kidsBuf)
  })
  row('カラム読み(int×50)', pg6, en6)

  // 7. 複合条件
  const pg7 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query(pgComposite, [42, 300])
  })
  const en7 = bench(W, R, () => {
    const kids = enchu.children('members', 42)
    enchu.getColumnInt('members', 'int_val', kids)
  })
  row('複合(eid+int)', pg7, en7)

  console.log('  └──────────────────────────┴──────────┴──────────┴──────────┘')

  await pg.end()
  console.log('\n  Done.')
}

function row(label: string, pg: number, en: number) {
  const speedup = en > 0 ? `${(pg / en).toFixed(0)}x` : 'N/A'
  console.log(`  │ ${label.padEnd(24)} │ ${fmt(pg).padStart(8)} │ ${fmt(en).padStart(8)} │ ${speedup.padStart(8)} │`)
}

main().catch(console.error)
