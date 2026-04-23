// Enchu Extend v18 — 全パターンベンチマーク
// Usage: npx tsx bench_full.ts

import * as enchu from './native'

const CACHE_DIR = '/tmp/enchu_extend_bench_full'
const PG_CONN = 'host=localhost port=5432 user=postgres password=postgres dbname=enchu_bench'

const SCHEMA = JSON.stringify({
  name: "projects",
  fields: [
    { name: "entity_id", type: "int" },
  ],
  tables: [
    {
      name: "members",
      fields: [
        { name: "text_val", type: "text" },
        { name: "float_val", type: "float" },
        { name: "int_val", type: "int" },
      ]
    },
    {
      name: "tasks",
      fields: [
        { name: "text_val", type: "text" },
        { name: "float_val", type: "float" },
        { name: "int_val", type: "int" },
      ]
    },
    {
      name: "costs",
      fields: [
        { name: "text_val", type: "text" },
        { name: "float_val", type: "float" },
        { name: "int_val", type: "int" },
      ]
    },
  ]
})

function pgQuery(sql: string): string {
  const { execSync } = require('child_process')
  return execSync(
    `docker exec enchu-postgres psql -U postgres -d enchu_bench -t -A -c "${sql}"`,
    { encoding: 'utf-8', maxBuffer: 100 * 1024 * 1024 }
  )
}

const WARMUP = 1000
const ITERATIONS = 50000
const PG_ITERS = 100

interface BenchResult {
  name: string
  enchuNs: number
  pgNs: number
  speedup: number
}

function benchEnchu(label: string, fn: () => void): number {
  for (let i = 0; i < WARMUP; i++) fn()
  const t = performance.now()
  for (let i = 0; i < ITERATIONS; i++) fn()
  return (performance.now() - t) / ITERATIONS * 1_000_000
}

function benchPg(label: string, fn: () => void): number {
  const t = performance.now()
  for (let i = 0; i < PG_ITERS; i++) fn()
  return (performance.now() - t) / PG_ITERS * 1_000_000
}

async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) {
    fs.rmSync(CACHE_DIR, { recursive: true })
  }

  console.log('=== Enchu Extend v18 — Full Benchmark ===\n')

  // --- Setup ---
  enchu.init(CACHE_DIR, SCHEMA, PG_CONN)

  const ENTITIES = 100
  console.log(`Loading ${ENTITIES} entities...`)
  const t0 = performance.now()

  for (let eid = 1; eid <= ENTITIES; eid++) {
    const membersRaw = pgQuery(
      `SELECT text_val, float_val, int_val FROM members WHERE entity_id = ${eid} LIMIT 50`
    )
    const members = membersRaw.trim().split('\n').filter(Boolean).map(line => {
      const [text_val, float_val, int_val] = line.split('|')
      return { text_val: text_val || '', float_val: parseFloat(float_val) || 0, int_val: parseInt(int_val) || 0 }
    })

    const tasksRaw = pgQuery(
      `SELECT text_val, float_val, int_val FROM tasks WHERE entity_id = ${eid} LIMIT 50`
    )
    const tasks = tasksRaw.trim().split('\n').filter(Boolean).map(line => {
      const [text_val, float_val, int_val] = line.split('|')
      return { text_val: text_val || '', float_val: parseFloat(float_val) || 0, int_val: parseInt(int_val) || 0 }
    })

    const costsRaw = pgQuery(
      `SELECT text_val, float_val, int_val FROM costs WHERE entity_id = ${eid} LIMIT 50`
    )
    const costs = costsRaw.trim().split('\n').filter(Boolean).map(line => {
      const [text_val, float_val, int_val] = line.split('|')
      return { text_val: text_val || '', float_val: parseFloat(float_val) || 0, int_val: parseInt(int_val) || 0 }
    })

    enchu.put(JSON.stringify({
      key: `entity_${eid}`,
      axes: { entity_id: String(eid) },
      tables: { members, tasks, costs }
    }))
  }

  enchu.sync()
  console.log(`Loaded in ${(performance.now() - t0).toFixed(0)}ms\n`)

  // サンプルデータ取得
  const sample = JSON.parse(enchu.lookup('entity_id', '1')!)
  const sampleText = sample.tables.members[0].text_val
  const sampleTaskText = sample.tables.tasks[0].text_val
  const sampleCostText = sample.tables.costs[0].text_val

  const results: BenchResult[] = []

  // --- 1. ルートフィールド検索 (entity_id) ---
  {
    const enchuNs = benchEnchu('entity_id', () => enchu.lookup('entity_id', '42'))
    const pgNs = benchPg('PG entity_id', () =>
      pgQuery(`SELECT * FROM members WHERE entity_id = 42 LIMIT 50`))
    results.push({ name: 'ルート検索 (entity_id)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 2. ネスト text 検索 (members.text_val) ---
  {
    const enchuNs = benchEnchu('members.text_val', () =>
      enchu.lookup('members.text_val', sampleText))
    const pgNs = benchPg('PG members.text_val', () =>
      pgQuery(`SELECT * FROM members WHERE text_val = '${sampleText}'`))
    results.push({ name: 'ネスト text (members.text_val)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 3. ネスト text 検索 (tasks.text_val) ---
  {
    const enchuNs = benchEnchu('tasks.text_val', () =>
      enchu.lookup('tasks.text_val', sampleTaskText))
    const pgNs = benchPg('PG tasks.text_val', () =>
      pgQuery(`SELECT * FROM tasks WHERE text_val = '${sampleTaskText}'`))
    results.push({ name: 'ネスト text (tasks.text_val)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 4. ネスト text 検索 (costs.text_val) ---
  {
    const enchuNs = benchEnchu('costs.text_val', () =>
      enchu.lookup('costs.text_val', sampleCostText))
    const pgNs = benchPg('PG costs.text_val', () =>
      pgQuery(`SELECT * FROM costs WHERE text_val = '${sampleCostText}'`))
    results.push({ name: 'ネスト text (costs.text_val)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 5. 存在しないキーの検索 (miss) ---
  {
    const enchuNs = benchEnchu('miss', () => enchu.lookup('entity_id', '99999'))
    const pgNs = benchPg('PG miss', () =>
      pgQuery(`SELECT * FROM members WHERE entity_id = 99999 LIMIT 50`))
    results.push({ name: 'ミス (存在しないキー)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 6. 連続異なるキー (ランダムアクセス) ---
  {
    const keys = Array.from({ length: ITERATIONS }, (_, i) => String((i % 100) + 1))
    let idx = 0
    for (let i = 0; i < WARMUP; i++) enchu.lookup('entity_id', keys[i % keys.length])
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) enchu.lookup('entity_id', keys[idx++])
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    idx = 0
    const pgKeys = Array.from({ length: PG_ITERS }, (_, i) => (i % 100) + 1)
    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = ${pgKeys[i]} LIMIT 50`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: 'ランダムアクセス (100キー巡回)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 7. lookup + JSON.parse (実用コスト) ---
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('entity_id', '42')
      if (r) JSON.parse(r)
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('entity_id', '42')
      if (r) JSON.parse(r)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      const raw = pgQuery(`SELECT * FROM members WHERE entity_id = 42 LIMIT 50`)
      raw.trim().split('\n') // パースもどき
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: 'lookup + JSON.parse (実用)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 結果表示 ---
  console.log('┌─────────────────────────────────────────┬────────────┬──────────────┬─────────┐')
  console.log('│ 検索パターン                            │ Enchu      │ PostgreSQL   │ 倍率    │')
  console.log('├─────────────────────────────────────────┼────────────┼──────────────┼─────────┤')
  for (const r of results) {
    const name = r.name.padEnd(35)
    const e = formatNs(r.enchuNs).padStart(10)
    const p = formatNs(r.pgNs).padStart(12)
    const s = `${r.speedup.toFixed(0)}x`.padStart(7)
    console.log(`│ ${name}     │ ${e} │ ${p} │ ${s} │`)
  }
  console.log('└─────────────────────────────────────────┴────────────┴──────────────┴─────────┘')

  // 平均
  const avgEnchu = results.reduce((s, r) => s + r.enchuNs, 0) / results.length
  const avgPg = results.reduce((s, r) => s + r.pgNs, 0) / results.length
  console.log(`\n平均: Enchu ${formatNs(avgEnchu)} / PG ${formatNs(avgPg)} → ${(avgPg / avgEnchu).toFixed(0)}x`)
}

function formatNs(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

main().catch(console.error)
