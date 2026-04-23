// Enchu lookup + JS filter vs PG WHERE — 実用ベンチマーク
// Usage: npx tsx bench_filter.ts

import * as enchu from './native'

const CACHE_DIR = '/tmp/enchu_extend_bench_filter'
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

const WARMUP = 500
const ITERATIONS = 20000
const PG_ITERS = 100

interface BenchResult {
  name: string
  enchuNs: number
  pgNs: number
  speedup: number
}

function formatNs(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) fs.rmSync(CACHE_DIR, { recursive: true })

  console.log('=== Enchu + JS filter vs PostgreSQL WHERE ===\n')

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

  // サンプルデータ確認
  const sample = JSON.parse(enchu.lookup('entity_id', '1')!)
  const sampleText = sample.tables.members[0].text_val
  const partialMatch = sampleText.substring(0, 10) // 部分一致用

  const results: BenchResult[] = []

  // --- 1. 完全一致 ---
  {
    // Enchu: lookup のみ
    let enchuResult: any
    for (let i = 0; i < WARMUP; i++) enchu.lookup('members.text_val', sampleText)
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      enchuResult = enchu.lookup('members.text_val', sampleText)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    // PG: WHERE =
    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE text_val = '${sampleText}'`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '完全一致', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 2. 部分一致 (LIKE '%xxx%') ---
  {
    // Enchu: lookup(entity_id) → parse → filter(includes)
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('entity_id', '1')
      if (r) {
        const d = JSON.parse(r)
        d.tables.members.filter((m: any) => m.text_val.includes(partialMatch))
      }
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('entity_id', '1')!
      const d = JSON.parse(r)
      d.tables.members.filter((m: any) => m.text_val.includes(partialMatch))
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    // PG: WHERE LIKE
    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 1 AND text_val LIKE '%${partialMatch}%'`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '部分一致 (LIKE)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 3. 数値範囲 (WHERE int_val BETWEEN) ---
  {
    // Enchu: lookup → parse → filter(range)
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('entity_id', '1')
      if (r) {
        const d = JSON.parse(r)
        d.tables.members.filter((m: any) => m.int_val >= 10 && m.int_val <= 30)
      }
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('entity_id', '1')!
      const d = JSON.parse(r)
      d.tables.members.filter((m: any) => m.int_val >= 10 && m.int_val <= 30)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    // PG: WHERE BETWEEN
    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 1 AND int_val BETWEEN 10 AND 30`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '数値範囲 (BETWEEN)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 4. ソート (ORDER BY) ---
  {
    // Enchu: lookup → parse → sort
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('entity_id', '1')
      if (r) {
        const d = JSON.parse(r)
        d.tables.members.sort((a: any, b: any) => b.float_val - a.float_val)
      }
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('entity_id', '1')!
      const d = JSON.parse(r)
      d.tables.members.sort((a: any, b: any) => b.float_val - a.float_val)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    // PG: ORDER BY
    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 1 ORDER BY float_val DESC`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: 'ソート (ORDER BY)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 5. 集計 (COUNT + SUM) ---
  {
    // Enchu: lookup → parse → reduce
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('entity_id', '1')
      if (r) {
        const d = JSON.parse(r)
        const count = d.tables.costs.length
        const sum = d.tables.costs.reduce((s: number, c: any) => s + c.float_val, 0)
      }
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('entity_id', '1')!
      const d = JSON.parse(r)
      const count = d.tables.costs.length
      const sum = d.tables.costs.reduce((s: number, c: any) => s + c.float_val, 0)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    // PG: COUNT + SUM
    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT COUNT(*), SUM(float_val) FROM costs WHERE entity_id = 1`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '集計 (COUNT + SUM)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 6. 複合: filter + sort + limit (TOP N) ---
  {
    // Enchu: lookup → parse → filter → sort → slice
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('entity_id', '1')
      if (r) {
        const d = JSON.parse(r)
        d.tables.members
          .filter((m: any) => m.int_val > 5)
          .sort((a: any, b: any) => b.float_val - a.float_val)
          .slice(0, 5)
      }
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('entity_id', '1')!
      const d = JSON.parse(r)
      d.tables.members
        .filter((m: any) => m.int_val > 5)
        .sort((a: any, b: any) => b.float_val - a.float_val)
        .slice(0, 5)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    // PG: WHERE + ORDER BY + LIMIT
    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 1 AND int_val > 5 ORDER BY float_val DESC LIMIT 5`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '複合 (filter+sort+TOP5)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 7. クロステーブル: members + costs 突合 ---
  {
    // Enchu: 1回のlookupで両テーブル取得 → JS突合
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('entity_id', '1')
      if (r) {
        const d = JSON.parse(r)
        const memberCount = d.tables.members.length
        const costTotal = d.tables.costs.reduce((s: number, c: any) => s + c.float_val, 0)
        const avgCostPerMember = costTotal / memberCount
      }
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('entity_id', '1')!
      const d = JSON.parse(r)
      const memberCount = d.tables.members.length
      const costTotal = d.tables.costs.reduce((s: number, c: any) => s + c.float_val, 0)
      const avgCostPerMember = costTotal / memberCount
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    // PG: 2クエリ (members COUNT + costs SUM)
    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      const mc = pgQuery(`SELECT COUNT(*) FROM members WHERE entity_id = 1`)
      const ct = pgQuery(`SELECT SUM(float_val) FROM costs WHERE entity_id = 1`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: 'クロステーブル突合', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 結果表示 ---
  console.log('┌──────────────────────────────┬────────────┬──────────────┬─────────┐')
  console.log('│ 操作                         │ Enchu+JS   │ PostgreSQL   │ 倍率    │')
  console.log('├──────────────────────────────┼────────────┼──────────────┼─────────┤')
  for (const r of results) {
    const name = r.name.padEnd(26)
    const e = formatNs(r.enchuNs).padStart(10)
    const p = formatNs(r.pgNs).padStart(12)
    const s = `${r.speedup.toFixed(0)}x`.padStart(7)
    console.log(`│ ${name}   │ ${e} │ ${p} │ ${s} │`)
  }
  console.log('└──────────────────────────────┴────────────┴──────────────┴─────────┘')

  const avgEnchu = results.reduce((s, r) => s + r.enchuNs, 0) / results.length
  const avgPg = results.reduce((s, r) => s + r.pgNs, 0) / results.length
  console.log(`\n平均: Enchu+JS ${formatNs(avgEnchu)} / PG ${formatNs(avgPg)} → ${(avgPg / avgEnchu).toFixed(0)}x`)
}

main().catch(console.error)
