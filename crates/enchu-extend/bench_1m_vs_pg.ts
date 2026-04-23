// Enchu 120万行 vs PostgreSQL 500万行 — 同等操作の比較
// Usage: npx tsx bench_1m_vs_pg.ts

import * as enchu from './native'

const CACHE_DIR = '/tmp/enchu_extend_bench_1m'

function pgQuery(sql: string): string {
  const { execSync } = require('child_process')
  return execSync(
    `docker exec enchu-postgres psql -U postgres -d enchu_bench -t -A -c "${sql}"`,
    { encoding: 'utf-8', maxBuffer: 100 * 1024 * 1024 }
  )
}

function formatNs(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

const PG_ITERS = 50

async function main() {
  const fs = require('fs')

  // --- Enchuキャッシュが既にあるか確認 ---
  if (!fs.existsSync(CACHE_DIR)) {
    console.log('先に bench_1m.ts を実行してキャッシュを作ってください')
    process.exit(1)
  }

  const SCHEMA = JSON.stringify({
    name: "companies",
    fields: [{ name: "company_id", type: "int" }],
    tables: [
      { name: "employees", fields: [
        { name: "name", type: "text" }, { name: "department", type: "text" },
        { name: "salary", type: "int" }, { name: "age", type: "int" },
      ]},
      { name: "invoices", fields: [
        { name: "client", type: "text" }, { name: "amount", type: "float" },
        { name: "status", type: "text" },
      ]},
    ]
  })

  enchu.open(CACHE_DIR, SCHEMA, 'host=localhost')
  enchu.sync()

  console.log('=== Enchu 120万行 vs PostgreSQL 500万行 ===\n')

  // PGテーブル情報
  const pgCount = pgQuery('SELECT COUNT(*) FROM members').trim()
  console.log(`PostgreSQL: members ${parseInt(pgCount).toLocaleString()}行`)
  console.log(`Enchu:      120万行 (10,000社 × 120行)\n`)

  const WARMUP = 500
  const ITERATIONS = 20000

  interface Result { name: string; enchuNs: number; pgNs: number; speedup: number }
  const results: Result[] = []

  // 1. 完全一致 (ID検索)
  {
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', '5000')
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) enchu.lookup('company_id', '5000')
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 42 LIMIT 100`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '完全一致 (ID)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 2. 部分一致 (LIKE)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.name.includes('田中'))
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.name.includes('田中'))
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 1 AND text_val LIKE '%row_0%'`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '部分一致 (LIKE)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 3. 数値範囲 (BETWEEN)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.salary >= 400000 && e.salary <= 500000)
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.salary >= 400000 && e.salary <= 500000)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 1 AND int_val BETWEEN 10 AND 30`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '数値範囲 (BETWEEN)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 4. ソート (ORDER BY)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.sort((a: any, b: any) => b.salary - a.salary)
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.sort((a: any, b: any) => b.salary - a.salary)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 1 ORDER BY float_val DESC`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: 'ソート (ORDER BY)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 5. 集計 (GROUP BY)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      const byDept: Record<string, number> = {}
      for (const e of d.tables.employees) {
        byDept[e.department] = (byDept[e.department] || 0) + e.salary
      }
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      const byDept: Record<string, number> = {}
      for (const e of d.tables.employees) {
        byDept[e.department] = (byDept[e.department] || 0) + e.salary
      }
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT text_val, COUNT(*), SUM(int_val) FROM members WHERE entity_id = 1 GROUP BY text_val`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '集計 (GROUP BY)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 6. 複合 (WHERE + ORDER BY + LIMIT)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees
        .filter((e: any) => e.department === '開発' && e.age >= 30)
        .sort((a: any, b: any) => b.salary - a.salary)
        .slice(0, 5)
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees
        .filter((e: any) => e.department === '開発' && e.age >= 30)
        .sort((a: any, b: any) => b.salary - a.salary)
        .slice(0, 5)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 1 AND int_val > 25 ORDER BY float_val DESC LIMIT 5`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: '複合 (filter+sort+TOP5)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 7. クロステーブル (2テーブル同時)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      const headcount = d.tables.employees.length
      const revenue = d.tables.invoices
        .filter((inv: any) => inv.status === 'paid')
        .reduce((s: number, inv: any) => s + inv.amount, 0)
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      const headcount = d.tables.employees.length
      const revenue = d.tables.invoices
        .filter((inv: any) => inv.status === 'paid')
        .reduce((s: number, inv: any) => s + inv.amount, 0)
    }
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT COUNT(*) FROM members WHERE entity_id = 1`)
      pgQuery(`SELECT SUM(float_val) FROM costs WHERE entity_id = 1`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: 'クロステーブル (2テーブル)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 8. ミス
  {
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', '999999')
    const t1 = performance.now()
    for (let i = 0; i < ITERATIONS; i++) enchu.lookup('company_id', '999999')
    const enchuNs = (performance.now() - t1) / ITERATIONS * 1_000_000

    const t2 = performance.now()
    for (let i = 0; i < PG_ITERS; i++) {
      pgQuery(`SELECT * FROM members WHERE entity_id = 999999 LIMIT 100`)
    }
    const pgNs = (performance.now() - t2) / PG_ITERS * 1_000_000

    results.push({ name: 'ミス (存在しない)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // --- 結果表示 ---
  console.log('┌───────────────────────────────┬────────────┬──────────────┬─────────┐')
  console.log('│ 操作                          │ Enchu+JS   │ PostgreSQL   │ 倍率    │')
  console.log('├───────────────────────────────┼────────────┼──────────────┼─────────┤')
  for (const r of results) {
    const name = r.name.padEnd(27)
    const e = formatNs(r.enchuNs).padStart(10)
    const p = formatNs(r.pgNs).padStart(12)
    const s = `${r.speedup.toFixed(0)}x`.padStart(7)
    console.log(`│ ${name}   │ ${e} │ ${p} │ ${s} │`)
  }
  console.log('└───────────────────────────────┴────────────┴──────────────┴─────────┘')

  const avgEnchu = results.reduce((s, r) => s + r.enchuNs, 0) / results.length
  const avgPg = results.reduce((s, r) => s + r.pgNs, 0) / results.length
  console.log(`\n平均: Enchu+JS ${formatNs(avgEnchu)} / PG ${formatNs(avgPg)} → ${(avgPg / avgEnchu).toFixed(0)}x`)
}

main().catch(console.error)
