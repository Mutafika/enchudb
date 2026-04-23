// Enchu + JS filter vs PostgreSQL — 100万行ベンチマーク
// Usage: npx tsx bench_1m.ts

import * as enchu from './native'

const CACHE_DIR = '/tmp/enchu_extend_bench_1m'

const SCHEMA = JSON.stringify({
  name: "companies",
  fields: [
    { name: "company_id", type: "int" },
  ],
  tables: [
    {
      name: "employees",
      fields: [
        { name: "name", type: "text" },
        { name: "department", type: "text" },
        { name: "salary", type: "int" },
        { name: "age", type: "int" },
      ]
    },
    {
      name: "invoices",
      fields: [
        { name: "client", type: "text" },
        { name: "amount", type: "float" },
        { name: "status", type: "text" },
      ]
    },
  ]
})

const NAMES = ['田中', '佐藤', '鈴木', '高橋', '伊藤', '渡辺', '山本', '中村', '小林', '加藤',
  '吉田', '山田', '佐々木', '松本', '井上', '木村', '林', '斎藤', '清水', '山口']
const DEPTS = ['営業', '開発', '人事', '経理', '企画', '総務', '法務', '広報']
const CLIENTS = ['A社', 'B社', 'C社', 'D社', 'E社', 'F社', 'G社', 'H社']
const STATUSES = ['paid', 'pending', 'overdue', 'draft']

function formatNs(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) fs.rmSync(CACHE_DIR, { recursive: true })

  console.log('=== Enchu 100万行ベンチマーク ===\n')

  enchu.init(CACHE_DIR, SCHEMA, 'host=localhost')

  // --- データ生成: 10,000社 × 社員100人 = 100万行 ---
  const COMPANIES = 10_000
  const EMPLOYEES_PER = 100
  const INVOICES_PER = 20
  const totalRows = COMPANIES * (EMPLOYEES_PER + INVOICES_PER)

  console.log(`生成: ${COMPANIES.toLocaleString()}社 × (社員${EMPLOYEES_PER} + 請求${INVOICES_PER}) = ${totalRows.toLocaleString()}行`)

  const t0 = performance.now()

  for (let cid = 1; cid <= COMPANIES; cid++) {
    const employees = []
    for (let i = 0; i < EMPLOYEES_PER; i++) {
      employees.push({
        name: `${NAMES[i % NAMES.length]}${cid}_${i}`,
        department: DEPTS[i % DEPTS.length],
        salary: 300000 + (i * 5000) + (cid % 100) * 1000,
        age: 22 + (i % 40),
      })
    }

    const invoices = []
    for (let i = 0; i < INVOICES_PER; i++) {
      invoices.push({
        client: `${CLIENTS[i % CLIENTS.length]}_${cid}`,
        amount: 100000 + (i * 50000) + (cid % 50) * 10000,
        status: STATUSES[i % STATUSES.length],
      })
    }

    enchu.put(JSON.stringify({
      key: `company_${cid}`,
      axes: { company_id: String(cid) },
      tables: { employees, invoices }
    }))

    if (cid % 2000 === 0) {
      const elapsed = ((performance.now() - t0) / 1000).toFixed(1)
      console.log(`  ${cid.toLocaleString()} / ${COMPANIES.toLocaleString()} (${elapsed}s)`)
    }
  }

  console.log('Syncing...')
  const syncStart = performance.now()
  enchu.sync()
  const syncTime = performance.now() - syncStart
  const loadTime = performance.now() - t0

  console.log(`ロード: ${(loadTime / 1000).toFixed(1)}s / Sync: ${(syncTime / 1000).toFixed(1)}s`)
  console.log(`${totalRows.toLocaleString()}行 cached\n`)

  // --- サンプル確認 ---
  const sample = JSON.parse(enchu.lookup('company_id', '1')!)
  console.log(`サンプル: company_1 → 社員${sample.tables.employees.length}人, 請求${sample.tables.invoices.length}件\n`)

  // --- ベンチマーク ---
  const WARMUP = 500
  const ITERATIONS = 20000

  interface Result { name: string; enchuNs: number; detail: string }
  const results: Result[] = []

  // 1. 完全一致 (company_id)
  {
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', '5000')
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) enchu.lookup('company_id', '5000')
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    results.push({ name: '完全一致 (company_id)', enchuNs: ns, detail: '→ 社員100人+請求20件' })
  }

  // 2. ネスト完全一致 (employees.name)
  {
    const searchName = sample.tables.employees[50].name
    for (let i = 0; i < WARMUP; i++) enchu.lookup('employees.name', searchName)
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) enchu.lookup('employees.name', searchName)
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    results.push({ name: 'ネスト完全一致 (name)', enchuNs: ns, detail: `"${searchName}"` })
  }

  // 3. 部分一致 (lookup + filter includes)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.name.includes('田中'))
    }
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.name.includes('田中'))
    }
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    const matchCount = JSON.parse(enchu.lookup('company_id', '1')!).tables.employees
      .filter((e: any) => e.name.includes('田中')).length
    results.push({ name: '部分一致 (LIKE 田中)', enchuNs: ns, detail: `100人中${matchCount}人ヒット` })
  }

  // 4. 数値範囲 (salary BETWEEN)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.salary >= 400000 && e.salary <= 500000)
    }
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.salary >= 400000 && e.salary <= 500000)
    }
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    const rangeCount = JSON.parse(enchu.lookup('company_id', '1')!).tables.employees
      .filter((e: any) => e.salary >= 400000 && e.salary <= 500000).length
    results.push({ name: '数値範囲 (給与40-50万)', enchuNs: ns, detail: `100人中${rangeCount}人ヒット` })
  }

  // 5. ソート (salary DESC)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.sort((a: any, b: any) => b.salary - a.salary)
    }
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.sort((a: any, b: any) => b.salary - a.salary)
    }
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    results.push({ name: 'ソート (給与DESC)', enchuNs: ns, detail: '100人を降順' })
  }

  // 6. 集計 (部署別 COUNT + AVG salary)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      const byDept: Record<string, { count: number; total: number }> = {}
      for (const e of d.tables.employees) {
        if (!byDept[e.department]) byDept[e.department] = { count: 0, total: 0 }
        byDept[e.department].count++
        byDept[e.department].total += e.salary
      }
    }
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      const byDept: Record<string, { count: number; total: number }> = {}
      for (const e of d.tables.employees) {
        if (!byDept[e.department]) byDept[e.department] = { count: 0, total: 0 }
        byDept[e.department].count++
        byDept[e.department].total += e.salary
      }
    }
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    results.push({ name: '集計 (部署別 GROUP BY)', enchuNs: ns, detail: `${DEPTS.length}部署` })
  }

  // 7. 複合: filter + sort + TOP5
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees
        .filter((e: any) => e.department === '開発' && e.age >= 30)
        .sort((a: any, b: any) => b.salary - a.salary)
        .slice(0, 5)
    }
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees
        .filter((e: any) => e.department === '開発' && e.age >= 30)
        .sort((a: any, b: any) => b.salary - a.salary)
        .slice(0, 5)
    }
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    const topResult = JSON.parse(enchu.lookup('company_id', '1')!).tables.employees
      .filter((e: any) => e.department === '開発' && e.age >= 30)
    results.push({ name: '複合 (開発+30歳↑+TOP5)', enchuNs: ns, detail: `該当${topResult.length}人→TOP5` })
  }

  // 8. クロステーブル: 社員数 + 請求合計 + 1人あたり
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      const headcount = d.tables.employees.length
      const revenue = d.tables.invoices
        .filter((inv: any) => inv.status === 'paid')
        .reduce((s: number, inv: any) => s + inv.amount, 0)
      const perHead = revenue / headcount
    }
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      const headcount = d.tables.employees.length
      const revenue = d.tables.invoices
        .filter((inv: any) => inv.status === 'paid')
        .reduce((s: number, inv: any) => s + inv.amount, 0)
      const perHead = revenue / headcount
    }
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    results.push({ name: 'クロス (売上/人)', enchuNs: ns, detail: '社員+請求→1人あたり売上' })
  }

  // 9. ランダムアクセス (10,000社からランダム)
  {
    const keys = Array.from({ length: ITERATIONS }, () => String(1 + Math.floor(Math.random() * COMPANIES)))
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', keys[i])
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) enchu.lookup('company_id', keys[i])
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    results.push({ name: 'ランダム (10,000社)', enchuNs: ns, detail: '毎回違う会社' })
  }

  // 10. ミス (存在しないキー)
  {
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', '999999')
    const t = performance.now()
    for (let i = 0; i < ITERATIONS; i++) enchu.lookup('company_id', '999999')
    const ns = (performance.now() - t) / ITERATIONS * 1_000_000
    results.push({ name: 'ミス (存在しない)', enchuNs: ns, detail: '即座にnull' })
  }

  // --- 結果表示 ---
  console.log('┌───────────────────────────────┬────────────┬──────────────────────────────┐')
  console.log('│ 操作                          │ レイテンシ │ 詳細                         │')
  console.log('├───────────────────────────────┼────────────┼──────────────────────────────┤')
  for (const r of results) {
    const name = r.name.padEnd(27)
    const e = formatNs(r.enchuNs).padStart(10)
    const detail = r.detail.padEnd(28)
    console.log(`│ ${name}   │ ${e} │ ${detail} │`)
  }
  console.log('└───────────────────────────────┴────────────┴──────────────────────────────┘')

  const avg = results.reduce((s, r) => s + r.enchuNs, 0) / results.length
  console.log(`\n平均レイテンシ: ${formatNs(avg)}`)
  console.log(`データ量: ${totalRows.toLocaleString()}行 (${COMPANIES.toLocaleString()}社)`)
}

main().catch(console.error)
