// Enchu vs PostgreSQL — フェア比較 (インデックスあり/なし)
// 同一Dockerネットワーク内、同じデータ、node-postgres直接接続
// Usage: docker build & docker run

import * as enchu from './native'
import { Client } from 'pg'

const CACHE_DIR = '/tmp/enchu_bench_real'
const PG_CONN = {
  host: process.env.PG_HOST || 'localhost',
  port: parseInt(process.env.PG_PORT || '5432'),
  user: 'postgres',
  password: process.env.PGPASSWORD || 'postgres',
  database: 'postgres',
}

const COMPANIES = parseInt(process.env.COMPANIES || '1000')
const EMPLOYEES_PER = parseInt(process.env.EMPLOYEES_PER || '100')
const TOTAL_ROWS = COMPANIES * EMPLOYEES_PER

const NAMES = ['田中', '佐藤', '鈴木', '高橋', '伊藤', '渡辺', '山本', '中村', '小林', '加藤']
const DEPTS = ['営業', '開発', '人事', '経理', '企画', '総務', '法務', '広報']

function formatNs(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(2)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) fs.rmSync(CACHE_DIR, { recursive: true })

  const pg = new Client(PG_CONN)
  await pg.connect()

  console.log('=== Enchu vs PostgreSQL — フェア比較 ===\n')

  // --- PGにテーブル作成 ---
  console.log('PGテーブル作成...')
  await pg.query('DROP TABLE IF EXISTS employees')
  await pg.query(`
    CREATE TABLE employees (
      id SERIAL PRIMARY KEY,
      company_id INT NOT NULL,
      name TEXT NOT NULL,
      department TEXT NOT NULL,
      salary INT NOT NULL,
      age INT NOT NULL
    )
  `)

  // --- 同じデータをPGとEnchuに投入 ---
  console.log(`データ投入: ${COMPANIES}社 × ${EMPLOYEES_PER}人 = ${TOTAL_ROWS.toLocaleString()}行`)

  const SCHEMA = JSON.stringify({
    name: "companies",
    fields: [{ name: "company_id", type: "int" }],
    tables: [{
      name: "employees",
      fields: [
        { name: "name", type: "text" },
        { name: "department", type: "text" },
        { name: "salary", type: "int" },
        { name: "age", type: "int" },
      ]
    }]
  })
  enchu.init(CACHE_DIR, SCHEMA, 'host=localhost')

  const pgInsertStart = performance.now()

  for (let cid = 1; cid <= COMPANIES; cid++) {
    const employees = []
    const pgValues: string[] = []

    for (let i = 0; i < EMPLOYEES_PER; i++) {
      const name = `${NAMES[i % NAMES.length]}${cid}_${i}`
      const dept = DEPTS[i % DEPTS.length]
      const salary = 300000 + (i * 5000) + (cid % 100) * 1000
      const age = 22 + (i % 40)

      employees.push({ name, department: dept, salary, age })
      pgValues.push(`(${cid}, '${name}', '${dept}', ${salary}, ${age})`)
    }

    // PGにバッチINSERT
    await pg.query(
      `INSERT INTO employees (company_id, name, department, salary, age) VALUES ${pgValues.join(',')}`
    )

    // Enchuにput
    enchu.put(JSON.stringify({
      key: `company_${cid}`,
      axes: { company_id: String(cid) },
      tables: { employees }
    }))

    if (cid % 200 === 0) {
      console.log(`  ${cid} / ${COMPANIES}`)
    }
  }

  enchu.sync()
  const loadTime = performance.now() - pgInsertStart
  console.log(`投入完了: ${(loadTime / 1000).toFixed(1)}s\n`)

  // PG行数確認
  const countRes = await pg.query('SELECT COUNT(*) FROM employees')
  console.log(`PG: ${parseInt(countRes.rows[0].count).toLocaleString()}行`)
  console.log(`Enchu: ${TOTAL_ROWS.toLocaleString()}行\n`)

  const WARMUP = 100
  const ITERS = parseInt(process.env.ITERS || '1000')

  // ======================================
  // Phase 1: インデックスなし
  // ======================================
  console.log('=' .repeat(60))
  console.log('Phase 1: インデックスなし')
  console.log('=' .repeat(60))
  console.log()

  interface Result { name: string; enchuNs: number; pgNs: number; speedup: number }
  const noIdxResults: Result[] = []

  // 1. company_id検索 (インデックスなし)
  {
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', '500')
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) enchu.lookup('company_id', '500')
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query('SELECT * FROM employees WHERE company_id = $1', [500])
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query('SELECT * FROM employees WHERE company_id = $1', [500])
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    noIdxResults.push({ name: 'ID検索 (company_id=500)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 2. 名前部分一致
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.name.includes('田中'))
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.name.includes('田中'))
    }
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query("SELECT * FROM employees WHERE company_id = 1 AND name LIKE '%田中%'")
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query("SELECT * FROM employees WHERE company_id = 1 AND name LIKE '%田中%'")
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    noIdxResults.push({ name: '部分一致 (LIKE 田中)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 3. 給与範囲
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.salary >= 400000 && e.salary <= 500000)
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.salary >= 400000 && e.salary <= 500000)
    }
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query('SELECT * FROM employees WHERE company_id = 1 AND salary BETWEEN 400000 AND 500000')
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query('SELECT * FROM employees WHERE company_id = 1 AND salary BETWEEN 400000 AND 500000')
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    noIdxResults.push({ name: '数値範囲 (給与40-50万)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 4. 部署で絞り+給与ソート+TOP5
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
    for (let i = 0; i < ITERS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees
        .filter((e: any) => e.department === '開発' && e.age >= 30)
        .sort((a: any, b: any) => b.salary - a.salary)
        .slice(0, 5)
    }
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) {
      await pg.query("SELECT * FROM employees WHERE company_id = 1 AND department = '開発' AND age >= 30 ORDER BY salary DESC LIMIT 5")
    }
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query("SELECT * FROM employees WHERE company_id = 1 AND department = '開発' AND age >= 30 ORDER BY salary DESC LIMIT 5")
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    noIdxResults.push({ name: '複合 (部署+年齢+TOP5)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 5. 全社横断検索 (インデックスなしの真骨頂)
  {
    // Enchu: ネストフィールド完全一致
    const searchName = `田中500_0`
    for (let i = 0; i < WARMUP; i++) enchu.lookup('employees.name', searchName)
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) enchu.lookup('employees.name', searchName)
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    // PG: インデックスなしで name = 'xxx' (10万行フルスキャン)
    for (let i = 0; i < 100; i++) await pg.query("SELECT * FROM employees WHERE name = $1", [searchName])
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query("SELECT * FROM employees WHERE name = $1", [searchName])
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    noIdxResults.push({ name: '全社横断 (name=xxx)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 6. ミス
  {
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', '999999')
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) enchu.lookup('company_id', '999999')
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query('SELECT * FROM employees WHERE company_id = $1', [999999])
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query('SELECT * FROM employees WHERE company_id = $1', [999999])
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    noIdxResults.push({ name: 'ミス', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  printResults('インデックスなし', noIdxResults)

  // ======================================
  // Phase 2: インデックスあり
  // ======================================
  console.log('\n' + '=' .repeat(60))
  console.log('Phase 2: FK(company_id)のみインデックス')
  console.log('=' .repeat(60))
  console.log()

  console.log('CREATE INDEX (PKのみ = 現実的)...')
  await pg.query('CREATE INDEX idx_emp_company ON employees (company_id)')
  await pg.query('ANALYZE employees')
  console.log('Done.\n')

  const idxResults: Result[] = []

  // 同じベンチを再実行
  // 1. company_id検索
  {
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', '500')
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) enchu.lookup('company_id', '500')
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query('SELECT * FROM employees WHERE company_id = $1', [500])
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query('SELECT * FROM employees WHERE company_id = $1', [500])
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    idxResults.push({ name: 'ID検索 (company_id=500)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 2. 名前部分一致 (インデックスあってもLIKE '%xxx%'は効かない)
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.name.includes('田中'))
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.name.includes('田中'))
    }
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query("SELECT * FROM employees WHERE company_id = 1 AND name LIKE '%田中%'")
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query("SELECT * FROM employees WHERE company_id = 1 AND name LIKE '%田中%'")
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    idxResults.push({ name: '部分一致 (LIKE 田中)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 3. 給与範囲
  {
    for (let i = 0; i < WARMUP; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.salary >= 400000 && e.salary <= 500000)
    }
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees.filter((e: any) => e.salary >= 400000 && e.salary <= 500000)
    }
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query('SELECT * FROM employees WHERE company_id = 1 AND salary BETWEEN 400000 AND 500000')
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query('SELECT * FROM employees WHERE company_id = 1 AND salary BETWEEN 400000 AND 500000')
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    idxResults.push({ name: '数値範囲 (給与40-50万)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 4. 複合
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
    for (let i = 0; i < ITERS; i++) {
      const r = enchu.lookup('company_id', '1')!
      const d = JSON.parse(r)
      d.tables.employees
        .filter((e: any) => e.department === '開発' && e.age >= 30)
        .sort((a: any, b: any) => b.salary - a.salary)
        .slice(0, 5)
    }
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) {
      await pg.query("SELECT * FROM employees WHERE company_id = 1 AND department = '開発' AND age >= 30 ORDER BY salary DESC LIMIT 5")
    }
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query("SELECT * FROM employees WHERE company_id = 1 AND department = '開発' AND age >= 30 ORDER BY salary DESC LIMIT 5")
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    idxResults.push({ name: '複合 (部署+年齢+TOP5)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 5. 全社横断 (nameインデックスあり)
  {
    const searchName = `田中500_0`
    for (let i = 0; i < WARMUP; i++) enchu.lookup('employees.name', searchName)
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) enchu.lookup('employees.name', searchName)
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query("SELECT * FROM employees WHERE name = $1", [searchName])
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query("SELECT * FROM employees WHERE name = $1", [searchName])
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    idxResults.push({ name: '全社横断 (name=xxx)', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  // 6. ミス
  {
    for (let i = 0; i < WARMUP; i++) enchu.lookup('company_id', '999999')
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) enchu.lookup('company_id', '999999')
    const enchuNs = (performance.now() - t1) / ITERS * 1_000_000

    for (let i = 0; i < 100; i++) await pg.query('SELECT * FROM employees WHERE company_id = $1', [999999])
    const t2 = performance.now()
    for (let i = 0; i < ITERS; i++) {
      await pg.query('SELECT * FROM employees WHERE company_id = $1', [999999])
    }
    const pgNs = (performance.now() - t2) / ITERS * 1_000_000

    idxResults.push({ name: 'ミス', enchuNs, pgNs, speedup: pgNs / enchuNs })
  }

  printResults('インデックスあり', idxResults)

  // クリーンアップ
  await pg.query('DROP TABLE employees')
  await pg.end()
}

function printResults(label: string, results: { name: string; enchuNs: number; pgNs: number; speedup: number }[]) {
  console.log(`\n[${label}]`)
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
  console.log(`平均: Enchu ${formatNs(avgEnchu)} / PG ${formatNs(avgPg)} → ${(avgPg / avgEnchu).toFixed(0)}x`)
}

main().catch(console.error)
