// Enchu Extend v3 (v20 Cylinder) vs PostgreSQL
// Usage: npx tsx test_v20.ts

import * as enchu from './native'

const CACHE_DIR = '/tmp/enchu_extend_v20_test'
const COMPANIES = 1000
const EMPS_PER = 100

const SCHEMA = JSON.stringify({
  name: "companies",
  fields: [
    { name: "name", type: "symbol" },
    { name: "city", type: "symbol" },
  ],
  tables: [
    {
      name: "employees",
      fields: [
        { name: "name", type: "symbol" },
        { name: "age", type: "int" },
      ]
    }
  ]
})

const cities = ["東京","大阪","名古屋","福岡","札幌","仙台","広島","京都","神戸","横浜"]

function pgQuery(sql: string): string {
  const { execSync } = require('child_process')
  return execSync(
    `docker exec enchu-postgres psql -U postgres -d enchu_bench -t -A -c "${sql}"`,
    { encoding: 'utf-8', maxBuffer: 100 * 1024 * 1024 }
  )
}

async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) {
    fs.rmSync(CACHE_DIR, { recursive: true })
  }

  console.log('=== Enchu Extend v3 (v20 Cylinder) ===\n')

  // Init
  console.log('Initializing...')
  enchu.init(CACHE_DIR, SCHEMA, '')

  // Load data
  const t0 = performance.now()
  for (let c = 0; c < COMPANIES; c++) {
    const cid = enchu.insert('companies')
    enchu.writeText('companies', cid, 'name', `company_${c}`)
    enchu.writeText('companies', cid, 'city', cities[c % 10])

    for (let e = 0; e < EMPS_PER; e++) {
      const eid = enchu.insertChild('employees', cid)
      enchu.writeText('employees', eid, 'name', `emp_${c}_${e}`)
      enchu.writeInt('employees', eid, 'age', 20 + (e % 50))
    }
  }
  enchu.flush() // Cylinder rebuild
  const loadTime = performance.now() - t0
  const total = COMPANIES + COMPANIES * EMPS_PER
  console.log(`Loaded ${total} rows in ${loadTime.toFixed(0)}ms\n`)

  // Vocab lookup
  const tokyoVid = enchu.vocabLookup('東京')
  console.log(`vocab "東京" → ${tokyoVid}\n`)

  const WARMUP = 1000
  const ITERS = 50000

  // === 1. Cylinder slice (city = 東京) ===
  if (tokyoVid != null) {
    for (let i = 0; i < WARMUP; i++) enchu.slice('companies', 'city', tokyoVid)
    const t1 = performance.now()
    for (let i = 0; i < ITERS; i++) enchu.slice('companies', 'city', tokyoVid)
    const sliceNs = (performance.now() - t1) / ITERS * 1_000_000
    const buf = enchu.slice('companies', 'city', tokyoVid)
    const ids = new Uint32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4)
    console.log(`slice(city=東京):     ${sliceNs.toFixed(0)}ns  (${ids.length} companies)`)
  }

  // === 2. Range slice (age 25-35) ===
  for (let i = 0; i < WARMUP; i++) enchu.sliceRange('employees', 'age', 25, 35)
  const t2 = performance.now()
  for (let i = 0; i < ITERS; i++) enchu.sliceRange('employees', 'age', 25, 35)
  const rangeNs = (performance.now() - t2) / ITERS * 1_000_000
  const ageBuf = enchu.sliceRange('employees', 'age', 25, 35)
  const ageIds = new Uint32Array(ageBuf.buffer, ageBuf.byteOffset, ageBuf.byteLength / 4)
  console.log(`sliceRange(age 25-35): ${rangeNs.toFixed(0)}ns  (${ageIds.length} employees)`)

  // === 3. getColumnInt ===
  const sampleBuf = enchu.sliceRange('employees', 'age', 30, 30)
  for (let i = 0; i < WARMUP; i++) enchu.getColumnInt('employees', 'age', sampleBuf)
  const t3 = performance.now()
  for (let i = 0; i < ITERS; i++) enchu.getColumnInt('employees', 'age', sampleBuf)
  const getIntNs = (performance.now() - t3) / ITERS * 1_000_000
  const sampleCount = sampleBuf.byteLength / 4
  console.log(`getColumnInt(${sampleCount}件):  ${getIntNs.toFixed(0)}ns`)

  // === 4. getColumnText ===
  const smallBuf = Buffer.alloc(40)
  for (let i = 0; i < 10; i++) smallBuf.writeUInt32LE(i, i * 4)
  const smallIds = new Uint32Array(smallBuf.buffer, smallBuf.byteOffset, 10)
  for (let i = 0; i < WARMUP; i++) enchu.getColumnText('employees', 'name', smallIds)
  const t4 = performance.now()
  for (let i = 0; i < ITERS; i++) enchu.getColumnText('employees', 'name', smallIds)
  const getTextNs = (performance.now() - t4) / ITERS * 1_000_000
  console.log(`getColumnText(10件):   ${getTextNs.toFixed(0)}ns`)

  // === 5. parent ===
  for (let i = 0; i < WARMUP; i++) enchu.parent('employees', 42)
  const t5 = performance.now()
  for (let i = 0; i < ITERS; i++) enchu.parent('employees', 42)
  const parentNs = (performance.now() - t5) / ITERS * 1_000_000
  console.log(`parent:                ${parentNs.toFixed(0)}ns`)

  // === 6. children ===
  for (let i = 0; i < WARMUP; i++) enchu.children('employees', 42)
  const t6 = performance.now()
  for (let i = 0; i < ITERS; i++) enchu.children('employees', 42)
  const childrenNs = (performance.now() - t6) / ITERS * 1_000_000
  const kids = enchu.children('employees', 42)
  console.log(`children(${kids.length}件):       ${childrenNs.toFixed(0)}ns`)

  console.log('\nDone.')
}

main().catch(console.error)
