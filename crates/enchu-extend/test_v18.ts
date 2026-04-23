// Enchu Extend v18 vs PostgreSQL — lookup speed test
// Usage: npx tsx test_v18.ts

import * as enchu from './native'

const CACHE_DIR = '/tmp/enchu_extend_v18_test'
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

// --- Load from PG via docker exec ---
function pgQuery(sql: string): string {
  const { execSync } = require('child_process')
  return execSync(
    `docker exec enchu-postgres psql -U postgres -d enchu_bench -t -A -c "${sql}"`,
    { encoding: 'utf-8', maxBuffer: 100 * 1024 * 1024 }
  )
}

// --- Main ---
async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) {
    fs.rmSync(CACHE_DIR, { recursive: true })
  }

  console.log('=== Enchu Extend v18 vs PostgreSQL ===\n')

  // Init Enchu
  console.log('Initializing Enchu v18...')
  enchu.init(CACHE_DIR, SCHEMA, PG_CONN)

  // Load subset of data (100 entities)
  const ENTITIES = 100
  console.log(`Loading ${ENTITIES} entities from PG...`)

  const t0 = performance.now()

  for (let eid = 1; eid <= ENTITIES; eid++) {
    // Fetch members for this entity
    const membersRaw = pgQuery(
      `SELECT text_val, float_val, int_val FROM members WHERE entity_id = ${eid} LIMIT 50`
    )
    const members = membersRaw.trim().split('\n').filter(Boolean).map(line => {
      const [text_val, float_val, int_val] = line.split('|')
      return {
        text_val: text_val || '',
        float_val: parseFloat(float_val) || 0,
        int_val: parseInt(int_val) || 0,
      }
    })

    // Fetch tasks
    const tasksRaw = pgQuery(
      `SELECT text_val, float_val, int_val FROM tasks WHERE entity_id = ${eid} LIMIT 50`
    )
    const tasks = tasksRaw.trim().split('\n').filter(Boolean).map(line => {
      const [text_val, float_val, int_val] = line.split('|')
      return {
        text_val: text_val || '',
        float_val: parseFloat(float_val) || 0,
        int_val: parseInt(int_val) || 0,
      }
    })

    // Fetch costs
    const costsRaw = pgQuery(
      `SELECT text_val, float_val, int_val FROM costs WHERE entity_id = ${eid} LIMIT 50`
    )
    const costs = costsRaw.trim().split('\n').filter(Boolean).map(line => {
      const [text_val, float_val, int_val] = line.split('|')
      return {
        text_val: text_val || '',
        float_val: parseFloat(float_val) || 0,
        int_val: parseInt(int_val) || 0,
      }
    })

    // Put into Enchu
    enchu.put(JSON.stringify({
      key: `entity_${eid}`,
      axes: { entity_id: String(eid) },
      tables: { members, tasks, costs }
    }))
  }

  enchu.sync()
  const loadTime = performance.now() - t0
  console.log(`Loaded in ${loadTime.toFixed(0)}ms`)

  // Show fields
  console.log('\nSearchable fields:', enchu.fields())

  // === Benchmark ===
  console.log('\n=== Benchmark: lookup entity_id ===\n')

  const WARMUP = 1000
  const ITERATIONS = 50000

  // Enchu lookup
  for (let i = 0; i < WARMUP; i++) {
    enchu.lookup('entity_id', '42')
  }

  const t1 = performance.now()
  for (let i = 0; i < ITERATIONS; i++) {
    enchu.lookup('entity_id', '42')
  }
  const enchuTime = (performance.now() - t1) / ITERATIONS * 1_000_000 // ns

  // PG lookup (via docker exec — not fair comparison but shows the gap)
  const PG_ITERS = 100
  const t2 = performance.now()
  for (let i = 0; i < PG_ITERS; i++) {
    pgQuery(`SELECT * FROM members WHERE entity_id = 42 LIMIT 50`)
  }
  const pgTime = (performance.now() - t2) / PG_ITERS * 1_000_000 // ns

  console.log(`Enchu v18:  ${enchuTime.toFixed(0)} ns/lookup`)
  console.log(`PostgreSQL: ${pgTime.toFixed(0)} ns/lookup`)
  console.log(`Speedup:    ${(pgTime / enchuTime).toFixed(0)}x`)

  // Nested field lookup
  console.log('\n=== Benchmark: lookup members.text_val ===\n')

  // Get a text_val to search for
  const sample = enchu.lookup('entity_id', '1')
  if (sample) {
    const parsed = JSON.parse(sample)
    const firstMember = parsed.tables?.members?.[0]
    if (firstMember?.text_val) {
      const searchVal = firstMember.text_val

      for (let i = 0; i < WARMUP; i++) {
        enchu.lookup('members.text_val', searchVal)
      }

      const t3 = performance.now()
      for (let i = 0; i < ITERATIONS; i++) {
        enchu.lookup('members.text_val', searchVal)
      }
      const nestedTime = (performance.now() - t3) / ITERATIONS * 1_000_000

      console.log(`Search value: "${searchVal}"`)
      console.log(`Enchu v18:  ${nestedTime.toFixed(0)} ns/lookup`)
      console.log(`(PG equivalent would need JOIN or subquery)`)
    }
  }

  // Cleanup
  console.log('\nDone.')
}

main().catch(console.error)
