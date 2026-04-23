// Enchu Extend v3 vs PostgreSQL — 全検索パターン比較
// PGデータ: enchu_bench (5テーブル × 500万行, 10万エンティティ × 50行)

import * as enchu from './native'

const CACHE_DIR = '/tmp/enchu_extend_bench'
const ENTITIES = 1000  // PGから読み込むエンティティ数

function pgQuery(sql: string): string {
  const { execSync } = require('child_process')
  return execSync(
    `docker exec enchu-postgres psql -U postgres -d enchu_bench -t -A -c "${sql}"`,
    { encoding: 'utf-8', maxBuffer: 100 * 1024 * 1024 }
  )
}

function bench(warmup: number, iters: number, fn: () => void): number {
  for (let i = 0; i < warmup; i++) fn()
  const t = performance.now()
  for (let i = 0; i < iters; i++) fn()
  return (performance.now() - t) / iters * 1_000_000 // ns
}

function fmt(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) fs.rmSync(CACHE_DIR, { recursive: true })

  console.log('╔══════════════════════════════════════════════════════════════╗')
  console.log('║  Enchu Extend v3 vs PostgreSQL                             ║')
  console.log(`║  ${ENTITIES} entities × 50 members each                         ║`)
  console.log('╚══════════════════════════════════════════════════════════════╝\n')

  // Schema
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

  // Init Enchu
  enchu.init(CACHE_DIR, SCHEMA, '')

  // Load from PG
  console.log(`Loading ${ENTITIES} entities from PG...`)
  const t0 = performance.now()

  for (let eid = 0; eid < ENTITIES; eid++) {
    const cid = enchu.insert('entities')
    enchu.writeInt('entities', cid, 'entity_id', eid)

    // members
    const membersRaw = pgQuery(
      `SELECT text_val, int_val FROM members WHERE entity_id = ${eid}`
    )
    for (const line of membersRaw.trim().split('\n').filter(Boolean)) {
      const [text, intStr] = line.split('|')
      const mid = enchu.insertChild('members', cid)
      enchu.writeText('members', mid, 'text_val', text || '')
      enchu.writeInt('members', mid, 'int_val', parseInt(intStr) || 0)
    }

    // tasks
    const tasksRaw = pgQuery(
      `SELECT text_val, int_val FROM tasks WHERE entity_id = ${eid}`
    )
    for (const line of tasksRaw.trim().split('\n').filter(Boolean)) {
      const [text, intStr] = line.split('|')
      const tid = enchu.insertChild('tasks', cid)
      enchu.writeText('tasks', tid, 'text_val', text || '')
      enchu.writeInt('tasks', tid, 'int_val', parseInt(intStr) || 0)
    }
  }

  enchu.flush()
  const loadTime = performance.now() - t0
  const memberCount = enchu.count('members')
  const taskCount = enchu.count('tasks')
  console.log(`Loaded in ${(loadTime / 1000).toFixed(1)}s (members: ${memberCount}, tasks: ${taskCount})\n`)

  const W = 500
  const R = 10000
  const PG_R = 100

  console.log('  ┌──────────────────────────┬──────────┬──────────┬──────────┐')
  console.log('  │ クエリ                   │       PG │   Extend │     倍率 │')
  console.log('  ├──────────────────────────┼──────────┼──────────┼──────────┤')

  // 1. 完全一致 (entity_id)
  const pg1 = bench(10, PG_R, () => {
    pgQuery("SELECT * FROM members WHERE entity_id = 42 LIMIT 1")
  })
  const en1 = bench(W, R, () => {
    enchu.slice('entities', 'entity_id', 42)
  })
  row('完全一致(entity_id)', pg1, en1)

  // 2. 完全一致 (text, 非ユニーク)
  const sampleText = `entity_0_members_row_0`
  const textVid = enchu.vocabLookup(sampleText)
  const pg2 = bench(10, PG_R, () => {
    pgQuery(`SELECT * FROM members WHERE text_val = '${sampleText}'`)
  })
  const en2 = textVid != null ? bench(W, R, () => {
    enchu.slice('members', 'text_val', textVid!)
  }) : 0
  row('完全一致(text_val)', pg2, en2)

  // 3. 範囲検索 (int_val 100-200)
  const pg3 = bench(10, PG_R, () => {
    pgQuery("SELECT count(*) FROM members WHERE int_val BETWEEN 100 AND 200")
  })
  const en3 = bench(W, R, () => {
    enchu.sliceRange('members', 'int_val', 100, 200)
  })
  row('範囲(int 100-200)', pg3, en3)

  // 4. 親子辿り (member → entity)
  const pg4 = bench(10, PG_R, () => {
    pgQuery("SELECT e.entity_id FROM members m JOIN members e ON m.entity_id = e.entity_id WHERE m.row_id = 0 AND m.entity_id = 42")
  })
  const en4 = bench(W, R, () => {
    enchu.parent('members', 42)
  })
  row('親辿り(parent)', pg4, en4)

  // 5. 子一覧 (entity → members)
  const pg5 = bench(10, PG_R, () => {
    pgQuery("SELECT * FROM members WHERE entity_id = 42")
  })
  const en5 = bench(W, R, () => {
    enchu.children('members', 42)
  })
  row('子一覧(children)', pg5, en5)

  // 6. カラム読み (int_valを取得)
  const kidsBuf = enchu.children('members', 42)
  const pg6 = bench(10, PG_R, () => {
    pgQuery("SELECT int_val FROM members WHERE entity_id = 42")
  })
  const en6 = bench(W, R, () => {
    enchu.getColumnInt('members', 'int_val', kidsBuf)
  })
  row('カラム読み(int×50)', pg6, en6)

  // 7. 複合条件 (entity_id=42のmembersでint_val > 300)
  const pg7 = bench(10, PG_R, () => {
    pgQuery("SELECT count(*) FROM members WHERE entity_id = 42 AND int_val > 300")
  })
  const en7 = bench(W, R, () => {
    const kids = enchu.children('members', 42)
    const ints = enchu.getColumnInt('members', 'int_val', kids)
    const intArr = new BigInt64Array(ints.buffer, ints.byteOffset, ints.byteLength / 8)
    let count = 0
    for (const v of intArr) if (v > 300n) count++
  })
  row('複合(eid+int>300)', pg7, en7)

  console.log('  └──────────────────────────┴──────────┴──────────┴──────────┘')
  console.log('\n  Done.')
}

function row(label: string, pg: number, en: number) {
  const speedup = en > 0 ? `${(pg / en).toFixed(0)}x` : 'N/A'
  console.log(`  │ ${label.padEnd(24)} │ ${fmt(pg).padStart(8)} │ ${fmt(en).padStart(8)} │ ${speedup.padStart(8)} │`)
}

main().catch(console.error)
