/**
 * スペックベンチ — 初回同期・差分同期・メモリ・ディスク使用量
 */
import { Client } from 'pg'

const enchu = require('./enchu-extend.node')

const PG_CONN = 'postgresql://postgres:enchu@localhost:5433/enchu_bench'
const CACHE_DIR = '/tmp/enchu_specs_bench'

async function main() {
  console.log('╔══════════════════════════════════════════════════════════════╗')
  console.log('║  スペックベンチ — 同期速度・メモリ・ディスク               ║')
  console.log('╚══════════════════════════════════════════════════════════════╝\n')

  const pg = new Client({ connectionString: PG_CONN })
  await pg.connect()
  const fs = await import('fs')
  const path = await import('path')

  // ══════ 1. 初回同期速度（件数別）══════
  console.log('  [1] 初回同期速度（Pg → Enchu）')
  console.log('  ┌──────────────┬──────────┬──────────────┬──────────┐')
  console.log('  │ 件数         │ Pgロード │ Enchu書込    │ 合計     │')
  console.log('  ├──────────────┼──────────┼──────────────┼──────────┤')

  for (const count of [10_000, 100_000, 1_000_000]) {
    // Pgセットアップ
    await pg.query('DROP TABLE IF EXISTS bench_items CASCADE')
    await pg.query(`
      CREATE TABLE bench_items (
        id SERIAL PRIMARY KEY,
        tenant_id INT NOT NULL,
        category INT NOT NULL,
        color INT NOT NULL,
        size INT NOT NULL,
        price INT NOT NULL
      )
    `)
    const BATCH = 1000
    for (let i = 0; i < count; i += BATCH) {
      const values: string[] = []
      for (let j = 0; j < BATCH && i + j < count; j++) {
        const id = i + j
        values.push(`(${id % 100}, ${id % 20}, ${id % 10}, ${id % 5}, ${id % 10000})`)
      }
      await pg.query(`INSERT INTO bench_items (tenant_id, category, color, size, price) VALUES ${values.join(',')}`)
    }

    // Enchuセットアップ
    const dir = `${CACHE_DIR}_${count}`
    fs.rmSync(dir, { recursive: true, force: true })
    const schema = JSON.stringify({
      name: 'bench_items',
      fields: [
        { name: 'tenant_id', type: 'int' },
        { name: 'category', type: 'int' },
        { name: 'color', type: 'int' },
        { name: 'size', type: 'int' },
        { name: 'price', type: 'int' },
      ],
      tables: [],
    })

    // OnceLock問題回避: initNamed使用
    enchu.init(dir, schema, PG_CONN)

    // Pgからロード
    const tPg = Date.now()
    const rows = await pg.query(
      `SELECT tenant_id, category, color, size, price FROM bench_items ORDER BY id`
    )
    const pgLoadMs = Date.now() - tPg

    // Enchuに書き込み
    const tEnchu = Date.now()
    for (const row of rows.rows) {
      const eid = enchu.insert('bench_items')
      enchu.writeInt('bench_items', eid, 'tenant_id', row.tenant_id)
      enchu.writeInt('bench_items', eid, 'category', row.category)
      enchu.writeInt('bench_items', eid, 'color', row.color)
      enchu.writeInt('bench_items', eid, 'size', row.size)
      enchu.writeInt('bench_items', eid, 'price', row.price)
    }
    enchu.flush()
    const enchuWriteMs = Date.now() - tEnchu

    const totalMs = pgLoadMs + enchuWriteMs
    console.log(`  │ ${count.toLocaleString().padStart(12)} │ ${(pgLoadMs / 1000).toFixed(1).padStart(7)}s │ ${(enchuWriteMs / 1000).toFixed(1).padStart(11)}s │ ${(totalMs / 1000).toFixed(1).padStart(7)}s │`)

    // ディスク使用量
    const diskBytes = getDirSize(dir)
    const diskMB = (diskBytes / 1024 / 1024).toFixed(1)

    // 件数別ディスク使用量を記録
    if (count === 1_000_000) {
      // 最後に表示
    }

    fs.rmSync(dir, { recursive: true, force: true })
  }
  console.log('  └──────────────┴──────────┴──────────────┴──────────┘')

  // ══════ 2. ディスク使用量 ══════
  console.log('\n  [2] ディスク使用量')
  console.log('  ┌──────────────┬──────────────┬──────────────┐')
  console.log('  │ 件数         │ Enchu (disk) │ 1件あたり    │')
  console.log('  ├──────────────┼──────────────┼──────────────┤')

  for (const count of [10_000, 100_000, 1_000_000]) {
    await pg.query('DROP TABLE IF EXISTS bench_items CASCADE')
    await pg.query(`
      CREATE TABLE bench_items (
        id SERIAL PRIMARY KEY,
        tenant_id INT, category INT, color INT, size INT, price INT
      )
    `)
    const BATCH = 1000
    for (let i = 0; i < count; i += BATCH) {
      const values: string[] = []
      for (let j = 0; j < BATCH && i + j < count; j++) {
        const id = i + j
        values.push(`(${id % 100}, ${id % 20}, ${id % 10}, ${id % 5}, ${id % 10000})`)
      }
      await pg.query(`INSERT INTO bench_items (tenant_id, category, color, size, price) VALUES ${values.join(',')}`)
    }

    const dir = `${CACHE_DIR}_disk_${count}`
    fs.rmSync(dir, { recursive: true, force: true })
    const schema = JSON.stringify({
      name: 'bench_items',
      fields: [
        { name: 'tenant_id', type: 'int' }, { name: 'category', type: 'int' },
        { name: 'color', type: 'int' }, { name: 'size', type: 'int' },
        { name: 'price', type: 'int' },
      ],
      tables: [],
    })
    enchu.init(dir, schema, PG_CONN)

    const rows = await pg.query(`SELECT tenant_id, category, color, size, price FROM bench_items ORDER BY id`)
    for (const row of rows.rows) {
      const eid = enchu.insert('bench_items')
      enchu.writeInt('bench_items', eid, 'tenant_id', row.tenant_id)
      enchu.writeInt('bench_items', eid, 'category', row.category)
      enchu.writeInt('bench_items', eid, 'color', row.color)
      enchu.writeInt('bench_items', eid, 'size', row.size)
      enchu.writeInt('bench_items', eid, 'price', row.price)
    }
    enchu.flush()

    const diskBytes = getDirSize(dir)
    const diskMB = diskBytes / 1024 / 1024
    const perRow = diskBytes / count

    console.log(`  │ ${count.toLocaleString().padStart(12)} │ ${diskMB.toFixed(1).padStart(9)}MB │ ${perRow.toFixed(0).padStart(8)}B │`)

    fs.rmSync(dir, { recursive: true, force: true })
  }
  console.log('  └──────────────┴──────────────┴──────────────┘')

  // ══════ 3. 差分同期レイテンシ ══════
  console.log('\n  [3] 差分同期レイテンシ（件数別）')
  console.log('  ┌──────────────┬──────────────┬──────────────┐')
  console.log('  │ 差分件数     │ 書込+flush   │ 1件あたり    │')
  console.log('  ├──────────────┼──────────────┼──────────────┤')

  // 100万件ベースのエンジンを作って差分を入れる
  await pg.query('DROP TABLE IF EXISTS bench_items CASCADE')
  await pg.query(`
    CREATE TABLE bench_items (
      id SERIAL PRIMARY KEY,
      tenant_id INT, category INT, color INT, size INT, price INT
    )
  `)
  for (let i = 0; i < 1_000_000; i += 1000) {
    const values: string[] = []
    for (let j = 0; j < 1000 && i + j < 1_000_000; j++) {
      const id = i + j
      values.push(`(${id % 100}, ${id % 20}, ${id % 10}, ${id % 5}, ${id % 10000})`)
    }
    await pg.query(`INSERT INTO bench_items (tenant_id, category, color, size, price) VALUES ${values.join(',')}`)
  }

  const syncDir = `${CACHE_DIR}_sync`
  fs.rmSync(syncDir, { recursive: true, force: true })
  const syncSchema = JSON.stringify({
    name: 'bench_items',
    fields: [
      { name: 'tenant_id', type: 'int' }, { name: 'category', type: 'int' },
      { name: 'color', type: 'int' }, { name: 'size', type: 'int' },
      { name: 'price', type: 'int' },
    ],
    tables: [],
  })
  enchu.init(syncDir, syncSchema, PG_CONN)

  const rows = await pg.query(`SELECT tenant_id, category, color, size, price FROM bench_items ORDER BY id`)
  for (const row of rows.rows) {
    const eid = enchu.insert('bench_items')
    enchu.writeInt('bench_items', eid, 'tenant_id', row.tenant_id)
    enchu.writeInt('bench_items', eid, 'category', row.category)
    enchu.writeInt('bench_items', eid, 'color', row.color)
    enchu.writeInt('bench_items', eid, 'size', row.size)
    enchu.writeInt('bench_items', eid, 'price', row.price)
  }
  enchu.flush()

  for (const diffCount of [10, 100, 1000, 10000]) {
    const t = Date.now()
    for (let i = 0; i < diffCount; i++) {
      const eid = Math.floor(Math.random() * 1_000_000)
      enchu.writeInt('bench_items', eid, 'price', Math.floor(Math.random() * 10000))
    }
    enchu.flush()
    const elapsed = Date.now() - t
    const perRow = elapsed / diffCount

    console.log(`  │ ${diffCount.toLocaleString().padStart(12)} │ ${elapsed.toFixed(0).padStart(9)}ms │ ${(perRow * 1000).toFixed(0).padStart(8)}µs │`)
  }
  console.log('  └──────────────┴──────────────┴──────────────┘')

  // ══════ 4. メモリ使用量 ══════
  console.log('\n  [4] プロセスメモリ使用量')
  const mem = process.memoryUsage()
  console.log(`      RSS:       ${(mem.rss / 1024 / 1024).toFixed(0)}MB`)
  console.log(`      Heap Used: ${(mem.heapUsed / 1024 / 1024).toFixed(0)}MB`)
  console.log(`      Heap Total:${(mem.heapTotal / 1024 / 1024).toFixed(0)}MB`)
  console.log(`      External:  ${(mem.external / 1024 / 1024).toFixed(0)}MB`)

  // cleanup
  await pg.query('DROP TABLE IF EXISTS bench_items CASCADE')
  await pg.end()
  for (const p of [syncDir]) {
    fs.rmSync(p, { recursive: true, force: true })
  }

  console.log('\n  Done.')
}

function getDirSize(dir: string): number {
  const fs = require('fs')
  const path = require('path')
  let total = 0
  try {
    const entries = fs.readdirSync(dir, { withFileTypes: true, recursive: true })
    for (const entry of entries) {
      if (entry.isFile()) {
        const fullPath = path.join(entry.parentPath || entry.path || dir, entry.name)
        try { total += fs.statSync(fullPath).size } catch {}
      }
    }
  } catch {}
  return total
}

main().catch(console.error)
