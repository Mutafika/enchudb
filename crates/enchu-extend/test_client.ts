/**
 * client.ts のテスト
 *
 * db.products.filter({ color: 'red' }) が実データを返すか
 */
import { Client } from 'pg'
import { enchu } from './src/client'

const PG_CONN = 'postgresql://postgres:enchu@localhost:5433/enchu_bench'

let passed = 0
let failed = 0

function assert(name: string, condition: boolean, detail?: string) {
  if (condition) {
    console.log(`  ✓ ${name}`)
    passed++
  } else {
    console.log(`  ✗ ${name}${detail ? ` — ${detail}` : ''}`)
    failed++
  }
}

async function main() {
  console.log('╔══════════════════════════════════════════════════════════════╗')
  console.log('║  client.ts テスト — db.table.filter() API                  ║')
  console.log('╚══════════════════════════════════════════════════════════════╝\n')

  // Pgにテストデータ投入
  const pg = new Client({ connectionString: PG_CONN })
  await pg.connect()

  await pg.query('DROP TABLE IF EXISTS items CASCADE')
  await pg.query(`
    CREATE TABLE items (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      color TEXT NOT NULL,
      size TEXT NOT NULL,
      price INT NOT NULL,
      category TEXT NOT NULL
    )
  `)

  const items = [
    ['赤Tシャツ',   'red',   'M', 2000, 'shirt'],
    ['青Tシャツ',   'blue',  'M', 2500, 'shirt'],
    ['赤Tシャツ L', 'red',   'L', 2200, 'shirt'],
    ['黒パンツ',    'black', 'M', 3000, 'pants'],
    ['赤パンツ',    'red',   'L', 3500, 'pants'],
    ['青帽子',      'blue',  'S', 1000, 'hat'],
    ['赤帽子',      'red',   'S', 1200, 'hat'],
    ['黒帽子',      'black', 'M', 1500, 'hat'],
  ]
  for (const [name, color, size, price, category] of items) {
    await pg.query(
      'INSERT INTO items (name, color, size, price, category) VALUES ($1, $2, $3, $4, $5)',
      [name, color, size, price, category]
    )
  }
  await pg.end()

  // Enchu初期化
  const db = await enchu(PG_CONN, { syncInterval: 0, tables: ['items'] })

  // ── filter テスト ──
  console.log('  filter:')

  const reds = db.items.filter({ color: 'red' })
  assert('color=red → 4件', reds.length === 4, `got ${reds.length}`)

  const redShirts = db.items.filter({ color: 'red', category: 'shirt' })
  assert('color=red, category=shirt → 2件', redShirts.length === 2, `got ${redShirts.length}`)

  const blueM = db.items.filter({ color: 'blue', size: 'M' })
  assert('color=blue, size=M → 1件', blueM.length === 1, `got ${blueM.length}`)
  if (blueM.length === 1) {
    assert('  name=青Tシャツ', blueM[0].name === '青Tシャツ', `got ${blueM[0].name}`)
    assert('  price=2500', blueM[0].price === 2500, `got ${blueM[0].price}`)
  }

  const noMatch = db.items.filter({ color: 'green' })
  assert('存在しない値 → 0件', noMatch.length === 0, `got ${noMatch.length}`)

  // ── count テスト ──
  console.log('\n  count:')

  const redCount = db.items.count({ color: 'red' })
  assert('count color=red → 4', redCount === 4, `got ${redCount}`)

  const shirtCount = db.items.count({ category: 'shirt' })
  assert('count category=shirt → 3', shirtCount === 3, `got ${shirtCount}`)

  const noCount = db.items.count({ color: 'green' })
  assert('count 存在しない → 0', noCount === 0, `got ${noCount}`)

  // ── limit/offset テスト ──
  console.log('\n  limit/offset:')

  const limited = db.items.filter({ color: 'red' }, { limit: 2 })
  assert('limit=2 → 2件', limited.length === 2, `got ${limited.length}`)

  const offsetted = db.items.filter({ color: 'red' }, { offset: 1, limit: 2 })
  assert('offset=1, limit=2 → 2件', offsetted.length === 2, `got ${offsetted.length}`)

  // ── all テスト ──
  console.log('\n  all:')

  const all = db.items.all()
  assert('all() → 8件', all.length === 8, `got ${all.length}`)

  const allLimited = db.items.all({ limit: 3 })
  assert('all(limit=3) → 3件', allLimited.length === 3, `got ${allLimited.length}`)

  // ── 実データの中身確認 ──
  console.log('\n  実データ:')

  const hats = db.items.filter({ category: 'hat' })
  assert('hat → 3件', hats.length === 3, `got ${hats.length}`)

  const hatNames = hats.map((h: any) => h.name).sort()
  assert('hat names 正しい',
    JSON.stringify(hatNames) === JSON.stringify(['赤帽子', '青帽子', '黒帽子']),
    `got ${JSON.stringify(hatNames)}`)

  // cleanup
  await (db as any).close()
  const pgClean = new Client({ connectionString: PG_CONN })
  await pgClean.connect()
  await pgClean.query('DROP TABLE IF EXISTS items CASCADE')
  await pgClean.end()

  const { rmSync } = await import('fs')
  try { rmSync('/tmp/enchu_cache_' + '*', { recursive: true, force: true }) } catch {}

  console.log(`\n  ════════════════════════════════════`)
  console.log(`  結果: ${passed} passed, ${failed} failed\n`)
  process.exit(failed > 0 ? 1 : 0)
}

main().catch(console.error)
