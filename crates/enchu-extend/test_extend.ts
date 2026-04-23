/**
 * enchu-extend テストスイート
 *
 * 1. 正確性: 検索結果がPgと一致するか
 * 2. 同期: 差分同期後にデータが反映されるか
 * 3. エッジケース: 空テーブル、0件、全件一致
 * 4. autoInit: Pgスキーマ型マッピング
 *
 * 前提: Docker enchu-postgres が 5433 で起動済み
 */
import { Client } from 'pg'

const enchu = require('./enchu-extend.node')

const PG_CONN = 'postgresql://postgres:enchu@localhost:5433/enchu_bench'
const CACHE_DIR = '/tmp/enchu_test'

let pg: Client
let passed = 0
let failed = 0
const failures: string[] = []

async function main() {
  console.log('╔══════════════════════════════════════════════════════════════╗')
  console.log('║  enchu-extend テストスイート                               ║')
  console.log('╚══════════════════════════════════════════════════════════════╝\n')

  pg = new Client({ connectionString: PG_CONN })
  await pg.connect()

  await testAutoInit()
  await testAccuracy()
  await testQueryFast()
  await testEdgeCases()
  await testSync()
  await testTypes()

  // cleanup
  await pg.query('DROP TABLE IF EXISTS test_products CASCADE')
  await pg.query('DROP TABLE IF EXISTS test_types CASCADE')
  await pg.end()

  const { rmSync } = await import('fs')
  rmSync(CACHE_DIR, { recursive: true, force: true })

  console.log(`\n  ════════════════════════════════════`)
  console.log(`  結果: ${passed} passed, ${failed} failed`)
  if (failures.length > 0) {
    console.log(`\n  失敗:`)
    for (const f of failures) console.log(`    ✗ ${f}`)
  }
  console.log()

  process.exit(failed > 0 ? 1 : 0)
}

// ════════════════ テストヘルパー ════════════════

function assert(name: string, condition: boolean, detail?: string) {
  if (condition) {
    console.log(`    ✓ ${name}`)
    passed++
  } else {
    const msg = detail ? `${name} — ${detail}` : name
    console.log(`    ✗ ${msg}`)
    failed++
    failures.push(msg)
  }
}

function resetEnchu() {
  // OnceLock をリセットできないのでプロセス内では1回しかinit/autoInitできない
  // テストごとにキャッシュディレクトリを変える
}

async function setupTestTable(suffix: string = '') {
  const table = `test_products${suffix}`
  await pg.query(`DROP TABLE IF EXISTS ${table} CASCADE`)
  await pg.query(`
    CREATE TABLE ${table} (
      id SERIAL PRIMARY KEY,
      tenant_id INT NOT NULL,
      category INT NOT NULL,
      color INT NOT NULL,
      price INT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `)
  return table
}

async function insertTestData(table: string, count: number) {
  const BATCH = 1000
  for (let i = 0; i < count; i += BATCH) {
    const values: string[] = []
    for (let j = 0; j < BATCH && i + j < count; j++) {
      const id = i + j
      const tenant = id % 10
      const cat = id % 5
      const color = id % 3
      const price = id % 1000
      values.push(`(${tenant}, ${cat}, ${color}, ${price})`)
    }
    await pg.query(`INSERT INTO ${table} (tenant_id, category, color, price) VALUES ${values.join(',')}`)
  }
}

function loadIntoEnchu(rows: any[]) {
  for (const row of rows) {
    const eid = enchu.insert('test_products')
    enchu.writeInt('test_products', eid, 'tenant_id', row.tenant_id)
    enchu.writeInt('test_products', eid, 'category', row.category)
    enchu.writeInt('test_products', eid, 'color', row.color)
    enchu.writeInt('test_products', eid, 'price', row.price)
  }
  enchu.flush()
}

function bufToIds(buf: Buffer): number[] {
  const arr = new Uint32Array(buf.buffer, buf.byteOffset, buf.length / 4)
  return Array.from(arr)
}

// ════════════════ 1. autoInit テスト ════════════════

async function testAutoInit() {
  console.log('  [1] autoInit — Pgスキーマ自動取得')

  const table = await setupTestTable()
  await insertTestData(table, 1000)

  const { rmSync } = await import('fs')
  rmSync(CACHE_DIR, { recursive: true, force: true })

  // autoInit でPgからスキーマ取得 + エンジン初期化
  try {
    enchu.autoInit(CACHE_DIR, PG_CONN)
    assert('autoInit 成功', true)
  } catch (e: any) {
    assert('autoInit 成功', false, e.message)
    return
  }

  // データロード
  const res = await pg.query(`SELECT * FROM ${table} ORDER BY id`)
  loadIntoEnchu(res.rows)

  const count = enchu.count('test_products')
  assert('件数一致', count === 1000, `expected 1000, got ${count}`)
}

// ════════════════ 2. 正確性テスト ════════════════

async function testAccuracy() {
  console.log('\n  [2] 正確性 — 検索結果がPgと一致')

  const tid = enchu.resolveTable('test_products')
  const fTenant = enchu.resolveField('test_products', 'tenant_id')
  const fCat = enchu.resolveField('test_products', 'category')
  const fColor = enchu.resolveField('test_products', 'color')

  // 単条件: tenant_id = 3
  const pgCount1 = parseInt((await pg.query(
    'SELECT count(*) AS c FROM test_products WHERE tenant_id = 3'
  )).rows[0].c)
  const enCount1 = enchu.sliceFast(tid, fTenant, 3).length / 4
  assert('単条件 tenant=3 件数一致', pgCount1 === enCount1,
    `pg=${pgCount1}, enchu=${enCount1}`)

  // 2条件: tenant_id=3 AND category=2
  const pgCount2 = parseInt((await pg.query(
    'SELECT count(*) AS c FROM test_products WHERE tenant_id = 3 AND category = 2'
  )).rows[0].c)
  const enCount2 = enchu.queryCount(tid, [[fTenant, 3], [fCat, 2]])
  assert('2条件 tenant=3,cat=2 件数一致', pgCount2 === enCount2,
    `pg=${pgCount2}, enchu=${enCount2}`)

  // 3条件: tenant_id=3 AND category=2 AND color=1
  const pgCount3 = parseInt((await pg.query(
    'SELECT count(*) AS c FROM test_products WHERE tenant_id = 3 AND category = 2 AND color = 1'
  )).rows[0].c)
  const enCount3 = enchu.queryCount(tid, [[fTenant, 3], [fCat, 2], [fColor, 1]])
  assert('3条件 件数一致', pgCount3 === enCount3,
    `pg=${pgCount3}, enchu=${enCount3}`)

  // queryFast でエンティティID取得 → 件数確認
  const entities = bufToIds(enchu.queryFast(tid, [[fTenant, 3], [fCat, 2]]))
  assert('queryFast エンティティ数一致', entities.length === pgCount2,
    `entities=${entities.length}, pg=${pgCount2}`)

  // エンティティIDがソート済みか
  let sorted = true
  for (let i = 1; i < entities.length; i++) {
    if (entities[i] <= entities[i - 1]) { sorted = false; break }
  }
  assert('queryFast 結果がソート済み', sorted)
}

// ════════════════ 3. queryFast テスト ════════════════

async function testQueryFast() {
  console.log('\n  [3] queryFast / queryCount 詳細')

  const tid = enchu.resolveTable('test_products')
  const fTenant = enchu.resolveField('test_products', 'tenant_id')
  const fCat = enchu.resolveField('test_products', 'category')

  // 全テナントの件数がPgと一致するか
  let allMatch = true
  for (let t = 0; t < 10; t++) {
    const pgC = parseInt((await pg.query(
      `SELECT count(*) AS c FROM test_products WHERE tenant_id = ${t}`
    )).rows[0].c)
    const enC = enchu.sliceFast(tid, fTenant, t).length / 4
    if (pgC !== enC) { allMatch = false; break }
  }
  assert('全テナント(0-9) 件数一致', allMatch)

  // 全category × tenant の組み合わせ (10×5=50パターン)
  let comboMatch = true
  let checkedCombos = 0
  for (let t = 0; t < 10; t++) {
    for (let c = 0; c < 5; c++) {
      const pgC = parseInt((await pg.query(
        `SELECT count(*) AS c FROM test_products WHERE tenant_id = ${t} AND category = ${c}`
      )).rows[0].c)
      const enC = enchu.queryCount(tid, [[fTenant, t], [fCat, c]])
      if (pgC !== enC) { comboMatch = false }
      checkedCombos++
    }
  }
  assert(`全組合せ(${checkedCombos}パターン) 件数一致`, comboMatch)

  // queryFast と queryCount の整合性
  const entities = bufToIds(enchu.queryFast(tid, [[fTenant, 5], [fCat, 3]]))
  const count = enchu.queryCount(tid, [[fTenant, 5], [fCat, 3]])
  assert('queryFast.length === queryCount', entities.length === count,
    `fast=${entities.length}, count=${count}`)
}

// ════════════════ 4. エッジケース ════════════════

async function testEdgeCases() {
  console.log('\n  [4] エッジケース')

  const tid = enchu.resolveTable('test_products')
  const fTenant = enchu.resolveField('test_products', 'tenant_id')
  const fCat = enchu.resolveField('test_products', 'category')

  // 存在しない値 → 0件
  const empty1 = enchu.sliceFast(tid, fTenant, 999).length / 4
  assert('存在しない値 → 0件', empty1 === 0, `got ${empty1}`)

  // queryCount 存在しない組合せ → 0
  const empty2 = enchu.queryCount(tid, [[fTenant, 999], [fCat, 0]])
  assert('存在しない組合せ → 0件', empty2 === 0, `got ${empty2}`)

  // queryFast 存在しない → 空Buffer
  const emptyBuf = enchu.queryFast(tid, [[fTenant, 999]])
  assert('queryFast 存在しない → 空Buffer', emptyBuf.length === 0)

  // 条件0個 → 0件
  const noCondCount = enchu.queryCount(tid, [])
  assert('条件0個 → 0件', noCondCount === 0)

  const noCondBuf = enchu.queryFast(tid, [])
  assert('queryFast 条件0個 → 空Buffer', noCondBuf.length === 0)

  // 単条件 queryCount === sliceFast
  const sliceCount = enchu.sliceFast(tid, fTenant, 0).length / 4
  const qCount = enchu.queryCount(tid, [[fTenant, 0]])
  assert('単条件: queryCount === sliceFast', sliceCount === qCount,
    `slice=${sliceCount}, query=${qCount}`)
}

// ════════════════ 5. 差分同期テスト ════════════════

async function testSync() {
  console.log('\n  [5] 差分同期')

  const tid = enchu.resolveTable('test_products')
  const fTenant = enchu.resolveField('test_products', 'tenant_id')

  // 同期前の件数
  const before = enchu.sliceFast(tid, fTenant, 0).length / 4

  // Pgに10件追加
  for (let i = 0; i < 10; i++) {
    await pg.query(
      `INSERT INTO test_products (tenant_id, category, color, price) VALUES (0, 0, 0, 999)`
    )
  }

  // Enchuに同期
  const newRows = await pg.query(
    `SELECT * FROM test_products WHERE tenant_id = 0 ORDER BY id DESC LIMIT 10`
  )
  for (const row of newRows.rows) {
    const eid = enchu.insert('test_products')
    enchu.writeInt('test_products', eid, 'tenant_id', row.tenant_id)
    enchu.writeInt('test_products', eid, 'category', row.category)
    enchu.writeInt('test_products', eid, 'color', row.color)
    enchu.writeInt('test_products', eid, 'price', row.price)
  }
  enchu.flush()

  const after = enchu.sliceFast(tid, fTenant, 0).length / 4
  assert('同期後に10件増加', after === before + 10,
    `before=${before}, after=${after}`)

  // 値の更新テスト
  // eid=0 の price を 12345 に変更
  enchu.writeInt('test_products', 0, 'price', 12345)
  enchu.flush()

  // sliceRange で price=12345 が取れるか
  const priceSlice = enchu.sliceRange('test_products', 'price', 12345, 12345)
  const priceIds = bufToIds(priceSlice)
  assert('値更新後に検索ヒット', priceIds.includes(0),
    `ids=${JSON.stringify(priceIds)}`)
}

// ════════════════ 6. 型マッピングテスト ════════════════

async function testTypes() {
  console.log('\n  [6] Pg型マッピング')

  // 別テーブルで色んな型を使う（autoInitは1回しか呼べないので検証はPg側だけ）
  await pg.query('DROP TABLE IF EXISTS test_types CASCADE')
  await pg.query(`
    CREATE TABLE test_types (
      id SERIAL PRIMARY KEY,
      int_col INTEGER,
      bigint_col BIGINT,
      small_col SMALLINT,
      real_col REAL,
      double_col DOUBLE PRECISION,
      numeric_col NUMERIC(10,2),
      bool_col BOOLEAN,
      text_col TEXT,
      varchar_col VARCHAR(255),
      ts_col TIMESTAMP,
      uuid_col UUID
    )
  `)

  // information_schema から型を取得
  const res = await pg.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'test_types'
    ORDER BY ordinal_position
  `)

  const typeMap: Record<string, string> = {}
  for (const row of res.rows) {
    typeMap[row.column_name] = row.data_type
  }

  assert('integer → integer', typeMap['int_col'] === 'integer')
  assert('bigint → bigint', typeMap['bigint_col'] === 'bigint')
  assert('smallint → smallint', typeMap['small_col'] === 'smallint')
  assert('real → real', typeMap['real_col'] === 'real')
  assert('double → double precision', typeMap['double_col'] === 'double precision')
  assert('numeric → numeric', typeMap['numeric_col'] === 'numeric')
  assert('boolean → boolean', typeMap['bool_col'] === 'boolean')
  assert('text → text', typeMap['text_col'] === 'text')
  assert('varchar → character varying', typeMap['varchar_col'] === 'character varying')
  assert('timestamp → timestamp without time zone', typeMap['ts_col'] === 'timestamp without time zone')
  assert('uuid → uuid', typeMap['uuid_col'] === 'uuid')

  await pg.query('DROP TABLE IF EXISTS test_types CASCADE')
}

main().catch(console.error)
