/**
 * 負荷テスト — Pg同期しながらEnchu検索
 *
 * 1. Pgに100万件のEC商品データを投入
 * 2. Enchuに初回同期
 * 3. Pgに書き込みを続けながらEnchuで検索 → 速度を測る
 *
 * 接続: postgresql://postgres@localhost:5433/enchu_bench
 */
import { Client } from 'pg'

const enchu = require('./enchu-extend.node')

const PG_CONN = 'postgresql://postgres:enchu@localhost:5433/enchu_bench'
const CACHE_DIR = '/tmp/enchu_load_test'
const TENANTS = 100
const PRODUCTS_PER_TENANT = 10_000
const N = TENANTS * PRODUCTS_PER_TENANT

const CATEGORIES = 20
const COLORS = 10
const SIZES = 5

async function main() {
  console.log('╔══════════════════════════════════════════════════════════════╗')
  console.log(`║  負荷テスト — Pg同期 + Enchu検索  (${N.toLocaleString()} 件)            ║`)
  console.log('╚══════════════════════════════════════════════════════════════╝\n')

  const pg = new Client({ connectionString: PG_CONN })
  await pg.connect()

  // ══════ 1. Pgにテーブル作成 + データ投入 ══════
  console.log('  [1] Pg セットアップ...')
  const t1 = Date.now()

  await pg.query('DROP TABLE IF EXISTS products CASCADE')
  await pg.query(`
    CREATE TABLE products (
      id SERIAL PRIMARY KEY,
      tenant_id INT NOT NULL,
      category INT NOT NULL,
      color INT NOT NULL,
      size INT NOT NULL,
      price INT NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `)
  await pg.query('CREATE INDEX idx_products_tenant ON products(tenant_id)')
  await pg.query('CREATE INDEX idx_products_updated ON products(updated_at)')

  // バッチINSERT (1000件ずつ)
  const BATCH = 1000
  for (let i = 0; i < N; i += BATCH) {
    const values: string[] = []
    for (let j = 0; j < BATCH && i + j < N; j++) {
      const id = i + j
      const tenant = Math.floor(id / PRODUCTS_PER_TENANT)
      const p = id % PRODUCTS_PER_TENANT
      values.push(`(${tenant}, ${p % CATEGORIES}, ${p % COLORS}, ${p % SIZES}, ${p % 10000})`)
    }
    await pg.query(`INSERT INTO products (tenant_id, category, color, size, price) VALUES ${values.join(',')}`)
  }
  console.log(`      ${N.toLocaleString()} 件投入完了 (${((Date.now() - t1) / 1000).toFixed(1)}s)`)

  // ══════ 2. Enchu初回同期 ══════
  console.log('\n  [2] Enchu 初回同期...')
  const t2 = Date.now()

  // キャッシュクリア
  const fs = await import('fs')
  fs.rmSync(CACHE_DIR, { recursive: true, force: true })

  // Pgからスキーマ自動取得 + エンジン初期化
  enchu.autoInit(CACHE_DIR, PG_CONN)

  // Pgから全件読み込み → Enchuに書き込み
  const LOAD_BATCH = 10000
  let offset = 0
  while (offset < N) {
    const res = await pg.query(
      `SELECT id, tenant_id, category, color, size, price FROM products ORDER BY id LIMIT $1 OFFSET $2`,
      [LOAD_BATCH, offset]
    )
    if (res.rows.length === 0) break

    for (const row of res.rows) {
      const eid = enchu.insert('products')
      enchu.writeInt('products', eid, 'tenant_id', row.tenant_id)
      enchu.writeInt('products', eid, 'category', row.category)
      enchu.writeInt('products', eid, 'color', row.color)
      enchu.writeInt('products', eid, 'size', row.size)
      enchu.writeInt('products', eid, 'price', row.price)
    }
    offset += res.rows.length
  }

  enchu.flush()
  console.log(`      同期完了 (${((Date.now() - t2) / 1000).toFixed(1)}s)`)
  console.log(`      Enchu件数: ${enchu.count('products')}`)

  // ══════ 3. 検索ベンチ (同期なし) ══════
  console.log('\n  [3] 検索ベンチマーク...')

  // ID事前解決（sliceFast用）
  const tid = enchu.resolveTable('products')
  const fTenant = enchu.resolveField('products', 'tenant_id')
  const fCat = enchu.resolveField('products', 'category')
  const fPrice = enchu.resolveField('products', 'price')

  console.log('\n  ┌───────────────────────────┬──────────────┬──────────────┬──────────┐')
  console.log('  │ クエリ                    │   Enchu (ns) │      Pg (ns) │     倍率 │')
  console.log('  ├───────────────────────────┼──────────────┼──────────────┼──────────┤')

  // 単条件: tenant_id = 42 (sliceFast)
  const enT1 = benchEnchu(1000, () => {
    const buf = enchu.sliceFast(tid, fTenant, 42)
    return buf.length / 4
  })
  const pgT1 = await benchPg(100, pg, 'SELECT count(*) FROM products WHERE tenant_id = 42')
  row('tenant=42 (fast)       ', enT1, pgT1)

  // 比較: 従来のslice
  const enT1old = benchEnchu(1000, () => {
    const buf = enchu.slice('products', 'tenant_id', 42)
    return buf.length / 4
  })
  row('tenant=42 (old)        ', enT1old, pgT1)

  // 2条件: queryFast (Rust内で交差、Buffer copy 1回)
  const enT2 = benchEnchu(1000, () => {
    const buf = enchu.queryFast(tid, [[fTenant, 42], [fCat, 5]])
    return buf.length / 4
  })
  const pgT2 = await benchPg(100, pg, 'SELECT count(*) FROM products WHERE tenant_id = 42 AND category = 5')
  row('tenant+cat (query)     ', enT2, pgT2)

  // 2条件: queryCount (件数だけ、Buffer確保すらなし)
  const enT2c = benchEnchu(1000, () => {
    return enchu.queryCount(tid, [[fTenant, 42], [fCat, 5]])
  })
  row('tenant+cat (count)     ', enT2c, pgT2)

  // 2条件: 旧方式 (比較用)
  const enT2old = benchEnchu(1000, () => {
    const a = enchu.sliceFast(tid, fTenant, 42)
    const b = enchu.sliceFast(tid, fCat, 5)
    return intersectCount(a, b)
  })
  row('tenant+cat (old)       ', enT2old, pgT2)

  // range: tenant_id=42 AND price < 1000
  const enT3 = benchEnchu(1000, () => {
    const a = enchu.sliceFast(tid, fTenant, 42)
    const b = enchu.sliceRange('products', 'price', 0, 999)
    return intersectCount(a, b)
  })
  const pgT3 = await benchPg(100, pg, 'SELECT count(*) FROM products WHERE tenant_id = 42 AND price < 1000')
  row('tenant+price<1K (range)', enT3, pgT3)

  console.log('  └───────────────────────────┴──────────────┴──────────────┴──────────┘')

  // ══════ 4. 1秒ポーリング同期 + 書き込み + 検索 同時実行 ══════
  console.log('\n  [4] Pg書き込み + 1秒同期 + Enchu検索（10秒間）...')

  // 書き込み用と同期用で別接続
  const pgWrite = new Client({ connectionString: PG_CONN })
  const pgSync = new Client({ connectionString: PG_CONN })
  await pgWrite.connect()
  await pgSync.connect()

  const TEST_DURATION_MS = 10_000
  let writesDone = 0
  let searchesDone = 0
  let syncsDone = 0
  let syncedRows = 0
  let searchTotalNs = 0n
  let running = true

  // --- 1秒ポーリング同期（直近2秒分を取る） ---
  const syncLoop = (async () => {
    while (running) {
      await new Promise(r => setTimeout(r, 1000))
      if (!running) break

      const updated = await pgSync.query(
        `SELECT id, price FROM products WHERE updated_at > NOW() - INTERVAL '2 seconds' ORDER BY id`
      )
      if (updated.rows.length > 0) {
        for (const row of updated.rows) {
          const eid = row.id - 1
          enchu.writeInt('products', eid, 'price', row.price)
        }
        enchu.flush()
        syncedRows += updated.rows.length
      }
      syncsDone++
    }
  })()

  // --- Pg書き込み（ランダムUPDATE）---
  const writeLoop = (async () => {
    while (running) {
      await pgWrite.query(
        `UPDATE products SET price = $1, updated_at = NOW() WHERE id = $2`,
        [Math.floor(Math.random() * 10000), Math.floor(Math.random() * N) + 1]
      )
      writesDone++
      if (writesDone % 10 === 0) await new Promise(r => setTimeout(r, 0))
    }
  })()

  // --- Enchu検索 (sliceFast) ---
  const searchLoop = (async () => {
    while (running) {
      const tenant = Math.floor(Math.random() * TENANTS)
      const start = process.hrtime.bigint()
      const buf = enchu.sliceFast(tid, fTenant, tenant)
      searchTotalNs += process.hrtime.bigint() - start
      searchesDone++
      if (searchesDone % 1000 === 0) await new Promise(r => setTimeout(r, 0))
    }
  })()

  // 10秒待って停止
  const t4 = Date.now()
  await new Promise(r => setTimeout(r, TEST_DURATION_MS))
  running = false
  // syncLoopのsleep中でも止まるように少し待つ
  await new Promise(r => setTimeout(r, 1500))

  const elapsed = Date.now() - t4
  const avgSearchNs = Number(searchTotalNs) / searchesDone

  console.log(`      経過時間:     ${(elapsed / 1000).toFixed(1)}s`)
  console.log(`      Pg書き込み:   ${writesDone.toLocaleString()} 件`)
  console.log(`      差分同期:     ${syncsDone} 回 (計 ${syncedRows.toLocaleString()} 行反映)`)
  console.log(`      Enchu検索:    ${searchesDone.toLocaleString()} 件`)
  console.log(`      検索平均:     ${fmt(avgSearchNs)}`)
  console.log(`      検索スループット: ${Math.floor(searchesDone / (elapsed / 1000)).toLocaleString()}/sec`)

  // cleanup
  await pgWrite.end()
  await pgSync.end()
  await pg.query('DROP TABLE IF EXISTS products')
  await pg.end()
  fs.rmSync(CACHE_DIR, { recursive: true, force: true })

  console.log('\n  Done.')
}

function benchEnchu(iterations: number, fn: () => number): number {
  // warmup
  for (let i = 0; i < 50; i++) fn()
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) fn()
  return Number(process.hrtime.bigint() - start) / iterations
}

async function benchPg(iterations: number, pg: Client, sql: string): Promise<number> {
  // warmup
  for (let i = 0; i < 10; i++) await pg.query(sql)
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) await pg.query(sql)
  return Number(process.hrtime.bigint() - start) / iterations
}

/** Buffer内のu32配列2つの交差件数 */
function intersectCount(a: Buffer, b: Buffer): number {
  const aa = new Uint32Array(a.buffer, a.byteOffset, a.length / 4)
  const bb = new Uint32Array(b.buffer, b.byteOffset, b.length / 4)
  let i = 0, j = 0, count = 0
  while (i < aa.length && j < bb.length) {
    if (aa[i] < bb[j]) i++
    else if (aa[i] > bb[j]) j++
    else { count++; i++; j++ }
  }
  return count
}

function fmt(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${Math.round(ns)}ns`
}

function row(label: string, en: number, pg: number) {
  const ratio = Math.floor(pg / en)
  console.log(`  │ ${label} │ ${fmt(en).padStart(12)} │ ${fmt(pg).padStart(12)} │ ${String(ratio + 'x').padStart(8)} │`)
}

main().catch(console.error)
