/**
 * Enchu Extend vs Meilisearch — マルチテナント ファセット検索バトル
 *
 * 同一データ100万件で検索速度を比較。
 * Enchu: napi経由（queryFast/sliceFast）
 * Meilisearch: HTTP API
 */

const enchu = require('./enchu-extend.node')

const PG_CONN = 'postgresql://postgres:enchu@localhost:5433/enchu_bench'
const CACHE_DIR = '/tmp/enchu_vs_meili'
const MEILI_URL = 'http://localhost:7700'
const MEILI_KEY = 'bench123'

const TENANTS = 100
const PRODUCTS_PER_TENANT = 10_000
const N = TENANTS * PRODUCTS_PER_TENANT

const CATEGORIES = 20
const COLORS = 10
const SIZES = 5

async function main() {
  console.log('╔══════════════════════════════════════════════════════════════════════╗')
  console.log(`║  Enchu Extend vs Meilisearch — マルチテナント ファセット検索       ║`)
  console.log(`║  ${N.toLocaleString()} 件 (${TENANTS}テナント × ${PRODUCTS_PER_TENANT.toLocaleString()}商品)                          ║`)
  console.log('╚══════════════════════════════════════════════════════════════════════╝\n')

  // ══════ Pg にデータ投入 ══════
  const { Client } = await import('pg')
  const pg = new Client({ connectionString: PG_CONN })
  await pg.connect()

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
      price INT NOT NULL
    )
  `)
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
  console.log(`      ${N.toLocaleString()} 件 (${((Date.now() - t1) / 1000).toFixed(1)}s)`)

  // ══════ Enchu セットアップ ══════
  console.log('\n  [2] Enchu セットアップ...')
  const t2 = Date.now()
  const fs = await import('fs')
  fs.rmSync(CACHE_DIR, { recursive: true, force: true })

  enchu.autoInit(CACHE_DIR, PG_CONN)

  // Pgからロード
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
  console.log(`      Enchu: ${enchu.count('products')} 件 (${((Date.now() - t2) / 1000).toFixed(1)}s)`)

  // ID事前解決
  const tid = enchu.resolveTable('products')
  const fTenant = enchu.resolveField('products', 'tenant_id')
  const fCat = enchu.resolveField('products', 'category')
  const fColor = enchu.resolveField('products', 'color')
  const fSize = enchu.resolveField('products', 'size')

  // ══════ Meilisearch セットアップ ══════
  console.log('\n  [3] Meilisearch セットアップ...')
  const t3 = Date.now()

  await meiliFetch('DELETE', '/indexes/products')
  await sleep(1000)
  await meiliFetch('POST', '/indexes', { uid: 'products', primaryKey: 'id' })
  await sleep(1000)
  await meiliFetch('PUT', '/indexes/products/settings/filterable-attributes',
    ['tenant_id', 'category', 'color', 'size', 'price'])
  await sleep(1000)

  // バッチ投入
  for (let tenant = 0; tenant < TENANTS; tenant++) {
    const docs: any[] = []
    for (let p = 0; p < PRODUCTS_PER_TENANT; p++) {
      const id = tenant * PRODUCTS_PER_TENANT + p
      docs.push({
        id,
        tenant_id: tenant,
        category: p % CATEGORIES,
        color: p % COLORS,
        size: p % SIZES,
        price: p % 10000,
      })
    }
    await meiliFetch('POST', '/indexes/products/documents', docs)
  }

  // indexing完了待ち
  process.stdout.write('      indexing...')
  while (true) {
    const resp = await meiliFetch('GET', '/tasks?statuses=enqueued,processing&limit=1')
    if (resp.total === 0) break
    await sleep(2000)
  }
  console.log(` (${((Date.now() - t3) / 1000).toFixed(1)}s)`)

  // ══════ バトル ══════
  const w = 50
  const r = 500

  console.log('\n  ┌───────────────────────────┬──────────────┬──────────────┬──────────┐')
  console.log('  │ クエリ                    │   Enchu (ns) │  Meili (ns)  │     倍率 │')
  console.log('  ├───────────────────────────┼──────────────┼──────────────┼──────────┤')

  // 1. 単条件: tenant_id = 42
  const en1 = benchSync(w, r, () => {
    return enchu.sliceFast(tid, fTenant, 42).length / 4
  })
  const me1 = await benchAsync(w, r, 'tenant_id = 42')
  row('tenant=42 (1cond)      ', en1, me1)

  // 2. 2条件: tenant_id=42 AND category=5
  const en2 = benchSync(w, r, () => {
    return enchu.queryCount(tid, [[fTenant, 42], [fCat, 5]])
  })
  const me2 = await benchAsync(w, r, 'tenant_id = 42 AND category = 5')
  row('tenant+cat (2cond)     ', en2, me2)

  // 3. 3条件: tenant_id=42 AND category=5 AND color=3
  const en3 = benchSync(w, r, () => {
    return enchu.queryCount(tid, [[fTenant, 42], [fCat, 5], [fColor, 3]])
  })
  const me3 = await benchAsync(w, r, 'tenant_id = 42 AND category = 5 AND color = 3')
  row('tenant+cat+col (3cond) ', en3, me3)

  // 4. 4条件
  const en4 = benchSync(w, r, () => {
    return enchu.queryCount(tid, [[fTenant, 42], [fCat, 5], [fColor, 3], [fSize, 1]])
  })
  const me4 = await benchAsync(w, r, 'tenant_id = 42 AND category = 5 AND color = 3 AND size = 1')
  row('tenant+4facets (4cond) ', en4, me4)

  // 5. 2条件でエンティティ取得（queryFast = 実データ返却）
  const en5 = benchSync(w, r, () => {
    return enchu.queryFast(tid, [[fTenant, 42], [fCat, 5]]).length / 4
  })
  const me5 = await benchAsync(w, r, 'tenant_id = 42 AND category = 5')
  row('tenant+cat (entities) ', en5, me5)

  console.log('  └───────────────────────────┴──────────────┴──────────────┴──────────┘')

  // cleanup
  await pg.query('DROP TABLE IF EXISTS products')
  await pg.end()
  await meiliFetch('DELETE', '/indexes/products')
  fs.rmSync(CACHE_DIR, { recursive: true, force: true })
  console.log('\n  Done.')
}

// ════════════════ helpers ════════════════

function benchSync(warmup: number, iterations: number, fn: () => number): number {
  for (let i = 0; i < warmup; i++) fn()
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) fn()
  return Number(process.hrtime.bigint() - start) / iterations
}

async function benchAsync(warmup: number, iterations: number, filter: string): Promise<number> {
  for (let i = 0; i < warmup; i++) await meiliSearch(filter)
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) await meiliSearch(filter)
  return Number(process.hrtime.bigint() - start) / iterations
}

async function meiliSearch(filter: string): Promise<number> {
  const resp = await meiliFetch('POST', '/indexes/products/search', {
    filter,
    limit: 10000,
    q: '',
  })
  return resp.estimatedTotalHits ?? 0
}

async function meiliFetch(method: string, path: string, body?: any): Promise<any> {
  const opts: RequestInit = {
    method,
    headers: {
      'Authorization': `Bearer ${MEILI_KEY}`,
      'Content-Type': 'application/json',
    },
  }
  if (body) opts.body = JSON.stringify(body)
  const resp = await fetch(`${MEILI_URL}${path}`, opts)
  if (!resp.ok && resp.status !== 404) {
    const text = await resp.text()
    throw new Error(`Meili ${method} ${path}: ${resp.status} ${text}`)
  }
  const text = await resp.text()
  return text ? JSON.parse(text) : {}
}

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)) }

function fmt(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${Math.round(ns)}ns`
}

function row(label: string, en: number, me: number) {
  const ratio = Math.floor(me / en)
  console.log(`  │ ${label} │ ${fmt(en).padStart(12)} │ ${fmt(me).padStart(12)} │ ${String(ratio + 'x').padStart(8)} │`)
}

main().catch(console.error)
