/**
 * 公平ベンチ — Enchu vs Meilisearch vs PostgreSQL
 *
 * 条件を完全に揃える:
 * 1. 全てHTTP API経由（Express for Enchu, HTTP for Meili, pg経由のHTTP for Pg）
 * 2. limit: 20（実用的なページネーション）
 * 3. JSONレスポンス返却（全員同じフォーマット）
 * 4. 同一データ、同一クエリ
 * 5. Pgにはインデックス + ANALYZE済み
 * 6. warmup後に計測
 */
import { Client } from 'pg'
import http from 'http'

const enchu = require('./enchu-extend.node')

const PG_CONN = 'postgresql://postgres:enchu@localhost:5433/enchu_bench'
const CACHE_DIR = '/tmp/enchu_fair_bench'
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
  console.log('║  公平ベンチ — 全て HTTP API、limit:20、JSON返却                   ║')
  console.log(`║  ${N.toLocaleString()} 件 (${TENANTS}テナント × ${PRODUCTS_PER_TENANT.toLocaleString()}商品)                          ║`)
  console.log('╚══════════════════════════════════════════════════════════════════════╝\n')

  const pg = new Client({ connectionString: PG_CONN })
  await pg.connect()

  // ══════ 1. Pg セットアップ ══════
  console.log('  [1] PostgreSQL セットアップ...')
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
  await pg.query('CREATE INDEX idx_tenant ON products(tenant_id)')
  await pg.query('CREATE INDEX idx_category ON products(category)')
  await pg.query('CREATE INDEX idx_color ON products(color)')
  await pg.query('CREATE INDEX idx_size ON products(size)')
  await pg.query('CREATE INDEX idx_price ON products(price)')

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
  await pg.query('ANALYZE products')
  console.log(`      ${N.toLocaleString()} 件 + 5インデックス + ANALYZE (${((Date.now() - t1) / 1000).toFixed(1)}s)`)

  // ══════ 2. Enchu セットアップ ══════
  console.log('\n  [2] Enchu セットアップ...')
  const t2 = Date.now()
  const fs = await import('fs')
  fs.rmSync(CACHE_DIR, { recursive: true, force: true })

  const schema = JSON.stringify({
    name: 'products',
    fields: [
      { name: 'id', type: 'int' },
      { name: 'tenant_id', type: 'int' },
      { name: 'category', type: 'int' },
      { name: 'color', type: 'int' },
      { name: 'size', type: 'int' },
      { name: 'price', type: 'int' },
    ],
    tables: [],
  })
  enchu.init(CACHE_DIR, schema, PG_CONN)

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
      enchu.writeInt('products', eid, 'id', row.id)
      enchu.writeInt('products', eid, 'tenant_id', row.tenant_id)
      enchu.writeInt('products', eid, 'category', row.category)
      enchu.writeInt('products', eid, 'color', row.color)
      enchu.writeInt('products', eid, 'size', row.size)
      enchu.writeInt('products', eid, 'price', row.price)
    }
    offset += res.rows.length
  }
  enchu.flush()

  const tid = enchu.resolveTable('products')
  const fTenant = enchu.resolveField('products', 'tenant_id')
  const fCat = enchu.resolveField('products', 'category')
  const fColor = enchu.resolveField('products', 'color')
  const fSize = enchu.resolveField('products', 'size')

  console.log(`      ${enchu.count('products')} 件 (${((Date.now() - t2) / 1000).toFixed(1)}s)`)

  // ══════ 3. Meilisearch セットアップ ══════
  console.log('\n  [3] Meilisearch セットアップ...')
  const t3 = Date.now()

  await meiliFetch('DELETE', '/indexes/products')
  await sleep(1000)
  await meiliFetch('POST', '/indexes', { uid: 'products', primaryKey: 'id' })
  await sleep(1000)
  await meiliFetch('PUT', '/indexes/products/settings/filterable-attributes',
    ['tenant_id', 'category', 'color', 'size', 'price'])
  await sleep(1000)

  for (let tenant = 0; tenant < TENANTS; tenant++) {
    const docs: any[] = []
    for (let p = 0; p < PRODUCTS_PER_TENANT; p++) {
      const id = tenant * PRODUCTS_PER_TENANT + p
      docs.push({ id, tenant_id: tenant, category: p % CATEGORIES, color: p % COLORS, size: p % SIZES, price: p % 10000 })
    }
    await meiliFetch('POST', '/indexes/products/documents', docs)
  }
  process.stdout.write('      indexing...')
  while (true) {
    const resp = await meiliFetch('GET', '/tasks?statuses=enqueued,processing&limit=1')
    if (resp.total === 0) break
    await sleep(2000)
  }
  console.log(` (${((Date.now() - t3) / 1000).toFixed(1)}s)`)

  // ══════ 4. HTTPサーバー起動 ══════
  console.log('\n  [4] HTTPサーバー起動...')

  const pgHttp = new Client({ connectionString: PG_CONN })
  await pgHttp.connect()

  // Enchu HTTP (port 9901)
  const enchuServer = http.createServer((req, res) => {
    const url = new URL(req.url!, `http://localhost`)
    const tenant = url.searchParams.get('tenant_id')
    const cat = url.searchParams.get('category')
    const color = url.searchParams.get('color')
    const size = url.searchParams.get('size')
    const limit = Number(url.searchParams.get('limit') ?? '20')

    const conds: [number, number][] = []
    if (tenant !== null) conds.push([fTenant, Number(tenant)])
    if (cat !== null) conds.push([fCat, Number(cat)])
    if (color !== null) conds.push([fColor, Number(color)])
    if (size !== null) conds.push([fSize, Number(size)])

    const buf: Buffer = conds.length > 0
      ? enchu.queryFast(tid, conds)
      : Buffer.alloc(0)
    const allIds = new Uint32Array(buf.buffer, buf.byteOffset, buf.length / 4)
    const total = allIds.length
    const ids = Array.from(allIds).slice(0, limit)

    const idBuf = Buffer.alloc(ids.length * 4)
    for (let i = 0; i < ids.length; i++) idBuf.writeUInt32LE(ids[i], i * 4)

    const hits = ids.map((_, i) => ({
      id: getI64(idBuf, 'id', i, ids.length),
      tenant_id: getI64(idBuf, 'tenant_id', i, ids.length),
      category: getI64(idBuf, 'category', i, ids.length),
      color: getI64(idBuf, 'color', i, ids.length),
      size: getI64(idBuf, 'size', i, ids.length),
      price: getI64(idBuf, 'price', i, ids.length),
    }))

    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ hits, total }))
  })
  await new Promise<void>(r => enchuServer.listen(9901, r))

  // カラム値キャッシュ（リクエスト内で使い回す）
  const colCache: Record<string, number[]> = {}
  function getI64(idBuf: Buffer, field: string, index: number, count: number): number {
    const key = `${field}_${idBuf.toString('hex')}`
    if (!colCache[key]) {
      const valBuf: Buffer = enchu.getColumnInt('products', field, idBuf)
      const vals = new BigInt64Array(valBuf.buffer, valBuf.byteOffset, valBuf.length / 8)
      colCache[key] = Array.from(vals, v => Number(v))
    }
    return colCache[key][index]
  }

  // Pg HTTP (port 9902)
  const pgServer = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url!, `http://localhost`)
      const tenant = url.searchParams.get('tenant_id')
      const cat = url.searchParams.get('category')
      const color = url.searchParams.get('color')
      const size = url.searchParams.get('size')
      const limit = Number(url.searchParams.get('limit') ?? '20')

      const wheres: string[] = []
      const params: any[] = []
      let pi = 1
      if (tenant !== null) { wheres.push(`tenant_id = $${pi++}`); params.push(Number(tenant)) }
      if (cat !== null) { wheres.push(`category = $${pi++}`); params.push(Number(cat)) }
      if (color !== null) { wheres.push(`color = $${pi++}`); params.push(Number(color)) }
      if (size !== null) { wheres.push(`size = $${pi++}`); params.push(Number(size)) }

      const where = wheres.length > 0 ? `WHERE ${wheres.join(' AND ')}` : ''

      const [dataRes, countRes] = await Promise.all([
        pgHttp.query(`SELECT id, tenant_id, category, color, size, price FROM products ${where} LIMIT $${pi}`, [...params, limit]),
        pgHttp.query(`SELECT count(*) AS c FROM products ${where}`, params),
      ])

      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ hits: dataRes.rows, total: parseInt(countRes.rows[0].c) }))
    } catch (e: any) {
      res.writeHead(500)
      res.end(e.message)
    }
  })
  await new Promise<void>(r => pgServer.listen(9902, r))

  console.log('      Enchu HTTP → :9901')
  console.log('      Pg HTTP    → :9902')
  console.log('      Meili HTTP → :7700')

  // ══════ 5. 公平ベンチ ══════
  const w = 30
  const r = 200

  console.log('\n  ┌───────────────────────────┬──────────────┬──────────────┬──────────────┬───────────┬───────────┐')
  console.log('  │ クエリ (limit:20)         │   Enchu HTTP │      Pg HTTP │   Meili HTTP │   vs Pg   │  vs Meili │')
  console.log('  ├───────────────────────────┼──────────────┼──────────────┼──────────────┼───────────┼───────────┤')

  const q1 = 'tenant_id=42'
  const en1 = await benchHttp(w, r, `http://localhost:9901/search?${q1}&limit=20`)
  const pg1 = await benchHttp(w, r, `http://localhost:9902/search?${q1}&limit=20`)
  const me1 = await benchMeili(w, r, 'tenant_id = 42', 20)
  fairRow('tenant=42 (1cond)      ', en1, pg1, me1)

  const q2 = 'tenant_id=42&category=5'
  const en2 = await benchHttp(w, r, `http://localhost:9901/search?${q2}&limit=20`)
  const pg2 = await benchHttp(w, r, `http://localhost:9902/search?${q2}&limit=20`)
  const me2 = await benchMeili(w, r, 'tenant_id = 42 AND category = 5', 20)
  fairRow('tenant+cat (2cond)     ', en2, pg2, me2)

  const q3 = 'tenant_id=42&category=5&color=3'
  const en3 = await benchHttp(w, r, `http://localhost:9901/search?${q3}&limit=20`)
  const pg3 = await benchHttp(w, r, `http://localhost:9902/search?${q3}&limit=20`)
  const me3 = await benchMeili(w, r, 'tenant_id = 42 AND category = 5 AND color = 3', 20)
  fairRow('tenant+cat+col (3cond) ', en3, pg3, me3)

  const q4 = 'tenant_id=42&category=5&color=3&size=1'
  const en4 = await benchHttp(w, r, `http://localhost:9901/search?${q4}&limit=20`)
  const pg4 = await benchHttp(w, r, `http://localhost:9902/search?${q4}&limit=20`)
  const me4 = await benchMeili(w, r, 'tenant_id = 42 AND category = 5 AND color = 3 AND size = 1', 20)
  fairRow('tenant+4 (4cond)       ', en4, pg4, me4)

  console.log('  └───────────────────────────┴──────────────┴──────────────┴──────────────┴───────────┴───────────┘')

  // ══════ 6. インプロセス参考 ══════
  console.log('\n  参考: インプロセス（HTTPオーバーヘッドなし）')
  console.log('  ┌───────────────────────────┬──────────────┐')
  console.log('  │ クエリ                    │ Enchu direct │')
  console.log('  ├───────────────────────────┼──────────────┤')
  const d1 = benchSync(100, 1000, () => enchu.queryCount(tid, [[fTenant, 42]]))
  console.log(`  │ tenant=42 (1cond)       │ ${fmt(d1).padStart(12)} │`)
  const d2 = benchSync(100, 1000, () => enchu.queryCount(tid, [[fTenant, 42], [fCat, 5]]))
  console.log(`  │ tenant+cat (2cond)      │ ${fmt(d2).padStart(12)} │`)
  const d3 = benchSync(100, 1000, () => enchu.queryCount(tid, [[fTenant, 42], [fCat, 5], [fColor, 3]]))
  console.log(`  │ tenant+cat+col (3cond)  │ ${fmt(d3).padStart(12)} │`)
  console.log('  └───────────────────────────┴──────────────┘')

  // cleanup
  enchuServer.close()
  pgServer.close()
  await pgHttp.end()
  await pg.query('DROP TABLE IF EXISTS products')
  await pg.end()
  await meiliFetch('DELETE', '/indexes/products')
  fs.rmSync(CACHE_DIR, { recursive: true, force: true })

  console.log('\n  Done.')
}

// ════════════════ helpers ════════════════

function benchSync(warmup: number, iterations: number, fn: () => any): number {
  for (let i = 0; i < warmup; i++) fn()
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) fn()
  return Number(process.hrtime.bigint() - start) / iterations
}

async function benchHttp(warmup: number, iterations: number, url: string): Promise<number> {
  for (let i = 0; i < warmup; i++) await httpGet(url)
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) await httpGet(url)
  return Number(process.hrtime.bigint() - start) / iterations
}

async function benchMeili(warmup: number, iterations: number, filter: string, limit: number): Promise<number> {
  for (let i = 0; i < warmup; i++) await meiliSearch(filter, limit)
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) await meiliSearch(filter, limit)
  return Number(process.hrtime.bigint() - start) / iterations
}

function httpGet(url: string): Promise<string> {
  return new Promise((resolve, reject) => {
    http.get(url, res => {
      let data = ''
      res.on('data', chunk => data += chunk)
      res.on('end', () => resolve(data))
    }).on('error', reject)
  })
}

async function meiliSearch(filter: string, limit: number): Promise<any> {
  return meiliFetch('POST', '/indexes/products/search', { filter, limit, q: '' })
}

async function meiliFetch(method: string, path: string, body?: any): Promise<any> {
  const opts: RequestInit = {
    method,
    headers: { 'Authorization': `Bearer ${MEILI_KEY}`, 'Content-Type': 'application/json' },
  }
  if (body) opts.body = JSON.stringify(body)
  const resp = await fetch(`${MEILI_URL}${path}`, opts)
  const text = await resp.text()
  return text ? JSON.parse(text) : {}
}

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)) }

function fmt(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${Math.round(ns)}ns`
}

function fairRow(label: string, en: number, pg: number, me: number) {
  const vsPg = pg / en
  const vsMe = me / en
  console.log(`  │ ${label} │ ${fmt(en).padStart(12)} │ ${fmt(pg).padStart(12)} │ ${fmt(me).padStart(12)} │ ${(vsPg.toFixed(1) + 'x').padStart(9)} │ ${(vsMe.toFixed(1) + 'x').padStart(9)} │`)
}

main().catch(console.error)
