/**
 * Enchu Extend vs Elasticsearch 8.17 — マルチテナント ファセット検索バトル
 *
 * 同一データ100万件で検索速度を比較。
 * Enchu: napi直（queryFast/sliceFast）
 * Elasticsearch: HTTP REST API（localhost）
 */

const enchu = require('./enchu-extend.node')

const ES_URL = 'http://localhost:9200'
const CACHE_DIR = '/tmp/enchu_vs_es'

const TENANTS = 100
const PRODUCTS_PER_TENANT = 10_000
const N = TENANTS * PRODUCTS_PER_TENANT

const CATEGORIES = 20
const COLORS = 10
const SIZES = 5

async function main() {
  console.log('╔══════════════════════════════════════════════════════════════════════╗')
  console.log(`║  Enchu Extend vs Elasticsearch 8.17 — ファセット検索              ║`)
  console.log(`║  ${N.toLocaleString()} 件 (${TENANTS}テナント × ${PRODUCTS_PER_TENANT.toLocaleString()}商品)                          ║`)
  console.log('╚══════════════════════════════════════════════════════════════════════╝\n')

  // ══════ 1. Enchu セットアップ ══════
  console.log('  [1] Enchu セットアップ...')
  const t1 = Date.now()
  const fs = await import('fs')
  fs.rmSync(CACHE_DIR, { recursive: true, force: true })

  enchu.init(CACHE_DIR, '')

  enchu.defineHimo('products.tenant_id', 'value', TENANTS)
  enchu.defineHimo('products.category', 'value', CATEGORIES)
  enchu.defineHimo('products.color', 'value', COLORS)
  enchu.defineHimo('products.size', 'value', SIZES)
  enchu.defineHimo('products.price', 'value', 10000)

  for (let i = 0; i < N; i++) {
    const eid = enchu.entity()
    const tenant = Math.floor(i / PRODUCTS_PER_TENANT)
    const p = i % PRODUCTS_PER_TENANT
    enchu.tie(eid, 'products.tenant_id', tenant)
    enchu.tie(eid, 'products.category', p % CATEGORIES)
    enchu.tie(eid, 'products.color', p % COLORS)
    enchu.tie(eid, 'products.size', p % SIZES)
    enchu.tie(eid, 'products.price', p % 10000)
  }
  enchu.rebuild()
  enchu.flush()

  const fTenant = enchu.resolveField('products', 'tenant_id')
  const fCat = enchu.resolveField('products', 'category')
  const fColor = enchu.resolveField('products', 'color')
  const fSize = enchu.resolveField('products', 'size')

  console.log(`      ${N.toLocaleString()} 件 (${((Date.now() - t1) / 1000).toFixed(1)}s)`)

  // ══════ 2. Elasticsearch セットアップ ══════
  console.log('\n  [2] Elasticsearch セットアップ...')
  const t2 = Date.now()

  // インデックス削除→作成
  await esFetch('DELETE', '/products')
  await esFetch('PUT', '/products', {
    settings: {
      number_of_shards: 1,
      number_of_replicas: 0,
      refresh_interval: '-1',  // indexing中はrefresh無効
    },
    mappings: {
      properties: {
        tenant_id: { type: 'integer' },
        category: { type: 'integer' },
        color: { type: 'integer' },
        size: { type: 'integer' },
        price: { type: 'integer' },
      },
    },
  })

  // バルク投入
  const BULK_SIZE = 5000
  for (let i = 0; i < N; i += BULK_SIZE) {
    let body = ''
    for (let j = 0; j < BULK_SIZE && i + j < N; j++) {
      const id = i + j
      const tenant = Math.floor(id / PRODUCTS_PER_TENANT)
      const p = id % PRODUCTS_PER_TENANT
      body += JSON.stringify({ index: { _id: id } }) + '\n'
      body += JSON.stringify({
        tenant_id: tenant,
        category: p % CATEGORIES,
        color: p % COLORS,
        size: p % SIZES,
        price: p % 10000,
      }) + '\n'
    }
    await esFetch('POST', '/products/_bulk', body, true)
  }

  // refresh して検索可能にする
  await esFetch('POST', '/products/_refresh')
  // refresh_interval を戻す
  await esFetch('PUT', '/products/_settings', { index: { refresh_interval: '1s' } })

  const countResp = await esFetch('GET', '/products/_count')
  console.log(`      ${countResp.count.toLocaleString()} 件 (${((Date.now() - t2) / 1000).toFixed(1)}s)`)

  // ══════ warmup ══════
  console.log('\n  [3] Warmup...')
  for (let i = 0; i < 50; i++) {
    enchu.queryCount(0, [[fTenant, 42], [fCat, 5]])
    await esSearch({ bool: { filter: [
      { term: { tenant_id: 42 } },
      { term: { category: 5 } },
    ]}})
  }
  console.log('      done')

  // ══════ バトル ══════
  const w = 50
  const r = 500

  console.log('\n  ┌────────────────────────────┬──────────────┬──────────────┬──────────┐')
  console.log('  │ クエリ                     │    Enchu     │    ES 8.17   │     倍率 │')
  console.log('  ├────────────────────────────┼──────────────┼──────────────┼──────────┤')

  // 1. 単条件: tenant_id = 42
  const en1 = benchSync(w, r, () => enchu.sliceFast(fTenant, 42).length / 4)
  const es1 = await benchAsync(w, r, { term: { tenant_id: 42 } })
  row('tenant=42 (1cond)       ', en1, es1)

  // 2. 2条件: tenant_id=42 AND category=5
  const en2 = benchSync(w, r, () => enchu.queryCount(0, [[fTenant, 42], [fCat, 5]]))
  const es2 = await benchAsync(w, r, { bool: { filter: [
    { term: { tenant_id: 42 } }, { term: { category: 5 } },
  ]}})
  row('tenant+cat (2cond)      ', en2, es2)

  // 3. 3条件
  const en3 = benchSync(w, r, () => enchu.queryCount(0, [[fTenant, 42], [fCat, 5], [fColor, 3]]))
  const es3 = await benchAsync(w, r, { bool: { filter: [
    { term: { tenant_id: 42 } }, { term: { category: 5 } }, { term: { color: 3 } },
  ]}})
  row('tenant+cat+col (3cond)  ', en3, es3)

  // 4. 4条件
  const en4 = benchSync(w, r, () => enchu.queryCount(0, [[fTenant, 42], [fCat, 5], [fColor, 3], [fSize, 1]]))
  const es4 = await benchAsync(w, r, { bool: { filter: [
    { term: { tenant_id: 42 } }, { term: { category: 5 } }, { term: { color: 3 } }, { term: { size: 1 } },
  ]}})
  row('tenant+4facets (4cond)  ', en4, es4)

  // 5. エンティティ取得
  const en5 = benchSync(w, r, () => enchu.queryFast(0, [[fTenant, 42], [fCat, 5]]).length / 4)
  const es5 = await benchAsync(w, r, { bool: { filter: [
    { term: { tenant_id: 42 } }, { term: { category: 5 } },
  ]}}, 500)
  row('tenant+cat (entities)  ', en5, es5)

  console.log('  └────────────────────────────┴──────────────┴──────────────┴──────────┘')

  // cleanup
  fs.rmSync(CACHE_DIR, { recursive: true, force: true })
  await esFetch('DELETE', '/products')
  console.log('\n  Done.')
}

// ════════════════ helpers ════════════════

function benchSync(warmup: number, iterations: number, fn: () => number): number {
  for (let i = 0; i < warmup; i++) fn()
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) fn()
  return Number(process.hrtime.bigint() - start) / iterations
}

async function benchAsync(warmup: number, iterations: number, query: any, size = 0): Promise<number> {
  for (let i = 0; i < warmup; i++) await esSearch(query, size)
  const start = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) await esSearch(query, size)
  return Number(process.hrtime.bigint() - start) / iterations
}

async function esSearch(query: any, size = 0): Promise<any> {
  return esFetch('POST', '/products/_search', {
    query,
    size,
    track_total_hits: true,
  })
}

async function esFetch(method: string, path: string, body?: any, ndjson = false): Promise<any> {
  const opts: RequestInit = {
    method,
    headers: {
      'Content-Type': ndjson ? 'application/x-ndjson' : 'application/json',
    },
  }
  if (body) opts.body = typeof body === 'string' ? body : JSON.stringify(body)
  const resp = await fetch(`${ES_URL}${path}`, opts)
  if (!resp.ok && resp.status !== 404) {
    const text = await resp.text()
    throw new Error(`ES ${method} ${path}: ${resp.status} ${text.slice(0, 200)}`)
  }
  const text = await resp.text()
  return text ? JSON.parse(text) : {}
}

function fmt(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${Math.round(ns)}ns`
}

function row(label: string, en: number, es: number) {
  const ratio = Math.floor(es / en)
  console.log(`  │ ${label} │ ${fmt(en).padStart(12)} │ ${fmt(es).padStart(12)} │ ${String(ratio + 'x').padStart(8)} │`)
}

main().catch(console.error)
