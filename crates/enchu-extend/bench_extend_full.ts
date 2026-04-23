// Enchu Extend v3 — 包括テスト
// 1. 規模テスト (10万エンティティ)
// 2. PGインデックス付き比較
// 4. キャッシュ無効化
// 5. メモリ使用量
// 6. 同時接続

import * as enchu from './native'
import { Client } from 'pg'

const CACHE_DIR = '/tmp/enchu_extend_full_test'

function fmt(ns: number): string {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(1)}ms`
  if (ns >= 1_000) return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

function bench(warmup: number, iters: number, fn: () => void): number {
  for (let i = 0; i < warmup; i++) fn()
  const t = performance.now()
  for (let i = 0; i < iters; i++) fn()
  return (performance.now() - t) / iters * 1_000_000
}

async function benchAsync(warmup: number, iters: number, fn: () => Promise<void>): Promise<number> {
  for (let i = 0; i < warmup; i++) await fn()
  const t = performance.now()
  for (let i = 0; i < iters; i++) await fn()
  return (performance.now() - t) / iters * 1_000_000
}

async function main() {
  const fs = require('fs')
  if (fs.existsSync(CACHE_DIR)) fs.rmSync(CACHE_DIR, { recursive: true })

  const pg = new Client({
    host: 'localhost', port: 5433,
    user: 'postgres', password: 'enchu',
    database: 'enchu_bench',
  })
  await pg.connect()

  const SCHEMA = JSON.stringify({
    name: "entities",
    fields: [{ name: "entity_id", type: "int" }],
    tables: [{
      name: "members",
      fields: [
        { name: "text_val", type: "symbol" },
        { name: "int_val", type: "int" },
      ]
    }]
  })

  // ════════════════════════════════════════
  // 1. 規模テスト — 10万エンティティ
  // ════════════════════════════════════════
  console.log('╔══════════════════════════════════════╗')
  console.log('║  1. 規模テスト (PGから直接ロード)    ║')
  console.log('╚══════════════════════════════════════╝\n')

  enchu.init(CACHE_DIR, SCHEMA, '')

  const ENTITIES = 100000
  const BATCH = 1000
  console.log(`Loading ${ENTITIES} entities (batch ${BATCH})...`)
  const t0 = performance.now()

  for (let start = 0; start < ENTITIES; start += BATCH) {
    const end = Math.min(start + BATCH, ENTITIES)

    // members一括取得
    const res = await pg.query(
      `SELECT entity_id, text_val, int_val FROM members WHERE entity_id >= $1 AND entity_id < $2 ORDER BY entity_id, row_id`,
      [start, end]
    )

    let currentEid = -1
    let cid = 0
    for (const row of res.rows) {
      const eid = Number(row.entity_id)
      if (eid !== currentEid) {
        currentEid = eid
        cid = enchu.insert('entities')
        enchu.writeInt('entities', cid, 'entity_id', currentEid)
      }
      const mid = enchu.insertChild('members', cid)
      enchu.writeText('members', mid, 'text_val', row.text_val || '')
      enchu.writeInt('members', mid, 'int_val', Number(row.int_val) || 0)
    }

    if (start % 10000 === 0) process.stdout.write(`  ${start}...`)
  }

  enchu.flush()
  const loadTime = performance.now() - t0
  const memberCount = enchu.count('members')
  console.log(`\n  Loaded: entities=${enchu.count('entities')}, members=${memberCount}`)
  console.log(`  Time: ${(loadTime / 1000).toFixed(1)}s\n`)

  // ════════════════════════════════════════
  // 2. PGインデックス付き比較
  // ════════════════════════════════════════
  console.log('╔══════════════════════════════════════╗')
  console.log('║  2. PGインデックス付き比較           ║')
  console.log('╚══════════════════════════════════════╝\n')

  // インデックス追加
  console.log('  Adding PG indexes...')
  await pg.query('CREATE INDEX IF NOT EXISTS idx_members_text ON members(text_val)')
  await pg.query('CREATE INDEX IF NOT EXISTS idx_members_int ON members(int_val)')
  console.log('  Done.\n')

  const W = 100
  const R = 5000
  const PG_W = 20
  const PG_R = 500

  console.log('  ┌──────────────────────────┬──────────┬──────────┬──────────┐')
  console.log('  │ クエリ                   │  PG+idx  │   Extend │     倍率 │')
  console.log('  ├──────────────────────────┼──────────┼──────────┼──────────┤')

  // 完全一致 (entity_id, PGにPKインデックスあり)
  const pg1 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query('SELECT * FROM members WHERE entity_id = $1 LIMIT 1', [42])
  })
  const en1 = bench(W, R, () => {
    enchu.slice('entities', 'entity_id', 42)
  })
  row('完全一致(entity_id)', pg1, en1)

  // 完全一致 (text_val, PGにインデックスあり)
  const pg2 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query("SELECT * FROM members WHERE text_val = $1", ['entity_42_members_row_0'])
  })
  const textVid = enchu.vocabLookup('entity_42_members_row_0')
  const en2 = textVid != null ? bench(W, R, () => {
    enchu.slice('members', 'text_val', textVid!)
  }) : 0
  row('完全一致(text+idx)', pg2, en2)

  // 範囲 (int_val, PGにインデックスあり)
  const pg3 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query('SELECT count(*) FROM members WHERE int_val BETWEEN $1 AND $2', [100, 200])
  })
  const en3 = bench(W, R, () => {
    enchu.sliceRange('members', 'int_val', 100, 200)
  })
  row('範囲(int+idx)', pg3, en3)

  // 親辿り
  const pg4 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query('SELECT entity_id FROM members WHERE entity_id = $1 AND row_id = 0', [42])
  })
  const en4 = bench(W, R, () => {
    enchu.parent('members', 42)
  })
  row('親辿り(parent)', pg4, en4)

  // 子一覧
  const pg5 = await benchAsync(PG_W, PG_R, async () => {
    await pg.query('SELECT * FROM members WHERE entity_id = $1', [42])
  })
  const en5 = bench(W, R, () => {
    enchu.children('members', 42)
  })
  row('子一覧(children)', pg5, en5)

  console.log('  └──────────────────────────┴──────────┴──────────┴──────────┘')

  // ════════════════════════════════════════
  // 4. キャッシュ無効化テスト
  // ════════════════════════════════════════
  console.log('\n╔══════════════════════════════════════╗')
  console.log('║  4. キャッシュ無効化                  ║')
  console.log('╚══════════════════════════════════════╝\n')

  // PGで更新
  await pg.query("UPDATE members SET text_val = 'UPDATED' WHERE entity_id = 42 AND row_id = 0")

  // Extend側はまだ古いデータ
  const kids42 = enchu.children('members', 42)
  const oldTexts = enchu.getColumnText('members', 'text_val', kids42)
  console.log(`  PG更新後、Extend: ${oldTexts[0]} (古い)`)

  // 再ロード（entity_id=42のmembersだけ）
  const t1 = performance.now()
  const updated = await pg.query('SELECT text_val, int_val FROM members WHERE entity_id = 42 ORDER BY row_id', [])
  // 既存のchildrenを読む
  const existingKids = enchu.children('members', 42)
  // 各行のデータを更新
  for (let i = 0; i < Math.min(updated.rows.length, existingKids.length); i++) {
    const eid = existingKids[i]
    enchu.writeText('members', eid, 'text_val', updated.rows[i].text_val || '')
    enchu.writeInt('members', eid, 'int_val', parseInt(updated.rows[i].int_val) || 0)
  }
  enchu.flush()
  const invalidateTime = performance.now() - t1

  const newTexts = enchu.getColumnText('members', 'text_val', existingKids)
  console.log(`  再ロード後、Extend: ${newTexts[0]} (新しい)`)
  console.log(`  無効化+再ロード: ${invalidateTime.toFixed(1)}ms`)

  // 元に戻す
  await pg.query("UPDATE members SET text_val = 'entity_42_members_row_0' WHERE entity_id = 42 AND row_id = 0")

  // ════════════════════════════════════════
  // 5. メモリ使用量
  // ════════════════════════════════════════
  console.log('\n╔══════════════════════════════════════╗')
  console.log('║  5. メモリ使用量                      ║')
  console.log('╚══════════════════════════════════════╝\n')

  const { execSync } = require('child_process')
  const du = execSync(`du -sh ${CACHE_DIR}`).toString().trim()
  console.log(`  ディスク使用量: ${du}`)

  const memUsage = process.memoryUsage()
  console.log(`  RSS: ${(memUsage.rss / 1024 / 1024).toFixed(1)}MB`)
  console.log(`  Heap: ${(memUsage.heapUsed / 1024 / 1024).toFixed(1)}MB`)
  console.log(`  External: ${(memUsage.external / 1024 / 1024).toFixed(1)}MB`)

  // ════════════════════════════════════════
  // 6. 同時接続テスト
  // ════════════════════════════════════════
  console.log('\n╔══════════════════════════════════════╗')
  console.log('║  6. 同時接続テスト                    ║')
  console.log('╚══════════════════════════════════════╝\n')

  // 疑似並行: Promise.allで同時にslice
  const CONCURRENT = 100
  const t2 = performance.now()
  const promises = Array.from({ length: CONCURRENT }, (_, i) => {
    return new Promise<void>((resolve) => {
      enchu.slice('entities', 'entity_id', i)
      enchu.children('members', i)
      resolve()
    })
  })
  await Promise.all(promises)
  const concurrentTime = performance.now() - t2
  console.log(`  ${CONCURRENT}件同時slice+children: ${concurrentTime.toFixed(2)}ms`)
  console.log(`  1件あたり: ${(concurrentTime / CONCURRENT * 1000).toFixed(0)}µs`)

  // クリーンアップ
  await pg.query('DROP INDEX IF EXISTS idx_members_text')
  await pg.query('DROP INDEX IF EXISTS idx_members_int')
  await pg.end()

  console.log('\n  All done.')
}

function row(label: string, pg: number, en: number) {
  const speedup = en > 0 ? `${(pg / en).toFixed(0)}x` : 'N/A'
  console.log(`  │ ${label.padEnd(24)} │ ${fmt(pg).padStart(8)} │ ${fmt(en).padStart(8)} │ ${speedup.padStart(8)} │`)
}

main().catch(console.error)
