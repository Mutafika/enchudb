/**
 * Enchu Extend — PG透過キャッシュ
 *
 * const db = await enchu('postgresql://...')
 * const results = db.products.filter({ color: 'red', size: 'M' })
 * // → [{ id: 1, color: 'red', size: 'M', price: 2000 }, ...]
 *
 * v23エンジン（紐モデル）でPGテーブルを透過的にキャッシュ。
 * 紐名は "table.field" 形式。検索は円柱交差で µs オーダー。
 */
import { Client } from 'pg'

const native = require('../native')

interface FilterOptions {
  limit?: number
  offset?: number
}

interface TableProxy {
  filter(conditions: Record<string, string | number>, options?: FilterOptions): Record<string, unknown>[]
  filterIds(conditions: Record<string, string | number>, options?: FilterOptions): number[]
  count(conditions?: Record<string, string | number>): number
  all(options?: FilterOptions): Record<string, unknown>[]
}

interface EnchuDb {
  close: () => Promise<void>
  [table: string]: TableProxy | (() => Promise<void>)
}

let instanceCounter = 0

export async function enchu(pgConn: string, options?: {
  name?: string
  dbPath?: string
  syncInterval?: number
  tables?: string[]
}): Promise<EnchuDb> {
  const instanceName = options?.name ?? `enchu_${instanceCounter++}`
  const dbPath = options?.dbPath ?? `/tmp/enchu_cache_${instanceName}.db`
  const syncInterval = options?.syncInterval ?? 1000

  const { unlinkSync } = await import('fs')
  try { unlinkSync(dbPath) } catch {}

  // 1. PGからスキーマ取得
  const pg = new Client({ connectionString: pgConn })
  await pg.connect()

  const tableFilter = options?.tables ? `AND table_name = ANY($1)` : ''
  const params = options?.tables ? [options.tables] : []

  const schemaRows = await pg.query(`
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' ${tableFilter}
    ORDER BY table_name, ordinal_position
  `, params)

  const tableSchemas: Record<string, { name: string, type: string }[]> = {}
  for (const row of schemaRows.rows) {
    if (!tableSchemas[row.table_name]) tableSchemas[row.table_name] = []
    tableSchemas[row.table_name].push({
      name: row.column_name,
      type: row.data_type,
    })
  }

  // 2. エンジン初期化 + 紐定義
  native.init(dbPath, pgConn)

  for (const [tableName, fields] of Object.entries(tableSchemas)) {
    for (const field of fields) {
      const himo = `${tableName}.${field.name}`
      const ht = isIntType(field.type) ? 'value' : 'symbol'
      native.defineHimo(himo, ht, 0)
    }
  }

  // 3. 全データロード（テーブルごとの entity ID 範囲を記録）
  const tableEntityIds: Record<string, number[]> = {}
  for (const [tableName, fields] of Object.entries(tableSchemas)) {
    const columns = fields.map(f => `"${f.name}"`).join(', ')
    const res = await pg.query(`SELECT ${columns} FROM "${tableName}" ORDER BY 1`)
    const ids: number[] = []

    for (const row of res.rows) {
      const eid = native.entity()
      ids.push(eid)
      for (const field of fields) {
        const val = row[field.name]
        if (val === null || val === undefined) continue
        const himo = `${tableName}.${field.name}`
        if (isIntType(field.type)) {
          native.tie(eid, himo, Number(val))
        } else {
          native.tieText(eid, himo, String(val))
        }
      }
    }
    tableEntityIds[tableName] = ids
  }
  native.rebuild()
  native.flush()

  // 4. 紐ID解決キャッシュ
  const himoIds: Record<string, Record<string, number>> = {}
  const fieldTypes: Record<string, Record<string, string>> = {}

  for (const [tableName, fields] of Object.entries(tableSchemas)) {
    himoIds[tableName] = {}
    fieldTypes[tableName] = {}
    for (const field of fields) {
      himoIds[tableName][field.name] = native.resolveField(tableName, field.name)
      fieldTypes[tableName][field.name] = field.type
    }
  }

  // 5. 差分同期
  let syncTimer: ReturnType<typeof setInterval> | null = null
  let syncPg: InstanceType<typeof Client> | null = null
  let syncErrors = 0

  if (syncInterval > 0) {
    syncPg = new Client({ connectionString: pgConn })
    await syncPg.connect()

    syncTimer = setInterval(async () => {
      if (!syncPg) return
      try {
        for (const [tableName, fields] of Object.entries(tableSchemas)) {
          const hasUpdatedAt = fields.some(f => f.name === 'updated_at')
          if (!hasUpdatedAt) continue

          const updated = await syncPg.query(
            `SELECT * FROM "${tableName}" WHERE updated_at > NOW() - INTERVAL '${Math.ceil(syncInterval / 1000) + 1} seconds' ORDER BY 1`
          )
          if (updated.rows.length === 0) continue

          for (const row of updated.rows) {
            const eid = Number(row.id) - 1
            for (const field of fields) {
              const val = row[field.name]
              if (val === null || val === undefined) continue
              const himo = `${tableName}.${field.name}`
              if (isIntType(field.type)) {
                native.tie(eid, himo, Number(val))
              } else {
                native.tieText(eid, himo, String(val))
              }
            }
          }
          native.rebuild()
          native.flush()
        }
        syncErrors = 0
      } catch (err) {
        syncErrors++
        if (syncErrors >= 3) {
          // PG接続リカバリ
          try {
            await syncPg.end().catch(() => {})
            syncPg = new Client({ connectionString: pgConn })
            await syncPg.connect()
            syncErrors = 0
          } catch {
            // リカバリ失敗、次回再試行
          }
        }
      }
    }, syncInterval)
  }

  // 6. Proxy API
  const db = new Proxy({} as EnchuDb, {
    get(_, prop: string) {
      if (prop === 'close') {
        return async () => {
          if (syncTimer) clearInterval(syncTimer)
          if (syncPg) await syncPg.end().catch(() => {})
          native.close()
          await pg.end().catch(() => {})
        }
      }

      const tName = prop
      if (!tableSchemas[tName]) return undefined

      const fids = himoIds[tName]
      const ftypes = fieldTypes[tName]
      const fields = tableSchemas[tName]

      // 条件→ entity ID リスト (共通ロジック)
      function resolveIds(conditions: Record<string, string | number>, opts?: FilterOptions): number[] {
        const conds: [number, number][] = []
        for (const [key, val] of Object.entries(conditions)) {
          const fid = fids[key]
          if (fid === undefined) continue
          let numVal: number
          if (typeof val === 'string') {
            const vid = native.vocabLookup(val)
            if (vid === null || vid === undefined) return []
            numVal = vid
          } else {
            numVal = val
          }
          conds.push([fid, numVal])
        }
        if (conds.length === 0) {
          const allIds = tableEntityIds[tName] ?? []
          const offset = opts?.offset ?? 0
          const limit = opts?.limit ?? allIds.length
          return allIds.slice(offset, offset + limit)
        }
        let ids = bufToIds(native.queryFast(0, condsToBuffer(conds)))
        const offset = opts?.offset ?? 0
        const limit = opts?.limit ?? ids.length
        if (offset > 0 || limit < ids.length) {
          ids = ids.slice(offset, offset + limit)
        }
        return ids
      }

      return {
        filter(conditions: Record<string, string | number>, opts?: FilterOptions): Record<string, unknown>[] {
          const ids = resolveIds(conditions, opts)
          return idsToObjects(tName, ids, fields, ftypes)
        },

        filterIds(conditions: Record<string, string | number>, opts?: FilterOptions): number[] {
          return resolveIds(conditions, opts)
        },

        count(conditions?: Record<string, string | number>): number {
          if (!conditions || Object.keys(conditions).length === 0) {
            return tableEntityIds[tName]?.length ?? 0
          }

          const conds: [number, number][] = []
          for (const [key, val] of Object.entries(conditions)) {
            const fid = fids[key]
            if (fid === undefined) continue

            let numVal: number
            if (typeof val === 'string') {
              const vid = native.vocabLookup(val)
              if (vid === null || vid === undefined) return 0
              numVal = vid
            } else {
              numVal = val
            }
            conds.push([fid, numVal])
          }

          return native.queryCount(0, condsToBuffer(conds))
        },

        all(opts?: FilterOptions): Record<string, unknown>[] {
          const allIds = tableEntityIds[tName] ?? []
          const offset = opts?.offset ?? 0
          const limit = opts?.limit ?? allIds.length
          const ids = allIds.slice(offset, offset + limit)
          return idsToObjects(tName, ids, fields, ftypes)
        },
      } as TableProxy
    },
  })

  return db
}

// ════════════════ ヘルパー ════════════════

function condsToBuffer(conds: [number, number][]): Buffer {
  const buf = Buffer.alloc(conds.length * 8)
  for (let i = 0; i < conds.length; i++) {
    buf.writeUInt32LE(conds[i][0], i * 8)
    buf.writeUInt32LE(conds[i][1], i * 8 + 4)
  }
  return buf
}

function bufToIds(buf: Buffer): number[] {
  const arr = new Uint32Array(buf.buffer, buf.byteOffset, buf.length / 4)
  return Array.from(arr)
}

function idsToObjects(
  table: string,
  ids: number[],
  fields: { name: string; type: string }[],
  _ftypes: Record<string, string>,
): Record<string, unknown>[] {
  if (ids.length === 0) return []

  const idBuf = Buffer.alloc(ids.length * 4)
  for (let i = 0; i < ids.length; i++) idBuf.writeUInt32LE(ids[i], i * 4)

  const fieldDefs = fields.map(f => `${f.name}:${isIntType(f.type) ? 'int' : 'text'}`)
  const buf: Buffer = native.batchGet(idBuf, table, fieldDefs)

  const result: Record<string, unknown>[] = []
  let off = 0
  for (const id of ids) {
    const obj: Record<string, unknown> = { _id: id }
    for (const field of fields) {
      if (isIntType(field.type)) {
        const v = buf.readUInt32LE(off)
        obj[field.name] = v === 0xFFFFFFFF ? null : v
        off += 4
      } else {
        const len = buf.readUInt32LE(off)
        off += 4
        if (len === 0xFFFFFFFF) {
          obj[field.name] = null
        } else {
          obj[field.name] = buf.toString('utf8', off, off + len)
          off += len
        }
      }
    }
    result.push(obj)
  }
  return result
}

function isIntType(pgType: string): boolean {
  return /integer|int[248]?|smallint|bigint|serial|bigserial/.test(pgType)
}
