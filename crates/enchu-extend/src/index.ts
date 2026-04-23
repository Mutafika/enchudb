/**
 * Enchu Extend — PG透過キャッシュ
 *
 * ```ts
 * import { enchu } from 'enchu-extend'
 * const db = await enchu('postgresql://...')
 * const results = db.products.filter({ color: 'red', size: 'M' })
 * ```
 */
export { enchu } from './client'
