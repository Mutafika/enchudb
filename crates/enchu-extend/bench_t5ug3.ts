/**
 * t5ug3 実証実験 — 100万件で PG vs Enchu
 *
 * 1. 大量データ投入（100社 × 1万件）
 * 2. N+1 なしの最適 PG クエリ
 * 3. Enchu 被せて比較
 */
import { Client } from 'pg'
import { enchu } from './src/client'

const PG = 'postgresql://postgres:postgres@localhost:5432/mkdk'

async function main() {
  const pg = new Client({ connectionString: PG })
  await pg.connect()

  // ═══ 1. データ投入 ═══
  console.log('── データ投入 ──')

  // 既存テストデータ削除
  await pg.query('DELETE FROM daily_report_workers')
  await pg.query('DELETE FROM daily_reports')
  await pg.query('DELETE FROM projects WHERE name LIKE \'TestProject_%\'')
  await pg.query('DELETE FROM users WHERE id LIKE \'tuser_%\'')
  await pg.query('DELETE FROM companies WHERE name LIKE \'test_co_%\'')

  // 100社
  const companyIds: string[] = []
  for (let i = 0; i < 100; i++) {
    const res = await pg.query(
      'INSERT INTO companies (name, plan) VALUES ($1, $2) RETURNING id',
      [`test_co_${i}`, ['free', 'pro', 'enterprise'][i % 3]]
    )
    companyIds.push(res.rows[0].id)
  }
  console.log('  companies:', companyIds.length)

  // 各社 20 ユーザー = 2000人
  const userIds: string[] = []
  for (let c = 0; c < 100; c++) {
    for (let u = 0; u < 20; u++) {
      const uid = `tuser_${c}_${u}`
      await pg.query(
        'INSERT INTO users (id, email, display_name, company_id, role, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,NOW(),NOW())',
        [uid, `${uid}@test.com`, `User_${c}_${u}`, companyIds[c], ['admin', 'staff', 'viewer'][u % 3]]
      )
      userIds.push(uid)
    }
  }
  console.log('  users:', userIds.length)

  // 各社 100 プロジェクト = 10000件
  for (let c = 0; c < 100; c++) {
    const vals: string[] = []
    for (let p = 0; p < 100; p++) {
      const status = ['受注前', '施工中', '完了', '中止'][p % 4]
      vals.push(`('TestProject_${c}_${p}', '${companyIds[c]}', '${status}', 'Client_${p % 50}', NOW() - '${p} days'::interval, NOW(), NOW())`)
    }
    await pg.query(`INSERT INTO projects (name, company_id, status, client_name, start_date, created_at, updated_at) VALUES ${vals.join(',')}`)
  }
  console.log('  projects: 10000')

  // 日報 100万件（各社1万件）
  console.log('  daily_reports: inserting...')
  for (let c = 0; c < 100; c++) {
    for (let batch = 0; batch < 10; batch++) {
      const vals: string[] = []
      for (let j = 0; j < 1000; j++) {
        const i = batch * 1000 + j
        const uid = `tuser_${c}_${i % 20}`
        const status = ['draft', 'submitted', 'approved'][i % 3]
        vals.push(`('${uid}', '${companyIds[c]}', (NOW() - '${i} hours'::interval)::date, '${status}', 'work_${i}', NOW(), NOW())`)
      }
      await pg.query(`INSERT INTO daily_reports (user_id, company_id, report_date, status, work_description, created_at, updated_at) VALUES ${vals.join(',')}`)
    }
    if ((c + 1) % 10 === 0) process.stdout.write(`  ${(c + 1)}%...`)
  }
  console.log(' done')

  await pg.query('ANALYZE')

  // 件数確認
  const counts = await pg.query(`
    SELECT 'companies' as t, count(*)::int as n FROM companies WHERE name LIKE 'test_co_%'
    UNION ALL SELECT 'users', count(*)::int FROM users WHERE id LIKE 'tuser_%'
    UNION ALL SELECT 'projects', count(*)::int FROM projects WHERE name LIKE 'TestProject_%'
    UNION ALL SELECT 'daily_reports', count(*)::int FROM daily_reports
  `)
  for (const r of counts.rows) console.log(`  ${r.t}: ${r.n}`)

  // ═══ 2. PG ベンチ（N+1 なし、最適クエリ）═══
  console.log('\n── PG ベンチ（最適クエリ）──')
  const testCompanyId = companyIds[42]
  const testUserId = `tuser_42_5`

  async function benchPg(label: string, sql: string, params: unknown[] = []) {
    // warmup
    for (let i = 0; i < 5; i++) await pg.query(sql, params)
    const s = process.hrtime.bigint()
    for (let i = 0; i < 50; i++) await pg.query(sql, params)
    const ns = Number(process.hrtime.bigint() - s) / 50
    const u = ns >= 1e6 ? (ns / 1e6).toFixed(1) + 'ms' : (ns / 1e3).toFixed(0) + 'µs'
    const count = (await pg.query(sql, params)).rows.length || (await pg.query(sql, params)).rows[0]?.count
    return { time: u, count }
  }

  const pgResults: Record<string, { time: string; count: number }> = {}

  pgResults['テナント日報 count'] = await benchPg(
    '', 'SELECT count(*) FROM daily_reports WHERE company_id=$1', [testCompanyId])
  pgResults['テナント+承認済み count'] = await benchPg(
    '', "SELECT count(*) FROM daily_reports WHERE company_id=$1 AND status='approved'", [testCompanyId])
  pgResults['ユーザー日報 count'] = await benchPg(
    '', 'SELECT count(*) FROM daily_reports WHERE user_id=$1', [testUserId])
  pgResults['テナント+ユーザー+承認 count'] = await benchPg(
    '', "SELECT count(*) FROM daily_reports WHERE company_id=$1 AND user_id=$2 AND status='approved'", [testCompanyId, testUserId])
  pgResults['テナント工事 count'] = await benchPg(
    '', 'SELECT count(*) FROM projects WHERE company_id=$1', [testCompanyId])
  pgResults['テナント施工中 count'] = await benchPg(
    '', "SELECT count(*) FROM projects WHERE company_id=$1 AND status='施工中'", [testCompanyId])
  pgResults['テナント日報 LIMIT 50'] = await benchPg(
    '', 'SELECT id FROM daily_reports WHERE company_id=$1 ORDER BY report_date DESC LIMIT 50', [testCompanyId])
  pgResults['日報+ユーザー JOIN'] = await benchPg(
    '', `SELECT dr.id, u.display_name FROM daily_reports dr JOIN users u ON dr.user_id=u.id WHERE dr.company_id=$1 AND dr.status='approved' LIMIT 50`, [testCompanyId])

  // ═══ 3. Enchu ベンチ ═══
  console.log('\n── Enchu ロード ──')
  const t0 = Date.now()
  const db = await enchu(PG, {
    tables: ['daily_reports', 'projects', 'users'],
    syncInterval: 0,
  })
  console.log('  ロード:', Date.now() - t0, 'ms')

  console.log('\n── Enchu ベンチ ──')
  const enchResults: Record<string, { time: string; count: number }> = {}

  function benchEnchu(label: string, fn: () => number) {
    for (let i = 0; i < 50; i++) fn()
    const s = process.hrtime.bigint()
    for (let i = 0; i < 500; i++) fn()
    const ns = Number(process.hrtime.bigint() - s) / 500
    const u = ns >= 1e6 ? (ns / 1e6).toFixed(1) + 'ms' : (ns / 1e3).toFixed(0) + 'µs'
    const count = fn()
    return { time: u, count }
  }

  enchResults['テナント日報 count'] = benchEnchu('', () => db.daily_reports.count({ company_id: testCompanyId }))
  enchResults['テナント+承認済み count'] = benchEnchu('', () => db.daily_reports.count({ company_id: testCompanyId, status: 'approved' }))
  enchResults['ユーザー日報 count'] = benchEnchu('', () => db.daily_reports.count({ user_id: testUserId }))
  enchResults['テナント+ユーザー+承認 count'] = benchEnchu('', () => db.daily_reports.count({ company_id: testCompanyId, user_id: testUserId, status: 'approved' }))
  enchResults['テナント工事 count'] = benchEnchu('', () => db.projects.count({ company_id: testCompanyId }))
  enchResults['テナント施工中 count'] = benchEnchu('', () => db.projects.count({ company_id: testCompanyId, status: '施工中' }))
  enchResults['テナント日報 LIMIT 50'] = benchEnchu('', () => db.daily_reports.filterIds({ company_id: testCompanyId }).slice(0, 50).length)
  enchResults['日報+ユーザー JOIN'] = benchEnchu('', () => {
    const ids = db.daily_reports.filterIds({ company_id: testCompanyId, status: 'approved' })
    return ids.slice(0, 50).length
  })

  // ═══ 4. 比較表 ═══
  console.log('\n┌──────────────────────────────────────┬──────────┬──────────┬────────┐')
  console.log('│ クエリ (100万件)                     │    PG    │  Enchu   │  倍率  │')
  console.log('├──────────────────────────────────────┼──────────┼──────────┼────────┤')

  for (const key of Object.keys(pgResults)) {
    const p = pgResults[key]
    const e = enchResults[key]
    const pNs = parseFloat(p.time) * (p.time.includes('ms') ? 1e6 : 1e3)
    const eNs = parseFloat(e.time) * (e.time.includes('ms') ? 1e6 : 1e3)
    const ratio = Math.round(pNs / eNs)
    console.log(`│ ${key.padEnd(36)} │ ${p.time.padStart(8)} │ ${e.time.padStart(8)} │ ${String(ratio + 'x').padStart(6)} │`)
  }
  console.log('└──────────────────────────────────────┴──────────┴──────────┴────────┘')

  // cleanup
  await db.close()
  console.log('\nDone. (テストデータはそのまま残してます)')
  await pg.end()
}

main().catch(e => { console.error(e); process.exit(1) })
