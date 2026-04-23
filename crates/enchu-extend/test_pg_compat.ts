import { Client } from 'pg'
import { enchu } from './src/client'
const PG = 'postgresql://postgres:enchu@localhost:5433/enchu_bench'

let passed = 0, failed = 0
function assert(name: string, cond: boolean, detail?: string) {
  if (cond) { console.log('  ✓', name); passed++ }
  else { console.log('  ✗', name, detail ? '— ' + detail : ''); failed++ }
}

async function pgCount(pg: InstanceType<typeof Client>, sql: string): Promise<number> {
  return parseInt((await pg.query(sql)).rows[0].count)
}

async function main() {
  const pg = new Client({ connectionString: PG })
  await pg.connect()

  console.log('╔════════════════════════════════════════════════════════════════╗')
  console.log('║     PG 互換性テスト — 全件PG比較                             ║')
  console.log('╚════════════════════════════════════════════════════════════════╝')

  // ═══ 1. カラム型 1000行 ═══
  console.log('\n── カラム型 (1000行) ──')
  await pg.query('DROP TABLE IF EXISTS type_test CASCADE')
  await pg.query(`CREATE TABLE type_test (
    id SERIAL PRIMARY KEY,
    t_text TEXT NOT NULL, t_varchar VARCHAR(100) NOT NULL,
    t_int INTEGER NOT NULL, t_bigint BIGINT NOT NULL, t_smallint SMALLINT NOT NULL,
    t_bool BOOLEAN NOT NULL, t_timestamp TIMESTAMP NOT NULL, t_date DATE NOT NULL,
    t_uuid UUID NOT NULL, t_jsonb JSONB NOT NULL
  )`)
  const dates = ['2026-01-01', '2026-06-15', '2026-12-31', '2025-03-10', '2024-07-04']
  for (let i = 0; i < 1000; i++) {
    await pg.query(
      `INSERT INTO type_test (t_text,t_varchar,t_int,t_bigint,t_smallint,t_bool,t_timestamp,t_date,t_uuid,t_jsonb)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [['red', 'blue', 'green', 'yellow', 'black'][i % 5], ['S', 'M', 'L', 'XL'][i % 4],
        i % 100, BigInt(i) * 1000000n, i % 10, i % 2 === 0,
        `2026-01-01 ${String(i % 24).padStart(2, '0')}:00:00`, dates[i % 5],
        `a0eebc99-9c0b-4ef8-bb6d-6bb9bd38${String(i % 10000).padStart(4, '0')}`,
        JSON.stringify({ cat: ['shirt', 'pants', 'hat'][i % 3] })])
  }
  {
    const db = await enchu(PG, { tables: ['type_test'], syncInterval: 0 })
    const pgTotal = await pgCount(pg, 'SELECT count(*) FROM type_test')
    assert('全件', db.type_test.count() === pgTotal, `E=${db.type_test.count()} PG=${pgTotal}`)

    // 全カラムの単条件filter — PGと一致確認
    for (const [col, val, sql] of [
      ['t_text', 'red', "t_text='red'"],
      ['t_text', 'blue', "t_text='blue'"],
      ['t_varchar', 'M', "t_varchar='M'"],
      ['t_varchar', 'XL', "t_varchar='XL'"],
      ['t_int', 0, 't_int=0'],
      ['t_int', 42, 't_int=42'],
      ['t_int', 99, 't_int=99'],
      ['t_smallint', 5, 't_smallint=5'],
      ['t_smallint', 0, 't_smallint=0'],
    ] as [string, string | number, string][]) {
      const eC = db.type_test.filter({ [col]: val }).length
      const pC = await pgCount(pg, `SELECT count(*) FROM type_test WHERE ${sql}`)
      assert(`${col}=${val} E=${eC} PG=${pC}`, eC === pC)
      // 中身確認
      const rows = db.type_test.filter({ [col]: val })
      assert(`${col}=${val} 中身正しい`, rows.every((r: Record<string, unknown>) => r[col] === val))
    }

    // 複合条件 — 全パターン PG比較
    for (const [cond, sql] of [
      [{ t_text: 'red', t_varchar: 'M' }, "t_text='red' AND t_varchar='M'"],
      [{ t_text: 'blue', t_varchar: 'L' }, "t_text='blue' AND t_varchar='L'"],
      [{ t_text: 'red', t_int: 42 }, "t_text='red' AND t_int=42"],
      [{ t_text: 'green', t_int: 0 }, "t_text='green' AND t_int=0"],
      [{ t_varchar: 'S', t_smallint: 3 }, "t_varchar='S' AND t_smallint=3"],
      [{ t_text: 'red', t_varchar: 'M', t_smallint: 5 }, "t_text='red' AND t_varchar='M' AND t_smallint=5"],
    ] as [Record<string, string | number>, string][]) {
      const eC = db.type_test.filter(cond).length
      const pC = await pgCount(pg, `SELECT count(*) FROM type_test WHERE ${sql}`)
      const label = Object.entries(cond).map(([k, v]) => `${k}=${v}`).join('+')
      assert(`複合 ${label} E=${eC} PG=${pC}`, eC === pC)
    }

    // count = filter.length
    for (const cond of [{ t_text: 'red' }, { t_int: 0 }, { t_text: 'blue', t_varchar: 'L' }]) {
      assert('count=filter ' + JSON.stringify(cond), db.type_test.count(cond) === db.type_test.filter(cond).length)
    }

    // filterIds = filter.length
    for (const cond of [{ t_text: 'red' }, { t_text: 'green', t_varchar: 'M' }]) {
      assert('filterIds=filter ' + JSON.stringify(cond), db.type_test.filterIds(cond).length === db.type_test.filter(cond).length)
    }

    // limit/offset
    const allRed = db.type_test.filter({ t_text: 'red' })
    const p1 = db.type_test.filter({ t_text: 'red' }, { limit: 10 })
    const p2 = db.type_test.filter({ t_text: 'red' }, { limit: 10, offset: 10 })
    assert('limit 10', p1.length === 10)
    assert('offset 動作', JSON.stringify(p1[0]) !== JSON.stringify(p2[0]))
    const last = db.type_test.filter({ t_text: 'red' }, { limit: 50, offset: allRed.length - 5 })
    assert('末尾ページ', last.length === 5, 'got ' + last.length)

    // 存在しない値
    assert('存在しない TEXT', db.type_test.filter({ t_text: 'purple' }).length === 0)
    assert('存在しない INT', db.type_test.filter({ t_int: 999 }).length === 0)
    await db.close()
  }

  // ═══ 2. NULL 50% ═══
  console.log('\n── NULL大量 (500行, 50%NULL) ──')
  await pg.query('DROP TABLE IF EXISTS null_heavy CASCADE')
  await pg.query('CREATE TABLE null_heavy (id SERIAL PRIMARY KEY, name TEXT, color TEXT, score INTEGER)')
  for (let i = 0; i < 500; i++) {
    const name = i % 2 === 0 ? `'name_${i}'` : 'NULL'
    const color = i % 3 === 0 ? `'${['red', 'blue', 'green'][Math.floor(i / 3) % 3]}'` : 'NULL'
    const score = i % 4 === 0 ? String(i % 100) : 'NULL'
    await pg.query(`INSERT INTO null_heavy (name, color, score) VALUES (${name}, ${color}, ${score})`)
  }
  {
    const db = await enchu(PG, { tables: ['null_heavy'], syncInterval: 0 })
    const pgT = await pgCount(pg, 'SELECT count(*) FROM null_heavy')
    assert('NULL全件', db.null_heavy.count() === pgT)

    for (const [col, val, sql] of [
      ['color', 'red', "color='red'"],
      ['color', 'blue', "color='blue'"],
      ['color', 'green', "color='green'"],
    ] as [string, string, string][]) {
      const eC = db.null_heavy.filter({ [col]: val }).length
      const pC = await pgCount(pg, `SELECT count(*) FROM null_heavy WHERE ${sql}`)
      assert(`NULL混在 ${col}=${val} E=${eC} PG=${pC}`, eC === pC)
      assert(`NULL混在 ${col}=${val} 中身`, db.null_heavy.filter({ [col]: val }).every((r: Record<string, unknown>) => r[col] === val))
    }

    // NULL混在 複合
    const eC = db.null_heavy.filter({ color: 'red', score: 0 }).length
    const pC = await pgCount(pg, "SELECT count(*) FROM null_heavy WHERE color='red' AND score=0")
    assert(`NULL複合 red+0 E=${eC} PG=${pC}`, eC === pC)
    await db.close()
  }

  // ═══ 3. UUID主キー 200行 ═══
  console.log('\n── UUID主キー (200行) ──')
  await pg.query('DROP TABLE IF EXISTS uuid_pk CASCADE')
  await pg.query('CREATE TABLE uuid_pk (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT, color TEXT, price INTEGER)')
  for (let i = 0; i < 200; i++) {
    await pg.query('INSERT INTO uuid_pk (name,color,price) VALUES ($1,$2,$3)',
      [`item_${i}`, ['red', 'blue', 'green', 'yellow'][i % 4], i * 10])
  }
  {
    const db = await enchu(PG, { tables: ['uuid_pk'], syncInterval: 0 })
    const pgT = await pgCount(pg, 'SELECT count(*) FROM uuid_pk')
    assert('UUID全件', db.uuid_pk.count() === pgT)
    for (const c of ['red', 'blue', 'green', 'yellow']) {
      const eC = db.uuid_pk.filter({ color: c }).length
      const pC = await pgCount(pg, `SELECT count(*) FROM uuid_pk WHERE color='${c}'`)
      assert(`UUID ${c} E=${eC} PG=${pC}`, eC === pC)
    }
    const eC = db.uuid_pk.filter({ color: 'red', price: 0 }).length
    const pC = await pgCount(pg, "SELECT count(*) FROM uuid_pk WHERE color='red' AND price=0")
    assert(`UUID複合 E=${eC} PG=${pC}`, eC === pC)
    assert('UUID limit', db.uuid_pk.filter({ color: 'red' }, { limit: 5 }).length === 5)
    assert('UUID filterIds', db.uuid_pk.filterIds({ color: 'green' }).length === db.uuid_pk.filter({ color: 'green' }).length)
    await db.close()
  }

  // ═══ 4. 複合主キー 300行 ═══
  console.log('\n── 複合主キー (300行) ──')
  await pg.query('DROP TABLE IF EXISTS composite_pk CASCADE')
  await pg.query('CREATE TABLE composite_pk (tenant_id INT, product_id INT, name TEXT, color TEXT, PRIMARY KEY(tenant_id, product_id))')
  for (let t = 0; t < 10; t++) for (let p = 0; p < 30; p++)
    await pg.query('INSERT INTO composite_pk VALUES ($1,$2,$3,$4)', [t, p, `item_${t}_${p}`, ['red', 'blue', 'green'][p % 3]])
  {
    const db = await enchu(PG, { tables: ['composite_pk'], syncInterval: 0 })
    for (const [cond, sql] of [
      [{ color: 'red' }, "color='red'"],
      [{ tenant_id: 0 }, 'tenant_id=0'],
      [{ tenant_id: 0, color: 'red' }, "tenant_id=0 AND color='red'"],
      [{ color: 'blue' }, "color='blue'"],
    ] as [Record<string, string | number>, string][]) {
      const eC = db.composite_pk.filter(cond).length
      const pC = await pgCount(pg, `SELECT count(*) FROM composite_pk WHERE ${sql}`)
      const label = Object.entries(cond).map(([k, v]) => `${k}=${v}`).join('+')
      assert(`複合PK ${label} E=${eC} PG=${pC}`, eC === pC)
    }
    await db.close()
  }

  // ═══ 5. id飛び番 ═══
  console.log('\n── id飛び番 ──')
  await pg.query('DROP TABLE IF EXISTS gap_id CASCADE')
  await pg.query('CREATE TABLE gap_id (id SERIAL PRIMARY KEY, color TEXT)')
  for (let i = 0; i < 100; i++) await pg.query('INSERT INTO gap_id (color) VALUES ($1)', [['red', 'blue'][i % 2]])
  await pg.query('DELETE FROM gap_id WHERE id % 2 = 0')
  {
    const db = await enchu(PG, { tables: ['gap_id'], syncInterval: 0 })
    const pgT = await pgCount(pg, 'SELECT count(*) FROM gap_id')
    assert('飛び番 全件', db.gap_id.count() === pgT, `E=${db.gap_id.count()} PG=${pgT}`)
    for (const c of ['red', 'blue']) {
      const eC = db.gap_id.filter({ color: c }).length
      const pC = await pgCount(pg, `SELECT count(*) FROM gap_id WHERE color='${c}'`)
      assert(`飛び番 ${c} E=${eC} PG=${pC}`, eC === pC)
    }
    await db.close()
  }

  // ═══ 6. 空テーブル ═══
  console.log('\n── 空テーブル ──')
  await pg.query('DROP TABLE IF EXISTS empty_t CASCADE')
  await pg.query('CREATE TABLE empty_t (id SERIAL PRIMARY KEY, name TEXT)')
  {
    const db = await enchu(PG, { tables: ['empty_t'], syncInterval: 0 })
    assert('空 all=0', db.empty_t.all().length === 0)
    assert('空 count=0', db.empty_t.count() === 0)
    assert('空 filter=[]', db.empty_t.filter({ name: 'x' }).length === 0)
    assert('空 filterIds=[]', db.empty_t.filterIds({ name: 'x' }).length === 0)
    await db.close()
  }

  // ═══ 7. 巨大テキスト ═══
  console.log('\n── 巨大テキスト (50行 × 100KB) ──')
  await pg.query('DROP TABLE IF EXISTS big_text CASCADE')
  await pg.query('CREATE TABLE big_text (id SERIAL PRIMARY KEY, content TEXT, tag TEXT)')
  for (let i = 0; i < 50; i++)
    await pg.query('INSERT INTO big_text (content,tag) VALUES ($1,$2)', ['x'.repeat(100000) + `_${i}`, ['big', 'med'][i % 2]])
  {
    const db = await enchu(PG, { tables: ['big_text'], syncInterval: 0 })
    const eC = db.big_text.filter({ tag: 'big' }).length
    const pC = await pgCount(pg, "SELECT count(*) FROM big_text WHERE tag='big'")
    assert(`巨大テキスト E=${eC} PG=${pC}`, eC === pC)
    assert('巨大テキスト 中身長さ', ((db.big_text.filter({ tag: 'big' })[0] as Record<string, unknown>).content as string)?.length > 100000)
    await db.close()
  }

  // ═══ 8. 50カラム × 100行 ═══
  console.log('\n── 大量カラム (50個 × 100行) ──')
  await pg.query('DROP TABLE IF EXISTS many_cols CASCADE')
  const colDefs = Array.from({ length: 50 }, (_, i) => `col${i} TEXT`).join(', ')
  await pg.query(`CREATE TABLE many_cols (id SERIAL PRIMARY KEY, ${colDefs})`)
  for (let row = 0; row < 100; row++) {
    const colNames = Array.from({ length: 50 }, (_, i) => `col${i}`).join(',')
    const vals = Array.from({ length: 50 }, (_, i) => `'val${i}_${['a', 'b', 'c', 'd', 'e'][row % 5]}'`).join(',')
    await pg.query(`INSERT INTO many_cols (${colNames}) VALUES (${vals})`)
  }
  {
    const db = await enchu(PG, { tables: ['many_cols'], syncInterval: 0 })
    for (const [cond, sql] of [
      [{ col0: 'val0_a' }, "col0='val0_a'"],
      [{ col25: 'val25_b' }, "col25='val25_b'"],
      [{ col0: 'val0_a', col49: 'val49_a' }, "col0='val0_a' AND col49='val49_a'"],
    ] as [Record<string, string>, string][]) {
      const eC = db.many_cols.filter(cond).length
      const pC = await pgCount(pg, `SELECT count(*) FROM many_cols WHERE ${sql}`)
      const label = Object.entries(cond).map(([k, v]) => `${k}=${v}`).join('+')
      assert(`50col ${label} E=${eC} PG=${pC}`, eC === pC)
    }
    await db.close()
  }

  // ═══ 9. 特殊文字 100行 ═══
  console.log('\n── 特殊文字 (100行) ──')
  await pg.query('DROP TABLE IF EXISTS special CASCADE')
  await pg.query('CREATE TABLE special (id SERIAL PRIMARY KEY, val TEXT, tag TEXT)')
  const specials = ['normal', '', '🔥💀🎉', '日本語テスト', '<script>alert(1)</script>',
    'SELECT * FROM', 'null', 'line\ntwo', 'tab\there', 'quote"here']
  for (let i = 0; i < 100; i++)
    await pg.query('INSERT INTO special (val,tag) VALUES ($1,$2)', [specials[i % 10], `g${i % 5}`])
  {
    const db = await enchu(PG, { tables: ['special'], syncInterval: 0 })
    for (const val of ['🔥💀🎉', '', '日本語テスト', '<script>alert(1)</script>', 'normal']) {
      const eC = db.special.filter({ val }).length
      const pC = parseInt((await pg.query('SELECT count(*) FROM special WHERE val=$1', [val])).rows[0].count)
      assert(`特殊文字 "${val.slice(0, 10)}" E=${eC} PG=${pC}`, eC === pC)
    }
    await db.close()
  }

  // ═══ 10. id無し 200行 ═══
  console.log('\n── id無しテーブル (200行) ──')
  await pg.query('DROP TABLE IF EXISTS no_id CASCADE')
  await pg.query('CREATE TABLE no_id (name TEXT, color TEXT, size TEXT)')
  for (let i = 0; i < 200; i++)
    await pg.query('INSERT INTO no_id VALUES ($1,$2,$3)', [`item_${i}`, ['red', 'blue', 'green', 'yellow'][i % 4], ['S', 'M', 'L'][i % 3]])
  {
    const db = await enchu(PG, { tables: ['no_id'], syncInterval: 0 })
    const pgT = await pgCount(pg, 'SELECT count(*) FROM no_id')
    assert('id無し 全件', db.no_id.count() === pgT)
    for (const [cond, sql] of [
      [{ color: 'red' }, "color='red'"],
      [{ color: 'red', size: 'M' }, "color='red' AND size='M'"],
    ] as [Record<string, string>, string][]) {
      const eC = db.no_id.filter(cond).length
      const pC = await pgCount(pg, `SELECT count(*) FROM no_id WHERE ${sql}`)
      const label = Object.entries(cond).map(([k, v]) => `${k}=${v}`).join('+')
      assert(`id無し ${label} E=${eC} PG=${pC}`, eC === pC)
    }
    await db.close()
  }

  // ═══ 11. 複数テーブル同時 ═══
  console.log('\n── 複数テーブル同時 (3 × 500行) ──')
  for (const t of ['multi_a', 'multi_b', 'multi_c']) await pg.query(`DROP TABLE IF EXISTS ${t} CASCADE`)
  await pg.query('CREATE TABLE multi_a (id SERIAL PRIMARY KEY, color TEXT, price INT)')
  await pg.query('CREATE TABLE multi_b (id SERIAL PRIMARY KEY, role TEXT, dept TEXT)')
  await pg.query('CREATE TABLE multi_c (id SERIAL PRIMARY KEY, tag TEXT, score INT)')
  for (let i = 0; i < 500; i++) {
    await pg.query('INSERT INTO multi_a (color,price) VALUES ($1,$2)', [['red', 'blue', 'green'][i % 3], i % 100])
    await pg.query('INSERT INTO multi_b (role,dept) VALUES ($1,$2)', [['admin', 'user', 'mod'][i % 3], ['eng', 'sales', 'hr', 'ops'][i % 4]])
    await pg.query('INSERT INTO multi_c (tag,score) VALUES ($1,$2)', [['hot', 'cold', 'warm'][i % 3], i % 50])
  }
  {
    const db = await enchu(PG, { tables: ['multi_a', 'multi_b', 'multi_c'], syncInterval: 0 })
    for (const [table, cond, sql] of [
      ['multi_a', { color: 'red' }, "color='red'"],
      ['multi_a', { color: 'blue', price: 0 }, "color='blue' AND price=0"],
      ['multi_b', { role: 'admin' }, "role='admin'"],
      ['multi_b', { role: 'admin', dept: 'eng' }, "role='admin' AND dept='eng'"],
      ['multi_c', { tag: 'hot' }, "tag='hot'"],
    ] as [string, Record<string, string | number>, string][]) {
      const eC = (db as Record<string, { filter: (c: Record<string, string | number>) => unknown[] }>)[table].filter(cond).length
      const pC = await pgCount(pg, `SELECT count(*) FROM ${table} WHERE ${sql}`)
      const label = Object.entries(cond).map(([k, v]) => `${k}=${v}`).join('+')
      assert(`${table} ${label} E=${eC} PG=${pC}`, eC === pC)
    }
    // テーブル別count
    for (const t of ['multi_a', 'multi_b', 'multi_c']) {
      const eC = (db as Record<string, { count: () => number }>)[t].count()
      const pC = await pgCount(pg, `SELECT count(*) FROM ${t}`)
      assert(`${t} count E=${eC} PG=${pC}`, eC === pC)
    }
    await db.close()
  }

  // cleanup
  for (const t of ['type_test', 'null_heavy', 'uuid_pk', 'composite_pk', 'gap_id', 'empty_t',
    'big_text', 'many_cols', 'special', 'no_id', 'multi_a', 'multi_b', 'multi_c']) {
    await pg.query(`DROP TABLE IF EXISTS ${t} CASCADE`)
  }
  await pg.end()

  console.log('\n══════════════════════════════════════')
  console.log('結果:', passed, 'passed,', failed, 'failed')
  process.exit(failed > 0 ? 1 : 0)
}
main().catch(e => { console.error(e); process.exit(1) })
