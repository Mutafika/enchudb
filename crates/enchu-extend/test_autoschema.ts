/**
 * ORM Auto-Schema 変換テスト
 *
 * Prismaパーサー全面テスト + Drizzleモック変換テスト + Loaderロジックテスト
 * Usage: npx tsx test_autoschema.ts
 */

import { prismaContentToLoadPlan } from './src/prisma'
import type { LoadPlan, CylinderField } from './src/types'

let passed = 0
let failed = 0
let skipped = 0

function assert(label: string, condition: boolean, detail?: string) {
  if (condition) {
    console.log(`  ✓ ${label}`)
    passed++
  } else {
    console.log(`  ✗ ${label}${detail ? ` — ${detail}` : ''}`)
    failed++
  }
}

function skip(label: string, reason: string) {
  console.log(`  ⊘ ${label} (skip: ${reason})`)
  skipped++
}

/* ================================================================
 * A. PRISMA パーサー
 * ================================================================ */

/* ----------------------------------------------------------------
 * A-1. 基本変換: 1ルート + 3子テーブル
 * ---------------------------------------------------------------- */

console.log('=== A-1. Prisma 基本変換 ===\n')

const PRISMA_BASIC = `
model Project {
  id              Int       @id @default(autoincrement())
  name            String
  clientName      String?
  contractAmount  Decimal?
  status          String?
  billing         Billing[]
  schedules       Schedule[]
  assignments     Assignment[]
}

model Billing {
  id             Int      @id @default(autoincrement())
  project        Project  @relation(fields: [projectId], references: [id])
  projectId      Int
  billingNumber  String?
  billingDate    DateTime?
  amount         Decimal?
  status         String?
}

model Schedule {
  id        Int      @id @default(autoincrement())
  project   Project  @relation(fields: [projectId], references: [id])
  projectId Int
  name      String?
  startDate DateTime?
  endDate   DateTime?
}

model Assignment {
  id             Int      @id @default(autoincrement())
  project        Project  @relation(fields: [projectId], references: [id])
  projectId      Int
  employeeId     String
  date           DateTime?
  assignmentType String?
}
`

const plan = prismaContentToLoadPlan(PRISMA_BASIC, 'Project')

// ルートテーブル
assert('ルートテーブル名', plan.rootTable === 'project')
assert('ルートPK', plan.rootPk === 'id')
assert('ルートカラム数 = 5', plan.rootColumns.length === 5,
  `got ${plan.rootColumns.length}: [${plan.rootColumns}]`)
assert('ルートカラムにname含む', plan.rootColumns.includes('name'))
assert('ルートカラムにclient_name含む', plan.rootColumns.includes('client_name'))
assert('ルートカラムにcontract_amount含む', plan.rootColumns.includes('contract_amount'))
assert('スキーマfields数一致', plan.schema.fields.length === plan.rootColumns.length)
assert('ルートschema.name一致', plan.schema.name === 'project')

// 子テーブル
assert('子テーブル3個', plan.childTables.length === 3,
  `got ${plan.childTables.length}`)

const billing = plan.childTables.find((c) => c.enchuName === 'billing')
assert('billing存在', !!billing)
assert('billing FK = project_id', billing?.fkColumn === 'project_id')
assert('billing カラムにFK無し', !billing?.columns.includes('project_id'))
assert('billing フィールド5個', billing?.fields.length === 5,
  `got ${billing?.fields.length}: [${billing?.fields.map(f => f.name)}]`)
assert('billing sqlName = billing', billing?.sqlName === 'billing')

const schedule = plan.childTables.find((c) => c.enchuName === 'schedule')
assert('schedule存在', !!schedule)
assert('schedule フィールド4個', schedule?.fields.length === 4,
  `got ${schedule?.fields.length}`)

const assignment = plan.childTables.find((c) => c.enchuName === 'assignment')
assert('assignment存在', !!assignment)
assert('assignment FK = project_id', assignment?.fkColumn === 'project_id')

// 型マッピング
assert('id型 = int',
  plan.schema.fields.find((f) => f.name === 'id')?.type === 'int')
assert('name型 = text',
  plan.schema.fields.find((f) => f.name === 'name')?.type === 'text')
assert('contract_amount型 = float',
  plan.schema.fields.find((f) => f.name === 'contract_amount')?.type === 'float')
assert('status型 = text',
  plan.schema.fields.find((f) => f.name === 'status')?.type === 'text')

// 子テーブルの型
assert('billing.amount型 = float',
  billing?.fields.find((f) => f.name === 'amount')?.type === 'float')
assert('billing.billing_date型 = text',
  billing?.fields.find((f) => f.name === 'billing_date')?.type === 'text')
assert('billing.status型 = text',
  billing?.fields.find((f) => f.name === 'status')?.type === 'text')

// JSON ラウンドトリップ
const schemaJson = JSON.stringify(plan.schema)
const parsed = JSON.parse(schemaJson)
assert('JSON parse可', parsed.name === 'project')
assert('JSON tables数一致', parsed.tables.length === 3)

/* ----------------------------------------------------------------
 * A-2. N:M リレーション → スキップ
 * ---------------------------------------------------------------- */

console.log('\n=== A-2. N:M スキップ ===\n')

const PRISMA_NM = `
model User {
  id     Int     @id
  name   String
  groups Group[]
}

model Group {
  id    Int    @id
  title String
  users User[]
}
`

const nmPlan = prismaContentToLoadPlan(PRISMA_NM, 'User')
assert('N:M 子テーブル0個', nmPlan.childTables.length === 0,
  `got ${nmPlan.childTables.length}`)
assert('ルートフィールドは残る', nmPlan.schema.fields.length === 2)

/* ----------------------------------------------------------------
 * A-3. 全Prisma型マッピング
 * ---------------------------------------------------------------- */

console.log('\n=== A-3. 全型マッピング ===\n')

const PRISMA_ALL_TYPES = `
model TypeTest {
  id        Int      @id
  strField  String
  intField  Int
  bigField  BigInt
  floatF    Float
  decField  Decimal
  boolF     Boolean
  dateF     DateTime
  jsonF     Json
  bytesF    Bytes
}
`

const typePlan = prismaContentToLoadPlan(PRISMA_ALL_TYPES, 'TypeTest')
const tf = typePlan.schema.fields

assert('String → text', tf.find((f) => f.name === 'str_field')?.type === 'text')
assert('Int → int', tf.find((f) => f.name === 'int_field')?.type === 'int')
assert('BigInt → int', tf.find((f) => f.name === 'big_field')?.type === 'int')
assert('Float → float', tf.find((f) => f.name === 'float_f')?.type === 'float')
assert('Decimal → float', tf.find((f) => f.name === 'dec_field')?.type === 'float')
assert('Boolean → bool', tf.find((f) => f.name === 'bool_f')?.type === 'bool')
assert('DateTime → text', tf.find((f) => f.name === 'date_f')?.type === 'text')
assert('Json → text', tf.find((f) => f.name === 'json_f')?.type === 'text')
assert('Bytes → text', tf.find((f) => f.name === 'bytes_f')?.type === 'text')

/* ----------------------------------------------------------------
 * A-4. snake_case 基本変換
 * ---------------------------------------------------------------- */

console.log('\n=== A-4. snake_case 基本 ===\n')

const PRISMA_CAMEL = `
model UserProfile {
  id              Int    @id
  firstName       String
  lastName        String
  emailAddress    String
  phoneNumber     String?
}
`

const camelPlan = prismaContentToLoadPlan(PRISMA_CAMEL, 'UserProfile')
assert('テーブル名 = user_profile', camelPlan.rootTable === 'user_profile')
assert('firstName → first_name', camelPlan.rootColumns.includes('first_name'))
assert('lastName → last_name', camelPlan.rootColumns.includes('last_name'))
assert('emailAddress → email_address', camelPlan.rootColumns.includes('email_address'))
assert('phoneNumber → phone_number', camelPlan.rootColumns.includes('phone_number'))

/* ----------------------------------------------------------------
 * A-5. snake_case アクロニム (バグ修正検証)
 * ---------------------------------------------------------------- */

console.log('\n=== A-5. snake_case アクロニム ===\n')

const PRISMA_ACRONYM = `
model APIConfig {
  id       Int    @id
  apiURL   String
  htmlBody String
  myXMLParser String
  simpleID Int
}
`

const acronymPlan = prismaContentToLoadPlan(PRISMA_ACRONYM, 'APIConfig')
assert('APIConfig → api_config', acronymPlan.rootTable === 'api_config')
assert('apiURL → api_url', acronymPlan.rootColumns.includes('api_url'),
  `got ${acronymPlan.rootColumns}`)
assert('htmlBody → html_body', acronymPlan.rootColumns.includes('html_body'))
assert('myXMLParser → my_xml_parser', acronymPlan.rootColumns.includes('my_xml_parser'),
  `got ${acronymPlan.rootColumns}`)
assert('simpleID → simple_id', acronymPlan.rootColumns.includes('simple_id'),
  `got ${acronymPlan.rootColumns}`)

/* ----------------------------------------------------------------
 * A-6. @@map テーブル名オーバーライド
 * ---------------------------------------------------------------- */

console.log('\n=== A-6. @@map テーブル名 ===\n')

const PRISMA_MAP = `
model ProjectBilling {
  id        Int     @id
  amount    Decimal
  project   MyProj  @relation(fields: [projId], references: [id])
  projId    Int

  @@map("project_billing_tbl")
}

model MyProj {
  id       Int              @id
  name     String
  billing  ProjectBilling[]

  @@map("projects")
}
`

const mapPlan = prismaContentToLoadPlan(PRISMA_MAP, 'MyProj')
assert('@@map ルートテーブル = projects', mapPlan.rootTable === 'projects')
assert('@@map 子テーブル = project_billing_tbl',
  mapPlan.childTables[0]?.sqlName === 'project_billing_tbl',
  `got ${mapPlan.childTables[0]?.sqlName}`)

/* ----------------------------------------------------------------
 * A-7. 1:1 リレーション
 * ---------------------------------------------------------------- */

console.log('\n=== A-7. 1:1 リレーション ===\n')

const PRISMA_ONE_TO_ONE = `
model User {
  id      Int      @id
  name    String
  profile Profile?
}

model Profile {
  id     Int    @id
  bio    String
  user   User   @relation(fields: [userId], references: [id])
  userId Int    @unique
}
`

const otoplan = prismaContentToLoadPlan(PRISMA_ONE_TO_ONE, 'User')
assert('1:1 は子テーブルとして含む', otoplan.childTables.length === 1)
assert('1:1 ルートフィールド2個', otoplan.schema.fields.length === 2)
assert('1:1 子テーブル名 = profile', otoplan.childTables[0]?.sqlName === 'profile')
assert('1:1 FK = user_id', otoplan.childTables[0]?.fkColumn === 'user_id')

/* ----------------------------------------------------------------
 * A-8. コメント・空行・///ドキュメント耐性
 * ---------------------------------------------------------------- */

console.log('\n=== A-8. コメント耐性 ===\n')

const PRISMA_COMMENTS = `
// This is a file-level comment

/// Model documentation
model Item {
  id    Int    @id  // inline comment
  // this line should be skipped
  name  String
  price Float

  /// relation doc
  tags  Tag[]
}

// Another comment between models

model Tag {
  id     Int    @id
  label  String
  item   Item   @relation(fields: [itemId], references: [id])
  itemId Int
}
`

const commentPlan = prismaContentToLoadPlan(PRISMA_COMMENTS, 'Item')
assert('コメント: ルートフィールド3個', commentPlan.schema.fields.length === 3,
  `got ${commentPlan.schema.fields.length}`)
assert('コメント: 子テーブル1個', commentPlan.childTables.length === 1)
assert('コメント: price型 = float',
  commentPlan.schema.fields.find((f) => f.name === 'price')?.type === 'float')
assert('コメント: Tag子テーブルlabel存在',
  commentPlan.childTables[0]?.fields.some(f => f.name === 'label'))

/* ----------------------------------------------------------------
 * A-9. @relation 順序逆 (バグ修正検証)
 * ---------------------------------------------------------------- */

console.log('\n=== A-9. @relation 順序逆 ===\n')

const PRISMA_REV_RELATION = `
model Order {
  id     Int    @id
  total  Float
  items  OrderItem[]
}

model OrderItem {
  id       Int    @id
  order    Order  @relation(references: [id], fields: [orderId])
  orderId  Int
  name     String
  qty      Int
}
`

const revPlan = prismaContentToLoadPlan(PRISMA_REV_RELATION, 'Order')
assert('逆順@relation: 子テーブル1個', revPlan.childTables.length === 1)
assert('逆順@relation: FK = order_id',
  revPlan.childTables[0]?.fkColumn === 'order_id',
  `got ${revPlan.childTables[0]?.fkColumn}`)
assert('逆順@relation: 子フィールド3個 (id,name,qty)',
  revPlan.childTables[0]?.fields.length === 3,
  `got ${revPlan.childTables[0]?.fields.length}: [${revPlan.childTables[0]?.fields.map(f => f.name)}]`)

/* ----------------------------------------------------------------
 * A-10. Enum型フィールド (バグ修正検証)
 * ---------------------------------------------------------------- */

console.log('\n=== A-10. Enum型フィールド ===\n')

const PRISMA_ENUM = `
enum Role {
  ADMIN
  USER
  MODERATOR
}

enum Status {
  ACTIVE
  INACTIVE
}

model Employee {
  id     Int    @id
  name   String
  role   Role
  status Status
  tasks  Task[]
}

model Task {
  id          Int      @id
  employee    Employee @relation(fields: [employeeId], references: [id])
  employeeId  Int
  title       String
}
`

const enumPlan = prismaContentToLoadPlan(PRISMA_ENUM, 'Employee')
assert('enum: ルートフィールド4個 (id,name,role,status)',
  enumPlan.schema.fields.length === 4,
  `got ${enumPlan.schema.fields.length}: [${enumPlan.schema.fields.map(f => f.name)}]`)
assert('enum: role存在', enumPlan.schema.fields.some(f => f.name === 'role'))
assert('enum: role → text', enumPlan.schema.fields.find(f => f.name === 'role')?.type === 'text')
assert('enum: status存在', enumPlan.schema.fields.some(f => f.name === 'status'))
assert('enum: status → text', enumPlan.schema.fields.find(f => f.name === 'status')?.type === 'text')
assert('enum: 子テーブル1個', enumPlan.childTables.length === 1)

/* ----------------------------------------------------------------
 * A-11. 自己参照リレーション
 * ---------------------------------------------------------------- */

console.log('\n=== A-11. 自己参照リレーション ===\n')

const PRISMA_SELF_REF = `
model Category {
  id         Int        @id
  name       String
  parentId   Int?
  parent     Category?  @relation("ParentChild", fields: [parentId], references: [id])
  children   Category[] @relation("ParentChild")
}
`

const selfPlan = prismaContentToLoadPlan(PRISMA_SELF_REF, 'Category')
// 自己参照: Category[] は自分自身へのリスト → 子テーブルとして検出されるべき
assert('自己参照: 子テーブル1個', selfPlan.childTables.length === 1,
  `got ${selfPlan.childTables.length}`)
assert('自己参照: FK = parent_id',
  selfPlan.childTables[0]?.fkColumn === 'parent_id',
  `got ${selfPlan.childTables[0]?.fkColumn}`)

/* ----------------------------------------------------------------
 * A-12. 複数リレーション（名前付き）
 * ---------------------------------------------------------------- */

console.log('\n=== A-12. 複数名前付きリレーション ===\n')

const PRISMA_MULTI_REL = `
model User {
  id           Int       @id
  name         String
  writtenPosts Post[]    @relation("WrittenPosts")
  likedPosts   Post[]    @relation("LikedPosts")
}

model Post {
  id        Int    @id
  title     String
  author    User   @relation("WrittenPosts", fields: [authorId], references: [id])
  authorId  Int
  likedBy   User?  @relation("LikedPosts", fields: [likedById], references: [id])
  likedById Int?
}
`

const multiRelPlan = prismaContentToLoadPlan(PRISMA_MULTI_REL, 'User')
// Post は2つのリレーションでUser参照。isList=trueの2つ分、子テーブルとして検出
// ただし同じモデルなので enchuName は同じ — 2つ出るか1つかは実装次第
assert('複数リレーション: 子テーブル >= 1',
  multiRelPlan.childTables.length >= 1,
  `got ${multiRelPlan.childTables.length}`)
assert('複数リレーション: Post子テーブル存在',
  multiRelPlan.childTables.some(c => c.sqlName === 'post'))

/* ----------------------------------------------------------------
 * A-13. Optional フィールドのみのモデル
 * ---------------------------------------------------------------- */

console.log('\n=== A-13. Optional フィールドのみ ===\n')

const PRISMA_OPT = `
model Metadata {
  id    Int     @id
  key   String?
  value String?
}
`

const optPlan = prismaContentToLoadPlan(PRISMA_OPT, 'Metadata')
assert('optional: フィールド3個', optPlan.schema.fields.length === 3)
assert('optional: key存在', optPlan.schema.fields.some(f => f.name === 'key'))
assert('optional: value存在', optPlan.schema.fields.some(f => f.name === 'value'))

/* ----------------------------------------------------------------
 * A-14. UUID型PK
 * ---------------------------------------------------------------- */

console.log('\n=== A-14. UUID型PK ===\n')

const PRISMA_UUID = `
model Tenant {
  id    String  @id @default(uuid())
  name  String
  items TenantItem[]
}

model TenantItem {
  id       String @id @default(uuid())
  tenant   Tenant @relation(fields: [tenantId], references: [id])
  tenantId String
  label    String
}
`

const uuidPlan = prismaContentToLoadPlan(PRISMA_UUID, 'Tenant')
assert('UUID: PK名 = id', uuidPlan.rootPk === 'id')
assert('UUID: id型 = text', uuidPlan.schema.fields.find(f => f.name === 'id')?.type === 'text')
assert('UUID: 子テーブル1個', uuidPlan.childTables.length === 1)
assert('UUID: FK = tenant_id', uuidPlan.childTables[0]?.fkColumn === 'tenant_id')

/* ----------------------------------------------------------------
 * A-15. 複合@unique（@idなし→先頭フィールドフォールバック）
 * ---------------------------------------------------------------- */

console.log('\n=== A-15. @idなしフォールバック ===\n')

const PRISMA_NO_ID = `
model Setting {
  key   String  @unique
  value String
}
`

const noIdPlan = prismaContentToLoadPlan(PRISMA_NO_ID, 'Setting')
assert('@idなし: PK = key (先頭フィールド名フォールバック)',
  noIdPlan.rootPk === 'key',
  `got ${noIdPlan.rootPk}`)

/* ----------------------------------------------------------------
 * A-16. 深いネスト（孫テーブル）
 * ---------------------------------------------------------------- */

console.log('\n=== A-16. 深いネスト ===\n')

const PRISMA_DEEP = `
model Company {
  id       Int       @id
  name     String
  projects Project[]
}

model Project {
  id        Int      @id
  company   Company  @relation(fields: [companyId], references: [id])
  companyId Int
  title     String
  tasks     Task[]
}

model Task {
  id        Int     @id
  project   Project @relation(fields: [projectId], references: [id])
  projectId Int
  name      String
}
`

const deepPlan = prismaContentToLoadPlan(PRISMA_DEEP, 'Company')
// Company → Project (子) + Task (孫、フラット化して project__task)
assert('深いネスト: 子テーブル2個 (Project + Task)',
  deepPlan.childTables.length === 2,
  `got ${deepPlan.childTables.length}`)
assert('深いネスト: project子テーブル',
  deepPlan.childTables[0]?.sqlName === 'project')
assert('深いネスト: 孫テーブル名 = project__task',
  deepPlan.childTables[1]?.enchuName === 'project__task',
  `got ${deepPlan.childTables[1]?.enchuName}`)
assert('深いネスト: 孫テーブルSQL名 = task',
  deepPlan.childTables[1]?.sqlName === 'task')

// Project をルートにすると Task が子テーブル
const deepPlan2 = prismaContentToLoadPlan(PRISMA_DEEP, 'Project')
assert('深いネスト(Project root): 子テーブル1個 (Taskのみ)',
  deepPlan2.childTables.length === 1)
assert('深いネスト(Project root): task子テーブル',
  deepPlan2.childTables[0]?.sqlName === 'task')

/* ----------------------------------------------------------------
 * A-17. エラーケース
 * ---------------------------------------------------------------- */

console.log('\n=== A-17. エラーケース ===\n')

// 存在しないモデル
let errCaught = false
try {
  prismaContentToLoadPlan(PRISMA_BASIC, 'NonExistent')
} catch (e: any) {
  errCaught = true
  assert('存在しないモデル: メッセージにモデル名含む', e.message.includes('NonExistent'))
  assert('存在しないモデル: 候補表示', e.message.includes('Project'))
}
assert('存在しないモデル: throwされた', errCaught)

// 空スキーマ
let emptyErr = false
try {
  prismaContentToLoadPlan('', 'Project')
} catch {
  emptyErr = true
}
assert('空スキーマ → エラー', emptyErr)

// generator/datasource ブロックは無視される
const PRISMA_WITH_GENERATOR = `
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

model Simple {
  id   Int    @id
  name String
}
`
const genPlan = prismaContentToLoadPlan(PRISMA_WITH_GENERATOR, 'Simple')
assert('generator/datasource無視: ルート検出', genPlan.rootTable === 'simple')
assert('generator/datasource無視: フィールド2個', genPlan.schema.fields.length === 2)

/* ================================================================
 * B. DRIZZLE モック変換テスト
 * ================================================================ */

console.log('\n=== B-1. Drizzle モックテスト ===\n')

// drizzle-orm をインストールせずに、getTableConfig の返り値を
// モックしてロジックを検証する

// drizzle.ts の mapColumnType と findPk は内部関数なので直接テスト不可
// 代わりに、同等ロジックをここに複製してテスト

function mockMapColumnType(col: { dataType: string; columnType: string }): string {
  const ct = col.columnType.toLowerCase()
  if (/serial|integer|bigint|smallint/.test(ct)) return 'int'
  if (/real|double|numeric|decimal/.test(ct)) return 'float'
  if (/boolean/.test(ct)) return 'bool'
  return 'text'
}

const drizzleTypeTests: [string, string, string][] = [
  // [columnType, dataType, expected]
  ['PgSerial', 'number', 'int'],
  ['PgInteger', 'number', 'int'],
  ['PgBigInt53', 'number', 'int'],
  ['PgBigInt64', 'bigint', 'int'],
  ['PgSmallInt', 'number', 'int'],
  ['PgBigSerial', 'number', 'int'],
  ['PgReal', 'number', 'float'],
  ['PgDoublePrecision', 'number', 'float'],
  ['PgNumeric', 'string', 'float'],
  ['PgBoolean', 'boolean', 'bool'],
  ['PgText', 'string', 'text'],
  ['PgVarchar', 'string', 'text'],
  ['PgTimestamp', 'date', 'text'],
  ['PgDate', 'date', 'text'],
  ['PgUUID', 'string', 'text'],
  ['PgJson', 'json', 'text'],
  ['PgJsonb', 'json', 'text'],
  ['PgEnumColumn', 'string', 'text'],
]

for (const [columnType, dataType, expected] of drizzleTypeTests) {
  const result = mockMapColumnType({ columnType, dataType })
  assert(`Drizzle ${columnType} → ${expected}`, result === expected,
    `got ${result}`)
}

/* ----------------------------------------------------------------
 * B-2. Drizzle PK検出ロジック
 * ---------------------------------------------------------------- */

console.log('\n=== B-2. Drizzle PK検出ロジック ===\n')

function mockFindPk(config: { columns: any[], primaryKeys?: any[] }): string {
  if (config.primaryKeys && config.primaryKeys.length > 0) {
    return config.primaryKeys[0].columns[0].name
  }
  const pkCol = config.columns.find((c: any) => c.primary === true)
  if (pkCol) return pkCol.name
  const serial = config.columns.find((c: any) => /serial/i.test(c.columnType))
  if (serial) return serial.name
  return config.columns[0].name
}

// .primary = true
assert('PK: primary=true',
  mockFindPk({
    columns: [
      { name: 'id', primary: true, columnType: 'PgSerial' },
      { name: 'name', primary: false, columnType: 'PgText' },
    ],
  }) === 'id')

// primaryKey がメソッド参照(truthy)でも誤検出しない
assert('PK: primaryKey=function は無視',
  mockFindPk({
    columns: [
      { name: 'id', primary: false, primaryKey: () => {}, columnType: 'PgSerial' },
      { name: 'name', primary: false, columnType: 'PgText' },
    ],
  }) === 'id')  // serialフォールバックで id

// テーブルレベルPK
assert('PK: テーブルレベルPK',
  mockFindPk({
    columns: [
      { name: 'tenant_id', primary: false, columnType: 'PgText' },
      { name: 'record_id', primary: false, columnType: 'PgText' },
    ],
    primaryKeys: [{ columns: [{ name: 'tenant_id' }] }],
  }) === 'tenant_id')

// serialフォールバック
assert('PK: serialフォールバック',
  mockFindPk({
    columns: [
      { name: 'data', columnType: 'PgText' },
      { name: 'auto_id', columnType: 'PgSerial' },
    ],
  }) === 'auto_id')

// 最終フォールバック: 先頭カラム
assert('PK: 先頭カラムフォールバック',
  mockFindPk({
    columns: [
      { name: 'code', columnType: 'PgText' },
      { name: 'label', columnType: 'PgText' },
    ],
  }) === 'code')

/* ================================================================
 * C. LOADER ロジックテスト
 * ================================================================ */

console.log('\n=== C-1. PG接続文字列パース ===\n')

// loader.ts の parsePgConn と同等ロジック
function parsePgConn(connStr: string): string | Record<string, unknown> {
  if (connStr.startsWith('postgresql://') || connStr.startsWith('postgres://')) {
    return connStr
  }
  const config: Record<string, unknown> = {}
  const pairs = connStr.match(/(\w+)=(\S+)/g) ?? []
  for (const pair of pairs) {
    const eq = pair.indexOf('=')
    const key = pair.slice(0, eq)
    const val = pair.slice(eq + 1)
    switch (key) {
      case 'host': config.host = val; break
      case 'port': config.port = parseInt(val); break
      case 'user': config.user = val; break
      case 'password': config.password = val; break
      case 'dbname': config.database = val; break
      case 'sslmode': config.ssl = val !== 'disable'; break
    }
  }
  return config
}

// URL形式
const urlConn = parsePgConn('postgresql://user:pass@localhost:5432/mydb')
assert('URL形式: そのまま返す', urlConn === 'postgresql://user:pass@localhost:5432/mydb')

const urlConn2 = parsePgConn('postgres://admin@db.host/prod')
assert('postgres:// 形式もOK', urlConn2 === 'postgres://admin@db.host/prod')

// libpq形式
const libpqConn = parsePgConn('host=localhost port=5432 user=postgres password=secret dbname=testdb') as Record<string, unknown>
assert('libpq host', libpqConn.host === 'localhost')
assert('libpq port', libpqConn.port === 5432)
assert('libpq user', libpqConn.user === 'postgres')
assert('libpq password', libpqConn.password === 'secret')
assert('libpq database', libpqConn.database === 'testdb')

// sslmode
const sslConn = parsePgConn('host=db.prod port=5432 sslmode=require') as Record<string, unknown>
assert('sslmode=require → ssl=true', sslConn.ssl === true)

const sslDisable = parsePgConn('host=localhost sslmode=disable') as Record<string, unknown>
assert('sslmode=disable → ssl=false', sslDisable.ssl === false)

/* ----------------------------------------------------------------
 * C-2. Loader put() JSON構造テスト
 * ---------------------------------------------------------------- */

console.log('\n=== C-2. put() JSON構造 ===\n')

// loadFromPg が生成する put JSON の構造を検証
// (実際のPG接続なし — 構造だけ検証)

function mockBuildPutJson(
  plan: LoadPlan,
  rootRow: Record<string, any>,
  childData: Record<string, Record<string, any>[]>,
): string {
  const pkStr = String(rootRow[plan.rootPk])
  const axes: Record<string, string> = {}
  for (const col of plan.rootColumns) {
    axes[col] = String(rootRow[col] ?? '')
  }
  const tables: Record<string, any[]> = {}
  for (const child of plan.childTables) {
    tables[child.enchuName] = childData[child.enchuName] ?? []
  }
  return JSON.stringify({ key: pkStr, axes, tables })
}

const putJson = mockBuildPutJson(
  plan,
  { id: 42, name: 'ProjectX', client_name: 'Acme', contract_amount: 1000000, status: 'active' },
  {
    billing: [
      { id: 1, billing_number: 'B-001', billing_date: '2024-01-15', amount: 500000, status: 'paid' },
      { id: 2, billing_number: 'B-002', billing_date: '2024-02-15', amount: 500000, status: 'pending' },
    ],
    schedule: [{ id: 1, name: 'Phase1', start_date: '2024-01-01', end_date: '2024-06-30' }],
    assignment: [],
  },
)

const putParsed = JSON.parse(putJson)
assert('put: key = "42"', putParsed.key === '42')
assert('put: axes.id = "42"', putParsed.axes.id === '42')
assert('put: axes.name = "ProjectX"', putParsed.axes.name === 'ProjectX')
assert('put: axes.contract_amount = "1000000"', putParsed.axes.contract_amount === '1000000')
assert('put: billing 2件', putParsed.tables.billing.length === 2)
assert('put: schedule 1件', putParsed.tables.schedule.length === 1)
assert('put: assignment 0件', putParsed.tables.assignment.length === 0)
assert('put: billing[0].amount = 500000', putParsed.tables.billing[0].amount === 500000)

// axes の null 変換
const putNullJson = mockBuildPutJson(
  plan,
  { id: 99, name: 'Test', client_name: null, contract_amount: undefined, status: '' },
  { billing: [], schedule: [], assignment: [] },
)
const putNull = JSON.parse(putNullJson)
assert('put: null → ""', putNull.axes.client_name === '')
assert('put: undefined → ""', putNull.axes.contract_amount === '')
assert('put: 空文字そのまま', putNull.axes.status === '')

/* ================================================================
 * D. init() JSON 互換性 (Rust側parse対応)
 * ================================================================ */

console.log('\n=== D. init() JSON互換性 ===\n')

const initJson = JSON.parse(JSON.stringify(plan.schema))
assert('name はstring', typeof initJson.name === 'string')
assert('fields は配列', Array.isArray(initJson.fields))
assert('tables は配列', Array.isArray(initJson.tables))
assert('field.name はstring', typeof initJson.fields[0]?.name === 'string')
assert('field.type はstring', typeof initJson.fields[0]?.type === 'string')
assert('table.name はstring', typeof initJson.tables[0]?.name === 'string')
assert('table.fields は配列', Array.isArray(initJson.tables[0]?.fields))

// type値がRust parse_field_type で認識されるか
const validTypes = new Set(['text', 'int', 'float', 'bool'])
const allFieldTypes = [
  ...initJson.fields.map((f: any) => f.type),
  ...initJson.tables.flatMap((t: any) => t.fields.map((f: any) => f.type)),
]
assert('全type値がRust互換',
  allFieldTypes.every((t: string) => validTypes.has(t)),
  `types: ${[...new Set(allFieldTypes)]}`)

// 複数プランでtype値チェック
for (const [label, p] of [
  ['enum', enumPlan],
  ['uuid', uuidPlan],
  ['acronym', acronymPlan],
  ['allTypes', typePlan],
] as [string, LoadPlan][]) {
  const types = [
    ...p.schema.fields.map(f => f.type),
    ...p.schema.tables.flatMap(t => t.fields.map(f => f.type)),
  ]
  assert(`${label}プラン: 全type値が有効`,
    types.every(t => validTypes.has(t)),
    `invalid: ${types.filter(t => !validTypes.has(t))}`)
}

/* ================================================================
 * E. ストレステスト
 * ================================================================ */

console.log('\n=== E. ストレステスト ===\n')

// 100モデル・各20フィールドのスキーマ
let bigSchema = ''
const childModels: string[] = []
for (let i = 0; i < 100; i++) {
  const modelName = `Model${i}`
  let fields = `  id Int @id\n`
  for (let j = 0; j < 20; j++) {
    const types = ['String', 'Int', 'Float', 'Boolean', 'DateTime']
    fields += `  field${j} ${types[j % types.length]}\n`
  }
  if (i > 0) {
    fields += `  root Root @relation(fields: [rootId], references: [id])\n`
    fields += `  rootId Int\n`
    childModels.push(modelName)
  } else {
    // Model0 = Root, リストフィールド追加
    for (let k = 1; k < 100; k++) {
      fields += `  model${k} Model${k}[]\n`
    }
  }
  bigSchema += `model ${modelName} {\n${fields}}\n\n`
}

// @@map でルート名変更
bigSchema = bigSchema.replace('model Model0 {', 'model Root {\n  @@map("root_table")')
// Model0 をルートとして参照できるように Root に変更
bigSchema = bigSchema.replaceAll('Model0', 'Root')

const t0 = performance.now()
const bigPlan = prismaContentToLoadPlan(bigSchema, 'Root')
const elapsed = performance.now() - t0

assert(`ストレス: 100モデルパース < 100ms`, elapsed < 100,
  `${elapsed.toFixed(1)}ms`)
assert('ストレス: ルートテーブル', bigPlan.rootTable === 'root_table')
assert('ストレス: 子テーブル99個', bigPlan.childTables.length === 99,
  `got ${bigPlan.childTables.length}`)
assert('ストレス: ルートフィールド21個', bigPlan.schema.fields.length === 21,
  `got ${bigPlan.schema.fields.length}`)
assert('ストレス: 子テーブルフィールド各21個',
  bigPlan.childTables.every(c => c.fields.length === 21),
  `some have != 21`)

// JSON生成
const bigJson = JSON.stringify(bigPlan.schema)
assert('ストレス: JSON生成可', bigJson.length > 1000)
const bigParsed = JSON.parse(bigJson)
assert('ストレス: JSONパース可', bigParsed.tables.length === 99)

/* ================================================================
 * F. 複合テスト — 実務スキーマ (全パターン同時)
 * ================================================================ */

console.log('\n=== F. 複合テスト — SaaS請求管理スキーマ ===\n')

const PRISMA_COMPLEX = `
enum Role {
  ADMIN
  MEMBER
  VIEWER
}

enum InvoiceStatus {
  DRAFT
  SENT
  PAID
  OVERDUE
}

model Company {
  id            Int       @id @default(autoincrement())
  name          String
  taxId         String?
  profile       CompanyProfile?
  departments   Department[]
  projects      Project[]
  tags          Tag[]
}

model CompanyProfile {
  id          Int     @id
  address     String
  phone       String
  website     String?
  company     Company @relation(fields: [companyId], references: [id])
  companyId   Int     @unique
}

model Department {
  id         Int        @id
  name       String
  budget     Float
  company    Company    @relation(fields: [companyId], references: [id])
  companyId  Int
  parentId   Int?
  parent     Department? @relation("DeptTree", fields: [parentId], references: [id])
  children   Department[] @relation("DeptTree")
  employees  Employee[]
}

model Employee {
  id           Int        @id
  firstName    String
  lastName     String
  email        String     @unique
  salary       Decimal
  isActive     Boolean    @default(true)
  role         Role       @default(MEMBER)
  department   Department @relation(fields: [departmentId], references: [id])
  departmentId Int
  assignments  ProjectAssignment[]
}

model Project {
  id          Int       @id
  code        String    @unique
  title       String
  startDate   DateTime
  endDate     DateTime?
  company     Company   @relation(fields: [companyId], references: [id])
  companyId   Int
  invoices    Invoice[]
  milestones  Milestone[]
  assignments ProjectAssignment[]
}

model Invoice {
  id          Int           @id
  number      String        @unique
  amount      Float
  tax         Float
  status      InvoiceStatus @default(DRAFT)
  issuedAt    DateTime
  paidAt      DateTime?
  project     Project       @relation(fields: [projectId], references: [id])
  projectId   Int
  lineItems   LineItem[]
}

model LineItem {
  id          Int     @id
  description String
  quantity    Int
  unitPrice   Float
  invoice     Invoice @relation(fields: [invoiceId], references: [id])
  invoiceId   Int
}

model Milestone {
  id          Int      @id
  name        String
  dueDate     DateTime
  completed   Boolean  @default(false)
  project     Project  @relation(fields: [projectId], references: [id])
  projectId   Int
}

model ProjectAssignment {
  id         Int      @id
  role       String
  startDate  DateTime
  employee   Employee @relation(fields: [employeeId], references: [id])
  employeeId Int
  project    Project  @relation(fields: [projectId], references: [id])
  projectId  Int
}

model Tag {
  id   Int    @id
  name String @unique
  companies Company[]
}
`

const cxPlan = prismaContentToLoadPlan(PRISMA_COMPLEX, 'Company')

// --- ルート ---
assert('CX: ルートテーブル名', cxPlan.rootTable === 'company')
assert('CX: ルートPK = id', cxPlan.rootPk === 'id')
assert('CX: ルートフィールド3個 (id, name, tax_id)',
  cxPlan.schema.fields.length === 3,
  `got ${cxPlan.schema.fields.length}: ${cxPlan.schema.fields.map(f=>f.name)}`)
assert('CX: tax_id は text',
  cxPlan.schema.fields.find(f => f.name === 'tax_id')?.type === 'text')

// --- 1:1 (CompanyProfile) ---
const profileChild = cxPlan.childTables.find(c => c.sqlName === 'company_profile')
assert('CX: 1:1 CompanyProfile 検出', !!profileChild)
assert('CX: 1:1 FK = company_id', profileChild?.fkColumn === 'company_id')
assert('CX: 1:1 フィールドに address',
  !!profileChild?.fields.find(f => f.name === 'address'))
assert('CX: 1:1 フィールドに website',
  !!profileChild?.fields.find(f => f.name === 'website'))

// --- 1:N (Department) ---
const deptChild = cxPlan.childTables.find(c => c.sqlName === 'department')
assert('CX: 1:N Department 検出', !!deptChild)
assert('CX: Department FK = company_id', deptChild?.fkColumn === 'company_id')
assert('CX: Department budget は float',
  deptChild?.fields.find(f => f.name === 'budget')?.type === 'float')

// --- 自己参照 (Department.children) ---
// department (Company→Department) + department__department (Department→Department自己参照)
const deptDirect = cxPlan.childTables.find(c => c.enchuName === 'department')
const deptSelf = cxPlan.childTables.find(c => c.enchuName === 'department__department')
assert('CX: Department 直接子テーブル検出', !!deptDirect)
assert('CX: Department 自己参照テーブル検出', !!deptSelf)
assert('CX: 自己参照 FK = parent_id', deptSelf?.fkColumn === 'parent_id')

// --- 深い階層 (Company → Department → Employee) ---
const empChild = cxPlan.childTables.find(c => c.enchuName === 'department__employee')
assert('CX: 孫テーブル Employee 検出 (department__employee)', !!empChild,
  `children: ${cxPlan.childTables.map(c=>c.enchuName)}`)
assert('CX: Employee FK = department_id', empChild?.fkColumn === 'department_id')
assert('CX: Employee salary は float',
  empChild?.fields.find(f => f.name === 'salary')?.type === 'float')
assert('CX: Employee is_active は bool',
  empChild?.fields.find(f => f.name === 'is_active')?.type === 'bool')
assert('CX: Employee role は text (enum)',
  empChild?.fields.find(f => f.name === 'role')?.type === 'text')

// --- 深い階層 (Company → Project → Invoice → LineItem) ---
const projChild = cxPlan.childTables.find(c => c.enchuName === 'project')
assert('CX: 1:N Project 検出', !!projChild)
assert('CX: Project FK = company_id', projChild?.fkColumn === 'company_id')

const invChild = cxPlan.childTables.find(c => c.enchuName === 'project__invoice')
assert('CX: 孫テーブル Invoice 検出 (project__invoice)', !!invChild,
  `children: ${cxPlan.childTables.map(c=>c.enchuName)}`)
assert('CX: Invoice amount は float',
  invChild?.fields.find(f => f.name === 'amount')?.type === 'float')
assert('CX: Invoice status は text (enum)',
  invChild?.fields.find(f => f.name === 'status')?.type === 'text')

const lineChild = cxPlan.childTables.find(c => c.enchuName === 'project__invoice__line_item')
assert('CX: ひ孫テーブル LineItem (project__invoice__line_item)', !!lineChild,
  `children: ${cxPlan.childTables.map(c=>c.enchuName)}`)
assert('CX: LineItem quantity は int',
  lineChild?.fields.find(f => f.name === 'quantity')?.type === 'int')
assert('CX: LineItem unit_price は float',
  lineChild?.fields.find(f => f.name === 'unit_price')?.type === 'float')

// --- Milestone (Project の子) ---
const msChild = cxPlan.childTables.find(c => c.enchuName === 'project__milestone')
assert('CX: 孫テーブル Milestone (project__milestone)', !!msChild)
assert('CX: Milestone completed は bool',
  msChild?.fields.find(f => f.name === 'completed')?.type === 'bool')

// --- N:M (Company ↔ Tag via implicit) ---
// Prismaの暗黙的N:Mは中間テーブルがスキーマに明示されないため検出不可
// → skipped or junction未検出のwarning
const tagChild = cxPlan.childTables.find(c => c.sqlName === 'tag')
console.log(`  (N:M Tag: ${tagChild ? 'detected' : 'skipped — implicit junction, expected'})`)

// --- 全体の整合性 ---
const allEnchuNames = cxPlan.childTables.map(c => c.enchuName)
assert('CX: enchuName に重複なし',
  new Set(allEnchuNames).size === allEnchuNames.length,
  `duplicates in: ${allEnchuNames}`)

// JSON出力テスト
const cxJson = JSON.stringify(cxPlan.schema)
const cxParsed = JSON.parse(cxJson)
assert('CX: JSON生成・パース可', !!cxParsed)
assert('CX: JSON tables と childTables 数一致',
  cxParsed.tables.length === cxPlan.childTables.length,
  `schema.tables=${cxParsed.tables.length} vs childTables=${cxPlan.childTables.length}`)

// 全フィールドの型が有効か
const cxAllTypes = [
  ...cxPlan.schema.fields.map(f => f.type),
  ...cxPlan.childTables.flatMap(c => c.fields.map(f => f.type)),
]
const cxValidTypes = new Set(['text', 'int', 'float', 'bool'])
assert('CX: 全フィールド型がRust互換',
  cxAllTypes.every(t => cxValidTypes.has(t)),
  `invalid: ${cxAllTypes.filter(t => !cxValidTypes.has(t))}`)

console.log(`\n  子テーブル一覧:`)
for (const c of cxPlan.childTables) {
  console.log(`    ${c.enchuName} (sql: ${c.sqlName}, fk: ${c.fkColumn}, fields: ${c.fields.length})`)
}

/* ================================================================
 * サマリー
 * ================================================================ */

console.log(`\n${'='.repeat(60)}`)
console.log(`結果: ${passed} passed, ${failed} failed, ${skipped} skipped`)
console.log('='.repeat(60))
if (failed > 0) process.exit(1)
