// ===================================================
// t5ug3-system での使い方
// ===================================================

// --- Step 1: enchu-extend をインストール ---
// cp /Users/kubo/Desktop/mutafika/enchu-extend/enchu-extend.node ./
// cp /Users/kubo/Desktop/mutafika/enchu-extend/index.js ./lib/enchu/
// cp /Users/kubo/Desktop/mutafika/enchu-extend/index.d.ts ./lib/enchu/

// --- Step 2: 初期化（app起動時に1回） ---
// lib/enchu/client.ts

import * as enchu from './index'

const CACHE_DIR = process.env.ENCHU_CACHE_DIR || '/tmp/enchu_t5ug3_cache'
const PG_CONN = process.env.DATABASE_URL || ''

let initialized = false

export function ensureEnchu() {
  if (initialized) return
  try {
    enchu.open(CACHE_DIR, PG_CONN)
  } catch {
    enchu.init(CACHE_DIR, PG_CONN)
  }
  initialized = true
}

// --- Step 3: DALに組み込む ---
// lib/dal/projects.ts

import { prisma } from '@/lib/prisma'
import * as enchu from '@/lib/enchu/client'

export async function getProjectWithDetails(projectId: string) {
  enchu.ensureEnchu()

  // 1. Enchu から取得（89ns）
  const cached = enchu.getProject(projectId)
  if (cached) {
    return JSON.parse(cached)
  }

  // 2. キャッシュミス → Prisma → PG（7クエリ、2ms）
  const project = await prisma.projects.findUnique({
    where: { id: projectId },
    include: {
      project_billing: true,
      project_schedules: true,
      project_material_costs: true,
      employee_assignments: true,
      project_files: true,
      project_subcontract_costs: true,
    }
  })

  if (!project) return null

  // 3. Enchu にキャッシュ
  enchu.putProject(JSON.stringify({
    project_id: project.id,
    company_id: project.company_id,
    tables: {
      project: [[
        project.name,
        project.client_name || '',
        project.address || '',
        project.status || '',
        String(project.contract_amount || 0),
      ]],
      billing: project.project_billing.map(b => [
        b.billing_number || '',
        b.billing_date?.toISOString() || '',
        String(b.billing_amount || 0),
        String(b.total_amount || 0),
        b.status || '',
      ]),
      schedules: project.project_schedules.map(s => [
        s.id,
        s.name || '',
      ]),
      material_costs: project.project_material_costs.map(m => [
        m.supplier_name || '',
        m.purchase_date?.toISOString() || '',
        String(m.amount || 0),
      ]),
      assignments: project.employee_assignments.map(a => [
        a.employee_id || '',
        a.date?.toISOString() || '',
        a.assignment_type || '',
      ]),
      files: project.project_files.map(f => [
        f.file_name || '',
        f.display_name || '',
        f.file_type || '',
        String(f.file_size || 0),
      ]),
    }
  }))
  enchu.sync()

  return project
}

// --- Step 4: API Route ---
// app/api/projects/[id]/route.ts

// import { getProjectWithDetails } from '@/lib/dal/projects'
//
// export async function GET(
//   req: Request,
//   { params }: { params: { id: string } }
// ) {
//   const project = await getProjectWithDetails(params.id)
//   if (!project) return Response.json({ error: 'not found' }, { status: 404 })
//   return Response.json(project)
// }

// --- Step 5: Prisma Middleware でキャッシュ無効化 ---
// lib/prisma.ts

// import * as enchu from '@/lib/enchu/client'
//
// prisma.$use(async (params, next) => {
//   const result = await next(params)
//
//   // 書き込み時にキャッシュ無効化
//   if (['create', 'update', 'delete', 'upsert'].includes(params.action)) {
//     const projectId = result?.project_id || params.args?.where?.project_id
//     if (projectId) {
//       try {
//         // 再取得してキャッシュ更新（invalidate + re-populate）
//         // TODO: invalidateProject実装後はinvalidateだけでOK
//         enchu.ensureEnchu()
//       } catch {}
//     }
//   }
//
//   return result
// })
