import { proxyAdminBackend } from '@/lib/server-api'

export async function POST(request: Request) {
  return proxyAdminBackend('/api/admin/feeds/discover', request)
}
