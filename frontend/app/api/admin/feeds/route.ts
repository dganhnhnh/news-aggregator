import { proxyAdminBackend } from '@/lib/server-api'

function path(request: Request): string {
  return `/api/admin/feeds${new URL(request.url).search}`
}

export async function GET(request: Request) {
  return proxyAdminBackend(path(request), request)
}

export async function POST(request: Request) {
  return proxyAdminBackend(path(request), request)
}

export async function PATCH(request: Request) {
  return proxyAdminBackend(path(request), request)
}
