import { proxyBackend } from '@/lib/server-api'

export async function GET(request: Request) {
  return proxyBackend(`/api/sources${new URL(request.url).search}`)
}
