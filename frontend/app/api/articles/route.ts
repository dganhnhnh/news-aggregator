import { proxyBackend } from '@/lib/server-api'

export async function GET(request: Request) {
  return proxyBackend(`/api/articles${new URL(request.url).search}`)
}
