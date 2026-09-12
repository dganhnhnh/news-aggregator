import { proxyBackend } from '@/lib/server-api'

export async function POST(request: Request) {
  return proxyBackend('/api/research/stream', request)
}
