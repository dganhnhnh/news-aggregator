import { proxyBackend } from '@/lib/server-api'

export async function GET() {
  return proxyBackend('/api/sources')
}
