import { proxyBackend } from '@/lib/server-api'

export async function GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
  const { id } = await params
  return proxyBackend(`/api/articles/${encodeURIComponent(id)}`)
}
