import { proxyBackend } from '@/lib/server-api'

export async function GET(
  request: Request,
  { params }: { params: Promise<{ slug: string }> },
) {
  const { slug } = await params
  const search = new URL(request.url).search
  return proxyBackend(`/api/sources/${encodeURIComponent(slug)}/articles${search}`)
}
