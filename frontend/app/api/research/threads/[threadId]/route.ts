import { proxyBackend } from '@/lib/server-api'

export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ threadId: string }> },
) {
  const { threadId } = await params
  return proxyBackend(`/api/research/threads/${encodeURIComponent(threadId)}`, request)
}
