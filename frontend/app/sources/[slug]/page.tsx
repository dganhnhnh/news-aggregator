import { Suspense } from 'react'
import { SidebarProvider } from '@/components/ui/sidebar'
import { AppSidebar } from '@/components/app-sidebar'
import { NavRail } from '@/components/nav-rail'
import { FeedView } from '@/components/feed/feed-view'
import { Skeleton } from '@/components/ui/skeleton'
import { getSources } from '@/lib/server-api'

export const dynamic = 'force-dynamic'

export default async function SourceFeedPage({
  params,
}: {
  params: Promise<{ slug: string }>
}) {
  const { slug } = await params
  const sources = await getSources()
  const source = sources.find((item) => item.slug === slug)

  return (
    <div className="flex h-svh w-full">
      <NavRail />
      <SidebarProvider className="min-h-0 flex-1">
        <AppSidebar />
        <div className="flex min-w-0 flex-1">
          <Suspense fallback={<FeedFallback />}>
            <FeedView lockedSource={{ slug, name: source?.name ?? slug }} />
          </Suspense>
        </div>
      </SidebarProvider>
    </div>
  )
}

function FeedFallback() {
  return (
    <div className="flex h-svh flex-1 flex-col gap-3 p-4">
      <Skeleton className="h-9 w-full" />
      <Skeleton className="h-9 w-full" />
      <Skeleton className="h-full w-full" />
    </div>
  )
}
