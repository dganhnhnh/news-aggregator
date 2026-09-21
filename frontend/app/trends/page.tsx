import { Suspense } from 'react'
import { NavRail } from '@/components/nav-rail'
import { TrendsView } from '@/components/trends/trends-view'
import { Skeleton } from '@/components/ui/skeleton'

export default function Page() {
  return (
    <div className="flex h-svh w-full">
      <NavRail />
      <div className="flex min-w-0 flex-1">
        {/* Required, not decorative: TrendsView calls useSearchParams(). */}
        <Suspense fallback={<TrendsFallback />}>
          <TrendsView />
        </Suspense>
      </div>
    </div>
  )
}

function TrendsFallback() {
  return (
    <div className="flex h-svh flex-1 flex-col gap-3 p-4">
      <Skeleton className="h-9 w-full" />
      <Skeleton className="h-64 w-full" />
    </div>
  )
}
