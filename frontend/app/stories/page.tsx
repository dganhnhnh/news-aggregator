import { Suspense } from 'react'
import { NavRail } from '@/components/nav-rail'
import { StoryView } from '@/components/stories/story-view'
import { Skeleton } from '@/components/ui/skeleton'

export default function Page() {
  return (
    <div className="flex h-svh w-full">
      <NavRail />
      <div className="flex min-w-0 flex-1">
        <Suspense fallback={<StoriesFallback />}>
          <StoryView />
        </Suspense>
      </div>
    </div>
  )
}

function StoriesFallback() {
  return (
    <div className="flex h-svh flex-1 flex-col gap-3 p-4">
      <Skeleton className="h-9 w-full" />
      <Skeleton className="h-9 w-full" />
      <Skeleton className="h-full w-full" />
    </div>
  )
}
