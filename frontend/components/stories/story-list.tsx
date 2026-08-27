'use client'

import { useEffect, useRef } from 'react'
import { AlertCircle, Inbox } from 'lucide-react'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Skeleton } from '@/components/ui/skeleton'
import { Button } from '@/components/ui/button'
import { Empty, EmptyHeader, EmptyMedia, EmptyTitle, EmptyDescription } from '@/components/ui/empty'
import { StoryRow } from './story-row'
import type { StorySummary } from '@/lib/types'

export function StoryList({
  stories,
  isLoading,
  isLoadingMore,
  hasMore,
  error,
  selectedId,
  onSelect,
  onLoadMore,
  onRetry,
}: {
  stories: StorySummary[]
  isLoading: boolean
  isLoadingMore: boolean
  hasMore: boolean
  error: unknown
  selectedId: number | null
  onSelect: (id: number) => void
  onLoadMore: () => void
  onRetry: () => void
}) {
  const sentinelRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const el = sentinelRef.current
    if (!el) return
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting && hasMore && !isLoadingMore && !error) {
          onLoadMore()
        }
      },
      { rootMargin: '200px' },
    )
    observer.observe(el)
    return () => observer.disconnect()
  }, [error, hasMore, isLoadingMore, onLoadMore])

  if (isLoading) {
    return (
      <div className="flex flex-col">
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i} className="flex flex-col gap-2 border-b border-border/60 px-4 py-3">
            <Skeleton className="h-3.5 w-24" />
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-3 w-4/5" />
          </div>
        ))}
      </div>
    )
  }

  if (stories.length === 0 && error) {
    return (
      <Empty className="h-full">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <AlertCircle />
          </EmptyMedia>
          <EmptyTitle>Could not load stories</EmptyTitle>
          <EmptyDescription>
            {error instanceof Error ? error.message : 'The news service is unavailable.'}
          </EmptyDescription>
          <Button variant="outline" size="sm" onClick={onRetry}>
            Try again
          </Button>
        </EmptyHeader>
      </Empty>
    )
  }

  if (stories.length === 0) {
    return (
      <Empty className="h-full">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <Inbox />
          </EmptyMedia>
          <EmptyTitle>No stories found</EmptyTitle>
          <EmptyDescription>
            Try widening the time range or clearing your filters.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    )
  }

  return (
    <ScrollArea className="h-full">
      <div className="flex flex-col">
        {stories.map((story) => (
          <StoryRow
            key={story.id}
            story={story}
            selected={story.id === selectedId}
            onSelect={() => onSelect(story.id)}
          />
        ))}
        <div ref={sentinelRef} className="h-1" />
        {isLoadingMore && (
          <div className="flex flex-col gap-2 border-b border-border/60 px-4 py-3">
            <Skeleton className="h-3.5 w-24" />
            <Skeleton className="h-4 w-full" />
          </div>
        )}
        {Boolean(error) && stories.length > 0 && (
          <div className="flex flex-col items-center gap-2 border-b border-border/60 px-4 py-5 text-center">
            <p className="text-xs text-destructive">Could not load more stories.</p>
            <Button variant="outline" size="sm" onClick={onRetry}>
              Try again
            </Button>
          </div>
        )}
        {!hasMore && stories.length > 0 && (
          <p className="px-4 py-6 text-center text-xs text-muted-foreground">
            You&apos;ve reached the end of this feed.
          </p>
        )}
      </div>
    </ScrollArea>
  )
}
