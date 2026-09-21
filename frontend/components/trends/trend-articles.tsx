'use client'

import { useEffect, useRef } from 'react'
import { AlertCircle, Inbox } from 'lucide-react'
import { ArticleRow } from '@/components/feed/article-row'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { useArticles } from '@/hooks/use-articles'
import type { TrendSeries } from '@/lib/types'

function dayWindow(series: TrendSeries, selectedDay: string | null) {
  if (!selectedDay) return { since: series.window.since, until: series.window.until }
  const start = new Date(`${selectedDay}T00:00:00Z`)
  const end = new Date(start.getTime() + 86_400_000)
  return {
    since: new Date(Math.max(start.getTime(), Date.parse(series.window.since))).toISOString(),
    until: new Date(Math.min(end.getTime(), Date.parse(series.window.until))).toISOString(),
  }
}

export function TrendArticles({
  series,
  selectedDay,
  topics,
  onClearDay,
}: {
  series: TrendSeries
  selectedDay: string | null
  topics: string[]
  onClearDay: () => void
}) {
  const sentinelRef = useRef<HTMLDivElement>(null)
  const articleWindow = dayWindow(series, selectedDay)
  const { articles, isLoading, isLoadingMore, hasMore, error, loadMore, mutate } = useArticles({
    q: series.query,
    since: articleWindow.since,
    until: articleWindow.until,
    topics,
    // The feed normally takes only three hits per source. Here the per-source
    // cap must equal the page size, or a source-heavy result set could return
    // fewer than `limit` and incorrectly signal the end of cursor pagination.
    limit: 20,
    maxPerSource: 20,
  })

  useEffect(() => {
    const sentinel = sentinelRef.current
    if (!sentinel || !hasMore || isLoading || isLoadingMore || error) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) loadMore()
      },
      { rootMargin: '250px' },
    )
    observer.observe(sentinel)
    return () => observer.disconnect()
  }, [hasMore, isLoading, isLoadingMore, error, loadMore])

  const selectedLabel = selectedDay
    ? new Date(`${selectedDay}T00:00:00Z`).toLocaleDateString('en-US', {
        month: 'long',
        day: 'numeric',
        year: 'numeric',
        timeZone: 'UTC',
      })
    : null

  return (
    <section className="mx-auto mt-8 w-full max-w-5xl border-t border-border pt-5">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div>
          <h2 className="text-base font-semibold">
            {selectedLabel ? `Articles on ${selectedLabel}` : 'Latest matching articles'}
          </h2>
          <p className="text-xs text-muted-foreground">
            {selectedLabel
              ? `Mentions of “${series.query}” on this UTC day`
              : `Most recent mentions of “${series.query}” in this range`}
          </p>
        </div>
        {selectedDay && (
          <Button variant="outline" size="sm" onClick={onClearDay}>
            Clear day
          </Button>
        )}
      </div>

      {isLoading ? (
        <div className="space-y-3 py-3">
          {Array.from({ length: 5 }, (_, index) => (
            <Skeleton key={index} className="h-20 w-full" />
          ))}
        </div>
      ) : articles.length === 0 ? (
        <div className="flex flex-col items-center gap-3 rounded-lg border border-dashed border-border px-6 py-12 text-center">
          {error ? (
            <AlertCircle className="size-5 text-destructive" />
          ) : (
            <Inbox className="size-5 text-muted-foreground" />
          )}
          <p className="text-sm font-medium">
            {error ? 'Could not load articles' : 'No matching articles found'}
          </p>
          {error ? (
            <Button variant="outline" size="sm" onClick={() => void mutate()}>
              Try again
            </Button>
          ) : selectedDay ? (
            <Button variant="outline" size="sm" onClick={onClearDay}>
              View latest articles
            </Button>
          ) : null}
        </div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-border bg-card">
          {articles.map((article) => (
            <ArticleRow
              key={article.id}
              article={article}
              selected={false}
              href={`/articles/${article.id}`}
            />
          ))}
          <div ref={sentinelRef} className="h-px" />
          {isLoadingMore && <Skeleton className="mx-4 my-4 h-16" />}
          {error && (
            <div className="flex items-center justify-center gap-3 px-4 py-5">
              <span className="text-xs text-destructive">Could not load more articles.</span>
              <Button variant="outline" size="sm" onClick={() => void mutate()}>
                Try again
              </Button>
            </div>
          )}
          {!hasMore && (
            <p className="px-4 py-5 text-center text-xs text-muted-foreground">
              You&apos;ve reached the end of these articles.
            </p>
          )}
        </div>
      )}
    </section>
  )
}
