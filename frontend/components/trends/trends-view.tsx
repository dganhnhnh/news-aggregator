'use client'

import { useEffect, useMemo, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { AlertCircle, SearchX, TrendingUp } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Empty, EmptyHeader, EmptyMedia, EmptyTitle, EmptyDescription } from '@/components/ui/empty'
import { Skeleton } from '@/components/ui/skeleton'
import { useDebouncedValue } from '@/hooks/use-debounced-value'
import { useTrends } from '@/hooks/use-trends'
import { sinceFromRange, type TimeRangeValue } from '@/lib/time'
import { cn } from '@/lib/utils'
import { TrendBarChart, type TrendMode } from './trend-bar-chart'
import { TrendArticles } from './trend-articles'
import { TrendsToolbar } from './trends-toolbar'

function isTimeRange(value: string | null): value is TimeRangeValue {
  return value === '7d' || value === '30d' || value === 'all'
}

export function TrendsView() {
  const router = useRouter()
  const searchParams = useSearchParams()

  const [input, setInput] = useState(() => searchParams.get('q') ?? '')
  const [range, setRange] = useState<TimeRangeValue>(() => {
    const fromUrl = searchParams.get('range')
    return isTimeRange(fromUrl) ? fromUrl : '30d'
  })
  const [mode, setMode] = useState<TrendMode>('count')
  const selectedTopics = searchParams.getAll('topic')

  const debounced = useDebouncedValue(input, 350)
  // Clearing the box returns to the empty state immediately instead of showing
  // the previous chart for another 350ms.
  const q = input.trim() === '' ? '' : debounced.trim()
  // `sinceFromRange` calls `new Date()`, so an unmemoized call would mint a new
  // SWR key on every render and refetch forever.
  const since = useMemo(() => sinceFromRange(range), [range])

  const { series, isLoading, isRefreshing, error, retry } = useTrends({
    q,
    since,
    topics: selectedTopics,
  })

  // Mirror the *debounced* query, so the URL is shareable without rewriting
  // history on every keystroke.
  useEffect(() => {
    const sp = new URLSearchParams(searchParams.toString())
    if (q) sp.set('q', q)
    else sp.delete('q')
    sp.set('range', range)
    const next = sp.toString()
    if (next !== searchParams.toString()) {
      router.replace(`?${next}`, { scroll: false })
    }
    // `searchParams` is intentionally read but not depended on: including it
    // would re-run this effect from its own replace.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q, range, router])

  function changeSelectedTopics(topics: string[]) {
    const sp = new URLSearchParams(searchParams.toString())
    sp.delete('topic')
    for (const topic of topics) sp.append('topic', topic)
    router.replace(`?${sp.toString()}`, { scroll: false })
  }

  return (
    <div className="flex h-svh min-w-0 flex-1 flex-col">
      <TrendsToolbar
        q={input}
        onQChange={setInput}
        range={range}
        onRangeChange={setRange}
        mode={mode}
        onModeChange={setMode}
        selectedTopics={selectedTopics}
        onSelectedTopicsChange={changeSelectedTopics}
        resultLabel={
          // Clearing the query hides the result count immediately, even while
          // the debounced search and SWR state settle.
          q && series
            ? `${series.totals.matches.toLocaleString()} mention${
                series.totals.matches === 1 ? '' : 's'
              } across ${series.days.length} day${series.days.length === 1 ? '' : 's'}` +
              ` · ${series.totals.articles.toLocaleString()} articles searched`
            : undefined
        }
      />
      <div className="min-h-0 flex-1 overflow-auto p-4">
        <TrendsBody
          key={[q, since, ...selectedTopics].join('|')}
          q={q}
          mode={mode}
          topics={selectedTopics}
          series={series}
          isLoading={isLoading}
          isRefreshing={isRefreshing}
          error={error}
          onRetry={retry}
        />
      </div>
    </div>
  )
}

function TrendsBody({
  q,
  mode,
  topics,
  series,
  isLoading,
  isRefreshing,
  error,
  onRetry,
}: {
  q: string
  mode: TrendMode
  topics: string[]
  series: ReturnType<typeof useTrends>['series']
  isLoading: boolean
  isRefreshing: boolean
  error: unknown
  onRetry: () => void
}) {
  const [selectedDay, setSelectedDay] = useState<string | null>(null)
  if (!q) {
    return (
      <Empty className="h-full">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <TrendingUp />
          </EmptyMedia>
          <EmptyTitle>Search for a trend</EmptyTitle>
          <EmptyDescription>
            Type a query to see how often it is mentioned each day.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    )
  }

  if (isLoading) {
    return (
      <div className="flex flex-col gap-2">
        <Skeleton className="h-64 w-full" />
        <Skeleton className="h-3 w-1/3" />
      </div>
    )
  }

  if (error && !series) {
    return (
      <Empty className="h-full">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <AlertCircle />
          </EmptyMedia>
          <EmptyTitle>Could not load the trend</EmptyTitle>
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

  if (!series) return null

  if (series.totals.matches === 0) {
    return (
      <Empty className="h-full">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <SearchX />
          </EmptyMedia>
          <EmptyTitle>No mentions found</EmptyTitle>
          <EmptyDescription>
            Nothing matched “{series.query}” in this window. Try a wider range or a
            different wording — very common words are ignored by the search index.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    )
  }

  return (
    <div className={cn('transition-opacity', isRefreshing && 'opacity-60')}>
      <TrendBarChart
        days={series.days}
        mode={mode}
        selectedDay={selectedDay}
        onSelectDay={setSelectedDay}
      />
      <TrendArticles
        key={selectedDay ?? 'latest'}
        series={series}
        selectedDay={selectedDay}
        topics={topics}
        onClearDay={() => setSelectedDay(null)}
      />
    </div>
  )
}
