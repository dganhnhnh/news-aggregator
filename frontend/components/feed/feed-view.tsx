'use client'

import { useMemo, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { FiltersBar } from './filters-bar'
import { ArticleList } from './article-list'
import { DetailPanel } from './detail-panel'
import { useArticles } from '@/hooks/use-articles'
import { sinceFromRange, type TimeRangeValue } from '@/lib/time'

export function FeedView({
  lockedSource,
}: {
  lockedSource?: { slug: string; name: string }
}) {
  const router = useRouter()
  const searchParams = useSearchParams()
  const selectedId = searchParams.get('id') ? Number(searchParams.get('id')) : null

  const [q, setQ] = useState('')
  const [range, setRange] = useState<TimeRangeValue>('7d')
  const [selectedSources, setSelectedSources] = useState<string[]>([])

  const since = useMemo(() => sinceFromRange(range), [range])
  const base = lockedSource
    ? `/api/sources/${encodeURIComponent(lockedSource.slug)}/articles`
    : '/api/articles'
  const sources = lockedSource ? undefined : selectedSources

  const { articles, isLoading, isLoadingMore, hasMore, error, loadMore, mutate } = useArticles({
    base,
    since,
    sources,
    q: q || undefined,
  })

  function selectArticle(id: number) {
    const sp = new URLSearchParams(searchParams.toString())
    sp.set('id', String(id))
    router.replace(`?${sp.toString()}`, { scroll: false })
  }

  return (
    <div className="flex h-svh min-w-0 flex-1 flex-col">
      <FiltersBar
        q={q}
        onQChange={setQ}
        range={range}
        onRangeChange={setRange}
        selectedSources={selectedSources}
        onSelectedSourcesChange={setSelectedSources}
        lockedSourceName={lockedSource?.name}
        resultLabel={
          isLoading ? undefined : `${articles.length} article${articles.length === 1 ? '' : 's'}`
        }
      />
      <div className="flex min-h-0 flex-1">
        <div className="w-full max-w-sm shrink-0 border-r border-border md:max-w-md">
          <ArticleList
            articles={articles}
            isLoading={isLoading}
            isLoadingMore={isLoadingMore}
            hasMore={hasMore}
            error={error}
            selectedId={selectedId}
            onSelect={selectArticle}
            onLoadMore={loadMore}
            onRetry={() => void mutate()}
          />
        </div>
        <div className="hidden min-w-0 flex-1 md:block">
          <DetailPanel articleId={selectedId} />
        </div>
      </div>
    </div>
  )
}
