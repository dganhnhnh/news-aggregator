'use client'

import { useMemo, useState } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { StoryFiltersBar } from './story-filters-bar'
import { StoryList } from './story-list'
import { StoryDetailPanel } from './story-detail-panel'
import { ArticlePreviewPanel } from './article-preview-panel'
import { useStories } from '@/hooks/use-stories'
import { sinceFromRange, type TimeRangeValue } from '@/lib/time'

export function StoryView() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const selectedId = searchParams.get('id') ? Number(searchParams.get('id')) : null
  const previewArticleId = searchParams.get('article') ? Number(searchParams.get('article')) : null

  const [range, setRange] = useState<TimeRangeValue>('all')
  const [selectedSources, setSelectedSources] = useState<string[]>([])
  const [minSources, setMinSources] = useState(2)
  const [minArticles, setMinArticles] = useState(2)

  const since = useMemo(() => sinceFromRange(range), [range])

  const { stories, isLoading, isLoadingMore, hasMore, error, loadMore, mutate } = useStories({
    since,
    sources: selectedSources,
    minSources,
    minArticles,
  })

  function selectStory(id: number) {
    const sp = new URLSearchParams(searchParams.toString())
    sp.set('id', String(id))
    sp.delete('article')
    router.replace(`?${sp.toString()}`, { scroll: false })
  }

  function selectArticlePreview(articleId: number) {
    const sp = new URLSearchParams(searchParams.toString())
    sp.set('article', String(articleId))
    router.replace(`?${sp.toString()}`, { scroll: false })
  }

  function closeArticlePreview() {
    const sp = new URLSearchParams(searchParams.toString())
    sp.delete('article')
    router.replace(`?${sp.toString()}`, { scroll: false })
  }

  return (
    <div className="flex h-svh min-w-0 flex-1 flex-col">
      <StoryFiltersBar
        range={range}
        onRangeChange={setRange}
        since={since}
        selectedSources={selectedSources}
        onSelectedSourcesChange={setSelectedSources}
        minSources={minSources}
        onMinSourcesChange={setMinSources}
        minArticles={minArticles}
        onMinArticlesChange={setMinArticles}
        resultLabel={
          isLoading ? undefined : `${stories.length} stor${stories.length === 1 ? 'y' : 'ies'}`
        }
      />
      <div className="flex min-h-0 flex-1">
        <div className="w-full max-w-sm shrink-0 border-r border-border md:max-w-md">
          <StoryList
            stories={stories}
            isLoading={isLoading}
            isLoadingMore={isLoadingMore}
            hasMore={hasMore}
            error={error}
            selectedId={selectedId}
            onSelect={selectStory}
            onLoadMore={loadMore}
            onRetry={() => void mutate()}
          />
        </div>
        <div className="hidden min-w-0 flex-1 flex-col border-r border-border md:flex">
          <StoryDetailPanel
            storyId={selectedId}
            onArticleClick={selectArticlePreview}
          />
        </div>
        <div
          className={`hidden shrink-0 flex-col overflow-hidden border-border transition-[width] duration-200 ease-in-out lg:flex ${
            previewArticleId ? 'w-full max-w-md border-l' : 'w-0'
          }`}
        >
          <div className="flex w-full max-w-md shrink-0 items-center justify-between border-b border-border bg-background/95 px-4 py-3 backdrop-blur">
            <h3 className="text-sm font-semibold">Article Preview</h3>
            <Button
              variant="ghost"
              size="sm"
              onClick={closeArticlePreview}
              className="h-6 w-6 p-0"
            >
              <X className="size-4" />
            </Button>
          </div>
          <div className="w-full max-w-md min-w-0 flex-1 overflow-hidden">
            <ArticlePreviewPanel articleId={previewArticleId} />
          </div>
        </div>
      </div>
    </div>
  )
}
