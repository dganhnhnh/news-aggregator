'use client'

import useSWRInfinite from 'swr/infinite'
import { buildStoriesKey, fetcher } from '@/lib/client-api'
import type { StoryPage } from '@/lib/types'

export type UseStoriesOptions = {
  since?: string
  until?: string
  sources?: string[]
  minSources?: number
  minArticles?: number
  limit?: number
}

export function useStories(options: UseStoriesOptions) {
  const { since, until, sources, minSources, minArticles, limit = 25 } = options

  const getKey = (pageIndex: number, previousPage: StoryPage | null) => {
    if (previousPage && previousPage.next_cursor === null) return null
    const cursor = pageIndex === 0 ? null : previousPage?.next_cursor
    if (pageIndex !== 0 && !cursor) return null
    return buildStoriesKey('/api/stories', {
      since,
      until,
      sources,
      minSources,
      minArticles,
      limit,
      cursor,
    })
  }

  const { data, error, isLoading, isValidating, size, setSize, mutate } =
    useSWRInfinite<StoryPage>(getKey, fetcher, {
      revalidateOnFocus: false,
      revalidateFirstPage: false,
    })

  const pages = data ?? []
  const stories = pages.flatMap((p) => p.items)
  const hasMore = pages.length === 0 || pages[pages.length - 1]?.next_cursor !== null
  const isLoadingMore = isValidating && size > pages.length

  return {
    stories,
    isLoading,
    isLoadingMore,
    hasMore,
    error,
    loadMore: () => setSize(size + 1),
    mutate,
  }
}
