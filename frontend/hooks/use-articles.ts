'use client'

import useSWRInfinite from 'swr/infinite'
import { buildArticlesKey, fetcher } from '@/lib/client-api'
import type { ArticlePage } from '@/lib/types'

export type UseArticlesOptions = {
  base?: string
  since?: string
  until?: string
  sources?: string[]
  q?: string
  limit?: number
}

export function useArticles(options: UseArticlesOptions) {
  const { base = '/api/articles', since, until, sources, q, limit = 25 } = options

  const getKey = (pageIndex: number, previousPage: ArticlePage | null) => {
    if (previousPage && previousPage.next_cursor === null) return null
    const cursor = pageIndex === 0 ? null : previousPage?.next_cursor
    if (pageIndex !== 0 && !cursor) return null
    return buildArticlesKey(base, { since, until, sources, q, limit, cursor })
  }

  const { data, error, isLoading, isValidating, size, setSize, mutate } =
    useSWRInfinite<ArticlePage>(getKey, fetcher, {
      revalidateOnFocus: false,
      revalidateFirstPage: false,
    })

  const pages = data ?? []
  const articles = pages.flatMap((p) => p.items)
  const hasMore = pages.length === 0 || pages[pages.length - 1]?.next_cursor !== null
  const isLoadingMore = isValidating && size > pages.length

  return {
    articles,
    isLoading,
    isLoadingMore,
    hasMore,
    error,
    loadMore: () => setSize(size + 1),
    mutate,
  }
}
