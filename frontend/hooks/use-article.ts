'use client'

import useSWR from 'swr'
import { fetcher } from '@/lib/client-api'
import type { ArticleDetail } from '@/lib/types'

export function useArticle(id: number | null) {
  const { data, error, isLoading, mutate } = useSWR<ArticleDetail>(
    id !== null ? `/api/articles/${id}` : null,
    fetcher,
    { revalidateOnFocus: false },
  )

  return {
    article: data ?? null,
    isLoading,
    error,
    retry: () => mutate(),
  }
}
