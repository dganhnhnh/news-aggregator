'use client'

import useSWR from 'swr'
import { fetcher } from '@/lib/client-api'
import type { ManagedFeed } from '@/lib/types'

export function useFeeds() {
  const { data, error, isLoading, mutate } = useSWR<ManagedFeed[]>(
    '/api/admin/feeds?status=all',
    fetcher,
    { revalidateOnFocus: false },
  )

  return {
    feeds: data ?? [],
    error,
    isLoading,
    refresh: mutate,
  }
}
