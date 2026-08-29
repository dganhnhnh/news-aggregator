'use client'

import useSWR from 'swr'
import { fetcher } from '@/lib/client-api'
import type { TopicCatalog } from '@/lib/types'

export function useTopics(taxonomy = 'default') {
  const key = `/api/topics?${new URLSearchParams({ taxonomy }).toString()}`
  const { data, error, isLoading, mutate } = useSWR<TopicCatalog>(key, fetcher, {
    keepPreviousData: true,
    revalidateOnFocus: false,
  })

  return {
    catalog: data ?? null,
    topics: data?.topics ?? [],
    isLoading,
    error,
    retry: () => mutate(),
  }
}
