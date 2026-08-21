'use client'

import useSWR from 'swr'
import { fetcher } from '@/lib/client-api'
import type { Source } from '@/lib/types'

export function useSources() {
  const { data, error, isLoading, mutate } = useSWR<Source[]>('/api/sources', fetcher, {
    revalidateOnFocus: false,
  })

  return {
    sources: data ?? [],
    isLoading,
    error,
    retry: () => mutate(),
  }
}
