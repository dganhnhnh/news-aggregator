'use client'

import useSWR from 'swr'
import { buildSourcesKey, fetcher } from '@/lib/client-api'
import { DEFAULT_START_TIME } from '@/lib/time'
import type { Source } from '@/lib/types'

export function useSources({ since = DEFAULT_START_TIME }: { since?: string } = {}) {
  const { data, error, isLoading, mutate } = useSWR<Source[]>(
    buildSourcesKey({ since }),
    fetcher,
    { revalidateOnFocus: false },
  )

  return {
    sources: data ?? [],
    isLoading,
    error,
    retry: () => mutate(),
  }
}
