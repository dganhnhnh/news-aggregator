'use client'

import useSWR from 'swr'
import { buildSourcesKey, fetcher } from '@/lib/client-api'
import { DEFAULT_START_TIME } from '@/lib/time'
import type { Source } from '@/lib/types'

export function useSources(
  { since = DEFAULT_START_TIME, enabled = true }: { since?: string; enabled?: boolean } = {},
) {
  const { data, error, isLoading, mutate } = useSWR<Source[]>(
    enabled ? buildSourcesKey({ since }) : null,
    fetcher,
    { keepPreviousData: true, revalidateOnFocus: false },
  )

  return {
    sources: data ?? [],
    isLoading,
    error,
    retry: () => mutate(),
  }
}
