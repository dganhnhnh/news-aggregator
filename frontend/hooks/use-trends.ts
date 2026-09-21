'use client'

import useSWR from 'swr'
import { buildTrendsKey, fetcher, type TrendsParams } from '@/lib/client-api'
import type { TrendSeries } from '@/lib/types'

export function useTrends(params: TrendsParams) {
  // Null key when the query is blank, so SWR stays idle until there is
  // something to chart.
  const key = buildTrendsKey(params)
  const { data, error, isLoading, isValidating, mutate } = useSWR<TrendSeries>(key, fetcher, {
    revalidateOnFocus: false,
  })

  return {
    series: data ?? null,
    isLoading,
    isRefreshing: isValidating && Boolean(data),
    error,
    retry: () => mutate(),
  }
}
