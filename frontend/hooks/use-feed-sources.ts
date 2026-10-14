'use client'

import useSWR from 'swr'
import { buildFeedsKey, fetcher } from '@/lib/client-api'
import { DEFAULT_START_TIME } from '@/lib/time'
import type { FeedPriority, FeedSource } from '@/lib/types'

// Feeds that have articles in the window, for the sidebar. Not the feed
// registry: that is `useFeeds`, which also lists feeds with nothing yet.
export function useFeedSources({
  since = DEFAULT_START_TIME,
  tier,
}: { since?: string; tier?: FeedPriority } = {}) {
  const { data, error, isLoading, mutate } = useSWR<FeedSource[]>(
    buildFeedsKey({ since, tier }),
    fetcher,
    { keepPreviousData: true, revalidateOnFocus: false },
  )

  return {
    feeds: data ?? [],
    isLoading,
    error,
    retry: () => mutate(),
  }
}
