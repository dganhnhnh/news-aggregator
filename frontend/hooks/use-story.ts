'use client'

import useSWR from 'swr'
import { fetcher } from '@/lib/client-api'
import type { StoryDetail } from '@/lib/types'

export function useStory(id: number | null) {
  const { data, error, isLoading, mutate } = useSWR<StoryDetail>(
    id !== null ? `/api/stories/${id}` : null,
    fetcher,
    { revalidateOnFocus: false },
  )

  return {
    story: data ?? null,
    isLoading,
    error,
    retry: () => mutate(),
  }
}
