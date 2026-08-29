import type {
  ArticleDetail,
  ArticlePage,
  Source,
  StoryDetail,
  StoryPage,
  TopicCatalog,
} from './types'

export class ApiError extends Error {
  status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

export async function fetcher<T>(url: string): Promise<T> {
  const res = await fetch(url)
  if (!res.ok) {
    const body = (await res.json().catch(() => null)) as
      | { detail?: unknown; error?: unknown }
      | null
    const message =
      (typeof body?.detail === 'string' && body.detail) ||
      (typeof body?.error === 'string' && body.error) ||
      `Request failed (${res.status})`
    throw new ApiError(message, res.status)
  }
  return res.json() as Promise<T>
}

export type ArticlesParams = {
  since?: string
  until?: string
  limit?: number
  cursor?: string | null
  sources?: string[]
  topics?: string[]
  q?: string
}

export type SourcesParams = {
  since?: string
  until?: string
}

export type StoriesParams = {
  since?: string
  until?: string
  limit?: number
  cursor?: string | null
  sources?: string[]
  topics?: string[]
  minSources?: number
  minArticles?: number
}

export function buildStoriesKey(base: string, params: StoriesParams): string {
  const sp = new URLSearchParams()
  if (params.since) sp.set('since', params.since)
  if (params.until) sp.set('until', params.until)
  if (params.limit) sp.set('limit', String(params.limit))
  if (params.cursor) sp.set('cursor', params.cursor)
  if (params.minSources) sp.set('min_sources', String(params.minSources))
  if (params.minArticles) sp.set('min_articles', String(params.minArticles))
  for (const s of params.sources ?? []) sp.append('source', s)
  for (const topic of params.topics ?? []) sp.append('topic', topic)
  const qs = sp.toString()
  return qs ? `${base}?${qs}` : base
}

export function buildArticlesKey(base: string, params: ArticlesParams): string {
  const sp = new URLSearchParams()
  if (params.since) sp.set('since', params.since)
  if (params.until) sp.set('until', params.until)
  if (params.limit) sp.set('limit', String(params.limit))
  if (params.cursor) sp.set('cursor', params.cursor)
  if (params.q) sp.set('q', params.q)
  for (const s of params.sources ?? []) sp.append('source', s)
  for (const topic of params.topics ?? []) sp.append('topic', topic)
  const qs = sp.toString()
  return qs ? `${base}?${qs}` : base
}

export function buildSourcesKey(params: SourcesParams = {}): string {
  const sp = new URLSearchParams()
  if (params.since) sp.set('since', params.since)
  const qs = sp.toString()
  return qs ? `/api/sources?${qs}` : '/api/sources'
}

export type { ArticleDetail, ArticlePage, Source, StoryDetail, StoryPage, TopicCatalog }
