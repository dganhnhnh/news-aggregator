import type {
  ArticleDetail,
  ArticlePage,
  ManagedFeed,
  Source,
  StoryDetail,
  StoryPage,
  TopicCatalog,
  TrendSeries,
} from './types'

export class ApiError extends Error {
  status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

function apiErrorMessage(body: { detail?: unknown; error?: unknown } | null, status: number) {
  if (typeof body?.detail === 'string' && body.detail) return body.detail
  if (
    typeof body?.detail === 'object' &&
    body.detail !== null &&
    'reason' in body.detail &&
    typeof body.detail.reason === 'string'
  ) {
    return body.detail.reason
  }
  if (typeof body?.error === 'string' && body.error) return body.error
  if (Array.isArray(body?.detail)) {
    const first = body.detail[0] as { msg?: unknown } | undefined
    if (typeof first?.msg === 'string') {
      return first.msg.replace(/^Value error,\s*/, '')
    }
  }
  return `Request failed (${status})`
}

export async function fetcher<T>(url: string): Promise<T> {
  const res = await fetch(url)
  if (!res.ok) {
    const body = (await res.json().catch(() => null)) as
      | { detail?: unknown; error?: unknown }
      | null
    throw new ApiError(apiErrorMessage(body, res.status), res.status)
  }
  return res.json() as Promise<T>
}

export async function apiRequest<T>(
  url: string,
  method: 'POST' | 'PATCH',
  body: unknown,
): Promise<T> {
  const res = await fetch(url, {
    method,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const responseBody = (await res.json().catch(() => null)) as
      | { detail?: unknown; error?: unknown }
      | null
    throw new ApiError(apiErrorMessage(responseBody, res.status), res.status)
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

export type TrendsParams = {
  q: string
  since?: string
  until?: string
  sources?: string[]
  topics?: string[]
}

// Returns null for a blank query, which is SWR's "do not fetch" signal: the
// endpoint requires `q`, so an empty box must not produce a request at all.
export function buildTrendsKey(params: TrendsParams): string | null {
  const q = params.q.trim()
  if (!q) return null
  const sp = new URLSearchParams()
  sp.set('q', q)
  if (params.since) sp.set('since', params.since)
  if (params.until) sp.set('until', params.until)
  for (const s of params.sources ?? []) sp.append('source', s)
  for (const topic of params.topics ?? []) sp.append('topic', topic)
  return `/api/trends?${sp.toString()}`
}

export function buildSourcesKey(params: SourcesParams = {}): string {
  const sp = new URLSearchParams()
  if (params.since) sp.set('since', params.since)
  const qs = sp.toString()
  return qs ? `/api/sources?${qs}` : '/api/sources'
}

export type {
  ArticleDetail,
  ArticlePage,
  ManagedFeed,
  Source,
  StoryDetail,
  StoryPage,
  TopicCatalog,
  TrendSeries,
}
