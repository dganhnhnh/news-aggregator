import { cache } from 'react'
import { NextResponse } from 'next/server'
import { DEFAULT_START_TIME } from './time'
import type { ArticleDetail, Source } from './types'

const DEFAULT_API_URL = 'http://localhost:8000'
const DEFAULT_ADMIN_API_URL = 'http://localhost:8001'

export class BackendApiError extends Error {
  status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = 'BackendApiError'
    this.status = status
  }
}

function apiUrl(): string {
  return (process.env.API_URL?.trim() || DEFAULT_API_URL).replace(/\/+$/, '')
}

function adminApiUrl(): string {
  return (process.env.ADMIN_API_URL?.trim() || DEFAULT_ADMIN_API_URL).replace(/\/+$/, '')
}

async function backendFetch(
  path: string,
  { baseUrl = apiUrl(), request }: { baseUrl?: string; request?: Request } = {},
): Promise<Response> {
  const method = request?.method ?? 'GET'
  const contentType = request?.headers.get('content-type')
  const accept = request?.headers.get('accept')
  try {
    return await fetch(`${baseUrl}${path}`, {
      method,
      body: method === 'GET' || method === 'HEAD' ? undefined : await request?.arrayBuffer(),
      cache: 'no-store',
      signal: request?.signal,
      headers: {
        accept: accept || 'application/json',
        ...(contentType ? { 'content-type': contentType } : {}),
      },
    })
  } catch {
    throw new BackendApiError('Backend unavailable', 502)
  }
}

async function responseError(response: Response): Promise<BackendApiError> {
  const body = (await response.json().catch(() => null)) as
    | { detail?: unknown; error?: unknown }
    | null
  const detail = body?.detail
  const message =
    (typeof detail === 'string' && detail) ||
    (typeof detail === 'object' &&
      detail !== null &&
      'reason' in detail &&
      typeof detail.reason === 'string' &&
      detail.reason) ||
    (typeof body?.error === 'string' && body.error) ||
    `Request failed (${response.status})`
  return new BackendApiError(message, response.status)
}

async function backendJson<T>(path: string): Promise<T> {
  const response = await backendFetch(path)
  if (!response.ok) throw await responseError(response)
  return response.json() as Promise<T>
}

export async function proxyBackend(path: string, request?: Request): Promise<NextResponse> {
  try {
    const response = await backendFetch(path, { request })
    const headers: Record<string, string> = {
      'cache-control': response.headers.get('cache-control') ?? 'no-store',
      'content-type': response.headers.get('content-type') ?? 'application/json',
    }
    for (const name of ['x-request-id', 'x-accel-buffering']) {
      const value = response.headers.get(name)
      if (value) headers[name] = value
    }
    return new NextResponse(response.body, {
      status: response.status,
      headers,
    })
  } catch (error) {
    if (error instanceof BackendApiError) {
      return NextResponse.json({ error: error.message }, { status: error.status })
    }
    throw error
  }
}

export async function proxyAdminBackend(path: string, request: Request): Promise<NextResponse> {
  try {
    const response = await backendFetch(path, { baseUrl: adminApiUrl(), request })
    return new NextResponse(response.body, {
      status: response.status,
      headers: {
        'cache-control': 'no-store',
        'content-type': response.headers.get('content-type') ?? 'application/json',
      },
    })
  } catch (error) {
    if (error instanceof BackendApiError) {
      return NextResponse.json({ error: error.message }, { status: error.status })
    }
    throw error
  }
}

export const getArticle = cache(async (id: string): Promise<ArticleDetail | null> => {
  if (!/^\d+$/.test(id)) return null

  try {
    return await backendJson<ArticleDetail>(`/api/articles/${encodeURIComponent(id)}`)
  } catch (error) {
    if (error instanceof BackendApiError && error.status === 404) return null
    throw error
  }
})

export const getSources = cache(async (since = DEFAULT_START_TIME): Promise<Source[]> => {
  const search = since ? `?${new URLSearchParams({ since }).toString()}` : ''
  return backendJson<Source[]>(`/api/sources${search}`)
})
