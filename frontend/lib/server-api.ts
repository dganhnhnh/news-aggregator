import { cache } from 'react'
import { NextResponse } from 'next/server'
import { DEFAULT_START_TIME } from './time'
import type { ArticleDetail, Source } from './types'

const DEFAULT_API_URL = 'http://localhost:8000'

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

async function backendFetch(path: string): Promise<Response> {
  try {
    return await fetch(`${apiUrl()}${path}`, {
      cache: 'no-store',
      headers: { accept: 'application/json' },
    })
  } catch {
    throw new BackendApiError('Backend unavailable', 502)
  }
}

async function responseError(response: Response): Promise<BackendApiError> {
  const body = (await response.json().catch(() => null)) as
    | { detail?: unknown; error?: unknown }
    | null
  const message =
    (typeof body?.detail === 'string' && body.detail) ||
    (typeof body?.error === 'string' && body.error) ||
    `Request failed (${response.status})`
  return new BackendApiError(message, response.status)
}

async function backendJson<T>(path: string): Promise<T> {
  const response = await backendFetch(path)
  if (!response.ok) throw await responseError(response)
  return response.json() as Promise<T>
}

export async function proxyBackend(path: string): Promise<NextResponse> {
  try {
    const response = await backendFetch(path)
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
