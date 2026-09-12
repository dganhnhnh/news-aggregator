import type { ResearchStreamEvent } from './types'

export class ResearchStreamError extends Error {
  status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = 'ResearchStreamError'
    this.status = status
  }
}

function errorMessage(body: unknown, status: number): string {
  if (!body || typeof body !== 'object') return `Request failed (${status})`
  const value = body as { detail?: unknown; error?: unknown }
  if (typeof value.detail === 'string') return value.detail
  if (value.detail && typeof value.detail === 'object') {
    const detail = value.detail as { reason?: unknown; message?: unknown }
    if (typeof detail.reason === 'string') return detail.reason
    if (typeof detail.message === 'string') return detail.message
  }
  if (typeof value.error === 'string') return value.error
  return `Request failed (${status})`
}

function parseFrame(frame: string): ResearchStreamEvent | null {
  let event = 'message'
  const data: string[] = []
  for (const line of frame.split(/\r?\n/)) {
    if (!line || line.startsWith(':')) continue
    if (line.startsWith('event:')) event = line.slice(6).trim()
    if (line.startsWith('data:')) data.push(line.slice(5).trimStart())
  }
  if (!data.length || event === 'message') return null
  return { event, data: JSON.parse(data.join('\n')) } as ResearchStreamEvent
}

function nextFrame(buffer: string): { frame: string; rest: string } | null {
  const boundary = buffer.match(/\r?\n\r?\n/)
  if (!boundary || boundary.index === undefined) return null
  return {
    frame: buffer.slice(0, boundary.index),
    rest: buffer.slice(boundary.index + boundary[0].length),
  }
}

export async function streamResearch({
  query,
  threadId,
  signal,
  onEvent,
}: {
  query: string
  threadId: string
  signal: AbortSignal
  onEvent: (event: ResearchStreamEvent) => void
}): Promise<void> {
  const response = await fetch('/api/research/stream', {
    method: 'POST',
    headers: {
      accept: 'text/event-stream',
      'content-type': 'application/json',
    },
    body: JSON.stringify({ query, thread_id: threadId }),
    signal,
  })
  if (!response.ok) {
    const body = await response.json().catch(() => null)
    throw new ResearchStreamError(errorMessage(body, response.status), response.status)
  }
  if (!response.body) {
    throw new ResearchStreamError('The research stream returned no body', 502)
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  try {
    while (true) {
      const { value, done } = await reader.read()
      buffer += decoder.decode(value, { stream: !done })
      let parsed = nextFrame(buffer)
      while (parsed) {
        buffer = parsed.rest
        const event = parseFrame(parsed.frame)
        if (event) onEvent(event)
        parsed = nextFrame(buffer)
      }
      if (done) break
    }
  } finally {
    reader.releaseLock()
  }
}
