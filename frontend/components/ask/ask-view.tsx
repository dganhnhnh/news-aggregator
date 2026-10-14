'use client'

import { useEffect, useRef, useState } from 'react'
import {
  ArrowUp,
  ExternalLink,
  Plus,
  RotateCcw,
  Sparkles,
  Square,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { MarkdownContent } from '@/components/feed/markdown-content'
import { streamResearch } from '@/lib/research-stream'
import { applyResearchEvent, type ChatMessage } from '@/lib/research-message'
import { ResearchProgress } from './research-progress'
import type { ResearchCitation, ResearchStreamEvent } from '@/lib/types'

const SESSION_KEY = 'news-radar:ask-ai'

type StoredChat = {
  threadId: string
  messages: ChatMessage[]
}

function createId(): string {
  return crypto.randomUUID()
}

function statusLabel(status?: ChatMessage['status']): string {
  if (status === 'verifying') return 'Verifying sources and correcting the answer…'
  if (status === 'searching') return 'Searching articles…'
  if (status === 'reading') return 'Reading sources…'
  return 'Synthesizing answer…'
}

function citationMarkdown(content: string, citations: ResearchCitation[]): string {
  // The API returns sources in order of first citation. Keep the same numbers
  // while streaming, before the source metadata arrives.
  const numbers = new Map(
    citations.map((citation, index) => [citation.article_id, index + 1]),
  )
  const linkedIds = new Set(citations
    .filter((citation) => /^https?:\/\//.test(citation.url))
    .map((citation) => citation.article_id))
  const numberedContent = content.replace(/\[A(\d+)\]/g, (_, rawId: string) => {
    const id = Number(rawId)
    if (!numbers.has(id)) numbers.set(id, numbers.size + 1)
    const label = numbers.get(id)
    return linkedIds.has(id) ? `[${label}][A${id}]` : `[${label}]`
  })
  const definitions = citations
    .filter((citation) => linkedIds.has(citation.article_id))
    .map((citation) => `[A${citation.article_id}]: <${citation.url}>`)
    .join('\n')
  return definitions ? `${numberedContent}\n\n${definitions}` : numberedContent
}

function publishedDate(value: string | null): string | null {
  if (!value) return null
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return null
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(date)
}

export function AskView() {
  const [threadId, setThreadId] = useState('')
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [isRunning, setIsRunning] = useState(false)
  const [hydrated, setHydrated] = useState(false)
  const abortRef = useRef<AbortController | null>(null)
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const stickToBottomRef = useRef(true)

  useEffect(() => {
    try {
      const saved = sessionStorage.getItem(SESSION_KEY)
      if (saved) {
        const parsed = JSON.parse(saved) as StoredChat
        if (parsed.threadId && Array.isArray(parsed.messages)) {
          setThreadId(parsed.threadId)
          setMessages(parsed.messages.filter((message) => !message.pending))
        }
      }
    } catch {
      sessionStorage.removeItem(SESSION_KEY)
    }
    setHydrated(true)
  }, [])

  useEffect(() => {
    if (!hydrated || !threadId) return
    const completedMessages = messages.filter((message) => !message.pending)
    sessionStorage.setItem(
      SESSION_KEY,
      JSON.stringify({ threadId, messages: completedMessages } satisfies StoredChat),
    )
  }, [hydrated, messages, threadId])

  useEffect(() => {
    if (!stickToBottomRef.current) return
    const viewport = scrollRef.current
    if (viewport) viewport.scrollTo({ top: viewport.scrollHeight, behavior: 'smooth' })
  }, [messages])

  useEffect(() => () => abortRef.current?.abort(), [])

  function updateAssistant(
    id: string,
    update: (message: ChatMessage) => ChatMessage,
  ) {
    setMessages((current) =>
      current.map((message) => (message.id === id ? update(message) : message)),
    )
  }

  async function runQuery(
    rawQuery: string,
    { appendUser, assistantId = createId() }: { appendUser: boolean; assistantId?: string },
  ) {
    const query = rawQuery.trim()
    if (!query || isRunning) return
    const activeThreadId = threadId || createId()
    if (!threadId) setThreadId(activeThreadId)

    const assistant: ChatMessage = {
      id: assistantId,
      role: 'assistant',
      content: '',
      query,
      status: 'synthesizing',
      pending: true,
      steps: [],
      reasoningOpen: true,
      answerStarted: false,
    }
    setMessages((current) =>
      appendUser
        ? [
            ...current,
            { id: createId(), role: 'user', content: query },
            assistant,
          ]
        : current.map((message) =>
            message.id === assistantId ? assistant : message,
          ),
    )
    setInput('')
    setIsRunning(true)
    stickToBottomRef.current = true
    const controller = new AbortController()
    abortRef.current = controller
    let completed = false
    let streamError = false

    function onEvent(event: ResearchStreamEvent) {
      if (event.event === 'done') completed = true
      if (event.event === 'error') streamError = true
      updateAssistant(assistantId, (message) => applyResearchEvent(message, event))
    }

    try {
      await streamResearch({
        query,
        threadId: activeThreadId,
        signal: controller.signal,
        onEvent,
      })
      if (!completed && !streamError) {
        throw new Error('The response stream ended before completion')
      }
    } catch (error) {
      const stopped = controller.signal.aborted
      updateAssistant(assistantId, (message) => ({
        ...message,
        pending: false,
        status: undefined,
        error: stopped
          ? 'Generation stopped.'
          : error instanceof Error
            ? error.message
            : 'Research failed. Please try again.',
      }))
    } finally {
      if (abortRef.current === controller) abortRef.current = null
      setIsRunning(false)
    }
  }

  function submit() {
    void runQuery(input, { appendUser: true })
  }

  function retry(message: ChatMessage) {
    if (message.query) {
      void runQuery(message.query, { appendUser: false, assistantId: message.id })
    }
  }

  function stop() {
    abortRef.current?.abort()
  }

  function newChat() {
    abortRef.current?.abort()
    const oldThreadId = threadId
    setThreadId(createId())
    setMessages([])
    setInput('')
    sessionStorage.removeItem(SESSION_KEY)
    if (oldThreadId) {
      void fetch(`/api/research/threads/${encodeURIComponent(oldThreadId)}`, {
        method: 'DELETE',
      })
    }
  }

  return (
    <main className="flex h-svh min-w-0 flex-1 flex-col bg-background">
      <header className="flex h-14 shrink-0 items-center justify-between border-b border-border px-4 sm:px-6">
        <div className="flex items-center gap-2.5">
          <div className="flex size-8 items-center justify-center rounded-lg bg-primary/12 text-primary">
            <Sparkles className="size-4" />
          </div>
          <h1 className="text-base font-semibold tracking-tight">Ask AI</h1>
        </div>
        <Button variant="ghost" size="sm" onClick={newChat} disabled={!messages.length}>
          <Plus data-icon="inline-start" />
          New chat
        </Button>
      </header>

      <div
        ref={scrollRef}
        onScroll={(event) => {
          const viewport = event.currentTarget
          stickToBottomRef.current =
            viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight < 96
        }}
        className="min-h-0 flex-1 overflow-y-auto scrollbar-thin"
      >
        <div className="mx-auto flex min-h-full w-full max-w-3xl flex-col px-4 py-8 sm:px-6">
          {!messages.length ? (
            <div className="m-auto flex max-w-md flex-col items-center py-16 text-center">
              <div className="mb-5 flex size-12 items-center justify-center rounded-2xl border border-primary/20 bg-primary/10 text-primary shadow-sm">
                <Sparkles className="size-5" />
              </div>
              <h2 className="text-xl font-semibold tracking-tight">Ask your news archive</h2>
              <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                Search recent coverage, compare sources, or trace how a story developed.
              </p>
            </div>
          ) : (
            <div className="flex flex-col gap-8" aria-live="polite">
              {messages.map((message) =>
                message.role === 'user' ? (
                  <div key={message.id} className="flex justify-end">
                    <div className="max-w-[85%] rounded-2xl rounded-br-md bg-secondary px-4 py-2.5 text-[15px] leading-relaxed text-secondary-foreground sm:max-w-[75%]">
                      {message.content}
                    </div>
                  </div>
                ) : (
                  <article key={message.id} className="flex gap-3 sm:gap-4">
                    <div className="mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-lg bg-primary/12 text-primary">
                      <Sparkles className="size-3.5" />
                    </div>
                    <div className="min-w-0 flex-1">
                      <ResearchProgress
                        message={message}
                        onToggle={() => updateAssistant(message.id, (current) => ({
                          ...current,
                          reasoningOpen: !(current.reasoningOpen ?? !current.answerStarted),
                        }))}
                      />
                      {message.content && (
                        <MarkdownContent
                          content={citationMarkdown(
                            message.content,
                            message.citations ?? [],
                          )}
                        />
                      )}
                      {message.pending && !message.content && (
                        <div className="flex items-center gap-2 py-1 text-sm text-muted-foreground">
                          <span className="size-1.5 animate-pulse rounded-full bg-primary" />
                          {statusLabel(message.status)}
                        </div>
                      )}
                      {message.pending && message.content && (
                        <span className="mt-1 inline-block h-4 w-0.5 animate-pulse bg-primary align-middle" />
                      )}
                      {message.error && (
                        <div className="rounded-lg border border-destructive/25 bg-destructive/5 p-3 text-sm text-destructive">
                          <p>{message.error}</p>
                          <Button
                            variant="ghost"
                            size="xs"
                            className="mt-2 text-destructive hover:text-destructive"
                            onClick={() => retry(message)}
                            disabled={isRunning}
                          >
                            <RotateCcw data-icon="inline-start" />
                            Retry
                          </Button>
                        </div>
                      )}
                      {!!message.citations?.length && !message.error && (
                        <div className="mt-5 border-t border-border pt-3">
                          <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">
                            Sources
                          </p>
                          <div className="flex flex-col gap-1.5">
                            {message.citations.map((citation, index) => {
                              const date = publishedDate(citation.published_at)
                              return (
                                <a
                                  key={citation.article_id}
                                  href={citation.url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="group flex items-start gap-3 rounded-lg border border-border px-3 py-2.5 transition-colors hover:bg-muted/60"
                                >
                                  <span className="rounded bg-primary/12 px-1.5 py-0.5 font-mono text-xs font-medium text-primary">
                                    {index + 1}
                                  </span>
                                  <span className="min-w-0 flex-1">
                                    <span className="line-clamp-1 block text-sm font-medium group-hover:text-primary">
                                      {citation.title || citation.url}
                                    </span>
                                    <span className="mt-0.5 block text-xs text-muted-foreground">
                                      {[citation.source?.name, date].filter(Boolean).join(' · ')}
                                    </span>
                                  </span>
                                  <ExternalLink className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
                                </a>
                              )
                            })}
                          </div>
                        </div>
                      )}
                    </div>
                  </article>
                ),
              )}
            </div>
          )}
        </div>
      </div>

      <div className="shrink-0 border-t border-border bg-background/95 px-4 py-4 backdrop-blur sm:px-6">
        <div className="mx-auto w-full max-w-3xl">
          <div className="relative rounded-xl border border-input bg-card shadow-sm transition-shadow focus-within:ring-2 focus-within:ring-ring/35">
            <Textarea
              value={input}
              onChange={(event) => setInput(event.target.value.slice(0, 4000))}
              onKeyDown={(event) => {
                if (
                  event.key === 'Enter' &&
                  !event.shiftKey &&
                  !event.nativeEvent.isComposing
                ) {
                  event.preventDefault()
                  submit()
                }
              }}
              placeholder="Ask about the news…"
              aria-label="Message Ask AI"
              disabled={isRunning}
              rows={1}
              className="max-h-40 min-h-12 resize-none border-0 bg-transparent py-3 pl-3 pr-14 shadow-none focus-visible:ring-0 dark:bg-transparent"
            />
            {isRunning ? (
              <Button
                size="icon-sm"
                variant="secondary"
                onClick={stop}
                aria-label="Stop generating"
                className="absolute bottom-2 right-2"
              >
                <Square className="size-3 fill-current" />
              </Button>
            ) : (
              <Button
                size="icon-sm"
                onClick={submit}
                disabled={!input.trim()}
                aria-label="Send message"
                className="absolute bottom-2 right-2"
              >
                <ArrowUp />
              </Button>
            )}
          </div>
          <p className="mt-2 text-center text-xs text-muted-foreground">
            Answers are grounded in retrieved articles. Check cited sources for details.
          </p>
        </div>
      </div>
    </main>
  )
}
