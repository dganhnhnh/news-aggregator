'use client'

import { FormEvent, useMemo, useState } from 'react'
import {
  CheckCircle2,
  ExternalLink,
  ListChecks,
  LoaderCircle,
  Rss,
  Search,
} from 'lucide-react'
import { toast } from 'sonner'
import { apiRequest, ApiError } from '@/lib/client-api'
import type {
  FeedBatchCreateResponse,
  FeedDiscoveryCandidate,
  FeedDiscoveryResponse,
  FeedPriority,
} from '@/lib/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from '@/components/ui/empty'
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

type CustomAddFeedsProps = {
  onFeedsAdded: () => Promise<unknown> | void
}

export function CustomAddFeeds({ onFeedsAdded }: CustomAddFeedsProps) {
  const [query, setQuery] = useState('')
  const [result, setResult] = useState<FeedDiscoveryResponse | null>(null)
  const [selectedUrls, setSelectedUrls] = useState<Set<string>>(new Set())
  const [priority, setPriority] = useState<FeedPriority>('normal')
  const [error, setError] = useState<string | null>(null)
  const [isSearching, setIsSearching] = useState(false)
  const [isApplying, setIsApplying] = useState(false)

  const availableCandidates = useMemo(
    () => result?.candidates.filter((candidate) => !candidate.subscribed) ?? [],
    [result],
  )
  const allAvailableSelected =
    availableCandidates.length > 0 &&
    availableCandidates.every((candidate) => selectedUrls.has(candidate.feed_url))

  async function searchFeeds(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)
    setIsSearching(true)
    setSelectedUrls(new Set())
    try {
      const discovered = await apiRequest<FeedDiscoveryResponse>(
        '/api/admin/feeds/discover',
        'POST',
        { query },
      )
      setResult(discovered)
    } catch (requestError) {
      setResult(null)
      setError(
        requestError instanceof ApiError
          ? requestError.message
          : 'Could not search for feeds',
      )
    } finally {
      setIsSearching(false)
    }
  }

  function setSelected(feedUrl: string, checked: boolean) {
    setSelectedUrls((current) => {
      const next = new Set(current)
      if (checked) next.add(feedUrl)
      else next.delete(feedUrl)
      return next
    })
  }

  function toggleAll(checked: boolean) {
    setSelectedUrls(
      checked
        ? new Set(availableCandidates.map((candidate) => candidate.feed_url))
        : new Set(),
    )
  }

  async function subscribeSelected() {
    if (!result || selectedUrls.size === 0) return
    const selected = result.candidates.filter((candidate) =>
      selectedUrls.has(candidate.feed_url),
    )
    setError(null)
    setIsApplying(true)
    try {
      const response = await apiRequest<FeedBatchCreateResponse>(
        '/api/admin/feeds/batch',
        'POST',
        {
          priority,
          feeds: selected.map((candidate) => ({
            feed_url: candidate.feed_url,
            source_label: candidate.source_label,
          })),
        },
      )
      setResult((current) =>
        current
          ? {
              ...current,
              candidates: current.candidates.map((candidate) =>
                selectedUrls.has(candidate.feed_url)
                  ? { ...candidate, subscribed: true }
                  : candidate,
              ),
            }
          : current,
      )
      setSelectedUrls(new Set())
      await onFeedsAdded()
      const skipped = response.skipped_count
        ? ` ${response.skipped_count} already existed.`
        : ''
      toast.success(`${response.created_count} feed(s) subscribed.${skipped}`)
    } catch (requestError) {
      const message =
        requestError instanceof ApiError
          ? requestError.message
          : 'Could not subscribe to selected feeds'
      setError(message)
      toast.error(message)
    } finally {
      setIsApplying(false)
    }
  }

  return (
    <div className="flex flex-col gap-5">
      <section className="rounded-xl border border-border bg-card p-4 sm:p-5">
        <form
          onSubmit={searchFeeds}
          className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-end"
        >
          <label className="flex min-w-0 flex-col gap-1.5 text-sm font-medium">
            Keyword or query
            <Input
              required
              value={query}
              onChange={(event) => {
                setQuery(event.target.value)
                setError(null)
              }}
              placeholder="e.g. artificial intelligence, climate technology"
              disabled={isSearching || isApplying}
              aria-invalid={Boolean(error)}
            />
          </label>
          <Button type="submit" disabled={isSearching || isApplying || !query.trim()}>
            {isSearching ? <LoaderCircle className="animate-spin" /> : <Search />}
            Find feeds
          </Button>
        </form>
        <div className="mt-2 min-h-5 text-sm">
          {error ? (
            <p className="text-destructive" role="alert">{error}</p>
          ) : (
            <p className="text-muted-foreground">
              Searches Google News for relevant publishers, then validates their RSS or Atom feeds.
            </p>
          )}
        </div>
      </section>

      {result && (
        <section className="flex min-h-0 flex-col gap-3">
          <div className="flex flex-col gap-3 rounded-xl border border-border bg-card p-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex min-w-0 items-center gap-3">
              <Checkbox
                checked={allAvailableSelected}
                onCheckedChange={toggleAll}
                disabled={availableCandidates.length === 0 || isApplying}
                aria-label="Select all available feeds"
              />
              <div className="min-w-0 text-sm">
                <p className="font-medium">
                  {result.candidates.length} feed candidate(s)
                </p>
                <p className="truncate text-xs text-muted-foreground">
                  From {result.publishers_considered} publishers and {result.articles_considered} articles
                </p>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <Select
                value={priority}
                onValueChange={(value) => setPriority(value as FeedPriority)}
                disabled={isApplying}
              >
                <SelectTrigger className="w-28"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectGroup>
                    <SelectItem value="normal">Normal</SelectItem>
                    <SelectItem value="instant">Instant</SelectItem>
                  </SelectGroup>
                </SelectContent>
              </Select>
              <Button
                onClick={() => void subscribeSelected()}
                disabled={selectedUrls.size === 0 || isApplying}
              >
                {isApplying ? <LoaderCircle className="animate-spin" /> : <ListChecks />}
                Subscribe {selectedUrls.size || ''}
              </Button>
            </div>
          </div>

          {result.candidates.length === 0 ? (
            <Empty className="min-h-64 border">
              <EmptyHeader>
                <EmptyMedia variant="icon"><Rss /></EmptyMedia>
                <EmptyTitle>No valid feeds found</EmptyTitle>
                <EmptyDescription>
                  Google News found publishers, but none exposed a valid RSS or Atom feed.
                </EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <div className="flex flex-col overflow-hidden rounded-xl border border-border bg-card">
              {result.candidates.map((candidate) => (
                <CandidateRow
                  key={candidate.feed_url}
                  candidate={candidate}
                  checked={selectedUrls.has(candidate.feed_url)}
                  disabled={isApplying}
                  onCheckedChange={(checked) => setSelected(candidate.feed_url, checked)}
                />
              ))}
            </div>
          )}
        </section>
      )}
    </div>
  )
}

function CandidateRow({
  candidate,
  checked,
  disabled,
  onCheckedChange,
}: {
  candidate: FeedDiscoveryCandidate
  checked: boolean
  disabled: boolean
  onCheckedChange: (checked: boolean) => void
}) {
  return (
    <article className="grid gap-3 border-b border-border p-4 last:border-b-0 sm:grid-cols-[auto_minmax(0,1fr)_auto] sm:items-start">
      <Checkbox
        className="mt-0.5"
        checked={candidate.subscribed || checked}
        onCheckedChange={onCheckedChange}
        disabled={candidate.subscribed || disabled}
        aria-label={`Select ${candidate.source_label}`}
      />

      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-sm font-semibold">{candidate.source_label}</h2>
          {candidate.subscribed ? (
            <Badge variant="secondary"><CheckCircle2 />Subscribed</Badge>
          ) : (
            <Badge variant="outline">{candidate.matched_articles} matching article(s)</Badge>
          )}
        </div>
        <a
          href={candidate.feed_url}
          target="_blank"
          rel="noreferrer"
          className="mt-1 flex min-w-0 items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
          title={candidate.feed_url}
        >
          <span className="truncate">{candidate.feed_url}</span>
          <ExternalLink className="size-3.5 shrink-0" />
        </a>
        {candidate.sample_titles[0] && (
          <p className="mt-2 line-clamp-1 text-xs text-muted-foreground">
            Example: {candidate.sample_titles[0]}
          </p>
        )}
      </div>

      <a
        href={candidate.website_url}
        target="_blank"
        rel="noreferrer"
        className="text-xs text-muted-foreground hover:text-foreground sm:pt-1"
      >
        Publisher <ExternalLink className="ml-1 inline size-3" />
      </a>
    </article>
  )
}
