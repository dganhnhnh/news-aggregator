'use client'

import { FormEvent, useMemo, useState } from 'react'
import {
  AlertCircle,
  Clock3,
  ExternalLink,
  LoaderCircle,
  Power,
  PowerOff,
  Rss,
} from 'lucide-react'
import { toast } from 'sonner'
import { apiRequest, ApiError } from '@/lib/client-api'
import type { FeedPriority, ManagedFeed } from '@/lib/types'
import { formatAbsolute, relativeTime } from '@/lib/time'
import { useFeeds } from '@/hooks/use-feeds'
import { useMinuteNow } from '@/hooks/use-minute-now'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  Empty,
  EmptyContent,
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
import { Skeleton } from '@/components/ui/skeleton'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'

type FeedTab = 'active' | 'inactive'

export function FeedManagementView() {
  const { feeds, error, isLoading, refresh } = useFeeds()
  const [tab, setTab] = useState<FeedTab>('active')
  const [feedUrl, setFeedUrl] = useState('')
  const [sourceLabel, setSourceLabel] = useState('')
  const [priority, setPriority] = useState<FeedPriority>('normal')
  const [formError, setFormError] = useState<string | null>(null)
  const [isAdding, setIsAdding] = useState(false)
  const [pendingUrl, setPendingUrl] = useState<string | null>(null)
  const [disableTarget, setDisableTarget] = useState<ManagedFeed | null>(null)

  const activeFeeds = useMemo(() => feeds.filter((feed) => feed.active), [feeds])
  const inactiveFeeds = useMemo(() => feeds.filter((feed) => !feed.active), [feeds])

  async function addFeed(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setFormError(null)
    setIsAdding(true)
    try {
      await apiRequest<ManagedFeed>('/api/admin/feeds', 'POST', {
        feed_url: feedUrl,
        source_label: sourceLabel,
        priority,
      })
      setFeedUrl('')
      setSourceLabel('')
      setPriority('normal')
      setTab('active')
      await refresh()
      toast.success('Feed added')
    } catch (requestError) {
      const message =
        requestError instanceof ApiError ? requestError.message : 'Could not add feed'
      setFormError(message)
    } finally {
      setIsAdding(false)
    }
  }

  async function updateFeed(feedUrlToUpdate: string, change: object, message: string) {
    setPendingUrl(feedUrlToUpdate)
    try {
      await apiRequest<ManagedFeed>('/api/admin/feeds', 'PATCH', {
        feed_url: feedUrlToUpdate,
        ...change,
      })
      await refresh()
      toast.success(message)
    } catch (requestError) {
      toast.error(
        requestError instanceof ApiError ? requestError.message : 'Could not update feed',
      )
    } finally {
      setPendingUrl(null)
    }
  }

  async function disableFeed() {
    if (!disableTarget) return
    const target = disableTarget
    setDisableTarget(null)
    await updateFeed(target.feed_url, { active: false }, 'Feed disabled')
  }

  return (
    <main className="flex min-w-0 flex-1 flex-col overflow-hidden">
      <header className="border-b border-border px-4 py-3 sm:px-6">
        <div className="mx-auto flex w-full max-w-6xl items-center gap-3">
          <span className="flex size-8 items-center justify-center rounded-lg bg-primary/15 text-primary">
            <Rss className="size-4" />
          </span>
          <div>
            <h1 className="text-base font-semibold">Feed management</h1>
            <p className="text-sm text-muted-foreground">Add feeds and control their crawl tier.</p>
          </div>
        </div>
      </header>

      <div className="scrollbar-thin flex-1 overflow-y-auto">
        <div className="mx-auto flex w-full max-w-6xl flex-col gap-6 px-4 py-5 sm:px-6 sm:py-7">
          <section className="rounded-xl border border-border bg-card p-4 sm:p-5">
            <form onSubmit={addFeed} className="grid gap-4 lg:grid-cols-[minmax(18rem,2fr)_minmax(12rem,1fr)_10rem_auto] lg:items-end">
              <label className="flex min-w-0 flex-col gap-1.5 text-sm font-medium">
                Feed URL
                <Input
                  type="url"
                  required
                  value={feedUrl}
                  onChange={(event) => {
                    setFeedUrl(event.target.value)
                    setFormError(null)
                  }}
                  placeholder="https://example.com/feed.xml"
                  disabled={isAdding}
                  aria-invalid={Boolean(formError)}
                />
              </label>

              <label className="flex min-w-0 flex-col gap-1.5 text-sm font-medium">
                Source label
                <Input
                  value={sourceLabel}
                  onChange={(event) => setSourceLabel(event.target.value)}
                  placeholder="Inferred from domain"
                  disabled={isAdding}
                />
              </label>

              <label className="flex flex-col gap-1.5 text-sm font-medium">
                Priority
                <Select
                  value={priority}
                  onValueChange={(value) => setPriority(value as FeedPriority)}
                  disabled={isAdding}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectGroup>
                      <SelectItem value="normal">Normal</SelectItem>
                      <SelectItem value="instant">Instant</SelectItem>
                    </SelectGroup>
                  </SelectContent>
                </Select>
              </label>

              <Button type="submit" disabled={isAdding} className="w-full lg:w-auto">
                {isAdding && <LoaderCircle className="animate-spin" />}
                Add feed
              </Button>
            </form>
            <div className="mt-2 min-h-5 text-sm">
              {formError ? (
                <p className="text-destructive" role="alert">{formError}</p>
              ) : (
                <p className="text-muted-foreground">
                  Leave the source label blank to use the feed&apos;s registrable domain.
                </p>
              )}
            </div>
          </section>

          <section className="min-h-0">
            <Tabs value={tab} onValueChange={(value) => setTab(value as FeedTab)}>
              <TabsList>
                <TabsTrigger value="active">Active {activeFeeds.length}</TabsTrigger>
                <TabsTrigger value="inactive">Inactive {inactiveFeeds.length}</TabsTrigger>
              </TabsList>
              <TabsContent value="active" className="mt-2">
                <FeedList
                  feeds={activeFeeds}
                  state="active"
                  isLoading={isLoading}
                  error={error}
                  pendingUrl={pendingUrl}
                  onRetry={() => void refresh()}
                  onPriorityChange={(feed, nextPriority) =>
                    void updateFeed(
                      feed.feed_url,
                      { priority: nextPriority },
                      `Priority changed to ${nextPriority}`,
                    )
                  }
                  onToggle={(feed) => setDisableTarget(feed)}
                />
              </TabsContent>
              <TabsContent value="inactive" className="mt-2">
                <FeedList
                  feeds={inactiveFeeds}
                  state="inactive"
                  isLoading={isLoading}
                  error={error}
                  pendingUrl={pendingUrl}
                  onRetry={() => void refresh()}
                  onPriorityChange={(feed, nextPriority) =>
                    void updateFeed(
                      feed.feed_url,
                      { priority: nextPriority },
                      `Priority changed to ${nextPriority}`,
                    )
                  }
                  onToggle={(feed) =>
                    void updateFeed(feed.feed_url, { active: true }, 'Feed enabled')
                  }
                />
              </TabsContent>
            </Tabs>
          </section>
        </div>
      </div>

      <Dialog open={Boolean(disableTarget)} onOpenChange={(open) => !open && setDisableTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Disable feed?</DialogTitle>
            <DialogDescription>
              {disableTarget?.source_label || disableTarget?.feed_url} will stop being selected for crawl until it is enabled again.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <DialogClose render={<Button variant="outline" />}>Cancel</DialogClose>
            <Button variant="destructive" onClick={() => void disableFeed()}>
              Disable feed
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </main>
  )
}

type FeedListProps = {
  feeds: ManagedFeed[]
  state: FeedTab
  isLoading: boolean
  error: unknown
  pendingUrl: string | null
  onRetry: () => void
  onPriorityChange: (feed: ManagedFeed, priority: FeedPriority) => void
  onToggle: (feed: ManagedFeed) => void
}

function FeedList({
  feeds,
  state,
  isLoading,
  error,
  pendingUrl,
  onRetry,
  onPriorityChange,
  onToggle,
}: FeedListProps) {
  if (isLoading) {
    return (
      <div className="flex flex-col gap-2">
        {Array.from({ length: 4 }).map((_, index) => (
          <Skeleton key={index} className="h-32 w-full rounded-xl" />
        ))}
      </div>
    )
  }

  if (error) {
    return (
      <Empty className="min-h-64 border">
        <EmptyHeader>
          <EmptyMedia variant="icon"><AlertCircle /></EmptyMedia>
          <EmptyTitle>Feeds unavailable</EmptyTitle>
          <EmptyDescription>The admin service could not be reached.</EmptyDescription>
        </EmptyHeader>
        <EmptyContent><Button variant="outline" onClick={onRetry}>Retry</Button></EmptyContent>
      </Empty>
    )
  }

  if (feeds.length === 0) {
    return (
      <Empty className="min-h-64 border">
        <EmptyHeader>
          <EmptyMedia variant="icon">{state === 'active' ? <Rss /> : <PowerOff />}</EmptyMedia>
          <EmptyTitle>No {state} feeds</EmptyTitle>
          <EmptyDescription>
            {state === 'active'
              ? 'Add a feed above to make it available to the crawl workers.'
              : 'Feeds you disable will appear here.'}
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    )
  }

  return (
    <div className="flex flex-col overflow-hidden rounded-xl border border-border bg-card">
      {feeds.map((feed) => (
        <FeedRow
          key={feed.feed_url}
          feed={feed}
          pending={pendingUrl === feed.feed_url}
          onPriorityChange={onPriorityChange}
          onToggle={onToggle}
        />
      ))}
    </div>
  )
}

function FeedRow({
  feed,
  pending,
  onPriorityChange,
  onToggle,
}: {
  feed: ManagedFeed
  pending: boolean
  onPriorityChange: (feed: ManagedFeed, priority: FeedPriority) => void
  onToggle: (feed: ManagedFeed) => void
}) {
  const now = useMinuteNow()
  const lastPoll = feed.last_polled_at ? relativeTime(feed.last_polled_at, now) : 'Never polled'
  const nextPoll = relativeTime(feed.next_poll_at, now)

  return (
    <article className="grid gap-4 border-b border-border p-4 last:border-b-0 md:grid-cols-[minmax(0,1.6fr)_minmax(13rem,1fr)_auto] md:items-center">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="truncate text-sm font-semibold">{feed.source_label || 'Unlabeled feed'}</h2>
          <Badge variant={feed.active ? 'secondary' : 'outline'}>
            {feed.active ? 'Active' : 'Inactive'}
          </Badge>
          {feed.consecutive_failures > 0 && (
            <Badge variant="destructive">{feed.consecutive_failures} failures</Badge>
          )}
        </div>
        <a
          href={feed.feed_url}
          target="_blank"
          rel="noreferrer"
          className="mt-1 flex min-w-0 items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
          title={feed.feed_url}
        >
          <span className="truncate">{feed.feed_url}</span>
          <ExternalLink className="size-3.5 shrink-0" />
        </a>
        {!feed.active && feed.disabled_reason && (
          <p className="mt-1 truncate text-xs text-muted-foreground" title={feed.disabled_reason}>
            {feed.disabled_reason}
          </p>
        )}
      </div>

      <div className="grid grid-cols-2 gap-3 text-sm text-muted-foreground">
        <div className="min-w-0">
          <span className="flex items-center gap-1 text-xs"><Clock3 className="size-3" />Last poll</span>
          <span className="mt-0.5 block truncate text-foreground" title={feed.last_polled_at ? formatAbsolute(feed.last_polled_at) : undefined}>
            {lastPoll}
          </span>
        </div>
        <div className="min-w-0">
          <span className="flex items-center gap-1 text-xs"><Clock3 className="size-3" />Next poll</span>
          <span className="mt-0.5 block truncate text-foreground" title={formatAbsolute(feed.next_poll_at)}>
            {nextPoll}
          </span>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2 md:justify-end">
        <Select
          value={feed.priority}
          onValueChange={(value) => onPriorityChange(feed, value as FeedPriority)}
          disabled={pending}
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
          variant={feed.active ? 'outline' : 'secondary'}
          onClick={() => onToggle(feed)}
          disabled={pending}
        >
          {pending ? <LoaderCircle className="animate-spin" /> : feed.active ? <PowerOff /> : <Power />}
          {feed.active ? 'Disable' : 'Enable'}
        </Button>
      </div>
    </article>
  )
}
