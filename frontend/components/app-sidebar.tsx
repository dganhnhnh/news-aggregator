'use client'

import { Fragment, Suspense, useMemo } from 'react'
import Link from 'next/link'
import { usePathname, useSearchParams } from 'next/navigation'
import { Radio } from 'lucide-react'
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from '@/components/ui/sidebar'
import { useFeedSources } from '@/hooks/use-feed-sources'
import { FEED_TIERS, feedHost, feedHref, parseTier, tierHref } from '@/lib/feed-label'
import { cn } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'

function feedIndex(display: string): string {
  const first = display.charAt(0).toUpperCase()
  return /^[A-Z]$/.test(first) ? first : '#'
}

// `useSearchParams` opts the tree out of static rendering unless it sits under
// a Suspense boundary, and three pages mount this sidebar.
export function AppSidebar() {
  return (
    <Suspense fallback={null}>
      <SidebarBody />
    </Suspense>
  )
}

function SidebarBody() {
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const tier = parseTier(searchParams.get('tier'))
  const activeFeed = pathname === '/' ? searchParams.get('feed') : null
  const { feeds, isLoading, error } = useFeedSources({ tier })
  const feedGroups = useMemo(() => {
    const groups = new Map<string, { feed: (typeof feeds)[number]; display: string }[]>()
    const sortedFeeds = feeds
      .map((feed) => ({ feed, display: feedHost(feed.feed_url) }))
      .sort(
        (a, b) =>
          a.display.localeCompare(b.display, undefined, { sensitivity: 'base', numeric: true }) ||
          a.feed.feed_url.localeCompare(b.feed.feed_url),
      )

    for (const entry of sortedFeeds) {
      const index = feedIndex(entry.display)
      const group = groups.get(index) ?? []
      group.push(entry)
      groups.set(index, group)
    }

    return Array.from(groups.entries())
  }, [feeds])

  return (
    <Sidebar collapsible="icon" className="data-[side=left]:left-14">
      <SidebarHeader className="border-b border-sidebar-border">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" render={<Link href="/" />}>
              <div className="flex flex-col leading-tight">
                <span className="font-semibold tracking-tight">News</span>
                <span className="text-xs text-muted-foreground">
                  Aggregated feed
                </span>
              </div>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Sources</SidebarGroupLabel>
          <SidebarGroupContent>
            <div
              role="group"
              aria-label="Feed tier"
              className="mb-2 flex gap-0.5 rounded-md bg-sidebar-accent/50 p-0.5 group-data-[collapsible=icon]:hidden"
            >
              {FEED_TIERS.map((option) => (
                <Link
                  key={option.label}
                  href={tierHref(option.value)}
                  aria-current={tier === option.value ? 'true' : undefined}
                  className={cn(
                    'flex-1 rounded px-2 py-1 text-center text-xs font-medium text-sidebar-foreground/70 transition-colors hover:text-sidebar-foreground',
                    tier === option.value && 'bg-sidebar text-sidebar-foreground shadow-sm',
                  )}
                >
                  {option.label}
                </Link>
              ))}
            </div>
            <SidebarMenu>
              {isLoading &&
                Array.from({ length: 8 }).map((_, i) => (
                  <SidebarMenuItem key={i}>
                    <div className="flex items-center gap-2 px-2 py-1.5">
                      <Skeleton className="size-4 shrink-0 rounded-full" />
                      <Skeleton className="h-3.5 w-full" />
                    </div>
                  </SidebarMenuItem>
                ))}
              {error && !isLoading && (
                <p className="px-2 py-1.5 text-xs text-destructive">Feeds unavailable</p>
              )}
              {!isLoading && !error && feeds.length === 0 && (
                <p className="px-2 py-1.5 text-xs text-muted-foreground">No feeds in this tier</p>
              )}
              {feedGroups.map(([index, group], groupIndex) => (
                <Fragment key={index}>
                  <SidebarMenuItem
                    className={`px-2 pb-1 text-[10px] font-semibold uppercase tracking-widest text-sidebar-foreground/45 group-data-[collapsible=icon]:hidden ${groupIndex === 0 ? 'pt-1' : 'pt-3'}`}
                  >
                    {index}
                  </SidebarMenuItem>
                  {group.map(({ feed, display }) => (
                    <SidebarMenuItem key={feed.feed_url}>
                      <SidebarMenuButton
                        render={<Link href={feedHref(feed.feed_url, tier)} title={feed.feed_url} />}
                        isActive={activeFeed === feed.feed_url}
                        tooltip={feed.feed_url}
                      >
                        <Radio />
                        <span className="truncate">{display}</span>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  ))}
                </Fragment>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="border-t border-sidebar-border">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton render={<Link href="/sources" />} isActive={pathname === '/sources'}>
              <span className="text-xs text-muted-foreground">
                Manage sources
              </span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
    </Sidebar>
  )
}
