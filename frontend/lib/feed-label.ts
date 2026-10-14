import type { FeedPriority } from './types'

export const FEED_TIERS: { value: FeedPriority | undefined; label: string }[] = [
  { value: undefined, label: 'All' },
  { value: 'instant', label: 'Instant' },
  { value: 'normal', label: 'Normal' },
]

export function parseTier(value: string | null | undefined): FeedPriority | undefined {
  return value === 'instant' || value === 'normal' ? value : undefined
}

// The feed's host as a reader would write it: `blog.cloudflare.com/rss/` ->
// `blog.cloudflare.com`, `www.` dropped. Subdomains stay, because they tell
// apart feeds that would otherwise collide (`*.substack.com`). Falls back to
// the raw text when the URL does not parse. The full URL is the tooltip.
export function feedHost(feedUrl: string): string {
  try {
    return new URL(feedUrl).hostname.toLowerCase().replace(/^www\./, '')
  } catch {
    return feedUrl.replace(/^[a-z][a-z0-9+.-]*:\/\//i, '').split('/')[0]
  }
}

export function feedHref(feedUrl: string, tier: FeedPriority | undefined): string {
  const sp = new URLSearchParams()
  sp.set('feed', feedUrl)
  if (tier) sp.set('tier', tier)
  return `/?${sp.toString()}`
}

export function tierHref(tier: FeedPriority | undefined): string {
  return tier ? `/?tier=${tier}` : '/'
}
