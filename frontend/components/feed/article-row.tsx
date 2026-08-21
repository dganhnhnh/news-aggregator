'use client'

import { ExternalLink } from 'lucide-react'
import { cn } from '@/lib/utils'
import { relativeTime } from '@/lib/time'
import type { ArticleSummary } from '@/lib/types'

function initialsFor(name: string) {
  return name.slice(0, 1).toUpperCase()
}

function hueFor(seed: string) {
  let hash = 0
  for (let i = 0; i < seed.length; i++) hash = (hash * 31 + seed.charCodeAt(i)) % 360
  return hash
}

export function ArticleRow({
  article,
  selected,
  onSelect,
}: {
  article: ArticleSummary
  selected: boolean
  onSelect: () => void
}) {
  const sourceName = article.source?.name ?? 'Unknown source'
  const hue = hueFor(article.source?.slug ?? sourceName)

  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        'group flex w-full flex-col gap-1.5 border-b border-border/60 px-4 py-3 text-left transition-colors',
        selected ? 'bg-accent' : 'hover:bg-accent/50',
      )}
    >
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        <span
          className="flex size-4 shrink-0 items-center justify-center rounded-full text-[9px] font-semibold text-white"
          style={{ backgroundColor: `oklch(0.55 0.13 ${hue})` }}
          aria-hidden="true"
        >
          {initialsFor(sourceName)}
        </span>
        <span className="truncate font-medium text-foreground/80">{sourceName}</span>
        <span aria-hidden="true">·</span>
        <time dateTime={article.event_at} className="shrink-0 tabular-nums">
          {relativeTime(article.event_at)}
        </time>
      </div>

      <h3
        className={cn(
          'text-[13.5px] font-semibold leading-snug text-balance',
          selected ? 'text-foreground' : 'text-foreground/90',
        )}
      >
        {article.title ?? (
          <span className="inline-flex items-center gap-1 text-muted-foreground">
            <ExternalLink className="size-3" />
            {article.url}
          </span>
        )}
      </h3>

      <p className="line-clamp-2 text-xs leading-relaxed text-muted-foreground">
        {article.excerpt}
      </p>
    </button>
  )
}
