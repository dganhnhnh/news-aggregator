'use client'

import { ExternalLink } from 'lucide-react'
import { cn } from '@/lib/utils'
import { relativeTime } from '@/lib/time'
import type { ArticleSummary } from '@/lib/types'
import { TopicBadgeGroup } from '@/components/topics/topic-badge'
import { HighlightedText } from '@/components/highlighted-text'

type ArticleRowProps = {
  article: ArticleSummary
  selected: boolean
} & ({ onSelect: () => void; href?: never } | { href: string; onSelect?: never })

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
  href,
}: ArticleRowProps) {
  const sourceName = article.source?.name ?? 'Unknown source'
  const hue = hueFor(article.source?.slug ?? sourceName)
  const rowClassName = cn(
    'group flex w-full flex-col gap-1.5 border-b border-border/60 px-4 py-3 text-left transition-colors',
    selected ? 'bg-accent' : 'hover:bg-accent/50',
  )

  const content = (
    <>
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
        {article.title ? (
          <HighlightedText segments={article.title_highlights} fallback={article.title} />
        ) : (
          <span className="inline-flex items-center gap-1 text-muted-foreground">
            <ExternalLink className="size-3" />
            {article.url}
          </span>
        )}
      </h3>

      <TopicBadgeGroup topics={article.topics} maxVisible={2} />

      <p className="line-clamp-2 text-xs leading-relaxed text-muted-foreground">
        <HighlightedText segments={article.excerpt_highlights} fallback={article.excerpt} />
      </p>
    </>
  )

  return href ? (
    <a href={href} target="_blank" rel="noopener noreferrer" className={rowClassName}>
      {content}
    </a>
  ) : (
    <button type="button" onClick={onSelect} className={rowClassName}>
      {content}
    </button>
  )
}
