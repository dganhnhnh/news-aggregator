'use client'

import { cn } from '@/lib/utils'
import { relativeTime } from '@/lib/time'
import type { StorySummary } from '@/lib/types'
import { TopicBadgeGroup } from '@/components/topics/topic-badge'

function initialsFor(name: string) {
  return name.slice(0, 1).toUpperCase()
}

function hueFor(seed: string) {
  let hash = 0
  for (let i = 0; i < seed.length; i++) hash = (hash * 31 + seed.charCodeAt(i)) % 360
  return hash
}

export function StoryRow({
  story,
  selected,
  onSelect,
}: {
  story: StorySummary
  selected: boolean
  onSelect: () => void
}) {
  const primarySource = story.sources[0]
  const sourceName = primarySource?.name ?? 'Unknown source'
  const hue = hueFor(primarySource?.slug ?? sourceName)

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
        <span className="truncate font-medium text-foreground/80">
          {sourceName}
          {story.source_count > 1 && ` +${story.source_count - 1}`}
        </span>
        <span aria-hidden="true">·</span>
        <time dateTime={story.last_seen_at} className="shrink-0 tabular-nums">
          {relativeTime(story.last_seen_at)}
        </time>
      </div>

      <h3
        className={cn(
          'text-[13.5px] font-semibold leading-snug text-balance',
          selected ? 'text-foreground' : 'text-foreground/90',
        )}
      >
        {story.headline ?? 'Untitled story'}
      </h3>

      <TopicBadgeGroup topics={story.topics} maxVisible={2} />

      <p className="text-xs leading-relaxed text-muted-foreground">
        {story.article_count} article{story.article_count === 1 ? '' : 's'} · {story.source_count}{' '}
        source{story.source_count === 1 ? '' : 's'}
      </p>
    </button>
  )
}
