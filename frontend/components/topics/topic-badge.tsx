import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import type { TopicRef } from '@/lib/types'

const TOPIC_COLORS = [
  'border-red-300/70 bg-red-100 text-red-800 dark:border-red-800 dark:bg-red-950/60 dark:text-red-200',
  'border-orange-300/70 bg-orange-100 text-orange-800 dark:border-orange-800 dark:bg-orange-950/60 dark:text-orange-200',
  'border-amber-300/70 bg-amber-100 text-amber-900 dark:border-amber-800 dark:bg-amber-950/60 dark:text-amber-200',
  'border-lime-300/70 bg-lime-100 text-lime-900 dark:border-lime-800 dark:bg-lime-950/60 dark:text-lime-200',
  'border-emerald-300/70 bg-emerald-100 text-emerald-800 dark:border-emerald-800 dark:bg-emerald-950/60 dark:text-emerald-200',
  'border-cyan-300/70 bg-cyan-100 text-cyan-900 dark:border-cyan-800 dark:bg-cyan-950/60 dark:text-cyan-200',
  'border-sky-300/70 bg-sky-100 text-sky-800 dark:border-sky-800 dark:bg-sky-950/60 dark:text-sky-200',
  'border-blue-300/70 bg-blue-100 text-blue-800 dark:border-blue-800 dark:bg-blue-950/60 dark:text-blue-200',
  'border-violet-300/70 bg-violet-100 text-violet-800 dark:border-violet-800 dark:bg-violet-950/60 dark:text-violet-200',
  'border-fuchsia-300/70 bg-fuchsia-100 text-fuchsia-800 dark:border-fuchsia-800 dark:bg-fuchsia-950/60 dark:text-fuchsia-200',
  'border-pink-300/70 bg-pink-100 text-pink-800 dark:border-pink-800 dark:bg-pink-950/60 dark:text-pink-200',
  'border-rose-300/70 bg-rose-100 text-rose-800 dark:border-rose-800 dark:bg-rose-950/60 dark:text-rose-200',
  'border-yellow-300/70 bg-yellow-100 text-yellow-900 dark:border-yellow-800 dark:bg-yellow-950/60 dark:text-yellow-200',
  'border-green-300/70 bg-green-100 text-green-900 dark:border-green-800 dark:bg-green-950/60 dark:text-green-200',
  'border-teal-300/70 bg-teal-100 text-teal-900 dark:border-teal-800 dark:bg-teal-950/60 dark:text-teal-200',
  'border-purple-300/70 bg-purple-100 text-purple-800 dark:border-purple-800 dark:bg-purple-950/60 dark:text-purple-200',
] as const

function colorIndex(slug: string): number {
  let hash = 0
  for (let index = 0; index < slug.length; index += 1) {
    hash = (hash * 31 + slug.charCodeAt(index)) >>> 0
  }
  return hash % TOPIC_COLORS.length
}

export function TopicBadge({ topic, className }: { topic: TopicRef; className?: string }) {
  return (
    <Badge
      variant="outline"
      className={cn('h-5 max-w-full px-2 text-[10px] leading-none', TOPIC_COLORS[colorIndex(topic.slug)], className)}
      title={topic.name}
    >
      <span className="truncate">{topic.name}</span>
    </Badge>
  )
}

export function TopicBadgeGroup({
  topics,
  maxVisible,
  className,
}: {
  topics: TopicRef[]
  maxVisible?: number
  className?: string
}) {
  if (topics.length === 0) return null
  const visible = maxVisible ? topics.slice(0, maxVisible) : topics
  const hidden = topics.slice(visible.length)

  return (
    <div className={cn('flex min-w-0 flex-wrap items-center gap-1', className)} aria-label="Topics">
      {visible.map((topic) => (
        <TopicBadge key={topic.slug} topic={topic} />
      ))}
      {hidden.length > 0 && (
        <Badge
          variant="secondary"
          className="h-5 px-1.5 text-[10px]"
          title={hidden.map((topic) => topic.name).join(', ')}
          aria-label={`${hidden.length} more topics: ${hidden.map((topic) => topic.name).join(', ')}`}
        >
          +{hidden.length}
        </Badge>
      )}
    </div>
  )
}
