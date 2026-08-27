'use client'

import { useState } from 'react'
import Link from 'next/link'
import { AlertCircle, BookOpen, ExternalLink, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Empty,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
  EmptyDescription,
} from '@/components/ui/empty'
import { useStory } from '@/hooks/use-story'
import { useArticle } from '@/hooks/use-article'
import { ApiError } from '@/lib/client-api'
import { formatAbsolute, relativeTime } from '@/lib/time'
import { MarkdownContent } from '@/components/feed/markdown-content'

export function StoryDetailPanel({
  storyId,
  onArticleClick,
}: {
  storyId: number | null
  onArticleClick?: (articleId: number) => void
}) {
  const { story, isLoading, error, retry } = useStory(storyId)

  if (storyId === null) {
    return (
      <Empty className="h-full">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <BookOpen />
          </EmptyMedia>
          <EmptyTitle>Select a story</EmptyTitle>
          <EmptyDescription>
            Choose a story from the list to see every article covering it.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
    )
  }

  if (error) {
    const notFound = error instanceof ApiError && error.status === 404
    return (
      <Empty className="h-full">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <AlertCircle />
          </EmptyMedia>
          <EmptyTitle>{notFound ? 'Story not found' : 'Could not load story'}</EmptyTitle>
          <EmptyDescription>
            {notFound ? 'This story is no longer available.' : error.message}
          </EmptyDescription>
          {!notFound && (
            <Button variant="outline" size="sm" onClick={() => void retry()}>
              Try again
            </Button>
          )}
        </EmptyHeader>
      </Empty>
    )
  }

  if (isLoading || !story) {
    return (
      <div className="flex flex-col gap-4 px-6 py-8">
        <Skeleton className="h-4 w-32" />
        <Skeleton className="h-8 w-4/5" />
        <Skeleton className="h-4 w-full" />
        <Skeleton className="h-4 w-full" />
        <Skeleton className="h-4 w-3/5" />
      </div>
    )
  }

  return (
    <ScrollArea className="h-full">
      <div className="mx-auto flex max-w-2xl flex-col px-6 py-8">
        <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
          <span>
            {story.article_count} article{story.article_count === 1 ? '' : 's'} ·{' '}
            {story.source_count} source{story.source_count === 1 ? '' : 's'}
          </span>
          <span aria-hidden="true">·</span>
          <span>updated {relativeTime(story.last_seen_at)}</span>
        </div>

        <h1 className="mt-4 text-balance text-2xl font-semibold leading-tight tracking-tight">
          {story.headline ?? 'Untitled story'}
        </h1>

        <div className="mt-6 flex flex-col">
          {story.members.map((member) => (
            <div
              key={member.article_id}
              className="group flex flex-col gap-1 border-b border-border/60 py-3 first:pt-0"
            >
              <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                <span className="font-medium text-foreground/80">
                  {member.source?.name ?? 'Unknown source'}
                </span>
                {member.published_at && (
                  <>
                    <span aria-hidden="true">·</span>
                    <time dateTime={member.published_at}>{formatAbsolute(member.published_at)}</time>
                  </>
                )}
              </div>
              <div className="flex items-start justify-between gap-2">
                <button
                  type="button"
                  onClick={() => onArticleClick?.(member.article_id)}
                  className="flex-1 text-left"
                >
                  <h3 className="text-sm font-medium leading-snug text-foreground/90 group-hover:text-foreground">
                    {member.title ?? member.url}
                  </h3>
                </button>
                <a
                  href={member.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="shrink-0 pt-0.5"
                >
                  <ExternalLink className="size-3 text-muted-foreground hover:text-foreground" />
                </a>
              </div>
            </div>
          ))}
        </div>
      </div>
    </ScrollArea>
  )
}
