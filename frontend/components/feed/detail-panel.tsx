'use client'

import Link from 'next/link'
import { AlertCircle, ExternalLink, FileText, Maximize2 } from 'lucide-react'
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
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { MarkdownContent } from './markdown-content'
import { useArticle } from '@/hooks/use-article'
import { ApiError } from '@/lib/client-api'
import { formatAbsolute, relativeTime } from '@/lib/time'
import { TopicBadgeGroup } from '@/components/topics/topic-badge'

export function DetailPanel({ articleId }: { articleId: number | null }) {
  const { article, isLoading, error, retry } = useArticle(articleId)

  if (articleId === null) {
    return (
      <Empty className="h-full">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <FileText />
          </EmptyMedia>
          <EmptyTitle>Select an article</EmptyTitle>
          <EmptyDescription>
            Choose an article from the list to read its full content here.
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
          <EmptyTitle>{notFound ? 'Article not found' : 'Could not load article'}</EmptyTitle>
          <EmptyDescription>
            {notFound ? 'This article is no longer available.' : error.message}
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

  if (isLoading || !article) {
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
      <article className="mx-auto flex max-w-2xl flex-col px-6 py-8">
        <div className="flex items-center justify-between gap-3">
          <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
            <span className="font-medium text-foreground/80">
              {article.source?.name ?? 'Unknown source'}
            </span>
            {article.published_at && (
              <>
                <span aria-hidden="true">·</span>
                <time dateTime={article.published_at}>
                  {formatAbsolute(article.published_at)}
                </time>
              </>
            )}
            <span aria-hidden="true">·</span>
            <span>crawled {relativeTime(article.crawled_at)}</span>
          </div>

          <div className="flex shrink-0 items-center gap-1">
            <Tooltip>
              <TooltipTrigger render={<Link href={`/articles/${article.id}`} />}>
                <Button variant="ghost" size="icon" nativeButton={false} render={<span />}>
                  <Maximize2 />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Open standalone view</TooltipContent>
            </Tooltip>
            <Tooltip>
              <TooltipTrigger
                render={
                  <a href={article.url_original ?? article.url} target="_blank" rel="noopener noreferrer" />
                }
              >
                <Button variant="ghost" size="icon" nativeButton={false} render={<span />}>
                  <ExternalLink />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Open original source</TooltipContent>
            </Tooltip>
          </div>
        </div>

        <h1 className="mt-4 text-balance text-2xl font-semibold leading-tight tracking-tight">
          {article.title ?? article.url}
        </h1>

        <TopicBadgeGroup topics={article.topics} className="mt-3" />

        <div className="mt-6">
          <MarkdownContent content={article.content} />
        </div>
      </article>
    </ScrollArea>
  )
}
