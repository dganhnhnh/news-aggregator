'use client'

import { AlertCircle, ExternalLink } from 'lucide-react'
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
import { MarkdownContent } from '@/components/feed/markdown-content'
import { useArticle } from '@/hooks/use-article'
import { ApiError } from '@/lib/client-api'
import { formatAbsolute, relativeTime } from '@/lib/time'

export function ArticlePreviewPanel({ articleId }: { articleId: number | null }) {
  const { article, isLoading, error, retry } = useArticle(articleId)

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
      <div className="flex flex-col gap-4 px-4 py-6">
        <Skeleton className="h-4 w-32" />
        <Skeleton className="h-6 w-4/5" />
        <Skeleton className="h-4 w-full" />
        <Skeleton className="h-4 w-full" />
        <Skeleton className="h-4 w-3/5" />
      </div>
    )
  }

  return (
    <ScrollArea className="h-full">
      <article className="flex flex-col px-4 py-6">
        <div className="flex items-center justify-between gap-2">
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <span className="font-medium text-foreground/80">
              {article.source?.name ?? 'Unknown source'}
            </span>
            {article.published_at && (
              <>
                <span aria-hidden="true">·</span>
                <time dateTime={article.published_at}>{formatAbsolute(article.published_at)}</time>
              </>
            )}
          </div>
          <a
            href={article.url_original ?? article.url}
            target="_blank"
            rel="noopener noreferrer"
            className="shrink-0"
          >
            <Button variant="ghost" size="icon" className="h-6 w-6">
              <ExternalLink className="size-3" />
            </Button>
          </a>
        </div>

        <h2 className="mt-3 text-balance text-sm font-semibold leading-tight tracking-tight">
          {article.title ?? article.url}
        </h2>

        <div className="mt-4 text-xs leading-relaxed text-muted-foreground">
          <MarkdownContent content={article.content} />
        </div>
      </article>
    </ScrollArea>
  )
}
