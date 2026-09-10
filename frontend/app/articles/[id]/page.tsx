import Link from 'next/link'
import { notFound } from 'next/navigation'
import { ArrowLeft, ExternalLink } from 'lucide-react'
import type { Metadata } from 'next'
import { Button } from '@/components/ui/button'
import { MarkdownContent } from '@/components/feed/markdown-content'
import { getArticle } from '@/lib/server-api'
import { formatAbsolute, relativeTime } from '@/lib/time'

export const dynamic = 'force-dynamic'

export async function generateMetadata({
  params,
}: {
  params: Promise<{ id: string }>
}): Promise<Metadata> {
  const { id } = await params
  const article = await getArticle(id)
  if (!article) return { title: 'Article not found — News' }
  const description = article.content || article.feed_summary
  return {
    title: `${article.title ?? article.url} — News`,
    description: description.slice(0, 160),
  }
}

export default async function ArticlePage({
  params,
}: {
  params: Promise<{ id: string }>
}) {
  const { id } = await params
  const article = await getArticle(id)
  if (!article) notFound()

  return (
    <div className="min-h-svh bg-background">
      <header className="sticky top-0 z-10 flex items-center justify-between border-b border-border bg-background/95 px-4 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/80">
        <Button variant="ghost" size="sm" render={<Link href="/" />}>
          <ArrowLeft data-icon="inline-start" />
          Back to feed
        </Button>
        <Button
          variant="outline"
          size="sm"
          render={
            <a href={article.url_original ?? article.url} target="_blank" rel="noopener noreferrer" />
          }
        >
          Original source
          <ExternalLink data-icon="inline-end" />
        </Button>
      </header>

      <article className="mx-auto flex max-w-2xl flex-col px-6 py-10">
        <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
          <span className="font-medium text-foreground/80">
            {article.source?.name ?? 'Unknown source'}
          </span>
          {article.published_at && (
            <>
              <span aria-hidden="true">·</span>
              <time dateTime={article.published_at}>{formatAbsolute(article.published_at)}</time>
            </>
          )}
          <span aria-hidden="true">·</span>
          <span>crawled {relativeTime(article.crawled_at)}</span>
        </div>

        <h1 className="mt-4 text-balance text-3xl font-semibold leading-tight tracking-tight">
          {article.title ?? article.url}
        </h1>

        <div className="mt-8">
          <MarkdownContent content={article.content || article.feed_summary} />
        </div>
      </article>
    </div>
  )
}
