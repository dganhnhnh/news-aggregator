export type Source = {
  slug: string
  name: string
  homepage: string | null
}

export type SourceRef = {
  slug: string
  name: string
}

export type ArticleSummary = {
  id: number
  url: string
  title: string | null
  excerpt: string
  source: SourceRef | null
  published_at: string | null
  crawled_at: string
  event_at: string
}

export type ArticleDetail = {
  id: number
  url: string
  url_original: string | null
  title: string | null
  content: string
  source: SourceRef | null
  published_at: string | null
  crawled_at: string
  event_at: string
}

export type ArticlePage = {
  items: ArticleSummary[]
  next_cursor: string | null
  window: {
    since: string
    until: string
  }
}

export type StorySummary = {
  id: number
  headline: string | null
  first_seen_at: string
  last_seen_at: string
  article_count: number
  source_count: number
  sources: SourceRef[]
}

export type StoryMember = {
  article_id: number
  title: string | null
  url: string
  source: SourceRef | null
  published_at: string | null
  event_at: string
}

export type StoryDetail = {
  id: number
  headline: string | null
  first_seen_at: string
  last_seen_at: string
  article_count: number
  source_count: number
  members: StoryMember[]
}

export type StoryPage = {
  items: StorySummary[]
  next_cursor: string | null
  window: {
    since: string
    until: string
  }
}
