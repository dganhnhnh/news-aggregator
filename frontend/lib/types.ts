export type Source = {
  slug: string
  name: string
  homepage: string | null
}

export type FeedPriority = 'instant' | 'normal'

export type ManagedFeed = {
  feed_url: string
  source_label: string
  priority: FeedPriority
  active: boolean
  resolved_url: string | null
  last_polled_at: string | null
  last_success_at: string | null
  next_poll_at: string
  consecutive_failures: number
  disabled_at: string | null
  disabled_reason: string | null
  updated_at: string
}

export type SourceRef = {
  slug: string
  name: string
}

export type ResearchCitation = {
  article_id: number
  title: string | null
  url: string
  source: SourceRef | null
  published_at: string | null
}

export type ResearchStreamEvent =
  | { event: 'start'; data: { request_id: string; thread_id: string } }
  | { event: 'status'; data: { phase: 'searching' | 'reading' | 'synthesizing'; tool?: string } }
  | { event: 'delta'; data: { text: string } }
  | { event: 'reset'; data: Record<string, never> }
  | { event: 'citations'; data: { citations: ResearchCitation[] } }
  | { event: 'done'; data: { request_id: string; thread_id: string } }
  | {
      event: 'error'
      data: { message: string; error_type: string; reason: string; request_id: string }
    }

export type TopicRef = {
  id: number
  slug: string
  name: string
}

export type Topic = TopicRef & {
  description: string | null
}

export type TaxonomyNode = {
  slug: string
  name: string
  topic_slugs: string[]
  children: TaxonomyNode[]
}

export type TopicCatalog = {
  taxonomy: {
    slug: string
    name: string
    revision: number
  }
  topics: Topic[]
  nodes: TaxonomyNode[]
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
  topics: TopicRef[]
}

export type ArticleDetail = {
  id: number
  url: string
  url_original: string | null
  title: string | null
  content: string
  feed_summary: string
  source: SourceRef | null
  published_at: string | null
  crawled_at: string
  event_at: string
  topics: TopicRef[]
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
  topics: TopicRef[]
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
  topics: TopicRef[]
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
