-- One-off cleanup of crawl_failures rows left behind by two parser bugs that
-- were fixed on 2026-08-24 (see crawl_error_rate_report_2026-08-21.md).
--
-- Every row below is unusable: the URL stored in the queue is not something the
-- crawler could ever fetch, so retrying it only burns attempts until the row
-- dead-letters. The underlying articles are still listed by their feeds and get
-- re-queued with correct URLs on the next poll.
--
-- Run once, against a database already at Alembic head:
--   psql "$DATABASE_URL" -f docs/crawl_failure_cleanup_2026-08-24.sql

BEGIN;

-- Relative entry links stored verbatim, e.g. "/2020/06/22/post.html".
-- Fixed by resolving entry links against the feed URL with urljoin().
DELETE FROM crawl_failures
WHERE failure_kind = 'article'
  AND url !~ '^https?://';

-- Podcast/media enclosures queued as if they were articles. Fixed by rejecting
-- enclosure links and media content types when selecting an entry's link.
DELETE FROM crawl_failures
WHERE failure_kind = 'article'
  AND lower(regexp_replace(url, '\?.*$', '')) ~
      '\.(mp3|m4a|m4v|aac|ogg|oga|opus|wav|flac|mp4|mov|avi|mkv|webm|jpg|jpeg|png|gif|webp|svg|pdf|epub|zip|gz|tar|dmg|exe)$';

-- Hosts mangled by the same missing separator, e.g. "tech.finn.no2017/...".
-- Restricted to hosts verified to be concatenation artifacts, so this does not
-- silently discard genuinely dead domains.
DELETE FROM crawl_failures
WHERE failure_kind = 'article'
  AND error_message LIKE '%ERR_NAME_NOT_RESOLVED%'
  AND url ~ '://[a-z0-9.-]+\.[a-z]{2,}[0-9]{4}/';

COMMIT;
