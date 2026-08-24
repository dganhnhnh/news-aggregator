# Crawl error-rate report — 2026-08-21

Snapshot observed at **2026-08-21 04:09 UTC** from the local PostgreSQL
database. The accompanying reusable queries are in
[`crawl_failure_diagnostics.sql`](crawl_failure_diagnostics.sql); follow
[`crawl-diagnostic-workflow.md`](crawl-diagnostic-workflow.md) to reproduce the
diagnosis and turn its results into actions.

## Executive summary

- The durable queue contains **581 unresolved failures**: **361 article**
  crawls and **220 feed** polls, across **217 sources**.
- All 581 rows are eligible for retry now; none is waiting for backoff and none
  has been dead-lettered. The oldest unresolved failure first appeared on
  2026-08-09 10:09 UTC; the newest was recorded on 2026-08-21 03:47 UTC.
- The current backlog has **974 recorded attempts**. Eighteen article rows and
  62 feed rows have reached at least three attempts.
- The leading root cause is browser `ERR_CONNECTION_REFUSED` (**143** rows),
  followed by feed-level connection failures (**97**) and generic crawler
  runtime errors (**62**).

`crawl_failures` is a *current-state retry queue*, not an event log: each
`(failure_kind, url)` has only one row whose `attempt_count` is incremented.
Therefore, the error-rate section uses `worker_runs.stats`, while the queue
section describes unresolved work only.

## Current queue by kind

| Kind | Open failures | Affected sources | Recorded attempts | Rows with ≥3 attempts |
| --- | ---: | ---: | ---: | ---: |
| Article | 361 | 31 | 571 | 18 |
| Feed | 220 | 188 | 403 | 62 |
| **Total** | **581** | **217** | **974** | **80** |

## Leading sources by unresolved failure count

The diagnostic SQL lists every source. The largest groups in this snapshot are:

| Source | Kind | Open failures | Recorded attempts | Avg. attempts |
| --- | --- | ---: | ---: | ---: |
| target tech | Article | 36 | 111 | 3.08 |
| Ars Technica - All content | Article | 32 | 32 | 1.00 |
| FINN Technology | Article | 20 | 40 | 2.00 |
| Nextdoor Engineering - Medium | Article | 20 | 26 | 1.30 |
| Ziff Media Engineering - Medium | Article | 20 | 20 | 1.00 |
| The Vergecast | Article | 20 | 20 | 1.00 |
| NYT > Technology | Article | 20 | 20 | 1.00 |
| Reply All | Article | 20 | 20 | 1.00 |
| Engadget - Technology News & Expert Reviews | Article | 20 | 20 | 1.00 |
| Lyft Engineering - Medium | Article | 14 | 24 | 1.71 |

`target tech` is the strongest repeat offender: 36 unresolved URLs account for
111 attempts. The broadest operational issue, however, is feed polling: 188
separate sources currently have a feed failure.

## Error-message clusters

URLs, dynamic crawler line numbers, and malformed-feed positions are normalized
in the SQL so identical operational failures are grouped together.

| Kind | Error type | Normalized error cluster | Open failures | Recorded attempts |
| --- | --- | --- | ---: | ---: |
| Article | CrawlError | `browser:ERR_CONNECTION_REFUSED` | 143 | 215 |
| Feed | ConnectError | `connection_failed` | 97 | 151 |
| Article | CrawlError | `crawler_runtime_error` | 62 | 161 |
| Article | CrawlError | `anti_bot:structural_minimal_content` | 41 | 41 |
| Feed | HTTPStatusError | `http_redirect:301` | 34 | 48 |
| Article | CrawlError | `empty_markdown` | 32 | 32 |
| Feed | HTTPStatusError | `http_client:404` | 31 | 71 |
| Article | CrawlError | `anti_bot:datadome_captcha` | 22 | 24 |
| Article | CrawlError | `anti_bot:other` (HTTP 403 HTML) | 22 | 22 |
| Article | CrawlError | `browser:ERR_NAME_NOT_RESOLVED` | 20 | 40 |
| Feed | ValueError | `malformed_rss_atom` | 18 | 43 |
| Feed | HTTPStatusError | `http_client:403` | 16 | 41 |
| Feed | ConnectTimeout | `connecttimeout` | 12 | 27 |
| Article | CrawlError | `browser:ERR_CONNECTION_CLOSED` | 8 | 16 |
| Article | CrawlError | `browser:ERR_CONNECTION_RESET` | 7 | 14 |
| Feed | HTTPStatusError | `http_redirect:302` | 5 | 7 |
| Feed | HTTPStatusError | `http_client:401` | 3 | 5 |
| Feed | HTTPStatusError | `http_client:429` | 2 | 6 |
| Article | CrawlError | `browser:ERR_ABORTED` | 2 | 4 |
| Article | CrawlError | `browser:ERR_NETWORK_CHANGED` | 2 | 2 |
| Feed | HTTPStatusError | `http_server:522` | 1 | 3 |
| Feed | ReadTimeout | `readtimeout` | 1 | 1 |

The three dominant families are connection/network failures (browser refusal,
DNS, closed/reset connections, and feed connects), crawler runtime failures,
and source-specific feed/content problems (redirects, missing feeds,
anti-bot defenses, and malformed XML).

## Crawl job error rates

| Period | Completed runs | Succeeded | Partial | Failed | Abandoned | Run-status incident rate | Crawl attempt failure rate | Feed poll failure rate | Retry success rate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All recorded runs | 27 | 6 | 19 | 0 | 2 | 77.78% | 21.28% (941 / 4,422) | 16.09% (166 / 1,032) | 30.92% (171 / 553) |
| Last 7 days | 16 | 1 | 14 | 0 | 1 | 93.75% | 50.94% (326 / 640) | 32.17% (166 / 516) | 33.48% (149 / 445) |

**Interpretation:** a `partial` crawl run is not necessarily an orchestration
failure. The worker deliberately closes as `partial` whenever it persisted one
or more crawl/feed failures (and also on a graceful shutdown). The attempt
failure rates are therefore the more direct reliability measures: they roughly
doubled in the trailing seven days, and feed poll failures account for all 166
recent feed failures.

## Immediate follow-up order

1. Investigate the 143 `ERR_CONNECTION_REFUSED` article crawls and 97 feed
   connection failures together: they may reflect a shared egress/browser or
   DNS/connectivity issue, rather than independent publisher failures.
2. Review the 62 crawler runtime errors using query 4's URL-level detail; the
   normalized cluster intentionally groups their varied underlying messages.
3. Correct permanent source configuration rather than repeatedly retrying
   deterministic failures: 301/302 redirects, 404/401 feed URLs, malformed
   RSS, and known anti-bot-protected sources.
4. Review the retry schedule before the next worker cycle. Every unresolved
   row is currently due, so the queue will attempt 581 items unless bounded by
   `MAX_RETRIES_PER_RUN`.

## Remediation status — 2026-08-24

Clusters 1–5 and 7 of the ranked plan are closed. Clusters 6 and 8 (a
write-time error taxonomy and the anti-bot/paywall sources) are untouched.

| # | Cluster | Change |
| --- | --- | --- |
| 1 | `crawler_runtime_error`, `ERR_NAME_NOT_RESOLVED` (~82) | Entry links are resolved against the feed's final URL with `urljoin`, so relative links no longer reach the crawler verbatim and hostnames are no longer concatenated with the path. |
| 2 | `anti_bot:structural_minimal_content` (~40) | Enclosure links, media content types and media file extensions are rejected when choosing an entry's link; an episode page still wins over its audio file. Skips are counted as `entries_skipped_non_article`. |
| 3 | `http_redirect:301/302` (39) | Permanent (301/308) redirect targets are stored in `feed_state.resolved_url` and polled thereafter, while `feed_url` stays the identity from the OPML/TSV so re-seeding cannot resurrect the old URL. Temporary redirects are deliberately not recorded. Stale rows are reconciled by `purge_stale_feed_failures()` at the start of each run. |
| 4 | `http_client:404`/`401` (34) | 401/404/410 on a feed are flagged `permanent`: the source is disabled and the failure dead-lettered on first occurrence instead of burning five attempts. 403 is excluded, since it is commonly transient anti-bot behaviour. This is self-healing — no manual feed-list curation is needed. |
| 5 | Medium/`content:encoded`, per-host load | An entry's own `content:encoded`/`<content>` body is rendered to Markdown and stored directly when it reaches `FEED_CONTENT_MIN_CHARS` (1200), so the page is never fetched; `summary`/`description` is deliberately excluded because it is a teaser by convention. Crawls are partitioned into per-host waves (`MAX_CRAWLS_PER_HOST`, default 1) with `CRAWL_DELAY_PER_HOST_SECONDS` (default 1.0) between them; batches of distinct hosts still run in a single wave. Counted as `articles_from_feed_content` / `from_feed=` in the run log. |
| 7 | `ERR_CONNECTION_CLOSED/RESET/ABORTED/NETWORK_CHANGED`, `readtimeout`, `http_server:522` (~21) | **No code change, by decision.** These are genuinely transient: the origin or the network dropped a connection that may well succeed later. Exponential backoff with full jitter (capped at one day) already spaces the retries out, and `MAX_FAILURE_ATTEMPTS = 5` dead-letters anything that turns out to be permanent. Adding a special case would either retry sooner than the origin can cope with or discard recoverable articles. Revisit only if a single host dominates this cluster, which is a throttling problem (cluster 5), not a retry-policy one. |

Existing queue rows created by the cluster 1 and 2 bugs are not repaired by the
code fix — the URLs stored in them are unfetchable. Clear them once with
`docs/crawl_failure_cleanup_2026-08-24.sql` (56 relative links, 40 media
enclosures, 20 mangled `tech.finn.no<year>` hosts); the underlying articles are
re-queued with correct URLs on the next poll.

### Follow-up — 2026-08-24, self-inflicted 403s

Investigating why `http://rss.slashdot.org/Slashdot/slashdotMain` still failed
after the cluster 3 fix turned up a separate cause, now also fixed:

- **The User-Agent was impersonating a Google crawler.** The default HTTP
  client announced `Mozilla/5.0 (compatible; GoogleNewsBot/1.0;
  +https://news.google.com)`. Bot filters answer a Googlebot-shaped agent from
  a non-Google IP with 403. Measured on the same URL: that agent gets 403,
  `news-radar/1.0` gets 200. Across a 20-feed sample from
  `data/engineering_blogs.opml` the honest agent is not worse anywhere. Since
  403 is (correctly) treated as transient, these failures were counting towards
  `--disable-after-failures` and would eventually have disabled working feeds.
  The default is now `news-radar/1.0` in `core.config.DEFAULT_USER_AGENT`,
  overridable via `FEED_USER_AGENT` or `--user-agent`. No feed had been
  disabled yet, so no repair was needed.
- **A redirect proved by a failed poll was discarded.** `_permanent_redirect_url`
  ran only on the 304 and success paths, so a 301→403 chain re-learned the same
  hop on every poll. The redirect is now read off the error response and
  persisted by `record_feed_failure(resolved_url=...)`, which already updates
  that row. Temporary redirects and connection failures still record nothing.
- **The run log named the feed file even for explicitly passed URLs**, which
  read as though the argument had been ignored. It now reports
  `N explicit feed URL(s)`.

One open question remains from the original investigation: resolved failures
were leaking rather than being deleted (17 feed, 4 article). The composite
`DELETE ... IN ((kind, url))` was verified to compile correctly, so the cause is
still unexplained. `purge_stale_feed_failures()` reconciles the feed half as a
side effect but is not a diagnosis.
