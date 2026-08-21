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
