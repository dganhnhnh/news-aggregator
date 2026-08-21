-- Read-only diagnostics for the current crawl failure queue and crawl worker.
-- Run from the repository root:
--   docker compose -f deployments/docker-compose.yml --env-file .env \
--     exec -T db psql -U news_radar -d news_radar \
--     -P pager=off -f /dev/stdin < docs/crawl_failure_diagnostics.sql
--
-- `crawl_failures` is a durable *current* retry queue, keyed by
-- (failure_kind, url); it does not preserve a separate row for every failed
-- attempt.  Use the worker_runs queries below for historical attempt rates.

\pset pager off
\pset null '(unknown)'

-- 1. Current queue health: retryable work, backoff work, and dead letters.
SELECT
    now() AS observed_at,
    count(*) AS open_failures,
    count(*) FILTER (
        WHERE dead_lettered_at IS NULL AND next_retry_at <= now()
    ) AS retry_due_now,
    count(*) FILTER (
        WHERE dead_lettered_at IS NULL AND next_retry_at > now()
    ) AS waiting_for_backoff,
    count(*) FILTER (WHERE dead_lettered_at IS NOT NULL) AS dead_lettered,
    min(first_failed_at) AS oldest_first_failure,
    max(last_failed_at) AS newest_failure
FROM crawl_failures;

-- 2. Current failures grouped by source.  A NULL source is kept visible.
SELECT
    failure_kind,
    coalesce(source, '(unknown source)') AS source,
    count(*) AS open_failures,
    sum(attempt_count) AS recorded_attempts,
    round(avg(attempt_count), 2) AS average_attempts,
    count(*) FILTER (WHERE dead_lettered_at IS NULL AND next_retry_at <= now())
        AS retry_due_now,
    count(*) FILTER (WHERE dead_lettered_at IS NOT NULL) AS dead_lettered,
    min(first_failed_at) AS oldest_first_failure,
    max(last_failed_at) AS newest_failure
FROM crawl_failures
GROUP BY failure_kind, coalesce(source, '(unknown source)')
ORDER BY open_failures DESC, failure_kind, source;

-- 3. Root-cause clusters.  URLs and crawler line numbers are removed so the
-- same operational error is grouped even when it occurred on many articles.
WITH classified AS (
    SELECT
        failure_kind,
        coalesce(source, '(unknown source)') AS source,
        error_type,
        CASE
            WHEN error_message ~ 'net::ERR_[A-Z_]+' THEN
                'browser:' || substring(error_message FROM 'net::(ERR_[A-Z_]+)')
            WHEN error_message ~ 'Redirect response ''[0-9]{3}' THEN
                'http_redirect:' || substring(
                    error_message FROM 'Redirect response ''([0-9]{3})'
                )
            WHEN error_message ~ 'Client error ''[0-9]{3}' THEN
                'http_client:' || substring(
                    error_message FROM 'Client error ''([0-9]{3})'
                )
            WHEN error_message ~ 'Server error ''[0-9]{3}' THEN
                'http_server:' || substring(
                    error_message FROM 'Server error ''([0-9]{3})'
                )
            WHEN error_type = 'ConnectError' THEN 'connection_failed'
            WHEN error_type IN ('ConnectTimeout', 'ReadTimeout') THEN lower(error_type)
            WHEN error_message LIKE 'Malformed RSS/Atom feed:%' THEN 'malformed_rss_atom'
            WHEN error_message LIKE 'Crawler returned empty Markdown content%' THEN
                'empty_markdown'
            WHEN error_message LIKE 'Blocked by anti-bot protection: Structural:%' THEN
                'anti_bot:structural_minimal_content'
            WHEN error_message LIKE 'Blocked by anti-bot protection: DataDome captcha%' THEN
                'anti_bot:datadome_captcha'
            WHEN error_message LIKE 'Blocked by anti-bot protection:%' THEN
                'anti_bot:other'
            WHEN error_message LIKE 'Unexpected error in _crawl_web%' THEN
                'crawler_runtime_error'
            ELSE left(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(error_message, 'https?://[^[:space:]'']+', '<url>', 'g'),
                        'line [0-9]+',
                        'line <n>',
                        'g'
                    ),
                    '[[:space:]]+',
                    ' ',
                    'g'
                ),
                180
            )
        END AS error_cluster,
        attempt_count,
        first_failed_at,
        last_failed_at,
        dead_lettered_at
    FROM crawl_failures
)
SELECT
    failure_kind,
    error_type,
    error_cluster,
    count(*) AS open_failures,
    count(DISTINCT source) AS affected_sources,
    sum(attempt_count) AS recorded_attempts,
    count(*) FILTER (WHERE dead_lettered_at IS NOT NULL) AS dead_lettered,
    min(first_failed_at) AS first_seen,
    max(last_failed_at) AS last_seen
FROM classified
GROUP BY failure_kind, error_type, error_cluster
ORDER BY open_failures DESC, recorded_attempts DESC, failure_kind, error_type;

-- 4. Detail list, ordered by retry urgency and recurrence.  Change LIMIT to
-- inspect more rows without changing the aggregation queries above.
WITH classified AS (
    SELECT
        *,
        CASE
            WHEN error_message ~ 'net::ERR_[A-Z_]+' THEN
                'browser:' || substring(error_message FROM 'net::(ERR_[A-Z_]+)')
            WHEN error_message ~ 'Redirect response ''[0-9]{3}' THEN
                'http_redirect:' || substring(
                    error_message FROM 'Redirect response ''([0-9]{3})'
                )
            WHEN error_message ~ 'Client error ''[0-9]{3}' THEN
                'http_client:' || substring(
                    error_message FROM 'Client error ''([0-9]{3})'
                )
            WHEN error_message ~ 'Server error ''[0-9]{3}' THEN
                'http_server:' || substring(
                    error_message FROM 'Server error ''([0-9]{3})'
                )
            WHEN error_type = 'ConnectError' THEN 'connection_failed'
            WHEN error_type IN ('ConnectTimeout', 'ReadTimeout') THEN lower(error_type)
            WHEN error_message LIKE 'Malformed RSS/Atom feed:%' THEN 'malformed_rss_atom'
            WHEN error_message LIKE 'Crawler returned empty Markdown content%' THEN
                'empty_markdown'
            WHEN error_message LIKE 'Blocked by anti-bot protection: Structural:%' THEN
                'anti_bot:structural_minimal_content'
            WHEN error_message LIKE 'Blocked by anti-bot protection: DataDome captcha%' THEN
                'anti_bot:datadome_captcha'
            WHEN error_message LIKE 'Blocked by anti-bot protection:%' THEN
                'anti_bot:other'
            WHEN error_message LIKE 'Unexpected error in _crawl_web%' THEN
                'crawler_runtime_error'
            ELSE left(regexp_replace(error_message, '[[:space:]]+', ' ', 'g'), 180)
        END AS error_cluster
    FROM crawl_failures
)
SELECT
    failure_kind,
    coalesce(source, '(unknown source)') AS source,
    error_type,
    error_cluster,
    attempt_count,
    first_failed_at,
    last_failed_at,
    next_retry_at,
    dead_lettered_at,
    url
FROM classified
ORDER BY
    (dead_lettered_at IS NOT NULL),
    (next_retry_at > now()),
    attempt_count DESC,
    last_failed_at ASC
LIMIT 100;

-- 5. Crawl-job rates from all recorded crawl runs and from the trailing 7 days.
-- A run-status incident is partial, failed, or abandoned.  The attempt rates
-- use the worker's counters, so they include failures that were later resolved.
WITH runs AS (
    SELECT
        started_at,
        status,
        coalesce((stats ->> 'crawl_succeeded')::bigint, 0) AS crawl_succeeded,
        coalesce((stats ->> 'crawl_failed')::bigint, 0) AS crawl_failed,
        coalesce((stats ->> 'feeds_processed')::bigint, 0) AS feeds_processed,
        coalesce((stats ->> 'feeds_failed')::bigint, 0) AS feeds_failed,
        coalesce((stats ->> 'retries_attempted')::bigint, 0) AS retries_attempted,
        coalesce((stats ->> 'retries_succeeded')::bigint, 0) AS retries_succeeded
    FROM worker_runs
    WHERE worker = 'crawl'
      AND status <> 'running'
), periods AS (
    SELECT 'all recorded runs'::text AS period, '-infinity'::timestamptz AS starts_at
    UNION ALL
    SELECT 'last 7 days', now() - interval '7 days'
)
SELECT
    periods.period,
    count(runs.*) AS completed_runs,
    count(*) FILTER (WHERE runs.status = 'succeeded') AS succeeded_runs,
    count(*) FILTER (WHERE runs.status = 'partial') AS partial_runs,
    count(*) FILTER (WHERE runs.status = 'failed') AS failed_runs,
    count(*) FILTER (WHERE runs.status = 'abandoned') AS abandoned_runs,
    round(
        100.0 * count(*) FILTER (
            WHERE runs.status IN ('partial', 'failed', 'abandoned')
        ) / nullif(count(runs.*), 0),
        2
    ) AS run_status_incident_rate_pct,
    sum(runs.crawl_succeeded) AS crawl_succeeded,
    sum(runs.crawl_failed) AS crawl_failed,
    round(
        100.0 * sum(runs.crawl_failed)
        / nullif(sum(runs.crawl_succeeded + runs.crawl_failed), 0),
        2
    ) AS crawl_attempt_failure_rate_pct,
    sum(runs.feeds_processed) AS feeds_processed,
    sum(runs.feeds_failed) AS feeds_failed,
    round(
        100.0 * sum(runs.feeds_failed)
        / nullif(sum(runs.feeds_processed), 0),
        2
    ) AS feed_poll_failure_rate_pct,
    sum(runs.retries_attempted) AS retries_attempted,
    sum(runs.retries_succeeded) AS retries_succeeded,
    round(
        100.0 * sum(runs.retries_succeeded)
        / nullif(sum(runs.retries_attempted), 0),
        2
    ) AS retry_success_rate_pct
FROM periods
LEFT JOIN runs ON runs.started_at >= periods.starts_at
GROUP BY periods.period, periods.starts_at
ORDER BY periods.starts_at;

-- 6. Most recent crawl runs, including an operational error message when one
-- exists.  Useful to correlate a spike in the queue with a particular run.
SELECT
    id,
    trigger,
    status,
    started_at,
    finished_at,
    coalesce((stats ->> 'crawl_succeeded')::bigint, 0) AS crawl_succeeded,
    coalesce((stats ->> 'crawl_failed')::bigint, 0) AS crawl_failed,
    coalesce((stats ->> 'feeds_processed')::bigint, 0) AS feeds_processed,
    coalesce((stats ->> 'feeds_failed')::bigint, 0) AS feeds_failed,
    coalesce((stats ->> 'retries_attempted')::bigint, 0) AS retries_attempted,
    coalesce((stats ->> 'retries_succeeded')::bigint, 0) AS retries_succeeded,
    error_message
FROM worker_runs
WHERE worker = 'crawl'
ORDER BY started_at DESC
LIMIT 20;
