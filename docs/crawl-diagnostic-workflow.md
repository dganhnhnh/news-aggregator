# Crawl failure diagnostic workflow

Use this runbook to produce a consistent, actionable crawl health report. The
queries are read-only.

## Run the diagnostics

From the repository root, with PostgreSQL running and `.env` configured:

```bash
docker compose -f deployments/docker-compose.yml --env-file .env \
  exec -T db psql -v ON_ERROR_STOP=1 -U news_radar -d news_radar \
  -P pager=off < docs/crawl_failure_diagnostics.sql
```

The SQL is in [crawl_failure_diagnostics.sql](crawl_failure_diagnostics.sql).
See [the 2026-08-21 report](crawl_error_rate_report_2026-08-21.md) for a worked
example.

## Diagnose in this order

1. **Queue health:** record open, retry-due, backoff, dead-lettered, oldest, and
   newest failures. A large due queue means the next run may need bounding.
2. **Source concentration:** rank sources by open failures and attempts. Start
   with sources having both high row counts and high average attempts.
3. **Error clusters:** rank normalized messages by open failures and affected
   sources. A cluster spanning many sources usually indicates shared
   infrastructure or crawler behavior.
4. **Representative rows:** inspect a few URLs from each leading cluster. Do
   not diagnose from one URL alone.
5. **Job rates:** compare the trailing seven days with all recorded runs using
   crawl-attempt, feed-poll, and retry-success rates.
6. **Recent runs:** correlate rate or queue spikes with the latest run status,
   parameters, shutdowns, and operational errors.

## Turn findings into actions

| Cluster | Likely scope | First action | Verify with |
| --- | --- | --- | --- |
| Browser connection/DNS errors across many sources | Shared network, DNS, browser, or egress | Test connectivity from the worker environment | Retry a small bounded sample |
| Redirect, 404, or 401 feed errors | Source configuration | Replace or remove the feed URL | Poll the corrected feed once |
| 403, 429, or anti-bot errors | Publisher policy or rate limiting | Review access policy, rate, and adapter choice | Confirm permitted access before replay |
| Malformed RSS/Atom | Feed or parser compatibility | Validate the response and inspect content type/body | Parse the feed fixture locally |
| Crawler runtime error or empty Markdown | Crawler/extraction behavior | Inspect representative URLs and full messages | Add a focused regression test |
| Dead-lettered rows | Persistent failure after retries | Fix the root cause before replaying | Replay a bounded set and check resolution |

Prefer fixes that remove a whole cluster. Avoid unbounded replay until a small
sample succeeds.

## Report template

```markdown
# Crawl error-rate report — YYYY-MM-DD

Observed at: YYYY-MM-DD HH:MM UTC
Scope: database/environment and comparison window

## Summary

- Queue: <open>, <due>, <backoff>, <dead-lettered>
- Leading source: <source> — <open rows>/<attempts>
- Leading cluster: <cluster> — <rows>, <sources>
- Seven-day rates: crawl <x%>, feed <y%>, retry success <z%>
- Change from baseline: <better/worse/flat and by how much>

## Priority actions

1. <cluster/source>: <action>, owner <name>, verify by <check>.
2. <cluster/source>: <action>, owner <name>, verify by <check>.
3. <cluster/source>: <action>, owner <name>, verify by <check>.

## Caveats

- `crawl_failures` contains unresolved current state, not every historical event.
- `partial` means the run observed failures or stopped gracefully; it is not
  necessarily a worker crash.
```

Keep each report as a dated snapshot. Compare identical time windows and retain
the exact observation time so queue and run-rate changes are reproducible.
