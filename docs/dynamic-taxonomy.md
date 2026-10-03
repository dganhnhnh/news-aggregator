# Story-grounded taxonomy discovery

`news-worker taxonomy-discover` proposes **additional** topics from real clusters
with at least two members in the selected event-time window. Closed historical
clusters qualify; merged redirects do not. Saved proposals fetch their referenced
story IDs directly, so a changed top-100 ranking cannot invalidate them. It uses the existing
LLM provider configuration. Existing topics, thresholds, exemplars, taxonomy trees
and inactive topics are preserved. Classification still runs locally with the
existing embedding model; there is no LLM request per article.

The initial window is seven days. A run sends at most 100 stories, three member
representatives per story and 500 summary characters per representative, together
with the current topic names and definitions. Stories are ordered by member count.
The model proposes at most 12 broad topics, references supporting story IDs, and
supplies bounded exemplars and keywords. Local validation rejects nonexistent or
singleton evidence, duplicate names/slugs, duplicate signals, malformed output and
prototype similarity >= 0.82 to an existing active or already accepted topic. The
similarity cutoff is a heuristic, not a measured precision guarantee. Growth stops
at 100 existing topics and requires consolidation. This limits fragmentation but
cannot fully detect semantic synonyms or bad upstream clusters.

Publication takes the exclusive classify lock briefly, after the LLM/embedding
work, so a classifier using an old snapshot cannot finish over the stale requeue.
A busy classifier produces a partial run; retry it. Complete topics, signals,
prototypes and nodes in an additive `discovered` taxonomy are inserted in one
transaction. Existing slugs are never overwritten. Stale embedded articles are
then queued only inside the selected window; the semantic service drains that
queue and refreshes affected cluster topics. A crash after publication is
recoverable with `classify --stale --last 7d`.

No schema migration is required. `topics`, `topic_signals`, `topic_prototypes`,
`taxonomies` and their node/link tables already express the new categories.
`worker_runs.params/stats` retain policy/model identity, input fingerprint, proposal,
evidence IDs, accepted/rejected topics and created/requeued counts. Successful
identical preview inputs can reuse a cached proposal. Changes to the provider's
model alias without changing its configured name are not detectable by this cache.

If operational review later needs aliases, merges or long-lived provenance beyond
worker-run retention, add `topic_aliases` (unique normalized alias -> topic ID) and
`taxonomy_proposals` (input hash, model/prompt version, evidence, review status,
reviewer and applied topic IDs). Keep these separate from article/cluster topics.
Any such change must start in `schema.py`, then use Alembic autogenerate against
head; no migration is needed for the current implementation.

## Preview, evaluate, apply

```sh
# Preview only: records an operational run, but changes no topic or article label.
uv run news-worker taxonomy-discover --last 7d --report /tmp/taxonomy-report.json

# Capture once, then compare every candidate against the same local inputs.
uv run python scripts/evaluate_taxonomy.py --capture --last 7d \
  --snapshot /tmp/taxonomy-corpus.json --output /tmp/baseline.json
uv run python scripts/evaluate_taxonomy.py \
  --snapshot /tmp/taxonomy-corpus.json --proposal /tmp/taxonomy-report.json \
  --output /tmp/candidate.json

# Publish a reviewed proposal using the SAME explicit window as the preview.
uv run news-worker taxonomy-discover --since START --until END \
  --proposal /tmp/taxonomy-report.json --apply --strict
```

A proposal/report file must reference stories still eligible in that window; use
fixed bounds when comparing or applying a saved preview. Evaluation exports local
cleaned article inputs; do not publish the snapshot as a public artifact. It makes
no LLM calls and never writes to PostgreSQL. Results contain overall coverage,
coverage outside discovery inputs, per-topic counts and a deterministic review
sample. Coverage is not accuracy. For micro precision/recall, pass `--gold` with an
independently reviewed JSON mapping of article IDs to complete relevant topic slug
lists. The outside-input split is descriptive, not a randomly assigned test set.

Daily discovery is ready in the current Compose/supercronic maintenance path but
**off by default**: set `TAXONOMY_DISCOVERY_ENABLED=true` in `.env` and recreate the
maintenance service to enable it. The service receives provider configuration and
mounts the shared embedding cache. The semantic service must be running to drain
reclassification jobs. The older suspended Airflow DAG is unchanged.

```sh
docker compose --env-file .env -f deployments/docker-compose.yml \
  up -d --no-deps maintenance
```

To disable discovery, set the flag false and recreate maintenance. Added topics
remain ordinary registry entries and can be edited/exported with the existing
administrative tooling. Re-importing a static seed with `--prune` deactivates
anything not in that seed, including discovered topics; do not use prune for the
normal additive workflow.

## Seven-day baseline experiment, 2026-10-03

Fixed window: 2026-09-26 04:41:39 to 2026-10-03 04:41:39 UTC
(11:41:39 in Vietnam). 1,788 embedded articles; 49 multi-article stories;
10 active topics, all focused on technology/science. Exact production text
preparation, including boilerplate profiles, reproduces the stored baseline.

| Experiment | Articles with >=1 topic | Coverage |
|---|---:|---:|
| Existing taxonomy, threshold 0.40 | 277 / 1,788 | 15.49% |
| Existing taxonomy, threshold 0.35 | 487 / 1,788 | 27.24% |
| Existing taxonomy, threshold 0.30 | 735 / 1,788 | 41.11% |

The global threshold reduction was **not applied**. Inspection of a deterministic
sample of newly assigned records finds clear topic-scope errors: article 17415
(Domino's Pizza valuation, 0.369), 15438 (Bank of East Asia, 0.332) and 17156
(Alaska Air investor presentation, 0.344) all become `big-tech-business`. Another
record about Amazon data-center policy (17576, 0.378) becomes `cybersecurity`.
These are error examples, not a quantitative precision estimate. They motivate
filling missing topic scopes instead of globally lowering old thresholds.

The 49 discovery stories contain 149 articles. The other 1,639 have baseline
coverage 237/1,639 (14.46%). Keep this outside-story group separate when measuring
whether proposed topics generalize beyond their supporting stories.

After user approval, three sequential requests were made to the configured
`ark.ap-southeast.bytepluses.com` endpoint with `deepseek-v4.1-flash`: the first
returned empty content, and two subsequent extraction requests succeeded with
thinking disabled. Requests were separated by local work (minutes), with automatic
retries disabled. Only the original 49 story summaries and registry were sent;
the article quality review below ran locally without another external LLM request.

| Candidate | All 1,788 articles | Outside the 49 discovery stories (1,639 articles) |
|---|---:|---:|
| Existing taxonomy | 277 (15.49%) | 237 (14.46%) |
| First LLM proposal | 401 (22.43%) | 322 (19.65%) |
| Refined LLM proposal | 810 (45.30%) | 693 (42.28%) |
| Reviewed final proposal | **784 (43.85%)** | **663 (40.45%)** |

The final proposal adds 10 topics: investing/markets, macroeconomics, corporate
earnings, shopping deals, sports broadcasts, politics/elections, legal/judicial,
environment/climate, games/puzzles, and the media/entertainment industry. Existing
10 topics and their 0.40 thresholds are unchanged; the new topics also use 0.40.
This adds coverage for 507 previously uncategorized articles in the fixed snapshot.

The second proposal over-assigned corporate earnings to investment commentary and
shopping deals to financial/technology articles. On 40 calibration articles
(sampled five per v2 topic, seed 13), the scope review found 14 errors in 53 new
labels. Narrower report-style exemplars, specific earnings-call/results phrases
and removal of ambiguous shopping terms reduced this to 4 errors in 43 labels.
Two valid small topics from v1 (puzzles and media industry) were retained.
The final artifact is **LLM-generated and then reviewed/edited by Codex**, not an
unmodified LLM response. Discovery prompt policy v3 incorporates these lessons.

A separate 60-article sample (seed 20261003), excluding discovery stories and all
calibration articles, was reviewed after freezing v3. Of its 67 new label
assignments, 59 were supported, 7 outside the defined scope, and 1 uncertain:
**88.1% strict label precision** when uncertain is counted as incorrect. 57/60
articles had at least one supported new label. This is a Codex review of titles
and cleaned leads, not an independently human-annotated benchmark. It measures
new-label precision on assigned articles only, not recall or overall corpus
accuracy; financial articles dominate this sample. Remaining errors include
macroeconomics on company/fund investment commentary and an earnings label on a
dividend-screening methodology article. No further tuning used this review set.

Artifacts: [reviewed proposal](experiments/taxonomy-2026-10-03/proposal-reviewed.json),
[metrics](experiments/taxonomy-2026-10-03/metrics.json),
[calibration review](experiments/taxonomy-2026-10-03/calibration-review.json),
[separate sample review](experiments/taxonomy-2026-10-03/holdout-review.json).

**Database status:** the reviewed topics have not been published, and the daily
schedule remains disabled. Two preview runs (19139, 19148) revealed historical
story-selection issues, which were fixed: include closed clusters, and resolve a
saved proposal's own evidence instead of the current top-100 list. A subsequent
live check/publication was blocked because automatic approval review exhausted
its usage allowance. This was a review-system failure, not a safety rejection.
The final evidence-filter SQL has a PostgreSQL integration test but its latest
revision has not been rerun live; earlier atomic publication/cache/window tests
passed in an isolated schema. Use the commands below to complete publication.

## Run the reviewed seven-day experiment

From the repository root, with `DATABASE_URL` already configured in `.env`:

```sh
cd /Users/anhnd29/projects/news-radar

# No LLM call: validate and publish the reviewed proposal; enqueue stale rows
# only within the exact original experiment window.
uv run news-worker taxonomy-discover \
  --since '2026-09-26T04:41:39.498213+00:00' \
  --until '2026-10-03T04:41:39.498213+00:00' \
  --proposal docs/experiments/taxonomy-2026-10-03/proposal-reviewed.json \
  --apply --strict --report /tmp/taxonomy-applied.json

# The running semantic service drains the queued jobs automatically.
# If it is not running, drain using the existing classify worker:
uv run news-worker classify --stale \
  --since '2026-09-26T04:41:39.498213+00:00' \
  --until '2026-10-03T04:41:39.498213+00:00' --strict
```

The classify worker's date window controls stale requeueing; its normal drain can
also process other already-pending jobs. Avoid this drain command if you need to
leave a separate pending backlog untouched. No old historical records outside the
window are explicitly requeued by taxonomy discovery. A busy classify lock causes
the publication command to exit nonzero without modifying topics; retry when the
active classify run completes. A repeated saved proposal preserves existing slugs.
If an evidence cluster has since merged or lost members, validation intentionally
fails; create a fresh preview rather than removing the validation.

For a **fresh LLM discovery** on the latest seven days, set this provider-specific
option in `.env` (it does not alter the chat agent's model configuration):

```dotenv
TAXONOMY_LLM_EXTRA_BODY={"thinking":{"type":"disabled"}}
TAXONOMY_DISCOVERY_ENABLED=false
```

BytePlus documents the `thinking.type` option in its
[Chat API reference](https://docs.byteplus.com/en/docs/modelark/chat-api).

```sh
# Preview first; inspect the generated JSON and compare coverage/quality.
uv run news-worker taxonomy-discover --last 7d --strict \
  --report /tmp/taxonomy-preview.json

# One-shot automatic discovery/publication on the current seven-day window.
# This makes one LLM request if there is no cached identical input.
uv run news-worker taxonomy-discover --last 7d --apply --strict \
  --report /tmp/taxonomy-current.json
```

Only enable `TAXONOMY_DISCOVERY_ENABLED=true` when daily automatic additions are
wanted. Recreate maintenance with the Compose command above; its existing cron
runs at 03:00 UTC (10:00 Vietnam). Keep the semantic service running. The adapter
uses no automatic API retries, so failed/rate-limited calls wait for a later
operator retry or scheduled run.

