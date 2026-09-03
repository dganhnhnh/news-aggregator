# Tuần 1 — Nền tảng + khung worker (kế hoạch chi tiết)

> Bản chi tiết hoá §4.1 và §10 "Tuần 1" của [spec.md](spec.md), đã đối chiếu với code
> hiện tại trong `backend/`. Chỗ nào lệch với spec v0.1 đều ghi rõ lý do ở mục 2.
>
> **Ngân sách: 16h** (spec v0.1 ước tính 10h — không đủ khi đưa cả hai worker vào tuần 1).

---

## 1. Mục tiêu & Deliverable

Cuối tuần 1 phải chạy được đúng ba câu lệnh sau, và cả ba đều an toàn khi chạy chồng
nhau hoặc bị kill giữa chừng:

```bash
# Định kỳ — đặt vào cron/systemd timer
news-worker crawl --once
news-worker embed --once

# On-demand, cửa sổ tuỳ ý
news-worker crawl --since 2026-08-01 --until 2026-08-05
news-worker embed --reprocess --last 7d
```

**Deliverable:** mỗi bài crawl về có `source_id` ổn định, `event_at`, `lang`, và một
vector 384 chiều trong `article_embeddings`. Mọi quyết định "retry / tiếp tục lần sau /
bỏ qua vĩnh viễn" đọc từ bảng, không từ bộ nhớ process.

**Định nghĩa "xong":**

```bash
# 1. Không còn job tồn đọng, không có job kẹt ở running
psql -c "SELECT stage, state, count(*) FROM processing_jobs GROUP BY 1,2;"

# 2. Lịch sử chạy đọc được, không có run nào 'abandoned'
psql -c "SELECT worker, trigger, status, count(*) FROM worker_runs GROUP BY 1,2,3;"

# 3. Feed nào đang bị tắt và vì sao
psql -c "SELECT feed_url, consecutive_failures, disabled_reason FROM feed_state WHERE disabled_at IS NOT NULL;"

# 4. ANN search trả kết quả hợp lý
psql -c "SELECT a.title FROM article_embeddings e JOIN raw_articles a ON a.id=e.article_id
         ORDER BY e.embedding <=> (SELECT embedding FROM article_embeddings LIMIT 1) LIMIT 5;"

# 5. Test xanh
uv run --extra dev pytest
```

Kèm `docs/measurements.md` ghi số đo đầu tiên: dedup hit rate L1, tỉ lệ feed trả 304,
tỉ lệ URL bị skip trước khi crawl, embedding throughput ở batch 1/16/64, thời gian
build + dung lượng index HNSW.

---



## 2. Các quyết định chốt trước khi code



### 2.1 KHÔNG partition `raw_articles`

Spec v0.1 §5 yêu cầu partition theo `published_at` theo tháng. Bỏ, vì:

- `published_at` **nullable** (rất nhiều RSS entry không có pubDate) và **mutable**
(re-crawl có thể bổ sung giá trị) → row phải nhảy partition.
- Postgres bắt buộc partition key phải nằm trong mọi UNIQUE constraint. `UNIQUE(url)`
sẽ phải thành `UNIQUE(url, published_at)` — phá vỡ bất biến cốt lõi
("article identity là normalized final URL", `CLAUDE.md`) và làm `ON CONFLICT (url)`
trong [postgres.py:210](../backend/adapters/storage/postgres.py:210) upsert sai.
- Partitioned table làm `alembic revision --autogenerate` sinh migration hỏng, xung đột
với quy trình bắt buộc trong `CLAUDE.md`.
- Quy mô không cần: 422 feed × ~20 bài ≈ vài nghìn bài/ngày. Partition có giá trị từ
~10–50M row trở lên.

**Thay bằng:** cột sinh `event_at = coalesce(published_at, crawled_at)` (NOT NULL,
immutable) + BRIN index.

### 2.2 Hàng đợi trên Postgres, KHÔNG Redis Streams (tuần 1)

Lý do đầy đủ ở [spec.md §4](spec.md). Tóm tắt: không có producer nào để đẩy vào stream,
và `SELECT ... FOR UPDATE SKIP LOCKED` trên `processing_jobs` cho đủ at-least-once,
lease/visibility timeout, DLQ, backpressure với 0 hạ tầng mới.

**Điều kiện để câu chuyện CV vẫn mạnh:** viết `processing_jobs` sau một port `JobQueue`
(`claim(stage, n) -> list[Job]`, `complete(job)`, `fail(job, err)`, `reclaim_expired()`).
Tuần 4 thêm `RedisStreamQueue` implement cùng port rồi **benchmark hai backend**.

### 2.3 Toàn bộ Python

Spec v0.1 §4 gợi ý Go cho worker + API. Bỏ hẳn: codebase hiện 100% Python.

### 2.4 `fastembed` thay vì `sentence-transformers`

MiniLM 384d, CPU, đúng như spec — nhưng qua `fastembed` (ONNX Runtime, ~50MB) thay vì
`sentence-transformers` (kéo theo torch ~2GB). Cùng model
(`sentence-transformers/all-MiniLM-L6-v2`), cùng số chiều. Đóng gói sau port `Embedder`.

### 2.5 Định danh nguồn = hostname của URL bài viết, KHÔNG phải tên feed

Điều chỉnh quan trọng nhất về mặt dữ liệu.

[raw_storage.py:69](../backend/services/raw_storage.py:69) hiện ưu tiên
`feed_title` → `query_label` → hostname, lưu thành **text tự do** trong
`raw_articles.source`. Toàn bộ trend engine (§7.1) dựa trên `n_distinct_sources`, mà với
feed kiểu Google News query thì `feed_title` là *câu truy vấn*, không phải publisher —
một feed sinh bài từ hàng chục toà soạn. Ngược lại cùng một publisher xuất hiện dưới
nhiều biến thể text sẽ bị đếm thành nhiều nguồn.

**Chốt:** bảng `sources`, `slug` = registrable hostname của **final URL sau crawl**.
Giữ `raw_articles.source` (text) làm nhãn hiển thị, thêm `source_id` FK làm định danh.

### 2.6 `backend/workers/` thay thế `news-service`

`news-service` hiện gộp ba việc: vòng lặp định kỳ, orchestration ingest, và ghi
`ingest_runs`. Tách ra:


| Cũ                           | Mới                                                           |
| ---------------------------- | ------------------------------------------------------------- |
| `news-service`               | `news-worker crawl --interval N` (giữ alias deprecate 1 tuần) |
| `services/ingestion.py`      | giữ nguyên — worker gọi vào, không viết lại                   |
| `services/news.py` vòng lặp  | `workers/base.py`                                             |
| `news-pipeline crawl/search` | giữ nguyên, dùng để debug một URL, không đổi                  |


`services/ingestion.py` và `services/raw_storage.py` **không bị viết lại** — worker chỉ
là lớp vận hành bao quanh chúng. Đây là lý do 16h là khả thi.

### 2.7 Nguồn dữ liệu hiện tại KHÔNG khớp taxonomy

`data/engineering_blogs.opml` có 422 feed, toàn blog kỹ thuật (`data/Tech.opml` mới thêm
cũng vậy). Taxonomy spec §3.1 khai báo `tech`, `finance/macro`, `health` — hai domain sau
sẽ nhận gần 0 bài. Tuần 1 không giải quyết (taxonomy là tuần 2) nhưng phải quyết trước
khi viết `taxonomy.yaml`. Xem mục 8.

---



## 3. Kiến trúc `backend/workers/`

```
backend/workers/
  __init__.py
  base.py        # Worker ABC + advisory lock + worker_runs + signals + reaper
  cli.py         # news-worker: parse chung --once/--interval/--since/--until/--last
  window.py      # RunWindow, parse "3d"/"12h"/ISO date
  crawl.py       # CrawlWorker
  embed.py       # EmbedWorker
  queue.py       # JobQueue port + PostgresJobQueue
```



### 3.1 `base.py` — cái mà mọi worker kế thừa

```python
class Worker(ABC):
    name: str                      # 'crawl' | 'embed'

    @abstractmethod
    async def execute(self, ctx: RunContext) -> RunOutcome: ...

    async def run_once(self, ctx: RunContext) -> int:
        """Trả về exit code. Lo toàn bộ phần vận hành chung."""
```

`run_once` làm đúng trình tự sau, worker con không phải lặp lại:

1. `reap_abandoned()` — run `status='running'` có `heartbeat_at` cũ hơn `3 × interval`
  ⇒ `status='abandoned'`. Chạy trước khi lấy lock để dọn cả run của process đã chết.
2. `pg_try_advisory_lock(hashtext('worker:' || name))`. Không lấy được ⇒ log INFO,
  **thoát 0** (cron chồng lịch là chuyện bình thường, không phải lỗi).
3. `INSERT worker_runs(worker, trigger, status='running', window_start, window_end,
  params)`→`run_id` chính là correlation ID cho mọi log của run.
4. Cài handler SIGTERM/SIGINT → set `ctx.shutdown` event. Worker con **phải kiểm tra
  `ctx.shutdown` giữa các chunk**, không phải giữa các item.
5. Gọi `execute(ctx)`. `ctx.checkpoint(stats)` cập nhật `stats` + `heartbeat_at`.
6. `finish`: `succeeded` / `partial` (có failure đã ghi nhận, sẽ retry) / `failed`
  (lỗi fatal) / `abandoned`.
7. Nhả advisory lock (tự nhả khi session đóng, nhưng nhả tường minh cho rõ ràng).

**Exit code:** `0` = succeeded, partial, hoặc không lấy được lock. `1` = failed.
Cờ `--strict` làm `partial` cũng trả `1` (dùng cho CI, không dùng cho cron).

### 3.2 CLI chung

```
news-worker <crawl|embed> [flags]

Cửa sổ thời gian (loại trừ nhau):
  --last 3d              mặc định; chấp nhận 30m / 12h / 3d / 2w
  --since 2026-08-01 [--until 2026-08-05]

Chạy:
  --once                 mặc định
  --interval 1800        vòng lặp; chỉ là while-loop quanh run_once
  --limit N              trần số item xử lý trong một run
  --dry-run              liệt kê việc sẽ làm, không ghi gì
  --strict               partial ⇒ exit 1

crawl:
  --feeds URL[,URL...]   chỉ chạy các feed này, bỏ qua lịch next_poll_at
  --ignore-schedule      poll mọi feed enabled bất kể next_poll_at
  --max-feeds N          0 = tất cả feed tới hạn

embed:
  --reprocess            đưa lại về pending các bài trong cửa sổ (kể cả đã done)
  --model-changed        chỉ re-enqueue bài có embedding khác model hiện tại
  --batch-size 32

Quản trị:
  news-worker replay --stage embed --limit 100
  news-worker replay --failures --kind article --limit 100
  news-worker enable-feed <url>
  news-worker disable-feed <url> --reason "..."
```

**Quy tắc cửa sổ mặc định (yêu cầu của đề bài):** không truyền gì ⇒ `--last 3d`. Lọc
theo `event_at` của entry.

**Entry không có ngày đăng:** lọt qua cửa sổ mặc định (coi như mới), **bị loại khỏi cửa
sổ tường minh** (`--since`/`--until`) vì không có cách khẳng định nó thuộc khoảng đó.
Đếm riêng và log số lượng — nếu tỉ lệ này cao thì backfill theo cửa sổ gần như vô nghĩa
và cần biết điều đó sớm.

### 3.3 Giới hạn thật của backfill — phải nói rõ trước khi implement


| Kiểu                                  | Lệnh                            | Có làm được không                             |
| ------------------------------------- | ------------------------------- | --------------------------------------------- |
| Re-process bài đã có                  | `embed --reprocess --since ...` | Có, mọi khoảng thời gian                      |
| Re-discover bài mới trong quá khứ gần | `crawl --last 3d`               | Có                                            |
| Re-discover quá khứ sâu               | `crawl --since 2026-01-01`      | **Không** — feed chỉ giữ 10–50 entry gần nhất |


Vì vậy discovery viết sau port `DiscoverySource`
(`discover(window, sources) -> AsyncIterator[DiscoveredEntry]`). Tuần 1 chỉ có
`RssDiscovery` bọc `RssFeedSearcher` sẵn có. Backfill sâu cần adapter sitemap/CDX/GDELT
— tuần 4, cắm vào cùng port, `CrawlWorker` không phải sửa.

Lệnh backfill sâu vẫn chạy được và trả về gần như rỗng. Worker phải **log WARNING rõ
ràng** khi `window_start` cũ hơn 7 ngày với `RssDiscovery`, để không ai tưởng nó hỏng.

### 3.4 Luồng `CrawlWorker.execute`

```
1. retry: lấy crawl_failures tới hạn (dead_lettered_at IS NULL), bounded --limit
2. chọn feed:  --feeds  →  danh sách tường minh
               ngược lại → WHERE disabled_at IS NULL AND next_poll_at <= now()
                           ORDER BY next_poll_at LIMIT max_feeds
3. mỗi nhóm max_concurrent_feeds:
     conditional GET (If-None-Match / If-Modified-Since từ feed_state)
       304  → feed_state.next_poll_at += interval; skip, không parse
       lỗi  → consecutive_failures++; backoff; ≥10 lần ⇒ disabled_at = now()
       200  → parse entries
     lọc theo cửa sổ thời gian
     lọc URL đã có   ← normalize_url(entry_url) IN (SELECT url, url_original FROM raw_articles)
     crawl theo chunk crawl_batch_size
     persist chunk: upsert raw_articles + upsert processing_jobs(stage='embed')
     ctx.checkpoint()
     cập nhật feed_state: etag, last_modified, last_success_at,
                          last_entry_published_at, consecutive_failures=0, next_poll_at
4. kiểm tra ctx.shutdown giữa các chunk
```

**Bước "lọc URL đã có" là thay đổi có giá trị nhất trong luồng này.** Hiện tại entry
được crawl bằng headless browser rồi mới bị loại ở bước persist vì trùng URL — toàn bộ
chi phí đắt nhất bị đốt vô ích, mỗi chu kỳ 30 phút, cho gần như toàn bộ entry của feed.
Đối chiếu cả `url` lẫn `url_original` vì URL trong feed có thể khác URL cuối sau redirect.

**Bước "feed rotation" hiện tại biến mất.** `RssFeedSearcher._feed_offset` là state
trong bộ nhớ, **mất khi restart** trừ khi truyền `--feed-offset` bằng tay. Thay bằng
`ORDER BY feed_state.next_poll_at` — bền vững, tự cân bằng, và không cần biến môi trường
`RSS_FEED_OFFSET` nữa.

### 3.5 Luồng `EmbedWorker.execute`

```
1. reclaim: UPDATE processing_jobs SET state='pending'
            WHERE stage='embed' AND state='running' AND lease_until < now()
2. --reprocess/--model-changed: re-enqueue theo cửa sổ trước khi vào vòng lặp
3. lặp tới khi hết pending hoặc chạm --limit hoặc ctx.shutdown:
     claim(batch_size)  → UPDATE ... SET state='running', lease_until=now()+5min,
                          locked_by=:run_id
                          WHERE (article_id,stage) IN (
                            SELECT ... WHERE stage='embed' AND state='pending'
                              AND available_at <= now()
                            ORDER BY available_at LIMIT :n FOR UPDATE SKIP LOCKED)
                          RETURNING article_id
                          -- COMMIT NGAY, không giữ lock suốt lúc embed
     build text  →  title + "\n\n" + first_paragraphs(content, 1200)
     embed batch
     upsert article_embeddings ON CONFLICT (article_id) DO UPDATE
     mark done
     ctx.checkpoint()
```

**Xử lý lỗi (poison-pill isolation):** batch fail ⇒ **thử lại từng item một**. Item nào
fail riêng thì `attempts++`, `available_at = now() + backoff+jitter`; `attempts >= 5` ⇒
`state='dead_letter'`, `last_error` giữ nguyên văn lỗi. Một bài hỏng không được kéo cả
batch 32 bài xuống.

**Bỏ qua khi không cần:** nếu `article_embeddings.content_hash = raw_articles.content_hash`
và `model` trùng model hiện tại ⇒ đánh `done` luôn, không embed lại.

---



## 4. Schema mục tiêu cuối tuần 1

`backend/adapters/storage/schema.py` là source of truth. Sau khi sửa, chạy đúng quy
trình autogenerate trong `CLAUDE.md`.

### 4.1 `sources` (mới)

```python
sources = Table(
    "sources", metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("slug", Text, nullable=False),          # "theverge.com"
    Column("name", Text, nullable=False),
    Column("homepage", Text),
    Column("default_domain", Text),                # gợi ý domain taxonomy, tuần 2
    Column("active", Boolean, nullable=False, server_default="true"),
    Column("first_seen_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("slug", name="uq_sources_slug"),
)
```

`slug` = hostname đã bỏ tiền tố `www.`/`m.`/`amp.`. Tạo lazy khi gặp bài đầu tiên,
một lần cho mỗi batch (`ON CONFLICT (slug) DO NOTHING`), **không** gọi per-article.

### 4.2 `raw_articles` (mở rộng)

```python
Column("source_id", BigInteger, ForeignKey("sources.id")),
Column("url_original", Text),        # URL trước normalize — vừa để đo L1, vừa để pre-dedup
Column("lang", Text),
Column("event_at", DateTime(timezone=True),
       Computed("coalesce(published_at, crawled_at)", persisted=True), nullable=False),
Column("search_tsv", TSVECTOR,
       Computed("to_tsvector('english', coalesce(title,'') || ' ' || left(content, 200000))",
                persisted=True)),
```

```python
Index("ix_raw_articles_event_at_brin", raw_articles.c.event_at, postgresql_using="brin")
Index("ix_raw_articles_search_tsv", raw_articles.c.search_tsv, postgresql_using="gin")
Index("ix_raw_articles_source_event", raw_articles.c.source_id, raw_articles.c.event_at)
Index("ix_raw_articles_url_original", raw_articles.c.url_original)   # pre-crawl dedup
```

Giữ nguyên `UNIQUE(url)` và `content_hash` BYTEA. Giữ tên bảng `raw_articles`.
`simhash` dời sang tuần 2 cùng dedup L2.

`left(content, 200000)` **bắt buộc:** tsvector giới hạn 1MB, bài dài sẽ làm INSERT fail.

### 4.3 `worker_runs` (đổi tên từ `ingest_runs`)

```python
worker_runs = Table(
    "worker_runs", metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("worker", Text, nullable=False),        # 'crawl' | 'embed'
    Column("trigger", Text, nullable=False),       # 'scheduled' | 'manual' | 'backfill'
    Column("status", Text, nullable=False),        # running|succeeded|partial|failed|abandoned
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("heartbeat_at", DateTime(timezone=True), nullable=False),
    Column("window_start", DateTime(timezone=True)),
    Column("window_end", DateTime(timezone=True)),
    Column("params", JSONB, nullable=False, server_default="{}"),
    Column("stats", JSONB, nullable=False, server_default="{}"),
    Column("error_message", Text),
)
Index("ix_worker_runs_worker_started", worker_runs.c.worker, worker_runs.c.started_at.desc())
Index("ix_worker_runs_running", worker_runs.c.heartbeat_at,
      postgresql_where=text("status = 'running'"))
```

`ingest_runs` hiện có 8 cột counter cứng, toàn bộ mang tính crawl. Embed worker có bộ
counter khác hẳn. Chuyển sang `stats` JSONB (`IngestRunStats` serialize bằng `asdict`),
giữ `worker_runs` là **một** nơi trả lời "cái gì đã chạy, khi nào, kết quả ra sao".

**Data migration:** `ALTER TABLE ingest_runs RENAME TO worker_runs`, thêm cột mới,
`UPDATE ... SET stats = jsonb_build_object('feeds_processed', feeds_processed, ...)`,
`worker = 'crawl'`, `trigger = 'scheduled'`, `heartbeat_at = coalesce(finished_at, started_at)`,
rồi drop 8 cột cũ. Bảng thuần vận hành, rủi ro thấp — nhưng vẫn phải review kỹ phần
autogenerate sinh ra cho rename (CLAUDE.md).

### 4.4 `feed_state` (mới)

```python
feed_state = Table(
    "feed_state", metadata,
    Column("feed_url", Text, primary_key=True),
    Column("source_label", Text),
    Column("etag", Text),
    Column("last_modified", Text),                 # giữ nguyên dạng header, không parse
    Column("last_polled_at", DateTime(timezone=True)),
    Column("last_success_at", DateTime(timezone=True)),
    Column("last_entry_published_at", DateTime(timezone=True)),
    Column("consecutive_failures", Integer, nullable=False, server_default="0"),
    Column("next_poll_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("disabled_at", DateTime(timezone=True)),
    Column("disabled_reason", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
Index("ix_feed_state_next_poll", feed_state.c.next_poll_at,
      postgresql_where=text("disabled_at IS NULL"))
```

Đây là bảng trả lời cả ba câu hỏi của đề bài ở tầng feed: retry (`consecutive_failures`

- `next_poll_at`), tiếp tục lần sau (`last_entry_published_at`, `etag`), bỏ qua vĩnh
viễn (`disabled_at`).

Seed từ OPML/TSV ở đầu mỗi run: feed có trong file mà chưa có row ⇒ insert với
`next_poll_at = now()`. Feed biến mất khỏi file ⇒ **không xoá row**, chỉ ngừng poll —
giữ lịch sử.

### 4.5 `processing_jobs` (mới)

```python
processing_jobs = Table(
    "processing_jobs", metadata,
    Column("article_id", BigInteger, ForeignKey("raw_articles.id", ondelete="CASCADE"),
           nullable=False),
    Column("stage", Text, nullable=False),         # 'embed' | 'cluster' | 'classify'
    Column("state", Text, nullable=False, server_default="pending"),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("available_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("lease_until", DateTime(timezone=True)),
    Column("locked_by", Text),
    Column("last_error", Text),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    PrimaryKeyConstraint("article_id", "stage", name="pk_processing_jobs"),
)
Index("ix_processing_jobs_ready", processing_jobs.c.stage, processing_jobs.c.available_at,
      postgresql_where=text("state = 'pending'"))
Index("ix_processing_jobs_leased", processing_jobs.c.lease_until,
      postgresql_where=text("state = 'running'"))
```

Bảng chung cho mọi stage, không phải cột `embed_state` trên `raw_articles`. Tuần 2–3
thêm `cluster` và `classify` chỉ là thêm giá trị `stage`, không cần migration.

Partial index là bắt buộc: chỉ số hàng `pending` mới nằm trong index, nên index của hàng
đợi luôn nhỏ dù bảng có hàng triệu row `done`.

### 4.6 `article_embeddings` (mới)

```python
article_embeddings = Table(
    "article_embeddings", metadata,
    Column("article_id", BigInteger, ForeignKey("raw_articles.id", ondelete="CASCADE"),
           primary_key=True),
    Column("model", Text, nullable=False),         # "all-MiniLM-L6-v2"
    Column("dim", Integer, nullable=False),        # 384
    Column("content_hash", LargeBinary, nullable=False),
    Column("embedding", Vector(384), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
```

HNSW tạo **thủ công trong migration** (`op.execute`), autogenerate không sinh được:

```sql
CREATE INDEX ix_article_embeddings_hnsw ON article_embeddings
  USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
```

`content_hash` cho phép phát hiện embedding cũ khi bài được re-crawl và nội dung đổi;
`model`/`dim` cho phép đổi model mà không phải drop bảng.

### 4.7 `crawl_failures` (bổ sung)

```python
Column("dead_lettered_at", DateTime(timezone=True)),
```

`due_failures()` thêm `AND dead_lettered_at IS NULL`. `attempt_count >= 5` ⇒ set
`dead_lettered_at` thay vì tiếp tục đẩy `next_retry_at`.

---



## 5. Task breakdown (16h)



### T1 — pgvector + Alembic hiểu kiểu `Vector` (2h)

1. `deployments/docker-compose.yml`: `postgres:16-alpine` → `pgvector/pgvector:pg16`.
  Cùng PG16 nên volume `news_radar_postgres_data` tương thích, dữ liệu không mất.
2. `pyproject.toml`: thêm `pgvector>=0.3,<1`, `fastembed>=0.4,<1`.
3. [migrations/env.py](../backend/migrations/env.py) hiện **chưa có** `render_item` ⇒
  autogenerate sẽ sinh `sa.NullType()` cho cột `Vector`. Thêm:
   truyền `render_item=render_item` vào **cả hai** `context.configure(...)`.
4. `op.execute("CREATE EXTENSION IF NOT EXISTS vector")` chèn tay vào đầu revision đã
  autogenerate — autogenerate không biết extension. Đây là ngoại lệ hợp lệ duy nhất
   với quy tắc "không sửa file autogenerate": nội dung sinh ra giữ nguyên, chỉ thêm
   một dòng lên trước.
5. `uv run alembic upgrade head && uv run alembic check`.

**Đây là task dễ tốn thời gian nhất tuần 1.** Nếu `alembic check` báo diff không dứt với
cột Vector, kiểm tra `render_item` trước khi nghi ngờ thứ khác.

### T2 — `sources` + định danh nguồn (2h)

1. Thêm `sources` + `raw_articles.source_id`.
2. `services/raw_storage.py`: thêm `source_slug(normalized_url) -> str`. Giữ
  `article_source()` như hiện tại cho cột `source` hiển thị.
3. `PostgresRawStorage._ensure_sources(connection, slugs) -> dict[str, int]`, gọi một
  lần cho mỗi batch trong `apply_batch` trước vòng upsert.
4. Data migration: backfill `sources` từ hostname của `raw_articles.url` hiện có, rồi
  UPDATE `source_id`.
5. Test: unit test `source_slug` (www/m/amp/port/subdomain); integration test khẳng
  định hai bài cùng host chia sẻ một `source_id` và chạy hai batch liên tiếp không
   sinh row `sources` trùng.



### T3 — Mở rộng `raw_articles` + nâng dedup L1 (2h)

1. Thêm cột ở §4.2 + index.
2. Nâng `normalize_url()`. Hiện chỉ bỏ `utm_*`, `fbclid`, `gclid`. Bổ sung:
  - sort query params (thứ tự khác nhau ⇒ cùng URL)
  - bỏ trailing slash (trừ path gốc `/`)
  - bỏ thêm `mc_cid`, `mc_eid`, `igshid`, `ref`, `ref_src`, `_hsenc`
  - bỏ hậu tố AMP (`/amp`, `/amp/`, `.amp`)
  - hạ prefix `m.`/`amp.` của hostname
3. Lưu URL gốc vào `url_original`.
4. `lang`: [crawler.py:79](../backend/adapters/retrieve_news_data/crawler.py:79) đang
  **hardcode** `language=None`; sửa để đọc `raw_meta.get("language")`. Không thêm thư
   viện detect ngôn ngữ trong tuần 1.
5. Test: bảng tham số ≥15 cặp input/expected cho `normalize_url`, gồm cả case **không**
  được gộp (`?page=2`, `?id=`).



### T4 — `workers/base.py` + CLI + `worker_runs` (3h)

1. Rename `ingest_runs` → `worker_runs` + data migration (§4.3).
2. `workers/window.py`: `RunWindow`, parse `3d`/`12h`/`2w`/ISO. `--last` và `--since`
  loại trừ nhau. Mặc định `--last 3d`.
3. `workers/base.py`: `Worker` ABC, `RunContext` (`run_id`, `window`, `shutdown`,
  `checkpoint()`), advisory lock, reaper, signal handler, exit code.
4. `workers/cli.py` + entry point `news-worker` trong `pyproject.toml`. Giữ
  `news-service` như alias in ra deprecation warning rồi gọi `crawl --interval`.
5. Test: advisory lock (instance thứ hai thoát 0); reaper đánh dấu `abandoned` đúng;
  parse cửa sổ; SIGTERM giữa hai chunk ⇒ run `partial`, chunk đã commit còn nguyên.



### T5 — `CrawlWorker` + `feed_state` (3h)

1. Thêm `feed_state` + seed từ OPML/TSV.
2. `DiscoverySource` port + `RssDiscovery` bọc `RssFeedSearcher` sẵn có.
3. Conditional GET trong `_fetch_rss`: truyền `etag`/`last_modified`, xử lý 304.
4. Lọc cửa sổ thời gian + lọc URL đã có (đối chiếu `url` và `url_original`).
5. Chọn feed theo `next_poll_at`; cập nhật `feed_state` sau mỗi feed;
  `consecutive_failures >= 10` ⇒ `disabled_at`.
6. Đổi User-Agent: hiện là
  `"Mozilla/5.0 (compatible; GoogleNewsBot/1.0; +https://news.google.com)"` — **giả
   dạng bot của Google**. Đổi thành UA của project kèm URL liên hệ.
7. Test: 304 ⇒ không parse, `next_poll_at` được đẩy; entry ngoài cửa sổ bị loại; entry
  không có ngày lọt cửa sổ mặc định nhưng bị loại khỏi cửa sổ tường minh; URL đã có
   không được crawl lại; feed fail 10 lần bị disable.



### T6 — DLQ + replay + jitter (1h)

1. `crawl_failures.dead_lettered_at` + ngưỡng `MAX_FAILURE_ATTEMPTS=5`.
2. Jitter trong `PostgresRawStorage.retry_delay()`. Hiện là
  `min(300 · 2**(n-1), 86400)` **không có jitter** ⇒ nhiều failure cùng lúc sẽ retry
   đồng loạt. Dùng full jitter `random.uniform(0, delay)`; inject `random` để test vẫn
   deterministic (hàm này đang có test).
3. `news-worker replay` / `enable-feed` / `disable-feed`.



### T7 — Diagnose failure, crawl better



### T8 — `EmbedWorker` + `processing_jobs` + HNSW (3h)

1. Thêm `processing_jobs` + `article_embeddings` + HNSW (SQL thủ công).
2. `workers/queue.py`: `JobQueue` port + `PostgresJobQueue` (claim/complete/fail/reclaim).
3. `adapters/embedding/fastembed_embedder.py` + port `Embedder`.
4. **Quy tắc text đầu vào — spec chưa nói, phải chốt:** MiniLM cắt ở 256 token
  (~1000–1200 ký tự). Embed toàn bộ Markdown là vô nghĩa. Dùng  `f"{title}\n\n{first_paragraphs(content, max_chars=1200)}"`, trong đó  `first_paragraphs` bỏ code fence và dòng chỉ chứa link. Đặt ở `services/text.py`.
5. `EmbedWorker`: reclaim → claim → embed → upsert → mark done, poison-pill isolation.
6. `apply_batch` enqueue `processing_jobs(stage='embed')` khi ghi hoặc khi content đổi.
7. Test: `FakeEmbedder` (không tải model, unit test không cần mạng); lease hết hạn ⇒
  job về `pending`; batch fail ⇒ chỉ item hỏng bị tính attempt; content không đổi ⇒  không embed lại. Test dùng model thật đánh dấu `@pytest.mark.integration`.

---



## 6. Thứ tự migration

Bốn revision. Mỗi revision phải `alembic upgrade head` + `alembic check` sạch trước khi
sang bước sau.


| #   | Nội dung                                                                                           | Task   |
| --- | -------------------------------------------------------------------------------------------------- | ------ |
| 1   | `CREATE EXTENSION vector` (chèn tay) + `sources` + `raw_articles.source_id` + backfill             | T1, T2 |
| 2   | Cột mở rộng `raw_articles` (`url_original`, `lang`, `event_at`, `search_tsv`) + index              | T3     |
| 3   | Rename `ingest_runs` → `worker_runs` + `stats` JSONB + data migration + `feed_state`               | T4, T5 |
| 4   | `processing_jobs` + `article_embeddings` + HNSW (`op.execute`) + `crawl_failures.dead_lettered_at` | T6, T7 |


Sau revision 4, backfill một lần:

```bash
psql -c "INSERT INTO processing_jobs (article_id, stage) SELECT id, 'embed' FROM raw_articles
         ON CONFLICT DO NOTHING;"
news-worker embed --once
```

---



## 7. Vận hành sau tuần 1

```cron
*/30 * * * *  cd /srv/news-radar && news-worker crawl --once >> /var/log/news-radar/crawl.log 2>&1
*/5  * * * *  cd /srv/news-radar && news-worker embed --once >> /var/log/news-radar/embed.log 2>&1
```

Cron chồng lịch là vô hại nhờ advisory lock. `--interval` chỉ dùng khi dev — cron cho
process isolation tốt hơn (một run rò bộ nhớ không ảnh hưởng run sau).

---



## 8. Best practice còn thiếu — đưa vào tuần 1 hay hoãn

Rà soát crawl worker và data-processing worker theo chuẩn thông thường. Cột cuối là
quyết định.


| Hạng mục                            | Hiện trạng                                         | Quyết định                                   |
| ----------------------------------- | -------------------------------------------------- | -------------------------------------------- |
| Dedup trước khi crawl               | Crawl xong mới loại ở persist                      | **T5** — khoản tiết kiệm lớn nhất            |
| Conditional GET (ETag)              | GET đầy đủ 422 feed mỗi 30 phút                    | **T5**                                       |
| Advisory lock                       | Không có; CLAUDE.md ghi "chỉ chạy một instance"    | **T4**                                       |
| Graceful SIGTERM                    | Chỉ bắt `KeyboardInterrupt`/`CancelledError`       | **T4**                                       |
| Heartbeat + reap run treo           | Không có; run chết kẹt `running` mãi               | **T4**                                       |
| Lease thay vì giữ `FOR UPDATE`      | Chưa có queue                                      | **T6**                                       |
| Poison-pill isolation               | Chưa có                                            | **T6**                                       |
| DLQ + replay                        | Retry vô hạn, không có trạng thái cuối             | **T7**                                       |
| Jitter                              | Không có                                           | **T7**                                       |
| User-Agent trung thực               | Giả dạng `GoogleNewsBot`                           | **T5** — 5 phút, sửa luôn                    |
| Cửa sổ thời gian                    | Không có khái niệm                                 | **T4/T5**                                    |
| Rate limit **theo host**            | Chỉ có giới hạn toàn cục `MAX_CONCURRENT_CRAWLS=2` | Tuần 2                                       |
| robots.txt                          | Không kiểm tra                                     | Tuần 2 — bật `check_robots_txt` của crawl4ai |
| Circuit breaker theo host           | Không có                                           | Tuần 2                                       |
| Tái dùng browser                    | Mở/đóng `AsyncWebCrawler` mỗi chunk 10 bài         | Tuần 2 — đo trước khi sửa                    |
| Decode cache bền vững               | Chỉ trong bộ nhớ, mất mỗi lần chạy                 | Tuần 2                                       |
| Adaptive polling theo tần suất feed | Không có                                           | Tuần 3 — `feed_state` đã đủ cột              |
| Prometheus `/metrics`               | Không có                                           | Tuần 3 — số liệu đã nằm sẵn trong bảng       |
| Sitemap / CDX discovery             | Không có                                           | Tuần 4 — cắm vào `DiscoverySource`           |


Ba hạng mục hoãn sang tuần 2 (rate limit theo host, robots.txt, circuit breaker) đi
chung một chỗ trong code và nên làm cùng nhau; tách ra làm hai lần sẽ tốn hơn.

---



## 9. Việc phải quyết trước tuần 2

Không chặn tuần 1, nhưng để càng lâu càng đắt:

1. **Nguồn dữ liệu vs taxonomy.** `engineering_blogs.opml` + `Tech.opml` đều là blog kỹ
  thuật. Bổ sung feed finance/health, hay thu hẹp taxonomy về beat kỹ thuật?
2. **Granularity của trend.** Với 422 feed và `MAX_CONCURRENT_CRAWLS=2`, một vòng phủ
  hết feed mất nhiều giờ. Bucket theo giờ sẽ đo chính scheduler của mình chứ không
   phải nhịp tin tức. Đề xuất: bucket theo `event_at`, granularity `day`.
3. **Baseline 28 ngày không kịp.** §7.2 cần rolling median 28 ngày cùng day-of-week;
  backfill GDELT lại xếp ở tuần 4. Phải chạy `news-worker crawl` liên tục **từ ngày
   đầu tuần 1**, và cân nhắc kéo backfill lên tuần 2.
4. `POST /alerts` **mâu thuẫn quy tắc read-only của API** (`CLAUDE.md`). Tách router có
  quyền ghi trên bảng riêng (`saved_queries`), và ghi rõ ràng buộc read-only chỉ áp
   dụng cho `raw_articles`/`crawl_failures`/`worker_runs`.

---



## 10. Rủi ro của riêng tuần 1


| Rủi ro                                              | Dấu hiệu                                                   | Xử lý                                                                          |
| --------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Alembic không hiểu `Vector`                         | revision sinh `sa.NullType()`, `alembic check` không sạch  | `render_item` ở T1; nếu vẫn kẹt, khai báo cột Vector bằng `op.execute`         |
| Rename `ingest_runs` sinh migration hỏng            | autogenerate sinh DROP + CREATE thay vì RENAME             | Review tay revision 3 trước khi apply; viết `op.alter_table` tường minh        |
| Đổi image sang `pgvector/pgvector:pg16`             | container không start                                      | Cùng PG16 nên volume tương thích; vẫn `pg_dump` trước khi đổi                  |
| Lease quá ngắn ⇒ job bị xử lý hai lần               | cùng `article_id` embed hai lần trong log                  | Upsert `article_embeddings` là idempotent nên vô hại; nới lease nếu batch chậm |
| Cửa sổ 3 ngày loại gần hết entry vì thiếu `pubDate` | `crawl_skipped_total{reason=out_of_window}` cao bất thường | Đếm riêng entry không có ngày ngay từ T5; nếu > 30% thì xem lại quy tắc §3.2   |
| `search_tsv` làm INSERT fail                        | lỗi "string is too long for tsvector"                      | `left(content, 200000)` trong Computed                                         |
| Tải model MiniLM lần đầu chậm trong CI              | test treo                                                  | `FakeEmbedder` cho unit test; model thật chỉ ở `@pytest.mark.integration`      |
| Feed bị disable oan do lỗi mạng tạm thời            | nhiều feed `disabled_at` sau một sự cố mạng                | Ngưỡng 10 lần liên tiếp (không phải 3); `news-worker enable-feed` để mở lại    |
