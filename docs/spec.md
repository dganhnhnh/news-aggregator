# News Trend Aggregator — Technical Spec

> Bản v0.1 — dùng để chỉnh sửa. Phần crawl/fetch coi như đã có sẵn, không nằm trong scope tài liệu này.

---

## 1. Mục tiêu

**Chính:** showcase kỹ năng backend hướng data-intensive — stream processing, vector search, time-series aggregation, reliability engineering, có số đo cụ thể.

**Phụ:** công cụ cá nhân phát hiện chủ đề đang nóng lên trong các lĩnh vực quan tâm (công nghệ, tài chính/macro, y tế, …).

**Định vị:** đơn vị dữ liệu của hệ thống là *story cluster* và *trend timeline*, không phải bài viết. Đây là điểm khác biệt so với RSS reader.

**Ngân sách:** ~40 giờ / 4 tuần. MVP chạy được sau tuần 2.

### Không làm

- Multi-tenant, auth phức tạp
- Train model từ đầu
- Gọi LLM cho từng bài viết
- Kafka, Elasticsearch, K8s multi-node
- Định vị như tín hiệu giao dịch (chỉ là macro narrative tracking)

---



## 2. Mô hình dữ liệu khái niệm

Ba tầng tách biệt, **không gộp vào một trường** `category`:

```
Domain/Beat  (taxonomy cố định, khai báo)     tech > semiconductors
     │
     ├─ Storyline  (chuỗi diễn biến, vòng đời tuần–tháng)
     │       │
     │       ├─ Cluster  (một sự kiện, vòng đời giờ–ngày)
     │       │      ├─ Article (nguồn A)
     │       │      ├─ Article (nguồn B)
     │       │      └─ Article (nguồn C)
     │       └─ Cluster (diễn biến tiếp theo)
```


| Tầng      | Tạo ra bằng                             | Thay đổi |
| --------- | --------------------------------------- | -------- |
| Beat      | Con người khai báo YAML                 | Hiếm     |
| Storyline | Nối cluster theo similarity + thời gian | Tự động  |
| Cluster   | Online clustering                       | Liên tục |


**Nguyên tắc:** cluster là **toàn cục và đa nhãn**, không cluster riêng theo từng lĩnh vực. Một tin có thể vừa `tech` vừa `finance`.

---



## 3. Taxonomy lĩnh vực



### 3.1 Khai báo

`taxonomy.yaml`, versioned trong git:

```yaml
domains:
  - slug: tech
    name: Công nghệ
    beats:
      - slug: semiconductors
        exemplars:
          - "chip manufacturer announces new process node"
          - "GPU shipments and foundry capacity"
          - "EUV lithography equipment export"
        keywords: [TSMC, ASML, wafer, foundry, nm process, GPU]
      - slug: ai-models
        exemplars: [...]
        keywords: [...]

  - slug: finance
    name: Tài chính / Macro
    beats:
      - slug: monetary-policy
        exemplars:
          - "central bank raises interest rates"
          - "inflation data comes in above expectations"
        keywords: [Fed, FOMC, CPI, rate hike, yield curve, ECB]
      - slug: equities
        ...

  - slug: health
    ...
```

- **Level 1 (domain):** 6–10 mục, cố định
- **Level 2 (beat):** 3–8 mục mỗi domain, có thể thêm dần
- **Level 3:** không khai báo — chính là storyline được phát hiện tự động



### 3.2 Phân loại — không cần train model

Điểm số cho mỗi cặp (article, beat) là tổ hợp tuyến tính của 3 tín hiệu:

```
score = w1 · prototype_sim + w2 · keyword_score + w3 · source_prior
```

**1. Prototype similarity (tín hiệu chính).** Mỗi beat có 5–10 câu exemplar → embed → lấy trung bình, chuẩn hóa L2 → `prototype_vector`. Sau đó `cosine(article_embedding, prototype_vector)`. Tái dùng đúng model embedding đã có, chi phí thêm gần bằng 0.

**2. Keyword score.** Đếm từ khóa khớp, chuẩn hóa theo độ dài. Precision cao, recall thấp — dùng để tăng độ tin cậy, không dùng một mình.

**3. Source prior.** `P(beat | source)` học từ lịch sử: nguồn nào thường viết về beat nào. Cập nhật hàng đêm. Có smoothing để nguồn mới không bị lệch.

**Gán nhãn:** đa nhãn với ngưỡng. Một bài có thể nhận 0, 1 hoặc nhiều beat.

- `score ≥ 0.40` → gán nhãn (hạ từ `0.55` sau lần đo 2026-08-28; xem
  [measurements.md](measurements.md) mục T6)
- Nếu không beat nào vượt ngưỡng → gán domain cha có điểm cao nhất, hoặc `uncategorized`

**Escape hatch:** khi cluster đạt ngưỡng kích thước mà không beat nào đủ tự tin, gọi LLM **một lần cho cả cluster** để phân loại. Không bao giờ gọi cho từng bài.

### 3.3 Từ article lên cluster

Nhãn của cluster = vote có trọng số từ các thành viên:

```
cluster_score(beat) = Σ (article_score(beat) × source_weight) / Σ source_weight
```

`source_weight` giảm dần nếu cùng một nguồn đóng góp nhiều bài vào cùng cluster — tránh việc một outlet spam làm lệch nhãn.

Nhãn cluster được tính lại mỗi khi có thành viên mới, hoặc theo micro-batch.

---



## 4. Kiến trúc

```
  OPML / TSV  (danh sách feed)
        │
        ▼
  ┌────────────────────────────────────────┐
  │ crawl worker      backend/workers/     │
  │  discover: RSS/Atom, conditional GET,  │
  │            cursor + trạng thái mỗi feed│
  │  filter:   cửa sổ thời gian, URL đã có │
  │  fetch:    crawl4ai, giới hạn theo host│
  │  L1 dedup: URL canonicalization        │
  └──────┬─────────────────────────────────┘
         │ upsert raw_articles
         │ + enqueue processing_jobs(stage='embed')
         ▼
  ┌────────────────────────────────────────┐
  │ Postgres = kho lưu trữ + hàng đợi      │
  │  processing_jobs: lease, attempts, DLQ │
  │  theo từng stage (SKIP LOCKED)         │
  └──────┬─────────────────────────────────┘
         ▼
  ┌────────────────────────────────────────┐
  │ embed worker      micro-batch 32–64    │
  │  MiniLM 384d, CPU (fastembed/ONNX)     │
  └──────┬─────────────────────────────────┘
         ▼  enqueue stage='cluster'
  ┌────────────────────────────────────────┐
  │ cluster worker    (tuần 2)             │
  │  online centroid match                 │
  │  multi-label beats                     │
  └──────┬─────────────────────────────────┘
         ▼
  ┌────────────────────────────────────────┐
  │ Postgres + pgvector + tsvector         │
  └──────┬─────────────────────────────────┘
    ┌────┴─────┬──────────────┬─────────────┐
    ▼          ▼              ▼             ▼
 rollup     storyline     LLM enrich      API
 worker     linker        (cluster ≥ N)   REST + SSE
    │
    ▼
 Trend engine (z-score per beat)
```

**Tại sao Postgres làm hàng đợi thay vì Redis Streams.** Bản v0.1 đặt Redis Stream
giữa crawler và pipeline. Bỏ, vì crawler hiện tại là **pull-based batch loop** chứ
không phải producer đẩy sự kiện — không có gì để đẩy vào stream mà không viết lại
producer trước. Ngoài ra `SELECT ... FOR UPDATE SKIP LOCKED` trên `processing_jobs`
cho đúng những thứ cần nói tới: at-least-once, consumer song song, visibility timeout
qua lease, dead-letter, backpressure — với 0 hạ tầng mới.

Redis Streams vẫn nằm trong kế hoạch, nhưng ở tuần 4 và như một **backend thứ hai của
cùng một interface** `JobQueue`, để benchmark hai bên cạnh nhau. Một so sánh có số đo
là câu chuyện tốt hơn hẳn "tôi dùng Redis".

**Stack:** toàn bộ Python — FastAPI, SQLAlchemy async, asyncpg, crawl4ai, fastembed.
Bỏ phương án Go: codebase hiện có đã là Python, học ngôn ngữ mới trong 40h là tự sát.

---



## 4.1 Mô hình vận hành worker

Mọi worker nằm trong `backend/workers/`, kế thừa cùng một lớp cơ sở và có chung ba
chế độ chạy. Hiện có hai worker: `crawl` và `embed`; tuần 2–3 thêm `cluster` và
`rollup` mà không đổi khung.

### 4.1.1 Ba chế độ chạy


| Chế độ               | Lệnh                                                      | Dùng khi                                          |
| -------------------- | --------------------------------------------------------- | ------------------------------------------------- |
| Một lần              | `news-worker crawl --once`                                | Cron / systemd timer (khuyến nghị cho production) |
| Định kỳ              | `news-worker crawl --interval 1800`                       | Dev, hoặc chạy như một process thường trú         |
| On-demand / backfill | `news-worker crawl --since 2026-07-01 --until 2026-07-15` | Lấp dữ liệu một khoảng thời gian cụ thể           |


Mặc định khi không chỉ định gì: **cửa sổ 3 ngày gần nhất** (`--last 3d`). Cửa sổ lọc
theo `event_at = coalesce(published_at, crawled_at)` của entry.

Cả ba chế độ đi qua **cùng một code path**. `--interval` chỉ là vòng lặp quanh
`run_once()`. Không có nhánh logic riêng cho backfill.

### 4.1.2 Giới hạn thật của backfill

Cần phân biệt hai thứ mà spec v0.1 gộp làm một:

- **Re-process** — chạy lại một stage trên các bài **đã có** trong `raw_articles`
theo cửa sổ thời gian. Hoạt động với mọi khoảng thời gian, không phụ thuộc nguồn.
`news-worker embed --since ... --reprocess`.
- **Re-discover** — tìm bài **mới** trong quá khứ. RSS/Atom **không làm được**: một
feed chỉ giữ 10–50 entry gần nhất, thường dưới 7 ngày. Đặt `--since 2026-01-01`
cho crawl worker sẽ chạy sạch nhưng gần như không trả về gì.

Vì vậy discovery được viết sau port `DiscoverySource`. Tuần 1 chỉ có `RssDiscovery`.
Backfill sâu hơn feed retention cần adapter khác (sitemap `news`/`lastmod`, Wayback
CDX, hoặc GDELT) — tuần 4, cắm vào cùng port, worker không phải sửa.

**Quy tắc entry không có ngày đăng:** rất nhiều feed thiếu `pubDate`. Entry không có
ngày được coi là "mới" và **lọt qua cửa sổ mặc định**, nhưng **bị loại khỏi cửa sổ
backfill tường minh** (khi có `--since`/`--until`) — vì không có cách nào khẳng định
nó thuộc khoảng đó.

### 4.1.3 Lịch sử chạy điều khiển hành vi

Ba bảng quyết định worker làm gì ở lần chạy sau. Không có trạng thái nào nằm trong bộ
nhớ process.


| Bảng              | Trả lời câu hỏi                                                                     |
| ----------------- | ----------------------------------------------------------------------------------- |
| `worker_runs`     | Lần chạy trước kết thúc thế nào? Có run nào đang treo không?                        |
| `feed_state`      | Feed nào tới hạn poll? Feed nào đã hỏng liên tục và phải tắt? Đã đọc tới entry nào? |
| `processing_jobs` | Item nào chờ xử lý / đang giữ lease / đã hết số lần thử?                            |
| `crawl_failures`  | URL nào tới hạn retry? URL nào đã dead-letter?                                      |


Vòng đời một đơn vị công việc:

```
pending ──claim──> running ──ok──> done
   ▲                  │
   │                  ├──lỗi, attempts < N──> pending  (next_retry_at = now + backoff + jitter)
   │                  └──lỗi, attempts ≥ N──> dead_letter   ← chỉ thoát ra bằng tay
   │
   └── lease hết hạn (worker chết giữa chừng) → quay lại pending
```

`dead_letter` là trạng thái **cuối, im lặng**: worker không bao giờ tự lấy lại. Thoát
ra bằng `news-worker replay --stage embed --limit 100` hoặc
`news-worker enable-feed <url>`.

### 4.1.4 Bất biến chung của mọi worker

1. **Một instance mỗi worker.** `pg_try_advisory_lock(hashtext('worker:<name>'))` ở
  đầu `run_once()`; không lấy được lock ⇒ log và thoát 0. Nhờ vậy cron chồng lịch là
   vô hại, và ràng buộc "chỉ chạy một `news-service`" hiện tại biến mất.
2. **Heartbeat.** `worker_runs.heartbeat_at` cập nhật mỗi checkpoint. Run có
  `status='running'` mà heartbeat cũ hơn 3× interval bị đánh dấu `abandoned` ở lần
   khởi động sau.
3. **Graceful shutdown.** SIGTERM/SIGINT ⇒ ngừng nhận việc mới, hoàn tất chunk đang
  chạy, ghi `worker_runs`, thoát 0. Không huỷ giữa chừng một chunk đã crawl xong
   nhưng chưa ghi.
4. **Checkpoint theo chunk.** Mỗi chunk commit độc lập; dừng giữa chừng không làm mất
  chunk đã xong. (Đã là quy tắc sẵn có của repo.)
5. **Idempotent.** Chạy lại cùng cửa sổ thời gian không tạo bản ghi trùng và không
  ghi đè dữ liệu bằng giá trị rỗng.
6. **Backoff có jitter.** `min(base · 2^(n−1), 24h)` rồi `random.uniform(0, delay)` —
  full jitter, tránh cả đàn failure retry đồng loạt.

---



## 5. Schema

```sql
-- Nguồn — slug = hostname của URL bài viết cuối cùng, KHÔNG phải tên feed
sources(id, slug, name, homepage, default_domain, active, first_seen_at,
  UNIQUE(slug))

-- Bài viết — KHÔNG partition (xem ghi chú bên dưới)
raw_articles(
  id, source_id, url, url_original, title, content, content_hash BYTEA,
  published_at, crawled_at, updated_at,
  event_at        GENERATED coalesce(published_at, crawled_at),  -- NOT NULL
  search_tsv      GENERATED to_tsvector('english', title || left(content, 200000)),
  lang,
  UNIQUE(url)
)

article_embeddings(article_id PK, model, dim, content_hash, embedding vector(384),
  created_at)
-- index: HNSW, vector_cosine_ops

-- ── Vận hành ────────────────────────────────────────────────────────────────
-- Lịch sử chạy của mọi worker (thay cho ingest_runs)
worker_runs(
  id, worker, trigger, status, started_at, finished_at, heartbeat_at,
  window_start, window_end, params JSONB, stats JSONB, error_message
)
-- worker : 'crawl' | 'embed' | 'cluster' | 'rollup'
-- trigger: 'scheduled' | 'manual' | 'backfill'
-- status : 'running' | 'succeeded' | 'partial' | 'failed' | 'abandoned'

-- Con trỏ + sức khoẻ của từng feed: quyết định poll gì ở lần sau
feed_state(
  feed_url PK, source_label,
  etag, last_modified,                -- conditional GET
  last_polled_at, last_success_at, last_entry_published_at,
  consecutive_failures, next_poll_at,
  disabled_at, disabled_reason,       -- bỏ qua vĩnh viễn tới khi can thiệp tay
  updated_at
)

-- Hàng đợi chung cho mọi stage xử lý sau khi bài đã nằm trong raw_articles
processing_jobs(
  article_id, stage, state, attempts, lease_until, locked_by,
  available_at, last_error, updated_at,
  PRIMARY KEY (article_id, stage)
)
-- stage: 'embed' | 'cluster' | 'classify'
-- state: 'pending' | 'running' | 'done' | 'dead_letter'

-- Lỗi ở tầng discovery/fetch — có trước, nay bổ sung dead-letter
crawl_failures(
  failure_kind, url, source, title, published_at,
  error_type, error_message, attempt_count,
  first_failed_at, last_failed_at, next_retry_at, dead_lettered_at,
  PRIMARY KEY (failure_kind, url)
)

-- Taxonomy
topics(id, slug, parent_id, level, display_name)
topic_prototypes(topic_id, embedding vector(384), exemplars TEXT[])
article_topics(article_id, topic_id, score REAL, method)  -- đa nhãn
cluster_topics(cluster_id, topic_id, score REAL)          -- suy ra từ thành viên

-- Cluster & storyline
clusters(
  id, storyline_id, centroid vector(384),
  first_seen_at, last_seen_at, n_articles, n_distinct_sources,
  headline, summary, summary_generated_at, status
)
cluster_members(cluster_id, article_id, similarity, added_at)
storylines(id, centroid vector(384), first_seen_at, last_seen_at, label)

-- Time-series rollup
topic_buckets(
  topic_id, bucket_start, granularity,   -- 'hour' | 'day'
  n_clusters, n_articles, n_distinct_sources,
  share REAL,        -- tỉ trọng trên tổng volume cùng bucket
  z_score REAL,
  PRIMARY KEY (topic_id, granularity, bucket_start)
)
```

**Tại sao bỏ partition** `raw_articles`**.** Bản v0.1 định partition theo `published_at`
theo tháng. Không làm được:

- `published_at` **nullable** (rất nhiều RSS entry thiếu `pubDate`) và **mutable**
(re-crawl có thể bổ sung) ⇒ row phải nhảy partition.
- Postgres bắt partition key phải nằm trong mọi UNIQUE constraint ⇒ `UNIQUE(url)` phải
thành `UNIQUE(url, published_at)`, phá vỡ bất biến "định danh bài = normalized final
URL" và làm `ON CONFLICT (url)` upsert sai.
- Partitioned table làm `alembic --autogenerate` sinh migration hỏng.
- Quy mô không cần: ~vài nghìn bài/ngày. Partition có giá trị từ ~10–50M row.

Thay bằng cột sinh `event_at` (NOT NULL, immutable) + BRIN. Nếu tuần 4 muốn có câu
chuyện partitioning để đo đạc thì partition `topic_buckets` — append-only, key không
đổi, không có unique constraint xung đột.

**Ghi chú index:**

- `raw_articles(event_at)` BRIN — quét theo cửa sổ thời gian, rẻ hơn btree nhiều
- `raw_articles(source_id, event_at)` — đếm `n_distinct_sources` theo bucket
- `raw_articles USING gin(search_tsv)` — full-text
- `article_embeddings USING hnsw (embedding vector_cosine_ops)`
- **Không** đánh HNSW lên `clusters.centroid`: centroid bị UPDATE liên tục, mà số
cluster active trong 72h chỉ vài trăm — seq scan nhanh hơn ANN
- `processing_jobs(stage, available_at)` partial `WHERE state = 'pending'` — index của
hàng đợi, phải nhỏ
- `feed_state(next_poll_at)` partial `WHERE disabled_at IS NULL`
- `article_topics(topic_id, article_id)` — composite, để lọc theo beat
- `topic_buckets(topic_id, granularity, bucket_start DESC)` — phục vụ trend chart

---



## 6. Clustering

**Thuật toán:** online centroid matching trong cửa sổ trượt.

```
với mỗi article đã embed:
  candidates ← ANN search trên clusters
                WHERE last_seen_at > now() - 72h
                LIMIT 20
  best ← argmax cosine(article, candidate.centroid)

  nếu cosine(best) ≥ 0.75  → thêm vào cluster, cập nhật centroid (running mean)
  ngược lại                → tạo cluster mới
```

**Merge định kỳ:** mỗi giờ, tìm các cặp cluster có `cosine(centroid) ≥ 0.85` và thời gian chồng lấn → gộp. Xử lý trường hợp hai cluster về cùng sự kiện được tạo ra do thứ tự đến của bài viết.

**Storyline linking:** khi tạo cluster mới, so centroid với các storyline đang hoạt động trong 30 ngày. Nếu `cosine ≥ 0.65` → gắn vào storyline đó, ngược lại tạo storyline mới.

**Tham số cần tune** (ghi lại giá trị đã thử và kết quả — đây là chất liệu cho phần đánh giá):


| Tham số               | Giá trị khởi đầu |
| --------------------- | ---------------- |
| Ngưỡng gán cluster    | 0.75             |
| Ngưỡng semantic dedup | 0.92             |
| Ngưỡng merge          | 0.85             |
| Ngưỡng storyline      | 0.65             |
| Cửa sổ cluster        | 72h              |
| Cửa sổ storyline      | 30 ngày          |


**Đánh giá:** gán nhãn tay ~100–150 bài thành cluster đúng, đo precision/recall/F1 khi quét ngưỡng. Cần thiết để chứng minh tham số không phải chọn bừa.

---



## 7. Trend engine



### 7.1 Chỉ số volume

**Không dùng số bài viết thô.** Ba metric, ưu tiên theo thứ tự:

1. `n_distinct_sources` — bao nhiêu nguồn khác nhau nói về beat này. Bền vững trước việc một outlet spam.
2. `n_clusters` — bao nhiêu câu chuyện riêng biệt.
3. `share` — tỉ trọng trên tổng volume cùng bucket. **Bắt buộc** vì whitelist thay đổi theo thời gian; nếu thêm 15 feed y tế thì volume y tế thô sẽ bùng nổ giả.



### 7.2 Burst detection

Baseline tính **riêng cho từng beat**, vì volume giữa các lĩnh vực chênh nhau cả bậc và tính mùa vụ khác nhau.

```
baseline ← rolling median 28 ngày của share(beat), cùng day-of-week
mad      ← median absolute deviation
z        ← (share_hiện_tại − baseline) / (1.4826 × mad)

burst nếu z ≥ 3.0 VÀ n_distinct_sources ≥ 3
```

Dùng median/MAD thay vì mean/stddev vì phân bố có đuôi dài và chính các burst trước đó sẽ làm hỏng mean.

`n_distinct_sources ≥ 3` là điều kiện chặn, tránh cảnh báo khi một nguồn đăng liên tiếp.

### 7.3 Nhãn trend

Gán cho storyline, so sánh cửa sổ hiện tại với cửa sổ trước:


| Nhãn      | Điều kiện                                    |
| --------- | -------------------------------------------- |
| Emergence | Không khớp storyline nào trước đó            |
| Growth    | size > 1.5× kỳ trước và ≥ ngưỡng tối thiểu   |
| Decay     | size < 0.5× kỳ trước                         |
| Drift     | `1 − cosine(centroid_t−1, centroid_t) ≥ 0.2` |
| Stable    | còn lại                                      |


Nhãn Drift đáng chú ý — nó bắt được trường hợp câu chuyện vẫn nóng nhưng góc kể đã chuyển hướng.

### 7.4 Rollup

Cron mỗi giờ, incremental (chỉ tính lại bucket của giờ vừa qua + giờ hiện tại), ghi vào `topic_buckets`. Không bao giờ `COUNT(*)` on-the-fly cho biểu đồ trend.

---



## 8. API

```
GET  /stories                  ?beat=&since=&limit=      danh sách cluster
GET  /stories/{id}                                        chi tiết + members
GET  /storylines/{id}                                     timeline các cluster
GET  /trends                   ?domain=&window=7d         beat kèm z-score, nhãn
GET  /trends/{beat}/timeline   ?granularity=hour|day      chuỗi thời gian
GET  /search                   ?q=&beat=&mode=hybrid      hybrid search
GET  /feed.rss                 ?beat=&min_sources=        output RSS
GET  /alerts/stream                                       SSE
POST /alerts                                              lưu saved query
```

**Hybrid search:** chạy song song BM25 (`ts_rank_cd` trên tsvector) và vector similarity, hợp nhất bằng Reciprocal Rank Fusion:

```
RRF_score(d) = Σ 1 / (k + rank_i(d))     với k = 60
```

Ghi lại so sánh chất lượng: chỉ BM25 vs chỉ vector vs RRF, trên ~20 truy vấn tự soạn.

**Output RSS là feature quan trọng** — người dùng đọc bằng reader họ đã có, bạn không phải làm UI tốt.

---



## 9. Reliability & Observability



### Reliability

- **Idempotency:** khoá dedup là `raw_articles.url` đã canonicalize. Upsert dùng
`ON CONFLICT (url) DO UPDATE ... WHERE content/metadata thực sự đổi` — **không phải**
`DO NOTHING`, vì bài được re-crawl và nội dung có thể cập nhật. Không bao giờ ghi đè
metadata đang có bằng giá trị rỗng. At-least-once + consumer idempotent =
effectively-once.
- **Retry:** exponential backoff **có full jitter** (`random.uniform(0, delay)`),
base 5 phút, trần 24h. Sau 5 lần fail → `dead_letter` + `news-worker replay`.
- **Lease thay vì giữ transaction:** claim job bằng UPDATE đặt `lease_until` +
`locked_by` rồi **commit ngay**, xử lý xong mới finalize. Không giữ `FOR UPDATE`
xuyên suốt thời gian embed — một batch 32 bài mất vài giây, giữ lock cả quãng đó sẽ
chặn worker khác và làm phình transaction. Lease hết hạn ⇒ job tự về `pending`.
- **Poison-pill isolation:** batch fail ⇒ thử lại từng item; item nào fail riêng thì
chỉ item đó bị tính attempt. Một bài hỏng không được kéo cả batch 32 bài xuống.
- **Single-instance:** `pg_try_advisory_lock` theo tên worker; instance thứ hai thoát 0.
- **Graceful shutdown:** SIGTERM → dừng nhận job mới, hoàn tất chunk đang chạy, ghi
`worker_runs`, thoát 0.
- **Backpressure:** claim size cố định; worker không đọc thêm khi chunk hiện tại chưa
commit. Giới hạn tài nguyên crawl là hằng số (`MAX_CONCURRENT_*`), không scale theo
số bài.
- **Checkpoint:** không có state trong process. Con trỏ feed nằm ở `feed_state`, tiến
độ stage nằm ở `processing_jobs`, tổng kết nằm ở `worker_runs`. Restart không mất gì.



### Nguyên tắc riêng của crawl worker

Những thứ này không phải "reliability" theo nghĩa hẹp, nhưng bỏ qua thì worker vừa
tốn kém vừa mất lịch sự với nguồn:

- **Dedup TRƯỚC khi crawl, không phải sau.** Hiện tại một entry được decode → crawl
bằng headless browser → rồi mới bị loại ở bước persist vì trùng URL. Đó là toàn bộ
chi phí đắt nhất bị đốt vô ích. Kiểm tra URL đã có trong `raw_articles` ngay sau
bước discovery.
- **Conditional GET cho feed.** Gửi `If-None-Match`/`If-Modified-Since` từ
`feed_state.etag`/`last_modified`; 304 ⇒ bỏ qua feed, không parse. Với 422 feed poll
mỗi 30 phút đây là khoản tiết kiệm lớn nhất.
- **Lịch sự theo host, không chỉ theo tổng.** `MAX_CONCURRENT_CRAWLS` là giới hạn
toàn cục; một batch vẫn có thể toàn bài của cùng một site. Cần thêm giới hạn đồng
thời và khoảng cách tối thiểu giữa hai request **cùng host**.
- **robots.txt.** Bật `check_robots_txt` của crawl4ai.
- **User-Agent trung thực.** UA hiện tại giả dạng bot của Google
(`GoogleNewsBot/1.0; +https://news.google.com`). Phải đổi thành UA của chính project
kèm URL liên hệ.
- **Circuit breaker theo host trong một run.** Host fail N lần liên tiếp ⇒ bỏ phần còn
lại của host đó trong run này, đẩy sang `crawl_failures`.
- **Tái dùng browser.** `Crawl4aiProvider.crawl_batch` đang mở/đóng `AsyncWebCrawler`
mỗi chunk 10 bài ⇒ khởi động lại Chromium liên tục. Dùng chung một browser cho cả run.
- **Decode cache bền vững.** Cache Google News URL → URL thật hiện chỉ nằm trong bộ
nhớ process, mất sau mỗi lần chạy. Lưu xuống bảng để không decode lại (decode đang
bị rate-limit 2 concurrent).



### Metrics (Prometheus)

- `queue_depth{stage}` — `count(*) FROM processing_jobs WHERE state='pending'`
- `processing_duration_seconds{stage}` — histogram, p50/p95/p99 cho từng tầng
- `worker_run_duration_seconds{worker}` / `worker_runs_total{worker,status}`
- `embedding_batch_size`, `embedding_throughput_per_sec`
- `cluster_size_distribution` — histogram
- `dedup_hits_total{level}` — L1 hiện tại; mở rộng nếu backlog L2/L3 được duyệt
- `dlq_size{stage}` + `feeds_disabled_total`
- `crawl_skipped_total{reason}` — `already_seen` / `out_of_window` / `feed_304`;
đây là thước đo trực tiếp cho việc dedup-trước-khi-crawl đáng giá bao nhiêu
- `classification_confidence` — histogram, phát hiện taxonomy drift

Tuần 1 chưa có Prometheus. Toàn bộ các con số trên đã có sẵn dưới dạng truy vấn trên
`worker_runs` / `processing_jobs` / `feed_state`; tuần 3 chỉ là bọc chúng lại thành
endpoint `/metrics`.

### Tracing

Trace được một bài từ lúc được discover đến lúc thuộc về cluster nào.
`worker_runs.id` là correlation ID: mọi log của một run mang cùng `run_id`, và
`processing_jobs.last_error` giữ lỗi gần nhất của từng item. Log có cấu trúc.

---



## 10. Kế hoạch 4 tuần

> **Ngân sách đã điều chỉnh: ~48h, không phải 40h.** Đưa cả crawl worker và embed
> worker vào tuần 1 làm nền tảng khiến tuần 1 nặng gấp đôi ước tính v0.1. Đây là đánh
> đổi có chủ đích: khung worker + bảng lịch sử làm xong một lần thì cluster worker và
> rollup worker ở tuần 2–3 gần như chỉ còn phần thuật toán. Mục 10.5 liệt kê thứ cắt
> được nếu phải giữ đúng 40h.



### Tuần 1 (16h) — Nền tảng + khung worker

Chi tiết đầy đủ: [week1-foundation.md](week1-foundation.md).


|     |                                                                          |
| --- | ------------------------------------------------------------------------ |
| 2h  | pgvector + hỗ trợ kiểu `Vector` cho Alembic autogenerate                 |
| 2h  | Bảng `sources` + định danh nguồn theo hostname + backfill                |
| 2h  | Mở rộng `raw_articles` (`event_at`, `lang`, tsvector) + nâng dedup L1    |
| 3h  | `workers/base.py`: run recording, advisory lock, lease, SIGTERM, CLI     |
| 3h  | crawl worker: `feed_state`, conditional GET, cửa sổ thời gian, pre-dedup |
| 3h  | embed worker: `processing_jobs`, lease claim, micro-batch, HNSW          |
| 1h  | DLQ + `news-worker replay` + jitter                                      |


*Deliverable:* `news-worker crawl --once` và `news-worker embed --once` chạy được từ
cron; bài chảy vào Postgres kèm embedding, không trùng lặp; mọi quyết định retry /
tiếp tục / bỏ qua đều đọc từ bảng chứ không từ bộ nhớ process.

### Tuần 2 (12h cap) — MVP


|     |                                                         |
| --- | ------------------------------------------------------- |
| 4h  | cluster worker: online clustering + merge               |
| 2h  | Taxonomy YAML + prototype classifier + gán nhãn cluster |
| 2h  | REST API cơ bản + Frontend đơn giản                     |
| 2h  | docker-compose đầy đủ + README có sơ đồ kiến trúc       |

10h được phân bổ cho feature; giữ 2h contingency cho integration/golden validation.
Dedup L2 SimHash và L3 semantic near-duplicate được đưa vào backlog, chỉ quyết định sau
khi đo duplicate rate còn lại, embedding cost, false positive và ảnh hưởng centroid/UX
trên dữ liệu đã qua quality gate. Đề xuất tương lai phải giữ cross-publisher evidence;
nếu L2 suppress processing thì cần audit relation bền vững thay vì chỉ bỏ article.


**→ Mốc MVP: mở web thấy story cluster hôm nay, gom theo lĩnh vực, mỗi story hiện đủ các nguồn.**

### Tuần 3 (10h) — Trend + độ tin cậy


|     |                                                           |
| --- | --------------------------------------------------------- |
| 3h  | rollup worker + burst detection theo beat + API `/trends` |
| 2h  | Storyline linking + nhãn trend                            |
| 2h  | Hybrid search với RRF                                     |
| 1h  | SSE + saved query alert                                   |
| 2h  | Prometheus metrics + Grafana dashboard                    |




### Tuần 4 (10h) — Hiệu năng + kể chuyện


|     |                                                                 |
| --- | --------------------------------------------------------------- |
| 2h  | Bộ eval clustering gán nhãn tay, quét ngưỡng, ghi kết quả       |
| 2h  | Backfill dataset lớn (GDELT / CC-NEWS) qua `DiscoverySource`    |
| 2h  | `RedisStreamQueue` implement `JobQueue` + benchmark vs Postgres |
| 2h  | Tối ưu dựa trên `EXPLAIN ANALYZE`, so sánh HNSW vs IVFFlat      |
| 2h  | `ARCHITECTURE.md` + blog post kỹ thuật                          |




### 10.5 Cắt gì nếu phải giữ 40h

Theo thứ tự cắt, mức ảnh hưởng tăng dần:

1. `RedisStreamQueue` + benchmark (−2h) — mất một điểm nhấn CV, không mất chức năng.
2. Conditional GET cho feed (−1h) — chỉ tốn băng thông, không sai kết quả.
3. Grafana dashboard, giữ lại Prometheus endpoint (−1h).
**Không cắt:** `feed_state`, `processing_jobs`, DLQ, advisory lock. Bỏ mấy thứ này thì
worker không đáp ứng được yêu cầu "quyết định dựa trên lịch sử chạy", và về sau phải
viết lại chứ không phải bổ sung.

---



## 11. Số liệu cần đo

Nguyên tắc: **không phải dùng công nghệ gì, mà là đo được gì và đánh đổi ra sao.**


| Hạng mục       | Cần có con số                                                        |
| -------------- | -------------------------------------------------------------------- |
| Dedup          | L1 hit rate; L2/L3 chỉ đo như input cho quyết định backlog           |
| Embedding      | Throughput bài/giây ở batch size 1 vs 16 vs 64                       |
| Clustering     | Precision/recall/F1 trên eval set, theo từng ngưỡng                  |
| Vector index   | HNSW vs IVFFlat: recall@10, latency p95, thời gian build, dung lượng |
| Search         | BM25 vs vector vs RRF trên bộ truy vấn tự soạn                       |
| API            | p50/p95/p99 dưới tải, throughput tối đa                              |
| Rollup         | Thời gian query trend chart: có rollup vs tính on-the-fly            |
| Classification | Precision/recall theo từng beat trên tập gán nhãn tay                |


Mọi con số cần kèm điều kiện: kích thước dataset, cấu hình máy, tham số.

---



## 12. Rủi ro đã biết


| Rủi ro                                                                                                       | Xử lý                                                                                                 |
| ------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| Taxonomy drift — beat khai báo không khớp thực tế                                                            | Theo dõi tỉ lệ `uncategorized`; nếu > 20% thì xem lại exemplars                                       |
| Cluster vỡ vụn hoặc gộp quá mức                                                                              | Eval set + theo dõi phân bố kích thước cluster                                                        |
| Whitelist thay đổi làm sai trend                                                                             | Dùng `share` và `n_distinct_sources`, không dùng count thô                                            |
| Chi phí LLM                                                                                                  | Chỉ gọi ở tầng cluster, khi `n_articles ≥ 3` và chưa có summary                                       |
| Cửa sổ 72h giữ quá nhiều cluster trong bộ nhớ                                                                | Cluster đóng sau 72h không hoạt động, chỉ giữ centroid                                                |
| **Nguồn dữ liệu không khớp taxonomy** — `engineering_blogs.opml` toàn blog kỹ thuật, không có finance/health | Bổ sung OPML cho finance + health, hoặc thu hẹp taxonomy về beat kỹ thuật. Phải quyết trước tuần 2    |
| **Baseline 28 ngày không kịp trong 4 tuần** — MAD sẽ rất nhiễu với ~14 ngày dữ liệu                          | Chạy `news-worker crawl` liên tục từ ngày đầu tuần 1; kéo backfill lịch sử lên tuần 2                 |
| **RSS không backfill được quá khứ sâu** — feed chỉ giữ 10–50 entry gần nhất                                  | Tách rõ re-process (làm được mọi khoảng) và re-discover (cần adapter sitemap/CDX, tuần 4)             |
| **Nhịp crawl bị nhầm thành nhịp tin tức** — 422 feed xoay vòng, `crawled_at` phản ánh scheduler              | Bucket theo `event_at`, granularity `day`; chỉ dùng `hour` khi một vòng crawl phủ hết feed dưới 1 giờ |
| **Cron chồng lịch chạy hai worker cùng lúc**                                                                 | `pg_try_advisory_lock` theo tên worker, instance thứ hai thoát 0                                      |


---



## 13. Câu hỏi mở

- [ ] Cửa sổ clustering 72h có đủ cho tin tài chính (diễn biến kéo dài nhiều ngày) không? Có nên để cửa sổ khác nhau theo domain?
- [ ] Storyline linking nên dựa trên centroid similarity hay dựa trên entity chung? Entity có thể chính xác hơn nhưng cần thêm bước NER.
- [ ] Có cần time-decay trên centroid không (bài mới ảnh hưởng nhiều hơn bài cũ)?
- [ ] Feature "time-to-story" (nguồn nào đưa tin trước) — đưa vào tier 2 hay tier 3?
- [ ] Sentiment theo cluster cho mảng tài chính — FinBERT hay bỏ qua trong scope này?
- [ ] Nguồn dữ liệu: bổ sung feed finance/health, hay thu hẹp taxonomy về beat kỹ thuật? **Chặn tuần 2.**
- [ ] Lịch chạy thật của crawl worker: 422 feed × ~20 bài ở `MAX_CONCURRENT_CRAWLS=2` mất nhiều giờ cho một vòng. Chọn poll ít feed thường xuyên, hay nhiều feed thưa hơn?
- [ ] `POST /alerts` ghi vào DB, mâu thuẫn quy tắc read-only của `backend/api/`. Tách router có quyền ghi trên bảng `saved_queries`, hay tách hẳn thành service riêng?
- [ ] Có giữ `news-service` và `news-pipeline` như alias tương thích ngược, hay thay hẳn bằng `news-worker`?
