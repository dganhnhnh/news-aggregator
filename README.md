# News Aggregator

Repo này crawl nội dung web trực tiếp hoặc từ RSS/Atom, lưu bản mới nhất của
mỗi article vào PostgreSQL, rồi embed, gán topic và gom article thành story
cluster toàn cục. FastAPI phục vụ article/story/taxonomy ở chế độ read-only;
frontend Next.js hiển thị Articles và Stories với topic tags.

```text
crawl → raw_articles → embed ─┬→ classify → article topics ─┐
                             └→ cluster  → story members ──┴→ story topics → API/UI
```

## Cài đặt

```bash
cp .env_example .env
uv sync --extra dev
uv run crawl4ai-setup
```

Điền `TAVILY_API_KEY` trong `.env` nếu dùng lệnh `search`.

## Khởi động PostgreSQL và API

PostgreSQL 16 và API chạy trong Compose (`deployments/docker-compose.yml`),
còn migration và worker chạy trên host. Khởi động DB trước, migrate schema,
rồi mới khởi động API:

```bash
docker compose -f deployments/docker-compose.yml --env-file .env up -d db
uv run alembic upgrade head
docker compose -f deployments/docker-compose.yml --env-file .env up -d --build api
curl --fail http://localhost:8000/health/db
```

Compose dùng named volume `news_radar_postgres_data`, vì vậy dữ liệu vẫn còn
khi container được restart hoặc tạo lại. Nếu port `5432` trên host đã bận, đổi
cả `POSTGRES_PORT` và port trong `DATABASE_URL`, ví dụ `55432`. `--env-file .env`
là bắt buộc vì file compose không còn nằm cạnh `.env` ở root.

Alembic **không** tự chạy khi service khởi động. API readiness `/health/db`
chỉ xanh sau khi DB đã migrate; container không chứa worker scheduler hay
cron. Xem log bằng `docker compose -f deployments/docker-compose.yml
--env-file .env logs -f api`.

### Thay đổi schema và tạo migration

`backend/adapters/storage/schema.py` là source of truth. Sau khi thay đổi
schema, luôn tạo revision bằng autogenerate trên DB đang ở migration head:

```bash
uv run alembic upgrade head
uv run alembic revision --autogenerate -m "describe schema change"
uv run alembic upgrade head
uv run alembic check
```

Không tạo file revision thủ công và không truyền `--rev-id`; giữ nguyên hex ID
và filename prefix do Alembic sinh. Luôn review nội dung autogenerate trước khi
apply vì rename và data migration có thể cần điều chỉnh thêm.

## Chạy flow Week 2 end-to-end

### 1. Bootstrap taxonomy và prototype

PostgreSQL là runtime source of truth; YAML chỉ dùng để bootstrap/import. Sau
migration mới hoặc sau khi exemplar/topic trong seed thay đổi:

```bash
uv run news-worker taxonomy-import --file data/taxonomy.seed.yaml
uv run news-worker taxonomy-rebuild
```

Import dùng stable slug và idempotent. Không thêm `--prune` trừ khi chủ đích
deactivate topic/node vắng khỏi file.

### 2. Crawl và drain các stage downstream

Ví dụ ingest một nhóm feed rồi xử lý toàn bộ queue vừa tạo:

```bash
uv run news-worker crawl \
  --feed-path data/engineering_blogs.opml \
  --max-feeds 5 \
  --max-articles-per-feed 5 \
  --once

uv run news-worker embed --once
uv run news-worker classify --once
uv run news-worker cluster --once
uv run news-worker cluster --merge-only --once
uv run news-worker classify --refresh-clusters --last 30d --once
```

`classify` và `cluster` độc lập nên có thể đổi thứ tự; attach/classify sẽ
refresh `cluster_topics` để hai đường cuối cùng hội tụ. Mỗi command drain queue
theo chunk đến khi hết việc hoặc đạt `--limit`.

Nếu nâng taxonomy trên database đã có embedding/cluster, requeue dữ liệu cũ
theo cửa sổ rồi drain lại:

```bash
uv run news-worker classify --stale --last 30d --once
uv run news-worker cluster --reprocess --last 30d --once
uv run news-worker cluster --merge-only --once
uv run news-worker classify --refresh-clusters --last 30d --once
```

Nếu đổi embedding model hoặc input builder, chạy `embed --model-changed`
trước; re-embedding sẽ enqueue lại cả `classify` và `cluster`.

### 3. Smoke-check dữ liệu, API và Stories UI

Queue, retry queue và worker runs xem bằng một lệnh:

```bash
uv run news-worker queue-status
```

Prototype, classifier coverage và cluster distribution vẫn cần truy vấn riêng:

```bash
docker compose -f deployments/docker-compose.yml --env-file .env exec db sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT model, dim, count(*) FROM topic_prototypes GROUP BY 1,2"'

docker compose -f deployments/docker-compose.yml --env-file .env exec db sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT assigned_count, count(*) FROM article_classifications GROUP BY 1 ORDER BY 1"'

docker compose -f deployments/docker-compose.yml --env-file .env exec db sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT n_articles, count(*) FROM clusters WHERE status <> '\''merged'\'' GROUP BY 1 ORDER BY 1"'

curl --fail http://localhost:8000/health/db
curl --fail 'http://localhost:8000/api/topics'
curl --fail 'http://localhost:8000/api/stories?limit=5'
```

Sau đó mở frontend:

```bash
cd frontend
cp .env.example .env.local
pnpm install
pnpm dev
```

Mở `http://localhost:3000`, chọn **Stories** và click một story để xem member.
`API_URL` chỉ dùng server-side trong Next.js và mặc định trỏ tới
`http://localhost:8000`.

## Chạy pipeline một lần

Các lệnh không có `--persist` chỉ emit JSON Lines ra stdout:

```bash
uv run news-pipeline search "gold market news" --max-results 5
uv run news-pipeline crawl https://example.com
uv run news-pipeline rss https://example.com/feed.xml --max-articles-per-feed 5
uv run news-pipeline rss --feed-path ./data/engineering_blogs.opml --max-feeds 5 --max-articles-per-feed 1
uv run news-pipeline rss --tsv-path ./queries.tsv --max-articles-per-feed 5
```

`crawl` và `rss` hỗ trợ lưu article/failure và tạo một `worker_runs` bằng cờ
`--persist`; kết quả article vẫn được emit ra stdout:

```bash
uv run news-pipeline crawl https://example.com --persist
uv run news-pipeline rss --feed-path ./data/engineering_blogs.opml --max-feeds 5 --max-articles-per-feed 1 --persist
```

`search` của Tavily không hỗ trợ `--persist`.

`news-pipeline rss` cũng nhận trực tiếp một hoặc nhiều URL RSS/Atom ở vị trí
đối số. URL trực tiếp và `--feed-path`/`--tsv-path` loại trừ lẫn nhau.

## Chạy worker

`news-worker` là entry point chung cho `crawl`, `embed`, `classify`, `cluster`
và các admin command taxonomy/replay/cleanup. Worker luôn yêu cầu PostgreSQL.
Mỗi crawl run xử lý tối đa `MAX_RETRIES_PER_RUN` failure đến hạn trước, rồi mới
ingest feed hiện tại. Mặc định chạy một lần rồi thoát:

```bash
uv run news-worker crawl --feed-path ./data/engineering_blogs.opml --max-feeds 5
uv run news-worker crawl --feed-path ./data/engineering_blogs.opml --interval 1800
```

Mỗi run ghi cửa sổ thời gian đã yêu cầu vào `worker_runs`; mặc định là `--last
3d`, hoặc chỉ định tường minh bằng `--since` / `--until` (ISO date hoặc
datetime, naive được đọc là UTC):

```bash
uv run news-worker crawl --last 12h
uv run news-worker crawl --since 2026-08-01 --until 2026-08-10
```

Cửa sổ dài hơn 7 ngày sẽ log WARNING vì RSS feed không giữ lại entry cũ đến
vậy. Ở giai đoạn hiện tại cửa sổ mới chỉ được ghi nhận, chưa dùng để lọc entry.

Crawler dùng giới hạn tài nguyên cố định, không tăng số browser workers theo
tổng số article. Mặc định fetch tối đa 2 feed, crawl tối đa 2 page đồng thời và
commit sau mỗi 10 kết quả. Có thể điều chỉnh rõ ràng:

```bash
uv run news-worker crawl \
  --feed-path ./data/engineering_blogs.opml \
  --max-concurrent-feeds 2 \
  --max-concurrent-crawls 2 \
  --crawl-batch-size 10
```

Mỗi chunk được upsert và checkpoint ngay; dừng sau đó không làm mất các chunk
đã commit. Khi `--max-feeds` nhỏ hơn tổng nguồn, run có `--interval` tự chuyển
sang nhóm tiếp theo trong cùng process. Dùng `--feed-offset` để chọn vị trí bắt
đầu cho một run đơn lẻ hoặc sau khi restart.

Mặc định worker chỉ log summary. Thêm `--emit-jsonl` nếu cần emit đầy đủ các
article đã crawl:

```bash
uv run news-worker crawl --feed-path ./data/engineering_blogs.opml --emit-jsonl
```

Failure được retry theo exponential backoff từ 5 phút đến tối đa 24 giờ. Một
failure thành công sẽ bị xóa; failure lặp lại chỉ tăng `attempt_count` trên cùng
row.

### Vận hành

- **Không chạy chồng:** mỗi worker giữ một advisory lock của PostgreSQL trong
  suốt run. Instance thứ hai log rồi thoát ngay với exit code 0 thay vì xếp
  hàng chờ, nên đặt cron dày hơn thời lượng run vẫn an toàn.
- **Exit code:** `0` khi run `succeeded`/`partial` hoặc khi không lấy được
  lock; `1` khi run `failed`. Thêm `--strict` nếu muốn `partial` cũng trả `1`.
- **`--exit-busy CODE`:** thoát `CODE` thay vì `0` khi lock đã có chủ, để
  scheduler phân biệt "bị bỏ qua" với "chạy xong". Mặc định vẫn `0` nên cron
  hiện có không đổi; `1` bị từ chối vì đó là mã lỗi.
- **`--summary-json PATH`:** append đúng một dòng JSON mô tả run vừa xong
  (`run_id`, `status`, `stats`, `error_message`). `-` hoặc `/dev/stdout` ghi
  vào dòng **cuối cùng** của stdout, là dòng Airflow đẩy lên XCom.
- **`--max-runtime DURATION`:** yêu cầu worker dừng êm sau ngân sách như
  `25m`; Airflow đặt hard timeout dài hơn 5 phút để chunk hiện tại kịp
  checkpoint và emit summary.
- **Tắt êm:** SIGTERM/SIGINT dừng ở ranh giới chunk kế tiếp, đóng sổ run ở
  trạng thái `partial` thay vì để kẹt `running`.
- **Run treo:** run nào ngừng heartbeat quá `max(3 × --interval,
  WORKER_HEARTBEAT_TIMEOUT_SECONDS)` sẽ bị run sau đánh dấu `abandoned`.

`news-service` vẫn chạy được nhưng đã deprecated: nó chỉ in cảnh báo, đổi
`--interval-seconds` thành `--interval` rồi gọi lại `news-worker crawl`.

## Instant crawl

Feed có `priority=0` được worker riêng poll liên tục; feed mặc định có
`priority=1` và chỉ do `news-worker crawl` định kỳ xử lý. Số nhỏ hơn là ưu
tiên cao hơn. Đổi tier cho một feed đã seed:

```bash
uv run news-worker set-feed-priority https://example.com/feed.xml 0
```

Chạy worker trên host:

```bash
uv run news-worker instant-crawl --last 12h --interval 30 --poll-interval 30
```

Hoặc chạy container cùng PostgreSQL và API:

```bash
docker compose --env-file .env -f deployments/docker-compose.yml up -d --build instant-crawl
```

`INSTANT_FEED_POLL_INTERVAL_SECONDS` mặc định là 30 giây. Worker này chỉ lấy
feed và retry crawl có priority 0; worker `crawl` của Airflow chỉ lấy priority
1. Bài mới tạo job semantic với cùng priority. `news_semantic` vẫn chạy batch
15 phút một lần và claim priority 0 trước priority 1; instant crawl không khởi
động semantic DAG cho từng bài.

## Điều phối bằng Airflow

Thay cho việc chạy tay chuỗi lệnh ở §"Chạy flow Week 2 end-to-end". Thiết kế và
lý do từng quyết định nằm ở [docs/orchestration-airflow.md](docs/orchestration-airflow.md).

Sáu DAG trong `orchestration/dags/`, mỗi task là **một lệnh `news-worker` chạy
trong container riêng** (`DockerOperator`):

| DAG | Lịch | Nội dung |
|---|---|---|
| `news_ingest` | `*/5 * * * *` | `preflight_db → crawl`; chỉ xử lý feed priority 1 |
| `news_semantic` | `*/15 * * * *` | queue gate → `embed → classify ‖ cluster → merge → refresh`; xử lý mọi priority |
| `news_maintenance` | `0 3 * * *` | `cluster --repair`, cleanup dry-run, `queue-status --fail-on-alert`; không tự replay poison item |
| `news_taxonomy_refresh` | Trigger tay | import → rebuild → reclassify; recluster là param mặc định tắt |
| `news_backfill` | Trigger tay | full flow strict trên cửa sổ `since`/`until` tối đa 31 ngày |
| `news_dlq_replay` | Trigger tay | preview mặc định; chỉ requeue dead letter khi `apply=true` |

Khởi động (build image worker trước, vì scheduler chỉ chạy container chứ không
build):

```bash
docker build -f deployments/Dockerfile.worker -t news-radar-worker:a1b2c3d .
```

```bash
NEWS_RADAR_REPO=$PWD docker compose --env-file .env -f deployments/docker-compose.yml -f deployments/docker-compose.airflow.yml up -d
```

Đặt `NEWS_RADAR_WORKER_IMAGE=news-radar-worker:a1b2c3d` trong `.env` cùng tag
vừa build. Production phải dùng Git SHA hoặc image digest bất biến; tag `dev`
chỉ là fallback cục bộ.

UI ở `http://localhost:8080`. `NEWS_RADAR_REPO` phải là đường dẫn **trên host**
tới repo: scheduler mount `$NEWS_RADAR_REPO/data` vào từng container worker,
và bind mount đó do Docker daemon của host giải quyết chứ không phải container
scheduler. Trên host mà `/var/run/docker.sock` không thuộc group `999`, đặt
thêm `AIRFLOW_DOCKER_GID`.

`airflow-init` tạo sẵn bốn pool `news_crawl` / `news_embed` / `news_cluster` /
`news_classify`, mỗi pool **1 slot**. Đó là toàn bộ mô hình concurrency: worker
đã tự loại trừ nhau bằng advisory lock, pool chỉ để Airflow không phải khởi
động container rồi nhận về một task `skipped`.

## Nguồn TSV và OPML

File TSV cần tối thiểu 4 cột. Pipeline dùng cột thứ 3 làm `query_label` và cột
thứ 4 làm `rss_url`; dòng trống và dòng bắt đầu bằng `#` được bỏ qua.

Với OPML, pipeline đọc mọi `outline` có `xmlUrl` và chuyển đổi:

- `title` (fallback sang `text`) → `query_label`
- `xmlUrl` → `rss_url`

`htmlUrl` không cần cho việc tải feed. `--tsv-path` là alias tương thích ngược
của `--feed-path`. Có thể đặt nguồn mặc định bằng `RSS_FEED_PATH`; biến cũ
`GOOGLE_NEWS_TSV_PATH` vẫn được hỗ trợ. `--max-feeds 0` nghĩa là xử lý toàn bộ.

## Storage và monitoring

Các nhóm dữ liệu chính:

- ingest: `raw_articles`, `sources`, `feed_state`, `crawl_failures`;
- orchestration: `processing_jobs`, `processing_dlq`, `worker_runs`;
- semantic: `article_embeddings`, `clusters`, `cluster_members`,
  `article_classifications`, `article_topics`, `cluster_topics`;
- taxonomy: `topics`, `topic_signals`, `topic_prototypes`, `taxonomies`,
  `taxonomy_nodes` và mapping node-topic.

`raw_articles` chỉ giữ bản Markdown mới nhất theo normalized URL; không lưu raw
HTML hay lịch sử version. `cluster_topics` là derived data có thể rebuild từ
article scores và source-balanced aggregation. Có thể kiểm tra nhanh trạng thái bằng:

```bash
uv run news-worker queue-status
```

Lệnh này read-only, gộp `processing_jobs` (theo stage/state), `crawl_failures`
(theo kind) và `worker_runs` trong 24h gần nhất. Hai cờ dành cho scheduler:

- `--json` in đúng một dòng JSON trên stdout, dùng được với `jq` hoặc XCom;
- `--fail-on-alert` thoát `1` khi có job `dead_letter`, có failure đã
  dead-letter, hoặc có run `failed`/`abandoned` trong cửa sổ. Backlog `pending`
  **không** phải alert — nó cần lịch sử nhiều lần đo, không đọc được từ một
  snapshot.

## Chạy API

`backend/api/` là FastAPI app read-only trên cùng schema, dùng chung
`PostgresRawStorage`
([backend/adapters/storage/postgres.py](backend/adapters/storage/postgres.py))
với pipeline/service. Yêu cầu PostgreSQL đã migrate:

```bash
uv run news-api
```

Mặc định lắng nghe `API_HOST`/`API_PORT` (`0.0.0.0:8000`); `CORS_ALLOW_ORIGINS`
trong `.env` là danh sách origin (phân tách bởi dấu phẩy) được phép gọi API,
mặc định `http://localhost:3000` cho frontend dev. Endpoint hiện có:

- `GET /health`, `GET /health/db`
- `GET /api/articles?limit=&cursor=&source=&topic=&q=`
- `GET /api/articles/{id}`
- `GET /api/sources`, `GET /api/sources/{slug}/articles`
- `GET /api/topics?taxonomy=default`
- `GET /api/stories?limit=&cursor=&source=&topic=&min_sources=&min_articles=`
- `GET /api/stories/{id}`

## Frontend

`frontend/` là Next.js app. Browser gọi route `/api/*` cùng origin; Next.js
server proxy request tới FastAPI theo `API_URL`, nên không expose URL nội bộ ra
client. Xem [frontend/README.md](frontend/README.md) cho cấu hình local.

## Kiểm thử

Suite mặc định không tải embedding model thật; semantic suite được tách bằng
marker. Unit test không yêu cầu DB:

```bash
uv run --extra dev pytest -m "not integration"
```

Integration test yêu cầu một DB đã migrate:

```bash
TEST_DATABASE_URL="$DATABASE_URL" uv run --extra dev pytest
```

Golden semantic suite dùng đúng MiniLM production và đúng 10 scenario:

```bash
uv run --extra dev pytest backend/tests/test_semantic_golden.py -m semantic
```

Frontend build gate:

```bash
cd frontend && pnpm build
```
