# News Aggregator

Repo này crawl nội dung web trực tiếp hoặc từ RSS/Atom, lưu bản mới nhất của
mỗi article vào PostgreSQL, đồng thời duy trì hàng đợi retry và thống kê từng
lần ingest. Backend (Python/FastAPI) nằm trong `backend/`, frontend (Next.js)
nằm trong `frontend/`.

## Cài đặt

```bash
cp .env_example .env
uv sync --extra dev
uv run crawl4ai-setup
```

Điền `TAVILY_API_KEY` trong `.env` nếu dùng lệnh `search`.

## Khởi động PostgreSQL

PostgreSQL 16 chạy trong Compose (`deployments/docker-compose.yml`), còn các
CLI job chạy trên host:

```bash
docker compose -f deployments/docker-compose.yml --env-file .env up -d db
uv run alembic upgrade head
```

Compose dùng named volume `news_radar_postgres_data`, vì vậy dữ liệu vẫn còn
khi container được restart hoặc tạo lại. Nếu port `5432` trên host đã bận, đổi
cả `POSTGRES_PORT` và port trong `DATABASE_URL`, ví dụ `55432`. `--env-file .env`
là bắt buộc vì file compose không còn nằm cạnh `.env` ở root.

Alembic không tự chạy khi service khởi động. Nếu DB chưa kết nối được hoặc
schema chưa migrate, job có persistence sẽ dừng trước khi crawl và in hướng
dẫn chạy migration.

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

## Chạy pipeline một lần

Các lệnh không có `--persist` chỉ emit JSON Lines ra stdout:

```bash
uv run news-pipeline search "gold market news" --max-results 5
uv run news-pipeline crawl https://example.com
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

## Chạy worker

`news-worker` là entry point cho mọi worker; hiện có subcommand `crawl`. Worker
luôn yêu cầu PostgreSQL. Mỗi run xử lý tối đa `MAX_RETRIES_PER_RUN` failure đến
hạn trước, rồi mới ingest feed hiện tại. Mặc định chạy một lần rồi thoát:

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
- **Tắt êm:** SIGTERM/SIGINT dừng ở ranh giới chunk kế tiếp, đóng sổ run ở
  trạng thái `partial` thay vì để kẹt `running`.
- **Run treo:** run nào ngừng heartbeat quá `max(3 × --interval,
  WORKER_HEARTBEAT_TIMEOUT_SECONDS)` sẽ bị run sau đánh dấu `abandoned`.

`news-service` vẫn chạy được nhưng đã deprecated: nó chỉ in cảnh báo, đổi
`--interval-seconds` thành `--interval` rồi gọi lại `news-worker crawl`.

## Nguồn TSV và OPML

File TSV cần tối thiểu 4 cột. Pipeline dùng cột thứ 3 làm `query_label` và cột
thứ 4 làm `rss_url`; dòng trống và dòng bắt đầu bằng `#` được bỏ qua.

Với OPML, pipeline đọc mọi `outline` có `xmlUrl` và chuyển đổi:

- `title` (fallback sang `text`) → `query_label`
- `xmlUrl` → `rss_url`

`htmlUrl` không cần cho việc tải feed. `--tsv-path` là alias tương thích ngược
của `--feed-path`. Có thể đặt nguồn mặc định bằng `RSS_FEED_PATH`; biến cũ
`GOOGLE_NEWS_TSV_PATH` vẫn được hỗ trợ. `--max-feeds 0` nghĩa là xử lý toàn bộ.

## Raw storage và monitoring

Schema có bốn bảng:

- `raw_articles`: nội dung Markdown mới nhất, URL chuẩn hóa và SHA-256
  `content_hash`; upsert chỉ update khi content hoặc metadata thay đổi.
- `sources`: nguồn đã chuẩn hóa mà `raw_articles` tham chiếu tới.
- `crawl_failures`: lỗi hiện tại và durable retry queue, không lưu lịch sử từng
  attempt.
- `worker_runs`: sổ vận hành chung cho mọi worker — trạng thái
  `running/succeeded/partial/failed/abandoned`, `heartbeat_at`, cửa sổ thời
  gian, cùng `params` và `stats` dạng JSONB.

Không lưu raw HTML hay phiên bản cũ. Có thể kiểm tra nhanh trạng thái bằng:

```bash
docker compose -f deployments/docker-compose.yml --env-file .env exec db psql -U news_radar -d news_radar -c "SELECT worker, status, count(*) FROM worker_runs GROUP BY worker, status;"
docker compose -f deployments/docker-compose.yml --env-file .env exec db psql -U news_radar -d news_radar -c "SELECT failure_kind, count(*) FROM crawl_failures GROUP BY failure_kind;"
```

## Chạy API

`backend/api/` là một FastAPI app read-only trên cùng schema (`raw_articles`),
dùng chung `PostgresRawStorage`
([backend/adapters/storage/postgres.py](backend/adapters/storage/postgres.py))
với pipeline/service. Yêu cầu PostgreSQL đã migrate:

```bash
uv run news-api
```

Mặc định lắng nghe `API_HOST`/`API_PORT` (`0.0.0.0:8000`); `CORS_ALLOW_ORIGINS`
trong `.env` là danh sách origin (phân tách bởi dấu phẩy) được phép gọi API,
mặc định `http://localhost:3000` cho frontend dev. Endpoint hiện có:

- `GET /health`, `GET /health/db`
- `GET /api/articles?limit=&offset=&source=&q=`
- `GET /api/articles/{id}`

## Frontend

`frontend/` hiện là chỗ trống, chưa scaffold. Xem
[frontend/README.md](frontend/README.md) để khởi tạo bằng `pnpm create next-app`
khi cần.

## Kiểm thử

Unit test không yêu cầu DB:

```bash
uv run --extra dev pytest -m "not integration"
```

Integration test yêu cầu một DB đã migrate:

```bash
TEST_DATABASE_URL="$DATABASE_URL" uv run --extra dev pytest
```
