# News Aggregator

Repo này crawl nội dung web trực tiếp hoặc từ RSS/Atom, lưu bản mới nhất của
mỗi article vào PostgreSQL, đồng thời duy trì hàng đợi retry và thống kê từng
lần ingest.

## Cài đặt

```bash
cp .env_example .env
uv sync --extra dev
uv run crawl4ai-setup
```

Điền `TAVILY_API_KEY` trong `.env` nếu dùng lệnh `search`.

## Khởi động PostgreSQL

PostgreSQL 16 chạy trong Compose, còn các CLI job chạy trên host:

```bash
docker compose up -d db
uv run alembic upgrade head
```

Compose dùng named volume `news_radar_postgres_data`, vì vậy dữ liệu vẫn còn
khi container được restart hoặc tạo lại. Nếu port `5432` trên host đã bận, đổi
cả `POSTGRES_PORT` và port trong `DATABASE_URL`, ví dụ `55432`.

Alembic không tự chạy khi service khởi động. Nếu DB chưa kết nối được hoặc
schema chưa migrate, job có persistence sẽ dừng trước khi crawl và in hướng
dẫn chạy migration.

### Thay đổi schema và tạo migration

`src/adapters/storage/schema.py` là source of truth. Sau khi thay đổi schema,
luôn tạo revision bằng autogenerate trên DB đang ở migration head:

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

`crawl` và `rss` hỗ trợ lưu article/failure và tạo một `ingest_runs` bằng cờ
`--persist`; kết quả article vẫn được emit ra stdout:

```bash
uv run news-pipeline crawl https://example.com --persist
uv run news-pipeline rss --feed-path ./data/engineering_blogs.opml --max-feeds 5 --max-articles-per-feed 1 --persist
```

`search` của Tavily không hỗ trợ `--persist`.

## Chạy background service

`news-service` luôn yêu cầu PostgreSQL. Mỗi iteration xử lý tối đa
`MAX_RETRIES_PER_RUN` failure đến hạn trước, rồi mới ingest feed hiện tại:

```bash
uv run news-service --feed-path ./data/engineering_blogs.opml --interval-seconds 1800
uv run news-service --feed-path ./data/engineering_blogs.opml --max-feeds 5 --once
```

Crawler dùng giới hạn tài nguyên cố định, không tăng số browser workers theo
tổng số article. Mặc định fetch tối đa 2 feed, crawl tối đa 2 page đồng thời và
commit sau mỗi 10 kết quả. Có thể điều chỉnh rõ ràng:

```bash
uv run news-service \
  --feed-path ./data/engineering_blogs.opml \
  --once \
  --max-concurrent-feeds 2 \
  --max-concurrent-crawls 2 \
  --crawl-batch-size 10
```

Mỗi chunk được upsert và checkpoint ngay; dừng sau đó không làm mất các chunk
đã commit. Khi `--max-feeds` nhỏ hơn tổng nguồn, service định kỳ tự chuyển sang
nhóm tiếp theo trong cùng process. Dùng `--feed-offset` để chọn vị trí bắt đầu
cho một lần `--once` hoặc sau khi restart service.

Mặc định service chỉ log summary. Thêm `--emit-jsonl` nếu cần emit đầy đủ các
article đã crawl:

```bash
uv run news-service --feed-path ./data/engineering_blogs.opml --once --emit-jsonl
```

Failure được retry theo exponential backoff từ 5 phút đến tối đa 24 giờ. Một
failure thành công sẽ bị xóa; failure lặp lại chỉ tăng `attempt_count` trên cùng
row. Chạy `--once` trả exit code khác 0 nếu iteration gặp lỗi fatal. Service
định kỳ ghi nhận iteration failed rồi thử lại ở chu kỳ sau. Hiện chỉ nên chạy
một instance `news-service`; claiming cho multi-worker chưa được triển khai.

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

Schema chỉ có ba bảng:

- `raw_articles`: nội dung Markdown mới nhất, URL chuẩn hóa và SHA-256
  `content_hash`; upsert chỉ update khi content hoặc metadata thay đổi.
- `crawl_failures`: lỗi hiện tại và durable retry queue, không lưu lịch sử từng
  attempt.
- `ingest_runs`: summary `running/succeeded/partial/failed` của mỗi invocation
  có persistence.

Không lưu raw HTML hay phiên bản cũ. Có thể kiểm tra nhanh trạng thái bằng:

```bash
docker compose exec db psql -U news_radar -d news_radar -c "SELECT status, count(*) FROM ingest_runs GROUP BY status;"
docker compose exec db psql -U news_radar -d news_radar -c "SELECT failure_kind, count(*) FROM crawl_failures GROUP BY failure_kind;"
```

## Kiểm thử

Unit test không yêu cầu DB:

```bash
uv run --extra dev pytest -m "not integration"
```

Integration test yêu cầu một DB đã migrate:

```bash
TEST_DATABASE_URL="$DATABASE_URL" uv run --extra dev pytest
```
