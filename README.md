# News Aggregator

Repo này tập trung vào hai năng lực: tìm kiếm bài viết và crawl nội dung trang.

## Cài đặt

```bash
cp .env_example .env
uv sync --extra dev
uv run crawl4ai-setup
```

Điền `TAVILY_API_KEY` trong `.env` nếu dùng Tavily.

## Chạy pipeline một lần

```bash
uv run news-pipeline search "gold market news" --max-results 5
uv run news-pipeline crawl https://example.com
uv run news-pipeline rss --tsv-path ./queries.tsv --max-articles-per-feed 5
uv run news-pipeline rss --feed-path ./data/engineering_blogs.opml --max-feeds 5 --max-articles-per-feed 1
```

Kết quả được ghi ra stdout dưới dạng JSON Lines để có thể pipe sang storage hoặc
worker khác mà không gắn chặt engine vào database.

## Chạy service định kỳ

Service chạy pipeline RSS/Atom theo chu kỳ, với nguồn feed là TSV hoặc OPML:

```bash
uv run news-service --tsv-path ./queries.tsv --interval-seconds 1800
uv run news-service --feed-path ./data/engineering_blogs.opml --max-feeds 5 --once
```

File TSV dùng cho Google News cần tối thiểu 4 cột; pipeline chuẩn hóa cột thứ 3
thành `query_label` và cột thứ 4 thành `rss_url`. Dòng trống và dòng bắt đầu bằng
`#` được bỏ qua.

Với OPML, pipeline đọc mọi phần tử `outline` có `xmlUrl` và chuyển đổi:

- `title` (fallback sang `text`) → `query_label`
- `xmlUrl` → `rss_url`

`htmlUrl` có trong OPML nhưng pipeline hiện không cần để tải feed. `--tsv-path`
được giữ làm alias tương thích ngược của `--feed-path`. Có thể đặt đường dẫn mặc
định bằng `RSS_FEED_PATH`; `GOOGLE_NEWS_TSV_PATH` cũ vẫn được hỗ trợ. File OPML
mẫu có hàng trăm feed, vì vậy có thể dùng `--max-feeds` để giới hạn số feed trong
mỗi lần chạy; giá trị `0` nghĩa là xử lý toàn bộ.

## Kiểm tra

```bash
uv run --extra dev pytest
```
