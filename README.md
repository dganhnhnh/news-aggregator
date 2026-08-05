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
```

Kết quả được ghi ra stdout dưới dạng JSON Lines để có thể pipe sang storage hoặc
worker khác mà không gắn chặt engine vào database.

## Chạy service định kỳ

Service hiện chạy pipeline Google News RSS theo chu kỳ:

```bash
uv run news-service --tsv-path ./queries.tsv --interval-seconds 1800
uv run news-service --tsv-path ./queries.tsv --once
```

File TSV dùng cho Google News cần tối thiểu 4 cột; pipeline đọc nhãn truy vấn ở
cột thứ 3 và RSS URL ở cột thứ 4. Dòng trống và dòng bắt đầu bằng `#` được bỏ qua.

## Kiểm tra

```bash
uv run --extra dev pytest
```
