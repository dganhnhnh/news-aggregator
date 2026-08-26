# Measurements

Chỉ số vận hành và data-quality được ghi lại theo từng task của
[week2-mvp.md](week2-mvp.md). Mỗi mục ghi ngày đo, câu lệnh dùng để đo, và số
liệu — không suy diễn xu hướng từ một lần đo.

---

## T0 — Data-quality remediation (2026-08-26)

T0 mới được chèn trước data audit vì block page giống nhau không phải duplicate:
nếu để chúng đi vào embedding, hệ thống sẽ gộp nhầm nhiều URL article thật chỉ
vì crawler nhận cùng một interstitial. Đã thực hiện:

- thêm CloudFront title `ERROR: The request could not be satisfied` vào quality gate;
- bỏ `js_challenge`/`solution`/`token` khỏi normalized URL riêng cho Reddit;
- chạy `news-worker cleanup-block-pages` dry-run rồi `--apply` trên database local.

Cleanup tìm và quarantine **42** summary-only row: 18 Engadget/CloudFront, 15
InfoQ/Imperva và 9 NYT/DataDome. Không xoá article/feed summary; chỉ xoá title không
tin cậy, invalidate embedding cũ, đảm bảo embed job dùng summary sạch và tạo/replay
`crawl_failures` để URL được crawl lại. Dry-run ngay sau apply trả về 0 row, chứng minh
command idempotent trên snapshot này.

```bash
uv run --env-file .env news-worker cleanup-block-pages --limit 100
uv run --env-file .env news-worker cleanup-block-pages --limit 100 --apply
uv run --env-file .env news-worker cleanup-block-pages --limit 100
```

## T1 — Data audit (2026-08-26)

Snapshot chạy trên `news-radar-db-1` (compose Postgres, `DATABASE_URL` trong
`.env`, port 5433) bằng `docker exec ... psql`. Không có `psql` client trên
host nên mọi câu lệnh dưới đây chạy qua `docker exec`. Các số liệu bên dưới là
baseline chụp **trước** khi T0 apply cleanup; giữ nguyên để so sánh thay vì viết lại
lịch sử measurement.

### Coverage tổng quan

| Bảng | Số dòng |
| --- | --- |
| `raw_articles` | 575 |
| `sources` | 95 |
| `article_embeddings` | **0** |
| `feed_state` | 68 |
| `crawl_failures` | 125 |

```sql
SELECT 'raw_articles' t, count(*) FROM raw_articles
UNION ALL SELECT 'sources', count(*) FROM sources
UNION ALL SELECT 'article_embeddings', count(*) FROM article_embeddings
UNION ALL SELECT 'feed_state', count(*) FROM feed_state
UNION ALL SELECT 'crawl_failures', count(*) FROM crawl_failures;
```

**Phát hiện quan trọng nhất: `article_embeddings` đang rỗng.** `worker_runs`
vẫn còn bốn lần chạy `embed` thành công gần nhất ngày 2026-08-21, lần lớn
nhất ghi `"embedded": 3299`, nhưng `raw_articles` hiện chỉ có 575 dòng và
`article_embeddings` có 0. Corpus 3299 bài đã embed đó không còn tồn tại —
`raw_articles` đã bị thu nhỏ (dev reset/re-seed) sau ngày embed, và
`article_embeddings` bị xoá theo `ON DELETE CASCADE`. **Trước khi bắt đầu T3
(cluster) hoặc T6 (classify), phải chạy lại `news-worker embed` trên 575 bài
hiện có** — nếu không hai worker mới sẽ không có gì để claim.

```sql
SELECT id, status, started_at, stats FROM worker_runs
WHERE worker = 'embed' ORDER BY started_at DESC LIMIT 4;
```

### Time range & undated rate

```sql
SELECT min(event_at), max(event_at), count(*) total,
       count(published_at) has_pub,
       count(*) - count(published_at) undated,
       round(100.0*(count(*)-count(published_at))/count(*), 1) undated_pct
FROM raw_articles;
```

| min(event_at) | max(event_at) | total | undated | undated % |
| --- | --- | --- | --- | --- |
| 2026-08-21 17:30 | 2026-08-26 14:44 | 575 | 23 | 4.0% |

Cửa sổ dữ liệu thực tế chỉ ~5 ngày (117h) — ngắn hơn `cluster_window` (72h)
cộng biên an toàn, nên corpus hiện có đủ để dựng golden case cho invariant
"trong 72h" nhưng case "cách nhau quá 72h" (scenario 9) phải set timestamp
tổng hợp trong fixture thay vì dùng đúng `event_at` gốc của hai bài — xem ghi
chú trong fixture.

### `content_source` distribution

```sql
SELECT content_source, count(*), round(100.0*count(*)/sum(count(*)) over(),1)
FROM raw_articles GROUP BY 1 ORDER BY 2 DESC;
```

| content_source | count | % |
| --- | --- | --- |
| crawl | 374 | 65.0% |
| feed_content | 134 | 23.3% |
| feed_summary | 67 | 11.7% |

`feed_summary` (67 dòng, 11.7%) là các bài crawl thất bại nhưng dek vẫn được
lưu theo đúng thiết kế ở [CLAUDE.md](../CLAUDE.md) — không phải bug.

### Content-length anomalies

```sql
SELECT count(*) FILTER (WHERE length(content)=0) empty,
       count(*) FILTER (WHERE length(content) BETWEEN 1 AND 200) tiny,
       count(*) FILTER (WHERE length(content) BETWEEN 201 AND 800) short,
       count(*) FILTER (WHERE length(content)>800) ok,
       round(avg(length(content))) avg_len,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY length(content)) median
FROM raw_articles;
```

| empty (0 chars) | tiny (1–200) | short (201–800) | ok (>800) | avg | median |
| --- | --- | --- | --- | --- | --- |
| 67 | 0 | 7 | 501 | 13,496 | 5,743 |

67 dòng rỗng khớp chính xác với 67 dòng `content_source='feed_summary'` ở
trên — không có dòng content=0 nào thuộc loại khác. `lang` chưa được populate
cho bất kỳ dòng nào (`lang IS NULL` toàn bộ 575), khớp với quyết định §14.2
của kế hoạch: chưa suy ra ngôn ngữ từ cột này.

### Exact-duplicate title/content

```sql
SELECT count(*) dup_title_groups, sum(c-1) redundant_rows FROM (
  SELECT title, count(*) c FROM raw_articles
  WHERE coalesce(title,'')<>'' GROUP BY title HAVING count(*)>1
) t;
-- 8 nhóm, 59 dòng dư
```

| Nhóm title trùng | Số dòng | Nguyên nhân |
| --- | --- | --- |
| "ERROR: The request could not be satisfied" | 18 | Block page — engadget.com |
| "Human Verification" | 15 | Block page — infoq.com |
| "What Zig felt like, coming from Rust" | 14 | **Crawl bug**, xem dưới |
| "nytimes.com" | 9 | Block/paywall shell — title = domain |
| "Thoreau BASIC by Tarjan" | 4 | itch.io — cùng URL, crawl lặp lại |
| "EVE Online: The Move to Python 3 Begins!" | 3 | reddit.com, cùng bug js_challenge |
| "Americans Want to Know..." (cnet) | 2 | cùng URL, crawl lặp |
| "Stop Using Conventional Commits" | 2 | reddit.com, cùng bug js_challenge |

**Bug đáng chú ý — Reddit `js_challenge` URL không ổn định.** 18 dòng có URL
chứa `js_challenge=1&...&solution=...&token=...`; các tham số `solution`/
`token` đổi giá trị mỗi lần crawl lại cùng một thread Reddit (bot-challenge
token), nên `UNIQUE(raw_articles.url)` không phát hiện được đây là cùng một
bài — mỗi lần crawl tạo một dòng "mới". Bốn thread Reddit bị ảnh hưởng, nặng
nhất là một thread bị crawl 14 lần trong cùng một giờ
(`2026-08-24 10:06:30+00`). Nội dung các dòng này **không
phải trang chặn** — đây là trang Reddit thật (nav + bài share), độ dài dao
động 16,961–83,563 ký tự tuỳ challenge lần đó render bao nhiêu. Ghi lại ở đây
để đánh giá phương án L2 trong backlog nếu duplicate vẫn còn sau canonicalization;
không mặc định cần SimHash chỉ vì dữ liệu lịch sử từng có lỗi URL identity.

```sql
SELECT split_part(url,'?',1) base, count(*)
FROM raw_articles WHERE url LIKE '%js_challenge=%' GROUP BY 1 ORDER BY 2 DESC;
--  .../1vwyjg2/what_zig_felt_like_coming_from_rust            | 14
--  .../1vxz0z4/eve_online_the_move_to_python_3_begins          | 2
--  .../1vwfrbg/stop_using_conventional_commits                 | 1
--  .../1vynzwf/casey_muratori_the_root_of_the_root_of_all_evil | 1
```

Không có `content_hash` trùng ngoài 3 nhóm nhỏ (cùng URL crawl lại, vô hại).

### CAPTCHA / block-page / rỗng cần lọc trước khi chọn golden input

```sql
SELECT count(*) FROM raw_articles
WHERE lower(title) ~ 'human verification|could not be satisfied|just a moment|access denied'
   OR length(content) = 0;
-- 67
```

67/575 dòng (11.7%) là block-page hoặc rỗng — loại hết trước khi chọn golden
scenario. Không có dòng nào khớp `core.content_quality.BLOCK_PAGE_TITLES`
khác ngoài "Human Verification"/"ERROR: The request could not be satisfied"
(bộ lọc thứ hai không nằm trong `BLOCK_PAGE_TITLES` — cân nhắc thêm ở tuần 3
nếu engadget tiếp tục xuất hiện).

### Multi-publisher same-event coverage

Corpus tuần này có nhiều sự kiện được từ 2–7 publisher đưa tin cùng lúc (ví
dụ: Apple công bố Mac mini M6 / Mac Studio M5 Ultra ngày 2026-08-25 — theverge,
cnet, mashable, theregister, apple.com, stratechery, hardware.slashdot đều có
bài; vụ AliExpress bị tố dùng audio fingerprinting — theregister, arstechnica,
yro.slashdot.org). Corpus đủ phong phú để chọn golden case thật, không cần bịa
dữ liệu tổng hợp.

### Golden scenario input selection

10 scenario theo §12.3 của kế hoạch được chọn từ dữ liệu thật ở trên (không
tham chiếu `raw_articles.id`, không fetch URL live — chỉ copy title/excerpt
ngắn/source/timestamp vào fixture). Chi tiết từng case, lý do chọn, và expected
result (ghi **trước khi chạy model**, theo yêu cầu §12.2) nằm trong
[`backend/tests/fixtures/week2_golden_cases.json`](../backend/tests/fixtures/week2_golden_cases.json).

| # | case_id | Nguồn thật |
| - | --- | --- |
| 1 | `same_event_two_publishers` | Apple Mac Studio/mini M6 — theverge + cnet |
| 2 | `same_event_strong_paraphrase` | AliExpress fingerprinting — theregister + arstechnica |
| 3 | `same_topic_different_event` | Apple Mac Studio launch vs AirTags discount |
| 4 | `same_publisher_duplicate` | Reddit js_challenge bug — cùng thread crawl 2 lần |
| 5 | `near_identical_cross_publisher` | Antibody-image story — arstechnica + science.slashdot |
| 6 | `multi_label_article` | "AI vendors turning to custom hardware..." — theregister |
| 7 | `hard_negative_keyword` | AirTags discount — chứa "AirTags"/"Air" (⊃ "AI") |
| 8 | `outside_taxonomy` | Peppermint oil cardiometabolic study — journals.plos.org |
| 9 | `similar_but_beyond_window` | Mac Studio articles, timestamp tổng hợp cách nhau >72h |
| 10 | `arrival_order_invariance` | Case 1, chạy theo thứ tự đảo |

**Ghi chú fixture:** case 9 dùng cùng cặp bài case 1 nhưng gán lại
`published_at` tổng hợp (cách nhau 4 ngày) — vì corpus thật hiện chỉ trải dài
~5 ngày nên không có cặp "semantic giống nhau, event khác nhau, cách nhau
>72h" tự nhiên trong 575 bài hiện có. Đây là lựa chọn có chủ đích, không phải
tham chiếu timestamp live.

**Chưa tính:** F1/precision/recall (không nằm trong scope T1/T9 theo §12.6).

---

## Chỗ trống cho các task sau (T2–T9)

Các bảng dưới đây được tạo trước theo §12.6 của kế hoạch; điền số liệu khi
task tương ứng hoàn thành. Không điền trước khi có code thật chạy.

| Hạng mục | Số đo cần ghi | Task | Trạng thái |
| --- | --- | --- | --- |
| Cluster | p50/p95 candidate query, articles/s, size distribution | T3–T4 | chưa đo |
| Classifier | articles/s, confidence histogram, uncategorized rate | T6 | chưa đo |
| Taxonomy | prototype rebuild time/topic, reclassify time/1k articles | T5 | chưa đo |
| API | stories p50/p95, query plan cho topic/source filter | T7 | chưa đo |
| Golden | N/10 case pass, `case_id` fail + reason | T9 | chưa đo |

## Số đo phục vụ quyết định backlog L2/L3

Các số đo này không phải acceptance gate của task Tuần 2. Chỉ thu thập sau khi L1 URL
identity, quality gate và clustering MVP đã chạy trên corpus sạch hơn.

| Candidate | Số đo cần có trước khi quyết định | Trạng thái |
| --- | --- | --- |
| L2 SimHash | duplicate rate còn lại theo source, embedding cost lãng phí, false-positive sample, candidates/article | backlog, chưa đo |
| L3 semantic | same-source repetition, ảnh hưởng centroid/headline/UX, cross-source evidence retained | backlog, chưa đo |
