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

## T6 — Classifier coverage và hạ threshold `0.55 → 0.40` (2026-08-28)

Đo trên snapshot `news-radar-db-1`: **1015** article đã embed, **8** active topic,
mỗi topic có prototype compile từ 5 exemplar. Công thức không đổi:
`0.80 * prototype_similarity + 0.20 * keyword_score`.

### Phân bố score theo topic (ở threshold cũ `0.55`)

| topic | n | mean | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- |
| `artificial-intelligence` | 1015 | 0.166 | 0.132 | 0.385 | 0.492 | **0.613** |
| `consumer-hardware` | 1015 | 0.143 | 0.129 | 0.273 | 0.403 | 0.538 |
| `cybersecurity` | 1015 | 0.136 | 0.123 | 0.244 | 0.452 | **0.615** |
| `cloud-infrastructure` | 1015 | 0.124 | 0.114 | 0.228 | 0.393 | 0.569 |
| `open-source` | 1015 | 0.108 | 0.093 | 0.211 | 0.374 | 0.573 |
| `big-tech-business` | 1015 | 0.107 | 0.101 | 0.203 | 0.293 | 0.426 |
| `programming-languages` | 1015 | 0.103 | 0.091 | 0.217 | 0.330 | 0.567 |
| `software-engineering` | 1015 | 0.092 | 0.083 | 0.190 | 0.314 | 0.483 |

```sql
SELECT t.slug, count(*) n,
       round(avg(at.score)::numeric,3) mean,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY at.score)::numeric,3) p50,
       round(percentile_cont(0.90) WITHIN GROUP (ORDER BY at.score)::numeric,3) p90,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY at.score)::numeric,3) p99,
       round(max(at.score)::numeric,3) max
FROM article_topics at JOIN topics t ON t.id = at.topic_id GROUP BY 1;
```

**Phát hiện chính:** score cao nhất toàn corpus là `0.615`, còn p99 của topic mạnh nhất
mới là `0.492`. Với trọng số `0.80`, threshold `0.55` đòi hỏi cosine prototype ≥
`0.6875` — MiniLM gần như không sinh ra giá trị đó giữa một article thật và prototype
trung bình hoá từ 5 exemplar. Ngưỡng cũ nằm ngoài dải mà công thức có thể đạt tới, nên
nó không phải "ngưỡng chặt" mà là ngưỡng tắt classifier.

### Sweep

| threshold | article có nhãn | coverage | nhãn/article đã gán |
| --- | --- | --- | --- |
| 0.55 | 10 | 1.0% | 1.00 |
| 0.50 | 23 | 2.3% | 1.00 |
| 0.45 | 59 | 5.8% | 1.00 |
| **0.40** | **129** | **12.7%** | **1.07** |
| 0.35 | 220 | 21.7% | 1.12 |
| 0.30 | 336 | 33.1% | 1.22 |

```sql
WITH best AS (SELECT article_id, max(score) top_score FROM article_topics GROUP BY 1),
     th AS (SELECT generate_series(30,55,1)/100.0 t)
SELECT th.t, count(*) FILTER (WHERE b.top_score >= th.t)
FROM th CROSS JOIN best b GROUP BY th.t ORDER BY th.t;
```

### Kiểm tra title bằng tay

Không có ground-truth label nên precision được ước lượng bằng cách đọc title theo từng
dải score (~150 title, các topic `artificial-intelligence`, `cybersecurity`,
`consumer-hardware`, `open-source`, `cloud-infrastructure`):

| dải score | quan sát |
| --- | --- |
| `≥ 0.45` | gần như không thấy false positive |
| `0.40–0.45` | vẫn đúng beat; vài case biên (bài deal "5TB cloud storage" vào `cloud-infrastructure`) |
| `0.35–0.40` | bắt đầu lẫn: "Compose Multiplatform 1.12.0", "CLion Roadmap", "Fitbit Air Pokémon Sleep" rơi vào `artificial-intelligence` |
| `0.30–0.35` | lẫn rõ: repo GitHub không liên quan, "Google Search cho trang trí nhà", "Google Play tại Gamescom" |

`0.40` là điểm cuối cùng trước khi nhiễu xuất hiện đều, đồng thời là điểm thấp nhất mà
**mọi** topic đều còn ít nhất một bài — không beat nào chết.

### Kết quả sau khi đổi

`news-worker taxonomy-import` phát hiện threshold đổi và tăng `classifier_revision`
`1 → 2` cho cả 8 topic (prototype không rebuild vì exemplar không đổi), nên
`--stale` requeue đúng toàn bộ 1015 article.

```bash
uv run news-worker taxonomy-import --file data/taxonomy.seed.yaml
uv run news-worker classify --last 30d --stale --once
```

| số đo | trước (`0.55`) | sau (`0.40`) |
| --- | --- | --- |
| article có nhãn | 10 (1.0%) | 129 (12.7%) |
| uncategorized | 1005 | 886 |
| cluster có nhãn | 10 | 105 |
| topic có ≥1 bài | 5/8 | 8/8 |
| throughput | 1015 article / 9.2s (~110 article/s) | như trước |

Phân bố theo topic sau khi đổi: `artificial-intelligence` 83, `cybersecurity` 20,
`consumer-hardware` 12, `cloud-infrastructure` 10, `open-source` 6,
`programming-languages` 3, `software-engineering` 3, `big-tech-business` 1.

**Chưa tính:** precision/recall/F1 có số. Ước lượng ở trên là đọc title thủ công trên
sample không cố định, không phải metric — frozen test split 100–150 bài và sweep đầy đủ
vẫn thuộc Tuần 4 theo §12.6. `0.80/0.20` giữ nguyên; chỉ threshold đổi.

---

## T9 — Golden semantic runner (2026-08-30)

Runner dùng model production `sentence-transformers/all-MiniLM-L6-v2` (384 chiều),
adapter `fastembed 0.8.0`, đúng `services.text.article_embed_text`, classifier
`0.80 × prototype + 0.20 × keyword`, threshold theo seed (`0.40`) và clustering
`0.75 / merge 0.85 / window 72h`. Test chỉ assert decision/member set; không assert
float score và không tính precision/recall/F1.

```bash
uv run --extra dev pytest backend/tests/test_semantic_golden.py -m semantic -q
# 10 passed
```

| Số đo | Kết quả |
| --- | --- |
| Golden scenarios | **10/10 pass** |
| `case_id` fail cuối cùng | Không có |
| Arrival order | forward/reverse cho case 10 có cùng canonical member set |
| Default suite | 315 pass, 33 skip, 10 semantic deselected |

Lần chạy đầu tiên cố ý giữ nguyên expectation đã chốt và trả `0/10`: fixture còn
`member_kind=near_duplicate` dù baseline §2.5 giữ mọi member, alias cũ
`privacy-security` không khớp slug runtime `cybersecurity`, và seed 8 topic thiếu
`semiconductors`/`science-research` cùng signal cho các case hardware, language và
browser fingerprinting. Clustering member set thực tế đã đúng cả 10 case ngay từ lần
chẩn đoán đầu; failure nằm ở taxonomy/classification input.

Cách xử lý theo thứ tự data/input của §12.2:

- bỏ field `expected_member_kinds` đã nằm ngoài schema MVP, nhưng giữ expectation hai
  article cùng publisher đều là member và `n_distinct_sources=1`;
- chuẩn hóa alias fixture `privacy-security → cybersecurity`;
- mở seed từ 8 lên 10 technical beat bằng `semiconductors` và `science-research`, rồi
  bổ sung exemplar/keyword domain cho custom silicon, research integrity, browser
  fingerprinting, Rust/Zig, Mac và tracking accessory;
- giữ nguyên expected cluster/topic intent, model, công thức, threshold `0.40` và mọi
  cluster threshold; không sửa expected theo float output.

Database đã import seed cũ cần chạy lại `taxonomy-import`, `taxonomy-rebuild`, rồi
reclassify stale article để runtime nhận hai topic và prototype revision mới. T9 runner
tự compile trực tiếp seed đã check-in nên không ghi database.

---

## T10 — Compose và MVP smoke (2026-08-30)

Đã validate `docker compose config`, build image `news-radar-api`, rồi khởi động
`db` + `api` bằng compose sau khi schema hiện có đã ở migration head. Compose không
chạy Alembic, taxonomy import, worker hoặc scheduler tự động.

| Gate | Kết quả |
| --- | --- |
| Compose DB | PostgreSQL/pgvector healthy, host port 5433 |
| Compose API | image build pass, `/health` và `/health/db` trả 200, container healthy |
| Taxonomy API | `/api/topics` trả default taxonomy và 10 active topic |
| Stories API | `/api/stories?limit=2` trả story/member/source data |
| Frontend | `pnpm build` pass với Next.js 16.3.0 + webpack, route manifest được generate đầy đủ |
| Backend default suite | 315 pass, 33 skip, 10 semantic deselected |
| Golden suite | 10/10 pass |

Snapshot smoke của database local sau khi drain worker:

| Số đo | Kết quả |
| --- | --- |
| Processing jobs | embed/classify/cluster đều 1,195 `done`, không còn state khác |
| Prototype | 10 topic, MiniLM 384 chiều |
| Classifier coverage | 186/1,195 article có nhãn (15.6%); 12 article đa nhãn |
| Uncategorized | 1,009/1,195 (84.4%) |
| Canonical/non-merged cluster | 993 tổng; 875 singleton; 108 cluster có ≥2 article |
| Cluster lớn nhất | 17 article |
| Article thuộc nhiều cluster | 0 |

Build frontend ban đầu lộ hai dependency môi trường: `next/font/google` cần network và
Turbopack/PostCSS bind cổng tạm trong sandbox. UI chuyển sang system font stack đã có
sẵn và build script cố định `next build --webpack`; không thay đổi API/UI behavior.

README root hiện là runbook để migrate, import/rebuild taxonomy, drain từng worker,
smoke SQL/API, mở Stories và chạy golden suite. Production scheduler, image slimming
và container hardening vẫn để sau MVP đúng scope T10.

---

## T11 — Baseline ingest trước khi song song hoá crawl (2026-09-03)

Đo trước khi quyết định có tách `news_ingest` thành planner tuần tự + N crawl
worker hay không. Harness: [`scripts/bench_ingest.py`](../scripts/bench_ingest.py),
đọc input từ database nhưng **không ghi gì** — không `apply_batch`, không
`record_feed_poll`, không `worker_runs` — nên phép đo không làm lệch lần chạy
production kế tiếp. Database: `news_radar` local (port 5433), snapshot 7 ngày.

### T11.1 — Độ trễ hiện tại (`published_at` → `crawled_at`)

| Tập | n | p25 | p50 | p75 | p90 |
| --- | --- | --- | --- | --- | --- |
| Toàn bộ 7 ngày | 1100 | — | **305 min** | — | 984 min |
| Chỉ source ≥10 bài/7d | 1035 | 96 min | 305 min | — | 925 min |
| Chỉ giai đoạn cron chạy đều (2026-09-02 09:00–16:30Z) | 197 | 43 min | **122 min** | 321 min | 618 min |

### T11.2 — Nhịp chạy thực tế của worker `crawl`

| Chỉ số | Giá trị |
| --- | --- |
| Số run / 7 ngày | 72 |
| Khoảng cách giữa 2 run — p50 | 31.7 min (đúng thiết kế) |
| Khoảng cách — p90 | **197.8 min** |
| Khoảng cách — max | **2384 min (~40 giờ)** |

Có một lỗ 12 tiếng liền (2026-09-02 16:00Z → 2026-09-03 04:00Z) và một ngày
(2026-09-01) không có run nào. Chênh lệch p50 305 min (cả tuần) so với 122 min
(giai đoạn cron đều) là phần độ trễ do **scheduler không chạy**, không phải do
throughput. Song song hoá không chạm được vào phần này.

### T11.3 — Concurrency polling feed: barrier so với sliding window

80 feed active lấy mẫu ngẫu nhiên (`--seed 11`), cùng một tập cho mọi mức, thứ
tự mức được xáo để cold-start không trùng với chiều tăng của biến.

```bash
uv run python scripts/bench_ingest.py feeds   --sample 80 --levels 16,2,24,8 --seed 11
uv run python scripts/bench_ingest.py sliding --sample 80 --levels 24,8,2     --seed 11
```

| Concurrency | `poll_feeds` hiện tại (gather barrier) | Sliding window | Tỉ lệ |
| --- | --- | --- | --- |
| 2 | 61.0s | 27.2s | 2.2× |
| 8 | 58.3s | **16.2s** | **3.6×** |
| 16 | 101.4s (vòng chạy đầu, cold) | — | — |
| 24 | 56.2s | 20.2s (cold) → 15.4s | 3.6× |

Thời lượng fetch từng feed (sliding, c=8): p50 **0.36s**, p90 1.17s, max 15.05s.

Lần chạy đầu ở mỗi mode bị phạt vì cold DNS/TLS (barrier c=16 = 101.4s; sliding
c=24 = 20.2s). Chạy lại sliding với thứ tự `8,24,12` để loại yếu tố này:

| Concurrency | Sliding (vòng 2) | "Ideal" = Σ(thời lượng từng feed)/c |
| --- | --- | --- |
| 8 | 17.2s | 7.8s |
| 12 | 16.3s | 4.9s |
| 24 | 15.4s | 3.3s |

Không có nghịch đảo — nhưng từ c≈8 trở lên tổng thời gian **bị ghim bởi một
fetch chậm nhất** (max 15.06s ở cả ba mức), nên concurrency thêm không mua được
gì. Đòn bẩy tiếp theo là hạ timeout 20s, không phải nâng concurrency.

**Phân bố đuôi — chọn timeout bao nhiêu.** Số feed vượt mỗi ngưỡng, hai lần
chạy độc lập trên cùng 80 feed:

| Lần chạy | >3s | >5s | >8s | >10s | >15s | failed |
| --- | --- | --- | --- | --- | --- | --- |
| c=8 | 5 | 5 | 4 | 4 | 4 | 6 |
| c=12 | 9 | 7 | 7 | 7 | 7 | 11 |

Đuôi **lưỡng cực**: gần như mọi feed xong dưới 3s, phần còn lại treo ở ~15.1s.
**Không có feed nào nằm giữa 8s và 15s ở cả hai lần chạy**, và số feed ở cụm
15s luôn nhỏ hơn số feed failed — tức cụm đó là feed đang hỏng sẵn, không phải
feed chậm mà lành. Trần 15.1s nằm dưới timeout httpx 20s nên nó đến từ tầng
resolver/connect của OS; timeout httpx 8s sẽ cắt trước. Kết luận: **hạ timeout
xuống 8s không làm hỏng feed lành nào** trong mẫu này, chỉ rút ngắn thời gian
thất bại từ 15s xuống 8s.

Hai kết luận, cả hai đều ngược với giả định ban đầu:

1. **Nâng `MAX_CONCURRENT_FEEDS` một mình gần như vô ích**: 2 → 24 chỉ đưa 61.0s
   xuống 56.2s (8%). Nguyên nhân ở
   [`poll_feeds`](../backend/adapters/retrieve_news_data/google_news_rss_searcher.py:520):
   `for feed_group in self._chunks(...)` + `asyncio.gather` là một **barrier** —
   mỗi nhóm phải xong hết mới sang nhóm sau, nên chi phí mỗi nhóm bằng thành
   viên chậm nhất. Với p50 0.36s và timeout 20s, tăng N chỉ gom thêm outlier vào
   cùng một nhóm.
2. **Sliding window mới là đòn bẩy**, và điểm ngọt là c≈8: từ đó trở lên tổng
   thời gian bị ghim bởi fetch chậm nhất chứ không giảm thêm. Sàn đó là timeout
   20s hardcode tại
   [google_news_rss_searcher.py:371](../backend/adapters/retrieve_news_data/google_news_rss_searcher.py:371).
   Với p90 = 1.3s, hạ timeout xuống 8s ảnh hưởng rất ít feed nhưng kéo sàn
   xuống gần mức "ideal" 4.9s.

### T11.4 — Concurrency crawl (crawl4ai)

24 URL bài viết đã crawl gần đây, mỗi host một URL (`--seed 3`, thứ tự mức xáo).

```bash
uv run python scripts/bench_ingest.py crawls --sample 24 --levels 4,2,8,6 --seed 3
```

| Concurrency | Tổng | s/URL |
| --- | --- | --- |
| 2 | 28.3s | 1.18 |
| 4 | 19.7s | 0.82 |
| 6 | **16.9s** | **0.71** |
| 8 | 18.2s | 0.76 |

2 → 6 cho **1.67×**; từ 6 trở lên không còn lợi. Đây là kịch bản tốt nhất (mọi
host khác nhau); run thật chậm hơn ~2.6× vì có failure, `_host_waves` và chunk
barrier `crawl_batch_size`.

### T11.5 — Độ lệch theo host (đầu vào cho bài toán chia tải)

```bash
uv run python scripts/bench_ingest.py hosts
```

210 host / 1104 bài trong 7 ngày. p50 = **1 bài/host**, max = 210
(mashable.com), **top-10 host chiếm 67.8%**. Theo từng run:

| run | bài | host | max/host | % host lớn nhất |
| --- | --- | --- | --- | --- |
| 911 | 86 | 23 | 15 | 17.4% |
| 881 | 54 | 26 | 9 | 16.7% |
| 852 | 63 | 23 | 14 | 22.2% |

Với `max_crawls_per_host=1` và delay 1s, host đông nhất của run 911 tạo **sàn
~112s** cho pha crawl (15 × (6.5s + 1s)) mà không worker nào vượt qua được.

### T11.6 — Feed nào thực sự xuất bản

| Nhóm (375 feed active) | Số feed |
| --- | --- |
| Có bài trong 24h | **37** |
| Có bài trong 7 ngày (không tính 24h) | 42 |
| Có bài trong 30 ngày (không tính 7d) | 58 |
| Im lặng > 30 ngày | **221 (59%)** |
| Chưa biết (`last_entry_published_at IS NULL`) | 17 |

Đồng thời: 251/375 có `etag`, 239 có `last_modified`, **79 (21%) không có
validator nào** → luôn trả 200 kèm full body. Tỉ lệ 304 thực tế 50.4%.
Feed failure trung bình **77/368 mỗi run** (21%); phân loại trong
`crawl_failures`: HTTPStatusError 60, ConnectError 43, ValueError 20,
ConnectTimeout 10.

Poll đều 30 phút cho cả 375 feed = 18.000 lượt/ngày, trong đó phần lớn dành cho
221 feed đã im lặng hơn một tháng.

### T11.7 — Mô hình wall clock và dự phóng

Kiểm chứng mô hình trên run 911 (402 feed, 152 fetched, 97 lượt crawl, 647s đo được):

| Pha | Hiện tại | Mô hình |
| --- | --- | --- |
| Poll feed | ~307s | 402 feed × (61.0s / 80) |
| Crawl | ~300s | 97 URL × 3.1 s/URL |
| Tổng | 607s (mô hình) so với **647s (đo)** | sai số 6% |

Dự phóng sau khi sửa (sliding window c=8 + `MAX_CONCURRENT_CRAWLS=6`):

| Pha | Sau khi sửa |
| --- | --- |
| Poll feed | ~81s (3.8×) |
| Crawl | ~180s (1.67×, sàn host 112s chưa chạm) |
| **Tổng** | **~260s so với 647s — 2.5×** |

Toàn bộ mức cải thiện này đạt được **trong một process**, bằng một thay đổi code
nhỏ (bỏ barrier) cộng hai biến môi trường. Thêm N crawl worker chỉ có thể lấy
thêm tối đa ~1.6× nữa trên pha crawl trước khi chạm sàn per-host 112s ở T11.5.

### T11.8 — Nhịp planner 5 phút: chi phí và lợi ích

`due_feeds()` lọc `next_poll_at <= now`, còn `record_feed_poll` đặt
`next_poll_at = now + FEED_POLL_INTERVAL_SECONDS` (1800s). Độ cũ của feed đo
được: p50 **26.4 min**, p90 29.6 min — tức là interval 30 phút đang là ràng buộc
thật sự và đang được tôn trọng, chứ không phải nhịp cron.

Vì vậy độ trễ lập lịch ≈ `interval/2 + cadence/2`:

| Cấu hình | interval | cadence | Độ trễ lập lịch kỳ vọng | Feed due mỗi chu kỳ |
| --- | --- | --- | --- | --- |
| Hiện tại | 30 min | 30 min | 30 min | ~375 |
| Chỉ đổi nhịp planner | 30 min | 5 min | 17.5 min | ~62 |
| Đổi cả hai (đồng loạt) | 10 min | 5 min | 7.5 min | ~190 |
| Adaptive theo T11.6 | 5 min / 30 min / 6h | 5 min | ~5 min cho feed có bài | ~50 |

Ngân sách poll mỗi ngày:

| Chiến lược | Lượt poll/ngày |
| --- | --- |
| Hiện tại (375 feed × 48) | 18.000 |
| Hạ interval đồng loạt xuống 10 phút | 54.000 (3×) |
| Adaptive: 37 feed × 5min + 42 × 30min + 296 × 6h | **13.856 (ít hơn hôm nay)** |

Adaptive interval vừa giảm tải publisher vừa cho nhóm 37 feed thực sự có bài
được poll dày gấp 6 lần. Đây là đòn bẩy độ trễ lớn nhất còn lại sau khi
scheduler chạy ổn định (T11.2), và nó không cần thêm worker nào.

### T12 — Cadence từ RSS fetch on-demand (2026-09-03)

Đo lại policy adaptive bằng chính nội dung RSS vừa fetch thay vì suy ra từ
`last_entry_published_at` trong database. Command gửi request unconditional
(không ETag/Last-Modified) đúng một lần cho mỗi feed, không ghi database:

```bash
uv run python scripts/bench_ingest.py cadence --sample 0 --concurrency 8 --timeout 20
```

Một publication event là một timestamp phân biệt sau khi bỏ entry trùng URL.
Policy yêu cầu tối thiểu 3 gap; thiếu bằng chứng ở tier 5m. Boundary là strict:
gap đúng 30m vào 5m, gap đúng 6h vào 30m.

| Chỉ số | Giá trị |
| --- | --- |
| Feed active | 369 |
| Fetch thành công / thất bại | 333 / 36 |
| Đủ ≥3 gap / thiếu mẫu | **320 / 13** |
| p50 số gap/feed thành công | 11 |
| Có batch trùng timestamp | 60 feed |
| Có / không có validator trong response mới | 290 / 43 |
| Thời gian fetch toàn bộ, c=8 | 101.9s |

Phân bố tier và budget; cột upper bound giả sử cả 36 fetch lỗi đều ở 5m. Đây
là trần bảo thủ — production vẫn dùng retry backoff nên số request thật thấp
hơn. Full-body bytes/ngày chỉ cộng 43 feed thành công không có validator và là
kích thước body đã decompress, không phải network transfer chính xác.

| Estimator | 5m | 30m | 6h | Poll/ngày, 333 feed thành công | Upper bound, cả 369 | Full body/ngày |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Mean | 13 | 10 | 310 | 5.464 | 15.832 | 43.6 MB |
| Median | 17 | 14 | 302 | 6.776 | 17.144 | 50.7 MB |
| Trimmed mean 10% | 14 | 12 | 307 | **5.836** | **16.204** | **47.0 MB** |

Baseline cố định 30m là 15.984 poll/ngày trên cùng 333 feed thành công, hoặc
17.712 trên cả 369 feed. Ngay cả upper bound của trimmed mean vẫn thấp hơn
baseline toàn tập 1.508 request/ngày (8.5%). Có 9 feed newest entry đã cũ hơn
24h nhưng vẫn vào tier 5m dưới cả ba estimator: cadence lịch sử không phát hiện
feed đã ngừng xuất bản. Tuy nhiên 9 feed này chưa làm budget vượt baseline, nên
chưa đủ căn cứ thêm idle-backoff trái với invariant strict của policy.

**Hướng chốt:** dùng trimmed mean 10% làm estimator ban đầu vì nó vẫn là
"khoảng cách trung bình" nhưng giảm ảnh hưởng của hai đầu phân bố; tối thiểu 3
gap; thiếu mẫu ở 5m; giữ tier cũ khi 304; fetch unconditional đúng một lần để
bootstrap. Lưu metric, sample count và tier trong `feed_state` để 304 có thể
dùng lại và để audit được invariant. Theo dõi riêng 9 feed stale/5m sau rollout;
chỉ thêm idle-backoff nếu actual poll/byte budget hoặc publisher load vượt gate.



Các bảng dưới đây được tạo trước theo §12.6 của kế hoạch; điền số liệu khi
task tương ứng hoàn thành. Không điền trước khi có code thật chạy.

| Hạng mục | Số đo cần ghi | Task | Trạng thái |
| --- | --- | --- | --- |
| Cluster | p50/p95 candidate query, articles/s, size distribution | T3–T4 | chưa đo |
| Classifier | articles/s, confidence histogram, uncategorized rate | T6 | đã đo (mục T6) |
| Taxonomy | prototype rebuild time/topic, reclassify time/1k articles | T5 | chưa đo |
| API | stories p50/p95, query plan cho topic/source filter | T7 | chưa đo |
| Golden | N/10 case pass, `case_id` fail + reason | T9 | 10/10, không còn case fail |

## Số đo phục vụ quyết định backlog L2/L3

Các số đo này không phải acceptance gate của task Tuần 2. Chỉ thu thập sau khi L1 URL
identity, quality gate và clustering MVP đã chạy trên corpus sạch hơn.

| Candidate | Số đo cần có trước khi quyết định | Trạng thái |
| --- | --- | --- |
| L2 SimHash | duplicate rate còn lại theo source, embedding cost lãng phí, false-positive sample, candidates/article | backlog, chưa đo |
| L3 semantic | same-source repetition, ảnh hưởng centroid/headline/UX, cross-source evidence retained | backlog, chưa đo |

---

## Resume — Evidence snapshot (2026-09-05)

Đối chiếu cho phần **News Aggregation Platform** trong
[base-resume.json](base-resume.json). Chỉ đọc DB local qua Compose; không chạy
thêm job hoặc sửa dữ liệu. Đây không phải phép đo rollout T13 hay load test.
Hai transaction chụp lúc **16:46:20 và 16:47:09 UTC**, cùng ngày 2026-09-05,
đều trả về 2.278 article.

### Số liệu được dùng trong resume

| Claim | Bằng chứng | Phạm vi |
| --- | --- | --- |
| 2.278 article từ 335 source | `count(*)` và `count(DISTINCT source_id)` trên `raw_articles`; không có `source_id` NULL | Corpus local tại thời điểm chụp, không phải lưu lượng/ngày hay giới hạn tải |
| Giảm 72% thời gian polling (58,3s → 16,2s) | Benchmark T11.3, cùng mẫu 80 feed và concurrency 8; `(58.3 - 16.2) / 58.3 = 72.2%`, làm tròn 72% | Benchmark bước polling ngày 2026-09-03, không phải tăng tốc toàn pipeline; code hiện có bounded worker pool/backpressure trong `RssFeedSearcher.poll_feeds` |
| Classification khoảng 110 article/s | `worker_runs.id = 583`: `classified=1015`, `failed=0`, thời lượng 9,242s; `1015 / 9.242 ≈ 109.8` | Một lần chạy local ngày 2026-08-28, xử lý embedding đã có; không bao gồm crawl, embedding hoặc khởi động container |

T11.3 là kết quả lịch sử của benchmark barrier/sliding trước thay đổi.
`scripts/bench_ingest.py feeds` hiện gọi adapter đã dùng sliding worker pool,
nên chạy lại hai mode trên code hiện tại không tái tạo baseline barrier cũ.

### Query kiểm chứng

Chạy SQL bên dưới qua lệnh (giá trị kết nối lấy trong container, không in `.env`):

```bash
docker compose -f deployments/docker-compose.yml --env-file .env exec -T db \
  sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT now() AS measured_at, count(*) AS articles,
       count(DISTINCT source_id) AS article_sources,
       count(*) FILTER (WHERE source_id IS NULL) AS without_source
FROM raw_articles;

SELECT id, started_at, status, stats->>'classified' AS classified,
       stats->>'failed' AS failed,
       round(extract(epoch FROM finished_at - started_at)::numeric, 3) AS seconds,
       round((stats->>'classified')::numeric /
             extract(epoch FROM finished_at - started_at), 1) AS articles_per_second
FROM worker_runs WHERE id = 583 AND worker = 'classify';

SELECT stage, state, count(*) FROM processing_jobs GROUP BY 1, 2 ORDER BY 1, 2;
SELECT poll_interval_seconds, count(*), count(cadence_checked_at) AS bootstrapped
FROM feed_state WHERE disabled_at IS NULL GROUP BY 1 ORDER BY 1;
COMMIT;
SQL
```

### Đối chiếu triển khai và giới hạn diễn giải

- Corpus có 2.278 embedding, 2.278 classification record và 2.278 cluster member;
  cả ba stage queue đều có 2.278 job `done`, không có state khác ở snapshot.
  Classification record có thể là `uncategorized`; không suy ra 100% bài có topic
  hay độ chính xác của classifier. Có 1.914 cluster chưa merged, trong đó 169
  có nhiều bài và 101 có nhiều source; không gọi tất cả là story đa nguồn.
- `PostgresRawStorage.apply_batch` commit article và enqueue embed trong cùng
  transaction; `upsert_embeddings` commit vector và enqueue classify/cluster
  trong cùng transaction. `PostgresJobQueue` có lease/reclaim, retry/backoff và
  dead-letter; `workers/base.py` có checkpoint và cooperative shutdown. Đây là
  cơ chế phục hồi đã triển khai, không phải bằng chứng exactly-once hay zero loss
  trong mọi trường hợp.
- DAG `news_ingest` có lịch 5 phút và asset gate theo queue; `news_semantic`
  chạy classify/cluster độc lập sau embed. DAG backfill và replay đã có. Không
  claim topic-invariant gate đang hoạt động: gate cuối `news_semantic.py` hiện
  đang bị comment trong working tree.
- Cadence thích ứng đã có trong code nhưng **chưa có bằng chứng đang áp dụng
  trong runtime**: cả 348 active feed đều có `poll_interval_seconds` NULL và
  `cadence_checked_at` NULL; run crawl 1317 còn ghi `poll_interval=1800`.
  Không đưa mức tiết kiệm request dự phóng 8,5% của T12 vào resume như thành quả
  vận hành thực tế.
- Trong 7 ngày trước snapshot đầu, crawl có 171 run `partial`, 10 `succeeded`
  và 2 `abandoned`. Không dùng trạng thái Airflow thành công hoặc queue đã drain
  để suy ra SLA, uptime hay tỷ lệ crawl thành công. Không dùng dự phóng 2,5× ở
  T11.7, mục tiêu publication-to-ingestion latency, hay accuracy chưa được đo.

---

## T14 — Boilerplate trong text embed, trước và sau khi sửa (2026-09-23)

Xuất phát từ một cụm hiển thị trên UI gồm hai bài không liên quan: *"The AI
Researcher Who Just Quit Anthropic…"* (wired.com) và *"With another huge
infusion of cash, Stoke Space goes even bigger"* (arstechnica.com). Tại thời
điểm đo, cụm đó (`clusters.id = 2436`) đã có **29 thành viên**, 28 Wired và 1
Ars, trải 14 ngày.

### Nguyên nhân đo được

`services/text.py:first_paragraphs` lấy 1.200 ký tự đầu của Markdown đã crawl,
và với Wired toàn bộ 1.200 ký tự đó là banner cookie của Condé Nast ("Privacy
Center…"). Cả 29 thành viên của cụm có **cùng một chuỗi body** sau
`first_paragraphs` — chỉ có 1 body phân biệt trong 29 bài. `is_nav_dense` không
chặn được vì banner là văn xuôi, không phải danh sách link.

| Nguồn | Bài có body bắt đầu bằng banner CMP |
| --- | --- |
| wired.com | 37/37 |
| arstechnica.com | 13/160 |

Similarity lưu trong `cluster_members` cho thấy cụm tự củng cố: bài thứ hai vào
với 0,7686; các bài sau vào với 0,80–0,93 khi centroid đã ngả về phía banner.

```sql
SELECT cm.cluster_id, cm.article_id, round(cm.similarity::numeric, 4), cm.added_at
FROM cluster_members cm WHERE cm.cluster_id = 2436 ORDER BY cm.added_at;
```

### Snapshot database trước khi sửa

Đo lúc **2026-09-23 11:09:29 UTC** trên `news-radar-db-1`.

| Chỉ số | Giá trị |
| --- | --- |
| `raw_articles` | 6.909 |
| `article_embeddings` | 6.901 |
| Cluster có thành viên | 5.987 |
| Cluster nhiều bài | 397 |
| Cluster nhiều source | 194 |
| Cluster lớn nhất | 30 bài |
| Cluster trải > 72h | 35 |
| Bài nằm trong cluster nhiều bài | 1.311 |

### Replay offline: corpus 6.873 bài, export 2026-09-22

Harness ngoài repo (`.scratch-boilerplate/`, không commit) embed lại corpus bằng
đúng `FastEmbedEmbedder` rồi chạy lại `services/clustering.py` với threshold
0,75 và window 72h. Biến duy nhất là text đưa vào model. Thước đo chất lượng là
cosine giữa **embedding của riêng title** hai bài bị gom chung — một proxy, không
phải nhãn người gán; cặp ≥0,60 tính là gom đúng, <0,45 tính là gom sai.

| | Text hiện tại | Sau khi lọc boilerplate |
| --- | --- | --- |
| Cặp bài bị gom chung | 3.849 | 3.632 |
| Cặp gom đúng (≥0,60) | 3.095 | 3.170 |
| Cặp gom sai (<0,45) | 409 | 136 |
| Tỉ lệ gom sai | 10,6% | 3,7% |
| Cluster nhiều source | 201 | 222 |
| Cluster coherence <0,35 | 4 (18 bài) | 1 (2 bài) |

Sweep threshold trên cùng corpus:

| Threshold | Hiện tại: đúng / sai / %sai / đa nguồn | Sau khi lọc: đúng / sai / %sai / đa nguồn |
| --- | --- | --- |
| 0,70 | 3.912 / 1.085 / 17,9% / 294 | 3.933 / 323 / 6,2% / 320 |
| 0,72 | 3.573 / 859 / 16,6% / 252 | 3.646 / 229 / 5,1% / 282 |
| 0,75 | 3.095 / 409 / 10,6% / 201 | 3.170 / 136 / 3,7% / 222 |
| 0,78 | 2.584 / 160 / 5,4% / 133 | 2.549 / 14 / 0,5% / 154 |

Hai số đo riêng của cặp bài trong cụm 2436: similarity 0,769 → 0,207. Toàn bộ
cặp trong wired.com vượt 0,75: 125/666 → 1/666.

### Giới hạn diễn giải

- Thước đo "đúng/sai" dựa trên similarity của title, không phải nhãn người gán.
  Nó không thiên vị bản lọc: text embed hiện tại cũng chứa title.
- Replay sắp xếp bài theo `event_at`, còn worker thật xử lý theo thứ tự claim
  của queue, nên số cluster trong replay (404 nhiều bài / 201 đa nguồn) không
  trùng khít số trong database (397 / 194).
- Sweep chỉ nói bản lọc trội hơn ở mọi threshold đã thử. Không dùng nó làm cơ sở
  đổi `CLUSTER_THRESHOLD`; việc đó cần sweep riêng sau khi corpus đã sạch.
- Lọc boilerplate **không** động tới cụm bài lặp theo mẫu hằng ngày (NYT
  Connections, Pokémon deals, lịch bóng đá của mashable.com). Cụm lớn nhất vẫn
  là 32 bài ở cả hai bản, và số cluster trải > 72h vẫn là 35–36.

### Thay đổi đã áp dụng

Profile chrome theo từng source, dựng từ document frequency của dòng đã chuẩn
hoá (`core/boilerplate.py`), lưu ở `source_boilerplate`, áp dụng trong
`services/text.py:article_text` **trước** khi xếp hạng text — vì gỡ banner chính
là thứ quyết định nấc 2 chứa bài báo hay chứa cookie policy. Lệnh
`news-worker boilerplate-refresh` dựng lại profile (lock riêng, có dòng
`worker_runs`), chạy hằng đêm trong `news_maintenance`.

Chạy trên DB thật, 2026-09-23:

| Bước | Kết quả |
| --- | --- |
| `boilerplate-refresh` | 703 source xét, **69 profile**, 2.270 dòng; 634 source bị bỏ qua vì < 5 bài |
| `embed --reprocess --since 2026-08-01` | 6.931 requeue, **3.333 embed lại**, 3.598 giữ nguyên hash |
| `cluster --since 2026-08-01` | 3.333 claim, 2.858 cluster mới, 475 attach, 3.324 đánh giá lại, 2.327 cluster đóng |
| `cluster --repair` | 0 drift |

**3.333/6.931 = 48% corpus đổi text embed** — đó là phần thực sự có chrome bị gỡ.

### Đo lại trên database sau khi sửa

Cùng harness, cùng judge title-only như phần trước.

| Chỉ số | Trước | Sau |
| --- | --- | --- |
| Cặp bài bị gom chung | 3.700 | 2.988 |
| Cặp gom đúng (≥0,60) | 2.815 | 2.620 |
| Cặp gom sai (<0,45) | **475** | **102** |
| Tỉ lệ gom sai | **12,8%** | **3,4%** |
| Mean coherence | 0,7694 | 0,7737 |
| Cluster coherence <0,35 | 4 (19 bài) | 1 (2 bài) |
| Cluster nhiều bài | 397 | 359 |
| Cluster nhiều source | 194 | 161 |
| Bài trong cluster nhiều bài | 1.311 | 1.154 |
| Cluster trải > 72h | 35 | 33 |
| Cluster lớn nhất | 30 | 30 |

Cụm 29 bài của Wired đã biến mất, cụm 9 bài gom theo chrome của GitHub cũng vậy.
**Cặp bài ban đầu đã tách: article 7161 → cluster 7640, article 7562 → cluster
6442.** Cụm lớn nhất còn lại là NYT Connections (30 bài) — đúng phần đã dự báo là
lọc boilerplate không chạm tới.

Phân bố nguồn text sau khi sửa: `content` 5.754, `feed_summary` 1.023, `title` 154.

### Vì sao số cặp gom đúng giảm 2.815 → 2.620

Bóc theo từng cặp: giữ 2.654 cặp (3,1% sai), mất 1.046 cặp (**37,7% sai**), thêm
334 cặp (6,3% sai). Phần mất đi chủ yếu là rác, nhưng vẫn mất 441 cặp "đúng" theo
judge. Hai nguyên nhân, đo được riêng:

1. **Judge bị nhiễu ở loạt bài cùng khuôn của một tờ báo.** 368/441 cặp mất là
   content/content, và chỉ 58% cùng source so với baseline 85% ở nhóm giữ lại —
   phần lớn là deal roundup, "How to watch…" cùng một tờ, title giống nhau nhưng
   nội dung khác hẳn. Judge title-only chấm chúng là đúng; chúng không đúng.
2. **Lần recluster này là rebuild *một phần*, sai thứ tự.** Chỉ 3.333 bài đổi text
   bị tách ra và đánh giá lại, theo thứ tự claim của queue chứ không theo
   `event_at`; 3.598 bài còn lại giữ nguyên cụm cũ. Greedy clustering phụ thuộc
   thứ tự, nên đây không phải trạng thái mà một lần rebuild sạch sẽ cho ra.

Để tách hai hiệu ứng, replay lại đúng policy production trên **chính các vector
mới trong DB**, theo thứ tự `event_at` (`.scratch-boilerplate/replay_db.py`):

| Chỉ số | DB trước | DB sau (rebuild một phần) | Replay trên vector mới |
| --- | --- | --- | --- |
| Cluster nhiều bài | 397 | 359 | 420 |
| Cluster nhiều source | 194 | 161 | **228** |
| Bài trong cluster nhiều bài | 1.311 | 1.154 | 1.344 |
| Cặp gom đúng | 2.815 | 2.620 | **3.172** |
| Cặp gom sai | 475 | 102 | 134 |
| Tỉ lệ gom sai | 12,8% | 3,4% | 3,7% |
| Cluster coherence <0,35 | 4 | 1 | 1 |

Replay khớp với dự báo offline ở phần trên (3.170 cặp đúng, 222 cluster đa nguồn).
Kết luận: embedding mới tốt hơn hẳn trên mọi chỉ số; phần hụt trong DB là hệ quả
của rebuild một phần, không phải của thay đổi. Một lần rebuild sạch
(`cluster_members`/`clusters` xoá rồi chạy lại theo `event_at`) sẽ lấy lại
khoảng 67 cluster đa nguồn và ~550 cặp đúng. Đó là thao tác phá huỷ, chưa chạy.

### Điểm còn hở

- **arstechnica.com chưa được lọc.** Banner CMP của nó chỉ xuất hiện ở 13/169
  bài (7,7%), quá thấp so với sàn DF 0,30 nên profile của nó chỉ có 4 dòng, và
  bài Ars trong cặp gốc vẫn embed kèm banner. Hạ DF xuống dưới 7,7% sẽ ăn vào text
  thật; sửa đúng phải là luật cross-source, nhưng luật đó phá nguyên tắc "một dòng
  là chrome khi *chính source này* lặp lại nó" và cần sweep riêng.
- 634/703 source dưới ngưỡng 5 bài nên chưa có profile; chúng chiếm 11% corpus.
- Profile là dữ liệu dẫn xuất và **không** tự động embed lại: đổi profile là tín
  hiệu để operator chạy `embed --reprocess`, DAG hằng đêm không tự làm việc đó.
