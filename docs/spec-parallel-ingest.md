# Giảm độ trễ luồng ingest và song song hoá crawl (kế hoạch triển khai)

> Chi tiết hoá `news_ingest` của [orchestration-airflow.md](orchestration-airflow.md)
> §4, và trả lời câu hỏi còn mở §12.2 của tài liệu đó ("nhịp `*/30` cho crawl
> là giả định, chưa đo").
>
> **Trạng thái: TẦNG 1–2 VÀ INSTANT TIER ĐÃ TRIỂN KHAI, CHỜ ĐO PRODUCTION
> (cập nhật 2026-09-08).** Các
> baseline đến từ [measurements.md §T11](measurements.md); policy cadence mới
> được sweep bằng RSS fetch on-demand ở [§T12](measurements.md). Cả hai dùng
> [`scripts/bench_ingest.py`](../scripts/bench_ingest.py) trên chính database
> và parser mà `CrawlWorker` dùng. Tầng 3 vẫn chỉ là thiết kế có điều kiện.
>
> **Bối cảnh:** đề xuất ban đầu là tách `news_ingest` thành một planner tuần tự
> (kiểm tra etag/content hash, đếm việc, chia đều) cộng N crawl worker chạy song
> song. Đo đạc cho thấy hướng đó **đúng nhưng không phải việc nên làm trước**:
> nút cổ chai lớn nhất không nằm ở thiếu worker, mà ở một `asyncio.gather`
> barrier trong `poll_feeds` và ở lịch poll cố định 30 phút. Tài liệu này xếp
> lại thứ tự theo giá trị trên chi phí, giữ nguyên thiết kế N worker ở tầng
> cuối cùng.

---

## 0. Instant tier bổ sung

Tối ưu adaptive trong tài liệu này vẫn là đường normal. Một nhóm feed nhỏ cần
publication-to-ingestion thấp hơn có `feed_state.priority=0` và được
`news-worker instant-crawl` trong container riêng poll liên tục với interval
cố định (mặc định 30 giây). `news-worker crawl` của Airflow chỉ lấy priority
1; hai worker có advisory lock riêng và cùng upsert idempotent vào
`raw_articles`.

Priority đi cùng `crawl_failures` và `processing_jobs`; số nhỏ hơn được claim
trước, nên retry và các stage embed/classify/cluster không làm mất tier của
bài. Semantic không chạy theo từng lần instant crawl: DAG `news_semantic` có
lịch 15 phút và queue gate riêng, rồi drain chung cả hai priority. Thiết kế
không thêm crawl task table, broker, push/WebSub hay một instant semantic
worker.

Đổi một feed đã seed sang instant bằng
`news-worker set-feed-priority <url> 0`; đổi lại normal bằng priority 1.
Khi cập nhật source, chạy compose với `up -d --build instant-crawl`; nếu không,
Docker có thể tiếp tục dùng tag `news-radar-worker:dev` đã build từ code cũ.

---

## 1. Mục tiêu

Giảm độ trễ từ lúc bài được xuất bản đến lúc nó có trong `raw_articles`, mà
không làm giảm độ tin cậy của luồng và không thêm một code path orchestration
thứ hai.

**Deliverable:** độ trễ p50 của nhóm feed thực sự có bài xuống dưới 30 phút
(hiện tại 305 phút trên cả tuần, 122 phút trong giai đoạn scheduler chạy đều),
với tổng số lượt poll mỗi ngày **không tăng**.

### Không làm

- **Không** nâng `MAX_CONCURRENT_FEEDS` như một biện pháp độc lập. §4.1 cho
  thấy nó gần như vô hiệu chừng nào barrier còn đó.
- **Không** hạ `FEED_POLL_INTERVAL_SECONDS` đồng loạt cho cả 375 feed. Đó là
  cách tăng tải publisher gấp ba để phục vụ 10% feed thật sự có bài (§5.2).
- **Không** triển khai bảng `crawl_tasks` và N worker ở phase đầu. Tầng đó chỉ
  mua thêm tối đa ~1.6× và bị chặn bởi sàn per-host (§6), nên nó phải đứng sau
  hai tầng rẻ hơn.
- **Không** dùng Airflow dynamic task mapping để fan-out theo article. Giữ
  nguyên nguyên tắc §1 của [orchestration-airflow.md](orchestration-airflow.md):
  một task Airflow = một lệnh `news-worker`.
- Tầng adaptive ban đầu không đổi `raw_articles`, `crawl_failures` hay
  `worker_runs`. Instant tier sau đó thêm `priority` vào `crawl_failures`,
  `feed_state` và `processing_jobs`; không thêm queue crawl mới.

---

## 2. Nguyên tắc kiến trúc

**Đo trước, sửa sau, và ưu tiên thứ rẻ nhất trong một process.** Mọi thay đổi
trong tài liệu này phải chỉ ra được một số đo trong [measurements.md](measurements.md)
là lý do tồn tại của nó. Một thay đổi không có số đo đi kèm là một phỏng đoán.

Hệ quả kiểm chứng được: nếu gỡ toàn bộ tầng 3 (planner/fetcher tách rời) ra,
hệ thống vẫn phải đạt ~2.5× so với hôm nay. Nếu không, thứ tự phase này sai.

**Chính sách lập lịch feed là policy không I/O**, nên nó thuộc `core/` chứ
không phải nằm rải rác trong worker — cùng lý do `core/urls.py` và
`core/content_quality.py` tồn tại.

---

## 3. Hiện trạng đo được

Tóm tắt; số đầy đủ và cách tái lập ở
[measurements.md §T11–T12](measurements.md).

| # | Số đo | Giá trị | Hệ quả |
| --- | --- | --- | --- |
| 1 | Độ trễ `published_at` → `crawled_at`, p50 | 305 min (cả tuần) / **122 min** (khi cron chạy đều) | ~2.5× độ trễ là do scheduler không chạy, không phải throughput |
| 2 | Khoảng cách giữa hai run crawl | p50 31.7 min, **p90 197.8 min, max ~40h** | Availability là số hạng lớn nhất; Airflow xử lý phần này |
| 3 | `poll_feeds` 80 feed, c=2 → c=24 | 61.0s → 56.2s (**8%**) | Concurrency một mình vô dụng |
| 4 | Cùng việc đó, sliding window, c=8 | **16.2s (3.6×)** | Barrier là nút cổ chai thật |
| 5 | Thời lượng fetch từng feed | p50 0.36s, p90 1.3s, đuôi treo ở 15.1s | Mỗi nhóm barrier trả giá bằng thành viên chậm nhất |
| 6 | Số feed nằm giữa 8s và 15s | **0** (hai lần chạy độc lập) | Hạ timeout 20s → 8s không mất feed lành nào |
| 7 | Crawl 24 URL, c=2 → c=6 | 28.3s → 16.9s (**1.67×**) | Một biến môi trường, không cần code |
| 8 | Feed active có bài trong 24h | **37 / 375** (59% im lặng >30 ngày) | Poll đều 30 phút chi phần lớn ngân sách cho feed chết |
| 9 | Độ cũ feed hiện tại | p50 26.4 min, p90 29.6 min | Interval 1800s đang là ràng buộc thật, không phải nhịp cron |
| 10 | Phân bố host, 7 ngày | 210 host / 1104 bài, top-10 = **67.8%**, max 210 | Chia tải theo số bài là bất khả; phải chia theo host |
| 11 | Host lớn nhất trong một run | 15 bài → **sàn ~112s** | Trần cứng của mọi phương án nhiều worker |
| 12 | Cadence fetch on-demand | 333/369 fetch được; 320 đủ ≥3 gap | Có đủ coverage để thay proxy DB bằng cadence từ RSS body |
| 13 | Trimmed mean 10%, tier 5m/30m/6h | 14 / 12 / 307 trên 333 feed fetch được | Phần lớn feed thực sự phù hợp tier 6h |
| 14 | Poll budget strict upper bound | **16.204/ngày**, baseline 17.712 | Kể cả coi 36 fetch lỗi là 5m, tổng request vẫn giảm 8.5% |
| 15 | Feed stale >24h nhưng cadence tier 5m | 9 | Theo dõi sau rollout; chưa đủ lý do phá invariant strict bằng idle-backoff |

Mô hình wall clock đã kiểm chứng trên run 911 (dự đoán 607s, đo 647s, sai 6%):

| Pha | Hôm nay | Sau tầng 1 | Ghi chú |
| --- | --- | --- | --- |
| Poll feed | ~307s | ~81s | sliding window c=8 |
| Crawl | ~300s | ~180s | `MAX_CONCURRENT_CRAWLS=6` |
| **Tổng** | **647s** | **~260s (2.5×)** | vẫn trong **một** process |

---

## 4. Tầng 1 — Bỏ barrier trong `poll_feeds`

Đây là toàn bộ phần thắng lớn, và nó nằm trong một hàm.

### 4.1 Nguyên nhân

[`poll_feeds`](../backend/adapters/retrieve_news_data/google_news_rss_searcher.py:507)
chia target thành nhóm `max_concurrent_feeds` rồi `asyncio.gather` từng nhóm:

```python
for feed_group in self._chunks(list(targets), self._max_concurrent_feeds):
    fetched = await asyncio.gather(*[_fetch_rss(...) for target in feed_group],
                                   return_exceptions=True)
```

Nhóm sau chỉ bắt đầu khi nhóm trước xong **hết**, nên chi phí mỗi nhóm bằng
thành viên chậm nhất. Với p50 0.36s và một đuôi treo ở 15.1s, tăng `N` chỉ gom
thêm outlier vào cùng một nhóm — đúng như số đo #3.

### 4.2 Thay đổi

Đổi barrier thành sliding window bằng **fixed worker pool** cỡ
`max_concurrent_feeds`, đẩy kết quả qua `asyncio.Queue`, và `poll_feeds` yield
theo thứ tự hoàn thành. Fixed pool tương đương semaphore về giới hạn
concurrency nhưng không tạo một task cho mỗi feed.

Ba ràng buộc bắt buộc, mỗi cái ứng với một cách làm sai:

1. **Queue phải có `maxsize`** (đề xuất: `max_concurrent_feeds`). Consumer
   (`crawl.py`) làm việc nặng cho mỗi feed — crawl bài rồi ghi DB. Không có
   backpressure, producer sẽ fetch xong cả 375 feed trong lúc consumer còn ở
   feed đầu tiên, và nếu run bị cắt giữa chừng thì toàn bộ phần fetch thừa là
   lãng phí đã bỏ ra. Barrier hiện tại đang cho backpressure một cách tình cờ;
   sliding window phải cho nó một cách chủ đích.
2. **Thứ tự yield không còn là thứ tự `targets`.** Không nơi nào trong
   `crawl.py` phụ thuộc thứ tự, nhưng test thì có thể — đây là điểm cần soát.
3. **Exception vẫn phải thành `FeedPoll(status="failed")`, không được thoát ra
   ngoài generator.** Một task fetch chết mà không được bọc sẽ giết cả run;
   `return_exceptions=True` hôm nay đang lo việc này.

### 4.3 Timeout fetch

[`_fetch_rss`](../backend/adapters/retrieve_news_data/google_news_rss_searcher.py:371)
hardcode `timeout=20.0`. Sau khi bỏ barrier, tổng thời gian bị ghim bởi **một**
fetch chậm nhất, nên timeout trở thành sàn thật sự (số đo #5).

Đưa nó thành `Settings.feed_fetch_timeout_seconds` (`FEED_FETCH_TIMEOUT_SECONDS`),
**mặc định 8.0**. Căn cứ: số đo #6 — đuôi lưỡng cực, không feed nào nằm giữa 8s
và 15s trong hai lần chạy độc lập, và cụm 15.1s luôn ít hơn số feed đã failed,
tức đó là feed đang hỏng sẵn chứ không phải feed chậm mà lành. Trần 15.1s nằm
**dưới** timeout httpx 20s nên nó đến từ tầng resolver/connect của OS; timeout
httpx 8s sẽ cắt trước và biến một thất bại 15s thành một thất bại 8s.

> **Rủi ro phải theo dõi:** một fetch timeout đi vào
> [`record_feed_failure`](../backend/adapters/storage/postgres.py:2444), cộng
> `consecutive_failures`, và đủ `disable_after_failures` lần thì feed **bị tắt
> hẳn**. Mẫu 80 feed nói chi phí là 0, nhưng mẫu 80 không phải 375. Điều kiện
> bật: sau lần chạy production đầu tiên, đối chiếu `stats.feeds_disabled` và
> `crawl_failures` với `ConnectTimeout`; nếu có feed lành bị tắt, nâng lên 12s
> chứ không quay lại 20s.

### 4.4 `feedparser.parse` chạy trên event loop

[Dòng 386](../backend/adapters/retrieve_news_data/google_news_rss_searcher.py:386)
là CPU đồng bộ nằm trong coroutine. Dưới barrier nó bị che; dưới sliding window
c=8 nó trở thành điểm serialize tiềm tàng.

**Chưa sửa.** Số đo #5 cho thấy ở c≥8 tổng thời gian bằng đúng fetch chậm nhất
(15.06s), nên parse hiện **không** phải ràng buộc. Ghi lại ở đây như một mục
theo dõi: nếu sau khi hạ timeout xuống 8s mà tổng thời gian không giảm tương
ứng, `asyncio.to_thread(feedparser.parse, response.text)` là bước tiếp theo —
và chỉ khi đó.

### 4.5 `MAX_CONCURRENT_CRAWLS`

`2` → `6`, trong [.env_example](../.env_example) và trong `_common.py`. Số đo
#7: 1.67×, và quá 6 thì hết lợi. Đây là thay đổi duy nhất trong tầng 1 không
đụng vào code.

---

## 5. Tầng 2 — Nhịp planner và lịch poll thích ứng

### 5.1 Vì sao đổi nhịp cron một mình là chưa đủ

[`due_feeds`](../backend/adapters/storage/postgres.py:2380) lọc
`next_poll_at <= now`; [`crawl.py:138`](../backend/workers/crawl.py:138) đặt
`next_poll_at = now + FEED_POLL_INTERVAL_SECONDS` (1800s). Độ cũ feed đo được
p50 26.4 min (số đo #9) — tức interval 30 phút đang được tôn trọng đúng và nó,
chứ không phải cron, mới là ràng buộc.

Độ trễ lập lịch kỳ vọng ≈ `interval/2 + cadence/2`:

| Cấu hình | interval | cadence | Độ trễ lập lịch | Feed due mỗi chu kỳ | Poll/ngày |
| --- | --- | --- | --- | --- | --- |
| Hôm nay | 30 min | 30 min | 30 min | ~375 | 18.000 |
| Chỉ đổi cron | 30 min | **5 min** | 17.5 min | ~62 | 18.000 |
| Hạ interval đồng loạt | 10 min | 5 min | 7.5 min | ~190 | **54.000 (3×)** |
| **Adaptive, T12 trimmed mean** | 5m / 30m / 6h | **5 min** | **~5 min** cho tier nhanh | ~56 | **≤16.204** |

Hàng cuối là số đo mới thay cho dự phóng 13.856 dựa trên độ mới bài cuối. T12
fetch unconditional 369 feed: 333 thành công, 320 đủ mẫu; trimmed mean xếp
14/12/307 feed vào tier 5m/30m/6h. Upper bound 16.204 đã giả sử cả 36 fetch lỗi
đều ở 5m; production retry backoff làm số thực tế thấp hơn.

### 5.2 Chính sách adaptive

Một hàm thuần trong `core/feed_schedule.py` nhận `(entry_url, published_at)` từ
**toàn bộ RSS body vừa fetch**, trước `max_articles_per_feed`, window,
`known_urls` và crawl. Nó bỏ URL trùng, coi các bài cùng timestamp là một
publication event, bỏ timestamp tương lai quá tolerance, rồi tính trimmed mean
10% trên các gap liên tiếp.

| Cadence evidence | Interval |
| --- | --- |
| Ít hơn 3 gap hợp lệ | 5 phút |
| Trimmed mean `≤ 30m` | 5 phút |
| Trimmed mean `> 30m` và `≤ 6h` | 30 phút |
| Trimmed mean `> 6h` | 6 giờ |

Boundary là strict theo yêu cầu: đúng 30m vẫn ở 5m, đúng 6h vẫn ở 30m. Vì 304
không có body, `feed_state` lưu `poll_interval_seconds`, observed gap/count và
`cadence_checked_at`. Feed chưa có `cadence_checked_at` bỏ ETag/Last-Modified
đúng một lần để bootstrap; 304 sau đó giữ tier đã lưu; failure giữ cadence và
dùng retry backoff hiện tại. Cả ba tier, min gaps, trim fraction và future
tolerance là config và được ghi vào `worker_runs.params`.

`last_entry_published_at` vẫn được cập nhật để quan sát, nhưng không còn là
input của scheduler. Migration cadence là cần thiết: suy tier từ
`next_poll_at - last_polled_at` vừa mơ hồ khi run trễ, vừa không audit được
invariant.

### 5.3 Nhịp DAG và một cái bẫy trong `worker()`

`news_ingest` đổi `schedule="*/30 * * * *"` → `"*/5 * * * *"`.

`max_active_runs=1` vẫn giữ: run chồng nhau không mua thêm gì (advisory lock sẽ
cho exit 99) và chỉ làm bẩn UI. Nhưng `dagrun_timeout` 25 phút thì phải xuống —
ở nhịp 5 phút, một dagrun sống 25 phút nghĩa là 4 run kế tiếp xếp hàng vô ích.

**Bẫy:** [`worker()`](../orchestration/dags/_common.py:136) tính ngân sách
cooperative bằng `timeout - 5 phút`:

```python
"--max-runtime", f"{max(1, int(timeout.total_seconds() // 60) - 5)}m",
```

Con số 5 phút cố định đó chỉ đúng khi `timeout` lớn. Đặt `timeout=4m` cho task
crawl sẽ ra `max-runtime=1m` — worker tự cắt mình sau một phút, run nào cũng
`partial`. Sửa thành tỉ lệ, ví dụ `timeout - min(5m, 25% × timeout)`; với
`timeout=8m` thì ngân sách là 6m, dư cho một run ~4.3 phút đã dự phóng ở §3, và
vẫn còn 2 phút để checkpoint trước hard timeout.

Đề xuất cụ thể: `execution_timeout=8m` cho task crawl, `dagrun_timeout=9m`.

---

## 6. Tầng 3 — Tách planner và fetcher (thiết kế, chưa lên lịch làm)

Đây là đề xuất ban đầu. Nó đúng về nguyên tắc, và tài liệu này giữ lại nguyên
vẹn — nhưng đặt sau, vì trần của nó thấp hơn nhiều so với hình dung ban đầu.

### 6.1 Trần thực tế

Sau tầng 1, pha crawl còn ~180s. Sàn per-host là **~112s** (số đo #11: host
đông nhất của một run có 15 bài, `max_crawls_per_host=1` cộng delay 1s ⇒
15 × (6.5s + 1s)). Vậy N worker mua được nhiều nhất ~1.6×, và con số đó không
cải thiện khi tăng N.

### 6.2 Hình dạng

```text
news_ingest (*/5)
  preflight_db → plan → [fetch_0 … fetch_N-1] → queue_gate
```

- **`plan`** (tuần tự, một instance): poll toàn bộ feed due bằng conditional
  GET, lọc theo window, gọi `known_urls`, rồi **ghi việc vào bảng
  `crawl_tasks`** thay vì tự crawl. Ghi `record_feed_poll` ngay tại đây.
- **`fetch_i`** (N instance): claim việc từ `crawl_tasks` theo đúng pattern
  `FOR UPDATE SKIP LOCKED` + `lease_until` + `locked_by` mà
  [`PostgresJobQueue.claim`](../backend/workers/queue.py) đã dùng cho
  `processing_jobs`. Không phát minh cơ chế thứ hai.

### 6.3 Ba điều bắt buộc phải đúng

1. **Đơn vị claim là host, không phải row.** `max_crawls_per_host=1` được thực
   thi *trong process* bởi [`_host_waves`](../backend/adapters/retrieve_news_data/crawler.py:37).
   Hai worker cùng claim hai URL của `mashable.com` sẽ phá chính sách lịch sự đó
   mà không có gì phát hiện được. Đây là điều chỉnh quan trọng nhất so với
   thiết kế ban đầu: "chia đều workload giữa các worker" theo **số bài** là sai;
   phải chia theo **host**, và vì top-10 host chiếm 67.8% nên việc chia sẽ
   **không** đều — worker giữ host lớn nhất chạy 112s trong khi các worker khác
   rảnh. Đó chính là lý do trần chỉ ~1.6×.
2. **Bỏ được sự ràng buộc giữa `record_feed_poll` và crawl.** Hôm nay
   [crawl.py:261](../backend/workers/crawl.py:261) cố tình *không* ghi poll khi
   run bị cắt giữa chừng, vì entry chưa crawl sẽ mất khi nó rơi khỏi feed. Khi
   việc đã nằm bền vững trong `crawl_tasks`, lý do đó biến mất và ghi poll ngay
   trở thành đúng — nhưng chỉ khi đó.
3. **`apply_batch` phải ghi theo thứ tự xác định.**
   [postgres.py:669](../backend/adapters/storage/postgres.py:669) duyệt
   `failure_by_key.items()` và [dòng 733](../backend/adapters/storage/postgres.py:733)
   duyệt `for article in articles` theo thứ tự dict. Một writer thì vô hại; N
   writer upsert các key chồng nhau theo thứ tự khác nhau thì deadlock.
   `_ensure_sources` đã sort ([dòng 621](../backend/adapters/storage/postgres.py:621));
   article phải sort theo `url`, failure theo `(kind, url)`. Rẻ, và ở tầng 3 nó
   là bắt buộc chứ không còn là phòng xa.

### 6.4 Điều kiện để bắt đầu tầng 3

Không bắt đầu cho tới khi tầng 1 và 2 đã chạy production và `worker_runs` cho
thấy pha crawl **thật sự** là phần lớn nhất của một run. Nếu sau tầng 1+2 mà
độ trễ đã đạt mục tiêu §1, tầng 3 không nên làm: nó thêm một bảng, một vòng đời
lease, và một lớp lỗi mới (task mồ côi, lease hết hạn, host bị claim kép) để
đổi lấy ~1.6× trên một pha đã không còn là nút cổ chai.

---

## 7. Tổng hợp thay đổi ở backend

| # | File | Thay đổi | Tầng |
| --- | --- | --- | --- |
| 1 | [google_news_rss_searcher.py:507](../backend/adapters/retrieve_news_data/google_news_rss_searcher.py:507) | `poll_feeds`: barrier → sliding window có bounded queue | 1 |
| 2 | [google_news_rss_searcher.py:371](../backend/adapters/retrieve_news_data/google_news_rss_searcher.py:371) | `timeout=20.0` → tham số, mặc định 8.0 | 1 |
| 3 | [core/config.py:107](../backend/core/config.py:107) | thêm `FEED_FETCH_TIMEOUT_SECONDS`; `MAX_CONCURRENT_CRAWLS` 2 → 6 | 1 |
| 4 | `core/feed_schedule.py` (mới) | đo cadence từ RSS observations và chọn strict tier bằng trimmed mean | 2 |
| 5 | [storage_models.py:77](../backend/models/storage_models.py:77) | `FeedPollTarget` thêm persisted tier + bootstrap marker | 2 |
| 6 | [schema.py](../backend/adapters/storage/schema.py) + migration | thêm tier, observed gap/count, `cadence_checked_at` vào `feed_state` | 2 |
| 7 | [postgres.py:2380](../backend/adapters/storage/postgres.py:2380) | đọc/ghi cadence state atomically cùng poll state | 2 |
| 8 | [crawl.py](../backend/workers/crawl.py) | tính cadence trước mọi filter; 304 reuse tier; ghi policy vào run params | 2 |
| 9 | `crawl_tasks` + repository + worker mới | bảng việc và claim theo host | 3 |
| 10 | [postgres.py:669,733](../backend/adapters/storage/postgres.py:669) | sort article/failure trước khi upsert | 3 |

Tầng 1 không có migration. Tầng 2 có một migration additive trên `feed_state`;
các dòng hiện tại để bootstrap marker `NULL` và được phân loại bằng fetch mới.

## 8. Thay đổi ở orchestration

| # | File | Thay đổi | Tầng |
| --- | --- | --- | --- |
| 1 | [news_ingest.py:23](../orchestration/dags/news_ingest.py:23) | `*/30` → `*/5`; `dagrun_timeout` 25m → 9m; crawl `timeout` 20m → 8m | 2 |
| 2 | [_common.py:136](../orchestration/dags/_common.py:136) | ngân sách `--max-runtime` theo tỉ lệ thay vì trừ cứng 5 phút | 2 |
| 3 | `_common.py` `COMMON.environment` | concurrency, fetch timeout và toàn bộ `FEED_POLL_*` policy | 1–2 |
| 4 | `news_ingest` | `plan` + N `fetch_i` thay task `crawl` đơn | 3 |

Pool `news_crawl` giữ 1 slot ở tầng 1–2. Tầng 3 sẽ cần nâng lên N, và khi đó
mất luôn tính chất "advisory lock là safety net" ở
[orchestration-airflow.md §3](orchestration-airflow.md) mục 1 — một lý do nữa
để tầng 3 đứng sau.

---

## 9. Lộ trình

| Phase | Nội dung | Deliverable | Rủi ro |
| --- | --- | --- | --- |
| **1 — implemented** | Sliding worker pool + bounded queue + timeout 8s + `MAX_CONCURRENT_CRAWLS=6` | Chờ production xác nhận run crawl ~260s thay vì 647s | Thấp: không migration, không đổi ngữ nghĩa kết quả |
| **2A — measured** | Unconditional cadence sweep trên 369 feed | Trimmed mean 10%, min 3 gap; upper bound 16.204 poll/ngày | Hoàn tất ở measurements T12 |
| **2B — implemented** | Cadence policy/state/bootstrap, cron `*/5`, sửa ngân sách `max-runtime` | Chờ một tuần đo latency, request và byte budget | Trung bình: đổi lịch poll; additive migration |
| **3** | `crawl_tasks`, claim theo host, N fetcher, sort trong `apply_batch` | ~1.6× nữa trên pha crawl | Cao: bảng mới, lease, mất mutual exclusion đơn giản |

Phase 1 và 2 độc lập nhau và có thể làm song song; phase 2 **không** cần phase
1 xong. Nhưng nên chạy phase 1 trước để lần đo lại sau đó không lẫn hai biến.

Baseline T11 và cadence sweep T12 đã được giữ nguyên. Sau rollout tầng 1–2,
ghi production results thành T13; không sửa đè T11/T12.

---

## 10. Rủi ro đã biết

| Rủi ro | Giảm thiểu |
| --- | --- |
| Timeout 8s làm feed lành bị tính là failure và cuối cùng bị `disable_after_failures` tắt hẳn | Số đo #6 nói chi phí là 0 trên mẫu 80 feed, nhưng phải đối chiếu `stats.feeds_disabled` và `crawl_failures.ConnectTimeout` sau run production đầu tiên. Nếu có feed lành bị tắt: nâng lên 12s, không quay lại 20s |
| Sliding window bỏ backpressure ⇒ fetch chạy trước consumer, run bị cắt là phí công | `asyncio.Queue(maxsize=max_concurrent_feeds)`; đây là ràng buộc thiết kế, không phải tuỳ chọn |
| Nhịp `*/5` với `max_active_runs=1` làm dagrun xếp hàng | `dagrun_timeout=9m` và `execution_timeout=8m`; run chồng vẫn exit 99 và hiện `skipped` như hôm nay |
| `--max-runtime` âm/quá ngắn khi hạ `execution_timeout` | Sửa công thức trong `_common.py` **trong cùng commit** với việc hạ timeout, không tách ra |
| Feed mới/thiếu mẫu bị poll quá thưa và mất bài | Không đủ 3 gap luôn vào 5m; chỉ slow down khi có evidence đúng strict boundary |
| Existing feed có validator trả 304 nên không bootstrap được | Khi `cadence_checked_at IS NULL`, adapter cố ý bỏ conditional headers đúng một lần |
| Feed stale nhưng cadence lịch sử nhanh ở 5m mãi | T12 thấy 9 feed; theo dõi actual budget, chưa thêm idle-backoff vì upper bound vẫn thấp hơn baseline |
| Tier 5m làm publisher không validator chịu full body thường xuyên | T12 đo 43 feed fetch được không validator, trimmed-mean projection 47 MB decompressed/ngày; gate theo actual bytes/failure trước khi thêm ngoại lệ |
| Tầng 3: hai worker cùng crawl một host, phá `max_crawls_per_host` | Claim theo host chứ không theo row (§6.3.1). Không có cách nào phát hiện việc này sau khi đã xảy ra, nên nó phải đúng từ thiết kế |
| Tầng 3: deadlock khi N writer upsert cùng key khác thứ tự | Sort trong `apply_batch` (§6.3.3), làm **trước** khi bật worker thứ hai |
| Toàn bộ tài liệu tối ưu sai chỗ nếu scheduler vẫn không chạy đều | Số đo #1 và #2: availability là số hạng lớn nhất. Không bắt đầu phase nào cho tới khi Airflow `news_ingest` đã chạy sạch — xem [orchestration-airflow.md §9](orchestration-airflow.md) |

---

## 11. Định nghĩa "xong"

```bash
# Phase 1 — một run crawl xuống dưới 5 phút
psql -c "SELECT run_id, finished_at - started_at AS duration, stats->>'feeds_processed'
         FROM worker_runs WHERE worker='crawl' ORDER BY started_at DESC LIMIT 5;"
#    → duration p50 < 5 phút với số feed tương đương run 911 (402 feed)

# Phase 1 — không có feed lành nào bị tắt vì timeout mới
psql -c "SELECT count(*) FROM feed_state
         WHERE disabled_at > now() - interval '24 hours';"
#    → 0, hoặc mọi dòng đều là 404/410/401 thật

# Phase 2 — strict invariant và bootstrap coverage
psql -c "SELECT count(*) FILTER (WHERE cadence_checked_at IS NULL) unclassified,
                count(*) FILTER (WHERE poll_interval_seconds = 1800
                                  AND observed_publish_gap_seconds <= 1800) bad_30m,
                count(*) FILTER (WHERE poll_interval_seconds = 21600
                                  AND observed_publish_gap_seconds <= 21600) bad_6h
         FROM feed_state WHERE disabled_at IS NULL;"
#    → bad_30m = 0, bad_6h = 0; unclassified chỉ còn feed chưa fetch thành công

# Phase 2 — tổng ngân sách poll không tăng
psql -c "SELECT sum(86400.0 / poll_interval_seconds)
         FROM feed_state
         WHERE disabled_at IS NULL AND poll_interval_seconds IS NOT NULL;"
#    → extrapolated toàn tập < baseline 17.712/ngày

# Phase 2 — mục tiêu §1
psql -c "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY
           extract(epoch FROM crawled_at - published_at)/60)
         FROM raw_articles WHERE crawled_at > now() - interval '48 hours';"
#    → < 30 phút

# Phase 3 — không worker nào crawl chồng một host
#    → cần một assertion trong test tích hợp; không có cách kiểm sau sự việc
```

---

## 12. Quyết định đã chốt và câu hỏi còn mở

**Đã chốt**

- **2026-09-03 — thứ tự đảo so với đề xuất ban đầu.** Planner tuần tự + N
  fetcher là tầng 3, không phải tầng 1. Căn cứ: số đo #3 (concurrency một mình
  cho 8%), #4 (barrier là nguyên nhân, 3.6×) và #11 (sàn per-host chặn mọi
  phương án nhiều worker ở ~1.6×).
- **2026-09-03 — chia tải phải theo host, không theo số bài.** Số đo #10:
  top-10 host chiếm 67.8%. Chia theo số bài vừa không cân được, vừa phá
  `max_crawls_per_host`.
- **2026-09-03 — lịch poll thích ứng thay vì hạ interval đồng loạt.** Số đo #8
  và bảng §5.1: adaptive cho feed sống nhịp 5 phút mà tổng poll/ngày vẫn giảm.
- **2026-09-03 — cadence lấy từ RSS fetch on-demand, không từ article DB.** T12
  cho 320/333 feed thành công đủ ≥3 gap. Chọn trimmed mean 10%; strict tier
  5m/30m/6h cho upper bound 16.204 request/ngày, thấp hơn baseline 8.5%.
- **2026-09-03 — chưa thêm idle-backoff.** Có 9 feed stale/5m, nhưng budget
  tổng vẫn đạt gate. Giữ policy dễ audit và chỉ mở ngoại lệ nếu T13 chứng minh
  publisher load hoặc actual byte budget có vấn đề.

**Còn mở**

1. **Actual production latency/budget sau rollout.** T12 là snapshot một lần;
   cần một tuần T13 để đo tier churn, poll/ngày, bytes và p50 ingest latency.
2. **Có cần idle-backoff không.** Chỉ trả lời sau T13 cho 9 feed stale/5m;
   không thêm dựa trên giả định vì nó phá rule “slow tier phải có evidence”.
3. **Có cần tầng 3 không.** Chỉ bắt đầu nếu `worker_runs` sau tầng 1–2 cho thấy
   crawl vẫn chiếm phần lớn run và p50 latency chưa đạt mục tiêu. Nếu cần, N=2
   là điểm bắt đầu vì sàn per-host 112s giới hạn lợi ích tối đa khoảng 1.6×.
