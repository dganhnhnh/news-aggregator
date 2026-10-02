# Điều phối worker bằng process chạy liên tục + Postgres LISTEN/NOTIFY

> **Trạng thái: ĐÃ TRIỂN KHAI (2026-09-29).** Thay cho các DAG `news_ingest`,
> `news_semantic` và `news_maintenance` trong
> [orchestration-airflow.md](orchestration-airflow.md). Airflow đang **tạm
> ngưng**: service đã bị comment khỏi `deployments/docker-compose.airflow.yml`,
> còn `orchestration/dags/` giữ nguyên để có thể bật lại.

---

## 1. Vì sao bỏ Airflow cho luồng định kỳ

Với Airflow, mỗi task là một container `DockerOperator` dùng một lần. Hệ quả:

- **Nạp lại MiniLM mỗi 15 phút.** Mỗi lần embed là một container mới.
- **Semantic trễ tới 15 phút.** Bài đã nằm trong DB nhưng phải chờ tới lượt cron.
- **Chạy nguyên một stack chỉ để bấm giờ.** Gồm scheduler, apiserver,
  dag-processor và một metadata DB riêng.

Mô hình mới giữ nguyên nguyên tắc cũ: một lần chạy là một lệnh `news-worker`,
và `worker_runs` là sổ cái. Phần thay đổi là **ai quyết định khi nào chạy**:

| Việc | Airflow (cũ) | Hiện tại |
|---|---|---|
| Ingest | DAG `*/5`, container mới mỗi lần | service `ingest`: `crawl --interval 300 --max-runtime 8m` |
| Semantic | DAG `*/15` với `queue_gate` | service `semantic`: đánh thức bằng NOTIFY, dự phòng bằng timer 300s |
| Bảo trì | DAG 03:00 UTC | service `maintenance`: supercronic chạy `news-worker maintenance` lúc 03:00 UTC |
| Backfill, DLQ, taxonomy | DAG chạy tay trên UI | `docker compose run --rm semantic ...` (xem §5) |

Celery, Redis Streams và các lựa chọn khác đã được cân nhắc. Lý do chưa dùng:
`processing_jobs` đã có claim `SKIP LOCKED`, lease, backoff, dead-letter và
priority. Một broker thứ hai sẽ tạo ra hai nguồn sự thật. Interface
`JobQueue` và tín hiệu đánh thức được tách riêng, nên sau này vẫn thay được.

## 2. Tín hiệu: doorbell chứ không phải mailbox

```
crawl / instant-crawl / embed / replay ...
        │  INSERT processing_jobs ... ;  SELECT pg_notify('news_radar_jobs', 'embed')
        ▼  (cùng transaction → chỉ giao khi COMMIT, rollback thì không có gì)
news-worker semantic  ── LISTEN news_radar_jobs ──► event.set()
        │
        └─► actionable_jobs(): đọc lại hàng đợi rồi mới quyết định chạy stage nào
```

- `_enqueue_processing_jobs` gửi NOTIFY. Mọi đường enqueue đều đi qua hàm này,
  nên không có đường nào bị sót.
- Mất một thông báo không làm mất việc. Việc vẫn nằm trong `processing_jobs`, và
  timer `--fallback-interval` (300s) sẽ nhặt lên. Timer này cũng bắt các job
  hết backoff (`available_at` tới hạn) mà không ai enqueue thêm.
- Postgres tự gộp các payload trùng nhau trong một transaction. Các thông báo
  tới trong lúc đang chạy một pass được gộp thành **một** pass kế tiếp.

## 3. Một pass của `semantic`

```
embed ──► ┬─ classify ─────────────────┬─► refresh (classify --refresh-clusters)
          └─ cluster ──► merge (throttle)┘       (throttle)
```

1. `embed` chạy nếu `actionable_jobs` > 0. Sau đó đếm lại classify và cluster,
   vì chính embed là bước đưa việc vào hai stage này.
2. `classify` và `cluster → merge` chạy song song bằng `asyncio.gather`.
3. `merge` và `refresh` chỉ chạy khi từ lần trước đã có cluster hoặc classify
   làm việc. Mỗi loại tối đa một lần trong `--merge-every` / `--refresh-every`
   (mặc định 15m, giống nhịp DAG cũ).
4. Stage không có việc thì không chạy, nên cũng không sinh row `worker_runs`.

Nhịp chạy:

- Pass trước có claim được việc thì chạy pass tiếp theo ngay.
- Nếu không, chờ NOTIFY hoặc fallback timer.
- Hai lần bắt đầu pass luôn cách nhau ít nhất `--min-pass-interval` (60s).
  Đây là debounce cho instant-crawl, vốn commit mỗi 30s.

Model embedding được nạp **một lần** cho cả đời process. Mỗi stage vẫn đi qua
`Worker.run_once`, tức là giữ đúng lock, row `worker_runs`, heartbeat và
metric của riêng nó. Grafana và `admin_api/metrics.py` không phải sửa gì.

`--max-runtime` của từng stage lấy theo timeout cũ của DAG: embed 30m,
classify/cluster 20m, merge/refresh 15m. Nay nó chỉ còn tác dụng chia lượt:
một backlog dài ở một stage không chặn các stage khác quá lâu. Ngân sách này
chỉ kết thúc **lượt chạy**, không kết thúc process.

## 4. Scale

```bash
docker compose --env-file .env -f deployments/docker-compose.yml up -d --scale semantic=3
```

Muốn chạy trên máy khác: dùng cùng image `news-radar-worker`, chạy lệnh
`semantic`, và trỏ `DATABASE_URL` về cùng PostgreSQL. Không cần broker.

| Stage | Chế độ lock | Khi có N replica |
|---|---|---|
| embed (drain) | shared | cả N cùng claim, `SKIP LOCKED` chia việc |
| classify (drain) | shared | cả N cùng claim; refresh `cluster_topics` khoá theo từng cluster |
| cluster, merge, repair | exclusive | một replica chạy, các replica khác bỏ qua lượt đó |
| refresh (`classify --refresh-clusters`) | exclusive | một replica chạy |
| embed `--reprocess`/`--model-changed`, classify `--reprocess`/`--stale` | exclusive | chờ mọi replica drain buông lock |
| crawl, instant-crawl | exclusive | **không scale `ingest`**, xem spec-parallel-ingest |

Các lưu ý khi scale:

- Merge hoặc refresh gặp lock bận thì vẫn được giữ là "còn nợ". Replica đó
  sẽ thử lại ở pass sau.
- Ingest vẫn chỉ có một run tại một thời điểm. Muốn crawl song song cần claim
  theo host, là việc của [spec-parallel-ingest.md](spec-parallel-ingest.md).
- Mỗi replica `semantic` giữ một connection LISTEN, cộng với các connection
  từ pool của nó. Cần tính vào `max_connections` khi scale lớn.

## 5. Runbook

Mọi lệnh bên dưới dùng tiền tố:

```bash
DC="docker compose --env-file .env -f deployments/docker-compose.yml"
```

**Triển khai / cập nhật**

```bash
$DC up -d --build ingest semantic maintenance
$DC logs -f ingest semantic
```

Khi ingest commit chunk đầu tiên, semantic phải log `Woken by notify` trong
vòng vài giây.

**Tình trạng hàng đợi**

```bash
$DC run --rm semantic queue-status
```

**Backfill** (thay DAG `news_backfill`)

```bash
$DC run --rm ingest crawl --strict --since 2026-09-01 --until 2026-09-08
$DC run --rm semantic semantic --once
```

`semantic --once` chạy các pass cho tới khi không còn việc, chạy merge và
refresh, rồi thoát. Exit code là 1 nếu có stage nào lỗi. Nếu service
`semantic` đang chạy thì bước này không bắt buộc, vì NOTIFY đã đánh thức nó.

**Replay dead-letter** (thay DAG `news_dlq_replay`)

```bash
$DC run --rm semantic replay --stage embed --limit 200
$DC run --rm semantic replay --failures --kind article --limit 200
```

Replay dead-letter của `processing_jobs` cũng gửi NOTIFY, nên semantic tự nhặt.
Replay của `crawl_failures` được `ingest` xử lý ở run kế tiếp.

**Làm mới taxonomy** (thay DAG `news_taxonomy_refresh`)

```bash
$DC run --rm semantic taxonomy-import --file /data/taxonomy.seed.yaml
$DC run --rm semantic taxonomy-rebuild
$DC run --rm semantic classify --strict --stale --last 30d
$DC run --rm semantic classify --refresh-clusters --last 30d
```

Service `semantic` không mount `/data`. Khi cần file seed, thêm
`-v "$PWD/data:/data:ro"` vào lệnh `run`.

**Bảo trì ngay, không chờ 03:00**

```bash
$DC exec maintenance news-worker maintenance
```

Lệnh gồm bốn bước, chạy tuần tự:

1. `cluster --repair --last 7d`
2. `boilerplate-refresh`
3. `cleanup-block-pages --limit 200` (chỉ dry run)
4. `queue-status --json --fail-on-alert`

Bước 4 luôn chạy, kể cả khi các bước trước lỗi. Exit code là 1 nếu có bất kỳ
bước nào lỗi.

**Dừng an toàn**

```bash
$DC stop semantic
```

`stop_grace_period` là 60s. SIGTERM dừng stage đang chạy sau chunk hiện tại, và
row `worker_runs` được đóng với trạng thái `partial` (không bị `abandoned`).

## 6. Bật lại Airflow

Xem header của `deployments/docker-compose.airflow.yml`.

1. Dừng `ingest`, `semantic` và `maintenance` trước.
2. Bỏ comment các service trong file đó.
3. `up -d`.

Volume metadata của Airflow không bị xoá, nên lịch sử DAG vẫn còn.
