# Điều phối worker bằng Airflow (kế hoạch triển khai)

> Chi tiết hoá §4.1 "Mô hình vận hành worker" của [spec.md](spec.md), thay thế
> phần "Cron / systemd timer" ở §4.1.1 cho môi trường có nhiều hơn một worker.
>
> **Trạng thái: ĐÃ TRIỂN KHAI (2026-08-30), chưa chạy trên môi trường thật.**
> Đích triển khai là **single-host Docker** trên **Airflow 3.3.1 / Python
> 3.12**. Toàn bộ code của Phase 1–3 đã có trong repo (§9). Đã kiểm chứng:
> bốn DAG parse sạch bằng `DagBag` thật; mọi command line chúng sinh ra đều
> qua được `build_parser` + `validate` của `news-worker`; image
> `news-radar-worker` build và chạy được, `--summary-json /dev/stdout` ra đúng
> dòng cuối, `--exit-busy 99` cho exit 99 khi lock đã có chủ và exit 0 khi
> không truyền cờ. Chưa kiểm chứng: một dagrun thật do scheduler Airflow chạy.
> Việc còn lại là bật stack, chạy song song với cron và đối chiếu `worker_runs`
> — xem §9 và §11. Mục 12 còn 2 điểm tuning, không chặn.
>
> **Bối cảnh:** sau tuần 2, một vòng xử lý đầy đủ cần 5–6 lệnh `news-worker`
> chạy đúng thứ tự (xem [README.md](../README.md) §"Chạy flow Week 2
> end-to-end"). Chạy tay không lặp lại được, và cron thì không diễn đạt được
> quan hệ phụ thuộc giữa các stage.

---

## 1. Mục tiêu

Thay chuỗi lệnh thủ công bằng lịch chạy tự động có quan hệ phụ thuộc tường
minh, quan sát được, và chạy lại được sau lỗi — mà **không** chuyển bất kỳ
logic nghiệp vụ nào ra khỏi `backend/`.

**Deliverable:** crawl → embed → classify/cluster → merge → refresh chạy tự
động theo lịch; một lỗi ở stage bất kỳ hiện ra trong UI kèm log; backfill và
taxonomy-refresh trigger được từ UI với tham số, không cần SSH.

### Không làm

- Không chuyển `processing_jobs` / `crawl_failures` sang Airflow. Hàng đợi
  work-item và retry queue vẫn nằm trong PostgreSQL.
- Không dùng dynamic task mapping để fan-out theo article. Đó chính là việc
  `PostgresJobQueue.claim()` đang làm bằng `FOR UPDATE SKIP LOCKED`, kèm
  lease, backoff và dead-letter ([backend/workers/queue.py](../backend/workers/queue.py)).
- Không chạy Alembic trong DAG định kỳ. Migration là bước deploy.
- Không đưa threshold/trọng số classify vào Airflow Variables (xem §10).
- Không dùng Airflow làm nơi lưu metric. `worker_runs` vẫn là sổ cái duy nhất.

---

## 2. Nguyên tắc kiến trúc

**Airflow chỉ làm điều phối và lập lịch.** Một task Airflow = một lệnh
`news-worker` nguyên vẹn, không phải một article.

Hệ quả kiểm chứng được: gỡ Airflow ra và quay lại cron phải không mất gì ngoài
lịch chạy. Nếu một thay đổi vi phạm điều này, nó sai.

Airflow là **một trigger nữa** bên cạnh cron và chạy tay, không phải nguồn sự
thật thay thế. Mọi run vẫn ghi vào `worker_runs` với `trigger` tường minh.

---

## 3. Đối chiếu contract worker hiện tại với Airflow

Đây là phần quan trọng nhất. Bỏ qua bảng này thì Airflow sẽ báo xanh trong khi
không làm gì.

| # | Hành vi hiện tại | Vấn đề | Xử lý |
|---|---|---|---|
| 1 | Không lấy được advisory lock ⇒ log + **exit 0** ([base.py:138](../backend/workers/base.py:138)) | Airflow báo task **success** trong khi không làm gì. Failure mode nguy hiểm nhất: DAG xanh, queue phình. | Mutual exclusion đẩy lên Airflow: `max_active_runs=1` + **pool riêng mỗi worker, slot = 1**. Advisory lock tụt xuống vai trò safety net. Bổ sung §7.1 để trạng thái này *nhìn thấy được*. |
| 2 | `--since/--until` ⇒ `window.explicit` ⇒ `trigger="backfill"` ([base.py:214](../backend/workers/base.py:214)) | Map `data_interval` của Airflow vào `--since/--until` sẽ ghi **mọi run định kỳ thành backfill**, hỏng toàn bộ thống kê vận hành. | DAG định kỳ dùng `--last`. Chỉ DAG backfill dùng `--since/--until`. Mọi task truyền `--trigger` tường minh. |
| 3 | Worker là **drain queue**, không time-partitioned | `catchup=True` sinh N dagrun lịch sử vô nghĩa, tranh nhau cùng một queue. | `catchup=False` + `max_active_runs=1` trên **mọi** DAG. |
| 4 | Đã có 2 tầng retry bền vững: `crawl_failures` (backoff 5′→24h) và `processing_jobs.attempts` → `dead_letter` sau 5 lần | `retries` của Airflow tạo tầng thứ ba, không quan sát được, làm nhiễu backoff. | `retries=0` cho crawl/embed/classify/cluster. `retries=1` chỉ cho task hạ tầng. |
| 5 | SIGTERM ⇒ dừng ở ranh giới chunk, đóng run `partial`, exit 0 | Điểm **khớp tốt**: `execution_timeout` gửi SIGTERM. | `execution_timeout` < schedule interval cho mọi task. Timeout cho ra run `partial` sạch, không phải run kẹt `running`. |
| 6 | `partial` là bình thường (feed lỗi lẻ tẻ); `--strict` biến nó thành exit 1 | Bật `--strict` ở DAG định kỳ ⇒ alert fatigue ⇒ tắt alert. | **Không** `--strict` ở DAG định kỳ. Chỉ dùng ở `news_taxonomy_refresh` và `news_backfill`, nơi `partial` nghĩa là dữ liệu chưa nhất quán. |

---

## 4. Bản đồ DAG

Bốn DAG, tách theo **nhịp** và **rủi ro**.

```text
news_ingest              (*/30 phút)
  preflight_db → crawl → queue_gate ─┐
                                     └─▶ Asset("news-radar://processing_jobs")
                                                    │
news_semantic            (schedule = Asset trên)    ▼
  embed ─┬─▶ classify ──┐
         └─▶ cluster ───┴─▶ cluster_merge → refresh_cluster_topics
                                                    │
                                                    └─▶ Asset("news-radar://stories")

news_maintenance         (0 3 * * *)
  cluster_repair → replay_dead_letters → cleanup_block_pages (dry-run) → run_health_report

news_taxonomy_refresh    (schedule=None, params)
  taxonomy_import → taxonomy_rebuild → classify --stale → cluster --reprocess
                                     → cluster_merge → refresh_cluster_topics
```

| DAG | Lịch | Vì sao tách riêng |
|---|---|---|
| `news_ingest` | `*/30 * * * *` | Phụ thuộc mạng ngoài, `partial` là bình thường |
| `news_semantic` | Asset-triggered | Tính toán nội bộ, `failed` ở đây mới là tín hiệu thật; `embed` phải nạp MiniLM nên chạy dày là lãng phí |
| `news_maintenance` | Hằng ngày 03:00 | Thao tác sửa chữa, không được xen vào đường ingest |
| `news_taxonomy_refresh` | Trigger tay + params | Thay đổi taxonomy kéo theo reclassify diện rộng, phải có người quyết định |

**Vì sao không gộp `ingest` và `semantic` làm một DAG:** trộn chung thì một
feed hỏng sẽ che mất một lỗi classifier, và hai nhịp chạy hợp lý của chúng
khác nhau. Asset cho đúng ngữ nghĩa `depends_on` mong muốn: `news_semantic`
chỉ chạy khi `news_ingest` **thật sự** đưa được job vào queue.

**Kỷ luật bắt buộc trong `news_semantic`:** `refresh_cluster_topics` là task
cuối, luôn chạy, `trigger_rule="none_failed_min_one_success"`. `cluster_topics`
là derived data bị `_refresh_cluster_topics_locked` ghi đè toàn bộ — chạy thừa
vô hại, bỏ sót thì story mang label cũ.

`classify` và `cluster` độc lập nên chạy song song, đúng như README mô tả.

---

## 5. Runtime

### 5.1 Quyết định: `DockerOperator`, Airflow chạy trong compose

**Đích triển khai: single-host Docker** (chốt 2026-08-30). Không Kubernetes,
không Astronomer. Toàn bộ mục này viết cho đúng bối cảnh đó.

Lý do quyết định: **runtime của crawler (Chromium + system libs Playwright +
model cache) không nên là trạng thái của máy chạy scheduler.** README yêu cầu
`uv run crawl4ai-setup` để tải browser binary; với `BashOperator` thì "phiên
bản Chromium đang chạy" là trạng thái của host, không phải của commit.

Ba lợi ích cộng thêm: giới hạn `mem_limit`/`shm_size` per task (mặc định
`/dev/shm` 64MB là nguyên nhân kinh điển làm Chromium chết ngẫu nhiên); OOM của
`embed` chỉ giết container chứ không giết Airflow worker; rollback được từng
task bằng image tag.

**`PythonOperator` (import code worker in-process) bị loại**, nhưng **không**
vì lý do dependency: §5.5 cho thấy Airflow 3.3.1 và toàn bộ dependency của repo
cài chung một môi trường được. Hai lý do thật:

1. `main()` kết thúc bằng `raise SystemExit`
   ([cli.py:747](../backend/workers/cli.py:747)) và `_dispatch` gọi
   `asyncio.run`. Import vào task nghĩa là bypass `main()` rồi tự tái tạo logic
   dựng `Settings`/parser/window — tức **code path orchestration thứ hai**,
   đúng thứ CLAUDE.md cấm.
2. Blast radius: `embed` nạp MiniLM; một OOM sẽ giết luôn process Airflow
   worker, kéo theo mọi task đang chạy chung.

Cộng thêm một lý do vận hành: môi trường chung đó có **689 package**, nơi
constraints của Airflow và `numpy`/`onnxruntime`/`pydantic` của
fastembed/crawl4ai phải đồng thuận vĩnh viễn. Hôm nay resolve được không đảm
bảo lần nâng cấp Airflow sau vẫn resolve được.

**Tính đảo ngược:** vì mọi task là "một chuỗi argv của `news-worker`", đổi sang
`BashOperator` hay `KubernetesPodOperator` chỉ là thay phần thân hàm `worker()`
trong `_common.py`; bốn file DAG không đổi một dòng. Vì vậy `DockerOperator`
**không được** xuất hiện rải rác trong các file DAG.

### 5.2 Vị trí file

```text
orchestration/
  dags/
    _common.py              # factory worker(); nơi duy nhất biết đến operator
    news_ingest.py
    news_semantic.py
    news_maintenance.py
    news_taxonomy_refresh.py
deployments/
  Dockerfile.worker         # mới
  docker-compose.airflow.yml # mới, metadata DB riêng
```

DAG **không** đặt dưới `backend/` vì `pyproject.toml` khai báo
`packages.find where = ["backend"]` — mọi thứ ở đó sẽ bị đóng gói vào wheel.

### 5.3 `Dockerfile.worker`

Khác `Dockerfile.api` ở ba điểm bắt buộc:

1. Chạy `uv run crawl4ai-setup` — image API hiện **không** có browser binary.
2. `ENTRYPOINT ["news-worker"]` với venv trong `PATH`, **không** phải
   `CMD ["uv", "run", ...]`. Nếu PID 1 là `uv`, SIGTERM có thể không tới process
   Python và toàn bộ hợp đồng "tắt êm" ở §3.5 mất tác dụng.
3. `--init` (tini) để reap zombie — crawl4ai sinh process con Chromium.

### 5.4 Factory và DAG mẫu

```python
# orchestration/dags/_common.py
COMMON = dict(
    image="news-radar-worker:latest",
    docker_url="unix://var/run/docker.sock",
    network_mode="news-radar_default",
    auto_remove="force",
    mount_tmp_dir=False,
    shm_size=1024 * 1024 * 1024,
    mounts=[
        Mount("/models", "news_radar_model_cache", type="volume"),
        Mount("/data", f"{REPO}/data", type="bind", read_only=True),
    ],
    environment={
        "DATABASE_URL": "{{ conn.news_radar_pg.get_uri() }}",
        "FASTEMBED_CACHE_PATH": "/models",
    },
)

def worker(task_id, *argv, pool, timeout, trigger="scheduled", **kw):
    """Nơi duy nhất trong repo biết đang dùng operator nào."""
    return DockerOperator(
        task_id=task_id,
        command=[*argv, "--once", "--trigger", trigger, "--exit-busy", "99"],
        skip_on_exit_code=99,
        pool=pool, retries=0, execution_timeout=timeout,
        do_xcom_push=True,
        **COMMON, **kw,
    )
```

Hai điểm bản triển khai khác phác thảo trên, cả hai đều do thực tế CLI:

1. Có thêm `admin()` bên cạnh `worker()`. `replay`, `cleanup-block-pages`,
   `queue-status`, `taxonomy-import` **không** nhận `--once/--trigger/
   --summary-json` (chúng không mở window, không giữ lock, không ghi
   `worker_runs`), nên nhét cờ worker vào sẽ làm argparse fail. `admin()` cũng
   đứng ngoài bốn pool ở §6: chiếm slot `news_embed` duy nhất chỉ để requeue
   vài dòng dead-letter là lãng phí.
2. `command` là **một chuỗi**, không phải list. docker-py `shlex.split` chuỗi
   command ([`docker/types/containers.py`](https://github.com/docker/docker-py)),
   nhờ đó một biểu thức Jinja render ra rỗng — `{{ '--prune' if params.prune
   else '' }}` trong `news_taxonomy_refresh` — biến mất khỏi argv thay vì tới
   argparse dưới dạng chuỗi rỗng.

```python
# orchestration/dags/news_ingest.py
JOBS = Asset("news-radar://processing_jobs")

@dag(schedule="*/30 * * * *", catchup=False, max_active_runs=1,
     dagrun_timeout=timedelta(minutes=25))
def news_ingest():
    preflight = SQLExecuteQueryOperator(
        task_id="preflight_db", conn_id="news_radar_pg", retries=1,
        sql="SELECT 1 FROM alembic_version",   # kiểm tra, không migrate
    )
    crawl = worker(
        "crawl", "crawl", "--feed-path", "/data/engineering_blogs.opml",
        "--last", "12h", pool="news_crawl", timeout=timedelta(minutes=20),
    )

    @task(outlets=[JOBS])
    def queue_gate():
        """Chỉ phát Asset khi thật sự có việc; tránh nạp MiniLM cho queue rỗng."""
        pending = PostgresHook("news_radar_pg").get_first(
            "SELECT count(*) FROM processing_jobs "
            "WHERE stage='embed' AND state='pending' AND available_at <= now()"
        )[0]
        if not pending:
            raise AirflowSkipException("không có job embed nào pending")

    preflight >> crawl >> queue_gate()
```

### 5.5 Version và kết quả resolve (đã kiểm chứng 2026-08-30)

**Chốt: Airflow 3.3.1 trên Python 3.12**, cài kèm constraints chính thức.

```bash
AF=3.3.1
uv pip install "apache-airflow==${AF}" \
  apache-airflow-providers-postgres apache-airflow-providers-docker \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AF}/constraints-3.12.txt"
```

| | |
|---|---|
| Bản stable mới nhất | `3.3.1` (2026-08-12), patch thứ 2 của nhánh 3.3 (`3.3.0` ra 2026-07-06) |
| Phương án bảo thủ | `3.2.2` (2026-05-29), patch cuối của nhánh 3.2 — chọn nếu muốn bản đã chạy lâu hơn |
| `requires_python` | `>=3.10, !=3.15` — repo đang `>=3.12`, khớp |
| Constraints 3.12 | có, HTTP 200, 728 dòng |
| Provider pin | `providers-docker==4.5.9`, `providers-postgres==7.0.2` |

**Kết quả resolve thực tế (uv, `--python-version 3.12`):**

| Tổ hợp | Kết quả |
|---|---|
| Airflow 3.3.1 + postgres + docker provider | ✅ 449 package |
| Airflow 3.3.1 + **toàn bộ dependency của repo** | ✅ 689 package |

**Không có xung đột SQLAlchemy.** Airflow 3 đã chuyển sang
`sqlalchemy[asyncio]>=2.0.50` (constraints ghim `2.0.51`), tương thích với
`sqlalchemy[asyncio]>=2.0,<3` của repo. Ràng buộc `<2.0` là chuyện của Airflow
2.x và **không còn áp dụng**. Các cặp khác cũng chồng lấn: Airflow cần
`fastapi>=0.129,<0.137` với repo `>=0.115,<1`, `uvicorn>=0.37` với repo
`>=0.34,<1`.

**Hệ quả cho `queue_gate`:** dùng `PostgresHook` in-process **được**.
`providers-postgres` chạy trên `psycopg2-binary`, hoàn toàn tách khỏi
`asyncpg`/SQLAlchemy của repo. Không cần gọi §7.2 qua container.

**Tham số `DockerOperator` trong §5.4 đã đối chiếu source của
`providers-docker==4.5.9`:** `skip_on_exit_code`, `shm_size`, `mem_limit`,
`mount_tmp_dir`, `auto_remove` (`"never"|"success"|"force"`) đều tồn tại. Thứ tự
xử lý đúng như thiết kế cần — `StatusCode in skip_on_exit_code` được kiểm tra
**trước** nhánh coi exit code khác 0 là lỗi, nên exit 99 cho ra `skipped` chứ
không phải `failed`.

---

## 6. Pool và concurrency

| Pool | Slots | Task | Lý do |
|---|---|---|---|
| `news_crawl` | 1 | crawl (mọi DAG) | Một browser pool tại một thời điểm; `MAX_CONCURRENT_CRAWLS` đã bound bên trong |
| `news_embed` | 1 | embed | Model chiếm RAM; hai instance = OOM |
| `news_cluster` | 1 | cluster, merge-only, repair | Ba chế độ ghi cùng bảng `clusters` |
| `news_classify` | 1 | classify (mọi mode) | Như trên |

Bốn pool riêng chứ không phải một pool chung, để `classify` và `cluster` vẫn
song song được.

---

## 7. Thay đổi cần thêm ở backend

Không bắt buộc để Airflow chạy được, nhưng quyết định độ tin cậy. Xếp theo
giá trị/chi phí. **Cả ba đã làm xong** (2026-08-30).

### 7.1 `--exit-busy CODE` khi không lấy được advisory lock — ĐÃ LÀM

Mặc định vẫn `0` — cron hiện có không đổi. Airflow truyền `--exit-busy 99` kèm
`skip_on_exit_code=99`, task hiện **skipped** thay vì success giả. Đây là fix
trực tiếp cho §3 mục 1.

Phạm vi: [base.py:138-144](../backend/workers/base.py:138) và
[cli.py](../backend/workers/cli.py) (`_add_common_arguments`). Cả nhánh
`taxonomy-rebuild` ở `_dispatch_admin` cũng có cùng pattern.

*Xong khi:* run thứ hai chạy chồng thoát 99; không truyền cờ thì vẫn thoát 0;
có unit test cho cả hai. → `exit_busy_for()` trong
[base.py](../backend/workers/base.py), cờ ở `_add_common_arguments` và ở
`taxonomy-rebuild`; `--exit-busy 1` bị `_validate_exit_busy` từ chối vì trùng
mã lỗi. Test: `test_worker_base.py`.

### 7.2 `news-worker queue-status --json` — ĐÃ LÀM

Đọc `processing_jobs` group by stage/state, `crawl_failures` group by kind,
`worker_runs` gần nhất theo worker. Cho phép `queue_gate` và
`run_health_report` dùng CLI thay vì để DAG viết SQL thẳng vào bảng — giữ
nguyên tắc "thêm read method vào `PostgresRawStorage`". Thay thế luôn loạt
lệnh `docker compose exec db psql` trong README §3.

*Xong khi:* thay được cả 4 câu SQL smoke-check trong README bằng một lệnh.
→ `PostgresRawStorage.queue_status()` (read-only, ba câu group-by trên một
connection) trả `QueueStatusSnapshot`; `alerts()` **cố ý không** coi backlog
`pending` là alert vì điều đó cần lịch sử nhiều lần đo. README §3 và §"Storage
và monitoring" đã thay bằng `news-worker queue-status`; hai truy vấn còn lại
(`topic_prototypes`, `article_classifications`, `clusters`) nằm ngoài phạm vi
lệnh này nên vẫn là psql. Test: `test_models.py`, `test_cli_storage.py`,
`test_worker_storage_integration.py`.

### 7.3 `--summary-json /dev/stdout` — ĐÃ LÀM

Emit `RunOutcome.stats` + `run_id` thành **đúng một dòng JSON cuối cùng** trên
stdout, sau mọi log. Đã đối chiếu source `providers-docker==4.5.9`: với
`do_xcom_push=True`, operator đẩy `log_lines[-1]` — đúng dòng cuối — nên yêu
cầu "JSON phải là dòng cuối" là bắt buộc, không phải khuyến nghị. (Có
`xcom_all=True` để lấy toàn bộ log, nhưng đẩy cả log vào XCom sẽ phình
metadata DB.)

*Xong khi:* XCom của task crawl chứa `run_id` khớp với `worker_runs`. →
`emit_summary()` được gọi từ `Worker._finish`, tức cả trên nhánh thành công,
nhánh lỗi và nhánh bị cancel; lỗi ghi summary chỉ log chứ không làm hỏng run.
`worker()` trong `_common.py` truyền sẵn `--summary-json /dev/stdout` cho mọi
task worker. Đã chạy thật trong `news-radar-worker:latest`: dòng cuối stdout là
JSON, và `run_id` trong đó khớp với hàng `worker_runs` tương ứng. Phần chưa
kiểm chứng là bước cuối — XCom do chính Airflow đẩy.

---

## 8. Quan sát và cảnh báo

Không xây dashboard trong Airflow. `worker_runs` đã là sổ cái. Airflow chỉ cần
task `run_health_report` hằng ngày, **fail** nếu:

- có run `abandoned` trong 24h (process chết không kịp đóng sổ);
- có row `dead_letter` mới trong `processing_jobs`;
- `crawl_failures` có `attempt_count` bất thường cao;
- **`pending` của một stage tăng đơn điệu qua 3 lần đo** — producer nhanh hơn
  consumer. Đây là chỉ số quan trọng nhất và cron hiện tại hoàn toàn không cho
  thấy.

---

## 9. Lộ trình

| Phase | Nội dung | Deliverable | Trạng thái |
|---|---|---|---|
| **1** | `Dockerfile.worker`, `docker-compose.airflow.yml` (LocalExecutor, metadata DB **riêng**), `_common.py`, `news_ingest` | Trigger tay `news_ingest`, đối chiếu `worker_runs` với một run cron tương đương. Cron cũ vẫn chạy song song. | Code xong |
| **2** | `news_semantic` + Asset; bật lịch `news_ingest`, tắt cron; làm §7.1 và §7.2 | Một vòng crawl→story tự động, đầy đủ, không thao tác tay | Code xong |
| **3** | `news_maintenance`, `news_taxonomy_refresh` với params; §7.3 | Xoá được mục "Chạy flow Week 2 end-to-end" khỏi README | Code xong |

**"Code xong" chưa phải "chạy xong".** Cả ba phase đã có trong repo, nhưng
kiểm chứng mới dừng ở mức tĩnh: `DagBag` load bốn DAG không lỗi import, và mọi
command line các DAG sinh ra đều qua được `build_parser` + `validate` của
`news-worker`. Chưa có run thật nào trên Airflow.

Vì vậy thứ tự bật vẫn giữ nguyên: **`news_ingest` phải được trigger tay và đối
chiếu với một run cron tương đương trước.** `news_ingest` đang để lịch
`*/30 * * * *` trong code, nên khi bật stack lần đầu hãy để DAG ở trạng thái
paused và trigger tay, thay vì sửa file thành `schedule=None`.

Cron cũ phải chạy song song suốt Phase 1. Chỉ tắt cron sau khi Phase 2 chạy
sạch liên tục 48h.

---

## 10. Rủi ro đã biết

| Rủi ro | Giảm thiểu |
|---|---|
| **`docker stop` chỉ cho 10s rồi SIGKILL** — chunk không kịp xong ⇒ run kẹt `running`. Đã xác nhận trong source `providers-docker==4.5.9`: `on_kill()` gọi `self.cli.stop(container_id)` **không truyền timeout**, tức nhận mặc định 10s của Docker, và operator **không expose** tham số để nới. | Không mất dữ liệu (mỗi chunk đã commit + checkpoint). `reap_abandoned` dọn sau `WORKER_HEARTBEAT_TIMEOUT_SECONDS` (900s) nhưng **chỉ khi run kế tiếp khởi động** ([base.py:134](../backend/workers/base.py:134)) ⇒ có run zombie hiển thị tới 15 phút. Giữ chunk nhỏ (`CRAWL_BATCH_SIZE=10`, `EMBED_BATCH_SIZE=32` hiện đủ nhỏ). |
| SIGTERM không tới process Python | `ENTRYPOINT ["news-worker"]` + tini, xem §5.3 |
| Mount `docker.sock` = quyền root trên host | **Chấp nhận** trong bối cảnh single-host một người vận hành. Điều kiện kèm theo: Airflow webserver **không** expose ra ngoài localhost, và không ai ngoài chủ máy có quyền tạo DAG. Nếu một trong hai điều kiện đổi, phải xem lại §5.1. |
| **Single-host: worker chia RAM/CPU với PostgreSQL và API.** `embed` nạp MiniLM, `crawl` chạy Chromium — cả hai đều có thể làm PostgreSQL swap. | `mem_limit` tường minh trên mọi worker container (đây là lợi ích chính của §5.1 so với `BashOperator`). Pool slot = 1 ở §6 đã chặn hai worker nặng chạy cùng lúc. Đặt `news_maintenance` lúc 03:00, tách khỏi nhịp ingest. Cần đo RSS thực tế của `embed` và `crawl` ở Phase 1 rồi mới đặt số. |
| Airflow metadata DB dùng chung instance với `news_radar` | Tách instance. Tải connection của scheduler rất khác, và autovacuum của Airflow không nên đụng vào bảng pgvector. |
| Hai scheduler chồng nhau | Không bao giờ dùng `--interval` bên trong task Airflow; luôn `--once`. |
| Threshold classify bị chỉnh qua Airflow UI | Giữ chúng trong `worker_runs.params` như hiện tại. CLAUDE.md yêu cầu thay đổi phải kèm sweep + snapshot trong [measurements.md](measurements.md); biến thành Variable sẽ phá kỷ luật đó. |
| Đổi `PostgresJobQueue` sang Redis ở tuần 4 | Thiết kế này không đổi — Airflow chưa bao giờ chạm `processing_jobs` ngoài việc **đếm**. |

---

## 11. Định nghĩa "xong"

```bash
# 1. Một vòng đầy đủ chạy tự động, không thao tác tay
#    → news_ingest và news_semantic đều xanh trong 48h liên tục

# 2. Run chồng hiện skipped, không phải success
uv run news-worker embed --once --exit-busy 99   # khi một embed khác đang chạy
echo $?   # 99

# 3. Mọi run định kỳ được ghi đúng trigger
psql -c "SELECT worker, trigger, count(*) FROM worker_runs
         WHERE started_at > now() - interval '24 hours' GROUP BY 1,2;"
#    → không có dòng nào trigger='backfill'

# 4. Không có run kẹt hoặc bị bỏ rơi
psql -c "SELECT worker, status, count(*) FROM worker_runs
         WHERE started_at > now() - interval '24 hours'
         GROUP BY 1,2 HAVING status IN ('running','abandoned');"
#    → rỗng

# 5. Queue không phình
uv run news-worker queue-status --json
#    → pending mỗi stage không tăng đơn điệu qua 3 lần đo
```

---

## 12. Quyết định đã chốt và câu hỏi còn mở

**Đã chốt**

- **2026-08-30 — đích triển khai: single-host Docker.** Không Kubernetes,
  không Astronomer. Kéo theo: `DockerOperator` (§5.1), chấp nhận mount
  `docker.sock` kèm điều kiện, và thêm ràng buộc tài nguyên chung máy (§10).

- **2026-08-30 — Airflow 3.3.1 trên Python 3.12**, cài kèm constraints chính
  thức. Đã resolve thử: không có xung đột dependency, kể cả khi cài chung với
  toàn bộ dependency của repo. Chi tiết ở §5.5. Giả định cũ "Airflow ghim
  SQLAlchemy `<2.0`" là của Airflow 2.x và **đã bị bác bỏ**; lập luận loại
  `PythonOperator` ở §5.1 được viết lại cho đúng.

**Còn mở**

1. **Nhịp `*/30` cho crawl** là giả định, chưa đo. Cần đối chiếu với
   `feed_poll_interval_seconds` và thời lượng thực tế của một run crawl trước
   khi chốt.
2. **`news_taxonomy_refresh` có nên tự chạy `cluster --reprocess` không?** Chi
   phí lớn và không phải lần đổi taxonomy nào cũng cần. Hiện đề xuất để nó là
   một param mặc định `false`.
