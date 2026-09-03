# Tuần 2 — Story cluster + taxonomy động (kế hoạch chi tiết)

> Bản chi tiết hoá §3, §6 và §10 "Tuần 2" của [spec.md](spec.md), đã đối chiếu
> với code hiện tại sau tuần 1 trong `backend/` và frontend Next.js hiện có.
>
> **Ngân sách đề xuất: 14.5h.** Mốc 12h trong spec chỉ đủ cho taxonomy YAML cố định,
> classifier tối giản và gần như không có đường reclassify. Hai giờ rưỡi tăng thêm
> dùng để chặn dữ liệu crawl lỗi trước embedding, đưa taxonomy vào PostgreSQL, tách
> semantic topic khỏi cây điều hướng, và làm các đường xử lý idempotent. Mục 11 ghi
> rõ cách cắt về 12h nếu bắt buộc.
>
> **Trạng thái: CHỐT để triển khai.** Tuần 2 bắt đầu bằng data-quality remediation rồi
> data audit, và kết thúc bằng một golden semantic suite gồm đúng 10 case thủ công.
> Mọi thay đổi scope sau tài liệu này phải ghi rõ task/thời gian nào bị thay thế.

---



## 1. Mục tiêu & Deliverable

Cuối tuần 2, pipeline phải chạy được từ article đã embed tới story cluster đã gán
topic, rồi hiển thị được trên web:

```bash
# Seed/import taxonomy ban đầu và build prototype
uv run news-worker taxonomy-import --file data/taxonomy.seed.yaml
uv run news-worker taxonomy-rebuild

# Xử lý article mới hoặc backfill article đã có
uv run news-worker classify --once
uv run news-worker cluster --once
uv run news-worker classify --reprocess --last 7d
uv run news-worker cluster --reprocess --last 7d

# API + frontend
uv run news-api
cd frontend && pnpm dev
```

**Deliverable:** mở trang Stories thấy các sự kiện trong hôm nay, mỗi story gom các
article từ nhiều publisher, có một hoặc nhiều topic và mở chi tiết để xem member. API
lọc được theo topic/source; UI filter nâng cao không nằm trong critical path. Clustering
là toàn cục; taxonomy chỉ gắn nhãn và tổ chức cách duyệt, không tạo cluster riêng cho
từng domain.

**Định nghĩa "xong":**

```bash
# 1. Không còn job classify/cluster bị kẹt
psql -c "SELECT stage, state, count(*) FROM processing_jobs
         WHERE stage IN ('classify', 'cluster') GROUP BY 1,2;"

# 2. Prototype dùng cùng model/dimension với article embedding
psql -c "SELECT p.model, p.dim, count(*) FROM topic_prototypes p GROUP BY 1,2;"

# 3. Có cluster đa nguồn và không có article nằm trong hai cluster canonical
psql -c "SELECT count(*) FROM clusters WHERE n_distinct_sources >= 2;"
psql -c "SELECT article_id, count(*) FROM cluster_members GROUP BY 1 HAVING count(*) > 1;"

# 4. Đo được coverage classifier và phân bố cluster
psql -c "SELECT assigned_count, count(*) FROM article_classifications GROUP BY 1 ORDER BY 1;"
psql -c "SELECT n_articles, count(*) FROM clusters GROUP BY 1 ORDER BY 1;"

# 5. API, unit/integration test và 10 golden semantic case đều xanh
curl 'http://localhost:8000/api/stories?limit=5'
curl 'http://localhost:8000/api/topics'
uv run --extra dev pytest
uv run --extra dev pytest backend/tests/test_semantic_golden.py -m semantic
cd frontend && pnpm build
```

Kèm cập nhật `docs/measurements.md`: phân bố kích thước cluster, classifier coverage,
confidence histogram, thời gian xử lý mỗi article và số cluster candidate phải so
sánh. Chỉ số phục vụ quyết định L2/L3 được giữ riêng trong backlog, không phải gate MVP.

---



## 2. Các quyết định chốt trước khi code



### 2.1 Cluster toàn cục, taxonomy không tham gia tạo cluster

Thứ tự đúng về mặt domain là:

```text
article embedding ──→ global cluster ──→ story
        │                    │
        └─→ article topics ──┴─→ cluster topics
```

Không chạy clustering riêng cho `tech`, `finance` hay một feed của user. Một sự kiện
có thể đồng thời mang nhiều topic; đổi taxonomy không được làm thay đổi
`cluster_members`.

Về vận hành, `classify` và `cluster` là hai stage độc lập. Embed thành công enqueue cả
hai. Stage nào chạy trước cũng được:

- `cluster` thêm member thì aggregate các article topic hiện có.
- `classify` ghi topic thì tìm cluster chứa article và aggregate lại.

Nhờ vậy classifier lỗi hoặc taxonomy chưa seed không chặn story clustering.

### 2.2 PostgreSQL là source of truth; YAML chỉ để seed/import/export

Runtime classifier chỉ đọc `topics`, `topic_signals` và `topic_prototypes` từ
PostgreSQL. Không đọc YAML trong hot path và không cần restart worker khi đổi taxonomy.

`data/taxonomy.seed.yaml` vẫn tồn tại để:

- bootstrap môi trường mới;
- review một taxonomy snapshot trong Git;
- import/export giữa các môi trường;
- làm fixture dễ đọc cho test.

Import dùng `slug` ổn định để upsert. Mặc định **không xoá** topic vắng mặt trong file;
chỉ `--prune` mới deactivate, và phải in danh sách sẽ bị ảnh hưởng trước khi ghi.

### 2.3 Semantic topic tách khỏi cây điều hướng

`topics` là các concept mà classifier hiểu, ví dụ `semiconductors`. Cây hiển thị nằm
trong `taxonomies`/`taxonomy_nodes`. Bảng nối cho phép cùng một topic xuất hiện ở nhiều
nhánh:

```text
Technology > Hardware > Semiconductors
Investing  > Industries > Semiconductors
```

Di chuyển hoặc đổi tên node chỉ đổi presentation data, **không** build lại prototype
và **không** reclassify article. Chỉ thay exemplar/keyword/threshold mới tăng
`topics.classifier_revision` và làm các classification cũ stale.

### 2.4 Không học `source_prior` trong MVP

Tuần 2 dùng hai tín hiệu:

```text
score = 0.80 × prototype_similarity + 0.20 × keyword_score
```

`source_prior` để tuần 3 hoặc sau khi có tập nhãn tin cậy. Học prior trực tiếp từ dự
đoán của chính classifier tạo vòng lặp tự củng cố. Schema giữ chỗ cho component này,
nhưng giá trị tuần 2 luôn là `NULL` và trọng số bằng 0.

Giá trị `0.80/0.20` và threshold mặc định là điểm bắt đầu, không phải kết luận.
Phải ghi chúng vào `worker_runs.params`; 10 golden case tuần 2 chỉ bắt lỗi semantic rõ
ràng, còn threshold sweep và báo precision/recall/F1 đầy đủ để tuần 4.

Threshold mặc định đã đổi `0.55 → 0.40` ngày 2026-08-28 sau khi T6 chạy trên toàn bộ
1015 article đã embed. Đây không phải tinh chỉnh để làm golden case xanh: ở `0.55` chỉ
**1.0%** article nhận nhãn trong khi score cao nhất mà bất kỳ topic nào đạt được là
`0.615`, tức ngưỡng nằm gần như ngoài dải giá trị mà công thức có thể sinh ra. Chi tiết
sweep, snapshot phân bố và mẫu title đã kiểm tra bằng tay nằm ở
[measurements.md](measurements.md) mục T6. Trọng số `0.80/0.20` **không** đổi.

### 2.5 Hoãn dedup L2/L3; MVP giữ đầy đủ article evidence

Mục tiêu trước mắt là phân cụm article theo cùng sự kiện/chủ đề một cách đơn giản. MVP
không thêm SimHash L2, `article_duplicates`, hay nhánh semantic duplicate L3. Mọi
article đã qua quality gate đều được embed và trở thành cluster member; cả bài cùng
publisher lẫn khác publisher đều đóng góp vào centroid và counters.

L1 URL identity và T0 content-quality remediation vẫn xử lý các lỗi đã biết trước
embedding. L2/L3 chỉ được cân nhắc lại khi số đo thực tế cho thấy duplicate còn lại làm
tốn embedding đáng kể, gây nhiễu centroid hoặc làm UI khó đọc; tiêu chí quyết định nằm
ở backlog §15.

### 2.6 Không dùng ANN index cho active cluster

Cuối tuần 2 chỉ có vài trăm cluster active trong cửa sổ 72 giờ. Query trực tiếp:

```sql
SELECT id, 1 - (centroid <=> :embedding) AS similarity
FROM clusters
WHERE status = 'open' AND last_seen_at >= now() - interval '72 hours'
ORDER BY centroid <=> :embedding
LIMIT 20;
```

Không tạo HNSW cho `clusters.centroid`: centroid update liên tục, index churn lớn hơn
lợi ích. HNSW hiện tại chỉ phục vụ `article_embeddings` ổn định.

### 2.7 Cluster topic là derived data và phải rebuild được

`article_topics` giữ score của classifier. `cluster_topics` chỉ là materialized
aggregation để API đọc nhanh. Có lệnh rebuild cluster topic theo cửa sổ; không có logic
nào coi row trong `cluster_topics` là dữ liệu gốc.

Mỗi publisher có đúng một vote cho một topic:

```text
source_score(topic) = max(article_score(topic) của source trong cluster)
cluster_score(topic) = sum(source_score) / n_distinct_sources
```

Article chưa có row cho topic được tính là score 0. Điều này ngăn một outlet đăng nhiều
bài làm lệch nhãn.

### 2.8 Custom feed chưa nằm trên critical path tuần 2

Tuần 2 chỉ đảm bảo schema và API không khóa đường phát triển custom feed:

- story filter dùng stable `topic_id`/`source_id`, không dùng tên hiển thị;
- clustering không phụ thuộc user/feed;
- taxonomy node không phải classifier label;
- `feed_state.feed_url` không bị nhầm với `sources.id`.

`profiles`, `custom_feeds` và provenance `article_discoveries` được mô tả ở mục 10,
nhưng chưa migrate trong tuần 2. Auth/phân quyền hoàn toàn ngoài scope.

### 2.9 Evaluation bắt đầu bằng đúng 10 golden case

Tuần 2 không chờ tới khi có 100–150 article gán nhãn mới kiểm tra semantic quality.
Chốt một golden smoke suite gồm **10 scenario thủ công**; mỗi scenario có thể chứa 1–4
article ngắn, nên “10 case” không đồng nghĩa “10 article”. Cùng fixture được dùng cho
cả clustering và classification để không nhân đôi công gán nhãn.

Golden suite chỉ trả lời:

- các case điển hình có cluster/tách đúng không;
- topic bắt buộc/cấm có được gán hợp lý không;
- duplicate/source evidence/time-window/arrival-order có giữ đúng invariant không;
- một thay đổi code hoặc model có làm hành vi đã chốt bị regression không.

Không dùng 10 case để công bố F1 hay tune lặp tới khi test xanh. Expected result phải
được ghi trước khi nhìn output model; nếu output khác thì điều tra code/data/model trước
khi sửa fixture. Full offline evaluation 100–150 article và threshold sweep vẫn thuộc
tuần 4.

---



## 3. Kiến trúc mục tiêu cuối tuần 2

```text
backend/
  adapters/storage/
    schema.py                 # thêm cluster và taxonomy tables
    postgres.py               # chỉ repository/query, không chứa scoring policy
  services/
    clustering.py             # pure matching/centroid/merge policy
    classification.py         # pure score + source-balanced aggregation
    taxonomy.py               # validate seed, hash definition, compile prototype
  workers/
    cluster.py                # queue/lease/checkpoint; gọi clustering service
    classify.py               # queue/lease/checkpoint; gọi classifier service
    cli.py                    # thêm worker + taxonomy admin commands
  api/
    routers/stories.py
    routers/topics.py
    schemas.py
  tests/
    fixtures/week2_golden_cases.json
    test_clustering.py
    test_classification.py
    test_cluster_worker.py
    test_classify_worker.py
    test_story_api.py
    test_semantic_golden.py
    test_week2_storage_integration.py

data/
  taxonomy.seed.yaml          # bootstrap, không phải runtime source of truth

frontend/
  app/stories/
  components/stories/
  hooks/use-stories.ts
```

Các service thuật toán nhận dataclass/list/vector và trả kết quả, không tự mở DB. Worker
lo orchestration; `PostgresRawStorage` lo I/O; domain logic vẫn test được không cần
PostgreSQL hay model thật.

### 3.1 Luồng article sau embed

```text
EmbedWorker upsert embedding
  └─ idempotently enqueue trước khi complete embed job:
       processing_jobs(article_id, stage='classify')
       processing_jobs(article_id, stage='cluster')

ClassifyWorker                         ClusterWorker
  claim batch                            claim batch
  load active topic prototypes           load article embedding
  score article × topics                 find active cluster candidates
  replace article_topics                 add/create cluster
  refresh affected cluster_topics        refresh cluster_topics
  complete/retry/DLQ                     complete/retry/DLQ
```

Nếu article content đổi và được embed lại, cả hai downstream job phải được đưa từ
`done`/`dead_letter` về `pending`, reset lease/error, giống quy tắc enqueue embed hiện
tại. Không tạo thêm bảng queue theo worker.

Thứ tự `upsert embedding → enqueue downstream → complete embed` là bất biến bắt buộc.
Nếu process chết sau upsert nhưng trước enqueue, embed lease hết hạn và lần retry thấy
embedding unchanged vẫn phải ensure hai downstream job rồi mới complete. Nếu chết sau
enqueue nhưng trước complete, upsert queue idempotent làm lần retry vô hại. Không có
khe nào mà embed job đã `done` nhưng downstream job chưa tồn tại.

### 3.2 `ClusterWorker.execute`

```text
1. reclaim expired stage='cluster'
2. --reprocess: enqueue article có embedding trong cửa sổ nhưng chưa có membership
3. mỗi chunk:
     claim jobs, COMMIT ngay
     load article + embedding + source
     lấy top 20 cluster open trong 72h theo cosine
     best >= 0.75:
       add member; cập nhật centroid running mean
     không có best:
       tạo cluster mới, headline = title article
     recompute n_articles, n_distinct_sources, last_seen_at, cluster_topics
     complete job + checkpoint
4. sau ingest, nếu --merge hoặc run scheduled:
     tìm pair open có overlap thời gian và cosine >= 0.85
     merge transaction theo canonical id nhỏ hơn/cũ hơn
5. kiểm tra shutdown giữa các chunk và giữa các merge batch
```

`UNIQUE(cluster_members.article_id)` bảo đảm retry không gắn article vào hai cluster.
Nếu job chạy lại sau khi transaction đã commit nhưng trước khi queue complete, worker
đọc membership hiện tại và complete thay vì update centroid lần hai.

### 3.3 Merge cluster

Merge phải chạy trong một transaction:

1. Lock hai cluster theo thứ tự `id` để tránh deadlock.
2. Chọn canonical cluster: `first_seen_at` cũ hơn, tie-break bằng `id` nhỏ hơn.
3. Move member; `ON CONFLICT (article_id)` không nhân đôi.
4. Tính lại centroid từ toàn bộ member, không lấy trung bình hai centroid không
  trọng số.
5. Tính lại timestamps/counters/headline/topics.
6. Mark cluster còn lại `status='merged'`, `merged_into_id=canonical_id`.

API không trả cluster `merged`; truy cập trực tiếp id cũ trả redirect metadata tới
canonical id, không trả 404 bí ẩn.

### 3.4 `ClassifyWorker.execute`

```text
1. reclaim expired stage='classify'
2. load một classifier snapshot: active topics + prototype + signals
3. --reprocess/--topic-changed: enqueue article stale trong cửa sổ
4. mỗi chunk:
     claim jobs, COMMIT ngay
     load article embedding + title/content/source
     với mỗi active topic:
       prototype_similarity = max(0, cosine(article, prototype))
       keyword_score = min(1, tổng keyword weight khớp / 2)
       final_score = 0.8 * prototype_similarity + 0.2 * keyword_score
       assigned = final_score >= topic.threshold
     transaction replace article_topics + article_classifications
     refresh topic của cluster chứa article nếu có
     complete job + checkpoint
```

Keyword match không dùng substring thô (`AI` không được match vào `said`). Normalize
Unicode/case, phrase match theo token boundary; keyword chỉ boost và không thể tự vượt
threshold với trọng số 0.2.

Prototype compile dùng:

```text
prototype = L2_normalize(mean(L2_normalize(embed(exemplar_i))))
```

Topic không có tối thiểu ba exemplar hợp lệ không được activate. Nếu model/dimension
prototype khác article embedding, worker fail job với lỗi có thể hành động, không âm
thầm tính cosine sai.

---



## 4. Schema mục tiêu cuối tuần 2

`backend/adapters/storage/schema.py` tiếp tục là schema source of truth. Mọi revision
được tạo bằng Alembic autogenerate trên database đã ở head.

### 4.1 Cluster

```python
clusters = Table(
    "clusters", metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("centroid", Vector(384), nullable=False),
    Column("centroid_count", Integer, nullable=False),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("n_articles", Integer, nullable=False),
    Column("n_distinct_sources", Integer, nullable=False),
    Column("headline", Text),
    Column("status", Text, nullable=False),       # open | closed | merged
    Column("merged_into_id", BigInteger, ForeignKey("clusters.id")),
)
Index("ix_clusters_open_last_seen", clusters.c.last_seen_at,
      postgresql_where=text("status = 'open'"))

cluster_members = Table(
    "cluster_members", metadata,
    Column("cluster_id", BigInteger,
           ForeignKey("clusters.id", ondelete="CASCADE"), nullable=False),
    Column("article_id", BigInteger,
           ForeignKey("raw_articles.id", ondelete="CASCADE"), nullable=False),
    Column("similarity", Float, nullable=False),
    Column("added_at", DateTime(timezone=True), nullable=False,
           server_default=func.now()),
    PrimaryKeyConstraint("cluster_id", "article_id"),
    UniqueConstraint("article_id", name="uq_cluster_members_article"),
)
```

`centroid_count` đếm toàn bộ member. MVP không phân loại `primary`/`near_duplicate`;
nếu thêm L3 sau này thì migration và semantics của counter phải được quyết định riêng.

### 4.2 Semantic topic + classifier signals

```python
topics = Table(
    "topics", metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("slug", Text, nullable=False, unique=True),
    Column("display_name", Text, nullable=False),
    Column("description", Text),
    Column("threshold", Float, nullable=False, server_default="0.40"),
    Column("classifier_revision", Integer, nullable=False, server_default="1"),
    Column("active", Boolean, nullable=False, server_default="false"),
    Column("updated_at", DateTime(timezone=True), nullable=False,
           server_default=func.now()),
)

topic_signals = Table(
    "topic_signals", metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("topic_id", BigInteger,
           ForeignKey("topics.id", ondelete="CASCADE"), nullable=False),
    Column("kind", Text, nullable=False),          # exemplar | keyword
    Column("value", Text, nullable=False),
    Column("weight", Float, nullable=False, server_default="1"),
    Column("language", Text),
    Column("active", Boolean, nullable=False, server_default="true"),
    UniqueConstraint("topic_id", "kind", "value"),
)

topic_prototypes = Table(
    "topic_prototypes", metadata,
    Column("topic_id", BigInteger,
           ForeignKey("topics.id", ondelete="CASCADE"), primary_key=True),
    Column("model", Text, nullable=False),
    Column("dim", Integer, nullable=False),
    Column("definition_hash", LargeBinary, nullable=False),
    Column("embedding", Vector(384), nullable=False),
    Column("generated_at", DateTime(timezone=True), nullable=False,
           server_default=func.now()),
)
```

Prototype chỉ valid khi `definition_hash` khớp tập exemplar active hiện tại. Mọi thay
đổi ảnh hưởng scoring (exemplar, keyword, weight, threshold) đều tăng
`topics.classifier_revision`, nhưng chỉ thay exemplar mới cần build lại vector.

### 4.3 Taxonomy view

```python
taxonomies = Table(
    "taxonomies", metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("slug", Text, nullable=False, unique=True),
    Column("display_name", Text, nullable=False),
    Column("revision", Integer, nullable=False, server_default="1"),
    Column("active", Boolean, nullable=False, server_default="true"),
)

taxonomy_nodes = Table(
    "taxonomy_nodes", metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("taxonomy_id", BigInteger,
           ForeignKey("taxonomies.id", ondelete="CASCADE"), nullable=False),
    Column("parent_id", BigInteger, ForeignKey("taxonomy_nodes.id")),
    Column("slug", Text, nullable=False),
    Column("display_name", Text, nullable=False),
    Column("sort_order", Integer, nullable=False, server_default="0"),
    UniqueConstraint("taxonomy_id", "parent_id", "slug"),
)
Index("uq_taxonomy_nodes_root_slug", taxonomy_nodes.c.taxonomy_id,
      taxonomy_nodes.c.slug, unique=True,
      postgresql_where=taxonomy_nodes.c.parent_id.is_(None))

taxonomy_node_topics = Table(
    "taxonomy_node_topics", metadata,
    Column("node_id", BigInteger,
           ForeignKey("taxonomy_nodes.id", ondelete="CASCADE"), nullable=False),
    Column("topic_id", BigInteger,
           ForeignKey("topics.id", ondelete="CASCADE"), nullable=False),
    PrimaryKeyConstraint("node_id", "topic_id"),
)
```

Group node có thể không map topic; leaf node có thể map một hoặc nhiều topic. Không đặt
`parent_id` hay `level` trên `topics`, vì hai cột đó lại khóa semantic concept vào một
cây duy nhất.

### 4.4 Article/cluster topic scores

```python
article_classifications = Table(
    "article_classifications", metadata,
    Column("article_id", BigInteger,
           ForeignKey("raw_articles.id", ondelete="CASCADE"), primary_key=True),
    Column("model", Text, nullable=False),
    Column("classifier_set_hash", LargeBinary, nullable=False),
    Column("max_score", Float, nullable=False),
    Column("assigned_count", Integer, nullable=False),
    Column("classified_at", DateTime(timezone=True), nullable=False,
           server_default=func.now()),
)

article_topics = Table(
    "article_topics", metadata,
    Column("article_id", BigInteger,
           ForeignKey("raw_articles.id", ondelete="CASCADE"), nullable=False),
    Column("topic_id", BigInteger,
           ForeignKey("topics.id", ondelete="CASCADE"), nullable=False),
    Column("score", Float, nullable=False),
    Column("prototype_score", Float, nullable=False),
    Column("keyword_score", Float, nullable=False),
    Column("source_prior", Float),
    Column("assigned", Boolean, nullable=False),
    Column("classifier_revision", Integer, nullable=False),
    PrimaryKeyConstraint("article_id", "topic_id"),
)
Index("ix_article_topics_assigned", article_topics.c.topic_id,
      article_topics.c.article_id, postgresql_where=text("assigned"))

cluster_topics = Table(
    "cluster_topics", metadata,
    Column("cluster_id", BigInteger,
           ForeignKey("clusters.id", ondelete="CASCADE"), nullable=False),
    Column("topic_id", BigInteger,
           ForeignKey("topics.id", ondelete="CASCADE"), nullable=False),
    Column("score", Float, nullable=False),
    Column("assigned", Boolean, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False,
           server_default=func.now()),
    PrimaryKeyConstraint("cluster_id", "topic_id"),
)
Index("ix_cluster_topics_assigned", cluster_topics.c.topic_id,
      cluster_topics.c.cluster_id, postgresql_where=text("assigned"))
```

Tuần 2 lưu score cho mọi active topic, không chỉ row vượt threshold. Với dưới 50 topic
và quy mô vài nghìn article/ngày, chi phí này chấp nhận được và cho phép aggregate/eval
đúng. Đo kích thước ở `measurements.md`; nếu taxonomy tăng hàng trăm topic mới chuyển
sang top-K + assigned rows.

---



## 5. Clustering policy chi tiết



### 5.1 Online match

Ngưỡng ban đầu:


| Tham số               | Giá trị | Ý nghĩa                                       |
| --------------------- | ------- | --------------------------------------------- |
| `cluster_window`      | 72h     | Chỉ so cluster còn hoạt động                  |
| `cluster_threshold`   | 0.75    | Add article làm cluster member                |
| `merge_threshold`     | 0.85    | Merge nếu cửa sổ sự kiện overlap              |
| `cluster_close_after` | 72h     | Open không có member mới chuyển sang `closed` |


Centroid update cho member thứ `n`:

```text
new_centroid = (old_centroid × n + article_embedding) / (n + 1)
```

`centroid` lưu arithmetic mean chưa L2-normalize. Cosine distance chuẩn hóa khi so
sánh; giữ mean chưa normalize giúp running update chính xác và không phụ thuộc thứ tự
article đến. Nếu normalize sau từng update thì không thể khôi phục vector sum cũ chỉ từ
`centroid × n`.

Không dùng arrival order làm tie-break ngầm. Nếu hai candidate cùng similarity trong
sai số `1e-6`, chọn cluster có `last_seen_at` gần article hơn, rồi `id` nhỏ hơn.

### 5.2 Headline MVP

Không gọi LLM trong tuần 2. Headline là title của member gần centroid nhất;
tie-break bằng publisher đầu tiên rồi article id. Khi centroid/merge đổi, headline có
thể recompute. Summary để `NULL` tới bước LLM enrich sau MVP.

---



## 6. Taxonomy lifecycle



### 6.1 Seed format

YAML mô tả cả semantic topics và navigation mapping, nhưng chỉ admin command đọc nó:

```yaml
version: 1
topics:
  - slug: semiconductors
    name: Semiconductors
    threshold: 0.40
    exemplars:
      - chip manufacturer announces a new process node
      - foundry capacity and wafer shipments
      - export restrictions on lithography equipment
    keywords:
      - value: TSMC
        weight: 1.0
      - value: foundry
        weight: 0.7

taxonomies:
  - slug: default
    name: Default
    nodes:
      - slug: technology
        name: Technology
        children:
          - slug: hardware
            name: Hardware
            topics: [semiconductors]
```

Validation chạy hết trước khi ghi bất kỳ row nào: slug unique, reference tồn tại,
không cycle, threshold trong `[0,1]`, active topic có ít nhất ba exemplar, keyword
weight dương. Import toàn bộ trong một transaction.

### 6.2 Thay đổi và invalidation


| Thay đổi                        | Prototype        | Reclassify       | Recluster                    |
| ------------------------------- | ---------------- | ---------------- | ---------------------------- |
| Đổi `display_name` topic        | Không            | Không            | Không                        |
| Di chuyển/đổi tên taxonomy node | Không            | Không            | Không                        |
| Thêm mapping node ↔ topic       | Không            | Không            | Không                        |
| Đổi exemplar                    | Có               | Có               | Không                        |
| Đổi keyword/weight/threshold    | Nếu exemplar đổi | Có               | Không                        |
| Deactivate topic                | Không            | Xoá nhãn derived | Không                        |
| Đổi embedding model             | Có tất cả        | Có tất cả        | Có nếu article embedding đổi |


Reclassify mặc định chỉ enqueue article 30 ngày gần nhất; backfill sâu phải chỉ định
`--since`. `classifier_set_hash` giúp worker skip article đã dùng đúng tập topic
revision hiện tại.

### 6.3 Admin CLI tuần 2

```text
news-worker taxonomy-import --file PATH [--prune]
news-worker taxonomy-export --file PATH
news-worker taxonomy-rebuild [--topic SLUG]
news-worker classify --reprocess [--topic SLUG] [window]
```

Chưa làm write API/admin UI. Điều chỉnh taxonomy không cần sửa Python nhưng vẫn đi qua
CLI có validation và structured change summary, tránh sửa SQL tay. Các run build
prototype/reclassify được ghi trong `worker_runs`; import/export ngắn chỉ log mutation
summary và revision mới.

---



## 7. REST API + frontend MVP



### 7.1 API read-only

```text
GET /api/topics?taxonomy=default
  → cây navigation + stable topic references

GET /api/stories?topic=&source=&since=&until=&limit=&cursor=
  → story canonical, newest first theo (last_seen_at, id)

GET /api/stories/{id}
  → headline, topics, counters, member articles và source
```

`topic` và `source` nhận slug, có thể lặp lại. Nhiều topic/source dùng OR trong cùng
nhóm và AND giữa hai nhóm. Filter story dùng `EXISTS`/join trên `cluster_topics` và
member source; không load toàn bộ rồi lọc trong Python.

API tiếp tục chỉ đọc qua `PostgresRawStorage`; router không query table trực tiếp và
không ghi taxonomy/cluster.

### 7.2 Frontend

Frontend đã có article/source feed, vì vậy không scaffold lại. Tuần 2 chỉ thêm:

1. Bật nav item Stories.
2. Story list: headline, relative time, topic badges, số article, số source.
3. Detail panel/page tối thiểu hiển thị member/source.
4. Loading/empty/error state theo pattern article feed hiện tại.

Không làm frontend topic tree/filter nâng cao, Trends, storyline, SSE, saved query
editor hay custom feed builder trong tuần 2. API topic/source filter vẫn phải hoàn tất
ở T7 để frontend hoặc custom feed dùng sau này.

---



## 8. Task breakdown (14.5h)



### T0 — Data-quality remediation trước embedding (0.5h)

1. Nhận diện thêm CloudFront `ERROR: The request could not be satisfied` tại quality
  gate; block/interstitial là crawl failure, không phải duplicate article.
2. Bỏ `js_challenge`/`solution`/`token` khỏi identity **chỉ trên Reddit**, vì đây là
  transport state làm một thread sinh nhiều URL; không bỏ các key chung này ở host khác.
3. Chạy `news-worker cleanup-block-pages` dry-run rồi `--apply`: xoá title interstitial
  khỏi row summary-only lịch sử, invalidate embedding cũ, giữ feed summary, và đưa URL
  vào retry queue để crawl lại.
4. Unit/integration test detector, host-scoped canonicalization và cleanup idempotent.

**Vì sao:** nếu đưa block page giống nhau vào L2, hệ thống sẽ gộp nhầm nhiều article
thật khác nhau chỉ vì chúng cùng nhận một trang chặn. Quality/identity phải sạch trước
khi đo dedup hoặc tạo embedding, nếu không mọi metric và cluster phía sau đều sai.

**Done:** block page mới không được persist như body/title tin cậy; biến thể challenge
Reddit có cùng normalized URL; cleanup giữ summary nhưng không còn embedding/title rác
và mỗi URL bị ảnh hưởng có durable crawl failure đang chờ retry.

### T1 — Data audit + chọn golden inputs (0.5h)

1. Chụp snapshot article/source/feed/embedding coverage, time range, undated rate,
  exact duplicate title/content và content-length anomalies.
2. Lọc CAPTCHA/human-verification/empty/too-short pages trước khi chọn case.
3. Chọn input cho đúng 10 scenario ở §12.3; copy title + short excerpt + source +
  timestamp vào fixture, không tham chiếu database id hay fetch URL live khi test.
4. Ghi expected cluster/topic **trước** khi chạy model.

**Done:** có data-audit note trong `measurements.md` và fixture skeleton đủ 10
`case_id`, dù semantic runner sẽ hoàn thiện ở T9.

### T2 — Cluster schema + repository primitives (1.5h)

1. Thêm `clusters`, `cluster_members` và index active-window.
2. Dataclass cho candidate/member/cluster result; không để SQLAlchemy row lọt vào
  service thuật toán.
3. Repository methods: candidates, create, attach idempotent, refresh counters,
  lookup membership, close stale.
4. Integration test unique article membership và retry transaction.

**Done:** create/attach chạy lặp không nhân member hoặc centroid count.

### T3 — `ClusterWorker` online matching (2.5h)

1. Pure clustering policy: cosine, threshold, tie-break, running centroid.
2. Worker dùng `JobQueue` hiện tại: reclaim/claim/fail/complete/checkpoint.
3. Sửa `EmbedWorker` để luôn ensure cả `classify`/`cluster` job trước khi complete,
  kể cả nhánh skip embedding unchanged; backfill/reprocess theo `event_at`.
4. Mọi article vượt `cluster_threshold` đều là member và cập nhật centroid; không có
  nhánh suppress/`near_duplicate` trong MVP.
5. CLI/settings/stats + poison-pill isolation.
6. Unit tests bằng vector nhỏ/Fake repository; worker integration bằng pgvector thật.

**Done:** kill giữa hai chunk không mất chunk đã commit; replay job không update
centroid lần hai.

### T4 — Merge + repair cluster (1h)

1. Pair scan active cluster theo overlap + threshold.
2. Transaction merge theo §3.3, redirect metadata và recompute chính xác.
3. Thêm primitive `rebuild_cluster(cluster_id)` lấy `cluster_members` làm source of
  truth và tính lại atomically: arithmetic-mean centroid từ embedding hiện tại,
  `centroid_count`, `n_articles`, `n_distinct_sources`, min/max event time, headline và
  cluster topics. Cluster không còn member phải được delete hoặc close theo một policy
  chốt rõ, không giữ aggregate cũ.
4. Phân biệt retry unchanged với article đã re-embed. Nếu embedding của member đổi,
  không được complete chỉ vì membership đã tồn tại: remove/re-evaluate membership bằng vector mới, rebuild cluster cũ, rồi attach vào cluster phù hợp một cách idempotent.
5. Không dựa riêng vào `ON DELETE CASCADE` khi xoá article. Mọi delete path trong app
  phải lấy affected cluster và rebuild trong cùng transaction; `cluster --repair`
  phát hiện/sửa drift do dữ liệu lịch sử hoặc thao tác ngoài repository.
6. `cluster --merge-only`, `--repair`, `--reprocess`, close stale cluster.
7. Test merge A→B chạy lại an toàn; re-embedding không cộng hai phiên bản vector; xoá
  member không để centroid/counters/timestamps/headline stale; repair chạy hai lần vẫn
  cho cùng kết quả.

**Done:** hai cluster cùng sự kiện hội tụ về một canonical cluster; id cũ truy ra được
`merged_into_id`. Với mọi cluster canonical, aggregate khớp phép tính lại từ membership
và embedding hiện tại; retry, re-embedding, deletion và repair không tạo duplicate hoặc
làm `centroid_count` lệch `COUNT(cluster_members)`.

### T5 — Topic registry + taxonomy view + prototype compiler (2.5h)

1. Thêm schema §4.2–4.3.
2. YAML validator/import/export deterministic; DB là runtime source.
3. Definition hash + revision/invalidation rules.
4. Compile exemplar batch bằng `Embedder` hiện có; normalize prototype.
5. Admin CLI và unit/integration tests; import hai lần không tạo duplicate.
6. Seed taxonomy khởi đầu phù hợp nguồn hiện có, ưu tiên các beat kỹ thuật thay vì tạo
  finance/health rỗng chỉ để giống ví dụ spec.

**Done:** di chuyển topic giữa hai node không đổi classifier revision; sửa exemplar
tạo đúng một prototype revision mới.

### T6 — `ClassifyWorker` + aggregate cluster topic (2.5h)

1. Thêm `article_classifications`, `article_topics`, `cluster_topics` + partial index.
2. Pure classifier tính component score, threshold và classifier set hash.
3. Worker claim batch, replace score transactionally, refresh cluster nếu đã tồn tại.
4. Cluster attach/merge cũng gọi cùng một aggregate service.
5. Reprocess topic/window, stale detection, CLI/settings/stats/DLQ.
6. Test multi-label, uncategorized, keyword boundary, source-balanced vote và hai thứ
  tự `classify→cluster`/`cluster→classify` cho cùng kết quả.

**Done:** mọi article embedding đủ điều kiện có `article_classifications`; mọi cluster
có label derived đúng sau khi cả hai queue drain.

### T7 — Stories/topics REST API (1.5h)

1. Repository read methods + Pydantic schemas + routers §7.1.
2. Keyset cursor `(last_seen_at, id)`, filters thực hiện trong SQL.
3. Merged story lookup trả canonical reference.
4. Unit API test bằng fake storage và PostgreSQL integration cho filter/pagination.

**Done:** query topic/source không sinh duplicate story và không N+1 member query.

### T8 — Frontend Stories tối thiểu (0.5h)

1. Types/client proxy/hooks cho topics/stories.
2. Bật Stories nav và list; click dùng detail response hiện có hoặc panel tối thiểu.
3. Loading/error/empty states và `pnpm build` xanh; không redesign article UI, chưa
  làm topic tree/filter nâng cao trong task này.

**Done:** người dùng mở web và thấy story cùng số lượng publisher/member.

### T9 — Golden semantic runner + 10 case thủ công (1h)

1. Hoàn thiện `backend/tests/fixtures/week2_golden_cases.json` theo schema §12.2.
2. Viết `test_semantic_golden.py`: load model thật, chạy classify/cluster in-memory,
  normalize cluster member sets và assert expected/forbidden topics.
3. Thêm marker `semantic` vào pytest config; suite thường vẫn dùng `FakeEmbedder` và
  không tải model.
4. Chạy 10 case theo thứ tự gốc và ít nhất case arrival-order theo thứ tự đảo.
5. Ghi report pass/fail theo `case_id`; không tính F1 và không assert exact float.

**Done:** cả 10 scenario pass. Nếu còn case fail thì report phải giải thích failure,
nhưng T9 và mốc MVP vẫn chưa được đánh dấu hoàn thành; không sửa expected result chỉ để
làm test xanh.

### T10 — Compose + README + smoke notes (0.5h)

1. Wire API vào compose ở mức dev; không tự chạy migration và không nhét cron
  vào container.
2. README có flow crawl→embed→classify/cluster→API, bootstrap taxonomy và lệnh chạy
  semantic suite.
3. Cập nhật `measurements.md` với coverage/distribution/report từ T1/T9.

**Done:** documented commands đủ để migrate, seed, drain worker, mở Stories và chạy
golden suite; production scheduler/container hardening để sau MVP.

---



## 9. Thứ tự migration và triển khai

Hai revision; mỗi revision phải được tạo bằng `uv run alembic revision --autogenerate`
trên DB ở head, rồi review, `upgrade head` và `alembic check` trước revision tiếp theo.


| #   | Nội dung                                                         | Task  |
| --- | ---------------------------------------------------------------- | ----- |
| 1   | `clusters` + `cluster_members`                                   | T2–T4 |
| 2   | topic/taxonomy/prototype + article/cluster classification tables | T5–T6 |


Sau revision 2:

```bash
uv run news-worker taxonomy-import --file data/taxonomy.seed.yaml
uv run news-worker taxonomy-rebuild

# Backfill downstream jobs cho article embeddings đã có từ tuần 1
uv run news-worker classify --reprocess --last 30d
uv run news-worker cluster --reprocess --last 30d

# Drain theo thứ tự nào cũng được; chạy lại lần cuối để chứng minh eventual consistency
uv run news-worker classify --once
uv run news-worker cluster --once
uv run news-worker classify --once

# Gate semantic bắt buộc trước khi chốt MVP
uv run --extra dev pytest backend/tests/test_semantic_golden.py -m semantic
```

Không hand-write revision và không gộp backfill model/prototype vào migration. Migration
chỉ đổi schema; command có run record mới thực hiện công việc có thể lâu/retry.

### 9.1 Lịch vận hành MVP

```cron
*/30 * * * * cd /srv/news-radar && uv run news-worker crawl --once
*/5  * * * * cd /srv/news-radar && uv run news-worker embed --once
*/5  * * * * cd /srv/news-radar && uv run news-worker classify --once
*/5  * * * * cd /srv/news-radar && uv run news-worker cluster --once
7    * * * * cd /srv/news-radar && uv run news-worker cluster --merge-only
```

Classify và cluster có advisory lock riêng nên chạy song song được. Eventual consistency
cho phép story xuất hiện trước topic trong vài phút; API không coi đó là lỗi.

---



## 10. Đường mở cho custom feed sau tuần 2

Custom feed là saved query trên global story graph, không copy article và không cluster
theo user:

```text
profiles(id, name)  -- default profile khi chưa có auth

custom_feeds(id, profile_id, name, slug, active)
custom_feed_topics(feed_id, topic_id, min_score)
custom_feed_sources(feed_id, source_id, mode)  -- include | exclude
custom_feed_rules(feed_id, language, query, min_distinct_sources, window)
```

Nếu user chỉ chọn publisher đã có, filter bằng `sources.id` là đủ. Nếu user được thêm
RSS URL bất kỳ và feed cá nhân phải phản ánh chính xác URL discovery đó, cần thêm:

```text
ingestion_feeds(id, feed_url, label, active, ...poll configuration...)
profile_feed_subscriptions(profile_id, ingestion_feed_id)
article_discoveries(article_id, ingestion_feed_id, discovered_at)
```

`feed_state` hiện chỉ giữ trạng thái poll, còn `raw_articles.source_id` là publisher
của final URL. Không có `article_discoveries` thì không thể biết bài đến từ RSS URL nào,
đặc biệt với Google News/query feed có nhiều publisher.

API tương lai:

```text
POST /api/custom-feeds
PUT  /api/custom-feeds/{id}/topics
PUT  /api/custom-feeds/{id}/sources
PUT  /api/custom-feeds/{id}/subscriptions
GET  /api/custom-feeds/{id}/stories
GET  /api/custom-feeds/{id}.rss
```

Chưa có auth thì `profile_id` chỉ là namespace, không phải security boundary. Khi thêm
auth sau này, bổ sung ownership/authorization quanh các bảng cấu hình; không đổi cluster
hay article identity.

---



## 11. Nếu bắt buộc giữ đúng 12h

Cắt theo thứ tự sau:

1. Compose frontend/API ở T10, giữ run trên host và README commands (−0.5h).
2. Taxonomy export và `--prune`, giữ import idempotent (−0.5h).
3. Frontend Stories ở T8, kiểm tra bằng API/curl trong MVP (−0.5h).
4. Merge command tự động, giữ schema `merged_into_id` và dời merge sang đầu tuần 3
  (−1h).

**Không cắt:** global cluster identity, idempotent membership, DB-backed topic registry,
semantic topic tách taxonomy node, classifier revision, source-balanced cluster vote và
API story detail. T0 quality remediation, T1 data audit và T9 với 10 golden case cũng
không cắt: nếu bỏ chúng thì Tuần 2 không có bằng chứng semantic tối thiểu và không còn
là kế hoạch data-driven.

---



## 12. Test matrix và số đo



### 12.1 Unit test không cần PostgreSQL/model thật


| Nhóm        | Cases bắt buộc                                               |
| ----------- | ------------------------------------------------------------ |
| Clustering  | create/add/duplicate/tie-break/centroid; arrival order cases |
| Merge       | canonical selection; exact recompute; retry idempotent       |
| Taxonomy    | invalid slug/ref/cycle; deterministic hash/import/export     |
| Classifier  | multi-label; keyword boundary; threshold; missing prototype  |
| Aggregation | mỗi source một vote; absent topic = 0; merge recompute       |
| API         | cursor/filter/404/merged reference; no duplicate story       |


Toàn bộ test thuật toán dùng vector nhỏ deterministic hoặc `FakeEmbedder`; unit suite
không tải MiniLM và không cần network.

### 12.2 Golden fixture — một nguồn cho cả cluster và classify

File `backend/tests/fixtures/week2_golden_cases.json` dùng key ổn định, không dùng
`raw_articles.id` và không fetch URL live. Chỉ snapshot title + short excerpt cần cho
embedding, tránh fixture lớn và kết quả thay đổi khi publisher sửa bài.

```json
[
  {
    "case_id": "same_event_different_sources",
    "description": "Hai publisher đưa tin cùng một đợt tăng giá sản phẩm",
    "articles": [
      {
        "key": "price-source-a",
        "source": "source-a.example",
        "published_at": "2026-08-24T10:00:00Z",
        "title": "Company plans a price increase for its next device",
        "text": "A short, stable excerpt used by the real embedder.",
        "expected_topics": ["consumer-hardware"],
        "forbidden_topics": ["cybersecurity"]
      },
      {
        "key": "price-source-b",
        "source": "source-b.example",
        "published_at": "2026-08-24T11:00:00Z",
        "title": "Next device lineup may become more expensive",
        "text": "A second publisher reports the same announced price change.",
        "expected_topics": ["consumer-hardware"],
        "forbidden_topics": ["cybersecurity"]
      }
    ],
    "expected_clusters": [["price-source-a", "price-source-b"]]
  }
]
```

Expected labels/member sets được viết trước lần chạy model đầu tiên. Khi output khác,
điều tra theo thứ tự data quality → input builder/model version → algorithm/threshold →
manual expectation. Chỉ sửa expectation khi có lý do domain rõ ràng và ghi lại reason.

### 12.3 Danh sách chốt đúng 10 golden scenario


| #   | Scenario                               | Kỳ vọng chính                            |
| --- | -------------------------------------- | ---------------------------------------- |
| 1   | Cùng event, hai publisher              | cùng cluster, giữ đủ hai source          |
| 2   | Cùng event, title paraphrase mạnh      | vẫn cùng cluster                         |
| 3   | Cùng topic nhưng khác event            | khác cluster, có thể cùng topic          |
| 4   | Hai bản crawl cùng publisher           | cùng cluster; cả hai là member trong MVP |
| 5   | Nội dung gần giống từ hai publisher    | cùng cluster và giữ cả hai evidence      |
| 6   | Article đa nhãn                        | có đủ hai `expected_topics`              |
| 7   | Hard negative chứa keyword             | `forbidden_topic` không được assigned    |
| 8   | Article ngoài taxonomy                 | không assigned topic/`uncategorized`     |
| 9   | Semantic giống nhưng cách nhau quá 72h | không cùng event cluster                 |
| 10  | Arrival order khác nhau                | sau merge có cùng canonical member set   |


Một scenario có thể chứa 1–4 article; cùng article được dùng đồng thời cho expected
cluster và expected topics. Không bổ sung scenario thứ 11 trong Tuần 2: case mới phát
hiện khi implement được ghi backlog cho evaluation mở rộng, trừ khi nó thay thế một
case trùng giá trị kiểm thử.

### 12.4 Semantic runner dùng model thật

`test_semantic_golden.py` được đánh dấu `@pytest.mark.semantic`; unit suite thường vẫn
dùng `FakeEmbedder`. Runner:

1. Load đúng model và cùng `embed_text()` với production.
2. Chạy classify + cluster in-memory trên từng case.
3. So sánh cluster dưới dạng set các article key, không so cluster id.
4. Assert `expected_topics ⊆ assigned_topics` và
  `forbidden_topics ∩ assigned_topics = ∅`.
5. Assert distinct-source retention và case time-window nếu khai báo.
6. Chạy case 10 theo ít nhất hai permutation rồi so canonical member sets.

Không assert cosine/final score chính xác nhiều chữ số; chỉ assert quyết định, ranking
hoặc margin khi case yêu cầu. Model name/version và input builder phải được ghi trong
report để phân biệt regression code với model drift.

### 12.5 PostgreSQL integration

1. Hai worker claim concurrent không nhận cùng job.
2. Retry attach không tăng `centroid_count` lần hai.
3. Unique article membership giữ đúng qua merge.
4. Topic import hai lần không nhân signal/node/mapping.
5. Reclassify thay row cũ atomically; reader không thấy nửa revision.
6. Cluster topics giống nhau ở hai thứ tự worker.
7. Stories filter/pagination dùng index và không N+1.



### 12.6 Số đo phải ghi


| Hạng mục   | Số đo                                                     |
| ---------- | --------------------------------------------------------- |
| Data audit | embedding/undated/junk coverage, unique titles/content    |
| Golden     | số case pass/10, danh sách `case_id` fail và reason       |
| Cluster    | p50/p95 candidate query, articles/s, size distribution    |
| Classifier | articles/s, confidence histogram, uncategorized rate      |
| Taxonomy   | prototype rebuild time/topic, reclassify time/1k articles |
| API        | stories p50/p95, query plan cho topic/source filter       |


Tuần 2 chỉ báo `N/10 golden cases passed`, không tính precision/recall/F1 từ sample quá
nhỏ và đã được dùng để chỉnh implementation. Tuần 4 mới tạo development/frozen split
trên 100–150 article, quét threshold và báo metric clustering/classification.

---



## 13. Rủi ro của riêng tuần 2


| Rủi ro                         | Dấu hiệu                                  | Xử lý                                                           |
| ------------------------------ | ----------------------------------------- | --------------------------------------------------------------- |
| Cluster vỡ vụn                 | phần lớn cluster size = 1                 | log best similarity; tune 0.75 trên sample, không đổi âm thầm   |
| Cluster gộp quá mức            | story chứa hai sự kiện khác nhau          | giới hạn 72h + eval sample; thêm entity/time feature ở tuần 3   |
| Arrival order làm kết quả khác | backfill hai thứ tự cho cluster khác nhau | deterministic tie-break + hourly merge + test permutation       |
| Taxonomy không khớp nguồn      | `assigned_count=0` > 20%                  | seed beat kỹ thuật trước; xem confidence histogram/exemplars    |
| Keyword gây false positive     | keyword ngắn như `AI` match text thường   | Unicode/token boundary; keyword chỉ tối đa 0.2 score            |
| Prototype/model lệch           | cosine error hoặc score bất thường        | check model/dim trước batch; DLQ có actionable error            |
| Reclassify tạo backlog lớn     | pending classify tăng liên tục            | default 30d, micro-batch, classifier set hash skip unchanged    |
| Merge deadlock                 | transaction timeout                       | lock cluster theo id tăng dần; một cluster worker advisory lock |
| `article_topics` tăng nhanh    | table/index lớn hơn dự kiến               | partial index chỉ assigned; đo trước khi chuyển top-K           |
| Golden fixture chứa crawl rác  | model fail vì CAPTCHA/empty page          | T0 quality gate; fixture dùng snapshot excerpt đã review        |
| Overfit vào 10 golden case     | chỉnh threshold tới khi đúng đúng 10 case | không báo F1; giữ change log; frozen set 100–150 bài ở tuần 4   |
| Frontend vượt scope            | dành thời gian redesign article UI        | chỉ bật Stories trên component patterns sẵn có                  |


---



## 14. Các mặc định đã chốt để triển khai

1. **Taxonomy seed:** 6–10 beat kỹ thuật phù hợp corpus đã crawl; không seed
  finance/health rỗng chỉ để giống ví dụ spec.
2. **Ngôn ngữ exemplar:** English trước vì corpus hiện quan sát chủ yếu là English;
  không suy ra ngôn ngữ từ `lang` cho tới khi crawler populate cột này đáng tin cậy.
3. **Cluster/article topic threshold:** bắt đầu `0.55`, hạ xuống `0.40` ngày 2026-08-28
  theo số đo T6 (xem [measurements.md](measurements.md)); vẫn chưa thêm một threshold
  cluster riêng trong Tuần 2 để tránh tăng tham số trên sample nhỏ.
4. **Compose:** dev full stack ở mức tối thiểu; cron/systemd cho worker production;
  không thêm scheduler container.
5. **Evaluation:** đúng 10 golden scenario ở §12.3, expected result ghi trước output,
  báo pass/10; không báo F1 trong Tuần 2.
6. **Mở rộng eval:** giữ 10 case làm regression suite; tập 100–150 article và frozen
  test split thuộc Tuần 4, không làm phình scope Tuần 2.

Đây là baseline đã chốt. Khi dữ liệu cho thấy baseline sai, thay đổi phải kèm snapshot,
case/report bị ảnh hưởng và giá trị cũ/mới trong `worker_runs.params` hoặc
`measurements.md`; không chỉnh threshold âm thầm để làm golden suite xanh.

---



## 15. Backlog sau MVP



### Dedup L2/L3 (chờ số liệu và quyết định)

L2 lexical near-duplicate và L3 semantic near-duplicate không thuộc task hay migration
Tuần 2. MVP giữ mọi article hợp lệ trong cluster để ưu tiên correctness, source evidence
và một data model đơn giản. Hai hướng chỉ được mở lại độc lập sau khi cluster chạy trên
dữ liệu đã qua T0:

- **L2 candidate:** SimHash 64-bit trên title + leading content, match cùng `source_id`
trong 7 ngày với Hamming distance ban đầu `<= 3`; nếu suppress embedding thì phải có
`article_duplicates(article_id, canonical_article_id, method, distance, created_at)`
để audit, re-evaluate khi content đổi và đo false positive. `raw_articles.simhash`
chỉ thêm cùng lúc với capability này, không thêm trước để “giữ chỗ”.
- **L3 candidate:** similarity rất cao trong cùng publisher (ngưỡng thử nghiệm `0.92`)
có thể được đánh dấu `near_duplicate` và không kéo centroid. Việc này cần migration
cho member semantics và quy tắc recompute rõ ràng; cross-publisher evidence luôn giữ.

Trước khi quyết định, đo trên sample đã loại block page và canonicalize Reddit: duplicate
rate còn lại theo source, chi phí embedding lãng phí, mức centroid/headline bị lệch và
false-positive sample giữa “cùng chủ đề” với “cùng bài”. Chỉ triển khai L2 nếu tiết kiệm
chi phí hoặc giảm rác đủ đáng kể; chỉ triển khai L3 nếu member lặp cùng source thực sự
làm cluster/UX xấu. Nếu không đạt các điều kiện đó, tiếp tục dựa vào L1 + quality gate +
clustering hiện tại.

### Làm sạch boilerplate ngay tại crawler (P2, chưa xếp task)

Corpus ngày 2026-08-26 có 81/582 article crawl bị nav-dense; 25 row đã dùng feed
summary sạch, còn 56 row không có summary nay dùng title-only để tránh publisher chrome
làm lệch embedding. Nếu title-only làm giảm recall clustering đáng kể, cải thiện
upstream theo thứ tự:

1. Thử global `excluded_tags` cho `nav`, `header`, `footer`, `aside` trong
  `CrawlerRunConfig`; không bật mặc định trước khi review sample thật.
2. Thêm source-specific target selector cho các family chiếm phần lớn lỗi hiện tại:
  CNET, Slashdot, Mashable và Apple; giữ registry ngoài crawler adapter để policy không
  trộn vào I/O plumbing.
3. Recrawl một sample cố định trước/sau, đo leading-link count, body coverage và
  unrelated same-source cosine; chỉ rollout selector đã chứng minh không cắt mất bài.
4. Reprocess embedding trong cửa sổ bị ảnh hưởng sau khi extraction policy đổi.

Không đưa P2 vào T0–T10 hoặc ngân sách 14.5h. Chưa làm headline-anchor extraction P1:
MVP ưu tiên fallback title-only đơn giản, deterministic và dễ audit.
