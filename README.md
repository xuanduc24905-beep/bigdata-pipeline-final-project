# Lambda Architecture for Music Data Analytics

Pipeline phân tích dữ liệu âm nhạc theo kiến trúc Lambda, kết hợp Batch Layer (Spark + HDFS + Hive) và Speed Layer (Kafka + Spark Structured Streaming), trực quan hóa qua Streamlit với trang phân tích xu hướng âm nhạc 100 năm (1921→2020).

## Kiến trúc Lambda

```
                    ┌─────────────────────────────────┐
                    │   Spotify 1921-2020 (~600K tracks)│
                    └──────────┬──────────────────────┘
                               │
              ┌────────────────┼────────────────────┐
              │ BATCH LAYER    │                     │ SPEED LAYER
              ▼                │                     ▼
   00_load_csv.py              │         03_kafka_producer.py
   (Schema mapping +           │         (CSV → Kafka topic)
    decade column)             │                     │
              │                │         03_spark_streaming.py
   HDFS /music/raw/            │         (Kafka → HDFS append)
              │                │                     │
   01_eda.py ─┤                │              HDFS /music/streaming/
   (EDA + decade stats)        │
              │
   02_kmeans.py
   (KMeans clustering k=3~8)
              │
   04_hive_query.py
   (Temporal analysis via Hive)
              │
   05_export.py
   (HDFS Parquet → /data/*.parquet)
              │
              └─────────────────────────► Streamlit Dashboard
                                          (5 trang, live refresh)
```

## Stack

| Thành phần | Công nghệ |
|---|---|
| Dataset | Spotify 1921-2020, ~600K tracks (Kaggle) |
| Message queue | Apache Kafka + Zookeeper |
| Batch processing | Apache Spark 3.5 (MLlib, SQL) |
| Speed layer | Spark Structured Streaming |
| Lưu trữ | Hadoop HDFS (1 namenode + 2 datanode) |
| Data warehouse | Apache Hive + PostgreSQL metastore |
| Dashboard | Streamlit (5 trang, tự động refresh theo giây) |
| Hạ tầng | Docker Compose (12 containers) |

## Dataset

**Spotify Dataset 1921-2020** từ Kaggle (~600K tracks):

```
Kaggle: https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks
Schema gốc: id, name, artists, release_date, year,
            acousticness, danceability, duration_ms, energy, explicit,
            instrumentalness, key, liveness, loudness, mode,
            popularity, speechiness, tempo, valence
```

Tải file `tracks.csv` từ Kaggle, đặt tại `data/spotify_tracks.csv`.

Script `00_load_csv.py` sẽ tự động:
- Rename `id` → `track_id`, `name` → `track_name`
- Tạo cột `decade` từ `year` (ví dụ: 1993 → "1990s")
- Loại bỏ duplicates và NaN

## Cài đặt & Khởi động

**Yêu cầu:** Docker, Docker Compose, ~16GB RAM

```bash
git clone <repo-url>
cd Lambda-Architecture-Music-Analytics

# Đặt Spotify dataset tại:
# data/spotify_tracks.csv

docker compose up -d
```

Chờ ~2 phút để tất cả 12 services khởi động.

## Chạy pipeline

### Cách 1 — Tự động (khuyến nghị)

```bash
./run_pipeline.sh
```

Script tự động chạy toàn bộ 6 bước batch, sau đó khởi động streaming.

### Cách 2 — Thủ công từng bước

**Bước 1 — Xử lý CSV (mapping schema + tạo decade):**
```bash
docker exec spark-master python /spark-jobs/00_load_csv.py
```

**Bước 2 — Upload lên HDFS:**
```bash
docker exec namenode bash -c \
  "hdfs dfs -mkdir -p /music/raw && hdfs dfs -put -f /data/spotify_tracks.csv /music/raw/"
```

**Bước 3 — EDA + làm sạch dữ liệu:**
```bash
docker exec spark-master bash -c \
  "spark-submit --master spark://spark-master:7077 /spark-jobs/01_eda.py"
```

**Bước 4 — KMeans clustering:**
```bash
docker exec spark-master bash -c \
  "spark-submit --master spark://spark-master:7077 /spark-jobs/02_kmeans.py"
```

**Bước 5 — Truy vấn Hive (phân tích thời gian):**
```bash
docker exec spark-master bash -c \
  "spark-submit --master spark://spark-master:7077 /spark-jobs/04_hive_query.py"
```

**Bước 6 — Export ra local:**
```bash
docker exec spark-master bash -c \
  "spark-submit --master spark://spark-master:7077 /spark-jobs/05_export.py"
```

## Streaming thời gian thực

```bash
# Terminal 1 — Kafka producer (đẩy 600K tracks liên tục từ CSV, ~100 tracks/giây)
docker exec -it spark-master python /spark-jobs/03_kafka_producer.py

# Terminal 2 — Spark Structured Streaming ghi xuống HDFS
docker exec -it spark-master bash -c \
  "spark-submit --master spark://spark-master:7077 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
   /spark-jobs/03_spark_streaming.py"
```

Dashboard trang **Live Stream** tự refresh mỗi giây từ HDFS.

## Dashboard (http://localhost:8501)

| Trang | Nội dung |
|---|---|
| Overview | Thống kê tổng quan, tracks per decade, scatter plot theo cluster |
| Timeline Analysis | **Xu hướng âm nhạc 1921→2020** (Loudness War, Energy trend, Explicit rise, Radar chart) |
| Cluster Analysis | KMeans clusters, composition by decade, browse tracks |
| Top Tracks | Lọc theo decade, xếp hạng theo popularity |
| Live Stream | Dữ liệu thời gian thực từ HDFS, tự động cập nhật mỗi giây |

## Insights chính

| Insight | Mô tả |
|---|---|
| **Loudness War** | Loudness tăng từ ~-20 dBFS (1920s) lên ~-7 dBFS (2010s) do mastering kỹ thuật số |
| **Energy vs Acoustic** | Energy tăng dần, acousticness giảm mạnh từ 1960s khi điện âm phổ biến |
| **KMeans = Era** | Cluster tự nhiên tương ứng với các era âm nhạc, không cần nhãn genre |
| **Explicit Rise** | Tỷ lệ explicit tăng đột biến sau 1990 (hip-hop + streaming) |
| **Popularity Driver** | Danceability và loudness có tương quan dương mạnh nhất với popularity |

## Spark Jobs

| File | Mô tả |
|---|---|
| `00_load_csv.py` | Đọc Kaggle CSV, map schema (id→track_id, name→track_name), tạo cột decade |
| `01_eda.py` | EDA, thống kê theo decade, Loudness War, tương quan popularity → HDFS Parquet |
| `02_kmeans.py` | KMeans k=3~8 (elbow + silhouette), cluster = era âm nhạc → HDFS Parquet |
| `03_kafka_producer.py` | Stream 600K tracks từ CSV vào Kafka liên tục (~100 tracks/giây) |
| `03_spark_streaming.py` | Tiêu thụ Kafka, enrich data (era, popularity_tier), ghi append xuống HDFS |
| `04_hive_query.py` | 10 analytical queries: Loudness War, energy trend, explicit rise, popularity drivers |
| `05_export.py` | Export HDFS Parquet → local `/data/` cho Streamlit đọc |

## Web UIs

| Service | URL |
|---|---|
| Streamlit Dashboard | http://localhost:8501 |
| Spark Master | http://localhost:8080 |
| HDFS Namenode | http://localhost:9870 |
| YARN Resource Manager | http://localhost:8088 |
| HiveServer2 Web UI | http://localhost:10002 |
