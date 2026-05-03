#!/bin/bash
# Lambda Architecture — Music Analytics Pipeline
# Dataset: Spotify 1921-2020 (~600K tracks)
set -e

SPARK="docker exec spark-master bash -c"
SPARK_SUBMIT="spark-submit --master spark://spark-master:7077"

echo "=============================================="
echo "  Lambda Architecture — Music Analytics"
echo "  Dataset: Spotify 1921-2020 (~600K tracks)"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=============================================="
echo ""

# ── BATCH PHASE (AIRFLOW) ───────────────────────────────────
echo "=============================================="
echo "  Batch Layer đã được chuyển sang Apache Airflow!"
echo "  Vui lòng truy cập Web UI để chạy DAG:"
echo "  👉 http://localhost:8083"
echo "=============================================="
echo ""

# ── STREAMING PHASE ──────────────────────────────────────────
echo "Starting streaming layer..."

docker exec -d spark-master bash -c \
  "python /spark-jobs/03_kafka_producer.py > /tmp/producer.log 2>&1"
echo "  [+] Kafka producer started  (log: /tmp/producer.log)"

docker exec -d spark-master bash -c \
  "spark-submit --master spark://spark-master:7077 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
   /spark-jobs/03_spark_streaming.py > /tmp/streaming.log 2>&1"
echo "  [+] Spark Streaming started (log: /tmp/streaming.log)"

echo ""
echo "=============================================="
echo "  Pipeline đang chạy!"
echo ""
echo "  Airflow UI   : http://localhost:8083 (admin/admin)"
echo "  Dashboard    : http://localhost:8501"
echo "  Spark UI     : http://localhost:8080"
echo "  HDFS UI      : http://localhost:9870"
echo "  HiveServer2  : http://localhost:10002"
echo ""
echo "  Xem log streaming:"
echo "  docker exec spark-master tail -f /tmp/producer.log"
echo "  docker exec spark-master tail -f /tmp/streaming.log"
echo "=============================================="
