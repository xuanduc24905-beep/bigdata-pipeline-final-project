from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Spotify Hive Query") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "10g") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.hive.metastore.schema.verification", "false") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE DATABASE IF NOT EXISTS music")
spark.sql("USE music")

cleaned      = spark.read.parquet("hdfs://namenode:9000/music/processed/cleaned")
clustered    = spark.read.parquet("hdfs://namenode:9000/music/processed/clustered")
decade_stats = spark.read.parquet("hdfs://namenode:9000/music/processed/decade_stats")

cleaned.createOrReplaceTempView("tracks_cleaned")
clustered.createOrReplaceTempView("tracks_clustered")
decade_stats.createOrReplaceTempView("decade_stats")

# ── 1. Tổng quan theo thập kỷ ────────────────────────────────
print("\n=== 1. Tổng quan theo thập kỷ ===")
spark.sql("""
    SELECT decade, track_count, avg_popularity,
           avg_energy, avg_acousticness, avg_loudness, avg_danceability,
           explicit_pct
    FROM decade_stats
    ORDER BY decade
""").show(20)

# ── 2. Loudness War: loudness tăng dần 1920→2020 ────────────
print("\n=== 2. Loudness War (1920→2020) ===")
spark.sql("""
    SELECT decade,
           ROUND(AVG(loudness), 3)  AS avg_loudness,
           ROUND(MIN(loudness), 2)  AS min_loudness,
           ROUND(MAX(loudness), 2)  AS max_loudness,
           COUNT(*)                  AS tracks
    FROM tracks_cleaned
    GROUP BY decade
    ORDER BY decade
""").show(20)

# ── 3. Energy trend và Acoustic Retreat ──────────────────────
print("\n=== 3. Energy vs Acousticness trend theo thập kỷ ===")
spark.sql("""
    SELECT decade,
           ROUND(AVG(energy),       3) AS avg_energy,
           ROUND(AVG(acousticness), 3) AS avg_acousticness,
           ROUND(AVG(danceability), 3) AS avg_danceability,
           ROUND(AVG(valence),      3) AS avg_valence,
           COUNT(*)                     AS tracks
    FROM tracks_cleaned
    GROUP BY decade
    ORDER BY decade
""").show(20)

# ── 4. Explicit content rise sau 1990 ────────────────────────
print("\n=== 4. Explicit Content — Rise sau 1990 ===")
spark.sql("""
    SELECT decade,
           COUNT(*) AS total,
           SUM(CASE WHEN explicit = 'True' THEN 1 ELSE 0 END) AS explicit_count,
           ROUND(
               SUM(CASE WHEN explicit = 'True' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
           ) AS explicit_pct
    FROM tracks_cleaned
    GROUP BY decade
    ORDER BY decade
""").show(20)

# ── 5. Popularity drivers — real audio feature correlation ───
print("\n=== 5. Popularity Drivers (top 1000 tracks, sorted by correlation) ===")
spark.sql("""
    SELECT
        ROUND(CORR(popularity, danceability),     4) AS corr_danceability,
        ROUND(CORR(popularity, energy),           4) AS corr_energy,
        ROUND(CORR(popularity, loudness),         4) AS corr_loudness,
        ROUND(CORR(popularity, acousticness),     4) AS corr_acousticness,
        ROUND(CORR(popularity, instrumentalness), 4) AS corr_instrumentalness,
        ROUND(CORR(popularity, valence),          4) AS corr_valence,
        ROUND(CORR(popularity, tempo),            4) AS corr_tempo,
        ROUND(CORR(popularity, speechiness),      4) AS corr_speechiness,
        ROUND(CORR(popularity, year),             4) AS corr_year
    FROM tracks_cleaned
""").show()

# ── 6. Cluster Distribution ───────────────────────────────────
print("\n=== 6. Cluster Distribution ===")
spark.sql("""
    SELECT cluster,
           COUNT(*) AS track_count,
           ROUND(AVG(popularity),   2) AS avg_popularity,
           ROUND(AVG(danceability), 3) AS avg_danceability,
           ROUND(AVG(energy),       3) AS avg_energy,
           ROUND(AVG(valence),      3) AS avg_valence,
           ROUND(AVG(loudness),     2) AS avg_loudness,
           ROUND(AVG(acousticness), 3) AS avg_acousticness
    FROM tracks_clustered
    GROUP BY cluster
    ORDER BY cluster
""").show()

# ── 7. Cluster composition by era ────────────────────────────
print("\n=== 7. Cluster × Decade Distribution ===")
spark.sql("""
    SELECT cluster, decade, COUNT(*) AS count
    FROM tracks_clustered
    GROUP BY cluster, decade
    ORDER BY cluster, decade
""").show(100)

# ── 8. Top 20 tracks all-time ────────────────────────────────
print("\n=== 8. Top 20 All-Time Tracks ===")
spark.sql("""
    SELECT track_name, artists, year, decade, popularity, cluster
    FROM tracks_clustered
    ORDER BY popularity DESC
    LIMIT 20
""").show(truncate=False)

# ── 9. Best decade for each cluster ──────────────────────────
print("\n=== 9. Dominant Era per Cluster ===")
spark.sql("""
    SELECT cluster, decade, COUNT(*) AS cnt
    FROM (
        SELECT cluster, decade,
               ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY COUNT(*) DESC) AS rn
        FROM tracks_clustered
        GROUP BY cluster, decade
    ) t
    WHERE rn = 1
    ORDER BY cluster
""").show()

# ── 10. Top 5 danceable tracks per decade ────────────────────
print("\n=== 10. Top 5 Danceable Tracks per Decade ===")
spark.sql("""
    SELECT decade, track_name, artists, danceability, energy, valence, year
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY decade ORDER BY danceability DESC) AS rn
        FROM tracks_cleaned
    ) t
    WHERE rn <= 5
    ORDER BY decade, danceability DESC
""").show(100, truncate=False)

print("\nAll Hive queries done")
spark.stop()
