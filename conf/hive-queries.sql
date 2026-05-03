-- ================================================================
-- MUSIC ANALYTICS — HIVE QUERIES
-- Chạy bằng: beeline -u jdbc:hive2://localhost:10000
-- ================================================================

-- 1. Tạo database
CREATE DATABASE IF NOT EXISTS music;
USE music;

-- ================================================================
-- 2. TẠO BẢNG TỪ PARQUET TRÊN HDFS
-- ================================================================

-- Bảng tracks đã clean
CREATE EXTERNAL TABLE IF NOT EXISTS music.tracks_cleaned (
    track_id        STRING,
    artists         STRING,
    album_name      STRING,
    track_name      STRING,
    popularity      FLOAT,
    duration_ms     FLOAT,
    explicit        STRING,
    danceability    FLOAT,
    energy          FLOAT,
    loudness        FLOAT,
    speechiness     FLOAT,
    acousticness    FLOAT,
    instrumentalness FLOAT,
    liveness        FLOAT,
    valence         FLOAT,
    tempo           FLOAT,
    track_genre     STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/music/processed/cleaned';

-- Bảng kết quả clustering
CREATE EXTERNAL TABLE IF NOT EXISTS music.tracks_clustered (
    track_id        STRING,
    track_name      STRING,
    artists         STRING,
    album_name      STRING,
    track_genre     STRING,
    popularity      FLOAT,
    danceability    FLOAT,
    energy          FLOAT,
    valence         FLOAT,
    tempo           FLOAT,
    acousticness    FLOAT,
    instrumentalness FLOAT,
    cluster         INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/music/processed/clustered';

-- Bảng genre stats
CREATE EXTERNAL TABLE IF NOT EXISTS music.genre_stats (
    track_genre      STRING,
    track_count      BIGINT,
    avg_popularity   FLOAT,
    avg_danceability FLOAT,
    avg_energy       FLOAT,
    avg_valence      FLOAT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/music/processed/genre_stats';

-- ================================================================
-- 3. ANALYTICAL QUERIES
-- ================================================================

-- Q1: Top 10 genre phổ biến nhất
SELECT track_genre,
       track_count,
       avg_popularity,
       avg_danceability,
       avg_energy
FROM music.genre_stats
ORDER BY avg_popularity DESC
LIMIT 10;

-- Q2: Phân bố tracks theo cluster
SELECT cluster,
       COUNT(*) as track_count,
       ROUND(AVG(popularity), 2) as avg_popularity,
       ROUND(AVG(danceability), 3) as avg_danceability,
       ROUND(AVG(energy), 3) as avg_energy,
       ROUND(AVG(valence), 3) as avg_valence,
       ROUND(AVG(tempo), 1) as avg_tempo
FROM music.tracks_clustered
GROUP BY cluster
ORDER BY cluster;

-- Q3: Top 20 tracks nổi tiếng nhất
SELECT track_name, artists, track_genre, popularity, cluster
FROM music.tracks_clustered
ORDER BY popularity DESC
LIMIT 20;

-- Q4: Genre nào có nhiều tracks explicit nhất
SELECT track_genre,
       COUNT(*) as total,
       SUM(CASE WHEN explicit = 'True' THEN 1 ELSE 0 END) as explicit_count,
       ROUND(SUM(CASE WHEN explicit = 'True' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as explicit_pct
FROM music.tracks_cleaned
GROUP BY track_genre
ORDER BY explicit_pct DESC
LIMIT 10;

-- Q5: Tracks danceability cao nhất theo từng cluster
SELECT cluster, track_name, artists, danceability, energy, valence
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cluster ORDER BY danceability DESC) as rn
    FROM music.tracks_clustered
) t
WHERE rn <= 5
ORDER BY cluster, danceability DESC;
