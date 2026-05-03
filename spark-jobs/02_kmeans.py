from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder \
    .appName("Spotify KMeans") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "10g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "32") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/music/raw/spotify_tracks.csv")

df = df.drop(*[c for c in df.columns if c.startswith("_c")])

feature_cols = ["danceability", "energy", "loudness", "speechiness",
                "acousticness", "instrumentalness", "liveness", "valence", "tempo"]

for col in feature_cols:
    df = df.withColumn(col, F.col(col).cast("float"))

df = df.dropna(subset=feature_cols + ["track_id", "year"])
print(f"Loaded {df.count()} rows")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
df_assembled = assembler.transform(df)

scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                        withStd=True, withMean=True)
scaler_model = scaler.fit(df_assembled)
df_scaled = scaler_model.transform(df_assembled)

print("\n--- Elbow Method (k=3..8) ---")
evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
silhouette_scores = {}

for k in range(3, 9):
    kmeans = KMeans(featuresCol="features", k=k, seed=42, maxIter=20)
    model = kmeans.fit(df_scaled)
    predictions = model.transform(df_scaled)
    score = evaluator.evaluate(predictions)
    silhouette_scores[k] = score
    print(f"  k={k}: silhouette = {score:.4f}")

best_k = max(silhouette_scores, key=silhouette_scores.get)
print(f"\nBest k = {best_k}")

kmeans_final = KMeans(featuresCol="features", k=best_k, seed=42, maxIter=50)
model_final = kmeans_final.fit(df_scaled)
df_clustered = model_final.transform(df_scaled)

print("\n--- Cluster Analysis ---")
df_clustered.groupBy("prediction").agg(
    F.count("*").alias("track_count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability"),
    F.round(F.avg("energy"), 3).alias("avg_energy"),
    F.round(F.avg("valence"), 3).alias("avg_valence"),
    F.round(F.avg("tempo"), 1).alias("avg_tempo"),
    F.round(F.avg("acousticness"), 3).alias("avg_acousticness"),
).orderBy("prediction").show()

print("\n--- Cluster by Era ---")
df_clustered.groupBy("prediction", "decade").agg(
    F.count("*").alias("count"),
).orderBy("prediction", "decade").show(100)

df_result = df_clustered.select(
    "track_id", "track_name", "artists",
    "year", "decade",
    "popularity",
    "danceability", "energy", "valence", "tempo",
    "acousticness", "instrumentalness", "loudness",
    F.col("prediction").alias("cluster"),
)

df_result.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/music/processed/clustered")

df_clustered.groupBy("prediction").agg(
    F.count("*").alias("track_count"),
    F.round(F.avg("popularity"), 2).alias("avg_popularity"),
    F.round(F.avg("danceability"), 3).alias("avg_danceability"),
    F.round(F.avg("energy"), 3).alias("avg_energy"),
    F.round(F.avg("valence"), 3).alias("avg_valence"),
    F.round(F.avg("loudness"), 2).alias("avg_loudness"),
    F.round(F.avg("acousticness"), 3).alias("avg_acousticness"),
).write.mode("overwrite") \
 .parquet("hdfs://namenode:9000/music/processed/cluster_stats")

print(f"KMeans complete. Best k={best_k}")
spark.stop()
