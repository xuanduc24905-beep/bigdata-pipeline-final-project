from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Spotify Kafka Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.cores", "8") \
    .config("spark.sql.shuffle.partitions", "32") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("track_id",         StringType(),  True),
    StructField("track_name",       StringType(),  True),
    StructField("artists",          StringType(),  True),
    StructField("album_name",       StringType(),  True),
    StructField("track_genre",      StringType(),  True),
    StructField("release_date",     StringType(),  True),
    StructField("year",             FloatType(),   True),
    StructField("decade",           StringType(),  True),
    StructField("acousticness",     FloatType(),   True),
    StructField("danceability",     FloatType(),   True),
    StructField("duration_ms",      FloatType(),   True),
    StructField("energy",           FloatType(),   True),
    StructField("explicit",         StringType(),  True),
    StructField("instrumentalness", FloatType(),   True),
    StructField("key",              FloatType(),   True),
    StructField("liveness",         FloatType(),   True),
    StructField("loudness",         FloatType(),   True),
    StructField("mode",             FloatType(),   True),
    StructField("popularity",       FloatType(),   True),
    StructField("speechiness",      FloatType(),   True),
    StructField("tempo",            FloatType(),   True),
    StructField("valence",          FloatType(),   True),
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "music-stream") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_kafka.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data"),
    F.col("timestamp").alias("kafka_timestamp"),
).select("data.*", "kafka_timestamp")

df_processed = df_parsed \
    .withColumn("popularity_tier",
        F.when(F.col("popularity") >= 80, "Viral")
         .when(F.col("popularity") >= 60, "Popular")
         .when(F.col("popularity") >= 40, "Average")
         .otherwise("Low")
    ) \
    .withColumn("energy_level",
        F.when(F.col("energy") >= 0.7, "High")
         .when(F.col("energy") >= 0.4, "Medium")
         .otherwise("Low")
    ) \
    .withColumn("is_danceable",
        F.when(F.col("danceability") >= 0.7, True).otherwise(False)
    ) \
    .withColumn("era",
        F.when(F.col("year") < 1950, "Pre-50s")
         .when(F.col("year") < 1970, "50s-60s")
         .when(F.col("year") < 1980, "70s")
         .when(F.col("year") < 1990, "80s")
         .when(F.col("year") < 2000, "90s")
         .when(F.col("year") < 2010, "2000s")
         .when(F.col("year") < 2020, "2010s")
         .otherwise("2020s")
    ) \
    .withColumn("ingestion_time", F.current_timestamp()) \
    .dropna(subset=["track_id", "track_name"])

query_hdfs = df_processed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/music/streaming/tracks") \
    .option("checkpointLocation", "hdfs://namenode:9000/music/streaming/checkpoint") \
    .outputMode("append") \
    .trigger(processingTime="1 seconds") \
    .start()

query_console = df_processed \
    .groupBy("decade", "popularity_tier") \
    .agg(
        F.count("*").alias("count"),
        F.round(F.avg("popularity"), 2).alias("avg_popularity"),
        F.round(F.avg("energy"), 3).alias("avg_energy"),
    ) \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime="5 seconds") \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping streaming...")
finally:
    query_hdfs.stop()
    query_console.stop()
    spark.stop()
    print("Streaming stopped")
