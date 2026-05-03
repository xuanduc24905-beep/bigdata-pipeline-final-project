from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Export") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

for name in ["cleaned", "clustered", "decade_stats", "cluster_stats"]:
    df = spark.read.parquet(f"hdfs://namenode:9000/music/processed/{name}")
    df.toPandas().to_parquet(f"/data/{name}.parquet", index=False)
    print(f"Exported {name}: {df.count()} rows")

spark.stop()
