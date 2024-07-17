from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("kafka_to_kafka") \
    .config("spark.some.config.option", "config-value") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN") 

# read kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("subscribe", "source_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# processed df
processed_df = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("key", col("key")) \
    .withColumn("value", col("value"))

# write kafka topic
ds = processed_df \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("topic", "sink_topic") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

# print log
console_output = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

ds.awaitTermination()
console_output.awaitTermination()
