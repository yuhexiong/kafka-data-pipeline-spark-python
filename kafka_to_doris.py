from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("kafka_to_doris") \
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

# define kafka schema
value_schema = StructType() \
    .add("device_id", StringType()) \
    .add("device_name", StringType()) \
    .add("manufacturer", StringType()) \
    .add("model", StringType()) \
    .add("description", StringType()) \
    .add("location", StringType())

# processed df
processed_df = df \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), value_schema).alias("data")) \
    .select("data.*") 

# write doris table
ds = processed_df \
        .writeStream \
        .format("doris") \
        .option("checkpointLocation", "./checkpoint") \
        .option("doris.table.identifier", "database.sink_table") \
        .option("doris.fenodes", "host:8030") \
        .option("user", "root") \
        .option("password", "password") \
        .start()

# print log
console_output = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

ds.awaitTermination()
console_output.awaitTermination()
