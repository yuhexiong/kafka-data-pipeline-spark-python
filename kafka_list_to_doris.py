from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, ArrayType, DoubleType

spark = SparkSession.builder \
    .appName("kafka_list_to_doris") \
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
value_schema = StructType([
    StructField("payload", ArrayType(StructType([
        StructField("device_id", StringType()),
        StructField("device_name", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("manufacturer", StringType()),
        StructField("model", StringType()),
        StructField("description", StringType()),
        StructField("location", StringType()),
        StructField("battery_voltage", StringType())
    ])))
])

# processed df
processed_df = df \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), value_schema).alias("data")) \
    .select("data.payload") \
    .withColumn("payload", explode(col("payload"))) \
    .select(
        col("payload.device_id").alias("id"),
        col("payload.device_name").alias("device_name"),
        col("payload.timestamp").alias("timestamp"),
        col("payload.manufacturer").alias("manufacturer"),
        col("payload.model").alias("model"),
        col("payload.description").alias("description"),
        col("payload.location").alias("location"),
        col("payload.battery_voltage").cast(
            DoubleType()).alias("battery_voltage")
    )

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
