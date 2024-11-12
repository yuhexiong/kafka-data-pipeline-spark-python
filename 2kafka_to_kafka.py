from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StringType, StructField, ArrayType, TimestampType, DoubleType

spark = SparkSession.builder \
    .appName("2kafka_to_kafka") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# read kafka topic
df1 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("subscribe", "source_topic1") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# define kafka schema
schema_df1 = StructType([
    StructField("payload", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("device_name", StringType()),
        StructField("manufacturer", StringType()),
        StructField("model", StringType()),
        StructField("description", StringType()),
        StructField("location", StringType()),
        StructField("battery_voltage", StringType())
    ])))
])

# processed df
df1_parsed = df1 \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema_df1).alias("data")) \
    .select("data.payload") \
    .withColumn("payload", explode(col("payload"))) \
    .select(
        col("payload.device_id").alias("id"),
        col("payload.device_name").alias("device_name"),
        col("payload.manufacturer").alias("manufacturer"),
        col("payload.model").alias("model"),
        col("payload.description").alias("description"),
        col("payload.location").alias("location"),
        col("payload.battery_voltage").cast(
            DoubleType()).alias("battery_voltage")
    )

console_output1 = df1_parsed \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# read kafka topic
df2 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("subscribe", "source_topic2") \
    .option("startingOffsets", "earliest") \
    .load()

# define kafka schema
schema_df2 = StructType([
    StructField("device_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", TimestampType()),
])

# processed df
df2_parsed = df2 \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema_df2).alias("json_data")) \
    .select(col("json_data.device_id").alias("device_id"),
            col("json_data.timestamp").alias("timestamp"),
            col("json_data.status").alias("status"))

console_output2 = df2_parsed \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# join df
joined_df = df1_parsed.join(df2_parsed, df1_parsed.id == df2_parsed.device_id)
joined_df_with_key_value = joined_df \
    .selectExpr("name AS key", "to_json(struct(*)) AS value")

# write kafka
ds = joined_df_with_key_value \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("topic", "sink_topic") \
    .option("checkpointLocation", "./checkpoint/sink_topic") \
    .start()

console_output3 = joined_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# print log
ds.awaitTermination()
console_output1.awaitTermination()
console_output2.awaitTermination()
console_output3.awaitTermination()
