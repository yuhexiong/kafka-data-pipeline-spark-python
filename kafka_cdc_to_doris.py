from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, StructField, TimestampType

spark = SparkSession.builder \
    .appName("kafka_cdc_to_doris") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# read kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("subscribe", "source_topic") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# define kafka schema
value_schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", StringType()),
            StructField("device_name", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("manufacturer", StringType()),
            StructField("model", StringType()),
            StructField("description", StringType()),
            StructField("location", StringType()),
            StructField("battery_voltage", StringType())
        ]))
    ]))
])

# processed df
processed_df = df \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), value_schema).alias("data")) \
    .select("data.payload.after.*")

# write doris table
ds = processed_df \
    .writeStream \
    .format("doris") \
    .option("checkpointLocation", "./checkpoint/sink_table") \
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
