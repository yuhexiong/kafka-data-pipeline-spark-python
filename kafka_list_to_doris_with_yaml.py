from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, when, length
from pyspark.sql.types import StructType, StringType, StructField, ArrayType, DoubleType, TimestampType
import yaml

spark = SparkSession.builder \
    .appName("kafka_list_to_doris_with_yaml") \
    .config("spark.some.config.option", "config-value") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# setting yaml
with open("kafka_list_to_doris.yaml", "r") as file:
    config = yaml.safe_load(file)
source = config['source']
process = config['process']
sink = config['sink']

# read kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",  source['bootstrapServers']) \
    .option("subscribe", source['topic']) \
    .option("startingOffsets", "earliest") \
    .load()


def get_spark_type(type_str):
    if type_str == 'double':
        return DoubleType()
    elif type_str == 'timestamp':
        return TimestampType()
    else:
        return StringType()


fields = process['fields']
schema_fields = []
for field in fields:
    if field['from_type'] == 'array' or field['from_type'] == 'list':
        sub_fields = field['items']
        sub_fields = [StructField(sub_field['name'], get_spark_type(
            sub_field['from_type'])) for sub_field in sub_fields]
        schema_fields.append(StructField(
            field['name'], ArrayType(StructType(sub_fields))))
    else:
        schema_fields.append(StructField(
            field['name'], get_spark_type(field['from_type'])))

value_schema = StructType(schema_fields)

# define doris schema
selected_columns = []


def get_selected_column(name, alias, from_type, to_type):
    column = col(name)
    if to_type == 'timestamp':
        column = when(
            length(column.cast("string")) > 10,
            (column.cast("long") / 1000).cast(get_spark_type(to_type))
        ).otherwise(column.cast(get_spark_type(to_type)))
    elif to_type is not None and to_type != "" and from_type != to_type:
        column = column.cast(get_spark_type(to_type))

    column = column.alias(alias)
    return column


for field in fields:
    if field['from_type'] == 'array' or field['from_type'] == 'list':
        sub_fields = field['items']
        for sub_field in sub_fields:
            column = get_selected_column(f"{field['name']}.{
                                         sub_field['name']}", sub_field['alias'], sub_field['from_type'], sub_field['to_type'])
            selected_columns.append(column)
    else:
        column = get_selected_column(
            f"{field['name']}", field['alias'], field['from_type'], field['to_type'])
        selected_columns.append(column)

# processed df
processed_df = df
for step in process['steps']:
    if step['operation'] == "select_expr":
        processed_df = processed_df.selectExpr(step['value'])
    elif step['operation'] == "from_json":
        processed_df = processed_df.select(
            from_json(col("json_string"), value_schema).alias(step['value']))
    elif step['operation'] == "select_column":
        processed_df = processed_df.select(step['value'])
    elif step['operation'] == "explode_column":
        processed_df = processed_df.withColumn(
            step['value'], explode(col(step['value'])))
    elif step['operation'] == "select_columns":
        processed_df = processed_df.select(*selected_columns)

# write doris table
ds = processed_df \
    .writeStream \
    .format("doris") \
    .option("checkpointLocation", "./checkpoint") \
    .option("doris.table.identifier", sink['table']) \
    .option("doris.fenodes", sink['feNodes']) \
    .option("user", sink['username']) \
    .option("password", sink['password']) \
    .start()

# print log
console_output = processed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

ds.awaitTermination()
console_output.awaitTermination()
