from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, length
from pyspark.sql.types import StructType, StringType, StructField, DoubleType, TimestampType, ArrayType
import yaml

spark = SparkSession.builder \
    .appName("kafka_cdc_to_doris_with_yaml") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# setting yaml
with open("kafka_cdc_to_doris_setting.yaml", "r") as file:
    config = yaml.safe_load(file)
process = config['process']
source = config['source']
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



def process_schema_fields(fields):
    schema_fields = []
    for field in fields:
        if field['from_type'] == 'array' or field['from_type'] == 'list':
            sub_fields = field['items']
            sub_schema_fields = [StructField(sub_field['name'], get_spark_type(
                sub_field['from_type'])) for sub_field in sub_fields]
            schema_fields.append(StructField(
                field['name'], ArrayType(StructType(sub_schema_fields))))
        elif field['from_type'] == 'object':
            sub_fields = field.get('fields', [])
            sub_schema_fields = process_schema_fields(sub_fields)
            schema_fields.append(StructField(
                field['name'], StructType(sub_schema_fields)))
        else:
            schema_fields.append(StructField(
                field['name'], get_spark_type(field['from_type'])))
    return schema_fields


# define kafka value schema
fields = process['fields']
schema_fields = process_schema_fields(fields)
value_schema = StructType(schema_fields)


# define doris schema
def get_selected_column(name, alias, from_type, to_type):
    column = col(name)

    if to_type == 'timestamp':
        # use string length to determine whether it is milliseconds, then convert to long
        column = when(
            length(column.cast("string")) > 10,
            (column.cast("long") / 1000).cast(to_type)
        ).otherwise(column.cast(to_type))
    elif to_type is not None and to_type != "" and from_type != to_type:
        column = column.cast(get_spark_type(to_type))
    column = column.alias(alias)
    return column


def process_column_fields(field, parent_name=""):
    selected_columns = []
    # decide whether to add a prefix based on parent_name
    current_name = f"{parent_name}.{
        field['name']}" if parent_name else field['name']

    if field['from_type'] == 'array' or field['from_type'] == 'list':
        sub_fields = field['items']
        for sub_field in sub_fields:
            selected_columns.extend(
                process_column_fields(sub_field, current_name))
    elif field['from_type'] == 'object':
        sub_fields = field['fields']
        for sub_field in sub_fields:
            selected_columns.extend(
                process_column_fields(sub_field, current_name))
    else:
        column = get_selected_column(
            current_name, field['alias'], field['from_type'], field['to_type'])
        selected_columns.append(column)

    return selected_columns


# processed df
processed_df = df
for step in process['steps']:
    if step['operation'] == "select_expr":
        processed_df = processed_df.selectExpr(step['value'])
    elif step['operation'] == "from_json":
        processed_df = processed_df.select(
            from_json(col(step['from_value']), value_schema).alias(step['to_value']))
    elif step['operation'] == "select_column":
        processed_df = processed_df.select(step['value'])
    elif step['operation'] == "select_columns":
        selected_columns = []
        for field in fields:
            selected_columns.extend(
                process_column_fields(field, step['value']))
        processed_df = processed_df.select(*selected_columns)

# doris sink
ds = processed_df \
    .writeStream \
    .format("doris") \
    .option("checkpointLocation", f"./checkpoint/{sink['table']}") \
    .option("doris.table.identifier", sink['table']) \
    .option("doris.fenodes", sink['feNodes']) \
    .option("user", sink['user']) \
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
