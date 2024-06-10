from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, date_format, from_unixtime, arrays_zip, explode, coalesce

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Transformer") \
    .getOrCreate()

schema_user = StructType([
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),
    StructField("active", BooleanType(), True),
    StructField("createdDate", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("lastLogin", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("role", StringType(), True),
    StructField("signUpSource", StringType(), True),
    StructField("state", StringType(), True)
])



def read_and_transform_json(json_file_path_receipt):
    # Read the JSON file with the specified schema
    df_users = spark.read.schema(schema_user).json(json_file_path_receipt)

    df_flattened_users = df_users.withColumn("userId", col("_id.$oid")) \
    .withColumn("createdDate",date_format(from_unixtime(col("createdDate.$date") / 1000), "yyyy-MM")) \
    .withColumn("lastLogin", date_format(from_unixtime(col("lastLogin.$date") / 1000), "yyyy-MM"))

    return df_flattened_users
