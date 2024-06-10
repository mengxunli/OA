from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import read_user, read_receipt

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Quality Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Path to the JSON files
json_file_path_user = "/Users/mengxunli/Downloads/Fetch_OA/users.json"
json_file_path_receipt = "/Users/mengxunli/Downloads/Fetch_OA/receipts.json"

# Read and transform user data
df_flattened_users = read_user.read_and_transform_json(json_file_path_user)

# Data Quality Issue 1: Duplicate records in users.json
total_user_count = df_flattened_users.count()
distinct_user_count = df_flattened_users.distinct().count()
duplicate_user_count = total_user_count - distinct_user_count

print(f"User Count: {total_user_count}, User Distinct Count: {distinct_user_count}")
print(f"Data Quality Issue 1: There are {duplicate_user_count} duplicate records in users.json, which is about {duplicate_user_count / total_user_count * 100:.2f}%")

# Read and transform receipt data
df_flattened_receipts = read_receipt.read_and_transform_json(json_file_path_receipt)

# Data Quality Issue 2: Missing brandCode and rewards group
missing_brandcode_count = df_flattened_receipts.filter("brandCode is null").count()
total_receipts_count = df_flattened_receipts.count()
missing_brandcode_percentage = missing_brandcode_count / total_receipts_count * 100

df_flattened_receipts.filter("brandCode is null").select("id","Brandname","brandCode").show(10, False)
print(f"Data Quality Issue 2: Brandcode missing. {missing_brandcode_percentage:.2f}% of receipts are missing brandCode")

# Data Quality Issue 3: Comparison of data volume and fill rate over time
df_flattened_receipts.groupBy("dateScanned") \
    .agg(countDistinct('id').alias('Receipts_Scanned_Count')) \
    .orderBy("dateScanned", ascending=False) \
    .show(20, False)

print(f"Data Quality Issue 3: Comparing earlier months, the recent month has much less data and a lower fill rate of brandCode.")

# Data Quality Issue 4: Rejected receipts with same information scanned by the same user
df_flattened_receipts.groupBy("userId", "Brandname", "partnerItemId", "dateScanned") \
    .agg(countDistinct('id').alias('ct')) \
    .orderBy('ct', ascending=False) \
    .show(20)

print(f"Data Quality Issue 4: Many receipts have the same information and are scanned by the same user. Categorize rejection reasons to better filter out duplicate receipts.")
