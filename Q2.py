from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
import read_receipt
import read_user

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read JSON with Schema") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def subtract_months(date_str, months):
    """Subtract a given number of months from a date string in 'YYYY-MM' format."""
    date_obj = datetime.strptime(date_str, "%Y-%m")
    new_date_obj = date_obj - relativedelta(months=months)
    return new_date_obj.strftime("%Y-%m")

# Path to the JSON files
json_file_path_receipt = "/Users/mengxunli/Downloads/Fetch_OA/receipts.json"
json_file_path_user = "/Users/mengxunli/Downloads/Fetch_OA/users.json"

# Read and transform receipt data
receipts_df = read_receipt.read_and_transform_json(json_file_path_receipt)

# Convert dateScanned to date type and find the latest month
latest_month = receipts_df.agg(max("dateScanned").alias("latest_date")).collect()[0]["latest_date"]
recent_month = subtract_months(latest_month, 1)

# Filter receipts for the most recent month
recent_month_df = receipts_df.filter(
    (receipts_df["dateScanned"] <= latest_month) & (receipts_df["dateScanned"] >= recent_month)
)

# Calculate top 5 brands by receipts scanned for the most recent month
top_5_recent = recent_month_df.groupBy("Brandname") \
    .agg(countDistinct('id').alias('Receipt_Scanned_Count')) \
    .orderBy('Receipt_Scanned_Count', ascending=False) \
    .limit(20)

top_5_recent.show(10, False)

print(f'What are the top 5 brands by receipts scanned for most recent month? \n \
      The top 5 brands are Thindust, Mueller, Flipbelt, Sargento, Miller Lite \n \
      Bradname is a new column I created to extract the brand name from brandcode, rewards group, description. This logic can be found in read_receipt.py \n \
       This is because many records do not have information that can be directly used or unique key that can be found in brands.json')
# Filter receipts for the previous month
prev_month = subtract_months(recent_month, 1)
prev_month_df = receipts_df.filter(
    (receipts_df["dateScanned"] < recent_month) & (receipts_df["dateScanned"] >= prev_month)
)

# Calculate top 5 brands by receipts scanned for the previous month
top_5_prev = prev_month_df.groupBy("Brandname") \
    .agg(countDistinct('id').alias('Receipt_Scanned_Count')) \
    .orderBy('Receipt_Scanned_Count', ascending=False) \
    .limit(20)

top_5_prev.show(10, False)

print(f'How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month? \n \
      For previous month, the top 5 brands are Ben AND JERRYS, HUGGIES, KLARBRUNN, PEPSI, FOLGERS')



# Read and transform user data
users_df = read_user.read_and_transform_json(json_file_path_user).distinct()

# Find the latest user creation date
user_latest_date = users_df.agg(max("createdDate").alias("create_latest_date")).collect()[0]["create_latest_date"]
six_months_ago = subtract_months(user_latest_date, 6)

# Filter users created within the past 6 months
six_month_users_df = users_df.filter(
    (users_df["createdDate"] < user_latest_date) & (users_df["createdDate"] >= six_months_ago)
)

# Join receipts with users created in the past 6 months
recent_users_receipts_df = receipts_df.join(six_month_users_df, on='userId', how='inner').distinct()

# Calculate the brand with the most spend among recent users
top_brand_spend = recent_users_receipts_df.groupBy("Brandname") \
    .agg(sum('finalPrice').alias('totalSpent')) \
    .orderBy('totalSpent', ascending=False) \
    .limit(10)

top_brand_spend.show(5, False)


'''
Which brand has the most spend among users who were created within the past 6 months?
Which brand has the most transactions among users who were created within the past 6 months?
'''

print(f'Which brand has the most spend among users who were created within the past 6 months? \n \
       HUGGIES has the most spend.')

# Calculate the brand with the most transactions among recent users
top_brand_transactions = recent_users_receipts_df.groupBy("Brandname") \
    .agg(countDistinct('id').alias('Transaction_Count')) \
    .orderBy('Transaction_Count', ascending=False) \
    .limit(10)

top_brand_transactions.show(5, False)

print(f'Which brand has the most transactions among users who were created within the past 6 months? \n \
      Thindust has the most transactions.')
# Stop the Spark session
spark.stop()
