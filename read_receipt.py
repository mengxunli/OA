# data_transformer.py

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, date_format, from_unixtime, arrays_zip, explode, coalesce

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Transformer") \
    .getOrCreate()

# Define the schema for the receipts JSON data
schema_receipt = StructType([
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),
    StructField("bonusPointsEarned", LongType(), True),
    StructField("bonusPointsEarnedReason", StringType(), True),
    StructField("createDate", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("dateScanned", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("finishedDate", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("modifyDate", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("pointsAwardedDate", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("pointsEarned", StringType(), True),
    StructField("purchaseDate", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("purchasedItemCount", LongType(), True),
    StructField("rewardsReceiptItemList", ArrayType(StructType([
        StructField("barcode", StringType(), True),
        StructField("brandCode", StringType(), True),
        StructField("rewardsGroup", StringType(), True),
        StructField("description", StringType(), True),
        StructField("finalPrice", StringType(), True),
        StructField("itemPrice", StringType(), True),
        StructField("needsFetchReview", BooleanType(), True),
        StructField("partnerItemId", StringType(), True),
        StructField("preventTargetGapPoints", BooleanType(), True),
        StructField("quantityPurchased", LongType(), True),
        StructField("userFlaggedDescription", StringType(), True),
        StructField("userFlaggedNewItem", BooleanType(), True),
        StructField("userFlaggedPrice", StringType(), True),
        StructField("userFlaggedQuantity", LongType(), True)
    ])), True),
    StructField("rewardsReceiptStatus", StringType(), True),
    StructField("totalSpent", StringType(), True),
    StructField("userId", StringType(), True)
])

def read_and_transform_json(json_file_path_receipt):
    # Read the JSON file with the specified schema
    df_receipts = spark.read.schema(schema_receipt).json(json_file_path_receipt)

    # Flatten the DataFrame and transform timestamp to date
    df_flattened_receipts = df_receipts.withColumn("id", col("_id.$oid")) \
        .withColumn("createDate", date_format(from_unixtime(col("createDate.$date") / 1000), "yyyy-MM")) \
        .withColumn("dateScanned", date_format(from_unixtime(col("dateScanned.$date") / 1000), "yyyy-MM")) \
        .withColumn("finishedDate", date_format(from_unixtime(col("finishedDate.$date") / 1000), "yyyy-MM")) \
        .withColumn("modifyDate", date_format(from_unixtime(col("modifyDate.$date") / 1000), "yyyy-MM")) \
        .withColumn("pointsAwardedDate", date_format(from_unixtime(col("pointsAwardedDate.$date") / 1000), "yyyy-MM")) \
        .withColumn("purchaseDate", date_format(from_unixtime(col("purchaseDate.$date") / 1000), "yyyy-MM"))
    
    # Further transformation
    tmp = df_flattened_receipts.withColumn("new", arrays_zip("rewardsReceiptItemList.brandCode", "rewardsReceiptItemList.rewardsGroup",
                                                             "rewardsReceiptItemList.description","rewardsReceiptItemList.userFlaggedDescription",
                                                             "rewardsReceiptItemList.finalPrice","rewardsReceiptItemList.barcode","rewardsReceiptItemList.partnerItemId")) \
        .withColumn("new", explode("new"))\
        .withColumn("Brandname", coalesce("new.brandCode", "new.rewardsGroup", "new.description","new.userFlaggedDescription"))\
        .withColumn("brandCode",col("new.brandCode"))\
        .withColumn("finalPrice",col("new.finalPrice"))\
        .withColumn("barcode",col("new.barcode"))\
        .withColumn("partnerItemId",col("new.partnerItemId"))\
        .select("partnerItemId",'finalPrice','dateScanned','id','Brandname','userId',"brandCode").distinct()
    
    return tmp
