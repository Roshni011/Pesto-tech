from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AdvertiseX Data Processing") \
    .getOrCreate()

# Define schema for ad impressions JSON data
impressions_schema = StructType([
    StructField("ad_creative_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("website", StringType(), True)
])

# Read ad impressions JSON data
impressions_df = spark.read.json("path_to_impressions.json", schema=impressions_schema)

# Define schema for clicks/conversions CSV data
clicks_schema = StructType([
    StructField("event_timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("ad_campaign_id", StringType(), True),
    StructField("conversion_type", StringType(), True)
])

# Read clicks/conversions CSV data
clicks_df = spark.read.csv("path_to_clicks.csv", schema=clicks_schema, header=True)

# Define schema for bid requests Avro data
bid_requests_schema = StructType([
    StructField("user_info", StringType(), True),
    StructField("auction_details", StringType(), True),
    StructField("ad_targeting_criteria", StringType(), True)
])

# Read bid requests Avro data
bid_requests_df = spark.read.format("avro").load("path_to_bid_requests.avro")

# Join ad impressions with clicks and conversions
joined_df = impressions_df.join(clicks_df, "user_id", "left_outer")

# Define storage location for processed data
processed_data_path = "path_to_processed_data"

# Write processed data to storage
joined_df.write.mode("overwrite").parquet(processed_data_path)

# Stop Spark session
spark.stop()
