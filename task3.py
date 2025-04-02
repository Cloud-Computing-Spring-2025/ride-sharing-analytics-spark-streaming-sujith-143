from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


# Create Spark Session
spark = SparkSession.builder \
    .appName("RideSharingAnalytics") \
    .getOrCreate()

# Define the streaming DataFrame from a socket source
streaming_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data (assuming each line is a JSON string)
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

parsed_df = streaming_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Convert timestamp column to a proper timestamp type
from pyspark.sql.functions import to_timestamp

parsed_df = parsed_df.withColumn("event_time", to_timestamp("timestamp"))

# Apply watermark and aggregate fare amounts in 5-minute windows
aggregated_df = parsed_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "5 minutes")) \
    .agg(sum("fare_amount").alias("total_fare"))

def write_to_csv(batch_df, batch_id):
    file_path = f"output/window/taxi_data_batch_{batch_id}.csv"
    batch_df.toPandas().to_csv(file_path, mode="w", index=False)
    print(f"Saved batch {batch_id} to {file_path}")


query = aggregated_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .foreachBatch(write_to_csv) \
    .option("path", "output_csv") \
    .option("header", "true") \
    .start()

query.awaitTermination()