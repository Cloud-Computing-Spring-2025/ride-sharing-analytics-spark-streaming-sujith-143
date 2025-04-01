from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("StreamingTaxiData").getOrCreate()

# Define schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse JSON data
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp column to TimestampType
parsed_stream = parsed_stream.withColumn("event_time", col("timestamp").cast(TimestampType()))

# Add Watermark for Aggregations
parsed_stream = parsed_stream.withWatermark("event_time", "10 minutes")

# Define a function to process each micro-batch
def write_to_csv(batch_df, batch_id):
    file_path = f"output/aggregated/taxi_data_batch_{batch_id}.csv"
    batch_df.toPandas().to_csv(file_path, mode="w", index=False)
    print(f"Saved batch {batch_id} to {file_path}")

# Write the stream using `foreachBatch`
query = parsed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_csv) \
    .option("path", "output/aggregated/") \
    .start()

query.awaitTermination()