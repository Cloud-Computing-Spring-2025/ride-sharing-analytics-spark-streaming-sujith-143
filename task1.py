from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StructuredStreamingIngestion") \
    .getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", FloatType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Ingest streaming data from the socket (localhost:9999)
streaming_input_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# The input data from the socket is a string, so we need to parse it as JSON
json_df = streaming_input_df.select(from_json(col("value"), schema).alias("data"))

# Extract the fields from the parsed JSON
parsed_df = json_df.select("data.trip_id", "data.driver_id", "data.distance_km", "data.fare_amount", "data.timestamp")

# Store the parsed streaming data into CSV format in the "output" folder
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/streaming_data") \
    .option("checkpointLocation", "output/checkpoints") \
    .option("header", True) \
    .start()

# Wait for the termination of the stream
query.awaitTermination()
