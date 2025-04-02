
# **Real-Time Ride-Sharing Analytics with Apache Spark**  

## **Overview**  
This project implements a **real-time analytics pipeline** for a ride-sharing platform using **Apache Spark Structured Streaming**. The system processes continuous ride-sharing data, performs aggregations, and identifies trends over time.  

The pipeline consists of three main tasks:  
- **Task 1:** Ingest and parse real-time ride data.  
- **Task 2:** Perform driver-level real-time aggregations.  
- **Task 3:** Analyze fare trends using a sliding time window.  

---

## **Prerequisites**  
Ensure you have the following installed:  
- **Apache Spark (>=3.0.0)**  
- **Python (>=3.7)**  
- **faker(used for generating fake data such as names, addresses, phone numbers, timestamps)**  

To install pyspark&faker (if not installed), use:  
```sh
pip install pyspark
pip install faker
```

---

## **Task 1: Streaming Data Ingestion and Parsing**  

### **Code Explanation**  
- **Initialize Spark Session:**  
  ```python
  spark = SparkSession.builder.appName("StructuredStreamingIngestion").getOrCreate()
  ```
  This initializes a Spark session for structured streaming.

- **Read data from a socket source:**  
  ```python
  streaming_input_df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
  ```
  This sets up Spark to ingest real-time data from a socket (localhost:9999).

- **Parse JSON input into structured columns:**  
  ```python
  json_df = streaming_input_df.select(from_json(col("value"), schema).alias("data"))
  parsed_df = json_df.select("data.trip_id", "data.driver_id", "data.distance_km", "data.fare_amount", "data.timestamp")
  ```
  The `from_json()` function extracts structured fields from incoming JSON messages.

- **Write streaming data to CSV files:**  
  ```python
  query = parsed_df.writeStream.outputMode("append").format("csv").option("path", "output/streaming_data").start()
  query.awaitTermination()
  ```
  This continuously writes parsed streaming data into CSV files in the `output/streaming_data/` directory.

### **Execution Commands**  
**1. Start the Netcat server:**  
```sh
python data_generator.py
```
**2. Run Task 1 script:**  
```sh
python task1.py
```

---

## **Task 2: Real-Time Aggregations (Driver-Level Earnings & Distance)**  

### **Code Explanation**  
- **Aggregate total earnings and average distance per driver:**  
  ```python
  parsed_stream = parsed_stream.withWatermark("event_time", "10 minutes")
  ```
  This adds a **watermark** to handle late data.

  ```python
  aggregated_df = parsed_stream.groupBy("driver_id") \
      .agg(sum("fare_amount").alias("total_fare"), avg("distance_km").alias("avg_distance"))
  ```
  This computes:
  - **Total fare earned per driver (`SUM(fare_amount)`)**  
  - **Average trip distance per driver (`AVG(distance_km)`)**  

- **Write the aggregated results to CSV:**  
  ```python
  query = aggregated_df.writeStream.outputMode("append").foreachBatch(write_to_csv).start()
  ```
  This writes the aggregated data in batches using a custom function.  

### **Execution Commands**  
**Run Task 2 script:**  
```sh
python task2.py
```

---

## **Task 3: Windowed Fare Amount Analysis**  

### **Code Explanation**  
- **Convert `timestamp` to a proper timestamp type:**  
  ```python
  parsed_df = parsed_df.withColumn("event_time", to_timestamp("timestamp"))
  ```
  This ensures Spark treats timestamps correctly.

- **Apply a sliding time window for fare aggregation:**  
  ```python
  aggregated_df = parsed_df.withWatermark("event_time", "10 minutes") \
      .groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
      .agg(sum("fare_amount").alias("total_fare"))
  ```
  This computes **total fare amount in 5-minute windows**, sliding every **1 minute**.

- **Save results to CSV in batches:**  
  ```python
  query = aggregated_df.writeStream.outputMode("append").foreachBatch(write_to_csv).start()
  ```

### **Execution Commands**  
**Run Task 3 script:**  
```sh
python task3.py
```

---

## **Output Files**  
The streaming results are stored as CSV files in:  
- **Task 1 Output:** `output/streaming_data/`  
- **Task 2 Output:** `output/aggregated/`  
- **Task 3 Output:** `output/window/`  

---

## **Conclusion**  
This project demonstrates how to use **Apache Spark Structured Streaming** for real-time data ingestion, aggregation, and trend analysis in a ride-sharing platform. 
