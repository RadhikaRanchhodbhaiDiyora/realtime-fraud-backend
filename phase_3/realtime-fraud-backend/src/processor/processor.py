import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
import time
import redis
import json
from datetime import datetime

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
RAW_TOPIC = 'raw_transactions'
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = 6379

# Define the schema of the JSON message value
schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("amt", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("city", StringType(), True),
    StructField("unix_time", LongType(), True),
    StructField("is_fraud", IntegerType(), True), # The target class for aggregation
    StructField("cc_num_hash", StringType(), True), 
    StructField("trans_num_hash", StringType(), True) 
])

# Global Redis connection (will be instantiated in start_spark_streaming)
r = None

def write_to_redis(df, epoch_id):
    """Writes the aggregated DataFrame batch to Redis."""
    global r
    
    # Initialize connection if not done already (Spark runs this function on the worker nodes)
    if r is None:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
            r.ping()
        except Exception as e:
            print(f"Redis Connection Error: {e}")
            return

    if df.count() > 0:
        # Convert DataFrame to a list of dicts
        records = df.toJSON().collect()
        
        # Use a pipeline for efficient batch writing to Redis
        pipe = r.pipeline()
        for record_json in records:
            record = json.loads(record_json)
            
            # Create a unique key using the window end time, merchant, and city
            # This is the "Data Delivery" mechanism.
            window_end_iso = record['window']['end']
            key_parts = [
                'metrics',
                window_end_iso.split('T')[-1].split('.')[0].replace(':', '-'), # Time part
                record['merchant'], 
                record['city']
            ]
            redis_key = ':'.join(key_parts)
            
            # Store the aggregated data
            pipe.set(redis_key, json.dumps(record))
        
        pipe.execute()
        print(f"Processor Service: Epoch {epoch_id}: Wrote {len(records)} aggregated records to Redis.")

def start_spark_streaming():
    print("Processor Service: Starting Spark Structured Streaming...")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("RealTimeFraudProcessor") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR") # Reduce noisy log output
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    
    # 1. Read Stream from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", RAW_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Deserialize JSON data and create an event timestamp column
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Create the timestamp column (from milliseconds to seconds) for windowing
    df_with_ts = df_parsed.withColumn("event_timestamp", (col("unix_time")).cast("timestamp"))
    
    # 3. Apply Windowed Aggregation (Core Logic: 5-minute Tumbling Window)
    windowed_metrics = df_with_ts \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes"), # Defines the aggregation window
            col("merchant"),
            col("city")
        ) \
        .agg(
            count("trans_num_hash").alias("transaction_count"),
            sum("amt").alias("total_amount"),
            sum(col("is_fraud")).alias("fraud_count") # Total number of fraud events in the window
        ) \
        .filter(col("transaction_count") > 0) # Only keep windows with data

    # 4. Write Stream to Sink (Console and Redis)
    
    # Console output for quick verification
    query_console = windowed_metrics.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Redis output (using foreachBatch for custom sink logic)
    query_redis = windowed_metrics.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_redis) \
        .option("checkpointLocation", "/tmp/checkpoints/fraud_metrics_redis") \
        .start()

    # Wait for the termination of the query
    query_console.awaitTermination()
    query_redis.awaitTermination()

if __name__ == '__main__':
    # Give Kafka and the producer a moment to fill up the topic
    time.sleep(20) 
    start_spark_streaming()