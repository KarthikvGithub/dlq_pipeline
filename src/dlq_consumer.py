from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import spark_schema, apply_corrections, get_validation_condition, batch_log_to_elasticsearch
import os
import time

# Configurable Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "nyc_taxi_dlq")
STREAM_TOPIC = os.getenv("STREAM_TOPIC", "nyc_taxi_stream")
FAILED_TOPIC = os.getenv("FAILED_TOPIC", "nyc_taxi_failed")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

def create_spark_session():
    """Create and configure a Spark session with DLQ optimizations"""
    return SparkSession.builder \
        .appName("DLQConsumer") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/dlq_checkpoints") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .getOrCreate()

def process_dlq_stream():
    """Process DLQ stream with native Spark operations"""
    spark = create_spark_session()
    
    # Read from DLQ topic
    dlq_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", DLQ_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse and enhance DLQ records
    parsed_stream = dlq_stream.select(
        F.from_json(F.col("value").cast("string"), spark_schema).alias("data")
    ).select("data.*")

    # Apply data corrections and increment retry count
    processed_stream = apply_corrections(parsed_stream) \
        .withColumn("retry_count", F.coalesce(F.col("retry_count"), F.lit(0)) + 1)

    # Validate using shared condition
    validation_condition = get_validation_condition()
    validated_stream = processed_stream.withColumn("is_valid", validation_condition)

    # Split into retryable and failed streams
    retryable = validated_stream.filter(
        F.col("is_valid") & (F.col("retry_count") <= MAX_RETRIES)
    )
    failed = validated_stream.filter(
        ~F.col("is_valid") | (F.col("retry_count") > MAX_RETRIES)
    )

    # Prepare records for reprocessing
    retry_output = retryable.select(
        F.to_json(F.struct("*")).alias("value")
    )
    
    failed_output = failed.select(
        F.to_json(F.struct("*")).alias("value")
    )

    # Write streams with batch logging
    retry_query = retry_output.writeStream \
        .foreachBatch(lambda df, batch_id: batch_log_to_elasticsearch(df, batch_id)) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", STREAM_TOPIC) \
        .option("checkpointLocation", "/tmp/dlq_retry_checkpoints") \
        .start()

    failed_query = failed_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", FAILED_TOPIC) \
        .option("checkpointLocation", "/tmp/dlq_failed_checkpoints") \
        .start()

    # Monitor processing
    while True:
        print("=== DLQ Processing Metrics ===")
        for q in spark.streams.active:
            print(f"Stream {q.name}: {q.status}")
        time.sleep(60)

    retry_query.awaitTermination()
    failed_query.awaitTermination()

if __name__ == "__main__":
    process_dlq_stream()