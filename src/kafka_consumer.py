from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import spark_schema, apply_corrections, batch_log_to_elasticsearch, get_validation_condition
import os
import time
from pathlib import Path
import requests

# Configurable Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "nyc_taxi_stream")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "nyc_taxi_dlq")
CHECKPOINT_LOCATION = "/tmp/checkpoints"
DLQ_CHECKPOINT_LOCATION = "/tmp/dlq_checkpoints"

def create_spark_session():
    """Create and configure a Spark session with monitoring"""
    return SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.executor.extraJavaOptions", "-Dcom.sun.management.jmxremote") \
        .getOrCreate()

def process_stream():
    """Main streaming processing function"""
    spark = create_spark_session()
    
    # Read from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", SOURCE_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON with schema
    parsed_df = kafka_stream.select(
        F.from_json(F.col("value").cast("string"), spark_schema).alias("data")
        .select("data.*")
    )

    # Apply data corrections
    corrected_df = apply_corrections(parsed_df)

    # Add validation flag
    validation_condition = get_validation_condition()
    processed_df = corrected_df.withColumn("is_valid", validation_condition)

    # Split stream
    valid_records = processed_df.filter(F.col("is_valid"))
    invalid_records = processed_df.filter(~F.col("is_valid"))

    # Process valid records
    valid_query = valid_records.writeStream \
        .foreachBatch(batch_log_to_elasticsearch) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    # Process invalid records (DLQ)
    dlq_query = invalid_records.select(
        F.to_json(F.struct("*")).alias("value")
    ).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", DLQ_TOPIC) \
        .option("checkpointLocation", DLQ_CHECKPOINT_LOCATION) \
        .start()

    # Monitor streams
    while True:
        for q in spark.streams.active:
            print(f"Query {q.name}: Status {q.status}, Progress {q.recentProgress}")
        time.sleep(60)

    valid_query.awaitTermination()
    dlq_query.awaitTermination()

if __name__ == "__main__":
    process_stream()