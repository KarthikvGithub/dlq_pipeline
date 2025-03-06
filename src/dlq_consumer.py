from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json
from utils import spark_schema, validate_schema_udf, apply_corrections_udf, log_to_elasticsearch_udf
import os
import requests

# Configurable Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "nyc_taxi_dlq")
STREAM_TOPIC = os.getenv("STREAM_TOPIC", "nyc_taxi_stream")
FAILED_TOPIC = os.getenv("FAILED_TOPIC", "nyc_taxi_failed")
MAX_RETRIES = 3

def create_spark_session():
    """Create and configure a Spark session."""
    return SparkSession.builder \
        .appName("DLQConsumer") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/dlq_checkpoints") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

def check_kafka_health():
    """Verify Kafka is available."""
    try:
        response = requests.get(f"http://{KAFKA_BROKER}/health")
        return response.status_code == 200
    except Exception:
        return False

def check_elasticsearch_health():
    """Verify Elasticsearch is available."""
    try:
        response = requests.get(f"{os.getenv('ELASTICSEARCH_ENDPOINT', 'http://elasticsearch:9200')}/_cluster/health")
        return response.status_code == 200
    except Exception:
        return False

def process_dlq_stream():
    """Process the DLQ stream using Spark Structured Streaming."""
    try:
        if not check_kafka_health():
            raise Exception("Kafka is not available")
        if not check_elasticsearch_health():
            raise Exception("Elasticsearch is not available")

        spark = create_spark_session()

        # Read from DLQ Kafka topic
        dlq_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", DLQ_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse JSON and apply schema
        parsed_stream = dlq_stream.withColumn("value_str", col("value").cast("string")) \
            .withColumn("data", from_json(col("value_str"), spark_schema)) \
            .select("data.*", "value_str")

        # Add retry count and apply corrections
        processed_stream = parsed_stream \
            .withColumn("retry_count", col("retry_count").cast("integer")) \
            .withColumn("corrected_value", apply_corrections_udf(col("value_str"))) \
            .withColumn("is_valid", validate_schema_udf(col("corrected_value"))) \
            .withColumn("log_status", log_to_elasticsearch_udf(col("value_str"), lit("dlq_processing_event")))

        # Split into retryable and failed records
        retryable_records = processed_stream.filter(
            (col("is_valid") == True) & (col("retry_count") <= MAX_RETRIES)
        )
        failed_records = processed_stream.filter(
            (col("is_valid") == False) | (col("retry_count") > MAX_RETRIES)
        )

        # Write retryable records back to the main stream
        retry_query = retryable_records.selectExpr(
            "CAST(corrected_value AS STRING) AS value"
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", STREAM_TOPIC) \
            .option("checkpointLocation", "/tmp/dlq_retry_checkpoints") \
            .start()

        # Write failed records to the failed topic
        failed_query = failed_records.selectExpr(
            "CAST(corrected_value AS STRING) AS value"
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", FAILED_TOPIC) \
            .option("checkpointLocation", "/tmp/dlq_failed_checkpoints") \
            .start()

        # Wait for termination
        retry_query.awaitTermination()
        failed_query.awaitTermination()

    except Exception as e:
        print(f"❌ Error in process_dlq_stream: {str(e)}")
        raise

if __name__ == "__main__":
    process_dlq_stream()