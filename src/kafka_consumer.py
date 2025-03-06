from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from utils import spark_schema, validate_schema_udf, apply_corrections_udf, log_to_elasticsearch_udf
import os
from pathlib import Path
import requests

# Configurable Kafka settings
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "nyc_taxi_stream")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "nyc_taxi_dlq")

def create_spark_session():
    """Create and configure a Spark session."""
    # Get the absolute path to metrics.properties
    project_root = Path(__file__).parent.parent
    metrics_config_path = project_root / "config" / "metrics.properties"
    
    return SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.metrics.conf", str(metrics_config_path)) \
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

def process_stream():
    """Main function to process the Kafka stream."""
    try:
        if not check_kafka_health():
            raise Exception("Kafka is not available")
        if not check_elasticsearch_health():
            raise Exception("Elasticsearch is not available")

        spark = create_spark_session()

        # Read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", SOURCE_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse JSON and apply schema
        parsed_df = df.withColumn("value_str", col("value").cast("string")) \
            .withColumn("data", from_json(col("value_str"), spark_schema)) \
            .select("data.*")

        # Validate schema and apply corrections
        processed_df = parsed_df \
            .withColumn("is_valid", validate_schema_udf(col("value_str"))) \
            .withColumn("corrected_value", apply_corrections_udf(col("value_str"))) \
            .withColumn("log_status", log_to_elasticsearch_udf(col("value_str"), lit("processing_event")))

        # Split into valid and invalid records
        valid_records = processed_df.filter("is_valid = True")
        invalid_records = processed_df.filter("is_valid = False")

        # Write valid records to console (for testing)
        valid_query = valid_records.writeStream \
            .format("console") \
            .outputMode("append") \
            .start()

        # Write invalid records to DLQ topic
        dlq_query = invalid_records.selectExpr("CAST(corrected_value AS STRING) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("topic", DLQ_TOPIC) \
            .option("checkpointLocation", "/tmp/dlq_checkpoints") \
            .start()

        # Wait for termination
        valid_query.awaitTermination()
        dlq_query.awaitTermination()

    except Exception as e:
        print(f"❌ Error in process_stream: {str(e)}")
        raise

if __name__ == "__main__":
    process_stream()