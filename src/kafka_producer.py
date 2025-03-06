import json
import time
import random
import boto3
import csv
from confluent_kafka import Producer
from botocore.exceptions import ClientError
from typing import Dict, Any

# Kafka configuration
KAFKA_BROKER = "kafka:29092"
TOPIC = "nyc_taxi_stream"
S3_BUCKET = "nyc-taxi-dlq-data"
S3_KEY = "raw/yellow_tripdata_2015-01.csv"
LIMIT = 10000  # Max records to send
FAILURE_PROBABILITY = 0.01  # 1% failure rate
LOCAL_CSV_PATH = "./data/yellow_tripdata_2015-01.csv"

# Initialize Kafka producer
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'enable.idempotence': True,
    'acks': 'all',
    'retries': 3
})

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def introduce_failure(row: Dict[str, Any]) -> Dict[str, Any]:
    """Introduce failure in 1% of records."""
    if random.random() > FAILURE_PROBABILITY:
        return row  # 99% valid records

    failure_type = random.choice(["missing_field", "invalid_value"])
    
    if failure_type == "missing_field":
        row.pop(random.choice(list(row.keys())), None)
    elif failure_type == "invalid_value":
        field = random.choice(list(row.keys()))
        if isinstance(row[field], (int, float)):
            row[field] = -abs(row[field])
        else:
            row[field] = "invalid_value"
    
    return row

def infer_type(value: str) -> Any:
    """Infer the type of a CSV value."""
    if not value.strip():
        return None
    
    try:
        # Try integer first
        return int(value)
    except ValueError:
        try:
            # Try float if integer fails
            return float(value)
        except ValueError:
            # Return as string if all else fails
            return value.strip()

def convert_row_types(row: Dict[str, str]) -> Dict[str, Any]:
    """Convert CSV string values to inferred types."""
    return {k: infer_type(v) for k, v in row.items()}

def get_s3_object() -> list:
    """Fetch CSV data from S3."""
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        return obj['Body'].read().decode('utf-8').splitlines()
    except ClientError as e:
        print(f"Failed to fetch S3 object: {e}")
        raise
        
def get_local_csv_data() -> list:
    """Read CSV data from a local file."""
    try:
        with open(LOCAL_CSV_PATH, mode="r") as file:
            return list(csv.DictReader(file))
    except Exception as e:
        print(f"Failed to read local CSV file: {e}")
        raise

def produce_messages():
    count = 0
    # lines = get_s3_object()
    # reader = csv.DictReader(lines)
    reader = get_local_csv_data()
    
    for row in reader:
        if count >= LIMIT:
            break

        # Convert types and clean data
        converted_row = convert_row_types(row)
        corrupted_row = introduce_failure(converted_row)
        
        producer.produce(
            topic=TOPIC,
            value=json.dumps(corrupted_row),
            callback=delivery_report
        )
        
        if count % 1000 == 0:
            producer.flush()
        
        time.sleep(0.01)
        count += 1

    producer.flush()
    print(f"✅ Sent {count} records from s3://{S3_BUCKET}/{S3_KEY} to Kafka.")

if __name__ == "__main__":
    produce_messages()
