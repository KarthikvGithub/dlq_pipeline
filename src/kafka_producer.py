import json
import os
import time
import random
import logging
import boto3
import pyarrow.csv as pv
import pyarrow as pa
from confluent_kafka import Producer
from botocore.exceptions import ClientError
import pandas as pd
from utils import parse_aws_config
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "nyc_taxi_stream")
S3_BUCKET = os.getenv("S3_BUCKET", "dlq-pipeline-source-data")
S3_KEY = os.getenv("S3_KEY", "raw/yellow_tripdata_2016-03.csv")
LOCAL_CSV_PATH = os.getenv("LOCAL_CSV_PATH", "./data/yellow_tripdata_2015-01.csv")
MAX_RECORDS = 100000
FAILURE_RATE = 0.01
BATCH_SIZE = 1000

# Initialize Kafka producer with optimized settings
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'enable.idempotence': True,
    'acks': 'all',
    'retries': 5,
    'compression.type': 'lz4',
    'batch.num.messages': 10000,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.ms': 500
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def generate_corruption_plan(num_records: int) -> List[Dict]:
    """Pre-generate corruption patterns for batch processing."""
    return [{
        'corrupt': random.random() < FAILURE_RATE,
        'type': random.choice(["missing_field", "invalid_value"]),
        'field': random.choice([
            'VendorID', 'passenger_count', 'trip_distance', 
            'fare_amount', 'total_amount', 'payment_type'
        ])
    } for _ in range(num_records)]

def corrupt_record(record: Dict, plan: Dict) -> Dict:
    """Apply corruption based on pre-generated plan."""
    if not plan['corrupt']:
        return record
    
    if plan['type'] == "missing_field":
        record.pop(plan['field'], None)
    else:
        if isinstance(record.get(plan['field']), (int, float)):
            record[plan['field']] = -abs(record[plan['field']])
        else:
            record[plan['field']] = "invalid_value"
    
    return record

def load_data() -> pd.DataFrame:
    """Load data from S3 or local file with efficient parsing."""
    dtypes = {
        'VendorID': 'int8',
        'passenger_count': 'int8',
        'RateCodeID': 'int8',
        'payment_type': 'int8',
        'store_and_fwd_flag': 'category'
    }
    aws_credentials = parse_aws_config()
    try:
        # Optimized S3 loading with smart_open
        from smart_open import open
        s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
        
        # Use pyarrow for faster CSV parsing
        return pd.read_csv(
            open(s3_uri, 'rb', transport_params={
                'client': boto3.client('s3',
                    aws_access_key_id=aws_credentials['AWS_ACCESS_KEY_ID'],
                    aws_secret_access_key=aws_credentials['AWS_SECRET_ACCESS_KEY']
                )
            }),
            dtype=dtypes,
            # parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            low_memory=False,
            nrows=MAX_RECORDS  # Read only needed rows upfront
        )
    except Exception as s3_error:
        logger.warning(f"S3 load failed: {s3_error}, trying local")
        
        # Memory-mapped local file loading
        return pd.read_csv(
            LOCAL_CSV_PATH,
            dtype=dtypes,
            # parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
            memory_map=True,  # Use memory mapping for faster I/O
            low_memory=False,
            nrows=MAX_RECORDS
        )
    

def produce_messages():
    """Main production loop with batch processing."""
    # Load and prepare data

    print("Loading Data...")
    df = load_data().head(MAX_RECORDS)
    print("Loaded..")

    records = df.to_dict('records')
    corruption_plan = generate_corruption_plan(len(records))
    
    start_time = time.time()
    produced_count = 0
    
    try:
        for i, (record, plan) in enumerate(zip(records, corruption_plan)):
            # Apply corruption
            corrupted = corrupt_record(record, plan)
            
            # Serialize and produce
            producer.produce(
                topic=TOPIC,
                value=json.dumps(corrupted),
                callback=delivery_report
            )
            
            # Batch control
            if (i + 1) % BATCH_SIZE == 0:
                producer.poll(0.1)
                logger.info(f"Produced {i+1} records ({((i+1)/len(records))*100:.1f}%)")
                
            produced_count = i + 1
            
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user")
    finally:
        # Cleanup
        remaining = producer.flush(10)
        logger.info(f"Flushed {remaining} remaining messages")
        
        duration = time.time() - start_time
        logger.info(f"""
            Production summary:
            - Total records: {produced_count}
            - Duration: {duration:.2f} seconds
            - Throughput: {produced_count/duration:.2f} msg/sec
        """)

if __name__ == "__main__":
    produce_messages()