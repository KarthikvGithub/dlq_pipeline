import json
import logging
import logging.config
import os
import time
import boto3
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType, StringType
from pathlib import Path
from botocore.exceptions import ClientError
from typing import Optional
from datetime import datetime

# Load schema from config file
SCHEMA_PATH = Path(__file__).parent.parent / "config" / "schema.json"
with open(SCHEMA_PATH, "r") as f:
    SCHEMA = json.load(f)

# Load logging configuration from config file
LOGGING_CONFIG_PATH = Path(__file__).parent.parent / "config" / "logging.json"
with open(LOGGING_CONFIG_PATH, "r") as f:
    logging.config.dictConfig(json.load(f))

logger = logging.getLogger(__name__)

# Define schema for Spark DataFrame
spark_schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", FloatType()),
    StructField("pickup_longitude", FloatType()),
    StructField("pickup_latitude", FloatType()),
    StructField("RateCodeID", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("dropoff_longitude", FloatType()),
    StructField("dropoff_latitude", FloatType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", FloatType()),
    StructField("extra", FloatType()),
    StructField("mta_tax", FloatType()),
    StructField("tip_amount", FloatType()),
    StructField("tolls_amount", FloatType()),
    StructField("improvement_surcharge", FloatType()),
    StructField("total_amount", FloatType()),
    StructField("retry_count", IntegerType())
])

# def validate_schema(df):
#     """Native Spark schema validation using DataFrame operations"""
#     return df.filter(
#         F.col("VendorID").isNotNull() &
#         F.col("tpep_pickup_datetime").isNotNull() &
#         F.col("tpep_dropoff_datetime").isNotNull() &
#         F.col("passenger_count").between(0, 9) &
#         F.col("fare_amount").geq(0) &
#         F.col("total_amount").geq(0) &
#         F.col("RateCodeID").isin([1, 2, 3, 4, 5, 6]) &
#         F.col("store_and_fwd_flag").isin(["Y", "N"])
#     )

def parse_aws_config(filepath="/run/secrets/aws_config"):
    aws_config = {}
    with open(filepath, "r") as f:
        for line in f:
            key, value = line.strip().split("=", 1)
            aws_config[key] = value
    return aws_config

def get_validation_condition():
    """Native Spark validation conditions"""
    return (
        (F.col("VendorID").isNotNull()) 
        & (F.col("tpep_pickup_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime").isNotNull())
        & (F.col("passenger_count").between(0, 9))
        & ((F.col("fare_amount") >= 0))
        & ((F.col("total_amount") >= 0))
        & (F.col("RateCodeID").isin([1, 2, 3, 4, 5, 6]))
        & (F.col("store_and_fwd_flag").isin(["Y", "N"]))
    )


def apply_corrections(df):
    """Native Spark data transformations"""
    numeric_fields = [
        "VendorID", "passenger_count", "trip_distance",
        "pickup_longitude", "pickup_latitude", "RateCodeID",
        "dropoff_longitude", "dropoff_latitude", "payment_type",
        "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount"
    ]
    
    # Clean numeric fields
    for field in numeric_fields:
        df = df.withColumn(field, 
            F.regexp_replace(F.col(field).cast("string"), "[^0-9.-]", "")
            .cast(FloatType())
        )
        
    # Special handling for integer fields
    for field in ["VendorID", "passenger_count", "RateCodeID", "payment_type"]:
        df = df.withColumn(field, F.coalesce(F.col(field).cast(IntegerType()), F.lit(0)))
    
    # Absolute value for fare amount
    df = df.withColumn("fare_amount", F.abs(F.col("fare_amount")))
    
    # Generate UUID
    return df.withColumn("uuid", F.expr("uuid()"))

def batch_log_to_elasticsearch(df, batch_id):
    """Batch process records for Elasticsearch using foreachBatch"""
    es_endpoint = os.getenv('ELASTICSEARCH_ENDPOINT', 'http://elasticsearch:9200')
    headers = {"Content-Type": "application/x-ndjson"}
    
    # Convert DataFrame to JSON records
    records = [row.asDict() for row in df.collect()]
    
    # Prepare bulk request
    bulk_data = []
    for record in records:
        bulk_data.append(json.dumps({"index": {}}))
        bulk_data.append(json.dumps({
            "@timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "message": "DLQ processing event",
            "data": {
                "event": "stream_processing",
                "record": record,
                "timestamp": int(time.time() * 1000)
            }
        }))
    
    # Send bulk request
    try:
        response = requests.post(
            f"{es_endpoint}/_bulk",
            data="\n".join(bulk_data),
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {str(e)}")

def upload_to_s3(file_path: str, bucket: str, s3_key: str) -> bool:
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    try:
        s3.upload_file(file_path, bucket, s3_key)
        logger.info(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"S3 upload error: {e}")
        return False

def get_s3_object(bucket: str, key: str) -> Optional[str]:
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')
    except ClientError as e:
        logger.error(f"Failed to fetch S3 object: {e}")
        return None

def create_s3_presigned_url(bucket: str, key: str, expiration: int = 3600) -> Optional[str]:
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    try:
        return s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
    except ClientError as e:
        logger.error(f"Failed to generate presigned URL: {e}")
        return None