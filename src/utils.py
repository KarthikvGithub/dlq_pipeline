# import json
# import logging
# import logging.config
# import uuid
# from jsonschema import validate
# from pathlib import Path

# # Load schema from config file
# SCHEMA_PATH = Path(__file__).parent.parent / "config" / "schema.json"
# with open(SCHEMA_PATH, "r") as f:
#     SCHEMA = json.load(f)

# # Load logging configuration from config file
# LOGGING_CONFIG_PATH = Path(__file__).parent.parent / "config" / "logging.json"
# with open(LOGGING_CONFIG_PATH, "r") as f:
#     logging.config.dictConfig(json.load(f))

# logger = logging.getLogger(__name__)

# def validate_schema(record: dict) -> bool:
#     """Validate the record against the schema."""
#     try:
#         validate(instance=record, schema=SCHEMA['schema'])
#         return True
#     except Exception as e:
#         logger.error("Validation failed", extra={"error": str(e), "record": record})
#         return False

# def apply_corrections(record: dict) -> dict:
#     """Apply corrections to a record."""
#     # Convert numeric fields from strings to numbers
#     numeric_fields = [
#         "VendorID", "passenger_count", "trip_distance",
#         "pickup_longitude", "pickup_latitude", "RateCodeID",
#         "dropoff_longitude", "dropoff_latitude", "payment_type",
#         "fare_amount", "extra", "mta_tax", "tip_amount",
#         "tolls_amount", "improvement_surcharge", "total_amount"
#     ]

#     for field in numeric_fields:
#         if field in record:
#             # Handle empty values and special cases
#             value = record[field]
#             if isinstance(value, str):
#                 # Remove dollar signs and commas
#                 value = value.replace('$', '').replace(',', '')
#                 # Handle empty string case
#                 if value.strip() == '':  
#                     value = '0'
#                 # Handle .XX decimal format
#                 if value.startswith('.'):
#                     value = '0' + value
                
#                 try:
#                     # Convert to float first to handle decimals
#                     record[field] = float(value)
#                     # Convert to int if appropriate
#                     if field in ["VendorID", "passenger_count", "RateCodeID", "payment_type"]:
#                         record[field] = int(float(value))
#                 except ValueError:
#                     record[field] = 0.0  # Default to 0 if conversion fails

#     # Ensure fare_amount is non-negative
#     record['fare_amount'] = abs(record.get('fare_amount', 0))
    
#     # Generate UUID if missing
#     if 'uuid' not in record:
#         record['uuid'] = str(uuid.uuid4())
    
#     return record

# import boto3
# import os
# from botocore.exceptions import ClientError
# import requests
# import psycopg2

# def upload_to_s3(file_path: str, bucket: str, s3_key: str):
#     s3 = boto3.client('s3',
#         aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
#         aws_secret_access_key=os.getenv('AWS_SECRET_KEY'))
#     try:
#         s3.upload_file(file_path, bucket, s3_key)
#         print(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")
#     except ClientError as e:
#         print(f"S3 upload error: {e}")


# def is_duplicate(message_id: str) -> bool:
#     conn = psycopg2.connect(os.getenv('POSTGRES_URI'))
#     cursor = conn.cursor()
#     cursor.execute("SELECT 1 FROM processed_messages WHERE message_id = %s", (message_id,))
#     return cursor.fetchone() is not None


# # utils.py
# def send_alert(message):
#     requests.post(
#         os.getenv('ALERT_WEBHOOK'),
#         json={
#             'text': f"[DLQ Alert] {message}",
#             'severity': 'critical'
#         }
#     )



import json
import logging
import logging.config
import uuid
import os
import time
import boto3
import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, StringType, StructType, StructField, IntegerType, FloatType, TimestampType
from jsonschema import validate
from pathlib import Path
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional
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

@udf(returnType=BooleanType())
def validate_schema_udf(record_json: str) -> bool:
    """Spark UDF for schema validation."""
    try:
        record = json.loads(record_json)
        validate(instance=record, schema=SCHEMA['schema'])
        return True
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        return False

@udf(returnType=StringType())
def apply_corrections_udf(record_json: str) -> str:
    """Spark UDF for applying corrections."""
    try:
        record = json.loads(record_json)
        numeric_fields = [
            "VendorID", "passenger_count", "trip_distance",
            "pickup_longitude", "pickup_latitude", "RateCodeID",
            "dropoff_longitude", "dropoff_latitude", "payment_type",
            "fare_amount", "extra", "mta_tax", "tip_amount",
            "tolls_amount", "improvement_surcharge", "total_amount"
        ]

        for field in numeric_fields:
            if field in record:
                value = record[field]
                if isinstance(value, str):
                    value = value.replace('$', '').replace(',', '')
                    if value.strip() == '':
                        value = '0'
                    if value.startswith('.'):
                        value = '0' + value
                    
                    try:
                        record[field] = float(value)
                        if field in ["VendorID", "passenger_count", "RateCodeID", "payment_type"]:
                            record[field] = int(float(value))
                    except ValueError:
                        record[field] = 0.0

        record['fare_amount'] = abs(record.get('fare_amount', 0))
        
        if 'uuid' not in record:
            record['uuid'] = str(uuid.uuid4())
        
        return json.dumps(record)
    except Exception as e:
        logger.error(f"Correction failed: {str(e)}")
        return record_json

@udf(returnType=StringType())
def log_to_elasticsearch_udf(record_json: str, event_type: str) -> str:
    """Spark UDF for logging to Elasticsearch."""
    try:
        record = json.loads(record_json)
        log_entry = {
            "@timestamp": datetime.utcnow().isoformat(),
            "level": "INFO",
            "message": "DLQ processing event",
            "data": {
                "event": event_type,
                "record": record,
                "timestamp": int(time.time() * 1000)
            }
        }
        
        es_endpoint = os.getenv('ELASTICSEARCH_ENDPOINT', 'http://elasticsearch:9200')
        response = requests.post(
            f"{es_endpoint}/dlq-logs/_doc",
            json=log_entry,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        response.raise_for_status()
        return "success"
    except Exception as e:
        logger.error(f"Failed to log to Elasticsearch: {e}")
        return f"error: {str(e)}"

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