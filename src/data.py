# import kagglehub

# # Download latest version
# path = kagglehub.dataset_download("elemento/nyc-yellow-taxi-trip-data")

# print("Path to dataset files:", path)


import os
import boto3
import kagglehub
from pathlib import Path
from botocore.exceptions import NoCredentialsError, ClientError

# Configuration
S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'nyc-taxi-dlq-data')
S3_PREFIX = 'raw/'
LOCAL_DATA_DIR = Path('./data/')
DATASET_HANDLE = "elemento/nyc-yellow-taxi-trip-data"

def upload_to_s3(local_path: Path):
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
    
    for file in local_path.glob('**/*'):
        if file.is_file() and file.suffix in ['.csv', '.parquet']:
            s3_key = f"{S3_PREFIX}{file.name}"
            try:
                s3.upload_file(str(file), S3_BUCKET, s3_key)
                print(f"Uploaded {file.name} to s3://{S3_BUCKET}/{s3_key}")
            except (NoCredentialsError, ClientError) as e:
                print(f"Error uploading {file.name}: {str(e)}")
                raise

def ensure_data_available():
    # Create data directory if not exists
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Check for existing data files
    existing_files = list(LOCAL_DATA_DIR.glob('*.csv')) + list(LOCAL_DATA_DIR.glob('*.parquet'))
    
    if not existing_files:
        print("Downloading dataset from Kaggle...")
        dataset_path = kagglehub.dataset_download(DATASET_HANDLE)
        print(f"Dataset downloaded to: {dataset_path}")
        
        # Handle Kaggle's zip format
        if str(dataset_path).endswith('.zip'):
            from zipfile import ZipFile
            with ZipFile(dataset_path, 'r') as zip_ref:
                zip_ref.extractall(LOCAL_DATA_DIR)
            os.remove(dataset_path)
    else:
        print(f"Using existing local data in {LOCAL_DATA_DIR}")

    # Upload to S3
    print("Uploading data to S3...")
    upload_to_s3(LOCAL_DATA_DIR)
    print("Data pipeline ready. Kafka producer can now stream from S3.")

if __name__ == "__main__":
    ensure_data_available()