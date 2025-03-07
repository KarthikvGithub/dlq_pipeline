import os
import boto3
import kagglehub
from pathlib import Path
import shutil
from utils import parse_aws_config
from botocore.exceptions import NoCredentialsError, ClientError

# **/data


# Configuration
LOCAL_DATA_DIR = Path('./data/')
DATASET_HANDLE = "elemento/nyc-yellow-taxi-trip-data"


def upload_to_s3(local_path: Path):
    if not any(local_path.glob('*')):
        print("No files found to upload.")
        print(local_path)
        return
    aws_credentials = parse_aws_config()
    s3 = boto3.client('s3',
        aws_access_key_id=aws_credentials['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=aws_credentials['AWS_SECRET_ACCESS_KEY']
    )

    S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'dlq-pipeline-source-data')
    S3_PREFIX = 'raw/'
    
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
        dataset_path = Path(dataset_path)
        print(f"Dataset downloaded to: {dataset_path}")
        
        if not os.path.exists(dataset_path):
            print("Dataset not downloaded. Check Kaggle credentials.")
            return

        for file in dataset_path.glob("**/*"):
            if file.is_file() and file.suffix in ['.csv', '.parquet']:
                shutil.copy(file, LOCAL_DATA_DIR)
                print(f"Copied {file.name} to {LOCAL_DATA_DIR}")
    else:
        print(f"Using existing local data in {LOCAL_DATA_DIR}")

    # Upload to S3
    print("Uploading data to S3...")
    upload_to_s3(LOCAL_DATA_DIR)
    print("Data pipeline ready. Kafka producer can now stream from S3.")

if __name__ == "__main__":
    ensure_data_available()