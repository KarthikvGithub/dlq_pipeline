from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from dlq_consumer import consume_dlq_messages
from kafka_consumer import consume_messages
from utils import log_to_elasticsearch
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True,
}

def check_dlq_size():
    from src.dlq_consumer import dlq_consumer
    from confluent_kafka import KafkaException

    try:
        metadata = dlq_consumer.list_topics("nyc_taxi_dlq", timeout=10)
        partitions = metadata.topics["nyc_taxi_dlq"].partitions

        total_messages = 0
        for partition in partitions.values():
            low, high = dlq_consumer.get_watermark_offsets(partition, timeout=10)
            total_messages += high - low

        logger.info(f"DLQ size: {total_messages} messages")
        log_to_elasticsearch({"dlq_size": total_messages, "timestamp": datetime.now()})

    except KafkaException as e:
        logger.error(f"Failed to check DLQ size: {e}")
        raise

def start_reprocessing():
    from src.dlq_consumer import dlq_consumer, producer

    try:
        consume_dlq_messages()
        logger.info("DLQ reprocessing completed successfully")
    except Exception as e:
        logger.error(f"Failed to reprocess DLQ messages: {e}")
        raise

def backup_dlq_to_s3():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Export DLQ data to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_path = f"/tmp/dlq_backup_{timestamp}.csv"
        
        with open(local_path, 'w') as f:
            pg_hook.copy_expert(
                "COPY (SELECT * FROM dlq_messages) TO STDOUT WITH CSV HEADER",
                f
            )
        
        # Upload to S3
        s3_key = f"dlq_backups/{timestamp}.csv"
        s3_hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=os.getenv('AWS_S3_BUCKET')
        )
        
        logger.info(f"DLQ backup uploaded to s3://{os.getenv('AWS_S3_BUCKET')}/{s3_key}")
        
    except Exception as e:
        logger.error(f"Failed to backup DLQ: {e}")
        raise

with DAG(
    'dlq_monitoring',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    check_dlq_task = PythonOperator(
        task_id='check_dlq_metrics',
        python_callable=check_dlq_size,
    )

    reprocess_dlq_task = PythonOperator(
        task_id='trigger_reprocessing',
        python_callable=start_reprocessing,
    )

    backup_dlq_task = PythonOperator(
        task_id='backup_dlq_to_s3',
        python_callable=backup_dlq_to_s3,
    )

    check_dlq_task >> [reprocess_dlq_task, backup_dlq_task]