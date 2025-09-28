import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utils import (
    bash,
    process_and_upload_to_minio
)
from minio import Minio
import os

# Khởi tạo Spark session từ utils.py



with DAG(
    dag_id="careerviet_recruit",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["scrapy", "minio", "careerviet", "parquet"],
) as dag:
    run_scrapy_task = BashOperator(
        task_id="crawl",
        # bash_command=f"""
        # echo "Starting Scrapy spider..."
        # echo "Current working directory: $(pwd)"
        # echo "Changing directory to {CRAWLER_DIR}"
        
        # cd {CRAWLER_DIR}/careerviet

        # python -m scrapy runspider {os.path.basename(CAREERVIET_SPIDER_FILE_PATH)} -o /opt/airflow/crawler/job.jsonl --set FEED_EXPORT_ENCODING=utf-8 --set FEED_FORMAT=jsonl

        # echo "Scrapy command finished. Checking for output file..."

        # if [ ! -f "/opt/airflow/crawler/job.jsonl" ]; then
        #     echo "Error: Raw output file '{os.path.basename(RAW_OUTPUT_FILE_PATH)}' not found! The spider might have failed to run or produce output."
        #     exit 1
        # else
        #     echo "Raw output file '{os.path.basename(RAW_OUTPUT_FILE_PATH)}' found. Task successful."
        # fi
        # """,
        bash_command = bash(media="careerviet", type = "recruit")

    )

    process_and_upload_to_minio_task = PythonOperator(
        task_id="job",
        python_callable=process_and_upload_to_minio,
        op_kwargs={"media": "careerviet"},
        provide_context=True,
    )
    
    # process_and_upload_to_minio_task_company = PythonOperator(
    #     task_id="company",
    #     python_callable=process_and_upload_to_minio_company,
    #     op_kwargs={"media": "careerviet"},
    #     provide_context=True,
    # )

    run_scrapy_task >> process_and_upload_to_minio_task