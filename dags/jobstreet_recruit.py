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
    dag_id="jobstreet_recruit",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["scrapy", "minio", "jobstreet", "parquet"],
) as dag:
    run_scrapy_task = BashOperator(
        task_id="crawl",
        bash_command=bash(media="jobstreet", type="recruit")

    )

    process_and_upload_to_minio_task = PythonOperator(
        task_id="job",
        python_callable=process_and_upload_to_minio,
        op_kwargs={"media": "jobstreet"},
        provide_context=True,
    )
    
    # process_and_upload_to_minio_task_company = PythonOperator(
    #     task_id="company",
    #     python_callable=process_and_upload_to_minio_company,
    #     op_kwargs={"media": "jobstreet"},
    #     provide_context=True,
    # )

    run_scrapy_task >> process_and_upload_to_minio_task