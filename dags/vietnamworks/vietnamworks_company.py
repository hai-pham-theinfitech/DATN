import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils import *

from minio import Minio
import os

# Khởi tạo Spark session từ utils.py



with DAG(
    dag_id="vietnamworks_company",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["scrapy", "minio", "vietnamworks", "parquet"],
) as dag:
    run_scrapy_task = BashOperator(
        task_id="crawl",
        bash_command=bash(media="vietnamworks", type="company")

    )

   
    
    process_and_upload_to_minio_task_company = PythonOperator(
        task_id="company",
        python_callable=process_and_upload_to_minio_company,
        op_kwargs={"media": "vietnamworks"},
        provide_context=True,
    )

    run_scrapy_task >> process_and_upload_to_minio_task_company