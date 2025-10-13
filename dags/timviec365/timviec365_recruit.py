import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from utils import (
#     CRAWLER_DIR,
#     SPIDER_FILE_PATH,
#     RAW_OUTPUT_FILE_PATH,
#     timviec365_SPIDER_FILE_PATH,
#     process_and_upload_to_minio,

# )
from utils import *
from minio import Minio
import os


with DAG(
    dag_id="timviec365_recruit",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["scrapy", "minio", "timviec365", "parquet"],
) as dag:
    run_scrapy_task = BashOperator(
        task_id="crawl",
        bash_command=bash(media="timviec365", type="recruit"),

    )

    process_and_upload_to_minio_task = PythonOperator(
        task_id="job",
        python_callable=process_and_upload_to_minio,
        op_kwargs={"media": "timviec365"},
        provide_context=True,
    )
    
    

    run_scrapy_task >> process_and_upload_to_minio_task