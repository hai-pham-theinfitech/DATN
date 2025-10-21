import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils import *
from minio import Minio
import os

# Khởi tạo Spark session từ utils.py



with DAG(
    dag_id="topcv_identify",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["scrapy", "minio", "topcv", "parquet", "identify_company"],
) as dag:
    run_scrapy_task = BashOperator(
        task_id="crawl",    
        bash_command="cd /opt/airflow/crawler && python -m topcv.identify_companies"
    )

   