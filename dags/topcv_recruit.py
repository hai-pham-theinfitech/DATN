import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utils import bash, process_and_upload_to_minio
import os
import json

# Headers phải được encode đúng JSON để không bị lỗi f-string
headers_json = json.dumps({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.google.com"
})

bash_command = bash("topcv", type="recruit")

with DAG(
    dag_id="topcv_recruit",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule=None,
    catchup=False,
    tags=["scrapy", "minio", "topcv", "parquet"],
) as dag:

    run_scrapy_task = BashOperator(
        task_id="crawl",
        bash_command=bash_command,
    )

    process_and_upload_to_minio_task = PythonOperator(
        task_id="job",
        python_callable=process_and_upload_to_minio,
        op_kwargs={"media": "topcv"},
        provide_context=True,
    )

    run_scrapy_task >> process_and_upload_to_minio_task

