import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import json
from minio import Minio
from datetime import datetime
# Định nghĩa đường dẫn
CRAWLER_DIR = "/opt/airflow/crawler"
# SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "crawler.py")
# #careerviet
# CAREERVIET_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "careerviet_recruit.py")
# CAREERVIET_COMPANY_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "careerviet_company.py")
# #vieclam24h
# VIECLAM24H_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "vieclam24h_recruit.py")
# VIECLAM24H_COMPANY_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "vieclam24h_company.py")
# #careerlink
# CAREERLINK_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "careerlink_recruit.py")
# CAREERLINK_COMPANY_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "careerlink_company.py")
# #topcv
# TOPCV_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "topcv_recruit.py")
# TOPCV_COMPANY_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "topcv_company.py")
# #vietnamworks
# VIETNAMWORKS_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "vietnamworks_recruit.py")
# VIETNAMWORKS_COMPANY_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "vietnamworks_company.py")
# #jobstreet
# JOBSTREET_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "jobstreet_recruit.py")
# JOBSTREET_COMPANY_SPIDER_FILE_PATH = os.path.join(CRAWLER_DIR, "jobstreet_company.py")

ingest_id =  datetime.utcnow().strftime("%Y%m%dT")

RAW_COMPANY = os.path.join(CRAWLER_DIR, "job.jsonl")
PARQUET_OUTPUT_FILE_PATH = os.path.join(CRAWLER_DIR, "job.parquet")

RAW_COMPANY = os.path.join(CRAWLER_DIR, "company.jsonl")
PARQUET_COMPANY = os.path.join(CRAWLER_DIR, "company.parquet")

# Cấu hình MinIO từ biến môi trường
MINIO_HOST = os.environ.get("MINIO_HOST", "localhost:9000")
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET_NAME = "datn"

# Cấu hình Spark với Delta Lake
def get_spark_session():
    builder = SparkSession.builder \
        .appName("DeltaLakeWriter") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_HOST}") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()



def process_and_upload_to_minio(media: str, **kwargs):
    """
    Đọc file JSON Lines của media, loại bỏ duplicate company_id, ghi vào Delta Lake,
    và upload toàn bộ Delta folder lên MinIO.
    """
    json_path = f"/opt/airflow/crawler/{media}/{media}_recruit.jsonl"
    delta_output_path = f"s3a://{MINIO_BUCKET_NAME}/raw/{media}/recruit"

    # 1. Khởi tạo Spark
    spark = get_spark_session()

    # 2. Đọc JSON Lines trực tiếp bằng Spark (tự infer schema)
    try:
        df = spark.read.option("multiLine", False).json(json_path)
        print(f"DataFrame created: {df.count()} rows, columns: {df.columns}")
    except Exception as e:
        print(f"Error reading JSON Lines with Spark: {e}")
        return

    # 3. Loại bỏ các company_id duplicate
    try:
        df_distinct = df.dropDuplicates(["job_id"])
        print(f"Distinct job_id count: {df_distinct.count()}")
    except Exception as e:
        print(f"Error removing duplicates: {e}")
        return

    # 4. Ghi Delta Lake (overwrite)
    try:
        df_distinct.write.format("delta").mode("overwrite").save(delta_output_path)
        print(f"Data written to Delta Lake at {delta_output_path}")
    except Exception as e:
        print(f"Error writing to Delta Lake: {e}")
        return

    # 5. Kiểm tra Delta log
    try:
        delta_log_path = os.path.join(delta_output_path, "_delta_log")
        if os.path.exists(delta_log_path):
            print(f"Delta Log exists at {delta_log_path}")
        else:
            print(f"Delta Log not found at {delta_log_path}")
    except Exception as e:
        print(f"Error checking Delta Log: {e}")
        return

    # 6. Upload MinIO
    try:
        client = Minio(
            MINIO_HOST,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # Upload tất cả file trong thư mục Delta
        for root, dirs, files in os.walk(delta_output_path):
            for file in files:
                file_path = os.path.join(root, file)
                object_name = os.path.relpath(file_path, delta_output_path)
                client.fput_object(MINIO_BUCKET_NAME, object_name, file_path)
                print(f"Uploaded {file_path} -> bucket {MINIO_BUCKET_NAME} as {object_name}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")

        
        
        

def process_and_upload_to_minio_company(media: str, **kwargs):
    """
    Đọc file JSON Lines của media, loại bỏ duplicate company_id, ghi vào Delta Lake,
    và upload toàn bộ Delta folder lên MinIO.
    """
    json_path = f"/opt/airflow/crawler/{media}/{media}_company.jsonl"
    delta_output_path = f"s3a://{MINIO_BUCKET_NAME}/raw/{media}/company"

    # 1. Khởi tạo Spark
    spark = get_spark_session()

    # 2. Đọc JSON Lines trực tiếp bằng Spark (tự infer schema)
    try:
        df = spark.read.option("multiLine", False).json(json_path)
        print(f"DataFrame created: {df.count()} rows, columns: {df.columns}")
    except Exception as e:
        print(f"Error reading JSON Lines with Spark: {e}")
        return

    # 3. Loại bỏ các company_id duplicate
    try:
        df_distinct = df.dropDuplicates(["company_id"])
        print(f"Distinct company_id count: {df_distinct.count()}")
    except Exception as e:
        print(f"Error removing duplicates: {e}")
        return

    # 4. Ghi Delta Lake (overwrite)
    try:
        df_distinct.write.format("delta").mode("overwrite").save(delta_output_path)
        print(f"Data written to Delta Lake at {delta_output_path}")
    except Exception as e:
        print(f"Error writing to Delta Lake: {e}")
        return

    # 5. Kiểm tra Delta log
    try:
        delta_log_path = os.path.join(delta_output_path, "_delta_log")
        if os.path.exists(delta_log_path):
            print(f"Delta Log exists at {delta_log_path}")
        else:
            print(f"Delta Log not found at {delta_log_path}")
    except Exception as e:
        print(f"Error checking Delta Log: {e}")
        return

    # 6. Upload MinIO
    try:
        client = Minio(
            MINIO_HOST,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # Upload tất cả file trong thư mục Delta
        for root, dirs, files in os.walk(delta_output_path):
            for file in files:
                file_path = os.path.join(root, file)
                object_name = os.path.relpath(file_path, delta_output_path)
                client.fput_object(MINIO_BUCKET_NAME, object_name, file_path)
                print(f"Uploaded {file_path} -> bucket {MINIO_BUCKET_NAME} as {object_name}")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")







       
def bash(media: str, type: str):
    command = f"""
        echo "Starting Scrapy spider..."
        echo "Changing directory to {CRAWLER_DIR}"
        cd {CRAWLER_DIR}

        PYTHONPATH={CRAWLER_DIR}:/home/airflow/.local/lib/python3.10/site-packages python -m scrapy runspider {media}/{media}_{type}.py -o /opt/airflow/crawler/{media}/{media}_{type}.jsonl --set FEED_EXPORT_ENCODING=utf-8 --set FEED_FORMAT=jsonl

        echo "Scrapy command finished. Checking for output file..."

        if [ ! -f "/opt/airflow/crawler/{media}/{media}_{type}.jsonl" ]; then
            echo "Error: Raw output file '/opt/airflow/crawler/{media}/{media}_{type}.jsonl' not found! The spider might have failed to run or produce output."
            exit 1
        else
            echo "Raw output file '/opt/airflow/crawler/{media}/{media}_{type}.jsonl' found. Task successful."
        fi
    """
    return command


        