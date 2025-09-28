# job_portal_etl_project/plugins/minio_pipeline.py

import json
from minio import Minio
from minio.error import S3Error
from datetime import datetime
import os

class MinIOJsonPipeline:
    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key, minio_secure, minio_bucket_name):
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_secure = minio_secure
        self.minio_bucket_name = minio_bucket_name
        self.client = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            minio_endpoint=crawler.settings.get('MINIO_ENDPOINT'),
            minio_access_key=crawler.settings.get('MINIO_ACCESS_KEY'),
            minio_secret_key=crawler.settings.get('MINIO_SECRET_KEY'),
            minio_secure=crawler.settings.getbool('MINIO_SECURE', False),
            minio_bucket_name=crawler.settings.get('MINIO_BUCKET_NAME', 'joblistings-raw')
        )

    def open_spider(self, spider):
        try:
            self.client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.minio_secure
            )
            if not self.client.bucket_exists(self.minio_bucket_name):
                self.client.make_bucket(self.minio_bucket_name)
                spider.logger.info(f"MinIO bucket '{self.minio_bucket_name}' created.")
            else:
                spider.logger.info(f"MinIO bucket '{self.minio_bucket_name}' already exists.")
        except S3Error as e:
            spider.logger.error(f"Error connecting to MinIO or creating bucket: {e}")
            raise

    def process_item(self, item, spider):
        item_dict = dict(item)
        item_dict['crawl_timestamp'] = datetime.now().isoformat()
        json_line = json.dumps(item_dict, ensure_ascii=False) + '\n'

        current_date = datetime.now()
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")
        timestamp_for_filename = current_date.strftime("%Y%m%d%H%M%S%f")

        object_name = f"raw_data/job_listings/{year}/{month}/{day}/{spider.name}_{timestamp_for_filename}.jsonl"

        try:
            self.client.put_object(
                self.minio_bucket_name,
                object_name,
                data=json_line.encode('utf-8'),
                length=len(json_line.encode('utf-8')),
                content_type='application/jsonl'
            )
            spider.logger.info(f"Item saved to MinIO: {object_name}")
        except S3Error as e:
            spider.logger.error(f"Error saving item to MinIO: {e}")
            raise
        return item

    def close_spider(self, spider):
        spider.logger.info("MinIO pipeline closed.")