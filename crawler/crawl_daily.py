from deltalake import DeltaTable


def check_crawled_job(media: str):
    dt = DeltaTable(
        f"s3://datn/raw/{media}/recruit",
        storage_options={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_FORCE_PATH_STYLE": "true"
        }
    )
    df = dt.to_pandas(columns=["job_id"])
    job_ids = df["job_id"].tolist()
    return job_ids

def check_crawled_company(media: str):
    dt = DeltaTable(
        f"s3://datn/raw/{media}/company",
        storage_options={
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "AWS_ENDPOINT_URL": "http://localhost:9000",
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_FORCE_PATH_STYLE": "true"
        }
    )
    df = dt.to_pandas(columns=["company_id"])
    job_ids = df["company_id"].tolist()
    return job_ids



