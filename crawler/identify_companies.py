
from pyspark.sql import SparkSession
from enum import Enum
from delta.tables import DeltaTable
import pyspark.sql.functions as F

from match_company import *


def create_spark_session():
    """Tạo Spark session với cấu hình MinIO và Delta Lake"""
    return SparkSession.builder \
        .appName("MinIO with Delta Lake") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "io.delta:delta-spark_2.12:3.0.0") \
        .getOrCreate()


class IdentifyCompany(str, Enum):
    MASTER_COMPANY_ONLY_NAME = "MASTER_COMPANY_ONLY_NAME"
    MASTER_COMPANY_DOMAIN = "MASTER_COMPANY_DOMAIN"
    MASTER_NAME_ADDRESS = "MASTER_NAME_ADDRESS" 
    MASTER_WARD_PROVINCE_NAME = "MASTER_WARD_PROVINCE_NAME"
    MASTER_NAME_DOMAIN = "MASTER_NAME_DOMAIN"
    MASTER_STREET_PROVINCE = "MASTER_STREET_PROVINCE"
    MASTER_STREET_WARD_PROVINCE = "MASTER_STREET_WARD_PROVINCE"
    
    
def identify_company(
    media: str,
    master_table: str = "s3a://datn/master/company",
    identify_method: IdentifyCompany = IdentifyCompany.MASTER_COMPANY_ONLY_NAME,
):
    
    spark = create_spark_session()
    table =   f"s3a://datn/raw/{media}/company"
    uuid_table =  f"s3a://datn/uuid/{media}"
    company_df = spark.read.format("delta").load(table)
    
    company_df = company_df.dropDuplicates(["company_id"])
    master_df = spark.read.format("delta").load(master_table)

  
    if identify_method == IdentifyCompany.MASTER_COMPANY_ONLY_NAME:
        df = match_company_only_name(company_df, master_df, column = "company_id")
    elif identify_method == IdentifyCompany.MASTER_COMPANY_DOMAIN:
        if media == 'vietnamworks':
            pass
        df = match_company_domain(company_df, master_df, column = "company_id")
        
    elif identify_method == IdentifyCompany.MASTER_NAME_ADDRESS:
        df = match_company_name_address(company_df, master_df, column = "company_id")
    elif identify_method == IdentifyCompany.MASTER_WARD_PROVINCE_NAME:
        from match_company import match_company_ward_province_name
        df = match_company_ward_province_name(company_df, master_df, column = "company_id")
    elif identify_method == IdentifyCompany.MASTER_NAME_DOMAIN:
        from match_company import match_company_name_domain
        if media == 'vietnamworks':
            pass
        df = match_company_name_domain(company_df, master_df, column = "company_id")
    elif identify_method == IdentifyCompany.MASTER_STREET_PROVINCE:
        from match_company import match_province_street_address
        df = match_province_street_address(company_df, master_df, column = "company_id")
    elif identify_method == IdentifyCompany.MASTER_STREET_WARD_PROVINCE:
        from match_company import match_address_ward_province
        df = match_address_ward_province(company_df, master_df, column = "company_id")
        
    else:
        assert False

    if DeltaTable.isDeltaTable(spark, uuid_table):
        table = DeltaTable.forPath(spark, uuid_table)
        dest_table = table.alias("dest")
        dest = dest_table.toDF()
        src = df.alias("src")
        src = src.join(
            dest,
            (src["corporate_number"] == dest["corporate_number"]) & (src["identify_method"] <= dest["identify_method"]),
            "anti",
        )
        dest_table.merge(
            src,
            (dest["company_id"] == src["company_id"]) | (dest["corporate_number"] == src["corporate_number"]),
        ).whenNotMatchedInsertAll().whenMatchedUpdate(
            (src["identify_method"] >= dest["identify_method"]) & (dest["company_id"] == src["company_id"]),
            {x: src[x] for x in src.columns if x != "created_at"},
        ).whenMatchedDelete(
            (dest["corporate_number"] == src["corporate_number"]) & (dest["company_id"] != src["company_id"])
        ).execute()
        
               

    else:
        df.write.format("delta").option("overwrite", "True").save(uuid_table)


def run_identify(media: str):
    for method in IdentifyCompany:
        print(f"Identify method: {method}")
        identify_company(media = media, identify_method=method)
# identify() 
