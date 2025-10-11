
from pyspark.sql import SparkSession
from enum import Enum
from delta.tables import DeltaTable
import pyspark.sql.functions as F

from match_company import match_company_only_name


def create_spark_session():
    """Tạo Spark session với cấu hình MinIO và Delta Lake"""
    return SparkSession.builder \
        .appName("MinIO with Delta Lake") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
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


class IdentifyPersonRelocation(str, Enum):
    MASTER_COMPANY_ONLY_NAME = "MASTER_COMPANY_ONLY_NAME"
    MASTER_COMPANY_DOMAIN = "MASTER_COMPANY_DOMAIN"
    MASTER_PROVINCE = "MASTER_PROVINCE"
    MASTER_WARD = "MASTER_WARD"
    MASTER_WARD_PROVINCE = "MASTER_WARD_PROVINCE"
    MASTER_NAME_DOMAIN = "MASTER_NAME_DOMAIN"
    
    
def identify_person_relocation(
    table: str =  "s3a://datn/raw/vieclam24h/company",
    master_table: str = "s3a://datn/master/company",
    uuid_table: str = "s3a://datn/uuid/vieclam24h",
    identify_method: IdentifyPersonRelocation = IdentifyPersonRelocation.MASTER_COMPANY_ONLY_NAME,
):
    spark = create_spark_session()

    company_df = spark.read.format("delta").load(table)
    

    master_df = spark.read.format("delta").load(master_table)
  
    if identify_method == IdentifyPersonRelocation.MASTER_COMPANY_ONLY_NAME:
        df = match_company_only_name(company_df, master_df, column = "company_id")
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
        df.write.format("delta").save(uuid_table)
identify_person_relocation()
    
