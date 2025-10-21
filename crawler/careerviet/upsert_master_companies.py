

# from identify_companies import create_spark_session 
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from match_company import ColumnInfo, create_spark_session, upsert_companies_table


        
def upsert_companies_from_careerviet(
    master_company_table: str = "s3a://datn/master/company",
    company_table: str = "s3a://datn/raw/careerviet/company",
    uuid_table: str = "s3a://datn/uuid/careerviet",
):
    spark = create_spark_session()
    company_df = spark.read.format("delta").load(company_table)
    media_name = "careerviet"
    quality = 29000
    
    if DeltaTable.isDeltaTable(spark, uuid_table):
        uuid_df = spark.read.format("delta").load(uuid_table)
        uuid_df = uuid_df.select("company_id", "corporate_number")
        
        # Join company_df với uuid_df để lấy corporate_number
        company_with_uuid = company_df.join(
            uuid_df, 
            company_df["company_id"] == uuid_df["company_id"], 
            "left"
        ).drop(uuid_df["company_id"])
        
        company_with_uuid = company_with_uuid.withColumn(
            "corporate_number",
            F.when(
                F.col("corporate_number").isNull(),
                F.sha2(F.coalesce(F.col("company_only_name").cast("string"), F.lit("")), 256)
            ).otherwise(F.col("corporate_number"))
        )
    
    else:
        company_with_uuid = company_df.withColumn(
            "corporate_number",
            F.sha2(F.coalesce(F.col("company_only_name").cast("string"), F.lit("")), 256)
        )
    company_with_uuid = company_with_uuid.dropDuplicates(["corporate_number"])
    upsert_companies_table(
        spark,
        media_name,
        company_with_uuid, 
        [
            ColumnInfo("company_id", "careerviet_company_id", quality),
            ColumnInfo("company_name", None, quality),
            ColumnInfo("company_only_name", "standard_name", quality),
            ColumnInfo("company_address","address", quality),
            ColumnInfo("company_description", "description", quality),
            ColumnInfo("company_domain", "domain", quality),
            ColumnInfo("source_company_url", "company_url", quality),
            ColumnInfo("province", "province", quality),
            ColumnInfo("ward", "ward", quality),
            ColumnInfo("street", "street", quality),
            ColumnInfo("district", "district", quality),

        ],  
        master_company_table,
    )
upsert_companies_from_careerviet()