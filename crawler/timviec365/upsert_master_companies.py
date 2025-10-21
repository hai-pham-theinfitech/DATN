

# from identify_companies import create_spark_session 
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from match_company import ColumnInfo, create_spark_session, upsert_companies_table


        
def upsert_companies_from_timviec365(
    master_company_table: str = "s3a://datn/master/company",
    company_table: str = "s3a://datn/raw/timviec365/company",
    uuid_table: str = "s3a://datn/uuid/timviec365",
):
    spark = create_spark_session()
    company_df = spark.read.format("delta").load(company_table)
    company_df = company_df.dropDuplicates(["company_id"])
    media_name = "timviec365"
    quality = 20000
    
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
            ColumnInfo("company_id", "timviec365_company_id", quality),
            ColumnInfo("company_name", None, quality),
            ColumnInfo("company_only_name", "standard_name", quality),
            ColumnInfo("company_url", None, quality),
            ColumnInfo("company_industry", None, quality),
            ColumnInfo("company_size",None, quality),
            ColumnInfo("company_email", "email", quality),
            ColumnInfo("company_established_date", "establish_date", quality),
            ColumnInfo("company_industry", "industry", quality),
            ColumnInfo("company_representative", "representative", quality),
            ColumnInfo("province", "province", quality),
            ColumnInfo("district", "district", quality),
            ColumnInfo("ward", "ward", quality),
            ColumnInfo("street", "street", quality),
            
            
            
        ],  
        master_company_table,
    )
upsert_companies_from_timviec365()