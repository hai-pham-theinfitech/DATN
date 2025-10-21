from match_company import create_spark_session
import pyspark.sql.functions as F
from match_job import ColumnInfo, upsert_recruits_table

def upsert_recruits_table_from_topcv(
    master_recruit_table: str = 's3a://datn/master/recruit',
    recruit_table: str = 's3a://datn/raw/topcv/recruit',
    master_company_table: str = 's3a://datn/master/company',
):

    spark = create_spark_session()

    topcv_recruit_df = spark.read.format("delta").load(recruit_table)
    topcv_mapping_df = spark.read.format("delta").load(master_company_table)

    topcv_recruit_df = topcv_recruit_df.withColumn("media_code", F.lit("topcv"))
    topcv_recruit_df = topcv_recruit_df.withColumn("job_employment_type", F.array("job_employment_type"))

    if topcv_mapping_df:
        mapping_df = (
            topcv_mapping_df.groupBy("topcv_company_id")
            .agg(
                F.count("corporate_number").alias("count"),
                F.first("corporate_number").alias("corporate_number"),
                F.first("company_name").alias("master_company_name"),
                F.first("province").alias("province"),
            )
            .select("topcv_company_id", "corporate_number", "master_company_name", "province")
        )
        topcv_recruit_df = topcv_recruit_df.join(mapping_df, 
                topcv_recruit_df["job_company_id"] == mapping_df["topcv_company_id"]).drop(mapping_df["topcv_company_id"])

    media_name="topcv"
    upsert_recruits_table(
        spark,
        media_name,
        topcv_recruit_df,
        [
            ColumnInfo("job_id", "media_internal_id"),
            ColumnInfo("media_code", None),
            ColumnInfo("job_company_id", "company_id"),
            ColumnInfo("job_company_url", "company_url"),
            ColumnInfo("job_description", "description"),
            ColumnInfo("job_employment_type", "employment_type"),
            ColumnInfo("job_experience_requirements", "experience"),
            ColumnInfo("job_industry", "industries"),
            ColumnInfo("job_posted_date", "posted_at"),
            ColumnInfo("job_title", "title"),   
            ColumnInfo("job_min_salary", "min_salary"),
            ColumnInfo("job_max_salary", "max_salary"),
            ColumnInfo("source_job_url", "url"),
            ColumnInfo("corporate_number", "corporate_number"),
            ColumnInfo("job_benefits", "benefits"),
            ColumnInfo("province", "province"),
            
            
            ],
        master_recruit_table,
    )
    
upsert_recruits_table_from_topcv()