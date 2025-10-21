from match_company import create_spark_session
import pyspark.sql.functions as F
from match_job import ColumnInfo, upsert_recruits_table

def upsert_recruits_table_from_vieclam24h(
    master_recruit_table: str = 's3a://datn/master/recruit',
    recruit_table: str = 's3a://datn/raw/vieclam24h/recruit',
    master_company_table: str = 's3a://datn/master/company',
):

    spark = create_spark_session()

    vieclam24h_recruit_df = spark.read.format("delta").load(recruit_table)
    vieclam24h_mapping_df = spark.read.format("delta").load(master_company_table)

    vieclam24h_recruit_df = vieclam24h_recruit_df.withColumn("media_code", F.lit("vieclam24h"))
    vieclam24h_recruit_df = vieclam24h_recruit_df.withColumn("job_employment_type", F.array("job_employment_type"))

    if vieclam24h_mapping_df:
        mapping_df = (
            vieclam24h_mapping_df.groupBy("vieclam24h_company_id")
            .agg(
                F.count("corporate_number").alias("count"),
                F.first("corporate_number").alias("corporate_number"),
                F.first("company_name").alias("master_company_name"),
                F.first("province").alias("province"),
            )
            .select("vieclam24h_company_id", "corporate_number", "master_company_name","province")
        )
        vieclam24h_recruit_df = vieclam24h_recruit_df.join(mapping_df, 
                vieclam24h_recruit_df["job_company_id"] == mapping_df["vieclam24h_company_id"]).drop(mapping_df["vieclam24h_company_id"])

    media_name="vieclam24h"
    upsert_recruits_table(
        spark,
        media_name,
        vieclam24h_recruit_df,
        [   
            ColumnInfo("province", "province"),
            ColumnInfo("job_id", "media_internal_id"),
            ColumnInfo("media_code", None),
            ColumnInfo("job_company_id", "company_id"),
            ColumnInfo("job_company_url", "company_url"),
            ColumnInfo("job_description", "description"),
            ColumnInfo("job_benefits", "benefits"),
            ColumnInfo("job_date_posted", "posted_at"),
            ColumnInfo("job_employment_type", "employment_type"),
            ColumnInfo("job_education_requirements", "degree"),
            ColumnInfo("job_industry", "industries"),
            ColumnInfo("source_job_url", "url"),
            ColumnInfo("job_title", "title"),
            ColumnInfo("job_min_salary", "min_salary"),
            ColumnInfo("job_max_salary", "max_salary"),
            ColumnInfo("corporate_number", "corporate_number"),
            ColumnInfo("job_skills", "skills"),
            
            
            ],
        master_recruit_table,
    )
    
upsert_recruits_table_from_vieclam24h()