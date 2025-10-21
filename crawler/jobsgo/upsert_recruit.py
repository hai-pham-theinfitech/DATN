from match_company import create_spark_session
import pyspark.sql.functions as F
from match_job import ColumnInfo, upsert_recruits_table

def upsert_recruits_table_from_jobsgo(
    master_recruit_table: str = 's3a://datn/master/recruit',
    recruit_table: str = 's3a://datn/raw/jobsgo/recruit',
    master_company_table: str = 's3a://datn/master/company',
):

    spark = create_spark_session()

    jobsgo_recruit_df = spark.read.format("delta").load(recruit_table)
    jobsgo_mapping_df = spark.read.format("delta").load(master_company_table)

    jobsgo_recruit_df = jobsgo_recruit_df.withColumn("media_code", F.lit("jobsgo"))
    
    if jobsgo_mapping_df:
        mapping_df = (
            jobsgo_mapping_df.groupBy("jobsgo_company_id")
            .agg(
                F.count("corporate_number").alias("count"),
                F.first("corporate_number").alias("corporate_number"),
                F.first("company_name").alias("master_company_name"),
                F.first("province").alias("province"),
            )
            .select("jobsgo_company_id", "corporate_number", "master_company_name","province")
        )
        jobsgo_recruit_df = jobsgo_recruit_df.join(mapping_df, 
                jobsgo_recruit_df["job_company_id"] == mapping_df["jobsgo_company_id"]).drop(mapping_df["jobsgo_company_id"])

    media_name="jobsgo"
    upsert_recruits_table(
        spark,
        media_name,
        jobsgo_recruit_df,
        [
            ColumnInfo("job_id", "media_internal_id"),
            ColumnInfo("media_code", None),
            ColumnInfo("job_company_id", "company_id"),
            ColumnInfo("end_at", "end_at"),
            ColumnInfo("job_benefit", "benefits"),
            ColumnInfo("job_company_name", "company_name"),
            ColumnInfo("job_description", "description"),
            ColumnInfo("job_employment_type", "employment_type"),
            ColumnInfo("job_exp_requirement", "experience"),
            ColumnInfo("job_industry", "industries"),
            ColumnInfo("job_max_salary", "max_salary"),
            ColumnInfo("job_min_salary", "min_salary"),
            ColumnInfo("job_title", "title"),
            ColumnInfo("source_job_url", "url"),
            ColumnInfo("corporate_number", "corporate_number"),
            ColumnInfo("province", "province"),

            
            ],
        master_recruit_table,
    )
    
upsert_recruits_table_from_jobsgo()