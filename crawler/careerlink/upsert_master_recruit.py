from crawler.match_job import upsert_recruits_table, ColumnInfo
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from crawler.timviec365.identify_companies import create_spark_session


def upsert_recruits_table_from_careerlink(
    master_recruit_table: str = 's3a://datn/master/recruit',
    recruit_table: str = 's3a://datn/raw/careerlink/recruit',
    master_company_table: str = 's3a://datn/master/company',

):
    spark = create_spark_session()
    recruit_df = spark.read.format("delta").load(recruit_table)
    mapping_df = spark.read.format("delta").load(master_company_table)
   

    recruit_df = recruit_df.withColumn("media_code", F.lit("careerlink"))

    # map to corporate_number + master company name by careerlink company id if available
    if mapping_df and "careerlink_company_id" in mapping_df.columns and "job_company_id" in recruit_df.columns:
        company_map = (
            mapping_df.groupBy("careerlink_company_id")
            .agg(
                F.count("corporate_number").alias("count"),
                F.first("corporate_number").alias("corporate_number"),
                F.first("name_raw").alias("master_company_name"),
            )
            .select("careerlink_company_id", "corporate_number", "master_company_name")
        )
        recruit_df = recruit_df.join(
            company_map,
            recruit_df["job_company_id"] == company_map["careerlink_company_id"],
            "left",
        ).drop(company_map["careerlink_company_id"])

    # Parse job_salary_normalized into separate columns
    recruit_df = recruit_df.withColumn("job_salary_year_min", F.col("job_salary_normalized.year").getItem(0).getItem("min"))
    recruit_df = recruit_df.withColumn("job_salary_year_max", F.col("job_salary_normalized.year").getItem(0).getItem("max"))
    recruit_df = recruit_df.withColumn("job_salary_year_value", F.col("job_salary_normalized.year").getItem(0).getItem("value"))
    
    recruit_df = recruit_df.withColumn("job_salary_month_min", F.col("job_salary_normalized.month").getItem(0).getItem("min"))
    recruit_df = recruit_df.withColumn("job_salary_month_max", F.col("job_salary_normalized.month").getItem(0).getItem("max"))
    recruit_df = recruit_df.withColumn("job_salary_month_value", F.col("job_salary_normalized.month").getItem(0).getItem("value"))
    
    recruit_df = recruit_df.withColumn("job_salary_day_min", F.col("job_salary_normalized.day").getItem(0).getItem("min"))
    recruit_df = recruit_df.withColumn("job_salary_day_max", F.col("job_salary_normalized.day").getItem(0).getItem("max"))
    recruit_df = recruit_df.withColumn("job_salary_day_value", F.col("job_salary_normalized.day").getItem(0).getItem("value"))
    
    recruit_df = recruit_df.withColumn("job_salary_hour_min", F.col("job_salary_normalized.hour").getItem(0).getItem("min"))
    recruit_df = recruit_df.withColumn("job_salary_hour_max", F.col("job_salary_normalized.hour").getItem(0).getItem("max"))
    recruit_df = recruit_df.withColumn("job_salary_hour_value", F.col("job_salary_normalized.hour").getItem(0).getItem("value"))

    media_name = "careerlink"
    upsert_recruits_table(
        spark,
        media_name,
        recruit_df,
        [
            ColumnInfo("source_job_url", "source_recruit_url"),
            ColumnInfo("job_id", "media_internal_id"),
            ColumnInfo("media_code", None),
            ColumnInfo("corporate_number", None),
            ColumnInfo("_ingest_id", None),
            ColumnInfo("job_title_normalized", "title"),
            ColumnInfo("job_content", "content"),
            ColumnInfo("job_working_location", "working_location"),
            ColumnInfo("job_working_time", "working_time"),
            ColumnInfo("job_remote_flag", "remote_flag"),
            ColumnInfo("job_full_remote_flag", "full_remote_flag"),
            ColumnInfo("job_employment_type_normalized", "employment_type_codes"),
            ColumnInfo("job_welfare_benefit", "welfare_benefit"),
            ColumnInfo("job_holiday", "holiday"),
            # tech/tags
            ColumnInfo("job_tech_languages", "tech_languages"),
            ColumnInfo("job_tech_frameworks", "tech_frameworks"),
            ColumnInfo("job_tech_clouds", "tech_clouds"),
            ColumnInfo("job_tech_databases", "tech_databases"),
            ColumnInfo("job_communication_tools", "communication_tools"),
            ColumnInfo("job_management_tools", "management_tools"),
            ColumnInfo("job_internal_tools", "internal_tools"),
            ColumnInfo("job_crm_tools", "crm_tools"),
            ColumnInfo("job_skills", "skill_codes"),
            # salary
            ColumnInfo("job_salary", "salary_raw"),
            ColumnInfo("job_salary_year_min", "salary_year_min"),
            ColumnInfo("job_salary_year_max", "salary_year_max"),
            ColumnInfo("job_salary_year_value", "salary_year_value"),
            ColumnInfo("job_salary_month_min", "salary_month_min"),
            ColumnInfo("job_salary_month_max", "salary_month_max"),
            ColumnInfo("job_salary_month_value", "salary_month_value"),
            ColumnInfo("job_salary_day_min", "salary_day_min"),
            ColumnInfo("job_salary_day_max", "salary_day_max"),
            ColumnInfo("job_salary_day_value", "salary_day_value"),
            ColumnInfo("job_salary_hour_min", "salary_hour_min"),
            ColumnInfo("job_salary_hour_max", "salary_hour_max"),
            ColumnInfo("job_salary_hour_value", "salary_hour_value"),
            # linkage
            ColumnInfo("job_company_id", None),
        ],
        master_recruit_table,
    )