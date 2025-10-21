from match_company import create_spark_session
import pyspark.sql.functions as F
from match_job import ColumnInfo, upsert_recruits_table

def upsert_recruits_table_from_timviec365(
    master_recruit_table: str = 's3a://datn/master/recruit',
    recruit_table: str = 's3a://datn/raw/timviec365/recruit',
    master_company_table: str = 's3a://datn/master/company',
):

    spark = create_spark_session()

    timviec365_recruit_df = spark.read.format("delta").load(recruit_table)
    timviec365_mapping_df = spark.read.format("delta").load(master_company_table)

    timviec365_recruit_df = timviec365_recruit_df.withColumn("media_code", F.lit("timviec365"))
    timviec365_recruit_df = timviec365_recruit_df.withColumn(
    "industries",
    F.concat_ws(",", F.col("industries")) 
)
    if timviec365_mapping_df:
        mapping_df = (
            timviec365_mapping_df.groupBy("timviec365_company_id")
            .agg(
                F.count("corporate_number").alias("count"),
                F.first("corporate_number").alias("corporate_number"),
                F.first("company_name").alias("master_company_name"),
                F.first("province").alias("province"),
            )
            .select("timviec365_company_id", "corporate_number", "master_company_name", "province")
        )
        timviec365_recruit_df = timviec365_recruit_df.join(mapping_df, 
                timviec365_recruit_df["job_company_id"] == mapping_df["timviec365_company_id"]).drop(mapping_df["timviec365_company_id"])

    media_name="timviec365"
    upsert_recruits_table(
        spark,
        media_name,
        timviec365_recruit_df,
        [
            ColumnInfo("job_id", "media_internal_id"),
            ColumnInfo("media_code", None),
            ColumnInfo("job_company_id", "company_id"),
            ColumnInfo("degree", "degree"),
            ColumnInfo("age", "age"),
            ColumnInfo("address", "address"),
            ColumnInfo("benefits", "benefits"),
            ColumnInfo("gender", "gender"),
            ColumnInfo("headcount","recruit_count"),
            ColumnInfo("industries", "industries"),
            ColumnInfo("job_company_name", "company_name"),
            ColumnInfo("source_url", "url"),
            ColumnInfo("job_title", "title"),
            ColumnInfo("job_min_salary", "min_salary"),
            ColumnInfo("job_max_salary", "max_salary"),
            ColumnInfo("job_description", "description"),
            ColumnInfo("requirements", "requirements"),
            ColumnInfo("corporate_number", "corporate_number"),
            ColumnInfo("province", "province"),
            
            
            ],
        master_recruit_table,
    )
    
upsert_recruits_table_from_timviec365()