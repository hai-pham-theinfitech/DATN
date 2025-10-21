from match_company import create_spark_session
import pyspark.sql.functions as F
from match_job import ColumnInfo, upsert_recruits_table

def upsert_recruits_table_from_careerviet(
    master_recruit_table: str = 's3a://datn/master/recruit',
    recruit_table: str = 's3a://datn/raw/careerviet/recruit',
    master_company_table: str = 's3a://datn/master/company',
):

    spark = create_spark_session()

    careerviet_recruit_df = spark.read.format("delta").load(recruit_table)
    careerviet_mapping_df = spark.read.format("delta").load(master_company_table)
    careerviet_recruit_df = careerviet_recruit_df.withColumn("media_code", F.lit("careerviet"))
    

    
    careerviet_recruit_df = careerviet_recruit_df.withColumn(
    "job_max_salary",
    F.when(F.col("job_salary").rlike(r'^[\d.,\s]+$'), 
           F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("job_salary"), r'\.', ''), r',', ''), r'\s', '').cast("int")
    ).otherwise(F.lit(None))
)
#     careerviet_recruit_df = careerviet_recruit_df.withColumn( "job_company_id", F.split(F.reverse(F.col("job_company_url")), "\.").getItem(1) # Tách theo dấu '.' và lấy phần tử thứ 2 từ phải qua trái )
# )
#     careerviet_recruit_df = careerviet_recruit_df.withColumn("job_company_id", F.reverse(F.col("job_company_id")))
    if careerviet_mapping_df:
        mapping_df = (
            careerviet_mapping_df.groupBy("careerviet_company_id")
            .agg(
                F.count("corporate_number").alias("count"),
                F.first("corporate_number").alias("corporate_number"),
                F.first("company_name").alias("master_company_name"),
                F.first("province").alias("province"),
            )
            .select("careerviet_company_id", "corporate_number", "master_company_name", "province")
        )
        careerviet_recruit_df = careerviet_recruit_df.join(mapping_df, 
                careerviet_recruit_df["job_company_id"] == mapping_df["careerviet_company_id"]).drop(mapping_df["careerviet_company_id"])
    careerviet_recruit_df.printSchema()
    mapping_df.printSchema()
    media_name="careerviet"
    upsert_recruits_table(
        spark,
        media_name,
        careerviet_recruit_df,
        [
            ColumnInfo("corporate_number", "corporate_number"),
            ColumnInfo("job_id", "media_internal_id"),
            ColumnInfo("media_code", None),
            ColumnInfo("job_company_id", "company_id"),
            ColumnInfo("job_benefits", "benefits"),
            ColumnInfo("job_company_name", "company_name"),
            ColumnInfo("job_education_requirement", "degree"),
            ColumnInfo("job_employment_type", "employment_type"),
            ColumnInfo("job_exp_requirement", "experience"),
            ColumnInfo("job_industry", "industries"),
            ColumnInfo("job_max_salary", "max_salary"),
            ColumnInfo("job_skills", "skills"),
            ColumnInfo("job_title", "title"),
            ColumnInfo("job_description", "description"),
            ColumnInfo("source_job_url", "url"),
            ColumnInfo("province", "province"),
            
            
            
            
            ],
        master_recruit_table,
    )
    
upsert_recruits_table_from_careerviet()