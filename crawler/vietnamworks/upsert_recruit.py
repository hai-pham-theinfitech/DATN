from match_company import create_spark_session
import pyspark.sql.functions as F
from match_job import ColumnInfo, upsert_recruits_table

def upsert_recruits_table_from_vietnamworks(
    master_recruit_table: str = 's3a://datn/master/recruit',
    recruit_table: str = 's3a://datn/raw/vietnamworks/recruit',
    master_company_table: str = 's3a://datn/master/company',
):

    spark = create_spark_session()

    vietnamworks_recruit_df = spark.read.format("delta").load(recruit_table)
    vietnamworks_mapping_df = spark.read.format("delta").load(master_company_table)

    from pyspark.sql import functions as F

# Chuyển đổi kiểu dữ liệu từ long sang decimal(10,2)
    vietnamworks_recruit_df = vietnamworks_recruit_df.withColumn("job_salary_max", F.col("job_salary_max").cast("decimal(10,2)"))
    vietnamworks_recruit_df = vietnamworks_recruit_df.withColumn("job_salary_min", F.col("job_salary_min").cast("decimal(10,2)"))

    vietnamworks_recruit_df = vietnamworks_recruit_df.withColumn("media_code", F.lit("vietnamworks"))
    vietnamworks_recruit_df = vietnamworks_recruit_df.withColumn("job_industry", F.array("job_industry"))
    vietnamworks_recruit_df = vietnamworks_recruit_df.withColumn(
    "job_id", 
    F.col("job_id").cast("string")  
)
    
    vietnamworks_recruit_df =  vietnamworks_recruit_df.withColumn("job_benefits", F.concat_ws(", ", F.col("job_benefits")))
    vietnamworks_recruit_df.printSchema()
 # Chia theo dấu phẩy
    if vietnamworks_mapping_df:
        mapping_df = (
            vietnamworks_mapping_df.groupBy("vietnamworks_company_id")
            .agg(
                F.count("corporate_number").alias("count"),
                F.first("corporate_number").alias("corporate_number"),
                F.first("company_name").alias("master_company_name"),
                F.first("province").alias("province"),
            )
            .select("vietnamworks_company_id", "corporate_number", "master_company_name", "province")
        )
        vietnamworks_recruit_df = vietnamworks_recruit_df.join(mapping_df, 
                vietnamworks_recruit_df["job_company_id"] == mapping_df["vietnamworks_company_id"]).drop(mapping_df["vietnamworks_company_id"])
    # vietnamworks_recruit_df.printSchema()
    media_name="vietnamworks"
    upsert_recruits_table(
        spark,
        media_name,
        vietnamworks_recruit_df,
        [   
            ColumnInfo("province", "province"),
            ColumnInfo("job_id", "media_internal_id"),
            ColumnInfo("media_code", "media_code"),
            ColumnInfo("job_benefits", "benefits"),
            ColumnInfo("job_company_id", "company_id"),
            ColumnInfo("job_company_name", "company_name"),
            ColumnInfo("job_description", "description"),
            ColumnInfo("job_expired_at", "end_at"),
            # ColumnInfo("job_industry", "industries"),
            ColumnInfo("job_posted_at", "posted_at"),
            ColumnInfo("job_requirement", "requirements"),
            ColumnInfo("job_salary_min", "min_salary"),
            ColumnInfo("job_salary_max", "max_salary"),  
            ColumnInfo("job_title", "title"),
            ColumnInfo("job_url", "url"),
            ColumnInfo("corporate_number", "corporate_number"),
            # ColumnInfo("job_skills", "skills"),
            
           
            
            
            ],
        master_recruit_table,
    )
    
upsert_recruits_table_from_vietnamworks()