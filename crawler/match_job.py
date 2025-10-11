
from typing import Optional, NamedTuple
from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pendulum.parser import parse as pendulum_parse


from timviec365.upsert_master_companies import create_table_from_schema, merge_schema

from pyspark.sql import functions as F

def create_spark_session():
    """Tạo Spark session với cấu hình MinIO và Delta Lake"""
    return SparkSession.builder \
        .appName("MinIO with Delta Lake") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
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

class ColumnInfo(NamedTuple):
    name: str
    alias: Optional[str]

def upsert_recruits_table(
    spark: SparkSession,
    media_name: str,
    recruit_df: DataFrame,
    upsert_cols: list[ColumnInfo],
    master_recruit_table: str,
):
    cols = []
    for col in upsert_cols:
        alias = col.alias or col.name
        cols.append(F.col(col.name).alias(alias))

    recruit_df = recruit_df.select(*cols)

    df = recruit_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

    table = create_table_from_schema(spark, df, master_recruit_table)
    merge_schema(spark, table, df)
    table = DeltaTable.forPath(spark, master_recruit_table)
    table_df = table.toDF()

    missing_cols = set(table_df.columns) - set(df.columns)

    df = df.withColumns({x: F.lit(None) for x in missing_cols})




    cols = {}

    for col in table_df.columns:
        if col == "created_at":
            continue
        if col == "_ingest_id":
            cols[col] = F.when(table_df[col].isNotNull(), table_df[col]).otherwise(df[col])
            continue
        cols[col] = F.when(df[col].isNotNull(), df[col]).otherwise(table_df[col])

    table.merge(
        df, (table_df["media_internal_id"] == df["media_internal_id"]) & (table_df["media_code"] == df["media_code"])
    ).whenNotMatchedInsertAll().whenMatchedUpdate(
        set=cols,
    ).execute()
    
def upsert_recruits_table_from_timviec365(
    master_recruit_table: str = 's3a://datn/master/recruit',
    recruit_table: str = 's3a://datn/raw/timviec365/recruit',
    master_company_table: str = 's3a://datn/master/company',
):

    spark = create_spark_session()

    timviec365_recruit_df = spark.read.format("delta").load(recruit_table)
    timviec365_mapping_df = spark.read.format("delta").load(master_company_table)

    timviec365_recruit_df = timviec365_recruit_df.withColumn("media_code", F.lit("timviec365"))

    if timviec365_mapping_df:
        mapping_df = (
            timviec365_mapping_df.groupBy("timviec365_company_id")
            .agg(
                F.count("corporate_number").alias("count"),
                F.first("corporate_number").alias("corporate_number"),
                F.first("company_name").alias("master_company_name"),
            )
            .select("timviec365_company_id", "corporate_number", "master_company_name")
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
            ],
        master_recruit_table,
    )
    
upsert_recruits_table_from_timviec365()