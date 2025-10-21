import pyspark.sql.types as T
import random
from enum import IntEnum
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame


# Larger is better
# class IdentifyMethod(IntEnum):
#     # TODO: pending delete
#     DODA_COMPANY_NAME = 70000
#     NTA_COMPANY_NAME = 75000
#     DODA_COMPANY_NAME_CAPITAL = 80000
#     DODA_COMPANY_NAME_REPRESENTATIVE_NAME = 90000
#     IDENTIFIED_HP_URL = 95000
#     NTA_COMPANY_NAME_ADDRESS = 100000
#     # new identify method
#     MASTER_NAME = 75000
#     MASTER_PHONE = 80000
#     MASTER_DOMAIN = 80000
#     MASTER_ADDRESS_PRESIDENT = 85000
#     MASTER_PHONE_PRESIDENT = 90000
#     MASTER_PHONE_ADDRESS = 90000
#     MASTER_DOMAIN_PRESIDENT = 95000
#     MASTER_DOMAIN_POSTAL = 95000
#     MASTER_DOMAIN_ADDRESS = 95000
#     MASTER_DOMAIN_PHONE = 95000
#     MASTER_NAME_PRESIDENT = 95000
#     MASTER_NAME_POSTAL = 100000
#     MASTER_NAME_ADDRESS = 100000
#     MASTER_NAME_PHONE = 100000
#     MASTER_NAME_DOMAIN = 105000
#     MASTER_PHONE_ADDRESS_PRESIDENT = 105000
#     MASTER_DOMAIN_ADDRESS_PRESIDENT = 110000
#     MASTER_DOMAIN_PHONE_PRESIDENT = 110000
#     MASTER_DOMAIN_PHONE_ADDRESS = 110000
#     MASTER_NAME_ADDRESS_PRESIDENT = 110000
#     MASTER_NAME_PHONE_PRESIDENT = 110000
#     MASTER_NAME_PHONE_ADDRESS = 110000
#     MASTER_NAME_DOMAIN_PRESIDENT = 120000
#     MASTER_NAME_DOMAIN_POSTAL = 120000
#     MASTER_NAME_DOMAIN_ADDRESS = 120000
#     MASTER_NAME_DOMAIN_PHONE = 120000
#     MASTER_HEPBURN_COMPANY = 65000

class companyIdentifyMethod(IntEnum):
    MASTER_COMPANY_DOMAIN = 30000
    MASTER_NAME_ADDRESS = 35000
    MASTER_WARD_PROVINCE_NAME = 38000
    MASTER_NAME_DOMAIN = 39000
    MASTER_COMPANY_ONLY_NAME = 40000
    MASTER_ADDRESS_PROVINCE = 45000
    MASTER_ADDRESS_WARD_PROVINCE = 47500
    
from typing import NamedTuple, Optional
import operator
from functools import reduce

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window as W

from datetime import datetime
from typing import Dict
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import DataType
from pendulum import parse as pendulum_parse
from pyspark.sql import SparkSession, DataFrame
from delta import DeltaTable

import uuid

import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Tạo Spark session với cấu hình MinIO và Delta Lake"""
    return SparkSession.builder \
        .appName("MinIO with Delta Lake") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
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
    column: str
    alias: Optional[str]
    quality: Optional[int]
def create_table_from_schema(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    extra_columns: Dict[str, DataType] = {},
    partition_by: list[str] = []
):
    schema = df.schema
    for field in schema.fields:
        field.nullable = True
    for k, v in extra_columns.items():
        schema.add(k, v)
    builder  = DeltaTable.createIfNotExists(spark).location(path).addColumns(schema)
    if len(partition_by) > 0:
        builder.partitionedBy(partition_by)
    return builder.execute()

def merge_schema(spark: SparkSession, table: DeltaTable, df: DataFrame):
    dest = table.toDF()
    table_detail = table.detail().collect()[0]
    missing_columns = set(df.columns).difference(dest.columns)
    if missing_columns:
        col_defs = []
        for col in missing_columns:
            col_defs.append(f"{col} {df.schema[col].dataType.simpleString()}")
        sql = f"ALTER TABLE delta.`{table_detail.location}` ADD COLUMNS ({','.join(col_defs)})"
        spark.sql(sql)


def upsert_companies_table(
    spark: SparkSession,
    media_name: str,
    data_df: DataFrame,
    upsert_cols: list[ColumnInfo],
    master_company_table: str,
):
    all_columns = [F.col("corporate_number")]
    for info in upsert_cols:
        col = info.alias or info.column
        col_type = data_df.select(info.column).schema.fields[0].dataType.typeName()
        data_df = data_df.withColumn(
            f"{col}_quality",
            F.when(
                F.col(info.column).isNotNull() & (
                    F.col(info.column) != F.lit("")
                ),
                F.lit(info.quality),
            ).otherwise(F.lit(0)) if col_type == "string"
            else F.when(
                F.col(info.column).isNotNull() & (
                    F.col(info.column) != F.array()
                ),
                F.lit(info.quality),
            ).otherwise(F.lit(0)) if col_type == "array"
            else F.when(
                F.col(info.column).isNotNull(),
                F.lit(info.quality),
            ).otherwise(F.lit(0)),
        ).withColumn(
            f"{col}_updated_at",
            F.current_timestamp(),
        )
        col_type = data_df.select(info.column).schema.fields[0].dataType.typeName()
        all_columns.append(F.col(info.column).alias(col))
        all_columns.append(F.col(f"{col}_quality"))
        all_columns.append(F.col(f"{col}_updated_at"))
    data_df = data_df.select(*all_columns)
    data_df = data_df.withColumn("updated_at", F.current_timestamp()).withColumn(
        "created_at", F.current_timestamp()
    )

    table = create_table_from_schema(spark, data_df, master_company_table)
    merge_schema(spark, table, data_df)
    table = DeltaTable.forPath(spark, master_company_table)

    dest_table = table.alias("dest")
    dest = dest_table.toDF()

    missing_cols = set(dest.columns) - set(data_df.columns)

    data_df = data_df.withColumns({x: F.lit(None) for x in missing_cols})

    src = data_df.alias("src")


    updated_values = {}
    updated_at_conditions = []
    for info in upsert_cols:
        col = info.alias or info.column
        col_type = src.select(col).schema.fields[0].dataType.typeName()
        quality_condition = (
                dest[f"{col}_quality"].isNull()
                | ((dest[f"{col}_quality"] <= src[f"{col}_quality"]))
            )
        if col_type == "string":
            condition = (
                src[col].isNotNull()
                & (src[col] != F.lit(""))
                & quality_condition
            )
        elif col_type == "array":
            condition = (
                src[col].isNotNull()
                & (src[col] != F.array())
                & quality_condition
            )
        else:
            condition = (
                src[col].isNotNull()
                & quality_condition
            )
        for c in [col, f"{col}_updated_at", f"{col}_quality"]:
            updated_values[c] = F.when(
                condition,
                src[c],
            ).otherwise(dest[c])
        updated_at_conditions.append(condition)
    updated_values["updated_at"] = F.when(
        reduce(operator.or_, updated_at_conditions), src["updated_at"]
    ).otherwise(dest["updated_at"])
    dest_table.merge(
        src, dest["corporate_number"] == src["corporate_number"]
    ).whenNotMatchedInsertAll().whenMatchedUpdate(set=updated_values).execute()
   
    
def extract_unique_corporate_number(
    unidentified_df: DataFrame,
    identified_df: DataFrame,
    conditions: list[Column],
    company_id_column: str = "company_id",
    corporate_number_column: str = "corporate_number",
) -> DataFrame:
    
    identifying = unidentified_df.join(identified_df, conditions)
    return (
        identifying.groupBy(company_id_column)
        .agg(
            F.count(corporate_number_column).alias("_tmp_count"),
            F.first(corporate_number_column).alias(corporate_number_column),
        )
        .filter(F.col("_tmp_count") == 1)
        .groupBy(corporate_number_column)
        .agg(
            F.count(company_id_column).alias("_tmp_count"),
            F.first(company_id_column).alias(company_id_column),
        )
        .filter(F.col("_tmp_count") == 1)
        .select(company_id_column, corporate_number_column)
    )

def match_company_only_name(identifying_df: DataFrame, master_df: DataFrame, column: str = "company_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["company_only_name"] == master_df["company_only_name"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(companyIdentifyMethod.MASTER_COMPANY_ONLY_NAME.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df
def match_company_name_address(identifying_df: DataFrame, master_df: DataFrame, column: str = "company_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["street"] == master_df["street"],
                identifying_df["company_only_name"] == master_df["company_only_name"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(companyIdentifyMethod.MASTER_NAME_ADDRESS.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df

def match_company_domain(identifying_df: DataFrame, master_df: DataFrame, column: str = "company_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["company_domain"] == master_df["company_domain"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(companyIdentifyMethod.MASTER_COMPANY_DOMAIN.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df
# def match_company_ward(identifying_df: DataFrame, master_df: DataFrame, column: str = "company_id") -> DataFrame:
#     matched_df = (
#         extract_unique_corporate_number(
#             identifying_df,
#             master_df,
#             [
#                 identifying_df["ward"] == master_df["ward"],
#             ],
#             column,
#             "corporate_number"
#         )
#         .withColumn(
#             "identify_method",
#             F.lit(companyIdentifyMethod.MASTER_WARD.value),
#         )
#         .withColumn("updated_at", F.current_timestamp())
#         .withColumn("created_at", F.current_timestamp())
#     )
#     return matched_df
def match_company_ward_province_name(identifying_df: DataFrame, master_df: DataFrame, column: str = "company_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["ward"] == master_df["ward"],
                identifying_df["province"] == master_df["province"],
                identifying_df["company_only_name"] == master_df["company_only_name"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(companyIdentifyMethod.MASTER_WARD_PROVINCE_NAME.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df
def match_company_name_domain(identifying_df: DataFrame, master_df: DataFrame, column: str = "company_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["company_only_name"] == master_df["company_only_name"],
                identifying_df["company_domain"] == master_df["company_domain"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(companyIdentifyMethod.MASTER_NAME_DOMAIN.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df

def match_province_street_address(identifying_df: DataFrame, master_df: DataFrame, column: str = "company_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["province"] == master_df["province"],
                identifying_df["street"] == master_df["street"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(companyIdentifyMethod.MASTER_ADDRESS_PROVINCE.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df

def match_address_ward_province(identifying_df: DataFrame, master_df: DataFrame, column: str = "company_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["ward"] == master_df["ward"],
                identifying_df["province"] == master_df["province"],
                identifying_df["street"] == master_df["street"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(companyIdentifyMethod.MASTER_ADDRESS_WARD_PROVINCE.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df

