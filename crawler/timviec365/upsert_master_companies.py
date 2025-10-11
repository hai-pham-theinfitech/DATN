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

from utils import get_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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


def upsert_persons_table(
    spark: SparkSession,
    media_name: str,
    data_df: DataFrame,
    upsert_cols: list[ColumnInfo],
    master_person_table: str,
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

    table = create_table_from_schema(spark, data_df, master_person_table)
    merge_schema(spark, table, data_df)
    table = DeltaTable.forPath(spark, master_person_table)

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


def upsert_persons_from_linkedin(
    master_person_table: str = "s3a://datn/master/company",
    person_table: str = "s3a://datn/raw/timviec365/company",
    uuid_table: str = "s3a://datn/uuid/timviec365",
):
    spark = get_spark_session()
    person_df = spark.read.format("delta").load(person_table)
    media_name = "timviec365"
    quality = 10
    
    if DeltaTable.isDeltaTable(spark, uuid_table):
        uuid_df = spark.read.format("delta").load(uuid_table)
        uuid_df = uuid_df.select("company_id", "corporate_number")
        
        # Join person_df với uuid_df để lấy corporate_number
        person_with_uuid = person_df.join(
            uuid_df, 
            person_df["company_id"] == uuid_df["company_id"], 
            "left"
        ).drop(uuid_df["company_id"])
        
        person_with_uuid = person_with_uuid.withColumn(
            "corporate_number",
            F.when(
                F.col("corporate_number").isNull(),
                F.sha2(F.coalesce(F.col("company_only_name").cast("string"), F.lit("")), 256)
            ).otherwise(F.col("corporate_number"))
        )
    
    else:
        person_with_uuid = person_df.withColumn(
            "corporate_number",
            F.sha2(F.coalesce(F.col("company_only_name").cast("string"), F.lit("")), 256)
        )
    
    # Thay person_df bằng person_with_uuid khi gọi upsert_persons_table
    upsert_persons_table(
        spark,
        media_name,
        person_with_uuid,  # Đổi từ person_df thành person_with_uuid
        [
            ColumnInfo("company_id", "timviec365_company_id", quality),
            ColumnInfo("company_name", None, quality),
        ],  
        master_person_table,
    )
upsert_persons_from_linkedin()