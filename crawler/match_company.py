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

class PersonIdentifyMethod(IntEnum):
    MASTER_COMPANY_DOMAIN = 30000
    MASTER_PROVINCE = 35000
    MASTER_WARD = 37500
    MASTER_WARD_PROVINCE = 38000
    MASTER_NAME_DOMAIN = 39000
    MASTER_COMPANY_ONLY_NAME = 40000
    MASTER_ADDRESS_PROVINCE = 45000
    MASTER_ADDRESS_WARD_PROVINCE = 47500
    
    
    
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

def match_company_only_name(identifying_df: DataFrame, master_df: DataFrame, column: str = "person_id") -> DataFrame:
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
            F.lit(PersonIdentifyMethod.MASTER_COMPANY_ONLY_NAME.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df
def match_company_province(identifying_df: DataFrame, master_df: DataFrame, column: str = "person_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["province"] == master_df["province"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(PersonIdentifyMethod.MASTER_COMPANY_ONLY_NAME.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df

def match_company_domain(identifying_df: DataFrame, master_df: DataFrame, column: str = "person_id") -> DataFrame:
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
            F.lit(PersonIdentifyMethod.MASTER_COMPANY_ONLY_NAME.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df
def match_company_ward(identifying_df: DataFrame, master_df: DataFrame, column: str = "person_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["ward"] == master_df["ward"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(PersonIdentifyMethod.MASTER_WARD.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df
def match_company_ward_province(identifying_df: DataFrame, master_df: DataFrame, column: str = "person_id") -> DataFrame:
    matched_df = (
        extract_unique_corporate_number(
            identifying_df,
            master_df,
            [
                identifying_df["ward"] == master_df["ward"],
                identifying_df["province"] == master_df["province"],
            ],
            column,
            "corporate_number"
        )
        .withColumn(
            "identify_method",
            F.lit(PersonIdentifyMethod.MASTER_WARD_PROVINCE.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df
def match_company_name_domain(identifying_df: DataFrame, master_df: DataFrame, column: str = "person_id") -> DataFrame:
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
            F.lit(PersonIdentifyMethod.MASTER_NAME_DOMAIN.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df

def match_province_street_address(identifying_df: DataFrame, master_df: DataFrame, column: str = "person_id") -> DataFrame:
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
            F.lit(PersonIdentifyMethod.MASTER_NAME_DOMAIN.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df

def match_address_ward_province(identifying_df: DataFrame, master_df: DataFrame, column: str = "person_id") -> DataFrame:
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
            F.lit(PersonIdentifyMethod.MASTER_ADDRESS_WARD_PROVINCE.value),
        )
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
    )
    return matched_df

