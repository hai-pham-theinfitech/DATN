import pyspark.sql.functions as F
import pyspark.sql.types as T

# from match_company import create_spark_session
import openai  
from openai import OpenAI

from match_company import create_spark_session
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkContext  # Import SparkContext
from pyspark.sql import SparkSession
from delta.tables import DeltaTable  # Import DeltaTable


# sc = SparkContext.getOrCreate()
# spark = SparkSession(sc)
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark import SparkContext  # Import SparkContext
from pyspark.sql import SparkSession
import openai  # Import the openai module
from openai import OpenAI



@F.udf(T.StringType())
def extract_from_pdf(text_data: str, openai_api_key: str = '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') -> str:
    try:
        

        client = OpenAI(
  base_url="https://openrouter.ai/api/v1",
  api_key=openai_api_key,
)
        completion = client.chat.completions.create(
            model="deepseek/deepseek-v3.2-exp",
            messages=[
                    {
                        "role": "user",
                        "content" : f"""
I need you to classify the text below into one of the following job categories:

["Information Technology (IT)", "Business and Finance", "Healthcare", "Education", "Engineering", "Culture - Creativity", "Legal", "Customer Service", "Agriculture and Environment", "Sales", "Food and Beverage", "Media"]

Please classify the text below and return one of the results from the list above.
Do not add extra characters, do not reason, and do not make assumptions. If your classification does not match exactly, please assign it to the nearest category.

Note: STRICTLY return only the string, DO NOT return JSON.

Example: Hotel Receptionist => Customer Service, Salesperson => Sales, Accountant => Business and Finance, Programmer => Information Technology (IT)

Once again, please make sure to ONLY return the string, DO NOT return JSON.

Text:
{text_data}
"""        
}
                ],
            temperature=1
        )

        parsed = completion.choices[0].message.content
        
        return parsed

    except Exception as e:
        print(f"[Structured output error] {e}")
        return str(e)



# def create_spark_session():
#     """Tạo Spark session với cấu hình MinIO và Delta Lake"""
#     return SparkSession.builder \
#         .appName("MinIO with Delta Lake") \
#         .master("local[*]") \
#         .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
#         .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#         .config("spark.jars.packages", 
#                 "io.delta:delta-core_2.12:1.0.0,"  # Cập nhật phiên bản Delta phù hợp
#                 "org.apache.hadoop:hadoop-aws:3.3.4,"
#                 "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
#         .getOrCreate()


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable  # Import DeltaTable

def sh():
    
    spark = create_spark_session()
    df= spark.read.format("delta").load("s3a://datn/master/recruit")

    df.printSchema()

    # df = df.withColumn('employment_type_str', F.concat_ws('/', F.col('employment_type')))

    # df.write \
    #     .format("delta") \
    #     .mode("overwrite") \
    #     .option("overwriteSchema", "true") \
    #     .save("s3a://datn/master/recruit")



    
sh()