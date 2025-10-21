import pyspark.sql.functions as F
import pyspark.sql.types as T

from match_company import create_spark_session
import openai  
from openai import OpenAI

# from match_company import create_spark_session
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
def extract_industry(text_data: str, openai_api_key: str = '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') -> str:
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
Bạn là một chuyên gia phân loại lĩnh vực hành nghề của các công ty. Nhiệm vụ của bạn là phân loại và sắp xếp các lĩnh vực hành nghề từ đoạn văn bản mô tả dưới đây. Dưới đây là danh sách các lĩnh vực hành nghề mà bạn cần phân loại:
[
    "Công nghệ thông tin (IT)",
    "Sản xuất",
    "Tài chính và ngân hàng",
    "Thương mại điện tử",
    "Dịch vụ chuyên môn",
    "Xây dựng và bất động sản",
    "Y tế và dược phẩm",
    "Giáo dục và đào tạo",
    "Vận tải và logistics",
    "Năng lượng và tài nguyên",
    "Nông nghiệp",
    "Du lịch và khách sạn",
    "Tiêu dùng",
    "Truyền thông và giải trí",
    "Môi trường và bảo vệ thiên nhiên"
];
.
Không trả về các kí tự thừa, và không đưa ra giả định. Nếu phân loại của bạn không khớp chính xác, hãy gán nó cho danh mục gần nhất.
Ví dụ về đầu vào và đầu ra: "Công ty hoạt động từ năm 2015, chuyên hoạt động trong lĩnh vực vận chuyển hàng hóa" -> "Vận tải và logistics"
Note: STRICTLY return only the string, DO NOT return JSON.
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





from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta.tables import DeltaTable  # Import DeltaTable

def shiet():
    # Tạo Spark session
    spark = create_spark_session()

    df_new = spark.read.format("delta").load("s3a://datn/master/company")
    df_new = df_new.withColumn(
    "category",
    F.when(extract_industry(F.col("description")).isNotNull(), extract_industry(F.col("description"))).otherwise(F.lit(None))
)
    

    df_new.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://datn/master/recruit")

    
shiet()