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
def extract_expertise(text_data: str, openai_api_key: str = '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') -> str:
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
Bạn là một chuyên gia phân loại kĩ năng nghề nghiệp. Nhiệm vụ của bạn là phân loại và trích xuất các kĩ năng từ đoạn văn bản dưới đây. Dưới đây là danh sách các kĩ năng mà bạn cần phân loại:
[
    "Ngoại ngữ", 
    "Kỹ năng văn phòng", 
    "Kỹ năng công nghệ thông tin (IT)", 
    "Kỹ năng cơ khí/điện", 
    "Kỹ năng thiết kế đồ họa", 
    "Kỹ năng kế toán", 
    "Kỹ năng quản lý dự án", 
    "Kỹ năng phân tích dữ liệu", 
    "Giao tiếp", 
    "Làm việc nhóm", 
    "Giải quyết vấn đề", 
    "Quản lý thời gian", 
    "Lãnh đạo", 
    "Quản lý xung đột", 
    "Tư duy phản biện", 
    "Tạo động lực", 
    "Tự học và thích ứng", 
    "Quản lý nhân sự", 
    "Quản lý tài chính", 
    "Quản lý chiến lược", 
    "Sáng tạo nội dung", 
    "Nghiên cứu và phát triển (R&D)", 
    "Bán hàng", 
    "Tiếp thị kỹ thuật số", 
    "Dịch vụ khách hàng", 
    "Thuyết trình", 
    "Lắng nghe", 
    "Quan hệ đối ngoại", 
    "Nghiên cứu thị trường", 
    "Phân tích dữ liệu", 
    "Quản lý vận hành", 
    "Quản lý chuỗi cung ứng", 
    "Quản lý cơ sở dữ liệu", 
    "Phát triển phần mềm", 
    "Bảo mật mạng"
]

Nếu có nhiều hơn một kĩ năng trong đoạn văn bản, hãy phân tách chúng bằng dấu phẩy.
Không trả về các kí tự thừa, và không đưa ra giả định. Nếu phân loại của bạn không khớp chính xác, hãy gán nó cho danh mục gần nhất.
Ví dụ về đầu vào và đầu ra: "Ứng viên tốt nghiệp ngành Kinh tế Xây dựng và có kinh nghiệm trên 5 năm" -> "Kỹ năng quản lý dự án, Kỹ năng phân tích dữ liệu, Kỹ năng quản lý tài chính"
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

    df_new = spark.read.format("delta").load("s3a://datn/master/recruit")
    df_new = df_new.withColumn(
    "expertise",
    F.when(F.col("expertise").isNull(), extract_expertise(F.col("description"))).otherwise(F.col("expertise"))
)
    
    # df_new.select("requirements", "expertise").filter(F.col("expertise").isNotNull()).show(20, truncate=False)

    df_new.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://datn/master/recruit")

    
shiet()