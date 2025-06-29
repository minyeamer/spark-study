from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 프로그래밍 스타일로 스키마를 정의한다.
schema = StructType([
    StructField("Id", IntegerType(), False),
    StructField("First", StringType(), False),
    StructField("Last", StringType(), False),
    StructField("Url", StringType(), False),
    StructField("Published", StringType(), False),
    StructField("Hits", IntegerType(), False),
    StructField("Campaigns", ArrayType(StringType()), False)])

# DDL을 사용해서 스키마를 정의할 수도 있다.
# schema = "'Id' INT, 'First', STRING, 'Last' STRING, 'Url' STRING, " \
#         "'Published' STRING, 'Hits' INT, 'Campaigns' ARRAY<STRING>"

# 예제 데이터를 생성한다.
data = [
    [1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
    [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
    [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
    [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
    [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
    [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]]

if __name__ == "__main__":
    spark = (SparkSession
        .builder
        .appName("Example-3_6")
        .getOrCreate())

    # 위에서 정의한 스키마로 DataFrame을 생성하고 상위 행을 출력한다.
    blogs_df = spark.createDataFrame(data, schema)
    blogs_df.show()

    # DataFrame 처리에 사용된 스키마를 출력한다.
    print(blogs_df.printSchema())

    # 표현식을 사용해 값을 계산하고 결과를 출력한다. 모두 동일한 결과를 보여준다.
    blogs_df.select(expr("Hits") * 2).show(2)
    blogs_df.select(col("Hits") * 2).show(2)
    blogs_df.select(expr("Hits * 2")).show(2)

    # 블로그 우수 방문자를 계산하고 결과를 출력한다.
    blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    spark.stop()