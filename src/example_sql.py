from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# 데이터 경로
csv_file = "data/flights/departuredelays.csv"

# 스키마를 추론하여 데이터를 읽기
df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))

# 데이터로부터 임시뷰를 생성
df.createOrReplaceTempView("delay_flights")

# SQL 쿼리 실행
spark.sql("""
SELECT distance, origin, destination
FROM delay_flights
WHERE distance > 1000
ORDER BY distance DESC;""").show(10)

# SQL 쿼리를 DataFrame API로 표현
from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
    .where(col("distance") > 1000)
    .orderBy(desc("distance"))).show(10)

# 임시뷰 목록 조회
spark.sql("SHOW TABLES;").show()

# 테이블 스키마 조회
spark.sql("DESC delay_flights;").show()

# 데이터 베이스 생성
spark.sql("CREATE DATABASE learn_spark_db;")
spark.sql("USE learn_spark_db;")
spark.sql("SELECT current_database();").show()

# 관리형 테이블 생성
table = "managed_us_delay_flights_tbl"
schema = "date STRING, delay INT, distaince INT, origin STRING, destination STRING"
spark.sql("CREATE TABLE {} ({});".format(table, schema))

# 관리형 테이블 생성 (DataFrame API)
csv_file = "data/flights/departuredelays.csv"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable(table, mode="overwrite")

# 비관리형 테이블 생성
import os

table = "unmanaged_us_delay_flights_tbl"
schema = "date STRING, delay INT, distaince INT, origin STRING, destination STRING"
csv_file = os.path.join(os.getcwd(), "data/flights/departuredelays.csv")

spark.sql("CREATE TABLE {} ({}) USING csv OPTIONS (PATH '{}');".format(table, schema, csv_file))

# 비관리형 테이블 생성 (DataFrame API)
(flights_df
	.write
    .option("path", "/tmp/data/us_flights_delay")
    .saveAsTable(table, mode="overwrite"))

# 뷰 생성
table = "us_origin_airport_SFO"
spark.sql("""
CREATE OR REPLACE GLOBAL TEMP VIEW {} AS
SELECT date, delay, origin, destination FROM delay_flights
WHERE origin = 'SFO';
""".format(table))

# 뷰 생성 (DataFrame API)
from pyspark.sql.functions import col
table = "us_origin_airport_SFO"
(df.select("date", "delay", "origin", "destination")
    .where(col("origin") == "SFO")
    .createOrReplaceGlobalTempView(table))

spark.sql("SELECT * FROM global_temp.{};".format(table)).show(5)

# 메타데이터
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("delay_flights")

# 테이블 캐싱
spark.sql("CACHE TABLE delay_flights;")
spark.catalog.isCached("delay_flights")

# 캐시 삭제
spark.sql("UNCACHE TABLE delay_flights;")
spark.catalog.isCached("delay_flights")

# DataFrame 변환
spark.table("delay_flights")