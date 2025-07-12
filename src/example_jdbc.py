from pyspark.sql import SparkSession
import os

# PostgreSQL JDBC 드라이버 추가
SPARK_HOME = os.environ.get("SPARK_HOME")
spark = (SparkSession
    .builder
    .config("spark.driver.extraClassPath", f"{SPARK_HOME}/jars/postgresql-42.7.7.jar") \
    .appName("PostgresExample")
    .getOrCreate())

# PostgreSQL 데이터 읽기
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "spark_schema.users") \
    .option("user", "spark") \
    .option("password", "spark") \
    .load()
df.show()

# PostgreSQL 데이터 쓰기 (새 테이블)
df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "spark_schema.new_users") \
    .option("user", "spark") \
    .option("password", "spark") \
    .save()

# PostgreSQL 데이터 쓰기 (기존 테이블)
df.select("name").write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "spark_schema.users") \
    .option("user", "spark") \
    .option("password", "spark") \
    .mode("append") \
    .save()


# MySQL 연결
SPARK_HOME = os.environ.get("SPARK_HOME")
spark = (SparkSession
    .builder
    .config("spark.driver.extraClassPath", f"{SPARK_HOME}/jars/mysql-connector-j-8.4.0.jar") \
    .appName("MySQLExample")
    .getOrCreate())

# MySQL 데이터 읽기
df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/spark_db") \
    .option("dbtable", "users") \
    .option("user", "spark") \
    .option("password", "spark") \
    .load()
df.show()

# MySQL 데이터 쓰기 (새 테이블)
df.write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/spark_db") \
    .option("dbtable", "new_users") \
    .option("user", "spark") \
    .option("password", "spark") \
    .save()

# MySQL 데이터 쓰기 (기존 테이블)
df.select("name").write.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/spark_db") \
    .option("dbtable", "users") \
    .option("user", "spark") \
    .option("password", "spark") \
    .mode("append") \
    .save()