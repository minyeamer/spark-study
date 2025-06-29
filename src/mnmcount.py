from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # SparkSession 객체를 만든다.
    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())

    # 인자에서 M&M 데이터가 들어있는 파일 이름을 얻는다.
    mnm_file = sys.argv[1]

    # 데이터가 CSV 형식이며 헤더가 있음을 알리고 스키마를 추론하도록 한다.
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))
    mnm_df.show(n=5, truncate=False)

    # State, Color, Count 필드를 선택하고 State, Color를 기준으로 Count를 sum 집계한다.
    # select, groupBy, sum, orderBy 메서드를 연결하여 연속적으로 호출한다.
    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # 상위 60개 결과를 보여주고, 모든 행 개수를 count 집계해 출력한다.
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # 위 집계 과정에서 중간에 where 메서드를 추가해 캘리포니아(CA) 주에 대해서만 집계한다.
    ca_count_mnm_df = (mnm_df.select("*")
                        .where(mnm_df.State == 'CA')
                        .groupBy("State", "Color")
                        .sum("Count")
                        .orderBy("sum(Count)", ascending=False))

    # 상위 10개 결과를 보여준다.
    ca_count_mnm_df.show(n=10, truncate=False)

    # SparkSession을 멈춘다.
    spark.stop()