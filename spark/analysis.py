from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, length, when, regexp_extract
from pyspark.sql.window import Window
import time

spark = SparkSession.builder.appName("DoubanAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

path = "file:///opt/spark/work/douban_movies.csv"

print("=== 数据加载 ===")
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .csv(path)
)

print("=== Schema ===")
df.printSchema()

print("=== 前5行 ===")
df.show(5, truncate=False)

total = df.count()
print("原始数据总行数:", total)

print("=== 缺失值比例 ===")
for c in df.columns:
    missing = df.filter(col(c).isNull() | (length(trim(col(c).cast("string"))) == 0)).count()
    rate = missing / total if total > 0 else 0
    print(f"{c}: 缺失 {missing} 行, 缺失比例 {rate:.4f}")

clean_df = df.dropDuplicates()

clean_df = clean_df.filter(
    col("title").isNotNull()
    & (length(trim(col("title").cast("string"))) > 0)
    & col("rating_score").isNotNull()
)

clean_df = clean_df.withColumn("rating_num", col("rating_score").cast("double"))
clean_df = clean_df.filter(col("rating_num").isNotNull())

clean_df = clean_df.withColumn(
    "year_num",
    regexp_extract(col("year").cast("string"), r"(\d{4})", 1).cast("int")
)

clean_df = clean_df.withColumn(
    "genre_clean",
    when(col("genres").isNull() | (length(trim(col("genres").cast("string"))) == 0), "Unknown")
    .otherwise(trim(col("genres").cast("string")))
)

clean_df = clean_df.withColumn(
    "country_clean",
    when(col("countries").isNull() | (length(trim(col("countries").cast("string"))) == 0), "Unknown")
    .otherwise(trim(col("countries").cast("string")))
)

clean_df = clean_df.withColumn(
    "director_clean",
    when(col("directors").isNull() | (length(trim(col("directors").cast("string"))) == 0), "Unknown")
    .otherwise(trim(col("directors").cast("string")))
)

clean_df = clean_df.withColumn("title_clean", col("title").cast("string"))

before_count = total
after_count = clean_df.count()

print("=== 清洗结果 ===")
print("清洗前行数:", before_count)
print("清洗后行数:", after_count)
print("删除行数:", before_count - after_count)

print("=== 清洗后统计信息 ===")
clean_df.select("rating_num", "year_num").describe().show()

print("=== 清洗后前5行 ===")
clean_df.select(
    "title_clean", "rating_num", "year_num",
    "genre_clean", "country_clean", "director_clean"
).show(5, truncate=False)

clean_df.createOrReplaceTempView("movies")

print("=== A-2 查询1：按电影类型统计数量和平均评分 GROUP BY ===")
q1 = spark.sql("""
SELECT genre_clean AS genre,
       COUNT(*) AS movie_count,
       ROUND(AVG(rating_num), 2) AS avg_rating,
       ROUND(MAX(rating_num), 2) AS max_rating,
       ROUND(MIN(rating_num), 2) AS min_rating
FROM movies
GROUP BY genre_clean
ORDER BY movie_count DESC
LIMIT 20
""")
q1.show(20, truncate=False)

print("=== A-2 查询2：评分最高 Top 10 电影 ORDER BY + LIMIT ===")
q2 = spark.sql("""
SELECT title_clean AS title,
       rating_num AS rating,
       year_num AS year,
       genre_clean AS genre,
       country_clean AS country,
       director_clean AS director
FROM movies
ORDER BY rating_num DESC
LIMIT 10
""")
q2.show(10, truncate=False)

print("=== A-2 查询3：按年份统计电影数量和平均评分 时间趋势 ===")
q3 = spark.sql("""
SELECT year_num AS year,
       COUNT(*) AS movie_count,
       ROUND(AVG(rating_num), 2) AS avg_rating
FROM movies
WHERE year_num IS NOT NULL
GROUP BY year_num
ORDER BY year_num
LIMIT 50
""")
q3.show(50, truncate=False)

print("=== A-2 查询4：窗口函数，每年评分最高电影 ===")
q4 = spark.sql("""
SELECT year, title, rating, genre, director
FROM (
    SELECT year_num AS year,
           title_clean AS title,
           rating_num AS rating,
           genre_clean AS genre,
           director_clean AS director,
           ROW_NUMBER() OVER(PARTITION BY year_num ORDER BY rating_num DESC) AS rn
    FROM movies
    WHERE year_num IS NOT NULL
) t
WHERE rn = 1
ORDER BY year
LIMIT 50
""")
q4.show(50, truncate=False)

print("=== A-3 PySpark 查询耗时 ===")
start = time.time()
spark.sql("""
SELECT genre_clean,
       COUNT(*) AS movie_count,
       AVG(rating_num) AS avg_rating
FROM movies
GROUP BY genre_clean
ORDER BY movie_count DESC
""").collect()
end = time.time()

print(f"PySpark query time: {end - start:.4f} seconds")

spark.stop()