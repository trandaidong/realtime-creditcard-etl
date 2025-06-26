from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CompactSmallFiles") \
    .getOrCreate()

# Đọc toàn bộ dữ liệu đã ghi từ streaming (dưới dạng CSV)
df = spark.read.option("header", "false") \
    .csv("hdfs://localhost:9000/transactions_by_hour")

# Gộp lại thành 1 file (hoặc n file nếu dùng repartition(n))
df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "false") \
    .csv("hdfs://localhost:9000/transactions_by_hour_compacted")

spark.stop()