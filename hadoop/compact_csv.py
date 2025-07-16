from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

# --- Khởi tạo Spark ---
spark = SparkSession.builder \
    .appName("CompactSmallFiles") \
    .getOrCreate()

# --- Lấy danh sách file CSV thực tế trong /transactions ---
hadoop_conf = spark._jsc.hadoopConfiguration()
java_import(spark._jvm, "org.apache.hadoop.fs.Path")
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

transactions_path = spark._jvm.Path("hdfs://localhost:9000/transactions")
file_statuses = fs.listStatus(transactions_path)

# Lọc ra danh sách file .csv thật sự (ví dụ: part-00000.csv)
csv_paths = [
    str(f.getPath())
    for f in file_statuses
    if f.getPath().getName().endswith(".csv") and f.getPath().getName().startswith("part-")
]

# Nếu không có file nào thì kết thúc luôn
if not csv_paths:
    print("Không có file .csv nào để gộp trong /transactions.")
    spark.stop()
    exit()


# --- Đọc và gộp các file ---
df = spark.read.option("header", "false").csv(csv_paths)

# Gộp thành 1 file duy nhất và ghi vào thư mục compacted
df.coalesce(1) \
  .write \
  .mode("append") \
  .option("header", "false") \
  .csv("hdfs://localhost:9000/transactions_compacted")

# --- XÓA các file .csv trong thư mục gốc ---
for status in file_statuses:
    file_path = status.getPath()
    file_name = file_path.getName()

    # Chỉ xóa các file part-*.csv
    if file_name.endswith(".csv") and file_name.startswith("part-"):
        fs.delete(file_path, False)
        print(f"🗑️ Đã xóa: {file_name}")
# --- Kết thúc ---
spark.stop()
