import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, count

# 1. Параметры доступа к PostgreSQL
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "tpcds")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASSWORD = os.getenv("PGPASSWORD", "postgres")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# 2. Инициализация SparkSession с локальным каталогом логов событий
LOG_DIR = os.path.abspath("logs/spark_events")
os.makedirs(LOG_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("T12_JDBC_Verification")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", f"file://{LOG_DIR}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("1. Получение эталонных метрик напрямую из PostgreSQL")
print("=" * 60)

conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

cur.execute("SELECT count(*) FROM date_dim;")
pg_date_dim = cur.fetchone()[0]

cur.execute("SELECT count(*) FROM item;")
pg_item = cur.fetchone()[0]

PART_COL = "ss_sold_date_sk"
cur.execute(f"SELECT MIN({PART_COL}), MAX({PART_COL}), COUNT(*), COUNT(*) - COUNT({PART_COL}) FROM store_sales;")
min_val, max_val, pg_store_sales, null_cnt = cur.fetchone()

cur.close()
conn.close()

print(f"PG date_dim:          {pg_date_dim}")
print(f"PG item:              {pg_item}")
print(f"PG store_sales:       {pg_store_sales}")
print(f"Диапазон {PART_COL}:  MIN={min_val}, MAX={max_val}, NULLs={null_cnt}")

print("\n" + "=" * 60)
print("2. Чтение измерений date_dim и item (1 поток / раздел)")
print("=" * 60)

df_date_dim = spark.read.jdbc(url=JDBC_URL, table="date_dim", properties=JDBC_PROPS)
df_item = spark.read.jdbc(url=JDBC_URL, table="item", properties=JDBC_PROPS)

spark_date_cnt = df_date_dim.count()
spark_item_cnt = df_item.count()

assert spark_date_cnt == pg_date_dim, f"Расхождение в date_dim: {spark_date_cnt} != {pg_date_dim}"
assert spark_item_cnt == pg_item, f"Расхождение в item: {spark_item_cnt} != {pg_item}"

print(f"[OK] date_dim сошлась: {spark_date_cnt} строк (разделов: {df_date_dim.rdd.getNumPartitions()})")
print(f"[OK] item сошлась:     {spark_item_cnt} строк (разделов: {df_item.rdd.getNumPartitions()})")

print("\n" + "=" * 60)
print("3. Согласованное параллельное чтение store_sales")
print("=" * 60)

NUM_PARTITIONS = 8
FETCH_SIZE = "50000"

df_store_sales = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "store_sales")
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .option("partitionColumn", PART_COL)
    .option("lowerBound", str(min_val))
    .option("upperBound", str(max_val))
    .option("numPartitions", str(NUM_PARTITIONS))
    .option("fetchsize", FETCH_SIZE)
    .load()
)

actual_partitions = df_store_sales.rdd.getNumPartitions()
spark_ss_cnt = df_store_sales.count()

print(f"Число разделов: {actual_partitions}")
print(f"Строк в Spark:  {spark_ss_cnt}")

assert spark_ss_cnt == pg_store_sales, f"Расхождение в store_sales: {spark_ss_cnt} != {pg_store_sales}"
assert actual_partitions == NUM_PARTITIONS, f"Число разделов {actual_partitions} != {NUM_PARTITIONS}"

print("[OK] store_sales: количество строк полностью совпадает с PostgreSQL!")

print("\n" + "=" * 60)
print("4. Проверка балансировки партиций (Data Skew)")
print("=" * 60)

skew_check = (
    df_store_sales.groupBy(spark_partition_id().alias("partition_id"))
    .agg(count("*").alias("row_count"))
    .orderBy("partition_id")
)
skew_check.show(NUM_PARTITIONS, truncate=False)

spark.stop()
print("=" * 60)
print("Валидация T12 завершена успешно!")
print("=" * 60)