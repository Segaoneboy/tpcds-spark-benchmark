#!/usr/bin/env python3
import os
import time
import psycopg2
from pyspark.sql import SparkSession

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

OUTPUT_BASE_DIR = "data/formats"

print(">>> [1/4] Получение диапазонов партиционирования из PostgreSQL...")
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

PART_COL = "ss_sold_date_sk"
cur.execute(f"SELECT MIN({PART_COL}), MAX({PART_COL}), COUNT(*) FROM store_sales;")
min_val, max_val, pg_store_sales = cur.fetchone()

cur.close()
conn.close()

print(f"Диапазон {PART_COL}: MIN={min_val}, MAX={max_val}, всего строк: {pg_store_sales}")

print("\n>>> [2/4] Запуск SparkSession...")
spark = (
    SparkSession.builder
    .appName("W2_ETL_Formats_Pipeline")
    .master("local[*]")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-avro_2.12:3.5.4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n>>> [3/4] Чтение store_sales и справочников через JDBC...")
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

df_date_dim = spark.read.jdbc(url=JDBC_URL, table="date_dim", properties=JDBC_PROPS)
df_item = spark.read.jdbc(url=JDBC_URL, table="item", properties=JDBC_PROPS)

df_date_dim.write.mode("overwrite").parquet(f"{OUTPUT_BASE_DIR}/dimensions/date_dim")
df_item.write.mode("overwrite").parquet(f"{OUTPUT_BASE_DIR}/dimensions/item")
print("Справочники сохранены в dimensions/.")

print("\n>>> [4/4] Запись форматов в small и large...")
FORMATS = [
    ("parquet", "snappy", "parquet",   {},                 "T18", "T19"),
    ("orc",     "snappy", "orc",       {},                 "T20", "T21"),
    ("avro",    "snappy", "avro",      {},                 "T22", "T23"),
    ("csv",     "none",   "csv_plain", {"header": "true"}, "T24", "T25"),
    ("csv",     "gzip",   "csv_gz",    {"header": "true"}, "T26", "T27")
]

for fmt, codec, folder, opts, t_small, t_large in FORMATS:
    target = f"{OUTPUT_BASE_DIR}/{folder}"
    
    t0 = time.time()
    w_small = df_store_sales.repartition(16).write.format(fmt).mode("overwrite")
    if codec != "none":
        w_small = w_small.option("compression", codec)
    for k, v in opts.items():
        w_small = w_small.option(k, v)
    w_small.save(f"{target}/small/store_sales")
    print(f"[{t_small}] {folder} small готов ({round(time.time() - t0, 2)} c)")

    t0 = time.time()
    w_large = df_store_sales.coalesce(1).write.format(fmt).mode("overwrite")
    if codec != "none":
        w_large = w_large.option("compression", codec)
    for k, v in opts.items():
        w_large = w_large.option(k, v)
    w_large.save(f"{target}/large/store_sales")
    print(f"[{t_large}] {folder} large готов ({round(time.time() - t0, 2)} c)")

spark.stop()
print("\n>>> Все форматы успешно сгенерированы!")