#!/usr/bin/env python3
import csv
import os
import subprocess
import time
import psycopg2
from pyspark.sql import SparkSession

DB_HOST = os.getenv("PGHOST", "127.0.0.1")
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

HDFS_BASE_DIR = os.getenv("HDFS_BASE_DIR", "hdfs:///user/admin/tpcds_benchmark/data")
METRICS_OUT = "results/raw_metrics/write_metrics.csv"

def get_hdfs_dir_stats(hdfs_path: str):
    out = subprocess.check_output(["hdfs", "dfs", "-count", hdfs_path], text=True).strip().split()
    return int(out[1]), int(out[2])

conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cur = conn.cursor()
PART_COL = "ss_sold_date_sk"
cur.execute(f"SELECT MIN({PART_COL}), MAX({PART_COL}), COUNT(*) FROM store_sales;")
min_val, max_val, pg_cnt = cur.fetchone()
cur.close()
conn.close()
print(f">>> PG store_sales: rows={pg_cnt}, {PART_COL}=[{min_val}..{max_val}]")

spark = (
    SparkSession.builder
    .appName("W2_Generate_Formats_ADH")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

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
    .option("numPartitions", "8")
    .option("fetchsize", "50000")
    .load()
)

spark.read.jdbc(url=JDBC_URL, table="date_dim", properties=JDBC_PROPS).write.mode("overwrite").parquet(f"{HDFS_BASE_DIR}/dimensions/date_dim")
spark.read.jdbc(url=JDBC_URL, table="item", properties=JDBC_PROPS).write.mode("overwrite").parquet(f"{HDFS_BASE_DIR}/dimensions/item")

FORMATS = [
    ("parquet", "snappy", "parquet",   {},                 "T18", "T19"),
    ("orc",     "snappy", "orc",       {},                 "T20", "T21"),
    ("avro",    "snappy", "avro",      {},                 "T22", "T23"),
    ("csv",     "none",   "csv_plain", {"header": "true"}, "T24", "T25"),
    ("csv",     "gzip",   "csv_gz",    {"header": "true"}, "T26", "T27"),
]

os.makedirs(os.path.dirname(METRICS_OUT), exist_ok=True)
rows = []

for fmt, codec, folder, opts, t_small, t_large in FORMATS:
    for layout, parts, task_id in [("small", 16, t_small), ("large", 1, t_large)]:
        target = f"{HDFS_BASE_DIR}/{layout}/{folder}/store_sales"
        t0 = time.perf_counter()

        df_out = df_store_sales.repartition(parts) if parts > 1 else df_store_sales.coalesce(1)
        writer = df_out.write.format(fmt).mode("overwrite")
        if codec != "none":
            writer = writer.option("compression", codec)
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.save(target)

        elapsed = round(time.perf_counter() - t0, 3)
        f_cnt, s_bytes = get_hdfs_dir_stats(target)
        s_mb = round(s_bytes / (1024 * 1024), 2)
        print(f"[{task_id}] {layout}/{folder}: {elapsed}s | files={f_cnt} | size={s_mb} MiB")

        rows.append({
            "task_id": task_id,
            "layout": layout,
            "format": folder,
            "codec": codec,
            "write_time_sec": elapsed,
            "file_count": f_cnt,
            "size_bytes": s_bytes,
            "size_mb": s_mb,
            "hdfs_path": target,
        })

with open(METRICS_OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

spark.stop()
print(f">>> Готово! Метрики сохранены в {METRICS_OUT}")
