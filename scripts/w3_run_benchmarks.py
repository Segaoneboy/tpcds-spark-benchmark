#!/usr/bin/env python3
"""
Единый бенчмарк чтения W3 (T39, T41-T47):
  - Читает готовые 10 вариантов store_sales из HDFS (/user/admin/tpcds_benchmark/data/{layout}/{format}/store_sales)
  - Выполняет командные SQL-запросы scripts/t35.sql (Q1), t36.sql (Q2), t37.sql (Q3), t38.sql (Q4)
  - Протокол замеров: 1 warmup + 3 measured с random.shuffle порядка вариантов в каждом раунде
  - Фиксирует input_partitions (Source) и rdd_partitions (финальный план), без cache/persist
  - Сохраняет EXPLAIN FORMATTED (T45), сверку агрегатов (T46) и метаданные Event Log (T47)
"""
import argparse
import csv
import json
import os
import random
import re
import statistics
import time
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_HDFS_BASE = os.getenv("HDFS_BASE_DIR", "hdfs:///user/admin/tpcds_benchmark/data").rstrip("/")
OUT_DIR = ROOT / "results" / "raw_metrics"
PLANS_DIR = ROOT / "results" / "explain_plans"

FORMATS = ["parquet", "orc", "avro", "csv_plain", "csv_gz"]
LAYOUTS = ["small", "large"]


def load_sql(rel_path: str) -> str:
    raw = (ROOT / rel_path).read_text(encoding="utf-8")
    lines = [re.sub(r"--.*$", "", line) for line in raw.splitlines()]
    return "\n".join(lines).strip().rstrip(";")


def read_store_sales(spark: SparkSession, hdfs_base: str, layout: str, fmt: str, ss_schema):
    path = f"{hdfs_base}/{layout}/{fmt}/store_sales"
    if fmt in ("parquet", "orc", "avro"):
        return spark.read.format(fmt).load(path)
    # csv_plain и csv_gz писались в w2_etl_formats.py с header=true и разделителем ','
    return (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .schema(ss_schema)
        .csv(path)
    )


def serialize_row(row) -> str:
    return json.dumps([str(v) if v is not None else None for v in row], ensure_ascii=False)


def main():
    ap = argparse.ArgumentParser(description="W3 TPC-DS Read Benchmarks (T41-T47)")
    ap.add_argument("--hdfs-base", default=DEFAULT_HDFS_BASE, help="Корень данных в HDFS")
    ap.add_argument("--warmup", type=int, default=1, help="Число прогревочных раундов")
    ap.add_argument("--runs", type=int, default=3, help="Число измеряемых раундов")
    ap.add_argument("--seed", type=int, default=42, help="Seed для перемешивания порядка запуска")
    args = ap.parse_args()

    hdfs_base = args.hdfs_base.rstrip("/")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PLANS_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("W3_TPCDS_Read_Benchmarks_T41_T47")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    app_id = spark.sparkContext.applicationId
    event_log_dir = spark.conf.get("spark.eventLog.dir", "hdfs:/var/log/spark/apps")

    # Загружаем эталонную схему store_sales и справочник item
    ss_schema = spark.read.parquet(f"{hdfs_base}/large/parquet/store_sales").schema
    df_item = spark.read.parquet(f"{hdfs_base}/dimensions/item")
    df_item.createOrReplaceTempView("item")

    queries_q1_q3 = [
        ("T41", "Q1_full_scan", load_sql("scripts/t35.sql")),
        ("T42", "Q2_selective", load_sql("scripts/t36.sql")),
        ("T43", "Q3_join_item", load_sql("scripts/t37.sql")),
    ]
    q4_sql = load_sql("scripts/t38.sql")

    combinations = [(layout, fmt) for layout in LAYOUTS for fmt in FORMATS]

    # 1. T45: Сохраняем полные планы EXPLAIN FORMATTED один раз для всех комбинаций
    print("\n[T45] Снятие планов выполнения EXPLAIN FORMATTED...")
    source_parts_map = {}
    for layout, fmt in combinations:
        df_ss = read_store_sales(spark, hdfs_base, layout, fmt, ss_schema)
        src_parts = df_ss.rdd.getNumPartitions()
        source_parts_map[(layout, fmt)] = src_parts
        df_ss.createOrReplaceTempView("store_sales")

        plan_path = PLANS_DIR / f"explain_{layout}_{fmt}.txt"
        with plan_path.open("w", encoding="utf-8") as pf:
            for task_id, q_name, q_text in queries_q1_q3:
                exp = "\n".join(r[0] for r in spark.sql(f"EXPLAIN FORMATTED {q_text}").collect())
                pf.write(f"=== {task_id} | {q_name} ({layout}/{fmt}, source_partitions={src_parts}) ===\n{exp}\n\n")

    # 2. Генерируем расписание запусков Q1-Q3: warmup + measured со случайным перемешиванием (random.shuffle)
    schedule = []
    for w_idx in range(1, args.warmup + 1):
        shuffled = list(combinations)
        random.Random(args.seed + w_idx).shuffle(shuffled)
        for layout, fmt in shuffled:
            for task_id, q_name, q_text in queries_q1_q3:
                schedule.append(("warmup", w_idx, layout, fmt, task_id, q_name, q_text))

    for r_idx in range(1, args.runs + 1):
        shuffled = list(combinations)
        random.Random(args.seed + 100 + r_idx).shuffle(shuffled)
        for layout, fmt in shuffled:
            for task_id, q_name, q_text in queries_q1_q3:
                schedule.append(("measured", r_idx, layout, fmt, task_id, q_name, q_text))

    raw_runs = []
    q1_signatures = {}
    total_steps = len(schedule)
    print(f"\n[T41-T43] Запуск замеров Q1-Q3: всего {total_steps} выполнений ({args.warmup} warmup + {args.runs} measured)...")

    for step_idx, (phase, run_idx, layout, fmt, task_id, q_name, q_text) in enumerate(schedule, 1):
        spark.catalog.clearCache()
        df_ss = read_store_sales(spark, hdfs_base, layout, fmt, ss_schema)
        src_parts = source_parts_map[(layout, fmt)]
        df_ss.createOrReplaceTempView("store_sales")

        df_res = spark.sql(q_text)
        rdd_parts = df_res.rdd.getNumPartitions()

        t0 = time.perf_counter()
        rows = df_res.collect()
        dt = round(time.perf_counter() - t0, 4)

        print(
            f"[{step_idx:3d}/{total_steps}] ({phase:8s} #{run_idx}) "
            f"{layout:5s}/{fmt:9s} | {q_name:14s} | {dt:7.3f} s | src_p={src_parts}, rdd_p={rdd_parts}"
        )

        raw_runs.append({
            "timestamp": datetime.now().isoformat(),
            "app_id": app_id,
            "task_id": task_id,
            "phase": phase,
            "run_idx": run_idx,
            "layout": layout,
            "format": fmt,
            "query": q_name,
            "source_partitions": src_parts,
            "rdd_partitions": rdd_parts,
            "exec_time_sec": dt,
            "result_rows": len(rows),
        })

        if phase == "measured" and q_name == "Q1_full_scan" and rows:
            q1_signatures[(layout, fmt)] = serialize_row(rows[0])

    # 3. T44: Q4 (CSV vs CSV.GZ для small и large)
    print("\n[T44] Запуск Q4: сравнение CSV vs CSV.GZ (small и large)...")
    q4_validation = []
    for layout in LAYOUTS:
        df_csv = read_store_sales(spark, hdfs_base, layout, "csv_plain", ss_schema)
        df_gz = read_store_sales(spark, hdfs_base, layout, "csv_gz", ss_schema)
        p_csv = df_csv.rdd.getNumPartitions()
        p_gz = df_gz.rdd.getNumPartitions()
        df_csv.createOrReplaceTempView("store_sales_csv")
        df_gz.createOrReplaceTempView("store_sales_csvgz")

        exp_q4 = "\n".join(r[0] for r in spark.sql(f"EXPLAIN FORMATTED {q4_sql}").collect())
        (PLANS_DIR / f"explain_Q4_T44_{layout}.txt").write_text(exp_q4, encoding="utf-8")

        q4_schedule = [("warmup", w) for w in range(1, args.warmup + 1)] + [("measured", r) for r in range(1, args.runs + 1)]
        last_rows = []
        for phase, run_idx in q4_schedule:
            spark.catalog.clearCache()
            df_res = spark.sql(q4_sql)
            rdd_parts = df_res.rdd.getNumPartitions()

            t0 = time.perf_counter()
            rows = df_res.collect()
            dt = round(time.perf_counter() - t0, 4)
            print(
                f"  [T44 | Q4_{layout:5s}] ({phase:8s} #{run_idx}) {dt:7.3f} s "
                f"(csv_p={p_csv}, csvgz_p={p_gz}, rdd_p={rdd_parts})"
            )

            raw_runs.append({
                "timestamp": datetime.now().isoformat(),
                "app_id": app_id,
                "task_id": "T44",
                "phase": phase,
                "run_idx": run_idx,
                "layout": layout,
                "format": f"csv({p_csv}p)_vs_csvgz({p_gz}p)",
                "query": "Q4_csv_vs_csvgz",
                "source_partitions": f"{p_csv}/{p_gz}",
                "rdd_partitions": rdd_parts,
                "exec_time_sec": dt,
                "result_rows": len(rows),
            })
            if phase == "measured":
                last_rows = rows

        all_ok = all(r["validation_status"] == "OK" for r in last_rows)
        q4_validation.append({
            "query": "Q4_csv_vs_csvgz",
            "layout": layout,
            "format": "csv_plain+csv_gz",
            "status": "PASS" if all_ok else "MISMATCH",
            "agg_signature": f"stores_checked={len(last_rows)}, all_ok={all_ok}",
        })

    # 4. Формируем сводную таблицу (медиана, среднее, min-max, std по measured запускам)
    summary_rows = []
    seen_keys = []
    for r in raw_runs:
        key = (r["task_id"], r["query"], r["layout"], r["format"], r["source_partitions"], r["rdd_partitions"])
        if key not in seen_keys:
            seen_keys.append(key)

    for task_id, q_name, layout, fmt, src_p, rdd_p in seen_keys:
        vals = [
            r["exec_time_sec"]
            for r in raw_runs
            if r["phase"] == "measured"
            and r["task_id"] == task_id
            and r["layout"] == layout
            and r["format"] == fmt
        ]
        summary_rows.append({
            "task_id": task_id,
            "query": q_name,
            "layout": layout,
            "format": fmt,
            "source_partitions": src_p,
            "rdd_partitions": rdd_p,
            "min_sec": round(min(vals), 4),
            "median_sec": round(statistics.median(vals), 4),
            "mean_sec": round(statistics.mean(vals), 4),
            "max_sec": round(max(vals), 4),
            "std_sec": round(statistics.stdev(vals), 4) if len(vals) > 1 else 0.0,
        })

    # 5. T46: Сверка агрегатов Q1 между всеми 10 вариантами + Q4
    validation_checks = []
    ref_sig = q1_signatures.get(("large", "parquet")) or next(iter(q1_signatures.values()))
    for layout, fmt in combinations:
        sig = q1_signatures.get((layout, fmt), "")
        validation_checks.append({
            "query": "Q1_full_scan",
            "layout": layout,
            "format": fmt,
            "status": "PASS" if sig == ref_sig else "MISMATCH",
            "agg_signature": sig,
        })
    validation_checks.extend(q4_validation)

    # Сохраняем все файлы результатов
    with (OUT_DIR / "read_metrics_raw.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(raw_runs[0].keys()))
        w.writeheader()
        w.writerows(raw_runs)

    with (OUT_DIR / "read_metrics_summary.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        w.writeheader()
        w.writerows(summary_rows)

    with (OUT_DIR / "t46_query_validation.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(validation_checks[0].keys()))
        w.writeheader()
        w.writerows(validation_checks)

    # 6. T47: Метаданные Event Log
    event_meta = {
        "task_id": "T47",
        "application_id": app_id,
        "event_log_hdfs_dir": event_log_dir,
        "expected_event_log_path": f"{event_log_dir.rstrip('/')}/{app_id}",
        "fetch_command": f"hdfs dfs -get {event_log_dir.rstrip('/')}/{app_id} results/event_logs/",
    }
    (OUT_DIR / "t47_event_log_info.json").write_text(json.dumps(event_meta, indent=2), encoding="utf-8")
    print(f"\n>>> Бенчмарк W3 завершён! Application ID (T47): {app_id}")
    spark.stop()


if __name__ == "__main__":
    main()
