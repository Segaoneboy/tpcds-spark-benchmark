#!/usr/bin/env python3
"""
Сверка целостности всех вариантов хранения store_sales с эталоном из PostgreSQL.

Зависит от: T28 (форматы записаны в data/formats) и T11 (эталонные метрики).

Для каждого из 10 вариантов (parquet, orc, avro, csv_plain, csv_gz x small, large) сравнивает с PostgreSQL:
  row_count, sum_quantity, sum_net_paid, min/max ss_sold_date_sk, min/max календарной даты
  (через date_dim) и схему (имена колонок, порядок, типы).

Эталон берётся из PostgreSQL напрямую (psycopg2). Он же сверяется с файлом T11
(results/reference_metrics/reference_metrics.csv), если тот есть.

Запуск из корня репозитория (переменные PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD, как в других скриптах):
  python3 scripts/validate_integrity.py
  python3 scripts/validate_integrity.py --formats parquet csv_gz --variants large
  python3 scripts/validate_integrity.py --dump-reference results/raw_metrics/pg_reference.json
  python3 scripts/validate_integrity.py --reference-json results/raw_metrics/pg_reference.json   # без PostgreSQL

Результаты (results/raw_metrics/):
  integrity_summary.csv   - итог по каждому варианту (PASS/FAIL)
  integrity_metrics.csv   - каждая метрика: значение в PostgreSQL, в варианте, статус
  integrity_schema.csv    - каждая колонка: тип в PostgreSQL, в варианте, статус

Код возврата: 0 - все варианты PASS, 1 - есть расхождения, ошибки или отсутствующие варианты.
"""
import argparse
import csv
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

FORMATS = ["parquet", "orc", "avro", "csv_plain", "csv_gz"]
VARIANTS = ["small", "large"]
SPARK_FORMAT = {"parquet": "parquet", "orc": "orc", "avro": "avro", "csv_plain": "csv", "csv_gz": "csv"}
TABLE = "store_sales"

METRICS = [
    "row_count",
    "sum_quantity",
    "sum_net_paid",
    "min_date_sk",
    "max_date_sk",
    "min_date",
    "max_date",
]
DATE_METRICS = ("min_date", "max_date")
SKIPPED = "__SKIPPED__"

PG_TO_SPARK = {
    "integer": "int",
    "smallint": "smallint",
    "bigint": "bigint",
    "character": "string",
    "character varying": "string",
    "text": "string",
    "date": "date",
    "timestamp without time zone": "timestamp",
    "boolean": "boolean",
    "real": "float",
    "double precision": "double",
}


# ----------------------------------------------------------------------------
# Чистая логика (без Spark и PostgreSQL) - легко проверяется отдельно
# ----------------------------------------------------------------------------
def pg_type_to_spark(data_type, precision=None, scale=None):
    """Тип PostgreSQL (information_schema) -> строка типа Spark (как DataType.simpleString())."""
    if data_type == "numeric":
        if precision is None:
            return "decimal(38,18)"  # так Spark JDBC читает numeric без ограничений
        return f"decimal({precision},{scale or 0})"
    return PG_TO_SPARK.get(data_type, data_type)


def normalize(metric, value):
    """Приводит значение метрики к сопоставимому виду: Decimal/int, ISO-дата, либо None."""
    if value is None or value == "NULL":
        return None
    if metric in DATE_METRICS:
        return value.isoformat() if hasattr(value, "isoformat") else str(value)[:10]
    d = Decimal(str(value))
    if metric != "sum_net_paid" and d == d.to_integral_value():
        return int(d)
    return d


def compare_metrics(reference, got):
    """reference/got: {metric: value}. Возвращает список строк отчёта."""
    rows = []
    for m in METRICS:
        ref_v = normalize(m, reference.get(m))
        if got.get(m, SKIPPED) == SKIPPED:
            rows.append({"metric": m, "pg_value": ref_v, "variant_value": "", "status": "SKIPPED"})
            continue
        got_v = normalize(m, got[m])
        rows.append(
            {
                "metric": m,
                "pg_value": "" if ref_v is None else ref_v,
                "variant_value": "" if got_v is None else got_v,
                "status": "MATCH" if ref_v == got_v else "MISMATCH",
            }
        )
    return rows


def compare_schema(expected, actual, check_types=True):
    """
    expected: [(имя, тип Spark)] из PostgreSQL; actual: [(имя, тип или None)] из варианта.
    Возвращает (строки отчёта, всё ли совпало).
    """
    rows = []
    act = {n.lower(): (i, t) for i, (n, t) in enumerate(actual)}
    expected_names = set()
    for i, (name, exp_type) in enumerate(expected):
        key = name.lower()
        expected_names.add(key)
        if key not in act:
            rows.append({"column": name, "position": i + 1, "pg_type": exp_type,
                         "variant_type": "", "status": "MISSING"})
            continue
        j, act_type = act[key]
        problems = []
        if check_types and act_type is not None and act_type != exp_type:
            problems.append("TYPE")
        if j != i:
            problems.append("ORDER")
        rows.append({"column": name, "position": i + 1, "pg_type": exp_type,
                     "variant_type": "n/a" if act_type is None else act_type,
                     "status": "+".join(problems) or "OK"})
    for j, (name, act_type) in enumerate(actual):
        if name.lower() not in expected_names:
            rows.append({"column": name, "position": j + 1, "pg_type": "",
                         "variant_type": "n/a" if act_type is None else act_type, "status": "EXTRA"})
    return rows, all(r["status"] == "OK" for r in rows)


def load_t11_reference(root):
    """Эталон T11 (reference_metrics.csv) для store_sales в тех же ключах, что и METRICS."""
    path = root / "results" / "reference_metrics" / "reference_metrics.csv"
    if not path.is_file():
        return None
    wanted = {
        ("count", "*"): "row_count",
        ("sum", "ss_quantity"): "sum_quantity",
        ("sum", "ss_net_paid"): "sum_net_paid",
        ("min", "ss_sold_date_sk"): "min_date_sk",
        ("max", "ss_sold_date_sk"): "max_date_sk",
    }
    out = {}
    with path.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r.get("table_name") != TABLE:
                continue
            key = wanted.get((r.get("metric"), r.get("column_name")))
            if key:
                out[key] = r["value"]
    return out


def variant_status(metric_rows, schema_ok, error):
    if error:
        return "ERROR"
    bad = [r for r in metric_rows if r["status"] == "MISMATCH"]
    return "PASS" if not bad and schema_ok else "FAIL"


# ----------------------------------------------------------------------------
# PostgreSQL
# ----------------------------------------------------------------------------
def fetch_pg_reference():
    import psycopg2  # импорт внутри, чтобы остальную логику можно было проверять без драйвера

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "tpcds"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*), SUM(ss_quantity), SUM(ss_net_paid), "
            f"MIN(ss_sold_date_sk), MAX(ss_sold_date_sk) FROM {TABLE}"
        )
        cnt, sq, snp, mn_sk, mx_sk = cur.fetchone()
        # календарные даты через date_dim (внутреннее соединение, как и в проверке вариантов)
        cur.execute(
            f"SELECT MIN(d.d_date), MAX(d.d_date) FROM {TABLE} s "
            f"JOIN date_dim d ON d.d_date_sk = s.ss_sold_date_sk"
        )
        mn_d, mx_d = cur.fetchone()
        cur.execute(
            "SELECT column_name, data_type, numeric_precision, numeric_scale "
            "FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = %s "
            "ORDER BY ordinal_position",
            (TABLE,),
        )
        schema = [
            {"name": n, "pg_type": t, "spark_type": pg_type_to_spark(t, p, s)}
            for n, t, p, s in cur.fetchall()
        ]
    finally:
        conn.close()
    if not schema:
        raise RuntimeError(f"таблица {TABLE} не найдена в PostgreSQL")

    def s(v):
        return None if v is None else (v.isoformat() if hasattr(v, "isoformat") else str(v))

    metrics = {
        "row_count": s(cnt), "sum_quantity": s(sq), "sum_net_paid": s(snp),
        "min_date_sk": s(mn_sk), "max_date_sk": s(mx_sk), "min_date": s(mn_d), "max_date": s(mx_d),
    }
    return {"metrics": metrics, "schema": schema}


# ----------------------------------------------------------------------------
# Spark
# ----------------------------------------------------------------------------
def make_spark(driver_memory):
    from pyspark.sql import SparkSession

    jars = sorted(str(p) for p in (ROOT / "jars").glob("spark-avro_*.jar"))
    b = (
        SparkSession.builder.appName("Validate_Integrity")
        .master("local[*]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "16")
    )
    if jars:
        b = b.config("spark.jars", ",".join(jars))
    else:
        b = b.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.4")
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_variant(spark, fmt, path, pg_schema):
    """Возвращает (DataFrame, фактическая схема [(имя, тип|None)], check_types)."""
    if fmt.startswith("csv"):
        # CSV не хранит типы: имена берём из заголовка, значения читаем по схеме PostgreSQL.
        # FAILFAST - нечитаемое значение даёт ошибку, а не тихий NULL.
        header = spark.read.option("header", "true").csv(path).columns
        ddl = ", ".join(f"`{c['name']}` {c['spark_type']}" for c in pg_schema)
        df = (spark.read.option("header", "true").option("mode", "FAILFAST")
              .schema(ddl).csv(path))
        return df, [(n, None) for n in header], False
    df = spark.read.format(SPARK_FORMAT[fmt]).load(path)
    return df, [(f.name, f.dataType.simpleString()) for f in df.schema.fields], True


def spark_metrics(spark, df, date_dim):
    from pyspark.sql import functions as F

    r = df.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum("ss_quantity").alias("sum_quantity"),
        F.sum("ss_net_paid").alias("sum_net_paid"),
        F.min("ss_sold_date_sk").alias("min_date_sk"),
        F.max("ss_sold_date_sk").alias("max_date_sk"),
    ).first()
    m = r.asDict()
    if date_dim is None:
        m.update({k: SKIPPED for k in DATE_METRICS})
    else:
        j = (df.select("ss_sold_date_sk")
             .join(F.broadcast(date_dim), F.col("ss_sold_date_sk") == F.col("d_date_sk"))
             .agg(F.min("d_date").alias("min_date"), F.max("d_date").alias("max_date")).first())
        m.update(j.asDict())
    return m


# ----------------------------------------------------------------------------
def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--base", default=str(ROOT / "data" / "formats"))
    ap.add_argument("--out-dir", default=str(ROOT / "results" / "raw_metrics"))
    ap.add_argument("--formats", nargs="+", choices=FORMATS, default=FORMATS)
    ap.add_argument("--variants", nargs="+", choices=VARIANTS, default=VARIANTS)
    ap.add_argument("--driver-memory", default="6g")
    ap.add_argument("--reference-json", help="взять эталон из JSON, не обращаясь к PostgreSQL")
    ap.add_argument("--dump-reference", help="сохранить эталон из PostgreSQL в JSON")
    args = ap.parse_args(argv)

    base, out_dir = Path(args.base), Path(args.out_dir)
    problems = []  # критичные замечания для кода возврата

    # 1. Эталон
    if args.reference_json:
        ref = json.loads(Path(args.reference_json).read_text(encoding="utf-8"))
        print(f"Эталон загружен из {args.reference_json}")
    else:
        print("Эталон: запрос к PostgreSQL...")
        ref = fetch_pg_reference()
    if args.dump_reference:
        Path(args.dump_reference).parent.mkdir(parents=True, exist_ok=True)
        Path(args.dump_reference).write_text(json.dumps(ref, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Эталон сохранён: {args.dump_reference}")
    ref_metrics, pg_schema = ref["metrics"], ref["schema"]
    expected_schema = [(c["name"], c["spark_type"]) for c in pg_schema]

    # 2. Эталон PostgreSQL против файла T11
    t11 = load_t11_reference(ROOT)
    if t11 is None:
        print("[INFO] файл T11 (results/reference_metrics/reference_metrics.csv) не найден, сверка эталонов пропущена")
    else:
        diffs = [m for m, v in t11.items() if normalize(m, v) != normalize(m, ref_metrics.get(m))]
        if diffs:
            problems.append("эталон PostgreSQL не совпадает с T11")
            for m in diffs:
                print(f"[WARN] эталон расходится с T11: {m}: PostgreSQL={ref_metrics.get(m)} T11={t11[m]}")
        else:
            print("[OK] эталон PostgreSQL совпадает с T11")

    # 3. Варианты
    selected = [(f, v) for f in FORMATS if f in args.formats for v in VARIANTS if v in args.variants]
    present = [(f, v) for f, v in selected if (base / f / v / TABLE).is_dir()]
    spark = date_dim = None
    if present:
        spark = make_spark(args.driver_memory)
        dd_path = base / "dimensions" / "date_dim"
        if dd_path.is_dir():
            date_dim = spark.read.parquet(str(dd_path)).select("d_date_sk", "d_date")
        else:
            print(f"[WARN] нет {dd_path}: min/max даты будут пропущены (останутся min/max ss_sold_date_sk)")

    summary, all_metrics, all_schema = [], [], []
    for fmt, var in selected:
        path = base / fmt / var / TABLE
        row = {"format": fmt, "variant": var, "status": "", "metrics_ok": "", "schema_ok": "", "detail": ""}
        if (fmt, var) not in present:
            row.update(status="MISSING", detail=f"нет каталога {path}")
            summary.append(row)
            continue
        print(f"\n>>> {fmt}/{var}")
        error, metric_rows, schema_ok = None, [], False
        try:
            df, actual_schema, check_types = read_variant(spark, fmt, str(path), pg_schema)
            schema_rows, schema_ok = compare_schema(expected_schema, actual_schema, check_types)
            metric_rows = compare_metrics(ref_metrics, spark_metrics(spark, df, date_dim))
            for r in schema_rows:
                all_schema.append({"format": fmt, "variant": var, **r})
            for r in metric_rows:
                all_metrics.append({"format": fmt, "variant": var, **r})
        except Exception as exc:  # один сбойный вариант не должен останавливать остальные
            error = f"{type(exc).__name__}: {str(exc).splitlines()[0][:300]}"
        status = variant_status(metric_rows, schema_ok, error)
        bad_m = [r["metric"] for r in metric_rows if r["status"] == "MISMATCH"]
        detail = error or "; ".join(
            ([f"расходятся метрики: {', '.join(bad_m)}"] if bad_m else [])
            + ([] if schema_ok else ["схема не совпадает"])
        )
        row.update(status=status, detail=detail,
                   metrics_ok="" if error else ("yes" if not bad_m else "NO"),
                   schema_ok="" if error else ("yes" if schema_ok else "NO"))
        summary.append(row)
        print(f"    {status}{(': ' + detail) if detail else ''}")

    if spark is not None:
        spark.stop()

    write_csv(out_dir / "integrity_summary.csv", summary,
              ["format", "variant", "status", "metrics_ok", "schema_ok", "detail"])
    write_csv(out_dir / "integrity_metrics.csv", all_metrics,
              ["format", "variant", "metric", "pg_value", "variant_value", "status"])
    write_csv(out_dir / "integrity_schema.csv", all_schema,
              ["format", "variant", "column", "position", "pg_type", "variant_type", "status"])

    # 4. Итог
    print("\n=== Итог ===")
    print(f"{'формат':<10} {'вариант':<6} {'статус':<8} метрики  схема")
    for r in summary:
        print(f"{r['format']:<10} {r['variant']:<6} {r['status']:<8} {r['metrics_ok'] or '-':<8} "
              f"{r['schema_ok'] or '-':<5} {r['detail']}")
    print(f"\nФайлы: {out_dir}/integrity_summary.csv, integrity_metrics.csv, integrity_schema.csv")

    not_pass = [r for r in summary if r["status"] != "PASS"]
    if not_pass:
        problems.append(f"вариантов не PASS: {len(not_pass)} из {len(summary)}")
    for p in problems:
        print(f"[!] {p}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
