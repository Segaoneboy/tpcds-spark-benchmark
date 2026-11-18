#!/usr/bin/env python3
"""
T29: сверка целостности всех вариантов хранения store_sales с эталоном из PostgreSQL.

Зависит от: T28 (форматы записаны) и T11 (эталонные метрики).

Для каждого из 10 вариантов (parquet, orc, avro, csv_plain, csv_gz x small, large) сравнивает с PostgreSQL:
  row_count, null_date_sk (число NULL в ss_sold_date_sk), sum_quantity, sum_net_paid,
  min/max ss_sold_date_sk, min/max календарной даты (через date_dim) и схему (имена, порядок, типы).

ЗАПУСК. Память и jar-файлы в коде НЕ задаются, их передаёт spark3-submit:
  bash scripts/run_spark.sh scripts/validate_integrity.py
run_spark.sh берёт из configs/env.*.sh: --driver-memory 2g --executor-memory 1536m --executor-cores 2 и
--jars postgresql-42.7.3.jar, spark-avro_2.13-3.5.4.jar (Scala 2.13, как на стенде). Чтение Avro требует
этот jar, поэтому всегда запускайте через spark3-submit (run_spark.sh), а не голым python3.
Если run_spark.sh не передаёт аргументы дальше (в старой версии не передаёт), используйте переменные
окружения VALIDATE_FORMATS / VALIDATE_LAYOUTS / VALIDATE_SCHEMA_ONLY=1 или вызывайте spark3-submit напрямую.

Где лежат данные (как пишет w2_etl_formats.py):
  <BASE>/<layout>/<format>/store_sales           layout: small | large
  <BASE>/dimensions/date_dim                      (parquet)
  BASE по умолчанию = $HDFS_BASE_DIR (как в w2_etl_formats.py).

Параметры CSV должны совпадать с записью в w2_etl_formats.py: {"header": "true"}, разделитель не задан -> ','.
Они вынесены в --csv-header (true) и --csv-sep (','); читаем с ними, а не с зашитой запятой.

Проверка типов ПЕРЕД финальным прогоном (одна быстрая операция, данные не сканируются):
  bash scripts/run_spark.sh scripts/validate_integrity.py --schema-only --formats avro --layouts small
В консоль печатается таблица "колонка / тип в PostgreSQL (ожидаемый в Spark) / реальный тип в файле".
Для Avro и ORC смотрите на decimal(7,2) и int: именно здесь сериализация могла бы изменить тип.

Эталон берётся из PostgreSQL (psycopg2, переменные PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD, как в w2),
и сверяется с results/reference_metrics/reference_metrics.csv (T11), если файл есть.
  --dump-reference FILE     сохранить эталон в JSON      --reference-json FILE   взять эталон из JSON

Результаты (results/raw_metrics/):
  integrity_summary.csv   итог по каждому варианту (PASS/FAIL/ERROR/MISSING)
  integrity_metrics.csv   каждая метрика: значение в PostgreSQL, в варианте, статус
  integrity_schema.csv    каждая колонка: тип в PostgreSQL, в варианте, статус, пояснение
Код возврата: 0 - все PASS, 1 - есть расхождения, ошибки или нет каталогов.
"""
import argparse
import csv
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Та же переменная и то же значение по умолчанию, что в w2_etl_formats.py
DEFAULT_BASE = os.getenv("HDFS_BASE_DIR", "hdfs:///user/admin/tpcds_benchmark/data")

FORMATS = ["parquet", "orc", "avro", "csv_plain", "csv_gz"]
LAYOUTS = ["small", "large"]
SPARK_FORMAT = {"parquet": "parquet", "orc": "orc", "avro": "avro", "csv_plain": "csv", "csv_gz": "csv"}
TABLE = "store_sales"

# Параметры записи CSV в w2_etl_formats.py: {"header": "true"}, разделитель не задан (по умолчанию ',')
CSV_HEADER_DEFAULT = "true"
CSV_SEP_DEFAULT = ","

METRICS = ["row_count", "null_date_sk", "sum_quantity", "sum_net_paid",
           "min_date_sk", "max_date_sk", "min_date", "max_date"]
DATE_METRICS = ("min_date", "max_date")
SKIPPED = "__SKIPPED__"

PG_TO_SPARK = {
    "integer": "int", "smallint": "smallint", "bigint": "bigint",
    "character": "string", "character varying": "string", "text": "string",
    "date": "date", "timestamp without time zone": "timestamp", "boolean": "boolean",
    "real": "float", "double precision": "double",
}


# ----------------------------------------------------------------------------
# Чистая логика (без Spark и PostgreSQL): легко проверяется отдельно
# ----------------------------------------------------------------------------
def pg_type_to_spark(data_type, precision=None, scale=None):
    """Тип PostgreSQL (information_schema) -> строка типа Spark (как DataType.simpleString())."""
    if data_type == "numeric":
        if precision is None:
            return "decimal(38,18)"  # так Spark JDBC читает numeric без ограничений
        return f"decimal({precision},{scale or 0})"
    return PG_TO_SPARK.get(data_type, data_type)


def normalize(metric, value):
    """Приводит значение метрики к сопоставимому виду: int/Decimal, ISO-дата либо None."""
    if value is None or value == "NULL":
        return None
    if metric in DATE_METRICS:
        return value.isoformat() if hasattr(value, "isoformat") else str(value)[:10]
    d = Decimal(str(value))
    if metric != "sum_net_paid" and d == d.to_integral_value():
        return int(d)
    return d


def compare_metrics(reference, got):
    rows = []
    for m in METRICS:
        ref_v = normalize(m, reference.get(m))
        if got.get(m, SKIPPED) == SKIPPED:
            rows.append({"metric": m, "pg_value": ref_v if ref_v is not None else "",
                         "variant_value": "", "status": "SKIPPED"})
            continue
        got_v = normalize(m, got[m])
        rows.append({"metric": m, "pg_value": "" if ref_v is None else ref_v,
                     "variant_value": "" if got_v is None else got_v,
                     "status": "MATCH" if ref_v == got_v else "MISMATCH"})
    return rows


def type_hint(expected, actual):
    """Подсказка о вероятной причине расхождения типа после сериализации."""
    if expected.startswith("decimal") and actual.startswith("decimal"):
        return "отличаются precision/scale decimal"
    if expected.startswith("decimal"):
        return f"decimal прочитан как {actual}: потерян логический тип decimal"
    if expected == "date" and actual in ("int", "bigint", "string"):
        return f"date прочитан как {actual}: потерян логический тип date"
    if expected in ("smallint", "tinyint") and actual == "int":
        return "Avro не имеет 8/16-битных типов: Spark пишет их как int"
    if expected == "int" and actual == "bigint":
        return "int расширен до bigint"
    return ""


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
            rows.append({"column": name, "position": i + 1, "pg_type": exp_type, "variant_type": "",
                         "status": "MISSING", "note": "колонки нет в варианте"})
            continue
        j, act_type = act[key]
        problems, note = [], ""
        if check_types and act_type is not None and act_type != exp_type:
            problems.append("TYPE")
            note = type_hint(exp_type, act_type)
        if j != i:
            problems.append("ORDER")
            note = (note + "; " if note else "") + f"позиция {j + 1} вместо {i + 1}"
        rows.append({"column": name, "position": i + 1, "pg_type": exp_type,
                     "variant_type": "n/a" if act_type is None else act_type,
                     "status": "+".join(problems) or "OK", "note": note})
    for j, (name, act_type) in enumerate(actual):
        if name.lower() not in expected_names:
            rows.append({"column": name, "position": j + 1, "pg_type": "",
                         "variant_type": "n/a" if act_type is None else act_type,
                         "status": "EXTRA", "note": "лишняя колонка"})
    return rows, all(r["status"] == "OK" for r in rows)


def load_t11_reference(root):
    """Эталон T11 (reference_metrics.csv) для store_sales в тех же ключах, что и METRICS."""
    path = root / "results" / "reference_metrics" / "reference_metrics.csv"
    if not path.is_file():
        return None
    wanted = {
        ("count", "*"): "row_count",
        ("null_count", "ss_sold_date_sk"): "null_date_sk",
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


def variant_path(base, layout, fmt):
    return f"{base.rstrip('/')}/{layout}/{fmt}/{TABLE}"


# ----------------------------------------------------------------------------
# PostgreSQL
# ----------------------------------------------------------------------------
def fetch_pg_reference():
    import psycopg2  # импорт внутри, чтобы остальную логику можно было проверять без драйвера

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "127.0.0.1"), port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "tpcds"), user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"))
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*), COUNT(*) FILTER (WHERE ss_sold_date_sk IS NULL), SUM(ss_quantity), "
            f"SUM(ss_net_paid), MIN(ss_sold_date_sk), MAX(ss_sold_date_sk) FROM {TABLE}")
        cnt, nulls, sq, snp, mn_sk, mx_sk = cur.fetchone()
        # календарные даты через date_dim (внутреннее соединение: строки с NULL в ss_sold_date_sk не входят)
        cur.execute(f"SELECT MIN(d.d_date), MAX(d.d_date) FROM {TABLE} s "
                    f"JOIN date_dim d ON d.d_date_sk = s.ss_sold_date_sk")
        mn_d, mx_d = cur.fetchone()
        cur.execute(
            "SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = %s ORDER BY ordinal_position", (TABLE,))
        schema = [{"name": n, "pg_type": t, "spark_type": pg_type_to_spark(t, p, s)}
                  for n, t, p, s in cur.fetchall()]
    finally:
        conn.close()
    if not schema:
        raise RuntimeError(f"таблица {TABLE} не найдена в PostgreSQL")

    def s(v):
        return None if v is None else (v.isoformat() if hasattr(v, "isoformat") else str(v))

    return {"metrics": {"row_count": s(cnt), "null_date_sk": s(nulls), "sum_quantity": s(sq),
                        "sum_net_paid": s(snp), "min_date_sk": s(mn_sk), "max_date_sk": s(mx_sk),
                        "min_date": s(mn_d), "max_date": s(mx_d)},
            "schema": schema}


# ----------------------------------------------------------------------------
# Spark. Память, master и jar-файлы задаёт spark3-submit (run_spark.sh), в коде их нет.
# ----------------------------------------------------------------------------
def make_spark():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("T29_Validate_Integrity").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def path_exists(spark, path):
    """Проверка пути через Hadoop FileSystem (hdfs://, file://); если проверить нельзя, пусть ответит Spark."""
    try:
        p = spark._jvm.org.apache.hadoop.fs.Path(path)
        return bool(p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p))
    except Exception:
        return True


def read_variant(spark, fmt, path, pg_schema, csv_header, csv_sep):
    """Возвращает (DataFrame, фактическая схема [(имя, тип|None)], check_types)."""
    if fmt.startswith("csv"):
        # CSV не хранит типы: значения читаем по схеме PostgreSQL. FAILFAST - нечитаемое значение даёт
        # ошибку, а не тихий NULL. Параметры header/sep те же, что при записи в w2_etl_formats.py.
        ddl = ", ".join(f"`{c['name']}` {c['spark_type']}" for c in pg_schema)
        df = (spark.read.option("header", csv_header).option("sep", csv_sep)
              .option("mode", "FAILFAST").schema(ddl).csv(path))
        if csv_header == "true":
            names = spark.read.option("header", "true").option("sep", csv_sep).csv(path).columns
            return df, [(n, None) for n in names], False
        return df, [(c["name"], None) for c in pg_schema], False  # без заголовка имён в файле нет
    df = spark.read.format(SPARK_FORMAT[fmt]).load(path)
    return df, [(f.name, f.dataType.simpleString()) for f in df.schema.fields], True


def spark_metrics(df, date_dim):
    """Все метрики за один проход по данным (left join с date_dim: d_date_sk уникален, строки не размножаются)."""
    from pyspark.sql import functions as F

    sk = F.col("ss_sold_date_sk")
    aggs = [F.count(F.lit(1)).alias("row_count"),
            F.sum(F.when(sk.isNull(), 1).otherwise(0)).alias("null_date_sk"),
            F.sum("ss_quantity").alias("sum_quantity"),
            F.sum("ss_net_paid").alias("sum_net_paid"),
            F.min("ss_sold_date_sk").alias("min_date_sk"),
            F.max("ss_sold_date_sk").alias("max_date_sk")]
    src = df.select("ss_sold_date_sk", "ss_quantity", "ss_net_paid")
    if date_dim is not None:
        src = src.join(F.broadcast(date_dim), sk == F.col("d_date_sk"), "left")
        aggs += [F.min("d_date").alias("min_date"), F.max("d_date").alias("max_date")]
    m = src.agg(*aggs).first().asDict()
    if date_dim is None:
        m.update({k: SKIPPED for k in DATE_METRICS})
    return m


# ----------------------------------------------------------------------------
def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def env_list(name, allowed):
    v = os.getenv(name)
    return [x for x in v.replace(",", " ").split() if x in allowed] if v else None


def print_schema_table(fmt, layout, rows):
    print(f"\n--- схема {layout}/{fmt}: ожидаемый тип (по PostgreSQL) и реальный тип в файле ---")
    print(f"{'#':>2} {'колонка':<24} {'ожидается':<14} {'в файле':<14} статус")
    for r in rows:
        extra = f"  ({r['note']})" if r.get("note") else ""
        print(f"{r['position']:>2} {r['column']:<24} {r['pg_type']:<14} {r['variant_type']:<14} {r['status']}{extra}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--base", default=DEFAULT_BASE, help="корень данных (по умолчанию $HDFS_BASE_DIR)")
    ap.add_argument("--out-dir", default=str(ROOT / "results" / "raw_metrics"))
    ap.add_argument("--formats", nargs="+", choices=FORMATS,
                    default=env_list("VALIDATE_FORMATS", FORMATS) or FORMATS)
    ap.add_argument("--layouts", nargs="+", choices=LAYOUTS,
                    default=env_list("VALIDATE_LAYOUTS", LAYOUTS) or LAYOUTS)
    ap.add_argument("--csv-header", choices=["true", "false"], default=CSV_HEADER_DEFAULT,
                    help="как писался CSV в w2_etl_formats.py (там header=true)")
    ap.add_argument("--csv-sep", default=CSV_SEP_DEFAULT,
                    help="разделитель CSV (в w2_etl_formats.py не задан -> ',')")
    ap.add_argument("--schema-only", action="store_true",
                    default=os.getenv("VALIDATE_SCHEMA_ONLY") == "1",
                    help="только схема: печатает ожидаемые и реальные типы, данные не сканируются")
    ap.add_argument("--reference-json", help="взять эталон из JSON, не обращаясь к PostgreSQL")
    ap.add_argument("--dump-reference", help="сохранить эталон из PostgreSQL в JSON")
    args = ap.parse_args(argv)

    out_dir = Path(args.out_dir)
    problems = []

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

    # 3. Spark и варианты
    spark = make_spark()
    base = args.base
    date_dim = None
    if not args.schema_only:
        dd_path = f"{base.rstrip('/')}/dimensions/date_dim"
        if path_exists(spark, dd_path):
            date_dim = spark.read.parquet(dd_path).select("d_date_sk", "d_date")
        else:
            print(f"[WARN] нет {dd_path}: min/max календарной даты пропущены (min/max ss_sold_date_sk проверяются)")

    selected = [(f, l) for f in FORMATS if f in args.formats for l in LAYOUTS if l in args.layouts]
    summary, all_metrics, all_schema = [], [], []
    for fmt, layout in selected:
        path = variant_path(base, layout, fmt)
        row = {"layout": layout, "format": fmt, "status": "", "metrics_ok": "", "schema_ok": "", "detail": ""}
        if not path_exists(spark, path):
            row.update(status="MISSING", detail=f"нет каталога {path}")
            summary.append(row)
            continue
        print(f"\n>>> {layout}/{fmt}")
        error, metric_rows, schema_ok = None, [], False
        try:
            df, actual_schema, check_types = read_variant(spark, fmt, path, pg_schema,
                                                          args.csv_header, args.csv_sep)
            schema_rows, schema_ok = compare_schema(expected_schema, actual_schema, check_types)
            all_schema += [{"layout": layout, "format": fmt, **r} for r in schema_rows]
            if args.schema_only:
                print_schema_table(fmt, layout, schema_rows)
            else:
                metric_rows = compare_metrics(ref_metrics, spark_metrics(df, date_dim))
                all_metrics += [{"layout": layout, "format": fmt, **r} for r in metric_rows]
        except Exception as exc:  # один сбойный вариант не должен останавливать остальные
            error = f"{type(exc).__name__}: {str(exc).splitlines()[0][:300]}"
        bad_m = [r["metric"] for r in metric_rows if r["status"] == "MISMATCH"]
        status = variant_status(metric_rows, schema_ok, error)
        detail = error or "; ".join(([f"расходятся метрики: {', '.join(bad_m)}"] if bad_m else [])
                                    + ([] if schema_ok else ["схема не совпадает"]))
        row.update(status=status, detail=detail,
                   metrics_ok="" if (error or args.schema_only) else ("yes" if not bad_m else "NO"),
                   schema_ok="" if error else ("yes" if schema_ok else "NO"))
        summary.append(row)
        print(f"    {status}{(': ' + detail) if detail else ''}")

    spark.stop()

    write_csv(out_dir / "integrity_summary.csv", summary,
              ["layout", "format", "status", "metrics_ok", "schema_ok", "detail"])
    if not args.schema_only:
        write_csv(out_dir / "integrity_metrics.csv", all_metrics,
                  ["layout", "format", "metric", "pg_value", "variant_value", "status"])
    write_csv(out_dir / "integrity_schema.csv", all_schema,
              ["layout", "format", "column", "position", "pg_type", "variant_type", "status", "note"])

    # 4. Итог
    print("\n=== Итог ===")
    print(f"{'layout':<6} {'формат':<10} {'статус':<8} метрики  схема")
    for r in summary:
        print(f"{r['layout']:<6} {r['format']:<10} {r['status']:<8} {r['metrics_ok'] or '-':<8} "
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
